/*-------------------------------------------------------------------------
 *
 * cluster_formation_marker.c
 *	  Durable cold-formation marker codec + validation (see
 *	  cluster_formation_marker.h for the design and the frozen 5.22
 *	  observation-window + arbiter marker ruling).
 *
 *	  The on-disk image is a fixed little-endian layout inside one
 *	  CLUSTER_VOTING_SLOT_BYTES slot (region 7): [header 72 bytes][compact
 *	  member entries: uint8 node_id + uint64 incarnation, n_admitted of
 *	  them].  CRC32C covers [magic .. _pad2] (host-order struct fields are
 *	  NOT the wire image — encode/decode bridge explicitly, like the JCMK
 *	  codec).
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "cluster/cluster_formation_marker.h"
#include "cluster/cluster_voting_disk_io.h"
#include "utils/pg_crc.h"

/* Little-endian wire helpers (mirror cluster_control_root.c). */
static void
write_u16_le(uint8 *dst, uint16 value)
{
	dst[0] = (uint8)(value & 0xff);
	dst[1] = (uint8)((value >> 8) & 0xff);
}

static void
write_u32_le(uint8 *dst, uint32 value)
{
	int i;

	for (i = 0; i < 4; i++)
		dst[i] = (uint8)((value >> (8 * i)) & 0xff);
}

static void
write_u64_le(uint8 *dst, uint64 value)
{
	int i;

	for (i = 0; i < 8; i++)
		dst[i] = (uint8)((value >> (8 * i)) & 0xff);
}

static uint16
read_u16_le(const uint8 *src)
{
	return (uint16)src[0] | ((uint16)src[1] << 8);
}

static uint32
read_u32_le(const uint8 *src)
{
	return (uint32)src[0] | ((uint32)src[1] << 8) | ((uint32)src[2] << 16) | ((uint32)src[3] << 24);
}

static uint64
read_u64_le(const uint8 *src)
{
	uint64 v = 0;
	int i;

	for (i = 0; i < 8; i++)
		v |= (uint64)src[i] << (8 * i);
	return v;
}

void
cluster_formation_marker_compute_crc(ClusterFormationCommitMarker *marker)
{
	pg_crc32c crc;

	INIT_CRC32C(crc);
	COMP_CRC32C(crc, marker, offsetof(ClusterFormationCommitMarker, crc32c));
	FIN_CRC32C(crc);
	marker->crc32c = (uint32)crc;
}

/*
 * B′ P0 fix: CRC32C over the WIRE image — header bytes [0, 68) (magic ..
 * _pad2, EXCLUDING the crc field itself at 68..71) PLUS the compact
 * incarnation table at [72, 72 + n_admitted*9).  The previous scheme
 * covered only the header (68 bytes on the wire), so a corrupted /
 * tampered incarnation table passed validation; and encode computed the
 * CRC on the HOST struct while decode checked the WIRE bytes — two
 * different byte streams.  Both sides now hash the identical wire
 * ranges.
 */
static uint32
formation_marker_wire_crc(const uint8 slot_bytes[CLUSTER_VOTING_SLOT_BYTES], int n_admitted)
{
	pg_crc32c crc;

	INIT_CRC32C(crc);
	COMP_CRC32C(crc, slot_bytes, 68);
	if (n_admitted > 0)
		COMP_CRC32C(crc, slot_bytes + CLUSTER_FORMATION_MARKER_HEADER_BYTES,
					n_admitted * CLUSTER_FORMATION_MARKER_ENTRY_BYTES);
	FIN_CRC32C(crc);
	return (uint32)crc;
}

/*
 * Encode a host-order marker into the 512-byte wire image.  Returns false
 * on structural violations (bad magic/version/phase, member set over the
 * capacity, self-inconsistent n_admitted).  incarnation_by_node must
 * provide a non-zero incarnation for every admitted member.
 */
bool
cluster_formation_marker_encode(const ClusterFormationCommitMarker *marker,
								const uint64 *incarnation_by_node,
								uint8 slot_bytes[CLUSTER_VOTING_SLOT_BYTES])
{
	int member;
	uint32 pop = 0;
	int off;

	if (marker == NULL || incarnation_by_node == NULL || slot_bytes == NULL
		|| marker->magic != CLUSTER_FORMATION_MARKER_MAGIC
		|| marker->version != CLUSTER_FORMATION_MARKER_VERSION
		|| marker->phase != CLUSTER_FORMATION_MARKER_PHASE_COMMITTED
		|| marker->formation_generation == 0 || marker->arbiter_node >= CLUSTER_MAX_NODES
		|| marker->arbiter_incarnation == 0 || marker->commit_nonce == 0)
		return false;

	/*
	 * B′ P0 fix (48/128 capacity mismatch): the compact table holds at
	 * most CLUSTER_FORMATION_MARKER_MAX_MEMBERS entries, but the
	 * admitted_nodes bitmap is 16 bytes (128 bits).  A member index at or
	 * beyond MAX_MEMBERS can never be encoded into the table — accept
	 * only [0, MAX_MEMBERS) and treat any higher bitmap bit as a
	 * structural violation (fail-closed).
	 */
	for (member = 0; member < CLUSTER_MAX_NODES; member++)
		if ((marker->admitted_nodes[member / 8] & (uint8)(1u << (member % 8))) != 0) {
			if (member >= CLUSTER_FORMATION_MARKER_MAX_MEMBERS)
				return false;
			pop++;
		}
	if (pop == 0 || pop != marker->n_admitted || pop > CLUSTER_FORMATION_MARKER_MAX_MEMBERS)
		return false;

	memset(slot_bytes, 0, CLUSTER_VOTING_SLOT_BYTES);
	write_u32_le(slot_bytes + 0, marker->magic);
	write_u16_le(slot_bytes + 4, marker->version);
	slot_bytes[6] = marker->phase;
	write_u64_le(slot_bytes + 8, marker->formation_generation);
	write_u64_le(slot_bytes + 16, marker->formation_epoch);
	write_u64_le(slot_bytes + 24, marker->arbiter_node);
	write_u64_le(slot_bytes + 32, marker->arbiter_incarnation);
	write_u64_le(slot_bytes + 40, marker->commit_nonce);
	memcpy(slot_bytes + 48, marker->admitted_nodes, CLUSTER_FORMATION_MARKER_BITMAP_BYTES);
	write_u16_le(slot_bytes + 64, marker->n_admitted);

	/* Compact member entries. */
	off = CLUSTER_FORMATION_MARKER_HEADER_BYTES;
	for (member = 0; member < CLUSTER_MAX_NODES; member++) {
		if ((marker->admitted_nodes[member / 8] & (uint8)(1u << (member % 8))) == 0)
			continue;
		if (incarnation_by_node[member] == 0)
			return false;
		slot_bytes[off++] = (uint8)member;
		write_u64_le(slot_bytes + off, incarnation_by_node[member]);
		off += 8;
	}
	/* B′ P0 fix: the wire CRC covers the header + the compact table —
	 * computed HERE, after the table is laid out, so the stored crc field
	 * matches the exact bytes decode will hash. */
	write_u32_le(slot_bytes + 68, formation_marker_wire_crc(slot_bytes, (int)marker->n_admitted));
	return true;
}

/*
 * Decode a wire image into host-order structs.  Returns false on any
 * structural violation (bad magic/version/phase/CRC, member set over
 * capacity).  incarnation_by_node (optional, CLUSTER_MAX_NODES entries) is
 * filled from the compact table (zero for non-members).
 */
bool
cluster_formation_marker_decode(const uint8 slot_bytes[CLUSTER_VOTING_SLOT_BYTES],
								ClusterFormationCommitMarker *marker, uint64 *incarnation_by_node)
{
	ClusterFormationCommitMarker decoded;
	uint32 expected_crc;
	uint32 pop = 0;
	int off;
	int i;

	if (slot_bytes == NULL || marker == NULL)
		return false;
	memset(&decoded, 0, sizeof(decoded));
	decoded.magic = read_u32_le(slot_bytes + 0);
	decoded.version = read_u16_le(slot_bytes + 4);
	decoded.phase = slot_bytes[6];
	decoded.formation_generation = read_u64_le(slot_bytes + 8);
	decoded.formation_epoch = read_u64_le(slot_bytes + 16);
	decoded.arbiter_node = read_u64_le(slot_bytes + 24);
	decoded.arbiter_incarnation = read_u64_le(slot_bytes + 32);
	decoded.commit_nonce = read_u64_le(slot_bytes + 40);
	memcpy(decoded.admitted_nodes, slot_bytes + 48, CLUSTER_FORMATION_MARKER_BITMAP_BYTES);
	decoded.n_admitted = read_u16_le(slot_bytes + 64);
	expected_crc = read_u32_le(slot_bytes + 68);

	if (decoded.magic != CLUSTER_FORMATION_MARKER_MAGIC
		|| decoded.version != CLUSTER_FORMATION_MARKER_VERSION
		|| decoded.phase != CLUSTER_FORMATION_MARKER_PHASE_COMMITTED
		|| decoded.formation_generation == 0 || decoded.arbiter_node >= CLUSTER_MAX_NODES
		|| decoded.arbiter_incarnation == 0 || decoded.commit_nonce == 0)
		return false;

	/*
	 * B′ P0 fix (out-of-bounds read): n_admitted is attacker/bit-rot
	 * controlled WIRE data; the CRC below hashes [72, 72 + n_admitted*9),
	 * which would read past the 512-byte slot for a corrupt huge count.
	 * Clamp BEFORE hashing — the count can never exceed the compact-table
	 * capacity.
	 */
	if (decoded.n_admitted == 0 || decoded.n_admitted > CLUSTER_FORMATION_MARKER_MAX_MEMBERS)
		return false;

	/*
	 * B′ P0 fix: verify n_admitted BEFORE hashing — the wire CRC covers
	 * the header [0,68) PLUS the compact table [72, 72+n*9), exactly the
	 * byte ranges encode hashed.  A corrupt n_admitted therefore fails
	 * the structural checks below anyway; a table tamper fails here.
	 */
	if (formation_marker_wire_crc(slot_bytes, (int)decoded.n_admitted) != expected_crc)
		return false;

	for (i = 0; i < CLUSTER_MAX_NODES; i++)
		if ((decoded.admitted_nodes[i / 8] & (uint8)(1u << (i % 8))) != 0) {
			/* B′ P0 fix: mirror of the encode-side 48/128 capacity
			 * clamp — a bitmap bit at or beyond MAX_MEMBERS cannot be
			 * represented in the compact table. */
			if (i >= CLUSTER_FORMATION_MARKER_MAX_MEMBERS)
				return false;
			pop++;
		}
	if (pop == 0 || pop != decoded.n_admitted || pop > CLUSTER_FORMATION_MARKER_MAX_MEMBERS)
		return false;

	if (incarnation_by_node != NULL)
		memset(incarnation_by_node, 0, CLUSTER_MAX_NODES * sizeof(uint64));
	off = CLUSTER_FORMATION_MARKER_HEADER_BYTES;
	for (i = 0; i < (int)decoded.n_admitted; i++) {
		int node_id = slot_bytes[off++];
		uint64 inc = read_u64_le(slot_bytes + off);

		off += 8;
		if (node_id < 0 || node_id >= CLUSTER_MAX_NODES
			|| (decoded.admitted_nodes[node_id / 8] & (uint8)(1u << (node_id % 8))) == 0
			|| inc == 0)
			return false;
		if (incarnation_by_node != NULL)
			incarnation_by_node[node_id] = inc;
	}
	*marker = decoded;
	return true;
}

uint64
cluster_formation_marker_incarnation_for(const uint64 *incarnation_by_node, int32 node_id)
{
	if (incarnation_by_node == NULL || node_id < 0 || node_id >= CLUSTER_MAX_NODES)
		return 0;
	return incarnation_by_node[node_id];
}

/*
 * Validate a raw slot image (used by the readback / majority checks).
 * On success decodes into out_decoded + incarnation table.
 */
bool
cluster_formation_marker_validate(const uint8 slot_bytes[CLUSTER_VOTING_SLOT_BYTES],
								  ClusterFormationCommitMarker *out_decoded,
								  uint64 *out_incarnations)
{
	return cluster_formation_marker_decode(slot_bytes, out_decoded, out_incarnations);
}
