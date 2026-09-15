/*-------------------------------------------------------------------------
 *
 * cluster_hw_snapshot_codec.c
 *    Shared frontend/backend codec for the existing HW snapshot v1 bytes.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 2026, pgrac contributors
 *
 *-------------------------------------------------------------------------
 */
#ifdef FRONTEND
#include "postgres_fe.h"
#else
#include "postgres.h"
#endif

#include "common/cluster_hw_snapshot_codec.h"
#include "port/pg_crc32c.h"


/*
 * cluster_hw_snapshot_codec_size -- header (48) + 20*n entries + crc (4).
 */
size_t
cluster_hw_snapshot_codec_size(uint32 n_entries)
{
	return (size_t)CLUSTER_HW_SNAPSHOT_HEADER_SIZE
		   + (size_t)n_entries * CLUSTER_HW_SNAPSHOT_ENTRY_SIZE + CLUSTER_HW_SNAPSHOT_CRC_SIZE;
}

/*
 * cluster_hw_snapshot_codec_serialize -- lay the header, then the n_entries 20-byte
 * (resid, next_hwm) pairs, then a trailing CRC32C over everything before it.
 *
 *	The header struct and the entry struct have no padding (StaticAssert'd
 *	48 / 20), so a fully-initialised input serialises to a deterministic byte
 *	image -- the CRC cannot depend on uninitialised padding.
 */
size_t
cluster_hw_snapshot_codec_serialize(const ClusterHwSnapshotHeader *hdr, const void *entries,
									char *buf, size_t buflen)
{
	size_t required;
	size_t crc_offset;
	pg_crc32c crc;

	Assert(hdr != NULL && buf != NULL);
	Assert(hdr->n_entries == 0 || entries != NULL);

	required = cluster_hw_snapshot_codec_size(hdr->n_entries);
	if (buflen < required)
		return 0;

	memcpy(buf, hdr, CLUSTER_HW_SNAPSHOT_HEADER_SIZE);
	if (hdr->n_entries > 0)
		memcpy(buf + CLUSTER_HW_SNAPSHOT_HEADER_SIZE, entries,
			   (size_t)hdr->n_entries * CLUSTER_HW_SNAPSHOT_ENTRY_SIZE);

	crc_offset = required - CLUSTER_HW_SNAPSHOT_CRC_SIZE;
	INIT_CRC32C(crc);
	COMP_CRC32C(crc, buf, crc_offset);
	FIN_CRC32C(crc);
	memcpy(buf + crc_offset, &crc, CLUSTER_HW_SNAPSHOT_CRC_SIZE);

	return required;
}

/*
 * cluster_hw_snapshot_codec_deserialize -- validate then load.
 *
 *	Validation order is structural before cryptographic: a buffer too short to
 *	hold even an empty envelope, or a wrong magic/version, is rejected before
 *	any CRC work (a foreign byte order shows up as a wrong magic).  A declared
 *	entry count larger than the caller's capacity fails closed (R6) rather than
 *	silently truncating the authority.  Only once the structure checks out is
 *	the CRC recomputed and compared.
 */
ClusterHwSnapshotValidity
cluster_hw_snapshot_codec_deserialize(const char *buf, size_t len, ClusterHwSnapshotHeader *hdr_out,
									  void *entries_out, uint32 max_entries)
{
	ClusterHwSnapshotHeader hdr;
	size_t required;
	size_t crc_offset;
	pg_crc32c crc;
	pg_crc32c stored;

	Assert(hdr_out != NULL);

	/* Need at least an empty envelope: header + trailing CRC. */
	if (buf == NULL || len < (size_t)CLUSTER_HW_SNAPSHOT_HEADER_SIZE + CLUSTER_HW_SNAPSHOT_CRC_SIZE)
		return CLUSTER_HW_SNAPSHOT_INVALID_SHORT;

	memcpy(&hdr, buf, CLUSTER_HW_SNAPSHOT_HEADER_SIZE);

	if (hdr.magic != CLUSTER_HW_SNAPSHOT_MAGIC || hdr.version != CLUSTER_HW_SNAPSHOT_VERSION)
		return CLUSTER_HW_SNAPSHOT_INVALID_MAGIC;

	/* Fail closed if the caller cannot hold the declared entries (R6). */
	if (hdr.n_entries > max_entries)
		return CLUSTER_HW_SNAPSHOT_INVALID_SHORT;

	required = cluster_hw_snapshot_codec_size(hdr.n_entries);
	if (len < required)
		return CLUSTER_HW_SNAPSHOT_INVALID_SHORT;

	crc_offset = required - CLUSTER_HW_SNAPSHOT_CRC_SIZE;
	INIT_CRC32C(crc);
	COMP_CRC32C(crc, buf, crc_offset);
	FIN_CRC32C(crc);
	memcpy(&stored, buf + crc_offset, CLUSTER_HW_SNAPSHOT_CRC_SIZE);
	if (!EQ_CRC32C(crc, stored))
		return CLUSTER_HW_SNAPSHOT_INVALID_CRC;

	*hdr_out = hdr;
	if (hdr.n_entries > 0 && entries_out != NULL)
		memcpy(entries_out, buf + CLUSTER_HW_SNAPSHOT_HEADER_SIZE,
			   (size_t)hdr.n_entries * CLUSTER_HW_SNAPSHOT_ENTRY_SIZE);

	return CLUSTER_HW_SNAPSHOT_VALID;
}
