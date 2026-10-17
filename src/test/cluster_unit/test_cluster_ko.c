/*-------------------------------------------------------------------------
 *
 * test_cluster_ko.c
 *	  Unit tests for the KO (object-reuse flush) pure layer (spec-5.7 D6, §3.5).
 *
 *	  Covers the KO resource-id encoder: per-relfilenode identity (no fork --
 *	  a DROP/TRUNCATE removes all forks of the relfilenode), the 0xF6 namespace
 *	  marker, and non-collision with the SQ (0xF0) / CF (0xF1) / HW (0xF2) /
 *	  DL (0xF3) / IR (0xF5) namespaces.  The cross-node apply-after-drop flush
 *	  barrier (KO_FLUSH fanout + ACK, §3.6) is exercised by the 2-node TAP
 *	  (t/297);  this file pins the pure encoding that routes a drop to its KO(X).
 *
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/test/cluster_unit/test_cluster_ko.c
 *
 * NOTES
 *	  This is a pgrac-original file.
 *	  Spec: spec-5.7-misc-enqueue-classes.md (D6, §3.5)
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "cluster/cluster_dl.h"
#include "cluster/cluster_hw.h"
#include "cluster/cluster_ir.h"
#include "cluster/cluster_ko.h"
#include "cluster/cluster_sequence.h"
#include "storage/relfilelocator.h"

#undef printf
#undef fprintf
#undef snprintf

#include "unit_test.h"

UT_DEFINE_GLOBALS();

void
ExceptionalCondition(const char *conditionName, const char *fileName, int lineNumber)
{
	printf("# Assert failed: %s at %s:%d\n", conditionName, fileName, lineNumber);
	abort();
}

/* ======================================================================
 * U1 -- KO resid encoding: per relfilenode, namespace 0xF6, no fork.
 * ====================================================================== */
UT_TEST(test_ko_resid_encode)
{
	RelFileLocator rloc;
	ClusterResId r;

	rloc.spcOid = 1663;
	rloc.dbOid = 5;
	rloc.relNumber = 16384;

	memset(&r, 0xEE, sizeof(r));
	cluster_ko_resid_encode(rloc, &r);

	UT_ASSERT_EQ(r.field1, 5);	   /* dbOid */
	UT_ASSERT_EQ(r.field2, 16384); /* relNumber (ABA defence -- relfilenode reuse) */
	UT_ASSERT_EQ(r.field3, 1663);  /* spcOid */
	UT_ASSERT_EQ(r.field4, 0);	   /* KO is per-relfilenode: covers all forks */
	UT_ASSERT_EQ(r.type, CLUSTER_KO_RESID_TYPE);
	UT_ASSERT_EQ(r.type, 0xF6);
	UT_ASSERT_EQ(r.lockmethodid, DEFAULT_LOCKMETHOD);
}

/* namespace 0xF6 is distinct from SQ / CF / HW / DL / IR */
UT_TEST(test_ko_resid_namespace_distinct)
{
	RelFileLocator rloc;
	ClusterResId r;

	rloc.spcOid = 1663;
	rloc.dbOid = 5;
	rloc.relNumber = 16384;
	cluster_ko_resid_encode(rloc, &r);

	UT_ASSERT_NE(r.type, CLUSTER_SQ_RESID_TYPE); /* != 0xF0 */
	UT_ASSERT_NE(r.type, CLUSTER_CF_RESID_TYPE); /* != 0xF1 */
	UT_ASSERT_NE(r.type, CLUSTER_HW_RESID_TYPE); /* != 0xF2 */
	UT_ASSERT_NE(r.type, CLUSTER_DL_RESID_TYPE); /* != 0xF3 */
	UT_ASSERT_NE(r.type, CLUSTER_IR_RESID_TYPE); /* != 0xF5 */
}

/* a KO resid (per-relfilenode) and a DL resid (per-relation) of the SAME
 * relation share the relfilenode triple but differ in type -- the drop's
 * flush barrier (KO) and a bulk-load lease (DL) are separate resources. */
UT_TEST(test_ko_vs_dl_resid_distinct)
{
	RelFileLocator rloc;
	ClusterResId ko, dl;

	rloc.spcOid = 1663;
	rloc.dbOid = 5;
	rloc.relNumber = 16384;
	cluster_ko_resid_encode(rloc, &ko);
	cluster_dl_resid_encode(rloc, &dl);

	UT_ASSERT_NE(ko.type, dl.type);		/* 0xF6 != 0xF3 */
	UT_ASSERT_EQ(ko.field1, dl.field1); /* same db */
	UT_ASSERT_EQ(ko.field2, dl.field2); /* same relNumber */
	UT_ASSERT_EQ(ko.field3, dl.field3); /* same spc */
}

/* two different relfilenodes get distinct KO resids (different relNumber):
 * relfilenode reuse (ABA) after a drop maps to a NEW resid, so a stale flush
 * for the old incarnation never aliases the new one. */
UT_TEST(test_ko_resid_per_relfilenode)
{
	RelFileLocator a, b;
	ClusterResId ra, rb;

	a.spcOid = b.spcOid = 1663;
	a.dbOid = b.dbOid = 5;
	a.relNumber = 16384;
	b.relNumber = 16385;
	cluster_ko_resid_encode(a, &ra);
	cluster_ko_resid_encode(b, &rb);

	UT_ASSERT_NE(ra.field2, rb.field2); /* distinct relNumber -> distinct resid */
}

static ClusterKoSharedMessageV2
shared_fixture(void)
{
	ClusterKoSharedMessageV2 m = { 0 };
	m.verb = CLUSTER_KO_SHARED_REQUEST;
	m.batch_id = UINT64CONST(0x0102030405060708);
	m.epoch = UINT64CONST(0x1112131415161718);
	m.origin_boot = UINT64CONST(0x2122232425262728);
	m.peer_boot = UINT64CONST(0x3132333435363738);
	m.key.system_identifier = UINT64CONST(0x4142434445464748);
	m.key.database_incarnation = UINT64CONST(0x5152535455565758);
	m.key.locator = (RelFileLocator){ 1663, 0, 16384 };
	m.origin_node = 3;
	m.peer_node = 7;
	m.members[0] = 0x88;
	m.members[15] = 0x80;
	for (int i = 0; i < 16; i++) {
		m.key.storage_uuid[i] = 0x21 + i;
		m.incarnation[i] = 0x41 + i;
	}
	for (int i = 0; i < 32; i++)
		m.member_digest[i] = 0x81 + i;
	return m;
}

UT_TEST(shared_wire_has_exact_bytes_and_unaligned_global_roundtrip)
{
	const uint8 golden[CLUSTER_KO_SHARED_V2_BYTES]
		= { 'P',  'K',	'O',  '2',	2,	  0,	160,  0,	1,	  0,	0,	  0,	0,	  0,
			0,	  0,	8,	  7,	6,	  5,	4,	  3,	2,	  1,	0x18, 0x17, 0x16, 0x15,
			0x14, 0x13, 0x12, 0x11, 0x28, 0x27, 0x26, 0x25, 0x24, 0x23, 0x22, 0x21, 0x38, 0x37,
			0x36, 0x35, 0x34, 0x33, 0x32, 0x31, 0x48, 0x47, 0x46, 0x45, 0x44, 0x43, 0x42, 0x41,
			0x58, 0x57, 0x56, 0x55, 0x54, 0x53, 0x52, 0x51, 0x21, 0x22, 0x23, 0x24, 0x25, 0x26,
			0x27, 0x28, 0x29, 0x2a, 0x2b, 0x2c, 0x2d, 0x2e, 0x2f, 0x30, 0x41, 0x42, 0x43, 0x44,
			0x45, 0x46, 0x47, 0x48, 0x49, 0x4a, 0x4b, 0x4c, 0x4d, 0x4e, 0x4f, 0x50, 0x7f, 6,
			0,	  0,	0,	  0,	0,	  0,	0,	  0x40, 0,	  0,	3,	  0,	7,	  0,
			0x88, 0,	0,	  0,	0,	  0,	0,	  0,	0,	  0,	0,	  0,	0,	  0,
			0,	  0x80, 0x81, 0x82, 0x83, 0x84, 0x85, 0x86, 0x87, 0x88, 0x89, 0x8a, 0x8b, 0x8c,
			0x8d, 0x8e, 0x8f, 0x90, 0x91, 0x92, 0x93, 0x94, 0x95, 0x96, 0x97, 0x98, 0x99, 0x9a,
			0x9b, 0x9c, 0x9d, 0x9e, 0x9f, 0xa0 };
	ClusterKoSharedMessageV2 m = shared_fixture(), out;
	uint8 bytes[CLUSTER_KO_SHARED_V2_BYTES + 2];
	memset(bytes, 0xee, sizeof(bytes));
	UT_ASSERT(cluster_ko_shared_encode_v2(&m, bytes + 1, sizeof(golden)));
	UT_ASSERT_EQ(memcmp(bytes + 1, golden, sizeof(golden)), 0);
	UT_ASSERT_EQ(bytes[0], 0xee);
	UT_ASSERT_EQ(bytes[sizeof(bytes) - 1], 0xee);
	UT_ASSERT(cluster_ko_shared_decode_v2(bytes + 1, sizeof(golden), &out));
	UT_ASSERT_EQ(out.key.locator.dbOid, 0);
	UT_ASSERT_EQ(out.peer_boot, m.peer_boot);
	UT_ASSERT_EQ(out.members[15], 0x80);
	memset(bytes, 0, sizeof(bytes));
	UT_ASSERT(cluster_ko_shared_encode_v2(&out, bytes, sizeof(golden)));
	UT_ASSERT_EQ(memcmp(bytes, golden, sizeof(golden)), 0);
}

UT_TEST(shared_encoder_rejects_missing_identity_without_changing_output)
{
	for (int fault = 0; fault < 23; fault++) {
		ClusterKoSharedMessageV2 m = shared_fixture();
		uint8 bytes[CLUSTER_KO_SHARED_V2_BYTES], before[CLUSTER_KO_SHARED_V2_BYTES];
		memset(bytes, 0xec, sizeof(bytes));
		memcpy(before, bytes, sizeof(bytes));
		switch (fault) {
		case 0:
			m.batch_id = 0;
			break;
		case 1:
			m.batch_id = UINT64_MAX;
			break;
		case 2:
			m.epoch = 0;
			break;
		case 3:
			m.epoch = UINT64_MAX;
			break;
		case 4:
			m.origin_boot = 0;
			break;
		case 5:
			m.origin_boot = UINT64_MAX;
			break;
		case 6:
			m.peer_boot = 0;
			break;
		case 7:
			m.peer_boot = UINT64_MAX;
			break;
		case 8:
			m.key.system_identifier = 0;
			break;
		case 9:
			m.key.database_incarnation = 0;
			break;
		case 10:
			memset(m.key.storage_uuid, 0, 16);
			break;
		case 11:
			memset(m.incarnation, 0, 16);
			break;
		case 12:
			m.key.locator.spcOid = 0;
			break;
		case 13:
			m.key.locator.relNumber = 0;
			break;
		case 14:
			m.origin_node = 128;
			break;
		case 15:
			m.peer_node = 128;
			break;
		case 16:
			m.origin_node = m.peer_node;
			break;
		case 17:
			m.members[0] &= ~8;
			break;
		case 18:
			m.members[0] &= ~128;
			break;
		case 19:
			memset(m.member_digest, 0, 32);
			break;
		case 20:
			m.verb = 0;
			break;
		case 21:
			m.status = CLUSTER_KO_SHARED_DONE;
			break;
		case 22:
			m.verb = CLUSTER_KO_SHARED_ACK;
			m.status = 0;
			break;
		}
		UT_ASSERT(!cluster_ko_shared_encode_v2(&m, bytes, sizeof(bytes)));
		UT_ASSERT_EQ(memcmp(bytes, before, sizeof(bytes)), 0);
	}
}

UT_TEST(shared_decoder_refuses_old_short_extra_and_reserved_bytes)
{
	ClusterKoSharedMessageV2 m = shared_fixture(), out, before;
	uint8 bytes[CLUSTER_KO_SHARED_V2_BYTES + 1];
	const unsigned offsets[] = { 0, 1, 2, 3, 4, 5, 6, 7, 9, 10, 11, 12, 13, 14, 15, 109, 111 };
	memset(&out, 0xce, sizeof(out));
	memcpy(&before, &out, sizeof(out));
	UT_ASSERT(cluster_ko_shared_encode_v2(&m, bytes, CLUSTER_KO_SHARED_V2_BYTES));
	for (size_t len = 0; len < sizeof(bytes) + 1; len++) {
		if (len == CLUSTER_KO_SHARED_V2_BYTES)
			continue;
		UT_ASSERT(!cluster_ko_shared_decode_v2(bytes, len, &out));
		UT_ASSERT_EQ(memcmp(&out, &before, sizeof(out)), 0);
	}
	for (size_t i = 0; i < lengthof(offsets); i++) {
		unsigned at = offsets[i];
		bytes[at] ^= 0x80;
		UT_ASSERT(!cluster_ko_shared_decode_v2(bytes, CLUSTER_KO_SHARED_V2_BYTES, &out));
		UT_ASSERT_EQ(memcmp(&out, &before, sizeof(out)), 0);
		bytes[at] ^= 0x80;
	}
}

UT_TEST(shared_decoder_refuses_zero_required_wire_identity)
{
	const unsigned offsets[] = { 16, 24, 32, 40, 48, 56, 64, 80, 96, 104, 112, 128 };
	const unsigned widths[] = { 8, 8, 8, 8, 8, 8, 16, 16, 4, 4, 16, 32 };
	for (size_t i = 0; i < lengthof(offsets); i++) {
		ClusterKoSharedMessageV2 m = shared_fixture(), out, before;
		uint8 bytes[CLUSTER_KO_SHARED_V2_BYTES];
		memset(&out, 0xce, sizeof(out));
		memcpy(&before, &out, sizeof(out));
		UT_ASSERT(cluster_ko_shared_encode_v2(&m, bytes, sizeof(bytes)));
		memset(bytes + offsets[i], 0, widths[i]);
		UT_ASSERT(!cluster_ko_shared_decode_v2(bytes, sizeof(bytes), &out));
		UT_ASSERT_EQ(memcmp(&out, &before, sizeof(out)), 0);
	}
}

UT_TEST(shared_ack_requires_done_and_every_original_cut_byte)
{
	ClusterKoSharedMessageV2 request = shared_fixture(), ack = request, decoded;
	uint8 bytes[CLUSTER_KO_SHARED_V2_BYTES];
	ack.verb = CLUSTER_KO_SHARED_ACK;
	ack.status = CLUSTER_KO_SHARED_DONE;
	UT_ASSERT(cluster_ko_shared_ack_matches_v2(&request, &ack));
	UT_ASSERT(cluster_ko_shared_encode_v2(&ack, bytes, sizeof(bytes)));
	for (size_t i = 0; i < sizeof(bytes); i++) {
		bytes[i] ^= 1;
		if (cluster_ko_shared_decode_v2(bytes, sizeof(bytes), &decoded))
			UT_ASSERT(!cluster_ko_shared_ack_matches_v2(&request, &decoded));
		bytes[i] ^= 1;
	}
	ack.status = CLUSTER_KO_SHARED_FAILED;
	UT_ASSERT(cluster_ko_shared_encode_v2(&ack, bytes, sizeof(bytes)));
	UT_ASSERT(cluster_ko_shared_decode_v2(bytes, sizeof(bytes), &decoded));
	UT_ASSERT(!cluster_ko_shared_ack_matches_v2(&request, &ack));
	UT_ASSERT(!cluster_ko_shared_ack_matches_v2(&request, &request));
	UT_ASSERT(!cluster_ko_shared_ack_matches_v2(&ack, &request));
}

UT_TEST(shared_codecs_refuse_alias_and_null_without_output_mutation)
{
	union {
		ClusterKoSharedMessageV2 value;
		uint8 bytes[512];
	} u, before;
	ClusterKoSharedMessageV2 m = shared_fixture();

	memset(&u, 0xed, sizeof(u));
	u.value = m;
	memcpy(&before, &u, sizeof(u));
	UT_ASSERT(!cluster_ko_shared_encode_v2(&u.value, u.bytes, CLUSTER_KO_SHARED_V2_BYTES));
	UT_ASSERT_EQ(memcmp(&u, &before, sizeof(u)), 0);
	UT_ASSERT(cluster_ko_shared_encode_v2(&m, u.bytes, CLUSTER_KO_SHARED_V2_BYTES));
	memcpy(&before, &u, sizeof(u));
	UT_ASSERT(!cluster_ko_shared_decode_v2(u.bytes, CLUSTER_KO_SHARED_V2_BYTES, &u.value));
	UT_ASSERT_EQ(memcmp(&u, &before, sizeof(u)), 0);
	UT_ASSERT(!cluster_ko_shared_encode_v2(NULL, u.bytes, CLUSTER_KO_SHARED_V2_BYTES));
	UT_ASSERT(!cluster_ko_shared_encode_v2(&m, NULL, CLUSTER_KO_SHARED_V2_BYTES));
	UT_ASSERT(!cluster_ko_shared_decode_v2(NULL, CLUSTER_KO_SHARED_V2_BYTES, &m));
	UT_ASSERT(!cluster_ko_shared_decode_v2(u.bytes, CLUSTER_KO_SHARED_V2_BYTES, NULL));
	UT_ASSERT(!cluster_ko_shared_ack_matches_v2(NULL, &m));
	UT_ASSERT(!cluster_ko_shared_ack_matches_v2(&m, NULL));
}

UT_TEST(shared_wire_accepts_highest_node_without_truncating_bitmap)
{
	ClusterKoSharedMessageV2 m = shared_fixture(), out;
	uint8 bytes[CLUSTER_KO_SHARED_V2_BYTES];
	m.peer_node = 127;
	UT_ASSERT(cluster_ko_shared_encode_v2(&m, bytes, sizeof(bytes)));
	UT_ASSERT(cluster_ko_shared_decode_v2(bytes, sizeof(bytes), &out));
	UT_ASSERT_EQ(out.peer_node, 127);
	UT_ASSERT_EQ(out.members[15], 0x80);
	UT_ASSERT_EQ(sizeof(KoFlushHeader), 32);
	UT_ASSERT_EQ(sizeof(KoFlushAckHeader), 24);
}

static ClusterKoSharedMessageV3
shared_v3_fixture(void)
{
	ClusterKoSharedMessageV3 m = { 0 };

	m.base = shared_fixture();
	m.base.key.locator.dbOid = 5;
	m.ax_resid.field1 = 5;
	m.ax_resid.field2 = 4242; /* relation OID, not the relfilenode */
	m.ax_resid.field3 = 0;
	m.ax_resid.field4 = 0;
	m.ax_resid.type = LOCKTAG_RELATION;
	m.ax_resid.lockmethodid = DEFAULT_LOCKMETHOD;
	m.ax_holder.node_id = m.base.origin_node;
	m.ax_holder.procno = 77;
	m.ax_holder.cluster_epoch = m.base.epoch;
	m.ax_holder.request_id = UINT64CONST(0x7172737475767778);
	m.ax_request_id = m.ax_holder.request_id;
	m.ax_master_generation = UINT64CONST(0x8182838485868788);
	m.ax_master_node = m.base.origin_node;
	m.ax_mode = AccessExclusiveLock;
	m.operation = CLUSTER_KO_SHARED_V3_OPERATION_DROP;
	m.full_fork_mask = CLUSTER_KO_SHARED_V3_FULL_FORK_MASK;
	m.space_mutation_token = UINT64CONST(0x9192939495969798);
	return m;
}

UT_TEST(shared_v3_wire_binds_ax_and_rejects_v2_or_reserved)
{
	ClusterKoSharedMessageV3 request = shared_v3_fixture(), ack = request, out, before;
	uint8 bytes[CLUSTER_KO_SHARED_V3_BYTES], legacy[CLUSTER_KO_SHARED_V2_BYTES];

	memset(bytes, 0xee, sizeof(bytes));
	UT_ASSERT(cluster_ko_shared_encode_v3(&request, bytes, sizeof(bytes)));
	UT_ASSERT_EQ(memcmp(bytes, "PKO3", 4), 0);
	UT_ASSERT_EQ(bytes[4], CLUSTER_KO_SHARED_V3_VERSION);
	UT_ASSERT_EQ(bytes[6], CLUSTER_KO_SHARED_V3_BYTES);
	UT_ASSERT_EQ(bytes[7], 0);
	UT_ASSERT_EQ(bytes[160], 5);
	UT_ASSERT_EQ(bytes[164], 0x92); /* relation OID 4242, little endian */
	UT_ASSERT_EQ(bytes[220], (uint8)AccessExclusiveLock);
	UT_ASSERT(cluster_ko_shared_decode_v3(bytes, sizeof(bytes), &out));
	UT_ASSERT_EQ(out.ax_resid.field2, 4242);
	UT_ASSERT_EQ(out.ax_holder.procno, 77);
	UT_ASSERT_EQ(out.full_fork_mask, CLUSTER_KO_SHARED_V3_FULL_FORK_MASK);
	UT_ASSERT_EQ(out.space_mutation_token, request.space_mutation_token);

	before = out;
	bytes[12] = 1; /* reserved in the inherited v2 prefix */
	UT_ASSERT(!cluster_ko_shared_decode_v3(bytes, sizeof(bytes), &out));
	UT_ASSERT_EQ(memcmp(&out, &before, sizeof(out)), 0);
	bytes[12] = 0;
	UT_ASSERT(cluster_ko_shared_encode_v2(&request.base, legacy, sizeof(legacy)));
	UT_ASSERT(!cluster_ko_shared_decode_v3(legacy, sizeof(legacy), &out));

	ack.base.verb = CLUSTER_KO_SHARED_ACK;
	ack.base.status = CLUSTER_KO_SHARED_DONE;
	UT_ASSERT(cluster_ko_shared_ack_matches_v3(&request, &ack));
	ack.space_mutation_token++;
	UT_ASSERT(!cluster_ko_shared_ack_matches_v3(&request, &ack));
}

UT_TEST(shared_v3_codec_allows_remote_ges_holder_identity)
{
	ClusterKoSharedMessageV3 request = shared_v3_fixture(), out;
	uint8 bytes[CLUSTER_KO_SHARED_V3_BYTES];

	/* Codec validation carries the holder identity and membership proof. The
	 * GES holder may be a peer; the producer binds local origin ownership in
	 * ko_shared_prepare_request_v3 before this wire is admitted. */
	request.ax_holder.node_id = request.base.peer_node;
	request.ax_master_node = request.base.peer_node;
	UT_ASSERT(cluster_ko_shared_encode_v3(&request, bytes, sizeof(bytes)));
	UT_ASSERT(cluster_ko_shared_decode_v3(bytes, sizeof(bytes), &out));
	UT_ASSERT_EQ(out.ax_holder.node_id, request.base.peer_node);
	UT_ASSERT_EQ(out.ax_master_node, request.base.peer_node);

	/* The relation/formation suffix is exact, even though the holder may be
	 * remote. Cross-database, cross-epoch and non-member identities are not
	 * alternate GES shapes; they are stale or misrouted and must fail closed. */
	request.ax_resid.field1++;
	UT_ASSERT(!cluster_ko_shared_encode_v3(&request, bytes, sizeof(bytes)));
	request.ax_resid.field1--;
	request.ax_holder.cluster_epoch++;
	UT_ASSERT(!cluster_ko_shared_encode_v3(&request, bytes, sizeof(bytes)));
	request.ax_holder.cluster_epoch--;
	request.base.members[request.ax_holder.node_id / 8] &=
		(uint8) ~(1u << (request.ax_holder.node_id % 8));
	UT_ASSERT(!cluster_ko_shared_encode_v3(&request, bytes, sizeof(bytes)));
}

UT_TEST(shared_v3_failure_matches_identity_but_never_done)
{
	ClusterKoSharedMessageV3 request = shared_v3_fixture(), result = request;
	result.base.verb = CLUSTER_KO_SHARED_ACK;
	result.base.status = CLUSTER_KO_SHARED_FAILED;
	UT_ASSERT(cluster_ko_shared_result_matches_v3(&request, &result));
	UT_ASSERT(!cluster_ko_shared_ack_matches_v3(&request, &result));
	result.space_mutation_token++;
	UT_ASSERT(!cluster_ko_shared_result_matches_v3(&request, &result));
	result = request;
	UT_ASSERT(!cluster_ko_shared_result_matches_v3(&request, &result));
}

static ClusterKoSharedMessageV4
shared_v4_fixture(void)
{
	ClusterKoSharedMessageV4 m = { 0 };
	m.object = shared_v3_fixture();
	return m;
}

UT_TEST(shared_v4_roundtrip_has_its_own_contract_and_full_identity)
{
	const uint16 operations[] = { CLUSTER_KO_SHARED_V3_OPERATION_DROP,
		CLUSTER_KO_SHARED_V3_OPERATION_REWRITE_OLD };
	for (size_t op = 0; op < lengthof(operations); op++) {
		for (int result = 0; result < 3; result++) {
			ClusterKoSharedMessageV4 m = shared_v4_fixture(), out;
			ClusterKoSharedMessageV3 legacy_out;
			uint8 bytes[CLUSTER_KO_SHARED_V4_BYTES + 1], old[CLUSTER_KO_SHARED_V3_BYTES];
			m.object.operation = operations[op];
			if (result != 0) {
				m.object.base.verb = CLUSTER_KO_SHARED_ACK;
				m.object.base.status = result == 1 ? CLUSTER_KO_SHARED_DONE : CLUSTER_KO_SHARED_FAILED;
			}
			/* A holder/master may be remote; physical permission is not a codec fact. */
			m.object.ax_holder.node_id = m.object.base.peer_node;
			m.object.ax_master_node = m.object.base.peer_node;
			UT_ASSERT(cluster_ko_shared_encode_v4(&m, bytes + 1, CLUSTER_KO_SHARED_V4_BYTES));
			UT_ASSERT_EQ(memcmp(bytes + 1, "PKO4", 4), 0);
			UT_ASSERT_EQ(bytes[5], 4);
			UT_ASSERT_EQ(bytes[6], 0);
			UT_ASSERT_EQ(bytes[7], 240);
			UT_ASSERT_EQ(bytes[8], 0);
			UT_ASSERT(cluster_ko_shared_decode_v4(bytes + 1, CLUSTER_KO_SHARED_V4_BYTES, &out));
			UT_ASSERT_EQ(memcmp(&m, &out, sizeof(m)), 0);
			UT_ASSERT(cluster_ko_shared_encode_v3(&m.object, old, sizeof(old)));
			UT_ASSERT_EQ(memcmp(bytes + 9, old + 8, sizeof(old) - 8), 0);
			UT_ASSERT(!cluster_ko_shared_decode_v3(bytes + 1, CLUSTER_KO_SHARED_V4_BYTES,
				&legacy_out));
		}
	}
}

UT_TEST(shared_v4_decoder_rejects_old_unknown_truncated_and_reserved_frames)
{
	ClusterKoSharedMessageV4 m = shared_v4_fixture(), out, before;
	uint8 bytes[CLUSTER_KO_SHARED_V4_BYTES + 1], legacy[CLUSTER_KO_SHARED_V3_BYTES];
	const unsigned reserved[] = { 0, 1, 2, 3, 4, 5, 6, 7, 12, 13, 14, 15,
		228, 229, 230, 231 };
	memset(&out, 0xce, sizeof(out));
	before = out;
	UT_ASSERT(cluster_ko_shared_encode_v3(&m.object, legacy, sizeof(legacy)));
	UT_ASSERT(!cluster_ko_shared_decode_v4(legacy, sizeof(legacy), &out));
	UT_ASSERT_EQ(memcmp(&out, &before, sizeof(out)), 0);
	UT_ASSERT(cluster_ko_shared_encode_v2(&m.object.base, legacy, CLUSTER_KO_SHARED_V2_BYTES));
	UT_ASSERT(!cluster_ko_shared_decode_v4(legacy, CLUSTER_KO_SHARED_V2_BYTES, &out));
	UT_ASSERT(cluster_ko_shared_encode_v4(&m, bytes, CLUSTER_KO_SHARED_V4_BYTES));
	for (size_t len = 0; len <= sizeof(bytes); len++) {
		if (len == CLUSTER_KO_SHARED_V4_BYTES)
			continue;
		UT_ASSERT(!cluster_ko_shared_decode_v4(bytes, len, &out));
		UT_ASSERT_EQ(memcmp(&out, &before, sizeof(out)), 0);
	}
	for (size_t i = 0; i < lengthof(reserved); i++) {
		bytes[reserved[i]] ^= 0x80;
		UT_ASSERT(!cluster_ko_shared_decode_v4(bytes, CLUSTER_KO_SHARED_V4_BYTES, &out));
		UT_ASSERT_EQ(memcmp(&out, &before, sizeof(out)), 0);
		bytes[reserved[i]] ^= 0x80;
	}
}

UT_TEST(shared_v4_rejects_inplace_truncate_and_inexact_object_identity)
{
	for (int which = 0; which < 12; which++) {
		ClusterKoSharedMessageV4 m = shared_v4_fixture(), out, before;
		uint8 bytes[CLUSTER_KO_SHARED_V4_BYTES], saved[sizeof(bytes)];
		memset(bytes, 0xed, sizeof(bytes));
		memcpy(saved, bytes, sizeof(bytes));
		switch (which) {
		case 0: m.object.operation = CLUSTER_KO_SHARED_V3_OPERATION_TRUNCATE; break;
		case 1: m.object.operation = 0; break;
		case 2: m.object.operation = UINT16_MAX; break;
		case 3: m.object.ax_mode = ShareLock; break;
		case 4: m.object.ax_request_id++; break;
		case 5: m.object.ax_holder.cluster_epoch++; break;
		case 6: m.object.ax_resid.field1++; break;
		case 7: m.object.ax_master_generation = 0; break;
		case 8: m.object.full_fork_mask ^= 1; break;
		case 9: m.object.space_mutation_token = 0; break;
		case 10: m.object.base.origin_boot = 0; break;
		case 11: memset(m.object.base.incarnation, 0, 16); break;
		}
		UT_ASSERT(!cluster_ko_shared_encode_v4(&m, bytes, sizeof(bytes)));
		UT_ASSERT_EQ(memcmp(bytes, saved, sizeof(bytes)), 0);
		/* The old in-place contract is valid v3; changing only its wire
		 * envelope must not make it a committed old-incarnation discard. */
		if (which == 0) {
			UT_ASSERT(cluster_ko_shared_encode_v3(&m.object, bytes, sizeof(bytes)));
			memcpy(bytes, "PKO4", 4);
			bytes[4] = 4;
			memset(&out, 0xce, sizeof(out));
			before = out;
			UT_ASSERT(!cluster_ko_shared_decode_v4(bytes, sizeof(bytes), &out));
			UT_ASSERT_EQ(memcmp(&out, &before, sizeof(out)), 0);
		}
	}
}

UT_TEST(shared_v4_result_requires_every_original_byte_and_separates_failed)
{
	ClusterKoSharedMessageV4 request = shared_v4_fixture(), ack = request, changed;
	uint8 bytes[CLUSTER_KO_SHARED_V4_BYTES];
	ack.object.base.verb = CLUSTER_KO_SHARED_ACK;
	ack.object.base.status = CLUSTER_KO_SHARED_DONE;
	UT_ASSERT(cluster_ko_shared_result_matches_v4(&request, &ack));
	UT_ASSERT(cluster_ko_shared_ack_matches_v4(&request, &ack));
	UT_ASSERT(cluster_ko_shared_encode_v4(&ack, bytes, sizeof(bytes)));
	for (size_t i = 0; i < sizeof(bytes); i++) {
		bytes[i] ^= 1;
		if (cluster_ko_shared_decode_v4(bytes, sizeof(bytes), &changed))
			UT_ASSERT(!cluster_ko_shared_result_matches_v4(&request, &changed));
		bytes[i] ^= 1;
	}
	ack.object.base.status = CLUSTER_KO_SHARED_FAILED;
	UT_ASSERT(cluster_ko_shared_result_matches_v4(&request, &ack));
	UT_ASSERT(!cluster_ko_shared_ack_matches_v4(&request, &ack));
	ack.object.space_mutation_token++;
	UT_ASSERT(!cluster_ko_shared_result_matches_v4(&request, &ack));
	UT_ASSERT(!cluster_ko_shared_result_matches_v4(&request, &request));
	request.object.operation = ack.object.operation = CLUSTER_KO_SHARED_V3_OPERATION_TRUNCATE;
	ack.object.space_mutation_token--;
	UT_ASSERT(!cluster_ko_shared_result_matches_v4(&request, &ack));
}

UT_TEST(shared_v4_refuses_alias_null_and_wrong_output_length)
{
	union {
		ClusterKoSharedMessageV4 value;
		uint8 bytes[512];
	} storage, before;
	ClusterKoSharedMessageV4 request = shared_v4_fixture();
	memset(&storage, 0xed, sizeof(storage));
	storage.value = request;
	before = storage;
	UT_ASSERT(!cluster_ko_shared_encode_v4(&storage.value, storage.bytes + 1, 240));
	UT_ASSERT_EQ(memcmp(&storage, &before, sizeof(storage)), 0);
	UT_ASSERT(!cluster_ko_shared_encode_v4(&request, storage.bytes, 239));
	UT_ASSERT(!cluster_ko_shared_encode_v4(&request, storage.bytes, 241));
	UT_ASSERT_EQ(memcmp(&storage, &before, sizeof(storage)), 0);
	UT_ASSERT(cluster_ko_shared_encode_v4(&request, storage.bytes + 1, 240));
	before = storage;
	UT_ASSERT(!cluster_ko_shared_decode_v4(storage.bytes + 1, 240, &storage.value));
	UT_ASSERT_EQ(memcmp(&storage, &before, sizeof(storage)), 0);
	UT_ASSERT(!cluster_ko_shared_encode_v4(NULL, storage.bytes, 240));
	UT_ASSERT(!cluster_ko_shared_encode_v4(&request, NULL, 240));
	UT_ASSERT(!cluster_ko_shared_decode_v4(NULL, 240, &request));
	UT_ASSERT(!cluster_ko_shared_decode_v4(storage.bytes, 240, NULL));
	UT_ASSERT(!cluster_ko_shared_result_matches_v4(NULL, &request));
	UT_ASSERT(!cluster_ko_shared_ack_matches_v4(&request, NULL));
}

int
main(void)
{
	UT_PLAN(19);
	UT_RUN(test_ko_resid_encode);
	UT_RUN(test_ko_resid_namespace_distinct);
	UT_RUN(test_ko_vs_dl_resid_distinct);
	UT_RUN(test_ko_resid_per_relfilenode);
	UT_RUN(shared_wire_has_exact_bytes_and_unaligned_global_roundtrip);
	UT_RUN(shared_encoder_rejects_missing_identity_without_changing_output);
	UT_RUN(shared_decoder_refuses_old_short_extra_and_reserved_bytes);
	UT_RUN(shared_decoder_refuses_zero_required_wire_identity);
	UT_RUN(shared_ack_requires_done_and_every_original_cut_byte);
	UT_RUN(shared_codecs_refuse_alias_and_null_without_output_mutation);
	UT_RUN(shared_wire_accepts_highest_node_without_truncating_bitmap);
	UT_RUN(shared_v3_wire_binds_ax_and_rejects_v2_or_reserved);
	UT_RUN(shared_v3_codec_allows_remote_ges_holder_identity);
	UT_RUN(shared_v3_failure_matches_identity_but_never_done);
	UT_RUN(shared_v4_roundtrip_has_its_own_contract_and_full_identity);
	UT_RUN(shared_v4_decoder_rejects_old_unknown_truncated_and_reserved_frames);
	UT_RUN(shared_v4_rejects_inplace_truncate_and_inexact_object_identity);
	UT_RUN(shared_v4_result_requires_every_original_byte_and_separates_failed);
	UT_RUN(shared_v4_refuses_alias_null_and_wrong_output_length);
	UT_DONE();

	return ut_failed_count == 0 ? 0 : 1;
}
