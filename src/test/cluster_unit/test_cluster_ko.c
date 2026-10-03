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
	ClusterKoSharedMessageV2 m = {0};
	m.verb = CLUSTER_KO_SHARED_REQUEST;
	m.batch_id = UINT64CONST(0x0102030405060708);
	m.epoch = UINT64CONST(0x1112131415161718);
	m.origin_boot = UINT64CONST(0x2122232425262728);
	m.peer_boot = UINT64CONST(0x3132333435363738);
	m.key.system_identifier = UINT64CONST(0x4142434445464748);
	m.key.database_incarnation = UINT64CONST(0x5152535455565758);
	m.key.locator = (RelFileLocator){1663, 0, 16384};
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
	const uint8 golden[CLUSTER_KO_SHARED_V2_BYTES] = {
		'P','K','O','2', 2,0,160,0, 1,0,0,0, 0,0,0,0,
		8,7,6,5,4,3,2,1, 0x18,0x17,0x16,0x15,0x14,0x13,0x12,0x11,
		0x28,0x27,0x26,0x25,0x24,0x23,0x22,0x21,
		0x38,0x37,0x36,0x35,0x34,0x33,0x32,0x31,
		0x48,0x47,0x46,0x45,0x44,0x43,0x42,0x41,
		0x58,0x57,0x56,0x55,0x54,0x53,0x52,0x51,
		0x21,0x22,0x23,0x24,0x25,0x26,0x27,0x28,
		0x29,0x2a,0x2b,0x2c,0x2d,0x2e,0x2f,0x30,
		0x41,0x42,0x43,0x44,0x45,0x46,0x47,0x48,
		0x49,0x4a,0x4b,0x4c,0x4d,0x4e,0x4f,0x50,
		0x7f,6,0,0, 0,0,0,0, 0,0x40,0,0, 3,0,7,0,
		0x88,0,0,0,0,0,0,0, 0,0,0,0,0,0,0,0x80,
		0x81,0x82,0x83,0x84,0x85,0x86,0x87,0x88,
		0x89,0x8a,0x8b,0x8c,0x8d,0x8e,0x8f,0x90,
		0x91,0x92,0x93,0x94,0x95,0x96,0x97,0x98,
		0x99,0x9a,0x9b,0x9c,0x9d,0x9e,0x9f,0xa0
	};
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
		case 0: m.batch_id = 0; break;
		case 1: m.batch_id = UINT64_MAX; break;
		case 2: m.epoch = 0; break;
		case 3: m.epoch = UINT64_MAX; break;
		case 4: m.origin_boot = 0; break;
		case 5: m.origin_boot = UINT64_MAX; break;
		case 6: m.peer_boot = 0; break;
		case 7: m.peer_boot = UINT64_MAX; break;
		case 8: m.key.system_identifier = 0; break;
		case 9: m.key.database_incarnation = 0; break;
		case 10: memset(m.key.storage_uuid, 0, 16); break;
		case 11: memset(m.incarnation, 0, 16); break;
		case 12: m.key.locator.spcOid = 0; break;
		case 13: m.key.locator.relNumber = 0; break;
		case 14: m.origin_node = 128; break;
		case 15: m.peer_node = 128; break;
		case 16: m.origin_node = m.peer_node; break;
		case 17: m.members[0] &= ~8; break;
		case 18: m.members[0] &= ~128; break;
		case 19: memset(m.member_digest, 0, 32); break;
		case 20: m.verb = 0; break;
		case 21: m.status = CLUSTER_KO_SHARED_DONE; break;
		case 22: m.verb = CLUSTER_KO_SHARED_ACK; m.status = 0; break;
		}
		UT_ASSERT(!cluster_ko_shared_encode_v2(&m, bytes, sizeof(bytes)));
		UT_ASSERT_EQ(memcmp(bytes, before, sizeof(bytes)), 0);
	}
}

UT_TEST(shared_decoder_refuses_old_short_extra_and_reserved_bytes)
{
	ClusterKoSharedMessageV2 m = shared_fixture(), out, before;
	uint8 bytes[CLUSTER_KO_SHARED_V2_BYTES + 1];
	const unsigned offsets[] = {0,1,2,3,4,5,6,7,9,10,11,12,13,14,15,109,111};
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
	const unsigned offsets[] = {16,24,32,40,48,56,64,80,96,104,112,128};
	const unsigned widths[] = {8,8,8,8,8,8,16,16,4,4,16,32};
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
	union { ClusterKoSharedMessageV2 value; uint8 bytes[512]; } u, before;
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

int
main(void)
{
	UT_PLAN(11);
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
	UT_DONE();

	return ut_failed_count == 0 ? 0 : 1;
}
