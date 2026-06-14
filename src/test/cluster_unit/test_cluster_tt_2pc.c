/*-------------------------------------------------------------------------
 *
 * test_cluster_tt_2pc.c
 *	  cluster_unit tests for the spec-3.15 2PC record serialize/parse
 *	  layer (pure functions; no backend linked beyond the record TU).
 *
 *	      S1  round-trip: single binding
 *	      S2  round-trip: multi binding + sub-links (field-exact)
 *	      S3  empty payload round-trip (header-only record)
 *	      S4  cap rejection: bindings > MAX -> serialize returns 0
 *	      S5  cap rejection: sublinks > MAX -> serialize returns 0
 *	      S6  dstcap too small -> serialize returns 0
 *	      S7  CRC corruption (flip one payload byte) -> parse false
 *	      S8  version mismatch -> parse false
 *	      S9  magic mismatch -> parse false
 *	      S10 length mismatch (truncated / padded) -> parse false
 *	      S11 count fields tampered (crc re-stamped) -> length check trips
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/test/cluster_unit/test_cluster_tt_2pc.c
 *
 * NOTES
 *	  This is a pgrac-original file.
 *	  Spec: spec-3.15-2pc-prepared-visibility.md (FROZEN v0.2) §2.1/§4.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "cluster/cluster_tt_2pc.h"

/* Drop PG's port.h printf -> pg_printf override; unit_test.h uses
 * stdlib printf (libpgport is linked for CRC32C only). */
#undef printf

#include "unit_test.h"

UT_DEFINE_GLOBALS();


static ClusterTT2PCBinding
mk_binding(uint32 seg, uint16 off, uint16 wrap, uint32 epoch, TransactionId xid)
{
	ClusterTT2PCBinding b;

	memset(&b, 0, sizeof(b));
	b.undo_segment_id = seg;
	b.slot_offset = off;
	b.wrap = wrap;
	b.cluster_epoch = epoch;
	b.xid = xid;
	return b;
}

static ClusterTT2PCSubLink
mk_link(uint32 cxid, uint32 pxid)
{
	ClusterTT2PCSubLink l;

	memset(&l, 0, sizeof(l));
	l.child_key.origin_node_id = 1;
	l.child_key.undo_segment_id = 7;
	l.child_key.tt_slot_id = 3;
	l.child_key.cluster_epoch = 42;
	l.child_key.local_xid = cxid;
	l.parent_key = l.child_key;
	l.parent_key.local_xid = pxid;
	return l;
}


UT_TEST(test_s1_roundtrip_single_binding)
{
	ClusterTT2PCBinding b = mk_binding(11, 5, 2, 9, 1001);
	char buf[256];
	uint32 len;
	ClusterTT2PCParsed p;

	len = cluster_tt_2pc_serialize(&b, NULL, 1, NULL, 0, buf, sizeof(buf));
	UT_ASSERT_EQ((int)(len == cluster_tt_2pc_record_size(CLUSTER_TT_2PC_VERSION, 1, 0)), 1);
	UT_ASSERT_EQ((int)cluster_tt_2pc_parse_record(buf, len, &p), 1);
	UT_ASSERT_EQ((int)p.nbindings, 1);
	UT_ASSERT_EQ((int)p.nsublinks, 0);
	UT_ASSERT_EQ((int)p.bindings[0].undo_segment_id, 11);
	UT_ASSERT_EQ((int)p.bindings[0].slot_offset, 5);
	UT_ASSERT_EQ((int)p.bindings[0].wrap, 2);
	UT_ASSERT_EQ((int)p.bindings[0].cluster_epoch, 9);
	UT_ASSERT_EQ((int)p.bindings[0].xid, 1001);
}

UT_TEST(test_s2_roundtrip_multi_with_sublinks)
{
	ClusterTT2PCBinding bs[3];
	ClusterTT2PCSubLink ls[2];
	char buf[1024];
	uint32 len;
	ClusterTT2PCParsed p;
	int i;

	for (i = 0; i < 3; i++)
		bs[i] = mk_binding(100 + i, (uint16)i, (uint16)(i * 7), 5, 2000 + i);
	ls[0] = mk_link(3001, 2000);
	ls[1] = mk_link(3002, 2000);

	len = cluster_tt_2pc_serialize(bs, NULL, 3, ls, 2, buf, sizeof(buf));
	UT_ASSERT_EQ((int)(len > 0), 1);
	UT_ASSERT_EQ((int)cluster_tt_2pc_parse_record(buf, len, &p), 1);
	UT_ASSERT_EQ((int)p.nbindings, 3);
	UT_ASSERT_EQ((int)p.nsublinks, 2);
	for (i = 0; i < 3; i++) {
		UT_ASSERT_EQ((int)p.bindings[i].undo_segment_id, 100 + i);
		UT_ASSERT_EQ((int)p.bindings[i].xid, 2000 + i);
	}
	UT_ASSERT_EQ((int)p.sublinks[0].child_key.local_xid, 3001);
	UT_ASSERT_EQ((int)p.sublinks[1].child_key.local_xid, 3002);
	UT_ASSERT_EQ((int)p.sublinks[1].parent_key.local_xid, 2000);
}

UT_TEST(test_s3_roundtrip_empty)
{
	char buf[64];
	uint32 len;
	ClusterTT2PCParsed p;

	len = cluster_tt_2pc_serialize(NULL, NULL, 0, NULL, 0, buf, sizeof(buf));
	UT_ASSERT_EQ((int)(len == sizeof(ClusterTT2PCRecord)), 1);
	UT_ASSERT_EQ((int)cluster_tt_2pc_parse_record(buf, len, &p), 1);
	UT_ASSERT_EQ((int)p.nbindings, 0);
	UT_ASSERT_EQ((int)p.nsublinks, 0);
	UT_ASSERT_EQ((int)(p.bindings == NULL), 1);
	UT_ASSERT_EQ((int)(p.sublinks == NULL), 1);
}

UT_TEST(test_s4_cap_bindings_reject)
{
	ClusterTT2PCBinding bs[CLUSTER_TT_2PC_MAX_BINDINGS + 1];
	char buf[4096];
	int i;

	for (i = 0; i <= CLUSTER_TT_2PC_MAX_BINDINGS; i++)
		bs[i] = mk_binding(1, 0, 0, 1, 100 + i);
	UT_ASSERT_EQ((int)cluster_tt_2pc_serialize(bs, NULL, CLUSTER_TT_2PC_MAX_BINDINGS + 1, NULL, 0,
											   buf, sizeof(buf)),
				 0);
}

UT_TEST(test_s5_cap_sublinks_reject)
{
	ClusterTT2PCSubLink ls[CLUSTER_TT_2PC_MAX_SUBLINKS + 1];
	char buf[8192];
	int i;

	for (i = 0; i <= CLUSTER_TT_2PC_MAX_SUBLINKS; i++)
		ls[i] = mk_link(100 + i, 50);
	UT_ASSERT_EQ((int)cluster_tt_2pc_serialize(NULL, NULL, 0, ls, CLUSTER_TT_2PC_MAX_SUBLINKS + 1,
											   buf, sizeof(buf)),
				 0);
}

UT_TEST(test_s6_dstcap_too_small_reject)
{
	ClusterTT2PCBinding b = mk_binding(1, 0, 0, 1, 100);
	char buf[8];

	UT_ASSERT_EQ((int)cluster_tt_2pc_serialize(&b, NULL, 1, NULL, 0, buf, sizeof(buf)), 0);
}

UT_TEST(test_s7_crc_corruption_reject)
{
	ClusterTT2PCBinding b = mk_binding(11, 5, 2, 9, 1001);
	char buf[256];
	uint32 len;
	ClusterTT2PCParsed p;

	len = cluster_tt_2pc_serialize(&b, NULL, 1, NULL, 0, buf, sizeof(buf));
	buf[sizeof(ClusterTT2PCRecord) + 3] ^= 0x40; /* flip a payload bit */
	UT_ASSERT_EQ((int)cluster_tt_2pc_parse_record(buf, len, &p), 0);
}

UT_TEST(test_s8_version_mismatch_reject)
{
	ClusterTT2PCBinding b = mk_binding(11, 5, 2, 9, 1001);
	char buf[256];
	uint32 len;
	ClusterTT2PCParsed p;
	ClusterTT2PCRecord *hdr = (ClusterTT2PCRecord *)buf;

	len = cluster_tt_2pc_serialize(&b, NULL, 1, NULL, 0, buf, sizeof(buf));
	hdr->version = CLUSTER_TT_2PC_VERSION + 1;
	UT_ASSERT_EQ((int)cluster_tt_2pc_parse_record(buf, len, &p), 0);
}

UT_TEST(test_s9_magic_mismatch_reject)
{
	ClusterTT2PCBinding b = mk_binding(11, 5, 2, 9, 1001);
	char buf[256];
	uint32 len;
	ClusterTT2PCParsed p;
	ClusterTT2PCRecord *hdr = (ClusterTT2PCRecord *)buf;

	len = cluster_tt_2pc_serialize(&b, NULL, 1, NULL, 0, buf, sizeof(buf));
	hdr->magic = 0xDEADBEEF;
	UT_ASSERT_EQ((int)cluster_tt_2pc_parse_record(buf, len, &p), 0);
}

UT_TEST(test_s10_length_mismatch_reject)
{
	ClusterTT2PCBinding b = mk_binding(11, 5, 2, 9, 1001);
	char buf[256];
	uint32 len;
	ClusterTT2PCParsed p;

	len = cluster_tt_2pc_serialize(&b, NULL, 1, NULL, 0, buf, sizeof(buf));
	UT_ASSERT_EQ((int)cluster_tt_2pc_parse_record(buf, len - 1, &p), 0); /* truncated */
	UT_ASSERT_EQ((int)cluster_tt_2pc_parse_record(buf, len + 1, &p), 0); /* padded */
	UT_ASSERT_EQ((int)cluster_tt_2pc_parse_record(buf, sizeof(ClusterTT2PCRecord) - 1, &p), 0);
}

UT_TEST(test_s11_count_tamper_trips_length_check)
{
	ClusterTT2PCBinding b = mk_binding(11, 5, 2, 9, 1001);
	char buf[256];
	uint32 len;
	ClusterTT2PCParsed p;
	ClusterTT2PCRecord *hdr = (ClusterTT2PCRecord *)buf;

	len = cluster_tt_2pc_serialize(&b, NULL, 1, NULL, 0, buf, sizeof(buf));
	/* Claim 2 bindings: even with a freshly-stamped CRC the length
	 * arithmetic must trip (defence-in-depth ordering). */
	hdr->nbindings = 2;
	UT_ASSERT_EQ((int)cluster_tt_2pc_parse_record(buf, len, &p), 0);
}

/* spec-4.8 D7-A: v2 heads[] section round-trips parallel to bindings[]. */
UT_TEST(test_s12_v2_heads_roundtrip)
{
	ClusterTT2PCBinding bs[2];
	UBA heads[2];
	char buf[512];
	uint32 len;
	ClusterTT2PCParsed p;

	bs[0] = mk_binding(11, 5, 2, 9, 1001);
	bs[1] = mk_binding(12, 6, 3, 9, 1002);
	memset(heads, 0, sizeof(heads));
	heads[0].raw[0] = 0xABCD1234;
	heads[0].raw[1] = 0x5678;
	heads[1].raw[0] = 0x0; /* binding 1: no head (InvalidUba) */
	heads[1].raw[1] = 0x0;

	len = cluster_tt_2pc_serialize(bs, heads, 2, NULL, 0, buf, sizeof(buf));
	UT_ASSERT_EQ((int)(len == cluster_tt_2pc_record_size(CLUSTER_TT_2PC_VERSION, 2, 0)), 1);
	UT_ASSERT_EQ((int)cluster_tt_2pc_parse_record(buf, len, &p), 1);
	UT_ASSERT_EQ((int)p.version, CLUSTER_TT_2PC_VERSION);
	UT_ASSERT_EQ((int)(p.heads != NULL), 1);
	UT_ASSERT_EQ((int)(p.heads[0].raw[0] == 0xABCD1234), 1);
	UT_ASSERT_EQ((int)(p.heads[0].raw[1] == 0x5678), 1);
	UT_ASSERT_EQ((int)UBA_is_invalid(p.heads[1]), 1); /* binding 1: InvalidUba */
}

/* spec-4.8 D7-A: NULL heads serializes an all-InvalidUba heads[] section; the
 * record is still v2 (heads != NULL on parse) -> D7 no-op per binding. */
UT_TEST(test_s13_v2_null_heads_all_invalid)
{
	ClusterTT2PCBinding b = mk_binding(11, 5, 2, 9, 1001);
	char buf[256];
	uint32 len;
	ClusterTT2PCParsed p;

	len = cluster_tt_2pc_serialize(&b, NULL, 1, NULL, 0, buf, sizeof(buf));
	UT_ASSERT_EQ((int)cluster_tt_2pc_parse_record(buf, len, &p), 1);
	UT_ASSERT_EQ((int)p.version, CLUSTER_TT_2PC_VERSION);
	UT_ASSERT_EQ((int)(p.heads != NULL), 1);
	UT_ASSERT_EQ((int)UBA_is_invalid(p.heads[0]), 1);
}


int
main(void)
{
	UT_RUN(test_s1_roundtrip_single_binding);
	UT_RUN(test_s2_roundtrip_multi_with_sublinks);
	UT_RUN(test_s3_roundtrip_empty);
	UT_RUN(test_s4_cap_bindings_reject);
	UT_RUN(test_s5_cap_sublinks_reject);
	UT_RUN(test_s6_dstcap_too_small_reject);
	UT_RUN(test_s7_crc_corruption_reject);
	UT_RUN(test_s8_version_mismatch_reject);
	UT_RUN(test_s9_magic_mismatch_reject);
	UT_RUN(test_s10_length_mismatch_reject);
	UT_RUN(test_s11_count_tamper_trips_length_check);
	UT_RUN(test_s12_v2_heads_roundtrip);
	UT_RUN(test_s13_v2_null_heads_all_invalid);

	UT_DONE();
	return ut_failed_count != 0 ? 1 : 0;
}
