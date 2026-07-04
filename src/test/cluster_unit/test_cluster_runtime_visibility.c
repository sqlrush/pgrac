/*-------------------------------------------------------------------------
 *
 * test_cluster_runtime_visibility.c
 *	  Standalone unit tests for the spec-6.12i live-authority gate
 *	  (cluster_vis_live_authority_covers_policy): the D-i2 window predicate
 *	  that admits by-xid resolution of a recycled remote ITL slot only when
 *	  the co-sampled authority provably covers the tuple's page version.
 *
 *	  Every "doubt" branch must return false so the caller keeps the
 *	  pre-existing 53R97 fail-closed boundary (规则 8.A: this wave widens
 *	  "resolve when provable", never widens "resolve when unprovable").
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/test/cluster_unit/test_cluster_runtime_visibility.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "cluster/cluster_gcs_block.h" /* undo-fetch tag + auth trailer (CP2) */
#include "cluster/cluster_runtime_visibility.h"

#include "unit_test.h"

UT_DEFINE_GLOBALS();

/* Assert hook stub so the cassert libpgport links standalone. */
void
ExceptionalCondition(const char *conditionName pg_attribute_unused(),
					 const char *fileName pg_attribute_unused(),
					 int lineNumber pg_attribute_unused())
{
	abort();
}

#define LOCAL_EPOCH 7

static ClusterLiveAuthority
mk_auth(uint64 epoch, XLogRecPtr hwm, uint64 gen)
{
	ClusterLiveAuthority a;

	a.origin_epoch = epoch;
	a.live_hwm_lsn = hwm;
	a.tt_generation = gen;
	return a;
}

/* All three admit conditions hold -> resolve is permitted (the ONLY true). */
UT_TEST(test_covers_when_epoch_match_and_hwm_ge_anchor)
{
	ClusterLiveAuthority a = mk_auth(LOCAL_EPOCH, 5000, 42);

	/* hwm (5000) >= anchor (5000): boundary-equal covers. */
	UT_ASSERT_EQ(cluster_vis_live_authority_covers_policy(5000, a, LOCAL_EPOCH), true);
	/* hwm (5000) > anchor (4096): covers. */
	UT_ASSERT_EQ(cluster_vis_live_authority_covers_policy(4096, a, LOCAL_EPOCH), true);
}

/* Authority from a different reconfig generation -> fail closed. */
UT_TEST(test_failclosed_when_epoch_differs)
{
	ClusterLiveAuthority older = mk_auth(LOCAL_EPOCH - 1, 9000, 42);
	ClusterLiveAuthority newer = mk_auth(LOCAL_EPOCH + 1, 9000, 42);

	UT_ASSERT_EQ(cluster_vis_live_authority_covers_policy(1, older, LOCAL_EPOCH), false);
	UT_ASSERT_EQ(cluster_vis_live_authority_covers_policy(1, newer, LOCAL_EPOCH), false);
}

/* No authority sampled (invalid hwm) -> fail closed, never guess. */
UT_TEST(test_failclosed_when_hwm_invalid)
{
	ClusterLiveAuthority a = mk_auth(LOCAL_EPOCH, InvalidXLogRecPtr, 42);

	/* Even with anchor 0, an absent authority must not admit. */
	UT_ASSERT_EQ(cluster_vis_live_authority_covers_policy(0, a, LOCAL_EPOCH), false);
}

/* Origin durable TT does not yet cover this page version -> fail closed. */
UT_TEST(test_failclosed_when_hwm_below_anchor)
{
	ClusterLiveAuthority a = mk_auth(LOCAL_EPOCH, 4095, 42);

	/* hwm (4095) < anchor (4096): under-covered window. */
	UT_ASSERT_EQ(cluster_vis_live_authority_covers_policy(4096, a, LOCAL_EPOCH), false);
}

/*
 * Combined-doubt: epoch mismatch takes precedence even when the hwm would
 * otherwise cover -- proves conditions are ANDed, not ORed.
 */
UT_TEST(test_failclosed_epoch_mismatch_dominates_good_hwm)
{
	ClusterLiveAuthority a = mk_auth(LOCAL_EPOCH + 3, 0xFFFFFFFF, 42);

	UT_ASSERT_EQ(cluster_vis_live_authority_covers_policy(1, a, LOCAL_EPOCH), false);
}

/*
 * CP2: origin_epoch is a FULL-WIDTH uint64 equality — an epoch differing
 * only above bit 32 must fail (guards the uint32 sketch upgrade: a
 * truncated comparison would alias these two and false-admit).
 */
UT_TEST(test_failclosed_epoch_differs_above_32bit)
{
	uint64 wide_epoch = ((uint64)1 << 32) + LOCAL_EPOCH;
	ClusterLiveAuthority a = mk_auth(wide_epoch, 9000, 42);

	UT_ASSERT_EQ(cluster_vis_live_authority_covers_policy(1, a, LOCAL_EPOCH), false);
	/* Same wide epoch on both sides still admits (sanity). */
	UT_ASSERT_EQ(cluster_vis_live_authority_covers_policy(1, a, wide_epoch), true);
}

/* CP2: authority trailer little-endian carrier roundtrip (wire ABI). */
UT_TEST(test_undo_auth_trailer_roundtrip)
{
	ClusterGcsUndoAuthTrailer t;
	static const uint64 cases[]
		= { 0, 1, 0x0123456789ABCDEFULL, 0xFFFFFFFFFFFFFFFFULL, ((uint64)1 << 32) };

	memset(&t, 0, sizeof(t));
	for (size_t i = 0; i < sizeof(cases) / sizeof(cases[0]); i++) {
		ClusterGcsUndoAuthTrailerSetTtGeneration(&t, cases[i]);
		UT_ASSERT_EQ(ClusterGcsUndoAuthTrailerGetTtGeneration(&t), cases[i]);
	}
	/* The setter must not touch the reserved (must-be-zero) tail. */
	for (size_t i = 0; i < sizeof(t.reserved_0); i++)
		UT_ASSERT_EQ(t.reserved_0[i], 0);
}

/* CP2: synthetic undo-address tag roundtrip + magic discrimination. */
UT_TEST(test_undo_fetch_tag_roundtrip)
{
	BufferTag tag = GcsBlockUndoFetchTagMake(7, 0);
	BufferTag real_tag;
	uint32 seg = 0;
	uint32 blk = 99;

	UT_ASSERT_EQ(GcsBlockUndoFetchTagDecode(tag, &seg, &blk), true);
	UT_ASSERT_EQ(seg, 7);
	UT_ASSERT_EQ(blk, 0);

	/* A real-relation-looking tag must NOT decode as an undo address. */
	memset(&real_tag, 0, sizeof(real_tag));
	real_tag.spcOid = (Oid)1663; /* pg_default */
	real_tag.dbOid = (Oid)5;
	real_tag.relNumber = (RelFileNumber)16384;
	real_tag.forkNum = MAIN_FORKNUM;
	real_tag.blockNum = 0;
	UT_ASSERT_EQ(GcsBlockUndoFetchTagDecode(real_tag, &seg, &blk), false);
}

int
main(void)
{
	UT_PLAN(8);
	UT_RUN(test_covers_when_epoch_match_and_hwm_ge_anchor);
	UT_RUN(test_failclosed_when_epoch_differs);
	UT_RUN(test_failclosed_when_hwm_invalid);
	UT_RUN(test_failclosed_when_hwm_below_anchor);
	UT_RUN(test_failclosed_epoch_mismatch_dominates_good_hwm);
	UT_RUN(test_failclosed_epoch_differs_above_32bit);
	UT_RUN(test_undo_auth_trailer_roundtrip);
	UT_RUN(test_undo_fetch_tag_roundtrip);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
