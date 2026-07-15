/*-------------------------------------------------------------------------
 *
 * test_cluster_undo_horizon.c
 *	  Unit tests for the cluster-wide undo retention horizon pure fold
 *	  (spec-5.22e D5-1).
 *
 *	  Truth table U1-U16: the fold returns OK with the scn_time_cmp-min
 *	  floor only when EVERY required MEMBER peer has a stable, capable,
 *	  current-epoch, well-formed, monotone, fresh report; every unproven
 *	  edge STALLS with first-fail attribution (reason + blame node) and
 *	  never falls back to the local horizon (rule 8.A / Q3'').
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/test/cluster_unit/test_cluster_undo_horizon.c
 *
 * NOTES
 *	  This is a pgrac-original file.
 *	  Spec: spec-5.22e-undo-cluster-retention-horizon.md (D5-1, §4.1)
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "cluster/cluster_undo_horizon.h"

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

/*
 * scn_time_cmp -- byte-faithful local stub (mirrors test_cluster_cr_tuple.c):
 * the fold compares peer bounds vs the local horizon through it.  Linking
 * cluster_scn.o would drag in shmem/atomics; the header-only scn_local()
 * inline matches the real impl.
 */
int
scn_time_cmp(SCN a, SCN b)
{
	uint64 la = scn_local(a);
	uint64 lb = scn_local(b);

	if (la < lb)
		return -1;
	if (la > lb)
		return 1;
	return 0;
}

/* ---- helpers ------------------------------------------------------- */

#define TEST_EPOCH ((uint64)7)
#define TEST_NOW_US ((uint64)90 * 1000 * 1000) /* 90s, roomy */
#define TEST_LOCAL_IV ((uint32)1000)		   /* 1s tick */
#define TEST_SELF 0

/* fresh under window = 3 * max(1000,1000)ms = 3s */
#define FRESH_RECV_US (TEST_NOW_US - (uint64)1 * 1000 * 1000)

static void
bm_set(uint8 *bm, int node)
{
	bm[node >> 3] |= (uint8)(1u << (node & 7));
}

/* a fully healthy report view */
static ClusterUndoHorizonReportView
mk_view(uint64 scn)
{
	ClusterUndoHorizonReportView v;

	memset(&v, 0, sizeof(v));
	v.valid = true;
	v.stable = true;
	v.has_capability = true;
	v.regression_flagged = false;
	v.epoch = TEST_EPOCH;
	v.horizon_scn = (SCN)scn;
	v.recv_at_us = FRESH_RECV_US;
	v.sender_interval_ms = 1000;
	return v;
}

typedef struct FoldResult {
	ClusterUndoHorizonFoldStatus st;
	ClusterUndoHorizonFloor floor;
	ClusterUndoHorizonStallReason reason;
	int32 blame;
} FoldResult;

static FoldResult
run_fold(SCN local, const ClusterUndoHorizonReportView *views, int nviews, const uint8 *required)
{
	FoldResult r;

	memset(&r, 0, sizeof(r));
	r.floor.scn = (SCN)0xdeadbeef;
	r.floor.epoch = 0xdead;
	r.reason = CLUSTER_UNDO_HORIZON_STALL_NONE;
	r.blame = -2;
	r.st = cluster_undo_horizon_cluster_floor(local, views, nviews, required, TEST_SELF, TEST_EPOCH,
											  TEST_NOW_US, TEST_LOCAL_IV, &r.floor, &r.reason,
											  &r.blame);
	return r;
}

/* ---- U1: empty required set => OK, floor = local ------------------- */
UT_TEST(test_u1_no_required_peer)
{
	uint8 req[CLUSTER_RECONFIG_DEAD_BITMAP_BYTES] = { 0 };
	FoldResult r = run_fold((SCN)500, NULL, 0, req);

	UT_ASSERT_EQ(r.st, CLUSTER_UNDO_HORIZON_FOLD_OK);
	UT_ASSERT_EQ((uint64)r.floor.scn, (uint64)500);
	UT_ASSERT_EQ(r.floor.epoch, TEST_EPOCH);
	UT_ASSERT_EQ(r.reason, CLUSTER_UNDO_HORIZON_STALL_NONE);
}

/* ---- U2: all required fresh + epoch match => min fold -------------- */
UT_TEST(test_u2_all_fresh_min)
{
	uint8 req[CLUSTER_RECONFIG_DEAD_BITMAP_BYTES] = { 0 };
	ClusterUndoHorizonReportView views[3];
	FoldResult r;

	views[0] = mk_view(0); /* self slot: ignored */
	views[1] = mk_view(700);
	views[2] = mk_view(900);
	bm_set(req, 1);
	bm_set(req, 2);

	r = run_fold((SCN)800, views, 3, req);
	UT_ASSERT_EQ(r.st, CLUSTER_UNDO_HORIZON_FOLD_OK);
	UT_ASSERT_EQ((uint64)r.floor.scn, (uint64)700);
	UT_ASSERT_EQ(r.floor.epoch, TEST_EPOCH);
}

/* ---- U3: required peer never reported => MISSING + blame ----------- */
UT_TEST(test_u3_missing)
{
	uint8 req[CLUSTER_RECONFIG_DEAD_BITMAP_BYTES] = { 0 };
	ClusterUndoHorizonReportView views[2];
	FoldResult r;

	views[0] = mk_view(0);
	views[1] = mk_view(700);
	views[1].valid = false;
	bm_set(req, 1);

	r = run_fold((SCN)800, views, 2, req);
	UT_ASSERT_EQ(r.st, CLUSTER_UNDO_HORIZON_FOLD_STALLED);
	UT_ASSERT_EQ(r.reason, CLUSTER_UNDO_HORIZON_STALL_MISSING);
	UT_ASSERT_EQ(r.blame, 1);
}

/* ---- U4: report older than 3 x max(sender, local) => STALE --------- */
UT_TEST(test_u4_stale)
{
	uint8 req[CLUSTER_RECONFIG_DEAD_BITMAP_BYTES] = { 0 };
	ClusterUndoHorizonReportView views[2];
	FoldResult r;

	views[0] = mk_view(0);
	views[1] = mk_view(700);
	/* window = 3s; age it 3.5s */
	views[1].recv_at_us = TEST_NOW_US - (uint64)3500 * 1000;
	bm_set(req, 1);

	r = run_fold((SCN)800, views, 2, req);
	UT_ASSERT_EQ(r.st, CLUSTER_UNDO_HORIZON_FOLD_STALLED);
	UT_ASSERT_EQ(r.reason, CLUSTER_UNDO_HORIZON_STALL_STALE);
	UT_ASSERT_EQ(r.blame, 1);
}

/* ---- U5: report epoch != current => EPOCH --------------------------- */
UT_TEST(test_u5_epoch_mismatch)
{
	uint8 req[CLUSTER_RECONFIG_DEAD_BITMAP_BYTES] = { 0 };
	ClusterUndoHorizonReportView views[2];
	FoldResult r;

	views[0] = mk_view(0);
	views[1] = mk_view(700);
	views[1].epoch = TEST_EPOCH - 1;
	bm_set(req, 1);

	r = run_fold((SCN)800, views, 2, req);
	UT_ASSERT_EQ(r.st, CLUSTER_UNDO_HORIZON_FOLD_STALLED);
	UT_ASSERT_EQ(r.reason, CLUSTER_UNDO_HORIZON_STALL_EPOCH);
	UT_ASSERT_EQ(r.blame, 1);
}

/* ---- U6: peer horizon below local => floor follows the peer -------- */
UT_TEST(test_u6_peer_below_local)
{
	uint8 req[CLUSTER_RECONFIG_DEAD_BITMAP_BYTES] = { 0 };
	ClusterUndoHorizonReportView views[2];
	FoldResult r;

	views[0] = mk_view(0);
	views[1] = mk_view(100);
	bm_set(req, 1);

	r = run_fold((SCN)800, views, 2, req);
	UT_ASSERT_EQ(r.st, CLUSTER_UNDO_HORIZON_FOLD_OK);
	UT_ASSERT_EQ((uint64)r.floor.scn, (uint64)100);
}

/* ---- U7: InvalidScn report => MALFORMED ----------------------------- */
UT_TEST(test_u7_invalid_scn)
{
	uint8 req[CLUSTER_RECONFIG_DEAD_BITMAP_BYTES] = { 0 };
	ClusterUndoHorizonReportView views[2];
	FoldResult r;

	views[0] = mk_view(0);
	views[1] = mk_view(0);
	views[1].horizon_scn = InvalidScn;
	bm_set(req, 1);

	r = run_fold((SCN)800, views, 2, req);
	UT_ASSERT_EQ(r.st, CLUSTER_UNDO_HORIZON_FOLD_STALLED);
	UT_ASSERT_EQ(r.reason, CLUSTER_UNDO_HORIZON_STALL_MALFORMED);
	UT_ASSERT_EQ(r.blame, 1);
}

/* ---- U7b: invalid LOCAL horizon => MALFORMED, blame self ------------ */
UT_TEST(test_u7b_invalid_local)
{
	uint8 req[CLUSTER_RECONFIG_DEAD_BITMAP_BYTES] = { 0 };
	ClusterUndoHorizonReportView views[2];
	FoldResult r;

	views[0] = mk_view(0);
	views[1] = mk_view(700);
	bm_set(req, 1);

	r = run_fold(InvalidScn, views, 2, req);
	UT_ASSERT_EQ(r.st, CLUSTER_UNDO_HORIZON_FOLD_STALLED);
	UT_ASSERT_EQ(r.reason, CLUSTER_UNDO_HORIZON_STALL_MALFORMED);
	UT_ASSERT_EQ(r.blame, TEST_SELF);
}

/* ---- U8: ghost view for a non-required node is ignored -------------- */
UT_TEST(test_u8_ghost_ignored)
{
	uint8 req[CLUSTER_RECONFIG_DEAD_BITMAP_BYTES] = { 0 };
	ClusterUndoHorizonReportView views[3];
	FoldResult r;

	views[0] = mk_view(0);
	views[1] = mk_view(700);
	views[2] = mk_view(50);			 /* ghost: NOT required; lower than all */
	views[2].epoch = TEST_EPOCH - 3; /* and stale-epoch: must not matter */
	bm_set(req, 1);

	r = run_fold((SCN)800, views, 3, req);
	UT_ASSERT_EQ(r.st, CLUSTER_UNDO_HORIZON_FOLD_OK);
	UT_ASSERT_EQ((uint64)r.floor.scn, (uint64)700);
}

/* ---- U9: DEAD/REMOVED peer dropped from required => not consulted --- */
UT_TEST(test_u9_dead_removed_dropped)
{
	uint8 req[CLUSTER_RECONFIG_DEAD_BITMAP_BYTES] = { 0 };
	ClusterUndoHorizonReportView views[3];
	FoldResult r;

	views[0] = mk_view(0);
	views[1] = mk_view(700);
	views[2] = mk_view(0);
	views[2].valid = false; /* node2 died; caller removed it from
								 * required -- its missing report must
								 * not stall the fold */
	bm_set(req, 1);

	r = run_fold((SCN)800, views, 3, req);
	UT_ASSERT_EQ(r.st, CLUSTER_UNDO_HORIZON_FOLD_OK);
	UT_ASSERT_EQ((uint64)r.floor.scn, (uint64)700);
}

/* ---- U10: recv_at in the future => conservative STALE --------------- */
UT_TEST(test_u10_future_recv)
{
	uint8 req[CLUSTER_RECONFIG_DEAD_BITMAP_BYTES] = { 0 };
	ClusterUndoHorizonReportView views[2];
	FoldResult r;

	views[0] = mk_view(0);
	views[1] = mk_view(700);
	views[1].recv_at_us = TEST_NOW_US + 1;
	bm_set(req, 1);

	r = run_fold((SCN)800, views, 2, req);
	UT_ASSERT_EQ(r.st, CLUSTER_UNDO_HORIZON_FOLD_STALLED);
	UT_ASSERT_EQ(r.reason, CLUSTER_UNDO_HORIZON_STALL_STALE);
	UT_ASSERT_EQ(r.blame, 1);
}

/* ---- U11: multi-node declared but required empty (cold formation) --- */
UT_TEST(test_u11_cold_formation)
{
	uint8 req[CLUSTER_RECONFIG_DEAD_BITMAP_BYTES] = { 0 };
	ClusterUndoHorizonReportView views[4];
	FoldResult r;
	int i;

	/* declared 4 nodes, none MEMBER yet: caller passes an empty required
	 * set and whatever sits in the slots must be ignored */
	for (i = 0; i < 4; i++) {
		views[i] = mk_view(1);
		views[i].valid = false;
	}
	r = run_fold((SCN)800, views, 4, req);
	UT_ASSERT_EQ(r.st, CLUSTER_UNDO_HORIZON_FOLD_OK);
	UT_ASSERT_EQ((uint64)r.floor.scn, (uint64)800);
}

/* ---- U12: seqlock torn read => TORN --------------------------------- */
UT_TEST(test_u12_torn)
{
	uint8 req[CLUSTER_RECONFIG_DEAD_BITMAP_BYTES] = { 0 };
	ClusterUndoHorizonReportView views[2];
	FoldResult r;

	views[0] = mk_view(0);
	views[1] = mk_view(700);
	views[1].stable = false;
	bm_set(req, 1);

	r = run_fold((SCN)800, views, 2, req);
	UT_ASSERT_EQ(r.st, CLUSTER_UNDO_HORIZON_FOLD_STALLED);
	UT_ASSERT_EQ(r.reason, CLUSTER_UNDO_HORIZON_STALL_TORN);
	UT_ASSERT_EQ(r.blame, 1);
}

/* ---- U13: slow-tick sender is not misjudged stale ------------------- */
UT_TEST(test_u13_slow_sender_window)
{
	uint8 req[CLUSTER_RECONFIG_DEAD_BITMAP_BYTES] = { 0 };
	ClusterUndoHorizonReportView views[2];
	FoldResult r;

	views[0] = mk_view(0);
	views[1] = mk_view(700);
	views[1].sender_interval_ms = 10000; /* 10s tick */
	/* age 20s: stale under 3 x local(1s), fresh under 3 x sender(10s) */
	views[1].recv_at_us = TEST_NOW_US - (uint64)20 * 1000 * 1000;
	bm_set(req, 1);

	r = run_fold((SCN)800, views, 2, req);
	UT_ASSERT_EQ(r.st, CLUSTER_UNDO_HORIZON_FOLD_OK);
	UT_ASSERT_EQ((uint64)r.floor.scn, (uint64)700);
}

/* ---- U13b: out-of-range sender interval => MALFORMED ---------------- */
UT_TEST(test_u13b_bad_interval)
{
	uint8 req[CLUSTER_RECONFIG_DEAD_BITMAP_BYTES] = { 0 };
	ClusterUndoHorizonReportView views[2];
	FoldResult r;

	views[0] = mk_view(0);
	views[1] = mk_view(700);
	views[1].sender_interval_ms = 60001; /* > MAX: handler rejects on
											 * the wire; the fold re-checks
											 * defensively */
	bm_set(req, 1);

	r = run_fold((SCN)800, views, 2, req);
	UT_ASSERT_EQ(r.st, CLUSTER_UNDO_HORIZON_FOLD_STALLED);
	UT_ASSERT_EQ(r.reason, CLUSTER_UNDO_HORIZON_STALL_MALFORMED);
	UT_ASSERT_EQ(r.blame, 1);
}

/* ---- U14: same-epoch regression latch => REGRESSION ----------------- */
UT_TEST(test_u14_regression)
{
	uint8 req[CLUSTER_RECONFIG_DEAD_BITMAP_BYTES] = { 0 };
	ClusterUndoHorizonReportView views[2];
	FoldResult r;

	views[0] = mk_view(0);
	views[1] = mk_view(700); /* previously accepted value still in the
								 * slot -- it must NOT be used once the
								 * peer contradicted it */
	views[1].regression_flagged = true;
	bm_set(req, 1);

	r = run_fold((SCN)800, views, 2, req);
	UT_ASSERT_EQ(r.st, CLUSTER_UNDO_HORIZON_FOLD_STALLED);
	UT_ASSERT_EQ(r.reason, CLUSTER_UNDO_HORIZON_STALL_REGRESSION);
	UT_ASSERT_EQ(r.blame, 1);
}

/* U15 (wire validation matrix) lives with the D5-2 handler tests. */

/* ---- U16: required peer without current capability => NOCAP --------- */
UT_TEST(test_u16_nocap)
{
	uint8 req[CLUSTER_RECONFIG_DEAD_BITMAP_BYTES] = { 0 };
	ClusterUndoHorizonReportView views[2];
	FoldResult r;

	views[0] = mk_view(0);
	views[1] = mk_view(700);		 /* even with a fresh old report... */
	views[1].has_capability = false; /* ...no CURRENT-connection cap */
	bm_set(req, 1);

	r = run_fold((SCN)800, views, 2, req);
	UT_ASSERT_EQ(r.st, CLUSTER_UNDO_HORIZON_FOLD_STALLED);
	UT_ASSERT_EQ(r.reason, CLUSTER_UNDO_HORIZON_STALL_NOCAP);
	UT_ASSERT_EQ(r.blame, 1);
}

/* ---- U16b: first-fail attribution order is deterministic ------------ */
UT_TEST(test_u16b_blame_order)
{
	uint8 req[CLUSTER_RECONFIG_DEAD_BITMAP_BYTES] = { 0 };
	ClusterUndoHorizonReportView views[3];
	FoldResult r;

	views[0] = mk_view(0);
	views[1] = mk_view(700);
	views[1].valid = false; /* node1: MISSING */
	views[2] = mk_view(700);
	views[2].has_capability = false; /* node2: NOCAP */
	bm_set(req, 1);
	bm_set(req, 2);

	/* lowest failing node id wins: node1 / MISSING */
	r = run_fold((SCN)800, views, 3, req);
	UT_ASSERT_EQ(r.st, CLUSTER_UNDO_HORIZON_FOLD_STALLED);
	UT_ASSERT_EQ(r.reason, CLUSTER_UNDO_HORIZON_STALL_MISSING);
	UT_ASSERT_EQ(r.blame, 1);
}

/* ---- U17: idle-unconstrained sentinel never wins the min fold -------- */
UT_TEST(test_u17_idle_sentinel_skipped)
{
	uint8 req[CLUSTER_RECONFIG_DEAD_BITMAP_BYTES] = { 0 };
	ClusterUndoHorizonReportView views[3];
	FoldResult r;

	views[0] = mk_view(0); /* self slot: ignored */
	views[1] = mk_view((uint64)CLUSTER_UNDO_HORIZON_REPORT_UNCONSTRAINED);
	views[2] = mk_view(900);
	bm_set(req, 1);
	bm_set(req, 2);

	/* the sentinel peer contributes nothing; min(local 800, peer2 900) */
	r = run_fold((SCN)800, views, 3, req);
	UT_ASSERT_EQ(r.st, CLUSTER_UNDO_HORIZON_FOLD_OK);
	UT_ASSERT_EQ((uint64)r.floor.scn, (uint64)800);
	UT_ASSERT_EQ(r.floor.epoch, TEST_EPOCH);

	/* every required peer idle => floor is exactly the local horizon */
	views[2] = mk_view((uint64)CLUSTER_UNDO_HORIZON_REPORT_UNCONSTRAINED);
	r = run_fold((SCN)800, views, 3, req);
	UT_ASSERT_EQ(r.st, CLUSTER_UNDO_HORIZON_FOLD_OK);
	UT_ASSERT_EQ((uint64)r.floor.scn, (uint64)800);
}

/* ---- U17b: sentinel reports still owe every proof obligation --------- */
UT_TEST(test_u17b_idle_sentinel_still_proven)
{
	uint8 req[CLUSTER_RECONFIG_DEAD_BITMAP_BYTES] = { 0 };
	ClusterUndoHorizonReportView views[2];
	FoldResult r;

	/* a STALE sentinel stalls exactly like a stale real report: an idle
	 * peer that stops reporting is unproven coverage, not "unconstrained
	 * forever" */
	views[0] = mk_view(0);
	views[1] = mk_view((uint64)CLUSTER_UNDO_HORIZON_REPORT_UNCONSTRAINED);
	views[1].recv_at_us = TEST_NOW_US - (uint64)10 * 1000 * 1000; /* 10s old */
	bm_set(req, 1);

	r = run_fold((SCN)800, views, 2, req);
	UT_ASSERT_EQ(r.st, CLUSTER_UNDO_HORIZON_FOLD_STALLED);
	UT_ASSERT_EQ(r.reason, CLUSTER_UNDO_HORIZON_STALL_STALE);
	UT_ASSERT_EQ(r.blame, 1);

	/* an epoch-mismatched sentinel stalls too */
	views[1] = mk_view((uint64)CLUSTER_UNDO_HORIZON_REPORT_UNCONSTRAINED);
	views[1].epoch = TEST_EPOCH - 1;
	r = run_fold((SCN)800, views, 2, req);
	UT_ASSERT_EQ(r.st, CLUSTER_UNDO_HORIZON_FOLD_STALLED);
	UT_ASSERT_EQ(r.reason, CLUSTER_UNDO_HORIZON_STALL_EPOCH);
	UT_ASSERT_EQ(r.blame, 1);
}

/* ---- reason names cover every enum arm ------------------------------ */
UT_TEST(test_reason_names)
{
	UT_ASSERT_STR_EQ(cluster_undo_horizon_stall_reason_name(CLUSTER_UNDO_HORIZON_STALL_NONE),
					 "none");
	UT_ASSERT_STR_EQ(cluster_undo_horizon_stall_reason_name(CLUSTER_UNDO_HORIZON_STALL_NOCAP),
					 "nocap");
	UT_ASSERT_STR_EQ(cluster_undo_horizon_stall_reason_name(CLUSTER_UNDO_HORIZON_STALL_MISSING),
					 "missing");
	UT_ASSERT_STR_EQ(cluster_undo_horizon_stall_reason_name(CLUSTER_UNDO_HORIZON_STALL_TORN),
					 "torn");
	UT_ASSERT_STR_EQ(cluster_undo_horizon_stall_reason_name(CLUSTER_UNDO_HORIZON_STALL_EPOCH),
					 "epoch");
	UT_ASSERT_STR_EQ(cluster_undo_horizon_stall_reason_name(CLUSTER_UNDO_HORIZON_STALL_MALFORMED),
					 "malformed");
	UT_ASSERT_STR_EQ(cluster_undo_horizon_stall_reason_name(CLUSTER_UNDO_HORIZON_STALL_REGRESSION),
					 "regression");
	UT_ASSERT_STR_EQ(cluster_undo_horizon_stall_reason_name(CLUSTER_UNDO_HORIZON_STALL_STALE),
					 "stale");
}

int
main(void)
{
	UT_PLAN(21);

	UT_RUN(test_u1_no_required_peer);
	UT_RUN(test_u2_all_fresh_min);
	UT_RUN(test_u3_missing);
	UT_RUN(test_u4_stale);
	UT_RUN(test_u5_epoch_mismatch);
	UT_RUN(test_u6_peer_below_local);
	UT_RUN(test_u7_invalid_scn);
	UT_RUN(test_u7b_invalid_local);
	UT_RUN(test_u8_ghost_ignored);
	UT_RUN(test_u9_dead_removed_dropped);
	UT_RUN(test_u10_future_recv);
	UT_RUN(test_u11_cold_formation);
	UT_RUN(test_u12_torn);
	UT_RUN(test_u13_slow_sender_window);
	UT_RUN(test_u13b_bad_interval);
	UT_RUN(test_u14_regression);
	UT_RUN(test_u16_nocap);
	UT_RUN(test_u16b_blame_order);
	UT_RUN(test_u17_idle_sentinel_skipped);
	UT_RUN(test_u17b_idle_sentinel_still_proven);
	UT_RUN(test_reason_names);

	UT_DONE();
}
