/*-------------------------------------------------------------------------
 *
 * test_cluster_vis_undo_verdict_map.c
 *	  Unit tests for the spec-5.22f D6 fresh-ref visibility consumer pure
 *	  helpers (shared-catalog seed/joiner visibility, Layer-2 D6).
 *
 *	  D6-1 adds two PURE helpers to cluster_visibility_verdict.c:
 *	    cluster_vis_from_undo_verdict        -- map a D3 five-value verdict
 *	                                            onto the ClusterVisResolve
 *	                                            out-params (U1-U7);
 *	    cluster_vis_freshref_origin_decision -- the fresh-ref origin
 *	                                            decision ASK/STALE (U8-U10).
 *	  Both are dependency-free (no scn_time_cmp, no shmem, no elog) so the
 *	  mapping + origin-decision truth tables are a fully enumerable unit
 *	  test, linking cluster_visibility_verdict.o standalone -- exactly as
 *	  test_cluster_visibility_variants does for the OBS-2~5 tables.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/test/cluster_unit/test_cluster_vis_undo_verdict_map.c
 *
 * NOTES
 *	  This is a pgrac-original file.
 *	  Spec: spec-5.22f-shared-catalog-seed-visibility-consumer.md (D6, §4.1).
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "cluster/cluster_scn.h"		  /* SCN, InvalidScn */
#include "cluster/cluster_tt_status.h"	  /* ClusterTTStatus */
#include "cluster/cluster_undo_verdict.h" /* ClusterUndoVerdictResult + kinds */
#include "cluster/cluster_visibility_resolve.h"

/* Drop PG's port.h printf -> pg_printf override; unit_test.h uses stdlib
 * printf and we don't link libpgport into this binary. */
#undef printf

#include "unit_test.h"

UT_DEFINE_GLOBALS();


/* Seed a ClusterVisResolve with obvious garbage so a helper that fails to
 * overwrite a field leaks it (U7 residue guard). */
static void
poison_out(ClusterVisResolve *out)
{
	memset(out, 0, sizeof(*out));
	out->evidence = CLUSTER_VIS_EVIDENCE_LOCAL;
	out->status = CLUSTER_TT_STATUS_IN_PROGRESS;
	out->commit_scn = (SCN)987654321;
	out->commit_scn_is_bound = true;
}

static ClusterUndoVerdictResult
make_verdict(ClusterUndoVerdictKind kind, SCN commit_scn)
{
	ClusterUndoVerdictResult v = { 0 };

	v.kind = (uint8)kind;
	v.commit_scn = commit_scn;
	return v;
}


/* ======================================================================
 * U1 -- COMMITTED_EXACT{scn} -> REMOTE/COMMITTED/scn, is_bound=false, true.
 * ====================================================================== */
UT_TEST(test_map_committed_exact)
{
	ClusterVisResolve out;
	bool terminal;

	poison_out(&out);
	terminal = cluster_vis_from_undo_verdict(
		make_verdict(CLUSTER_UNDO_VERDICT_COMMITTED_EXACT, (SCN)5000), &out);

	UT_ASSERT_EQ((int)terminal, 1);
	UT_ASSERT_EQ((int)out.evidence, (int)CLUSTER_VIS_EVIDENCE_REMOTE);
	UT_ASSERT_EQ((int)out.status, (int)CLUSTER_TT_STATUS_COMMITTED);
	UT_ASSERT_EQ((uint64)out.commit_scn, (uint64)5000);
	UT_ASSERT_EQ((int)out.commit_scn_is_bound, 0);
}

/* ======================================================================
 * U2 -- COMMITTED_BOUND{scn} -> REMOTE/COMMITTED/scn, is_bound=TRUE, true.
 * The bound must never be mistaken for an exact commit_scn (Amendment-2
 * traced through: consumers never stamp / cache a bound).
 * ====================================================================== */
UT_TEST(test_map_committed_bound_sets_is_bound)
{
	ClusterVisResolve out;
	bool terminal;

	poison_out(&out);
	terminal = cluster_vis_from_undo_verdict(
		make_verdict(CLUSTER_UNDO_VERDICT_COMMITTED_BOUND, (SCN)6000), &out);

	UT_ASSERT_EQ((int)terminal, 1);
	UT_ASSERT_EQ((int)out.evidence, (int)CLUSTER_VIS_EVIDENCE_REMOTE);
	UT_ASSERT_EQ((int)out.status, (int)CLUSTER_TT_STATUS_COMMITTED);
	UT_ASSERT_EQ((uint64)out.commit_scn, (uint64)6000);
	UT_ASSERT_EQ((int)out.commit_scn_is_bound, 1);
}

/* ======================================================================
 * U3 -- ABORTED -> REMOTE/ABORTED/InvalidScn, is_bound=false, true.
 * ====================================================================== */
UT_TEST(test_map_aborted)
{
	ClusterVisResolve out;
	bool terminal;

	poison_out(&out);
	terminal = cluster_vis_from_undo_verdict(make_verdict(CLUSTER_UNDO_VERDICT_ABORTED, InvalidScn),
											 &out);

	UT_ASSERT_EQ((int)terminal, 1);
	UT_ASSERT_EQ((int)out.evidence, (int)CLUSTER_VIS_EVIDENCE_REMOTE);
	UT_ASSERT_EQ((int)out.status, (int)CLUSTER_TT_STATUS_ABORTED);
	UT_ASSERT_EQ((uint64)out.commit_scn, (uint64)InvalidScn);
	UT_ASSERT_EQ((int)out.commit_scn_is_bound, 0);
}

/* ======================================================================
 * U4 -- UNKNOWN_FAIL_CLOSED -> STALE_OR_AMBIGUOUS/UNKNOWN/InvalidScn, false
 * (Rule 8.A: caller keeps 53R97; never treated committed/visible).
 * ====================================================================== */
UT_TEST(test_map_unknown_is_stale_fail_closed)
{
	ClusterVisResolve out;
	bool terminal;

	poison_out(&out);
	terminal = cluster_vis_from_undo_verdict(
		make_verdict(CLUSTER_UNDO_VERDICT_UNKNOWN_FAIL_CLOSED, InvalidScn), &out);

	UT_ASSERT_EQ((int)terminal, 0);
	UT_ASSERT_EQ((int)out.evidence, (int)CLUSTER_VIS_EVIDENCE_STALE_OR_AMBIGUOUS);
	UT_ASSERT_EQ((int)out.status, (int)CLUSTER_TT_STATUS_UNKNOWN);
	UT_ASSERT_EQ((uint64)out.commit_scn, (uint64)InvalidScn);
	UT_ASSERT_EQ((int)out.commit_scn_is_bound, 0);
}

/* ======================================================================
 * U5 -- IN_PROGRESS (D3 folds it to UNKNOWN, defensive) -> STALE, false.
 * ====================================================================== */
UT_TEST(test_map_in_progress_is_stale_fail_closed)
{
	ClusterVisResolve out;
	bool terminal;

	poison_out(&out);
	terminal = cluster_vis_from_undo_verdict(
		make_verdict(CLUSTER_UNDO_VERDICT_IN_PROGRESS, InvalidScn), &out);

	UT_ASSERT_EQ((int)terminal, 0);
	UT_ASSERT_EQ((int)out.evidence, (int)CLUSTER_VIS_EVIDENCE_STALE_OR_AMBIGUOUS);
	UT_ASSERT_EQ((int)out.status, (int)CLUSTER_TT_STATUS_UNKNOWN);
}

/* ======================================================================
 * U6 -- a zero-initialised verdict {0} (== UNKNOWN_FAIL_CLOSED, L10) maps to
 * STALE / false, so an un-proven verdict struct is never accidentally visible.
 * ====================================================================== */
UT_TEST(test_map_zero_init_verdict_is_stale)
{
	ClusterUndoVerdictResult v = { 0 };
	ClusterVisResolve out;
	bool terminal;

	UT_ASSERT_EQ((int)CLUSTER_UNDO_VERDICT_UNKNOWN_FAIL_CLOSED, 0);

	poison_out(&out);
	terminal = cluster_vis_from_undo_verdict(v, &out);

	UT_ASSERT_EQ((int)terminal, 0);
	UT_ASSERT_EQ((int)out.evidence, (int)CLUSTER_VIS_EVIDENCE_STALE_OR_AMBIGUOUS);
}

/* ======================================================================
 * U7 -- residue guard: a STALE map must overwrite commit_scn + is_bound so a
 * caller's earlier terminal residue never leaks (poison_out seeds 987654321 /
 * is_bound=true; the STALE map must clear both).
 * ====================================================================== */
UT_TEST(test_map_stale_clears_scn_residue)
{
	ClusterVisResolve out;

	poison_out(&out);
	(void)cluster_vis_from_undo_verdict(
		make_verdict(CLUSTER_UNDO_VERDICT_UNKNOWN_FAIL_CLOSED, InvalidScn), &out);

	UT_ASSERT_EQ((uint64)out.commit_scn, (uint64)InvalidScn);
	UT_ASSERT_EQ((int)out.commit_scn_is_bound, 0);
}

/* ======================================================================
 * U8 -- origin decision: derived == ref_origin (corroborated) -> ASK.
 * ====================================================================== */
UT_TEST(test_origin_corroborated_is_ask)
{
	UT_ASSERT_EQ((int)cluster_vis_freshref_origin_decision(2, 2),
				 (int)CLUSTER_VIS_FRESHREF_ORIGIN_ASK);
}

/* ======================================================================
 * U9 -- origin decision: derived == -1 (underivable: below-floor pre-striping
 * seed / striping off) -> ASK (P1-a: NOT fail-closed; = root-cause #1 path).
 * ====================================================================== */
UT_TEST(test_origin_underivable_is_ask_not_failclosed)
{
	UT_ASSERT_EQ((int)cluster_vis_freshref_origin_decision(-1, 2),
				 (int)CLUSTER_VIS_FRESHREF_ORIGIN_ASK);
}

/* ======================================================================
 * U10 -- origin decision: derived >= 0 && derived != ref_origin (striping bug
 * / page corruption / alias) -> STALE (Rule 8.A integrity guard).
 * ====================================================================== */
UT_TEST(test_origin_derivable_mismatch_is_stale)
{
	UT_ASSERT_EQ((int)cluster_vis_freshref_origin_decision(3, 2),
				 (int)CLUSTER_VIS_FRESHREF_ORIGIN_STALE);
}

int
main(void)
{
	UT_PLAN(10);
	UT_RUN(test_map_committed_exact);
	UT_RUN(test_map_committed_bound_sets_is_bound);
	UT_RUN(test_map_aborted);
	UT_RUN(test_map_unknown_is_stale_fail_closed);
	UT_RUN(test_map_in_progress_is_stale_fail_closed);
	UT_RUN(test_map_zero_init_verdict_is_stale);
	UT_RUN(test_map_stale_clears_scn_residue);
	UT_RUN(test_origin_corroborated_is_ask);
	UT_RUN(test_origin_underivable_is_ask_not_failclosed);
	UT_RUN(test_origin_derivable_mismatch_is_stale);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
