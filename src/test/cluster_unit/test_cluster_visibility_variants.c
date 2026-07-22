/*-------------------------------------------------------------------------
 *
 * test_cluster_visibility_variants.c
 *	  cluster_unit full enumeration of the spec-3.14 §2.2 OBS truth
 *	  tables (Self / Toast / Update xmin / Update xmax / Dirty).
 *
 *	  These are pure status->verdict functions, so the test enumerates
 *	  every ClusterTTStatus (the executable copy of the spec truth
 *	  tables, L212 single source).  No buffer / no page needed.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/test/cluster_unit/test_cluster_visibility_variants.c
 *
 * NOTES
 *	  This is a pgrac-original file.
 *	  Spec: spec-3.14-remaining-visibility-paths.md (FROZEN v0.2) §2.2.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/htup_details.h"
#include "cluster/cluster_tt_status.h"
#include "cluster/cluster_visibility_resolve.h"

/* Drop PG's port.h printf -> pg_printf override; unit_test.h uses
 * stdlib printf and we don't link libpgport in this test binary. */
#undef printf

#include "unit_test.h"

UT_DEFINE_GLOBALS();


/* The six terminal-or-in-progress states the resolver can hand a verdict. */
static const ClusterTTStatus all_states[]
	= { CLUSTER_TT_STATUS_UNKNOWN, CLUSTER_TT_STATUS_IN_PROGRESS, CLUSTER_TT_STATUS_COMMITTED,
		CLUSTER_TT_STATUS_ABORTED, CLUSTER_TT_STATUS_CLEANED_OUT, CLUSTER_TT_STATUS_SUBCOMMITTED };


/*
 * P0-27: VACUUM freeze is an authoritative xmin-committed proof.  A frozen
 * tuple may retain its historical raw xmin and ITL index after that page slot
 * is legally recycled, so resolving the stale xmin identity is both needless
 * and wrong.  Ordinary COMMITTED/INVALID hints are deliberately NOT widened:
 * only the exact FROZEN bit pair bypasses cluster resolution.
 */
UT_TEST(test_update_xmin_frozen_precheck)
{
	UT_ASSERT_EQ((int)cluster_vis_xmin_needs_resolution(HEAP_XMIN_FROZEN), 0);
	UT_ASSERT_EQ((int)cluster_vis_xmin_needs_resolution(HEAP_XMIN_FROZEN | HEAP_XMAX_INVALID), 0);
	UT_ASSERT_EQ((int)cluster_vis_xmin_needs_resolution(HEAP_XMIN_COMMITTED), 1);
	UT_ASSERT_EQ((int)(cluster_vis_xmin_needs_resolution(HEAP_XMIN_COMMITTED)
					   && cluster_vis_evidence_route(CLUSTER_VIS_EVIDENCE_STALE_OR_AMBIGUOUS, false)
							  == CLUSTER_VIS_ROUTE_FAILCLOSED_UNKNOWN),
				 1);
	UT_ASSERT_EQ((int)cluster_vis_xmin_needs_resolution(HEAP_XMIN_INVALID), 1);
	UT_ASSERT_EQ((int)cluster_vis_xmin_needs_resolution(0), 1);
}


/*
 * P0-28: a TID selected on one node remains a legal statement target while
 * PCM-X waits for a newer page image.  Once an ITL slot is recycled, neither
 * xmin hints nor remote-evidence lookup can prove that no peer still names
 * that TID.  Until a real cluster-wide horizon exists, shared-storage tuples
 * must bypass every local prune verdict, including xmin-invalid/unknown forms.
 */
UT_TEST(test_prune_requires_cluster_wide_horizon)
{
	UT_ASSERT_EQ((int)cluster_vis_prune_must_defer(true, false), 1);
	UT_ASSERT_EQ((int)cluster_vis_prune_must_defer(true, true), 0);
	UT_ASSERT_EQ((int)cluster_vis_prune_must_defer(false, false), 0);
	UT_ASSERT_EQ((int)cluster_vis_prune_must_defer(false, true), 0);
}


/* ---- OBS-4 Self ---- */
UT_TEST(test_obs4_self_full_table)
{
	UT_ASSERT_EQ((int)cluster_vis_self_verdict(CLUSTER_TT_STATUS_COMMITTED), (int)CVV_VISIBLE);
	UT_ASSERT_EQ((int)cluster_vis_self_verdict(CLUSTER_TT_STATUS_CLEANED_OUT), (int)CVV_VISIBLE);
	UT_ASSERT_EQ((int)cluster_vis_self_verdict(CLUSTER_TT_STATUS_IN_PROGRESS), (int)CVV_INVISIBLE);
	UT_ASSERT_EQ((int)cluster_vis_self_verdict(CLUSTER_TT_STATUS_SUBCOMMITTED), (int)CVV_INVISIBLE);
	UT_ASSERT_EQ((int)cluster_vis_self_verdict(CLUSTER_TT_STATUS_ABORTED), (int)CVV_INVISIBLE);
	UT_ASSERT_EQ((int)cluster_vis_self_verdict(CLUSTER_TT_STATUS_UNKNOWN),
				 (int)CVV_FAILCLOSED_UNKNOWN);
}

/* ---- OBS-5 Toast (permissive: only ABORTED hides; UNKNOWN must be heard) ---- */
UT_TEST(test_obs5_toast_full_table)
{
	UT_ASSERT_EQ((int)cluster_vis_toast_verdict(CLUSTER_TT_STATUS_ABORTED), (int)CVV_INVISIBLE);
	UT_ASSERT_EQ((int)cluster_vis_toast_verdict(CLUSTER_TT_STATUS_UNKNOWN),
				 (int)CVV_FAILCLOSED_UNKNOWN);
	UT_ASSERT_EQ((int)cluster_vis_toast_verdict(CLUSTER_TT_STATUS_COMMITTED), (int)CVV_VISIBLE);
	UT_ASSERT_EQ((int)cluster_vis_toast_verdict(CLUSTER_TT_STATUS_CLEANED_OUT), (int)CVV_VISIBLE);
	UT_ASSERT_EQ((int)cluster_vis_toast_verdict(CLUSTER_TT_STATUS_IN_PROGRESS), (int)CVV_VISIBLE);
	UT_ASSERT_EQ((int)cluster_vis_toast_verdict(CLUSTER_TT_STATUS_SUBCOMMITTED), (int)CVV_VISIBLE);
}

/* ---- OBS-2 Update xmin gate ---- */
UT_TEST(test_obs2_update_xmin_full_table)
{
	UT_ASSERT_EQ((int)cluster_vis_update_xmin_verdict(CLUSTER_TT_STATUS_COMMITTED),
				 (int)CVV_VISIBLE);
	UT_ASSERT_EQ((int)cluster_vis_update_xmin_verdict(CLUSTER_TT_STATUS_CLEANED_OUT),
				 (int)CVV_VISIBLE);
	UT_ASSERT_EQ((int)cluster_vis_update_xmin_verdict(CLUSTER_TT_STATUS_IN_PROGRESS),
				 (int)CVV_INVISIBLE);
	UT_ASSERT_EQ((int)cluster_vis_update_xmin_verdict(CLUSTER_TT_STATUS_SUBCOMMITTED),
				 (int)CVV_INVISIBLE);
	UT_ASSERT_EQ((int)cluster_vis_update_xmin_verdict(CLUSTER_TT_STATUS_ABORTED),
				 (int)CVV_INVISIBLE);
	UT_ASSERT_EQ((int)cluster_vis_update_xmin_verdict(CLUSTER_TT_STATUS_UNKNOWN),
				 (int)CVV_FAILCLOSED_UNKNOWN);
}

/* ---- OBS-2 Update xmax outcome (update vs delete) ---- */
UT_TEST(test_obs2_update_xmax_full_table)
{
	/* update writer */
	UT_ASSERT_EQ((int)cluster_vis_update_xmax_verdict(CLUSTER_TT_STATUS_ABORTED, false),
				 (int)CVV_VISIBLE);
	UT_ASSERT_EQ((int)cluster_vis_update_xmax_verdict(CLUSTER_TT_STATUS_COMMITTED, false),
				 (int)CVV_GONE_UPDATED);
	UT_ASSERT_EQ((int)cluster_vis_update_xmax_verdict(CLUSTER_TT_STATUS_CLEANED_OUT, false),
				 (int)CVV_GONE_UPDATED);
	UT_ASSERT_EQ((int)cluster_vis_update_xmax_verdict(CLUSTER_TT_STATUS_IN_PROGRESS, false),
				 (int)CVV_BEING_MODIFIED);
	UT_ASSERT_EQ((int)cluster_vis_update_xmax_verdict(CLUSTER_TT_STATUS_SUBCOMMITTED, false),
				 (int)CVV_BEING_MODIFIED);
	UT_ASSERT_EQ((int)cluster_vis_update_xmax_verdict(CLUSTER_TT_STATUS_UNKNOWN, false),
				 (int)CVV_FAILCLOSED_UNKNOWN);
	/* delete writer: committed -> TM_Deleted */
	UT_ASSERT_EQ((int)cluster_vis_update_xmax_verdict(CLUSTER_TT_STATUS_COMMITTED, true),
				 (int)CVV_GONE_DELETED);
	UT_ASSERT_EQ((int)cluster_vis_update_xmax_verdict(CLUSTER_TT_STATUS_ABORTED, true),
				 (int)CVV_VISIBLE);
}

/* ---- OBS-3 Dirty: in-progress -> 53R9H conflict (no wait layer) ---- */
UT_TEST(test_obs3_dirty_full_table)
{
	/* xmin side */
	UT_ASSERT_EQ((int)cluster_vis_dirty_verdict(CLUSTER_TT_STATUS_IN_PROGRESS, false, false),
				 (int)CVV_FAILCLOSED_CONFLICT);
	UT_ASSERT_EQ((int)cluster_vis_dirty_verdict(CLUSTER_TT_STATUS_SUBCOMMITTED, false, false),
				 (int)CVV_FAILCLOSED_CONFLICT);
	UT_ASSERT_EQ((int)cluster_vis_dirty_verdict(CLUSTER_TT_STATUS_COMMITTED, false, false),
				 (int)CVV_VISIBLE);
	UT_ASSERT_EQ((int)cluster_vis_dirty_verdict(CLUSTER_TT_STATUS_ABORTED, false, false),
				 (int)CVV_INVISIBLE);
	UT_ASSERT_EQ((int)cluster_vis_dirty_verdict(CLUSTER_TT_STATUS_UNKNOWN, false, false),
				 (int)CVV_FAILCLOSED_UNKNOWN);
	/* xmax side */
	UT_ASSERT_EQ((int)cluster_vis_dirty_verdict(CLUSTER_TT_STATUS_IN_PROGRESS, true, false),
				 (int)CVV_FAILCLOSED_CONFLICT);
	UT_ASSERT_EQ((int)cluster_vis_dirty_verdict(CLUSTER_TT_STATUS_ABORTED, true, false),
				 (int)CVV_VISIBLE);
	UT_ASSERT_EQ((int)cluster_vis_dirty_verdict(CLUSTER_TT_STATUS_COMMITTED, true, false),
				 (int)CVV_GONE_UPDATED);
	UT_ASSERT_EQ((int)cluster_vis_dirty_verdict(CLUSTER_TT_STATUS_COMMITTED, true, true),
				 (int)CVV_GONE_DELETED);
}

/* ---- meta: no status maps to an out-of-range verdict (exhaustive sweep) ---- */
UT_TEST(test_all_verdicts_in_range)
{
	int i;

	for (i = 0; i < (int)(sizeof(all_states) / sizeof(all_states[0])); i++) {
		ClusterTTStatus st = all_states[i];

		UT_ASSERT_EQ(cluster_vis_self_verdict(st) <= CVV_FAILCLOSED_CONFLICT, 1);
		UT_ASSERT_EQ(cluster_vis_toast_verdict(st) <= CVV_FAILCLOSED_CONFLICT, 1);
		UT_ASSERT_EQ(cluster_vis_update_xmin_verdict(st) <= CVV_FAILCLOSED_CONFLICT, 1);
		UT_ASSERT_EQ(cluster_vis_update_xmax_verdict(st, false) <= CVV_FAILCLOSED_CONFLICT, 1);
		UT_ASSERT_EQ(cluster_vis_dirty_verdict(st, false, false) <= CVV_FAILCLOSED_CONFLICT, 1);
	}
}


/*
 * spec-3.21 §2.3: CR image xmax-side MVCC verdict.  Unlike the SatisfiesUpdate
 * xmax verdict (status-only), the snapshot read must compare the committed
 * deleter's commit_scn to read_scn.  The defining fix: an uncommitted deleter
 * (IN_PROGRESS / ABORTED) means the row was LIVE at read_scn -> VISIBLE (the CR
 * image still carries that xmax because the construct correctly stops the chain
 * walk at write_scn <= read_scn).  spec-3.21 D0.6: 538 in-progress false-invisibles.
 */
UT_TEST(test_obs_cr_xmax_full_table)
{
	/* committed_scn_decision is the caller's decide_by_scn(commit_scn, read_scn):
	 *   VISIBLE   = delete committed at/before read_scn  (commit_scn <= read_scn)
	 *   INVISIBLE = delete committed after read_scn       (commit_scn  > read_scn)
	 *   UNKNOWN   = commit_scn unresolved (InvalidScn). */

	/* Uncommitted deleter -> row live at read_scn -> VISIBLE (scn decision N/A). */
	UT_ASSERT_EQ(
		(int)cluster_vis_cr_xmax_verdict(CLUSTER_TT_STATUS_IN_PROGRESS, CLUSTER_VISIBILITY_UNKNOWN),
		(int)CVV_VISIBLE);
	UT_ASSERT_EQ(
		(int)cluster_vis_cr_xmax_verdict(CLUSTER_TT_STATUS_ABORTED, CLUSTER_VISIBILITY_UNKNOWN),
		(int)CVV_VISIBLE);
	UT_ASSERT_EQ((int)cluster_vis_cr_xmax_verdict(CLUSTER_TT_STATUS_SUBCOMMITTED,
												  CLUSTER_VISIBILITY_UNKNOWN),
				 (int)CVV_VISIBLE);

	/* Committed delete at/before snapshot (decide VISIBLE) -> tuple INVISIBLE. */
	UT_ASSERT_EQ(
		(int)cluster_vis_cr_xmax_verdict(CLUSTER_TT_STATUS_COMMITTED, CLUSTER_VISIBILITY_VISIBLE),
		(int)CVV_INVISIBLE);
	UT_ASSERT_EQ(
		(int)cluster_vis_cr_xmax_verdict(CLUSTER_TT_STATUS_CLEANED_OUT, CLUSTER_VISIBILITY_VISIBLE),
		(int)CVV_INVISIBLE);

	/* Committed delete AFTER snapshot (decide INVISIBLE) -> row live -> VISIBLE. */
	UT_ASSERT_EQ(
		(int)cluster_vis_cr_xmax_verdict(CLUSTER_TT_STATUS_COMMITTED, CLUSTER_VISIBILITY_INVISIBLE),
		(int)CVV_VISIBLE);
	UT_ASSERT_EQ((int)cluster_vis_cr_xmax_verdict(CLUSTER_TT_STATUS_CLEANED_OUT,
												  CLUSTER_VISIBILITY_INVISIBLE),
				 (int)CVV_VISIBLE);

	/* Unknown status -> fail-closed, NEVER silently invisible. */
	UT_ASSERT_EQ(
		(int)cluster_vis_cr_xmax_verdict(CLUSTER_TT_STATUS_UNKNOWN, CLUSTER_VISIBILITY_UNKNOWN),
		(int)CVV_FAILCLOSED_UNKNOWN);
	/* Committed but commit_scn unresolved (decide UNKNOWN) -> fail-closed, not invisible. */
	UT_ASSERT_EQ(
		(int)cluster_vis_cr_xmax_verdict(CLUSTER_TT_STATUS_COMMITTED, CLUSTER_VISIBILITY_UNKNOWN),
		(int)CVV_FAILCLOSED_UNKNOWN);
}


/*
 * Serve-stall round-6: evidence -> consumer route, full 8-row enumeration.
 * The load-bearing rows: a raw current-xid match NEVER routes away from
 * REMOTE or STALE evidence (striping-off/below-floor raw-xid collision with
 * a remote writer must follow the resolved verdict / fail closed, never the
 * native self/cmin path).
 */
UT_TEST(test_evidence_route_full_table)
{
	/* REMOTE: verdict wins, current-xid match irrelevant. */
	UT_ASSERT_EQ((int)cluster_vis_evidence_route(CLUSTER_VIS_EVIDENCE_REMOTE, false),
				 (int)CLUSTER_VIS_ROUTE_REMOTE_VERDICT);
	UT_ASSERT_EQ((int)cluster_vis_evidence_route(CLUSTER_VIS_EVIDENCE_REMOTE, true),
				 (int)CLUSTER_VIS_ROUTE_REMOTE_VERDICT);

	/* STALE/AMBIGUOUS: fail-closed, current-xid match irrelevant. */
	UT_ASSERT_EQ((int)cluster_vis_evidence_route(CLUSTER_VIS_EVIDENCE_STALE_OR_AMBIGUOUS, false),
				 (int)CLUSTER_VIS_ROUTE_FAILCLOSED_UNKNOWN);
	UT_ASSERT_EQ((int)cluster_vis_evidence_route(CLUSTER_VIS_EVIDENCE_STALE_OR_AMBIGUOUS, true),
				 (int)CLUSTER_VIS_ROUTE_FAILCLOSED_UNKNOWN);

	/* LOCAL / NONE: only here may the raw current-xid test route to self. */
	UT_ASSERT_EQ((int)cluster_vis_evidence_route(CLUSTER_VIS_EVIDENCE_LOCAL, true),
				 (int)CLUSTER_VIS_ROUTE_NATIVE_SELF);
	UT_ASSERT_EQ((int)cluster_vis_evidence_route(CLUSTER_VIS_EVIDENCE_LOCAL, false),
				 (int)CLUSTER_VIS_ROUTE_NATIVE);
	UT_ASSERT_EQ((int)cluster_vis_evidence_route(CLUSTER_VIS_EVIDENCE_NONE, true),
				 (int)CLUSTER_VIS_ROUTE_NATIVE_SELF);
	UT_ASSERT_EQ((int)cluster_vis_evidence_route(CLUSTER_VIS_EVIDENCE_NONE, false),
				 (int)CLUSTER_VIS_ROUTE_NATIVE);
}

int
main(void)
{
	UT_RUN(test_update_xmin_frozen_precheck);
	UT_RUN(test_prune_requires_cluster_wide_horizon);
	UT_RUN(test_obs4_self_full_table);
	UT_RUN(test_obs5_toast_full_table);
	UT_RUN(test_obs2_update_xmin_full_table);
	UT_RUN(test_obs2_update_xmax_full_table);
	UT_RUN(test_obs3_dirty_full_table);
	UT_RUN(test_obs_cr_xmax_full_table);
	UT_RUN(test_all_verdicts_in_range);
	UT_RUN(test_evidence_route_full_table);

	UT_DONE();
	return ut_failed_count != 0 ? 1 : 0;
}
