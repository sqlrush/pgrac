/*-------------------------------------------------------------------------
 *
 * test_cluster_undo_verdict.c
 *	  Unit tests for the D3 cross-node xid -> commit_scn verdict taxonomy
 *	  (spec-5.22c, shared undo physical data plane under GCS, Layer-2 D3).
 *
 *	  D3-1 (this increment) pins the five-value verdict taxonomy and the two
 *	  pure mapping helpers: wire ClusterGcsUndoVerdictPage -> taxonomy and
 *	  block-proof ClusterVisTtProof -> taxonomy.  The mappers are pure
 *	  (dependency-free: no scn_time_cmp, no shmem, no elog) so the taxonomy
 *	  layer links standalone against the runtime-visibility policy object.
 *	  The read_scn admissibility gate of a COMMITTED_BOUND is the consumer's
 *	  (D3-3), deliberately outside these dependency-free mappers -- exactly
 *	  as cluster_vis_undo_verdict_page_usable defers it (policy.c NOTES).
 *
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/test/cluster_unit/test_cluster_undo_verdict.c
 *
 * NOTES
 *	  This is a pgrac-original file.
 *	  Spec: spec-5.22c-ttslot-verdict-shared-undo.md (D3, §4.1)
 *
 *	  The mapping helpers live in cluster_runtime_visibility_policy.o (the
 *	  dependency-free vis policy object); the test links that object plus
 *	  libpgport and needs no self-node or SCN-clock globals.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "cluster/cluster_gcs_block.h"			/* ClusterGcsUndoVerdictPage + wire kinds */
#include "cluster/cluster_runtime_visibility.h" /* ClusterVisTtProof */
#include "cluster/cluster_scn.h"				/* SCN, InvalidScn */
#include "cluster/cluster_undo_verdict.h"

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
 * Build a structurally valid verdict page for the asked xid, so the mapper's
 * internal cluster_vis_undo_verdict_page_usable() gate accepts it and the
 * test exercises the taxonomy mapping (not the structural refusal).
 */
static void
make_verdict_page(ClusterGcsUndoVerdictPage *v, uint8 kind, TransactionId xid, SCN commit_scn,
				  SCN horizon_scn, uint16 wrap)
{
	memset(v, 0, sizeof(*v));
	v->magic = CLUSTER_GCS_UNDO_VERDICT_MAGIC;
	v->version = CLUSTER_GCS_UNDO_VERDICT_VERSION;
	v->xid_echo = (uint64)xid;
	v->verdict = kind;
	v->commit_scn = commit_scn;
	v->horizon_scn = horizon_scn;
	v->wrap = wrap;
}

/* ======================================================================
 * U1 -- wire page -> five-value taxonomy mapping (D3-1, Amendment-2):
 * COMMITTED_EXACT -> COMMITTED_EXACT{commit_scn,wrap};
 * COMMITTED_BELOW_HORIZON -> COMMITTED_BOUND{commit_scn=horizon};
 * ABORTED -> ABORTED; structurally-unusable page -> UNKNOWN_FAIL_CLOSED.
 * BOUND is a distinct kind from EXACT so a switch handling only EXACT falls
 * through to a fail-closed default (never treats a horizon bound as exact).
 * ====================================================================== */
UT_TEST(test_undo_verdict_from_wire_page_mapping)
{
	ClusterGcsUndoVerdictPage v;
	ClusterUndoVerdictResult r;
	TransactionId xid = 791;

	/* EXACT -> COMMITTED_EXACT{commit_scn,wrap} */
	make_verdict_page(&v, CLUSTER_GCS_UNDO_VERDICT_COMMITTED_EXACT, xid, 5000, InvalidScn, 7);
	r = cluster_undo_verdict_from_wire_page(&v, xid);
	UT_ASSERT_EQ(r.kind, CLUSTER_UNDO_VERDICT_COMMITTED_EXACT);
	UT_ASSERT_EQ(r.commit_scn, 5000);
	UT_ASSERT_EQ(r.wrap, 7);

	/* BELOW_HORIZON -> COMMITTED_BOUND{commit_scn=horizon}; the read_scn
	 * admissibility gate is the consumer's, not this pure mapper's. */
	make_verdict_page(&v, CLUSTER_GCS_UNDO_VERDICT_COMMITTED_BELOW_HORIZON, xid, InvalidScn, 6000,
					  0);
	r = cluster_undo_verdict_from_wire_page(&v, xid);
	UT_ASSERT_EQ(r.kind, CLUSTER_UNDO_VERDICT_COMMITTED_BOUND);
	UT_ASSERT_EQ(r.commit_scn, 6000);

	/* Amendment-2: BOUND is never EXACT (distinct kind values), and the
	 * BOUND result's kind must never equal EXACT. */
	UT_ASSERT_NE(CLUSTER_UNDO_VERDICT_COMMITTED_BOUND, CLUSTER_UNDO_VERDICT_COMMITTED_EXACT);
	UT_ASSERT_NE(r.kind, CLUSTER_UNDO_VERDICT_COMMITTED_EXACT);

	/* ABORTED -> ABORTED */
	make_verdict_page(&v, CLUSTER_GCS_UNDO_VERDICT_ABORTED, xid, InvalidScn, InvalidScn, 0);
	r = cluster_undo_verdict_from_wire_page(&v, xid);
	UT_ASSERT_EQ(r.kind, CLUSTER_UNDO_VERDICT_ABORTED);

	/* structurally unusable (echo != asked xid) -> UNKNOWN_FAIL_CLOSED */
	make_verdict_page(&v, CLUSTER_GCS_UNDO_VERDICT_COMMITTED_EXACT, xid, 5000, InvalidScn, 7);
	r = cluster_undo_verdict_from_wire_page(&v, xid + 1);
	UT_ASSERT_EQ(r.kind, CLUSTER_UNDO_VERDICT_UNKNOWN_FAIL_CLOSED);
}

/* ======================================================================
 * U2 -- taxonomy zero-value contract (D3-1, L10 / Rule 8.A): a
 * zero-initialised result is UNKNOWN_FAIL_CLOSED with InvalidScn, so any
 * struct that is not explicitly proven terminal defaults to fail-closed.
 * ====================================================================== */
UT_TEST(test_undo_verdict_zero_value_is_fail_closed)
{
	ClusterUndoVerdictResult r = { 0 };

	/* the enum's zero value MUST be the fail-closed sentinel */
	UT_ASSERT_EQ((int)CLUSTER_UNDO_VERDICT_UNKNOWN_FAIL_CLOSED, 0);
	UT_ASSERT_EQ((uint64)InvalidScn, 0);

	/* a {0} result therefore reads as UNKNOWN with no scn */
	UT_ASSERT_EQ(r.kind, CLUSTER_UNDO_VERDICT_UNKNOWN_FAIL_CLOSED);
	UT_ASSERT_EQ((uint64)r.commit_scn, (uint64)InvalidScn);
	UT_ASSERT_EQ(r.wrap, 0);
}

/* ======================================================================
 * U3 -- block-proof -> taxonomy mapping (D3-1, CP3 leg): COMMITTED ->
 * COMMITTED_EXACT{scn,wrap}; ABORTED -> ABORTED; NONE -> UNKNOWN_FAIL_CLOSED
 * (the orchestrator reads a CP3 NONE/UNKNOWN as "fall to CP5", never as the
 * caller's terminal answer).
 * ====================================================================== */
UT_TEST(test_undo_verdict_from_block_proof_mapping)
{
	ClusterUndoVerdictResult r;

	r = cluster_undo_verdict_from_block_proof(CLUSTER_VIS_TT_PROOF_COMMITTED, 4242, 9);
	UT_ASSERT_EQ(r.kind, CLUSTER_UNDO_VERDICT_COMMITTED_EXACT);
	UT_ASSERT_EQ((uint64)r.commit_scn, 4242);
	UT_ASSERT_EQ(r.wrap, 9);

	r = cluster_undo_verdict_from_block_proof(CLUSTER_VIS_TT_PROOF_ABORTED, InvalidScn, 0);
	UT_ASSERT_EQ(r.kind, CLUSTER_UNDO_VERDICT_ABORTED);

	r = cluster_undo_verdict_from_block_proof(CLUSTER_VIS_TT_PROOF_NONE, InvalidScn, 0);
	UT_ASSERT_EQ(r.kind, CLUSTER_UNDO_VERDICT_UNKNOWN_FAIL_CLOSED);
}

int
main(void)
{
	UT_PLAN(3);
	UT_RUN(test_undo_verdict_from_wire_page_mapping);
	UT_RUN(test_undo_verdict_zero_value_is_fail_closed);
	UT_RUN(test_undo_verdict_from_block_proof_mapping);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
