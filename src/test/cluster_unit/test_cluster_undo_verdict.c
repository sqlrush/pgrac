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
#include "cluster/cluster_runtime_visibility.h" /* ClusterVisTtProof + covers policy */
#include "cluster/cluster_scn.h"				/* SCN, InvalidScn */
#include "cluster/cluster_undo_gcs.h"			/* cluster_undo_grant_armed (D3-2 pin) */
#include "cluster/cluster_undo_resid.h"			/* cluster_undo_resid_encode (D3-2 pin) */
#include "cluster/cluster_undo_verdict.h"

#undef printf
#undef fprintf
#undef snprintf

#include "unit_test.h"

UT_DEFINE_GLOBALS();

/*
 * Stub self-node global.  cluster_undo_gcs.o (the D2 routing object linked for
 * the D3-2 armed-gate pin) resolves cluster_node_id from cluster_guc.c in a
 * real backend; the test owns it so the routing object links standalone.
 */
int cluster_node_id = 0;

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

/* ======================================================================
 * U4 -- resid encoding contract the D3-2 CP3 path composes (spec-5.22c
 * §2.3): owner->field4, seg->field1, block0->field2, gen->field3; the master
 * of an undo resid IS the encoded owner (owner-as-master, never a hash).  A
 * carried ITL-ref generation is preserved in field3 (Amendment-1).
 * ====================================================================== */
UT_TEST(test_undo_verdict_resid_encode_contract)
{
	ClusterResId rid;
	int32 owner = -1;
	uint32 seg = 0;
	uint32 block = 99;
	uint32 gen = 99;

	/* D3-2 encodes (origin, segment, block0, gen) */
	cluster_undo_resid_encode(2, 7, 0 /* block0 */, 0 /* gen, Amendment-1 neutral */, &rid);
	cluster_undo_resid_decode(&rid, &owner, &seg, &block, &gen);
	UT_ASSERT_EQ(owner, 2);
	UT_ASSERT_EQ(seg, 7);
	UT_ASSERT_EQ(block, 0);
	UT_ASSERT_EQ(gen, 0);

	/* owner-as-master: the master IS the owner, never a hash of the tag */
	UT_ASSERT_EQ(cluster_undo_resid_master(&rid), 2);

	/* a carried ITL-ref generation is preserved in field3 */
	cluster_undo_resid_encode(3, 9, 0, 5, &rid);
	cluster_undo_resid_decode(&rid, &owner, &seg, &block, &gen);
	UT_ASSERT_EQ(gen, 5);
	UT_ASSERT_EQ(cluster_undo_resid_master(&rid), 3);
}

/* ======================================================================
 * U6 -- version-coverage gate the D3-2 CP3 path applies on the S-grant
 * authority (D2-deferred, §3.5): hwm >= anchor + matching epoch -> covers;
 * hwm < anchor -> refuse (origin durable TT does not yet cover this tuple
 * version); epoch mismatch -> refuse (authority from a different reconfig
 * generation).  Fail-closed on every doubt (Rule 8.A).
 * ====================================================================== */
UT_TEST(test_undo_verdict_version_coverage_gate)
{
	ClusterLiveAuthority auth;
	uint64 epoch = 7;

	auth.origin_epoch = epoch;
	auth.tt_generation = 1;

	/* hwm >= anchor, epoch matches -> covers */
	auth.live_hwm_lsn = 2000;
	UT_ASSERT(cluster_vis_live_authority_covers_policy(1000, auth, epoch));

	/* hwm < anchor -> does not cover (fall-closed) */
	auth.live_hwm_lsn = 500;
	UT_ASSERT(!cluster_vis_live_authority_covers_policy(1000, auth, epoch));

	/* epoch mismatch -> refuse even with a covering hwm */
	auth.live_hwm_lsn = 2000;
	UT_ASSERT(!cluster_vis_live_authority_covers_policy(1000, auth, epoch + 1));
}

/* ======================================================================
 * U11 -- coherence-off fallback gate (spec-5.22c §3.4, Q2): the D3-2 CP3
 * path takes the D2 owner-as-master S-grant ONLY when armed (coherence on
 * AND peer-mode); every other combination keeps the 6.12i best-effort fetch
 * verbatim (回归安全 -- coherence off is byte-for-byte the old path).
 * ====================================================================== */
UT_TEST(test_undo_verdict_coherence_off_fallback)
{
	/* armed only when BOTH coherence and peer-mode hold */
	UT_ASSERT(cluster_undo_grant_armed(true /*coherence*/, true /*peer*/));

	/* coherence off -> not armed -> best-effort fallback (no regression) */
	UT_ASSERT(!cluster_undo_grant_armed(false /*coherence*/, true /*peer*/));

	/* single-node / non-peer -> not armed regardless of coherence */
	UT_ASSERT(!cluster_undo_grant_armed(true /*coherence*/, false /*peer*/));
	UT_ASSERT(!cluster_undo_grant_armed(false, false));
}

/* ======================================================================
 * U13 -- verdict_resolve outcome folding (spec-5.22c D3-3, Amendment-2):
 * the entry folds the runtime resolver's (ok, committed, scn, is_bound) into
 * the taxonomy.  !ok -> UNKNOWN_FAIL_CLOSED; committed & !is_bound -> EXACT;
 * committed & is_bound -> BOUND (never EXACT); !committed -> ABORTED.  This
 * is the point the legacy is_bound boolean is collapsed into the kind so a
 * consumer never sees the side axis.
 * ====================================================================== */
UT_TEST(test_undo_verdict_from_resolve_folding)
{
	ClusterUndoVerdictResult r;

	/* resolver failed (fetch/covers/deny miss) -> fail-closed */
	r = cluster_undo_verdict_from_resolve(false, false, InvalidScn, false);
	UT_ASSERT_EQ(r.kind, CLUSTER_UNDO_VERDICT_UNKNOWN_FAIL_CLOSED);

	/* committed, exact scn -> COMMITTED_EXACT{scn} */
	r = cluster_undo_verdict_from_resolve(true, true, 8000, false);
	UT_ASSERT_EQ(r.kind, CLUSTER_UNDO_VERDICT_COMMITTED_EXACT);
	UT_ASSERT_EQ((uint64)r.commit_scn, 8000);

	/* committed, bound only -> COMMITTED_BOUND{scn}, NEVER EXACT (Amendment-2) */
	r = cluster_undo_verdict_from_resolve(true, true, 9000, true);
	UT_ASSERT_EQ(r.kind, CLUSTER_UNDO_VERDICT_COMMITTED_BOUND);
	UT_ASSERT_EQ((uint64)r.commit_scn, 9000);
	UT_ASSERT_NE(r.kind, CLUSTER_UNDO_VERDICT_COMMITTED_EXACT);

	/* not committed -> ABORTED */
	r = cluster_undo_verdict_from_resolve(true, false, InvalidScn, false);
	UT_ASSERT_EQ(r.kind, CLUSTER_UNDO_VERDICT_ABORTED);
}

/* ======================================================================
 * U7 -- 8.A gate: a structurally malformed verdict page is never evidence
 * (spec-5.22c §3.2, Q7).  An EXACT kind with no commit_scn, a BELOW_HORIZON
 * carrying a stray exact scn, an unknown kind, or a wrong magic all fail
 * closed to UNKNOWN_FAIL_CLOSED -- never guessed committed.
 * ====================================================================== */
UT_TEST(test_undo_verdict_wire_page_malformed_fail_closed)
{
	ClusterGcsUndoVerdictPage v;
	ClusterUndoVerdictResult r;
	TransactionId xid = 555;

	/* EXACT kind but no commit_scn (unstamped) -> UNKNOWN */
	make_verdict_page(&v, CLUSTER_GCS_UNDO_VERDICT_COMMITTED_EXACT, xid, InvalidScn, InvalidScn, 0);
	r = cluster_undo_verdict_from_wire_page(&v, xid);
	UT_ASSERT_EQ(r.kind, CLUSTER_UNDO_VERDICT_UNKNOWN_FAIL_CLOSED);

	/* BELOW_HORIZON but carries a stray exact commit_scn -> UNKNOWN */
	make_verdict_page(&v, CLUSTER_GCS_UNDO_VERDICT_COMMITTED_BELOW_HORIZON, xid, 5000, 6000, 0);
	r = cluster_undo_verdict_from_wire_page(&v, xid);
	UT_ASSERT_EQ(r.kind, CLUSTER_UNDO_VERDICT_UNKNOWN_FAIL_CLOSED);

	/* unknown verdict kind (99) -> UNKNOWN (never guess) */
	make_verdict_page(&v, 99, xid, InvalidScn, InvalidScn, 0);
	r = cluster_undo_verdict_from_wire_page(&v, xid);
	UT_ASSERT_EQ(r.kind, CLUSTER_UNDO_VERDICT_UNKNOWN_FAIL_CLOSED);

	/* wrong magic (corrupt / foreign carrier) -> UNKNOWN */
	make_verdict_page(&v, CLUSTER_GCS_UNDO_VERDICT_COMMITTED_EXACT, xid, 5000, InvalidScn, 3);
	v.magic = 0xDEADBEEFu;
	r = cluster_undo_verdict_from_wire_page(&v, xid);
	UT_ASSERT_EQ(r.kind, CLUSTER_UNDO_VERDICT_UNKNOWN_FAIL_CLOSED);
}

/* ======================================================================
 * U8 -- segment-generation gate is structural, not the anti-ABA gate
 * (spec-5.22c Amendment-1, Q4).  D3 encodes rid.field3 == gen (its own known
 * value), so D2's generation_matches compares two equal caller values and is
 * structurally TRUE -- it is a real comparison (a genuine mismatch fails
 * closed), but D3 makes it always-true by construction, so it provides NO
 * independent anti-ABA proof on the requester path.
 * ====================================================================== */
UT_TEST(test_undo_verdict_generation_structural_not_anti_aba)
{
	ClusterResId rid;

	cluster_undo_resid_encode(2, 7, 0, 4, &rid);

	/* field3 == gen -> structurally passes (D3's construction) */
	UT_ASSERT(cluster_undo_resid_generation_matches(&rid, 4));

	/* it IS a real comparison: a genuine mismatch fails closed.  D3 chooses
	 * gen == field3 to make it pass, it does not disable the check. */
	UT_ASSERT(!cluster_undo_resid_generation_matches(&rid, 5));

	/* The real anti-ABA on the requester path is the slot-level xid+wrap
	 * positive proof (cluster_vis_tt_block_positive_proof), whose recycle /
	 * ambiguity truth table is covered by test_cluster_runtime_visibility and
	 * consumed verbatim by the D3-2 CP3 leg. */
}

/* ======================================================================
 * U10 -- IN_PROGRESS is reserved but never produced by D3 (spec-5.22c Q6,
 * §3.3).  The taxonomy keeps the value位 for a future write-path consumer,
 * but every D3 mapper folds an unproven-terminal outcome to UNKNOWN, never
 * IN_PROGRESS (cross-node has no origin-live ProcArray proof; conflating
 * "running" with "crash-lost" would breach 8.A).
 * ====================================================================== */
UT_TEST(test_undo_verdict_in_progress_never_produced)
{
	ClusterGcsUndoVerdictPage v;
	TransactionId xid = 321;

	UT_ASSERT_EQ((int)CLUSTER_UNDO_VERDICT_IN_PROGRESS, 4); /* value位 reserved */

	/* no wire kind maps to IN_PROGRESS (wire is EXACT/BELOW_HORIZON/ABORTED) */
	make_verdict_page(&v, CLUSTER_GCS_UNDO_VERDICT_COMMITTED_EXACT, xid, 5000, InvalidScn, 1);
	UT_ASSERT_NE(cluster_undo_verdict_from_wire_page(&v, xid).kind,
				 CLUSTER_UNDO_VERDICT_IN_PROGRESS);

	/* from_resolve never yields IN_PROGRESS across its whole domain */
	UT_ASSERT_NE(cluster_undo_verdict_from_resolve(false, false, InvalidScn, false).kind,
				 CLUSTER_UNDO_VERDICT_IN_PROGRESS);
	UT_ASSERT_NE(cluster_undo_verdict_from_resolve(true, true, 1, false).kind,
				 CLUSTER_UNDO_VERDICT_IN_PROGRESS);
	UT_ASSERT_NE(cluster_undo_verdict_from_resolve(true, true, 1, true).kind,
				 CLUSTER_UNDO_VERDICT_IN_PROGRESS);
	UT_ASSERT_NE(cluster_undo_verdict_from_resolve(true, false, InvalidScn, false).kind,
				 CLUSTER_UNDO_VERDICT_IN_PROGRESS);
}

/* ======================================================================
 * U12 -- the slot-wrap positive proof is the real anti-ABA, even when the
 * segment-generation gate is structurally true (spec-5.22c Amendment-1).  A
 * recycled segment changes the slot bytes so the xid+wrap proof comes back
 * NONE; D3 maps a NONE proof to UNKNOWN_FAIL_CLOSED (never committed).  The
 * proof truth table itself lives in test_cluster_runtime_visibility; here we
 * pin the D3 layer's fail-closed handling of a proof miss under a
 * structurally-true generation gate.
 * ====================================================================== */
UT_TEST(test_undo_verdict_slot_wrap_is_the_real_anti_aba)
{
	ClusterResId rid;
	ClusterUndoVerdictResult r;

	/* generation gate structurally true (does not weaken safety) ... */
	cluster_undo_resid_encode(2, 7, 0, 4, &rid);
	UT_ASSERT(cluster_undo_resid_generation_matches(&rid, 4));

	/* ... yet a recycled segment -> proof NONE -> D3 fail-closes to UNKNOWN */
	r = cluster_undo_verdict_from_block_proof(CLUSTER_VIS_TT_PROOF_NONE, InvalidScn, 0);
	UT_ASSERT_EQ(r.kind, CLUSTER_UNDO_VERDICT_UNKNOWN_FAIL_CLOSED);
}

int
main(void)
{
	UT_PLAN(11);
	UT_RUN(test_undo_verdict_from_wire_page_mapping);
	UT_RUN(test_undo_verdict_zero_value_is_fail_closed);
	UT_RUN(test_undo_verdict_from_block_proof_mapping);
	UT_RUN(test_undo_verdict_resid_encode_contract);
	UT_RUN(test_undo_verdict_version_coverage_gate);
	UT_RUN(test_undo_verdict_coherence_off_fallback);
	UT_RUN(test_undo_verdict_from_resolve_folding);
	UT_RUN(test_undo_verdict_wire_page_malformed_fail_closed);
	UT_RUN(test_undo_verdict_generation_structural_not_anti_aba);
	UT_RUN(test_undo_verdict_in_progress_never_produced);
	UT_RUN(test_undo_verdict_slot_wrap_is_the_real_anti_aba);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
