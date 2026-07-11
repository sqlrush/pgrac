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

#include "access/multixact.h"		   /* MultiXactStatus* member kinds (D3-b) */
#include "cluster/cluster_gcs_block.h" /* undo-fetch tag + auth trailer (CP2) */
#include "cluster/cluster_runtime_visibility.h"
#include "cluster/cluster_undo_segment.h" /* fake TT header blocks (CP3) */
#include "cluster/cluster_undo_verdict.h" /* ClusterUndoVerdictResult (D4-6) */

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

/* policy.o references scn_time_cmp (spec-7.1a D3); SCNs here are plain
 * monotonic test values, so the total-order stub is a raw compare. */
int
scn_time_cmp(SCN a, SCN b)
{
	if (a == b)
		return 0;
	return (a > b) ? 1 : -1;
}

#define LOCAL_EPOCH 7

static ClusterLiveAuthority
mk_auth(uint64 epoch, XLogRecPtr hwm, uint64 gen, SCN authority_scn)
{
	ClusterLiveAuthority a;

	a.origin_epoch = epoch;
	a.live_hwm_lsn = hwm;
	a.tt_generation = gen;
	a.authority_scn = authority_scn;
	return a;
}

/* All admit conditions hold -> resolve is permitted (the ONLY true). */
UT_TEST(test_covers_when_epoch_match_and_scn_ge_demand)
{
	ClusterLiveAuthority a = mk_auth(LOCAL_EPOCH, 5000, 42, (SCN)900);

	/* authority_scn (900) == demand (900): boundary-equal covers. */
	UT_ASSERT_EQ(cluster_vis_live_authority_covers_policy((SCN)900, a, LOCAL_EPOCH), true);
	/* authority_scn (900) > demand (450): covers. */
	UT_ASSERT_EQ(cluster_vis_live_authority_covers_policy((SCN)450, a, LOCAL_EPOCH), true);
}

/*
 * spec-7.1a D3 U2 (false-refuse regression): the former gate compared the
 * origin flush LSN against the tuple page LSN, refusing whenever the page
 * was last written by ANOTHER WAL thread with a numerically larger LSN
 * (gaps §C.2, 10/10 measured).  The LSN must not gate any more: a tiny
 * (but valid) hwm with a covering SCN admits.
 */
UT_TEST(test_covers_ignores_cross_thread_lsn)
{
	ClusterLiveAuthority a = mk_auth(LOCAL_EPOCH, 1 /* tiny but valid */, 42, (SCN)900);

	UT_ASSERT_EQ(cluster_vis_live_authority_covers_policy((SCN)900, a, LOCAL_EPOCH), true);
}

/* Authority from a different reconfig generation -> fail closed. */
UT_TEST(test_failclosed_when_epoch_differs)
{
	ClusterLiveAuthority older = mk_auth(LOCAL_EPOCH - 1, 9000, 42, (SCN)900);
	ClusterLiveAuthority newer = mk_auth(LOCAL_EPOCH + 1, 9000, 42, (SCN)900);

	UT_ASSERT_EQ(cluster_vis_live_authority_covers_policy((SCN)1, older, LOCAL_EPOCH), false);
	UT_ASSERT_EQ(cluster_vis_live_authority_covers_policy((SCN)1, newer, LOCAL_EPOCH), false);
}

/* No authority sampled (invalid hwm) -> fail closed, never guess. */
UT_TEST(test_failclosed_when_hwm_invalid)
{
	ClusterLiveAuthority a = mk_auth(LOCAL_EPOCH, InvalidXLogRecPtr, 42, (SCN)900);

	UT_ASSERT_EQ(cluster_vis_live_authority_covers_policy((SCN)1, a, LOCAL_EPOCH), false);
}

/* Older peer shipped no SCN co-sample (zero trailer) -> fail closed. */
UT_TEST(test_failclosed_when_authority_scn_absent)
{
	ClusterLiveAuthority a = mk_auth(LOCAL_EPOCH, 9000, 42, InvalidScn);

	UT_ASSERT_EQ(cluster_vis_live_authority_covers_policy((SCN)1, a, LOCAL_EPOCH), false);
}

/* Caller supplied no demand -> fail closed (never guess a demand). */
UT_TEST(test_failclosed_when_demand_invalid)
{
	ClusterLiveAuthority a = mk_auth(LOCAL_EPOCH, 9000, 42, (SCN)900);

	UT_ASSERT_EQ(cluster_vis_live_authority_covers_policy(InvalidScn, a, LOCAL_EPOCH), false);
}

/*
 * spec-7.1a D3 U2 (false-pass guard): origin clock BEHIND the demand ->
 * the shipped content is not provably conclusive for this demand; refuse
 * (the observe of the shipped SCN makes the retry self-heal).
 */
UT_TEST(test_failclosed_when_authority_scn_below_demand)
{
	ClusterLiveAuthority a = mk_auth(LOCAL_EPOCH, 9000, 42, (SCN)899);

	UT_ASSERT_EQ(cluster_vis_live_authority_covers_policy((SCN)900, a, LOCAL_EPOCH), false);
}

/*
 * Combined-doubt: epoch mismatch takes precedence even when the SCN would
 * otherwise cover -- proves conditions are ANDed, not ORed.
 */
UT_TEST(test_failclosed_epoch_mismatch_dominates_good_scn)
{
	ClusterLiveAuthority a = mk_auth(LOCAL_EPOCH + 3, 0xFFFFFFFF, 42, (SCN)9999);

	UT_ASSERT_EQ(cluster_vis_live_authority_covers_policy((SCN)1, a, LOCAL_EPOCH), false);
}

/*
 * CP2: origin_epoch is a FULL-WIDTH uint64 equality — an epoch differing
 * only above bit 32 must fail (guards the uint32 sketch upgrade: a
 * truncated comparison would alias these two and false-admit).
 */
UT_TEST(test_failclosed_epoch_differs_above_32bit)
{
	uint64 wide_epoch = ((uint64)1 << 32) + LOCAL_EPOCH;
	ClusterLiveAuthority a = mk_auth(wide_epoch, 9000, 42, (SCN)900);

	UT_ASSERT_EQ(cluster_vis_live_authority_covers_policy((SCN)1, a, LOCAL_EPOCH), false);
	/* Same wide epoch on both sides still admits (sanity). */
	UT_ASSERT_EQ(cluster_vis_live_authority_covers_policy((SCN)1, a, wide_epoch), true);
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
		/* spec-7.1a D3: the SCN carrier rides the former reserved bytes. */
		ClusterGcsUndoAuthTrailerSetAuthorityScn(&t, cases[i] ^ 0x5a5a5a5a5a5a5a5aULL);
		UT_ASSERT_EQ(ClusterGcsUndoAuthTrailerGetAuthorityScn(&t),
					 cases[i] ^ 0x5a5a5a5a5a5a5a5aULL);
		/* The two carriers are independent (no overlap). */
		UT_ASSERT_EQ(ClusterGcsUndoAuthTrailerGetTtGeneration(&t), cases[i]);
	}
	/* Zeroed trailer reads as InvalidScn (older-peer fail-closed signal). */
	memset(&t, 0, sizeof(t));
	UT_ASSERT_EQ(ClusterGcsUndoAuthTrailerGetAuthorityScn(&t), 0);
}

/* ============================================================
 * CP3: positive-proof scan truth table (cluster_vis_tt_block_positive_proof)
 * ============================================================ */

#define PROOF_SEG 7
#define PROOF_OWNER 2 /* origin node 1 -> owner_instance 2 */

static PGAlignedBlock proof_block;

static UndoSegmentHeaderData *
mk_header(uint32 segment_id, uint8 owner, uint16 nslots)
{
	UndoSegmentHeaderData *hdr = (UndoSegmentHeaderData *)proof_block.data;

	memset(proof_block.data, 0, BLCKSZ);
	hdr->segment_id = segment_id;
	hdr->owner_instance = owner;
	hdr->tt_slots_count = nslots;
	return hdr;
}

static void
set_slot(UndoSegmentHeaderData *hdr, int i, TransactionId xid, uint16 wrap, uint8 status, SCN scn)
{
	hdr->tt_slots[i].xid = xid;
	hdr->tt_slots[i].wrap = wrap;
	hdr->tt_slots[i].status = status;
	hdr->tt_slots[i].commit_scn = scn;
}

/* Exactly-one terminal match resolves; UNUSED same-xid bytes are ignored. */
UT_TEST(test_ttproof_committed_and_aborted)
{
	UndoSegmentHeaderData *hdr = mk_header(PROOF_SEG, PROOF_OWNER, TT_SLOTS_PER_SEGMENT);
	SCN scn = InvalidScn;
	uint16 wrap = 0;

	set_slot(hdr, 3, 1000, 5, (uint8)TT_SLOT_COMMITTED, (SCN)777);
	set_slot(hdr, 9, 2000, 2, (uint8)TT_SLOT_ABORTED, InvalidScn);
	/* UNUSED slot carrying the same xid bytes must NOT count as residue. */
	set_slot(hdr, 11, 1000, 9, (uint8)TT_SLOT_UNUSED, InvalidScn);

	UT_ASSERT_EQ(cluster_vis_tt_block_positive_proof(proof_block.data, PROOF_SEG, PROOF_OWNER, 1000,
													 &scn, &wrap),
				 CLUSTER_VIS_TT_PROOF_COMMITTED);
	UT_ASSERT_EQ(scn, (SCN)777);
	UT_ASSERT_EQ(wrap, 5);

	UT_ASSERT_EQ(cluster_vis_tt_block_positive_proof(proof_block.data, PROOF_SEG, PROOF_OWNER, 2000,
													 &scn, &wrap),
				 CLUSTER_VIS_TT_PROOF_ABORTED);
	UT_ASSERT_EQ(wrap, 2);
}

/* spec-7.1a hardening (C1b): PROOF_COMMITTED is EVIDENCE, not a verdict --
 * the consumer routes it to the origin verdict leg and takes NO payload from
 * the stamp (a durable COMMITTED stamp lands at 2PC pre-commit, so a
 * stamped-then-crashed xid is in-doubt).  Pin the NULL-out-param consumer
 * shape: the proof classification itself is unchanged and payload pointers
 * are optional. */
UT_TEST(test_ttproof_committed_is_evidence_not_verdict)
{
	UndoSegmentHeaderData *hdr = mk_header(PROOF_SEG, PROOF_OWNER, TT_SLOTS_PER_SEGMENT);

	set_slot(hdr, 3, 1000, 5, (uint8)TT_SLOT_COMMITTED, (SCN)777);

	/* The hardened fast leg passes NULL/NULL: classification only. */
	UT_ASSERT_EQ(cluster_vis_tt_block_positive_proof(proof_block.data, PROOF_SEG, PROOF_OWNER, 1000,
													 NULL, NULL),
				 CLUSTER_VIS_TT_PROOF_COMMITTED);
}

/* spec-5.22c/5.22f Hardening (root-cause-#1 integration review, #1) --
 * "recycled-ref-disguised-as-fresh".  A slot recycled and reused by the SAME
 * raw xid bytes but a BUMPED wrap, carrying a COMMITTED stamp for a DIFFERENT
 * commit, still classifies COMMITTED: the scan matches on xid and REPORTS the
 * slot's current wrap/scn -- it does NOT compare against the ref's expected
 * wrap (it has none to compare).  So a single-block COMMITTED proof is EVIDENCE
 * only; the anti-ABA belongs to the caller, applied downstream (D4
 * complete-scan uniqueness, or the CP5 verdict leg's wrap-suspect gate -- see
 * test_cluster_tt_durable test_wrap_suspect_below_horizon_unreliable_is_suspect
 * and the C1b routing pinned by t/365 L4 / t/359 L3).  This case (same xid,
 * bumped wrap) is distinct from the recycled DIFFERENT-xid -> NONE case pinned
 * by test_cluster_undo_verdict U12: here the proof is COMMITTED but the
 * reported scn/wrap are the RECYCLED commit's, so trusting them terminally
 * would false-commit.  Pin the report-not-gate contract so no future change
 * lets the fast leg conclude EXACT from a proof's COMMITTED. */
UT_TEST(test_ttproof_recycled_same_xid_reports_bumped_wrap_not_gated)
{
	UndoSegmentHeaderData *hdr = mk_header(PROOF_SEG, PROOF_OWNER, TT_SLOTS_PER_SEGMENT);
	SCN scn = InvalidScn;
	uint16 wrap = 0;

	/* A fresh ref bound (xid 1000, wrap 5); the slot's wrap was bumped to 99 by
	 * a whole-segment recycle, then the bytes reused by the same raw xid with a
	 * COMMITTED stamp for an unrelated commit (scn 9999). */
	set_slot(hdr, 4, 1000, 99, (uint8)TT_SLOT_COMMITTED, (SCN)9999);

	/* Still COMMITTED (matches on xid), and it reports the CURRENT slot wrap
	 * (99) and scn (9999) -- the recycled commit's, NOT the ref's (wrap 5).
	 * The scan cannot self-detect the ABA; the caller must route to the verdict
	 * leg so the wrap-suspect gate can refuse. */
	UT_ASSERT_EQ(cluster_vis_tt_block_positive_proof(proof_block.data, PROOF_SEG, PROOF_OWNER, 1000,
													 &scn, &wrap),
				 CLUSTER_VIS_TT_PROOF_COMMITTED);
	UT_ASSERT_EQ(wrap, 99);
	UT_ASSERT_EQ((uint64)scn, 9999);
}

/* 0-match / ACTIVE / COMMITTED-without-scn: never a proof (user boundary:
 * a single fetched block's 0-match is NOT recycled/aborted evidence). */
UT_TEST(test_ttproof_zero_match_active_invalid_scn)
{
	UndoSegmentHeaderData *hdr = mk_header(PROOF_SEG, PROOF_OWNER, TT_SLOTS_PER_SEGMENT);
	SCN scn = (SCN)1;
	uint16 wrap = 1;

	set_slot(hdr, 0, 1000, 1, (uint8)TT_SLOT_ACTIVE, InvalidScn);
	set_slot(hdr, 1, 2000, 1, (uint8)TT_SLOT_COMMITTED, InvalidScn); /* unstamped */

	/* 0-match: xid 3000 nowhere in this block. */
	UT_ASSERT_EQ(cluster_vis_tt_block_positive_proof(proof_block.data, PROOF_SEG, PROOF_OWNER, 3000,
													 &scn, &wrap),
				 CLUSTER_VIS_TT_PROOF_NONE);
	UT_ASSERT_EQ(scn, InvalidScn); /* out-params reset on refusal */
	/* single ACTIVE match: in progress, not terminal. */
	UT_ASSERT_EQ(cluster_vis_tt_block_positive_proof(proof_block.data, PROOF_SEG, PROOF_OWNER, 1000,
													 NULL, NULL),
				 CLUSTER_VIS_TT_PROOF_NONE);
	/* single COMMITTED match without a valid commit_scn: unstamped, refuse. */
	UT_ASSERT_EQ(cluster_vis_tt_block_positive_proof(proof_block.data, PROOF_SEG, PROOF_OWNER, 2000,
													 NULL, NULL),
				 CLUSTER_VIS_TT_PROOF_NONE);
}

/* Same-xid residue (RECYCLABLE counts) and an unparseable status byte both
 * poison the verdict — defense in depth over the one-era argument. */
UT_TEST(test_ttproof_ambiguity_and_garbage)
{
	UndoSegmentHeaderData *hdr = mk_header(PROOF_SEG, PROOF_OWNER, TT_SLOTS_PER_SEGMENT);

	set_slot(hdr, 3, 1000, 5, (uint8)TT_SLOT_COMMITTED, (SCN)777);
	set_slot(hdr, 7, 1000, 4, (uint8)TT_SLOT_RECYCLABLE, (SCN)555);
	UT_ASSERT_EQ(cluster_vis_tt_block_positive_proof(proof_block.data, PROOF_SEG, PROOF_OWNER, 1000,
													 NULL, NULL),
				 CLUSTER_VIS_TT_PROOF_NONE);

	/* Garbage status ANYWHERE refuses even an otherwise-clean match. */
	hdr = mk_header(PROOF_SEG, PROOF_OWNER, TT_SLOTS_PER_SEGMENT);
	set_slot(hdr, 3, 1000, 5, (uint8)TT_SLOT_COMMITTED, (SCN)777);
	set_slot(hdr, 40, 4000, 1, (uint8)7, InvalidScn);
	UT_ASSERT_EQ(cluster_vis_tt_block_positive_proof(proof_block.data, PROOF_SEG, PROOF_OWNER, 1000,
													 NULL, NULL),
				 CLUSTER_VIS_TT_PROOF_NONE);
}

/* spec-5.22d A1 (D4-8): the scan CORE under the positive-proof wrapper.
 * Same parse discipline, but nmatch is reported (the cross-segment
 * aggregation needs it) and unparseable bytes are a distinct POISONED
 * status (the aggregate must refuse-all, never skip-and-continue). */
UT_TEST(test_tt_block_xid_scan_core)
{
	UndoSegmentHeaderData *hdr = mk_header(PROOF_SEG, PROOF_OWNER, TT_SLOTS_PER_SEGMENT);
	int nmatch = -1;
	ClusterVisTtProof proof = CLUSTER_VIS_TT_PROOF_COMMITTED;
	SCN scn = (SCN)1;
	uint16 wrap = 1;

	/* OK + 0 matches: parseable block, no evidence; outs reset. */
	UT_ASSERT_EQ(cluster_vis_tt_block_xid_scan(proof_block.data, PROOF_SEG, PROOF_OWNER, 3000,
											   &nmatch, &proof, &scn, &wrap),
				 CLUSTER_VIS_TT_BLOCK_SCAN_OK);
	UT_ASSERT_EQ(nmatch, 0);
	UT_ASSERT_EQ(proof, CLUSTER_VIS_TT_PROOF_NONE);
	UT_ASSERT_EQ(scn, InvalidScn);

	/* OK + unique COMMITTED: terminal, scn/wrap out. */
	set_slot(hdr, 3, 1000, 5, (uint8)TT_SLOT_COMMITTED, (SCN)777);
	UT_ASSERT_EQ(cluster_vis_tt_block_xid_scan(proof_block.data, PROOF_SEG, PROOF_OWNER, 1000,
											   &nmatch, &proof, &scn, &wrap),
				 CLUSTER_VIS_TT_BLOCK_SCAN_OK);
	UT_ASSERT_EQ(nmatch, 1);
	UT_ASSERT_EQ(proof, CLUSTER_VIS_TT_PROOF_COMMITTED);
	UT_ASSERT_EQ(scn, (SCN)777);
	UT_ASSERT_EQ(wrap, 5);

	/* OK + unique ABORTED: terminal. */
	set_slot(hdr, 9, 2000, 2, (uint8)TT_SLOT_ABORTED, InvalidScn);
	UT_ASSERT_EQ(cluster_vis_tt_block_xid_scan(proof_block.data, PROOF_SEG, PROOF_OWNER, 2000,
											   &nmatch, &proof, &scn, &wrap),
				 CLUSTER_VIS_TT_BLOCK_SCAN_OK);
	UT_ASSERT_EQ(nmatch, 1);
	UT_ASSERT_EQ(proof, CLUSTER_VIS_TT_PROOF_ABORTED);
	UT_ASSERT_EQ(wrap, 2);

	/* OK + unique ACTIVE: found but NOT terminal (in-doubt) — nmatch says
	 * "evidence lives here", proof says "not provable". */
	set_slot(hdr, 12, 2500, 1, (uint8)TT_SLOT_ACTIVE, InvalidScn);
	UT_ASSERT_EQ(cluster_vis_tt_block_xid_scan(proof_block.data, PROOF_SEG, PROOF_OWNER, 2500,
											   &nmatch, &proof, &scn, &wrap),
				 CLUSTER_VIS_TT_BLOCK_SCAN_OK);
	UT_ASSERT_EQ(nmatch, 1);
	UT_ASSERT_EQ(proof, CLUSTER_VIS_TT_PROOF_NONE);

	/* OK + 2 matches inside one block (RECYCLABLE residue counts). */
	set_slot(hdr, 7, 1000, 4, (uint8)TT_SLOT_RECYCLABLE, (SCN)555);
	UT_ASSERT_EQ(cluster_vis_tt_block_xid_scan(proof_block.data, PROOF_SEG, PROOF_OWNER, 1000,
											   &nmatch, &proof, &scn, &wrap),
				 CLUSTER_VIS_TT_BLOCK_SCAN_OK);
	UT_ASSERT_EQ(nmatch, 2);
	UT_ASSERT_EQ(proof, CLUSTER_VIS_TT_PROOF_NONE);
	UT_ASSERT_EQ(scn, InvalidScn);

	/* POISONED: header identity mismatch (segment / owner), count over. */
	UT_ASSERT_EQ(cluster_vis_tt_block_xid_scan(proof_block.data, PROOF_SEG + 1, PROOF_OWNER, 1000,
											   &nmatch, &proof, &scn, &wrap),
				 CLUSTER_VIS_TT_BLOCK_SCAN_POISONED);
	UT_ASSERT_EQ(cluster_vis_tt_block_xid_scan(proof_block.data, PROOF_SEG, PROOF_OWNER + 1, 1000,
											   &nmatch, &proof, &scn, &wrap),
				 CLUSTER_VIS_TT_BLOCK_SCAN_POISONED);
	hdr->tt_slots_count = TT_SLOTS_PER_SEGMENT + 1;
	UT_ASSERT_EQ(cluster_vis_tt_block_xid_scan(proof_block.data, PROOF_SEG, PROOF_OWNER, 1000,
											   &nmatch, &proof, &scn, &wrap),
				 CLUSTER_VIS_TT_BLOCK_SCAN_POISONED);
	hdr->tt_slots_count = TT_SLOTS_PER_SEGMENT;

	/* POISONED: a garbage status byte anywhere poisons the whole block,
	 * whatever xid is asked. */
	set_slot(hdr, 40, 4000, 1, (uint8)7, InvalidScn);
	UT_ASSERT_EQ(cluster_vis_tt_block_xid_scan(proof_block.data, PROOF_SEG, PROOF_OWNER, 2000,
											   &nmatch, &proof, &scn, &wrap),
				 CLUSTER_VIS_TT_BLOCK_SCAN_POISONED);
	set_slot(hdr, 40, 0, 0, (uint8)TT_SLOT_UNUSED, InvalidScn);

	/* POISONED: caller-bug inputs refuse-all (NULL block / invalid xid). */
	UT_ASSERT_EQ(cluster_vis_tt_block_xid_scan(NULL, PROOF_SEG, PROOF_OWNER, 1000, &nmatch, &proof,
											   &scn, &wrap),
				 CLUSTER_VIS_TT_BLOCK_SCAN_POISONED);
	UT_ASSERT_EQ(cluster_vis_tt_block_xid_scan(proof_block.data, PROOF_SEG, PROOF_OWNER,
											   InvalidTransactionId, &nmatch, &proof, &scn, &wrap),
				 CLUSTER_VIS_TT_BLOCK_SCAN_POISONED);
}

/* Header identity mismatches: not provably the asked-for TT -> refuse. */
UT_TEST(test_ttproof_header_mismatch)
{
	UndoSegmentHeaderData *hdr = mk_header(PROOF_SEG, PROOF_OWNER, TT_SLOTS_PER_SEGMENT);

	set_slot(hdr, 3, 1000, 5, (uint8)TT_SLOT_COMMITTED, (SCN)777);

	/* wrong segment id */
	UT_ASSERT_EQ(cluster_vis_tt_block_positive_proof(proof_block.data, PROOF_SEG + 1, PROOF_OWNER,
													 1000, NULL, NULL),
				 CLUSTER_VIS_TT_PROOF_NONE);
	/* wrong owner instance */
	UT_ASSERT_EQ(cluster_vis_tt_block_positive_proof(proof_block.data, PROOF_SEG, PROOF_OWNER + 1,
													 1000, NULL, NULL),
				 CLUSTER_VIS_TT_PROOF_NONE);
	/* over-range slot count */
	hdr->tt_slots_count = TT_SLOTS_PER_SEGMENT + 1;
	UT_ASSERT_EQ(cluster_vis_tt_block_positive_proof(proof_block.data, PROOF_SEG, PROOF_OWNER, 1000,
													 NULL, NULL),
				 CLUSTER_VIS_TT_PROOF_NONE);
}

/* CP5 (D-i4): verdict-page structural validation truth table — every field
 * must be exactly consistent with the claimed kind, or the page is refused
 * (the caller keeps 53R97). */
UT_TEST(test_undo_verdict_page_usable)
{
	ClusterGcsUndoVerdictPage v;

	/* Baseline good EXACT page. */
	memset(&v, 0, sizeof(v));
	v.magic = CLUSTER_GCS_UNDO_VERDICT_MAGIC;
	v.version = CLUSTER_GCS_UNDO_VERDICT_VERSION;
	v.xid_echo = 1000;
	v.verdict = (uint8)CLUSTER_GCS_UNDO_VERDICT_COMMITTED_EXACT;
	v.commit_scn = 777;
	v.horizon_scn = InvalidScn;
	v.wrap = 5;
	UT_ASSERT_EQ(cluster_vis_undo_verdict_page_usable(&v, 1000), true);

	/* NULL page / invalid asked xid. */
	UT_ASSERT_EQ(cluster_vis_undo_verdict_page_usable(NULL, 1000), false);
	UT_ASSERT_EQ(cluster_vis_undo_verdict_page_usable(&v, InvalidTransactionId), false);

	/* Wrong magic / version / echo (incl. a widened echo with high bits). */
	v.magic = 0xDEADBEEF;
	UT_ASSERT_EQ(cluster_vis_undo_verdict_page_usable(&v, 1000), false);
	v.magic = CLUSTER_GCS_UNDO_VERDICT_MAGIC;
	v.version = 2;
	UT_ASSERT_EQ(cluster_vis_undo_verdict_page_usable(&v, 1000), false);
	v.version = CLUSTER_GCS_UNDO_VERDICT_VERSION;
	UT_ASSERT_EQ(cluster_vis_undo_verdict_page_usable(&v, 1001), false);
	v.xid_echo = (((uint64)1) << 32) + 1000; /* high word poisons the echo */
	UT_ASSERT_EQ(cluster_vis_undo_verdict_page_usable(&v, 1000), false);
	v.xid_echo = 1000;

	/* Non-zero reserved bytes poison the page. */
	v.reserved_0[3] = 1;
	UT_ASSERT_EQ(cluster_vis_undo_verdict_page_usable(&v, 1000), false);
	v.reserved_0[3] = 0;
	v.reserved_1[0] = 1;
	UT_ASSERT_EQ(cluster_vis_undo_verdict_page_usable(&v, 1000), false);
	v.reserved_1[0] = 0;

	/* EXACT kind-field consistency: needs commit_scn, refuses a horizon. */
	v.commit_scn = InvalidScn;
	UT_ASSERT_EQ(cluster_vis_undo_verdict_page_usable(&v, 1000), false);
	v.commit_scn = 777;
	v.horizon_scn = 500;
	UT_ASSERT_EQ(cluster_vis_undo_verdict_page_usable(&v, 1000), false);
	v.horizon_scn = InvalidScn;

	/* BELOW_HORIZON: needs a horizon, refuses an exact scn (a stray
	 * commit_scn could leak into stamp/cache paths). */
	v.verdict = (uint8)CLUSTER_GCS_UNDO_VERDICT_COMMITTED_BELOW_HORIZON;
	v.commit_scn = InvalidScn;
	v.horizon_scn = 500;
	UT_ASSERT_EQ(cluster_vis_undo_verdict_page_usable(&v, 1000), true);
	v.commit_scn = 777;
	UT_ASSERT_EQ(cluster_vis_undo_verdict_page_usable(&v, 1000), false);
	v.commit_scn = InvalidScn;
	v.horizon_scn = InvalidScn;
	UT_ASSERT_EQ(cluster_vis_undo_verdict_page_usable(&v, 1000), false);

	/* ABORTED: carries no scn of any kind. */
	v.verdict = (uint8)CLUSTER_GCS_UNDO_VERDICT_ABORTED;
	UT_ASSERT_EQ(cluster_vis_undo_verdict_page_usable(&v, 1000), true);
	v.commit_scn = 777;
	UT_ASSERT_EQ(cluster_vis_undo_verdict_page_usable(&v, 1000), false);
	v.commit_scn = InvalidScn;
	v.horizon_scn = 500;
	UT_ASSERT_EQ(cluster_vis_undo_verdict_page_usable(&v, 1000), false);
	v.horizon_scn = InvalidScn;

	/* Unknown kinds refuse (0 and one past the last known). */
	v.verdict = 0;
	UT_ASSERT_EQ(cluster_vis_undo_verdict_page_usable(&v, 1000), false);
	v.verdict = (uint8)CLUSTER_GCS_UNDO_VERDICT_ABORTED + 1;
	UT_ASSERT_EQ(cluster_vis_undo_verdict_page_usable(&v, 1000), false);
}

/* spec-5.22d D4-6: structural validation of an AUTHORITY-served verdict page
 * (version 2 provenance).  Same field discipline as the v1 table above, with
 * two deliberate tightenings: ONLY version 2 is evidence (a v1 owner-served
 * page can never masquerade as an authority serve, and vice versa — the v1
 * gate's strict ==1 already refuses version 2, pinned above), and the
 * BELOW_HORIZON kind is refused outright — the authority block0 prove has no
 * horizon leg, so a bound-carrying page is malformed by construction. */
UT_TEST(test_undo_authority_verdict_page_usable)
{
	ClusterGcsUndoVerdictPage v;

	/* Baseline good authority EXACT page. */
	memset(&v, 0, sizeof(v));
	v.magic = CLUSTER_GCS_UNDO_VERDICT_MAGIC;
	v.version = CLUSTER_GCS_UNDO_VERDICT_VERSION_AUTHORITY;
	v.xid_echo = 1000;
	v.verdict = (uint8)CLUSTER_GCS_UNDO_VERDICT_COMMITTED_EXACT;
	v.commit_scn = 777;
	v.horizon_scn = InvalidScn;
	v.wrap = 5;
	UT_ASSERT_EQ(cluster_vis_undo_authority_verdict_page_usable(&v, 1000), true);

	/* NULL page / invalid asked xid. */
	UT_ASSERT_EQ(cluster_vis_undo_authority_verdict_page_usable(NULL, 1000), false);
	UT_ASSERT_EQ(cluster_vis_undo_authority_verdict_page_usable(&v, InvalidTransactionId), false);

	/* An owner-served v1 page is NOT authority evidence. */
	v.version = CLUSTER_GCS_UNDO_VERDICT_VERSION;
	UT_ASSERT_EQ(cluster_vis_undo_authority_verdict_page_usable(&v, 1000), false);
	v.version = CLUSTER_GCS_UNDO_VERDICT_VERSION_AUTHORITY;

	/* Wrong magic / echo (incl. poisoned high word). */
	v.magic = 0xDEADBEEF;
	UT_ASSERT_EQ(cluster_vis_undo_authority_verdict_page_usable(&v, 1000), false);
	v.magic = CLUSTER_GCS_UNDO_VERDICT_MAGIC;
	UT_ASSERT_EQ(cluster_vis_undo_authority_verdict_page_usable(&v, 1001), false);
	v.xid_echo = (((uint64)1) << 32) + 1000;
	UT_ASSERT_EQ(cluster_vis_undo_authority_verdict_page_usable(&v, 1000), false);
	v.xid_echo = 1000;

	/* Non-zero reserved bytes poison the page. */
	v.reserved_0[5] = 1;
	UT_ASSERT_EQ(cluster_vis_undo_authority_verdict_page_usable(&v, 1000), false);
	v.reserved_0[5] = 0;
	v.reserved_1[2] = 1;
	UT_ASSERT_EQ(cluster_vis_undo_authority_verdict_page_usable(&v, 1000), false);
	v.reserved_1[2] = 0;

	/* EXACT kind-field consistency: needs commit_scn, refuses a horizon. */
	v.commit_scn = InvalidScn;
	UT_ASSERT_EQ(cluster_vis_undo_authority_verdict_page_usable(&v, 1000), false);
	v.commit_scn = 777;
	v.horizon_scn = 500;
	UT_ASSERT_EQ(cluster_vis_undo_authority_verdict_page_usable(&v, 1000), false);
	v.horizon_scn = InvalidScn;

	/* BELOW_HORIZON: refused OUTRIGHT on the authority leg (no horizon leg
	 * in the block0 prove), even in its v1-legal shape. */
	v.verdict = (uint8)CLUSTER_GCS_UNDO_VERDICT_COMMITTED_BELOW_HORIZON;
	v.commit_scn = InvalidScn;
	v.horizon_scn = 500;
	UT_ASSERT_EQ(cluster_vis_undo_authority_verdict_page_usable(&v, 1000), false);
	v.horizon_scn = InvalidScn;

	/* ABORTED: carries no scn of any kind. */
	v.verdict = (uint8)CLUSTER_GCS_UNDO_VERDICT_ABORTED;
	UT_ASSERT_EQ(cluster_vis_undo_authority_verdict_page_usable(&v, 1000), true);
	v.commit_scn = 777;
	UT_ASSERT_EQ(cluster_vis_undo_authority_verdict_page_usable(&v, 1000), false);
	v.commit_scn = InvalidScn;

	/* Unknown kinds refuse. */
	v.verdict = 0;
	UT_ASSERT_EQ(cluster_vis_undo_authority_verdict_page_usable(&v, 1000), false);
	v.verdict = (uint8)CLUSTER_GCS_UNDO_VERDICT_ABORTED + 1;
	UT_ASSERT_EQ(cluster_vis_undo_authority_verdict_page_usable(&v, 1000), false);
}

/* spec-5.22d D4-6 (user 8.A amend #1): the authority reply is evidence ONLY
 * under the full binding — the reply sender IS the elected authority and the
 * reply epoch IS the stamped request epoch, exactly.  The transport's HC100
 * >= check is a general pre-filter and deliberately NOT sufficient here; the
 * epoch±1 rows pin the strict equality. */
UT_TEST(test_undo_authority_reply_binding)
{
	/* the one admissible shape */
	UT_ASSERT_EQ(cluster_vis_undo_authority_reply_binding_ok(3, 3, 42, 42), true);

	/* wrong sender: a peer that is not the elected authority (incl. the
	 * dead owner's id and a negative junk id) is never evidence */
	UT_ASSERT_EQ(cluster_vis_undo_authority_reply_binding_ok(2, 3, 42, 42), false);
	UT_ASSERT_EQ(cluster_vis_undo_authority_reply_binding_ok(4, 3, 42, 42), false);
	UT_ASSERT_EQ(cluster_vis_undo_authority_reply_binding_ok(-1, 3, 42, 42), false);

	/* invalid elected authority: nothing can bind to it */
	UT_ASSERT_EQ(cluster_vis_undo_authority_reply_binding_ok(3, -1, 42, 42), false);
	UT_ASSERT_EQ(cluster_vis_undo_authority_reply_binding_ok(-1, -1, 42, 42), false);

	/* epoch must match EXACTLY: ±1 both refuse (HC100 >= would admit +1) */
	UT_ASSERT_EQ(cluster_vis_undo_authority_reply_binding_ok(3, 3, 41, 42), false);
	UT_ASSERT_EQ(cluster_vis_undo_authority_reply_binding_ok(3, 3, 43, 42), false);
}

/* spec-5.22d D4-6: serve-side page fill -> requester-side accept closes the
 * loop over one prove result, so the two wire ends can never disagree about
 * what an authority page carries.  fill refuses every kind the block0 prove
 * cannot legitimately produce (UNKNOWN, BOUND, IN_PROGRESS). */
UT_TEST(test_undo_authority_verdict_page_fill_roundtrip)
{
	ClusterGcsUndoVerdictPage v;
	ClusterUndoVerdictResult r;
	ClusterUndoVerdictResult mapped;

	/* COMMITTED_EXACT roundtrip: scn + wrap survive, version is 2. */
	memset(&v, 0xAA, sizeof(v)); /* poison: fill must overwrite everything */
	r.kind = (uint8)CLUSTER_UNDO_VERDICT_COMMITTED_EXACT;
	r.commit_scn = 777;
	r.wrap = 5;
	UT_ASSERT_EQ(cluster_undo_authority_verdict_page_fill(&v, 1000, &r), true);
	UT_ASSERT_EQ(v.version, CLUSTER_GCS_UNDO_VERDICT_VERSION_AUTHORITY);
	UT_ASSERT_EQ(cluster_vis_undo_authority_verdict_page_usable(&v, 1000), true);
	mapped = cluster_undo_verdict_from_authority_wire_page(&v, 1000);
	UT_ASSERT_EQ(mapped.kind, (uint8)CLUSTER_UNDO_VERDICT_COMMITTED_EXACT);
	UT_ASSERT_EQ((long long)mapped.commit_scn, 777);
	UT_ASSERT_EQ(mapped.wrap, 5);

	/* ABORTED roundtrip: no scn of any kind on the page. */
	memset(&v, 0xAA, sizeof(v));
	r.kind = (uint8)CLUSTER_UNDO_VERDICT_ABORTED;
	r.commit_scn = InvalidScn;
	r.wrap = 3;
	UT_ASSERT_EQ(cluster_undo_authority_verdict_page_fill(&v, 1000, &r), true);
	UT_ASSERT_EQ(cluster_vis_undo_authority_verdict_page_usable(&v, 1000), true);
	mapped = cluster_undo_verdict_from_authority_wire_page(&v, 1000);
	UT_ASSERT_EQ(mapped.kind, (uint8)CLUSTER_UNDO_VERDICT_ABORTED);
	UT_ASSERT_EQ((long long)mapped.commit_scn, (long long)InvalidScn);

	/* fill refuses what the prove cannot produce: UNKNOWN / BOUND /
	 * IN_PROGRESS never become a wire page. */
	r.kind = (uint8)CLUSTER_UNDO_VERDICT_UNKNOWN_FAIL_CLOSED;
	r.commit_scn = InvalidScn;
	UT_ASSERT_EQ(cluster_undo_authority_verdict_page_fill(&v, 1000, &r), false);
	r.kind = (uint8)CLUSTER_UNDO_VERDICT_COMMITTED_BOUND;
	r.commit_scn = 500;
	UT_ASSERT_EQ(cluster_undo_authority_verdict_page_fill(&v, 1000, &r), false);
	r.kind = (uint8)CLUSTER_UNDO_VERDICT_IN_PROGRESS;
	r.commit_scn = InvalidScn;
	UT_ASSERT_EQ(cluster_undo_authority_verdict_page_fill(&v, 1000, &r), false);

	/* EXACT without a valid scn is not fillable (nothing to prove). */
	r.kind = (uint8)CLUSTER_UNDO_VERDICT_COMMITTED_EXACT;
	r.commit_scn = InvalidScn;
	UT_ASSERT_EQ(cluster_undo_authority_verdict_page_fill(&v, 1000, &r), false);

	/* fill guards its inputs: NULL page / invalid xid. */
	r.kind = (uint8)CLUSTER_UNDO_VERDICT_COMMITTED_EXACT;
	r.commit_scn = 777;
	UT_ASSERT_EQ(cluster_undo_authority_verdict_page_fill(NULL, 1000, &r), false);
	UT_ASSERT_EQ(cluster_undo_authority_verdict_page_fill(&v, InvalidTransactionId, &r), false);
	UT_ASSERT_EQ(cluster_undo_authority_verdict_page_fill(&v, 1000, NULL), false);

	/* the mapper refuses a v1 owner-served page (wrong provenance). */
	memset(&v, 0, sizeof(v));
	v.magic = CLUSTER_GCS_UNDO_VERDICT_MAGIC;
	v.version = CLUSTER_GCS_UNDO_VERDICT_VERSION;
	v.xid_echo = 1000;
	v.verdict = (uint8)CLUSTER_GCS_UNDO_VERDICT_COMMITTED_EXACT;
	v.commit_scn = 777;
	v.horizon_scn = InvalidScn;
	mapped = cluster_undo_verdict_from_authority_wire_page(&v, 1000);
	UT_ASSERT_EQ(mapped.kind, (uint8)CLUSTER_UNDO_VERDICT_UNKNOWN_FAIL_CLOSED);
	UT_ASSERT_EQ((long long)mapped.commit_scn, (long long)InvalidScn);
}

/* spec-7.1 D3-b: structural validation of a batched multixact member-verdict
 * page.  true only for a SERVED page whose every member is internally
 * consistent (lock-only carry no verdict/scn; updaters carry a known verdict
 * whose scn fields match the kind), or the page is refused (caller keeps
 * 53R97).  Covers the batch boundaries 1 / 2 / MAX / MAX+1 (Rule 8.A). */
UT_TEST(test_undo_multi_verdict_page_usable)
{
	PGAlignedBlock blk;
	ClusterGcsUndoMultiVerdictPage *v = (ClusterGcsUndoMultiVerdictPage *)blk.data;
	MultiXactId asked = 4242;
	int i;

	/* Baseline good SERVED page: 1 updater EXACT + 1 lock-only. */
	memset(blk.data, 0, BLCKSZ);
	v->magic = CLUSTER_GCS_UNDO_MULTI_VERDICT_MAGIC;
	v->version = CLUSTER_GCS_UNDO_MULTI_VERDICT_VERSION;
	v->mxid_echo = asked;
	v->status = (uint8)CLUSTER_GCS_UNDO_MULTI_VERDICT_SERVED;
	v->nmembers = 2;
	v->members[0].member_status = MultiXactStatusUpdate; /* updater */
	v->members[0].verdict = (uint8)CLUSTER_GCS_UNDO_VERDICT_COMMITTED_EXACT;
	v->members[0].commit_scn = 777;
	v->members[0].horizon_scn = InvalidScn;
	v->members[0].wrap = 3;
	v->members[0].xid = 111;
	v->members[1].member_status = MultiXactStatusForShare; /* lock-only */
	v->members[1].verdict = 0;
	v->members[1].xid = 222;
	UT_ASSERT_EQ(cluster_vis_undo_multi_verdict_page_usable(v, asked), true);

	/* NULL page / invalid asked mxid. */
	UT_ASSERT_EQ(cluster_vis_undo_multi_verdict_page_usable(NULL, asked), false);
	UT_ASSERT_EQ(cluster_vis_undo_multi_verdict_page_usable(v, InvalidMultiXactId), false);

	/* Wrong magic / version / echo (incl. a widened echo with high bits). */
	v->magic = 0xDEADBEEF;
	UT_ASSERT_EQ(cluster_vis_undo_multi_verdict_page_usable(v, asked), false);
	v->magic = CLUSTER_GCS_UNDO_MULTI_VERDICT_MAGIC;
	v->version = 2;
	UT_ASSERT_EQ(cluster_vis_undo_multi_verdict_page_usable(v, asked), false);
	v->version = CLUSTER_GCS_UNDO_MULTI_VERDICT_VERSION;
	UT_ASSERT_EQ(cluster_vis_undo_multi_verdict_page_usable(v, 4243), false);
	v->mxid_echo = (((uint64)1) << 32) + asked; /* high word poisons the echo */
	UT_ASSERT_EQ(cluster_vis_undo_multi_verdict_page_usable(v, asked), false);
	v->mxid_echo = asked;

	/* Only a SERVED page carries consumable members (DENIED never reaches
	 * here, but re-check defence-in-depth). */
	v->status = (uint8)CLUSTER_GCS_UNDO_MULTI_VERDICT_UNPROVABLE;
	UT_ASSERT_EQ(cluster_vis_undo_multi_verdict_page_usable(v, asked), false);
	v->status = (uint8)CLUSTER_GCS_UNDO_MULTI_VERDICT_SERVED;

	/* nmembers bounds: < 2 refused (a real multi has >= 2 members; the origin
	 * ships NO_MEMBERS for < 2), MAX ok (below), MAX+1 refused. */
	v->nmembers = 0;
	UT_ASSERT_EQ(cluster_vis_undo_multi_verdict_page_usable(v, asked), false);
	v->nmembers = 1;
	UT_ASSERT_EQ(cluster_vis_undo_multi_verdict_page_usable(v, asked), false);
	v->nmembers = CLUSTER_GCS_UNDO_MULTI_VERDICT_MAX_MEMBERS + 1;
	UT_ASSERT_EQ(cluster_vis_undo_multi_verdict_page_usable(v, asked), false);
	v->nmembers = 2;

	/* Non-zero reserved bytes poison the page. */
	v->reserved_0[2] = 1;
	UT_ASSERT_EQ(cluster_vis_undo_multi_verdict_page_usable(v, asked), false);
	v->reserved_0[2] = 0;

	/* Out-of-range member_status (> MaxMultiXactStatus) refused. */
	v->members[0].member_status = 9;
	UT_ASSERT_EQ(cluster_vis_undo_multi_verdict_page_usable(v, asked), false);
	v->members[0].member_status = MultiXactStatusUpdate;

	/* A lock-only member must carry NO verdict and NO scn. */
	v->members[1].verdict = (uint8)CLUSTER_GCS_UNDO_VERDICT_ABORTED;
	UT_ASSERT_EQ(cluster_vis_undo_multi_verdict_page_usable(v, asked), false);
	v->members[1].verdict = 0;
	v->members[1].commit_scn = 5;
	UT_ASSERT_EQ(cluster_vis_undo_multi_verdict_page_usable(v, asked), false);
	v->members[1].commit_scn = InvalidScn;

	/* Updater EXACT consistency: needs commit_scn, refuses a horizon. */
	v->members[0].commit_scn = InvalidScn;
	UT_ASSERT_EQ(cluster_vis_undo_multi_verdict_page_usable(v, asked), false);
	v->members[0].commit_scn = 777;
	v->members[0].horizon_scn = 500;
	UT_ASSERT_EQ(cluster_vis_undo_multi_verdict_page_usable(v, asked), false);
	v->members[0].horizon_scn = InvalidScn;

	/* Updater BELOW_HORIZON: needs a horizon, refuses an exact scn. */
	v->members[0].verdict = (uint8)CLUSTER_GCS_UNDO_VERDICT_COMMITTED_BELOW_HORIZON;
	v->members[0].commit_scn = InvalidScn;
	v->members[0].horizon_scn = 500;
	UT_ASSERT_EQ(cluster_vis_undo_multi_verdict_page_usable(v, asked), true);
	v->members[0].commit_scn = 777;
	UT_ASSERT_EQ(cluster_vis_undo_multi_verdict_page_usable(v, asked), false);
	v->members[0].commit_scn = InvalidScn;
	v->members[0].horizon_scn = InvalidScn;
	UT_ASSERT_EQ(cluster_vis_undo_multi_verdict_page_usable(v, asked), false);

	/* Updater ABORTED: carries no scn of any kind. */
	v->members[0].verdict = (uint8)CLUSTER_GCS_UNDO_VERDICT_ABORTED;
	UT_ASSERT_EQ(cluster_vis_undo_multi_verdict_page_usable(v, asked), true);
	v->members[0].commit_scn = 1;
	UT_ASSERT_EQ(cluster_vis_undo_multi_verdict_page_usable(v, asked), false);
	v->members[0].commit_scn = InvalidScn;

	/* Updater with an unknown verdict (0 / one past the last known) refuses
	 * the WHOLE page (8.A: never a partial-proof multi). */
	v->members[0].verdict = 0;
	UT_ASSERT_EQ(cluster_vis_undo_multi_verdict_page_usable(v, asked), false);
	v->members[0].verdict = (uint8)CLUSTER_GCS_UNDO_VERDICT_ABORTED + 1;
	UT_ASSERT_EQ(cluster_vis_undo_multi_verdict_page_usable(v, asked), false);

	/* Capacity boundary: MAX all-updater members round-trip; poisoning the
	 * LAST member still refuses (the validator walks every member). */
	memset(blk.data, 0, BLCKSZ);
	v->magic = CLUSTER_GCS_UNDO_MULTI_VERDICT_MAGIC;
	v->version = CLUSTER_GCS_UNDO_MULTI_VERDICT_VERSION;
	v->mxid_echo = asked;
	v->status = (uint8)CLUSTER_GCS_UNDO_MULTI_VERDICT_SERVED;
	v->nmembers = CLUSTER_GCS_UNDO_MULTI_VERDICT_MAX_MEMBERS;
	for (i = 0; i < CLUSTER_GCS_UNDO_MULTI_VERDICT_MAX_MEMBERS; i++) {
		v->members[i].member_status = MultiXactStatusUpdate;
		v->members[i].verdict = (uint8)CLUSTER_GCS_UNDO_VERDICT_ABORTED;
		v->members[i].xid = 1000 + (TransactionId)i;
	}
	UT_ASSERT_EQ(cluster_vis_undo_multi_verdict_page_usable(v, asked), true);
	v->members[CLUSTER_GCS_UNDO_MULTI_VERDICT_MAX_MEMBERS - 1].commit_scn = 9;
	UT_ASSERT_EQ(cluster_vis_undo_multi_verdict_page_usable(v, asked), false);
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
	UT_PLAN(23);
	UT_RUN(test_covers_when_epoch_match_and_scn_ge_demand);
	UT_RUN(test_covers_ignores_cross_thread_lsn);
	UT_RUN(test_failclosed_when_epoch_differs);
	UT_RUN(test_failclosed_when_hwm_invalid);
	UT_RUN(test_failclosed_when_authority_scn_absent);
	UT_RUN(test_failclosed_when_demand_invalid);
	UT_RUN(test_failclosed_when_authority_scn_below_demand);
	UT_RUN(test_failclosed_epoch_mismatch_dominates_good_scn);
	UT_RUN(test_failclosed_epoch_differs_above_32bit);
	UT_RUN(test_ttproof_committed_and_aborted);
	UT_RUN(test_ttproof_committed_is_evidence_not_verdict);
	UT_RUN(test_ttproof_recycled_same_xid_reports_bumped_wrap_not_gated);
	UT_RUN(test_ttproof_zero_match_active_invalid_scn);
	UT_RUN(test_ttproof_ambiguity_and_garbage);
	UT_RUN(test_tt_block_xid_scan_core);
	UT_RUN(test_ttproof_header_mismatch);
	UT_RUN(test_undo_auth_trailer_roundtrip);
	UT_RUN(test_undo_verdict_page_usable);
	UT_RUN(test_undo_authority_verdict_page_usable);
	UT_RUN(test_undo_authority_reply_binding);
	UT_RUN(test_undo_authority_verdict_page_fill_roundtrip);
	UT_RUN(test_undo_multi_verdict_page_usable);
	UT_RUN(test_undo_fetch_tag_roundtrip);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
