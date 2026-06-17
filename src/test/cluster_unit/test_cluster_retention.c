/*-------------------------------------------------------------------------
 *
 * test_cluster_retention.c
 *	  pgrac spec-3.12 D6 — retention horizon pure-predicate tests.
 *
 *	  Exercises the two pure judgement helpers that drive the
 *	  own-instance retention gate (spec-3.12 D2/D3):
 *	    cluster_tt_slot_recyclable()     — TT-slot allocator status gate
 *	    cluster_undo_segment_recyclable() — undo-segment header gate
 *
 *	  Both are pure (no shmem / no LWLock / no I/O); the only external
 *	  symbol they reference is scn_time_cmp(), which this binary stubs
 *	  with the same local-scn-only ordering the real comparator uses
 *	  (cluster_scn.c is covered by test_cluster_scn).
 *
 *	  Coverage (spec-3.12 §4.1):
 *	    U1  COMMITTED + commit_scn < horizon            -> recyclable
 *	    U2  COMMITTED + commit_scn > horizon            -> retained
 *	    U3  ABORTED (any commit_scn)                    -> recyclable (C7)
 *	    U4  ACTIVE / FREE                               -> not recyclable
 *	    U7  COMMITTED + commit_scn == horizon (strict <) -> retained
 *	    U8  InvalidScn horizon (cluster disabled)       -> recyclable
 *	    U8b COMMITTED + InvalidScn commit_scn (rule 8.A) -> retained
 *	    U5  SEGMENT_COMMITTED watermark < / == / > horizon + empty
 *	    U10 SEGMENT_ALLOCATED/ACTIVE/FULL-but-ACTIVE precondition
 *
 * Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * Spec: spec-3.12-retention-horizon.md (§4.1, D6)
 *
 * IDENTIFICATION
 *	  src/test/cluster_unit/test_cluster_retention.c
 *
 *-------------------------------------------------------------------------
 */
#define USE_PGRAC_CLUSTER 1

#include "postgres.h"

#include <string.h>

#include "cluster/cluster_scn.h"
#include "cluster/cluster_tt_slot.h"
#include "cluster/cluster_undo_retention.h"
#include "cluster/cluster_undo_segment.h"

#undef printf
#undef fprintf
#undef snprintf

#include "unit_test.h"


UT_DEFINE_GLOBALS();


/*
 * scn_time_cmp stub — mirrors the real comparator's contract: visibility
 * ordering uses local_scn only (node_id bits are masked off).  All test SCNs
 * use node 0, so scn_local(v) == v.
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


/* Build a node-0 SCN whose local value is v (v >= 1 is a real SCN). */
static inline SCN
mk_scn(uint64 v)
{
	return (SCN)v;
}


/* Zero a segment header and set its lifecycle state. */
static void
init_header(UndoSegmentHeaderData *hdr, uint8 state)
{
	memset(hdr, 0, sizeof(*hdr));
	hdr->segment_state = state;
	hdr->tt_slots_count = TT_SLOTS_PER_SEGMENT;
}


/* Put one COMMITTED on-disk slot with commit_scn into slot[idx]. */
static void
set_committed_slot(UndoSegmentHeaderData *hdr, int idx, SCN commit_scn)
{
	hdr->tt_slots[idx].status = TT_SLOT_COMMITTED;
	hdr->tt_slots[idx].commit_scn = commit_scn;
}


/* ===== U1-U4, U7, U8: cluster_tt_slot_recyclable ===== */

UT_TEST(test_u1_committed_below_horizon_recyclable)
{
	UT_ASSERT_EQ((int)cluster_tt_slot_recyclable(CTS_COMMITTED, mk_scn(5), mk_scn(10)), 1);
}

UT_TEST(test_u2_committed_above_horizon_retained)
{
	UT_ASSERT_EQ((int)cluster_tt_slot_recyclable(CTS_COMMITTED, mk_scn(15), mk_scn(10)), 0);
}

UT_TEST(test_u3_aborted_always_recyclable)
{
	/* C7: ABORTED is invisible to any read_scn; commit_scn ignored. */
	UT_ASSERT_EQ((int)cluster_tt_slot_recyclable(CTS_ABORTED, mk_scn(999), mk_scn(10)), 1);
	UT_ASSERT_EQ((int)cluster_tt_slot_recyclable(CTS_ABORTED, InvalidScn, mk_scn(10)), 1);
}

UT_TEST(test_u4_active_free_not_recyclable)
{
	UT_ASSERT_EQ((int)cluster_tt_slot_recyclable(CTS_ACTIVE, mk_scn(5), mk_scn(10)), 0);
	UT_ASSERT_EQ((int)cluster_tt_slot_recyclable(CTS_FREE, mk_scn(5), mk_scn(10)), 0);
}

UT_TEST(test_u7_committed_equal_horizon_retained)
{
	/* horizon == min active read_scn; a reader at read_scn == commit_scn
	 * needs the pre-image -> strict '<' means equality is retained. */
	UT_ASSERT_EQ((int)cluster_tt_slot_recyclable(CTS_COMMITTED, mk_scn(10), mk_scn(10)), 0);
}

UT_TEST(test_u8_invalid_horizon_recyclable)
{
	/* InvalidScn horizon == cluster disabled / no retention constraint. */
	UT_ASSERT_EQ((int)cluster_tt_slot_recyclable(CTS_COMMITTED, mk_scn(5), InvalidScn), 1);
}

UT_TEST(test_u8b_committed_invalid_commit_scn_retained)
{
	/* rule 8.A: a COMMITTED slot whose commit_scn is unresolved cannot be
	 * proven below the horizon -> fail-closed (retain). */
	UT_ASSERT_EQ((int)cluster_tt_slot_recyclable(CTS_COMMITTED, InvalidScn, mk_scn(10)), 0);
}


/* ===== U5, U10: cluster_undo_segment_recyclable ===== */

UT_TEST(test_u5_segment_committed_watermark_below_horizon)
{
	UndoSegmentHeaderData hdr;

	init_header(&hdr, SEGMENT_COMMITTED);
	set_committed_slot(&hdr, 0, mk_scn(3));
	set_committed_slot(&hdr, 1, mk_scn(7)); /* watermark = max = 7 */
	UT_ASSERT_EQ((int)cluster_undo_segment_recyclable(&hdr, mk_scn(10)), 1);
}

UT_TEST(test_u5_segment_committed_watermark_at_or_above_horizon)
{
	UndoSegmentHeaderData hdr;

	init_header(&hdr, SEGMENT_COMMITTED);
	set_committed_slot(&hdr, 0, mk_scn(3));
	set_committed_slot(&hdr, 1, mk_scn(10)); /* watermark = 10 == horizon */
	UT_ASSERT_EQ((int)cluster_undo_segment_recyclable(&hdr, mk_scn(10)), 0);

	set_committed_slot(&hdr, 2, mk_scn(20)); /* watermark = 20 > horizon */
	UT_ASSERT_EQ((int)cluster_undo_segment_recyclable(&hdr, mk_scn(10)), 0);
}

UT_TEST(test_u5_segment_committed_no_committed_slot_recyclable)
{
	UndoSegmentHeaderData hdr;

	/* SEGMENT_COMMITTED with no live COMMITTED slot -> watermark absent ->
	 * recyclable. */
	init_header(&hdr, SEGMENT_COMMITTED);
	UT_ASSERT_EQ((int)cluster_undo_segment_recyclable(&hdr, mk_scn(10)), 1);
}

UT_TEST(test_u5_segment_committed_invalid_commit_scn_retained)
{
	UndoSegmentHeaderData hdr;

	/* rule 8.A: a COMMITTED-status on-disk slot with InvalidScn commit_scn
	 * (partial write) cannot be resolved -> retain the whole segment. */
	init_header(&hdr, SEGMENT_COMMITTED);
	hdr.tt_slots[0].status = TT_SLOT_COMMITTED;
	hdr.tt_slots[0].commit_scn = InvalidScn;
	UT_ASSERT_EQ((int)cluster_undo_segment_recyclable(&hdr, mk_scn(10)), 0);
}

UT_TEST(test_u8_segment_invalid_horizon_recyclable)
{
	UndoSegmentHeaderData hdr;

	init_header(&hdr, SEGMENT_COMMITTED);
	set_committed_slot(&hdr, 0, mk_scn(100)); /* even high watermark */
	UT_ASSERT_EQ((int)cluster_undo_segment_recyclable(&hdr, InvalidScn), 1);
}

UT_TEST(test_u10_segment_non_committed_state_never_recyclable)
{
	UndoSegmentHeaderData hdr;

	/* Even with zero committed slots (which would look "empty"), any state
	 * other than SEGMENT_COMMITTED must not be recyclable (C5/U10): it may
	 * be actively written or freshly allocated. */
	init_header(&hdr, SEGMENT_ALLOCATED);
	UT_ASSERT_EQ((int)cluster_undo_segment_recyclable(&hdr, mk_scn(10)), 0);

	init_header(&hdr, SEGMENT_ACTIVE);
	UT_ASSERT_EQ((int)cluster_undo_segment_recyclable(&hdr, mk_scn(10)), 0);

	/* FULL-but-ACTIVE: flag set, state still ACTIVE. */
	init_header(&hdr, SEGMENT_ACTIVE);
	hdr.segment_flags |= UNDO_SEGMENT_FLAG_FULL;
	UT_ASSERT_EQ((int)cluster_undo_segment_recyclable(&hdr, mk_scn(10)), 0);

	init_header(&hdr, SEGMENT_RECYCLABLE);
	UT_ASSERT_EQ((int)cluster_undo_segment_recyclable(&hdr, mk_scn(10)), 0);
}

UT_TEST(test_u10_segment_null_header_not_recyclable)
{
	UT_ASSERT_EQ((int)cluster_undo_segment_recyclable(NULL, mk_scn(10)), 0);
}


/* ===== spec-4.12a D1: cluster_undo_record_segment_drainable ===== *
 *
 * Pure ACTIVE -> COMMITTED drain gate for record segments.  The six 8.A
 * hard gates (CLAUDE.md rule 8.A) all fail-closed toward "retain":
 *   guard 1 (in-flight)  : seal upper_scn strictly below the active-write
 *                          boundary; UNKNOWN seal / equality -> retain.
 *   guard 2 (fixed_first): never the spec-3.4b fixed first segment.
 *   guard 3 (active)     : never the record / TT cursor's current segment.
 *   guard 6 (prepared)   : any unresolved cluster-TT prepared xact -> retain
 *                          all (Q11-A minimal-safe).
 *   plus: only SEGMENT_ACTIVE candidates; NULL header -> retain.
 */

/* Build an ACTIVE record segment header with id + sealed upper_scn. */
static void
init_record_seg(UndoSegmentHeaderData *hdr, uint32 segment_id, SCN seal_upper_scn)
{
	init_header(hdr, SEGMENT_ACTIVE);
	hdr->segment_id = segment_id;
	UndoSegmentHeader_set_record_seal_upper_scn(hdr, seal_upper_scn);
}

/* A boundary with one in-flight writer at first_undo_scn == v. */
static ClusterUndoActiveBoundary
bnd_finite(uint64 v)
{
	ClusterUndoActiveBoundary b = { .infinite = false, .scn = mk_scn(v) };

	return b;
}

/* The "no in-flight writer" boundary. */
static ClusterUndoActiveBoundary
bnd_infinite(void)
{
	ClusterUndoActiveBoundary b = { .infinite = true, .scn = InvalidScn };

	return b;
}

/* segment id space: pick ids well clear of the excluded ones below. */
#define DRAIN_SEG 500
#define DRAIN_FIXED_FIRST 1
#define DRAIN_ACTIVE_REC 600
#define DRAIN_ACTIVE_TT 700

UT_TEST(test_d1_drainable_seal_below_boundary)
{
	UndoSegmentHeaderData hdr;

	/* seal upper_scn (5) strictly below boundary (10) -> no in-flight writer
	 * could have undo here -> drainable. */
	init_record_seg(&hdr, DRAIN_SEG, mk_scn(5));
	UT_ASSERT_EQ((int)cluster_undo_record_segment_drainable(&hdr, bnd_finite(10), false,
															DRAIN_FIXED_FIRST, DRAIN_ACTIVE_REC,
															DRAIN_ACTIVE_TT, false),
				 1);
}

UT_TEST(test_d1_retain_seal_equal_boundary)
{
	UndoSegmentHeaderData hdr;

	/* strict '<': seal == boundary -> an in-flight writer may have begun at the
	 * seal point -> retain. */
	init_record_seg(&hdr, DRAIN_SEG, mk_scn(10));
	UT_ASSERT_EQ((int)cluster_undo_record_segment_drainable(&hdr, bnd_finite(10), false,
															DRAIN_FIXED_FIRST, DRAIN_ACTIVE_REC,
															DRAIN_ACTIVE_TT, false),
				 0);
}

UT_TEST(test_d1_retain_seal_above_boundary)
{
	UndoSegmentHeaderData hdr;

	init_record_seg(&hdr, DRAIN_SEG, mk_scn(20));
	UT_ASSERT_EQ((int)cluster_undo_record_segment_drainable(&hdr, bnd_finite(10), false,
															DRAIN_FIXED_FIRST, DRAIN_ACTIVE_REC,
															DRAIN_ACTIVE_TT, false),
				 0);
}

UT_TEST(test_d1_drainable_boundary_infinite)
{
	UndoSegmentHeaderData hdr;

	/* No in-flight writer at all -> any sealed segment drains (quiesce path). */
	init_record_seg(&hdr, DRAIN_SEG, mk_scn(99));
	UT_ASSERT_EQ((int)cluster_undo_record_segment_drainable(&hdr, bnd_infinite(), false,
															DRAIN_FIXED_FIRST, DRAIN_ACTIVE_REC,
															DRAIN_ACTIVE_TT, false),
				 1);
}

UT_TEST(test_d1_retain_unsealed_upper_scn)
{
	UndoSegmentHeaderData hdr;

	/* guard 1/4: an unsealed (InvalidScn) upper bound cannot be proven safe,
	 * even when no in-flight writer exists -> retain (fail-closed). */
	init_record_seg(&hdr, DRAIN_SEG, InvalidScn);
	UT_ASSERT_EQ((int)cluster_undo_record_segment_drainable(&hdr, bnd_infinite(), false,
															DRAIN_FIXED_FIRST, DRAIN_ACTIVE_REC,
															DRAIN_ACTIVE_TT, false),
				 0);
}

UT_TEST(test_d1_retain_fixed_first)
{
	UndoSegmentHeaderData hdr;

	/* guard 2: the fixed first segment is shared with the write cursor start. */
	init_record_seg(&hdr, DRAIN_FIXED_FIRST, mk_scn(5));
	UT_ASSERT_EQ((int)cluster_undo_record_segment_drainable(&hdr, bnd_infinite(), false,
															DRAIN_FIXED_FIRST, DRAIN_ACTIVE_REC,
															DRAIN_ACTIVE_TT, false),
				 0);
}

UT_TEST(test_d1_retain_active_record_segment)
{
	UndoSegmentHeaderData hdr;

	/* guard 3: the record cursor's current segment is still being written. */
	init_record_seg(&hdr, DRAIN_ACTIVE_REC, mk_scn(5));
	UT_ASSERT_EQ((int)cluster_undo_record_segment_drainable(&hdr, bnd_infinite(), false,
															DRAIN_FIXED_FIRST, DRAIN_ACTIVE_REC,
															DRAIN_ACTIVE_TT, false),
				 0);
}

UT_TEST(test_d1_retain_active_tt_segment)
{
	UndoSegmentHeaderData hdr;

	/* guard 3: the TT cursor's current segment is still in use. */
	init_record_seg(&hdr, DRAIN_ACTIVE_TT, mk_scn(5));
	UT_ASSERT_EQ((int)cluster_undo_record_segment_drainable(&hdr, bnd_infinite(), false,
															DRAIN_FIXED_FIRST, DRAIN_ACTIVE_REC,
															DRAIN_ACTIVE_TT, false),
				 0);
}

UT_TEST(test_d1_retain_any_unresolved_prepared)
{
	UndoSegmentHeaderData hdr;

	/* guard 6: an otherwise-drainable segment is retained while any unresolved
	 * cluster-TT prepared xact exists (its undo may be consumed by ROLLBACK
	 * PREPARED). */
	init_record_seg(&hdr, DRAIN_SEG, mk_scn(5));
	UT_ASSERT_EQ((int)cluster_undo_record_segment_drainable(&hdr, bnd_finite(10),
															/* any_unresolved_prepared = */ true,
															DRAIN_FIXED_FIRST, DRAIN_ACTIVE_REC,
															DRAIN_ACTIVE_TT, false),
				 0);
}

UT_TEST(test_d1_retain_non_active_states)
{
	UndoSegmentHeaderData hdr;

	/* Only SEGMENT_ACTIVE is a candidate; every other state retains (硬门 4). */
	init_record_seg(&hdr, DRAIN_SEG, mk_scn(5));

	hdr.segment_state = SEGMENT_ALLOCATED;
	UT_ASSERT_EQ((int)cluster_undo_record_segment_drainable(&hdr, bnd_infinite(), false,
															DRAIN_FIXED_FIRST, DRAIN_ACTIVE_REC,
															DRAIN_ACTIVE_TT, false),
				 0);
	hdr.segment_state = SEGMENT_COMMITTED;
	UT_ASSERT_EQ((int)cluster_undo_record_segment_drainable(&hdr, bnd_infinite(), false,
															DRAIN_FIXED_FIRST, DRAIN_ACTIVE_REC,
															DRAIN_ACTIVE_TT, false),
				 0);
	hdr.segment_state = SEGMENT_RECYCLABLE;
	UT_ASSERT_EQ((int)cluster_undo_record_segment_drainable(&hdr, bnd_infinite(), false,
															DRAIN_FIXED_FIRST, DRAIN_ACTIVE_REC,
															DRAIN_ACTIVE_TT, false),
				 0);
}

UT_TEST(test_d1_drainable_full_but_active)
{
	UndoSegmentHeaderData hdr;

	/* FULL is a flag, not a state: a FULL-but-ACTIVE sealed segment with no
	 * in-flight writer is the common drain candidate. */
	init_record_seg(&hdr, DRAIN_SEG, mk_scn(5));
	hdr.segment_flags |= UNDO_SEGMENT_FLAG_FULL;
	UT_ASSERT_EQ((int)cluster_undo_record_segment_drainable(&hdr, bnd_infinite(), false,
															DRAIN_FIXED_FIRST, DRAIN_ACTIVE_REC,
															DRAIN_ACTIVE_TT, false),
				 1);
}

UT_TEST(test_d1_null_header_not_drainable)
{
	UT_ASSERT_EQ((int)cluster_undo_record_segment_drainable(NULL, bnd_infinite(), false,
															DRAIN_FIXED_FIRST, DRAIN_ACTIVE_REC,
															DRAIN_ACTIVE_TT, false),
				 0);
}

UT_TEST(test_d3_retain_during_recovery)
{
	UndoSegmentHeaderData hdr;

	/*
	 * spec-4.12a D3 (硬门 4): recovery_in_progress is the single auditable
	 * fail-closed recovery gate.  An otherwise fully drainable segment (sealed,
	 * boundary infinite, no prepared xact, not fixed/active) drains only while
	 * recovery is NOT in progress; during WAL replay the active-write registry
	 * is empty, so the {infinite} boundary cannot prove the absence of prepared
	 * / in-flight undo until RecoverPreparedTransactions rebuilds the
	 * protected-slot view -- the gate must therefore retain (fail-closed),
	 * overriding every other gate.
	 */
	init_record_seg(&hdr, DRAIN_SEG, mk_scn(5));

	/* control: not in recovery -> the same segment is drainable. */
	UT_ASSERT_EQ((int)cluster_undo_record_segment_drainable(&hdr, bnd_infinite(), false,
															DRAIN_FIXED_FIRST, DRAIN_ACTIVE_REC,
															DRAIN_ACTIVE_TT, false),
				 1);
	/* in recovery -> retain, even though every per-segment gate would pass. */
	UT_ASSERT_EQ((int)cluster_undo_record_segment_drainable(&hdr, bnd_infinite(), false,
															DRAIN_FIXED_FIRST, DRAIN_ACTIVE_REC,
															DRAIN_ACTIVE_TT, true),
				 0);
}


int
main(void)
{
	UT_RUN(test_u1_committed_below_horizon_recyclable);
	UT_RUN(test_u2_committed_above_horizon_retained);
	UT_RUN(test_u3_aborted_always_recyclable);
	UT_RUN(test_u4_active_free_not_recyclable);
	UT_RUN(test_u7_committed_equal_horizon_retained);
	UT_RUN(test_u8_invalid_horizon_recyclable);
	UT_RUN(test_u8b_committed_invalid_commit_scn_retained);
	UT_RUN(test_u5_segment_committed_watermark_below_horizon);
	UT_RUN(test_u5_segment_committed_watermark_at_or_above_horizon);
	UT_RUN(test_u5_segment_committed_no_committed_slot_recyclable);
	UT_RUN(test_u5_segment_committed_invalid_commit_scn_retained);
	UT_RUN(test_u8_segment_invalid_horizon_recyclable);
	UT_RUN(test_u10_segment_non_committed_state_never_recyclable);
	UT_RUN(test_u10_segment_null_header_not_recyclable);

	/* spec-4.12a D1: record-segment drain gate. */
	UT_RUN(test_d1_drainable_seal_below_boundary);
	UT_RUN(test_d1_retain_seal_equal_boundary);
	UT_RUN(test_d1_retain_seal_above_boundary);
	UT_RUN(test_d1_drainable_boundary_infinite);
	UT_RUN(test_d1_retain_unsealed_upper_scn);
	UT_RUN(test_d1_retain_fixed_first);
	UT_RUN(test_d1_retain_active_record_segment);
	UT_RUN(test_d1_retain_active_tt_segment);
	UT_RUN(test_d1_retain_any_unresolved_prepared);
	UT_RUN(test_d1_retain_non_active_states);
	UT_RUN(test_d1_drainable_full_but_active);
	UT_RUN(test_d1_null_header_not_drainable);

	/* spec-4.12a D3: crash-recovery fail-closed gate (硬门 4). */
	UT_RUN(test_d3_retain_during_recovery);

	return ut_failed_count == 0 ? 0 : 1;
}
