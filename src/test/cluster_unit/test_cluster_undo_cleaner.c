/*-------------------------------------------------------------------------
 *
 * test_cluster_undo_cleaner.c
 *	  cluster_unit tests for the spec-3.13 Undo Cleaner pure decision
 *	  surfaces.
 *
 *	  Step 4 (D3) scope: the XLOG_UNDO_SEGMENT_RECYCLE redo decision
 *	  table (header-inline, no WAL machinery linked):
 *	    R1  same generation + COMMITTED            -> APPLY
 *	    R2  same generation + RECYCLABLE           -> APPLY (idempotent)
 *	    R3  disk generation higher                 -> SKIP_STALE
 *	    R4  disk generation lower                  -> BAD_GENERATION (PANIC)
 *	    R5  same generation + earlier legal states -> APPLY
 *	    R5b same generation + illegal state         -> BAD_STATE (PANIC)
 *
 *	  Steps 6-7 append the REUSE redo table + wrap-retire edges here.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/test/cluster_unit/test_cluster_undo_cleaner.c
 *
 * NOTES
 *	  This is a pgrac-original file.
 *	  Spec: spec-3.13-undo-cleaner-tt-gc.md (FROZEN v0.3) §2.4 redo 表.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "cluster/cluster_undo_segment.h"
#include "cluster/storage/cluster_undo_xlog.h"

/* Drop PG's port.h printf -> pg_printf override; unit_test.h uses
 * stdlib printf and we don't link libpgport in this test binary. */
#undef printf

#include "unit_test.h"

UT_DEFINE_GLOBALS();


static xl_undo_segment_recycle
make_rec(uint32 gen)
{
	xl_undo_segment_recycle rec;

	memset(&rec, 0, sizeof(rec));
	rec.segment_id = 1;
	rec.expected_generation = gen;
	rec.instance = 1;
	rec.old_state = (uint8)SEGMENT_COMMITTED;
	rec.new_state = (uint8)SEGMENT_RECYCLABLE;
	return rec;
}


UT_TEST(test_r1_same_gen_committed_applies)
{
	xl_undo_segment_recycle rec = make_rec(5);

	UT_ASSERT_EQ((int)cluster_undo_segment_recycle_redo_decide(5, (uint8)SEGMENT_COMMITTED, &rec),
				 (int)CLUSTER_SEGRECYCLE_REDO_APPLY);
}

UT_TEST(test_r2_same_gen_recyclable_idempotent)
{
	xl_undo_segment_recycle rec = make_rec(5);

	UT_ASSERT_EQ((int)cluster_undo_segment_recycle_redo_decide(5, (uint8)SEGMENT_RECYCLABLE, &rec),
				 (int)CLUSTER_SEGRECYCLE_REDO_APPLY);
}

UT_TEST(test_r3_disk_gen_higher_skips_stale)
{
	/* a later whole-segment reuse is already durable on disk. */
	xl_undo_segment_recycle rec = make_rec(5);

	UT_ASSERT_EQ((int)cluster_undo_segment_recycle_redo_decide(6, (uint8)SEGMENT_ACTIVE, &rec),
				 (int)CLUSTER_SEGRECYCLE_REDO_SKIP_STALE);
}

UT_TEST(test_r4_disk_gen_lower_is_corruption)
{
	/* v0.3 (2): the preceding REUSE redo must have aligned the on-disk
	 * generation; lower means lost writes -> fail loud, never skip. */
	xl_undo_segment_recycle rec = make_rec(5);

	UT_ASSERT_EQ((int)cluster_undo_segment_recycle_redo_decide(4, (uint8)SEGMENT_COMMITTED, &rec),
				 (int)CLUSTER_SEGRECYCLE_REDO_BAD_GENERATION);
}

UT_TEST(test_r5_same_gen_earlier_states_apply)
{
	xl_undo_segment_recycle rec = make_rec(5);

	UT_ASSERT_EQ((int)cluster_undo_segment_recycle_redo_decide(5, (uint8)SEGMENT_ALLOCATED, &rec),
				 (int)CLUSTER_SEGRECYCLE_REDO_APPLY);
	UT_ASSERT_EQ((int)cluster_undo_segment_recycle_redo_decide(5, (uint8)SEGMENT_ACTIVE, &rec),
				 (int)CLUSTER_SEGRECYCLE_REDO_APPLY);
}

UT_TEST(test_r5b_same_gen_illegal_state_is_corruption)
{
	xl_undo_segment_recycle rec = make_rec(5);

	UT_ASSERT_EQ((int)cluster_undo_segment_recycle_redo_decide(5, (uint8)SEGMENT_INVALID, &rec),
				 (int)CLUSTER_SEGRECYCLE_REDO_BAD_STATE);
}


UT_TEST(test_d3_record_segment_drain_recycle_redo_coverage)
{
	/*
	 * spec-4.12a D3 (硬门 4): confirm the EXISTING recycle redo decide table
	 * already covers a *record* segment driven through the new
	 * ACTIVE -> COMMITTED -> RECYCLABLE lifecycle, so 4.12a needs no new redo
	 * logic.  The ACTIVE -> COMMITTED advance (cluster_undo_try_mark_record_
	 * segment_committed) is a no-WAL direct header write, identical in shape to
	 * the long-shipped TT-segment mark_committed -- so a crash may leave block 0
	 * at ANY not-newer state (ALLOCATED if never sealed, ACTIVE if the COMMITTED
	 * write was lost, COMMITTED if it reached disk) while the durable
	 * XLOG_UNDO_SEGMENT_RECYCLE (old=COMMITTED, new=RECYCLABLE) replays.  Redo
	 * must APPLY for every state <= new_state (deterministic convergence) and
	 * fail-closed PANIC otherwise.  This pins that coverage for the 4.12a record
	 * lifecycle (L250 real-path).
	 */
	xl_undo_segment_recycle rec = make_rec(7); /* old=COMMITTED, new=RECYCLABLE */

	/* same generation, every not-newer on-disk state -> APPLY. */
	UT_ASSERT_EQ((int)cluster_undo_segment_recycle_redo_decide(7, (uint8)SEGMENT_ALLOCATED, &rec),
				 (int)CLUSTER_SEGRECYCLE_REDO_APPLY);
	UT_ASSERT_EQ((int)cluster_undo_segment_recycle_redo_decide(7, (uint8)SEGMENT_ACTIVE, &rec),
				 (int)CLUSTER_SEGRECYCLE_REDO_APPLY);
	UT_ASSERT_EQ((int)cluster_undo_segment_recycle_redo_decide(7, (uint8)SEGMENT_COMMITTED, &rec),
				 (int)CLUSTER_SEGRECYCLE_REDO_APPLY);
	UT_ASSERT_EQ((int)cluster_undo_segment_recycle_redo_decide(7, (uint8)SEGMENT_RECYCLABLE, &rec),
				 (int)CLUSTER_SEGRECYCLE_REDO_APPLY);

	/* corruption sentinel -> fail-closed (BAD_STATE PANIC at redo). */
	UT_ASSERT_EQ((int)cluster_undo_segment_recycle_redo_decide(7, (uint8)SEGMENT_INVALID, &rec),
				 (int)CLUSTER_SEGRECYCLE_REDO_BAD_STATE);
	/* a lower on-disk generation is impossible post-REUSE -> fail-closed. */
	UT_ASSERT_EQ((int)cluster_undo_segment_recycle_redo_decide(6, (uint8)SEGMENT_COMMITTED, &rec),
				 (int)CLUSTER_SEGRECYCLE_REDO_BAD_GENERATION);
}


/* ---- XLOG_UNDO_SEGMENT_REUSE (0x50) redo table (D4) ---- */

static xl_undo_segment_reuse
make_reuse(uint32 old_gen)
{
	xl_undo_segment_reuse rec;

	memset(&rec, 0, sizeof(rec));
	rec.segment_id = 1;
	rec.old_generation = old_gen;
	rec.new_generation = old_gen + 1;
	rec.instance = 1;
	return rec;
}

UT_TEST(test_r6_reuse_fresh_or_torn_header_applies)
{
	xl_undo_segment_reuse rec = make_reuse(4);

	UT_ASSERT_EQ((int)cluster_undo_segment_reuse_redo_decide(false, 0, &rec),
				 (int)CLUSTER_SEGREUSE_REDO_APPLY);
}

UT_TEST(test_r7_reuse_old_or_new_generation_applies_idempotent)
{
	xl_undo_segment_reuse rec = make_reuse(4);

	UT_ASSERT_EQ((int)cluster_undo_segment_reuse_redo_decide(true, 4, &rec),
				 (int)CLUSTER_SEGREUSE_REDO_APPLY);
	UT_ASSERT_EQ((int)cluster_undo_segment_reuse_redo_decide(true, 5, &rec),
				 (int)CLUSTER_SEGREUSE_REDO_APPLY);
}

UT_TEST(test_r8_reuse_disk_newer_skips_stale)
{
	xl_undo_segment_reuse rec = make_reuse(4);

	UT_ASSERT_EQ((int)cluster_undo_segment_reuse_redo_decide(true, 6, &rec),
				 (int)CLUSTER_SEGREUSE_REDO_SKIP_STALE);
}

UT_TEST(test_r9_reuse_disk_older_than_old_gen_is_corruption)
{
	xl_undo_segment_reuse rec = make_reuse(4);

	UT_ASSERT_EQ((int)cluster_undo_segment_reuse_redo_decide(true, 3, &rec),
				 (int)CLUSTER_SEGREUSE_REDO_BAD_GENERATION);
}


int
main(void)
{
	UT_RUN(test_r1_same_gen_committed_applies);
	UT_RUN(test_r2_same_gen_recyclable_idempotent);
	UT_RUN(test_r3_disk_gen_higher_skips_stale);
	UT_RUN(test_r4_disk_gen_lower_is_corruption);
	UT_RUN(test_r5_same_gen_earlier_states_apply);
	UT_RUN(test_r5b_same_gen_illegal_state_is_corruption);
	UT_RUN(test_d3_record_segment_drain_recycle_redo_coverage); /* spec-4.12a D3 */
	UT_RUN(test_r6_reuse_fresh_or_torn_header_applies);
	UT_RUN(test_r7_reuse_old_or_new_generation_applies_idempotent);
	UT_RUN(test_r8_reuse_disk_newer_skips_stale);
	UT_RUN(test_r9_reuse_disk_older_than_old_gen_is_corruption);

	UT_DONE();
	return ut_failed_count != 0 ? 1 : 0;
}
