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
 *	    R5  same generation + alien state (ACTIVE) -> BAD_STATE (PANIC)
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

UT_TEST(test_r5_same_gen_alien_state_is_corruption)
{
	xl_undo_segment_recycle rec = make_rec(5);

	UT_ASSERT_EQ((int)cluster_undo_segment_recycle_redo_decide(5, (uint8)SEGMENT_ACTIVE, &rec),
				 (int)CLUSTER_SEGRECYCLE_REDO_BAD_STATE);
}


int
main(void)
{
	UT_RUN(test_r1_same_gen_committed_applies);
	UT_RUN(test_r2_same_gen_recyclable_idempotent);
	UT_RUN(test_r3_disk_gen_higher_skips_stale);
	UT_RUN(test_r4_disk_gen_lower_is_corruption);
	UT_RUN(test_r5_same_gen_alien_state_is_corruption);

	UT_DONE();
	return ut_failed_count != 0 ? 1 : 0;
}
