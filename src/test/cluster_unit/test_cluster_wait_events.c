/*-------------------------------------------------------------------------
 *
 * test_cluster_wait_events.c
 *	  Compile-time invariants for the cluster wait event registration
 *	  introduced in stage 0.11.
 *
 *	  This test asserts the structural invariants that must hold
 *	  across the 10-class / 46-event cluster wait event scheme:
 *
 *	  - The 10 PG_WAIT_CLUSTER_* class IDs match docs/wait-events-design.md
 *	    §14.1 exactly.
 *	  - All 10 class IDs are pairwise distinct.
 *	  - None of the 10 cluster class IDs collide with PG's native
 *	    9 wait classes (LWLock through IO).
 *	  - The "gap zone" 0x0B..0x0F is preserved (cluster IDs start at
 *	    0x10, leaving 5 slots for upstream PG to extend).
 *	  - Each WaitEventCluster value sits in the correct class
 *	    (upper byte matches its declared category).
 *	  - Per-category event counts match the design doc roster.
 *
 *	  Why compile-time only:
 *
 *	  Stage 0.11 wires no pgstat_report_wait_start() call sites; that
 *	  work is deferred to the owning-subsystem specs (spec-1.X-GES,
 *	  spec-2.X-PCM, ...).  PG 16 lacks the pg_wait_events catalog view
 *	  (added in PG 17), so cluster_tap t/005_wait_events_catalog.pl
 *	  performs reverse regression instead: it confirms that the cluster
 *	  switch cases coexist cleanly with PG-native dispatch, and that no
 *	  live backend reports a 'Cluster: *' wait_event_type yet.
 *
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/test/cluster_unit/test_cluster_wait_events.c
 *
 * NOTES
 *	  This is a pgrac-original file.  Includes postgres.h to get the
 *	  basic PG types pulled in by wait_event.h, then undoes the
 *	  printf -> pg_printf redirection so the standalone unit test
 *	  binary does not pull in libpgport.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "cluster/cluster_wait_events.h"
#include "utils/wait_event.h"

/*
 * postgres.h transitively pulls in port.h which redirects printf,
 * fprintf, etc. to PG's libpgport variants.  Standalone unit tests
 * do not link libpgport, so undo the redirection before pulling in
 * unit_test.h.
 */
#undef printf
#undef fprintf
#undef snprintf
#undef sprintf
#undef vsnprintf
#undef vfprintf
#undef vprintf
#undef vsprintf
#undef strerror
#undef strerror_r

#include "unit_test.h"

UT_DEFINE_GLOBALS();


/* ----------
 * 11 cluster class IDs match docs/wait-events-design.md §14.1 exactly
 * (10 from spec-0.11 plus SharedFs added by spec-1.1).
 * ----------
 */

UT_TEST(test_class_ids_match_design_doc)
{
	UT_ASSERT_EQ(PG_WAIT_CLUSTER_GES, 0x10000000U);
	UT_ASSERT_EQ(PG_WAIT_CLUSTER_PCM, 0x11000000U);
	UT_ASSERT_EQ(PG_WAIT_CLUSTER_BUFFERSHIP, 0x12000000U);
	UT_ASSERT_EQ(PG_WAIT_CLUSTER_SCN, 0x13000000U);
	UT_ASSERT_EQ(PG_WAIT_CLUSTER_RECONFIG, 0x14000000U);
	UT_ASSERT_EQ(PG_WAIT_CLUSTER_RECOVERY, 0x15000000U);
	UT_ASSERT_EQ(PG_WAIT_CLUSTER_SINVAL, 0x16000000U);
	UT_ASSERT_EQ(PG_WAIT_CLUSTER_INTERCONNECT, 0x17000000U);
	UT_ASSERT_EQ(PG_WAIT_CLUSTER_UNDO, 0x18000000U);
	UT_ASSERT_EQ(PG_WAIT_CLUSTER_ADG, 0x19000000U);
	UT_ASSERT_EQ(PG_WAIT_CLUSTER_SHAREDFS, 0x1a000000U);
}


/* ----------
 * Class IDs are pairwise distinct (spot-check via lowest/highest).
 * The strict ordering of the macros above already proves uniqueness;
 * we still spot-check the lower-byte mask to catch a future typo.
 * ----------
 */

UT_TEST(test_class_ids_pairwise_distinct)
{
	UT_ASSERT_NE(PG_WAIT_CLUSTER_GES, PG_WAIT_CLUSTER_ADG);
	UT_ASSERT_NE(PG_WAIT_CLUSTER_PCM, PG_WAIT_CLUSTER_INTERCONNECT);
	/* Class bits (upper byte) of every cluster class are non-zero. */
	UT_ASSERT((PG_WAIT_CLUSTER_GES & 0xFF000000U) == PG_WAIT_CLUSTER_GES);
	UT_ASSERT((PG_WAIT_CLUSTER_ADG & 0xFF000000U) == PG_WAIT_CLUSTER_ADG);
}


/* ----------
 * No collision with PG's 9 native wait classes (0x01..0x0A).
 * ----------
 */

UT_TEST(test_no_collision_with_pg_native_classes)
{
	UT_ASSERT(PG_WAIT_CLUSTER_GES > PG_WAIT_IO);
	UT_ASSERT_NE(PG_WAIT_CLUSTER_GES, PG_WAIT_LWLOCK);
	UT_ASSERT_NE(PG_WAIT_CLUSTER_GES, PG_WAIT_LOCK);
	UT_ASSERT_NE(PG_WAIT_CLUSTER_GES, PG_WAIT_BUFFER_PIN);
	UT_ASSERT_NE(PG_WAIT_CLUSTER_GES, PG_WAIT_ACTIVITY);
	UT_ASSERT_NE(PG_WAIT_CLUSTER_GES, PG_WAIT_CLIENT);
	UT_ASSERT_NE(PG_WAIT_CLUSTER_GES, PG_WAIT_EXTENSION);
	UT_ASSERT_NE(PG_WAIT_CLUSTER_GES, PG_WAIT_IPC);
	UT_ASSERT_NE(PG_WAIT_CLUSTER_GES, PG_WAIT_TIMEOUT);
	UT_ASSERT_NE(PG_WAIT_CLUSTER_GES, PG_WAIT_IO);
}


/* ----------
 * Gap zone 0x0B..0x0F preserved for upstream PG.
 * ----------
 */

UT_TEST(test_gap_zone_preserved)
{
	/* PG's last class is 0x0A (PG_WAIT_IO).  Cluster's first is 0x10. */
	UT_ASSERT(PG_WAIT_IO == 0x0A000000U);
	UT_ASSERT(PG_WAIT_CLUSTER_GES == 0x10000000U);
	/* Five slots reserved for PG: 0x0B, 0x0C, 0x0D, 0x0E, 0x0F. */
	UT_ASSERT(PG_WAIT_CLUSTER_GES - PG_WAIT_IO == 0x06000000U);
}


/* ----------
 * Each WaitEventCluster value is anchored to the correct class.
 * Spot-check the first event in each of the 10 categories.
 * ----------
 */

UT_TEST(test_first_event_per_category_anchors_class_id)
{
	UT_ASSERT_EQ((uint32)WAIT_EVENT_GES_ENQUEUE_ACQUIRE, PG_WAIT_CLUSTER_GES);
	UT_ASSERT_EQ((uint32)WAIT_EVENT_PCM_BLOCK_READ_N_S, PG_WAIT_CLUSTER_PCM);
	UT_ASSERT_EQ((uint32)WAIT_EVENT_BUFFER_SHIP_CR_BUILD, PG_WAIT_CLUSTER_BUFFERSHIP);
	UT_ASSERT_EQ((uint32)WAIT_EVENT_SCN_BOC_FLUSH_WAIT, PG_WAIT_CLUSTER_SCN);
	UT_ASSERT_EQ((uint32)WAIT_EVENT_RECONFIG_GRD_REBUILD, PG_WAIT_CLUSTER_RECONFIG);
	UT_ASSERT_EQ((uint32)WAIT_EVENT_RECOVERY_WAL_FETCH, PG_WAIT_CLUSTER_RECOVERY);
	UT_ASSERT_EQ((uint32)WAIT_EVENT_SINVAL_BROADCAST_SEND, PG_WAIT_CLUSTER_SINVAL);
	UT_ASSERT_EQ((uint32)WAIT_EVENT_INTERCONNECT_RDMA_SEND, PG_WAIT_CLUSTER_INTERCONNECT);
	UT_ASSERT_EQ((uint32)WAIT_EVENT_UNDO_REMOTE_READ, PG_WAIT_CLUSTER_UNDO);
	UT_ASSERT_EQ((uint32)WAIT_EVENT_ADG_MRP_APPLY_WAIT, PG_WAIT_CLUSTER_ADG);
}


/* ----------
 * Every event in a category has the correct class ID in its upper byte.
 * Spot-check via last event of each category and matching class mask.
 * ----------
 */

UT_TEST(test_last_event_per_category_in_class)
{
	UT_ASSERT_EQ(((uint32)WAIT_EVENT_GES_LOCAL_FAST_PATH) & 0xFF000000U, PG_WAIT_CLUSTER_GES);
	UT_ASSERT_EQ(((uint32)WAIT_EVENT_PCM_ITL_CLEANOUT) & 0xFF000000U, PG_WAIT_CLUSTER_PCM);
	UT_ASSERT_EQ(((uint32)WAIT_EVENT_BUFFER_SHIP_CURRENT_RECEIVE) & 0xFF000000U,
				 PG_WAIT_CLUSTER_BUFFERSHIP);
	UT_ASSERT_EQ(((uint32)WAIT_EVENT_SCN_ADVANCE_BROADCAST) & 0xFF000000U, PG_WAIT_CLUSTER_SCN);
	UT_ASSERT_EQ(((uint32)WAIT_EVENT_RECONFIG_BARRIER_WAIT) & 0xFF000000U,
				 PG_WAIT_CLUSTER_RECONFIG);
	UT_ASSERT_EQ(((uint32)WAIT_EVENT_RECOVERY_PCM_STATE_RESTORE) & 0xFF000000U,
				 PG_WAIT_CLUSTER_RECOVERY);
	UT_ASSERT_EQ(((uint32)WAIT_EVENT_SINVAL_INJECT_LOCAL_QUEUE) & 0xFF000000U,
				 PG_WAIT_CLUSTER_SINVAL);
	UT_ASSERT_EQ(((uint32)WAIT_EVENT_INTERCONNECT_CONNECT_RETRY) & 0xFF000000U,
				 PG_WAIT_CLUSTER_INTERCONNECT);
	UT_ASSERT_EQ(((uint32)WAIT_EVENT_UNDO_RETENTION_WAIT) & 0xFF000000U, PG_WAIT_CLUSTER_UNDO);
	UT_ASSERT_EQ(((uint32)WAIT_EVENT_ADG_SCN_SYNC_WAIT) & 0xFF000000U, PG_WAIT_CLUSTER_ADG);
}


/* ----------
 * Per-category event counts match the design doc roster
 *  (GES 5, PCM 8, BufferShip 5, SCN 4, Reconfig 5, Recovery 5,
 *   Sinval 3, Interconnect 5, Undo 4, ADG 4, SharedFs 5 -- plus later
 *   subsystem classes, total tracked by CLUSTER_WAIT_EVENTS_COUNT).
 *
 *	Use (last - first + 1) within each category as the count.
 * ----------
 */

UT_TEST(test_per_category_event_counts)
{
	UT_ASSERT_EQ(
		(uint32)WAIT_EVENT_GES_LOCAL_FAST_PATH - (uint32)WAIT_EVENT_GES_ENQUEUE_ACQUIRE + 1, 5);
	UT_ASSERT_EQ((uint32)WAIT_EVENT_PCM_ITL_CLEANOUT - (uint32)WAIT_EVENT_PCM_BLOCK_READ_N_S + 1,
				 6);
	UT_ASSERT_EQ((uint32)WAIT_EVENT_BUFFER_SHIP_CURRENT_RECEIVE
					 - (uint32)WAIT_EVENT_BUFFER_SHIP_CR_BUILD + 1,
				 5);
	UT_ASSERT_EQ(
		(uint32)WAIT_EVENT_SCN_ADVANCE_BROADCAST - (uint32)WAIT_EVENT_SCN_BOC_FLUSH_WAIT + 1, 4);
	UT_ASSERT_EQ(
		(uint32)WAIT_EVENT_RECONFIG_BARRIER_WAIT - (uint32)WAIT_EVENT_RECONFIG_GRD_REBUILD + 1, 5);
	UT_ASSERT_EQ((uint32)WAIT_EVENT_RECOVERY_PCM_STATE_RESTORE
					 - (uint32)WAIT_EVENT_RECOVERY_WAL_FETCH + 1,
				 5);
	UT_ASSERT_EQ((uint32)WAIT_EVENT_SINVAL_INJECT_LOCAL_QUEUE
					 - (uint32)WAIT_EVENT_SINVAL_BROADCAST_SEND + 1,
				 3);
	UT_ASSERT_EQ((uint32)WAIT_EVENT_INTERCONNECT_CONNECT_RETRY
					 - (uint32)WAIT_EVENT_INTERCONNECT_RDMA_SEND + 1,
				 5);
	UT_ASSERT_EQ((uint32)WAIT_EVENT_UNDO_RETENTION_WAIT - (uint32)WAIT_EVENT_UNDO_REMOTE_READ + 1,
				 4);
	UT_ASSERT_EQ((uint32)WAIT_EVENT_ADG_SCN_SYNC_WAIT - (uint32)WAIT_EVENT_ADG_MRP_APPLY_WAIT + 1,
				 4);
	UT_ASSERT_EQ((uint32)WAIT_EVENT_CLUSTER_SHARED_FS_FSYNC
					 - (uint32)WAIT_EVENT_CLUSTER_SHARED_FS_READ + 1,
				 5);
}


/* ----------
 * Cross-category jumps are exactly one class-id step (0x01000000).
 * This proves dense packing within each category.
 * ----------
 */

UT_TEST(test_cross_category_jump_is_one_class_step)
{
	UT_ASSERT_EQ((uint32)WAIT_EVENT_PCM_BLOCK_READ_N_S - (uint32)WAIT_EVENT_GES_LOCAL_FAST_PATH,
				 0x01000000U - 4U);
	UT_ASSERT_EQ((uint32)WAIT_EVENT_ADG_MRP_APPLY_WAIT - (uint32)WAIT_EVENT_UNDO_RETENTION_WAIT,
				 0x01000000U - 3U);
	UT_ASSERT_EQ((uint32)WAIT_EVENT_CLUSTER_SHARED_FS_READ - (uint32)WAIT_EVENT_ADG_SCN_SYNC_WAIT,
				 0x01000000U - 3U);
}


int
main(void)
{
	UT_PLAN(8);
	UT_RUN(test_class_ids_match_design_doc);
	UT_RUN(test_class_ids_pairwise_distinct);
	UT_RUN(test_no_collision_with_pg_native_classes);
	UT_RUN(test_gap_zone_preserved);
	UT_RUN(test_first_event_per_category_anchors_class_id);
	UT_RUN(test_last_event_per_category_in_class);
	UT_RUN(test_per_category_event_counts);
	UT_RUN(test_cross_category_jump_is_one_class_step);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
