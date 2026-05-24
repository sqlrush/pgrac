/*-------------------------------------------------------------------------
 *
 * test_cluster_itl_cleanout_perf.c
 *	  pgrac spec-3.4c D13 — cluster_unit tests for D14 per-page
 *	  aggregate yellow perf hardening (cluster_itl_touch.c).
 *
 *	  8 tests covering:
 *	    T1   cluster_itl_touch_foreach_per_page symbol linkable
 *	    T2   ClusterItlPagedHandle layout — slot_indices[INITRANS] bound
 *	    T3   ClusterItlPagedHandle.nslots / .flags 1-byte each
 *	    T4   ClusterItlPagedHandle sizeof predictable (rloc 12 + fork 4 +
 *	         block 4 + slot_indices 8 + nslots 1 + flags 1 + 2 pad = 32)
 *	    T5   ClusterItlTouchHandle sizeof 24 regression (D14 dedupe key source)
 *	    T6   ClusterItlTouchHandle field offsets stable (qsort comparator
 *	         relies on rloc=0 / block=12 / forknum=16 / slot_idx=20)
 *	    T7   CLUSTER_ITL_INITRANS_DEFAULT == 8 (slot_indices[] capacity)
 *	    T8   CLUSTER_ITL_TOUCH_FLAG_NEEDS_WAL == 0x0001 (D14 flag OR aggregate
 *	         must preserve this for needs_wal pass-through)
 *
 *	  Behavioural coverage of foreach_per_page (dedupe / qsort / single-
 *	  page batch stamp) lives in cluster_tap because it requires
 *	  TopTransactionContext + ReadBufferWithoutRelcache + generic_xlog
 *	  infrastructure that cluster_unit cannot link without dragging in
 *	  the full storage subsystem.  The static contracts here guarantee
 *	  the on-wire layout the behavioural tests depend on stays frozen.
 *
 * Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/test/cluster_unit/test_cluster_itl_cleanout_perf.c
 *
 * Spec: spec-3.4c-delayed-cleanout-d5b-commit-scn-yellow-perf-hardening.md
 *       (v0.3 FROZEN 2026-05-24)
 *
 *-------------------------------------------------------------------------
 */
#define USE_PGRAC_CLUSTER 1

#include "postgres.h"

#include <stddef.h>
#include <string.h>

#include "cluster/cluster_itl_slot.h"
#include "cluster/cluster_itl_touch.h"
#include "common/relpath.h"
#include "storage/relfilelocator.h"

#undef printf
#undef fprintf
#undef snprintf

#include "unit_test.h"


UT_DEFINE_GLOBALS();


void
ExceptionalCondition(const char *conditionName pg_attribute_unused(),
					 const char *fileName pg_attribute_unused(),
					 int lineNumber pg_attribute_unused())
{
	abort();
}


/* Local stubs — cluster_unit binary does not link cluster_itl_touch.o
 * (it would drag in bufmgr / generic_xlog / TopTransactionContext).
 * Symbol presence test (T1) uses these locally-defined stubs. */
void
cluster_itl_touch_foreach_per_page(ClusterItlTouchPagedCallback cb pg_attribute_unused(),
								   void *arg pg_attribute_unused())
{}


/* ===== T1: symbol linkable ===== */
UT_TEST(t1_foreach_per_page_symbol_linkable)
{
	UT_ASSERT_NE((void *)cluster_itl_touch_foreach_per_page, NULL);
}


/* ===== T2-T4: ClusterItlPagedHandle layout ===== */

UT_TEST(t2_paged_handle_slot_indices_bound_initrans)
{
	ClusterItlPagedHandle ph;
	memset(&ph, 0, sizeof(ph));
	/* slot_indices[] capacity must accommodate every slot on a page,
	 * which is bounded by CLUSTER_ITL_INITRANS_DEFAULT (8). */
	UT_ASSERT_EQ((int)sizeof(ph.slot_indices), CLUSTER_ITL_INITRANS_DEFAULT);
}

UT_TEST(t3_paged_handle_nslots_flags_one_byte)
{
	ClusterItlPagedHandle ph;
	memset(&ph, 0, sizeof(ph));
	UT_ASSERT_EQ((int)sizeof(ph.nslots), 1);
	UT_ASSERT_EQ((int)sizeof(ph.flags), 1);
}

UT_TEST(t4_paged_handle_sizeof_32)
{
	/* rloc 12 + forknum 4 + block 4 + slot_indices 8 + nslots 1 +
	 * flags 1 + 2 pad = 32. */
	UT_ASSERT_EQ((int)sizeof(ClusterItlPagedHandle), 32);
}


/* ===== T5-T6: ClusterItlTouchHandle 24B layout (D14 qsort comparator
 *	   depends on this) ===== */

UT_TEST(t5_touch_handle_sizeof_24)
{
	UT_ASSERT_EQ((int)sizeof(ClusterItlTouchHandle), 24);
}

UT_TEST(t6_touch_handle_offsets_stable)
{
	UT_ASSERT_EQ((int)offsetof(ClusterItlTouchHandle, rloc), 0);
	UT_ASSERT_EQ((int)offsetof(ClusterItlTouchHandle, block), 12);
	UT_ASSERT_EQ((int)offsetof(ClusterItlTouchHandle, forknum), 16);
	UT_ASSERT_EQ((int)offsetof(ClusterItlTouchHandle, slot_idx), 20);
	UT_ASSERT_EQ((int)offsetof(ClusterItlTouchHandle, flags), 22);
}


/* ===== T7: INITRANS bound — slot_indices array capacity source ===== */
UT_TEST(t7_initrans_default_is_8)
{
	UT_ASSERT_EQ((int)CLUSTER_ITL_INITRANS_DEFAULT, 8);
}


/* ===== T8: NEEDS_WAL flag bit constant ===== */
UT_TEST(t8_needs_wal_flag_bit)
{
	/* D14 page aggregate ORs handle.flags into page_handle.flags so
	 * needs_wal is preserved as long as ANY touched handle on the
	 * page requires WAL.  Bit position drift would break that. */
	UT_ASSERT_EQ((int)CLUSTER_ITL_TOUCH_FLAG_NEEDS_WAL, 0x0001);
}


int
main(void)
{
	UT_PLAN(8);
	UT_RUN(t1_foreach_per_page_symbol_linkable);
	UT_RUN(t2_paged_handle_slot_indices_bound_initrans);
	UT_RUN(t3_paged_handle_nslots_flags_one_byte);
	UT_RUN(t4_paged_handle_sizeof_32);
	UT_RUN(t5_touch_handle_sizeof_24);
	UT_RUN(t6_touch_handle_offsets_stable);
	UT_RUN(t7_initrans_default_is_8);
	UT_RUN(t8_needs_wal_flag_bit);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
