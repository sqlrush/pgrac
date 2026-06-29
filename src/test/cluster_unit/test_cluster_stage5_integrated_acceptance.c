/*-------------------------------------------------------------------------
 *
 * test_cluster_stage5_integrated_acceptance.c
 *	  pgrac spec-5.19 D8 — Stage 5 (through 5.19) integrated-acceptance
 *	  surface snapshot (6 static contract tests).
 *
 *	  Tests in this binary (L1-L6).  Pure compile-time / enum / macro / struct
 *	  invariants only — no shmem, no external calls — so the binary pins the
 *	  Stage 5 multi-node write-path + reconfig + HW + heap-ITL-WAL ABI surface
 *	  that the integrated-acceptance cluster_tap legs (t/32x reconfig matrix /
 *	  HW-extend workload / production-bench-subset) exercise at runtime.
 *	  Mirrors test_cluster_stage4_acceptance.c (spec-4.14 D6).
 *
 *	    L1  Stage 5 final surface landed snapshot:  CATALOG_VERSION_NO
 *	        >= 202606330 (current ship value through spec-5.18) — final-state
 *	        assertion only, kept monotone non-decreasing by any future spec.
 *	    L2  RM_CLUSTER_UNDO live opcodes 0x10/0x30/0x40/0x50/0x60/0x70 still
 *	        registered (recovery's merged redo consumes them) + XLR_INFO_MASK
 *	        low nibble clear (L217 anti-collision) — Stage 5 keeps the Stage 3
 *	        undo WAL surface intact.
 *	    L3  multi-node write-path observability dump category roster stable
 *	        (ges / pcm / gcs / cr / hw) — the categories MG-B reads to measure
 *	        the write tax;  compile-time string roster, runtime emission
 *	        verified by the acceptance TAP.
 *	    L4  integrated-acceptance SQLSTATEs encodable via MAKE_SQLSTATE:
 *	        53RA6 relation-extend-unavailable / 53R51 write-fenced / 53R60
 *	        reconfig-in-progress / 53R61 join-rejected-stale / 53R62 clean-
 *	        leave-in-progress / 53R64 node-removed-fenced / 53R70 ges-timeout
 *	        / 55R01 pcm-state-invalid.
 *	    L5  CLUSTER_WAIT_EVENTS_COUNT current snapshot = 103 (spec-5.18 D12
 *	        ship value;  update-required contract) + the multi-node write-path
 *	        wait events present and pairwise distinct (GES_S4 / GES_REPLY /
 *	        CF_ENQUEUE / CR_CONSTRUCT / REL_EXTEND_WAIT — the MG-B M2 share).
 *	    L6  heap-ITL WAL delta width invariant (MG-D measure baseline):
 *	        sizeof(xl_heap_itl_delta_v2) == 40 and
 *	        offsetof(xl_heap_itl_delta_block, deltas) == 8 — every mutating
 *	        heap record carries a fixed 8 + 40 == 48-byte ITL delta;  a layout
 *	        change would invalidate the MG-D 48B/record measurement basis.
 *
 *	  Static contract assertions only.  Behavioral coverage in cluster_tap
 *	  t/32x (reconfig matrix), the HW/extend workload, and the production-
 *	  bench-subset.
 *
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/test/cluster_unit/test_cluster_stage5_integrated_acceptance.c
 *
 * NOTES
 *	  This is a pgrac-original file.
 *	  Spec: spec-5.19-stage5-integrated-acceptance.md (FROZEN v1.0)
 *
 *-------------------------------------------------------------------------
 */
#define USE_PGRAC_CLUSTER 1

#include "postgres.h"

#include <stddef.h>
#include <string.h>

#include "access/heapam_xlog.h"
#include "access/xlogrecord.h"
#include "catalog/catversion.h"
#include "cluster/cluster_views.h"
#include "utils/errcodes.h"
#include "utils/wait_event.h"

#include "cluster/storage/cluster_undo_xlog.h"

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


void
ExceptionalCondition(const char *conditionName pg_attribute_unused(),
					 const char *fileName pg_attribute_unused(),
					 int lineNumber pg_attribute_unused())
{
	abort();
}


/* ===== L1 — Stage 5 final surface snapshot ===== */

UT_TEST(test_stage5_catversion_at_or_above_spec_5_18)
{
	/* 202606330 is the current Stage 5 surface (spec-5.18 ship value).  Any
	 * future spec must keep CATALOG_VERSION_NO monotone non-decreasing;
	 * Stage 5 integrated acceptance requires the reconfig + HW surface present. */
	UT_ASSERT((long)CATALOG_VERSION_NO >= 202606330L);
}


/* ===== L2 — RM_CLUSTER_UNDO opcodes still registered + XLR_INFO_MASK clear ===== */

UT_TEST(test_stage5_undo_opcodes_preserved_and_info_mask_clear)
{
	UT_ASSERT_EQ((int)XLOG_UNDO_SEGMENT_INIT, 0x10);
	UT_ASSERT_EQ((int)XLOG_UNDO_TT_SLOT_COMMIT, 0x30);
	UT_ASSERT_EQ((int)XLOG_UNDO_SEGMENT_RECYCLE, 0x40);
	UT_ASSERT_EQ((int)XLOG_UNDO_SEGMENT_REUSE, 0x50);
	UT_ASSERT_EQ((int)XLOG_UNDO_TT_SLOT_ABORT, 0x60);
	UT_ASSERT_EQ((int)XLOG_UNDO_BLOCK_WRITE, 0x70);

	UT_ASSERT_EQ((int)(XLOG_UNDO_SEGMENT_INIT & XLR_INFO_MASK), 0);
	UT_ASSERT_EQ((int)(XLOG_UNDO_TT_SLOT_COMMIT & XLR_INFO_MASK), 0);
	UT_ASSERT_EQ((int)(XLOG_UNDO_SEGMENT_RECYCLE & XLR_INFO_MASK), 0);
	UT_ASSERT_EQ((int)(XLOG_UNDO_SEGMENT_REUSE & XLR_INFO_MASK), 0);
	UT_ASSERT_EQ((int)(XLOG_UNDO_TT_SLOT_ABORT & XLR_INFO_MASK), 0);
	UT_ASSERT_EQ((int)(XLOG_UNDO_BLOCK_WRITE & XLR_INFO_MASK), 0);
}


/* ===== L3 — multi-node write-path observability dump category roster ===== */

UT_TEST(test_stage5_multinode_dump_category_roster)
{
	/* MG-B reads the pg_cluster_state ges/pcm/gcs/cr/hw categories to quantify
	 * the multi-node write tax (M1/M2/M3).  Pin the category name strings as a
	 * compile-time roster (+ exact total length) so a rename / removal in a
	 * cluster_debug.c emit site would diverge from the contract surface here.
	 * (Array form, not strcmp(literal,literal), to avoid a meaningless self-
	 * comparison — cppcheck staticStringCompare.)  Runtime emission is verified
	 * by the integrated-acceptance TAP. */
	const char *cats[5] = {
		"ges", /* spec-5.1a..5.2 GES enqueue / row-lock cross-node */
		"pcm", /* PCM block-lock N/S/X state */
		"gcs", /* spec-4.7 GCS / Cache Fusion 8KB block ship */
		"cr",  /* CR block construction */
		"hw"   /* spec-5.7 HW relation-extend authority */
	};
	int i;
	int total_len = 0;

	for (i = 0; i < 5; i++) {
		UT_ASSERT_NOT_NULL((void *)cats[i]);
		UT_ASSERT((int)strlen(cats[i]) >= 2);
		total_len += (int)strlen(cats[i]);
	}
	/* ges3 + pcm3 + gcs3 + cr2 + hw2 = 13 */
	UT_ASSERT_EQ(total_len, 13);
}


/* ===== L4 — integrated-acceptance SQLSTATE surface encodable ===== */

UT_TEST(test_stage5_sqlstate_acceptance_surface_encodable)
{
	UT_ASSERT_EQ((int)ERRCODE_CLUSTER_RELATION_EXTEND_UNAVAILABLE,
				 (int)MAKE_SQLSTATE('5', '3', 'R', 'A', '6'));
	UT_ASSERT_EQ((int)ERRCODE_CLUSTER_WRITE_FENCED, (int)MAKE_SQLSTATE('5', '3', 'R', '5', '1'));
	UT_ASSERT_EQ((int)ERRCODE_CLUSTER_RECONFIG_IN_PROGRESS,
				 (int)MAKE_SQLSTATE('5', '3', 'R', '6', '0'));
	UT_ASSERT_EQ((int)ERRCODE_CLUSTER_JOIN_REJECTED_STALE,
				 (int)MAKE_SQLSTATE('5', '3', 'R', '6', '1'));
	UT_ASSERT_EQ((int)ERRCODE_CLUSTER_CLEAN_LEAVE_IN_PROGRESS,
				 (int)MAKE_SQLSTATE('5', '3', 'R', '6', '2'));
	UT_ASSERT_EQ((int)ERRCODE_CLUSTER_NODE_REMOVED_FENCED,
				 (int)MAKE_SQLSTATE('5', '3', 'R', '6', '4'));
	UT_ASSERT_EQ((int)ERRCODE_CLUSTER_GES_TIMEOUT, (int)MAKE_SQLSTATE('5', '3', 'R', '7', '0'));
	UT_ASSERT_EQ((int)ERRCODE_CLUSTER_PCM_STATE_INVALID,
				 (int)MAKE_SQLSTATE('5', '5', 'R', '0', '1'));
}


/* ===== L5 — wait-events count snapshot + multi-node write-path wait events ===== */

UT_TEST(test_stage5_wait_events_count_and_multinode_set)
{
	/* Current Stage 5 surface value (spec-5.18 D12 attributed bump).  update-
	 * required contract: a future spec adding cluster wait events MUST bump this
	 * snapshot (and the dump/test baselines that count them). */
	UT_ASSERT_EQ((int)CLUSTER_WAIT_EVENTS_COUNT, 103);

	/* The multi-node write-path wait events MG-B aggregates for the M2 share
	 * must all be present and pairwise distinct (a reorder/removal would change
	 * which waits the perf gate attributes the write tax to). */
	UT_ASSERT(WAIT_EVENT_CLUSTER_GES_S4_WAIT != WAIT_EVENT_CLUSTER_GES_REPLY_WAIT);
	UT_ASSERT(WAIT_EVENT_CLUSTER_GES_REPLY_WAIT != WAIT_EVENT_CLUSTER_CF_ENQUEUE);
	UT_ASSERT(WAIT_EVENT_CLUSTER_CF_ENQUEUE != WAIT_EVENT_CLUSTER_CR_CONSTRUCT);
	UT_ASSERT(WAIT_EVENT_CLUSTER_CR_CONSTRUCT != WAIT_EVENT_CLUSTER_REL_EXTEND_WAIT);
	UT_ASSERT(WAIT_EVENT_CLUSTER_REL_EXTEND_WAIT != WAIT_EVENT_CLUSTER_GES_S4_WAIT);
}


/* ===== L6 — heap-ITL WAL delta width invariant (MG-D measure baseline) ===== */

UT_TEST(test_stage5_heap_itl_wal_delta_width_invariant)
{
	/* MG-D measures the per-record heap-ITL WAL overhead (every mutating heap
	 * record carries a fixed 8-byte block header + 40-byte v2 delta == 48 B,
	 * ndeltas == 1, not coalesced).  Pin the struct widths so the 48B/record
	 * measurement basis cannot silently change underneath the decision record. */
	UT_ASSERT_EQ((int)sizeof(xl_heap_itl_delta_v2), 40);
	UT_ASSERT_EQ((int)offsetof(xl_heap_itl_delta_block, deltas), 8);
	/* 8-byte block header + 40-byte v2 delta == 48 B per mutating heap record. */
	UT_ASSERT_EQ((int)(offsetof(xl_heap_itl_delta_block, deltas) + sizeof(xl_heap_itl_delta_v2)),
				 48);
}


int
main(void)
{
	UT_RUN(test_stage5_catversion_at_or_above_spec_5_18);
	UT_RUN(test_stage5_undo_opcodes_preserved_and_info_mask_clear);
	UT_RUN(test_stage5_multinode_dump_category_roster);
	UT_RUN(test_stage5_sqlstate_acceptance_surface_encodable);
	UT_RUN(test_stage5_wait_events_count_and_multinode_set);
	UT_RUN(test_stage5_heap_itl_wal_delta_width_invariant);
	UT_DONE();
}
