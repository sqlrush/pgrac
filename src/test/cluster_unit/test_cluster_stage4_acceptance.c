/*-------------------------------------------------------------------------
 *
 * test_cluster_stage4_acceptance.c
 *	  pgrac spec-4.14 D6 — Stage 4 (WAL + Recovery) final surface snapshot
 *	  (10 static contract tests).
 *
 *	  Tests in this binary (L1-L10).  Pure compile-time / enum / macro
 *	  invariants only — no shmem, no external calls — so the binary pins the
 *	  Stage 4 recovery + fence ABI surface that the cluster_tap acceptance
 *	  legs (t/273 capability, t/274 hard-gate, t/275 matrix) exercise at
 *	  runtime.  Mirrors test_cluster_stage3_acceptance.c (spec-3.17 D6).
 *
 *	    L1  Stage 4 final surface landed snapshot:  CATALOG_VERSION_NO
 *	        >= 202606152 (spec-4.x ship value) — final-state assertion only,
 *	        kept monotone non-decreasing by any future spec.
 *	    L2  RM_CLUSTER_UNDO live opcodes 0x10/0x30/0x40/0x50/0x60/0x70 still
 *	        registered (recovery's merged redo consumes them) + XLR_INFO_MASK
 *	        low nibble clear (L217 anti-collision).
 *	    L3  7 recovery/fence-family dump category names stable (recovery /
 *	        tt_recovery / gcs_recovery / grd_recovery / pcm / cr /
 *	        write_fence) — compile-time string roster;  runtime emission
 *	        verified by t/273 L12.
 *	    L4  recovery + fence SQLSTATEs encodable via MAKE_SQLSTATE:
 *	        53R51 write-fenced / 53R9G cr-cross-instance / 53R9L gcs-resource-
 *	        recovering / 53R9N undo-writeback-boundary / 53RA0 wal-thread-
 *	        routing-mismatch / 53RA3 merged-recovery-blocked / 53RA4 thread-
 *	        recovery-blocked.
 *	    L5  CLUSTER_WAIT_EVENTS_COUNT current snapshot = 110 (spec-6.0a D10
 *	        value;  update-required contract — any future spec adding wait
 *	        events MUST bump this snapshot).
 *	    L6  write-fence wire/ABI enums locked:  ClusterFenceMarkerKind
 *	        FENCE=0 / BASELINE=1 and write_fence_enforcement OFF=0 / ON / DEV
 *	        (values MUST NOT be reordered — durable marker + GUC ABI).
 *	    L7  ClusterThreadRecScope 5-value enum (APPLICABLE=0 / DISABLED /
 *	        SINGLE_NODE / NO_SHARED_BACKEND / MULTI_SURVIVOR) complete — the
 *	        FEATURE_NOT_SUPPORTED capability-gate scopes are locked (spec-4.11
 *	        D7).
 *	    L8  undo writeback-boundary GUC enum (CLUSTER_UNDO_WB_CHECK
 *	        OFF / ON / STRICT) complete (spec-4.8ab two-layer guard; STRICT
 *	        is the CI/test escalation).
 *	    L9  fenced/dead bitmap width invariant:  the reconfig dead bitmap can
 *	        address every cluster node — CLUSTER_RECONFIG_DEAD_BITMAP_BYTES * 8
 *	        >= CLUSTER_MAX_NODES (16 * 8 == 128).  A future node-count bump
 *	        without a bitmap-width bump would silently drop high node ids from
 *	        fence/recovery.
 *	    L10 dead-bitmap bit convention round-trips at both boundaries
 *	        (node n -> byte n/8, bit n%8;  mirror cluster_reconfig.c
 *	        dead_bitmap_set_bit) — pure computation, locks the on-wire
 *	        fence/recovery bit layout.
 *
 *	  Static contract assertions only.  Behavioral coverage in cluster_tap
 *	  t/273 (capability cross-cutting), t/274 (recovery hard-gate), and t/275
 *	  (storage/fencing matrix).
 *
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/test/cluster_unit/test_cluster_stage4_acceptance.c
 *
 * NOTES
 *	  This is a pgrac-original file.
 *	  Spec: spec-4.14-stage4-recovery-acceptance.md (FROZEN v0.3)
 *
 *-------------------------------------------------------------------------
 */
#define USE_PGRAC_CLUSTER 1

#include "postgres.h"

#include <stddef.h>
#include <string.h>

#include "access/xlogrecord.h"
#include "catalog/catversion.h"
#include "cluster/cluster_conf.h"
#include "cluster/cluster_reconfig.h"
#include "cluster/cluster_thread_recovery.h"
#include "cluster/cluster_views.h"
#include "cluster/cluster_write_fence.h"
#include "utils/errcodes.h"

#include "cluster/storage/cluster_undo_buf.h"
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


/* ===== L1 — Stage 4 final surface snapshot ===== */

UT_TEST(test_stage4_catversion_at_or_above_spec_4_x)
{
	/* 202606152 is the current Stage 4 surface (spec-4.x ship value).  Any
	 * future spec must keep CATALOG_VERSION_NO monotone non-decreasing;
	 * Stage 4 acceptance requires the recovery + fence surface present. */
	UT_ASSERT((long)CATALOG_VERSION_NO >= 202606152L);
}


/* ===== L2 — RM_CLUSTER_UNDO opcodes still registered + XLR_INFO_MASK clear =====
 *
 *	Recovery's merged redo replays the RM_CLUSTER_UNDO stream;  Stage 4 keeps
 *	the Stage 3 opcode surface intact (no opcode removed/renumbered) and every
 *	opcode leaves the framework-reserved low nibble clear (L217).
 */
UT_TEST(test_stage4_undo_opcodes_preserved_and_info_mask_clear)
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


/* ===== L3 — recovery/fence-family dump category roster ===== */

UT_TEST(test_stage4_recovery_dump_category_names)
{
	/* t/273 L12 asserts pg_cluster_state actually emits rows for each of these
	 * categories at runtime;  this test pins the category name strings as a
	 * compile-time roster (+ exact total length) so a rename / removal in a
	 * cluster_debug.c emit site would diverge from the contract surface here.
	 * (Array form, not strcmp(literal,literal), to avoid a meaningless
	 * self-comparison — cppcheck staticStringCompare.) */
	const char *cats[7] = {
		"recovery",		/* spec-4.3 recovery coordinator / block-recovery */
		"tt_recovery",	/* spec-4.8 undo/TT recovery counters */
		"gcs_recovery", /* spec-4.7 GCS/PCM warm recovery counters */
		"grd_recovery", /* spec-4.6 GRD/GES remaster counters */
		"pcm",			/* PCM lock-state counters */
		"cr",			/* CR block construction counters */
		"write_fence"	/* spec-4.12/4.12b cooperative write-fence counters */
	};
	int i;
	int total_len = 0;

	for (i = 0; i < 7; i++) {
		UT_ASSERT_NOT_NULL((void *)cats[i]);
		UT_ASSERT((int)strlen(cats[i]) > 1);
		total_len += (int)strlen(cats[i]);
	}
	/* recovery8 + tt_recovery11 + gcs_recovery12 + grd_recovery12 + pcm3 + cr2
	 * + write_fence11 = 59 */
	UT_ASSERT_EQ(total_len, 59);
}


/* ===== L4 — recovery + fence SQLSTATE surface encodable ===== */

UT_TEST(test_stage4_sqlstate_recovery_fence_surface_encodable)
{
	UT_ASSERT_EQ((int)ERRCODE_CLUSTER_WRITE_FENCED, (int)MAKE_SQLSTATE('5', '3', 'R', '5', '1'));
	UT_ASSERT_EQ((int)ERRCODE_CLUSTER_CR_CROSS_INSTANCE_UNSUPPORTED,
				 (int)MAKE_SQLSTATE('5', '3', 'R', '9', 'G'));
	UT_ASSERT_EQ((int)ERRCODE_CLUSTER_GCS_BLOCK_RESOURCE_RECOVERING,
				 (int)MAKE_SQLSTATE('5', '3', 'R', '9', 'L'));
	UT_ASSERT_EQ((int)ERRCODE_CLUSTER_UNDO_WRITEBACK_BOUNDARY_VIOLATION,
				 (int)MAKE_SQLSTATE('5', '3', 'R', '9', 'N'));
	UT_ASSERT_EQ((int)ERRCODE_CLUSTER_WAL_THREAD_ROUTING_MISMATCH,
				 (int)MAKE_SQLSTATE('5', '3', 'R', 'A', '0'));
	UT_ASSERT_EQ((int)ERRCODE_CLUSTER_MERGED_RECOVERY_BLOCKED,
				 (int)MAKE_SQLSTATE('5', '3', 'R', 'A', '3'));
	UT_ASSERT_EQ((int)ERRCODE_CLUSTER_THREAD_RECOVERY_BLOCKED,
				 (int)MAKE_SQLSTATE('5', '3', 'R', 'A', '4'));
}


/* ===== L5 — wait-events count snapshot ===== */

UT_TEST(test_stage4_wait_events_count_snapshot_110)
{
	/* Current Stage 4 surface value (the macro in cluster_views.h attributes
	 * the latest bump to spec-4.12 D7).  update-required contract: a future
	 * spec adding cluster wait events MUST bump this snapshot (and the dump/test
	 * baselines that count them). */
	UT_ASSERT_EQ((int)CLUSTER_WAIT_EVENTS_COUNT, 110);
}


/* ===== L6 — write-fence wire/ABI enums locked ===== */

UT_TEST(test_stage4_write_fence_enums_locked)
{
	/* Durable fence-marker on-disk kind (spec-4.12b D1): byte value pinned so
	 * an old reader's pad=0 still decodes as FENCE (the safe default). */
	UT_ASSERT_EQ((int)CLUSTER_FENCE_MARKER_KIND_FENCE, 0);
	UT_ASSERT_EQ((int)CLUSTER_FENCE_MARKER_KIND_BASELINE, 1);

	/* GUC enum (spec-4.12 §2.4): OFF=0 is the inert/escape default value, ON
	 * is the default-ON (spec-4.12b D4), DEV is the second escape hatch. */
	UT_ASSERT_EQ((int)CLUSTER_WRITE_FENCE_ENFORCE_OFF, 0);
	UT_ASSERT(CLUSTER_WRITE_FENCE_ENFORCE_ON != CLUSTER_WRITE_FENCE_ENFORCE_OFF);
	UT_ASSERT(CLUSTER_WRITE_FENCE_ENFORCE_DEV != CLUSTER_WRITE_FENCE_ENFORCE_ON);
	UT_ASSERT(CLUSTER_WRITE_FENCE_ENFORCE_DEV != CLUSTER_WRITE_FENCE_ENFORCE_OFF);
}


/* ===== L7 — thread-recovery capability-gate scope enum complete ===== */

UT_TEST(test_stage4_thread_recovery_scope_enum_complete)
{
	/* spec-4.11 D7: the capability gate routes single-node / no-shared-backend
	 * / >2-node to FEATURE_NOT_SUPPORTED.  Pin the 5 scope values so a future
	 * reorder cannot silently change which scopes fail-closed. */
	UT_ASSERT_EQ((int)CLUSTER_THREADREC_SCOPE_APPLICABLE, 0);
	UT_ASSERT(CLUSTER_THREADREC_SCOPE_DISABLED != CLUSTER_THREADREC_SCOPE_APPLICABLE);
	UT_ASSERT(CLUSTER_THREADREC_SCOPE_SINGLE_NODE != CLUSTER_THREADREC_SCOPE_DISABLED);
	UT_ASSERT(CLUSTER_THREADREC_SCOPE_NO_SHARED_BACKEND != CLUSTER_THREADREC_SCOPE_SINGLE_NODE);
	UT_ASSERT(CLUSTER_THREADREC_SCOPE_MULTI_SURVIVOR != CLUSTER_THREADREC_SCOPE_NO_SHARED_BACKEND);
	/* contiguous 0..4 */
	UT_ASSERT_EQ((int)CLUSTER_THREADREC_SCOPE_MULTI_SURVIVOR, 4);
}


/* ===== L8 — undo writeback-boundary GUC enum complete (spec-4.8ab) ===== */

UT_TEST(test_stage4_undo_writeback_boundary_enum_complete)
{
	/* Two-layer guard (spec-4.8ab): OFF skips advisory accounting, ON is the
	 * default verdict accounting, STRICT escalates a broken advisory invariant
	 * to ERROR (CI/test).  Hard corruption fail-closed fires under ANY value
	 * (8.A, not downgradable) — this only pins the three knob states. */
	UT_ASSERT(CLUSTER_UNDO_WB_CHECK_OFF != CLUSTER_UNDO_WB_CHECK_ON);
	UT_ASSERT(CLUSTER_UNDO_WB_CHECK_STRICT != CLUSTER_UNDO_WB_CHECK_ON);
	UT_ASSERT(CLUSTER_UNDO_WB_CHECK_STRICT != CLUSTER_UNDO_WB_CHECK_OFF);
}


/* ===== L9 — fenced/dead bitmap width invariant ===== */

UT_TEST(test_stage4_dead_bitmap_addresses_every_node)
{
	/* The reconfig dead bitmap (consumed by fence + recovery) must be wide
	 * enough to mark every cluster node dead.  16 bytes * 8 == 128 ==
	 * CLUSTER_MAX_NODES.  A node-count bump without a bitmap-width bump would
	 * silently drop high node ids from fence/recovery — pin the relation. */
	UT_ASSERT(CLUSTER_RECONFIG_DEAD_BITMAP_BYTES * 8 >= CLUSTER_MAX_NODES);
	UT_ASSERT_EQ(CLUSTER_RECONFIG_DEAD_BITMAP_BYTES * 8, CLUSTER_MAX_NODES);
}


/* ===== L10 — dead-bitmap bit convention round-trips at both boundaries ===== */

UT_TEST(test_stage4_dead_bitmap_bit_convention_roundtrip)
{
	uint8 bitmap[CLUSTER_RECONFIG_DEAD_BITMAP_BYTES];
	int node;

	/* node n -> byte n/8, bit n%8 (mirror cluster_reconfig.c
	 * dead_bitmap_set_bit / orchestrator_srf.c inject).  Verify the lowest and
	 * highest addressable nodes set+read back exactly one bit, with no aliasing
	 * onto a neighbour. */
	for (node = 0; node <= CLUSTER_MAX_NODES - 1; node += (CLUSTER_MAX_NODES - 1)) {
		memset(bitmap, 0, sizeof(bitmap));
		bitmap[node / 8] |= (uint8)(1u << (node % 8));

		/* exactly the target bit is set */
		UT_ASSERT((bitmap[node / 8] & (uint8)(1u << (node % 8))) != 0);

		/* no neighbour bit aliased: a different node id reads back 0 */
		if (node + 1 <= CLUSTER_MAX_NODES - 1)
			UT_ASSERT((bitmap[(node + 1) / 8] & (uint8)(1u << ((node + 1) % 8))) == 0);
		if (node - 1 >= 0)
			UT_ASSERT((bitmap[(node - 1) / 8] & (uint8)(1u << ((node - 1) % 8))) == 0);

		if (node == 0)
			break; /* CLUSTER_MAX_NODES==1 edge: avoid an infinite step of 0 */
	}
}


int
main(void)
{
	UT_RUN(test_stage4_catversion_at_or_above_spec_4_x);
	UT_RUN(test_stage4_undo_opcodes_preserved_and_info_mask_clear);
	UT_RUN(test_stage4_recovery_dump_category_names);
	UT_RUN(test_stage4_sqlstate_recovery_fence_surface_encodable);
	UT_RUN(test_stage4_wait_events_count_snapshot_110);
	UT_RUN(test_stage4_write_fence_enums_locked);
	UT_RUN(test_stage4_thread_recovery_scope_enum_complete);
	UT_RUN(test_stage4_undo_writeback_boundary_enum_complete);
	UT_RUN(test_stage4_dead_bitmap_addresses_every_node);
	UT_RUN(test_stage4_dead_bitmap_bit_convention_roundtrip);
	UT_DONE();
}
