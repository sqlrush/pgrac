/*-------------------------------------------------------------------------
 *
 * test_cluster_stage5_beta_acceptance.c
 *	  pgrac spec-5.21 D3 — Stage 5 (through 5.21) beta close-out release
 *	  surface snapshot (6 static contract tests).
 *
 *	  Tests in this binary (L1-L6).  Pure compile-time / enum / macro / struct
 *	  invariants only — no shmem, no external calls — so the binary pins the
 *	  Stage 5 RAC-core release surface that the beta close-out is cut against.
 *	  Mirrors test_cluster_stage5_integrated_acceptance.c (spec-5.19 D8) and
 *	  test_cluster_stage4_acceptance.c (spec-4.14 D6).
 *
 *	    L1  Beta release surface snapshot:  CATALOG_VERSION_NO >= 202606380
 *	        (current ship value) — final-state floor only, kept monotone
 *	        non-decreasing by any future spec.  No on-disk change in 5.21.
 *	    L2  RM_CLUSTER_UNDO live opcodes 0x10/0x30/0x40/0x50/0x60/0x70 still
 *	        registered (crash-recovery redo consumes them) + XLR_INFO_MASK low
 *	        nibble clear (L217 anti-collision) — the beta keeps the cluster WAL
 *	        opcode surface intact.
 *	    L3  Stage 5 wait-event roster: the reconfig waits (5.13-5.16) and the
 *	        multi-node write-path waits are all present and pairwise distinct,
 *	        so a reorder/removal at the release commit would fail compile here.
 *	        Runtime dump emission verified by the beta integrated-acceptance
 *	        TAP (t/345).
 *	    L4  beta-acceptance SQLSTATE surface encodable via MAKE_SQLSTATE:
 *	        53R51 write-fenced / 53RA6 relation-extend / 53R60 reconfig-in-
 *	        progress / 53R61 join-rejected-stale / 53R62 clean-leave-in-progress
 *	        / 53R64 node-removed-fenced / 53R70 ges-timeout / 55R01
 *	        pcm-state-invalid.
 *	    L5  CLUSTER_WAIT_EVENTS_COUNT current snapshot = 118 (update-required
 *	        contract) — a future spec adding cluster wait events MUST bump this
 *	        snapshot and the dump/test baselines that count them.
 *	    L6  7 RAC core presence (roadmap: only all seven make it "RAC core"):
 *	        each core subsystem's sentinel symbol/enum is referenced at
 *	        compile time, so a release commit that accidentally dropped a core
 *	        subsystem would fail to build here — the beta cannot be cut without
 *	        all seven present.
 *	          1 shared storage      CLUSTER_SMGR_SMGRSW_INDEX
 *	          2 Cache Fusion + PCM   PCM_STATE_N/S/X + GcsBlockRequestPayload
 *	          3 cross-node MVCC      ClusterCrVerdict tri-state
 *	          4 crash recovery       RM_CLUSTER_UNDO_ID + XLOG_UNDO_BLOCK_WRITE
 *	          5 fencing              ERRCODE_CLUSTER_WRITE_FENCED (53R51)
 *	          6 GES + deadlock       GES_MODE_FIRST..LAST + CLUSTER_DL_RESID_TYPE
 *	          7 online reconfig      RECONFIG_KIND_{FAIL_STOP,CLEAN_LEAVE,
 *	                                 JOIN_COMMITTED} + ERRCODE_CLUSTER_NODE_
 *	                                 REMOVED_FENCED
 *
 *	  Static contract assertions only.  Behavioral coverage in cluster_tap
 *	  t/344 (chaos soak) and t/345 (integrated beta acceptance).
 *
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/test/cluster_unit/test_cluster_stage5_beta_acceptance.c
 *
 * NOTES
 *	  This is a pgrac-original file.
 *	  Spec: spec-5.21-stage5-beta-acceptance.md (FROZEN v1.0)
 *
 *-------------------------------------------------------------------------
 */
#define USE_PGRAC_CLUSTER 1

#include "postgres.h"

#include <stddef.h>
#include <string.h>

#include "access/rmgr.h"
#include "access/xlogrecord.h"
#include "catalog/catversion.h"
#include "utils/errcodes.h"
#include "utils/wait_event.h"

#include "cluster/cluster_views.h"
#include "cluster/storage/cluster_smgr.h"
#include "cluster/storage/cluster_undo_xlog.h"
#include "cluster/cluster_buffer_desc.h"
#include "cluster/cluster_cr.h"
#include "cluster/cluster_dl.h"
#include "cluster/cluster_ges_mode.h"
#include "cluster/cluster_gcs_block.h"
#include "cluster/cluster_reconfig.h"

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


/* ===== L1 — beta release surface snapshot ===== */

UT_TEST(test_beta_catversion_floor)
{
	/* 202606380 is the current ship surface at the beta close-out commit.  5.21
	 * introduces no on-disk change, so catversion must not drop below this; any
	 * future spec keeps CATALOG_VERSION_NO monotone non-decreasing. */
	UT_ASSERT((long)CATALOG_VERSION_NO >= 202606380L);
}


/* ===== L2 — RM_CLUSTER_UNDO opcodes still registered + XLR_INFO_MASK clear ===== */

UT_TEST(test_beta_undo_opcodes_preserved_and_info_mask_clear)
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


/* ===== L3 — Stage 5 reconfig + write-path wait-event roster present/distinct ===== */

UT_TEST(test_beta_stage5_wait_event_roster)
{
	/* Reconfig waits (5.13-5.16 online reconfiguration) present + distinct — a
	 * release commit that dropped the reconfig wait surface would diverge here. */
	UT_ASSERT(WAIT_EVENT_CLUSTER_BGPROC_LMON_RECONFIG_TICK
			  != WAIT_EVENT_CLUSTER_GRD_SHARD_REMASTER);

	/* Multi-node write-path waits (GES / Cache Fusion enqueue / CR construct /
	 * relation-extend) present + pairwise distinct. */
	UT_ASSERT(WAIT_EVENT_CLUSTER_GES_S4_WAIT != WAIT_EVENT_CLUSTER_GES_REPLY_WAIT);
	UT_ASSERT(WAIT_EVENT_CLUSTER_GES_REPLY_WAIT != WAIT_EVENT_CLUSTER_CF_ENQUEUE);
	UT_ASSERT(WAIT_EVENT_CLUSTER_CF_ENQUEUE != WAIT_EVENT_CLUSTER_CR_CONSTRUCT);
	UT_ASSERT(WAIT_EVENT_CLUSTER_CR_CONSTRUCT != WAIT_EVENT_CLUSTER_REL_EXTEND_WAIT);
	UT_ASSERT(WAIT_EVENT_CLUSTER_REL_EXTEND_WAIT != WAIT_EVENT_CLUSTER_GES_S4_WAIT);
}


/* ===== L4 — beta-acceptance SQLSTATE surface encodable ===== */

UT_TEST(test_beta_sqlstate_acceptance_surface_encodable)
{
	UT_ASSERT_EQ((int)ERRCODE_CLUSTER_WRITE_FENCED, (int)MAKE_SQLSTATE('5', '3', 'R', '5', '1'));
	UT_ASSERT_EQ((int)ERRCODE_CLUSTER_RELATION_EXTEND_UNAVAILABLE,
				 (int)MAKE_SQLSTATE('5', '3', 'R', 'A', '6'));
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


/* ===== L5 — wait-events count snapshot (update-required contract) ===== */

UT_TEST(test_beta_wait_events_count)
{
	/* Current Stage 5 beta surface value.  update-required contract: a future
	 * spec adding cluster wait events MUST bump this snapshot (and the dump/test
	 * baselines that count them). */
	UT_ASSERT_EQ((int)CLUSTER_WAIT_EVENTS_COUNT, 118);
}


/* ===== L6 — 7 RAC core presence code-level sentinels ===== */

UT_TEST(test_beta_seven_rac_core_present)
{
	/* Core 1 — shared storage: the cluster smgr occupies the second smgrsw[]
	 * slot so two instances can open the same relation concurrently. */
	UT_ASSERT_EQ((int)CLUSTER_SMGR_SMGRSW_INDEX, 1);

	/* Core 2 — Cache Fusion + PCM: the block-lock state machine (N/S/X) and the
	 * Cache Fusion block-ship wire ABI are present. */
	UT_ASSERT_EQ((int)PCM_STATE_N, 0);
	UT_ASSERT_EQ((int)PCM_STATE_S, 1);
	UT_ASSERT_EQ((int)PCM_STATE_X, 2);
	UT_ASSERT((int)sizeof(GcsBlockRequestPayload) > 0);

	/* Core 3 — cross-node MVCC: the CR gate's tri-state verdict is present and
	 * its three outcomes are distinct. */
	UT_ASSERT_EQ((int)CLUSTER_CR_NOT_APPLICABLE, 0);
	UT_ASSERT(CLUSTER_CR_DECIDED != CLUSTER_CR_NOT_APPLICABLE);
	UT_ASSERT(CLUSTER_CR_FAILCLOSED != CLUSTER_CR_DECIDED);

	/* Core 4 — cluster crash recovery: the cluster undo resource manager is
	 * registered (replays cluster WAL during recovery) and its block-write
	 * opcode is intact. */
	UT_ASSERT((int)RM_CLUSTER_UNDO_ID > (int)RM_XLOG_ID);
	UT_ASSERT((int)RM_CLUSTER_UNDO_ID <= (int)RM_MAX_ID);
	UT_ASSERT_EQ((int)XLOG_UNDO_BLOCK_WRITE, 0x70);

	/* Core 5 — fencing / split-brain: the fail-closed write-fence SQLSTATE is
	 * encodable. */
	UT_ASSERT_EQ((int)ERRCODE_CLUSTER_WRITE_FENCED, (int)MAKE_SQLSTATE('5', '3', 'R', '5', '1'));

	/* Core 6 — full GES + cross-node deadlock: the GES enqueue mode range and
	 * the deadlock detector's cluster resource-id type are present. */
	UT_ASSERT_EQ((int)GES_MODE_FIRST, 1);
	UT_ASSERT_EQ((int)GES_MODE_LAST, 8);
	UT_ASSERT_EQ((int)GES_MODE_COUNT, 8);
	UT_ASSERT_EQ((int)CLUSTER_DL_RESID_TYPE, 0xF3);

	/* Core 7 — online reconfiguration (join + clean leave + removal): the
	 * reconfig-kind enum carries all live edges and the node-removed fence
	 * SQLSTATE is encodable. */
	UT_ASSERT_EQ((int)RECONFIG_KIND_FAIL_STOP, 1);
	UT_ASSERT_EQ((int)RECONFIG_KIND_CLEAN_LEAVE, 2);
	UT_ASSERT_EQ((int)RECONFIG_KIND_JOIN_COMMITTED, 4);
	UT_ASSERT_EQ((int)ERRCODE_CLUSTER_NODE_REMOVED_FENCED,
				 (int)MAKE_SQLSTATE('5', '3', 'R', '6', '4'));
}


int
main(void)
{
	UT_RUN(test_beta_catversion_floor);
	UT_RUN(test_beta_undo_opcodes_preserved_and_info_mask_clear);
	UT_RUN(test_beta_stage5_wait_event_roster);
	UT_RUN(test_beta_sqlstate_acceptance_surface_encodable);
	UT_RUN(test_beta_wait_events_count);
	UT_RUN(test_beta_seven_rac_core_present);
	UT_DONE();
}
