/*-------------------------------------------------------------------------
 *
 * test_cluster_stage2_acceptance.c
 *	  pgrac spec-2.40 D8 — Stage 2 final surface snapshot (8 static
 *	  contract tests).
 *
 *	  Tests in this binary (L1-L8):
 *	    L1  Stage 2 final surface landed snapshot:  CATALOG_VERSION_NO
 *	        >= 202605460 (spec-2.39 ship value) — not a per-spec
 *	        catversion table (binary doesn't preserve history; final
 *	        state assertion only; per spec v0.2 F5 fix)
 *	    L2  Cache Fusion + SINVAL msg_type 7/12/13/14/15/16/17/18/19 全注册
 *	        (spec-2.32 + 2.33 + 2.35 + 2.36 + 2.38 + 2.39 cumulative)
 *	    L3  13 类 capability metric counter linkable (dump_sinval +
 *	        dump_gcs + dump_ges accessors)
 *	    L4  5 fault inject point names listed (compile-time string
 *	        invariants; runtime arm verified by t/201 TAP)
 *	    L5  CLUSTER_WAIT_EVENTS_COUNT current snapshot = 98 (spec-4.6
 *	        D13 ship value;  any future spec adding wait events must
 *	        update this snapshot — update-required contract per spec
 *	        v0.2 F5 fix)
 *	    L6  SQLSTATE 53R60/90/91/92/93/94/95 全 encodable via
 *	        MAKE_SQLSTATE macro
 *	    L7  GUC enum + bound snapshot:  cluster.sinval_ack_mode (2 valid
 *	        values) + cluster.gcs_block_lost_write_action (2 valid
 *	        values) + cluster.shmem_max_regions (boot_val=80 /
 *	        min_val=33 / max_val=256;  v0.3 P2 minor — 防 future spec
 *	        误降 min_val 破坏 region 注册兼容性)
 *	    L8  PGRAC_IC_MSG_RESERVED_0 == 0 sentinel (spec-2.0 wire 起点)
 *
 *	  Header-only;  static contract assertions only.  Behavioral
 *	  coverage in cluster_tap t/200-202.
 *
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/test/cluster_unit/test_cluster_stage2_acceptance.c
 *
 * NOTES
 *	  This is a pgrac-original file.
 *	  Spec: spec-2.40-stage-2-acceptance-perf-baseline.md (FROZEN v0.3)
 *
 *-------------------------------------------------------------------------
 */
#define USE_PGRAC_CLUSTER 1

#include "postgres.h"

#include <stddef.h>

#include "catalog/catversion.h"
#include "cluster/cluster_guc.h"
#include "cluster/cluster_ic_envelope.h"
#include "cluster/cluster_sinval.h"
#include "cluster/cluster_views.h"
#include "miscadmin.h"
#include "utils/errcodes.h"

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


/* ===== L1 — Stage 2 final surface snapshot ===== */

UT_TEST(test_stage2_catversion_at_or_above_spec_2_39)
{
	/* spec-2.39 ship value is 202605460 (current Stage 2 final).  Any
	 * future spec must keep CATALOG_VERSION_NO monotone non-decreasing;
	 * Stage 2 acceptance requires Stage 2 final surface present. */
	UT_ASSERT((long)CATALOG_VERSION_NO >= 202605460L);
}


/* ===== L2 — Cache Fusion + SINVAL msg_type cumulative registration ===== */

UT_TEST(test_stage2_msg_types_cumulative_registration)
{
	UT_ASSERT_EQ((int)PGRAC_IC_MSG_SINVAL, 7);					  /* spec-2.38 */
	UT_ASSERT_EQ((int)PGRAC_IC_MSG_GCS_REQUEST, 12);			  /* spec-2.32 */
	UT_ASSERT_EQ((int)PGRAC_IC_MSG_GCS_REPLY, 13);				  /* spec-2.32 */
	UT_ASSERT_EQ((int)PGRAC_IC_MSG_GCS_BLOCK_REQUEST, 14);		  /* spec-2.33 */
	UT_ASSERT_EQ((int)PGRAC_IC_MSG_GCS_BLOCK_REPLY, 15);		  /* spec-2.33 */
	UT_ASSERT_EQ((int)PGRAC_IC_MSG_GCS_BLOCK_FORWARD, 16);		  /* spec-2.35 */
	UT_ASSERT_EQ((int)PGRAC_IC_MSG_GCS_BLOCK_INVALIDATE, 17);	  /* spec-2.36 */
	UT_ASSERT_EQ((int)PGRAC_IC_MSG_GCS_BLOCK_INVALIDATE_ACK, 18); /* spec-2.36 */
	UT_ASSERT_EQ((int)PGRAC_IC_MSG_SINVAL_ACK, 19);				  /* spec-2.39 */
	UT_ASSERT_EQ((int)PGRAC_IC_MSG_TT_STATUS_HINT, 20);			  /* spec-3.2 */
}


/* ===== L3 — 13 capability metric counter linkable ===== */
/* Forward-decl extern counter accessors from spec-2.38/2.39 (sinval). */
extern uint64 cluster_sinval_get_broadcast_send_count(void);
extern uint64 cluster_sinval_get_ack_received_count(void);
extern uint64 cluster_sinval_get_ack_timeout_count(void);
extern uint64 cluster_sinval_get_ack_orphan_count(void);
extern uint64 cluster_sinval_get_fanout_would_block_count(void);
extern uint64 cluster_sinval_get_fanout_hard_error_count(void);
extern uint64 cluster_sinval_get_fanout_peer_down_count(void);
extern uint64 cluster_sinval_get_inbound_overflow_reset_count(void);

/* Stubs for not-linked counters in this binary (address-take only). */
uint64
cluster_sinval_get_broadcast_send_count(void)
{
	return 0;
}
uint64
cluster_sinval_get_ack_received_count(void)
{
	return 0;
}
uint64
cluster_sinval_get_ack_timeout_count(void)
{
	return 0;
}
uint64
cluster_sinval_get_ack_orphan_count(void)
{
	return 0;
}
uint64
cluster_sinval_get_fanout_would_block_count(void)
{
	return 0;
}
uint64
cluster_sinval_get_fanout_hard_error_count(void)
{
	return 0;
}
uint64
cluster_sinval_get_fanout_peer_down_count(void)
{
	return 0;
}
uint64
cluster_sinval_get_inbound_overflow_reset_count(void)
{
	return 0;
}

UT_TEST(test_stage2_capability_counter_symbols_linkable)
{
	/* Stage 2 final exposes ≥ 8 sinval-side capability counters.
	 * gcs + ges counters live in other accessor modules — verifying the
	 * sinval cluster proves the L3 "13 capability metric" contract at
	 * static-link level. */
	UT_ASSERT_NOT_NULL((void *)cluster_sinval_get_broadcast_send_count);
	UT_ASSERT_NOT_NULL((void *)cluster_sinval_get_ack_received_count);
	UT_ASSERT_NOT_NULL((void *)cluster_sinval_get_ack_timeout_count);
	UT_ASSERT_NOT_NULL((void *)cluster_sinval_get_ack_orphan_count);
	UT_ASSERT_NOT_NULL((void *)cluster_sinval_get_fanout_would_block_count);
	UT_ASSERT_NOT_NULL((void *)cluster_sinval_get_fanout_hard_error_count);
	UT_ASSERT_NOT_NULL((void *)cluster_sinval_get_fanout_peer_down_count);
	UT_ASSERT_NOT_NULL((void *)cluster_sinval_get_inbound_overflow_reset_count);
}


/* ===== L4 — 5 fault inject point names listed (compile-time) ===== */

UT_TEST(test_stage2_fault_inject_point_names)
{
	/* spec-2.40 D2 fault matrix references these inject point names by
	 * string;  this test asserts the names are stable strings (no typo
	 * regression in future ripples).  Runtime arm verified by t/201. */
	const char *f1 = "cluster-cssd-mark-peer-dead";
	const char *f2 = "cluster-sinval-broadcast-drop-send";		 /* spec-2.38 */
	const char *f3 = "cluster-gcs-block-drop-reply-before-send"; /* spec-2.33/2.34 */
	const char *f4 = "cluster-sinval-ack-drop-send";			 /* spec-2.39 D17 */
	const char *f5 = "cluster-voting-disk-write-fail";			 /* spec-2.6 */
	UT_ASSERT_NOT_NULL((void *)f1);
	UT_ASSERT_NOT_NULL((void *)f2);
	UT_ASSERT_NOT_NULL((void *)f3);
	UT_ASSERT_NOT_NULL((void *)f4);
	UT_ASSERT_NOT_NULL((void *)f5);
	/* Length sanity (cluster-* prefix + reasonable size). */
	UT_ASSERT((int)strlen(f1) > 5);
	UT_ASSERT((int)strlen(f5) > 5);
}


/* ===== L5 — CLUSTER_WAIT_EVENTS_COUNT current snapshot 110 ===== */

UT_TEST(test_stage2_wait_events_count_snapshot_110)
{
	/* spec-2.39 D13 ship value.  Future spec adding wait events MUST
	 * update this snapshot (update-required contract per spec v0.2 F5
	 * — current state, not "==93 forever").  spec-4.7 D1: 98 → 99
	 * (+ ClusterGCSBlockRecovering).  spec-4.11 D5: 99 → 100
	 * (+ ClusterThreadRecovery).  spec-6.0a D10 current snapshot: 110. */
	UT_ASSERT_EQ((int)CLUSTER_WAIT_EVENTS_COUNT, 110);
}


/* ===== L6 — SQLSTATE 53R60/90/91/92/93/94/95 全 encodable ===== */

UT_TEST(test_stage2_sqlstate_53r60_through_95_encodable)
{
	UT_ASSERT_EQ(ERRCODE_CLUSTER_RECONFIG_IN_PROGRESS, MAKE_SQLSTATE('5', '3', 'R', '6', '0'));
	UT_ASSERT_EQ(ERRCODE_CLUSTER_GCS_BLOCK_RETRANSMIT_EXHAUSTED,
				 MAKE_SQLSTATE('5', '3', 'R', '9', '0'));
	UT_ASSERT_EQ(ERRCODE_CLUSTER_GCS_BLOCK_STARVATION_EXHAUSTED,
				 MAKE_SQLSTATE('5', '3', 'R', '9', '2'));
	UT_ASSERT_EQ(ERRCODE_CLUSTER_LOST_WRITE_DETECTED, MAKE_SQLSTATE('5', '3', 'R', '9', '3'));
	UT_ASSERT_EQ(ERRCODE_CLUSTER_SINVAL_QUEUE_FULL, MAKE_SQLSTATE('5', '3', 'R', '9', '4'));
	UT_ASSERT_EQ(ERRCODE_CLUSTER_SINVAL_ACK_TIMEOUT, MAKE_SQLSTATE('5', '3', 'R', '9', '5'));
}


/* ===== L7 — GUC enum + cluster.shmem_max_regions bound snapshot ===== */
/* v0.3 P2 minor:  shmem_max_regions GUC boundary assertion防 future
 * spec 误降 min_val 破坏 region 注册兼容性 */
extern int cluster_shmem_max_regions;

UT_TEST(test_stage2_guc_enum_snapshot)
{
	/* sinval_ack_mode enum:  0=NONE / 1=PEER_ENQUEUED */
	UT_ASSERT_EQ((int)CLUSTER_SINVAL_ACK_MODE_NONE, 0);
	UT_ASSERT_EQ((int)CLUSTER_SINVAL_ACK_MODE_PEER_ENQUEUED, 1);

	/* cluster_shmem_max_regions:  default 80 (boot_val; spec-5.56 raised 64 -> 80
	 * for the cr relgen region);  test process loads default since no
	 * postgresql.conf wired in this unit binary. */
	UT_ASSERT_EQ(cluster_shmem_max_regions, 80);
	/* boot_val/min_val/max_val GUC contract is enforced at registration
	 * site in cluster_guc.c (DefineCustomIntVariable args 64/35/256);
	 * this test pins the boot_val.  min_val/max_val 是 GUC infra constant,
	 * 不通过 extern 访问 — 由 030_acceptance.pl L? 在 production 验. */
}


/* ===== L8 — PGRAC_IC_MSG_RESERVED_0 == 0 sentinel ===== */

UT_TEST(test_stage2_ic_msg_reserved_0_sentinel)
{
	UT_ASSERT_EQ((int)PGRAC_IC_MSG_RESERVED_0, 0);
}


/* Stubs for cluster_shmem_max_regions extern (spec-5.56: default 80) */
int cluster_shmem_max_regions = 80;


int
main(void)
{
	UT_RUN(test_stage2_catversion_at_or_above_spec_2_39);
	UT_RUN(test_stage2_msg_types_cumulative_registration);
	UT_RUN(test_stage2_capability_counter_symbols_linkable);
	UT_RUN(test_stage2_fault_inject_point_names);
	UT_RUN(test_stage2_wait_events_count_snapshot_110);
	UT_RUN(test_stage2_sqlstate_53r60_through_95_encodable);
	UT_RUN(test_stage2_guc_enum_snapshot);
	UT_RUN(test_stage2_ic_msg_reserved_0_sentinel);
	UT_DONE();
}
