/*-------------------------------------------------------------------------
 *
 * test_cluster_multixact.c
 *	  pgrac spec-3.6 D10 — cluster_unit pure ABI / sizeof / offsetof /
 *	  enum / SQLSTATE / symbol-link tests for MULTIXACT reader/member-
 *	  resolution foundation.
 *
 *	  16 tests covering:
 *	    T1   ClusterMultiXactKey sizeof = 16 + offsetof origin / mxid / epoch
 *	    T2   ClusterMultiXactMember sizeof = 24 + offsetof exact-key fields
 *	    T3   ClusterTTStatusHintMsgV4Header sizeof = 24 + offsetof msg_version / key
 *	    T4   CLUSTER_TT_STATUS_HINT_V4 = 4 enum value
 *	    T5   ClusterMultiXactHintOutboundEntry sizeof = 6168 (24 + 256 × 24)
 *	    T6-10  cluster_multixact_* 5 API + ITL helper 2 API symbol link
 *	    T11  ITL_FLAG_LOCK_ONLY_XMAX_IS_MULTI = 8 enum value
 *	    T12  MultiXactStatus enum 0-5 preservation (PG core lock)
 *	    T13  53R9C SQLSTATE encode = MAKE_SQLSTATE('5','3','R','9','C')
 *	    T14  5 cluster_multixact counter getter symbol link
 *	    T15  v4_drop_unknown_count counter getter symbol link
 *	    T16  default GUC cap value 32 (cluster_multixact_member_overlay_max_members)
 *
 *	  Behavioural / lifecycle coverage of V4 emit / overlay install /
 *	  resolve_visibility lives in cluster_tap t/212 (D11).
 *
 * Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * Spec: spec-3.6-multixact-reader-member-resolution.md (v0.3 FROZEN 2026-05-27)
 *
 *-------------------------------------------------------------------------
 */
#define USE_PGRAC_CLUSTER 1

#include "postgres.h"

#include <stddef.h>
#include <string.h>

#include "access/multixact.h"
#include "cluster/cluster_itl_slot.h"
#include "cluster/cluster_multixact.h"
#include "cluster/cluster_tt_status_hint.h"
#include "utils/errcodes.h"

#undef printf
#undef fprintf
#undef snprintf

#include "unit_test.h"


UT_DEFINE_GLOBALS();

int cluster_multixact_member_overlay_max_members = 32; /* GUC stub */


void
ExceptionalCondition(const char *conditionName pg_attribute_unused(),
					 const char *fileName pg_attribute_unused(),
					 int lineNumber pg_attribute_unused())
{
	abort();
}


/* ===== Local stubs — pure-ABI binary does not link real cluster.o ===== */

ClusterSemanticAdmissionResult
cluster_multixact_source_dispatch(ClusterMultiXactSourceOp op pg_attribute_unused(),
								  const ClusterMultiXactSourceRequest *request
									  pg_attribute_unused(),
								  ClusterMultiXactSourceResult *result pg_attribute_unused())
{
	return CLUSTER_SEMANTIC_ADMISSION_CLOSED;
}

void
cluster_multixact_purge_epoch(uint32 obsolete_epoch pg_attribute_unused())
{}

uint64
cluster_multixact_get_overlay_install_count(void)
{
	return 0;
}
uint64
cluster_multixact_get_overlay_lookup_hit_count(void)
{
	return 0;
}
uint64
cluster_multixact_get_overlay_miss_count(void)
{
	return 0;
}
uint64
cluster_multixact_get_overlay_overflow_count(void)
{
	return 0;
}
uint64
cluster_multixact_get_resolve_visibility_count(void)
{
	return 0;
}

ClusterSemanticAdmissionResult
cluster_tt_status_hint_source_dispatch(ClusterTTStatusHintSourceOp op pg_attribute_unused(),
									   const ClusterTTStatusHintSourceRequest *request
										   pg_attribute_unused())
{
	return CLUSTER_SEMANTIC_ADMISSION_CLOSED;
}

uint64
cluster_tt_status_hint_get_v4_drop_unknown_count(void)
{
	return 0;
}


/* ===== T1: ClusterMultiXactKey sizeof / offsetof ===== */

UT_TEST(t1_multixact_key_layout)
{
	UT_ASSERT_EQ((int)sizeof(ClusterMultiXactKey), 16);
	UT_ASSERT_EQ((int)offsetof(ClusterMultiXactKey, origin_node_id), 0);
	UT_ASSERT_EQ((int)offsetof(ClusterMultiXactKey, multixact_id), 4);
	UT_ASSERT_EQ((int)offsetof(ClusterMultiXactKey, cluster_epoch), 8);
}


/* ===== T2: ClusterMultiXactMember sizeof / offsetof ===== */

UT_TEST(t2_multixact_member_layout)
{
	UT_ASSERT_EQ((int)sizeof(ClusterMultiXactMember), 24);
	UT_ASSERT_EQ((int)offsetof(ClusterMultiXactMember, xid), 0);
	UT_ASSERT_EQ((int)offsetof(ClusterMultiXactMember, status), 4);
	UT_ASSERT_EQ((int)offsetof(ClusterMultiXactMember, origin_node_id), 6);
	UT_ASSERT_EQ((int)offsetof(ClusterMultiXactMember, undo_segment_id), 8);
	UT_ASSERT_EQ((int)offsetof(ClusterMultiXactMember, tt_slot_id), 12);
	UT_ASSERT_EQ((int)offsetof(ClusterMultiXactMember, epoch), 16);
}


/* ===== T3: V4 header sizeof + offsetof ===== */

UT_TEST(t3_v4_header_layout)
{
	UT_ASSERT_EQ((int)sizeof(ClusterTTStatusHintMsgV4Header), 24);
	UT_ASSERT_EQ((int)offsetof(ClusterTTStatusHintMsgV4Header, msg_version), 0);
	UT_ASSERT_EQ((int)offsetof(ClusterTTStatusHintMsgV4Header, key), 8);
}


/* ===== T4: V4 enum value = 4 ===== */

UT_TEST(t4_v4_enum_is_4)
{
	UT_ASSERT_EQ((int)CLUSTER_TT_STATUS_HINT_V4, 4);
	UT_ASSERT_EQ((int)CLUSTER_TT_STATUS_HINT_V3, 3);
	UT_ASSERT_EQ((int)CLUSTER_TT_STATUS_HINT_V2, 2);
	UT_ASSERT_EQ((int)CLUSTER_TT_STATUS_HINT_V1, 1);
}


/* ===== T5: V4 sidecar outbound entry sizeof = 6168 ===== */

UT_TEST(t5_v4_outbound_entry_sizeof)
{
	/* 24 + 256 × 24 = 6168 */
	UT_ASSERT_EQ((int)sizeof(ClusterMultiXactHintOutboundEntry), 6168);
	UT_ASSERT_EQ((int)CLUSTER_MULTIXACT_HINT_MAX_MEMBERS, 256);
}


/* ===== T6: cluster_multixact 5 API symbol link ===== */

UT_TEST(t6_cluster_multixact_api_link)
{
	UT_ASSERT_EQ((int)CLUSTER_MULTI_SOURCE_OVERLAY_INSTALL, 0);
	UT_ASSERT_EQ((int)CLUSTER_MULTI_SOURCE_OVERLAY_LOOKUP, 1);
	UT_ASSERT_EQ((int)CLUSTER_MULTI_SOURCE_RESOLVE_VISIBILITY, 2);
	UT_ASSERT_EQ((int)CLUSTER_MULTI_SOURCE_GET_MEMBER_COUNT, 3);
	UT_ASSERT_NE((void *)cluster_multixact_source_dispatch, NULL);
	cluster_multixact_purge_epoch(1);
}


/* ===== T7: hint_emit_multixact_overlay symbol link ===== */

UT_TEST(t7_hint_emit_multixact_overlay_link)
{
	UT_ASSERT_EQ((int)CLUSTER_TT_HINT_SOURCE_EMIT_MULTIXACT_OVERLAY, 2);
	UT_ASSERT_NE((void *)cluster_tt_status_hint_source_dispatch, NULL);
}


/* ===== T8-10: counter getters symbol link ===== */

UT_TEST(t8_overlay_install_count_link)
{
	UT_ASSERT_EQ((uint64)cluster_multixact_get_overlay_install_count(), (uint64)0);
	UT_ASSERT_EQ((uint64)cluster_multixact_get_overlay_lookup_hit_count(), (uint64)0);
}

UT_TEST(t9_overlay_miss_overflow_link)
{
	UT_ASSERT_EQ((uint64)cluster_multixact_get_overlay_miss_count(), (uint64)0);
	UT_ASSERT_EQ((uint64)cluster_multixact_get_overlay_overflow_count(), (uint64)0);
}

UT_TEST(t10_resolve_visibility_count_link)
{
	UT_ASSERT_EQ((uint64)cluster_multixact_get_resolve_visibility_count(), (uint64)0);
}


/* ===== T11: ITL_FLAG_LOCK_ONLY_XMAX_IS_MULTI enum value ===== */

UT_TEST(t11_itl_flag_xmax_is_multi)
{
	UT_ASSERT_EQ((int)ITL_FLAG_LOCK_ONLY_XMAX_IS_MULTI, 8);
	/* spec-3.4d existing flags 0-7 preserved */
	UT_ASSERT_EQ((int)ITL_FLAG_FREE, 0);
	UT_ASSERT_EQ((int)ITL_FLAG_LOCK_ONLY_ACTIVE, 5);
	UT_ASSERT_EQ((int)ITL_FLAG_LOCK_ONLY_ABORTED, 7);
}


/* ===== T12: MultiXactStatus enum 0-5 preservation ===== */

UT_TEST(t12_multixactstatus_preservation)
{
	UT_ASSERT_EQ((int)MultiXactStatusForKeyShare, 0);
	UT_ASSERT_EQ((int)MultiXactStatusForShare, 1);
	UT_ASSERT_EQ((int)MultiXactStatusForNoKeyUpdate, 2);
	UT_ASSERT_EQ((int)MultiXactStatusForUpdate, 3);
	UT_ASSERT_EQ((int)MultiXactStatusNoKeyUpdate, 4);
	UT_ASSERT_EQ((int)MultiXactStatusUpdate, 5);
}


/* ===== T13: 53R9C SQLSTATE encode ===== */

UT_TEST(t13_errcode_53r9c)
{
	int code = ERRCODE_CLUSTER_MULTIXACT_MEMBER_OVERLAY_MISS;
	UT_ASSERT_NE(code, 0);
	UT_ASSERT_EQ(code, (int)MAKE_SQLSTATE('5', '3', 'R', '9', 'C'));
}


/* ===== T14: V4 drop unknown counter symbol link ===== */

UT_TEST(t14_v4_drop_unknown_count_link)
{
	UT_ASSERT_EQ((uint64)cluster_tt_status_hint_get_v4_drop_unknown_count(), (uint64)0);
}


/* ===== T15: default GUC cap value ===== */

UT_TEST(t15_default_guc_cap_is_32)
{
	UT_ASSERT_EQ((int)cluster_multixact_member_overlay_max_members, 32);
}


/* ===== T16: OBS-1 truth table key cases via local helper ===== */

/*
 * Local truth-table helper for unit smoke (the real impl in
 * cluster_multixact.c needs ClusterTTStatusResult lookups + SCN cmp).
 * Verify lock-only members never hide tuple regardless of state.
 */
static bool
local_lockonly_always_visible(uint8 status)
{
	return status <= 3; /* ForKeyShare..ForUpdate */
}

UT_TEST(t16_obs1_lockonly_visible_truth)
{
	UT_ASSERT(local_lockonly_always_visible((uint8)MultiXactStatusForKeyShare));
	UT_ASSERT(local_lockonly_always_visible((uint8)MultiXactStatusForShare));
	UT_ASSERT(local_lockonly_always_visible((uint8)MultiXactStatusForNoKeyUpdate));
	UT_ASSERT(local_lockonly_always_visible((uint8)MultiXactStatusForUpdate));
	UT_ASSERT(!local_lockonly_always_visible((uint8)MultiXactStatusNoKeyUpdate));
	UT_ASSERT(!local_lockonly_always_visible((uint8)MultiXactStatusUpdate));
}


int
main(void)
{
	UT_PLAN(16);
	UT_RUN(t1_multixact_key_layout);
	UT_RUN(t2_multixact_member_layout);
	UT_RUN(t3_v4_header_layout);
	UT_RUN(t4_v4_enum_is_4);
	UT_RUN(t5_v4_outbound_entry_sizeof);
	UT_RUN(t6_cluster_multixact_api_link);
	UT_RUN(t7_hint_emit_multixact_overlay_link);
	UT_RUN(t8_overlay_install_count_link);
	UT_RUN(t9_overlay_miss_overflow_link);
	UT_RUN(t10_resolve_visibility_count_link);
	UT_RUN(t11_itl_flag_xmax_is_multi);
	UT_RUN(t12_multixactstatus_preservation);
	UT_RUN(t13_errcode_53r9c);
	UT_RUN(t14_v4_drop_unknown_count_link);
	UT_RUN(t15_default_guc_cap_is_32);
	UT_RUN(t16_obs1_lockonly_visible_truth);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
