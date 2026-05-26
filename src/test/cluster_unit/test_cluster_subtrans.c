/*-------------------------------------------------------------------------
 *
 * test_cluster_subtrans.c
 *	  pgrac spec-3.5 D12 — cluster_unit pure ABI / symbol-link tests for
 *	  SUBTRANS cross-node visibility module.
 *
 *	  12 tests covering:
 *	    T1   CLUSTER_TT_STATUS_SUBCOMMITTED = 5 (preserves 0-4)
 *	    T2   ClusterTTStatusResult struct has has_parent_key + parent_key
 *	    T3   ClusterTTStatusHintMsgV3 sizeof = 64
 *	    T4   ClusterTTStatusHintMsgV3 offsetof msg_version = 0
 *	    T5   ClusterTTStatusHintMsgV3 offsetof key = 8 (HC184)
 *	    T6   ClusterTTStatusHintMsgV3 offsetof commit_scn = 32 (D8 lock)
 *	    T7   ClusterTTStatusHintMsgV3 offsetof parent_key = 40 (L203)
 *	    T8   CLUSTER_TT_STATUS_HINT_V3 = 3 enum value
 *	    T9   cluster_subtrans_* API symbols link (4 funcs)
 *	    T10  cluster_tt_status_install_subcommitted symbol links
 *	    T11  cluster_tt_status_hint_emit_subcommitted symbol links
 *	    T12  ERRCODE_PREPARE_TRANSACTION_WITH_CLUSTER_SUBTRANS_STATE
 *	         encodes as 53R9B
 *
 *	  Behavioural / lifecycle coverage of subxact emit / lazy follow /
 *	  PREPARE guard lives in cluster_tap t/210 + t/211 (D13/D14).
 *
 * Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * Spec: spec-3.5-subtrans-cross-node-visibility.md (v0.3 FROZEN 2026-05-26)
 *
 *-------------------------------------------------------------------------
 */
#define USE_PGRAC_CLUSTER 1

#include "postgres.h"

#include <stddef.h>
#include <string.h>

#include "cluster/cluster_subtrans.h"
#include "cluster/cluster_tt_status.h"
#include "cluster/cluster_tt_status_hint.h"
#include "utils/errcodes.h"

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


/* ===== Local stubs — pure-ABI binary does not link real cluster.o ===== */

bool
cluster_subtrans_emit_subcommit(TransactionId child_xid pg_attribute_unused(),
								TransactionId parent_xid pg_attribute_unused())
{
	return false;
}

bool
cluster_subtrans_emit_subabort(TransactionId child_xid pg_attribute_unused())
{
	return false;
}

ClusterTTStatusResult
cluster_subtrans_lookup_parent(const ClusterTTStatusResult *child_result,
							   int						   depth_remaining pg_attribute_unused())
{
	ClusterTTStatusResult r;

	memset(&r, 0, sizeof(r));
	if (child_result != NULL)
		r = *child_result;
	return r;
}

bool
cluster_subtrans_xact_has_state(TransactionId top_xid pg_attribute_unused())
{
	return false;
}

bool
cluster_subtrans_ensure_parent_binding(TransactionId		parent_xid pg_attribute_unused(),
									   ClusterTTStatusKey *parent_key_out)
{
	if (parent_key_out != NULL)
		memset(parent_key_out, 0, sizeof(*parent_key_out));
	return false;
}

uint64
cluster_subtrans_get_chain_depth_exceeded_count(void)
{
	return 0;
}

uint64
cluster_subtrans_get_xact_has_state_check_count(void)
{
	return 0;
}

bool
cluster_tt_status_install_subcommitted(const ClusterTTStatusKey *child_key pg_attribute_unused(),
									   const ClusterTTStatusKey *parent_key pg_attribute_unused())
{
	return false;
}

void
cluster_tt_status_hint_emit_subcommitted(const ClusterTTStatusKey *child_key pg_attribute_unused(),
										 const ClusterTTStatusKey *parent_key pg_attribute_unused())
{}

uint64
cluster_tt_status_get_subcommitted_install_count(void)
{
	return 0;
}
uint64
cluster_tt_status_get_subcommitted_lookup_hit_count(void)
{
	return 0;
}
uint64
cluster_tt_status_get_parent_chain_follow_count(void)
{
	return 0;
}
uint64
cluster_tt_status_hint_get_v3_downgrade_count(void)
{
	return 0;
}


/* ===== T1: SUBCOMMITTED enum value = 5 ===== */

UT_TEST(t1_subcommitted_enum_is_5)
{
	UT_ASSERT_EQ((int) CLUSTER_TT_STATUS_UNKNOWN, 0);
	UT_ASSERT_EQ((int) CLUSTER_TT_STATUS_IN_PROGRESS, 1);
	UT_ASSERT_EQ((int) CLUSTER_TT_STATUS_COMMITTED, 2);
	UT_ASSERT_EQ((int) CLUSTER_TT_STATUS_ABORTED, 3);
	UT_ASSERT_EQ((int) CLUSTER_TT_STATUS_CLEANED_OUT, 4);
	UT_ASSERT_EQ((int) CLUSTER_TT_STATUS_SUBCOMMITTED, 5);
}


/* ===== T2: ClusterTTStatusResult has has_parent_key + parent_key ===== */

UT_TEST(t2_result_struct_has_parent_metadata)
{
	ClusterTTStatusResult r;

	memset(&r, 0, sizeof(r));
	r.status = CLUSTER_TT_STATUS_SUBCOMMITTED;
	r.has_parent_key = true;
	r.parent_key.origin_node_id = 7;

	UT_ASSERT_EQ((int) r.status, 5);
	UT_ASSERT(r.has_parent_key);
	UT_ASSERT_EQ((int) r.parent_key.origin_node_id, 7);
}


/* ===== T3: V3 wire sizeof = 64 ===== */

UT_TEST(t3_v3_sizeof_is_64)
{
	UT_ASSERT_EQ((int) sizeof(ClusterTTStatusHintMsgV3), 64);
}


/* ===== T4-T7: V3 offsetof locks (progressive extend convention) ===== */

UT_TEST(t4_v3_offsetof_msg_version_is_0)
{
	UT_ASSERT_EQ((int) offsetof(ClusterTTStatusHintMsgV3, msg_version), 0);
}

UT_TEST(t5_v3_offsetof_key_is_8)
{
	/* HC184 wire ABI lock — V1/V2/V3 same key offset. */
	UT_ASSERT_EQ((int) offsetof(ClusterTTStatusHintMsgV3, key), 8);
}

UT_TEST(t6_v3_offsetof_commit_scn_is_32)
{
	/* spec-3.3 D8 wire ABI lock — V2/V3 same commit_scn offset. */
	UT_ASSERT_EQ((int) offsetof(ClusterTTStatusHintMsgV3, commit_scn), 32);
}

UT_TEST(t7_v3_offsetof_parent_key_is_40)
{
	/* L203 progressive extend convention — V3 appended @ offset 40. */
	UT_ASSERT_EQ((int) offsetof(ClusterTTStatusHintMsgV3, parent_key), 40);
}


/* ===== T8: CLUSTER_TT_STATUS_HINT_V3 enum value = 3 ===== */

UT_TEST(t8_hint_v3_enum_is_3)
{
	UT_ASSERT_EQ((int) CLUSTER_TT_STATUS_HINT_V1, 1);
	UT_ASSERT_EQ((int) CLUSTER_TT_STATUS_HINT_V2, 2);
	UT_ASSERT_EQ((int) CLUSTER_TT_STATUS_HINT_V3, 3);
}


/* ===== T9: cluster_subtrans_* API symbols link ===== */

UT_TEST(t9_cluster_subtrans_api_symbols_link)
{
	ClusterTTStatusResult r;
	ClusterTTStatusKey k;
	bool ok;

	memset(&r, 0, sizeof(r));
	memset(&k, 0, sizeof(k));

	/* All four public API are reachable (linker test). */
	ok = cluster_subtrans_emit_subcommit(InvalidTransactionId, InvalidTransactionId);
	UT_ASSERT(!ok); /* stub returns false */

	ok = cluster_subtrans_emit_subabort(InvalidTransactionId);
	UT_ASSERT(!ok);

	(void) cluster_subtrans_lookup_parent(&r, 32);

	ok = cluster_subtrans_xact_has_state(InvalidTransactionId);
	UT_ASSERT(!ok);

	ok = cluster_subtrans_ensure_parent_binding(InvalidTransactionId, &k);
	UT_ASSERT(!ok);
}


/* ===== T10: install_subcommitted symbol link ===== */

UT_TEST(t10_install_subcommitted_links)
{
	ClusterTTStatusKey child;
	ClusterTTStatusKey parent;
	bool ok;

	memset(&child, 0, sizeof(child));
	memset(&parent, 0, sizeof(parent));

	ok = cluster_tt_status_install_subcommitted(&child, &parent);
	UT_ASSERT(!ok); /* stub returns false */
}


/* ===== T11: hint_emit_subcommitted symbol link ===== */

UT_TEST(t11_hint_emit_subcommitted_links)
{
	ClusterTTStatusKey child;
	ClusterTTStatusKey parent;

	memset(&child, 0, sizeof(child));
	memset(&parent, 0, sizeof(parent));

	/* Stub no-op;  test passes if call links. */
	cluster_tt_status_hint_emit_subcommitted(&child, &parent);

	/* Counter getter also links. */
	UT_ASSERT_EQ((uint64) cluster_tt_status_hint_get_v3_downgrade_count(), (uint64) 0);
}


/* ===== T12: ERRCODE 53R9B encodes correctly ===== */

UT_TEST(t12_errcode_53r9b)
{
	/*
	 * MAKE_SQLSTATE("53R9B") is the canonical PG packed encoding
	 * (PG_DIAG_SQLSTATE 5-char SQLSTATE).  Validate that the
	 * generated macro is non-zero and equal to MAKE_SQLSTATE("53R9B").
	 */
	int code = ERRCODE_PREPARE_TRANSACTION_WITH_CLUSTER_SUBTRANS_STATE;

	UT_ASSERT_NE(code, 0);
	UT_ASSERT_EQ(code, (int) MAKE_SQLSTATE('5', '3', 'R', '9', 'B'));
}


int
main(void)
{
	UT_PLAN(12);
	UT_RUN(t1_subcommitted_enum_is_5);
	UT_RUN(t2_result_struct_has_parent_metadata);
	UT_RUN(t3_v3_sizeof_is_64);
	UT_RUN(t4_v3_offsetof_msg_version_is_0);
	UT_RUN(t5_v3_offsetof_key_is_8);
	UT_RUN(t6_v3_offsetof_commit_scn_is_32);
	UT_RUN(t7_v3_offsetof_parent_key_is_40);
	UT_RUN(t8_hint_v3_enum_is_3);
	UT_RUN(t9_cluster_subtrans_api_symbols_link);
	UT_RUN(t10_install_subcommitted_links);
	UT_RUN(t11_hint_emit_subcommitted_links);
	UT_RUN(t12_errcode_53r9b);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
