/* Author: SqlRush <sqlrush@gmail.com>
 * Copyright (c) 2026, pgrac contributors.
 * Native ANALYZE prefix + real allocator/local/durable BIND. The first target
 * lock is a negative-only lookup boundary: no sampling or statistics success.
 * Admission, physical IO/WAL and exact-owner scheduling use the parent seams.
 */
#define TT_ACTIVE_OWNER_FIXTURE_MAIN analyze_active_owner_fixture_main
#include "test_cluster_tt_active_owner.c"
#undef main
#include "access/xact.h"
#include "commands/vacuum.h"
#include "foreign/fdwapi.h"
#include "tcop/utility.h"

bool XactReadOnly;
static bool analyze_parallel, analyze_recovery, analyze_child;
static TransactionId analyze_top, analyze_current;
static unsigned analyze_xids, analyze_lookups, analyze_unbound;
static bool analyze_expect_binding;
static VacuumParams analyze_params;
static RangeVar analyze_target;
static ClusterConf analyze_topology = {.node_count = 4};
static BufferAccessStrategy vac_strategy;

bool
IsInParallelMode(void)
{
	return analyze_parallel;
}

bool
RecoveryInProgress(void)
{
	return analyze_recovery;
}

bool
IsSubTransaction(void)
{
	return analyze_child;
}

TransactionId
GetCurrentTransactionIdIfAny(void)
{
	return analyze_current;
}

TransactionId
GetTopTransactionIdIfAny(void)
{
	return analyze_top;
}

TransactionId
GetTopTransactionId(void)
{
	if (!TransactionIdIsValid(analyze_top)) {
		analyze_xids++;
		analyze_top = analyze_current = 100;
	}
	return analyze_top;
}

Relation
vacuum_open_relation(Oid relid, RangeVar *relation, bits32 options, bool verbose,
					 LOCKMODE lmode)
{
	ClusterCanonicalTxnBinding binding;

	analyze_lookups++;
	UT_ASSERT_EQ(relid, 701);
	UT_ASSERT(relation == &analyze_target);
	UT_ASSERT_EQ(options, analyze_params.options & ~VACOPT_VACUUM);
	UT_ASSERT_EQ(verbose, analyze_params.log_min_duration >= 0);
	UT_ASSERT_EQ(lmode, ShareUpdateExclusiveLock);
	UT_ASSERT_EQ(lock_depth, 0);
	UT_ASSERT_EQ(modifier_enters, modifier_leaves);
	if (analyze_expect_binding
		&& !cluster_tt_local_get_published_binding(analyze_top, &binding))
		analyze_unbound++;
	/* Missing/skip lookup only; this cannot stand in for a completed ANALYZE. */
	return NULL;
}

/* The extracted prefix stops at the real first target-lock call. Its later
 * sampling locals are intentionally unused in this negative-only boundary. */
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-variable"
#pragma GCC diagnostic ignored "-Wunused-but-set-variable"
#include "test_cluster_analyze_preflight.inc"
#pragma GCC diagnostic pop

static void
reset_analyze(bool full)
{
	if (full)
		start_full_active_test();
	else
		reset_active_owner();
	ClusterConfShmem = &analyze_topology;
	cluster_shared_catalog = true;
	slot_wait_terminal_capacity = full;
	/* Retain the same physical image across native wait, BIND and lookups. */
	g_modifier_recheck_hook = NULL;
	enable_sealed_header_io();
	XactReadOnly = analyze_parallel = analyze_recovery = analyze_child = false;
	analyze_top = analyze_current = InvalidTransactionId;
	analyze_xids = analyze_lookups = analyze_unbound = 0;
	analyze_expect_binding = true;
	analyze_params = (VacuumParams){.options = VACOPT_ANALYZE, .log_min_duration = -1};
	analyze_target = (RangeVar){.type = T_RangeVar, .relname = "analyze_preflight"};
	last_ereport_errcode = 0;
}

static bool
call_analyze(bool in_outer)
{
	volatile bool caught = false;

	PG_TRY();
	{
		analyze_rel(701, &analyze_target, &analyze_params, NIL, in_outer, NULL);
	}
	PG_CATCH();
	{
		caught = true;
	}
	PG_END_TRY();
	return caught;
}

UT_TEST(test_analyze_full_selected_segment_binds_after_original_wait_before_target_lock)
{
	for (unsigned outer = 0; outer < 2; outer++) {
		reset_analyze(true);
		UT_ASSERT(!call_analyze(outer != 0));
		UT_ASSERT_EQ(analyze_lookups, 1);
		UT_ASSERT_EQ(analyze_unbound, 0);
		UT_ASSERT_EQ(analyze_xids, 1);
		UT_ASSERT_EQ(g_bind_emit_calls, 1);
		UT_ASSERT_EQ(slot_wait_calls, 1);
		UT_ASSERT_EQ(capacity_waits, 1);
		UT_ASSERT_EQ(cold_selection_calls, 0);
		UT_ASSERT_EQ(cluster_tt_slot_current_segment(0), 1);
		UT_ASSERT_EQ(cluster_tt_local_binding_count, 1);
	}
}

UT_TEST(test_analyze_catalog_route_and_same_top_reuse_precede_each_original_lookup)
{
	for (unsigned mode = 0; mode < 8; mode++) {
		bool bound = (mode & 3) == 3;

		reset_analyze(false);
		cluster_enabled = (mode & 1) != 0;
		cluster_shared_catalog = (mode & 2) != 0;
		cluster_shared_config = (mode & 4) != 0;
		analyze_expect_binding = bound;
		for (unsigned i = 0; i < 2; i++) {
			analyze_params.options = VACOPT_ANALYZE | (i ? VACOPT_VACUUM | VACOPT_VERBOSE : 0);
			analyze_params.log_min_duration = i ? 0 : -1;
			UT_ASSERT(!call_analyze(i != 0));
			UT_ASSERT_EQ(analyze_lookups, i + 1);
			UT_ASSERT_EQ(analyze_unbound, 0);
			UT_ASSERT_EQ(analyze_xids, bound);
			UT_ASSERT_EQ(g_bind_emit_calls, bound);
			UT_ASSERT_EQ(cluster_tt_local_binding_count, bound);
		}
	}
}

UT_TEST(test_analyze_context_child_and_top_rejections_precede_xid_bind_and_target_lock)
{
	static const int errors[] = {ERRCODE_READ_ONLY_SQL_TRANSACTION,
		ERRCODE_INVALID_TRANSACTION_STATE, ERRCODE_READ_ONLY_SQL_TRANSACTION,
		ERRCODE_FEATURE_NOT_SUPPORTED, ERRCODE_INTERNAL_ERROR};

	for (unsigned guard = 0; guard < lengthof(errors); guard++) {
		reset_analyze(false);
		XactReadOnly = guard == 0;
		analyze_parallel = guard == 1;
		analyze_recovery = guard == 2;
		analyze_child = guard == 3;
		if (guard == 4) {
			analyze_current = 101;
			analyze_top = 100;
		}
		UT_ASSERT(call_analyze(false));
		UT_ASSERT_EQ(last_ereport_errcode, errors[guard]);
		UT_ASSERT_EQ(analyze_xids + analyze_lookups + g_bind_emit_calls, 0);
		UT_ASSERT_EQ(cluster_tt_local_binding_count, 0);
	}
}

UT_TEST(test_analyze_failed_canonical_publication_stops_before_target_lock)
{
	reset_analyze(false);
	cluster_shared_config = true;
	active_owner_start_state = CLUSTER_NORMAL_START_EXISTING_OTHER;
	active_owner_recovered_open = false;
	UT_ASSERT(call_analyze(false));
	UT_ASSERT_EQ(last_ereport_errcode, ERRCODE_DATA_CORRUPTED);
	UT_ASSERT_EQ(analyze_lookups + g_bind_emit_calls, 0);
	UT_ASSERT_EQ(cluster_tt_local_binding_count, 0);
}

UT_TEST(test_analyze_original_native_wait_errors_propagate_without_target_lock_or_bind)
{
	static const int errors[] = {ERRCODE_QUERY_CANCELED, ERRCODE_LOCK_NOT_AVAILABLE,
		ERRCODE_T_R_DEADLOCK_DETECTED, ERRCODE_CLUSTER_RECONFIG_IN_PROGRESS};

	for (unsigned i = 0; i < lengthof(errors); i++) {
		reset_analyze(true);
		slot_wait_error = errors[i];
		UT_ASSERT(call_analyze(false));
		UT_ASSERT_EQ(last_ereport_errcode, errors[i]);
		UT_ASSERT_EQ(analyze_lookups + g_bind_emit_calls, 0);
		UT_ASSERT_EQ(cluster_tt_local_binding_count, 0);
		UT_ASSERT_EQ(slot_wait_begins, 1);
		UT_ASSERT_EQ(slot_wait_ends, 1);
		UT_ASSERT_EQ(slot_wait_cancels, errors[i] == ERRCODE_QUERY_CANCELED);
		UT_ASSERT_EQ(modifier_enters, modifier_leaves);
	}
}

int
main(void)
{
	UT_PLAN(5);
	UT_RUN(test_analyze_full_selected_segment_binds_after_original_wait_before_target_lock);
	UT_RUN(test_analyze_catalog_route_and_same_top_reuse_precede_each_original_lookup);
	UT_RUN(test_analyze_context_child_and_top_rejections_precede_xid_bind_and_target_lock);
	UT_RUN(test_analyze_failed_canonical_publication_stops_before_target_lock);
	UT_RUN(test_analyze_original_native_wait_errors_propagate_without_target_lock_or_bind);
	UT_DONE();
	return ut_failed_count ? 1 : 0;
}
