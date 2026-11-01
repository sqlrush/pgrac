/* Author: SqlRush <sqlrush@gmail.com>
 * Copyright (c) 2026, pgrac contributors.
 * Original subscription entries/utility BIND + real allocator/local/durable
 * producer. Option parsing and first catalog lock are negative ERROR boundaries;
 * this does not simulate successful subscription, permissions or replication.
 */
#define TT_ACTIVE_OWNER_FIXTURE_MAIN subscription_active_owner_fixture_main
#include "test_cluster_tt_active_owner.c"
#undef main
#include "access/table.h"
#include "access/xact.h"
#include "catalog/pg_subscription.h"
#include "cluster/cluster_ic.h"
#include "commands/subscriptioncmds.h"
#include "miscadmin.h"
#include "replication/walreceiver.h"
#include "tcop/utility.h"
#include "utils/acl.h"
#include "test_cluster_subscription_types.inc"

bool XactReadOnly;
int cluster_interconnect_tier = CLUSTER_IC_TIER_1;
static bool sub_parallel, sub_recovery, sub_child, sub_is_top;
static TransactionId sub_top, sub_current;
static unsigned sub_xids, sub_lookups, sub_unbound, sub_parses;
static bool sub_expect_binding;
static ClusterConf sub_topology = {.node_count = 4};
static AlterSubscriptionStmt sub_alter;
static DropSubscriptionStmt sub_drop;
static CreateSubscriptionStmt sub_create;

bool
IsInParallelMode(void)
{
	return sub_parallel;
}

bool
RecoveryInProgress(void)
{
	return sub_recovery;
}

bool
IsSubTransaction(void)
{
	return sub_child;
}

bool
IsInTransactionBlock(bool isTopLevel)
{
	return false;
}

Oid
GetUserId(void)
{
	return 42;
}

TransactionId
GetCurrentTransactionIdIfAny(void)
{
	return sub_current;
}

TransactionId
GetTopTransactionIdIfAny(void)
{
	return sub_top;
}

TransactionId
GetTopTransactionId(void)
{
	if (!TransactionIdIsValid(sub_top)) {
		sub_xids++;
		sub_top = sub_current = 100;
	}
	return sub_top;
}

Relation
table_open(Oid relationId, LOCKMODE lockmode)
{
	ClusterCanonicalTxnBinding binding;

	sub_lookups++;
	UT_ASSERT_EQ(relationId, SubscriptionRelationId);
	UT_ASSERT_EQ(lockmode, RowExclusiveLock);
	UT_ASSERT_EQ(lock_depth, 0);
	UT_ASSERT_EQ(modifier_enters, modifier_leaves);
	if (sub_expect_binding && !cluster_tt_local_get_published_binding(sub_top, &binding))
		sub_unbound++;
	ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT), errmsg("negative-only subscription lookup")));
	return NULL;
}

static void
parse_subscription_options(ParseState *pstate, List *options, bits32 supported_opts, SubOpts *opts)
{
	sub_parses++;
	UT_ASSERT(pstate == NULL);
	UT_ASSERT(options == NIL);
	UT_ASSERT(supported_opts != 0);
	UT_ASSERT(opts != NULL);
	ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT), errmsg("negative-only subscription options")));
}

/* Keep the original declarations while stopping at each first external call. */
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-variable"
#pragma GCC diagnostic ignored "-Wunused-but-set-variable"
#include "test_cluster_subscription_preflight.inc"
#pragma GCC diagnostic pop

static void
reset_subscription(bool full)
{
	if (full)
		start_full_active_test();
	else
		reset_active_owner();
	ClusterConfShmem = &sub_topology;
	cluster_shared_catalog = true;
	slot_wait_terminal_capacity = full;
	g_modifier_recheck_hook = NULL;
	enable_sealed_header_io();
	XactReadOnly = sub_parallel = sub_recovery = sub_child = false;
	sub_is_top = true;
	sub_top = sub_current = InvalidTransactionId;
	sub_xids = sub_lookups = sub_unbound = sub_parses = 0;
	sub_expect_binding = true;
	sub_alter = (AlterSubscriptionStmt){.type = T_AlterSubscriptionStmt,
		.subname = "preflight_subscription", .kind = ALTER_SUBSCRIPTION_OPTIONS};
	sub_drop = (DropSubscriptionStmt){.type = T_DropSubscriptionStmt,
		.subname = "preflight_subscription", .behavior = DROP_RESTRICT};
	sub_create = (CreateSubscriptionStmt){.type = T_CreateSubscriptionStmt,
		.subname = "preflight_subscription", .conninfo = "not-connected"};
	last_ereport_errcode = 0;
}

/* Utility is tested only with unrestricted outer context, or child/top errors.
 * The original standard_ProcessUtility context classifier is not simulated. */
static bool
call_subscription(unsigned command, bool utility)
{
	volatile bool caught = false;
	Node *statement = command == 0 ? (Node *)&sub_alter
		: command == 1 ? (Node *)&sub_drop : (Node *)&sub_create;

	PG_TRY();
	{
		if (utility)
			PreflightClusterSharedCatalogWrite(statement, sub_is_top);
		if (command == 0)
			(void)AlterSubscription(NULL, &sub_alter, sub_is_top);
		else if (command == 1)
			DropSubscription(&sub_drop, sub_is_top);
		else
			(void)CreateSubscription(NULL, &sub_create, sub_is_top);
	}
	PG_CATCH();
	{
		caught = true;
	}
	PG_END_TRY();
	return caught;
}

UT_TEST(test_subscription_native_full_segment_waits_and_binds_before_first_catalog_lock)
{
	for (unsigned shape = 0; shape < 10; shape++) {
		reset_subscription(true);
		if (shape < 8)
			sub_alter.kind = (AlterSubscriptionType)shape;
		else
			sub_drop.missing_ok = shape == 9;
		sub_is_top = (shape & 1) != 0;
		UT_ASSERT(call_subscription(shape < 8 ? 0 : 1, false));
		UT_ASSERT_EQ(last_ereport_errcode, ERRCODE_UNDEFINED_OBJECT);
		UT_ASSERT_EQ(sub_lookups, 1);
		UT_ASSERT_EQ(sub_unbound + sub_parses, 0);
		UT_ASSERT_EQ(sub_xids, 1);
		UT_ASSERT_EQ(g_bind_emit_calls, 1);
		UT_ASSERT_EQ(slot_wait_calls, 1);
		UT_ASSERT_EQ(capacity_waits, 1);
		UT_ASSERT_EQ(cold_selection_calls, 0);
		UT_ASSERT_EQ(cluster_tt_slot_current_segment(0), 1);
		UT_ASSERT_EQ(cluster_tt_local_binding_count, 1);
	}
}

UT_TEST(test_subscription_native_route_and_utility_warm_binding_use_original_top_once)
{
	for (unsigned command = 0; command < 2; command++) {
		for (unsigned mode = 0; mode < 8; mode++) {
			for (unsigned utility = 0; utility < 2; utility++) {
				bool bound = (mode & 3) == 3;

				reset_subscription(false);
				cluster_enabled = (mode & 1) != 0;
				cluster_shared_catalog = (mode & 2) != 0;
				cluster_shared_config = (mode & 4) != 0;
				sub_expect_binding = bound;
				if (utility && bound) {
					Node *stmt = command == 0 ? (Node *)&sub_alter : (Node *)&sub_drop;
					ClusterCanonicalTxnBinding binding;

					PreflightClusterSharedCatalogWrite(stmt, true);
					UT_ASSERT(cluster_tt_local_get_published_binding(sub_top, &binding));
					UT_ASSERT_EQ(sub_lookups, 0);
				}
				UT_ASSERT(call_subscription(command, utility != 0));
				UT_ASSERT_EQ(last_ereport_errcode, ERRCODE_UNDEFINED_OBJECT);
				UT_ASSERT_EQ(sub_lookups, 1);
				UT_ASSERT_EQ(sub_unbound + sub_parses, 0);
				UT_ASSERT_EQ(sub_xids, bound);
				UT_ASSERT_EQ(g_bind_emit_calls, bound);
				UT_ASSERT_EQ(cluster_tt_local_binding_count, bound);
			}
		}
	}
}

UT_TEST(test_subscription_native_context_and_both_entry_child_top_guards_precede_bind)
{
	static const int errors[] = {ERRCODE_READ_ONLY_SQL_TRANSACTION,
		ERRCODE_INVALID_TRANSACTION_STATE, ERRCODE_READ_ONLY_SQL_TRANSACTION,
		ERRCODE_FEATURE_NOT_SUPPORTED, ERRCODE_INTERNAL_ERROR};

	for (unsigned command = 0; command < 2; command++) {
		for (unsigned guard = 0; guard < lengthof(errors); guard++) {
			for (unsigned utility = 0; utility < (guard < 3 ? 1 : 2); utility++) {
				reset_subscription(false);
				XactReadOnly = guard == 0;
				sub_parallel = guard == 1;
				sub_recovery = guard == 2;
				sub_child = guard == 3;
				if (guard == 4) {
					sub_top = 100;
					sub_current = 101;
				}
				UT_ASSERT(call_subscription(command, utility != 0));
				UT_ASSERT_EQ(last_ereport_errcode, errors[guard]);
				UT_ASSERT_EQ(sub_xids + sub_lookups + sub_parses + g_bind_emit_calls, 0);
				UT_ASSERT_EQ(cluster_tt_local_binding_count, 0);
			}
		}
	}
}

UT_TEST(test_subscription_failed_canonical_publication_precedes_catalog_lookup)
{
	for (unsigned command = 0; command < 2; command++) {
		for (unsigned utility = 0; utility < 2; utility++) {
			reset_subscription(false);
			cluster_shared_config = true;
			active_owner_start_state = CLUSTER_NORMAL_START_EXISTING_OTHER;
			active_owner_recovered_open = false;
			UT_ASSERT(call_subscription(command, utility != 0));
			UT_ASSERT_EQ(last_ereport_errcode, ERRCODE_DATA_CORRUPTED);
			UT_ASSERT_EQ(sub_lookups + sub_parses + g_bind_emit_calls, 0);
			UT_ASSERT_EQ(cluster_tt_local_binding_count, 0);
		}
	}
}

UT_TEST(test_subscription_original_wait_errors_propagate_before_catalog_lock_or_bind)
{
	static const int errors[] = {ERRCODE_QUERY_CANCELED, ERRCODE_LOCK_NOT_AVAILABLE,
		ERRCODE_T_R_DEADLOCK_DETECTED, ERRCODE_CLUSTER_RECONFIG_IN_PROGRESS};

	for (unsigned command = 0; command < 2; command++) {
		for (unsigned utility = 0; utility < 2; utility++) {
			for (unsigned i = 0; i < lengthof(errors); i++) {
				reset_subscription(true);
				slot_wait_error = errors[i];
				UT_ASSERT(call_subscription(command, utility != 0));
				UT_ASSERT_EQ(last_ereport_errcode, errors[i]);
				UT_ASSERT_EQ(sub_lookups + sub_parses + g_bind_emit_calls, 0);
				UT_ASSERT_EQ(cluster_tt_local_binding_count, 0);
				UT_ASSERT_EQ(slot_wait_begins, 1);
				UT_ASSERT_EQ(slot_wait_ends, 1);
				UT_ASSERT_EQ(slot_wait_cancels, errors[i] == ERRCODE_QUERY_CANCELED);
				UT_ASSERT_EQ(modifier_enters, modifier_leaves);
			}
		}
	}
}

UT_TEST(test_subscription_unsafe_cold_wait_refuses_and_original_warm_binding_reuses)
{
	for (unsigned command = 0; command < 2; command++) {
		ClusterCanonicalTxnBinding binding;

		reset_subscription(true);
		slot_wait_context_safe = false;
		UT_ASSERT(call_subscription(command, false));
		UT_ASSERT_EQ(last_ereport_errcode, ERRCODE_PROGRAM_LIMIT_EXCEEDED);
		UT_ASSERT_EQ(sub_lookups + sub_parses + g_bind_emit_calls, 0);
		UT_ASSERT_EQ(cluster_tt_local_binding_count, 0);
		UT_ASSERT_EQ(slot_wait_begins + slot_wait_calls, 0);
		UT_ASSERT_EQ(modifier_enters, modifier_leaves);

		reset_subscription(false);
		UT_ASSERT(cluster_tt_local_prepare_canonical_active(GetTopTransactionId(), &binding));
		slot_wait_context_safe = false;
		UT_ASSERT(call_subscription(command, false));
		UT_ASSERT_EQ(last_ereport_errcode, ERRCODE_UNDEFINED_OBJECT);
		UT_ASSERT_EQ(sub_lookups, 1);
		UT_ASSERT_EQ(sub_unbound + sub_parses, 0);
		UT_ASSERT_EQ(sub_xids, 1);
		UT_ASSERT_EQ(g_bind_emit_calls, 1);
		UT_ASSERT_EQ(cluster_tt_local_binding_count, 1);
		UT_ASSERT_EQ(slot_wait_begins + slot_wait_calls + capacity_waits, 0);
	}
}

UT_TEST(test_subscription_native_create_preserves_original_shared_scope_refusal_before_options)
{
	for (unsigned mode = 0; mode < 8; mode++) {
		for (unsigned top = 0; top < 2; top++) {
			bool refused = (mode & 6) != 0;

			reset_subscription(false);
			cluster_enabled = (mode & 1) != 0;
			cluster_shared_catalog = (mode & 2) != 0;
			cluster_shared_config = (mode & 4) != 0;
			sub_is_top = top != 0;
			XactReadOnly = sub_parallel = sub_recovery = sub_child = true;
			UT_ASSERT(call_subscription(2, false));
			UT_ASSERT_EQ(last_ereport_errcode,
				refused ? ERRCODE_FEATURE_NOT_SUPPORTED : ERRCODE_UNDEFINED_OBJECT);
			UT_ASSERT_EQ(sub_parses, !refused);
			UT_ASSERT_EQ(sub_xids + sub_lookups + g_bind_emit_calls, 0);
			UT_ASSERT_EQ(cluster_tt_local_binding_count, 0);
		}
	}
}

UT_TEST(test_subscription_preflight_keeps_pure_select_and_unsupported_create_unbound)
{
	Node select = {.type = T_SelectStmt};

	reset_subscription(true);
	PreflightClusterSharedCatalogWrite(&select, true);
	PreflightClusterSharedCatalogWrite((Node *)&sub_create, true);
	UT_ASSERT_EQ(sub_xids + sub_lookups + sub_parses + g_bind_emit_calls, 0);
	UT_ASSERT_EQ(cluster_tt_local_binding_count, 0);
	UT_ASSERT_EQ(slot_wait_begins + slot_wait_calls + capacity_waits, 0);
}

int
main(void)
{
	UT_PLAN(8);
	UT_RUN(test_subscription_native_full_segment_waits_and_binds_before_first_catalog_lock);
	UT_RUN(test_subscription_native_route_and_utility_warm_binding_use_original_top_once);
	UT_RUN(test_subscription_native_context_and_both_entry_child_top_guards_precede_bind);
	UT_RUN(test_subscription_failed_canonical_publication_precedes_catalog_lookup);
	UT_RUN(test_subscription_original_wait_errors_propagate_before_catalog_lock_or_bind);
	UT_RUN(test_subscription_unsafe_cold_wait_refuses_and_original_warm_binding_reuses);
	UT_RUN(test_subscription_native_create_preserves_original_shared_scope_refusal_before_options);
	UT_RUN(test_subscription_preflight_keeps_pure_select_and_unsupported_create_unbound);
	UT_DONE();
	return ut_failed_count ? 1 : 0;
}
