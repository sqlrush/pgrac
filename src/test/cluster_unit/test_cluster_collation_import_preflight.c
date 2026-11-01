/* Author: SqlRush <sqlrush@gmail.com>
 * Copyright (c) 2026, pgrac contributors.
 * Actual import prefix/helper plus real allocator/local/durable BIND. The
 * enumeration boundary only raises ERROR; no locale/provider/import succeeds.
 * Admission, retention, native-owner scheduling and physical IO/WAL are doubles.
 */
#define TT_ACTIVE_OWNER_FIXTURE_MAIN import_active_owner_fixture_main
#include "test_cluster_tt_active_owner.c"
#undef main
#include "access/xact.h"
#include "tcop/utility.h"
#include "utils/fmgrprotos.h"
#include "utils/syscache.h"

bool XactReadOnly;
static bool import_parallel, import_recovery, import_child;
static bool import_superuser, import_namespace, import_expect_binding;
static TransactionId import_top, import_current;
static unsigned import_xids, import_lookups, import_entries, import_unbound;
static ClusterConf import_topology = { .node_count = 4 };

bool
IsInParallelMode(void)
{
	return import_parallel;
}

bool
RecoveryInProgress(void)
{
	return import_recovery;
}

bool
IsSubTransaction(void)
{
	return import_child;
}

TransactionId
GetCurrentTransactionIdIfAny(void)
{
	return import_current;
}

TransactionId
GetTopTransactionIdIfAny(void)
{
	return import_top;
}

TransactionId
GetTopTransactionId(void)
{
	if (!TransactionIdIsValid(import_top)) {
		import_xids++;
		import_top = import_current = 100;
	}
	return import_top;
}

bool
superuser(void)
{
	return import_superuser;
}

bool
SearchSysCacheExists(int cacheid, Datum key1, Datum key2, Datum key3, Datum key4)
{
	import_lookups++;
	UT_ASSERT_EQ(cacheid, NAMESPACEOID);
	UT_ASSERT_EQ(DatumGetObjectId(key1), 701);
	UT_ASSERT_EQ(key2 + key3 + key4, 0);
	return import_namespace;
}

static void import_enumeration_boundary(void) pg_attribute_noreturn();

static void
import_enumeration_boundary(void)
{
	ClusterCanonicalTxnBinding binding;

	import_entries++;
	UT_ASSERT_EQ(lock_depth, 0);
	UT_ASSERT_EQ(modifier_enters, modifier_leaves);
	if (import_expect_binding && !cluster_tt_local_get_published_binding(import_top, &binding))
		import_unbound++;
	ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT),
					errmsg("negative-only import enumeration boundary")));
	pg_unreachable();
}

#include "test_cluster_collation_import_preflight.inc"

static void
reset_import(bool full)
{
	if (full)
		start_full_active_test();
	else
		reset_active_owner();
	ClusterConfShmem = &import_topology;
	cluster_enabled = cluster_shared_catalog = true;
	slot_wait_terminal_capacity = full;
	g_modifier_recheck_hook = NULL;
	enable_sealed_header_io();
	XactReadOnly = import_parallel = import_recovery = import_child = false;
	import_superuser = import_namespace = import_expect_binding = true;
	import_top = import_current = InvalidTransactionId;
	import_xids = import_lookups = import_entries = import_unbound = 0;
	last_ereport_errcode = 0;
}

static bool
call_import(void)
{
	LOCAL_FCINFO(fcinfo, 1);
	volatile bool caught = false;

	memset(fcinfo, 0, SizeForFunctionCallInfo(1));
	fcinfo->nargs = 1;
	fcinfo->args[0].value = ObjectIdGetDatum(701);
	PG_TRY();
	{
		(void)pg_import_system_collations(fcinfo);
	}
	PG_CATCH();
	{
		caught = true;
	}
	PG_END_TRY();
	return caught;
}

UT_TEST(test_import_waits_and_binds_before_enumeration_without_switching_segment)
{
	for (unsigned commit = 0; commit < 2; commit++) {
		reset_import(true);
		slot_wait_commit = commit != 0;
		UT_ASSERT(call_import());
		UT_ASSERT_EQ(last_ereport_errcode, ERRCODE_UNDEFINED_OBJECT);
		UT_ASSERT_EQ(import_lookups, 1);
		UT_ASSERT_EQ(import_entries, 1);
		UT_ASSERT_EQ(import_unbound, 0);
		UT_ASSERT_EQ(import_xids, 1);
		UT_ASSERT_EQ(g_bind_emit_calls, 1);
		UT_ASSERT_EQ(slot_wait_calls, 1);
		UT_ASSERT_EQ(slot_wait_begins, 1);
		UT_ASSERT_EQ(slot_wait_ends, 1);
		UT_ASSERT_EQ(capacity_waits, 1);
		UT_ASSERT_EQ(cold_selection_calls, 0);
		UT_ASSERT_EQ(cluster_tt_slot_current_segment(0), 1);
		UT_ASSERT_EQ(cluster_tt_local_binding_count, 1);
	}
}

UT_TEST(test_import_route_profiles_bind_only_shared_catalog_storage)
{
	for (unsigned mode = 0; mode < 8; mode++) {
		bool bound = (mode & 3) == 3;

		reset_import(false);
		cluster_enabled = (mode & 1) != 0;
		cluster_shared_catalog = (mode & 2) != 0;
		cluster_shared_config = (mode & 4) != 0;
		import_expect_binding = bound;
		UT_ASSERT(call_import());
		UT_ASSERT_EQ(last_ereport_errcode, ERRCODE_UNDEFINED_OBJECT);
		UT_ASSERT_EQ(import_lookups, 1);
		UT_ASSERT_EQ(import_entries, 1);
		UT_ASSERT_EQ(import_unbound, 0);
		UT_ASSERT_EQ(import_xids, bound);
		UT_ASSERT_EQ(g_bind_emit_calls, bound);
		UT_ASSERT_EQ(cluster_tt_local_binding_count, bound);
	}
}

UT_TEST(test_import_privilege_and_namespace_errors_still_precede_preflight)
{
	for (unsigned bad = 0; bad < 2; bad++) {
		reset_import(false);
		import_superuser = bad != 0;
		import_namespace = false;
		XactReadOnly = import_parallel = import_child = true;
		UT_ASSERT(call_import());
		UT_ASSERT_EQ(last_ereport_errcode,
					 bad == 0 ? ERRCODE_INSUFFICIENT_PRIVILEGE : ERRCODE_UNDEFINED_SCHEMA);
		UT_ASSERT_EQ(import_lookups, bad);
		UT_ASSERT_EQ(import_entries + import_xids + g_bind_emit_calls, 0);
		UT_ASSERT_EQ(cluster_tt_local_binding_count, 0);
	}
}

UT_TEST(test_import_context_child_and_top_refusals_precede_xid_and_enumeration)
{
	static const int errors[] = { ERRCODE_READ_ONLY_SQL_TRANSACTION,
		ERRCODE_INVALID_TRANSACTION_STATE, ERRCODE_READ_ONLY_SQL_TRANSACTION,
		ERRCODE_FEATURE_NOT_SUPPORTED, ERRCODE_INTERNAL_ERROR };

	for (unsigned guard = 0; guard < lengthof(errors); guard++) {
		reset_import(false);
		XactReadOnly = guard == 0;
		import_parallel = guard == 1;
		import_recovery = guard == 2;
		import_child = guard == 3;
		if (guard == 4) {
			import_top = 100;
			import_current = 101;
		}
		UT_ASSERT(call_import());
		UT_ASSERT_EQ(last_ereport_errcode, errors[guard]);
		UT_ASSERT_EQ(import_lookups, 1);
		UT_ASSERT_EQ(import_entries + import_xids + g_bind_emit_calls, 0);
		UT_ASSERT_EQ(cluster_tt_local_binding_count, 0);
	}
}

UT_TEST(test_import_failed_canonical_publication_stops_before_enumeration)
{
	reset_import(false);
	cluster_shared_config = true;
	active_owner_start_state = CLUSTER_NORMAL_START_EXISTING_OTHER;
	active_owner_recovered_open = false;
	UT_ASSERT(call_import());
	UT_ASSERT_EQ(last_ereport_errcode, ERRCODE_DATA_CORRUPTED);
	UT_ASSERT_EQ(import_entries + g_bind_emit_calls, 0);
	UT_ASSERT_EQ(cluster_tt_local_binding_count, 0);
}

UT_TEST(test_import_native_errors_keep_original_sqlstate_without_enumeration)
{
	static const int errors[] = { ERRCODE_QUERY_CANCELED, ERRCODE_LOCK_NOT_AVAILABLE,
		ERRCODE_T_R_DEADLOCK_DETECTED, ERRCODE_CLUSTER_RECONFIG_IN_PROGRESS, ERRCODE_IO_ERROR };

	for (unsigned i = 0; i < lengthof(errors); i++) {
		reset_import(true);
		slot_wait_error = errors[i];
		UT_ASSERT(call_import());
		UT_ASSERT_EQ(last_ereport_errcode, errors[i]);
		UT_ASSERT_EQ(import_entries + g_bind_emit_calls, 0);
		UT_ASSERT_EQ(cluster_tt_local_binding_count, 0);
		UT_ASSERT_EQ(slot_wait_begins, 1);
		UT_ASSERT_EQ(slot_wait_ends, 1);
		UT_ASSERT_EQ(slot_wait_cancels, errors[i] == ERRCODE_QUERY_CANCELED);
		UT_ASSERT_EQ(modifier_enters, modifier_leaves);
	}
}

UT_TEST(test_import_keeps_unsafe_capacity_refusal_and_original_warm_binding)
{
	ClusterCanonicalTxnBinding binding;

	reset_import(true);
	slot_wait_context_safe = false;
	UT_ASSERT(call_import());
	UT_ASSERT_EQ(last_ereport_errcode, ERRCODE_PROGRAM_LIMIT_EXCEEDED);
	UT_ASSERT_EQ(import_entries + g_bind_emit_calls, 0);
	UT_ASSERT_EQ(cluster_tt_local_binding_count, 0);
	UT_ASSERT_EQ(slot_wait_calls + slot_wait_begins, 0);

	reset_import(false);
	UT_ASSERT(cluster_tt_local_prepare_canonical_active(GetTopTransactionId(), &binding));
	slot_wait_context_safe = false;
	UT_ASSERT(call_import());
	UT_ASSERT_EQ(last_ereport_errcode, ERRCODE_UNDEFINED_OBJECT);
	UT_ASSERT_EQ(import_entries, 1);
	UT_ASSERT_EQ(import_unbound, 0);
	UT_ASSERT_EQ(import_xids, 1);
	UT_ASSERT_EQ(g_bind_emit_calls, 1);
	UT_ASSERT_EQ(cluster_tt_local_binding_count, 1);
	UT_ASSERT_EQ(slot_wait_calls + slot_wait_begins + capacity_waits, 0);
}

int
main(void)
{
	UT_PLAN(7);
	UT_RUN(test_import_waits_and_binds_before_enumeration_without_switching_segment);
	UT_RUN(test_import_route_profiles_bind_only_shared_catalog_storage);
	UT_RUN(test_import_privilege_and_namespace_errors_still_precede_preflight);
	UT_RUN(test_import_context_child_and_top_refusals_precede_xid_and_enumeration);
	UT_RUN(test_import_failed_canonical_publication_stops_before_enumeration);
	UT_RUN(test_import_native_errors_keep_original_sqlstate_without_enumeration);
	UT_RUN(test_import_keeps_unsafe_capacity_refusal_and_original_warm_binding);
	UT_DONE();
	return ut_failed_count ? 1 : 0;
}
