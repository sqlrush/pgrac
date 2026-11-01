/* Author: SqlRush <sqlrush@gmail.com>
 * Copyright (c) 2026, pgrac contributors.
 * Complete DISCARD/namespace cleanup bodies plus real TT allocation and abort.
 * Deletion only raises ERROR; no successful directory or exit cleanup is faked.
 * Transaction inputs, admission, owner scheduling, CV and physical IO are doubles.
 */
#define TT_ACTIVE_OWNER_FIXTURE_MAIN discard_active_owner_fixture_main
#include "test_cluster_tt_active_owner.c"
#undef main
#include "access/xact.h"
#include "catalog/dependency.h"
#include "catalog/namespace.h"
#include "catalog/pg_namespace.h"
#include "commands/async.h"
#include "commands/discard.h"
#include "commands/prepare.h"
#include "commands/sequence.h"
#include "tcop/utility.h"
#include "utils/guc.h"
#include "utils/plancache.h"
#include "utils/portal.h"
#include "utils/snapmgr.h"
#include "test_cluster_discard_xact_types.inc"

/* Only blockState is read by the extracted transaction-block guard. */
static struct {
	TBlockState blockState;
} discard_xact_state;
static typeof(discard_xact_state) *CurrentTransactionState = &discard_xact_state;
static Oid myTempNamespace, myTempToastNamespace;
bool XactReadOnly;
int MyXactFlags;
static bool discard_parallel, discard_recovery, discard_child, discard_in_block;
static bool discard_expect_binding;
static TransactionId discard_top, discard_current;
static unsigned discard_xids, discard_entries, discard_unbound;
static unsigned discard_callback_aborts, discard_callback_starts, discard_snapshot_pushes;
static unsigned discard_snapshot_pops, discard_callback_commits, discard_status_installs;
static unsigned discard_hints;
static char discard_trace[32];
static unsigned discard_trace_length;
static SnapshotData discard_snapshot;
static ClusterConf discard_topology = { .node_count = 4 };

bool
IsInParallelMode(void)
{
	return discard_parallel;
}

bool
RecoveryInProgress(void)
{
	return discard_recovery;
}

bool
IsSubTransaction(void)
{
	return discard_child;
}

bool
IsTransactionBlock(void)
{
	return discard_in_block;
}

TransactionId
GetCurrentTransactionIdIfAny(void)
{
	return discard_current;
}

TransactionId
GetTopTransactionIdIfAny(void)
{
	return discard_top;
}

TransactionId
GetTopTransactionId(void)
{
	if (!TransactionIdIsValid(discard_top)) {
		discard_xids++;
		discard_top = discard_current = 100;
	}
	return discard_top;
}

static void
trace_discard(char step)
{
	UT_ASSERT(discard_trace_length + 1 < sizeof(discard_trace));
	discard_trace[discard_trace_length++] = step;
	discard_trace[discard_trace_length] = '\0';
}

void
PortalHashTableDeleteAll(void)
{
	trace_discard('P');
}

void
SetPGVariable(const char *name, List *args, bool is_local)
{
	UT_ASSERT(strcmp(name, "session_authorization") == 0);
	UT_ASSERT(args == NIL && !is_local);
	trace_discard('A');
}

void
ResetAllOptions(void)
{
	trace_discard('O');
}

void
DropAllPreparedStatements(void)
{
	trace_discard('D');
}

void
Async_UnlistenAll(void)
{
	trace_discard('U');
}

void
LockReleaseAll(LOCKMETHODID method, bool all_locks)
{
	UT_ASSERT_EQ(method, USER_LOCKMETHOD);
	UT_ASSERT(all_locks);
	trace_discard('L');
}

void
ResetPlanCache(void)
{
	trace_discard('C');
}

void
ResetSequenceCaches(void)
{
	trace_discard('Q');
}

void
performDeletion(const ObjectAddress *object, DropBehavior behavior, int flags)
{
	ClusterCanonicalTxnBinding binding;

	discard_entries++;
	trace_discard('T');
	UT_ASSERT_EQ(object->classId, NamespaceRelationId);
	UT_ASSERT_EQ(object->objectId, 701);
	UT_ASSERT_EQ(object->objectSubId, 0);
	UT_ASSERT_EQ(behavior, DROP_CASCADE);
	UT_ASSERT_EQ(flags, PERFORM_DELETION_INTERNAL | PERFORM_DELETION_QUIETLY
						| PERFORM_DELETION_SKIP_ORIGINAL | PERFORM_DELETION_SKIP_EXTENSIONS);
	UT_ASSERT_EQ(lock_depth, 0);
	UT_ASSERT_EQ(modifier_enters, modifier_leaves);
	if (discard_expect_binding && !cluster_tt_local_get_published_binding(discard_top, &binding))
		discard_unbound++;
	ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT),
					errmsg("negative-only first temp deletion-lock boundary")));
	pg_unreachable();
}

void
AbortOutOfAnyTransaction(void)
{
	UT_ASSERT_EQ(cluster_tt_local_binding_count, 0);
	discard_callback_aborts++;
	discard_top = discard_current = InvalidTransactionId;
}

void
StartTransactionCommand(void)
{
	discard_callback_starts++;
}

Snapshot
GetTransactionSnapshot(void)
{
	return &discard_snapshot;
}

void
PushActiveSnapshot(Snapshot snapshot)
{
	UT_ASSERT(snapshot == &discard_snapshot);
	discard_snapshot_pushes++;
}

void
PopActiveSnapshot(void)
{
	discard_snapshot_pops++;
}

void
CommitTransactionCommand(void)
{
	discard_callback_commits++;
}

ClusterSemanticAdmissionResult
cluster_tt_status_source_dispatch(ClusterTTStatusSourceOp operation,
								  const ClusterTTStatusSourceRequest *request,
								  ClusterTTStatusSourceResult *result)
{
	memset(result, 0, sizeof(*result));
	UT_ASSERT(operation == CLUSTER_TT_SOURCE_INSTALL_LOCAL || operation == CLUSTER_TT_SOURCE_LOOKUP
			  || operation == CLUSTER_TT_SOURCE_BUMP_SELF_CONSUMER_HIT);
	if (operation == CLUSTER_TT_SOURCE_INSTALL_LOCAL) {
		UT_ASSERT_EQ(request->status, CLUSTER_TT_STATUS_ABORTED);
		UT_ASSERT_EQ(request->key->local_xid, 100);
		discard_status_installs++;
	}
	result->bool_value = true;
	result->lookup.authoritative = true;
	result->lookup.status = CLUSTER_TT_STATUS_ABORTED;
	return CLUSTER_SEMANTIC_ADMISSION_OK;
}

ClusterSemanticAdmissionResult
cluster_tt_status_hint_source_dispatch(ClusterTTStatusHintSourceOp operation,
									   const ClusterTTStatusHintSourceRequest *request)
{
	UT_ASSERT_EQ(operation, CLUSTER_TT_HINT_SOURCE_EMIT);
	UT_ASSERT_EQ(request->status, CLUSTER_TT_STATUS_ABORTED);
	UT_ASSERT_EQ(request->key->local_xid, 100);
	discard_hints++;
	return CLUSTER_SEMANTIC_ADMISSION_OK;
}

#include "test_cluster_discard_preflight.inc"

static void
reset_discard(bool full)
{
	if (full)
		start_full_active_test();
	else
		reset_active_owner();
	ClusterConfShmem = &discard_topology;
	cluster_enabled = cluster_shared_catalog = true;
	slot_wait_terminal_capacity = full;
	g_modifier_recheck_hook = NULL;
	enable_sealed_header_io();
	myTempNamespace = 701;
	myTempToastNamespace = 702;
	XactReadOnly = discard_parallel = discard_recovery = discard_child = discard_in_block = false;
	discard_expect_binding = true;
	discard_top = discard_current = InvalidTransactionId;
	discard_xids = discard_entries = discard_unbound = 0;
	discard_callback_aborts = discard_callback_starts = discard_snapshot_pushes = 0;
	discard_snapshot_pops = discard_callback_commits = discard_status_installs = discard_hints = 0;
	discard_trace_length = 0;
	discard_trace[0] = '\0';
	MyXactFlags = 0;
	discard_xact_state.blockState = TBLOCK_STARTED;
	last_ereport_errcode = 0;
}

static bool
call_discard(DiscardMode target, bool is_top_level)
{
	DiscardStmt stmt = { .type = T_DiscardStmt, .target = target };
	volatile bool caught = false;

	PG_TRY();
	{
		DiscardCommand(&stmt, is_top_level);
	}
	PG_CATCH();
	{
		caught = true;
	}
	PG_END_TRY();
	return caught;
}

static void
assert_no_new_binding(void)
{
	UT_ASSERT_EQ(cluster_tt_local_binding_count, 0);
	UT_ASSERT_EQ(g_bind_emit_calls, 0);
	UT_ASSERT_EQ(modifier_enters, modifier_leaves);
	UT_ASSERT_EQ(lock_depth, 0);
	for (unsigned i = 0; i < 48; i++)
		UT_ASSERT(allocator_storage.per_node[0].slots[i].status != CTS_ACTIVE
				  || allocator_storage.per_node[0].slots[i].xid != 100);
}

UT_TEST(test_temp_and_all_bind_after_full_slot_wait_before_first_delete_lock)
{
	const DiscardMode targets[] = { DISCARD_TEMP, DISCARD_ALL };

	for (unsigned t = 0; t < lengthof(targets); t++) {
		for (unsigned commit = 0; commit < 2; commit++) {
			reset_discard(true);
			slot_wait_commit = commit != 0;
			UT_ASSERT(call_discard(targets[t], true));
			UT_ASSERT_EQ(last_ereport_errcode, ERRCODE_UNDEFINED_OBJECT);
			UT_ASSERT_EQ(discard_entries, 1);
			UT_ASSERT_EQ(discard_unbound, 0);
			UT_ASSERT_EQ(discard_xids, 1);
			UT_ASSERT_EQ(g_bind_emit_calls, 1);
			UT_ASSERT_EQ(slot_wait_calls, 1);
			UT_ASSERT_EQ(slot_wait_begins, 1);
			UT_ASSERT_EQ(slot_wait_ends, 1);
			UT_ASSERT_EQ(capacity_waits, 1);
			UT_ASSERT_EQ(cold_selection_calls, 0);
			UT_ASSERT_EQ(cluster_tt_slot_current_segment(0), 1);
			UT_ASSERT_EQ(cluster_tt_local_binding_count, 1);
			UT_ASSERT(strcmp(discard_trace, t == 0 ? "T" : "PAODULCT") == 0);
			UT_ASSERT_EQ((MyXactFlags & XACT_FLAGS_NEEDIMMEDIATECOMMIT) != 0, t == 1);
		}
	}
}

UT_TEST(test_no_namespace_pure_commands_and_nonshared_do_not_bind)
{
	const DiscardMode targets[] = { DISCARD_TEMP, DISCARD_ALL, DISCARD_PLANS, DISCARD_SEQUENCES };

	for (unsigned mode = 0; mode < 8; mode++) {
		for (unsigned t = 0; t < lengthof(targets); t++) {
			for (unsigned has_namespace = 0; has_namespace < 2; has_namespace++) {
				bool cleanup = has_namespace && t < 2;
				bool bound = cleanup && (mode & 3) == 3;

				reset_discard(false);
				cluster_enabled = (mode & 1) != 0;
				cluster_shared_catalog = (mode & 2) != 0;
				cluster_shared_config = (mode & 4) != 0;
				if (!has_namespace)
					myTempNamespace = myTempToastNamespace = InvalidOid;
				discard_expect_binding = bound;
				UT_ASSERT_EQ(call_discard(targets[t], true), cleanup);
				UT_ASSERT_EQ(discard_entries, cleanup);
				UT_ASSERT_EQ(discard_unbound, 0);
				UT_ASSERT_EQ(discard_xids, bound);
				UT_ASSERT_EQ(g_bind_emit_calls, bound);
				UT_ASSERT_EQ(cluster_tt_local_binding_count, bound);
			}
		}
	}
	reset_discard(false);
	myTempNamespace = myTempToastNamespace = InvalidOid;
	XactReadOnly = discard_parallel = discard_recovery = discard_child = true;
	UT_ASSERT(!call_discard(DISCARD_TEMP, true));
	UT_ASSERT_EQ(discard_xids + discard_entries, 0);
	assert_no_new_binding();
}

UT_TEST(test_existing_namespace_shared_context_guards_precede_xid_and_cleanup)
{
	static const int errors[] = { ERRCODE_READ_ONLY_SQL_TRANSACTION,
		ERRCODE_INVALID_TRANSACTION_STATE, ERRCODE_READ_ONLY_SQL_TRANSACTION,
		ERRCODE_FEATURE_NOT_SUPPORTED, ERRCODE_INTERNAL_ERROR };

	for (unsigned guard = 0; guard < lengthof(errors); guard++) {
		reset_discard(false);
		XactReadOnly = guard == 0;
		discard_parallel = guard == 1;
		discard_recovery = guard == 2;
		discard_child = guard == 3;
		if (guard == 4) {
			discard_top = 100;
			discard_current = 101;
		}
		UT_ASSERT(call_discard(DISCARD_TEMP, true));
		UT_ASSERT_EQ(last_ereport_errcode, errors[guard]);
		UT_ASSERT_EQ(discard_entries + discard_xids, 0);
		assert_no_new_binding();
	}
}

UT_TEST(test_all_original_transaction_restrictions_still_precede_temp_step)
{
	for (unsigned guard = 0; guard < 4; guard++) {
		reset_discard(false);
		XactReadOnly = true;
		discard_in_block = guard == 0;
		discard_child = guard == 1;
		MyXactFlags = guard == 2 ? XACT_FLAGS_PIPELINING : 0;
		UT_ASSERT(call_discard(DISCARD_ALL, guard != 3));
		UT_ASSERT_EQ(last_ereport_errcode, ERRCODE_ACTIVE_SQL_TRANSACTION);
		UT_ASSERT_EQ(discard_entries + discard_xids + discard_trace_length, 0);
		UT_ASSERT_EQ(MyXactFlags & XACT_FLAGS_NEEDIMMEDIATECOMMIT, 0);
		assert_no_new_binding();
	}
	reset_discard(false);
	XactReadOnly = true;
	UT_ASSERT(call_discard(DISCARD_ALL, true));
	UT_ASSERT_EQ(last_ereport_errcode, ERRCODE_READ_ONLY_SQL_TRANSACTION);
	UT_ASSERT(strcmp(discard_trace, "PAODULC") == 0);
	assert_no_new_binding();
}

UT_TEST(test_unsafe_capacity_stays_refused_and_warm_binding_reuses_original_slot)
{
	const DiscardMode targets[] = { DISCARD_TEMP, DISCARD_ALL };

	for (unsigned t = 0; t < lengthof(targets); t++) {
		ClusterCanonicalTxnBinding binding;

		reset_discard(true);
		slot_wait_context_safe = false;
		UT_ASSERT(call_discard(targets[t], true));
		UT_ASSERT_EQ(last_ereport_errcode, ERRCODE_PROGRAM_LIMIT_EXCEEDED);
		UT_ASSERT_EQ(discard_entries + slot_wait_calls + slot_wait_begins, 0);
		assert_no_new_binding();
		reset_discard(false);
		UT_ASSERT(cluster_tt_local_prepare_canonical_active(GetTopTransactionId(), &binding));
		slot_wait_context_safe = false;
		UT_ASSERT(call_discard(targets[t], true));
		UT_ASSERT_EQ(last_ereport_errcode, ERRCODE_UNDEFINED_OBJECT);
		UT_ASSERT_EQ(discard_entries, 1);
		UT_ASSERT_EQ(discard_unbound, 0);
		UT_ASSERT_EQ(discard_xids, 1);
		UT_ASSERT_EQ(g_bind_emit_calls, 1);
		UT_ASSERT_EQ(cluster_tt_local_binding_count, 1);
		UT_ASSERT_EQ(slot_wait_calls + capacity_waits, 0);
	}
}

UT_TEST(test_native_wait_errors_preserve_sqlstate_and_leave_no_caller_slot)
{
	const DiscardMode targets[] = { DISCARD_TEMP, DISCARD_ALL };
	static const int errors[] = { ERRCODE_QUERY_CANCELED, ERRCODE_LOCK_NOT_AVAILABLE,
		ERRCODE_T_R_DEADLOCK_DETECTED, ERRCODE_CLUSTER_RECONFIG_IN_PROGRESS, ERRCODE_IO_ERROR };

	for (unsigned t = 0; t < lengthof(targets); t++) {
		for (unsigned i = 0; i < lengthof(errors); i++) {
			reset_discard(true);
			slot_wait_error = errors[i];
			UT_ASSERT(call_discard(targets[t], true));
			UT_ASSERT_EQ(last_ereport_errcode, errors[i]);
			UT_ASSERT_EQ(discard_entries, 0);
			UT_ASSERT_EQ(slot_wait_begins, 1);
			UT_ASSERT_EQ(slot_wait_ends, 1);
			UT_ASSERT_EQ(slot_wait_cancels, errors[i] == ERRCODE_QUERY_CANCELED);
			UT_ASSERT_EQ(slot_wait_event, WAIT_EVENT_CLIENT_READ);
			assert_no_new_binding();
		}
	}
}

UT_TEST(test_failed_canonical_publication_stops_before_temp_deletion)
{
	reset_discard(false);
	cluster_shared_config = true;
	active_owner_start_state = CLUSTER_NORMAL_START_EXISTING_OTHER;
	active_owner_recovered_open = false;
	UT_ASSERT(call_discard(DISCARD_TEMP, true));
	UT_ASSERT_EQ(last_ereport_errcode, ERRCODE_DATA_CORRUPTED);
	UT_ASSERT_EQ(discard_entries, 0);
	assert_no_new_binding();
}

UT_TEST(test_deletion_error_binding_is_retired_only_after_original_durable_abort)
{
	const DiscardMode targets[] = { DISCARD_TEMP, DISCARD_ALL };

	for (unsigned t = 0; t < lengthof(targets); t++) {
		ClusterCanonicalTxnBinding binding;
		UndoSegmentHeaderData *resident = (UndoSegmentHeaderData *)g_current_resident;

		reset_discard(false);
		UT_ASSERT(call_discard(targets[t], true));
		UT_ASSERT_EQ(last_ereport_errcode, ERRCODE_UNDEFINED_OBJECT);
		if (!cluster_tt_local_get_published_binding(discard_top, &binding)) {
			UT_ASSERT(false);
			continue;
		}
		UT_ASSERT_EQ(resident->tt_slots[binding.slot_offset].status, TT_SLOT_ACTIVE);
		g_require_abort_flush_before_write = true;
		UT_ASSERT(cluster_tt_local_preabort_durable_finish(discard_top));
		UT_ASSERT(g_abort_flush_seen);
		UT_ASSERT_EQ(g_abort_exact_emit_calls, 1);
		UT_ASSERT_EQ(resident->tt_slots[binding.slot_offset].status, TT_SLOT_ABORTED);
		cluster_tt_local_record_abort(discard_top);
		UT_ASSERT_EQ(cluster_tt_local_binding_count, 0);
		UT_ASSERT_EQ(discard_status_installs, 1);
		UT_ASSERT_EQ(discard_hints, 1);
		UT_ASSERT_EQ(allocator_storage.per_node[0].slots[binding.slot_offset].status, CTS_ABORTED);
		UT_ASSERT_EQ(modifier_enters, modifier_leaves);
		UT_ASSERT_EQ(lock_depth, 0);
	}
}

UT_TEST(test_readonly_refusal_keeps_original_exit_callback_cleanup_entry)
{
	volatile bool caught = false;

	reset_discard(false);
	XactReadOnly = true;
	UT_ASSERT(call_discard(DISCARD_TEMP, true));
	UT_ASSERT_EQ(last_ereport_errcode, ERRCODE_READ_ONLY_SQL_TRANSACTION);
	UT_ASSERT_EQ(discard_entries, 0);
	assert_no_new_binding();
	discard_expect_binding = false;
	discard_entries = 0;
	PG_TRY();
	{
		RemoveTempRelationsCallback(0, (Datum)0);
	}
	PG_CATCH();
	{
		caught = true;
	}
	PG_END_TRY();
	UT_ASSERT(caught);
	UT_ASSERT_EQ(last_ereport_errcode, ERRCODE_UNDEFINED_OBJECT);
	UT_ASSERT(XactReadOnly);
	UT_ASSERT_EQ(discard_entries, 1);
	UT_ASSERT_EQ(discard_callback_aborts, 1);
	UT_ASSERT_EQ(discard_callback_starts, 1);
	UT_ASSERT_EQ(discard_snapshot_pushes, 1);
	UT_ASSERT_EQ(discard_snapshot_pops + discard_callback_commits, 0);
	UT_ASSERT_EQ(discard_xids, 0);
	assert_no_new_binding();
}

int
main(void)
{
	UT_PLAN(9);
	UT_RUN(test_temp_and_all_bind_after_full_slot_wait_before_first_delete_lock);
	UT_RUN(test_no_namespace_pure_commands_and_nonshared_do_not_bind);
	UT_RUN(test_existing_namespace_shared_context_guards_precede_xid_and_cleanup);
	UT_RUN(test_all_original_transaction_restrictions_still_precede_temp_step);
	UT_RUN(test_unsafe_capacity_stays_refused_and_warm_binding_reuses_original_slot);
	UT_RUN(test_native_wait_errors_preserve_sqlstate_and_leave_no_caller_slot);
	UT_RUN(test_failed_canonical_publication_stops_before_temp_deletion);
	UT_RUN(test_deletion_error_binding_is_retired_only_after_original_durable_abort);
	UT_RUN(test_readonly_refusal_keeps_original_exit_callback_cleanup_entry);
	UT_DONE();
	return ut_failed_count ? 1 : 0;
}
