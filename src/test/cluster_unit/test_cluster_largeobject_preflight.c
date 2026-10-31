/* Author: SqlRush <sqlrush@gmail.com>
 * Copyright (c) 2026, pgrac contributors.
 * Complete native LO mutation entries + real allocator/local/durable BIND.
 * Operation leaves are ERROR-only boundaries, never successful mutation stubs.
 * Admission, physical IO/WAL and exact-owner scheduling use the parent seams.
 */
#define TT_ACTIVE_OWNER_FIXTURE_MAIN largeobject_active_owner_fixture_main
#include "test_cluster_tt_active_owner.c"
#undef main
#include "access/detoast.h"
#include "access/genam.h"
#include "access/sysattr.h"
#include "access/xact.h"
#include "catalog/dependency.h"
#include "catalog/indexing.h"
#include "catalog/objectaccess.h"
#include "catalog/pg_largeobject.h"
#include "storage/large_object.h"
#include "tcop/utility.h"
#include "utils/acl.h"
#include "utils/fmgroids.h"
#include "utils/rel.h"

bool XactReadOnly;
static bool lo_parallel, lo_recovery, lo_child;
static TransactionId lo_top, lo_current;
static unsigned lo_xids, lo_deletions, lo_unbound, lo_cci;
static unsigned lo_mutation_entries;
static int lo_write_result;
static Relation lo_heap_r, lo_index_r;
static bool lo_expect_binding;
static ClusterConf lo_topology = {.node_count = 4};

bool
IsInParallelMode(void)
{
	return lo_parallel;
}

bool
RecoveryInProgress(void)
{
	return lo_recovery;
}

bool
IsSubTransaction(void)
{
	return lo_child;
}

TransactionId
GetCurrentTransactionIdIfAny(void)
{
	return lo_current;
}

TransactionId
GetTopTransactionIdIfAny(void)
{
	return lo_top;
}

TransactionId
GetTopTransactionId(void)
{
	if (!TransactionIdIsValid(lo_top)) {
		lo_xids++;
		lo_top = lo_current = 100;
	}
	return lo_top;
}

/* Like the parent's errmsg observer, leave ERROR/SQLSTATE to errfinish. */
int
errmsg_internal(const char *fmt pg_attribute_unused(), ...)
{
	return 0;
}

void
performDeletion(const ObjectAddress *object, DropBehavior behavior, int flags)
{
	ClusterCanonicalTxnBinding binding;

	lo_deletions++;
	UT_ASSERT_EQ(object->classId, LargeObjectRelationId);
	UT_ASSERT_EQ(object->objectId, 701);
	UT_ASSERT_EQ(object->objectSubId, 0);
	UT_ASSERT_EQ(behavior, DROP_CASCADE);
	UT_ASSERT_EQ(flags, 0);
	UT_ASSERT_EQ(lock_depth, 0);
	UT_ASSERT_EQ(modifier_enters, modifier_leaves);
	if (lo_expect_binding && !cluster_tt_local_get_published_binding(lo_top, &binding))
		lo_unbound++;
	/* Stop before the real first deletion lock; do not claim delete success. */
	ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT), errmsg("negative-only deletion boundary")));
}

void
CommandCounterIncrement(void)
{
	lo_cci++;
	/* Mutations never succeed in this fixture. */
	UT_ASSERT(false);
}

/* These leaves model the first metadata/open boundary. They never simulate a
 * successful mutation; native code beyond them is compiled but not executed. */
static void lo_mutation_boundary(void) pg_attribute_noreturn();
static void open_lo_relation(void) pg_attribute_noreturn();
Oid LargeObjectCreate(Oid loid) pg_attribute_noreturn();

static void
lo_mutation_boundary(void)
{
	ClusterCanonicalTxnBinding binding;

	lo_mutation_entries++;
	UT_ASSERT_EQ(lock_depth, 0);
	UT_ASSERT_EQ(modifier_enters, modifier_leaves);
	if (lo_expect_binding && !cluster_tt_local_get_published_binding(lo_top, &binding))
		lo_unbound++;
	ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT), errmsg("negative-only LO mutation boundary")));
	pg_unreachable();
}

Oid
LargeObjectCreate(Oid loid)
{
	UT_ASSERT_EQ(loid, 701);
	lo_mutation_boundary();
}

static void
open_lo_relation(void)
{
	lo_mutation_boundary();
}

#include "test_cluster_largeobject_preflight.inc"

static void
reset_lo(bool full)
{
	if (full)
		start_full_active_test();
	else
		reset_active_owner();
	ClusterConfShmem = &lo_topology;
	cluster_shared_catalog = true;
	slot_wait_terminal_capacity = full;
	/* Retain one physical image across native wait, terminal recheck and BIND. */
	g_modifier_recheck_hook = NULL;
	enable_sealed_header_io();
	XactReadOnly = lo_parallel = lo_recovery = lo_child = false;
	lo_top = lo_current = InvalidTransactionId;
	lo_xids = lo_deletions = lo_unbound = lo_cci = 0;
	lo_mutation_entries = 0;
	lo_write_result = -1;
	lo_expect_binding = true;
	last_ereport_errcode = 0;
}

static bool
call_lo(void)
{
	volatile bool caught = false;

	PG_TRY();
	{
		(void)inv_drop(701);
	}
	PG_CATCH();
	{
		caught = true;
	}
	PG_END_TRY();
	return caught;
}

UT_TEST(test_largeobject_full_selected_segment_binds_after_original_wait_before_deletion_lock)
{
	reset_lo(true);
	UT_ASSERT(call_lo());
	UT_ASSERT_EQ(last_ereport_errcode, ERRCODE_UNDEFINED_OBJECT);
	UT_ASSERT_EQ(lo_deletions, 1);
	UT_ASSERT_EQ(lo_unbound + lo_cci, 0);
	UT_ASSERT_EQ(lo_xids, 1);
	UT_ASSERT_EQ(g_bind_emit_calls, 1);
	UT_ASSERT_EQ(slot_wait_calls, 1);
	UT_ASSERT_EQ(capacity_waits, 1);
	UT_ASSERT_EQ(cold_selection_calls, 0);
	UT_ASSERT_EQ(cluster_tt_slot_current_segment(0), 1);
	UT_ASSERT_EQ(cluster_tt_local_binding_count, 1);
}

UT_TEST(test_largeobject_catalog_route_precedes_original_deletion)
{
	for (unsigned mode = 0; mode < 8; mode++) {
		bool bound = (mode & 3) == 3;

		reset_lo(false);
		cluster_enabled = (mode & 1) != 0;
		cluster_shared_catalog = (mode & 2) != 0;
		cluster_shared_config = (mode & 4) != 0;
		lo_expect_binding = bound;
		UT_ASSERT(call_lo());
		UT_ASSERT_EQ(last_ereport_errcode, ERRCODE_UNDEFINED_OBJECT);
		UT_ASSERT_EQ(lo_deletions, 1);
		UT_ASSERT_EQ(lo_unbound + lo_cci, 0);
		UT_ASSERT_EQ(lo_xids, bound);
		UT_ASSERT_EQ(g_bind_emit_calls, bound);
		UT_ASSERT_EQ(cluster_tt_local_binding_count, bound);
	}
}

UT_TEST(test_largeobject_context_child_and_top_rejections_precede_xid_bind_and_deletion)
{
	static const int errors[] = {ERRCODE_READ_ONLY_SQL_TRANSACTION,
		ERRCODE_INVALID_TRANSACTION_STATE, ERRCODE_READ_ONLY_SQL_TRANSACTION,
		ERRCODE_FEATURE_NOT_SUPPORTED, ERRCODE_INTERNAL_ERROR};

	for (unsigned guard = 0; guard < lengthof(errors); guard++) {
		reset_lo(false);
		XactReadOnly = guard == 0;
		lo_parallel = guard == 1;
		lo_recovery = guard == 2;
		lo_child = guard == 3;
		if (guard == 4) {
			lo_current = 101;
			lo_top = 100;
		}
		UT_ASSERT(call_lo());
		UT_ASSERT_EQ(last_ereport_errcode, errors[guard]);
		UT_ASSERT_EQ(lo_xids + lo_deletions + g_bind_emit_calls + lo_cci, 0);
		UT_ASSERT_EQ(cluster_tt_local_binding_count, 0);
	}
}

UT_TEST(test_largeobject_failed_canonical_publication_stops_before_deletion)
{
	reset_lo(false);
	cluster_shared_config = true;
	active_owner_start_state = CLUSTER_NORMAL_START_EXISTING_OTHER;
	active_owner_recovered_open = false;
	UT_ASSERT(call_lo());
	UT_ASSERT_EQ(last_ereport_errcode, ERRCODE_DATA_CORRUPTED);
	UT_ASSERT_EQ(lo_deletions + g_bind_emit_calls + lo_cci, 0);
	UT_ASSERT_EQ(cluster_tt_local_binding_count, 0);
}

UT_TEST(test_largeobject_original_native_wait_errors_propagate_without_deletion_or_bind)
{
	static const int errors[] = {ERRCODE_QUERY_CANCELED, ERRCODE_LOCK_NOT_AVAILABLE,
		ERRCODE_T_R_DEADLOCK_DETECTED, ERRCODE_CLUSTER_RECONFIG_IN_PROGRESS};

	for (unsigned i = 0; i < lengthof(errors); i++) {
		reset_lo(true);
		slot_wait_error = errors[i];
		UT_ASSERT(call_lo());
		UT_ASSERT_EQ(last_ereport_errcode, errors[i]);
		UT_ASSERT_EQ(lo_deletions + g_bind_emit_calls + lo_cci, 0);
		UT_ASSERT_EQ(cluster_tt_local_binding_count, 0);
		UT_ASSERT_EQ(slot_wait_begins, 1);
		UT_ASSERT_EQ(slot_wait_ends, 1);
		UT_ASSERT_EQ(slot_wait_cancels, errors[i] == ERRCODE_QUERY_CANCELED);
		UT_ASSERT_EQ(modifier_enters, modifier_leaves);
	}
}

UT_TEST(test_largeobject_original_unsafe_wait_refuses_but_existing_binding_reuses)
{
	ClusterCanonicalTxnBinding binding;

	reset_lo(true);
	slot_wait_context_safe = false;
	UT_ASSERT(call_lo());
	UT_ASSERT_EQ(last_ereport_errcode, ERRCODE_PROGRAM_LIMIT_EXCEEDED);
	UT_ASSERT_EQ(lo_deletions + g_bind_emit_calls + lo_cci, 0);
	UT_ASSERT_EQ(cluster_tt_local_binding_count, 0);
	UT_ASSERT_EQ(slot_wait_begins + slot_wait_calls, 0);
	UT_ASSERT_EQ(modifier_enters, modifier_leaves);

	/* Warm binding comes from the original producer, not a previous error. */
	reset_lo(false);
	UT_ASSERT(cluster_tt_local_prepare_canonical_active(GetTopTransactionId(), &binding));
	slot_wait_context_safe = false;
	UT_ASSERT(call_lo());
	UT_ASSERT_EQ(last_ereport_errcode, ERRCODE_UNDEFINED_OBJECT);
	UT_ASSERT_EQ(lo_deletions, 1);
	UT_ASSERT_EQ(lo_unbound + lo_cci, 0);
	UT_ASSERT_EQ(lo_xids, 1);
	UT_ASSERT_EQ(g_bind_emit_calls, 1);
	UT_ASSERT_EQ(cluster_tt_local_binding_count, 1);
	UT_ASSERT_EQ(slot_wait_begins + slot_wait_calls + capacity_waits, 0);
}

static bool
call_lo_mutation(unsigned kind, int flags, int nbytes, int64 offset, int64 len)
{
	LargeObjectDesc desc;
	volatile bool caught = false;

	memset(&desc, 0, sizeof(desc));
	desc.id = 701;
	desc.flags = flags;
	desc.offset = offset;
	PG_TRY();
	{
		if (kind == 0)
			(void)inv_create(701);
		else if (kind == 1)
			lo_write_result = inv_write(&desc, "x", nbytes);
		else
			inv_truncate(&desc, len);
	}
	PG_CATCH();
	{
		caught = true;
	}
	PG_END_TRY();
	return caught;
}

static bool
call_valid_lo_mutation(unsigned kind)
{
	return call_lo_mutation(kind, IFS_WRLOCK, 1, 0, 0);
}

UT_TEST(test_largeobject_mutators_wait_and_bind_before_first_operation_boundary)
{
	for (unsigned kind = 0; kind < 3; kind++) {
		for (unsigned commit = 0; commit < 2; commit++) {
			reset_lo(true);
			slot_wait_commit = commit != 0;
			UT_ASSERT(call_valid_lo_mutation(kind));
			UT_ASSERT_EQ(last_ereport_errcode, ERRCODE_UNDEFINED_OBJECT);
			UT_ASSERT_EQ(lo_mutation_entries, 1);
			UT_ASSERT_EQ(lo_unbound + lo_deletions + lo_cci, 0);
			UT_ASSERT_EQ(lo_xids, 1);
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
}

UT_TEST(test_largeobject_mutator_route_modes_preserve_nonshared_entry)
{
	for (unsigned kind = 0; kind < 3; kind++) {
		for (unsigned mode = 0; mode < 8; mode++) {
			bool bound = (mode & 3) == 3;

			reset_lo(false);
			cluster_enabled = (mode & 1) != 0;
			cluster_shared_catalog = (mode & 2) != 0;
			cluster_shared_config = (mode & 4) != 0;
			lo_expect_binding = bound;
			UT_ASSERT(call_valid_lo_mutation(kind));
			UT_ASSERT_EQ(last_ereport_errcode, ERRCODE_UNDEFINED_OBJECT);
			UT_ASSERT_EQ(lo_mutation_entries, 1);
			UT_ASSERT_EQ(lo_unbound + lo_deletions + lo_cci, 0);
			UT_ASSERT_EQ(lo_xids, bound);
			UT_ASSERT_EQ(g_bind_emit_calls, bound);
			UT_ASSERT_EQ(cluster_tt_local_binding_count, bound);
		}
	}
}

UT_TEST(test_largeobject_mutator_context_child_and_top_checks_precede_xid_and_operation)
{
	static const int errors[] = { ERRCODE_READ_ONLY_SQL_TRANSACTION,
		ERRCODE_INVALID_TRANSACTION_STATE, ERRCODE_READ_ONLY_SQL_TRANSACTION,
		ERRCODE_FEATURE_NOT_SUPPORTED, ERRCODE_INTERNAL_ERROR };

	for (unsigned kind = 0; kind < 3; kind++) {
		for (unsigned guard = 0; guard < lengthof(errors); guard++) {
			reset_lo(false);
			XactReadOnly = guard == 0;
			lo_parallel = guard == 1;
			lo_recovery = guard == 2;
			lo_child = guard == 3;
			if (guard == 4) {
				lo_current = 101;
				lo_top = 100;
			}
			UT_ASSERT(call_valid_lo_mutation(kind));
			UT_ASSERT_EQ(last_ereport_errcode, errors[guard]);
			UT_ASSERT_EQ(lo_xids + lo_mutation_entries + lo_deletions + g_bind_emit_calls + lo_cci, 0);
			UT_ASSERT_EQ(cluster_tt_local_binding_count, 0);
		}
	}
}

UT_TEST(test_largeobject_mutators_refuse_unpublished_canonical_scope_before_operation)
{
	for (unsigned kind = 0; kind < 3; kind++) {
		reset_lo(false);
		cluster_shared_config = true;
		active_owner_start_state = CLUSTER_NORMAL_START_EXISTING_OTHER;
		active_owner_recovered_open = false;
		UT_ASSERT(call_valid_lo_mutation(kind));
		UT_ASSERT_EQ(last_ereport_errcode, ERRCODE_DATA_CORRUPTED);
		UT_ASSERT_EQ(lo_mutation_entries + lo_deletions + g_bind_emit_calls + lo_cci, 0);
		UT_ASSERT_EQ(cluster_tt_local_binding_count, 0);
	}
}

UT_TEST(test_largeobject_mutators_preserve_native_wait_errors_without_operation)
{
	static const int errors[] = { ERRCODE_QUERY_CANCELED, ERRCODE_LOCK_NOT_AVAILABLE,
		ERRCODE_T_R_DEADLOCK_DETECTED, ERRCODE_CLUSTER_RECONFIG_IN_PROGRESS, ERRCODE_IO_ERROR };

	for (unsigned kind = 0; kind < 3; kind++) {
		for (unsigned i = 0; i < lengthof(errors); i++) {
			reset_lo(true);
			slot_wait_error = errors[i];
			UT_ASSERT(call_valid_lo_mutation(kind));
			UT_ASSERT_EQ(last_ereport_errcode, errors[i]);
			UT_ASSERT_EQ(lo_mutation_entries + lo_deletions + g_bind_emit_calls + lo_cci, 0);
			UT_ASSERT_EQ(cluster_tt_local_binding_count, 0);
			UT_ASSERT_EQ(slot_wait_begins, 1);
			UT_ASSERT_EQ(slot_wait_ends, 1);
			UT_ASSERT_EQ(slot_wait_cancels, errors[i] == ERRCODE_QUERY_CANCELED);
			UT_ASSERT_EQ(modifier_enters, modifier_leaves);
		}
	}
}

UT_TEST(test_largeobject_mutators_keep_unsafe_refusal_and_original_warm_binding)
{
	for (unsigned kind = 0; kind < 3; kind++) {
		ClusterCanonicalTxnBinding binding;

		reset_lo(true);
		slot_wait_context_safe = false;
		UT_ASSERT(call_valid_lo_mutation(kind));
		UT_ASSERT_EQ(last_ereport_errcode, ERRCODE_PROGRAM_LIMIT_EXCEEDED);
		UT_ASSERT_EQ(lo_mutation_entries + lo_deletions + g_bind_emit_calls + lo_cci, 0);
		UT_ASSERT_EQ(cluster_tt_local_binding_count, 0);
		UT_ASSERT_EQ(slot_wait_begins + slot_wait_calls, 0);

		reset_lo(false);
		UT_ASSERT(cluster_tt_local_prepare_canonical_active(GetTopTransactionId(), &binding));
		slot_wait_context_safe = false;
		UT_ASSERT(call_valid_lo_mutation(kind));
		UT_ASSERT_EQ(last_ereport_errcode, ERRCODE_UNDEFINED_OBJECT);
		UT_ASSERT_EQ(lo_mutation_entries, 1);
		UT_ASSERT_EQ(lo_unbound + lo_deletions + lo_cci, 0);
		UT_ASSERT_EQ(lo_xids, 1);
		UT_ASSERT_EQ(g_bind_emit_calls, 1);
		UT_ASSERT_EQ(cluster_tt_local_binding_count, 1);
		UT_ASSERT_EQ(slot_wait_begins + slot_wait_calls + capacity_waits, 0);
	}
}

UT_TEST(test_largeobject_mutator_permission_noop_and_size_checks_still_precede_preflight)
{
	static const struct {
		unsigned kind;
		int flags, nbytes;
		int64 offset, len;
		int error;
	} cases[] = {
		{ 1, IFS_RDLOCK, 1, 0, 0, ERRCODE_INSUFFICIENT_PRIVILEGE },
		{ 1, IFS_WRLOCK, 0, 0, 0, 0 },
		{ 1, IFS_WRLOCK, -1, 0, 0, 0 },
		{ 1, IFS_WRLOCK, 1, MAX_LARGE_OBJECT_SIZE, 0, ERRCODE_INVALID_PARAMETER_VALUE },
		{ 2, IFS_RDLOCK, 1, 0, 0, ERRCODE_INSUFFICIENT_PRIVILEGE },
		{ 2, IFS_WRLOCK, 1, 0, -1, ERRCODE_INVALID_PARAMETER_VALUE },
		{ 2, IFS_WRLOCK, 1, 0, MAX_LARGE_OBJECT_SIZE + 1, ERRCODE_INVALID_PARAMETER_VALUE }
	};

	for (unsigned i = 0; i < lengthof(cases); i++) {
		reset_lo(false);
		XactReadOnly = lo_child = true;
		UT_ASSERT_EQ(call_lo_mutation(cases[i].kind, cases[i].flags, cases[i].nbytes,
									 cases[i].offset, cases[i].len), cases[i].error != 0);
		UT_ASSERT_EQ(last_ereport_errcode, cases[i].error);
		if (cases[i].error == 0)
			UT_ASSERT_EQ(lo_write_result, 0);
		UT_ASSERT_EQ(lo_xids + lo_mutation_entries + lo_deletions + g_bind_emit_calls + lo_cci, 0);
		UT_ASSERT_EQ(cluster_tt_local_binding_count, 0);
	}
}

int
main(void)
{
	UT_PLAN(13);
	UT_RUN(test_largeobject_full_selected_segment_binds_after_original_wait_before_deletion_lock);
	UT_RUN(test_largeobject_catalog_route_precedes_original_deletion);
	UT_RUN(test_largeobject_context_child_and_top_rejections_precede_xid_bind_and_deletion);
	UT_RUN(test_largeobject_failed_canonical_publication_stops_before_deletion);
	UT_RUN(test_largeobject_original_native_wait_errors_propagate_without_deletion_or_bind);
	UT_RUN(test_largeobject_original_unsafe_wait_refuses_but_existing_binding_reuses);
	UT_RUN(test_largeobject_mutators_wait_and_bind_before_first_operation_boundary);
	UT_RUN(test_largeobject_mutator_route_modes_preserve_nonshared_entry);
	UT_RUN(test_largeobject_mutator_context_child_and_top_checks_precede_xid_and_operation);
	UT_RUN(test_largeobject_mutators_refuse_unpublished_canonical_scope_before_operation);
	UT_RUN(test_largeobject_mutators_preserve_native_wait_errors_without_operation);
	UT_RUN(test_largeobject_mutators_keep_unsafe_refusal_and_original_warm_binding);
	UT_RUN(test_largeobject_mutator_permission_noop_and_size_checks_still_precede_preflight);
	UT_DONE();
	return ut_failed_count ? 1 : 0;
}
