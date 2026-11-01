/*-------------------------------------------------------------------------
 * test_cluster_heap_tuple_vm_authority.c
 *    Row/chain locks prepare VM authority outside heap content locks.
 *
 * Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 *-------------------------------------------------------------------------
 */
int tuple_vm_inherited_main(void);
#define main tuple_vm_inherited_main
#include "test_cluster_heap_lock_vm_pin.c"
#undef main

/* The row bracket's MX planning/cancellation leaves are controlled. The
 * original tuple acquisition/rebind and complete VM bracket execute below. */
typedef int ClusterCurrentMxStampPlan;
static unsigned row_entries, row_fallthroughs, row_cancels;

static void
row_cancel(ClusterCurrentMxStampPlan *plan)
{
	row_cancels++;
	*plan = 0;
}

#define cluster_current_mx_stamp_cancel row_cancel
#include "test_cluster_heap_vm_tuple_writer.inc"

/* Parent compatibility: execute its unchanged bracket, with the ordinary
 * acquisition as a controlled leaf. This is not the full parent MX helper. */
static bool pg_attribute_unused()
cluster_current_mx_stamp_lock_buffer(Buffer target, bool barrier,
	ClusterBufferBarrierSiteId site, bool *replaced,
	ClusterCurrentMxStampPlan *first, ClusterCurrentMxStampPlan *second,
	ResourceXAuxiliaryAcquireContext *context, Buffer *alias)
{
	bool acquired = false;
	PG_TRY();
	{
		acquired = ClusterLockBufferExclusiveAuxiliaryAliasAware(&target, alias, NULL, context);
		*replaced = !BufferIsValid(target);
	}
	PG_CATCH();
	{
		row_cancel(first);
		PG_RE_THROW();
	}
	PG_END_TRY();
	return acquired;
}

#define cluster_current_mx_operation_restart(plan) (*(plan) = 0)
static void
row_vm(bool versioned)
{
	Relation relation_arg = &relation;
	Buffer selected = 1, *buffer = &selected, vmbuffer = vm;
	BlockNumber block = 17;
	TransactionId creation_xmin = creator;
	HeapTupleData data;
	HeapTuple tuple_arg = &data;
	ItemPointerData tid_data;
	ItemPointer tid = &tid_data;
	Page page;
	ItemId lp;
	bool vm_locked = false, cluster_page_versioned = versioned;
	ResourceXAuxiliaryAcquireContext cluster_vm_context = { 0 };
	ClusterHeapVmMutationState cluster_vm_state = { 0 };
	ClusterCurrentMxStampPlan cluster_current_mx_lock_plan = 0;
	int cluster_current_mx_operation = 0;
	bool cluster_did_lock_stamp = false, cluster_lock_needs_itl_slot = false;
	bool cluster_lock_undo_ready = false;
	int cluster_lock_slot_idx = CLUSTER_ITL_SLOT_UNALLOCATED;

	(void)cluster_vm_state;
	ItemPointerSet(tid, block, 2);
	acquire(&vmbuffer);
	data = tuple;
#define relation relation_arg
#define tuple tuple_arg
l3:
	cluster_heap_rebind_locked_tuple(relation, *buffer, tuple, &creation_xmin);
	if (row_entries > 0) {
		UT_ASSERT_EQ(cluster_current_mx_lock_plan, 0);
		UT_ASSERT_EQ(cluster_current_mx_operation, 0);
		UT_ASSERT(!cluster_did_lock_stamp && !cluster_lock_needs_itl_slot && !cluster_lock_undo_ready);
		UT_ASSERT_EQ(cluster_lock_slot_idx, CLUSTER_ITL_SLOT_UNALLOCATED);
		UT_ASSERT(tuple->t_data == (HeapTupleHeader)PageGetItem(pages[0].data, PageGetItemId(pages[0].data, 2)));
	}
	/* Original l3 reclassification/planning lies outside this fixture. */
	row_entries++;
	cluster_current_mx_lock_plan = cluster_current_mx_operation = 1;
	cluster_did_lock_stamp = cluster_lock_needs_itl_slot = cluster_lock_undo_ready = true;
	cluster_lock_slot_idx = 2;
	page = BufferGetPage(*buffer);
#include "test_cluster_heap_vm_tuple_lock.inc"
#undef relation
#undef tuple
	(void)vm_locked;
	vm = vmbuffer;
	tuple = data;
	row_fallthroughs++;
}
#undef cluster_current_mx_stamp_cancel
#undef cluster_current_mx_operation_restart

static void
prepare_entry(void)
{
	reset(true, true);
	vm_forbid_nested = true;
	row_entries = row_fallthroughs = row_cancels = 0;
}

/* entry0=row, entry1=no-ITL chain, entry2=ITL chain. */
static bool
try_entry(int entry, bool versioned)
{
	volatile bool completed = false;
	PG_TRY();
	{
		if (entry == 0)
			row_vm(versioned);
		else
			chain_vm(entry == 2);
		completed = true;
	}
	PG_CATCH();
	{
		/* Observe only; ResourceOwner/error recovery is not simulated. */
	}
	PG_END_TRY();
	return completed;
}

static void
check_no_apply(void)
{
	UT_ASSERT_EQ(row_fallthroughs + chain_fallthroughs, 0);
	UT_ASSERT(!vm_locked);
	UT_ASSERT_EQ(waits_with_vm, 0);
}

UT_TEST(tuple_owners_prepare_cold_vm_without_nested_acquisition)
{
	for (int entry = 0; entry < 3; entry++) {
		prepare_entry();
		UT_ASSERT(try_entry(entry, true));
		check_current();
		UT_ASSERT(vm_locked && vm_pinned);
		UT_ASSERT_EQ(nested_calls, 0);
		UT_ASSERT_EQ(warm_calls, 1);
		UT_ASSERT_EQ(vm_captures, 1);
		UT_ASSERT_EQ(vm_resumes, 1);
		UT_ASSERT_EQ(waits, 2);
		UT_ASSERT_EQ(row_fallthroughs + chain_fallthroughs, 1);
	}
}
UT_TEST(tuple_owners_keep_original_cursor_through_clean_refusals)
{
	for (int entry = 0; entry < 3; entry++)
		for (int replacement = 0; replacement < 2; replacement++) {
			prepare_entry();
			warm_refusals = 4;
			warm_replaces_pin = replacement != 0;
			UT_ASSERT(try_entry(entry, true));
			check_current();
			UT_ASSERT_EQ(warm_calls, 5);
			UT_ASSERT_EQ(context_starts, 1);
			UT_ASSERT_EQ(vm_captures, 1);
			UT_ASSERT_EQ(vm_resumes, 1);
			UT_ASSERT_EQ(nested_calls, 0);
		}
}
UT_TEST(tuple_owners_retry_busy_original_writer_without_apply)
{
	for (int entry = 0; entry < 3; entry++) {
		prepare_entry();
		vm_resume_refusals = 3;
		UT_ASSERT(try_entry(entry, true));
		check_current();
		UT_ASSERT_EQ(warm_calls, 4);
		UT_ASSERT_EQ(vm_captures, 4);
		UT_ASSERT_EQ(vm_resumes, 4);
		UT_ASSERT_EQ(nested_calls, 0);
		UT_ASSERT_EQ(row_fallthroughs + chain_fallthroughs, 1);
	}
}
UT_TEST(tuple_owners_reject_writer_changed_while_heap_reacquires)
{
	for (int entry = 0; entry < 3; entry++) {
		prepare_entry();
		vm_invalidate_on_relock = true;
		UT_ASSERT(try_entry(entry, true));
		check_current();
		UT_ASSERT_EQ(warm_calls, 2);
		UT_ASSERT_EQ(vm_captures, 2);
		UT_ASSERT_EQ(vm_resumes, 2);
		UT_ASSERT_EQ(waits, 3);
		UT_ASSERT_EQ(row_fallthroughs + chain_fallthroughs, 1);
	}
}
UT_TEST(tuple_owners_missing_original_writer_fails_before_apply)
{
	for (int entry = 0; entry < 3; entry++) {
		prepare_entry();
		vm_missing_writer = true;
		UT_ASSERT(!try_entry(entry, true));
		check_no_apply();
		UT_ASSERT(!locked);
		UT_ASSERT_EQ(vm_captures, 1);
	}
}
UT_TEST(tuple_owners_capture_error_unlocks_only_vm_scope)
{
	for (int entry = 0; entry < 3; entry++) {
		prepare_entry();
		vm_writer_error = 1;
		UT_ASSERT(!try_entry(entry, true));
		check_no_apply();
		UT_ASSERT(!locked);
		UT_ASSERT_EQ(vm_captures, 1);
	}
}
UT_TEST(tuple_owners_resume_error_cannot_reach_apply)
{
	for (int entry = 0; entry < 3; entry++) {
		prepare_entry();
		vm_writer_error = 2;
		UT_ASSERT(!try_entry(entry, true));
		check_no_apply();
		UT_ASSERT(locked);
		UT_ASSERT_EQ(vm_resumes, 1);
		if (entry == 0)
			UT_ASSERT_EQ(row_cancels, 2); /* Retry plus original ERROR guarantee. */
	}
}
UT_TEST(tuple_owners_auxiliary_error_retains_no_content_locks)
{
	for (int entry = 0; entry < 3; entry++) {
		prepare_entry();
		warm_error = true;
		UT_ASSERT(!try_entry(entry, true));
		check_no_apply();
		UT_ASSERT(!locked);
		UT_ASSERT_EQ(warm_calls, 1);
	}
}
UT_TEST(tuple_owners_recheck_af_after_unlocked_acquisition)
{
	for (int entry = 0; entry < 3; entry++) {
		prepare_entry();
		warm_change = 1;
		UT_ASSERT(try_entry(entry, true));
		check_current();
		UT_ASSERT(!vm_locked);
		UT_ASSERT_EQ(warm_calls, 1);
		UT_ASSERT_EQ(vm_resumes, 0);
	}
}
UT_TEST(tuple_owners_discard_vm_pin_when_current_loses_pd_visible)
{
	for (int entry = 0; entry < 3; entry++) {
		prepare_entry();
		warm_change = 2;
		UT_ASSERT(try_entry(entry, true));
		check_current();
		UT_ASSERT(!vm_locked && !vm_pinned && vm == InvalidBuffer);
		UT_ASSERT_EQ(warm_calls, 1);
	}
}
UT_TEST(tuple_owners_reject_changed_creator_or_pruned_tuple_before_apply)
{
	for (int entry = 0; entry < 3; entry++)
		for (int change = 3; change <= 4; change++) {
			prepare_entry();
			warm_change = change;
			UT_ASSERT(!try_entry(entry, true));
			check_no_apply();
			UT_ASSERT(locked);
			UT_ASSERT_EQ(warm_calls, 1);
		}
}
UT_TEST(tuple_owners_proved_af_zero_needs_no_vm_writer)
{
	for (int entry = 0; entry < 3; entry++) {
		prepare_entry();
		needs_clear = false;
		UT_ASSERT(try_entry(entry, true));
		check_current();
		UT_ASSERT(!vm_locked && vm_pinned);
		UT_ASSERT_EQ(waits, 1);
		UT_ASSERT_EQ(nested_calls + warm_calls + vm_captures + vm_resumes, 0);
	}
}
UT_TEST(chain_cancel_after_vm_prepare_retains_no_obstructing_pin)
{
	for (int entry = 1; entry < 3; entry++) {
		prepare_entry();
		vm_cancel_after_warm = true;
		UT_ASSERT(!try_entry(entry, true));
		check_no_apply();
		UT_ASSERT(!locked && !vm_pinned);
		UT_ASSERT_EQ(vm_interrupts, 1);
		UT_ASSERT_EQ(vm_captures, 1);
	}
}
UT_TEST(tuple_native_row_keeps_original_auxiliary_branch)
{
	prepare_entry();
	vm_forbid_nested = false;
	UT_ASSERT(try_entry(0, false));
	check_current();
	UT_ASSERT(vm_locked);
	UT_ASSERT_EQ(nested_calls, 1);
	UT_ASSERT_EQ(warm_calls + vm_captures + vm_resumes, 0);
}

int
main(void)
{
	UT_PLAN(14);
	UT_RUN(tuple_owners_prepare_cold_vm_without_nested_acquisition);
	UT_RUN(tuple_owners_keep_original_cursor_through_clean_refusals);
	UT_RUN(tuple_owners_retry_busy_original_writer_without_apply);
	UT_RUN(tuple_owners_reject_writer_changed_while_heap_reacquires);
	UT_RUN(tuple_owners_missing_original_writer_fails_before_apply);
	UT_RUN(tuple_owners_capture_error_unlocks_only_vm_scope);
	UT_RUN(tuple_owners_resume_error_cannot_reach_apply);
	UT_RUN(tuple_owners_auxiliary_error_retains_no_content_locks);
	UT_RUN(tuple_owners_recheck_af_after_unlocked_acquisition);
	UT_RUN(tuple_owners_discard_vm_pin_when_current_loses_pd_visible);
	UT_RUN(tuple_owners_reject_changed_creator_or_pruned_tuple_before_apply);
	UT_RUN(tuple_owners_proved_af_zero_needs_no_vm_writer);
	UT_RUN(chain_cancel_after_vm_prepare_retains_no_obstructing_pin);
	UT_RUN(tuple_native_row_keeps_original_auxiliary_branch);
	UT_DONE();
	return ut_failed_count != 0;
}
