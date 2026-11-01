/*-------------------------------------------------------------------------
 * test_cluster_heap_dml_single_vm_authority.c
 *    Original DELETE/TEMP_LOCK brackets prepare VM outside heap content.
 *
 * Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 *-------------------------------------------------------------------------
 */
int dml_single_vm_inherited_main(void);
#define main dml_single_vm_inherited_main
#include "test_cluster_heap_lock_vm_pin.c"
#undef main

typedef int ClusterCurrentMxStampPlan;
static unsigned dml_entries, dml_fallthroughs, dml_cancels, dml_key_frees;
static unsigned dml_native_sites, dml_post, dml_reentry;
static bool dml_temp, dml_initial_alias;
static HeapTuple dml_key = (HeapTuple)(uintptr_t)1;

void
BufferGetTag(Buffer buffer, RelFileLocator *locator, ForkNumber *fork, BlockNumber *block)
{
	UT_ASSERT(buffer == 2 && vm_pinned);
	*locator = relation.rd_locator;
	*fork = VISIBILITYMAP_FORKNUM;
	*block = 0;
}

void
heap_freetuple(HeapTuple value)
{
	UT_ASSERT(value == dml_key);
	dml_key_frees++;
}

void
ClusterObserveBufferBarrierReceipt(ClusterBufferBarrierSiteId site,
		ClusterBufferBarrierPhase phase, RelFileLocator locator,
		ForkNumber fork, BlockNumber block, ClusterBufferBarrierOutcome outcome,
		uint64 proof)
{
	UT_ASSERT_EQ(site, dml_temp ? CLUSTER_BUFFER_BARRIER_SITE_HEAP_UPDATE_PRETOAST_VM
		: CLUSTER_BUFFER_BARRIER_SITE_HEAP_DELETE_VM);
	UT_ASSERT(RelFileLocatorEquals(locator, relation.rd_locator));
	UT_ASSERT(fork == VISIBILITYMAP_FORKNUM && block == 0);
	UT_ASSERT(!vm_locked);
	if (phase == CLUSTER_BUFFER_BARRIER_PHASE_CALLER_POST) {
		UT_ASSERT(!locked);
		UT_ASSERT(outcome == CLUSTER_BUFFER_BARRIER_OUTCOME_POSTCONDITION_OK
			&& proof == CLUSTER_BUFFER_BARRIER_PROOF_CALLER_POST);
		dml_post++;
	} else {
		UT_ASSERT_EQ(phase, CLUSTER_BUFFER_BARRIER_PHASE_REENTRY);
		UT_ASSERT(locked);
		UT_ASSERT(outcome == CLUSTER_BUFFER_BARRIER_OUTCOME_REQUALIFIED
			&& proof == CLUSTER_BUFFER_BARRIER_PROOF_REENTRY);
		dml_reentry++;
	}
}

static void
dml_cancel(ClusterCurrentMxStampPlan *plan)
{
	if (plan != NULL) {
		dml_cancels++;
		*plan = 0;
	}
}

/* Original common MX helper is unchanged; its acquisition/cancellation leaf
 * is controlled here, including the parent-bracket runtime RED. */
static bool
cluster_current_mx_stamp_lock_buffer(Buffer buffer, bool barrier,
		ClusterBufferBarrierSiteId site, bool *replaced,
		ClusterCurrentMxStampPlan *first, ClusterCurrentMxStampPlan *second,
		ResourceXAuxiliaryAcquireContext *context, Buffer *alias)
{
	bool acquired = false;

	UT_ASSERT(barrier);
	UT_ASSERT_EQ(site, dml_temp ? CLUSTER_BUFFER_BARRIER_SITE_HEAP_UPDATE_PRETOAST_VM
		: CLUSTER_BUFFER_BARRIER_SITE_HEAP_DELETE_VM);
	dml_native_sites++;
	PG_TRY();
	{
		acquired = ClusterLockBufferExclusiveAuxiliaryAliasAware(&buffer, alias, NULL, context);
		*replaced = !BufferIsValid(buffer);
	}
	PG_CATCH();
	{
		dml_cancel(first);
		dml_cancel(second);
		PG_RE_THROW();
	}
	PG_END_TRY();
	return acquired;
}

#define cluster_current_mx_stamp_cancel dml_cancel
#define cluster_current_mx_operation_restart(plan) (*(plan) = 0)
#include "test_cluster_heap_vm_dml_writer.inc"

static bool
dml_original_vm_content(void)
{
	return vm_locked;
}

/* Planning before l1/l2 is controlled. The complete original VM bracket,
 * CID/replica-copy unwind, heap acquisition and tuple rebind execute. */
static void
execute_dml(bool pretoast, bool versioned)
{
	Relation relation_arg = &relation;
	Buffer buffer = 1, vmbuffer = vm, vmbuffer_new = InvalidBuffer;
	BlockNumber block = 17;
	TransactionId creation_xmin = creator;
	HeapTupleData tp, oldtup;
	ItemPointerData target;
	ItemPointer tid = &target, otid = &target;
	Page page;
	ItemId lp;
	bool vm_locked = false, cluster_page_versioned = versioned;
	bool iscombo = false, old_key_copied = false, cluster_current_mx_recomposed = true;
	HeapTuple old_key_tuple = NULL;
	CommandId cid = 1, pgrac_entry_cid = 1;
	ClusterCurrentMxStampPlan cluster_current_mx_plan = 0;
	ClusterCurrentMxStampPlan cluster_current_mx_temp_lock_plan = 0;
	int cluster_current_mx_operation = 0;
	ResourceXAuxiliaryAcquireContext cluster_vm_context = { 0 };
	ClusterHeapVmMutationState cluster_vm_state = { 0 };

	(void)cluster_vm_state;
	(void)cluster_page_versioned;
	ItemPointerSet(tid, block, 2);
	acquire(&vmbuffer);
	tp = oldtup = tuple;
	if (pretoast && dml_initial_alias) {
		vmbuffer_new = vmbuffer;
		extra_vm_pins++;
	}
#define relation relation_arg
l1:
l2:
	cluster_heap_rebind_locked_tuple(relation, buffer, pretoast ? &oldtup : &tp, &creation_xmin);
	if (dml_entries > 0) {
		UT_ASSERT(cid == pgrac_entry_cid && !iscombo);
		if (pretoast) {
			UT_ASSERT_EQ(cluster_current_mx_temp_lock_plan, 0);
			UT_ASSERT_EQ(cluster_current_mx_operation, 0);
			UT_ASSERT_EQ(vmbuffer_new, InvalidBuffer);
		} else {
			UT_ASSERT_EQ(cluster_current_mx_plan, 0);
			UT_ASSERT(old_key_tuple == NULL && !old_key_copied);
		}
	}
	if (++dml_entries > 32)
		elog(ERROR, "fixture requalification made no progress");
	/* Original planner/replica-identity producer is outside this fixture. */
	cluster_current_mx_plan = cluster_current_mx_temp_lock_plan = 1;
	cluster_current_mx_operation = 1;
	cid = 2;
	iscombo = true;
	if (!pretoast) {
		old_key_tuple = dml_key;
		old_key_copied = true;
	}
	page = BufferGetPage(buffer);
	if (pretoast) {
#include "test_cluster_heap_vm_pretoast.inc"
	} else {
#include "test_cluster_heap_vm_delete.inc"
	}
#undef relation
	UT_ASSERT_EQ(vm_locked, dml_original_vm_content());
	vm = vmbuffer;
	tuple = pretoast ? oldtup : tp;
	dml_fallthroughs++;
}
#undef cluster_current_mx_stamp_cancel
#undef cluster_current_mx_operation_restart

static void
prepare_dml(bool pretoast, bool alias)
{
	reset(true, true);
	vm_forbid_nested = true;
	dml_temp = pretoast;
	dml_initial_alias = alias;
	dml_entries = dml_fallthroughs = dml_cancels = dml_key_frees = 0;
	dml_native_sites = dml_post = dml_reentry = 0;
}

static bool
try_dml(bool versioned)
{
	volatile bool done = false;

	PG_TRY();
	{
		execute_dml(dml_temp, versioned);
		done = true;
	}
	PG_CATCH();
	{
		/* Inspect original ownership; no simulated ResourceOwner recovery. */
	}
	PG_END_TRY();
	return done;
}

static void
check_dml_no_apply(void)
{
	UT_ASSERT_EQ(dml_fallthroughs, 0);
	UT_ASSERT(!vm_locked);
	UT_ASSERT_EQ(waits_with_vm, 0);
}

UT_TEST(single_dml_cold_vm_prepares_outside_heap_and_requalifies)
{
	for (int pre = 0; pre < 2; pre++) {
		prepare_dml(pre, false);
		UT_ASSERT(try_dml(true));
		check_current();
		UT_ASSERT(vm_locked && vm_pinned);
		UT_ASSERT_EQ(dml_entries, 2);
		UT_ASSERT_EQ(dml_fallthroughs, 1);
		UT_ASSERT_EQ(nested_calls + dml_native_sites, 0);
		UT_ASSERT_EQ(warm_calls, 1);
		UT_ASSERT_EQ(vm_captures, 1);
		UT_ASSERT_EQ(vm_resumes, 1);
		UT_ASSERT(dml_post == 1 && dml_reentry == 1);
		UT_ASSERT_EQ(dml_key_frees, pre ? 0 : 1);
	}
}
UT_TEST(single_dml_retains_auxiliary_age_across_clean_refusals)
{
	for (int pre = 0; pre < 2; pre++)
		for (int replace = 0; replace < 2; replace++) {
			prepare_dml(pre, false);
			warm_refusals = 4;
			warm_replaces_pin = replace;
			UT_ASSERT(try_dml(true));
			check_current();
			UT_ASSERT_EQ(warm_calls, 5);
			UT_ASSERT_EQ(context_starts, 1);
			UT_ASSERT_EQ(vm_captures, 1);
			UT_ASSERT_EQ(dml_fallthroughs, 1);
			UT_ASSERT(dml_post == 5 && dml_reentry == 5);
		}
}
UT_TEST(single_dml_retries_busy_original_writer_before_apply)
{
	for (int pre = 0; pre < 2; pre++) {
		prepare_dml(pre, false);
		vm_resume_refusals = 3;
		UT_ASSERT(try_dml(true));
		check_current();
		UT_ASSERT_EQ(warm_calls, 4);
		UT_ASSERT_EQ(vm_resumes, 4);
		UT_ASSERT_EQ(nested_calls, 0);
		UT_ASSERT_EQ(dml_entries, 5);
		UT_ASSERT_EQ(dml_fallthroughs, 1);
	}
}
UT_TEST(single_dml_rejects_original_writer_changed_during_heap_wait)
{
	for (int pre = 0; pre < 2; pre++) {
		prepare_dml(pre, false);
		vm_invalidate_on_relock = true;
		UT_ASSERT(try_dml(true));
		check_current();
		UT_ASSERT_EQ(warm_calls, 2);
		UT_ASSERT_EQ(vm_resumes, 2);
		UT_ASSERT_EQ(dml_entries, 3);
		UT_ASSERT_EQ(dml_fallthroughs, 1);
	}
}
UT_TEST(single_dml_missing_original_writer_cannot_publish)
{
	for (int pre = 0; pre < 2; pre++) {
		prepare_dml(pre, false);
		vm_missing_writer = true;
		UT_ASSERT(!try_dml(true));
		check_dml_no_apply();
		UT_ASSERT(!locked);
		UT_ASSERT_EQ(vm_captures, 1);
	}
}
UT_TEST(single_dml_capture_error_unwinds_vm_content)
{
	for (int pre = 0; pre < 2; pre++) {
		prepare_dml(pre, false);
		vm_writer_error = 1;
		UT_ASSERT(!try_dml(true));
		check_dml_no_apply();
		UT_ASSERT(!locked);
		UT_ASSERT_EQ(vm_captures, 1);
	}
}
UT_TEST(single_dml_resume_error_preserves_original_plan_cancellation)
{
	for (int pre = 0; pre < 2; pre++) {
		prepare_dml(pre, false);
		vm_writer_error = 2;
		UT_ASSERT(!try_dml(true));
		check_dml_no_apply();
		UT_ASSERT(locked);
		UT_ASSERT_EQ(vm_resumes, 1);
		UT_ASSERT_EQ(dml_cancels, pre ? 2 : 3);
	}
}
UT_TEST(single_dml_auxiliary_error_owns_no_content_locks)
{
	for (int pre = 0; pre < 2; pre++) {
		prepare_dml(pre, false);
		warm_error = true;
		UT_ASSERT(!try_dml(true));
		check_dml_no_apply();
		UT_ASSERT(!locked);
		UT_ASSERT_EQ(warm_calls, 1);
	}
}
UT_TEST(single_dml_rechecks_pd_after_unlocked_vm_wait)
{
	for (int pre = 0; pre < 2; pre++) {
		prepare_dml(pre, false);
		warm_change = 2;
		UT_ASSERT(try_dml(true));
		check_current();
		UT_ASSERT(!vm_locked && !vm_pinned && vm == InvalidBuffer);
		UT_ASSERT_EQ(warm_calls, 1);
		UT_ASSERT_EQ(vm_resumes, 0);
	}
}
UT_TEST(single_dml_creator_change_or_pruned_tuple_cannot_reach_apply)
{
	for (int pre = 0; pre < 2; pre++)
		for (int change = 3; change <= 4; change++) {
			prepare_dml(pre, false);
			warm_change = change;
			UT_ASSERT(!try_dml(true));
			check_dml_no_apply();
			UT_ASSERT(locked);
			UT_ASSERT_EQ(warm_calls, 1);
		}
}
UT_TEST(single_dml_cancel_after_vm_warm_retains_no_obstructing_pin)
{
	for (int pre = 0; pre < 2; pre++) {
		prepare_dml(pre, pre);
		vm_cancel_after_warm = true;
		UT_ASSERT(!try_dml(true));
		check_dml_no_apply();
		UT_ASSERT(!locked && !vm_pinned && extra_vm_pins == 0);
		UT_ASSERT_EQ(vm_interrupts, 1);
	}
}
UT_TEST(pretoast_declared_equal_alias_is_dropped_before_heap_wait)
{
	prepare_dml(true, true);
	UT_ASSERT(try_dml(true));
	check_current();
	UT_ASSERT(vm_locked && vm_pinned);
	UT_ASSERT_EQ(extra_vm_pins, 0);
	UT_ASSERT_EQ(warm_calls, 1);
	UT_ASSERT_EQ(vm_resumes, 1);
}
UT_TEST(pretoast_reobserve_invalidates_all_declared_equal_aliases)
{
	prepare_dml(true, true);
	warm_refusals = 1;
	warm_replaces_pin = true;
	UT_ASSERT(try_dml(true));
	check_current();
	UT_ASSERT(vm_locked && vm_pinned);
	UT_ASSERT_EQ(extra_vm_pins, 0);
	UT_ASSERT_EQ(warm_calls, 2);
	UT_ASSERT_EQ(context_starts, 1);
}
UT_TEST(single_dml_native_keeps_original_site_and_auxiliary_entry)
{
	for (int pre = 0; pre < 2; pre++) {
		prepare_dml(pre, false);
		vm_forbid_nested = false;
		UT_ASSERT(try_dml(false));
		check_current();
		UT_ASSERT(vm_locked);
		UT_ASSERT_EQ(dml_native_sites, 1);
		UT_ASSERT_EQ(nested_calls, 1);
		UT_ASSERT_EQ(warm_calls + vm_captures + vm_resumes, 0);
	}
}
UT_TEST(single_dml_nonvisible_heap_needs_no_vm_writer)
{
	for (int pre = 0; pre < 2; pre++) {
		prepare_dml(pre, false);
		PageClearAllVisible(pages[0].data);
		PageClearAllVisible(incoming.data);
		UT_ASSERT(try_dml(true));
		check_current();
		UT_ASSERT(!vm_locked && !vm_pinned);
		UT_ASSERT_EQ(nested_calls + warm_calls + vm_captures + vm_resumes, 0);
	}
}

int
main(void)
{
	UT_PLAN(15);
	UT_RUN(single_dml_cold_vm_prepares_outside_heap_and_requalifies);
	UT_RUN(single_dml_retains_auxiliary_age_across_clean_refusals);
	UT_RUN(single_dml_retries_busy_original_writer_before_apply);
	UT_RUN(single_dml_rejects_original_writer_changed_during_heap_wait);
	UT_RUN(single_dml_missing_original_writer_cannot_publish);
	UT_RUN(single_dml_capture_error_unwinds_vm_content);
	UT_RUN(single_dml_resume_error_preserves_original_plan_cancellation);
	UT_RUN(single_dml_auxiliary_error_owns_no_content_locks);
	UT_RUN(single_dml_rechecks_pd_after_unlocked_vm_wait);
	UT_RUN(single_dml_creator_change_or_pruned_tuple_cannot_reach_apply);
	UT_RUN(single_dml_cancel_after_vm_warm_retains_no_obstructing_pin);
	UT_RUN(pretoast_declared_equal_alias_is_dropped_before_heap_wait);
	UT_RUN(pretoast_reobserve_invalidates_all_declared_equal_aliases);
	UT_RUN(single_dml_native_keeps_original_site_and_auxiliary_entry);
	UT_RUN(single_dml_nonvisible_heap_needs_no_vm_writer);
	UT_DONE();
	return ut_failed_count != 0;
}
