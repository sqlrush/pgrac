/* Author: SqlRush <sqlrush@gmail.com>
 * Copyright (c) 2026, pgrac contributors.
 * Actual executor/heap -> allocator/local -> original capacity CV -> fresh
 * admission/BIND -> local abort. CTRC proof/floor and cleaner completion are
 * independent leaves, as are inherited native scheduling/admission/IO/WAL.
 * This does not execute the physical pool classifier, release owner or SQL.
 */
#define TT_ACTIVE_OWNER_EXTERNAL_CAPACITY_WAIT
#define EMPTY_UNDO_PREFLIGHT_FIXTURE_MAIN capacity_bind_empty_fixture_main
#include "test_cluster_empty_undo_preflight.c"
#include "cluster/cluster_heap_horizon.h"

bool cluster_undo_cleaner_enabled = true;
int cluster_lmon_main_loop_interval = 2000;
static bool bind_subscribed, bind_recheck_first, bind_drift, bind_no_owner;
static bool bind_same_segment, bind_lease_drift;
static unsigned bind_prepares, bind_cancels, bind_sleeps, bind_probes, bind_supply_after;
static int bind_wait_error;

static bool
undo_cleaner_capacity_context_safe(void)
{
	return cluster_undo_cleaner_capacity_context_safe();
}

static bool
undo_cleaner_capacity_floor(ClusterUndoHorizonFloor *floor)
{
	floor->scn = 1000;
	floor->epoch = cluster_epoch_get_current();
	return true;
}

ClusterHeapFreezeCohortStatus
cluster_undo_capacity_history_observe(ClusterHeapFreezeCohortObservation *out pg_attribute_unused(),
									  bool *producer)
{
	*producer = false; /* No LMON history producer is linked by this fixture. */
	return CLUSTER_HEAP_FREEZE_COHORT_INCOMPLETE;
}

ClusterHeapFreezeCohortStatus
cluster_undo_capacity_history_current(
	const ClusterHeapFreezeCohortObservation *expected pg_attribute_unused())
{
	abort();
}

ClusterCapacityReasonV1
cluster_ctrc_capacity_probe_history_v1(uint32 segment pg_attribute_unused(),
	SCN horizon pg_attribute_unused(), uint64 epoch pg_attribute_unused(),
	const ClusterCtrcTxnKeyV1 *key pg_attribute_unused(),
	ClusterCapacityHistoryPass *pass pg_attribute_unused(),
	ClusterCapacityObservationV1 *out pg_attribute_unused())
{
	abort();
}

ClusterCtrcCapacityProbeResult
cluster_ctrc_capacity_probe_current(uint32 segment, SCN horizon, uint64 epoch,
								   ClusterCtrcTxnKeyV1 *key)
{
	UT_ASSERT_EQ(segment, 1);
	UT_ASSERT_EQ(horizon, 1000);
	UT_ASSERT_EQ(epoch, 17);
	UT_ASSERT_EQ(lock_depth, 0);
	UT_ASSERT_EQ(modifier_enters, modifier_leaves);
	MemSet(key, 0, sizeof(*key));
	return CLUSTER_CTRC_CAPACITY_REFUSE;
}

ClusterCapacityReasonV1
cluster_ctrc_capacity_probe_pool_segment_v1(uint32 segment, SCN horizon, uint64 epoch,
	const ClusterCtrcTxnKeyV1 *prior, ClusterCapacityObservationV1 *out)
{
	ClusterCapacitySegmentIdentityV1 *identity = &out->segment;
	ClusterCtrcTxnKeyV1 zero = { 0 };

	UT_ASSERT_EQ(segment, 1);
	UT_ASSERT_EQ(horizon, 1000);
	UT_ASSERT_EQ(epoch, 17);
	UT_ASSERT_EQ(memcmp(prior, &zero, sizeof(zero)), 0);
	UT_ASSERT_EQ(lock_depth, 0);
	UT_ASSERT_EQ(modifier_enters, modifier_leaves);
	UT_ASSERT_EQ(cluster_tt_local_binding_count, 0);
	UT_ASSERT_EQ(g_bind_emit_calls + g_ctrc_reserve_calls + g_current_acquire_calls, 0);
	/* A broken allocator/controller loop must fail finitely, not hang CI. */
	if (++bind_probes > 10)
		ereport(ERROR, (errcode(ERRCODE_QUERY_CANCELED), errmsg("test recheck bound exceeded")));
	MemSet(out, 0, sizeof(*out));
	if (bind_no_owner)
		return CLUSTER_CAPACITY_UNPROVEN_OR_NO_OWNER;
	if (bind_recheck_first && bind_probes == 1)
		return CLUSTER_CAPACITY_SUPPLY_RECHECK;
	out->scope = CLUSTER_CAPACITY_SCOPE_SEGMENT;
	out->phase = CTRC_ORIGIN_EMPTY;
	out->owner_worker = 1;
	identity->owner_instance = 1;
	identity->segment_id = 2;
	identity->current_segment = 1;
	identity->current_binding_generation = allocator_storage.per_node[0].binding_generation;
	identity->epoch = epoch;
	identity->segment_generation = 7;
	identity->system_identifier = GetSystemIdentifier();
	identity->origin_boot_incarnation = 23;
	identity->formation_epoch = 19;
	identity->admission_record_generation = 29;
	identity->root_id = 31;
	identity->root_generation = 37;
	return CLUSTER_CAPACITY_OWNER_PROGRESS_POSSIBLE;
}

void
ConditionVariablePrepareToSleep(ConditionVariable *cv)
{
	UT_ASSERT(cv == &undo_cleaner_state->capacity_cv);
	UT_ASSERT(!bind_subscribed && undo_cleaner_capacity_context_safe());
	bind_subscribed = true;
	bind_prepares++;
}

bool
ConditionVariableTimedSleep(ConditionVariable *cv, long timeout, uint32 event)
{
	UT_ASSERT(cv == &undo_cleaner_state->capacity_cv);
	UT_ASSERT(bind_subscribed && undo_cleaner_capacity_context_safe());
	UT_ASSERT_EQ(timeout, 2000);
	UT_ASSERT_EQ(event, WAIT_EVENT_CLUSTER_UNDO_CAPACITY_WAIT);
	UT_ASSERT_EQ(cluster_tt_local_binding_count, 0);
	UT_ASSERT_EQ(g_bind_emit_calls + g_ctrc_reserve_calls + g_current_acquire_calls, 0);
	UT_ASSERT(!slot_wait_in_native);
	bind_sleeps++;
	pgstat_report_wait_start(event);
	if (bind_wait_error != 0)
		ereport(ERROR, (errcode(bind_wait_error), errmsg("controlled capacity wait error")));
	if (bind_drift)
		modifier_refused = true;
	if (bind_lease_drift)
		capacity_lease_current = false;
	if (bind_sleeps == bind_supply_after) {
		/* The real release owner is a boundary, not a fake CV grant. Only
		 * its explicit completion makes the original allocator see supply. */
		if (bind_same_segment)
			cluster_tt_slot_free(1, 0);
		else
			cold_capacity_refused = false;
	}
	pgstat_report_wait_end();
	return true; /* Original timed recheck, never an allocator grant. */
}

bool
ConditionVariableCancelSleep(void)
{
	bind_subscribed = false;
	bind_cancels++;
	return true;
}

#include "test_cluster_undo_capacity_bind_wait.inc"

/* The inherited selector controls physical storage. Supply the selected
 * segment's page once, after actual allocation, without resetting a published
 * ACTIVE/terminal page on subsequent admission checks. */
static void
bind_selected_media(void)
{
	uint32 selected = cluster_tt_slot_current_segment(0);

	if (cluster_tt_local_binding_count == 0 || selected == g_canned_block_segment)
		return;
	UT_ASSERT_EQ(selected, 2);
	UT_ASSERT_EQ(g_bind_emit_calls + g_ctrc_reserve_calls + g_current_acquire_calls, 0);
	select_physical_fixture(selected);
	enable_sealed_header_io();
}

static void
bind_reset(bool rowmark, bool active)
{
	empty_reset(rowmark);
	g_modifier_recheck_hook = bind_selected_media;
	bind_lease_drift = false;
	bind_same_segment = active;
	cluster_undo_retention_horizon_enabled = true;
	bind_subscribed = bind_recheck_first = bind_drift = bind_no_owner = false;
	bind_prepares = bind_cancels = bind_sleeps = bind_probes = 0;
	bind_wait_error = 0;
	bind_supply_after = 3;
	capacity_selected_segment = 1;
	if (active) {
		slot_wait_test = slot_wait_terminal_capacity = slot_wait_commit = true;
		fill_selected_segment(CTS_ACTIVE);
	} else {
		cold_fixture = cold_capacity_refused = capacity_test = true;
		fill_selected_segment(CTS_COMMITTED);
	}
	for (unsigned i = 0; i < CLUSTER_UNDO_CLEANER_WORKER_TYPES; i++) {
		undo_cleaner_state->workers[i].status = UNDO_CLEANER_READY;
		undo_cleaner_state->workers[i].pid = 123;
		undo_cleaner_state->workers[i].latch = &MyProc->procLatch;
		undo_cleaner_state->workers[i].incarnation_generation = 83 + i;
	}
}

static void
bind_assert_counts(unsigned waits, unsigned attempts, unsigned repolls, unsigned cancelled, unsigned refused)
{
	ClusterUndoCapacityStats stats;

	UT_ASSERT(cluster_undo_cleaner_capacity_stats_snapshot(&stats));
	UT_ASSERT_EQ(stats.pool_wait_count, waits);
	UT_ASSERT_EQ(stats.wakeup_recheck_count, repolls);
	UT_ASSERT_EQ(stats.cancel_count, cancelled);
	UT_ASSERT_EQ(stats.refused_proof_count + stats.refused_context_count, refused);
	UT_ASSERT_EQ(bind_sleeps, waits);
	UT_ASSERT_EQ(bind_prepares, attempts);
	UT_ASSERT_EQ(bind_cancels, attempts);
	UT_ASSERT(!bind_subscribed);
	UT_ASSERT_EQ(*my_wait_event_info, WAIT_EVENT_CLIENT_READ);
	UT_ASSERT_EQ(lock_depth, 0);
}

static void
bind_abort_and_reuse(const ClusterCanonicalTxnBinding *binding)
{
	ClusterCanonicalTxnBinding repeated;
	void *bindings = cluster_tt_local_bindings;
	unsigned waits = bind_sleeps;

	empty_assert_active(binding);
	UT_ASSERT(empty_preflight(&repeated));
	UT_ASSERT_EQ(memcmp(binding, &repeated, sizeof(repeated)), 0);
	UT_ASSERT_EQ(bind_sleeps, waits);
	UT_ASSERT_EQ(g_bind_emit_calls, 1);
	UT_ASSERT(cluster_tt_local_preabort_durable_finish(100));
	UT_ASSERT(empty_record_abort());
	empty_assert_finished(binding, false);
	free(bindings);
}

UT_TEST(test_actual_capacity_cv_rechecks_then_binds_once_before_empty_caller_work)
{
	for (unsigned rowmark = 0; rowmark < 2; rowmark++) {
		ClusterCanonicalTxnBinding binding;

		bind_reset(rowmark != 0, false);
		if (!empty_preflight(&binding))
			return;
		UT_ASSERT_EQ(binding.segment_id, 2);
		UT_ASSERT_EQ(cold_selection_calls, 4);
		UT_ASSERT_EQ(modifier_enters, 4);
		bind_assert_counts(3, 3, 3, 0, 0);
		bind_abort_and_reuse(&binding);
	}
}

UT_TEST(test_actual_slot_wait_terminal_cv_and_same_segment_bind_compose)
{
	ClusterCanonicalTxnBinding binding;

	bind_reset(false, true);
	if (!empty_preflight(&binding))
		return;
	UT_ASSERT_EQ(slot_wait_calls, 1);
	UT_ASSERT_EQ(slot_wait_begins, 1);
	UT_ASSERT_EQ(slot_wait_ends, 1);
	UT_ASSERT_EQ(binding.segment_id, 1);
	UT_ASSERT_EQ(binding.slot_offset, 0);
	UT_ASSERT_EQ(cold_selection_calls, 0);
	for (unsigned i = 1; i < TT_SLOTS_PER_SEGMENT; i++) {
		UT_ASSERT_EQ(allocator_storage.per_node[0].slots[i].status, CTS_ACTIVE);
		UT_ASSERT_EQ(allocator_storage.per_node[0].slots[i].xid, 200 + i);
	}
	bind_assert_counts(3, 3, 3, 0, 0);
	bind_abort_and_reuse(&binding);
}

UT_TEST(test_actual_supply_recheck_reenters_allocator_without_granting_a_slot)
{
	ClusterCanonicalTxnBinding binding;

	bind_reset(false, false);
	bind_recheck_first = true;
	if (!empty_preflight(&binding))
		return;
	UT_ASSERT_EQ(cold_selection_calls, 5);
	UT_ASSERT_EQ(bind_probes, 4);
	bind_assert_counts(3, 4, 3, 0, 0);
	bind_abort_and_reuse(&binding);
}

UT_TEST(test_actual_cv_errors_leave_no_binding_and_preserve_original_error_counts)
{
	static const int errors[] = { ERRCODE_QUERY_CANCELED, ERRCODE_RAISE_EXCEPTION };

	for (unsigned i = 0; i < lengthof(errors); i++) {
		bind_reset(false, false);
		bind_wait_error = errors[i];
		require_capacity_error(errors[i]);
		bind_assert_counts(1, 1, 0, i == 0 ? 1 : 0, 0);
		list_free(empty_statement.resultRelations);
	}
}

UT_TEST(test_actual_cv_wakeup_cannot_bridge_changed_or_unsafe_admission)
{
	for (unsigned mode = 0; mode < 4; mode++) {
		bind_reset(false, false);
		bind_drift = mode == 0;
		capacity_other_admission = mode == 1;
		bind_no_owner = mode == 2;
		bind_lease_drift = mode == 3;
		if (bind_lease_drift)
			cluster_shared_catalog = cluster_xid_striping = true;
		require_capacity_error(mode == 0 || mode == 3 ? ERRCODE_CLUSTER_RECONFIG_IN_PROGRESS
									   : ERRCODE_PROGRAM_LIMIT_EXCEEDED);
		if (mode == 0 || mode == 3) {
			UT_ASSERT_EQ(modifier_refused, mode == 0);
			if (mode == 3) {
				UT_ASSERT_EQ(capacity_lease_captures, 1);
				UT_ASSERT(!capacity_lease_current);
			}
			bind_assert_counts(1, 1, 1, 0, 0);
		} else {
			UT_ASSERT_EQ(capacity_other_admission, mode == 1);
			bind_assert_counts(0, mode == 1 ? 0 : 1, 0, 0, 1);
		}
		list_free(empty_statement.resultRelations);
	}
}

int
main(void)
{
	UT_PLAN(5);
	UT_RUN(test_actual_capacity_cv_rechecks_then_binds_once_before_empty_caller_work);
	UT_RUN(test_actual_slot_wait_terminal_cv_and_same_segment_bind_compose);
	UT_RUN(test_actual_supply_recheck_reenters_allocator_without_granting_a_slot);
	UT_RUN(test_actual_cv_errors_leave_no_binding_and_preserve_original_error_counts);
	UT_RUN(test_actual_cv_wakeup_cannot_bridge_changed_or_unsafe_admission);
	UT_DONE();
	return ut_failed_count != 0;
}
