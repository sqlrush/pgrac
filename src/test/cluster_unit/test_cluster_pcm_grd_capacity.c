/*-------------------------------------------------------------------------
 *
 * test_cluster_pcm_grd_capacity.c
 *    GRD exhaustion through the real PCM allocator, waiter and GCS consumer.
 *
 * The registry and process environment are controlled inputs. Retirement,
 * reason classification, waiting, deadline accounting and PI retention use
 * the actual product implementations. A returning CV boundary schedules one
 * explicit producer action; it never supplies acquisition authority.
 *
 * Portions Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *    src/test/cluster_unit/test_cluster_pcm_grd_capacity.c
 *
 *-------------------------------------------------------------------------
 */
#define FAKE_PCM_MAX_ENTRIES 2048
#define FAKE_PCM_HEADER_BYTES (128 * 1024 * 1024)
int pcm_capacity_fixture_main(void);
#define main pcm_capacity_fixture_main
#define ConditionVariableSleep capacity_fixture_sleep
#define ProcessInterrupts capacity_fixture_interrupts
#include "test_cluster_pcm_lock.c"
#undef main
#undef ConditionVariableSleep
#undef ProcessInterrupts

static void (*capacity_sleep_action)(void);
static BufferTag capacity_cold_tag;
static BufferTag capacity_target;
static uint64 capacity_elapsed_us;
static unsigned capacity_sleep_calls;

void ConditionVariableSleep(ConditionVariable *cv, uint32 wait_event_info);
void ProcessInterrupts(void);

void
ConditionVariableSleep(ConditionVariable *cv, uint32 wait_event_info)
{
	UT_ASSERT_EQ(fake_lwlock_depth, 0);
	capacity_sleep_calls++;
	capacity_fixture_sleep(cv, wait_event_info);
	fake_pcm_clock_us += capacity_elapsed_us;
	if (capacity_sleep_action != NULL)
		capacity_sleep_action();
}

void
ProcessInterrupts(void)
{
	if (InterruptPending)
		ereport(ERROR, (errcode(ERRCODE_QUERY_CANCELED), errmsg("capacity caller cancelled")));
}

#include "test_cluster_pcm_capacity_helpers.inc"

static ResourceXBootstrapRoundAction
capacity_step(BufferTag tag, uint64 deadline)
{
	ResourceXAssertion assertion;
	ResourceXDecodedFrame dispatch;
	ResourceXAcquisitionRef terminal;

	UT_ASSERT(resource_x_assertion_init(&tag, 0, &assertion));
	return cluster_pcm_lock_resource_x_bootstrap_round_step_exact(
		&assertion, 0, 17, 31, 77, 51, 61, deadline, 900,
		fake_pcm_clock_us, 50, false, 0, &dispatch, &terminal);
}

/* Choose a registry position, not a block-number surrogate for that position. */
static void
capacity_fill(bool all_active, bool one_cold)
{
	bool selected = false;

	reset_fake_pcm_runtime(FAKE_PCM_MAX_ENTRIES);
	cluster_node_id = fake_gcs_master_node = 0;
	ut_master_session = 31;
	master_durable_admission = true;
	capacity_sleep_action = NULL;
	capacity_elapsed_us = 0;
	capacity_sleep_calls = 0;
	fake_pcm_clock_us = 100;
	capacity_target = make_tag(20000);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_gate_bind_formation_exact(17),
				 RESOURCE_X_APPLY_APPLIED);
	for (uint32 index = 0; index < FAKE_PCM_MAX_ENTRIES; index++) {
		BufferTag tag = make_tag(10000 + index);
		PcmEntryRef ref;
		PcmEntryAcquireResult acquired;
		bool cold;

		UT_ASSERT(pcm_entry_ref_acquire(&tag, true, &ref, &acquired));
		cold = one_cold && !selected && ref.registry_slot == PCM_GRD_SYNC_RECLAIM_BUDGET;
		if (cold) {
			capacity_cold_tag = tag;
			selected = true;
		}
		pcm_entry_ref_release(&ref);
		if (all_active && !cold)
			UT_ASSERT_EQ(cluster_pcm_lock_apply_gcs_transition_result(tag, PCM_TRANS_N_TO_X, 0),
						 PCM_GCS_TRANSITION_APPLIED);
	}
	UT_ASSERT(!one_cold || selected);
}

/* One pass through the verbatim GCS consumer. A continue means the caller
 * must repeat its original buffer/KO/round checks; this wrapper grants none. */
static ResourceXApplyResult
capacity_consume(ResourceXBootstrapRoundAction action, ResourceXCallerWitness *caller_witness,
				 uint64 *absolute_deadline_us_io)
{
	BufferTag resource = capacity_target;
	ResourceXGateSnapshot gate;
	ClusterSemanticAdmissionToken admission = { 0 };
	uint64 absolute_deadline_us = *absolute_deadline_us_io;
	uint64 capacity_membership_generation = fake_membership_cut_generation;
	uint64 master_session = ut_master_session;
	int32 master_node = fake_gcs_master_node;
	ResourceXApplyResult wait_result;
	ResourceXApplyResult result = RESOURCE_X_APPLY_APPLIED;
	const char *diagnostic_stage = NULL;
	struct { uint64 threshold_us; } wait_diagnostic = { 0 };

	UT_ASSERT(cluster_pcm_lock_resource_x_gate_snapshot(&gate));
	admission.entered = true;
	for (unsigned pass = 0; pass < 1; pass++) {
#include "test_cluster_pcm_capacity_consumer.inc"
	}
	(void)diagnostic_stage;
	(void)wait_diagnostic;
	return result;
}

static void capacity_tick(void) { cluster_pcm_lock_lmon_reclaim_tick(); }
static void capacity_change_member(void) {
	fake_membership_cut_generation += 2;
	cluster_pcm_lock_lmon_reclaim_tick();
}
static void capacity_change_gate(void) {
	ResourceXGateSnapshot gate;
	UT_ASSERT(cluster_pcm_lock_resource_x_gate_snapshot(&gate));
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_gate_fail_closed_exact(&gate), RESOURCE_X_APPLY_APPLIED);
}
static void capacity_cancel(void) { InterruptPending = true; }
static void capacity_change_session(void) { ut_master_session++; }
static void capacity_close_admission(void) { master_durable_admission = false; }
static void capacity_close_ko(void) { object_admission_allowed = false; }
static void capacity_release_holder(void) {
	cluster_pcm_lock_release(make_tag(10000));
	cluster_pcm_lock_lmon_reclaim_tick();
}

UT_TEST(cold_entries_reclaim_within_original_sync_budget)
{
	PcmGrdLifecycleStats stats;
	capacity_fill(false, false);
	UT_ASSERT_EQ(capacity_step(capacity_target, 1000), RESOURCE_X_BOOTSTRAP_ROUND_DISPATCH_REQUEST);
	cluster_pcm_grd_lifecycle_stats_snapshot(&stats);
	UT_ASSERT_EQ(stats.reclaim_success_count, PCM_GRD_SYNC_RECLAIM_BUDGET);
	UT_ASSERT_EQ(stats.capacity_wait_count, 0);
}

UT_TEST(cold_entry_at_registry_64_waits_for_original_lmon)
{
	ResourceXCallerWitness caller = { 0 };
	uint64 deadline = 1000;
	PcmGrdLifecycleStats stats;
	ResourceXBootstrapRoundAction action;
	PcmRetireRefusal refusal;
	PcmEntryRef ref;
	PcmEntryAcquireResult acquired;
	uint64 generation;

	capacity_fill(true, true);
	UT_ASSERT(pcm_entry_ref_acquire(&capacity_cold_tag, false, &ref, &acquired));
	generation = ref.binding_generation;
	UT_ASSERT_EQ(ref.registry_slot, PCM_GRD_SYNC_RECLAIM_BUDGET);
	pcm_entry_ref_release(&ref);
	UT_ASSERT(pcm_entry_retire_classify_exact(&capacity_cold_tag, generation, &refusal));
	action = capacity_step(capacity_target, deadline);
	UT_ASSERT_EQ(action, RESOURCE_X_BOOTSTRAP_ROUND_NO_CAPACITY);
	capacity_sleep_action = capacity_tick;
	capacity_elapsed_us = 100000; /* Deliberately exceeds the original caller budget. */
	UT_ASSERT_EQ(capacity_consume(action, &caller, &deadline), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(deadline, 101000);
	UT_ASSERT_EQ(deadline - fake_pcm_clock_us, 900);
	cluster_pcm_grd_lifecycle_stats_snapshot(&stats);
	UT_ASSERT_EQ(stats.capacity_wait_count, 1);
	UT_ASSERT_EQ(stats.reclaim_success_count, 1);
	UT_ASSERT_EQ(fake_cv_sleep_wait_event, WAIT_EVENT_PCM_GRD_CAPACITY_WAIT);
	UT_ASSERT(!fake_cv_prepared);
	UT_ASSERT_EQ(capacity_step(capacity_target, deadline), RESOURCE_X_BOOTSTRAP_ROUND_DISPATCH_REQUEST);
	UT_ASSERT_EQ(ut_ereport_fired_count, 0);
}

UT_TEST(retirement_after_registration_is_not_lost)
{
	PcmGrdLifecycleStats stats;
	capacity_fill(true, true);
	UT_ASSERT_EQ(capacity_step(capacity_target, 1000), RESOURCE_X_BOOTSTRAP_ROUND_NO_CAPACITY);
	fake_cv_prepare_hook = capacity_tick;
	UT_ASSERT_EQ(cluster_pcm_lock_wait_for_capacity_exact(&capacity_target, 17, 2), RESOURCE_X_APPLY_APPLIED);
	cluster_pcm_grd_lifecycle_stats_snapshot(&stats);
	UT_ASSERT_EQ(stats.reclaim_success_count, 1);
	UT_ASSERT_EQ(stats.capacity_wait_count, 0);
	UT_ASSERT_EQ(capacity_sleep_calls, 0);
	UT_ASSERT(!fake_cv_prepared);
}

UT_TEST(spurious_wakes_do_not_reclaim_active_x_or_expire_capacity_wait)
{
	ResourceXCallerWitness caller = { 0 };
	uint64 deadline = 1000;
	PcmGrdLifecycleStats stats;
	capacity_fill(true, false);
	capacity_elapsed_us = 100000;
	capacity_sleep_action = capacity_tick;
	for (unsigned pass = 0; pass < 3; pass++) {
		ResourceXBootstrapRoundAction action = capacity_step(capacity_target, deadline);
		UT_ASSERT_EQ(action, RESOURCE_X_BOOTSTRAP_ROUND_NO_CAPACITY);
		UT_ASSERT_EQ(capacity_consume(action, &caller, &deadline), RESOURCE_X_APPLY_APPLIED);
	}
	cluster_pcm_grd_lifecycle_stats_snapshot(&stats);
	UT_ASSERT_EQ(stats.live_entries, FAKE_PCM_MAX_ENTRIES);
	UT_ASSERT_EQ(stats.reclaim_success_count, 0);
	UT_ASSERT_EQ(stats.capacity_wait_count, 3);
	UT_ASSERT_EQ(deadline, 301000);
	capacity_sleep_action = capacity_release_holder;
	UT_ASSERT_EQ(capacity_consume(RESOURCE_X_BOOTSTRAP_ROUND_NO_CAPACITY, &caller, &deadline), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(capacity_step(capacity_target, deadline), RESOURCE_X_BOOTSTRAP_ROUND_DISPATCH_REQUEST);
	UT_ASSERT_EQ(ut_ereport_fired_count, 0);
}

UT_TEST(ko_refusal_stays_distinct_even_when_the_registry_is_full)
{
	ResourceXCallerWitness caller = { 0 };
	uint64 deadline = 1000;
	ResourceXBootstrapRoundAction action;
	capacity_fill(true, false);
	cluster_shared_config = true;
	UT_ASSERT_EQ(capacity_step(capacity_target, deadline), RESOURCE_X_BOOTSTRAP_ROUND_NO_CAPACITY);
	object_admission_allowed = false;
	action = capacity_step(capacity_target, deadline);
	UT_ASSERT_EQ(action, RESOURCE_X_BOOTSTRAP_ROUND_BACKPRESSURE);
	UT_ASSERT_EQ(capacity_consume(action, &caller, &deadline), RESOURCE_X_APPLY_BAD_STATE);
	UT_ASSERT_EQ(deadline, 1000);
	UT_ASSERT_EQ(capacity_sleep_calls, 0);
}

UT_TEST(wakeup_does_not_admit_a_newly_closed_object)
{
	ResourceXCallerWitness caller = { 0 };
	uint64 deadline = 1000;
	capacity_fill(true, true);
	cluster_shared_config = true;
	capacity_sleep_action = capacity_close_ko;
	UT_ASSERT_EQ(capacity_consume(capacity_step(capacity_target, deadline), &caller, &deadline), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(capacity_step(capacity_target, deadline), RESOURCE_X_BOOTSTRAP_ROUND_BACKPRESSURE);
}

UT_TEST(member_and_gate_changes_wake_and_invalidate)
{
	for (int change = 0; change < 2; change++) {
		int broadcasts;
		capacity_fill(true, false);
		broadcasts = fake_cv_broadcast_count;
		capacity_sleep_action = change == 0 ? capacity_change_member : capacity_change_gate;
		UT_ASSERT_EQ(cluster_pcm_lock_wait_for_capacity_exact(&capacity_target, 17, 2), RESOURCE_X_APPLY_STALE);
		UT_ASSERT(fake_cv_broadcast_count > broadcasts);
		UT_ASSERT(!fake_cv_prepared);
	}
}

UT_TEST(session_and_semantic_changes_reject_after_wakeup)
{
	for (int change = 0; change < 2; change++) {
		ResourceXCallerWitness caller = { 0 };
		uint64 deadline = 1000;
		capacity_fill(true, false);
		capacity_sleep_action = change == 0 ? capacity_change_session : capacity_close_admission;
		UT_ASSERT_EQ(capacity_consume(capacity_step(capacity_target, deadline), &caller, &deadline), RESOURCE_X_APPLY_STALE);
		UT_ASSERT(!fake_cv_prepared);
	}
}

UT_TEST(held_lwlock_refuses_capacity_sleep)
{
	LWLock lock;
	capacity_fill(true, false);
	LWLockInitialize(&lock, 0);
	LWLockAcquire(&lock, LW_EXCLUSIVE);
	UT_ASSERT_EQ(cluster_pcm_lock_wait_for_capacity_exact(&capacity_target, 17, 2), RESOURCE_X_APPLY_INVALID);
	LWLockRelease(&lock);
	UT_ASSERT_EQ(capacity_sleep_calls, 0);
}

UT_TEST(cancellation_unwinds_registration)
{
	volatile bool caught = false;
	capacity_fill(true, false);
	fake_cv_prepare_hook = capacity_cancel;
	fake_error_use_pg_exception_stack = true;
	PG_TRY(); {
		(void)cluster_pcm_lock_wait_for_capacity_exact(&capacity_target, 17, 2);
	} PG_CATCH(); { caught = true; } PG_END_TRY();
	fake_error_use_pg_exception_stack = false;
	InterruptPending = false;
	UT_ASSERT(caught);
	UT_ASSERT_EQ(fake_last_sqlerrcode, ERRCODE_QUERY_CANCELED);
	UT_ASSERT(!fake_cv_prepared);
	UT_ASSERT_EQ(fake_cv_cancel_count, 1);
	UT_ASSERT_EQ(capacity_sleep_calls, 0);
}

UT_TEST(existing_target_is_a_recheck_hint_without_sleep_or_authority)
{
	BufferTag tag = make_tag(10000);
	PcmGrdLifecycleStats stats;
	capacity_fill(true, false);
	UT_ASSERT_EQ(cluster_pcm_lock_wait_for_capacity_exact(&tag, 17, 2), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(capacity_sleep_calls, 0);
	cluster_pcm_grd_lifecycle_stats_snapshot(&stats);
	UT_ASSERT_EQ(stats.live_entries, FAKE_PCM_MAX_ENTRIES);
	UT_ASSERT_EQ(cluster_pcm_lock_query(tag), PCM_STATE_X);
}

UT_TEST(joined_caller_cannot_reset_its_first_deadline)
{
	ResourceXCallerWitness caller = { 0 };
	uint64 deadline = 1000;
	capacity_fill(true, false);
	caller.joined_request.assertion_sequence = 1;
	UT_ASSERT_EQ(capacity_consume(RESOURCE_X_BOOTSTRAP_ROUND_NO_CAPACITY, &caller, &deadline), RESOURCE_X_APPLY_STALE);
	UT_ASSERT_EQ(deadline, 1000);
	UT_ASSERT_EQ(capacity_sleep_calls, 0);
}

UT_TEST(real_cached_x_eviction_preserves_unretired_pi)
{
	ClusterPcmLocalPiSnapshotV1 pi;
	PcmGrdLifecycleStats before, after;
	cached_x_eviction_case(true);
	cluster_shared_config = local_pi_writer_ready = true;
	cluster_pcm_grd_lifecycle_stats_snapshot(&before);
	for (unsigned tick = 0; tick < 9; tick++)
		cluster_pcm_lock_lmon_reclaim_tick();
	cluster_pcm_grd_lifecycle_stats_snapshot(&after);
	UT_ASSERT(cluster_pcm_local_pi_snapshot_v1(make_tag(161), &pi));
	UT_ASSERT_EQ(after.live_entries, before.live_entries);
	UT_ASSERT_EQ(after.reclaim_success_count, before.reclaim_success_count);
	UT_ASSERT(pi.first.flags != 0 && pi.last.flags != 0);
}

int
main(void)
{
	UT_PLAN(13);
	UT_RUN(cold_entries_reclaim_within_original_sync_budget);
	UT_RUN(cold_entry_at_registry_64_waits_for_original_lmon);
	UT_RUN(retirement_after_registration_is_not_lost);
	UT_RUN(spurious_wakes_do_not_reclaim_active_x_or_expire_capacity_wait);
	UT_RUN(ko_refusal_stays_distinct_even_when_the_registry_is_full);
	UT_RUN(wakeup_does_not_admit_a_newly_closed_object);
	UT_RUN(member_and_gate_changes_wake_and_invalidate);
	UT_RUN(session_and_semantic_changes_reject_after_wakeup);
	UT_RUN(held_lwlock_refuses_capacity_sleep);
	UT_RUN(cancellation_unwinds_registration);
	UT_RUN(existing_target_is_a_recheck_hint_without_sleep_or_authority);
	UT_RUN(joined_caller_cannot_reset_its_first_deadline);
	UT_RUN(real_cached_x_eviction_preserves_unretired_pi);
	UT_DONE();
	return ut_failed_count != 0;
}
