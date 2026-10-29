/* Actual original job/pin/owner/registry. Native locks, allocator, S1-S5,
 * callback dispatch, transport and time are explicit standalone boundaries.
 * Portions Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com> */
#define PGRAC_WAL_INPUTS_JOB_EMBEDDED
#include "test_cluster_wal_inputs_job.c"

static void
pin_begin(void)
{
	foundation_begin();
	UT_ASSERT(active_read_pin == NULL && pin_native_refs == 0);
	pin_native_calls = pin_fail_at = pin_throw_at = 0;
	pin_native_result = LOCKACQUIRE_OK;
	pin_bound_before_native = false;
	g_seven_result = CLUSTER_LOCK_ACQUIRE_NEED_PG_NATIVE_LOCK;
}

static ClusterWalrReleaseResult
pin_first_retire(ClusterWalInputsJobV1 *job, ClusterWalReadPinV1 **pin)
{
	ClusterWalInputsJobResultV1 result = cluster_wal_inputs_job_retire_v1(job);
	*pin = job->pin;
	return result == WAL_INPUT_JOB_DONE ? CLUSTER_WALR_RELEASE_CONFIRMED
		: result == WAL_INPUT_JOB_PENDING ? CLUSTER_WALR_RELEASE_UNCONFIRMED : CLUSTER_WALR_RELEASE_INVALID;
}

static void
pin_cleanup(ClusterWalInputsJobV1 **job, ClusterWalReadPinV1 **pin)
{
	ClusterWalInputsJobResultV1 result = cluster_wal_inputs_job_retire_v1(*job);
	for (unsigned i = 0; result == WAL_INPUT_JOB_PENDING && i < 260; ++i) {
		uint64 count;
		(void)acknowledge_retirements();
		(void)cluster_lock_owners_redeclare(&count);
		result = cluster_wal_inputs_job_poll_v1(*job);
	}
	*pin = (*job)->pin;
	UT_ASSERT_EQ(result, WAL_INPUT_JOB_DONE);
	UT_ASSERT(*pin == NULL && active_read_pin == NULL && pin_native_refs == 0);
	foundation_finish(job);
	foundation_end();
}

UT_TEST(pin_binding_precedes_first_grant_in_original_job_context)
{
	uint16 threads[] = { 1 };
	ClusterWalReadPinV1 *pin = NULL;
	ClusterWalInputsJobV1 *job;
	JobFixtureAllocation *entry;
	MemoryContext allocation_context = NULL;

	pin_begin();
	job = foundation_open(WAL_INPUT_JOB_RETAINED_CUT);
	UT_ASSERT_EQ(cluster_wal_read_pin_acquire_job_v1(threads, 1, job, &pin), CLUSTER_WAL_PIN_OK);
	for (entry = job_fixture_allocations; entry != NULL; entry = entry->next)
		if (entry->allocation == pin)
			allocation_context = entry->context;
	UT_ASSERT(job->pin == pin);
	UT_ASSERT_EQ(job->pin_generation, pin->generation);
	UT_ASSERT(allocation_context == job->context);
	pin_cleanup(&job, &pin);
}

UT_TEST(pin_failed_second_install_keeps_both_original_native_refs)
{
	uint16 threads[] = { 1, 2 };
	ClusterWalReadPinV1 *pin = NULL;
	ClusterWalInputsJobV1 *job;

	pin_begin();
	job = foundation_open(WAL_INPUT_JOB_RETAINED_CUT);
	pin_fail_at = 2;
	UT_ASSERT_EQ(cluster_wal_read_pin_acquire_job_v1(threads, 2, job, &pin), CLUSTER_WAL_PIN_RELEASE_UNCERTAIN);
	UT_ASSERT_EQ(pin_native_refs, 2);
	UT_ASSERT(job->pin == pin);
	pin_cleanup(&job, &pin);
}

UT_TEST(pin_throwing_second_install_keeps_original_native_debt)
{
	uint16 threads[] = { 1, 2 };
	ClusterWalReadPinV1 *pin = NULL;
	ClusterWalInputsJobV1 *job;
	volatile bool caught = false;

	pin_begin();
	job = foundation_open(WAL_INPUT_JOB_RETAINED_CUT);
	pin_throw_at = 2;
	PG_TRY(); { (void)cluster_wal_read_pin_acquire_job_v1(threads, 2, job, &pin); }
	PG_CATCH(); { caught = true; } PG_END_TRY();
	throw_during_install = false;
	pin = active_read_pin;
	UT_ASSERT(caught && pin != NULL);
	UT_ASSERT_EQ(pin_native_refs, 2);
	UT_ASSERT(job->pin == pin);
	pin_cleanup(&job, &pin);
}

UT_TEST(pin_first_unconfirmed_member_stops_synchronous_progress)
{
	uint16 threads[] = { 1, 2, 3 };
	ClusterWalReadPinV1 *pin = NULL;
	ClusterWalInputsJobV1 *job;
	unsigned waits;

	pin_begin();
	job = foundation_open(WAL_INPUT_JOB_RETAINED_CUT);
	UT_ASSERT_EQ(cluster_wal_read_pin_acquire_job_v1(threads, 3, job, &pin), CLUSTER_WAL_PIN_OK);
	waits = owner_wait_calls;
	UT_ASSERT_EQ(pin_first_retire(job, &pin), CLUSTER_WALR_RELEASE_UNCONFIRMED);
	printf("# first-release wait calls=%u\n", owner_wait_calls - waits);
	UT_ASSERT(owner_wait_calls - waits <= 650);
	UT_ASSERT_EQ(pin_native_refs, 3);
	pin_cleanup(&job, &pin);
}

static void
pin_ack_member(const ClusterLockAcquireRequest *request, bool wrong)
{
	ClusterControlRequestView view;
	ClusterControlRequestCut cut;
	ClusterControlRetireMessage ack;
	uint32 cursor = 0;
	bool found = false;
	unsigned wakes = job_wakes;

	while (cluster_control_request_next(&cursor, &view))
		if (view.message.key.holder.request_id == request->holder.request_id
			&& memcmp(&view.message.key.resid, &request->resid, sizeof(request->resid)) == 0) {
			found = true;
			break;
		}
	UT_ASSERT(found);
	control_cut(&cut);
	UT_ASSERT(cluster_control_request_claim(&view.handle, retire_driver, &cut, &ack));
	ack.verb = CLUSTER_CONTROL_RETIRED;
	if (wrong)
		++ack.key.holder.request_id;
	control_receive_ack(&ack, cut.master);
	UT_ASSERT_EQ(job_wakes, wakes + (wrong ? 0 : 1));
}

UT_TEST(pin_cf_receipt_precedes_any_walr_retirement)
{
	uint16 threads[] = { 1 };
	ClusterWalReadPinV1 *pin = NULL;
	ClusterWalInputsJobV1 *job;
	unsigned waits;

	pin_begin();
	job = foundation_open(WAL_INPUT_JOB_RETAINED_CUT);
	UT_ASSERT_EQ(cluster_wal_read_pin_acquire_job_v1(threads, 1, job, &pin), CLUSTER_WAL_PIN_OK);
	g_seven_result = CLUSTER_LOCK_ACQUIRE_OK_GRANTED;
	UT_ASSERT(cluster_cf_lock(ShareLock));
	UT_ASSERT_EQ(cluster_cf_unlock_job_v1(ShareLock, NULL, &job->cf), CF_JOB_PENDING);
	waits = owner_wait_calls;
	UT_ASSERT_EQ(cluster_wal_inputs_job_retire_v1(job), WAL_INPUT_JOB_PENDING);
	UT_ASSERT(cluster_lock_owner_request_usable(&pin->locks[0].request));
	UT_ASSERT(!cluster_wal_read_pin_covers_v1(pin, 1));
	UT_ASSERT_EQ(pin_native_refs, 1);
	job_ack(&job->cf);
	UT_ASSERT_EQ(cluster_wal_inputs_job_poll_v1(job), WAL_INPUT_JOB_PENDING);
	UT_ASSERT(!cluster_lock_owner_request_usable(&pin->locks[0].request));
	UT_ASSERT_EQ(owner_wait_calls, waits);
	pin_cleanup(&job, &pin);
}

UT_TEST(pin_original_identity_backlinks_and_cursor_cannot_be_substituted)
{
	uint16 threads[] = { 1 };
	ClusterWalReadPinV1 *pin = NULL;
	ClusterWalInputsJobV1 *job;
	uint64 generation;

	pin_begin();
	job = foundation_open(WAL_INPUT_JOB_RETAINED_CUT);
	UT_ASSERT_EQ(cluster_wal_read_pin_acquire_job_v1(threads, 1, job, &pin), CLUSTER_WAL_PIN_OK);
	generation = pin->generation;
	job->pin = NULL;
	job->pin_generation = 0;
	UT_ASSERT_EQ(cluster_wal_inputs_jobs_stop_census_v1(), WAL_INPUT_JOB_INVALID);
	job->pin = pin;
	job->pin_generation = generation;
	++pin->generation;
	++job->pin_generation;
	UT_ASSERT_EQ(cluster_wal_inputs_job_retire_v1(job), WAL_INPUT_JOB_INVALID);
	pin->generation = job->pin_generation = generation;
	++MyProc->cluster_grd_generation;
	UT_ASSERT_EQ(cluster_wal_read_pin_release_job_v1(&pin, job), CLUSTER_WALR_RELEASE_INVALID);
	--MyProc->cluster_grd_generation;
	UT_ASSERT_EQ(cluster_wal_inputs_job_retire_v1(job), WAL_INPUT_JOB_PENDING);
	job->walr_next = 0;
	UT_ASSERT_EQ(cluster_wal_inputs_job_poll_v1(job), WAL_INPUT_JOB_INVALID);
	job->walr_next = pin->cursor;
	UT_ASSERT_EQ(pin_native_refs, 1);
	pin_cleanup(&job, &pin);
}

UT_TEST(pin_legacy_release_and_early_finish_refuse_job_storage)
{
	uint16 threads[] = { 1 };
	ClusterWalReadPinV1 *pin = NULL, *foreign = (ClusterWalReadPinV1 *)1;
	ClusterWalInputsJobV1 *job;

	pin_begin();
	job = foundation_open(WAL_INPUT_JOB_RETAINED_CUT);
	UT_ASSERT_EQ(cluster_wal_read_pin_acquire_job_v1(threads, 1, job, &pin), CLUSTER_WAL_PIN_OK);
	UT_ASSERT(pin_bound_before_native);
	UT_ASSERT_EQ(cluster_wal_read_pin_release_v1(&pin), CLUSTER_WALR_RELEASE_INVALID);
	UT_ASSERT_EQ(cluster_wal_read_pin_release_job_v1(&foreign, job), CLUSTER_WALR_RELEASE_INVALID);
	UT_ASSERT_EQ(cluster_wal_inputs_job_finish_v1(&job), WAL_INPUT_JOB_PENDING);
	UT_ASSERT_EQ(cluster_wal_inputs_jobs_stop_census_v1(), WAL_INPUT_JOB_PENDING);
	UT_ASSERT_EQ(pin_native_refs, 1);
	pin_cleanup(&job, &pin);
}

UT_TEST(pin_unpublished_refusal_keeps_original_guard_until_caller_consumes_it)
{
	uint16 threads[] = { 1 };
	ClusterWalReadPinV1 *pin = NULL;
	ClusterWalInputsJobV1 *job;
	uint64 count;

	pin_begin();
	job = foundation_open(WAL_INPUT_JOB_RETAINED_CUT);
	g_seven_result = CLUSTER_LOCK_ACQUIRE_NOT_AVAIL;
	UT_ASSERT_EQ(cluster_wal_read_pin_acquire_job_v1(threads, 1, job, &pin), CLUSTER_WAL_PIN_RELEASE_UNCERTAIN);
	UT_ASSERT_EQ(pin_native_refs, 0);
	UT_ASSERT(cluster_lock_owners_redeclare(&count));
	UT_ASSERT_EQ(count, 1);
	UT_ASSERT_EQ(pg_atomic_read_u32(&MyProc->cluster_grd_registered_count), 1);
	pin_cleanup(&job, &pin);
}

UT_TEST(pin_unpublished_error_keeps_cleanup_locator_and_original_error)
{
	uint16 threads[] = { 1 };
	ClusterWalReadPinV1 *pin = NULL;
	ClusterWalInputsJobV1 *job;
	volatile bool caught = false;

	pin_begin();
	job = foundation_open(WAL_INPUT_JOB_RETAINED_CUT);
	throw_before_publication = true;
	PG_TRY(); { (void)cluster_wal_read_pin_acquire_job_v1(threads, 1, job, &pin); }
	PG_CATCH(); { caught = true; } PG_END_TRY();
	throw_before_publication = false;
	pin = job->pin;
	UT_ASSERT(caught && pin != NULL);
	UT_ASSERT((job->flags & WAL_INPUT_JOB_FLAG_ERROR) != 0);
	UT_ASSERT_EQ(pin_native_refs, 0);
	pin_cleanup(&job, &pin);
}

UT_TEST(pin_reentry_and_original_callbacks_never_restart_confirmation_budget)
{
	uint16 threads[] = { 1, 2 };
	ClusterWalReadPinV1 *pin = NULL;
	ClusterWalInputsJobV1 *job;
	unsigned waits;

	pin_begin();
	job = foundation_open(WAL_INPUT_JOB_RETAINED_CUT);
	UT_ASSERT_EQ(cluster_wal_read_pin_acquire_job_v1(threads, 2, job, &pin), CLUSTER_WAL_PIN_OK);
	UT_ASSERT_EQ(cluster_wal_inputs_job_retire_v1(job), WAL_INPUT_JOB_PENDING);
	waits = owner_wait_calls;
	UT_ASSERT_EQ(cluster_wal_inputs_job_retire_v1(job), WAL_INPUT_JOB_PENDING);
	UT_ASSERT_EQ(cluster_wal_read_pin_release_job_v1(&pin, job), CLUSTER_WALR_RELEASE_UNCONFIRMED);
	job_resource_callback(RESOURCE_RELEASE_BEFORE_LOCKS, false, true, job_resource_argument);
	UT_ASSERT_EQ(job->state, WAL_INPUT_JOB_STATE_RETIRING_WALR);
	UT_ASSERT(read_pin_job_owner_cleanup(pin, false));
	UT_ASSERT_EQ(cluster_wal_inputs_job_retire_v1(job), WAL_INPUT_JOB_PENDING);
	UT_ASSERT_EQ(owner_wait_calls, waits);
	UT_ASSERT_EQ(pin_native_refs, 2);
	pin_cleanup(&job, &pin);
}

UT_TEST(pin_healthy_first_release_can_confirm_synchronously)
{
	uint16 threads[] = { 1, 2 };
	ClusterWalReadPinV1 *pin = NULL;
	ClusterWalInputsJobV1 *job;
	unsigned waits;

	pin_begin();
	job = foundation_open(WAL_INPUT_JOB_RETAINED_CUT);
	UT_ASSERT_EQ(cluster_wal_read_pin_acquire_job_v1(threads, 2, job, &pin), CLUSTER_WAL_PIN_OK);
	waits = owner_wait_calls;
	owner_ack_on_wait = true;
	UT_ASSERT_EQ(cluster_wal_inputs_job_retire_v1(job), WAL_INPUT_JOB_DONE);
	owner_ack_on_wait = false;
	UT_ASSERT(job->pin == NULL && active_read_pin == NULL);
	UT_ASSERT_EQ(pin_native_refs, 0);
	UT_ASSERT(owner_wait_calls > waits);
	pin = NULL;
	pin_cleanup(&job, &pin);
}

UT_TEST(pin_ack_cannot_be_forged_and_native_tag_is_checked_before_consumption)
{
	uint16 threads[] = { 1 };
	ClusterWalReadPinV1 *pin = NULL;
	ClusterWalInputsJobV1 *job;
	LOCKTAG original;

	pin_begin();
	job = foundation_open(WAL_INPUT_JOB_RETAINED_CUT);
	UT_ASSERT_EQ(cluster_wal_read_pin_acquire_job_v1(threads, 1, job, &pin), CLUSTER_WAL_PIN_OK);
	UT_ASSERT_EQ(cluster_wal_inputs_job_retire_v1(job), WAL_INPUT_JOB_PENDING);
	pin_ack_member(&pin->locks[0].request, true);
	UT_ASSERT_EQ(cluster_wal_inputs_job_poll_v1(job), WAL_INPUT_JOB_PENDING);
	UT_ASSERT_EQ(pin_native_refs, 1);
	original = pin->locks[0].request.locktag;
	++pin->locks[0].request.locktag.locktag_field2;
	UT_ASSERT_EQ(cluster_wal_inputs_job_poll_v1(job), WAL_INPUT_JOB_INVALID);
	UT_ASSERT_EQ(pg_atomic_read_u32(&MyProc->cluster_grd_registered_count), 1);
	pin->locks[0].request.locktag = original;
	pin_cleanup(&job, &pin);
}

UT_TEST(pin_owner_cleanup_survives_work_reset_and_exit_without_waiting)
{
	uint16 threads[] = { 1 };
	ClusterWalReadPinV1 *pin = NULL;
	ClusterWalInputsJobV1 *job;
	unsigned waits;

	pin_begin();
	job = foundation_open(WAL_INPUT_JOB_RETAINED_CUT);
	UT_ASSERT_EQ(cluster_wal_read_pin_acquire_job_v1(threads, 1, job, &pin), CLUSTER_WAL_PIN_OK);
	waits = owner_wait_calls;
	proc_exit_inprogress = true;
	UT_ASSERT(read_pin_job_owner_cleanup(pin, false));
	job_resource_callback(RESOURCE_RELEASE_BEFORE_LOCKS, false, true, job_resource_argument);
	MemoryContextReset(work_context);
	UT_ASSERT(job->pin == pin && active_read_pin == pin);
	UT_ASSERT_EQ(pin_native_refs, 1);
	UT_ASSERT_EQ(owner_wait_calls, waits);
	UT_ASSERT_EQ(cluster_wal_inputs_job_poll_v1(job), WAL_INPUT_JOB_INVALID);
	proc_exit_inprogress = false;
	UT_ASSERT(!cluster_wal_read_pin_covers_v1(pin, 1));
	pin_cleanup(&job, &pin);
}

UT_TEST(pin_single_active_allocation_and_original_aux_role_remain_required)
{
	uint16 threads[] = { 1 };
	ClusterWalReadPinV1 *pin = NULL, *other_pin = NULL;
	ClusterWalInputsJobV1 *job, *other;

	pin_begin();
	job = foundation_open(WAL_INPUT_JOB_RETAINED_CUT);
	other = foundation_open(WAL_INPUT_JOB_PI_BATCH);
	UT_ASSERT_EQ(cluster_wal_read_pin_acquire_job_v1(threads, 1, job, &pin), CLUSTER_WAL_PIN_OK);
	UT_ASSERT_EQ(cluster_wal_read_pin_acquire_job_v1(threads, 1, other, &other_pin), CLUSTER_WAL_PIN_CAPACITY);
	UT_ASSERT(other_pin == NULL && other->pin == NULL);
	foundation_finish(&other);
	CurrentResourceOwner = (ResourceOwner)&job_fixture_foreign_owner;
	UT_ASSERT_EQ(cluster_wal_inputs_job_retire_v1(job), WAL_INPUT_JOB_INVALID);
	CurrentResourceOwner = AuxProcessResourceOwner;
	pin_cleanup(&job, &pin);
}

UT_TEST(pin_128_exact_ack_events_drive_single_member_ticks_without_sleep)
{
	uint16 threads[128];
	ClusterWalReadPinV1 *pin = NULL;
	ClusterWalInputsJobV1 *job;
	unsigned waits, ack_events = 0, completion_events = 0;

	pin_begin();
	for (unsigned i = 0; i < lengthof(threads); ++i)
		threads[i] = (uint16)(i + 1);
	job = foundation_open(WAL_INPUT_JOB_RETAINED_CUT);
	UT_ASSERT_EQ(cluster_wal_read_pin_acquire_job_v1(threads, 128, job, &pin), CLUSTER_WAL_PIN_OK);
	UT_ASSERT_EQ(pin_native_refs, 128);
	job_resource_callback(RESOURCE_RELEASE_BEFORE_LOCKS, false, true, job_resource_argument);
	waits = owner_wait_calls;
	for (unsigned remaining = 128; remaining > 0; --remaining) {
		unsigned wakes;
		ClusterWalInputsJobResultV1 result;

		/* Entry after prior completion's original-latch wake starts only the
		 * next retirement. A missing ACK never advances the native ref. */
		UT_ASSERT_EQ(cluster_wal_inputs_job_poll_v1(job), WAL_INPUT_JOB_PENDING);
		UT_ASSERT_EQ(pin_native_refs, remaining);
		pin_ack_member(&pin->locks[remaining - 1].request, false);
		++ack_events;
		wakes = job_wakes;
		result = cluster_wal_inputs_job_poll_v1(job);
		UT_ASSERT_EQ(pin_native_refs, remaining - 1);
		UT_ASSERT_EQ(result, remaining == 1 ? WAL_INPUT_JOB_DONE : WAL_INPUT_JOB_PENDING);
		UT_ASSERT_EQ(job_wakes, wakes + (remaining > 1 ? 1 : 0));
		completion_events += remaining > 1;
	}
	UT_ASSERT_EQ(ack_events, 128);
	UT_ASSERT_EQ(completion_events, 127);
	UT_ASSERT_EQ(owner_wait_calls, waits);
	UT_ASSERT_EQ(cluster_wal_inputs_jobs_stop_census_v1(), WAL_INPUT_JOB_DONE);
	printf("# original ACK wakes=%u; local retirement completion wakes=%u; wait calls=0\n",
		ack_events, completion_events);
	pin = NULL;
	pin_cleanup(&job, &pin);
}

UT_TEST(pin_interrupt_during_first_confirmation_retains_original_custody)
{
	uint16 threads[] = { 1, 2 };
	ClusterWalReadPinV1 *pin = NULL;
	ClusterWalInputsJobV1 *job;
	volatile bool caught = false;
	unsigned waits;

	pin_begin();
	job = foundation_open(WAL_INPUT_JOB_RETAINED_CUT);
	UT_ASSERT_EQ(cluster_wal_read_pin_acquire_job_v1(threads, 2, job, &pin), CLUSTER_WAL_PIN_OK);
	job_throw_on_wait = true;
	PG_TRY(); { (void)cluster_wal_inputs_job_retire_v1(job); }
	PG_CATCH(); { caught = true; } PG_END_TRY();
	UT_ASSERT(caught && !job_throw_on_wait);
	pin = job->pin;
	UT_ASSERT_EQ(pin_native_refs, 2);
	UT_ASSERT_EQ(job->state, WAL_INPUT_JOB_STATE_RETIRING_WALR);
	UT_ASSERT(pin->retiring && pin->cursor == 2 && job->walr_next == 2);
	waits = owner_wait_calls;
	UT_ASSERT_EQ(cluster_wal_inputs_job_retire_v1(job), WAL_INPUT_JOB_PENDING);
	UT_ASSERT_EQ(owner_wait_calls, waits);
	pin_cleanup(&job, &pin);
}

#ifndef PGRAC_WAL_INPUTS_JOB_PIN_EMBEDDED
int
main(void)
{
	MyProc = &job_procs[42];
	MyProc->pgprocno = 42;
	MyLatch = &MyProc->procLatch;
	job_proc_global.allProcs = job_procs;
	job_proc_global.allProcCount = lengthof(job_procs);
	pg_atomic_init_u32(&MyProc->cluster_grd_registered_count, 0);
	pg_atomic_init_u64(&MyProc->cluster_grd_redeclare_acked, 0);
	pg_atomic_init_u64(&MyProc->cluster_grd_redeclare_acked_epoch, 0);
	MyProc->cluster_grd_generation = 1;
	cluster_shared_config = true;
	cluster_control_request_shmem_init();
	control_driver = retire_driver = cluster_control_request_driver_start();
	UT_PLAN(16);
	UT_RUN(pin_binding_precedes_first_grant_in_original_job_context);
	UT_RUN(pin_failed_second_install_keeps_both_original_native_refs);
	UT_RUN(pin_throwing_second_install_keeps_original_native_debt);
	UT_RUN(pin_first_unconfirmed_member_stops_synchronous_progress);
	UT_RUN(pin_cf_receipt_precedes_any_walr_retirement);
	UT_RUN(pin_original_identity_backlinks_and_cursor_cannot_be_substituted);
	UT_RUN(pin_legacy_release_and_early_finish_refuse_job_storage);
	UT_RUN(pin_unpublished_refusal_keeps_original_guard_until_caller_consumes_it);
	UT_RUN(pin_unpublished_error_keeps_cleanup_locator_and_original_error);
	UT_RUN(pin_reentry_and_original_callbacks_never_restart_confirmation_budget);
	UT_RUN(pin_healthy_first_release_can_confirm_synchronously);
	UT_RUN(pin_ack_cannot_be_forged_and_native_tag_is_checked_before_consumption);
	UT_RUN(pin_owner_cleanup_survives_work_reset_and_exit_without_waiting);
	UT_RUN(pin_single_active_allocation_and_original_aux_role_remain_required);
	UT_RUN(pin_128_exact_ack_events_drive_single_member_ticks_without_sleep);
	UT_RUN(pin_interrupt_during_first_confirmation_retains_original_custody);
	UT_DONE();
	return ut_failed_count != 0;
}
#endif
