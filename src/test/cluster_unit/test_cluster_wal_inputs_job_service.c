/* Original auxiliary loop prefixes with real job/CF/WALR/owner/ACK bodies.
 * Native interrupts, latch reset, transport, clock and memory are explicit
 * offline boundaries. This is not an installed service-loop witness.
 * Author: SqlRush <sqlrush@gmail.com> */
#define PGRAC_WAL_INPUTS_JOB_PIN_EMBEDDED
#include "test_cluster_wal_inputs_job_pin.c"
#include "cluster/cluster_pi_writeback.h"
#include "cluster/cluster_clean_leave.h"
#include "test_cluster_pi_empty_batch_fixture.h"
#include "storage/spin.h"

static unsigned loop_interrupts, loop_resets;
static void (*ack_during_reset)(void);

static void
loop_reset(Latch *latch)
{
	UT_ASSERT(latch == MyLatch);
	++loop_resets;
	latch->is_set = false;
	if (ack_during_reset != NULL)
		ack_during_reset();
}

static void HandleMainLoopInterrupts(void) { ++loop_interrupts; }
static void HandleCheckpointerInterrupts(void) { ++loop_interrupts; }
static void AbsorbSyncRequests(void) {}
static void CheckpointerStartupSyncPoll(void) {}
static void cl_normal_stop_trace_peer_acks(void) {}
/* No concurrent process exists in this offline fixture. Contention refuses. */
int
s_lock(volatile slock_t *lock, const char *file, int line, const char *func)
{
	(void)lock; (void)file; (void)line; (void)func;
	abort();
	return 0;
}
static bool service_stopping;
bool cluster_normal_stop_requested(void) { return service_stopping; }

/* Exact production loop prefixes, including ResetLatch ordering. */
#define ResetLatch loop_reset
static void
bgwriter_tick(void)
{
#include "test_cluster_wal_inputs_job_bgwriter.inc"
}
static void
checkpointer_tick(void)
{
#include "test_cluster_wal_inputs_job_checkpointer.inc"
}
static void
stop_tick(void)
{
#include "test_cluster_wal_inputs_job_stop.inc"
}
static void
stopped_tick(void)
{
	ClusterNormalStopModuleObservation normal_stop_observation = {0};

#include "test_cluster_wal_inputs_job_stopped.inc"
}
#undef ResetLatch

static ClusterNormalStopPollResult
inputs_stop_module(ClusterNormalStopModuleObservation *out)
{
	ClusterNormalStopModuleObservation current = {.module = "WAL_INPUT_JOB"};
	ClusterNormalStopPollResult result = CLUSTER_NORMAL_STOP_INVALID;

	/* This is the actual new case in the original all-owner census. Its
	 * aggregation and peer/identity gates retain their existing tests. */
	switch (15) {
#include "test_cluster_wal_inputs_job_stop_module.inc"
	}
	*out = current;
	return result;
}

/* Whole production shared type and quiescence function, not a fake flag. */
static struct WritebackShared *wb_shared;
#include "test_cluster_wal_inputs_job_quiesce.inc"
static WritebackShared service_writeback;

static void
service_ack_members(void (*tick)(void), bool bgwriter)
{
	uint16 threads[128];
	ClusterWalReadPinV1 *pin = NULL;
	ClusterWalInputsJobV1 *job;
	unsigned waits, resets, interrupts;

	pin_begin();
	MyAuxProcType = CheckpointerProcess;
	if (bgwriter) {
		MyBackendType = B_BG_WRITER;
		MyAuxProcType = BgWriterProcess;
	}
	for (unsigned i = 0; i < lengthof(threads); ++i)
		threads[i] = (uint16)(i + 1);
	job = foundation_open(bgwriter ? WAL_INPUT_JOB_PI_REBUILD : WAL_INPUT_JOB_RETAINED_CUT);
	UT_ASSERT_EQ(cluster_wal_read_pin_acquire_job_v1(threads, 128, job, &pin), CLUSTER_WAL_PIN_OK);
	job_resource_callback(RESOURCE_RELEASE_BEFORE_LOCKS, false, true, job_resource_argument);
	waits = owner_wait_calls;
	resets = loop_resets;
	interrupts = loop_interrupts;
	for (unsigned remaining = 128; remaining > 0; --remaining) {
		unsigned wakes;

		tick(); /* Original retirement-completion event begins the next member. */
		UT_ASSERT_EQ(pin_native_refs, remaining);
		UT_ASSERT_EQ(cluster_wal_inputs_jobs_stop_census_v1(), WAL_INPUT_JOB_PENDING);
		pin_ack_member(&pin->locks[remaining - 1].request, false);
		wakes = job_wakes;
		tick();
		UT_ASSERT_EQ(pin_native_refs, remaining - 1);
		UT_ASSERT_EQ(job_wakes, wakes + (remaining > 1 ? 1 : 0));
		/* A continuation wake must survive the prefix's latch reset. */
		UT_ASSERT_EQ(MyLatch->is_set, remaining > 1);
	}
	UT_ASSERT_EQ(loop_resets - resets, 256);
	UT_ASSERT_EQ(loop_interrupts - interrupts,
		tick == bgwriter_tick || tick == checkpointer_tick ? 256 : 0);
	UT_ASSERT_EQ(owner_wait_calls, waits);
	UT_ASSERT_EQ(cluster_wal_inputs_jobs_stop_census_v1(), WAL_INPUT_JOB_DONE);
	pin = NULL;
	pin_cleanup(&job, &pin);
}

UT_TEST(service_bgwriter_128_acks_progress_without_delay_or_new_wait)
{
	service_ack_members(bgwriter_tick, true);
}

UT_TEST(service_checkpointer_128_acks_progress_without_checkpoint_timer)
{
	service_ack_members(checkpointer_tick, false);
}

UT_TEST(service_stop_loop_128_acks_progress_without_new_deadline)
{
	service_ack_members(stop_tick, false);
}

UT_TEST(service_stopped_publication_loop_keeps_original_ack_progress)
{
	service_ack_members(stopped_tick, false);
}

UT_TEST(service_bgwriter_quiescence_cannot_hide_retained_or_invalid_debt)
{
	uint16 threads[] = {1};
	ClusterWalReadPinV1 *pin = NULL;
	ClusterWalInputsJobV1 *job;
	volatile bool caught = false;

	pin_begin();
	MyAuxProcType = CheckpointerProcess;
	MyBackendType = B_BG_WRITER;
	MyAuxProcType = BgWriterProcess;
	job = foundation_open(WAL_INPUT_JOB_PI_NOTICE);
	UT_ASSERT_EQ(cluster_wal_read_pin_acquire_job_v1(threads, 1, job, &pin), CLUSTER_WAL_PIN_OK);
	memset(&service_writeback, 0, sizeof(service_writeback));
	SpinLockInit(&service_writeback.lock);
	wb_shared = &service_writeback;
	service_stopping = true;
	wb_shared->bgwriter_flush_quiesced = true; /* An older cut is not reusable. */
	cluster_pi_writeback_bgwriter_quiesced_v1();
	UT_ASSERT(!wb_shared->bgwriter_flush_quiesced);
	UT_ASSERT_EQ(cluster_wal_inputs_job_retire_v1(job), WAL_INPUT_JOB_PENDING);
	cluster_pi_writeback_bgwriter_quiesced_v1();
	UT_ASSERT(!wb_shared->bgwriter_flush_quiesced);
	++job->owner_proc_generation;
	job_expect_fatal = true;
	PG_TRY(); { cluster_pi_writeback_bgwriter_quiesced_v1(); }
	PG_CATCH(); { caught = true; } PG_END_TRY();
	job_expect_fatal = false;
	--job->owner_proc_generation;
	UT_ASSERT(caught);
	UT_ASSERT(!wb_shared->bgwriter_flush_quiesced);
	bgwriter_tick();
	pin_ack_member(&pin->locks[0].request, false);
	bgwriter_tick();
	UT_ASSERT_EQ(pin_native_refs, 0);
	cluster_pi_writeback_bgwriter_quiesced_v1();
	UT_ASSERT(wb_shared->bgwriter_flush_quiesced);
	service_stopping = false;
	wb_shared = NULL;
	pin = NULL;
	pin_cleanup(&job, &pin);
}

UT_TEST(service_stop_module_is_passive_and_invalid_debt_beats_pending)
{
	uint16 threads[] = {1};
	ClusterWalReadPinV1 *pin = NULL;
	ClusterWalInputsJobV1 *job, *other;
	ClusterNormalStopModuleObservation observation;
	unsigned waits;

	pin_begin();
	MyAuxProcType = CheckpointerProcess;
	job = foundation_open(WAL_INPUT_JOB_RETAINED_CUT);
	UT_ASSERT_EQ(cluster_wal_read_pin_acquire_job_v1(threads, 1, job, &pin), CLUSTER_WAL_PIN_OK);
	waits = owner_wait_calls;
	UT_ASSERT_EQ(inputs_stop_module(&observation), CLUSTER_NORMAL_STOP_PENDING);
	UT_ASSERT(strcmp(observation.reason, "WAL_INPUT_JOB_CLEANUP_PENDING") == 0);
	UT_ASSERT_EQ(pin_native_refs, 1);
	UT_ASSERT(cluster_lock_owner_request_usable(&pin->locks[0].request));
	other = foundation_open(WAL_INPUT_JOB_PI_BATCH);
	++other->generation;
	UT_ASSERT_EQ(inputs_stop_module(&observation), CLUSTER_NORMAL_STOP_INVALID);
	UT_ASSERT(strcmp(observation.reason, "WAL_INPUT_JOB_IDENTITY_INVALID") == 0);
	--other->generation;
	foundation_finish(&other);
	UT_ASSERT_EQ(owner_wait_calls, waits);
	pin_cleanup(&job, &pin);
	UT_ASSERT_EQ(inputs_stop_module(&observation), CLUSTER_NORMAL_STOP_READY);
}

UT_TEST(service_original_cf_confirmation_precedes_walr_in_both_aux_loops)
{
	for (unsigned role = 0; role < 2; ++role) {
		uint16 threads[] = {1};
		ClusterWalReadPinV1 *pin = NULL;
		ClusterWalInputsJobV1 *job;
		unsigned waits;
		void (*tick)(void) = role ? bgwriter_tick : checkpointer_tick;

		pin_begin();
		MyAuxProcType = CheckpointerProcess;
		if (role) {
			MyBackendType = B_BG_WRITER;
			MyAuxProcType = BgWriterProcess;
		}
		job = foundation_open(role ? WAL_INPUT_JOB_PI_REBUILD : WAL_INPUT_JOB_RETAINED_CUT);
		UT_ASSERT_EQ(cluster_wal_read_pin_acquire_job_v1(threads, 1, job, &pin), CLUSTER_WAL_PIN_OK);
		g_seven_result = CLUSTER_LOCK_ACQUIRE_OK_GRANTED;
		UT_ASSERT(cluster_cf_lock(ShareLock));
		UT_ASSERT_EQ(cluster_cf_unlock_job_v1(ShareLock, NULL, &job->cf), CF_JOB_PENDING);
		job_resource_callback(RESOURCE_RELEASE_BEFORE_LOCKS, false, true, job_resource_argument);
		waits = owner_wait_calls;
		tick();
		UT_ASSERT(cluster_lock_owner_request_usable(&pin->locks[0].request));
		UT_ASSERT_EQ(pin_native_refs, 1);
		job_ack(&job->cf);
		tick();
		UT_ASSERT(!cluster_lock_owner_request_usable(&pin->locks[0].request));
		UT_ASSERT_EQ(cluster_wal_inputs_jobs_stop_census_v1(), WAL_INPUT_JOB_PENDING);
		pin_ack_member(&pin->locks[0].request, false);
		tick();
		UT_ASSERT_EQ(cluster_wal_inputs_jobs_stop_census_v1(), WAL_INPUT_JOB_DONE);
		UT_ASSERT_EQ(owner_wait_calls, waits);
		pin = NULL;
		pin_cleanup(&job, &pin);
	}
}

UT_TEST(service_foreign_identity_refuses_before_interrupt_or_business_work)
{
	for (unsigned role = 0; role < 2; ++role) {
		ClusterWalInputsJobV1 *job;
		unsigned interrupts = loop_interrupts;
		volatile bool caught = false;
		void (*tick)(void) = role ? bgwriter_tick : checkpointer_tick;

		pin_begin();
		MyAuxProcType = role ? BgWriterProcess : CheckpointerProcess;
		if (role) MyBackendType = B_BG_WRITER;
		job = foundation_open(role ? WAL_INPUT_JOB_PI_NOTICE : WAL_INPUT_JOB_PI_BATCH);
		++job->owner_pid;
		job_expect_fatal = true;
		PG_TRY(); { tick(); } PG_CATCH(); { caught = true; } PG_END_TRY();
		job_expect_fatal = false;
		--job->owner_pid;
		UT_ASSERT(caught);
		UT_ASSERT_EQ(loop_interrupts, interrupts);
		foundation_finish(&job);
		foundation_end();
	}
}

static ClusterWalReadPinV1 *race_pin;
static void
ack_at_reset(void)
{
	ack_during_reset = NULL;
	pin_ack_member(&race_pin->locks[race_pin->cursor - 1].request, false);
}

UT_TEST(service_ack_at_latch_reset_is_observed_before_interrupt_handling)
{
	for (unsigned role = 0; role < 2; ++role) {
		uint16 threads[] = {1, 2};
		ClusterWalInputsJobV1 *job;
		void (*tick)(void) = role ? bgwriter_tick : checkpointer_tick;

		pin_begin();
		MyAuxProcType = CheckpointerProcess;
		if (role) {
			MyBackendType = B_BG_WRITER;
			MyAuxProcType = BgWriterProcess;
		}
		job = foundation_open(role ? WAL_INPUT_JOB_PI_NOTICE : WAL_INPUT_JOB_PI_BATCH);
		UT_ASSERT_EQ(cluster_wal_read_pin_acquire_job_v1(threads, 2, job, &race_pin), CLUSTER_WAL_PIN_OK);
		job_resource_callback(RESOURCE_RELEASE_BEFORE_LOCKS, false, true, job_resource_argument);
		tick();
		ack_during_reset = ack_at_reset;
		tick();
		UT_ASSERT(ack_during_reset == NULL);
		UT_ASSERT_EQ(pin_native_refs, 1);
		UT_ASSERT(MyLatch->is_set);
		pin_cleanup(&job, &race_pin);
	}
}

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
	setvbuf(stdout, NULL, _IONBF, 0);
	UT_PLAN(9);
	UT_RUN(service_bgwriter_128_acks_progress_without_delay_or_new_wait);
	UT_RUN(service_checkpointer_128_acks_progress_without_checkpoint_timer);
	UT_RUN(service_ack_at_latch_reset_is_observed_before_interrupt_handling);
	UT_RUN(service_stop_loop_128_acks_progress_without_new_deadline);
	UT_RUN(service_bgwriter_quiescence_cannot_hide_retained_or_invalid_debt);
	UT_RUN(service_stop_module_is_passive_and_invalid_debt_beats_pending);
	UT_RUN(service_original_cf_confirmation_precedes_walr_in_both_aux_loops);
	UT_RUN(service_stopped_publication_loop_keeps_original_ack_progress);
	UT_RUN(service_foreign_identity_refuses_before_interrupt_or_business_work);
	UT_DONE();
	return ut_failed_count != 0;
}
