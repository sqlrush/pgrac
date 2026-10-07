/* Author: SqlRush <sqlrush@gmail.com> */
/* Actual checkpointer shutdown handler; native disk, signals and protocol
 * completion are boundary fixtures, not a full-cluster shutdown witness. */
#include "postgres.h"
#include "access/xlog.h"
#include "cluster/cluster_clean_leave.h"
#include "cluster/cluster_wal_state.h"
#include "cluster/cluster_recovery_duty.h"
#include "../../backend/cluster/cluster_control_root_private.h"
#include "cluster/cluster_wal_thread.h"
#include "cluster/cluster_pi_writeback.h"
#include "postmaster/interrupt.h"
#include "storage/procsignal.h"
#include "storage/ipc.h"
#include "utils/memutils.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "utils/guc.h"
#include "storage/latch.h"
#include "utils/wait_event.h"
#include <setjmp.h>

#undef printf
#undef fprintf
#include "unit_test.h"
UT_DEFINE_GLOBALS();

static jmp_buf exit_boundary;
static int exit_code, checkpoint_calls, stopped_calls, complete_calls, prepare_calls;
static int old_prepare_calls, old_close_calls, old_drain_calls, thread_close_calls;
static int report_calls, config_calls;
static bool pi_scope_retained;
static unsigned pi_scope_releases;
static bool retry_apply, retry_applied, shared_config_saw_retry;
static bool requested, prepare_ok, stopped_ok, complete_ok, fail_checkpoint, fail_at_finish;
static bool native_mode, native_stopped_ok;
static unsigned native_stopped_calls;
bool cluster_shared_config;
static bool v2_ref_ok, v2_observe_ok;
static unsigned v2_observe_calls;
static unsigned v2_cas_left, v2_waits;
static bool cancel_on_wait;
static Latch fixture_latch;
Latch *MyLatch = &fixture_latch;
static ClusterNormalStopFailure failure;
static ClusterPhase1FullStopPrepareResult old_prepare_result;
static char trace[32];
static int trace_pos;
void
ExceptionalCondition(const char *condition, const char *file, int line)
{
	fprintf(stderr, "unexpected production assertion: %s (%s:%d)\n", condition, file, line);
	abort();
}
bool ExitOnAnyError;
volatile sig_atomic_t ShutdownRequestPending, ConfigReloadPending, ProcSignalBarrierPending;
volatile sig_atomic_t LogMemoryContextPending;
PgStat_CheckpointerStats PendingCheckpointerStats;

static void
record(char c)
{
	trace[trace_pos++] = c;
	trace[trace_pos] = 0;
}
void
cluster_pi_writeback_checkpointer_release_v1(void)
{
	pi_scope_retained = false;
	pi_scope_releases++;
	record('R');
}
static void
UpdateSharedMemoryConfig(void)
{
	config_calls++;
	shared_config_saw_retry = retry_applied;
}
bool
cluster_shared_config_delivery_retry_idle(void)
{
	retry_applied = retry_apply;
	return retry_applied;
}
void
ProcessProcSignalBarrier(void)
{
	ProcSignalBarrierPending = false;
}
void
ProcessConfigFile(GucContext context)
{
	(void)context;
}
void
ProcessLogMemoryContextInterrupt(void)
{
	LogMemoryContextPending = false;
}
bool
cluster_normal_stop_requested(void)
{
	return requested;
}
ClusterNormalStopFailure
cluster_normal_stop_failure(void)
{
	return failure;
}
void
cluster_normal_stop_fail(ClusterNormalStopFailure f)
{
	if (failure == 0)
		failure = f;
}
bool
cluster_normal_stop_checkpoint_prepare(ClusterPhase1FullStopPlan *p,
									   ClusterNormalStopModuleObservation *o)
{
	prepare_calls++;
	record('P');
	memset(p, 0, sizeof(*p));
	p->valid = prepare_ok;
	p->epoch = 9;
	p->attempt_nonce = 101;
	p->absolute_deadline_us = 5000000;
	o->module = "FIXTURE_OWNER";
	o->reason = "FIXTURE_PENDING_OR_FAILURE";
	return prepare_ok;
}
bool
cluster_normal_stop_checkpoint_complete(ClusterPhase1FullStopPlan *p,
										ClusterNormalStopModuleObservation *o)
{
	(void)o;
	complete_calls++;
	record('C');
	UT_ASSERT_EQ(p->epoch, 9);
	UT_ASSERT_EQ(p->attempt_nonce, 101);
	UT_ASSERT_EQ(p->absolute_deadline_us, 5000000);
	if (fail_at_finish)
		failure = CLUSTER_NORMAL_STOP_FAILURE_SERVICE;
	return complete_ok;
}
ClusterPhase1FullStopPrepareResult
cluster_clean_leave_phase1_full_stop_prepare_exact(ClusterPhase1FullStopPlan *p)
{
	old_prepare_calls++;
	record('p');
	memset(p, 0, sizeof(*p));
	p->valid = old_prepare_result == CLUSTER_PHASE1_FULL_STOP_READY;
	return old_prepare_result;
}
bool
cluster_clean_leave_phase1_full_stop_close_exact(ClusterPhase1FullStopPlan *p)
{
	(void)p;
	old_close_calls++;
	record('c');
	return true;
}
bool
cluster_clean_leave_shutdown_drain(void)
{
	old_drain_calls++;
	record('d');
	return true;
}
bool
cluster_control_root_thread_clean_close_publish_retry(void)
{
	thread_close_calls++;
	record('t');
	return true;
}
void
ShutdownXLOG(int code, Datum arg)
{
	(void)code;
	(void)arg;
	UT_ASSERT(ExitOnAnyError);
	UT_ASSERT(!pi_scope_retained);
	checkpoint_calls++;
	record('W');
	if (fail_checkpoint) {
		exit_code = 1;
		longjmp(exit_boundary, 1);
	}
}
bool
cluster_wal_state_publish_stopped(void)
{
	stopped_calls++;
	record('S');
	return stopped_ok;
}

bool
cluster_wal_thread_current_v2_ref(ClusterWalSourceRef *out)
{
	memset(out, 0, sizeof(*out));
	out->claim.identity.origin_thread_id = 1;
	return v2_ref_ok;
}

ClusterControlRootResult
cluster_control_root_v3_shutdown_observe(const ClusterWalSourceRef *ref,
										 ClusterControlRootSnapshot *out,
										 ClusterControlRootFileToken *token)
{
	UT_ASSERT_EQ(checkpoint_calls, 1);
	UT_ASSERT_EQ(ref->claim.identity.origin_thread_id, 1);
	memset(out, 0, sizeof(*out));
	memset(token, 0, sizeof(*token));
	v2_observe_calls++;
	record('V');
	if (v2_cas_left != 0) {
		--v2_cas_left;
		return CLUSTER_CONTROL_ROOT_CAS_CONFLICT;
	}
	return v2_observe_ok ? CLUSTER_CONTROL_ROOT_OK_PRIMARY : CLUSTER_CONTROL_ROOT_IO_ERROR;
}

int
WaitLatch(Latch *latch, int events, long timeout, uint32 wait_event)
{
	UT_ASSERT(latch == MyLatch);
	UT_ASSERT((events & WL_EXIT_ON_PM_DEATH) != 0);
	UT_ASSERT_EQ(timeout, 20);
	UT_ASSERT_EQ(wait_event, WAIT_EVENT_CHECKPOINTER_MAIN);
	v2_waits++;
	return WL_TIMEOUT;
}
void
ResetLatch(Latch *latch)
{
	UT_ASSERT(latch == MyLatch);
}
bool
cluster_normal_stop_native_wal_mode(void)
{
	return native_mode;
}
bool
cluster_native_wal_shutdown_observe(bool stopped, int64 *started_at)
{
	UT_ASSERT(stopped);
	UT_ASSERT_EQ(checkpoint_calls, 1);
	native_stopped_calls++;
	record('N');
	*started_at = native_stopped_ok ? 555 : 0;
	return native_stopped_ok;
}
void
pgstat_report_checkpointer(void)
{
	report_calls++;
}
void
pgstat_report_wal(bool force)
{
	UT_ASSERT(force);
	report_calls++;
}
void
proc_exit(int code)
{
	exit_code = code;
	record('E');
	longjmp(exit_boundary, 1);
}

#undef ereport
#define ereport(level, fields)                                                                     \
	do {                                                                                           \
		if ((level) >= ERROR) {                                                                    \
			exit_code = 1;                                                                         \
			record('!');                                                                           \
			longjmp(exit_boundary, 1);                                                             \
		}                                                                                          \
	} while (0)
#undef CHECK_FOR_INTERRUPTS
#define CHECK_FOR_INTERRUPTS()                                                                     \
	do {                                                                                           \
		if (cancel_on_wait && v2_waits != 0) {                                                     \
			exit_code = 1;                                                                         \
			longjmp(exit_boundary, 1);                                                             \
		}                                                                                          \
	} while (0)
#include "test_cluster_checkpointer_stop.inc"

static void
reset_fixture(void)
{
	retry_apply = retry_applied = shared_config_saw_retry = false;
	cluster_shared_config = false;
	v2_ref_ok = v2_observe_ok = true;
	v2_observe_calls = 0;
	v2_cas_left = v2_waits = 0;
	cancel_on_wait = false;
	native_mode = false;
	native_stopped_ok = true;
	native_stopped_calls = 0;
	requested = true;
	prepare_ok = stopped_ok = complete_ok = true;
	fail_checkpoint = fail_at_finish = false;
	failure = 0;
	old_prepare_result = CLUSTER_PHASE1_FULL_STOP_NOT_APPLICABLE;
	ShutdownRequestPending = true;
	ConfigReloadPending = false;
	ProcSignalBarrierPending = LogMemoryContextPending = false;
	ExitOnAnyError = false;
	memset(&PendingCheckpointerStats, 0, sizeof(PendingCheckpointerStats));
	checkpoint_calls = stopped_calls = complete_calls = prepare_calls = 0;
	old_prepare_calls = old_close_calls = old_drain_calls = thread_close_calls = 0;
	report_calls = config_calls = trace_pos = 0;
	pi_scope_retained = true;
	pi_scope_releases = 0;
	trace[0] = 0;
	exit_code = -1;
}
static void
run_handler(void)
{
	if (setjmp(exit_boundary) == 0)
		HandleCheckpointerInterrupts();
}
static void
assert_no_fallback(void)
{
	UT_ASSERT_EQ(old_prepare_calls, 0);
	UT_ASSERT_EQ(old_close_calls, 0);
	UT_ASSERT_EQ(old_drain_calls, 0);
	UT_ASSERT_EQ(thread_close_calls, 0);
}

UT_TEST(no_shutdown_is_not_a_stop_request)
{
	reset_fixture();
	ShutdownRequestPending = false;
	run_handler();
	UT_ASSERT_EQ(exit_code, -1);
	UT_ASSERT_EQ(checkpoint_calls, 0);
	UT_ASSERT(pi_scope_retained);
	UT_ASSERT_EQ(pi_scope_releases, 0);
	assert_no_fallback();
}
UT_TEST(idle_retry_updates_native_shared_configuration)
{
	for (unsigned pending = 0; pending < 2; ++pending) {
		reset_fixture();
		ShutdownRequestPending = false;
		ConfigReloadPending = pending != 0;
		retry_apply = true;
		run_handler();
		UT_ASSERT(retry_applied);
		UT_ASSERT(shared_config_saw_retry);
		UT_ASSERT_EQ(checkpoint_calls, 0);
	}
}
UT_TEST(original_noncurrent_path_is_unchanged)
{
	reset_fixture();
	requested = false;
	run_handler();
	UT_ASSERT_EQ(exit_code, 0);
	UT_ASSERT(strcmp(trace, "RpWSdtE") == 0);
	UT_ASSERT_EQ(prepare_calls, 0);
	UT_ASSERT_EQ(complete_calls, 0);
	reset_fixture();
	requested = false;
	old_prepare_result = CLUSTER_PHASE1_FULL_STOP_READY;
	run_handler();
	UT_ASSERT_EQ(exit_code, 0);
	UT_ASSERT(strcmp(trace, "RpWScE") == 0);
}
UT_TEST(current_calls_real_shutdown_between_two_current_barriers)
{
	reset_fixture();
	run_handler();
	assert_no_fallback();
	UT_ASSERT_EQ(exit_code, 0);
	UT_ASSERT(strcmp(trace, "RPWSCE") == 0);
	UT_ASSERT_EQ(pi_scope_releases, 1);
	UT_ASSERT_EQ(PendingCheckpointerStats.requested_checkpoints, 1);
	UT_ASSERT_EQ(report_calls, 2);
}
UT_TEST(current_prepare_failure_never_checkpoints_or_falls_back)
{
	reset_fixture();
	prepare_ok = false;
	run_handler();
	assert_no_fallback();
	UT_ASSERT_EQ(exit_code, 1);
	UT_ASSERT_EQ(checkpoint_calls, 0);
	UT_ASSERT_EQ(stopped_calls, 0);
	UT_ASSERT_EQ(complete_calls, 0);
}
UT_TEST(checkpoint_error_cannot_publish_stopped)
{
	reset_fixture();
	fail_checkpoint = true;
	run_handler();
	assert_no_fallback();
	UT_ASSERT_EQ(exit_code, 1);
	UT_ASSERT_EQ(stopped_calls, 0);
	UT_ASSERT_EQ(complete_calls, 0);
}
UT_TEST(stopped_failure_cannot_close_or_exit_zero)
{
	reset_fixture();
	stopped_ok = false;
	run_handler();
	assert_no_fallback();
	UT_ASSERT_EQ(exit_code, 1);
	UT_ASSERT_EQ(checkpoint_calls, 1);
	UT_ASSERT_EQ(complete_calls, 0);
	UT_ASSERT(failure != 0);
}
UT_TEST(post_checkpoint_failure_never_uses_survivor_handoff)
{
	reset_fixture();
	complete_ok = false;
	run_handler();
	assert_no_fallback();
	UT_ASSERT_EQ(exit_code, 1);
	UT_ASSERT_EQ(complete_calls, 1);
	UT_ASSERT_EQ(report_calls, 0);
}
UT_TEST(late_failure_overrides_protocol_ready)
{
	reset_fixture();
	fail_at_finish = true;
	run_handler();
	assert_no_fallback();
	UT_ASSERT_EQ(exit_code, 1);
	UT_ASSERT_EQ(complete_calls, 1);
	UT_ASSERT_EQ(report_calls, 0);
}
UT_TEST(preexisting_failure_cannot_run_shutdown_checkpoint)
{
	reset_fixture();
	failure = CLUSTER_NORMAL_STOP_FAILURE_SERVICE;
	run_handler();
	assert_no_fallback();
	UT_ASSERT_EQ(exit_code, 1);
	UT_ASSERT_EQ(checkpoint_calls, 0);
}
UT_TEST(native_calls_real_shutdown_before_own_durable_observer)
{
	reset_fixture();
	native_mode = true;
	stopped_ok = false; /* registry is not configured */
	run_handler();
	UT_ASSERT_EQ(exit_code, 0);
	UT_ASSERT(strcmp(trace, "RPWNCE") == 0);
	UT_ASSERT_EQ(stopped_calls, 0);
	UT_ASSERT_EQ(native_stopped_calls, 1);
	assert_no_fallback();
}
UT_TEST(native_control_or_checkpoint_failure_never_closes)
{
	for (int before = 0; before < 2; before++) {
		reset_fixture();
		native_mode = true;
		native_stopped_ok = false;
		fail_checkpoint = before;
		run_handler();
		UT_ASSERT_EQ(exit_code, 1);
		UT_ASSERT_EQ(native_stopped_calls, before ? 0 : 1);
		UT_ASSERT_EQ(stopped_calls, 0);
		UT_ASSERT_EQ(complete_calls, 0);
		assert_no_fallback();
	}
}

UT_TEST(v2_uses_root_stop_evidence_not_flat_registry)
{
	reset_fixture();
	cluster_shared_config = true;
	stopped_ok = false;
	run_handler();
	UT_ASSERT_EQ(exit_code, 0);
	UT_ASSERT(strcmp(trace, "RPWVCE") == 0);
	UT_ASSERT_EQ(stopped_calls, 0);
	UT_ASSERT_EQ(native_stopped_calls, 0);
	UT_ASSERT_EQ(v2_observe_calls, 1);
	assert_no_fallback();
}

UT_TEST(v2_failed_stop_observation_cannot_complete)
{
	for (int fault = 0; fault < 3; ++fault) {
		reset_fixture();
		cluster_shared_config = true;
		v2_ref_ok = fault != 0;
		v2_observe_ok = fault != 1;
		fail_checkpoint = fault == 2;
		run_handler();
		UT_ASSERT_EQ(exit_code, 1);
		UT_ASSERT_EQ(complete_calls, 0);
		UT_ASSERT_EQ(stopped_calls, 0);
		UT_ASSERT_EQ(v2_observe_calls, fault == 1 ? 1 : 0);
		assert_no_fallback();
	}
}

UT_TEST(v2_missing_producer_cut_never_enters_legacy_drain)
{
	reset_fixture();
	cluster_shared_config = true;
	requested = false;
	run_handler();
	UT_ASSERT_EQ(exit_code, 1);
	UT_ASSERT_EQ(checkpoint_calls, 0);
	UT_ASSERT_EQ(v2_observe_calls, 0);
	UT_ASSERT_EQ(stopped_calls, 0);
	assert_no_fallback();
}

UT_TEST(v2_peer_root_publication_reobserves_without_fake_failure)
{
	for (int cancel = 0; cancel < 2; ++cancel) {
		reset_fixture();
		cluster_shared_config = true;
		v2_cas_left = 2;
		cancel_on_wait = cancel;
		run_handler();
		UT_ASSERT_EQ(exit_code, cancel ? 1 : 0);
		UT_ASSERT_EQ(v2_waits, cancel ? 1 : 2);
		UT_ASSERT_EQ(v2_observe_calls, cancel ? 1 : 3);
		UT_ASSERT_EQ(complete_calls, cancel ? 0 : 1);
		UT_ASSERT_EQ(checkpoint_calls, 1);
		UT_ASSERT_EQ(stopped_calls, 0);
		assert_no_fallback();
	}
}
int
main(void)
{
	UT_PLAN(16);
	UT_RUN(no_shutdown_is_not_a_stop_request);
	UT_RUN(idle_retry_updates_native_shared_configuration);
	UT_RUN(original_noncurrent_path_is_unchanged);
	UT_RUN(current_calls_real_shutdown_between_two_current_barriers);
	UT_RUN(current_prepare_failure_never_checkpoints_or_falls_back);
	UT_RUN(checkpoint_error_cannot_publish_stopped);
	UT_RUN(stopped_failure_cannot_close_or_exit_zero);
	UT_RUN(post_checkpoint_failure_never_uses_survivor_handoff);
	UT_RUN(late_failure_overrides_protocol_ready);
	UT_RUN(preexisting_failure_cannot_run_shutdown_checkpoint);
	UT_RUN(native_calls_real_shutdown_before_own_durable_observer);
	UT_RUN(native_control_or_checkpoint_failure_never_closes);
	UT_RUN(v2_uses_root_stop_evidence_not_flat_registry);
	UT_RUN(v2_failed_stop_observation_cannot_complete);
	UT_RUN(v2_missing_producer_cut_never_enters_legacy_drain);
	UT_RUN(v2_peer_root_publication_reobserves_without_fake_failure);
	UT_DONE();
	return ut_failed_count ? 1 : 0;
}
