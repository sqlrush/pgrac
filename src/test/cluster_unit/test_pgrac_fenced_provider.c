/*-------------------------------------------------------------------------
 *
 * test_pgrac_fenced_provider.c
 *	  RF-ROOT P4 provider ABI and terminal-verifier tests.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <errno.h>
#include <signal.h>
#include <sys/mman.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>

#include "pgrac_fenced_provider.h"

#undef printf
#undef fprintf
#undef snprintf

#include "unit_test.h"

UT_DEFINE_GLOBALS();

static int descendant_pid_fd = -1;
static uint64_t expected_resolve_deadline;
static volatile uint32 *retry_state;

static PgracFencedProviderResult
test_resolve(const PgracFencedTargetV1 *configured, PgracFencedTargetV1 *resolved,
			 int32 *native_status)
{
	if (pgrac_fenced_provider_callback_deadline_mono_ns() != expected_resolve_deadline)
		return PGRAC_FENCED_PROVIDER_CONFIG_ERROR;
	*resolved = *configured;
	*native_status = 0;
	return PGRAC_FENCED_PROVIDER_OK;
}

static PgracFencedProviderResult
mismatch_resolve(const PgracFencedTargetV1 *configured, PgracFencedTargetV1 *resolved,
				 int32 *native_status)
{
	*resolved = *configured;
	resolved->target_uuid[0] ^= 1;
	*native_status = 44;
	return PGRAC_FENCED_PROVIDER_OK;
}

static PgracFencedProviderResult
slow_resolve(const PgracFencedTargetV1 *configured, PgracFencedTargetV1 *resolved,
			 int32 *native_status)
{
	(void)configured;
	(void)resolved;
	(void)native_status;
	(void)usleep(200000);
	return PGRAC_FENCED_PROVIDER_OK;
}

static PgracFencedProviderResult
test_actuate(const PgracFencedTargetV1 *target, uint64_t deadline_mono_ns, int32 *native_status)
{
	(void)target;
	(void)deadline_mono_ns;
	*native_status = 0;
	return PGRAC_FENCED_PROVIDER_OK;
}

static PgracFencedProviderResult
test_readback(const PgracFencedTargetV1 *target, uint64_t deadline_mono_ns,
			  PgracFencedReadbackV1 *out)
{
	(void)deadline_mono_ns;
	memset(out, 0, sizeof(*out));
	out->state = PGRAC_FENCED_TARGET_OFF;
	out->io_drain_state = PGRAC_FENCED_IO_DRAIN_DRAINED;
	memcpy(out->observed_target_uuid, target->target_uuid, sizeof(out->observed_target_uuid));
	return PGRAC_FENCED_PROVIDER_OK;
}

static PgracFencedProviderResult
retry_readback(const PgracFencedTargetV1 *target, uint64_t deadline_mono_ns,
			   PgracFencedReadbackV1 *out)
{
	retry_state[0]++;
	if (retry_state[0] <= retry_state[1])
		return PGRAC_FENCED_PROVIDER_UNKNOWN;
	return test_readback(target, deadline_mono_ns, out);
}

static void
test_shutdown(void)
{}

static PgracFencedProviderResult
crash_actuate(const PgracFencedTargetV1 *target, uint64_t deadline_mono_ns, int32 *native_status)
{
	(void)target;
	(void)deadline_mono_ns;
	(void)native_status;
	_exit(9);
}

static PgracFencedProviderResult
slow_actuate(const PgracFencedTargetV1 *target, uint64_t deadline_mono_ns, int32 *native_status)
{
	(void)target;
	(void)deadline_mono_ns;
	(void)native_status;
	(void)usleep(200000);
	return PGRAC_FENCED_PROVIDER_OK;
}

static PgracFencedProviderResult
tree_actuate(const PgracFencedTargetV1 *target, uint64_t deadline_mono_ns, int32 *native_status)
{
	pid_t child;

	(void)target;
	(void)deadline_mono_ns;
	(void)native_status;
	child = fork();
	if (child < 0)
		_exit(91);
	if (child == 0) {
		pid_t self = getpid();

		(void)signal(SIGTERM, SIG_IGN);
		if (descendant_pid_fd < 0 || write(descendant_pid_fd, &self, sizeof(self)) != sizeof(self))
			_exit(92);
		for (;;)
			pause();
	}
	for (;;)
		pause();
}

static PgracFencedProviderResult
success_with_lingering_descendant_actuate(const PgracFencedTargetV1 *target,
										  uint64_t deadline_mono_ns, int32 *native_status)
{
	char ready;
	int ready_pipe[2];
	pid_t child;
	ssize_t got;

	(void)target;
	(void)deadline_mono_ns;
	if (pipe(ready_pipe) != 0)
		return PGRAC_FENCED_PROVIDER_UNKNOWN;
	child = fork();
	if (child < 0) {
		(void)close(ready_pipe[0]);
		(void)close(ready_pipe[1]);
		return PGRAC_FENCED_PROVIDER_UNKNOWN;
	}
	if (child == 0) {
		pid_t self = getpid();

		(void)close(ready_pipe[0]);
		(void)signal(SIGTERM, SIG_IGN);
		if (descendant_pid_fd < 0 || write(descendant_pid_fd, &self, sizeof(self)) != sizeof(self)
			|| write(ready_pipe[1], "R", 1) != 1)
			_exit(92);
		(void)close(ready_pipe[1]);
		for (;;)
			pause();
	}
	(void)close(ready_pipe[1]);
	do {
		got = read(ready_pipe[0], &ready, 1);
	} while (got < 0 && errno == EINTR);
	(void)close(ready_pipe[0]);
	if (got != 1)
		return PGRAC_FENCED_PROVIDER_UNKNOWN;
	*native_status = 0;
	return PGRAC_FENCED_PROVIDER_OK;
}

static uint64_t
deadline_after_ms(uint64_t milliseconds)
{
	struct timespec now;

	UT_ASSERT_EQ(clock_gettime(CLOCK_MONOTONIC, &now), 0);
	return (uint64_t)now.tv_sec * UINT64_C(1000000000) + (uint64_t)now.tv_nsec
		   + milliseconds * UINT64_C(1000000);
}

static bool
process_gone(pid_t pid)
{
	int attempts;

	for (attempts = 0; attempts < 100; attempts++) {
		if (kill(pid, 0) != 0 && errno == ESRCH)
			return true;
		(void)usleep(10000);
	}
	return false;
}

static void
make_test_ops(PgracFencedProviderOpsV1 *ops)
{
	memset(ops, 0, sizeof(*ops));
	ops->abi_version = PGRAC_FENCED_PROVIDER_ABI_V1;
	ops->struct_size = sizeof(*ops);
	ops->provider_id = PGRAC_FENCED_PROVIDER_ID_TEST_ONLY;
	ops->provider_name = "test-only";
	ops->resolve = test_resolve;
	ops->actuate_off = test_actuate;
	ops->readback = test_readback;
	ops->actuate_on = test_actuate;
	ops->shutdown = test_shutdown;
}

UT_TEST(test_provider_abi_exact_layout)
{
	UT_ASSERT_EQ(sizeof(PgracFencedReadbackV1), 32);
	UT_ASSERT_EQ(offsetof(PgracFencedReadbackV1, state), 0);
	UT_ASSERT_EQ(offsetof(PgracFencedReadbackV1, io_drain_state), 4);
	UT_ASSERT_EQ(offsetof(PgracFencedReadbackV1, native_status), 8);
	UT_ASSERT_EQ(offsetof(PgracFencedReadbackV1, reserved0), 12);
	UT_ASSERT_EQ(offsetof(PgracFencedReadbackV1, observed_target_uuid), 16);
}

UT_TEST(test_production_registry_honors_fexecve_gate)
{
	const PgracFencedProviderOpsV1 *ops;

	UT_ASSERT_NULL(pgrac_fenced_provider_lookup(PGRAC_FENCED_PROVIDER_ID_UNAVAILABLE));
	UT_ASSERT_NULL(pgrac_fenced_provider_lookup(PGRAC_FENCED_PROVIDER_ID_TEST_ONLY));
	ops = pgrac_fenced_provider_lookup(UINT16_C(0x0100));
#ifdef HAVE_FEXECVE
	UT_ASSERT_NOT_NULL(ops);
	if (ops == NULL)
		return;
	UT_ASSERT_EQ(ops->provider_id, UINT16_C(0x0100));
	UT_ASSERT_STR_EQ(ops->provider_name, "ipmi-lanplus-v1");
	UT_ASSERT(pgrac_fenced_provider_ops_valid(ops, false));
#else
	UT_ASSERT_NULL(ops);
#endif
	UT_ASSERT_NULL(pgrac_fenced_provider_lookup(UINT16_MAX));
}

UT_TEST(test_test_only_ops_require_explicit_test_validation)
{
	PgracFencedProviderOpsV1 ops;

	make_test_ops(&ops);
	UT_ASSERT(!pgrac_fenced_provider_ops_valid(&ops, false));
	UT_ASSERT(pgrac_fenced_provider_ops_valid(&ops, true));
	ops.abi_version = 2;
	UT_ASSERT(!pgrac_fenced_provider_ops_valid(&ops, true));
	make_test_ops(&ops);
	ops.reserved0 = 1;
	UT_ASSERT(!pgrac_fenced_provider_ops_valid(&ops, true));
	make_test_ops(&ops);
	ops.readback = NULL;
	UT_ASSERT(!pgrac_fenced_provider_ops_valid(&ops, true));
	make_test_ops(&ops);
	ops.provider_name = "provider-name-that-is-longer-than-31-bytes";
	UT_ASSERT(!pgrac_fenced_provider_ops_valid(&ops, true));
}

UT_TEST(test_recovery_terminal_matrix_is_exact)
{
	PgracFencedReadbackV1 readback;
	uint8 expected_uuid[16];

	memset(expected_uuid, 0x42, sizeof(expected_uuid));
	memset(&readback, 0, sizeof(readback));
	memcpy(readback.observed_target_uuid, expected_uuid, sizeof(expected_uuid));
	readback.state = PGRAC_FENCED_TARGET_OFF;
	readback.io_drain_state = PGRAC_FENCED_IO_DRAIN_DRAINED;
	UT_ASSERT_EQ(
		pgrac_fenced_provider_classify_recovery(PGRAC_FENCED_PROVIDER_OK, expected_uuid, &readback),
		PGRAC_FENCED_PROVIDER_TERMINAL_PROVEN);

	readback.io_drain_state = PGRAC_FENCED_IO_DRAIN_NOT_DRAINED;
	UT_ASSERT_EQ(
		pgrac_fenced_provider_classify_recovery(PGRAC_FENCED_PROVIDER_OK, expected_uuid, &readback),
		PGRAC_FENCED_PROVIDER_TERMINAL_REJECTED);
	readback.state = PGRAC_FENCED_TARGET_ON;
	readback.io_drain_state = PGRAC_FENCED_IO_DRAIN_DRAINED;
	UT_ASSERT_EQ(
		pgrac_fenced_provider_classify_recovery(PGRAC_FENCED_PROVIDER_OK, expected_uuid, &readback),
		PGRAC_FENCED_PROVIDER_TERMINAL_REJECTED);
	readback.state = PGRAC_FENCED_TARGET_UNKNOWN;
	UT_ASSERT_EQ(
		pgrac_fenced_provider_classify_recovery(PGRAC_FENCED_PROVIDER_OK, expected_uuid, &readback),
		PGRAC_FENCED_PROVIDER_TERMINAL_UNKNOWN);
	readback.state = PGRAC_FENCED_TARGET_OFF;
	readback.io_drain_state = PGRAC_FENCED_IO_DRAIN_DRAINED;
	readback.observed_target_uuid[0] ^= 1;
	UT_ASSERT_EQ(
		pgrac_fenced_provider_classify_recovery(PGRAC_FENCED_PROVIDER_OK, expected_uuid, &readback),
		PGRAC_FENCED_PROVIDER_TERMINAL_REJECTED);
	UT_ASSERT_EQ(pgrac_fenced_provider_classify_recovery(PGRAC_FENCED_PROVIDER_IO_ERROR,
														 expected_uuid, &readback),
				 PGRAC_FENCED_PROVIDER_TERMINAL_UNKNOWN);
	UT_ASSERT_EQ(pgrac_fenced_provider_classify_recovery(PGRAC_FENCED_PROVIDER_UNAVAILABLE,
														 expected_uuid, &readback),
				 PGRAC_FENCED_PROVIDER_TERMINAL_UNAVAILABLE);
}

UT_TEST(test_rejoin_on_requires_exact_on_drained)
{
	PgracFencedReadbackV1 readback;
	uint8 expected_uuid[16];

	memset(expected_uuid, 0x24, sizeof(expected_uuid));
	memset(&readback, 0, sizeof(readback));
	memcpy(readback.observed_target_uuid, expected_uuid, sizeof(expected_uuid));
	readback.state = PGRAC_FENCED_TARGET_ON;
	readback.io_drain_state = PGRAC_FENCED_IO_DRAIN_DRAINED;
	UT_ASSERT_EQ(pgrac_fenced_provider_classify_rejoin_on(PGRAC_FENCED_PROVIDER_OK, expected_uuid,
														  &readback),
				 PGRAC_FENCED_PROVIDER_TERMINAL_PROVEN);
	readback.io_drain_state = PGRAC_FENCED_IO_DRAIN_UNKNOWN;
	UT_ASSERT_EQ(pgrac_fenced_provider_classify_rejoin_on(PGRAC_FENCED_PROVIDER_OK, expected_uuid,
														  &readback),
				 PGRAC_FENCED_PROVIDER_TERMINAL_UNKNOWN);
	readback.state = PGRAC_FENCED_TARGET_OFF;
	readback.io_drain_state = PGRAC_FENCED_IO_DRAIN_DRAINED;
	UT_ASSERT_EQ(pgrac_fenced_provider_classify_rejoin_on(PGRAC_FENCED_PROVIDER_OK, expected_uuid,
														  &readback),
				 PGRAC_FENCED_PROVIDER_TERMINAL_REJECTED);
}

UT_TEST(test_worker_actuation_and_readback_return_exact_results)
{
	PgracFencedProviderOpsV1 ops;
	PgracFencedTargetV1 target;
	PgracFencedReadbackV1 readback;
	PgracFencedProviderResult result;
	int32 native_status = -1;

	make_test_ops(&ops);
	memset(&target, 0, sizeof(target));
	memset(target.target_uuid, 0x6a, sizeof(target.target_uuid));
	target.victim_node_id = 2;
	target.mapping_generation = 7;
	UT_ASSERT_EQ(pgrac_fenced_provider_worker_actuate(
					 &ops, true, false, &target, deadline_after_ms(1000), &result, &native_status),
				 PGRAC_FENCED_PROVIDER_WORKER_OK);
	UT_ASSERT_EQ(result, PGRAC_FENCED_PROVIDER_OK);
	UT_ASSERT_EQ(native_status, 0);

	UT_ASSERT_EQ(pgrac_fenced_provider_worker_readback(&ops, true, &target, deadline_after_ms(1000),
													   &result, &readback),
				 PGRAC_FENCED_PROVIDER_WORKER_OK);
	UT_ASSERT_EQ(result, PGRAC_FENCED_PROVIDER_OK);
	UT_ASSERT_EQ(readback.state, PGRAC_FENCED_TARGET_OFF);
	UT_ASSERT_EQ(readback.io_drain_state, PGRAC_FENCED_IO_DRAIN_DRAINED);
	UT_ASSERT(memcmp(readback.observed_target_uuid, target.target_uuid, sizeof(target.target_uuid))
			  == 0);
}

UT_TEST(test_worker_readback_retries_on_exact_backoff_schedule)
{
	PgracFencedProviderOpsV1 ops;
	PgracFencedTargetV1 target;
	PgracFencedReadbackV1 readback;
	PgracFencedProviderResult result;
	PgracFencedProviderWorkerResult worker_result;
	uint64_t started;
	uint64_t finished;

	make_test_ops(&ops);
	ops.readback = retry_readback;
	memset(&target, 0, sizeof(target));
	memset(target.target_uuid, 0x6b, sizeof(target.target_uuid));
	target.victim_node_id = 2;
	target.mapping_generation = 7;
	retry_state[0] = 0;
	retry_state[1] = 2;
	started = deadline_after_ms(0);
	worker_result = pgrac_fenced_provider_worker_readback_retry(
		&ops, true, &target, deadline_after_ms(2000), &result, &readback);
	finished = deadline_after_ms(0);
	UT_ASSERT_EQ(worker_result, PGRAC_FENCED_PROVIDER_WORKER_OK);
	UT_ASSERT_EQ(result, PGRAC_FENCED_PROVIDER_OK);
	UT_ASSERT_EQ(retry_state[0], 3);
	UT_ASSERT(finished >= started + UINT64_C(250000000));
	UT_ASSERT_EQ(readback.state, PGRAC_FENCED_TARGET_OFF);
	UT_ASSERT_EQ(readback.io_drain_state, PGRAC_FENCED_IO_DRAIN_DRAINED);
}

UT_TEST(test_worker_resolve_inherits_deadline_and_rebuilds_local_target)
{
	PgracFencedProviderOpsV1 ops;
	PgracFencedTargetV1 configured;
	PgracFencedTargetV1 resolved;
	PgracFencedProviderResult result;
	uint8 adapter_config[4] = { 1, 2, 3, 4 };
	uint64_t deadline = deadline_after_ms(1000);
	int32 native_status = -1;

	make_test_ops(&ops);
	memset(&configured, 0, sizeof(configured));
	memset(configured.target_uuid, 0x71, sizeof(configured.target_uuid));
	configured.victim_node_id = 7;
	configured.mapping_generation = 19;
	configured.adapter_config = adapter_config;
	configured.adapter_config_len = sizeof(adapter_config);
	expected_resolve_deadline = deadline;
	memset(&resolved, 0x7f, sizeof(resolved));
	UT_ASSERT_EQ(pgrac_fenced_provider_worker_resolve(&ops, true, &configured, deadline, &result,
													  &resolved, &native_status),
				 PGRAC_FENCED_PROVIDER_WORKER_OK);
	UT_ASSERT_EQ(result, PGRAC_FENCED_PROVIDER_OK);
	UT_ASSERT_EQ(native_status, 0);
	UT_ASSERT(memcmp(resolved.target_uuid, configured.target_uuid, sizeof(configured.target_uuid))
			  == 0);
	UT_ASSERT_EQ(resolved.victim_node_id, configured.victim_node_id);
	UT_ASSERT_EQ(resolved.mapping_generation, configured.mapping_generation);
	UT_ASSERT(resolved.adapter_config == configured.adapter_config);
	UT_ASSERT_EQ(resolved.adapter_config_len, configured.adapter_config_len);
}

UT_TEST(test_worker_resolve_rejects_mismatch_and_times_out)
{
	PgracFencedProviderOpsV1 ops;
	PgracFencedTargetV1 configured;
	PgracFencedTargetV1 resolved;
	PgracFencedProviderResult result;
	int32 native_status = -1;

	make_test_ops(&ops);
	memset(&configured, 0, sizeof(configured));
	memset(configured.target_uuid, 0x72, sizeof(configured.target_uuid));
	configured.victim_node_id = 8;
	configured.mapping_generation = 20;
	ops.resolve = mismatch_resolve;
	UT_ASSERT_EQ(pgrac_fenced_provider_worker_resolve(&ops, true, &configured,
													  deadline_after_ms(1000), &result, &resolved,
													  &native_status),
				 PGRAC_FENCED_PROVIDER_WORKER_OK);
	UT_ASSERT_EQ(result, PGRAC_FENCED_PROVIDER_REJECTED);
	UT_ASSERT_EQ(resolved.mapping_generation, 0);

	ops.resolve = slow_resolve;
	result = PGRAC_FENCED_PROVIDER_OK;
	UT_ASSERT_EQ(pgrac_fenced_provider_worker_resolve(&ops, true, &configured,
													  deadline_after_ms(20), &result, &resolved,
													  &native_status),
				 PGRAC_FENCED_PROVIDER_WORKER_TIMEOUT);
	UT_ASSERT_NE(result, PGRAC_FENCED_PROVIDER_OK);
	UT_ASSERT_EQ(resolved.mapping_generation, 0);
}

UT_TEST(test_worker_crash_and_timeout_never_return_provider_success)
{
	PgracFencedProviderOpsV1 ops;
	PgracFencedTargetV1 target;
	PgracFencedProviderResult result = PGRAC_FENCED_PROVIDER_OK;
	int32 native_status = 0;

	make_test_ops(&ops);
	memset(&target, 0, sizeof(target));
	memset(target.target_uuid, 0x39, sizeof(target.target_uuid));
	target.mapping_generation = 8;
	ops.actuate_off = crash_actuate;
	UT_ASSERT_EQ(pgrac_fenced_provider_worker_actuate(
					 &ops, true, false, &target, deadline_after_ms(1000), &result, &native_status),
				 PGRAC_FENCED_PROVIDER_WORKER_CRASHED);
	UT_ASSERT_NE(result, PGRAC_FENCED_PROVIDER_OK);

	make_test_ops(&ops);
	ops.actuate_off = slow_actuate;
	result = PGRAC_FENCED_PROVIDER_OK;
	UT_ASSERT_EQ(pgrac_fenced_provider_worker_actuate(
					 &ops, true, false, &target, deadline_after_ms(20), &result, &native_status),
				 PGRAC_FENCED_PROVIDER_WORKER_TIMEOUT);
	UT_ASSERT_NE(result, PGRAC_FENCED_PROVIDER_OK);
}

UT_TEST(test_worker_timeout_drains_descendant_process_group)
{
	PgracFencedProviderOpsV1 ops;
	PgracFencedTargetV1 target;
	PgracFencedProviderResult result = PGRAC_FENCED_PROVIDER_OK;
	int32 native_status = 0;
	int pid_pipe[2];
	int pipe_rc;
	pid_t descendant = -1;
	ssize_t got;
	bool gone;

	make_test_ops(&ops);
	memset(&target, 0, sizeof(target));
	memset(target.target_uuid, 0x5a, sizeof(target.target_uuid));
	target.mapping_generation = 13;
	ops.actuate_off = tree_actuate;
	pipe_rc = pipe(pid_pipe);
	UT_ASSERT_EQ(pipe_rc, 0);
	if (pipe_rc != 0)
		return;
	descendant_pid_fd = pid_pipe[1];
	UT_ASSERT_EQ(pgrac_fenced_provider_worker_actuate(
					 &ops, true, false, &target, deadline_after_ms(20), &result, &native_status),
				 PGRAC_FENCED_PROVIDER_WORKER_TIMEOUT);
	(void)close(pid_pipe[1]);
	descendant_pid_fd = -1;
	do {
		got = read(pid_pipe[0], &descendant, sizeof(descendant));
	} while (got < 0 && errno == EINTR);
	(void)close(pid_pipe[0]);
	UT_ASSERT_EQ(got, sizeof(descendant));
	if (got != sizeof(descendant))
		return;
	gone = process_gone(descendant);
	if (!gone)
		(void)kill(descendant, SIGKILL);
	UT_ASSERT(gone);
}

UT_TEST(test_worker_success_drains_descendants_after_leader_reap)
{
	PgracFencedProviderOpsV1 ops;
	PgracFencedTargetV1 target;
	int pid_pipe[2];
	int status = 0;
	int attempts;
	pid_t descendant = -1;
	pid_t runner;
	pid_t waited = 0;
	ssize_t got;
	bool gone;

	make_test_ops(&ops);
	memset(&target, 0, sizeof(target));
	memset(target.target_uuid, 0x5b, sizeof(target.target_uuid));
	target.mapping_generation = 14;
	ops.actuate_off = success_with_lingering_descendant_actuate;
	UT_ASSERT_EQ(pipe(pid_pipe), 0);
	descendant_pid_fd = pid_pipe[1];
	runner = fork();
	UT_ASSERT(runner >= 0);
	if (runner == 0) {
		PgracFencedProviderResult result = PGRAC_FENCED_PROVIDER_UNKNOWN;
		int32 native_status = -1;
		PgracFencedProviderWorkerResult worker_result;

		(void)close(pid_pipe[0]);
		worker_result = pgrac_fenced_provider_worker_actuate(
			&ops, true, false, &target, deadline_after_ms(3000), &result, &native_status);
		(void)close(pid_pipe[1]);
		_exit(worker_result == PGRAC_FENCED_PROVIDER_WORKER_OK && result == PGRAC_FENCED_PROVIDER_OK
					  && native_status == 0
				  ? 0
				  : 1);
	}
	(void)close(pid_pipe[1]);
	descendant_pid_fd = -1;
	for (attempts = 0; attempts < 400; attempts++) {
		waited = waitpid(runner, &status, WNOHANG);
		if (waited != 0)
			break;
		(void)usleep(10000);
	}
	if (waited == 0) {
		(void)kill(runner, SIGKILL);
		do {
			waited = waitpid(runner, &status, 0);
		} while (waited < 0 && errno == EINTR);
	}
	do {
		got = read(pid_pipe[0], &descendant, sizeof(descendant));
	} while (got < 0 && errno == EINTR);
	(void)close(pid_pipe[0]);
	if (got == sizeof(descendant) && !process_gone(descendant))
		(void)kill(descendant, SIGKILL);
	gone = got == sizeof(descendant) && process_gone(descendant);
	UT_ASSERT_EQ(waited, runner);
	UT_ASSERT(attempts < 400);
	UT_ASSERT(WIFEXITED(status));
	UT_ASSERT_EQ(WIFEXITED(status) ? WEXITSTATUS(status) : -1, 0);
	UT_ASSERT_EQ(got, sizeof(descendant));
	UT_ASSERT(gone);
}

UT_TEST(test_worker_rejects_test_provider_without_test_gate)
{
	PgracFencedProviderOpsV1 ops;
	PgracFencedTargetV1 target;
	PgracFencedProviderResult result = PGRAC_FENCED_PROVIDER_OK;
	int32 native_status = 0;

	make_test_ops(&ops);
	memset(&target, 0, sizeof(target));
	UT_ASSERT_EQ(pgrac_fenced_provider_worker_actuate(
					 &ops, false, false, &target, deadline_after_ms(1000), &result, &native_status),
				 PGRAC_FENCED_PROVIDER_WORKER_UNAVAILABLE);
	UT_ASSERT_EQ(result, PGRAC_FENCED_PROVIDER_UNAVAILABLE);
}

int
main(void)
{
	retry_state
		= mmap(NULL, sizeof(uint32) * 2, PROT_READ | PROT_WRITE, MAP_SHARED | MAP_ANON, -1, 0);
	if (retry_state == MAP_FAILED)
		return 1;
	UT_PLAN(13);
	UT_RUN(test_provider_abi_exact_layout);
	UT_RUN(test_production_registry_honors_fexecve_gate);
	UT_RUN(test_test_only_ops_require_explicit_test_validation);
	UT_RUN(test_recovery_terminal_matrix_is_exact);
	UT_RUN(test_rejoin_on_requires_exact_on_drained);
	UT_RUN(test_worker_actuation_and_readback_return_exact_results);
	UT_RUN(test_worker_readback_retries_on_exact_backoff_schedule);
	UT_RUN(test_worker_resolve_inherits_deadline_and_rebuilds_local_target);
	UT_RUN(test_worker_resolve_rejects_mismatch_and_times_out);
	UT_RUN(test_worker_crash_and_timeout_never_return_provider_success);
	UT_RUN(test_worker_timeout_drains_descendant_process_group);
	UT_RUN(test_worker_success_drains_descendants_after_leader_reap);
	UT_RUN(test_worker_rejects_test_provider_without_test_gate);
	UT_DONE();
	(void)munmap((void *)retry_state, sizeof(uint32) * 2);
	return ut_failed_count == 0 ? 0 : 1;
}
