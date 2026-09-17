/*-------------------------------------------------------------------------
 *
 * test_pgrac_fenced_ipmi_exec.c
 *    Fixed-descriptor IPMI command runner tests.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <errno.h>
#include <fcntl.h>
#include <signal.h>
#include <time.h>
#include <unistd.h>

#include "pgrac_fenced_ipmi.h"

#undef printf
#undef fprintf
#undef snprintf

#include "unit_test.h"

UT_DEFINE_GLOBALS();

int fexecve(int fd, char *const argv[], char *const envp[]);

static const char password_path[]
	= "/etc/pgrac/credentials/ipmi-00112233445566778899aabbccddeeff.password";
static int descendant_pid_fd = -1;

typedef struct ProcessTreePids {
	pid_t worker;
	pid_t command;
	pid_t descendant;
	pid_t process_group;
} ProcessTreePids;

static bool
exec_environment_exact(char *const envp[])
{
	return envp != NULL && envp[0] != NULL && envp[1] != NULL && envp[2] == NULL
		   && strcmp(envp[0], "LC_ALL=C") == 0 && strcmp(envp[1], "LANG=C") == 0;
}

int
fexecve(int fd, char *const argv[], char *const envp[])
{
	static const char guid[] = " 00 01 02 03 04 05 06 07 08 09 0a 0b 0c 0d 0e 0f\n";
	static const char status[] = "Chassis Power is off\n";
	char overflow[PGRAC_FENCED_IPMI_ACTION_OUTPUT_MAX + 1];

	if (fd < 0 || argv == NULL || !exec_environment_exact(envp) || strcmp(argv[0], "ipmitool") != 0
		|| argv[16] != NULL)
		_exit(90);
	if (strcmp(argv[10], "worker-tree") == 0) {
		pid_t command = getpid();
		pid_t worker = getppid();
		pid_t child = fork();

		if (child < 0)
			_exit(91);
		if (child == 0) {
			ProcessTreePids pids;

			(void)signal(SIGTERM, SIG_IGN);
			pids.worker = worker;
			pids.command = command;
			pids.descendant = getpid();
			pids.process_group = getpgrp();
			if (descendant_pid_fd < 0
				|| write(descendant_pid_fd, &pids, sizeof(pids)) != sizeof(pids))
				_exit(92);
			for (;;)
				pause();
		}
		for (;;)
			pause();
	}
	if (getpgrp() != getpid())
		_exit(90);
	if (strcmp(argv[10], "slow") == 0) {
		for (;;)
			pause();
	}
	if (strcmp(argv[10], "tree") == 0) {
		pid_t child = fork();

		if (child < 0)
			_exit(91);
		if (child == 0) {
			pid_t self = getpid();

			(void)signal(SIGTERM, SIG_IGN);
			if (descendant_pid_fd < 0
				|| write(descendant_pid_fd, &self, sizeof(self)) != sizeof(self))
				_exit(92);
			for (;;)
				pause();
		}
		for (;;)
			pause();
	}
	if (strcmp(argv[10], "fail") == 0)
		_exit(7);
	if (strcmp(argv[10], "stderr") == 0) {
		(void)write(STDERR_FILENO, "warning\n", 8);
		_exit(0);
	}
	if (strcmp(argv[10], "overflow") == 0) {
		memset(overflow, 'x', sizeof(overflow));
		(void)write(STDOUT_FILENO, overflow, sizeof(overflow));
		_exit(0);
	}
	if (strcmp(argv[15], "0x37") == 0)
		(void)write(STDOUT_FILENO, guid, sizeof(guid) - 1);
	else if (strcmp(argv[15], "status") == 0)
		(void)write(STDOUT_FILENO, status, sizeof(status) - 1);
	else
		(void)write(STDOUT_FILENO, "action\n", 7);
	_exit(0);
}

static uint64
deadline_after_ms(uint64 milliseconds)
{
	struct timespec now;

	UT_ASSERT_EQ(clock_gettime(CLOCK_MONOTONIC, &now), 0);
	return (uint64)now.tv_sec * UINT64_C(1000000000) + (uint64)now.tv_nsec
		   + milliseconds * UINT64_C(1000000);
}

static void
make_adapter(PgracFencedIpmiAdapterV1 *adapter)
{
	memset(adapter, 0, sizeof(*adapter));
	adapter->address_family = 4;
	adapter->address[0] = 192;
	adapter->address[2] = 2;
	adapter->address[3] = 10;
	adapter->port = 623;
	adapter->cipher_suite = 17;
}

static PgracFencedProviderResult
nested_test_resolve(const PgracFencedTargetV1 *configured, PgracFencedTargetV1 *resolved,
					int32 *native_status)
{
	*resolved = *configured;
	*native_status = 0;
	return PGRAC_FENCED_PROVIDER_OK;
}

static PgracFencedProviderResult
nested_test_actuate(const PgracFencedTargetV1 *target, uint64_t deadline_mono_ns,
					int32 *native_status)
{
	PgracFencedIpmiAdapterV1 adapter;
	PgracFencedIpmiCommandOutputV1 output;
	PgracFencedIpmiInvocationV1 invocation;
	int fd;
	bool ok;

	(void)target;
	*native_status = 0;
	make_adapter(&adapter);
	if (!pgrac_fenced_ipmi_invocation_build(&adapter, "worker-tree", password_path,
											PGRAC_FENCED_IPMI_COMMAND_OFF, &invocation))
		return PGRAC_FENCED_PROVIDER_CONFIG_ERROR;
	fd = open("/usr/bin/true", O_RDONLY);
	if (fd < 0)
		return PGRAC_FENCED_PROVIDER_CONFIG_ERROR;
	ok = pgrac_fenced_ipmi_command_run_fd(fd, &invocation, deadline_mono_ns, &output);
	(void)close(fd);
	return ok ? PGRAC_FENCED_PROVIDER_OK : PGRAC_FENCED_PROVIDER_UNKNOWN;
}

static PgracFencedProviderResult
nested_test_readback(const PgracFencedTargetV1 *target, uint64_t deadline_mono_ns,
					 PgracFencedReadbackV1 *out)
{
	(void)target;
	(void)deadline_mono_ns;
	memset(out, 0, sizeof(*out));
	return PGRAC_FENCED_PROVIDER_UNKNOWN;
}

static void
nested_test_shutdown(void)
{}

static void
make_nested_test_ops(PgracFencedProviderOpsV1 *ops)
{
	memset(ops, 0, sizeof(*ops));
	ops->abi_version = PGRAC_FENCED_PROVIDER_ABI_V1;
	ops->struct_size = sizeof(*ops);
	ops->provider_id = PGRAC_FENCED_PROVIDER_ID_TEST_ONLY;
	ops->provider_name = "nested-test";
	ops->resolve = nested_test_resolve;
	ops->actuate_off = nested_test_actuate;
	ops->readback = nested_test_readback;
	ops->actuate_on = nested_test_actuate;
	ops->shutdown = nested_test_shutdown;
}

static bool
run_command(const char *username, PgracFencedIpmiCommand command, uint64 timeout_ms,
			PgracFencedIpmiCommandOutputV1 *output)
{
	PgracFencedIpmiAdapterV1 adapter;
	PgracFencedIpmiInvocationV1 invocation;
	int fd;
	bool ok;

	make_adapter(&adapter);
	UT_ASSERT(pgrac_fenced_ipmi_invocation_build(&adapter, username, password_path, command,
												 &invocation));
	fd = open("/usr/bin/true", O_RDONLY);
	UT_ASSERT(fd >= 0);
	if (fd < 0)
		return false;
	ok = pgrac_fenced_ipmi_command_run_fd(fd, &invocation, deadline_after_ms(timeout_ms), output);
	(void)close(fd);
	return ok;
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

static size_t
make_target_adapter(uint8 *bytes)
{
	static const char path[] = "/usr/bin/true";
	size_t path_len = sizeof(path) - 1;
	size_t total_len = PGRAC_FENCED_IPMI_ADAPTER_FIXED_BYTES + path_len;

	memset(bytes, 0, PGRAC_FENCED_IPMI_ADAPTER_MAX_BYTES);
	memcpy(bytes, PGRAC_FENCED_IPMI_ADAPTER_MAGIC, 4);
	bytes[4] = PGRAC_FENCED_IPMI_ADAPTER_VERSION;
	bytes[6] = (uint8)total_len;
	bytes[7] = (uint8)(total_len >> 8);
	bytes[8] = 4;
	bytes[10] = 0x6f;
	bytes[11] = 0x02;
	bytes[12] = 192;
	bytes[14] = 2;
	bytes[15] = 10;
	bytes[28] = 17;
	bytes[30] = (uint8)path_len;
	memset(bytes + 32, 0x5a, 32);
	memcpy(bytes + PGRAC_FENCED_IPMI_ADAPTER_FIXED_BYTES, path, path_len);
	return total_len;
}

static size_t
make_exact_target_adapter(uint8 *bytes)
{
	size_t len = make_target_adapter(bytes);
	int fd = open("/usr/bin/true", O_RDONLY);

	UT_ASSERT(fd >= 0);
	if (fd >= 0) {
		UT_ASSERT(pgrac_fenced_ipmi_sha256_fd(fd, bytes + 32));
		(void)close(fd);
	}
	return len;
}

UT_TEST(test_runner_uses_fexecve_and_sanitized_environment)
{
	PgracFencedIpmiCommandOutputV1 output;
	uint8 uuid[16];

	UT_ASSERT(run_command("admin", PGRAC_FENCED_IPMI_COMMAND_GUID, 1000, &output));
	UT_ASSERT_EQ(output.exit_code, 0);
	UT_ASSERT_EQ(output.stderr_len, 0);
	UT_ASSERT(pgrac_fenced_ipmi_guid_result_parse(output.exit_code, output.stdout_bytes,
												  output.stdout_len, output.stderr_bytes,
												  output.stderr_len, uuid));
	UT_ASSERT_EQ(uuid[15], 15);
}

UT_TEST(test_runner_preserves_nonzero_and_stderr_for_strict_parser)
{
	PgracFencedIpmiCommandOutputV1 output;

	UT_ASSERT(run_command("fail", PGRAC_FENCED_IPMI_COMMAND_OFF, 1000, &output));
	UT_ASSERT_EQ(output.exit_code, 7);
	UT_ASSERT(!pgrac_fenced_ipmi_action_result_validate(output.exit_code, output.stdout_bytes,
														output.stdout_len, output.stderr_bytes,
														output.stderr_len));
	UT_ASSERT(run_command("stderr", PGRAC_FENCED_IPMI_COMMAND_OFF, 1000, &output));
	UT_ASSERT_EQ(output.exit_code, 0);
	UT_ASSERT_EQ(output.stderr_len, 8);
	UT_ASSERT(!pgrac_fenced_ipmi_action_result_validate(output.exit_code, output.stdout_bytes,
														output.stdout_len, output.stderr_bytes,
														output.stderr_len));
}

UT_TEST(test_runner_rejects_overflow_and_deadline)
{
	PgracFencedIpmiCommandOutputV1 output;

	UT_ASSERT(!run_command("overflow", PGRAC_FENCED_IPMI_COMMAND_OFF, 1000, &output));
	UT_ASSERT(!run_command("slow", PGRAC_FENCED_IPMI_COMMAND_OFF, 20, &output));
}

UT_TEST(test_runner_deadline_drains_descendant_process_group)
{
	PgracFencedIpmiCommandOutputV1 output;
	int pid_pipe[2];
	int pipe_rc;
	pid_t descendant = -1;
	ssize_t got;
	bool gone;

	pipe_rc = pipe(pid_pipe);
	UT_ASSERT_EQ(pipe_rc, 0);
	if (pipe_rc != 0)
		return;
	descendant_pid_fd = pid_pipe[1];
	UT_ASSERT(!run_command("tree", PGRAC_FENCED_IPMI_COMMAND_OFF, 20, &output));
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

UT_TEST(test_provider_deadline_owns_nested_command_descendants)
{
	PgracFencedProviderOpsV1 ops;
	PgracFencedTargetV1 target;
	PgracFencedProviderResult result = PGRAC_FENCED_PROVIDER_OK;
	ProcessTreePids pids;
	int32 native_status = 0;
	int pid_pipe[2];
	int pipe_rc;
	ssize_t got;
	bool command_gone;
	bool descendant_gone;

	make_nested_test_ops(&ops);
	memset(&target, 0, sizeof(target));
	memset(target.target_uuid, 0x61, sizeof(target.target_uuid));
	target.victim_node_id = 9;
	target.mapping_generation = 21;
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
		got = read(pid_pipe[0], &pids, sizeof(pids));
	} while (got < 0 && errno == EINTR);
	(void)close(pid_pipe[0]);
	UT_ASSERT_EQ(got, sizeof(pids));
	if (got != sizeof(pids))
		return;
	command_gone = process_gone(pids.command);
	descendant_gone = process_gone(pids.descendant);
	if (!command_gone || !descendant_gone)
		(void)kill(-pids.process_group, SIGKILL);
	UT_ASSERT_EQ(pids.process_group, pids.worker);
	UT_ASSERT(command_gone);
	UT_ASSERT(descendant_gone);
}

UT_TEST(test_provider_callbacks_fail_closed_before_any_command)
{
	const PgracFencedProviderOpsV1 *ops = pgrac_fenced_ipmi_provider_ops();
	PgracFencedTargetV1 configured;
	PgracFencedTargetV1 resolved;
	PgracFencedReadbackV1 readback;
	uint8 adapter[PGRAC_FENCED_IPMI_ADAPTER_MAX_BYTES];
	int32 native_status = -1;
	size_t i;

	UT_ASSERT_NOT_NULL(ops);
	if (ops == NULL)
		return;
	memset(&configured, 0, sizeof(configured));
	for (i = 0; i < sizeof(configured.target_uuid); i++)
		configured.target_uuid[i] = (uint8)i;
	configured.target_uuid[0] = 1;
	configured.victim_node_id = 2;
	configured.mapping_generation = 9;
	configured.adapter_config = adapter;
	configured.adapter_config_len = make_target_adapter(adapter);
	memset(&resolved, 0x7f, sizeof(resolved));
	UT_ASSERT_EQ(ops->resolve(&configured, &resolved, &native_status),
				 PGRAC_FENCED_PROVIDER_CONFIG_ERROR);
	UT_ASSERT_EQ(resolved.mapping_generation, 0);
	UT_ASSERT_EQ(ops->actuate_off(&configured, deadline_after_ms(1000), &native_status),
				 PGRAC_FENCED_PROVIDER_CONFIG_ERROR);
	memset(&readback, 0x7f, sizeof(readback));
	UT_ASSERT_EQ(ops->readback(&configured, deadline_after_ms(1000), &readback),
				 PGRAC_FENCED_PROVIDER_CONFIG_ERROR);
	UT_ASSERT_EQ(readback.state, 0);
}

UT_TEST(test_result_folding_requires_fresh_guid_before_status)
{
	PgracFencedIpmiCommandOutputV1 guid_output;
	PgracFencedIpmiCommandOutputV1 status_output;
	PgracFencedIpmiCommandOutputV1 action_output;
	PgracFencedTargetV1 configured;
	PgracFencedTargetV1 resolved;
	PgracFencedReadbackV1 readback;
	int32 native_status = -1;
	size_t i;

	memset(&configured, 0, sizeof(configured));
	for (i = 0; i < sizeof(configured.target_uuid); i++)
		configured.target_uuid[i] = (uint8)i;
	configured.victim_node_id = 3;
	configured.mapping_generation = 11;
	UT_ASSERT(run_command("admin", PGRAC_FENCED_IPMI_COMMAND_GUID, 1000, &guid_output));
	UT_ASSERT_EQ(
		pgrac_fenced_ipmi_resolve_result(&configured, &guid_output, &resolved, &native_status),
		PGRAC_FENCED_PROVIDER_OK);
	UT_ASSERT_EQ(resolved.victim_node_id, configured.victim_node_id);
	UT_ASSERT_EQ(resolved.mapping_generation, configured.mapping_generation);
	UT_ASSERT(memcmp(resolved.target_uuid, configured.target_uuid, 16) == 0);
	UT_ASSERT(run_command("admin", PGRAC_FENCED_IPMI_COMMAND_STATUS, 1000, &status_output));
	UT_ASSERT_EQ(
		pgrac_fenced_ipmi_readback_results(&configured, &guid_output, &status_output, &readback),
		PGRAC_FENCED_PROVIDER_OK);
	UT_ASSERT_EQ(readback.state, PGRAC_FENCED_TARGET_OFF);
	UT_ASSERT_EQ(readback.io_drain_state, PGRAC_FENCED_IO_DRAIN_UNKNOWN);
	guid_output.stdout_bytes[2] = '1';
	UT_ASSERT_EQ(
		pgrac_fenced_ipmi_readback_results(&configured, &guid_output, &status_output, &readback),
		PGRAC_FENCED_PROVIDER_UNKNOWN);
	UT_ASSERT_EQ(readback.state, 0);
	UT_ASSERT(run_command("admin", PGRAC_FENCED_IPMI_COMMAND_OFF, 1000, &action_output));
	UT_ASSERT_EQ(pgrac_fenced_ipmi_action_result(&action_output, &native_status),
				 PGRAC_FENCED_PROVIDER_OK);
	UT_ASSERT_EQ(native_status, 0);
}

UT_TEST(test_prevalidated_execution_rehashes_each_command)
{
	PgracFencedIpmiCommandOutputV1 output;
	PgracFencedTargetV1 target;
	uint8 adapter[PGRAC_FENCED_IPMI_ADAPTER_MAX_BYTES];
	size_t i;

	memset(&target, 0, sizeof(target));
	for (i = 0; i < sizeof(target.target_uuid); i++)
		target.target_uuid[i] = (uint8)i;
	target.victim_node_id = 4;
	target.mapping_generation = 12;
	target.adapter_config = adapter;
	target.adapter_config_len = make_exact_target_adapter(adapter);
	UT_ASSERT_EQ(pgrac_fenced_ipmi_execute_prevalidated(&target, "admin", password_path,
														PGRAC_FENCED_IPMI_COMMAND_GUID,
														deadline_after_ms(1000), &output),
				 PGRAC_FENCED_PROVIDER_OK);
	UT_ASSERT_EQ(output.stdout_len, 49);
	UT_ASSERT_EQ(pgrac_fenced_ipmi_execute_prevalidated(&target, "admin", password_path,
														PGRAC_FENCED_IPMI_COMMAND_STATUS,
														deadline_after_ms(1000), &output),
				 PGRAC_FENCED_PROVIDER_OK);
	UT_ASSERT_EQ(output.stdout_len, sizeof("Chassis Power is off\n") - 1);
	adapter[32] ^= 1;
	UT_ASSERT_EQ(pgrac_fenced_ipmi_execute_prevalidated(&target, "admin", password_path,
														PGRAC_FENCED_IPMI_COMMAND_OFF,
														deadline_after_ms(1000), &output),
				 PGRAC_FENCED_PROVIDER_CONFIG_ERROR);
}

int
main(void)
{
	UT_PLAN(8);
	UT_RUN(test_runner_uses_fexecve_and_sanitized_environment);
	UT_RUN(test_runner_preserves_nonzero_and_stderr_for_strict_parser);
	UT_RUN(test_runner_rejects_overflow_and_deadline);
	UT_RUN(test_runner_deadline_drains_descendant_process_group);
	UT_RUN(test_provider_deadline_owns_nested_command_descendants);
	UT_RUN(test_provider_callbacks_fail_closed_before_any_command);
	UT_RUN(test_result_folding_requires_fresh_guid_before_status);
	UT_RUN(test_prevalidated_execution_rehashes_each_command);
	UT_DONE();

	return ut_failed_count == 0 ? 0 : 1;
}
