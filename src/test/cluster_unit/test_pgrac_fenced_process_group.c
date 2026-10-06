/*-------------------------------------------------------------------------
 *
 * test_pgrac_fenced_process_group.c
 *    Exercise provider worker process-group races through the real executor.
 *
 * IDENTIFICATION
 *    src/test/cluster_unit/test_pgrac_fenced_process_group.c
 *
 * Author: SqlRush <sqlrush@gmail.com>
 * Portions Copyright (c) 2026, pgrac contributors
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <errno.h>
#include <sys/mman.h>
#include <time.h>
#include <unistd.h>

#include "pgrac_fenced_provider.h"

static int fixture_setpgid(pid_t pid, pid_t pgid);
static pid_t fixture_getpgid(pid_t pid);
static pid_t fixture_getsid(pid_t pid);

#define setpgid fixture_setpgid
#define getpgid fixture_getpgid
#define getsid fixture_getsid
#include "../../bin/pgrac_fenced/pgrac_fenced_provider.c"
#undef setpgid
#undef getpgid
#undef getsid

#undef printf
#undef fprintf
#undef snprintf
#include "unit_test.h"
UT_DEFINE_GLOBALS();

typedef enum GroupFault {
	GROUP_PARENT_EXACT,
	GROUP_CHILD_EXACT,
	GROUP_PARENT_MISMATCH,
	GROUP_CHILD_MISMATCH,
	SESSION_PARENT_MISMATCH,
	SESSION_CHILD_MISMATCH,
	GROUP_PARENT_IO_ERROR,
	GROUP_CHILD_IO_ERROR
} GroupFault;

static GroupFault fault;
static pid_t fixture_parent;
static volatile uint32 *callback_release;

static bool
fault_in_child(void)
{
	return fault == GROUP_CHILD_EXACT || fault == GROUP_CHILD_MISMATCH
		   || fault == SESSION_CHILD_MISMATCH || fault == GROUP_CHILD_IO_ERROR;
}

static bool
fault_here(void)
{
	return (getpid() != fixture_parent) == fault_in_child();
}

static int
fixture_setpgid(pid_t pid, pid_t pgid)
{
	int rc = setpgid(pid, pgid);
	int saved_errno = errno;

	if (pid > 0 && fault_in_child())
		*callback_release = 1;
	if (!fault_here()) {
		errno = saved_errno;
		return rc;
	}
	/* Preserve the real OS group. Inject only the redundant call's result. */
	if (rc == 0 || (saved_errno == EPERM && getpgid(pid) == (pid == 0 ? getpid() : pid))) {
		errno = fault == GROUP_PARENT_IO_ERROR || fault == GROUP_CHILD_IO_ERROR ? EIO : EPERM;
		return -1;
	}
	errno = saved_errno;
	return rc;
}

static pid_t
fixture_getpgid(pid_t pid)
{
	pid_t group = getpgid(pid);

	if (fault_here() && (fault == GROUP_PARENT_MISMATCH || fault == GROUP_CHILD_MISMATCH))
		return group == -1 ? -1 : group + 1;
	return group;
}

static pid_t
fixture_getsid(pid_t pid)
{
	pid_t session = getsid(pid);

	/* Preserve pre-fork capture; release after the parent observes the worker. */
	if (pid > 0 && fault_here()) {
		*callback_release = 1;
		if (fault == SESSION_PARENT_MISMATCH || fault == SESSION_CHILD_MISMATCH)
			return session == -1 ? -1 : session + 1;
	}
	return session;
}

static PgracFencedProviderResult
resolve_target(const PgracFencedTargetV1 *target, PgracFencedTargetV1 *resolved, int32 *status)
{
	struct timespec pause = { 0, 1000000 };
	uint64_t now;

	while (*callback_release == 0) {
		if (!monotonic_now_ns(&now) || now >= pgrac_fenced_provider_callback_deadline_mono_ns())
			return PGRAC_FENCED_PROVIDER_UNKNOWN;
		(void)nanosleep(&pause, NULL);
	}
	*resolved = *target;
	*status = 71;
	return PGRAC_FENCED_PROVIDER_OK;
}

static PgracFencedProviderResult
actuate_target(const PgracFencedTargetV1 *target, uint64_t deadline, int32 *status)
{
	(void)target;
	(void)deadline;
	*status = 0;
	return PGRAC_FENCED_PROVIDER_UNKNOWN;
}

static PgracFencedProviderResult
read_target(const PgracFencedTargetV1 *target, uint64_t deadline, PgracFencedReadbackV1 *out)
{
	(void)target;
	(void)deadline;
	memset(out, 0, sizeof(*out));
	return PGRAC_FENCED_PROVIDER_UNKNOWN;
}

static void
shutdown_provider(void)
{}

static void
check_worker(GroupFault injected, PgracFencedProviderWorkerResult expected)
{
	PgracFencedProviderOpsV1 ops = { 0 };
	PgracFencedTargetV1 target = { 0 }, resolved;
	PgracFencedProviderResult result;
	PgracFencedProviderWorkerResult actual;
	uint64_t now;
	int32 status;

	fault = injected;
	fixture_parent = getpid();
	callback_release = mmap(NULL, sizeof(*callback_release), PROT_READ | PROT_WRITE,
							MAP_SHARED | MAP_ANON, -1, 0);
	UT_ASSERT(callback_release != MAP_FAILED);
	if (callback_release == MAP_FAILED)
		return;
	*callback_release = 0;
	ops.abi_version = PGRAC_FENCED_PROVIDER_ABI_V1;
	ops.struct_size = sizeof(ops);
	ops.provider_id = PGRAC_FENCED_PROVIDER_ID_TEST_ONLY;
	ops.provider_name = "group-race";
	ops.resolve = resolve_target;
	ops.actuate_off = actuate_target;
	ops.actuate_on = actuate_target;
	ops.readback = read_target;
	ops.shutdown = shutdown_provider;
	target.target_uuid[0] = 1;
	target.mapping_generation = 1;
	UT_ASSERT(monotonic_now_ns(&now));
	actual = pgrac_fenced_provider_worker_resolve(&ops, true, &target, now + UINT64_C(2000000000),
												  &result, &resolved, &status);
	UT_ASSERT_EQ(actual, expected);
	if (expected == PGRAC_FENCED_PROVIDER_WORKER_OK) {
		UT_ASSERT_EQ(result, PGRAC_FENCED_PROVIDER_OK);
		UT_ASSERT_EQ(status, 71);
		UT_ASSERT(memcmp(&target, &resolved, sizeof(target)) == 0);
	} else
		UT_ASSERT_NE(result, PGRAC_FENCED_PROVIDER_OK);
	UT_ASSERT_EQ(munmap((void *)callback_release, sizeof(*callback_release)), 0);
}

UT_TEST(parent_already_established_group_is_accepted)
{
	check_worker(GROUP_PARENT_EXACT, PGRAC_FENCED_PROVIDER_WORKER_OK);
}
UT_TEST(child_already_established_group_is_accepted)
{
	check_worker(GROUP_CHILD_EXACT, PGRAC_FENCED_PROVIDER_WORKER_OK);
}
UT_TEST(wrong_group_stays_rejected)
{
	check_worker(GROUP_PARENT_MISMATCH, PGRAC_FENCED_PROVIDER_WORKER_UNAVAILABLE);
	check_worker(GROUP_CHILD_MISMATCH, PGRAC_FENCED_PROVIDER_WORKER_CRASHED);
}
UT_TEST(wrong_session_stays_rejected)
{
	check_worker(SESSION_PARENT_MISMATCH, PGRAC_FENCED_PROVIDER_WORKER_UNAVAILABLE);
	check_worker(SESSION_CHILD_MISMATCH, PGRAC_FENCED_PROVIDER_WORKER_CRASHED);
}
UT_TEST(other_errors_stay_rejected)
{
	check_worker(GROUP_PARENT_IO_ERROR, PGRAC_FENCED_PROVIDER_WORKER_UNAVAILABLE);
	check_worker(GROUP_CHILD_IO_ERROR, PGRAC_FENCED_PROVIDER_WORKER_CRASHED);
}

int
main(void)
{
	UT_PLAN(5);
	UT_RUN(parent_already_established_group_is_accepted);
	UT_RUN(child_already_established_group_is_accepted);
	UT_RUN(wrong_group_stays_rejected);
	UT_RUN(wrong_session_stays_rejected);
	UT_RUN(other_errors_stay_rejected);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
