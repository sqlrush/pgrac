/*
 * Execute the actual abort EXIT guard and its native postmaster pipe probe.
 * A real fork/pipe/exit supplies the death observation. Only the PANIC/error
 * sinks are replaced with distinct child exit codes; no transaction abort,
 * WAL, TT terminal, native backend unwind or distributed recovery is claimed.
 * Author: SqlRush <sqlrush@gmail.com>
 */
#include "postgres.h"

#include <fcntl.h>
#include <sys/wait.h>
#include <unistd.h>

#include "postmaster/postmaster.h"

#undef printf
#include "unit_test.h"

UT_DEFINE_GLOBALS();

bool IsUnderPostmaster;
int postmaster_alive_fds[2];
static bool cluster_abort_in_progress;

enum
{
	GUARD_PANIC = 73,
	UNEXPECTED_STAGE = 75,
	SETUP_FAILURE = 76
};

static void cluster_abort_completion_failed(const char *stage) pg_attribute_noreturn();
static void
cluster_abort_completion_failed(const char *stage)
{
	_exit(strcmp(stage, "EXIT") == 0 ? GUARD_PANIC : UNEXPECTED_STAGE);
}

#include "test_cluster_abort_exit_guard.inc"

static int
run_guard(bool armed, bool under_postmaster, bool alive, int pipe_fault, int exit_code)
{
	int fds[2];
	int status;
	pid_t pid;

	if (pipe(fds) < 0)
		return SETUP_FAILURE;
	if (fcntl(fds[0], F_SETFL, O_NONBLOCK) < 0)
	{
		close(fds[0]);
		close(fds[1]);
		return SETUP_FAILURE;
	}
	/* Close before fork so EOF cannot be withheld by the test parent. */
	if (!alive)
	{
		close(fds[1]);
		fds[1] = -1;
	}
	if (pipe_fault == 2 && write(fds[1], "x", 1) != 1)
	{
		close(fds[0]);
		close(fds[1]);
		return SETUP_FAILURE;
	}
	fflush(NULL);
	pid = fork();
	if (pid == 0)
	{
		postmaster_alive_fds[POSTMASTER_FD_WATCH] = fds[0];
		if (pipe_fault == 1)
		{
			close(fds[0]);
			postmaster_alive_fds[POSTMASTER_FD_WATCH] = -1;
		}
		IsUnderPostmaster = under_postmaster;
		cluster_abort_in_progress = armed;
		cluster_abort_exit_guard(exit_code, (Datum) 0);
		_exit(0);
	}
	close(fds[0]);
	if (fds[1] >= 0)
		close(fds[1]);
	if (pid < 0)
		return SETUP_FAILURE;
	while (waitpid(pid, &status, 0) < 0)
		if (errno != EINTR)
			return SETUP_FAILURE;
	return WIFEXITED(status) ? WEXITSTATUS(status) : SETUP_FAILURE;
}

UT_TEST(test_confirmed_postmaster_death_leaves_abort_for_crash_recovery)
{
	UT_ASSERT_EQ(run_guard(true, true, false, false, 1), 2);
}

UT_TEST(test_success_code_cannot_hide_unfinished_abort_after_parent_death)
{
	UT_ASSERT_EQ(run_guard(true, true, false, false, 0), 2);
}

UT_TEST(test_native_crash_exit_status_preserved_for_other_exit_codes)
{
	UT_ASSERT_EQ(run_guard(true, true, false, false, 2), 2);
	UT_ASSERT_EQ(run_guard(true, true, false, false, 7), 2);
}

UT_TEST(test_live_postmaster_still_panics_on_incomplete_abort)
{
	UT_ASSERT_EQ(run_guard(true, true, true, false, 1), GUARD_PANIC);
}

UT_TEST(test_standalone_incomplete_abort_still_panics)
{
	UT_ASSERT_EQ(run_guard(true, false, false, true, 1), GUARD_PANIC);
}

UT_TEST(test_bad_pipe_does_not_become_confirmed_postmaster_death)
{
	UT_ASSERT_EQ(run_guard(true, true, false, 1, 1), GUARD_PANIC);
}

UT_TEST(test_unexpected_pipe_data_does_not_become_confirmed_postmaster_death)
{
	UT_ASSERT_EQ(run_guard(true, true, true, 2, 1), GUARD_PANIC);
}

UT_TEST(test_completed_abort_does_not_read_postmaster_pipe)
{
	UT_ASSERT_EQ(run_guard(false, true, false, true, 0), 0);
}

UT_TEST(test_unarmed_guard_leaves_live_normal_exit_unchanged)
{
	UT_ASSERT_EQ(run_guard(false, true, true, false, 0), 0);
}

int
main(void)
{
	UT_PLAN(9);
	UT_RUN(test_confirmed_postmaster_death_leaves_abort_for_crash_recovery);
	UT_RUN(test_success_code_cannot_hide_unfinished_abort_after_parent_death);
	UT_RUN(test_native_crash_exit_status_preserved_for_other_exit_codes);
	UT_RUN(test_live_postmaster_still_panics_on_incomplete_abort);
	UT_RUN(test_standalone_incomplete_abort_still_panics);
	UT_RUN(test_bad_pipe_does_not_become_confirmed_postmaster_death);
	UT_RUN(test_unexpected_pipe_data_does_not_become_confirmed_postmaster_death);
	UT_RUN(test_completed_abort_does_not_read_postmaster_pipe);
	UT_RUN(test_unarmed_guard_leaves_live_normal_exit_unchanged);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
