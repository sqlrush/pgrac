/*-------------------------------------------------------------------------
 * test_cluster_port_runtime.c
 *   Real portability-library CRC and fail-stop standalone logging boundary.
 *
 * Portions Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *   src/test/cluster_unit/test_cluster_port_runtime.c
 *-------------------------------------------------------------------------
 */
#include "postgres.h"
#include "port/pg_crc32c.h"
#include <signal.h>
#include <sys/resource.h>
#include <sys/wait.h>
#include <unistd.h>
#undef printf
#include "unit_test.h"

UT_DEFINE_GLOBALS();

UT_TEST(test_crc32c_known_answer)
{
	pg_crc32c crc;
	INIT_CRC32C(crc);
	COMP_CRC32C(crc, "123456789", 9);
	FIN_CRC32C(crc);
	UT_ASSERT_EQ(crc, UINT32_C(0xe3069283));
}

UT_TEST(test_only_crc_debug_message_is_ignored)
{
	UT_ASSERT(!errstart(DEBUG1, NULL));
}

UT_TEST(test_unexpected_error_and_warning_abort)
{
	const int levels[] = { WARNING, ERROR };
	for (int i = 0; i < lengthof(levels); i++) {
		pid_t pid = fork();
		int status = 0;
		UT_ASSERT(pid >= 0);
		if (pid < 0)
			return;
		if (pid == 0) {
			struct rlimit limit = { 0, 0 };
			if (setrlimit(RLIMIT_CORE, &limit) != 0)
				_exit(2);
			(void)errstart_cold(levels[i], NULL);
			_exit(0);
		}
		UT_ASSERT_EQ(waitpid(pid, &status, 0), pid);
		UT_ASSERT(WIFSIGNALED(status));
		UT_ASSERT_EQ(WTERMSIG(status), SIGABRT);
	}
}

int
main(void)
{
	UT_PLAN(3);
	UT_RUN(test_crc32c_known_answer);
	UT_RUN(test_only_crc_debug_message_is_ignored);
	UT_RUN(test_unexpected_error_and_warning_abort);
	UT_DONE();
	return ut_failed_count ? 1 : 0;
}
