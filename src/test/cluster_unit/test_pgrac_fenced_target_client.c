/*-------------------------------------------------------------------------
 * test_pgrac_fenced_target_client.c
 *    Actual frontend invocation driver for isolated Linux process/TLS tests.
 *
 * Author: SqlRush <sqlrush@gmail.com>
 * Portions Copyright (c) 2026, pgrac contributors
 * IDENTIFICATION
 *    src/test/cluster_unit/test_pgrac_fenced_target_client.c
 *-------------------------------------------------------------------------
 */
#include "postgres_fe.h"
#include <errno.h>
#include <signal.h>
#include <time.h>
#include <unistd.h>
#include "pgrac_fenced_target_client.h"
#undef printf
#undef fprintf
#undef snprintf
#include "unit_test.h"

UT_DEFINE_GLOBALS();

UT_TEST(test_missing_owner_config_cannot_invoke)
{
	char output[PGRAC_TARGET_REPLY_MAX_BYTES + 1];
	size_t length = 99;
	memset(output, 'x', sizeof(output));
	UT_ASSERT(!pgrac_fenced_target_client_exchange(NULL, "{}\n", 3, UINT64_MAX, output,
												   sizeof(output), &length));
	UT_ASSERT_EQ(length, 0);
	UT_ASSERT_EQ(output[0], 0);
}

static int
exchange_driver(int argc, char **argv)
{
	PgracFencedTargetClientPaths paths;
	char command[PGRAC_TARGET_COMMAND_MAX_BYTES + 1];
	char output[PGRAC_TARGET_REPLY_MAX_BYTES + 1];
	size_t received, length = 99;
	uint64 deadline;
	char *end;
	if (argc != 4)
		return 2;
	errno = 0;
	deadline = strtoull(argv[3], &end, 10);
	if (errno != 0 || *end != '\0' || deadline == 0)
		return 2;
	paths.bundle_directory = argv[1];
	paths.config_file = argv[2];
	/* Test driver stands in the outer provider's already isolated group. */
	if (getpgrp() != getpid() && setpgid(0, 0) != 0)
		return 2;
	received = fread(command, 1, sizeof(command), stdin);
	if (ferror(stdin))
		return 2;
	if (!pgrac_fenced_target_client_exchange(&paths, command, received, deadline, output,
											 sizeof(output), &length))
		return output[0] == '\0' && length == 0 ? 1 : 3;
	return fwrite(output, 1, length, stdout) == length && fflush(stdout) == 0 ? 0 : 2;
}

int
main(int argc, char **argv)
{
	if (argc != 1)
		return exchange_driver(argc, argv);
	UT_PLAN(1);
	UT_RUN(test_missing_owner_config_cannot_invoke);
	UT_DONE();
	return ut_failed_count != 0;
}
