/*-------------------------------------------------------------------------
 * pgrac-fenced-drain-sign.c
 *    Non-setuid target signing filter. The owner supplies the secret by pipe.
 *
 * Author: SqlRush <sqlrush@gmail.com>
 * Portions Copyright (c) 2026, pgrac contributors
 * IDENTIFICATION
 *    src/bin/pgrac_fenced/pgrac-fenced-drain-sign.c
 * NOTES
 *    PGRAC-original. No secret arguments, environment or diagnostic output.
 *-------------------------------------------------------------------------
 */
#include "postgres_fe.h"
#ifndef WIN32
#include <sys/resource.h>
#endif
#ifdef __linux__
#include <sys/prctl.h>
#endif
#include "pgrac_fenced_drain_sign_filter.h"

int
main(int argc, char **argv)
{
	int result = 77;
#ifndef WIN32
	struct rlimit limit = { 0, 0 };
	/* A Linux pipe core collector ignores RLIMIT_CORE. Refuse before the
	 * first secret read unless the process itself is non-dumpable. */
	if (setrlimit(RLIMIT_CORE, &limit) == 0
#ifdef __linux__
		&& prctl(PR_SET_DUMPABLE, 0UL, 0UL, 0UL, 0UL) == 0
		&& prctl(PR_GET_DUMPABLE, 0UL, 0UL, 0UL, 0UL) == 0
#endif
		&& setvbuf(stdin, NULL, _IONBF, 0) == 0)
		result = pgrac_fenced_drain_sign_filter(argc, argv, stdin, stdout);
#else
	(void)argc;
	(void)argv;
#endif
	if (result != 0)
		fputs("PGRAC_DRAIN_UNSIGNED\n", stderr);
	return result;
}
