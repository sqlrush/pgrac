/*-------------------------------------------------------------------------
 * pgrac-fenced-map-verify.c
 *    Nonprivileged binary filter for authenticated target configuration.
 *
 * Author: SqlRush <sqlrush@gmail.com>
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * IDENTIFICATION
 *    src/bin/pgrac_fenced/pgrac-fenced-map-verify.c
 *
 * NOTES
 *    PGRAC-original. The caller supplies independently pinned expectations.
 *-------------------------------------------------------------------------
 */
#include "postgres_fe.h"

#include "pgrac_fenced_map_filter.h"

int
main(int argc, char **argv)
{
	int result = pgrac_fenced_map_filter(argc, argv, stdin, stdout);

	if (result != 0)
		fputs("PGRAC_MAP_UNVERIFIED\n", stderr);
	return result;
}
