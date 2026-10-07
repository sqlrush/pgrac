/*-------------------------------------------------------------------------
 * probe_pgrac_fenced_cib.c
 *    Read-only native observation smoke; never an isolation qualification.
 *
 * Author: SqlRush <sqlrush@gmail.com>
 * Portions Copyright (c) 2026, pgrac contributors
 * IDENTIFICATION
 *    src/test/cluster_unit/probe_pgrac_fenced_cib.c
 *-------------------------------------------------------------------------
 */
#include "postgres_fe.h"
#include <time.h>
#include "pgrac_fenced_cib.h"

int
main(void)
{
	PgracFencedCibObservation out;
	PgracFencedProviderResult result;
	struct timespec now;
	uint64 deadline;
	if (clock_gettime(CLOCK_MONOTONIC, &now) != 0 || now.tv_sec < 0
		|| (uint64)now.tv_sec > (UINT64_MAX - UINT64_C(6000000000)) / UINT64_C(1000000000))
		return 2;
	deadline = (uint64)now.tv_sec * UINT64_C(1000000000) + now.tv_nsec + UINT64_C(5000000000);
	result = pgrac_fenced_cib_observe(deadline, &out);
	printf("OBSERVATION_ONLY result=%d admin_epoch=" UINT64_FORMAT " epoch=" UINT64_FORMAT
		   " num_updates=" UINT64_FORMAT " configuration_sha256=",
		   result, out.admin_epoch, out.epoch, out.num_updates);
	for (unsigned n = 0; n < sizeof(out.configuration_digest); n++)
		printf("%02x", out.configuration_digest[n]);
	printf("\n");
	return result == PGRAC_FENCED_PROVIDER_OK ? 0 : 1;
}
