/*-------------------------------------------------------------------------
 * cf_wait.c
 *    Hold the real control-file enqueue for a bounded TAP contention case.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 * IDENTIFICATION
 *    src/test/cluster_tap/pre2/cf_wait.c
 * NOTES
 *    PGRAC test module, loaded only by an isolated TAP fixture. It neither
 *    writes the control file nor replaces the original enqueue owner.
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "fmgr.h"
#include "miscadmin.h"
#include "cluster/cluster_cf_enqueue.h"
#include "utils/timestamp.h"

PG_MODULE_MAGIC;
PG_FUNCTION_INFO_V1(pre2_cf_hold);

Datum
pre2_cf_hold(PG_FUNCTION_ARGS)
{
	int32 milliseconds = PG_GETARG_INT32(0);
	TimestampTz until;
	uint64 cookie;
	ClusterCfReleaseResult released;

	if (milliseconds < 1 || milliseconds > 5000)
		ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						errmsg("CF test hold must be between 1 and 5000 milliseconds")));
	if (!cluster_cf_lock(ExclusiveLock))
		ereport(ERROR,
				(errcode(ERRCODE_LOCK_NOT_AVAILABLE), errmsg("CF test holder was not granted")));
	cookie = cluster_cf_owner_cookie(ExclusiveLock);
	until = TimestampTzPlusMilliseconds(GetCurrentTimestamp(), milliseconds);
	PG_TRY();
	{
		ereport(LOG, (errmsg("CF TAP hold granted"),
					  errdetail("pid=%d cookie=" UINT64_FORMAT " hold_ms=%d", MyProcPid, cookie,
								milliseconds)));
		while (GetCurrentTimestamp() < until) {
			CHECK_FOR_INTERRUPTS();
			pg_usleep(10000L);
		}
	}
	PG_CATCH();
	{
		cluster_cf_unlock(ExclusiveLock);
		PG_RE_THROW();
	}
	PG_END_TRY();
	released = cluster_cf_unlock_confirmed(ExclusiveLock);
	if (released != CLUSTER_CF_RELEASE_CONFIRMED)
		ereport(ERROR, (errcode(ERRCODE_LOCK_NOT_AVAILABLE),
						errmsg("CF test holder release was not confirmed"),
						errdetail("cookie=" UINT64_FORMAT " result=%d", cookie, (int)released)));
	PG_RETURN_VOID();
}
