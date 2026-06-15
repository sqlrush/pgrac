/*-------------------------------------------------------------------------
 *
 * cluster_thread_recovery_driver_srf.c
 *	  pgrac TEST-ONLY SQL entry point driving the online thread-recovery DATA
 *	  DRIVER over a dead thread's per-thread WAL + shared storage, for the
 *	  spec-4.11 increment 3b-1 TAP (t/260).
 *
 *	  cluster_thread_drive_test(dead_tid int4, scan_lower pg_lsn,
 *	                            scan_upper pg_lsn) -> text
 *
 *	  Drives cluster_thread_recovery_drive_data(): it builds a reader over
 *	  <cluster.wal_threads_dir>/thread_<dead_tid> and runs the RMW engine over
 *	  [scan_lower, scan_upper] under the R13 harness, publishing NO authority.
 *	  Returns a ':'-delimited summary the cluster_tap parses:
 *
 *	      <result>:<records_scanned>:<blocks_applied>:<blocks_gated>:
 *	      <blocks_out_of_scope>:<recovered_through>
 *
 *	  where <result> is done / blocked / not_applicable.
 *
 *	  To exercise R13, the cluster_tap arms the cluster-thread-recovery-drive
 *	  injection point with a catchable ERROR (cluster_inject_fault) in the SAME
 *	  session before calling this -- the driver's PG_CATCH demotes it to BLOCKED
 *	  and the backend survives.
 *
 *	  TEST-ONLY: a diagnostic entry point, NOT a product query interface;
 *	  superuser-only.  Mirrors cluster_thread_recovery_replay_srf.c.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * Spec: spec-4.11-thread-recovery.md (FROZEN v0.3)
 *
 * IDENTIFICATION
 *	  src/backend/cluster/cluster_thread_recovery_driver_srf.c
 *
 * NOTES
 *	  This is a pgrac-original file (no derivation from PostgreSQL).
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "fmgr.h"

PG_FUNCTION_INFO_V1(cluster_thread_drive_test);

#ifdef USE_PGRAC_CLUSTER

#include "access/xlogdefs.h"
#include "miscadmin.h"
#include "utils/builtins.h"
#include "utils/pg_lsn.h"

#include "cluster/cluster_thread_recovery.h"

Datum
cluster_thread_drive_test(PG_FUNCTION_ARGS)
{
	int32 dead_tid;
	XLogRecPtr scan_lower;
	XLogRecPtr scan_upper;
	ClusterThreadReplayStats stats;
	ClusterThreadRecResult res;
	const char *result_text;
	char *out;

	if (!superuser())
		ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
						errmsg("cluster_thread_drive_test is superuser-only")));

	if (PG_ARGISNULL(0) || PG_ARGISNULL(1) || PG_ARGISNULL(2))
		ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						errmsg("dead_tid, scan_lower and scan_upper must not be NULL")));

	dead_tid = PG_GETARG_INT32(0);
	scan_lower = PG_GETARG_LSN(1);
	scan_upper = PG_GETARG_LSN(2);

	/*
	 * Out-of-uint16 ids are not a valid thread slot; pass them through to the
	 * driver as the closest invalid id so the driver's range gate returns
	 * BLOCKED (the test asserts fail-closed, not a SQL error).
	 */
	if (dead_tid < 0 || dead_tid > PG_UINT16_MAX)
		dead_tid = 0;

	res = cluster_thread_recovery_drive_data((uint16)dead_tid, scan_lower, scan_upper, &stats);

	switch (res) {
	case CLUSTER_THREADREC_DONE:
		result_text = "done";
		break;
	case CLUSTER_THREADREC_BLOCKED:
		result_text = "blocked";
		break;
	default:
		result_text = "not_applicable";
		break;
	}

	out = psprintf("%s:" UINT64_FORMAT ":" UINT64_FORMAT ":" UINT64_FORMAT ":" UINT64_FORMAT
				   ":%X/%X",
				   result_text, stats.records_scanned, stats.blocks_applied, stats.blocks_gated,
				   stats.blocks_out_of_scope, LSN_FORMAT_ARGS(stats.recovered_through));

	PG_RETURN_TEXT_P(cstring_to_text(out));
}

#else /* !USE_PGRAC_CLUSTER */

Datum
cluster_thread_drive_test(PG_FUNCTION_ARGS pg_attribute_unused())
{
	ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					errmsg("cluster_thread_drive_test requires --enable-cluster")));
}

#endif /* USE_PGRAC_CLUSTER */
