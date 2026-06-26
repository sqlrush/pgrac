/*-------------------------------------------------------------------------
 *
 * cluster_hang_srf.c
 *	  pgrac Hang Manager SQL entry point (spec-5.11 D5).
 *
 *	  pg_cluster_hang_dump(pid int4) -> bool
 *	    Asks the backend with the given pid to log its own cluster wait /
 *	    hang state (mirrors pg_log_backend_memory_contexts).  Superuser-only
 *	    by default (REVOKE in system_functions.sql).
 *
 *	  This entry point lives in its own translation unit so the pg_proc.dat
 *	  symbol resolves at link time in BOTH --enable-cluster and
 *	  --disable-cluster builds: the real implementation is gated by
 *	  USE_PGRAC_CLUSTER and the #else branch raises
 *	  ERRCODE_FEATURE_NOT_SUPPORTED (same contract as the other cluster SRF
 *	  stubs, e.g. cluster_ir_srf.c).
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * Spec: spec-5.11-hang-manager-skeleton.md (D5)
 *
 * IDENTIFICATION
 *	  src/backend/cluster/cluster_hang_srf.c
 *
 * NOTES
 *	  This is a pgrac-original file (no derivation from PostgreSQL).
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "fmgr.h"

PG_FUNCTION_INFO_V1(pg_cluster_hang_dump);

#ifdef USE_PGRAC_CLUSTER

#include "storage/backendid.h"
#include "storage/proc.h"		/* AuxiliaryPidGetProc, PGPROC */
#include "storage/procarray.h"	/* BackendPidGetProc */
#include "storage/procsignal.h" /* SendProcSignal, PROCSIG_CLUSTER_HANG_DUMP */

/*
 * pg_cluster_hang_dump(pid) — request a backend to log its own hang/wait
 * state.  Mirrors pg_log_backend_memory_contexts: WARNING (not ERROR) on a
 * vanished pid so a loop-through-resultset does not abort.
 */
Datum
pg_cluster_hang_dump(PG_FUNCTION_ARGS)
{
	int pid = PG_GETARG_INT32(0);
	const PGPROC *proc;
	BackendId backendId = InvalidBackendId;

	proc = BackendPidGetProc(pid);
	if (proc != NULL)
		backendId = proc->backendId;
	else
		proc = AuxiliaryPidGetProc(pid);

	if (proc == NULL) {
		ereport(WARNING, (errmsg("PID %d is not a PostgreSQL server process", pid)));
		PG_RETURN_BOOL(false);
	}

	if (SendProcSignal(pid, PROCSIG_CLUSTER_HANG_DUMP, backendId) < 0) {
		ereport(WARNING, (errmsg("could not send hang-dump signal to process %d: %m", pid)));
		PG_RETURN_BOOL(false);
	}

	PG_RETURN_BOOL(true);
}

#else /* !USE_PGRAC_CLUSTER */

Datum
pg_cluster_hang_dump(PG_FUNCTION_ARGS pg_attribute_unused())
{
	ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					errmsg("pg_cluster_hang_dump requires --enable-cluster")));
	PG_RETURN_NULL();
}

#endif /* USE_PGRAC_CLUSTER */
