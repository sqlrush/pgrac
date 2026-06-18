/*-------------------------------------------------------------------------
 *
 * cluster_ges_mode_backend.c
 *	  GES enqueue lock-mode encoding -- backend layer.
 *
 *	  Backend-only pieces of the GES mode contract: the startup self-check
 *	  that compares the frozen matrix against PG's live DoLockModesConflict,
 *	  the cluster.ges_mode_selfcheck GUC variable, and the SQL functions
 *	  that expose the matrix and parser to observability / TAP.
 *
 *	    --disable-cluster: the SQL functions become stubs that raise
 *	    ERRCODE_FEATURE_NOT_SUPPORTED so their symbols still resolve.
 *
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/backend/cluster/cluster_ges_mode_backend.c
 *
 * NOTES
 *	  Spec: spec-5.1a-ges-mode-encoding-compat-matrix.md
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "fmgr.h"
#include "utils/builtins.h"

PG_FUNCTION_INFO_V1(pg_cluster_ges_mode_matrix);
PG_FUNCTION_INFO_V1(cluster_ges_mode_compat);
PG_FUNCTION_INFO_V1(cluster_ges_mode_matches_pg);

#ifdef USE_PGRAC_CLUSTER

#include "funcapi.h"
#include "storage/lock.h" /* DoLockModesConflict */

#include "cluster/cluster_ges_mode.h"

/* GUC: cluster.ges_mode_selfcheck (enum off/warn/fatal; default fatal). */
int cluster_ges_mode_selfcheck = GES_MODE_SELFCHECK_FATAL;

#define CLUSTER_GES_MODE_MATRIX_NCOLS 5

/*
 * ges_mode_compat_matches_pg -- does the frozen matrix agree with PG?
 *
 *	Compares ges_modes_compatible() against !DoLockModesConflict() for all
 *	64 (held, wanted) cells.  Returns true only when every cell agrees.
 */
bool
ges_mode_compat_matches_pg(void)
{
	ClusterGesMode held;
	ClusterGesMode wanted;

	for (held = GES_MODE_FIRST; held <= GES_MODE_LAST; held++)
		for (wanted = GES_MODE_FIRST; wanted <= GES_MODE_LAST; wanted++)
			if (ges_modes_compatible(held, wanted)
				!= !DoLockModesConflict(held,
										wanted)) /* GES_MODE_OK: the contract self-check itself */
				return false;
	return true;
}

/*
 * cluster_ges_mode_init -- startup self-check of the frozen matrix.
 *
 *	Runs once at postmaster cluster init.  On any divergence from PG's
 *	live conflict table, fails closed per cluster.ges_mode_selfcheck:
 *	fatal refuses startup, warn logs and continues, off skips entirely.
 *	The check compares two compile-time const tables, so a divergence is
 *	deterministic; FATAL (not PANIC) cleanly refuses startup.
 */
void
cluster_ges_mode_init(void)
{
	ClusterGesMode held;
	ClusterGesMode wanted;

	if (cluster_ges_mode_selfcheck == GES_MODE_SELFCHECK_OFF)
		return;

	for (held = GES_MODE_FIRST; held <= GES_MODE_LAST; held++) {
		for (wanted = GES_MODE_FIRST; wanted <= GES_MODE_LAST; wanted++) {
			bool ours = ges_modes_compatible(held, wanted);
			bool pg = !DoLockModesConflict(
				held, wanted); /* GES_MODE_OK: the contract self-check itself */

			if (ours != pg) {
				int elevel
					= (cluster_ges_mode_selfcheck == GES_MODE_SELFCHECK_FATAL) ? FATAL : WARNING;

				ereport(elevel, (errcode(ERRCODE_INTERNAL_ERROR),
								 errmsg("GES mode compatibility matrix diverged from the lock "
										"conflict table at held=%d wanted=%d",
										held, wanted),
								 errhint("This indicates a build inconsistency; rebuild after any "
										 "change to the lock conflict table.")));
				/* warn: keep scanning so all divergences are reported. */
			}
		}
	}
}

/*
 * pg_cluster_ges_mode_matrix -- dump the frozen 8x8 matrix + DLM aliases.
 */
Datum
pg_cluster_ges_mode_matrix(PG_FUNCTION_ARGS)
{
	ReturnSetInfo *rsinfo = (ReturnSetInfo *)fcinfo->resultinfo;
	Datum values[CLUSTER_GES_MODE_MATRIX_NCOLS];
	bool nulls[CLUSTER_GES_MODE_MATRIX_NCOLS];
	ClusterGesMode held;
	ClusterGesMode wanted;

	InitMaterializedSRF(fcinfo, 0);
	memset(nulls, 0, sizeof(nulls));

	for (held = GES_MODE_FIRST; held <= GES_MODE_LAST; held++) {
		for (wanted = GES_MODE_FIRST; wanted <= GES_MODE_LAST; wanted++) {
			values[0] = CStringGetTextDatum(ges_mode_pg_name(held));
			values[1] = CStringGetTextDatum(ges_mode_pg_name(wanted));
			values[2] = BoolGetDatum(ges_modes_compatible(held, wanted));
			values[3] = CStringGetTextDatum(ges_dlm_mode_name(ges_mode_to_dlm(held)));
			values[4] = CStringGetTextDatum(ges_dlm_mode_name(ges_mode_to_dlm(wanted)));
			tuplestore_putvalues(rsinfo->setResult, rsinfo->setDesc, values, nulls);
		}
	}

	return (Datum)0;
}

/*
 * cluster_ges_mode_compat -- compatibility of two canonical PG mode names.
 *
 *	Accepts only the 8 canonical PG lock-mode names (case-insensitive).
 *	Unknown strings and DLM aliases fail closed with ERRCODE_INVALID_
 *	PARAMETER_VALUE; DLM aliases are display-only and never compat input.
 */
Datum
cluster_ges_mode_compat(PG_FUNCTION_ARGS)
{
	char *held_name = text_to_cstring(PG_GETARG_TEXT_PP(0));
	char *wanted_name = text_to_cstring(PG_GETARG_TEXT_PP(1));
	ClusterGesMode held = ges_mode_from_pg_name(held_name);
	ClusterGesMode wanted = ges_mode_from_pg_name(wanted_name);

	if (held == 0)
		ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						errmsg("unknown GES lock mode name: \"%s\"", held_name),
						errhint("Valid names are AccessShareLock .. AccessExclusiveLock.")));
	if (wanted == 0)
		ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						errmsg("unknown GES lock mode name: \"%s\"", wanted_name),
						errhint("Valid names are AccessShareLock .. AccessExclusiveLock.")));

	PG_RETURN_BOOL(ges_modes_compatible(held, wanted));
}

/*
 * cluster_ges_mode_matches_pg -- SQL wrapper over the matrix self-check.
 */
Datum
cluster_ges_mode_matches_pg(PG_FUNCTION_ARGS)
{
	PG_RETURN_BOOL(ges_mode_compat_matches_pg());
}

#else /* !USE_PGRAC_CLUSTER */

Datum
pg_cluster_ges_mode_matrix(PG_FUNCTION_ARGS)
{
	ereport(ERROR,
			(errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("cluster support is not enabled"),
			 errhint("Rebuild with --enable-cluster to use pg_cluster_ges_mode_matrix().")));
	PG_RETURN_NULL();
}

Datum
cluster_ges_mode_compat(PG_FUNCTION_ARGS)
{
	ereport(ERROR,
			(errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("cluster support is not enabled"),
			 errhint("Rebuild with --enable-cluster to use cluster_ges_mode_compat().")));
	PG_RETURN_NULL();
}

Datum
cluster_ges_mode_matches_pg(PG_FUNCTION_ARGS)
{
	ereport(ERROR,
			(errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("cluster support is not enabled"),
			 errhint("Rebuild with --enable-cluster to use cluster_ges_mode_matches_pg().")));
	PG_RETURN_NULL();
}

#endif /* USE_PGRAC_CLUSTER */
