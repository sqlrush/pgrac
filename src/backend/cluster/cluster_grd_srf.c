/*-------------------------------------------------------------------------
 *
 * cluster_grd_srf.c
 *	  pgrac cluster GRD shard catalog SRF (spec-2.14 D7).
 *
 *	  Implements `cluster_get_grd_shards()` -- the SRF backing the
 *	  `pg_cluster_grd_shards` view (system_views.sql).  The SRF lives
 *	  in its own file so cluster_unit standalone tests (which link
 *	  cluster_grd.o without PG runtime) can remain free of unresolved
 *	  symbols (InitMaterializedSRF, tuplestore_putvalues).  Mirrors
 *	  the spec-2.3 split where cluster_get_ic_msg_types lives in
 *	  cluster_ic_msg_types_srf.c.
 *
 *	  Volatility: STABLE.  master[] is process-local cache initialized
 *	  at postmaster phase 1; from a backend's perspective it does not
 *	  change within a single statement (Stage 6 DRM refresh will
 *	  invalidate caches via LMON-mediated reconfig epoch).
 *
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/backend/cluster/cluster_grd_srf.c
 *
 * NOTES
 *	  pgrac-original file.  Built only in --enable-cluster mode
 *	  (USE_PGRAC_CLUSTER); the disable-mode stub for cluster_get_grd_shards
 *	  lives in cluster_ic.c.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#ifdef USE_PGRAC_CLUSTER

#include "access/htup_details.h"
#include "funcapi.h"
#include "utils/builtins.h"

#include "cluster/cluster_grd.h"
#include "cluster/cluster_guc.h" /* cluster_node_id */

#define CLUSTER_GET_GRD_SHARDS_NCOLS 3
#define CLUSTER_GET_GRD_ENTRIES_NCOLS 11

/*
 * NB: PG_FUNCTION_INFO_V1(cluster_get_grd_shards) is emitted in
 * cluster_ic.c (always-linked file), so pg_proc.dat resolves the
 * symbol in both --enable-cluster and --disable-cluster builds.
 */
Datum
cluster_get_grd_shards(PG_FUNCTION_ARGS)
{
	ReturnSetInfo *rsinfo = (ReturnSetInfo *)fcinfo->resultinfo;
	int shard_id;

	InitMaterializedSRF(fcinfo, 0);

	for (shard_id = 0; shard_id < PGRAC_GRD_SHARD_COUNT; shard_id++) {
		Datum values[CLUSTER_GET_GRD_SHARDS_NCOLS];
		bool nulls[CLUSTER_GET_GRD_SHARDS_NCOLS];
		int32 master;

		master = cluster_grd_shard_master((uint32)shard_id);

		memset(nulls, 0, sizeof(nulls));
		values[0] = Int32GetDatum(shard_id);
		values[1] = Int32GetDatum(master);
		values[2] = BoolGetDatum(master == cluster_node_id);

		tuplestore_putvalues(rsinfo->setResult, rsinfo->setDesc, values, nulls);
	}

	return (Datum)0;
}


/*
 * spec-2.15 D8:  cluster_get_grd_entries SRF body.
 *
 *  Diagnostic / observability only — NOT for production hot-path queries
 *  (P1.5).  Iterates the entry HTAB via cluster_grd_entries_walk()
 *  visitor;  per-entry slock_t snapshot keeps each row consistent.
 *
 *  spec-2.16 forward-link TODO (P2.4 + I14):  before caller-side
 *  LockAcquire integration ships, this visitor must amend locking
 *  (wrap hash_seq_search in 4096-shard LW_SHARED acquire OR chunked
 *  snapshot) to defend against concurrent HASH_ENTER_NULL writers.
 *  本 spec 0 caller → 0 row → 无并发问题.
 *
 *  NB: PG_FUNCTION_INFO_V1(cluster_get_grd_entries) is emitted in
 *  cluster_ic.c (always-linked file), so pg_proc.dat resolves the
 *  symbol in both --enable-cluster and --disable-cluster builds.
 */
static void
emit_grd_entry_row(void *ctx, const int32 row_fields[11])
{
	ReturnSetInfo *rsinfo = (ReturnSetInfo *)ctx;
	Datum values[CLUSTER_GET_GRD_ENTRIES_NCOLS];
	bool nulls[CLUSTER_GET_GRD_ENTRIES_NCOLS];
	int i;

	memset(nulls, 0, sizeof(nulls));
	for (i = 0; i < CLUSTER_GET_GRD_ENTRIES_NCOLS; i++)
		values[i] = Int32GetDatum(row_fields[i]);

	tuplestore_putvalues(rsinfo->setResult, rsinfo->setDesc, values, nulls);
}

Datum
cluster_get_grd_entries(PG_FUNCTION_ARGS)
{
	ReturnSetInfo *rsinfo = (ReturnSetInfo *)fcinfo->resultinfo;

	InitMaterializedSRF(fcinfo, 0);

	cluster_grd_entries_walk(emit_grd_entry_row, (void *)rsinfo);

	return (Datum)0;
}

#endif /* USE_PGRAC_CLUSTER */
