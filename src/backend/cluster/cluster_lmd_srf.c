/*-------------------------------------------------------------------------
 *
 * cluster_lmd_srf.c
 *	  pgrac cluster LMD state catalog SRF (spec-2.19 D11).
 *
 *	  Implements `cluster_get_lmd_state()` -- the SRF backing the
 *	  `pg_cluster_lmd` view (system_views.sql).  Returns 1 row containing
 *	  LMD process pid + 4-state semantic分流 (state + reason) + 5
 *	  counter columns plus started_at / ready_at timestamps.
 *
 *	  HC2 4-state semantic split (spec-2.19 §1.4.6):
 *	    DISABLED              → reason = 'disabled_by_config'
 *	                            (lmd_enabled=off startup-only)
 *	    NOT_STARTED/STARTING/
 *	    DRAINING/STOPPED      → reason = 'lmd_not_ready'
 *	                            (53R81 raise on caller-side gate)
 *	    READY                 → reason = NULL
 *	    CRASHED (pid==0 +
 *	      enabled=on)         → reason = 'crashed_unavailable'
 *
 *	  The SRF lives in its own file so cluster_unit standalone tests
 *	  (which link cluster_lmd.o without PG runtime) can remain free of
 *	  unresolved symbols (InitMaterializedSRF, tuplestore_putvalues).
 *	  Mirrors the spec-2.14 D7 / spec-2.3 D8 split.
 *
 *	  Volatility: STABLE.  lmd_state atomic snapshot is taken within a
 *	  single statement;cross-call变化 expected only across LMD reaper
 *	  restart events.
 *
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/backend/cluster/cluster_lmd_srf.c
 *
 * NOTES
 *	  pgrac-original file.  Built only in --enable-cluster mode
 *	  (USE_PGRAC_CLUSTER);the disable-mode stub for cluster_get_lmd_state
 *	  lives in cluster_ic.c.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#ifdef USE_PGRAC_CLUSTER

#include "access/htup_details.h"
#include "funcapi.h"
#include "miscadmin.h" /* superuser() */
#include "utils/builtins.h"
#include "utils/timestamp.h"

#include "cluster/cluster_guc.h" /* cluster_lmd_enabled */
#include "cluster/cluster_lmd.h"

#define CLUSTER_GET_LMD_STATE_NCOLS 10

/*
 * NB: PG_FUNCTION_INFO_V1(cluster_get_lmd_state) is emitted in
 * cluster_ic.c (always-linked file), so pg_proc.dat resolves the
 * symbol in both --enable-cluster and --disable-cluster builds.
 */
Datum
cluster_get_lmd_state(PG_FUNCTION_ARGS)
{
	ReturnSetInfo *rsinfo = (ReturnSetInfo *)fcinfo->resultinfo;
	Datum values[CLUSTER_GET_LMD_STATE_NCOLS];
	bool nulls[CLUSTER_GET_LMD_STATE_NCOLS];
	ClusterLmdState state;
	pid_t pid;
	TimestampTz spawned_at;
	TimestampTz ready_at;
	const char *reason = NULL;

	InitMaterializedSRF(fcinfo, 0);

	state = cluster_lmd_get_state();
	pid = cluster_lmd_get_pid();
	spawned_at = cluster_lmd_get_spawned_at();
	ready_at = cluster_lmd_get_ready_at();

	/*
	 * HC2 4-state semantic split (§1.4.6).  reason is NULL only for READY;
	 * caller-side gate consumers can branch on `reason IS NULL` to
	 * distinguish steady-state from transient / failed / opt-out paths.
	 */
	switch (state) {
	case CLUSTER_LMD_DISABLED:
		reason = "disabled_by_config";
		break;
	case CLUSTER_LMD_READY:
		reason = NULL;
		break;
	case CLUSTER_LMD_STOPPED:
		reason = (pid == 0 && cluster_lmd_enabled) ? "crashed_unavailable" : "lmd_not_ready";
		break;
	case CLUSTER_LMD_NOT_STARTED:
	case CLUSTER_LMD_STARTING:
	case CLUSTER_LMD_DRAINING:
	default:
		reason = "lmd_not_ready";
		break;
	}

	memset(nulls, 0, sizeof(nulls));

	/* 0: pid (int4) — NULL when not yet spawned. */
	if (pid == 0)
		nulls[0] = true;
	else
		values[0] = Int32GetDatum((int32)pid);

	/* 1: state (text) */
	values[1] = CStringGetTextDatum(cluster_lmd_state_to_string(state));

	/* 2: reason (text) — NULL only when state == READY. */
	if (reason == NULL)
		nulls[2] = true;
	else
		values[2] = CStringGetTextDatum(reason);

	/* 3: started_at (timestamptz) — NULL when not yet spawned. */
	if (spawned_at == 0)
		nulls[3] = true;
	else
		values[3] = TimestampTzGetDatum(spawned_at);

	/* 4: ready_at (timestamptz) — NULL until first READY transition. */
	if (ready_at == 0)
		nulls[4] = true;
	else
		values[4] = TimestampTzGetDatum(ready_at);

	/* 5-9: 5 atomic counter columns as int8. */
	values[5] = Int64GetDatum((int64)cluster_lmd_get_started_count());
	values[6] = Int64GetDatum((int64)cluster_lmd_get_edge_submission_count());
	values[7] = Int64GetDatum((int64)cluster_lmd_get_wake_count());
	values[8] = Int64GetDatum((int64)cluster_lmd_get_idle_count());
	values[9] = Int64GetDatum((int64)cluster_lmd_get_error_count());

	tuplestore_putvalues(rsinfo->setResult, rsinfo->setDesc, values, nulls);

	return (Datum)0;
}


/*
 * pg_cluster_lmd_inject_wait_edge -- spec-2.22 D16 TEST-ONLY injector.
 *
 *	Bypasses GES path entirely and writes a synthetic (waiter, blocker)
 *	wait edge into the LMD graph via cluster_lmd_inject_wait_edge().
 *	Intended for TAP 109 cycle construction;production LMS handler is
 *	a single-node-GRANT stub until spec-2.23 BAST 配套 wires real
 *	conflict + waiter queue.
 *
 *	Returns boolean: true if edge added, false if cap rejected (HC12).
 *	Superuser-only;LOG audit + inject_call_count bump on every call.
 */
/*
 * NB: PG_FUNCTION_INFO_V1(pg_cluster_lmd_inject_wait_edge) is emitted in
 * cluster_ic.c (always-linked file), so pg_proc.dat resolves the symbol
 * in both --enable-cluster and --disable-cluster builds (mirror
 * cluster_get_lmd_state pattern).
 */
Datum
pg_cluster_lmd_inject_wait_edge(PG_FUNCTION_ARGS)
{
	int32 w_node = PG_GETARG_INT32(0);
	int32 w_procno = PG_GETARG_INT32(1);
	int64 w_request_id = PG_GETARG_INT64(2);
	int32 b_node = PG_GETARG_INT32(3);
	int32 b_procno = PG_GETARG_INT32(4);
	int64 b_request_id = PG_GETARG_INT64(5);
	ClusterLmdVertex waiter, blocker;
	bool ok;

	if (!superuser())
		ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
						errmsg("pg_cluster_lmd_inject_wait_edge is restricted to superusers"),
						errhint("Test-only injector; production GES correctness is undefined "
								"when used outside controlled test harnesses.")));

	memset(&waiter, 0, sizeof(waiter));
	waiter.node_id = w_node;
	waiter.procno = (uint32)w_procno;
	waiter.request_id = (uint64)w_request_id;
	waiter.cluster_epoch = 1; /* spec-2.26 wires real epoch later */

	memset(&blocker, 0, sizeof(blocker));
	blocker.node_id = b_node;
	blocker.procno = (uint32)b_procno;
	blocker.request_id = (uint64)b_request_id;
	blocker.cluster_epoch = 1;

	ereport(LOG, (errmsg("pg_cluster_lmd_inject_wait_edge: TEST-ONLY injection "
						 "waiter=(%d,%u," INT64_FORMAT ") blocker=(%d,%u," INT64_FORMAT ")",
						 w_node, (uint32)w_procno, w_request_id, b_node, (uint32)b_procno,
						 b_request_id)));

	ok = cluster_lmd_inject_wait_edge(&waiter, &blocker);
	PG_RETURN_BOOL(ok);
}


/*
 * pg_cluster_lmd_remove_wait_edges -- spec-5.8 D3 TEST/DIAGNOSTIC-ONLY
 * remover.
 *
 *	Removes EVERY wait edge whose waiter half matches the given 4-tuple
 *	(node, procno, cluster_epoch=1, request_id), mirroring the synthetic
 *	identity that pg_cluster_lmd_inject_wait_edge writes.  This exists because
 *	spec-5.8 D1a widened the graph key to (waiter, blocker), so a waiter
 *	blocked by several holders now keeps several edges and there is no longer
 *	any "redirect to overwrite" trick to break a synthetic cycle — a test must
 *	be able to truly delete a waiter's edges.  Wraps the already-shipped
 *	cluster_lmd_graph_remove_edge_by_waiter (unit-tested in
 *	test_cluster_lmd_graph U1b/U1c).
 *
 *	This is NOT a user-facing API: it is an LMD diagnostic / test hook only
 *	(superuser-only; name prefix pg_cluster_lmd_* signals test-only, like the
 *	injector).  Returns boolean: true if at least one edge was removed.
 */
/*
 * NB: PG_FUNCTION_INFO_V1(pg_cluster_lmd_remove_wait_edges) is emitted in
 * cluster_ic.c (always-linked file), so pg_proc.dat resolves the symbol in
 * both --enable-cluster and --disable-cluster builds.
 */
Datum
pg_cluster_lmd_remove_wait_edges(PG_FUNCTION_ARGS)
{
	int32 w_node = PG_GETARG_INT32(0);
	int32 w_procno = PG_GETARG_INT32(1);
	int64 w_request_id = PG_GETARG_INT64(2);
	ClusterLmdVertex waiter;
	bool removed;

	if (!superuser())
		ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
						errmsg("pg_cluster_lmd_remove_wait_edges is restricted to superusers"),
						errhint("Test/diagnostic-only edge remover; not a user-facing API.")));

	memset(&waiter, 0, sizeof(waiter));
	waiter.node_id = w_node;
	waiter.procno = (uint32)w_procno;
	waiter.request_id = (uint64)w_request_id;
	waiter.cluster_epoch = 1; /* mirror the injector's synthetic epoch */

	ereport(LOG, (errmsg("pg_cluster_lmd_remove_wait_edges: TEST-ONLY removal "
						 "waiter=(%d,%u," INT64_FORMAT ")",
						 w_node, (uint32)w_procno, w_request_id)));

	removed = cluster_lmd_graph_remove_edge_by_waiter(&waiter);
	PG_RETURN_BOOL(removed);
}

#endif /* USE_PGRAC_CLUSTER */
