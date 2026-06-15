/*-------------------------------------------------------------------------
 *
 * cluster_thread_recovery_orchestrator_srf.c
 *	  pgrac TEST-ONLY SQL entry points driving the online thread-recovery
 *	  ORCHESTRATOR (combined data + visibility pass + durability barrier +
 *	  3-way authority publish) for the spec-4.11 increment 3b-2 TAP (t/261).
 *
 *	  cluster_thread_replay_one_test(dead_tid int4, scan_lower pg_lsn,
 *	                                 scan_upper pg_lsn) -> text
 *	      drives cluster_thread_recovery_replay_one_window() over an EXPLICIT,
 *	      deterministic window: combined replay, durability barrier, and 3-way
 *	      authority publish on DONE.
 *
 *	  cluster_thread_replay_one_auto_test(dead_tid int4) -> text
 *	      drives cluster_thread_recovery_replay_one(): the scope gate + basic
 *	      window derivation (returns not_applicable on a single node / no shared
 *	      backend, the common single-machine case).
 *
 *	  Both return a ':'-delimited summary the cluster_tap parses:
 *
 *	      <result>:<records_scanned>:<blocks_applied>:<blocks_gated>:
 *	      <blocks_out_of_scope>:<recovered_through>
 *
 *	  where <result> is done / blocked / not_applicable.  To exercise R13, the
 *	  cluster_tap arms the cluster-thread-recovery-drive injection point with a
 *	  catchable ERROR before calling -- the orchestrator's harness demotes it to
 *	  BLOCKED and the backend survives.
 *
 *	  TEST-ONLY: diagnostic entry points, NOT product query interfaces;
 *	  superuser-only.  Mirror cluster_thread_recovery_driver_srf.c.
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
 *	  src/backend/cluster/cluster_thread_recovery_orchestrator_srf.c
 *
 * NOTES
 *	  This is a pgrac-original file (no derivation from PostgreSQL).
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "fmgr.h"

PG_FUNCTION_INFO_V1(cluster_thread_replay_one_test);
PG_FUNCTION_INFO_V1(cluster_thread_replay_one_auto_test);
PG_FUNCTION_INFO_V1(cluster_thread_local_complete_test);
PG_FUNCTION_INFO_V1(cluster_thread_gate_unfreeze_test);

#ifdef USE_PGRAC_CLUSTER

#include "access/xlogdefs.h"
#include "miscadmin.h"
#include "utils/builtins.h"
#include "utils/pg_lsn.h"

#include "cluster/cluster_conf.h" /* CLUSTER_MAX_NODES (dead bitmap width) */
#include "cluster/cluster_thread_recovery.h"

static const char *
threadrec_result_text(ClusterThreadRecResult res)
{
	switch (res) {
	case CLUSTER_THREADREC_DONE:
		return "done";
	case CLUSTER_THREADREC_BLOCKED:
		return "blocked";
	default:
		return "not_applicable";
	}
}

static text *
threadrec_summary(ClusterThreadRecResult res, const ClusterThreadReplayStats *stats)
{
	char *out = psprintf(
		"%s:" UINT64_FORMAT ":" UINT64_FORMAT ":" UINT64_FORMAT ":" UINT64_FORMAT ":%X/%X",
		threadrec_result_text(res), stats->records_scanned, stats->blocks_applied,
		stats->blocks_gated, stats->blocks_out_of_scope, LSN_FORMAT_ARGS(stats->recovered_through));

	return cstring_to_text(out);
}

Datum
cluster_thread_replay_one_test(PG_FUNCTION_ARGS)
{
	int32 dead_tid;
	XLogRecPtr scan_lower;
	XLogRecPtr scan_upper;
	ClusterThreadReplayStats stats;
	ClusterThreadRecResult res;

	if (!superuser())
		ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
						errmsg("cluster_thread_replay_one_test is superuser-only")));

	if (PG_ARGISNULL(0) || PG_ARGISNULL(1) || PG_ARGISNULL(2))
		ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						errmsg("dead_tid, scan_lower and scan_upper must not be NULL")));

	dead_tid = PG_GETARG_INT32(0);
	scan_lower = PG_GETARG_LSN(1);
	scan_upper = PG_GETARG_LSN(2);

	/* Out-of-uint16 ids fail closed in the orchestrator's range gate. */
	if (dead_tid < 0 || dead_tid > PG_UINT16_MAX)
		dead_tid = 0;

	res = cluster_thread_recovery_replay_one_window((uint16)dead_tid, scan_lower, scan_upper,
													/* episode_epoch */ 0, &stats);

	PG_RETURN_TEXT_P(threadrec_summary(res, &stats));
}

Datum
cluster_thread_replay_one_auto_test(PG_FUNCTION_ARGS)
{
	int32 dead_tid;
	ClusterThreadReplayStats stats;
	ClusterThreadRecResult res;

	if (!superuser())
		ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
						errmsg("cluster_thread_replay_one_auto_test is superuser-only")));

	if (PG_ARGISNULL(0))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("dead_tid must not be NULL")));

	dead_tid = PG_GETARG_INT32(0);
	if (dead_tid < 0 || dead_tid > PG_UINT16_MAX)
		dead_tid = 0;

	/* replay_one returns only a verdict (it derives its own window); the auto
	 * path's stats are not surfaced, so report zeros alongside the verdict. */
	memset(&stats, 0, sizeof(stats));
	res = cluster_thread_recovery_replay_one((uint16)dead_tid, /* episode_epoch */ 0);

	PG_RETURN_TEXT_P(threadrec_summary(res, &stats));
}

/*
 * cluster_thread_local_complete_test -- exercise the D3 unfreeze precondition
 * (spec-4.11 3b-3): does the node-local merged authority say dead_tid is
 * online-recovered up to required_lsn?  The TAP publishes authority via
 * cluster_thread_replay_one_test (on DONE) and then asserts this flips to true.
 */
Datum
cluster_thread_local_complete_test(PG_FUNCTION_ARGS)
{
	int32 dead_tid;
	XLogRecPtr required_lsn;

	if (!superuser())
		ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
						errmsg("cluster_thread_local_complete_test is superuser-only")));

	if (PG_ARGISNULL(0) || PG_ARGISNULL(1))
		ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						errmsg("dead_tid and required_lsn must not be NULL")));

	dead_tid = PG_GETARG_INT32(0);
	required_lsn = PG_GETARG_LSN(1);

	/* Out-of-uint16 ids map to no origin -> fail-closed false. */
	if (dead_tid < 0 || dead_tid > PG_UINT16_MAX)
		dead_tid = 0;

	PG_RETURN_BOOL(cluster_thread_recovery_local_complete((uint16)dead_tid, required_lsn));
}

/*
 * cluster_thread_gate_unfreeze_test -- exercise the reconfig-FSM unfreeze gate
 * predicate (spec-4.11 3b-3) with a controlled single-dead-node bitmap (node =
 * dead_tid - 1).  Returns true == "stay frozen".  The TAP drives it across the
 * GUC off/on and authority absent/present axes to prove the gate engages only
 * in scope and lifts only once the dead origin is materialized.
 */
Datum
cluster_thread_gate_unfreeze_test(PG_FUNCTION_ARGS)
{
	int32 dead_tid;
	uint64 dead[(CLUSTER_MAX_NODES + 63) / 64];
	int node;

	if (!superuser())
		ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
						errmsg("cluster_thread_gate_unfreeze_test is superuser-only")));

	if (PG_ARGISNULL(0))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("dead_tid must not be NULL")));

	dead_tid = PG_GETARG_INT32(0);

	memset(dead, 0, sizeof(dead));
	node = (int)dead_tid - 1;
	if (node >= 0 && node < CLUSTER_MAX_NODES)
		dead[node / 64] |= (UINT64CONST(1) << (node % 64));

	PG_RETURN_BOOL(
		cluster_thread_recovery_gate_unfreeze(dead, (int)(sizeof(dead) / sizeof(dead[0]))));
}

#else /* !USE_PGRAC_CLUSTER */

Datum
cluster_thread_replay_one_test(PG_FUNCTION_ARGS pg_attribute_unused())
{
	ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					errmsg("cluster_thread_replay_one_test requires --enable-cluster")));
}

Datum
cluster_thread_replay_one_auto_test(PG_FUNCTION_ARGS pg_attribute_unused())
{
	ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					errmsg("cluster_thread_replay_one_auto_test requires --enable-cluster")));
}

Datum
cluster_thread_local_complete_test(PG_FUNCTION_ARGS pg_attribute_unused())
{
	ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					errmsg("cluster_thread_local_complete_test requires --enable-cluster")));
}

Datum
cluster_thread_gate_unfreeze_test(PG_FUNCTION_ARGS pg_attribute_unused())
{
	ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					errmsg("cluster_thread_gate_unfreeze_test requires --enable-cluster")));
}

#endif /* USE_PGRAC_CLUSTER */
