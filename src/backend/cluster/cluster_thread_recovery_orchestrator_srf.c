/*-------------------------------------------------------------------------
 *
 * cluster_thread_recovery_orchestrator_srf.c
 *	  Read-only TEST SQL observers for online recovery state/capability.  STOP03
 *	  removes the former direct replay, worker-launch and reconfig mutators.
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

PG_FUNCTION_INFO_V1(cluster_thread_local_complete_test);
PG_FUNCTION_INFO_V1(cluster_thread_gate_unfreeze_test);
PG_FUNCTION_INFO_V1(cluster_thread_replay_slot_state_test);
PG_FUNCTION_INFO_V1(cluster_thread_capability_gate_test);

#ifdef USE_PGRAC_CLUSTER

#include "access/xlogdefs.h"
#include "miscadmin.h"
#include "utils/builtins.h"
#include "utils/pg_lsn.h"

#include "cluster/cluster_conf.h" /* CLUSTER_MAX_NODES (dead bitmap width) */
#include "cluster/cluster_thread_recovery.h"

/*
 * cluster_thread_local_complete_test -- exercise the D3 unfreeze precondition
 * (spec-4.11 3b-3): does the node-local merged authority say dead_tid is
 * online-recovered up to required_lsn?
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

/*
 * cluster_thread_replay_slot_state_test -- READ-ONLY observer of a dead thread's
 * online replay-state shmem slot (spec-4.11 3b-4b Part 4 e2e).  It only reads
 * the current state so a TAP can watch the slot transition
 * IDLE -> REPLAYING -> BLOCKED as the reconfig FSM launches the executor worker
 * and the worker fails closed on an unrecoverable dead thread, WITHOUT perturbing
 * it.  Returns the ClusterThreadRecReplayState int (idle 0 / replaying 1 /
 * done 2 / blocked 3), or -1 when dead_tid names no slot.  TEST-ONLY, superuser.
 */
Datum
cluster_thread_replay_slot_state_test(PG_FUNCTION_ARGS)
{
	int32 dead_tid;
	ClusterThreadRecReplayState state;

	if (!superuser())
		ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
						errmsg("cluster_thread_replay_slot_state_test is superuser-only")));

	if (PG_ARGISNULL(0))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("dead_tid must not be NULL")));

	dead_tid = PG_GETARG_INT32(0);
	/* Out-of-uint16 ids fail closed in the accessor's range gate (-> noslot). */
	if (dead_tid < 0 || dead_tid > PG_UINT16_MAX)
		dead_tid = 0;

	if (!cluster_thread_recovery_replay_read((uint16)dead_tid, &state, NULL))
		PG_RETURN_INT32(-1);

	PG_RETURN_INT32((int32)state);
}

/*
 * cluster_thread_capability_gate_test -- exercise the D7 capability gate (spec-4.11
 * §D7).  scope >= 0 drives cluster_thread_recovery_capability_gate with an explicit
 * ClusterThreadRecScope (0 APPLICABLE / 1 DISABLED / 2 SINGLE_NODE / 3
 * NO_SHARED_BACKEND), so the TAP can assert FEATURE_NOT_SUPPORTED
 * (SQLSTATE 0A000) for the hard-unsupported scopes deterministically on one machine
 * (no real no-backend cluster needed).  scope < 0 resolves the
 * live-runtime scope first (cluster_thread_recovery_current_scope).  Returns
 * 'ok:<scope_int>' when the gate does NOT raise (APPLICABLE / DISABLED /
 * SINGLE_NODE); otherwise the gate ereports and this never returns.  TEST-ONLY,
 * superuser-only.
 */
Datum
cluster_thread_capability_gate_test(PG_FUNCTION_ARGS)
{
	int32 scope_arg;
	ClusterThreadRecScope scope;

	if (!superuser())
		ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
						errmsg("cluster_thread_capability_gate_test is superuser-only")));

	if (PG_ARGISNULL(0))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("scope must not be NULL")));

	scope_arg = PG_GETARG_INT32(0);
	if (scope_arg < 0)
		scope = cluster_thread_recovery_current_scope();
	else if (scope_arg > (int32)CLUSTER_THREADREC_SCOPE_NO_SHARED_BACKEND)
		ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						errmsg("scope %d out of range [0, %d]", scope_arg,
							   (int)CLUSTER_THREADREC_SCOPE_NO_SHARED_BACKEND)));
	else
		scope = (ClusterThreadRecScope)scope_arg;

	/* Raises FEATURE_NOT_SUPPORTED only for NO_SHARED_BACKEND. */
	cluster_thread_recovery_capability_gate(scope);

	PG_RETURN_TEXT_P(cstring_to_text(psprintf("ok:%d", (int)scope)));
}

#else /* !USE_PGRAC_CLUSTER */

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

Datum
cluster_thread_replay_slot_state_test(PG_FUNCTION_ARGS pg_attribute_unused())
{
	ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					errmsg("cluster_thread_replay_slot_state_test requires --enable-cluster")));
}

Datum
cluster_thread_capability_gate_test(PG_FUNCTION_ARGS pg_attribute_unused())
{
	ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					errmsg("cluster_thread_capability_gate_test requires --enable-cluster")));
}

#endif /* USE_PGRAC_CLUSTER */
