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
PG_FUNCTION_INFO_V1(cluster_thread_replay_slot_test);
PG_FUNCTION_INFO_V1(cluster_thread_recovery_worker_run_test);
PG_FUNCTION_INFO_V1(cluster_thread_recovery_launch_test);
PG_FUNCTION_INFO_V1(cluster_thread_replay_slot_state_test);
PG_FUNCTION_INFO_V1(cluster_reconfig_inject_dead_node_test);

#ifdef USE_PGRAC_CLUSTER

#include "access/xlogdefs.h"
#include "miscadmin.h"
#include "utils/builtins.h"
#include "utils/pg_lsn.h"

#include "cluster/cluster_conf.h"	  /* CLUSTER_MAX_NODES (dead bitmap width) */
#include "cluster/cluster_cssd.h"	  /* cluster_cssd_get_dead_generation (inject) */
#include "cluster/cluster_grd.h"	  /* live recovery episode epoch (worker L235 test) */
#include "cluster/cluster_guc.h"	  /* cluster_node_id (inject coordinator) */
#include "cluster/cluster_reconfig.h" /* synthetic reconfig inject (Part 4 e2e) */
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
	const char *out = psprintf(
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

/*
 * cluster_thread_replay_slot_test -- exercise the per-thread online replay-state
 * shmem slot round-trip (spec-4.11 3b-4b Part 1): mark REPLAYING (stamping a
 * recognisable episode), read it back, write the terminal DONE, evaluate the
 * L235 epoch-staleness guard, then restore IDLE.  Returns 'noslot' when dead_tid
 * names no slot (a bad id / no shmem attached), else a ':'-delimited summary the
 * cluster_tap parses:
 *
 *	    <st0>:<st1>:<ep1>:<st2>:<abort_same>:<abort_diff>
 *
 * where stN is a ClusterThreadRecReplayState int (idle 0 / replaying 1 / done 2 /
 * blocked 3), ep1 is the episode stamped by mark_replaying, and the abort flags
 * are the pure L235 guard for same/different live episodes.  TEST-ONLY,
 * superuser-only.
 */
Datum
cluster_thread_replay_slot_test(PG_FUNCTION_ARGS)
{
	int32 dead_tid;
	uint16 tid;
	ClusterThreadRecReplayState st0;
	ClusterThreadRecReplayState st1;
	ClusterThreadRecReplayState st2;
	uint64 ep1 = 0;
	const uint64 stamp = UINT64CONST(0xABCD);
	bool abort_same;
	bool abort_diff;
	const char *out;

	if (!superuser())
		ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
						errmsg("cluster_thread_replay_slot_test is superuser-only")));

	if (PG_ARGISNULL(0))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("dead_tid must not be NULL")));

	dead_tid = PG_GETARG_INT32(0);
	/* Out-of-uint16 ids fail closed in the accessor's range gate (-> noslot). */
	if (dead_tid < 0 || dead_tid > PG_UINT16_MAX)
		dead_tid = 0;
	tid = (uint16)dead_tid;

	/* No slot for this id -> nothing to round-trip (the accessor failed closed). */
	if (!cluster_thread_recovery_replay_read(tid, &st0, NULL))
		PG_RETURN_TEXT_P(cstring_to_text("noslot"));

	cluster_thread_recovery_replay_mark_replaying(tid, stamp);
	cluster_thread_recovery_replay_read(tid, &st1, &ep1);

	cluster_thread_recovery_replay_set_state(tid, CLUSTER_THREADREC_REPLAY_DONE);
	cluster_thread_recovery_replay_read(tid, &st2, NULL);

	/* Evaluate the L235 guard against the epoch actually stamped and read back
	 * from the slot (ep1), not a literal -- so the round-tripped value drives the
	 * decision (and the comparison is not a compile-time constant fold). */
	abort_same = cluster_thread_recovery_replay_epoch_aborts(ep1, stamp);
	abort_diff = cluster_thread_recovery_replay_epoch_aborts(ep1, stamp + 1);

	/* Restore IDLE so reruns are idempotent (Part 1 has no live consumer yet). */
	cluster_thread_recovery_replay_set_state(tid, CLUSTER_THREADREC_REPLAY_IDLE);

	out = psprintf("%d:%d:" UINT64_FORMAT ":%d:%d:%d", (int)st0, (int)st1, ep1, (int)st2,
				   abort_same ? 1 : 0, abort_diff ? 1 : 0);
	PG_RETURN_TEXT_P(cstring_to_text(out));
}

/*
 * cluster_thread_recovery_worker_run_test -- exercise the executor worker's
 * testable core (spec-4.11 3b-4b Part 2) in-process: simulate the lmon launch by
 * stamping the slot REPLAYING at (live recovery episode + epoch_offset), then run
 * cluster_thread_recovery_worker_run and report  <res>:<final_slot_state>  (or
 * <res>:noslot for a bad id), then restore IDLE.  res is a ClusterThreadRecResult
 * (done 0 / blocked 1 / not_applicable 2); final_slot_state is a
 * ClusterThreadRecReplayState (idle 0 / replaying 1 / done 2 / blocked 3).
 *
 *	epoch_offset 0 + do_mark = a FRESH launch (epoch matches the live episode):
 *	the worker dispatches replay_one and writes the terminal slot state.  A
 *	non-zero offset makes the slot STALE so the L235 guard aborts BEFORE
 *	replay_one and leaves the slot REPLAYING.  do_mark=false leaves the slot IDLE
 *	to exercise the not-REPLAYING guard.  TEST-ONLY, superuser-only.
 */
Datum
cluster_thread_recovery_worker_run_test(PG_FUNCTION_ARGS)
{
	int32 dead_tid;
	int64 epoch_offset;
	bool do_mark;
	uint16 tid;
	ClusterThreadRecResult res;
	ClusterThreadRecReplayState final_state;
	const char *out;

	if (!superuser())
		ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
						errmsg("cluster_thread_recovery_worker_run_test is superuser-only")));

	if (PG_ARGISNULL(0) || PG_ARGISNULL(1) || PG_ARGISNULL(2))
		ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						errmsg("dead_tid, epoch_offset and do_mark must not be NULL")));

	dead_tid = PG_GETARG_INT32(0);
	epoch_offset = PG_GETARG_INT64(1);
	do_mark = PG_GETARG_BOOL(2);

	/* Out-of-uint16 ids fail closed in the slot accessor (-> noslot). */
	if (dead_tid < 0 || dead_tid > PG_UINT16_MAX)
		dead_tid = 0;
	tid = (uint16)dead_tid;

	if (do_mark) {
		uint64 epoch = cluster_grd_redeclare_episode_epoch() + (uint64)epoch_offset;

		cluster_thread_recovery_replay_mark_replaying(tid, epoch);
	}

	res = cluster_thread_recovery_worker_run(tid);

	if (!cluster_thread_recovery_replay_read(tid, &final_state, NULL))
		PG_RETURN_TEXT_P(cstring_to_text(psprintf("%d:noslot", (int)res)));

	out = psprintf("%d:%d", (int)res, (int)final_state);

	/* Restore IDLE so reruns are deterministic (Part 2 has no live launcher). */
	cluster_thread_recovery_replay_set_state(tid, CLUSTER_THREADREC_REPLAY_IDLE);

	PG_RETURN_TEXT_P(cstring_to_text(out));
}

/*
 * cluster_thread_recovery_launch_test -- exercise the lmon launch side (spec-4.11
 * 3b-4b Part 3) with a synthetic single-dead-node bitmap (node = dead_node).
 * Returns the dead thread's replay-slot state after the call as an int
 * (idle 0 / replaying 1 / done 2 / blocked 3), or 'noslot'.  On a single machine
 * the launch is OUT OF SCOPE (no peers), so it is a NO-OP -> the slot stays IDLE
 * (0): this pins that the FSM wiring never launches / never stamps a slot out of
 * scope (the in-scope firing is the 2-node e2e, Part 4).  TEST-ONLY, superuser.
 */
Datum
cluster_thread_recovery_launch_test(PG_FUNCTION_ARGS)
{
	int32 dead_node;
	uint16 dead_tid;
	uint64 dead[(CLUSTER_MAX_NODES + 63) / 64];
	ClusterThreadRecReplayState state;

	if (!superuser())
		ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
						errmsg("cluster_thread_recovery_launch_test is superuser-only")));

	if (PG_ARGISNULL(0))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("dead_node must not be NULL")));

	dead_node = PG_GETARG_INT32(0);

	memset(dead, 0, sizeof(dead));
	if (dead_node >= 0 && dead_node < CLUSTER_MAX_NODES)
		dead[dead_node / 64] |= (UINT64CONST(1) << (dead_node % 64));
	dead_tid = (uint16)(dead_node + 1);

	/* Clean baseline so the result reflects only this call. */
	cluster_thread_recovery_replay_set_state(dead_tid, CLUSTER_THREADREC_REPLAY_IDLE);

	cluster_thread_recovery_launch_workers(dead, (int)(sizeof(dead) / sizeof(dead[0])),
										   cluster_grd_redeclare_episode_epoch());

	if (!cluster_thread_recovery_replay_read(dead_tid, &state, NULL))
		PG_RETURN_TEXT_P(cstring_to_text("noslot"));

	/* Restore IDLE so reruns are deterministic. */
	cluster_thread_recovery_replay_set_state(dead_tid, CLUSTER_THREADREC_REPLAY_IDLE);

	PG_RETURN_TEXT_P(cstring_to_text(psprintf("%d", (int)state)));
}

/*
 * cluster_thread_replay_slot_state_test -- READ-ONLY observer of a dead thread's
 * online replay-state shmem slot (spec-4.11 3b-4b Part 4 e2e).  Unlike
 * cluster_thread_replay_slot_test (which round-trips / mutates the slot), this
 * only reads the current state so a TAP can watch the slot transition
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
 * cluster_reconfig_inject_dead_node_test -- drive a SYNTHETIC reconfig episode on
 * THIS node as coordinator (spec-4.11 3b-4b Part 4 e2e), marking dead_node dead in
 * the reconfig dead bitmap.  This publishes a ReconfigEvent the local lmon GRD
 * recovery FSM consumes on its next tick, so the e2e can exercise the REAL
 * inject -> FSM -> WAIT_CLUSTER -> launch_workers wiring deterministically, without
 * relying on a real peer death + CSSD deadband (the t/099 path).  The dead node is
 * NOT actually killed; the survivor recovers an (intentionally non-recoverable in
 * this single-machine harness) foreign thread and stays fail-closed frozen.
 *
 * The bit convention mirrors cluster_reconfig.c dead_bitmap_set_bit():
 * node n -> byte n/8, bit n%8 (the GRD FSM maps it to word n/64, bit n%64).
 * TEST-ONLY, superuser-only.
 */
Datum
cluster_reconfig_inject_dead_node_test(PG_FUNCTION_ARGS)
{
	int32 dead_node;
	uint8 dead_bitmap[CLUSTER_RECONFIG_DEAD_BITMAP_BYTES];

	if (!superuser())
		ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
						errmsg("cluster_reconfig_inject_dead_node_test is superuser-only")));

	if (PG_ARGISNULL(0))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("dead_node must not be NULL")));

	dead_node = PG_GETARG_INT32(0);
	if (dead_node < 0 || dead_node >= CLUSTER_MAX_NODES)
		ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						errmsg("dead_node %d out of range [0, %d)", dead_node, CLUSTER_MAX_NODES)));

	memset(dead_bitmap, 0, sizeof(dead_bitmap));
	dead_bitmap[dead_node / 8] |= (uint8)(1u << (dead_node % 8));

	cluster_reconfig_apply_epoch_bump_as_coordinator(dead_bitmap, cluster_node_id,
													 cluster_cssd_get_dead_generation());

	PG_RETURN_BOOL(true);
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

Datum
cluster_thread_replay_slot_test(PG_FUNCTION_ARGS pg_attribute_unused())
{
	ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					errmsg("cluster_thread_replay_slot_test requires --enable-cluster")));
}

Datum
cluster_thread_recovery_worker_run_test(PG_FUNCTION_ARGS pg_attribute_unused())
{
	ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					errmsg("cluster_thread_recovery_worker_run_test requires --enable-cluster")));
}

Datum
cluster_thread_recovery_launch_test(PG_FUNCTION_ARGS pg_attribute_unused())
{
	ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					errmsg("cluster_thread_recovery_launch_test requires --enable-cluster")));
}

Datum
cluster_thread_replay_slot_state_test(PG_FUNCTION_ARGS pg_attribute_unused())
{
	ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					errmsg("cluster_thread_replay_slot_state_test requires --enable-cluster")));
}

Datum
cluster_reconfig_inject_dead_node_test(PG_FUNCTION_ARGS pg_attribute_unused())
{
	ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					errmsg("cluster_reconfig_inject_dead_node_test requires --enable-cluster")));
}

#endif /* USE_PGRAC_CLUSTER */
