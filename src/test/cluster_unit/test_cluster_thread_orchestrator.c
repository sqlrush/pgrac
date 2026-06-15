/*-------------------------------------------------------------------------
 *
 * test_cluster_thread_orchestrator.c
 *	  pgrac spec-4.11 D1 increment 3b-2 — cluster_unit tests for the online
 *	  thread-recovery orchestrator's PURE decision helper.
 *
 *	  cluster_thread_recovery_decide_on_blocked(policy) -- the on_unrecoverable
 *	  escalation (Q5): the .c orchestrator reaches a FINAL BLOCKED and calls this
 *	  on the cluster.thread_recovery_on_unrecoverable GUC to decide whether to
 *	  return the BLOCKED (keep_frozen -- survivor lives) or PANIC the survivor.
 *	  Pinned in isolation so the escalation can NEVER turn the default keep_frozen
 *	  into a crash.  (The R13 elevel boundary and R14 writer admission live in
 *	  test_cluster_remote_xact.c; the scope gate in test_cluster_thread_driver.c.)
 *
 *	  Standalone executable per spec-0.4 §9.2.
 *
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/test/cluster_unit/test_cluster_thread_orchestrator.c
 *
 * Spec: spec-4.11-thread-recovery.md (FROZEN v0.3)
 *
 *-------------------------------------------------------------------------
 */
#define USE_PGRAC_CLUSTER 1

#include "postgres.h"

#include "cluster/cluster_thread_recovery.h"

#include "unit_test.h"

UT_DEFINE_GLOBALS();


UT_TEST(test_on_blocked_keep_frozen_default)
{
	/* The default keep_frozen policy returns the BLOCKED: only that thread's
	 * resources stay frozen, the survivor keeps running (minimum blast radius). */
	UT_ASSERT_EQ(
		(int)cluster_thread_recovery_decide_on_blocked(CLUSTER_THREADREC_ACTION_KEEP_FROZEN),
		(int)CLUSTER_THREADREC_ONBLOCKED_KEEP_FROZEN);
}

UT_TEST(test_on_blocked_panic_when_policy_panic)
{
	/* The panic escape valve crashes the survivor at postmaster level. */
	UT_ASSERT_EQ((int)cluster_thread_recovery_decide_on_blocked(CLUSTER_THREADREC_ACTION_PANIC),
				 (int)CLUSTER_THREADREC_ONBLOCKED_PANIC);
}

UT_TEST(test_on_blocked_unknown_policy_is_keep_frozen)
{
	/* Any value other than the explicit PANIC enum maps to keep_frozen: the
	 * escalation defaults to the SAFE (non-crashing) direction, never to PANIC. */
	UT_ASSERT_EQ((int)cluster_thread_recovery_decide_on_blocked(-1),
				 (int)CLUSTER_THREADREC_ONBLOCKED_KEEP_FROZEN);
	UT_ASSERT_EQ((int)cluster_thread_recovery_decide_on_blocked(999),
				 (int)CLUSTER_THREADREC_ONBLOCKED_KEEP_FROZEN);
}

UT_TEST(test_on_blocked_default_enum_is_keep_frozen)
{
	/* KEEP_FROZEN must be 0 so a zero-initialised / default policy is safe. */
	UT_ASSERT_EQ((int)CLUSTER_THREADREC_ONBLOCKED_KEEP_FROZEN, 0);
	UT_ASSERT((int)CLUSTER_THREADREC_ONBLOCKED_PANIC != 0);
}

UT_TEST(test_origin_for_tid_real_ids)
{
	/* spec-4.1: thread_id = node_id + 1, so origin = dead_tid - 1 across the
	 * whole real range [1, CLUSTER_WAL_THREAD_MAX]. */
	UT_ASSERT_EQ(cluster_thread_recovery_origin_for_tid(1), 0);
	UT_ASSERT_EQ(cluster_thread_recovery_origin_for_tid(2), 1);
	UT_ASSERT_EQ(cluster_thread_recovery_origin_for_tid(CLUSTER_WAL_THREAD_MAX),
				 (int)CLUSTER_WAL_THREAD_MAX - 1);
}

UT_TEST(test_origin_for_tid_legacy_zero_is_invalid)
{
	/* dead_tid 0 is the LEGACY (pre-activation) id, not a real thread; it must
	 * NOT alias to a valid origin (would be node 0 = false-complete = 8.A). */
	UT_ASSERT_EQ(cluster_thread_recovery_origin_for_tid(0), -1);
}

UT_TEST(test_origin_for_tid_out_of_range_is_invalid)
{
	/* Above the highest real thread id is fail-closed (-1), never a valid
	 * node; 0xFFFF is the permanently-invalid sentinel. */
	UT_ASSERT_EQ(cluster_thread_recovery_origin_for_tid(CLUSTER_WAL_THREAD_MAX + 1), -1);
	UT_ASSERT_EQ(cluster_thread_recovery_origin_for_tid(0xFFFF), -1);
}

UT_TEST(test_gate_decide_out_of_scope_never_gates)
{
	/* Out of scope (GUC off / single node / no shared backend / >2-node) the
	 * gate is a NO-OP regardless of the bitmaps -> false (no regression to the
	 * existing spec-4.6/4.7 unfreeze path). */
	uint64 dead[1] = { 0x2 };		  /* node 1 dead */
	uint64 materialized[1] = { 0x0 }; /* nothing materialized */

	UT_ASSERT(!cluster_thread_recovery_gate_decide(CLUSTER_THREADREC_SCOPE_DISABLED, dead,
												   materialized, 1));
	UT_ASSERT(!cluster_thread_recovery_gate_decide(CLUSTER_THREADREC_SCOPE_SINGLE_NODE, dead,
												   materialized, 1));
	UT_ASSERT(!cluster_thread_recovery_gate_decide(CLUSTER_THREADREC_SCOPE_NO_SHARED_BACKEND, dead,
												   materialized, 1));
	UT_ASSERT(!cluster_thread_recovery_gate_decide(CLUSTER_THREADREC_SCOPE_MULTI_SURVIVOR, dead,
												   materialized, 1));
}

UT_TEST(test_gate_decide_in_scope_incomplete_stays_frozen)
{
	/* In scope + a dead origin not yet materialized -> STAY FROZEN (true). */
	uint64 dead[1] = { 0x2 };		  /* node 1 dead */
	uint64 materialized[1] = { 0x0 }; /* node 1 NOT materialized */

	UT_ASSERT(cluster_thread_recovery_gate_decide(CLUSTER_THREADREC_SCOPE_APPLICABLE, dead,
												  materialized, 1));
}

UT_TEST(test_gate_decide_in_scope_all_complete_unfreezes)
{
	/* In scope + every dead origin materialized -> may unfreeze (false). */
	uint64 dead[1] = { 0x2 };		  /* node 1 dead */
	uint64 materialized[1] = { 0x2 }; /* node 1 materialized */

	UT_ASSERT(!cluster_thread_recovery_gate_decide(CLUSTER_THREADREC_SCOPE_APPLICABLE, dead,
												   materialized, 1));
}

UT_TEST(test_gate_decide_partial_complete_stays_frozen)
{
	/* Two dead origins, only one materialized -> the OTHER keeps us frozen. */
	uint64 dead[1] = { 0x3 };		  /* nodes 0 and 1 dead */
	uint64 materialized[1] = { 0x1 }; /* only node 0 materialized */

	UT_ASSERT(cluster_thread_recovery_gate_decide(CLUSTER_THREADREC_SCOPE_APPLICABLE, dead,
												  materialized, 1));
}

UT_TEST(test_gate_decide_null_or_empty_is_false)
{
	/* fail-closed inputs: a NULL bitmap or zero words gate nothing (false). */
	uint64 some[1] = { 0x2 };

	UT_ASSERT(
		!cluster_thread_recovery_gate_decide(CLUSTER_THREADREC_SCOPE_APPLICABLE, NULL, some, 1));
	UT_ASSERT(
		!cluster_thread_recovery_gate_decide(CLUSTER_THREADREC_SCOPE_APPLICABLE, some, NULL, 1));
	UT_ASSERT(
		!cluster_thread_recovery_gate_decide(CLUSTER_THREADREC_SCOPE_APPLICABLE, some, some, 0));
}

/*
 * spec-4.11 3b-4b Part 1: the L235 episode-epoch staleness guard.  The
 * per-thread replay slot stamps the GRD recovery_episode_epoch it was launched
 * under; the executor worker re-reads it and ABORTS (keeps the dead thread
 * frozen) if the live episode has moved on.  PURE so the abort boundary is
 * unit-pinned: it must NEVER mistake a moved-on episode for the same one (that
 * would let a stale worker publish authority into a newer reconfig -- 8.A).
 */
UT_TEST(test_epoch_aborts_same_epoch_continues)
{
	/* Slot launched under the live episode -> proceed (no abort). */
	UT_ASSERT(!cluster_thread_recovery_replay_epoch_aborts(7, 7));
	UT_ASSERT(!cluster_thread_recovery_replay_epoch_aborts(0, 0));
}

UT_TEST(test_epoch_aborts_different_epoch_aborts)
{
	/* The live episode advanced past the slot's stamp -> ABORT (stay frozen). */
	UT_ASSERT(cluster_thread_recovery_replay_epoch_aborts(7, 8));
	/* A slot stamped ahead of the observed live epoch is equally suspect. */
	UT_ASSERT(cluster_thread_recovery_replay_epoch_aborts(8, 7));
}

UT_TEST(test_epoch_aborts_unstamped_slot_aborts)
{
	/* An unstamped slot (epoch 0) under any real live episode is stale: it was
	 * never launched for this episode, so it must NOT proceed. */
	UT_ASSERT(cluster_thread_recovery_replay_epoch_aborts(0, 5));
	UT_ASSERT(cluster_thread_recovery_replay_epoch_aborts(5, 0));
}

/*
 * spec-4.11 3b-4b Part 2: the executor worker maps a replay_one verdict to the
 * terminal slot state.  PURE so the fail-closed direction is unit-pinned: ONLY a
 * DONE marks the slot DONE; everything else (BLOCKED, and the defensive
 * NOT_APPLICABLE that an in-scope-launched worker should never see) marks
 * BLOCKED, so the observable slot never claims "done" for an unfinished recovery.
 */
UT_TEST(test_worker_terminal_state_done_only_for_done)
{
	UT_ASSERT_EQ((int)cluster_thread_recovery_worker_terminal_state(CLUSTER_THREADREC_DONE),
				 (int)CLUSTER_THREADREC_REPLAY_DONE);
}

UT_TEST(test_worker_terminal_state_blocked_is_blocked)
{
	UT_ASSERT_EQ((int)cluster_thread_recovery_worker_terminal_state(CLUSTER_THREADREC_BLOCKED),
				 (int)CLUSTER_THREADREC_REPLAY_BLOCKED);
}

UT_TEST(test_worker_terminal_state_not_applicable_is_blocked)
{
	/* NOT_APPLICABLE -> BLOCKED, never DONE: an out-of-scope verdict must not
	 * present as a completed recovery (fail-closed observability). */
	UT_ASSERT_EQ(
		(int)cluster_thread_recovery_worker_terminal_state(CLUSTER_THREADREC_NOT_APPLICABLE),
		(int)CLUSTER_THREADREC_REPLAY_BLOCKED);
}


int
main(void)
{
	UT_PLAN(18);
	UT_RUN(test_on_blocked_keep_frozen_default);
	UT_RUN(test_on_blocked_panic_when_policy_panic);
	UT_RUN(test_on_blocked_unknown_policy_is_keep_frozen);
	UT_RUN(test_on_blocked_default_enum_is_keep_frozen);
	UT_RUN(test_origin_for_tid_real_ids);
	UT_RUN(test_origin_for_tid_legacy_zero_is_invalid);
	UT_RUN(test_origin_for_tid_out_of_range_is_invalid);
	UT_RUN(test_gate_decide_out_of_scope_never_gates);
	UT_RUN(test_gate_decide_in_scope_incomplete_stays_frozen);
	UT_RUN(test_gate_decide_in_scope_all_complete_unfreezes);
	UT_RUN(test_gate_decide_partial_complete_stays_frozen);
	UT_RUN(test_gate_decide_null_or_empty_is_false);
	UT_RUN(test_epoch_aborts_same_epoch_continues);
	UT_RUN(test_epoch_aborts_different_epoch_aborts);
	UT_RUN(test_epoch_aborts_unstamped_slot_aborts);
	UT_RUN(test_worker_terminal_state_done_only_for_done);
	UT_RUN(test_worker_terminal_state_blocked_is_blocked);
	UT_RUN(test_worker_terminal_state_not_applicable_is_blocked);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
