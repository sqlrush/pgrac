/*-------------------------------------------------------------------------
 *
 * test_cluster_recovery_plan.c
 *	  pgrac spec-4.3 D6 — cluster_unit tests for the recovery-plan pure
 *	  classifier (cluster_recovery_plan.h, header-only inline).
 *
 *	  16 tests covering the §3.2 truth table 1:1:
 *	    T1   priority 0: own thread -> OWN for EVERY slot verdict
 *	         (EMPTY / CORRUPT / OK+STOPPED / OK+ACTIVE-stale) -- P1-1:
 *	         at plan time the own slot is usually EMPTY or STOPPED
 *	    T2   EMPTY slot -> EMPTY
 *	    T3   OK + STOPPED -> CLEAN
 *	    T4   OK + ACTIVE + fresh -> ALIVE
 *	    T5   OK + ACTIVE + stale -> CRASHED_CANDIDATE
 *	    T6   CORRUPT -> UNKNOWN
 *	    T7   boundary: now - last_updated == stale_ms -> ALIVE ('>' is
 *	         stale)
 *	    T8   future last_updated (clock skew) -> ALIVE (conservative)
 *	    T9   P2: OK slot with node_id != tid-1 (identity invariant
 *	         violation, CRC-valid impossible owner) -> UNKNOWN, both
 *	         for ACTIVE and STOPPED states
 *	    T10  P2: node_id out of range (>= 128 via mismatch) -> UNKNOWN
 *	    T11  bitmap addressing locks: tid 1 -> word0 bit0, tid 64 ->
 *	         word0 bit63, tid 65 -> word1 bit0, tid 128 -> word1 bit63
 *	    T12  bitmap test/set round-trip + independence
 *	    T13  enum on-disk-stable values (verdict[] is uint8 in shmem)
 *	    T14  aggregate conservation: sum of per-verdict counts + own ==
 *	         threads scanned (simulated 128-slot sweep)
 *	    T15  bitmap<->verdict[] coherence on the simulated sweep
 *	    T16  verdict[] array bounds: THREADS+1 entries, [0] stays NONE
 *
 *	  Linkage mirrors test_cluster_wal_state: header-only inclusion +
 *	  libpgcommon/libpgport for pg_crc32c (slot fill helper) -- no
 *	  module .o, no stubs.
 *
 * Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * Spec: spec-4.3-recovery-coordinator-skeleton.md (FROZEN v1.0)
 *
 *-------------------------------------------------------------------------
 */
#define USE_PGRAC_CLUSTER 1

#include "postgres.h"

#include <stddef.h>
#include <string.h>

#include "cluster/cluster_recovery_plan.h"

#undef printf
#undef fprintf
#undef snprintf

#include "unit_test.h"

UT_DEFINE_GLOBALS();


/* cassert builds pull libpgport objects that reference this. */
void
ExceptionalCondition(const char *conditionName pg_attribute_unused(),
					 const char *fileName pg_attribute_unused(),
					 int lineNumber pg_attribute_unused())
{
	abort();
}

#define OWN_TID ((uint16)4)
#define STALE_MS 10000
#define NOW_US ((int64)1000000000) /* arbitrary plan-time clock */

/* A fresh last_updated: well inside the stale window. */
#define FRESH_US (NOW_US - (int64)1000 * 1000)
/* A stale last_updated: one full window + 1ms beyond. */
#define STALE_US (NOW_US - (int64)(STALE_MS + 1) * 1000)

static void
fill_ok_slot(ClusterWalStateSlot *s, uint16 tid, int32 node_id, uint32 state, int64 last_updated)
{
	cluster_wal_state_slot_fill(s, tid, node_id, state, 1, 100, last_updated, 7, 9);
}

UT_TEST(test_own_priority_beats_every_verdict)
{
	ClusterWalStateSlot s;

	/* own + EMPTY (first boot: ACTIVE publishes only at RUNNING) */
	memset(&s, 0, sizeof(s));
	UT_ASSERT_EQ((int)cluster_recovery_classify_slot(CLUSTER_WAL_SLOT_EMPTY, &s, OWN_TID, OWN_TID,
													 NOW_US, STALE_MS),
				 (int)CLUSTER_RECOVERY_THREAD_OWN);

	/* own + CORRUPT */
	UT_ASSERT_EQ((int)cluster_recovery_classify_slot(CLUSTER_WAL_SLOT_CORRUPT, &s, OWN_TID, OWN_TID,
													 NOW_US, STALE_MS),
				 (int)CLUSTER_RECOVERY_THREAD_OWN);

	/* own + STOPPED (clean shutdown) */
	fill_ok_slot(&s, OWN_TID, OWN_TID - 1, CLUSTER_WAL_SLOT_STATE_STOPPED, FRESH_US);
	UT_ASSERT_EQ((int)cluster_recovery_classify_slot(CLUSTER_WAL_SLOT_OK, &s, OWN_TID, OWN_TID,
													 NOW_US, STALE_MS),
				 (int)CLUSTER_RECOVERY_THREAD_OWN);

	/* own + ACTIVE + stale (own crash: still OWN, PG native replay) */
	fill_ok_slot(&s, OWN_TID, OWN_TID - 1, CLUSTER_WAL_SLOT_STATE_ACTIVE, STALE_US);
	UT_ASSERT_EQ((int)cluster_recovery_classify_slot(CLUSTER_WAL_SLOT_OK, &s, OWN_TID, OWN_TID,
													 NOW_US, STALE_MS),
				 (int)CLUSTER_RECOVERY_THREAD_OWN);
}

UT_TEST(test_empty_slot)
{
	ClusterWalStateSlot s;

	memset(&s, 0, sizeof(s));
	UT_ASSERT_EQ((int)cluster_recovery_classify_slot(CLUSTER_WAL_SLOT_EMPTY, &s, OWN_TID, 9, NOW_US,
													 STALE_MS),
				 (int)CLUSTER_RECOVERY_THREAD_EMPTY);
}

UT_TEST(test_stopped_is_clean)
{
	ClusterWalStateSlot s;

	fill_ok_slot(&s, 9, 8, CLUSTER_WAL_SLOT_STATE_STOPPED, STALE_US);
	UT_ASSERT_EQ(
		(int)cluster_recovery_classify_slot(CLUSTER_WAL_SLOT_OK, &s, OWN_TID, 9, NOW_US, STALE_MS),
		(int)CLUSTER_RECOVERY_THREAD_CLEAN);
}

UT_TEST(test_active_fresh_is_alive)
{
	ClusterWalStateSlot s;

	fill_ok_slot(&s, 9, 8, CLUSTER_WAL_SLOT_STATE_ACTIVE, FRESH_US);
	UT_ASSERT_EQ(
		(int)cluster_recovery_classify_slot(CLUSTER_WAL_SLOT_OK, &s, OWN_TID, 9, NOW_US, STALE_MS),
		(int)CLUSTER_RECOVERY_THREAD_ALIVE);
}

UT_TEST(test_active_stale_is_crashed_candidate)
{
	ClusterWalStateSlot s;

	fill_ok_slot(&s, 9, 8, CLUSTER_WAL_SLOT_STATE_ACTIVE, STALE_US);
	UT_ASSERT_EQ(
		(int)cluster_recovery_classify_slot(CLUSTER_WAL_SLOT_OK, &s, OWN_TID, 9, NOW_US, STALE_MS),
		(int)CLUSTER_RECOVERY_THREAD_CRASHED_CANDIDATE);
}

UT_TEST(test_corrupt_is_unknown)
{
	ClusterWalStateSlot s;

	memset(&s, 0xAA, sizeof(s));
	UT_ASSERT_EQ((int)cluster_recovery_classify_slot(CLUSTER_WAL_SLOT_CORRUPT, &s, OWN_TID, 9,
													 NOW_US, STALE_MS),
				 (int)CLUSTER_RECOVERY_THREAD_UNKNOWN);
}

UT_TEST(test_boundary_exactly_stale_ms_is_alive)
{
	ClusterWalStateSlot s;
	int64 exactly = NOW_US - (int64)STALE_MS * 1000;

	fill_ok_slot(&s, 9, 8, CLUSTER_WAL_SLOT_STATE_ACTIVE, exactly);
	UT_ASSERT_EQ(
		(int)cluster_recovery_classify_slot(CLUSTER_WAL_SLOT_OK, &s, OWN_TID, 9, NOW_US, STALE_MS),
		(int)CLUSTER_RECOVERY_THREAD_ALIVE);

	/* one microsecond past the window flips to CRASHED_CANDIDATE */
	fill_ok_slot(&s, 9, 8, CLUSTER_WAL_SLOT_STATE_ACTIVE, exactly - 1);
	UT_ASSERT_EQ(
		(int)cluster_recovery_classify_slot(CLUSTER_WAL_SLOT_OK, &s, OWN_TID, 9, NOW_US, STALE_MS),
		(int)CLUSTER_RECOVERY_THREAD_CRASHED_CANDIDATE);
}

UT_TEST(test_future_timestamp_is_alive)
{
	ClusterWalStateSlot s;

	fill_ok_slot(&s, 9, 8, CLUSTER_WAL_SLOT_STATE_ACTIVE, NOW_US + 5000000);
	UT_ASSERT_EQ(
		(int)cluster_recovery_classify_slot(CLUSTER_WAL_SLOT_OK, &s, OWN_TID, 9, NOW_US, STALE_MS),
		(int)CLUSTER_RECOVERY_THREAD_ALIVE);
}

UT_TEST(test_identity_invariant_violation_is_unknown)
{
	ClusterWalStateSlot s;

	/* CRC-valid ACTIVE slot claiming an impossible owner: node 7 in
	 * slot 9 (invariant says node_id == tid - 1 == 8).  Never ALIVE,
	 * never CRASHED_CANDIDATE. */
	fill_ok_slot(&s, 9, 7, CLUSTER_WAL_SLOT_STATE_ACTIVE, FRESH_US);
	UT_ASSERT_EQ(
		(int)cluster_recovery_classify_slot(CLUSTER_WAL_SLOT_OK, &s, OWN_TID, 9, NOW_US, STALE_MS),
		(int)CLUSTER_RECOVERY_THREAD_UNKNOWN);

	fill_ok_slot(&s, 9, 7, CLUSTER_WAL_SLOT_STATE_ACTIVE, STALE_US);
	UT_ASSERT_EQ(
		(int)cluster_recovery_classify_slot(CLUSTER_WAL_SLOT_OK, &s, OWN_TID, 9, NOW_US, STALE_MS),
		(int)CLUSTER_RECOVERY_THREAD_UNKNOWN);

	fill_ok_slot(&s, 9, 7, CLUSTER_WAL_SLOT_STATE_STOPPED, FRESH_US);
	UT_ASSERT_EQ(
		(int)cluster_recovery_classify_slot(CLUSTER_WAL_SLOT_OK, &s, OWN_TID, 9, NOW_US, STALE_MS),
		(int)CLUSTER_RECOVERY_THREAD_UNKNOWN);
}

UT_TEST(test_node_id_out_of_range_is_unknown)
{
	ClusterWalStateSlot s;

	fill_ok_slot(&s, 9, 127, CLUSTER_WAL_SLOT_STATE_ACTIVE, FRESH_US);
	UT_ASSERT_EQ(
		(int)cluster_recovery_classify_slot(CLUSTER_WAL_SLOT_OK, &s, OWN_TID, 9, NOW_US, STALE_MS),
		(int)CLUSTER_RECOVERY_THREAD_UNKNOWN);

	fill_ok_slot(&s, 9, -1, CLUSTER_WAL_SLOT_STATE_ACTIVE, FRESH_US);
	UT_ASSERT_EQ(
		(int)cluster_recovery_classify_slot(CLUSTER_WAL_SLOT_OK, &s, OWN_TID, 9, NOW_US, STALE_MS),
		(int)CLUSTER_RECOVERY_THREAD_UNKNOWN);
}

UT_TEST(test_bitmap_addressing_locks)
{
	ClusterRecoveryPlan plan;

	memset(&plan, 0, sizeof(plan));
	cluster_recovery_plan_candidate_set(&plan, 1);
	UT_ASSERT_EQ((long long)plan.candidate_bitmap[0], 1LL);
	memset(&plan, 0, sizeof(plan));
	cluster_recovery_plan_candidate_set(&plan, 64);
	UT_ASSERT((plan.candidate_bitmap[0] >> 63) == 1 && plan.candidate_bitmap[1] == 0);
	memset(&plan, 0, sizeof(plan));
	cluster_recovery_plan_candidate_set(&plan, 65);
	UT_ASSERT(plan.candidate_bitmap[0] == 0 && plan.candidate_bitmap[1] == 1);
	memset(&plan, 0, sizeof(plan));
	cluster_recovery_plan_candidate_set(&plan, 128);
	UT_ASSERT(plan.candidate_bitmap[0] == 0 && (plan.candidate_bitmap[1] >> 63) == 1);
}

UT_TEST(test_bitmap_roundtrip_independence)
{
	ClusterRecoveryPlan plan;
	uint16 tid;

	memset(&plan, 0, sizeof(plan));
	cluster_recovery_plan_candidate_set(&plan, 7);
	cluster_recovery_plan_candidate_set(&plan, 100);
	for (tid = 1; tid <= CLUSTER_RECOVERY_PLAN_THREADS; tid++) {
		bool want = (tid == 7 || tid == 100);

		UT_ASSERT_EQ((int)cluster_recovery_plan_candidate_test(&plan, tid), (int)want);
	}
}

UT_TEST(test_verdict_enum_values_stable)
{
	/* verdict[] is uint8 in the shmem mirror: pin the values. */
	UT_ASSERT_EQ((int)CLUSTER_RECOVERY_THREAD_NONE, 0);
	UT_ASSERT_EQ((int)CLUSTER_RECOVERY_THREAD_OWN, 1);
	UT_ASSERT_EQ((int)CLUSTER_RECOVERY_THREAD_CLEAN, 2);
	UT_ASSERT_EQ((int)CLUSTER_RECOVERY_THREAD_EMPTY, 3);
	UT_ASSERT_EQ((int)CLUSTER_RECOVERY_THREAD_CRASHED_CANDIDATE, 4);
	UT_ASSERT_EQ((int)CLUSTER_RECOVERY_THREAD_ALIVE, 5);
	UT_ASSERT_EQ((int)CLUSTER_RECOVERY_THREAD_UNKNOWN, 6);
}

/*
 * Simulated 128-slot sweep: the aggregation loop the backend runs,
 * reproduced here against synthetic slots to lock conservation and
 * bitmap<->verdict coherence (T14/T15/T16).
 */
static void
simulated_sweep(ClusterRecoveryPlan *plan)
{
	uint16 tid;

	memset(plan, 0, sizeof(*plan));
	plan->own_thread = OWN_TID;
	for (tid = 1; tid <= CLUSTER_RECOVERY_PLAN_THREADS; tid++) {
		ClusterWalStateSlot s;
		ClusterWalSlotVerdict v;
		ClusterRecoveryThreadVerdict verdict;

		if (tid == 9) { /* stale ACTIVE -> candidate */
			fill_ok_slot(&s, tid, (int32)tid - 1, CLUSTER_WAL_SLOT_STATE_ACTIVE, STALE_US);
			v = CLUSTER_WAL_SLOT_OK;
		} else if (tid == 12) { /* fresh ACTIVE -> alive */
			fill_ok_slot(&s, tid, (int32)tid - 1, CLUSTER_WAL_SLOT_STATE_ACTIVE, FRESH_US);
			v = CLUSTER_WAL_SLOT_OK;
		} else if (tid == 20) { /* clean stop */
			fill_ok_slot(&s, tid, (int32)tid - 1, CLUSTER_WAL_SLOT_STATE_STOPPED, FRESH_US);
			v = CLUSTER_WAL_SLOT_OK;
		} else if (tid == 33) { /* torn slot */
			memset(&s, 0xAA, sizeof(s));
			v = CLUSTER_WAL_SLOT_CORRUPT;
		} else if (tid == 40) { /* identity violation */
			fill_ok_slot(&s, tid, 7, CLUSTER_WAL_SLOT_STATE_ACTIVE, FRESH_US);
			v = CLUSTER_WAL_SLOT_OK;
		} else {
			memset(&s, 0, sizeof(s));
			v = CLUSTER_WAL_SLOT_EMPTY;
		}

		verdict = cluster_recovery_classify_slot(v, &s, OWN_TID, tid, NOW_US, STALE_MS);
		plan->verdict[tid] = (uint8)verdict;
		switch (verdict) {
		case CLUSTER_RECOVERY_THREAD_CLEAN:
			plan->n_clean++;
			break;
		case CLUSTER_RECOVERY_THREAD_EMPTY:
			plan->n_empty++;
			break;
		case CLUSTER_RECOVERY_THREAD_CRASHED_CANDIDATE:
			plan->n_crashed_candidate++;
			cluster_recovery_plan_candidate_set(plan, tid);
			break;
		case CLUSTER_RECOVERY_THREAD_ALIVE:
			plan->n_alive++;
			break;
		case CLUSTER_RECOVERY_THREAD_UNKNOWN:
			plan->n_unknown++;
			break;
		default:
			break;
		}
		plan->threads_scanned++;
	}
}

UT_TEST(test_sweep_count_conservation)
{
	ClusterRecoveryPlan plan;

	simulated_sweep(&plan);
	UT_ASSERT_EQ((int)plan.threads_scanned, CLUSTER_RECOVERY_PLAN_THREADS);
	/* own(1) + clean(1) + alive(1) + crashed(1) + unknown(2) + empty */
	UT_ASSERT_EQ((int)plan.n_crashed_candidate, 1);
	UT_ASSERT_EQ((int)plan.n_alive, 1);
	UT_ASSERT_EQ((int)plan.n_clean, 1);
	UT_ASSERT_EQ((int)plan.n_unknown, 2);
	UT_ASSERT_EQ((int)plan.n_empty, CLUSTER_RECOVERY_PLAN_THREADS - 6);
	UT_ASSERT_EQ((int)(plan.n_clean + plan.n_empty + plan.n_crashed_candidate + plan.n_alive
					   + plan.n_unknown + 1 /* own */),
				 (int)plan.threads_scanned);
}

UT_TEST(test_sweep_bitmap_verdict_coherence)
{
	ClusterRecoveryPlan plan;
	uint16 tid;

	simulated_sweep(&plan);
	for (tid = 1; tid <= CLUSTER_RECOVERY_PLAN_THREADS; tid++) {
		bool in_bitmap = cluster_recovery_plan_candidate_test(&plan, tid);
		bool is_crashed = plan.verdict[tid] == (uint8)CLUSTER_RECOVERY_THREAD_CRASHED_CANDIDATE;

		UT_ASSERT_EQ((int)in_bitmap, (int)is_crashed);
	}
	UT_ASSERT(cluster_recovery_plan_candidate_test(&plan, 9));
}

UT_TEST(test_verdict_array_bounds_and_zero_slot)
{
	ClusterRecoveryPlan plan;

	UT_ASSERT_EQ((int)sizeof(plan.verdict), CLUSTER_RECOVERY_PLAN_THREADS + 1);
	simulated_sweep(&plan);
	UT_ASSERT_EQ((int)plan.verdict[0], (int)CLUSTER_RECOVERY_THREAD_NONE);
	UT_ASSERT_EQ((int)plan.verdict[OWN_TID], (int)CLUSTER_RECOVERY_THREAD_OWN);
}


int
main(int argc, char **argv)
{
	UT_PLAN(16);

	UT_RUN(test_own_priority_beats_every_verdict);
	UT_RUN(test_empty_slot);
	UT_RUN(test_stopped_is_clean);
	UT_RUN(test_active_fresh_is_alive);
	UT_RUN(test_active_stale_is_crashed_candidate);
	UT_RUN(test_corrupt_is_unknown);
	UT_RUN(test_boundary_exactly_stale_ms_is_alive);
	UT_RUN(test_future_timestamp_is_alive);
	UT_RUN(test_identity_invariant_violation_is_unknown);
	UT_RUN(test_node_id_out_of_range_is_unknown);
	UT_RUN(test_bitmap_addressing_locks);
	UT_RUN(test_bitmap_roundtrip_independence);
	UT_RUN(test_verdict_enum_values_stable);
	UT_RUN(test_sweep_count_conservation);
	UT_RUN(test_sweep_bitmap_verdict_coherence);
	UT_RUN(test_verdict_array_bounds_and_zero_slot);

	UT_DONE();
	return ut_failed_count != 0 ? 1 : 0;
}
