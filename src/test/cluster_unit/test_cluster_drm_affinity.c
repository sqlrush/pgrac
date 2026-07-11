/*-------------------------------------------------------------------------
 *
 * test_cluster_drm_affinity.c
 *	  Standalone unit tests for pgrac DRM per-shard affinity collection.
 *
 *	  Links cluster_drm_affinity.o; ShmemInitStruct + region register +
 *	  backend-hook registration are stubbed, and the GUC globals the module
 *	  reads (cluster_drm_enabled / _sample_rate / _min_access_count) are
 *	  defined here so the real sample/flush/collect/reconcile paths run
 *	  standalone.  Wave 6.3b (spec-7.6): collection substrate only — no GRD /
 *	  GES linkage (the module takes shard_id + node directly), so these tests
 *	  exercise the statistics logic in isolation.  U-noselffeed (retry not
 *	  counted) is an admission-gating property of D2 and is covered by the
 *	  2-node cluster_tap collection leg, not here.
 *
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/test/cluster_unit/test_cluster_drm_affinity.c
 *
 * NOTES
 *	  pgrac-original test.  Spec: spec-7.6-drm-hot-resource-detection-remaster.md
 *	  (wave 6.3b; Amend v1.1-a / v1.1.2).
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <string.h>

#include "access/xact.h" /* XactCallback / XactEvent */
#include "storage/ipc.h" /* pg_on_exit_callback */

#include "cluster/cluster_drm_affinity.h"
#include "unit_test.h"

/* ============================================================
 * PG runtime + cross-module stubs.
 * ============================================================ */

void
ExceptionalCondition(const char *conditionName pg_attribute_unused(),
					 const char *fileName pg_attribute_unused(),
					 int lineNumber pg_attribute_unused())
{
	abort();
}

/*
 * ShmemInitStruct stub — a single static ClusterDrmAffinityShared instance
 * (BSS, naturally aligned) backs the one region this module registers.
 */
void *
ShmemInitStruct(const char *name, Size size, bool *foundPtr)
{
	if (name != NULL && strcmp(name, "pgrac cluster drm affinity") == 0) {
		static ClusterDrmAffinityShared drm_buf;

		Assert(size <= sizeof(drm_buf)); /* catch shmem layout growth */
		/* Always report "not found" so each drm_test_reset() re-inits the region
		 * to a clean slate — the tests want per-test isolation, not shmem-
		 * persistence semantics. */
		*foundPtr = false;
		return &drm_buf;
	}

	*foundPtr = true;
	return NULL;
}

void
cluster_shmem_register_region(const void *r pg_attribute_unused())
{}

/* backend-hook registration deps (register_backend_hooks pulls these in). */
bool IsUnderPostmaster = true;
int MyBackendId = 1;

void
before_shmem_exit(pg_on_exit_callback function pg_attribute_unused(),
				  Datum arg pg_attribute_unused())
{}

void
RegisterXactCallback(XactCallback callback pg_attribute_unused(), void *arg pg_attribute_unused())
{}

/* GUC globals the module reads. */
bool cluster_drm_enabled = false;
int cluster_drm_affinity_sample_rate = 16;
int cluster_drm_min_access_count = 1000;

/* ============================================================
 * Test helpers.
 * ============================================================ */

/*
 * Fresh region + deterministic sampling: rate = 1 makes every sample() hit
 * record (the per-backend countdown fires each call), so tests are exact.
 */
static void
drm_test_reset(void)
{
	cluster_drm_enabled = true;
	cluster_drm_affinity_sample_rate = 1;
	cluster_drm_min_access_count = 1;
	cluster_drm_affinity_shmem_init();			 /* re-inits the region to a clean slate */
	cluster_drm_affinity_reset_local_sampling(); /* drop carried-over per-backend cadence */
}

/*
 * Window-primitive helpers.  The window_* functions are pure with respect to a
 * PASSED slot (no drm_state needed), so these tests build a stack slot directly.
 */
#define WIN_US 10000000ULL /* 10 s window in microseconds */

static void
init_window_slot(ClusterDrmShardAffinity *slot)
{
	int k;

	memset(slot, 0, sizeof(*slot));
	for (k = 0; k < CLUSTER_MAX_NODES; k++)
		pg_atomic_init_u32(&slot->access_count[k], 0);
	pg_atomic_init_u32(&slot->ewma_total, 0);
	pg_atomic_init_u64(&slot->window_start_ts, 0);
	pg_atomic_init_u64(&slot->window_cluster_epoch, 0);
	pg_atomic_init_u32(&slot->window_master_generation, 0);
	pg_atomic_init_u64(&slot->window_sample_epoch, 0);
	slot->consecutive_hot_windows = 0;
	slot->last_migration_ts = 0;
}

/* node 0 (master) = 10, node 1 (dominant) = 100: total 110, ratio 90% — hot. */
static void
set_hot_counts(ClusterDrmShardAffinity *slot)
{
	int k;

	for (k = 0; k < CLUSTER_MAX_NODES; k++)
		pg_atomic_write_u32(&slot->access_count[k], 0);
	pg_atomic_write_u32(&slot->access_count[0], 10);
	pg_atomic_write_u32(&slot->access_count[1], 100);
}

/* total 1 (< min_access 50 at rate 1) — cold. */
static void
set_cold_counts(ClusterDrmShardAffinity *slot)
{
	int k;

	for (k = 0; k < CLUSTER_MAX_NODES; k++)
		pg_atomic_write_u32(&slot->access_count[k], 0);
	pg_atomic_write_u32(&slot->access_count[0], 1);
}

/* Judge + reset n consecutive hot windows; leaves consecutive_hot_windows == n. */
static void
build_hot_streak(ClusterDrmShardAffinity *slot, uint64 *now, int n, uint64 cepoch, uint32 mgen)
{
	int i;

	init_window_slot(slot);
	cluster_drm_window_open(slot, *now, cepoch, mgen);
	for (i = 0; i < n; i++) {
		set_hot_counts(slot);
		*now += WIN_US;
		cluster_drm_window_judge(slot, *now, WIN_US, 1, 50, 70, cepoch, mgen);
		cluster_drm_window_reset(slot, *now, cepoch, mgen);
	}
}

/* ============================================================
 * U-sample-basic — sample() + flush() records into the master matrix.
 * ============================================================ */
UT_TEST(test_drm_sample_records_to_master_matrix)
{
	drm_test_reset();

	cluster_drm_affinity_sample(5, 2, false);
	cluster_drm_affinity_sample(5, 2, false);
	cluster_drm_affinity_sample(5, 3, false);
	cluster_drm_affinity_flush_local_ring();

	UT_ASSERT_EQ(cluster_drm_affinity_access_count(5, 2), 2);
	UT_ASSERT_EQ(cluster_drm_affinity_access_count(5, 3), 1);
	UT_ASSERT_EQ(cluster_drm_affinity_get_counter(CLUSTER_DRM_AFFINITY_CTR_RECORDED), 3);
}

/* ============================================================
 * U5 — INV-DRM9 pure statistics: no cross-shard contamination + off = no record.
 *	(The stronger "master[]/shard_phase[] byte-unchanged" invariant is
 *	structural — this module links no GRD writer — and is asserted end-to-end
 *	by the 2-node collection cluster_tap leg.)
 * ============================================================ */
UT_TEST(test_drm_sample_pure_stats)
{
	drm_test_reset();

	cluster_drm_affinity_sample(5, 2, false);
	cluster_drm_affinity_flush_local_ring();

	/* Neighbour shards untouched — per-shard isolation. */
	UT_ASSERT_EQ(cluster_drm_affinity_access_count(6, 2), 0);
	UT_ASSERT_EQ(cluster_drm_affinity_access_count(4, 2), 0);

	/* drm_enabled = off → sample is a no-op (Amend v1.1-a④ cheap-exit). */
	cluster_drm_enabled = false;
	cluster_drm_affinity_sample(7, 1, false);
	cluster_drm_affinity_flush_local_ring();
	UT_ASSERT_EQ(cluster_drm_affinity_access_count(7, 1), 0);
	UT_ASSERT(cluster_drm_affinity_get_counter(CLUSTER_DRM_AFFINITY_CTR_SKIPPED_OFF) >= 1);
}

/* ============================================================
 * U-collect — candidate = shard crossing (normalized) min_access on this node.
 * ============================================================ */
UT_TEST(test_drm_collect_candidates_threshold)
{
	uint32 out[16];
	int n;
	int i;
	bool saw10 = false;
	bool saw11 = false;

	drm_test_reset();
	cluster_drm_min_access_count = 3; /* rate = 1 → normalized == raw */

	cluster_drm_affinity_sample(10, 1, false);
	cluster_drm_affinity_sample(10, 1, false);
	cluster_drm_affinity_sample(10, 1, false); /* shard 10: 3 hits → candidate */
	cluster_drm_affinity_sample(11, 1, false); /* shard 11: 1 hit  → below     */
	cluster_drm_affinity_flush_local_ring();

	n = cluster_drm_affinity_collect_candidates(out, 16);
	for (i = 0; i < n; i++) {
		if (out[i] == 10)
			saw10 = true;
		if (out[i] == 11)
			saw11 = true;
	}
	UT_ASSERT(saw10);
	UT_ASSERT(!saw11);
}

/* ============================================================
 * U-normalize — collect compares raw*sample_rate vs min_access (Amend v1.1-a⑤).
 *	Tests the normalization formula: a single raw hit at rate 4 counts as 4.
 * ============================================================ */
UT_TEST(test_drm_collect_normalized_by_rate)
{
	uint32 out[16];
	int n;
	int i;
	bool saw20 = false;

	drm_test_reset();

	cluster_drm_affinity_sample(20, 1, false);
	cluster_drm_affinity_sample(20, 1, false); /* raw = 2 recorded at rate 1 */
	cluster_drm_affinity_flush_local_ring();

	/* Now evaluate as if the active sample rate were 4: normalized = 2*4 = 8. */
	cluster_drm_affinity_sample_rate = 4;

	cluster_drm_min_access_count = 8; /* 8 >= 8 → candidate */
	n = cluster_drm_affinity_collect_candidates(out, 16);
	for (i = 0; i < n; i++)
		if (out[i] == 20)
			saw20 = true;
	UT_ASSERT(saw20);

	cluster_drm_min_access_count = 9; /* 8 < 9 → not a candidate */
	saw20 = false;
	n = cluster_drm_affinity_collect_candidates(out, 16);
	for (i = 0; i < n; i++)
		if (out[i] == 20)
			saw20 = true;
	UT_ASSERT(!saw20);
}

/* ============================================================
 * U-reconcile — sample_rate change reopens the window exactly ONCE and bumps
 *	the shared sample_epoch (Amend v1.1.2 R4.2, LMON single-writer).
 * ============================================================ */
UT_TEST(test_drm_reconcile_reopens_window_once)
{
	uint64 epoch0;
	uint64 epoch1;

	drm_test_reset();
	epoch0 = cluster_drm_affinity_get_sample_epoch();

	/* No change yet → reconcile is a no-op. */
	UT_ASSERT(!cluster_drm_affinity_reconcile_sample_rate());
	UT_ASSERT_EQ(cluster_drm_affinity_get_sample_epoch(), epoch0);

	/* Operator changes the rate → first reconcile reopens + bumps epoch. */
	cluster_drm_affinity_sample_rate = 8;
	UT_ASSERT(cluster_drm_affinity_reconcile_sample_rate());
	epoch1 = cluster_drm_affinity_get_sample_epoch();
	UT_ASSERT(epoch1 > epoch0);

	/* Same rate again → no second reopen (idempotent). */
	UT_ASSERT(!cluster_drm_affinity_reconcile_sample_rate());
	UT_ASSERT_EQ(cluster_drm_affinity_get_sample_epoch(), epoch1);
}

/* ============================================================
 * U-ring-lifecycle — a flush after a sample_epoch bump drops the stale batch
 *	(Amend v1.1.2 R4.3): ring entries carry the sample_epoch they were taken at.
 * ============================================================ */
UT_TEST(test_drm_ring_flush_drops_stale_epoch)
{
	uint64 dropped0;

	drm_test_reset();

	/* Sample under the current epoch but DON'T flush yet. */
	cluster_drm_affinity_sample(30, 1, false);

	/* Operator bumps the rate → LMON reconcile bumps sample_epoch. */
	cluster_drm_affinity_sample_rate = 8;
	UT_ASSERT(cluster_drm_affinity_reconcile_sample_rate());

	/* The buffered sample is now stale → flush drops it, matrix unchanged. */
	dropped0 = cluster_drm_affinity_get_counter(CLUSTER_DRM_AFFINITY_CTR_DROPPED_STALE);
	cluster_drm_affinity_flush_local_ring();
	UT_ASSERT_EQ(cluster_drm_affinity_access_count(30, 1), 0);
	UT_ASSERT(cluster_drm_affinity_get_counter(CLUSTER_DRM_AFFINITY_CTR_DROPPED_STALE) > dropped0);
}

/* ============================================================
 * U-window-identity — after a sample_epoch bump, a shard sampled under the old
 *	epoch is not a candidate until re-sampled at the new epoch (Amend v1.1.2 R5).
 * ============================================================ */
UT_TEST(test_drm_window_identity_discards_stale)
{
	uint32 out[16];
	int n;
	int i;
	bool saw40;

	drm_test_reset();
	cluster_drm_min_access_count = 1;

	cluster_drm_affinity_sample(40, 1, false);
	cluster_drm_affinity_flush_local_ring();

	/* Candidate under the current epoch. */
	saw40 = false;
	n = cluster_drm_affinity_collect_candidates(out, 16);
	for (i = 0; i < n; i++)
		if (out[i] == 40)
			saw40 = true;
	UT_ASSERT(saw40);

	/* Bump sample_epoch → shard 40's window is stale → not a candidate. */
	cluster_drm_affinity_sample_rate = 8;
	UT_ASSERT(cluster_drm_affinity_reconcile_sample_rate());
	saw40 = false;
	n = cluster_drm_affinity_collect_candidates(out, 16);
	for (i = 0; i < n; i++)
		if (out[i] == 40)
			saw40 = true;
	UT_ASSERT(!saw40);

	/* Re-sample at the new epoch → candidate again. */
	cluster_drm_affinity_sample(40, 1, false);
	cluster_drm_affinity_flush_local_ring();
	saw40 = false;
	n = cluster_drm_affinity_collect_candidates(out, 16);
	for (i = 0; i < n; i++)
		if (out[i] == 40)
			saw40 = true;
	UT_ASSERT(saw40);
}

/* ============================================================
 * U-local-remote-split — was_remote splits samples_local / samples_remote for
 *	observability, but BOTH are recorded into the matrix (Amend v1.1-a①: never
 *	filter to remote-only, else the master is blind to its own local heat).
 *	This is the counter surface the 2-node collection cluster_tap leg asserts
 *	self + peer on the current master with.
 * ============================================================ */
UT_TEST(test_drm_local_remote_split)
{
	drm_test_reset();

	cluster_drm_affinity_sample(50, 0, false); /* local  (self)  */
	cluster_drm_affinity_sample(50, 1, true);  /* remote (peer)  */
	cluster_drm_affinity_flush_local_ring();

	/* Both recorded — the master sees its own AND the peer's access. */
	UT_ASSERT_EQ(cluster_drm_affinity_access_count(50, 0), 1);
	UT_ASSERT_EQ(cluster_drm_affinity_access_count(50, 1), 1);
	UT_ASSERT_EQ(cluster_drm_affinity_get_counter(CLUSTER_DRM_AFFINITY_CTR_LOCAL), 1);
	UT_ASSERT_EQ(cluster_drm_affinity_get_counter(CLUSTER_DRM_AFFINITY_CTR_REMOTE), 1);
	UT_ASSERT_EQ(cluster_drm_affinity_get_counter(CLUSTER_DRM_AFFINITY_CTR_RECORDED), 2);
}

/* ============================================================
 * U-window-open-due — a fresh slot is not open; open() anchors + stamps
 *	identity; the window is due only once window_us has elapsed (Amend v1.1-b).
 * ============================================================ */
UT_TEST(test_drm_window_open_and_due)
{
	ClusterDrmShardAffinity slot;
	uint64 now = 1000000000ULL;

	init_window_slot(&slot);
	UT_ASSERT(!cluster_drm_window_is_open(&slot));

	cluster_drm_window_open(&slot, now, 7, 3);
	UT_ASSERT(cluster_drm_window_is_open(&slot));
	UT_ASSERT(!cluster_drm_window_due(&slot, now, WIN_US));				 /* just opened  */
	UT_ASSERT(!cluster_drm_window_due(&slot, now + WIN_US / 2, WIN_US)); /* half window  */
	UT_ASSERT(cluster_drm_window_due(&slot, now + WIN_US, WIN_US));		 /* full window  */

	UT_ASSERT_EQ(pg_atomic_read_u64(&slot.window_cluster_epoch), 7);
	UT_ASSERT_EQ(pg_atomic_read_u32(&slot.window_master_generation), 3);
}

/* ============================================================
 * U-tumbling — each completed hot window increments consecutive_hot_windows
 *	exactly once (tumbling / non-overlapping); reset clears the per-node counts.
 * ============================================================ */
UT_TEST(test_drm_window_judge_hot_streak)
{
	ClusterDrmShardAffinity slot;
	uint64 now = 1000000000ULL;
	bool hot;

	init_window_slot(&slot);
	cluster_drm_window_open(&slot, now, 7, 3);

	set_hot_counts(&slot);
	now += WIN_US;
	hot = cluster_drm_window_judge(&slot, now, WIN_US, 1, 50, 70, 7, 3);
	UT_ASSERT(hot);
	UT_ASSERT_EQ(slot.consecutive_hot_windows, 1);
	cluster_drm_window_reset(&slot, now, 7, 3);
	UT_ASSERT_EQ(pg_atomic_read_u32(&slot.access_count[1]), 0); /* reset cleared counts */

	set_hot_counts(&slot);
	now += WIN_US;
	hot = cluster_drm_window_judge(&slot, now, WIN_US, 1, 50, 70, 7, 3);
	UT_ASSERT(hot);
	UT_ASSERT_EQ(slot.consecutive_hot_windows, 2);
}

/* ============================================================
 * U-tumbling-cold — a cold window (below min_access) clears the streak to 0.
 * ============================================================ */
UT_TEST(test_drm_window_judge_cold_clears_streak)
{
	ClusterDrmShardAffinity slot;
	uint64 now = 1000000000ULL;
	bool hot;

	build_hot_streak(&slot, &now, 2, 7, 3);
	UT_ASSERT_EQ(slot.consecutive_hot_windows, 2);

	set_cold_counts(&slot);
	now += WIN_US;
	hot = cluster_drm_window_judge(&slot, now, WIN_US, 1, 50, 70, 7, 3);
	UT_ASSERT(!hot);
	UT_ASSERT_EQ(slot.consecutive_hot_windows, 0);
}

/* ============================================================
 * U-window-identity (R5) — a window whose {cluster_epoch, master_generation}
 *	identity changed spanned a remaster: the streak breaks (no stale heat).
 * ============================================================ */
UT_TEST(test_drm_window_judge_identity_change_breaks)
{
	ClusterDrmShardAffinity slot;
	uint64 now = 1000000000ULL;
	bool hot;

	build_hot_streak(&slot, &now, 2, 7, 3);

	set_hot_counts(&slot);
	now += WIN_US;
	/* master generation moved 3 -> 4 while the window was open. */
	hot = cluster_drm_window_judge(&slot, now, WIN_US, 1, 50, 70, 7, 4);
	UT_ASSERT(!hot);
	UT_ASSERT_EQ(slot.consecutive_hot_windows, 0);
}

/* ============================================================
 * U-window-overdue — a grossly overdue window means the shard went cold and
 *	stopped being scanned: the streak breaks even if the counts now look hot.
 * ============================================================ */
UT_TEST(test_drm_window_judge_overdue_breaks)
{
	ClusterDrmShardAffinity slot;
	uint64 now = 1000000000ULL;
	bool hot;

	build_hot_streak(&slot, &now, 2, 7, 3);

	set_hot_counts(&slot);
	now += 3 * WIN_US; /* anchor is > 2*window in the past */
	hot = cluster_drm_window_judge(&slot, now, WIN_US, 1, 50, 70, 7, 3);
	UT_ASSERT(!hot);
	UT_ASSERT_EQ(slot.consecutive_hot_windows, 0);
}

/* ============================================================
 * U-window-cap — consecutive_hot_windows saturates, never overflows.
 * ============================================================ */
UT_TEST(test_drm_window_consecutive_cap)
{
	ClusterDrmShardAffinity slot;
	uint64 now = 1000000000ULL;
	bool hot;

	init_window_slot(&slot);
	cluster_drm_window_open(&slot, now, 7, 3);
	slot.consecutive_hot_windows = CLUSTER_DRM_WINDOW_CONSECUTIVE_MAX;

	set_hot_counts(&slot);
	now += WIN_US;
	hot = cluster_drm_window_judge(&slot, now, WIN_US, 1, 50, 70, 7, 3);
	UT_ASSERT(hot);
	UT_ASSERT_EQ(slot.consecutive_hot_windows, CLUSTER_DRM_WINDOW_CONSECUTIVE_MAX);
}

/* ============================================================
 * U-scan-counters — the LMON scan observability surface tallies runs,
 *	candidates, per-reason verdicts, and auto-actionable proposals.
 * ============================================================ */
UT_TEST(test_drm_scan_observability_counters)
{
	drm_test_reset();

	cluster_drm_affinity_record_scan_run();
	cluster_drm_affinity_record_scan_run();
	cluster_drm_affinity_record_verdict(0, true);  /* migrate, proposed */
	cluster_drm_affinity_record_verdict(1, false); /* a skip            */
	cluster_drm_affinity_record_verdict(1, false); /* a skip            */

	UT_ASSERT_EQ(cluster_drm_affinity_get_scan_counter(CLUSTER_DRM_AFFINITY_SCAN_RUNS), 2);
	UT_ASSERT_EQ(cluster_drm_affinity_get_scan_counter(CLUSTER_DRM_AFFINITY_SCAN_CANDIDATES), 3);
	UT_ASSERT_EQ(cluster_drm_affinity_get_scan_counter(CLUSTER_DRM_AFFINITY_SCAN_PROPOSED), 1);
	UT_ASSERT_EQ(cluster_drm_affinity_get_scan_reason(0), 1);
	UT_ASSERT_EQ(cluster_drm_affinity_get_scan_reason(1), 2);
}

/* ============================================================ */

UT_DEFINE_GLOBALS();

int
main(int argc pg_attribute_unused(), char **const argv pg_attribute_unused())
{
	UT_PLAN(15);
	UT_RUN(test_drm_sample_records_to_master_matrix);
	UT_RUN(test_drm_sample_pure_stats);
	UT_RUN(test_drm_collect_candidates_threshold);
	UT_RUN(test_drm_collect_normalized_by_rate);
	UT_RUN(test_drm_reconcile_reopens_window_once);
	UT_RUN(test_drm_ring_flush_drops_stale_epoch);
	UT_RUN(test_drm_window_identity_discards_stale);
	UT_RUN(test_drm_local_remote_split);
	UT_RUN(test_drm_window_open_and_due);
	UT_RUN(test_drm_window_judge_hot_streak);
	UT_RUN(test_drm_window_judge_cold_clears_streak);
	UT_RUN(test_drm_window_judge_identity_change_breaks);
	UT_RUN(test_drm_window_judge_overdue_breaks);
	UT_RUN(test_drm_window_consecutive_cap);
	UT_RUN(test_drm_scan_observability_counters);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
