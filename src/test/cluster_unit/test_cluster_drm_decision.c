/*-------------------------------------------------------------------------
 *
 * test_cluster_drm_decision.c
 *	  Standalone unit tests for the pgrac DRM hotness decision engine.
 *
 *	  Links cluster_drm_decision.o.  cluster_drm_evaluate_shard is a pure
 *	  predicate (spec-7.6 6.3c / Amend v1.1-c), so the only externals are the
 *	  drm.* GUC globals it reads — defined here — and pg_atomic (header inline).
 *	  Each gate is exercised by perturbing ONE field of an all-gates-pass base
 *	  scenario.
 *
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/test/cluster_unit/test_cluster_drm_decision.c
 *
 * NOTES
 *	  pgrac-original test.  Spec: spec-7.6-drm-hot-resource-detection-remaster.md
 *	  (wave 6.3c; Amend v1.1-b/c/d).
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <string.h>

#include "cluster/cluster_drm_decision.h"
#include "unit_test.h"

/* PG runtime stub. */
void
ExceptionalCondition(const char *conditionName pg_attribute_unused(),
					 const char *fileName pg_attribute_unused(),
					 int lineNumber pg_attribute_unused())
{
	abort();
}

/* drm.* GUC globals the decision engine reads. */
int cluster_drm_min_access_count = 50;
int cluster_drm_affinity_ratio_pct = 70;
int cluster_drm_consecutive_triggers = 3;
int cluster_drm_affinity_window_ms = 10000;
int cluster_drm_cooldown_ms = 600000;
int cluster_drm_max_migrations_per_scan = 2;
int cluster_drm_migration_cost = 100;

/* ============================================================
 * Helpers.
 * ============================================================ */

static ClusterDrmShardAffinity g_slot;
static uint8 g_members[16];

/* Build a slot: node 0 (current master) = master_access, node 1 (dominant) =
 * dom_access, everything else cold. */
static void
build_slot(uint32 master_access, uint32 dom_access, uint32 consecutive, uint64 last_mig_ts)
{
	int k;

	memset(&g_slot, 0, sizeof(g_slot));
	for (k = 0; k < CLUSTER_MAX_NODES; k++)
		pg_atomic_init_u32(&g_slot.access_count[k], 0);
	pg_atomic_write_u32(&g_slot.access_count[0], master_access);
	pg_atomic_write_u32(&g_slot.access_count[1], dom_access);
	g_slot.consecutive_hot_windows = consecutive;
	g_slot.last_migration_ts = last_mig_ts;
}

static ClusterDrmDecisionCtx
base_ctx(void)
{
	ClusterDrmDecisionCtx ctx;

	memset(g_members, 0, sizeof(g_members));
	g_members[0] |= 1; /* node 0 member */
	g_members[0] |= 2; /* node 1 member */

	ctx.current_master = 0;
	ctx.now_us = 1000000000ULL; /* far past any last_migration_ts of 0 */
	ctx.active_sample_rate = 1;
	ctx.migrations_this_scan = 0;
	ctx.pinned = false;
	ctx.member_bitmap = g_members;
	ctx.cooldown_shift = 0;
	return ctx;
}

/* ============================================================
 * U-migrate — the all-gates-pass base scenario proposes MIGRATE to node 1.
 * ============================================================ */
UT_TEST(test_drm_decision_migrate_base)
{
	ClusterDrmDecisionCtx ctx = base_ctx();
	ClusterDrmVerdict v;

	build_slot(10 /*master*/, 100 /*dominant*/, 5 /*consecutive*/, 0 /*never migrated*/);
	v = cluster_drm_evaluate_shard(&g_slot, &ctx);

	UT_ASSERT(v.migrate);
	UT_ASSERT_EQ(v.reason, DRM_REASON_MIGRATE);
	UT_ASSERT_EQ(v.target_node, 1);
}

/* ============================================================
 * U-reason-matrix — one perturbation per gate (checked in gate order).
 * ============================================================ */
UT_TEST(test_drm_decision_skip_below_min_access)
{
	ClusterDrmDecisionCtx ctx = base_ctx();
	ClusterDrmVerdict v;

	build_slot(10, 100, 5, 0);
	cluster_drm_min_access_count = 200; /* 110 total < 200 */
	v = cluster_drm_evaluate_shard(&g_slot, &ctx);
	UT_ASSERT(!v.migrate);
	UT_ASSERT_EQ(v.reason, DRM_SKIP_BELOW_MIN_ACCESS);
	cluster_drm_min_access_count = 50;
}

UT_TEST(test_drm_decision_skip_ratio_low)
{
	ClusterDrmDecisionCtx ctx = base_ctx();
	ClusterDrmVerdict v;

	build_slot(50, 60, 5, 0); /* dominant 60/110 = 54% < 70% */
	v = cluster_drm_evaluate_shard(&g_slot, &ctx);
	UT_ASSERT(!v.migrate);
	UT_ASSERT_EQ(v.reason, DRM_SKIP_RATIO_LOW);
}

UT_TEST(test_drm_decision_skip_not_sustained)
{
	ClusterDrmDecisionCtx ctx = base_ctx();
	ClusterDrmVerdict v;

	build_slot(10, 100, 2, 0); /* consecutive 2 < triggers 3 */
	v = cluster_drm_evaluate_shard(&g_slot, &ctx);
	UT_ASSERT(!v.migrate);
	UT_ASSERT_EQ(v.reason, DRM_SKIP_NOT_SUSTAINED);
}

UT_TEST(test_drm_decision_skip_already_master)
{
	ClusterDrmDecisionCtx ctx = base_ctx();
	ClusterDrmVerdict v;

	build_slot(10, 100, 5, 0);
	ctx.current_master = 1; /* dominant is already master */
	v = cluster_drm_evaluate_shard(&g_slot, &ctx);
	UT_ASSERT(!v.migrate);
	UT_ASSERT_EQ(v.reason, DRM_SKIP_ALREADY_MASTER);
}

UT_TEST(test_drm_decision_skip_target_not_member)
{
	ClusterDrmDecisionCtx ctx = base_ctx();
	ClusterDrmVerdict v;

	build_slot(10, 100, 5, 0);
	g_members[0] = 1; /* only node 0 is a member; dominant node 1 is not */
	v = cluster_drm_evaluate_shard(&g_slot, &ctx);
	UT_ASSERT(!v.migrate);
	UT_ASSERT_EQ(v.reason, DRM_SKIP_TARGET_NOT_MEMBER);
}

UT_TEST(test_drm_decision_skip_pinned)
{
	ClusterDrmDecisionCtx ctx = base_ctx();
	ClusterDrmVerdict v;

	build_slot(10, 100, 5, 0);
	ctx.pinned = true;
	v = cluster_drm_evaluate_shard(&g_slot, &ctx);
	UT_ASSERT(!v.migrate);
	UT_ASSERT_EQ(v.reason, DRM_SKIP_PINNED);
}

UT_TEST(test_drm_decision_skip_cooldown)
{
	ClusterDrmDecisionCtx ctx = base_ctx();
	ClusterDrmVerdict v;

	/* migrated 1 second ago; cooldown is 600 s → still cooling. */
	build_slot(10, 100, 5, ctx.now_us - 1000000ULL);
	v = cluster_drm_evaluate_shard(&g_slot, &ctx);
	UT_ASSERT(!v.migrate);
	UT_ASSERT_EQ(v.reason, DRM_SKIP_COOLDOWN);
}

UT_TEST(test_drm_decision_skip_rate_limit)
{
	ClusterDrmDecisionCtx ctx = base_ctx();
	ClusterDrmVerdict v;

	build_slot(10, 100, 5, 0);
	ctx.migrations_this_scan = 2; /* == max_migrations_per_scan */
	v = cluster_drm_evaluate_shard(&g_slot, &ctx);
	UT_ASSERT(!v.migrate);
	UT_ASSERT_EQ(v.reason, DRM_SKIP_RATE_LIMIT);
}

UT_TEST(test_drm_decision_skip_no_net_benefit)
{
	ClusterDrmDecisionCtx ctx = base_ctx();
	ClusterDrmVerdict v;

	build_slot(10, 100, 5, 0);
	cluster_drm_migration_cost = 1000000; /* benefit 90*5=450 << cost */
	v = cluster_drm_evaluate_shard(&g_slot, &ctx);
	UT_ASSERT(!v.migrate);
	UT_ASSERT_EQ(v.reason, DRM_SKIP_NO_NET_BENEFIT);
	cluster_drm_migration_cost = 100;
}

/* ============================================================
 * U-benefit — binary-cost benefit = access[target] - access[current_master];
 *	migrate iff benefit * residence_windows > migration_cost (Amend v1.1-c).
 * ============================================================ */
UT_TEST(test_drm_decision_benefit_residence)
{
	ClusterDrmDecisionCtx ctx = base_ctx();
	ClusterDrmVerdict v;

	/* benefit = 100-10 = 90.  residence_windows = min(cooldown/window, consecutive)
	 * = min(60, consecutive).  Lower triggers to 1 so consecutive isolates the
	 * benefit gate.  consecutive=1 → total 90*1=90 <= cost(100) → NO benefit;
	 * consecutive=5 → 90*5=450 > 100 → migrate. */
	cluster_drm_consecutive_triggers = 1;

	build_slot(10, 100, 1, 0);
	v = cluster_drm_evaluate_shard(&g_slot, &ctx);
	UT_ASSERT(!v.migrate);
	UT_ASSERT_EQ(v.reason, DRM_SKIP_NO_NET_BENEFIT);

	build_slot(10, 100, 5, 0);
	v = cluster_drm_evaluate_shard(&g_slot, &ctx);
	UT_ASSERT(v.migrate);

	cluster_drm_consecutive_triggers = 3;
}

/* ============================================================
 * U-cooldown-backoff (Amend v1.1-d) — the adaptive cooldown_shift extends the
 *	base cooldown window by 2^shift; a migration that has cleared the base
 *	cooldown is still suppressed once the shard is on a backoff shift.
 * ============================================================ */
UT_TEST(test_drm_decision_cooldown_adaptive_backoff)
{
	ClusterDrmDecisionCtx ctx = base_ctx();
	ClusterDrmVerdict v;

	/* Base cooldown = 600000 ms = 600 s.  Migrated 100 s ago → still cooling. */
	build_slot(10, 100, 5, ctx.now_us - 100000000ULL);
	ctx.cooldown_shift = 0;
	v = cluster_drm_evaluate_shard(&g_slot, &ctx);
	UT_ASSERT(!v.migrate);
	UT_ASSERT_EQ(v.reason, DRM_SKIP_COOLDOWN);

	/* Migrated 700 s ago (> 600 s base) with shift 0 → cooldown cleared → migrate. */
	build_slot(10, 100, 5, ctx.now_us - 700000000ULL);
	ctx.cooldown_shift = 0;
	v = cluster_drm_evaluate_shard(&g_slot, &ctx);
	UT_ASSERT(v.migrate);

	/* Same 700 s ago, but shift 1 doubles the window to 1200 s → still cooling. */
	ctx.cooldown_shift = 1;
	v = cluster_drm_evaluate_shard(&g_slot, &ctx);
	UT_ASSERT(!v.migrate);
	UT_ASSERT_EQ(v.reason, DRM_SKIP_COOLDOWN);
}

/* ============================================================
 * U-purity (L409 / INV-DRM9) — evaluate mutates nothing.
 * ============================================================ */
UT_TEST(test_drm_decision_pure_no_mutation)
{
	ClusterDrmDecisionCtx ctx = base_ctx();
	uint32 a0_before, a1_before;
	uint32 consec_before;

	build_slot(10, 100, 5, 0);
	a0_before = pg_atomic_read_u32(&g_slot.access_count[0]);
	a1_before = pg_atomic_read_u32(&g_slot.access_count[1]);
	consec_before = g_slot.consecutive_hot_windows;

	(void)cluster_drm_evaluate_shard(&g_slot, &ctx);

	UT_ASSERT_EQ(pg_atomic_read_u32(&g_slot.access_count[0]), a0_before);
	UT_ASSERT_EQ(pg_atomic_read_u32(&g_slot.access_count[1]), a1_before);
	UT_ASSERT_EQ(g_slot.consecutive_hot_windows, consec_before);
}

/* ============================================================
 * U-pin-hook + reason-name — feature-083 hook is v1-false; names non-NULL.
 * ============================================================ */
UT_TEST(test_drm_decision_pin_hook_and_names)
{
	int r;

	UT_ASSERT(!cluster_drm_is_shard_pinned(0));
	UT_ASSERT(!cluster_drm_is_shard_pinned(4095));
	for (r = 0; r < DRM_REASON__COUNT; r++)
		UT_ASSERT_NOT_NULL((void *)cluster_drm_reason_name(r));
}

/* ============================================================ */

UT_DEFINE_GLOBALS();

int
main(int argc pg_attribute_unused(), char **const argv pg_attribute_unused())
{
	UT_PLAN(14);
	UT_RUN(test_drm_decision_migrate_base);
	UT_RUN(test_drm_decision_skip_below_min_access);
	UT_RUN(test_drm_decision_skip_ratio_low);
	UT_RUN(test_drm_decision_skip_not_sustained);
	UT_RUN(test_drm_decision_skip_already_master);
	UT_RUN(test_drm_decision_skip_target_not_member);
	UT_RUN(test_drm_decision_skip_pinned);
	UT_RUN(test_drm_decision_skip_cooldown);
	UT_RUN(test_drm_decision_skip_rate_limit);
	UT_RUN(test_drm_decision_skip_no_net_benefit);
	UT_RUN(test_drm_decision_benefit_residence);
	UT_RUN(test_drm_decision_cooldown_adaptive_backoff);
	UT_RUN(test_drm_decision_pure_no_mutation);
	UT_RUN(test_drm_decision_pin_hook_and_names);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
