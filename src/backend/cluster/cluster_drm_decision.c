/*-------------------------------------------------------------------------
 *
 * cluster_drm_decision.c
 *	  pgrac DRM hotness decision engine (spec-7.6 wave 6.3c).
 *
 *	  cluster_drm_evaluate_shard is a PURE predicate (INV-DRM9 / L409): given a
 *	  shard's per-node affinity slot and a caller-supplied context, it proposes
 *	  whether the shard's master should migrate and to which node.  It reads
 *	  nothing time- or topology-dependent itself (the caller passes now_us, the
 *	  live-member bitmap, the current master and the anti-thrash counters), so it
 *	  never mutates lock/master/GRD state and is directly unit-testable.
 *
 *	  Net-benefit model (Amend v1.1-c): under a binary hop cost (local grant = 0,
 *	  remote GES round-trip = 1) the frozen
 *	    benefit_rate(target) = Σ access[n] × (cost(n→current) − cost(n→target))
 *	  collapses to  access[target] − access[current_master].  Migrate iff
 *	    benefit_rate × expected_residence_windows > cluster.drm_migration_cost,
 *	  where expected_residence_windows = min(cooldown/window, consecutive_hot).
 *	  Per-node-pair cost differentiation is a forward optimization; the ratio
 *	  gate (cluster.drm_affinity_ratio_pct) is retained as a confidence shell.
 *
 *	  Gates run in a fixed order; the first that fires names the skip reason.
 *
 *	  See spec-7.6-drm-hot-resource-detection-remaster.md (§2.2/§3.2, Amend
 *	  v1.1-b/c/d).
 *
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/backend/cluster/cluster_drm_decision.c
 *
 * NOTES
 *	  pgrac-original file.  All exported symbols use the cluster_drm_ prefix.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "cluster/cluster_conf.h" /* CLUSTER_MAX_NODES */
#include "cluster/cluster_drm_decision.h"
#include "cluster/cluster_guc.h"

/* Adaptive-cooldown backoff is capped so the shift never overflows. */
#define DRM_COOLDOWN_MAX_SHIFT 6 /* up to 64x the base cooldown */

static bool
drm_bitmap_test(const uint8 *bitmap, int32 node)
{
	if (bitmap == NULL || node < 0 || node >= CLUSTER_MAX_NODES)
		return false;
	return (bitmap[node >> 3] & (1 << (node & 7))) != 0;
}

ClusterDrmVerdict
cluster_drm_evaluate_shard(const ClusterDrmShardAffinity *a, const ClusterDrmDecisionCtx *ctx)
{
	ClusterDrmVerdict v = { false, -1, DRM_SKIP_BELOW_MIN_ACCESS };
	uint64 total = 0;
	int32 dom = -1;
	uint32 dom_access = 0;
	uint32 master_access = 0;
	int rate;
	uint64 norm_total;
	int k;

	if (a == NULL || ctx == NULL)
		return v;

	/* Dominant node + total (both are pure reads). */
	for (k = 0; k < CLUSTER_MAX_NODES; k++) {
		uint32 c = pg_atomic_read_u32(unconstify(pg_atomic_uint32 *, &a->access_count[k]));

		total += c;
		if (c > dom_access) {
			dom_access = c;
			dom = k;
		}
	}
	if (ctx->current_master >= 0 && ctx->current_master < CLUSTER_MAX_NODES)
		master_access = pg_atomic_read_u32(
			unconstify(pg_atomic_uint32 *, &a->access_count[ctx->current_master]));

	v.target_node = dom;

	rate = (ctx->active_sample_rate < 1) ? 1 : ctx->active_sample_rate;
	norm_total = total * (uint64)rate;

	/* Gate 1 — absolute (sample-normalized) access floor. */
	if (norm_total < (uint64)cluster_drm_min_access_count) {
		v.reason = DRM_SKIP_BELOW_MIN_ACCESS;
		return v;
	}

	/* Gate 2 — dominant-node ratio confidence shell. */
	if (dom < 0 || total == 0
		|| (uint64)dom_access * 100 < total * (uint64)cluster_drm_affinity_ratio_pct) {
		v.reason = DRM_SKIP_RATIO_LOW;
		return v;
	}

	/* Gate 3 — sustained hotness (tumbling windows, Amend v1.1-b). */
	if (a->consecutive_hot_windows < (uint32)cluster_drm_consecutive_triggers) {
		v.reason = DRM_SKIP_NOT_SUSTAINED;
		return v;
	}

	/* Gate 4 — dominant already masters the shard: nothing to gain. */
	if (dom == ctx->current_master) {
		v.reason = DRM_SKIP_ALREADY_MASTER;
		return v;
	}

	/* Gate 5 — INV-DRM1: only migrate to a live published MEMBER. */
	if (!drm_bitmap_test(ctx->member_bitmap, dom)) {
		v.reason = DRM_SKIP_TARGET_NOT_MEMBER;
		return v;
	}

	/* Gate 6 — DBA affinity pin (feature-083 forward; v1 false). */
	if (ctx->pinned) {
		v.reason = DRM_SKIP_PINNED;
		return v;
	}

	/* Gate 7 — anti-thrash cooldown (adaptive backoff, Amend v1.1-d). */
	if (a->last_migration_ts != 0) {
		uint32 shift = (ctx->cooldown_shift > DRM_COOLDOWN_MAX_SHIFT) ? DRM_COOLDOWN_MAX_SHIFT
																	  : ctx->cooldown_shift;
		uint64 cooldown_us = (uint64)cluster_drm_cooldown_ms * 1000 * ((uint64)1 << shift);

		if (ctx->now_us > a->last_migration_ts
			&& (ctx->now_us - a->last_migration_ts) < cooldown_us) {
			v.reason = DRM_SKIP_COOLDOWN;
			return v;
		}
	}

	/* Gate 8 — per-scan migration rate limit. */
	if (ctx->migrations_this_scan >= cluster_drm_max_migrations_per_scan) {
		v.reason = DRM_SKIP_RATE_LIMIT;
		return v;
	}

	/* Gate 9 — net benefit (Amend v1.1-c).  Binary-cost benefit rate credited
	 * over the expected residence, capped by the cooldown horizon. */
	{
		int64 benefit_rate = (int64)dom_access - (int64)master_access;
		int window_ms = (cluster_drm_affinity_window_ms < 1) ? 1 : cluster_drm_affinity_window_ms;
		uint64 cooldown_windows = (uint64)cluster_drm_cooldown_ms / (uint64)window_ms;
		uint64 residence_windows = a->consecutive_hot_windows;
		uint64 total_benefit;

		if (residence_windows > cooldown_windows)
			residence_windows = cooldown_windows;

		total_benefit = (benefit_rate > 0) ? (uint64)benefit_rate * residence_windows : 0;

		if (benefit_rate <= 0 || total_benefit <= (uint64)cluster_drm_migration_cost) {
			v.reason = DRM_SKIP_NO_NET_BENEFIT;
			return v;
		}
	}

	/* All gates passed — propose migration to the dominant node. */
	v.migrate = true;
	v.reason = DRM_REASON_MIGRATE;
	return v;
}

bool
cluster_drm_is_shard_pinned(uint32 shard_id pg_attribute_unused())
{
	/* feature-083 DBA instance affinity is not yet shipped; DRM never skips for
	 * a pin in v1.  The gate exists so the pin implementation is a one-line
	 * change here + a real predicate. */
	return false;
}

const char *
cluster_drm_reason_name(int reason)
{
	static const char *const names[DRM_REASON__COUNT] = {
		"migrate",			 "below_min_access", "ratio_low", "not_sustained", "already_master",
		"target_not_member", "pinned",			 "cooldown",  "rate_limit",	   "no_net_benefit",
	};

	if (reason < 0 || reason >= DRM_REASON__COUNT)
		return "unknown";
	return names[reason];
}
