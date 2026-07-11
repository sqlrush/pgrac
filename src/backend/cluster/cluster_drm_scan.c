/*-------------------------------------------------------------------------
 *
 * cluster_drm_scan.c
 *	  pgrac DRM decision scan driver (spec-7.6 wave 6.3c).
 *
 *	  cluster_drm_lmon_scan_tick() runs on the LMON aux process's main-loop tick.
 *	  It is the driver that bridges the three DRM halves:
 *	    - collection substrate (cluster_drm_affinity.c, 6.3b): per-shard x per-node
 *	      access matrix + tumbling-window state (this driver rolls the windows);
 *	    - decision predicate (cluster_drm_decision.c, pure, INV-DRM9): proposes a
 *	      migration + names the skip reason;
 *	    - live topology (cluster_grd.c current master + per-shard master
 *	      generation, cluster_epoch.c, cluster_membership.c live-member set).
 *
 *	  Keeping the driver in its own file lets the collection + decision modules
 *	  stay dependency-clean (no GRD/epoch linkage there) so their standalone
 *	  cluster_unit tests do not need extra stubs.
 *
 *	  Flow per scan (self-gated to cluster.drm_scan_interval_ms):
 *	    1. reconcile the sample rate (LMON single writer, Amend v1.1.2 R4.2);
 *	    2. snapshot the live-member bitmap once (INV-DRM1 target check);
 *	    3. collect candidate shards (above the normalized access floor);
 *	    4. for each candidate: open its first window, or — once the window is due —
 *	       judge the completed window (updating consecutive_hot_windows), run the
 *	       decision predicate on the completed-window counts, count the verdict,
 *	       then reset the window.
 *
 *	  Wave 6.3c PROPOSES only.  Nothing here mutates lock/master/GRD state; the
 *	  live single-shard remaster executor is wave 6.3d.
 *
 *	  See spec-7.6-drm-hot-resource-detection-remaster.md (§3.2, Amend v1.1-b/c/d).
 *
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/backend/cluster/cluster_drm_scan.c
 *
 * NOTES
 *	  pgrac-original file.  All exported symbols use the cluster_drm_ prefix.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <string.h>

#include "utils/timestamp.h" /* GetCurrentTimestamp, TimestampTz */

#include "cluster/cluster_drm_affinity.h"
#include "cluster/cluster_drm_decision.h"
#include "cluster/cluster_drm_scan.h"
#include "cluster/cluster_epoch.h"
#include "cluster/cluster_grd.h"
#include "cluster/cluster_guc.h"
#include "cluster/cluster_membership.h"

/*
 * The shared per-reason counter array (cluster_drm_affinity.h) is sized without
 * referencing ClusterDrmReason to avoid an include cycle; assert here — where
 * both headers are visible — that it is wide enough.
 */
StaticAssertDecl(DRM_REASON__COUNT <= CLUSTER_DRM_SCAN_REASON_SLOTS,
				 "drm scan reason-counter array too small for ClusterDrmReason");

/*
 * Per-scan candidate cap.  collect_candidates returns only shards above the
 * normalized access floor, so the working set is small in practice; this bounds
 * the stack array and one scan's work.  If it is hit, the remaining hot shards
 * stay dirty + hot and are picked up on later scans (logged once — no silent cap).
 */
#define DRM_SCAN_MAX_CANDIDATES 1024

void
cluster_drm_lmon_scan_tick(void)
{
	static TimestampTz last_scan_at = 0;
	TimestampTz now;
	uint64 now_us;
	uint64 cluster_epoch;
	uint64 window_us;
	uint8 members[(CLUSTER_MAX_NODES + 7) / 8];
	uint32 candidates[DRM_SCAN_MAX_CANDIDATES];
	int ncand;
	int migrations_this_scan = 0;
	int rate;
	int i;

	if (!cluster_drm_enabled)
		return;

	/* Self-gate to the configured scan interval (both LMON drain sites call us). */
	now = GetCurrentTimestamp();
	if (last_scan_at != 0 && now - last_scan_at < (TimestampTz)cluster_drm_scan_interval_ms * 1000)
		return;
	last_scan_at = now;

	/*
	 * LMON is the single writer of the sample-rate reconcile (Amend v1.1.2 R4.2):
	 * a rate change bumps sample_epoch + clears the dirty bitmap so the scan does
	 * not consume stale windows.
	 */
	(void)cluster_drm_affinity_reconcile_sample_rate();
	cluster_drm_affinity_record_scan_run();

	now_us = (uint64)now;
	cluster_epoch = cluster_epoch_get_current();
	rate = (cluster_drm_affinity_sample_rate < 1) ? 1 : cluster_drm_affinity_sample_rate;
	window_us = (uint64)((cluster_drm_affinity_window_ms < 1) ? 1 : cluster_drm_affinity_window_ms)
				* UINT64CONST(1000);

	/* Snapshot the live-member set once per scan (INV-DRM1 dominant-is-member gate). */
	memset(members, 0, sizeof(members));
	for (i = 0; i < CLUSTER_MAX_NODES; i++)
		if (cluster_membership_is_member(i))
			members[i >> 3] |= (uint8)(1 << (i & 7));

	ncand = cluster_drm_affinity_collect_candidates(candidates, DRM_SCAN_MAX_CANDIDATES);
	if (ncand >= DRM_SCAN_MAX_CANDIDATES) {
		static bool warned = false;

		if (!warned) {
			ereport(LOG,
					(errmsg("cluster drm: decision scan candidate set hit the %d cap; remaining "
							"hot shards are deferred to later scans",
							DRM_SCAN_MAX_CANDIDATES)));
			warned = true;
		}
	}

	for (i = 0; i < ncand; i++) {
		uint32 shard = candidates[i];
		ClusterDrmShardAffinity *slot = cluster_drm_affinity_slot(shard);
		int32 master;
		uint32 mgen;
		ClusterDrmDecisionCtx ctx;
		ClusterDrmVerdict v;
		bool proposed;

		if (slot == NULL)
			continue;

		master = cluster_grd_shard_master(shard);
		mgen = cluster_grd_shard_master_generation(shard);

		/* First observation of this shard: open the window, decide next boundary. */
		if (!cluster_drm_window_is_open(slot)) {
			cluster_drm_window_open(slot, now_us, cluster_epoch, mgen);
			continue;
		}
		/* Tumbling windows are judged only once, at their boundary. */
		if (!cluster_drm_window_due(slot, now_us, window_us))
			continue;

		/*
		 * Judge the completed window (updates consecutive_hot_windows), THEN run
		 * the decision on the completed-window counts + the fresh streak, THEN
		 * reset the counts for the next window.
		 */
		(void)cluster_drm_window_judge(slot, now_us, window_us, rate, cluster_drm_min_access_count,
									   cluster_drm_affinity_ratio_pct, cluster_epoch, mgen);

		ctx.current_master = master;
		ctx.now_us = now_us;
		ctx.active_sample_rate = rate;
		ctx.migrations_this_scan = migrations_this_scan;
		ctx.pinned = cluster_drm_is_shard_pinned(shard);
		ctx.member_bitmap = members;
		ctx.cooldown_shift = 0; /* per-shard backoff is migration-driven (wave 6.3d) */

		v = cluster_drm_evaluate_shard(slot, &ctx);

		/*
		 * Wave 6.3c PROPOSES only — nothing is executed here.  drm_manual_only
		 * still tallies the reason (observability) but suppresses the auto-
		 * actionable proposal count + rate-limit consumption.
		 */
		proposed = v.migrate && !cluster_drm_manual_only;
		cluster_drm_affinity_record_verdict(v.reason, proposed);
		if (proposed)
			migrations_this_scan++;

		cluster_drm_window_reset(slot, now_us, cluster_epoch, mgen);
	}
}
