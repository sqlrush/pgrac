/*-------------------------------------------------------------------------
 *
 * cluster_drm_decision.h
 *	  Public interface for the pgrac DRM hotness decision engine.
 *
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/include/cluster/cluster_drm_decision.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef CLUSTER_DRM_DECISION_H
#define CLUSTER_DRM_DECISION_H

#include "c.h"
#include "cluster/cluster_drm_affinity.h" /* ClusterDrmShardAffinity */

/*
 * ClusterDrmReason — why a shard was (or was not) proposed for remaster.
 * Every skip path is named so it can be counted and observed (L13).  The
 * gates are evaluated in this order; the first that fires wins.
 */
typedef enum ClusterDrmReason {
	DRM_REASON_MIGRATE = 0,		/* proposed for migration to target_node   */
	DRM_SKIP_BELOW_MIN_ACCESS,	/* sample-normalized total < min_access    */
	DRM_SKIP_RATIO_LOW,			/* dominant/total < drm_affinity_ratio_pct */
	DRM_SKIP_NOT_SUSTAINED,		/* consecutive_hot_windows < triggers      */
	DRM_SKIP_ALREADY_MASTER,	/* dominant node already masters the shard */
	DRM_SKIP_TARGET_NOT_MEMBER, /* INV-DRM1: dominant is not a live MEMBER */
	DRM_SKIP_PINNED,			/* feature-083 DBA affinity pin (v1 false) */
	DRM_SKIP_COOLDOWN,			/* within the (adaptive) cooldown window   */
	DRM_SKIP_RATE_LIMIT,		/* drm_max_migrations_per_scan reached     */
	DRM_SKIP_NO_NET_BENEFIT,	/* benefit*residence <= drm_migration_cost */
	DRM_REASON__COUNT
} ClusterDrmReason;

typedef struct ClusterDrmVerdict {
	bool migrate;
	int32 target_node; /* dominant accessing node, -1 if none */
	int32 reason;	   /* ClusterDrmReason */
} ClusterDrmVerdict;

/*
 * Inputs the caller (LMON scan) supplies so cluster_drm_evaluate_shard stays a
 * pure predicate (L409): no clock reads, no membership calls, no shmem writes
 * inside — everything time/topology-dependent arrives here, which also makes
 * the predicate directly unit-testable.
 */
typedef struct ClusterDrmDecisionCtx {
	int32 current_master;		/* cluster_grd_shard_master(shard)        */
	uint64 now_us;				/* GetCurrentTimestamp() at scan time     */
	int active_sample_rate;		/* for min-access normalization           */
	int migrations_this_scan;	/* D4 rate-limit counter (caller keeps)  */
	bool pinned;				/* cluster_drm_is_shard_pinned(shard)     */
	const uint8 *member_bitmap; /* uint8[16] CLUSTER_MAX_NODES-bit live set */
	uint32 cooldown_shift;		/* D4 adaptive-cooldown backoff exponent  */
} ClusterDrmDecisionCtx;

/*
 * Pure predicate (Amend v1.1-c / INV-DRM9): proposes only, never mutates.  Net
 * benefit uses a binary hop cost (local=0 / remote=1), so under that cost the
 * frozen benefit_rate collapses to access[target]-access[current_master];
 * migrate iff benefit_rate * expected_residence_windows > drm_migration_cost.
 */
extern ClusterDrmVerdict cluster_drm_evaluate_shard(const ClusterDrmShardAffinity *a,
													const ClusterDrmDecisionCtx *ctx);

/* DBA affinity-pin guard hook (feature-083 forward; v1 always false). */
extern bool cluster_drm_is_shard_pinned(uint32 shard_id);

/* Observability: stable name for a ClusterDrmReason. */
extern const char *cluster_drm_reason_name(int reason);

#endif /* CLUSTER_DRM_DECISION_H */
