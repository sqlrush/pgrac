/*-------------------------------------------------------------------------
 *
 * cluster_drm_affinity.h
 *	  Public interface for pgrac DRM per-shard access-affinity collection.
 *
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/include/cluster/cluster_drm_affinity.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef CLUSTER_DRM_AFFINITY_H
#define CLUSTER_DRM_AFFINITY_H

#include "c.h"
#include "port/atomics.h"
#include "cluster/cluster_conf.h" /* CLUSTER_MAX_NODES */
#include "cluster/cluster_grd.h"  /* PGRAC_GRD_SHARD_COUNT */

/*
 * Per-shard access-affinity slot (shmem).  Populated ONLY on the node that
 * currently masters the shard (spec-7.6 Amend v1.1.2 R2, current-master
 * authoritative collection): remote requests land here via env->source_node_id
 * and local requests via cluster_node_id, so the master accumulates the full
 * per-node access matrix.  A non-master node's slot for a shard stays cold —
 * it never handles that shard's first-logical grants.
 *
 * INV-DRM9: pure statistics.  Nothing in this module mutates master/lock/GRD
 * state; it only touches its own shmem region.
 *
 * The window_* identity tuple (spec-7.6 Amend v1.1.2 R5) lets a scan discard a
 * stale window when {cluster_epoch, master_generation, sample_epoch} changes,
 * so a shard remastered away and back does not consume stale heat.  The tumbling
 * window ROLL logic itself lands in wave 6.3c (D3); wave 6.3b only records the
 * matrix + identity and answers candidate queries.
 */
typedef struct ClusterDrmShardAffinity {
	pg_atomic_uint32 access_count[CLUSTER_MAX_NODES]; /* sampled first-logical req per node */
	pg_atomic_uint32 ewma_total;					  /* time-decayed total (D3 consumes)    */
	pg_atomic_uint64 window_start_ts;				  /* tumbling window anchor us (D3)       */
	pg_atomic_uint64 window_cluster_epoch;			  /* window identity (R5)                 */
	pg_atomic_uint32 window_master_generation;		  /* window identity (R5)                 */
	pg_atomic_uint64 window_sample_epoch;			  /* window identity (R5)                 */
	uint32 consecutive_hot_windows;					  /* anti-thrash — LMON-only writer (D4) */
	uint64 last_migration_ts;						  /* cooldown anchor — LMON-only (D4)    */
} ClusterDrmShardAffinity;

/*
 * Shmem region layout.  Full 4096-slot array is always resident when the
 * cluster is enabled (spec-7.6 Amend v1.1-a④) — no candidate admission, so
 * there is no chicken-and-egg about where to count a not-yet-tracked shard.
 * dirty_shard_bitmap is atomic (Amend v1.1.2 R4.1): backends set bits with
 * fetch_or on flush, the LMON scan drains them with atomic exchange.
 */
typedef struct ClusterDrmAffinityShared {
	pg_atomic_uint64 sample_epoch; /* LMON single-writer; bumped on sample_rate change (R4.2) */
	pg_atomic_uint64
		active_sample_rate; /* last rate the LMON reconciled (R4.2)                   */
	/* observability counters (dump category "drm_affinity") */
	pg_atomic_uint64 samples_recorded;
	pg_atomic_uint64 samples_local;	 /* recorded, requesting_node == self  */
	pg_atomic_uint64 samples_remote; /* recorded, requesting_node == a peer */
	pg_atomic_uint64 samples_skipped_off;
	pg_atomic_uint64 samples_dropped_stale_epoch;
	pg_atomic_uint64 flush_batches;
	pg_atomic_uint64 dirty_shard_bitmap[(PGRAC_GRD_SHARD_COUNT + 63) / 64];
	ClusterDrmShardAffinity shard[PGRAC_GRD_SHARD_COUNT];
} ClusterDrmAffinityShared;

/* --- shmem lifecycle (wired via cluster_shmem.c registry) --- */
extern Size cluster_drm_affinity_shmem_size(void);
extern void cluster_drm_affinity_shmem_init(void);
extern void cluster_drm_affinity_shmem_register(void);

/*
 * Admission-side sample hook (spec-7.6 D2).  Called at the first-logical-request
 * boundary ON THE CURRENT MASTER — remote path from the GES dedup MISS branch,
 * local path from the local-master grant branch (both in cluster_ges.c).  NOT
 * called from cluster_grd_lookup_master (Amend v1.1.2 R3): that primitive is
 * also driven by probe / remaster housekeeping / release and would double-count
 * and pollute.  requesting_node = env->source_node_id (remote) or cluster_node_id
 * (local).  Cheap: the caller guards on cluster_drm_enabled first, then a
 * per-backend countdown decides whether this hit is sampled (no shared atomic,
 * no modulo on the hot path — Amend v1.1-a③).
 *
 * was_remote is observability ONLY (splits samples_local / samples_remote):
 * BOTH local and remote hits are recorded into the matrix — the flag never
 * filters (Amend v1.1-a①; recording only remote would blind the master to its
 * own local heat and mis-migrate the shard it accesses most).
 */
extern void cluster_drm_affinity_sample(uint32 shard_id, int32 requesting_node, bool was_remote);

/*
 * Flush the per-backend sample ring into the shared matrix (spec-7.6 Amend
 * v1.1.2 R4.3, explicit lifecycle).  Called on ring-full, transaction end,
 * backend exit, and the LMON drain.  Ring entries carrying a sample_epoch
 * other than the current shared sample_epoch are dropped as a stale batch.
 */
extern void cluster_drm_affinity_flush_local_ring(void);

/* Register the per-backend flush hooks (xact-end + before_shmem_exit).  Idempotent. */
extern void cluster_drm_affinity_register_backend_hooks(void);

/*
 * Drop this backend's un-flushed buffered samples and re-arm its sampling
 * cadence from scratch.  The per-backend countdown is calibrated for the sample
 * rate in effect when it was last reseeded, so after a large rate change it can
 * carry a stale budget; this re-syncs it.  Also used by unit tests for
 * deterministic per-test sampling.
 */
extern void cluster_drm_affinity_reset_local_sampling(void);

/*
 * LMON single-writer reconciliation (Amend v1.1.2 R4.2): if cluster.drm_
 * affinity_sample_rate changed since the last reconcile, reset counts + dirty,
 * bump sample_epoch (so in-flight backend batches at the old rate are dropped
 * on flush) and reopen the window.  Returns true when a reopen happened.
 */
extern bool cluster_drm_affinity_reconcile_sample_rate(void);

/*
 * Collect the shard ids that currently cross the sample-rate-normalized
 * min_access_count on THIS (master) node (Amend v1.1-a④/a⑤).  The candidate
 * table is just this returned index list — a decision working set, not a copy.
 * Returns the number written to out_shard_ids (<= max).  Pure read (INV-DRM9).
 */
extern int cluster_drm_affinity_collect_candidates(uint32 *out_shard_ids, int max);

/* --- read accessors for the dump category + unit tests (pure reads) --- */
extern uint32 cluster_drm_affinity_access_count(uint32 shard_id, int32 node);
extern uint64 cluster_drm_affinity_get_sample_epoch(void);
extern uint64 cluster_drm_affinity_get_counter(int which);

/* cluster_drm_affinity_get_counter() selectors. */
#define CLUSTER_DRM_AFFINITY_CTR_RECORDED 0
#define CLUSTER_DRM_AFFINITY_CTR_SKIPPED_OFF 1
#define CLUSTER_DRM_AFFINITY_CTR_DROPPED_STALE 2
#define CLUSTER_DRM_AFFINITY_CTR_FLUSH_BATCHES 3
#define CLUSTER_DRM_AFFINITY_CTR_LOCAL 4
#define CLUSTER_DRM_AFFINITY_CTR_REMOTE 5

#endif /* CLUSTER_DRM_AFFINITY_H */
