/*-------------------------------------------------------------------------
 *
 * cluster_drm_affinity.c
 *	  pgrac DRM per-shard access-affinity collection substrate.
 *
 *	  DRM (Dynamic Resource Mastering) migrates a hot shard's GES/PCM master
 *	  to the node that accesses it most.  This module is the collection half
 *	  (spec-7.6 wave 6.3b): it samples first-logical lock requests and builds,
 *	  ON THE CURRENT MASTER, a per-shard x per-node access matrix from which the
 *	  decision engine (wave 6.3c) picks migration candidates.
 *
 *	  Design (spec-7.6 Amend v1.1-a / v1.1.2):
 *	    - Current-master authoritative: only the node that masters a shard sees
 *	      every node's requests for it (remote via env->source_node_id, local via
 *	      cluster_node_id), so it alone can compute a dominant-node ratio.  The
 *	      admission-side call sites live in cluster_ges.c (D2); this module takes
 *	      shard_id + requesting_node directly and has NO GRD/GES linkage.
 *	    - INV-DRM9: pure statistics.  Nothing here mutates master/lock/GRD state.
 *	    - Per-backend sampling: a backend-local countdown + xorshift PRNG decides
 *	      1/N hits with no shared atomic or modulo on the hot path.  Sampled hits
 *	      buffer in a backend-local ring, batch-flushed into the shared matrix.
 *	    - Full 4096-slot array is always resident (no candidate admission); a
 *	      dirty-shard bitmap (atomic) bounds the LMON scan to touched shards.
 *	    - sample_epoch (LMON single-writer) is bumped when the sample rate
 *	      changes; ring entries carry the epoch they were taken at so a stale
 *	      batch is dropped, and a shard's window is discarded when its sampled
 *	      epoch no longer matches (Amend v1.1.2 R4/R5).
 *
 *	  See spec-7.6-drm-hot-resource-detection-remaster.md (§2.1/§3.1, Amend
 *	  v1.1-a / v1.1.2) for the frozen contract.
 *
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/backend/cluster/cluster_drm_affinity.c
 *
 * NOTES
 *	  pgrac-original file.  All exported symbols use the cluster_drm_ prefix.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/xact.h"
#include "miscadmin.h"
#include "storage/ipc.h"
#include "storage/shmem.h"

#include "cluster/cluster_drm_affinity.h"
#include "cluster/cluster_guc.h"
#include "cluster/cluster_shmem.h"

/* Backend-local sample ring capacity (batched flush into the shared matrix). */
#define DRM_LOCAL_RING 64

/* --- module shared state (bound in cluster_drm_affinity_shmem_init) --- */
static ClusterDrmAffinityShared *drm_state = NULL;

/* --- per-backend sampling state (no shared atomics on the decision) --- */
typedef struct DrmRingEntry {
	uint64 sample_epoch;
	uint32 shard;
	int32 node;
	bool was_remote;
} DrmRingEntry;

static DrmRingEntry drm_ring[DRM_LOCAL_RING];
static int drm_ring_len = 0;
static int drm_countdown = 0;
static uint32 drm_prng = 0;
static bool drm_prng_seeded = false;
static bool drm_backend_hooks_registered = false;

/* ----------
 * Shmem lifecycle
 * ----------
 */

Size
cluster_drm_affinity_shmem_size(void)
{
	return MAXALIGN(sizeof(ClusterDrmAffinityShared));
}

void
cluster_drm_affinity_shmem_init(void)
{
	bool found;

	drm_state = (ClusterDrmAffinityShared *)ShmemInitStruct(
		"pgrac cluster drm affinity", cluster_drm_affinity_shmem_size(), &found);

	if (!found) {
		int nwords = (PGRAC_GRD_SHARD_COUNT + 63) / 64;
		int initial_rate
			= (cluster_drm_affinity_sample_rate < 1) ? 1 : cluster_drm_affinity_sample_rate;
		int i;

		/*
		 * sample_epoch starts at 1 so a never-sampled slot (window_sample_epoch
		 * == 0) reads as stale and is skipped until its first real sample.
		 */
		pg_atomic_init_u64(&drm_state->sample_epoch, 1);
		pg_atomic_init_u64(&drm_state->active_sample_rate, (uint64)initial_rate);
		pg_atomic_init_u64(&drm_state->samples_recorded, 0);
		pg_atomic_init_u64(&drm_state->samples_local, 0);
		pg_atomic_init_u64(&drm_state->samples_remote, 0);
		pg_atomic_init_u64(&drm_state->samples_skipped_off, 0);
		pg_atomic_init_u64(&drm_state->samples_dropped_stale_epoch, 0);
		pg_atomic_init_u64(&drm_state->flush_batches, 0);

		for (i = 0; i < nwords; i++)
			pg_atomic_init_u64(&drm_state->dirty_shard_bitmap[i], 0);

		for (i = 0; i < PGRAC_GRD_SHARD_COUNT; i++) {
			ClusterDrmShardAffinity *slot = &drm_state->shard[i];
			int k;

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
	}
}

static const ClusterShmemRegion cluster_drm_affinity_region = {
	.name = "pgrac cluster drm affinity",
	.size_fn = cluster_drm_affinity_shmem_size,
	.init_fn = cluster_drm_affinity_shmem_init,
	.lwlock_count = 0, /* lock-free: atomic matrix + atomic dirty bitmap */
	.owner_subsys = "spec-7.6 DRM affinity",
	.reserved_flags = 0,
};

void
cluster_drm_affinity_shmem_register(void)
{
	cluster_shmem_register_region(&cluster_drm_affinity_region);
}

/* ----------
 * Per-backend flush lifecycle (Amend v1.1.2 R4.3)
 * ----------
 */

static void
drm_xact_callback(XactEvent event pg_attribute_unused(), void *arg pg_attribute_unused())
{
	/* Flush at every transaction boundary so buffered samples never linger
	 * across a rate change longer than one transaction. */
	cluster_drm_affinity_flush_local_ring();
}

static void
drm_backend_exit_callback(int code pg_attribute_unused(), Datum arg pg_attribute_unused())
{
	cluster_drm_affinity_flush_local_ring();
}

void
cluster_drm_affinity_register_backend_hooks(void)
{
	if (drm_backend_hooks_registered)
		return;
	if (!IsUnderPostmaster)
		return; /* real backends only */
	if (MyBackendId <= 0)
		return; /* not aux processes */

	RegisterXactCallback(drm_xact_callback, NULL);
	before_shmem_exit(drm_backend_exit_callback, (Datum)0);
	drm_backend_hooks_registered = true;
}

/* ----------
 * Sampling (admission hot path, spec-7.6 D2 calls this)
 * ----------
 */

void
cluster_drm_affinity_reset_local_sampling(void)
{
	drm_ring_len = 0;
	drm_countdown = 0;
	drm_prng_seeded = false;
}

/* xorshift32 — per-backend PRNG, never a shared/global RNG (Amend v1.1-a③). */
static inline uint32
drm_prng_next(void)
{
	drm_prng ^= drm_prng << 13;
	drm_prng ^= drm_prng >> 17;
	drm_prng ^= drm_prng << 5;
	return drm_prng;
}

void
cluster_drm_affinity_sample(uint32 shard_id, int32 requesting_node, bool was_remote)
{
	int rate;

	if (!cluster_drm_enabled) {
		if (drm_state != NULL)
			pg_atomic_fetch_add_u64(&drm_state->samples_skipped_off, 1);
		return;
	}
	if (drm_state == NULL)
		return;
	if (shard_id >= PGRAC_GRD_SHARD_COUNT || requesting_node < 0
		|| requesting_node >= CLUSTER_MAX_NODES)
		return;

	if (!drm_prng_seeded) {
		/* Seed per-backend so different backends sample independent hits. */
		drm_prng = ((uint32)MyBackendId * 2654435761u) ^ 0x9e3779b9u;
		if (drm_prng == 0)
			drm_prng = 0x1234567u;
		drm_prng_seeded = true;
		drm_countdown = 0; /* first eligible hit records */

		/* Lazily arm the flush hooks the first time this backend samples (no-op
		 * in aux processes, which flush on ring-full / the LMON drain instead). */
		cluster_drm_affinity_register_backend_hooks();
	}

	/* Cheapest common path: not a sample tick. */
	if (--drm_countdown > 0)
		return;

	/* Reseed the countdown to average ~rate hits between samples (1/N). */
	rate = cluster_drm_affinity_sample_rate;
	if (rate < 1)
		rate = 1;
	drm_countdown = 1 + (int)(drm_prng_next() % (uint32)(2 * rate - 1));

	if (drm_ring_len >= DRM_LOCAL_RING)
		cluster_drm_affinity_flush_local_ring();

	drm_ring[drm_ring_len].sample_epoch = pg_atomic_read_u64(&drm_state->sample_epoch);
	drm_ring[drm_ring_len].shard = shard_id;
	drm_ring[drm_ring_len].node = requesting_node;
	drm_ring[drm_ring_len].was_remote = was_remote;
	drm_ring_len++;
}

/* ----------
 * Flush the backend-local ring into the shared matrix
 * ----------
 */

static void
drm_apply_sample(uint64 cur_epoch, uint32 shard, int32 node, bool was_remote)
{
	ClusterDrmShardAffinity *slot = &drm_state->shard[shard];
	uint64 wse = pg_atomic_read_u64(&slot->window_sample_epoch);

	/*
	 * Window rolled (sample_epoch bumped): the first flusher to swap the slot's
	 * window identity to the current epoch clears its stale per-node counts.
	 * Lazy reset — reconcile does not sweep 2 MiB of counters (Amend v1.1.2 R4).
	 */
	if (wse != cur_epoch) {
		if (pg_atomic_compare_exchange_u64(&slot->window_sample_epoch, &wse, cur_epoch)) {
			int k;

			for (k = 0; k < CLUSTER_MAX_NODES; k++)
				pg_atomic_write_u32(&slot->access_count[k], 0);
		}
	}

	pg_atomic_fetch_add_u32(&slot->access_count[node], 1);
	pg_atomic_fetch_or_u64(&drm_state->dirty_shard_bitmap[shard / 64], UINT64CONST(1)
																		   << (shard % 64));
	pg_atomic_fetch_add_u64(&drm_state->samples_recorded, 1);
	/* was_remote splits the counters for observability only (Amend v1.1-a①). */
	if (was_remote)
		pg_atomic_fetch_add_u64(&drm_state->samples_remote, 1);
	else
		pg_atomic_fetch_add_u64(&drm_state->samples_local, 1);
}

void
cluster_drm_affinity_flush_local_ring(void)
{
	uint64 cur_epoch;
	int i;

	if (drm_state == NULL || drm_ring_len == 0)
		return;

	cur_epoch = pg_atomic_read_u64(&drm_state->sample_epoch);
	pg_atomic_fetch_add_u64(&drm_state->flush_batches, 1);

	for (i = 0; i < drm_ring_len; i++) {
		if (drm_ring[i].sample_epoch != cur_epoch) {
			/* Stale batch: the rate changed after this sample was taken. */
			pg_atomic_fetch_add_u64(&drm_state->samples_dropped_stale_epoch, 1);
			continue;
		}
		drm_apply_sample(cur_epoch, drm_ring[i].shard, drm_ring[i].node, drm_ring[i].was_remote);
	}

	drm_ring_len = 0;
}

/* ----------
 * LMON single-writer: reconcile the sample rate (Amend v1.1.2 R4.2)
 * ----------
 */

bool
cluster_drm_affinity_reconcile_sample_rate(void)
{
	int cur_rate;
	uint64 last_rate;
	int nwords;
	int w;

	if (drm_state == NULL)
		return false;

	cur_rate = cluster_drm_affinity_sample_rate;
	if (cur_rate < 1)
		cur_rate = 1;
	last_rate = pg_atomic_read_u64(&drm_state->active_sample_rate);

	if ((uint64)cur_rate == last_rate)
		return false;

	/*
	 * Rate changed.  Bump sample_epoch (in-flight backend batches at the old
	 * rate are dropped on flush) and clear the dirty bitmap so the scan does
	 * not revisit slots whose windows are now stale; per-shard counts are reset
	 * lazily on their next sample (drm_apply_sample).  Single LMON writer, so no
	 * lock is needed for the epoch bump / rate store.
	 */
	pg_atomic_fetch_add_u64(&drm_state->sample_epoch, 1);
	nwords = (PGRAC_GRD_SHARD_COUNT + 63) / 64;
	for (w = 0; w < nwords; w++)
		pg_atomic_write_u64(&drm_state->dirty_shard_bitmap[w], 0);
	pg_atomic_write_u64(&drm_state->active_sample_rate, (uint64)cur_rate);
	return true;
}

/* ----------
 * Candidate collection (pure read — INV-DRM9)
 * ----------
 */

int
cluster_drm_affinity_collect_candidates(uint32 *out_shard_ids, int max)
{
	uint64 cur_epoch;
	int rate;
	int min_access;
	int nwords;
	int n = 0;
	int w;

	if (drm_state == NULL || out_shard_ids == NULL || max <= 0)
		return 0;

	cur_epoch = pg_atomic_read_u64(&drm_state->sample_epoch);
	rate = cluster_drm_affinity_sample_rate;
	if (rate < 1)
		rate = 1;
	min_access = cluster_drm_min_access_count;
	nwords = (PGRAC_GRD_SHARD_COUNT + 63) / 64;

	for (w = 0; w < nwords && n < max; w++) {
		uint64 word = pg_atomic_read_u64(&drm_state->dirty_shard_bitmap[w]);
		int b;

		if (word == 0)
			continue;

		for (b = 0; b < 64 && n < max; b++) {
			uint32 shard;
			ClusterDrmShardAffinity *slot;
			uint64 total = 0;
			int k;

			if ((word & (UINT64CONST(1) << b)) == 0)
				continue;

			shard = (uint32)(w * 64 + b);
			if (shard >= PGRAC_GRD_SHARD_COUNT)
				continue;

			slot = &drm_state->shard[shard];
			/* Skip a shard whose window predates the current sample_epoch. */
			if (pg_atomic_read_u64(&slot->window_sample_epoch) != cur_epoch)
				continue;

			for (k = 0; k < CLUSTER_MAX_NODES; k++)
				total += pg_atomic_read_u32(&slot->access_count[k]);

			/* Normalize the sampled total by the sample rate (Amend v1.1-a⑤). */
			if (total * (uint64)rate >= (uint64)min_access)
				out_shard_ids[n++] = shard;
		}
	}

	return n;
}

/* ----------
 * Read accessors (dump category + unit tests) — pure reads
 * ----------
 */

uint32
cluster_drm_affinity_access_count(uint32 shard_id, int32 node)
{
	if (drm_state == NULL || shard_id >= PGRAC_GRD_SHARD_COUNT || node < 0
		|| node >= CLUSTER_MAX_NODES)
		return 0;
	return pg_atomic_read_u32(&drm_state->shard[shard_id].access_count[node]);
}

uint64
cluster_drm_affinity_get_sample_epoch(void)
{
	if (drm_state == NULL)
		return 0;
	return pg_atomic_read_u64(&drm_state->sample_epoch);
}

uint64
cluster_drm_affinity_get_counter(int which)
{
	if (drm_state == NULL)
		return 0;
	switch (which) {
	case CLUSTER_DRM_AFFINITY_CTR_RECORDED:
		return pg_atomic_read_u64(&drm_state->samples_recorded);
	case CLUSTER_DRM_AFFINITY_CTR_SKIPPED_OFF:
		return pg_atomic_read_u64(&drm_state->samples_skipped_off);
	case CLUSTER_DRM_AFFINITY_CTR_DROPPED_STALE:
		return pg_atomic_read_u64(&drm_state->samples_dropped_stale_epoch);
	case CLUSTER_DRM_AFFINITY_CTR_FLUSH_BATCHES:
		return pg_atomic_read_u64(&drm_state->flush_batches);
	case CLUSTER_DRM_AFFINITY_CTR_LOCAL:
		return pg_atomic_read_u64(&drm_state->samples_local);
	case CLUSTER_DRM_AFFINITY_CTR_REMOTE:
		return pg_atomic_read_u64(&drm_state->samples_remote);
	default:
		return 0;
	}
}
