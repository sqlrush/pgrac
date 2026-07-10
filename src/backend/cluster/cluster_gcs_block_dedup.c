/*-------------------------------------------------------------------------
 *
 * cluster_gcs_block_dedup.c
 *	  pgrac cluster GCS block reliability hardening — master-side dedup HTAB
 *	  implementation (spec-2.34 D2 + D5 + D6).
 *
 *	  Implements:
 *	    - shmem region "pgrac cluster gcs block dedup" with a per-worker
 *	      shard array (spec-7.3 D5): dedup_shards[worker_id] each own their
 *	      header + HTAB + LWLock, so the LMS worker pool never contends on
 *	      one lock and never shares dedup state across workers
 *	    - lookup_or_register / install_reply / remove APIs (worker_id-keyed)
 *	    - TTL sweep + node-dead + backend-exit GC (iterate every shard)
 *	    - before_shmem_exit local backend cleanup hook
 *	    - counter accessors that sum across shards + a misroute fail-closed
 *	      counter in the always-present ctl header (8.A)
 *
 *	  See cluster_gcs_block_dedup.h for the HC contracts and entry layout.
 *
 *	  spec-7.3 D5 承重 invariant: a block tag routes to exactly one worker
 *	  (worker[shard(tag)], D4), and every message for that tag — original +
 *	  retransmits — carries the same request_id, so the dedup entry for a
 *	  request lives in exactly one shard.  Accessing a shard other than the
 *	  routed worker is a mis-route (序破坏); the hot-path bounds guard fails
 *	  it closed (FULL + misroute_failclosed_count++) rather than serving
 *	  from the wrong shard.
 *
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/backend/cluster/cluster_gcs_block_dedup.c
 *
 * NOTES
 *	  This is a pgrac-original file.
 *	  Spec: spec-2.34-gcs-block-reliability-hardening.md (FROZEN v0.3)
 *	  Spec: spec-7.3-lms-worker-pool.md (D5 — per-worker shard)
 *	  Design: docs/cache-fusion-protocol-design.md
 *	  AD-005 (Cache Fusion full)
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#ifdef USE_PGRAC_CLUSTER

#include "cluster/cluster_conf.h" /* declared_node_count_early (spec-7.2a D4) */
#include "cluster/cluster_gcs_block_dedup.h"
#include "cluster/cluster_guc.h"
#include "cluster/cluster_lms_shard.h" /* CLUSTER_LMS_MAX_WORKERS */
#include "cluster/cluster_shmem.h"
#include "miscadmin.h"
#include "port/atomics.h"
#include "storage/ipc.h"
#include "storage/lwlock.h"
#include "storage/shmem.h"
#include "utils/elog.h"
#include "utils/hsearch.h"
#include "utils/timestamp.h"


/* ============================================================
 * Module-scope shmem state (spec-7.3 D5 — per-worker shards).
 *
 *	ClusterGcsBlockDedupShard is the old ClusterGcsBlockDedupShared renamed:
 *	one private lock + counter block + HTAB per LMS worker.  The ctl header
 *	holds cluster-wide state that must exist even when no worker touches its
 *	own shard (the misroute counter, the live shard count).
 * ============================================================ */

typedef struct ClusterGcsBlockDedupShard {
	LWLockPadded lock;				  /* guards this shard's HTAB + entry_count */
	pg_atomic_uint64 hit_count;		  /* CACHED_REPLY paths */
	pg_atomic_uint64 miss_count;	  /* MISS_REGISTERED paths */
	pg_atomic_uint64 collision_count; /* HC91 VALIDATION_FAIL */
	pg_atomic_uint64 full_count;	  /* HC92 FULL */
	pg_atomic_uint64 evict_count;	  /* spec-7.2a: eager reclaim + TTL sweep removed */
	pg_atomic_uint32 entry_count;	  /* live in-flight + completed entries */
} ClusterGcsBlockDedupShard;

typedef struct ClusterGcsBlockDedupCtl {
	pg_atomic_uint64 misroute_failclosed_count; /* spec-7.3 D5 — 8.A drops */
	int n_shards;								/* live shard count fixed at init */
	int max_entries_effective;					/* spec-7.2a D4: per-shard cap the
												 * HTABs were sized with (configured
												 * + auto-size floor); stamped once
												 * at init, read-only after */
} ClusterGcsBlockDedupCtl;

static ClusterGcsBlockDedupCtl *cluster_gcs_block_dedup_ctl = NULL;
static ClusterGcsBlockDedupShard *cluster_gcs_block_dedup_shards = NULL;
static HTAB *cluster_gcs_block_dedup_htabs[CLUSTER_LMS_MAX_WORKERS];
static int cluster_gcs_block_dedup_n_shards = 0;
static bool dedup_backend_exit_hook_registered = false;

/*
 * Upper bound on entries examined per cap-full eager-reclaim probe.  We do
 * NOT full-scan the HTAB under the exclusive lock on every cap-full MISS
 * (spec-7.2a §6 "no full scan"): if no reclaim-safe entry is found within the
 * probe budget, fall back to the fail-closed HC92 path.
 */
#define GCS_BLOCK_DEDUP_RECLAIM_MAX_PROBE 256

/*
 * spec-7.2a D5:  emit a saturation LOG only after this many additional
 * DENIED_DEDUP_FULL events accrue since the last report, so a persistently
 * full table logs at most once per this many drops (rule 17: no hot-path
 * flood).  The LMON TTL sweep is the sole evaluation site.
 */
#define GCS_BLOCK_DEDUP_FULL_LOG_THRESHOLD 64

/* Forward declarations for GC helpers used by the MISS-path eager reclaim. */
static int64 dedup_expiry_threshold_us(void);
static int dedup_reclaim_reclaimable_locked(ClusterGcsBlockDedupShard *shard, HTAB *htab,
											TimestampTz now, int want);


static int
cluster_gcs_block_dedup_effective_entries(void)
{
	/*
	 * Heavy GCS block-dedup storage is only meaningful for configured
	 * cluster nodes.  initdb/bootstrap runs with cluster_node_id = -1 and
	 * tiny shared_buffers; allocating the 8KB-entry HTAB there can exceed
	 * PG's bootstrap shmem budget before any cluster path is usable.
	 */
	if (!cluster_enabled || cluster_node_id < 0)
		return 0;

	{
		int configured
			= cluster_gcs_block_dedup_max_entries > 0 ? cluster_gcs_block_dedup_max_entries : 1024;
		int64 auto_floor;

		/*
		 * spec-7.2a D4 (Q4) auto-size lower bound: every connected backend on
		 * every declared node can hold one block request in flight against
		 * this master, so a configured cap below MaxConnections × node_count
		 * is guaranteed to saturate under distinct-read pressure.  Raise such
		 * configs to that floor, clamped at the GUC ceiling — auto-sizing
		 * widens a foot-gun config, it never grows shmem past what the DBA
		 * could configure by hand.  The node count comes from the pre-shmem
		 * conf sniff (cluster_conf_load() runs only after every region is
		 * initialised, so cluster_conf_node_count() is still 0 when
		 * size_fn/init_fn call here); with no readable pgrac.conf the sniff
		 * reports 1 and the floor degenerates to MaxConnections.  The floor
		 * applies PER SHARD (spec-7.3 D5): tags route to exactly one worker's
		 * shard, so a single hot shard must alone absorb the floor's
		 * worst-case in-flight population.
		 */
		auto_floor = (int64)MaxConnections * cluster_conf_declared_node_count_early();
		if (auto_floor > CLUSTER_GCS_BLOCK_DEDUP_MAX_ENTRIES_CEILING)
			auto_floor = CLUSTER_GCS_BLOCK_DEDUP_MAX_ENTRIES_CEILING;
		if (configured < (int)auto_floor)
			configured = (int)auto_floor;

		return configured;
	}
}

/*
 * spec-7.3 D5 — live shard count = configured LMS worker count, clamped to
 * the compile-time cap.  POSTMASTER-level GUC, so this is stable across the
 * shmem_size / shmem_init pair and for the process lifetime.
 */
static int
cluster_gcs_block_dedup_live_shards(void)
{
	int n = cluster_lms_workers;

	if (n < 1)
		n = 1;
	if (n > CLUSTER_LMS_MAX_WORKERS)
		n = CLUSTER_LMS_MAX_WORKERS;
	return n;
}

/*
 * Bytes carved by ShmemInitStruct: ctl header + the per-worker shard array
 * (the HTABs are allocated separately by ShmemInitHash).
 */
static Size
cluster_gcs_block_dedup_struct_bytes(int n_shards)
{
	Size sz;

	sz = MAXALIGN(sizeof(ClusterGcsBlockDedupCtl));
	sz = add_size(sz, mul_size(n_shards, MAXALIGN(sizeof(ClusterGcsBlockDedupShard))));
	return sz;
}


/* ============================================================
 * Shmem registry.
 * ============================================================ */

Size
cluster_gcs_block_dedup_shmem_size(void)
{
	Size sz;
	int cap;
	int n;

	cap = cluster_gcs_block_dedup_effective_entries();
	if (cap == 0)
		return 0;

	n = cluster_gcs_block_dedup_live_shards();
	sz = cluster_gcs_block_dedup_struct_bytes(n);
	sz = add_size(sz, mul_size(n, hash_estimate_size(cap, sizeof(GcsBlockDedupEntry))));
	return sz;
}

void
cluster_gcs_block_dedup_shmem_init(void)
{
	bool found;
	HASHCTL info;
	int cap;
	int n;
	int i;
	char *base;

	cap = cluster_gcs_block_dedup_effective_entries();
	if (cap == 0)
		return;

	n = cluster_gcs_block_dedup_live_shards();

	base = (char *)ShmemInitStruct("pgrac cluster gcs block dedup",
								   cluster_gcs_block_dedup_struct_bytes(n), &found);
	cluster_gcs_block_dedup_ctl = (ClusterGcsBlockDedupCtl *)base;
	cluster_gcs_block_dedup_shards
		= (ClusterGcsBlockDedupShard *)(base + MAXALIGN(sizeof(ClusterGcsBlockDedupCtl)));
	cluster_gcs_block_dedup_n_shards = n;

	if (!found) {
		pg_atomic_init_u64(&cluster_gcs_block_dedup_ctl->misroute_failclosed_count, 0);
		cluster_gcs_block_dedup_ctl->n_shards = n;
		/* spec-7.2a D4: stamp the per-shard cap the HTABs below are sized
		 * with, so the observability accessor reports the capacity actually
		 * in force (identical in every process, EXEC_BACKEND included). */
		cluster_gcs_block_dedup_ctl->max_entries_effective = cap;

		for (i = 0; i < n; i++) {
			ClusterGcsBlockDedupShard *shard = &cluster_gcs_block_dedup_shards[i];

			memset(shard, 0, sizeof(*shard));
			LWLockInitialize(&shard->lock.lock, LWTRANCHE_CLUSTER_GCS_BLOCK_DEDUP);
			pg_atomic_init_u64(&shard->hit_count, 0);
			pg_atomic_init_u64(&shard->miss_count, 0);
			pg_atomic_init_u64(&shard->collision_count, 0);
			pg_atomic_init_u64(&shard->full_count, 0);
			pg_atomic_init_u64(&shard->evict_count, 0);
			pg_atomic_init_u32(&shard->entry_count, 0);
		}
	}

	memset(&info, 0, sizeof(info));
	info.keysize = sizeof(GcsBlockDedupKey);
	info.entrysize = sizeof(GcsBlockDedupEntry);

	for (i = 0; i < n; i++) {
		char hname[96];

		/* shard 0 keeps the spec-2.34 name so lms_workers=1 is a
		 * byte-identical topology (spec-7.3 §3.5). */
		if (i == 0)
			snprintf(hname, sizeof(hname), "pgrac cluster gcs block dedup htab");
		else
			snprintf(hname, sizeof(hname), "pgrac cluster gcs block dedup htab %d", i);

		cluster_gcs_block_dedup_htabs[i]
			= ShmemInitHash(hname, cap, cap, &info, HASH_ELEM | HASH_BLOBS);
	}
}

static const ClusterShmemRegion cluster_gcs_block_dedup_region = {
	.name = "pgrac cluster gcs block dedup",
	.size_fn = cluster_gcs_block_dedup_shmem_size,
	.init_fn = cluster_gcs_block_dedup_shmem_init,
	.lwlock_count = 0,
	.owner_subsys = "cluster_gcs_block_dedup",
	.reserved_flags = 0,
};

void
cluster_gcs_block_dedup_module_init(void)
{
	cluster_shmem_register_region(&cluster_gcs_block_dedup_region);
}


/* ============================================================
 * spec-7.3 D5 — shard resolution.  Returns the shard for a hot-path
 * worker_id, or NULL (fail-closed) when the module is not initialized or
 * the worker_id is out of the live range (a mis-route, counted).
 * ============================================================ */
static ClusterGcsBlockDedupShard *
cluster_gcs_block_dedup_resolve_shard(int worker_id, HTAB **htab_out)
{
	if (cluster_gcs_block_dedup_ctl == NULL || cluster_gcs_block_dedup_shards == NULL)
		return NULL; /* module off — fail-closed, not a mis-route */

	if (worker_id < 0 || worker_id >= cluster_gcs_block_dedup_n_shards
		|| cluster_gcs_block_dedup_htabs[worker_id] == NULL) {
		/* A block tag reached the wrong worker: shard key = tag alone, and
		 * D3 negotiates a cluster-wide n_workers, so this is a code-path
		 * invariant break.  Fail closed (8.A), never serve wrong shard. */
		cluster_gcs_block_dedup_note_misroute();
		return NULL;
	}

	if (htab_out != NULL)
		*htab_out = cluster_gcs_block_dedup_htabs[worker_id];
	return &cluster_gcs_block_dedup_shards[worker_id];
}


/* ============================================================
 * Backend-exit hook registration (idempotent; called by the sender/backend
 * path once per backend before it issues block requests).
 * ============================================================ */

static void
dedup_backend_exit_callback(int code pg_attribute_unused(), Datum arg pg_attribute_unused())
{
	if (cluster_node_id < 0 || MyBackendId <= 0)
		return;
	cluster_gcs_block_dedup_cleanup_on_backend_exit((uint32)cluster_node_id, (int32)MyBackendId);
}

void
cluster_gcs_block_dedup_register_backend_exit_hook(void)
{
	if (dedup_backend_exit_hook_registered)
		return;
	if (!IsUnderPostmaster)
		return; /* only meaningful in backends */
	if (MyBackendId <= 0)
		return; /* auxiliary processes do not own backend ids */
	before_shmem_exit(dedup_backend_exit_callback, (Datum)0);
	dedup_backend_exit_hook_registered = true;
}


/* ============================================================
 * Public API.
 * ============================================================ */

GcsBlockDedupResult
cluster_gcs_block_dedup_lookup_or_register(int worker_id, const GcsBlockDedupKey *key,
										   BufferTag tag, uint8 transition_id,
										   GcsBlockDedupEntry *cached_reply_out)
{
	ClusterGcsBlockDedupShard *shard;
	HTAB *htab = NULL;
	GcsBlockDedupEntry *entry;
	bool found;
	GcsBlockDedupResult result;

	Assert(key != NULL);
	if (cached_reply_out != NULL)
		memset(cached_reply_out, 0, sizeof(*cached_reply_out));

	shard = cluster_gcs_block_dedup_resolve_shard(worker_id, &htab);
	if (shard == NULL)
		return GCS_BLOCK_DEDUP_FULL; /* not initialized / mis-route; fail closed */

	LWLockAcquire(&shard->lock.lock, LW_EXCLUSIVE);

	entry = (GcsBlockDedupEntry *)hash_search(htab, key, HASH_FIND, &found);

	if (found) {
		/* HC91 — entry value collision check */
		if (memcmp(&entry->tag, &tag, sizeof(BufferTag)) != 0
			|| entry->transition_id != transition_id) {
			pg_atomic_fetch_add_u64(&shard->collision_count, 1);
			LWLockRelease(&shard->lock.lock);
			return GCS_BLOCK_DEDUP_VALIDATION_FAIL;
		}

		/* PGRAC: spec-2.35 HC113/HC114 — forwarded entry path.  Master
		 * previously installed this entry with status=GRANTED_FROM_HOLDER
		 * to mark "forward in flight"; reply_header.sender_node carries
		 * the holder node id (not an 8KB cached block).  Caller must
		 * re-forward to the same holder rather than treat as silent
		 * duplicate.  This branch fires WHETHER OR NOT completed_at_ts
		 * is zero — the forward install_reply path stamps completed_at_ts
		 * so TTL sweep can age the entry; consumers distinguish FORWARDED
		 * from genuine CACHED_REPLY via the status field. */
		if (entry->status == (uint8)GCS_BLOCK_REPLY_GRANTED_FROM_HOLDER
			|| entry->status == (uint8)GCS_BLOCK_REPLY_X_GRANTED_FROM_HOLDER) {
			if (cached_reply_out != NULL)
				*cached_reply_out = *entry;
			LWLockRelease(&shard->lock.lock);
			return GCS_BLOCK_DEDUP_FORWARDED_DUPLICATE;
		}

		if (entry->completed_at_ts == 0) {
			LWLockRelease(&shard->lock.lock);
			return GCS_BLOCK_DEDUP_IN_FLIGHT_DUPLICATE;
		}

		pg_atomic_fetch_add_u64(&shard->hit_count, 1);
		if (cached_reply_out != NULL)
			*cached_reply_out = *entry;
		LWLockRelease(&shard->lock.lock);
		return GCS_BLOCK_DEDUP_CACHED_REPLY;
	}

	/* MISS path — insert new in-flight slot.  HASH_ENTER_NULL → may fail
	 * with cap reached; convert to FULL fail-closed (HC92). */
	entry = (GcsBlockDedupEntry *)hash_search(htab, key, HASH_ENTER_NULL, &found);
	if (entry == NULL) {
		/* PGRAC: spec-7.2a D1 — before failing closed, try to reclaim one
		 * reclaim-safe entry (aged past the 2x window, or a site-proven
		 * idempotent status) to make room.  Reclaim never removes an entry
		 * whose retransmitted duplicate could still be served incorrectly
		 * (§3.1); if nothing is safe to reclaim, keep the fail-closed HC92
		 * behavior below. */
		if (dedup_reclaim_reclaimable_locked(shard, htab, GetCurrentTimestamp(), 1) > 0)
			entry = (GcsBlockDedupEntry *)hash_search(htab, key, HASH_ENTER_NULL, &found);
	}
	if (entry == NULL) {
		pg_atomic_fetch_add_u64(&shard->full_count, 1);
		LWLockRelease(&shard->lock.lock);
		return GCS_BLOCK_DEDUP_FULL;
	}

	/* Reset entry to clean in-flight state. */
	memset(((char *)entry) + sizeof(GcsBlockDedupKey), 0,
		   sizeof(GcsBlockDedupEntry) - sizeof(GcsBlockDedupKey));
	entry->tag = tag;
	entry->transition_id = transition_id;
	entry->status = 0;
	entry->completed_at_ts = 0;
	entry->registered_at_ts = GetCurrentTimestamp();

	pg_atomic_fetch_add_u32(&shard->entry_count, 1);
	pg_atomic_fetch_add_u64(&shard->miss_count, 1);
	result = GCS_BLOCK_DEDUP_MISS_REGISTERED;

	LWLockRelease(&shard->lock.lock);
	return result;
}

void
cluster_gcs_block_dedup_install_reply_ex(int worker_id, const GcsBlockDedupKey *key,
										 GcsBlockReplyStatus status,
										 const GcsBlockReplyHeader *header, const char *block_data,
										 const ClusterSfDepVec *sf_dep_vec, bool has_sf_dep)
{
	ClusterGcsBlockDedupShard *shard;
	HTAB *htab = NULL;
	GcsBlockDedupEntry *entry;
	bool found;
	bool has_block_payload;

	Assert(key != NULL);
	Assert(header != NULL);

	shard = cluster_gcs_block_dedup_resolve_shard(worker_id, &htab);
	if (shard == NULL)
		return;

	LWLockAcquire(&shard->lock.lock, LW_EXCLUSIVE);

	entry = (GcsBlockDedupEntry *)hash_search(htab, key, HASH_FIND, &found);
	if (!found) {
		/* Entry got swept between MISS_REGISTERED and install_reply
		 * (rare:  TTL sweep + reconfig race).  Drop silently — the
		 * sender will eventually time out and retry. */
		LWLockRelease(&shard->lock.lock);
		return;
	}

	entry->status = (uint8)status;
	entry->reply_header = *header;
	entry->has_sf_dep = has_sf_dep;
	entry->sf_flags
		= has_sf_dep ? (GCS_BLOCK_REPLY_SF_HAS_DEP_VEC | GCS_BLOCK_REPLY_SF_EARLY_TRANSFER) : 0;
	entry->sf_dep_count = 0;
	cluster_sf_dep_vec_reset(&entry->sf_dep_vec);
	if (has_sf_dep && sf_dep_vec != NULL) {
		int i;

		for (i = 0; i < CLUSTER_SF_DEP_MAX_ORIGINS; i++) {
			if (XLogRecPtrIsInvalid(sf_dep_vec->required[i]))
				continue;
			entry->sf_dep_vec.required[i] = sf_dep_vec->required[i];
			entry->sf_dep_count++;
		}
	}
	has_block_payload = block_data != NULL
						&& (status == GCS_BLOCK_REPLY_GRANTED
							|| status == GCS_BLOCK_REPLY_READ_IMAGE_FROM_XHOLDER);
	if (has_block_payload)
		memcpy(entry->block_data, block_data, GCS_BLOCK_DATA_SIZE);
	else
		memset(entry->block_data, 0, GCS_BLOCK_DATA_SIZE);
	entry->completed_at_ts = GetCurrentTimestamp();

	LWLockRelease(&shard->lock.lock);
}

void
cluster_gcs_block_dedup_install_reply(int worker_id, const GcsBlockDedupKey *key,
									  GcsBlockReplyStatus status, const GcsBlockReplyHeader *header,
									  const char *block_data)
{
	cluster_gcs_block_dedup_install_reply_ex(worker_id, key, status, header, block_data, NULL,
											 false);
}

void
cluster_gcs_block_dedup_remove(int worker_id, const GcsBlockDedupKey *key)
{
	ClusterGcsBlockDedupShard *shard;
	HTAB *htab = NULL;
	bool found;

	Assert(key != NULL);

	shard = cluster_gcs_block_dedup_resolve_shard(worker_id, &htab);
	if (shard == NULL)
		return;

	LWLockAcquire(&shard->lock.lock, LW_EXCLUSIVE);
	(void)hash_search(htab, key, HASH_REMOVE, &found);
	if (found)
		pg_atomic_fetch_sub_u32(&shard->entry_count, 1);
	LWLockRelease(&shard->lock.lock);
}


/* ============================================================
 * GC paths — TTL sweep, backend exit, node DEAD.
 *
 * spec-7.3 D5 — these run in non-worker processes (LMON tick, requester
 * backend before_shmem_exit, CSSD DEAD hook) and a request's entries are
 * spread across shards by tag, so every GC path iterates all live shards.
 * ============================================================ */

/*
 * Compute the configured expiry threshold in microseconds.  The threshold
 * is 2 × the total exponential backoff window (so both completed and
 * in-flight entries are gone before the next retry round could possibly
 * re-arrive after a hot reconfig).  We approximate using the GUC
 * defaults; configurable backoff bases produce slightly larger thresholds
 * — sweeping conservatively biases toward retention.
 */
static int64
dedup_expiry_threshold_us(void)
{
	int64 initial_ms;
	int max_retries;
	int64 total_backoff_ms;

	initial_ms = cluster_gcs_block_retransmit_initial_backoff_ms > 0
					 ? cluster_gcs_block_retransmit_initial_backoff_ms
					 : 100;
	max_retries = cluster_gcs_block_retransmit_max_retries >= 0
					  ? cluster_gcs_block_retransmit_max_retries
					  : 4;

	/* total = initial × (2^max_retries - 1);  pin small max_retries to
	 * keep arithmetic in int64. */
	if (max_retries > 30)
		max_retries = 30;
	total_backoff_ms = initial_ms * ((int64)((1u << max_retries) - 1));
	return total_backoff_ms * 1000 * 2; /* × 2 safety margin (HC93) */
}

/*
 * dedup_reclaim_reclaimable_locked -- caller MUST hold this shard's dedup
 * lock exclusively.  Under cap pressure (HASH_ENTER_NULL about to fail),
 * scan the shard's HTAB and remove up to `want` reclaim-safe entries so the
 * MISS path can register instead of failing closed (spec-7.2a D1).
 *
 * Only entries GcsBlockDedupEntryIsReclaimSafe() approves are removed: an
 * entry aged past the 2x out-of-window threshold (safe for every status,
 * §3.1 theorem) or one whose status is site-proven in-window idempotent
 * (whitelist currently empty).  Reclaim NEVER removes an entry whose
 * retransmitted duplicate could be re-served incorrectly — it only ever
 * brings the FULL path forward in time, never sacrifices correctness.
 *
 * The scan is bounded to GCS_BLOCK_DEDUP_RECLAIM_MAX_PROBE entries so a
 * shard full of in-window entries does not turn every MISS into a full
 * O(cap) scan under the exclusive lock (spec-7.2a §6).  Returns the number
 * reclaimed.
 */
static int
dedup_reclaim_reclaimable_locked(ClusterGcsBlockDedupShard *shard, HTAB *htab, TimestampTz now,
								 int want)
{
	HASH_SEQ_STATUS seq;
	GcsBlockDedupEntry *entry;
	int64 out_of_window_us;
	int reclaimed = 0;
	int probed = 0;

	if (want <= 0)
		return 0;

	out_of_window_us = dedup_expiry_threshold_us();

	hash_seq_init(&seq, htab);
	while ((entry = (GcsBlockDedupEntry *)hash_seq_search(&seq)) != NULL) {
		if (GcsBlockDedupEntryIsReclaimSafe(entry, now, out_of_window_us)) {
			(void)hash_search(htab, &entry->key, HASH_REMOVE, NULL);
			reclaimed++;
		}

		if (reclaimed >= want || ++probed >= GCS_BLOCK_DEDUP_RECLAIM_MAX_PROBE) {
			hash_seq_term(&seq); /* early break must terminate the scan */
			break;
		}
	}

	if (reclaimed > 0) {
		pg_atomic_fetch_sub_u32(&shard->entry_count, (uint32)reclaimed);
		pg_atomic_fetch_add_u64(&shard->evict_count, (uint64)reclaimed);
	}
	return reclaimed;
}

void
cluster_gcs_block_dedup_sweep_expired(TimestampTz now)
{
	int64 threshold_us;
	int s;

	if (cluster_gcs_block_dedup_shards == NULL)
		return;

	threshold_us = dedup_expiry_threshold_us();

	/*
	 * spec-7.2a D5:  saturation LOG-once.  When DENIED_DEDUP_FULL keeps
	 * growing past a threshold across sweep cycles, emit one LOG so operators
	 * see sustained dedup saturation without flooding the hot request path
	 * (rule 17).  Lock-free (atomics only); the LMON sweep is the sole caller
	 * so the static high-water mark is process-local and race-free.  The
	 * counters aggregate over every shard (spec-7.3 D5).
	 */
	{
		static uint64 dedup_full_logged_hwm = 0;
		uint64 cur_full = cluster_gcs_block_dedup_get_full_count();

		if (cur_full - dedup_full_logged_hwm >= GCS_BLOCK_DEDUP_FULL_LOG_THRESHOLD) {
			elog(LOG,
				 "GCS block dedup saturating: %llu new DENIED_DEDUP_FULL since last report "
				 "(per-shard cap=%d, live entries=%llu); raise "
				 "cluster.gcs_block_dedup_max_entries if sustained",
				 (unsigned long long)(cur_full - dedup_full_logged_hwm),
				 cluster_gcs_block_dedup_ctl != NULL
					 ? cluster_gcs_block_dedup_ctl->max_entries_effective
					 : 0,
				 (unsigned long long)cluster_gcs_block_dedup_get_in_flight_count());
			dedup_full_logged_hwm = cur_full;
		}
	}

	for (s = 0; s < cluster_gcs_block_dedup_n_shards; s++) {
		ClusterGcsBlockDedupShard *shard = &cluster_gcs_block_dedup_shards[s];
		HTAB *htab = cluster_gcs_block_dedup_htabs[s];
		HASH_SEQ_STATUS seq;
		GcsBlockDedupEntry *entry;
		int removed = 0;

		if (htab == NULL)
			continue;

		LWLockAcquire(&shard->lock.lock, LW_EXCLUSIVE);

		hash_seq_init(&seq, htab);
		while ((entry = (GcsBlockDedupEntry *)hash_seq_search(&seq)) != NULL) {
			TimestampTz anchor;
			int64 age_us;

			anchor = entry->completed_at_ts != 0 ? entry->completed_at_ts : entry->registered_at_ts;
			if (anchor == 0)
				continue;

			age_us = (int64)(now - anchor);
			if (age_us > threshold_us) {
				(void)hash_search(htab, &entry->key, HASH_REMOVE, NULL);
				removed++;
			}
		}

		if (removed > 0) {
			pg_atomic_fetch_sub_u32(&shard->entry_count, (uint32)removed);
			/* spec-7.2a D5: evict_count aggregates eager reclaim + TTL sweep. */
			pg_atomic_fetch_add_u64(&shard->evict_count, (uint64)removed);
		}

		LWLockRelease(&shard->lock.lock);
	}
}

void
cluster_gcs_block_dedup_cleanup_on_backend_exit(uint32 origin_node_id, int32 backend_id)
{
	int s;

	if (cluster_gcs_block_dedup_shards == NULL)
		return;

	for (s = 0; s < cluster_gcs_block_dedup_n_shards; s++) {
		ClusterGcsBlockDedupShard *shard = &cluster_gcs_block_dedup_shards[s];
		HTAB *htab = cluster_gcs_block_dedup_htabs[s];
		HASH_SEQ_STATUS seq;
		GcsBlockDedupEntry *entry;
		int removed = 0;

		if (htab == NULL)
			continue;

		LWLockAcquire(&shard->lock.lock, LW_EXCLUSIVE);
		hash_seq_init(&seq, htab);
		while ((entry = (GcsBlockDedupEntry *)hash_seq_search(&seq)) != NULL) {
			if (entry->key.origin_node_id == origin_node_id
				&& entry->key.requester_backend_id == backend_id) {
				(void)hash_search(htab, &entry->key, HASH_REMOVE, NULL);
				removed++;
			}
		}
		if (removed > 0)
			pg_atomic_fetch_sub_u32(&shard->entry_count, (uint32)removed);
		LWLockRelease(&shard->lock.lock);
	}
}

void
cluster_gcs_block_dedup_cleanup_on_node_dead(uint32 node_id)
{
	int s;

	if (cluster_gcs_block_dedup_shards == NULL)
		return;

	for (s = 0; s < cluster_gcs_block_dedup_n_shards; s++) {
		ClusterGcsBlockDedupShard *shard = &cluster_gcs_block_dedup_shards[s];
		HTAB *htab = cluster_gcs_block_dedup_htabs[s];
		HASH_SEQ_STATUS seq;
		GcsBlockDedupEntry *entry;
		int removed = 0;

		if (htab == NULL)
			continue;

		LWLockAcquire(&shard->lock.lock, LW_EXCLUSIVE);
		hash_seq_init(&seq, htab);
		while ((entry = (GcsBlockDedupEntry *)hash_seq_search(&seq)) != NULL) {
			if (entry->key.origin_node_id == node_id) {
				(void)hash_search(htab, &entry->key, HASH_REMOVE, NULL);
				removed++;
			}
		}
		if (removed > 0)
			pg_atomic_fetch_sub_u32(&shard->entry_count, (uint32)removed);
		LWLockRelease(&shard->lock.lock);
	}
}


/* ============================================================
 * Observability accessors — aggregate (sum) over every live shard.
 * ============================================================ */

static uint64
cluster_gcs_block_dedup_sum_u64(size_t member_offset)
{
	uint64 total = 0;
	int s;

	if (cluster_gcs_block_dedup_shards == NULL)
		return 0;

	for (s = 0; s < cluster_gcs_block_dedup_n_shards; s++) {
		pg_atomic_uint64 *ctr
			= (pg_atomic_uint64 *)((char *)&cluster_gcs_block_dedup_shards[s] + member_offset);

		total += pg_atomic_read_u64(ctr);
	}
	return total;
}

uint64
cluster_gcs_block_dedup_get_hit_count(void)
{
	return cluster_gcs_block_dedup_sum_u64(offsetof(ClusterGcsBlockDedupShard, hit_count));
}

uint64
cluster_gcs_block_dedup_get_miss_count(void)
{
	return cluster_gcs_block_dedup_sum_u64(offsetof(ClusterGcsBlockDedupShard, miss_count));
}

uint64
cluster_gcs_block_dedup_get_collision_count(void)
{
	return cluster_gcs_block_dedup_sum_u64(offsetof(ClusterGcsBlockDedupShard, collision_count));
}

uint64
cluster_gcs_block_dedup_get_full_count(void)
{
	return cluster_gcs_block_dedup_sum_u64(offsetof(ClusterGcsBlockDedupShard, full_count));
}

uint64
cluster_gcs_block_dedup_get_in_flight_count(void)
{
	uint64 total = 0;
	int s;

	if (cluster_gcs_block_dedup_shards == NULL)
		return 0;

	for (s = 0; s < cluster_gcs_block_dedup_n_shards; s++)
		total += (uint64)pg_atomic_read_u32(&cluster_gcs_block_dedup_shards[s].entry_count);
	return total;
}

uint64
cluster_gcs_block_dedup_get_evict_count(void)
{
	return cluster_gcs_block_dedup_sum_u64(offsetof(ClusterGcsBlockDedupShard, evict_count));
}

/*
 * cluster_gcs_block_dedup_get_max_entries -- effective PER-SHARD dedup
 * capacity.
 *
 *	spec-7.2a D5:  reports the per-shard cap stamped at shmem init (the GUC
 *	value raised to the D4 auto-size floor — the size each shard HTAB was
 *	actually built with), or 0 when the HTABs were never allocated
 *	(initdb/bootstrap before a cluster node id is assigned, or vanilla mode).
 *	The occupancy ratio entry_count / max_entries is the saturation signal
 *	behind the DEDUP_FULL fail-closed path.
 */
uint64
cluster_gcs_block_dedup_get_max_entries(void)
{
	return cluster_gcs_block_dedup_ctl ? (uint64)cluster_gcs_block_dedup_ctl->max_entries_effective
									   : 0;
}

uint64
cluster_gcs_block_dedup_get_misroute_failclosed_count(void)
{
	return cluster_gcs_block_dedup_ctl
			   ? pg_atomic_read_u64(&cluster_gcs_block_dedup_ctl->misroute_failclosed_count)
			   : 0;
}

/*
 * spec-7.3 D5 — record a mis-routed dedup access (a block tag reaching the
 * wrong LMS worker).  Called both by the module's own bounds guard and by
 * the master-side handler when shard(tag) != its own DATA channel, so the
 * two fail-closed detectors share one observability face.
 */
void
cluster_gcs_block_dedup_note_misroute(void)
{
	if (cluster_gcs_block_dedup_ctl != NULL)
		pg_atomic_fetch_add_u64(&cluster_gcs_block_dedup_ctl->misroute_failclosed_count, 1);
}


#endif /* USE_PGRAC_CLUSTER */
