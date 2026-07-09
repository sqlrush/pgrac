/*-------------------------------------------------------------------------
 *
 * cluster_gcs_block_dedup.c
 *	  pgrac cluster GCS block reliability hardening — master-side dedup HTAB
 *	  implementation (spec-2.34 D2 + D5 + D6).
 *
 *	  Implements:
 *	    - shmem region "pgrac cluster gcs block dedup" with header + HTAB
 *	    - LWLock guarding HTAB lookup / install / sweep (HC90)
 *	    - lookup_or_register / install_reply / remove APIs
 *	    - TTL sweep (LMON tick body)
 *	    - before_shmem_exit local backend cleanup hook
 *	    - cleanup_on_node_dead (CSSD DEAD hook)
 *	    - 5 atomic counter accessors (hit / miss / collision / full / in_flight)
 *
 *	  See cluster_gcs_block_dedup.h for the HC contracts and entry layout.
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
 *	  Design: docs/cache-fusion-protocol-design.md
 *	  AD-005 (Cache Fusion full)
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#ifdef USE_PGRAC_CLUSTER

#include "cluster/cluster_gcs_block_dedup.h"
#include "cluster/cluster_guc.h"
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
 * Module-scope shmem state.
 * ============================================================ */

typedef struct ClusterGcsBlockDedupShared {
	LWLockPadded lock;				  /* guards HTAB + entry_count */
	pg_atomic_uint64 hit_count;		  /* CACHED_REPLY paths */
	pg_atomic_uint64 miss_count;	  /* MISS_REGISTERED paths */
	pg_atomic_uint64 collision_count; /* HC91 VALIDATION_FAIL */
	pg_atomic_uint64 full_count;	  /* HC92 FULL */
	pg_atomic_uint64 evict_count;	  /* spec-7.2a: eager reclaim + TTL sweep removed */
	pg_atomic_uint32 entry_count;	  /* live in-flight + completed entries */
} ClusterGcsBlockDedupShared;

static ClusterGcsBlockDedupShared *cluster_gcs_block_dedup_shared = NULL;
static HTAB *cluster_gcs_block_dedup_htab = NULL;
static bool dedup_backend_exit_hook_registered = false;

/*
 * Upper bound on entries examined per cap-full eager-reclaim probe.  We do
 * NOT full-scan the HTAB under the exclusive lock on every cap-full MISS
 * (spec-7.2a §6 "非全扫"): if no reclaim-safe entry is found within the probe
 * budget, fall back to the fail-closed HC92 path.
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
static int dedup_reclaim_reclaimable_locked(TimestampTz now, int want);


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

	return cluster_gcs_block_dedup_max_entries > 0 ? cluster_gcs_block_dedup_max_entries : 1024;
}


/* ============================================================
 * Shmem registry.
 * ============================================================ */

Size
cluster_gcs_block_dedup_shmem_size(void)
{
	Size sz;
	int cap;

	cap = cluster_gcs_block_dedup_effective_entries();
	if (cap == 0)
		return 0;

	sz = MAXALIGN(sizeof(ClusterGcsBlockDedupShared));
	sz = add_size(sz, hash_estimate_size(cap, sizeof(GcsBlockDedupEntry)));
	return sz;
}

void
cluster_gcs_block_dedup_shmem_init(void)
{
	bool found;
	HASHCTL info;
	int cap;

	cap = cluster_gcs_block_dedup_effective_entries();
	if (cap == 0)
		return;

	cluster_gcs_block_dedup_shared = (ClusterGcsBlockDedupShared *)ShmemInitStruct(
		"pgrac cluster gcs block dedup", MAXALIGN(sizeof(ClusterGcsBlockDedupShared)), &found);

	if (!found) {
		memset(cluster_gcs_block_dedup_shared, 0, sizeof(*cluster_gcs_block_dedup_shared));
		LWLockInitialize(&cluster_gcs_block_dedup_shared->lock.lock,
						 LWTRANCHE_CLUSTER_GCS_BLOCK_DEDUP);
		pg_atomic_init_u64(&cluster_gcs_block_dedup_shared->hit_count, 0);
		pg_atomic_init_u64(&cluster_gcs_block_dedup_shared->miss_count, 0);
		pg_atomic_init_u64(&cluster_gcs_block_dedup_shared->collision_count, 0);
		pg_atomic_init_u64(&cluster_gcs_block_dedup_shared->full_count, 0);
		pg_atomic_init_u64(&cluster_gcs_block_dedup_shared->evict_count, 0);
		pg_atomic_init_u32(&cluster_gcs_block_dedup_shared->entry_count, 0);
	}

	memset(&info, 0, sizeof(info));
	info.keysize = sizeof(GcsBlockDedupKey);
	info.entrysize = sizeof(GcsBlockDedupEntry);

	cluster_gcs_block_dedup_htab = ShmemInitHash("pgrac cluster gcs block dedup htab", cap, cap,
												 &info, HASH_ELEM | HASH_BLOBS);
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
cluster_gcs_block_dedup_lookup_or_register(const GcsBlockDedupKey *key, BufferTag tag,
										   uint8 transition_id,
										   GcsBlockDedupEntry *cached_reply_out)
{
	GcsBlockDedupEntry *entry;
	bool found;
	GcsBlockDedupResult result;

	Assert(key != NULL);
	if (cached_reply_out != NULL)
		memset(cached_reply_out, 0, sizeof(*cached_reply_out));

	if (cluster_gcs_block_dedup_shared == NULL || cluster_gcs_block_dedup_htab == NULL)
		return GCS_BLOCK_DEDUP_FULL; /* not initialized; fail closed */

	LWLockAcquire(&cluster_gcs_block_dedup_shared->lock.lock, LW_EXCLUSIVE);

	entry = (GcsBlockDedupEntry *)hash_search(cluster_gcs_block_dedup_htab, key, HASH_FIND, &found);

	if (found) {
		/* HC91 — entry value collision check */
		if (memcmp(&entry->tag, &tag, sizeof(BufferTag)) != 0
			|| entry->transition_id != transition_id) {
			pg_atomic_fetch_add_u64(&cluster_gcs_block_dedup_shared->collision_count, 1);
			LWLockRelease(&cluster_gcs_block_dedup_shared->lock.lock);
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
			LWLockRelease(&cluster_gcs_block_dedup_shared->lock.lock);
			return GCS_BLOCK_DEDUP_FORWARDED_DUPLICATE;
		}

		if (entry->completed_at_ts == 0) {
			LWLockRelease(&cluster_gcs_block_dedup_shared->lock.lock);
			return GCS_BLOCK_DEDUP_IN_FLIGHT_DUPLICATE;
		}

		pg_atomic_fetch_add_u64(&cluster_gcs_block_dedup_shared->hit_count, 1);
		if (cached_reply_out != NULL)
			*cached_reply_out = *entry;
		LWLockRelease(&cluster_gcs_block_dedup_shared->lock.lock);
		return GCS_BLOCK_DEDUP_CACHED_REPLY;
	}

	/* MISS path — insert new in-flight slot.  HASH_ENTER_NULL → may fail
	 * with cap reached; convert to FULL fail-closed (HC92). */
	entry = (GcsBlockDedupEntry *)hash_search(cluster_gcs_block_dedup_htab, key, HASH_ENTER_NULL,
											  &found);
	if (entry == NULL) {
		/* PGRAC: spec-7.2a D1 — before failing closed, try to reclaim one
		 * reclaim-safe entry (aged past the 2x window, or a site-proven
		 * idempotent status) to make room.  Reclaim never removes an entry
		 * whose retransmitted duplicate could still be served incorrectly
		 * (§3.1); if nothing is safe to reclaim, keep the fail-closed HC92
		 * behavior below. */
		if (dedup_reclaim_reclaimable_locked(GetCurrentTimestamp(), 1) > 0)
			entry = (GcsBlockDedupEntry *)hash_search(cluster_gcs_block_dedup_htab, key,
													  HASH_ENTER_NULL, &found);
	}
	if (entry == NULL) {
		pg_atomic_fetch_add_u64(&cluster_gcs_block_dedup_shared->full_count, 1);
		LWLockRelease(&cluster_gcs_block_dedup_shared->lock.lock);
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

	pg_atomic_fetch_add_u32(&cluster_gcs_block_dedup_shared->entry_count, 1);
	pg_atomic_fetch_add_u64(&cluster_gcs_block_dedup_shared->miss_count, 1);
	result = GCS_BLOCK_DEDUP_MISS_REGISTERED;

	LWLockRelease(&cluster_gcs_block_dedup_shared->lock.lock);
	return result;
}

void
cluster_gcs_block_dedup_install_reply_ex(const GcsBlockDedupKey *key, GcsBlockReplyStatus status,
										 const GcsBlockReplyHeader *header, const char *block_data,
										 const ClusterSfDepVec *sf_dep_vec, bool has_sf_dep)
{
	GcsBlockDedupEntry *entry;
	bool found;
	bool has_block_payload;

	Assert(key != NULL);
	Assert(header != NULL);

	if (cluster_gcs_block_dedup_shared == NULL || cluster_gcs_block_dedup_htab == NULL)
		return;

	LWLockAcquire(&cluster_gcs_block_dedup_shared->lock.lock, LW_EXCLUSIVE);

	entry = (GcsBlockDedupEntry *)hash_search(cluster_gcs_block_dedup_htab, key, HASH_FIND, &found);
	if (!found) {
		/* Entry got swept between MISS_REGISTERED and install_reply
		 * (rare:  TTL sweep + reconfig race).  Drop silently — the
		 * sender will eventually time out and retry. */
		LWLockRelease(&cluster_gcs_block_dedup_shared->lock.lock);
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

	LWLockRelease(&cluster_gcs_block_dedup_shared->lock.lock);
}

void
cluster_gcs_block_dedup_install_reply(const GcsBlockDedupKey *key, GcsBlockReplyStatus status,
									  const GcsBlockReplyHeader *header, const char *block_data)
{
	cluster_gcs_block_dedup_install_reply_ex(key, status, header, block_data, NULL, false);
}

void
cluster_gcs_block_dedup_remove(const GcsBlockDedupKey *key)
{
	bool found;

	Assert(key != NULL);
	if (cluster_gcs_block_dedup_shared == NULL || cluster_gcs_block_dedup_htab == NULL)
		return;

	LWLockAcquire(&cluster_gcs_block_dedup_shared->lock.lock, LW_EXCLUSIVE);
	(void)hash_search(cluster_gcs_block_dedup_htab, key, HASH_REMOVE, &found);
	if (found)
		pg_atomic_fetch_sub_u32(&cluster_gcs_block_dedup_shared->entry_count, 1);
	LWLockRelease(&cluster_gcs_block_dedup_shared->lock.lock);
}


/* ============================================================
 * GC paths — TTL sweep, backend exit, node DEAD.
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
 * dedup_reclaim_reclaimable_locked -- caller MUST hold the dedup lock
 * exclusively.  Under cap pressure (HASH_ENTER_NULL about to fail), scan the
 * HTAB and remove up to `want` reclaim-safe entries so the MISS path can
 * register instead of failing closed (spec-7.2a D1).
 *
 * Only entries GcsBlockDedupEntryIsReclaimSafe() approves are removed: an
 * entry aged past the 2x out-of-window threshold (safe for every status,
 * §3.1 theorem) or one whose status is site-proven in-window idempotent
 * (whitelist currently empty).  Reclaim NEVER removes an entry whose
 * retransmitted duplicate could be re-served incorrectly — it only ever
 * brings the FULL path forward in time, never sacrifices correctness.
 *
 * The scan is bounded to GCS_BLOCK_DEDUP_RECLAIM_MAX_PROBE entries so a table
 * full of in-window entries does not turn every MISS into a full O(cap) scan
 * under the exclusive lock (spec-7.2a §6).  Returns the number reclaimed.
 */
static int
dedup_reclaim_reclaimable_locked(TimestampTz now, int want)
{
	HASH_SEQ_STATUS seq;
	GcsBlockDedupEntry *entry;
	int64 out_of_window_us;
	int reclaimed = 0;
	int probed = 0;

	if (want <= 0)
		return 0;

	out_of_window_us = dedup_expiry_threshold_us();

	hash_seq_init(&seq, cluster_gcs_block_dedup_htab);
	while ((entry = (GcsBlockDedupEntry *)hash_seq_search(&seq)) != NULL) {
		if (GcsBlockDedupEntryIsReclaimSafe(entry, now, out_of_window_us)) {
			(void)hash_search(cluster_gcs_block_dedup_htab, &entry->key, HASH_REMOVE, NULL);
			reclaimed++;
		}

		if (reclaimed >= want || ++probed >= GCS_BLOCK_DEDUP_RECLAIM_MAX_PROBE) {
			hash_seq_term(&seq); /* early break must terminate the scan */
			break;
		}
	}

	if (reclaimed > 0) {
		pg_atomic_fetch_sub_u32(&cluster_gcs_block_dedup_shared->entry_count, (uint32)reclaimed);
		pg_atomic_fetch_add_u64(&cluster_gcs_block_dedup_shared->evict_count, (uint64)reclaimed);
	}
	return reclaimed;
}

void
cluster_gcs_block_dedup_sweep_expired(TimestampTz now)
{
	HASH_SEQ_STATUS seq;
	GcsBlockDedupEntry *entry;
	int64 threshold_us;
	int removed = 0;

	if (cluster_gcs_block_dedup_shared == NULL || cluster_gcs_block_dedup_htab == NULL)
		return;

	threshold_us = dedup_expiry_threshold_us();

	/*
	 * spec-7.2a D5:  saturation LOG-once.  When DENIED_DEDUP_FULL keeps growing
	 * past a threshold across sweep cycles, emit one LOG so operators see
	 * sustained dedup saturation without flooding the hot request path (rule
	 * 17).  Lock-free (atomics only); the LMON sweep is the sole caller so the
	 * static high-water mark is process-local and race-free.
	 */
	{
		static uint64 dedup_full_logged_hwm = 0;
		uint64 cur_full = pg_atomic_read_u64(&cluster_gcs_block_dedup_shared->full_count);

		if (cur_full - dedup_full_logged_hwm >= GCS_BLOCK_DEDUP_FULL_LOG_THRESHOLD)
		{
			elog(LOG,
				 "GCS block dedup saturating: %llu new DENIED_DEDUP_FULL since last report "
				 "(cap=%d, live entries=%u); raise cluster.gcs_block_dedup_max_entries if sustained",
				 (unsigned long long) (cur_full - dedup_full_logged_hwm),
				 cluster_gcs_block_dedup_effective_entries(),
				 pg_atomic_read_u32(&cluster_gcs_block_dedup_shared->entry_count));
			dedup_full_logged_hwm = cur_full;
		}
	}

	LWLockAcquire(&cluster_gcs_block_dedup_shared->lock.lock, LW_EXCLUSIVE);

	hash_seq_init(&seq, cluster_gcs_block_dedup_htab);
	while ((entry = (GcsBlockDedupEntry *)hash_seq_search(&seq)) != NULL) {
		TimestampTz anchor;
		int64 age_us;

		anchor = entry->completed_at_ts != 0 ? entry->completed_at_ts : entry->registered_at_ts;
		if (anchor == 0)
			continue;

		age_us = (int64)(now - anchor);
		if (age_us > threshold_us) {
			(void)hash_search(cluster_gcs_block_dedup_htab, &entry->key, HASH_REMOVE, NULL);
			removed++;
		}
	}

	if (removed > 0) {
		pg_atomic_fetch_sub_u32(&cluster_gcs_block_dedup_shared->entry_count, (uint32)removed);
		/* spec-7.2a D5: evict_count aggregates eager reclaim + TTL sweep. */
		pg_atomic_fetch_add_u64(&cluster_gcs_block_dedup_shared->evict_count, (uint64)removed);
	}

	LWLockRelease(&cluster_gcs_block_dedup_shared->lock.lock);
}

void
cluster_gcs_block_dedup_cleanup_on_backend_exit(uint32 origin_node_id, int32 backend_id)
{
	HASH_SEQ_STATUS seq;
	GcsBlockDedupEntry *entry;
	int removed = 0;

	if (cluster_gcs_block_dedup_shared == NULL || cluster_gcs_block_dedup_htab == NULL)
		return;

	LWLockAcquire(&cluster_gcs_block_dedup_shared->lock.lock, LW_EXCLUSIVE);
	hash_seq_init(&seq, cluster_gcs_block_dedup_htab);
	while ((entry = (GcsBlockDedupEntry *)hash_seq_search(&seq)) != NULL) {
		if (entry->key.origin_node_id == origin_node_id
			&& entry->key.requester_backend_id == backend_id) {
			(void)hash_search(cluster_gcs_block_dedup_htab, &entry->key, HASH_REMOVE, NULL);
			removed++;
		}
	}
	if (removed > 0)
		pg_atomic_fetch_sub_u32(&cluster_gcs_block_dedup_shared->entry_count, (uint32)removed);
	LWLockRelease(&cluster_gcs_block_dedup_shared->lock.lock);
}

void
cluster_gcs_block_dedup_cleanup_on_node_dead(uint32 node_id)
{
	HASH_SEQ_STATUS seq;
	GcsBlockDedupEntry *entry;
	int removed = 0;

	if (cluster_gcs_block_dedup_shared == NULL || cluster_gcs_block_dedup_htab == NULL)
		return;

	LWLockAcquire(&cluster_gcs_block_dedup_shared->lock.lock, LW_EXCLUSIVE);
	hash_seq_init(&seq, cluster_gcs_block_dedup_htab);
	while ((entry = (GcsBlockDedupEntry *)hash_seq_search(&seq)) != NULL) {
		if (entry->key.origin_node_id == node_id) {
			(void)hash_search(cluster_gcs_block_dedup_htab, &entry->key, HASH_REMOVE, NULL);
			removed++;
		}
	}
	if (removed > 0)
		pg_atomic_fetch_sub_u32(&cluster_gcs_block_dedup_shared->entry_count, (uint32)removed);
	LWLockRelease(&cluster_gcs_block_dedup_shared->lock.lock);
}


/* ============================================================
 * Observability accessors.
 * ============================================================ */

uint64
cluster_gcs_block_dedup_get_hit_count(void)
{
	return cluster_gcs_block_dedup_shared
			   ? pg_atomic_read_u64(&cluster_gcs_block_dedup_shared->hit_count)
			   : 0;
}

uint64
cluster_gcs_block_dedup_get_miss_count(void)
{
	return cluster_gcs_block_dedup_shared
			   ? pg_atomic_read_u64(&cluster_gcs_block_dedup_shared->miss_count)
			   : 0;
}

uint64
cluster_gcs_block_dedup_get_collision_count(void)
{
	return cluster_gcs_block_dedup_shared
			   ? pg_atomic_read_u64(&cluster_gcs_block_dedup_shared->collision_count)
			   : 0;
}

uint64
cluster_gcs_block_dedup_get_full_count(void)
{
	return cluster_gcs_block_dedup_shared
			   ? pg_atomic_read_u64(&cluster_gcs_block_dedup_shared->full_count)
			   : 0;
}

uint64
cluster_gcs_block_dedup_get_in_flight_count(void)
{
	return cluster_gcs_block_dedup_shared
			   ? (uint64)pg_atomic_read_u32(&cluster_gcs_block_dedup_shared->entry_count)
			   : 0;
}

uint64
cluster_gcs_block_dedup_get_evict_count(void)
{
	return cluster_gcs_block_dedup_shared
			   ? pg_atomic_read_u64(&cluster_gcs_block_dedup_shared->evict_count)
			   : 0;
}

/*
 * cluster_gcs_block_dedup_get_max_entries -- effective dedup capacity.
 *
 *	spec-7.2a D5:  reports the effective entry ceiling resolved by
 *	cluster_gcs_block_dedup_effective_entries() (the GUC value, or 0 during
 *	initdb/bootstrap before a cluster node id is assigned).  The occupancy
 *	ratio entry_count / max_entries is the saturation signal behind the
 *	DEDUP_FULL fail-closed path.
 */
uint64
cluster_gcs_block_dedup_get_max_entries(void)
{
	return (uint64) cluster_gcs_block_dedup_effective_entries();
}


#endif /* USE_PGRAC_CLUSTER */
