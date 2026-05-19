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

typedef struct ClusterGcsBlockDedupShared
{
	LWLockPadded lock;					/* guards HTAB + entry_count */
	pg_atomic_uint64 hit_count;			/* CACHED_REPLY paths */
	pg_atomic_uint64 miss_count;		/* MISS_REGISTERED paths */
	pg_atomic_uint64 collision_count;	/* HC91 VALIDATION_FAIL */
	pg_atomic_uint64 full_count;		/* HC92 FULL */
	pg_atomic_uint32 entry_count;		/* live in-flight + completed entries */
} ClusterGcsBlockDedupShared;

static ClusterGcsBlockDedupShared *cluster_gcs_block_dedup_shared = NULL;
static HTAB *cluster_gcs_block_dedup_htab = NULL;
static bool dedup_backend_exit_hook_registered = false;


/* ============================================================
 * Shmem registry.
 * ============================================================ */

Size
cluster_gcs_block_dedup_shmem_size(void)
{
	Size		sz;
	int			cap;

	cap = cluster_gcs_block_dedup_max_entries > 0
		? cluster_gcs_block_dedup_max_entries
		: 1024;

	sz = MAXALIGN(sizeof(ClusterGcsBlockDedupShared));
	sz = add_size(sz, hash_estimate_size(cap, sizeof(GcsBlockDedupEntry)));
	return sz;
}

void
cluster_gcs_block_dedup_shmem_init(void)
{
	bool		found;
	HASHCTL		info;
	int			cap;

	cluster_gcs_block_dedup_shared = (ClusterGcsBlockDedupShared *)
		ShmemInitStruct("pgrac cluster gcs block dedup",
						MAXALIGN(sizeof(ClusterGcsBlockDedupShared)), &found);

	if (!found)
	{
		memset(cluster_gcs_block_dedup_shared, 0,
			   sizeof(*cluster_gcs_block_dedup_shared));
		LWLockInitialize(&cluster_gcs_block_dedup_shared->lock.lock,
						 LWTRANCHE_CLUSTER_GCS_BLOCK_DEDUP);
		pg_atomic_init_u64(&cluster_gcs_block_dedup_shared->hit_count, 0);
		pg_atomic_init_u64(&cluster_gcs_block_dedup_shared->miss_count, 0);
		pg_atomic_init_u64(&cluster_gcs_block_dedup_shared->collision_count, 0);
		pg_atomic_init_u64(&cluster_gcs_block_dedup_shared->full_count, 0);
		pg_atomic_init_u32(&cluster_gcs_block_dedup_shared->entry_count, 0);
	}

	cap = cluster_gcs_block_dedup_max_entries > 0
		? cluster_gcs_block_dedup_max_entries
		: 1024;

	memset(&info, 0, sizeof(info));
	info.keysize = sizeof(GcsBlockDedupKey);
	info.entrysize = sizeof(GcsBlockDedupEntry);

	cluster_gcs_block_dedup_htab
		= ShmemInitHash("pgrac cluster gcs block dedup htab",
						cap, cap, &info, HASH_ELEM | HASH_BLOBS);
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
 * Backend-exit hook registration (idempotent;  called by the GCS_BLOCK
 * master-side handler once per backend on first dedup interaction).
 * ============================================================ */

static void
dedup_backend_exit_callback(int code pg_attribute_unused(),
							Datum arg pg_attribute_unused())
{
	if (cluster_node_id < 0 || MyBackendId <= 0)
		return;
	cluster_gcs_block_dedup_cleanup_on_backend_exit((uint32) cluster_node_id,
													(int32) MyBackendId);
}

static void
dedup_ensure_backend_exit_hook(void)
{
	if (dedup_backend_exit_hook_registered)
		return;
	if (!IsUnderPostmaster)
		return;					/* only meaningful in backends */
	before_shmem_exit(dedup_backend_exit_callback, (Datum) 0);
	dedup_backend_exit_hook_registered = true;
}


/* ============================================================
 * Public API.
 * ============================================================ */

GcsBlockDedupResult
cluster_gcs_block_dedup_lookup_or_register(const GcsBlockDedupKey *key,
										   BufferTag tag, uint8 transition_id,
										   GcsBlockDedupEntry **out_entry)
{
	GcsBlockDedupEntry *entry;
	bool		found;
	GcsBlockDedupResult result;

	Assert(key != NULL);
	if (out_entry != NULL)
		*out_entry = NULL;

	if (cluster_gcs_block_dedup_shared == NULL || cluster_gcs_block_dedup_htab == NULL)
		return GCS_BLOCK_DEDUP_FULL;	/* not initialized; fail closed */

	dedup_ensure_backend_exit_hook();

	LWLockAcquire(&cluster_gcs_block_dedup_shared->lock.lock, LW_EXCLUSIVE);

	entry = (GcsBlockDedupEntry *) hash_search(cluster_gcs_block_dedup_htab,
											   key, HASH_FIND, &found);

	if (found)
	{
		/* HC91 — entry value collision check */
		if (memcmp(&entry->tag, &tag, sizeof(BufferTag)) != 0
			|| entry->transition_id != transition_id)
		{
			pg_atomic_fetch_add_u64(&cluster_gcs_block_dedup_shared->collision_count, 1);
			LWLockRelease(&cluster_gcs_block_dedup_shared->lock.lock);
			return GCS_BLOCK_DEDUP_VALIDATION_FAIL;
		}

		if (entry->completed_at_ts == 0)
		{
			LWLockRelease(&cluster_gcs_block_dedup_shared->lock.lock);
			return GCS_BLOCK_DEDUP_IN_FLIGHT_DUPLICATE;
		}

		pg_atomic_fetch_add_u64(&cluster_gcs_block_dedup_shared->hit_count, 1);
		if (out_entry != NULL)
			*out_entry = entry;
		LWLockRelease(&cluster_gcs_block_dedup_shared->lock.lock);
		return GCS_BLOCK_DEDUP_CACHED_REPLY;
	}

	/* MISS path — insert new in-flight slot.  HASH_ENTER_NULL → may fail
	 * with cap reached; convert to FULL fail-closed (HC92). */
	entry = (GcsBlockDedupEntry *) hash_search(cluster_gcs_block_dedup_htab,
											   key, HASH_ENTER_NULL, &found);
	if (entry == NULL)
	{
		pg_atomic_fetch_add_u64(&cluster_gcs_block_dedup_shared->full_count, 1);
		LWLockRelease(&cluster_gcs_block_dedup_shared->lock.lock);
		return GCS_BLOCK_DEDUP_FULL;
	}

	/* Reset entry to clean in-flight state. */
	memset(((char *) entry) + sizeof(GcsBlockDedupKey), 0,
		   sizeof(GcsBlockDedupEntry) - sizeof(GcsBlockDedupKey));
	entry->tag = tag;
	entry->transition_id = transition_id;
	entry->status = 0;
	entry->completed_at_ts = 0;
	entry->registered_at_ts = GetCurrentTimestamp();

	pg_atomic_fetch_add_u32(&cluster_gcs_block_dedup_shared->entry_count, 1);
	pg_atomic_fetch_add_u64(&cluster_gcs_block_dedup_shared->miss_count, 1);
	if (out_entry != NULL)
		*out_entry = entry;
	result = GCS_BLOCK_DEDUP_MISS_REGISTERED;

	LWLockRelease(&cluster_gcs_block_dedup_shared->lock.lock);
	return result;
}

void
cluster_gcs_block_dedup_install_reply(const GcsBlockDedupKey *key,
									  GcsBlockReplyStatus status,
									  const GcsBlockReplyHeader *header,
									  const char *block_data)
{
	GcsBlockDedupEntry *entry;
	bool		found;

	Assert(key != NULL);
	Assert(header != NULL);

	if (cluster_gcs_block_dedup_shared == NULL || cluster_gcs_block_dedup_htab == NULL)
		return;

	LWLockAcquire(&cluster_gcs_block_dedup_shared->lock.lock, LW_EXCLUSIVE);

	entry = (GcsBlockDedupEntry *) hash_search(cluster_gcs_block_dedup_htab,
											   key, HASH_FIND, &found);
	if (!found)
	{
		/* Entry got swept between MISS_REGISTERED and install_reply
		 * (rare:  TTL sweep + reconfig race).  Drop silently — the
		 * sender will eventually time out and retry. */
		LWLockRelease(&cluster_gcs_block_dedup_shared->lock.lock);
		return;
	}

	entry->status = (uint8) status;
	entry->reply_header = *header;
	if (block_data != NULL && status == GCS_BLOCK_REPLY_GRANTED)
		memcpy(entry->block_data, block_data, GCS_BLOCK_DATA_SIZE);
	else
		memset(entry->block_data, 0, GCS_BLOCK_DATA_SIZE);
	entry->completed_at_ts = GetCurrentTimestamp();

	LWLockRelease(&cluster_gcs_block_dedup_shared->lock.lock);
}

void
cluster_gcs_block_dedup_remove(const GcsBlockDedupKey *key)
{
	bool		found;

	Assert(key != NULL);
	if (cluster_gcs_block_dedup_shared == NULL || cluster_gcs_block_dedup_htab == NULL)
		return;

	LWLockAcquire(&cluster_gcs_block_dedup_shared->lock.lock, LW_EXCLUSIVE);
	(void) hash_search(cluster_gcs_block_dedup_htab, key, HASH_REMOVE, &found);
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
	int64		initial_ms;
	int			max_retries;
	int64		total_backoff_ms;

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
	total_backoff_ms = initial_ms * ((int64) ((1u << max_retries) - 1));
	return total_backoff_ms * 1000 * 2;	/* × 2 safety margin (HC93) */
}

void
cluster_gcs_block_dedup_sweep_expired(TimestampTz now)
{
	HASH_SEQ_STATUS seq;
	GcsBlockDedupEntry *entry;
	int64		threshold_us;
	int			removed = 0;

	if (cluster_gcs_block_dedup_shared == NULL || cluster_gcs_block_dedup_htab == NULL)
		return;

	threshold_us = dedup_expiry_threshold_us();

	LWLockAcquire(&cluster_gcs_block_dedup_shared->lock.lock, LW_EXCLUSIVE);

	hash_seq_init(&seq, cluster_gcs_block_dedup_htab);
	while ((entry = (GcsBlockDedupEntry *) hash_seq_search(&seq)) != NULL)
	{
		TimestampTz anchor;
		int64		age_us;

		anchor = entry->completed_at_ts != 0
			? entry->completed_at_ts
			: entry->registered_at_ts;
		if (anchor == 0)
			continue;

		age_us = (int64) (now - anchor);
		if (age_us > threshold_us)
		{
			(void) hash_search(cluster_gcs_block_dedup_htab, &entry->key,
							   HASH_REMOVE, NULL);
			removed++;
		}
	}

	if (removed > 0)
		pg_atomic_fetch_sub_u32(&cluster_gcs_block_dedup_shared->entry_count,
								(uint32) removed);

	LWLockRelease(&cluster_gcs_block_dedup_shared->lock.lock);
}

void
cluster_gcs_block_dedup_cleanup_on_backend_exit(uint32 origin_node_id,
												int32 backend_id)
{
	HASH_SEQ_STATUS seq;
	GcsBlockDedupEntry *entry;
	int			removed = 0;

	if (cluster_gcs_block_dedup_shared == NULL || cluster_gcs_block_dedup_htab == NULL)
		return;

	LWLockAcquire(&cluster_gcs_block_dedup_shared->lock.lock, LW_EXCLUSIVE);
	hash_seq_init(&seq, cluster_gcs_block_dedup_htab);
	while ((entry = (GcsBlockDedupEntry *) hash_seq_search(&seq)) != NULL)
	{
		if (entry->key.origin_node_id == origin_node_id
			&& entry->key.requester_backend_id == backend_id)
		{
			(void) hash_search(cluster_gcs_block_dedup_htab, &entry->key,
							   HASH_REMOVE, NULL);
			removed++;
		}
	}
	if (removed > 0)
		pg_atomic_fetch_sub_u32(&cluster_gcs_block_dedup_shared->entry_count,
								(uint32) removed);
	LWLockRelease(&cluster_gcs_block_dedup_shared->lock.lock);
}

void
cluster_gcs_block_dedup_cleanup_on_node_dead(uint32 node_id)
{
	HASH_SEQ_STATUS seq;
	GcsBlockDedupEntry *entry;
	int			removed = 0;

	if (cluster_gcs_block_dedup_shared == NULL || cluster_gcs_block_dedup_htab == NULL)
		return;

	LWLockAcquire(&cluster_gcs_block_dedup_shared->lock.lock, LW_EXCLUSIVE);
	hash_seq_init(&seq, cluster_gcs_block_dedup_htab);
	while ((entry = (GcsBlockDedupEntry *) hash_seq_search(&seq)) != NULL)
	{
		if (entry->key.origin_node_id == node_id)
		{
			(void) hash_search(cluster_gcs_block_dedup_htab, &entry->key,
							   HASH_REMOVE, NULL);
			removed++;
		}
	}
	if (removed > 0)
		pg_atomic_fetch_sub_u32(&cluster_gcs_block_dedup_shared->entry_count,
								(uint32) removed);
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
		? (uint64) pg_atomic_read_u32(&cluster_gcs_block_dedup_shared->entry_count)
		: 0;
}


#endif							/* USE_PGRAC_CLUSTER */
