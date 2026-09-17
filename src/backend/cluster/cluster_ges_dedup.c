/*-------------------------------------------------------------------------
 *
 * cluster_ges_dedup.c
 *	  pgrac GES retransmit dedup HTAB — spec-2.27 D2 implementation.
 *
 *	  HTAB located in 'pgrac cluster ges dedup' shmem region; key is
 *	  the 5-tuple ClusterGesDedupKey (origin_node_id, opcode, request_id,
 *	  cluster_epoch, shard_master_generation).  Entry value is the cached
 *	  GES_REPLY blob (52B = GesReplyPayload spec-2.23) + processed timestamp
 *	  + status flag distinguishing in-flight vs cached.
 *
 *	  HC51 / HC52 invariants:  IN_FLIGHT_DUPLICATE handling (caller must
 *	  drop/defer not re-process), STALE_REPROCESS sweeping, FULL fail-
 *	  closed with no eviction of in-flight entries.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 * IDENTIFICATION
 *	  src/backend/cluster/cluster_ges_dedup.c
 *
 * NOTES
 *	  pgrac-original file.  Compiled only in --enable-cluster builds.
 *	  Spec: spec-2.27-ges-reliability-hardening.md (FROZEN v0.2).
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"
#include "cluster/cluster_clean_leave.h"

#include "cluster/cluster_ges_dedup.h"
#include "cluster/cluster_guc.h"
#include "cluster/cluster_lms.h"
#include "cluster/cluster_shmem.h"
#include "miscadmin.h"
#include "port/atomics.h"
#include "storage/lwlock.h"
#include "storage/shmem.h"
#include "utils/hsearch.h"
#include "utils/timestamp.h"

#define CLUSTER_GES_DEDUP_REPLY_BLOB_LEN 52 /* GesReplyPayload spec-2.23 */

typedef struct ClusterGesDedupEntry {
	ClusterGesDedupKey key;	  /* 32B HASH_BLOBS key */
	TimestampTz processed_ts; /* set at register time */
	uint16 cached_reply_len;  /* 0 ⇒ in-flight, no cached reply */
	uint16 status;			  /* ClusterGesDedupLookupStatus snapshot */
	uint8 cached_reply_blob[CLUSTER_GES_DEDUP_REPLY_BLOB_LEN];
} ClusterGesDedupEntry;

typedef struct ClusterGesDedupShared {
	pg_atomic_uint64 hit_cached_count;
	pg_atomic_uint64 in_flight_dup_count;
	pg_atomic_uint64 stale_reprocess_count;
	pg_atomic_uint64 full_reject_count;
	pg_atomic_uint32 entry_count; /* approximate; HTAB authoritative */
} ClusterGesDedupShared;

static ClusterGesDedupShared *cluster_ges_dedup_shared = NULL;
static HTAB *cluster_ges_dedup_htab = NULL;
static LWLock *cluster_ges_dedup_lock = NULL;

/* ============================================================
 * Shmem lifecycle.
 * ============================================================ */

Size
cluster_ges_dedup_shmem_size(void)
{
	Size sz;
	int cap;

	cap = cluster_ges_dedup_max_entries > 0 ? cluster_ges_dedup_max_entries : 8192;

	sz = MAXALIGN(sizeof(ClusterGesDedupShared));
	sz = add_size(sz, hash_estimate_size((Size)cap, sizeof(ClusterGesDedupEntry)));
	return sz;
}

void
cluster_ges_dedup_shmem_request(void)
{
	/*
	 * cluster_request_shmem() already reserves bytes for every registered
	 * region by calling region.size_fn().  Keep this hook tranche-only so
	 * diagnostic size walks cannot accidentally double-request addin shmem.
	 */
	RequestNamedLWLockTranche("ClusterGesDedup", 1);
}

void
cluster_ges_dedup_shmem_init(void)
{
	bool found;
	HASHCTL info;
	int cap;

	cluster_ges_dedup_shared = (ClusterGesDedupShared *)ShmemInitStruct(
		"pgrac cluster ges dedup", MAXALIGN(sizeof(ClusterGesDedupShared)), &found);

	if (!found) {
		memset(cluster_ges_dedup_shared, 0, sizeof(*cluster_ges_dedup_shared));
		pg_atomic_init_u64(&cluster_ges_dedup_shared->hit_cached_count, 0);
		pg_atomic_init_u64(&cluster_ges_dedup_shared->in_flight_dup_count, 0);
		pg_atomic_init_u64(&cluster_ges_dedup_shared->stale_reprocess_count, 0);
		pg_atomic_init_u64(&cluster_ges_dedup_shared->full_reject_count, 0);
		pg_atomic_init_u32(&cluster_ges_dedup_shared->entry_count, 0);
	}

	cap = cluster_ges_dedup_max_entries > 0 ? cluster_ges_dedup_max_entries : 8192;

	memset(&info, 0, sizeof(info));
	info.keysize = sizeof(ClusterGesDedupKey);
	info.entrysize = sizeof(ClusterGesDedupEntry);

	cluster_ges_dedup_htab
		= ShmemInitHash("pgrac cluster ges dedup htab", cap, cap, &info, HASH_ELEM | HASH_BLOBS);

	if (!IsBootstrapProcessingMode())
		cluster_ges_dedup_lock = &(GetNamedLWLockTranche("ClusterGesDedup"))[0].lock;
}

static const ClusterShmemRegion cluster_ges_dedup_region = {
	.name = "pgrac cluster ges dedup",
	.size_fn = cluster_ges_dedup_shmem_size,
	.init_fn = cluster_ges_dedup_shmem_init,
	.lwlock_count = 1,
	.owner_subsys = "cluster_ges_dedup",
	.reserved_flags = 0,
};

void
cluster_ges_dedup_shmem_register(void)
{
	cluster_shmem_register_region(&cluster_ges_dedup_region);
}

static void
ges_dedup_stop_held_lock(LWLock *lock, LWLockMode mode, void *arg)
{
	(void)lock;
	(void)mode;
	*((bool *)arg) = true;
}

ClusterNormalStopPollResult
cluster_ges_dedup_normal_stop_poll(const char **domain, uint64 *key, const char **reason)
{
	ClusterNormalStopPollResult result = CLUSTER_NORMAL_STOP_READY;
	HASH_SEQ_STATUS scan;
	ClusterGesDedupEntry *entry;
	const char *why = "NONE";
	uint64 request = 0;
	uint64 seen = 0;
	bool held_lock = false;

	if (domain != NULL)
		*domain = "GES_DEDUP";
	if (key != NULL)
		*key = 0;
	if (!IsUnderPostmaster || MyBackendType != B_LMON)
		why = "GES_DEDUP_OBSERVER_ROLE";
	else if (cluster_ges_dedup_shared == NULL || cluster_ges_dedup_htab == NULL
			 || cluster_ges_dedup_lock == NULL)
		why = "GES_DEDUP_UNINITIALIZED";
	else {
		ForEachLWLockHeldByMe(ges_dedup_stop_held_lock, &held_lock);
		if (held_lock)
			why = "GES_DEDUP_CALLER_LOCK";
	}
	if (strcmp(why, "NONE") != 0) {
		if (reason != NULL)
			*reason = why;
		return CLUSTER_NORMAL_STOP_INVALID;
	}

	/* The approximate count is not proof. Scan the original table without
	 * sweeping or releasing anything. Cached reply bytes are history; the
	 * actual GRD holder, native probe and outbound each have separate owners. */
	LWLockAcquire(cluster_ges_dedup_lock, LW_SHARED);
	hash_seq_init(&scan, cluster_ges_dedup_htab);
	while ((entry = hash_seq_search(&scan)) != NULL) {
		ClusterNormalStopPollResult one = CLUSTER_NORMAL_STOP_READY;
		const char *cause = "NONE";

		seen++;
		if (seen > cluster_ges_dedup_capacity()
			|| entry->cached_reply_len > CLUSTER_GES_DEDUP_REPLY_BLOB_LEN
			|| (entry->cached_reply_len == 0 ? entry->status != CLUSTER_GES_DEDUP_MISS_REGISTERED
											 : entry->status != CLUSTER_GES_DEDUP_CACHED_REPLY)) {
			one = CLUSTER_NORMAL_STOP_INVALID;
			cause = "GES_DEDUP_REPLY_SHAPE";
		} else if (entry->cached_reply_len == 0) {
			one = CLUSTER_NORMAL_STOP_PENDING;
			cause = "GES_DEDUP_IN_FLIGHT";
		}
		if (one < result) {
			result = one;
			why = cause;
			request = entry->key.request_id;
		}
	}
	LWLockRelease(cluster_ges_dedup_lock);
	if (key != NULL)
		*key = request;
	if (reason != NULL)
		*reason = why;
	return result;
}

/* ============================================================
 * Public API.
 * ============================================================ */

uint32
cluster_ges_dedup_capacity(void)
{
	return (uint32)(cluster_ges_dedup_max_entries > 0 ? cluster_ges_dedup_max_entries : 8192);
}

uint32
cluster_ges_dedup_entry_count(void)
{
	if (cluster_ges_dedup_shared == NULL)
		return 0;
	return pg_atomic_read_u32(&cluster_ges_dedup_shared->entry_count);
}

uint64
cluster_ges_dedup_hit_cached_count(void)
{
	if (cluster_ges_dedup_shared == NULL)
		return 0;
	return pg_atomic_read_u64(&cluster_ges_dedup_shared->hit_cached_count);
}

uint64
cluster_ges_dedup_in_flight_dup_count(void)
{
	if (cluster_ges_dedup_shared == NULL)
		return 0;
	return pg_atomic_read_u64(&cluster_ges_dedup_shared->in_flight_dup_count);
}

uint64
cluster_ges_dedup_stale_reprocess_count(void)
{
	if (cluster_ges_dedup_shared == NULL)
		return 0;
	return pg_atomic_read_u64(&cluster_ges_dedup_shared->stale_reprocess_count);
}

uint64
cluster_ges_dedup_full_reject_count(void)
{
	if (cluster_ges_dedup_shared == NULL)
		return 0;
	return pg_atomic_read_u64(&cluster_ges_dedup_shared->full_reject_count);
}

ClusterGesDedupLookupStatus
cluster_ges_dedup_lookup_or_register(const ClusterGesDedupKey *key, uint8 *reply_out,
									 uint16 reply_buf_len, uint16 *reply_len_out)
{
	ClusterGesDedupEntry *entry;
	bool found;
	ClusterGesDedupLookupStatus result;

	Assert(key != NULL);
	Assert(reply_len_out != NULL);
	if (cluster_ges_dedup_htab == NULL || cluster_ges_dedup_lock == NULL)
		return CLUSTER_GES_DEDUP_FULL; /* not ready → fail closed */

	*reply_len_out = 0;

	/* HC51 invalidation model:
	 *
	 *  Entries are keyed on the caller's shard_master_generation (part of
	 *  the 5-tuple ClusterGesDedupKey).  Stale entries from a prior LMS
	 *  generation are invalidated *exclusively* via
	 *  cluster_ges_dedup_drop_stale_entries() which LMS runs at restart
	 *  after bumping lms_restart_generation.  An inline stale check here
	 *  would create an infinite drop-and-reregister loop because the
	 *  caller would re-insert under the same (stale) caller-gen key.  The
	 *  STALE_REPROCESS status is reserved for the sweep path. */

	LWLockAcquire(cluster_ges_dedup_lock, LW_EXCLUSIVE);

	entry = (ClusterGesDedupEntry *)hash_search(cluster_ges_dedup_htab, key, HASH_FIND, &found);

	if (found && entry != NULL) {
		if (entry->cached_reply_len == 0) {
			/* HC52 IN_FLIGHT_DUPLICATE — caller MUST drop / defer, never
			 * re-process (double-grant risk). */
			LWLockRelease(cluster_ges_dedup_lock);
			pg_atomic_fetch_add_u64(&cluster_ges_dedup_shared->in_flight_dup_count, 1);
			return CLUSTER_GES_DEDUP_IN_FLIGHT_DUPLICATE;
		} else {
			uint16 to_copy = entry->cached_reply_len;
			if (to_copy > reply_buf_len)
				to_copy = reply_buf_len;
			if (reply_out != NULL && to_copy > 0)
				memcpy(reply_out, entry->cached_reply_blob, to_copy);
			*reply_len_out = to_copy;
			LWLockRelease(cluster_ges_dedup_lock);
			pg_atomic_fetch_add_u64(&cluster_ges_dedup_shared->hit_cached_count, 1);
			return CLUSTER_GES_DEDUP_CACHED_REPLY;
		}
	}

	/* MISS path:  register a fresh in-flight entry. */
	if (!cluster_normal_stop_service_new_work(false)) {
		LWLockRelease(cluster_ges_dedup_lock);
		return CLUSTER_GES_DEDUP_FULL;
	}
	{
		uint32 count = pg_atomic_read_u32(&cluster_ges_dedup_shared->entry_count);
		uint32 cap = cluster_ges_dedup_capacity();

		if (count >= cap) {
			LWLockRelease(cluster_ges_dedup_lock);
			pg_atomic_fetch_add_u64(&cluster_ges_dedup_shared->full_reject_count, 1);
			return CLUSTER_GES_DEDUP_FULL;
		}

		entry = (ClusterGesDedupEntry *)hash_search(cluster_ges_dedup_htab, key, HASH_ENTER_NULL,
													&found);
		if (entry == NULL) {
			LWLockRelease(cluster_ges_dedup_lock);
			pg_atomic_fetch_add_u64(&cluster_ges_dedup_shared->full_reject_count, 1);
			return CLUSTER_GES_DEDUP_FULL;
		}

		entry->key = *key;
		entry->processed_ts = GetCurrentTimestamp();
		entry->cached_reply_len = 0;
		entry->status = (uint16)CLUSTER_GES_DEDUP_MISS_REGISTERED;
		memset(entry->cached_reply_blob, 0, sizeof(entry->cached_reply_blob));
		pg_atomic_fetch_add_u32(&cluster_ges_dedup_shared->entry_count, 1);
		result = CLUSTER_GES_DEDUP_MISS_REGISTERED;
	}

	LWLockRelease(cluster_ges_dedup_lock);
	return result;
}

void
cluster_ges_dedup_record_reply(const ClusterGesDedupKey *key, const uint8 *reply, uint16 reply_len)
{
	ClusterGesDedupEntry *entry;
	bool found;

	Assert(key != NULL);
	if (cluster_ges_dedup_htab == NULL || cluster_ges_dedup_lock == NULL)
		return;
	if (reply_len > CLUSTER_GES_DEDUP_REPLY_BLOB_LEN)
		reply_len = CLUSTER_GES_DEDUP_REPLY_BLOB_LEN;

	LWLockAcquire(cluster_ges_dedup_lock, LW_EXCLUSIVE);

	entry = (ClusterGesDedupEntry *)hash_search(cluster_ges_dedup_htab, key, HASH_FIND, &found);
	if (found && entry != NULL) {
		entry->processed_ts = GetCurrentTimestamp();
		if (reply != NULL && reply_len > 0)
			memcpy(entry->cached_reply_blob, reply, reply_len);
		entry->cached_reply_len = reply_len;
		entry->status = (uint16)CLUSTER_GES_DEDUP_CACHED_REPLY;
	}

	LWLockRelease(cluster_ges_dedup_lock);
}

bool
cluster_ges_dedup_remove_completed(const ClusterGesDedupKey *key)
{
	ClusterGesDedupEntry *entry;
	bool found;
	bool removed = false;

	Assert(key != NULL);
	if (cluster_ges_dedup_htab == NULL || cluster_ges_dedup_lock == NULL)
		return false;

	LWLockAcquire(cluster_ges_dedup_lock, LW_EXCLUSIVE);
	entry = (ClusterGesDedupEntry *)hash_search(cluster_ges_dedup_htab, key, HASH_FIND, &found);
	if (found && entry != NULL && entry->cached_reply_len > 0) {
		(void)hash_search(cluster_ges_dedup_htab, key, HASH_REMOVE, &found);
		Assert(found);
		pg_atomic_fetch_sub_u32(&cluster_ges_dedup_shared->entry_count, 1);
		removed = true;
	}
	LWLockRelease(cluster_ges_dedup_lock);

	return removed;
}

uint32
cluster_ges_dedup_drop_stale_entries(void)
{
	HASH_SEQ_STATUS scan;
	ClusterGesDedupEntry *entry;
	uint32 swept = 0;
	uint64 current_master_gen;

	if (cluster_ges_dedup_htab == NULL || cluster_ges_dedup_lock == NULL)
		return 0;

	current_master_gen = cluster_lms_get_shard_master_generation();

	LWLockAcquire(cluster_ges_dedup_lock, LW_EXCLUSIVE);

	hash_seq_init(&scan, cluster_ges_dedup_htab);
	while ((entry = (ClusterGesDedupEntry *)hash_seq_search(&scan)) != NULL) {
		if (entry->key.shard_master_generation != current_master_gen) {
			(void)hash_search(cluster_ges_dedup_htab, &entry->key, HASH_REMOVE, NULL);
			pg_atomic_fetch_sub_u32(&cluster_ges_dedup_shared->entry_count, 1);
			pg_atomic_fetch_add_u64(&cluster_ges_dedup_shared->stale_reprocess_count, 1);
			swept++;
		}
	}

	LWLockRelease(cluster_ges_dedup_lock);
	return swept;
}
