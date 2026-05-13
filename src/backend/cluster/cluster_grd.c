/*-------------------------------------------------------------------------
 *
 * cluster_grd.c
 *	  Global Resource Directory (GRD) routing substrate — spec-2.14.
 *
 *	  Implements ClusterResId 16-byte canonical wire encoding + 4096 shard
 *	  routing via hash_bytes_extended (PG-native) + declared-node-aware
 *	  static master map + observability accessors.
 *
 *	  See cluster_grd.h for the protocol contract, scope边界, performance
 *	  hook design (Stage 6 swap point), counter invariant.
 *	  See spec-2.14-grd-resource-identity-shard-routing.md (frozen v0.4)
 *	  for design rationale.
 *
 *	  AD-002 PCM vs GES 分工 + AD-011 不移植 LC/RC Lock.
 *
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/backend/cluster/cluster_grd.c
 *
 * NOTES
 *	  This is a pgrac-original file.  Substrate routing layer only;
 *	  entry holders/waiters/hash table lands in spec-2.15;  caller-side
 *	  integration in spec-2.15+; cross-node real send in spec-2.16.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "cluster/cluster_conf.h"
#include "cluster/cluster_grd.h"
#include "cluster/cluster_guc.h" /* cluster_node_id, cluster_grd_max_entries */
#include "cluster/cluster_shmem.h"
#include "common/hashfn.h" /* hash_bytes_extended (spec-2.29 同款) */
#include "miscadmin.h"
#include "port/atomics.h"
#include "storage/lock.h"
#include "storage/lwlock.h"
#include "storage/shmem.h"
#include "storage/spin.h"
#include "utils/elog.h"
#include "utils/hsearch.h"


/* ============================================================
 * Shmem region state.
 * ============================================================ */

static ClusterGrdShared *cluster_grd_state = NULL;

/* spec-2.15 v0.3 P1.3:  Per-shard LWLock array (named tranche).  4096
 * LWLock managed by PG lwlock.c — cluster_grd_shmem_init only obtains
 * the array pointer; PG auto-initializes the lock objects. */
static LWLockPadded *cluster_grd_shard_locks = NULL;

/* spec-2.15:  HTAB for entry storage.  NULL when cluster.grd_max_entries
 * = 0 (skeleton mode → NOT_READY sentinel) or non-cluster builds. */
static HTAB *cluster_grd_entry_htab = NULL;

/* spec-2.15 v0.4 P1.1:  HTAB init size = Max(GUC, PGRAC_GRD_SHARD_COUNT)
 * — HASH_PARTITION=4096 forces dynahash nbuckets >= 4096 (nbuckets =
 * max(next_pow2(n), num_partitions)).  naive max_size=GUC=16 would let
 * ShmemInitHash severely under-estimate size → init FATAL.  Use
 * hash_estimate_size(hash_init_max_size, sizeof(ClusterGrdEntry)) for
 * real reservation.  Cached at shmem_size first-call for diagnostic
 * consistency (size_fn must stay pure per I15).
 */
static Size cluster_grd_entries_alloc_bytes = 0;


/* ============================================================
 * spec-2.15:  Private file-static entry struct body (P1.1 opaque body —
 *   header only declares opaque handle).  struct layout reserves
 *   holders/waiters/converts arrays for spec-2.16 mutator API;  本 spec
 *   仅初始化 zero,无 mutation 路径.
 *
 *   v0.3 scope 收紧 P1.3:  cap constants live here (private),不暴露;
 *   spec-2.16 mutator API public extern 时再 expose 或 keep private.
 * ============================================================ */

#define PGRAC_GRD_MAX_HOLDERS 16
#define PGRAC_GRD_MAX_WAITERS 16
#define PGRAC_GRD_MAX_CONVERTS 8

typedef struct ClusterGrdHolder {
	int32 node_id;
	LOCKMODE mode;
	TransactionId xid;
} ClusterGrdHolder;

typedef struct ClusterGrdWaiter {
	int32 node_id;
	LOCKMODE mode;
	TimestampTz wait_start;
} ClusterGrdWaiter;

typedef struct ClusterGrdConvert {
	int32 node_id;
	LOCKMODE current_mode;
	LOCKMODE requested_mode;
} ClusterGrdConvert;

struct ClusterGrdEntry {
	ClusterResId resid; /* hash key (16B) */
	slock_t lock;		/* entry-level spinlock (Q11 + P1.3 minor) */
	int ngranted;
	ClusterGrdHolder holders[PGRAC_GRD_MAX_HOLDERS];
	int nwaiters;
	ClusterGrdWaiter waiters[PGRAC_GRD_MAX_WAITERS];
	int nconverts;
	ClusterGrdConvert converts[PGRAC_GRD_MAX_CONVERTS];
	uint64 last_modified_scn;
	uint32 state_flags; /* 预留 spec-2.16 grant pending/DRM in-flight */
};


/* ============================================================
 * spec-2.15 v0.3 P1.1:  named tranche request hook.  Single-call
 *   contract — invoked once by cluster_request_shmem() inside the
 *   process_shmem_requests_in_progress window.  size_fn stays pure so
 *   diagnostic paths (cluster_shmem_get_total_bytes) can call it N
 *   times without triggering RequestNamedLWLockTranche (which is
 *   restricted to the request phase).
 * ============================================================ */

void
cluster_grd_request_lwlocks(void)
{
	RequestNamedLWLockTranche("ClusterGrdShard", PGRAC_GRD_SHARD_COUNT);
}


/* ============================================================
 * Shmem region lifecycle.
 *
 *   spec-2.15 v0.4 P1.1:  entry HTAB allocation gated on
 *   cluster.grd_max_entries GUC.  GUC=0 → only ClusterGrdShared
 *   allocated (skeleton mode, lookup_or_create returns NOT_READY).
 *   GUC>0 → hash_init_max_size = Max(GUC, PGRAC_GRD_SHARD_COUNT) and
 *   ShmemInitHash uses that size; grd_allocated_bytes reflects the
 *   hash_estimate_size() pre-computation.
 * ============================================================ */

static Size
grd_entries_init_max_size(void)
{
	/* v0.4 P1.1:  HASH_PARTITION=4096 forces nbuckets >= 4096; raise
	 * the dynahash init max_size to match so the ShmemInitHash
	 * reservation is realistic. */
	if (cluster_grd_max_entries <= 0)
		return 0;
	return Max((Size)cluster_grd_max_entries, (Size)PGRAC_GRD_SHARD_COUNT);
}

static Size
grd_entries_estimate_bytes(void)
{
	Size init_max_size = grd_entries_init_max_size();

	if (init_max_size == 0)
		return 0;
	return hash_estimate_size(init_max_size, sizeof(ClusterGrdEntry));
}

Size
cluster_grd_shmem_size(void)
{
	/* size_fn MUST stay pure (idempotent) per I15 — cluster_shmem_get_
	 * total_bytes() calls this N times for diagnostics.  No side effect
	 * (no RequestNamedLWLockTranche, no global state mutation). */
	return add_size(sizeof(ClusterGrdShared), grd_entries_estimate_bytes());
}

void
cluster_grd_shmem_init(void)
{
	bool found;
	Size entry_alloc = grd_entries_estimate_bytes();

	cluster_grd_state = ShmemInitStruct("pgrac cluster grd", sizeof(ClusterGrdShared), &found);
	if (!found) {
		int i;

		/* spec-2.14 D3 init zero (Q9 all-atomic, no LWLock). */
		for (i = 0; i < PGRAC_GRD_SHARD_COUNT; i++)
			pg_atomic_init_u32(&cluster_grd_state->master[i], 0);
		pg_atomic_init_u32(&cluster_grd_state->master_map_initialized, 0);
		pg_atomic_init_u64(&cluster_grd_state->resid_encode_count, 0);
		pg_atomic_init_u64(&cluster_grd_state->shard_lookup_count, 0);
		pg_atomic_init_u64(&cluster_grd_state->local_master_lookup_count, 0);
		pg_atomic_init_u64(&cluster_grd_state->remote_master_lookup_count, 0);
		pg_atomic_init_u64(&cluster_grd_state->master_map_refresh_count, 0);

		/* spec-2.15 v0.3 NEW counters.  entry_current_count is the
		 * current-size source for cap checks and grd_entry_count; the
		 * three lifetime counters are exposed as pg_cluster_state rows. */
		pg_atomic_init_u64(&cluster_grd_state->entry_current_count, 0);
		pg_atomic_init_u64(&cluster_grd_state->entry_create_count, 0);
		pg_atomic_init_u64(&cluster_grd_state->entry_lookup_hit_count, 0);
		pg_atomic_init_u64(&cluster_grd_state->entry_full_count, 0);

		/* spec-2.16 D1:  4 cap counter + 5 nofail counter
		 * (skeleton-init;  mutator + nofail path 真激活在 Step 2-4). */
		pg_atomic_init_u64(&cluster_grd_state->holders_full_count, 0);
		pg_atomic_init_u64(&cluster_grd_state->waiters_full_count, 0);
		pg_atomic_init_u64(&cluster_grd_state->converts_full_count, 0);
		pg_atomic_init_u64(&cluster_grd_state->ngranted_promoted_count, 0);

		pg_atomic_init_u64(&cluster_grd_state->ges_work_queue_full_count, 0);
		pg_atomic_init_u64(&cluster_grd_state->ges_cleanup_deferred_count, 0);
		pg_atomic_init_u64(&cluster_grd_state->ges_inbound_validation_fail_count, 0);
		pg_atomic_init_u64(&cluster_grd_state->ges_reply_deferred_count, 0);
		pg_atomic_init_u64(&cluster_grd_state->ges_reply_dropped_count, 0);
	}

	/* spec-2.15 v0.4 P1.1:  entry HTAB allocation gated on GUC.  GUC=0
	 * → htab stays NULL → lookup_or_create returns NOT_READY → entire
	 * shard partition LWLock path also unused → skip GetNamedLWLockTranche
	 * lookup entirely.  Bootstrap mode (initdb --boot) runs cluster_init_
	 * shmem without process_shmem_requests / cluster_grd_request_lwlocks,
	 * so the tranche is not registered there;  gating here keeps the
	 * skeleton-mode path FATAL-free. */
	cluster_grd_shard_locks = NULL;

	if (entry_alloc > 0) {
		HASHCTL info;
		Size init_max_size = grd_entries_init_max_size();

		/* spec-2.15 v0.3 P1.3 + I15:  obtain the named tranche array
		 * pointer (PG lwlock.c auto-initialized the 4096 LWLock;
		 * DO NOT call LWLockInitialize manually per I4 + I15).  Only
		 * reachable when cluster_grd_request_lwlocks() has run, i.e.
		 * full postmaster init under cluster.grd_max_entries > 0. */
		cluster_grd_shard_locks = GetNamedLWLockTranche("ClusterGrdShard");

		memset(&info, 0, sizeof(info));
		info.keysize = sizeof(ClusterResId);
		info.entrysize = sizeof(ClusterGrdEntry);
		info.num_partitions = PGRAC_GRD_SHARD_COUNT;
		/* spec-2.15 v0.4 P1.1 I13:  HASHCTL.hash NOT set — single hash
		 * source 走 hash_search_with_hash_value(hashvalue) with
		 * cluster_grd_hash_resource() 32-bit projection.  Leaving
		 * info.hash NULL means dynahash uses tag_hash by default for
		 * HASH_BLOBS — but we always call hash_search_with_hash_value
		 * so the default never fires; defensive choice. */

		cluster_grd_entry_htab
			= ShmemInitHash("pgrac cluster grd entries", init_max_size, init_max_size, &info,
							HASH_ELEM | HASH_BLOBS | HASH_PARTITION);
		cluster_grd_entries_alloc_bytes = entry_alloc;
	} else {
		cluster_grd_entry_htab = NULL;
		cluster_grd_entries_alloc_bytes = 0;
	}
}

static const ClusterShmemRegion cluster_grd_region = {
	.name = "pgrac cluster grd",
	.size_fn = cluster_grd_shmem_size,
	.init_fn = cluster_grd_shmem_init,
	.lwlock_count = 0, /* spec-2.14 Q9: lock-free (L106 inherit) */
	.owner_subsys = "cluster_grd",
	.reserved_flags = 0,
};

void
cluster_grd_shmem_register(void)
{
	cluster_shmem_register_region(&cluster_grd_region);
}


/* ============================================================
 * Wire encoding / decoding (D2; Q4 user-correction).
 *
 *	Explicit field-by-field encode/decode — NOT memcpy(LOCKTAG).
 *	Wire ABI boundary;  LOCKTAG internal layout 与 ClusterResId 16B
 *	wire ABI 解耦.
 *
 *	v0.4 P1.1:  cluster_grd_resid_encode() must fetch_add resid_encode_count
 *	each call (observability;  was missing in v0.2/v0.3 spec body).
 * ============================================================ */

void
cluster_grd_resid_encode(const LOCKTAG *src, ClusterResId *dst)
{
	Assert(src != NULL);
	Assert(dst != NULL);

	if (src == NULL || dst == NULL)
		return;

	dst->field1 = src->locktag_field1;
	dst->field2 = src->locktag_field2;
	dst->field3 = src->locktag_field3;
	dst->field4 = src->locktag_field4;
	dst->type = src->locktag_type;
	dst->lockmethodid = src->locktag_lockmethodid;

	/* v0.4 P1.1:  increment observability counter on every encode. */
	if (cluster_grd_state != NULL)
		pg_atomic_fetch_add_u64(&cluster_grd_state->resid_encode_count, 1);
}

void
cluster_grd_resid_decode(const ClusterResId *src, LOCKTAG *dst)
{
	Assert(src != NULL);
	Assert(dst != NULL);

	if (src == NULL || dst == NULL)
		return;

	dst->locktag_field1 = src->field1;
	dst->locktag_field2 = src->field2;
	dst->locktag_field3 = src->field3;
	dst->locktag_field4 = src->field4;
	dst->locktag_type = src->type;
	dst->locktag_lockmethodid = src->lockmethodid;
}


/* ============================================================
 * Cluster-aware lock type classifier (Q5 user-correction).
 *
 *	pgrac mapping function — NOT new LockTagType enum.  Returns true
 *	for the 4 cluster-coordinated lock classes; false for all others
 *	(PG-native lock manager handles them locally).
 * ============================================================ */

bool
cluster_grd_is_cluster_aware(const LOCKTAG *tag)
{
	Assert(tag != NULL);

	if (tag == NULL)
		return false;

	switch ((LockTagType)tag->locktag_type) {
	case LOCKTAG_RELATION:	  /* TM 表锁跨节点 (feature-024) */
	case LOCKTAG_TRANSACTION: /* TX 行锁等待图 (feature-023) */
	case LOCKTAG_OBJECT:	  /* catalog object 跨节点 */
	case LOCKTAG_ADVISORY:	  /* user lock 跨节点 (feature-078) */
		return true;
	default:
		return false; /* PAGE / TUPLE / RELATION_EXTEND /
									 * VIRTUALTRANSACTION / etc 本地 only */
	}
}


/* ============================================================
 * Performance hook API split (P1.2 v0.2; Stage 6 swap point).
 *
 *	cluster_grd_hash_resource is the ONLY function whose body Stage 6
 *	replaces (xxhash3 / RDMA-aware locality hash).  Hash input is
 *	14 bytes (P1.1 v0.2): field1-3 + type + lockmethodid;  skip ONLY
 *	field4 (tuple offset, co-locates same-page tuples in spec-2.16
 *	batched routing).
 *
 *	Counter invariant (v0.4 P1.2):
 *	  shard_lookup_count >= local_master_lookup_count +
 *	                        remote_master_lookup_count
 *	  shard_lookup() thin wrapper increments total only;
 *	  lookup_master() increments total + local-or-remote.
 * ============================================================ */

uint64
cluster_grd_hash_resource(const ClusterResId *resid)
{
	uint8 hash_input[14];

	Assert(resid != NULL);

	if (resid == NULL)
		return 0;

	/* Pack 14B input: field1-3 + type + lockmethodid.  Skip ONLY field4. */
	memcpy(&hash_input[0], &resid->field1, 4);
	memcpy(&hash_input[4], &resid->field2, 4);
	memcpy(&hash_input[8], &resid->field3, 4);
	hash_input[12] = resid->type;
	hash_input[13] = resid->lockmethodid; /* v0.2 P1.1: identity 必含 */

	return hash_bytes_extended(hash_input, sizeof(hash_input), 0);
}

uint32
cluster_grd_shard_for_hash(uint64 hash)
{
	return (uint32)(hash % PGRAC_GRD_SHARD_COUNT);
}

uint32
cluster_grd_shard_for_resource(const ClusterResId *resid)
{
	/* compose hash_resource + shard_for_hash;  no counter (pure). */
	return cluster_grd_shard_for_hash(cluster_grd_hash_resource(resid));
}

int32
cluster_grd_lookup_master(const ClusterResId *resid)
{
	uint32 shard_id;
	int32 master;

	Assert(cluster_grd_state != NULL);

	shard_id = cluster_grd_shard_for_resource(resid);
	pg_atomic_fetch_add_u64(&cluster_grd_state->shard_lookup_count, 1);

	if (pg_atomic_read_u32(&cluster_grd_state->master_map_initialized) == 0)
		return -1;

	master = (int32)pg_atomic_read_u32(&cluster_grd_state->master[shard_id]);
	if (master == cluster_node_id)
		pg_atomic_fetch_add_u64(&cluster_grd_state->local_master_lookup_count, 1);
	else
		pg_atomic_fetch_add_u64(&cluster_grd_state->remote_master_lookup_count, 1);

	return master;
}

uint32
cluster_grd_shard_lookup(const ClusterResId *resid)
{
	uint32 shard_id;

	Assert(cluster_grd_state != NULL);

	shard_id = cluster_grd_shard_for_resource(resid);
	/* Thin compat wrapper:  total counter only;  does NOT read master
	 * so local/remote counters NOT incremented (Counter invariant
	 * v0.4 P1.2:  shard_lookup_count >= local + remote). */
	pg_atomic_fetch_add_u64(&cluster_grd_state->shard_lookup_count, 1);
	return shard_id;
}


/* ============================================================
 * Master mapping (Q10 + Q11; declared-node-aware).
 *
 *	v0.4 P2.1 修正:  use existing cluster_conf_lookup_node() scan +
 *	cluster_conf_node_count() cross-check (NOT cluster_conf_get_declared_nodes
 *	which does NOT exist;  规则 23 linkdb SSOT).
 * ============================================================ */

int32
cluster_grd_shard_master(uint32 shard_id)
{
	Assert(cluster_grd_state != NULL);
	if (shard_id >= PGRAC_GRD_SHARD_COUNT)
		return -1;
	if (pg_atomic_read_u32(&cluster_grd_state->master_map_initialized) == 0)
		return -1;
	return (int32)pg_atomic_read_u32(&cluster_grd_state->master[shard_id]);
}

bool
cluster_grd_is_local_master(uint32 shard_id)
{
	int32 master = cluster_grd_shard_master(shard_id);

	return master >= 0 && master == cluster_node_id;
}

void
cluster_grd_master_map_init(void)
{
	int32 declared[CLUSTER_MAX_NODES];
	int declared_count = 0;
	int i;

	Assert(cluster_grd_state != NULL);

	/* Q10 + P2.1:  collect declared node_ids in scan order (= 升序)
	 * via existing cluster_conf_lookup_node().  Sparse node_id
	 * (e.g. pgrac.conf declares 0/2/5) yields declared = [0, 2, 5]. */
	for (i = 0; i < CLUSTER_MAX_NODES; i++) {
		if (cluster_conf_lookup_node(i) != NULL)
			declared[declared_count++] = i;
	}
	if (declared_count <= 0) {
		ereport(FATAL, (errcode(ERRCODE_CONFIG_FILE_ERROR),
						errmsg("cluster_grd: no declared nodes in pgrac.conf"),
						errhint("Declare at least one [node.N] entry in pgrac.conf "
								"before initializing the GRD master map.")));
		return;
	}
	Assert(declared_count == cluster_conf_node_count());

	/* Distribute 4096 shards over declared nodes (round-robin in
	 * declared-list order, NOT modulo node_id directly). */
	for (i = 0; i < PGRAC_GRD_SHARD_COUNT; i++) {
		int idx = i % declared_count;

		pg_atomic_write_u32(&cluster_grd_state->master[i], (uint32)declared[idx]);
	}
	pg_atomic_write_u32(&cluster_grd_state->master_map_initialized, 1);
	pg_atomic_fetch_add_u64(&cluster_grd_state->master_map_refresh_count, 1);
}

void
cluster_grd_master_map_refresh(void)
{
	/* Stage 6 DRM placeholder — real implementation will be LMON-mediated
	 * full master[] refresh + epoch field check.  Body for now is a no-op
	 * except for the observability counter increment so future spec-2.X
	 * test可以 observe call site exists. */
	if (cluster_grd_state != NULL)
		pg_atomic_fetch_add_u64(&cluster_grd_state->master_map_refresh_count, 1);
}


/* ============================================================
 * Observability accessors (D6 dump_grd consumers; 7 accessors).
 *
 *	v0.4 P1.1 修正:  补 shard_lookup_count + resid_encode_count
 *	accessor (v0.2/v0.3 shmem 有 field 但漏 accessor extern).
 * ============================================================ */

uint32
cluster_grd_local_master_count(void)
{
	uint32 count = 0;
	int i;

	Assert(cluster_grd_state != NULL);
	for (i = 0; i < PGRAC_GRD_SHARD_COUNT; i++) {
		if ((int32)pg_atomic_read_u32(&cluster_grd_state->master[i]) == cluster_node_id)
			count++;
	}
	return count;
}

uint32
cluster_grd_remote_master_count(void)
{
	Assert(cluster_grd_state != NULL);
	return PGRAC_GRD_SHARD_COUNT - cluster_grd_local_master_count();
}

uint64
cluster_grd_shard_lookup_count(void)
{
	Assert(cluster_grd_state != NULL);
	return pg_atomic_read_u64(&cluster_grd_state->shard_lookup_count);
}

uint64
cluster_grd_local_master_lookup_count(void)
{
	Assert(cluster_grd_state != NULL);
	return pg_atomic_read_u64(&cluster_grd_state->local_master_lookup_count);
}

uint64
cluster_grd_remote_master_lookup_count(void)
{
	Assert(cluster_grd_state != NULL);
	return pg_atomic_read_u64(&cluster_grd_state->remote_master_lookup_count);
}

uint64
cluster_grd_resid_encode_count(void)
{
	Assert(cluster_grd_state != NULL);
	return pg_atomic_read_u64(&cluster_grd_state->resid_encode_count);
}

uint64
cluster_grd_master_map_refresh_count_get(void)
{
	Assert(cluster_grd_state != NULL);
	return pg_atomic_read_u64(&cluster_grd_state->master_map_refresh_count);
}


/* cluster_get_grd_shards SRF body lives in cluster_grd_srf.c (mirror
 * spec-2.3 cluster_ic_msg_types_srf.c split pattern) so test_cluster_grd
 * standalone unit test可以 link cluster_grd.o without pulling in
 * InitMaterializedSRF / tuplestore_putvalues / etc PG runtime symbols. */


/* ============================================================
 * spec-2.15:  Entry table API — lookup/create + release.
 *
 *   I13 hash 单源:cluster_grd_hash_resource() 算 14B hash;shard_id =
 *   hash64 % 4096;HTAB bucket via hash_search_with_hash_value() with
 *   32-bit projection of same hash64.  绝不让 dynahash 自己 hash key.
 *
 *   I17 double-cap check:
 *     1. HASH_FIND existing entry first; existing entries must remain
 *        reusable even when the table is at soft cap.
 *     2. Soft cap reads entry_current_count atomically and applies only
 *        to new entries.
 *     3. HASH_ENTER_NULL → NULL remains the hard-cap/OOM defensive path.
 * ============================================================ */

ClusterGrdEntryResult
cluster_grd_entry_lookup_or_create(const ClusterResId *resid, bool create, ClusterGrdEntry **out)
{
	uint64 hash64;
	uint32 shard_id;
	uint32 hashvalue;
	bool found;
	ClusterGrdEntry *entry;

	Assert(resid != NULL);
	Assert(out != NULL);
	if (resid == NULL || out == NULL)
		return CLUSTER_GRD_ENTRY_ERROR;

	*out = NULL;

	/* Step 1: skeleton-mode fast path (cluster.grd_max_entries=0 → htab
	 * NULL → NOT_READY).  caller 必处理(spec-2.16 caller-side 真激活前
	 * 固定走此路径). */
	if (cluster_grd_entry_htab == NULL)
		return CLUSTER_GRD_ENTRY_NOT_READY;

	/* Step 2: I13 single hash source — shard_id 与 HTAB bucket 必同源.
	 * cluster_grd_hash_resource() returns 14B hash (skip field4); use
	 * % 4096 for shard_id and 32-bit projection for HTAB hashvalue. */
	hash64 = cluster_grd_hash_resource(resid);
	shard_id = (uint32)(hash64 % PGRAC_GRD_SHARD_COUNT);
	hashvalue = (uint32)hash64;

	/* Step 3: shard partition LWLock acquire (I5 + I6 — shard partition
	 * LWLock 必先于 entry slock_t). */
	LWLockAcquire(&cluster_grd_shard_locks[shard_id].lock, LW_EXCLUSIVE);

	/* Step 4: always look for an existing entry before any cap decision.
	 * Otherwise a table at soft cap would reject reusing an already-created
	 * resource and return FULL incorrectly. */
	entry
		= hash_search_with_hash_value(cluster_grd_entry_htab, resid, hashvalue, HASH_FIND, &found);
	if (entry != NULL) {
		pg_atomic_fetch_add_u64(&cluster_grd_state->entry_lookup_hit_count, 1);
		LWLockRelease(&cluster_grd_shard_locks[shard_id].lock);
		*out = entry;
		return CLUSTER_GRD_ENTRY_OK;
	}

	if (!create) {
		LWLockRelease(&cluster_grd_shard_locks[shard_id].lock);
		return CLUSTER_GRD_ENTRY_NOT_FOUND;
	}

	/* Step 5: new-entry soft cap.  Use our own atomic current count rather
	 * than hash_get_num_entries(); future remove will decrement this counter
	 * in cluster_grd_entry_release while holding the proper partition lock. */
	if (pg_atomic_read_u64(&cluster_grd_state->entry_current_count)
		>= (uint64)cluster_grd_max_entries) {
		LWLockRelease(&cluster_grd_shard_locks[shard_id].lock);
		pg_atomic_fetch_add_u64(&cluster_grd_state->entry_full_count, 1);
		ereport(LOG, (errmsg("cluster_grd: entry table soft cap reached "
							 "(cluster.grd_max_entries = %d)",
							 cluster_grd_max_entries)));
		return CLUSTER_GRD_ENTRY_FULL;
	}

	/* Step 6: HASH_ENTER_NULL only after existing lookup + soft cap.  NOT
	 * HASH_ENTER because the latter ereport(ERROR) cannot support the FULL
	 * sentinel. */
	entry = hash_search_with_hash_value(cluster_grd_entry_htab, resid, hashvalue, HASH_ENTER_NULL,
										&found);

	/* Step 7: sentinel 5 paths — FULL on HASH_ENTER_NULL OOM defensive
	 * bounce; OK otherwise. */
	if (entry == NULL) {
		LWLockRelease(&cluster_grd_shard_locks[shard_id].lock);
		/* HASH_ENTER_NULL returned NULL — shmem OOM defensive. */
		pg_atomic_fetch_add_u64(&cluster_grd_state->entry_full_count, 1);
		ereport(LOG, (errmsg("cluster_grd: HASH_ENTER_NULL returned NULL "
							 "(shmem OOM defensive bounce)")));
		return CLUSTER_GRD_ENTRY_FULL;
	}

	if (!found) {
		/* New entry — init slock + body zero. */
		SpinLockInit(&entry->lock);
		entry->ngranted = 0;
		entry->nwaiters = 0;
		entry->nconverts = 0;
		entry->last_modified_scn = 0;
		entry->state_flags = 0;
		/* holders / waiters / converts arrays left uninitialized;
		 * spec-2.16 mutator path initializes per-slot on add. */
		pg_atomic_fetch_add_u64(&cluster_grd_state->entry_current_count, 1);
		pg_atomic_fetch_add_u64(&cluster_grd_state->entry_create_count, 1);
	}
	pg_atomic_fetch_add_u64(&cluster_grd_state->entry_lookup_hit_count, 1);

	/* Step 8: release shard partition LWLock — caller holds entry handle. */
	LWLockRelease(&cluster_grd_shard_locks[shard_id].lock);

	*out = entry;
	return CLUSTER_GRD_ENTRY_OK;
}

void
cluster_grd_entry_release(ClusterGrdEntry *entry)
{
	/* spec-2.15 RESERVED no-op (v0.3 P1.3 contract unified — header doc
	 * + impl 一致).  本 spec 不保证任何 side effect:不 decrement
	 * refcount,不 remove entry,不改 holders/waiters/converts 状态.
	 *
	 * spec-2.16 caller-side 集成时真实装 logic (API signature 不变,body
	 * 加):decrement refcount + 若 ngranted == 0 && nwaiters == 0 &&
	 * nconverts == 0 → HASH_REMOVE + DRM reclaim path (Stage 6).
	 */
	(void)entry;
}


/* ============================================================
 * spec-2.15 v0.3:  6 observability accessor (P1.2 metric scope 收紧).
 *
 *   3 derived/internal (GUC value / entry_current_count / static
 *   allocated_bytes) + 3 public atomic lifetime counters
 *   (entry_create_count / entry_lookup_hit_count / entry_full_count)
 *   = 6 cleanly-observable metrics.
 *
 *   holder/waiter/convert counter 推 spec-2.16 配 mutator API.
 * ============================================================ */

int
cluster_grd_max_entries_get(void)
{
	return cluster_grd_max_entries;
}

int
cluster_grd_entry_count(void)
{
	if (cluster_grd_entry_htab == NULL)
		return 0;
	return (int)pg_atomic_read_u64(&cluster_grd_state->entry_current_count);
}

Size
cluster_grd_allocated_bytes(void)
{
	return cluster_grd_entries_alloc_bytes;
}

uint64
cluster_grd_entry_create_count(void)
{
	Assert(cluster_grd_state != NULL);
	return pg_atomic_read_u64(&cluster_grd_state->entry_create_count);
}

uint64
cluster_grd_entry_lookup_hit_count(void)
{
	Assert(cluster_grd_state != NULL);
	return pg_atomic_read_u64(&cluster_grd_state->entry_lookup_hit_count);
}

uint64
cluster_grd_entry_full_count(void)
{
	Assert(cluster_grd_state != NULL);
	return pg_atomic_read_u64(&cluster_grd_state->entry_full_count);
}


/* ============================================================
 * spec-2.15 D8:  SRF row visitor — hash_seq_search the entry HTAB
 *   and emit one row per entry under per-entry slock_t snapshot.
 *
 *   **spec-2.16 forward-link TODO (P2.4 + I14)**:
 *   Wrap hash_seq_search in full 4096-shard LW_SHARED acquire OR
 *   chunked snapshot to defend concurrent HASH_ENTER_NULL writers
 *   once caller-side LockAcquire integration lands.  本 spec 0
 *   caller → 0 row → 无并发问题 (本 walker safe).
 * ============================================================ */

void
cluster_grd_entries_walk(ClusterGrdEntryRowVisitor visitor, void *ctx)
{
	HASH_SEQ_STATUS status;
	ClusterGrdEntry *entry;

	Assert(visitor != NULL);

	/* skeleton mode (cluster.grd_max_entries=0) → 0 row.  Mirrors the
	 * NOT_READY sentinel surface for SRF callers. */
	if (cluster_grd_entry_htab == NULL)
		return;

	hash_seq_init(&status, cluster_grd_entry_htab);
	while ((entry = (ClusterGrdEntry *)hash_seq_search(&status)) != NULL) {
		int32 fields[11];

		/* per-entry slock_t snapshot — short critical section (memcpy
		 * fixed-size struct fields).  spec-2.16 mutator writers also
		 * acquire entry->lock so snapshot is consistent. */
		SpinLockAcquire(&entry->lock);
		fields[0] = (int32)(cluster_grd_hash_resource(&entry->resid) % PGRAC_GRD_SHARD_COUNT);
		fields[1] = (int32)entry->resid.field1;
		fields[2] = (int32)entry->resid.field2;
		fields[3] = (int32)entry->resid.field3;
		fields[4] = (int32)entry->resid.field4;
		fields[5] = (int32)entry->resid.type;
		fields[6] = (int32)entry->resid.lockmethodid;
		fields[7] = entry->ngranted;
		fields[8] = entry->nwaiters;
		fields[9] = entry->nconverts;
		fields[10] = (int32)entry->state_flags;
		SpinLockRelease(&entry->lock);

		visitor(ctx, fields);
	}
}


/* ============================================================
 * spec-2.16 D2:  9 counter accessor + mutator stub + should_globalize
 *   stub + LOCKMODE compat stub + cleanup stub.
 *
 *   All mutator bodies are规则 8 ERRCODE_FEATURE_NOT_SUPPORTED stubs
 *   with errhint pointing to the activating Step (Step 4 D9).
 *   Skeleton phase guarantees Step 1 ship does not break cluster_unit
 *   or PG 219 regression.
 * ============================================================ */

uint64
cluster_grd_holders_full_count(void)
{
	Assert(cluster_grd_state != NULL);
	return pg_atomic_read_u64(&cluster_grd_state->holders_full_count);
}

uint64
cluster_grd_waiters_full_count(void)
{
	Assert(cluster_grd_state != NULL);
	return pg_atomic_read_u64(&cluster_grd_state->waiters_full_count);
}

uint64
cluster_grd_converts_full_count(void)
{
	Assert(cluster_grd_state != NULL);
	return pg_atomic_read_u64(&cluster_grd_state->converts_full_count);
}

uint64
cluster_grd_ngranted_promoted_count(void)
{
	Assert(cluster_grd_state != NULL);
	return pg_atomic_read_u64(&cluster_grd_state->ngranted_promoted_count);
}

uint64
cluster_grd_ges_work_queue_full_count(void)
{
	Assert(cluster_grd_state != NULL);
	return pg_atomic_read_u64(&cluster_grd_state->ges_work_queue_full_count);
}

uint64
cluster_grd_ges_cleanup_deferred_count(void)
{
	Assert(cluster_grd_state != NULL);
	return pg_atomic_read_u64(&cluster_grd_state->ges_cleanup_deferred_count);
}

uint64
cluster_grd_ges_inbound_validation_fail_count(void)
{
	Assert(cluster_grd_state != NULL);
	return pg_atomic_read_u64(&cluster_grd_state->ges_inbound_validation_fail_count);
}

uint64
cluster_grd_ges_reply_deferred_count(void)
{
	Assert(cluster_grd_state != NULL);
	return pg_atomic_read_u64(&cluster_grd_state->ges_reply_deferred_count);
}

uint64
cluster_grd_ges_reply_dropped_count(void)
{
	Assert(cluster_grd_state != NULL);
	return pg_atomic_read_u64(&cluster_grd_state->ges_reply_dropped_count);
}


/* ============================================================
 * should_globalize — D10 skeleton.
 *
 *   Step 1:  always return false (no LOCKTAG enters cluster path).
 *   Step 4 D10:  O(1) allowlist (RELATION / TRANSACTION / OBJECT /
 *   ADVISORY classes per cluster_grd_is_cluster_aware contract).
 * ============================================================ */

bool
cluster_grd_should_globalize(const LOCKTAG *tag pg_attribute_unused())
{
	return false; /* skeleton — Step 4 D10 真激活 */
}


/* ============================================================
 * LOCKMODE compat — D9 helper (Step 1 skeleton).
 *
 *   Step 4 D9:  wires to lmgr/lock.c LockMethodConflicts (NEW
 *   exposed symbol via PGRAC MODIFICATIONS in lock.c).  Skeleton
 *   returns true conservatively (any-mode conflicts any-mode) to
 *   keep safety contract — no false GRANT before Step 4 真激活.
 * ============================================================ */

bool
cluster_grd_lockmode_conflicts(int held pg_attribute_unused(), int wanted pg_attribute_unused())
{
	return true; /* skeleton — Step 4 D9 wires real LockMethodConflicts */
}


/* ============================================================
 * Mutator stubs — Step 4 D9 真激活.
 *
 *   规则 8:  ERRCODE_FEATURE_NOT_SUPPORTED + errhint pointing to
 *   spec-2.16 Step 4.  cluster_unit tests in Step 6 必须显式 expect
 *   FEATURE_NOT_SUPPORTED until Step 4 lands.
 * ============================================================ */

ClusterGrdEntryResult
cluster_grd_entry_grant_holder(ClusterGrdEntry *entry pg_attribute_unused(),
							   const ClusterGrdHolderId *holder pg_attribute_unused(),
							   int mode pg_attribute_unused())
{
	ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					errmsg("cluster_grd_entry_grant_holder not implemented in Step 1"),
					errhint("spec-2.16 Step 4 D9 activates the 6-step state machine + mutator body")));
	return CLUSTER_GRD_ENTRY_ERROR; /* unreachable */
}

ClusterGrdEntryResult
cluster_grd_entry_release_holder(ClusterGrdEntry *entry pg_attribute_unused(),
								 const ClusterGrdHolderId *holder pg_attribute_unused())
{
	ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					errmsg("cluster_grd_entry_release_holder not implemented in Step 1"),
					errhint("spec-2.16 Step 4 D9 activates release + HASH_REMOVE")));
	return CLUSTER_GRD_ENTRY_ERROR;
}

ClusterGrdEntryResult
cluster_grd_entry_add_waiter(ClusterGrdEntry *entry pg_attribute_unused(),
							 const ClusterGrdHolderId *holder pg_attribute_unused(),
							 int mode pg_attribute_unused())
{
	ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					errmsg("cluster_grd_entry_add_waiter not implemented in Step 1"),
					errhint("spec-2.16 Step 4 D9 activates waiter + cap surface")));
	return CLUSTER_GRD_ENTRY_ERROR;
}

ClusterGrdEntryResult
cluster_grd_entry_promote_waiter(ClusterGrdEntry *entry pg_attribute_unused(),
								 const ClusterGrdHolderId *holder pg_attribute_unused())
{
	ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					errmsg("cluster_grd_entry_promote_waiter not implemented in Step 1"),
					errhint("spec-2.16 Step 4 D9 grant decision callback")));
	return CLUSTER_GRD_ENTRY_ERROR;
}


/* ============================================================
 * CSSD DEAD / stale-epoch cleanup stubs — Step 4 D11 真激活.
 * ============================================================ */

void
cluster_grd_cleanup_on_node_dead(int32 dead_node_id)
{
	/* Step 1 skeleton:  log DEBUG2 only.  Step 4 D11 sweeps holders[]
	 * + waiters[] per I48 (holder.node_id == dead_node_id, NO epoch
	 * filter; independent of stale-epoch sweep). */
	ereport(DEBUG2, (errmsg_internal("cluster_grd_cleanup_on_node_dead(%d): "
									 "skeleton no-op (spec-2.16 Step 4 D11 真激活)",
									 dead_node_id)));
}

void
cluster_grd_cleanup_stale_epoch(uint64 current_epoch)
{
	/* Step 1 skeleton:  log DEBUG2 only.  Step 4 D11 sweeps holders[]
	 * per I48 (holder.cluster_epoch < current_epoch).  Independent rule
	 * from DEAD cleanup. */
	ereport(DEBUG2, (errmsg_internal("cluster_grd_cleanup_stale_epoch(%lu): "
									 "skeleton no-op (spec-2.16 Step 4 D11 真激活)",
									 (unsigned long)current_epoch)));
}
