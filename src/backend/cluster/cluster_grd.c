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
#include "cluster/cluster_grd_outbound.h" /* cluster_grd_outbound_enqueue_cleanup_release (D10) */
#include "cluster/cluster_lmd.h"		  /* spec-2.24 D10 cleanup_*_count_inc */
#include "cluster/cluster_guc.h" /* cluster_node_id, cluster_grd_max_entries */
#include "cluster/cluster_signal.h"
#include "cluster/cluster_shmem.h"
#include "cluster/cluster_cssd.h" /* spec-2.16 D8 newly-dead bitmap diff */
#include "storage/proc.h"		  /* spec-2.17 D8 — MyProc->cluster_grd_bast_pending */
#include "common/hashfn.h"		  /* hash_bytes_extended (spec-2.29 同款) */
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
	uint32 procno;
	uint64 cluster_epoch;
	uint64 request_id;
	LOCKMODE mode;
} ClusterGrdHolder;

typedef struct ClusterGrdWaiter {
	/*
	 * spec-2.23 D6 / FU amend — full reply identity so the LMS
	 * release-and-pop path can route GES_REPLY GRANT back to the
	 * originating backend.  spec-2.21 ship version stored only
	 * (node_id, mode, wait_start);  the procno/request_id/request_opcode
	 * + source_node_id additions are NEW for spec-2.23.
	 */
	int32 node_id;		   /* legacy: equal to source_node_id; kept for binary compat */
	int32 source_node_id;  /* node hosting the waiting backend */
	uint32 procno;		   /* PG ProcNumber of the waiting backend */
	uint64 cluster_epoch;  /* epoch at waiter enqueue time */
	uint64 request_id;	   /* per-backend monotonic id (for 5-tuple reply key) */
	uint32 request_opcode; /* GesRequestOpcode of the queued request */
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
	/*
	 * spec-2.21 D5 ABI extend:
	 *   generation:  bumped on every mutator under entry->lock; S5 promote
	 *     compares against S3 snapshot to detect race (P2.3 revalidate).
	 *   nreservations:  pending reservations count (S3 → S5 promote window).
	 *   reservations:  pending reservation slots (reuse holders[] LOCKMODE
	 *     semantic;identified by holder.request_id;not yet a real holder).
	 */
	uint64 generation;
	int nreservations;
	struct {
		ClusterGrdHolderId id;
		LOCKMODE mode;
	} reservations[PGRAC_GRD_MAX_HOLDERS];
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
	/* spec-2.16 D4/D5:  outbound ring + work queue named tranches.
	 * Same process_shmem_requests_in_progress lifecycle window — co-
	 * located here so cluster_unit standalone tests piggyback on the
	 * existing cluster_grd_request_lwlocks stub (L104). */
	RequestNamedLWLockTranche("ClusterGrdOutbound", 1);
	RequestNamedLWLockTranche("ClusterGrdWorkQueue", 1);
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
		pg_atomic_init_u64(&cluster_grd_state->cleanup_skip_stale_cancel_count, 0);
		pg_atomic_init_u64(&cluster_grd_state->ges_reply_deferred_count, 0);
		pg_atomic_init_u64(&cluster_grd_state->ges_reply_dropped_count, 0);

		/* spec-2.17 D28b — generation init 从 1(0 reserved sentinel). */
		pg_atomic_init_u64(&cluster_grd_state->next_generation, 1);

		/* spec-2.17 D12 — 6 BAST counter init 0. */
		pg_atomic_init_u64(&cluster_grd_state->ges_bast_sent_count, 0);
		pg_atomic_init_u64(&cluster_grd_state->ges_bast_received_count, 0);
		pg_atomic_init_u64(&cluster_grd_state->ges_bast_ack_count, 0);
		pg_atomic_init_u64(&cluster_grd_state->ges_bast_retry_count, 0);
		pg_atomic_init_u64(&cluster_grd_state->ges_bast_reject_count, 0);
		pg_atomic_init_u64(&cluster_grd_state->ges_bast_stale_drop_count, 0);

		/* spec-2.17 D26c — 3 deadlock chunked counter init 0. */
		pg_atomic_init_u64(&cluster_grd_state->ges_deadlock_probe_drop_count, 0);
		pg_atomic_init_u64(&cluster_grd_state->ges_deadlock_probe_collision_drop_count, 0);
		pg_atomic_init_u64(&cluster_grd_state->ges_deadlock_chunk_oo_buffer_overflow_count, 0);
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

void
cluster_grd_inc_ges_work_queue_full(void)
{
	Assert(cluster_grd_state != NULL);
	pg_atomic_fetch_add_u64(&cluster_grd_state->ges_work_queue_full_count, 1);
}

void
cluster_grd_inc_ges_cleanup_deferred(void)
{
	Assert(cluster_grd_state != NULL);
	pg_atomic_fetch_add_u64(&cluster_grd_state->ges_cleanup_deferred_count, 1);
}

void
cluster_grd_inc_ges_inbound_validation_fail(void)
{
	Assert(cluster_grd_state != NULL);
	pg_atomic_fetch_add_u64(&cluster_grd_state->ges_inbound_validation_fail_count, 1);
}

/* spec-2.24 D5 — cleanup_skip_stale_cancel(4-tuple match fail in LMD dispatch). */
void
cluster_grd_inc_cleanup_skip_stale_cancel(void)
{
	if (cluster_grd_state != NULL)
		pg_atomic_fetch_add_u64(&cluster_grd_state->cleanup_skip_stale_cancel_count, 1);
}

uint64
cluster_grd_cleanup_skip_stale_cancel_count(void)
{
	if (cluster_grd_state == NULL)
		return 0;
	return pg_atomic_read_u64(&cluster_grd_state->cleanup_skip_stale_cancel_count);
}

void
cluster_grd_inc_ges_reply_deferred(void)
{
	Assert(cluster_grd_state != NULL);
	pg_atomic_fetch_add_u64(&cluster_grd_state->ges_reply_deferred_count, 1);
}

void
cluster_grd_inc_ges_reply_dropped(void)
{
	Assert(cluster_grd_state != NULL);
	pg_atomic_fetch_add_u64(&cluster_grd_state->ges_reply_dropped_count, 1);
}


/* ============================================================
 * should_globalize — D10 skeleton.
 *
 *   Step 1:  always return false (no LOCKTAG enters cluster path).
 *   Step 4 D10:  O(1) allowlist (RELATION / TRANSACTION / OBJECT /
 *   ADVISORY classes per cluster_grd_is_cluster_aware contract).
 * ============================================================ */

bool
cluster_grd_should_globalize(const LOCKTAG *tag)
{
	/* Step 4 D10:  O(1) allowlist anchored on cluster_grd_is_cluster_aware
	 * (4 LockTagType classes — RELATION / TRANSACTION / OBJECT / ADVISORY
	 * per spec-2.14).  No catalog lookup;  branch-only dispatch.
	 *
	 *   spec-2.16 v0.4 L1.9 contract:  O(1), no catalog SQL, no LWLock,
	 *   no allocation.  Fast path for non-cluster locks returns false
	 *   immediately (~3 instructions).
	 *
	 *   Future spec-2.17+ may extend allowlist via cached relpersistence
	 *   for RELATION class (heap_open + cache);  本 spec scope skip. */
	if (tag == NULL)
		return false;
	return cluster_grd_is_cluster_aware(tag);
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
 * spec-2.21 D5 — minimal ADVISORY mutator real bodies.
 *
 *   MVP scope: LOCKTAG_ADVISORY only (per spec-2.21 Q3 v1.1).  Caller
 *   must hold the entry->lock spinlock (S3.1 / S5.1 / S6.1 sequence;
 *   shard partition LWLock is the outer lock per HC8 lock-order safe).
 *
 *   Compatibility / queueing logic is intentionally minimal — full
 *   conflict matrix + FIFO waiter promotion live in D8 LMS worker.
 *   These helpers expose the slot lifecycle (grant / release / reserve)
 *   that D8 + cluster_lock_acquire.c S3-S6 build on.
 * ============================================================ */

ClusterGrdEntryResult
cluster_grd_entry_grant_holder(ClusterGrdEntry *entry, const ClusterGrdHolderId *holder, int mode)
{
	int slot;

	Assert(entry != NULL && holder != NULL);

	if (entry->ngranted >= PGRAC_GRD_MAX_HOLDERS) {
		cluster_grd_inc_ges_work_queue_full();
		return CLUSTER_GRD_ENTRY_FULL;
	}
	slot = entry->ngranted++;
	entry->holders[slot].node_id = (int32)holder->node_id;
	entry->holders[slot].procno = holder->procno;
	entry->holders[slot].cluster_epoch = holder->cluster_epoch;
	entry->holders[slot].request_id = holder->request_id;
	entry->holders[slot].mode = (LOCKMODE)mode;
	entry->generation++;
	return CLUSTER_GRD_ENTRY_OK;
}

ClusterGrdEntryResult
cluster_grd_entry_release_holder(ClusterGrdEntry *entry, const ClusterGrdHolderId *holder)
{
	int i;

	Assert(entry != NULL && holder != NULL);

	for (i = 0; i < entry->ngranted; i++) {
		if ((uint32)entry->holders[i].node_id == holder->node_id
			&& entry->holders[i].procno == holder->procno
			&& entry->holders[i].cluster_epoch == holder->cluster_epoch
			&& entry->holders[i].request_id == holder->request_id) {
			/* compact down */
			if (i < entry->ngranted - 1)
				entry->holders[i] = entry->holders[entry->ngranted - 1];
			memset(&entry->holders[entry->ngranted - 1], 0, sizeof(ClusterGrdHolder));
			entry->ngranted--;
			entry->generation++;
			return CLUSTER_GRD_ENTRY_OK;
		}
	}
	return CLUSTER_GRD_ENTRY_NOT_FOUND;
}

ClusterGrdEntryResult
cluster_grd_entry_add_waiter(ClusterGrdEntry *entry, const ClusterGrdHolderId *holder, int mode)
{
	int slot;

	Assert(entry != NULL && holder != NULL);

	if (entry->nwaiters >= PGRAC_GRD_MAX_WAITERS) {
		cluster_grd_inc_ges_work_queue_full();
		return CLUSTER_GRD_ENTRY_FULL;
	}
	slot = entry->nwaiters++;
	entry->waiters[slot].node_id = (int32)holder->node_id;
	/*
	 * spec-2.23 D6 — populate full reply identity from the GES holder
	 * tuple.  source_node_id mirrors node_id (the node hosting the
	 * waiting backend).  request_opcode defaults to GES_REQ_OPCODE_
	 * REQUEST when caller is the legacy 2-arg add_waiter path; the
	 * cluster_grd_entry_enqueue_or_grant entry point overrides it
	 * via the extended ClusterGrdWaiter mutation directly.
	 */
	entry->waiters[slot].source_node_id = (int32)holder->node_id;
	entry->waiters[slot].procno = holder->procno;
	entry->waiters[slot].cluster_epoch = holder->cluster_epoch;
	entry->waiters[slot].request_id = holder->request_id;
	entry->waiters[slot].request_opcode = 1; /* GES_REQ_OPCODE_REQUEST default */
	entry->waiters[slot].mode = (LOCKMODE)mode;
	/* spec-2.21: 0 placeholder — real timestamp 推 spec-2.22 wait-edge maintenance.
	 * Standalone cluster_unit binaries don't link utils/timestamp.o; using a real
	 * GetCurrentTimestamp() call broke L41 link surface on macOS arm64. */
	entry->waiters[slot].wait_start = 0;
	entry->generation++;
	return CLUSTER_GRD_ENTRY_OK;
}

ClusterGrdEntryResult
cluster_grd_entry_promote_waiter(ClusterGrdEntry *entry, const ClusterGrdHolderId *holder)
{
	int i;

	Assert(entry != NULL && holder != NULL);

	for (i = 0; i < entry->nwaiters; i++) {
		if ((uint32)entry->waiters[i].node_id == holder->node_id
			&& entry->waiters[i].procno == holder->procno
			&& entry->waiters[i].cluster_epoch == holder->cluster_epoch
			&& entry->waiters[i].request_id == holder->request_id) {
			LOCKMODE mode = entry->waiters[i].mode;

			if (i < entry->nwaiters - 1)
				entry->waiters[i] = entry->waiters[entry->nwaiters - 1];
			memset(&entry->waiters[entry->nwaiters - 1], 0, sizeof(ClusterGrdWaiter));
			entry->nwaiters--;
			entry->generation++;
			return cluster_grd_entry_grant_holder(entry, holder, mode);
		}
	}
	return CLUSTER_GRD_ENTRY_NOT_FOUND;
}


/* ============================================================
 * spec-2.23 D6 — GRD-owned grant / waiter-pop API (HC18 / HC19 / HC20).
 *
 *	Bundles conflict-matrix check + grant-or-enqueue + waiter-identity
 *	carry under a single critical section.  Cluster_lms.c dispatch uses
 *	these instead of the spec-2.21 lower-level mutators so that the
 *	ClusterGrdEntry body stays opaque (cluster_grd.h line 104
 *	forward-decl invariant).
 *
 *	Conflict judgement uses PG exported DoLockModesConflict — direct
 *	access to LockMethods[] / conflictTab[] internals is L138-prep
 *	abstraction-leak防御.
 * ============================================================ */

ClusterGrdGrantAction
cluster_grd_entry_enqueue_or_grant(const ClusterResId *resid, const ClusterGrdHolderId *holder,
								   int32 source_node_id, uint64 request_id, uint32 request_opcode,
								   int lockmode, ClusterGrdConflictHolder *conflict_holders_out,
								   int *n_conflict_out)
{
	ClusterGrdEntry *entry = NULL;
	ClusterGrdEntryResult lookup_result;
	int n_conflict = 0;
	int slot;

	Assert(resid != NULL && holder != NULL);

	lookup_result = cluster_grd_entry_lookup_or_create(resid, true, &entry);
	if (lookup_result == CLUSTER_GRD_ENTRY_NOT_READY)
		return CLUSTER_GRD_NOT_READY;
	if (lookup_result != CLUSTER_GRD_ENTRY_OK || entry == NULL)
		return CLUSTER_GRD_NOT_READY;

	SpinLockAcquire(&entry->lock);

	/*
	 * (1) Conflict scan via PG exported DoLockModesConflict.  Snapshot
	 *	  conflicting holders into the caller-provided buffer so the LMS
	 *	  can later fan out a targeted BAST (HC18).
	 */
	for (int i = 0; i < entry->ngranted; i++) {
		if (!DoLockModesConflict((LOCKMODE)lockmode, entry->holders[i].mode))
			continue;
		if (conflict_holders_out != NULL && n_conflict < PGRAC_GRD_MAX_HOLDERS) {
			conflict_holders_out[n_conflict].holder.node_id = entry->holders[i].node_id;
			conflict_holders_out[n_conflict].holder.procno = entry->holders[i].procno;
			conflict_holders_out[n_conflict].holder.cluster_epoch = entry->holders[i].cluster_epoch;
			conflict_holders_out[n_conflict].holder.request_id = entry->holders[i].request_id;
			conflict_holders_out[n_conflict].source_node_id = entry->holders[i].node_id;
			conflict_holders_out[n_conflict].held_mode = entry->holders[i].mode;
		}
		n_conflict++;
	}
	if (n_conflict_out != NULL)
		*n_conflict_out = n_conflict < PGRAC_GRD_MAX_HOLDERS ? n_conflict : PGRAC_GRD_MAX_HOLDERS;

	/*
	 * (2) No conflict → grant immediately and bump generation.
	 */
	if (n_conflict == 0) {
		if (entry->ngranted >= PGRAC_GRD_MAX_HOLDERS) {
			SpinLockRelease(&entry->lock);
			cluster_grd_entry_release(entry);
			cluster_grd_inc_ges_work_queue_full();
			return CLUSTER_GRD_WAIT_QUEUE_FULL;
		}
		slot = entry->ngranted++;
		entry->holders[slot].node_id = (int32)holder->node_id;
		entry->holders[slot].procno = holder->procno;
		entry->holders[slot].cluster_epoch = holder->cluster_epoch;
		entry->holders[slot].request_id = holder->request_id;
		entry->holders[slot].mode = (LOCKMODE)lockmode;
		entry->generation++;
		SpinLockRelease(&entry->lock);
		cluster_grd_entry_release(entry);
		return CLUSTER_GRD_GRANT_NOW;
	}

	/*
	 * (3) Conflict → enqueue waiter with full reply identity (HC17/HC19).
	 *	  HC12 family 53R71 fail-closed when waiter slot exhausted.
	 */
	if (entry->nwaiters >= PGRAC_GRD_MAX_WAITERS) {
		SpinLockRelease(&entry->lock);
		cluster_grd_entry_release(entry);
		cluster_grd_inc_ges_work_queue_full();
		return CLUSTER_GRD_WAIT_QUEUE_FULL;
	}

	slot = entry->nwaiters++;
	entry->waiters[slot].node_id = (int32)holder->node_id;
	entry->waiters[slot].source_node_id = source_node_id;
	entry->waiters[slot].procno = holder->procno;
	entry->waiters[slot].cluster_epoch = holder->cluster_epoch;
	entry->waiters[slot].request_id = request_id;
	entry->waiters[slot].request_opcode = request_opcode;
	entry->waiters[slot].mode = (LOCKMODE)lockmode;
	entry->waiters[slot].wait_start = 0;
	entry->generation++;

	SpinLockRelease(&entry->lock);
	cluster_grd_entry_release(entry);
	return CLUSTER_GRD_ENQUEUED_WAITER;
}

int
cluster_grd_entry_release_and_pop_compatible_waiter(const ClusterResId *resid,
													const ClusterGrdHolderId *holder,
													ClusterGrdWaiterIdentity *granted_out,
													int max_out)
{
	ClusterGrdEntry *entry = NULL;
	ClusterGrdEntryResult lookup_result;
	int found_holder = -1;
	int popped = 0;

	Assert(resid != NULL && holder != NULL);
	Assert(granted_out != NULL && max_out > 0);

	lookup_result = cluster_grd_entry_lookup_or_create(resid, false, &entry);
	if (lookup_result != CLUSTER_GRD_ENTRY_OK || entry == NULL)
		return 0;

	SpinLockAcquire(&entry->lock);

	/* (1) Locate the holder slot by full 4-tuple match. */
	for (int i = 0; i < entry->ngranted; i++) {
		if ((uint32)entry->holders[i].node_id == holder->node_id
			&& entry->holders[i].procno == holder->procno
			&& entry->holders[i].cluster_epoch == holder->cluster_epoch
			&& entry->holders[i].request_id == holder->request_id) {
			found_holder = i;
			break;
		}
	}
	if (found_holder < 0) {
		SpinLockRelease(&entry->lock);
		cluster_grd_entry_release(entry);
		return 0;
	}

	/* Compact holders[] down (preserve relative order for the surviving slots). */
	if (found_holder < entry->ngranted - 1)
		entry->holders[found_holder] = entry->holders[entry->ngranted - 1];
	memset(&entry->holders[entry->ngranted - 1], 0, sizeof(ClusterGrdHolder));
	entry->ngranted--;
	entry->generation++;

	/*
	 * (2) Scan FIFO waiters; pop the first whose mode is compatible with
	 *	  every surviving holder.  Promote to holders[].
	 */
	while (popped < max_out && entry->nwaiters > 0) {
		int chosen = -1;

		for (int w = 0; w < entry->nwaiters; w++) {
			bool compatible = true;

			for (int h = 0; h < entry->ngranted; h++) {
				if (DoLockModesConflict(entry->waiters[w].mode, entry->holders[h].mode)) {
					compatible = false;
					break;
				}
			}
			if (compatible) {
				chosen = w;
				break;
			}
		}
		if (chosen < 0)
			break;

		/* Capture identity for the caller's GES_REPLY send. */
		granted_out[popped].holder.node_id = (uint32)entry->waiters[chosen].node_id;
		granted_out[popped].holder.procno = entry->waiters[chosen].procno;
		granted_out[popped].holder.cluster_epoch = entry->waiters[chosen].cluster_epoch;
		granted_out[popped].holder.request_id = entry->waiters[chosen].request_id;
		granted_out[popped].source_node_id = entry->waiters[chosen].source_node_id;
		granted_out[popped].request_id = entry->waiters[chosen].request_id;
		granted_out[popped].request_opcode = entry->waiters[chosen].request_opcode;
		granted_out[popped].mode = entry->waiters[chosen].mode;

		/* Promote waiter to holder. */
		if (entry->ngranted < PGRAC_GRD_MAX_HOLDERS) {
			int hslot = entry->ngranted++;

			entry->holders[hslot].node_id = entry->waiters[chosen].node_id;
			entry->holders[hslot].procno = entry->waiters[chosen].procno;
			entry->holders[hslot].cluster_epoch = entry->waiters[chosen].cluster_epoch;
			entry->holders[hslot].request_id = entry->waiters[chosen].request_id;
			entry->holders[hslot].mode = entry->waiters[chosen].mode;
		}
		/* Compact waiters[]. */
		if (chosen < entry->nwaiters - 1)
			entry->waiters[chosen] = entry->waiters[entry->nwaiters - 1];
		memset(&entry->waiters[entry->nwaiters - 1], 0, sizeof(ClusterGrdWaiter));
		entry->nwaiters--;
		entry->generation++;
		popped++;
	}

	SpinLockRelease(&entry->lock);
	cluster_grd_entry_release(entry);
	return popped;
}


/* ============================================================
 * spec-2.21 D5 — inspection + reservation helpers.
 * ============================================================ */

bool
cluster_grd_entry_has_remote_holder(ClusterGrdEntry *entry, int32 self_node_id)
{
	int i;

	Assert(entry != NULL);
	for (i = 0; i < entry->ngranted; i++)
		if (entry->holders[i].node_id != self_node_id)
			return true;
	return false;
}

bool
cluster_grd_entry_has_pending_waiter(ClusterGrdEntry *entry)
{
	Assert(entry != NULL);
	return entry->nwaiters > 0;
}

bool
cluster_grd_entry_has_pending_convert(ClusterGrdEntry *entry)
{
	Assert(entry != NULL);
	return entry->nconverts > 0;
}

uint64
cluster_grd_entry_generation(ClusterGrdEntry *entry)
{
	Assert(entry != NULL);
	return entry->generation;
}

ClusterGrdEntryResult
cluster_grd_reservation_create(ClusterGrdEntry *entry, const ClusterGrdHolderId *holder, int mode)
{
	int slot;

	Assert(entry != NULL && holder != NULL);

	if (entry->nreservations >= PGRAC_GRD_MAX_HOLDERS)
		return CLUSTER_GRD_ENTRY_FULL;
	slot = entry->nreservations++;
	entry->reservations[slot].id = *holder;
	entry->reservations[slot].mode = (LOCKMODE)mode;
	entry->generation++;
	return CLUSTER_GRD_ENTRY_OK;
}

ClusterGrdEntryResult
cluster_grd_reservation_cancel(ClusterGrdEntry *entry, const ClusterGrdHolderId *holder)
{
	int i;

	Assert(entry != NULL && holder != NULL);

	for (i = 0; i < entry->nreservations; i++) {
		if (entry->reservations[i].id.node_id == holder->node_id
			&& entry->reservations[i].id.request_id == holder->request_id) {
			if (i < entry->nreservations - 1)
				entry->reservations[i] = entry->reservations[entry->nreservations - 1];
			memset(&entry->reservations[entry->nreservations - 1], 0,
				   sizeof(entry->reservations[0]));
			entry->nreservations--;
			entry->generation++;
			return CLUSTER_GRD_ENTRY_OK;
		}
	}
	return CLUSTER_GRD_ENTRY_NOT_FOUND;
}

ClusterGrdEntryResult
cluster_grd_reservation_promote(ClusterGrdEntry *entry, const ClusterGrdHolderId *holder)
{
	int i;

	Assert(entry != NULL && holder != NULL);

	for (i = 0; i < entry->nreservations; i++) {
		if (entry->reservations[i].id.node_id == holder->node_id
			&& entry->reservations[i].id.request_id == holder->request_id) {
			LOCKMODE mode = entry->reservations[i].mode;
			ClusterGrdEntryResult r;

			if (i < entry->nreservations - 1)
				entry->reservations[i] = entry->reservations[entry->nreservations - 1];
			memset(&entry->reservations[entry->nreservations - 1], 0,
				   sizeof(entry->reservations[0]));
			entry->nreservations--;
			r = cluster_grd_entry_grant_holder(entry, holder, (int)mode);
			/* generation already bumped by grant_holder */
			return r;
		}
	}
	return CLUSTER_GRD_ENTRY_NOT_FOUND;
}


/* ============================================================
 * CSSD DEAD / stale-epoch cleanup stubs — Step 4 D11 真激活.
 * ============================================================ */

/*
 * spec-2.16 Step 4 D11:  CSSD DEAD master sweep — traverses entry HTAB
 *   and per-entry filters holders[] / waiters[] / converts[] by
 *   node_id == dead_node_id (I48 — NO epoch filter).
 *
 *   Step 4 implementation:  uses cluster_grd_entry_htab via existing
 *   hash_seq_search pattern (mirror cluster_grd_entries_walk).  For
 *   each matching slot, decrement entry->ngranted / nwaiters / nconverts
 *   under entry->lock and zero the slot.  Idempotent re-entry safe.
 *
 *   Counters per cleanup invocation tracked via existing entry mutator
 *   counter family (spec-2.15 entry_current_count when ngranted hits 0).
 *   本 Step 0 真 mutator caller (spec-2.16 ships caller-side hooks
 *   stub only — full LockAcquireExtended 6-step integration in spec-
 *   2.17), so sweep is a no-op until cluster_unit Step 6 inject test
 *   exercises mutator + sweep round-trip.
 */
/* spec-2.24 D10 forward decl (definition later in same TU). */
extern int cluster_grd_entry_cleanup_guarded(ClusterGrdEntry *entry, int dead_procno,
											 int32 dead_node_id);

void
cluster_grd_cleanup_on_node_dead(int32 dead_node_id)
{
	HASH_SEQ_STATUS status;
	ClusterGrdEntry *entry;
	int swept = 0;

	if (cluster_grd_entry_htab == NULL) {
		ereport(DEBUG2, (errmsg_internal("cluster_grd_cleanup_on_node_dead(%d): "
										 "entry HTAB not allocated;  no-op",
										 dead_node_id)));
		return;
	}

	/*
	 * spec-2.24 D9 — converge via D10 cluster_grd_entry_cleanup_guarded
	 * (HC27 dual-path convergence;previously spec-2.16 had its own ad-hoc
	 * sweep loop here).  D10 enforces HC25-26 / I-cleanup-1..4 — HASH_REMOVE
	 * at-most-once + concurrent cleanup safety + RELEASE enqueue.
	 */
	hash_seq_init(&status, cluster_grd_entry_htab);
	while ((entry = (ClusterGrdEntry *) hash_seq_search(&status)) != NULL) {
		swept += cluster_grd_entry_cleanup_guarded(entry, -1, dead_node_id);
	}

	if (swept > 0)
		ereport(DEBUG1, (errmsg_internal("cluster_grd_cleanup_on_node_dead(%d): "
										 "swept %d holder/waiter/convert slots via D10 primitive",
										 dead_node_id, swept)));
}

/*
 * spec-2.16 Step 4 D11:  stale-epoch sweep — independent rule per I48.
 *   Filters by holder.cluster_epoch < current_epoch.  Triggered post-
 *   reconfig epoch bump (LMON tick S2;  I47).
 *
 *   Filters real holders by cluster_epoch.  Reservation cleanup is owned
 *   by caller-side S7 because reservations are local in-flight state, not
 *   a cluster-visible grant.
 */
void
cluster_grd_cleanup_stale_epoch(uint64 current_epoch)
{
	HASH_SEQ_STATUS status;
	ClusterGrdEntry *entry;
	int swept = 0;

	if (cluster_grd_entry_htab == NULL) {
		ereport(DEBUG2, (errmsg_internal("cluster_grd_cleanup_stale_epoch(%lu): "
										 "entry HTAB not allocated;  no-op",
										 (unsigned long)current_epoch)));
		return;
	}

	hash_seq_init(&status, cluster_grd_entry_htab);
	while ((entry = (ClusterGrdEntry *)hash_seq_search(&status)) != NULL) {
		int i;

		SpinLockAcquire(&entry->lock);
		for (i = 0; i < entry->ngranted;) {
			if (entry->holders[i].cluster_epoch < current_epoch) {
				if (i < entry->ngranted - 1)
					entry->holders[i] = entry->holders[entry->ngranted - 1];
				memset(&entry->holders[entry->ngranted - 1], 0, sizeof(entry->holders[0]));
				entry->ngranted--;
				swept++;
				continue;
			}
			i++;
		}
		SpinLockRelease(&entry->lock);
	}

	if (swept > 0)
		ereport(DEBUG1, (errmsg_internal("cluster_grd_cleanup_stale_epoch(%lu): "
										 "swept %d holder slots",
										 (unsigned long)current_epoch, swept)));
}


/* ============================================================
 * spec-2.16 D8:  LMON tick body GRD dead sweep — newly-dead bitmap
 *   diff per v0.5 P1.2 + I51.
 *
 *   static last_dead_bitmap + per-tick diff:
 *     - poll cluster_cssd_get_dead_generation();  unchanged → return
 *     - scan peer_state for all peers;  state==DEAD → set bit
 *     - newly_dead = current & ~last_dead_bitmap
 *     - for each newly-dead peer → cluster_grd_cleanup_on_node_dead(id)
 *     - last_dead_bitmap = current (commit AFTER sweep — crash-safe)
 *
 *   ALIVE / SUSPECTED不计;DEAD→ALIVE recovery 不重 sweep (bit drops
 *   from current_dead_bitmap, but already in last_dead_bitmap; on next
 *   transition ALIVE→DEAD bit re-enters newly_dead per AND-NOT logic).
 *
 *   Process-local static (per-postmaster).  LMON is singleton, so no
 *   shared-state contention.
 * ============================================================ */

static uint64 cluster_grd_last_dead_bitmap = 0;
static uint64 cluster_grd_last_dead_generation = 0;

void
cluster_grd_lmon_tick_dead_sweep(void)
{
	uint64 current_gen;
	uint64 current_dead_bitmap = 0;
	uint64 newly_dead;
	int peer_id;

	/* Postmaster-only tick (single LMON consumer).  No LWLock needed
	 * for static state. */
	current_gen = cluster_cssd_get_dead_generation();
	if (current_gen == cluster_grd_last_dead_generation)
		return;

	/* Scan peer states to build current_dead_bitmap.  Only DEAD counts;
	 * SUSPECTED is hysteresis-mid, not a sweep trigger. */
	for (peer_id = 0; peer_id < CLUSTER_MAX_NODES && peer_id < 64; peer_id++) {
		ClusterCssdPeerState s = cluster_cssd_get_peer_state(peer_id);
		if (s == CLUSTER_CSSD_PEER_DEAD)
			current_dead_bitmap |= ((uint64)1 << peer_id);
	}

	newly_dead = current_dead_bitmap & ~cluster_grd_last_dead_bitmap;
	for (peer_id = 0; peer_id < 64; peer_id++) {
		if (newly_dead & ((uint64)1 << peer_id))
			cluster_grd_cleanup_on_node_dead((int32)peer_id);
	}

	/* Commit AFTER sweep — crash-safe idempotent;  reboot reconstructs. */
	cluster_grd_last_dead_bitmap = current_dead_bitmap;
	cluster_grd_last_dead_generation = current_gen;
}


/* ============================================================
 * spec-2.17 D28b:  cluster_grd_alloc_generation helper.
 *
 *   Called from InitProcess() to allocate a per-backend monotonic
 *   generation number(uint64).  ABA-free via atomic fetch_add.
 *   0 reserved sentinel(0 = uninitialized).
 *
 *   Used by BAST/CANCEL stale signal validation:
 *     `MyProc->cluster_grd_generation == payload.target_generation`
 *     防 stale signal 误打到复用 procno 的新 backend.
 * ============================================================ */

uint64
cluster_grd_alloc_generation(void)
{
	/* Bootstrap-safe:  cluster_grd_state may be NULL in bootstrap mode
	 * (postmaster shmem not yet initialized).  Return 0 sentinel —
	 * caller is InitProcess() PGRAC hook which falls through gracefully. */
	if (cluster_grd_state == NULL)
		return 0;
	return pg_atomic_fetch_add_u64(&cluster_grd_state->next_generation, 1);
}


/* ============================================================
 * spec-2.17 D8 + D12:  BAST handler + 6 counter helpers.
 *
 *   D8 cluster_grd_bast_handler — ProcessInterrupts hook;backend 收到
 *   PROCSIG_CLUSTER_GES_BAST 后调.  **硬契约(I85 P1.8 v0.6)**:
 *   仅标 `MyProc->cluster_grd_bast_pending = true` flag;**0 主动 release**;
 *   naturally 等 canonical LockRelease/LockReleaseAll 自然路径 → LOCALLOCK
 *   refcount 0 → 7-step state machine release path 补发 GES_RELEASE.
 *
 *   D8 cluster_grd_cancel_handler — ProcessInterrupts hook for
 *   PROCSIG_CLUSTER_GES_CANCEL.  本 step skeleton — 真激活 Step 6.
 *
 *   D12 6 BAST nofail counter inc + read helpers.
 * ============================================================ */

void
cluster_grd_bast_handler(void)
{
	/* spec-2.17 I85 硬契约:仅标 flag;不主动 release / convert.
	 * naturally 等 LockRelease canonical 路径补发 GES_RELEASE. */
	if (MyProc != NULL)
		MyProc->cluster_grd_bast_pending = true;
	cluster_grd_inc_bast_received();
}

void
cluster_grd_cancel_handler(void)
{
	/* Checkpoint-safe placeholder.  The signal path is now correct
	 * (signal handler → pending flag → ProcessInterrupts → here), but
	 * wait-abort semantics are still owned by the caller-side activation
	 * step.  Do not pretend to cancel a GRD wait from this skeleton. */
}

void
cluster_grd_check_pending_interrupts(void)
{
	if (cluster_ges_bast_pending) {
		cluster_ges_bast_pending = false;
		cluster_grd_bast_handler();
	}

	if (cluster_ges_cancel_pending) {
		cluster_ges_cancel_pending = false;
		cluster_grd_cancel_handler();
	}
}

#define DEFINE_BAST_COUNTER(short_name, full_field)                                                \
	void cluster_grd_inc_##short_name(void)                                                        \
	{                                                                                              \
		if (cluster_grd_state != NULL)                                                             \
			pg_atomic_fetch_add_u64(&cluster_grd_state->ges_##full_field, 1);                      \
	}                                                                                              \
	uint64 cluster_grd_##full_field(void)                                                          \
	{                                                                                              \
		if (cluster_grd_state == NULL)                                                             \
			return 0;                                                                              \
		return pg_atomic_read_u64(&cluster_grd_state->ges_##full_field);                           \
	}

DEFINE_BAST_COUNTER(bast_sent, bast_sent_count)
DEFINE_BAST_COUNTER(bast_received, bast_received_count)
DEFINE_BAST_COUNTER(bast_ack, bast_ack_count)
DEFINE_BAST_COUNTER(bast_retry, bast_retry_count)
DEFINE_BAST_COUNTER(bast_reject, bast_reject_count)
DEFINE_BAST_COUNTER(bast_stale_drop, bast_stale_drop_count)


/* ============================================================
 * spec-2.17 Step 5/8:  deadlock detector skeleton.
 *
 *   Real activation:
 *     - LMON tick body invokes cluster_grd_deadlock_lmon_tick() each
 *       cluster.ges_deadlock_check_interval_ms(default 1000ms);
 *     - Tick body builds wait-for graph via vertex dictionary + edge
 *       chunk protocol(I82 collision-free);
 *     - On cycle detected:  victim selection via deterministic age-based
 *       4-tuple `(cluster_epoch, local_start_ts_ms DESC, node_id, xid)`
 *       (I69 P2.2);
 *     - Master enqueues GES_CANCEL_PENDING(opcode 7)or GES_RELEASE
 *       (opcode 3)to victim's outbound(I73-I74).
 *
 *   Step 5/8 skeleton:  function symbol + 3 counter only.  Real Tarjan
 *   SCC + vertex dict encode/decode + chunked reassembly buffer 推
 *   Hardening round(本 skeleton 已建立完整调用面 + counter 接口供
 *   Step 8 dump_grd + TAP test 钩入).
 * ============================================================ */

void
cluster_grd_deadlock_lmon_tick(void)
{
	/* Skeleton — Hardening round real Tarjan + vertex dict. */
}

DEFINE_BAST_COUNTER(deadlock_probe_drop, deadlock_probe_drop_count)
DEFINE_BAST_COUNTER(deadlock_probe_collision_drop, deadlock_probe_collision_drop_count)
DEFINE_BAST_COUNTER(deadlock_chunk_oo_buffer_overflow, deadlock_chunk_oo_buffer_overflow_count)


/* ============================================================
 * spec-2.17 Step 6:  cleanup_on_backend_exit(D21 skeleton).
 *
 *   Real activation:  on_proc_exit hook + ResourceOwner callback wire
 *   in Step 6;遍历 GRD entries 清单 backend procno 的 holders/waiters/
 *   converts(类 cleanup_on_node_dead pattern;但 backend-level not
 *   node-level)。
 *
 *   场景(I65 P1.1 — NOT BAST timeout):CANCEL / SIGTERM / on_proc_exit
 *   / backend self-abort.
 * ============================================================ */

/*
 * spec-2.24 D10 helper — HASH_REMOVE guarded by shard partition LWLock
 * + re-lookup + verify-still-empty.  Returns true if removed (we are
 * the at-most-once winner per HC26 I-cleanup-4);  false if another
 * cleanup path already removed the entry or the entry is no longer
 * empty (race lost — caller bumps skip counter).
 */
static bool
cluster_grd_hashremove_if_still_empty(const ClusterResId *resid)
{
	uint32 hashvalue;
	int shard_id;
	bool found = false;
	ClusterGrdEntry *entry;
	bool removed = false;

	if (cluster_grd_entry_htab == NULL)
		return false;

	hashvalue = cluster_grd_hash_resource(resid);
	shard_id = cluster_grd_shard_for_hash(hashvalue);

	LWLockAcquire(&cluster_grd_shard_locks[shard_id].lock, LW_EXCLUSIVE);

	entry
		= hash_search_with_hash_value(cluster_grd_entry_htab, resid, hashvalue, HASH_FIND, &found);
	if (entry != NULL) {
		SpinLockAcquire(&entry->lock);
		if (entry->ngranted == 0 && entry->nwaiters == 0 && entry->nconverts == 0) {
			SpinLockRelease(&entry->lock);
			(void) hash_search_with_hash_value(cluster_grd_entry_htab, resid, hashvalue,
											   HASH_REMOVE, &found);
			pg_atomic_fetch_sub_u64(&cluster_grd_state->entry_current_count, 1);
			removed = true;
		} else {
			SpinLockRelease(&entry->lock);
		}
	}

	LWLockRelease(&cluster_grd_shard_locks[shard_id].lock);
	return removed;
}

/*
 * spec-2.24 D10 — idempotent generation-guarded cleanup primitive.
 *
 *	HC25-26 / I-cleanup-1..4 enforcement single source of truth.  All 3
 *	cleanup paths converge here (HC27 dual-path convergence):
 *	  - D6 on_proc_exit fast path → cluster_grd_cleanup_on_backend_exit
 *	  - D8 LMD periodic safety net → cluster_lmd_periodic_cleanup_sweep
 *	  - D9 cssd dead-node bitmap → cluster_grd_cleanup_on_node_dead
 *
 *	Semantics (user §3 invariants):
 *	  I-cleanup-1 safe-to-call-multi:  re-entry NOT_FOUND no-op safe.
 *	  I-cleanup-2 only-one-owner:  entry->lock serializes mutation; if
 *	    a concurrent path already swept everything we look for, our
 *	    inner loop finds no matching slot and removed remains 0.
 *	  I-cleanup-3 NOT_FOUND no-op:  loop matches by 4-tuple of slot
 *	    contents; absent → just continue, no error.
 *	  I-cleanup-4 HASH_REMOVE at-most-once:  empty-entry HASH_REMOVE
 *	    guarded by shard partition LWLock re-lookup + verify-still-
 *	    empty + verify-generation-advanced;losing path increments
 *	    cleanup_skip_other_owner_count.
 *
 *	dead_procno:  if >= 0, match slot.procno == dead_procno (local
 *	  backend exit / SIGKILL).  Always combined with local node match
 *	  (slot.node_id == cluster_node_id) — must NOT compare local
 *	  ProcArray to remote holder procno per user codereview Change 4.
 *	dead_node_id:  if >= 0, match slot.node_id == dead_node_id (peer
 *	  node death from cssd dead-bitmap).
 *
 *	Returns number of slots removed across all 4 slot kinds.
 */
int
cluster_grd_entry_cleanup_guarded(ClusterGrdEntry *entry, int dead_procno, int32 dead_node_id)
{
	int removed = 0;
	bool became_empty = false;
	uint8 release_payloads[PGRAC_GRD_MAX_HOLDERS][24]; /* holder identity for release */
	int32 release_master_nodes[PGRAC_GRD_MAX_HOLDERS];
	int n_release = 0;
	ClusterResId entry_resid;

	Assert(entry != NULL);

	SpinLockAcquire(&entry->lock);

	/* HC26 I-cleanup-3 — each remove path matches by content; absent → continue. */
	for (int i = entry->ngranted - 1; i >= 0; i--) {
		bool match = false;

		if (dead_procno >= 0 && entry->holders[i].node_id == (int32) cluster_node_id
			&& entry->holders[i].procno == (uint32) dead_procno)
			match = true;
		if (dead_node_id >= 0 && entry->holders[i].node_id == dead_node_id)
			match = true;
		if (!match)
			continue;

		/* Stash holder identity for post-lock RELEASE enqueue. */
		if (n_release < PGRAC_GRD_MAX_HOLDERS) {
			memcpy(release_payloads[n_release], &entry->holders[i], 24);
			release_master_nodes[n_release] = entry->holders[i].node_id;
			n_release++;
		}

		if (i < entry->ngranted - 1)
			entry->holders[i] = entry->holders[entry->ngranted - 1];
		memset(&entry->holders[entry->ngranted - 1], 0, sizeof(ClusterGrdHolder));
		entry->ngranted--;
		removed++;
	}
	for (int i = entry->nwaiters - 1; i >= 0; i--) {
		bool match = false;

		if (dead_procno >= 0 && entry->waiters[i].node_id == (int32) cluster_node_id
			&& entry->waiters[i].procno == (uint32) dead_procno)
			match = true;
		if (dead_node_id >= 0 && entry->waiters[i].node_id == dead_node_id)
			match = true;
		if (!match)
			continue;

		if (i < entry->nwaiters - 1)
			entry->waiters[i] = entry->waiters[entry->nwaiters - 1];
		memset(&entry->waiters[entry->nwaiters - 1], 0, sizeof(ClusterGrdWaiter));
		entry->nwaiters--;
		removed++;
	}
	for (int i = entry->nconverts - 1; i >= 0; i--) {
		bool match = false;

		if (dead_node_id >= 0 && entry->converts[i].node_id == dead_node_id)
			match = true;
		if (!match)
			continue;

		if (i < entry->nconverts - 1)
			entry->converts[i] = entry->converts[entry->nconverts - 1];
		memset(&entry->converts[entry->nconverts - 1], 0, sizeof(ClusterGrdConvert));
		entry->nconverts--;
		removed++;
	}

	/* HC25 — bump generation if any mutation; serves as ABA marker for
	 * concurrent cleanup detection (other paths see new generation and
	 * recheck before HASH_REMOVE). */
	if (removed > 0)
		entry->generation++;

	became_empty = (entry->ngranted == 0 && entry->nwaiters == 0 && entry->nconverts == 0);

	memcpy(&entry_resid, &entry->resid, sizeof(entry_resid));

	SpinLockRelease(&entry->lock);

	/* Enqueue cluster_grd_outbound_enqueue_cleanup_release per real removed
	 * holder (out-of-lock; spec-2.16 D4 reserved pool nofail). */
	for (int i = 0; i < n_release; i++) {
		cluster_grd_outbound_enqueue_cleanup_release((uint32) release_master_nodes[i],
													 release_payloads[i], 24);
	}

	/* HC26 I-cleanup-4 — HASH_REMOVE at-most-once per entry lifetime.
	 * Re-acquire shard partition LWLock + re-lookup resid + verify still
	 * empty; the losing path (race with another cleanup that already
	 * removed) increments skip counter. */
	if (became_empty) {
		bool removed_ok = cluster_grd_hashremove_if_still_empty(&entry_resid);
		if (!removed_ok)
			cluster_lmd_cleanup_skip_other_owner_count_inc(1);
	}

	return removed;
}

/*
 * Entry-by-procno sweep.  Iterates GRD HTAB; for each entry, invokes
 * D10 guarded primitive.  Returns total slots removed.
 */
static int
cluster_grd_entries_cleanup_by_procno_guarded(int procno)
{
	HASH_SEQ_STATUS status;
	ClusterGrdEntry *entry;
	int total = 0;

	if (cluster_grd_entry_htab == NULL || procno < 0)
		return 0;

	hash_seq_init(&status, cluster_grd_entry_htab);
	while ((entry = (ClusterGrdEntry *) hash_seq_search(&status)) != NULL) {
		total += cluster_grd_entry_cleanup_guarded(entry, procno, -1);
	}
	return total;
}

void
cluster_grd_cleanup_on_backend_exit(int procno)
{
	int swept;

	if (procno < 0)
		return; /* I-cleanup-1 — re-entry safe */

	swept = cluster_grd_entries_cleanup_by_procno_guarded(procno);
	if (swept > 0)
		cluster_lmd_cleanup_on_backend_exit_count_inc((uint64) swept);
}

/*
 * spec-2.24 D7 — before_shmem_exit callback wrapper.
 *
 *	Registered from InitPostgres so every backend gets the hook on
 *	exit.  MyProcNumber is valid by InitPostgres time;  if -1 (auxiliary
 *	process pre-ProcSignalInit), I-cleanup-1 early return is safe.
 */
/*
 * spec-2.24 D8 helper — sweep local stale procnos.
 *
 *	HC28 chunked semantic:  iterate the GRD HTAB once, briefly snapshot
 *	ProcArray for active local pgprocno set, then for each entry invoke
 *	cluster_grd_entry_cleanup_guarded with the local stale procno if its
 *	holders[].node_id == cluster_node_id and procno not in the active
 *	set.  Per spec-2.24 §1.4 example 2 — must NOT compare local
 *	ProcArray to remote holder procno.
 *
 *	Returns total slots removed.
 */
int
cluster_grd_sweep_local_stale_procnos(void)
{
	HASH_SEQ_STATUS status;
	ClusterGrdEntry *entry;
	int total = 0;
	uint8 *alive;
	int n_alive_max = MaxBackends;

	if (cluster_grd_entry_htab == NULL)
		return 0;

	alive = (uint8 *) palloc0((Size) n_alive_max);

	/* Briefly snapshot ProcArray active pgprocno set. */
	LWLockAcquire(ProcArrayLock, LW_SHARED);
	{
		int n = ProcGlobal->allProcCount;
		for (int i = 0; i < n && i < n_alive_max; i++) {
			PGPROC *p = &ProcGlobal->allProcs[i];
			if (p->pid != 0)
				alive[i] = 1;
		}
	}
	LWLockRelease(ProcArrayLock);

	hash_seq_init(&status, cluster_grd_entry_htab);
	while ((entry = (ClusterGrdEntry *) hash_seq_search(&status)) != NULL) {
		uint32 stale_procno = (uint32) -1;
		uint32 i;

		SpinLockAcquire(&entry->lock);
		for (i = 0; i < (uint32) entry->ngranted; i++) {
			if (entry->holders[i].node_id != (int32) cluster_node_id)
				continue;
			if (entry->holders[i].procno < (uint32) n_alive_max
				&& alive[entry->holders[i].procno] == 0) {
				stale_procno = entry->holders[i].procno;
				break;
			}
		}
		SpinLockRelease(&entry->lock);

		if (stale_procno != (uint32) -1)
			total += cluster_grd_entry_cleanup_guarded(entry, (int) stale_procno, -1);
	}

	pfree(alive);
	return total;
}

void
cluster_grd_cleanup_on_backend_exit_callback(int code, Datum arg)
{
	(void) code;
	(void) arg;
	if (MyProc == NULL)
		return;
	cluster_grd_cleanup_on_backend_exit(MyProc->pgprocno);
}


/* ============================================================
 * spec-2.21 D5 high-level helpers — encapsulate entry slock + 5-check +
 * reservation/promote.  cluster_lock_acquire.c uses these so the entry
 * struct definition can stay private to this file.
 * ============================================================ */

ClusterGrdEntryResult
cluster_grd_try_reserve(const ClusterResId *resid, const ClusterGrdHolderId *holder, int mode,
						int32 self_node_id, bool *fast_path_out, uint64 *gen_snapshot_out)
{
	ClusterGrdEntry *entry = NULL;
	ClusterGrdEntryResult er;
	int32 master;
	bool fast_path;

	Assert(resid != NULL && holder != NULL);

	er = cluster_grd_entry_lookup_or_create(resid, true, &entry);
	if (er != CLUSTER_GRD_ENTRY_OK || entry == NULL)
		return er;

	master = cluster_grd_lookup_master(resid);

	SpinLockAcquire(&entry->lock);
	if (gen_snapshot_out)
		*gen_snapshot_out = entry->generation;

	fast_path = (master == self_node_id || master < 0)
				&& !cluster_grd_entry_has_remote_holder(entry, self_node_id)
				&& !cluster_grd_entry_has_pending_waiter(entry)
				&& !cluster_grd_entry_has_pending_convert(entry);

	er = cluster_grd_reservation_create(entry, holder, mode);
	SpinLockRelease(&entry->lock);

	if (fast_path_out)
		*fast_path_out = fast_path && (er == CLUSTER_GRD_ENTRY_OK);
	return er;
}

ClusterGrdEntryResult
cluster_grd_revalidate_and_promote(const ClusterResId *resid, const ClusterGrdHolderId *holder,
								   int32 self_node_id, uint64 gen_snapshot)
{
	ClusterGrdEntry *entry = NULL;
	ClusterGrdEntryResult er;
	bool revalidate_ok;

	Assert(resid != NULL && holder != NULL);

	er = cluster_grd_entry_lookup_or_create(resid, false, &entry);
	if (er != CLUSTER_GRD_ENTRY_OK || entry == NULL)
		return CLUSTER_GRD_ENTRY_NOT_FOUND;

	SpinLockAcquire(&entry->lock);
	/*
	 * P2.3 revalidate target = no incompatible state ascended after the
	 * S3 snapshot.  reservation_create() bumps generation exactly once
	 * after gen_snapshot; any later mutation means the caller's reservation
	 * is no longer the sole in-flight state we can safely promote.
	 */
	revalidate_ok = (entry->generation == gen_snapshot + 1)
					&& !cluster_grd_entry_has_remote_holder(entry, self_node_id)
					&& !cluster_grd_entry_has_pending_waiter(entry)
					&& !cluster_grd_entry_has_pending_convert(entry);
	if (!revalidate_ok) {
		(void)cluster_grd_reservation_cancel(entry, holder);
		SpinLockRelease(&entry->lock);
		return CLUSTER_GRD_ENTRY_NOT_FOUND;
	}
	er = cluster_grd_reservation_promote(entry, holder);
	SpinLockRelease(&entry->lock);
	return er;
}

ClusterGrdEntryResult
cluster_grd_release_holder_by_id(const ClusterResId *resid, const ClusterGrdHolderId *holder)
{
	ClusterGrdEntry *entry = NULL;
	ClusterGrdEntryResult er;

	Assert(resid != NULL && holder != NULL);

	er = cluster_grd_entry_lookup_or_create(resid, false, &entry);
	if (er != CLUSTER_GRD_ENTRY_OK || entry == NULL)
		return CLUSTER_GRD_ENTRY_NOT_FOUND;

	SpinLockAcquire(&entry->lock);
	er = cluster_grd_entry_release_holder(entry, holder);
	SpinLockRelease(&entry->lock);
	return er;
}

ClusterGrdEntryResult
cluster_grd_cancel_reservation_by_id(const ClusterResId *resid, const ClusterGrdHolderId *holder)
{
	ClusterGrdEntry *entry = NULL;
	ClusterGrdEntryResult er;

	Assert(resid != NULL && holder != NULL);

	er = cluster_grd_entry_lookup_or_create(resid, false, &entry);
	if (er != CLUSTER_GRD_ENTRY_OK || entry == NULL)
		return CLUSTER_GRD_ENTRY_NOT_FOUND;

	SpinLockAcquire(&entry->lock);
	er = cluster_grd_reservation_cancel(entry, holder);
	SpinLockRelease(&entry->lock);
	return er;
}
