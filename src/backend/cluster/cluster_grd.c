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
#include "cluster/cluster_ges.h"	  /* GesRequestPayload / RELEASE cleanup payload */
#include "cluster/cluster_ges_mode.h" /* spec-5.1b D1: ges_modes_compatible (frozen matrix) */
#include "cluster/cluster_grd.h"
#include "cluster/cluster_grd_outbound.h" /* cluster_grd_outbound_enqueue_cleanup_release (D10) */
#include "cluster/cluster_lmd.h"		  /* spec-2.24 D10 cleanup_*_count_inc */
#include "cluster/cluster_guc.h"		  /* cluster_node_id, cluster_grd_max_entries */
#include "cluster/cluster_lms.h"		  /* spec-4.6 D2 — Q3-C wire routing token */
#include "cluster/cluster_pcm_lock.h"	  /* spec-2.36 HC124 pending_x node-dead cleanup */
#include "cluster/cluster_gcs.h"		  /* spec-4.7 D2 — cluster_gcs_lookup_master */
#include "cluster/cluster_gcs_block.h"	  /* spec-4.7 D2 — block re-declare scan + send */
#include "cluster/cluster_signal.h"
#include "cluster/cluster_shmem.h"
#include "cluster/cluster_cssd.h"			 /* spec-2.16 D8 newly-dead bitmap diff */
#include "cluster/cluster_epoch.h"			 /* spec-4.6 D1 — accepted epoch reads */
#include "cluster/cluster_reconfig.h"		 /* spec-4.6 D1 — reconfig event consume */
#include "cluster/cluster_thread_recovery.h" /* spec-4.11 D3 — unfreeze gate */
#include "storage/procsignal.h"				 /* spec-4.6 D3 — redeclare broadcast */
#include "storage/sinvaladt.h"				 /* spec-4.6 D3 — BackendIdGetProc */
#include "utils/timestamp.h"				 /* spec-4.6 D1 — barrier deadline */
#include "storage/proc.h"					 /* spec-2.17 D8 — MyProc->cluster_grd_bast_pending */
#include "common/hashfn.h"					 /* hash_bytes_extended (spec-2.29 同款) */
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
	int32 node_id;					/* legacy: equal to source_node_id; kept for binary compat */
	int32 source_node_id;			/* node hosting the waiting backend */
	uint32 procno;					/* PG ProcNumber of the waiting backend */
	uint64 cluster_epoch;			/* epoch at waiter enqueue time */
	uint64 request_id;				/* per-backend monotonic id (for 5-tuple reply key) */
	uint64 shard_master_generation; /* spec-2.27 dedup key carry */
	uint32 request_opcode;			/* GesRequestOpcode of the queued request */
	LOCKMODE mode;
	TimestampTz wait_start;
} ClusterGrdWaiter;

/*
 * spec-5.1b D2 — ClusterGrdConvert was promoted to cluster_grd.h (full
 * struct: locator (node,procno,current_mode) vs convert_request_id reply
 * key) so the convert state machine + drain helper are a public,
 * unit-testable surface.  The entry below embeds the converts[] array of
 * the header type.
 */

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
		for (i = 0; i < PGRAC_GRD_SHARD_COUNT; i++) {
			pg_atomic_init_u32(&cluster_grd_state->master[i], 0);
			/* spec-4.6 D2/D1: remaster generation + recovery phase. */
			pg_atomic_init_u32(&cluster_grd_state->master_generation[i], 0);
			pg_atomic_init_u32(&cluster_grd_state->shard_phase[i], (uint32)GRD_SHARD_NORMAL);
		}
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

		/* spec-5.1b D9 — convert state-machine counters. */
		pg_atomic_init_u64(&cluster_grd_state->convert_granted_inplace_count, 0);
		pg_atomic_init_u64(&cluster_grd_state->convert_enqueued_count, 0);
		pg_atomic_init_u64(&cluster_grd_state->convert_illegal_count, 0);

		pg_atomic_init_u64(&cluster_grd_state->ges_work_queue_full_count, 0);
		pg_atomic_init_u64(&cluster_grd_state->ges_cleanup_deferred_count, 0);
		pg_atomic_init_u64(&cluster_grd_state->ges_inbound_validation_fail_count, 0);
		pg_atomic_init_u64(&cluster_grd_state->cleanup_skip_stale_cancel_count, 0);
		/* spec-2.25 D13 — RELATION + OBJECT cluster gate hit counter. */
		pg_atomic_init_u64(&cluster_grd_state->relation_object_cluster_path_count, 0);
		/* spec-2.26 D5 — TRANSACTION cluster gate hit counter. */
		pg_atomic_init_u64(&cluster_grd_state->transaction_cluster_path_count, 0);
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

		/* spec-4.6 D2/D5 — remaster epoch + 13 grd_recovery counters. */
		pg_atomic_init_u64(&cluster_grd_state->reconfig_remaster_epoch, 0);
		/* spec-4.6 D1 — recovery sequence cursor. */
		pg_atomic_init_u64(&cluster_grd_state->recovery_last_event_id, 0);
		pg_atomic_init_u32(&cluster_grd_state->recovery_state, (uint32)GRD_RECOVERY_IDLE);
		for (i = 0; i < (CLUSTER_MAX_NODES + 63) / 64; i++)
			pg_atomic_init_u64(&cluster_grd_state->recovery_dead_bitmap[i], 0);
		pg_atomic_init_u64(&cluster_grd_state->recovery_event_old_epoch, 0);
		pg_atomic_init_u64(&cluster_grd_state->recovery_redeclare_generation, 0);
		pg_atomic_init_u64(&cluster_grd_state->recovery_barrier_deadline, 0);
		/* spec-4.6 P0#3 cluster gate — per-node barrier-done epochs. */
		for (i = 0; i < CLUSTER_MAX_NODES; i++)
			pg_atomic_init_u64(&cluster_grd_state->recovery_done_epoch[i], 0);
		pg_atomic_init_u64(&cluster_grd_state->remaster_started_count, 0);
		pg_atomic_init_u64(&cluster_grd_state->remaster_done_count, 0);
		pg_atomic_init_u64(&cluster_grd_state->remaster_failed_count, 0);
		pg_atomic_init_u64(&cluster_grd_state->shards_remastered_count, 0);
		pg_atomic_init_u64(&cluster_grd_state->holders_redeclared_count, 0);
		pg_atomic_init_u64(&cluster_grd_state->holders_rebound_count, 0);
		pg_atomic_init_u64(&cluster_grd_state->waiters_requeued_count, 0);
		pg_atomic_init_u64(&cluster_grd_state->converts_requeued_count, 0);
		pg_atomic_init_u64(&cluster_grd_state->stale_request_drop_count, 0);
		pg_atomic_init_u64(&cluster_grd_state->rebuild_timeout_count, 0);
		pg_atomic_init_u64(&cluster_grd_state->block_path_failclosed_count, 0);
		pg_atomic_init_u64(&cluster_grd_state->unaffected_holder_survived_count, 0);
		pg_atomic_init_u64(&cluster_grd_state->stale_holder_swept_count, 0);
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

	/*
	 * spec-2.26 D2 / HC40 — LOCKTAG_TRANSACTION origin wrapper.
	 *
	 *	PG SET_LOCKTAG_TRANSACTION leaves field2/3/4 zero (only field1
	 *	carries the local TransactionId).  Cluster wrapper overlays
	 *	field2 = cluster_node_id (origin_node_id) for cross-instance
	 *	GES routing while preserving field1 unchanged.  Other GES /
	 *	GRD layers (holder identity, envelope epoch validation) supply
	 *	the remaining stale-defence dimensions (HC43 / HC44).
	 *
	 *	HC47 caller contract: cluster_lock_should_globalize must have
	 *	rejected invalid cluster_node_id ranges before reaching this
	 *	encoder.  Assert in debug for defense in depth; production
	 *	leaves the (LOCKTAG-native) field2 = 0 unchanged on Assert miss
	 *	to avoid silently writing 0xFFFFFFFF to the wire (R11).
	 */
	if (src->locktag_type == LOCKTAG_TRANSACTION) {
		Assert(src->locktag_field2 == 0);
		Assert(src->locktag_field3 == 0);
		Assert(src->locktag_field4 == 0);
		/* Always clear the PG-native padding fields before overlaying the
		 * origin.  This keeps the encoder defensive even if a future caller
		 * bypasses the gate with a malformed TRANSACTION locktag. */
		dst->field2 = 0;
		dst->field3 = 0;
		dst->field4 = 0;
		if (cluster_node_id >= 0 && cluster_node_id < CLUSTER_MAX_NODES)
			dst->field2 = (uint32)cluster_node_id;
		/* else leave origin_node_id = 0 — caller gate is expected to have
		 * prevented us reaching here, but never write an out-of-range id. */
	}

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

	/*
	 * spec-2.26 D2 / HC40 — TRANSACTION reverse decode must NOT
	 * propagate origin_node_id back into PG-native LOCKTAG.field2.
	 * PG SET_LOCKTAG_TRANSACTION layout has field2 = 0 invariant;
	 * any downstream call comparing the decoded LOCKTAG against a
	 * freshly-built PG LOCKTAG would mis-match if we left field2 = N.
	 */
	if (src->type == LOCKTAG_TRANSACTION)
		dst->locktag_field2 = 0;
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
	/* Affinity/DRM placeholder (Stage 6) — ALIVE→ALIVE migration only.
	 * Failure-driven remaster is REAL since spec-4.6:  see
	 * cluster_grd_master_map_remaster() below.  Body stays a no-op
	 * except for the observability counter. */
	if (cluster_grd_state != NULL)
		pg_atomic_fetch_add_u64(&cluster_grd_state->master_map_refresh_count, 1);
}

/* spec-4.6 D2 — dead-bitmap bit test (CLUSTER_MAX_NODES bits as uint64
 * words;  word [node >> 6], bit (node & 63)). */
static inline bool
grd_dead_bitmap_test(const uint64 *dead_bitmap, int32 node_id)
{
	if (node_id < 0 || node_id >= CLUSTER_MAX_NODES)
		return false;
	return (dead_bitmap[node_id >> 6] >> (node_id & 63)) & 1;
}

/*
 * cluster_grd_master_map_remaster
 *
 *	spec-4.6 D2 — failure-driven remaster.  See cluster_grd.h for the
 *	full contract (deterministic survivor recompute from the accepted
 *	membership snapshot;  idempotent;  generation bumps only on real
 *	master movement).
 *
 *	Concurrency:  master[] writes are lock-free atomics;  callers are
 *	ordered by the spec-4.6 D1 P0-P7 barrier (P1 freeze precedes this,
 *	so backend lookups on affected shards are already fenced by the
 *	shard phase, and unaffected shards never change here).
 */
uint32
cluster_grd_master_map_remaster(const uint64 *dead_bitmap, uint64 reconfig_epoch)
{
	int32 survivors[CLUSTER_MAX_NODES];
	int survivor_count = 0;
	uint32 moved = 0;
	int i;

	Assert(cluster_grd_state != NULL);

	if (dead_bitmap == NULL)
		return 0;
	if (pg_atomic_read_u32(&cluster_grd_state->master_map_initialized) == 0)
		return 0; /* no map yet — nothing to remaster */

	/*
	 * Accepted membership snapshot = declared list (pgrac.conf, ascending
	 * scan order) minus the reconfig-accepted dead bits.  NEVER ad-hoc
	 * local peer_state (hard-gate #1:  every node must compute the same
	 * survivor list from the same snapshot).
	 */
	for (i = 0; i < CLUSTER_MAX_NODES; i++) {
		if (cluster_conf_lookup_node(i) == NULL)
			continue;
		if (!grd_dead_bitmap_test(dead_bitmap, i))
			survivors[survivor_count++] = i;
	}
	if (survivor_count <= 0) {
		/* Total declared death — fail-closed upstream (QVOTEC quorum
		 * loss freezes writes);  never reassign shards to nobody. */
		return 0;
	}

	for (i = 0; i < PGRAC_GRD_SHARD_COUNT; i++) {
		uint32 cur = pg_atomic_read_u32(&cluster_grd_state->master[i]);
		int32 target;

		if (!grd_dead_bitmap_test(dead_bitmap, (int32)cur))
			continue; /* master alive — failure-driven only, no affinity */

		target = survivors[i % survivor_count];
		pg_atomic_write_u32(&cluster_grd_state->master[i], (uint32)target);
		pg_atomic_fetch_add_u32(&cluster_grd_state->master_generation[i], 1);
		moved++;
	}

	if (moved > 0) {
		pg_atomic_write_u64(&cluster_grd_state->reconfig_remaster_epoch, reconfig_epoch);
		pg_atomic_fetch_add_u64(&cluster_grd_state->shards_remastered_count, moved);
		pg_atomic_fetch_add_u64(&cluster_grd_state->master_map_refresh_count, 1);
		ereport(DEBUG1, (errmsg_internal("cluster_grd_master_map_remaster: moved %u shards "
										 "to %d survivors (reconfig epoch " UINT64_FORMAT ")",
										 moved, survivor_count, reconfig_epoch)));
	}
	return moved;
}

/*
 * cluster_grd_lookup_master_gen
 *
 *	spec-4.6 D2 — master lookup + wire routing token.  Q3-C:  the token
 *	is the EXISTING (accepted_epoch<<32)|lms_restart_gen verbatim;  the
 *	per-shard remaster generation never rides the wire.
 */
int32
cluster_grd_lookup_master_gen(const ClusterResId *resid, uint64 *out_routing_generation)
{
	if (out_routing_generation)
		*out_routing_generation = cluster_lms_get_shard_master_generation();
	return cluster_grd_lookup_master(resid);
}

uint32
cluster_grd_shard_master_generation(uint32 shard_id)
{
	Assert(cluster_grd_state != NULL);
	if (shard_id >= PGRAC_GRD_SHARD_COUNT)
		return 0;
	return pg_atomic_read_u32(&cluster_grd_state->master_generation[shard_id]);
}

/*
 * Shard recovery phase accessors — spec-4.6 D1/D4.
 *
 *	LMON is the only writer (set_phase);  backends read lock-free on
 *	the request path.  Unknown shard ids read as NORMAL (defensive:
 *	the caller-side resid→shard mapping already bounds the id).
 */
ClusterGrdShardPhase
cluster_grd_shard_phase(uint32 shard_id)
{
	if (cluster_grd_state == NULL || shard_id >= PGRAC_GRD_SHARD_COUNT)
		return GRD_SHARD_NORMAL;
	return (ClusterGrdShardPhase)pg_atomic_read_u32(&cluster_grd_state->shard_phase[shard_id]);
}

void
cluster_grd_shard_set_phase(uint32 shard_id, ClusterGrdShardPhase phase)
{
	Assert(cluster_grd_state != NULL);
	if (shard_id >= PGRAC_GRD_SHARD_COUNT)
		return;
	pg_atomic_write_u32(&cluster_grd_state->shard_phase[shard_id], (uint32)phase);
}


/* ============================================================
 * spec-4.6 D1 — GRD recovery sequence (P0-P7).
 *
 *	LMON tick driver, sequenced AFTER cluster_reconfig_lmon_tick in the
 *	LMON main loop:
 *	  P0 accept DEAD     reconfig event published (quorum/evict/fence)
 *	  P1 freeze affected shards whose CURRENT master is dead → FROZEN
 *	  P2 cleanup dead    dead_sweep (earlier in the tick, I47)
 *	  P3 scoped sweep    epoch-stale leftovers on affected shards only
 *	  P4 remaster        cluster_grd_master_map_remaster (D2)
 *	  P5 rebuild         REBUILDING + redeclare broadcast + ack barrier
 *	  P6 global sweep    post-barrier ONLY (P0#3)
 *	  P7 unfreeze        NORMAL
 *	Phase regressions are impossible by construction (single LMON
 *	writer + the recovery_state cursor);  the P5→P6 edge is the hard
 *	barrier:  running the global sweep before every live backend acked
 *	would re-create P0#2 (deleting live-but-not-yet-rebound holders).
 * ============================================================ */

#define GRD_SHARD_BITMAP_WORDS (PGRAC_GRD_SHARD_COUNT / 64)

static inline bool
grd_shard_bitmap_test(const uint64 *bm, uint32 shard)
{
	return (bm[shard >> 6] >> (shard & 63)) & 1;
}

static inline void
grd_shard_bitmap_set(uint64 *bm, uint32 shard)
{
	bm[shard >> 6] |= ((uint64)1 << (shard & 63));
}

uint64
cluster_grd_redeclare_generation(void)
{
	/* NULL-safe:  InitProcess seeds the PGPROC ack from this before the
	 * backend ever touches GRD state;  shmem-less contexts read 0. */
	if (cluster_grd_state == NULL)
		return 0;
	return pg_atomic_read_u64(&cluster_grd_state->recovery_redeclare_generation);
}

uint64
cluster_grd_redeclare_episode_epoch(void)
{
	if (cluster_grd_state == NULL)
		return 0;
	return pg_atomic_read_u64(&cluster_grd_state->recovery_episode_epoch);
}

/*
 * cluster_grd_recovery_in_progress -- spec-4.7 D7 (P0 code-review fix).
 *
 *	True while this node's reconfig recovery FSM is NOT idle, i.e. an episode
 *	is freezing / rebinding / awaiting the cluster REDECLARE_DONE barrier.
 *	The block-resource phase gate uses this to keep every dead-static-master
 *	block fail-closed (RECOVERING) for the whole episode:  until this node
 *	reaches IDLE it has not seen every survivor's REDECLARE_DONE (now gated on
 *	their block re-declare scans completing), so a held block may not yet have
 *	been re-declared to its recovery-aware master — serving it would risk an
 *	8.A double-grant.  Reaching IDLE implies all survivor scans completed.
 */
bool
cluster_grd_recovery_in_progress(void)
{
	if (cluster_grd_state == NULL)
		return false;
	return pg_atomic_read_u32(&cluster_grd_state->recovery_state) != (uint32)GRD_RECOVERY_IDLE;
}

/* spec-4.6 D4/D5 — recovery counter bump helpers for out-of-module
 * call sites (S4 stale mapping in cluster_lock_acquire.c;  GCS block
 * fail-closed guard in cluster_gcs_block.c). */
void
cluster_grd_inc_stale_request_drop(void)
{
	if (cluster_grd_state != NULL)
		pg_atomic_fetch_add_u64(&cluster_grd_state->stale_request_drop_count, 1);
}

void
cluster_grd_inc_block_path_failclosed(void)
{
	if (cluster_grd_state != NULL)
		pg_atomic_fetch_add_u64(&cluster_grd_state->block_path_failclosed_count, 1);
}

/* spec-4.6 D5 — bulk counter snapshot for the dump path. */
void
cluster_grd_recovery_counters_snapshot(ClusterGrdRecoveryCounters *out)
{
	memset(out, 0, sizeof(*out));
	if (cluster_grd_state == NULL)
		return;
	out->remaster_started = pg_atomic_read_u64(&cluster_grd_state->remaster_started_count);
	out->remaster_done = pg_atomic_read_u64(&cluster_grd_state->remaster_done_count);
	out->remaster_failed = pg_atomic_read_u64(&cluster_grd_state->remaster_failed_count);
	out->shards_remastered = pg_atomic_read_u64(&cluster_grd_state->shards_remastered_count);
	out->holders_redeclared = pg_atomic_read_u64(&cluster_grd_state->holders_redeclared_count);
	out->holders_rebound = pg_atomic_read_u64(&cluster_grd_state->holders_rebound_count);
	out->waiters_requeued = pg_atomic_read_u64(&cluster_grd_state->waiters_requeued_count);
	out->converts_requeued = pg_atomic_read_u64(&cluster_grd_state->converts_requeued_count);
	out->stale_request_drop = pg_atomic_read_u64(&cluster_grd_state->stale_request_drop_count);
	out->rebuild_timeout = pg_atomic_read_u64(&cluster_grd_state->rebuild_timeout_count);
	out->block_path_failclosed
		= pg_atomic_read_u64(&cluster_grd_state->block_path_failclosed_count);
	out->unaffected_holder_survived
		= pg_atomic_read_u64(&cluster_grd_state->unaffected_holder_survived_count);
	out->stale_holder_swept = pg_atomic_read_u64(&cluster_grd_state->stale_holder_swept_count);
}

/*
 * cluster_grd_cleanup_stale_epoch_scoped — spec-4.6 P0#2 (P3).
 *
 *	Pre-remaster hygiene:  remove epoch-stale holders/waiters ONLY on
 *	the affected (about-to-be-remastered) shards, so the new master
 *	does not inherit dirt from an earlier mastership era.  Unaffected
 *	shards are NOT touched here — their live holders are rebound in
 *	place by their owner backends (P5) and the global sweep waits for
 *	the ack barrier (P6).
 */
void
cluster_grd_cleanup_stale_epoch_scoped(uint64 current_epoch, const uint64 *affected_shards)
{
	HASH_SEQ_STATUS status;
	ClusterGrdEntry *entry;
	int swept = 0;

	if (cluster_grd_entry_htab == NULL || affected_shards == NULL)
		return;

	hash_seq_init(&status, cluster_grd_entry_htab);
	while ((entry = (ClusterGrdEntry *)hash_seq_search(&status)) != NULL) {
		int i;

		if (!grd_shard_bitmap_test(affected_shards, cluster_grd_shard_for_resource(&entry->resid)))
			continue;

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
		for (i = 0; i < entry->nwaiters;) {
			if (entry->waiters[i].cluster_epoch < current_epoch) {
				if (i < entry->nwaiters - 1)
					entry->waiters[i] = entry->waiters[entry->nwaiters - 1];
				memset(&entry->waiters[entry->nwaiters - 1], 0, sizeof(entry->waiters[0]));
				entry->nwaiters--;
				swept++;
				continue;
			}
			i++;
		}
		/* spec-5.1b §3 clause 14: convert queue entries are epoch-swept with
		 * holders/waiters (the convert carries its enqueue cluster_epoch).
		 * Latent in 5.1b (converts[] is production-empty until the spec-5.2
		 * producer lands), kept complete for that producer. */
		for (i = 0; i < entry->nconverts;) {
			if (entry->converts[i].cluster_epoch < current_epoch) {
				if (i < entry->nconverts - 1)
					entry->converts[i] = entry->converts[entry->nconverts - 1];
				memset(&entry->converts[entry->nconverts - 1], 0, sizeof(entry->converts[0]));
				entry->nconverts--;
				swept++;
				continue;
			}
			i++;
		}
		SpinLockRelease(&entry->lock);
	}

	if (swept > 0)
		ereport(DEBUG1, (errmsg_internal("cluster_grd_cleanup_stale_epoch_scoped(" UINT64_FORMAT
										 "): swept %d affected-shard slots",
										 current_epoch, swept)));
}

/*
 * cluster_grd_cleanup_stale_epoch_postbarrier — spec-4.6 P0#3 (P6).
 *
 *	Global stale sweep, legal ONLY after the redeclare ack barrier:
 *	every live backend has rebound all its registered grants to the
 *	current epoch, so any remaining old-epoch holder/waiter is provably
 *	unclaimed (its backend exited mid-window, or its release was
 *	epoch-rejected during the window) and MUST be removed — a leaked
 *	holder blocks the resource forever.  Converts carry no epoch and
 *	are not swept here (dead-node converts fall to dead_sweep;  a
 *	live-node stale convert self-resolves through its requester's
 *	timeout + retry).
 */
uint32
cluster_grd_cleanup_stale_epoch_postbarrier(uint64 current_epoch)
{
	HASH_SEQ_STATUS status;
	ClusterGrdEntry *entry;
	uint32 swept = 0;
	uint32 waiters_dropped = 0;

	if (cluster_grd_entry_htab == NULL)
		return 0;

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
		for (i = 0; i < entry->nwaiters;) {
			if (entry->waiters[i].cluster_epoch < current_epoch) {
				if (i < entry->nwaiters - 1)
					entry->waiters[i] = entry->waiters[entry->nwaiters - 1];
				memset(&entry->waiters[entry->nwaiters - 1], 0, sizeof(entry->waiters[0]));
				entry->nwaiters--;
				waiters_dropped++;
				continue;
			}
			i++;
		}
		SpinLockRelease(&entry->lock);
	}

	if (swept > 0)
		pg_atomic_fetch_add_u64(&cluster_grd_state->stale_holder_swept_count, swept);
	if (waiters_dropped > 0)
		pg_atomic_fetch_add_u64(&cluster_grd_state->waiters_requeued_count, waiters_dropped);
	if (swept > 0 || waiters_dropped > 0)
		ereport(DEBUG1,
				(errmsg_internal("cluster_grd_cleanup_stale_epoch_postbarrier(" UINT64_FORMAT
								 "): swept %u leaked holders + %u stale waiters",
								 current_epoch, swept, waiters_dropped)));
	return swept;
}

/*
 * Broadcast PROCSIG_CLUSTER_GRD_REDECLARE to every live backend.
 * Pattern mirrors cluster_reconfig_broadcast_local_procsig.
 */
static int
grd_recovery_broadcast_redeclare(void)
{
	int beid;
	int signaled = 0;
	pid_t self_pid = MyProcPid;

	for (beid = 1; beid <= MaxBackends; beid++) {
		PGPROC *proc = BackendIdGetProc((BackendId)beid);
		pid_t pid;

		if (proc == NULL)
			continue;
		pid = proc->pid;
		if (pid == 0 || pid == self_pid)
			continue;
		(void)SendProcSignal(pid, PROCSIG_CLUSTER_GRD_REDECLARE, (BackendId)beid);
		signaled++;
	}
	return signaled;
}

/*
 * Barrier check:  every live backend has acked the redeclare
 * generation.  Backends born after the broadcast were seeded with the
 * current generation at InitProcess (they hold no stale-epoch grants);
 * backends that exited simply drop out of the scan (their leaked
 * master-side state is exactly what P6 sweeps).
 */
static bool
grd_recovery_barrier_complete(uint64 gen, uint64 episode_epoch)
{
	int beid;
	pid_t self_pid = MyProcPid;

	for (beid = 1; beid <= MaxBackends; beid++) {
		PGPROC *proc = BackendIdGetProc((BackendId)beid);

		if (proc == NULL)
			continue;
		if (proc->pid == 0 || proc->pid == self_pid)
			continue;

		/*
		 * Scope filter:  a proc holding ZERO cluster_registered grants
		 * has nothing to rebind (any later acquire mints the current
		 * epoch), and sinval-registered processes that never run the
		 * generic ProcessInterrupts path (autovacuum launcher, logical
		 * replication launcher) would otherwise wedge the barrier
		 * forever.
		 */
		if (pg_atomic_read_u32(&proc->cluster_grd_registered_count) == 0)
			continue;
		if (pg_atomic_read_u64(&proc->cluster_grd_redeclare_acked) < gen)
			return false;
		/* P0-1:  the ack must be coherent with the LOCKED episode epoch.
		 * A proc that acked an earlier generation under the old epoch and
		 * then short-circuited (acked >= gen via a stale generation race)
		 * must not satisfy the barrier for a newer epoch. */
		if (pg_atomic_read_u64(&proc->cluster_grd_redeclare_acked_epoch) != episode_epoch)
			return false;
	}
	return true;
}

/*
 * spec-4.6 P0#3 cluster gate — announce "my local rebind barrier is
 * complete for `epoch`" to every declared peer (fire-and-forget;  the
 * WAIT_CLUSTER state re-announces each tick).  Standard
 * GesRequestPayload, zero wire-ABI change.
 */
static void
grd_recovery_broadcast_done(uint64 epoch)
{
	GesRequestPayload req;
	uint64 master_gen = cluster_lms_get_shard_master_generation();
	int i;

	memset(&req, 0, sizeof(req));
	req.opcode = GES_REQ_OPCODE_REDECLARE_DONE;
	req.holder_node_id = (uint32)cluster_node_id;
	req.holder_procno = 0;
	req.holder_cluster_epoch_lo = (uint32)(epoch & 0xffffffffu);
	req.holder_cluster_epoch_hi = (uint32)(epoch >> 32);
	req.shard_master_generation_lo = (uint32)(master_gen & 0xffffffffu);
	req.shard_master_generation_hi = (uint32)(master_gen >> 32);

	for (i = 0; i < CLUSTER_MAX_NODES; i++) {
		if (i == cluster_node_id)
			continue;
		if (cluster_conf_lookup_node(i) == NULL)
			continue;
		/* Dead peers neither gate P6 nor drain their inbound — do not
		 * stuff per-tick announcements into the outbound ring for them. */
		if (cluster_cssd_get_peer_state(i) == CLUSTER_CSSD_PEER_DEAD)
			continue;
		(void)cluster_grd_outbound_enqueue_backend_request((uint32)i, &req, sizeof(req));
	}
}

/* REDECLARE_DONE receiver (cluster_ges.c inbound handler). */
void
cluster_grd_recovery_mark_peer_done(int32 node, uint64 epoch)
{
	uint64 prev;

	if (cluster_grd_state == NULL || node < 0 || node >= CLUSTER_MAX_NODES)
		return;
	/* Monotonic max:  late/duplicate announcements never regress. */
	prev = pg_atomic_read_u64(&cluster_grd_state->recovery_done_epoch[node]);
	while (epoch > prev) {
		if (pg_atomic_compare_exchange_u64(&cluster_grd_state->recovery_done_epoch[node], &prev,
										   epoch))
			break;
	}
}

/*
 * spec-4.6 P0-1 / P1-2 — abort the in-flight episode back to IDLE.
 *
 *	Called when a mid-episode epoch bump (P0-1) or a fresh reconfig event
 *	(P1-2:  a SECOND node died during recovery) invalidates the locked
 *	episode.  Affected shards STAY frozen (fail-closed; never opened
 *	half-rebuilt) and the master map keeps its already-remastered state;
 *	resetting recovery_last_event_id forces the IDLE branch to re-consume
 *	the (now-current) event next tick, which re-snapshots the epoch,
 *	bumps a fresh redeclare generation (forcing every backend to re-walk
 *	and rebind under the new epoch), and re-runs P1-P7.  Convergent:  the
 *	epoch eventually stops moving and an episode completes.
 */
static void
grd_recovery_abort_to_idle(void)
{
	pg_atomic_fetch_add_u64(&cluster_grd_state->remaster_failed_count, 1);
	pg_atomic_write_u64(&cluster_grd_state->recovery_last_event_id, 0);
	pg_atomic_write_u32(&cluster_grd_state->recovery_state, (uint32)GRD_RECOVERY_IDLE);
	ereport(DEBUG1,
			(errmsg_internal("cluster_grd_recovery: episode aborted (epoch moved / new event "
							 "mid-recovery); shards stay frozen, re-running under the new epoch")));
}


/*
 * spec-4.7 D2 — survivor block re-declare scan (Q6-A' worker-centric).
 *
 *	BufferDesc.pcm_state is shared per-node authoritative state (not the
 *	backend-private GES LocalLockHash), so the block re-declare is NOT a
 *	backend-cooperative protocol like the GES rebind — the LMON reconfig tick
 *	itself scans the shared buffer pool in bounded chunks and sends one
 *	GCS_BLOCK_REDECLARE per locally-held S/X buffer to the block's current
 *	(remastered) master.  The cursor is LMON-process-local (the LMON aux
 *	process is the sole tick driver — no shmem needed);  it re-arms to 0 each
 *	time a reconfig episode locks a new episode epoch and advances by
 *	GRD_BLOCK_REDECLARE_CHUNK per tick until it reaches NBuffers, so a large
 *	pool scan never blocks the heartbeat (§6 risk mitigation).  Epoch
 *	coherence (L235) is enforced by the WAIT_BARRIER/WAIT_CLUSTER epoch guards
 *	that abort the episode on a mid-episode bump.
 */
#define GRD_BLOCK_REDECLARE_CHUNK 256
static int grd_block_redeclare_cursor = 0;
static uint64 grd_block_redeclare_epoch = 0;
static bool grd_block_redeclare_done = false;

static void
grd_block_redeclare_cb(BufferTag tag, uint8 held_mode, XLogRecPtr page_lsn, void *arg)
{
	uint64 episode_epoch = *(const uint64 *)arg;
	int master = cluster_gcs_lookup_master(tag);

	cluster_gcs_block_send_redeclare(tag, held_mode, page_lsn, episode_epoch, master);
}

/* Non-static (exposed via cluster_grd.h) so the unit test can drive the scan
 * cursor + assert grd_block_redeclare_scan_complete tracks NBuffers without
 * spinning up the full reconfig FSM. */
void
grd_block_redeclare_step(uint64 episode_epoch)
{
	int next;

	/* Re-arm to the start of the pool whenever a fresh episode locks a new
	 * epoch (the previous episode's partial scan is abandoned — the new epoch
	 * re-stamps every re-declare). */
	if (grd_block_redeclare_epoch != episode_epoch) {
		grd_block_redeclare_epoch = episode_epoch;
		grd_block_redeclare_cursor = 0;
		grd_block_redeclare_done = false;
	}

	if (grd_block_redeclare_done)
		return;

	/* Bounded chunk;  scan_chunk caps the cursor at NBuffers and returns the
	 * cursor unchanged once the whole pool has been scanned this episode. */
	next
		= cluster_bufmgr_redeclare_scan_chunk(grd_block_redeclare_cursor, GRD_BLOCK_REDECLARE_CHUNK,
											  grd_block_redeclare_cb, &episode_epoch);
	if (next == grd_block_redeclare_cursor)
		grd_block_redeclare_done = true; /* reached NBuffers — whole pool scanned */
	grd_block_redeclare_cursor = next;
}

/*
 * grd_block_redeclare_scan_complete -- spec-4.7 D2/D7 (P0 code-review fix).
 *
 *	True iff THIS survivor's block re-declare scan for `episode_epoch` has
 *	swept the whole buffer pool (every locally-held S/X block has been
 *	re-declared to its recovery-aware master).  This MUST be a precondition of
 *	announcing REDECLARE_DONE:  otherwise a held block whose buffer sits after
 *	the scan cursor is never re-declared, the episode reaches IDLE, the new
 *	master treats it as cold, and serves it from shared storage while the
 *	original holder still holds X → 8.A double-grant.
 */
bool
grd_block_redeclare_scan_complete(uint64 episode_epoch)
{
	return grd_block_redeclare_epoch == episode_epoch && grd_block_redeclare_done;
}

void
cluster_grd_recovery_lmon_tick(void)
{
	uint32 state;

	if (!cluster_enabled || cluster_grd_state == NULL)
		return;
	if (pg_atomic_read_u32(&cluster_grd_state->master_map_initialized) == 0)
		return;

	state = pg_atomic_read_u32(&cluster_grd_state->recovery_state);

	if (state == (uint32)GRD_RECOVERY_IDLE) {
		ReconfigEvent evt;
		uint64 ev_id;
		int b;

		cluster_reconfig_get_last_event(&evt);
		ev_id = evt.event_id;
		if (ev_id == 0 || ev_id == pg_atomic_read_u64(&cluster_grd_state->recovery_last_event_id)) {
			/*
			 * P1-3 (Fable review) — idle:  track the CURRENT epoch as the
			 * pre-reconfig baseline.  We must NOT trust evt.old_epoch as
			 * the WAIT_EPOCH gate baseline:  for a non-coordinator
			 * survivor it is a fresh read taken when the survivor-role
			 * event was published, and an IC piggyback can deliver the
			 * coordinator's bumped epoch BEFORE this node's own deadband
			 * fires, making old_epoch already == the post-bump epoch and
			 * wedging WAIT_EPOCH forever (cur <= old).  The last stable
			 * idle epoch is the reliable "before reconfig" value.
			 */
			pg_atomic_write_u64(&cluster_grd_state->recovery_event_old_epoch,
								cluster_epoch_get_current());
			return;
		}

		/* P0 accept:  a fresh reconfig event (quorum-accepted dead set).
		 * recovery_event_old_epoch already holds last idle tick's stable
		 * epoch (the genuine pre-reconfig baseline) — do NOT overwrite it
		 * with evt.old_epoch here. */
		pg_atomic_write_u64(&cluster_grd_state->recovery_last_event_id, ev_id);
		for (b = 0; b < (CLUSTER_MAX_NODES + 63) / 64; b++) {
			uint64 word = 0;
			int j;

			for (j = 0; j < 8; j++) {
				int byte_idx = b * 8 + j;

				if (byte_idx < CLUSTER_RECONFIG_DEAD_BITMAP_BYTES)
					word |= ((uint64)evt.dead_bitmap[byte_idx]) << (8 * j);
			}
			pg_atomic_write_u64(&cluster_grd_state->recovery_dead_bitmap[b], word);
		}
		pg_atomic_fetch_add_u64(&cluster_grd_state->remaster_started_count, 1);
		pg_atomic_write_u32(&cluster_grd_state->recovery_state, (uint32)GRD_RECOVERY_WAIT_EPOCH);
		state = (uint32)GRD_RECOVERY_WAIT_EPOCH;
		ereport(DEBUG1,
				(errmsg_internal(
					"cluster_grd_recovery: P0 accept event " UINT64_FORMAT
					" (pre-reconfig baseline epoch " UINT64_FORMAT ")",
					ev_id, pg_atomic_read_u64(&cluster_grd_state->recovery_event_old_epoch))));
		/* fall through — the coordinator already bumped the epoch this
		 * tick, so P1-P5 usually run immediately below. */
	}

	if (state == (uint32)GRD_RECOVERY_WAIT_EPOCH) {
		uint64 cur_epoch = cluster_epoch_get_current();
		uint64 old_epoch = pg_atomic_read_u64(&cluster_grd_state->recovery_event_old_epoch);
		uint64 dead[(CLUSTER_MAX_NODES + 63) / 64];
		uint64 affected[GRD_SHARD_BITMAP_WORDS];
		uint32 nfrozen = 0;
		uint32 moved;
		uint64 gen;
		TimestampTz deadline;
		int i;
		int signaled;

		/*
		 * Gate on the ACCEPTED epoch having advanced past the episode's
		 * old epoch:  the coordinator bumped it earlier this tick;  a
		 * non-coordinator survivor observes it via IC envelope piggyback
		 * a tick or two later.  Running the rebind before the local
		 * epoch advances would mint holders the new master rejects.
		 */
		if (cur_epoch <= old_epoch)
			return;

		for (i = 0; i < (CLUSTER_MAX_NODES + 63) / 64; i++)
			dead[i] = pg_atomic_read_u64(&cluster_grd_state->recovery_dead_bitmap[i]);

		/* P1 freeze affected:  master map not yet flipped, so affected =
		 * shards whose CURRENT master carries a dead bit. */
		memset(affected, 0, sizeof(affected));
		for (i = 0; i < PGRAC_GRD_SHARD_COUNT; i++) {
			uint32 m = pg_atomic_read_u32(&cluster_grd_state->master[i]);

			if (grd_dead_bitmap_test(dead, (int32)m)) {
				cluster_grd_shard_set_phase((uint32)i, GRD_SHARD_FROZEN);
				grd_shard_bitmap_set(affected, (uint32)i);
				nfrozen++;
			}
		}
		ereport(DEBUG1,
				(errmsg_internal("cluster_grd_recovery: P1 freeze %u affected shards", nfrozen)));

		/* P3 scoped stale sweep (P2 dead_sweep ran earlier in the tick,
		 * I47:  dead sweep precedes the epoch bump). */
		cluster_grd_cleanup_stale_epoch_scoped(cur_epoch, affected);

		/* P4 remaster (D2, deterministic from the accepted snapshot). */
		moved = cluster_grd_master_map_remaster(dead, cur_epoch);
		ereport(DEBUG1,
				(errmsg_internal("cluster_grd_recovery: P4 remaster moved %u shards", moved)));

		/* Affected shards now have a live master but unrebuilt holder
		 * state:  REBUILDING (requests stay fenced until P7). */
		for (i = 0; i < PGRAC_GRD_SHARD_COUNT; i++) {
			if (grd_shard_bitmap_test(affected, (uint32)i))
				cluster_grd_shard_set_phase((uint32)i, GRD_SHARD_REBUILDING);
		}

		/* P0-1:  LOCK the episode to this epoch.  Everything downstream
		 * (rebind ack, barrier, REDECLARE_DONE, P6 sweep) is coherent
		 * against this one value;  a mid-episode bump aborts back to IDLE
		 * (see the WAIT_BARRIER / WAIT_CLUSTER epoch-change guards). */
		pg_atomic_write_u64(&cluster_grd_state->recovery_episode_epoch, cur_epoch);

		/* P5 arm the cooperative-rebind barrier + broadcast.  The rebind
		 * is CLUSTER-WIDE (P0#3):  the epoch bump staled EVERY stored
		 * holder identity, not only those on remastered shards. */
		gen = pg_atomic_add_fetch_u64(&cluster_grd_state->recovery_redeclare_generation, 1);
		deadline
			= TimestampTzPlusMilliseconds(GetCurrentTimestamp(), cluster_grd_rebuild_timeout_ms);
		pg_atomic_write_u64(&cluster_grd_state->recovery_barrier_deadline, (uint64)deadline);
		signaled = grd_recovery_broadcast_redeclare();
		pg_atomic_write_u32(&cluster_grd_state->recovery_state, (uint32)GRD_RECOVERY_WAIT_BARRIER);
		ereport(DEBUG1, (errmsg_internal("cluster_grd_recovery: P5 redeclare gen " UINT64_FORMAT
										 " broadcast to %d backends",
										 gen, signaled)));
		return; /* barrier evaluated from the next tick on */
	}

	if (state == (uint32)GRD_RECOVERY_WAIT_BARRIER) {
		uint64 gen = pg_atomic_read_u64(&cluster_grd_state->recovery_redeclare_generation);
		uint64 episode_epoch = pg_atomic_read_u64(&cluster_grd_state->recovery_episode_epoch);

		/* P0-1 epoch-coherence guard:  a SECOND epoch bump landed
		 * mid-episode (e.g. a third node's heartbeat flap re-fired
		 * reconfig).  The holders rebound under episode_epoch are now
		 * stale;  abort to IDLE and re-consume the event under the new
		 * epoch (shards stay frozen the whole time). */
		if (cluster_epoch_get_current() != episode_epoch) {
			grd_recovery_abort_to_idle();
			return;
		}

		/* spec-4.7 D2 — advance the survivor block re-declare scan one chunk
		 * while the GES rebind barrier is still pending (worker-centric, runs
		 * in this tick;  epoch-coherent via the guard just above). */
		grd_block_redeclare_step(episode_epoch);

		/*
		 * spec-4.7 D2/D7 (P0 code-review fix) — REDECLARE_DONE may be announced
		 * ONLY after BOTH the GES rebind barrier AND this survivor's block
		 * re-declare scan are complete.  Without the scan-complete conjunct, a
		 * fast GES barrier would let the episode reach IDLE before a held block
		 * (buffer after the scan cursor) was re-declared → the new master
		 * serves it as cold while the original node still holds X → 8.A
		 * double-grant.  The scan advances one chunk per tick above, so this
		 * just defers the announce until the whole pool is swept.
		 */
		if (grd_recovery_barrier_complete(gen, episode_epoch)
			&& grd_block_redeclare_scan_complete(episode_epoch)) {
			/*
			 * Local rebind barrier complete:  announce to every survivor
			 * (REDECLARE_DONE) and record self for the LOCKED episode
			 * epoch.  P6 must NOT run yet — this master's HTAB holds
			 * grants owned by REMOTE backends, and sweeping before THEIR
			 * barriers complete would delete a live-but-not-yet-rebound
			 * holder (double grant).
			 */
			pg_atomic_write_u64(&cluster_grd_state->recovery_done_epoch[cluster_node_id],
								episode_epoch);
			grd_recovery_broadcast_done(episode_epoch);
			pg_atomic_write_u32(&cluster_grd_state->recovery_state,
								(uint32)GRD_RECOVERY_WAIT_CLUSTER);
			ereport(DEBUG1,
					(errmsg_internal("cluster_grd_recovery: local barrier done "
									 "(gen " UINT64_FORMAT " epoch " UINT64_FORMAT
									 "); announced REDECLARE_DONE, waiting for all survivors",
									 gen, episode_epoch)));
			return;
		}

		if (GetCurrentTimestamp()
			> (TimestampTz)pg_atomic_read_u64(&cluster_grd_state->recovery_barrier_deadline)) {
			TimestampTz deadline;

			/*
			 * Barrier deadline expired:  fail-closed.  Affected shards
			 * STAY frozen (a half-rebuilt shard is never opened — the
			 * 53R9I surface on the request path is the user-visible
			 * fail-closed), the global sweep does NOT run, and the
			 * redeclare is re-broadcast with a fresh deadline so a
			 * slow-but-alive backend converges on its next CFI.
			 * (ereport(FATAL) here would crash-loop the respawned LMON
			 * against the same stalled barrier;  WARNING + retry keeps
			 * the fail-closed posture without self-DoS.)
			 */
			pg_atomic_fetch_add_u64(&cluster_grd_state->rebuild_timeout_count, 1);
			pg_atomic_fetch_add_u64(&cluster_grd_state->remaster_failed_count, 1);
			ereport(WARNING,
					(errcode(ERRCODE_CLUSTER_GRD_SHARD_REMASTERING),
					 errmsg("cluster GRD holder-rebuild barrier timed out; affected shards "
							"stay frozen"),
					 errhint("A backend has not acked the cooperative rebind within "
							 "cluster.grd_rebuild_timeout_ms; re-broadcasting.")));
			deadline = TimestampTzPlusMilliseconds(GetCurrentTimestamp(),
												   cluster_grd_rebuild_timeout_ms);
			pg_atomic_write_u64(&cluster_grd_state->recovery_barrier_deadline, (uint64)deadline);
			(void)grd_recovery_broadcast_redeclare();
		}
		return;
	}

	if (state == (uint32)GRD_RECOVERY_WAIT_CLUSTER) {
		uint64 episode_epoch = pg_atomic_read_u64(&cluster_grd_state->recovery_episode_epoch);
		uint64 dead[(CLUSTER_MAX_NODES + 63) / 64];
		bool all_done = true;
		int i;

		/* P0-1 epoch-coherence guard (same as WAIT_BARRIER):  a
		 * mid-episode bump invalidates every node's locked barrier, so
		 * abort and re-run under the new epoch rather than sweep with a
		 * fresher epoch than the holders were rebound under. */
		if (cluster_epoch_get_current() != episode_epoch) {
			grd_recovery_abort_to_idle();
			return;
		}

		/* spec-4.7 D2 — keep advancing the block re-declare scan after the GES
		 * rebind barrier completed, so a large pool is fully re-declared within
		 * the recovery window (no-op once the cursor reaches NBuffers). */
		grd_block_redeclare_step(episode_epoch);

		for (i = 0; i < (CLUSTER_MAX_NODES + 63) / 64; i++)
			dead[i] = pg_atomic_read_u64(&cluster_grd_state->recovery_dead_bitmap[i]);

		/*
		 * spec-4.11 D1 (3b-4b Part 3) — launch one per-episode online thread-
		 * recovery worker for each in-scope dead origin (idempotent per tick;
		 * a NO-OP out of scope, so the spec-4.7 path is unchanged).  The worker
		 * online-replays the dead thread's WAL data + visibility to shared
		 * storage and publishes the node-local materialization authority; the
		 * D3 gate below then holds the GES shards frozen until it lands.
		 */
		cluster_thread_recovery_launch_workers(dead, (CLUSTER_MAX_NODES + 63) / 64, episode_epoch);

		/*
		 * P6 cluster gate (P0#3):  EVERY survivor (declared ∧ not dead in
		 * this episode) must have announced its local barrier for the
		 * LOCKED episode epoch.  No timeout:  fail-closed — affected
		 * shards stay frozen until the cluster converges;  the lagging
		 * node's own rebuild_timeout WARNING is the observability surface.
		 */
		for (i = 0; i < CLUSTER_MAX_NODES; i++) {
			if (cluster_conf_lookup_node(i) == NULL)
				continue;
			if (grd_dead_bitmap_test(dead, i))
				continue;
			if (pg_atomic_read_u64(&cluster_grd_state->recovery_done_epoch[i]) < episode_epoch) {
				all_done = false;
				break;
			}
		}

		/* Re-announce each tick until released:  REDECLARE_DONE is
		 * fire-and-forget and a lost packet must not wedge a peer. */
		grd_recovery_broadcast_done(episode_epoch);

		if (all_done) {
			uint32 swept;
			uint32 unfrozen = 0;

			/*
			 * spec-4.11 D3 (3b-3) — the re-declare barrier restored the GES/GCS
			 * block PROTOCOL state, but a dead origin's WAL DATA must be online-
			 * replayed to shared storage before any survivor serves it; otherwise
			 * P7 would unfreeze the shards (resuming GES grants) while the dead
			 * thread's committed changes are still missing -> a survivor could act
			 * on a stale resource (8.A).  Stay in WAIT_CLUSTER until every dead
			 * origin's thread recovery is materialized on THIS node.  No timeout:
			 * fail-closed (the per-block cluster_gcs_block_phase_for_tag gate
			 * keeps the same posture for individual blocks; this gate additionally
			 * holds the GES shards frozen).  Out of scope — online_thread_recovery
			 * off (the default) / no shared backend / >2-node — the gate is a
			 * no-op, so the spec-4.7 unfreeze path is unchanged (no regression).
			 * REDECLARE_DONE was re-announced just above, so peers stay converged
			 * while we wait for the replay (the live executor lands in 3b-4).
			 */
			if (cluster_thread_recovery_gate_unfreeze(dead, (CLUSTER_MAX_NODES + 63) / 64)) {
				ereport(DEBUG1,
						(errmsg_internal(
							"cluster_grd_recovery: re-declare barrier passed but a dead origin's "
							"thread recovery is not yet materialized; staying frozen "
							"(epoch " UINT64_FORMAT ")",
							episode_epoch)));
				return;
			}

			/* P6 post-barrier global sweep (P0#3 + P0-1:  legal HERE and
			 * coherent against the LOCKED episode epoch — holders rebound
			 * under it survive, only genuinely older state is removed). */
			swept = cluster_grd_cleanup_stale_epoch_postbarrier(episode_epoch);

			/* P7 unfreeze. */
			for (i = 0; i < PGRAC_GRD_SHARD_COUNT; i++) {
				if (pg_atomic_read_u32(&cluster_grd_state->shard_phase[i])
					!= (uint32)GRD_SHARD_NORMAL) {
					cluster_grd_shard_set_phase((uint32)i, GRD_SHARD_NORMAL);
					unfrozen++;
				}
			}
			pg_atomic_fetch_add_u64(&cluster_grd_state->remaster_done_count, 1);
			pg_atomic_write_u32(&cluster_grd_state->recovery_state, (uint32)GRD_RECOVERY_IDLE);
			ereport(DEBUG1,
					(errmsg_internal("cluster_grd_recovery: cluster gate passed; P6 swept %u "
									 "leaked slots; P7 unfroze %u shards (epoch " UINT64_FORMAT ")",
									 swept, unfrozen, episode_epoch)));
		}
	}
}

/*
 * cluster_grd_entry_rebind_or_insert_holder — spec-4.6 D3 master side.
 *
 *	Match key = (node_id, procno, lockmode) + resid:  a backend holds at
 *	most one grant per (resid, mode) (LOCALLOCK uniqueness), so the
 *	stale epoch/request_id of the old identity carry no information the
 *	master could verify.  Match → overwrite identity in place
 *	(unaffected-shard rebind;  idempotent for retransmits);  no match →
 *	insert (remastered-shard rebuild;  the new master fills holders[]
 *	from re-declarations ONLY).  Defensive double-grant check:  an
 *	insert that CONFLICTS with another backend's live holder is refused
 *	(the pre-crash grant set was compatible by construction, so a
 *	conflict here means a protocol anomaly — fail closed, the sender
 *	stays un-acked and its shard frozen).
 */
ClusterGrdEntryResult
cluster_grd_entry_rebind_or_insert_holder(const ClusterResId *resid,
										  const ClusterGrdHolderId *new_holder,
										  int32 source_node_id, int lockmode)
{
	ClusterGrdEntry *entry = NULL;
	ClusterGrdEntryResult er;
	int i;

	Assert(resid != NULL && new_holder != NULL);

	er = cluster_grd_entry_lookup_or_create(resid, true, &entry);
	if (er != CLUSTER_GRD_ENTRY_OK || entry == NULL)
		return er;

	SpinLockAcquire(&entry->lock);

	/* In-place rebind:  same backend + same mode. */
	for (i = 0; i < entry->ngranted; i++) {
		if ((uint32)entry->holders[i].node_id == new_holder->node_id
			&& entry->holders[i].procno == new_holder->procno
			&& entry->holders[i].mode == (LOCKMODE)lockmode) {
			uint32 rb_shard = cluster_grd_shard_for_resource(resid);

			entry->holders[i].cluster_epoch = new_holder->cluster_epoch;
			entry->holders[i].request_id = new_holder->request_id;
			entry->generation++;
			SpinLockRelease(&entry->lock);
			pg_atomic_fetch_add_u64(&cluster_grd_state->holders_rebound_count, 1);
			/* L13 evidence:  an in-place rebind on a NORMAL-phase shard
			 * is a surviving holder on an UNAFFECTED shard — it was not
			 * deleted by the scoped sweep and stays operable. */
			if (cluster_grd_shard_phase(rb_shard) == GRD_SHARD_NORMAL)
				pg_atomic_fetch_add_u64(&cluster_grd_state->unaffected_holder_survived_count, 1);
			return CLUSTER_GRD_ENTRY_OK;
		}
	}

	/* Defensive double-grant refusal (see header comment). */
	for (i = 0; i < entry->ngranted; i++) {
		if (((uint32)entry->holders[i].node_id != new_holder->node_id
			 || entry->holders[i].procno != new_holder->procno)
			&& !ges_modes_compatible(entry->holders[i].mode,
									 (LOCKMODE)lockmode)) { /* spec-5.1b D1: frozen matrix */
			SpinLockRelease(&entry->lock);
			return CLUSTER_GRD_ENTRY_ERROR;
		}
	}

	if (entry->ngranted >= PGRAC_GRD_MAX_HOLDERS) {
		pg_atomic_fetch_add_u64(&cluster_grd_state->holders_full_count, 1);
		SpinLockRelease(&entry->lock);
		return CLUSTER_GRD_ENTRY_FULL;
	}

	entry->holders[entry->ngranted].node_id = (int32)new_holder->node_id;
	entry->holders[entry->ngranted].procno = new_holder->procno;
	entry->holders[entry->ngranted].cluster_epoch = new_holder->cluster_epoch;
	entry->holders[entry->ngranted].request_id = new_holder->request_id;
	entry->holders[entry->ngranted].mode = (LOCKMODE)lockmode;
	entry->ngranted++;
	entry->generation++;
	SpinLockRelease(&entry->lock);

	(void)source_node_id; /* identity already carried by new_holder */
	pg_atomic_fetch_add_u64(&cluster_grd_state->holders_redeclared_count, 1);
	return CLUSTER_GRD_ENTRY_OK;
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

/* spec-2.25 D13 — RELATION + OBJECT cluster gate hit counter accessor. */
void
cluster_grd_inc_relation_object_cluster_path(void)
{
	if (cluster_grd_state != NULL)
		pg_atomic_fetch_add_u64(&cluster_grd_state->relation_object_cluster_path_count, 1);
}

uint64
cluster_grd_relation_object_cluster_path_count(void)
{
	if (cluster_grd_state == NULL)
		return 0;
	return pg_atomic_read_u64(&cluster_grd_state->relation_object_cluster_path_count);
}

/* spec-2.26 D5 — TRANSACTION cluster gate hit counter accessor. */
void
cluster_grd_inc_transaction_cluster_path(void)
{
	if (cluster_grd_state != NULL)
		pg_atomic_fetch_add_u64(&cluster_grd_state->transaction_cluster_path_count, 1);
}

uint64
cluster_grd_transaction_cluster_path_count(void)
{
	if (cluster_grd_state == NULL)
		return 0;
	return pg_atomic_read_u64(&cluster_grd_state->transaction_cluster_path_count);
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
 *	Conflict judgement uses the frozen 8x8 compatibility matrix via
 *	ges_modes_compatible() (spec-5.1b D1).  Per the spec-5.1a contract
 *	ges_modes_compatible(held, wanted) == !DoLockModesConflict(wanted,
 *	held);  the startup self-check in cluster_ges_mode_backend.c guards
 *	that equivalence against PG's live conflict table.
 * ============================================================ */

ClusterGrdGrantAction
cluster_grd_entry_enqueue_or_grant(const ClusterResId *resid, const ClusterGrdHolderId *holder,
								   int32 source_node_id, uint64 request_id,
								   uint64 shard_master_generation, uint32 request_opcode,
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
		/*
		 * spec-5.1b D1: frozen-matrix conflict check.  Contract (spec-5.1a
		 * §2.2): ges_modes_compatible(held, wanted) == !DoLockModesConflict(
		 * wanted, held);  matrix is symmetric (5.1a U3) so the canonical
		 * (held, wanted) argument order is behaviourally identical.
		 */
		if (ges_modes_compatible(entry->holders[i].mode, (LOCKMODE)lockmode))
			continue;
		/*
		 * spec-5.1c D5: same-backend self-conflict exclusion.  Under PG's
		 * additive lock model a backend holding this resid in one mode may
		 * request a second, conflicting mode (a different LOCALLOCK that
		 * re-enters the cluster gate, e.g. xact-advisory share then
		 * exclusive).  The requester is not a conflict against its own prior
		 * hold; without this the master would enqueue/BAST the request behind
		 * the requester's own holder slot -> cross-node self-deadlock (the
		 * holder waits on itself).  Identity is {node_id, procno}: the full
		 * 4-tuple would carry a fresh request_id for the additive re-acquire
		 * and never match.  (A real cross-node convert -- same backend, mode
		 * change of the SAME hold -- is the spec-5.2 path and self-excludes
		 * in cluster_grd_entry_request_convert.)
		 */
		if ((uint32)entry->holders[i].node_id == holder->node_id
			&& entry->holders[i].procno == holder->procno)
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
	entry->waiters[slot].shard_master_generation = shard_master_generation;
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
	 *
	 * spec-4.6 P0#3 window guard:  a waiter queued under an OLD epoch
	 * must never be promoted — its GRANT reply would echo the stale
	 * holder tuple and be rejected by the requester's inbound
	 * validation, leaving a zombie grant nobody owns.  Drop such
	 * waiters here (the requester self-retries under the current
	 * epoch);  count as stale_request_drop.
	 */
	while (popped < max_out && entry->nwaiters > 0) {
		int chosen = -1;
		uint64 cur_epoch = cluster_epoch_get_current();

		for (int w = 0; w < entry->nwaiters;) {
			if (entry->waiters[w].cluster_epoch < cur_epoch) {
				if (w < entry->nwaiters - 1)
					entry->waiters[w] = entry->waiters[entry->nwaiters - 1];
				memset(&entry->waiters[entry->nwaiters - 1], 0, sizeof(entry->waiters[0]));
				entry->nwaiters--;
				pg_atomic_fetch_add_u64(&cluster_grd_state->stale_request_drop_count, 1);
				continue;
			}
			w++;
		}

		for (int w = 0; w < entry->nwaiters; w++) {
			bool compatible = true;

			for (int h = 0; h < entry->ngranted; h++) {
				/* spec-5.1b D1: frozen-matrix conflict check. */
				if (!ges_modes_compatible(entry->holders[h].mode, entry->waiters[w].mode)) {
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
		granted_out[popped].shard_master_generation
			= entry->waiters[chosen].shard_master_generation;
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


/* ============================================================
 * spec-5.1b — GES lock-conversion (convert) state machine.
 *
 *	D4 partial-order decision + D5 convert-priority drain + anti-
 *	starvation + D6 fail-closed invariants.  All raw mutators (caller
 *	holds entry->lock, mirroring grant_holder).  LOGIC-only in spec-5.1b:
 *	the live cross-node producer (opcode-2) is an explicit REJECT (D3);
 *	the real backend convert trigger + wire/identity land in spec-5.2.
 *
 *	Holder identity for the convert locator is (node_id, procno,
 *	current_mode) — the REDECLARE convention — and the convert's own slot
 *	is self-excluded from the UPGRADE conflict scan, so a holder already
 *	in a self-conflicting mode (SUE/SRE/E/AE) can still upgrade.
 * ============================================================ */

/* Locate the holder being converted by the (node,procno,mode) locator. */
static int
grd_find_holder_slot(ClusterGrdEntry *entry, int32 node_id, uint32 procno, LOCKMODE mode)
{
	for (int i = 0; i < entry->ngranted; i++) {
		if (entry->holders[i].node_id == node_id && entry->holders[i].procno == procno
			&& entry->holders[i].mode == mode)
			return i;
	}
	return -1;
}

ClusterGrdConvertResult
cluster_grd_entry_request_convert(ClusterGrdEntry *entry, const ClusterGrdConvert *req,
								  bool *out_drain_hint)
{
	int hslot;
	ClusterGesConvertClass klass;

	Assert(entry != NULL && req != NULL);
	Assert(cluster_grd_state != NULL);

	if (out_drain_hint != NULL)
		*out_drain_hint = false;

	hslot = grd_find_holder_slot(entry, req->node_id, req->procno, req->current_mode);
	if (hslot < 0) {
		/*
		 * The requester claims to hold (node,procno,current_mode) but the
		 * GRD has no such holder — fail closed (53R74 mapping forward 5.2),
		 * never fabricate a grant.
		 */
		pg_atomic_fetch_add_u64(&cluster_grd_state->convert_illegal_count, 1);
		return CLUSTER_GRD_CONVERT_ILLEGAL;
	}

	klass = ges_mode_convert_class(entry->holders[hslot].mode, req->requested_mode);
	switch (klass) {
	case GES_CONVERT_SAME:
		/* idempotent no-op. */
		pg_atomic_fetch_add_u64(&cluster_grd_state->convert_granted_inplace_count, 1);
		return CLUSTER_GRD_CONVERT_GRANTED_INPLACE;

	case GES_CONVERT_DOWNGRADE:
		/* compat-set widens → always grantable in place; signal drain so
		 * the caller can re-evaluate blocked converts/waiters. */
		entry->holders[hslot].mode = req->requested_mode;
		entry->generation++;
		if (out_drain_hint != NULL)
			*out_drain_hint = true;
		pg_atomic_fetch_add_u64(&cluster_grd_state->convert_granted_inplace_count, 1);
		return CLUSTER_GRD_CONVERT_GRANTED_INPLACE;

	case GES_CONVERT_UPGRADE:
		/*
		 * requested_mode must be compatible with every OTHER holder
		 * (self-excluded by slot) before we may mutate in place; otherwise
		 * enqueue with strict priority over new waiters (anti-starvation).
		 */
		for (int i = 0; i < entry->ngranted; i++) {
			if (i == hslot)
				continue;
			if (!ges_modes_compatible(entry->holders[i].mode, req->requested_mode)) {
				if (entry->nconverts >= PGRAC_GRD_MAX_CONVERTS) {
					pg_atomic_fetch_add_u64(&cluster_grd_state->converts_full_count, 1);
					return CLUSTER_GRD_CONVERT_QUEUE_FULL;
				}
				entry->converts[entry->nconverts++] = *req;
				entry->generation++;
				pg_atomic_fetch_add_u64(&cluster_grd_state->convert_enqueued_count, 1);
				return CLUSTER_GRD_CONVERT_ENQUEUED;
			}
		}
		entry->holders[hslot].mode = req->requested_mode;
		entry->generation++;
		pg_atomic_fetch_add_u64(&cluster_grd_state->convert_granted_inplace_count, 1);
		return CLUSTER_GRD_CONVERT_GRANTED_INPLACE;

	case GES_CONVERT_LATERAL:
	default:
		/* incomparable modes are not a conversion (two distinct locks); a
		 * caller wanting both must take a fresh REQUEST. */
		pg_atomic_fetch_add_u64(&cluster_grd_state->convert_illegal_count, 1);
		return CLUSTER_GRD_CONVERT_ILLEGAL;
	}
}

/* Remove convert slot c (swap-with-last compaction). */
static void
grd_convert_remove(ClusterGrdEntry *entry, int c)
{
	if (c < entry->nconverts - 1)
		entry->converts[c] = entry->converts[entry->nconverts - 1];
	memset(&entry->converts[entry->nconverts - 1], 0, sizeof(ClusterGrdConvert));
	entry->nconverts--;
	entry->generation++;
}

int
cluster_grd_entry_drain_converts_then_waiters(ClusterGrdEntry *entry,
											  ClusterGrdGrantIdentity *granted_out, int max_out)
{
	int n = 0;

	Assert(entry != NULL && granted_out != NULL && max_out > 0);
	Assert(cluster_grd_state != NULL);

	/*
	 * Phase 1 — grant every pending convert now compatible with the other
	 * holders (self-excluded), in place.  Granting a convert changes a
	 * holder mode, which may unblock an earlier-skipped convert, so we
	 * restart the scan after each grant.  Each grant removes one queue
	 * entry, so the loop is bounded.
	 */
	for (int c = 0; c < entry->nconverts && n < max_out;) {
		ClusterGrdConvert *cv = &entry->converts[c];
		int hslot = grd_find_holder_slot(entry, cv->node_id, cv->procno, cv->current_mode);
		bool compatible = true;

		if (hslot < 0) {
			/* the holder being converted vanished (released / swept) — drop
			 * the orphaned convert rather than block the queue forever. */
			grd_convert_remove(entry, c);
			continue; /* re-check slot c (now the swapped-in entry) */
		}
		for (int i = 0; i < entry->ngranted; i++) {
			if (i == hslot)
				continue;
			if (!ges_modes_compatible(entry->holders[i].mode, cv->requested_mode)) {
				compatible = false;
				break;
			}
		}
		if (!compatible) {
			c++; /* still blocked — leave queued, try the next */
			continue;
		}

		entry->holders[hslot].mode = cv->requested_mode;
		granted_out[n].holder.node_id = (uint32)cv->node_id;
		granted_out[n].holder.procno = cv->procno;
		granted_out[n].holder.cluster_epoch = cv->cluster_epoch;
		granted_out[n].holder.request_id = cv->convert_request_id;
		granted_out[n].source_node_id = cv->source_node_id;
		granted_out[n].request_opcode = GES_REQ_OPCODE_CONVERT;
		granted_out[n].shard_master_generation = cv->shard_master_generation;
		granted_out[n].mode = cv->requested_mode;
		n++;
		pg_atomic_fetch_add_u64(&cluster_grd_state->convert_granted_inplace_count, 1);
		grd_convert_remove(entry, c);
		c = 0; /* a holder mode changed — re-scan from the front */
	}

	/*
	 * Phase 2 — pop ONE FIFO REQUEST waiter compatible with every holder
	 * AND every still-pending convert target (anti-starvation: never grant
	 * a waiter that would block a queued convert).
	 */
	if (n < max_out) {
		for (int w = 0; w < entry->nwaiters; w++) {
			bool ok = true;

			for (int h = 0; h < entry->ngranted; h++) {
				if (!ges_modes_compatible(entry->holders[h].mode, entry->waiters[w].mode)) {
					ok = false;
					break;
				}
			}
			for (int cc = 0; ok && cc < entry->nconverts; cc++) {
				if (!ges_modes_compatible(entry->converts[cc].requested_mode,
										  entry->waiters[w].mode)) {
					ok = false;
					break;
				}
			}
			if (!ok)
				continue;

			granted_out[n].holder.node_id = (uint32)entry->waiters[w].node_id;
			granted_out[n].holder.procno = entry->waiters[w].procno;
			granted_out[n].holder.cluster_epoch = entry->waiters[w].cluster_epoch;
			granted_out[n].holder.request_id = entry->waiters[w].request_id;
			granted_out[n].source_node_id = entry->waiters[w].source_node_id;
			granted_out[n].request_opcode = entry->waiters[w].request_opcode;
			granted_out[n].shard_master_generation = entry->waiters[w].shard_master_generation;
			granted_out[n].mode = entry->waiters[w].mode;
			n++;

			if (entry->ngranted < PGRAC_GRD_MAX_HOLDERS) {
				int hs = entry->ngranted++;

				entry->holders[hs].node_id = entry->waiters[w].node_id;
				entry->holders[hs].procno = entry->waiters[w].procno;
				entry->holders[hs].cluster_epoch = entry->waiters[w].cluster_epoch;
				entry->holders[hs].request_id = entry->waiters[w].request_id;
				entry->holders[hs].mode = entry->waiters[w].mode;
			}
			if (w < entry->nwaiters - 1)
				entry->waiters[w] = entry->waiters[entry->nwaiters - 1];
			memset(&entry->waiters[entry->nwaiters - 1], 0, sizeof(ClusterGrdWaiter));
			entry->nwaiters--;
			entry->generation++;
			break; /* one waiter per drain (multi-convert + 1 waiter, §1.2 D5) */
		}
	}
	return n;
}

/*
 * cluster_grd_entry_bast_consume -- advance the pending convert queue then a
 * waiter after a BAST-induced holder release/downgrade (spec-5.1c D3; caller
 * holds entry->lock).
 *
 *	Thin named seam over cluster_grd_entry_drain_converts_then_waiters so the
 *	first live convert producer (spec-5.2) has a single BAST-side entry point
 *	to call.  Until spec-5.2 the convert queue is never populated in
 *	production (cross-node CONVERT is rejected FEATURE_NOT_SUPPORTED), so this
 *	is LOGIC exercised by cluster_unit only.  The live multi-grant release
 *	path keeps the spec-5.1b release_and_pop(granted[1]) signature unchanged.
 *
 *	released_holder identifies the holder that gave way (informational: the
 *	drain re-evaluates the full holder set against each pending convert /
 *	waiter; spec-5.2 may use it to scope the re-evaluation).  Returns the
 *	number of identities written to granted_out (<= max_out; buffer should
 *	hold PGRAC_GRD_MAX_CONVERTS_PUBLIC + 1).
 */
int
cluster_grd_entry_bast_consume(ClusterGrdEntry *entry, const ClusterGrdHolderId *released_holder,
							   ClusterGrdGrantIdentity *granted_out, int max_out)
{
	(void)released_holder; /* informational; consumed by spec-5.2 */

	if (entry == NULL || granted_out == NULL || max_out <= 0)
		return 0;
	return cluster_grd_entry_drain_converts_then_waiters(entry, granted_out, max_out);
}

bool
cluster_grd_entry_request_blocked_by_pending_convert(ClusterGrdEntry *entry, int wanted_mode)
{
	Assert(entry != NULL);
	for (int c = 0; c < entry->nconverts; c++) {
		if (!ges_modes_compatible(entry->converts[c].requested_mode, (LOCKMODE)wanted_mode))
			return true;
	}
	return false;
}

int
cluster_grd_entry_ngranted(ClusterGrdEntry *entry)
{
	Assert(entry != NULL);
	return entry->ngranted;
}

int
cluster_grd_entry_nconverts(ClusterGrdEntry *entry)
{
	Assert(entry != NULL);
	return entry->nconverts;
}

bool
cluster_grd_entry_holder_mode(ClusterGrdEntry *entry, int32 node_id, uint32 procno,
							  LOCKMODE *out_mode)
{
	Assert(entry != NULL);
	for (int i = 0; i < entry->ngranted; i++) {
		if (entry->holders[i].node_id == node_id && entry->holders[i].procno == procno) {
			if (out_mode != NULL)
				*out_mode = entry->holders[i].mode;
			return true;
		}
	}
	return false;
}

uint64
cluster_grd_convert_granted_inplace_count(void)
{
	Assert(cluster_grd_state != NULL);
	return pg_atomic_read_u64(&cluster_grd_state->convert_granted_inplace_count);
}

uint64
cluster_grd_convert_enqueued_count(void)
{
	Assert(cluster_grd_state != NULL);
	return pg_atomic_read_u64(&cluster_grd_state->convert_enqueued_count);
}

uint64
cluster_grd_convert_illegal_count(void)
{
	Assert(cluster_grd_state != NULL);
	return pg_atomic_read_u64(&cluster_grd_state->convert_illegal_count);
}

uint64
cluster_grd_convert_queue_full_count(void)
{
	/* convert queue-full reuses the existing converts_full_count (D9). */
	Assert(cluster_grd_state != NULL);
	return pg_atomic_read_u64(&cluster_grd_state->converts_full_count);
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
	uint64 pending_x_cleared = 0;

	pending_x_cleared = cluster_pcm_lock_clear_pending_x_for_node(dead_node_id);
	if (pending_x_cleared > 0)
		ereport(DEBUG1, (errmsg_internal("cluster_grd_cleanup_on_node_dead(%d): "
										 "cleared " UINT64_FORMAT " PCM pending_x entries",
										 dead_node_id, pending_x_cleared)));

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
	while ((entry = (ClusterGrdEntry *)hash_seq_search(&status)) != NULL) {
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

/*
 * cluster_grd_bast_local_deliver_ok -- pure best-effort delivery guard for a
 * local BAST target (spec-5.1c D5).  Returns true when the master should send
 * PROCSIG_CLUSTER_GES_BAST to the resolved backend.
 *
 *	Guards: (1) procno in range; (2) the holder's recorded cluster_epoch is
 *	the current epoch (drops cross-epoch stale holders, mirroring the CANCEL
 *	path); (3) the resolved slot has a live backend (pid != 0 and a valid
 *	BackendId).  This is best-effort: it does NOT detect same-epoch procno
 *	reuse (an exited holder whose slot a new backend now occupies) -- that is
 *	acceptable because BAST only sets an advisory flag (no proactive release,
 *	I85), so a spurious flag has no correctness effect.  No shmem access, so
 *	it is unit-testable standalone (the caller resolves pid/BackendId via
 *	GetPGProcByNumber and SendProcSignal re-checks pss_pid == pid).
 */
bool
cluster_grd_bast_local_deliver_ok(uint32 procno, int proc_count, uint64 holder_epoch,
								  uint64 current_epoch, int target_pid, int target_backendid)
{
	if (proc_count <= 0 || procno >= (uint32)proc_count)
		return false; /* procno out of range */
	if (holder_epoch != current_epoch)
		return false; /* stale (cross-epoch) holder */
	if (target_pid == 0 || target_backendid == InvalidBackendId)
		return false; /* no live backend at this slot */
	return true;
}

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

	/* spec-4.6 D3 — cooperative holder rebind.  Clear-then-work (a new
	 * broadcast re-arms the flag);  the walker is no-throw and acks the
	 * barrier generation only on full success, so a partial pass simply
	 * leaves this backend un-acked until LMON's re-broadcast. */
	if (cluster_grd_redeclare_pending) {
		cluster_grd_redeclare_pending = false;
		cluster_grd_redeclare_all_registered();
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
		if (entry->ngranted == 0 && entry->nwaiters == 0 && entry->nconverts == 0
			&& entry->nreservations == 0) {
			SpinLockRelease(&entry->lock);
			(void)hash_search_with_hash_value(cluster_grd_entry_htab, resid, hashvalue, HASH_REMOVE,
											  &found);
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
	GesRequestPayload release_payloads[PGRAC_GRD_MAX_HOLDERS];
	int n_release = 0;
	ClusterResId entry_resid;

	Assert(entry != NULL);

	SpinLockAcquire(&entry->lock);

	/* HC26 I-cleanup-3 — each remove path matches by content; absent → continue. */
	for (int i = entry->ngranted - 1; i >= 0; i--) {
		bool match = false;

		if (dead_procno >= 0 && entry->holders[i].node_id == (int32)cluster_node_id
			&& entry->holders[i].procno == (uint32)dead_procno)
			match = true;
		if (dead_node_id >= 0 && entry->holders[i].node_id == dead_node_id)
			match = true;
		if (!match)
			continue;

		/* Stash a full GES_RELEASE payload for post-lock enqueue. */
		if (n_release < PGRAC_GRD_MAX_HOLDERS) {
			memset(&release_payloads[n_release], 0, sizeof(GesRequestPayload));
			release_payloads[n_release].opcode = GES_REQ_OPCODE_RELEASE;
			release_payloads[n_release].lockmode = (uint32)entry->holders[i].mode;
			release_payloads[n_release].holder_node_id = (uint32)entry->holders[i].node_id;
			release_payloads[n_release].holder_procno = entry->holders[i].procno;
			release_payloads[n_release].holder_cluster_epoch_lo
				= (uint32)(entry->holders[i].cluster_epoch & 0xffffffffu);
			release_payloads[n_release].holder_cluster_epoch_hi
				= (uint32)(entry->holders[i].cluster_epoch >> 32);
			release_payloads[n_release].holder_request_id_lo
				= (uint32)(entry->holders[i].request_id & 0xffffffffu);
			release_payloads[n_release].holder_request_id_hi
				= (uint32)(entry->holders[i].request_id >> 32);
			memcpy(release_payloads[n_release].resid, &entry->resid,
				   sizeof(release_payloads[n_release].resid));
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

		if (dead_procno >= 0 && entry->waiters[i].node_id == (int32)cluster_node_id
			&& entry->waiters[i].procno == (uint32)dead_procno)
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

		/* spec-5.1b §3 clause 14: convert queue entries sweep with holders/
		 * waiters — match the local-backend-death case symmetrically (the
		 * convert locator carries (node_id, procno)).  Latent in 5.1b
		 * (converts[] is production-empty: opcode-2 is rejected, no live
		 * producer until spec-5.2), but keeps the sweep complete for the
		 * 5.2 producer. */
		if (dead_procno >= 0 && entry->converts[i].node_id == (int32)cluster_node_id
			&& entry->converts[i].procno == (uint32)dead_procno)
			match = true;
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
	for (int i = entry->nreservations - 1; i >= 0; i--) {
		bool match = false;

		if (dead_procno >= 0 && entry->reservations[i].id.node_id == (uint32)cluster_node_id
			&& entry->reservations[i].id.procno == (uint32)dead_procno)
			match = true;
		if (dead_node_id >= 0 && entry->reservations[i].id.node_id == (uint32)dead_node_id)
			match = true;
		if (!match)
			continue;

		if (i < entry->nreservations - 1)
			entry->reservations[i] = entry->reservations[entry->nreservations - 1];
		memset(&entry->reservations[entry->nreservations - 1], 0, sizeof(entry->reservations[0]));
		entry->nreservations--;
		removed++;
	}

	/* HC25 — bump generation if any mutation; serves as ABA marker for
	 * concurrent cleanup detection (other paths see new generation and
	 * recheck before HASH_REMOVE). */
	if (removed > 0)
		entry->generation++;

	became_empty = (entry->ngranted == 0 && entry->nwaiters == 0 && entry->nconverts == 0
					&& entry->nreservations == 0);

	memcpy(&entry_resid, &entry->resid, sizeof(entry_resid));

	SpinLockRelease(&entry->lock);

	/* Enqueue one full GES_RELEASE per removed real holder.  Route to the
	 * resource master, not to the holder node. */
	if (n_release > 0) {
		int32 master = cluster_grd_lookup_master(&entry_resid);

		if (master >= 0 && master != cluster_node_id) {
			for (int i = 0; i < n_release; i++)
				cluster_grd_outbound_enqueue_cleanup_release((uint32)master, &release_payloads[i],
															 sizeof(GesRequestPayload));
		}
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
	while ((entry = (ClusterGrdEntry *)hash_seq_search(&status)) != NULL) {
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
		cluster_lmd_cleanup_on_backend_exit_count_inc((uint64)swept);
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

	alive = (uint8 *)palloc0((Size)n_alive_max);

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
	while ((entry = (ClusterGrdEntry *)hash_seq_search(&status)) != NULL) {
		uint32 stale_procno = (uint32)-1;
		uint32 i;

		SpinLockAcquire(&entry->lock);
		for (i = 0; i < (uint32)entry->ngranted; i++) {
			if (entry->holders[i].node_id != (int32)cluster_node_id)
				continue;
			if (entry->holders[i].procno < (uint32)n_alive_max
				&& alive[entry->holders[i].procno] == 0) {
				stale_procno = entry->holders[i].procno;
				break;
			}
		}
		SpinLockRelease(&entry->lock);

		if (stale_procno != (uint32)-1)
			total += cluster_grd_entry_cleanup_guarded(entry, (int)stale_procno, -1);
	}

	pfree(alive);
	return total;
}

void
cluster_grd_cleanup_on_backend_exit_callback(int code, Datum arg)
{
	(void)code;
	(void)arg;
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
