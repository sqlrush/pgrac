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
#include "port/pg_crc32c.h"
#include "storage/bufpage.h"
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
	/* GCS-race round-2 RC-F: completion-proof outcomes. */
	pg_atomic_uint64 done_marked_count;	  /* identity-verified DONE stamped */
	pg_atomic_uint64 done_mismatch_count; /* DONE dropped: miss / identity / in-flight */
	/* GCS-race round-2 review F5 (calibration 2): registration routing. */
	pg_atomic_uint64 hint_violation_count;	 /* capable peer, hint 0 / over-max: denied */
	pg_atomic_uint64 legacy_pin_count;		 /* no-capability peer: protocol-max pin */
	pg_atomic_uint64 pcm_x_stage_count;		 /* RESERVED -> immutable image */
	pg_atomic_uint64 pcm_x_replay_count;	 /* exact image replay */
	pg_atomic_uint64 pcm_x_release_count;	 /* exact terminal release */
	pg_atomic_uint64 pcm_x_failclosed_count; /* malformed, stale, or full image leg */
	pg_atomic_uint32 entry_count;			 /* live in-flight + completed entries */
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
/* Process-local LMS cursors.  Each DATA worker owns exactly one shard in one
 * process, so this adds no shared-memory region or cross-worker authority. */
static GcsBlockDedupKey dedup_pcm_x_work_cursor[CLUSTER_LMS_MAX_WORKERS];
static bool dedup_pcm_x_work_cursor_valid[CLUSTER_LMS_MAX_WORKERS];
/* When both classes stay runnable, alternate the single-work LMS tick budget.
 * The initial false value preserves RESERVED-first admission while bounding a
 * READY replay behind at most one reservation tick. */
static bool dedup_pcm_x_prefer_ready_next[CLUSTER_LMS_MAX_WORKERS];
/* Process-local wake hint.  An empty scan clears it; exact reserve/rearm sets
 * it.  This keeps ordinary GCS traffic from rescanning every 8KB entry on
 * every LMS tick when no PCM-X image work exists. */
static bool dedup_pcm_x_work_pending[CLUSTER_LMS_MAX_WORKERS];

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
	memset(dedup_pcm_x_work_cursor, 0, sizeof(dedup_pcm_x_work_cursor));
	memset(dedup_pcm_x_work_cursor_valid, 0, sizeof(dedup_pcm_x_work_cursor_valid));
	memset(dedup_pcm_x_prefer_ready_next, 0, sizeof(dedup_pcm_x_prefer_ready_next));
	memset(dedup_pcm_x_work_pending, 0, sizeof(dedup_pcm_x_work_pending));
	for (i = 0; i < n; i++)
		dedup_pcm_x_work_pending[i] = true;

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
			pg_atomic_init_u64(&shard->done_marked_count, 0);
			pg_atomic_init_u64(&shard->done_mismatch_count, 0);
			pg_atomic_init_u64(&shard->hint_violation_count, 0);
			pg_atomic_init_u64(&shard->legacy_pin_count, 0);
			pg_atomic_init_u64(&shard->pcm_x_stage_count, 0);
			pg_atomic_init_u64(&shard->pcm_x_replay_count, 0);
			pg_atomic_init_u64(&shard->pcm_x_release_count, 0);
			pg_atomic_init_u64(&shard->pcm_x_failclosed_count, 0);
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
 * PCM-X image-entry validation.
 * ============================================================ */

static void
dedup_pcm_x_note_failclosed(ClusterGcsBlockDedupShard *shard)
{
	if (shard != NULL)
		pg_atomic_fetch_add_u64(&shard->pcm_x_failclosed_count, 1);
}

/* The generic smart-fusion metadata cell is eight bytes and PCM-X entry
 * kinds never use it.  Keep the exact revoke token there without changing
 * the fixed shared-memory entry layout. */
static uint64
dedup_pcm_x_reservation_token_get(const GcsBlockDedupEntry *entry)
{
	uint64 token;

	memcpy(&token, &entry->has_sf_dep, sizeof(token));
	return token;
}

static void
dedup_pcm_x_reservation_token_set(GcsBlockDedupEntry *entry, uint64 token)
{
	memcpy(&entry->has_sf_dep, &token, sizeof(token));
}

static bool
dedup_pcm_x_source_state_valid(uint8 pcm_state)
{
	return pcm_state == (uint8)PCM_STATE_N || pcm_state == (uint8)PCM_STATE_S
		   || pcm_state == (uint8)PCM_STATE_X;
}

static uint32
dedup_pcm_x_block_checksum(const char *block_data)
{
	pg_crc32c crc;

	INIT_CRC32C(crc);
	COMP_CRC32C(crc, block_data, GCS_BLOCK_DATA_SIZE);
	FIN_CRC32C(crc);
	return (uint32)crc;
}

static bool
dedup_pcm_x_key_valid(const GcsBlockDedupKey *key)
{
	return key != NULL && key->origin_node_id < PCM_X_PROTOCOL_NODE_LIMIT
		   && key->requester_backend_id > 0
		   && cluster_pcm_x_image_id_decode(key->request_id, NULL, NULL);
}

static bool
dedup_pcm_x_binding_valid(const GcsBlockDedupKey *key, const BufferTag *tag,
						  const GcsBlockPcmXImageBinding *binding, bool reserved)
{
	const PcmXTicketRef *ref;
	const PcmXImageToken *image;
	int32 requester_node;
	int32 requester_backend_id;

	if (!dedup_pcm_x_key_valid(key) || tag == NULL || binding == NULL
		|| binding->master_session == 0)
		return false;

	ref = &binding->identity.ref;
	image = &binding->identity.image;
	if (memcmp(&ref->identity.tag, tag, sizeof(*tag)) != 0
		|| ref->identity.node_id != (int32)key->origin_node_id
		|| ref->identity.cluster_epoch != key->cluster_epoch || ref->identity.wait_seq == 0
		|| ref->handle.ticket_id == 0 || ref->handle.queue_generation == 0
		|| ref->grant_generation == 0 || image->image_id != key->request_id
		|| image->source_node >= PCM_X_PROTOCOL_NODE_LIMIT || cluster_node_id < 0
		|| cluster_node_id >= PCM_X_PROTOCOL_NODE_LIMIT
		|| image->source_node != (uint32)cluster_node_id)
		return false;

	if (!cluster_gcs_requester_id_decode(ref->identity.request_id, &requester_node,
										 &requester_backend_id, NULL)
		|| requester_node != ref->identity.node_id
		|| requester_backend_id != key->requester_backend_id)
		return false;

	if (reserved && (image->page_scn != 0 || image->page_lsn != 0 || image->page_checksum != 0))
		return false;
	return true;
}

/* Generic entries retain the established signed TTL meaning of
 * pinned_lifetime_us.  PCM-X entries are excluded from generic GC, so that
 * same fixed 8-byte cell carries their exact required source SCN without
 * changing the 8472-byte entry ABI. */
static uint64
dedup_pcm_x_required_page_scn_get(const GcsBlockDedupEntry *entry)
{
	return entry != NULL ? (uint64)entry->pinned_lifetime_us : 0;
}

static void
dedup_pcm_x_binding_from_entry(const GcsBlockDedupEntry *entry, GcsBlockPcmXImageBinding *binding)
{
	binding->identity = entry->payload_meta.pcm_x_identity;
	binding->master_session = entry->pcm_x_master_session;
	binding->required_page_scn = dedup_pcm_x_required_page_scn_get(entry);
}

static bool
dedup_pcm_x_reservation_equal(const GcsBlockDedupEntry *entry,
							  const GcsBlockPcmXImageBinding *binding)
{
	const GcsBlockPcmXImageIdentity *stored = &entry->payload_meta.pcm_x_identity;
	const GcsBlockPcmXImageIdentity *incoming = &binding->identity;

	return entry->pcm_x_master_session == binding->master_session
		   && dedup_pcm_x_required_page_scn_get(entry) == binding->required_page_scn
		   && memcmp(&stored->ref, &incoming->ref, sizeof(stored->ref)) == 0
		   && stored->image.image_id == incoming->image.image_id
		   && stored->image.source_own_generation == incoming->image.source_own_generation
		   && stored->image.source_node == incoming->image.source_node;
}

static bool
dedup_pcm_x_ready_payload_valid(const GcsBlockDedupKey *key, const BufferTag *tag,
								const GcsBlockPcmXImageBinding *binding,
								const GcsBlockReplyHeader *reply_header, const char *block_data)
{
	const PcmXImageToken *image;
	PageHeaderData page_header;
	static const uint8 zero_reserved[sizeof(reply_header->reserved_0)] = { 0 };

	if (!dedup_pcm_x_binding_valid(key, tag, binding, false) || reply_header == NULL
		|| block_data == NULL)
		return false;

	image = &binding->identity.image;
	if (reply_header->request_id != key->request_id || reply_header->page_lsn != image->page_lsn
		|| reply_header->epoch != key->cluster_epoch
		|| reply_header->checksum != image->page_checksum
		|| reply_header->sender_node != (int32)image->source_node
		|| reply_header->requester_backend_id != key->requester_backend_id
		|| reply_header->transition_id != (uint8)PCM_TRANS_N_TO_S
		|| reply_header->status != (uint8)GCS_BLOCK_REPLY_READ_IMAGE_FROM_XHOLDER
		|| GcsBlockReplyHeaderGetForwardingMasterNode(reply_header)
			   != GCS_BLOCK_REPLY_NO_FORWARDING_MASTER
		|| memcmp(reply_header->reserved_0, zero_reserved, sizeof(zero_reserved)) != 0)
		return false;

	memcpy(&page_header, block_data, sizeof(page_header));
	return image->page_lsn == (uint64)PageXLogRecPtrGet(page_header.pd_lsn)
		   && image->page_scn == (uint64)page_header.pd_block_scn
		   && image->page_checksum == dedup_pcm_x_block_checksum(block_data);
}

static bool
dedup_pcm_x_entry_payload_valid(const GcsBlockDedupKey *key, const BufferTag *tag,
								const GcsBlockDedupEntry *entry)
{
	GcsBlockPcmXImageBinding binding;

	dedup_pcm_x_binding_from_entry(entry, &binding);
	return entry->transition_id == (uint8)PCM_TRANS_N_TO_S
		   && entry->status == (uint8)GCS_BLOCK_REPLY_READ_IMAGE_FROM_XHOLDER
		   && dedup_pcm_x_ready_payload_valid(key, tag, &binding, &entry->reply_header,
											  entry->block_data);
}


static bool
dedup_pcm_x_entry_ready_valid(const GcsBlockDedupKey *key, const BufferTag *tag,
							  const GcsBlockDedupEntry *entry)
{
	return entry->entry_kind == GCS_BLOCK_DEDUP_ENTRY_PCM_X_IMAGE
		   && dedup_pcm_x_entry_payload_valid(key, tag, entry);
}


static bool
dedup_pcm_x_entry_drained_valid(const GcsBlockDedupKey *key, const BufferTag *tag,
								const GcsBlockDedupEntry *entry)
{
	return entry->entry_kind == GCS_BLOCK_DEDUP_ENTRY_PCM_X_DRAINED && entry->request_flags > 0
		   && entry->request_flags <= (uint8)PCM_X_PROTOCOL_NODE_LIMIT
		   && dedup_pcm_x_reservation_token_get(entry) == 0
		   && dedup_pcm_x_entry_payload_valid(key, tag, entry);
}


/* ============================================================
 * Public API.
 * ============================================================ */

GcsBlockDedupResult
cluster_gcs_block_dedup_lookup_or_register(int worker_id, const GcsBlockDedupKey *key,
										   BufferTag tag, uint8 transition_id,
										   uint32 requester_lifetime_hint_ms,
										   bool requester_done_capable,
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
	if (cluster_pcm_x_image_id_decode(key->request_id, NULL, NULL)) {
		dedup_pcm_x_note_failclosed(shard);
		return GCS_BLOCK_DEDUP_VALIDATION_FAIL;
	}

	LWLockAcquire(&shard->lock.lock, LW_EXCLUSIVE);

	entry = (GcsBlockDedupEntry *)hash_search(htab, key, HASH_FIND, &found);

	if (found) {
		if (entry->entry_kind != GCS_BLOCK_DEDUP_ENTRY_GENERIC) {
			dedup_pcm_x_note_failclosed(shard);
			LWLockRelease(&shard->lock.lock);
			return GCS_BLOCK_DEDUP_VALIDATION_FAIL;
		}

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
		 * from genuine CACHED_REPLY via the status field.
		 *
		 * A READ_IMAGE_FROM_XHOLDER entry is a forward marker TOO when it
		 * came from the xheld-read FORWARD install (no page payload;
		 * forwarding_master_node stamped).  Classifying that marker as
		 * CACHED_REPLY resends it payload-less: its header checksum was
		 * never computed (0), and the 31-hash of the all-zero page is also
		 * 0, so the resend VERIFIES at the requester and installs a zero
		 * page — a PageIsNew false-empty read (8.A).  The master-DIRECT
		 * xheld serve also installs READ_IMAGE but WITH the page and with
		 * NO_FORWARDING_MASTER — that one is a genuine cached reply, so the
		 * forwarding_master_node field is the discriminator. */
		if (entry->status == (uint8)GCS_BLOCK_REPLY_GRANTED_FROM_HOLDER
			|| entry->status == (uint8)GCS_BLOCK_REPLY_X_GRANTED_FROM_HOLDER
			|| (entry->status == (uint8)GCS_BLOCK_REPLY_READ_IMAGE_FROM_XHOLDER
				&& GcsBlockReplyHeaderGetForwardingMasterNode(&entry->reply_header)
					   != GCS_BLOCK_REPLY_NO_FORWARDING_MASTER)) {
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

	/*
	 * MISS path — registration-time TTL routing (review F5 / calibration 2).
	 * Validate the wire hint BEFORE inserting: a violating request never
	 * claims a slot.  A GCS_DONE_V1-capable peer MUST carry its legal
	 * lifetime -- hint 0 (protocol violation) or a hint above what any
	 * legal configuration could produce (would pin the 8KB slot for days)
	 * is counted and DENIED, never served.
	 */
	if (requester_done_capable
		&& (requester_lifetime_hint_ms == 0
			|| (int64)requester_lifetime_hint_ms > GCS_BLOCK_DEDUP_MAX_PROTOCOL_LIFETIME_MS)) {
		pg_atomic_fetch_add_u64(&shard->hint_violation_count, 1);
		LWLockRelease(&shard->lock.lock);
		return GCS_BLOCK_DEDUP_VALIDATION_FAIL;
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
	entry->entry_kind = GCS_BLOCK_DEDUP_ENTRY_GENERIC;
	entry->completed_at_ts = 0;
	entry->registered_at_ts = GetCurrentTimestamp();

	/*
	 * GCS-race round-2 RC-F + review F5: pin the entry's whole TTL posture
	 * NOW, by capability.  A capable peer's validated wire hint is its own
	 * legal-request lifetime (backoff + reply-timeout budget); a legacy
	 * peer's window is unknowable (its GUCs may all be longer than this
	 * master's), so it pins the PROTOCOL-MAXIMUM lifetime -- an ~1 h
	 * availability cost under cap pressure (DENIED FULL), never an early
	 * reclaim (the master-formula fallback re-opened the re-execution P0).
	 * Sweep and reclaim consume only these pinned values -- a later SUSET
	 * change on the master can never re-shorten the window a live request
	 * registered under.
	 */
	entry->done_at_ts = 0;
	if (requester_done_capable)
		entry->pinned_lifetime_us = (int64)requester_lifetime_hint_ms * 1000 * 2;
	else {
		entry->pinned_lifetime_us = GCS_BLOCK_DEDUP_MAX_PROTOCOL_LIFETIME_MS * 1000 * 2;
		pg_atomic_fetch_add_u64(&shard->legacy_pin_count, 1);
	}
	entry->pinned_done_linger_us
		= (int64)(cluster_gcs_reply_timeout_ms > 0 ? cluster_gcs_reply_timeout_ms : 5000) * 1000
		  * 2;

	pg_atomic_fetch_add_u32(&shard->entry_count, 1);
	pg_atomic_fetch_add_u64(&shard->miss_count, 1);
	result = GCS_BLOCK_DEDUP_MISS_REGISTERED;

	LWLockRelease(&shard->lock.lock);
	return result;
}

static bool
dedup_pending_x_denial_is_exact(const GcsBlockDedupEntry *entry)
{
	const GcsBlockReplyHeader *header = &entry->reply_header;

	return entry->entry_kind == GCS_BLOCK_DEDUP_ENTRY_GENERIC
		   && entry->transition_id == (uint8)PCM_TRANS_N_TO_S
		   && entry->status == (uint8)GCS_BLOCK_REPLY_DENIED_PENDING_X
		   && entry->completed_at_ts != 0 && header->request_id == entry->key.request_id
		   && header->epoch == entry->key.cluster_epoch && header->sender_node == cluster_node_id
		   && header->requester_backend_id == entry->key.requester_backend_id
		   && header->transition_id == (uint8)PCM_TRANS_N_TO_S
		   && header->status == (uint8)GCS_BLOCK_REPLY_DENIED_PENDING_X
		   && GcsBlockReplyHeaderGetForwardingMasterNode(header)
				  == GCS_BLOCK_REPLY_NO_FORWARDING_MASTER;
}

static bool
dedup_pending_x_entry_has_legacy_s_right(const GcsBlockDedupEntry *entry)
{
	GcsBlockReplyStatus status;

	if (entry->completed_at_ts == 0)
		return true;
	status = (GcsBlockReplyStatus)entry->status;
	return status == GCS_BLOCK_REPLY_GRANTED || status == GCS_BLOCK_REPLY_GRANTED_STORAGE_FALLBACK
		   || status == GCS_BLOCK_REPLY_GRANTED_FROM_HOLDER
		   || status == GCS_BLOCK_REPLY_READ_IMAGE_FROM_XHOLDER
		   || status == GCS_BLOCK_REPLY_S_GRANTED_XHOLDER_DOWNGRADE;
}

static void
dedup_pending_x_install_denial(GcsBlockDedupEntry *entry)
{
	GcsBlockReplyHeader denial;

	memset(&denial, 0, sizeof(denial));
	denial.request_id = entry->key.request_id;
	denial.epoch = entry->key.cluster_epoch;
	denial.sender_node = cluster_node_id;
	denial.requester_backend_id = entry->key.requester_backend_id;
	denial.transition_id = (uint8)PCM_TRANS_N_TO_S;
	denial.status = (uint8)GCS_BLOCK_REPLY_DENIED_PENDING_X;
	GcsBlockReplyHeaderSetForwardingMasterNode(&denial, GCS_BLOCK_REPLY_NO_FORWARDING_MASTER);

	entry->status = (uint8)GCS_BLOCK_REPLY_DENIED_PENDING_X;
	entry->reply_header = denial;
	entry->has_sf_dep = false;
	entry->sf_flags = 0;
	entry->sf_dep_count = 0;
	cluster_sf_dep_vec_reset(&entry->payload_meta.sf_dep_vec);
	memset(entry->block_data, 0, sizeof(entry->block_data));
	entry->completed_at_ts = GetCurrentTimestamp();
	entry->done_at_ts = 0;
}

/* PCM-X queue arbitration must revoke the right of an older legacy reader
 * before type-49 can wait on that reader's GRANT_PENDING reservation.  The
 * scan returns at most one newly terminated identity per call so the caller
 * can send it without a bounded/sentinel array; after all live rights are
 * gone it returns one unacknowledged cached denial for loss recovery. */
GcsBlockPendingXDenyResult
cluster_gcs_block_dedup_pending_x_deny_next(int worker_id, const BufferTag *tag,
											GcsBlockDedupEntry *denied_out)
{
	ClusterGcsBlockDedupShard *shard;
	HTAB *htab = NULL;
	HASH_SEQ_STATUS scan;
	GcsBlockDedupEntry *entry;
	GcsBlockDedupEntry replay;
	bool have_replay = false;

	Assert(tag != NULL);
	Assert(denied_out != NULL);
	if (tag == NULL || denied_out == NULL)
		return GCS_BLOCK_PENDING_X_DENY_INVALID;
	memset(denied_out, 0, sizeof(*denied_out));

	shard = cluster_gcs_block_dedup_resolve_shard(worker_id, &htab);
	if (shard == NULL)
		return GCS_BLOCK_PENDING_X_DENY_INVALID;

	LWLockAcquire(&shard->lock.lock, LW_EXCLUSIVE);
	hash_seq_init(&scan, htab);
	while ((entry = (GcsBlockDedupEntry *)hash_seq_search(&scan)) != NULL) {
		if (entry->entry_kind != GCS_BLOCK_DEDUP_ENTRY_GENERIC
			|| entry->transition_id != (uint8)PCM_TRANS_N_TO_S
			|| memcmp(&entry->tag, tag, sizeof(*tag)) != 0 || entry->done_at_ts != 0)
			continue;

		if (dedup_pending_x_denial_is_exact(entry)) {
			if (!have_replay) {
				replay = *entry;
				have_replay = true;
			}
			continue;
		}
		if (entry->status == (uint8)GCS_BLOCK_REPLY_DENIED_PENDING_X
			&& entry->completed_at_ts != 0) {
			dedup_pcm_x_note_failclosed(shard);
			hash_seq_term(&scan);
			LWLockRelease(&shard->lock.lock);
			return GCS_BLOCK_PENDING_X_DENY_INVALID;
		}
		if (!dedup_pending_x_entry_has_legacy_s_right(entry))
			continue;

		dedup_pending_x_install_denial(entry);
		*denied_out = *entry;
		hash_seq_term(&scan);
		LWLockRelease(&shard->lock.lock);
		return GCS_BLOCK_PENDING_X_DENY_NEW;
	}

	if (have_replay)
		*denied_out = replay;
	LWLockRelease(&shard->lock.lock);
	return have_replay ? GCS_BLOCK_PENDING_X_DENY_REPLAY : GCS_BLOCK_PENDING_X_DENY_NOT_FOUND;
}

GcsBlockPendingXDenyResult
cluster_gcs_block_dedup_pending_x_deny_exact(int worker_id, const GcsBlockDedupKey *key,
											 const BufferTag *tag, uint8 transition_id,
											 GcsBlockDedupEntry *denied_out)
{
	ClusterGcsBlockDedupShard *shard;
	HTAB *htab = NULL;
	GcsBlockDedupEntry *entry;
	bool found = false;

	Assert(key != NULL);
	Assert(tag != NULL);
	Assert(denied_out != NULL);
	if (key == NULL || tag == NULL || denied_out == NULL
		|| transition_id != (uint8)PCM_TRANS_N_TO_S)
		return GCS_BLOCK_PENDING_X_DENY_INVALID;
	memset(denied_out, 0, sizeof(*denied_out));

	shard = cluster_gcs_block_dedup_resolve_shard(worker_id, &htab);
	if (shard == NULL)
		return GCS_BLOCK_PENDING_X_DENY_INVALID;
	LWLockAcquire(&shard->lock.lock, LW_EXCLUSIVE);
	entry = (GcsBlockDedupEntry *)hash_search(htab, key, HASH_FIND, &found);
	if (!found || entry->entry_kind != GCS_BLOCK_DEDUP_ENTRY_GENERIC
		|| memcmp(&entry->tag, tag, sizeof(*tag)) != 0 || entry->transition_id != transition_id) {
		if (found && entry->entry_kind != GCS_BLOCK_DEDUP_ENTRY_GENERIC)
			dedup_pcm_x_note_failclosed(shard);
		LWLockRelease(&shard->lock.lock);
		return GCS_BLOCK_PENDING_X_DENY_INVALID;
	}
	if (dedup_pending_x_denial_is_exact(entry)) {
		*denied_out = *entry;
		LWLockRelease(&shard->lock.lock);
		return GCS_BLOCK_PENDING_X_DENY_REPLAY;
	}
	if (entry->status == (uint8)GCS_BLOCK_REPLY_DENIED_PENDING_X && entry->completed_at_ts != 0) {
		dedup_pcm_x_note_failclosed(shard);
		LWLockRelease(&shard->lock.lock);
		return GCS_BLOCK_PENDING_X_DENY_INVALID;
	}

	dedup_pending_x_install_denial(entry);
	*denied_out = *entry;
	LWLockRelease(&shard->lock.lock);
	return GCS_BLOCK_PENDING_X_DENY_NEW;
}

bool
cluster_gcs_block_dedup_set_request_flags_exact(int worker_id, const GcsBlockDedupKey *key,
												const BufferTag *tag, uint8 transition_id,
												uint8 request_flags)
{
	ClusterGcsBlockDedupShard *shard;
	HTAB *htab = NULL;
	GcsBlockDedupEntry *entry;
	bool found = false;
	bool updated = false;

	Assert(key != NULL);
	Assert(tag != NULL);
	if (key == NULL || tag == NULL || (request_flags & ~GCS_BLOCK_DEDUP_REQUEST_F_VALID_MASK) != 0)
		return false;
	shard = cluster_gcs_block_dedup_resolve_shard(worker_id, &htab);
	if (shard == NULL)
		return false;
	LWLockAcquire(&shard->lock.lock, LW_EXCLUSIVE);
	entry = (GcsBlockDedupEntry *)hash_search(htab, key, HASH_FIND, &found);
	if (found && entry->entry_kind == GCS_BLOCK_DEDUP_ENTRY_GENERIC
		&& memcmp(&entry->tag, tag, sizeof(*tag)) == 0 && entry->transition_id == transition_id) {
		uint8 pinned_flags = GCS_BLOCK_DEDUP_REQUEST_F_PINNED | request_flags;

		if (entry->request_flags == 0)
			entry->request_flags = pinned_flags;
		updated = entry->request_flags == pinned_flags;
	} else if (found && entry->entry_kind != GCS_BLOCK_DEDUP_ENTRY_GENERIC)
		dedup_pcm_x_note_failclosed(shard);
	LWLockRelease(&shard->lock.lock);
	return updated;
}

GcsBlockPcmXImageResult
cluster_gcs_block_dedup_pcm_x_reserve(int worker_id, const GcsBlockDedupKey *key,
									  const BufferTag *tag,
									  const GcsBlockPcmXImageBinding *reserved_binding)
{
	ClusterGcsBlockDedupShard *shard;
	HTAB *htab = NULL;
	GcsBlockDedupEntry *entry;
	bool found = false;

	shard = cluster_gcs_block_dedup_resolve_shard(worker_id, &htab);
	if (shard == NULL)
		return GCS_BLOCK_PCM_X_IMAGE_FULL;
	if (!dedup_pcm_x_binding_valid(key, tag, reserved_binding, true)) {
		dedup_pcm_x_note_failclosed(shard);
		return GCS_BLOCK_PCM_X_IMAGE_INVALID;
	}

	LWLockAcquire(&shard->lock.lock, LW_EXCLUSIVE);
	entry = (GcsBlockDedupEntry *)hash_search(htab, key, HASH_FIND, &found);
	if (found) {
		if (entry->entry_kind == GCS_BLOCK_DEDUP_ENTRY_PCM_X_DRAINED
			&& !dedup_pcm_x_entry_drained_valid(key, tag, entry)) {
			dedup_pcm_x_note_failclosed(shard);
			LWLockRelease(&shard->lock.lock);
			return GCS_BLOCK_PCM_X_IMAGE_STALE;
		}
		if ((entry->entry_kind == GCS_BLOCK_DEDUP_ENTRY_PCM_X_RESERVED
			 || entry->entry_kind == GCS_BLOCK_DEDUP_ENTRY_PCM_X_IMAGE
			 || entry->entry_kind == GCS_BLOCK_DEDUP_ENTRY_PCM_X_MATERIALIZED_UNCOMMITTED
			 || entry->entry_kind == GCS_BLOCK_DEDUP_ENTRY_PCM_X_DRAINED)
			&& entry->transition_id == (uint8)PCM_TRANS_N_TO_S
			&& memcmp(&entry->tag, tag, sizeof(*tag)) == 0
			&& dedup_pcm_x_reservation_equal(entry, reserved_binding)) {
			if (entry->entry_kind != GCS_BLOCK_DEDUP_ENTRY_PCM_X_DRAINED)
				dedup_pcm_x_work_pending[worker_id] = true;
			LWLockRelease(&shard->lock.lock);
			return GCS_BLOCK_PCM_X_IMAGE_DUPLICATE;
		}
		dedup_pcm_x_note_failclosed(shard);
		LWLockRelease(&shard->lock.lock);
		return GCS_BLOCK_PCM_X_IMAGE_STALE;
	}

	entry = (GcsBlockDedupEntry *)hash_search(htab, key, HASH_ENTER_NULL, &found);
	if (entry == NULL
		&& dedup_reclaim_reclaimable_locked(shard, htab, GetCurrentTimestamp(), 1) > 0)
		entry = (GcsBlockDedupEntry *)hash_search(htab, key, HASH_ENTER_NULL, &found);
	if (entry == NULL || found) {
		pg_atomic_fetch_add_u64(&shard->full_count, 1);
		dedup_pcm_x_note_failclosed(shard);
		LWLockRelease(&shard->lock.lock);
		return GCS_BLOCK_PCM_X_IMAGE_FULL;
	}

	memset(((char *)entry) + sizeof(GcsBlockDedupKey), 0,
		   sizeof(GcsBlockDedupEntry) - sizeof(GcsBlockDedupKey));
	entry->tag = *tag;
	entry->transition_id = (uint8)PCM_TRANS_N_TO_S;
	entry->entry_kind = GCS_BLOCK_DEDUP_ENTRY_PCM_X_RESERVED;
	entry->pcm_x_master_session = reserved_binding->master_session;
	entry->pinned_lifetime_us = (int64)reserved_binding->required_page_scn;
	entry->payload_meta.pcm_x_identity = reserved_binding->identity;
	entry->registered_at_ts = GetCurrentTimestamp();
	pg_atomic_fetch_add_u32(&shard->entry_count, 1);
	dedup_pcm_x_work_pending[worker_id] = true;
	LWLockRelease(&shard->lock.lock);
	return GCS_BLOCK_PCM_X_IMAGE_RESERVED;
}

GcsBlockPcmXImageResult
cluster_gcs_block_dedup_pcm_x_materialize(int worker_id, const GcsBlockDedupKey *key,
										  const BufferTag *tag,
										  const GcsBlockPcmXImageBinding *ready_binding,
										  uint64 reservation_token, uint8 source_pcm_state,
										  const GcsBlockReplyHeader *reply_header,
										  const char *block_data)
{
	ClusterGcsBlockDedupShard *shard;
	HTAB *htab = NULL;
	GcsBlockDedupEntry *entry;
	bool found = false;

	shard = cluster_gcs_block_dedup_resolve_shard(worker_id, &htab);
	if (shard == NULL)
		return GCS_BLOCK_PCM_X_IMAGE_FULL;
	if (reservation_token == 0 || !dedup_pcm_x_source_state_valid(source_pcm_state)
		|| !dedup_pcm_x_ready_payload_valid(key, tag, ready_binding, reply_header, block_data)) {
		dedup_pcm_x_note_failclosed(shard);
		return GCS_BLOCK_PCM_X_IMAGE_INVALID;
	}

	LWLockAcquire(&shard->lock.lock, LW_EXCLUSIVE);
	entry = (GcsBlockDedupEntry *)hash_search(htab, key, HASH_FIND, &found);
	if (!found) {
		dedup_pcm_x_note_failclosed(shard);
		LWLockRelease(&shard->lock.lock);
		return GCS_BLOCK_PCM_X_IMAGE_NOT_FOUND;
	}
	if (entry->entry_kind == GCS_BLOCK_DEDUP_ENTRY_PCM_X_IMAGE
		|| entry->entry_kind == GCS_BLOCK_DEDUP_ENTRY_PCM_X_MATERIALIZED_UNCOMMITTED) {
		GcsBlockPcmXImageBinding stored_binding;

		dedup_pcm_x_binding_from_entry(entry, &stored_binding);
		if (entry->transition_id == (uint8)PCM_TRANS_N_TO_S
			&& memcmp(&entry->tag, tag, sizeof(*tag)) == 0
			&& GcsBlockPcmXImageBindingEqual(&stored_binding, ready_binding)
			&& dedup_pcm_x_entry_payload_valid(key, tag, entry)
			&& (entry->entry_kind == GCS_BLOCK_DEDUP_ENTRY_PCM_X_IMAGE
				|| (dedup_pcm_x_reservation_token_get(entry) == reservation_token
					&& entry->request_flags == source_pcm_state))
			&& memcmp(&entry->reply_header, reply_header, sizeof(*reply_header)) == 0
			&& memcmp(entry->block_data, block_data, GCS_BLOCK_DATA_SIZE) == 0) {
			LWLockRelease(&shard->lock.lock);
			return GCS_BLOCK_PCM_X_IMAGE_DUPLICATE;
		}
		dedup_pcm_x_note_failclosed(shard);
		LWLockRelease(&shard->lock.lock);
		return GCS_BLOCK_PCM_X_IMAGE_STALE;
	}
	if (entry->entry_kind != GCS_BLOCK_DEDUP_ENTRY_PCM_X_RESERVED
		|| entry->transition_id != (uint8)PCM_TRANS_N_TO_S
		|| memcmp(&entry->tag, tag, sizeof(*tag)) != 0
		|| !dedup_pcm_x_reservation_equal(entry, ready_binding)) {
		dedup_pcm_x_note_failclosed(shard);
		LWLockRelease(&shard->lock.lock);
		return GCS_BLOCK_PCM_X_IMAGE_STALE;
	}

	{
		GcsBlockPcmXImageIdentity reserved_identity = entry->payload_meta.pcm_x_identity;
		GcsBlockPcmXImageBinding stored_binding;

		entry->reply_header = *reply_header;
		entry->status = (uint8)GCS_BLOCK_REPLY_READ_IMAGE_FROM_XHOLDER;
		entry->request_flags = source_pcm_state;
		dedup_pcm_x_reservation_token_set(entry, reservation_token);
		entry->payload_meta.pcm_x_identity = ready_binding->identity;
		memcpy(entry->block_data, block_data, GCS_BLOCK_DATA_SIZE);
		entry->completed_at_ts = GetCurrentTimestamp();
		dedup_pcm_x_binding_from_entry(entry, &stored_binding);
		if (!dedup_pcm_x_ready_payload_valid(key, tag, &stored_binding, &entry->reply_header,
											 entry->block_data)) {
			memset(&entry->reply_header, 0, sizeof(entry->reply_header));
			memset(entry->block_data, 0, GCS_BLOCK_DATA_SIZE);
			entry->payload_meta.pcm_x_identity = reserved_identity;
			entry->status = 0;
			entry->request_flags = 0;
			dedup_pcm_x_reservation_token_set(entry, 0);
			entry->completed_at_ts = 0;
			dedup_pcm_x_note_failclosed(shard);
			LWLockRelease(&shard->lock.lock);
			return GCS_BLOCK_PCM_X_IMAGE_INVALID;
		}
	}
	entry->entry_kind = GCS_BLOCK_DEDUP_ENTRY_PCM_X_MATERIALIZED_UNCOMMITTED;
	dedup_pcm_x_work_pending[worker_id] = true;
	pg_atomic_fetch_add_u64(&shard->pcm_x_stage_count, 1);
	LWLockRelease(&shard->lock.lock);
	return GCS_BLOCK_PCM_X_IMAGE_STORED;
}


GcsBlockPcmXImageResult
cluster_gcs_block_dedup_pcm_x_publish_ready_exact(int worker_id, const GcsBlockDedupKey *key,
												  const BufferTag *tag,
												  const GcsBlockPcmXImageBinding *ready_binding)
{
	ClusterGcsBlockDedupShard *shard;
	HTAB *htab = NULL;
	GcsBlockDedupEntry *entry;
	GcsBlockPcmXImageBinding stored_binding;
	bool found = false;

	shard = cluster_gcs_block_dedup_resolve_shard(worker_id, &htab);
	if (shard == NULL)
		return GCS_BLOCK_PCM_X_IMAGE_FULL;
	if (!dedup_pcm_x_binding_valid(key, tag, ready_binding, false)) {
		dedup_pcm_x_note_failclosed(shard);
		return GCS_BLOCK_PCM_X_IMAGE_INVALID;
	}

	LWLockAcquire(&shard->lock.lock, LW_EXCLUSIVE);
	entry = (GcsBlockDedupEntry *)hash_search(htab, key, HASH_FIND, &found);
	if (!found) {
		dedup_pcm_x_note_failclosed(shard);
		LWLockRelease(&shard->lock.lock);
		return GCS_BLOCK_PCM_X_IMAGE_NOT_FOUND;
	}
	dedup_pcm_x_binding_from_entry(entry, &stored_binding);
	if (entry->transition_id != (uint8)PCM_TRANS_N_TO_S
		|| memcmp(&entry->tag, tag, sizeof(*tag)) != 0
		|| !GcsBlockPcmXImageBindingEqual(&stored_binding, ready_binding)
		|| !dedup_pcm_x_entry_payload_valid(key, tag, entry)) {
		dedup_pcm_x_note_failclosed(shard);
		LWLockRelease(&shard->lock.lock);
		return GCS_BLOCK_PCM_X_IMAGE_STALE;
	}
	if (entry->entry_kind == GCS_BLOCK_DEDUP_ENTRY_PCM_X_IMAGE) {
		LWLockRelease(&shard->lock.lock);
		return GCS_BLOCK_PCM_X_IMAGE_DUPLICATE;
	}
	if (entry->entry_kind != GCS_BLOCK_DEDUP_ENTRY_PCM_X_MATERIALIZED_UNCOMMITTED) {
		dedup_pcm_x_note_failclosed(shard);
		LWLockRelease(&shard->lock.lock);
		return GCS_BLOCK_PCM_X_IMAGE_STALE;
	}

	entry->entry_kind = GCS_BLOCK_DEDUP_ENTRY_PCM_X_IMAGE;
	entry->request_flags = 0;
	dedup_pcm_x_reservation_token_set(entry, 0);
	dedup_pcm_x_work_pending[worker_id] = true;
	LWLockRelease(&shard->lock.lock);
	return GCS_BLOCK_PCM_X_IMAGE_STORED;
}

GcsBlockPcmXImageResult
cluster_gcs_block_dedup_pcm_x_lookup(int worker_id, const GcsBlockDedupKey *key,
									 const BufferTag *tag,
									 const GcsBlockPcmXImageBinding *expected_binding,
									 GcsBlockDedupEntry *cached_reply_out)
{
	ClusterGcsBlockDedupShard *shard;
	HTAB *htab = NULL;
	GcsBlockDedupEntry *entry;
	GcsBlockPcmXImageBinding stored_binding;
	bool found = false;
	bool binding_is_reserved;

	if (cached_reply_out != NULL)
		memset(cached_reply_out, 0, sizeof(*cached_reply_out));
	shard = cluster_gcs_block_dedup_resolve_shard(worker_id, &htab);
	if (shard == NULL)
		return GCS_BLOCK_PCM_X_IMAGE_FULL;
	binding_is_reserved = expected_binding != NULL && expected_binding->identity.image.page_scn == 0
						  && expected_binding->identity.image.page_lsn == 0
						  && expected_binding->identity.image.page_checksum == 0;
	if (!dedup_pcm_x_binding_valid(key, tag, expected_binding, binding_is_reserved)) {
		dedup_pcm_x_note_failclosed(shard);
		return GCS_BLOCK_PCM_X_IMAGE_INVALID;
	}

	LWLockAcquire(&shard->lock.lock, LW_EXCLUSIVE);
	entry = (GcsBlockDedupEntry *)hash_search(htab, key, HASH_FIND, &found);
	if (!found) {
		dedup_pcm_x_note_failclosed(shard);
		LWLockRelease(&shard->lock.lock);
		return GCS_BLOCK_PCM_X_IMAGE_NOT_FOUND;
	}
	if (entry->entry_kind == GCS_BLOCK_DEDUP_ENTRY_PCM_X_DRAINED) {
		dedup_pcm_x_binding_from_entry(entry, &stored_binding);
		if (entry->transition_id != (uint8)PCM_TRANS_N_TO_S
			|| memcmp(&entry->tag, tag, sizeof(*tag)) != 0
			|| !GcsBlockPcmXImageBindingEqual(&stored_binding, expected_binding)
			|| !dedup_pcm_x_entry_drained_valid(key, tag, entry)) {
			dedup_pcm_x_note_failclosed(shard);
			LWLockRelease(&shard->lock.lock);
			return GCS_BLOCK_PCM_X_IMAGE_STALE;
		}
		LWLockRelease(&shard->lock.lock);
		return GCS_BLOCK_PCM_X_IMAGE_NOT_FOUND;
	}
	if (entry->transition_id != (uint8)PCM_TRANS_N_TO_S
		|| memcmp(&entry->tag, tag, sizeof(*tag)) != 0
		|| (entry->entry_kind != GCS_BLOCK_DEDUP_ENTRY_PCM_X_RESERVED
			&& entry->entry_kind != GCS_BLOCK_DEDUP_ENTRY_PCM_X_IMAGE
			&& entry->entry_kind != GCS_BLOCK_DEDUP_ENTRY_PCM_X_MATERIALIZED_UNCOMMITTED)) {
		dedup_pcm_x_note_failclosed(shard);
		LWLockRelease(&shard->lock.lock);
		return GCS_BLOCK_PCM_X_IMAGE_STALE;
	}
	dedup_pcm_x_binding_from_entry(entry, &stored_binding);
	if (!GcsBlockPcmXImageBindingEqual(&stored_binding, expected_binding)) {
		dedup_pcm_x_note_failclosed(shard);
		LWLockRelease(&shard->lock.lock);
		return GCS_BLOCK_PCM_X_IMAGE_STALE;
	}
	if (entry->entry_kind == GCS_BLOCK_DEDUP_ENTRY_PCM_X_RESERVED) {
		dedup_pcm_x_note_failclosed(shard);
		LWLockRelease(&shard->lock.lock);
		return GCS_BLOCK_PCM_X_IMAGE_NOT_READY;
	}
	if (entry->entry_kind == GCS_BLOCK_DEDUP_ENTRY_PCM_X_MATERIALIZED_UNCOMMITTED) {
		if (!dedup_pcm_x_entry_payload_valid(key, tag, entry)) {
			dedup_pcm_x_note_failclosed(shard);
			LWLockRelease(&shard->lock.lock);
			return GCS_BLOCK_PCM_X_IMAGE_STALE;
		}
		LWLockRelease(&shard->lock.lock);
		return GCS_BLOCK_PCM_X_IMAGE_NOT_READY;
	}
	if (!dedup_pcm_x_entry_ready_valid(key, tag, entry)) {
		dedup_pcm_x_note_failclosed(shard);
		LWLockRelease(&shard->lock.lock);
		return GCS_BLOCK_PCM_X_IMAGE_STALE;
	}

	if (cached_reply_out != NULL)
		*cached_reply_out = *entry;
	pg_atomic_fetch_add_u64(&shard->pcm_x_replay_count, 1);
	LWLockRelease(&shard->lock.lock);
	return GCS_BLOCK_PCM_X_IMAGE_REPLAY;
}

GcsBlockPcmXImageResult
cluster_gcs_block_dedup_pcm_x_drain_status_exact(int worker_id, const GcsBlockDedupKey *key,
												 const BufferTag *tag,
												 const GcsBlockPcmXImageBinding *binding)
{
	ClusterGcsBlockDedupShard *shard;
	HTAB *htab = NULL;
	GcsBlockDedupEntry *entry;
	GcsBlockPcmXImageBinding stored_binding;
	bool found = false;

	shard = cluster_gcs_block_dedup_resolve_shard(worker_id, &htab);
	if (shard == NULL)
		return GCS_BLOCK_PCM_X_IMAGE_FULL;
	if (!dedup_pcm_x_binding_valid(key, tag, binding, false)) {
		dedup_pcm_x_note_failclosed(shard);
		return GCS_BLOCK_PCM_X_IMAGE_INVALID;
	}

	LWLockAcquire(&shard->lock.lock, LW_SHARED);
	entry = (GcsBlockDedupEntry *)hash_search(htab, key, HASH_FIND, &found);
	if (!found) {
		dedup_pcm_x_note_failclosed(shard);
		LWLockRelease(&shard->lock.lock);
		return GCS_BLOCK_PCM_X_IMAGE_NOT_FOUND;
	}
	dedup_pcm_x_binding_from_entry(entry, &stored_binding);
	if (entry->transition_id != (uint8)PCM_TRANS_N_TO_S
		|| memcmp(&entry->tag, tag, sizeof(*tag)) != 0
		|| !GcsBlockPcmXImageBindingEqual(&stored_binding, binding)
		|| (entry->entry_kind != GCS_BLOCK_DEDUP_ENTRY_PCM_X_IMAGE
			&& entry->entry_kind != GCS_BLOCK_DEDUP_ENTRY_PCM_X_DRAINED)
		|| (entry->entry_kind == GCS_BLOCK_DEDUP_ENTRY_PCM_X_IMAGE
				? !dedup_pcm_x_entry_ready_valid(key, tag, entry)
				: !dedup_pcm_x_entry_drained_valid(key, tag, entry))) {
		dedup_pcm_x_note_failclosed(shard);
		LWLockRelease(&shard->lock.lock);
		return GCS_BLOCK_PCM_X_IMAGE_STALE;
	}
	if (entry->entry_kind == GCS_BLOCK_DEDUP_ENTRY_PCM_X_DRAINED) {
		LWLockRelease(&shard->lock.lock);
		return GCS_BLOCK_PCM_X_IMAGE_DUPLICATE;
	}
	LWLockRelease(&shard->lock.lock);
	return GCS_BLOCK_PCM_X_IMAGE_NOT_READY;
}


GcsBlockPcmXImageResult
cluster_gcs_block_dedup_pcm_x_release_exact(int worker_id, const GcsBlockDedupKey *key,
											const BufferTag *tag,
											const GcsBlockPcmXImageBinding *binding,
											int32 drained_master_node)
{
	ClusterGcsBlockDedupShard *shard;
	HTAB *htab = NULL;
	GcsBlockDedupEntry *entry;
	GcsBlockPcmXImageBinding stored_binding;
	bool found = false;
	bool binding_is_reserved;

	shard = cluster_gcs_block_dedup_resolve_shard(worker_id, &htab);
	if (shard == NULL)
		return GCS_BLOCK_PCM_X_IMAGE_FULL;
	binding_is_reserved = binding != NULL && binding->identity.image.page_scn == 0
						  && binding->identity.image.page_lsn == 0
						  && binding->identity.image.page_checksum == 0;
	if (!dedup_pcm_x_binding_valid(key, tag, binding, binding_is_reserved)) {
		dedup_pcm_x_note_failclosed(shard);
		return GCS_BLOCK_PCM_X_IMAGE_INVALID;
	}
	if (drained_master_node < -1 || drained_master_node >= PCM_X_PROTOCOL_NODE_LIMIT) {
		dedup_pcm_x_note_failclosed(shard);
		return GCS_BLOCK_PCM_X_IMAGE_INVALID;
	}

	LWLockAcquire(&shard->lock.lock, LW_EXCLUSIVE);
	entry = (GcsBlockDedupEntry *)hash_search(htab, key, HASH_FIND, &found);
	if (!found) {
		dedup_pcm_x_note_failclosed(shard);
		LWLockRelease(&shard->lock.lock);
		return GCS_BLOCK_PCM_X_IMAGE_NOT_FOUND;
	}
	dedup_pcm_x_binding_from_entry(entry, &stored_binding);
	if (entry->transition_id != (uint8)PCM_TRANS_N_TO_S
		|| memcmp(&entry->tag, tag, sizeof(*tag)) != 0
		|| (entry->entry_kind != GCS_BLOCK_DEDUP_ENTRY_PCM_X_RESERVED
			&& entry->entry_kind != GCS_BLOCK_DEDUP_ENTRY_PCM_X_IMAGE
			&& entry->entry_kind != GCS_BLOCK_DEDUP_ENTRY_PCM_X_MATERIALIZED_UNCOMMITTED
			&& entry->entry_kind != GCS_BLOCK_DEDUP_ENTRY_PCM_X_DRAINED)
		|| !GcsBlockPcmXImageBindingEqual(&stored_binding, binding)) {
		dedup_pcm_x_note_failclosed(shard);
		LWLockRelease(&shard->lock.lock);
		return GCS_BLOCK_PCM_X_IMAGE_STALE;
	}
	if (entry->entry_kind == GCS_BLOCK_DEDUP_ENTRY_PCM_X_DRAINED) {
		if (!dedup_pcm_x_entry_drained_valid(key, tag, entry)) {
			dedup_pcm_x_note_failclosed(shard);
			LWLockRelease(&shard->lock.lock);
			return GCS_BLOCK_PCM_X_IMAGE_STALE;
		}
		if (drained_master_node < 0 || entry->request_flags != (uint8)(drained_master_node + 1)) {
			dedup_pcm_x_note_failclosed(shard);
			LWLockRelease(&shard->lock.lock);
			return GCS_BLOCK_PCM_X_IMAGE_STALE;
		}
		LWLockRelease(&shard->lock.lock);
		return GCS_BLOCK_PCM_X_IMAGE_DUPLICATE;
	}
	if (entry->entry_kind == GCS_BLOCK_DEDUP_ENTRY_PCM_X_IMAGE) {
		if (drained_master_node < 0) {
			dedup_pcm_x_note_failclosed(shard);
			LWLockRelease(&shard->lock.lock);
			return GCS_BLOCK_PCM_X_IMAGE_INVALID;
		}
		if (!dedup_pcm_x_entry_ready_valid(key, tag, entry)) {
			dedup_pcm_x_note_failclosed(shard);
			LWLockRelease(&shard->lock.lock);
			return GCS_BLOCK_PCM_X_IMAGE_STALE;
		}
		entry->entry_kind = GCS_BLOCK_DEDUP_ENTRY_PCM_X_DRAINED;
		entry->request_flags = (uint8)(drained_master_node + 1);
		entry->done_at_ts = 0;
		pg_atomic_fetch_add_u64(&shard->pcm_x_release_count, 1);
		LWLockRelease(&shard->lock.lock);
		return GCS_BLOCK_PCM_X_IMAGE_RELEASED;
	}
	if (drained_master_node != -1) {
		dedup_pcm_x_note_failclosed(shard);
		LWLockRelease(&shard->lock.lock);
		return GCS_BLOCK_PCM_X_IMAGE_INVALID;
	}

	(void)hash_search(htab, key, HASH_REMOVE, &found);
	if (!found) {
		dedup_pcm_x_note_failclosed(shard);
		LWLockRelease(&shard->lock.lock);
		return GCS_BLOCK_PCM_X_IMAGE_STALE;
	}
	pg_atomic_fetch_sub_u32(&shard->entry_count, 1);
	pg_atomic_fetch_add_u64(&shard->pcm_x_release_count, 1);
	LWLockRelease(&shard->lock.lock);
	return GCS_BLOCK_PCM_X_IMAGE_RELEASED;
}


bool
cluster_gcs_block_dedup_pcm_x_retire_up_to(uint64 cluster_epoch, int32 authenticated_master_node,
										   uint64 authenticated_master_session,
										   uint64 retire_through_ticket_id)
{
	int s;

	if (authenticated_master_node < 0 || authenticated_master_node >= PCM_X_PROTOCOL_NODE_LIMIT
		|| authenticated_master_session == 0 || retire_through_ticket_id == 0
		|| cluster_gcs_block_dedup_shards == NULL)
		return false;

	for (s = 0; s < cluster_gcs_block_dedup_n_shards; s++) {
		ClusterGcsBlockDedupShard *shard = &cluster_gcs_block_dedup_shards[s];
		HTAB *htab = cluster_gcs_block_dedup_htabs[s];
		HASH_SEQ_STATUS scan;
		GcsBlockDedupEntry *entry;
		int removed = 0;

		if (htab == NULL)
			continue;
		LWLockAcquire(&shard->lock.lock, LW_EXCLUSIVE);
		hash_seq_init(&scan, htab);
		while ((entry = (GcsBlockDedupEntry *)hash_seq_search(&scan)) != NULL) {
			const PcmXTicketRef *ref;

			if (entry->entry_kind != GCS_BLOCK_DEDUP_ENTRY_PCM_X_DRAINED)
				continue;
			if (!dedup_pcm_x_entry_drained_valid(&entry->key, &entry->tag, entry)) {
				dedup_pcm_x_note_failclosed(shard);
				hash_seq_term(&scan);
				LWLockRelease(&shard->lock.lock);
				return false;
			}
			ref = &entry->payload_meta.pcm_x_identity.ref;
			if (ref->identity.cluster_epoch == cluster_epoch
				&& entry->request_flags == (uint8)(authenticated_master_node + 1)
				&& entry->pcm_x_master_session == authenticated_master_session
				&& ref->handle.ticket_id <= retire_through_ticket_id) {
				(void)hash_search(htab, &entry->key, HASH_REMOVE, NULL);
				removed++;
			}
		}
		if (removed > 0)
			pg_atomic_fetch_sub_u32(&shard->entry_count, (uint32)removed);
		LWLockRelease(&shard->lock.lock);
	}
	return true;
}


GcsBlockPcmXImageResult
cluster_gcs_block_dedup_pcm_x_preserve_finish_error_exact(
	int worker_id, const GcsBlockDedupKey *key, const BufferTag *tag,
	const GcsBlockPcmXImageBinding *binding, uint64 reservation_token, uint8 source_pcm_state)
{
	ClusterGcsBlockDedupShard *shard;
	HTAB *htab = NULL;
	GcsBlockDedupEntry *entry;
	GcsBlockPcmXImageBinding stored_binding;
	bool found = false;

	shard = cluster_gcs_block_dedup_resolve_shard(worker_id, &htab);
	if (shard == NULL)
		return GCS_BLOCK_PCM_X_IMAGE_FULL;
	if (reservation_token == 0 || !dedup_pcm_x_source_state_valid(source_pcm_state)
		|| !dedup_pcm_x_binding_valid(key, tag, binding, false)) {
		dedup_pcm_x_note_failclosed(shard);
		return GCS_BLOCK_PCM_X_IMAGE_INVALID;
	}

	LWLockAcquire(&shard->lock.lock, LW_SHARED);
	entry = (GcsBlockDedupEntry *)hash_search(htab, key, HASH_FIND, &found);
	if (!found) {
		dedup_pcm_x_note_failclosed(shard);
		LWLockRelease(&shard->lock.lock);
		return GCS_BLOCK_PCM_X_IMAGE_NOT_FOUND;
	}
	dedup_pcm_x_binding_from_entry(entry, &stored_binding);
	if (entry->entry_kind != GCS_BLOCK_DEDUP_ENTRY_PCM_X_MATERIALIZED_UNCOMMITTED
		|| entry->transition_id != (uint8)PCM_TRANS_N_TO_S
		|| memcmp(&entry->tag, tag, sizeof(*tag)) != 0
		|| !GcsBlockPcmXImageBindingEqual(&stored_binding, binding)
		|| !dedup_pcm_x_entry_payload_valid(key, tag, entry)
		|| dedup_pcm_x_reservation_token_get(entry) != reservation_token
		|| entry->request_flags != source_pcm_state) {
		dedup_pcm_x_note_failclosed(shard);
		LWLockRelease(&shard->lock.lock);
		return GCS_BLOCK_PCM_X_IMAGE_STALE;
	}
	LWLockRelease(&shard->lock.lock);
	return GCS_BLOCK_PCM_X_IMAGE_COMMIT_PENDING;
}


static void
dedup_pcm_x_copy_work(const GcsBlockDedupEntry *entry, GcsBlockPcmXImageWork *work)
{
	memset(work, 0, sizeof(*work));
	work->key = entry->key;
	dedup_pcm_x_binding_from_entry(entry, &work->binding);
	if (entry->entry_kind == GCS_BLOCK_DEDUP_ENTRY_PCM_X_MATERIALIZED_UNCOMMITTED) {
		work->reservation_token = dedup_pcm_x_reservation_token_get(entry);
		work->source_pcm_state = entry->request_flags;
	}
	work->tag = entry->tag;
	work->entry_kind = entry->entry_kind;
}


static int
dedup_pcm_x_key_compare(const GcsBlockDedupKey *left, const GcsBlockDedupKey *right)
{
	if (left->origin_node_id != right->origin_node_id)
		return left->origin_node_id < right->origin_node_id ? -1 : 1;
	if (left->requester_backend_id != right->requester_backend_id)
		return left->requester_backend_id < right->requester_backend_id ? -1 : 1;
	if (left->request_id != right->request_id)
		return left->request_id < right->request_id ? -1 : 1;
	if (left->cluster_epoch != right->cluster_epoch)
		return left->cluster_epoch < right->cluster_epoch ? -1 : 1;
	return 0;
}


static void
dedup_pcm_x_consider_work(const GcsBlockDedupEntry *entry, const GcsBlockDedupKey *cursor,
						  bool cursor_valid, GcsBlockPcmXImageWork *after, bool *have_after,
						  GcsBlockPcmXImageWork *wrap, bool *have_wrap)
{
	bool is_after = cursor_valid && dedup_pcm_x_key_compare(&entry->key, cursor) > 0;

	if (is_after && (!*have_after || dedup_pcm_x_key_compare(&entry->key, &after->key) < 0)) {
		dedup_pcm_x_copy_work(entry, after);
		*have_after = true;
	}
	if (!*have_wrap || dedup_pcm_x_key_compare(&entry->key, &wrap->key) < 0) {
		dedup_pcm_x_copy_work(entry, wrap);
		*have_wrap = true;
	}
}


/* Return at most one immutable work token per LMS tick.  Admission work
 * includes fresh RESERVED entries and commit-only retries for immutable
 * MATERIALIZED_UNCOMMITTED entries; the common exact-key cursor prevents a
 * contended commit from monopolizing the worker.  Admission wins the first
 * mixed-class tick, then selections alternate with READY replay. */
GcsBlockPcmXImageResult
cluster_gcs_block_dedup_pcm_x_next_work(int worker_id, GcsBlockPcmXImageWork *work_out)
{
	ClusterGcsBlockDedupShard *shard;
	GcsBlockPcmXImageWork ready_after;
	GcsBlockPcmXImageWork ready_wrap;
	GcsBlockPcmXImageWork reserved_after;
	GcsBlockPcmXImageWork reserved_wrap;
	HTAB *htab = NULL;
	HASH_SEQ_STATUS scan;
	GcsBlockDedupEntry *entry;
	GcsBlockPcmXImageBinding binding;
	GcsBlockPcmXImageResult result = GCS_BLOCK_PCM_X_IMAGE_NOT_FOUND;
	bool have_ready_after = false;
	bool have_ready_wrap = false;
	bool have_reserved_after = false;
	bool have_reserved_wrap = false;

	if (work_out != NULL)
		memset(work_out, 0, sizeof(*work_out));
	shard = cluster_gcs_block_dedup_resolve_shard(worker_id, &htab);
	if (shard == NULL)
		return GCS_BLOCK_PCM_X_IMAGE_FULL;
	if (work_out == NULL) {
		dedup_pcm_x_note_failclosed(shard);
		return GCS_BLOCK_PCM_X_IMAGE_INVALID;
	}
	if (!dedup_pcm_x_work_pending[worker_id])
		return GCS_BLOCK_PCM_X_IMAGE_NOT_FOUND;

	LWLockAcquire(&shard->lock.lock, LW_EXCLUSIVE);
	hash_seq_init(&scan, htab);
	while ((entry = (GcsBlockDedupEntry *)hash_seq_search(&scan)) != NULL) {
		if (entry->entry_kind != GCS_BLOCK_DEDUP_ENTRY_PCM_X_RESERVED
			&& entry->entry_kind != GCS_BLOCK_DEDUP_ENTRY_PCM_X_IMAGE
			&& entry->entry_kind != GCS_BLOCK_DEDUP_ENTRY_PCM_X_MATERIALIZED_UNCOMMITTED)
			continue;
		/* A READY entry admitted to the outbound ring sleeps until an exact
		 * type-49 retransmit clears this marker. */
		if (entry->entry_kind == GCS_BLOCK_DEDUP_ENTRY_PCM_X_IMAGE && entry->done_at_ts != 0)
			continue;
		dedup_pcm_x_binding_from_entry(entry, &binding);
		if (entry->transition_id != (uint8)PCM_TRANS_N_TO_S
			|| (entry->entry_kind == GCS_BLOCK_DEDUP_ENTRY_PCM_X_RESERVED
				&& !dedup_pcm_x_binding_valid(&entry->key, &entry->tag, &binding, true))
			|| (entry->entry_kind == GCS_BLOCK_DEDUP_ENTRY_PCM_X_MATERIALIZED_UNCOMMITTED
				&& (!dedup_pcm_x_entry_payload_valid(&entry->key, &entry->tag, entry)
					|| dedup_pcm_x_reservation_token_get(entry) == 0
					|| !dedup_pcm_x_source_state_valid(entry->request_flags)))
			|| (entry->entry_kind == GCS_BLOCK_DEDUP_ENTRY_PCM_X_IMAGE
				&& !dedup_pcm_x_entry_ready_valid(&entry->key, &entry->tag, entry))) {
			dedup_pcm_x_note_failclosed(shard);
			result = GCS_BLOCK_PCM_X_IMAGE_INVALID;
			break;
		}
		if (entry->entry_kind == GCS_BLOCK_DEDUP_ENTRY_PCM_X_RESERVED
			|| entry->entry_kind == GCS_BLOCK_DEDUP_ENTRY_PCM_X_MATERIALIZED_UNCOMMITTED) {
			dedup_pcm_x_consider_work(entry, &dedup_pcm_x_work_cursor[worker_id],
									  dedup_pcm_x_work_cursor_valid[worker_id], &reserved_after,
									  &have_reserved_after, &reserved_wrap, &have_reserved_wrap);
		} else
			dedup_pcm_x_consider_work(entry, &dedup_pcm_x_work_cursor[worker_id],
									  dedup_pcm_x_work_cursor_valid[worker_id], &ready_after,
									  &have_ready_after, &ready_wrap, &have_ready_wrap);
	}
	/* hash_seq_search() terminates a naturally exhausted scan itself.  Only
	 * the validation-failure break above leaves an open scan to close here. */
	if (result != GCS_BLOCK_PCM_X_IMAGE_NOT_FOUND)
		hash_seq_term(&scan);
	if (result == GCS_BLOCK_PCM_X_IMAGE_NOT_FOUND) {
		bool have_ready = have_ready_after || have_ready_wrap;
		bool have_reserved = have_reserved_after || have_reserved_wrap;
		bool choose_ready = have_ready && have_reserved && dedup_pcm_x_prefer_ready_next[worker_id];

		if (have_reserved && !choose_ready) {
			*work_out = have_reserved_after ? reserved_after : reserved_wrap;
			result = work_out->entry_kind == GCS_BLOCK_DEDUP_ENTRY_PCM_X_RESERVED
						 ? GCS_BLOCK_PCM_X_IMAGE_RESERVED
						 : GCS_BLOCK_PCM_X_IMAGE_COMMIT_PENDING;
		} else if (have_ready) {
			*work_out = have_ready_after ? ready_after : ready_wrap;
			result = GCS_BLOCK_PCM_X_IMAGE_REPLAY;
		}
		if (result != GCS_BLOCK_PCM_X_IMAGE_NOT_FOUND) {
			dedup_pcm_x_work_cursor[worker_id] = work_out->key;
			dedup_pcm_x_work_cursor_valid[worker_id] = true;
			if (have_ready && have_reserved)
				dedup_pcm_x_prefer_ready_next[worker_id] = !choose_ready;
		}
	}
	if (result == GCS_BLOCK_PCM_X_IMAGE_NOT_FOUND || result == GCS_BLOCK_PCM_X_IMAGE_INVALID)
		dedup_pcm_x_work_pending[worker_id] = false;
	LWLockRelease(&shard->lock.lock);
	return result;
}


/* Mark only outbound-ring admission, not application completion.  The exact
 * DRAIN_POLL consumer remains the sole owner of byte release. */
GcsBlockPcmXImageResult
cluster_gcs_block_dedup_pcm_x_mark_staged_exact(int worker_id, const GcsBlockDedupKey *key,
												const BufferTag *tag,
												const GcsBlockPcmXImageBinding *ready_binding)
{
	ClusterGcsBlockDedupShard *shard;
	HTAB *htab = NULL;
	GcsBlockDedupEntry *entry;
	GcsBlockPcmXImageBinding stored_binding;
	bool found = false;

	shard = cluster_gcs_block_dedup_resolve_shard(worker_id, &htab);
	if (shard == NULL)
		return GCS_BLOCK_PCM_X_IMAGE_FULL;
	if (!dedup_pcm_x_binding_valid(key, tag, ready_binding, false)) {
		dedup_pcm_x_note_failclosed(shard);
		return GCS_BLOCK_PCM_X_IMAGE_INVALID;
	}
	LWLockAcquire(&shard->lock.lock, LW_EXCLUSIVE);
	entry = (GcsBlockDedupEntry *)hash_search(htab, key, HASH_FIND, &found);
	if (!found) {
		dedup_pcm_x_note_failclosed(shard);
		LWLockRelease(&shard->lock.lock);
		return GCS_BLOCK_PCM_X_IMAGE_NOT_FOUND;
	}
	dedup_pcm_x_binding_from_entry(entry, &stored_binding);
	if (entry->entry_kind != GCS_BLOCK_DEDUP_ENTRY_PCM_X_IMAGE
		|| entry->transition_id != (uint8)PCM_TRANS_N_TO_S
		|| memcmp(&entry->tag, tag, sizeof(*tag)) != 0
		|| !GcsBlockPcmXImageBindingEqual(&stored_binding, ready_binding)
		|| !dedup_pcm_x_entry_ready_valid(key, tag, entry)) {
		dedup_pcm_x_note_failclosed(shard);
		LWLockRelease(&shard->lock.lock);
		return GCS_BLOCK_PCM_X_IMAGE_STALE;
	}
	if (entry->done_at_ts != 0) {
		LWLockRelease(&shard->lock.lock);
		return GCS_BLOCK_PCM_X_IMAGE_DUPLICATE;
	}
	entry->done_at_ts = GetCurrentTimestamp();
	LWLockRelease(&shard->lock.lock);
	return GCS_BLOCK_PCM_X_IMAGE_STAGED;
}


/* Roll back only a failed outbound-ring admission.  The complete READY
 * binding is required because the reservation-shaped type-49 rearm API has a
 * different authority: it is remote retransmit evidence, not a local enqueue
 * transaction. */
GcsBlockPcmXImageResult
cluster_gcs_block_dedup_pcm_x_unmark_staged_exact(int worker_id, const GcsBlockDedupKey *key,
												  const BufferTag *tag,
												  const GcsBlockPcmXImageBinding *ready_binding)
{
	ClusterGcsBlockDedupShard *shard;
	HTAB *htab = NULL;
	GcsBlockDedupEntry *entry;
	GcsBlockPcmXImageBinding stored_binding;
	bool found = false;

	shard = cluster_gcs_block_dedup_resolve_shard(worker_id, &htab);
	if (shard == NULL)
		return GCS_BLOCK_PCM_X_IMAGE_FULL;
	if (!dedup_pcm_x_binding_valid(key, tag, ready_binding, false)) {
		dedup_pcm_x_note_failclosed(shard);
		return GCS_BLOCK_PCM_X_IMAGE_INVALID;
	}
	LWLockAcquire(&shard->lock.lock, LW_EXCLUSIVE);
	entry = (GcsBlockDedupEntry *)hash_search(htab, key, HASH_FIND, &found);
	if (!found) {
		dedup_pcm_x_note_failclosed(shard);
		LWLockRelease(&shard->lock.lock);
		return GCS_BLOCK_PCM_X_IMAGE_NOT_FOUND;
	}
	dedup_pcm_x_binding_from_entry(entry, &stored_binding);
	if (entry->entry_kind != GCS_BLOCK_DEDUP_ENTRY_PCM_X_IMAGE
		|| entry->transition_id != (uint8)PCM_TRANS_N_TO_S
		|| memcmp(&entry->tag, tag, sizeof(*tag)) != 0
		|| !GcsBlockPcmXImageBindingEqual(&stored_binding, ready_binding)
		|| !dedup_pcm_x_entry_ready_valid(key, tag, entry)) {
		dedup_pcm_x_note_failclosed(shard);
		LWLockRelease(&shard->lock.lock);
		return GCS_BLOCK_PCM_X_IMAGE_STALE;
	}
	if (entry->done_at_ts == 0) {
		LWLockRelease(&shard->lock.lock);
		return GCS_BLOCK_PCM_X_IMAGE_DUPLICATE;
	}
	entry->done_at_ts = 0;
	dedup_pcm_x_work_pending[worker_id] = true;
	LWLockRelease(&shard->lock.lock);
	return GCS_BLOCK_PCM_X_IMAGE_REARMED;
}


/* A byte-exact type-49 retransmit means the master has not applied type 50.
 * It may re-open only the matching READY outbound marker; a still-RESERVED
 * entry already appears in the normal work scan and needs no state change. */
GcsBlockPcmXImageResult
cluster_gcs_block_dedup_pcm_x_rearm_exact(int worker_id, const GcsBlockDedupKey *key,
										  const BufferTag *tag,
										  const GcsBlockPcmXImageBinding *reserved_binding)
{
	ClusterGcsBlockDedupShard *shard;
	HTAB *htab = NULL;
	GcsBlockDedupEntry *entry;
	bool found = false;

	shard = cluster_gcs_block_dedup_resolve_shard(worker_id, &htab);
	if (shard == NULL)
		return GCS_BLOCK_PCM_X_IMAGE_FULL;
	if (!dedup_pcm_x_binding_valid(key, tag, reserved_binding, true)) {
		dedup_pcm_x_note_failclosed(shard);
		return GCS_BLOCK_PCM_X_IMAGE_INVALID;
	}
	LWLockAcquire(&shard->lock.lock, LW_EXCLUSIVE);
	entry = (GcsBlockDedupEntry *)hash_search(htab, key, HASH_FIND, &found);
	if (!found) {
		dedup_pcm_x_note_failclosed(shard);
		LWLockRelease(&shard->lock.lock);
		return GCS_BLOCK_PCM_X_IMAGE_NOT_FOUND;
	}
	if ((entry->entry_kind != GCS_BLOCK_DEDUP_ENTRY_PCM_X_RESERVED
		 && entry->entry_kind != GCS_BLOCK_DEDUP_ENTRY_PCM_X_IMAGE
		 && entry->entry_kind != GCS_BLOCK_DEDUP_ENTRY_PCM_X_DRAINED)
		|| entry->transition_id != (uint8)PCM_TRANS_N_TO_S
		|| memcmp(&entry->tag, tag, sizeof(*tag)) != 0
		|| !dedup_pcm_x_reservation_equal(entry, reserved_binding)) {
		dedup_pcm_x_note_failclosed(shard);
		LWLockRelease(&shard->lock.lock);
		return GCS_BLOCK_PCM_X_IMAGE_STALE;
	}
	if (entry->entry_kind == GCS_BLOCK_DEDUP_ENTRY_PCM_X_RESERVED) {
		dedup_pcm_x_work_pending[worker_id] = true;
		LWLockRelease(&shard->lock.lock);
		return GCS_BLOCK_PCM_X_IMAGE_NOT_READY;
	}
	if (entry->entry_kind == GCS_BLOCK_DEDUP_ENTRY_PCM_X_DRAINED) {
		if (!dedup_pcm_x_entry_drained_valid(key, tag, entry)) {
			dedup_pcm_x_note_failclosed(shard);
			LWLockRelease(&shard->lock.lock);
			return GCS_BLOCK_PCM_X_IMAGE_STALE;
		}
		LWLockRelease(&shard->lock.lock);
		return GCS_BLOCK_PCM_X_IMAGE_NOT_READY;
	}
	if (!dedup_pcm_x_entry_ready_valid(key, tag, entry)) {
		dedup_pcm_x_note_failclosed(shard);
		LWLockRelease(&shard->lock.lock);
		return GCS_BLOCK_PCM_X_IMAGE_STALE;
	}
	if (entry->done_at_ts == 0) {
		dedup_pcm_x_work_pending[worker_id] = true;
		LWLockRelease(&shard->lock.lock);
		return GCS_BLOCK_PCM_X_IMAGE_DUPLICATE;
	}
	entry->done_at_ts = 0;
	dedup_pcm_x_work_pending[worker_id] = true;
	LWLockRelease(&shard->lock.lock);
	return GCS_BLOCK_PCM_X_IMAGE_REARMED;
}


/* A newly forked LMS has no proof about which instruction the previous owner
 * completed.  Any dedicated entry is therefore retained recovery evidence;
 * the caller transitions the PCM-X runtime to RECOVERY_BLOCKED. */
bool
cluster_gcs_block_dedup_pcm_x_restart_audit(int worker_id)
{
	ClusterGcsBlockDedupShard *shard;
	HTAB *htab = NULL;
	HASH_SEQ_STATUS scan;
	GcsBlockDedupEntry *entry;
	bool evidence_found = false;

	shard = cluster_gcs_block_dedup_resolve_shard(worker_id, &htab);
	if (shard == NULL)
		return true;

	LWLockAcquire(&shard->lock.lock, LW_SHARED);
	hash_seq_init(&scan, htab);
	while ((entry = (GcsBlockDedupEntry *)hash_seq_search(&scan)) != NULL) {
		if (entry->entry_kind != GCS_BLOCK_DEDUP_ENTRY_GENERIC) {
			evidence_found = true;
			hash_seq_term(&scan);
			break;
		}
	}
	if (evidence_found)
		dedup_pcm_x_note_failclosed(shard);
	LWLockRelease(&shard->lock.lock);
	return evidence_found;
}

/*
 * cluster_gcs_block_dedup_mark_done — GCS-race round-2 RC-F: consume a
 * requester completion proof.  Full identity verification + COMPLETED
 * check happen under the same exclusive shard lock as the stamp, so a
 * concurrent retransmit lookup can never observe a half-marked entry.
 */
bool
cluster_gcs_block_dedup_mark_done(int worker_id, const GcsBlockDedupKey *key, const BufferTag *tag,
								  uint8 transition_id)
{
	ClusterGcsBlockDedupShard *shard;
	HTAB *htab = NULL;
	GcsBlockDedupEntry *entry;
	bool found = false;
	bool stamped = false;

	Assert(key != NULL);
	Assert(tag != NULL);

	shard = cluster_gcs_block_dedup_resolve_shard(worker_id, &htab);
	if (shard == NULL)
		return false;

	LWLockAcquire(&shard->lock.lock, LW_EXCLUSIVE);
	entry = (GcsBlockDedupEntry *)hash_search(htab, key, HASH_FIND, &found);
	if (found && entry->entry_kind == GCS_BLOCK_DEDUP_ENTRY_GENERIC
		&& memcmp(&entry->tag, tag, sizeof(BufferTag)) == 0 && entry->transition_id == transition_id
		&& entry->completed_at_ts != 0) {
		if (entry->done_at_ts == 0)
			entry->done_at_ts = GetCurrentTimestamp();
		stamped = true; /* duplicate DONE re-stamps nothing: idempotent */
		pg_atomic_fetch_add_u64(&shard->done_marked_count, 1);
	} else {
		if (found && entry->entry_kind != GCS_BLOCK_DEDUP_ENTRY_GENERIC)
			dedup_pcm_x_note_failclosed(shard);
		pg_atomic_fetch_add_u64(&shard->done_mismatch_count, 1);
	}
	LWLockRelease(&shard->lock.lock);
	return stamped;
}

/*
 * cluster_gcs_block_dedup_note_done_mismatch — count a handler-level DONE
 * drop (transport identity binding / reserved-pad validation, review F6)
 * on the shard that would have consumed it.  Same counter as mark_done's
 * internal mismatch arm: operators read one "DONE dropped" surface.
 */
void
cluster_gcs_block_dedup_note_done_mismatch(int worker_id)
{
	ClusterGcsBlockDedupShard *shard;
	HTAB *htab = NULL;

	shard = cluster_gcs_block_dedup_resolve_shard(worker_id, &htab);
	if (shard == NULL)
		return;
	pg_atomic_fetch_add_u64(&shard->done_mismatch_count, 1);
}

/*
 * cluster_gcs_block_dedup_lifetime_ms — pure shared lifetime formula
 * (mirrors dedup_expiry_threshold_us WITHOUT the x2 margin or the us
 * conversion; the consumer applies its own margin).
 */
uint32
cluster_gcs_block_dedup_lifetime_ms(int initial_backoff_ms, int max_retries, int reply_timeout_ms)
{
	int64 lifetime_ms;

	if (initial_backoff_ms <= 0)
		initial_backoff_ms = 100;
	if (max_retries < 0)
		max_retries = 4;
	if (max_retries > 30)
		max_retries = 30;
	if (reply_timeout_ms <= 0)
		reply_timeout_ms = 5000;

	lifetime_ms = (int64)initial_backoff_ms * ((int64)((1u << max_retries) - 1))
				  + (int64)(max_retries + 1) * reply_timeout_ms;
	if (lifetime_ms > (int64)PG_UINT32_MAX)
		lifetime_ms = (int64)PG_UINT32_MAX;
	return (uint32)lifetime_ms;
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
	if (entry->entry_kind != GCS_BLOCK_DEDUP_ENTRY_GENERIC) {
		dedup_pcm_x_note_failclosed(shard);
		LWLockRelease(&shard->lock.lock);
		return;
	}
	/* A queue-kind pending-X claim has already terminated this exact legacy
	 * reader.  Any asynchronous old producer has permanently lost the right
	 * to restore GRANTED/FORWARD/page bytes; preserve the cached denial for
	 * retransmit until exact DONE. */
	if (dedup_pending_x_denial_is_exact(entry)) {
		LWLockRelease(&shard->lock.lock);
		return;
	}

	entry->status = (uint8)status;
	entry->reply_header = *header;
	entry->has_sf_dep = has_sf_dep;
	entry->sf_flags
		= has_sf_dep ? (GCS_BLOCK_REPLY_SF_HAS_DEP_VEC | GCS_BLOCK_REPLY_SF_EARLY_TRANSFER) : 0;
	entry->sf_dep_count = 0;
	cluster_sf_dep_vec_reset(&entry->payload_meta.sf_dep_vec);
	if (has_sf_dep && sf_dep_vec != NULL) {
		int i;

		for (i = 0; i < CLUSTER_SF_DEP_MAX_ORIGINS; i++) {
			if (XLogRecPtrIsInvalid(sf_dep_vec->required[i]))
				continue;
			entry->payload_meta.sf_dep_vec.required[i] = sf_dep_vec->required[i];
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
	GcsBlockDedupEntry *entry;
	bool found;

	Assert(key != NULL);

	shard = cluster_gcs_block_dedup_resolve_shard(worker_id, &htab);
	if (shard == NULL)
		return;

	LWLockAcquire(&shard->lock.lock, LW_EXCLUSIVE);
	entry = (GcsBlockDedupEntry *)hash_search(htab, key, HASH_FIND, &found);
	if (found && entry->entry_kind == GCS_BLOCK_DEDUP_ENTRY_GENERIC) {
		(void)hash_search(htab, key, HASH_REMOVE, &found);
		Assert(found);
		pg_atomic_fetch_sub_u32(&shard->entry_count, 1);
	} else if (found)
		dedup_pcm_x_note_failclosed(shard);
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
 * is 2 × the LEGAL REQUEST LIFETIME: every attempt may wait out a full
 * cluster.gcs_reply_timeout_ms before its retry fires, so the lifetime is
 * (max_retries + 1) reply-timeout windows PLUS the exponential backoff
 * total.  The pre-fix formula covered only the backoff component — with
 * the defaults that swept a still-live request's entry at 3s while its
 * attempts legally span ~26.5s (S3 rig: 25.5s vs 57.75s), so a late
 * retransmit re-registered as a MISS and re-executed a request whose
 * earlier attempt may already have granted.  Sweeping conservatively
 * biases toward retention.
 */
static int64
dedup_expiry_threshold_us(void)
{
	int64 initial_ms;
	int max_retries;
	int64 reply_timeout_ms;
	int64 lifetime_ms;

	initial_ms = cluster_gcs_block_retransmit_initial_backoff_ms > 0
					 ? cluster_gcs_block_retransmit_initial_backoff_ms
					 : 100;
	max_retries = cluster_gcs_block_retransmit_max_retries >= 0
					  ? cluster_gcs_block_retransmit_max_retries
					  : 4;
	reply_timeout_ms = cluster_gcs_reply_timeout_ms > 0 ? cluster_gcs_reply_timeout_ms : 5000;

	/* backoff total = initial × (2^max_retries - 1);  pin small max_retries
	 * to keep arithmetic in int64. */
	if (max_retries > 30)
		max_retries = 30;
	lifetime_ms = initial_ms * ((int64)((1u << max_retries) - 1))
				  + (int64)(max_retries + 1) * reply_timeout_ms;
	return lifetime_ms * 1000 * 2; /* × 2 safety margin (HC93) */
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
			int64 deadline_us;

			if (entry->entry_kind != GCS_BLOCK_DEDUP_ENTRY_GENERIC)
				continue;

			/*
			 * GCS-race round-2 RC-F: per-entry pinned deadlines.  A
			 * DONE-proven entry only lingers its pinned quarantine (reorder
			 * slop) from the proof; everything else ages its pinned
			 * registration-time lifetime from the reply/registration
			 * anchor.  Entries with no pinned value (0 — impossible after
			 * this build's registration path, but cheap to guard) fall
			 * back to the sweep-time threshold.
			 */
			if (entry->done_at_ts != 0) {
				anchor = entry->done_at_ts;
				deadline_us = entry->pinned_done_linger_us;
			} else {
				anchor = entry->completed_at_ts != 0 ? entry->completed_at_ts
													 : entry->registered_at_ts;
				deadline_us = entry->pinned_lifetime_us;
			}
			if (anchor == 0)
				continue;
			if (deadline_us <= 0)
				deadline_us = threshold_us;

			age_us = (int64)(now - anchor);
			if (age_us > deadline_us) {
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
			if (entry->entry_kind == GCS_BLOCK_DEDUP_ENTRY_GENERIC
				&& entry->key.origin_node_id == origin_node_id
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
			if (entry->entry_kind == GCS_BLOCK_DEDUP_ENTRY_GENERIC
				&& entry->key.origin_node_id == node_id) {
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

uint64
cluster_gcs_block_dedup_get_done_marked_count(void)
{
	return cluster_gcs_block_dedup_sum_u64(offsetof(ClusterGcsBlockDedupShard, done_marked_count));
}

uint64
cluster_gcs_block_dedup_get_done_mismatch_count(void)
{
	return cluster_gcs_block_dedup_sum_u64(
		offsetof(ClusterGcsBlockDedupShard, done_mismatch_count));
}

uint64
cluster_gcs_block_dedup_get_hint_violation_count(void)
{
	return cluster_gcs_block_dedup_sum_u64(
		offsetof(ClusterGcsBlockDedupShard, hint_violation_count));
}

uint64
cluster_gcs_block_dedup_get_legacy_pin_count(void)
{
	return cluster_gcs_block_dedup_sum_u64(offsetof(ClusterGcsBlockDedupShard, legacy_pin_count));
}

uint64
cluster_gcs_block_dedup_get_pcm_x_stage_count(void)
{
	return cluster_gcs_block_dedup_sum_u64(offsetof(ClusterGcsBlockDedupShard, pcm_x_stage_count));
}

uint64
cluster_gcs_block_dedup_get_pcm_x_replay_count(void)
{
	return cluster_gcs_block_dedup_sum_u64(offsetof(ClusterGcsBlockDedupShard, pcm_x_replay_count));
}

uint64
cluster_gcs_block_dedup_get_pcm_x_release_count(void)
{
	return cluster_gcs_block_dedup_sum_u64(
		offsetof(ClusterGcsBlockDedupShard, pcm_x_release_count));
}

uint64
cluster_gcs_block_dedup_get_pcm_x_failclosed_count(void)
{
	return cluster_gcs_block_dedup_sum_u64(
		offsetof(ClusterGcsBlockDedupShard, pcm_x_failclosed_count));
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
