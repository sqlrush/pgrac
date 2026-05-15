/*-------------------------------------------------------------------------
 *
 * cluster_grd.h
 *	  Global Resource Directory (GRD) routing substrate — spec-2.14.
 *
 *	  GRD is the shared substrate (GES + GCS routing) per AD-002 PCM vs
 *	  GES 分工 + feature-011.  This module ships the routing layer only:
 *	    - ClusterResId 16-byte canonical wire encoding
 *	    - 4096 shard fixed via hash_bytes_extended (PG-native)
 *	    - declared-node-aware static master map (atomic master[4096])
 *	    - Observability (pg_cluster_grd_shards view + dump_grd 8 emit_row)
 *
 *	  NOT in this spec (deferred):
 *	    - GRD entry holders[] / waiters / convert queue (spec-2.15)
 *	    - per-shard hash table for entry storage (spec-2.15)
 *	    - per-shard LWLock/slock_t (spec-2.15 true entry table)
 *	    - caller-side LockAcquire integration (spec-2.15+)
 *	    - cross-node real send (spec-2.16; spec-2.13 producer_mask NONE保持)
 *	    - DRM (Stage 6)
 *	    - PCM 复用 (spec-3.X — namespace cluster_grd 已为复用预留)
 *
 *	  Performance hook design (Stage 6 swap point):
 *	    cluster_grd_hash_resource(resid)        -> uint64 hash
 *	        (ONLY function whose body changes when Stage 6 swaps to
 *	         xxhash3 / RDMA-aware locality hash)
 *	    cluster_grd_shard_for_hash(uint64)      -> uint32 shard_id
 *	    cluster_grd_shard_for_resource(resid)   -> uint32 (compose)
 *	    cluster_grd_lookup_master(resid)        -> int32 master_node_id
 *	        (full lookup with local/remote counter)
 *	    cluster_grd_shard_lookup(resid)         -> uint32 shard_id
 *	        (thin compat wrapper; only total counter)
 *
 *	  Hash input is 14 bytes (P1.1 v0.2 user correction):
 *	    field1[4] + field2[4] + field3[4] + type[1] + lockmethodid[1]
 *	    Skip ONLY field4 (tuple offset — co-locate same-page tuples).
 *	    lockmethodid IS included (identity preservation).
 *
 *	  AD-002 PCM vs GES 分工:  GRD is shared substrate (GES + GCS routing).
 *	  AD-011 不移植 LC/RC Lock.
 *
 *	  Spec: spec-2.14-grd-resource-identity-shard-routing.md (frozen v0.4)
 *	  Design: docs/ges-lock-protocol-design.md v1.0 §4 §5
 *	  Feature: feature-011 GRD resource sharding (本 spec land 路由 substrate 子集)
 *
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/include/cluster/cluster_grd.h
 *
 * NOTES
 *	  This is a pgrac-original file (no derivation from PostgreSQL).
 *	  All symbols are backend-only (#ifndef FRONTEND) to prevent frontend
 *	  tools from accidentally pulling in cluster_grd_state references
 *	  (L8 inheritance + spec-2.11 P2 pattern).
 *
 *-------------------------------------------------------------------------
 */
#ifndef CLUSTER_GRD_H
#define CLUSTER_GRD_H

#ifndef FRONTEND

#include "port/atomics.h"
#include "storage/lock.h" /* LOCKTAG */

/*
 * 4096 shard fixed (Q8 user-approved).  Future amend path:
 *   - 实测 hot shard → 升 GUC cluster.grd_shard_count (spec-2.X future)
 *   - shard count change = wire ABI 不变(routing 仅本节点 in-memory);只需
 *     reconfig 时全集群同步 amend(LMON 协调)
 */
#define PGRAC_GRD_SHARD_COUNT 4096

/*
 * ClusterResId — 16-byte canonical wire encoding (Q4 user-correction).
 *
 *	Maps to LOCKTAG fields via explicit cluster_grd_resid_encode/decode
 *	functions.  DO NOT memcpy(LOCKTAG) directly — wire ABI must be
 *	stable across PG version升级.  Layout intentionally mirrors LOCKTAG
 *	field sizes (16 bytes total) to allow trivial 1:1 mapping today
 *	while keeping encode/decode functions as the wire-ABI boundary.
 */
typedef struct ClusterResId {
	uint32 field1;		/* maps from LOCKTAG.locktag_field1 */
	uint32 field2;		/* maps from LOCKTAG.locktag_field2 */
	uint32 field3;		/* maps from LOCKTAG.locktag_field3 */
	uint16 field4;		/* maps from LOCKTAG.locktag_field4 */
	uint8 type;			/* maps from LOCKTAG.locktag_type */
	uint8 lockmethodid; /* maps from LOCKTAG.locktag_lockmethodid */
} ClusterResId;

StaticAssertDecl(sizeof(ClusterResId) == 16, "ClusterResId wire ABI 16-byte lock (spec-2.14 §2.1)");

/*
 * Opaque ClusterGrdEntry handle.  Body 完全私有 cluster_grd.c file-static
 * per spec-2.15 v0.2 P1.1 — defends forward compat with spec-2.16 grant +
 * spec-2.17 deadlock + Stage 6 DRM layout changes.
 */
typedef struct ClusterGrdEntry ClusterGrdEntry;

/*
 * spec-2.15:  Result enum for entry lookup/create API.
 *
 *  Sentinel separation per CLAUDE.md 规则 8 + spec-2.15 v0.2/v0.3 corrections:
 *  NOT_READY (GUC=0, entry HTAB 未分配) vs NOT_FOUND (lookup miss with
 *  create=false) vs FULL (HTAB cap / holders/waiters/convert cap hit) —
 *  caller 必显式分流,不允许把 NOT_READY 当 NOT_FOUND;不允许把 FULL 当
 *  NOT_FOUND.  ERROR=4 reserved for spec-2.16+ generic fatal path.
 *
 *  v0.2 P2.7:  StaticAssertDecl on enum VALUE not sizeof(enum)
 *  (C enum size implementation-defined, not ABI 契约).
 */
typedef enum ClusterGrdEntryResult {
	CLUSTER_GRD_ENTRY_OK = 0,		 /* entry returned via *out */
	CLUSTER_GRD_ENTRY_NOT_READY = 1, /* GUC=0,entry HTAB 未分配 / skeleton 状态 */
	CLUSTER_GRD_ENTRY_NOT_FOUND = 2, /* lookup miss with create=false */
	CLUSTER_GRD_ENTRY_FULL = 3,		 /* HTAB / holders/waiters/converts cap hit */
	CLUSTER_GRD_ENTRY_ERROR = 4		 /* RESERVED — spec-2.16+ generic fatal path */
} ClusterGrdEntryResult;

StaticAssertDecl(CLUSTER_GRD_ENTRY_OK == 0, "v0.2 P2.7 enum value ABI lock");
StaticAssertDecl(CLUSTER_GRD_ENTRY_NOT_READY == 1, "v0.2 P2.7 enum value ABI lock");
StaticAssertDecl(CLUSTER_GRD_ENTRY_NOT_FOUND == 2, "v0.2 P2.7 enum value ABI lock");
StaticAssertDecl(CLUSTER_GRD_ENTRY_FULL == 3, "v0.2 P2.7 enum value ABI lock");
StaticAssertDecl(CLUSTER_GRD_ENTRY_ERROR == 4, "v0.2 P2.7 enum value ABI lock");

/*
 * Shmem region "pgrac cluster grd" — Q9 lock-free + Q11 shmem cache.
 *	master[i] = node_id that owns shard i (atomic uint32; LMON-mediated
 *	init + future DRM refresh).
 *	v0.2 observability expansion + v0.4 P1.1 补全:  6 counters for
 *	answer "lookup volume / local vs remote ratio / refresh history /
 *	encode volume" without後续 spec 追加 emit_row.
 */
typedef struct ClusterGrdShared {
	pg_atomic_uint32 master[PGRAC_GRD_SHARD_COUNT];
	pg_atomic_uint32 master_map_initialized;	 /* 0 until LMON init */
	pg_atomic_uint64 resid_encode_count;		 /* incremented per encode */
	pg_atomic_uint64 shard_lookup_count;		 /* total lookups */
	pg_atomic_uint64 local_master_lookup_count;	 /* lookup_master == self */
	pg_atomic_uint64 remote_master_lookup_count; /* lookup_master != self */
	pg_atomic_uint64 master_map_refresh_count;	 /* init + future DRM */

	/* spec-2.15 v0.3:  3 NEW public atomic counters (P1.3 scope 收紧 —
	 * 删 holder/waiter/convert counter,推 spec-2.16 配 mutator API).
	 * entry_current_count is an internal current-size source for soft-cap
	 * and observability; it is not a separate pg_cluster_state row. */
	pg_atomic_uint64 entry_current_count;	 /* current live HTAB entries */
	pg_atomic_uint64 entry_create_count;	 /* lifetime ++ on HASH_ENTER_NULL OK + new */
	pg_atomic_uint64 entry_lookup_hit_count; /* lifetime ++ on OK return (hit 语义;P2.5) */
	pg_atomic_uint64 entry_full_count;		 /* lifetime ++ on FULL */

	/* spec-2.16 D1:  4 cap counter + 5 nofail counter (skeleton-init;
	 * mutator bodies + nofail paths land Step 2-4). */
	pg_atomic_uint64 holders_full_count;
	pg_atomic_uint64 waiters_full_count;
	pg_atomic_uint64 converts_full_count;
	pg_atomic_uint64 ngranted_promoted_count;

	pg_atomic_uint64 ges_work_queue_full_count;			/* v0.4 L1.3 */
	pg_atomic_uint64 ges_cleanup_deferred_count;		/* v0.4 L1.3 cleanup dirty-list */
	pg_atomic_uint64 ges_inbound_validation_fail_count; /* v0.4 L1.8 */
	pg_atomic_uint64 ges_reply_deferred_count;			/* v0.5 P1.1 reply dirty-list */
	pg_atomic_uint64 ges_reply_dropped_count;			/* v0.6 L1.1 dirty-list full drop */

	/* spec-2.17 D12 — 6 BAST nofail counter(Q12 v0.6 rename:
	 * sent / received / ack / retry / reject / stale_drop;timeout 拆 3). */
	pg_atomic_uint64 ges_bast_sent_count;
	pg_atomic_uint64 ges_bast_received_count;
	pg_atomic_uint64 ges_bast_ack_count;
	pg_atomic_uint64 ges_bast_retry_count;
	pg_atomic_uint64 ges_bast_reject_count;
	pg_atomic_uint64 ges_bast_stale_drop_count;

	/* spec-2.17 Step 5/8 — deadlock chunked protocol 3 counter(D26c). */
	pg_atomic_uint64 ges_deadlock_probe_drop_count;
	pg_atomic_uint64 ges_deadlock_probe_collision_drop_count;
	pg_atomic_uint64 ges_deadlock_chunk_oo_buffer_overflow_count;

	/* spec-2.17 D28b — backend startup generation atomic counter.
	 * InitProcess() atomic fetch_add 1 → MyProc->cluster_grd_generation.
	 * init 从 1 开始(0 reserved sentinel = uninitialized). */
	pg_atomic_uint64 next_generation;
} ClusterGrdShared;

/* spec-2.17 D28b — extern atomic generation alloc helper(InitProcess hook). */
extern uint64 cluster_grd_alloc_generation(void);

/* spec-2.17 D14-D18 — deadlock detector(skeleton phase;Step 5/8 真激活
 * vertex dict + Tarjan + victim selection). */
extern void cluster_grd_deadlock_lmon_tick(void); /* periodic 500ms */
extern void cluster_grd_inc_deadlock_probe_drop(void);
extern void cluster_grd_inc_deadlock_probe_collision_drop(void);
extern void cluster_grd_inc_deadlock_chunk_oo_buffer_overflow(void);
extern uint64 cluster_grd_deadlock_probe_drop_count(void);
extern uint64 cluster_grd_deadlock_probe_collision_drop_count(void);
extern uint64 cluster_grd_deadlock_chunk_oo_buffer_overflow_count(void);

/* spec-2.17 D21 — cleanup_on_backend_exit(I65 — CANCEL/SIGTERM/
 * on_proc_exit/self-abort;NOT BAST timeout). */
extern void cluster_grd_cleanup_on_backend_exit(int procno);
extern void cluster_grd_check_pending_interrupts(void);

/* spec-2.17 D8 + D12 — BAST handler + 6 counter helpers(skeleton phase). */
extern void cluster_grd_bast_handler(void);	  /* ProcessInterrupts hook */
extern void cluster_grd_cancel_handler(void); /* ProcessInterrupts hook */
extern void cluster_grd_inc_bast_sent(void);
extern void cluster_grd_inc_bast_received(void);
extern void cluster_grd_inc_bast_ack(void);
extern void cluster_grd_inc_bast_retry(void);
extern void cluster_grd_inc_bast_reject(void);
extern void cluster_grd_inc_bast_stale_drop(void);
extern uint64 cluster_grd_bast_sent_count(void);
extern uint64 cluster_grd_bast_received_count(void);
extern uint64 cluster_grd_bast_ack_count(void);
extern uint64 cluster_grd_bast_retry_count(void);
extern uint64 cluster_grd_bast_reject_count(void);
extern uint64 cluster_grd_bast_stale_drop_count(void);

extern Size cluster_grd_shmem_size(void);
extern void cluster_grd_shmem_init(void);
extern void cluster_grd_shmem_register(void);

/*
 * Wire encoding / decoding — Q4 user-correction:  explicit
 * field-by-field encode/decode,  NOT memcpy(LOCKTAG).  Wire ABI boundary.
 *
 *	v0.4 P1.1: cluster_grd_resid_encode() must fetch_add resid_encode_count
 *	each call (observability — was missing in v0.2/v0.3 spec body).
 */
extern void cluster_grd_resid_encode(const LOCKTAG *src, ClusterResId *dst);
extern void cluster_grd_resid_decode(const ClusterResId *src, LOCKTAG *dst);

/*
 * Cluster-aware lock type classifier — Q5 user-correction:  pgrac
 * mapping function,  NOT new LockTagType enum value.  Returns true if
 * LOCKTAG is a cluster-coordinated lock (currently 4 classes:
 * RELATION / TRANSACTION / OBJECT / ADVISORY).  TT/IS/CI/XR/CLUSTER_*
 * deferred to spec-2.X.
 */
extern bool cluster_grd_is_cluster_aware(const LOCKTAG *tag);

/*
 * Performance hook API (P1.2 v0.2 split — Stage 6 single-swap point).
 *
 *	cluster_grd_hash_resource:  pure hash function.  ONLY function
 *	  whose body Stage 6 替换 (xxhash3 / RDMA-aware locality hash).
 *	  Hash input is 14 bytes (P1.1 v0.2):
 *	    field1[4] + field2[4] + field3[4] + type[1] + lockmethodid[1]
 *	  Skip ONLY field4 (tuple offset);  lockmethodid IS included for
 *	  identity preservation.
 *	cluster_grd_shard_for_hash:  pure modulo.
 *	cluster_grd_shard_for_resource:  compose hash_resource +
 *	  shard_for_hash (no counter increment).
 *	cluster_grd_lookup_master:  full lookup (shard_for_resource +
 *	  master[shard] + total counter + local-or-remote counter).
 *	  Returns master node_id, or -1 when the master map is not initialized.
 *	cluster_grd_shard_lookup:  thin compat wrapper.  Returns shard_id
 *	  + increments total counter only (does NOT read master, hence
 *	  does NOT increment local/remote counter).
 *
 *	Counter invariant (v0.4 P1.2):
 *	  shard_lookup_count >= local_master_lookup_count +
 *	                        remote_master_lookup_count
 *	  (>= not =;  shard_lookup increments total without master read).
 */
extern uint64 cluster_grd_hash_resource(const ClusterResId *resid);
extern uint32 cluster_grd_shard_for_hash(uint64 hash);
extern uint32 cluster_grd_shard_for_resource(const ClusterResId *resid);
extern int32 cluster_grd_lookup_master(const ClusterResId *resid);
extern uint32 cluster_grd_shard_lookup(const ClusterResId *resid);

/*
 * Master mapping — Q10 + Q11 user-correction:
 *	master[shard_id] is initialized via cluster_grd_master_map_init()
 *	on LMON startup.  Mapping is declared-node-aware:
 *	  declared_list = scan 0..CLUSTER_MAX_NODES via existing
 *	    cluster_conf_lookup_node() (skip NULL);  scan order = sorted
 *	    node_id ascending;  Assert(len == cluster_conf_node_count())
 *	  idx = shard_id % len(declared_list)
 *	  master[shard_id] = declared_list[idx]
 *	NOT modulo cluster_node_id directly (node_id can be sparse;
 *	pgrac.conf allows 0/2/5 declared without 1/3/4).
 */
extern int32 cluster_grd_shard_master(uint32 shard_id);
extern bool cluster_grd_is_local_master(uint32 shard_id);

/*
 * Master map lifecycle — LMON entry point (D3).
 */
extern void cluster_grd_master_map_init(void);
extern void cluster_grd_master_map_refresh(void); /* Stage 6 DRM placeholder */

/*
 * Observability accessors — D6 dump_grd consumers (8 emit_row in
 * cluster_debug.c).
 *
 *	v0.4 P1.1 修正:  v0.3 D6 列 8 emit_row 但 v0.3 §2.1 extern 缺
 *	cluster_grd_shard_lookup_count + cluster_grd_resid_encode_count;
 *	v0.4 补 2 accessor → 7 total.
 */
extern uint32 cluster_grd_local_master_count(void);
extern uint32 cluster_grd_remote_master_count(void);
extern uint64 cluster_grd_shard_lookup_count(void);
extern uint64 cluster_grd_local_master_lookup_count(void);
extern uint64 cluster_grd_remote_master_lookup_count(void);
extern uint64 cluster_grd_resid_encode_count(void);
extern uint64 cluster_grd_master_map_refresh_count_get(void);


/* ============================================================
 * spec-2.15:  entry table infrastructure (HTAB + named tranche +
 *   opaque entry + sentinel API + observability).  caller-side
 *   LockAcquire integration + holders/waiters/convert mutator API
 *   推 spec-2.16(本 spec 0 caller / 0 mutation).
 * ============================================================ */

/*
 * spec-2.15 v0.3 P1.1:  named tranche request hook (lifecycle fix).
 *
 *  Called ONCE from cluster_request_shmem() during the
 *  process_shmem_requests_in_progress window — RequestNamedLWLockTranche
 *  has lifetime constraint that prohibits calls outside this window.
 *
 *  size_fn (cluster_grd_shmem_size) MUST stay pure (idempotent, no side
 *  effect) — cluster_shmem_get_total_bytes() calls size_fn N times for
 *  diagnostics;  if Request were hidden in size_fn the diagnostic path
 *  would re-call RequestNamedLWLockTranche → FATAL.
 *
 *  cluster_grd_shmem_init() then calls GetNamedLWLockTranche("ClusterGrdShard")
 *  to obtain the array pointer;  PG lwlock.c initializes the 4096 LWLock
 *  automatically — DO NOT call LWLockInitialize manually.
 */
extern void cluster_grd_request_lwlocks(void);

/*
 * spec-2.15:  Entry lookup/create API — 唯一入口 caller 拿 entry handle.
 *
 *  GUC `cluster.grd_max_entries == 0` 时 → CLUSTER_GRD_ENTRY_NOT_READY
 *  (entry HTAB 未分配);caller 必处理 (spec-2.16 caller-side 真激活前
 *  固定走此路径).
 *
 *  create=true → ShmemInitHash HASH_ENTER_NULL (v0.3 P1.2);
 *  create=false → HASH_FIND.
 *
 *  v0.4 P1.2 + review fix: lookup existing entry first; soft cap only
 *  applies to new entries and reads entry_current_count atomically.
 *  hard cap HASH_ENTER_NULL 返回 NULL → FULL (defensive 防 shmem OOM).
 *
 *  shard partition LWLock (named tranche) acquired internally;
 *  caller 无需自己持锁.
 *
 *  v0.4 P1.1 hash 单源 (I13):shard_id 和 HTAB bucket 用同一
 *  cluster_grd_hash_resource() 的 32-bit 投影 + hash_search_with_hash_value();
 *  绝不调 hash_search() 让 dynahash 用 HASHCTL.hash 自己算.
 */
extern ClusterGrdEntryResult cluster_grd_entry_lookup_or_create(const ClusterResId *resid,
																bool create, ClusterGrdEntry **out);

/*
 * spec-2.15:  Entry release — RESERVED no-op (v0.3 P1.3 contract unified).
 *
 *  本 spec 不保证任何 side effect:
 *    - 不 decrement refcount
 *    - 不 remove entry from HTAB
 *    - 不改 holders/waiters/converts 状态
 *
 *  caller 调 release 是 stub safe-call (本 spec 0 caller,future spec-2.16+
 *  caller-side 集成时按 reserved no-op 契约调用).  真 refcount + HASH_REMOVE
 *  + reclaim logic 在 spec-2.16 caller-side 实装 (API signature 不变,body
 *  加 logic).
 */
extern void cluster_grd_entry_release(ClusterGrdEntry *entry);

/*
 * spec-2.15 v0.3:  6 observability accessor extern — scope 收紧 P1.3 选 B
 *  删 3 holder/waiter/convert accessor (推 spec-2.16).
 *
 *  All accessors are atomic_read O(1) / GUC / init-time constants —
 *  cleanly observable (P1.2:no PG-HTAB-unfriendly bucket_count/max_chain).
 */
extern int cluster_grd_max_entries_get(void);  /* GUC value (derived) */
extern int cluster_grd_entry_count(void);	   /* current live entries (atomic) */
extern Size cluster_grd_allocated_bytes(void); /* init 时计算固定 (derived) */
extern uint64
cluster_grd_entry_create_count(void); /* lifetime ++ on HASH_ENTER_NULL OK + new (atomic) */
extern uint64
cluster_grd_entry_lookup_hit_count(void); /* lifetime ++ on OK return (atomic;P2.5 hit 语义) */
extern uint64 cluster_grd_entry_full_count(void); /* lifetime ++ on FULL (atomic) */


/*
 * spec-2.15 D8:  SRF row visitor.  Iterates the entry HTAB (when
 * allocated) and invokes `visitor(ctx, row_fields)` per entry under
 * per-entry slock_t snapshot.  The 11 row_fields columns are
 * (shard_id, field1, field2, field3, field4, type, lockmethodid,
 * ngranted, nwaiters, nconverts, state_flags) — all stored as int32.
 *
 * This indirection lets `cluster_get_grd_entries` SRF (cluster_grd_
 * srf.c) emit rows without exposing the private ClusterGrdEntry layout.
 * GUC=0 / htab==NULL → visitor invoked zero times (caller sees empty
 * result set, matching the NOT_READY sentinel surface).
 *
 * **spec-2.16 forward-link (P2.4 + I14)**:  before caller-side
 * LockAcquire 集成 ships, this visitor must amend locking — wrap
 * hash_seq_search in full 4096-shard LW_SHARED acquire or chunked
 * snapshot to defend concurrent HASH_ENTER_NULL writers.  本 spec
 * 0 caller → 0 row → 无并发问题.
 */
typedef void (*ClusterGrdEntryRowVisitor)(void *ctx, const int32 row_fields[11]);

extern void cluster_grd_entries_walk(ClusterGrdEntryRowVisitor visitor, void *ctx);


/* ============================================================
 * spec-2.16 D1:  mutator + LOCKMODE compat + 4 cap counter + 5
 *   nofail counter + should_globalize + 6-step state machine helpers.
 *
 *   Skeleton phase (Step 1):  extern + struct + counter init only;
 *   mutator bodies + state machine activation land in Step 2-4 per
 *   spec-2.16 §5 Sprint A plan.  Stub bodies use规则 8
 *   ERRCODE_FEATURE_NOT_SUPPORTED with errhint pointing to the
 *   activating Step.
 * ============================================================ */

/*
 * 4-tuple GES holder identity (spec-2.16 v0.3 L1.7 + v0.4 I49):
 *   (node_id, cluster_epoch, procno, request_id)
 *
 *   - node_id:       originating cluster_node_id
 *   - cluster_epoch: epoch at request time (per spec-2.4); used for
 *                    stale-epoch cleanup discrimination (I48)
 *   - procno:        PG ProcNumber of the requesting backend
 *   - request_id:    per-backend monotonic counter (D3 pending key
 *                    disambiguator)
 *
 *   Used in:
 *     - GRD entry holders[] / waiters[] (spec-2.16 mutator)
 *     - cluster_grd_pending.h key (4-tuple HTAB)
 *     - inbound 5-item validation (I36-I37)
 *     - GesRequestPayload / GesReplyPayload wire (cluster_ges.h
 *       inlines the 6 uint32 fields explicitly per L8 frontend safety)
 */
typedef struct ClusterGrdHolderId {
	uint32 node_id;
	uint32 procno;
	uint64 cluster_epoch;
	uint64 request_id;
} ClusterGrdHolderId;

StaticAssertDecl(sizeof(ClusterGrdHolderId) == 24, "ClusterGrdHolderId 4-tuple ABI 24-byte lock");

/*
 * 4 cap counter + 5 nofail counter (Q12 v0.6).
 *
 *   cap counter (4):  holders_full / waiters_full / converts_full /
 *     ngranted_promoted (set each cap surface, observability);
 *   nofail counter (5):
 *     - ges_work_queue_full_count       (v0.4 L1.3)
 *     - ges_cleanup_deferred_count      (v0.4 L1.3 cleanup dirty-list)
 *     - ges_inbound_validation_fail_count (v0.4 L1.8 5-item validation)
 *     - ges_reply_deferred_count        (v0.5 P1.1 reply dirty-list)
 *     - ges_reply_dropped_count         (v0.6 L1.1 dirty-list full drop)
 *
 *   All atomic uint64;  hot path 0-LWLock per L106.  Counters reside
 *   in ClusterGrdShared (extending spec-2.15 v0.3 entry counters).
 */

/* extern accessors (cluster_debug emit_row + observability views) */
extern uint64 cluster_grd_holders_full_count(void);
extern uint64 cluster_grd_waiters_full_count(void);
extern uint64 cluster_grd_converts_full_count(void);
extern uint64 cluster_grd_ngranted_promoted_count(void);

extern uint64 cluster_grd_ges_work_queue_full_count(void);
extern uint64 cluster_grd_ges_cleanup_deferred_count(void);
extern uint64 cluster_grd_ges_inbound_validation_fail_count(void);
extern uint64 cluster_grd_ges_reply_deferred_count(void);
extern uint64 cluster_grd_ges_reply_dropped_count(void);

/* atomic inc helpers (D4 outbound + D5 work_queue producers) */
extern void cluster_grd_inc_ges_work_queue_full(void);
extern void cluster_grd_inc_ges_cleanup_deferred(void);
extern void cluster_grd_inc_ges_inbound_validation_fail(void);
extern void cluster_grd_inc_ges_reply_deferred(void);
extern void cluster_grd_inc_ges_reply_dropped(void);

/*
 * should_globalize (D10) — O(1) no-catalog allowlist.
 *
 *   Returns true if the given LOCKTAG should be cluster-globalized
 *   (route through GES) rather than handled by PG-local lmgr only.
 *   Skeleton (Step 1):  return false unconditionally (mirrors v0.3
 *   skeleton DEFER contract).  Real allowlist body lands Step 4 D10.
 */
extern bool cluster_grd_should_globalize(const struct LOCKTAG *tag);

/*
 * LOCKMODE compatibility — Q2 v0.4 ★ B:  expose PG lmgr/lock.c
 * LockMethodConflicts helper rather than复刻 matrix.
 *
 *   For now (Step 1) provide a thin wrapper extern that Step 4 D9
 *   wires to the (NEW exposed) lmgr/lock.c LockMethodConflicts symbol.
 *   Skeleton body returns true (any mode conflicts with any) to keep
 *   safety contract before Step 4 activation.
 */
extern bool cluster_grd_lockmode_conflicts(int /* LOCKMODE */ held, int /* LOCKMODE */ wanted);

/*
 * Mutator API — caller (lmgr/lock.c PGRAC MODIFICATIONS Step 4 D9)
 * grants / releases / converts a holder under the shard partition
 * LWLock + entry slock_t.  Skeleton (Step 1):  ERRCODE_FEATURE_NOT_SUPPORTED
 * + errhint pointing to Step 4 activation.
 *
 *   grant_holder:    add to entry->holders[] at given mode
 *   release_holder:  remove (refcount 1→0 path);  may HASH_REMOVE entry
 *                    when ngranted==0 && nwaiters==0 && nconverts==0
 *   add_waiter:      add to entry->waiters[]
 *   promote_waiter:  waiter → holder (grant decision callback)
 *
 *   All return ClusterGrdEntryResult sentinel (reuse spec-2.15 enum;
 *   FULL covers all 4 cap surfaces).
 */
extern ClusterGrdEntryResult cluster_grd_entry_grant_holder(ClusterGrdEntry *entry,
															const ClusterGrdHolderId *holder,
															int /* LOCKMODE */ mode);
extern ClusterGrdEntryResult cluster_grd_entry_release_holder(ClusterGrdEntry *entry,
															  const ClusterGrdHolderId *holder);
extern ClusterGrdEntryResult cluster_grd_entry_add_waiter(ClusterGrdEntry *entry,
														  const ClusterGrdHolderId *holder,
														  int /* LOCKMODE */ mode);
extern ClusterGrdEntryResult cluster_grd_entry_promote_waiter(ClusterGrdEntry *entry,
															  const ClusterGrdHolderId *holder);

/*
 * spec-2.21 D5 NEW:  minimal ADVISORY mutator + inspection API.
 *
 *   These extend spec-2.15/2.16 mutator scaffolding for the spec-2.21
 *   ADVISORY-only MVP — full RELATION/TRANSACTION/OBJECT activation is
 *   spec-2.25 lock class expansion.
 *
 *   Snapshot helpers (no_remote_holder / no_pending_waiter / no_pending_
 *   convert / master_generation):  caller pre-acquires the shard
 *   partition LWLock + entry slock_t before calling; helpers return
 *   atomic snapshot of relevant state for S3 local-fast-path 5-check.
 *
 *   Reservation API (reservation_create / cancel / promote):  spec-2.21
 *   S3 reserves a holder slot under shard LWLock, releases LWLock for
 *   PG-native LockAcquire, then re-acquires + promotes (or cancels on
 *   revalidate fail per HC9 / P2.3).
 */
extern bool cluster_grd_entry_has_remote_holder(ClusterGrdEntry *entry, int32 self_node_id);
extern bool cluster_grd_entry_has_pending_waiter(ClusterGrdEntry *entry);
extern bool cluster_grd_entry_has_pending_convert(ClusterGrdEntry *entry);

/* Entry-level generation counter (bumped on every mutator under entry->lock). */
extern uint64 cluster_grd_entry_generation(ClusterGrdEntry *entry);

extern ClusterGrdEntryResult cluster_grd_reservation_create(ClusterGrdEntry *entry,
															const ClusterGrdHolderId *holder,
															int /* LOCKMODE */ mode);
extern ClusterGrdEntryResult cluster_grd_reservation_cancel(ClusterGrdEntry *entry,
															const ClusterGrdHolderId *holder);
extern ClusterGrdEntryResult cluster_grd_reservation_promote(ClusterGrdEntry *entry,
															 const ClusterGrdHolderId *holder);

/*
 * spec-2.21 D5 high-level helpers — encapsulate entry slock + 5-check +
 * reservation/promote under cluster_grd.c so callers in cluster_lock_
 * acquire.c don't need internal struct visibility.
 *
 *   try_reserve:  S3.1-S3.3 — lookup/create entry, snapshot generation,
 *     run 5-check, reservation_create.  Returns:
 *       _OK with fast_path_out=true:  caller may use PG-native fast path
 *       _OK with fast_path_out=false: caller must walk S4 remote path
 *       _FULL / _NOT_READY:           caller maps to FAIL_RESERVATION_FULL
 *                                     / FAIL_GRD_NOT_READY
 *
 *   revalidate_and_promote:  S5 — re-acquire entry slock, verify no remote
 *     holder ascended after snapshot, promote reservation -> holder.
 *     Returns OK on success;  NOT_FOUND if reservation already lost (race).
 *
 *   release_holder_by_id:  S6 — release holder under entry slock + remove
 *     entry from HTAB if last holder.
 */
extern ClusterGrdEntryResult cluster_grd_try_reserve(const ClusterResId *resid,
													 const ClusterGrdHolderId *holder, int mode,
													 int32 self_node_id, bool *fast_path_out,
													 uint64 *gen_snapshot_out);

extern ClusterGrdEntryResult cluster_grd_revalidate_and_promote(const ClusterResId *resid,
																const ClusterGrdHolderId *holder,
																int32 self_node_id,
																uint64 gen_snapshot);

extern ClusterGrdEntryResult cluster_grd_release_holder_by_id(const ClusterResId *resid,
															  const ClusterGrdHolderId *holder);

extern ClusterGrdEntryResult cluster_grd_cancel_reservation_by_id(const ClusterResId *resid,
																  const ClusterGrdHolderId *holder);

/*
 * CSSD DEAD cleanup entry point (Step 4 D11 + LMON tick polling D8).
 *
 *   Called by LMON tick body when cluster_cssd_get_dead_generation()
 *   detects a newly-dead peer (per spec-2.16 v0.5 P1.2 last_dead_bitmap
 *   diff).  Sweeps all GRD entries: holder.node_id == dead_node_id
 *   → release (independent of epoch per I48).
 *
 *   Idempotent (safe re-entry on bitmap re-sync).  Skeleton (Step 1):
 *   no-op + DEBUG2 log.  Body activation Step 4 D11.
 */
extern void cluster_grd_cleanup_on_node_dead(int32 dead_node_id);

/*
 * LMON tick poll — newly-dead bitmap diff per spec-2.16 v0.5 P1.2 + I51.
 *
 *   Called from cluster_lmon_main_tick body BEFORE reconfig_lmon_tick
 *   (per I47 LMON tick order: S1 GRD sweep → S2 reconfig epoch bump).
 *   Polls cluster_cssd_get_dead_generation() — if changed, scans peer
 *   states to build current_dead_bitmap, diffs against static
 *   last_dead_bitmap, and invokes cleanup_on_node_dead for each
 *   newly-dead peer.  SUSPECTED state不计;DEAD→ALIVE recovery 不重 sweep.
 *
 *   last_dead_bitmap is committed AFTER sweep (crash-safe idempotent;
 *   reboot 从 0 重建).
 */
extern void cluster_grd_lmon_tick_dead_sweep(void);

/*
 * Stale-epoch sweep (Step 4 D11):  holder.cluster_epoch < current_epoch
 *   → release.  Triggered post-reconfig epoch bump (LMON tick Step
 *   S2 per I47).  Independent rule from DEAD cleanup (I48 — touched
 *   conditions don't merge).
 */
extern void cluster_grd_cleanup_stale_epoch(uint64 current_epoch);

#endif /* !FRONTEND */

#endif /* CLUSTER_GRD_H */
