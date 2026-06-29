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

#include "cluster/cluster_conf.h" /* CLUSTER_MAX_NODES (spec-4.6 D1 bitmap sizing) */
#include "datatype/timestamp.h"	  /* TimestampTz (spec-5.1b ClusterGrdConvert) */
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
/*
 * spec-4.6 D1/D4 — per-shard recovery phase.
 *
 *	NORMAL      steady state, requests served.
 *	FROZEN      reconfig accepted a dead master for this shard; new
 *	            requests short-wait (cluster.grd_remaster_wait_ms) then
 *	            fail-closed 53R9I.
 *	REBUILDING  master map already remastered (D2); cooperative holder
 *	            rebuild (D3) in flight; requests treated like FROZEN.
 *
 *	Stored as pg_atomic_uint32 per shard (no atomic u8 in PG); only
 *	LMON transitions phases, backends read lock-free.
 */
typedef enum ClusterGrdShardPhase {
	GRD_SHARD_NORMAL = 0,
	GRD_SHARD_FROZEN = 1,
	GRD_SHARD_REBUILDING = 2,
} ClusterGrdShardPhase;

/* spec-5.16 D2 — direction of the current remaster episode (shmem-only). */
typedef enum ClusterGrdRemasterDirection {
	GRD_REMASTER_DIR_NONE = 0, /* idle / never run */
	GRD_REMASTER_DIR_FAIL = 1, /* spec-4.6 failure-driven */
	GRD_REMASTER_DIR_JOIN = 2  /* spec-5.16 join-driven */
} ClusterGrdRemasterDirection;

typedef struct ClusterGrdShared {
	pg_atomic_uint32 master[PGRAC_GRD_SHARD_COUNT];

	/* spec-4.6 D2 — per-shard remaster generation:  ++ ONLY when this
	 * shard's master actually changes (failure-driven remaster).  Shmem
	 * scope ONLY for idempotency / observability;  NEVER placed on the
	 * wire (Q3-C:  the wire token stays the existing
	 * shard_master_generation = (accepted_epoch<<32)|lms_restart_gen,
	 * because 4.6 remasters 1:1 with the reconfig epoch bump). */
	pg_atomic_uint32 master_generation[PGRAC_GRD_SHARD_COUNT];

	/* spec-4.6 D1/D4 — per-shard recovery phase (ClusterGrdShardPhase). */
	pg_atomic_uint32 shard_phase[PGRAC_GRD_SHARD_COUNT];

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

	/* spec-2.24 D12 — cleanup_skip_stale_cancel(D5 4-tuple match fail).
	 * Placed in cluster_grd_state instead of cluster_lmd_graph_state
	 * because the increment site is in the cluster_grd CANCEL dispatch
	 * helper(close to ges_inbound_validation_fail_count semantics). */
	pg_atomic_uint64 cleanup_skip_stale_cancel_count;

	/* spec-2.25 D13 — should_globalize gate command hit count for
	 * RELATION + OBJECT path (HC23..HC27 branches).  Distinct from
	 * ADVISORY (covered by existing should_globalize_advisory_count
	 * — if any).  Bumped on every successful gate-true return for
	 * either of the two NEW LOCKTAG types.  Surfaced via dump_grd. */
	pg_atomic_uint64 relation_object_cluster_path_count;

	/* spec-2.26 D5 — should_globalize gate hit count for LOCKTAG_
	 * TRANSACTION path (HC39 / HC47 branches).  Bumped on every
	 * successful gate-true return from XactLockTable* code paths
	 * (auto-acquired by every write xact via XactLockTableInsert,
	 * by waiters via XactLockTableWait, etc.).  Mutually exclusive
	 * with relation_object_cluster_path_count (different LOCKTAG
	 * type).  Surfaced via dump_grd (40th row after spec-2.25). */
	pg_atomic_uint64 transaction_cluster_path_count;

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

	/* spec-4.6 D2 — reconfig epoch of the most recent failure-driven
	 * remaster (observability + idempotency cross-check). */
	pg_atomic_uint64 reconfig_remaster_epoch;

	/* spec-4.6 D1 — GRD recovery sequence cursor (LMON-only writer;
	 * shmem so an LMON respawn resumes instead of replaying P1-P7).
	 *	recovery_last_event_id   last reconfig event consumed
	 *	recovery_state           ClusterGrdRecoveryState
	 *	recovery_dead_bitmap     accepted dead bitmap of the episode
	 *	recovery_event_old_epoch episode's pre-bump epoch (observe gate)
	 *	recovery_redeclare_generation  cooperative-rebind barrier ++;
	 *	   backends ack into PGPROC.cluster_grd_redeclare_acked
	 *	recovery_barrier_deadline  TimestampTz barrier expiry */
	pg_atomic_uint64 recovery_last_event_id;
	pg_atomic_uint32 recovery_state;
	pg_atomic_uint64 recovery_dead_bitmap[(CLUSTER_MAX_NODES + 63) / 64];
	pg_atomic_uint64 recovery_event_old_epoch;
	pg_atomic_uint64 recovery_redeclare_generation;
	pg_atomic_uint64 recovery_barrier_deadline;

	/*
	 * spec-4.6 P0-1 (Fable review) — the epoch this episode is LOCKED to.
	 * Set when WAIT_EPOCH passes; the rebind barrier, the REDECLARE_DONE
	 * announcements, and the P6 sweep are ALL coherent against this one
	 * value.  If a SECOND epoch bump lands mid-episode (CSSD
	 * dead_generation bumps on ANY peer transition -> reconfig re-fire ->
	 * coordinator epoch++, even with an unchanged dead set), the tick
	 * aborts the episode to IDLE and re-consumes the event under the new
	 * epoch (shards stay frozen).  Without this lock the P6 sweep would
	 * use a fresher epoch than the holders were rebound under and delete
	 * a live holder -> cross-node double grant (rule 8.A).
	 */
	pg_atomic_uint64 recovery_episode_epoch;

	/* spec-4.6 P0#3 cluster gate — per-node epoch for which that node
	 * last announced "local rebind barrier complete" (REDECLARE_DONE).
	 * P6 requires done_epoch[s] >= current epoch for EVERY survivor. */
	pg_atomic_uint64 recovery_done_epoch[CLUSTER_MAX_NODES];

	/* spec-4.6 D5 — 13 grd_recovery counters (dump category
	 * 'grd_recovery';  each has a t/249 leg).  Incremented along
	 * D1-D4 paths as the corresponding deliverable lands. */
	pg_atomic_uint64 remaster_started_count;		   /* P1 freeze entered */
	pg_atomic_uint64 remaster_done_count;			   /* P7 unfreeze reached */
	pg_atomic_uint64 remaster_failed_count;			   /* barrier/timeout fail-closed */
	pg_atomic_uint64 shards_remastered_count;		   /* D2 shards moved */
	pg_atomic_uint64 holders_redeclared_count;		   /* D3 redeclare accepted */
	pg_atomic_uint64 holders_rebound_count;			   /* D3 old→new rebind done (L14) */
	pg_atomic_uint64 waiters_requeued_count;		   /* D3 waiter re-declared */
	pg_atomic_uint64 converts_requeued_count;		   /* D3 convert re-declared */
	pg_atomic_uint64 stale_request_drop_count;		   /* D4 stale gen/epoch drop (53R9J) */
	pg_atomic_uint64 rebuild_timeout_count;			   /* D3 barrier timeout (53R9I) */
	pg_atomic_uint64 block_path_failclosed_count;	   /* D4 GCS/PCM 53R9K (L12) */
	pg_atomic_uint64 unaffected_holder_survived_count; /* L13 sweep-scope guard */
	pg_atomic_uint64 stale_holder_swept_count;		   /* P6 post-barrier sweep (L15) */

	/* spec-5.1b D9 — convert state-machine observability counters.
	 * convert_queue_full reuses the existing converts_full_count above;
	 * these three cover the in-place / enqueued / illegal verdicts.
	 * Bumped by cluster_grd_entry_request_convert under entry->lock. */
	pg_atomic_uint64 convert_granted_inplace_count;
	pg_atomic_uint64 convert_enqueued_count;
	pg_atomic_uint64 convert_illegal_count;

	/*
	 * spec-5.10 — GES enqueue lock-starvation fairness.
	 *
	 *	starvation_protection_enabled is the cluster-global runtime toggle
	 *	(1 = on, default).  The GES grant decision reads it directly so a
	 *	`cluster.ges_starvation_protection = off` SIGHUP takes effect on the
	 *	next grant without waiting for the LMON sweep that clears already-boosted
	 *	waiters (spec-5.10 §2.3 / §3.7 / P1#1).  The four counters are
	 *	observability only (dump_grd; D7).
	 */
	pg_atomic_uint32 starvation_protection_enabled;
	/*
	 * spec-5.10 fix-forward — runtime-off sweep request.  The protection
	 * assign-hook sets this when the toggle goes OFF; the LMON tick test-and-
	 * clears it and runs cluster_grd_clear_all_boosted in the cluster context
	 * (P1#1: never sweep the shared GRD/LMD graph from a GUC assign-hook).
	 */
	pg_atomic_uint32 starvation_sweep_pending;
	pg_atomic_uint64 starvation_boost_count;				/* waiters boosted to HOL */
	pg_atomic_uint64 starvation_barrier_enqueued_count;		/* jumpers held behind a boost */
	pg_atomic_uint64 starvation_barrier_publish_fail_count; /* edge publish/revalidate fail */
	pg_atomic_uint64 starvation_max_skip_observed;			/* high-water skip_count seen */

	/*
	 * spec-5.16 D2/D3b — online node-join remaster (PCM block snap-back fence
	 * + optional GRD logical-lock rebalance).  Shmem-ONLY, NEVER on the wire
	 * (INV-R2:  the existing epoch token is the staleness discriminator;  the
	 * intra-epoch old(survivor)/new(joiner) ambiguity is serialized by FREEZE,
	 * not by these fields).
	 *
	 *	join_pcm_fence_epoch       JOIN_COMMITTED epoch the joiner-home block
	 *	                           fence is armed for (0 = never armed).  Armed
	 *	                           SYNCHRONOUSLY by 5.15 commit_member /
	 *	                           note_self_admitted (D3b, INV-R13) on the
	 *	                           joiner BEFORE is_member(self) flips, and by
	 *	                           each survivor's LMON P0-accept (D2).
	 *	                           Monotonic max (a later join re-arms higher).
	 *	join_pcm_fenced_member     the rejoining MEMBER set this fence covers
	 *	                           (PCM home = hash%declared_count = node domain,
	 *	                           NOT the 4096-shard domain).  CLUSTER_MAX_NODES
	 *	                           bits as uint64 words.
	 *	recovery_direction         ClusterGrdRemasterDirection — FAIL (4.6) /
	 *	                           JOIN (5.16) / NONE (idle).  Observability +
	 *	                           recompute selection;  does not change the FSM.
	 */
	pg_atomic_uint64 join_pcm_fence_epoch;
	pg_atomic_uint64 join_pcm_fenced_member[(CLUSTER_MAX_NODES + 63) / 64];
	pg_atomic_uint32 recovery_direction;

	/* spec-5.16 D5 — join-direction remaster counters (dump_grd grd_recovery
	 * segment;  kept distinct from the failure-driven remaster_* counters so
	 * ops can tell the two remaster kinds apart — §8 Q6-A). */
	pg_atomic_uint64 join_remaster_started_count;			 /* JOIN episode entered */
	pg_atomic_uint64 join_remaster_done_count;				 /* JOIN episode reached IDLE */
	pg_atomic_uint64 join_shards_remastered_count;			 /* GRD shards moved to joiner */
	pg_atomic_uint64 join_block_views_rebuilt_count;		 /* joiner-home fences lifted */
	pg_atomic_uint64 join_block_recovering_failclosed_count; /* 53R9L denied (both gates) */
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

/* spec-2.24 D7 — before_shmem_exit callback wrapper for InitPostgres
 * registration site.  Reads MyProcNumber + delegates to cleanup_on_
 * backend_exit (idempotent per I-cleanup-1). */
extern void cluster_grd_cleanup_on_backend_exit_callback(int code, Datum arg);

/* spec-2.24 D8 — local stale-procno sweep helper.  Called from LMD
 * periodic safety net (HC28 — local-only;remote node death via D9). */
extern int cluster_grd_sweep_local_stale_procnos(void);

extern void cluster_grd_check_pending_interrupts(void);

/* spec-2.17 D8 + D12 — BAST handler + 6 counter helpers(skeleton phase). */
extern void cluster_grd_bast_handler(void); /* ProcessInterrupts hook */
/* spec-5.1c D5: pure best-effort delivery guard for a local BAST target. */
extern bool cluster_grd_bast_local_deliver_ok(uint32 procno, int proc_count, uint64 holder_epoch,
											  uint64 current_epoch, int target_pid,
											  int target_backendid);
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
 * spec-4.6 D2 — failure-driven remaster (NOT affinity/DRM, NOT
 * ALIVE→ALIVE).
 *
 *	Called by LMON AFTER quorum accepts the reconfig epoch (eviction /
 *	fence result included), the epoch bump, and the scoped stale sweep.
 *	Deterministically re-assigns every shard whose CURRENT master has
 *	its bit set in dead_bitmap to a SURVIVOR:
 *	  survivors = declared list (pgrac.conf, ascending) minus dead bits
 *	  new master[shard] = survivors[shard % survivor_count]
 *	All nodes compute the same result from the same accepted membership
 *	snapshot (declared conf + accepted dead bitmap + reconfig_epoch);
 *	NEVER from ad-hoc local peer_state reads.
 *
 *	dead_bitmap:  CLUSTER_MAX_NODES bits as uint64 words
 *	  (word [node >> 6], bit (node & 63));  caller passes the accepted
 *	  reconfig dead bitmap.
 *	Idempotent:  re-running with the same dead_bitmap is a no-op (the
 *	affected masters are survivors after the first run);
 *	master_generation[shard] bumps ONLY when the master actually moves.
 *	Returns the number of shards remastered.
 */
extern uint32 cluster_grd_master_map_remaster(const uint64 *dead_bitmap, uint64 reconfig_epoch);

/*
 * spec-5.16 D1 — membership-aware deterministic master-map recompute.
 *
 *	JOIN-DIRECTION ONLY.  The shipped failure-driven cluster_grd_master_map_
 *	remaster() is NOT touched (Q3=A parallel function).  Per shard i:
 *	  home    = declared[i % declared_count];
 *	  desired = is_member(home) ? home : deterministic_survivor(active, i);
 *	  master[i] != desired -> write + master_generation[i]++.
 *	Returns the number of shards actually moved (0 = idempotent no-op).
 *
 *	active_member is the CLUSTER_MAX_NODES-bit accepted-MEMBER set, uint8[16]
 *	(the reconfig dead/join bitmap shape;  CLUSTER_RECONFIG_DEAD_BITMAP_BYTES).
 *	It MUST be projected from the 5.15 membership SSOT (cluster_membership_is_
 *	member), NEVER ad-hoc CSSD peer_state (INV-R1/R3).  In a pure failure
 *	scenario (no home revival) the per-shard result equals the old remaster
 *	(U2):  the steady-state map is home-based, so home-dead -> survivor[i%n]
 *	matches the dead-only reassignment.
 */
extern uint32 cluster_grd_master_map_recompute_for_membership(const uint8 *active_member,
															  uint64 epoch);

/*
 * spec-5.16 D2/D3b — online-join PCM block snap-back fence accessors.
 *
 *	cluster_grd_arm_join_pcm_fence:  set join_pcm_fence_epoch = the joiner's
 *	    JOIN_COMMITTED epoch + fenced_member_bitmap = rejoining_set.  Called
 *	    SYNCHRONOUSLY (NEVER from the LMON tick — INV-R13) by 5.15
 *	    note_self_admitted on the joiner BEFORE is_member(self) flips, and by
 *	    each survivor's LMON P0-accept.  Idempotent (monotonic-max epoch).
 *	cluster_grd_join_remaster_in_progress:  recovery_direction == JOIN.
 *	The two BufferTag predicates (active_for_shard / block_view_rebuilt) live
 *	in cluster_gcs_block.h (BufferTag is in scope there).
 */
extern void cluster_grd_arm_join_pcm_fence(const uint8 *rejoining_set /* [16] */);
extern bool cluster_grd_join_remaster_in_progress(void);

/*
 * spec-4.6 D2 — per-shard master lookup + wire routing token.
 *
 *	Q3-C:  *out_routing_generation is the EXISTING shard_master_
 *	generation token = (accepted_epoch << 32) | lms_restart_gen,
 *	returned UNCHANGED — the wire ABI is not touched.  The per-shard
 *	remaster generation lives in shmem master_generation[] for
 *	idempotency/observability ONLY and is NEVER placed on the wire
 *	(4.6 remasters 1:1 with the epoch bump, so the epoch alone is a
 *	sufficient staleness discriminator;  stale replies are dropped by
 *	the existing epoch-keyed dedup).
 */
extern int32 cluster_grd_lookup_master_gen(const ClusterResId *resid,
										   uint64 *out_routing_generation);

/* spec-4.6 D2 — per-shard remaster generation read (observability). */
extern uint32 cluster_grd_shard_master_generation(uint32 shard_id);

/* spec-4.6 D1/D4 — shard recovery phase (LMON-only writer). */
extern ClusterGrdShardPhase cluster_grd_shard_phase(uint32 shard_id);
extern void cluster_grd_shard_set_phase(uint32 shard_id, ClusterGrdShardPhase phase);

/*
 * spec-4.6 D1 — GRD recovery sequence (P0-P7, LMON tick driver).
 *
 *	IDLE         no episode in flight.
 *	WAIT_EPOCH   reconfig event accepted;  waiting for the local accepted
 *	             epoch to advance past the episode's old epoch (the
 *	             coordinator bumps in the same tick;  non-coordinator
 *	             survivors observe via IC piggyback) before P1-P5 run.
 *	WAIT_BARRIER P5 redeclare broadcast done;  waiting for every live
 *	             backend's PGPROC ack;  then P6 post-barrier sweep + P7
 *	             unfreeze.  Deadline expiry keeps shards frozen
 *	             (fail-closed) and re-broadcasts.
 */
typedef enum ClusterGrdRecoveryState {
	GRD_RECOVERY_IDLE = 0,
	GRD_RECOVERY_WAIT_EPOCH = 1,
	GRD_RECOVERY_WAIT_BARRIER = 2,
	/* spec-4.6 P0#3 cluster gate:  local barrier done + announced;
	 * waiting for every survivor's REDECLARE_DONE before P6. */
	GRD_RECOVERY_WAIT_CLUSTER = 3,
} ClusterGrdRecoveryState;

extern void cluster_grd_recovery_lmon_tick(void);
extern uint64 cluster_grd_redeclare_generation(void);
/* spec-4.6 P0-1 — the epoch the current episode is locked to (0 = none). */
extern uint64 cluster_grd_redeclare_episode_epoch(void);
/* spec-4.7 D7 (P0 fix) — recovery FSM not IDLE → block phase gate keeps every
 * dead-static-master block RECOVERING for the whole episode (held blocks may
 * not be re-declared to their recovery-aware master yet). */
extern bool cluster_grd_recovery_in_progress(void);
/* spec-4.7 D2/D7 (P0 fix) — survivor block re-declare scan step + completion
 * predicate.  scan_complete MUST hold before REDECLARE_DONE is announced
 * (else a late-scanned held block is served as cold → 8.A double-grant).
 * Exposed for the unit test to drive the cursor without the reconfig FSM. */
extern void grd_block_redeclare_step(uint64 episode_epoch);
extern bool grd_block_redeclare_scan_complete(uint64 episode_epoch);

/* spec-4.6 P0#3 cluster gate — REDECLARE_DONE receiver (cluster_ges.c
 * inbound handler):  record that `node` completed its local rebind
 * barrier for `epoch`. */
extern void cluster_grd_recovery_mark_peer_done(int32 node, uint64 epoch);

/* spec-4.6 D4/D5 — recovery counter bumps for out-of-module call sites. */
extern void cluster_grd_inc_stale_request_drop(void);
extern void cluster_grd_inc_block_path_failclosed(void);
/* spec-5.16 D5 — join-direction 53R9L fail-closed bump (requester + master gate). */
extern void cluster_grd_inc_join_block_failclosed(void);

/* spec-4.6 D5 — bulk snapshot of the 13 grd_recovery counters for the
 * pg_cluster_state dump (category 'grd_recovery';  one t/249 leg each). */
typedef struct ClusterGrdRecoveryCounters {
	uint64 remaster_started;
	uint64 remaster_done;
	uint64 remaster_failed;
	uint64 shards_remastered;
	uint64 holders_redeclared;
	uint64 holders_rebound;
	uint64 waiters_requeued;
	uint64 converts_requeued;
	uint64 stale_request_drop;
	uint64 rebuild_timeout;
	uint64 block_path_failclosed;
	uint64 unaffected_holder_survived;
	uint64 stale_holder_swept;
	/* spec-5.16 D5 — join-direction remaster counters (distinct from the
	 * failure-driven remaster_* above; §8 Q6-A). */
	uint64 join_remaster_started;
	uint64 join_remaster_done;
	uint64 join_shards_remastered;
	uint64 join_block_views_rebuilt;
	uint64 join_block_recovering_failclosed;
} ClusterGrdRecoveryCounters;

extern void cluster_grd_recovery_counters_snapshot(ClusterGrdRecoveryCounters *out);

/* spec-4.6 P0#2 — pre-remaster stale-epoch sweep SCOPED to the affected
 * (dead-master) shards;  affected_shards is a PGRAC_GRD_SHARD_COUNT-bit
 * bitmap (uint64 words).  Global sweeping before the rebind barrier
 * would delete surviving holders on unaffected shards → double grant. */
extern void cluster_grd_cleanup_stale_epoch_scoped(uint64 current_epoch,
												   const uint64 *affected_shards);

/* spec-4.6 P0#3 — post-barrier GLOBAL stale sweep (P6).  Only legal
 * AFTER the redeclare ack barrier completes:  every live backend has
 * rebound ALL its registered grants, so remaining old-epoch state is
 * provably unclaimed (mid-window backend exit / epoch-rejected release)
 * and MUST be removed (a leaked holder blocks the resource forever).
 * Returns the number of slots swept. */
extern uint32 cluster_grd_cleanup_stale_epoch_postbarrier(uint64 current_epoch);

/* spec-4.6 D3 — master-side insert-or-rebind for GES_REDECLARE (see
 * GES_REQ_OPCODE_REDECLARE contract in cluster_ges.h).  struct tag +
 * forward declaration:  the ClusterGrdHolderId typedef appears later
 * in this header. */
struct ClusterGrdHolderId;
extern ClusterGrdEntryResult
cluster_grd_entry_rebind_or_insert_holder(const ClusterResId *resid,
										  const struct ClusterGrdHolderId *new_holder,
										  int32 source_node_id, int lockmode);

/* spec-4.6 D3 — backend-side cooperative rebind walker (defined in
 * cluster_lock_acquire.c;  runs at CFI from cluster_grd_check_pending_
 * interrupts, no-throw).  Walks this backend's cluster_registered
 * LOCALLOCKs, rebinds every grant to the current epoch, and acks the
 * barrier generation on full success. */
extern void cluster_grd_redeclare_all_registered(void);

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
 * spec-5.8 D1c/D1e — waiter deadlock metadata threaded from the waiting
 * backend's D1d wait-state into the master-side WFG vertex.  Neither field is
 * part of the waiter identity (the 4-tuple ClusterGrdHolderId is);  they are
 * carried so a TX edge can resolve holder=(node,xid) and so D5 can ABA-match
 * the victim's wait_seq.  Both may be zero / InvalidTransactionId.
 */
typedef struct ClusterGrdWaiterMeta {
	TransactionId xid;
	uint64 wait_seq;
} ClusterGrdWaiterMeta;

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

/* spec-2.24 D5 — cleanup_skip_stale_cancel(4-tuple match fail in LMD CANCEL dispatch). */
extern uint64 cluster_grd_cleanup_skip_stale_cancel_count(void);

/* spec-2.25 D13 — RELATION + OBJECT cluster gate hit counter (HC23..HC27). */
extern void cluster_grd_inc_relation_object_cluster_path(void);
extern uint64 cluster_grd_relation_object_cluster_path_count(void);

/* spec-2.26 D5 — TRANSACTION cluster gate hit counter (HC39 / HC47). */
extern void cluster_grd_inc_transaction_cluster_path(void);
extern uint64 cluster_grd_transaction_cluster_path_count(void);
extern void cluster_grd_inc_cleanup_skip_stale_cancel(void);
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

/* spec-5.3 L11 — remove a queued REQUEST waiter by (node, procno, request_id);
 * OK if removed, NOT_FOUND if already granted (timeout-vs-grant race). */
extern ClusterGrdEntryResult cluster_grd_cancel_waiter_by_id(const ClusterResId *resid,
															 const ClusterGrdHolderId *holder);

/*
 * spec-5.9 D4 — wait_seq-exact dequeue primitives for the CANCEL_WAIT path.
 * cancel_waiter_by_id_seq removes a queued REQUEST waiter only when its 4-tuple
 * AND spec-5.8 wait_seq match (ABA guard against slot reuse);
 * cancel_convert_by_id is the new convert-queue equivalent (key = node_id,
 * procno, cluster_epoch, convert_request_id, wait_seq).  Both return NOT_FOUND
 * on any mismatch and touch only waiters[]/converts[], never a granted holder.
 */
extern ClusterGrdEntryResult cluster_grd_cancel_waiter_by_id_seq(const ClusterResId *resid,
																 const ClusterGrdHolderId *holder,
																 uint64 wait_seq);
extern ClusterGrdEntryResult cluster_grd_cancel_convert_by_id(const ClusterResId *resid,
															  const ClusterGrdHolderId *holder,
															  uint64 wait_seq);

/* ============================================================
 * spec-2.23 D6 — GRD-owned grant / waiter-pop API.
 *
 *	HC18 / HC19 / HC20 enforcement (spec-2.23 §3.2):  the LMS daemon
 *	must drive cross-node grant decisions through GRD-owned APIs so the
 *	ClusterGrdEntry body remains opaque (header declares forward decl
 *	only at line 104).  spec-2.21 ship paths (`cluster_grd_entry_grant_
 *	holder` / `add_waiter` / `release_holder`) stay intact for the local
 *	S5 promote path; spec-2.23 adds two higher-level entry points that
 *	bundle conflict matrix + waiter-identity carry + entry generation
 *	bump under a single critical section.
 * ============================================================ */

/*
 * Per-entry cap exposed to LMS dispatch so callers can size the
 * conflict-holder snapshot buffer.  The cap mirrors the private
 * cluster_grd.c PGRAC_GRD_MAX_HOLDERS (16);  surfacing the value via
 * the header keeps cluster_lms.c / cluster_ges.c free of cluster_grd.c
 * internal struct layout knowledge.
 */
#define PGRAC_GRD_MAX_HOLDERS_PUBLIC 16

/*
 * Conflict-holder snapshot returned to the LMS dispatch path.  Carries
 * enough identity for the BAST send target list (Step 5 D4 — HC18:
 * targeted BAST filtered by DoLockModesConflict, never peer broadcast).
 */
typedef struct ClusterGrdConflictHolder {
	ClusterGrdHolderId holder;
	int32 source_node_id; /* hosting node — BAST destination */
	LOCKMODE held_mode;
} ClusterGrdConflictHolder;

/*
 * Waiter identity returned by release_and_pop — carries the full
 * 5-tuple parts the LMS needs to build a GES_REPLY GRANT envelope
 * and route it back to the originating backend.
 */
typedef struct ClusterGrdWaiterIdentity {
	ClusterGrdHolderId holder;
	int32 source_node_id;
	uint64 request_id;
	uint64 shard_master_generation;
	uint32 request_opcode;
	LOCKMODE mode;
} ClusterGrdWaiterIdentity;

/*
 * enqueue_or_grant result discriminator.  Step 4 D6 dispatch:
 *   GRANT_NOW         → LMS sends GES_REPLY GRANT immediately
 *   ENQUEUED_WAITER   → LMS triggers targeted BAST (Step 5 D4); reply
 *                       sent later when release_and_pop wakes this waiter
 *   WAIT_QUEUE_FULL   → LMS sends GES_REPLY REJECT 53R71 fail-closed
 *   NOT_READY         → GRD not yet initialised; LMS retries on next tick
 */
typedef enum ClusterGrdGrantAction {
	CLUSTER_GRD_GRANT_NOW = 0,
	CLUSTER_GRD_ENQUEUED_WAITER = 1,
	CLUSTER_GRD_WAIT_QUEUE_FULL = 2,
	CLUSTER_GRD_NOT_READY = 3,
	/* spec-5.5 D5 — conditional (NOWAIT) acquire saw a conflict.  Returned
	 * ONLY by cluster_grd_entry_grant_conditional (try-lock path):  the entry
	 * was NOT mutated (no waiter enqueued, no holder added) so the caller
	 * rejects with GES_REJECT_REASON_LOCK_CONFLICT without a BAST. */
	CLUSTER_GRD_CONFLICT_NOWAIT = 4,
} ClusterGrdGrantAction;

/*
 * Single-shot grant-or-enqueue under the entry lock.
 *
 *	source_node_id / request_id / request_opcode carry forward into the
 *	waiter slot (HC17/HC19) so the LMS can later route a GES_REPLY GRANT
 *	to the originating backend without round-tripping through caller
 *	state.  conflict_holders_out / n_conflict_out fill the BAST target
 *	snapshot when result == ENQUEUED_WAITER; both may be NULL when the
 *	caller doesn't need the snapshot (e.g. GRANT_NOW path).
 *
 *	conflict_holders_out buffer must hold at least PGRAC_GRD_MAX_HOLDERS
 *	entries (16).  *n_conflict_out is 0 on GRANT_NOW.
 */
extern ClusterGrdGrantAction cluster_grd_entry_enqueue_or_grant(
	const ClusterResId *resid, const ClusterGrdHolderId *holder, int32 source_node_id,
	uint64 request_id, uint64 shard_master_generation, uint32 request_opcode,
	int /* LOCKMODE */ lockmode, ClusterGrdConflictHolder *conflict_holders_out,
	int *n_conflict_out);

/*
 * spec-5.8 D1c/D1e — waiter-metadata variant.  Identical to the above but
 * stamps the enqueued waiter's deadlock metadata (xid for TX-edge resolution,
 * wait_seq for D5 ABA revalidate) onto the master-side WFG vertex.  The plain
 * entry point forwards here with a zero meta.  Both meta fields are metadata,
 * never part of the waiter identity.
 */
extern ClusterGrdGrantAction cluster_grd_entry_enqueue_or_grant_meta(
	const ClusterResId *resid, const ClusterGrdHolderId *holder, int32 source_node_id,
	uint64 request_id, ClusterGrdWaiterMeta meta, uint64 shard_master_generation,
	uint32 request_opcode, int /* LOCKMODE */ lockmode,
	ClusterGrdConflictHolder *conflict_holders_out, int *n_conflict_out);

/*
 * spec-5.5 D5 — conditional (NOWAIT) variant of the above for try-locks.
 *
 *	Runs the identical conflict scan + same-backend self-exclusion + no-conflict
 *	grant as cluster_grd_entry_enqueue_or_grant (single source of truth), but on
 *	conflict returns CLUSTER_GRD_CONFLICT_NOWAIT WITHOUT enqueuing a waiter.  The
 *	conflict_holders_out / n_conflict_out snapshot is NOT filled (no BAST is sent
 *	on the conditional path).  Same signature for call-site symmetry.
 */
extern ClusterGrdGrantAction cluster_grd_entry_grant_conditional(
	const ClusterResId *resid, const ClusterGrdHolderId *holder, int32 source_node_id,
	uint64 request_id, uint64 shard_master_generation, uint32 request_opcode,
	int /* LOCKMODE */ lockmode, ClusterGrdConflictHolder *conflict_holders_out,
	int *n_conflict_out);

/* spec-5.8 D1c/D1e — waiter-metadata variant of the conditional (NOWAIT) grant.
 * The NOWAIT path enqueues no waiter, so meta is carried only for call-site
 * symmetry; the plain entry point forwards here with a zero meta. */
extern ClusterGrdGrantAction cluster_grd_entry_grant_conditional_meta(
	const ClusterResId *resid, const ClusterGrdHolderId *holder, int32 source_node_id,
	uint64 request_id, ClusterGrdWaiterMeta meta, uint64 shard_master_generation,
	uint32 request_opcode, int /* LOCKMODE */ lockmode,
	ClusterGrdConflictHolder *conflict_holders_out, int *n_conflict_out);

/*
 * spec-5.10 D7 — GES enqueue lock-starvation fairness GUCs.  max_skips is the
 * bounded jump count after which a starved waiter is boosted to head-of-line
 * (cluster.ges_starvation_max_skips; <= 0 disables boosting).  Defined in
 * cluster_guc.c (backend) / stubbed in the cluster_unit harness.
 */
extern int cluster_ges_starvation_max_skips;

/* spec-5.10 — cluster-global starvation-protection toggle (shared flag). */
extern bool cluster_grd_starvation_protection_enabled(void);
extern void cluster_grd_set_starvation_protection(bool enabled);

/* spec-5.10 D7 — starvation-fairness observability counters. */
extern uint64 cluster_grd_starvation_boost_count(void);
extern uint64 cluster_grd_starvation_barrier_enqueued_count(void);
extern uint64 cluster_grd_starvation_barrier_publish_fail_count(void);
extern uint64 cluster_grd_starvation_max_skip_observed(void);

/* spec-5.10 D8 — boosted-state sweeps (LMON context; runtime-off + node-dead). */
extern uint32 cluster_grd_clear_all_boosted(void);
extern uint32 cluster_grd_clear_boosted_for_node(int32 dead_node);
/* spec-5.10 fix-forward — LMON-tick runtime-off sweep (test-and-clear pending). */
extern uint32 cluster_grd_lmon_tick_starvation_sweep(void);

/*
 * spec-5.10 D6 — read a queued REQUEST waiter's enqueue-fairness state
 * (skip_count / boosted / fair_queue_seq) by its 4-tuple identity.  Returns
 * true iff a matching waiter exists.  Out parameters may be NULL.  Read-only.
 */
extern bool cluster_grd_entry_describe_waiter(const ClusterResId *resid,
											  const ClusterGrdHolderId *id, uint32 *out_skip_count,
											  bool *out_boosted, uint64 *out_fair_queue_seq);

/*
 * Release a holder + pop the first FIFO-compatible waiter.
 *
 *	Returns the number of waiters granted (0 if none compatible).
 *	granted_out buffer must hold at least 1 entry (Step 4 ships single-
 *	pop semantics; a future amend may coalesce multiple shared-mode
 *	waiters into one release path).  The caller is responsible for
 *	sending GES_REPLY GRANT for each populated identity.
 *
 *	If the holder identity is not currently in the holders[] array,
 *	the function returns 0 with *granted_out unchanged.
 */
extern int cluster_grd_entry_release_and_pop_compatible_waiter(
	const ClusterResId *resid, const ClusterGrdHolderId *holder,
	ClusterGrdWaiterIdentity *granted_out, int max_out);


/* ============================================================
 * spec-5.1b — GES lock-conversion (convert) state machine.
 *
 *	Activates the convert-queue LOGIC consumed by spec-5.1a's frozen
 *	compatibility matrix + partial-order classification.  In spec-5.1b
 *	this state machine has NO live cross-node producer — the inbound
 *	opcode-2 convert path is an explicit FEATURE_NOT_SUPPORTED reject
 *	(D3), and the real backend convert trigger + wire/identity model are
 *	co-designed with the first real consumer in spec-5.2 (TX row-lock
 *	upgrade).  These entries provide the GRD-owned decision logic + a
 *	multi-grant drain helper, exercised end-to-end by the cluster_unit
 *	suite (U1-U11).
 *
 *	PG is an additive lock model (holding mode M then requesting M' adds a
 *	new LOCALLOCK, lock.h), so there is no native lock conversion; the
 *	holder being converted is located by (node_id, procno, current_mode) +
 *	resid — the REDECLARE convention ("at most one grant per (resid,
 *	mode)") — NOT by request_id.  convert_request_id is the convert's OWN
 *	reply key, distinct from the old grant's id.
 * ============================================================ */

/* Per-entry convert-queue cap exposed for caller buffer sizing (mirrors
 * the private cluster_grd.c PGRAC_GRD_MAX_CONVERTS). */
#define PGRAC_GRD_MAX_CONVERTS_PUBLIC 8

/*
 * D2 — convert-queue entry.  In-memory shmem only (NOT a wire ABI; the
 * convert wire encoding is forward-deferred to spec-5.2).  Populated by
 * the cluster_unit harness / internal injection in spec-5.1b.
 *
 *	Identity split (review P0 / Q4=B): the holder being converted is
 *	located by (node_id, procno, current_mode); convert_request_id is the
 *	convert's own reply key (≠ the old grant request_id).
 */
typedef struct ClusterGrdConvert {
	int32 node_id;					/* node holding the lock being converted */
	int32 source_node_id;			/* node that initiated the convert (reply routing) */
	uint32 procno;					/* PG ProcNumber of the holder */
	uint64 cluster_epoch;			/* epoch at enqueue (stale-epoch sweep) */
	LOCKMODE current_mode;			/* locator: (node,procno,current_mode)+resid */
	LOCKMODE requested_mode;		/* target mode */
	uint64 convert_request_id;		/* convert's own reply key (≠ old grant id) */
	uint64 shard_master_generation; /* spec-2.27 dedup key carry */
	uint32 request_opcode;			/* = GES_REQ_OPCODE_CONVERT */
	TransactionId waiter_xid;		/* spec-5.8 D1c — converter's xid (former pad slot) */
	TimestampTz wait_start;			/* enqueue timestamp (timeout / observability) */
	uint64 wait_seq;				/* spec-5.8 D1e — converter's D1d wait-state seq */
	/*
	 * spec-5.10 D1 — GES enqueue lock-starvation fairness state, mirroring the
	 * private ClusterGrdWaiter fields.  Master-local + shmem-only (NEVER on the
	 * wire); fair_queue_seq is the ordering key, not a wait identity (Q12 / L5;
	 * the canonical wait identity is waiter_xid + wait_seq above, spec-5.8).
	 *
	 * RESERVED for the converts-as-victim direction (forward).  spec-5.10's
	 * convert handling (D3) only protects a boosted *waiter* from a convert
	 * flood (drain Phase-0 cap); it does NOT mint/boost these convert fields,
	 * so a pending convert starved by holder-compatible requests is not yet
	 * protected (a pre-existing spec-5.1b behaviour, not a 5.10 regression).
	 * The snapshot copies these so a future direction can make a boosted
	 * convert a barrier target without an ABI change.
	 */
	uint64 fair_queue_seq; /* master-local monotonic order (D4; convert: reserved) */
	uint32 skip_count;	   /* scan-on-grant jump count (D2; convert: reserved) */
	bool boosted;		   /* head-of-line boosted (D2; convert: reserved) */
} ClusterGrdConvert;

StaticAssertDecl(sizeof(ClusterGrdConvert) == 88,
				 "ClusterGrdConvert layout pinned (spec-5.1b D2 64B; spec-5.8 D1e +8 wait_seq; "
				 "spec-5.10 +16 fair_queue_seq/skip_count/boosted)");

/*
 * D4 — convert grant decision result.
 *
 *	GRANTED_INPLACE  SAME / DOWNGRADE / compatible UPGRADE — holder mode
 *	                 mutated in place under entry->lock.
 *	ENQUEUED         conflicting UPGRADE — queued, takes priority over new
 *	                 waiters (anti-starvation).
 *	ILLEGAL          LATERAL (incomparable) or no matching holder —
 *	                 fail-closed (53R74 / SQLSTATE mapping forward 5.2).
 *	QUEUE_FULL       convert queue exhausted (+ converts_full_count).
 *	NOT_READY        GRD not initialised.
 */
typedef enum ClusterGrdConvertResult {
	CLUSTER_GRD_CONVERT_GRANTED_INPLACE = 0,
	CLUSTER_GRD_CONVERT_ENQUEUED = 1,
	CLUSTER_GRD_CONVERT_ILLEGAL = 2,
	CLUSTER_GRD_CONVERT_QUEUE_FULL = 3,
	CLUSTER_GRD_CONVERT_NOT_READY = 4
} ClusterGrdConvertResult;

/*
 * D5 — multi-grant identity returned by the drain helper.  opcode-tagged
 * (REQUEST waiter vs CONVERT) so spec-5.2 can route each GES_REPLY GRANT
 * to the correct reply path.
 */
typedef struct ClusterGrdGrantIdentity {
	ClusterGrdHolderId holder;
	int32 source_node_id;
	uint32 request_opcode;			/* GES_REQ_OPCODE_REQUEST or _CONVERT */
	uint64 shard_master_generation; /* dedup reply key carry */
	LOCKMODE mode;					/* granted mode */
} ClusterGrdGrantIdentity;

/*
 * D4 — request a convert against an existing holder (caller holds
 * entry->lock; raw mutator like grant_holder).  out_drain_hint is set
 * true on DOWNGRADE (strength relaxed → caller should drain the queue).
 */
extern ClusterGrdConvertResult cluster_grd_entry_request_convert(ClusterGrdEntry *entry,
																 const ClusterGrdConvert *req,
																 bool *out_drain_hint);

/*
 * D5 — convert-priority drain (caller holds entry->lock).  Grants every
 * pending convert that is now compatible with the surviving holders
 * (in-place, FIFO), THEN pops a single FIFO REQUEST waiter compatible
 * with both the holders and every still-pending convert target.  Returns
 * the number of identities written to granted_out (≤ max_out;  buffer
 * should hold PGRAC_GRD_MAX_CONVERTS_PUBLIC + 1).
 */
extern int cluster_grd_entry_drain_converts_then_waiters(ClusterGrdEntry *entry,
														 ClusterGrdGrantIdentity *granted_out,
														 int max_out);

/*
 * spec-5.1c D3 -- BAST-side named seam over the drain (caller holds
 * entry->lock).  LOGIC until spec-5.2 wires a live convert producer.
 */
extern int cluster_grd_entry_bast_consume(ClusterGrdEntry *entry,
										  const ClusterGrdHolderId *released_holder,
										  ClusterGrdGrantIdentity *granted_out, int max_out);

/*
 * D5 — anti-starvation predicate (caller holds entry->lock): true when a
 * new request at wanted_mode must wait because some pending convert's
 * target mode conflicts with it (granting it would starve the convert).
 */
extern bool cluster_grd_entry_request_blocked_by_pending_convert(ClusterGrdEntry *entry,
																 int /* LOCKMODE */ wanted_mode);

/* Inspection helpers (unit-test + observability; caller holds entry->lock). */
extern int cluster_grd_entry_ngranted(ClusterGrdEntry *entry);
extern int cluster_grd_entry_nconverts(ClusterGrdEntry *entry);
extern bool cluster_grd_entry_holder_mode(ClusterGrdEntry *entry, int32 node_id, uint32 procno,
										  LOCKMODE *out_mode);

/* D9 — convert observability counter accessors. */
extern uint64 cluster_grd_convert_granted_inplace_count(void);
extern uint64 cluster_grd_convert_enqueued_count(void);
extern uint64 cluster_grd_convert_illegal_count(void);
extern uint64 cluster_grd_convert_queue_full_count(void);


/* ============================================================
 * spec-5.3 — master-side convert / release-drain / rollback API.
 *
 *	Live consumers of the 5.1b convert state machine: each does the GRD
 *	entry lookup + entry->lock dance so cluster_ges.c's work-queue drain
 *	(REQUEST / CONVERT / CONVERT_ROLLBACK) and the GES RELEASE path stay
 *	symmetric and the ClusterGrdEntry body stays opaque.
 * ============================================================ */

/*
 * Master-side opcode-2 CONVERT: locate the OLD holder by (node_id, procno,
 * current_mode) + resid and run the partial-order convert decision.  On
 * GRANTED_INPLACE the slot is upgraded + request_id rebound to
 * convert_request_id (§3.1a).  On ENQUEUED conflict_holders_out[] is filled
 * (BAST targets).  ILLEGAL → fail-closed (53R74).
 */
extern ClusterGrdConvertResult
cluster_grd_convert_or_enqueue(const ClusterResId *resid, int32 node_id, uint32 procno,
							   uint64 cluster_epoch, LOCKMODE current_mode, LOCKMODE requested_mode,
							   uint64 convert_request_id, int32 source_node_id,
							   uint64 shard_master_generation,
							   ClusterGrdConflictHolder *conflict_holders_out, int *n_conflict_out);

/* spec-5.8 D1c/D1e — waiter-metadata variant.  Stamps the enqueued convert's
 * xid + wait_seq onto its master-side WFG convert-waiter vertex.  The plain
 * entry point forwards here with a zero meta. */
extern ClusterGrdConvertResult cluster_grd_convert_or_enqueue_meta(
	const ClusterResId *resid, int32 node_id, uint32 procno, uint64 cluster_epoch,
	LOCKMODE current_mode, LOCKMODE requested_mode, uint64 convert_request_id, int32 source_node_id,
	uint64 shard_master_generation, ClusterGrdWaiterMeta meta,
	ClusterGrdConflictHolder *conflict_holders_out, int *n_conflict_out);

/*
 * spec-5.3 §3.5 native-probe clear path: commit a convert located by the
 * precise REDECLARE locator (node_id, procno, current_mode).  A backend may
 * hold multiple cluster modes on one resid (e.g. SHARE + SHARE UPDATE
 * EXCLUSIVE), so (node, procno) alone is ambiguous.  The master did not
 * pre-mutate during the probe window, so this is the point the upgrade
 * actually takes effect after a CLEAR aggregate.
 */
extern ClusterGrdConvertResult
cluster_grd_convert_grant_by_backend(const ClusterResId *resid, int32 node_id, uint32 procno,
									 uint64 cluster_epoch, LOCKMODE current_mode,
									 LOCKMODE requested_mode, uint64 convert_request_id,
									 int32 source_node_id, uint64 shard_master_generation);

/*
 * GES RELEASE live path: remove the holder then drain converts + one waiter
 * (stale-epoch entries dropped first).  Returns granted identities (each
 * tagged REQUEST / CONVERT) for the caller to route via GES_REPLY GRANT.
 */
extern int cluster_grd_release_and_drain(const ClusterResId *resid,
										 const ClusterGrdHolderId *holder,
										 ClusterGrdGrantIdentity *granted_out, int max_out);

/*
 * opcode-14 CONVERT_ROLLBACK (§3.1a T4): strict inverse of the convert — locate
 * the upgraded slot by (node_id, procno, upgraded_mode) and restore both its
 * mode (→ old_mode) and request_id (→ old_request_id).  NOT a release (which
 * would delete the holder and leave a false-grant).  Raw mutator variant takes
 * the entry under the caller's lock; the wrapper does the lookup + lock.
 */
extern ClusterGrdEntryResult
cluster_grd_entry_rollback_convert(ClusterGrdEntry *entry, int32 node_id, uint32 procno,
								   LOCKMODE upgraded_mode, LOCKMODE old_mode, uint64 old_request_id,
								   uint64 convert_request_id);
extern ClusterGrdEntryResult cluster_grd_rollback_convert(const ClusterResId *resid, int32 node_id,
														  uint32 procno, LOCKMODE upgraded_mode,
														  LOCKMODE old_mode, uint64 old_request_id,
														  uint64 convert_request_id);


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
 * spec-5.13 D4 — clean-leave cooperative GES drain (a planned, proactive dual
 * of the failure path; reuses remaster + cleanup_on_node_dead).  drain_self
 * returns the number of master shards moved off the leaving node;
 * verify_no_leftover is the CL-I2 read-only proof (no leaving holder/waiter/
 * convert in any entry + no shard still mastered by the leaving node).
 */
extern uint32 cluster_grd_clean_leave_drain_self(int32 leaving_node, uint64 leave_epoch);
extern bool cluster_grd_clean_leave_verify_no_leftover(int32 leaving_node);

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
