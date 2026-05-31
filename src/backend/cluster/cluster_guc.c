/*-------------------------------------------------------------------------
 *
 * cluster_guc.c
 *	  pgrac cluster GUC registration and storage.
 *
 *	  Stage 0.13 establishes the cluster GUC framework and registers
 *	  the first cluster GUC, cluster_node_id.  See spec-0.13 and
 *	  docs/cluster-guc-design.md for the registration policy and the
 *	  full SSOT roster of planned GUCs.
 *
 *	  Why one GUC and not the full ~24 in the design doc:
 *
 *	  GUCs are user-visible knobs that promise behavior on change.
 *	  Registering a GUC that no subsystem reads turns SHOW into a
 *	  liar (DBA sets it, observes nothing) and violates CLAUDE.md
 *	  rule 8.  Each remaining GUC ships with the spec that introduces
 *	  its first reader.  This file gains a new DefineCustomXxxVariable
 *	  block per GUC over the next stages.
 *
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/backend/cluster/cluster_guc.c
 *
 * NOTES
 *	  This is a pgrac-original file (no derivation from PostgreSQL).
 *	  All exported symbols use the cluster_ prefix and live under the
 *	  "cluster_*" GUC namespace per CLAUDE.md rule 16.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "utils/guc.h"

#include "cluster/cluster_cr_cache.h" /* cluster_cr_cache_max_blocks (spec-3.10 D4) */
#include "cluster/cluster_guc.h"
#include "cluster/cluster_ic.h"				   /* ClusterICTier enum values */
#include "cluster/cluster_inject.h"			   /* cluster_injection_assign_hook (stage 0.27) */
#include "cluster/cluster_pcm_lock.h"		   /* cluster_pcm_grd_max_entries (stage 1.7) */
#include "cluster/storage/cluster_shared_fs.h" /* ClusterSharedFsBackendId (stage 1.1) */


/*
 * Storage for cluster GUC variables.  PG's GUC machinery writes here
 * on startup and (for non-PGC_POSTMASTER variables) on SIGHUP / SET.
 *
 * Boot values must match the boot value passed to DefineCustomXxxVariable
 * below, so that reads before cluster_init_guc() runs (e.g. from very
 * early postmaster code) see a sane default.
 */
int cluster_node_id = -1;
int cluster_interconnect_tier = CLUSTER_IC_TIER_STUB;
char *cluster_config_file = NULL;	   /* boot value filled by DefineCustomStringVariable */
char *cluster_injection_points = NULL; /* boot value filled by DefineCustomStringVariable */
int cluster_shared_storage_backend = CLUSTER_SHARED_FS_BACKEND_STUB;
bool cluster_smgr_user_relations = false;
int cluster_shmem_max_regions = 64;
int cluster_grd_max_entries = 0;
int cluster_ges_request_timeout_ms = 60000; /* spec-2.16 D12 + v0.5 P1.5 */

#ifdef ENABLE_INJECTION
#define CLUSTER_SHMEM_MIN_REGIONS 41
#else
#define CLUSTER_SHMEM_MIN_REGIONS 40
#endif

/* spec-2.23 D11: */
int cluster_lmd_probe_collect_timeout_ms = 3000; /* coordinator REPORT collect deadline */
int cluster_ges_reply_wait_max_entries = 1024;	 /* 5-tuple wait table cap */

/* spec-2.24 D11: */
int cluster_lmd_cleanup_sweep_interval_ms = 5000; /* LMD safety net cleanup interval */

/* spec-2.25 D9 — native-lock probe tunables (HC29 / HC32). */
int cluster_lms_native_lock_probe_max_inflight = 8;		   /* per-shard slot capacity */
int cluster_lms_native_lock_probe_retry_interval_ms = 500; /* retry poll cadence */
int cluster_lms_native_lock_probe_retry_budget = 60;	   /* ~30s @ 500ms before 53R83 */

/* spec-2.27 D4 — GES retransmit + dedup HTAB tunables (HC51 / HC52 / HC53). */
int cluster_ges_retransmit_max_attempts = 5; /* finite default; 0 = disabled */
int cluster_ges_dedup_max_entries = 8192;	 /* LMS-owned HTAB cap */

/* spec-2.17 NEW GUCs(v0.6 frozen baseline). */
int cluster_ges_bast_retry_interval_ms = 10000;	   /* D11 */
int cluster_ges_bast_max_retries = 3;			   /* D11 */
int cluster_ges_deadlock_check_interval_ms = 1000; /* D17 */
int cluster_ges_deadlock_chunk_timeout_ms = 2000;  /* D25 */
int cluster_ges_deadlock_max_edges = 1024;		   /* D24 */
int cluster_ges_deadlock_max_vertices = 256;	   /* D24 */
int cluster_ges_deadlock_max_in_flight_probes = 4; /* D24 */
int cluster_ges_deadlock_tick_budget_us = 5000;	   /* D26 */

/*
 * spec-2.16 D12 + v0.5 P1.5 helper:  effective timeout for GES grant
 *   request.  PG lock_timeout=0 means "disabled" (无限等),  must NOT
 *   degenerate to immediate timeout (I53).
 *
 *     effective = (lock_timeout==0)
 *                   ? ges_request_timeout_ms
 *                   : min(lock_timeout, ges_request_timeout_ms)
 *
 *   NOWAIT path (caller passes lock_timeout=-1) returns 0 → caller
 *   short-circuits.
 */
int
cluster_ges_effective_timeout_ms(int lock_timeout_ms)
{
	if (lock_timeout_ms < 0)
		return 0; /* NOWAIT — caller short-circuits */
	if (lock_timeout_ms == 0)
		return cluster_ges_request_timeout_ms; /* PG disabled → use ours */
	if (lock_timeout_ms < cluster_ges_request_timeout_ms)
		return lock_timeout_ms;
	return cluster_ges_request_timeout_ms;
}

/*
 * Spec-1.10 (2026-05-03) phase transition timeouts (HC4 user 修订 4).
 * Per-phase deadlines in seconds; defaults match background-process-
 * design.md §4.3.  Stage 1.10 stub handlers don't naturally trigger
 * timeouts (they return immediately); the cluster-startup-phase-N-enter
 * inject point + sleep fault simulates a stuck phase for regression
 * coverage.  Stage 1.11+ real handlers consult these GUCs.
 */
int cluster_phase1_timeout = 60;
int cluster_phase2_timeout = 30;
int cluster_phase3_timeout = 600;
int cluster_phase4_timeout = 30;


/*
 * cluster.lmon_main_loop_interval (Stage 1.11 Sprint B; spec-1.11 D8).
 * LMON main-loop tick / WaitLatch timeout in milliseconds.
 */
int cluster_lmon_main_loop_interval = 1000;

/* spec-1.12 Sprint B D8: cluster.lck_main_loop_interval (mirror). */
int cluster_lck_main_loop_interval = 1000;

/* spec-1.13 D8: cluster.diag_main_loop_interval (mirror). */
int cluster_diag_main_loop_interval = 1000;

/* spec-1.14 D8: cluster.cluster_stats_main_loop_interval (mirror). */
int cluster_cluster_stats_main_loop_interval = 1000;

/* spec-2.5 D9: CSSD main-loop tick interval (ms). */
int cluster_cssd_main_loop_interval_ms = 1000;
/* spec-2.5 D9: CSSD heartbeat broadcast interval (ms). */
int cluster_cssd_heartbeat_interval_ms = 1000;
/* spec-2.5 D9: CSSD dead deadband factor (multiplier of heartbeat interval). */
int cluster_cssd_dead_deadband_factor = 3;

/* spec-2.6 Sprint A Step 4 D12: 4 voting disk / quorum-lite GUCs. */
char *cluster_voting_disks = NULL; /* CSV path list, default empty */
int cluster_quorum_poll_interval_ms = 2000;
int cluster_voting_disk_io_timeout_ms = 5000;
int cluster_voting_disk_size_bytes = 65536;

/* spec-2.28 Sprint A Step 1 D7: 4 fence-lite GUCs (Q8 user approve). */
bool cluster_self_fence_enabled = true;	   /* default fail-safe */
int cluster_self_fence_grace_ms = 30000;   /* 30s = 7.5x lease */
bool cluster_freeze_writes_enabled = true; /* default fail-safe */
int cluster_fence_audit_log = 1;		   /* CLUSTER_FENCE_AUDIT_LOG_LOG */

/* spec-2.2 D7 -- Tier 1 TCP transport tuning (PGC_POSTMASTER per §3.3). */
int cluster_interconnect_heartbeat_interval_ms = 1000;
int cluster_interconnect_connect_timeout_ms = 5000;
int cluster_interconnect_recv_timeout_ms = 30000;

/* spec-2.4 D9 -- chunked framing + TCP KeepAlive tuning (PGC_POSTMASTER). */
int cluster_interconnect_payload_max_bytes = 64 * 1024 * 1024; /* 64 MB */
int cluster_interconnect_chunk_reassembly_timeout_ms = 10000;  /* 10s */
int cluster_interconnect_tcp_keepidle_sec = 60;
int cluster_interconnect_tcp_keepintvl_sec = 10;
int cluster_interconnect_tcp_keepcnt = 6;

/*
 * cluster.undo_segments_per_instance (spec-1.22 D7).  Number of undo
 * segment files reserved per cluster instance.  Stage 1.22 declares
 * this GUC + default value only; real consumption (segment pool sizing
 * + on-demand allocation) lands in feature-117 retention activation.
 *
 * Range [1, 1024].  Default 16 ≈ 1 GB undo capacity per instance with
 * 64 MB segments (per docs/undo-segment-design.md §3.5).
 */
int cluster_undo_segments_per_instance = 16;

/*
 * spec-3.7 D9: 3 NEW GUC (Q9=A keep existing undo_segments_per_instance default 16).
 *	cluster.undo_tablespace_path        -- default "pg_undo" (relative to PGDATA)
 *	cluster.undo_segment_size_mb        -- default 32 MB
 *	cluster.undo_record_inline_max_bytes -- default 1024
 */
char *cluster_undo_tablespace_path = NULL; /* set in DefineCustomStringVariable */
int cluster_undo_segment_size_mb = 32;
int cluster_undo_record_inline_max_bytes = 1024;

/*
 * spec-3.8 D9 NEW GUCs (Step 7 真注册;Step 3 先 declare 让 build pass):
 *   cluster.undo_segments_max_per_instance  -- default 256 = CLUSTER_UNDO_SEGS_PER_INSTANCE
 *   cluster.undo_segment_create_timeout_ms  -- default 5000ms
 */
int cluster_undo_segments_max_per_instance = 256;
int cluster_undo_segment_create_timeout_ms = 5000;

/*
 * spec-3.9 D1 NEW GUC:
 *   cluster.cr_chain_walk_max_steps -- CR block construction chain walker
 *   single-call hard cap (infinite-loop / corruption guard).  Default 4096,
 *   range [64, 65536], PGC_SIGHUP.  Exceeding it ereports DATA_CORRUPTED.
 */
int cluster_cr_chain_walk_max_steps = 4096;

/*
 * spec-3.9 D5 NEW GUC:
 *   cluster.cr_mvcc_gate -- master switch for the own-instance CR 3-tier
 *   MVCC short-circuit gate in HeapTupleSatisfiesMVCC.  DEFAULT OFF.
 *
 *   The CR construction machinery (cluster_cr_construct_block + chain walker
 *   + inverse helpers) is fully shipped and unit/TAP-tested; but the precise
 *   firing condition for routing a live SELECT through a reconstructed
 *   historical block — vs trusting PG-style multi-version heap + spec-3.3
 *   SCN visibility — is a visibility-correctness decision deferred to user
 *   codereview (see spec-3.9 Step 5 NOTE).  Default-off keeps existing
 *   spec-3.2/3.3 visibility behavior byte-for-byte; t/215 flips it on to
 *   exercise the end-to-end CR read path.  PGC_USERSET so a test session can
 *   scope it without a reload.
 */
bool cluster_cr_mvcc_gate = true;

/*
 * cluster.boc_sweep_interval_ms (spec-1.17 D4 v0.2).  walwriter BOC
 * sweep target staleness in ms.  Range [1, 1000]; default 100ms.  Actual
 * sweep frequency is bounded by Min(WalWriterDelay, this); user must
 * tune wal_writer_delay to match if sub-WalWriterDelay sweep wanted.
 * 100us range deferred to a future high-frequency-timing spec (custom
 * timer / wakeup mechanism, not walwriter loop).
 */
/* PGRAC: spec-2.10 D1 — default 1 → 100ms;must match GUC default per
 * check_GUC_init Assert (boot_val 与 C-var 初值不一致会触发 guc.c:4820 TRAP). */
int cluster_boc_sweep_interval_ms = 100;

/* PGRAC: spec-2.12 D1 — SCN cross-instance propagation lag bound.
 *
 *   Configuration bound (NOT enforcement action) — used by TAP 102 as
 *   hard threshold for real cross-node propagation latency assertion.
 *   In-process metric (scn_observe_staleness) is local proxy;  true
 *   cross-node propagation lag requires NTP and is measured externally.
 *
 *   Q2.4 spec frozen: "propagation_lag" reflects ANY SCN propagation
 *   (commit / abort advance / BOC tick / envelope piggyback),  NOT
 *   only commit (spec-2.0 §469 original wording was inaccurate).
 *
 *   C-var init must match GUC default per check_GUC_init Assert
 *   (spec-2.10 / spec-2.11 lesson inherited). */
int cluster_scn_max_propagation_lag_ms = 5000;


/*
 * cluster.enabled (Stage 1.11 Sprint B HC4 闭环; spec-1.11 D8).
 * Runtime cluster mode gate.  Sprint A relied on compile-time
 * USE_PGRAC_CLUSTER; Sprint B adds runtime control so a cluster build
 * can run as a non-cluster postgres without spawning LMON.
 */
bool cluster_enabled = true;


/*
 * cluster.allow_single_node (spec-2.1 D1; Stage 2.1 backward-compat
 * mode gate).
 *
 *	Stage 2.1 default = true permits Stage 1.X single-node fallback
 *	when pgrac.conf is absent or cluster.node_id is unset (-1).  Set
 *	to false to enforce strict multi-node validation.
 *
 *	BOUNDARY INVARIANT (spec-2.1 §3.5):  allow_single_node = on ONLY
 *	permits fallback when multi-node configuration is absent.  It does
 *	NOT downgrade malformed or explicit multi-node configuration
 *	errors -- those still FATAL regardless of allow_single_node value.
 */
bool cluster_allow_single_node = true;

/*
 * spec-2.19 D12:  cluster.lmd_enabled.
 *
 *	PGC_POSTMASTER bool, default on.  When off (set in postgresql.conf
 *	or via -c at startup), LMD process is NOT forked; spec-2.17
 *	caller-side 4-node deadlock-detection legacy path remains active
 *	(HC1 startup-time fallback;v0.2 P1.3).  Runtime SET is rejected by
 *	PG's PGC_POSTMASTER enforcement; restart required to flip ownership.
 *	HC4 exact predicate: caller-side ownership gate consults
 *	cluster_lmd_is_ready() (exact state == LMD_READY); when LMD is
 *	enabled=on but not yet READY, backend callers receive SQLSTATE
 *	53R81 cluster_lmd_unavailable (silent fallback to caller-side is
 *	forbidden — single ownership path硬契约).
 */
bool cluster_lmd_enabled = true;

/*
 * spec-2.20 D12:  cluster.lms_enabled.
 *
 *	PGC_POSTMASTER bool, default on.  Mirror cluster_lmd_enabled semantic
 *	(spec-2.19).  When off (set in postgresql.conf or via -c at startup),
 *	LMS process is NOT used for grant decisions; spec-2.17 caller-side
 *	legacy path走 PG-native LockAcquire skip cluster gate (HC1 startup-
 *	time fallback;spec-2.18 §1.4 F1 deferred 53R80 wording 一致)。
 *	Runtime SET 被 PG PGC_POSTMASTER enforcement reject;restart required
 *	to flip ownership.  HC4 exact predicate:caller-side ownership gate
 *	走 cluster_lms_is_ready()(exact == LMS_READY);enabled=on 但非 READY
 *	→ backend receives SQLSTATE 53R80 cluster_lms_unavailable。
 */
bool cluster_lms_enabled = true;

/*
 * spec-2.21 D2:cluster.lock_acquire_cluster_path emergency bypass GUC.
 * Default true; PGC_POSTMASTER context.  Set false only for P0 incident
 * response to skip the cluster gate entirely (PG-native lock only).
 */
bool cluster_lock_acquire_cluster_path = true;

/*
 * spec-2.21 D2:cluster.local_fast_path_enabled toggle GUC.
 * Default true; PGC_SIGHUP context.  Set false for fault-injection /
 * chaos testing to force remote-master path on all acquires.
 */
bool cluster_local_fast_path_enabled = true;

/*
 * spec-2.22 D9:cluster.lmd_max_wait_edges cap.  Default 1024.
 * PGC_POSTMASTER — postmaster restart required to resize HTAB.
 */
int cluster_lmd_max_wait_edges = 1024;

/*
 * spec-2.22 D9:cluster.lmd_scan_interval_ms scan loop period.
 * Default 1000ms.  PGC_SIGHUP — runtime tunable.
 */
int cluster_lmd_scan_interval_ms = 1000;

/*
 * PGRAC: spec-2.33 D8 — cluster.gcs_reply_timeout_ms (HC85).
 * Default 5000ms.  PGC_SUSET — superusers and test fixtures may tune;
 * unprivileged users may not perturb the Cache Fusion hot path.
 */
int cluster_gcs_reply_timeout_ms = 5000;

/*
 * PGRAC: spec-2.34 D8 — 3 NEW GUC for GCS block reliability hardening.
 * HC92 + HC97 — see cluster_guc.h for semantics.
 */
int cluster_gcs_block_retransmit_max_retries = 4;
int cluster_gcs_block_retransmit_initial_backoff_ms = 100;
int cluster_gcs_block_dedup_max_entries = 1024;

/*
 * PGRAC: spec-2.36 D8 — 3 NEW GUC for CF 3-way protocol (X writer
 * transfer + reader starvation guard).
 *
 *   cluster_gcs_block_invalidate_ack_timeout_ms (HC116):  master backend
 *     wait deadline for a single INVALIDATE_ACK msg_type 18 reply from a
 *     holder.  Budget exhaustion (via spec-2.34 retransmit GUC) maps to
 *     DENIED_INVALIDATE_TIMEOUT reply (status 11) → 53R91 SQLSTATE.
 *   cluster_gcs_block_starvation_backoff_ms (HC117):  reader backoff
 *     base for DENIED_PENDING_X retry loop;  backoff = base × 2^attempt.
 *   cluster_gcs_block_starvation_max_retries (HC117):  reader retry
 *     budget;  exhaustion → 53R92 SQLSTATE.
 */
int cluster_gcs_block_invalidate_ack_timeout_ms = 1500;
int cluster_gcs_block_starvation_backoff_ms = 100;
int cluster_gcs_block_starvation_max_retries = 8;

/*
 * PGRAC: spec-2.37 D11 — 1 NEW enum GUC for lost-write detection action.
 *
 *   cluster_gcs_block_lost_write_action (HC131):
 *     CLUSTER_GCS_LOST_WRITE_ACTION_ERROR (default,production)
 *       — sender ereport(53R93) terminal,统计 lost_write_detected_count++.
 *     CLUSTER_GCS_LOST_WRITE_ACTION_WARN (staging/diagnostic only)
 *       — sender 不 ereport,只 WARNING log + counter;不打断业务但 silent
 *         corruption 风险 — 仅用于 fault injection / TAP edge case 测试.
 */
typedef enum {
	CLUSTER_GCS_LOST_WRITE_ACTION_ERROR = 0,
	CLUSTER_GCS_LOST_WRITE_ACTION_WARN = 1
} ClusterGcsLostWriteAction;
int cluster_gcs_block_lost_write_action = CLUSTER_GCS_LOST_WRITE_ACTION_ERROR;

static const struct config_enum_entry cluster_gcs_block_lost_write_action_options[]
	= { { "error", CLUSTER_GCS_LOST_WRITE_ACTION_ERROR, false },
		{ "warn", CLUSTER_GCS_LOST_WRITE_ACTION_WARN, false },
		{ NULL, 0, false } };


/*
 * PGRAC: spec-2.38 D8 — 3 NEW GUC for SI Broadcaster skeleton.
 *
 *   cluster_sinval_broadcast_batch_size (HC138):  outbound queue drain
 *     batch upper bound;  1..CLUSTER_SINVAL_BATCH_MAX (=64);  check hook
 *     enforces range to prevent runtime misconfigure.
 *   cluster_sinval_broadcast_batch_timeout_ms (HC136):  SI Broadcaster
 *     main loop WaitLatch timeout (ms);  PGC_SIGHUP — aux process
 *     reload picks up new value on next loop iteration.  Direct
 *     PGC_SUSET would be misleading because session-set values don't
 *     reach the aux process global anyway.
 *   cluster_sinval_broadcast_max_queue_size (HC132/HC133):  ring buffer
 *     capacity for both outbound and inbound queues;  PGC_POSTMASTER
 *     because shmem size is computed at startup from this value.
 */
int cluster_sinval_broadcast_batch_size = 32;
int cluster_sinval_broadcast_batch_timeout_ms = 10;
int cluster_sinval_broadcast_max_queue_size = 1024;

/* spec-2.39 D12:  3 NEW GUC for ack/barrier production gate. */
int cluster_sinval_ack_mode = CLUSTER_SINVAL_ACK_MODE_PEER_ENQUEUED;
int cluster_sinval_ack_timeout_ms = 5000;
int cluster_sinval_ack_wait_slots = 256;

/* spec-3.1 D8:  2 NEW GUC for TT status overlay (D2). */
int cluster_tt_status_overlay_max_entries = 32768;
int cluster_tt_status_overlay_ttl_ms = 30000;
/* PGRAC spec-3.5 D5:  bounded reader lazy follow depth. */
int cluster_subtrans_max_chain_depth = 32;

/* PGRAC spec-3.6 D9:  MULTIXACT reader/member-resolution foundation GUCs. */
int cluster_multixact_member_overlay_max_members = 32;
int cluster_multixact_member_overlay_max_entries = 16384;
int cluster_multixact_hint_outbound_slots = 1024;

/* spec-3.2 D7:  2 NEW GUC for cross-node TT status hint wire (v0.3 删 commit_only). */
int cluster_tt_status_hint_outbound_capacity = 256;
int cluster_tt_status_hint_emit_mode = CLUSTER_TT_STATUS_HINT_EMIT_ALL_STATUS;

static const struct config_enum_entry cluster_tt_status_hint_emit_mode_options[]
	= { { "disabled", CLUSTER_TT_STATUS_HINT_EMIT_DISABLED, false },
		{ "all_status", CLUSTER_TT_STATUS_HINT_EMIT_ALL_STATUS, false },
		{ NULL, 0, false } };

#ifdef ENABLE_INJECTION
/* spec-3.2 D5b:  test-only GUC (production binary 0 触达). */
bool cluster_test_force_visibility_cluster_path = false;
#endif

static const struct config_enum_entry cluster_sinval_ack_mode_options[]
	= { { "none", CLUSTER_SINVAL_ACK_MODE_NONE, false },
		{ "peer_enqueued", CLUSTER_SINVAL_ACK_MODE_PEER_ENQUEUED, false },
		{ NULL, 0, false } };


/*
 * Mapping from the cluster.interconnect_tier GUC enum string to the
 * ClusterICTier C enum.  PG's GUC machinery copies the int into
 * cluster_interconnect_tier; cluster_ic_init then dispatches.  The
 * "hidden" flag is false because we want SHOW / pg_settings to display
 * every legal value to the DBA (even tiers that are not yet implemented
 * are shown -- attempting to use one fails at startup with a precise
 * errhint pointing to the Stage where it lands).
 */
static const struct config_enum_entry cluster_interconnect_tier_options[]
	= { { "stub", CLUSTER_IC_TIER_STUB, false }, { "mock", CLUSTER_IC_TIER_MOCK, false },
		{ "tier1", CLUSTER_IC_TIER_1, false },	 { "tier2", CLUSTER_IC_TIER_2, false },
		{ "tier3", CLUSTER_IC_TIER_3, false },	 { NULL, 0, false } };


/*
 * Mapping for cluster.shared_storage_backend.  Mirrors
 * ClusterSharedFsBackendId enum positionally.  All six backends are
 * advertised; only stub and local are registered at stage 1.1, so
 * picking one of the other four causes cluster_shared_fs_init to
 * FATAL with an errhint pointing to Stage 2.  See
 * docs/cluster-shared-fs-design.md §4.
 */
static const struct config_enum_entry cluster_shared_storage_backend_options[]
	= { { "stub", CLUSTER_SHARED_FS_BACKEND_STUB, false },
		{ "local", CLUSTER_SHARED_FS_BACKEND_LOCAL, false },
		{ "block_device", CLUSTER_SHARED_FS_BACKEND_BLOCK_DEVICE, false },
		{ "cluster_fs", CLUSTER_SHARED_FS_BACKEND_CLUSTER_FS, false },
		{ "rbd", CLUSTER_SHARED_FS_BACKEND_RBD, false },
		{ "multi_attach", CLUSTER_SHARED_FS_BACKEND_MULTI_ATTACH, false },
		{ NULL, 0, false } };


/*
 * cluster_init_guc -- register all cluster GUC variables.
 *
 *	Called once from PostmasterMain after PG's built-in GUCs are
 *	loaded.  See cluster_guc.h for contract and docs/cluster-guc-design.md
 *	§2 for the placement rationale.
 *
 *	When adding a new GUC: (1) extend cluster_guc.h with the extern
 *	declaration, (2) add the storage definition above, (3) add a new
 *	DefineCustomXxxVariable block here, and (4) update
 *	docs/cluster-guc-design.md §3.1 with the registration stage.
 *	cluster_unit and cluster_tap should grow tests for the new GUC in
 *	the same commit.
 */
/*
 * spec-2.27 D4 / HC53 — cross-GUC invariant double-direction check_hook.
 *
 *	cluster.ges_request_timeout_ms = -1 (perpetual wait) is only safe when
 *	retransmit is also enabled (otherwise reply-drop strands the waiter).
 *	Reject either direction that would break the invariant:
 *
 *	  (a) SET cluster.ges_request_timeout_ms = -1
 *	      while cluster.ges_retransmit_max_attempts == 0
 *	      → ERROR invalid_parameter_value, value unchanged.
 *
 *	  (b) SET cluster.ges_retransmit_max_attempts = 0
 *	      while current cluster.ges_request_timeout_ms == -1
 *	      → ERROR, value unchanged.
 */
static bool
cluster_ges_request_timeout_ms_check_hook(int *newval, void **extra, GucSource source)
{
	(void)extra;
	(void)source;
	if (*newval == -1 && cluster_ges_retransmit_max_attempts <= 0) {
		GUC_check_errcode(ERRCODE_INVALID_PARAMETER_VALUE);
		GUC_check_errdetail("cluster.ges_request_timeout_ms = -1 (perpetual wait) requires "
							"cluster.ges_retransmit_max_attempts > 0 so dropped replies are "
							"retransmitted.  Current cluster.ges_retransmit_max_attempts = %d.",
							cluster_ges_retransmit_max_attempts);
		return false;
	}
	return true;
}

static bool
cluster_ges_retransmit_max_attempts_check_hook(int *newval, void **extra, GucSource source)
{
	(void)extra;
	(void)source;
	if (*newval == 0 && cluster_ges_request_timeout_ms == -1) {
		GUC_check_errcode(ERRCODE_INVALID_PARAMETER_VALUE);
		GUC_check_errdetail("cluster.ges_retransmit_max_attempts = 0 is incompatible with "
							"cluster.ges_request_timeout_ms = -1 (perpetual wait).  Reset "
							"cluster.ges_request_timeout_ms to a finite value first.");
		return false;
	}
	return true;
}

void
cluster_init_guc(void)
{
	CLUSTER_INJECTION_POINT("cluster-guc-init-pre-define");

	/*
	 * GUC name uses the dot-prefixed "cluster.node_id" form per PG's
	 * convention for custom (non-built-in) GUCs (valid_custom_variable_name
	 * in guc.c requires at least one '.').  The underlying C variable
	 * stays cluster_node_id (snake_case per CLAUDE.md rule 12).
	 */
	DefineCustomIntVariable("cluster.node_id",
							gettext_noop("Numeric identifier of this node in the cluster."),
							gettext_noop("Set to -1 (the default) when running outside a cluster.  "
										 "When configured, the value is logged by CLUSTER_LOG and "
										 "will be used for cross-node coordination starting in "
										 "Stage 1+ subsystem implementations."),
							&cluster_node_id, -1, /* boot value */
							-1,					  /* min */
							127,				  /* max */
							PGC_POSTMASTER,		  /* requires restart */
							0,					  /* flags */
							NULL,				  /* check_hook */
							NULL,				  /* assign_hook */
							NULL);				  /* show_hook */

	/*
	 * cluster.interconnect_tier -- selects the cluster_ic vtable.
	 * Stage 0.18 only ships the stub vtable; tier1 / tier2 / tier3
	 * are accepted by GUC parsing but rejected at cluster_ic_init
	 * with a precise errhint.  See cluster_ic.c::cluster_ic_init and
	 * docs/cluster-ic-design.md §3.
	 */
	DefineCustomEnumVariable(
		"cluster.interconnect_tier", gettext_noop("Cluster interconnect tier vtable selection."),
		gettext_noop("stub (default) keeps cross-node IPC disabled; tier1 (TCP) "
					 "lands in Stage 2; tier2 / tier3 (RDMA) land in Stage 6+. "
					 "See docs/cluster-ic-design.md."),
		&cluster_interconnect_tier, CLUSTER_IC_TIER_STUB,  /* boot value */
		cluster_interconnect_tier_options, PGC_POSTMASTER, /* tier change requires restart */
		0,												   /* flags */
		NULL,											   /* check_hook */
		NULL,											   /* assign_hook */
		NULL);											   /* show_hook */

	/*
	 * cluster.config_file -- path to pgrac.conf.  Default "pgrac.conf"
	 * is interpreted relative to postmaster cwd (which is PGDATA after
	 * ChangeToDataDir).  Stage 2+ multi-node setups typically point
	 * this at shared storage.  See spec-0.19-conf-framework.md §2.4
	 * and docs/cluster-conf-design.md §4.
	 */
	DefineCustomStringVariable(
		"cluster.config_file",
		gettext_noop("Path to the pgrac cluster topology configuration file."),
		gettext_noop("Default \"pgrac.conf\" is resolved relative to PGDATA. "
					 "Set to an absolute path to use shared storage for "
					 "multi-node deployments."),
		&cluster_config_file, "pgrac.conf", /* boot value */
		PGC_POSTMASTER,						/* topology reload requires restart */
		0,									/* flags */
		NULL,								/* check_hook */
		NULL,								/* assign_hook */
		NULL);								/* show_hook */

	/*
	 * cluster.injection_points -- comma-separated list of injection point
	 * names to auto-arm at startup with fault_type=WARNING (counter-only +
	 * warn).  Runtime arming via the cluster_inject_fault() SRF is
	 * independent and not gated by this GUC.  PGC_SUSET so DBAs can flip
	 * it on a running server via ALTER SYSTEM + SIGHUP without a restart.
	 * See spec-0.27-error-injection.md §2.5 and docs/error-injection-design.md.
	 */
	DefineCustomStringVariable(
		"cluster.injection_points",
		gettext_noop("Comma-separated list of cluster injection points to auto-arm at startup."),
		gettext_noop("Each named point is armed with fault_type=WARNING (counter + warn). "
					 "Names not in the registry yield a WARNING and are ignored. "
					 "Default empty (no auto-arm)."),
		&cluster_injection_points, "", /* boot value */
		PGC_SUSET,					   /* superuser, runtime SET allowed */
		GUC_LIST_INPUT,				   /* comma-separated list */
		NULL,						   /* check_hook */
		cluster_injection_assign_hook, /* assign_hook */
		NULL);						   /* show_hook */

	/*
	 * cluster.shared_storage_backend -- selects the cluster_shared_fs
	 * vtable activated by cluster_shared_fs_init.  Boot default "stub"
	 * keeps stage-0 behaviour unchanged for users who upgrade without
	 * explicitly opting into the new abstraction layer.  See
	 * docs/cluster-shared-fs-design.md §4 and
	 * spec-1.1-shared-fs-skeleton.md.
	 */
	DefineCustomEnumVariable("cluster.shared_storage_backend",
							 gettext_noop("Cluster shared-storage backend selection."),
							 gettext_noop("stub (default) keeps cluster_shared_fs disabled "
										  "(every call ereports FEATURE_NOT_SUPPORTED); local "
										  "is single-node passthrough to fd.c; block_device, "
										  "cluster_fs, rbd, and multi_attach land in Stage 2."),
							 &cluster_shared_storage_backend, CLUSTER_SHARED_FS_BACKEND_STUB,
							 cluster_shared_storage_backend_options,
							 PGC_POSTMASTER, /* backend selection requires restart */
							 0,				 /* flags */
							 NULL,			 /* check_hook */
							 NULL,			 /* assign_hook */
							 NULL);			 /* show_hook */

	/*
	 * cluster.smgr_user_relations -- opt-in switch routing user-
	 * relation block I/O through cluster_smgr (smgr_which=1) instead
	 * of md.c (smgr_which=0).  Default off keeps the existing PG
	 * smgr path completely unchanged; the GUC only takes effect for
	 * permanent (non-temp) relations and only when
	 * shared_storage_backend != stub.  Startup-time cross-check
	 * lives in cluster_shared_fs_init (cluster_shared_fs.c).  See
	 * spec-1.2-smgr-cluster.md §3.2 + docs/cluster-smgr-design.md §6.
	 */
	DefineCustomBoolVariable(
		"cluster.smgr_user_relations",
		gettext_noop("Route permanent relations through cluster_smgr instead of md.c."),
		gettext_noop("When on (combined with shared_storage_backend != stub), "
					 "permanent non-temp relations route through cluster_smgr "
					 "-> cluster_shared_fs at smgropen time.  Stage 1.2 single-"
					 "node single-file passthrough; user data stored as one "
					 "file per (rlocator, fork) without the md.c .seg suffix."),
		&cluster_smgr_user_relations, false,
		PGC_POSTMASTER, /* smgr_which is cached per-relation; restart required */
		0,				/* flags */
		NULL,			/* check_hook */
		NULL,			/* assign_hook */
		NULL);			/* show_hook */

	/*
	 * cluster.shmem_max_regions (spec-1.3): capacity of the cluster shmem
	 * region registry.  Default 64 covers the stage 1.3 baseline plus the
	 * reserved regions planned in cluster-shmem-design.md §3.2 with a wide
	 * safety margin.  Range [40, 256] in production builds -- 40 is the
	 * minimum to fit the spec-3.6 baseline after adding the MultiXact
	 * overlay region on top of spec-3.5's SUBTRANS state region.  Test
	 * injection builds use 41 because the visibility-inject shmem region is
	 * compiled in.  256 is the
	 * upper engineering bound (raise via source-code change if more are
	 * needed).  PGC_POSTMASTER because the registry array is palloc'd once
	 * at postmaster init from this value.  Min was raised 8 -> 16 in
	 * spec-1.15 for cluster_scn, 16 -> 17 in spec-2.13 for cluster_ges,
	 * 28 -> 29 in spec-2.34 for cluster_gcs_block_dedup, and 29 -> 31 in
	 * spec-2.38 for ClusterSinvalOutbound + ClusterSinvalInbound, 31 -> 33
	 * in spec-2.39 for ClusterSinvalAckWait + ClusterSinvalAckOutbound,
	 * 33 -> 35 in spec-3.1 for ClusterTTStatus + ClusterTTLocalSeq,
	 * 35 -> 36 in spec-3.2 for the visibility injection region, 36 -> 39
	 * through the spec-3.4/spec-3.5 ITL/TT/SUBTRANS regions, and 39 -> 40
	 * in spec-3.6 for the MultiXact overlay, so the published lower bound
	 * remains bootable.
	 */
	DefineCustomIntVariable("cluster.shmem_max_regions",
							gettext_noop("Capacity of the pgrac cluster shmem region registry."),
							gettext_noop("Maximum number of regions that may be registered via "
										 "cluster_shmem_register_region.  Each cluster subsystem "
										 "(cluster_ctl, cluster_conf, future GRD/PCM/GES/...) "
										 "registers one region.  Raise if FATAL on startup with "
										 "errcode 53400 \"cluster shmem registry capacity "
										 "exceeded\"."),
							&cluster_shmem_max_regions, 64, CLUSTER_SHMEM_MIN_REGIONS, 256,
							PGC_POSTMASTER, /* registry array is palloc'd once at init */
							0,				/* flags */
							NULL,			/* check_hook */
							NULL,			/* assign_hook */
							NULL);			/* show_hook */

	/*
	 * spec-2.15:  cluster.grd_max_entries
	 *
	 *  Maximum number of cluster_grd entry table slots.  Default 0 — entry
	 *  HTAB not allocated; cluster_grd_entry_lookup_or_create() returns
	 *  CLUSTER_GRD_ENTRY_NOT_READY sentinel.  Non-zero enables ShmemInitHash
	 *  allocation;  v0.4 P1.1:  PG dynahash HASH_PARTITION=4096 forces
	 *  nbuckets >= 4096, so the effective init_max_size used internally is
	 *  Max(GUC, 4096) and shmem reservation comes from
	 *  hash_estimate_size(Max(GUC, 4096), sizeof(ClusterGrdEntry)).  Even
	 *  GUC=16 reserves ~3-5MB shmem.  production 推荐 NBuffers × 2 (spec-
	 *  2.16+ 真激活 caller-side LockAcquire 集成时调整).
	 *
	 *  PGC_POSTMASTER because ShmemInitHash is called once at postmaster
	 *  init from this value.
	 */
	DefineCustomIntVariable("cluster.grd_max_entries",
							gettext_noop("Maximum number of cluster_grd entry table slots."),
							gettext_noop("Default 0 means skeleton mode: entry HTAB not allocated, "
										 "cluster_grd_entry_lookup_or_create() returns NOT_READY. "
										 "Non-zero allocates the entry table.  Note PG dynahash "
										 "HASH_PARTITION=4096 forces internal nbuckets >= 4096, "
										 "so even GUC=16 reserves ~3-5MB shmem via "
										 "hash_estimate_size(Max(GUC, 4096), entry_size). "
										 "Production 推荐 NBuffers × 2 (spec-2.16+ caller-side)."),
							&cluster_grd_max_entries, 0, 0, 1048576,
							PGC_POSTMASTER, /* ShmemInitHash size fixed at init */
							0,				/* flags */
							NULL,			/* check_hook */
							NULL,			/* assign_hook */
							NULL);			/* show_hook */

	/*
	 * Stage 1.7: cluster.pcm_grd_max_entries
	 *
	 *	Maximum number of GrdEntry slots in the cluster_pcm_grd shmem
	 *	region.  Default 0 (Q4 user 修订 2026-05-02): no GRD shmem
	 *	allocated by default; cluster_pcm_grd_init() handles size=0
	 *	by early-returning before ShmemInitStruct (Q5 user 修订: PG
	 *	ShmemInitStruct(name, 0, &found) behavior is undefined).
	 *	Range [0, 1048576] (max ~128 MB at sizeof(GrdEntry) ~128 B).
	 *	PGC_POSTMASTER (startup-fixed).
	 *
	 *	Stage 2.X PCM 真值激活 spec will change default to NBuffers.
	 *
	 *	Spec: spec-1.7-pcm-state-placeholder.md §1.2 Deliverable 3 +
	 *	      §11.1 GUC checklist.
	 */
	/*
	 * spec-2.16 D12 + v0.5 P1.5 + v0.6 L1.6:
	 *   cluster.ges_request_timeout_ms
	 *
	 *   GES cross-node grant request timeout (ms).  Range [1, 600000]
	 *   (1ms - 10min).  Default 60000 (60s).  Removes -1 perpetual wait
	 *   per v0.6 L1.6 (spec-2.17 deadlock ship 后 amend range放开).
	 *   PGC_USERSET — backend-tunable.
	 *
	 *   effective_timeout = (lock_timeout == 0) ?
	 *     ges_request_timeout_ms : min(lock_timeout, ges_request_timeout_ms)
	 *   per v0.5 P1.5 (cluster_ges_effective_timeout_ms helper).
	 */
	DefineCustomIntVariable(
		"cluster.ges_request_timeout_ms",
		gettext_noop("Timeout for cross-node GES grant request (ms)."),
		gettext_noop("Range [-1, 600000] (1ms - 10min;  -1 = perpetual wait).  Default 60000.  "
					 "Backend waits this long for grant reply before rolling back via "
					 "GES_RELEASE.  PG lock_timeout=0 (disabled) does NOT short-circuit "
					 "this — backend uses ges_request_timeout_ms when lock_timeout=0.  "
					 "spec-2.27 HC53:  setting -1 (perpetual wait) requires "
					 "cluster.ges_retransmit_max_attempts > 0 so dropped replies are "
					 "retransmitted;  attempts to set -1 with retransmit=0 are rejected."),
		&cluster_ges_request_timeout_ms, 60000, -1, 600000, PGC_USERSET, GUC_UNIT_MS,
		cluster_ges_request_timeout_ms_check_hook, NULL, NULL);

	/* spec-2.27 D4 NEW — GES retransmit + dedup HTAB tunables (HC51..HC53). */
	DefineCustomIntVariable(
		"cluster.ges_retransmit_max_attempts",
		gettext_noop("Maximum GES REQUEST/RELEASE retransmit attempts before fail-closed."),
		gettext_noop("Range [0, 50].  Default 5 (≈ 3.1s of exponential backoff: "
					 "100/200/400/800/1600 ms).  0 disables retransmit entirely (spec-2.26 "
					 "behaviour).  In perpetual-wait mode (cluster.ges_request_timeout_ms = -1) "
					 "this becomes a starvation-warning threshold:  retransmit continues "
					 "indefinitely but priority_starvation_observed_count + WARNING fire at "
					 "the half / three-quarter marks.  HC53 invariant:  cannot set to 0 while "
					 "cluster.ges_request_timeout_ms = -1."),
		&cluster_ges_retransmit_max_attempts, 5, 0, 50, PGC_SIGHUP, 0,
		cluster_ges_retransmit_max_attempts_check_hook, NULL, NULL);
	DefineCustomIntVariable(
		"cluster.ges_dedup_max_entries",
		gettext_noop("LMS-owned GES retransmit dedup HTAB capacity (entries)."),
		gettext_noop("Range [256, 1048576].  Default 8192.  Receiver-side dedup HTAB lives in "
					 "shmem and survives LMS process restart (stale entries swept by "
					 "lms_restart_generation bump).  Cap reached → REJECT_BUSY fail-closed; "
					 "**never evict in-flight entries** (HC51 — eviction would re-introduce "
					 "double-grant risk).  PGC_POSTMASTER — sized at startup."),
		&cluster_ges_dedup_max_entries, 8192, 256, 1048576, PGC_POSTMASTER, 0, NULL, NULL, NULL);

	/* spec-2.23 D11 NEW:  coordinator REPORT collect deadline. */
	DefineCustomIntVariable("cluster.lmd_probe_collect_timeout_ms",
							gettext_noop("Coordinator DEADLOCK_REPORT collect deadline (ms)."),
							gettext_noop("Range [100, 30000].  Default 3000.  Coordinator LMD "
										 "broadcasts DEADLOCK_PROBE then waits up to this deadline "
										 "for N-1 REPORTs.  Partial REPORT increments "
										 "probe_partial_count (HC8) and the union edge merge is "
										 "skipped for that tick."),
							&cluster_lmd_probe_collect_timeout_ms, 3000, 100, 30000, PGC_SIGHUP,
							GUC_UNIT_MS, NULL, NULL, NULL);

	/* spec-2.23 D11 NEW:  reply wait HTAB cap (5-tuple key). */
	DefineCustomIntVariable(
		"cluster.ges_reply_wait_max_entries",
		gettext_noop("Cap on the cross-node GES reply wait HTAB (5-tuple key)."),
		gettext_noop("Range [64, 65536].  Default 1024.  Backends inserting a "
					 "GES_REQUEST/RELEASE wait entry beyond the cap fail closed "
					 "with SQLSTATE 53R71 — request is rolled back rather than "
					 "blocking indefinitely.  PGC_POSTMASTER — restart required."),
		&cluster_ges_reply_wait_max_entries, 1024, 64, 65536, PGC_POSTMASTER, 0, NULL, NULL, NULL);

	/* spec-2.24 D11 NEW:  LMD safety net cleanup sweep interval (HC28). */
	DefineCustomIntVariable(
		"cluster.lmd_cleanup_sweep_interval_ms",
		gettext_noop("LMD periodic dead-backend cleanup sweep interval (ms)."),
		gettext_noop("Range [100, 60000].  Default 5000ms.  LMD daemon walks GRD "
					 "entries every interval looking for local backends whose procno "
					 "is no longer alive in ProcArray (SIGKILL safety net per HC28). "
					 "Remote-node death is handled separately by cssd dead-bitmap.  "
					 "0 disables sweep (unrecommended outside benchmarking).  TAP "
					 "may set 500ms for fast verify."),
		&cluster_lmd_cleanup_sweep_interval_ms, 5000, 100, 60000, PGC_SIGHUP, GUC_UNIT_MS, NULL,
		NULL, NULL);

	/* spec-2.25 D9 NEW:  native-lock probe tunables (HC29 / HC32 / 53R83). */
	DefineCustomIntVariable(
		"cluster.lms_native_lock_probe_max_inflight",
		gettext_noop("Per-shard LMS native-lock probe collector slot capacity."),
		gettext_noop("Range [1, 64].  Default 8.  Each LMS shard maintains this many "
					 "concurrent probe slots — each slot tracks a single in-flight "
					 "fan-out (LOCKTAG, lockmode) probe + N-1 expected replies + "
					 "aggregated status.  Slot exhaustion enqueues new probes to "
					 "the LMS pending queue (wait event ClusterLmsNativeProbeWait) "
					 "until capacity frees.  PGC_POSTMASTER — shmem region sized "
					 "at startup."),
		&cluster_lms_native_lock_probe_max_inflight, 8, 1, 64, PGC_POSTMASTER, 0, NULL, NULL, NULL);
	DefineCustomIntVariable(
		"cluster.lms_native_lock_probe_retry_interval_ms",
		gettext_noop("LMS native-lock probe retry-poll cadence when peers return "
					 "HOLDER_CONFLICT / WAITER_CONFLICT / timeout."),
		gettext_noop("Range [50, 60000].  Default 500ms.  LMS re-fans-out the same "
					 "probe (probe_id epoch advanced) until aggregate reaches CLEAR "
					 "or retry_budget is exhausted (then SQLSTATE 53R83 fail-closed). "
					 "Shorter intervals shorten DDL wait but raise interconnect load."),
		&cluster_lms_native_lock_probe_retry_interval_ms, 500, 50, 60000, PGC_SIGHUP, GUC_UNIT_MS,
		NULL, NULL, NULL);
	DefineCustomIntVariable(
		"cluster.lms_native_lock_probe_retry_budget",
		gettext_noop("Cumulative retry budget per requester before native-lock "
					 "probe fails closed with 53R83."),
		gettext_noop("Range [1, 3600].  Default 60 (≈30s with the 500ms cadence "
					 "default).  budget exceeded → SQLSTATE 53R83 "
					 "ERRCODE_CLUSTER_NATIVE_LOCK_PROBE_TIMEOUT returned to caller; "
					 "transaction must retry / abort.  spec-2.27 fairness escalation "
					 "(priority-boost-after-K) will reduce default after wire."),
		&cluster_lms_native_lock_probe_retry_budget, 60, 1, 3600, PGC_SIGHUP, 0, NULL, NULL, NULL);

	/* spec-2.17 D11:  BAST retry GUC(Q11 v0.6 — 不 kill healthy holder). */
	DefineCustomIntVariable("cluster.ges_bast_retry_interval_ms",
							gettext_noop("BAST retry interval (ms) when holder is non-responsive."),
							gettext_noop("Range [1000, 60000].  Default 10000(10s).  Master 重发 "
										 "周期(不是 kill 阈值)— Q11 v0.6 不 kill healthy holder。"),
							&cluster_ges_bast_retry_interval_ms, 10000, 1000, 60000, PGC_SIGHUP,
							GUC_UNIT_MS, NULL, NULL, NULL);
	DefineCustomIntVariable(
		"cluster.ges_bast_max_retries",
		gettext_noop("Maximum BAST retry attempts before REJECT to new requester."),
		gettext_noop("Range [1, 10].  Default 3.  超此次数 master enqueue REJECT。"),
		&cluster_ges_bast_max_retries, 3, 1, 10, PGC_SIGHUP, 0, NULL, NULL, NULL);

	/* spec-2.17 D17 + D25 + D24 + D26:  deadlock detector GUCs. */
	DefineCustomIntVariable(
		"cluster.ges_deadlock_check_interval_ms",
		gettext_noop("Deadlock probe periodic interval (ms)."),
		gettext_noop("Range [100, 10000].  Default 1000.  LMON tick body 周期扫描。"),
		&cluster_ges_deadlock_check_interval_ms, 1000, 100, 10000, PGC_SIGHUP, GUC_UNIT_MS, NULL,
		NULL, NULL);
	DefineCustomIntVariable(
		"cluster.ges_deadlock_chunk_timeout_ms",
		gettext_noop("Deadlock probe chunked reassembly timeout (ms)."),
		gettext_noop("Range [500, 30000].  Default 2000.  超时 drop entire probe。"),
		&cluster_ges_deadlock_chunk_timeout_ms, 2000, 500, 30000, PGC_SIGHUP, GUC_UNIT_MS, NULL,
		NULL, NULL);
	DefineCustomIntVariable(
		"cluster.ges_deadlock_max_edges", gettext_noop("Deadlock graph max edges per probe."),
		gettext_noop("Range [64, 65536].  Default 1024.  Hard cap protects LMON。"),
		&cluster_ges_deadlock_max_edges, 1024, 64, 65536, PGC_SIGHUP, 0, NULL, NULL, NULL);
	DefineCustomIntVariable(
		"cluster.ges_deadlock_max_vertices", gettext_noop("Deadlock graph max vertices per probe."),
		gettext_noop("Range [16, 16384].  Default 256."), &cluster_ges_deadlock_max_vertices, 256,
		16, 16384, PGC_SIGHUP, 0, NULL, NULL, NULL);
	DefineCustomIntVariable(
		"cluster.ges_deadlock_max_in_flight_probes",
		gettext_noop("Max concurrent in-flight deadlock probes per coordinator."),
		gettext_noop("Range [1, 32].  Default 4.  Back-pressure防 probe storm。"),
		&cluster_ges_deadlock_max_in_flight_probes, 4, 1, 32, PGC_SIGHUP, 0, NULL, NULL, NULL);
	DefineCustomIntVariable("cluster.ges_deadlock_tick_budget_us",
							gettext_noop("Max time(us)budget for deadlock work per LMON tick."),
							gettext_noop("Range [500, 50000].  Default 5000(5ms).  超 budget → "
										 "drop newest probe + degrade mode让其他子系统跑。"),
							&cluster_ges_deadlock_tick_budget_us, 5000, 500, 50000, PGC_SIGHUP, 0,
							NULL, NULL, NULL);

	DefineCustomIntVariable(
		"cluster.pcm_grd_max_entries",
		gettext_noop("Maximum entries in the PCM GRD master shmem region."),
		gettext_noop("spec-2.30 PCM 9-state machine activation:  -1 (default) = "
					 "auto-resolve to NBuffers at startup;  0 = explicit disable "
					 "(preserves spec-1.7 stub:  mutation API ereports 0A000 "
					 "FEATURE_NOT_SUPPORTED, query returns N);  positive value "
					 "must cover NBuffers (HC62 fail-closed FATAL on shortfall)."),
		&cluster_pcm_grd_max_entries, -1, -1, 1048576, PGC_POSTMASTER, /* startup-fixed */
		0,															   /* flags */
		NULL,														   /* check_hook */
		NULL,														   /* assign_hook */
		NULL);														   /* show_hook */

	/* ----------
	 * Stage 1.10 (2026-05-03) — postmaster startup phase transition
	 * timeouts (HC4 user 修订 4).  Per background-process-design.md
	 * §4.3.  Stage 1.10 stub handlers don't naturally trigger
	 * timeouts; cluster-startup-phase-N-enter inject point + sleep
	 * fault simulates a stuck phase for regression coverage.  Real
	 * timeout enforcement activates in 1.11+ when phase handlers
	 * have actual work that can hang.
	 *
	 *	Spec: spec-1.10-postmaster-startup-phase-skeleton.md §2.2 GUC table.
	 * ----------
	 */
	DefineCustomIntVariable("cluster.phase1_timeout",
							gettext_noop("Phase 1 (cluster basics) transition timeout in seconds."),
							gettext_noop("Maximum wall-clock time for Phase 1 handler "
										 "(interconnect listener / heartbeat / LMON join).  "
										 "Exceeding this triggers ereport(FATAL, errcode "
										 "PGRAC_E_PHASE_TRANSITION_TIMEOUT) so postmaster "
										 "startup fails cleanly.  Default matches background-"
										 "process-design.md §4.3."),
							&cluster_phase1_timeout, 60, 1, 3600, PGC_POSTMASTER, GUC_UNIT_S, NULL,
							NULL, NULL);

	DefineCustomIntVariable("cluster.phase2_timeout",
							gettext_noop("Phase 2 (lock services) transition timeout in seconds."),
							gettext_noop("Maximum wall-clock time for Phase 2 handler "
										 "(LMS / LMD / LCK spawn).  Exceeding this triggers "
										 "ereport(FATAL, PGRAC_E_PHASE_TRANSITION_TIMEOUT)."),
							&cluster_phase2_timeout, 30, 1, 3600, PGC_POSTMASTER, GUC_UNIT_S, NULL,
							NULL, NULL);

	DefineCustomIntVariable(
		"cluster.phase3_timeout", gettext_noop("Phase 3 (recovery) transition timeout in seconds."),
		gettext_noop("Maximum wall-clock time for Phase 3 handler "
					 "(crash recovery / Recovery Coordinator / merged "
					 "recovery).  Exceeding this triggers ereport(FATAL, "
					 "PGRAC_E_PHASE_TRANSITION_TIMEOUT)."),
		&cluster_phase3_timeout, 600, 60, 3600, PGC_POSTMASTER, GUC_UNIT_S, NULL, NULL, NULL);

	DefineCustomIntVariable("cluster.phase4_timeout",
							gettext_noop("Phase 4 (normal startup) transition timeout in seconds."),
							gettext_noop("Maximum wall-clock time for Phase 4 handler "
										 "(walwriter / bgwriter / DIAG / Cluster Stats "
										 "spawn).  Exceeding this triggers ereport(FATAL, "
										 "PGRAC_E_PHASE_TRANSITION_TIMEOUT)."),
							&cluster_phase4_timeout, 30, 1, 3600, PGC_POSTMASTER, GUC_UNIT_S, NULL,
							NULL, NULL);

	/* ----------
	 * Stage 1.11 Sprint B (2026-05-04) — LMON GUCs (spec-1.11 D8).
	 *
	 *	Spec: spec-1.11-lmon-skeleton.md §2.2 GUC table
	 *	      (cluster.lmon_main_loop_interval) +
	 *	      4 实质 HC #2 (cluster.enabled HC4 闭环).
	 * ----------
	 */
	DefineCustomIntVariable(
		"cluster.lmon_main_loop_interval",
		gettext_noop("LMON main-loop tick interval in milliseconds."),
		gettext_noop("How often the LMON aux process wakes from its main loop to "
					 "advance last_liveness_tick_at + main_loop_iters and check "
					 "for shutdown / SIGHUP.  Sprint A used a hardcoded 1000ms "
					 "baseline; Sprint B exposes this as PGC_SIGHUP so operators "
					 "can dial telemetry granularity at runtime.  Lower value -> "
					 "finer last_liveness_tick_at resolution + faster shutdown "
					 "response; higher value -> lower wakeup overhead.  Sprint A "
					 "LMON has no real consumer work, so any value in range is "
					 "functionally equivalent."),
		&cluster_lmon_main_loop_interval, 1000, 100, 60000, PGC_SIGHUP, GUC_UNIT_MS, NULL, NULL,
		NULL);

	DefineCustomIntVariable(
		"cluster.lck_main_loop_interval",
		gettext_noop("LCK main-loop tick interval in milliseconds."),
		gettext_noop("Same semantics as cluster.lmon_main_loop_interval; controls "
					 "the LCK aux process main-loop WaitLatch timeout (spec-1.12 "
					 "Sprint B D8).  Sprint A LCK has no real consumer work, so "
					 "any value in range is functionally equivalent."),
		&cluster_lck_main_loop_interval, 1000, 100, 60000, PGC_SIGHUP, GUC_UNIT_MS, NULL, NULL,
		NULL);

	DefineCustomIntVariable(
		"cluster.diag_main_loop_interval",
		gettext_noop("DIAG main-loop tick interval in milliseconds."),
		gettext_noop("Same semantics as cluster.lmon_main_loop_interval / "
					 "cluster.lck_main_loop_interval; controls the DIAG aux "
					 "process main-loop WaitLatch timeout (spec-1.13 D8). "
					 "DIAG 1.13 has no real consumer work yet (cross-node "
					 "diagnostic / hang dump / etc. land in Stage 2+), so any "
					 "value in range is functionally equivalent at this stage."),
		&cluster_diag_main_loop_interval, 1000, 100, 60000, PGC_SIGHUP, GUC_UNIT_MS, NULL, NULL,
		NULL);

	/*
	 * spec-2.2 D7 (2026-05-07) -- Tier 1 TCP transport tuning GUCs.
	 * All PGC_POSTMASTER per spec-2.2 §3.3 (runtime change would
	 * leave in-flight connect / recv state in inconsistent timeout
	 * windows).
	 */
	DefineCustomIntVariable(
		"cluster.interconnect_heartbeat_interval_ms",
		gettext_noop("Tier1 IC heartbeat tick interval in milliseconds."),
		gettext_noop("LMON sends a HEARTBEAT msg to every CONNECTED peer at "
					 "this cadence (spec-2.2 §2.1).  Lower value -> earlier "
					 "transport-down detection at cost of higher idle wakeup; "
					 "higher value -> lower CPU overhead but slower peer state "
					 "transition.  Per spec-2.2 §3.6 boundary invariant, "
					 "missed heartbeats only mark peer_state DOWN (transport-"
					 "level liveness); they do NOT trigger fence / quorum / "
					 "membership change (those land in spec-2.5+ / 2.6+ / 2.28+)."),
		&cluster_interconnect_heartbeat_interval_ms, 1000, 100, 60000, PGC_POSTMASTER, GUC_UNIT_MS,
		NULL, NULL, NULL);

	DefineCustomIntVariable("cluster.interconnect_connect_timeout_ms",
							gettext_noop("Tier1 IC active-connect SO_ERROR poll timeout in ms."),
							gettext_noop("Per-peer nonblocking connect(2) waits up to this many "
										 "ms for SO_ERROR to settle before scheduling a reconnect "
										 "(spec-2.2 §2.1 + §3.10 connection-level rejection).  "
										 "Per spec-2.2 §3.3 PGC_POSTMASTER -- runtime change "
										 "would leave half-finished connects in inconsistent "
										 "timeout windows."),
							&cluster_interconnect_connect_timeout_ms, 5000, 1000, 60000,
							PGC_POSTMASTER, GUC_UNIT_MS, NULL, NULL, NULL);

	DefineCustomIntVariable("cluster.interconnect_recv_timeout_ms",
							gettext_noop("Tier1 IC per-peer recv read deadline in milliseconds."),
							gettext_noop("Per-peer recv(2) read deadline (HELLO handshake + "
										 "subsequent message reads).  Exceeding this without "
										 "data triggers peer_state -> DOWN per spec-2.2 §3.10 "
										 "(connection-level rejection; never FATAL).  Per spec-2.2 "
										 "§3.3 PGC_POSTMASTER."),
							&cluster_interconnect_recv_timeout_ms, 30000, 1000, 600000,
							PGC_POSTMASTER, GUC_UNIT_MS, NULL, NULL, NULL);

	/* spec-2.4 D9 -- chunked framing + TCP KeepAlive 5 GUC. */
	DefineCustomIntVariable("cluster.interconnect_payload_max_bytes",
							gettext_noop("Maximum cluster_ic_send_envelope_chunked payload bytes."),
							gettext_noop("Hard upper bound on cluster_ic_send_envelope_chunked "
										 "len argument.  Caller passing larger ereport(ERROR) "
										 "ERRCODE_PROGRAM_LIMIT_EXCEEDED at entry.  Range "
										 "16 MB ~ 256 MB;default 64 MB conservative.  per "
										 "spec-2.4 Q3 (GUC enforce, not silent truncate)."),
							&cluster_interconnect_payload_max_bytes, 64 * 1024 * 1024,
							16 * 1024 * 1024, 256 * 1024 * 1024, PGC_POSTMASTER, GUC_UNIT_BYTE,
							NULL, NULL, NULL);

	DefineCustomIntVariable(
		"cluster.interconnect_chunk_reassembly_timeout_ms",
		gettext_noop("Chunked reassembly partial-frame timeout in milliseconds."),
		gettext_noop("LMON main tick scans per-peer reassembly state;peer "
					 "with started_at older than this threshold gets close+ "
					 "LOG `53R21`+counter bump.  Scan period == LMON main "
					 "loop interval.  per spec-2.4 §3.4."),
		&cluster_interconnect_chunk_reassembly_timeout_ms, 10000, 1000, 60000, PGC_POSTMASTER,
		GUC_UNIT_MS, NULL, NULL, NULL);

	DefineCustomIntVariable("cluster.interconnect_tcp_keepidle_sec",
							gettext_noop("Tier1 TCP_KEEPIDLE socket option in seconds."),
							gettext_noop("setsockopt(TCP_KEEPIDLE) per-peer applied at "
										 "accept_one / finish_connect.  Linux uses TCP_KEEPIDLE; "
										 "macOS uses TCP_KEEPALIVE alias.  Default 60s.  "
										 "Layered with spec-2.2 v1.0.1 F2 application-level "
										 "3x heartbeat liveness scan -- TCP keepalive is "
										 "kernel-level fallback for app-dead-but-socket-live."),
							&cluster_interconnect_tcp_keepidle_sec, 60, 30, 600, PGC_POSTMASTER,
							GUC_UNIT_S, NULL, NULL, NULL);

	DefineCustomIntVariable("cluster.interconnect_tcp_keepintvl_sec",
							gettext_noop("Tier1 TCP_KEEPINTVL socket option in seconds."),
							gettext_noop("setsockopt(TCP_KEEPINTVL) per-peer applied at "
										 "accept_one / finish_connect.  Default 10s."),
							&cluster_interconnect_tcp_keepintvl_sec, 10, 10, 60, PGC_POSTMASTER,
							GUC_UNIT_S, NULL, NULL, NULL);

	DefineCustomIntVariable("cluster.interconnect_tcp_keepcnt",
							gettext_noop("Tier1 TCP_KEEPCNT socket option (probe count)."),
							gettext_noop("setsockopt(TCP_KEEPCNT) per-peer applied at "
										 "accept_one / finish_connect.  Default 6 -- combined "
										 "with keepidle=60s + keepintvl=10s yields 60 + 6*10 "
										 "= 120s worst-case kernel-level half-open detection."),
							&cluster_interconnect_tcp_keepcnt, 6, 3, 20, PGC_POSTMASTER, 0, NULL,
							NULL, NULL);

	DefineCustomIntVariable("cluster.cluster_stats_main_loop_interval",
							gettext_noop("Cluster Stats main-loop tick interval in milliseconds."),
							gettext_noop("Same semantics as cluster.diag_main_loop_interval; "
										 "controls the Cluster Stats aux process main-loop "
										 "WaitLatch timeout (spec-1.14 D8).  Cluster Stats 1.14 "
										 "has no real consumer work yet (pg_stat_cluster_* view "
										 "filling / cross-node aggregation / history retention "
										 "land in Stage 2+), so any value in range is "
										 "functionally equivalent at this stage."),
							&cluster_cluster_stats_main_loop_interval, 1000, 100, 60000, PGC_SIGHUP,
							GUC_UNIT_MS, NULL, NULL, NULL);

	/* spec-2.5 D9: 3 NEW CSSD GUCs (PGC_POSTMASTER per spec §2.3 — applied
	 * at postmaster init;hot-reload via SIGHUP not supported because
	 * heartbeat interval / deadband factor are baked into peer state
	 * machine + first-tick grace period at CssdMain READY publish). */
	DefineCustomIntVariable(
		"cluster.cssd_main_loop_interval_ms",
		gettext_noop("CSSD aux process main-loop tick interval in milliseconds."),
		gettext_noop("Mirror of cluster.diag_main_loop_interval / "
					 "cluster_stats_main_loop_interval;CSSD MainLoop "
					 "WaitLatch timeout (spec-2.5 D9).  Independent of "
					 "heartbeat broadcast interval -- the loop tick drives "
					 "deadband-scan + outbound queue read + read result "
					 "from previous LMON drain;the actual heartbeat send "
					 "interval is governed by cssd_heartbeat_interval_ms."),
		&cluster_cssd_main_loop_interval_ms, 1000, 100, 60000, PGC_POSTMASTER, GUC_UNIT_MS, NULL,
		NULL, NULL);

	DefineCustomIntVariable(
		"cluster.cssd_heartbeat_interval_ms",
		gettext_noop("CSSD heartbeat broadcast period in milliseconds."),
		gettext_noop("Per-tick CSSD broadcast period (spec-2.5 D9).  Default "
					 "1000ms = 1Hz heartbeat per peer.  Range [100, 10000] ms.  "
					 "DEAD threshold is computed as factor × this interval "
					 "(see cssd_dead_deadband_factor).  Tuning narrower (100-"
					 "500ms) gives faster failover detection at the cost of "
					 "extra cross-node traffic;wider (3-10s) reduces traffic "
					 "but delays DEAD detection."),
		&cluster_cssd_heartbeat_interval_ms, 1000, 100, 10000, PGC_POSTMASTER, GUC_UNIT_MS, NULL,
		NULL, NULL);

	DefineCustomIntVariable(
		"cluster.cssd_dead_deadband_factor",
		gettext_noop("CSSD dead-detection deadband as a multiple of heartbeat interval."),
		gettext_noop("DEAD threshold = factor × cssd_heartbeat_interval_ms.  "
					 "SUSPECTED threshold = max(2, factor-1) × interval (spec-"
					 "2.5 Q5 ★ B 3-stage hysteresis).  Default 3 → SUSPECTED "
					 "at 2s, DEAD at 3s (matches Oracle CSS / Pacemaker / etcd "
					 "industry baseline).  Range [2, 10];admin can widen for "
					 "long PG GC pause tolerance, narrow for tighter SLA."),
		&cluster_cssd_dead_deadband_factor, 3, 2, 10, PGC_POSTMASTER, 0, NULL, NULL, NULL);

	/*
	 * spec-2.6 Sprint A Step 4 D12: 4 voting disk / quorum-lite GUCs.
	 *
	 * cluster.voting_disks (string CSV) — default empty.  Per Q7 v0.2,
	 * empty + multi-node + cluster.allow_single_node=off = postmaster
	 * startup FATAL (avoid silent fail-open).  Set via postgresql.conf
	 * to a comma-separated list of voting disk file paths
	 * (e.g. "/voting/disk1,/voting/disk2,/voting/disk3").
	 */
	DefineCustomStringVariable(
		"cluster.voting_disks", gettext_noop("Comma-separated list of voting disk file paths."),
		gettext_noop("Quorum-lite voting disk paths (spec-2.6 Q1/Q7 v0.2).  "
					 "Empty (default) = qvotec disabled.  Multi-node + empty "
					 "+ cluster.allow_single_node=off triggers postmaster "
					 "startup FATAL.  Recommended: 3 disks across distinct "
					 "failure domains; 1 / 5 / 7 also valid.  Each disk file "
					 "size = cluster.voting_disk_size_bytes."),
		&cluster_voting_disks, NULL, PGC_POSTMASTER, 0, NULL, NULL, NULL);

	/*
	 * cluster.quorum_poll_interval_ms (spec-2.6 D12).  qvotec MainLoop
	 * tick interval.  Default 2000ms = 0.5Hz; range [500, 30000] ms.
	 * Lease (Q4 v0.2) = poll_ts + 2 × this — backend in_quorum check
	 * fails if now ≥ lease (defends qvotec hung silent stale-OK).
	 */
	DefineCustomIntVariable("cluster.quorum_poll_interval_ms",
							gettext_noop("Quorum voting disk poll period in milliseconds."),
							gettext_noop("qvotec MainLoop tick (spec-2.6 D12).  Default 2000ms.  "
										 "Lease window = 2 × this — backend in_quorum check "
										 "fails after lease expiry to defend against qvotec "
										 "hung (Q4 v0.2 lease-based).  Range [500, 30000] ms."),
							&cluster_quorum_poll_interval_ms, 2000, 500, 30000, PGC_POSTMASTER,
							GUC_UNIT_MS, NULL, NULL, NULL);

	/*
	 * cluster.voting_disk_io_timeout_ms (spec-2.6 D12).  Per-I/O
	 * read/write/fsync deadline.  Default 5000ms; range [500, 60000].
	 */
	DefineCustomIntVariable("cluster.voting_disk_io_timeout_ms",
							gettext_noop("Voting disk single I/O timeout in milliseconds."),
							gettext_noop("Per slot R/W/fsync deadline (spec-2.6 D12).  EIO "
										 "after this → disks_ok_count-- counter increments.  "
										 "Default 5000ms.  Range [500, 60000] ms."),
							&cluster_voting_disk_io_timeout_ms, 5000, 500, 60000, PGC_POSTMASTER,
							GUC_UNIT_MS, NULL, NULL, NULL);

	/*
	 * cluster.voting_disk_size_bytes (spec-2.6 D12).  Voting disk file
	 * size — pre-allocated by qvotec on first boot.  Each instance owns
	 * one 512-byte slot at offset (node_id × sizeof(ClusterVotingSlot)).
	 * Default 65536 bytes = 128 instances × 512.  Range [4096, 1048576].
	 */
	DefineCustomIntVariable("cluster.voting_disk_size_bytes",
							gettext_noop("Voting disk file size in bytes."),
							gettext_noop("Pre-allocated voting disk size (spec-2.6 D12).  Each "
										 "instance owns one 512-byte slot at offset (node_id "
										 "× 512).  Default 65536 = 128 instance slots.  Range "
										 "[4096, 1048576] bytes; must be a multiple of 512."),
							&cluster_voting_disk_size_bytes, 65536, 4096, 1048576, PGC_POSTMASTER,
							GUC_UNIT_BYTE, NULL, NULL, NULL);

	/* spec-2.28 Sprint A Step 1 D7: 4 fence-lite GUCs (Q8 user approve). */

	/*
	 * cluster.self_fence_enabled (spec-2.28 D7).  Default fail-safe:
	 * postmaster auto-shutdown when ClusterFenceShmem.self_fence_
	 * requested_at_us has been set for >= cluster.self_fence_grace_ms.
	 * Off → ops handles shutdown manually (pg_ctl stop) on persistent
	 * quorum loss.  Per Invariant I1 / §3.6.1, dev/test default escape
	 * is `cluster.allow_single_node = on` which keeps quorum_state at
	 * INITIALIZING so the request is never made — this GUC is not the
	 * dev kill switch.
	 */
	DefineCustomBoolVariable(
		"cluster.self_fence_enabled",
		gettext_noop("Enable postmaster self-shutdown on persistent quorum loss."),
		gettext_noop("When on (default), postmaster initiates fast shutdown "
					 "(SIGINT-driven) cluster.self_fence_grace_ms milliseconds "
					 "after a quorum loss broadcast.  When off, in-flight "
					 "transactions are still aborted via PROCSIG_CLUSTER_"
					 "FREEZE_WRITES (controlled by cluster.freeze_writes_"
					 "enabled), but postmaster stays up — operator must "
					 "stop manually.  Dev/test escape is cluster.allow_"
					 "single_node = on (qvotec stays in INITIALIZING; this "
					 "GUC is irrelevant in that mode)."),
		&cluster_self_fence_enabled, true, PGC_POSTMASTER, 0, NULL, NULL, NULL);

	/*
	 * cluster.self_fence_grace_ms (spec-2.28 D7).  Delay between
	 * quorum loss broadcast and postmaster pmdie.  Per Invariant I1
	 * this only delays SELF-SHUTDOWN — the in-flight tx freeze
	 * broadcast is immediate.  Default 30000ms = 7.5x Q4 v0.2 lease
	 * window (4s) to absorb 4-10s transient flap; range allows
	 * 1-300 seconds.
	 */
	DefineCustomIntVariable(
		"cluster.self_fence_grace_ms",
		gettext_noop("Delay before postmaster self-shutdown on persistent quorum loss (ms)."),
		gettext_noop("Time between LMON's quorum-loss broadcast and the "
					 "postmaster's SIGINT-driven fast shutdown.  ONLY delays "
					 "self-shutdown; the in-flight transaction abort path "
					 "(PROCSIG_CLUSTER_FREEZE_WRITES) is immediate per "
					 "Invariant I1.  Default 30000 ms = 7.5× Q4 v0.2 lease "
					 "window — absorbs transient quorum flaps before self-"
					 "fence triggers.  Range [1000, 300000] ms."),
		&cluster_self_fence_grace_ms, 30000, 1000, 300000, PGC_POSTMASTER, GUC_UNIT_MS, NULL, NULL,
		NULL);

	/*
	 * cluster.freeze_writes_enabled (spec-2.28 D7).  Master switch for
	 * the in-flight tx freeze path.  Off → cluster_fence_check_inter
	 * rupts returns silently and the freeze flag is harmlessly absorbed;
	 * commit gate (spec-2.6 v0.14.1) still fail-closes via lease.  Off
	 * is for dev/debug only — production should keep on.
	 */
	DefineCustomBoolVariable(
		"cluster.freeze_writes_enabled",
		gettext_noop("Enable PROCSIG_CLUSTER_FREEZE_WRITES in-flight transaction abort."),
		gettext_noop("When on (default), backends receiving the freeze signal "
					 "ereport(ERROR) on next CHECK_FOR_INTERRUPTS, rolling back "
					 "in-flight transactions.  When off, the signal is absorbed "
					 "silently and only the commit-boundary fail-closed gate "
					 "(spec-2.6) prevents writes — useful for diagnosing "
					 "fence-induced abort behaviour without losing in-flight "
					 "work.  Per Invariant I2, this does NOT bypass the commit "
					 "gate either way."),
		&cluster_freeze_writes_enabled, true, PGC_POSTMASTER, 0, NULL, NULL, NULL);

	/*
	 * cluster.fence_audit_log (spec-2.28 D7).  Verbosity of fence-
	 * related events in the postmaster log.
	 */
	{
		static const struct config_enum_entry cluster_fence_audit_log_options[] = {
			{ "off", 0, false },
			{ "log", 1, false },
			{ "debug", 2, false },
			{ NULL, 0, false },
		};

		DefineCustomEnumVariable(
			"cluster.fence_audit_log", gettext_noop("Verbosity of fence-related log events."),
			gettext_noop("'off' suppresses all fence log lines (silent operation; "
						 "rely on counters and views).  'log' (default) emits one "
						 "LOG line per broadcast (freeze / thaw / self-fence "
						 "initiated).  'debug' adds DEBUG2 entries for per-backend "
						 "freeze signal receipt — verbose, dev/test only."),
			&cluster_fence_audit_log, 1, /* default = log */
			cluster_fence_audit_log_options, PGC_POSTMASTER, 0, NULL, NULL, NULL);
	}

	/*
	 * cluster.boc_sweep_interval_ms (spec-1.17 D4 v0.2).
	 * walwriter periodic BOC sweep staleness target.  Default 1ms;
	 * range [1, 1000] ms.  walwriter wake rate (WalWriterDelay default
	 * 200ms) caps actual sweep frequency.
	 */
	/*
	 * cluster.undo_segments_per_instance (spec-1.22 D7).  Reserved
	 * undo segment count per instance.  Stage 1.22 ships the GUC +
	 * default 16; real consumption deferred to feature-117.
	 */
	DefineCustomIntVariable("cluster.undo_segments_per_instance",
							gettext_noop("Reserved undo segment count per cluster instance."),
							gettext_noop("Default 16 segments × 64 MB = 1 GB undo capacity "
										 "per instance.  Stage 1.22 declares this GUC and "
										 "default value only; segment pool sizing + "
										 "on-demand allocation activates in feature-117 "
										 "(undo retention).  See "
										 "docs/undo-segment-design.md §3.5."),
							&cluster_undo_segments_per_instance, 16, 1, 1024, PGC_POSTMASTER, 0,
							NULL, NULL, NULL);

	/*
	 * spec-3.7 D9 — 3 NEW undo GUCs.
	 */
	DefineCustomStringVariable(
		"cluster.undo_tablespace_path",
		gettext_noop("Relative path under PGDATA for the per-instance undo tablespace."),
		gettext_noop("Spec-3.7 D9. Default \"pg_undo\". Per-instance subdir "
					 "layout pg_undo/instance_<N>/seg_<id>.dat. PGC_POSTMASTER "
					 "because runtime change would race segment lookups; "
					 "hot-add tablespace path 推 spec-3.8 lifecycle."),
		&cluster_undo_tablespace_path, "pg_undo", PGC_POSTMASTER, 0, NULL, NULL, NULL);

	DefineCustomIntVariable(
		"cluster.undo_segment_size_mb", gettext_noop("Per-segment file size in MB."),
		gettext_noop("Spec-3.7 D9. Default 32 MB. Affects per-instance "
					 "undo capacity (= undo_segments_per_instance × this). "
					 "PGC_POSTMASTER — segment header at block 0 encodes "
					 "segment_size_blocks at create time."),
		&cluster_undo_segment_size_mb, 32, 8, 1024, PGC_POSTMASTER, 0, NULL, NULL, NULL);

	DefineCustomIntVariable("cluster.undo_record_inline_max_bytes",
							gettext_noop("Maximum inline payload size for a single undo record."),
							gettext_noop("Spec-3.7 D9. Default 1024. Records with payload "
										 "exceeding this cap are rejected with SQLSTATE 53R9D "
										 "(cluster_undo_record_invalid_uba). Large-record / "
										 "toast-overflow path 推 spec-3.X."),
							&cluster_undo_record_inline_max_bytes, 1024, 16, 8192, PGC_SIGHUP, 0,
							NULL, NULL, NULL);

	/*
	 * spec-3.8 D9 — 2 NEW undo lifecycle GUCs.
	 */
	DefineCustomIntVariable(
		"cluster.undo_segments_max_per_instance",
		gettext_noop("Hard cap of per-instance undo segment pool size."),
		gettext_noop("Spec-3.8 D9. Default 256 = CLUSTER_UNDO_SEGS_PER_INSTANCE "
					 "encoding limit (F2 codex review). Range 16..256; "
					 "超过 256 需要 future ABI/segment-id encoding spec. "
					 "SIGHUP reload allowed but configured value lower than current "
					 "pool size results in WARNING + effective floor at current pool "
					 "(no retro-shrink). Hard cap reached → SQLSTATE 53R9E "
					 "(cluster_undo_segments_hard_cap_reached)."),
		&cluster_undo_segments_max_per_instance, 256, 16, 256, PGC_SIGHUP, 0, NULL, NULL, NULL);

	DefineCustomIntVariable(
		"cluster.undo_segment_create_timeout_ms",
		gettext_noop("Segment file create + initial fsync elapsed-time guard."),
		gettext_noop("Spec-3.8 D9. Default 5000ms. Elapsed-time check after file "
					 "create + fsync completes; exceeding this threshold raises "
					 "SQLSTATE 53R9D with errhint to check storage latency. "
					 "Not an asynchronous I/O cancellation/preemption mechanism."),
		&cluster_undo_segment_create_timeout_ms, 5000, 100, 60000, PGC_SIGHUP, 0, NULL, NULL, NULL);

	DefineCustomIntVariable(
		"cluster.cr_chain_walk_max_steps",
		gettext_noop("Hard cap on undo chain walk steps per CR block construction."),
		gettext_noop("Spec-3.9 D1. Default 4096, range 64..65536, PGC_SIGHUP. "
					 "Bounds the own-instance CR construction chain walker so a "
					 "corrupt / cyclic undo chain cannot loop forever; exceeding it "
					 "raises SQLSTATE XX001 (data_corrupted) with errhint "
					 "\"chain walk infinite loop suspected\". Tune higher if a hot "
					 "row legitimately accumulates a very long in-snapshot undo chain."),
		&cluster_cr_chain_walk_max_steps, 4096, 64, 65536, PGC_SIGHUP, 0, NULL, NULL, NULL);

	DefineCustomBoolVariable(
		"cluster.cr_mvcc_gate",
		gettext_noop("Enable the own-instance CR 3-tier MVCC short-circuit gate."),
		gettext_noop("Spec-3.9 D5. DEFAULT ON. When on, a cluster-source snapshot reading "
					 "a local-origin tuple whose block (pd_block_scn) and ITL write_scn are "
					 "both newer than the snapshot read_scn is resolved through a "
					 "reconstructed read_scn-consistent block image. Only meaningful in a "
					 "multi-node cluster (cluster_conf_has_peers); single-node snapshots are "
					 "LOCAL-source and never reach this path. Set off to fall back to the "
					 "spec-3.2/3.3 SCN/PG-native visibility path."),
		&cluster_cr_mvcc_gate, true, PGC_USERSET, 0, NULL, NULL, NULL);

	DefineCustomIntVariable(
		"cluster.cr_cache_max_blocks",
		gettext_noop("Backend-local CR block cache capacity in 8 KB blocks (0 disables)."),
		gettext_noop("Spec-3.10 D4. Default 64, range 0..4096, PGC_USERSET. Caches full-block "
					 "CR images keyed by (RelFileLocator, fork, block, read_scn, page_lsn) so a "
					 "block re-read under the same snapshot/version is served without re-walking "
					 "the undo chains. 0 disables caching (every CR is reconstructed; useful for "
					 "perf A/B and as a fallback). Each block costs 8 KB of backend-local memory."),
		&cluster_cr_cache_max_blocks, 64, 0, 4096, PGC_USERSET, 0, NULL, NULL, NULL);

	DefineCustomIntVariable(
		"cluster.boc_sweep_interval_ms",
		gettext_noop("walwriter BOC sweep staleness target in milliseconds."),
		gettext_noop("walwriter cluster_scn_boc_tick() runs at most every "
					 "cluster.boc_sweep_interval_ms.  Used for last_advance_at "
					 "timestamp refresh, wraparound watermark check, and Stage 2+ "
					 "cross-node broadcast pulse.  Actual frequency is bounded by "
					 "Max(WalWriterDelay, this); set wal_writer_delay below this "
					 "value if you want sub-200ms BOC.  100us-class precision "
					 "needs a future high-frequency-timing spec."),
		/* PGRAC: spec-2.10 D1 — default 1 → 100ms.  spec-2.9 skeleton phase
		 * used eager 1ms cadence.  100ms降的是 walwriter wake / shmem
		 * atomic / boc_sweep_count growth churn 100x;IC fanout cadence
		 * 不动(LMON tick 1000ms 是 bottleneck per spec-2.10 §0 Q5 / §3.1).
		 * Range 1..1000 保持. */
		&cluster_boc_sweep_interval_ms, 100, 1, 1000, PGC_SIGHUP, GUC_UNIT_MS, NULL, NULL, NULL);

	/* PGRAC: spec-2.12 D1 — SCN cross-instance propagation lag bound.
	 * Configuration only (no enforcement action — TAP 102 uses this as
	 * hard threshold;  in-process metric is local staleness proxy). */
	DefineCustomIntVariable(
		"cluster.scn_max_propagation_lag_ms",
		gettext_noop("SCN cross-instance propagation lag bound in milliseconds."),
		gettext_noop("Configuration bound used by TAP convergence verification "
					 "tests and future Hardening alarms.  In-process metric is "
					 "scn_observe_staleness (local proxy via "
					 "pg_cluster_state.scn.scn_seconds_since_last_observe);  "
					 "true cross-node propagation lag requires NTP and is "
					 "measured externally by TAP 102.  Range covers 0.1s "
					 "tight assertions to 60s WAN-tolerant deployments."),
		&cluster_scn_max_propagation_lag_ms, 5000, 100, 60000, PGC_SIGHUP, GUC_UNIT_MS, NULL, NULL,
		NULL);

	DefineCustomBoolVariable(
		"cluster.enabled",
		gettext_noop("Runtime cluster mode gate (Stage 1.11 Sprint B HC4 闭环)."),
		gettext_noop("When on (default for --enable-cluster builds), the postmaster "
					 "phase machinery spawns LMON + future cluster background "
					 "processes (LCK / DIAG / Cluster Stats / Heartbeat).  When "
					 "off, the phase 1 driver degrades to spec-1.10 stub behavior "
					 "(no LMON spawn) and a non-cluster single-instance postgres "
					 "is the result.  Useful for running PG regression tests / "
					 "pgbench on a cluster-built binary without the cluster "
					 "control plane."),
		&cluster_enabled, true, PGC_POSTMASTER, 0, NULL, NULL, NULL);

	/*
	 * cluster.allow_single_node (spec-2.1 D1; Stage 2.1 backward-compat
	 * mode gate).  Boot value matches storage initialiser (true) so reads
	 * before this registration runs see the safe default.  Boundary
	 * invariant (spec-2.1 §3.5): allow_single_node = on permits fallback
	 * ONLY when multi-node configuration is absent; malformed conf still
	 * FATAL.
	 */
	DefineCustomBoolVariable(
		"cluster.allow_single_node",
		gettext_noop("Allow pgrac to start in single-node mode "
					 "(no pgrac.conf or invalid cluster.node_id)."),
		gettext_noop("When on (Stage 2.1 default for backward compatibility), "
					 "pgrac.conf missing or cluster.node_id invalid emits WARNING "
					 "and falls back to single-node operation.  When off (Stage 2 "
					 "strict mode), such conditions emit FATAL during postmaster "
					 "startup.  This flag does NOT downgrade malformed or explicit "
					 "multi-node configuration errors -- those still FATAL "
					 "regardless of this value (spec-2.1 §3.5 boundary invariant)."),
		&cluster_allow_single_node, true, PGC_POSTMASTER, 0, NULL, NULL, NULL);

	/*
	 * spec-2.19 D12 — cluster.lmd_enabled.
	 *
	 *	PGC_POSTMASTER bool, default true.  See cluster_lmd_enabled global
	 *	declaration above for HC1 / HC4 semantics.  Runtime SET rejected by
	 *	PGC_POSTMASTER enforcement.
	 */
	DefineCustomBoolVariable(
		"cluster.lmd_enabled",
		gettext_noop("Enable the LMD (Lock Manager Daemon — deadlock detection actor) "
					 "cluster background process."),
		gettext_noop("When on (default), postmaster forks LMD at PM_RUN and "
					 "spec-2.17 caller-side 4-node deadlock-detection legacy path "
					 "is hard-disabled once LMD reaches READY.  When off, LMD is "
					 "not forked and the caller-side legacy path remains active.  "
					 "PGC_POSTMASTER:restart required to flip ownership (HC1 "
					 "fail-closed startup-time fallback;spec-2.19 v0.2 P1.3)."),
		&cluster_lmd_enabled, true, PGC_POSTMASTER, 0, NULL, NULL, NULL);

	/*
	 * spec-2.20 D12 — cluster.lms_enabled.
	 *
	 *	PGC_POSTMASTER bool, default true.  Mirror cluster_lmd_enabled.
	 *	See cluster_lms_enabled declaration above.
	 */
	DefineCustomBoolVariable(
		"cluster.lms_enabled",
		gettext_noop("Enable the LMS (Lock Master Server) cluster grant decision daemon."),
		gettext_noop("When on (default), spec-2.17 caller-side 7-step state "
					 "machine routes cluster-aware lock acquires through LMS "
					 "(spec-2.18 daemon + spec-2.20 grant decision body).  "
					 "When off, the spec-2.17 caller-side legacy path走 "
					 "PG-native LockAcquire skip cluster gate.  PGC_POSTMASTER:"
					 "restart required to flip ownership (HC1 fail-closed "
					 "startup-time fallback;spec-2.18 §1.4 F1 deferred wording)。"),
		&cluster_lms_enabled, true, PGC_POSTMASTER, 0, NULL, NULL, NULL);

	/* spec-2.21 D2:emergency bypass GUC */
	DefineCustomBoolVariable("cluster.lock_acquire_cluster_path",
							 gettext_noop("Enable the cluster lock acquire gate path."),
							 gettext_noop("When true (default), PG LockAcquireExtended routes "
										  "cluster-aware locks (LOCKTAG_ADVISORY xact-level) "
										  "through the 7-step state machine.  When false, all "
										  "locks skip the cluster gate and use PG-native path "
										  "only — emergency bypass for P0 incidents.  "
										  "PGC_POSTMASTER:restart required."),
							 &cluster_lock_acquire_cluster_path, true, PGC_POSTMASTER, 0, NULL,
							 NULL, NULL);

	/* spec-2.21 D2:local-fast-path toggle GUC */
	DefineCustomBoolVariable("cluster.local_fast_path_enabled",
							 gettext_noop("Enable the S3 local-fast-path 5-check (local master + "
										  "no remote holder/waiter/convert + generation stable)."),
							 gettext_noop("When true (default), cluster lock acquires on resources "
										  "mastered locally with no remote contention bypass the "
										  "LMS work_queue.  When false, all acquires take the "
										  "remote-master path — perf degradation ~10x vs "
										  "spec-1.23 baseline; for fault-injection / chaos "
										  "testing.  PGC_SIGHUP."),
							 &cluster_local_fast_path_enabled, true, PGC_SIGHUP, 0, NULL, NULL,
							 NULL);

	/* spec-2.22 D9:LMD wait-edge cap GUC. */
	DefineCustomIntVariable(
		"cluster.lmd_max_wait_edges", gettext_noop("Maximum LMD wait-for graph edges."),
		gettext_noop("Cap for spec-2.22 LMD wait-for graph.  Overflow is "
					 "fail-closed (HC12): submit returns false; caller "
					 "ereports ERRCODE_CLUSTER_LMD_WAIT_EDGE_FULL (53R82).  "
					 "Severely disallowed to fall back to PG local "
					 "deadlock_timeout because cluster wait edges are "
					 "invisible to PG-native detector."),
		&cluster_lmd_max_wait_edges, 1024, 64, 65536, PGC_POSTMASTER, 0, NULL, NULL, NULL);

	/* spec-2.22 D9:LMD scan loop period GUC. */
	DefineCustomIntVariable(
		"cluster.lmd_scan_interval_ms", gettext_noop("LMD Tarjan scan loop period (ms)."),
		gettext_noop("LmdMain Tarjan scan period.  Lower = faster deadlock "
					 "detection at higher CPU.  CV wake on edge submission "
					 "also triggers scan out-of-band.  PGC_SIGHUP."),
		&cluster_lmd_scan_interval_ms, 1000, 50, 60000, PGC_SIGHUP, 0, NULL, NULL, NULL);

	/*
	 * PGRAC: spec-2.33 D8 — cluster.gcs_reply_timeout_ms (HC85).
	 * Range [100, 60000];  defined via DefineCustomIntVariable's built-in
	 * min/max validator (no separate check_hook needed for plain range).
	 * PGC_SUSET so unprivileged users cannot perturb the Cache Fusion hot
	 * path; superusers + test fixtures may tune for fault injection.
	 */
	DefineCustomIntVariable(
		"cluster.gcs_reply_timeout_ms", gettext_noop("GCS block-ship request reply timeout (ms)."),
		gettext_noop("Sender ConditionVariableTimedSleep deadline for GCS "
					 "block-ship reply.  On expiry, request cleanup + "
					 "ereport(ERRCODE_QUERY_CANCELED) with errhint "
					 "pointing to spec-2.34 retransmit.  HC85.  PGC_SUSET."),
		&cluster_gcs_reply_timeout_ms, 5000, 100, 60000, PGC_SUSET, 0, NULL, NULL, NULL);

	/*
	 * PGRAC: spec-2.34 D8 — 3 NEW GUC for GCS block reliability hardening.
	 * HC92 dedup cap + HC97 retransmit math.
	 */
	DefineCustomIntVariable(
		"cluster.gcs_block_retransmit_max_retries",
		gettext_noop("Maximum retry attempts after initial GCS block-ship reply timeout."),
		gettext_noop("After the initial GCS_BLOCK_REQUEST send fails to receive "
					 "a reply within cluster.gcs_reply_timeout_ms, the sender "
					 "may retry up to this many times using exponential backoff "
					 "(see cluster.gcs_block_retransmit_initial_backoff_ms).  "
					 "N=0 disables retransmit.  Budget exhausted raises "
					 "SQLSTATE 53R90.  HC97.  PGC_SUSET."),
		&cluster_gcs_block_retransmit_max_retries, 4, 0, 8, PGC_SUSET, 0, NULL, NULL, NULL);

	DefineCustomIntVariable(
		"cluster.gcs_block_retransmit_initial_backoff_ms",
		gettext_noop("Initial backoff before retry 1 (subsequent retries double)."),
		gettext_noop("Exponential backoff base for GCS block-ship retransmit:  "
					 "retry 1 waits this much, retry 2 doubles, etc.  Default "
					 "100 → 100/200/400/800 ms for N=4 retries (total 1500 ms).  "
					 "HC97.  PGC_SUSET."),
		&cluster_gcs_block_retransmit_initial_backoff_ms, 100, 10, 5000, PGC_SUSET, 0, NULL, NULL,
		NULL);

	DefineCustomIntVariable("cluster.gcs_block_dedup_max_entries",
							gettext_noop("Master-side GCS block dedup HTAB capacity (entries)."),
							gettext_noop("Each entry occupies sizeof(GcsBlockDedupEntry) = 8312B.  "
										 "Default 1024 → ~8.4MB shmem on each node serving as "
										 "GCS block-ship master; bootstrap/initdb with no "
										 "configured cluster.node_id does not allocate the HTAB.  "
										 "HASH_ENTER_NULL on cap → "
										 "DENIED_DEDUP_FULL fail-closed (sender retries via "
										 "HC96 transient).  HC92.  PGC_POSTMASTER."),
							&cluster_gcs_block_dedup_max_entries, 1024, 256, 16384, PGC_POSTMASTER,
							0, NULL, NULL, NULL);

	/*
	 * PGRAC: spec-2.36 D8 — 3 NEW GUC for CF 3-way (X transfer +
	 * reader starvation).
	 */
	DefineCustomIntVariable(
		"cluster.gcs_block_invalidate_ack_timeout_ms",
		gettext_noop("CF 3-way master deadline for a single INVALIDATE_ACK."),
		gettext_noop("Master backend waits up to this many milliseconds for an "
					 "INVALIDATE_ACK (msg_type 18) reply from a single S/X holder "
					 "during 3-way broadcast.  Combined with cluster.gcs_block_"
					 "retransmit_max_retries this bounds the worst-case 3-way "
					 "transfer latency before sender sees DENIED_INVALIDATE_TIMEOUT "
					 "→ 53R91.  HC116.  PGC_SUSET."),
		&cluster_gcs_block_invalidate_ack_timeout_ms, 1500, 100, 60000, PGC_SUSET, 0, NULL, NULL,
		NULL);
	DefineCustomIntVariable(
		"cluster.gcs_block_starvation_backoff_ms",
		gettext_noop("S barrier reader backoff base for DENIED_PENDING_X retry."),
		gettext_noop("Reader exponential backoff base in milliseconds for the "
					 "HC117 S barrier retry loop.  Actual backoff = base × "
					 "2^attempt.  HC117.  PGC_SUSET."),
		&cluster_gcs_block_starvation_backoff_ms, 100, 1, 60000, PGC_SUSET, 0, NULL, NULL, NULL);
	DefineCustomIntVariable(
		"cluster.gcs_block_starvation_max_retries", gettext_noop("S barrier reader retry budget."),
		gettext_noop("Reader DENIED_PENDING_X retry budget.  Budget exhausted → "
					 "ereport(53R92);  upper-layer transaction may retry the "
					 "whole statement.  HC117.  PGC_SUSET."),
		&cluster_gcs_block_starvation_max_retries, 8, 0, 64, PGC_SUSET, 0, NULL, NULL, NULL);

	/*
	 * PGRAC: spec-2.37 D11 — 1 NEW enum GUC for lost-write detection action.
	 */
	DefineCustomEnumVariable(
		"cluster.gcs_block_lost_write_action",
		gettext_noop("Action when GCS block ship triggers lost-write detection."),
		gettext_noop("error (default, production): sender ereport(53R93) terminal denial.  "
					 "warn (staging / diagnostic only): WARNING log + counter, business "
					 "not interrupted but silent corruption risk.  HC131.  PGC_SUSET."),
		&cluster_gcs_block_lost_write_action, CLUSTER_GCS_LOST_WRITE_ACTION_ERROR,
		cluster_gcs_block_lost_write_action_options, PGC_SUSET, 0, NULL, NULL, NULL);

	/*
	 * PGRAC: spec-2.38 D8 — 3 NEW GUC for SI Broadcaster skeleton.
	 */
	DefineCustomIntVariable(
		"cluster.sinval_broadcast_batch_size",
		gettext_noop("Outbound sinval batch drain upper bound."),
		gettext_noop("SI Broadcaster aux process drains up to this many entries "
					 "from the outbound queue per main loop iteration.  Range "
					 "1..64 (CLUSTER_SINVAL_BATCH_MAX).  HC138 wire ABI bound.  "
					 "PGC_POSTMASTER."),
		&cluster_sinval_broadcast_batch_size, 32, 1, 64, PGC_POSTMASTER, 0, NULL, NULL, NULL);
	DefineCustomIntVariable("cluster.sinval_broadcast_batch_timeout_ms",
							gettext_noop("SI Broadcaster main loop WaitLatch timeout (ms)."),
							gettext_noop("SI Broadcaster aux process wakes every N ms even if no "
										 "latch is set, to flush partial batches and process "
										 "overflow reset requests.  PGC_SIGHUP — aux process "
										 "picks up new value on next loop iteration after reload.  "
										 "HC136."),
							&cluster_sinval_broadcast_batch_timeout_ms, 10, 1, 60000, PGC_SIGHUP, 0,
							NULL, NULL, NULL);
	DefineCustomIntVariable("cluster.sinval_broadcast_max_queue_size",
							gettext_noop("Outbound + inbound queue ring buffer capacity."),
							gettext_noop("Slot count of both ClusterSinvalOutbound and "
										 "ClusterSinvalInbound ring buffers (shared cap).  "
										 "Outbound full → cluster_sinval_enqueue_batch returns "
										 "false (HC134 fail-closed → 53R94).  Inbound full → "
										 "fail-safe SIResetAll() by aux process (HC134).  "
										 "PGC_POSTMASTER."),
							&cluster_sinval_broadcast_max_queue_size, 1024, 64, 65536,
							PGC_POSTMASTER, 0, NULL, NULL, NULL);

	/*
	 * spec-2.39 D12:  cluster.sinval_ack_mode (enum) — DDL commit ack barrier
	 * mode.  none = fire-and-forget (spec-2.38 行为);peer_enqueued = wait
	 * until all declared+CSSD-ALIVE peers ACK received the batch into their
	 * inbound queue (or走 RESET_PENDING fail-safe).  PGC_SIGHUP reload.
	 */
	DefineCustomEnumVariable("cluster.sinval_ack_mode",
							 gettext_noop("Sinval propagation ack/barrier mode."),
							 gettext_noop("none = fire-and-forget;peer_enqueued = wait IC ACK "
										  "from each declared+CSSD-ALIVE peer (default).  Caller "
										  "blocks WaitLatch until cluster.sinval_ack_timeout_ms."),
							 &cluster_sinval_ack_mode, CLUSTER_SINVAL_ACK_MODE_PEER_ENQUEUED,
							 cluster_sinval_ack_mode_options, PGC_SIGHUP, 0, NULL, NULL, NULL);

	/*
	 * spec-2.39 D12:  cluster.sinval_ack_timeout_ms (int) — ack wait timeout.
	 * On timeout WARN with SQLSTATE 53R95 + bump ack_timeout_count + DDL
	 * continues (already committed locally + WAL flushed, no rollback).
	 */
	DefineCustomIntVariable(
		"cluster.sinval_ack_timeout_ms", gettext_noop("Sinval ack wait timeout in milliseconds."),
		gettext_noop("Maximum time cluster_sinval_enqueue_and_wait_ack will "
					 "block waiting for peer ACKs.  Timeout → WARN 53R95 + "
					 "ack_timeout_count++ + DDL continues.  PGC_SIGHUP."),
		&cluster_sinval_ack_timeout_ms, 5000, 100, 60000, PGC_SIGHUP, 0, NULL, NULL, NULL);

	/*
	 * spec-2.39 D12:  cluster.sinval_ack_wait_slots (int) — ack_wait HTAB
	 * capacity (concurrent in-flight DDL ack waits).  PGC_POSTMASTER because
	 * shmem is allocated once at startup from this value.
	 */
	DefineCustomIntVariable(
		"cluster.sinval_ack_wait_slots", gettext_noop("Capacity of ClusterSinvalAckWait HTAB."),
		gettext_noop("Maximum concurrent in-flight DDL ack waits per node.  "
					 "Full → cluster_sinval_enqueue_and_wait_ack returns "
					 "ENQUEUE_FAILED + bump outbound_queue_full_count + LMON "
					 "broadcasts SINVAL_RESET_ALL_BROADCAST fail-safe.  "
					 "PGC_POSTMASTER."),
		&cluster_sinval_ack_wait_slots, 256, 64, 4096, PGC_POSTMASTER, 0, NULL, NULL, NULL);

	/*
	 * spec-3.1 D8:  Undo TT status overlay GUCs.
	 *
	 * cluster.tt_status_overlay_max_entries — bounded HTAB capacity for
	 * cross-node transaction status overlay; HTAB shmem sized at postmaster
	 * startup; PGC_POSTMASTER.
	 *
	 * cluster.tt_status_overlay_ttl_ms — soft TTL in ms; lookup_exact ages
	 * out entries past this age and returns UNKNOWN (HC181 fail-closed).
	 */
	DefineCustomIntVariable(
		"cluster.tt_status_overlay_max_entries",
		gettext_noop("Capacity of cluster Undo TT status overlay HTAB (spec-3.1 D2)."),
		gettext_noop(
			"Bounded in-memory cache of {origin_node, undo_segment, tt_slot, epoch, xid} "
			"exact-key transaction status entries.  Miss returns CLUSTER_TT_STATUS_UNKNOWN "
			"with authoritative=false (HC181 fail-closed); MUST NOT silent fallback to "
			"PG CLOG (L176).  PGC_POSTMASTER."),
		&cluster_tt_status_overlay_max_entries, 32768, 1024, 1048576, PGC_POSTMASTER, 0, NULL, NULL,
		NULL);

	DefineCustomIntVariable(
		"cluster.tt_status_overlay_ttl_ms",
		gettext_noop("TTL in milliseconds for cluster Undo TT status overlay entries."),
		gettext_noop("Soft TTL applied on lookup_exact; entries older than this age are aged out "
					 "and lookup returns UNKNOWN.  Default 30000 (30s) covers typical OLTP active-"
					 "xact window.  PGC_SIGHUP."),
		&cluster_tt_status_overlay_ttl_ms, 30000, 1000, 600000, PGC_SIGHUP, 0, NULL, NULL, NULL);

	/*
	 * PGRAC spec-3.5 D5:  cluster.subtrans_max_chain_depth — bounded depth
	 * for reader lazy follow of SUBCOMMITTED parent_key chain.  Hard
	 * lookup_parent recursion stops at this depth and returns UNKNOWN ->
	 * 53R97 fail-closed (HC205;L199;NOT PG-native fallback).  Default 32
	 * covers ORM-stacked savepoint workloads; production deep-nesting
	 * deployments may raise.  PGC_SIGHUP.
	 */
	DefineCustomIntVariable(
		"cluster.subtrans_max_chain_depth",
		gettext_noop("Bounded depth for cluster SUBTRANS reader lazy parent_key follow."),
		gettext_noop("spec-3.5 D5:  reader recursion on SUBCOMMITTED parent_key chain stops "
					 "at this depth and raises 53R97 fail-closed.  Default 32 covers ORM-"
					 "stacked savepoint workloads."),
		&cluster_subtrans_max_chain_depth, 32, 4, 1024, PGC_SIGHUP, 0, NULL, NULL, NULL);

	/*
	 * PGRAC spec-3.6 D9:  cluster.multixact_member_overlay_max_members
	 *   Per-message hard cap on V4 sidecar wire member_count.  Sender
	 *   overflow / receiver overflow → fail-closed + counter +1.
	 *   PGC_SIGHUP.
	 */
	DefineCustomIntVariable(
		"cluster.multixact_member_overlay_max_members",
		gettext_noop("Per-message hard cap on V4 sidecar wire member_count."),
		gettext_noop("spec-3.6 D9:  V4 sidecar payload member_count <= this cap.  "
					 "Overflow (sender or receiver) → fail-closed + counter +1."),
		&cluster_multixact_member_overlay_max_members, 32, 4, 256, PGC_SIGHUP, 0, NULL, NULL, NULL);

	/*
	 * PGRAC spec-3.6 D9:  cluster.multixact_member_overlay_max_entries
	 *   Overlay HTAB capacity (number of distinct cluster multixact keys).
	 *   PGC_POSTMASTER.
	 */
	DefineCustomIntVariable(
		"cluster.multixact_member_overlay_max_entries",
		gettext_noop("Capacity of cluster MultiXact member overlay HTAB (spec-3.6 D2)."),
		gettext_noop("Bounded in-memory cache of remote-composed MultiXact id -> members list.  "
					 "Miss returns 53R9C fail-closed (NOT silent fallback to PG SLRU).  "
					 "PGC_POSTMASTER."),
		&cluster_multixact_member_overlay_max_entries, 16384, 1024, 1048576, PGC_POSTMASTER, 0,
		NULL, NULL, NULL);

	/*
	 * PGRAC spec-3.6 D9:  cluster.multixact_hint_outbound_slots
	 *   V4 sidecar outbound queue slot count.  default 1024 slots ×
	 *   6168B per slot ≈ 6.0 MiB shmem.  PGC_POSTMASTER.
	 */
	DefineCustomIntVariable("cluster.multixact_hint_outbound_slots",
							gettext_noop("V4 sidecar outbound queue slot count (spec-3.6 D4)."),
							gettext_noop("Each slot reserves 6168B (header 24 + 256 × 24).  "
										 "Default 1024 ≈ 6.0 MiB shmem.  PGC_POSTMASTER."),
							&cluster_multixact_hint_outbound_slots, 1024, 128, 8192, PGC_POSTMASTER,
							0, NULL, NULL, NULL);

	/*
	 * spec-3.2 D7:  cross-node TT status hint wire GUCs.
	 *
	 * cluster.tt_status_hint_outbound_capacity — outbound ring slot count;
	 * sized at postmaster startup; PGC_POSTMASTER.
	 *
	 * cluster.tt_status_hint_emit_mode — enum {disabled, all_status};
	 * v0.3 删 commit_only(避免 ABORTED status 永久不传播);PGC_SIGHUP.
	 */
	DefineCustomIntVariable(
		"cluster.tt_status_hint_outbound_capacity",
		gettext_noop("Capacity of cluster TT status hint outbound ring (spec-3.2 D3)."),
		gettext_noop("LMON-mediated fanout queue;fire-and-forget no ack;full → drop + "
					 "WARNING (commit hot path not blocked).  PGC_POSTMASTER."),
		&cluster_tt_status_hint_outbound_capacity, 256, 64, 4096, PGC_POSTMASTER, 0, NULL, NULL,
		NULL);

	DefineCustomEnumVariable(
		"cluster.tt_status_hint_emit_mode",
		gettext_noop("Emit mode for cross-node TT status hint propagation (spec-3.2 D7)."),
		gettext_noop("disabled:0 emit;all_status:emit on commit + abort (default;"
					 "v0.3 removed commit_only to avoid ABORTED never propagating)."),
		&cluster_tt_status_hint_emit_mode, CLUSTER_TT_STATUS_HINT_EMIT_ALL_STATUS,
		cluster_tt_status_hint_emit_mode_options, PGC_SIGHUP, 0, NULL, NULL, NULL);

#ifdef ENABLE_INJECTION
	/*
	 * spec-3.2 D5b:  test-only GUC.  Production binary (no --enable-
	 * injection-points) does not register this — symbol absent.
	 */
	DefineCustomBoolVariable(
		"cluster_test_force_visibility_cluster_path",
		gettext_noop("Test-only:  force HeapTupleSatisfiesMVCC cluster path entry via "
					 "spec-3.2 D5b inject table (overrides placeholder ITL ref reader)."),
		gettext_noop("Test build only;production binary 0 触达.  Enable + SQL UDF "
					 "cluster_test_inject_visibility_tt_ref(...) to inject authoritative "
					 "remote exact TT ref;visibility fork then takes cluster path.  "
					 "PGC_SIGHUP."),
		&cluster_test_force_visibility_cluster_path, false, PGC_SIGHUP, GUC_NOT_IN_SAMPLE, NULL,
		NULL, NULL);
#endif
}
