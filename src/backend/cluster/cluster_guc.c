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

#include "cluster/cluster_block_recovery.h"		 /* spec-4.10 D1 online block recovery GUCs */
#include "cluster/cluster_thread_recovery.h"	 /* spec-4.11 D1 online thread recovery GUCs */
#include "cluster/cluster_write_fence.h"		 /* spec-4.12 D7 write-fence enforcement GUCs */
#include "cluster/cluster_conf.h"				 /* cluster_conf_has_peers (spec-3.18 D2b latch) */
#include "cluster/cluster_cr_admit.h"			 /* cluster_cr_pool_admit* (spec-5.52 D8) */
#include "cluster/cluster_cr_coordinator_stat.h" /* cluster_cross_instance_cr_* (spec-5.57 D3) */
#include "cluster/cluster_cr_cache.h"			 /* cluster_cr_cache_max_blocks (spec-3.10 D4) */
#include "cluster/cluster_grd.h"				 /* spec-5.10 starvation-protection shared flag */
#include "cluster/cluster_cr_pool.h"			 /* cluster_shared_cr_pool_* (spec-5.51 D8) */
#include "cluster/cluster_resolver_cache.h" /* cluster_shared_resolver_cache_* (spec-5.55 D7) */
#include "cluster/cluster_guc.h"
#include "cluster/cluster_hang.h"			  /* CLUSTER_HANG_MAX_SAMPLES (spec-5.11 D7) */
#include "cluster/cluster_hang_resolve.h"	  /* HANG_RESOLVE_* + disposition GUCs (spec-5.12 D6) */
#include "cluster/storage/cluster_undo_buf.h" /* cluster_undo_buf_writeback_allowed (spec-3.18 D1) */
#include "cluster/cluster_ic.h"				  /* ClusterICTier enum values */
#include "cluster/cluster_ic_rdma.h"		  /* RDMA GUC enum values */
#include "cluster/cluster_inject.h"			  /* cluster_injection_assign_hook (stage 0.27) */
#include "cluster/cluster_pcm_lock.h"		  /* cluster_pcm_grd_max_entries (stage 1.7) */
#include "cluster/storage/cluster_shared_fs.h" /* ClusterSharedFsBackendId (stage 1.1) */
#include "cluster/cluster_xlog.h"


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
char *cluster_wal_threads_dir = NULL;  /* spec-4.1 D5; '' = flat pg_wal layout */

/* spec-4.3 D2: recovery-plan staleness threshold (observational only). */
int cluster_recovery_stale_active_ms = 10000;

/* spec-4.4 D2: recovery worker cap (0 = plan-only, no workers). */
int cluster_recovery_workers_max = 4;

/* spec-4.5 D9: merged k-way recovery (default OFF, Q8) + wait timeout. */
bool cluster_merged_recovery = false;
int cluster_recovery_merge_wait_timeout = 10000;
/* spec-5.59 D1: cross-node profiling switch (default OFF, zero hot-path cost). */
bool cluster_xnode_profile_enabled = false;
/* spec-6.4: ADG physical standby / read-only service knobs. */
int cluster_dg_role = CLUSTER_DG_ROLE_PRIMARY;
int cluster_dg_mode = CLUSTER_DG_MODE_ASYNC;
bool cluster_enable_adg = false;
bool cluster_apply_master_election = true;
char *cluster_adg_rfs_conninfos = NULL;
int cluster_adg_primary_thread_count = 0;
int cluster_adg_lag_threshold_sec = 10;
int cluster_max_standby_delay = 30;
int cluster_apply_master_switch_drain_ms = 5000;
int cluster_adg_lease_takeover_grace_ms = 5000;
int cluster_adg_barrier_interval_ms = 1000;
int cluster_wal_sender_timeout_sec = 60;
int cluster_wal_receiver_timeout_sec = 60;
/* spec-6.12c: read-layer-3 resolver terminal memo (default OFF). */
bool cluster_page_scn_shortcut = false;
/* spec-6.12a: quiescent-block S-cache via X->S downgrade (default OFF). */
bool cluster_read_scache = false;
/* spec-6.12e1: GES release-side handoff verify + counters (default OFF). */
bool cluster_ges_handoff = false;
/* spec-6.12b: cross-instance CR-server data plane (default OFF = 53R9G). */
bool cluster_crossnode_cr_data_plane = false;
/* spec-6.12g: block self-containment (active-ITL migration + opportunistic
 * commit cleanout; default OFF = the spec-5.2 D11 writer-transfer deferral). */
bool cluster_block_self_contained = false;
/* spec-6.12e2: master->holder BAST nudge on live-X-holder deny (default
 * OFF = e1 release-side handoff only). */
bool cluster_ges_bast = false;
/* spec-6.12h: keep a Past Image on block transfer/invalidate (default
 * OFF = flush + drop the copy). */
bool cluster_past_image = false;
/* spec-6.12i: active-runtime cross-instance recycled-slot visibility
 * resolution via undo-block CF fetch (default OFF = 53R97). */
bool cluster_crossnode_runtime_visibility = false;
/* spec-6.15 D1: xid space segmentation -- striped allocation (default
 * OFF = vanilla dense per-node xid allocation). */
bool cluster_xid_striping = false;
/* spec-6.15 D5/D3: herding slack (xid-value gap tolerated between
 * stripe slots; also the seeded activation-floor headroom). */
int cluster_xid_herding_slack = 4194304;
/* spec-6.12d: instance space-affinity mode + lease cap (default OFF). */
int cluster_space_affinity = CLUSTER_SPACE_AFFINITY_OFF;
int cluster_space_lease_blocks = 64;

static const struct config_enum_entry cluster_space_affinity_options[]
	= { { "off", CLUSTER_SPACE_AFFINITY_OFF, false },
		{ "static", CLUSTER_SPACE_AFFINITY_STATIC, false },
		{ "dynamic", CLUSTER_SPACE_AFFINITY_DYNAMIC, false },
		{ NULL, 0, false } };
/* spec-6.5: cluster-aware backup / restore / PITR target knobs. */
char *cluster_recovery_target_scn = NULL;
char *cluster_recovery_target_cluster_time = NULL;
char *cluster_recovery_target_name = NULL;
int cluster_recovery_target_action = CLUSTER_RECOVERY_TARGET_ACTION_PAUSE;
bool cluster_enable_pitr_restore_points = false;
int cluster_pitr_restore_point_interval_ms = 0;
int cluster_restore_point_drain_timeout_ms = 30000;
int cluster_backup_wal_retention = 0;
int cluster_backup_parallel_channels = 1;
int cluster_backup_manifest_checksums = CLUSTER_BACKUP_MANIFEST_CHECKSUM_CRC32C;
int cluster_shared_storage_backend = CLUSTER_SHARED_FS_BACKEND_STUB;
/* spec-4.5a D2: shared data root for the cluster_fs (shared_fs) backend. */
char *cluster_shared_data_dir = NULL;
/* spec-4.5a D2: optional external-preset shared-storage uuid (sentinel). */
char *cluster_shared_storage_uuid = NULL;
/* spec-6.0a: raw block-device backend configuration. */
char *cluster_block_device_path = NULL;
bool cluster_block_device_use_odirect = true;
int cluster_storage_fence_driver = CLUSTER_STORAGE_FENCE_DRIVER_AUTO;
/*
 * spec-5.6 Da3: opt-in switch for the shared pg_control authority.  Default
 * off (Hardening v1.0.1): a node only migrates its global/pg_control into the
 * shared authority + symlink, and enforces the startup gate, when this is on.
 * Off keeps the stock per-node pg_control path so existing shared-data
 * (shared-nothing-simulation) clusters are unaffected.
 */
bool cluster_controlfile_shared_authority = false;
bool cluster_smgr_user_relations = false;
/*
 * spec-6.14 D1: shared system-catalog single authority.  Off by default; on
 * requires smgr_user_relations=on + shared_data_dir + controlfile_shared_
 * authority (startup vet in cluster_shared_fs_init).  cluster.oid_lease_size
 * is the per-node OID lease block size (D6).
 */
bool cluster_shared_catalog = false;
int cluster_oid_lease_size = 8192;
int cluster_shmem_max_regions = 80; /* spec-5.56: 64 -> 80 (cr relgen region; restore margin) */

/* spec-3.18 D1: undo block buffer pool slot count (0 = disabled). */
int cluster_undo_buffers = 2048;
/*
 * spec-3.18 D1/D2b + spec-3.25 D4: buffered write-back.  DEFAULT ON since
 * spec-3.25 (auto semantics): the runtime latch in
 * cluster_undo_buf_writeback_allowed() forces write-through whenever the node
 * has peers, so the default only takes effect on a known single-node topology.
 * The check_hook is feedback-only (never rejects).
 */
bool cluster_undo_buffer_writeback = true;

/*
 * spec-4.8ab D5: undo checkpoint-writeback boundary check mode (two-layer
 * model, §3.1).  Backing variable for cluster.undo_writeback_boundary_check;
 * read in cluster_undo_buf.c via the header extern.  DEFAULT ON -- but this GUC
 * controls ONLY the advisory verdict-accounting layer.  The hard corruption
 * fail-closed guards (cluster_undo_boundary_violation) are unconditional under
 * every value (off included), so 8.A correctness is never gated by the GUC.
 */
int cluster_undo_writeback_boundary_check = CLUSTER_UNDO_WB_CHECK_ON;
static const struct config_enum_entry cluster_undo_writeback_boundary_check_options[]
	= { { "off", CLUSTER_UNDO_WB_CHECK_OFF, false },
		{ "on", CLUSTER_UNDO_WB_CHECK_ON, false },
		{ "strict", CLUSTER_UNDO_WB_CHECK_STRICT, false },
		{ NULL, 0, false } };

int cluster_grd_max_entries = 1024;
bool cluster_grd_entry_reclaim = true;
int cluster_grd_entry_reclaim_max_per_sweep = 256;
int cluster_ges_request_timeout_ms = 60000;	   /* spec-2.16 D12 + v0.5 P1.5 */
int cluster_cf_enqueue_timeout_ms = 30000;	   /* spec-5.6 Dc4b — CF X/S grant wait */
int cluster_ges_starvation_max_skips = 8;	   /* spec-5.10 — boost after N skips */
bool cluster_ges_starvation_protection = true; /* spec-5.10 — fairness on/off */

/*
 * spec-5.10 P1#1 — assign-hook for cluster.ges_starvation_protection.  Flips
 * the cluster-global shared flag only (a single atomic write is assign-hook
 * safe); the boosted-state sweep that clears already-barriered waiters runs in
 * the LMON cluster context (cluster_grd_clear_all_boosted), never here.
 */
static void
cluster_ges_starvation_protection_assign_hook(bool newval, void *extra pg_attribute_unused())
{
	cluster_grd_set_starvation_protection(newval);
}

/* spec-5.3 D10 — TM cross-node convert tunables. */
int cluster_ges_convert_timeout_ms = 30000; /* finite convert wait before 53R70 */
int cluster_tm_convert_mode = CLUSTER_TM_CONVERT_MODE_CONVERT;

/* spec-4.6 D4/D1 — failure-driven remaster tunables. */
int cluster_grd_remaster_wait_ms = 200;	   /* frozen-shard short wait before 53R9I */
int cluster_grd_rebuild_timeout_ms = 5000; /* holder-rebuild barrier deadline */

/* spec-5.4 D8 — SQ sequence lock tunables. */
int cluster_sequence_default_cache = 100;		/* CREATE-time CACHE injection default */
int cluster_sequence_cache_floor_optin = 0;		/* opt-in seqcache floor (0 = off) */
int cluster_sequence_refill_timeout_ms = 30000; /* SQ refill wait before fail-closed */

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

/* spec-5.11 D7: Hang Manager (DIAG-hosted long-wait sampler) GUCs. */
bool cluster_hang_manager_enabled = true;
int cluster_hang_sample_interval_ms = 10000;
int cluster_hang_threshold_ms = 60000;
bool cluster_hang_dump_enabled = true;
int cluster_hang_max_chain_depth = 100;
int cluster_hang_max_sampled = 64;

/* spec-5.12 D6: Hang Manager disposition GUCs.  Default mode = advisory
 * (evaluate + recommend, never act); enforce is explicit opt-in (spec §8 Q4).
 * priority / cpu are NOT weights — PG exposes no such signal (spec §8 Q7). */
int cluster_hang_resolution_mode = HANG_RESOLVE_ADVISORY;
int cluster_hang_resolution_confirm_rounds = 2;
int cluster_hang_resolution_soft_timeout_ms = 5000;
int cluster_hang_resolution_max_per_round = 1;
double cluster_hang_victim_w_age = 0.5;
double cluster_hang_victim_w_rollback = 0.3;
double cluster_hang_victim_w_blockers = 0.2;

static const struct config_enum_entry cluster_hang_resolution_mode_options[]
	= { { "off", HANG_RESOLVE_OFF, false },
		{ "advisory", HANG_RESOLVE_ADVISORY, false },
		{ "enforce", HANG_RESOLVE_ENFORCE, false },
		{ NULL, 0, false } };

/* spec-5.13 D11: clean leave reconfiguration opt-in (default OFF, 5.6 paradigm)
 * + drain deadline.  OFF = byte-identical to today (no clean-leave path). */
bool cluster_clean_leave_enabled = false;
int cluster_clean_leave_drain_timeout_ms = 30000;

/* spec-1.14 D8: cluster.cluster_stats_main_loop_interval (mirror). */
int cluster_cluster_stats_main_loop_interval = 1000;

/* spec-3.13 D1: Undo Cleaner mirrors. */
int cluster_undo_cleaner_interval_ms = 30000;
bool cluster_undo_cleaner_enabled = true;
int cluster_undo_cleaner_batch_segments = 8;

/* spec-4.12a D1: enable record-segment ACTIVE -> COMMITTED drain on rollover
 * (the leak fix).  Default on; off reverts to the pre-4.12a leak behaviour for
 * the L1 repro / emergency disable.  It gates only the optimization (the drain
 * attempt), NOT the 8.A guard -- when off no ACTIVE -> COMMITTED advance runs,
 * so it can never cause false reclaim (spec §2.3 / Q7). */
bool cluster_undo_record_segment_commit_on_rollover = true;

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
int cluster_voting_disk_size_bytes = 328192; /* spec-6.15: (5 × 128 + 1) × 512 */

/* spec-5.15 D7 — online declared-node join.  Default off (capability opt-in;
 * fail-closed-safe via INV-J8 — a DEAD node is never auto-readmitted when off).
 * Registered in cluster_guc.c init (PGC_POSTMASTER). */
bool cluster_online_join = false;
/* spec-5.15 D7 — join convergence / commit deadline (PGC_SIGHUP). */
int cluster_join_convergence_timeout_ms = 30000;
/*
 * spec-5.16 D6 — gate the OPTIONAL GRD logical-lock rebalance on node rejoin
 * (move the joiner's home-shard GES mastership back from the survivor).  The
 * PCM block snap-back fence is NOT gated by this — it is forced correctness
 * bound to cluster.online_join (INV-R13);  off only leaves GRD mastership on
 * the survivor (load imbalance, no correctness impact).  Default off (opt-in).
 */
bool cluster_join_remaster_enabled = false;

/* spec-5.18 D13 — permanent node removal opt-in + cleanup ACK-barrier deadline. */
bool cluster_online_node_removal = false;
int cluster_node_removal_cleanup_timeout_ms = 30000;

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
int cluster_interconnect_rdma_fallback = CLUSTER_IC_RDMA_FALLBACK_AUTO;
int cluster_interconnect_rdma_provider = CLUSTER_IC_RDMA_PROVIDER_AUTO;
int cluster_interconnect_rdma_completion = CLUSTER_IC_RDMA_COMPLETION_EVENT;
int cluster_interconnect_rdma_busypoll_us = 50;
bool cluster_interconnect_rdma_crc_offload = false;
int cluster_interconnect_rdma_inline_max = 256;
int cluster_interconnect_rdma_max_send_wr = 256;

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
int cluster_undo_extent_blocks = 4; /* spec-3.18 D3 extent granularity */

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
 * cluster.cr_gate_no_peer_fastpath (spec-3.24 D1).  DEFAULT ON.
 * When on, a no-peer + session-local cluster snapshot skips the CR/SCN cluster
 * visibility fork entirely and uses the PG-native MVCC body: with no peers the
 * AD-012 例外 9 "never PG-native" premise (a remote xid absent from the local
 * ProcArray) is void, so for a session-local snapshot (row #1) the PG-native
 * verdict equals the SCN/CR verdict.  Imported snapshots (row #2) and any
 * has-peers topology are excluded.  Default flipped ON after the spec-3.24 D1
 * differential (t/239) AND the clean-CI Dfp stop gate both proved equivalence
 * (CR gate residual Dfp vs C = 0.2-0.3% << 2%); turn off as a diagnostic escape
 * hatch.  CR-specific single-node tests pin it off to keep exercising CR.
 */
bool cluster_cr_gate_no_peer_fastpath = true;

/* spec-5.54 D5: tuple-level / verdict-only CR read fast path, default OFF. */
bool cluster_cr_tuple_level_fastpath = false;

/*
 * cluster.tt_durable_lookup (spec-3.11 D7).  When on (default), visibility / CR
 * resolve commit_scn from the durable undo-segment-header TT slot on overlay
 * miss (D5) and resolve the watermark gate by xid (D6).  Off falls back to the
 * spec-3.10 overlay-only behavior: an overlay miss / watermark>read_scn fails
 * closed (53R97 / 53R9F).  Durability writes (commit stamp + redo) are NOT
 * gated -- only the read-side resolution -- so toggling off never loses data.
 */
bool cluster_tt_durable_lookup = true;

/*
 * cluster.tt_recovery_resolve_active (spec-4.8 D1).  When on (default), the
 * startup process, after redo + prepared-xact recovery, resolves every crash-
 * left TT_SLOT_ACTIVE durable slot to TT_SLOT_ABORTED unless its owning xact is
 * committed or a resurrected prepared 2PC xact (fail-closed: an ACTIVE slot we
 * cannot prove live is aborted).  Off skips the resolution: slots stay ACTIVE
 * and fall through to the pre-4.8 by-xid 0-match path (no correctness loss --
 * only the explicit ABORTED verdict + housekeeping).  PGC_POSTMASTER: the
 * resolution runs once at startup.
 */
bool cluster_tt_recovery_resolve_active = true;

/*
 * spec-6.2 Cache Fusion terminal authority + Smart Fusion.  Both correctness-
 * sensitive halves default OFF so an upgraded binary preserves Stage 5
 * conservative behavior until explicitly enabled.
 */
bool cluster_cf_terminal_authority = false;
int cluster_cf_delayed_cleanout = CLUSTER_CF_DELAYED_CLEANOUT_READER;
bool cluster_smart_fusion = false;
int cluster_smart_fusion_tier_min = CLUSTER_IC_TIER_3;
int cluster_smart_fusion_commit_brake_timeout_ms = 5000;
int cluster_smart_fusion_origin_durable_gossip_ms = 50;
static bool cluster_smart_fusion_enable_requested = false;

/*
 * cluster.undo_retention_horizon_enabled (spec-3.12 D5).  When on (default),
 * the TT-slot / undo-segment allocators keep COMMITTED slots/segments alive
 * while a live reader's read_scn still needs the durable pre-image (own-
 * instance retention horizon gate; retires the spec-3.11 L4 watermark
 * fail-closed).  Off bypasses the gate and recycles COMMITTED/ABORTED
 * immediately (spec-3.11 behavior) -- debug / rollback escape hatch (C6).
 * Read on the allocation slow path only, so SIGHUP toggling is race-free
 * (each alloc re-reads the GUC + recomputes the horizon).
 */
bool cluster_undo_retention_horizon_enabled = true;

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
 * spec-5.5 D7:cluster.advisory_lock_enabled — master switch for cross-node
 * advisory (user) lock globalization.  Default true.  When false, both
 * session- and xact-scoped pg_advisory_lock* route PG-native (single-node
 * semantics):  this is a forensic/test-only UNSAFE downgrade that silently
 * disables cross-node advisory mutual exclusion (Q6/§3.3 R3).  PGC_SUSET.
 */
bool cluster_advisory_lock_enabled = true;

/*
 * spec-5.7 HW: gate the cluster relation-extend block-number authority.  on =
 * permanent shared relations are extended through the cluster authority (HW(X) +
 * HW_ALLOC).  off = forensic/test-only UNSAFE downgrade: extends fall back to
 * the PG-native local FileSize path, which silently corrupts in a multi-node
 * cluster (two nodes can allocate the same block range).  PGC_SUSET.
 */
bool cluster_relation_extend_lock_enabled = true;
bool cluster_tablespace_ddl_lock_enabled = true; /* spec-5.7 TT */
bool cluster_object_reuse_flush_enabled = true;	 /* spec-5.7 KO */
/* spec-5.14 D6: diag-only; when on, each touched fail-stop abort LOGs the
 * aborting transaction's touched-set hex (false-positive investigation). */
bool cluster_touched_peers_trace = false;

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
 * spec-5.8 D3 — coordinator-driven cross-node deadlock detection controls.
 *
 *	deadlock_detection_enabled gates the whole coordinator scan; the two
 *	interval GUCs tune the scan period and the two-round confirm delay.  The
 *	confirm round is the Rule 8.A transient filter: a cross-node cycle is
 *	cancelled only if observed identically (same members, same per-node graph
 *	generations, same cycle) in both rounds.
 */
bool cluster_lmd_deadlock_detection_enabled = true;
int cluster_lmd_global_dd_interval_ms = 2000;
int cluster_lmd_deadlock_confirm_interval_ms = 500;

/*
 * PGRAC: spec-5.9 D10 — deadlock victim policy + cancel robustness knobs.
 *
 *	cancel_ack_timeout_ms (default 1000, < global_dd_interval_ms 2000): the
 *	coordinator retransmits a cross-node cancel if no CANCEL_ACK arrives within
 *	this window.  cancel_max_retransmit (default 3): bounded retransmit attempts
 *	before escalating to an alternate victim;  0 disables retransmit (~= 5.8
 *	fire-and-forget).  victim_repeat_window_ms (default 5000): anti-thrash
 *	window — a victim 4-tuple chosen within this window that re-deadlocks is a
 *	livelock symptom, so the coordinator prefers an alternate.
 */
int cluster_cancel_ack_timeout_ms = 1000;
int cluster_cancel_max_retransmit = 3;
int cluster_victim_repeat_window_ms = 5000;

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

/* PGRAC: spec-4.7a D2/Q8 — hold-until-revoked node-level PCM cache kill-switch.
 * ON (default): the bufmgr acquire gate skips the remote master round-trip when
 * the node already holds a covering PCM mode (X ⊇ {S,X}, S ⊇ S), and X is held
 * across content-lock unlock (released only on INVALIDATE / eviction).  OFF:
 * reverts to per-LockBuffer remote requests + X released on every unlock (the
 * spec-2.33 behavior) — an escape hatch if a stale-grant edge is ever found. */
bool cluster_gcs_block_local_cache = true;
/* spec-5.2 D4: cross-node TX enqueue completion wait.  On (default) makes a
 * remote row-lock conflict block until the holder completes; off reverts to
 * the spec-3.4d fail-closed (53R98) honest degradation. */
bool cluster_tx_enqueue_wait_enabled = true;
bool cluster_ic_duty_lazy = true; /* spec-7.2 D1 duty-chain on-demand gating */
int cluster_gcs_block_dedup_max_entries = 1024;

/*
 * PGRAC: spec-4.7 D1 — cluster.gcs_block_recovery_wait_ms.  Bounded backend
 * wait (ms) on a block resource in GCS_BLOCK_RECOVERING (survivor re-declare /
 * master rebuild after reconfiguration) before fail-closing 53R9L.  Default
 * 200ms — short enough to retry promptly, long enough to absorb an in-flight
 * rebuild.  PGC_SIGHUP (operators may tune without restart).
 */
int cluster_gcs_block_recovery_wait_ms = 200;

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

/* spec-4.10 D1: online single-block recovery (storage defined here; logic in
 * cluster_block_recovery.c). */
bool cluster_online_block_recovery = true;
int cluster_block_recovery_on_unrecoverable = CLUSTER_BLKREC_ACTION_ERROR;

static const struct config_enum_entry cluster_block_recovery_on_unrecoverable_options[]
	= { { "error", CLUSTER_BLKREC_ACTION_ERROR, false },
		{ "panic", CLUSTER_BLKREC_ACTION_PANIC, false },
		{ NULL, 0, false } };

/* spec-4.11 D1: online single-thread recovery (storage defined here; logic in
 * cluster_thread_recovery.c).  Dev default OFF (Q7/P2): default-on only once
 * the D7 capability gate is complete and every unsupported environment is
 * proven fail-closed; flip to true at ship. */
bool cluster_online_thread_recovery = false;
int cluster_thread_recovery_on_unrecoverable = CLUSTER_THREADREC_ACTION_KEEP_FROZEN;

static const struct config_enum_entry cluster_thread_recovery_on_unrecoverable_options[]
	= { { "keep_frozen", CLUSTER_THREADREC_ACTION_KEEP_FROZEN, false },
		{ "panic", CLUSTER_THREADREC_ACTION_PANIC, false },
		{ NULL, 0, false } };

/* spec-4.12 D7: cooperative write-fence GUC storage (referenced from
 * cluster_write_fence.c via the header externs).  Defaults match the registered
 * boot values below (on / 6000 ms -- spec-4.12b D4 flipped enforcement to ON). */
int cluster_write_fence_enforcement = CLUSTER_WRITE_FENCE_ENFORCE_ON;
int cluster_write_fence_lease_ms = 6000;

/* spec-4.12b D4: cooperative write-fence enforcement now ships default ON.  The
 * spec-4.12b baseline-marker subsystem (D2) keeps a healthy steady-state cluster's
 * durable authority marker fresh, so the D5 hot gate no longer fails every write
 * closed for lack of a marker; the D3 bring-up latch + qvotec quorum gate cover the
 * boot window.  A single node / no-voting-disk deployment auto-degrades to a no-op
 * at runtime (cluster_write_fence_enforcing(), LOG-once from qvotec startup) so it
 * stays writable out of the box.  "dev" / "off" remain explicit escape hatches. */
static const struct config_enum_entry cluster_write_fence_enforcement_options[]
	= { { "off", CLUSTER_WRITE_FENCE_ENFORCE_OFF, false },
		{ "on", CLUSTER_WRITE_FENCE_ENFORCE_ON, false },
		{ "dev", CLUSTER_WRITE_FENCE_ENFORCE_DEV, false },
		{ NULL, 0, false } };

/* spec-5.3 D10 — same-backend TM table-lock upgrade routing: Oracle DLM
 * convert (default, Q1=A) vs PG additive double-hold escape hatch (Q1=B). */
static const struct config_enum_entry cluster_tm_convert_mode_options[]
	= { { "convert", CLUSTER_TM_CONVERT_MODE_CONVERT, false },
		{ "additive", CLUSTER_TM_CONVERT_MODE_ADDITIVE, false },
		{ NULL, 0, false } };

/*
 * spec-5.1b D7: the cluster.ges_mode_selfcheck GUC (and its
 * cluster_ges_mode_selfcheck_options off/warn/fatal table) was removed.
 * The frozen matrix now drives the live GRD grant decision, so a drift is a
 * P0 double-grant hazard with no safe "continue anyway" tier — the startup
 * self-check is unconditional and always FATAL (cluster_ges_mode_init).
 */


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

static const struct config_enum_entry cluster_cf_delayed_cleanout_options[]
	= { { "off", CLUSTER_CF_DELAYED_CLEANOUT_OFF, false },
		{ "reader", CLUSTER_CF_DELAYED_CLEANOUT_READER, false },
		{ "eager", CLUSTER_CF_DELAYED_CLEANOUT_EAGER, false },
		{ NULL, 0, false } };

static const struct config_enum_entry cluster_smart_fusion_tier_min_options[]
	= { { "tier3", CLUSTER_IC_TIER_3, false }, { NULL, 0, false } };

static const struct config_enum_entry cluster_interconnect_rdma_fallback_options[]
	= { { "auto", CLUSTER_IC_RDMA_FALLBACK_AUTO, false },
		{ "off", CLUSTER_IC_RDMA_FALLBACK_OFF, false },
		{ NULL, 0, false } };

static const struct config_enum_entry cluster_interconnect_rdma_provider_options[]
	= { { "auto", CLUSTER_IC_RDMA_PROVIDER_AUTO, false },
		{ "verbs", CLUSTER_IC_RDMA_PROVIDER_VERBS, false },
		{ "mlx5", CLUSTER_IC_RDMA_PROVIDER_MLX5, false },
		{ NULL, 0, false } };

static const struct config_enum_entry cluster_interconnect_rdma_completion_options[]
	= { { "event", CLUSTER_IC_RDMA_COMPLETION_EVENT, false },
		{ "busypoll", CLUSTER_IC_RDMA_COMPLETION_BUSYPOLL, false },
		{ NULL, 0, false } };


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

static const struct config_enum_entry cluster_dg_role_options[]
	= { { "primary", CLUSTER_DG_ROLE_PRIMARY, false },
		{ "standby", CLUSTER_DG_ROLE_STANDBY, false },
		{ NULL, 0, false } };

static const struct config_enum_entry cluster_dg_mode_options[]
	= { { "async", CLUSTER_DG_MODE_ASYNC, false },
		{ "sync", CLUSTER_DG_MODE_SYNC, false },
		{ "max_availability", CLUSTER_DG_MODE_MAX_AVAILABILITY, false },
		{ NULL, 0, false } };

static const struct config_enum_entry cluster_recovery_target_action_options[]
	= { { "pause", CLUSTER_RECOVERY_TARGET_ACTION_PAUSE, false },
		{ "promote", CLUSTER_RECOVERY_TARGET_ACTION_PROMOTE, false },
		{ "shutdown", CLUSTER_RECOVERY_TARGET_ACTION_SHUTDOWN, false },
		{ NULL, 0, false } };

static const struct config_enum_entry cluster_backup_manifest_checksum_options[]
	= { { "crc32c", CLUSTER_BACKUP_MANIFEST_CHECKSUM_CRC32C, false }, { NULL, 0, false } };

static const struct config_enum_entry cluster_storage_fence_driver_options[]
	= { { "disabled", CLUSTER_STORAGE_FENCE_DRIVER_DISABLED, false },
		{ "auto", CLUSTER_STORAGE_FENCE_DRIVER_AUTO, false },
		{ "scsi3_pr", CLUSTER_STORAGE_FENCE_DRIVER_SCSI3_PR, false },
		{ NULL, 0, false } };

/*
 * check_cluster_shared_data_dir -- GUC check_hook for
 *	cluster.shared_data_dir (spec-4.5a D2).
 *
 *	An empty value is allowed (the shared_fs backend is not in use, or the
 *	startup cross-check in cluster_shared_fs_init will reject the
 *	combination shared_storage_backend=cluster_fs + empty dir).  A
 *	non-empty value must be an absolute path: the shared_fs backend
 *	prepends it to a relative relpath, and every node must name the same
 *	shared mount, which only an absolute path can express unambiguously.
 */
static bool
check_cluster_shared_data_dir(char **newval, void **extra, GucSource source)
{
	if (*newval != NULL && (*newval)[0] != '\0' && !is_absolute_path(*newval)) {
		GUC_check_errdetail("cluster.shared_data_dir must be an absolute path.");
		return false;
	}
	return true;
}

static bool
check_cluster_block_device_path(char **newval, void **extra, GucSource source)
{
	if (*newval != NULL && (*newval)[0] != '\0' && !is_absolute_path(*newval)) {
		GUC_check_errdetail("cluster.block_device_path must be an absolute path.");
		return false;
	}
	return true;
}

/*
 * spec-6.12d: dynamic space affinity needs the spec-6.3 DRM remaster
 * machinery; reject the value explicitly until that ships (rule 8 --
 * never a silently inert setting).
 */
static bool
check_cluster_space_affinity(int *newval, void **extra, GucSource source)
{
	if (*newval == CLUSTER_SPACE_AFFINITY_DYNAMIC) {
		GUC_check_errdetail("cluster.space_affinity = \"dynamic\" requires the "
							"DRM remaster machinery (spec-6.3), which is not "
							"implemented yet; use \"static\" or \"off\".");
		return false;
	}
	return true;
}


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
/*
 * spec-4.1 D5 — cluster.wal_threads_dir shape check.
 *
 *	Accepts the empty string (flat layout) or an absolute path; a
 *	relative path would silently depend on the postmaster cwd and make
 *	the startup validator (cluster_wal_thread_init) compare the wrong
 *	directory.  Semantic validation (directory exists, pg_wal resolves
 *	into it, claim ownership) is startup's job, not the parser's.
 */
static bool
cluster_wal_threads_dir_check_hook(char **newval, void **extra, GucSource source)
{
	(void)extra;
	(void)source;
	if (*newval == NULL || (*newval)[0] == '\0')
		return true;
	if (!is_absolute_path(*newval)) {
		GUC_check_errcode(ERRCODE_INVALID_PARAMETER_VALUE);
		GUC_check_errdetail("cluster.wal_threads_dir must be an absolute path (or empty to keep "
							"the flat pg_wal layout).");
		return false;
	}
	return true;
}

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

/*
 * spec-3.18 D2b:  undo buffer write-back is WAL-protected (XLOG_UNDO_BLOCK_WRITE
 * FPI/delta + redo) and made durable by the checkpoint write-back flush
 * (CheckPointGuts -> cluster_undo_buf_flush_all) + eviction flush +
 * DELAY_CHKPT_START, so enabling the GUC is safe in a SINGLE-NODE topology.
 *
 * The hard enforcement is the runtime latch in
 * cluster_undo_buf_writeback_allowed() (pool exists && GUC on && no peers) --
 * write-back is structurally impossible while the node has peers, because
 * buffered undo is durable only via local WAL + checkpoint flush and a remote
 * node reading shared storage would see stale undo (no Cache Fusion yet).
 *
 * This hook only adds operator-visible feedback:  warn when the GUC is turned
 * on in a peered topology (it will silently no-op back to write-through).  At
 * initial startup ClusterConfShmem is not yet loaded, so cluster_conf_has_peers()
 * is safely false (no spurious warning);  the warning fires on a SIGHUP that
 * enables it after the cluster config is up.  Accept the value either way --
 * the runtime latch, not a rejection, is the safety boundary.
 */
static bool
cluster_undo_buffer_writeback_check_hook(bool *newval, void **extra, GucSource source)
{
	(void)extra;
	(void)source;
	if (*newval && cluster_conf_has_peers())
		ereport(WARNING,
				(errmsg("cluster.undo_buffer_writeback has no effect in a multi-node cluster"),
				 errdetail("Buffered undo write-back is durable only via local WAL + checkpoint "
						   "flush;  a peer reading shared storage would see stale undo."),
				 errhint("Undo continues to use the write-through path (per-commit fsync to "
						 "shared storage).  Single-node deployments may enable write-back.")));
	return true;
}

static const char *
cluster_show_adg_primary_thread_count(void)
{
	static char nbuf[16];
	int count;

	count = cluster_conf_node_count();
	if (count < 0 || count > CLUSTER_WAL_THREAD_MAX)
		count = 0;
	snprintf(nbuf, sizeof(nbuf), "%d", count);
	return nbuf;
}

/*
 * spec-6.2 post-ship guardrail: Smart Fusion's early-transfer enabled path is
 * a correctness-sensitive visibility / durability authority.  Keep the
 * substrate counters, wire definitions, and off-path behavior, but reject the
 * runtime path until checkpoint writeback, 2PC, and dependency-consumer
 * contracts are closed end to end.
 */
static bool
cluster_smart_fusion_check_hook(bool *newval, void **extra, GucSource source)
{
	(void)extra;
	(void)source;

	if (!*newval)
		return true;

	cluster_smart_fusion_enable_requested = true;
	GUC_check_errcode(ERRCODE_INVALID_PARAMETER_VALUE);
	GUC_check_errdetail("cluster.smart_fusion is fail-closed after the "
						"v0.121.0-stage6.2 review because the enabled path still "
						"lacks complete checkpoint, 2PC, and dependency-consumer "
						"soundness; leave it off until those spec-6.2 blockers "
						"are fixed.");
	return false;
}

bool
cluster_smart_fusion_failclosed_requested(void)
{
	return cluster_smart_fusion_enable_requested;
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
					 "uses TCP; tier2 selects the RDMA-capable transport mux; "
					 "tier3 requests the RDMA optimized provider and may fall back "
					 "to generic verbs when mlx5dv is unavailable. "
					 "See docs/cluster-ic-design.md."),
		&cluster_interconnect_tier, CLUSTER_IC_TIER_STUB,  /* boot value */
		cluster_interconnect_tier_options, PGC_POSTMASTER, /* tier change requires restart */
		0,												   /* flags */
		NULL,											   /* check_hook */
		NULL,											   /* assign_hook */
		NULL);											   /* show_hook */

	DefineCustomEnumVariable(
		"cluster.interconnect_rdma_fallback",
		gettext_noop("Policy for RDMA-to-TCP interconnect fallback."),
		gettext_noop("auto allows the transport mux to use tier1 TCP when RDMA is unavailable "
					 "for a peer. off fails closed if RDMA cannot be used."),
		&cluster_interconnect_rdma_fallback, CLUSTER_IC_RDMA_FALLBACK_AUTO,
		cluster_interconnect_rdma_fallback_options, PGC_POSTMASTER, 0, NULL, NULL, NULL);

	DefineCustomEnumVariable(
		"cluster.interconnect_rdma_provider",
		gettext_noop("RDMA provider selection for tier2/tier3 interconnect."),
		gettext_noop("auto and verbs request generic libibverbs; mlx5 requests the "
					 "spec-6.13 optimized provider and falls back according to "
					 "cluster.interconnect_rdma_fallback."),
		&cluster_interconnect_rdma_provider, CLUSTER_IC_RDMA_PROVIDER_AUTO,
		cluster_interconnect_rdma_provider_options, PGC_POSTMASTER, 0, NULL, NULL, NULL);

	DefineCustomEnumVariable(
		"cluster.interconnect_rdma_completion",
		gettext_noop("RDMA completion model for the interconnect."),
		gettext_noop("event integrates with the LMON wait loop; busypoll drains the "
					 "CQ on the LMON tick within cluster.interconnect_rdma_busypoll_us."),
		&cluster_interconnect_rdma_completion, CLUSTER_IC_RDMA_COMPLETION_EVENT,
		cluster_interconnect_rdma_completion_options, PGC_POSTMASTER, 0, NULL, NULL, NULL);

	DefineCustomIntVariable(
		"cluster.interconnect_rdma_busypoll_us",
		gettext_noop("RDMA busy-poll spin budget in microseconds."),
		gettext_noop("Spin budget consumed by cluster.interconnect_rdma_completion=busypoll "
					 "before the LMON loop yields."),
		&cluster_interconnect_rdma_busypoll_us, 50, 0, 10000, PGC_SIGHUP, 0, NULL, NULL, NULL);

	DefineCustomBoolVariable(
		"cluster.interconnect_rdma_crc_offload",
		gettext_noop("Reserved RDMA control-plane CRC offload switch."),
		gettext_noop("Spec-6.1 rejects this setting when enabled; block shipping always "
					 "uses application-level CRC32C."),
		&cluster_interconnect_rdma_crc_offload, false, PGC_POSTMASTER, 0, NULL, NULL, NULL);

	DefineCustomIntVariable(
		"cluster.interconnect_rdma_inline_max",
		gettext_noop("RDMA inline-send threshold in bytes."),
		gettext_noop("Small control frames at or below this size may use inline send when "
					 "the provider supports it."),
		&cluster_interconnect_rdma_inline_max, 256, 0, 4096, PGC_POSTMASTER, GUC_UNIT_BYTE, NULL,
		NULL, NULL);

	DefineCustomIntVariable(
		"cluster.interconnect_rdma_max_send_wr",
		gettext_noop("RDMA send work request queue depth per peer."),
		gettext_noop("Defines the per-peer RDMA send queue depth before backpressure."),
		&cluster_interconnect_rdma_max_send_wr, 256, 16, 4096, PGC_POSTMASTER, 0, NULL, NULL, NULL);

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
	 * cluster.wal_threads_dir -- shared-storage root of the per-thread
	 * WAL stream layout (spec-4.1 D5).  Empty (the default) keeps the
	 * flat pg_wal layout untouched.  When set, postmaster startup
	 * validates that $PGDATA/pg_wal resolves to
	 * <dir>/thread_<cluster.node_id + 1> and that this node owns the
	 * thread claim file; any mismatch is FATAL 53RA0/53RA1
	 * (cluster_wal_thread_init, spec-4.1 §2.4 -- never a silent
	 * fallback).  The relocation itself is bootstrap-managed
	 * (pgrac-init --wal-threads-dir via initdb -X); the only consumer
	 * of this GUC is the startup validator, no hot path reads it.
	 */
	DefineCustomStringVariable(
		"cluster.wal_threads_dir",
		gettext_noop("Shared-storage root directory of the per-thread WAL layout."),
		gettext_noop("Empty keeps the flat pg_wal layout.  When set, pg_wal must resolve "
					 "to <dir>/thread_<id> for this node (id = cluster.node_id + 1); "
					 "startup is refused otherwise."),
		&cluster_wal_threads_dir, "",		/* boot value */
		PGC_POSTMASTER,						/* layout is fixed at startup */
		0,									/* flags */
		cluster_wal_threads_dir_check_hook, /* check_hook */
		NULL,								/* assign_hook */
		NULL);								/* show_hook */

	/*
	 * cluster.recovery_stale_active_ms -- staleness window for the
	 * spec-4.3 recovery plan pass.  An ACTIVE registry slot whose
	 * last_updated is older than this is classified CRASHED_CANDIDATE
	 * (fresher -> ALIVE).  Observational only in this stage: nothing
	 * acts on the classification (spec-4.5 merged replay will).  The
	 * default is 10x the cluster.cluster_stats_main_loop_interval
	 * default (1000ms) so a peer with default refresh cadence has a
	 * 10-tick margin before being reported as a crash candidate;
	 * operators running a larger refresh interval on PEER nodes must
	 * raise this accordingly (the slot does not carry the writer's
	 * interval -- spec-4.3 §6 R2, slated for the spec-4.5 ABI review).
	 */
	DefineCustomIntVariable(
		"cluster.recovery_stale_active_ms",
		gettext_noop("Staleness window before an ACTIVE WAL-state slot is reported as a "
					 "crash candidate."),
		gettext_noop("Observational: the recovery plan only reports the classification."),
		&cluster_recovery_stale_active_ms, 10000, /* boot value */
		1000, 3600000,							  /* min / max */
		PGC_POSTMASTER,							  /* plan runs once at startup */
		GUC_UNIT_MS,							  /* flags */
		NULL,									  /* check_hook */
		NULL,									  /* assign_hook */
		NULL);									  /* show_hook */

	/*
	 * cluster.recovery_workers_max -- cap on the spec-4.4 candidate
	 * stream-validation workers (dynamic bgworkers; slots come out of
	 * the max_worker_processes TOTAL pool, which parallel query also
	 * draws from).  0 disables spawning (plan-only).  The default
	 * leaves half of the default max_worker_processes(8) budget free.
	 */
	DefineCustomIntVariable(
		"cluster.recovery_workers_max", gettext_noop("Maximum recovery stream-validation workers."),
		gettext_noop("0 disables worker spawning; workers only run when the recovery "
					 "plan reports crash candidates."),
		&cluster_recovery_workers_max, 4, /* boot value */
		0, 16,							  /* min / max (pool slots) */
		PGC_POSTMASTER,					  /* launch happens at startup */
		0,								  /* flags */
		NULL,							  /* check_hook */
		NULL,							  /* assign_hook */
		NULL);							  /* show_hook */

	/*
	 * cluster.merged_recovery -- enable cold-crash k-way SCN merged
	 * recovery (spec-4.5).  Default OFF (Q8): merged replay only engages
	 * on a plain local crash when no foreign node is ALIVE and there are
	 * crash candidates, and even then a 53RA3 hard gate must pass.  OFF
	 * is today's single-stream behaviour, byte-identical.
	 */
	DefineCustomBoolVariable(
		"cluster.merged_recovery", gettext_noop("Enable cold-crash k-way SCN merged recovery."),
		gettext_noop("Off keeps single-stream recovery (this node's own thread only)."),
		&cluster_merged_recovery, false, PGC_POSTMASTER, 0, NULL, NULL, NULL);

	/*
	 * cluster.xnode_profile -- cross-node per-bucket profiling (spec-5.59).
	 * Default OFF: every probe early-returns on one branch with no clock
	 * read, keeping instrumented hot paths byte-equivalent to the
	 * un-instrumented baseline.  SUSET so a superuser can toggle it at
	 * runtime for a measurement window without a restart.
	 */
	DefineCustomBoolVariable("cluster.xnode_profile",
							 gettext_noop("Enable cross-node performance profiling buckets."),
							 gettext_noop("Off keeps all instrumented paths at zero overhead."),
							 &cluster_xnode_profile_enabled, false, PGC_SUSET, 0, NULL, NULL, NULL);

	/*
	 * spec-6.4 ADG role and read-only service knobs.  Defaults keep existing
	 * primary behavior byte-identical: ADG is off and no standby apply process
	 * starts unless role=standby and enable_adg=on at postmaster start.
	 */
	DefineCustomEnumVariable(
		"cluster.dg_role", gettext_noop("Cluster Data Guard role for this instance."),
		gettext_noop("primary is the normal writer role; standby enables ADG standby "
					 "startup paths when cluster.enable_adg is on."),
		&cluster_dg_role, CLUSTER_DG_ROLE_PRIMARY, cluster_dg_role_options, PGC_POSTMASTER, 0, NULL,
		NULL, NULL);

	DefineCustomEnumVariable(
		"cluster.dg_mode", gettext_noop("Cluster Data Guard shipping acknowledgement mode."),
		gettext_noop("async does not wait for standby acknowledgement; sync waits for at "
					 "least one standby; max_availability may degrade to async when no "
					 "standby is reachable."),
		&cluster_dg_mode, CLUSTER_DG_MODE_ASYNC, cluster_dg_mode_options, PGC_SIGHUP, 0, NULL, NULL,
		NULL);

	DefineCustomBoolVariable(
		"cluster.enable_adg", gettext_noop("Enable ADG standby apply and read-only service."),
		gettext_noop("Only a standby role with this setting on starts the ADG MRP path."),
		&cluster_enable_adg, false, PGC_POSTMASTER, 0, NULL, NULL, NULL);

	/*
	 * PGC_POSTMASTER on purpose: flipping election off at runtime would let
	 * every standby in a multi-node cluster appoint itself Apply Master on
	 * the next reload, so the off mode is a start-time, single-node-only
	 * decision (MRP refuses to start with it off when peers are declared).
	 */
	DefineCustomBoolVariable(
		"cluster.apply_master_election",
		gettext_noop("Enable automatic ADG Apply Master election."),
		gettext_noop("When on, the standby cluster elects one Apply Master using a voting-disk "
					 "majority and the durable apply-master term lease."),
		&cluster_apply_master_election, true, PGC_POSTMASTER, 0, NULL, NULL, NULL);

	DefineCustomStringVariable(
		"cluster.adg_rfs_conninfos", gettext_noop("ADG RFS upstream connection strings."),
		gettext_noop("Semicolon-separated libpq connection strings used by the standby RFS "
					 "coordinator to receive per-thread WAL from primary instances.  Each entry "
					 "may start with \"thread_id=N\" followed by whitespace before the libpq "
					 "connection string; entries without a prefix use their 1-based order."),
		&cluster_adg_rfs_conninfos, "", PGC_POSTMASTER, GUC_NOT_IN_SAMPLE, NULL, NULL, NULL);

	DefineCustomIntVariable(
		"cluster.adg_primary_thread_count", gettext_noop("Primary ADG WAL thread count."),
		gettext_noop("Read-only replication protocol surface for ADG standby thread discovery."),
		&cluster_adg_primary_thread_count, 0, 0, CLUSTER_WAL_THREAD_MAX, PGC_INTERNAL,
		GUC_NOT_IN_SAMPLE | GUC_NO_SHOW_ALL, NULL, NULL, cluster_show_adg_primary_thread_count);

	DefineCustomIntVariable(
		"cluster.adg_lag_threshold_sec",
		gettext_noop("ADG apply lag threshold for read-only service errors."),
		gettext_noop("Standby reads fail with cluster_adg_apply_lag_excessive when the "
					 "tracked lag exceeds this threshold."),
		&cluster_adg_lag_threshold_sec, 10, 1, 300, PGC_SIGHUP, GUC_UNIT_S, NULL, NULL, NULL);

	DefineCustomIntVariable(
		"cluster.max_standby_delay",
		gettext_noop("Maximum delay before ADG read-only queries yield to apply."),
		gettext_noop("-1 disables the delay limit; otherwise long standby reads that block "
					 "apply are cancelled after this many seconds."),
		&cluster_max_standby_delay, 30, -1, 86400, PGC_SIGHUP, GUC_UNIT_S, NULL, NULL, NULL);

	DefineCustomIntVariable(
		"cluster.apply_master_switch_drain_ms",
		gettext_noop("ADG Apply Master switch drain window."),
		gettext_noop("In-flight standby reads are given this drain window during Apply "
					 "Master failover before stricter conflict handling applies."),
		&cluster_apply_master_switch_drain_ms, 5000, 0, 600000, PGC_SIGHUP, GUC_UNIT_MS, NULL, NULL,
		NULL);

	DefineCustomIntVariable(
		"cluster.adg_lease_takeover_grace_ms",
		gettext_noop("Grace period past apply-master lease expiry before takeover."),
		gettext_noop("A standby may only take over an expired apply-master lease this long "
					 "after its expiry; the deposed master stops shared-storage writes at "
					 "most halfway through it, and the other half absorbs clock skew."),
		&cluster_adg_lease_takeover_grace_ms, 5000, 0, 600000, PGC_SIGHUP, GUC_UNIT_MS, NULL, NULL,
		NULL);

	DefineCustomIntVariable(
		"cluster.adg_barrier_interval_ms", gettext_noop("ADG consistency barrier interval."),
		gettext_noop("Primary walwriter emits periodic thread-safe SCN barriers at this "
					 "interval while ADG is enabled; 0 disables the heartbeat and leaves "
					 "only commit-driven barriers."),
		&cluster_adg_barrier_interval_ms, 1000, 0, 300000, PGC_SIGHUP, GUC_UNIT_MS, NULL, NULL,
		NULL);

	DefineCustomIntVariable(
		"cluster.wal_sender_timeout_sec", gettext_noop("ADG LNS WAL sender timeout."),
		gettext_noop("Primary-side per-thread ADG shipping times out after this many seconds."),
		&cluster_wal_sender_timeout_sec, 60, 1, 3600, PGC_SIGHUP, GUC_UNIT_S, NULL, NULL, NULL);

	DefineCustomIntVariable(
		"cluster.wal_receiver_timeout_sec", gettext_noop("ADG RFS WAL receiver timeout."),
		gettext_noop("Standby-side per-thread ADG receive waits time out after this many seconds."),
		&cluster_wal_receiver_timeout_sec, 60, 1, 3600, PGC_SIGHUP, GUC_UNIT_S, NULL, NULL, NULL);

	/*
	 * cluster.page_scn_shortcut -- spec-6.12 wave c (read layer 3).  When
	 * on, the cluster visibility resolver memoizes TERMINAL remote
	 * transaction outcomes (exact TT key, per top-level transaction) and
	 * replays them instead of repeating the TT overlay lookup for every
	 * tuple.  Terminal outcomes are immutable, so the memo never answers
	 * anything the same transaction's authoritative lookup did not
	 * already answer.  Default OFF: resolver behaviour byte-identical to
	 * the 5.59 baseline.  SUSET for measurement-window toggling (same
	 * rationale as cluster.xnode_profile).
	 */
	DefineCustomBoolVariable(
		"cluster.page_scn_shortcut",
		gettext_noop("Enable the cross-node visibility resolver terminal-outcome memo."),
		gettext_noop("Off keeps the per-tuple TT lookup path byte-identical."),
		&cluster_page_scn_shortcut, false, PGC_SUSET, 0, NULL, NULL, NULL);

	/*
	 * cluster.read_scache -- spec-6.12 wave a (read layer 1).  When on, a
	 * master==holder node serving a cross-node read of a QUIESCENT block
	 * (no active ITL) flushes the page storage-current, downgrades its own
	 * PCM X to S and grants the requester a durable S copy instead of a
	 * one-shot read image -- repeat reads then hit the requester's local S
	 * buffer with zero wire traffic (LockBuffer covered-mode fast path).
	 * The pre-existing S invalidate-before-X machinery provides the real
	 * invalidation path; S also becomes a revoked-write state at the ITL
	 * forward-write gate.  Default OFF: every cross-node read of an X-held
	 * block keeps the one-shot read-image behaviour (5.59 baseline).
	 * SIGHUP: the decision runs in the LMON serve path, not per-session.
	 */
	DefineCustomBoolVariable(
		"cluster.read_scache", gettext_noop("Enable quiescent-block S-caching via X->S downgrade."),
		gettext_noop("Off keeps one-shot read-image shipping for X-held blocks."),
		&cluster_read_scache, false, PGC_SIGHUP, 0, NULL, NULL, NULL);

	/*
	 * cluster.ges_handoff -- spec-6.12 wave e1.  The deterministic
	 * release-side drain (spec-5.3 D3) already grants the next compatible
	 * convert/waiter in a single pass; this switch arms the 8.A-dual
	 * verifier (no-double-grant / no-stale-holder / no-lost-waiter) over
	 * every drain and the e1_* counters in the xnode_lever dump category,
	 * so an invariant break surfaces as a counter + LOG.  Default OFF: the
	 * drain path is byte-identical (verify skipped).  SUSET for a
	 * measurement / chaos window without a restart.
	 */
	DefineCustomBoolVariable(
		"cluster.ges_handoff",
		gettext_noop("Verify the GES release-side deterministic handoff invariants."),
		gettext_noop("Off keeps the drain path byte-identical (no verify, no counters)."),
		&cluster_ges_handoff, false, PGC_SUSET, 0, NULL, NULL, NULL);

	/*
	 * cluster.crossnode_cr_data_plane -- spec-6.12 wave b (read layer 2).
	 * When on, a CR construction whose newest candidate chain lives in a
	 * REMOTE instance's undo (the spec-5.57 class-3 boundary that
	 * otherwise fails closed 53R9G) asks that origin's CR-server for the
	 * result: the origin's LMS constructs from its own current block +
	 * local undo/TT and ships one CR page back (full, or a
	 * write_scn-DESC-prefix partial the requester finishes locally).
	 * Any uncertainty on either side -- origin cannot complete, chains
	 * interleave across homes, timeout, checksum, GUC off on the origin
	 * -- keeps the unchanged 53R9G fail-closed (Rule 8.A; the CR result
	 * is never installed as current and never flushed).  Default OFF:
	 * cross-instance CR keeps the 5.57 fail-closed boundary
	 * byte-identical.  SUSET for measurement-window toggling.
	 */
	DefineCustomBoolVariable(
		"cluster.crossnode_cr_data_plane",
		gettext_noop("Enable the cross-instance CR-server data plane (spec-6.12b)."),
		gettext_noop("Off keeps cross-instance CR fail-closed (SQLSTATE 53R9G)."),
		&cluster_crossnode_cr_data_plane, false, PGC_SUSET, 0, NULL, NULL, NULL);

	/*
	 * cluster.crossnode_runtime_visibility -- spec-6.12 wave i (缺口 A).
	 * When on, the by-xid durable-TT resolution of a RECYCLED remote ITL
	 * slot — today admitted only behind the crash-recovery materialized
	 * marker — may also be admitted in ACTIVE runtime, but ONLY behind the
	 * D-i2 live authority gate: the origin's LMS co-samples {origin_epoch,
	 * live_hwm_lsn, tt_generation} into the very undo-block reply that
	 * carries its TT (D-i1 fetch, riding the 6.12b LMS ship + GCS block
	 * wire), and the requester resolves only when that authority provably
	 * covers the tuple's page version in the current membership epoch.
	 * Proof-insufficient / epoch-changed / fetch-failed keeps the unchanged
	 * 53R97 fail-closed (Rule 8.A: this wave only widens "resolve when
	 * provable", never "resolve when unprovable").  Default OFF: the
	 * materialized-marker-only boundary stays byte-identical.  SUSET for
	 * measurement-window toggling.
	 */
	DefineCustomBoolVariable(
		"cluster.crossnode_runtime_visibility",
		gettext_noop("Enable active-runtime cross-instance recycled-slot visibility "
					 "resolution (spec-6.12i)."),
		gettext_noop("Off keeps active-runtime recycled-slot resolution fail-closed "
					 "(SQLSTATE 53R97)."),
		&cluster_crossnode_runtime_visibility, false, PGC_SUSET, 0, NULL, NULL, NULL);

	/*
	 * cluster.xid_striping -- spec-6.15 D1 (AD-012 exception 10 xid
	 * space segmentation).  When on, this node only issues 32-bit xids
	 * congruent to its declared node slot modulo 16, making the xid
	 * value space globally unique across cluster nodes.  Requires
	 * cluster.node_id in [0, 15]; boot fails closed otherwise (see
	 * cluster_init_shmem).  POSTMASTER context: activation is a
	 * whole-node restart decision, never a runtime flip.
	 */
	DefineCustomBoolVariable(
		"cluster.xid_striping",
		gettext_noop("Stripe xid allocation into per-node congruence classes (spec-6.15)."),
		gettext_noop("Each declared node only issues xids congruent to its node slot "
					 "modulo 16, making xid values self-describing about their origin "
					 "node. Requires cluster.node_id between 0 and 15."),
		&cluster_xid_striping, false, PGC_POSTMASTER, 0, NULL, NULL, NULL);

	/*
	 * cluster.xid_herding_slack -- spec-6.15 D5/D3.  Allowed xid-value
	 * gap between the fastest and slowest stripe slots before a lagging
	 * node observe-and-jumps its allocator forward (D3), and the extra
	 * headroom added above the shared nextXid high watermark when the
	 * activation floor is seeded (D5b).  SIGHUP: herding cadence is a
	 * runtime tuning knob; the seeded floor uses the value in effect at
	 * activation time only.
	 */
	DefineCustomIntVariable(
		"cluster.xid_herding_slack",
		gettext_noop("Allowed xid gap between stripe slots before herding jumps (spec-6.15)."),
		gettext_noop("Lagging nodes jump their striped xid allocator forward once the "
					 "cluster-wide watermark leads by more than this many xid values; "
					 "the hard fail-closed refusal limit is 64x this value."),
		&cluster_xid_herding_slack, 4194304, 65536, 268435456, PGC_SIGHUP, 0, NULL, NULL, NULL);

	/*
	 * cluster.block_self_contained -- spec-6.12 wave g (write-write
	 * collapse root).  When on, a block may be X-transferred across
	 * instances WITH an uncommitted (ACTIVE) ITL slot -- the spec-5.2 D11
	 * deferral that kept a block pinned to its holder until the holder's
	 * transaction went terminal is lifted, so a same-block DIFFERENT-row
	 * writer no longer waits on the holder's unrelated row.  The holder's
	 * later commit stamps the ITL slot only if the block is still resident
	 * (opportunistic cleanout); a drifted ACTIVE slot is left unstamped and
	 * every reader resolves its committed-ness through the TT authority
	 * (ITL->UBA->TT, AD-006), exactly as for any remote ITL ref.  SAME-row
	 * conflicts still serialize through the cross-node TX enqueue wait
	 * (spec-5.2 D4/D5, t/280).  8.A: the TT is the SOLE post-stamp-skip
	 * commit_scn authority; any UNKNOWN resolution fails closed (53R97 /
	 * 53R9G), never visible.  Default OFF: the D11 deferral is
	 * byte-identical.  SUSET for a measurement / chaos window.
	 */
	DefineCustomBoolVariable(
		"cluster.block_self_contained",
		gettext_noop("Allow active-ITL block migration + opportunistic commit cleanout "
					 "(spec-6.12g)."),
		gettext_noop("Off keeps the spec-5.2 D11 writer-transfer deferral for active-ITL blocks."),
		&cluster_block_self_contained, false, PGC_SUSET, 0, NULL, NULL, NULL);

	/*
	 * cluster.ges_bast -- spec-6.12 wave e2 (㉔ form).  When the master
	 * must DENY an X request because ANOTHER live node holds the block in
	 * X (the HG7 conservative deny), it additionally sends the holder a
	 * fire-and-forget BAST nudge: the holder's LMON tries the quiescent
	 * X->S self-downgrade right away instead of waiting for a natural
	 * release, so the requester's bounded retry can proceed through the
	 * S-invalidate grant path (Oracle BAST -> holder LMS background
	 * yield; the foreground session is never interrupted).  The nudge is
	 * advisory: any refusal (active ITL, pinned, raced) leaves the
	 * deny-retry (e1 release-side) path untouched.  Default OFF =
	 * byte-identical deny behaviour.  SUSET for measurement windows.
	 */
	DefineCustomBoolVariable(
		"cluster.ges_bast",
		gettext_noop("Send a BAST nudge to a live X holder blocking a peer writer "
					 "(spec-6.12e2)."),
		gettext_noop("Off keeps the deny-and-retry path without nudging the holder."),
		&cluster_ges_bast, false, PGC_SUSET, 0, NULL, NULL, NULL);

	/*
	 * cluster.past_image -- spec-6.12 wave h (AD-002 PI orthogonal state).
	 * When a block leaves this node (X-transfer ship+drop, S-copy
	 * invalidate), keep the local copy as a Past Image instead of dropping
	 * it: buffer_type = BUF_TYPE_PI with BM_VALID and every dirty flag
	 * cleared, so native bufmgr semantics guarantee the hard invariant "a
	 * PI never serves a query" (a !VALID buffer is an IO-needed miss) and
	 * a later local read of the same block re-reads/installs over the
	 * bytes -- the implicit discard.  PIs are fail-safe: dropping one at
	 * any moment only loses the crash-recovery shortcut (D-h3), never
	 * correctness (storage + full redo remain).  Default OFF = today's
	 * flush + drop.  SUSET for measurement windows.
	 */
	DefineCustomBoolVariable(
		"cluster.past_image",
		gettext_noop("Keep a Past Image copy when a block is transferred or invalidated "
					 "(spec-6.12h)."),
		gettext_noop("Off drops the local copy after WAL flush (no recovery shortcut)."),
		&cluster_past_image, false, PGC_SUSET, 0, NULL, NULL, NULL);

	/*
	 * cluster.space_affinity -- spec-6.12 wave d (static).  static makes
	 * the cluster relation-extend path over-ask the HW master
	 * (max(need, cluster.space_lease_blocks)), zero-extend the whole
	 * grant (file stays dense) and park the unconsumed tail as this
	 * node's private lease, consumed block-at-a-time before the shared
	 * FSM (Q17-A extent-interior per-instance grouping; cuts HW master
	 * round-trips by the lease factor).  dynamic is rejected until the
	 * spec-6.3 DRM machinery ships (rule 8 explicit rejection).  Default
	 * OFF: extend path byte-identical.
	 */
	DefineCustomEnumVariable(
		"cluster.space_affinity",
		gettext_noop("Instance space-affinity mode for cluster relation extends."),
		gettext_noop("off = plain authority extends; static = per-node HW space leases."),
		&cluster_space_affinity, CLUSTER_SPACE_AFFINITY_OFF, cluster_space_affinity_options,
		PGC_SIGHUP, 0, check_cluster_space_affinity, NULL, NULL);

	/*
	 * cluster.space_lease_blocks -- spec-6.12 wave d.  Per-grant lease
	 * cap; the transient bloat upper bound is
	 * sum over active (relation, fork) leases of lease_blocks x nodes
	 * (spec-6.12 v0.4 amendment 9), which the D0 bloat gate consumes.
	 */
	DefineCustomIntVariable(
		"cluster.space_lease_blocks", gettext_noop("Blocks handed to a node per HW space lease."),
		gettext_noop("Caps per-lease transient bloat (unconsumed zero pages)."),
		&cluster_space_lease_blocks, 64, 1, 8192, PGC_SIGHUP, 0, NULL, NULL, NULL);

	/*
	 * cluster.clean_leave_enabled -- opt-in cooperative clean-leave
	 * reconfiguration (spec-5.13).  Default OFF (5.6 opt-in paradigm): a
	 * surviving node only drains-and-leaves on plan when this is on cluster-
	 * wide.  Off keeps today's behaviour byte-identical (no clean-leave path;
	 * a departure is handled as a crash via fail-stop reconfig).
	 */
	DefineCustomBoolVariable(
		"cluster.clean_leave_enabled",
		gettext_noop("Enable cooperative clean-leave reconfiguration."),
		gettext_noop("Off treats a node departure as a crash (fail-stop reconfig)."),
		&cluster_clean_leave_enabled, false, PGC_POSTMASTER, 0, NULL, NULL, NULL);

	/*
	 * cluster.clean_leave_drain_timeout_ms -- fail-closed deadline for a clean
	 * leave's cooperative drain (spec-5.13 Q9).  If the drain + all-survivor
	 * barrier do not complete within this window the leave escalates to fail-
	 * stop (clean leave never weakens fail-stop safety, CL-I7).  Aligns with
	 * the feature-082 30s reconfig barrier.
	 */
	DefineCustomIntVariable(
		"cluster.clean_leave_drain_timeout_ms",
		gettext_noop("Fail-closed deadline for a clean leave's cooperative drain."),
		gettext_noop("If the drain + barrier exceed this, the leave escalates to fail-stop."),
		&cluster_clean_leave_drain_timeout_ms, 30000, 1000, 600000, PGC_POSTMASTER, GUC_UNIT_MS,
		NULL, NULL, NULL);

	/*
	 * cluster.recovery_merge_wait_timeout -- how long the merge
	 * coordinator waits for the spec-4.4 stream-validation workers
	 * before falling back to inline re-validation (spec-4.5 Q6).
	 */
	DefineCustomIntVariable(
		"cluster.recovery_merge_wait_timeout",
		gettext_noop("Time to wait for stream-validation workers before merged recovery."),
		gettext_noop("After this, the coordinator re-validates candidate streams inline."),
		&cluster_recovery_merge_wait_timeout, 10000, 0, 600000, PGC_POSTMASTER, GUC_UNIT_MS, NULL,
		NULL, NULL);

	DefineCustomStringVariable(
		"cluster.recovery_target_scn", gettext_noop("Cluster PITR target SCN."),
		gettext_noop("When set, cluster PITR status resolves the requested SCN against "
					 "cluster restore points and refuses unreachable targets."),
		&cluster_recovery_target_scn, "", PGC_POSTMASTER, 0, NULL, NULL, NULL);

	DefineCustomStringVariable(
		"cluster.recovery_target_cluster_time", gettext_noop("Cluster PITR target timestamp."),
		gettext_noop("During archive recovery of a cluster backup set, the target "
					 "snaps to the latest manifest restore point not later than "
					 "the requested timestamp."),
		&cluster_recovery_target_cluster_time, "", PGC_POSTMASTER, 0, NULL, NULL, NULL);

	DefineCustomStringVariable(
		"cluster.recovery_target_name", gettext_noop("Cluster PITR named restore point target."),
		gettext_noop("During archive recovery of a cluster backup set, the named target "
					 "must match a restore point recorded in the backup manifest."),
		&cluster_recovery_target_name, "", PGC_POSTMASTER, 0, NULL, NULL, NULL);

	DefineCustomEnumVariable(
		"cluster.recovery_target_action",
		gettext_noop("Action to take when a cluster PITR target is reached."),
		gettext_noop("Accepted values are pause, promote, and shutdown.  The setting is "
					 "honored after startup recovery reaches the resolved cluster "
					 "restore point."),
		&cluster_recovery_target_action, CLUSTER_RECOVERY_TARGET_ACTION_PAUSE,
		cluster_recovery_target_action_options, PGC_POSTMASTER, 0, NULL, NULL, NULL);

	DefineCustomBoolVariable(
		"cluster.enable_pitr_restore_points",
		gettext_noop("Enable automatic cluster restore point creation."),
		gettext_noop("Manual pg_cluster_create_restore_point is available regardless of "
					 "this setting.  Automatic background creation is enabled only "
					 "when no declared peers are present."),
		&cluster_enable_pitr_restore_points, false, PGC_SIGHUP, 0, NULL, NULL, NULL);

	DefineCustomIntVariable("cluster.pitr_restore_point_interval_ms",
							gettext_noop("Interval for automatic cluster PITR restore points."),
							gettext_noop("Zero disables automatic restore point scheduling."),
							&cluster_pitr_restore_point_interval_ms, 0, 0, 86400000, PGC_SIGHUP,
							GUC_UNIT_MS, NULL, NULL, NULL);

	DefineCustomIntVariable(
		"cluster.restore_point_drain_timeout_ms",
		gettext_noop("Timeout for cluster restore-point commit drain."),
		gettext_noop("A restore-point fence fails closed if in-flight commits do not drain "
					 "within this timeout."),
		&cluster_restore_point_drain_timeout_ms, 30000, 1, 600000, PGC_SUSET, GUC_UNIT_MS, NULL,
		NULL, NULL);

	DefineCustomIntVariable(
		"cluster.backup_wal_retention",
		gettext_noop("Cluster backup WAL retention hint in megabytes."),
		gettext_noop("The 6.5 manifest/status surface reports the setting.  Durable "
					 "backup pins still retain WAL from the backup start REDO point."),
		&cluster_backup_wal_retention, 0, 0, INT_MAX, PGC_SIGHUP, GUC_UNIT_MB, NULL, NULL, NULL);

	DefineCustomIntVariable(
		"cluster.backup_parallel_channels", gettext_noop("Maximum cluster backup copy channels."),
		gettext_noop("Maximum copy-channel capacity reported by the cluster backup surface."),
		&cluster_backup_parallel_channels, 1, 1, CLUSTER_MAX_NODES, PGC_SIGHUP, 0, NULL, NULL,
		NULL);

	DefineCustomEnumVariable(
		"cluster.backup_manifest_checksums",
		gettext_noop("Checksum mode for cluster backup manifests."),
		gettext_noop("crc32c protects the in-memory and SQL-visible manifest substrate; "
					 "6.5 does not provide an unchecked manifest mode."),
		&cluster_backup_manifest_checksums, CLUSTER_BACKUP_MANIFEST_CHECKSUM_CRC32C,
		cluster_backup_manifest_checksum_options, PGC_SIGHUP, 0, NULL, NULL, NULL);

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
	 * cluster.write_fence_enforcement -- spec-4.12 cooperative write-fence (split-
	 * brain recovery guard).  ON makes every shared-storage write consult the local
	 * write-fence token (stale epoch / expired lease / self-fenced -> 53R51 / PANIC).
	 * spec-4.12b D4: ships default ON now that the baseline-marker subsystem keeps a
	 * healthy cluster's authority fresh; a single node / no-voting-disk deployment
	 * auto-degrades to a no-op at runtime (cluster_write_fence_enforcing()).
	 * PGC_POSTMASTER so the mode is fixed for the postmaster lifetime.
	 */
	DefineCustomEnumVariable(
		"cluster.write_fence_enforcement",
		gettext_noop("Cooperative write-fence enforcement mode."),
		gettext_noop("on (default) rejects shared-storage writes from a "
					 "stale / lease-expired / self-fenced node (auto-degrades "
					 "to a no-op with no voting disks); off is a no-op; dev is "
					 "the single-node / test escape hatch."),
		&cluster_write_fence_enforcement, CLUSTER_WRITE_FENCE_ENFORCE_ON,
		cluster_write_fence_enforcement_options, PGC_POSTMASTER, 0, NULL, NULL, NULL);

	/*
	 * cluster.write_fence_lease_ms -- spec-4.12 the local token's lease duration.
	 * qvotec refreshes it every poll from the durable marker; once it expires (the
	 * node failed to refresh = partition) the hot write path fails closed.  Default
	 * 6000 ms ~= 3x the default qvotec poll interval.  SIGHUP-tunable.
	 */
	DefineCustomIntVariable(
		"cluster.write_fence_lease_ms",
		gettext_noop("Cooperative write-fence token lease duration (milliseconds)."),
		gettext_noop("After this long without a qvotec refresh from the durable marker, the "
					 "local write-fence token expires and shared-storage writes fail closed."),
		&cluster_write_fence_lease_ms, 6000, 1000, 600000, PGC_SIGHUP, GUC_UNIT_MS, NULL, NULL,
		NULL);

	/*
	 * cluster.shared_data_dir -- shared data root for the cluster_fs
	 * (shared_fs) backend (spec-4.5a D2).  The shared_fs backend resolves
	 * every relation file under <shared_data_dir>/<relpathperm> so that
	 * all nodes pointing at the same shared mount land on the same file.
	 * Required (non-empty, absolute) when shared_storage_backend=cluster_fs;
	 * the startup cross-check lives in cluster_shared_fs_init.  Default
	 * empty keeps the GUC inert for stub/local deployments.
	 */
	DefineCustomStringVariable(
		"cluster.shared_data_dir",
		gettext_noop("Shared data root for the cluster_fs shared-storage backend."),
		gettext_noop("Absolute path on the shared mount that every node points at.  "
					 "The shared_fs backend stores each relation file under "
					 "<shared_data_dir>/<relpath>.  Required when "
					 "shared_storage_backend=cluster_fs; ignored otherwise."),
		&cluster_shared_data_dir, "",
		PGC_POSTMASTER,				   /* path is frozen for the postmaster lifetime */
		0,							   /* flags */
		check_cluster_shared_data_dir, /* check_hook */
		NULL,						   /* assign_hook */
		NULL);						   /* show_hook */

	/*
	 * cluster.controlfile_shared_authority -- spec-5.6 Da3 opt-in switch for
	 * the shared pg_control authority.  Default off (Hardening v1.0.1): when
	 * on, this node's global/pg_control is migrated into the single shared
	 * authority under shared_data_dir and replaced by a symlink, and startup
	 * fail-closes if the local path is not that symlink or the identity does
	 * not match.  Off keeps the stock per-node control file.
	 */
	DefineCustomBoolVariable(
		"cluster.controlfile_shared_authority",
		gettext_noop("Use a single shared pg_control authority under cluster.shared_data_dir."),
		gettext_noop("When on, this node migrates global/pg_control into the shared "
					 "authority and symlinks to it, failing closed on a per-node or "
					 "foreign control file.  Off keeps the stock per-node pg_control."),
		&cluster_controlfile_shared_authority, false, PGC_POSTMASTER, 0, NULL, NULL, NULL);

	/*
	 * cluster.shared_storage_uuid -- optional external-preset identity for
	 * the shared root (spec-4.5a D2).  When set, every node must agree on
	 * it and it is matched against the value recorded in the shared-root
	 * sentinel (pgrac_shared.control); a mismatch is FATAL, catching two
	 * nodes that point at different shared roots.  When empty, the first
	 * node to attach generates and records a random uuid.
	 */
	DefineCustomStringVariable(
		"cluster.shared_storage_uuid",
		gettext_noop("Optional external identity for the cluster_fs shared root."),
		gettext_noop("When set, all nodes must agree on this value and it is matched "
					 "against the shared-root sentinel; a mismatch is FATAL.  When "
					 "empty, the first node generates a random uuid."),
		&cluster_shared_storage_uuid, "",
		PGC_POSTMASTER, /* identity is frozen for the postmaster lifetime */
		0,				/* flags */
		NULL,			/* check_hook */
		NULL,			/* assign_hook */
		NULL);			/* show_hook */

	DefineCustomStringVariable(
		"cluster.block_device_path",
		gettext_noop("Raw block-device path for the block_device shared-storage backend."),
		gettext_noop(
			"Absolute device or file path used by cluster.shared_storage_backend=block_device.  "
			"The backend stores raw layout metadata and relation extents directly in this device."),
		&cluster_block_device_path, "", PGC_POSTMASTER, 0, check_cluster_block_device_path, NULL,
		NULL);

	DefineCustomBoolVariable(
		"cluster.block_device_use_odirect",
		gettext_noop("Require direct I/O for the raw block-device backend."),
		gettext_noop(
			"When on, the block_device backend opens cluster.block_device_path with PG_O_DIRECT "
			"and fails closed if that cannot be honored."),
		&cluster_block_device_use_odirect, true, PGC_POSTMASTER, 0, NULL, NULL, NULL);

	DefineCustomEnumVariable(
		"cluster.storage_fence_driver", gettext_noop("Shared-storage fencing driver selection."),
		gettext_noop(
			"auto detects available fencing support; scsi3_pr requires SCSI-3 persistent "
			"reservation capability and fails closed if unavailable; disabled reports no fence."),
		&cluster_storage_fence_driver, CLUSTER_STORAGE_FENCE_DRIVER_AUTO,
		cluster_storage_fence_driver_options, PGC_POSTMASTER, 0, NULL, NULL, NULL);

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
	 * cluster.shared_catalog -- spec-6.14 D1 master switch for the shared
	 * system-catalog single authority.  Off keeps the existing per-node
	 * catalog copies (byte-identical to the current cluster behaviour); on
	 * routes catalog storage into the single shared tree with cross-node
	 * cache coherency (Cache Fusion) and a shared relmapper + OID authority.
	 * On requires cluster.smgr_user_relations=on + cluster.shared_data_dir +
	 * cluster.controlfile_shared_authority=on; the startup vet in
	 * cluster_shared_fs_init FATALs otherwise, and every node must agree
	 * (join-time consistency check, fail-closed).  PGC_POSTMASTER: routing
	 * and bootstrap mode are fixed at startup.
	 */
	DefineCustomBoolVariable(
		"cluster.shared_catalog",
		gettext_noop("Route system catalogs through a single shared authority."),
		gettext_noop("When on (combined with cluster.smgr_user_relations=on, "
					 "cluster.shared_data_dir set and "
					 "cluster.controlfile_shared_authority=on), all permanent "
					 "relations -- catalogs included -- live in one shared tree "
					 "kept coherent across nodes, and DDL on one node becomes "
					 "visible on the others.  Off keeps per-node catalog copies. "
					 "All nodes in a cluster must set the same value."),
		&cluster_shared_catalog, false,
		PGC_POSTMASTER, /* routing + bootstrap mode fixed at startup */
		0,				/* flags */
		NULL,			/* check_hook */
		NULL,			/* assign_hook */
		NULL);			/* show_hook */

	/*
	 * cluster.oid_lease_size -- spec-6.14 D6.  Under shared_catalog=on a node
	 * leases this many OIDs at a time from the shared durable OID authority
	 * and consumes them node-locally before refilling.  Larger = fewer
	 * cross-node refills; the cost of a larger lease is a bigger OID segment
	 * wasted when a node crashes mid-lease (harmless -- the OID space wraps
	 * around and GetNewOidWithIndex re-checks uniqueness).  No effect when
	 * shared_catalog=off.
	 */
	DefineCustomIntVariable(
		"cluster.oid_lease_size",
		gettext_noop("Number of OIDs a node leases at a time from the shared OID authority."),
		gettext_noop("Range [1024, 1048576].  Default 8192.  Only meaningful when "
					 "cluster.shared_catalog=on.  Larger values reduce cross-node "
					 "OID-refill round trips at the cost of a larger OID segment "
					 "wasted on a mid-lease node crash (harmless: the OID space "
					 "wraps and uniqueness is re-checked at index insert)."),
		&cluster_oid_lease_size, 8192, 1024, 1048576,
		PGC_POSTMASTER, /* lease pool sized at startup */
		0,				/* flags */
		NULL,			/* check_hook */
		NULL,			/* assign_hook */
		NULL);			/* show_hook */

	/*
	 * cluster.undo_retention_horizon_enabled (spec-3.12 D5).  Own-instance
	 * retention gate for TT slots / undo segments; default on (correctness
	 * benefit -- retires spec-3.11 L4).  PGC_SIGHUP so it can be flipped off
	 * at runtime for debugging / rollback to spec-3.11 immediate recycle.
	 */
	DefineCustomBoolVariable(
		"cluster.undo_retention_horizon_enabled",
		gettext_noop("Retain committed undo / TT slots until no live reader needs them."),
		gettext_noop("When on, the TT-slot and undo-segment allocators keep a COMMITTED "
					 "slot/segment alive while a live snapshot's read_scn is at or below its "
					 "commit_scn (own-instance retention horizon).  Off recycles COMMITTED / "
					 "ABORTED state immediately (spec-3.11 behavior); a reader that then needs "
					 "the recycled pre-image fails with 53R9F."),
		&cluster_undo_retention_horizon_enabled, true, PGC_SIGHUP, 0, /* flags */
		NULL,														  /* check_hook */
		NULL,														  /* assign_hook */
		NULL);														  /* show_hook */

	/*
	 * cluster.shmem_max_regions (spec-1.3): capacity of the cluster shmem
	 * region registry.  Default 80 (spec-5.56: raised 64 -> 80 when the
	 * per-relation CR generation region took the live count to 65 with the
	 * visibility-inject region compiled in; restores a wide safety margin) covers
	 * the current baseline plus the reserved regions planned in
	 * cluster-shmem-design.md §3.2.  Range [40, 256] in production builds -- 40 is the
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
							&cluster_shmem_max_regions, 80, CLUSTER_SHMEM_MIN_REGIONS, 256,
							PGC_POSTMASTER, /* registry array is palloc'd once at init */
							0,				/* flags */
							NULL,			/* check_hook */
							NULL,			/* assign_hook */
							NULL);			/* show_hook */

	/* spec-3.18 D1: undo block buffer pool (AD-014 form restoration). */
	DefineCustomIntVariable(
		"cluster.undo_buffers", gettext_noop("Number of cluster undo block buffer pool slots."),
		gettext_noop("Each slot caches one 8KB undo DATA block (block 0 is "
					 "not poolable).  0 disables the pool (direct smgr I/O).  "
					 "Default 2048 = ~16MB per instance."),
		&cluster_undo_buffers, 2048, 0, 1048576, PGC_POSTMASTER, /* shmem sized once at init */
		0, NULL, NULL, NULL);

	DefineCustomBoolVariable(
		"cluster.undo_buffer_writeback",
		gettext_noop("Enable buffered write-back for the undo buffer pool."),
		gettext_noop("On = buffered undo, durable via WAL + checkpoint flush "
					 "(drops the per-commit undo fsync).  DEFAULT ON since "
					 "spec-3.25 D4 (auto semantics): the runtime guard forces "
					 "write-through whenever the node has peers, so the default "
					 "only takes effect on a single-node topology.  Off = "
					 "write-through (per-commit fsync to shared storage)."),
		&cluster_undo_buffer_writeback, true, PGC_SIGHUP, 0,
		cluster_undo_buffer_writeback_check_hook, NULL, NULL);

	/*
	 * spec-4.8ab D5: cluster.undo_writeback_boundary_check -- advisory layer of
	 * the checkpoint-writeback boundary contract (two-layer model, §3.1).  This
	 * GUC controls ONLY the advisory verdict accounting / extra CI invariant;
	 * the hard corruption fail-closed (53R9N / PANIC on WAL-before-data /
	 * checkpoint-coverage / evidence-eviction) is UNCONDITIONAL and is NOT gated
	 * by this GUC (8.A is never downgradable -- off only saves the advisory
	 * accounting cost, it does not disable the hard guards).  SIGHUP-tunable.
	 */
	DefineCustomEnumVariable(
		"cluster.undo_writeback_boundary_check",
		gettext_noop("Advisory layer of the undo checkpoint-writeback boundary contract."),
		gettext_noop("off skips advisory verdict accounting (the hard fail-closed boundary guards "
					 "still run); on (default) records HOLD_WAL / HOLD_EVIDENCE write-back verdict "
					 "counters; strict additionally raises an ERROR on a broken advisory invariant "
					 "for aggressive CI/test exposure.  This GUC never disables the hard 8.A "
					 "boundary guards."),
		&cluster_undo_writeback_boundary_check, CLUSTER_UNDO_WB_CHECK_ON,
		cluster_undo_writeback_boundary_check_options, PGC_SIGHUP, 0, NULL, NULL, NULL);

	/*
	 * spec-2.15:  cluster.grd_max_entries
	 *
	 *  Maximum number of cluster_grd entry table slots.  Default 1024 (spec-5.7
	 *  §3.1d: the GES substrate now has real relation-extend / lock callers, so a
	 *  cluster-enabled node must be able to coordinate out of the box -- the
	 *  spec-2.15 "flip when caller-side integration lands" note).  An explicit 0
	 *  selects skeleton mode: entry HTAB not allocated;
	 *  cluster_grd_entry_lookup_or_create() returns CLUSTER_GRD_ENTRY_NOT_READY
	 *  sentinel.  Non-zero enables ShmemInitHash
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
							gettext_noop("Default 1024 allocates the entry table so a cluster node "
										 "can coordinate out of the box.  An explicit 0 selects "
										 "skeleton mode: entry HTAB not allocated, "
										 "cluster_grd_entry_lookup_or_create() returns NOT_READY. "
										 "Note PG dynahash "
										 "HASH_PARTITION=4096 forces internal nbuckets >= 4096, "
										 "so even GUC=16 reserves ~3-5MB shmem via "
										 "hash_estimate_size(Max(GUC, 4096), entry_size). "
										 "Production 推荐 NBuffers × 2 (spec-2.16+ caller-side)."),
							&cluster_grd_max_entries, 1024, 0, 1048576,
							PGC_POSTMASTER, /* ShmemInitHash size fixed at init */
							0,				/* flags */
							NULL,			/* check_hook */
							NULL,			/* assign_hook */
							NULL);			/* show_hook */

	DefineCustomBoolVariable(
		"cluster.grd_entry_reclaim",
		gettext_noop("Enable safe cold reclaim for GRD resource entries."),
		gettext_noop(
			"When on, lookup pins are released with a copy-resid last-ref protocol and "
			"cold holderless entries are removed after shard-LWLock and entry-spinlock "
			"recheck.  Turning this off keeps the pin discipline but disables HASH_REMOVE, "
			"matching the legacy cap-only entry table shape."),
		&cluster_grd_entry_reclaim, true, PGC_SIGHUP, 0, NULL, NULL, NULL);

	DefineCustomIntVariable(
		"cluster.grd_entry_reclaim_max_per_sweep",
		gettext_noop("Maximum GRD cold entries reclaimed by one LMON sweep."),
		gettext_noop("Bounds the background cold-entry reclaim pass.  0 disables the sweep while "
					 "leaving eager last-unpin reclaim enabled."),
		&cluster_grd_entry_reclaim_max_per_sweep, 256, 0, 65536, PGC_SIGHUP, 0, NULL, NULL, NULL);

	/*
	 * spec-5.10:  cluster.ges_starvation_max_skips — bounded fairness threshold
	 * for GES enqueue lock-starvation protection.  A waiter jumped this many
	 * times by holder-compatible grants/converts is boosted to head-of-line; a
	 * later conflicting jumper is held behind it.  0 disables boosting (skips
	 * are still counted for observability).  Default 8 (CF HC117 magnitude).
	 */
	DefineCustomIntVariable(
		"cluster.ges_starvation_max_skips",
		gettext_noop("Skip count after which a starved GES waiter is boosted to head-of-line."),
		gettext_noop("A queued waiter jumped this many times by holder-compatible grants is "
					 "boosted; a later conflicting jumper is held behind it.  0 disables boosting "
					 "(skips are still counted for observability).  Default 8."),
		&cluster_ges_starvation_max_skips, 8, 0, 1000000, PGC_SUSET, 0, NULL, NULL, NULL);

	/*
	 * spec-5.10:  cluster.ges_starvation_protection — master switch for GES
	 * enqueue lock-starvation fairness.  Default on (mirrors spec-4.12b's
	 * default-on fence posture).  Turning it off flips a cluster-global shared
	 * flag so the next grant decision falls straight back to spec-5.1b; the LMON
	 * sweep then clears already-boosted waiters + retracts FAIRNESS_BARRIER
	 * edges (clean escape, P1#1).  PGC_SIGHUP — a cluster-global runtime toggle.
	 */
	DefineCustomBoolVariable(
		"cluster.ges_starvation_protection",
		gettext_noop("Enables GES enqueue lock-starvation fairness protection."),
		gettext_noop("On (default) boosts starved waiters to head-of-line and holds conflicting "
					 "jumpers behind them; off falls back to spec-5.1b compatible-with-holder "
					 "granting.  The off transition flips a cluster-global shared flag and the "
					 "LMON sweep clears already-boosted waiters."),
		&cluster_ges_starvation_protection, true, PGC_SIGHUP, 0, NULL,
		cluster_ges_starvation_protection_assign_hook, NULL);

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

	/*
	 * spec-5.6 Dc4b — CF (control-file) enqueue timeout.  Bounds the wait for a
	 * CF X/S grant before fail-closed (53R70).  Always finite (no 0/-1 special
	 * cases): a hung checkpoint waiting forever on CF would stall the cluster,
	 * so the CF acquire path passes this value straight through as the GES
	 * per-acquire timeout.
	 */
	DefineCustomIntVariable(
		"cluster.cf_enqueue_timeout_ms",
		gettext_noop("Timeout for acquiring the shared control-file (CF) enqueue (ms)."),
		gettext_noop("Range [1000, 600000].  Default 30000 (30s).  A checkpoint or "
					 "strong-consistency control-file read waits this long for the "
					 "cross-node CF X/S grant before failing closed (53R70).  Only "
					 "meaningful when cluster.controlfile_shared_authority is on."),
		&cluster_cf_enqueue_timeout_ms, 30000, 1000, 600000, PGC_SIGHUP, GUC_UNIT_MS, NULL, NULL,
		NULL);

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

	/* spec-5.3 D10 — TM cross-node convert tunables. */
	DefineCustomIntVariable(
		"cluster.ges_convert_timeout_ms",
		gettext_noop("Finite wait for a cross-node lock-conversion (convert) grant reply."),
		gettext_noop("Range [1000, 600000] (1s - 10min).  Default 30000.  A same-backend TM "
					 "table-lock upgrade (e.g. LOCK TABLE ... IN SHARE MODE then ALTER TABLE) "
					 "that conflicts with another node's holder waits this long for the holder "
					 "to release before failing closed with 53R70."),
		&cluster_ges_convert_timeout_ms, 30000, 1000, 600000, PGC_SIGHUP, GUC_UNIT_MS, NULL, NULL,
		NULL);

	DefineCustomEnumVariable(
		"cluster.tm_convert_mode",
		gettext_noop("How a same-backend TM table-lock upgrade is routed across nodes."),
		gettext_noop("'convert' (default) performs an Oracle DLM-style in-place lock conversion "
					 "(opcode-2 CONVERT;  the master upgrades the holder slot and rebinds its "
					 "request id).  'additive' keeps the PG additive model (a second holder "
					 "entry, self-excluded) as an escape hatch — no convert state machine, no "
					 "release-ownership transfer."),
		&cluster_tm_convert_mode, CLUSTER_TM_CONVERT_MODE_CONVERT, cluster_tm_convert_mode_options,
		PGC_SIGHUP, 0, NULL, NULL, NULL);

	/* spec-4.6 D4/D1 — failure-driven remaster tunables. */
	DefineCustomIntVariable(
		"cluster.grd_remaster_wait_ms",
		gettext_noop("Short wait on a GRD shard frozen by failure-driven remaster (ms)."),
		gettext_noop("Range [0, 60000].  Default 200.  A request landing on a shard in "
					 "FROZEN/REBUILDING phase waits up to this long (wait event "
					 "ClusterGrdShardRemaster) for the remaster to finish;  expiry raises "
					 "53R9I cluster_grd_shard_remastering (fail-closed, application retries)."),
		&cluster_grd_remaster_wait_ms, 200, 0, 60000, PGC_SIGHUP, GUC_UNIT_MS, NULL, NULL, NULL);
	DefineCustomIntVariable(
		"cluster.grd_rebuild_timeout_ms",
		gettext_noop("Holder-rebuild barrier deadline after a failure-driven remaster (ms)."),
		gettext_noop("Range [100, 600000].  Default 5000.  LMON waits this long for every "
					 "live backend to ack the cooperative holder rebind;  expiry bumps "
					 "rebuild_timeout, KEEPS the affected shards frozen (fail-closed — a "
					 "half-rebuilt shard is never opened), and re-broadcasts the redeclare "
					 "request with a fresh deadline."),
		&cluster_grd_rebuild_timeout_ms, 5000, 100, 600000, PGC_SIGHUP, GUC_UNIT_MS, NULL, NULL,
		NULL);

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
	 * spec-5.11 D7 -- Hang Manager GUCs.  The DIAG main loop hosts the
	 * long-wait sampler (it fulfils the hang-dump duty HC3/HC6 deferred);
	 * these PGC_SIGHUP knobs gate it.  cluster.hang_max_sampled must not
	 * exceed CLUSTER_HANG_MAX_SAMPLES (the shared store is fixed-size); the
	 * GUC max is pinned to that constant.
	 */
	DefineCustomBoolVariable(
		"cluster.hang_manager_enabled",
		gettext_noop("Enables the DIAG-hosted Hang Manager long-wait sampler."),
		gettext_noop("When off, DIAG skips long-wait sampling entirely; the hang "
					 "category then freezes at the last sampled state and keeps "
					 "its cumulative counters (not zeroed) (spec-5.11)."),
		&cluster_hang_manager_enabled, true, PGC_SIGHUP, 0, NULL, NULL, NULL);

	DefineCustomIntVariable(
		"cluster.hang_sample_interval_ms",
		gettext_noop("Interval between Hang Manager long-wait sampling rounds."),
		gettext_noop("DIAG samples no more often than this; the effective "
					 "cadence is also bounded below by cluster.diag_main_loop_"
					 "interval (spec-5.11 D1)."),
		&cluster_hang_sample_interval_ms, 10000, 100, 600000, PGC_SIGHUP, GUC_UNIT_MS, NULL, NULL,
		NULL);

	DefineCustomIntVariable(
		"cluster.hang_threshold_ms",
		gettext_noop("Wait duration at/over which a backend is reported as a hang."),
		gettext_noop("A backend waiting on a resource at least this long (and "
					 "not excluded as idle / bgworker / confirmed-deadlock) is "
					 "recorded as a long-wait sample (spec-5.11)."),
		&cluster_hang_threshold_ms, 60000, 1000, 86400000, PGC_SIGHUP, GUC_UNIT_MS, NULL, NULL,
		NULL);

	DefineCustomBoolVariable(
		"cluster.hang_dump_enabled",
		gettext_noop("Enables Hang Manager long-wait LOG-once and dump accounting."),
		gettext_noop("When off, sampling still runs and pg_cluster_state still "
					 "reports the hang category; only the long-wait LOG-once and "
					 "the dumps_emitted / last_dump_emitted_at accounting are "
					 "suppressed (spec-5.11)."),
		&cluster_hang_dump_enabled, true, PGC_SIGHUP, 0, NULL, NULL, NULL);

	DefineCustomIntVariable(
		"cluster.hang_max_chain_depth",
		gettext_noop("Maximum wait-chain depth the Hang Manager walks before stopping."),
		gettext_noop("Bounds the wait-chain skeleton walk against a runaway / "
					 "cyclic chain (spec-5.11; full chain analysis is feature-054)."),
		&cluster_hang_max_chain_depth, 100, 1, 10000, PGC_SIGHUP, 0, NULL, NULL, NULL);

	DefineCustomIntVariable(
		"cluster.hang_max_sampled",
		gettext_noop("Maximum long-wait samples kept per round (top-N by duration)."),
		gettext_noop("Bounds the per-round shared sample store; excess long-waits "
					 "are dropped (longest kept) and the dump marks the round "
					 "truncated.  Cannot exceed the fixed store capacity "
					 "(CLUSTER_HANG_MAX_SAMPLES = 64) (spec-5.11)."),
		&cluster_hang_max_sampled, 64, 1, CLUSTER_HANG_MAX_SAMPLES, PGC_SIGHUP, 0, NULL, NULL,
		NULL);

	/*
	 * spec-5.12 D6 -- Hang Manager disposition GUCs.  The DIAG main loop, right
	 * after sampling, evaluates the spec-5.11 actionable samples and (in
	 * enforce mode) disposes of the root blocker via a cancel -> terminate
	 * ladder.  Factory default is advisory (dry-run): evaluate + recommend but
	 * never cancel/terminate, so operators can observe recommendation quality
	 * before granting enforce.  All PGC_SIGHUP (no on-disk / ABI effect).
	 */
	DefineCustomEnumVariable(
		"cluster.hang_resolution_mode",
		gettext_noop("Hang Manager disposition mode (off / advisory / enforce)."),
		gettext_noop("off: do not evaluate disposition (spec-5.11 diagnostics still run). "
					 "advisory (default): evaluate + record recommendations and counters "
					 "but never cancel or terminate (dry-run). enforce: actually dispose "
					 "of the root blocker via a cancel -> terminate ladder (spec-5.12)."),
		&cluster_hang_resolution_mode, HANG_RESOLVE_ADVISORY, cluster_hang_resolution_mode_options,
		PGC_SIGHUP, 0, NULL, NULL, NULL);

	DefineCustomIntVariable(
		"cluster.hang_resolution_confirm_rounds",
		gettext_noop("Consecutive rounds a victim identity must stay an actionable "
					 "long-wait before disposition (hysteresis)."),
		gettext_noop("Guards against disposing on a transient spike; the identity must "
					 "remain a COMPLETE long-wait root blocker for this many consecutive "
					 "sampling rounds before any signal is sent (spec-5.12 D5)."),
		&cluster_hang_resolution_confirm_rounds, 2, 1, 100, PGC_SIGHUP, 0, NULL, NULL, NULL);

	DefineCustomIntVariable(
		"cluster.hang_resolution_soft_timeout_ms",
		gettext_noop("Grace period between disposition tiers (cancel -> terminate -> degrade)."),
		gettext_noop("Tier escalation is cross-round and non-blocking: a victim still "
					 "hanging this long after the previous tier escalates to the next "
					 "tier on a later sampling round (DIAG never sleeps, spec-5.12 §2.3)."),
		&cluster_hang_resolution_soft_timeout_ms, 5000, 100, 600000, PGC_SIGHUP, GUC_UNIT_MS, NULL,
		NULL, NULL);

	DefineCustomIntVariable(
		"cluster.hang_resolution_max_per_round",
		gettext_noop("Maximum number of victims disposed per evaluation round."),
		gettext_noop("Rate-limits disposition so a burst of false hangs cannot cause a "
					 "cascade of cancels/terminates in a single round (spec-5.12)."),
		&cluster_hang_resolution_max_per_round, 1, 1, CLUSTER_HANG_MAX_SAMPLES, PGC_SIGHUP, 0, NULL,
		NULL, NULL);

	DefineCustomRealVariable(
		"cluster.hang_victim_w_age", gettext_noop("Victim score weight for transaction age."),
		gettext_noop("Higher weight prefers disposing the oldest transaction (spec-5.12 §2.2). "
					 "There are deliberately no priority / cpu weights: PG exposes no "
					 "per-session priority or per-backend CPU accounting (spec §8 Q7)."),
		&cluster_hang_victim_w_age, 0.5, 0.0, 1000.0, PGC_SIGHUP, 0, NULL, NULL, NULL);

	DefineCustomRealVariable(
		"cluster.hang_victim_w_rollback",
		gettext_noop("Victim score weight for rollback cost (proxied by held lock count)."),
		gettext_noop("Higher weight prefers disposing the cheaper-to-roll-back victim, "
					 "i.e. the one holding fewer locks (spec-5.12 §2.2)."),
		&cluster_hang_victim_w_rollback, 0.3, 0.0, 1000.0, PGC_SIGHUP, 0, NULL, NULL, NULL);

	DefineCustomRealVariable(
		"cluster.hang_victim_w_blockers",
		gettext_noop("Victim score weight for root-ness (number of waiters blocked)."),
		gettext_noop("Higher weight prefers disposing the victim that unblocks the most "
					 "waiters (spec-5.12 §2.2)."),
		&cluster_hang_victim_w_blockers, 0.2, 0.0, 1000.0, PGC_SIGHUP, 0, NULL, NULL, NULL);

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

	/* spec-3.13 D1: Undo Cleaner GUC trio (专项 #9 §3.3.1 30s 承诺实名化). */
	DefineCustomIntVariable("cluster.undo_cleaner_interval_ms",
							gettext_noop("Undo Cleaner pass interval in milliseconds."),
							gettext_noop("Cadence of proactive undo/TT-slot retention GC passes "
										 "(spec-3.13). 0 = pressure-wakeup only."),
							&cluster_undo_cleaner_interval_ms, 30000, 0, 3600000, PGC_SIGHUP, 0,
							NULL, NULL, NULL);
	DefineCustomBoolVariable(
		"cluster.undo_cleaner_enabled",
		gettext_noop("Enable proactive undo/TT-slot retention GC passes."),
		gettext_noop("off = the Undo Cleaner process stays resident but each pass no-ops, "
					 "reverting to spec-3.12 lazy-only recycling."),
		&cluster_undo_cleaner_enabled, true, PGC_SIGHUP, 0, NULL, NULL, NULL);
	DefineCustomIntVariable(
		"cluster.undo_cleaner_batch_segments",
		gettext_noop("Max own-instance undo segments scanned per cleaner pass."),
		gettext_noop("Bounds per-pass tail latency (spec-3.13 R7)."),
		&cluster_undo_cleaner_batch_segments, 8, 1, 256, PGC_SIGHUP, 0, NULL, NULL, NULL);
	DefineCustomBoolVariable(
		"cluster.undo_record_segment_commit_on_rollover",
		gettext_noop("Advance a drained record undo segment ACTIVE -> COMMITTED on rollover."),
		gettext_noop("off reverts to pre-4.12a behaviour where record segments were never "
					 "reclaimed (the leak); gates only the optimization, never the 8.A guard."),
		&cluster_undo_record_segment_commit_on_rollover, true, PGC_SIGHUP, 0, NULL, NULL, NULL);

	/* spec-5.7 HW: cluster relation-extend block-number authority gate. */
	DefineCustomBoolVariable(
		"cluster.relation_extend_lock_enabled",
		gettext_noop(
			"Extend permanent shared relations through the cluster block-number authority."),
		gettext_noop(
			"off is a forensic/test-only UNSAFE downgrade: extends revert to the PG-native "
			"local FileSize path, which silently corrupts in a multi-node cluster."),
		&cluster_relation_extend_lock_enabled, true, PGC_SUSET, 0, NULL, NULL, NULL);

	/* spec-5.14 D6: diag-only trace of touched fail-stop aborts. */
	DefineCustomBoolVariable(
		"cluster.touched_peers_trace",
		gettext_noop("Log the touched-peers set of each transaction aborted by a "
					 "fail-stop reconfiguration."),
		gettext_noop("Diagnostic only (default off): when on, every touched fail-stop "
					 "abort emits a LOG line with the aborting transaction's touched "
					 "node bitmap, to investigate unexpected (false-positive) aborts. "
					 "Has no effect on the abort decision itself."),
		&cluster_touched_peers_trace, false, PGC_SUSET, 0, NULL, NULL, NULL);

	/* spec-5.7 TT (§3.3): cross-node mutex around CREATE/DROP/ALTER/RENAME
	 * TABLESPACE + the placement-DDL TT(S) in-use guard. */
	DefineCustomBoolVariable(
		"cluster.tablespace_ddl_lock_enabled",
		gettext_noop("Serialise tablespace DDL (CREATE/DROP/ALTER/RENAME) across the cluster."),
		gettext_noop(
			"off reverts to the PG-native local-only catalog locks, which do not coordinate "
			"concurrent tablespace DDL between nodes."),
		&cluster_tablespace_ddl_lock_enabled, true, PGC_SUSET, 0, NULL, NULL, NULL);

	/* spec-5.7 KO (§3.5): cross-node object-reuse flush barrier before a
	 * relation's storage is physically removed/truncated (DROP/TRUNCATE/VACUUM
	 * truncation). */
	DefineCustomBoolVariable(
		"cluster.object_reuse_flush_enabled",
		gettext_noop(
			"Flush a relation's buffers on every peer before its storage is removed or truncated."),
		gettext_noop(
			"off skips the cross-node flush barrier: a peer's stale dirty buffers could be written "
			"back after the file is unlinked, recreating the file or corrupting a reused "
			"relfilenode in a multi-node cluster."),
		&cluster_object_reuse_flush_enabled, true, PGC_SUSET, 0, NULL, NULL, NULL);

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
	 * cluster.voting_disk_size_bytes (spec-2.6 D12; spec-5.13 doubled;
	 * spec-5.15 tripled; spec-6.4 quadrupled; spec-6.15 adds the xid
	 * stripe regions).  Voting disk file size — pre-allocated on first
	 * boot.  Per-instance 512-byte regions: region 1 = the voting slot at
	 * offset (node_id × 512); region 2 = the clean-leave marker at
	 * ((CLUSTER_MAX_NODES + node_id) × 512); region 3 = the join-commit
	 * marker at ((2 × CLUSTER_MAX_NODES + node_id) × 512); region 4 = the
	 * ADG apply-master lease marker at ((3 × CLUSTER_MAX_NODES + node_id)
	 * × 512); region 5 = the xid stripe slot at ((4 × CLUSTER_MAX_NODES +
	 * node_id) × 512); region 6 = ONE cluster-wide stripe activation
	 * record at (5 × CLUSTER_MAX_NODES × 512).  Default 328192 bytes =
	 * (5 × 128 + 1) × 512.  Range [4096, 1048576].
	 */
	DefineCustomIntVariable("cluster.voting_disk_size_bytes",
							gettext_noop("Voting disk file size in bytes."),
							gettext_noop("Pre-allocated voting disk size (spec-2.6 D12; spec-5.13 "
										 "doubled for the clean-leave marker region; spec-5.15 "
										 "tripled for the join-commit marker region; spec-6.4 "
										 "quadrupled for the ADG apply-master lease region; "
										 "spec-6.15 adds the xid stripe slot region and the "
										 "stripe activation record).  Each instance owns a "
										 "512-byte voting slot at offset (node_id × 512), a "
										 "leave-marker slot at ((128 + node_id) × 512), a "
										 "join-marker slot at ((256 + node_id) × 512), an ADG "
										 "lease slot at ((384 + node_id) × 512), and an xid "
										 "stripe slot at ((512 + node_id) × 512); one "
										 "cluster-wide stripe activation record lives at "
										 "(640 × 512).  Default 328192 = (5 × 128 + 1) × 512.  "
										 "Range [4096, 1048576] bytes; multiple of 512."),
							&cluster_voting_disk_size_bytes, 328192, 4096, 1048576, PGC_POSTMASTER,
							GUC_UNIT_BYTE, NULL, NULL, NULL);

	/* spec-5.15 D7 — online declared-node join (Q7/Q8). */
	DefineCustomBoolVariable("cluster.online_join",
							 gettext_noop("Allow a declared node to join/rejoin live membership "
										  "online (without a full cluster restart)."),
							 gettext_noop("Capability opt-in (default off, fail-closed-safe: a "
										  "DEAD node is never auto-readmitted while off).  On: a "
										  "declared node that comes back from down/absent is "
										  "vetted (monotonic incarnation guard) and published into "
										  "membership via a two-phase epoch-converged reconfig."),
							 &cluster_online_join, false, PGC_POSTMASTER, 0, NULL, NULL, NULL);

	/* spec-5.16 D6 — OPTIONAL GRD logical-lock rebalance on node rejoin.  The
	 * PCM block snap-back fence is NOT gated here (it is forced correctness
	 * bound to cluster.online_join, INV-R13); this only controls whether the
	 * joiner's home-shard GES mastership is moved back from the survivor. */
	DefineCustomBoolVariable(
		"cluster.join_remaster_enabled",
		gettext_noop("On node rejoin, move the joiner's home-shard GES "
					 "mastership back from the survivor (optional rebalance)."),
		gettext_noop("Default off: the joiner's logical-lock mastership stays "
					 "on the survivor that held it during the absence (load "
					 "imbalance only).  The PCM block view rebuild + RECOVERING "
					 "fence always run when cluster.online_join is on, "
					 "independent of this GUC (forced correctness)."),
		&cluster_join_remaster_enabled, false, PGC_POSTMASTER, 0, NULL, NULL, NULL);

	DefineCustomIntVariable("cluster.join_convergence_timeout_ms",
							gettext_noop("Deadline for an online join to converge + commit."),
							gettext_noop("If a joining node is not published MEMBER within this "
										 "bound it fails closed (53R61, restart with a fresh "
										 "incarnation) rather than half-admitting.  Range "
										 "[5000, 120000] ms."),
							&cluster_join_convergence_timeout_ms, 30000, 5000, 120000, PGC_SIGHUP,
							GUC_UNIT_MS, NULL, NULL, NULL);

	/*
	 * cluster.online_node_removal -- opt-in permanent node removal (spec-5.18).
	 * Default OFF (5.6 opt-in paradigm): pg_cluster_remove_node() returns
	 * rejected:feature_disabled and no removal path runs.  Permanent removal is a
	 * high-risk irreversible operation (fence + member-set shrink), so rollout is
	 * conservative; flipped on after the spec-5.19 4-node fault-matrix acceptance.
	 */
	DefineCustomBoolVariable(
		"cluster.online_node_removal",
		gettext_noop("Enable permanent removal (decommission) of a declared node."),
		gettext_noop("Off: pg_cluster_remove_node() is rejected (feature_disabled) and no "
					 "removal/fence/cleanup path runs.  On: an already-left or non-returning "
					 "declared node is permanently fenced, shrunk out of the member set, and "
					 "its GRD/GES/PCM leftover cleaned up cluster-wide."),
		&cluster_online_node_removal, false, PGC_POSTMASTER, 0, NULL, NULL, NULL);

	DefineCustomIntVariable(
		"cluster.node_removal_cleanup_timeout_ms",
		gettext_noop("Deadline for the post-shrink cluster-wide removal cleanup."),
		gettext_noop("If verify_no_leftover + all-survivor cleanup ACKs do not "
					 "complete within this bound, the removal enters "
					 "CLEANUP_BLOCKED (resumable, fail-closed — never reports "
					 "complete).  Range [5000, 120000] ms."),
		&cluster_node_removal_cleanup_timeout_ms, 30000, 5000, 120000, PGC_SIGHUP, GUC_UNIT_MS,
		NULL, NULL, NULL);

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

	DefineCustomIntVariable(
		"cluster.undo_extent_blocks",
		gettext_noop("Undo block extent size claimed per transaction."),
		gettext_noop("Spec-3.18 D3.  A transaction claims a run of this many "
					 "consecutive undo data blocks on its first write and then "
					 "advances a backend-local cursor within the extent -- only "
					 "an extent claim touches the shared lifecycle lock, which "
					 "removes the per-record cursor_lock serial hot-spot.  "
					 "1 = per-block (debug; the pre-D3 behavior).  Default 4."),
		&cluster_undo_extent_blocks, 4, 1, 256, PGC_SIGHUP, 0, NULL, NULL, NULL);

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
					 "reconstructed read_scn-consistent block image. Active whenever cluster "
					 "storage mode is on (cluster.enabled + a valid cluster.node_id), INCLUDING a "
					 "single-node cluster deployment (cluster snapshots are CLUSTER-source "
					 "there too). Set off to fall back to the "
					 "spec-3.2/3.3 SCN/PG-native visibility path."),
		&cluster_cr_mvcc_gate, true, PGC_USERSET, 0, NULL, NULL, NULL);

	DefineCustomBoolVariable(
		"cluster.cr_gate_no_peer_fastpath",
		gettext_noop("Skip the CR/SCN cluster visibility fork for a no-peer, "
					 "session-local snapshot (use the PG-native MVCC body)."),
		gettext_noop("Spec-3.24 D1. DEFAULT ON. When on, a "
					 "cluster-source snapshot that this backend took itself "
					 "(not an imported / SET TRANSACTION SNAPSHOT / parallel-"
					 "worker snapshot) and that runs with no cluster peers "
					 "bypasses the own-instance CR gate and SCN resolver and is "
					 "judged by the PG-native MVCC path. With no peers there are "
					 "no cross-node xids, so AD-012 例外 9's 'never PG-native' "
					 "premise is void and the PG-native verdict equals the "
					 "SCN/CR verdict. Default flipped on after the D1 differential "
					 "and clean-CI Dfp stop gate proved equivalence; set off as a "
					 "diagnostic escape hatch."),
		&cluster_cr_gate_no_peer_fastpath, true, PGC_USERSET, 0, NULL, NULL, NULL);

	DefineCustomBoolVariable(
		"cluster.cr_tuple_level_fastpath",
		gettext_noop("Use the tuple-level / verdict-only CR read fast path for a "
					 "single-candidate-chain block (compute-only; default off)."),
		gettext_noop("Spec-5.54. DEFAULT OFF. When on, a post-read_scn tuple whose "
					 "block has exactly one candidate transaction (nchains==1) and is "
					 "own-instance has its visibility verdict computed by reconstructing "
					 "ONLY the queried offnum on a backend-local scratch (single-chain "
					 "target-offnum walk), instead of materializing + caching the whole "
					 "read_scn block image. The verdict is bit-equivalent to the "
					 "full-block path or it fail-safe falls back to it; no tuple bytes "
					 "are emitted and no cross-backend cache is built. Default off "
					 "pending spec-5.58 differential-equivalence + perf validation; set "
					 "on only as a measurement / opt-in escape hatch."),
		&cluster_cr_tuple_level_fastpath, false, PGC_USERSET, 0, NULL, NULL, NULL);

	DefineCustomBoolVariable(
		"cluster.tt_durable_lookup",
		gettext_noop("Resolve commit_scn from the durable undo-header TT slot on overlay miss."),
		gettext_noop("Spec-3.11 D7. DEFAULT ON. When on, a visibility / CR lookup that misses "
					 "the in-memory TT status overlay (eviction / restart) falls through to the "
					 "durable TT slot in the undo segment header (D5), and the CR watermark gate "
					 "resolves an evicted post-snapshot writer by xid (D6), retiring the "
					 "spec-3.10 fail-closed for the still-bound case. Off falls back to the "
					 "spec-3.10 overlay-only path: an overlay miss / recycle watermark newer than "
					 "the snapshot fails closed (53R97 / 53R9F). Durability writes (commit stamp "
					 "+ crash redo) are never gated; toggling off only disables read-side "
					 "resolution and never loses durable commit_scn."),
		&cluster_tt_durable_lookup, true, PGC_USERSET, 0, NULL, NULL, NULL);

	DefineCustomBoolVariable(
		"cluster.tt_recovery_resolve_active",
		gettext_noop("Resolve crash-left ACTIVE durable TT slots to ABORTED at startup."),
		gettext_noop("Spec-4.8 D1. DEFAULT ON. After redo + prepared-xact recovery, the startup "
					 "process scans this instance's undo segment headers and durably transitions "
					 "every crash-left TT_SLOT_ACTIVE slot to TT_SLOT_ABORTED unless its owning "
					 "xact is committed (CLOG) or a resurrected prepared 2PC xact -- so cluster "
					 "visibility never treats an in-flight-at-crash transaction as committed "
					 "(规则 8.A; an ACTIVE slot that cannot be proven live is aborted). Off skips "
					 "the resolution: slots stay ACTIVE and fall through to the by-xid 0-match "
					 "path (no correctness loss, only the explicit verdict + housekeeping)."),
		&cluster_tt_recovery_resolve_active, true, PGC_POSTMASTER, 0, NULL, NULL, NULL);

	DefineCustomBoolVariable(
		"cluster.cf_terminal_authority",
		gettext_noop("Enable spec-6.2 Cache Fusion durable TT/undo terminal authority."),
		gettext_noop("Spec-6.2. Default off, preserving the Stage 5 conservative active-ITL "
					 "transfer boundary. When on, cross-instance undo / TT terminal decisions "
					 "must prove membership epoch, ownership, terminal outcome, and required "
					 "durable/retention evidence before callers may trust them. Missing evidence "
					 "fails closed; there is no native fallback or UNKNOWN-visible behavior."),
		&cluster_cf_terminal_authority, false, PGC_POSTMASTER, 0, NULL, NULL, NULL);

	DefineCustomEnumVariable(
		"cluster.cf_delayed_cleanout",
		gettext_noop("Choose the spec-6.2 delayed ITL cleanout policy."),
		gettext_noop("Spec-6.2. Values: off never writes back ITL hints; reader performs lazy "
					 "reader-path cleanout from durable TT authority; eager is reserved for a "
					 "future transfer-side sweep. The setting only controls hinting; verdicts "
					 "still come from durable TT authority and fail closed when unresolved."),
		&cluster_cf_delayed_cleanout, CLUSTER_CF_DELAYED_CLEANOUT_READER,
		cluster_cf_delayed_cleanout_options, PGC_SIGHUP, 0, NULL, NULL, NULL);

	DefineCustomBoolVariable(
		"cluster.smart_fusion",
		gettext_noop("Guarded spec-6.2 Smart Fusion early block-transfer dependency tracking."),
		gettext_noop("Spec-6.2. Default off and currently fail-closed if set on. The "
					 "substrate counters and wire definitions remain available, but the "
					 "early-transfer runtime path is rejected until checkpoint writeback, 2PC, "
					 "and dependency-consumer soundness are complete."),
		&cluster_smart_fusion, false, PGC_POSTMASTER, 0, cluster_smart_fusion_check_hook, NULL,
		NULL);

	DefineCustomEnumVariable(
		"cluster.smart_fusion_tier_min",
		gettext_noop("Minimum interconnect tier that may use Smart Fusion early transfer."),
		gettext_noop("Spec-6.2. Only tier3 is currently legal: Smart Fusion requires an "
					 "authenticated direct-wire path. TCP/tier1/tier2 deployments remain on "
					 "HC82 WAL-before-ship even if cluster.smart_fusion is on."),
		&cluster_smart_fusion_tier_min, CLUSTER_IC_TIER_3, cluster_smart_fusion_tier_min_options,
		PGC_POSTMASTER, 0, NULL, NULL, NULL);

	DefineCustomIntVariable(
		"cluster.smart_fusion_commit_brake_timeout_ms",
		gettext_noop("Timeout for the spec-6.2 Smart Fusion pre-commit dependency brake."),
		gettext_noop("A transaction that consumed an early-transfer dependent block waits up to "
					 "this many milliseconds, before writing its commit record, for all origin "
					 "redo dependencies to become durable. Timeout aborts the transaction with "
					 "a retryable Smart Fusion error instead of false-committing."),
		&cluster_smart_fusion_commit_brake_timeout_ms, 5000, 1, 600000, PGC_SIGHUP, GUC_UNIT_MS,
		NULL, NULL, NULL);

	DefineCustomIntVariable(
		"cluster.smart_fusion_origin_durable_gossip_ms",
		gettext_noop("Interval for publishing local durable WAL progress to Smart Fusion peers."),
		gettext_noop("Spec-6.2 origin durable-LSN gossip interval. Receivers release DBWR and "
					 "commit brakes only after observing durable progress; they never trust a "
					 "block marker as proof of durability."),
		&cluster_smart_fusion_origin_durable_gossip_ms, 50, 1, 60000, PGC_SIGHUP, GUC_UNIT_MS, NULL,
		NULL, NULL);

	DefineCustomIntVariable(
		"cluster.cr_cache_max_blocks",
		gettext_noop("Backend-local CR block cache capacity in 8 KB blocks (0 disables)."),
		gettext_noop("Spec-3.10 D4. Default 64, range 0..4096, PGC_USERSET. Caches full-block "
					 "CR images keyed by (RelFileLocator, fork, block, read_scn, page_lsn) so a "
					 "block re-read under the same snapshot/version is served without re-walking "
					 "the undo chains. 0 disables caching (every CR is reconstructed; useful for "
					 "perf A/B and as a fallback). Each block costs 8 KB of backend-local memory."),
		&cluster_cr_cache_max_blocks, 64, 0, 4096, PGC_USERSET, 0, NULL, NULL, NULL);

	/* spec-5.51 D8: dedicated shared (L2) CR buffer pool.  Both PGC_POSTMASTER:
	 * the pool size and on/off are fixed at shmem reservation.  Default off /
	 * size 0 = true zero memory (the region is registered but claims 0 bytes),
	 * so the spec-3.10 L1-only path is unchanged.  Real default size + sizing
	 * formula are bound to spec-5.50 profile evidence (not decided here). */
	DefineCustomBoolVariable(
		"cluster.shared_cr_pool_enabled",
		gettext_noop("Enable the dedicated shared (cross-backend) CR buffer pool (L2)."),
		gettext_noop("Spec-5.51. Default off. Master switch for the per-instance shared CR "
					 "block pool layered behind the backend-local L1 cache. PGC_POSTMASTER: "
					 "requires a restart. shared_cr_pool_size_blocks == 0 also disables it."),
		&cluster_shared_cr_pool_enabled, false, PGC_POSTMASTER, 0, NULL, NULL, NULL);

	DefineCustomIntVariable(
		"cluster.shared_cr_pool_size_blocks",
		gettext_noop("Shared CR buffer pool capacity in 8 KB blocks (0 = disabled / zero memory)."),
		gettext_noop(
			"Spec-5.51. Default 0 (true zero memory; the region is registered but "
			"reserves 0 bytes). PGC_POSTMASTER: requires a restart. Rounded down to a "
			"multiple of the partition count. The recommended non-zero default and "
			"sizing formula are determined by spec-5.50 profile evidence; do not set "
			"size > 0 for perf claims without it. Each block costs 8 KB of shared memory."),
		&cluster_shared_cr_pool_size_blocks, 0, 0, 262144, PGC_POSTMASTER, 0, NULL, NULL, NULL);

	/* spec-5.56 D4: per-relation lifecycle generation table (Part B; value-gated).
	 * PGC_POSTMASTER: the table size + its own shmem region are fixed at
	 * reservation time.  Default 0 = disabled => the smgrdounlinkall lifecycle bump
	 * stays the spec-5.53 unconditional GLOBAL epoch bump (coarse, whole-pool
	 * flush; zero behavior change).  > 0 enables fine-grained per-relation
	 * invalidation: a DROP/TRUNCATE bumps only the dropped relation's generation,
	 * so unrelated warm CR images survive (spec-5.56 D0 measured a 100% coarse
	 * blast radius per unrelated DDL).  Size it >= the pool's distinct-locator
	 * working set; on overflow an install serves-but-skips-cache (observable via
	 * cr_rel_gen_table_overflow_count).  Meaningless without the pool enabled. */
	DefineCustomIntVariable(
		"cluster.cr_pool_rel_generation_slots",
		gettext_noop("Per-relation CR lifecycle generation table size (0 = disabled; coarse)."),
		gettext_noop(
			"Spec-5.56 Part B. Default 0 (disabled => the coarse global-epoch bump, "
			"spec-5.53 behavior). PGC_POSTMASTER: requires a restart, own shmem region. "
			"> 0 enables fine-grained per-relation CR invalidation so an unrelated DROP / "
			"TRUNCATE does not flush the whole shared CR pool. Rounded down to a multiple "
			"of the partition count; size it >= the pool's distinct-relation working set."),
		&cluster_cr_pool_rel_generation_slots, 0, 0, 262144, PGC_POSTMASTER, 0, NULL, NULL, NULL);

	/* spec-5.52 D8: shared CR pool admission policy (backend-local, advisory).
	 * All PGC_SIGHUP: the admission state is backend-local with no shmem layout
	 * dependency, so the policy + thresholds are runtime-tunable.  Defaults
	 * (admit_all / cap 0 / pressure 0) reproduce spec-5.51 v1 when the pool is
	 * enabled and spec-3.10 when it is not -- zero behavior change.  Admission is
	 * a pure membership heuristic and never affects the served image (8.A). */
	{
		static const struct config_enum_entry cluster_cr_pool_admission_policy_options[]
			= { { "admit_all", CR_ADMIT_ALL, false },
				{ "no_admit", CR_ADMIT_NO_ADMIT, false },
				{ "scan_resistant", CR_ADMIT_SCAN_RESISTANT, false },
				{ NULL, 0, false } };

		DefineCustomEnumVariable(
			"cluster.cr_pool_admission_policy",
			gettext_noop("Insert-side admission policy for the shared CR buffer pool."),
			gettext_noop("Spec-5.52. Default admit_all = spec-5.51 v1 populate-on-construct. "
						 "no_admit stops inserting NEW images but is NOT a pool disable: already "
						 "pooled entries are still served (the true pool on/off is "
						 "shared_cr_pool_size_blocks/enabled, orthogonal to this GUC). "
						 "scan_resistant bypasses bulk/parallel/volatile/over-cap/under-pressure "
						 "candidates so one-shot scans cannot evict cross-backend hot CR images. "
						 "Membership-only: never affects which image is served (8.A)."),
			&cluster_cr_pool_admission_policy, CR_ADMIT_ALL,
			cluster_cr_pool_admission_policy_options, PGC_SIGHUP, 0, NULL, NULL, NULL);
	}

	DefineCustomIntVariable(
		"cluster.cr_pool_admit_relation_backend_cap",
		gettext_noop("Per-backend cap on CR pool admits for a single relation (0 disables)."),
		gettext_noop("Spec-5.52 D5. Default 0 (disabled). Under scan_resistant, limits how many "
					 "CR images THIS backend admits into the shared pool for one relation -- a "
					 "per-backend anti-monopoly throttle. NOT a global pool-occupancy cap (a "
					 "backend cannot observe global occupancy; the true global cap is forward "
					 "5.52 v2/5.58). Advisory; only affects hit/miss."),
		&cluster_cr_pool_admit_relation_backend_cap, 0, 0, 1048576, PGC_SIGHUP, 0, NULL, NULL,
		NULL);

	DefineCustomIntVariable(
		"cluster.cr_pool_admit_pressure_ratio",
		gettext_noop("CR pool evict:hit pressure threshold (percent) for admission throttling (0 "
					 "disables)."),
		gettext_noop("Spec-5.52 D6. Default 0 (disabled). Under scan_resistant, when the recent "
					 "shared-pool evict:hit ratio reaches this percent, admission is decimated "
					 "(negative feedback) so a thrashing pool can stabilize. Advisory; only "
					 "affects hit/miss. The threshold is pending spec-5.58 calibration."),
		&cluster_cr_pool_admit_pressure_ratio, 0, 0, 100000, PGC_SIGHUP, 0, NULL, NULL, NULL);

	/* spec-5.55 D7: shared resolver cache (CR Source 3 by-xid search-shortcut
	 * memo).  Both PGC_POSTMASTER: the entry count + measure switch are final at
	 * shmem reservation.  Default entries 0 / measure off = true zero memory (the
	 * region is registered but reserves 0 bytes), so the spec-3.22 by-xid path is
	 * byte-identical.  v1 ships in MEASURE mode only (the value gate, §0.6): it
	 * always re-runs the authoritative scan and never trusts the hint -- the
	 * trust path (flip-on) is gated on measured redundancy + re-probe hit rate. */
	DefineCustomBoolVariable(
		"cluster.resolver_cache_enabled",
		gettext_noop("Enable spec-5.55 shared resolver cache TRUST mode (skip the by-xid scan on a "
					 "re-validated + accepted hint)."),
		gettext_noop("Spec-5.55. Default off. When on (with resolver_cache_entries > 0), a CR "
					 "Source 3 by-xid resolution that hits the shared memo re-validates the hint "
					 "slot in O(1) and re-runs the SAME wrap_suspect acceptance as a fresh scan, "
					 "resolving WITHOUT the O(segments) scan (verdict-equivalent by construction). "
					 "The recommended non-zero default is bound to the §0.6 value gate evidence "
					 "(spec-5.58). PGC_POSTMASTER: requires a restart."),
		&cluster_resolver_cache_enabled, false, PGC_POSTMASTER, 0, NULL, NULL, NULL);

	DefineCustomBoolVariable(
		"cluster.resolver_cache_measure",
		gettext_noop("Enable spec-5.55 shared resolver cache MEASURE mode (value gate, no trust)."),
		gettext_noop("Spec-5.55 §0.6. Default off. When on (with resolver_cache_entries > 0), CR "
					 "Source 3 records whether its own-instance by-xid scan result was already "
					 "memoized by a peer backend and whether an O(1) re-validation + acceptance "
					 "would have passed -- the cross-backend redundancy + re-probe hit rate that "
					 "gate the trust path.  Never changes a visibility verdict (the authoritative "
					 "scan always runs).  Orthogonal to resolver_cache_enabled.  PGC_POSTMASTER."),
		&cluster_resolver_cache_measure, false, PGC_POSTMASTER, 0, NULL, NULL, NULL);

	DefineCustomIntVariable(
		"cluster.resolver_cache_entries",
		gettext_noop("Shared resolver cache hint-slot count (0 = disabled / zero memory)."),
		gettext_noop("Spec-5.55 D3/D7. Default 0 (true zero memory; the region is registered but "
					 "reserves 0 bytes).  PGC_POSTMASTER: requires a restart.  The recommended "
					 "non-zero default + sizing are bound to the §0.6 measure-leg value gate "
					 "evidence (spec-5.58); either resolver_cache_enabled or "
					 "resolver_cache_measure must be on for this to allocate.  Each hint slot "
					 "costs a few dozen bytes of shared memory."),
		&cluster_shared_resolver_cache_entries, 0, 0, 1048576, PGC_POSTMASTER, 0, NULL, NULL, NULL);

	/* spec-5.57 D3 §2.2: cross-instance CR read-path coordinator boundary.  This
	 * GUC ONLY gates the D3 observability surface (counter bumps / dump / probe /
	 * LOG-once); it NEVER gates the fail-closed 53R9G boundary, which is
	 * non-degradable and fires under any value (8.A).  off != spec-5.56 zero
	 * behavior change: the CR-path remote-undo read is unified to fail-closed
	 * 53R9G under every value. */
	{
		static const struct config_enum_entry cluster_cross_instance_cr_coordinator_options[]
			= { { "off", CR_COORD_MODE_OFF, false },
				{ "boundary", CR_COORD_MODE_BOUNDARY, false },
				{ "forward", CR_COORD_MODE_FORWARD, false },
				{ NULL, 0, false } };

		DefineCustomEnumVariable(
			"cluster.cross_instance_cr_coordinator",
			gettext_noop(
				"Observability mode for the cross-instance CR read-path coordinator boundary."),
			gettext_noop(
				"Spec-5.57. Default boundary = the fail-closed cross-instance boundary plus "
				"D3 counters. off turns OFF only the observability surface (counters/dump/"
				"probe/LOG-once); the fail-closed 53R9G boundary itself is non-degradable "
				"and fires under EVERY value (8.A) -- runtime-warm cross-instance CR/undo "
				"reads never become visible. forward is a contract placeholder: it stays "
				"fail-closed (the data plane is Stage 6, #119 undo-block Cache Fusion) but "
				"LOG-once tells the operator so."),
			&cluster_cross_instance_cr_coordinator, CR_COORD_MODE_BOUNDARY,
			cluster_cross_instance_cr_coordinator_options, PGC_SIGHUP, 0, NULL, NULL, NULL);
	}

	DefineCustomBoolVariable(
		"cluster.cross_instance_cr_probe",
		gettext_noop(
			"Count class③ runtime-warm cross-instance CR hits for the spec-5.57 measure-leg."),
		gettext_noop(
			"Spec-5.57 D0. Default off. When on, a class③ (runtime-warm remote) CR hit "
			"additionally bumps the cross_instance_boundary_probe counter -- COUNT ONLY, "
			"behavior unchanged: the read is still fail-closed 53R9G (never visible). Used "
			"to measure that the cross-instance data plane is unreachable in Stage 5.5 "
			"(value-gate -> Stage 6)."),
		&cluster_cross_instance_cr_probe, false, PGC_USERSET, 0, NULL, NULL, NULL);

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

	/* spec-5.5 D7:cross-node advisory (user) lock master switch. */
	DefineCustomBoolVariable(
		"cluster.advisory_lock_enabled",
		gettext_noop("Enable cross-node globalization of advisory (user) locks."),
		gettext_noop("When true (default), session- and xact-scoped "
					 "pg_advisory_lock* acquire cross-node mutual exclusion via "
					 "the GES enqueue substrate.  When false, advisory locks route "
					 "PG-native (single-node semantics) — a forensic/test-only "
					 "UNSAFE downgrade that SILENTLY disables cross-node advisory "
					 "mutual exclusion.  Multi-node deployments that rely on "
					 "advisory locks for application coordination MUST NOT disable "
					 "this.  PGC_SUSET."),
		&cluster_advisory_lock_enabled, true, PGC_SUSET, 0, NULL, NULL, NULL);

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

	/* spec-5.8 D3 — coordinator-driven cross-node deadlock detection. */
	DefineCustomBoolVariable(
		"cluster.deadlock_detection_enabled",
		gettext_noop("Enable coordinator-driven cross-node deadlock detection."),
		gettext_noop("When off, only the per-node local Tarjan scan runs and "
					 "cross-node deadlocks rely on the finite GES / TX enqueue "
					 "timeouts.  PGC_SIGHUP."),
		&cluster_lmd_deadlock_detection_enabled, true, PGC_SIGHUP, 0, NULL, NULL, NULL);

	DefineCustomIntVariable(
		"cluster.global_dd_interval_ms",
		gettext_noop("Coordinator cross-node deadlock scan period (ms)."),
		gettext_noop("Only the HC16 lowest-active node runs the cross-node scan, "
					 "at this period.  PGC_SIGHUP."),
		&cluster_lmd_global_dd_interval_ms, 2000, 100, 600000, PGC_SIGHUP, 0, NULL, NULL, NULL);

	DefineCustomIntVariable(
		"cluster.deadlock_confirm_interval_ms",
		gettext_noop("Delay between the two coordinator deadlock-confirm rounds (ms)."),
		gettext_noop("A cross-node cycle is cancelled only if observed identically "
					 "in both rounds separated by this delay (Rule 8.A transient "
					 "filter).  PGC_SIGHUP."),
		&cluster_lmd_deadlock_confirm_interval_ms, 500, 50, 60000, PGC_SIGHUP, 0, NULL, NULL, NULL);

	/* PGRAC: spec-5.9 D10 — victim policy + cancel robustness GUCs.  PGC_SUSET
	 * so unprivileged users cannot perturb cross-node deadlock resolution. */
	DefineCustomIntVariable(
		"cluster.cancel_ack_timeout_ms",
		gettext_noop(
			"Coordinator wait for a cross-node deadlock CANCEL_ACK before retransmit (ms)."),
		gettext_noop("Should stay below cluster.global_dd_interval_ms.  PGC_SUSET."),
		&cluster_cancel_ack_timeout_ms, 1000, 50, 60000, PGC_SUSET, GUC_UNIT_MS, NULL, NULL, NULL);

	DefineCustomIntVariable(
		"cluster.cancel_max_retransmit",
		gettext_noop("Bounded cross-node deadlock cancel retransmit attempts before escalation."),
		gettext_noop("0 disables retransmit (5.8 fire-and-forget).  Exhaustion escalates to an "
					 "alternate victim, then a finite-timeout fallback (never a force-kill).  "
					 "PGC_SUSET."),
		&cluster_cancel_max_retransmit, 3, 0, 100, PGC_SUSET, 0, NULL, NULL, NULL);

	DefineCustomIntVariable(
		"cluster.victim_repeat_window_ms",
		gettext_noop("Anti-thrash window for repeated deadlock victim selection (ms)."),
		gettext_noop("A victim re-selected within this window prefers an alternate to avoid "
					 "livelock (advisory — falls back to youngest if none exists).  PGC_SUSET."),
		&cluster_victim_repeat_window_ms, 5000, 0, 600000, PGC_SUSET, GUC_UNIT_MS, NULL, NULL,
		NULL);

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
	 * PGRAC: spec-4.7 D1 — cluster.gcs_block_recovery_wait_ms.  Bounded
	 * backend wait on a block resource in GCS_BLOCK_RECOVERING before
	 * fail-closing 53R9L (survivor re-declare / master rebuild in progress).
	 * PGC_SIGHUP — operators tune without restart.
	 */
	DefineCustomIntVariable(
		"cluster.gcs_block_recovery_wait_ms",
		gettext_noop("Bounded wait (ms) on a recovering GCS block resource before 53R9L."),
		gettext_noop("After reconfiguration a block resource enters RECOVERING "
					 "(survivor re-declare / master rebuild).  A backend touching "
					 "such a block waits up to this long for the rebuild, then "
					 "fail-closes ERRCODE_CLUSTER_GCS_BLOCK_RESOURCE_RECOVERING "
					 "(53R9L) — never a stale local / old-master fallback.  spec-4.7 D1."),
		&cluster_gcs_block_recovery_wait_ms, 200, 0, 60000, PGC_SIGHUP, 0, NULL, NULL, NULL);

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

	DefineCustomBoolVariable(
		"cluster.gcs_block_local_cache",
		gettext_noop("Hold PCM block locks until revoked (node-level cache)."),
		gettext_noop("When on (default), the bufmgr acquire gate skips the "
					 "remote GCS master round-trip when this node already holds "
					 "a covering PCM mode (X covers S/X, S covers S), and X is "
					 "held across content-lock unlock (released only on "
					 "INVALIDATE or eviction).  This eliminates the per-LockBuffer "
					 "round-trip storm.  Off reverts to the spec-2.33 per-acquire "
					 "request behavior (escape hatch).  spec-4.7a D2.  PGC_SUSET."),
		&cluster_gcs_block_local_cache, true, PGC_SUSET, 0, NULL, NULL, NULL);

	DefineCustomBoolVariable("cluster.tx_enqueue_wait",
							 gettext_noop("Block on a remote row lock until the holder completes."),
							 gettext_noop("When on (default), a backend that conflicts with a row "
										  "lock held by a transaction on another node blocks in a "
										  "cross-node TX enqueue completion wait until the holder "
										  "commits/aborts (or cluster.ges_request_timeout_ms "
										  "elapses), then re-judges.  Off reverts to the spec-3.4d "
										  "fail-closed (SQLSTATE 53R98) honest degradation.  "
										  "spec-5.2 D4.  PGC_SUSET."),
							 &cluster_tx_enqueue_wait_enabled, true, PGC_SUSET, 0, NULL, NULL,
							 NULL);

	DefineCustomBoolVariable(
		"cluster.ic_duty_lazy",
		gettext_noop("Run lazy-able LMON duty-chain drains on demand instead of every iteration."),
		gettext_noop("When on (default), the queue-consumption duty families in the "
					 "LMON main loop (outbound drains / ship-ready / sinval out / "
					 "tt-hint / dedup TTL / backup / GES work queue) run only when "
					 "their producer marked them dirty or on the >= 1 Hz floor.  "
					 "Correctness families (fence / sweeps / reconfig / recovery) "
					 "always run every iteration.  Off restores the run-every-"
					 "iteration behavior (escape hatch).  spec-7.2 D1.  PGC_SIGHUP."),
		&cluster_ic_duty_lazy, true, PGC_SIGHUP, 0, NULL, NULL, NULL);

	DefineCustomIntVariable(
		"cluster.gcs_block_retransmit_initial_backoff_ms",
		gettext_noop("Initial backoff before retry 1 (subsequent retries double)."),
		gettext_noop("Exponential backoff base for GCS block-ship retransmit:  "
					 "retry 1 waits this much, retry 2 doubles, etc.  Default "
					 "10 → 10/20/40/80 ms for N=4 retries (total 150 ms;  LAN "
					 "RTT scale, spec-7.2 D1).  Raise on slow or congested "
					 "interconnects.  The per-attempt reply wait itself stays "
					 "cluster.gcs_reply_timeout_ms.  HC97.  PGC_SUSET."),
		&cluster_gcs_block_retransmit_initial_backoff_ms, 10, 1, 5000, PGC_SUSET, 0, NULL, NULL,
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
	 * PGRAC: spec-4.10 D1 — online single-block recovery (2 NEW GUC).
	 */
	DefineCustomBoolVariable(
		"cluster.online_block_recovery",
		gettext_noop("Rebuild a corrupt/lost-write block from WAL on read instead of erroring."),
		gettext_noop("On (default): a checksum/header failure (single-node / own-thread "
					 "only) triggers online reconstruction from WAL before the "
					 "zero_damaged_pages / error policy; takes precedence over "
					 "ignore_checksum_failure.  Off: corruption falls straight to the "
					 "existing policy.  Multi-node blocks (foreign last-writer) are not "
					 "auto-recovered (forward Stage 5)."),
		&cluster_online_block_recovery, true, PGC_SIGHUP, 0, NULL, NULL, NULL);

	DefineCustomEnumVariable(
		"cluster.block_recovery_on_unrecoverable",
		gettext_noop("Action when a corrupt block cannot be rebuilt from WAL."),
		gettext_noop("error (default): fail the read with ERRCODE_DATA_CORRUPTED.  "
					 "panic: escalate to PANIC (operator opt-in).  Applies when "
					 "online_block_recovery is on and reconstruction is not possible "
					 "(no FPI base / WAL recycled / unsupported delta / cross-node)."),
		&cluster_block_recovery_on_unrecoverable, CLUSTER_BLKREC_ACTION_ERROR,
		cluster_block_recovery_on_unrecoverable_options, PGC_SIGHUP, 0, NULL, NULL, NULL);

	/*
	 * PGRAC: spec-4.11 D1 — online single-thread recovery (2 NEW GUC).
	 */
	DefineCustomBoolVariable(
		"cluster.online_thread_recovery",
		gettext_noop("Let a survivor online-replay a dead WAL thread's data to shared "
					 "storage in the reconfig freeze window instead of waiting for the "
					 "dead node's cold restart."),
		gettext_noop("Dev default off (flips on at ship once the capability gate is "
					 "complete).  2-node scope only; requires a shared-data (cluster_fs) "
					 "backend; single-node and >2-node survivors are not applicable / not "
					 "supported.  fail-closed: a thread that cannot be replayed completely "
					 "stays frozen (never serves a stale page)."),
		&cluster_online_thread_recovery, false, PGC_SIGHUP, 0, NULL, NULL, NULL);

	DefineCustomEnumVariable(
		"cluster.thread_recovery_on_unrecoverable",
		gettext_noop("Action when a dead thread cannot be online-recovered."),
		gettext_noop("keep_frozen (default): result-returning fail-closed; the dead "
					 "thread's resources stay frozen and the survivor keeps running.  "
					 "panic: escalate to PANIC the survivor (operator opt-in)."),
		&cluster_thread_recovery_on_unrecoverable, CLUSTER_THREADREC_ACTION_KEEP_FROZEN,
		cluster_thread_recovery_on_unrecoverable_options, PGC_SIGHUP, 0, NULL, NULL, NULL);

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

	/*
	 * spec-5.1b D7: cluster.ges_mode_selfcheck was removed.  The frozen
	 * matrix drives the live GRD grant decision, so the matrix-vs-lock-
	 * conflict-table startup self-check is now unconditional and always
	 * FATAL on divergence (cluster_ges_mode_init); there is no off/warn
	 * tier because there is no safe way to continue past a drift.
	 */

	/*
	 * spec-5.4 D8 — SQ sequence lock tunables.
	 *
	 * sequence_default_cache is injected at CREATE SEQUENCE time when the
	 * user did not write a CACHE clause (init_params can tell the two
	 * apart); an explicit CACHE N is never overridden.  cache_floor_optin
	 * is an opt-in safety net (0 = off) that lifts an existing tiny
	 * seqcache at runtime without silently rewriting user semantics.
	 * refill_timeout_ms bounds the SQ(X) refill wait before nextval fails
	 * closed (reuses 53R70).
	 */
	DefineCustomIntVariable(
		"cluster.sequence_default_cache",
		gettext_noop("Default CACHE size injected into new sequences in cluster mode."),
		gettext_noop("Applied only when CREATE SEQUENCE omits CACHE; an explicit CACHE N "
					 "is stored unchanged.  Larger values mean fewer cross-node refills."),
		&cluster_sequence_default_cache, 100, /* boot value */
		1, 1000000000,						  /* min / max */
		PGC_SUSET,							  /* superuser SET */
		0,									  /* flags */
		NULL, NULL, NULL);

	DefineCustomIntVariable(
		"cluster.sequence_cache_floor_optin",
		gettext_noop("Opt-in runtime floor for an existing sequence's CACHE size."),
		gettext_noop("0 disables (default): the runtime strictly honours catalog seqcache, "
					 "so an explicit CACHE 1 stays 1.  When > 0, sequences whose seqcache is "
					 "below this value refill in floor-sized batches without rewriting the "
					 "stored seqcache."),
		&cluster_sequence_cache_floor_optin, 0, /* boot value */
		0, 1000000000,							/* min / max */
		PGC_SIGHUP,								/* reload */
		0,										/* flags */
		NULL, NULL, NULL);

	DefineCustomIntVariable(
		"cluster.sequence_refill_timeout_ms",
		gettext_noop("Maximum wait for an SQ sequence segment refill before failing closed."),
		gettext_noop("Bounds the SQ(X) enqueue + authority grant + boundary writeback wait; "
					 "on timeout nextval raises 53R70 rather than risk a duplicate value."),
		&cluster_sequence_refill_timeout_ms, 30000, /* boot value */
		1000, 600000,								/* min / max */
		PGC_SIGHUP,									/* reload */
		GUC_UNIT_MS,								/* flags */
		NULL, NULL, NULL);

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
