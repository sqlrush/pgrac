/*-------------------------------------------------------------------------
 *
 * cluster_guc.h
 *	  pgrac cluster GUC registration entry point and exported variables.
 *
 *	  This header is the single source of truth for cluster GUC C
 *	  declarations.  Stage 0.13 introduces the registration mechanism
 *	  and activates the first cluster GUC, cluster_node_id (originally
 *	  a placeholder global in cluster_elog.c, see spec-0.9).  Future
 *	  cluster GUCs land here at the same time their owning subsystem
 *	  spec is implemented; see docs/cluster-guc-design.md §3.1 for the
 *	  Single Source of Truth registration roster.
 *
 *	  Responsibilities of this header:
 *
 *	  - Declare extern storage for every registered cluster GUC C
 *	    variable, so the rest of the cluster code can read its current
 *	    value without going through the SQL GUC layer.
 *	  - Declare cluster_init_guc(), the registration entry point that
 *	    PostmasterMain calls (under #ifdef USE_PGRAC_CLUSTER) right after
 *	    PG's built-in GUCs are loaded.
 *
 *	  Stage 0.13 does NOT register the ~24 cluster GUCs listed in the
 *	  background-process / error-codes design docs.  Each remaining
 *	  GUC is registered together with the spec that introduces its
 *	  first reader (CLAUDE.md rule 8).  That policy is documented in
 *	  docs/cluster-guc-design.md §1 and §7.
 *
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/include/cluster/cluster_guc.h
 *
 * NOTES
 *	  This is a pgrac-original file (no derivation from PostgreSQL).
 *	  The header is intentionally PG-free at the declaration level so
 *	  cluster_unit standalone tests can include it; the implementation
 *	  in cluster_guc.c does include "utils/guc.h" because
 *	  DefineCustomIntVariable() lives there.
 *
 *-------------------------------------------------------------------------
 */
#ifndef CLUSTER_GUC_H
#define CLUSTER_GUC_H


/*
 * cluster_init_guc -- register all cluster GUC variables.
 *
 *	Called from PostmasterMain after InitializeGUCOptions() has loaded
 *	PG's built-in GUCs.  Subsequent postgresql.conf parsing applies
 *	user-supplied values, so registration must happen first.
 *
 *	Idempotent within a process: PG's GUC layer rejects duplicate
 *	registrations with the same name, so calling this twice would
 *	abort startup.  PostmasterMain only calls it once.
 *
 *	Implementation lives in src/backend/cluster/cluster_guc.c.
 */
extern void cluster_init_guc(void);


/* ============================================================
 * Cluster GUC variables (storage owned by cluster_guc.c).
 *
 *	Declared here so that callers (e.g. CLUSTER_LOG in cluster_elog.h)
 *	can read the current value without locking.  Each value is updated
 *	by PG's GUC machinery on startup or reload.
 * ============================================================ */

/*
 * cluster_node_id -- numeric identifier of this node in the cluster.
 *
 *	-1  = unconfigured (running outside a cluster)
 *	0..127 = node id (range covers Stage 1+ planned 16-128 node clusters,
 *	         see CLAUDE.md AD-012 example 10).
 *
 *	context: PGC_POSTMASTER (requires server restart to change).
 */
extern int cluster_node_id;


/*
 * cluster_interconnect_tier -- which interconnect tier vtable to bind
 *	in cluster_ic_init.  Stored as int because PG's enum GUC machinery
 *	stores enum values in an int variable and maps them via the
 *	config_enum_entry array registered in cluster_guc.c.
 *
 *	0 (stub)  = no real wire traffic; target=self is no-op success,
 *	            target!=self ereports ERRCODE_FEATURE_NOT_SUPPORTED.
 *	1 (tier1) = TCP.
 *	2 (tier2) = RDMA baseline through the transport mux.
	 *	3 (tier3) = RDMA optimized tier; requests mlx5/direct-verbs and
	 *	            falls back according to cluster.interconnect_rdma_fallback.
 *
 *	context: PGC_POSTMASTER (tier change requires reinitialising the
 *	         interconnect stack; runtime SET is rejected).
 */
extern int cluster_interconnect_tier;


/*
 * cluster_config_file -- path to the pgrac.conf cluster topology file.
 *
 *	Default "pgrac.conf" (relative to postmaster cwd, typically PGDATA).
 *	Stage 2+ deployments often point this at shared storage (NFS / cloud
 *	multi-attach) so all nodes see an identical topology.
 *
 *	context: PGC_POSTMASTER (reload of topology is part of the future
 *	         reconfig protocol, not a simple file re-read; see
 *	         docs/cluster-conf-design.md §10 FAQ).
 */
extern char *cluster_config_file;


/*
 * cluster_wal_threads_dir -- shared-storage root of the per-thread WAL
 * stream layout (spec-4.1 D5).
 *
 *	Empty (default) keeps the flat pg_wal layout.  When set, postmaster
 *	startup requires $PGDATA/pg_wal to resolve to
 *	<dir>/thread_<cluster.node_id + 1> and validates the thread claim
 *	file; mismatches are FATAL 53RA0 / 53RA1 (cluster_wal_thread_init).
 *
 *	context: PGC_POSTMASTER (the WAL stream location cannot move while
 *	         the server runs).  check_hook: empty or absolute path.
 */
extern char *cluster_wal_threads_dir;

/*
 * cluster_recovery_stale_active_ms -- spec-4.3 recovery-plan staleness
 * window (ms) for ACTIVE slots; observational classification only.
 */
extern int cluster_recovery_stale_active_ms;

/*
 * cluster_recovery_workers_max -- spec-4.4 stream-validation worker cap
 * (dynamic bgworkers; 0 = plan-only).
 */
extern int cluster_recovery_workers_max;

/* spec-4.5 D9: merged recovery enable (default off) + worker wait. */
extern bool cluster_merged_recovery;
extern int cluster_recovery_merge_wait_timeout;

/* spec-5.59 D1: cross-node profiling switch (default off, zero hot-path cost). */
extern bool cluster_xnode_profile_enabled;

/* spec-6.4: ADG physical standby / read-only service configuration. */
#define CLUSTER_DG_ROLE_PRIMARY 0
#define CLUSTER_DG_ROLE_STANDBY 1

#define CLUSTER_DG_MODE_ASYNC 0
#define CLUSTER_DG_MODE_SYNC 1
#define CLUSTER_DG_MODE_MAX_AVAILABILITY 2

extern int cluster_dg_role;
extern int cluster_dg_mode;
extern bool cluster_enable_adg;
extern bool cluster_apply_master_election;
extern char *cluster_adg_rfs_conninfos;
extern int cluster_adg_primary_thread_count;
extern int cluster_adg_lag_threshold_sec;
extern int cluster_max_standby_delay;
extern int cluster_apply_master_switch_drain_ms;
extern int cluster_adg_lease_takeover_grace_ms;
extern int cluster_adg_barrier_interval_ms;
extern int cluster_wal_sender_timeout_sec;
extern int cluster_wal_receiver_timeout_sec;
/* spec-6.12c: resolver terminal memo switch (default off). */
extern bool cluster_page_scn_shortcut;

/* spec-6.12a: quiescent S-cache switch (default off). */
extern bool cluster_read_scache;

/* spec-6.12e1: GES release-side handoff verify switch (default off). */
extern bool cluster_ges_handoff;

/* spec-6.12b: cross-instance CR-server data plane (default off = 53R9G). */
extern bool cluster_crossnode_cr_data_plane;

/* spec-6.12g: block self-containment switch (default off = D11 deferral). */
extern bool cluster_block_self_contained;

/* spec-6.12e2: master->holder BAST nudge on live-X-holder deny (default
 * off = e1 release-side handoff only). */
extern bool cluster_ges_bast;

/* spec-6.12h: keep a Past Image on block transfer/invalidate (default
 * off = flush + drop the copy). */
extern bool cluster_past_image;

/* spec-6.12i: active-runtime cross-instance recycled-slot visibility
 * (undo-block CF fetch + live authority gate; default off = 53R97). */
extern bool cluster_crossnode_runtime_visibility;

/* spec-6.15 D1: xid space segmentation -- striped allocation (default
 * off = vanilla dense per-node xid allocation). */
extern bool cluster_xid_striping;
/* spec-6.15 D5/D3: herding slack (jump threshold; seed-floor headroom). */
extern int cluster_xid_herding_slack;

/*
 * spec-6.12d: instance space-affinity mode (default off).  static parks
 * the unconsumed tail of oversized HW grants as a per-node lease
 * (Q17-A extent-interior grouping); dynamic is spec-6.3 DRM territory
 * and is rejected at GUC check time until that ships.
 */
typedef enum ClusterSpaceAffinity {
	CLUSTER_SPACE_AFFINITY_OFF = 0,
	CLUSTER_SPACE_AFFINITY_STATIC,
	CLUSTER_SPACE_AFFINITY_DYNAMIC,
} ClusterSpaceAffinity;

extern int cluster_space_affinity;

/* spec-6.12d: per-lease grant-size cap in blocks (bloat bound input). */
extern int cluster_space_lease_blocks;


/*
 * cluster_injection_points -- comma-separated names to auto-arm at startup.
 *
 *	Empty by default.  Each name is armed with fault_type=WARNING; the
 *	cluster_inject_fault() SRF can later override per-point behaviour.
 *
 *	context: PGC_SUSET (runtime SET allowed for testing; auto-arm on
 *	         next backend startup).
 *
 *	See docs/error-injection-design.md §6 and spec-0.27-error-injection.md.
 */
extern char *cluster_injection_points;


/*
 * cluster_shared_storage_backend -- which cluster_shared_fs backend
 *	the postmaster activates at startup.
 *
 *	Boot default: 0 (CLUSTER_SHARED_FS_BACKEND_STUB; every cluster
 *	            shared-storage call ereports FEATURE_NOT_SUPPORTED).
 *	Range:        ClusterSharedFsBackendId enum (0..15).
 *	context:      PGC_POSTMASTER (changes require restart).
 *
 *	See docs/cluster-shared-fs-design.md §4 and
 *	spec-1.1-shared-fs-skeleton.md.
 */
extern int cluster_shared_storage_backend;


/*
 * cluster_shared_data_dir -- shared data root for the cluster_fs
 *	(shared_fs) backend (spec-4.5a D2).
 *
 *	The shared_fs backend resolves every relation file under
 *	<shared_data_dir>/<relpathperm> so all nodes pointing at the same
 *	shared mount land on the same file.  Required (non-empty, absolute)
 *	when shared_storage_backend=cluster_fs; the startup cross-check lives
 *	in cluster_shared_fs_init.
 *
 *	Boot default: "" (empty -- inert for stub/local).
 *	context:      PGC_POSTMASTER (path frozen for the postmaster lifetime).
 */
extern char *cluster_shared_data_dir;

/*
 * cluster.controlfile_shared_authority -- spec-5.6 Da3 opt-in for the shared
 * pg_control authority (default off, Hardening v1.0.1).
 *	context: PGC_POSTMASTER.
 */
extern bool cluster_controlfile_shared_authority;


/*
 * cluster_shared_storage_uuid -- optional external-preset identity for the
 *	cluster_fs shared root (spec-4.5a D2).  Matched against the shared-root
 *	sentinel; empty means the first node generates a random uuid.
 *
 *	Boot default: "" (empty).
 *	context:      PGC_POSTMASTER.
 */
extern char *cluster_shared_storage_uuid;

/* spec-6.0a: raw block-device backend configuration. */
typedef enum ClusterStorageFenceDriver {
	CLUSTER_STORAGE_FENCE_DRIVER_DISABLED = 0,
	CLUSTER_STORAGE_FENCE_DRIVER_AUTO = 1,
	CLUSTER_STORAGE_FENCE_DRIVER_SCSI3_PR = 2,
} ClusterStorageFenceDriver;

extern char *cluster_block_device_path;
extern bool cluster_block_device_use_odirect;
extern int cluster_storage_fence_driver;


/*
 * cluster_smgr_user_relations -- opt-in switch routing user-relation
 *	smgr operations through cluster_smgr (smgr_which=1) instead of
 *	md.c (smgr_which=0).  Default off keeps stage 0 / 1.1 behaviour
 *	unchanged for upgraders who haven't explicitly opted in.
 *
 *	Boot default: false (md.c for everything).
 *	context:      PGC_POSTMASTER (smgr selection is per-relation
 *	              cached at smgropen, so changes need a restart).
 *
 *	Startup-time cross-check: this GUC = on combined with
 *	shared_storage_backend = stub is incoherent (cluster_smgr would
 *	have no real backend); cluster_shared_fs_init ereports FATAL
 *	when the combination is detected.  See spec-1.2 §3.2.
 */
extern bool cluster_smgr_user_relations;

/*
 * cluster_shared_catalog -- spec-6.14 D1 master switch for the shared
 *	system-catalog single authority.  Default off (per-node catalog copies,
 *	byte-identical to current behaviour).  On requires smgr_user_relations,
 *	shared_data_dir and controlfile_shared_authority (startup vet in
 *	cluster_shared_fs_init) and every node must agree (join consistency
 *	check).  context: PGC_POSTMASTER.
 */
extern bool cluster_shared_catalog;

/*
 * cluster_oid_lease_size -- spec-6.14 D6.  Per-node OID lease block size for
 *	the shared OID authority (default 8192, range 1024..1048576).  Only
 *	meaningful when cluster_shared_catalog is on.  context: PGC_POSTMASTER.
 */
extern int cluster_oid_lease_size;


/*
 * cluster_shmem_max_regions -- capacity of the cluster shmem region
 *	registry (since stage 1.3).
 *
 *	Boot default: 64 (covers stage 0.14 cluster_ctl + 0.19 cluster_conf
 *	+ all 12 reserved regions in cluster-shmem-design.md §3.2 with a
 *	wide safety margin).
 *	Range:        [8, 256].
 *	context:      PGC_POSTMASTER (registry array is palloc'd once at
 *	              postmaster init from this value).
 *
 *	See docs/cluster-shmem-design.md §9.4 and
 *	spec-1.3-shmem-region-registry.md §2.2.
 */
extern int cluster_shmem_max_regions;


/* ----------
 * cluster.grd_max_entries (spec-2.15)
 *
 *	Maximum number of cluster_grd entry table slots.  Default 0 means
 *	skeleton mode: entry HTAB not allocated, cluster_grd_entry_lookup_
 *	or_create() returns CLUSTER_GRD_ENTRY_NOT_READY.  Non-zero enables
 *	ShmemInitHash allocation.
 *
 *	v0.4 P1.1:  HASH_PARTITION=4096 forces dynahash nbuckets >= 4096,
 *	so hash_init_max_size = Max(GUC, 4096) and shmem reservation comes
 *	from hash_estimate_size(Max(GUC, 4096), sizeof(ClusterGrdEntry)) —
 *	even GUC=16 reserves ~3-5 MB shmem.
 *	PGC_POSTMASTER.
 * ----------
 */
extern int cluster_grd_max_entries;
extern bool cluster_grd_entry_reclaim;
extern int cluster_grd_entry_reclaim_max_per_sweep;

/* spec-2.16 D12 + v0.5 P1.5:  cluster.ges_request_timeout_ms +
 * effective_timeout helper.  range [1, 600000];  default 60000. */
extern int cluster_ges_request_timeout_ms;
extern int cluster_ges_effective_timeout_ms(int lock_timeout_ms);

/* spec-5.6 Dc4b:  cluster.cf_enqueue_timeout_ms.  range [1000, 600000];
 * default 30000.  CF X/S grant wait before fail-closed (53R70). */
extern int cluster_cf_enqueue_timeout_ms;

/* spec-5.3 D10 — TM cross-node convert tunables.
 *   cluster.ges_convert_timeout_ms:  finite convert wait (1000..600000; 30000).
 *   cluster.tm_convert_mode:  Oracle DLM convert (default) vs PG additive. */
typedef enum ClusterTmConvertMode {
	CLUSTER_TM_CONVERT_MODE_CONVERT = 0,
	CLUSTER_TM_CONVERT_MODE_ADDITIVE = 1
} ClusterTmConvertMode;
extern int cluster_ges_convert_timeout_ms;
extern int cluster_tm_convert_mode;

/* spec-2.23 D11 NEW: coordinator REPORT collect timeout + reply wait cap. */
extern int cluster_lmd_probe_collect_timeout_ms;
extern int cluster_ges_reply_wait_max_entries;

/* spec-4.6 D4/D1:  failure-driven remaster tunables.
 * grd_remaster_wait_ms — backend short wait on a FROZEN/REBUILDING shard
 *   before fail-closed 53R9I (default 200ms;  SIGHUP).
 * grd_rebuild_timeout_ms — LMON holder-rebuild barrier deadline;  expiry
 *   keeps affected shards frozen (fail-closed) + re-broadcasts (default
 *   5000ms;  SIGHUP). */
extern int cluster_grd_remaster_wait_ms;
extern int cluster_grd_rebuild_timeout_ms;
extern int cluster_hw_remaster_retry_backoff_ms;
extern int cluster_hw_remaster_retry_max_attempts;

/* spec-5.4 D8: SQ sequence lock tunables. */
extern int cluster_sequence_default_cache;
extern int cluster_sequence_cache_floor_optin;
extern int cluster_sequence_refill_timeout_ms;

/* spec-2.24 D11 NEW: LMD periodic cleanup sweep interval. */
extern int cluster_lmd_cleanup_sweep_interval_ms;

/* spec-2.25 D9 NEW: per-node native-lock probe tunables. */
extern int cluster_lms_native_lock_probe_max_inflight;
extern int cluster_lms_native_lock_probe_retry_interval_ms;
extern int cluster_lms_native_lock_probe_retry_budget;

/* spec-2.27 D4 NEW: GES retransmit + dedup HTAB tunables. */
extern int cluster_ges_retransmit_max_attempts;
extern int cluster_ges_dedup_max_entries;

/* spec-2.17 NEW GUCs(v0.6 frozen baseline). */
extern int cluster_ges_bast_retry_interval_ms;		  /* D11 */
extern int cluster_ges_bast_max_retries;			  /* D11 */
extern int cluster_ges_deadlock_check_interval_ms;	  /* D17 */
extern int cluster_ges_deadlock_chunk_timeout_ms;	  /* D25 */
extern int cluster_ges_deadlock_max_edges;			  /* D24 */
extern int cluster_ges_deadlock_max_vertices;		  /* D24 */
extern int cluster_ges_deadlock_max_in_flight_probes; /* D24 */
extern int cluster_ges_deadlock_tick_budget_us;		  /* D26 */


/* ----------
 * cluster.phase{1..4}_timeout (Stage 1.10, spec-1.10 §2.2)
 *
 *	Per-phase wall-clock deadlines (seconds) for postmaster startup
 *	phase machinery.  Exceeding the deadline triggers ereport(FATAL,
 *	errcode PGRAC_E_PHASE_TRANSITION_TIMEOUT) so postmaster startup
 *	fails cleanly (HC4 user 修订 4).  Default values match
 *	background-process-design.md §4.3 (60 / 30 / 600 / 30 seconds).
 *
 *	Stage 1.10 stub handlers do not naturally trigger timeouts; the
 *	cluster-startup-phase-N-enter inject point + sleep fault
 *	simulates a stuck phase for regression coverage (060 L9b).
 *	Real timeout enforcement activates in 1.11+ when phase handlers
 *	have actual work that can hang.
 *
 *	context:      PGC_POSTMASTER (read once when postmaster init runs
 *	              the phase driver; child backends inherit via fork).
 * ----------
 */
extern int cluster_phase1_timeout;
extern int cluster_phase2_timeout;
extern int cluster_phase3_timeout;
extern int cluster_phase4_timeout;


/*
 * cluster.lmon_main_loop_interval: LMON main-loop tick / WaitLatch
 * timeout in milliseconds (Stage 1.11 Sprint B; spec-1.11 D8).
 *
 *	Sprint A used a hardcoded LMON_MAIN_LOOP_INTERVAL_MS = 1000.  Sprint
 *	B exposes the tick interval as PGC_SIGHUP so operators can dial
 *	telemetry granularity at runtime: lower value -> finer
 *	last_liveness_tick_at resolution + faster shutdown response;
 *	higher value -> lower wakeup overhead (Sprint A LMON does no real
 *	work, so even 60s is fine).
 *
 *	context:      PGC_SIGHUP
 *	default:      1000 (1 second; matches Sprint A hardcoded baseline)
 *	range:        [100, 60000] (millisecond)
 */
extern int cluster_lmon_main_loop_interval;
extern int cluster_lmon_slow_iteration_warn_ms;


/*
 * cluster.lck_main_loop_interval (spec-1.12 Sprint B D8): same pattern
 * as cluster.lmon_main_loop_interval; controls LCK aux process tick.
 */
extern int cluster_lck_main_loop_interval;


/*
 * cluster.diag_main_loop_interval (spec-1.13 D8): same pattern as
 * cluster.lmon_main_loop_interval; controls DIAG aux process tick.
 */
extern int cluster_diag_main_loop_interval;

/*
 * spec-5.11 D7: Hang Manager (DIAG-hosted long-wait sampler) GUCs.  All
 * PGC_SIGHUP — the DIAG main loop re-reads them on the next tick.
 */
/* spec-5.13 D11: clean-leave reconfiguration opt-in + drain deadline. */
extern bool cluster_clean_leave_enabled;
extern int cluster_clean_leave_drain_timeout_ms;

extern bool cluster_hang_manager_enabled;
extern int cluster_hang_sample_interval_ms;
extern int cluster_hang_threshold_ms;
extern bool cluster_hang_dump_enabled;
extern int cluster_hang_max_chain_depth;
extern int cluster_hang_max_sampled;

/* spec-5.12 D6: Hang Manager disposition GUCs (all PGC_SIGHUP). */
extern int cluster_hang_resolution_mode; /* ClusterHangResolveMode */
extern int cluster_hang_resolution_confirm_rounds;
extern int cluster_hang_resolution_soft_timeout_ms;
extern int cluster_hang_resolution_max_per_round;
extern double cluster_hang_victim_w_age;
extern double cluster_hang_victim_w_rollback;
extern double cluster_hang_victim_w_blockers;


/*
 * cluster.cluster_stats_main_loop_interval (spec-1.14 D8): same
 * pattern as cluster.lmon_main_loop_interval; controls Cluster Stats
 * aux process tick.
 */
extern int cluster_cluster_stats_main_loop_interval;

/*
 * spec-2.5 D9 -- CSSD aux process tunables (PGC_POSTMASTER).
 *   cssd_main_loop_interval_ms : MainLoop WaitLatch timeout.
 *   cssd_heartbeat_interval_ms : per-tick broadcast period.
 *   cssd_dead_deadband_factor  : DEAD threshold = factor × heartbeat
 *                                interval (default 3 → 3s);SUSPECTED
 *                                at max(2, factor-1) × interval.
 */
extern int cluster_cssd_main_loop_interval_ms;
/* spec-3.13 D1: Undo Cleaner pass cadence / enable / batch GUCs. */
extern int cluster_undo_cleaner_interval_ms;
extern bool cluster_undo_cleaner_enabled;
extern int cluster_undo_cleaner_batch_segments;
/* spec-4.12a D1: record-segment ACTIVE -> COMMITTED drain on rollover (leak fix). */
extern bool cluster_undo_record_segment_commit_on_rollover;
extern int cluster_cssd_heartbeat_interval_ms;
extern int cluster_cssd_dead_deadband_factor;

/*
 * spec-2.6 Sprint A Step 4 D12: voting disk + quorum-lite.
 *
 *   cluster.voting_disks               -- CSV path list (default empty)
 *   cluster.quorum_poll_interval_ms    -- 500..30000, default 2000
 *   cluster.voting_disk_io_timeout_ms  -- 500..60000, default 5000
 *   cluster.voting_disk_size_bytes     -- 4096..1048576, default 328192
 *                                         (spec-6.15: (5 × 128 + 1) × 512)
 *
 * All PGC_POSTMASTER.  Per Q4 v0.2 lease semantics, backend
 * in_quorum() check uses 2 × quorum_poll_interval_ms as the lease
 * window (defends qvotec hung silent stale-OK).
 */
extern char *cluster_voting_disks;
extern int cluster_quorum_poll_interval_ms;
extern int cluster_voting_disk_io_timeout_ms;
extern int cluster_voting_disk_size_bytes;
extern bool cluster_online_join;					/* spec-5.15 D7 */
extern int cluster_join_convergence_timeout_ms;		/* spec-5.15 D7 */
extern bool cluster_online_node_removal;			/* spec-5.18 D13 */
extern int cluster_node_removal_cleanup_timeout_ms; /* spec-5.18 D13 */
extern bool cluster_join_remaster_enabled;			/* spec-5.16 D6 */

/*
 * spec-2.2 D7 -- Tier 1 TCP transport tuning (PGC_POSTMASTER).
 * Defaults / ranges per spec-2.2 §3.3 + §2.1.
 */
extern int cluster_interconnect_heartbeat_interval_ms;
extern int cluster_interconnect_connect_timeout_ms;
extern int cluster_interconnect_recv_timeout_ms;

/* spec-2.4 D9: chunked framing + TCP KeepAlive 5 GUC. */
extern int cluster_interconnect_payload_max_bytes;
extern int cluster_interconnect_chunk_reassembly_timeout_ms;
extern int cluster_interconnect_tcp_keepidle_sec;
extern int cluster_interconnect_tcp_keepintvl_sec;
extern int cluster_interconnect_tcp_keepcnt;
extern int cluster_interconnect_rdma_fallback;
extern int cluster_interconnect_rdma_provider;
extern int cluster_interconnect_rdma_completion;
extern int cluster_interconnect_rdma_busypoll_us;
extern bool cluster_interconnect_rdma_crc_offload;
extern int cluster_interconnect_rdma_inline_max;
extern int cluster_interconnect_rdma_max_send_wr;

/*
 * cluster.boc_sweep_interval_ms (spec-1.17 D4): walwriter BOC sweep
 * staleness target in ms.  Range [1, 1000]; default 1.  walwriter wake
 * rate caps actual sweep frequency (Min(WalWriterDelay, this)).
 */
extern int cluster_boc_sweep_interval_ms;


/*
 * cluster.enabled: runtime cluster mode gate (Stage 1.11 Sprint B; HC4
 * 闭环).
 *
 *	Sprint A used compile-time #ifdef USE_PGRAC_CLUSTER as the de-facto
 *	cluster gate (HC4 GUC + L9 deferred to Sprint B per spec-1.11 §1.4
 *	Q-amend #4).  Sprint B introduces the runtime PGC_POSTMASTER GUC
 *	so an enable-cluster build can still be run as a single-instance
 *	non-cluster postgres without spawning LMON or other Stage 1.11+
 *	cluster background processes.
 *
 *	Phase 1 driver reads this GUC and falls back to the spec-1.10 stub
 *	(no LMON spawn) when cluster.enabled = false (HC4).
 *
 *	context:      PGC_POSTMASTER (read once at postmaster startup;
 *	              children inherit via fork)
 *	default:      true (cluster mode is the default for --enable-cluster
 *	              builds; non-cluster builds never compile this GUC)
 */
extern bool cluster_enabled;

/* spec-6.5: cluster-aware backup / restore / PITR target configuration. */
#define CLUSTER_RECOVERY_TARGET_ACTION_PAUSE 0
#define CLUSTER_RECOVERY_TARGET_ACTION_PROMOTE 1
#define CLUSTER_RECOVERY_TARGET_ACTION_SHUTDOWN 2

#define CLUSTER_BACKUP_MANIFEST_CHECKSUM_OFF 0
#define CLUSTER_BACKUP_MANIFEST_CHECKSUM_CRC32C 1

extern char *cluster_recovery_target_scn;
extern char *cluster_recovery_target_cluster_time;
extern char *cluster_recovery_target_name;
extern int cluster_recovery_target_action;
extern bool cluster_enable_pitr_restore_points;
extern int cluster_pitr_restore_point_interval_ms;
extern int cluster_restore_point_drain_timeout_ms;
extern int cluster_backup_wal_retention;
extern int cluster_backup_parallel_channels;
extern int cluster_backup_manifest_checksums;

/* spec-3.12 D5: own-instance undo/TT-slot retention horizon gate (default on). */
extern bool cluster_undo_retention_horizon_enabled;


/*
 * cluster.undo_segments_per_instance (spec-1.22 D7).
 *
 *	Reserved undo segment count per cluster instance.  Stage 1.22
 *	declares the GUC + default 16; real consumption deferred to
 *	feature-117 retention activation.
 *
 *	context: PGC_POSTMASTER
 *	default: 16
 *	range:   [1, 1024]
 */
extern int cluster_undo_segments_per_instance;

/*
 * spec-3.7 D9 NEW GUCs (Q9=A keep existing undo_segments_per_instance default 16):
 *   cluster.undo_tablespace_path     -- relative to PGDATA (default "pg_undo")
 *   cluster.undo_segment_size_mb     -- per segment file size (default 32)
 *   cluster.undo_record_inline_max_bytes -- max inline pre-image (default 1024)
 */
extern char *cluster_undo_tablespace_path;
extern int cluster_undo_segment_size_mb;
extern int cluster_undo_record_inline_max_bytes;
extern int cluster_undo_extent_blocks; /* spec-3.18 D3 extent granularity */

/*
 * spec-3.8 D9 NEW GUCs (registered in Step 7):
 *   cluster.undo_segments_max_per_instance    -- hard cap per-instance pool
 *                                                (default 256, range 16..256
 *                                                = CLUSTER_UNDO_SEGS_PER_INSTANCE
 *                                                linkdb SSOT;  F2 codex review)
 *   cluster.undo_segment_create_timeout_ms    -- segment file create + initial
 *                                                fsync elapsed-time guard
 *                                                (default 5000ms, range 100..60000)
 */
extern int cluster_undo_segments_max_per_instance;
extern int cluster_undo_segment_create_timeout_ms;

/*
 *   cluster.cr_chain_walk_max_steps (spec-3.9 D1) -- CR block construction
 *                                                    chain walker single-call
 *                                                    hard cap (default 4096,
 *                                                    range 64..65536, SIGHUP)
 */
extern int cluster_cr_chain_walk_max_steps;

/*
 *   cluster.cr_mvcc_gate (spec-3.9 D5) -- master switch for the own-instance
 *                                         CR 3-tier MVCC short-circuit gate
 *                                         (default off; PGC_USERSET;
 *                                         experimental pending codereview)
 */
extern bool cluster_cr_mvcc_gate;

/* cluster.cr_gate_no_peer_fastpath (spec-3.24 D1): a no-peer + session-local
 * cluster snapshot skips the CR/SCN visibility fork and uses the PG-native MVCC
 * body (AD-012 例外 9 row #1).  Default ON (the D1 differential + clean-CI Dfp
 * stop gate proved equivalence); CR-specific single-node tests pin it off. */
extern bool cluster_cr_gate_no_peer_fastpath;

/* spec-5.54 D5: tuple-level / verdict-only CR read fast path master switch
 * (default OFF, PGC_USERSET).  Read by the cluster_cr.c gate. */
extern bool cluster_cr_tuple_level_fastpath;

/* cluster.tt_durable_lookup (spec-3.11 D7): durable TT slot read-side resolve
 * on overlay miss / watermark gate (default on; PGC_USERSET). */
extern bool cluster_tt_durable_lookup;
/* spec-4.8 D1: resolve crash-left ACTIVE TT slots to ABORTED at startup
 * (default on; PGC_POSTMASTER). */
extern bool cluster_tt_recovery_resolve_active;
typedef enum ClusterCfDelayedCleanoutMode {
	CLUSTER_CF_DELAYED_CLEANOUT_OFF = 0,
	CLUSTER_CF_DELAYED_CLEANOUT_READER,
	CLUSTER_CF_DELAYED_CLEANOUT_EAGER
} ClusterCfDelayedCleanoutMode;

/* spec-6.2 Cache Fusion terminal authority + Smart Fusion. */
extern bool cluster_cf_terminal_authority;
extern int cluster_cf_delayed_cleanout;
extern bool cluster_smart_fusion;
extern int cluster_smart_fusion_tier_min;
extern int cluster_smart_fusion_commit_brake_timeout_ms;
extern int cluster_smart_fusion_origin_durable_gossip_ms;
extern bool cluster_smart_fusion_failclosed_requested(void);


/*
 * cluster.allow_single_node (spec-2.1 D1; Stage 2.1 backward-compat
 * mode gate).
 *
 *	Stage 2.1 introduces strict multi-node validation: pgrac.conf is
 *	required and cluster.node_id must be in valid range 0..127.  The
 *	allow_single_node = on default permits Stage 1.X single-node
 *	fallback (no pgrac.conf, cluster.node_id = -1) so that frozen
 *	Stage 1 specs (1.0-1.23) keep working unchanged.  Set to off to
 *	enforce strict mode (production deployments).
 *
 *	BOUNDARY INVARIANT (spec-2.1 §3.5; Q1 user 反审 caveat):
 *	allow_single_node = on ONLY permits fallback when multi-node
 *	configuration is ABSENT.  It does NOT downgrade malformed or
 *	explicit multi-node configuration errors:
 *	- pgrac.conf 不存在 + allow=on -> single-node fallback + LOG
 *	- pgrac.conf malformed (collision / out-of-range / etc) -> FATAL
 *	  regardless of allow_single_node value
 *	- pgrac.conf 存在 + cluster.node_id 不在 conf -> FATAL same
 *	allow=on is a Stage 2 development / backward-compat mode, NOT a
 *	production strict mode.
 *
 *	context: PGC_POSTMASTER (boot-time only)
 *	default: true (Stage 2.1 backward compat; spec-2.31 acceptance
 *	         may flip to false for strict mode)
 */
extern bool cluster_allow_single_node;

/*
 * cluster.lmd_enabled (spec-2.19 D12).
 *
 *	context: PGC_POSTMASTER (HC1 startup-time fallback;runtime SET
 *	         rejected by PGC_POSTMASTER enforcement)
 *	default: true
 *
 *	Off → spec-2.17 caller-side 4-node deadlock-detection legacy path
 *	active (LMD not forked).  On → postmaster forks LMD at PM_RUN,
 *	caller-side legacy hard-disabled once cluster_lmd_is_ready() returns
 *	true (exact state == LMD_READY).  Single ownership path硬契约
 *	(HC1 / v0.2 P1.3):enabled=on but LMD not READY → backend caller
 *	raises SQLSTATE 53R81 cluster_lmd_unavailable (silent fallback
 *	to caller-side legacy forbidden).
 */
extern bool cluster_lmd_enabled;

/*
 * cluster.lms_enabled (spec-2.20 D12).
 *
 *	context: PGC_POSTMASTER
 *	default: true
 *
 *	Off → spec-2.17 caller-side legacy path走 PG-native LockAcquire skip
 *	cluster gate.  On → cluster-aware lock acquires routed through
 *	7-step state machine + LMS grant decision body(spec-2.20 D3/D4)。
 *	Single ownership path硬契约(HC1):enabled=on 但 LMS not READY →
 *	backend caller raises SQLSTATE 53R80 cluster_lms_unavailable。
 */
extern bool cluster_lms_enabled;

/*
 * cluster.lock_acquire_cluster_path (spec-2.21 D2).
 *
 *	context: PGC_POSTMASTER
 *	default: true
 *
 *	Emergency bypass — when false, all PG LockAcquireExtended calls skip
 *	the cluster gate and use PG-native path only (even when cluster_enabled
 *	and IsClusterLockTag are true).  Use only as P0 incident response;
 *	production setting is true.
 */
extern bool cluster_lock_acquire_cluster_path;

/*
 * cluster.local_fast_path_enabled (spec-2.21 D2).
 *
 *	context: PGC_SIGHUP
 *	default: true
 *
 *	When false, S3 local-fast-path 5-check is skipped and all cluster-
 *	aware lock acquires take the remote-master path (perf degradation
 *	~10x vs spec-1.23 baseline; for fault-injection / chaos testing).
 */
extern bool cluster_local_fast_path_enabled;

/*
 * cluster.advisory_lock_enabled (spec-5.5 D7).
 *
 *	context: PGC_SUSET
 *	default: true
 *
 *	Master switch for cross-node advisory (user) lock globalization.  When
 *	false, both session- and xact-scoped pg_advisory_lock* route PG-native
 *	(single-node semantics) — a forensic/test-only UNSAFE downgrade that
 *	silently disables cross-node advisory mutual exclusion.  Read by the
 *	inline cluster_lock_should_globalize() advisory branch.
 */
extern bool cluster_advisory_lock_enabled;
extern bool cluster_relation_extend_lock_enabled; /* spec-5.7 HW */
extern bool cluster_tablespace_ddl_lock_enabled;  /* spec-5.7 TT */
extern bool cluster_object_reuse_flush_enabled;	  /* spec-5.7 KO */
extern bool cluster_touched_peers_trace;		  /* spec-5.14 D6 diag */

/*
 * cluster.lmd_max_wait_edges (spec-2.22 D9).
 *
 *	context: PGC_POSTMASTER
 *	default: 1024 (min 64, max 65536)
 *
 *	Cap on LMD wait-for graph edges.  Overflow → HC12 fail-closed
 *	(submit returns false; caller ereport 53R82).  Severely禁止 fallback
 *	PG local deadlock detector (blind wait across cluster).
 */
extern int cluster_lmd_max_wait_edges;

/*
 * cluster.lmd_scan_interval_ms (spec-2.22 D9).
 *
 *	context: PGC_SIGHUP
 *	default: 1000 (min 50, max 60000)
 *
 *	LmdMain Tarjan scan period.  Lower = faster deadlock detection,
 *	higher CPU.  CV wake on edge submission also triggers scan
 *	out-of-band (Q3 — timer 是 safety net,CV 是 low-latency path).
 */
extern int cluster_lmd_scan_interval_ms;


/*
 * cluster.deadlock_detection_enabled / global_dd_interval_ms /
 * deadlock_confirm_interval_ms (spec-5.8 D3 — coordinator-driven cross-node
 * deadlock detection).
 *
 *	deadlock_detection_enabled (bool, default on, PGC_SIGHUP): master gate
 *	  for the coordinator cross-node scan.  Off => only the per-node local
 *	  Tarjan scan runs; cross-node deadlocks rely on finite enqueue timeouts.
 *	global_dd_interval_ms (default 2000, PGC_SIGHUP): coordinator scan period
 *	  (only the HC16 lowest-active node runs it).
 *	deadlock_confirm_interval_ms (default 500, PGC_SIGHUP): delay between the
 *	  two coordinator confirm rounds (Rule 8.A transient filter).
 */
extern bool cluster_lmd_deadlock_detection_enabled;
extern int cluster_lmd_global_dd_interval_ms;
extern int cluster_lmd_deadlock_confirm_interval_ms;

/*
 * spec-5.9 D10 — deadlock victim policy + cancel robustness GUCs.
 *	cancel_ack_timeout_ms (default 1000): coordinator retransmit threshold for a
 *	  cross-node cancel with no CANCEL_ACK.
 *	cancel_max_retransmit (default 3): bounded retransmits before alternate-
 *	  victim escalation (0 = 5.8 fire-and-forget).
 *	victim_repeat_window_ms (default 5000): anti-thrash recent-victim window.
 */
extern int cluster_cancel_ack_timeout_ms;
extern int cluster_cancel_max_retransmit;
extern int cluster_victim_repeat_window_ms;


/*
 * cluster.gcs_reply_timeout_ms (spec-2.33 D8; HC85).
 *
 *	type: int   context: PGC_SUSET
 *	default: 5000 (min 100, max 60000)
 *
 *	GCS block-ship request reply timeout in milliseconds.  Sender uses
 *	ConditionVariableTimedSleep with this deadline; on expiry the request
 *	is cleaned up and ereport(ERRCODE_QUERY_CANCELED) is raised with an
 *	errhint pointing to spec-2.34 retransmit (HC86 — retry not in scope).
 *	PGC_SUSET (not USERSET) so unprivileged users cannot perturb the
 *	cache-fusion hot path; superusers + test fixtures may tune.
 */
extern int cluster_gcs_reply_timeout_ms;


/*
 * spec-2.34 D8 — GCS block reliability hardening (3 NEW GUC).
 *
 *	cluster.gcs_block_retransmit_max_retries
 *	  type: int   context: PGC_SUSET
 *	  default: 4 (min 0, max 8)
 *	  Number of retry attempts after initial GCS_BLOCK_REQUEST send fails
 *	  to receive a reply within cluster.gcs_reply_timeout_ms.  Each retry
 *	  uses exponential backoff (see initial_backoff_ms).  N=0 disables
 *	  retransmit (equivalent to spec-2.33 behavior).
 *
 *	cluster.gcs_block_retransmit_initial_backoff_ms
 *	  type: int   context: PGC_SUSET
 *	  default: 10 (min 1, max 5000;  spec-7.2 D1 lowered from 100/10)
 *	  Backoff before retry 1.  Subsequent retries double:  10 → 20 →
 *	  40 → 80 ms (with default max_retries=4, total backoff = 150 ms).
 *	  The per-attempt reply wait stays cluster.gcs_reply_timeout_ms;
 *	  this only paces the fast-deny / timeout retry cadence (HC96).
 *
 *	cluster.gcs_block_dedup_max_entries
 *	  type: int   context: PGC_POSTMASTER
 *	  default: 1024 (min 256, max 16384)
 *	  Per-node cap for the master-side dedup HTAB.  Each entry occupies
 *	  sizeof(GcsBlockDedupEntry) = 8312B, so default cap → ~8.4 MB shmem
 *	  on each node acting as GCS block-ship master.  HASH_ENTER_NULL on
 *	  cap → DENIED_DEDUP_FULL fail-closed (sender retries via HC96).
 *
 *	HC97 retry math + HC92 cap + HC93 GC + HC96 transient.
 */
extern int cluster_gcs_block_retransmit_max_retries;
extern int cluster_gcs_block_retransmit_initial_backoff_ms;

/* PGRAC: spec-4.7a D2/Q8 — hold-until-revoked node-level PCM cache kill-switch
 * (default on).  See cluster_guc.c for semantics. */
extern bool cluster_gcs_block_local_cache;
extern bool cluster_tx_enqueue_wait_enabled; /* spec-5.2 D4 */

/*
 *	cluster.ic_duty_lazy
 *	  type: bool   context: PGC_SIGHUP   default: on
 *	  spec-7.2 D1 — gate the lazy-able (queue-consumption) LMON duty
 *	  families behind producer dirty marks + a >= 1 Hz floor.  Never-lazy
 *	  correctness families are unaffected.  Off = pre-7.2 behavior.
 */
extern bool cluster_ic_duty_lazy;
extern bool cluster_crossnode_write_write; /* spec-7.1a D0 */
extern int cluster_gcs_block_dedup_max_entries;

/* PGRAC: spec-4.7 D1 — bounded wait (ms) on a RECOVERING block resource
 * before fail-closing 53R9L.  See cluster_guc.c for semantics. */
extern int cluster_gcs_block_recovery_wait_ms;

/*
 * PGRAC: spec-2.36 D8 — 3 NEW GUC for CF 3-way protocol.
 *
 *	cluster.gcs_block_invalidate_ack_timeout_ms — HC116;  master single
 *	INVALIDATE_ACK wait deadline (default 1500).
 *	cluster.gcs_block_starvation_backoff_ms — HC117;  reader DENIED_
 *	PENDING_X backoff base (default 100).
 *	cluster.gcs_block_starvation_max_retries — HC117;  reader retry
 *	budget (default 8).
 */
extern int cluster_gcs_block_invalidate_ack_timeout_ms;
extern int cluster_gcs_block_starvation_backoff_ms;
extern int cluster_gcs_block_starvation_max_retries;

/*
 * PGRAC: spec-2.37 D11 — 1 NEW enum GUC for lost-write detection action.
 *
 *	cluster.gcs_block_lost_write_action — HC131:
 *	  CLUSTER_GCS_LOST_WRITE_ACTION_ERROR=0 (default)
 *	  CLUSTER_GCS_LOST_WRITE_ACTION_WARN=1 (staging/diagnostic)
 */
extern int cluster_gcs_block_lost_write_action;

/*
 * PGRAC: spec-2.38 D8 — 3 NEW GUC for SI Broadcaster skeleton.
 *
 *	cluster.sinval_broadcast_batch_size — outbound drain batch upper
 *	  bound (default 32, range 1..CLUSTER_SINVAL_BATCH_MAX).
 *	cluster.sinval_broadcast_batch_timeout_ms — main loop WaitLatch
 *	  timeout (default 10);  PGC_SIGHUP only.
 *	cluster.sinval_broadcast_max_queue_size — ring buffer capacity for
 *	  both outbound + inbound queues (default 1024).
 */
extern int cluster_sinval_broadcast_batch_size;
extern int cluster_sinval_broadcast_batch_timeout_ms;
extern int cluster_sinval_broadcast_max_queue_size;

/* spec-2.39 D12:  3 NEW GUC for ack/barrier production gate. */
typedef enum ClusterSinvalAckMode {
	CLUSTER_SINVAL_ACK_MODE_NONE = 0,
	CLUSTER_SINVAL_ACK_MODE_PEER_ENQUEUED = 1
} ClusterSinvalAckMode;
extern int cluster_sinval_ack_mode;
extern int cluster_sinval_ack_timeout_ms;
extern int cluster_sinval_ack_wait_slots;

/* spec-3.1 D8:  2 NEW GUC for TT status overlay (D2). */
extern int cluster_tt_status_overlay_max_entries;
extern int cluster_tt_status_overlay_ttl_ms;

/* PGRAC spec-3.5 D5:  bounded depth for SUBCOMMITTED parent chain follow. */
extern int cluster_subtrans_max_chain_depth;

/* PGRAC spec-3.6 D9:  MULTIXACT reader/member-resolution foundation GUCs. */
extern int cluster_multixact_member_overlay_max_members;
extern int cluster_multixact_member_overlay_max_entries;
extern int cluster_multixact_hint_outbound_slots;

/* spec-3.2 D7:  2 NEW GUC for cross-node TT status hint wire propagation. */
typedef enum ClusterTTStatusHintEmitMode {
	CLUSTER_TT_STATUS_HINT_EMIT_DISABLED = 0,
	CLUSTER_TT_STATUS_HINT_EMIT_ALL_STATUS = 1
} ClusterTTStatusHintEmitMode;
extern int cluster_tt_status_hint_outbound_capacity;
extern int cluster_tt_status_hint_emit_mode;

#ifdef ENABLE_INJECTION
/* spec-3.2 D5b:  test-only GUC (production binary 0 触达). */
extern bool cluster_test_force_visibility_cluster_path;
#endif


#endif /* CLUSTER_GUC_H */
