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
 *	1 (tier1) = TCP, lands in Stage 2.
 *	2 (tier2) = RDMA optimized, lands in Stage 6+.
 *	3 (tier3) = RDMA production-grade, lands in Stage 6+.
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

/* spec-2.16 D12 + v0.5 P1.5:  cluster.ges_request_timeout_ms +
 * effective_timeout helper.  range [1, 600000];  default 60000. */
extern int cluster_ges_request_timeout_ms;
extern int cluster_ges_effective_timeout_ms(int lock_timeout_ms);


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
extern int cluster_cssd_heartbeat_interval_ms;
extern int cluster_cssd_dead_deadband_factor;

/*
 * spec-2.6 Sprint A Step 4 D12: voting disk + quorum-lite.
 *
 *   cluster.voting_disks               -- CSV path list (default empty)
 *   cluster.quorum_poll_interval_ms    -- 500..30000, default 2000
 *   cluster.voting_disk_io_timeout_ms  -- 500..60000, default 5000
 *   cluster.voting_disk_size_bytes     -- 4096..1048576, default 65536
 *
 * All PGC_POSTMASTER.  Per Q4 v0.2 lease semantics, backend
 * in_quorum() check uses 2 × quorum_poll_interval_ms as the lease
 * window (defends qvotec hung silent stale-OK).
 */
extern char *cluster_voting_disks;
extern int cluster_quorum_poll_interval_ms;
extern int cluster_voting_disk_io_timeout_ms;
extern int cluster_voting_disk_size_bytes;

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


#endif /* CLUSTER_GUC_H */
