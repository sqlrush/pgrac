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


#endif /* CLUSTER_GUC_H */
