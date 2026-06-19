/*-------------------------------------------------------------------------
 *
 * cluster_shmem.c
 *	  pgrac cluster shared-memory registration and control block.
 *
 *	  Stage 0.14 established the cluster shmem framework (PG-side hook
 *	  points + ClusterShmemCtl 64-byte control block) using a hard-coded
 *	  dispatch model.  Stage 1.3 evolves the dispatcher into a region
 *	  registry: each subsystem declares a static const ClusterShmemRegion
 *	  and registers it via cluster_shmem_register_region during postmaster
 *	  init.  cluster_request_shmem / cluster_init_shmem then iterate the
 *	  registry and dispatch by table.
 *
 *	  External ABI is unchanged: cluster_request_shmem,
 *	  cluster_init_shmem, cluster_shmem_size, and the ClusterShmem global
 *	  pointer all keep the same signatures and lifetimes that 0.14
 *	  shipped.  The two PG core hook points (miscinit.c
 *	  process_shmem_requests and ipci.c CreateSharedMemoryAndSemaphores)
 *	  call the same entry points -- only their internals refactored.
 *
 *	  Why one orchestration point per subsystem instead of letting each
 *	  subsystem edit PG core files directly: keeps the PGRAC
 *	  MODIFICATIONS to PG paths to the two single-line callbacks at the
 *	  top of this file.  Stage 1.3 takes this further -- new cluster
 *	  subsystems do not even touch this file: they call
 *	  cluster_shmem_register_region from their own *_init() helper.
 *
 *	  See docs/cluster-shmem-design.md §9 (region registry) and §10
 *	  (diagnostic views) for the full design.
 *
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/backend/cluster/cluster_shmem.c
 *
 * NOTES
 *	  This is a pgrac-original file (no derivation from PostgreSQL).
 *	  All exported symbols use the cluster_ prefix.  ShmemInitStruct
 *	  region names use the "pgrac " prefix to avoid colliding with
 *	  PG-internal or user extension names.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "miscadmin.h" /* IsUnderPostmaster */
#include "storage/shmem.h"
#include "utils/memutils.h" /* TopMemoryContext */
#include "utils/timestamp.h"

#include "cluster/cluster_conf.h"
#include "cluster/cluster_remote_xact.h" /* PGRAC: spec-4.5a G5 SLRU */ /* cluster_conf_shmem_size / init */
#include "cluster/cluster_elog.h"										/* CLUSTER_LOG */
#include "cluster/cluster_guc.h"			/* cluster_node_id / cluster_shmem_max_regions */
#include "cluster/cluster_ic.h"				/* cluster_ic_init / shutdown (stage 0.18) */
#include "cluster/cluster_ic_tier1.h"		/* cluster_ic_tier1_shmem_register (spec-2.2 D3) */
#include "cluster/cluster_cssd.h"			/* cluster_cssd_shmem_register (2.5 Sprint A) */
#include "cluster/cluster_undo_cleaner.h"	/* cluster_undo_cleaner_shmem_register (3.13) */
#include "cluster/cluster_diag.h"			/* cluster_diag_shmem_register (1.13 Sprint A) */
#include "cluster/cluster_inject.h"			/* CLUSTER_INJECTION_POINT */
#include "cluster/cluster_lck.h"			/* cluster_lck_shmem_register (1.12 Sprint A) */
#include "cluster/cluster_epoch.h"			/* cluster_epoch_shmem_register (2.4) */
#include "cluster/cluster_scn.h"			/* cluster_scn_shmem_register (1.15) */
#include "cluster/cluster_ges.h"			/* cluster_ges_shmem_register (spec-2.13) */
#include "cluster/cluster_ges_reply_wait.h" /* cluster_ges_reply_wait_shmem_register (spec-2.23 D1) */
#include "cluster/cluster_grd.h"			/* cluster_grd_shmem_register (spec-2.14) */
#include "cluster/cluster_grd_pending.h"	/* cluster_grd_pending_shmem_register (spec-2.16 D3) */
#include "cluster/cluster_ges_dedup.h"		/* cluster_ges_dedup_shmem_register (spec-2.27 D2) */
#include "cluster/cluster_wal_thread.h"		/* cluster_wal_thread_shmem_register (spec-4.1 D7) */
#include "cluster/cluster_recovery_plan.h" /* cluster_recovery_plan_shmem_register (spec-4.3 D4) */
#include "cluster/cluster_block_recovery.h" /* cluster_block_recovery_shmem_register (spec-4.10 D6) */
#include "cluster/cluster_grd_outbound.h"	/* cluster_grd_outbound_shmem_register (spec-2.16 D4) */
#include "cluster/cluster_grd_work_queue.h" /* cluster_grd_work_queue_shmem_register (spec-2.16 D5) */
#include "cluster/cluster_stats.h"			/* cluster_stats_shmem_register (1.14 Sprint A) */
#include "cluster/cluster_lmon.h"			/* cluster_lmon_shmem_register (1.11 Sprint A) */
#include "cluster/cluster_gcs.h"			/* cluster_gcs_module_init (spec-2.32 D2) */
#include "cluster/cluster_gcs_block.h"		/* cluster_gcs_block_module_init (spec-2.33 D3) */
#include "cluster/cluster_gcs_block_dedup.h" /* cluster_gcs_block_dedup_module_init (spec-2.34 D2) */
#include "cluster/cluster_pcm_lock.h"		 /* cluster_pcm_lock_module_init (stage 1.7) */
#include "cluster/cluster_sinval.h"			 /* cluster_sinval_module_init (spec-2.38 D2/D3) */
#include "cluster/cluster_tt_status.h"		 /* cluster_tt_status_shmem_register (spec-3.1 D2) */
#include "cluster/cluster_tt_local.h"		 /* cluster_tt_local_shmem_register (spec-3.1 D5) */
#include "cluster/cluster_tt_slot.h"		 /* cluster_tt_slot_shmem_register (spec-3.4b D3) */
#include "cluster/cluster_tt_status_hint.h" /* cluster_tt_status_hint_shmem_register (spec-3.2 D3) */
#include "cluster/cluster_tx_enqueue.h"		/* cluster_tx_enqueue_shmem_register (spec-5.2 D4/D6) */
#include "cluster/cluster_subtrans.h"		/* cluster_subtrans_shmem_register (spec-3.5 D5) */
#include "cluster/cluster_multixact.h"		/* cluster_multixact_shmem_register (spec-3.6 D2) */
#include "cluster/cluster_undo_record_api.h" /* cluster_undo_record_shmem_register (spec-3.7 D5) */
#include "cluster/cluster_cr.h"				 /* cluster_cr_shmem_register (spec-3.9 D2) */
#include "cluster/cluster_tt_durable.h"		 /* cluster_tt_durable_shmem_register (spec-3.11 D7) */
#include "cluster/cluster_visibility_inject.h" /* cluster_visibility_inject_shmem_register (spec-3.2 D5b) */
#include "cluster/cluster_itl.h"			   /* cluster_lock_path_shmem_register (spec-3.4e D6) */
#include "cluster/cluster_qvotec.h" /* cluster_qvotec_shmem_register (spec-2.6 Sprint A Step 1) */
#include "cluster/cluster_fence.h"	/* cluster_fence_shmem_register (spec-2.28 Sprint A Step 1) */
#include "cluster/cluster_reconfig.h" /* cluster_reconfig_shmem_register (spec-2.29 Sprint A Step 1) */
#include "cluster/cluster_write_fence.h" /* cluster_write_fence_shmem_register (spec-4.12 D7) */
#include "cluster/cluster_lms.h" /* cluster_lms_shmem_register (spec-2.18 Sprint A Step 1) */
#include "cluster/cluster_lmd.h" /* cluster_lmd_shmem_register (spec-2.19 Sprint A Step 1) */
/* spec-2.7 hardening F1: cluster_smgr_shmem_register;intentionally no
 * trailing line-end comment so the longer storage/ path doesn't force
 * clang-format to realign every neighbour include above. */
#include "cluster/storage/cluster_smgr.h"
#include "cluster/cluster_shmem.h"
#include "cluster/storage/cluster_undo_buf.h" /* spec-3.18 D1 region */
#include "cluster/cluster_startup_phase.h"	  /* cluster_phase_shmem_register (1.10.1) */
#include "cluster/cluster_version_macros.h"


/* ============================================================
 * Process-local pointer to the control block.
 * ============================================================ */
ClusterShmemCtl *ClusterShmem = NULL;


/* ============================================================
 * Registry storage (stage 1.3).
 *
 *	Allocated once in cluster_init_shmem_module() with capacity =
 *	cluster.shmem_max_regions GUC.  Frozen at the entry of
 *	cluster_request_shmem (further register calls fail).
 *
 *	On EXEC_BACKEND child processes the registry array is rebuilt
 *	(cluster_init_shmem_module runs again under IsUnderPostmaster);
 *	register API tolerates this by silently treating duplicate name
 *	in child processes as an idempotent rebind.
 * ============================================================ */
static ClusterShmemRegion *registry = NULL;
static int registry_count = 0;
static int registry_capacity = 0;
static bool registry_frozen = false;


/* ============================================================
 * Forward declarations.
 * ============================================================ */
static Size cluster_ctl_shmem_size(void);
static void cluster_ctl_shmem_init(void);


/* ============================================================
 * Foundational region descriptors (stage 1.3).
 *
 *	cluster_ctl owns the ClusterShmemCtl control block.
 *	cluster_conf owns the ClusterConf topology shmem (stage 0.19).
 *	Both are registered from cluster_init_shmem_module() so the
 *	registry is the single dispatch path -- no hard-coded fallback.
 * ============================================================ */
static const ClusterShmemRegion cluster_ctl_region = {
	.name = "pgrac cluster control",
	.size_fn = cluster_ctl_shmem_size,
	.init_fn = cluster_ctl_shmem_init,
	.lwlock_count = 0,
	.owner_subsys = "cluster_ctl",
	.reserved_flags = 0,
};

static const ClusterShmemRegion cluster_conf_region = {
	.name = "pgrac cluster conf",
	.size_fn = cluster_conf_shmem_size,
	.init_fn = cluster_conf_shmem_init,
	.lwlock_count = 0,
	.owner_subsys = "cluster_conf",
	.reserved_flags = 0,
};


/* ============================================================
 * Registry API (stage 1.3).
 * ============================================================ */

/*
 * cluster_shmem_register_region -- register one region.
 *
 *	Adds the supplied region descriptor to the static registry array.
 *	Errors out (ERROR) if the registry is frozen, the name is
 *	duplicated, or capacity is exceeded.
 *
 *	On EXEC_BACKEND child processes (IsUnderPostmaster == true), a
 *	duplicate name from re-running cluster_init_shmem_module is
 *	silently accepted as a rebind (no error).
 */
void
cluster_shmem_register_region(const ClusterShmemRegion *region)
{
	int i;

	CLUSTER_INJECTION_POINT("cluster-shmem-register-region");

	Assert(region != NULL);
	Assert(region->name != NULL);
	Assert(region->size_fn != NULL);
	Assert(region->init_fn != NULL);
	Assert(region->owner_subsys != NULL);
	Assert(region->reserved_flags == 0);

	if (registry == NULL)
		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						errmsg("cluster_shmem_register_region called before "
							   "cluster_init_shmem_module"),
						errhint("Call cluster_init() before registering shmem regions.")));

	if (region->name == NULL || region->size_fn == NULL || region->init_fn == NULL
		|| region->owner_subsys == NULL || region->reserved_flags != 0)
		ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						errmsg("invalid cluster shmem region descriptor"),
						errhint("Every region descriptor must set name, size_fn, init_fn, "
								"owner_subsys, and reserved_flags=0.")));

	if (registry_frozen)
		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						errmsg("cluster_shmem_register_region called after registry frozen"),
						errhint("Register subsystems before cluster_request_shmem entry.")));

	for (i = 0; i < registry_count; i++) {
		if (strcmp(registry[i].name, region->name) == 0) {
			/*
			 * Duplicate name.  On POSIX fork platforms this should never
			 * happen (registry is built once in postmaster).  On
			 * EXEC_BACKEND children it can happen because each child
			 * re-runs the init path -- treat as idempotent rebind.
			 */
			if (IsUnderPostmaster)
				return;

			ereport(ERROR, (errcode(ERRCODE_DUPLICATE_OBJECT),
							errmsg("duplicate cluster shmem region \"%s\"", region->name),
							errhint("Region already registered by subsystem \"%s\".",
									registry[i].owner_subsys)));
		}
	}

	if (registry_count >= registry_capacity)
		ereport(ERROR, (errcode(ERRCODE_CONFIGURATION_LIMIT_EXCEEDED),
						errmsg("cluster shmem registry capacity exceeded "
							   "(current=%d, max=%d)",
							   registry_count, registry_capacity),
						errhint("Raise cluster.shmem_max_regions GUC (currently %d).",
								registry_capacity)));

	registry[registry_count] = *region;
	registry_count++;
}

/*
 * cluster_shmem_lookup_region -- find a region by name.
 *
 *	Returns a pointer to the registered descriptor or NULL if not
 *	found.  Pointer is stable for the postmaster lifetime.
 */
const ClusterShmemRegion *
cluster_shmem_lookup_region(const char *name)
{
	int i;

	if (registry == NULL || name == NULL)
		return NULL;

	for (i = 0; i < registry_count; i++) {
		if (strcmp(registry[i].name, name) == 0)
			return &registry[i];
	}
	return NULL;
}

/*
 * cluster_shmem_iter_regions -- iterator-style traversal.
 *
 *	Caller initializes *idx = 0 and calls repeatedly.  Returns true
 *	with *out filled per call; returns false when exhausted.  Used by
 *	pg_cluster_shmem SRF and pg_cluster_state dump.
 */
bool
cluster_shmem_iter_regions(int *idx, ClusterShmemRegion *out)
{
	Assert(idx != NULL);
	Assert(out != NULL);

	if (registry == NULL || *idx < 0 || *idx >= registry_count)
		return false;

	*out = registry[*idx];
	(*idx)++;
	return true;
}

/*
 * cluster_shmem_get_region_count -- number of registered regions.
 */
int
cluster_shmem_get_region_count(void)
{
	return registry_count;
}

/*
 * cluster_shmem_get_total_bytes -- sum of size_fn() across registry.
 *
 *	Used by pg_cluster_state.shmem.total_bytes.
 */
Size
cluster_shmem_get_total_bytes(void)
{
	Size total = 0;
	int i;

	for (i = 0; i < registry_count; i++)
		total = add_size(total, registry[i].size_fn());
	return total;
}


/* ============================================================
 * Module init -- foundational region registration.
 * ============================================================ */

/*
 * cluster_init_shmem_module -- allocate registry and register the
 *	foundational regions (cluster_ctl + cluster_conf).
 *
 *	Called from cluster_init() during postmaster startup, after
 *	cluster_init_guc.  Other subsystems (cluster_ic, future grd / pcm /
 *	ges / ...) register their own regions from their own init helpers.
 *
 *	On EXEC_BACKEND child processes this runs again; the duplicate-name
 *	branch in cluster_shmem_register_region treats re-registration as
 *	idempotent rebind (no error).
 */
void
cluster_init_shmem_module(void)
{
	/*
	 * First call: allocate the registry array.  Capacity is bound by the
	 * cluster.shmem_max_regions GUC.  In bootstrap and check modes
	 * (initdb --boot / --check) cluster_init_guc never runs, so the
	 * cluster_shmem_max_regions C global retains its boot value (64).
	 */
	if (registry == NULL) {
		registry_capacity = cluster_shmem_max_regions;
		registry = (ClusterShmemRegion *)MemoryContextAllocZero(
			TopMemoryContext, sizeof(ClusterShmemRegion) * registry_capacity);
		registry_count = 0;
		registry_frozen = false;
	}

	/*
	 * Idempotent foundational region registration.  Callers can be:
	 *	  - miscinit.c::process_shared_preload_libraries (postmaster mode,
	 *	    via cluster_init())
	 *	  - cluster_init_shmem (bootstrap / check / single-user fallback,
	 *	    when process_shared_preload_libraries was skipped)
	 *	  - EXEC_BACKEND child re-running cluster_init()
	 *
	 *	The lookup-then-register pattern keeps each foundational region
	 *	registered exactly once per registry, regardless of caller mix.
	 */
	if (cluster_shmem_lookup_region(cluster_ctl_region.name) == NULL)
		cluster_shmem_register_region(&cluster_ctl_region);
	if (cluster_shmem_lookup_region(cluster_conf_region.name) == NULL)
		cluster_shmem_register_region(&cluster_conf_region);

	/* spec-3.18 D1: undo block buffer pool (size_fn returns 0 when
	 * cluster.undo_buffers=0, so registration is near-free when disabled). */
	if (cluster_shmem_lookup_region("pgrac undo buffer pool") == NULL)
		cluster_undo_buf_register_region();

	/*
	 * Stage 1.7: register cluster_pcm_grd (PCM GRD master state).
	 * Region size_fn returns 0 if cluster.pcm_grd_max_entries=0
	 * (default), so this registration is essentially zero-cost when
	 * PCM is not enabled.  Stage 2.X PCM 真值激活 spec changes the
	 * GUC default to NBuffers.
	 *
	 * Spec: spec-1.7-pcm-state-placeholder.md §1.2 Deliverable 3 +
	 *       §11.2 shmem registry checklist.
	 */
	if (cluster_shmem_lookup_region("pgrac cluster pcm grd") == NULL)
		cluster_pcm_lock_module_init();

	/*
	 * spec-2.32 D2: register cluster_gcs shmem region (outstanding-request
	 * table per-backend + module counters).  Single-source-of-truth for
	 * GCS request/reply protocol state; cluster_pcm_lock dispatches through
	 * cluster_gcs_send_transition_and_wait when master != self.
	 *
	 * Spec: spec-2.32-gcs-request-protocol-skeleton.md §1.2 D2 + D3 + D6.
	 */
	if (cluster_shmem_lookup_region("pgrac cluster gcs") == NULL)
		cluster_gcs_module_init();

	/*
	 * spec-2.33 D3: register cluster_gcs_block shmem region (block-shipping
	 * outstanding table per-backend + 8 data-plane observability counters).
	 * Separate from cluster_gcs shmem so observability can distinguish
	 * control-plane vs data-plane contention.
	 *
	 * Spec: spec-2.33-gcs-block-shipping-substrate.md §1.2 D3.
	 */
	if (cluster_shmem_lookup_region("pgrac cluster gcs block") == NULL)
		cluster_gcs_block_module_init();

	/*
	 * spec-2.34 D2: register cluster_gcs_block_dedup shmem region
	 * (master-side dedup HTAB + 5 counter + LWLock for HC90/HC91/HC92).
	 * Separate region from cluster_gcs_block so memory accounting can
	 * distinguish data-plane reliability dedup from data-plane outstanding
	 * slot tables.
	 *
	 * Spec: spec-2.34-gcs-block-reliability-hardening.md §1.2 D2.
	 */
	if (cluster_shmem_lookup_region("pgrac cluster gcs block dedup") == NULL)
		cluster_gcs_block_dedup_module_init();

	/*
	 * spec-2.38 D2/D3: register cluster_sinval outbound + inbound shmem
	 * regions (ring buffer queues for SI Broadcaster aux process — HC132
	 * outbound 独立防 echo loop;HC133 inbound nonblocking try-enqueue).
	 *
	 * Spec: spec-2.38-si-broadcaster-skeleton.md §1.2 D2 + D3.
	 */
	if (cluster_shmem_lookup_region("pgrac cluster sinval outbound") == NULL)
		cluster_sinval_module_init();

	/*
	 * spec-3.1 D2:  register cluster_tt_status overlay shmem region
	 * (bounded HTAB keyed by ClusterTTStatusKey 24B exact key).  HC181
	 * miss returns UNKNOWN fail-closed; HC182 epoch fencing on lookup;
	 * HC183 wire-stable key layout.  L176 lesson — TT, not CLOG.
	 *
	 * Spec: spec-3.1-cluster-xid-status-foundation.md §1.2 D2.
	 */
	if (cluster_shmem_lookup_region("pgrac cluster tt status overlay") == NULL)
		cluster_tt_status_shmem_register();

	/*
	 * spec-3.1 D5:  register cluster_tt_local provisional tt_slot_id
	 * monotonic counter shmem.
	 */
	if (cluster_shmem_lookup_region("pgrac cluster tt local seq") == NULL)
		cluster_tt_local_shmem_register();

	/*
	 * spec-3.4b D3:  register per-segment TT slot allocator shmem
	 * (per-node 48-slot table + per-node LWLock).  Production cluster
	 * path real allocator replaces the spec-3.1 provisional
	 * tt_slot_id minting.
	 */
	if (cluster_shmem_lookup_region("pgrac cluster tt slot allocator") == NULL)
		cluster_tt_slot_shmem_register();

	/*
	 * spec-3.2 D3:  register cluster_tt_status_hint outbound ring +
	 * counters shmem (LMON-mediated fanout;  HC185 producer mask).
	 */
	if (cluster_shmem_lookup_region("pgrac cluster tt status hint outbound") == NULL)
		cluster_tt_status_hint_shmem_register();

	/* spec-5.2 D4/D6: cross-node TX enqueue completion-wait waiter table. */
	if (cluster_shmem_lookup_region("pgrac cluster tx enqueue") == NULL)
		cluster_tx_enqueue_shmem_register();

	/*
	 * PGRAC spec-3.5 D5:  register cluster_subtrans shmem (counter-only).
	 */
	if (cluster_shmem_lookup_region("pgrac cluster subtrans state") == NULL)
		cluster_subtrans_shmem_register();

	/*
	 * PGRAC spec-3.6 D2:  register cluster_multixact overlay HTAB +
	 * counters + LWLock.
	 */
	if (cluster_shmem_lookup_region("pgrac cluster multixact overlay") == NULL)
		cluster_multixact_shmem_register();

	/*
	 * PGRAC spec-3.7 D5:  register cluster_undo_record cursor + counters
	 * shmem region (per-instance record-level allocator state on top of
	 * spec-3.4b segment-level allocator).
	 */
	cluster_undo_record_shmem_register();

	/*
	 * PGRAC spec-3.9 D2:  register cluster_cr counters shmem region
	 * (own-instance CR block construction; 9 atomic counters, 0 LWLock).
	 */
	cluster_cr_shmem_register();

	/*
	 * PGRAC spec-3.11 D7:  register durable TT slot counters shmem region
	 * (commit / lookup hit-miss / by-xid scan / redo apply; 5 atomic
	 * counters, 0 LWLock).
	 */
	cluster_tt_durable_shmem_register();

	/*
	 * spec-3.2 D5b: register test-only visibility inject shmem.  The
	 * production build exports a no-op register helper; ENABLE_INJECTION
	 * builds use this table to drive real HeapTupleSatisfiesMVCC 53R97 TAP
	 * coverage without changing production visibility behavior.
	 */
	if (cluster_shmem_lookup_region("pgrac cluster visibility inject") == NULL)
		cluster_visibility_inject_shmem_register();

	/*
	 * spec-3.4e D6 / F2 P0:  register lock-path counter shmem for
	 * cross-backend aggregation of cluster_remote_row_lock_fail_closed_count
	 * (class 4 hot-row metric depends on pgbench collector backend reading
	 * deltas across N client backends).  Other 4 lock-path counters remain
	 * per-backend (Sprint A scope control;  Hardening v1.0.1 expands).
	 */
	if (cluster_shmem_lookup_region("pgrac cluster lock-path counters") == NULL)
		cluster_lock_path_shmem_register();

	/*
	 * Stage 1.10.1 (F1 hardening): register cluster_startup_phase shmem
	 * region.  Phase state was a process-local static; EXEC_BACKEND
	 * children re-exec'd and saw stale PRE_INIT.  Migrating to shmem
	 * gives every backend a coherent view.
	 *
	 * Spec: spec-1.10.1-postmaster-phase-hardening.md D1 F1.
	 */
	if (cluster_shmem_lookup_region("pgrac cluster startup phase") == NULL)
		cluster_phase_shmem_register();

	/*
	 * Stage 1.11 (Sprint A): register cluster_lmon shmem region.  LMON
	 * is the first cluster aux process spawned by postmaster; its
	 * status / spawned_at / ready_at / liveness tick / shutdown flag
	 * live in shmem (HC3 limited scope: SQL-visible state via shmem,
	 * postmaster reaper PID stays as process-local LmonPID).
	 *
	 * Spec: spec-1.11-lmon-skeleton.md Sprint A D1+D2+D7.
	 */
	if (cluster_shmem_lookup_region("pgrac cluster lmon") == NULL)
		cluster_lmon_shmem_register();

	/* spec-1.12 Sprint A D7: register cluster_lck shmem region. */
	if (cluster_shmem_lookup_region("pgrac cluster lck") == NULL)
		cluster_lck_shmem_register();

	/* spec-1.13 Sprint A D7: register cluster_diag shmem region. */
	if (cluster_shmem_lookup_region("pgrac cluster diag") == NULL)
		cluster_diag_shmem_register();

	/* spec-1.14 Sprint A D7: register cluster_stats shmem region. */
	if (cluster_shmem_lookup_region("pgrac cluster stats") == NULL)
		cluster_stats_shmem_register();

	/* spec-2.5 Sprint A D6: register cluster_cssd shmem region. */
	if (cluster_shmem_lookup_region("pgrac cluster cssd") == NULL)
		cluster_cssd_shmem_register();

	/* spec-3.13 D1: register Undo Cleaner shmem region. */
	if (cluster_shmem_lookup_region("pgrac cluster undo cleaner") == NULL)
		cluster_undo_cleaner_shmem_register();

	/* spec-2.2 D3 (2026-05-07): register cluster_ic_tier1 shmem region. */
	if (cluster_shmem_lookup_region("pgrac cluster_ic_tier1") == NULL)
		cluster_ic_tier1_shmem_register();

	/* spec-1.15 D3: register cluster_scn shmem region (encoding layer). */
	if (cluster_shmem_lookup_region("pgrac cluster scn") == NULL)
		cluster_scn_shmem_register();

	/* spec-2.13 D2: register cluster_ges shmem region (GES protocol
	 * skeleton; 2 atomic uint64 defer counters; lock-free per Q4.1). */
	if (cluster_shmem_lookup_region("pgrac cluster ges") == NULL)
		cluster_ges_shmem_register();

	/* spec-2.23 D1: register cluster_ges_reply_wait shmem region
	 * (cross-node reply correlation HTAB; 5-tuple key HC17;
	 * LWTRANCHE_CLUSTER_GES_REPLY_WAIT). */
	if (cluster_shmem_lookup_region("pgrac cluster ges reply wait") == NULL)
		cluster_ges_reply_wait_shmem_register();

	/* spec-2.14 D5: register cluster_grd shmem region (GRD routing
	 * substrate; 4096 atomic master[] + 5 atomic uint64 counters;
	 * lock-free per Q9). */
	if (cluster_shmem_lookup_region("pgrac cluster grd") == NULL)
		cluster_grd_shmem_register();

	/* spec-2.16 D3: register cluster_grd_pending shmem region
	 * (skeleton phase — 0 byte size_fn).  Step 2 D4 wires real HTAB. */
	if (cluster_shmem_lookup_region("pgrac cluster grd pending") == NULL)
		cluster_grd_pending_shmem_register();

	/* spec-2.16 D4: register cluster_grd_outbound shmem region (LMON-
	 * owned ring + reserved pool + 2 dirty-list;真激活 ring + nofail
	 * counter;Step 3/4 wires real producers).  D5: work queue. */
	if (cluster_shmem_lookup_region("pgrac cluster grd outbound") == NULL)
		cluster_grd_outbound_shmem_register();
	if (cluster_shmem_lookup_region("pgrac cluster grd work queue") == NULL)
		cluster_grd_work_queue_shmem_register();

	/*
	 * spec-2.4 D2: register cluster_epoch shmem region.  64-byte cache-
	 * line aligned (false sharing防 on hot envelope build/verify path);
	 * 48 bytes reserved for spec-2.29 reconfig coordinator metadata.
	 */
	if (cluster_shmem_lookup_region("pgrac cluster epoch") == NULL)
		cluster_epoch_shmem_register();

	/*
	 * spec-2.7 hardening F1 (2026-05-09): register cluster_smgr shmem
	 * region (one pg_atomic_uint64 for the cross-instance broadcast
	 * STUB call counter).  Promoted from per-process pg_atomic on
	 * pre-ship review — process-local counter contradicted both the
	 * manual claim of cluster-wide accumulation and the
	 * pg_stat_cluster_counters cluster-wide口径.
	 */
	if (cluster_shmem_lookup_region("pgrac cluster smgr") == NULL)
		cluster_smgr_shmem_register();

	/*
	 * spec-2.6 Sprint A Step 1 D9: register cluster_qvotec shmem
	 * region.  128-byte (2 cache lines) struct holding lease-based
	 * quorum_state per Q4 v0.2 — backend helper cluster_qvotec_in_
	 * quorum() reads quorum_state + lease_expire_at_us atomic to
	 * decide fail-closed.  Postmaster reaper / phase 4 driver
	 * wiring lands in Step 3 (D7+D8).
	 */
	if (cluster_shmem_lookup_region("pgrac cluster qvotec") == NULL)
		cluster_qvotec_shmem_register();

	/*
	 * spec-2.28 Sprint A Step 1 D2: register cluster_fence shmem
	 * region.  Single-tranche LWLock + 6 lifetime counters for
	 * Fence-lite (freeze / thaw timestamps, self-fence pending +
	 * initiated counter).  Per Invariant I3, prerequisite is
	 * v0.14.2-stage2.6 nightly run 25618433189 ✓.  Step 2 D3 wires
	 * ProcSignal handler bodies, Step 3 D5 wires LMON broadcast,
	 * Step 3 D6 wires postmaster kill(SIGINT) trigger.
	 */
	if (cluster_shmem_lookup_region("pgrac cluster fence") == NULL)
		cluster_fence_shmem_register();

	/*
	 * spec-2.29 Sprint A Step 1 D2: register cluster_reconfig shmem
	 * region.  Single-tranche LWLock guards last_applied ReconfigEvent
	 * publish path + 3 lifetime atomic counters (apply / dedup_skip /
	 * procsig_broadcast).  Step 2 wires LMON tick body + ProcessInterrupts
	 * D4 integration;Step 3 wires envelope verify path observe (D20) +
	 * pg_cluster_reconfig_state SRF.  ReconfigEvent dead_bitmap is uint8[16]
	 * (128 nodes per P2.8 fix).
	 */
	if (cluster_shmem_lookup_region("pgrac cluster reconfig") == NULL)
		cluster_reconfig_shmem_register();

	/*
	 * spec-4.12 D7: register the cooperative write-fence token region.  Holds the
	 * local authorized_epoch + lease + self_fenced token (qvotec refreshes it from
	 * the durable voting-disk marker) plus the LMON->qvotec marker-submit mailbox
	 * and the observability counters.
	 */
	if (cluster_shmem_lookup_region("pgrac cluster write fence") == NULL)
		cluster_write_fence_shmem_register();

	/*
	 * spec-2.18 Sprint A Step 1 D6/D7: register cluster_lms shmem region.
	 * Single-tranche LWLock guards non-atomic LMS fields (pid /
	 * spawned_at / ready_at / stopped_at / shutdown_requested).  lms_state
	 * itself is atomic for HC4 single-ownership lock-free read on the
	 * LMON hot-path (cluster_lms_owns_grant).  Real LMON ↔ LMS ownership
	 * transfer guard wires in Step 3.
	 */
	if (cluster_shmem_lookup_region("pgrac cluster lms") == NULL)
		cluster_lms_shmem_register();

	/*
	 * PGRAC (spec-2.19 Sprint A Step 1 D7):register LMD shmem region.
	 * Separate ClusterLmdShmem region (L98 cross-spec ownership invariant —
	 * new actor / new shmem region / new LWLock tranche;不与 LMS shmem
	 * 共享 region 违反 single-responsibility).  Idempotent guard via region
	 * lookup mirrors LMS pattern.  Region lifecycle: cluster_lmd_shmem_size
	 * + cluster_lmd_shmem_init invoked via spec-1.3 registry (see
	 * cluster_lmd_region descriptor in cluster_lmd.c).
	 */
	if (cluster_shmem_lookup_region("pgrac cluster lmd") == NULL)
		cluster_lmd_shmem_register();

	/*
	 * PGRAC (spec-2.27 D2):  register GES dedup HTAB shmem region.
	 * 'pgrac cluster ges dedup' — LMS-owned dedup table (HC51 — entries
	 * persist across LMS process restart so retransmit deduplication
	 * survives crash;  stale generation entries swept at LMS startup).
	 */
	if (cluster_shmem_lookup_region("pgrac cluster ges dedup") == NULL)
		cluster_ges_dedup_shmem_register();

	/*
	 * PGRAC (spec-4.1 D7):  register the per-thread WAL routing region.
	 * 'pgrac wal thread' — one page-stamp counter + a small identity
	 * mirror written once by cluster_wal_thread_init() so the dump
	 * accessors stay correct under EXEC_BACKEND.
	 */
	if (cluster_shmem_lookup_region("pgrac wal thread") == NULL)
		cluster_wal_thread_shmem_register();

	/*
	 * PGRAC (spec-4.3 D4):  register the recovery-plan mirror region.
	 * 'pgrac recovery plan' — the per-thread verdict table + counts
	 * published by the startup process plan pass; read by the dump
	 * accessors in any backend (EXEC_BACKEND safe via ShmemInitStruct
	 * rebind).
	 */
	if (cluster_shmem_lookup_region("pgrac recovery plan") == NULL)
		cluster_recovery_plan_shmem_register();

	if (cluster_shmem_lookup_region("pgrac block recovery") == NULL)
		cluster_block_recovery_shmem_register();
}


/* ============================================================
 * Public entry points -- called by PG core.
 * ============================================================ */

/*
 * cluster_shmem_size -- diagnostic helper, returns the total bytes
 *	the cluster subsystem will request.
 */
Size
cluster_shmem_size(void)
{
	return cluster_shmem_get_total_bytes();
}

/*
 * cluster_request_shmem -- reserve shmem space for every cluster
 *	subsystem.  Called from miscinit.c during the
 *	process_shmem_requests phase.  PG forbids RequestAddinShmemSpace
 *	outside this phase, so this is the only window in which we can
 *	register.
 *
 *	Stage 1.3: iterates the registry and calls size_fn per row.
 */
void
cluster_request_shmem(void)
{
	int idx;
	ClusterShmemRegion region;

	CLUSTER_INJECTION_POINT("cluster-shmem-request");

	/*
	 * Freeze the registry: any subsequent cluster_shmem_register_region
	 * call will fail.  Subsystems must register before this point.
	 */
	registry_frozen = true;

	idx = 0;
	while (cluster_shmem_iter_regions(&idx, &region))
		RequestAddinShmemSpace(region.size_fn());

	/*
	 * spec-2.15 v0.3 P1.1 + I15:  named tranche request hook is invoked
	 * exactly once from within the process_shmem_requests_in_progress
	 * window — kept outside size_fn so that diagnostic paths
	 * (cluster_shmem_get_total_bytes) may call size_fn repeatedly
	 * without re-triggering RequestNamedLWLockTranche.
	 */
	cluster_grd_request_lwlocks();
	cluster_ges_dedup_shmem_request();

	/*
	 * spec-4.5a G5: pg_xact_remote SLRU manages its own shmem (SimpleLru),
	 * so it is reserved/requested directly here rather than through the
	 * region registry (whose init_fn/size_fn contract is for plain
	 * ShmemInitStruct regions).
	 */
	RequestAddinShmemSpace(cluster_remote_xact_shmem_size());
	cluster_remote_xact_shmem_request();
}

/*
 * cluster_init_shmem -- allocate and initialise every cluster shmem
 *	region.  Called from ipci.c::CreateSharedMemoryAndSemaphores()
 *	after PG's built-in ShmemInit calls and before the user
 *	shmem_startup_hook.
 *
 *	Stage 1.3: iterates the registry and calls init_fn per row.  Each
 *	init_fn must use ShmemInitStruct so EXEC_BACKEND children rebind.
 */
void
cluster_init_shmem(void)
{
	int idx;
	ClusterShmemRegion region;

	CLUSTER_INJECTION_POINT("cluster-init-pre-shmem");

	/*
	 * initdb's bootstrap postgres does not run process_shared_preload_libraries
	 * or process_shmem_requests, so no addin shmem has been reserved.  In that
	 * path the registry was never built and cluster_node_id is still the disabled
	 * sentinel.  Do not lazily register/init the full cluster region set from the
	 * tiny bootstrap shmem slop; real cluster shmem is created on normal
	 * postmaster start after initdb completes.
	 */
	if (registry == NULL && cluster_node_id < 0)
		return;

	/*
	 * Bootstrap and standalone modes skip process_shared_preload_libraries
	 * (and thus cluster_init -> cluster_init_shmem_module).  Build the
	 * registry lazily here so the foundational regions are always
	 * present before we iterate.
	 */
	if (registry == NULL)
		cluster_init_shmem_module();

	idx = 0;
	while (cluster_shmem_iter_regions(&idx, &region)) {
		CLUSTER_INJECTION_POINT("cluster-shmem-region-init-pre");
		region.init_fn();
		CLUSTER_INJECTION_POINT("cluster-shmem-region-init-post");
	}

	/* spec-4.5a G5: SLRU init (self-managed shmem, outside the registry). */
	cluster_remote_xact_shmem_init();

	/*
	 * spec-3.4b D8 / Q4 HC (L191): code-enforced automatic flush of the
	 * cluster TT status overlay immediately after all shmem regions are
	 * initialised.  Postmaster bootstrap path; backend forks inherit the
	 * cleaned HTAB.  This is the only correctness guarantee that
	 * production reader / spec-3.2 D5 fork never consumes stale provisional
	 * spec-3.1/3.2/3.3/3.4a overlay entries during the spec-3.4b activation
	 * boundary.  reconfig epoch bump (cluster_reconfig.c:571) remains as
	 * runtime fallback only.
	 */
	cluster_tt_status_flush_all_at_activation();

	/*
	 * Stage 0.19: parse pgrac.conf into ClusterConfShmem.  Must run
	 * after the conf shmem region is allocated (above) and before
	 * cluster_ic_init (below) -- Stage 2+ TCP vtable.tier_init will
	 * read interconnect_addr from the topology.  Stub mode at 0.18
	 * does not consume the topology, so the relative ordering is for
	 * forward symmetry.
	 *
	 * spec-2.1 Hardening v1.0.1 F2: gate cluster_conf_load on
	 * cluster_enabled (caller primary gate, per L15 pattern).  Without
	 * this gate, cluster.enabled=off + allow_single_node=off + missing
	 * pgrac.conf would FATAL, violating the vanilla PG path promise
	 * (spec-1.16 D13 docstring).  cluster_conf_load itself also has a
	 * defensive early-return on !cluster_enabled (belt-and-suspenders;
	 * future callers must not rely on that alone -- always gate here).
	 * Cross-ref: lessons L11 (1.17 walwriter idle CPU regression),
	 * L15 (1.16 early-return fragility), L36 (1.18 WAL replay observe
	 * wrapper), L48 meta (lesson SSOT enforce 不能假设单点).
	 */
	if (cluster_enabled)
		cluster_conf_load();

	/*
	 * spec-2.14 D4: initialize GRD master map after cluster_conf is
	 * loaded.  declared-node-aware: scans cluster_conf_lookup_node() over
	 * 0..CLUSTER_MAX_NODES + cluster_conf_node_count() cross-check
	 * (handles sparse node_id 0/2/5 in pgrac.conf; per Q10 + P2.1).
	 *
	 * Must run AFTER cluster_conf_load() (depends on declared topology)
	 * and AFTER the shmem region init loop above (depends on
	 * cluster_grd_state non-NULL).
	 *
	 * Triple gate per L15 caller primary pattern:
	 *   1. cluster_enabled — disable-cluster path skips entirely
	 *   2. cluster_node_id >= 0 — bootstrap / --check / --boot have
	 *      cluster_node_id = -1 (default) and single-node fallback
	 *      populates nodes[0].node_id = -1, which yields declared_count = 0
	 *      via the i = 0..CLUSTER_MAX_NODES scan (negative ids skipped) →
	 *      i % 0 UB in the distribution loop.  Real postmaster start with
	 *      cluster.node_id GUC set in postgresql.conf gets here cleanly.
	 *   3. cluster_conf_node_count() > 0 — defensive symmetry.
	 */
	if (cluster_enabled && cluster_node_id >= 0 && cluster_conf_node_count() > 0)
		cluster_grd_master_map_init();

	/*
	 * Stage 0.18: bind the cluster_ic vtable for the configured tier.
	 * Stub mode allocates no shmem; tier1+ (Stage 2+) will piggyback
	 * its own *_shmem_init helper above.  Done here -- inside
	 * cluster_init_shmem -- because (a) GUCs are already loaded and
	 * (b) Stage 2+ will need shmem to be ready when the tier_init
	 * hook fires.
	 *
	 * spec-2.1 Hardening v1.0.2 D-I1 (extends F2; codex review P1
	 * post-Sprint B): gate cluster_ic_init on cluster_enabled (caller
	 * primary, per L15 pattern -- same as cluster_conf_load above).
	 * Without this gate, cluster.enabled=off + cluster.interconnect_tier=
	 * tier1 would FATAL inside cluster_ic_init (tier1 raises
	 * ERRCODE_FEATURE_NOT_SUPPORTED until Stage 2 implementation lands),
	 * violating the postgresql.conf.sample promise that cluster.enabled=
	 * off means "vanilla PG behaviour: no cluster shmem, no SCN advance,
	 * no IC tier_init".  v1.0.1 only gated cluster_conf_load -- v1.0.2
	 * extends the same caller gate to cluster_ic_init.  cluster_ic_init
	 * itself also has a defensive early-return on !cluster_enabled
	 * (belt-and-suspenders; same family as cluster_conf_load defensive
	 * guard).  Cross-ref lessons L11/L15/L36/L48 (cluster_enabled gate
	 * forgot family) -- this is the second neighbor of F2 caught by
	 * post-ship review (codex follow-up scan 2026-05-07 P1).
	 */
	if (cluster_enabled)
		cluster_ic_init();

	CLUSTER_INJECTION_POINT("cluster-init-post-shmem");
}


/* ============================================================
 * Cluster control block (stage 0.14, registry-driven since 1.3).
 *
 *	Immutable metadata: magic, packed version, snapshot of
 *	cluster_node_id at init, and creation timestamp.  No locking
 *	is required because writes happen exactly once in the postmaster
 *	before any backend forks.
 * ============================================================ */

static Size
cluster_ctl_shmem_size(void)
{
	return MAXALIGN(sizeof(ClusterShmemCtl));
}

static void
cluster_ctl_shmem_init(void)
{
	bool found;

	ClusterShmem = (ClusterShmemCtl *)ShmemInitStruct("pgrac cluster control",
													  cluster_ctl_shmem_size(), &found);

	if (!found) {
		/*
		 * First-time initialisation.  We are in the postmaster, before
		 * any backend has forked, so no locking is needed.
		 */
		memset(ClusterShmem, 0, sizeof(*ClusterShmem));
		ClusterShmem->magic = CLUSTER_SHMEM_MAGIC;
		ClusterShmem->version_packed
			= ((uint32)PGRAC_VERSION_MAJOR << 24) | ((uint32)PGRAC_VERSION_MINOR << 16)
			  | ((uint32)PGRAC_VERSION_PATCH << 8) | ((uint32)PGRAC_STAGE_STEP);
		ClusterShmem->node_id_at_init = cluster_node_id;
		ClusterShmem->created_at = GetCurrentTimestamp();

		CLUSTER_LOG(LOG,
					"cluster shmem control block allocated "
					"(%zu bytes, magic=0x%08x, version=0x%08x)",
					cluster_ctl_shmem_size(), ClusterShmem->magic, ClusterShmem->version_packed);
	}
	/*
	 * EXEC_BACKEND children land here with found = true; ClusterShmem
	 * is rebound to the existing region and we do nothing else.
	 */
}
