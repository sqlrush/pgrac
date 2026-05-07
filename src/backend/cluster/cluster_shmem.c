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

#include "cluster/cluster_conf.h"	  /* cluster_conf_shmem_size / init */
#include "cluster/cluster_elog.h"	  /* CLUSTER_LOG */
#include "cluster/cluster_guc.h"	  /* cluster_node_id / cluster_shmem_max_regions */
#include "cluster/cluster_ic.h"		  /* cluster_ic_init / shutdown (stage 0.18) */
#include "cluster/cluster_diag.h"	  /* cluster_diag_shmem_register (1.13 Sprint A) */
#include "cluster/cluster_inject.h"	  /* CLUSTER_INJECTION_POINT */
#include "cluster/cluster_lck.h"	  /* cluster_lck_shmem_register (1.12 Sprint A) */
#include "cluster/cluster_scn.h"	  /* cluster_scn_shmem_register (1.15) */
#include "cluster/cluster_stats.h"	  /* cluster_stats_shmem_register (1.14 Sprint A) */
#include "cluster/cluster_lmon.h"	  /* cluster_lmon_shmem_register (1.11 Sprint A) */
#include "cluster/cluster_pcm_lock.h" /* cluster_pcm_lock_module_init (stage 1.7) */
#include "cluster/cluster_shmem.h"
#include "cluster/cluster_startup_phase.h" /* cluster_phase_shmem_register (1.10.1) */
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

	/* spec-1.15 D3: register cluster_scn shmem region (encoding layer). */
	if (cluster_shmem_lookup_region("pgrac cluster scn") == NULL)
		cluster_scn_shmem_register();
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
	 * Stage 0.18: bind the cluster_ic vtable for the configured tier.
	 * Stub mode allocates no shmem; tier1+ (Stage 2+) will piggyback
	 * its own *_shmem_init helper above.  Done here -- inside
	 * cluster_init_shmem -- because (a) GUCs are already loaded and
	 * (b) Stage 2+ will need shmem to be ready when the tier_init
	 * hook fires.
	 */
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
