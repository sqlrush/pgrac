/*-------------------------------------------------------------------------
 *
 * cluster_shmem.c
 *	  pgrac cluster shared-memory registration and control block.
 *
 *	  Stage 0.14 establishes the cluster shmem framework.  All cluster
 *	  subsystems funnel through this file's two public entry points:
 *
 *	    cluster_request_shmem()  -- Phase 1 (miscinit.c)
 *	      Reserves the size of every cluster shmem region by calling
 *	      RequestAddinShmemSpace() and (when needed) RequestNamedLWLock-
 *	      Tranche().  Subsystems append their own *_shmem_request()
 *	      static helpers below.
 *
 *	    cluster_init_shmem()     -- Phase 2 (ipci.c)
 *	      Allocates each region via ShmemInitStruct() and runs first-
 *	      time initialisation when ShmemInitStruct returns found = false.
 *	      Subsystems append their own *_shmem_init() static helpers.
 *
 *	  Why one entry per subsystem instead of letting each subsystem edit
 *	  PG core files directly: keeps the PGRAC MODIFICATIONS to PG paths
 *	  to the two single-line callbacks at the top of this file -- new
 *	  cluster subsystems never touch miscinit.c or ipci.c again.
 *
 *	  Stage 0.14 ships only the control block (ClusterShmemCtl).  See
 *	  docs/cluster-shmem-design.md §3 for the full registration roster
 *	  and CLAUDE.md rule 8 for why we do not pre-allocate placeholder
 *	  regions or LWLocks.
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

#include "storage/shmem.h"
#include "utils/timestamp.h"

#include "cluster/cluster_guc.h"  /* cluster_node_id */
#include "cluster/cluster_elog.h" /* CLUSTER_LOG */
#include "cluster/cluster_shmem.h"
#include "cluster/cluster_version_macros.h"


/* ============================================================
 * Process-local pointer to the control block.
 * ============================================================ */
ClusterShmemCtl *ClusterShmem = NULL;


/* ============================================================
 * Forward declarations of per-subsystem helpers (stage 0.14).
 *
 *	Each future subsystem (GRD, PCM, GES, ...) declares its own pair
 *	of static helpers and adds matching calls to the public entry
 *	points below.
 * ============================================================ */
static Size cluster_ctl_shmem_size(void);
static void cluster_ctl_shmem_request(void);
static void cluster_ctl_shmem_init(void);


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
	Size total = 0;

	total = add_size(total, cluster_ctl_shmem_size());
	/* Future: total = add_size(total, grd_shmem_size()); ... */

	return total;
}

/*
 * cluster_request_shmem -- reserve shmem space for every cluster
 *	subsystem.  Called from miscinit.c during the
 *	process_shmem_requests phase.  PG forbids RequestAddinShmemSpace
 *	outside this phase, so this is the only window in which we can
 *	register.
 */
void
cluster_request_shmem(void)
{
	cluster_ctl_shmem_request();
	/* Future: grd_shmem_request(); pcm_shmem_request(); ... */
}

/*
 * cluster_init_shmem -- allocate and initialise every cluster shmem
 *	region.  Called from ipci.c::CreateSharedMemoryAndSemaphores()
 *	after PG's built-in ShmemInit calls and before the user
 *	shmem_startup_hook.
 *
 *	On EXEC_BACKEND child processes ShmemInitStruct returns
 *	found = true; helpers in this file must skip first-time
 *	initialisation in that case.
 */
void
cluster_init_shmem(void)
{
	cluster_ctl_shmem_init();
	/* Future: grd_shmem_init(); pcm_shmem_init(); ... */
}


/* ============================================================
 * Cluster control block (stage 0.14).
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
cluster_ctl_shmem_request(void)
{
	RequestAddinShmemSpace(cluster_ctl_shmem_size());
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
