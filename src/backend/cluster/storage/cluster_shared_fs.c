/*-------------------------------------------------------------------------
 *
 * cluster_shared_fs.c
 *	  pgrac shared-storage abstraction layer: registry, dispatch,
 *	  lifecycle.
 *
 *	  Stage 1.1 framework.  Owns:
 *	    - the backend vtable registry (16-slot array indexed by
 *	      ClusterSharedFsBackendId);
 *	    - cluster_shared_fs_init / _shutdown lifecycle hooks called
 *	      from cluster_init / before_shmem_exit;
 *	    - the nine caller-facing I/O dispatch wrappers that forward to
 *	      active_ops->*.
 *
 *	  Backend selection is start-time only and freezes for the
 *	  postmaster's lifetime (see docs/cluster-shared-fs-design.md §0
 *	  principle 2).  Stage 1.1 always registers the stub and local
 *	  built-ins; Stage 2+ backend modules call
 *	  cluster_shared_fs_register_backend() from their own init path.
 *
 *	  See docs/cluster-shared-fs-design.md for the full design;
 *	  specs/spec-1.1-shared-fs-skeleton.md for the stage scope.
 *
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/backend/cluster/storage/cluster_shared_fs.c
 *
 * NOTES
 *	  This is a pgrac-original file (no derivation from PostgreSQL).
 *	  All exported symbols use the cluster_shared_fs_* prefix per
 *	  CLAUDE.md rule 12.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "miscadmin.h"
#include "storage/ipc.h"

#include "cluster/cluster_guc.h"
#include "cluster/cluster_inject.h"
#include "cluster/storage/cluster_shared_fs.h"


#ifdef USE_PGRAC_CLUSTER

/*
 * Registry: an array indexed by ClusterSharedFsBackendId.  Slot
 * contents NULL = unregistered.  Initialised once in
 * cluster_shared_fs_init; read-only thereafter.
 *
 * Allocation note: backends register themselves *during*
 * cluster_shared_fs_init by storing the vtable pointer here.  No
 * dynamic memory; the vtable structs are static in their respective
 * modules, so we just keep pointers.
 */
static const ClusterSharedFsOps *cluster_shared_fs_registry[CLUSTER_SHARED_FS_BACKEND_MAX];

/*
 * Active vtable pointer.  Set at the end of cluster_shared_fs_init
 * once the cluster.shared_storage_backend GUC has been resolved to a
 * registered backend; NULL until then (and forever in disable-cluster
 * builds, where this whole file is not linked).
 */
static const ClusterSharedFsOps *cluster_shared_fs_active_ops = NULL;

/*
 * Re-entrancy guard: register_backend is only legal while
 * cluster_shared_fs_init is on the stack.  Ensures Stage 2+ backend
 * authors don't accidentally try to register from EXEC_BACKEND code or
 * a deferred SQL function.
 */
static bool cluster_shared_fs_init_in_progress = false;


/* ----------
 * GUC -> backend id mapping.
 *
 *	The cluster.shared_storage_backend GUC is an enum whose value
 *	(stored in cluster_shared_storage_backend) lines up with the
 *	ClusterSharedFsBackendId enum positionally.  The mapping is
 *	identity, but a wrapper makes the contract explicit and gives us
 *	one place to extend if the GUC ever splits from the registry id
 *	(unlikely).
 * ----------
 */
static ClusterSharedFsBackendId
cluster_shared_fs_guc_to_backend_id(int guc_value)
{
	return (ClusterSharedFsBackendId)guc_value;
}


/* ----------
 * Registration
 * ----------
 */

void
cluster_shared_fs_register_backend(const ClusterSharedFsOps *ops)
{
	int id;

	if (!cluster_shared_fs_init_in_progress)
		ereport(FATAL,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("cluster_shared_fs_register_backend called outside cluster_shared_fs_init"),
				 errdetail("Backend registration is only legal during postmaster init.")));

	if (ops == NULL || ops->name == NULL)
		ereport(FATAL, (errcode(ERRCODE_INTERNAL_ERROR),
						errmsg("cluster_shared_fs backend registered with NULL ops or name")));

	id = (int)ops->id;
	if (id < 0 || id >= CLUSTER_SHARED_FS_BACKEND_MAX)
		ereport(FATAL,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("cluster_shared_fs backend \"%s\" has out-of-range id %d", ops->name, id),
				 errdetail("Valid range is [0, %d).", CLUSTER_SHARED_FS_BACKEND_MAX)));

	if (ops->open == NULL || ops->close == NULL || ops->read == NULL || ops->write == NULL
		|| ops->extend == NULL || ops->nblocks == NULL || ops->truncate == NULL
		|| ops->immedsync == NULL || ops->unlink == NULL || ops->init == NULL
		|| ops->shutdown == NULL)
		ereport(FATAL, (errcode(ERRCODE_INTERNAL_ERROR),
						errmsg("cluster_shared_fs backend \"%s\" has NULL callbacks", ops->name),
						errdetail("All eleven vtable members must be non-NULL.")));

	CLUSTER_INJECTION_POINT("cluster-shared-fs-backend-register");

	if (cluster_shared_fs_registry[id] != NULL) {
		ereport(WARNING, (errmsg("cluster_shared_fs backend id %d (\"%s\") already registered as "
								 "\"%s\"; ignoring duplicate",
								 id, ops->name, cluster_shared_fs_registry[id]->name)));
		return;
	}

	cluster_shared_fs_registry[id] = ops;
	elog(DEBUG1, "cluster_shared_fs: registered backend \"%s\" at id %d", ops->name, id);
}


/* ----------
 * Lifecycle
 * ----------
 */

void
cluster_shared_fs_init(void)
{
	ClusterSharedFsBackendId requested;
	const ClusterSharedFsOps *ops;

	CLUSTER_INJECTION_POINT("cluster-shared-fs-init-top");

	if (cluster_shared_fs_active_ops != NULL)
		ereport(FATAL,
				(errcode(ERRCODE_INTERNAL_ERROR), errmsg("cluster_shared_fs_init called twice")));

	cluster_shared_fs_init_in_progress = true;

	/*
	 * Built-in registrations.  Stage 2+ backend modules add their
	 * own cluster_shared_fs_register_backend(...) calls between here
	 * and the GUC resolution below.
	 */
	cluster_shared_fs_register_backend(&cluster_shared_fs_stub_ops);
	cluster_shared_fs_register_backend(&cluster_shared_fs_local_ops);

	/*
	 * Resolve cluster.shared_storage_backend GUC -> active vtable.
	 * Out-of-range index (Stage 2+ backend without its module
	 * compiled in) becomes a precise FATAL with errhint pointing the
	 * DBA at the relevant Stage / spec.
	 */
	requested = cluster_shared_fs_guc_to_backend_id(cluster_shared_storage_backend);
	ops = cluster_shared_fs_get_backend_at((int)requested);

	if (ops == NULL)
		ereport(FATAL,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("cluster.shared_storage_backend selected backend (id %d) is not available",
						(int)requested),
				 errhint("Backends \"stub\" and \"local\" are built in; "
						 "\"block_device\", \"cluster_fs\", \"rbd\", and \"multi_attach\" "
						 "land in Stage 2.  Set cluster.shared_storage_backend=stub or "
						 "=local in postgresql.conf and restart.")));

	cluster_shared_fs_active_ops = ops;
	elog(LOG, "cluster_shared_fs: active backend is \"%s\" (id %d)", ops->name, (int)ops->id);

	/* Backend's own init runs after we record it as active. */
	ops->init();

	/*
	 * Stage 1.2 cross-check: cluster.smgr_user_relations=on requires a
	 * real backend.  Stub backend has every callback ereport
	 * FEATURE_NOT_SUPPORTED, so routing user relations through
	 * cluster_smgr -> cluster_shared_fs -> stub would FATAL on the
	 * first I/O.  Catch the misconfiguration at postmaster init.
	 */
	if (cluster_smgr_user_relations &&
		cluster_shared_storage_backend == CLUSTER_SHARED_FS_BACKEND_STUB)
		ereport(FATAL,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("cluster.smgr_user_relations=on requires shared_storage_backend != stub"),
				 errhint("Set cluster.shared_storage_backend=local in postgresql.conf "
						 "to enable cluster_smgr routing, or revert "
						 "cluster.smgr_user_relations=off.")));

	cluster_shared_fs_init_in_progress = false;

	/*
	 * Register a process-exit hook so the active backend has a chance
	 * to clean up (close fd's, release locks, etc.) regardless of
	 * exit path.
	 */
	before_shmem_exit((pg_on_exit_callback)cluster_shared_fs_shutdown, 0);
}


void
cluster_shared_fs_shutdown(void)
{
	if (cluster_shared_fs_active_ops == NULL)
		return;

	cluster_shared_fs_active_ops->shutdown();
	cluster_shared_fs_active_ops = NULL;
}


/* ----------
 * Accessors
 * ----------
 */

const ClusterSharedFsOps *
cluster_shared_fs_get_active_ops(void)
{
	return cluster_shared_fs_active_ops;
}


int
cluster_shared_fs_get_registered_count(void)
{
	int count = 0;
	int i;

	for (i = 0; i < CLUSTER_SHARED_FS_BACKEND_MAX; i++) {
		if (cluster_shared_fs_registry[i] != NULL)
			count++;
	}
	return count;
}


const ClusterSharedFsOps *
cluster_shared_fs_get_backend_at(int id)
{
	if (id < 0 || id >= CLUSTER_SHARED_FS_BACKEND_MAX)
		return NULL;
	return cluster_shared_fs_registry[id];
}


/* ----------
 * Caller-facing dispatch wrappers
 *
 *	Thin pass-through; centralised so future stages can plumb wait
 *	events / pgstat counters in one place.  Stage 1.1: pure dispatch.
 * ----------
 */

#define ENSURE_ACTIVE()                                                                            \
	do {                                                                                           \
		if (cluster_shared_fs_active_ops == NULL)                                                  \
			ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR),                                       \
							errmsg("cluster_shared_fs is not initialised")));                      \
	} while (0)


void
cluster_shared_fs_open(RelFileLocator rlocator, ForkNumber forknum,
					   ClusterSharedFsHandle **out_handle)
{
	ENSURE_ACTIVE();
	cluster_shared_fs_active_ops->open(rlocator, forknum, out_handle);
}


void
cluster_shared_fs_close(ClusterSharedFsHandle *handle)
{
	ENSURE_ACTIVE();
	cluster_shared_fs_active_ops->close(handle);
}


int
cluster_shared_fs_read(ClusterSharedFsHandle *handle, BlockNumber blocknum, char *buf)
{
	ENSURE_ACTIVE();
	return cluster_shared_fs_active_ops->read(handle, blocknum, buf);
}


int
cluster_shared_fs_write(ClusterSharedFsHandle *handle, BlockNumber blocknum, const char *buf)
{
	ENSURE_ACTIVE();
	return cluster_shared_fs_active_ops->write(handle, blocknum, buf);
}


void
cluster_shared_fs_extend(ClusterSharedFsHandle *handle, BlockNumber blocknum)
{
	ENSURE_ACTIVE();
	cluster_shared_fs_active_ops->extend(handle, blocknum);
}


BlockNumber
cluster_shared_fs_nblocks(ClusterSharedFsHandle *handle)
{
	ENSURE_ACTIVE();
	return cluster_shared_fs_active_ops->nblocks(handle);
}


void
cluster_shared_fs_truncate(ClusterSharedFsHandle *handle, BlockNumber nblocks)
{
	ENSURE_ACTIVE();
	cluster_shared_fs_active_ops->truncate(handle, nblocks);
}


void
cluster_shared_fs_immedsync(ClusterSharedFsHandle *handle)
{
	ENSURE_ACTIVE();
	cluster_shared_fs_active_ops->immedsync(handle);
}


void
cluster_shared_fs_unlink(RelFileLocator rlocator, ForkNumber forknum)
{
	ENSURE_ACTIVE();
	cluster_shared_fs_active_ops->unlink(rlocator, forknum);
}

#endif /* USE_PGRAC_CLUSTER */
