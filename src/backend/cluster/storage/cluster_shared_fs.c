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
 *	    - the caller-facing dispatch wrappers that forward to
 *	      active_ops->* (core storage, lifecycle, durability/fence, and
 *	      advisory callbacks).
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

	if (ops == NULL || ops->name == NULL || ops->caps == NULL)
		ereport(FATAL,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("cluster_shared_fs backend registered with NULL ops, name, or caps")));

	id = (int)ops->id;
	if (id < 0 || id >= CLUSTER_SHARED_FS_BACKEND_MAX)
		ereport(FATAL,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("cluster_shared_fs backend \"%s\" has out-of-range id %d", ops->name, id),
				 errdetail("Valid range is [0, %d).", CLUSTER_SHARED_FS_BACKEND_MAX)));

	if (ops->exists == NULL || ops->open_existing == NULL || ops->create == NULL
		|| ops->close == NULL || ops->read == NULL || ops->write == NULL || ops->extend == NULL
		|| ops->nblocks == NULL || ops->truncate == NULL || ops->immedsync == NULL
		|| ops->unlink == NULL || ops->init == NULL || ops->shutdown == NULL
		|| ops->barrier_sync == NULL || ops->register_fence_key == NULL
		|| ops->fence_capability == NULL || ops->prefetch == NULL || ops->writeback == NULL)
		ereport(FATAL, (errcode(ERRCODE_INTERNAL_ERROR),
						errmsg("cluster_shared_fs backend \"%s\" has NULL callbacks", ops->name),
						errdetail("All provider vtable members must be non-NULL "
								  "(Sprint A 2026-05-02: open split into exists / "
								  "open_existing / create).")));

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
	cluster_shared_fs_register_backend(&cluster_shared_fs_block_device_ops);
	/*
	 * PGRAC: spec-4.5a D3 -- shared_fs (id 3 CLUSTER_FS) is the first
	 * cluster_shared_fs backend on genuinely cross-node-shared storage.
	 * Registering it turns cluster.shared_storage_backend=cluster_fs from
	 * the Stage-2 "not registered -> FATAL" placeholder into a live
	 * passthrough over the configured cluster.shared_data_dir root.
	 */
	cluster_shared_fs_register_backend(&cluster_shared_fs_sharedfs_ops);

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
				 errhint("Backends \"stub\", \"local\", \"block_device\", and "
						 "\"cluster_fs\" (shared_fs) are built in; \"rbd\" and "
						 "\"multi_attach\" remain future Stage 6 backends.  Set "
						 "cluster.shared_storage_backend to one of the built-in "
						 "backends in postgresql.conf and restart.")));

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
	if (cluster_smgr_user_relations
		&& cluster_shared_storage_backend == CLUSTER_SHARED_FS_BACKEND_STUB)
		ereport(FATAL,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("cluster.smgr_user_relations=on requires shared_storage_backend != stub"),
				 errhint("Set cluster.shared_storage_backend=local in postgresql.conf "
						 "to enable cluster_smgr routing, or revert "
						 "cluster.smgr_user_relations=off.")));

	/*
	 * PGRAC: spec-1.7.2 2026-05-03 F2 fix — EXPERIMENTAL WARNING
	 * lifecycle (post-codex round 3 IsUnderPostmaster guard).
	 *
	 * Spec-1.7.1 Sprint A originally placed this WARNING in
	 * cluster_smgr_init() (cluster_smgr.c) under the !IsUnderPostmaster
	 * guard.  Codex review 2026-05-03 found that PG smgrinit() is "called
	 * during backend startup, *not* during postmaster start" (see
	 * smgr.c:162) AND backends always have IsUnderPostmaster=true, so
	 * the WARNING never actually fired in normal flow -- a P1 lifecycle
	 * bug.
	 *
	 * cluster_shared_fs_init runs from CreateSharedMemoryAndSemaphores
	 * (storage/ipc/ipci.c).  On Linux/macOS fork model the postmaster
	 * runs it once with IsUnderPostmaster=false; child backends inherit
	 * shared state via fork() without re-running it.  But on Windows
	 * EXEC_BACKEND mode every child re-runs it with IsUnderPostmaster=
	 * true, so without an !IsUnderPostmaster guard we'd get one WARNING
	 * per backend startup -- spammy and contrary to "postmaster-once"
	 * semantics (codex round 3 P2 finding 1, 2026-05-03).
	 *
	 * Same call site as the stub-backend FATAL cross-check above: GUC
	 * has been parsed, backends have been registered.  Emitting here
	 * with !IsUnderPostmaster guarantees one WARNING per postmaster
	 * startup and zero per backend on ALL platforms when cluster.smgr_
	 * user_relations is on.
	 *
	 * 019_smgr_cluster.pl L2b / L8 assert this WARNING appears in the
	 * postmaster logfile, so future regressions are caught.  Spec-1.7.2
	 * DoD #19 makes this a hard gate.
	 */
	if (cluster_smgr_user_relations && !IsUnderPostmaster)
		ereport(WARNING,
				(errmsg("cluster.smgr_user_relations is experimental"),
				 errdetail("Shared-storage fsync/barrier registration is active, but "
						   "cross-instance cache invalidation and catalog coordination remain "
						   "experimental."),
				 errhint("Do not treat this early shared-storage path as shipped until the "
						 "spec-5.19/5.21 close-out and final Stage 6 D0 re-ground are complete.")));

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


const ClusterSharedFsCaps *
cluster_shared_fs_get_active_caps(void)
{
	if (cluster_shared_fs_active_ops == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR), errmsg("cluster_shared_fs is not initialised")));
	return cluster_shared_fs_active_ops->caps;
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


/*
 * Sprint A 2026-05-02 (spec-1.X-cluster-smgr-hardening): vtable split
 * `open` (ambiguous create-or-open with O_CREAT side effect) into 3
 * callbacks.  cluster_smgr / future Stage 2 共享存储后端 callers MUST
 * use the appropriate variant -- exists() for read-only existence
 * check, open_existing() to open a known-existing file, create() for
 * new file creation.
 */
bool
cluster_shared_fs_exists(RelFileLocator rlocator, ForkNumber forknum)
{
	ENSURE_ACTIVE();
	return cluster_shared_fs_active_ops->exists(rlocator, forknum);
}

void
cluster_shared_fs_open_existing(RelFileLocator rlocator, ForkNumber forknum,
								ClusterSharedFsHandle **out_handle)
{
	ENSURE_ACTIVE();
	cluster_shared_fs_active_ops->open_existing(rlocator, forknum, out_handle);
}

void
cluster_shared_fs_create(RelFileLocator rlocator, ForkNumber forknum, bool isRedo,
						 ClusterSharedFsHandle **out_handle)
{
	ENSURE_ACTIVE();
	cluster_shared_fs_active_ops->create(rlocator, forknum, isRedo, out_handle);
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

int
cluster_shared_fs_barrier_sync(ClusterSharedFsHandle *handle)
{
	ENSURE_ACTIVE();
	return cluster_shared_fs_active_ops->barrier_sync(handle);
}

int
cluster_shared_fs_register_fence_key(int node_id)
{
	ENSURE_ACTIVE();
	return cluster_shared_fs_active_ops->register_fence_key(node_id);
}

ClusterFenceCapability
cluster_shared_fs_fence_capability(void)
{
	ENSURE_ACTIVE();
	return cluster_shared_fs_active_ops->fence_capability();
}


bool
cluster_shared_fs_prefetch(ClusterSharedFsHandle *handle, BlockNumber blocknum)
{
	ENSURE_ACTIVE();
	return cluster_shared_fs_active_ops->prefetch(handle, blocknum);
}


void
cluster_shared_fs_writeback(ClusterSharedFsHandle *handle, BlockNumber blocknum,
							BlockNumber nblocks)
{
	ENSURE_ACTIVE();
	cluster_shared_fs_active_ops->writeback(handle, blocknum, nblocks);
}

#endif /* USE_PGRAC_CLUSTER */
