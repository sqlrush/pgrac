/*-------------------------------------------------------------------------
 *
 * cluster_shared_fs_local.c
 *	  Single-node passthrough cluster_shared_fs backend (Stage 1.1).
 *
 *	  Wraps PG's existing fd.c VFD layer so that the cluster_shared_fs
 *	  vtable can be exercised end-to-end (open / read / write / extend
 *	  / nblocks / truncate / immedsync / close / unlink) without any
 *	  cluster machinery.  Used by:
 *
 *	    - the cluster_unit / cluster_tap tests in stage 1.1, to
 *	      validate the vtable shape over a real I/O path;
 *	    - stage 1.2 onwards, when smgr_cluster.c routes block I/O
 *	      through cluster_shared_fs and a single-node deployment can
 *	      pick this backend explicitly via
 *	      cluster.shared_storage_backend=local.
 *
 *	  Important non-properties (see docs/cluster-shared-fs-design.md
 *	  §7.3): no SCSI-3 PR, no fence, no GCS / Cache Fusion interaction,
 *	  no lock coordination, no segment splitting (1GB md.c segments
 *	  are NOT replicated here).  This backend is "single-node
 *	  passthrough" not "production single-node deployment"; users on
 *	  a single node should keep cluster.shared_storage_backend=stub
 *	  (default) so that PG's normal md.c continues to handle storage.
 *
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/backend/cluster/storage/cluster_shared_fs_local.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <errno.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include "common/relpath.h"
#include "storage/block.h"
#include "storage/fd.h"
#include "utils/memutils.h"
#include "utils/wait_event.h"

#include "cluster/cluster_inject.h"
#include "cluster/storage/cluster_shared_fs.h"


#ifdef USE_PGRAC_CLUSTER

static const ClusterSharedFsCaps cluster_shared_fs_local_caps = {
	.supports_odirect = false,
	.required_io_alignment = 0,
	.supports_scsi3_pr = false,
	.durability_class = CLUSTER_DURABILITY_BUFFERED,
	.max_nodes = 1,
};

/*
 * Per-fork open-file state.  Owned by the caller via the opaque
 * ClusterSharedFsHandle pointer; lives in TopMemoryContext so it
 * survives the smgr-style usage pattern (open at relation-first-touch,
 * close at smgr-relation-release).
 */
struct ClusterSharedFsHandle {
	RelFileLocator rlocator;
	ForkNumber forknum;
	File vfd;
	bool opened;
};


static char *
cluster_shared_fs_local_relpath(RelFileLocator rlocator, ForkNumber forknum)
{
	/*
	 * relpathperm = "regular relation" path (no temporary-relation
	 * backend suffix).  Stage 1.1 local backend does not handle temp
	 * relations: PG's bufmgr already refuses to take temp blocks
	 * through smgr_cluster, and stage 1.2's cluster smgr will also
	 * route temp through md.c directly.
	 */
	return relpathperm(rlocator, forknum);
}


/*
 * Sprint A 2026-05-02 (spec-1.X-cluster-smgr-hardening): split the
 * old `_local_open` (which carried O_CREAT side effect violating
 * vtable contract) into three separate callbacks: exists / open_
 * existing / create.  This eliminates the cluster_smgr_exists hack
 * that used local path stat() to bypass the vtable.
 */
static bool
cluster_shared_fs_local_exists(RelFileLocator rlocator, ForkNumber forknum)
{
	char *path;
	struct stat st;
	bool result;

	path = cluster_shared_fs_local_relpath(rlocator, forknum);

	/*
	 * PGRAC: spec-1.7.2 2026-05-03 F3 fix — distinguish "file does not
	 * exist" (ENOENT) from "stat failed for another reason" (EACCES,
	 * EIO, ENOTDIR, etc.).  Treating every stat() failure as "does
	 * not exist" hid permission and I/O errors as "file missing", which
	 * could let a corrupted catalog claim a relation is gone.
	 */
	if (stat(path, &st) == 0) {
		result = true;
	} else if (errno == ENOENT) {
		result = false;
	} else {
		int save_errno = errno;

		ereport(ERROR, (errcode_for_file_access(),
						errmsg("could not stat cluster_shared_fs.local file \"%s\": %m", path),
						errhint("Check filesystem permissions and disk health.")));
		/* unreachable (errcode_for_file_access reads errno) */
		errno = save_errno;
		result = false;
	}

	pfree(path);
	return result;
}

static void
cluster_shared_fs_local_open_existing(RelFileLocator rlocator, ForkNumber forknum,
									  ClusterSharedFsHandle **out_handle)
{
	ClusterSharedFsHandle *handle;
	char *path;
	File vfd;
	MemoryContext oldcxt;

	CLUSTER_INJECTION_POINT("cluster-shared-fs-local-open");

	path = cluster_shared_fs_local_relpath(rlocator, forknum);

	/*
	 * O_RDWR only -- caller asserts file exists.  Returns ENOENT path
	 * if missing (caller is responsible for using create() before
	 * open_existing() or for handling errcode_for_file_access()).
	 */
	vfd = PathNameOpenFile(path, O_RDWR | PG_BINARY);
	if (vfd < 0)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("cluster_shared_fs.local: could not open existing file \"%s\": %m", path)));

	oldcxt = MemoryContextSwitchTo(TopMemoryContext);
	handle = (ClusterSharedFsHandle *)palloc0(sizeof(ClusterSharedFsHandle));
	MemoryContextSwitchTo(oldcxt);

	handle->rlocator = rlocator;
	handle->forknum = forknum;
	handle->vfd = vfd;
	handle->opened = true;

	*out_handle = handle;

	pfree(path);
}

static void
cluster_shared_fs_local_create(RelFileLocator rlocator, ForkNumber forknum, bool isRedo,
							   ClusterSharedFsHandle **out_handle)
{
	ClusterSharedFsHandle *handle;
	char *path;
	File vfd;
	MemoryContext oldcxt;

	CLUSTER_INJECTION_POINT("cluster-shared-fs-local-open");

	path = cluster_shared_fs_local_relpath(rlocator, forknum);

	/*
	 * PGRAC: spec-1.7.2 2026-05-03 F1 fix — match md.c mdcreate
	 * (md.c:218) semantics:
	 *   !isRedo -> O_CREAT|O_EXCL: error on existing file
	 *   isRedo  -> O_CREAT (no O_EXCL): idempotent for WAL redo replay
	 *
	 * Without this, a stale relfilenode file from a crashed CREATE
	 * could be silently reused, leaving stale block contents visible
	 * to the new relation -- a P1 data integrity hole.
	 *
	 * Mirrors md.c's two-step retry: try O_CREAT|O_EXCL first; on
	 * EEXIST during redo, fall back to opening the existing file.
	 */
	vfd = PathNameOpenFile(path, O_RDWR | O_CREAT | O_EXCL | PG_BINARY);
	if (vfd < 0 && isRedo && errno == EEXIST) {
		/* Redo replay: existing file is OK; reopen without O_EXCL. */
		vfd = PathNameOpenFile(path, O_RDWR | PG_BINARY);
	}
	if (vfd < 0)
		ereport(ERROR, (errcode_for_file_access(),
						errmsg("cluster_shared_fs.local: could not create file \"%s\": %m", path)));

	oldcxt = MemoryContextSwitchTo(TopMemoryContext);
	handle = (ClusterSharedFsHandle *)palloc0(sizeof(ClusterSharedFsHandle));
	MemoryContextSwitchTo(oldcxt);

	handle->rlocator = rlocator;
	handle->forknum = forknum;
	handle->vfd = vfd;
	handle->opened = true;

	*out_handle = handle;

	pfree(path);
}


static void
cluster_shared_fs_local_close(ClusterSharedFsHandle *handle)
{
	if (handle == NULL)
		return;
	if (handle->opened) {
		FileClose(handle->vfd);
		handle->opened = false;
	}
	pfree(handle);
}


static int
cluster_shared_fs_local_read(ClusterSharedFsHandle *handle, BlockNumber blocknum, char *buf)
{
	off_t offset;
	int nbytes;

	Assert(handle != NULL && handle->opened);

	offset = (off_t)blocknum * BLCKSZ;
	nbytes = FileRead(handle->vfd, buf, BLCKSZ, offset, WAIT_EVENT_DATA_FILE_READ);

	if (nbytes < 0)
		ereport(ERROR, (errcode_for_file_access(),
						errmsg("cluster_shared_fs.local: could not read block %u: %m", blocknum)));

	if (nbytes != BLCKSZ)
		ereport(ERROR,
				(errcode(ERRCODE_DATA_CORRUPTED),
				 errmsg("cluster_shared_fs.local: short read of block %u (got %d, expected %d)",
						blocknum, nbytes, BLCKSZ)));
	return nbytes;
}


static int
cluster_shared_fs_local_write(ClusterSharedFsHandle *handle, BlockNumber blocknum, const char *buf)
{
	off_t offset;
	int nbytes;

	Assert(handle != NULL && handle->opened);

	offset = (off_t)blocknum * BLCKSZ;
	nbytes = FileWrite(handle->vfd, buf, BLCKSZ, offset, WAIT_EVENT_DATA_FILE_WRITE);

	if (nbytes < 0)
		ereport(ERROR, (errcode_for_file_access(),
						errmsg("cluster_shared_fs.local: could not write block %u: %m", blocknum)));

	if (nbytes != BLCKSZ)
		ereport(ERROR,
				(errcode(ERRCODE_DISK_FULL),
				 errmsg("cluster_shared_fs.local: short write of block %u (wrote %d, expected %d)",
						blocknum, nbytes, BLCKSZ)));
	return nbytes;
}


static void
cluster_shared_fs_local_extend(ClusterSharedFsHandle *handle, BlockNumber blocknum)
{
	/*
	 * Extend = write a zero-filled block at the new tail.  Mirrors
	 * mdextend() semantics: PG callers expect zero pages at the new
	 * positions until they overwrite with real data.
	 */
	char zerobuf[BLCKSZ];

	memset(zerobuf, 0, sizeof(zerobuf));
	cluster_shared_fs_local_write(handle, blocknum, zerobuf);
}


static BlockNumber
cluster_shared_fs_local_nblocks(ClusterSharedFsHandle *handle)
{
	off_t size;

	Assert(handle != NULL && handle->opened);

	size = FileSize(handle->vfd);
	if (size < 0)
		ereport(ERROR, (errcode_for_file_access(),
						errmsg("cluster_shared_fs.local: could not stat file: %m")));

	/*
	 * Sprint A 2026-05-02 (codex review supplemental P2):
	 *
	 * Partial-block detection: file size MUST be a whole multiple of
	 * BLCKSZ.  Previously this function silently truncated via integer
	 * division, hiding storage corruption (post-crash truncated tail,
	 * filesystem misalignment, etc.).  ERROR-out lets the user see the
	 * issue immediately rather than discovering it via missing rows
	 * downstream.
	 *
	 * Spec: spec-1.X-cluster-smgr-hardening Sprint A item #2.
	 */
	if (size % BLCKSZ != 0)
		ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED),
						errmsg("cluster_shared_fs.local: relation file size " INT64_FORMAT
							   " is not a multiple of BLCKSZ %d",
							   (int64)size, BLCKSZ),
						errdetail("Trailing %ld bytes are partial-block; would have been silently "
								  "truncated by integer division before Sprint A hardening.",
								  (long)(size % BLCKSZ)),
						errhint("This indicates storage corruption (e.g. post-crash truncated "
								"tail, filesystem misalignment).  "
								"Restore the relation file from a known-good backup, or run "
								"pg_resetwal if the cluster is in a recoverable state.")));

	return (BlockNumber)(size / BLCKSZ);
}


static void
cluster_shared_fs_local_truncate(ClusterSharedFsHandle *handle, BlockNumber nblocks)
{
	off_t newsize;

	Assert(handle != NULL && handle->opened);

	newsize = (off_t)nblocks * BLCKSZ;
	if (FileTruncate(handle->vfd, newsize, WAIT_EVENT_DATA_FILE_TRUNCATE) < 0)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("cluster_shared_fs.local: could not truncate to %u blocks: %m", nblocks)));
}


static void
cluster_shared_fs_local_immedsync(ClusterSharedFsHandle *handle)
{
	Assert(handle != NULL && handle->opened);

	if (FileSync(handle->vfd, WAIT_EVENT_DATA_FILE_IMMEDIATE_SYNC) < 0)
		ereport(ERROR, (errcode_for_file_access(),
						errmsg("cluster_shared_fs.local: could not fsync: %m")));
}


static void
cluster_shared_fs_local_unlink(RelFileLocator rlocator, ForkNumber forknum)
{
	char *path = cluster_shared_fs_local_relpath(rlocator, forknum);

	if (unlink(path) < 0 && errno != ENOENT)
		ereport(ERROR, (errcode_for_file_access(),
						errmsg("cluster_shared_fs.local: could not unlink \"%s\": %m", path)));

	pfree(path);
}


/* Lifecycle: nothing to set up or tear down at the backend level. */
static void
cluster_shared_fs_local_init(void)
{}
static void
cluster_shared_fs_local_shutdown(void)
{}

static int
cluster_shared_fs_local_barrier_sync(ClusterSharedFsHandle *handle)
{
	cluster_shared_fs_local_immedsync(handle);
	return 0;
}

static int
cluster_shared_fs_local_register_fence_key(int node_id)
{
	(void)node_id;
	return EOPNOTSUPP;
}

static ClusterFenceCapability
cluster_shared_fs_local_fence_capability(void)
{
	return CLUSTER_FENCE_CAP_NONE;
}


const ClusterSharedFsOps cluster_shared_fs_local_ops = {
	.name = "local",
	.id = CLUSTER_SHARED_FS_BACKEND_LOCAL,
	.caps = &cluster_shared_fs_local_caps,

	.exists = cluster_shared_fs_local_exists,
	.open_existing = cluster_shared_fs_local_open_existing,
	.create = cluster_shared_fs_local_create,
	.close = cluster_shared_fs_local_close,
	.read = cluster_shared_fs_local_read,
	.write = cluster_shared_fs_local_write,
	.extend = cluster_shared_fs_local_extend,
	.nblocks = cluster_shared_fs_local_nblocks,
	.truncate = cluster_shared_fs_local_truncate,
	.immedsync = cluster_shared_fs_local_immedsync,
	.unlink = cluster_shared_fs_local_unlink,

	.init = cluster_shared_fs_local_init,
	.shutdown = cluster_shared_fs_local_shutdown,

	.barrier_sync = cluster_shared_fs_local_barrier_sync,
	.register_fence_key = cluster_shared_fs_local_register_fence_key,
	.fence_capability = cluster_shared_fs_local_fence_capability,
};

#endif /* USE_PGRAC_CLUSTER */
