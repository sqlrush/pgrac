/*-------------------------------------------------------------------------
 *
 * cluster_shared_fs_sharedfs.c
 *	  Shared-filesystem cluster_shared_fs backend (Stage 4.5a, id 3
 *	  CLUSTER_FS).
 *
 *	  The first cluster_shared_fs backend that stores relation files on
 *	  storage genuinely SHARED across nodes.  It mirrors the local
 *	  backend's 13-callback shape but resolves every relation path under
 *	  a single configured root, cluster.shared_data_dir, instead of each
 *	  node's own PGDATA:
 *
 *	    local backend:   relpathperm(rlocator)            (per-node PGDATA)
 *	    sharedfs:        <shared_data_dir>/<relpathperm>  (one shared tree)
 *
 *	  Because the path is owner-agnostic, any node that resolves the same
 *	  (spcOid, dbOid, relNumber, forknum) lands on the SAME file.  Two
 *	  nodes running the same DDL therefore agree on the relfilenode and
 *	  write the same shared file -- which is what lets spec-4.5 k-way
 *	  merged recovery honestly apply a crashed peer's SHARED-storage page
 *	  (spec-3.18 V-2 / spec-4.5 A-closure described why local/stub could
 *	  not: their pages are per-node).
 *
 *	  Non-properties (AD-004, like the local backend): this is a
 *	  passthrough over PG's fd.c VFD layer on a shared mount.  No SCSI-3
 *	  PR, no fence, no O_DIRECT, no 1GB segment splitting, no stripe, no
 *	  redundancy -- the shared filesystem / block layer (NFS, GFS2, OCFS2,
 *	  multi-attach + cluster FS, NVMe-oF) provides the cross-node
 *	  coherence.  pgrac does not self-build a volume manager.
 *
 *	  Production deployment additionally needs cross-node agreement on the
 *	  relfilenode <-> table mapping (feature #11 catalog coordination),
 *	  which spec-4.5a explicitly scopes out: the spec-4.5a harness relies
 *	  on the same-DDL same-relfile coincidence (asserted by t/248 L0
 *	  pg_relation_filepath preflight), not on #11.
 *
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/backend/cluster/storage/cluster_shared_fs_sharedfs.c
 *
 * NOTES
 *	  This is a pgrac-original file (no derivation from PostgreSQL).
 *	  The cluster_shared_fs_sharedfs_* symbols are available only when
 *	  configured with --enable-cluster (USE_PGRAC_CLUSTER defined).
 *
 *	  Spec: spec-4.5a-shared-storage-data-backend.md (FROZEN v1.0, D1)
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <errno.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include "common/file_perm.h"
#include "common/relpath.h"
#include "storage/block.h"
#include "storage/fd.h"
#include "utils/memutils.h"
#include "utils/wait_event.h"

#include "cluster/cluster_guc.h"
#include "cluster/cluster_inject.h"
#include "cluster/storage/cluster_shared_fs.h"


#ifdef USE_PGRAC_CLUSTER

/*
 * Per-fork open-file state.  Identical shape to the local backend's
 * handle: the only difference between the two backends is which path
 * relpath() resolves to, so the in-memory state is the same.  Lives in
 * TopMemoryContext to survive the smgr open-at-first-touch /
 * close-at-release pattern.
 */
struct ClusterSharedFsHandle {
	RelFileLocator rlocator;
	ForkNumber forknum;
	File vfd;
	bool opened;
};


/*
 * cluster_shared_fs_sharedfs_relpath -- relation path under the shared
 *	root.  Unlike the local backend (relpathperm = relative to each
 *	node's PGDATA cwd), this prepends the configured shared_data_dir so
 *	every node resolves to the same shared file.
 *
 *	FATAL-free guard: shared_data_dir must be a non-empty absolute path.
 *	cluster.shared_data_dir is PGC_POSTMASTER and cross-checked at
 *	startup (cluster_shared_fs_init), so an empty value here means the
 *	backend was activated without its required configuration; report a
 *	clear ERROR rather than silently writing under a relative cwd.
 */
static char *
cluster_shared_fs_sharedfs_relpath(RelFileLocator rlocator, ForkNumber forknum)
{
	char *rel;
	char *full;

	if (cluster_shared_data_dir == NULL || cluster_shared_data_dir[0] == '\0')
		ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
						errmsg("cluster.shared_data_dir must be set when "
							   "cluster.shared_storage_backend=cluster_fs"),
						errhint("Set cluster.shared_data_dir to an absolute path on the "
								"shared mount that every node points at.")));

	rel = relpathperm(rlocator, forknum); /* base/<db>/<relfile> */
	full = psprintf("%s/%s", cluster_shared_data_dir, rel);
	pfree(rel);
	return full; /* <root>/base/<db>/<relfile> */
}


/*
 * cluster_shared_fs_sharedfs_ensure_parent -- mkdir -p the directory
 *	holding `path`.
 *
 *	The local backend relies on PG's TablespaceCreateDbspace having
 *	already created base/<db> under PGDATA.  Under the shared root those
 *	directories do not exist yet, so create() must materialise the parent
 *	tree (<root>/base, <root>/base/<db>) before opening the file.  The
 *	root itself is the user's responsibility (and is validated, with its
 *	cross-node sentinel, at startup).
 */
static void
cluster_shared_fs_sharedfs_ensure_parent(const char *path)
{
	char *dir = pstrdup(path);
	char *slash = strrchr(dir, '/');

	if (slash != NULL && slash != dir) {
		*slash = '\0';
		/* pg_mkdir_p creates every missing component; EEXIST is fine. */
		if (pg_mkdir_p(dir, pg_dir_create_mode) != 0 && errno != EEXIST)
			ereport(ERROR, (errcode_for_file_access(),
							errmsg("cluster_shared_fs.shared_fs: could not create parent "
								   "directory \"%s\": %m",
								   dir)));
	}
	pfree(dir);
}


static bool
cluster_shared_fs_sharedfs_exists(RelFileLocator rlocator, ForkNumber forknum)
{
	char *path;
	struct stat st;
	bool result;

	path = cluster_shared_fs_sharedfs_relpath(rlocator, forknum);

	/*
	 * Distinguish ENOENT ("file does not exist") from other stat()
	 * failures (EACCES, EIO, ENOTDIR).  Mirrors the local backend's
	 * spec-1.7.2 F3 fix: treating every failure as "missing" would hide
	 * a permission/IO error as a vanished relation.
	 */
	if (stat(path, &st) == 0)
		result = true;
	else if (errno == ENOENT)
		result = false;
	else {
		ereport(ERROR, (errcode_for_file_access(),
						errmsg("cluster_shared_fs.shared_fs: could not stat file \"%s\": %m", path),
						errhint("Check shared-filesystem permissions and health.")));
		result = false; /* unreachable */
	}

	pfree(path);
	return result;
}


static void
cluster_shared_fs_sharedfs_open_existing(RelFileLocator rlocator, ForkNumber forknum,
										 ClusterSharedFsHandle **out_handle)
{
	ClusterSharedFsHandle *handle;
	char *path;
	File vfd;
	MemoryContext oldcxt;

	CLUSTER_INJECTION_POINT("cluster-shared-fs-sharedfs-open");

	path = cluster_shared_fs_sharedfs_relpath(rlocator, forknum);

	vfd = PathNameOpenFile(path, O_RDWR | PG_BINARY);
	if (vfd < 0)
		ereport(
			ERROR,
			(errcode_for_file_access(),
			 errmsg("cluster_shared_fs.shared_fs: could not open existing file \"%s\": %m", path)));

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
cluster_shared_fs_sharedfs_create(RelFileLocator rlocator, ForkNumber forknum, bool isRedo,
								  ClusterSharedFsHandle **out_handle)
{
	ClusterSharedFsHandle *handle;
	char *path;
	File vfd;
	MemoryContext oldcxt;

	CLUSTER_INJECTION_POINT("cluster-shared-fs-sharedfs-open");

	path = cluster_shared_fs_sharedfs_relpath(rlocator, forknum);

	/* The shared root has no pre-created base/<db>; materialise it. */
	cluster_shared_fs_sharedfs_ensure_parent(path);

	/*
	 * md.c mdcreate (md.c:218) semantics, identical to the local backend:
	 *   !isRedo -> O_CREAT|O_EXCL (error on an existing file -- a stale
	 *              relfilenode file from a crashed CREATE must not be
	 *              silently reused with its old block contents);
	 *   isRedo  -> idempotent (reopen an existing file).
	 */
	vfd = PathNameOpenFile(path, O_RDWR | O_CREAT | O_EXCL | PG_BINARY);
	if (vfd < 0 && isRedo && errno == EEXIST)
		vfd = PathNameOpenFile(path, O_RDWR | PG_BINARY);
	if (vfd < 0)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("cluster_shared_fs.shared_fs: could not create file \"%s\": %m", path)));

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
cluster_shared_fs_sharedfs_close(ClusterSharedFsHandle *handle)
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
cluster_shared_fs_sharedfs_read(ClusterSharedFsHandle *handle, BlockNumber blocknum, char *buf)
{
	off_t offset;
	int nbytes;

	Assert(handle != NULL && handle->opened);

	offset = (off_t)blocknum * BLCKSZ;
	nbytes = FileRead(handle->vfd, buf, BLCKSZ, offset, WAIT_EVENT_DATA_FILE_READ);

	if (nbytes < 0)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("cluster_shared_fs.shared_fs: could not read block %u: %m", blocknum)));

	if (nbytes != BLCKSZ)
		ereport(ERROR,
				(errcode(ERRCODE_DATA_CORRUPTED),
				 errmsg("cluster_shared_fs.shared_fs: short read of block %u (got %d, expected %d)",
						blocknum, nbytes, BLCKSZ)));
	return nbytes;
}


static int
cluster_shared_fs_sharedfs_write(ClusterSharedFsHandle *handle, BlockNumber blocknum,
								 const char *buf)
{
	off_t offset;
	int nbytes;

	Assert(handle != NULL && handle->opened);

	offset = (off_t)blocknum * BLCKSZ;
	nbytes = FileWrite(handle->vfd, buf, BLCKSZ, offset, WAIT_EVENT_DATA_FILE_WRITE);

	if (nbytes < 0)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("cluster_shared_fs.shared_fs: could not write block %u: %m", blocknum)));

	if (nbytes != BLCKSZ)
		ereport(
			ERROR,
			(errcode(ERRCODE_DISK_FULL),
			 errmsg("cluster_shared_fs.shared_fs: short write of block %u (wrote %d, expected %d)",
					blocknum, nbytes, BLCKSZ)));
	return nbytes;
}


static void
cluster_shared_fs_sharedfs_extend(ClusterSharedFsHandle *handle, BlockNumber blocknum)
{
	/* Zero-fill the new tail block; mirrors mdextend(). */
	char zerobuf[BLCKSZ];

	memset(zerobuf, 0, sizeof(zerobuf));
	cluster_shared_fs_sharedfs_write(handle, blocknum, zerobuf);
}


static BlockNumber
cluster_shared_fs_sharedfs_nblocks(ClusterSharedFsHandle *handle)
{
	off_t size;

	Assert(handle != NULL && handle->opened);

	size = FileSize(handle->vfd);
	if (size < 0)
		ereport(ERROR, (errcode_for_file_access(),
						errmsg("cluster_shared_fs.shared_fs: could not stat file: %m")));

	/*
	 * File size MUST be a whole multiple of BLCKSZ; a partial tail block
	 * is storage corruption (post-crash truncated tail, misalignment) and
	 * is reported rather than silently truncated by integer division.
	 * Mirrors the local backend's Sprint A hardening.
	 */
	if (size % BLCKSZ != 0)
		ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED),
						errmsg("cluster_shared_fs.shared_fs: relation file size " INT64_FORMAT
							   " is not a multiple of BLCKSZ %d",
							   (int64)size, BLCKSZ),
						errhint("This indicates shared-storage corruption (truncated tail / "
								"misalignment).  Restore from a known-good backup.")));

	return (BlockNumber)(size / BLCKSZ);
}


static void
cluster_shared_fs_sharedfs_truncate(ClusterSharedFsHandle *handle, BlockNumber nblocks)
{
	off_t newsize;

	Assert(handle != NULL && handle->opened);

	newsize = (off_t)nblocks * BLCKSZ;
	if (FileTruncate(handle->vfd, newsize, WAIT_EVENT_DATA_FILE_TRUNCATE) < 0)
		ereport(ERROR, (errcode_for_file_access(),
						errmsg("cluster_shared_fs.shared_fs: could not truncate to %u blocks: %m",
							   nblocks)));
}


static void
cluster_shared_fs_sharedfs_immedsync(ClusterSharedFsHandle *handle)
{
	Assert(handle != NULL && handle->opened);

	if (FileSync(handle->vfd, WAIT_EVENT_DATA_FILE_IMMEDIATE_SYNC) < 0)
		ereport(ERROR, (errcode_for_file_access(),
						errmsg("cluster_shared_fs.shared_fs: could not fsync: %m")));
}


static void
cluster_shared_fs_sharedfs_unlink(RelFileLocator rlocator, ForkNumber forknum)
{
	char *path = cluster_shared_fs_sharedfs_relpath(rlocator, forknum);

	if (unlink(path) < 0 && errno != ENOENT)
		ereport(ERROR, (errcode_for_file_access(),
						errmsg("cluster_shared_fs.shared_fs: could not unlink \"%s\": %m", path)));

	pfree(path);
}


/* Lifecycle: nothing to set up or tear down at the backend level. */
static void
cluster_shared_fs_sharedfs_init(void)
{}
static void
cluster_shared_fs_sharedfs_shutdown(void)
{}


const ClusterSharedFsOps cluster_shared_fs_sharedfs_ops = {
	.name = "shared_fs",
	.id = CLUSTER_SHARED_FS_BACKEND_CLUSTER_FS,

	.exists = cluster_shared_fs_sharedfs_exists,
	.open_existing = cluster_shared_fs_sharedfs_open_existing,
	.create = cluster_shared_fs_sharedfs_create,
	.close = cluster_shared_fs_sharedfs_close,
	.read = cluster_shared_fs_sharedfs_read,
	.write = cluster_shared_fs_sharedfs_write,
	.extend = cluster_shared_fs_sharedfs_extend,
	.nblocks = cluster_shared_fs_sharedfs_nblocks,
	.truncate = cluster_shared_fs_sharedfs_truncate,
	.immedsync = cluster_shared_fs_sharedfs_immedsync,
	.unlink = cluster_shared_fs_sharedfs_unlink,

	.init = cluster_shared_fs_sharedfs_init,
	.shutdown = cluster_shared_fs_sharedfs_shutdown,
};

#endif /* USE_PGRAC_CLUSTER */
