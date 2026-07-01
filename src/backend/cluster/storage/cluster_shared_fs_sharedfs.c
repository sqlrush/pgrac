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
#include <sys/file.h>
#include <sys/stat.h>
#include <unistd.h>

#include "common/file_perm.h"
#include "common/relpath.h"
#include "miscadmin.h"
#include "port/pg_crc32c.h"
#include "storage/block.h"
#include "storage/fd.h"
#include "utils/memutils.h"
#include "utils/wait_event.h"

#include "cluster/cluster_conf.h"
#include "cluster/cluster_guc.h"
#include "cluster/cluster_inject.h"
#include "cluster/storage/cluster_shared_fs.h"


#ifdef USE_PGRAC_CLUSTER

static const ClusterSharedFsCaps cluster_shared_fs_sharedfs_caps = {
	.supports_odirect = false,
	.required_io_alignment = 0,
	.supports_scsi3_pr = false,
	.durability_class = CLUSTER_DURABILITY_BUFFERED,
	.max_nodes = CLUSTER_MAX_NODES,
};

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
	 * Owner-agnostic create (spec-4.5a §3.1).  Unlike md.c mdcreate's
	 * O_EXCL (a per-node data dir, where an existing file can only be a
	 * stale crashed CREATE), the shared root is written by EVERY
	 * participant: a peer that ran the same DDL has legitimately created
	 * this relfilenode file already, and its blocks ARE the shared
	 * relation.  Adopting the existing file is the point of a shared-data
	 * backend; per-relation creation ownership needs the cluster catalog
	 * protocol (feature #11), not file-level O_EXCL.  isRedo keeps the
	 * same open-existing behaviour.
	 */
	vfd = PathNameOpenFile(path, O_RDWR | O_CREAT | O_EXCL | PG_BINARY);
	if (vfd < 0 && errno == EEXIST) {
		vfd = PathNameOpenFile(path, O_RDWR | PG_BINARY);
		if (vfd >= 0 && !isRedo)
			elog(DEBUG1, "cluster_shared_fs.shared_fs: adopting existing shared file \"%s\"", path);
	}
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


/* ----------------------------------------------------------------
 * Cross-node shared-root sentinel (spec-4.5a D2).
 *
 *	<shared_data_dir>/pgrac_shared.control is a small fixed-size control
 *	file that PROVES two nodes point at the same shared root.  Each node
 *	records its cluster.node_id in the participant set on attach; the
 *	merged-recovery capability gate (cluster_recovery_merge.c) only
 *	engages for a crashed peer whose node_id is a participant -- a lone
 *	node pointing at its own empty directory is the only participant, so
 *	the gate stays fail-closed.
 *
 *	Written THROUGH backend-internal raw path I/O (open/pread/pwrite/
 *	fsync), NOT the ClusterSharedFsOps vtable (which only addresses
 *	relation files by RelFileLocator/forknum -- there is no arbitrary
 *	control-file callback).  Cross-node sharedness still holds because
 *	every node's backend resolves the SAME absolute path under the root.
 *
 *	Cross-node write serialization uses flock(); on a shared mount where
 *	advisory locks are unreliable a clobbered append merely drops a
 *	participant, which makes the capability gate fail CLOSED (the node
 *	re-attaches on its next start) -- never a false engage.
 * ----------------------------------------------------------------
 */
#define PGRAC_SHARED_CONTROL_MAGIC 0x50475343 /* 'PGSC' */
#define PGRAC_SHARED_CONTROL_VERSION 1
#define PGRAC_SHARED_CONTROL_FILE "pgrac_shared.control"
/* storage_uuid is 32 hex chars + NUL; the length lives in the public header
 * (CLUSTER_SHARED_UUID_LEN) so the spec-5.6 CF contract can size matching
 * buffers without duplicating the literal. */

typedef struct PgracSharedControl {
	uint32 magic;
	uint32 layout_version;
	char storage_uuid[CLUSTER_SHARED_UUID_LEN];
	char _pad[3];
	uint32 participant_count;
	int32 participant_node_ids[CLUSTER_MAX_NODES];
	pg_crc32c crc; /* over [0, offsetof(crc)) */
} PgracSharedControl;

static char *
cluster_shared_fs_sentinel_path(void)
{
	return psprintf("%s/%s", cluster_shared_data_dir, PGRAC_SHARED_CONTROL_FILE);
}

static pg_crc32c
cluster_shared_fs_sentinel_crc(const PgracSharedControl *c)
{
	pg_crc32c crc;

	INIT_CRC32C(crc);
	COMP_CRC32C(crc, c, offsetof(PgracSharedControl, crc));
	FIN_CRC32C(crc);
	return crc;
}

static void
cluster_shared_fs_sentinel_gen_uuid(char *out)
{
	uint8 raw[16];
	static const char hex[] = "0123456789abcdef";
	int i;

	if (!pg_strong_random(raw, sizeof(raw)))
		ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR),
						errmsg("cluster_shared_fs: could not generate storage uuid")));
	for (i = 0; i < 16; i++) {
		out[i * 2] = hex[(raw[i] >> 4) & 0xf];
		out[i * 2 + 1] = hex[raw[i] & 0xf];
	}
	out[32] = '\0';
}

/*
 * Read + validate the control file.  Returns true on a structurally valid
 * record; sets *reason on failure (a zero-length file fails with "empty").
 */
static bool
cluster_shared_fs_sentinel_read(int fd, PgracSharedControl *out, const char **reason)
{
	ssize_t n = pg_pread(fd, out, sizeof(*out), 0);

	if (n == 0) {
		*reason = "empty";
		return false;
	}
	if (n != (ssize_t)sizeof(*out)) {
		*reason = "short read";
		return false;
	}
	if (out->magic != PGRAC_SHARED_CONTROL_MAGIC) {
		*reason = "bad magic";
		return false;
	}
	if (out->layout_version != PGRAC_SHARED_CONTROL_VERSION) {
		*reason = "bad version";
		return false;
	}
	if (out->crc != cluster_shared_fs_sentinel_crc(out)) {
		*reason = "bad crc";
		return false;
	}
	if (out->participant_count > CLUSTER_MAX_NODES) {
		*reason = "bad participant_count";
		return false;
	}
	return true;
}

/*
 * cluster_shared_fs_sentinel_attach -- record this node in the shared-root
 *	participant set (postmaster-once; see the init callback).
 */
void
cluster_shared_fs_sentinel_attach(void)
{
	char *path = cluster_shared_fs_sentinel_path();
	int fd;
	PgracSharedControl ctl;
	const char *reason = NULL;
	bool found = false;
	uint32 i;

	fd = OpenTransientFile(path, O_RDWR | PG_BINARY);
	if (fd < 0 && errno == ENOENT)
		fd = OpenTransientFile(path, O_RDWR | O_CREAT | PG_BINARY);
	if (fd < 0)
		ereport(FATAL, (errcode_for_file_access(),
						errmsg("cluster_shared_fs: could not open shared-root sentinel \"%s\": %m",
							   path)));

	if (flock(fd, LOCK_EX) != 0)
		ereport(FATAL, (errcode_for_file_access(),
						errmsg("cluster_shared_fs: could not lock sentinel \"%s\": %m", path)));

	if (!cluster_shared_fs_sentinel_read(fd, &ctl, &reason)) {
		off_t sz = lseek(fd, 0, SEEK_END);

		/* A non-empty file that fails validation is corruption: fail-closed. */
		if (sz != 0)
			ereport(FATAL, (errcode(ERRCODE_DATA_CORRUPTED),
							errmsg("cluster_shared_fs: shared-root sentinel \"%s\" is corrupt (%s)",
								   path, reason)));
		memset(&ctl, 0, sizeof(ctl));
		ctl.magic = PGRAC_SHARED_CONTROL_MAGIC;
		ctl.layout_version = PGRAC_SHARED_CONTROL_VERSION;
		if (cluster_shared_storage_uuid != NULL && cluster_shared_storage_uuid[0] != '\0')
			strlcpy(ctl.storage_uuid, cluster_shared_storage_uuid, sizeof(ctl.storage_uuid));
		else
			cluster_shared_fs_sentinel_gen_uuid(ctl.storage_uuid);
	}

	/* An external preset uuid must match the recorded identity. */
	if (cluster_shared_storage_uuid != NULL && cluster_shared_storage_uuid[0] != '\0'
		&& strcmp(ctl.storage_uuid, cluster_shared_storage_uuid) != 0)
		ereport(
			FATAL,
			(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
			 errmsg("cluster_shared_fs: shared-root sentinel uuid \"%s\" does not match "
					"cluster.shared_storage_uuid \"%s\"",
					ctl.storage_uuid, cluster_shared_storage_uuid),
			 errhint("Two nodes point at different shared roots, or a stale sentinel exists.")));

	for (i = 0; i < ctl.participant_count; i++) {
		if (ctl.participant_node_ids[i] == cluster_node_id) {
			found = true;
			break;
		}
	}

	if (!found) {
		if (ctl.participant_count >= CLUSTER_MAX_NODES)
			ereport(FATAL, (errcode(ERRCODE_CONFIGURATION_LIMIT_EXCEEDED),
							errmsg("cluster_shared_fs: shared-root participant set is full (%d)",
								   CLUSTER_MAX_NODES)));
		ctl.participant_node_ids[ctl.participant_count++] = cluster_node_id;
		ctl.crc = cluster_shared_fs_sentinel_crc(&ctl);
		if (pg_pwrite(fd, &ctl, sizeof(ctl), 0) != (ssize_t)sizeof(ctl))
			ereport(FATAL,
					(errcode_for_file_access(),
					 errmsg("cluster_shared_fs: could not write sentinel \"%s\": %m", path)));
		if (pg_fsync(fd) != 0)
			ereport(FATAL,
					(errcode_for_file_access(),
					 errmsg("cluster_shared_fs: could not fsync sentinel \"%s\": %m", path)));
	}

	if (flock(fd, LOCK_UN) != 0)
		ereport(WARNING, (errcode_for_file_access(),
						  errmsg("cluster_shared_fs: could not unlock sentinel \"%s\": %m", path)));
	CloseTransientFile(fd);

	elog(LOG,
		 "cluster_shared_fs: attached to shared root \"%s\" (uuid %s, node %d, %u participant(s))",
		 cluster_shared_data_dir, ctl.storage_uuid, cluster_node_id, ctl.participant_count);
	pfree(path);
}

/*
 * cluster_shared_fs_sentinel_has_participant -- is node_id in the shared-
 *	root participant set?  A missing or corrupt sentinel returns false
 *	(fail-closed: participation cannot be proven, so the capability gate
 *	must not engage for that peer).
 */
bool
cluster_shared_fs_sentinel_has_participant(int node_id)
{
	char *path = cluster_shared_fs_sentinel_path();
	int fd;
	PgracSharedControl ctl;
	const char *reason = NULL;
	bool found = false;

	fd = OpenTransientFile(path, O_RDONLY | PG_BINARY);
	if (fd < 0) {
		pfree(path);
		return false;
	}
	if (cluster_shared_fs_sentinel_read(fd, &ctl, &reason)) {
		uint32 i;

		for (i = 0; i < ctl.participant_count; i++) {
			if (ctl.participant_node_ids[i] == node_id) {
				found = true;
				break;
			}
		}
	}
	CloseTransientFile(fd);
	pfree(path);
	return found;
}

/*
 * cluster_shared_fs_get_storage_uuid -- copy the recorded shared-storage uuid
 *	into `out`.  Writes "" when cluster.shared_data_dir is unset or the
 *	sentinel is missing/corrupt, so callers treat an unknown identity as
 *	"cannot bind" (fail-closed).  Read-only, no shmem dependency: usable from
 *	the startup process before cluster shmem is fully up, mirroring
 *	cluster_shared_fs_sentinel_has_participant.
 */
void
cluster_shared_fs_get_storage_uuid(char *out, size_t outlen)
{
	char *path;
	int fd;
	PgracSharedControl ctl;
	const char *reason = NULL;

	if (out == NULL || outlen == 0)
		return;
	out[0] = '\0';

	if (cluster_shared_data_dir == NULL || cluster_shared_data_dir[0] == '\0')
		return;

	path = cluster_shared_fs_sentinel_path();
	fd = OpenTransientFile(path, O_RDONLY | PG_BINARY);
	if (fd < 0) {
		pfree(path);
		return;
	}
	if (cluster_shared_fs_sentinel_read(fd, &ctl, &reason))
		strlcpy(out, ctl.storage_uuid, outlen);
	CloseTransientFile(fd);
	pfree(path);
}


/*
 * Lifecycle.  The shared_fs backend attaches to the cross-node sentinel on
 * init, postmaster-once: cluster_shared_fs_init runs from
 * CreateSharedMemoryAndSemaphores (ipci.c), which the postmaster calls
 * directly (IsUnderPostmaster == false) and EXEC_BACKEND children re-run
 * (IsUnderPostmaster == true).  The attach is idempotent, but the guard
 * keeps the single-writer intent explicit (CLAUDE.md rule 16).
 */
static void
cluster_shared_fs_sharedfs_init(void)
{
	if (!IsUnderPostmaster)
		cluster_shared_fs_sentinel_attach();
}
static void
cluster_shared_fs_sharedfs_shutdown(void)
{}

static int
cluster_shared_fs_sharedfs_barrier_sync(ClusterSharedFsHandle *handle)
{
	cluster_shared_fs_sharedfs_immedsync(handle);
	return 0;
}

static int
cluster_shared_fs_sharedfs_register_fence_key(int node_id)
{
	(void)node_id;
	return EOPNOTSUPP;
}

static ClusterFenceCapability
cluster_shared_fs_sharedfs_fence_capability(void)
{
	return CLUSTER_FENCE_CAP_NONE;
}

static bool
cluster_shared_fs_sharedfs_prefetch(ClusterSharedFsHandle *handle, BlockNumber blocknum)
{
	off_t offset;

	if (handle == NULL || !handle->opened)
		return false;

	offset = (off_t)blocknum * BLCKSZ;
	return FilePrefetch(handle->vfd, offset, BLCKSZ, WAIT_EVENT_DATA_FILE_PREFETCH) == 0;
}

static void
cluster_shared_fs_sharedfs_writeback(ClusterSharedFsHandle *handle, BlockNumber blocknum,
									 BlockNumber nblocks)
{
	off_t offset;
	off_t nbytes;

	if (handle == NULL || !handle->opened || nblocks == 0)
		return;

	offset = (off_t)blocknum * BLCKSZ;
	nbytes = (off_t)nblocks * BLCKSZ;
	FileWriteback(handle->vfd, offset, nbytes, WAIT_EVENT_DATA_FILE_FLUSH);
}


const ClusterSharedFsOps cluster_shared_fs_sharedfs_ops = {
	.name = "shared_fs",
	.id = CLUSTER_SHARED_FS_BACKEND_CLUSTER_FS,
	.caps = &cluster_shared_fs_sharedfs_caps,

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

	.barrier_sync = cluster_shared_fs_sharedfs_barrier_sync,
	.register_fence_key = cluster_shared_fs_sharedfs_register_fence_key,
	.fence_capability = cluster_shared_fs_sharedfs_fence_capability,
	.prefetch = cluster_shared_fs_sharedfs_prefetch,
	.writeback = cluster_shared_fs_sharedfs_writeback,
};

#endif /* USE_PGRAC_CLUSTER */
