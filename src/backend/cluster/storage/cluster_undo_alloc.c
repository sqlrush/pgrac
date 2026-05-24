/*-------------------------------------------------------------------------
 *
 * cluster_undo_alloc.c
 *	  pgrac undo segment allocator (runtime API).
 *
 *	  Stage 1.22 entry points for backend-side undo segment management:
 *	    - cluster_undo_path_resolve(): pure path builder
 *	    - cluster_undo_segment_allocate(): create + init + WAL-protect
 *	      one segment file under $PGDATA/pg_undo/instance_<N>/seg_<id>.dat
 *
 *	  Allocator NOT called during initdb (initdb writes the seed segment
 *	  directly via libpgport in initdb.c -- frontend / no WAL).  Backend
 *	  allocator is exercised at:
 *	    - Stage 1.22: cluster_unit harness tests + cluster_tap L5/L6
 *	    - Stage 2+ (feature-117): on-demand at first transaction binding
 *
 *	  Spec: spec-1.22-undo-tablespace-bootstrap.md §2.3 + §D5.
 *
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/backend/cluster/storage/cluster_undo_alloc.c
 *
 * NOTES
 *	  This is a pgrac-original file (no derivation from PostgreSQL).
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include "access/xlog.h"
#include "cluster/cluster_scn.h" /* SCN_MAX_VALID_NODE_ID */
#include "cluster/cluster_undo_segment.h"
#include "cluster/cluster_undo_segment_init.h"
#include "cluster/storage/cluster_undo_alloc.h"
#include "cluster/storage/cluster_undo_xlog.h"
#include "miscadmin.h"
#include "storage/bufpage.h"
#include "storage/fd.h"


/* spec-3.4b D2: forward decl of cluster.node_id GUC owned by cluster_guc.c. */
extern int cluster_node_id;


/*
 * cluster_undo_path_resolve
 *
 *   Pure path builder (no I/O, no errors apart from buffer overflow).
 *
 *   Hardening v1.0.4 P1-1 (directory naming separation):
 *     - The owner_instance VALUE in headers / WAL payloads is
 *       (cluster_node_id + 1), so 0 stays the "unallocated" sentinel.
 *     - The DIRECTORY NAME on disk uses cluster_node_id directly
 *       (= owner_instance - 1) so that single-node default
 *       (cluster_node_id = 0) lays out at pg_undo/instance_0/...
 *       matching the initdb seed segment (also at instance_0/).
 *       Otherwise allocator segments would land at instance_1/ and
 *       split from the seed, breaking the per-instance subdir
 *       invariant.
 *
 *   Caller MUST pass owner_instance in [1, UNDO_OWNER_INSTANCE_MAX];
 *   the assert catches sentinel-0 misuse.
 */
int
cluster_undo_path_resolve(uint8 owner_instance, uint32 segment_id, char *buf, size_t buf_size)
{
	int ret;

	if (buf == NULL || buf_size == 0)
		return -1;
	Assert(owner_instance >= 1 && owner_instance <= UNDO_OWNER_INSTANCE_MAX);

	/* directory uses cluster_node_id (= owner_instance - 1) */
	ret = snprintf(buf, buf_size, "%s/pg_undo/instance_%u/seg_%u.dat", DataDir,
				   (unsigned)(owner_instance - 1), (unsigned)segment_id);
	if (ret < 0 || (size_t)ret >= buf_size)
		return -1;
	return 0;
}


/*
 * Open-or-create the segment file.  Returns the fd (caller closes).
 *
 * Idempotent: if the file already exists, opens it with O_RDWR.  Helps
 * Stage 1.22 callers that ask the allocator twice (e.g., test harness
 * sanity checks) without orphan files.
 */
static int
open_or_create_segment(const char *path, bool *out_created)
{
	int fd;

	*out_created = false;

	fd = BasicOpenFile(path, O_RDWR | O_CREAT | O_EXCL | PG_BINARY);
	if (fd >= 0) {
		*out_created = true;
		return fd;
	}

	if (errno != EEXIST)
		ereport(ERROR, (errcode_for_file_access(),
						errmsg("could not create undo segment file \"%s\": %m", path)));

	/* Already exists -- open for read/write. */
	fd = BasicOpenFile(path, O_RDWR | PG_BINARY);
	if (fd < 0)
		ereport(ERROR, (errcode_for_file_access(),
						errmsg("could not open existing undo segment file \"%s\": %m", path)));
	return fd;
}


/*
 * Ensure the parent directory pg_undo/instance_<N>/ exists.
 *
 * Stage 1.22: only instance_0/ is created at initdb time.  Backend
 * allocations to instance >= 2 (cross-instance, deferred to Stage 2+)
 * would also need the subdir.  For Stage 1.22 we keep this defensive:
 * the allocator never reaches instance != 1 because the public API
 * rejects it, but if a future caller does, mkdir() handles it.
 */
static void
ensure_instance_subdir(uint8 owner_instance)
{
	char path[MAXPGPATH];
	int ret;

	Assert(owner_instance >= 1 && owner_instance <= UNDO_OWNER_INSTANCE_MAX);

	/* directory uses cluster_node_id (= owner_instance - 1); see
	 * cluster_undo_path_resolve docstring. */
	ret = snprintf(path, sizeof(path), "%s/pg_undo/instance_%u", DataDir,
				   (unsigned)(owner_instance - 1));
	if (ret < 0 || (size_t)ret >= sizeof(path))
		ereport(ERROR, (errcode(ERRCODE_NAME_TOO_LONG),
						errmsg("undo instance subdir path too long: owner_instance=%u",
							   (unsigned)owner_instance)));

	if (mkdir(path, S_IRWXU) < 0 && errno != EEXIST)
		ereport(ERROR, (errcode_for_file_access(),
						errmsg("could not create undo instance subdir \"%s\": %m", path)));
}


/*
 * cluster_undo_segment_allocate
 *
 *   Body:
 *     1. Validate owner_instance (Stage 1.22 single-node restriction).
 *     2. Resolve segment file path.
 *     3. Ensure parent directory exists.
 *     4. Open-or-create the file (idempotent).
 *     5. Generate the 8 KB header bytes via the shared helper.
 *     6. Extend file to UNDO_SEGMENT_SIZE_BYTES if it was just created.
 *     7. pwrite block 0 + fsync.
 *     8. Emit XLOG_UNDO_SEGMENT_INIT WAL record so crash replay can
 *        rebuild the segment header.
 */
void
cluster_undo_segment_allocate(uint32 segment_id, uint8 owner_instance)
{
	char path[MAXPGPATH];
	PGAlignedBlock page;
	int fd;
	bool created;
	ssize_t written;

	/*
	 * spec-3.4b D2 multi-instance unlock.
	 *
	 *   The previous Stage 1.22 single-node restriction
	 *   ("owner_instance must equal CLUSTER_UNDO_DEFAULT_OWNER (1)")
	 *   is replaced with the multi-instance validation chain below.
	 */
	if (owner_instance == CLUSTER_UNDO_OWNER_INVALID || owner_instance > UNDO_OWNER_INSTANCE_MAX)
		ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						errmsg("owner_instance %u out of range [1, %u]", (unsigned)owner_instance,
							   (unsigned)UNDO_OWNER_INSTANCE_MAX)));

	/* Spec-3.4b F2: segment 0 is the bootstrap-only sentinel. */
	if (segment_id == 0)
		ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						errmsg("segment_id 0 is bootstrap-only and not allocatable")));

	/* Spec-3.4b F4: exact-key alias guard against 16-bit truncation. */
	if (segment_id > UINT16_MAX)
		ereport(ERROR, (errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
						errmsg("segment_id %u exceeds spec-3.4b limit of UINT16_MAX (%u)",
							   (unsigned)segment_id, (unsigned)UINT16_MAX),
						errhint("Stage 4/5 may bump ClusterUndoTTSlotRef.undo_segment_id "
								"and ClusterTTStatusKey.undo_segment_id to wider types.")));

	/*
	 * Spec-3.4b D2: a node may only allocate segments whose id falls
	 * within its reserved per-instance range, i.e. the derived
	 * owner_instance from segment_id must match the requested
	 * owner_instance.  Cross-instance mis-allocation is a programming
	 * bug -- raise PANIC so corruption is impossible.
	 */
	{
		uint32 derived_inst = ((segment_id - 1) / CLUSTER_UNDO_SEGS_PER_INSTANCE) + 1;

		if (derived_inst != (uint32)owner_instance)
			ereport(PANIC, (errcode(ERRCODE_DATA_CORRUPTED),
							errmsg("segment_id %u maps to owner_instance %u, "
								   "but allocate was called with owner_instance %u",
								   (unsigned)segment_id, (unsigned)derived_inst,
								   (unsigned)owner_instance)));

		/* And that owner must be this node. */
		if ((int)owner_instance != cluster_node_id + 1)
			ereport(PANIC,
					(errcode(ERRCODE_DATA_CORRUPTED),
					 errmsg("attempt to allocate segment owned by instance %u "
							"from cluster_node_id %d (own owner_instance %d)",
							(unsigned)owner_instance, cluster_node_id, cluster_node_id + 1)));
	}

	if (cluster_undo_path_resolve(owner_instance, segment_id, path, sizeof(path)) != 0)
		ereport(ERROR, (errcode(ERRCODE_NAME_TOO_LONG),
						errmsg("undo segment path too long: instance=%u seg=%u",
							   (unsigned)owner_instance, (unsigned)segment_id)));

	ensure_instance_subdir(owner_instance);

	fd = open_or_create_segment(path, &created);
	(void)created; /* tracked for logging/observability; ftruncate is unconditional */

	cluster_undo_segment_make_header_bytes(segment_id, owner_instance, page.data);

	/*
	 * Hardening v1.0.4 P1-2: unconditional ftruncate.
	 *
	 * v1.0.3 only ftruncate'd when created=true, but a crash between
	 * O_CREAT and ftruncate leaves a partial / zero-size orphan file.
	 * The next allocate call sees the existing file, so created=false,
	 * and the file would never reach UNDO_SEGMENT_SIZE_BYTES -- segment
	 * tail accesses would EOF.  ftruncate is idempotent (shrink-to-same
	 * and extend-to-target are both no-ops when size already matches),
	 * so unconditional is safe and self-healing.  Mirrors the redo
	 * handler's 6-step idempotent pattern (cluster_undo_xlog.c).
	 */
	if (ftruncate(fd, (off_t)UNDO_SEGMENT_SIZE_BYTES) != 0) {
		int save_errno = errno;

		close(fd);
		errno = save_errno;
		ereport(ERROR, (errcode_for_file_access(),
						errmsg("could not extend undo segment file \"%s\" to %d bytes: %m", path,
							   UNDO_SEGMENT_SIZE_BYTES)));
	}

	written = pg_pwrite(fd, page.data, BLCKSZ, 0);
	if (written != BLCKSZ) {
		int save_errno = errno;

		close(fd);
		errno = save_errno;
		ereport(ERROR, (errcode_for_file_access(),
						errmsg("could not write undo segment header for \"%s\": "
							   "wrote %zd of %d bytes",
							   path, written, BLCKSZ)));
	}

	if (pg_fsync(fd) != 0) {
		int save_errno = errno;

		close(fd);
		errno = save_errno;
		ereport(ERROR, (errcode_for_file_access(),
						errmsg("could not fsync undo segment file \"%s\": %m", path)));
	}

	if (close(fd) != 0)
		ereport(ERROR, (errcode_for_file_access(),
						errmsg("could not close undo segment file \"%s\": %m", path)));

	/*
	 * Hardening v1.0.4 P1-2: fsync parent directory after file creation /
	 * truncate.  Required for the create case so the dirent is durable;
	 * harmless for the already-exists case.  Mirrors redo handler.
	 */
	{
		char dir[MAXPGPATH];
		int dret;

		dret = snprintf(dir, sizeof(dir), "%s/pg_undo/instance_%u", DataDir,
						(unsigned)(owner_instance - 1));
		if (dret >= 0 && (size_t)dret < sizeof(dir))
			fsync_fname(dir, true);
	}

	/*
	 * Emit WAL record so crash recovery can recreate the header (the
	 * segment file itself is created earlier in this function; a crash
	 * between file creation and WAL emit leaves an orphan empty file
	 * which is harmless -- segment_id allocation is idempotent and the
	 * file will be reinitialized on next allocate call).
	 *
	 * For Stage 1.22 we always emit; Stage 2+ may add a fast-path that
	 * skips emit when the segment is being created from a known-good
	 * shared-storage replica (feature-119).
	 */
	(void)cluster_undo_emit_segment_init(owner_instance, segment_id, page.data);
}


/*
 * cluster_undo_active_segment_for_node_or_create
 *
 *   spec-3.4b D2: return the segment_id of this node's active undo
 *   segment, creating it on first call.  See header for the encoding.
 */
uint32
cluster_undo_active_segment_for_node_or_create(int node_id)
{
	uint32 segment_id;
	uint8 owner_instance;

	if (node_id < 0 || node_id > SCN_MAX_VALID_NODE_ID)
		ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						errmsg("node_id %d out of range [0, %d] for undo segment selection",
							   node_id, SCN_MAX_VALID_NODE_ID)));

	owner_instance = (uint8)(node_id + 1);

	/* per_instance_slot = 0 (spec-3.4b MVP, single active segment per node) */
	segment_id = (uint32)node_id * CLUSTER_UNDO_SEGS_PER_INSTANCE + 1;

	/*
	 * cluster_undo_segment_allocate is idempotent: it returns silently if
	 * the file already exists with the right header.  Calling it here
	 * means the first DML on this node lazily provisions the segment,
	 * subsequent calls are cheap no-ops.
	 */
	cluster_undo_segment_allocate(segment_id, owner_instance);

	return segment_id;
}
