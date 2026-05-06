/*-------------------------------------------------------------------------
 *
 * cluster_undo_xlog.c
 *	  pgrac undo segment WAL resource manager (RM_CLUSTER_UNDO).
 *
 *	  Stage 1.22 ships ONE subtype: XLOG_UNDO_SEGMENT_INIT, emitted by
 *	  cluster_undo_segment_allocate when block 0 of a fresh seg_<id>.dat
 *	  file is initialized.  redo handler reads the payload and pwrites
 *	  the page image directly to the segment file via cluster_undo_path
 *	  (no PG buffer manager / smgr involvement -- pg_undo files live
 *	  outside PG's RelFileLocator namespace).
 *
 *	  The payload identification scheme (instance + segment_id) is the
 *	  v0.2 P1-A 修订: XLOG_FPI assumes RelFileLocator/ForkNumber/BlockNumber
 *	  block tags routed via XLogReadBufferForRedo() -> smgr -> relpath(),
 *	  which is incompatible with $PGDATA/pg_undo/instance_N/seg_M.dat.
 *	  Custom RM record bypasses that routing layer entirely.
 *
 *	  Spec: spec-1.22-undo-tablespace-bootstrap.md §D14a.
 *
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/backend/cluster/storage/cluster_undo_xlog.c
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
#include "access/xloginsert.h"
#include "access/xlogreader.h"
#include "access/xlogutils.h"
#include "cluster/cluster_undo_segment.h" /* UNDO_SEGMENT_SIZE_BYTES */
#include "cluster/storage/cluster_undo_xlog.h"
#include "miscadmin.h"
#include "storage/bufpage.h"
#include "storage/fd.h"


/*
 * Build a path: $PGDATA/pg_undo/instance_<N>/seg_<segment_id>.dat
 *
 *   Both encoder (cluster_undo_emit_segment_init) and decoder
 *   (cluster_undo_redo) use this layout.  pg_undo subdir is
 *   established by initdb (D4).  instance_<N> subdir is created
 *   on demand by cluster_undo_redo_segment_init (Stage 1.22 ships
 *   only instance_0 from initdb; redo path may need other instance
 *   subdirs on standbys / cross-instance crash recovery).
 *
 *   Hardening v1.0.4 P1-1: directory naming uses cluster_node_id
 *   (= owner_instance - 1) so that single-node default
 *   (cluster_node_id = 0, owner_instance = 1) lands at instance_0/
 *   matching the initdb seed.  See cluster_undo_alloc.c
 *   cluster_undo_path_resolve docstring for full rationale.
 *
 *   Returns 0 on success, -1 on path-too-long.  Caller supplies
 *   buf with capacity >= MAXPGPATH.
 */
static int
build_undo_segment_path(uint8 owner_instance, uint32 segment_id, char *buf, size_t buf_size)
{
	int ret;

	Assert(owner_instance >= 1 && owner_instance <= UNDO_OWNER_INSTANCE_MAX);
	ret = snprintf(buf, buf_size, "%s/pg_undo/instance_%u/seg_%u.dat", DataDir,
				   (unsigned)(owner_instance - 1), (unsigned)segment_id);
	if (ret < 0 || (size_t)ret >= buf_size)
		return -1;
	return 0;
}


/*
 * Ensure $PGDATA/pg_undo/instance_<N>/ exists (creates if missing).
 *
 *   Idempotent: EEXIST is not an error.  PANIC on any other failure
 *   (recovery contract -- a half-replayed undo segment is corruption).
 *
 *   Stage 1.22 redo path is the primary user: standbys + cross-instance
 *   crash recovery may see XLOG_UNDO_SEGMENT_INIT records for instance
 *   subdirs that initdb never created locally.  pg_undo/ itself is
 *   established by initdb (D4) and assumed to exist.
 */
static void
ensure_undo_instance_subdir(uint8 owner_instance)
{
	char path[MAXPGPATH];
	int ret;

	Assert(owner_instance >= 1 && owner_instance <= UNDO_OWNER_INSTANCE_MAX);

	/* directory uses cluster_node_id (= owner_instance - 1) per Hardening v1.0.4 P1-1 */
	ret = snprintf(path, sizeof(path), "%s/pg_undo/instance_%u", DataDir,
				   (unsigned)(owner_instance - 1));
	if (ret < 0 || (size_t)ret >= sizeof(path))
		ereport(PANIC, (errmsg("undo instance subdir path too long: owner_instance=%u",
							   (unsigned)owner_instance)));

	if (mkdir(path, S_IRWXU) != 0 && errno != EEXIST)
		ereport(PANIC, (errcode_for_file_access(),
						errmsg("could not create undo instance subdir \"%s\": %m", path)));
}


/*
 * cluster_undo_emit_segment_init
 *
 *   Backend caller emits XLOG_UNDO_SEGMENT_INIT for the just-written
 *   segment header block.  page_image is the 8 KB page bytes (typically
 *   just produced by PageInitUndoSegmentHeader on a locked buffer; the
 *   caller copies them here so the WAL record is self-contained -- the
 *   redo handler doesn't need a buffer manager lookup).
 */
XLogRecPtr
cluster_undo_emit_segment_init(uint8 instance, uint32 segment_id, const char *page_image)
{
	xl_cluster_undo_segment_init hdr;
	XLogRecPtr lsn;

	Assert(instance >= 1);
	Assert(page_image != NULL);

	memset(&hdr, 0, sizeof(hdr));
	hdr.instance = instance;
	hdr.segment_id = segment_id;

	XLogBeginInsert();
	XLogRegisterData((char *)&hdr, sizeof(hdr));
	XLogRegisterData((char *)page_image, BLCKSZ);

	lsn = XLogInsert(RM_CLUSTER_UNDO_ID, XLOG_UNDO_SEGMENT_INIT);

	return lsn;
}


/*
 * Replay XLOG_UNDO_SEGMENT_INIT.
 *
 *   Spec-1.22 Hardening v1.0.3: full idempotent create + size + restore
 *   semantics for crash / standby replay.  Because pg_undo/ files live
 *   outside PG's RelFileLocator namespace, neither SMGR nor XLOG_FPI
 *   manage file lifecycle for us; the redo handler must own:
 *     1. mkdir parent directory (instance_<N>/) if missing
 *     2. open(O_CREAT | O_RDWR) (creates file if missing)
 *     3. ftruncate to UNDO_SEGMENT_SIZE_BYTES (extends sparse if missing)
 *     4. pwrite block 0 with the WAL-shipped page image
 *     5. fsync file
 *     6. fsync parent directory (durable dirent for create case)
 *
 *   Idempotent across replay scenarios:
 *     - Segment + dir + size all present (allocator path normal): every
 *       step is a no-op except pwrite (overwrites block 0 with same bytes)
 *     - Standby never saw the allocator: full create + extend + write
 *     - Operator deleted seg_<id>.dat between checkpoints: rebuild
 *     - Crash mid-allocator (between create and extend): ftruncate
 *       extends to full size; pwrite restores block 0
 *
 *   Errors promote to PANIC per the standard recovery contract.
 */
static void
cluster_undo_redo_segment_init(XLogReaderState *record)
{
	xl_cluster_undo_segment_init *hdr;
	char *payload;
	const char *page_image;
	char path[MAXPGPATH];
	int fd;
	ssize_t written;

	payload = XLogRecGetData(record);
	if (XLogRecGetDataLen(record) != sizeof(*hdr) + BLCKSZ)
		ereport(PANIC, (errmsg("invalid XLOG_UNDO_SEGMENT_INIT record length: %u",
							   XLogRecGetDataLen(record))));

	hdr = (xl_cluster_undo_segment_init *)payload;
	page_image = payload + sizeof(*hdr);

	if (build_undo_segment_path(hdr->instance, hdr->segment_id, path, sizeof(path)) != 0)
		ereport(PANIC, (errmsg("undo segment path too long: instance=%u seg=%u", hdr->instance,
							   hdr->segment_id)));

	/* Step 1: ensure parent instance subdir exists (idempotent on EEXIST). */
	ensure_undo_instance_subdir(hdr->instance);

	/* Step 2: open the segment file, creating if missing. */
	fd = BasicOpenFile(path, O_CREAT | O_RDWR | PG_BINARY);
	if (fd < 0)
		ereport(PANIC, (errcode_for_file_access(),
						errmsg("could not open or create undo segment file \"%s\": %m", path)));

	/*
	 * Step 3: ensure file is exactly UNDO_SEGMENT_SIZE_BYTES (64 MB).
	 * ftruncate is idempotent: shrink-to-same-size and extend-to-target
	 * are both no-ops when the file already has the target size.  Tail
	 * bytes are sparse zeros until the allocator path writes real undo
	 * records (deferred to feature-117).
	 */
	if (ftruncate(fd, (off_t)UNDO_SEGMENT_SIZE_BYTES) != 0) {
		int save_errno = errno;

		close(fd);
		errno = save_errno;
		ereport(PANIC, (errcode_for_file_access(),
						errmsg("could not extend undo segment file \"%s\" to %d bytes: %m", path,
							   UNDO_SEGMENT_SIZE_BYTES)));
	}

	/* Step 4: pwrite block 0 with the WAL-shipped page image. */
	written = pg_pwrite(fd, page_image, BLCKSZ, 0);
	if (written != BLCKSZ) {
		int save_errno = errno;

		close(fd);
		errno = save_errno;
		ereport(PANIC, (errcode_for_file_access(),
						errmsg("could not write undo segment header for \"%s\": "
							   "wrote %zd of %d bytes",
							   path, written, BLCKSZ)));
	}

	/* Step 5: fsync the file (durable block 0 + tail allocation). */
	if (pg_fsync(fd) != 0) {
		int save_errno = errno;

		close(fd);
		errno = save_errno;
		ereport(PANIC, (errcode_for_file_access(),
						errmsg("could not fsync undo segment file \"%s\": %m", path)));
	}

	if (close(fd) != 0)
		ereport(PANIC, (errcode_for_file_access(),
						errmsg("could not close undo segment file \"%s\": %m", path)));

	/*
	 * Step 6: fsync the parent directory (instance_<N>/).  Required for
	 * the create case (Step 2 with O_CREAT) so the dirent is durable;
	 * harmless for the already-exists case.  fsync_parent_path is
	 * idempotent and tolerates missing intermediate directories.
	 */
	{
		char dir[MAXPGPATH];
		int dret;

		/* directory uses cluster_node_id (= owner_instance - 1) per Hardening v1.0.4 P1-1 */
		dret = snprintf(dir, sizeof(dir), "%s/pg_undo/instance_%u", DataDir,
						(unsigned)(hdr->instance - 1));
		if (dret >= 0 && (size_t)dret < sizeof(dir))
			fsync_fname(dir, true);
	}
}


/*
 * cluster_undo_redo -- RM_CLUSTER_UNDO redo handler entry point.
 *
 *   Dispatches by xl_info & XLR_INFO_MASK after stripping the framework
 *   bits (XLR_SPECIAL_REL_UPDATE etc.; we don't use those in stage 1.22).
 */
void
cluster_undo_redo(XLogReaderState *record)
{
	uint8 info = XLogRecGetInfo(record) & ~XLR_INFO_MASK;

	switch (info) {
	case XLOG_UNDO_SEGMENT_INIT:
		cluster_undo_redo_segment_init(record);
		break;
	default:
		ereport(PANIC, (errmsg("cluster_undo_redo: unknown op code %u", info)));
	}
}


/*
 * NOTE: cluster_undo_desc + cluster_undo_identify live in
 * src/backend/access/rmgrdesc/clusterundodesc.c so they are picked up
 * by both the backend xlog.o linker and the frontend pg_waldump build
 * (which globs src/backend/access/rmgrdesc/ "*desc.c" into its OBJS).
 */
