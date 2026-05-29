/*-------------------------------------------------------------------------
 *
 * cluster_undo_smgr.c
 *	  pgrac undo segment file I/O abstraction layer (spec-3.7 D7 carryover,
 *	  spec-3.8 真 ship implementation).
 *
 *	  Provides block-level read/write/create/fsync of per-instance undo
 *	  segment files.  Wraps the same BasicOpenFile + pg_pread/pwrite +
 *	  pg_fsync I/O pattern used inline by cluster_undo_record.c +
 *	  cluster_undo_alloc.c.
 *
 *	  Future refactor (Hardening v1.0.X):  cluster_undo_record.c inline
 *	  read_undo_block / write_undo_block should call into this layer to
 *	  centralize I/O paths.  spec-3.9 CR construction + spec-3.10 CR
 *	  cache will hook above this layer.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * Spec: spec-3.8-undo-segment-lifecycle-autoextend.md (FROZEN v0.3 +
 *       Hardening v1.0.1;  D7 carryover from spec-3.7 Hardening v1.0.3)
 *
 * IDENTIFICATION
 *	  src/backend/cluster/storage/cluster_undo_smgr.c
 *
 * NOTES
 *	  This is a pgrac-original file (no derivation from PostgreSQL).
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <fcntl.h>
#include <unistd.h>

#include "miscadmin.h"
#include "storage/fd.h"
#include "utils/elog.h"

#include "cluster/cluster_undo_smgr.h"
#include "cluster/cluster_undo_segment.h"
#include "cluster/storage/cluster_undo_alloc.h"


static int
open_segment_rdonly(uint32 segment_id, uint8 owner_instance)
{
	char path[MAXPGPATH];
	int ret;

	ret = cluster_undo_path_resolve(owner_instance, segment_id, path, sizeof(path));
	if (ret != 0)
		return -1;

	return BasicOpenFile(path, O_RDONLY | PG_BINARY);
}


static int
open_segment_rdwr(uint32 segment_id, uint8 owner_instance)
{
	char path[MAXPGPATH];
	int ret;

	ret = cluster_undo_path_resolve(owner_instance, segment_id, path, sizeof(path));
	if (ret != 0)
		return -1;

	return BasicOpenFile(path, O_RDWR | PG_BINARY);
}


bool
cluster_undo_smgr_read_block(uint32 segment_id, uint8 owner_instance, uint32 block_no, char *buf)
{
	int fd;
	off_t offset;
	ssize_t nread;
	bool ok;

	if (buf == NULL || block_no >= UNDO_BLOCKS_PER_SEGMENT)
		return false;

	fd = open_segment_rdonly(segment_id, owner_instance);
	if (fd < 0)
		return false;

	offset = (off_t)block_no * BLCKSZ;
	nread = pg_pread(fd, buf, BLCKSZ, offset);
	ok = (nread == BLCKSZ);

	close(fd);
	return ok;
}


bool
cluster_undo_smgr_write_block(uint32 segment_id, uint8 owner_instance, uint32 block_no,
							  const char *buf, bool do_fsync)
{
	int fd;
	off_t offset;
	ssize_t nwritten;
	bool ok = true;

	if (buf == NULL || block_no >= UNDO_BLOCKS_PER_SEGMENT)
		return false;

	fd = open_segment_rdwr(segment_id, owner_instance);
	if (fd < 0)
		return false;

	offset = (off_t)block_no * BLCKSZ;
	nwritten = pg_pwrite(fd, buf, BLCKSZ, offset);
	if (nwritten != BLCKSZ)
		ok = false;

	if (ok && do_fsync) {
		if (pg_fsync(fd) != 0)
			ok = false;
	}

	close(fd);
	return ok;
}


int
cluster_undo_smgr_create_segment_file(uint32 segment_id, uint8 owner_instance)
{
	if (owner_instance < 1 || owner_instance > UNDO_OWNER_INSTANCE_MAX)
		return -2;

	/*
	 * cluster_undo_segment_allocate is idempotent — if file already
	 * exists with valid header, no rewrite;  if it doesn't, create +
	 * init + WAL-protect.  May ereport(ERROR) on FS error.
	 */
	cluster_undo_segment_allocate(segment_id, owner_instance);

	/* Verify file exists after the call. */
	{
		char path[MAXPGPATH];
		int ret;
		int fd;

		ret = cluster_undo_path_resolve(owner_instance, segment_id, path, sizeof(path));
		if (ret != 0)
			return -2;

		fd = BasicOpenFile(path, O_RDONLY | PG_BINARY);
		if (fd < 0)
			return -1;
		close(fd);
	}

	return 0;
}


bool
cluster_undo_smgr_fsync_segment_file(uint32 segment_id, uint8 owner_instance)
{
	int fd;
	bool ok;

	fd = open_segment_rdwr(segment_id, owner_instance);
	if (fd < 0)
		return false;

	ok = (pg_fsync(fd) == 0);

	close(fd);
	return ok;
}
