/*-------------------------------------------------------------------------
 *
 * cluster_raw_xlog.c
 *	  WAL redo/emit for spec-6.0a raw block-device layout metadata pages.
 *
 *	  The raw block-device provider owns allocator metadata outside PG's
 *	  normal relation forks.  RM_CLUSTER_RAW_LAYOUT logs full BLCKSZ page
 *	  images for those metadata pages so crash restart and WAL replay can
 *	  restore the raw superblock/bitmap/directory/extent-slot contract
 *	  before relation data is trusted.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/backend/cluster/storage/cluster_raw_xlog.c
 *
 * NOTES
 *	  This is a pgrac-original file (no derivation from PostgreSQL).
 *	  Spec: spec-6.0a-production-shared-storage-backend-matrix.md
 *	  (FROZEN, crash-safe RM_CLUSTER_RAW_LAYOUT metadata WAL).
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <fcntl.h>
#include <unistd.h>

#include "access/xlog.h"
#include "access/xloginsert.h"
#include "access/xlogreader.h"
#include "cluster/cluster_guc.h"
#include "cluster/storage/cluster_raw_xlog.h"
#include "storage/fd.h"

#ifdef USE_PGRAC_CLUSTER

XLogRecPtr
cluster_raw_layout_emit_write(uint64 offset, const char *image)
{
	xl_cluster_raw_layout_write rec;

	if (!XLogInsertAllowed())
		return InvalidXLogRecPtr;

	if (image == NULL || offset % BLCKSZ != 0)
		ereport(ERROR, (errcode(ERRCODE_CLUSTER_STORAGE_IO_ALIGNMENT),
						errmsg("invalid raw layout WAL image at offset " UINT64_FORMAT, offset)));

	memset(&rec, 0, sizeof(rec));
	rec.offset = offset;
	rec.nbytes = BLCKSZ;

	XLogBeginInsert();
	XLogRegisterData((char *)&rec, sizeof(rec));
	XLogRegisterData(unconstify(char *, image), BLCKSZ);

	return XLogInsert(RM_CLUSTER_RAW_LAYOUT_ID, XLOG_CLUSTER_RAW_LAYOUT_WRITE);
}

void
cluster_raw_layout_redo(XLogReaderState *record)
{
	char *payload = XLogRecGetData(record);
	uint8 info = XLogRecGetInfo(record) & ~XLR_INFO_MASK;
	xl_cluster_raw_layout_write *rec;
	char *image;
	int fd;

	if (info != XLOG_CLUSTER_RAW_LAYOUT_WRITE)
		ereport(PANIC, (errmsg("cluster_raw_layout_redo: unknown op %u", info)));

	rec = (xl_cluster_raw_layout_write *)payload;
	image = payload + sizeof(*rec);

	if (rec->nbytes != BLCKSZ || rec->offset % BLCKSZ != 0)
		ereport(PANIC, (errcode(ERRCODE_CLUSTER_STORAGE_IO_ALIGNMENT),
						errmsg("cluster raw layout WAL record has invalid offset/length"),
						errdetail("offset=" UINT64_FORMAT " nbytes=%u", rec->offset, rec->nbytes)));

	if (cluster_block_device_path == NULL || cluster_block_device_path[0] == '\0')
		ereport(PANIC, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
						errmsg("cluster.block_device_path is required to replay raw layout WAL")));

	fd = BasicOpenFile(cluster_block_device_path, O_RDWR | PG_BINARY);
	if (fd < 0)
		ereport(PANIC, (errcode_for_file_access(),
						errmsg("could not open raw block device \"%s\" during WAL replay: %m",
							   cluster_block_device_path)));

	if (pg_pwrite(fd, image, BLCKSZ, (off_t)rec->offset) != BLCKSZ)
		ereport(PANIC, (errcode_for_file_access(),
						errmsg("could not replay raw layout page at offset " UINT64_FORMAT ": %m",
							   rec->offset)));
	if (pg_fsync(fd) != 0)
		ereport(PANIC, (errcode_for_file_access(),
						errmsg("could not fsync raw block device \"%s\" during WAL replay: %m",
							   cluster_block_device_path)));

	close(fd);
}

#endif /* USE_PGRAC_CLUSTER */
