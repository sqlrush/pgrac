/*-------------------------------------------------------------------------
 *
 * cluster_rfs.c
 *	  ADG Remote File Server (RFS) helpers.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * IDENTIFICATION
 *	  src/backend/cluster/cluster_rfs.c
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#ifdef USE_PGRAC_CLUSTER

#include <sys/stat.h>
#include <unistd.h>

#include "access/xlog.h"
#include "access/xlog_internal.h"
#include "cluster/cluster_adg.h"
#include "cluster/cluster_guc.h"
#include "cluster/cluster_mrp.h"
#include "cluster/cluster_rfs.h"
#include "cluster/cluster_standby_scn.h"
#include "cluster/cluster_xlog.h"
#include "libpq/pqformat.h"
#include "storage/fd.h"
#include "utils/wait_event.h"

typedef struct ClusterRfsThreadFile {
	int fd;
	TimeLineID tli;
	XLogSegNo segno;
	bool valid;
} ClusterRfsThreadFile;

typedef struct ClusterRfsPendingHeader {
	bool active;
	XLogRecPtr recptr;
	Size len;
	char data[sizeof(XLogPageHeaderData)];
} ClusterRfsPendingHeader;

static ClusterRfsThreadFile cluster_rfs_thread_files[CLUSTER_WAL_THREAD_MAX + 1];
static bool cluster_rfs_thread_files_initialized = false;
static ClusterRfsPendingHeader cluster_rfs_pending_header;

static bool
cluster_rfs_landing_enabled(void)
{
	return cluster_mrp_should_start() && cluster_wal_threads_dir != NULL
		   && cluster_wal_threads_dir[0] != '\0';
}

static bool
cluster_rfs_valid_real_thread_id(uint16 thread_id)
{
	return thread_id >= XLP_THREAD_ID_FIRST_REAL && thread_id <= CLUSTER_WAL_THREAD_MAX;
}

static void
cluster_rfs_init_thread_files(void)
{
	int i;

	if (cluster_rfs_thread_files_initialized)
		return;
	for (i = 0; i <= CLUSTER_WAL_THREAD_MAX; i++) {
		cluster_rfs_thread_files[i].fd = -1;
		cluster_rfs_thread_files[i].tli = 0;
		cluster_rfs_thread_files[i].segno = 0;
		cluster_rfs_thread_files[i].valid = false;
	}
	cluster_rfs_thread_files_initialized = true;
}

static bool
cluster_rfs_thread_dir_path(uint16 thread_id, char *buf, size_t buflen)
{
	int n;

	n = snprintf(buf, buflen, "%s/thread_%u", cluster_wal_threads_dir, (unsigned)thread_id);
	return n > 0 && (size_t)n < buflen;
}

static bool
cluster_rfs_thread_segment_path(uint16 thread_id, TimeLineID tli, XLogSegNo segno, char *buf,
								size_t buflen)
{
	char dir[MAXPGPATH];
	char fname[MAXFNAMELEN];
	int n;

	if (!cluster_rfs_thread_dir_path(thread_id, dir, sizeof(dir)))
		return false;
	XLogFileName(fname, tli, segno, wal_segment_size);
	n = snprintf(buf, buflen, "%s/%s", dir, fname);
	return n > 0 && (size_t)n < buflen;
}

static int
cluster_rfs_open_thread_segment(uint16 thread_id, TimeLineID tli, XLogSegNo segno)
{
	ClusterRfsThreadFile *tf;
	char dir[MAXPGPATH];
	char path[MAXPGPATH];
	bool created = false;
	int fd;

	if (!cluster_rfs_valid_real_thread_id(thread_id))
		ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						errmsg("ADG RFS received invalid WAL thread id %u", (unsigned)thread_id)));
	if (!cluster_rfs_thread_dir_path(thread_id, dir, sizeof(dir))
		|| !cluster_rfs_thread_segment_path(thread_id, tli, segno, path, sizeof(path)))
		ereport(ERROR, (errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
						errmsg("ADG RFS per-thread WAL path is too long")));

	cluster_rfs_init_thread_files();
	tf = &cluster_rfs_thread_files[thread_id];
	if (tf->fd >= 0 && tf->valid && tf->tli == tli && tf->segno == segno)
		return tf->fd;

	if (tf->fd >= 0) {
		if (pg_fsync(tf->fd) != 0) {
			int save_errno = errno;

			close(tf->fd);
			tf->fd = -1;
			tf->valid = false;
			errno = save_errno;
			ereport(ERROR, (errcode_for_file_access(),
							errmsg("could not fsync ADG RFS WAL segment \"%s\": %m", path)));
		}
		close(tf->fd);
		tf->fd = -1;
		tf->valid = false;
	}

	fd = BasicOpenFile(path, O_RDWR | PG_BINARY | O_CLOEXEC);
	if (fd < 0 && errno == ENOENT) {
		struct stat st;

		if (stat(dir, &st) != 0 || !S_ISDIR(st.st_mode))
			ereport(ERROR,
					(errcode_for_file_access(),
					 errmsg("ADG RFS WAL thread directory \"%s\" is not accessible: %m", dir)));

		fd = BasicOpenFile(path, O_RDWR | O_CREAT | O_EXCL | PG_BINARY | O_CLOEXEC);
		created = (fd >= 0);
	}
	if (fd < 0)
		ereport(ERROR, (errcode_for_file_access(),
						errmsg("could not open ADG RFS WAL segment \"%s\": %m", path)));
	if (created)
		fsync_fname(dir, true);

	tf->fd = fd;
	tf->tli = tli;
	tf->segno = segno;
	tf->valid = true;
	return fd;
}

static void
cluster_rfs_write_thread_chunk(uint16 thread_id, TimeLineID tli, XLogRecPtr recptr, const char *buf,
							   Size nbytes)
{
	XLogSegNo segno;
	uint32 startoff;
	int fd;
	ssize_t nwritten;

	if (nbytes == 0)
		return;
	if (XLogRecPtrIsInvalid(recptr))
		ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						errmsg("ADG RFS cannot route WAL at an invalid LSN")));

	XLByteToSeg(recptr, segno, wal_segment_size);
	startoff = XLogSegmentOffset(recptr, wal_segment_size);
	if (startoff + nbytes > (Size)wal_segment_size)
		ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						errmsg("ADG RFS WAL write crosses a segment boundary")));

	fd = cluster_rfs_open_thread_segment(thread_id, tli, segno);
	errno = 0;
	pgstat_report_wait_start(WAIT_EVENT_WAL_WRITE);
	nwritten = pg_pwrite(fd, buf, nbytes, (off_t)startoff);
	pgstat_report_wait_end();
	if (nwritten != (ssize_t)nbytes) {
		if (nwritten >= 0)
			errno = EIO;
		ereport(ERROR, (errcode_for_file_access(),
						errmsg("could not write ADG RFS WAL for thread %u at %X/%X: %m",
							   (unsigned)thread_id, LSN_FORMAT_ARGS(recptr))));
	}
}

static uint16
cluster_rfs_thread_from_header(const XLogPageHeaderData *hdr)
{
	if (hdr->xlp_magic == XLOG_PAGE_MAGIC
		&& cluster_xlog_validate_page_header(hdr->xlp_thread_id, hdr->xlp_cluster_flags,
											 XLP_THREAD_ID_INVALID))
		return hdr->xlp_thread_id;
	return XLP_THREAD_ID_INVALID;
}

static uint16
cluster_rfs_read_native_page_thread(int native_wal_fd, XLogRecPtr recptr)
{
	XLogPageHeaderData hdr;
	XLogRecPtr page_start;
	uint32 page_offset;
	uint32 seg_offset;
	ssize_t nread;

	if (native_wal_fd < 0)
		return XLP_THREAD_ID_INVALID;

	page_offset = recptr % XLOG_BLCKSZ;
	page_start = recptr - page_offset;
	seg_offset = XLogSegmentOffset(page_start, wal_segment_size);
	errno = 0;
	nread = pg_pread(native_wal_fd, &hdr, sizeof(hdr), (off_t)seg_offset);
	if (nread != (ssize_t)sizeof(hdr))
		return XLP_THREAD_ID_INVALID;
	return cluster_rfs_thread_from_header(&hdr);
}

static void
cluster_rfs_backfill_page_prefix(uint16 thread_id, TimeLineID tli, int native_wal_fd,
								 XLogRecPtr recptr)
{
	char page_prefix[XLOG_BLCKSZ];
	XLogRecPtr page_start;
	uint32 page_offset;
	uint32 seg_offset;
	ssize_t nread;

	page_offset = recptr % XLOG_BLCKSZ;
	if (page_offset == 0)
		return;
	if (native_wal_fd < 0)
		ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						errmsg("ADG RFS cannot backfill a partial WAL page without native WAL")));

	page_start = recptr - page_offset;
	seg_offset = XLogSegmentOffset(page_start, wal_segment_size);
	errno = 0;
	nread = pg_pread(native_wal_fd, page_prefix, page_offset, (off_t)seg_offset);
	if (nread != (ssize_t)page_offset) {
		if (nread >= 0)
			errno = EIO;
		ereport(ERROR, (errcode_for_file_access(),
						errmsg("could not read native WAL page prefix at %X/%X for ADG RFS: %m",
							   LSN_FORMAT_ARGS(page_start))));
	}
	cluster_rfs_write_thread_chunk(thread_id, tli, page_start, page_prefix, page_offset);
}

static bool
cluster_rfs_finish_pending_header(const char **buf, Size *nbytes, XLogRecPtr *recptr,
								  TimeLineID tli, uint16 *current_thread_id)
{
	Size need;
	Size take;
	uint16 header_thread_id;

	if (!cluster_rfs_pending_header.active)
		return true;
	if (*recptr != cluster_rfs_pending_header.recptr + cluster_rfs_pending_header.len)
		ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED),
						errmsg("ADG RFS received a non-contiguous split WAL page header")));

	need = sizeof(XLogPageHeaderData) - cluster_rfs_pending_header.len;
	take = Min(need, *nbytes);
	memcpy(cluster_rfs_pending_header.data + cluster_rfs_pending_header.len, *buf, take);
	cluster_rfs_pending_header.len += take;
	*buf += take;
	*nbytes -= take;
	*recptr += take;

	if (cluster_rfs_pending_header.len < sizeof(XLogPageHeaderData))
		return false;

	header_thread_id = cluster_rfs_thread_from_header(
		(const XLogPageHeaderData *)cluster_rfs_pending_header.data);
	if (!cluster_rfs_valid_real_thread_id(header_thread_id))
		ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED),
						errmsg("ADG RFS could not route a split WAL page header")));

	cluster_rfs_write_thread_chunk(header_thread_id, tli, cluster_rfs_pending_header.recptr,
								   cluster_rfs_pending_header.data, sizeof(XLogPageHeaderData));
	cluster_mrp_mark_thread_received_span(header_thread_id, cluster_rfs_pending_header.recptr,
										  cluster_rfs_pending_header.recptr
											  + sizeof(XLogPageHeaderData));
	*current_thread_id = header_thread_id;
	cluster_rfs_pending_header.active = false;
	cluster_rfs_pending_header.len = 0;
	return true;
}

void
cluster_rfs_observe_received_chunk(const char *buf, Size nbytes, XLogRecPtr recptr, TimeLineID tli,
								   int native_wal_fd, uint16 *last_thread_id)
{
	XLogRecPtr chunk_end;
	uint16 observed_thread_id;
	bool landing_enabled;

	if (!cluster_mrp_should_start())
		return;
	if (buf == NULL || nbytes == 0 || XLogRecPtrIsInvalid(recptr))
		return;

	observed_thread_id = last_thread_id != NULL ? *last_thread_id : XLP_THREAD_ID_LEGACY;
	chunk_end = recptr + nbytes;
	landing_enabled = cluster_rfs_landing_enabled();

	if (!landing_enabled) {
		XLogRecPtr page_lsn;
		uint32 page_offset;

		page_offset = recptr % XLOG_BLCKSZ;
		if (page_offset == 0)
			page_lsn = recptr;
		else
			page_lsn = recptr + (XLOG_BLCKSZ - page_offset);

		while (page_lsn <= chunk_end && chunk_end - page_lsn >= sizeof(XLogPageHeaderData)) {
			Size offset = (Size)(page_lsn - recptr);
			XLogPageHeaderData hdr;
			uint16 header_thread_id;

			memcpy(&hdr, buf + offset, sizeof(hdr));
			header_thread_id = cluster_rfs_thread_from_header(&hdr);
			if (header_thread_id != XLP_THREAD_ID_INVALID)
				observed_thread_id = header_thread_id;
			page_lsn += XLOG_BLCKSZ;
		}

		if (last_thread_id != NULL)
			*last_thread_id = observed_thread_id;
		cluster_standby_scn_mark_received(observed_thread_id, chunk_end);
		return;
	}

	if (!cluster_rfs_finish_pending_header(&buf, &nbytes, &recptr, tli, &observed_thread_id))
		return;

	while (nbytes > 0) {
		uint32 page_offset = recptr % XLOG_BLCKSZ;
		Size piece = Min(nbytes, (Size)(XLOG_BLCKSZ - page_offset));
		uint16 route_thread_id = observed_thread_id;

		if (page_offset == 0) {
			if (nbytes < sizeof(XLogPageHeaderData)) {
				cluster_rfs_pending_header.active = true;
				cluster_rfs_pending_header.recptr = recptr;
				cluster_rfs_pending_header.len = nbytes;
				memcpy(cluster_rfs_pending_header.data, buf, nbytes);
				break;
			} else {
				XLogPageHeaderData hdr;
				uint16 header_thread_id;

				memcpy(&hdr, buf, sizeof(hdr));
				header_thread_id = cluster_rfs_thread_from_header(&hdr);
				route_thread_id = header_thread_id;
			}
		} else if (!cluster_rfs_valid_real_thread_id(route_thread_id))
			route_thread_id = cluster_rfs_read_native_page_thread(native_wal_fd, recptr);

		if (!cluster_rfs_valid_real_thread_id(route_thread_id))
			ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED),
							errmsg("ADG RFS could not route WAL at %X/%X to a real thread",
								   LSN_FORMAT_ARGS(recptr))));
		if (page_offset != 0)
			cluster_rfs_backfill_page_prefix(route_thread_id, tli, native_wal_fd, recptr);
		cluster_rfs_write_thread_chunk(route_thread_id, tli, recptr, buf, piece);

		observed_thread_id = route_thread_id;
		cluster_mrp_mark_thread_received_span(observed_thread_id, recptr, recptr + piece);

		buf += piece;
		recptr += piece;
		nbytes -= piece;
	}

	if (last_thread_id != NULL)
		*last_thread_id = observed_thread_id;
	if (recptr == chunk_end && observed_thread_id != XLP_THREAD_ID_INVALID)
		cluster_standby_scn_mark_received(observed_thread_id, chunk_end);
}

void
cluster_rfs_flush_received_threads(void)
{
	int thread_id;

	if (!cluster_rfs_landing_enabled() || !cluster_rfs_thread_files_initialized)
		return;

	for (thread_id = XLP_THREAD_ID_FIRST_REAL; thread_id <= CLUSTER_WAL_THREAD_MAX; thread_id++) {
		ClusterRfsThreadFile *tf = &cluster_rfs_thread_files[thread_id];

		if (tf->fd < 0 || !tf->valid)
			continue;
		pgstat_report_wait_start(WAIT_EVENT_WAL_SYNC);
		if (pg_fsync(tf->fd) != 0) {
			int save_errno = errno;

			pgstat_report_wait_end();
			errno = save_errno;
			ereport(ERROR, (errcode_for_file_access(),
							errmsg("could not fsync ADG RFS WAL for thread %d: %m", thread_id)));
		}
		pgstat_report_wait_end();
	}
}

void
cluster_rfs_append_reply_trailer(StringInfo reply_message, uint16 last_thread_id)
{
	if (reply_message == NULL || !cluster_mrp_should_start())
		return;

	pq_sendint(reply_message, CLUSTER_ADG_REPLY_MAGIC, 4);
	pq_sendint16(reply_message, CLUSTER_ADG_REPLY_VERSION);
	pq_sendint16(reply_message, last_thread_id);
	pq_sendint64(reply_message, (uint64)cluster_mrp_standby_consistent_scn());
	pq_sendint64(reply_message, cluster_mrp_apply_master_term());
}

#endif /* USE_PGRAC_CLUSTER */
