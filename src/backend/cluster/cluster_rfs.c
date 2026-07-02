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

#include <ctype.h>
#include <sys/stat.h>
#include <unistd.h>

#include "access/xlog.h"
#include "access/xlog_internal.h"
#include "access/xlogrecovery.h"
#include "cluster/cluster_adg.h"
#include "cluster/cluster_guc.h"
#include "cluster/cluster_mrp.h"
#include "cluster/cluster_rfs.h"
#include "cluster/cluster_standby_scn.h"
#include "cluster/cluster_xlog.h"
#include "fmgr.h"
#include "libpq/pqformat.h"
#include "libpq/pqsignal.h"
#include "miscadmin.h"
#include "postmaster/interrupt.h"
#include "replication/walreceiver.h"
#include "storage/fd.h"
#include "storage/ipc.h"
#include "tcop/tcopprot.h"
#include "utils/guc.h"
#include "utils/memutils.h"
#include "utils/ps_status.h"
#include "utils/timestamp.h"
#include "utils/wait_event.h"

#define RFS_IDLE_TIMEOUT_MS 1000
#define RFS_POLL_TIMEOUT_MS 100
#define RFS_RECONNECT_MIN_MS 1000
#define RFS_RECONNECT_MAX_MS 30000

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

typedef enum ClusterRfsUpstreamState {
	CLUSTER_RFS_UPSTREAM_DISCONNECTED = 0,
	CLUSTER_RFS_UPSTREAM_STREAMING
} ClusterRfsUpstreamState;

typedef struct ClusterRfsUpstream {
	int index;
	uint16 expected_thread_id;
	char *conninfo;
	WalReceiverConn *conn;
	ClusterRfsUpstreamState state;
	TimeLineID tli;
	XLogRecPtr start_lsn;
	XLogRecPtr write_lsn;
	XLogRecPtr latest_wal_end;
	TimestampTz latest_wal_end_time;
	TimestampTz last_msg_receipt_time;
	TimestampTz last_msg_send_time;
	TimestampTz next_reconnect_time;
	int reconnect_attempts;
	uint16 last_thread_id;
	uint64 received_messages;
	uint64 received_bytes;
	uint64 error_count;
	pgsocket wait_fd;
	ClusterRfsPendingHeader pending_header;
	StringInfoData incoming_message;
	StringInfoData reply_message;
	bool message_buffers_initialized;
} ClusterRfsUpstream;

static ClusterRfsThreadFile cluster_rfs_thread_files[CLUSTER_WAL_THREAD_MAX + 1];
static bool cluster_rfs_thread_files_initialized = false;
static ClusterRfsPendingHeader cluster_rfs_pending_header;

static const char *
cluster_rfs_skip_ws(const char *p)
{
	while (p != NULL && (*p == ' ' || *p == '\t' || *p == '\n' || *p == '\r'))
		p++;
	return p;
}

int
cluster_rfs_configured_upstream_count(void)
{
	const char *p = cluster_adg_rfs_conninfos;
	int count = 0;

	p = cluster_rfs_skip_ws(p);
	while (p != NULL && *p != '\0') {
		const char *start = p;
		const char *end;

		while (*p != '\0' && *p != ';')
			p++;
		end = p;
		while (end > start
			   && (end[-1] == ' ' || end[-1] == '\t' || end[-1] == '\n' || end[-1] == '\r'))
			end--;
		if (end > start)
			count++;
		if (*p == ';')
			p++;
		p = cluster_rfs_skip_ws(p);
	}

	return count;
}

bool
cluster_rfs_should_start(void)
{
	if (!cluster_mrp_should_start())
		return false;
	return cluster_rfs_configured_upstream_count() > 0;
}

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

static bool
cluster_rfs_parse_thread_prefix(const char **start_inout, const char *end, uint16 *thread_id_out)
{
	const char *p;
	uint64 value = 0;

	if (start_inout == NULL || *start_inout == NULL || thread_id_out == NULL)
		return false;

	p = cluster_rfs_skip_ws(*start_inout);
	if (p >= end || strncmp(p, "thread_id=", 10) != 0)
		return false;
	p += 10;
	if (p >= end || !isdigit((unsigned char)*p))
		ereport(FATAL, (errcode(ERRCODE_CONFIG_FILE_ERROR),
						errmsg("invalid cluster.adg_rfs_conninfos thread_id prefix")));
	while (p < end && isdigit((unsigned char)*p)) {
		value = value * 10 + (uint64)(*p - '0');
		if (value > CLUSTER_WAL_THREAD_MAX)
			ereport(FATAL, (errcode(ERRCODE_CONFIG_FILE_ERROR),
							errmsg("cluster.adg_rfs_conninfos thread_id exceeds %d",
								   CLUSTER_WAL_THREAD_MAX)));
		p++;
	}
	if (p < end && *p != ' ' && *p != '\t' && *p != '\n' && *p != '\r')
		ereport(
			FATAL,
			(errcode(ERRCODE_CONFIG_FILE_ERROR),
			 errmsg("cluster.adg_rfs_conninfos thread_id prefix must be followed by whitespace")));
	if (!cluster_rfs_valid_real_thread_id((uint16)value))
		ereport(FATAL, (errcode(ERRCODE_CONFIG_FILE_ERROR),
						errmsg("cluster.adg_rfs_conninfos thread_id must be between %d and %d",
							   XLP_THREAD_ID_FIRST_REAL, CLUSTER_WAL_THREAD_MAX)));

	*thread_id_out = (uint16)value;
	*start_inout = cluster_rfs_skip_ws(p);
	return true;
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
cluster_rfs_finish_pending_header(ClusterRfsPendingHeader *pending, const char **buf, Size *nbytes,
								  XLogRecPtr *recptr, TimeLineID tli, uint16 expected_thread_id,
								  uint16 *current_thread_id)
{
	Size need;
	Size take;
	uint16 header_thread_id;

	if (!pending->active)
		return true;
	if (*recptr != pending->recptr + pending->len)
		ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED),
						errmsg("ADG RFS received a non-contiguous split WAL page header")));

	need = sizeof(XLogPageHeaderData) - pending->len;
	take = Min(need, *nbytes);
	memcpy(pending->data + pending->len, *buf, take);
	pending->len += take;
	*buf += take;
	*nbytes -= take;
	*recptr += take;

	if (pending->len < sizeof(XLogPageHeaderData))
		return false;

	header_thread_id = cluster_rfs_thread_from_header((const XLogPageHeaderData *)pending->data);
	if (!cluster_rfs_valid_real_thread_id(header_thread_id))
		ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED),
						errmsg("ADG RFS could not route a split WAL page header")));
	if (cluster_rfs_valid_real_thread_id(expected_thread_id)
		&& header_thread_id != expected_thread_id)
		ereport(ERROR, (errcode(ERRCODE_PROTOCOL_VIOLATION),
						errmsg("ADG RFS received WAL for thread %u but is bound to thread %u",
							   (unsigned)header_thread_id, (unsigned)expected_thread_id)));

	cluster_rfs_write_thread_chunk(header_thread_id, tli, pending->recptr, pending->data,
								   sizeof(XLogPageHeaderData));
	cluster_mrp_mark_thread_received_span(header_thread_id, pending->recptr,
										  pending->recptr + sizeof(XLogPageHeaderData));
	*current_thread_id = header_thread_id;
	pending->active = false;
	pending->len = 0;
	return true;
}

static void
cluster_rfs_observe_received_chunk_internal(const char *buf, Size nbytes, XLogRecPtr recptr,
											TimeLineID tli, int native_wal_fd,
											uint16 *last_thread_id,
											ClusterRfsPendingHeader *pending_header,
											uint16 expected_thread_id,
											bool allow_landed_page_prefix)
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

	if (!cluster_rfs_finish_pending_header(pending_header, &buf, &nbytes, &recptr, tli,
										   expected_thread_id, &observed_thread_id))
		return;

	while (nbytes > 0) {
		uint32 page_offset = recptr % XLOG_BLCKSZ;
		Size piece = Min(nbytes, (Size)(XLOG_BLCKSZ - page_offset));
		uint16 route_thread_id = observed_thread_id;

		if (page_offset == 0) {
			if (nbytes < sizeof(XLogPageHeaderData)) {
				pending_header->active = true;
				pending_header->recptr = recptr;
				pending_header->len = nbytes;
				memcpy(pending_header->data, buf, nbytes);
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
		if (cluster_rfs_valid_real_thread_id(expected_thread_id)
			&& route_thread_id != expected_thread_id)
			ereport(ERROR, (errcode(ERRCODE_PROTOCOL_VIOLATION),
							errmsg("ADG RFS received WAL for thread %u but is bound to thread %u",
								   (unsigned)route_thread_id, (unsigned)expected_thread_id)));
		if (page_offset != 0 && !allow_landed_page_prefix)
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
cluster_rfs_observe_received_chunk(const char *buf, Size nbytes, XLogRecPtr recptr, TimeLineID tli,
								   int native_wal_fd, uint16 *last_thread_id)
{
	cluster_rfs_observe_received_chunk_internal(buf, nbytes, recptr, tli, native_wal_fd,
												last_thread_id, &cluster_rfs_pending_header,
												XLP_THREAD_ID_LEGACY, false);
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

static TimestampTz
cluster_rfs_timestamp_after_ms(long delay_ms)
{
	return GetCurrentTimestamp() + ((TimestampTz)delay_ms * 1000);
}

static long
cluster_rfs_reconnect_delay_ms(int attempts)
{
	long delay = RFS_RECONNECT_MIN_MS;
	int shift = Min(attempts, 5);

	delay <<= shift;
	return Min(delay, RFS_RECONNECT_MAX_MS);
}

static XLogRecPtr
cluster_rfs_page_start(XLogRecPtr ptr)
{
	uint32 offset = ptr % XLOG_BLCKSZ;

	return ptr - offset;
}

static void
cluster_rfs_init_message_buffers(ClusterRfsUpstream *upstream)
{
	if (upstream->message_buffers_initialized)
		return;
	initStringInfo(&upstream->incoming_message);
	initStringInfo(&upstream->reply_message);
	upstream->message_buffers_initialized = true;
}

static void
cluster_rfs_disconnect_upstream(ClusterRfsUpstream *upstream, bool schedule_reconnect)
{
	TimeLineID next_tli = 0;

	if (upstream->conn != NULL) {
		if (upstream->state == CLUSTER_RFS_UPSTREAM_STREAMING) {
			PG_TRY();
			{
				walrcv_endstreaming(upstream->conn, &next_tli);
			}
			PG_CATCH();
			{
				FlushErrorState();
			}
			PG_END_TRY();
		}
		walrcv_disconnect(upstream->conn);
	}

	upstream->conn = NULL;
	upstream->state = CLUSTER_RFS_UPSTREAM_DISCONNECTED;
	upstream->wait_fd = PGINVALID_SOCKET;
	upstream->pending_header.active = false;
	upstream->pending_header.len = 0;
	if (schedule_reconnect) {
		upstream->next_reconnect_time = cluster_rfs_timestamp_after_ms(
			cluster_rfs_reconnect_delay_ms(upstream->reconnect_attempts));
		upstream->reconnect_attempts++;
	} else {
		upstream->next_reconnect_time = 0;
		upstream->reconnect_attempts = 0;
	}
}

static void
cluster_rfs_send_reply(ClusterRfsUpstream *upstream, bool force, bool request_reply)
{
	XLogRecPtr apply_lsn;
	TimestampTz now;

	if (upstream->conn == NULL)
		return;
	if (!force && wal_receiver_status_interval <= 0)
		return;

	now = GetCurrentTimestamp();
	if (!force && upstream->last_msg_send_time != 0
		&& !TimestampDifferenceExceeds(upstream->last_msg_send_time, now,
									   wal_receiver_status_interval * 1000))
		return;

	apply_lsn = GetXLogReplayRecPtr(NULL);
	resetStringInfo(&upstream->reply_message);
	pq_sendbyte(&upstream->reply_message, 'r');
	pq_sendint64(&upstream->reply_message, upstream->write_lsn);
	pq_sendint64(&upstream->reply_message, upstream->write_lsn);
	pq_sendint64(&upstream->reply_message, apply_lsn);
	pq_sendint64(&upstream->reply_message, now);
	pq_sendbyte(&upstream->reply_message, request_reply ? 1 : 0);
	cluster_rfs_append_reply_trailer(&upstream->reply_message, upstream->last_thread_id);

	walrcv_send(upstream->conn, upstream->reply_message.data, upstream->reply_message.len);
	upstream->last_msg_send_time = now;
}

static void
cluster_rfs_process_wal_message(ClusterRfsUpstream *upstream, char *buf, Size len)
{
	int hdrlen = sizeof(int64) + sizeof(int64) + sizeof(int64);
	XLogRecPtr data_start;
	XLogRecPtr wal_end;
	TimestampTz send_time;

	if (len < hdrlen)
		ereport(ERROR, (errcode(ERRCODE_PROTOCOL_VIOLATION),
						errmsg("invalid ADG RFS WAL message received from primary")));

	resetStringInfo(&upstream->incoming_message);
	appendBinaryStringInfo(&upstream->incoming_message, buf, hdrlen);
	data_start = pq_getmsgint64(&upstream->incoming_message);
	wal_end = pq_getmsgint64(&upstream->incoming_message);
	send_time = pq_getmsgint64(&upstream->incoming_message);

	buf += hdrlen;
	len -= hdrlen;

	upstream->latest_wal_end = wal_end;
	upstream->latest_wal_end_time = send_time;
	upstream->last_msg_receipt_time = GetCurrentTimestamp();

	cluster_rfs_observe_received_chunk_internal(
		buf, len, data_start, upstream->tli, -1, &upstream->last_thread_id,
		&upstream->pending_header, upstream->expected_thread_id, true);
	if (cluster_rfs_valid_real_thread_id(upstream->last_thread_id)
		&& upstream->last_thread_id != upstream->expected_thread_id)
		ereport(ERROR,
				(errcode(ERRCODE_PROTOCOL_VIOLATION),
				 errmsg("ADG RFS upstream %d received WAL for thread %u but is bound to thread %u",
						upstream->index, (unsigned)upstream->last_thread_id,
						(unsigned)upstream->expected_thread_id)));
	if (upstream->write_lsn < data_start + len)
		upstream->write_lsn = data_start + len;
	upstream->received_messages++;
	upstream->received_bytes += len;
}

static bool
cluster_rfs_process_keepalive(ClusterRfsUpstream *upstream, char *buf, Size len)
{
	int hdrlen = sizeof(int64) + sizeof(int64) + sizeof(char);
	XLogRecPtr wal_end;
	TimestampTz send_time;
	bool reply_requested;

	if (len != hdrlen)
		ereport(ERROR, (errcode(ERRCODE_PROTOCOL_VIOLATION),
						errmsg("invalid ADG RFS keepalive message received from primary")));

	resetStringInfo(&upstream->incoming_message);
	appendBinaryStringInfo(&upstream->incoming_message, buf, hdrlen);
	wal_end = pq_getmsgint64(&upstream->incoming_message);
	send_time = pq_getmsgint64(&upstream->incoming_message);
	reply_requested = pq_getmsgbyte(&upstream->incoming_message) != 0;

	upstream->latest_wal_end = wal_end;
	upstream->latest_wal_end_time = send_time;
	upstream->last_msg_receipt_time = GetCurrentTimestamp();
	return reply_requested;
}

static void
cluster_rfs_process_stream_message(ClusterRfsUpstream *upstream, char *buf, int len)
{
	if (len <= 0)
		return;

	switch (buf[0]) {
	case 'w':
		cluster_rfs_process_wal_message(upstream, &buf[1], len - 1);
		break;
	case 'k':
		if (cluster_rfs_process_keepalive(upstream, &buf[1], len - 1))
			cluster_rfs_send_reply(upstream, true, false);
		break;
	default:
		ereport(ERROR, (errcode(ERRCODE_PROTOCOL_VIOLATION),
						errmsg("invalid ADG RFS replication message type %d", buf[0])));
	}
}

static void
cluster_rfs_connect_upstream(ClusterRfsUpstream *upstream)
{
	char *err = NULL;
	char *primary_sysid;
	char standby_sysid[32];
	int primary_thread_count;
	WalRcvStreamOptions options;
	char appname[NAMEDATALEN];

	cluster_rfs_init_message_buffers(upstream);
	snprintf(appname, sizeof(appname), "rfs%u", (unsigned)upstream->expected_thread_id);

	upstream->conn = walrcv_connect(upstream->conninfo, false, false, appname, &err);
	if (upstream->conn == NULL)
		ereport(ERROR, (errcode(ERRCODE_CONNECTION_FAILURE),
						errmsg("could not connect ADG RFS upstream %d: %s", upstream->index,
							   err != NULL ? err : "unknown error")));

	primary_sysid = walrcv_identify_system(upstream->conn, &upstream->tli);
	snprintf(standby_sysid, sizeof(standby_sysid), UINT64_FORMAT, GetSystemIdentifier());
	if (strcmp(primary_sysid, standby_sysid) != 0)
		ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
						errmsg("ADG RFS upstream %d system identifier differs from standby",
							   upstream->index),
						errdetail("The primary's identifier is %s, the standby's identifier is %s.",
								  primary_sysid, standby_sysid)));
	pfree(primary_sysid);

	primary_thread_count = walrcv_get_adg_primary_thread_count(upstream->conn);
	if (primary_thread_count <= 0 || primary_thread_count > CLUSTER_WAL_THREAD_MAX)
		ereport(ERROR, (errcode(ERRCODE_PROTOCOL_VIOLATION),
						errmsg("invalid ADG primary thread count %d", primary_thread_count)));
	cluster_mrp_note_primary_thread_count((uint16)primary_thread_count);

	upstream->start_lsn = cluster_rfs_page_start(GetXLogReplayRecPtr(NULL));
	upstream->write_lsn = upstream->start_lsn;
	upstream->last_thread_id = XLP_THREAD_ID_LEGACY;
	upstream->pending_header.active = false;
	upstream->pending_header.len = 0;

	memset(&options, 0, sizeof(options));
	options.logical = false;
	options.slotname = NULL;
	options.startpoint = upstream->start_lsn;
	options.proto.physical.startpointTLI = upstream->tli;
	options.proto.physical.adg_thread_id = upstream->expected_thread_id;
	if (!walrcv_startstreaming(upstream->conn, &options))
		ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
						errmsg("ADG RFS upstream %d contains no more WAL on timeline %u",
							   upstream->index, upstream->tli)));

	upstream->state = CLUSTER_RFS_UPSTREAM_STREAMING;
	upstream->reconnect_attempts = 0;
	upstream->next_reconnect_time = 0;
	upstream->last_msg_receipt_time = GetCurrentTimestamp();
	upstream->last_msg_send_time = 0;
	cluster_rfs_send_reply(upstream, true, false);
}

static void
cluster_rfs_step_upstream(ClusterRfsUpstream *upstream)
{
	char *buf = NULL;
	int len;
	bool got_data = false;

	if (upstream->state != CLUSTER_RFS_UPSTREAM_STREAMING || upstream->conn == NULL)
		return;

	do {
		upstream->wait_fd = PGINVALID_SOCKET;
		len = walrcv_receive(upstream->conn, &buf, &upstream->wait_fd);
		if (len > 0) {
			got_data = true;
			cluster_rfs_process_stream_message(upstream, buf, len);
		} else if (len < 0) {
			ereport(LOG, (errmsg("ADG RFS upstream %d ended streaming at %X/%X", upstream->index,
								 LSN_FORMAT_ARGS(upstream->write_lsn))));
			cluster_rfs_disconnect_upstream(upstream, true);
			return;
		}
	} while (len > 0);

	if (got_data) {
		cluster_rfs_flush_received_threads();
		cluster_rfs_send_reply(upstream, false, false);
	}
}

static void
cluster_rfs_log_upstream_error(ClusterRfsUpstream *upstream)
{
	ErrorData *edata;

	edata = CopyErrorData();
	FlushErrorState();
	upstream->error_count++;
	cluster_rfs_disconnect_upstream(upstream, true);
	ereport(LOG, (errmsg("ADG RFS upstream %d disconnected; will retry", upstream->index),
				  errdetail("%s", edata->message)));
	FreeErrorData(edata);
}

static void
cluster_rfs_maybe_connect_upstream(ClusterRfsUpstream *upstream)
{
	TimestampTz now;

	if (upstream->state == CLUSTER_RFS_UPSTREAM_STREAMING)
		return;

	now = GetCurrentTimestamp();
	if (upstream->next_reconnect_time != 0 && now < upstream->next_reconnect_time)
		return;

	PG_TRY();
	{
		cluster_rfs_connect_upstream(upstream);
		ereport(LOG, (errmsg("ADG RFS upstream %d for thread %u connected and streaming from %X/%X",
							 upstream->index, (unsigned)upstream->expected_thread_id,
							 LSN_FORMAT_ARGS(upstream->start_lsn))));
	}
	PG_CATCH();
	{
		cluster_rfs_log_upstream_error(upstream);
	}
	PG_END_TRY();
}

static void
cluster_rfs_maybe_step_upstream(ClusterRfsUpstream *upstream)
{
	PG_TRY();
	{
		cluster_rfs_step_upstream(upstream);
	}
	PG_CATCH();
	{
		cluster_rfs_log_upstream_error(upstream);
	}
	PG_END_TRY();
}

static int
cluster_rfs_parse_upstreams(ClusterRfsUpstream *upstreams, int max_upstreams)
{
	const char *p = cluster_adg_rfs_conninfos;
	bool seen_threads[CLUSTER_WAL_THREAD_MAX + 1];
	int count = 0;

	memset(seen_threads, 0, sizeof(seen_threads));
	p = cluster_rfs_skip_ws(p);
	while (p != NULL && *p != '\0') {
		const char *start = p;
		const char *end;
		Size len;

		while (*p != '\0' && *p != ';')
			p++;
		end = p;
		while (end > start
			   && (end[-1] == ' ' || end[-1] == '\t' || end[-1] == '\n' || end[-1] == '\r'))
			end--;
		len = (Size)(end - start);
		if (len > 0) {
			const char *conn_start = start;
			uint16 expected_thread_id = (uint16)(count + XLP_THREAD_ID_FIRST_REAL);

			if (count >= max_upstreams)
				ereport(FATAL,
						(errcode(ERRCODE_CONFIG_FILE_ERROR),
						 errmsg("cluster.adg_rfs_conninfos declares too many upstream connections"),
						 errhint("Configure at most %d semicolon-separated upstream connections.",
								 CLUSTER_WAL_THREAD_MAX)));
			if (expected_thread_id > CLUSTER_WAL_THREAD_MAX)
				ereport(
					FATAL,
					(errcode(ERRCODE_CONFIG_FILE_ERROR),
					 errmsg("cluster.adg_rfs_conninfos declares too many upstream connections")));
			(void)cluster_rfs_parse_thread_prefix(&conn_start, end, &expected_thread_id);
			if (conn_start >= end)
				ereport(FATAL,
						(errcode(ERRCODE_CONFIG_FILE_ERROR),
						 errmsg("cluster.adg_rfs_conninfos entry has no connection string")));
			if (seen_threads[expected_thread_id])
				ereport(FATAL, (errcode(ERRCODE_CONFIG_FILE_ERROR),
								errmsg("cluster.adg_rfs_conninfos declares duplicate thread_id %u",
									   (unsigned)expected_thread_id)));
			seen_threads[expected_thread_id] = true;
			len = (Size)(end - conn_start);
			memset(&upstreams[count], 0, sizeof(upstreams[count]));
			upstreams[count].index = count + 1;
			upstreams[count].expected_thread_id = expected_thread_id;
			upstreams[count].conninfo = pnstrdup(conn_start, len);
			upstreams[count].wait_fd = PGINVALID_SOCKET;
			upstreams[count].last_thread_id = XLP_THREAD_ID_LEGACY;
			count++;
		}
		if (*p == ';')
			p++;
		p = cluster_rfs_skip_ws(p);
	}

	return count;
}

static int
cluster_rfs_active_upstreams(ClusterRfsUpstream *upstreams, int upstream_count)
{
	int active = 0;

	for (int i = 0; i < upstream_count; i++) {
		if (upstreams[i].state == CLUSTER_RFS_UPSTREAM_STREAMING)
			active++;
	}
	return active;
}

static void
cluster_rfs_disconnect_all_upstreams(ClusterRfsUpstream *upstreams, int upstream_count)
{
	for (int i = 0; i < upstream_count; i++)
		cluster_rfs_disconnect_upstream(&upstreams[i], false);
}

void
RfsMain(void)
{
	int upstream_count;
	ClusterRfsUpstream *upstreams;
	char activitymsg[64];

	if (!IsUnderPostmaster)
		elog(FATAL, "RFS coordinator must run under postmaster");

	MyBackendType = B_RFS;
	init_ps_display(NULL);

	pqsignal(SIGHUP, SignalHandlerForConfigReload);
	pqsignal(SIGINT, SignalHandlerForShutdownRequest);
	pqsignal(SIGTERM, SignalHandlerForShutdownRequest);
	pqsignal(SIGALRM, SIG_IGN);
	pqsignal(SIGPIPE, SIG_IGN);
	pqsignal(SIGUSR1, SIG_IGN);
	pqsignal(SIGUSR2, SIG_IGN);
	pqsignal(SIGCHLD, SIG_DFL);
	sigprocmask(SIG_SETMASK, &UnBlockSig, NULL);

	if (!cluster_rfs_should_start())
		proc_exit(0);

	upstream_count = cluster_rfs_configured_upstream_count();
	if (upstream_count <= 0 || upstream_count > CLUSTER_WAL_THREAD_MAX)
		ereport(FATAL, (errcode(ERRCODE_CONFIG_FILE_ERROR),
						errmsg("cluster.adg_rfs_conninfos declares %d upstream connections",
							   upstream_count),
						errhint("Configure between 1 and %d semicolon-separated upstream "
								"connection strings.",
								CLUSTER_WAL_THREAD_MAX)));

	load_file("libpqwalreceiver", false);
	if (WalReceiverFunctions == NULL)
		elog(ERROR, "libpqwalreceiver didn't initialize correctly");

	upstreams = palloc0(sizeof(ClusterRfsUpstream) * upstream_count);
	if (cluster_rfs_parse_upstreams(upstreams, upstream_count) != upstream_count)
		ereport(FATAL, (errcode(ERRCODE_CONFIG_FILE_ERROR),
						errmsg("cluster.adg_rfs_conninfos changed during RFS startup")));

	snprintf(activitymsg, sizeof(activitymsg), "rfs coordinator 0/%d streaming", upstream_count);
	set_ps_display(activitymsg);

	for (;;) {
		CHECK_FOR_INTERRUPTS();

		if (ConfigReloadPending) {
			ConfigReloadPending = false;
			ProcessConfigFile(PGC_SIGHUP);
		}
		if (ShutdownRequestPending)
			break;

		for (int i = 0; i < upstream_count; i++)
			cluster_rfs_maybe_connect_upstream(&upstreams[i]);
		for (int i = 0; i < upstream_count; i++)
			cluster_rfs_maybe_step_upstream(&upstreams[i]);

		snprintf(activitymsg, sizeof(activitymsg), "rfs coordinator %d/%d streaming",
				 cluster_rfs_active_upstreams(upstreams, upstream_count), upstream_count);
		set_ps_display(activitymsg);

		(void)WaitLatch(MyLatch, WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH,
						RFS_POLL_TIMEOUT_MS, WAIT_EVENT_ADG_WAL_RECEIVE_LAG);
		ResetLatch(MyLatch);
	}

	cluster_rfs_disconnect_all_upstreams(upstreams, upstream_count);
	proc_exit(0);
}

#endif /* USE_PGRAC_CLUSTER */
