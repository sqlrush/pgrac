/* PGRAC: verify actual WAL from the exact root-selected writer namespace.
 * Author: SqlRush <sqlrush@gmail.com>
 */
#include "postgres.h"
#include <dirent.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>
#include "access/xlog.h"
#include "access/xlog_internal.h"
#include "access/xlogreader.h"
#include "catalog/pg_control.h"
#include "cluster/cluster_wal_tail.h"
#include "cluster/cluster_wal_thread.h"
#include "miscadmin.h"
#include "storage/fd.h"

typedef struct WalTailSegment {
	struct WalTailSegment *next;
	XLogSegNo number;
	struct stat identity;
} WalTailSegment;

typedef struct WalTailWork {
	const char *root;
	ClusterWalSourceRef ref;
	int dirs[3];
	int segment_fd;
	int suffix_fd;
	DIR *suffix_scan;
	XLogRecPtr last_page_requested;
	XLogRecPtr page_read_at;
	size_t page_bytes;
	bool incomplete_tail;
	WalTailSegment *segments;
	WalTailSegment *current;
	XLogReaderState *reader;
	ClusterControlRootResult result;
	ClusterWalTailObservation observed;
	XLogRecPtr checkpoint_start;
	pg_crc32c checkpoint_crc;
	bool startup_mode;
	bool sync_inputs;
	bool checkpoint_prefix;
	XLogRecPtr flush_end;
	bool flush_boundary;
	const ClusterControlRootSnapshot *sealed;
	ClusterWalRecordVisitor visitor;
	void *visitor_arg;
	ClusterWalStartupObservation startup;
} WalTailWork;

/* Use the same pinned namespace and exact file identities as the scan. A
 * physical fsync is not an isolation certificate or a new durable promise.
 * Descriptor ownership stays in work across ERROR/cancellation.
 * Author: SqlRush <sqlrush@gmail.com> */
static ClusterControlRootResult wal_startup_sync_inputs(WalTailWork *work);

static bool
wal_tail_zero(const char *bytes, size_t size)
{
	for (size_t i = 0; i < size; ++i)
		if (bytes[i] != 0)
			return false;
	return true;
}

static bool
wal_tail_owned(const struct stat *st, bool directory)
{
	return (directory ? S_ISDIR(st->st_mode) : S_ISREG(st->st_mode) && st->st_nlink == 1)
		   && st->st_uid == geteuid() && (st->st_mode & (S_IWGRP | S_IWOTH)) == 0;
}

static bool
wal_tail_same(const struct stat *a, const struct stat *b, bool directory)
{
	return wal_tail_owned(a, directory) && wal_tail_owned(b, directory) && a->st_dev == b->st_dev
		   && a->st_ino == b->st_ino && (directory || a->st_size == b->st_size);
}

static bool
wal_tail_segment_current(WalTailWork *work, const WalTailSegment *segment)
{
	char filename[MAXFNAMELEN];
	struct stat current;
	XLogFileName(filename, work->ref.timeline, segment->number, work->reader->segcxt.ws_segsize);
	return fstatat(work->dirs[2], filename, &current, AT_SYMLINK_NOFOLLOW) == 0
		   && wal_tail_same(&segment->identity, &current, false);
}

static bool
wal_tail_close_segment(WalTailWork *work)
{
	bool same = true;
	int fd = work->segment_fd;
	struct stat st;
	work->segment_fd = -1;
	if (fd >= 0) {
		same = fstat(fd, &st) == 0 && work->current != NULL
			   && wal_tail_same(&work->current->identity, &st, false)
			   && wal_tail_segment_current(work, work->current);
		if (close(fd) != 0) {
			work->result = CLUSTER_CONTROL_ROOT_IO_ERROR;
			return false;
		}
		if (!same)
			work->result = CLUSTER_CONTROL_ROOT_STALE_TOKEN;
	}
	return same;
}

/* One open segment at a time, including records crossing many segments.
 * The recovery owner supplies retention/isolated-source stability. Remember
 * every used inode to detect namespace replacement before returning evidence.
 * A missing/short suffix is accepted only after every selected bound is met;
 * permission, real I/O and foreign-identity failures are never a torn tail.
 */
static int
wal_tail_page(XLogReaderState *reader, XLogRecPtr pageptr, int required,
			  XLogRecPtr recordptr pg_attribute_unused(), char *page)
{
	WalTailWork *work = reader->private_data;
	XLogSegNo number;
	struct stat st;
	XLogPageHeader header;
	size_t used = 0;
	size_t wanted = XLOG_BLCKSZ;
	CHECK_FOR_INTERRUPTS();
	if (required < 0 || required > XLOG_BLCKSZ || pageptr % XLOG_BLCKSZ != 0) {
		work->result = CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT;
		return XLREAD_FAIL;
	}
	if (work->flush_end != InvalidXLogRecPtr) {
		if (pageptr >= work->flush_end || (uint64)required > work->flush_end - pageptr) {
			work->flush_boundary = true;
			return XLREAD_FAIL;
		}
		wanted = Min((XLogRecPtr)XLOG_BLCKSZ, work->flush_end - pageptr);
	}
	work->last_page_requested = Max(work->last_page_requested, pageptr);
	XLByteToSeg(pageptr, number, reader->segcxt.ws_segsize);
	if (work->segment_fd < 0 || work->current->number != number) {
		char filename[MAXFNAMELEN];
		WalTailSegment *segment;
		if (!wal_tail_close_segment(work))
			return XLREAD_FAIL;
		/* Allocate before opening: catch cleanup owns every descriptor. */
		segment = palloc0(sizeof(*segment));
		segment->number = number;
		segment->next = work->segments;
		work->segments = segment;
		work->current = segment;
		XLogFileName(filename, work->ref.timeline, number, reader->segcxt.ws_segsize);
		work->segment_fd = openat(work->dirs[2], filename,
								  O_RDONLY | O_NOFOLLOW | O_NONBLOCK | O_CLOEXEC | PG_BINARY);
		if (work->segment_fd < 0) {
			int saved_errno = errno;
			/* An absent next segment may be a legal unacknowledged tail.
			 * Do not put an absent file in the set of read segment identities. */
			work->segments = segment->next;
			work->current = NULL;
			pfree(segment);
			if (saved_errno != ENOENT)
				work->result = CLUSTER_CONTROL_ROOT_IO_ERROR;
			else
				work->incomplete_tail = true;
			return XLREAD_FAIL;
		}
		if (fstat(work->segment_fd, &segment->identity) != 0
			|| !wal_tail_owned(&segment->identity, false)
			|| segment->identity.st_size > reader->segcxt.ws_segsize) {
			work->result = CLUSTER_CONTROL_ROOT_IO_ERROR;
			return XLREAD_FAIL;
		}
	}
	if (fstat(work->segment_fd, &st) != 0 || !wal_tail_same(&work->current->identity, &st, false)
		|| !wal_tail_segment_current(work, work->current)) {
		work->result = CLUSTER_CONTROL_ROOT_STALE_TOKEN;
		return XLREAD_FAIL;
	}
	while (used < wanted) {
		ssize_t count = pread(work->segment_fd, page + used, wanted - used,
							  XLogSegmentOffset(pageptr, reader->segcxt.ws_segsize) + used);
		if (count < 0 && errno == EINTR) {
			CHECK_FOR_INTERRUPTS();
			continue;
		}
		if (count < 0) {
			work->result = CLUSTER_CONTROL_ROOT_IO_ERROR;
			return XLREAD_FAIL;
		}
		if (count == 0)
			break;
		used += count;
	}
	work->page_read_at = pageptr;
	work->page_bytes = used;
	if (used < (size_t)required) {
		work->incomplete_tail = true;
		return XLREAD_FAIL;
	}
	header = (XLogPageHeader)page;
	/* Zero/unwritten tail headers are diagnosed by the native decoder. */
	if (used >= SizeOfXLogLongPHD && header->xlp_magic == XLOG_PAGE_MAGIC
		&& (header->xlp_info & XLP_LONG_HEADER) != 0) {
		XLogLongPageHeader long_header = (XLogLongPageHeader)page;
		if (long_header->xlp_sysid != work->ref.claim.identity.system_identifier
			|| long_header->xlp_seg_size != reader->segcxt.ws_segsize
			|| long_header->xlp_xlog_blcksz != XLOG_BLCKSZ) {
			work->result = CLUSTER_CONTROL_ROOT_IDENTITY_MISMATCH;
			return XLREAD_FAIL;
		}
	}
	if (used >= SizeOfXLogShortPHD && header->xlp_magic == XLOG_PAGE_MAGIC
		&& (header->xlp_thread_id != work->ref.claim.identity.origin_thread_id
			|| header->xlp_tli != work->ref.timeline)) {
		work->result = CLUSTER_CONTROL_ROOT_IDENTITY_MISMATCH;
		return XLREAD_FAIL;
	}
	if (wal_tail_zero(page, used)
		|| (used >= SizeOfXLogShortPHD && header->xlp_magic == XLOG_PAGE_MAGIC
			&& header->xlp_pageaddr < pageptr)) {
		work->incomplete_tail = true;
		return XLREAD_FAIL;
	}
	return (int)used;
}

/* A CRC-valid successor linked to the failed record is positive evidence of
 * written input even within the same page.  This is only a refusal witness,
 * never permission to decode/apply the successor or skip the damaged record. */
static bool
wal_tail_has_same_page_successor(const WalTailWork *work, size_t offset)
{
	const char *page = work->reader->readBuf;
	XLogRecord failed, next;
	size_t successor;
	pg_crc32c crc;

	if (work->page_bytes - offset < SizeOfXLogRecord)
		return false;
	memcpy(&failed, page + offset, SizeOfXLogRecord);
	if (failed.xl_tot_len < SizeOfXLogRecord
		|| failed.xl_tot_len > work->page_bytes - offset)
		return false;
	successor = MAXALIGN(offset + failed.xl_tot_len);
	if (successor > work->page_bytes || work->page_bytes - successor < SizeOfXLogRecord)
		return false;
	memcpy(&next, page + successor, SizeOfXLogRecord);
	if (next.xl_prev != work->page_read_at + offset || next.xl_tot_len < SizeOfXLogRecord
		|| next.xl_tot_len > work->page_bytes - successor)
		return false;
	INIT_CRC32C(crc);
	COMP_CRC32C(crc, page + successor + SizeOfXLogRecord, next.xl_tot_len - SizeOfXLogRecord);
	COMP_CRC32C(crc, page + successor, offsetof(XLogRecord, xl_crc));
	FIN_CRC32C(crc);
	return EQ_CRC32C(next.xl_crc, crc);
}

/* XLogWrite writes whole pages: a later inserter can leave a valid record
 * header but an incomplete body in the last initialized page.  Native CRC
 * failure there can be EOF, just like an entirely unwritten record suffix.
 * Known input bounds and initialized successor pages are checked separately.
 * Invalid headers/links and CRC-valid malformed bodies remain refusals. */
static bool
wal_tail_normal_end(const WalTailWork *work)
{
	const XLogReaderState *reader = work->reader;
	const XLogPageHeaderData *header = (const XLogPageHeaderData *)reader->readBuf;
	XLogRecPtr page = reader->currRecPtr - reader->currRecPtr % XLOG_BLCKSZ;
	size_t offset = reader->currRecPtr % XLOG_BLCKSZ;
	size_t header_size;

	if (work->incomplete_tail)
		return true;
	if (work->page_read_at != page || work->page_bytes < SizeOfXLogShortPHD
		|| header->xlp_magic != XLOG_PAGE_MAGIC || header->xlp_pageaddr != page
		|| header->xlp_thread_id != work->ref.claim.identity.origin_thread_id
		|| header->xlp_tli != work->ref.timeline)
		return false;
	header_size = XLogPageHeaderSize(header);
	if (offset == 0)
		offset = header_size;
	if (offset < header_size || offset >= work->page_bytes)
		return false;
	if (reader->cluster_record_crc_failed)
		return !wal_tail_has_same_page_successor(work, offset);
	return wal_tail_zero(reader->readBuf + offset, work->page_bytes - offset);
}

static bool
wal_tail_paths_current(WalTailWork *work)
{
	char thread[32], generation[48];
	const char *parts[] = { thread, generation };
	struct stat current, pinned;
	snprintf(thread, sizeof(thread), "thread_%u", work->ref.claim.identity.origin_thread_id);
	snprintf(generation, sizeof(generation), "generation_" UINT64_FORMAT,
			 work->ref.claim.identity.origin_owner_incarnation);
	for (size_t i = 0; i < lengthof(work->dirs); ++i) {
		if (work->dirs[i] < 0 || fstat(work->dirs[i], &pinned) != 0
			|| (i == 0 ? lstat(work->root, &current)
					   : fstatat(work->dirs[i - 1], parts[i - 1], &current, AT_SYMLINK_NOFOLLOW))
				   != 0
			|| !wal_tail_same(&pinned, &current, true))
			return false;
	}
	for (WalTailSegment *s = work->segments; s != NULL; s = s->next)
		if (!wal_tail_segment_current(work, s))
			return false;
	return true;
}

/* A missing/invalid tail page is not EOF if a later segment has actually
 * been initialized at its own address. Filenames alone are insufficient:
 * native preallocation is zero-filled and recycling retains old addresses.
 * Do not count headers traversed while reading an incomplete final record.
 * This scan only refuses known gaps; it never skips a hole or supplies redo.
 * Author: SqlRush <sqlrush@gmail.com> */
static ClusterControlRootResult
wal_tail_successor_pages(WalTailWork *work, XLogSegNo number, off_t start, off_t size)
{
	for (off_t offset = start; offset < size; offset += XLOG_BLCKSZ) {
		XLogLongPageHeaderData header;
		size_t wanted = offset == 0 ? SizeOfXLogLongPHD : SizeOfXLogShortPHD;
		size_t used = 0;
		XLogRecPtr page = number * work->reader->segcxt.ws_segsize + offset;

		while (used < wanted) {
			ssize_t n;
			CHECK_FOR_INTERRUPTS();
			n = pread(work->suffix_fd, (char *)&header + used, wanted - used, offset + used);
			if (n < 0 && errno == EINTR)
				continue;
			if (n < 0)
				return CLUSTER_CONTROL_ROOT_IO_ERROR;
			if (n == 0)
				break;
			used += n;
		}
		if (used != wanted || header.std.xlp_magic != XLOG_PAGE_MAGIC
			|| header.std.xlp_pageaddr != page)
			continue;
		if (header.std.xlp_tli != work->ref.timeline
			|| header.std.xlp_thread_id != work->ref.claim.identity.origin_thread_id
			|| (offset == 0
				&& (header.xlp_sysid != work->ref.claim.identity.system_identifier
					|| header.xlp_seg_size != work->reader->segcxt.ws_segsize
					|| header.xlp_xlog_blcksz != XLOG_BLCKSZ)))
			return CLUSTER_CONTROL_ROOT_IDENTITY_MISMATCH;
		return CLUSTER_CONTROL_ROOT_BAD_RECORD_CRC;
	}
	return CLUSTER_CONTROL_ROOT_OK_PRIMARY;
}

static ClusterControlRootResult
wal_tail_no_written_successor(WalTailWork *work, XLogRecPtr lower)
{
	XLogSegNo last_segment;
	int scan_fd;
	ClusterControlRootResult result = CLUSTER_CONTROL_ROOT_OK_PRIMARY;

	XLByteToSeg(Max(lower, work->last_page_requested), last_segment,
				work->reader->segcxt.ws_segsize);
	scan_fd = openat(work->dirs[2], ".", O_RDONLY | O_DIRECTORY | O_NOFOLLOW | O_CLOEXEC);
	if (scan_fd < 0)
		return CLUSTER_CONTROL_ROOT_IO_ERROR;
	work->suffix_scan = fdopendir(scan_fd);
	if (work->suffix_scan == NULL) {
		(void)close(scan_fd);
		return CLUSTER_CONTROL_ROOT_IO_ERROR;
	}
	for (;;) {
		struct dirent *entry;
		struct stat before, after, named;
		XLogSegNo number;
		TimeLineID timeline;
		char canonical[MAXFNAMELEN];
		off_t start;
		int fd;

		CHECK_FOR_INTERRUPTS();
		errno = 0;
		entry = readdir(work->suffix_scan);
		if (entry == NULL) {
			if (errno != 0)
				result = CLUSTER_CONTROL_ROOT_IO_ERROR;
			break;
		}
		if (!IsXLogFileName(entry->d_name))
			continue;
		XLogFromFileName(entry->d_name, &timeline, &number, work->reader->segcxt.ws_segsize);
		if (timeline != work->ref.timeline || number < last_segment)
			continue;
		XLogFileName(canonical, timeline, number, work->reader->segcxt.ws_segsize);
		if (strcmp(canonical, entry->d_name) != 0
			|| number > PG_UINT64_MAX / work->reader->segcxt.ws_segsize)
			return CLUSTER_CONTROL_ROOT_RANGE_INVALID;
		work->suffix_fd = openat(work->dirs[2], entry->d_name,
								 O_RDONLY | O_NOFOLLOW | O_NONBLOCK | O_CLOEXEC | PG_BINARY);
		if (work->suffix_fd < 0 || fstat(work->suffix_fd, &before) != 0
			|| !wal_tail_owned(&before, false) || before.st_size > work->reader->segcxt.ws_segsize)
			return CLUSTER_CONTROL_ROOT_IO_ERROR;
		start = number == last_segment
					? XLogSegmentOffset(Max(lower, work->last_page_requested),
										work->reader->segcxt.ws_segsize)
						  + XLOG_BLCKSZ
					: 0;
		/* lower can be a record address; begin strictly after its page. */
		start -= start % XLOG_BLCKSZ;
		result = wal_tail_successor_pages(work, number, start, before.st_size);
		if (fstat(work->suffix_fd, &after) != 0
			|| fstatat(work->dirs[2], entry->d_name, &named, AT_SYMLINK_NOFOLLOW) != 0
			|| !wal_tail_same(&before, &after, false) || !wal_tail_same(&before, &named, false))
			return CLUSTER_CONTROL_ROOT_STALE_TOKEN;
		fd = work->suffix_fd;
		work->suffix_fd = -1;
		if (close(fd) != 0)
			return CLUSTER_CONTROL_ROOT_IO_ERROR;
		if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
			return result;
	}
	{
		DIR *scan = work->suffix_scan;
		work->suffix_scan = NULL;
		if (closedir(scan) != 0)
			result = CLUSTER_CONTROL_ROOT_IO_ERROR;
	}
	return result;
}

/* PGRAC: physical validity does not imply a supported shared recovery profile.
 * Apply to both sealed recovery and checkpoint-less startup scans; a current
 * off setting never makes historical optional SIDE effects disappear. */
static ClusterControlRootResult
wal_profile_supported(XLogReaderState *reader)
{
	const uint8 *data = (const uint8 *)XLogRecGetData(reader);
	uint8 info = XLogRecGetInfo(reader) & ~XLR_INFO_MASK;
	if (XLogRecGetRmid(reader) == RM_COMMIT_TS_ID)
		return CLUSTER_CONTROL_ROOT_PROFILE_UNSUPPORTED;
	if (XLogRecGetRmid(reader) != RM_XLOG_ID)
		return CLUSTER_CONTROL_ROOT_OK_PRIMARY;
	if (info == XLOG_PARAMETER_CHANGE) {
		if (XLogRecGetDataLen(reader) != sizeof(xl_parameter_change)
			|| data[offsetof(xl_parameter_change, track_commit_timestamp)] > 1)
			return CLUSTER_CONTROL_ROOT_RANGE_INVALID;
		if (data[offsetof(xl_parameter_change, track_commit_timestamp)] != 0)
			return CLUSTER_CONTROL_ROOT_PROFILE_UNSUPPORTED;
	} else if (info == XLOG_CHECKPOINT_SHUTDOWN || info == XLOG_CHECKPOINT_ONLINE) {
		CheckPoint checkpoint;
		if (XLogRecGetDataLen(reader) != sizeof(checkpoint))
			return CLUSTER_CONTROL_ROOT_RANGE_INVALID;
		memcpy(&checkpoint, data, sizeof(checkpoint));
		if (TransactionIdIsValid(checkpoint.oldestCommitTsXid)
			|| TransactionIdIsValid(checkpoint.newestCommitTsXid))
			return CLUSTER_CONTROL_ROOT_PROFILE_UNSUPPORTED;
	}
	return CLUSTER_CONTROL_ROOT_OK_PRIMARY;
}

/* Fold every complete native parameter obligation, not only the latest one.
 * This is early sizing evidence, not permission to apply the configuration. */
static ClusterControlRootResult
wal_startup_parameters(WalTailWork *work)
{
	XLogReaderState *reader = work->reader;
	xl_parameter_change parameters;
	const uint8 *data;
	ClusterWalStartupObservation *out = &work->startup;

	if (XLogRecGetRmid(reader) != RM_XLOG_ID
		|| (XLogRecGetInfo(reader) & ~XLR_INFO_MASK) != XLOG_PARAMETER_CHANGE)
		return CLUSTER_CONTROL_ROOT_OK_PRIMARY;
	if (XLogRecGetDataLen(reader) != sizeof(parameters) || XLogRecMaxBlockId(reader) >= 0)
		return CLUSTER_CONTROL_ROOT_RANGE_INVALID;
	data = (const uint8 *)XLogRecGetData(reader);
	/* Validate native bool bytes before loading them as C boolean values. */
	if (data[offsetof(xl_parameter_change, wal_log_hints)] > 1
		|| data[offsetof(xl_parameter_change, track_commit_timestamp)] > 1)
		return CLUSTER_CONTROL_ROOT_RANGE_INVALID;
	memcpy(&parameters, data, sizeof(parameters));
	if (parameters.MaxConnections <= 0 || parameters.max_worker_processes < 0
		|| parameters.max_wal_senders < 0 || parameters.max_prepared_xacts < 0
		|| parameters.max_locks_per_xact <= 0 || parameters.wal_level < WAL_LEVEL_MINIMAL
		|| parameters.wal_level > WAL_LEVEL_LOGICAL)
		return CLUSTER_CONTROL_ROOT_RANGE_INVALID;
	out->max_connections = Max(out->max_connections, parameters.MaxConnections);
	out->max_worker_processes = Max(out->max_worker_processes, parameters.max_worker_processes);
	out->max_wal_senders = Max(out->max_wal_senders, parameters.max_wal_senders);
	out->max_prepared_xacts = Max(out->max_prepared_xacts, parameters.max_prepared_xacts);
	out->max_locks_per_xact = Max(out->max_locks_per_xact, parameters.max_locks_per_xact);
	out->parameter_records++;
	return CLUSTER_CONTROL_ROOT_OK_PRIMARY;
}

/* Classify every complete startup record, including the native suffix.
 * A physical observation is not native side-state closure or replay authority.
 * Unknown effects remain visible so a checkpoint-less finalizer cannot mistake
 * a valid WAL stream for an empty initialization. Author: SqlRush <sqlrush@gmail.com>
 */
static ClusterControlRootResult
wal_startup_classify(WalTailWork *work)
{
	XLogReaderState *reader = work->reader;
	ClusterWalStartupObservation *out = &work->startup;
	uint8 info = XLogRecGetInfo(reader) & ~XLR_INFO_MASK;
	const uint8 *data = (const uint8 *)XLogRecGetData(reader);
	bool supported = XLogRecGetXid(reader) == InvalidTransactionId && XLogRecMaxBlockId(reader) < 0
					 && (XLogRecGetInfo(reader) & XLR_INFO_MASK) == 0;
	ClusterControlRootResult result;

	if (XLogRecGetRmid(reader) != RM_XLOG_ID)
		supported = false;
	else if (info == XLOG_PARAMETER_CHANGE) {
		result = wal_startup_parameters(work);
		if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
			return result;
	} else if (info == XLOG_FPW_CHANGE) {
		if (XLogRecGetDataLen(reader) != sizeof(bool) || data[0] > 1
			|| XLogRecMaxBlockId(reader) >= 0)
			return CLUSTER_CONTROL_ROOT_RANGE_INVALID;
		out->fpw_records++;
		out->fpw_disabled |= data[0] == 0;
	} else if (info == XLOG_CHECKPOINT_SHUTDOWN || info == XLOG_CHECKPOINT_ONLINE) {
		CheckPoint checkpoint;
		if (XLogRecGetDataLen(reader) != sizeof(checkpoint) || XLogRecMaxBlockId(reader) >= 0
			|| data[offsetof(CheckPoint, fullPageWrites)] > 1)
			return CLUSTER_CONTROL_ROOT_RANGE_INVALID;
		memcpy(&checkpoint, data, sizeof(checkpoint));
		/* Match the native checkpoint's identity and redo bounds. Promotion
		 * still validates the complete recovery input and its side files. */
		if (checkpoint.ThisTimeLineID != work->ref.timeline)
			return CLUSTER_CONTROL_ROOT_IDENTITY_MISMATCH;
		if (checkpoint.PrevTimeLineID == 0 || checkpoint.PrevTimeLineID > checkpoint.ThisTimeLineID
			|| checkpoint.redo == InvalidXLogRecPtr || checkpoint.redo > reader->ReadRecPtr
			|| (info == XLOG_CHECKPOINT_SHUTDOWN && checkpoint.redo != reader->ReadRecPtr)
			|| !TransactionIdIsNormal(XidFromFullTransactionId(checkpoint.nextXid)))
			return CLUSTER_CONTROL_ROOT_RANGE_INVALID;
		if (out->checkpoint_records == 0) {
			out->checkpoint_start = reader->ReadRecPtr;
			out->checkpoint_end = reader->EndRecPtr;
			out->checkpoint_crc = reader->record->header.xl_crc;
			out->checkpoint_info = info;
			out->checkpoint = checkpoint;
		}
		out->checkpoint_records++;
	} else
		supported = false;
	if (!supported)
		out->unsupported_records++;
	return CLUSTER_CONTROL_ROOT_OK_PRIMARY;
}

static ClusterControlRootResult
wal_tail_scan(WalTailWork *work, int segment_size, XLogRecPtr lower, XLogRecPtr minimum)
{
	const int flags = O_RDONLY | O_DIRECTORY | O_NOFOLLOW | O_CLOEXEC;
	char thread[32], generation[48];
	const char *parts[] = { thread, generation };
	ClusterWalThreadClaimV2 claim;
	XLogRecord *record;
	char *error = NULL;
	bool checkpoint_seen = work->checkpoint_start == 0;
	struct stat st;
	ClusterControlRootResult result;

	work->reader
		= XLogReaderAllocate(segment_size, NULL, XL_ROUTINE(.page_read = wal_tail_page), work);
	if (work->reader == NULL)
		return CLUSTER_CONTROL_ROOT_IO_ERROR;
	work->reader->system_identifier = work->ref.claim.identity.system_identifier;
	work->reader->cluster_expected_thread_id = work->ref.claim.identity.origin_thread_id;
	work->reader->seg.ws_tli = work->ref.timeline;
	snprintf(thread, sizeof(thread), "thread_%u", work->ref.claim.identity.origin_thread_id);
	snprintf(generation, sizeof(generation), "generation_" UINT64_FORMAT,
			 work->ref.claim.identity.origin_owner_incarnation);
	work->dirs[0] = open(work->root, flags);
	if (work->dirs[0] < 0 || fstat(work->dirs[0], &st) != 0 || !wal_tail_owned(&st, true))
		return CLUSTER_CONTROL_ROOT_IO_ERROR;
	for (size_t i = 0; i < lengthof(parts); ++i) {
		work->dirs[i + 1] = openat(work->dirs[i], parts[i], flags);
		if (work->dirs[i + 1] < 0 || fstat(work->dirs[i + 1], &st) != 0
			|| !wal_tail_owned(&st, true))
			return CLUSTER_CONTROL_ROOT_IO_ERROR;
	}
	result = cluster_wal_claim_v2_read(work->root, &work->ref.claim, &claim);
	if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
		return result;
	if (!wal_tail_paths_current(work))
		return CLUSTER_CONTROL_ROOT_STALE_TOKEN;
	XLogBeginRead(work->reader, lower);
	for (;;) {
		work->incomplete_tail = false;
		record = XLogReadRecord(work->reader, &error);
		if (record == NULL)
			break;
		CHECK_FOR_INTERRUPTS();
		/* Native flush may stop at a page boundary within a record, or
		 * before its alignment/switch padding. Only a complete end inside
		 * the qualified cut may enter the provisional visitor. */
		if (work->flush_end != InvalidXLogRecPtr && work->reader->EndRecPtr > work->flush_end) {
			work->flush_boundary = true;
			break;
		}
		result = wal_profile_supported(work->reader);
		if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
			return result;
		if (work->observed.records == 0 && work->reader->ReadRecPtr != lower)
			return CLUSTER_CONTROL_ROOT_RANGE_INVALID;
		if (work->startup_mode) {
			if (work->observed.records == 0 && record->xl_prev != InvalidXLogRecPtr)
				return CLUSTER_CONTROL_ROOT_RANGE_INVALID;
			result = wal_startup_classify(work);
			if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
				return result;
		}
		if (work->checkpoint_start != 0 && work->reader->ReadRecPtr == work->checkpoint_start) {
			if (record->xl_crc != work->checkpoint_crc)
				return CLUSTER_CONTROL_ROOT_BAD_RECORD_CRC;
			checkpoint_seen = true;
		}
		if (!checkpoint_seen && work->reader->ReadRecPtr > work->checkpoint_start)
			return CLUSTER_CONTROL_ROOT_BAD_RECORD_CRC;
		/* A complete suffix outside the sealed owner cut is not a torn tail.
		 * Do not even feed it into the provisional plan. */
		if (work->sealed != NULL
			&& work->reader->EndRecPtr > work->sealed->validated_tail_lsn_exclusive)
			return CLUSTER_CONTROL_ROOT_STALE_TOKEN;
		if (work->checkpoint_prefix && work->reader->EndRecPtr > minimum)
			return CLUSTER_CONTROL_ROOT_RANGE_INVALID;
		if (work->visitor != NULL && !work->visitor(work->reader, work->visitor_arg))
			return CLUSTER_CONTROL_ROOT_RECONFIG_WAIT;
		work->observed.complete_end = work->reader->EndRecPtr;
		work->observed.last_record_start = work->reader->ReadRecPtr;
		work->observed.last_record_crc = record->xl_crc;
		++work->observed.records;
		if (work->flush_end != InvalidXLogRecPtr
			&& work->observed.complete_end == work->flush_end) {
			work->flush_boundary = true;
			break;
		}
		if (work->checkpoint_prefix && work->observed.complete_end == minimum)
			break;
	}
	if (work->result != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
		return work->result;
	if (work->flush_end != InvalidXLogRecPtr && !work->flush_boundary)
		return CLUSTER_CONTROL_ROOT_BAD_RECORD_CRC;
	if (!work->checkpoint_prefix && work->flush_end == InvalidXLogRecPtr
		&& !wal_tail_normal_end(work))
		return CLUSTER_CONTROL_ROOT_BAD_RECORD_CRC;
	if (!checkpoint_seen
		|| (!(work->startup_mode && work->observed.records == 0)
			&& work->observed.complete_end < minimum))
		return CLUSTER_CONTROL_ROOT_BAD_RECORD_CRC;
	if (work->checkpoint_prefix
		&& (work->observed.complete_end != minimum
			|| work->observed.last_record_start != work->checkpoint_start
			|| work->observed.last_record_crc != work->checkpoint_crc))
		return CLUSTER_CONTROL_ROOT_BAD_RECORD_CRC;
	if (work->sealed != NULL
		&& (work->observed.complete_end != work->sealed->validated_tail_lsn_exclusive
			|| work->observed.last_record_start != work->sealed->tail_last_record_lsn
			|| work->observed.last_record_crc != work->sealed->tail_last_record_crc32c))
		return CLUSTER_CONTROL_ROOT_BAD_RECORD_CRC;
	if (!wal_tail_close_segment(work))
		return work->result;
	if (!work->checkpoint_prefix && work->flush_end == InvalidXLogRecPtr) {
		result = wal_tail_no_written_successor(work, lower);
		if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
			return result;
	}
	if (!wal_tail_paths_current(work))
		return CLUSTER_CONTROL_ROOT_STALE_TOKEN;
	if (work->sync_inputs) {
		result = wal_startup_sync_inputs(work);
		if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
			return result;
	}
	result = cluster_wal_claim_v2_read(work->root, &work->ref.claim, &claim);
	if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
		return result;
	if (!wal_tail_paths_current(work))
		return CLUSTER_CONTROL_ROOT_STALE_TOKEN;
	return CLUSTER_CONTROL_ROOT_OK_PRIMARY;
}

static ClusterControlRootResult
wal_startup_sync_file(WalTailWork *work, int dir, const char *name, const struct stat *expected,
					  off_t size)
{
	struct stat before, after, named;
	CHECK_FOR_INTERRUPTS();
	work->segment_fd
		= openat(dir, name, O_RDONLY | O_NOFOLLOW | O_NONBLOCK | O_CLOEXEC | PG_BINARY);
	if (work->segment_fd < 0 || fstat(work->segment_fd, &before) != 0
		|| !wal_tail_owned(&before, false) || (size >= 0 && before.st_size != size))
		return CLUSTER_CONTROL_ROOT_IO_ERROR;
	if ((expected != NULL && !wal_tail_same(expected, &before, false))
		|| fstatat(dir, name, &named, AT_SYMLINK_NOFOLLOW) != 0
		|| !wal_tail_same(&before, &named, false))
		return CLUSTER_CONTROL_ROOT_STALE_TOKEN;
	if (pg_fsync(work->segment_fd) != 0)
		return CLUSTER_CONTROL_ROOT_IO_ERROR;
	if (fstat(work->segment_fd, &after) != 0 || fstatat(dir, name, &named, AT_SYMLINK_NOFOLLOW) != 0
		|| !wal_tail_same(&before, &after, false) || !wal_tail_same(&before, &named, false))
		return CLUSTER_CONTROL_ROOT_STALE_TOKEN;
	{
		int fd = work->segment_fd;
		work->segment_fd = -1;
		if (close(fd) != 0)
			return CLUSTER_CONTROL_ROOT_IO_ERROR;
	}
	return CLUSTER_CONTROL_ROOT_OK_PRIMARY;
}

static ClusterControlRootResult
wal_startup_sync_inputs(WalTailWork *work)
{
	ClusterControlRootResult result;
	if (!enableFsync || !work->startup_mode || !wal_tail_paths_current(work))
		return CLUSTER_CONTROL_ROOT_STALE_TOKEN;
	for (WalTailSegment *s = work->segments; s != NULL; s = s->next) {
		char filename[MAXFNAMELEN];
		XLogFileName(filename, work->ref.timeline, s->number, work->reader->segcxt.ws_segsize);
		result = wal_startup_sync_file(work, work->dirs[2], filename, &s->identity, -1);
		if (result != 0)
			return result;
	}
	result = wal_startup_sync_file(work, work->dirs[2], CLUSTER_WAL_THREAD_CLAIM_FILENAME, NULL,
								   CLUSTER_WAL_CLAIM_V2_BYTES);
	if (result != 0)
		return result;
	for (int i = lengthof(work->dirs) - 1; i >= 0; --i) {
		CHECK_FOR_INTERRUPTS();
		if (pg_fsync(work->dirs[i]) != 0)
			return CLUSTER_CONTROL_ROOT_IO_ERROR;
	}
	return wal_tail_paths_current(work) ? CLUSTER_CONTROL_ROOT_OK_PRIMARY
										: CLUSTER_CONTROL_ROOT_STALE_TOKEN;
}

static ClusterControlRootResult
wal_tail_release(WalTailWork *work, ClusterControlRootResult result)
{
	if (work->suffix_fd >= 0 && close(work->suffix_fd) != 0)
		result = CLUSTER_CONTROL_ROOT_IO_ERROR;
	work->suffix_fd = -1;
	if (work->suffix_scan != NULL && closedir(work->suffix_scan) != 0)
		result = CLUSTER_CONTROL_ROOT_IO_ERROR;
	work->suffix_scan = NULL;
	if (work->segment_fd >= 0 && close(work->segment_fd) != 0)
		result = CLUSTER_CONTROL_ROOT_IO_ERROR;
	work->segment_fd = -1;
	for (size_t i = 0; i < lengthof(work->dirs); ++i) {
		if (work->dirs[i] >= 0 && close(work->dirs[i]) != 0)
			result = CLUSTER_CONTROL_ROOT_IO_ERROR;
		work->dirs[i] = -1;
	}
	if (work->reader != NULL) {
		XLogReaderFree(work->reader);
		work->reader = NULL;
	}
	while (work->segments != NULL) {
		WalTailSegment *segment = work->segments;
		work->segments = segment->next;
		pfree(segment);
	}
	return result;
}

static ClusterControlRootResult
wal_tail_observe_common(const char *wal_root, const ClusterWalSourceRef *ref, int segment_size,
						XLogRecPtr scan_lower, XLogRecPtr minimum_end, XLogRecPtr checkpoint_start,
						pg_crc32c checkpoint_crc, ClusterWalTailObservation *out,
						ClusterWalStartupObservation *startup, bool sync_inputs,
						bool checkpoint_prefix, const ClusterControlRootSnapshot *sealed,
						ClusterWalRecordVisitor visitor, void *arg, XLogRecPtr flush_end)
{
	WalTailWork *work;
	ClusterControlRootResult result;
	if (out == NULL)
		return CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT;
	memset(out, 0, sizeof(*out));
	if (wal_root == NULL || wal_root[0] == '\0' || !IsValidWalSegSize(segment_size)
		|| scan_lower == 0 || minimum_end < scan_lower
		|| (startup == NULL && minimum_end == scan_lower)
		|| (flush_end != InvalidXLogRecPtr && flush_end < minimum_end)
		|| (checkpoint_start != 0
			&& (checkpoint_start < scan_lower || checkpoint_start >= minimum_end))
		|| ref == NULL || ref->timeline == 0 || !cluster_wal_claim_v2_ref_valid(&ref->claim))
		return CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT;
	work = palloc0(sizeof(*work));
	work->root = wal_root;
	work->ref = *ref;
	work->startup_mode = startup != NULL;
	work->sync_inputs = sync_inputs;
	work->checkpoint_prefix = checkpoint_prefix;
	work->flush_end = flush_end;
	work->sealed = sealed;
	work->visitor = visitor;
	work->visitor_arg = arg;
	work->checkpoint_start = checkpoint_start;
	work->checkpoint_crc = checkpoint_crc;
	work->segment_fd = -1;
	work->suffix_fd = -1;
	for (size_t i = 0; i < lengthof(work->dirs); ++i)
		work->dirs[i] = -1;
	PG_TRY();
	{
		result = wal_tail_scan(work, segment_size, scan_lower, minimum_end);
	}
	PG_CATCH();
	{
		(void)wal_tail_release(work, CLUSTER_CONTROL_ROOT_IO_ERROR);
		pfree(work);
		PG_RE_THROW();
	}
	PG_END_TRY();
	result = wal_tail_release(work, result);
	if (result == CLUSTER_CONTROL_ROOT_OK_PRIMARY) {
		work->observed.database_incarnation = work->ref.claim.database_incarnation;
		*out = work->observed;
		if (startup != NULL) {
			*startup = work->startup;
			startup->tail = work->observed;
		}
	}
	pfree(work);
	return result;
}

ClusterControlRootResult
cluster_wal_tail_observe(const char *wal_root, const ClusterWalSourceRef *ref,
						 int segment_size, XLogRecPtr scan_lower, XLogRecPtr minimum_end,
						 ClusterWalTailObservation *out)
{
	return wal_tail_observe_common(wal_root, ref, segment_size, scan_lower, minimum_end, 0, 0, out,
								   NULL, false, false, NULL, NULL, NULL, InvalidXLogRecPtr);
}

ClusterControlRootResult
cluster_wal_startup_observe(const char *wal_root, const ClusterWalSourceRef *ref,
							int segment_size, XLogRecPtr first_segment,
							ClusterWalStartupObservation *out)
{
	ClusterWalTailObservation tail;
	XLogRecPtr lower;
	if (out != NULL)
		memset(out, 0, sizeof(*out));
	if (out == NULL || !IsValidWalSegSize(segment_size) || first_segment == 0
		|| first_segment % segment_size != 0 || first_segment > UINT64_MAX - SizeOfXLogLongPHD)
		return CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT;
	lower = first_segment + SizeOfXLogLongPHD;
	return wal_tail_observe_common(wal_root, ref, segment_size, lower, lower, 0, 0, &tail, out,
								   false, false, NULL, NULL, NULL, InvalidXLogRecPtr);
}

ClusterControlRootResult
cluster_wal_startup_sync(const char *wal_root, const ClusterWalSourceRef *ref,
						 int segment_size, XLogRecPtr first_segment,
						 ClusterWalStartupObservation *out)
{
	ClusterWalTailObservation tail;
	XLogRecPtr lower;
	if (out != NULL)
		memset(out, 0, sizeof(*out));
	if (out == NULL || !enableFsync || !IsValidWalSegSize(segment_size) || first_segment == 0
		|| first_segment % segment_size != 0 || first_segment > UINT64_MAX - SizeOfXLogLongPHD)
		return CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT;
	lower = first_segment + SizeOfXLogLongPHD;
	return wal_tail_observe_common(wal_root, ref, segment_size, lower, lower, 0, 0, &tail, out,
								   true, false, NULL, NULL, NULL, InvalidXLogRecPtr);
}

/* PGRAC: bind the root-selected checkpoint and the actual native tail.
 * Author: SqlRush <sqlrush@gmail.com> */
ClusterControlRootResult
cluster_wal_tail_observe_checkpoint(const char *wal_root, const ClusterWalSourceRef *ref,
									int segment_size, XLogRecPtr scan_lower, XLogRecPtr minimum_end,
									XLogRecPtr checkpoint_start, pg_crc32c checkpoint_crc,
									ClusterWalTailObservation *out)
{
	if (checkpoint_start == 0) {
		if (out != NULL)
			memset(out, 0, sizeof(*out));
		return CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT;
	}
	return wal_tail_observe_common(wal_root, ref, segment_size, scan_lower, minimum_end,
								   checkpoint_start, checkpoint_crc, out, NULL, false, false, NULL,
								   NULL, NULL, InvalidXLogRecPtr);
}

ClusterControlRootResult
cluster_wal_tail_visit_sealed(const char *wal_root, const ClusterWalSourceRef *ref,
							  int segment_size, const ClusterControlRootSnapshot *sealed,
							  XLogRecPtr checkpoint_start, ClusterWalRecordVisitor visitor,
							  void *arg, ClusterWalTailObservation *out)
{
	ClusterControlRootSnapshot expected;
	const uint32 flags
		= CLUSTER_CONTROL_ROOT_FLAG_CLAIM_VALID | CLUSTER_CONTROL_ROOT_FLAG_CHECKPOINT_VALID
		  | CLUSTER_CONTROL_ROOT_FLAG_TAIL_VALID | CLUSTER_CONTROL_ROOT_FLAG_TAIL_LAST_RECORD_VALID;
	if (out == NULL)
		return CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT;
	if (sealed != NULL)
		expected = *sealed;
	memset(out, 0, sizeof(*out));
	if (sealed == NULL || ref == NULL || checkpoint_start == 0
		|| expected.lifecycle != CLUSTER_CONTROL_ROOT_LIFECYCLE_RECOVERY_REQUIRED
		|| (expected.root_flags & flags) != flags
		|| memcmp(&expected.identity, &ref->claim.identity, sizeof(expected.identity)) != 0
		|| expected.checkpoint_tli != ref->timeline || expected.tail_tli != ref->timeline
		|| expected.tail_validation_kind != CLUSTER_CONTROL_ROOT_TAIL_WAL_RECORD_SCAN_V1
		|| expected.tail_last_record_lsn < expected.checkpoint_lower_lsn
		|| expected.tail_last_record_lsn >= expected.validated_tail_lsn_exclusive)
		return CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT;
	return wal_tail_observe_common(wal_root, ref, segment_size, expected.checkpoint_lower_lsn,
								   expected.validated_tail_lsn_exclusive, checkpoint_start,
								   expected.checkpoint_record_crc32c, out, NULL, false, false,
								   &expected, visitor, arg, InvalidXLogRecPtr);
}

ClusterControlRootResult
cluster_wal_checkpoint_prefix_observe(const char *wal_root, const ClusterWalSourceRef *ref,
									  int segment_size, XLogRecPtr physical_lower,
									  XLogRecPtr checkpoint_end, XLogRecPtr checkpoint_start,
									  pg_crc32c checkpoint_crc, ClusterWalTailObservation *out)
{
	if (checkpoint_start == InvalidXLogRecPtr) {
		if (out != NULL)
			memset(out, 0, sizeof(*out));
		return CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT;
	}
	return wal_tail_observe_common(wal_root, ref, segment_size, physical_lower, checkpoint_end,
								   checkpoint_start, checkpoint_crc, out, NULL, false, true, NULL,
								   NULL, NULL, InvalidXLogRecPtr);
}

ClusterControlRootResult
cluster_wal_flushed_prefix_visit(const char *wal_root, const ClusterWalSourceRef *ref,
								 int segment_size, XLogRecPtr physical_lower,
								 XLogRecPtr minimum_end, XLogRecPtr flushed_end,
								 XLogRecPtr checkpoint_start, pg_crc32c checkpoint_crc,
								 ClusterWalRecordVisitor visitor, void *arg,
								 ClusterWalTailObservation *out)
{
	if (checkpoint_start == InvalidXLogRecPtr || flushed_end == InvalidXLogRecPtr) {
		if (out != NULL)
			memset(out, 0, sizeof(*out));
		return CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT;
	}
	return wal_tail_observe_common(wal_root, ref, segment_size, physical_lower, minimum_end,
								   checkpoint_start, checkpoint_crc, out, NULL, false, false, NULL,
								   visitor, arg, flushed_end);
}
