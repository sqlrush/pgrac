/* PGRAC: verify actual WAL against the exact writer's durable promise.
 * Author: SqlRush <sqlrush@gmail.com>
 */
#include "postgres.h"
#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>
#include "access/xlog.h"
#include "access/xlog_internal.h"
#include "access/xlogreader.h"
#include "catalog/pg_control.h"
#include "cluster/cluster_wal_tail.h"
#include "miscadmin.h"

typedef struct WalTailSegment {
	struct WalTailSegment *next;
	XLogSegNo number;
	struct stat identity;
} WalTailSegment;

typedef struct WalTailWork {
	const char *root;
	ClusterWalDurablePrefixRef ref;
	int dirs[3];
	int segment_fd;
	WalTailSegment *segments;
	WalTailSegment *current;
	XLogReaderState *reader;
	ClusterControlRootResult result;
	ClusterWalTailObservation observed;
	XLogRecPtr checkpoint_start;
	pg_crc32c checkpoint_crc;
	bool startup_mode;
	ClusterWalStartupObservation startup;
} WalTailWork;

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
 * A missing/short suffix is only interpreted after the promise is matched;
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
	CHECK_FOR_INTERRUPTS();
	if (required < 0 || required > XLOG_BLCKSZ || pageptr % XLOG_BLCKSZ != 0) {
		work->result = CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT;
		return XLREAD_FAIL;
	}
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
	while (used < XLOG_BLCKSZ) {
		ssize_t count = pread(work->segment_fd, page + used, XLOG_BLCKSZ - used,
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
	if (used < (size_t)required)
		return XLREAD_FAIL;
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
	return (int)used;
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
	return CLUSTER_CONTROL_ROOT_OK_PRIMARY;
}

static ClusterControlRootResult
wal_tail_scan(WalTailWork *work, int segment_size, XLogRecPtr lower, XLogRecPtr minimum)
{
	const int flags = O_RDONLY | O_DIRECTORY | O_NOFOLLOW | O_CLOEXEC;
	char thread[32], generation[48];
	const char *parts[] = { thread, generation };
	ClusterWalThreadClaimV2 claim;
	ClusterWalDurablePrefix after;
	XLogRecord *record;
	char *error = NULL;
	bool promise_seen = false;
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
	result
		= cluster_wal_durable_prefix_read(work->root, &work->ref, &work->observed.durable_prefix);
	if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
		return result;
	if (work->startup_mode && work->observed.durable_prefix.exclusive_end == 0)
		promise_seen = true; /* Real EMPTY, never synthesized on read failure. */
	else if (work->observed.durable_prefix.record_start < lower
			 || work->observed.durable_prefix.exclusive_end <= lower)
		return CLUSTER_CONTROL_ROOT_RANGE_INVALID;
	if (!wal_tail_paths_current(work))
		return CLUSTER_CONTROL_ROOT_STALE_TOKEN;
	XLogBeginRead(work->reader, lower);
	while ((record = XLogReadRecord(work->reader, &error)) != NULL) {
		CHECK_FOR_INTERRUPTS();
		if (work->observed.records == 0 && work->reader->ReadRecPtr != lower)
			return CLUSTER_CONTROL_ROOT_RANGE_INVALID;
		if (work->startup_mode) {
			if (work->observed.records == 0 && record->xl_prev != InvalidXLogRecPtr)
				return CLUSTER_CONTROL_ROOT_RANGE_INVALID;
			result = wal_startup_parameters(work);
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
		if (work->reader->ReadRecPtr == work->observed.durable_prefix.record_start) {
			if (work->reader->EndRecPtr != work->observed.durable_prefix.exclusive_end
				|| record->xl_crc != work->observed.durable_prefix.record_crc)
				return CLUSTER_CONTROL_ROOT_BAD_RECORD_CRC;
			promise_seen = true;
		}
		if (!promise_seen && work->reader->ReadRecPtr > work->observed.durable_prefix.record_start)
			return CLUSTER_CONTROL_ROOT_BAD_RECORD_CRC;
		work->observed.complete_end = work->reader->EndRecPtr;
		work->observed.last_record_start = work->reader->ReadRecPtr;
		work->observed.last_record_crc = record->xl_crc;
		++work->observed.records;
	}
	if (work->result != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
		return work->result;
	if (!promise_seen || !checkpoint_seen
		|| (!(work->startup_mode && work->observed.records == 0)
			&& work->observed.complete_end < minimum))
		return CLUSTER_CONTROL_ROOT_BAD_RECORD_CRC;
	if (!wal_tail_close_segment(work))
		return work->result;
	if (!wal_tail_paths_current(work))
		return CLUSTER_CONTROL_ROOT_STALE_TOKEN;
	result = cluster_wal_claim_v2_read(work->root, &work->ref.claim, &claim);
	if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
		return result;
	result = cluster_wal_durable_prefix_read(work->root, &work->ref, &after);
	if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
		return result;
	if (after.sequence != work->observed.durable_prefix.sequence
		|| after.exclusive_end != work->observed.durable_prefix.exclusive_end
		|| after.record_start != work->observed.durable_prefix.record_start
		|| after.record_crc != work->observed.durable_prefix.record_crc
		|| !wal_tail_paths_current(work))
		return CLUSTER_CONTROL_ROOT_STALE_TOKEN;
	return CLUSTER_CONTROL_ROOT_OK_PRIMARY;
}

static ClusterControlRootResult
wal_tail_release(WalTailWork *work, ClusterControlRootResult result)
{
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
wal_tail_observe_common(const char *wal_root, const ClusterWalDurablePrefixRef *ref,
						int segment_size, XLogRecPtr scan_lower, XLogRecPtr minimum_end,
						XLogRecPtr checkpoint_start, pg_crc32c checkpoint_crc,
						ClusterWalTailObservation *out, ClusterWalStartupObservation *startup)
{
	WalTailWork *work;
	ClusterControlRootResult result;
	ClusterWalDurablePrefix empty = { 1, 0, 0, 0 };
	uint8 validation[CLUSTER_WAL_DURABLE_PREFIX_BYTES];
	if (out == NULL)
		return CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT;
	memset(out, 0, sizeof(*out));
	if (wal_root == NULL || wal_root[0] == '\0' || !IsValidWalSegSize(segment_size)
		|| scan_lower == 0 || minimum_end < scan_lower
		|| (startup == NULL && minimum_end == scan_lower)
		|| (checkpoint_start != 0
			&& (checkpoint_start < scan_lower || checkpoint_start >= minimum_end))
		|| cluster_wal_durable_prefix_encode(ref, &empty, validation)
			   != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
		return CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT;
	work = palloc0(sizeof(*work));
	work->root = wal_root;
	work->ref = *ref;
	work->startup_mode = startup != NULL;
	work->checkpoint_start = checkpoint_start;
	work->checkpoint_crc = checkpoint_crc;
	work->segment_fd = -1;
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
cluster_wal_tail_observe(const char *wal_root, const ClusterWalDurablePrefixRef *ref,
						 int segment_size, XLogRecPtr scan_lower, XLogRecPtr minimum_end,
						 ClusterWalTailObservation *out)
{
	return wal_tail_observe_common(wal_root, ref, segment_size, scan_lower, minimum_end, 0, 0, out,
								   NULL);
}

ClusterControlRootResult
cluster_wal_startup_observe(const char *wal_root, const ClusterWalDurablePrefixRef *ref,
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
	return wal_tail_observe_common(wal_root, ref, segment_size, lower, lower, 0, 0, &tail, out);
}

/* PGRAC: bind the root-selected checkpoint, not only the final PGWP record.
 * Author: SqlRush <sqlrush@gmail.com> */
ClusterControlRootResult
cluster_wal_tail_observe_checkpoint(const char *wal_root, const ClusterWalDurablePrefixRef *ref,
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
								   checkpoint_start, checkpoint_crc, out, NULL);
}
