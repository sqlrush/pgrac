/*-------------------------------------------------------------------------
 * PGRAC: native group-flush durable WAL promise.
 * Author: SqlRush <sqlrush@gmail.com>
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include "access/xlog.h"
#include "access/xlog_internal.h"
#include "access/xlogreader.h"
#include "cluster/cluster_epoch.h"
#include "cluster/cluster_external_fence.h"
#include "cluster/cluster_guc.h"
#include "cluster/cluster_membership.h"
#include "cluster/cluster_qvotec.h"
#include "cluster/cluster_reconfig.h"
#include "cluster/cluster_wal_durable_prefix.h"
#include "cluster/cluster_wal_thread.h"
#include "cluster/cluster_write_fence.h"
#include "common/cryptohash.h"
#include "miscadmin.h"
#include "storage/fd.h"
#include "storage/lwlock.h"
#include "utils/memutils.h"
#include "utils/resowner.h"

typedef struct PublishSegment {
	struct PublishSegment *next;
	XLogSegNo number;
	int fd;
} PublishSegment;

typedef struct PublishWork {
	ClusterWalDurablePrefixRef ref;
	uint64 epoch;
	int dirs[4];
	int datafd, routefd, claimfd, currentfd, tempfd;
	char thread[32], generation[48], temporary[80];
	bool temp_owned, boundary;
	uint8 claim_bytes[CLUSTER_WAL_CLAIM_V2_BYTES];
	uint8 previous_bytes[CLUSTER_WAL_DURABLE_PREFIX_BYTES];
	ClusterWalDurablePrefix previous, next;
	PublishSegment *segments;
	XLogReaderState *reader;
	XLogRecPtr upper;
	ClusterControlRootResult read_result;
} PublishWork;

static MemoryContext publish_context;
static ResourceOwner publish_owner;
static pg_cryptohash_ctx *publish_hash;

void
cluster_wal_durable_publish_init(void)
{
	Assert(CritSectionCount == 0);
	if (publish_context == NULL) {
		ResourceOwner saved_owner = CurrentResourceOwner;
		MemoryContext saved_context = CurrentMemoryContext;

		publish_context = AllocSetContextCreate(TopMemoryContext, "WAL durable promise",
												ALLOCSET_DEFAULT_SIZES);
		MemoryContextAllowInCriticalSection(publish_context, true);
		PG_TRY();
		{
			publish_owner = ResourceOwnerCreate(NULL, "WAL durable promise");
			CurrentResourceOwner = publish_owner;
			/* Native OpenSSL uses TopMemoryContext, not CurrentMemoryContext.
			 * The fallback uses CurrentMemoryContext; keep both process-lived,
			 * not in a query/postmaster context that can be reset after fork.
			 * Allocate once here, never create/free a hash inside XLogWrite. */
			MemoryContextSwitchTo(TopMemoryContext);
			publish_hash = pg_cryptohash_create(PG_SHA256);
			if (publish_hash == NULL)
				ereport(ERROR, (errmsg("could not initialize WAL promise hash")));
			CurrentResourceOwner = saved_owner;
			MemoryContextSwitchTo(saved_context);
		}
		PG_CATCH();
		{
			CurrentResourceOwner = saved_owner;
			MemoryContextSwitchTo(saved_context);
			if (publish_owner != NULL) {
				ResourceOwnerRelease(publish_owner, RESOURCE_RELEASE_BEFORE_LOCKS, false, false);
				ResourceOwnerDelete(publish_owner);
			}
			publish_owner = NULL;
			publish_hash = NULL;
			MemoryContextDelete(publish_context);
			publish_context = NULL;
			PG_RE_THROW();
		}
		PG_END_TRY();
	}
}

static bool
publish_runtime(const PublishWork *work)
{
	ClusterWriteFenceObservation fence;
	ClusterWalDurablePrefixRef fresh;
	const ClusterControlRootIdentity *id = &work->ref.claim.identity;

	if (!cluster_enabled || !cluster_shared_config || !enableFsync || CritSectionCount == 0
		|| !LWLockHeldByMeInMode(WALWriteLock, LW_EXCLUSIVE)
		|| !cluster_external_fence_runtime_active() || cluster_node_id != id->origin_node_id
		|| id->system_identifier != GetSystemIdentifier()
		|| cluster_qvotec_get_self_incarnation() != id->origin_owner_incarnation
		|| cluster_membership_get_state(cluster_node_id) != CLUSTER_MEMBER_MEMBER
		|| cluster_membership_get_last_admitted_incarnation(cluster_node_id)
			   != id->origin_owner_incarnation
		|| cluster_epoch_get_current() != work->epoch || work->epoch == 0
		|| cluster_reconfig_has_pending_prebump_stage()
		|| !cluster_wal_thread_current_v2_ref(&fresh)
		|| memcmp(&fresh, &work->ref, sizeof(fresh)) != 0)
		return false;
	cluster_write_fence_observe(&fence);
	return fence.enforcing && fence.attached && fence.engaged && fence.allowed && !fence.self_fenced
		   && fence.epoch_current == work->epoch && fence.authorized_epoch == work->epoch;
}

static bool
publish_owned(const struct stat *st, bool directory)
{
	return (directory ? S_ISDIR(st->st_mode) : S_ISREG(st->st_mode) && st->st_nlink == 1)
		   && st->st_uid == geteuid() && (st->st_mode & (S_IWGRP | S_IWOTH)) == 0;
}

static bool
publish_stat(int fd, bool directory, size_t size)
{
	struct stat st;
	return fd >= 0 && fstat(fd, &st) == 0 && publish_owned(&st, directory)
		   && (directory || st.st_size == size);
}

static bool
publish_exact(int fd, void *bytes, size_t size)
{
	size_t used = 0;
	if (!publish_stat(fd, false, size))
		return false;
	while (used < size) {
		ssize_t n = pread(fd, (char *)bytes + used, size - used, used);
		if (n < 0 && errno == EINTR)
			continue;
		if (n <= 0)
			return false;
		used += n;
	}
	return publish_stat(fd, false, size);
}

static bool
publish_entry_matches(int parent, const char *name, int fd, bool directory, size_t size)
{
	struct stat current, pinned;
	return fd >= 0 && fstat(fd, &pinned) == 0
		   && (parent < 0 ? lstat(name, &current)
						  : fstatat(parent, name, &current, AT_SYMLINK_NOFOLLOW))
				  == 0
		   && publish_owned(&current, directory) && publish_owned(&pinned, directory)
		   && pinned.st_dev == current.st_dev && pinned.st_ino == current.st_ino
		   && (directory || (pinned.st_size == size && current.st_size == size));
}

/* pg_wal is intentionally a symlink, unlike each authority directory. Pin its
 * actual target and re-open that same route at each publication boundary. */
static bool
publish_paths(const PublishWork *work)
{
	const char *parts[] = { work->thread, work->generation, "durable_prefix" };
	struct stat st, target;
	uint8 bytes[CLUSTER_WAL_CLAIM_V2_BYTES];
	int route;
	bool match;

	for (size_t i = 0; i < lengthof(work->dirs); ++i)
		if (!publish_entry_matches(i ? work->dirs[i - 1] : -1,
								   i ? parts[i - 1] : cluster_wal_threads_dir, work->dirs[i], true,
								   0))
			return false;
	if (!publish_entry_matches(-1, DataDir, work->datafd, true, 0)
		|| fstatat(work->datafd, "pg_wal", &st, AT_SYMLINK_NOFOLLOW) != 0 || !S_ISLNK(st.st_mode)
		|| st.st_uid != geteuid())
		return false;
	route = openat(work->datafd, "pg_wal", O_RDONLY | O_DIRECTORY | O_CLOEXEC);
	if (route < 0)
		return false;
	match = fstat(route, &st) == 0 && fstat(work->dirs[2], &target) == 0 && publish_owned(&st, true)
			&& st.st_dev == target.st_dev && st.st_ino == target.st_ino;
	if (close(route) != 0)
		match = false;
	if (!match
		|| !publish_entry_matches(work->dirs[2], "pgrac_thread.claim", work->claimfd, false,
								  sizeof(bytes))
		|| !publish_exact(work->claimfd, bytes, sizeof(bytes))
		|| memcmp(bytes, work->claim_bytes, sizeof(bytes)) != 0)
		return false;
	for (PublishSegment *seg = work->segments; seg != NULL; seg = seg->next) {
		char name[MAXFNAMELEN];
		XLogFileName(name, work->ref.timeline, seg->number, wal_segment_size);
		if (!publish_entry_matches(work->dirs[2], name, seg->fd, false, wal_segment_size))
			return false;
	}
	return true;
}

static ClusterControlRootResult
publish_open(PublishWork *work)
{
	const int dirflags = O_RDONLY | O_DIRECTORY | O_NOFOLLOW | O_CLOEXEC;
	const int fileflags = O_RDONLY | O_NOFOLLOW | O_CLOEXEC | O_NONBLOCK | PG_BINARY;
	const char *parts[] = { work->thread, work->generation, "durable_prefix" };
	uint8 hash[32];
	ClusterControlRootResult result;

	snprintf(work->thread, sizeof(work->thread), "thread_%u",
			 work->ref.claim.identity.origin_thread_id);
	snprintf(work->generation, sizeof(work->generation), "generation_" UINT64_FORMAT,
			 work->ref.claim.identity.origin_owner_incarnation);
	work->dirs[0] = open(cluster_wal_threads_dir, dirflags);
	if (!publish_stat(work->dirs[0], true, 0))
		return CLUSTER_CONTROL_ROOT_IO_ERROR;
	for (size_t i = 0; i < lengthof(parts); ++i) {
		work->dirs[i + 1] = openat(work->dirs[i], parts[i], dirflags);
		if (!publish_stat(work->dirs[i + 1], true, 0))
			return CLUSTER_CONTROL_ROOT_IO_ERROR;
	}
	work->datafd = open(DataDir, dirflags);
	if (!publish_stat(work->datafd, true, 0))
		return CLUSTER_CONTROL_ROOT_IO_ERROR;
	work->claimfd = openat(work->dirs[2], "pgrac_thread.claim", fileflags);
	if (!publish_exact(work->claimfd, work->claim_bytes, sizeof(work->claim_bytes)))
		return CLUSTER_CONTROL_ROOT_IO_ERROR;
	/* Bootstrap checked this immutable claim's full semantics against the
	 * root-selected identity. Recheck those exact bytes by its independent SHA,
	 * without allocating a native crypto owner/context in this critical section. */
	if (pg_cryptohash_init(publish_hash) < 0
		|| pg_cryptohash_update(publish_hash, work->claim_bytes, sizeof(work->claim_bytes)) < 0
		|| pg_cryptohash_final(publish_hash, hash, sizeof(hash)) < 0)
		return CLUSTER_CONTROL_ROOT_IO_ERROR;
	if (memcmp(hash, work->ref.claim.claim_sha256, sizeof(hash)) != 0)
		return CLUSTER_CONTROL_ROOT_HASH_MISMATCH;
	/* Native pg_fsync requires a writable regular descriptor even on systems
	 * where a bare fsync happens to accept read-only ones. */
	work->currentfd = openat(work->dirs[3], "current",
							 O_RDWR | O_NOFOLLOW | O_CLOEXEC | O_NONBLOCK | PG_BINARY);
	if (!publish_exact(work->currentfd, work->previous_bytes, sizeof(work->previous_bytes)))
		return CLUSTER_CONTROL_ROOT_IO_ERROR;
	result = cluster_wal_durable_prefix_decode(work->previous_bytes, sizeof(work->previous_bytes),
											   &work->ref, &work->previous);
	if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
		return result;
	return publish_paths(work) ? CLUSTER_CONTROL_ROOT_OK_PRIMARY : CLUSTER_CONTROL_ROOT_STALE_TOKEN;
}

static int
publish_read_page(XLogReaderState *reader, XLogRecPtr pageptr, int required,
				  XLogRecPtr recordptr pg_attribute_unused(), char *page)
{
	PublishWork *work = reader->private_data;
	PublishSegment *segment;
	XLogSegNo segno;
	size_t available, used = 0;
	XLogPageHeader header;

	if (required < 0 || required > XLOG_BLCKSZ || pageptr % XLOG_BLCKSZ != 0)
		goto invalid;
	if (pageptr >= work->upper || work->upper - pageptr < required) {
		work->boundary = true;
		return XLREAD_FAIL;
	}
	available = Min(work->upper - pageptr, XLOG_BLCKSZ);
	XLByteToSeg(pageptr, segno, wal_segment_size);
	for (segment = work->segments; segment != NULL; segment = segment->next)
		if (segment->number == segno)
			break;
	if (segment == NULL) {
		char name[MAXFNAMELEN];
		segment = palloc0(sizeof(*segment));
		segment->number = segno;
		segment->next = work->segments;
		work->segments = segment;
		XLogFileName(name, work->ref.timeline, segno, wal_segment_size);
		segment->fd
			= openat(work->dirs[2], name, O_RDWR | O_NOFOLLOW | O_CLOEXEC | O_NONBLOCK | PG_BINARY);
	}
	if (!publish_stat(segment->fd, false, wal_segment_size))
		goto invalid;
	while (used < available) {
		ssize_t n = pread(segment->fd, page + used, available - used,
						  XLogSegmentOffset(pageptr, wal_segment_size) + used);
		if (n < 0 && errno == EINTR)
			continue;
		if (n <= 0)
			goto invalid;
		used += n;
	}
	header = (XLogPageHeader)page;
	if (header->xlp_thread_id != work->ref.claim.identity.origin_thread_id
		|| header->xlp_tli != work->ref.timeline) {
		work->read_result = CLUSTER_CONTROL_ROOT_IDENTITY_MISMATCH;
		return XLREAD_FAIL;
	}
	return available;
invalid:
	work->read_result = CLUSTER_CONTROL_ROOT_IO_ERROR;
	return XLREAD_FAIL;
}

static ClusterControlRootResult
publish_scan(PublishWork *work, XLogRecPtr physical_flush)
{
	XLogRecord *record;
	char *error = NULL;

	work->upper = Max(physical_flush, work->previous.exclusive_end);
	work->read_result = CLUSTER_CONTROL_ROOT_OK_PRIMARY;
	work->reader = XLogReaderAllocate(wal_segment_size, NULL,
									  XL_ROUTINE(.page_read = publish_read_page), work);
	if (work->reader == NULL)
		return CLUSTER_CONTROL_ROOT_IO_ERROR;
	work->reader->system_identifier = work->ref.claim.identity.system_identifier;
	work->reader->cluster_expected_thread_id = work->ref.claim.identity.origin_thread_id;
	work->reader->seg.ws_tli = work->ref.timeline;
	XLogBeginRead(work->reader, work->previous.record_start);
	record = XLogReadRecord(work->reader, &error);
	if (record == NULL || work->reader->ReadRecPtr != work->previous.record_start
		|| work->reader->EndRecPtr != work->previous.exclusive_end
		|| record->xl_crc != work->previous.record_crc)
		return CLUSTER_CONTROL_ROOT_BAD_RECORD_CRC;
	work->next = work->previous;
	while (work->next.exclusive_end < physical_flush) {
		work->boundary = false;
		record = XLogReadRecord(work->reader, &error);
		if (record == NULL) {
			if (work->read_result != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
				return work->read_result;
			if (!work->boundary || error != NULL)
				return CLUSTER_CONTROL_ROOT_BAD_RECORD_CRC;
			break;
		}
		if (work->reader->EndRecPtr > physical_flush)
			break;
		if (work->reader->ReadRecPtr < work->next.exclusive_end
			|| work->reader->EndRecPtr <= work->next.exclusive_end)
			return CLUSTER_CONTROL_ROOT_RANGE_INVALID;
		work->next.exclusive_end = work->reader->EndRecPtr;
		work->next.record_start = work->reader->ReadRecPtr;
		work->next.record_crc = record->xl_crc;
	}
	if (work->next.exclusive_end > work->previous.exclusive_end) {
		if (work->previous.sequence == UINT64_MAX)
			return CLUSTER_CONTROL_ROOT_SEQUENCE_EXHAUSTED;
		++work->next.sequence;
	}
	return cluster_wal_durable_prefix_successor(&work->ref, &work->previous, &work->next);
}

static bool
publish_current_matches(PublishWork *work, int fd, const uint8 *expected)
{
	uint8 bytes[CLUSTER_WAL_DURABLE_PREFIX_BYTES];
	return publish_entry_matches(work->dirs[3], "current", fd, false, sizeof(bytes))
		   && publish_exact(fd, bytes, sizeof(bytes))
		   && memcmp(bytes, expected, sizeof(bytes)) == 0;
}

static ClusterControlRootResult
publish_persist(PublishWork *work)
{
	uint8 bytes[CLUSTER_WAL_DURABLE_PREFIX_BYTES];
	ClusterControlRootResult result;
	size_t used = 0;
	int published_fd;

	result = cluster_wal_durable_prefix_encode(&work->ref, &work->next, bytes);
	if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
		return result;
	for (PublishSegment *seg = work->segments; seg != NULL; seg = seg->next)
		if (pg_fsync(seg->fd) != 0)
			return CLUSTER_CONTROL_ROOT_IO_ERROR;
	if (pg_fsync(work->dirs[2]) != 0)
		return CLUSTER_CONTROL_ROOT_IO_ERROR;
	if (!publish_runtime(work) || !publish_paths(work)
		|| !publish_current_matches(work, work->currentfd, work->previous_bytes))
		return CLUSTER_CONTROL_ROOT_STALE_TOKEN;
	if (work->next.sequence != work->previous.sequence) {
		snprintf(work->temporary, sizeof(work->temporary), ".current.%d." UINT64_FORMAT, MyProcPid,
				 work->next.sequence);
		work->tempfd = openat(work->dirs[3], work->temporary,
							  O_RDWR | O_CREAT | O_EXCL | O_NOFOLLOW | O_CLOEXEC | PG_BINARY, 0600);
		if (work->tempfd < 0)
			return CLUSTER_CONTROL_ROOT_IO_ERROR;
		work->temp_owned = true;
		while (used < sizeof(bytes)) {
			ssize_t n = write(work->tempfd, bytes + used, sizeof(bytes) - used);
			if (n < 0 && errno == EINTR)
				continue;
			if (n <= 0)
				return CLUSTER_CONTROL_ROOT_IO_ERROR;
			used += n;
		}
		if (pg_fsync(work->tempfd) != 0)
			return CLUSTER_CONTROL_ROOT_IO_ERROR;
		if (!publish_runtime(work) || !publish_paths(work)
			|| !publish_current_matches(work, work->currentfd, work->previous_bytes)
			|| !publish_entry_matches(work->dirs[3], work->temporary, work->tempfd, false,
									  sizeof(bytes)))
			return CLUSTER_CONTROL_ROOT_STALE_TOKEN;
		if (renameat(work->dirs[3], work->temporary, work->dirs[3], "current") != 0)
			return CLUSTER_CONTROL_ROOT_IO_ERROR;
		work->temp_owned = false;
		published_fd = work->tempfd;
	} else {
		published_fd = work->currentfd;
		if (pg_fsync(published_fd) != 0)
			return CLUSTER_CONTROL_ROOT_IO_ERROR;
	}
	if (pg_fsync(work->dirs[3]) != 0)
		return CLUSTER_CONTROL_ROOT_IO_ERROR;
	if (!publish_runtime(work) || !publish_paths(work)
		|| !publish_current_matches(work, published_fd, bytes))
		return CLUSTER_CONTROL_ROOT_STALE_TOKEN;
	return CLUSTER_CONTROL_ROOT_OK_PRIMARY;
}

static ClusterControlRootResult
publish_cleanup(PublishWork *work, ClusterControlRootResult result)
{
	int fds[] = { work->datafd, work->routefd, work->claimfd, work->currentfd, work->tempfd };
	if (work->reader != NULL)
		XLogReaderFree(work->reader);
	if (work->temp_owned) {
		struct stat st;
		if (fstat(work->tempfd, &st) != 0
			|| !publish_entry_matches(work->dirs[3], work->temporary, work->tempfd, false,
									  st.st_size)
			|| unlinkat(work->dirs[3], work->temporary, 0) != 0)
			result = CLUSTER_CONTROL_ROOT_IO_ERROR;
	}
	for (PublishSegment *seg = work->segments; seg != NULL; seg = seg->next)
		if (seg->fd >= 0 && close(seg->fd) != 0)
			result = CLUSTER_CONTROL_ROOT_IO_ERROR;
	for (size_t i = 0; i < lengthof(fds); ++i)
		if (fds[i] >= 0 && close(fds[i]) != 0)
			result = CLUSTER_CONTROL_ROOT_IO_ERROR;
	for (size_t i = 0; i < lengthof(work->dirs); ++i)
		if (work->dirs[i] >= 0 && close(work->dirs[i]) != 0)
			result = CLUSTER_CONTROL_ROOT_IO_ERROR;
	return result;
}

ClusterControlRootResult
cluster_wal_durable_publish(TimeLineID timeline, XLogRecPtr physical_flush,
							XLogRecPtr previous_flush, XLogRecPtr *covered)
{
	PublishWork work;
	MemoryContext saved_context;
	ResourceOwner saved_owner;
	ClusterControlRootResult result;
	XLogRecPtr end;

	if (covered == NULL)
		return CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT;
	*covered = 0;
	memset(&work, 0, sizeof(work));
	for (size_t i = 0; i < lengthof(work.dirs); ++i)
		work.dirs[i] = -1;
	work.datafd = work.routefd = work.claimfd = work.currentfd = work.tempfd = -1;
	work.epoch = cluster_epoch_get_current();
	if (publish_context == NULL || publish_owner == NULL || publish_hash == NULL
		|| physical_flush == 0 || physical_flush < previous_flush
		|| !IsValidWalSegSize(wal_segment_size) || cluster_wal_threads_dir == NULL
		|| cluster_wal_threads_dir[0] == '\0' || DataDir == NULL || DataDir[0] == '\0'
		|| !cluster_wal_thread_current_v2_ref(&work.ref) || work.ref.timeline != timeline
		|| !publish_runtime(&work))
		return CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT;
	saved_context = MemoryContextSwitchTo(publish_context);
	saved_owner = CurrentResourceOwner;
	CurrentResourceOwner = publish_owner;
	/* All ERROR-capable work is in the native critical section: allocation
	 * failure cannot return a successful flush with leaked resources. Ordinary
	 * IO/validation failures below clean their raw fds and leave output zero. */
	result = publish_open(&work);
	if (result == CLUSTER_CONTROL_ROOT_OK_PRIMARY) {
		if (work.previous.exclusive_end == 0 || work.previous.exclusive_end < previous_flush)
			result = CLUSTER_CONTROL_ROOT_RANGE_INVALID;
		else
			result = publish_scan(&work, physical_flush);
	}
	if (result == CLUSTER_CONTROL_ROOT_OK_PRIMARY)
		result = publish_persist(&work);
	end = work.next.exclusive_end;
	result = publish_cleanup(&work, result);
	/* The only owner resource is the preallocated hash, retained across flushes.
	 * Do not run transaction lock release or extension callbacks under WALWriteLock. */
	CurrentResourceOwner = saved_owner;
	MemoryContextSwitchTo(saved_context);
	MemoryContextReset(publish_context);
	if (result == CLUSTER_CONTROL_ROOT_OK_PRIMARY) {
		if (!publish_runtime(&work))
			return CLUSTER_CONTROL_ROOT_STALE_TOKEN;
		*covered = end;
	}
	return result;
}
