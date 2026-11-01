/* Selected native CLOG recovery I/O. Author: SqlRush <sqlrush@gmail.com> */
#include "postgres.h"

#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include "access/clog.h"
#include "access/clog_storage.h"
#include "access/clog_wal.h"
#include "access/xlogutils.h"
#include "miscadmin.h"
#include "storage/fd.h"

static bool
clog_replay_overlap(const void *left, Size left_bytes,
					const void *right, Size right_bytes)
{
	uintptr_t a = (uintptr_t)left, b = (uintptr_t)right;

	return a <= b ? b - a < left_bytes : a - b < right_bytes;
}

static bool
clog_replay_read(int fd, void *bytes, Size length, off_t offset)
{
	ssize_t count = pg_pread(fd, bytes, length, offset);

	if (count == (ssize_t)length)
		return true;
	if (count >= 0)
		errno = EIO;
	return false;
}

static bool
clog_replay_extent(const struct stat *st)
{
	return S_ISREG(st->st_mode) && st->st_size >= 0 &&
		st->st_size <= CLUSTER_CLOG_FILE_HEADER_BYTES +
			CLUSTER_CLOG_PAGES_PER_FILE * CLUSTER_CLOG_FRAME_BYTES;
}

static bool
clog_replay_same_file(const struct stat *before, const struct stat *after)
{
	return clog_replay_extent(after) && before->st_dev == after->st_dev &&
		before->st_ino == after->st_ino && after->st_size >= before->st_size;
}

static bool
clog_replay_header(int fd, const ClusterClogFileBindingV2 *expected,
				   uint8 header[CLUSTER_CLOG_FILE_HEADER_BYTES])
{
	return clog_replay_read(fd, header, CLUSTER_CLOG_FILE_HEADER_BYTES, 0) &&
		cluster_clog_file_header_matches_v2(header, CLUSTER_CLOG_FILE_HEADER_BYTES, expected);
}

/* Refuse a complete, independently self-consistent different file header.
 * A torn header is repairable only because the original recovery owner has
 * already selected this managed file from control and retained native WAL. */
static bool
clog_replay_other_header(const uint8 header[CLUSTER_CLOG_FILE_HEADER_BYTES],
						const ClusterClogFileBindingV2 *expected)
{
	ClusterClogFileBindingV2 disk;

	if (!cluster_recovery_file_identity_decode_v2(header + 32, 128, &disk.identity) ||
		disk.identity.object_id >= 4096)
		return false;
	disk.segment = (uint32)disk.identity.object_id;
	return cluster_clog_file_header_matches_v2(header, CLUSTER_CLOG_FILE_HEADER_BYTES, &disk) &&
		!cluster_clog_file_header_matches_v2(header, CLUSTER_CLOG_FILE_HEADER_BYTES, expected);
}

/* The original owner's unflushed state cannot erase valid durable effects. */
static bool
clog_replay_covers(const ClusterClogPageIdentityV2 *memory,
				   const uint8 *memory_bytes,
				   const ClusterClogPageIdentityV2 *disk,
				   const uint8 *disk_bytes)
{
	if (memory->identity.logical_page != disk->identity.logical_page ||
		memory->identity.page_generation != disk->identity.page_generation ||
		!FullTransactionIdEquals(memory->identity.birth_full_xid, disk->identity.birth_full_xid) ||
		memory->identity.page_lsn < disk->identity.page_lsn ||
		memory->image_lsn < disk->image_lsn)
		return false;
	for (Size i = 0; i < CLUSTER_CLOG_PAGE_BYTES; i++)
	{
		for (unsigned shift = 0; shift < 8; shift += 2)
		{
			unsigned a = (disk_bytes[i] >> shift) & 3;
			unsigned b = (memory_bytes[i] >> shift) & 3;

			if (a != b && a != TRANSACTION_STATUS_IN_PROGRESS &&
				!(a == TRANSACTION_STATUS_SUB_COMMITTED &&
				  (b == TRANSACTION_STATUS_COMMITTED || b == TRANSACTION_STATUS_ABORTED)))
				return false;
		}
	}
	return true;
}

bool
cluster_clog_storage_replay_v2(int fd, const ClusterClogFileBindingV2 *expected,
							  uint8 info, const void *record, Size record_length,
							  XLogRecPtr record_end, const void *current_frame,
							  Size current_length, XLogRecPtr *applied_end)
{
	ClusterClogFileBindingV2 binding;
	ClusterClogWalObservationV2 observed;
	ClusterClogPageIdentityV2 disk, memory, checked;
	uint8 saved_record[CLUSTER_CLOG_PAGE_WAL_BYTES_V2];
	uint8 reference[CLUSTER_CLOG_FILE_REFERENCE_BYTES];
	uint8 header[CLUSTER_CLOG_FILE_HEADER_BYTES];
	uint8 frame[CLUSTER_CLOG_FRAME_BYTES], disk_frame[CLUSTER_CLOG_FRAME_BYTES];
	uint8 disk_payload[CLUSTER_CLOG_PAGE_BYTES], memory_payload[CLUSTER_CLOG_PAGE_BYTES];
	struct stat before, after;
	uint64 offset = 0;
	const void *write_bytes;
	Size write_length;
	ssize_t count;
#ifndef WIN32
	int flags;
#endif

	if (fd < 0 || expected == NULL || record == NULL || applied_end == NULL ||
		(info != CLUSTER_RECOVERY_CLOG_SEGMENT_WAL_V2 &&
		 info != CLUSTER_RECOVERY_CLOG_PAGE_IMAGE_WAL_V2) ||
		record_length != (info == CLUSTER_RECOVERY_CLOG_SEGMENT_WAL_V2 ?
						  CLUSTER_CLOG_SEGMENT_WAL_BYTES_V2 : CLUSTER_CLOG_PAGE_WAL_BYTES_V2) ||
		record_end == InvalidXLogRecPtr || record_end == UINT64_MAX ||
		(current_frame == NULL ? current_length != 0 :
		 current_length != CLUSTER_CLOG_FRAME_BYTES) ||
		(info == CLUSTER_RECOVERY_CLOG_SEGMENT_WAL_V2 && current_frame != NULL) ||
		clog_replay_overlap(applied_end, sizeof(*applied_end), expected, sizeof(*expected)) ||
		clog_replay_overlap(applied_end, sizeof(*applied_end), record, record_length) ||
		(current_frame != NULL &&
		 clog_replay_overlap(applied_end, sizeof(*applied_end), current_frame, current_length)) ||
		!InRecovery || MyBackendType != B_STARTUP || IsBootstrapProcessingMode() || !enableFsync)
	{
		errno = EINVAL;
		return false;
	}
	binding = *expected;
	memcpy(saved_record, record, record_length);
	if (!cluster_clog_wal_decode_v2(info, saved_record, record_length, &observed) ||
		!cluster_recovery_file_identity_encode_v2(&binding.identity, reference, sizeof(reference)) ||
		binding.segment != observed.binding.segment ||
		memcmp(reference, saved_record, sizeof(reference)) != 0)
	{
		errno = EILSEQ;
		return false;
	}
	if (info == CLUSTER_RECOVERY_CLOG_PAGE_IMAGE_WAL_V2 &&
		(!cluster_clog_wal_page_restore_v2(&binding, saved_record, record_length, record_end,
										 current_frame, current_length, frame, sizeof(frame)) ||
		 !cluster_clog_file_offset_v2(observed.page.identity.logical_page %
									  CLUSTER_CLOG_PAGES_PER_FILE, &offset)))
	{
		errno = EILSEQ;
		return false;
	}
#ifdef WIN32
	errno = ENOSYS;
	return false;
#else
	flags = fcntl(fd, F_GETFL);
	if (flags < 0)
		return false;
	if ((flags & O_ACCMODE) != O_RDWR || (flags & O_APPEND) != 0)
	{
		errno = EINVAL;
		return false;
	}
#endif
	if (fstat(fd, &before) != 0)
		return false;
	if (!clog_replay_extent(&before))
	{
		errno = EILSEQ;
		return false;
	}

	if (info == CLUSTER_RECOVERY_CLOG_SEGMENT_WAL_V2)
	{
		if (before.st_size >= sizeof(header) &&
			(!clog_replay_read(fd, header, sizeof(header), 0) ||
			 clog_replay_other_header(header, &binding)))
			return false;
		write_bytes = observed.image;
		write_length = CLUSTER_CLOG_FILE_HEADER_BYTES;
	}
	else
	{
		if (!clog_replay_header(fd, &binding, header))
			return false;
		if (before.st_size >= (off_t)(offset + sizeof(disk_frame)))
		{
			if (!clog_replay_read(fd, disk_frame, sizeof(disk_frame), (off_t)offset))
				return false;
			if (cluster_clog_file_page_decode_v2(&binding, observed.page.identity.logical_page,
												disk_frame, sizeof(disk_frame), &disk,
												disk_payload, sizeof(disk_payload)))
			{
				if (current_frame != NULL)
				{
					if (!cluster_clog_file_page_decode_v2(&binding, observed.page.identity.logical_page,
														 current_frame, current_length, &memory,
														 memory_payload, sizeof(memory_payload)) ||
						!clog_replay_covers(&memory, memory_payload, &disk, disk_payload))
						return false;
				}
				else if (!cluster_clog_wal_page_restore_v2(&binding, saved_record, record_length,
														 record_end, disk_frame, sizeof(disk_frame),
														 frame, sizeof(frame)))
					return false;
			}
		}
		/* Recheck the selected immutable header and held file after the fold,
		 * before the first mutation. Other pages may only extend the file. */
		if (!clog_replay_header(fd, &binding, header))
			return false;
		write_bytes = frame;
		write_length = sizeof(frame);
	}
	if (fstat(fd, &after) != 0 || !clog_replay_same_file(&before, &after) || !enableFsync)
		return false;
	count = pg_pwrite(fd, write_bytes, write_length, (off_t)offset);
	if (count != (ssize_t)write_length)
	{
		if (count >= 0)
			errno = EIO;
		return false;
	}
	if (!enableFsync || pg_fsync(fd) != 0 || !enableFsync)
		return false;
	if (info == CLUSTER_RECOVERY_CLOG_SEGMENT_WAL_V2)
	{
		if (!clog_replay_header(fd, &binding, header) ||
			memcmp(header, write_bytes, write_length) != 0)
			return false;
	}
	else
	{
		if (!cluster_clog_storage_read_page_v2(fd, &binding, observed.page.identity.logical_page,
											 &checked, disk_payload, sizeof(disk_payload)) ||
			!cluster_clog_file_page_encode_v2(&binding, &checked, disk_payload, sizeof(disk_payload),
											 disk_frame, sizeof(disk_frame)) ||
			memcmp(disk_frame, frame, sizeof(frame)) != 0)
			return false;
	}
	if (fstat(fd, &after) != 0 || !clog_replay_same_file(&before, &after) ||
		after.st_size < (off_t)(offset + write_length))
		return false;
	*applied_end = record_end;
	return true;
}
