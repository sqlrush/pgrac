/* Local CLOG full-frame WAL-before-data writes. Author: SqlRush <sqlrush@gmail.com> */
#include "postgres.h"

#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include "access/clog_storage.h"
#include "access/clog_wal.h"
#include "access/xlog.h"
#include "access/xloginsert.h"
#include "access/xlogutils.h"
#include "miscadmin.h"
#include "storage/fd.h"
#include "storage/proc.h"
#ifdef USE_PGRAC_CLUSTER
#include "cluster/cluster_guc.h"
#endif

static bool
clog_write_overlap(const void *left, Size left_bytes,
				   const void *right, Size right_bytes)
{
	uintptr_t a = (uintptr_t)left, b = (uintptr_t)right;

	return a <= b ? b - a < left_bytes : a - b < right_bytes;
}

static bool
clog_write_same_birth(const ClusterClogPageIdentityV2 *left,
					  const ClusterClogPageIdentityV2 *right)
{
	return left->identity.logical_page == right->identity.logical_page &&
		left->identity.page_generation == right->identity.page_generation &&
		left->identity.birth_observed && right->identity.birth_observed &&
		FullTransactionIdEquals(left->identity.birth_full_xid, right->identity.birth_full_xid);
}

bool
cluster_clog_storage_write_page_v2(int fd, const ClusterClogFileBindingV2 *expected,
								  const ClusterClogPageIdentityV2 *page,
								  const void *payload, Size payload_length,
								  XLogRecPtr *last_end)
{
	ClusterClogFileBindingV2 binding;
	ClusterClogPageIdentityV2 snapshot, previous, checked;
	uint8 record[CLUSTER_CLOG_PAGE_WAL_BYTES_V2];
	uint8 checked_payload[CLUSTER_CLOG_PAGE_BYTES];
	uint8 *frame = record + 128;
	PGPROC *proc = MyProc;
	XLogRecPtr end = InvalidXLogRecPtr;
	uint64 offset;
	bool own_delay, success = false;
	int origin = 0;
#ifndef WIN32
	int flags;
#endif
	ssize_t written;

#ifdef USE_PGRAC_CLUSTER
	if (cluster_enabled)
		origin = cluster_node_id;
#endif
	if (fd < 0 || expected == NULL || page == NULL || payload == NULL ||
		last_end == NULL || payload_length != sizeof(checked_payload) ||
		clog_write_overlap(last_end, sizeof(*last_end), expected, sizeof(*expected)) ||
		clog_write_overlap(last_end, sizeof(*last_end), page, sizeof(*page)) ||
		clog_write_overlap(last_end, sizeof(*last_end), payload, payload_length))
	{
		errno = EINVAL;
		return false;
	}
	binding = *expected;
	snapshot = *page;
	/* These checks exclude local WAL for foreign/recovery/bootstrap writes.
	 * They do not replace the caller's original registration/source custody. */
	if (proc == NULL || !enableFsync || InRecovery || RecoveryInProgress() || IsBootstrapProcessingMode() ||
		origin < 0 || origin >= 128 || binding.identity.origin != origin ||
		binding.identity.system_identifier != GetSystemIdentifier() ||
		binding.identity.redo_timeline != GetWALInsertionTimeLine() ||
		!cluster_recovery_file_identity_encode_v2(&binding.identity, record, 128) ||
		!cluster_clog_file_page_encode_v2(&binding, &snapshot, payload, payload_length,
										 frame, CLUSTER_CLOG_FRAME_BYTES) ||
		!cluster_clog_file_offset_v2(snapshot.identity.logical_page %
								   CLUSTER_CLOG_PAGES_PER_FILE, &offset))
	{
		errno = EINVAL;
		return false;
	}
#ifdef WIN32
	/* This borrowed-FD path requires reliable append/access-mode inspection. */
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
	own_delay = (proc->delayChkptFlags & DELAY_CHKPT_START) == 0;
	proc->delayChkptFlags |= DELAY_CHKPT_START;
	PG_TRY();
	{
		do
		{
			if (!cluster_clog_storage_read_page_v2(fd, &binding, snapshot.identity.logical_page,
												 &previous, checked_payload, sizeof(checked_payload)))
				break;
			if (!clog_write_same_birth(&snapshot, &previous) ||
				snapshot.image_lsn != previous.image_lsn ||
				snapshot.identity.page_lsn < previous.identity.page_lsn)
			{
				errno = EILSEQ;
				break;
			}
			/* Always include the complete captured frame. Native insertion still
			 * rechecks its redo state under its insertion lock; no cached redo
			 * point, slot history or full_page_writes flag can omit this image. */
			XLogBeginInsert();
			XLogRegisterData((char *)record, sizeof(record));
			end = XLogInsert(RM_CLOG_ID, CLUSTER_RECOVERY_CLOG_PAGE_IMAGE_WAL_V2);
			if (end == InvalidXLogRecPtr || end == UINT64_MAX ||
				end <= snapshot.identity.page_lsn || end <= snapshot.image_lsn)
			{
				errno = EILSEQ;
				break;
			}
			/* XLogInsert has copied record. Stamp its actual end only in the
			 * private disk image; the native replay owner must do the same. */
			snapshot.image_lsn = end;
			if (!cluster_clog_file_page_encode_v2(&binding, &snapshot, frame + 64,
					CLUSTER_CLOG_PAGE_BYTES, frame, CLUSTER_CLOG_FRAME_BYTES))
				break;
			XLogFlush(end);
			if (!cluster_clog_storage_read_page_v2(fd, &binding, snapshot.identity.logical_page,
												 &checked, checked_payload, sizeof(checked_payload)))
				break;
			if (!enableFsync || !clog_write_same_birth(&previous, &checked) ||
				previous.image_lsn != checked.image_lsn ||
				previous.identity.page_lsn != checked.identity.page_lsn)
			{
				errno = EILSEQ;
				break;
			}
			written = pg_pwrite(fd, frame, CLUSTER_CLOG_FRAME_BYTES, (off_t)offset);
			if (written != CLUSTER_CLOG_FRAME_BYTES)
			{
				if (written >= 0)
					errno = ENOSPC;
				break;
			}
			if (!enableFsync || pg_fsync(fd) != 0 || !enableFsync ||
				!cluster_clog_storage_read_page_v2(fd, &binding, snapshot.identity.logical_page,
												 &checked, checked_payload, sizeof(checked_payload)))
				break;
			if (!clog_write_same_birth(&snapshot, &checked) ||
				snapshot.image_lsn != checked.image_lsn ||
				snapshot.identity.page_lsn != checked.identity.page_lsn ||
				memcmp(frame + 64, checked_payload, sizeof(checked_payload)) != 0)
			{
				errno = EIO;
				break;
			}
			success = true;
		} while (0);
	}
	PG_FINALLY();
	{
		if (own_delay)
			proc->delayChkptFlags &= ~DELAY_CHKPT_START;
	}
	PG_END_TRY();
	if (success)
		*last_end = end;
	return success;
}

bool
cluster_clog_storage_initialize_v2(int fd, const ClusterClogFileBindingV2 *expected,
								   const ClusterClogPageIdentityV2 *page,
								   const void *payload, Size payload_length,
								   XLogRecPtr *last_end)
{
	ClusterClogFileBindingV2 binding;
	ClusterClogPageIdentityV2 snapshot, checked;
	uint8 header_record[CLUSTER_CLOG_SEGMENT_WAL_BYTES_V2];
	uint8 page_record[CLUSTER_CLOG_PAGE_WAL_BYTES_V2];
	uint8 checked_payload[CLUSTER_CLOG_PAGE_BYTES];
	uint8 *header = header_record + 128;
	uint8 *frame = page_record + 128;
	PGPROC *proc = MyProc;
	XLogRecPtr header_end, end = InvalidXLogRecPtr;
	struct stat before, after;
	uint64 offset;
	bool own_delay, success = false;
	int origin = 0;
#ifndef WIN32
	int flags;
#endif
	ssize_t written;

#ifdef USE_PGRAC_CLUSTER
	if (cluster_enabled)
		origin = cluster_node_id;
#endif
	if (fd < 0 || expected == NULL || page == NULL || payload == NULL ||
		last_end == NULL || payload_length != sizeof(checked_payload) ||
		clog_write_overlap(last_end, sizeof(*last_end), expected, sizeof(*expected)) ||
		clog_write_overlap(last_end, sizeof(*last_end), page, sizeof(*page)) ||
		clog_write_overlap(last_end, sizeof(*last_end), payload, payload_length))
	{
		errno = EINVAL;
		return false;
	}
	binding = *expected;
	snapshot = *page;
	/* The caller supplies the original registration and observed birth. No
	 * existing frame, raw xid, local epoch or target file can fill either in. */
	if (proc == NULL || !enableFsync || InRecovery || RecoveryInProgress() || IsBootstrapProcessingMode() ||
		origin < 0 || origin >= 128 || binding.identity.origin != origin ||
		binding.identity.system_identifier != GetSystemIdentifier() ||
		binding.identity.redo_timeline != GetWALInsertionTimeLine() ||
		snapshot.image_lsn != InvalidXLogRecPtr ||
		!cluster_recovery_file_identity_encode_v2(&binding.identity, header_record, 128) ||
		!cluster_clog_file_header_encode_v2(&binding, header, CLUSTER_CLOG_FILE_HEADER_BYTES) ||
		!cluster_clog_file_page_encode_v2(&binding, &snapshot, payload, payload_length,
										 frame, CLUSTER_CLOG_FRAME_BYTES) ||
		!cluster_clog_file_offset_v2(snapshot.identity.logical_page %
								   CLUSTER_CLOG_PAGES_PER_FILE, &offset))
	{
		errno = EINVAL;
		return false;
	}
	memcpy(page_record, header_record, 128);
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
	own_delay = (proc->delayChkptFlags & DELAY_CHKPT_START) == 0;
	proc->delayChkptFlags |= DELAY_CHKPT_START;
	PG_TRY();
	{
		do
		{
			if (fstat(fd, &before) != 0)
				break;
			if (!S_ISREG(before.st_mode) || before.st_size != 0)
			{
				errno = EEXIST;
				break;
			}
			/* The header itself must be recoverable after a torn first write.
			 * Both complete images precede every physical file write. */
			XLogBeginInsert();
			XLogRegisterData((char *)header_record, sizeof(header_record));
			header_end = XLogInsert(RM_CLOG_ID, CLUSTER_RECOVERY_CLOG_SEGMENT_WAL_V2);
			if (header_end == InvalidXLogRecPtr || header_end == UINT64_MAX ||
				header_end <= snapshot.identity.page_lsn)
			{
				errno = EILSEQ;
				break;
			}
			XLogBeginInsert();
			XLogRegisterData((char *)page_record, sizeof(page_record));
			end = XLogInsert(RM_CLOG_ID, CLUSTER_RECOVERY_CLOG_PAGE_IMAGE_WAL_V2);
			if (end == InvalidXLogRecPtr || end == UINT64_MAX || end <= header_end)
			{
				errno = EILSEQ;
				break;
			}
			/* The WAL copy still contains the captured image_lsn of zero. Only
			 * this private disk image is sealed with the actual page-record end. */
			snapshot.image_lsn = end;
			if (!cluster_clog_file_page_encode_v2(&binding, &snapshot, frame + 64,
					CLUSTER_CLOG_PAGE_BYTES, frame, CLUSTER_CLOG_FRAME_BYTES))
				break;
			XLogFlush(end);
			if (fstat(fd, &after) != 0)
				break;
			if (!enableFsync || !S_ISREG(after.st_mode) || after.st_size != 0 ||
				after.st_dev != before.st_dev || after.st_ino != before.st_ino)
			{
				errno = EILSEQ;
				break;
			}
			written = pg_pwrite(fd, header, CLUSTER_CLOG_FILE_HEADER_BYTES, 0);
			if (written != CLUSTER_CLOG_FILE_HEADER_BYTES)
			{
				if (written >= 0)
					errno = ENOSPC;
				break;
			}
			written = pg_pwrite(fd, frame, CLUSTER_CLOG_FRAME_BYTES, (off_t)offset);
			if (written != CLUSTER_CLOG_FRAME_BYTES)
			{
				if (written >= 0)
					errno = ENOSPC;
				break;
			}
			if (!enableFsync || pg_fsync(fd) != 0 || !enableFsync ||
				!cluster_clog_storage_read_page_v2(fd, &binding, snapshot.identity.logical_page,
												 &checked, checked_payload, sizeof(checked_payload)))
				break;
			if (!clog_write_same_birth(&snapshot, &checked) ||
				snapshot.image_lsn != checked.image_lsn ||
				snapshot.identity.page_lsn != checked.identity.page_lsn ||
				memcmp(frame + 64, checked_payload, sizeof(checked_payload)) != 0)
			{
				errno = EIO;
				break;
			}
			success = true;
		} while (0);
	}
	PG_FINALLY();
	{
		if (own_delay)
			proc->delayChkptFlags &= ~DELAY_CHKPT_START;
	}
	PG_END_TRY();
	if (success)
		*last_end = end;
	return success;
}
