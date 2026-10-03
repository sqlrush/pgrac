/* Author: SqlRush <sqlrush@gmail.com> */
/* Original creator's exact, read-only native shutdown record inspection.
 * Author: SqlRush <sqlrush@gmail.com> */
#include "postgres_fe.h"

#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include "access/xlog_internal.h"
#include "access/xlogreader.h"
#include "catalog/catversion.h"
#include "storage/bufpage.h"
#include "pgrac_wal.h"

#define CHECKPOINT_RECORD_SIZE \
	(SizeOfXLogRecord + SizeOfXLogRecordDataHeaderShort + sizeof(CheckPoint))

typedef struct InitdbWalRead
{
	int directory_fd;
	const ControlFileData *control;
	uint16 thread;
	XLogRecPtr page;
	XLogSegNo segment;
	int fd[2];
	struct stat held[2];
} InitdbWalRead;

static bool
same_file(const struct stat *a, const struct stat *b)
{
	return a->st_dev == b->st_dev && a->st_ino == b->st_ino
		&& a->st_mode == b->st_mode && a->st_uid == b->st_uid
		&& a->st_nlink == b->st_nlink && a->st_size == b->st_size
		&& a->st_mtime == b->st_mtime && a->st_ctime == b->st_ctime;
}

static bool
file_unchanged(InitdbWalRead *read, int index)
{
	char name[MAXFNAMELEN];
	struct stat held, named;

	XLogFileName(name, read->control->checkPointCopy.ThisTimeLineID,
		read->segment + index, read->control->xlog_seg_size);
	return fstat(read->fd[index], &held) == 0
		&& fstatat(read->directory_fd, name, &named, AT_SYMLINK_NOFOLLOW) == 0
		&& same_file(&read->held[index], &held) && same_file(&held, &named);
}

static int
read_page(XLogReaderState *reader, XLogRecPtr page, int requested,
	XLogRecPtr record, char *buffer)
{
	InitdbWalRead *read = reader->private_data;
	const ControlFileData *control = read->control;
	XLogSegNo segment;
	XLogPageHeader header = (XLogPageHeader) buffer;
	uint32 offset = XLogSegmentOffset(page, control->xlog_seg_size);
	ssize_t n;
	int index;

	XLByteToSeg(page, segment, control->xlog_seg_size);
	/* One checkpoint fits at most two pages; the decoder also verifies the
	 * first page of its segment.  Never chase a corrupt length into history. */
	if (record != control->checkPoint || requested < 0 || requested > XLOG_BLCKSZ
		|| page % XLOG_BLCKSZ != 0 || segment < read->segment
		|| segment > read->segment + 1
		|| (page != read->page && page != read->page + XLOG_BLCKSZ
			&& !(segment == read->segment && offset == 0)))
		return -1;
	index = segment - read->segment;
	if (read->fd[index] < 0)
	{
		char name[MAXFNAMELEN];
		struct stat *st = &read->held[index];

		XLogFileName(name, control->checkPointCopy.ThisTimeLineID, segment,
			control->xlog_seg_size);
		read->fd[index] = openat(read->directory_fd, name,
			O_RDONLY | O_NOFOLLOW | O_NONBLOCK | O_CLOEXEC | PG_BINARY);
		if (read->fd[index] < 0 || fstat(read->fd[index], st) != 0
			|| !S_ISREG(st->st_mode) || st->st_nlink != 1
			|| st->st_uid != geteuid() || (st->st_mode & (S_IWGRP | S_IWOTH)) != 0
			|| st->st_size != control->xlog_seg_size)
			return -1;
	}
	do { n = pread(read->fd[index], buffer, XLOG_BLCKSZ, offset); }
	while (n < 0 && errno == EINTR);
	if (n != XLOG_BLCKSZ || !file_unchanged(read, index)
		|| header->xlp_thread_id != read->thread
		|| header->xlp_tli != control->checkPointCopy.ThisTimeLineID)
		return -1;
	if (page == read->page)
	{
		uint32 length;

		/* xl_tot_len always fits on the aligned first page, even if the rest
		 * of the header crosses it.  Bound the decoder's allocation first. */
		memcpy(&length, buffer + control->checkPoint % XLOG_BLCKSZ, sizeof(length));
		if (length != CHECKPOINT_RECORD_SIZE)
			return -1;
	}
	reader->seg.ws_tli = control->checkPointCopy.ThisTimeLineID;
	return XLOG_BLCKSZ;
}

bool
pgrac_initdb_wal_observe(int directory_fd, const ControlFileData *control,
	uint16 thread, PgracInitdbWalObservation *out)
{
	InitdbWalRead read = {0};
	XLogReaderState *reader;
	XLogRecord *record;
	PgracInitdbWalObservation observed = {0};
	struct stat directory;
	pg_crc32c crc;
	char *error = NULL;
	bool valid = false;

	if (out == NULL)
		return false;
	memset(out, 0, sizeof(*out));
	if (control == NULL || thread == 0 || thread > 128 || directory_fd < 0)
		return false;
	INIT_CRC32C(crc);
	COMP_CRC32C(crc, control, offsetof(ControlFileData, crc));
	FIN_CRC32C(crc);
	if (!EQ_CRC32C(crc, control->crc) || control->system_identifier == 0
		|| control->pg_control_version != PG_CONTROL_VERSION
		|| control->catalog_version_no != CATALOG_VERSION_NO
		|| control->state != DB_SHUTDOWNED
		|| control->blcksz != BLCKSZ || control->xlog_blcksz != XLOG_BLCKSZ
		|| !IsValidWalSegSize(control->xlog_seg_size)
		|| control->data_checksum_version != PG_DATA_CHECKSUM_VERSION
		|| control->checkPoint < control->xlog_seg_size
		|| control->checkPoint > UINT64_MAX - 2 * XLOG_BLCKSZ
		|| control->checkPoint % MAXIMUM_ALIGNOF != 0
		|| !XRecOffIsValid(control->checkPoint)
		|| control->checkPointCopy.redo != control->checkPoint
		|| control->checkPointCopy.ThisTimeLineID != 1
		|| control->checkPointCopy.PrevTimeLineID != 1
		|| fstat(directory_fd, &directory) != 0 || !S_ISDIR(directory.st_mode)
		|| directory.st_uid != geteuid()
		|| (directory.st_mode & (S_IWGRP | S_IWOTH)) != 0)
		return false;
	read.directory_fd = directory_fd;
	read.control = control;
	read.thread = thread;
	read.page = control->checkPoint - control->checkPoint % XLOG_BLCKSZ;
	XLByteToSeg(control->checkPoint, read.segment, control->xlog_seg_size);
	read.fd[0] = read.fd[1] = -1;
	reader = XLogReaderAllocate(control->xlog_seg_size, NULL,
		XL_ROUTINE(.page_read = read_page), &read);
	if (reader == NULL)
		return false;
	reader->system_identifier = control->system_identifier;
	reader->cluster_expected_thread_id = thread;
	XLogBeginRead(reader, control->checkPoint);
	record = XLogReadRecord(reader, &error);
	if (record != NULL && reader->ReadRecPtr == control->checkPoint
		&& record->xl_rmid == RM_XLOG_ID && record->xl_info == XLOG_CHECKPOINT_SHUTDOWN
		&& record->xl_xid == InvalidTransactionId && record->xl_tot_len == CHECKPOINT_RECORD_SIZE
		&& XLogRecMaxBlockId(reader) == -1
		&& XLogRecGetDataLen(reader) == sizeof(CheckPoint)
		&& memcmp(XLogRecGetData(reader), &control->checkPointCopy, sizeof(CheckPoint)) == 0
		&& reader->EndRecPtr > control->checkPoint
		&& reader->EndRecPtr <= read.page + 2 * XLOG_BLCKSZ)
	{
		observed.checkpoint_start = reader->ReadRecPtr;
		observed.checkpoint_end = reader->EndRecPtr;
		observed.checkpoint_crc = record->xl_crc;
		valid = true;
	}
	XLogReaderFree(reader);
	for (int i = 0; i < 2; ++i)
	{
		if (read.fd[i] >= 0)
		{
			if (!file_unchanged(&read, i))
				valid = false;
			if (close(read.fd[i]) != 0)
				valid = false;
		}
	}
	if (valid)
		*out = observed;
	return valid;
}
