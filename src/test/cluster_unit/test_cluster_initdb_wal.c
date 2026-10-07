/* Exact checkpoint readback with the native decoder and real files.
 * Author: SqlRush <sqlrush@gmail.com> */
#include "postgres_fe.h"
#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>
#include "access/xlog_internal.h"
#include "catalog/catversion.h"
#include "storage/bufpage.h"
#include "../../bin/initdb/pgrac_wal.h"
#include "unit_test.h"

UT_DEFINE_GLOBALS();
static char directory[MAXPGPATH];
static char filename[MAXFNAMELEN];
static int directory_fd;
static ControlFileData control;
static PGAlignedBlock pages[2];
static XLogRecPtr expected_end;

static void
control_crc(void)
{
	INIT_CRC32C(control.crc);
	COMP_CRC32C(control.crc, &control, offsetof(ControlFileData, crc));
	FIN_CRC32C(control.crc);
}

static void
save_pages(void)
{
	int fd = openat(directory_fd, filename, O_CREAT | O_TRUNC | O_RDWR, 0600);
	UT_ASSERT(fd >= 0);
	UT_ASSERT(ftruncate(fd, control.xlog_seg_size) == 0);
	UT_ASSERT(write(fd, pages, sizeof(pages)) == sizeof(pages));
	UT_ASSERT(close(fd) == 0);
}

static void
prepare(bool crossing, bool other_rmid)
{
	char record_bytes[SizeOfXLogRecord + SizeOfXLogRecordDataHeaderShort + sizeof(CheckPoint)];
	XLogRecord record = { 0 };
	XLogLongPageHeader header = (XLogLongPageHeader)pages[0].data;
	unsigned offset = crossing ? XLOG_BLCKSZ - 16 : SizeOfXLogLongPHD;

	memset(&control, 0, sizeof(control));
	memset(pages, 0, sizeof(pages));
	control.system_identifier = UINT64_C(0x123456789);
	control.pg_control_version = PG_CONTROL_VERSION;
	control.catalog_version_no = CATALOG_VERSION_NO;
	control.state = DB_SHUTDOWNED;
	control.blcksz = BLCKSZ;
	control.xlog_blcksz = XLOG_BLCKSZ;
	control.xlog_seg_size = 1024 * 1024;
	control.data_checksum_version = PG_DATA_CHECKSUM_VERSION;
	control.checkPoint = control.xlog_seg_size + offset;
	control.checkPointCopy.redo = control.checkPoint;
	control.checkPointCopy.ThisTimeLineID = 1;
	control.checkPointCopy.PrevTimeLineID = 1;
	control.checkPointCopy.fullPageWrites = true;
	control.checkPointCopy.nextXid = FullTransactionIdFromU64(751);
	control_crc();
	header->std.xlp_magic = XLOG_PAGE_MAGIC;
	header->std.xlp_info = XLP_LONG_HEADER;
	header->std.xlp_tli = 1;
	header->std.xlp_pageaddr = control.xlog_seg_size;
	header->std.xlp_thread_id = 7;
	header->xlp_sysid = control.system_identifier;
	header->xlp_seg_size = control.xlog_seg_size;
	header->xlp_xlog_blcksz = XLOG_BLCKSZ;
	record.xl_tot_len = sizeof(record_bytes);
	record.xl_rmid = other_rmid ? RM_HEAP_ID : RM_XLOG_ID;
	record.xl_info = XLOG_CHECKPOINT_SHUTDOWN;
	record.xl_scn = UINT64_C(87345);
	memcpy(record_bytes, &record, SizeOfXLogRecord);
	record_bytes[SizeOfXLogRecord] = (char)XLR_BLOCK_ID_DATA_SHORT;
	record_bytes[SizeOfXLogRecord + 1] = sizeof(CheckPoint);
	memcpy(record_bytes + SizeOfXLogRecord + SizeOfXLogRecordDataHeaderShort,
		   &control.checkPointCopy, sizeof(CheckPoint));
	INIT_CRC32C(record.xl_crc);
	COMP_CRC32C(record.xl_crc, record_bytes + SizeOfXLogRecord,
				sizeof(record_bytes) - SizeOfXLogRecord);
	COMP_CRC32C(record.xl_crc, &record, offsetof(XLogRecord, xl_crc));
	FIN_CRC32C(record.xl_crc);
	memcpy(record_bytes, &record, SizeOfXLogRecord);
	if (crossing) {
		XLogPageHeader continuation = (XLogPageHeader)pages[1].data;
		continuation->xlp_magic = XLOG_PAGE_MAGIC;
		continuation->xlp_info = XLP_FIRST_IS_CONTRECORD;
		continuation->xlp_tli = 1;
		continuation->xlp_pageaddr = control.xlog_seg_size + XLOG_BLCKSZ;
		continuation->xlp_thread_id = 7;
		continuation->xlp_rem_len = sizeof(record_bytes) - 16;
		memcpy(pages[0].data + offset, record_bytes, 16);
		memcpy(pages[1].data + SizeOfXLogShortPHD, record_bytes + 16, sizeof(record_bytes) - 16);
		expected_end = control.xlog_seg_size + XLOG_BLCKSZ + SizeOfXLogShortPHD
					   + MAXALIGN(sizeof(record_bytes) - 16);
	} else {
		memcpy(pages[0].data + offset, record_bytes, sizeof(record_bytes));
		expected_end = control.checkPoint + MAXALIGN(sizeof(record_bytes));
	}
	XLogFileName(filename, 1, 1, control.xlog_seg_size);
	save_pages();
}

static void
refused(void)
{
	PgracInitdbWalObservation out, zero = { 0 };
	memset(&out, 0xA5, sizeof(out));
	UT_ASSERT(!pgrac_initdb_wal_observe(directory_fd, &control, 7, &out));
	UT_ASSERT(memcmp(&out, &zero, sizeof(out)) == 0);
}

UT_TEST(exact_native_checkpoint)
{
	PgracInitdbWalObservation out;
	prepare(false, false);
	UT_ASSERT(pgrac_initdb_wal_observe(directory_fd, &control, 7, &out));
	UT_ASSERT(out.checkpoint_start == control.checkPoint && out.checkpoint_end == expected_end);
	UT_ASSERT(out.checkpoint_crc == ((XLogRecord *)(pages[0].data + SizeOfXLogLongPHD))->xl_crc);
	UT_ASSERT(out.checkpoint_scn == UINT64_C(87345));
}

UT_TEST(native_checkpoint_spans_pages)
{
	PgracInitdbWalObservation out;
	prepare(true, false);
	UT_ASSERT(pgrac_initdb_wal_observe(directory_fd, &control, 7, &out));
	UT_ASSERT(out.checkpoint_start == control.checkPoint && out.checkpoint_end == expected_end);
	UT_ASSERT(out.checkpoint_scn == UINT64_C(87345));
}

UT_TEST(wrong_thread_including_legacy_refuses)
{
	for (int thread = 0; thread <= 8; thread += 8) {
		prepare(false, false);
		((XLogPageHeader)pages[0].data)->xlp_thread_id = thread;
		save_pages();
		refused();
	}
	prepare(true, false);
	((XLogPageHeader)pages[1].data)->xlp_thread_id = 8;
	save_pages();
	refused();
}

UT_TEST(wrong_sysid_timeline_and_record_refuse)
{
	prepare(false, false);
	((XLogLongPageHeader)pages[0].data)->xlp_sysid++;
	save_pages();
	refused();
	prepare(false, false);
	((XLogPageHeader)pages[0].data)->xlp_tli = 2;
	save_pages();
	refused();
	prepare(false, true);
	refused();
}

UT_TEST(crc_and_control_payload_mismatch_refuse)
{
	prepare(false, false);
	((XLogRecord *)(pages[0].data + SizeOfXLogLongPHD))->xl_scn++;
	save_pages();
	refused();
	prepare(false, false);
	pages[0].data[SizeOfXLogLongPHD + SizeOfXLogRecord + 9] ^= 1;
	save_pages();
	refused();
	prepare(false, false);
	control.checkPointCopy.nextOid++;
	control_crc();
	refused();
	prepare(false, false);
	control.crc++;
	refused();
}

UT_TEST(missing_short_and_alias_files_refuse)
{
	int fd;
	prepare(false, false);
	fd = openat(directory_fd, filename, O_WRONLY);
	UT_ASSERT(fd >= 0 && ftruncate(fd, 2 * XLOG_BLCKSZ) == 0 && close(fd) == 0);
	refused();
	UT_ASSERT(unlinkat(directory_fd, filename, 0) == 0);
	refused();
	prepare(false, false);
	UT_ASSERT(renameat(directory_fd, filename, directory_fd, "saved") == 0);
	UT_ASSERT(symlinkat("saved", directory_fd, filename) == 0);
	refused();
	UT_ASSERT(unlinkat(directory_fd, filename, 0) == 0);
	UT_ASSERT(linkat(directory_fd, "saved", directory_fd, filename, 0) == 0);
	refused();
	UT_ASSERT(unlinkat(directory_fd, "saved", 0) == 0);
}

UT_TEST(nonshutdown_or_wrong_checkpoint_refuses)
{
	prepare(false, false);
	control.state = DB_IN_PRODUCTION;
	control_crc();
	refused();
	prepare(false, false);
	control.checkPoint += MAXIMUM_ALIGNOF;
	control_crc();
	refused();
}

UT_TEST(corrupt_length_is_bounded_before_decoder_allocation)
{
	uint32 length = UINT32_MAX;

	prepare(false, false);
	memcpy(pages[0].data + SizeOfXLogLongPHD, &length, sizeof(length));
	save_pages();
	refused();
	prepare(true, false);
	memcpy(pages[0].data + XLOG_BLCKSZ - 16, &length, sizeof(length));
	save_pages();
	refused();
}

int
main(void)
{
	strlcpy(directory, "/tmp/pgrac-initdb-wal-XXXXXX", sizeof(directory));
	UT_ASSERT(mkdtemp(directory) != NULL);
	directory_fd = open(directory, O_RDONLY | O_DIRECTORY);
	UT_ASSERT(directory_fd >= 0);
	UT_PLAN(8);
	UT_RUN(exact_native_checkpoint);
	UT_RUN(native_checkpoint_spans_pages);
	UT_RUN(wrong_thread_including_legacy_refuses);
	UT_RUN(wrong_sysid_timeline_and_record_refuse);
	UT_RUN(crc_and_control_payload_mismatch_refuse);
	UT_RUN(missing_short_and_alias_files_refuse);
	UT_RUN(nonshutdown_or_wrong_checkpoint_refuses);
	UT_RUN(corrupt_length_is_bounded_before_decoder_allocation);
	UT_ASSERT(unlinkat(directory_fd, filename, 0) == 0 && close(directory_fd) == 0);
	UT_ASSERT(rmdir(directory) == 0);
	UT_DONE();
	return ut_failed_count ? 1 : 0;
}
