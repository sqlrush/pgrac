#ifndef TEST_CLUSTER_D0_NATIVE_READER_FIXTURE_H
#define TEST_CLUSTER_D0_NATIVE_READER_FIXTURE_H

/* Native completed-reader D0 raw custody; synthetic WAL on a real local FD.
 * No source/ROOT/cut/graph authority or live redo admission is supplied. */
#include "postgres.h"
#include "access/rmgr.h"
#include "access/xlog_internal.h"
#include "access/xlogreader.h"
#include "catalog/pg_control.h"
#include "cluster/cluster_recovery_ctrc_record.h"
#include "port/pg_crc32c.h"
#include <fcntl.h>
#include <unistd.h>
#include "unit_test.h"

#define SEG_BYTES (16 * 1024 * 1024)
#define SEG_START ((XLogRecPtr)SEG_BYTES)
#define RAW_BYTES (SizeOfXLogRecord + SizeOfXLogRecordDataHeaderLong + CLUSTER_RECOVERY_CTRC_BYTES)
#define SYSTEM_ID UINT64_C(0x1020304050607080)

typedef struct NativeFixture {
	int fd;
	XLogReaderState *reader;
	XLogRecPtr first, second;
	uint8 raw[3][RAW_BYTES];
} NativeFixture;

static ClusterRecoveryCTRCReleaseV2
sample(void)
{
	ClusterRecoveryCTRCReleaseV2 r = { 0 };
	r.version = 2;
	r.length = 320;
	r.flags = 1;
	r.root_id = UINT64_C(0x0102030405060708);
	r.root_generation = UINT64_C(0x1112131415161718);
	r.formation_epoch = UINT64_C(0x21222324);
	r.recovery_serial = UINT64_C(0x3132333435363738);
	r.database_incarnation = UINT64_C(0x4142434445464748);
	r.writer_boot = UINT64_C(0x5152535455565758);
	r.covered_history = UINT64_C(0x6162636465666768);
	r.highest_fpi_end = UINT64_C(0x7172737475767778);
	r.cohort_bitmap = 5;
	r.origin_node_id = 1;
	r.writer_thread = 3;
	r.target_owner = 2;
	r.slot_offset = 5;
	r.wrap = 9;
	r.terminal_outcome = TT_SLOT_COMMITTED;
	r.segment_id = 257;
	r.segment_generation = 0;
	r.xid = 0x400102;
	r.before
		= (TTSlot){ .xid = r.xid,
					.wrap = r.wrap,
					.status = TT_SLOT_COMMITTED,
					.commit_scn = UINT64_C(0x8182838485868788),
					.first_undo_block = { .raw = { UINT64_C(0x300000102), UINT64_C(0x1000005) } } };
	r.after = r.before;
	r.after.flags = TT_SLOT_FLAG_CTRC_RELEASE_PROVEN;
	for (unsigned i = 0; i < 32; i++) {
		r.graph_sha256[i] = i + 1;
		r.input_sha256[i] = i + 33;
	}
	return r;
}

static void
seal_raw(uint8 raw[RAW_BYTES])
{
	XLogRecord h;
	pg_crc32c crc;
	memcpy(&h, raw, SizeOfXLogRecord);
	INIT_CRC32C(crc);
	COMP_CRC32C(crc, raw + SizeOfXLogRecord, RAW_BYTES - SizeOfXLogRecord);
	COMP_CRC32C(crc, &h, offsetof(XLogRecord, xl_crc));
	FIN_CRC32C(crc);
	h.xl_crc = crc;
	memcpy(raw, &h, SizeOfXLogRecord);
}

static void
make_raw(uint8 raw[RAW_BYTES], uint8 info, RmgrId rmid, TransactionId xid,
	XLogRecPtr previous, bool valid_payload)
{
	ClusterRecoveryCTRCReleaseV2 value = sample();
	XLogRecord header = { 0 };
	uint32 length = CLUSTER_RECOVERY_CTRC_BYTES;
	memset(raw, 0, RAW_BYTES);
	header.xl_tot_len = RAW_BYTES; header.xl_prev = previous;
	header.xl_rmid = rmid; header.xl_info = info; header.xl_xid = xid;
	memcpy(raw, &header, SizeOfXLogRecord);
	raw[SizeOfXLogRecord] = XLR_BLOCK_ID_DATA_LONG;
	memcpy(raw + SizeOfXLogRecord + 1, &length, sizeof(length));
	if (valid_payload)
		UT_ASSERT(cluster_recovery_ctrc_encode_v2(&value, SYSTEM_ID,
			raw + SizeOfXLogRecord + SizeOfXLogRecordDataHeaderLong,
			CLUSTER_RECOVERY_CTRC_BYTES));
	seal_raw(raw);
}

static int
page_read(XLogReaderState *reader, XLogRecPtr page, int required,
	XLogRecPtr target, char *out)
{
	NativeFixture *f = reader->private_data;
	ssize_t n;
	(void)target;
	reader->seg.ws_tli = 1;
	if (page < SEG_START || page - SEG_START >= 2 * XLOG_BLCKSZ)
		return -1;
	n = pread(f->fd, out, XLOG_BLCKSZ, (off_t)(page - SEG_START));
	return n >= required ? (int)n : -1;
}

static bool
open_fixture(NativeFixture *f, uint8 info, TransactionId xid, bool valid_payload)
{
	uint8 pages[2 * XLOG_BLCKSZ] = { 0 };
	XLogLongPageHeader long_header = (XLogLongPageHeader)pages;
	XLogPageHeader next_header = (XLogPageHeader)(pages + XLOG_BLCKSZ);
	const size_t first_part = 128;
	const size_t tail = RAW_BYTES - first_part;
	size_t second;
	char file[] = "/tmp/pgrac-d0-native-reader-XXXXXX";
	memset(f, 0, sizeof(*f)); f->fd = -1;
	f->first = SEG_START + XLOG_BLCKSZ - first_part;
	second = XLOG_BLCKSZ + SizeOfXLogShortPHD + MAXALIGN(tail);
	f->second = SEG_START + second;
	make_raw(f->raw[0], info, RM_CLUSTER_UNDO_ID, xid, InvalidXLogRecPtr, valid_payload);
	make_raw(f->raw[1], CLUSTER_RECOVERY_CTRC_WAL_INFO, RM_CLUSTER_UNDO_ID,
		InvalidTransactionId, f->first, true);
	make_raw(f->raw[2], XLOG_NOOP, RM_XLOG_ID, InvalidTransactionId, f->second, true);
	long_header->std.xlp_magic = XLOG_PAGE_MAGIC;
	long_header->std.xlp_info = XLP_LONG_HEADER;
	long_header->std.xlp_tli = 1; long_header->std.xlp_pageaddr = SEG_START;
	long_header->std.xlp_thread_id = 3;
	long_header->std.xlp_cluster_flags = XLP_CLUSTER_FLAGS_RESERVED;
	long_header->xlp_sysid = SYSTEM_ID; long_header->xlp_seg_size = SEG_BYTES;
	long_header->xlp_xlog_blcksz = XLOG_BLCKSZ;
	next_header->xlp_magic = XLOG_PAGE_MAGIC;
	next_header->xlp_info = XLP_FIRST_IS_CONTRECORD;
	next_header->xlp_tli = 1; next_header->xlp_pageaddr = SEG_START + XLOG_BLCKSZ;
	next_header->xlp_rem_len = tail; next_header->xlp_thread_id = 3;
	next_header->xlp_cluster_flags = XLP_CLUSTER_FLAGS_RESERVED;
	memcpy(pages + XLOG_BLCKSZ - first_part, f->raw[0], first_part);
	memcpy(pages + XLOG_BLCKSZ + SizeOfXLogShortPHD, f->raw[0] + first_part, tail);
	memcpy(pages + second, f->raw[1], RAW_BYTES);
	memcpy(pages + second + MAXALIGN(RAW_BYTES), f->raw[2], RAW_BYTES);
	f->fd = mkstemp(file);
	if (f->fd < 0) return false;
	(void)unlink(file);
	if (pwrite(f->fd, pages, sizeof(pages), 0) != sizeof(pages)) {
		close(f->fd); f->fd = -1; return false;
	}
	f->reader = XLogReaderAllocate(SEG_BYTES, NULL, XL_ROUTINE(.page_read = page_read), f);
	if (f->reader == NULL) { close(f->fd); f->fd = -1; return false; }
	f->reader->system_identifier = SYSTEM_ID;
	f->reader->cluster_expected_thread_id = 3;
	XLogBeginRead(f->reader, f->first);
	return true;
}

static void
close_fixture(NativeFixture *f)
{
	if (f->reader != NULL) XLogReaderFree(f->reader);
	if (f->fd >= 0) close(f->fd);
}


#endif
