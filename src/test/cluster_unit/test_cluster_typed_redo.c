/* Actual cold block consumer and native reader; storage/driver are boundaries.
 * Author: SqlRush <sqlrush@gmail.com> */
#define USE_PGRAC_CLUSTER 1
#include "postgres.h"
#include "cluster/cluster_page_wal.h"
#include <setjmp.h>
#include "catalog/pg_control.h"
#include "access/xlogutils.h"
#include "cluster/cluster_page_cold_redo.h"
#include "cluster/cluster_space_storage.h"
#include "miscadmin.h"
#include "unit_test.h"

UT_DEFINE_GLOBALS();
bool InRecovery = true;
bool cluster_enabled = true;
bool cluster_shared_config = true;
int cluster_node_id;
int NBuffers = 8, NLocBuffer;
BackendType MyBackendType = B_STARTUP;
bool cluster_recmerge_window_active, cluster_recmerge_apply_foreign;
uint64 cluster_recmerge_window_scn, cluster_recmerge_window_own_lsn;
static jmp_buf fail_jump;
static bool refusal_expected;
static unsigned reads, dirty, locks, space_reads, restored;
static bool locked, exists = true, space_ok = true, verdict_ok = true;
static bool cold_write_allowed;
volatile uint32 CritSectionCount;
static ClusterColdRedoBlockV1 verdict;
static ClusterSpaceIdentity space;
static PGAlignedBlock data, image;
static XLogReaderState reader;
static DecodedXLogRecord *decoded;
static RelFileLocator locator = { 1663, 1, 42 };

void
ExceptionalCondition(const char *condition, const char *file, int line)
{
	fprintf(stderr, "%s at %s:%d\n", condition, file, line);
	abort();
}
static void
pg_attribute_noreturn() fail(void)
{
	if (!refusal_expected) {
		fprintf(stderr, "unexpected typed redo refusal\n");
		abort();
	}
	longjmp(fail_jump, 1);
}
#undef ereport
#define ereport(level, rest)                                                                       \
	do {                                                                                           \
		if (false)                                                                                 \
			(void)(rest);                                                                          \
		fail();                                                                                    \
	} while (0)
#undef elog
#define elog(level, ...) fail()
#undef BufferGetPage
#define BufferGetPage(buffer) ((Page)data.data)

bool
cluster_page_wal_cold_redo_write_allowed_v1(void)
{
	return cold_write_allowed;
}
void
cluster_page_wal_cold_redo_fail_v1(void)
{
	UT_ASSERT(locked);
	cold_write_allowed = false;
}
bool
cluster_cold_redo_block_decision_v1(XLogReaderState *r, uint8 block, ClusterColdRedoBlockV1 *out)
{
	UT_ASSERT(r == &reader && block == 0);
	*out = verdict;
	return verdict_ok;
}
bool
cluster_space_relation_read_redo_identity(RelFileLocator loc, ClusterSpaceIdentity *out)
{
	UT_ASSERT(!locked);
	UT_ASSERT(RelFileLocatorEquals(loc, locator));
	space_reads++;
	*out = space;
	return space_ok;
}
bool
XLogRecGetBlockTagExtended(XLogReaderState *r, uint8 block, RelFileLocator *loc,
						   ForkNumber *forknum, BlockNumber *blkno, Buffer *prefetch)
{
	if (r != &reader || block != 0)
		return false;
	*loc = locator;
	*forknum = MAIN_FORKNUM;
	*blkno = 3;
	if (prefetch)
		*prefetch = InvalidBuffer;
	return true;
}
void
BufferGetTag(Buffer buffer, RelFileLocator *loc, ForkNumber *forknum, BlockNumber *blkno)
{
	UT_ASSERT_EQ(buffer, 1);
	*loc = locator;
	*forknum = MAIN_FORKNUM;
	*blkno = 3;
}
Buffer
XLogReadBufferExtended(RelFileLocator loc, ForkNumber forknum, BlockNumber blockno,
					   ReadBufferMode mode, Buffer prefetch)
{
	reads++;
	UT_ASSERT(!locked);
	if (!exists && mode == RBM_NORMAL_NO_LOG)
		return InvalidBuffer;
	exists = true;
	if (mode == RBM_ZERO_AND_LOCK || mode == RBM_ZERO_AND_CLEANUP_LOCK)
		locked = true;
	return 1;
}
void
LockBuffer(Buffer buffer, int mode)
{
	UT_ASSERT_EQ(buffer, 1);
	UT_ASSERT_EQ(mode, BUFFER_LOCK_EXCLUSIVE);
	locks++;
	locked = true;
}
void
LockBufferForCleanup(Buffer buffer)
{
	LockBuffer(buffer, BUFFER_LOCK_EXCLUSIVE);
}
void
MarkBufferDirty(Buffer buffer)
{
	UT_ASSERT(locked);
	cluster_page_cold_redo_dirty_v1(buffer);
	dirty++;
}
void
FlushOneBuffer(Buffer buffer)
{
	UT_ASSERT(false);
}
bool
RestoreBlockImage(XLogReaderState *record, uint8 block, char *page)
{
	restored++;
	memcpy(page, image.data, BLCKSZ);
	return true;
}
#include "../../backend/cluster/cluster_page_cold_redo.c"
#include "test_cluster_typed_redo_reader.inc"

XLogRedoAction
XLogReadBufferForRedo(XLogReaderState *record, uint8 id, Buffer *buffer)
{
	return XLogReadBufferForRedoExtended(record, id, RBM_NORMAL, false, buffer);
}
void
UnlockReleaseBuffer(Buffer buffer)
{
	UT_ASSERT_EQ(buffer, 1);
	UT_ASSERT(locked);
	locked = false;
}
/* Extracted original XLOG_FPI/FPI_FOR_HINT loop, not a replacement consumer. */
#include "test_cluster_typed_redo_fpi.inc"

static void
reset(void)
{
	cluster_page_cold_redo_abort_v1();
	reads = dirty = locks = space_reads = restored = 0;
	CritSectionCount = 0;
	cold_write_allowed = true;
	locked = false;
	exists = space_ok = verdict_ok = true;
	cluster_recmerge_window_active = false;
	memset(&space, 0, sizeof(space));
	space.incarnation[0] = 8;
	memset(&verdict, 0, sizeof(verdict));
	verdict.action = CLUSTER_COLD_REDO_APPLY;
	verdict.expected_kind = CLUSTER_COLD_DATA_PRESENT;
	verdict.expected_before.segment_incarnation[0] = 8;
	verdict.expected_before.mutation_token = 90;
	verdict.result.segment_incarnation[0] = 8;
	verdict.result.mutation_token = 7; /* Opaque, deliberately numerically older. */
	memset(&data, 0, sizeof(data));
	((PageHeader)data.data)->pd_upper = BLCKSZ;
	((PageHeader)data.data)->pd_block_scn = 90;
	PageSetLSN((Page)data.data, 0x9000);
	image = data;
	((PageHeader)image.data)->pd_block_scn = 7;
	memset(&reader, 0, sizeof(reader));
	memset(decoded, 0, offsetof(DecodedXLogRecord, blocks) + sizeof(DecodedBkpBlock));
	reader.record = decoded;
	reader.ReadRecPtr = 0x100;
	reader.EndRecPtr = 0x200;
	decoded->lsn = 0x100;
	decoded->next_lsn = 0x200;
	decoded->header.xl_crc = 9;
	decoded->max_block_id = 0;
	decoded->blocks[0].in_use = true;
	decoded->blocks[0].rlocator = locator;
	decoded->blocks[0].blkno = 3;
	decoded->blocks[0].forknum = MAIN_FORKNUM;
}
#define EXPECT_REFUSED(stmt)                                                                       \
	do {                                                                                           \
		refusal_expected = true;                                                                   \
		if (setjmp(fail_jump) == 0) {                                                              \
			stmt;                                                                                  \
			UT_ASSERT(false);                                                                      \
		}                                                                                          \
		refusal_expected = false;                                                                  \
	} while (0)

UT_TEST(skip_never_opens_storage_or_returns_buffer)
{
	Buffer buffer = 4;
	reset();
	verdict.action = CLUSTER_COLD_REDO_SKIP;
	cluster_page_cold_redo_begin_v1(&reader);
	UT_ASSERT_EQ(XLogReadBufferForRedoExtended(&reader, 0, RBM_NORMAL, false, &buffer),
				 BLK_NOTFOUND);
	UT_ASSERT_EQ(buffer, InvalidBuffer);
	UT_ASSERT_EQ(reads, 0);
	UT_ASSERT_EQ(space_reads, 0);
	cluster_page_cold_redo_end_v1(&reader);
}
UT_TEST(apply_ignores_numeric_lsn_and_scn_and_stamps_under_lock)
{
	Buffer buffer;
	reset();
	cluster_recmerge_window_active = true;
	cluster_recmerge_window_scn = 1;
	cluster_page_cold_redo_begin_v1(&reader);
	UT_ASSERT_EQ(XLogReadBufferForRedoExtended(&reader, 0, RBM_NORMAL, false, &buffer),
				 BLK_NEEDS_REDO);
	UT_ASSERT_EQ(((PageHeader)data.data)->pd_block_scn, 90);
	MarkBufferDirty(buffer);
	UT_ASSERT_EQ(((PageHeader)data.data)->pd_block_scn, 7);
	cluster_page_cold_redo_end_v1(&reader);
}
UT_TEST(expected_token_incarnation_and_completion_must_match)
{
	Buffer buffer;
	reset();
	space.incarnation[0]++;
	EXPECT_REFUSED(cluster_page_cold_redo_begin_v1(&reader));
	UT_ASSERT_EQ(reads, 0);
	reset();
	cluster_page_cold_redo_begin_v1(&reader);
	((PageHeader)data.data)->pd_block_scn++;
	EXPECT_REFUSED(XLogReadBufferForRedoExtended(&reader, 0, RBM_NORMAL, false, &buffer));
	UT_ASSERT_EQ(dirty, 0);
	reset();
	cluster_page_cold_redo_begin_v1(&reader);
	EXPECT_REFUSED(cluster_page_cold_redo_end_v1(&reader));
}
UT_TEST(full_image_checks_result_before_restoring_target)
{
	Buffer buffer;
	reset();
	decoded->blocks[0].has_image = decoded->blocks[0].apply_image = true;
	cluster_page_cold_redo_begin_v1(&reader);
	UT_ASSERT_EQ(XLogReadBufferForRedoExtended(&reader, 0, RBM_NORMAL, true, &buffer),
				 BLK_RESTORED);
	UT_ASSERT_EQ(((PageHeader)data.data)->pd_block_scn, 7);
	UT_ASSERT_EQ(dirty, 1);
	cluster_page_cold_redo_end_v1(&reader);
	reset();
	decoded->blocks[0].has_image = decoded->blocks[0].apply_image = true;
	((PageHeader)image.data)->pd_block_scn = 6;
	cluster_page_cold_redo_begin_v1(&reader);
	EXPECT_REFUSED(XLogReadBufferForRedoExtended(&reader, 0, RBM_NORMAL, false, &buffer));
	UT_ASSERT_EQ(reads, 0);
	UT_ASSERT_EQ(dirty, 0);
}
UT_TEST(initialization_checks_absent_or_all_zero_before_dirty)
{
	Buffer buffer;
	reset();
	exists = false;
	verdict.expected_kind = CLUSTER_COLD_DATA_ABSENT;
	decoded->blocks[0].flags = BKPBLOCK_WILL_INIT;
	cluster_page_cold_redo_begin_v1(&reader);
	UT_ASSERT_EQ(XLogReadBufferForRedoExtended(&reader, 0, RBM_ZERO_AND_LOCK, false, &buffer),
				 BLK_NEEDS_REDO);
	UT_ASSERT_EQ(reads, 2);
	((PageHeader)data.data)->pd_upper = BLCKSZ;
	MarkBufferDirty(buffer);
	UT_ASSERT_EQ(((PageHeader)data.data)->pd_block_scn, 7);
	cluster_page_cold_redo_end_v1(&reader);
	reset();
	verdict.expected_kind = CLUSTER_COLD_DATA_UNFORMATTED;
	verdict.expected_before.mutation_token = 0;
	memset(data.data, 0, BLCKSZ);
	decoded->blocks[0].flags = BKPBLOCK_WILL_INIT;
	cluster_page_cold_redo_begin_v1(&reader);
	UT_ASSERT_EQ(XLogReadBufferForRedoExtended(&reader, 0, RBM_ZERO_AND_LOCK, false, &buffer),
				 BLK_NEEDS_REDO);
	((PageHeader)data.data)->pd_upper = BLCKSZ;
	MarkBufferDirty(buffer);
	UT_ASSERT_EQ(((PageHeader)data.data)->pd_block_scn, 7);
	cluster_page_cold_redo_end_v1(&reader);
	reset();
	verdict.expected_kind = CLUSTER_COLD_DATA_UNFORMATTED;
	verdict.expected_before.mutation_token = 0;
	memset(data.data, 0, BLCKSZ);
	data.data[BLCKSZ - 1] = 1;
	decoded->blocks[0].flags = BKPBLOCK_WILL_INIT;
	cluster_page_cold_redo_begin_v1(&reader);
	EXPECT_REFUSED(XLogReadBufferForRedoExtended(&reader, 0, RBM_ZERO_AND_LOCK, false, &buffer));
}
UT_TEST(unformatted_delta_refuses_before_data_io)
{
	Buffer buffer;
	reset();
	verdict.expected_kind = CLUSTER_COLD_DATA_UNFORMATTED;
	verdict.expected_before.mutation_token = 0;
	memset(data.data, 0, BLCKSZ);
	cluster_page_cold_redo_begin_v1(&reader);
	EXPECT_REFUSED(XLogReadBufferForRedoExtended(&reader, 0, RBM_NORMAL, false, &buffer));
	UT_ASSERT_EQ(reads, 0);
	UT_ASSERT_EQ(dirty, 0);
}
UT_TEST(dirty_violations_are_reported_only_after_critical_section)
{
	for (int fault = 0; fault < 5; fault++) {
		Buffer buffer = 1;
		volatile bool threw = false;
		reset();
		cluster_page_cold_redo_begin_v1(&reader);
		if (fault != 0)
			UT_ASSERT_EQ(XLogReadBufferForRedoExtended(&reader, 0, RBM_NORMAL, false, &buffer),
						 BLK_NEEDS_REDO);
		else
			locked = true; /* An unregistered direct read may not gain a result token. */
		if (fault == 1)
			reader.ReadRecPtr++;
		if (fault == 2)
			verdict.result.mutation_token++;
		if (fault == 3)
			verdict_ok = false;
		if (fault == 4)
			((PageHeader)data.data)->pd_upper = 0;
		CritSectionCount = 1;
		refusal_expected = true;
		if (setjmp(fail_jump) == 0)
			MarkBufferDirty(buffer);
		else
			threw = true;
		refusal_expected = false;
		CritSectionCount = 0;
		UT_ASSERT(!threw);
		UT_ASSERT(!cold_write_allowed);
		UT_ASSERT_EQ(((PageHeader)data.data)->pd_block_scn, 90);
		/* Even if the transient evidence is restored, the violation is sticky. */
		reader.ReadRecPtr = 0x100;
		verdict.result.mutation_token = 7;
		verdict_ok = true;
		((PageHeader)data.data)->pd_upper = BLCKSZ;
		UnlockReleaseBuffer(buffer);
		EXPECT_REFUSED(cluster_page_cold_redo_end_v1(&reader));
		cluster_page_cold_redo_abort_v1();
		UT_ASSERT(!cold_write_allowed); /* Local cleanup cannot reopen shared writes. */
	}
}
UT_TEST(xlog_fpi_skips_without_read_or_release_and_applies_exact_image)
{
	for (int skip = 0; skip < 2; skip++) {
		reset();
		decoded->header.xl_info = XLOG_FPI;
		decoded->blocks[0].has_image = decoded->blocks[0].apply_image = true;
		verdict.action = skip ? CLUSTER_COLD_REDO_SKIP : CLUSTER_COLD_REDO_APPLY;
		exists = !skip;
		cluster_page_cold_redo_begin_v1(&reader);
		refusal_expected = true;
		if (setjmp(fail_jump) == 0)
			test_native_fpi_redo(&reader);
		else
			UT_ASSERT(false);
		refusal_expected = false;
		UT_ASSERT_EQ(reads, skip ? 0 : 1);
		UT_ASSERT_EQ(dirty, skip ? 0 : 1);
		UT_ASSERT(!locked);
		cluster_page_cold_redo_end_v1(&reader);
	}
}
UT_TEST(native_and_wrong_record_keep_their_original_boundaries)
{
	Buffer buffer;
	reset();
	verdict.action = CLUSTER_COLD_REDO_NATIVE;
	UT_ASSERT_EQ(XLogReadBufferForRedoExtended(&reader, 0, RBM_NORMAL, false, &buffer), BLK_DONE);
	UT_ASSERT_EQ(space_reads, 0);
	reset();
	MyBackendType = B_BG_WRITER;
	EXPECT_REFUSED(cluster_page_cold_redo_begin_v1(&reader));
	MyBackendType = B_STARTUP;
	reset();
	cold_write_allowed = false;
	EXPECT_REFUSED(cluster_page_cold_redo_begin_v1(&reader));
	UT_ASSERT_EQ(reads, 0);
	reset();
	verdict_ok = false;
	EXPECT_REFUSED(cluster_page_cold_redo_begin_v1(&reader));
	reset();
	cluster_page_cold_redo_begin_v1(&reader);
	reader.ReadRecPtr++;
	EXPECT_REFUSED(XLogReadBufferForRedoExtended(&reader, 0, RBM_NORMAL, false, &buffer));
}
int
main(void)
{
	decoded = calloc(1, offsetof(DecodedXLogRecord, blocks) + sizeof(DecodedBkpBlock));
	UT_PLAN(9);
	UT_RUN(skip_never_opens_storage_or_returns_buffer);
	UT_RUN(apply_ignores_numeric_lsn_and_scn_and_stamps_under_lock);
	UT_RUN(expected_token_incarnation_and_completion_must_match);
	UT_RUN(full_image_checks_result_before_restoring_target);
	UT_RUN(initialization_checks_absent_or_all_zero_before_dirty);
	UT_RUN(native_and_wrong_record_keep_their_original_boundaries);
	UT_RUN(unformatted_delta_refuses_before_data_io);
	UT_RUN(dirty_violations_are_reported_only_after_critical_section);
	UT_RUN(xlog_fpi_skips_without_read_or_release_and_applies_exact_image);
	free(decoded);
	UT_DONE();
	return ut_failed_count ? 1 : 0;
}
