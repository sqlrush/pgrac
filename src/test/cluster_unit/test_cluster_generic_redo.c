/*-------------------------------------------------------------------------
 *
 * test_cluster_generic_redo.c
 *    Generic cleanup replay uses exact versions, not numeric LSN ordering.
 *
 * Portions Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *    src/test/cluster_unit/test_cluster_generic_redo.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"
#include "cluster/cluster_page_wal.h"
#include <setjmp.h>
#include "access/generic_xlog.h"
#include "access/xlogutils.h"
#include "cluster/cluster_guc.h"
#include "cluster/cluster_page_cold_redo.h"
#include "cluster/cluster_space_storage.h"
#include "miscadmin.h"
#include "storage/bufmgr.h"
#include "unit_test.h"

UT_DEFINE_GLOBALS();
bool cluster_shared_config = true;
bool cluster_enabled = true, InRecovery = true;
BackendType MyBackendType = B_STARTUP;
int NBuffers = 4, NLocBuffer, cluster_node_id;
char *BufferBlocks;
Block *LocalBufferBlockPointers;
volatile uint32 CritSectionCount;
bool cluster_recmerge_window_active, cluster_recmerge_apply_foreign;
uint64 cluster_recmerge_window_scn, cluster_recmerge_window_own_lsn;

static PGAlignedBlock pages[4], images[4], original[4];
static uint8 deltas[4][32];
static XLogReaderState reader;
static DecodedXLogRecord *decoded;
static bool pins[4], locks[4], identity_ok;
static unsigned old_reads, exact_reads, dirty, identity_reads;
static bool error_ready;
static jmp_buf error_jump;
static bool cold_published;
static unsigned skip_mask;
bool
cluster_page_wal_cold_redo_write_allowed_v1(void)
{
	return true;
}
void
cluster_page_wal_cold_redo_fail_v1(void)
{
	abort(); /* This fixture exercises only qualified native dirty publication. */
}

bool
cluster_cold_redo_block_decision_v1(XLogReaderState *record, uint8 id, ClusterColdRedoBlockV1 *out)
{
	memset(out, 0, sizeof(*out));
	if (!cold_published) {
		out->action = CLUSTER_COLD_REDO_NATIVE;
		return true;
	}
	UT_ASSERT(record == &reader && id <= decoded->max_block_id);
	out->action = (skip_mask & (1U << id)) ? CLUSTER_COLD_REDO_SKIP : CLUSTER_COLD_REDO_APPLY;
	out->expected_kind = CLUSTER_COLD_DATA_PRESENT;
	out->expected_before = decoded->page_version_edge.entries[id].before;
	memcpy(out->result.segment_incarnation,
		   decoded->page_version_edge.entries[id].result_incarnation, 16);
	out->result.mutation_token = decoded->page_version_edge.result_token;
	return true;
}

#include "test_cluster_typed_redo_reader.inc"

void
ExceptionalCondition(const char *c, const char *f, int l)
{
	printf("# assertion %s at %s:%d\n", c, f, l);
	abort();
}

bool
RecoveryInProgress(void)
{
	return true;
}

bool
cluster_space_relation_read_redo_identity(RelFileLocator locator, ClusterSpaceIdentity *out)
{
	unsigned i;
	for (i = 0; i < 4; i++)
		if (pins[i] || locks[i])
			abort();
	identity_reads++;
	memset(out, 0, sizeof(*out));
	out->key.locator = locator;
	out->state = CLUSTER_SPACE_IDENTITY_LIVE;
	memset(out->incarnation, 33, 16);
	return identity_ok;
}

Buffer
XLogReadBufferExtended(RelFileLocator loc, ForkNumber forknum, BlockNumber block,
					   ReadBufferMode mode, Buffer recent)
{
	if (loc.relNumber != 900 || forknum != MAIN_FORKNUM || block >= 4
		|| (mode != RBM_NORMAL && mode != RBM_NORMAL_NO_LOG) || recent != InvalidBuffer
		|| pins[block])
		abort();
	/* Skipped blocks are deliberately unavailable, not an alternate read path. */
	if (cold_published && (skip_mask & (1U << block)))
		return InvalidBuffer;
	pins[block] = true;
	exact_reads++;
	return block + 1;
}

/* Characterizes the old read-for-redo boundary for a numerically newer LSN;
 * shared replay must bypass this decision, not reinterpret BLK_DONE. */
XLogRedoAction
XLogReadBufferForRedo(XLogReaderState *record, uint8 id, Buffer *buffer)
{
	(void)record;
	if (cold_published)
		return XLogReadBufferForRedoExtended(record, id, RBM_NORMAL, false, buffer);
	old_reads++;
	pins[id] = locks[id] = true;
	*buffer = id + 1;
	return BLK_DONE;
}

void
LockBuffer(Buffer buffer, int mode)
{
	if (!pins[buffer - 1] || mode != BUFFER_LOCK_EXCLUSIVE || locks[buffer - 1])
		abort();
	locks[buffer - 1] = true;
}

void
LockBufferForCleanup(Buffer buffer)
{
	LockBuffer(buffer, BUFFER_LOCK_EXCLUSIVE);
}

void
BufferGetTag(Buffer buffer, RelFileLocator *locator, ForkNumber *forknum, BlockNumber *blockno)
{
	UT_ASSERT(buffer >= 1 && buffer <= 4);
	*locator = decoded->blocks[buffer - 1].rlocator;
	*forknum = MAIN_FORKNUM;
	*blockno = buffer - 1;
}

void
FlushOneBuffer(Buffer buffer)
{
	abort();
}

void
UnlockReleaseBuffer(Buffer buffer)
{
	if (buffer < 1 || buffer > 4)
		abort();
	if (!pins[buffer - 1] || !locks[buffer - 1])
		abort();
	pins[buffer - 1] = locks[buffer - 1] = false;
}

void
MarkBufferDirty(Buffer buffer)
{
	if (!pins[buffer - 1] || !locks[buffer - 1])
		abort();
	cluster_page_cold_redo_dirty_v1(buffer);
	dirty++;
}

bool
errstart(int level, const char *domain)
{
	(void)domain;
	if ((level == ERROR || level == FATAL) && error_ready && CritSectionCount == 0)
		return true;
	abort();
}

int
errmsg_internal(const char *format, ...)
{
	(void)format;
	if (error_ready)
		longjmp(error_jump, 1);
	abort();
}

int
errmsg(const char *format, ...)
{
	return 0;
}
int
errdetail(const char *format, ...)
{
	UT_ASSERT_EQ(CritSectionCount, 0);
	return 0;
}
void
errfinish(const char *file, int line, const char *func)
{
	if (error_ready)
		longjmp(error_jump, 1);
	abort();
}

int
errcode(int code)
{
	(void)code;
	return 0;
}

static void
reset(int nblocks, bool image)
{
	int i;
	cluster_page_cold_redo_abort_v1();
	cold_published = false;
	skip_mask = 0;
	cluster_recmerge_window_active = cluster_recmerge_apply_foreign = false;
	free(decoded);
	decoded = calloc(1, offsetof(DecodedXLogRecord, blocks) + 4 * sizeof(DecodedBkpBlock));
	memset(&reader, 0, sizeof(reader));
	memset(pages, 0, sizeof(pages));
	memset(pins, 0, sizeof(pins));
	memset(locks, 0, sizeof(locks));
	reader.record = decoded;
	reader.ReadRecPtr = 600;
	reader.EndRecPtr = 800;
	decoded->header.xl_rmid = RM_GENERIC_ID;
	decoded->max_block_id = nblocks - 1;
	decoded->has_page_version_edge = true;
	decoded->page_version_edge.result_token = 200;
	decoded->page_version_edge.entry_count = nblocks;
	BufferBlocks = pages[0].data;
	for (i = 0; i < nblocks; i++) {
		PageHeader page = (PageHeader)pages[i].data;
		DecodedBkpBlock *b = &decoded->blocks[i];
		RfPageVersionEdgeEntryV1 *e = &decoded->page_version_edge.entries[i];
		uint16 off = offsetof(PageHeaderData, pd_block_scn), len = sizeof(uint64);
		uint64 result = 200;

		page->pd_lower = SizeOfPageHeaderData + 16;
		page->pd_upper = BLCKSZ - 16;
		page->pd_special = BLCKSZ;
		page->pd_pagesize_version = BLCKSZ | PG_PAGE_LAYOUT_VERSION;
		page->pd_block_scn = 20 + i;
		PageSetLSN(pages[i].data, 9000); /* another origin's numeric coordinate */
		images[i] = pages[i];
		((PageHeader)images[i].data)->pd_block_scn = 200;
		images[i].data[SizeOfPageHeaderData] = 77;
		b->in_use = true;
		b->rlocator = (RelFileLocator){ 1663, 5, 900 };
		b->forknum = MAIN_FORKNUM;
		b->blkno = i;
		b->component_ordinal = i;
		b->has_image = b->apply_image = image;
		b->bimg_len = BLCKSZ;
		b->bkp_image = images[i].data;
		b->bimg_info = image ? BKPIMAGE_APPLY : 0;
		memcpy(deltas[i], &off, 2);
		memcpy(deltas[i] + 2, &len, 2);
		memcpy(deltas[i] + 4, &result, 8);
		off = SizeOfPageHeaderData;
		len = 1;
		memcpy(deltas[i] + 12, &off, 2);
		memcpy(deltas[i] + 14, &len, 2);
		deltas[i][16] = 77;
		b->has_data = !image;
		b->data_len = image ? 0 : 17;
		b->data = (char *)deltas[i];
		e->block_id = i;
		e->component_ordinal = i;
		e->page_class = RF_PAGE_CLASS_ORDINARY;
		e->before_kind = e->result_kind = RF_PAGE_STATE_PRESENT;
		e->before.mutation_token = 20 + i;
		memset(e->before.segment_incarnation, 33, 16);
		memset(e->result_incarnation, 33, 16);
		e->edge_flags = image ? RF_PAGE_EDGE_FULL_IMAGE_APPLY | RF_PAGE_EDGE_FULL_COVERAGE : 0;
	}
	memcpy(original, pages, sizeof(pages));
	old_reads = exact_reads = dirty = identity_reads = CritSectionCount = 0;
	identity_ok = true;
	cluster_shared_config = true;
	cluster_recmerge_window_active = false;
}

static bool
replay_refuses(void)
{
	bool rejected;
	error_ready = true;
	if (setjmp(error_jump) == 0) {
		generic_redo(&reader);
		rejected = false;
	} else
		rejected = true;
	error_ready = false;
	return rejected;
}

UT_TEST(delta_replays_exact_before_even_with_greater_lsn)
{
	reset(2, false);
	generic_redo(&reader);
	UT_ASSERT_EQ(old_reads, 0);
	UT_ASSERT_EQ(dirty, 2);
	UT_ASSERT_EQ(((PageHeader)pages[0].data)->pd_block_scn, 200);
	UT_ASSERT_EQ(pages[1].data[SizeOfPageHeaderData], 77);
	UT_ASSERT_EQ(PageGetLSN(pages[0].data), 800);
}

UT_TEST(fpi_cannot_overwrite_unrelated_predecessor)
{
	reset(2, true);
	((PageHeader)pages[1].data)->pd_block_scn = 88;
	memcpy(original, pages, sizeof(pages));
	UT_ASSERT(replay_refuses());
	UT_ASSERT_EQ(dirty, 0);
	UT_ASSERT(memcmp(original, pages, sizeof(pages)) == 0);
}

UT_TEST(missing_version_edge_refuses)
{
	reset(1, false);
	decoded->has_page_version_edge = false;
	UT_ASSERT(replay_refuses());
	UT_ASSERT_EQ(exact_reads, 0);
}

UT_TEST(exact_result_is_idempotent_for_delta_and_fpi)
{
	int image;
	for (image = 0; image < 2; image++) {
		reset(2, image);
		generic_redo(&reader);
		UT_ASSERT_EQ(dirty, 2);
		cluster_recmerge_window_active = true;
		cluster_recmerge_window_scn = 9999;
		cluster_recmerge_apply_foreign = true;
		cluster_recmerge_window_own_lsn = 6000;
		generic_redo(&reader);
		UT_ASSERT_EQ(dirty, 2);
		UT_ASSERT_EQ(((PageHeader)pages[1].data)->pd_block_scn, 200);
		UT_ASSERT(!pins[0] && !pins[1] && !locks[0] && !locks[1]);
	}
	cluster_recmerge_window_active = cluster_recmerge_apply_foreign = false;
}

UT_TEST(result_token_with_conflicting_cleanup_bytes_refuses)
{
	int image;
	for (image = 0; image < 2; image++) {
		reset(1, image);
		pages[0] = images[0];
		pages[0].data[SizeOfPageHeaderData] = 88;
		memcpy(original, pages, sizeof(pages));
		UT_ASSERT(replay_refuses());
		UT_ASSERT_EQ(dirty, 0);
		UT_ASSERT(!pins[0] && !locks[0]);
		UT_ASSERT(memcmp(original, pages, sizeof(pages)) == 0);
	}
}

UT_TEST(malformed_or_unproven_record_never_changes_first_page)
{
	int fault;
	for (fault = 0; fault < 12; fault++) {
		reset(2, false);
		switch (fault) {
		case 0:
			identity_ok = false;
			break;
		case 1:
			decoded->page_version_edge.entries[1].result_incarnation[0]++;
			break;
		case 2:
			decoded->page_version_edge.entries[1].before.segment_incarnation[0]++;
			break;
		case 3:
			decoded->page_version_edge.entries[1].component_ordinal++;
			break;
		case 4:
			decoded->blocks[1].blkno = 0;
			break;
		case 5:
			decoded->blocks[1].data_len = 16;
			break;
		case 6:
			deltas[1][2] = 255;
			break;
		case 7:
			decoded->page_version_edge.result_token = 0;
			break;
		case 8:
			decoded->blocks[1].flags |= BKPBLOCK_WILL_INIT;
			break;
		case 9:
			decoded->page_version_edge.entry_count--;
			break;
		case 10:
			((PageHeader)pages[1].data)->pd_upper = 1;
			break;
		case 11:
			deltas[1][4] = 88;
			break; /* result token absent from delta */
		}
		memcpy(original, pages, sizeof(pages));
		UT_ASSERT(replay_refuses());
		UT_ASSERT_EQ(dirty, 0);
		UT_ASSERT(!pins[0] && !pins[1] && !locks[0] && !locks[1]);
		UT_ASSERT(memcmp(original, pages, sizeof(pages)) == 0);
	}
}

UT_TEST(nonshared_replay_keeps_native_path)
{
	reset(1, false);
	cluster_shared_config = false;
	generic_redo(&reader);
	UT_ASSERT_EQ(old_reads, 1);
	UT_ASSERT_EQ(exact_reads, 0);
	UT_ASSERT_EQ(dirty, 0);
}

UT_TEST(cold_generic_mixed_skips_use_no_storage_and_apply_other_blocks)
{
	for (int image = 0; image < 2; image++) {
		for (unsigned mask = 0; mask < 16; mask++) {
			reset(4, image);
			skip_mask = mask;
			cold_published = true;
			for (int i = 0; i < 4; i++)
				if (mask & (1U << i))
					memset(pages[i].data, 0x55, BLCKSZ);
			memcpy(original, pages, sizeof(pages));
			cluster_page_cold_redo_begin_v1(&reader);
			identity_reads = 0; /* begin is the sole pre-DATA SPACE proof. */
			UT_ASSERT(!replay_refuses());
			UT_ASSERT_EQ(identity_reads, 0);
			for (int i = 0; i < 4; i++) {
				UT_ASSERT(!pins[i] && !locks[i]);
				if (mask & (1U << i))
					UT_ASSERT(memcmp(pages[i].data, original[i].data, BLCKSZ) == 0);
				else {
					UT_ASSERT_EQ(((PageHeader)pages[i].data)->pd_block_scn, 200);
					UT_ASSERT_EQ(pages[i].data[SizeOfPageHeaderData], 77);
				}
			}
			/* Only validate completion after a successful native replay. */
			if (dirty == 4 - __builtin_popcount(mask))
				cluster_page_cold_redo_end_v1(&reader);
			UT_ASSERT_EQ(dirty, 4 - __builtin_popcount(mask));
			UT_ASSERT_EQ(exact_reads, dirty);
		}
	}
}

UT_TEST(cold_generic_requires_bracket_and_validates_all_wal_before_io)
{
	reset(2, false);
	cold_published = true;
	UT_ASSERT(replay_refuses());
	UT_ASSERT_EQ(exact_reads, 0);
	reset(2, false);
	cold_published = true;
	cluster_page_cold_redo_begin_v1(&reader);
	decoded->blocks[1].data_len = 16;
	UT_ASSERT(replay_refuses());
	UT_ASSERT_EQ(exact_reads, 0);
	UT_ASSERT_EQ(dirty, 0);
}

int
main(void)
{
	UT_PLAN(9);
	UT_RUN(delta_replays_exact_before_even_with_greater_lsn);
	UT_RUN(fpi_cannot_overwrite_unrelated_predecessor);
	UT_RUN(missing_version_edge_refuses);
	UT_RUN(exact_result_is_idempotent_for_delta_and_fpi);
	UT_RUN(result_token_with_conflicting_cleanup_bytes_refuses);
	UT_RUN(malformed_or_unproven_record_never_changes_first_page);
	UT_RUN(nonshared_replay_keeps_native_path);
	UT_RUN(cold_generic_mixed_skips_use_no_storage_and_apply_other_blocks);
	UT_RUN(cold_generic_requires_bracket_and_validates_all_wal_before_io);
	free(decoded);
	UT_DONE();
	return ut_failed_count != 0;
}
