/*-------------------------------------------------------------------------
 *
 * test_cluster_vm_redo.c
 *    Original heap redo VM consumers with buffer/root boundary fixtures.
 *
 * Portions Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *    src/test/cluster_unit/test_cluster_vm_redo.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <setjmp.h>

#include "access/heapam_xlog.h"
#include "access/visibilitymap.h"
#include "access/xlog.h"
#include "access/xlogutils.h"
#include "cluster/cluster_guc.h"
#include "cluster/cluster_space_storage.h"
#include "cluster/storage/cluster_smgr.h"
#include "miscadmin.h"
#include "storage/bufmgr.h"
#include "storage/smgr.h"
#include "utils/rel.h"
#include "utils/inval.h"
#include "unit_test.h"

UT_DEFINE_GLOBALS();

#define VM_HEAP_BLOCKS ((BLCKSZ - MAXALIGN(SizeOfPageHeaderData)) * 4)
bool cluster_shared_config = true, cluster_enabled = true;
int cluster_node_id, NBuffers = 2, NLocBuffer, wal_level = WAL_LEVEL_REPLICA;
char *BufferBlocks;
Block *LocalBufferBlockPointers;
volatile uint32 CritSectionCount;
bool cluster_recmerge_window_active, cluster_recmerge_apply_foreign;
uint64 cluster_recmerge_window_scn, cluster_recmerge_window_own_lsn;

static PGAlignedBlock pages[2], images[2];
static RelFileLocator locator = { 1663, 5, 16384 };
static SMgrRelationData storage;
static ClusterSpaceIdentity identity;
static XLogReaderState reader;
static DecodedXLogRecord *decoded;
static uint8 payload[64];
static bool pins[2], locks[2], identity_ok;
static unsigned runtime_reads, restart_reads, dirty, fake_count;
static jmp_buf error_jump;
static bool error_ready;

void
ExceptionalCondition(const char *c, const char *f, int l)
{
	printf("# Unexpected assertion %s at %s:%d\n", c, f, l);
	abort();
}
bool
RecoveryInProgress(void)
{
	return true;
}
int
cluster_smgr_which_for(RelFileLocator loc, BackendId backend)
{
	if (!RelFileLocatorEquals(loc, locator) || backend != InvalidBackendId)
		abort();
	return 1;
}

/* Real fake-relcache allocation is linked from xlogutils. The runtime
 * identity boundary refuses exactly that recovery-only Relation. */
bool
cluster_space_relation_get_identity(Relation rel, ClusterSpaceIdentity *out)
{
	(void)out;
	if (RelationGetRelid(rel) != InvalidOid || rel->rd_isvalid || !RecoveryInProgress())
		abort();
	runtime_reads++;
	return false;
}

/* Exact restart namespace decoding is separately exercised against the real
 * SPACE reader. This fixture only supplies its already-validated value. */
bool
cluster_space_relation_read_redo_identity(RelFileLocator loc, ClusterSpaceIdentity *out)
{
	if (!RecoveryInProgress() || !RelFileLocatorEquals(loc, locator) || pins[0] || pins[1])
		abort();
	restart_reads++;
	if (identity_ok)
		*out = identity;
	return identity_ok;
}
bool
cluster_space_init_vm_buffer_wal(const ClusterSpaceIdentity *id, Buffer buf)
{
	(void)id;
	(void)buf;
	abort(); /* Recovery MUST NOT allocate a token or generate WAL. */
}

void *
palloc0(Size size)
{
	fake_count++;
	return calloc(1, size);
}
void
pfree(void *p)
{
	fake_count--;
	free(p);
}
SMgrRelation
smgropen(RelFileLocator loc, BackendId backend)
{
	if (!RelFileLocatorEquals(loc, locator) || backend != InvalidBackendId)
		abort();
	return &storage;
}
void
smgrsetowner(SMgrRelation *owner, SMgrRelation rel)
{
	*owner = rel;
	rel->smgr_owner = owner;
}
void
smgrclearowner(SMgrRelation *owner, SMgrRelation rel)
{
	*owner = NULL;
	rel->smgr_owner = NULL;
}
bool
smgrexists(SMgrRelation rel, ForkNumber forknum)
{
	if (rel != &storage || forknum != VISIBILITYMAP_FORKNUM)
		abort();
	return true;
}
BlockNumber
smgrnblocks(SMgrRelation rel, ForkNumber forknum)
{
	if (rel != &storage || forknum != VISIBILITYMAP_FORKNUM)
		abort();
	return 2;
}
static Buffer
read_vm(RelFileLocator loc, ForkNumber forknum, BlockNumber block)
{
	if (!RelFileLocatorEquals(loc, locator) || forknum != VISIBILITYMAP_FORKNUM || block > 1
		|| pins[block])
		abort();
	pins[block] = true;
	return block + 1;
}
Buffer
ReadBufferExtended(Relation rel, ForkNumber forknum, BlockNumber block, ReadBufferMode mode,
				   BufferAccessStrategy strategy)
{
	if (strategy != NULL || (mode != RBM_NORMAL && mode != RBM_ZERO_ON_ERROR))
		abort();
	return read_vm(rel->rd_locator, forknum, block);
}
Buffer
XLogReadBufferExtended(RelFileLocator loc, ForkNumber forknum, BlockNumber block,
					   ReadBufferMode mode, Buffer recent)
{
	if (mode != RBM_NORMAL || recent != InvalidBuffer)
		abort();
	return read_vm(loc, forknum, block);
}
Buffer
ExtendBufferedRelTo(BufferManagerRelation bmr, ForkNumber forknum, BufferAccessStrategy strategy,
					uint32 flags, BlockNumber blocks, ReadBufferMode mode)
{
	(void)bmr;
	(void)forknum;
	(void)strategy;
	(void)flags;
	(void)blocks;
	(void)mode;
	abort();
}
void
CacheInvalidateSmgr(RelFileLocatorBackend loc)
{
	(void)loc;
	abort();
}
BlockNumber
BufferGetBlockNumber(Buffer buf)
{
	if (buf < 1 || buf > 2 || !pins[buf - 1])
		abort();
	return buf - 1;
}
void
LockBuffer(Buffer buf, int mode)
{
	if (buf < 1 || buf > 2 || !pins[buf - 1])
		abort();
	if (mode == BUFFER_LOCK_EXCLUSIVE && !locks[buf - 1])
		locks[buf - 1] = true;
	else if (mode == BUFFER_LOCK_UNLOCK && locks[buf - 1])
		locks[buf - 1] = false;
	else
		abort();
}
Buffer
LockBufferForVisibilityMapPageInit(Buffer buf)
{
	LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);
	return buf;
}
void
ReleaseBuffer(Buffer buf)
{
	if (buf < 1 || buf > 2 || !pins[buf - 1] || locks[buf - 1])
		abort();
	pins[buf - 1] = false;
}
void
UnlockReleaseBuffer(Buffer buf)
{
	LockBuffer(buf, BUFFER_LOCK_UNLOCK);
	ReleaseBuffer(buf);
}
void
MarkBufferDirty(Buffer buf)
{
	if (buf < 1 || buf > 2 || !pins[buf - 1] || !locks[buf - 1])
		abort();
	dirty++;
}
bool
errstart(int level, const char *domain)
{
	(void)domain;
	if (!error_ready || level < ERROR)
		abort();
	return true;
}
int
errmsg_internal(const char *fmt, ...)
{
	(void)fmt;
	return 0;
}
void
errfinish(const char *f, int l, const char *func)
{
	(void)f;
	(void)l;
	(void)func;
	longjmp(error_jump, 1);
}

#include "test_cluster_vm_redo_consumers.inc"

static void (*consumers[])(XLogReaderState *)
	= { vm_consumer_1, vm_consumer_2, vm_consumer_3, vm_consumer_4,
		vm_consumer_5, vm_consumer_6, vm_consumer_7 };

static uint8
consumer_flags(int which)
{
	switch (which) {
	case 0:
		return XLH_DELETE_ALL_VISIBLE_CLEARED;
	case 1:
		return XLH_INSERT_ALL_VISIBLE_CLEARED;
	case 2:
		return XLH_INSERT_ALL_VISIBLE_CLEARED;
	case 3:
		return XLH_UPDATE_OLD_ALL_VISIBLE_CLEARED;
	case 4:
		return XLH_UPDATE_NEW_ALL_VISIBLE_CLEARED;
	default:
		return XLH_LOCK_ALL_FROZEN_CLEARED;
	}
}
static void
set_flags(int which, uint8 flags)
{
	switch (which) {
	case 0:
		((xl_heap_delete *)payload)->flags = flags;
		break;
	case 1:
		((xl_heap_insert *)payload)->flags = flags;
		break;
	case 2:
		((xl_heap_multi_insert *)payload)->flags = flags;
		break;
	case 3:
	case 4:
		((xl_heap_update *)payload)->flags = flags;
		break;
	case 5:
		((xl_heap_lock *)payload)->flags = flags;
		break;
	case 6:
		((xl_heap_lock_updated *)payload)->flags = flags;
		break;
	}
}
static void
add_image(int id, int block, int flags)
{
	DecodedBkpBlock *b = &decoded->blocks[id];
	RfPageVersionEdgeEntryV1 *e
		= &decoded->page_version_edge.entries[decoded->page_version_edge.entry_count++];
	b->in_use = true;
	b->rlocator = locator;
	b->forknum = VISIBILITYMAP_FORKNUM;
	b->blkno = block;
	b->has_image = b->apply_image = true;
	b->bimg_len = BLCKSZ;
	b->bkp_image = images[block].data;
	b->bimg_info = BKPIMAGE_APPLY;
	b->component_ordinal = id;
	images[block] = pages[block];
	((PageHeader)images[block].data)->pd_block_scn = 200;
	PageGetContents(images[block].data)[0] &= ~flags;
	e->block_id = id;
	e->component_ordinal = id;
	e->page_class = RF_PAGE_CLASS_ORDINARY;
	e->before_kind = e->result_kind = RF_PAGE_STATE_PRESENT;
	e->before.mutation_token = 100;
	memcpy(e->before.segment_incarnation, identity.incarnation, 16);
	memcpy(e->result_incarnation, identity.incarnation, 16);
	e->edge_flags = RF_PAGE_EDGE_FULL_IMAGE_APPLY | RF_PAGE_EDGE_FULL_COVERAGE;
	decoded->max_block_id = id;
}
static void
reset(int which)
{
	int i;
	free(decoded);
	decoded = calloc(1, offsetof(DecodedXLogRecord, blocks) + 4 * sizeof(DecodedBkpBlock));
	memset(&reader, 0, sizeof(reader));
	memset(payload, 0, sizeof(payload));
	memset(&identity, 0, sizeof(identity));
	memset(&storage, 0, sizeof(storage));
	memset(pins, 0, sizeof(pins));
	memset(locks, 0, sizeof(locks));
	identity.incarnation[15] = 7;
	identity.key.locator = locator;
	identity_ok = true;
	storage.smgr_cached_nblocks[VISIBILITYMAP_FORKNUM] = 2;
	BufferBlocks = pages[0].data;
	cluster_shared_config = true;
	runtime_reads = restart_reads = dirty = fake_count = CritSectionCount = 0;
	cluster_recmerge_window_active = cluster_recmerge_apply_foreign = false;
	for (i = 0; i < 2; i++) {
		PageInit(pages[i].data, BLCKSZ, 0);
		((PageHeader)pages[i].data)->pd_block_scn = 100;
		PageGetContents(pages[i].data)[0] = VISIBILITYMAP_VALID_BITS;
	}
	reader.record = decoded;
	reader.EndRecPtr = 0x4000;
	decoded->main_data = (char *)payload;
	decoded->has_page_version_edge = true;
	decoded->page_version_edge.result_token = 200;
	decoded->blocks[0].in_use = true;
	decoded->blocks[0].rlocator = locator;
	decoded->blocks[0].forknum = MAIN_FORKNUM;
	decoded->blocks[0].blkno = 0;
	add_image(2, 0, which >= 5 ? VISIBILITYMAP_ALL_FROZEN : VISIBILITYMAP_VALID_BITS);
	set_flags(which, consumer_flags(which));
}
static bool
run(int which)
{
	bool ok;
	error_ready = true;
	if (setjmp(error_jump) == 0) {
		consumers[which](&reader);
		ok = true;
	} else
		ok = false;
	error_ready = false;
	return ok;
}
static void
positive(int which)
{
	reset(which);
	UT_ASSERT(run(which));
	UT_ASSERT_EQ(runtime_reads, 0);
	UT_ASSERT_EQ(((PageHeader)pages[0].data)->pd_block_scn, 200);
	UT_ASSERT_EQ(PageGetContents(pages[0].data)[0], which >= 5 ? VISIBILITYMAP_ALL_VISIBLE : 0);
	UT_ASSERT_EQ(dirty, 1);
	UT_ASSERT(!pins[0] && !pins[1] && !locks[0] && !locks[1]);
}
UT_TEST(test_delete)
{
	positive(0);
}
UT_TEST(test_insert)
{
	positive(1);
}
UT_TEST(test_multi_insert)
{
	positive(2);
}
UT_TEST(test_update_old)
{
	positive(3);
}
UT_TEST(test_update_new)
{
	positive(4);
}
UT_TEST(test_lock)
{
	positive(5);
}
UT_TEST(test_lock_updated)
{
	positive(6);
}
UT_TEST(test_legacy_native_clear)
{
	reset(0);
	cluster_shared_config = false;
	UT_ASSERT(run(0));
	UT_ASSERT_EQ(PageGetContents(pages[0].data)[0], 0);
	UT_ASSERT_EQ(((PageHeader)pages[0].data)->pd_block_scn, 100);
	UT_ASSERT_EQ(runtime_reads, 0);
	UT_ASSERT_EQ(restart_reads, 0);
	UT_ASSERT_EQ(fake_count, 0);
}

UT_TEST(test_same_vm_update_is_idempotent)
{
	PGAlignedBlock saved;
	reset(3);
	set_flags(3, XLH_UPDATE_OLD_ALL_VISIBLE_CLEARED | XLH_UPDATE_NEW_ALL_VISIBLE_CLEARED);
	UT_ASSERT(run(3));
	saved = pages[0];
	UT_ASSERT(run(4));
	UT_ASSERT_EQ(dirty, 1);
	UT_ASSERT(memcmp(saved.data, pages[0].data, BLCKSZ) == 0);
	UT_ASSERT_EQ(fake_count, 0);
	UT_ASSERT_EQ(runtime_reads, 0);
}

UT_TEST(test_update_different_vm_pages)
{
	reset(3);
	decoded->blocks[1] = decoded->blocks[0];
	decoded->blocks[0].blkno = VM_HEAP_BLOCKS;
	add_image(3, 1, VISIBILITYMAP_VALID_BITS);
	set_flags(3, XLH_UPDATE_OLD_ALL_VISIBLE_CLEARED | XLH_UPDATE_NEW_ALL_VISIBLE_CLEARED);
	UT_ASSERT(run(3));
	UT_ASSERT_EQ(dirty, 1);
	UT_ASSERT_EQ(((PageHeader)pages[1].data)->pd_block_scn, 100);
	UT_ASSERT(run(4));
	UT_ASSERT_EQ(dirty, 2);
	UT_ASSERT_EQ(((PageHeader)pages[1].data)->pd_block_scn, 200);
	UT_ASSERT_EQ(PageGetContents(pages[0].data)[0], 0);
	UT_ASSERT_EQ(PageGetContents(pages[1].data)[0], 0);
	UT_ASSERT(!pins[0] && !pins[1] && !locks[0] && !locks[1]);
}

static void
rejected_unchanged(void)
{
	PGAlignedBlock saved = pages[0];
	UT_ASSERT(!run(0));
	UT_ASSERT(memcmp(saved.data, pages[0].data, BLCKSZ) == 0);
	UT_ASSERT_EQ(dirty, 0);
	UT_ASSERT_EQ(runtime_reads, 0);
	UT_ASSERT(!pins[0] && !pins[1] && !locks[0] && !locks[1]);
}

UT_TEST(test_no_restart_identity_or_wrong_incarnation)
{
	reset(0);
	identity_ok = false;
	rejected_unchanged();
	reset(0);
	identity.incarnation[15]++;
	rejected_unchanged();
	reset(0);
	decoded->page_version_edge.entries[0].before.segment_incarnation[0]++;
	rejected_unchanged();
	reset(0);
	decoded->page_version_edge.entries[0].result_incarnation[0]++;
	rejected_unchanged();
}

UT_TEST(test_missing_or_ambiguous_recorded_image)
{
	reset(0);
	decoded->has_page_version_edge = false;
	rejected_unchanged();
	reset(0);
	decoded->page_version_edge.entry_count = 0;
	rejected_unchanged();
	reset(0);
	decoded->blocks[2].has_image = false;
	rejected_unchanged();
	reset(0);
	decoded->blocks[2].apply_image = false;
	rejected_unchanged();
	reset(0);
	decoded->blocks[2].forknum = MAIN_FORKNUM;
	rejected_unchanged();
	reset(0);
	decoded->blocks[2].rlocator.relNumber++;
	rejected_unchanged();
	reset(0);
	add_image(3, 0, VISIBILITYMAP_VALID_BITS);
	rejected_unchanged();
	reset(0);
	decoded->page_version_edge.entries[1] = decoded->page_version_edge.entries[0];
	decoded->page_version_edge.entry_count++;
	rejected_unchanged();
}

UT_TEST(test_image_shape_token_and_clear_proof)
{
	reset(0);
	((PageHeader)images[0].data)->pd_block_scn++;
	rejected_unchanged();
	reset(0);
	((PageHeader)images[0].data)->pd_flags |= PD_HAS_ITL;
	rejected_unchanged();
	reset(0);
	((PageHeader)images[0].data)->pd_lower++;
	rejected_unchanged();
	reset(0);
	PageGetContents(images[0].data)[0] = VISIBILITYMAP_VALID_BITS;
	rejected_unchanged();
	reset(0);
	decoded->page_version_edge.entries[0].before_kind = RF_PAGE_STATE_UNFORMATTED;
	rejected_unchanged();
	reset(0);
	decoded->page_version_edge.entries[0].component_ordinal++;
	rejected_unchanged();
}

UT_TEST(test_predecessor_is_not_a_numeric_order)
{
	reset(0);
	((PageHeader)pages[0].data)->pd_block_scn = 300;
	rejected_unchanged();
	reset(0);
	((PageHeader)pages[0].data)->pd_block_scn = 99;
	rejected_unchanged();
	reset(0);
	memset(pages[0].data, 0, BLCKSZ);
	rejected_unchanged();
	reset(0);
	((PageHeader)pages[0].data)->pd_special = 0;
	rejected_unchanged();
	reset(0);
	((PageHeader)pages[0].data)->pd_block_scn = 200;
	rejected_unchanged();
}

UT_TEST(test_redo_lsn_hook_cannot_replace_version)
{
	reset(0);
	cluster_recmerge_window_active = true;
	cluster_recmerge_window_scn = 999;
	UT_ASSERT(run(0));
	UT_ASSERT_EQ(((PageHeader)pages[0].data)->pd_block_scn, 200);
	UT_ASSERT_EQ(PageGetLSN(pages[0].data), reader.EndRecPtr);
	UT_ASSERT_EQ(dirty, 1);
	UT_ASSERT(run(0));
	UT_ASSERT_EQ(dirty, 1);
}

UT_TEST(test_no_clear_flag_requires_no_vm_work)
{
	reset(0);
	set_flags(0, 0);
	identity_ok = false;
	decoded->has_page_version_edge = false;
	UT_ASSERT(run(0));
	UT_ASSERT_EQ(dirty, 0);
	UT_ASSERT_EQ(restart_reads, 0);
	UT_ASSERT_EQ(runtime_reads, 0);
}

UT_TEST(test_already_clear_but_versioned_vm_still_replays)
{
	reset(0);
	set_flags(0, 0);
	PageGetContents(pages[0].data)[0] = 0;
	UT_ASSERT(run(0));
	UT_ASSERT_EQ(dirty, 1);
	UT_ASSERT_EQ(((PageHeader)pages[0].data)->pd_block_scn, 200);
	reset(0);
	set_flags(0, 0);
	decoded->blocks[2].in_use = false;
	UT_ASSERT(run(0));
	UT_ASSERT_EQ(restart_reads, 0);
	UT_ASSERT_EQ(dirty, 0);
}

static bool
run_visible(uint8 flags)
{
	bool ok;
	((xl_heap_visible *)payload)->flags = flags;
	error_ready = true;
	if (setjmp(error_jump) == 0)
		ok = visibilitymap_set_versioned_redo(&reader, locator, 0, flags);
	else
		ok = false;
	error_ready = false;
	return ok;
}

static void
reset_visible(void)
{
	DecodedBkpBlock vm;
	reset(0);
	vm = decoded->blocks[2];
	decoded->blocks[1] = decoded->blocks[0];
	decoded->blocks[0] = vm;
	decoded->blocks[0].component_ordinal = 0;
	decoded->blocks[2].in_use = false;
	decoded->max_block_id = 1;
	decoded->page_version_edge.entries[0].block_id = 0;
	decoded->page_version_edge.entries[0].component_ordinal = 0;
	decoded->header.xl_rmid = RM_HEAP2_ID;
	decoded->header.xl_info = XLOG_HEAP2_VISIBLE;
	decoded->main_data_len = SizeOfHeapVisible;
}

UT_TEST(test_visible_exact_image_and_repeat)
{
	for (int frozen = 0; frozen < 2; frozen++) {
		uint8 flags = VISIBILITYMAP_ALL_VISIBLE | (frozen ? VISIBILITYMAP_ALL_FROZEN : 0);
		reset_visible();
		PageGetContents(pages[0].data)[0] = 0;
		PageGetContents(images[0].data)[0] = flags;
		UT_ASSERT(run_visible(flags));
		UT_ASSERT_EQ(PageGetContents(pages[0].data)[0], flags);
		UT_ASSERT_EQ(((PageHeader)pages[0].data)->pd_block_scn, 200);
		UT_ASSERT_EQ(dirty, 1);
		UT_ASSERT(run_visible(flags));
		UT_ASSERT_EQ(dirty, 1);
		UT_ASSERT_EQ(runtime_reads, 0);
		UT_ASSERT_EQ(fake_count, 0);
		UT_ASSERT(!pins[0] && !pins[1] && !locks[0] && !locks[1]);
	}
}

UT_TEST(test_visible_refuses_wrong_result_and_predecessor)
{
	for (int bad = 0; bad < 7; bad++) {
		PGAlignedBlock saved;
		uint8 flags = VISIBILITYMAP_VALID_BITS;
		reset_visible();
		PageGetContents(pages[0].data)[0] = 0;
		PageGetContents(images[0].data)[0] = flags;
		if (bad == 0)
			PageGetContents(images[0].data)[0] = VISIBILITYMAP_ALL_VISIBLE;
		else if (bad == 1)
			((PageHeader)pages[0].data)->pd_block_scn = 300;
		else if (bad == 2)
			flags = VISIBILITYMAP_ALL_FROZEN;
		else if (bad == 3)
			flags = 0;
		else if (bad == 4)
			flags = 128;
		else if (bad == 5)
			decoded->header.xl_info = XLOG_HEAP2_FREEZE_PAGE;
		else
			decoded->blocks[1].blkno = VM_HEAP_BLOCKS;
		saved = pages[0];
		UT_ASSERT(!run_visible(flags));
		UT_ASSERT(memcmp(saved.data, pages[0].data, BLCKSZ) == 0);
		UT_ASSERT_EQ(dirty, 0);
		UT_ASSERT(!pins[0] && !locks[0]);
	}
}

int
main(void)
{
	UT_PLAN(19);
	UT_RUN(test_delete);
	UT_RUN(test_insert);
	UT_RUN(test_multi_insert);
	UT_RUN(test_update_old);
	UT_RUN(test_update_new);
	UT_RUN(test_lock);
	UT_RUN(test_lock_updated);
	UT_RUN(test_legacy_native_clear);
	UT_RUN(test_same_vm_update_is_idempotent);
	UT_RUN(test_update_different_vm_pages);
	UT_RUN(test_no_restart_identity_or_wrong_incarnation);
	UT_RUN(test_missing_or_ambiguous_recorded_image);
	UT_RUN(test_image_shape_token_and_clear_proof);
	UT_RUN(test_predecessor_is_not_a_numeric_order);
	UT_RUN(test_redo_lsn_hook_cannot_replace_version);
	UT_RUN(test_no_clear_flag_requires_no_vm_work);
	UT_RUN(test_already_clear_but_versioned_vm_still_replays);
	UT_RUN(test_visible_exact_image_and_repeat);
	UT_RUN(test_visible_refuses_wrong_result_and_predecessor);
	UT_DONE();
	return ut_failed_count ? 1 : 0;
}
