/*-------------------------------------------------------------------------
 *
 * test_cluster_btree_redo.c
 *    Detached btree effects compared with actual native redo bodies.
 *
 * Portions Copyright (c) 1996-2023, PostgreSQL Global Development Group
 * Portions Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *    src/test/cluster_unit/test_cluster_btree_redo.c
 *
 * NOTES
 *    Buffer/WAL-reader fixtures bound the native code; real page helpers
 *    construct the bytes. This is not whole-record recovery qualification.
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/nbtree.h"
#include "access/nbtxlog.h"
#include "access/xlogutils.h"
#include "cluster/cluster_block_apply.h"
#include "storage/standby.h"
#include "unit_test.h"

UT_DEFINE_GLOBALS();
#define TEST_BLOCKS 5
int cluster_node_id, NBuffers = TEST_BLOCKS, NLocBuffer;
HotStandbyState standbyState = STANDBY_DISABLED;
char *BufferBlocks;
Block *LocalBufferBlockPointers;
bool cluster_recmerge_window_active, cluster_recmerge_apply_foreign;
uint64 cluster_recmerge_window_scn, cluster_recmerge_window_own_lsn;
static PGAlignedBlock pages[TEST_BLOCKS], original[TEST_BLOCKS], output, main_data,
	payload[TEST_BLOCKS];
static bool locks[TEST_BLOCKS];
static unsigned skip_mask, restored_mask, data_reads[TEST_BLOCKS];
static unsigned dirties, releases, page_comparisons, rejection_checks;
static XLogReaderState reader;
static DecodedXLogRecord *decoded;
static void *allocations[1024];
static unsigned allocation_count;

void *
palloc(Size size)
{
	void *p = malloc(size);
	UT_ASSERT(p != NULL && allocation_count < lengthof(allocations));
	allocations[allocation_count++] = p;
	return p;
}
void *
palloc0(Size size)
{
	void *p = palloc(size);
	memset(p, 0, size);
	return p;
}
void
pfree(void *p)
{
	for (unsigned i = 0; i < allocation_count; i++)
		if (allocations[i] == p) {
			allocations[i] = NULL;
			free(p);
			return;
		}
	abort();
}

void
ExceptionalCondition(const char *c, const char *f, int l)
{
	printf("# unexpected %s %s:%d\n", c, f, l);
	abort();
}
bool
RestoreBlockImage(XLogReaderState *record, uint8 id, char *page)
{
	(void)record;
	(void)id;
	(void)page;
	abort();
}
ClusterBlkApplyResult
cluster_block_apply_heap(XLogReaderState *record, uint8 id, char *page)
{
	(void)record;
	(void)id;
	(void)page;
	abort();
}
char *
XLogRecGetBlockData(XLogReaderState *record, uint8 id, Size *length)
{
	DecodedBkpBlock *block = XLogRecGetBlock(record, id);
	if (length != NULL)
		*length = block->data_len;
	return block->has_data ? block->data : NULL;
}
XLogRedoAction
XLogReadBufferForRedo(XLogReaderState *record, uint8 id, Buffer *buffer)
{
	UT_ASSERT(record == &reader && id < TEST_BLOCKS && decoded->blocks[id].in_use && !locks[id]);
	if (skip_mask & (1U << id)) {
		*buffer = InvalidBuffer;
		return BLK_NOTFOUND;
	}
	data_reads[id]++;
	*buffer = id + 1;
	locks[id] = true;
	return (restored_mask & (1U << id)) ? BLK_RESTORED : BLK_NEEDS_REDO;
}
XLogRedoAction
XLogReadBufferForRedoExtended(XLogReaderState *record, uint8 id, ReadBufferMode mode, bool cleanup,
							  Buffer *buffer)
{
	UT_ASSERT((mode == RBM_NORMAL && cleanup) || (mode == RBM_ZERO_AND_LOCK && !cleanup));
	return XLogReadBufferForRedo(record, id, buffer);
}
void
ResolveRecoveryConflictWithSnapshot(TransactionId xid, bool catalog, RelFileLocator loc)
{
	(void)xid;
	(void)catalog;
	(void)loc;
	abort(); /* This constructor differential does not claim standby admission. */
}
Buffer
XLogInitBufferForRedo(XLogReaderState *record, uint8 id)
{
	Buffer buffer;
	(void)XLogReadBufferForRedo(record, id, &buffer);
	return buffer;
}
BlockNumber
BufferGetBlockNumber(Buffer buffer)
{
	UT_ASSERT(buffer >= 1 && buffer <= TEST_BLOCKS && locks[buffer - 1]);
	return decoded->blocks[buffer - 1].blkno;
}
void
MarkBufferDirty(Buffer buffer)
{
	UT_ASSERT(buffer >= 1 && buffer <= TEST_BLOCKS && locks[buffer - 1]);
	dirties++;
}
void
UnlockReleaseBuffer(Buffer buffer)
{
	UT_ASSERT(buffer >= 1 && buffer <= TEST_BLOCKS && locks[buffer - 1]);
	locks[buffer - 1] = false;
	releases++;
}
bool
XLogRecGetBlockTagExtended(XLogReaderState *record, uint8 id, RelFileLocator *loc, ForkNumber *fork,
						   BlockNumber *blk, Buffer *prefetch)
{
	const DecodedBkpBlock *b;
	if (!XLogRecHasBlockRef(record, id))
		return false;
	b = XLogRecGetBlock(record, id);
	if (loc)
		*loc = b->rlocator;
	if (fork)
		*fork = b->forknum;
	if (blk)
		*blk = b->blkno;
	if (prefetch)
		*prefetch = InvalidBuffer;
	return true;
}
void
XLogRecGetBlockTag(XLogReaderState *record, uint8 id, RelFileLocator *loc, ForkNumber *fork,
				   BlockNumber *blk)
{
	UT_ASSERT(XLogRecGetBlockTagExtended(record, id, loc, fork, blk, NULL));
}

static bool _bt_posting_valid(IndexTuple posting);
#include "test_cluster_btree_copytuple.inc"
#include "test_cluster_btree_posting.inc"
#include "test_cluster_btree_pageinit.inc"
#include "test_cluster_btree_redo_native.inc"

static void
reset(uint8 opcode)
{
	free(decoded);
	for (unsigned i = 0; i < allocation_count; i++)
		free(allocations[i]);
	allocation_count = 0;
	decoded
		= calloc(1, offsetof(DecodedXLogRecord, blocks) + TEST_BLOCKS * sizeof(DecodedBkpBlock));
	memset(&reader, 0, sizeof(reader));
	memset(&main_data, 0, sizeof(main_data));
	memset(payload, 0, sizeof(payload));
	memset(locks, 0, sizeof(locks));
	reader.record = decoded;
	reader.EndRecPtr = 800;
	decoded->header.xl_rmid = RM_BTREE_ID;
	decoded->header.xl_info = opcode;
	decoded->main_data = main_data.data;
	decoded->max_block_id = TEST_BLOCKS - 1;
	BufferBlocks = pages[0].data;
	dirties = releases = skip_mask = restored_mask = 0;
	memset(data_reads, 0, sizeof(data_reads));
	for (int i = 0; i < TEST_BLOCKS; i++) {
		DecodedBkpBlock *b = &decoded->blocks[i];
		b->rlocator = (RelFileLocator){ 1663, 5, 900 };
		b->forknum = MAIN_FORKNUM;
		b->blkno = i == 0 ? 2 : i == 1 ? 1 : i + 2;
		b->data = payload[i].data;
		_bt_pageinit(original[i].data, BLCKSZ);
		BTPageGetOpaque(original[i].data)->btpo_flags = BTP_LEAF;
		((PageHeader)original[i].data)->pd_block_scn = 100 + i;
		pages[i] = original[i];
	}
}

static void
metadata(uint8 id)
{
	xl_btree_metadata meta;
	memset(&meta, 0, sizeof(meta));
	meta.version = BTREE_VERSION;
	meta.root = meta.fastroot = 2;
	meta.level = meta.fastlevel = 1;
	meta.last_cleanup_num_delpages = 9;
	meta.allequalimage = true;
	memcpy(payload[id].data, &meta, sizeof(meta));
	decoded->blocks[id].in_use = true;
	decoded->blocks[id].has_data = true;
	decoded->blocks[id].data_len = sizeof(meta);
	decoded->blocks[id].flags = BKPBLOCK_WILL_INIT;
	decoded->blocks[id].blkno = BTREE_METAPAGE;
}

static Size
tuple(char *destination, uint64 key)
{
	IndexTupleData itup;
	memset(&itup, 0, sizeof(itup));
	itup.t_info = sizeof(itup) + sizeof(key);
	ItemPointerSet(&itup.t_tid, key, 1);
	memcpy(destination, &itup, sizeof(itup));
	memcpy(destination + sizeof(itup), &key, sizeof(key));
	return sizeof(itup) + sizeof(key);
}


/* Replay the same physical record with independently selected block verdicts. */
static void
redo_current(void)
{
	uint8 info = XLogRecGetInfo(&reader) & ~XLR_INFO_MASK;
	switch (info) {
	case XLOG_BTREE_INSERT_LEAF:
	case XLOG_BTREE_INSERT_UPPER:
	case XLOG_BTREE_INSERT_META:
	case XLOG_BTREE_INSERT_POST:
		btree_xlog_insert(info == XLOG_BTREE_INSERT_LEAF || info == XLOG_BTREE_INSERT_POST,
						  info == XLOG_BTREE_INSERT_META, info == XLOG_BTREE_INSERT_POST, &reader);
		break;
	case XLOG_BTREE_SPLIT_L:
	case XLOG_BTREE_SPLIT_R:
		btree_xlog_split(info == XLOG_BTREE_SPLIT_L, &reader);
		break;
	case XLOG_BTREE_MARK_PAGE_HALFDEAD:
		btree_xlog_mark_page_halfdead(info, &reader);
		break;
	case XLOG_BTREE_UNLINK_PAGE:
	case XLOG_BTREE_UNLINK_PAGE_META:
		btree_xlog_unlink_page(info, &reader);
		break;
	case XLOG_BTREE_NEWROOT:
		btree_xlog_newroot(&reader);
		break;
	case XLOG_BTREE_META_CLEANUP:
		_bt_restore_meta(&reader, 0);
		break;
	case XLOG_BTREE_DEDUP:
		btree_xlog_dedup(&reader);
		break;
	case XLOG_BTREE_VACUUM:
		btree_xlog_vacuum(&reader);
		break;
	case XLOG_BTREE_DELETE:
		btree_xlog_delete(&reader);
		break;
	default:
		abort();
	}
}
static void
compare_mixed_verdicts(void)
{
	PGAlignedBlock expected[TEST_BLOCKS], skipped;
	memcpy(expected, pages, sizeof(expected));
	memset(&skipped, 0xA5, BLCKSZ);
	for (unsigned selected = 0; selected < TEST_BLOCKS; selected++) {
		if (!decoded->blocks[selected].in_use)
			continue;
		for (unsigned restored = 0; restored < 2; restored++) {
			memcpy(pages, original, sizeof(pages));
			skip_mask = restored ? 0 : 1U << selected;
			restored_mask = restored ? 1U << selected : 0;
			pages[selected] = restored ? expected[selected] : skipped;
			memset(data_reads, 0, sizeof(data_reads));
			dirties = releases = 0;
			redo_current();
			for (unsigned i = 0; i < TEST_BLOCKS; i++) {
				UT_ASSERT(!locks[i]);
				if (!decoded->blocks[i].in_use)
					continue;
				UT_ASSERT(memcmp(pages[i].data,
								 i == selected && !restored ? skipped.data : expected[i].data,
								 BLCKSZ)
						  == 0);
				UT_ASSERT_EQ(data_reads[i], i == selected && !restored ? 0 : 1);
			}
		}
	}
	skip_mask = restored_mask = 0;
}
static void
compare_all(void)
{
	for (int i = 0; i < TEST_BLOCKS; i++) {
		ClusterBlkApplyResult result;
		UT_ASSERT(!locks[i]);
		if (!decoded->blocks[i].in_use)
			continue;
		output = original[i];
		result = cluster_block_apply_one(&reader, i, output.data);
		UT_ASSERT_EQ(result, CLUSTER_BLKAPPLY_OK);
		if (result == CLUSTER_BLKAPPLY_OK) {
			UT_ASSERT(memcmp(pages[i].data, output.data, BLCKSZ) == 0);
			page_comparisons++;
		}
	}
	UT_ASSERT_EQ(dirties, releases);
	compare_mixed_verdicts();
}

UT_TEST(test_native_retail_insert_and_child_meta)
{
	for (int kind = 0; kind < 3; kind++) {
		reset(kind == 0	  ? XLOG_BTREE_INSERT_LEAF
			  : kind == 1 ? XLOG_BTREE_INSERT_UPPER
						  : XLOG_BTREE_INSERT_META);
		((xl_btree_insert *)main_data.data)->offnum = 1;
		decoded->main_data_len = SizeOfBtreeInsert;
		decoded->blocks[0].in_use = decoded->blocks[0].has_data = true;
		decoded->blocks[0].data_len = tuple(payload[0].data, 42);
		if (kind > 0) {
			decoded->blocks[1].in_use = true;
			BTPageGetOpaque(original[0].data)->btpo_flags = 0;
			BTPageGetOpaque(original[0].data)->btpo_level = 1;
			BTPageGetOpaque(original[1].data)->btpo_flags |= BTP_INCOMPLETE_SPLIT;
			pages[0] = original[0];
			pages[1] = original[1];
		}
		if (kind == 2)
			metadata(2);
		btree_xlog_insert(kind == 0, kind == 2, false, &reader);
		compare_all();
	}
}

UT_TEST(test_native_meta_cleanup_and_newroot)
{
	reset(XLOG_BTREE_META_CLEANUP);
	metadata(0);
	_bt_restore_meta(&reader, 0);
	compare_all();
	for (int level = 0; level < 2; level++) {
		xl_btree_newroot rec;
		reset(XLOG_BTREE_NEWROOT);
		memset(&rec, 0, sizeof(rec));
		rec.rootblk = 2;
		rec.level = level;
		memcpy(main_data.data, &rec, SizeOfBtreeNewroot);
		decoded->main_data_len = SizeOfBtreeNewroot;
		decoded->blocks[0].in_use = true;
		decoded->blocks[0].flags = BKPBLOCK_WILL_INIT;
		if (level > 0) {
			Size one = tuple(payload[0].data, 45);
			decoded->blocks[0].data_len = one + tuple(payload[0].data + one, 42);
			decoded->blocks[0].has_data = true;
			decoded->blocks[1].in_use = true;
			BTPageGetOpaque(original[1].data)->btpo_flags |= BTP_INCOMPLETE_SPLIT;
			pages[1] = original[1];
		}
		metadata(2);
		btree_xlog_newroot(&reader);
		compare_all();
	}
}

static void
retail_fixture(void)
{
	reset(XLOG_BTREE_INSERT_LEAF);
	((xl_btree_insert *)main_data.data)->offnum = FirstOffsetNumber;
	decoded->main_data_len = SizeOfBtreeInsert;
	decoded->blocks[0].in_use = decoded->blocks[0].has_data = true;
	decoded->blocks[0].data_len = tuple(payload[0].data, 42);
}

static void
reject_unchanged(uint8 id)
{
	output = original[id];
	UT_ASSERT_EQ(cluster_block_apply_one(&reader, id, output.data), CLUSTER_BLKAPPLY_FAILED);
	UT_ASSERT(memcmp(output.data, original[id].data, BLCKSZ) == 0);
	rejection_checks++;
}

static Size
posting_tuple(char *destination)
{
	Size keysize = tuple(destination, 42);
	IndexTuple itup = (IndexTuple)destination;
	Size size = MAXALIGN(keysize + 3 * sizeof(ItemPointerData));
	itup->t_info = size;
	BTreeTupleSetPosting(itup, 3, keysize);
	for (int i = 0; i < 3; i++)
		ItemPointerSet(BTreeTupleGetPostingN(itup, i), 20 + 20 * i, 1);
	return size;
}

UT_TEST(test_native_posting_insert)
{
	for (int offset = 1; offset < 3; offset++) {
		PGAlignedBlock old;
		Size size;
		uint16 postingoff = offset;
		retail_fixture();
		decoded->header.xl_info = XLOG_BTREE_INSERT_POST;
		((xl_btree_insert *)main_data.data)->offnum = 2;
		size = posting_tuple(old.data);
		UT_ASSERT_EQ(PageAddItem(original[0].data, old.data, size, 1, false, false), 1);
		memcpy(payload[0].data, &postingoff, sizeof(postingoff));
		decoded->blocks[0].data_len
			= sizeof(postingoff) + tuple(payload[0].data + sizeof(postingoff), 10 + 20 * offset);
		pages[0] = original[0];
		btree_xlog_insert(true, false, true, &reader);
		compare_all();
	}
}

static void
split_fixture(bool left, bool sibling, uint32 level, bool posting)
{
	PGAlignedBlock item;
	xl_btree_split rec = { 0 };
	Size length = 0, size;
	OffsetNumber min = sibling ? 2 : 1;
	BTPageOpaque opaque;

	reset(left ? XLOG_BTREE_SPLIT_L : XLOG_BTREE_SPLIT_R);
	rec.level = level;
	rec.firstrightoff = min + 2;
	rec.newitemoff = left ? min + 1 : min + 3;
	if (posting) {
		rec.postingoff = 1;
		rec.newitemoff = min + 1;
		if (!left)
			rec.firstrightoff = rec.newitemoff;
	}
	memcpy(main_data.data, &rec, SizeOfBtreeSplit);
	decoded->main_data_len = SizeOfBtreeSplit;
	decoded->blocks[0].in_use = decoded->blocks[0].has_data = true;
	decoded->blocks[1].in_use = decoded->blocks[1].has_data = true;
	decoded->blocks[1].flags = BKPBLOCK_WILL_INIT;
	decoded->blocks[2].in_use = sibling;
	decoded->blocks[3].in_use = level != 0;
	opaque = BTPageGetOpaque(original[0].data);
	opaque->btpo_flags = level == 0 ? BTP_LEAF : 0;
	opaque->btpo_level = level;
	opaque->btpo_prev = 77;
	opaque->btpo_next = sibling ? decoded->blocks[2].blkno : P_NONE;
	opaque->btpo_cycleid = 7;
	if (sibling) {
		size = tuple(item.data, 900);
		UT_ASSERT_EQ(PageAddItem(original[0].data, item.data, size, P_HIKEY, false, false),
					 P_HIKEY);
		BTPageGetOpaque(original[2].data)->btpo_flags = level == 0 ? BTP_LEAF : 0;
		BTPageGetOpaque(original[2].data)->btpo_level = level;
		BTPageGetOpaque(original[2].data)->btpo_prev = decoded->blocks[0].blkno;
	}
	for (int i = 0; i < 4; i++) {
		size = posting && i == 0 ? posting_tuple(item.data) : tuple(item.data, 20 + 80 * i);
		UT_ASSERT_EQ(PageAddItem(original[0].data, item.data, size, min + i, false, false),
					 min + i);
	}
	if (left || posting)
		length += tuple(payload[0].data, 30);
	length += tuple(payload[0].data + length, 180);
	decoded->blocks[0].data_len = length;
	/* Native right-image payload uses reverse physical tuple order. */
	length = tuple(payload[1].data, 260);
	length += tuple(payload[1].data + length, 180);
	if (!left)
		length += tuple(payload[1].data + length, 200);
	if (sibling)
		length += tuple(payload[1].data + length, 900);
	decoded->blocks[1].data_len = length;
	if (level != 0) {
		BTPageGetOpaque(original[3].data)->btpo_flags
			= BTP_INCOMPLETE_SPLIT | (level == 1 ? BTP_LEAF : 0);
		BTPageGetOpaque(original[3].data)->btpo_level = level - 1;
	}
	memcpy(pages, original, sizeof(pages));
}

UT_TEST(test_native_split_left_right_leaf_internal)
{
	for (int left = 0; left < 2; left++)
		for (int sibling = 0; sibling < 2; sibling++)
			for (int level = 0; level < 3; level++) {
				split_fixture(left, sibling, level, false);
				btree_xlog_split(left, &reader);
				compare_all();
			}
	for (int left = 0; left < 2; left++)
		for (int sibling = 0; sibling < 2; sibling++) {
			split_fixture(left, sibling, 0, true);
			btree_xlog_split(left, &reader);
			compare_all();
		}
}

UT_TEST(test_native_halfdead_parent_and_leaf)
{
	PGAlignedBlock item;
	xl_btree_mark_page_halfdead rec = { 0 };
	reset(XLOG_BTREE_MARK_PAGE_HALFDEAD);
	rec.poffset = 1;
	rec.leafblk = 2;
	rec.leftblk = 17;
	rec.rightblk = 19;
	rec.topparent = InvalidBlockNumber;
	memcpy(main_data.data, &rec, SizeOfBtreeMarkPageHalfDead);
	decoded->main_data_len = SizeOfBtreeMarkPageHalfDead;
	decoded->blocks[0].in_use = decoded->blocks[1].in_use = true;
	decoded->blocks[0].flags = BKPBLOCK_WILL_INIT;
	BTPageGetOpaque(original[1].data)->btpo_flags = 0;
	BTPageGetOpaque(original[1].data)->btpo_level = 1;
	for (int i = 0; i < 3; i++) {
		Size size = tuple(item.data, 10 + 10 * i);
		UT_ASSERT_EQ(PageAddItem(original[1].data, item.data, size, i + 1, false, false), i + 1);
	}
	pages[1] = original[1];
	btree_xlog_mark_page_halfdead(XLOG_BTREE_MARK_PAGE_HALFDEAD, &reader);
	compare_all();
}

static void
unlink_fixture(bool left, bool meta, uint32 level)
{
	xl_btree_unlink_page rec = { 0 };
	reset(meta ? XLOG_BTREE_UNLINK_PAGE_META : XLOG_BTREE_UNLINK_PAGE);
	rec.leftsib = left ? decoded->blocks[1].blkno : P_NONE;
	rec.rightsib = decoded->blocks[2].blkno;
	rec.level = level;
	rec.safexid = FullTransactionIdFromEpochAndXid(5, 20);
	rec.leafleftsib = 32;
	rec.leafrightsib = 34;
	rec.leaftopparent = level > 1 ? 30 : InvalidBlockNumber;
	memcpy(main_data.data, &rec, SizeOfBtreeUnlinkPage);
	decoded->main_data_len = SizeOfBtreeUnlinkPage;
	decoded->blocks[0].in_use = decoded->blocks[2].in_use = true;
	decoded->blocks[0].flags = BKPBLOCK_WILL_INIT;
	decoded->blocks[1].in_use = left;
	decoded->blocks[3].in_use = level > 0;
	decoded->blocks[3].flags = BKPBLOCK_WILL_INIT;
	for (int i = 1; i <= 2; i++) {
		BTPageGetOpaque(original[i].data)->btpo_flags = level == 0 ? BTP_LEAF : 0;
		BTPageGetOpaque(original[i].data)->btpo_level = level;
	}
	BTPageGetOpaque(original[1].data)->btpo_next = decoded->blocks[0].blkno;
	BTPageGetOpaque(original[2].data)->btpo_prev = decoded->blocks[0].blkno;
	if (meta)
		metadata(4);
	memcpy(pages, original, sizeof(pages));
}

UT_TEST(test_native_unlink_siblings_leaf_and_meta)
{
	for (int left = 0; left < 2; left++)
		for (int meta = 0; meta < 2; meta++)
			for (int level = 0; level < 3; level++) {
				unlink_fixture(left, meta, level);
				btree_xlog_unlink_page(decoded->header.xl_info, &reader);
				compare_all();
			}
}

static void
halfdead_sibling(uint8 id)
{
	IndexTupleData high = { 0 };
	BTPageOpaque opaque = BTPageGetOpaque(original[id].data);
	opaque->btpo_flags = BTP_LEAF | BTP_HALF_DEAD;
	opaque->btpo_level = 0;
	if (P_RIGHTMOST(opaque))
		opaque->btpo_next = 99;
	high.t_info = sizeof(high);
	BTreeTupleSetTopParent(&high, InvalidBlockNumber);
	UT_ASSERT_EQ(PageAddItem(original[id].data, (Item)&high, sizeof(high), P_HIKEY, false, false),
				 P_HIKEY);
	pages[id] = original[id];
}

UT_TEST(test_native_link_update_keeps_halfdead_sibling)
{
	for (int left = 0; left < 2; left++) {
		split_fixture(left, true, 0, false);
		halfdead_sibling(2);
		btree_xlog_split(left, &reader);
		compare_all();
	}
	for (int id = 1; id <= 2; id++) {
		unlink_fixture(true, false, 0);
		halfdead_sibling(id);
		btree_xlog_unlink_page(decoded->header.xl_info, &reader);
		compare_all();
	}
	for (int bad = 0; bad < 4; bad++) {
		split_fixture(false, true, 0, false);
		halfdead_sibling(2);
		if (bad == 0)
			BTPageGetOpaque(original[2].data)->btpo_flags |= BTP_DELETED;
		if (bad == 1)
			BTPageGetOpaque(original[2].data)->btpo_flags &= ~BTP_LEAF;
		if (bad == 2)
			BTPageGetOpaque(original[2].data)->btpo_flags |= BTP_ROOT;
		if (bad == 3)
			((PageHeader)original[2].data)->pd_lower = SizeOfPageHeaderData;
		reject_unchanged(2);
	}
}

UT_TEST(test_wal_posting_array_bounds_unchanged)
{
	/* Valid outer length cannot certify an out-of-bounds inner posting list. */
	for (int split = 0; split < 2; split++) {
		uint8 id = split ? 1 : 0;
		if (split)
			split_fixture(false, false, 0, false);
		else
			retail_fixture();
		BTreeTupleSetPosting((IndexTuple)payload[id].data, 2, 4096);
		reject_unchanged(id);
	}
	for (int split = 0; split < 2; split++)
		for (int bad = 0; bad < 7; bad++) {
			uint8 id = split ? 1 : 0;
			IndexTuple posting;
			if (split)
				split_fixture(false, false, 0, false);
			else
				retail_fixture();
			decoded->blocks[id].data_len = posting_tuple(payload[id].data);
			posting = (IndexTuple)payload[id].data;
			if (bad == 0)
				BTreeTupleSetPosting(posting, BT_OFFSET_MASK, 16);
			if (bad == 1)
				BTreeTupleSetPosting(posting, 3, 0);
			if (bad == 2)
				ItemPointerSet(BTreeTupleGetPostingN(posting, 2), 20, 1);
			if (bad == 3)
				ItemPointerSetInvalid(BTreeTupleGetPostingN(posting, 1));
			if (bad == 4)
				posting->t_info = (posting->t_info & ~INDEX_SIZE_MASK) | 24;
			if (bad == 5)
				ItemPointerSetBlockNumber(&posting->t_tid, 17);
			if (bad == 6)
				ItemPointerSetOffsetNumber(&posting->t_tid, 1 | BT_IS_POSTING);
			/* Codec cannot assume WAL payload alignment before checking it. */
			memmove(payload[id].data + 1, payload[id].data, decoded->blocks[id].data_len);
			decoded->blocks[id].data = payload[id].data + 1;
			reject_unchanged(id);
		}
}

UT_TEST(test_native_wal_posting_images_and_unaligned_decode)
{
	for (int split = 0; split < 2; split++)
		for (int unaligned = 0; unaligned < 2; unaligned++) {
			uint8 id = split ? 1 : 0;
			if (split)
				split_fixture(false, false, 0, false);
			else
				retail_fixture();
			decoded->blocks[id].data_len = posting_tuple(payload[id].data);
			if (split)
				btree_xlog_split(false, &reader);
			else
				btree_xlog_insert(true, false, false, &reader);
			if (unaligned) {
				memmove(payload[id].data + 1, payload[id].data, decoded->blocks[id].data_len);
				decoded->blocks[id].data = payload[id].data + 1;
			}
			compare_all();
		}
}

static void
maintenance_page(bool sibling)
{
	PGAlignedBlock item;
	OffsetNumber min = sibling ? 2 : 1;
	BTPageOpaque opaque = BTPageGetOpaque(original[0].data);
	opaque->btpo_next = sibling ? 9 : P_NONE;
	opaque->btpo_flags |= BTP_HAS_GARBAGE;
	if (sibling) {
		Size size = tuple(item.data, 900);
		UT_ASSERT_EQ(PageAddItem(original[0].data, item.data, size, P_HIKEY, false, false),
					 P_HIKEY);
	}
	for (int i = 0; i < 5; i++) {
		Size size = i == 0 || i == 2 ? posting_tuple(item.data) : tuple(item.data, 80 + 50 * i);
		if (i == 2)
			for (int j = 0; j < 3; j++)
				ItemPointerSet(BTreeTupleGetPostingN((IndexTuple)item.data, j), 100 + 20 * j, 1);
		UT_ASSERT_EQ(PageAddItem(original[0].data, item.data, size, min + i, false, false),
					 min + i);
	}
	pages[0] = original[0];
	decoded->blocks[0].in_use = decoded->blocks[0].has_data = true;
}

UT_TEST(test_native_delete_vacuum_posting_and_offsets)
{
	for (int vacuum = 0; vacuum < 2; vacuum++)
		for (int sibling = 0; sibling < 2; sibling++)
			for (int mode = 0; mode < 3; mode++) {
				OffsetNumber min = sibling ? 2 : 1;
				uint16 ndeleted = mode == 1 ? 0 : 2, nupdated = mode == 0 ? 0 : 2;
				uint16 words[12];
				unsigned n = 0;
				reset(vacuum ? XLOG_BTREE_VACUUM : XLOG_BTREE_DELETE);
				maintenance_page(sibling);
				if (vacuum) {
					xl_btree_vacuum rec = { ndeleted, nupdated };
					memcpy(main_data.data, &rec, SizeOfBtreeVacuum);
					decoded->main_data_len = SizeOfBtreeVacuum;
				} else {
					xl_btree_delete rec = { 0 };
					rec.ndeleted = ndeleted;
					rec.nupdated = nupdated;
					rec.snapshotConflictHorizon = 100;
					rec.isCatalogRel = false;
					memcpy(main_data.data, &rec, SizeOfBtreeDelete);
					decoded->main_data_len = SizeOfBtreeDelete;
				}
				if (ndeleted) {
					words[n++] = min + 1;
					words[n++] = min + 4;
				}
				if (nupdated) {
					words[n++] = min;
					words[n++] = min + 2;
					words[n++] = 2;
					words[n++] = 0;
					words[n++] = 2; /* first posting -> single TID */
					words[n++] = 1;
					words[n++] = 1; /* second remains a posting */
				}
				memcpy(payload[0].data, words, n * sizeof(uint16));
				decoded->blocks[0].data_len = n * sizeof(uint16);
				if (vacuum)
					btree_xlog_vacuum(&reader);
				else
					btree_xlog_delete(&reader);
				compare_all();
			}
}

static void
dedup_fixture(bool sibling, bool posting)
{
	PGAlignedBlock item;
	OffsetNumber min = sibling ? 2 : 1;
	xl_btree_dedup rec = { 2 };
	BTDedupInterval intervals[2] = { { min, 3 }, { min + 3, 2 } };
	reset(XLOG_BTREE_DEDUP);
	BTPageGetOpaque(original[0].data)->btpo_next = sibling ? 9 : P_NONE;
	BTPageGetOpaque(original[0].data)->btpo_flags |= BTP_HAS_GARBAGE;
	if (sibling) {
		Size size = tuple(item.data, 900);
		UT_ASSERT_EQ(PageAddItem(original[0].data, item.data, size, P_HIKEY, false, false),
					 P_HIKEY);
	}
	for (int i = 0; i < 5; i++) {
		uint64 key = i < 3 ? 42 : 84;
		Size size = i == 0 && posting ? posting_tuple(item.data) : tuple(item.data, 20 + 80 * i);
		memcpy(item.data + sizeof(IndexTupleData), &key, sizeof(key));
		UT_ASSERT_EQ(PageAddItem(original[0].data, item.data, size, min + i, false, false),
					 min + i);
	}
	pages[0] = original[0];
	memcpy(main_data.data, &rec, SizeOfBtreeDedup);
	decoded->main_data_len = SizeOfBtreeDedup;
	memcpy(payload[0].data, intervals, sizeof(intervals));
	decoded->blocks[0].in_use = decoded->blocks[0].has_data = true;
	decoded->blocks[0].data_len = sizeof(intervals);
}

UT_TEST(test_native_dedup_groups_with_existing_posting)
{
	for (int sibling = 0; sibling < 2; sibling++)
		for (int posting = 0; posting < 2; posting++) {
			dedup_fixture(sibling, posting);
			btree_xlog_dedup(&reader);
			compare_all();
		}
}

UT_TEST(test_split_and_posting_malformed_unchanged)
{
	for (int bad = 0; bad < 18; bad++) {
		xl_btree_split *rec;
		uint8 id = 0;
		split_fixture(true, true, 0, true);
		rec = (xl_btree_split *)main_data.data;
		switch (bad) {
		case 0:
			decoded->main_data_len--;
			break;
		case 1:
			rec->firstrightoff = 0;
			break;
		case 2:
			rec->firstrightoff = MaxOffsetNumber;
			break;
		case 3:
			rec->newitemoff = 0;
			break;
		case 4:
			rec->newitemoff = 6;
			break;
		case 5:
			rec->postingoff = 3;
			break;
		case 6:
			decoded->blocks[0].data_len--;
			break;
		case 7:
			((IndexTuple)payload[0].data)->t_info = 0;
			break;
		case 8:
			ItemPointerSet(&((IndexTuple)payload[0].data)->t_tid, 40, 1);
			break;
		case 9:
			decoded->blocks[1].flags = 0;
			break;
		case 10:
			decoded->blocks[1].rlocator.relNumber++;
			break;
		case 11:
			decoded->blocks[1].blkno = BTREE_METAPAGE;
			break;
		case 12:
			decoded->blocks[1].data_len--;
			id = 1;
			break;
		case 13:
			decoded->blocks[1].data_len = 0;
			id = 1;
			break;
		case 14:
			BTPageGetOpaque(original[0].data)->btpo_next++;
			break;
		case 15:
			BTPageGetOpaque(original[2].data)->btpo_prev++;
			id = 2;
			break;
		case 16:
			decoded->blocks[2].data_len = 1;
			id = 2;
			break;
		case 17:
			rec->level = 1;
			break;
		}
		reject_unchanged(id);
	}
	for (int bad = 0; bad < 12; bad++) {
		PGAlignedBlock old;
		Size size;
		uint16 postingoff = 1;
		IndexTuple posting;
		retail_fixture();
		decoded->header.xl_info = XLOG_BTREE_INSERT_POST;
		((xl_btree_insert *)main_data.data)->offnum = 2;
		size = posting_tuple(old.data);
		UT_ASSERT_EQ(PageAddItem(original[0].data, old.data, size, 1, false, false), 1);
		posting = (IndexTuple)PageGetItem(original[0].data, PageGetItemId(original[0].data, 1));
		memcpy(payload[0].data, &postingoff, sizeof(postingoff));
		decoded->blocks[0].data_len
			= sizeof(postingoff) + tuple(payload[0].data + sizeof(postingoff), 30);
		switch (bad) {
		case 0:
			payload[0].data[0] = payload[0].data[1] = 0;
			break;
		case 1:
			postingoff = 3;
			memcpy(payload[0].data, &postingoff, sizeof(postingoff));
			break;
		case 2:
			decoded->blocks[0].data_len = 1;
			break;
		case 3:
			((xl_btree_insert *)main_data.data)->offnum = 1;
			break;
		case 4:
			((xl_btree_insert *)main_data.data)->offnum = 3;
			break;
		case 5:
			ItemPointerSetBlockNumber(&posting->t_tid, BLCKSZ);
			break;
		case 6:
			ItemPointerSetBlockNumber(&posting->t_tid, 15);
			break;
		case 7:
			ItemPointerSetOffsetNumber(&posting->t_tid, BT_IS_POSTING | 1);
			break;
		case 8:
			ItemPointerSet(BTreeTupleGetPostingN(posting, 2), 40, 1);
			break;
		case 9:
			posting->t_info &= ~INDEX_ALT_TID_MASK;
			break;
		case 10:
			ItemPointerSetOffsetNumber(BTreeTupleGetPostingN(posting, 1), 0);
			break;
		case 11:
			decoded->blocks[0].data_len--;
			break;
		}
		reject_unchanged(0);
	}
}

UT_TEST(test_maintenance_and_unlink_malformed_unchanged)
{
	for (int bad = 0; bad < 12; bad++) {
		xl_btree_vacuum rec = { 1, 1 };
		uint16 words[5] = { 2, 1, 1, 1, 0 };
		reset(XLOG_BTREE_VACUUM);
		maintenance_page(false);
		memcpy(main_data.data, &rec, SizeOfBtreeVacuum);
		decoded->main_data_len = SizeOfBtreeVacuum;
		decoded->blocks[0].data_len = 4 * sizeof(uint16);
		switch (bad) {
		case 0:
			decoded->main_data_len--;
			break;
		case 1:
			words[0] = 0;
			break;
		case 2:
			words[0] = 6;
			break;
		case 3:
			words[1] = words[0];
			break;
		case 4:
			words[1] = 0;
			break;
		case 5:
			words[2] = 0;
			break;
		case 6:
			words[2] = 3;
			break;
		case 7:
			words[3] = 3;
			break;
		case 8:
			words[2] = 2;
			words[3] = 1;
			words[4] = 1;
			decoded->blocks[0].data_len += 2;
			break;
		case 9:
			decoded->blocks[0].data_len--;
			break;
		case 10:
			decoded->blocks[0].data_len += 2;
			break;
		case 11:
			BTPageGetOpaque(original[0].data)->btpo_flags = 0;
			break;
		}
		memcpy(payload[0].data, words, sizeof(words));
		reject_unchanged(0);
	}
	for (int bad = 0; bad < 9; bad++) {
		BTDedupInterval *v;
		dedup_fixture(true, true);
		v = (BTDedupInterval *)payload[0].data;
		switch (bad) {
		case 0:
			decoded->main_data_len--;
			break;
		case 1:
			((xl_btree_dedup *)main_data.data)->nintervals++;
			break;
		case 2:
			v[0].baseoff = 1;
			break;
		case 3:
			v[0].nitems = 1;
			break;
		case 4:
			v[0].nitems = 20;
			break;
		case 5:
			v[1].baseoff--;
			break;
		case 6:
			v[1].baseoff = 7;
			break;
		case 7:
			decoded->blocks[0].data_len--;
			break;
		case 8:
			BTPageGetOpaque(original[0].data)->btpo_flags = 0;
			break;
		}
		reject_unchanged(0);
	}
	for (int bad = 0; bad < 13; bad++) {
		xl_btree_unlink_page *rec;
		uint8 id = 0;
		unlink_fixture(true, true, 2);
		rec = (xl_btree_unlink_page *)main_data.data;
		switch (bad) {
		case 0:
			decoded->main_data_len--;
			break;
		case 1:
			rec->leftsib++;
			break;
		case 2:
			rec->rightsib = 0;
			break;
		case 3:
			rec->safexid = InvalidFullTransactionId;
			break;
		case 4:
			decoded->blocks[3].in_use = false;
			break;
		case 5:
			decoded->blocks[0].flags = 0;
			break;
		case 6:
			BTPageGetOpaque(original[1].data)->btpo_next++;
			id = 1;
			break;
		case 7:
			BTPageGetOpaque(original[2].data)->btpo_prev++;
			id = 2;
			break;
		case 8:
			decoded->blocks[3].flags = 0;
			id = 3;
			break;
		case 9:
			rec->leafrightsib = 0;
			id = 3;
			break;
		case 10:
			decoded->blocks[4].data_len--;
			id = 4;
			break;
		case 11:
			decoded->blocks[0].data_len++;
			break;
		case 12:
			decoded->blocks[1].blkno = decoded->blocks[2].blkno;
			break;
		}
		reject_unchanged(id);
	}
}

UT_TEST(test_retail_bad_record_and_page_unchanged)
{
	for (int bad = 0; bad < 20; bad++) {
		retail_fixture();
		switch (bad) {
		case 0:
			decoded->main_data_len--;
			break;
		case 1:
			decoded->main_data = NULL;
			break;
		case 2:
			((xl_btree_insert *)main_data.data)->offnum = 0;
			break;
		case 3:
			((xl_btree_insert *)main_data.data)->offnum = 2;
			break;
		case 4:
			decoded->blocks[0].data_len--;
			break;
		case 5:
			decoded->blocks[0].has_data = false;
			break;
		case 6:
			((IndexTuple)payload[0].data)->t_info = 1;
			break;
		case 7:
			decoded->blocks[0].forknum = VISIBILITYMAP_FORKNUM;
			break;
		case 8:
			decoded->blocks[0].blkno = InvalidBlockNumber;
			break;
		case 9:
			decoded->blocks[0].flags = BKPBLOCK_WILL_INIT;
			break;
		case 10:
			decoded->blocks[0].blkno = BTREE_METAPAGE;
			break;
		case 11:
			decoded->blocks[1].in_use = true;
			break;
		case 12:
			((PageHeader)original[0].data)->pd_lower++;
			break;
		case 13:
			((PageHeader)original[0].data)->pd_upper = BLCKSZ;
			break;
		case 14:
			((PageHeader)original[0].data)->pd_special--;
			break;
		case 15:
			((PageHeader)original[0].data)->pd_pagesize_version = 0;
			break;
		case 16:
			BTPageGetOpaque(original[0].data)->btpo_flags = BTP_META;
			break;
		case 17:
			BTPageGetOpaque(original[0].data)->btpo_flags = 0;
			break;
		case 18:
			BTPageGetOpaque(original[0].data)->btpo_flags |= BTP_DELETED;
			break;
		case 19:
			BTPageGetOpaque(original[0].data)->btpo_flags |= BTP_HALF_DEAD;
			break;
		}
		reject_unchanged(0);
	}
}

UT_TEST(test_child_meta_and_newroot_bad_shape_unchanged)
{
	for (int bad = 0; bad < 11; bad++) {
		retail_fixture();
		decoded->header.xl_info = XLOG_BTREE_INSERT_UPPER;
		decoded->blocks[1].in_use = true;
		BTPageGetOpaque(original[0].data)->btpo_flags = 0;
		BTPageGetOpaque(original[1].data)->btpo_flags |= BTP_INCOMPLETE_SPLIT;
		switch (bad) {
		case 0:
			decoded->blocks[1].in_use = false;
			break;
		case 1:
			decoded->blocks[1].rlocator.relNumber++;
			break;
		case 2:
			decoded->blocks[1].blkno = decoded->blocks[0].blkno;
			break;
		case 3:
			decoded->blocks[1].forknum = VISIBILITYMAP_FORKNUM;
			break;
		case 4:
			decoded->blocks[1].flags = BKPBLOCK_WILL_INIT;
			break;
		case 5:
			BTPageGetOpaque(original[1].data)->btpo_flags &= ~BTP_INCOMPLETE_SPLIT;
			break;
		case 6:
			decoded->blocks[1].has_data = true;
			decoded->blocks[1].data_len = tuple(payload[1].data, 99);
			break;
		default:
			decoded->header.xl_info = XLOG_BTREE_INSERT_META;
			metadata(2);
			if (bad == 7)
				decoded->blocks[2].data_len--;
			if (bad == 8)
				payload[2].data[offsetof(xl_btree_metadata, allequalimage)] = 2;
			if (bad == 9)
				((xl_btree_metadata *)payload[2].data)->version = BTREE_MIN_VERSION;
			if (bad == 10)
				decoded->blocks[2].flags = 0;
		}
		reject_unchanged(bad < 4 ? 0 : bad < 7 ? 1 : 2);
	}
	for (int bad = 0; bad < 6; bad++) {
		reset(XLOG_BTREE_NEWROOT);
		((xl_btree_newroot *)main_data.data)->rootblk = 2;
		((xl_btree_newroot *)main_data.data)->level = 1;
		decoded->main_data_len = SizeOfBtreeNewroot;
		decoded->blocks[0].in_use = decoded->blocks[0].has_data = true;
		decoded->blocks[0].flags = BKPBLOCK_WILL_INIT;
		decoded->blocks[0].data_len = tuple(payload[0].data, 42);
		decoded->blocks[1].in_use = true;
		metadata(2);
		if (bad == 1)
			((xl_btree_newroot *)main_data.data)->rootblk++;
		if (bad == 2)
			decoded->blocks[0].flags = 0;
		if (bad == 3)
			decoded->blocks[0].data_len--;
		if (bad == 4)
			decoded->blocks[0].data_len = 0;
		if (bad == 5) {
			((xl_btree_newroot *)main_data.data)->level = 0;
			decoded->blocks[1].in_use = false;
		}
		reject_unchanged(0);
	}
}

int
main(void)
{
	UT_PLAN(15);
	UT_RUN(test_native_retail_insert_and_child_meta);
	UT_RUN(test_native_meta_cleanup_and_newroot);
	UT_RUN(test_retail_bad_record_and_page_unchanged);
	UT_RUN(test_child_meta_and_newroot_bad_shape_unchanged);
	UT_RUN(test_native_posting_insert);
	UT_RUN(test_native_split_left_right_leaf_internal);
	UT_RUN(test_native_halfdead_parent_and_leaf);
	UT_RUN(test_native_unlink_siblings_leaf_and_meta);
	UT_RUN(test_native_link_update_keeps_halfdead_sibling);
	UT_RUN(test_wal_posting_array_bounds_unchanged);
	UT_RUN(test_native_wal_posting_images_and_unaligned_decode);
	UT_RUN(test_native_delete_vacuum_posting_and_offsets);
	UT_RUN(test_native_dedup_groups_with_existing_posting);
	UT_RUN(test_split_and_posting_malformed_unchanged);
	UT_RUN(test_maintenance_and_unlink_malformed_unchanged);
	UT_DONE();
	printf("# Native whole-page comparisons: %u\n", page_comparisons);
	printf("# Malformed variants requiring unchanged output: %u\n", rejection_checks);
	for (unsigned i = 0; i < allocation_count; i++)
		free(allocations[i]);
	free(decoded);
	return ut_failed_count != 0;
}
