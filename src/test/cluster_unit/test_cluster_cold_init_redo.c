/* Original native redo constructors with independent per-block decisions.
 * Author: SqlRush <sqlrush@gmail.com> */
#include "postgres.h"
#include "access/brin_page.h"
#include "access/brin_pageops.h"
#include "access/brin_revmap.h"
#include "access/brin_xlog.h"
#include "access/gin_private.h"
#include "access/ginxlog.h"
#include "access/gist_private.h"
#include "access/gistxlog.h"
#include "access/hash.h"
#include "access/hash_xlog.h"
#include "access/spgist_private.h"
#include "access/spgxlog.h"
#include "access/xlogutils.h"
#include "commands/sequence.h"
#include "port/pg_bitutils.h"
#include "unit_test.h"

UT_DEFINE_GLOBALS();
#define BLOCKS 5
int cluster_node_id, NBuffers = BLOCKS, NLocBuffer;
char *BufferBlocks;
Block *LocalBufferBlockPointers;
bool cluster_shared_config = true, cluster_recmerge_window_active, cluster_recmerge_apply_foreign;
uint64 cluster_recmerge_window_scn, cluster_recmerge_window_own_lsn;
static PGAlignedBlock pages[BLOCKS], payload[BLOCKS], main_data;
static XLogReaderState reader;
static DecodedXLogRecord *decoded;
static bool locks[BLOCKS];
static unsigned skips, restores, reads[BLOCKS], dirties[BLOCKS], comparisons;
static void *allocated[1024];
static unsigned nalloc;
void *
palloc(Size n)
{
	void *p = malloc(n ? n : 1);
	UT_ASSERT(p && nalloc < lengthof(allocated));
	allocated[nalloc++] = p;
	return p;
}
void *
palloc0(Size n)
{
	void *p = palloc(n);
	memset(p, 0, n);
	return p;
}
void
pfree(void *p)
{
	for (unsigned i = 0; i < nalloc; i++)
		if (allocated[i] == p) {
			free(p);
			allocated[i] = NULL;
			return;
		}
	abort();
}
void
ExceptionalCondition(const char *c, const char *f, int l)
{
	fprintf(stderr, "%s %s:%d\n", c, f, l);
	abort();
}
XLogRedoAction
XLogReadBufferForRedo(XLogReaderState *r, uint8 id, Buffer *buf)
{
	UT_ASSERT(r == &reader && id < BLOCKS && decoded->blocks[id].in_use && !locks[id]);
	if (skips & (1U << id)) {
		*buf = InvalidBuffer;
		return BLK_NOTFOUND;
	}
	locks[id] = true;
	reads[id]++;
	*buf = id + 1;
	return (restores & (1U << id)) ? BLK_RESTORED : BLK_NEEDS_REDO;
}
XLogRedoAction
XLogReadBufferForRedoExtended(XLogReaderState *r, uint8 id, ReadBufferMode mode, bool cleanup,
							  Buffer *buf)
{
	UT_ASSERT(mode == RBM_NORMAL || mode == RBM_ZERO_AND_LOCK || mode == RBM_ZERO_AND_CLEANUP_LOCK);
	return XLogReadBufferForRedo(r, id, buf);
}
Buffer
XLogInitBufferForRedo(XLogReaderState *r, uint8 id)
{
	Buffer b;
	XLogReadBufferForRedo(r, id, &b);
	return b;
}
BlockNumber
BufferGetBlockNumber(Buffer b)
{
	UT_ASSERT(b > 0 && b <= BLOCKS && locks[b - 1]);
	return decoded->blocks[b - 1].blkno;
}
void
MarkBufferDirty(Buffer b)
{
	UT_ASSERT(b > 0 && b <= BLOCKS && locks[b - 1]);
	dirties[b - 1]++;
}
void
UnlockReleaseBuffer(Buffer b)
{
	UT_ASSERT(b > 0 && b <= BLOCKS && locks[b - 1]);
	locks[b - 1] = false;
}
void
FlushOneBuffer(Buffer b)
{
	abort();
}
bool
XLogRecGetBlockTagExtended(XLogReaderState *r, uint8 id, RelFileLocator *loc, ForkNumber *fork,
						   BlockNumber *block, Buffer *prefetch)
{
	DecodedBkpBlock *b;
	if (!XLogRecHasBlockRef(r, id))
		return false;
	b = XLogRecGetBlock(r, id);
	if (loc)
		*loc = b->rlocator;
	if (fork)
		*fork = b->forknum;
	if (block)
		*block = b->blkno;
	if (prefetch)
		*prefetch = InvalidBuffer;
	return true;
}
void
XLogRecGetBlockTag(XLogReaderState *r, uint8 id, RelFileLocator *loc, ForkNumber *fork,
				   BlockNumber *block)
{
	UT_ASSERT(XLogRecGetBlockTagExtended(r, id, loc, fork, block, NULL));
}
char *
XLogRecGetBlockData(XLogReaderState *r, uint8 id, Size *n)
{
	if (n)
		*n = decoded->blocks[id].data_len;
	return decoded->blocks[id].data;
}
#define HEAPBLK_TO_REVMAP_INDEX(pagesPerRange, heapBlk)                                            \
	(((heapBlk) / (pagesPerRange)) % REVMAP_PAGE_MAXITEMS)
#include "test_cluster_cold_init_native.inc"

static void
reset(void)
{
	free(decoded);
	for (unsigned i = 0; i < nalloc; i++)
		free(allocated[i]);
	nalloc = 0;
	decoded = calloc(1, offsetof(DecodedXLogRecord, blocks) + BLOCKS * sizeof(DecodedBkpBlock));
	memset(&reader, 0, sizeof(reader));
	memset(&main_data, 0, sizeof(main_data));
	memset(pages, 0, sizeof(pages));
	memset(payload, 0, sizeof(payload));
	memset(locks, 0, sizeof(locks));
	memset(reads, 0, sizeof(reads));
	memset(dirties, 0, sizeof(dirties));
	skips = restores = 0;
	reader.record = decoded;
	reader.EndRecPtr = 600;
	decoded->main_data = main_data.data;
	decoded->max_block_id = BLOCKS - 1;
	BufferBlocks = pages[0].data;
	for (int i = 0; i < BLOCKS; i++) {
		DecodedBkpBlock *b = &decoded->blocks[i];
		b->rlocator = (RelFileLocator){ 1663, 5, 900 };
		b->blkno = 100 + i;
		b->forknum = MAIN_FORKNUM;
		b->data = payload[i].data;
		b->has_data = true;
		PageInit(pages[i].data, BLCKSZ, 0);
	}
}
static void
block(int id, bool init)
{
	decoded->blocks[id].in_use = true;
	decoded->blocks[id].flags = init ? BKPBLOCK_WILL_INIT : 0;
}
static Size
inner(char *out, bool node)
{
	PGAlignedBlock tuple = { 0 };
	SpGistInnerTuple t = (SpGistInnerTuple)tuple.data;
	t->nNodes = node ? 1 : 0;
	t->size = SGITHDRSZ + (node ? SGNTHDRSZ : 0);
	if (node)
		((SpGistNodeTuple)(tuple.data + SGITHDRSZ))->t_info = SGNTHDRSZ;
	memcpy(out, tuple.data, t->size);
	return t->size;
}
static void
parent(int id)
{
	PGAlignedBlock tuple;
	Size n = inner(tuple.data, true);
	SpGistInitPage(pages[id].data, 0);
	UT_ASSERT_EQ(PageAddItem(pages[id].data, tuple.data, n, 1, false, false), 1);
	block(id, false);
}
static void
leaf(char *out)
{
	SpGistLeafTupleData t = { 0 };
	t.size = SGLTHDRSZ(false);
	memcpy(out, &t, sizeof(t));
}

/* Each case contains only constructor input, not a copy of redo logic. */
static void
prepare(int kind)
{
	reset();
	switch (kind) {
	case 0: {
		xl_hash_init_meta_page *r = (void *)main_data.data;
		r->ffactor = 100;
		block(0, true);
		break;
	}
	case 1: {
		xl_hash_init_bitmap_page *r = (void *)main_data.data;
		r->bmsize = 128;
		block(0, true);
		block(1, false);
		_hash_pageinit(pages[1].data, BLCKSZ);
		break;
	}
	case 2: {
		xl_hash_add_ovfl_page *r = (void *)main_data.data;
		r->bmsize = 128;
		for (int i = 0; i < 5; i++) {
			block(i, i == 0 || i == 3);
			_hash_pageinit(pages[i].data, BLCKSZ);
			decoded->blocks[i].data_len = sizeof(uint32);
		}
		*(uint32 *)payload[4].data = 7;
		break;
	}
	case 3: {
		xl_hash_split_allocate_page *r = (void *)main_data.data;
		r->new_bucket = 2;
		r->new_bucket_flag = LH_BUCKET_PAGE;
		for (int i = 0; i < 3; i++) {
			block(i, i == 1);
			_hash_pageinit(pages[i].data, BLCKSZ);
		}
		break;
	}
	case 4: {
		xl_brin_createidx *r = (void *)main_data.data;
		r->pagesPerRange = 32;
		r->version = BRIN_CURRENT_VERSION;
		block(0, true);
		break;
	}
	case 5: {
		xl_brin_insert *r = (void *)main_data.data;
		r->pagesPerRange = 32;
		r->offnum = 1;
		decoded->header.xl_info = XLOG_BRIN_INIT_PAGE;
		block(0, true);
		block(1, false);
		decoded->blocks[0].data_len = sizeof(BrinTuple);
		brin_page_init(pages[1].data, BRIN_PAGETYPE_REVMAP);
		break;
	}
	case 6: {
		xl_brin_revmap_extend *r = (void *)main_data.data;
		r->targetBlk = 1;
		block(0, false);
		block(1, true);
		decoded->blocks[1].blkno = 1;
		brin_metapage_init(pages[0].data, 32, BRIN_CURRENT_VERSION);
		break;
	}
	case 7:
		block(0, true);
		break;
	case 8: {
		ginxlogUpdateMeta *r = (void *)main_data.data;
		r->prevTail = 1;
		r->newRightlink = 2;
		block(0, true);
		decoded->blocks[0].blkno = GIN_METAPAGE_BLKNO;
		block(1, false);
		GinInitPage(pages[1].data, GIN_LIST, BLCKSZ);
		break;
	}
	case 9: {
		ginxlogInsertListPage *r = (void *)main_data.data;
		r->rightlink = InvalidBlockNumber;
		block(0, true);
		break;
	}
	case 10: {
		ginxlogDeleteListPages *r = (void *)main_data.data;
		r->ndeleted = 2;
		for (int i = 0; i < 3; i++)
			block(i, true);
		decoded->blocks[0].blkno = GIN_METAPAGE_BLKNO;
		break;
	}
	case 11: {
		gistxlogPageSplit *r = (void *)main_data.data;
		r->npage = 2;
		r->origleaf = true;
		r->origrlink = InvalidBlockNumber;
		for (int i = 0; i < 3; i++) {
			block(i, i > 0);
			gistinitpage(pages[i].data, F_LEAF);
			decoded->blocks[i].data_len = sizeof(int);
		}
		break;
	}
	case 12: {
		spgxlogAddLeaf *r = (void *)main_data.data;
		r->newPage = true;
		r->offnumLeaf = 1;
		leaf(main_data.data + sizeof(*r));
		block(0, true);
		break;
	}
	case 13: {
		spgxlogMoveLeafs *r = (void *)main_data.data;
		r->newPage = true;
		r->offnumParent = 1;
		r->stateSrc.isBuild = true;
		*(OffsetNumber *)(main_data.data + SizeOfSpgxlogMoveLeafs) = 1;
		leaf(main_data.data + SizeOfSpgxlogMoveLeafs + sizeof(OffsetNumber));
		block(0, false);
		SpGistInitPage(pages[0].data, SPGIST_LEAF);
		block(1, true);
		parent(2);
		break;
	}
	case 14: {
		spgxlogAddNode *r = (void *)main_data.data;
		r->newPage = true;
		r->offnum = r->offnumNew = 1;
		r->parentBlk = -1;
		r->stateSrc.isBuild = true;
		inner(main_data.data + sizeof(*r), false);
		parent(0);
		block(1, true);
		break;
	}
	case 15: {
		spgxlogSplitTuple *r = (void *)main_data.data;
		Size n;
		r->newPage = true;
		r->offnumPrefix = r->offnumPostfix = 1;
		n = inner(main_data.data + sizeof(*r), false);
		inner(main_data.data + sizeof(*r) + n, false);
		parent(0);
		block(1, true);
		break;
	}
	case 16: {
		spgxlogPickSplit *r = (void *)main_data.data;
		r->initSrc = r->initDest = r->initInner = true;
		r->offnumInner = r->offnumParent = 1;
		r->stateSrc.isBuild = true;
		inner(main_data.data + SizeOfSpgxlogPickSplit, false);
		for (int i = 0; i < 3; i++)
			block(i, true);
		parent(3);
		break;
	}
	case 17: {
		xl_seq_rec *r = (void *)main_data.data;
		r->write_scn = 888;
		decoded->header.xl_info = XLOG_SEQ_LOG;
		decoded->main_data_len = sizeof(*r) + 16;
		block(0, true);
		break;
	}
	default:
		abort();
	}
}
static void
redo(int kind)
{
	switch (kind) {
	case 0:
		hash_xlog_init_meta_page(&reader);
		break;
	case 1:
		hash_xlog_init_bitmap_page(&reader);
		break;
	case 2:
		hash_xlog_add_ovfl_page(&reader);
		break;
	case 3:
		hash_xlog_split_allocate_page(&reader);
		break;
	case 4:
		brin_xlog_createidx(&reader);
		break;
	case 5:
		brin_xlog_insert_update(&reader, (void *)main_data.data);
		break;
	case 6:
		brin_xlog_revmap_extend(&reader);
		break;
	case 7:
		ginRedoCreatePTree(&reader);
		break;
	case 8:
		ginRedoUpdateMetapage(&reader);
		break;
	case 9:
		ginRedoInsertListPage(&reader);
		break;
	case 10:
		ginRedoDeleteListPages(&reader);
		break;
	case 11:
		gistRedoPageSplitRecord(&reader);
		break;
	case 12:
		spgRedoAddLeaf(&reader);
		break;
	case 13:
		spgRedoMoveLeafs(&reader);
		break;
	case 14:
		spgRedoAddNode(&reader);
		break;
	case 15:
		spgRedoSplitTuple(&reader);
		break;
	case 16:
		spgRedoPickSplit(&reader);
		break;
	case 17:
		seq_redo(&reader);
		break;
	default:
		abort();
	}
}
static void
check_cases(int first, int last)
{
	PGAlignedBlock expected[BLOCKS], poison;
	memset(&poison, 0xA5, BLCKSZ);
	for (int kind = first; kind <= last; kind++) {
		prepare(kind);
		redo(kind);
		memcpy(expected, pages, sizeof(expected));
		for (int selected = 0; selected < BLOCKS; selected++) {
			if (!decoded->blocks[selected].in_use)
				continue;
			for (int restored = 0; restored < 2; restored++) {
				prepare(kind);
				skips = restored ? 0 : 1U << selected;
				restores = restored ? 1U << selected : 0;
				pages[selected] = restored ? expected[selected] : poison;
				redo(kind);
				for (int i = 0; i < BLOCKS; i++) {
					UT_ASSERT(!locks[i]);
					if (!decoded->blocks[i].in_use)
						continue;
					UT_ASSERT(memcmp(pages[i].data,
									 i == selected && !restored ? poison.data : expected[i].data,
									 BLCKSZ)
							  == 0);
					UT_ASSERT_EQ(reads[i], i == selected && !restored ? 0 : 1);
					if (i == selected && !restored)
						UT_ASSERT_EQ(dirties[i], 0);
					comparisons++;
				}
			}
		}
	}
}
UT_TEST(hash_mixed_verdicts)
{
	check_cases(0, 3);
}
UT_TEST(brin_mixed_verdicts)
{
	check_cases(4, 6);
}
UT_TEST(gin_mixed_verdicts)
{
	check_cases(7, 10);
}
UT_TEST(gist_mixed_verdicts)
{
	check_cases(11, 11);
}
UT_TEST(spgist_mixed_verdicts)
{
	check_cases(12, 16);
}
UT_TEST(sequence_mixed_verdicts)
{
	check_cases(17, 17);
}
int
main(void)
{
	UT_PLAN(6);
	UT_RUN(hash_mixed_verdicts);
	UT_RUN(brin_mixed_verdicts);
	UT_RUN(gin_mixed_verdicts);
	UT_RUN(gist_mixed_verdicts);
	UT_RUN(spgist_mixed_verdicts);
	UT_RUN(sequence_mixed_verdicts);
	printf("# Mixed block byte comparisons: %u\n", comparisons);
	UT_DONE();
	reset();
	free(decoded);
	return ut_failed_count ? 1 : 0;
}
