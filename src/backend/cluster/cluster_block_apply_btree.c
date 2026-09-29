/*-------------------------------------------------------------------------
 *
 * cluster_block_apply_btree.c
 *    Native btree WAL effects on a private, detached page.
 *
 * Portions Copyright (c) 1996-2023, PostgreSQL Global Development Group
 * Portions Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *    src/backend/cluster/cluster_block_apply_btree.c
 *
 * NOTES
 *    Re-expresses nbtxlog.c page effects without buffer I/O, installation
 *    authority or numeric LSN admission. Only native-differential entries
 *    enter the matrix. The caller owns exact dependency and whole-record
 *    installation; constructing bytes does not grant either authority.
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#ifdef USE_PGRAC_CLUSTER
#include "access/nbtree.h"
#include "access/nbtxlog.h"
#include "cluster/cluster_block_apply.h"

static bool btree_posting_bytes_valid(const char *data, IndexTuple header);
static bool btree_tuple_size(const char *data, Size available, Size *size);

static bool
btree_page_valid(Page page, bool sibling_link)
{
	PageHeader header = (PageHeader)page;
	OffsetNumber maximum;

	if (PageIsNew(page) || PageGetPageSize(page) != BLCKSZ
		|| PageGetPageLayoutVersion(page) != PG_PAGE_LAYOUT_VERSION
		|| header->pd_special != BLCKSZ - MAXALIGN(sizeof(BTPageOpaqueData))
		|| header->pd_lower < SizeOfPageHeaderData || header->pd_lower > header->pd_upper
		|| header->pd_upper > header->pd_special
		|| (header->pd_lower - SizeOfPageHeaderData) % sizeof(ItemIdData) != 0)
		return false;
	maximum = PageGetMaxOffsetNumber(page);
	if (maximum > MaxIndexTuplesPerPage || P_ISMETA(BTPageGetOpaque(page))
		|| P_ISDELETED(BTPageGetOpaque(page)))
		return false;
	/* A half-dead leaf stays linked until UNLINK, but cannot accept tuples. */
	if (P_ISHALFDEAD(BTPageGetOpaque(page))
		&& (!sibling_link || !P_ISLEAF(BTPageGetOpaque(page))
			|| BTPageGetOpaque(page)->btpo_level != 0 || P_ISROOT(BTPageGetOpaque(page))
			|| P_RIGHTMOST(BTPageGetOpaque(page)) || maximum != P_HIKEY))
		return false;
	for (OffsetNumber off = FirstOffsetNumber; off <= maximum; off++) {
		ItemId item = PageGetItemId(page, off);
		Size start = ItemIdGetOffset(item), size = ItemIdGetLength(item);
		Size tuple_size;

		if (!ItemIdHasStorage(item) || (ItemIdGetFlags(item) != LP_NORMAL && !ItemIdIsDead(item))
			|| size < sizeof(IndexTupleData) || start != MAXALIGN(start) || start < header->pd_upper
			|| start + MAXALIGN(size) > header->pd_special)
			return false;
		if (!btree_tuple_size(page + start, size, &tuple_size)
			|| (P_ISHALFDEAD(BTPageGetOpaque(page)) && tuple_size != sizeof(IndexTupleData)))
			return false;
		for (OffsetNumber prior = FirstOffsetNumber; prior < off; prior++) {
			ItemId other = PageGetItemId(page, prior);
			Size other_start = ItemIdGetOffset(other);
			if (start < other_start + MAXALIGN(ItemIdGetLength(other))
				&& other_start < start + MAXALIGN(size))
				return false;
		}
	}
	return true;
}

static bool
btree_tuple_size(const char *data, Size available, Size *size)
{
	IndexTupleData tuple;

	if (data == NULL || available < sizeof(tuple))
		return false;
	memcpy(&tuple, data, sizeof(tuple));
	*size = IndexTupleSize(&tuple);
	if (*size < sizeof(tuple) || *size > available)
		return false;
	return !BTreeTupleIsPosting(&tuple) || btree_posting_bytes_valid(data, &tuple);
}

/* Native restore order is reversed: WAL contains pd_upper..pd_special. */
static bool
btree_restore_items(Page page, const char *data, Size length)
{
	Size offsets[MaxIndexTuplesPerPage], sizes[MaxIndexTuplesPerPage];
	Size cursor = 0;
	int count = 0;

	while (cursor < length) {
		Size size;
		if (count == MaxIndexTuplesPerPage
			|| !btree_tuple_size(data + cursor, length - cursor, &size)
			|| MAXALIGN(size) > length - cursor)
			return false;
		offsets[count] = cursor;
		sizes[count++] = MAXALIGN(size);
		cursor += MAXALIGN(size);
	}
	for (int i = count - 1; i >= 0; i--)
		if (PageAddItem(page, (Item)(data + offsets[i]), sizes[i], count - i, false, false)
			== InvalidOffsetNumber)
			return false;
	return true;
}

static bool
btree_restore_meta(XLogReaderState *record, uint8 id, Page page)
{
	xl_btree_metadata rec;
	Size length;
	const char *data = XLogRecGetBlockData(record, id, &length);
	BTMetaPageData *meta;
	const DecodedBkpBlock *block = XLogRecGetBlock(record, id);

	if (block->blkno != BTREE_METAPAGE || !(block->flags & BKPBLOCK_WILL_INIT) || data == NULL
		|| length != sizeof(rec) || (uint8)data[offsetof(xl_btree_metadata, allequalimage)] > 1)
		return false;
	memcpy(&rec, data, sizeof(rec));
	if (rec.version < BTREE_NOVAC_VERSION || rec.version > BTREE_VERSION)
		return false;
	PageInit(page, BLCKSZ, sizeof(BTPageOpaqueData));
	meta = BTPageGetMeta(page);
	meta->btm_magic = BTREE_MAGIC;
	meta->btm_version = rec.version;
	meta->btm_root = rec.root;
	meta->btm_level = rec.level;
	meta->btm_fastroot = rec.fastroot;
	meta->btm_fastlevel = rec.fastlevel;
	meta->btm_last_cleanup_num_delpages = rec.last_cleanup_num_delpages;
	meta->btm_last_cleanup_num_heap_tuples = -1.0;
	meta->btm_allequalimage = rec.allequalimage;
	BTPageGetOpaque(page)->btpo_flags = BTP_META;
	((PageHeader)page)->pd_lower = (char *)meta + sizeof(*meta) - page;
	return true;
}

/* Exact tag shape, not an authority check or a permission to initialize. */
static bool
btree_record_blocks_valid(XLogReaderState *record, uint8 mask)
{
	RelFileLocator locator = { 0 };
	bool have_locator = false;

	for (int i = 0; i <= XLogRecMaxBlockId(record); i++) {
		const DecodedBkpBlock *block;
		if (!XLogRecHasBlockRef(record, i))
			continue;
		block = XLogRecGetBlock(record, i);
		if (i > 4 || !(mask & (1 << i)) || block->forknum != MAIN_FORKNUM
			|| block->blkno == InvalidBlockNumber)
			return false;
		if (have_locator && !RelFileLocatorEquals(locator, block->rlocator))
			return false;
		locator = block->rlocator;
		have_locator = true;
		for (int prior = 0; prior < i; prior++)
			if (XLogRecHasBlockRef(record, prior)
				&& XLogRecGetBlock(record, prior)->blkno == block->blkno)
				return false;
	}
	for (int i = 0; i < 5; i++)
		if ((mask & (1 << i)) && !XLogRecHasBlockRef(record, i))
			return false;
	return true;
}

static int
btree_tid_compare(ItemPointer a, ItemPointer b)
{
	BlockNumber ab = ItemPointerGetBlockNumberNoCheck(a);
	BlockNumber bb = ItemPointerGetBlockNumberNoCheck(b);
	OffsetNumber ao = ItemPointerGetOffsetNumberNoCheck(a);
	OffsetNumber bo = ItemPointerGetOffsetNumberNoCheck(b);

	return ab != bb ? (ab < bb ? -1 : 1) : ao != bo ? (ao < bo ? -1 : 1) : 0;
}

/* WAL bytes can be unaligned. Bound the array before copying any TID. */
static bool
btree_posting_bytes_valid(const char *data, IndexTuple header)
{
	Size size = IndexTupleSize(header), keysize;
	uint16 count;
	ItemPointerData prior;

	keysize = BTreeTupleGetPostingOffset(header);
	count = BTreeTupleGetNPosting(header);
	if (count < 2 || keysize < sizeof(IndexTupleData) || keysize != MAXALIGN(keysize)
		|| keysize + count * sizeof(ItemPointerData) > size || size != MAXALIGN(size))
		return false;
	for (int i = 0; i < count; i++) {
		ItemPointerData tid;
		memcpy(&tid, data + keysize + i * sizeof(tid), sizeof(tid));
		if (!ItemPointerIsValid(&tid) || (i > 0 && btree_tid_compare(&prior, &tid) >= 0))
			return false;
		prior = tid;
	}
	return true;
}

/* Page callers have already bounded the outer tuple with btree_page_valid. */
static bool
btree_posting_valid(IndexTuple item)
{
	return BTreeTupleIsPosting(item) && btree_posting_bytes_valid((char *)item, item);
}

/* _bt_swap_posting's byte effect, with bounded caller-owned workspaces. */
static bool
btree_swap_posting(IndexTuple newitem, IndexTuple old, uint16 offset, char *replacement)
{
	uint16 count;
	IndexTuple posting = (IndexTuple)replacement;

	if (!btree_posting_valid(old) || BTreeTupleIsPivot(newitem) || BTreeTupleIsPosting(newitem)
		|| !ItemPointerIsValid(&newitem->t_tid))
		return false;
	count = BTreeTupleGetNPosting(old);
	if (offset == 0 || offset >= count
		|| btree_tid_compare(BTreeTupleGetPostingN(old, offset - 1), &newitem->t_tid) >= 0
		|| btree_tid_compare(&newitem->t_tid, BTreeTupleGetPostingN(old, offset)) >= 0)
		return false;
	memcpy(replacement, old, IndexTupleSize(old));
	memmove(BTreeTupleGetPostingN(posting, offset + 1), BTreeTupleGetPostingN(posting, offset),
			(count - offset - 1) * sizeof(ItemPointerData));
	ItemPointerCopy(&newitem->t_tid, BTreeTupleGetPostingN(posting, offset));
	ItemPointerCopy(BTreeTupleGetPostingN(old, count - 1), &newitem->t_tid);
	return true;
}

static bool
btree_insert_posting(XLogReaderState *record, Page page, OffsetNumber offset)
{
	PGAlignedBlock newitem, replacement;
	Size length, size;
	uint16 postingoff;
	const char *data = XLogRecGetBlockData(record, 0, &length);
	IndexTuple old;

	if (data == NULL || length < sizeof(postingoff)
		|| offset <= P_FIRSTDATAKEY(BTPageGetOpaque(page))
		|| offset > PageGetMaxOffsetNumber(page) + 1)
		return false;
	memcpy(&postingoff, data, sizeof(postingoff));
	data += sizeof(postingoff);
	length -= sizeof(postingoff);
	if (!btree_tuple_size(data, length, &size) || size != length || size > BLCKSZ)
		return false;
	memcpy(newitem.data, data, size);
	old = (IndexTuple)PageGetItem(page, PageGetItemId(page, offset - 1));
	if (!btree_swap_posting((IndexTuple)newitem.data, old, postingoff, replacement.data))
		return false;
	memcpy(old, replacement.data, IndexTupleSize(old));
	return PageAddItem(page, newitem.data, size, offset, false, false) != InvalidOffsetNumber;
}

static bool
btree_split(XLogReaderState *record, uint8 id, Page original, Page result)
{
	xl_btree_split rec;
	bool left = (XLogRecGetInfo(record) & ~XLR_INFO_MASK) == XLOG_BTREE_SPLIT_L;
	bool sibling = XLogRecHasBlockRef(record, 2);
	uint8 mask = 3 | (sibling ? 4 : 0);
	BlockNumber oldblk, newblk, nextblk;
	BTPageOpaque opaque;
	Size length;
	const char *data = XLogRecGetBlockData(record, id, &length);

	if (XLogRecGetData(record) == NULL || XLogRecGetDataLen(record) != SizeOfBtreeSplit)
		return false;
	memcpy(&rec, XLogRecGetData(record), SizeOfBtreeSplit);
	if (rec.level != 0)
		mask |= 8;
	if (id > 3 || !(mask & (1 << id)) || !btree_record_blocks_valid(record, mask)
		|| (rec.level != 0 && rec.postingoff != 0))
		return false;
	for (int i = 0; i < 4; i++)
		if ((mask & (1 << i))
			&& (XLogRecGetBlock(record, i)->blkno == BTREE_METAPAGE
				|| ((XLogRecGetBlock(record, i)->flags & BKPBLOCK_WILL_INIT) != 0) != (i == 1)))
			return false;
	oldblk = XLogRecGetBlock(record, 0)->blkno;
	newblk = XLogRecGetBlock(record, 1)->blkno;
	nextblk = sibling ? XLogRecGetBlock(record, 2)->blkno : P_NONE;
	if (id == 1) {
		PageInit(result, BLCKSZ, sizeof(BTPageOpaqueData));
		opaque = BTPageGetOpaque(result);
		opaque->btpo_prev = oldblk;
		opaque->btpo_next = nextblk;
		opaque->btpo_level = rec.level;
		opaque->btpo_flags = rec.level == 0 ? BTP_LEAF : 0;
		return data != NULL && btree_restore_items(result, data, length)
			   && PageGetMaxOffsetNumber(result) >= (sibling ? 2 : 1);
	}
	if (!btree_page_valid(original, id == 2))
		return false;
	opaque = BTPageGetOpaque(original);
	if (id == 2 || id == 3) {
		if (length != 0)
			return false;
		memcpy(result, original, BLCKSZ);
		opaque = BTPageGetOpaque(result);
		if (id == 2) {
			if (opaque->btpo_level != rec.level || opaque->btpo_prev != oldblk)
				return false;
			opaque->btpo_prev = newblk;
		} else {
			if (!P_INCOMPLETE_SPLIT(opaque) || opaque->btpo_level != rec.level - 1)
				return false;
			opaque->btpo_flags &= ~BTP_INCOMPLETE_SPLIT;
		}
		return true;
	} else {
		PGAlignedBlock newitem, replacement;
		Size newsize = 0, highsize, cursor = 0;
		OffsetNumber min = P_FIRSTDATAKEY(opaque), max = PageGetMaxOffsetNumber(original);
		OffsetNumber off, dest = P_FIRSTKEY, replace = InvalidOffsetNumber;
		const char *highkey;

		if (opaque->btpo_level != rec.level || P_ISLEAF(opaque) != (rec.level == 0)
			|| opaque->btpo_next != nextblk || rec.firstrightoff < min
			|| rec.firstrightoff > max + 1 || rec.newitemoff < min || rec.newitemoff > max + 1
			|| data == NULL || (left && rec.newitemoff > rec.firstrightoff)
			|| (!left && rec.newitemoff < rec.firstrightoff))
			return false;
		if (left || rec.postingoff != 0) {
			if (!btree_tuple_size(data, length, &newsize) || MAXALIGN(newsize) > length)
				return false;
			newsize = MAXALIGN(newsize);
			memcpy(newitem.data, data, newsize);
			cursor = newsize;
			if (rec.postingoff != 0) {
				IndexTuple old;
				replace = rec.newitemoff - 1;
				if (replace < min || replace >= rec.firstrightoff
					|| (!left && rec.firstrightoff != rec.newitemoff))
					return false;
				old = (IndexTuple)PageGetItem(original, PageGetItemId(original, replace));
				if (!btree_swap_posting((IndexTuple)newitem.data, old, rec.postingoff,
										replacement.data))
					return false;
			}
		}
		highkey = data + cursor;
		if (!btree_tuple_size(highkey, length - cursor, &highsize)
			|| cursor + MAXALIGN(highsize) != length)
			return false;
		PageInit(result, BLCKSZ, sizeof(BTPageOpaqueData));
		memcpy(PageGetSpecialPointer(result), PageGetSpecialPointer(original),
			   sizeof(BTPageOpaqueData));
		if (PageAddItem(result, (Item)highkey, MAXALIGN(highsize), P_HIKEY, false, false)
			== InvalidOffsetNumber)
			return false;
		for (off = min; off < rec.firstrightoff; off++) {
			ItemId item = PageGetItemId(original, off);
			const char *bytes = PageGetItem(original, item);
			Size size = ItemIdGetLength(item);
			if (off == replace) {
				bytes = replacement.data;
				size = MAXALIGN(IndexTupleSize((IndexTuple)bytes));
			} else if (left && off == rec.newitemoff) {
				if (PageAddItem(result, newitem.data, newsize, dest++, false, false)
					== InvalidOffsetNumber)
					return false;
			}
			if (PageAddItem(result, (Item)bytes, size, dest++, false, false) == InvalidOffsetNumber)
				return false;
		}
		if (left && off == rec.newitemoff
			&& PageAddItem(result, newitem.data, newsize, dest, false, false)
				   == InvalidOffsetNumber)
			return false;
		opaque = BTPageGetOpaque(result);
		opaque->btpo_flags = BTP_INCOMPLETE_SPLIT | (rec.level == 0 ? BTP_LEAF : 0);
		opaque->btpo_next = newblk;
		opaque->btpo_cycleid = 0;
		return true;
	}
}

static bool
btree_halfdead_page(Page page, BlockNumber left, BlockNumber right, BlockNumber top)
{
	IndexTupleData high = { 0 };
	BTPageOpaque opaque;

	PageInit(page, BLCKSZ, sizeof(BTPageOpaqueData));
	opaque = BTPageGetOpaque(page);
	opaque->btpo_prev = left;
	opaque->btpo_next = right;
	opaque->btpo_flags = BTP_HALF_DEAD | BTP_LEAF;
	high.t_info = sizeof(high);
	BTreeTupleSetTopParent(&high, top);
	return PageAddItem(page, (Item)&high, sizeof(high), P_HIKEY, false, false)
		   != InvalidOffsetNumber;
}

static bool
btree_mark_halfdead(XLogReaderState *record, uint8 id, Page original, Page result)
{
	xl_btree_mark_page_halfdead rec;
	const DecodedBkpBlock *block = XLogRecGetBlock(record, id);
	BTPageOpaque opaque;
	IndexTuple first, next;

	if (id > 1 || !btree_record_blocks_valid(record, 3) || XLogRecGetData(record) == NULL
		|| XLogRecGetDataLen(record) != SizeOfBtreeMarkPageHalfDead)
		return false;
	memcpy(&rec, XLogRecGetData(record), SizeOfBtreeMarkPageHalfDead);
	if (rec.leafblk != XLogRecGetBlock(record, 0)->blkno || rec.leafblk == BTREE_METAPAGE
		|| rec.leftblk == InvalidBlockNumber || rec.rightblk == P_NONE
		|| rec.rightblk == InvalidBlockNumber || block->blkno == BTREE_METAPAGE
		|| block->data_len != 0 || ((block->flags & BKPBLOCK_WILL_INIT) != 0) != (id == 0))
		return false;
	if (id == 0)
		return btree_halfdead_page(result, rec.leftblk, rec.rightblk, rec.topparent);
	if (!btree_page_valid(original, false))
		return false;
	opaque = BTPageGetOpaque(original);
	if (P_ISLEAF(opaque) || rec.poffset < P_FIRSTDATAKEY(opaque)
		|| rec.poffset >= PageGetMaxOffsetNumber(original))
		return false;
	memcpy(result, original, BLCKSZ);
	first = (IndexTuple)PageGetItem(result, PageGetItemId(result, rec.poffset));
	next = (IndexTuple)PageGetItem(result, PageGetItemId(result, rec.poffset + 1));
	BTreeTupleSetDownLink(first, BTreeTupleGetDownLink(next));
	PageIndexTupleDelete(result, rec.poffset + 1);
	return true;
}

static bool
btree_unlink(XLogReaderState *record, uint8 id, Page original, Page result)
{
	xl_btree_unlink_page rec;
	const DecodedBkpBlock *block = XLogRecGetBlock(record, id);
	bool meta = (XLogRecGetInfo(record) & ~XLR_INFO_MASK) == XLOG_BTREE_UNLINK_PAGE_META;
	uint8 mask;
	BTPageOpaque opaque;

	if (XLogRecGetData(record) == NULL || XLogRecGetDataLen(record) != SizeOfBtreeUnlinkPage)
		return false;
	memcpy(&rec, XLogRecGetData(record), SizeOfBtreeUnlinkPage);
	mask = 5 | (rec.leftsib != P_NONE ? 2 : 0) | (rec.level > 0 ? 8 : 0) | (meta ? 16 : 0);
	if (id > 4 || !(mask & (1 << id)) || !btree_record_blocks_valid(record, mask)
		|| rec.leftsib == InvalidBlockNumber || rec.rightsib == P_NONE
		|| rec.rightsib == InvalidBlockNumber || !FullTransactionIdIsNormal(rec.safexid)
		|| (rec.level <= 1 && rec.leaftopparent != InvalidBlockNumber)
		|| (rec.leftsib != P_NONE && XLogRecGetBlock(record, 1)->blkno != rec.leftsib)
		|| XLogRecGetBlock(record, 2)->blkno != rec.rightsib)
		return false;
	if (id == 4)
		return btree_restore_meta(record, id, result);
	if (block->blkno == BTREE_METAPAGE || block->data_len != 0
		|| ((block->flags & BKPBLOCK_WILL_INIT) != 0) != (id == 0 || id == 3))
		return false;
	if (id == 0) {
		PageInit(result, BLCKSZ, sizeof(BTPageOpaqueData));
		opaque = BTPageGetOpaque(result);
		opaque->btpo_prev = rec.leftsib;
		opaque->btpo_next = rec.rightsib;
		opaque->btpo_level = rec.level;
		BTPageSetDeleted(result, rec.safexid);
		if (rec.level == 0)
			opaque->btpo_flags |= BTP_LEAF;
		return true;
	}
	if (id == 3) {
		if (rec.leafleftsib == InvalidBlockNumber || rec.leafrightsib == P_NONE
			|| rec.leafrightsib == InvalidBlockNumber)
			return false;
		return btree_halfdead_page(result, rec.leafleftsib, rec.leafrightsib, rec.leaftopparent);
	}
	if (!btree_page_valid(original, true))
		return false;
	memcpy(result, original, BLCKSZ);
	opaque = BTPageGetOpaque(result);
	if (opaque->btpo_level != rec.level)
		return false;
	if (id == 1) {
		if (opaque->btpo_next != XLogRecGetBlock(record, 0)->blkno)
			return false;
		opaque->btpo_next = rec.rightsib;
	} else {
		if (opaque->btpo_prev != XLogRecGetBlock(record, 0)->blkno)
			return false;
		opaque->btpo_prev = rec.leftsib;
	}
	return true;
}

/* _bt_form_posting/_bt_update_posting zero padding is part of the image. */
static bool
btree_form_posting(IndexTuple base, ItemPointer tids, int count, char *result, Size maximum)
{
	Size keysize, size;
	IndexTuple item = (IndexTuple)result;

	if (BTreeTupleIsPivot(base) || count <= 0 || count > BT_OFFSET_MASK
		|| (BTreeTupleIsPosting(base) && !btree_posting_valid(base)))
		return false;
	keysize = BTreeTupleIsPosting(base) ? BTreeTupleGetPostingOffset(base) : IndexTupleSize(base);
	size = count == 1 ? keysize : MAXALIGN(keysize + count * sizeof(ItemPointerData));
	if (keysize != MAXALIGN(keysize) || size > maximum || size > INDEX_SIZE_MASK)
		return false;
	for (int i = 0; i < count; i++)
		if (!ItemPointerIsValid(&tids[i]) || (i && btree_tid_compare(&tids[i - 1], &tids[i]) >= 0))
			return false;
	memset(result, 0, size);
	memcpy(result, base, keysize);
	item->t_info = (item->t_info & ~INDEX_SIZE_MASK) | size;
	if (count > 1) {
		BTreeTupleSetPosting(item, count, keysize);
		memcpy(BTreeTupleGetPosting(item), tids, count * sizeof(ItemPointerData));
	} else {
		item->t_info &= ~INDEX_ALT_TID_MASK;
		ItemPointerCopy(tids, &item->t_tid);
	}
	return true;
}

static bool
btree_delete_items(XLogReaderState *record, Page original, Page result)
{
	uint16 ndeleted, nupdated;
	OffsetNumber deleted[MaxIndexTuplesPerPage], updated[MaxIndexTuplesPerPage];
	bool occupied[MaxIndexTuplesPerPage + 1] = { false };
	Size length, cursor;
	const char *data = XLogRecGetBlockData(record, 0, &length);
	const char *main_data = XLogRecGetData(record);
	BTPageOpaque opaque;
	OffsetNumber min, max;

	if (main_data == NULL || !btree_page_valid(original, false))
		return false;
	if ((XLogRecGetInfo(record) & ~XLR_INFO_MASK) == XLOG_BTREE_VACUUM) {
		xl_btree_vacuum rec;
		if (XLogRecGetDataLen(record) != SizeOfBtreeVacuum)
			return false;
		memcpy(&rec, main_data, SizeOfBtreeVacuum);
		ndeleted = rec.ndeleted;
		nupdated = rec.nupdated;
	} else {
		xl_btree_delete rec;
		if (XLogRecGetDataLen(record) != SizeOfBtreeDelete
			|| (uint8)main_data[offsetof(xl_btree_delete, isCatalogRel)] > 1)
			return false;
		memcpy(&rec, main_data, SizeOfBtreeDelete);
		ndeleted = rec.ndeleted;
		nupdated = rec.nupdated;
	}
	opaque = BTPageGetOpaque(original);
	min = P_FIRSTDATAKEY(opaque);
	max = PageGetMaxOffsetNumber(original);
	cursor = (ndeleted + nupdated) * sizeof(OffsetNumber);
	if (!P_ISLEAF(opaque) || ndeleted > max || nupdated > max || cursor > length
		|| (length != 0 && data == NULL))
		return false;
	if (ndeleted)
		memcpy(deleted, data, ndeleted * sizeof(OffsetNumber));
	if (nupdated)
		memcpy(updated, data + ndeleted * sizeof(OffsetNumber), nupdated * sizeof(OffsetNumber));
	for (int group = 0; group < 2; group++) {
		OffsetNumber *offsets = group == 0 ? deleted : updated;
		int count = group == 0 ? ndeleted : nupdated;
		for (int i = 0; i < count; i++) {
			OffsetNumber off = offsets[i];
			if (off < min || off > max || occupied[off] || (i && offsets[i - 1] >= off))
				return false;
			occupied[off] = true;
		}
	}
	memcpy(result, original, BLCKSZ);
	for (int u = 0; u < nupdated; u++) {
		PGAlignedBlock replacement;
		ItemPointerData tids[BLCKSZ / sizeof(ItemPointerData)];
		IndexTuple old = (IndexTuple)PageGetItem(result, PageGetItemId(result, updated[u]));
		uint16 removed, indices[BLCKSZ / sizeof(ItemPointerData)];
		int count, next = 0, kept = 0;
		if (!btree_posting_valid(old) || length - cursor < SizeOfBtreeUpdate)
			return false;
		memcpy(&removed, data + cursor, sizeof(removed));
		cursor += SizeOfBtreeUpdate;
		count = BTreeTupleGetNPosting(old);
		if (removed == 0 || removed >= count || removed > lengthof(indices)
			|| length - cursor < removed * sizeof(uint16))
			return false;
		memcpy(indices, data + cursor, removed * sizeof(uint16));
		cursor += removed * sizeof(uint16);
		for (int i = 0; i < removed; i++)
			if (indices[i] >= count || (i && indices[i - 1] >= indices[i]))
				return false;
		for (int i = 0; i < count; i++) {
			if (next < removed && indices[next] == i)
				next++;
			else
				tids[kept++] = *BTreeTupleGetPostingN(old, i);
		}
		if (!btree_form_posting(old, tids, kept, replacement.data, BTMaxItemSize(result))
			|| !PageIndexTupleOverwrite(result, updated[u], replacement.data,
										MAXALIGN(IndexTupleSize((IndexTuple)replacement.data))))
			return false;
	}
	if (cursor != length)
		return false;
	if (ndeleted)
		PageIndexMultiDelete(result, deleted, ndeleted);
	BTPageGetOpaque(result)->btpo_flags &= ~BTP_HAS_GARBAGE;
	return true;
}

static bool
btree_dedup(XLogReaderState *record, Page original, Page result)
{
	xl_btree_dedup rec;
	BTDedupInterval intervals[MaxIndexTuplesPerPage];
	BTPageOpaque opaque;
	OffsetNumber min, max, off, dest;
	Size length;
	const char *data = XLogRecGetBlockData(record, 0, &length);
	int next = 0;

	if (XLogRecGetData(record) == NULL || XLogRecGetDataLen(record) != SizeOfBtreeDedup
		|| !btree_page_valid(original, false))
		return false;
	memcpy(&rec, XLogRecGetData(record), SizeOfBtreeDedup);
	opaque = BTPageGetOpaque(original);
	min = P_FIRSTDATAKEY(opaque);
	max = PageGetMaxOffsetNumber(original);
	if (!P_ISLEAF(opaque) || min > max || rec.nintervals > max
		|| length != rec.nintervals * sizeof(BTDedupInterval) || (length && data == NULL))
		return false;
	if (length)
		memcpy(intervals, data, length);
	for (int i = 0; i < rec.nintervals; i++) {
		BTDedupInterval *v = &intervals[i];
		if (v->baseoff < min || v->nitems < 2 || (unsigned)v->baseoff + v->nitems > max + 1
			|| (i && intervals[i - 1].baseoff + intervals[i - 1].nitems > v->baseoff))
			return false;
	}
	PageInit(result, BLCKSZ, sizeof(BTPageOpaqueData));
	memcpy(PageGetSpecialPointer(result), PageGetSpecialPointer(original),
		   sizeof(BTPageOpaqueData));
	if (!P_RIGHTMOST(opaque)) {
		ItemId high = PageGetItemId(original, P_HIKEY);
		if (PageAddItem(result, PageGetItem(original, high), ItemIdGetLength(high), P_HIKEY, false,
						false)
			== InvalidOffsetNumber)
			return false;
	}
	dest = min;
	for (off = min; off <= max;) {
		PGAlignedBlock replacement;
		ItemPointerData tids[BLCKSZ / sizeof(ItemPointerData)];
		ItemId item = PageGetItemId(original, off);
		IndexTuple base = (IndexTuple)PageGetItem(original, item);
		const char *bytes = (char *)base;
		Size size = IndexTupleSize(base);
		unsigned span = 1, count = 0;
		if (BTreeTupleIsPivot(base) || (BTreeTupleIsPosting(base) && !btree_posting_valid(base)))
			return false;
		if (next < rec.nintervals && off == intervals[next].baseoff) {
			span = intervals[next++].nitems;
			for (unsigned j = 0; j < span; j++) {
				IndexTuple member
					= (IndexTuple)PageGetItem(original, PageGetItemId(original, off + j));
				unsigned n = 1;
				ItemPointer from = &member->t_tid;
				if (BTreeTupleIsPivot(member))
					return false;
				if (BTreeTupleIsPosting(member)) {
					if (!btree_posting_valid(member))
						return false;
					n = BTreeTupleGetNPosting(member);
					from = BTreeTupleGetPosting(member);
				}
				if (count + n > lengthof(tids))
					return false;
				memcpy(tids + count, from, n * sizeof(ItemPointerData));
				count += n;
			}
			if (!btree_form_posting(base, tids, count, replacement.data, BTMaxItemSize(result)))
				return false;
			bytes = replacement.data;
			size = IndexTupleSize((IndexTuple)bytes);
		}
		if (PageAddItem(result, (Item)bytes, size, dest++, false, false) == InvalidOffsetNumber)
			return false;
		off += span;
	}
	BTPageGetOpaque(result)->btpo_flags &= ~BTP_HAS_GARBAGE;
	return next == rec.nintervals;
}

ClusterBlkApplyResult
cluster_block_apply_btree(XLogReaderState *record, uint8 block_id, char *page)
{
	PGAlignedBlock scratch;
	uint8 operation = XLogRecGetInfo(record) & ~XLR_INFO_MASK;
	const char *main_data = XLogRecGetData(record);
	const DecodedBkpBlock *block = XLogRecGetBlock(record, block_id);
	xl_btree_insert insert = { 0 };
	xl_btree_newroot root = { 0 };
	bool initializing = false, child = false, meta = false;
	uint8 mask;

	if (XLogRecGetRmid(record) != RM_BTREE_ID)
		return CLUSTER_BLKAPPLY_UNSUPPORTED;
	if (operation == XLOG_BTREE_VACUUM || operation == XLOG_BTREE_DELETE
		|| operation == XLOG_BTREE_DEDUP) {
		if (block_id != 0 || !btree_record_blocks_valid(record, 1) || block->blkno == BTREE_METAPAGE
			|| (block->flags & BKPBLOCK_WILL_INIT)
			|| !(operation == XLOG_BTREE_DEDUP ? btree_dedup(record, page, scratch.data)
											   : btree_delete_items(record, page, scratch.data)))
			return CLUSTER_BLKAPPLY_FAILED;
		PageSetLSN(scratch.data, record->EndRecPtr);
		memcpy(page, scratch.data, BLCKSZ);
		return CLUSTER_BLKAPPLY_OK;
	}
	if (operation == XLOG_BTREE_SPLIT_L || operation == XLOG_BTREE_SPLIT_R
		|| operation == XLOG_BTREE_MARK_PAGE_HALFDEAD || operation == XLOG_BTREE_UNLINK_PAGE
		|| operation == XLOG_BTREE_UNLINK_PAGE_META) {
		bool applied;
		if (operation == XLOG_BTREE_MARK_PAGE_HALFDEAD)
			applied = btree_mark_halfdead(record, block_id, page, scratch.data);
		else if (operation == XLOG_BTREE_UNLINK_PAGE || operation == XLOG_BTREE_UNLINK_PAGE_META)
			applied = btree_unlink(record, block_id, page, scratch.data);
		else
			applied = btree_split(record, block_id, page, scratch.data);
		if (!applied)
			return CLUSTER_BLKAPPLY_FAILED;
		PageSetLSN(scratch.data, record->EndRecPtr);
		memcpy(page, scratch.data, BLCKSZ);
		return CLUSTER_BLKAPPLY_OK;
	}
	switch (operation) {
	case XLOG_BTREE_INSERT_LEAF:
	case XLOG_BTREE_INSERT_POST:
	case XLOG_BTREE_INSERT_UPPER:
	case XLOG_BTREE_INSERT_META:
		if (main_data == NULL || XLogRecGetDataLen(record) != SizeOfBtreeInsert)
			return CLUSTER_BLKAPPLY_FAILED;
		memcpy(&insert, main_data, SizeOfBtreeInsert);
		if (insert.offnum < FirstOffsetNumber || insert.offnum > MaxIndexTuplesPerPage)
			return CLUSTER_BLKAPPLY_FAILED;
		mask = operation == XLOG_BTREE_INSERT_LEAF || operation == XLOG_BTREE_INSERT_POST ? 1
			   : operation == XLOG_BTREE_INSERT_UPPER									  ? 3
																						  : 7;
		child = block_id == 1;
		meta = block_id == 2;
		break;
	case XLOG_BTREE_META_CLEANUP:
		if (XLogRecGetDataLen(record) != 0)
			return CLUSTER_BLKAPPLY_FAILED;
		mask = 1;
		meta = true;
		break;
	case XLOG_BTREE_NEWROOT:
		if (main_data == NULL || XLogRecGetDataLen(record) != SizeOfBtreeNewroot)
			return CLUSTER_BLKAPPLY_FAILED;
		memcpy(&root, main_data, SizeOfBtreeNewroot);
		mask = root.level == 0 ? 5 : 7;
		child = block_id == 1;
		meta = block_id == 2;
		initializing = block_id == 0;
		if (!XLogRecHasBlockRef(record, 0) || XLogRecGetBlock(record, 0)->blkno != root.rootblk)
			return CLUSTER_BLKAPPLY_FAILED;
		break;
	default:
		return CLUSTER_BLKAPPLY_UNSUPPORTED;
	}
	if (block_id > 2 || !(mask & (1 << block_id)) || !btree_record_blocks_valid(record, mask))
		return CLUSTER_BLKAPPLY_FAILED;
	if (meta) {
		if (!btree_restore_meta(record, block_id, scratch.data))
			return CLUSTER_BLKAPPLY_FAILED;
	} else if (initializing) {
		BTPageOpaque opaque;
		Size length;
		const char *data = XLogRecGetBlockData(record, block_id, &length);

		if (block->blkno == BTREE_METAPAGE || !(block->flags & BKPBLOCK_WILL_INIT))
			return CLUSTER_BLKAPPLY_FAILED;
		PageInit(scratch.data, BLCKSZ, sizeof(BTPageOpaqueData));
		opaque = BTPageGetOpaque(scratch.data);
		opaque->btpo_flags = BTP_ROOT | (root.level == 0 ? BTP_LEAF : 0);
		opaque->btpo_prev = opaque->btpo_next = P_NONE;
		opaque->btpo_level = root.level;
		opaque->btpo_cycleid = 0;
		if (root.level > 0 && (data == NULL || !btree_restore_items(scratch.data, data, length)))
			return CLUSTER_BLKAPPLY_FAILED;
		if (root.level > 0 && PageGetMaxOffsetNumber(scratch.data) != 2)
			return CLUSTER_BLKAPPLY_FAILED;
		if (root.level == 0 && length != 0)
			return CLUSTER_BLKAPPLY_FAILED;
	} else {
		BTPageOpaque opaque;
		if (block->blkno == BTREE_METAPAGE || (block->flags & BKPBLOCK_WILL_INIT)
			|| !btree_page_valid(page, false))
			return CLUSTER_BLKAPPLY_FAILED;
		memcpy(scratch.data, page, BLCKSZ);
		opaque = BTPageGetOpaque(scratch.data);
		if (child) {
			if (!P_INCOMPLETE_SPLIT(opaque) || block->data_len != 0)
				return CLUSTER_BLKAPPLY_FAILED;
			opaque->btpo_flags &= ~BTP_INCOMPLETE_SPLIT;
		} else if (operation == XLOG_BTREE_INSERT_POST) {
			if (!P_ISLEAF(opaque) || !btree_insert_posting(record, scratch.data, insert.offnum))
				return CLUSTER_BLKAPPLY_FAILED;
		} else {
			Size length, tuple_size;
			const char *data = XLogRecGetBlockData(record, block_id, &length);
			if (!btree_tuple_size(data, length, &tuple_size) || tuple_size != length
				|| insert.offnum > PageGetMaxOffsetNumber(scratch.data) + 1
				|| P_ISLEAF(opaque) != (operation == XLOG_BTREE_INSERT_LEAF)
				|| PageAddItem(scratch.data, (Item)data, length, insert.offnum, false, false)
					   == InvalidOffsetNumber)
				return CLUSTER_BLKAPPLY_FAILED;
		}
	}
	PageSetLSN(scratch.data, record->EndRecPtr);
	memcpy(page, scratch.data, BLCKSZ);
	return CLUSTER_BLKAPPLY_OK;
}
#endif
