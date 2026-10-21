/*-------------------------------------------------------------------------
 * cluster_heap_freeze.c
 *    Bounded physical input capture for shared heap freeze preparation.
 *
 * Portions Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/htup_details.h"
#include "cluster/cluster_heap_freeze.h"

bool
cluster_heap_freeze_capture_page_v1(const ClusterSpaceIdentity *identity,
								  BlockNumber block, Page page,
								  ClusterHeapFreezePageInputV1 *out)
{
	PageHeader header;
	OffsetNumber maxoff;
	uint16 normal = 0;
	uint16 multi = 0;

	if (identity == NULL || identity->state != CLUSTER_SPACE_IDENTITY_LIVE
		|| block == InvalidBlockNumber || page == NULL || out == NULL)
		return false;

	header = (PageHeader) page;
	if (PageGetPageSize(page) != BLCKSZ
		|| PageGetPageLayoutVersion(page) != PG_PAGE_LAYOUT_VERSION
		|| header->pd_lower < SizeOfPageHeaderData
		|| header->pd_lower > header->pd_upper
		|| header->pd_upper > header->pd_special
		|| header->pd_special > BLCKSZ
		|| (header->pd_lower - SizeOfPageHeaderData) % sizeof(ItemIdData) != 0
		|| header->pd_special != MAXALIGN(header->pd_special))
		return false;

	maxoff = PageGetMaxOffsetNumber(page);
	if (maxoff > MaxHeapTuplesPerPage
		|| (PageHasItl(page) && PageGetSpecialSize(page) < CLUSTER_ITL_ARRAY_SIZE))
		return false;

	/* Validate every dereference before publishing any part of the output. */
	for (OffsetNumber offnum = FirstOffsetNumber; offnum <= maxoff;
		 offnum = OffsetNumberNext(offnum))
	{
		ItemId item = PageGetItemId(page, offnum);
		unsigned offset;
		unsigned length;
		HeapTupleHeader tuple;

		if (!ItemIdIsNormal(item))
			continue;
		offset = ItemIdGetOffset(item);
		length = ItemIdGetLength(item);
		if (offset < header->pd_upper || offset != MAXALIGN(offset)
			|| length < SizeofHeapTupleHeader
			|| offset + length > header->pd_special)
			return false;
		tuple = (HeapTupleHeader) (page + offset);
		if (tuple->t_hoff < SizeofHeapTupleHeader || tuple->t_hoff > length)
			return false;
		normal++;
		if (tuple->t_infomask & HEAP_XMAX_IS_MULTI)
			multi++;
	}

	/* The image preserves physical identity; counts never establish proof. */
	out->identity = *identity;
	out->block = block;
	out->max_offset = maxoff;
	out->normal_tuples = normal;
	out->multi_tuples = multi;
	memcpy(out->image.data, page, BLCKSZ);
	return true;
}
