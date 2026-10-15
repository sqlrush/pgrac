/*-------------------------------------------------------------------------
 * cluster_recovery_page_census.c
 *   Cold heap/history references joined to the observed transaction forest.
 *
 * Portions Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 *-------------------------------------------------------------------------
 */
#include "postgres.h"
#include "cluster/cluster_recovery_page_census.h"

#ifdef USE_PGRAC_CLUSTER
struct ClusterRecoveryPageCensus {
	ClusterSpaceIdentity identity;
	ClusterRecoveryReferenceGraph *graph;
	ClusterRecoveryPageHistory *history;
	ClusterRecoveryHeapScan *heap;
	ClusterRecoveryGraphLookup *lookup;
	ClusterRecoveryHeapEntry pending;
	uint64 version;
	bool inverse;
	ClusterRecoveryPageCensusResult status;
};

static ClusterRecoveryPageCensusResult
page_census_fail(ClusterRecoveryPageCensus *census, ClusterRecoveryPageCensusResult result)
{
	if (census != NULL) {
		census->status = result;
		cluster_recovery_reference_graph_invalidate(census->graph);
	}
	return result;
}

static ClusterRecoveryPageCensusResult
page_census_history_result(ClusterRecoveryPageCensus *census, ClusterRecoveryHistoryResult result)
{
	ClusterRecoveryPageCensusResult mapped;

	switch (result) {
	case CLUSTER_RECOVERY_HISTORY_BASE_REQUIRED:
		mapped = CLUSTER_RECOVERY_PAGE_BASE_REQUIRED;
		break;
	case CLUSTER_RECOVERY_HISTORY_CAPACITY:
		mapped = CLUSTER_RECOVERY_PAGE_CAPACITY;
		break;
	case CLUSTER_RECOVERY_HISTORY_IDENTITY:
		mapped = CLUSTER_RECOVERY_PAGE_IDENTITY;
		break;
	case CLUSTER_RECOVERY_HISTORY_INVALID:
		mapped = CLUSTER_RECOVERY_PAGE_INVALID;
		break;
	default:
		mapped = CLUSTER_RECOVERY_PAGE_CORRUPT;
		break;
	}
	return page_census_fail(census, mapped);
}

static ClusterRecoveryPageCensusResult
page_census_lookup_result(ClusterRecoveryPageCensus *census,
						  ClusterRecoveryGraphLookupResult result)
{
	ClusterRecoveryPageCensusResult mapped;

	switch (result) {
	case CLUSTER_RECOVERY_LOOKUP_MISSING:
		mapped = CLUSTER_RECOVERY_PAGE_BASE_REQUIRED;
		break;
	case CLUSTER_RECOVERY_LOOKUP_CAPACITY:
		mapped = CLUSTER_RECOVERY_PAGE_CAPACITY;
		break;
	case CLUSTER_RECOVERY_LOOKUP_CHANGED:
		mapped = CLUSTER_RECOVERY_PAGE_CHANGED;
		break;
	case CLUSTER_RECOVERY_LOOKUP_IDENTITY:
		mapped = CLUSTER_RECOVERY_PAGE_IDENTITY;
		break;
	default:
		mapped = CLUSTER_RECOVERY_PAGE_INVALID;
		break;
	}
	return page_census_fail(census, mapped);
}

ClusterRecoveryPageCensusResult
cluster_recovery_page_census_begin(const ClusterRecoveryRelationPage *page,
								   ClusterRecoveryReferenceGraph *graph, size_t memory_budget,
								   ClusterRecoveryPageCensus **out)
{
	ClusterRecoveryPageCensus *census;
	ClusterRecoveryHistoryResult result;
	/* The heap scanner owns less than two pages; one page is a conservative
	 * fixed lookup budget checked by its own allocator. History receives the
	 * rest, including its validator scratch, without borrowing graph budget. */
	size_t fixed = sizeof(*census) + 3 * BLCKSZ;

	if (out != NULL)
		*out = NULL;
	if (out == NULL || page == NULL || page->page == NULL || page->forknum != MAIN_FORKNUM
		|| page->block == InvalidBlockNumber || page->block >= page->reservation.next_block
		|| page->identity_token == 0 || page->reservation_token == 0
		|| !cluster_recovery_reference_graph_observed(graph))
		return CLUSTER_RECOVERY_PAGE_INVALID;
	if (memory_budget <= fixed)
		return CLUSTER_RECOVERY_PAGE_CAPACITY;
	census = calloc(1, sizeof(*census));
	if (census == NULL)
		return CLUSTER_RECOVERY_PAGE_CAPACITY;
	census->graph = graph;
	census->identity = page->reservation.identity;
	result = cluster_recovery_page_history_begin(&census->identity, page->forknum, page->block,
												 page->page, BLCKSZ, memory_budget - fixed,
												 &census->history);
	if (result != CLUSTER_RECOVERY_HISTORY_MORE) {
		ClusterRecoveryPageCensusResult failed = page_census_history_result(census, result);
		cluster_recovery_page_census_destroy(census);
		return failed;
	}
	*out = census;
	return CLUSTER_RECOVERY_PAGE_MORE;
}

static ClusterRecoveryPageCensusResult
page_census_lookup_begin(ClusterRecoveryPageCensus *census, const ClusterTxLocator *locator)
{
	ClusterRecoveryGraphLookupResult result = cluster_recovery_reference_lookup_begin(
		census->graph, census->identity.key.system_identifier,
		census->identity.key.database_incarnation, locator, BLCKSZ, &census->lookup);

	return result == CLUSTER_RECOVERY_LOOKUP_MORE ? CLUSTER_RECOVERY_PAGE_MORE
												  : page_census_lookup_result(census, result);
}

ClusterRecoveryPageCensusResult
cluster_recovery_page_census_next(ClusterRecoveryPageCensus *census,
								  ClusterRecoveryPageReference *out)
{
	if (out != NULL)
		memset(out, 0, sizeof(*out));
	if (census == NULL || out == NULL)
		return page_census_fail(census, CLUSTER_RECOVERY_PAGE_INVALID);
	if (census->status >= CLUSTER_RECOVERY_PAGE_INVALID)
		return census->status;
	if (!cluster_recovery_reference_graph_observed(census->graph))
		return page_census_fail(census, CLUSTER_RECOVERY_PAGE_CHANGED);
	if (census->status == CLUSTER_RECOVERY_PAGE_DRAINED)
		return census->status;
	if (census->lookup != NULL) {
		ClusterRecoveryGraphRecord record;
		ClusterRecoveryGraphLookupResult result
			= cluster_recovery_reference_lookup_next(census->lookup, &record);

		if (result == CLUSTER_RECOVERY_LOOKUP_MORE)
			return CLUSTER_RECOVERY_PAGE_MORE;
		if (result != CLUSTER_RECOVERY_LOOKUP_RECORD)
			return page_census_lookup_result(census, result);
		if (record.record->header.target_locator.spcOid != census->identity.key.locator.spcOid
			|| record.record->header.target_locator.dbOid != census->identity.key.locator.dbOid
			|| record.record->header.target_locator.relNumber
				   != census->identity.key.locator.relNumber
			|| record.record->header.target_fork != MAIN_FORKNUM)
			return page_census_fail(census, CLUSTER_RECOVERY_PAGE_IDENTITY);
		cluster_recovery_reference_lookup_destroy(census->lookup);
		census->lookup = NULL;
		if (census->inverse) {
			ClusterRecoveryHistoryResult accepted;

			if (census->version == UINT64_MAX)
				return page_census_fail(census, CLUSTER_RECOVERY_PAGE_CAPACITY);
			accepted = cluster_recovery_page_history_accept(census->history, &record.transaction,
															record.record_segment_generation,
															record.record);
			if (accepted != CLUSTER_RECOVERY_HISTORY_MORE)
				return page_census_history_result(census, accepted);
			census->version++;
			census->inverse = false;
			return CLUSTER_RECOVERY_PAGE_MORE;
		}
		out->version = census->version;
		out->entry = census->pending;
		out->transaction = record;
		return CLUSTER_RECOVERY_PAGE_ENTRY;
	}
	if (census->heap != NULL) {
		ClusterRecoveryHeapEntry entry;
		ClusterRecoveryHeapResult result = cluster_recovery_heap_scan_next(census->heap, &entry);

		if (result == CLUSTER_RECOVERY_HEAP_END) {
			cluster_recovery_heap_scan_destroy(census->heap);
			census->heap = NULL;
			return CLUSTER_RECOVERY_PAGE_MORE;
		}
		if (result != CLUSTER_RECOVERY_HEAP_ENTRY)
			return page_census_fail(census, CLUSTER_RECOVERY_PAGE_CORRUPT);
		if (entry.kind == CLUSTER_RECOVERY_HEAP_ITL) {
			if (entry.itl.flags == ITL_FLAG_FREE) {
				if (!cluster_undo_history_prior_valid(&entry.itl))
					return page_census_fail(census, CLUSTER_RECOVERY_PAGE_CORRUPT);
			} else {
				ClusterTxLocator locator = { 0 };
				uint32 segment, block;
				uint16 slot, row;

				if (!TransactionIdIsNormal(entry.itl.xid) || !SCN_VALID(entry.itl.write_scn)
					|| !uba_decode_record(entry.itl.undo_segment_head, &segment, &block, &slot,
										  &row))
					return page_census_fail(census, CLUSTER_RECOVERY_PAGE_BASE_REQUIRED);
				locator.uba = entry.itl.undo_segment_head;
				locator.xid = entry.itl.xid;
				locator.itl_kind = entry.itl.flags;
				locator.itl_slot_index = entry.index;
				locator.tt_wrap = TT_WRAP_INVALID;
				census->pending = entry;
				return page_census_lookup_begin(census, &locator);
			}
		}
		out->version = census->version;
		out->entry = entry;
		return CLUSTER_RECOVERY_PAGE_ENTRY;
	}
	{
		ClusterRecoveryHistoryEvent event;
		ClusterRecoveryHistoryResult result
			= cluster_recovery_page_history_next(census->history, &event);

		if (result == CLUSTER_RECOVERY_HISTORY_PAGE) {
			ClusterRecoveryHeapResult heap
				= cluster_recovery_heap_scan_begin(event.page, BLCKSZ, &census->heap);
			if (heap != CLUSTER_RECOVERY_HEAP_MORE)
				return page_census_fail(census, heap == CLUSTER_RECOVERY_HEAP_CAPACITY
													? CLUSTER_RECOVERY_PAGE_CAPACITY
													: CLUSTER_RECOVERY_PAGE_CORRUPT);
			return CLUSTER_RECOVERY_PAGE_MORE;
		}
		if (result == CLUSTER_RECOVERY_HISTORY_NEED_RECORD) {
			census->inverse = true;
			return page_census_lookup_begin(census, &event.locator);
		}
		if (result == CLUSTER_RECOVERY_HISTORY_DRAINED)
			return census->status = CLUSTER_RECOVERY_PAGE_DRAINED;
		return page_census_history_result(census, result);
	}
}

void
cluster_recovery_page_census_destroy(ClusterRecoveryPageCensus *census)
{
	if (census != NULL) {
		cluster_recovery_reference_lookup_destroy(census->lookup);
		cluster_recovery_heap_scan_destroy(census->heap);
		cluster_recovery_page_history_destroy(census->history);
		free(census);
	}
}
#endif
