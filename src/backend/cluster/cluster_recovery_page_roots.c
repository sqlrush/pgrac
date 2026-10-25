/*-------------------------------------------------------------------------
 * cluster_recovery_page_roots.c
 *   Read-only PAGE/history roots for the unstarted cold TX graph.
 *
 * Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 *-------------------------------------------------------------------------
 */
#include "postgres.h"
#include "cluster/cluster_recovery_page_roots.h"
#include "cluster/cluster_recovery_heap_scan.h"

#ifdef USE_PGRAC_CLUSTER
struct ClusterRecoveryPageRoots {
	ClusterRecoveryUndoStore *store;
	ClusterRecoveryRootSet *roots;
	ClusterRecoveryReferenceGraph *graph;
	ClusterRecoveryRelationScan *relation;
	ClusterRecoveryPageHistory *history;
	ClusterRecoveryHeapScan *heap;
	ClusterRecoveryRelationPage page;
	ClusterRecoveryUndoRecord record;
	ClusterTxLocator pending;
	ClusterRecoveryPageRootsDiag diag;
	uint8 root_digest[32];
	uint32 root_count, next_root;
	size_t history_budget;
	bool reading, inverse;
	ClusterRecoveryGraphResult status;
};

static ClusterRecoveryGraphResult
page_roots_fail(ClusterRecoveryPageRoots *cursor, ClusterRecoveryGraphResult result)
{
	cursor->status = result;
	cluster_recovery_reference_graph_invalidate(cursor->graph);
	cluster_recovery_root_set_invalidate(cursor->roots);
	return result;
}

ClusterRecoveryGraphResult
cluster_recovery_page_roots_begin(ClusterRecoveryUndoStore *store, size_t memory_budget,
								  ClusterRecoveryPageRoots **out)
{
	ClusterRecoveryReferenceGraph *graph = cluster_recovery_undo_store_graph(store);
	ClusterRecoveryRootSet *roots = cluster_recovery_undo_store_root_set(store);
	ClusterRecoveryPageRoots *cursor;
	uint8 digest[32];
	/* Eight pages for the original relation reader and its codec scratch;
	 * two for the original copied heap scanner. History owns the remainder. */
	size_t fixed = sizeof(*cursor) + 10 * BLCKSZ;

	if (out != NULL)
		*out = NULL;
	if (out == NULL || !cluster_recovery_reference_graph_accepts_roots(graph)
		|| !cluster_recovery_root_set_digest(roots, digest))
		return CLUSTER_RECOVERY_GRAPH_INVALID;
	if (memory_budget <= fixed)
		return CLUSTER_RECOVERY_GRAPH_CAPACITY;
	cursor = calloc(1, sizeof(*cursor));
	if (cursor == NULL)
		return CLUSTER_RECOVERY_GRAPH_CAPACITY;
	cursor->store = store;
	cursor->roots = roots;
	cursor->graph = graph;
	cursor->root_count = cluster_recovery_root_set_count(roots);
	memcpy(cursor->root_digest, digest, 32);
	cursor->history_budget = memory_budget - fixed;
	*out = cursor;
	return CLUSTER_RECOVERY_GRAPH_MORE;
}

static ClusterRecoveryGraphResult
page_roots_record(ClusterRecoveryPageRoots *cursor)
{
	ClusterRecoveryTxnKey key;
	uint32 generation;
	ClusterRecoveryGraphResult result = cluster_recovery_undo_store_read_base(
		cursor->store, cursor->pending.uba, cursor->pending.xid, &key, &cursor->record, &generation);

	cursor->diag.graph_result = result;
	if (result == CLUSTER_RECOVERY_GRAPH_PENDING) {
		cursor->diag.deferred_references++;
		if (cursor->inverse) {
			cursor->diag.deferred_histories++;
			cluster_recovery_page_history_destroy(cursor->history);
			cursor->history = NULL;
		}
	} else if (result == CLUSTER_RECOVERY_GRAPH_RECORD) {
		const ClusterSpaceIdentityKey *target = &cursor->page.reservation.identity.key;
		const UndoRecordHeader *record = &cursor->record.header;

		if (key.system_identifier != target->system_identifier
			|| key.database_incarnation != target->database_incarnation
			|| record->target_locator.spcOid != target->locator.spcOid
			|| record->target_locator.dbOid != target->locator.dbOid
			|| record->target_locator.relNumber != target->locator.relNumber
			|| record->target_fork != MAIN_FORKNUM)
			return page_roots_fail(cursor, CLUSTER_RECOVERY_GRAPH_IDENTITY);
		result = cluster_recovery_reference_graph_add(cursor->graph, &key, cursor->pending.uba);
		if (result != CLUSTER_RECOVERY_GRAPH_MORE)
			return page_roots_fail(cursor, result);
		cursor->diag.references++;
		if (cursor->inverse) {
			cursor->diag.history_result = cluster_recovery_page_history_accept(
				cursor->history, &key, generation, &cursor->record);
			if (cursor->diag.history_result != CLUSTER_RECOVERY_HISTORY_MORE)
				return page_roots_fail(cursor,
					cursor->diag.history_result == CLUSTER_RECOVERY_HISTORY_CAPACITY
						? CLUSTER_RECOVERY_GRAPH_CAPACITY : CLUSTER_RECOVERY_GRAPH_CORRUPT);
		}
	} else
		return page_roots_fail(cursor, result);
	cursor->reading = cursor->inverse = false;
	return CLUSTER_RECOVERY_GRAPH_MORE;
}

static ClusterRecoveryGraphResult
page_roots_next(ClusterRecoveryPageRoots *cursor)
{
	uint8 digest[32];

	if (cursor->status > CLUSTER_RECOVERY_GRAPH_DRAINED)
		return cursor->status;
	if (!cluster_recovery_reference_graph_accepts_roots(cursor->graph)
		|| cluster_recovery_undo_store_graph(cursor->store) != cursor->graph
		|| !cluster_recovery_root_set_digest(cursor->roots, digest)
		|| memcmp(digest, cursor->root_digest, 32) != 0
		|| cluster_recovery_root_set_count(cursor->roots) != cursor->root_count)
		return page_roots_fail(cursor, CLUSTER_RECOVERY_GRAPH_CHANGED);
	if (cursor->status == CLUSTER_RECOVERY_GRAPH_DRAINED)
		return cursor->status;
	if (cursor->reading)
		return page_roots_record(cursor);
	if (cursor->heap != NULL) {
		ClusterRecoveryHeapEntry entry;
		ClusterRecoveryHeapResult result = cluster_recovery_heap_scan_next(cursor->heap, &entry);

		if (result == CLUSTER_RECOVERY_HEAP_END) {
			cluster_recovery_heap_scan_destroy(cursor->heap);
			cursor->heap = NULL;
		} else if (result != CLUSTER_RECOVERY_HEAP_ENTRY)
			return page_roots_fail(cursor, CLUSTER_RECOVERY_GRAPH_CORRUPT);
		else if (entry.kind == CLUSTER_RECOVERY_HEAP_ITL) {
			if (entry.itl.flags == ITL_FLAG_FREE) {
				if (!cluster_undo_history_prior_valid(&entry.itl))
					return page_roots_fail(cursor, CLUSTER_RECOVERY_GRAPH_CORRUPT);
			} else {
				if (!SCN_VALID(entry.itl.write_scn))
					return page_roots_fail(cursor, CLUSTER_RECOVERY_GRAPH_IDENTITY);
				cursor->pending = (ClusterTxLocator){ 0 };
				cursor->pending.uba = entry.itl.undo_segment_head;
				cursor->pending.xid = entry.itl.xid;
				cursor->pending.tt_wrap = TT_WRAP_INVALID;
				cursor->pending.itl_kind = entry.itl.flags;
				cursor->pending.itl_slot_index = entry.index;
				cursor->reading = true;
			}
		}
		return CLUSTER_RECOVERY_GRAPH_MORE;
	}
	if (cursor->history != NULL) {
		ClusterRecoveryHistoryEvent event;
		ClusterRecoveryHistoryResult result;

		result = cluster_recovery_page_history_next(cursor->history, &event);
		cursor->diag.history_result = result;
		if (result == CLUSTER_RECOVERY_HISTORY_PAGE) {
			ClusterRecoveryHeapResult heap = cluster_recovery_heap_scan_begin(event.page, BLCKSZ,
																			  &cursor->heap);
			if (heap != CLUSTER_RECOVERY_HEAP_MORE)
				return page_roots_fail(cursor, heap == CLUSTER_RECOVERY_HEAP_CAPACITY
					? CLUSTER_RECOVERY_GRAPH_CAPACITY : CLUSTER_RECOVERY_GRAPH_CORRUPT);
			cursor->diag.versions++;
		} else if (result == CLUSTER_RECOVERY_HISTORY_NEED_RECORD) {
			cursor->pending = event.locator;
			cursor->reading = cursor->inverse = true;
		} else if (result == CLUSTER_RECOVERY_HISTORY_DRAINED) {
			cluster_recovery_page_history_destroy(cursor->history);
			cursor->history = NULL;
		} else
			return page_roots_fail(cursor, result == CLUSTER_RECOVERY_HISTORY_CAPACITY
				? CLUSTER_RECOVERY_GRAPH_CAPACITY : CLUSTER_RECOVERY_GRAPH_CORRUPT);
		return CLUSTER_RECOVERY_GRAPH_MORE;
	}
	if (cursor->relation != NULL) {
		ClusterRecoveryRelationResult result;

		result = cluster_recovery_relation_next(cursor->relation, &cursor->page);
		cursor->diag.relation_result = result;
		if (result == CLUSTER_RECOVERY_RELATION_PAGE) {
			cursor->diag.pages++;
			if (PageHasItl((Page)cursor->page.page)) {
				if (cursor->page.block >= cursor->page.reservation.next_block)
					return page_roots_fail(cursor, CLUSTER_RECOVERY_GRAPH_IDENTITY);
				cursor->diag.history_result = cluster_recovery_page_history_begin(
					&cursor->page.reservation.identity, MAIN_FORKNUM, cursor->page.block,
					cursor->page.page, BLCKSZ, cursor->history_budget, &cursor->history);
				if (cursor->diag.history_result != CLUSTER_RECOVERY_HISTORY_MORE)
					return page_roots_fail(cursor,
						cursor->diag.history_result == CLUSTER_RECOVERY_HISTORY_CAPACITY
							? CLUSTER_RECOVERY_GRAPH_CAPACITY : CLUSTER_RECOVERY_GRAPH_CORRUPT);
			}
		} else if (result == CLUSTER_RECOVERY_RELATION_DRAINED) {
			cluster_recovery_relation_destroy(cursor->relation);
			cursor->relation = NULL;
		} else if (result != CLUSTER_RECOVERY_RELATION_MORE)
			return page_roots_fail(cursor, result == CLUSTER_RECOVERY_RELATION_CAPACITY
				? CLUSTER_RECOVERY_GRAPH_CAPACITY : CLUSTER_RECOVERY_GRAPH_IO);
		return CLUSTER_RECOVERY_GRAPH_MORE;
	}
	for (unsigned work = 0; work < 128 && cursor->next_root < cursor->root_count; work++) {
		uint32 index = cursor->next_root++;
		const ClusterRecoveryRootEntry *entry = cluster_recovery_root_set_entry(cursor->roots, index);

		if (entry == NULL)
			return page_roots_fail(cursor, CLUSTER_RECOVERY_GRAPH_CHANGED);
		if (entry->file.kind != CLUSTER_RECOVERY_FILE_RELATION
			|| entry->file.forknum != MAIN_FORKNUM
			|| (entry->origins & CLUSTER_RECOVERY_ROOT_PHYSICAL) == 0)
			continue;
		cursor->diag.relation_result = cluster_recovery_relation_begin(
			cursor->roots, index, 8 * BLCKSZ, &cursor->relation);
		/* Keep structural absence for the unchanged later owner; it is never
		 * a proof that this relation has no PAGE/history references. */
		if (cursor->diag.relation_result == CLUSTER_RECOVERY_RELATION_STRUCTURE_REQUIRED) {
			cursor->diag.deferred_relations++;
			return CLUSTER_RECOVERY_GRAPH_MORE;
		}
		if (cursor->diag.relation_result != CLUSTER_RECOVERY_RELATION_MORE)
			return page_roots_fail(cursor,
				cursor->diag.relation_result == CLUSTER_RECOVERY_RELATION_CAPACITY
					? CLUSTER_RECOVERY_GRAPH_CAPACITY : CLUSTER_RECOVERY_GRAPH_IO);
		return CLUSTER_RECOVERY_GRAPH_MORE;
	}
	if (cursor->next_root != cursor->root_count)
		return CLUSTER_RECOVERY_GRAPH_MORE;
	return cursor->status = CLUSTER_RECOVERY_GRAPH_DRAINED;
}

ClusterRecoveryGraphResult
cluster_recovery_page_roots_step(ClusterRecoveryPageRoots *cursor, ClusterRecoveryPageRootsDiag *diag)
{
	ClusterRecoveryGraphResult result;

	if (diag != NULL)
		memset(diag, 0, sizeof(*diag));
	if (cursor == NULL)
		return CLUSTER_RECOVERY_GRAPH_INVALID;
	result = page_roots_next(cursor);
	if (diag != NULL)
		*diag = cursor->diag;
	return result;
}

void
cluster_recovery_page_roots_destroy(ClusterRecoveryPageRoots *cursor)
{
	if (cursor != NULL) {
		cluster_recovery_heap_scan_destroy(cursor->heap);
		cluster_recovery_page_history_destroy(cursor->history);
		cluster_recovery_relation_destroy(cursor->relation);
		free(cursor);
	}
}
#endif
