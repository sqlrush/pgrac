/*-------------------------------------------------------------------------
 * cluster_recovery_native_inventory.c
 *    Two-pass native effects under the original cold input scope.
 *
 * Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 *-------------------------------------------------------------------------
 */
#include "postgres.h"
#ifdef USE_PGRAC_CLUSTER
#include "catalog/storage_xlog.h"
#include "cluster/cluster_recovery_native_inventory.h"
#include "cluster/cluster_recovery_record_hash.h"
#include "cluster/cluster_recovery_wal_targets.h"
#include "cluster/cluster_side_undo.h"

#define INVENTORY_SEED_TICK 128

typedef struct NativeInventorySource {
	ClusterWalSourceRef source;
	RfContributorStreamCutV1 cut;
	ClusterWalTailObservation observed;
	uint8 transcript[32];
} NativeInventorySource;

typedef struct NativeInventoryRecord {
	struct NativeInventoryRecord *next;
	ClusterRecoveryNativeScan *scan;
	uint32 source_index;
	uint64 ordinal;
} NativeInventoryRecord;

struct ClusterRecoveryNativeInventory {
	NativeInventorySource *sources;
	NativeInventoryRecord *head;
	NativeInventoryRecord *tail;
	NativeInventoryRecord *cursor;
	ClusterRecoveryRootSet *roots; /* borrowed; no further access after collection */
	pg_cryptohash_ctx *hash;
	ClusterRecoveryNativeInventoryDiag diag;
	ClusterRecoveryNativeInventoryResult status;
	size_t budget;
	uint32 source_count;
	uint32 source_cursor;
	uint64 source_records;
	unsigned pass;
	bool active;
	bool complete;
	ClusterRecoveryReferenceGraph *seed_graph; /* borrowed; outlives this inventory */
	NativeInventoryRecord *seed_cursor;
	NativeInventoryRecord *seed_source_first;
	NativeInventoryRecord *seed_search;
	const ClusterUndoDecoded *seed_binding; /* original immutable TT_BIND */
	bool seed_searching;
	bool seed_done;
	ClusterRecoveryRootSet *mx_roots; /* borrowed; outlives this inventory */
	ClusterRecoveryMultixactJoin *mx_join;
	NativeInventoryRecord *mx_cursor;
	size_t mx_budget;
	uint8 mx_digest[32];
	ClusterRecoveryMultixactJoinResult mx_status;
	bool mx_done;
};

static void
inventory_revoke_dependents(ClusterRecoveryNativeInventory *work)
{
	if (work != NULL && work->mx_roots != NULL)
		cluster_recovery_root_set_invalidate(work->mx_roots);
	if (work != NULL && work->seed_graph != NULL) {
		cluster_recovery_reference_graph_invalidate(work->seed_graph);
		if (work->diag.graph_result < CLUSTER_RECOVERY_GRAPH_INVALID)
			work->diag.graph_result = CLUSTER_RECOVERY_GRAPH_CHANGED;
	}
}

static bool
inventory_source_equal(const ClusterWalSourceRef *a, const ClusterWalSourceRef *b)
{
	return cluster_control_root_identity_equal(&a->claim.identity, &b->claim.identity)
		   && a->claim.database_incarnation == b->claim.database_incarnation
		   && a->claim.max_config_generation == b->claim.max_config_generation
		   && memcmp(a->claim.claim_sha256, b->claim.claim_sha256, 32) == 0
		   && a->timeline == b->timeline;
}

static bool
inventory_cut_equal(const RfContributorStreamCutV1 *a, const RfContributorStreamCutV1 *b)
{
	return a->failed_thread == b->failed_thread && a->flags == b->flags
		   && a->timeline_id == b->timeline_id && a->scan_begin_inclusive == b->scan_begin_inclusive
		   && a->scan_end_exclusive == b->scan_end_exclusive
		   && a->contributor_count == b->contributor_count
		   && a->component_count == b->component_count
		   && a->origin_owner_incarnation == b->origin_owner_incarnation;
}

static bool
inventory_tail_equal(const ClusterWalTailObservation *a, const ClusterWalTailObservation *b)
{
	return a->complete_end == b->complete_end && a->last_record_start == b->last_record_start
		   && a->last_record_crc == b->last_record_crc && a->records == b->records
		   && a->database_incarnation == b->database_incarnation;
}

static RfPageProofDetailV1
inventory_refuse(ClusterRecoveryNativeInventory *work, ClusterRecoveryNativeInventoryResult result,
				 RfPageProofDetailV1 detail)
{
	work->status = result;
	inventory_revoke_dependents(work);
	return detail;
}

static RfPageProofDetailV1
inventory_source(const ClusterWalSourceRef *source, const RfContributorStreamCutV1 *cut,
				 const ClusterWalTailObservation *completed, void *arg)
{
	static const uint8 domain[] = "PGRAC-E1-WAL-SOURCE-V1";
	ClusterRecoveryNativeInventory *work = arg;
	NativeInventorySource *saved;
	uint8 transcript[32];

	if (work->source_cursor >= work->source_count || source == NULL || cut == NULL)
		return inventory_refuse(work, CLUSTER_RECOVERY_NATIVE_INVENTORY_CHANGED,
								RF_PAGE_PROOF_DETAIL_SOURCE_GAP);
	saved = &work->sources[work->source_cursor];
	if (completed == NULL) {
		if (work->active)
			return inventory_refuse(work, CLUSTER_RECOVERY_NATIVE_INVENTORY_CHANGED,
									RF_PAGE_PROOF_DETAIL_SOURCE_GAP);
		if (work->pass == 0) {
			/* Empty terminals have no record callback. Bind them here too. */
			if (work->roots != NULL
				&& !cluster_recovery_root_set_targets_match(
					work->roots, source->claim.identity.system_identifier,
					source->claim.database_incarnation, source->claim.identity.storage_uuid))
				return inventory_refuse(work, CLUSTER_RECOVERY_NATIVE_INVENTORY_CHANGED,
										RF_PAGE_PROOF_DETAIL_IDENTITY_MISMATCH);
			saved->source = *source;
			saved->cut = *cut;
		} else if (!inventory_source_equal(&saved->source, source)
				   || !inventory_cut_equal(&saved->cut, cut))
			return inventory_refuse(work, CLUSTER_RECOVERY_NATIVE_INVENTORY_CHANGED,
									RF_PAGE_PROOF_DETAIL_IDENTITY_MISMATCH);
		if (pg_cryptohash_init(work->hash) != 0
			|| pg_cryptohash_update(work->hash, domain, sizeof(domain)) != 0)
			return inventory_refuse(work, CLUSTER_RECOVERY_NATIVE_INVENTORY_INVALID,
									RF_PAGE_PROOF_DETAIL_SOURCE_GAP);
		work->active = true;
		work->source_records = 0;
	} else {
		if (!work->active || completed->records != work->source_records
			|| !inventory_source_equal(&saved->source, source)
			|| !inventory_cut_equal(&saved->cut, cut))
			return inventory_refuse(work, CLUSTER_RECOVERY_NATIVE_INVENTORY_CHANGED,
									RF_PAGE_PROOF_DETAIL_SOURCE_GAP);
		if (pg_cryptohash_final(work->hash, transcript, sizeof(transcript)) != 0)
			return inventory_refuse(work, CLUSTER_RECOVERY_NATIVE_INVENTORY_INVALID,
									RF_PAGE_PROOF_DETAIL_SOURCE_GAP);
		if (work->pass == 0) {
			saved->observed = *completed;
			memcpy(saved->transcript, transcript, sizeof(transcript));
		} else if (!inventory_tail_equal(&saved->observed, completed)
				   || memcmp(saved->transcript, transcript, sizeof(transcript)) != 0
				   || (work->cursor != NULL && work->cursor->source_index == work->source_cursor))
			return inventory_refuse(work, CLUSTER_RECOVERY_NATIVE_INVENTORY_CHANGED,
									RF_PAGE_PROOF_DETAIL_SOURCE_GAP);
		work->source_cursor++;
		work->active = false;
	}
	return RF_PAGE_PROOF_DETAIL_OK;
}

static RfPageProofDetailV1
inventory_record(XLogReaderState *reader, const ClusterWalSourceRef *source,
				 const RfContributorStreamCutV1 *cut, void *arg)
{
	ClusterRecoveryNativeInventory *work = arg;
	NativeInventoryRecord *record;
	ClusterRecoveryNativeResult native;
	uint64 ordinal;
	bool owns;

	if (!work->active || reader == NULL || reader->record == NULL)
		return inventory_refuse(work, CLUSTER_RECOVERY_NATIVE_INVENTORY_CHANGED,
								RF_PAGE_PROOF_DETAIL_SOURCE_GAP);
	if (work->source_records == UINT64_MAX || work->diag.records == UINT64_MAX)
		return inventory_refuse(work, CLUSTER_RECOVERY_NATIVE_INVENTORY_CAPACITY,
								RF_PAGE_PROOF_DETAIL_CAPACITY);
	ordinal = work->source_records++;
	if (!cluster_recovery_record_hash_update(work->hash, reader))
		return inventory_refuse(work, CLUSTER_RECOVERY_NATIVE_INVENTORY_INVALID,
								RF_PAGE_PROOF_DETAIL_SOURCE_GAP);
	if (work->pass == 0 && work->roots != NULL) {
		ClusterRecoveryWalTargetsDiag targets;

		/* Scratch native parsing is freed before retaining the native record
		 * below; both use the same remaining owned-allocation budget. */
		if (!cluster_recovery_wal_targets_collect(work->roots, reader, source, cut,
												  work->budget - work->diag.owned_bytes,
												  &targets)) {
			bool capacity = targets.native_result == CLUSTER_RECOVERY_NATIVE_CAPACITY
							|| targets.roots_result == CLUSTER_RECOVERY_ROOTS_CAPACITY
							|| targets.route_detail == RF_PAGE_PROOF_DETAIL_CAPACITY;

			work->diag.native_result = targets.native_result;
			return inventory_refuse(work,
									capacity ? CLUSTER_RECOVERY_NATIVE_INVENTORY_CAPACITY
											 : CLUSTER_RECOVERY_NATIVE_INVENTORY_UNSUPPORTED,
									capacity ? RF_PAGE_PROOF_DETAIL_CAPACITY
											 : RF_PAGE_PROOF_DETAIL_SIDE_INCOMPLETE);
		}
		if (UINT64_MAX - work->diag.page_targets < targets.page_targets
			|| UINT64_MAX - work->diag.rebuildable_targets < targets.rebuildable_targets
			|| UINT64_MAX - work->diag.structural_effects < targets.structural_effects
			|| UINT64_MAX - work->diag.undo_targets < targets.undo_targets
			|| UINT64_MAX - work->diag.unowned_target_records < targets.unowned_records)
			return inventory_refuse(work, CLUSTER_RECOVERY_NATIVE_INVENTORY_CAPACITY,
									RF_PAGE_PROOF_DETAIL_CAPACITY);
		work->diag.page_targets += targets.page_targets;
		work->diag.rebuildable_targets += targets.rebuildable_targets;
		work->diag.structural_effects += targets.structural_effects;
		work->diag.undo_targets += targets.undo_targets;
		work->diag.unowned_target_records += targets.unowned_records;
	}
	/* Keep undo records as well as native effects, with the same complete
	 * source/cut/payload re-observation. Enumeration discharges no effects. */
	owns = XLogRecGetRmid(reader) == RM_XACT_ID || XLogRecGetRmid(reader) == RM_CLUSTER_UNDO_ID
		   || XLogRecGetRmid(reader) == RM_MULTIXACT_ID || XLogRecGetRmid(reader) == RM_CLOG_ID
		   || XLogRecGetRmid(reader) == RM_COMMIT_TS_ID
		   || (XLogRecGetRmid(reader) == RM_SMGR_ID
			   && (XLogRecGetInfo(reader) & ~XLR_INFO_MASK) == XLOG_SMGR_CREATE);
	if (work->pass == 1) {
		record = work->cursor;
		if (!owns) {
			if (record != NULL && record->source_index == work->source_cursor
				&& record->ordinal <= ordinal)
				return inventory_refuse(work, CLUSTER_RECOVERY_NATIVE_INVENTORY_CHANGED,
										RF_PAGE_PROOF_DETAIL_SOURCE_GAP);
			return RF_PAGE_PROOF_DETAIL_OK;
		}
		if (record == NULL || record->source_index != work->source_cursor
			|| record->ordinal != ordinal)
			return inventory_refuse(work, CLUSTER_RECOVERY_NATIVE_INVENTORY_CHANGED,
									RF_PAGE_PROOF_DETAIL_SOURCE_GAP);
		native = cluster_recovery_native_scan_recheck(record->scan, reader, source, cut);
		work->diag.native_result = native;
		if (native != CLUSTER_RECOVERY_NATIVE_UNCHANGED)
			return inventory_refuse(work, CLUSTER_RECOVERY_NATIVE_INVENTORY_CHANGED,
									RF_PAGE_PROOF_DETAIL_IDENTITY_MISMATCH);
		work->cursor = record->next;
		return RF_PAGE_PROOF_DETAIL_OK;
	}
	work->diag.records++;
	if (!owns) {
		work->diag.other_records++;
		return RF_PAGE_PROOF_DETAIL_OK;
	}
	if (sizeof(*record) > work->budget - work->diag.owned_bytes)
		return inventory_refuse(work, CLUSTER_RECOVERY_NATIVE_INVENTORY_CAPACITY,
								RF_PAGE_PROOF_DETAIL_CAPACITY);
	record = calloc(1, sizeof(*record));
	if (record == NULL)
		return inventory_refuse(work, CLUSTER_RECOVERY_NATIVE_INVENTORY_CAPACITY,
								RF_PAGE_PROOF_DETAIL_CAPACITY);
	native = cluster_recovery_native_scan_begin(
		reader, source, cut, work->budget - work->diag.owned_bytes - sizeof(*record),
		&record->scan);
	work->diag.native_result = native;
	if (native != CLUSTER_RECOVERY_NATIVE_MORE) {
		free(record);
		return inventory_refuse(work,
								native == CLUSTER_RECOVERY_NATIVE_CAPACITY
									? CLUSTER_RECOVERY_NATIVE_INVENTORY_CAPACITY
									: CLUSTER_RECOVERY_NATIVE_INVENTORY_UNSUPPORTED,
								native == CLUSTER_RECOVERY_NATIVE_CAPACITY
									? RF_PAGE_PROOF_DETAIL_CAPACITY
									: RF_PAGE_PROOF_DETAIL_SIDE_INCOMPLETE);
	}
	work->diag.owned_bytes += sizeof(*record) + cluster_recovery_native_scan_bytes(record->scan);
	if (XLogRecGetRmid(reader) == RM_CLUSTER_UNDO_ID)
		work->diag.undo_records++;
	else
		work->diag.native_records++;
	if (XLogRecGetRmid(reader) == RM_MULTIXACT_ID)
		work->diag.multixact_records++;
	if (XLogRecGetRmid(reader) == RM_CLOG_ID)
		work->diag.clog_records++;
	if (XLogRecGetRmid(reader) == RM_COMMIT_TS_ID)
		work->diag.commit_ts_records++;
	record->source_index = work->source_cursor;
	record->ordinal = ordinal;
	if (work->tail == NULL)
		work->head = record;
	else
		work->tail->next = record;
	work->tail = record;
	return RF_PAGE_PROOF_DETAIL_OK;
}

static void
inventory_read_pass(ClusterRecoveryNativeInventory *work, ClusterWalInputsV1 *inputs)
{
	uint64 records = 0;

	work->source_cursor = 0;
	work->cursor = work->head;
	work->active = false;
	work->diag.input_result = cluster_wal_inputs_cold_census_v1(
		inputs, inventory_record, inventory_source, work, &records, &work->diag.input_detail);
	if (work->diag.input_result != CLUSTER_CONTROL_ROOT_OK_PRIMARY) {
		if (work->status == CLUSTER_RECOVERY_NATIVE_INVENTORY_OBSERVED)
			work->status = CLUSTER_RECOVERY_NATIVE_INVENTORY_INPUT;
	} else if (work->active || work->source_cursor != work->source_count
			   || records != work->diag.records || (work->pass == 1 && work->cursor != NULL))
		work->status = CLUSTER_RECOVERY_NATIVE_INVENTORY_CHANGED;
}

static ClusterRecoveryNativeInventoryResult
inventory_build(ClusterWalInputsV1 *inputs, ClusterRecoveryRootSet *roots, size_t memory_budget,
				ClusterRecoveryNativeInventory **out, ClusterRecoveryNativeInventoryDiag *diag)
{
	ClusterRecoveryNativeInventory *work;
	ClusterRecoveryNativeInventoryResult result;
	uint32 count;
	size_t prefix = MAXALIGN(sizeof(*work)), size;

	if (out != NULL)
		*out = NULL;
	if (diag != NULL)
		memset(diag, 0, sizeof(*diag));
	if (out == NULL || diag == NULL || inputs == NULL)
		return CLUSTER_RECOVERY_NATIVE_INVENTORY_INVALID;
	count = cluster_wal_inputs_count_v1(inputs);
	if (count == 0 || count > CLUSTER_WAL_INPUTS_MAX)
		return CLUSTER_RECOVERY_NATIVE_INVENTORY_INVALID;
	if (memory_budget < prefix || count > (memory_budget - prefix) / sizeof(*work->sources))
		return CLUSTER_RECOVERY_NATIVE_INVENTORY_CAPACITY;
	size = prefix + count * sizeof(*work->sources);
	work = calloc(1, size);
	if (work == NULL)
		return CLUSTER_RECOVERY_NATIVE_INVENTORY_CAPACITY;
	work->sources = (NativeInventorySource *)((char *)work + prefix);
	work->roots = roots;
	work->budget = memory_budget;
	work->source_count = work->diag.sources = count;
	work->diag.owned_bytes = size;
	work->status = CLUSTER_RECOVERY_NATIVE_INVENTORY_OBSERVED;
	PG_TRY();
	{
		work->hash = pg_cryptohash_create(PG_SHA256);
		if (work->hash == NULL)
			work->status = CLUSTER_RECOVERY_NATIVE_INVENTORY_CAPACITY;
		for (work->pass = 0;
			 work->pass < 2 && work->status == CLUSTER_RECOVERY_NATIVE_INVENTORY_OBSERVED;
			 work->pass++)
			inventory_read_pass(work, inputs);
	}
	PG_CATCH();
	{
		cluster_recovery_root_set_invalidate(roots);
		cluster_recovery_native_inventory_destroy(work);
		PG_RE_THROW();
	}
	PG_END_TRY();
	*diag = work->diag;
	result = work->status;
	if (result != CLUSTER_RECOVERY_NATIVE_INVENTORY_OBSERVED) {
		cluster_recovery_native_inventory_destroy(work);
		return result;
	}
	work->complete = true;
	work->roots = NULL;
	pg_cryptohash_free(work->hash);
	work->hash = NULL;
	work->cursor = work->head;
	*out = work;
	return CLUSTER_RECOVERY_NATIVE_INVENTORY_OBSERVED;
}

ClusterRecoveryNativeInventoryResult
cluster_recovery_native_inventory_build(ClusterWalInputsV1 *inputs, size_t memory_budget,
										ClusterRecoveryNativeInventory **out,
										ClusterRecoveryNativeInventoryDiag *diag)
{
	return inventory_build(inputs, NULL, memory_budget, out, diag);
}

ClusterRecoveryNativeInventoryResult
cluster_recovery_native_inventory_collect_roots(ClusterWalInputsV1 *inputs,
												ClusterRecoveryRootSet *roots, size_t memory_budget,
												ClusterRecoveryNativeInventory **out,
												ClusterRecoveryNativeInventoryDiag *diag)
{
	ClusterRecoveryNativeInventoryResult result;

	if (roots == NULL) {
		if (out != NULL)
			*out = NULL;
		if (diag != NULL)
			memset(diag, 0, sizeof(*diag));
		return CLUSTER_RECOVERY_NATIVE_INVENTORY_INVALID;
	}
	result = inventory_build(inputs, roots, memory_budget, out, diag);
	if (result != CLUSTER_RECOVERY_NATIVE_INVENTORY_OBSERVED)
		cluster_recovery_root_set_invalidate(roots);
	return result;
}

ClusterRecoveryNativeInventoryResult
cluster_recovery_native_inventory_recheck(ClusterWalInputsV1 *inputs,
										  ClusterRecoveryNativeInventory *inventory,
										  ClusterRecoveryNativeInventoryDiag *diag)
{
	NativeInventoryRecord *saved_cursor;

	if (diag != NULL)
		memset(diag, 0, sizeof(*diag));
	if (inventory == NULL)
		return CLUSTER_RECOVERY_NATIVE_INVENTORY_INVALID;
	if (inventory->status != CLUSTER_RECOVERY_NATIVE_INVENTORY_OBSERVED) {
		if (diag != NULL)
			*diag = inventory->diag;
		return inventory->status;
	}
	if (!inventory->complete || inputs == NULL || diag == NULL) {
		inventory->status = CLUSTER_RECOVERY_NATIVE_INVENTORY_INVALID;
		inventory->complete = false;
		inventory_revoke_dependents(inventory);
		return inventory->status;
	}
	inventory->complete = false;
	saved_cursor = inventory->cursor;
	inventory->pass = 1;
	PG_TRY();
	{
		if (cluster_wal_inputs_count_v1(inputs) != inventory->source_count)
			inventory->status = CLUSTER_RECOVERY_NATIVE_INVENTORY_CHANGED;
		else {
			inventory->hash = pg_cryptohash_create(PG_SHA256);
			if (inventory->hash == NULL)
				inventory->status = CLUSTER_RECOVERY_NATIVE_INVENTORY_CAPACITY;
			else
				inventory_read_pass(inventory, inputs);
		}
	}
	PG_CATCH();
	{
		inventory->status = CLUSTER_RECOVERY_NATIVE_INVENTORY_INPUT;
		inventory_revoke_dependents(inventory);
		pg_cryptohash_free(inventory->hash);
		inventory->hash = NULL;
		*diag = inventory->diag;
		PG_RE_THROW();
	}
	PG_END_TRY();
	pg_cryptohash_free(inventory->hash);
	inventory->hash = NULL;
	inventory->cursor = saved_cursor;
	inventory->complete = inventory->status == CLUSTER_RECOVERY_NATIVE_INVENTORY_OBSERVED;
	if (!inventory->complete)
		inventory_revoke_dependents(inventory);
	*diag = inventory->diag;
	return inventory->status;
}

ClusterRecoveryNativeInventoryResult
cluster_recovery_native_inventory_next(ClusterRecoveryNativeInventory *inventory,
									   ClusterRecoveryNativeEffect *out)
{
	ClusterRecoveryNativeResult native;

	if (out != NULL)
		memset(out, 0, sizeof(*out));
	if (inventory == NULL || out == NULL || !inventory->complete
		|| inventory->status != CLUSTER_RECOVERY_NATIVE_INVENTORY_OBSERVED)
		return CLUSTER_RECOVERY_NATIVE_INVENTORY_INVALID;
	if (inventory->cursor == NULL)
		return CLUSTER_RECOVERY_NATIVE_INVENTORY_END;
	native = cluster_recovery_native_scan_next(inventory->cursor->scan, out);
	if (native == CLUSTER_RECOVERY_NATIVE_END) {
		inventory->cursor = inventory->cursor->next;
		if (inventory->cursor == NULL)
			return CLUSTER_RECOVERY_NATIVE_INVENTORY_END;
		/* A fresh native scan always has a primary effect, including empty
		 * terminal/message arrays. At most two scan calls, not a list walk. */
		native = cluster_recovery_native_scan_next(inventory->cursor->scan, out);
	}
	if (native == CLUSTER_RECOVERY_NATIVE_EFFECT)
		return CLUSTER_RECOVERY_NATIVE_INVENTORY_EFFECT;
	inventory->status = CLUSTER_RECOVERY_NATIVE_INVENTORY_CHANGED;
	inventory_revoke_dependents(inventory);
	return inventory->status;
}

static ClusterRecoveryGraphResult
inventory_seed_fail(ClusterRecoveryNativeInventory *inventory, ClusterRecoveryGraphResult result)
{
	inventory->status = CLUSTER_RECOVERY_NATIVE_INVENTORY_INVALID;
	inventory->complete = false;
	inventory->diag.graph_result = result;
	inventory_revoke_dependents(inventory);
	return result;
}

static ClusterRecoveryGraphResult
inventory_seed_step(ClusterRecoveryNativeInventory *inventory)
{
	unsigned work = 0;

	if (inventory->seed_done)
		return CLUSTER_RECOVERY_GRAPH_DRAINED;
	if (!cluster_recovery_reference_graph_accepts_roots(inventory->seed_graph))
		return inventory_seed_fail(inventory, CLUSTER_RECOVERY_GRAPH_INVALID);
	while (work < INVENTORY_SEED_TICK) {
		NativeInventoryRecord *current = inventory->seed_cursor;
		ClusterRecoveryNativeEffect effect;
		const ClusterUndoDecoded *head;

		if (current == NULL) {
			inventory->seed_done = true;
			return CLUSTER_RECOVERY_GRAPH_DRAINED;
		}
		if (cluster_recovery_native_scan_primary(current->scan, &effect)
			!= CLUSTER_RECOVERY_NATIVE_EFFECT)
			return inventory_seed_fail(inventory, CLUSTER_RECOVERY_GRAPH_CHANGED);
		work++;
		if (current->source_index != inventory->seed_source_first->source_index)
			inventory->seed_source_first = current;
		head = effect.kind == CLUSTER_RECOVERY_NATIVE_UNDO ? effect.bytes : NULL;
		if (head == NULL || head->kind != CLUSTER_UNDO_KIND_TT_SET_HEAD) {
			inventory->seed_cursor = current->next;
			continue;
		}
		if (!inventory->seed_searching) {
			inventory->seed_search = inventory->seed_source_first;
			inventory->seed_binding = NULL;
			inventory->seed_searching = true;
		}
		while (inventory->seed_search != current && work < INVENTORY_SEED_TICK) {
			NativeInventoryRecord *candidate = inventory->seed_search;
			ClusterRecoveryNativeEffect prior;
			const ClusterUndoDecoded *undo;

			if (candidate == NULL || candidate->source_index != current->source_index
				|| cluster_recovery_native_scan_primary(candidate->scan, &prior)
					   != CLUSTER_RECOVERY_NATIVE_EFFECT)
				return inventory_seed_fail(inventory, CLUSTER_RECOVERY_GRAPH_CHANGED);
			if (inventory->diag.tt_binding_records_examined == UINT64_MAX)
				return inventory_seed_fail(inventory, CLUSTER_RECOVERY_GRAPH_CAPACITY);
			inventory->diag.tt_binding_records_examined++;
			work++;
			inventory->seed_search = candidate->next;
			if (prior.kind != CLUSTER_RECOVERY_NATIVE_UNDO)
				continue;
			undo = prior.bytes;
			if (undo->segment_id != head->segment_id)
				continue;
			if (undo->kind == CLUSTER_UNDO_KIND_SEGMENT_INIT
				|| undo->kind == CLUSTER_UNDO_KIND_SEGMENT_RECYCLE
				|| undo->kind == CLUSTER_UNDO_KIND_SEGMENT_REUSE)
				inventory->seed_binding = NULL;
			else if (undo->kind == CLUSTER_UNDO_KIND_TT_BIND
					 && undo->slot_offset == head->slot_offset)
				/* Do not search past the latest binding across xid/wrap reuse. */
				inventory->seed_binding = undo;
		}
		if (inventory->seed_search != current)
			return CLUSTER_RECOVERY_GRAPH_MORE;
		{
			const ClusterUndoDecoded *binding = inventory->seed_binding;
			ClusterRecoveryTxnKey key = { 0 };
			ClusterRecoveryGraphResult added;

			if (binding == NULL || binding->instance != head->instance || binding->xid != head->xid
				|| binding->wrap != head->wrap) {
				inventory->diag.tt_head_binding_missing++;
				return inventory_seed_fail(inventory, CLUSTER_RECOVERY_GRAPH_IDENTITY);
			}
			key.system_identifier = effect.record->source.claim.identity.system_identifier;
			key.database_incarnation = effect.record->source.claim.database_incarnation;
			key.origin_node_id = binding->instance - 1;
			key.segment_id = binding->segment_id;
			key.segment_generation = binding->expected_generation;
			key.slot_offset = binding->slot_offset;
			key.slot_wrap = binding->wrap;
			key.xid = binding->xid;
			if (inventory->diag.tt_roots_added == UINT64_MAX)
				return inventory_seed_fail(inventory, CLUSTER_RECOVERY_GRAPH_CAPACITY);
			added = cluster_recovery_reference_graph_add(inventory->seed_graph, &key,
														 head->first_undo_block);
			if (added != CLUSTER_RECOVERY_GRAPH_MORE)
				return inventory_seed_fail(inventory, added);
			inventory->diag.tt_roots_added++;
		}
		inventory->seed_searching = false;
		inventory->seed_binding = NULL;
		inventory->seed_cursor = current->next;
	}
	return CLUSTER_RECOVERY_GRAPH_MORE;
}

ClusterRecoveryGraphResult
cluster_recovery_native_inventory_seed_graph_step(ClusterRecoveryNativeInventory *inventory,
												  ClusterRecoveryReferenceGraph *graph,
												  ClusterRecoveryNativeInventoryDiag *diag)
{
	ClusterRecoveryGraphResult result;

	if (diag != NULL)
		memset(diag, 0, sizeof(*diag));
	if (inventory == NULL || graph == NULL || diag == NULL) {
		cluster_recovery_reference_graph_invalidate(graph);
		if (inventory != NULL)
			inventory_seed_fail(inventory, CLUSTER_RECOVERY_GRAPH_INVALID);
		return CLUSTER_RECOVERY_GRAPH_INVALID;
	}
	if (inventory->seed_graph != NULL && inventory->seed_graph != graph)
		result = inventory_seed_fail(inventory, CLUSTER_RECOVERY_GRAPH_INVALID);
	else if (inventory->status != CLUSTER_RECOVERY_NATIVE_INVENTORY_OBSERVED
			 || !inventory->complete) {
		cluster_recovery_reference_graph_invalidate(graph);
		result = inventory->diag.graph_result >= CLUSTER_RECOVERY_GRAPH_INVALID
					 ? inventory->diag.graph_result
					 : CLUSTER_RECOVERY_GRAPH_CHANGED;
	} else {
		if (inventory->seed_graph == NULL) {
			inventory->seed_graph = graph;
			inventory->seed_cursor = inventory->seed_source_first = inventory->head;
		}
		result = inventory_seed_step(inventory);
	}
	inventory->diag.graph_result = result;
	*diag = inventory->diag;
	return result;
}

static ClusterRecoveryMultixactJoinResult
inventory_mx_fail(ClusterRecoveryNativeInventory *inventory,
				  ClusterRecoveryMultixactJoinResult result)
{
	inventory->mx_status = result;
	inventory->status = CLUSTER_RECOVERY_NATIVE_INVENTORY_INVALID;
	inventory->complete = false;
	inventory_revoke_dependents(inventory);
	return result;
}

static ClusterRecoveryMultixactJoinResult
inventory_mx_step(ClusterRecoveryNativeInventory *inventory, ClusterRecoveryMultixactJoinEntry *out)
{
	unsigned visited = 0;

	if (inventory->mx_done)
		return CLUSTER_RECOVERY_MX_JOIN_DRAINED;
	if (inventory->mx_join != NULL) {
		ClusterRecoveryMultixactJoinResult result
			= cluster_recovery_multixact_join_next(inventory->mx_join, out);

		if (result == CLUSTER_RECOVERY_MX_JOIN_ENTRY) {
			uint64 *count = out->is_offset ? &inventory->diag.mx_compared_offsets
										   : &inventory->diag.mx_compared_members;
			if (*count == UINT64_MAX
				|| (!out->equal && inventory->diag.mx_physical_mismatches == UINT64_MAX)) {
				memset(out, 0, sizeof(*out));
				return inventory_mx_fail(inventory, CLUSTER_RECOVERY_MX_JOIN_CAPACITY);
			}
			(*count)++;
			if (!out->equal)
				inventory->diag.mx_physical_mismatches++;
		} else if (result == CLUSTER_RECOVERY_MX_JOIN_DRAINED) {
			cluster_recovery_multixact_join_destroy(inventory->mx_join);
			inventory->mx_join = NULL;
			inventory->mx_cursor = inventory->mx_cursor->next;
			return CLUSTER_RECOVERY_MX_JOIN_MORE;
		} else if (result > CLUSTER_RECOVERY_MX_JOIN_DRAINED)
			return inventory_mx_fail(inventory, result);
		return result;
	}
	while (inventory->mx_cursor != NULL && visited++ < INVENTORY_SEED_TICK) {
		ClusterRecoveryNativeEffect primary;
		ClusterRecoveryMultixactJoinResult result;

		if (cluster_recovery_native_scan_primary(inventory->mx_cursor->scan, &primary)
			!= CLUSTER_RECOVERY_NATIVE_EFFECT)
			return inventory_mx_fail(inventory, CLUSTER_RECOVERY_MX_JOIN_CHANGED);
		if (primary.kind != CLUSTER_RECOVERY_NATIVE_MULTIXACT_CREATE) {
			inventory->mx_cursor = inventory->mx_cursor->next;
			continue;
		}
		result
			= cluster_recovery_multixact_join_begin(inventory->mx_roots, inventory->mx_cursor->scan,
													inventory->mx_budget, &inventory->mx_join);
		if (result != CLUSTER_RECOVERY_MX_JOIN_MORE)
			return inventory_mx_fail(inventory, result);
		return result;
	}
	if (inventory->mx_cursor == NULL) {
		inventory->mx_done = true;
		return CLUSTER_RECOVERY_MX_JOIN_DRAINED;
	}
	return CLUSTER_RECOVERY_MX_JOIN_MORE;
}

ClusterRecoveryMultixactJoinResult
cluster_recovery_native_inventory_multixact_step(ClusterRecoveryNativeInventory *inventory,
												 ClusterRecoveryRootSet *roots,
												 size_t join_memory_budget,
												 ClusterRecoveryMultixactJoinEntry *out,
												 ClusterRecoveryNativeInventoryDiag *diag)
{
	ClusterRecoveryMultixactJoinResult result;
	uint8 digest[32];

	if (out != NULL)
		memset(out, 0, sizeof(*out));
	if (diag != NULL)
		memset(diag, 0, sizeof(*diag));
	if (inventory == NULL || roots == NULL || out == NULL || diag == NULL) {
		cluster_recovery_root_set_invalidate(roots);
		if (inventory != NULL)
			inventory_mx_fail(inventory, CLUSTER_RECOVERY_MX_JOIN_INVALID);
		return CLUSTER_RECOVERY_MX_JOIN_INVALID;
	}
	if (!inventory->complete || inventory->status != CLUSTER_RECOVERY_NATIVE_INVENTORY_OBSERVED) {
		cluster_recovery_root_set_invalidate(roots);
		result
			= inventory_mx_fail(inventory, inventory->mx_status > CLUSTER_RECOVERY_MX_JOIN_DRAINED
											   ? inventory->mx_status
											   : CLUSTER_RECOVERY_MX_JOIN_CHANGED);
	} else if (!cluster_recovery_root_set_digest(roots, digest)) {
		/* This can precede binding mx_roots (e.g. during a ROOT reread). */
		cluster_recovery_root_set_invalidate(roots);
		result = inventory_mx_fail(inventory, CLUSTER_RECOVERY_MX_JOIN_CHANGED);
	} else if (inventory->mx_roots != NULL
			   && (inventory->mx_roots != roots || inventory->mx_budget != join_memory_budget
				   || memcmp(inventory->mx_digest, digest, sizeof(digest)) != 0)) {
		cluster_recovery_root_set_invalidate(roots);
		result = inventory_mx_fail(inventory, CLUSTER_RECOVERY_MX_JOIN_CHANGED);
	} else {
		if (inventory->mx_roots == NULL) {
			const ClusterWalSourceRef *source = &inventory->sources[0].source;

			inventory->mx_roots = roots;
			inventory->mx_budget = join_memory_budget;
			inventory->mx_cursor = inventory->head;
			memcpy(inventory->mx_digest, digest, sizeof(digest));
			if (!cluster_recovery_root_set_storage_matches(
					roots, source->claim.identity.system_identifier,
					source->claim.database_incarnation, source->claim.identity.storage_uuid)) {
				result = inventory_mx_fail(inventory, CLUSTER_RECOVERY_MX_JOIN_CHANGED);
				*diag = inventory->diag;
				return result;
			}
		}
		PG_TRY();
		{
			result = inventory_mx_step(inventory, out);
		}
		PG_CATCH();
		{
			inventory_mx_fail(inventory, CLUSTER_RECOVERY_MX_JOIN_INVALID);
			memset(out, 0, sizeof(*out));
			*diag = inventory->diag;
			PG_RE_THROW();
		}
		PG_END_TRY();
	}
	*diag = inventory->diag;
	return result;
}

void
cluster_recovery_native_inventory_destroy(ClusterRecoveryNativeInventory *inventory)
{
	NativeInventoryRecord *record;

	if (inventory == NULL)
		return;
	inventory_revoke_dependents(inventory);
	cluster_recovery_multixact_join_destroy(inventory->mx_join);
	pg_cryptohash_free(inventory->hash);
	record = inventory->head;
	while (record != NULL) {
		NativeInventoryRecord *next = record->next;
		cluster_recovery_native_scan_destroy(record->scan);
		free(record);
		record = next;
	}
	free(inventory);
}
#endif
