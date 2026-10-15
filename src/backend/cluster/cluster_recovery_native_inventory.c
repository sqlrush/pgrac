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
};

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
			|| UINT64_MAX - work->diag.unowned_target_records < targets.unowned_records)
			return inventory_refuse(work, CLUSTER_RECOVERY_NATIVE_INVENTORY_CAPACITY,
									RF_PAGE_PROOF_DETAIL_CAPACITY);
		work->diag.page_targets += targets.page_targets;
		work->diag.rebuildable_targets += targets.rebuildable_targets;
		work->diag.structural_effects += targets.structural_effects;
		work->diag.unowned_target_records += targets.unowned_records;
	}
	/* This inventory owns XACT, plus native CREATE. Other records are
	 * explicitly counted and still require their original typed owners. */
	owns = XLogRecGetRmid(reader) == RM_XACT_ID
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
	work->diag.native_records++;
	record->source_index = work->source_cursor;
	record->ordinal = ordinal;
	if (work->tail == NULL)
		work->head = record;
	else
		work->tail->next = record;
	work->tail = record;
	return RF_PAGE_PROOF_DETAIL_OK;
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
			 work->pass++) {
			uint64 records = 0;

			work->source_cursor = 0;
			work->cursor = work->head;
			work->diag.input_result
				= cluster_wal_inputs_cold_census_v1(inputs, inventory_record, inventory_source,
													work, &records, &work->diag.input_detail);
			if (work->diag.input_result != CLUSTER_CONTROL_ROOT_OK_PRIMARY) {
				if (work->status == CLUSTER_RECOVERY_NATIVE_INVENTORY_OBSERVED)
					work->status = CLUSTER_RECOVERY_NATIVE_INVENTORY_INPUT;
				break;
			}
			if (work->active || work->source_cursor != count || records != work->diag.records
				|| (work->pass == 1 && work->cursor != NULL)) {
				work->status = CLUSTER_RECOVERY_NATIVE_INVENTORY_CHANGED;
				break;
			}
		}
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
	return inventory->status;
}

void
cluster_recovery_native_inventory_destroy(ClusterRecoveryNativeInventory *inventory)
{
	NativeInventoryRecord *record;

	if (inventory == NULL)
		return;
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
