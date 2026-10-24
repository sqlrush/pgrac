/*-------------------------------------------------------------------------
 * cluster_recovery_page_history.c
 *   Read-only retained page-history enumeration for cold recovery.
 *
 * Portions Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 *-------------------------------------------------------------------------
 */
#include "postgres.h"
#include "catalog/pg_tablespace_d.h"
#include "cluster/cluster_cr_history.h"
#include "cluster/cluster_recovery_heap_scan.h"
#include "cluster/cluster_recovery_page_history.h"

#ifdef USE_PGRAC_CLUSTER

typedef struct RecoveryHistoryVisit {
	UBA address;
	uint32 record_segment_generation;
} RecoveryHistoryVisit;

struct ClusterRecoveryPageHistory {
	ClusterSpaceIdentity identity;
	BufferTag tag;
	PGAlignedBlock page;
	PGAlignedBlock candidate;
	ClusterTxLocator pending;
	RecoveryHistoryVisit *visits;
	size_t capacity;
	size_t count;
	bool emit_page;
	bool waiting;
	ClusterRecoveryHistoryResult status;
};

static ClusterRecoveryHistoryResult
recovery_history_fail(ClusterRecoveryPageHistory *history, ClusterRecoveryHistoryResult result)
{
	if (history != NULL)
		history->status = result;
	return result;
}

static ClusterRecoveryHistoryResult
recovery_history_page_valid(const void *page)
{
	ClusterRecoveryHeapScan *scan = NULL;
	ClusterRecoveryHeapResult result = cluster_recovery_heap_scan_begin(page, BLCKSZ, &scan);

	cluster_recovery_heap_scan_destroy(scan);
	if (result == CLUSTER_RECOVERY_HEAP_MORE)
		return CLUSTER_RECOVERY_HISTORY_MORE;
	return result == CLUSTER_RECOVERY_HEAP_CAPACITY ? CLUSTER_RECOVERY_HISTORY_CAPACITY
													: CLUSTER_RECOVERY_HISTORY_CORRUPT;
}

static bool
recovery_history_identity_valid(const ClusterSpaceIdentity *identity)
{
	uint8 storage = 0, incarnation = 0;

	if (identity == NULL || identity->key.system_identifier == 0
		|| identity->key.database_incarnation == 0 || identity->key.locator.spcOid == InvalidOid
		|| identity->key.locator.relNumber == 0
		|| (identity->key.locator.spcOid == GLOBALTABLESPACE_OID
				? identity->key.locator.dbOid != InvalidOid
				: identity->key.locator.dbOid == InvalidOid)
		|| identity->sequence == 0 || identity->operation == 0
		|| (identity->state != CLUSTER_SPACE_IDENTITY_LIVE
			&& identity->state != CLUSTER_SPACE_IDENTITY_TOMBSTONED))
		return false;
	for (unsigned i = 0; i < sizeof(identity->incarnation); i++) {
		storage |= identity->key.storage_uuid[i];
		incarnation |= identity->incarnation[i];
	}
	return storage != 0 && incarnation != 0;
}

ClusterRecoveryHistoryResult
cluster_recovery_page_history_begin(const ClusterSpaceIdentity *identity, ForkNumber forknum,
									BlockNumber block, const void *page, size_t length,
									size_t memory_budget, ClusterRecoveryPageHistory **out)
{
	ClusterRecoveryPageHistory *history;
	ClusterRecoveryHistoryResult result;
	size_t capacity;

	if (out != NULL)
		*out = NULL;
	if (out == NULL || !recovery_history_identity_valid(identity) || page == NULL
		|| length != BLCKSZ || forknum != MAIN_FORKNUM || block == InvalidBlockNumber)
		return CLUSTER_RECOVERY_HISTORY_INVALID;
	/* Include the temporary full-page validator and its metadata in the
	 * budget. Caller-owned input and record bytes are not walker storage. */
	if (memory_budget <= sizeof(*history) + 2 * BLCKSZ)
		return CLUSTER_RECOVERY_HISTORY_CAPACITY;
	capacity = (memory_budget - sizeof(*history) - 2 * BLCKSZ) / sizeof(RecoveryHistoryVisit);
	if (capacity == 0)
		return CLUSTER_RECOVERY_HISTORY_CAPACITY;
	result = recovery_history_page_valid(page);
	if (result != CLUSTER_RECOVERY_HISTORY_MORE)
		return result;
	history = calloc(1, sizeof(*history));
	if (history == NULL)
		return CLUSTER_RECOVERY_HISTORY_CAPACITY;
	history->visits = calloc(capacity, sizeof(*history->visits));
	if (history->visits == NULL) {
		free(history);
		return CLUSTER_RECOVERY_HISTORY_CAPACITY;
	}
	history->identity = *identity;
	InitBufferTag(&history->tag, &identity->key.locator, forknum, block);
	history->capacity = capacity;
	history->emit_page = true;
	memcpy(history->page.data, page, BLCKSZ);
	*out = history;
	return CLUSTER_RECOVERY_HISTORY_MORE;
}

ClusterRecoveryHistoryResult
cluster_recovery_page_history_next(ClusterRecoveryPageHistory *history,
								   ClusterRecoveryHistoryEvent *out)
{
	const ClusterItlSlotData *slots;
	int newest = -1;
	uint32 segment, block;
	uint16 slot_offset, row;

	if (out != NULL)
		memset(out, 0, sizeof(*out));
	if (history == NULL || out == NULL)
		return recovery_history_fail(history, CLUSTER_RECOVERY_HISTORY_INVALID);
	if (history->status != CLUSTER_RECOVERY_HISTORY_MORE)
		return history->status;
	if (history->emit_page) {
		history->emit_page = false;
		out->page = history->page.data;
		return CLUSTER_RECOVERY_HISTORY_PAGE;
	}
	if (history->waiting) {
		out->locator = history->pending;
		return CLUSTER_RECOVERY_HISTORY_NEED_RECORD;
	}
	slots = ClusterPageGetItlSlots(history->page.data);
	for (unsigned i = 0; i < CLUSTER_ITL_INITRANS_DEFAULT; i++) {
		if (slots[i].flags == ITL_FLAG_FREE) {
			if (!cluster_undo_history_prior_valid(&slots[i]))
				return recovery_history_fail(history, CLUSTER_RECOVERY_HISTORY_CORRUPT);
			continue;
		}
		if (!TransactionIdIsNormal(slots[i].xid) || !SCN_VALID(slots[i].write_scn)
			|| !uba_decode_record(slots[i].undo_segment_head, &segment, &block, &slot_offset, &row))
			return recovery_history_fail(history, CLUSTER_RECOVERY_HISTORY_BASE_REQUIRED);
		if (newest < 0 || scn_time_cmp(slots[i].write_scn, slots[newest].write_scn) > 0)
			newest = (int)i;
	}
	if (newest < 0) {
		/* A persisted history-retirement watermark still needs its original
		 * horizon/base proof. Zero remaining ITLs does not supply that proof. */
		if (SCN_VALID(ClusterPageGetItlHeader(history->page.data)->itl_recycle_watermark_scn))
			return recovery_history_fail(history, CLUSTER_RECOVERY_HISTORY_BASE_REQUIRED);
		history->status = CLUSTER_RECOVERY_HISTORY_DRAINED;
		return history->status;
	}
	memset(&history->pending, 0, sizeof(history->pending));
	history->pending.uba = slots[newest].undo_segment_head;
	history->pending.xid = slots[newest].xid;
	history->pending.itl_kind = slots[newest].flags;
	history->pending.itl_slot_index = (uint8)newest;
	history->pending.tt_wrap = TT_WRAP_INVALID;
	history->waiting = true;
	out->locator = history->pending;
	return CLUSTER_RECOVERY_HISTORY_NEED_RECORD;
}

ClusterRecoveryHistoryResult
cluster_recovery_page_history_accept(ClusterRecoveryPageHistory *history,
									 const ClusterRecoveryTxnKey *transaction,
									 uint32 record_segment_generation,
									 const ClusterRecoveryUndoRecord *record)
{
	ClusterRecoveryGraphResult checked;
	ClusterRecoveryHistoryResult result;
	ClusterTxLocator canonical;
	ClusterTxResolveReason reason;

	if (history == NULL)
		return CLUSTER_RECOVERY_HISTORY_INVALID;
	if (history->status != CLUSTER_RECOVERY_HISTORY_MORE)
		return history->status;
	if (!history->waiting || transaction == NULL || record == NULL)
		return recovery_history_fail(history, CLUSTER_RECOVERY_HISTORY_INVALID);
	if (transaction->system_identifier != history->identity.key.system_identifier
		|| transaction->database_incarnation != history->identity.key.database_incarnation
		|| transaction->xid != history->pending.xid
		|| record->header.target_locator.spcOid != history->tag.spcOid
		|| record->header.target_locator.dbOid != history->tag.dbOid
		|| record->header.target_locator.relNumber != history->tag.relNumber
		|| record->header.target_fork != history->tag.forkNum)
		return recovery_history_fail(history, CLUSTER_RECOVERY_HISTORY_IDENTITY);
	checked = cluster_recovery_tx_record_check(transaction, history->pending.uba, record,
											   record_segment_generation);
	if (checked != CLUSTER_RECOVERY_GRAPH_MORE)
		return recovery_history_fail(history, checked == CLUSTER_RECOVERY_GRAPH_IDENTITY
												  ? CLUSTER_RECOVERY_HISTORY_IDENTITY
												  : CLUSTER_RECOVERY_HISTORY_CORRUPT);
	canonical = history->pending;
	canonical.tt_wrap = transaction->slot_wrap;
	if (!cluster_undo_record_validate_identity(&canonical, canonical.uba, &record->header, &reason))
		return recovery_history_fail(history, CLUSTER_RECOVERY_HISTORY_IDENTITY);
	if ((record->header.flags & UNDO_REC_FLAG_HAS_ITL_HISTORY) == 0)
		return recovery_history_fail(history, CLUSTER_RECOVERY_HISTORY_BASE_REQUIRED);
	for (size_t i = 0; i < history->count; i++)
		if (cluster_undo_record_uba_equal(history->visits[i].address, record->address))
			return recovery_history_fail(history, history->visits[i].record_segment_generation
														  == record_segment_generation
													  ? CLUSTER_RECOVERY_HISTORY_CYCLE
													  : CLUSTER_RECOVERY_HISTORY_IDENTITY);
	if (history->count == history->capacity)
		return recovery_history_fail(history, CLUSTER_RECOVERY_HISTORY_CAPACITY);
	memcpy(history->candidate.data, history->page.data, BLCKSZ);
	if (!cluster_cr_history_inverse(history->candidate.data, &history->tag, &canonical,
									canonical.uba, &record->header, (const char *)record->payload,
									record->header.payload_length))
		return recovery_history_fail(history, CLUSTER_RECOVERY_HISTORY_CORRUPT);
	result = recovery_history_page_valid(history->candidate.data);
	if (result != CLUSTER_RECOVERY_HISTORY_MORE)
		return recovery_history_fail(history, result);
	history->visits[history->count].address = record->address;
	history->visits[history->count++].record_segment_generation = record_segment_generation;
	memcpy(history->page.data, history->candidate.data, BLCKSZ);
	history->waiting = false;
	history->emit_page = true;
	return CLUSTER_RECOVERY_HISTORY_MORE;
}

void
cluster_recovery_page_history_destroy(ClusterRecoveryPageHistory *history)
{
	if (history != NULL) {
		free(history->visits);
		free(history);
	}
}
#endif
