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
#include "catalog/pg_tablespace_d.h"
#include "utils/relmapper.h"
#include "replication/origin.h"
#include "cluster/cluster_cf_enqueue.h"
#include "cluster/cluster_cold_recovery.h"
#include "cluster/cluster_recovery_native_inventory.h"
#include "cluster/cluster_native_startup.h"
#include "cluster/cluster_recovery_record_hash.h"
#include "cluster/cluster_recovery_wal_targets.h"
#include "cluster/cluster_side_undo.h"
#include "cluster/storage/cluster_undo_xlog.h"
#include "miscadmin.h"
#include "postmaster/interrupt.h"
#include "replication/origin.h"
#include "storage/lwlock.h"

#define INVENTORY_SEED_TICK 128

typedef ClusterRecoveryRelmapHistorySource NativeInventoryHistory;

typedef struct NativeInventorySource {
	ClusterWalSourceRef source;
	RfContributorStreamCutV1 cut;
	ClusterWalTailObservation observed;
	uint8 transcript[32];
	ClusterRecoveryRelmapHistorySource history;
	uint64 native_redo_records; /* original all-family suffix count, not an XID proof */
} NativeInventorySource;

typedef struct NativeInventoryRecord {
	struct NativeInventoryRecord *next;
	ClusterRecoveryNativeScan *scan;
	ClusterRecoveryTerminalEntry terminal; /* normalized once by the full terminal cursor */
	uint32 source_index;
	uint64 ordinal;
} NativeInventoryRecord;

typedef struct NativeInventoryPage {
	struct NativeInventoryPage *next;
	XLogReaderState reader;
	size_t bytes;
	uint32 source_index;
	uint64 ordinal;
	int max_block_id;
	uint8 digest[32];
	char error[1024]; /* original RestoreBlockImage diagnostic workspace */
	/* Normalized decoded header, blocks and owned payloads follow. */
} NativeInventoryPage;

/* Stack-local correspondence work during one explicit full physical reread.
 * Never retained as an authority or across a returned/refused call. */
typedef struct NativeInventoryRelmapTerminal {
	ClusterRecoveryRelmapSourceV1 source;
	uint32 source_index, expected, matched;
} NativeInventoryRelmapTerminal;

typedef struct NativeInventoryRelmapXid {
	ClusterColdParticipantV1 source;
	ClusterColdXidProgressV1 progress;
	uint32 source_index;
	uint64 before_map, after_map, map_ordinal;
	XLogRecPtr map_start, map_end;
	ClusterRecoveryRelmapPendingXidV1 *result; /* stack-only unpublished output */
	ClusterRecoveryRelmapPendingAllNativeV1 *all_native; /* optional private whole-set output */
	ClusterColdXidProgressV1 generations[CLUSTER_WAL_INPUTS_MAX]; /* other original sources */
	bool side_clog; /* opt-in exact carriers during this same private reread */
	uint32 clog_expected[CLUSTER_WAL_INPUTS_MAX], clog_matched[CLUSTER_WAL_INPUTS_MAX];
} NativeInventoryRelmapXid;

struct ClusterRecoveryNativeInventory {
	const ClusterRecoveryRelmapSourceV1 *relmap_owner; /* borrowed opaque history custody */
	NativeInventorySource *sources;
	NativeInventoryRecord *head;
	NativeInventoryRecord *tail;
	NativeInventoryRecord *cursor;
	NativeInventoryPage *pages, *pages_tail, *page_cursor;
	RfPageOnlinePlanV1 *page_build; /* private, only during an explicit physical recheck */
	NativeInventoryRelmapTerminal *relmap_terminal; /* private synchronous recheck only */
	NativeInventoryRelmapXid *relmap_xid; /* private synchronous recheck only */
	uint32 page_count;
	ClusterRecoveryRootSet *roots; /* borrowed; no further access after collection */
	pg_cryptohash_ctx *hash;
	ClusterRecoveryNativeInventoryDiag diag;
	ClusterRecoveryNativeInventoryResult status;
	size_t budget;
	uint32 source_count;
	uint32 source_cursor;
	uint64 source_records;
	uint64 source_native_redo_records;
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
	ClusterRecoveryRootSet *xact_roots; /* borrowed; outlives this inventory */
	ClusterRecoveryXactJoin *xact_join;
	NativeInventoryRecord *xact_cursor;
	size_t xact_budget;
	uint8 xact_digest[32];
	ClusterRecoveryXactJoinResult xact_status;
	bool xact_done;
	NativeInventoryRecord *terminal_cursor, *terminal_first, *terminal_search;
	const ClusterUndoDecoded *terminal_binding;
	const ClusterRecoveryNativeRecord *terminal_binding_record;
	ClusterRecoveryTerminalEntry terminal_pending;
	bool terminal_started, terminal_searching;
	ClusterRecoveryTerminalResult terminal_status;
	NativeInventoryRecord *terminal_lookup_cursor;
	ClusterRecoveryTerminalSummary terminal_lookup;
	bool terminal_lookup_active;
	NativeInventoryRecord *relmap_cursor;
	ClusterRelmapV2Image relmap_query;
	uint8 relmap_wire[CLUSTER_RELMAP_V2_FILE_SIZE];
	ClusterRecoveryRelmapFoldEntry relmap_pending;
	ClusterRecoveryRelmapFoldSummary relmap_summary;
	uint32 relmap_child_cursor;
	bool relmap_active;
	bool relmap_drained;
	uint32 relmap_wal_source;
	struct {
		uint64 before, after, overlap;
	} relmap_clog[CLUSTER_WAL_INPUTS_MAX];
};

/* Retain metadata from the SAME held input set, before and after each
 * complete physical pass. Never refresh a selected checkpoint or window. */
static bool
inventory_history_inputs(ClusterRecoveryNativeInventory *work, ClusterWalInputsV1 *inputs,
						 bool capture)
{
	if ((work->relmap_owner != NULL
		 && !cluster_wal_inputs_relmap_source_current_v1(work->relmap_owner))
		|| cluster_wal_inputs_count_v1(inputs) != work->source_count)
		return false;
	for (uint32 i = 0; i < work->source_count; ++i) {
		uint32 index = capture ? i : work->sources[i].history.input_index;
		const ClusterWalInputV1 *input = cluster_wal_inputs_at_v1(inputs, index);
		ClusterWalInputV1 *saved = &work->sources[i].history.input;
		if (input == NULL)
			return false;
		/* These original scope-owned carriers are zero-initialized values;
		 * retain all bytes, including complete native checkpoint/window and
		 * the alternative's original terminal metadata. */
		if (capture) {
			memcpy(saved, input, sizeof(*saved));
			work->sources[i].history.input_index = i;
		} else if (memcmp(saved, input, sizeof(*saved)) != 0)
			return false;
	}
	return work->relmap_owner == NULL
		|| cluster_wal_inputs_relmap_source_current_v1(work->relmap_owner);
}

static bool
inventory_history_checkpoint(ClusterRecoveryNativeInventory *work, XLogReaderState *reader)
{
	ClusterRecoveryRelmapHistorySource *history = &work->sources[work->source_cursor].history;
	const ClusterWalInputV1 *input = &history->input;
	ClusterRelmapCheckpointWindow window = { 0 };
	uint64 issued = 0;
	bool bound;

	if (input->kind != CLUSTER_WAL_INPUT_CHECKPOINT
		|| reader->ReadRecPtr != input->checkpoint_start)
		return true;
	bound = XLogRecGetRmid(reader) == RM_XLOG_ID
			&& (XLogRecGetInfo(reader) == XLOG_CHECKPOINT_ONLINE
				|| XLogRecGetInfo(reader) == XLOG_CHECKPOINT_SHUTDOWN)
			&& XLogRecGetXid(reader) == InvalidTransactionId && XLogRecMaxBlockId(reader) < 0
			&& !reader->record->has_page_version_edge
			&& XLogRecGetOrigin(reader) == InvalidRepOriginId
			&& !TransactionIdIsValid(reader->record->toplevel_xid)
			&& XLogRecGetDataLen(reader) == sizeof(input->native_checkpoint)
			&& XLogRecGetData(reader) != NULL
			&& memcmp(XLogRecGetData(reader), &input->native_checkpoint,
					  sizeof(input->native_checkpoint))
				   == 0;
	if (work->pass == 0) {
		history->checkpoint_observed = bound;
		if (bound) {
			history->checkpoint_end = reader->EndRecPtr;
			history->window_observed = cluster_relmap_checkpoint_window_decode_v2(
				input->native_window, &input->native_checkpoint, reader->ReadRecPtr,
				reader->EndRecPtr, &input->source.claim.identity,
				input->source.claim.database_incarnation, &history->window,
				&history->post_flush_issued);
		}
		return true;
	}
	if (history->checkpoint_observed != bound
		|| (bound && history->checkpoint_end != reader->EndRecPtr))
		return false;
	if (!bound)
		return true;
	return history->window_observed
			   == cluster_relmap_checkpoint_window_decode_v2(
				   input->native_window, &input->native_checkpoint, reader->ReadRecPtr,
				   reader->EndRecPtr, &input->source.claim.identity,
				   input->source.claim.database_incarnation, &window, &issued)
		   && memcmp(&window, &history->window, sizeof(window)) == 0
		   && issued == history->post_flush_issued;
}

static void
inventory_revoke_dependents(ClusterRecoveryNativeInventory *work)
{
	if (work != NULL && work->mx_roots != NULL)
		cluster_recovery_root_set_invalidate(work->mx_roots);
	if (work != NULL && work->xact_roots != NULL)
		cluster_recovery_root_set_invalidate(work->xact_roots);
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

static bool
inventory_page_hash(const XLogReaderState *reader, uint8 out[32])
{
	pg_cryptohash_ctx *volatile hash = NULL;
	volatile bool ok = false;

	PG_TRY();
	{
		hash = pg_cryptohash_create(PG_SHA256);
		ok = hash != NULL && pg_cryptohash_init(hash) == 0
			&& cluster_recovery_record_hash_update(hash, reader)
			&& pg_cryptohash_final(hash, out, 32) == 0;
	}
	PG_FINALLY();
	{
		pg_cryptohash_free(hash);
	}
	PG_END_TRY();
	return ok;
}

/* Verify every pointer against our one allocation before the record hasher
 * follows it. Rebound/oversized exported headers must not become reads. */
static bool
inventory_page_payload(char **at, const char *end, const char *value, size_t length)
{
	size_t aligned = MAXALIGN(length);

	if (aligned > (size_t)(end - *at) || value != (length == 0 ? NULL : *at))
		return false;
	*at += aligned;
	return true;
}

static bool
inventory_page_current(const ClusterRecoveryNativeInventory *work, const NativeInventoryPage *page)
{
	const DecodedXLogRecord *record = (const DecodedXLogRecord *)
		((const char *)page + MAXALIGN(sizeof(*page)));
	size_t fixed = MAXALIGN(sizeof(*page)) + MAXALIGN(sizeof(*record)
		+ (size_t)(page->max_block_id + 1) * sizeof(DecodedBkpBlock));
	const char *end = (const char *)page + page->bytes;
	char *at = (char *)page + fixed;
	uint8 digest[32];

	if (page->bytes < fixed || page->source_index >= work->source_count
		|| page->reader.record != record || record->max_block_id != page->max_block_id
		|| record->next != NULL || record->oversized
		|| record->size != page->bytes - MAXALIGN(sizeof(*page))
		|| page->reader.system_identifier
			!= work->sources[page->source_index].source.claim.identity.system_identifier
		|| page->reader.ReadRecPtr != record->lsn || page->reader.EndRecPtr != record->next_lsn
		|| !record->has_page_version_edge
		|| !inventory_page_payload(&at, end, record->main_data, record->main_data_len))
		return false;
	for (int i = 0; i <= page->max_block_id; i++) {
		const DecodedBkpBlock *block = &record->blocks[i];

		if (!block->in_use)
			continue;
		if ((block->has_data && !inventory_page_payload(&at, end, block->data, block->data_len))
			|| (block->has_image && !inventory_page_payload(&at, end, block->bkp_image, block->bimg_len)))
			return false;
	}
	return at == end && inventory_page_hash(&page->reader, digest)
		&& memcmp(digest, page->digest, sizeof(digest)) == 0;
}

static bool
inventory_pages_current(const ClusterRecoveryNativeInventory *work)
{
	const NativeInventoryPage *page = work->pages;
	uint32 count = 0;

	while (page != NULL && count < work->page_count) {
		if (!inventory_page_current(work, page))
			return false;
		count++;
		page = page->next;
	}
	return page == NULL && count == work->page_count;
}

static void
inventory_page_copy(char **at, char **out, const char *source, size_t length)
{
	*out = length == 0 ? NULL : *at;
	if (length != 0)
		memcpy(*at, source, length);
	*at += MAXALIGN(length);
}

static RfPageProofDetailV1
inventory_page_save(ClusterRecoveryNativeInventory *work, XLogReaderState *reader, uint64 ordinal)
{
	const DecodedXLogRecord *source = reader->record;
	NativeInventoryPage *page;
	DecodedXLogRecord *record;
	size_t fixed = MAXALIGN(sizeof(*page)) + MAXALIGN(sizeof(*record)
		+ (size_t)(source->max_block_id + 1) * sizeof(DecodedBkpBlock));
	size_t bytes = fixed + MAXALIGN((size_t)source->main_data_len);
	char *at;

	for (int i = 0; i <= source->max_block_id; i++) {
		const DecodedBkpBlock *block = &source->blocks[i];
		if (block->in_use) {
			if (block->has_data) bytes += MAXALIGN((size_t)block->data_len);
			if (block->has_image) bytes += MAXALIGN((size_t)block->bimg_len);
		}
	}
	if (work->page_count == UINT32_MAX || bytes > work->budget - work->diag.owned_bytes)
		return inventory_refuse(work, CLUSTER_RECOVERY_NATIVE_INVENTORY_CAPACITY,
			RF_PAGE_PROOF_DETAIL_CAPACITY);
	page = calloc(1, bytes);
	if (page == NULL)
		return inventory_refuse(work, CLUSTER_RECOVERY_NATIVE_INVENTORY_CAPACITY,
			RF_PAGE_PROOF_DETAIL_CAPACITY);
	/* Link immediately: the original owner also frees this allocation on ERROR. */
	if (work->pages_tail == NULL) work->pages = page;
	else work->pages_tail->next = page;
	work->pages_tail = page; work->page_count++;
	work->diag.owned_bytes += bytes;
	page->bytes = bytes; page->max_block_id = source->max_block_id;
	page->source_index = work->source_cursor; page->ordinal = ordinal;
	record = (DecodedXLogRecord *)((char *)page + MAXALIGN(sizeof(*page)));
	memcpy(record, source, sizeof(*record)
		+ (size_t)(source->max_block_id + 1) * sizeof(DecodedBkpBlock));
	record->next = NULL; record->oversized = false;
	record->size = bytes - MAXALIGN(sizeof(*page));
	at = (char *)page + fixed;
	inventory_page_copy(&at, &record->main_data, source->main_data, source->main_data_len);
	for (int i = 0; i <= source->max_block_id; i++) {
		DecodedBkpBlock *block = &record->blocks[i];
		if (!block->in_use) { memset(block, 0, sizeof(*block)); continue; }
		block->prefetch_buffer = InvalidBuffer;
		block->data = block->bkp_image = NULL;
		if (block->has_data) {
			inventory_page_copy(&at, &block->data, source->blocks[i].data, block->data_len);
			block->data_bufsz = block->data_len;
		} else block->data_len = block->data_bufsz = 0;
		if (block->has_image)
			inventory_page_copy(&at, &block->bkp_image, source->blocks[i].bkp_image, block->bimg_len);
		else {
			block->apply_image = false; block->bimg_info = 0;
			block->bimg_len = block->hole_offset = block->hole_length = 0;
		}
	}
	page->reader.system_identifier = reader->system_identifier;
	page->reader.ReadRecPtr = reader->ReadRecPtr; page->reader.EndRecPtr = reader->EndRecPtr;
	page->reader.record = record; page->reader.errormsg_buf = page->error;
	if (!inventory_page_hash(reader, page->digest)
		|| !inventory_page_current(work, page))
		return inventory_refuse(work, CLUSTER_RECOVERY_NATIVE_INVENTORY_INVALID,
			RF_PAGE_PROOF_DETAIL_SOURCE_GAP);
	return RF_PAGE_PROOF_DETAIL_OK;
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
			uint32 index;
			/* The original census sorts full sources; at_v1 preserves selector
			 * order. Bind by complete source, including empty/history inputs,
			 * and retain the original selector index for every later check. */
			for (index = work->source_cursor; index < work->source_count; ++index)
				if (inventory_source_equal(&work->sources[index].history.input.source, source))
					break;
			if (index == work->source_count)
				return inventory_refuse(work, CLUSTER_RECOVERY_NATIVE_INVENTORY_CHANGED,
										RF_PAGE_PROOF_DETAIL_IDENTITY_MISMATCH);
			if (index != work->source_cursor) {
				ClusterRecoveryRelmapHistorySource swap = saved->history;
				saved->history = work->sources[index].history;
				work->sources[index].history = swap;
			}
		}
		if (!inventory_source_equal(&saved->history.input.source, source))
			return inventory_refuse(work, CLUSTER_RECOVERY_NATIVE_INVENTORY_CHANGED,
									RF_PAGE_PROOF_DETAIL_IDENTITY_MISMATCH);
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
		work->source_native_redo_records = 0;
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
			saved->native_redo_records = work->source_native_redo_records;
		} else if (!inventory_tail_equal(&saved->observed, completed)
				   || saved->native_redo_records != work->source_native_redo_records
				   || memcmp(saved->transcript, transcript, sizeof(transcript)) != 0
				   || (work->cursor != NULL && work->cursor->source_index == work->source_cursor))
			return inventory_refuse(work, CLUSTER_RECOVERY_NATIVE_INVENTORY_CHANGED,
									RF_PAGE_PROOF_DETAIL_SOURCE_GAP);
		work->source_cursor++;
		work->active = false;
	}
	return RF_PAGE_PROOF_DETAIL_OK;
}

/* Preserve the complete physical origin's lifecycle stream, not just the
 * queried raw page. A pre-redo record or generation without a checkpoint
 * has no counter candidate; keep it explicitly unobserved, never attach an era.
 * The original native recheck has compared this complete owned record first. */
static bool
inventory_relmap_clog_record(ClusterRecoveryNativeInventory *work,
	const NativeInventoryRecord *saved, uint64 before, uint64 after, bool observed)
{
	NativeInventoryRelmapXid *xid = work->relmap_xid;
	ClusterRecoveryNativeEffect effect;
	ClusterRecoveryRelmapClogEventV1 value = {0}, *event = &value;
	ClusterRecoveryRelmapPendingXidV1 *out = xid->result;
	bool query_origin = work->sources[work->source_cursor].source.claim.identity.origin_node_id
		== work->relmap_query.pending_owner.node;

	if (!query_origin && xid->all_native == NULL)
		return true;
	if ((query_origin && out == NULL) || saved == NULL
		|| cluster_recovery_native_scan_primary(saved->scan, &effect)
			!= CLUSTER_RECOVERY_NATIVE_EFFECT
		|| (effect.kind != CLUSTER_RECOVERY_NATIVE_CLOG_ZERO
			&& effect.kind != CLUSTER_RECOVERY_NATIVE_CLOG_TRUNCATE)
		|| (query_origin && out->clog_count >= CLUSTER_RECOVERY_RELMAP_CLOG_HISTORY_MAX)
		|| (xid->all_native != NULL
			&& xid->all_native->clog_count >= CLUSTER_RECOVERY_RELMAP_CLOG_HISTORY_MAX))
		return false;
	event->source_index = saved->source_index;
	event->record_ordinal = saved->ordinal;
	event->read_lsn = effect.record->read_lsn;
	event->end_lsn = effect.record->end_lsn;
	event->record_crc = (uint32)effect.record->header.xl_crc;
	event->kind = effect.kind;
	if (effect.kind == CLUSTER_RECOVERY_NATIVE_CLOG_ZERO) {
		memcpy(&event->page, effect.record->payload, sizeof(event->page));
		if ((effect.record->header.xl_info & ~XLR_INFO_MASK) == CLOG_ZEROPAGE_FULL) {
			/* The original scanner validated all native birth bytes. Keep
			 * them independently of the counter candidate and pending xid. */
			memcpy(&event->full_birth, effect.record->payload, sizeof(event->full_birth));
			event->full_birth_observed = true;
		}
	} else {
		xl_clog_truncate truncate;
		memcpy(&truncate, effect.record->payload, sizeof(truncate));
		event->page = truncate.pageno;
		event->oldest_xact = truncate.oldestXact;
		event->oldest_database = truncate.oldestXactDb;
	}
	event->counter_observed = observed;
	if (observed) {
		event->counter_before = before;
		event->counter_after = after;
	}
	if (query_origin)
		memcpy(&out->clog[out->clog_count++], event, sizeof(*event));
	if (xid->all_native != NULL)
		memcpy(&xid->all_native->clog[xid->all_native->clog_count++], event, sizeof(*event));
	return true;
}

/* The original decoder owns all payload/child/checkpoint counter semantics.
 * This strict local-form observation uses none of its execution routes and
 * accepts no caller-provided counter inputs. Free its transient scratch on
 * normal refusal and ERROR; no decoded work survives this record callback. */
static bool
inventory_relmap_xid_record(ClusterRecoveryNativeInventory *work, XLogReaderState *reader,
	uint64 ordinal, const NativeInventoryRecord *saved)
{
	NativeInventoryRelmapXid *xid = work->relmap_xid;
	volatile ClusterColdDecodedV1 decoded = {0};
	ClusterColdXidProgressV1 *progress;
	const ClusterColdParticipantV1 *participant;
	uint64 before;
	volatile bool ok = false;

	if (xid == NULL)
		return true;
	if (work->source_cursor >= CLUSTER_WAL_INPUTS_MAX)
		return false;
	if (xid->side_clog && XLogRecGetRmid(reader) == RM_CLOG_ID) {
		uint32 index = work->source_cursor;
		const NativeInventorySource *saved_source;
		ClusterRecoveryRelmapSourceV1 source;
		if (index >= work->source_count || work->relmap_owner == NULL
			|| xid->clog_matched[index] >= xid->clog_expected[index])
			return false;
		saved_source = &work->sources[index];
		source = cluster_wal_inputs_relmap_source_v1(work->relmap_owner->scope,
			saved_source->history.input_index);
		if (!inventory_source_equal(&saved_source->source, &source.selected.source)
			|| !cluster_wal_inputs_relmap_clog_matches_v1(&source, reader))
			return false;
		xid->clog_matched[index]++;
	}
	progress = work->source_cursor == xid->source_index
		? &xid->progress : &xid->generations[work->source_cursor];
	participant = work->source_cursor == xid->source_index ? &xid->source : &progress->source;
	if (!progress->begun || reader->ReadRecPtr < participant->native_redo)
		return XLogRecGetRmid(reader) != RM_CLOG_ID
			|| inventory_relmap_clog_record(work, saved, 0, 0, false);
	if (progress->records == 0 && reader->ReadRecPtr != participant->native_redo)
		return false;
	/* Preserve the original cold decoder's frame-identity rejection even
	 * for a separate participant's RELMAP counter input. Native scan equality
	 * retains these bytes but does not certify raw/top identity consistency. */
	if ((TransactionIdIsValid(reader->record->header.xl_xid)
		 && !TransactionIdIsNormal(reader->record->header.xl_xid))
		|| (TransactionIdIsValid(reader->record->toplevel_xid)
			&& (!TransactionIdIsNormal(reader->record->toplevel_xid)
				|| !TransactionIdIsNormal(reader->record->header.xl_xid)
				|| reader->record->toplevel_xid != reader->record->header.xl_xid)))
		return false;
	before = progress->next_xid;
	PG_TRY();
	{
		const ClusterWalSourceRef *source = &work->sources[work->source_cursor].source;
		if (XLogRecGetRmid(reader) == RM_RELMAP_ID) {
			ClusterRecoveryNativeEffect effect;
			/* The generic cold effect decoder deliberately has no RELMAP
			 * execution route. This participant's original native scan has
			 * just revalidated the complete record/image. Native RELMAP has
			 * only a header-xid counter input, no payload/child counter.
			 * Do not relax the independent generic decoder or replay flags. */
			ok = saved != NULL && cluster_recovery_native_scan_primary(saved->scan, &effect)
				== CLUSTER_RECOVERY_NATIVE_EFFECT
				&& effect.kind == CLUSTER_RECOVERY_NATIVE_RELMAP_UPDATE;
			if (ok) {
				decoded.record.read_rec_ptr = reader->ReadRecPtr;
				decoded.record.end_rec_ptr = reader->EndRecPtr;
				decoded.record.prev_rec_ptr = reader->record->header.xl_prev;
				decoded.xid_input.record_xid = XLogRecGetXid(reader);
				decoded.xid_input.complete = true;
			}
		} else
			ok = cluster_cold_recovery_decode_v1(reader, source->claim.identity.system_identifier,
				source->claim.identity.storage_uuid, false, false, (ClusterColdDecodedV1 *)&decoded)
					== CLUSTER_COLD_OK;
		ok = ok && cluster_cold_xid_record_v1(progress, participant,
				(const ClusterColdRecordV1 *)&decoded.record,
				(const ClusterColdXidInputV1 *)&decoded.xid_input)
			&& progress->next_xid >= before;
	}
	PG_FINALLY();
	{
		cluster_cold_decoded_release_v1((ClusterColdDecodedV1 *)&decoded);
	}
	PG_END_TRY();
	if (ok && work->source_cursor == xid->source_index
		&& reader->EndRecPtr == work->relmap_query.pending_owner.wal_end) {
		if (xid->map_end != InvalidXLogRecPtr || XLogRecGetRmid(reader) != RM_RELMAP_ID)
			return false;
		xid->before_map = before;
		xid->after_map = progress->next_xid;
		xid->map_start = reader->ReadRecPtr;
		xid->map_end = reader->EndRecPtr;
		xid->map_ordinal = ordinal;
	}
	return ok && (XLogRecGetRmid(reader) != RM_CLOG_ID
		|| inventory_relmap_clog_record(work, saved, before, progress->next_xid, true));
}

static RfPageProofDetailV1
inventory_page_rebuildable(void *arg, const RfOpcodeRouteV1 *route,
	const RfPageVersionEdgeEntryV1 *edge, const DecodedBkpBlock *block)
{
	(void)arg;
	return route != NULL && route->record_owner == RF_ROUTE_OWNER_PAGE_CODEC
		&& edge != NULL && block != NULL && block->in_use
		&& edge->page_class == RF_PAGE_CLASS_REBUILDABLE_FSM
		? RF_PAGE_PROOF_DETAIL_OK : RF_PAGE_PROOF_DETAIL_CLASS_UNKNOWN;
}

static RfPageProofDetailV1
inventory_page_plan_feed(ClusterRecoveryNativeInventory *work, XLogReaderState *reader)
{
	const NativeInventorySource *source = &work->sources[work->source_cursor];
	RfDetachedRecordPlanV1 detached = {0};
	RfPageOnlineRecordIdentityV1 identity = {0};
	RfPageProofDetailV1 detail;

	if (reader->record->has_page_version_edge) {
		RfDetachedOwnerOpsV1 ops = {0};

		/* A class label cannot qualify a SIDE component. Only the original
		 * PAGE codecs and independently rebuildable FSM inputs belong here. */
		ops.preflight_rebuildable_component = inventory_page_rebuildable;
		detail = rf_page_detached_preflight_v1(reader, true, &ops, &detached);
		if (detail != RF_PAGE_PROOF_DETAIL_OK)
			return detail;
	} else {
		/* Advance the COMPLETE physical transcript through native/control
		 * tails too. This zero-component projection neither executes nor
		 * qualifies SIDE effects. Never hide an unversioned block reference. */
		if (reader->record->max_block_id >= 0)
			return RF_PAGE_PROOF_DETAIL_COMPONENT_INCOMPLETE;
		detached.source_record = reader;
		detached.preflight_complete = true;
	}
	identity.record.system_identifier = source->source.claim.identity.system_identifier;
	memcpy(identity.record.storage_uuid, source->source.claim.identity.storage_uuid, 16);
	identity.record.origin_thread = source->cut.failed_thread;
	identity.record.timeline_id = source->cut.timeline_id;
	identity.record.read_rec_ptr = reader->ReadRecPtr;
	identity.record.end_rec_ptr = reader->EndRecPtr;
	identity.record.record_crc = (uint32)reader->record->header.xl_crc;
	identity.record.rmid = reader->record->header.xl_rmid;
	identity.record.info = reader->record->header.xl_info;
	identity.participant_index = (uint16)work->source_cursor;
	return rf_page_online_plan_queue_record_v1(work->page_build, &detached, &identity);
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
	if (!inventory_history_checkpoint(work, reader))
		return inventory_refuse(work, CLUSTER_RECOVERY_NATIVE_INVENTORY_CHANGED,
								RF_PAGE_PROOF_DETAIL_IDENTITY_MISMATCH);
	if (work->source_records == UINT64_MAX || work->diag.records == UINT64_MAX)
		return inventory_refuse(work, CLUSTER_RECOVERY_NATIVE_INVENTORY_CAPACITY,
								RF_PAGE_PROOF_DETAIL_CAPACITY);
	ordinal = work->source_records++;
	if (work->sources[work->source_cursor].history.input.kind == CLUSTER_WAL_INPUT_CHECKPOINT
		&& reader->ReadRecPtr >= work->sources[work->source_cursor].history.input.native_checkpoint.redo)
		work->source_native_redo_records++;
	if (!cluster_recovery_record_hash_update(work->hash, reader))
		return inventory_refuse(work, CLUSTER_RECOVERY_NATIVE_INVENTORY_INVALID,
								RF_PAGE_PROOF_DETAIL_SOURCE_GAP);
	if (reader->record->has_page_version_edge) {
		if (work->pass == 0) {
			RfPageProofDetailV1 detail = inventory_page_save(work, reader, ordinal);
			if (detail != RF_PAGE_PROOF_DETAIL_OK) return detail;
		} else {
			NativeInventoryPage *page = work->page_cursor;
			if (page == NULL || page->source_index != work->source_cursor || page->ordinal != ordinal)
				return inventory_refuse(work, CLUSTER_RECOVERY_NATIVE_INVENTORY_CHANGED,
					RF_PAGE_PROOF_DETAIL_SOURCE_GAP);
			work->page_cursor = page->next;
		}
	}
	if (work->page_build != NULL) {
		RfPageProofDetailV1 detail = inventory_page_plan_feed(work, reader);

		if (detail != RF_PAGE_PROOF_DETAIL_OK)
			return inventory_refuse(work,
				detail == RF_PAGE_PROOF_DETAIL_CAPACITY || detail == RF_PAGE_PROOF_DETAIL_OOM
					? CLUSTER_RECOVERY_NATIVE_INVENTORY_CAPACITY
					: CLUSTER_RECOVERY_NATIVE_INVENTORY_UNSUPPORTED, detail);
	}
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
		   || XLogRecGetRmid(reader) == RM_COMMIT_TS_ID || XLogRecGetRmid(reader) == RM_RELMAP_ID
		   || (XLogRecGetRmid(reader) == RM_SMGR_ID
			   && (XLogRecGetInfo(reader) & ~XLR_INFO_MASK) == XLOG_SMGR_CREATE);
	if (work->pass == 1) {
		record = work->cursor;
		if (!owns) {
			if (record != NULL && record->source_index == work->source_cursor
				&& record->ordinal <= ordinal)
				return inventory_refuse(work, CLUSTER_RECOVERY_NATIVE_INVENTORY_CHANGED,
										RF_PAGE_PROOF_DETAIL_SOURCE_GAP);
			if (!inventory_relmap_xid_record(work, reader, ordinal, NULL))
				return inventory_refuse(work, CLUSTER_RECOVERY_NATIVE_INVENTORY_CHANGED,
					RF_PAGE_PROOF_DETAIL_IDENTITY_MISMATCH);
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
		if (!inventory_relmap_xid_record(work, reader, ordinal, record))
			return inventory_refuse(work, CLUSTER_RECOVERY_NATIVE_INVENTORY_CHANGED,
				RF_PAGE_PROOF_DETAIL_IDENTITY_MISMATCH);
		if (work->relmap_terminal != NULL
			&& work->source_cursor == work->relmap_terminal->source_index
			&& XLogRecGetRmid(reader) == RM_XACT_ID
			&& ((XLogRecGetInfo(reader) & XLOG_XACT_OPMASK) == XLOG_XACT_COMMIT
				|| (XLogRecGetInfo(reader) & XLOG_XACT_OPMASK) == XLOG_XACT_ABORT)) {
			NativeInventoryRelmapTerminal *terminal = work->relmap_terminal;
			if (terminal->matched >= terminal->expected
				|| !cluster_wal_inputs_relmap_terminal_matches_v1(&terminal->source, reader))
				return inventory_refuse(work, CLUSTER_RECOVERY_NATIVE_INVENTORY_CHANGED,
					RF_PAGE_PROOF_DETAIL_SIDE_INCOMPLETE);
			terminal->matched++;
		}
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
	if (XLogRecGetRmid(reader) == RM_RELMAP_ID)
		work->diag.relmap_records++;
	if (XLogRecGetRmid(reader) == RM_XACT_ID
		&& (XLogRecGetInfo(reader) & ~XLR_INFO_MASK) == XLOG_XACT_PREPARE)
		work->diag.prepare_records++;
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
	work->page_cursor = work->pages;
	work->active = false;
	if (!inventory_history_inputs(work, inputs, work->pass == 0)) {
		inventory_refuse(work, CLUSTER_RECOVERY_NATIVE_INVENTORY_CHANGED,
						 RF_PAGE_PROOF_DETAIL_IDENTITY_MISMATCH);
		return;
	}
	work->diag.input_result = cluster_wal_inputs_cold_census_v1(
		inputs, inventory_record, inventory_source, work, &records, &work->diag.input_detail);
	if (work->diag.input_result != CLUSTER_CONTROL_ROOT_OK_PRIMARY) {
		if (work->status == CLUSTER_RECOVERY_NATIVE_INVENTORY_OBSERVED)
			work->status = CLUSTER_RECOVERY_NATIVE_INVENTORY_INPUT;
	} else if (work->active || work->source_cursor != work->source_count
			   || records != work->diag.records
			   || (work->pass == 1 && (work->cursor != NULL || work->page_cursor != NULL)))
		work->status = CLUSTER_RECOVERY_NATIVE_INVENTORY_CHANGED;
	else if (!inventory_history_inputs(work, inputs, false))
		inventory_refuse(work, CLUSTER_RECOVERY_NATIVE_INVENTORY_CHANGED,
						 RF_PAGE_PROOF_DETAIL_IDENTITY_MISMATCH);
	if (work->status == CLUSTER_RECOVERY_NATIVE_INVENTORY_OBSERVED
		&& !inventory_pages_current(work))
		work->status = CLUSTER_RECOVERY_NATIVE_INVENTORY_CHANGED;
}

static ClusterRecoveryNativeInventoryResult
inventory_build(ClusterWalInputsV1 *inputs, ClusterRecoveryRootSet *roots, size_t memory_budget,
				ClusterRecoveryNativeInventory **out, ClusterRecoveryNativeInventoryDiag *diag,
				const ClusterRecoveryRelmapSourceV1 *relmap_owner)
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
	work->relmap_owner = relmap_owner;
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
	return inventory_build(inputs, NULL, memory_budget, out, diag, NULL);
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
	result = inventory_build(inputs, roots, memory_budget, out, diag, NULL);
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

static void
inventory_page_lock_held(LWLock *lock, LWLockMode mode, void *arg)
{
	(void)lock;
	(void)mode;
	*(bool *)arg = true;
}

ClusterRecoveryPagePlanBuildV1
cluster_recovery_native_inventory_page_plan_v1(ClusterWalInputsV1 *inputs,
	ClusterRecoveryNativeInventory *inventory, Size memory_budget)
{
	typedef struct PageBuild {
		RfPageOnlinePlanV1 *plan;
		RfPageProofDetailV1 detail;
		ClusterRecoveryNativeInventoryDiag diag;
	} PageBuild;
	ClusterRecoveryPagePlanBuildV1 result = {NULL, RF_PAGE_PROOF_DETAIL_INVALID_ARGUMENT};
	RfPageOnlinePlanRequestV1 request = {0};
	PageBuild *build;
	ClusterWalSourceRef *sources;
	RfContributorStreamCutV1 *cuts;
	XLogRecPtr *redo;
	Size bytes, budget = memory_budget == 0 ? RF_PAGE_ONLINE_PLAN_MAX_BYTES : memory_budget;
	uint32 count;
	bool held = false;

	if (inputs == NULL || inventory == NULL || !inventory->complete
		|| inventory->status != CLUSTER_RECOVERY_NATIVE_INVENTORY_OBSERVED
		|| inventory->page_build != NULL || CritSectionCount != 0 || ShutdownRequestPending)
		return result;
	/* No physical/ROOT I/O or native owner snapshot under caller PAGE/content
	 * protection, including a caller with no temporary LWLock headroom. */
	ForEachLWLockHeldByMe(inventory_page_lock_held, &held);
	if (held) {
		result.detail = RF_PAGE_PROOF_DETAIL_WOULD_BLOCK;
		return result;
	}
	count = inventory->source_count;
	if (count == 0 || count > RF_PAGE_STABLE_MAX_PARTICIPANTS || inventory->sources == NULL)
		return result;
	bytes = MAXALIGN(sizeof(*build)) + count * (MAXALIGN(sizeof(*sources))
		+ MAXALIGN(sizeof(*cuts)) + MAXALIGN(sizeof(*redo)));
	if (budget > RF_PAGE_ONLINE_PLAN_MAX_BYTES || budget <= bytes) {
		result.detail = RF_PAGE_PROOF_DETAIL_CAPACITY;
		return result;
	}
	build = calloc(1, bytes);
	if (build == NULL) {
		result.detail = RF_PAGE_PROOF_DETAIL_OOM;
		return result;
	}
	sources = (ClusterWalSourceRef *)((char *)build + MAXALIGN(sizeof(*build)));
	cuts = (RfContributorStreamCutV1 *)((char *)sources + count * MAXALIGN(sizeof(*sources)));
	redo = (XLogRecPtr *)((char *)cuts + count * MAXALIGN(sizeof(*cuts)));
	build->detail = RF_PAGE_PROOF_DETAIL_OK;
	PG_TRY();
	{
		for (uint32 i = 0; i < count; i++) {
			const NativeInventorySource *saved = &inventory->sources[i];
			const ClusterWalInputV1 *input = &saved->history.input;

			sources[i] = saved->source; cuts[i] = saved->cut;
			if (!inventory_source_equal(&sources[i], &input->source)) {
				build->detail = RF_PAGE_PROOF_DETAIL_IDENTITY_MISMATCH;
				break;
			}
			if (input->kind == CLUSTER_WAL_INPUT_CHECKPOINT && saved->history.checkpoint_observed)
				redo[i] = input->native_redo;
			else if (input->kind == CLUSTER_WAL_INPUT_INITIALIZER_TERMINAL)
				redo[i] = cuts[i].scan_end_exclusive;
			else {
				build->detail = RF_PAGE_PROOF_DETAIL_SOURCE_GAP;
				break;
			}
		}
		if (build->detail == RF_PAGE_PROOF_DETAIL_OK) {
			request.system_identifier = sources[0].claim.identity.system_identifier;
			memcpy(request.storage_uuid, sources[0].claim.identity.storage_uuid, 16);
			request.physical_cuts = cuts; request.redo_starts = redo;
			request.participant_count = count;
			/* Pure input binding only, never a native authority cookie. */
			request.retention_binding_cookie = (uint64)(uintptr_t)inventory;
			request.memory_budget = budget - bytes;
			build->detail = rf_page_online_plan_create_v1(&request, &build->plan);
		}
		if (build->detail == RF_PAGE_PROOF_DETAIL_OK
			&& !rf_page_online_plan_bind_sources_v1(build->plan, sources, count))
			build->detail = RF_PAGE_PROOF_DETAIL_CAPACITY;
		if (build->detail == RF_PAGE_PROOF_DETAIL_OK) {
			/* The original physical reread compares every full-generation
			 * source, checkpoint, tail, transcript and saved native/PAGE record.
			 * Its existing cursor restoration preserves native effect iteration. */
			inventory->page_build = build->plan;
			if (cluster_recovery_native_inventory_recheck(inputs, inventory, &build->diag)
				!= CLUSTER_RECOVERY_NATIVE_INVENTORY_OBSERVED)
				build->detail = build->diag.input_detail != RF_PAGE_PROOF_DETAIL_OK
					? build->diag.input_detail : RF_PAGE_PROOF_DETAIL_SOURCE_GAP;
			inventory->page_build = NULL;
		}
		if (build->detail == RF_PAGE_PROOF_DETAIL_OK)
			build->detail = rf_page_online_plan_seal_v1(build->plan);
	}
	PG_CATCH();
	{
		inventory->page_build = NULL;
		inventory->complete = false;
		(void)inventory_refuse(inventory, CLUSTER_RECOVERY_NATIVE_INVENTORY_INVALID,
			RF_PAGE_PROOF_DETAIL_SOURCE_GAP);
		rf_page_online_plan_destroy_v1(&build->plan);
		free(build);
		PG_RE_THROW();
	}
	PG_END_TRY();
	result.detail = build->detail;
	if (result.detail == RF_PAGE_PROOF_DETAIL_OK)
		result.plan = build->plan;
	else {
		inventory->complete = false;
		(void)inventory_refuse(inventory,
			result.detail == RF_PAGE_PROOF_DETAIL_CAPACITY || result.detail == RF_PAGE_PROOF_DETAIL_OOM
				? CLUSTER_RECOVERY_NATIVE_INVENTORY_CAPACITY : CLUSTER_RECOVERY_NATIVE_INVENTORY_CHANGED,
			result.detail);
		rf_page_online_plan_destroy_v1(&build->plan);
	}
	free(build);
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
	inventory_revoke_dependents(inventory);
	return inventory->status;
}

static ClusterRecoveryTerminalResult
inventory_terminal_fail(ClusterRecoveryNativeInventory *inventory,
						ClusterRecoveryTerminalResult result)
{
	inventory->terminal_status = result;
	inventory->status = CLUSTER_RECOVERY_NATIVE_INVENTORY_INVALID;
	inventory->complete = false;
	inventory_revoke_dependents(inventory);
	return result;
}

/* Normalize the original parsed carriers, never a visibility decision. */
static bool
inventory_terminal_prepare(const ClusterRecoveryNativeEffect *effect,
						   ClusterRecoveryTerminalEntry *out)
{
	const ClusterRecoveryNativeRecord *record = effect->record;
	ClusterRecoveryTxnKey *key = &out->transaction;
	uint16 origin = record->source.claim.identity.origin_thread_id;

	memset(out, 0, sizeof(*out));
	out->record = record;
	key->system_identifier = record->source.claim.identity.system_identifier;
	key->database_incarnation = record->source.claim.database_incarnation;
	key->origin_node_id = origin - 1;
	key->segment_generation = UINT32_MAX; /* absent is never generation zero */
	if (effect->kind == CLUSTER_RECOVERY_NATIVE_TERMINAL) {
		uint8 opcode = record->header.xl_info & XLOG_XACT_OPMASK;
		key->xid = record->top_xid;
		out->status = opcode == XLOG_XACT_COMMIT ? TT_SLOT_COMMITTED : TT_SLOT_ABORTED;
		out->scn = record->terminal_scn;
		out->identity = CLUSTER_RECOVERY_TERMINAL_NATIVE_ONLY;
		if (opcode != XLOG_XACT_COMMIT && opcode != XLOG_XACT_ABORT)
			return false;
		if (record->xinfo & XACT_XINFO_HAS_TT_COMMIT) {
			const xl_xact_tt_commit *tt = &record->tt_commit;
			/* Same exact producer fields as the original folded TT owner. */
			if (opcode != XLOG_XACT_COMMIT || tt->instance != origin || tt->segment_id == 0
				|| ((tt->segment_id - 1) / CLUSTER_UNDO_SEGS_PER_INSTANCE) + 1 != origin
				|| tt->segment_generation == UINT32_MAX || tt->slot_offset >= TT_SLOTS_PER_SEGMENT
				|| tt->wrap == TT_WRAP_INVALID || tt->xid != key->xid
				|| !TransactionIdIsNormal(tt->xid) || !SCN_VALID(tt->commit_scn)
				|| tt->format_version != CLUSTER_XACT_TT_COMMIT_VERSION || tt->flags != 0
				|| tt->reserved != 0 || tt->commit_scn != out->scn)
				return false;
			key->segment_id = tt->segment_id;
			key->segment_generation = tt->segment_generation;
			key->slot_offset = tt->slot_offset;
			key->slot_wrap = tt->wrap;
			out->identity = CLUSTER_RECOVERY_TERMINAL_EXACT;
		}
	} else {
		const ClusterUndoDecoded *tt = effect->bytes;
		if (tt->instance != origin)
			return false;
		key->segment_id = tt->segment_id;
		key->slot_offset = tt->slot_offset;
		key->slot_wrap = tt->wrap;
		key->xid = tt->xid;
		out->status = tt->kind == CLUSTER_UNDO_KIND_TT_COMMIT ? TT_SLOT_COMMITTED : TT_SLOT_ABORTED;
		out->scn = tt->commit_scn;
		out->identity = CLUSTER_RECOVERY_TERMINAL_BINDING_MISSING;
		if (tt->kind == CLUSTER_UNDO_KIND_TT_ABORT
			&& tt->format_version == CLUSTER_UNDO_TT_ABORT_EXACT_VERSION) {
			key->segment_generation = tt->expected_generation;
			out->identity = CLUSTER_RECOVERY_TERMINAL_EXACT;
		}
	}
	return true;
}

static ClusterRecoveryTerminalResult
inventory_terminal_step(ClusterRecoveryNativeInventory *inventory,
						ClusterRecoveryTerminalEntry *out)
{
	unsigned work = 0;

	if (inventory->terminal_status == CLUSTER_RECOVERY_TERMINAL_DRAINED)
		return inventory->terminal_status;
	if (!inventory->terminal_started) {
		inventory->terminal_first = inventory->terminal_cursor = inventory->head;
		inventory->terminal_started = true;
	}
	while (work < INVENTORY_SEED_TICK) {
		NativeInventoryRecord *current = inventory->terminal_cursor;
		ClusterRecoveryTerminalEntry *pending = &inventory->terminal_pending;
		if (current == NULL)
			return inventory->terminal_status = CLUSTER_RECOVERY_TERMINAL_DRAINED;
		if (!inventory->terminal_searching) {
			ClusterRecoveryNativeEffect effect;
			const ClusterUndoDecoded *undo;
			if (cluster_recovery_native_scan_primary(current->scan, &effect)
				!= CLUSTER_RECOVERY_NATIVE_EFFECT)
				return inventory_terminal_fail(inventory, CLUSTER_RECOVERY_TERMINAL_CHANGED);
			work++;
			if (current->source_index != inventory->terminal_first->source_index)
				inventory->terminal_first = current;
			undo = effect.kind == CLUSTER_RECOVERY_NATIVE_UNDO ? effect.bytes : NULL;
			if (effect.kind != CLUSTER_RECOVERY_NATIVE_TERMINAL
				&& (undo == NULL
					|| (undo->kind != CLUSTER_UNDO_KIND_TT_COMMIT
						&& undo->kind != CLUSTER_UNDO_KIND_TT_ABORT))) {
				inventory->terminal_cursor = current->next;
				continue;
			}
			if (!inventory_terminal_prepare(&effect, pending))
				return inventory_terminal_fail(inventory, CLUSTER_RECOVERY_TERMINAL_INVALID);
			if (pending->identity == CLUSTER_RECOVERY_TERMINAL_BINDING_MISSING) {
				inventory->terminal_search = inventory->terminal_first;
				inventory->terminal_binding = NULL;
				inventory->terminal_binding_record = NULL;
				inventory->terminal_searching = true;
			}
		}
		if (inventory->terminal_searching) {
			while (inventory->terminal_search != current && work < INVENTORY_SEED_TICK) {
				NativeInventoryRecord *candidate = inventory->terminal_search;
				ClusterRecoveryNativeEffect effect;
				const ClusterUndoDecoded *undo;
				if (candidate == NULL || candidate->source_index != current->source_index
					|| cluster_recovery_native_scan_primary(candidate->scan, &effect)
						   != CLUSTER_RECOVERY_NATIVE_EFFECT
					|| !inventory_source_equal(&effect.record->source, &pending->record->source)
					|| !inventory_cut_equal(&effect.record->cut, &pending->record->cut))
					return inventory_terminal_fail(inventory, CLUSTER_RECOVERY_TERMINAL_CHANGED);
				if (inventory->diag.terminal_binding_examined == UINT64_MAX)
					return inventory_terminal_fail(inventory, CLUSTER_RECOVERY_TERMINAL_CAPACITY);
				inventory->diag.terminal_binding_examined++;
				work++;
				inventory->terminal_search = candidate->next;
				if (effect.kind != CLUSTER_RECOVERY_NATIVE_UNDO)
					continue;
				undo = effect.bytes;
				if (undo->segment_id != pending->transaction.segment_id)
					continue;
				if (undo->kind == CLUSTER_UNDO_KIND_SEGMENT_INIT
					|| undo->kind == CLUSTER_UNDO_KIND_SEGMENT_RECYCLE
					|| undo->kind == CLUSTER_UNDO_KIND_SEGMENT_REUSE) {
					inventory->terminal_binding = NULL;
					inventory->terminal_binding_record = NULL;
				} else if (undo->kind == CLUSTER_UNDO_KIND_TT_BIND
						   && undo->slot_offset == pending->transaction.slot_offset) {
					inventory->terminal_binding = undo;
					inventory->terminal_binding_record = effect.record;
				}
			}
			if (inventory->terminal_search != current)
				return CLUSTER_RECOVERY_TERMINAL_MORE;
			if (inventory->terminal_binding != NULL) {
				const ClusterUndoDecoded *binding = inventory->terminal_binding;
				if (binding->instance != pending->transaction.origin_node_id + 1
					|| binding->xid != pending->transaction.xid
					|| binding->wrap != pending->transaction.slot_wrap)
					pending->identity = CLUSTER_RECOVERY_TERMINAL_BINDING_MISMATCH;
				else {
					pending->identity = CLUSTER_RECOVERY_TERMINAL_BOUND;
					pending->transaction.segment_generation = binding->expected_generation;
					pending->binding_record = inventory->terminal_binding_record;
				}
			}
		}
		if (inventory->diag.terminal_records == UINT64_MAX)
			return inventory_terminal_fail(inventory, CLUSTER_RECOVERY_TERMINAL_CAPACITY);
		inventory->diag.terminal_records++;
		if (pending->identity == CLUSTER_RECOVERY_TERMINAL_EXACT
			|| pending->identity == CLUSTER_RECOVERY_TERMINAL_BOUND)
			inventory->diag.terminal_bound++;
		else
			inventory->diag.terminal_unbound++;
		current->terminal = *pending;
		*out = *pending;
		inventory->terminal_cursor = current->next;
		inventory->terminal_searching = false;
		return CLUSTER_RECOVERY_TERMINAL_ENTRY;
	}
	return CLUSTER_RECOVERY_TERMINAL_MORE;
}

ClusterRecoveryTerminalResult
cluster_recovery_native_inventory_terminal_step(ClusterRecoveryNativeInventory *inventory,
												ClusterRecoveryTerminalEntry *out,
												ClusterRecoveryNativeInventoryDiag *diag)
{
	ClusterRecoveryTerminalResult result;
	if (out != NULL)
		memset(out, 0, sizeof(*out));
	if (diag != NULL)
		memset(diag, 0, sizeof(*diag));
	if (inventory == NULL)
		return CLUSTER_RECOVERY_TERMINAL_INVALID;
	if (out == NULL)
		result = inventory_terminal_fail(inventory, CLUSTER_RECOVERY_TERMINAL_INVALID);
	else if (!inventory->complete
			 || inventory->status != CLUSTER_RECOVERY_NATIVE_INVENTORY_OBSERVED)
		result = inventory_terminal_fail(inventory, CLUSTER_RECOVERY_TERMINAL_CHANGED);
	else
		result = inventory_terminal_step(inventory, out);
	if (diag != NULL)
		*diag = inventory->diag;
	return result;
}

static bool
inventory_terminal_key_equal(const ClusterRecoveryTxnKey *a, const ClusterRecoveryTxnKey *b)
{
	return a->system_identifier == b->system_identifier
		   && a->database_incarnation == b->database_incarnation
		   && a->origin_node_id == b->origin_node_id && a->segment_id == b->segment_id
		   && a->segment_generation == b->segment_generation && a->slot_offset == b->slot_offset
		   && a->slot_wrap == b->slot_wrap && a->xid == b->xid;
}

static ClusterRecoveryTerminalResult
inventory_terminal_lookup(ClusterRecoveryNativeInventory *inventory,
						  const ClusterRecoveryTxnKey *key, ClusterRecoveryTerminalSummary *out)
{
	ClusterRecoveryTerminalSummary *summary = &inventory->terminal_lookup;
	const ClusterWalSourceRef *source;
	unsigned examined = 0;

	if (!inventory->complete || inventory->status != CLUSTER_RECOVERY_NATIVE_INVENTORY_OBSERVED)
		return inventory_terminal_fail(inventory, CLUSTER_RECOVERY_TERMINAL_CHANGED);
	if (key == NULL || out == NULL || inventory->source_count == 0
		|| inventory->terminal_status != CLUSTER_RECOVERY_TERMINAL_DRAINED)
		return inventory_terminal_fail(inventory, CLUSTER_RECOVERY_TERMINAL_INVALID);
	source = &inventory->sources[0].source;
	if (key->system_identifier != source->claim.identity.system_identifier
		|| key->database_incarnation != source->claim.database_incarnation
		|| key->origin_node_id >= UNDO_OWNER_INSTANCE_MAX || key->segment_id == 0
		|| (key->segment_id - 1) / CLUSTER_UNDO_SEGS_PER_INSTANCE != key->origin_node_id
		|| key->segment_generation == UINT32_MAX || key->slot_offset >= TT_SLOTS_PER_SEGMENT
		|| key->slot_wrap == TT_WRAP_INVALID || !TransactionIdIsNormal(key->xid))
		return inventory_terminal_fail(inventory, CLUSTER_RECOVERY_TERMINAL_INVALID);
	if (!inventory->terminal_lookup_active) {
		memset(summary, 0, sizeof(*summary));
		summary->transaction = *key;
		inventory->terminal_lookup_cursor = inventory->head;
		inventory->terminal_lookup_active = true;
	} else if (!inventory_terminal_key_equal(key, &summary->transaction))
		return inventory_terminal_fail(inventory, CLUSTER_RECOVERY_TERMINAL_INVALID);

	while (inventory->terminal_lookup_cursor != NULL && examined < INVENTORY_SEED_TICK) {
		const ClusterRecoveryTerminalEntry *entry = &inventory->terminal_lookup_cursor->terminal;
		const ClusterRecoveryTxnKey *candidate = &entry->transaction;
		if (inventory->diag.terminal_lookup_examined == UINT64_MAX)
			return inventory_terminal_fail(inventory, CLUSTER_RECOVERY_TERMINAL_CAPACITY);
		inventory->diag.terminal_lookup_examined++;
		examined++;
		inventory->terminal_lookup_cursor = inventory->terminal_lookup_cursor->next;
		if (entry->record == NULL)
			continue;
		if (entry->identity != CLUSTER_RECOVERY_TERMINAL_EXACT
			&& entry->identity != CLUSTER_RECOVERY_TERMINAL_BOUND) {
			/* Without a complete era/slot identity, a same-origin raw xid
			 * cannot be dismissed as belonging to another wrap or transaction. */
			if (candidate->system_identifier == key->system_identifier
				&& candidate->database_incarnation == key->database_incarnation
				&& candidate->origin_node_id == key->origin_node_id && candidate->xid == key->xid)
				summary->unresolved_records++;
			continue;
		}
		if (!inventory_terminal_key_equal(candidate, key))
			continue;
		if (summary->first_exact == NULL)
			summary->first_exact = entry->record;
		summary->exact_records++;
		if (entry->status == TT_SLOT_COMMITTED) {
			if (summary->has_commit && summary->commit_scn != entry->scn)
				summary->conflict = true;
			if (!summary->has_commit)
				summary->commit_scn = entry->scn;
			summary->has_commit = true;
		} else if (entry->status == TT_SLOT_ABORTED)
			summary->has_abort = true;
		else
			return inventory_terminal_fail(inventory, CLUSTER_RECOVERY_TERMINAL_INVALID);
		if (summary->has_commit && summary->has_abort)
			summary->conflict = true;
	}
	if (inventory->terminal_lookup_cursor != NULL)
		return CLUSTER_RECOVERY_TERMINAL_MORE;
	*out = *summary;
	inventory->terminal_lookup_active = false;
	return CLUSTER_RECOVERY_TERMINAL_ENTRY;
}

ClusterRecoveryTerminalResult
cluster_recovery_native_inventory_terminal_lookup_step(ClusterRecoveryNativeInventory *inventory,
													   const ClusterRecoveryTxnKey *key,
													   ClusterRecoveryTerminalSummary *out,
													   ClusterRecoveryNativeInventoryDiag *diag)
{
	ClusterRecoveryTerminalResult result;
	if (out != NULL)
		memset(out, 0, sizeof(*out));
	if (diag != NULL)
		memset(diag, 0, sizeof(*diag));
	if (inventory == NULL)
		return CLUSTER_RECOVERY_TERMINAL_INVALID;
	result = inventory_terminal_lookup(inventory, key, out);
	if (diag != NULL)
		*diag = inventory->diag;
	return result;
}

static ClusterRecoveryRelmapFoldResult
inventory_relmap_fail(ClusterRecoveryNativeInventory *inventory,
					  ClusterRecoveryRelmapFoldResult result)
{
	inventory->status = CLUSTER_RECOVERY_NATIVE_INVENTORY_INVALID;
	inventory->complete = false;
	inventory_revoke_dependents(inventory);
	return result;
}

/* Native record framing and complete map shape were checked by the original
 * immutable scan. Compare only identities actually represented by this WAL. */
static bool
inventory_relmap_wal_matches(const ClusterRelmapV2Image *pending,
							 const ClusterRecoveryNativeRecord *record)
{
	TransactionId raw = (TransactionId)pending->pending_owner.top_full_xid;

	return record->source.claim.identity.origin_owner_incarnation
			   == pending->pending_owner.admitted_incarnation
		   && record->end_lsn == pending->pending_owner.wal_end && record->header.xl_xid == raw
		   && (!TransactionIdIsValid(record->toplevel_xid) || record->toplevel_xid == raw)
		   && record->database == pending->identity.dbid
		   && (pending->identity.shared
				   ? record->tablespace == GLOBALTABLESPACE_OID
				   : OidIsValid(record->tablespace) && record->tablespace != GLOBALTABLESPACE_OID)
		   && record->payload_length == MinSizeOfRelmapUpdate + sizeof(RelMapFile)
		   && memcmp(record->payload + MinSizeOfRelmapUpdate, &pending->pending, sizeof(RelMapFile))
				  == 0;
}

static bool
inventory_relmap_count(ClusterRecoveryNativeInventory *inventory,
					   const ClusterRecoveryRelmapFoldEntry *entry)
{
	ClusterRecoveryRelmapFoldSummary *summary = &inventory->relmap_summary;
	uint32 flags = entry->flags;

#define RELMAP_COUNT(condition_, field_)                                                           \
	do {                                                                                           \
		if (condition_) {                                                                          \
			if (summary->field_ == UINT64_MAX)                                                     \
				return false;                                                                      \
			summary->field_++;                                                                     \
		}                                                                                          \
	} while (0)
	RELMAP_COUNT(flags & CLUSTER_RECOVERY_RELMAP_FOLD_HEADER_XID, header_xid_records);
	RELMAP_COUNT(flags & CLUSTER_RECOVERY_RELMAP_FOLD_RECORD_TOP_XID, record_top_xid_records);
	RELMAP_COUNT(flags & CLUSTER_RECOVERY_RELMAP_FOLD_WAL_MATCH, wal_matches);
	RELMAP_COUNT(flags & CLUSTER_RECOVERY_RELMAP_FOLD_WAL_ALIAS, wal_aliases);
	RELMAP_COUNT((flags & CLUSTER_RECOVERY_RELMAP_FOLD_TOP_XID)
					 && (flags & CLUSTER_RECOVERY_RELMAP_FOLD_COMMIT),
				 top_commits);
	RELMAP_COUNT((flags & CLUSTER_RECOVERY_RELMAP_FOLD_TOP_XID)
					 && (flags & CLUSTER_RECOVERY_RELMAP_FOLD_ABORT),
				 top_aborts);
	RELMAP_COUNT(flags & CLUSTER_RECOVERY_RELMAP_FOLD_ASSIGNMENT, assignments);
	RELMAP_COUNT(flags & CLUSTER_RECOVERY_RELMAP_FOLD_PREPARE, prepares);
	RELMAP_COUNT(flags & CLUSTER_RECOVERY_RELMAP_FOLD_SUBXID, child_records);
	RELMAP_COUNT(flags & CLUSTER_RECOVERY_RELMAP_FOLD_CLOG_ZERO, clog_zeroes);
	RELMAP_COUNT(flags & CLUSTER_RECOVERY_RELMAP_FOLD_CLOG_TRUNCATE, clog_truncates);
#undef RELMAP_COUNT
	if (UINT64_MAX - summary->child_references < entry->subxid_references)
		return false;
	summary->child_references += entry->subxid_references;
	if (flags & CLUSTER_RECOVERY_RELMAP_FOLD_WAL_MATCH)
		inventory->relmap_wal_source = entry->source_index;
	if (flags
		& (CLUSTER_RECOVERY_RELMAP_FOLD_CLOG_ZERO | CLUSTER_RECOVERY_RELMAP_FOLD_CLOG_TRUNCATE)) {
		uint64 *count;
		uint64 end = inventory->relmap_query.pending_owner.wal_end;
		if (entry->record->end_lsn <= end)
			count = &inventory->relmap_clog[entry->source_index].before;
		else if (entry->record->read_lsn >= end)
			count = &inventory->relmap_clog[entry->source_index].after;
		else
			count = &inventory->relmap_clog[entry->source_index].overlap;
		if (*count == UINT64_MAX)
			return false;
		(*count)++;
	}
	return true;
}

/* Defer ordering until the COMPLETE query has established a unique original
 * map source. Numeric positions in other generations never establish order. */
static bool
inventory_relmap_history_finish(ClusterRecoveryNativeInventory *inventory)
{
	ClusterRecoveryRelmapFoldSummary *summary = &inventory->relmap_summary;
	for (uint32 i = 0; i < inventory->source_count; ++i) {
		uint64 before = inventory->relmap_clog[i].before;
		uint64 after = inventory->relmap_clog[i].after;
		uint64 overlap = inventory->relmap_clog[i].overlap;
		if (summary->wal_matches == 1 && inventory->relmap_wal_source == i) {
			if (UINT64_MAX - summary->clog_before_wal < before
				|| UINT64_MAX - summary->clog_after_wal < after)
				return false;
			summary->clog_before_wal += before;
			summary->clog_after_wal += after;
		} else {
			if (UINT64_MAX - overlap < before || UINT64_MAX - overlap - before < after)
				return false;
			overlap += before + after;
		}
		if (UINT64_MAX - summary->clog_unordered < overlap)
			return false;
		summary->clog_unordered += overlap;
	}
	return true;
}

static ClusterRecoveryRelmapFoldResult
inventory_relmap_step(ClusterRecoveryNativeInventory *inventory,
					  ClusterRecoveryRelmapFoldEntry *out,
					  ClusterRecoveryRelmapFoldSummary *summary)
{
	ClusterRecoveryRelmapFoldEntry *entry = &inventory->relmap_pending;
	const ClusterRelmapV2Image *pending = &inventory->relmap_query;
	TransactionId raw = (TransactionId)pending->pending_owner.top_full_xid;
	unsigned examined = 0;

	for (;;) {
		const ClusterRecoveryNativeRecord *record;
		bool xact;

		if (entry->record == NULL) {
			NativeInventoryRecord *saved = inventory->relmap_cursor;
			ClusterRecoveryNativeEffect primary;

			if (saved == NULL) {
				if (!inventory_relmap_history_finish(inventory))
					return inventory_relmap_fail(inventory, CLUSTER_RECOVERY_RELMAP_FOLD_CAPACITY);
				*summary = inventory->relmap_summary;
				inventory->relmap_active = false;
				inventory->relmap_drained = true;
				return CLUSTER_RECOVERY_RELMAP_FOLD_DRAINED;
			}
			if (examined == INVENTORY_SEED_TICK)
				return CLUSTER_RECOVERY_RELMAP_FOLD_MORE;
			if (inventory->diag.relmap_examined == UINT64_MAX)
				return inventory_relmap_fail(inventory, CLUSTER_RECOVERY_RELMAP_FOLD_CAPACITY);
			inventory->diag.relmap_examined++;
			examined++;
			if (saved->source_index >= inventory->source_count
				|| cluster_recovery_native_scan_primary(saved->scan, &primary)
					   != CLUSTER_RECOVERY_NATIVE_EFFECT
				|| primary.record == NULL
				|| !inventory_source_equal(&primary.record->source,
										   &inventory->sources[saved->source_index].source)
				|| !inventory_cut_equal(&primary.record->cut,
										&inventory->sources[saved->source_index].cut))
				return inventory_relmap_fail(inventory, CLUSTER_RECOVERY_RELMAP_FOLD_CHANGED);
			record = primary.record;
			if (record->source.claim.identity.origin_node_id != pending->pending_owner.node) {
				inventory->relmap_cursor = saved->next;
				continue;
			}
			entry->record = record;
			entry->history = inventory_source_equal(
								 &inventory->sources[saved->source_index].history.input.source,
								 &record->source)
								 ? &inventory->sources[saved->source_index].history
								 : NULL;
			entry->kind = primary.kind;
			entry->source_index = saved->source_index;
			entry->record_ordinal = saved->ordinal;
			inventory->relmap_child_cursor = 0;
			if (record->header.xl_xid == raw)
				entry->flags |= CLUSTER_RECOVERY_RELMAP_FOLD_HEADER_XID;
			if (record->toplevel_xid == raw)
				entry->flags |= CLUSTER_RECOVERY_RELMAP_FOLD_RECORD_TOP_XID;
			if (primary.kind == CLUSTER_RECOVERY_NATIVE_RELMAP_UPDATE
				&& (record->header.xl_xid == raw || record->toplevel_xid == raw))
				entry->flags |= inventory_relmap_wal_matches(pending, record)
									? CLUSTER_RECOVERY_RELMAP_FOLD_WAL_MATCH
									: CLUSTER_RECOVERY_RELMAP_FOLD_WAL_ALIAS;
			if (primary.kind == CLUSTER_RECOVERY_NATIVE_CLOG_ZERO) {
				int page;
				memcpy(&page, record->payload, sizeof(page));
				if ((uint32)page == raw / CLUSTER_NATIVE_CLOG_PER_PAGE)
					entry->flags |= CLUSTER_RECOVERY_RELMAP_FOLD_CLOG_ZERO;
			} else if (primary.kind == CLUSTER_RECOVERY_NATIVE_CLOG_TRUNCATE)
				/* A raw cutoff has no independent FullXid era. Keep every
				 * truncate of this physical origin, not just a guessed page. */
				entry->flags |= CLUSTER_RECOVERY_RELMAP_FOLD_CLOG_TRUNCATE;
		}
		record = entry->record;
		xact = entry->kind == CLUSTER_RECOVERY_NATIVE_TERMINAL
			   || entry->kind == CLUSTER_RECOVERY_NATIVE_ASSIGNMENT
			   || entry->kind == CLUSTER_RECOVERY_NATIVE_PREPARE;
		if (xact) {
			if (record->top_xid == raw)
				entry->flags |= CLUSTER_RECOVERY_RELMAP_FOLD_TOP_XID;
			while (inventory->relmap_child_cursor < record->nsubxacts) {
				TransactionId child;
				if (examined == INVENTORY_SEED_TICK)
					return CLUSTER_RECOVERY_RELMAP_FOLD_MORE;
				if (inventory->diag.relmap_examined == UINT64_MAX)
					return inventory_relmap_fail(inventory, CLUSTER_RECOVERY_RELMAP_FOLD_CAPACITY);
				inventory->diag.relmap_examined++;
				examined++;
				memcpy(&child,
					   record->payload + record->subxacts_offset
						   + (size_t)inventory->relmap_child_cursor * sizeof(child),
					   sizeof(child));
				inventory->relmap_child_cursor++;
				if (child == raw) {
					entry->flags |= CLUSTER_RECOVERY_RELMAP_FOLD_SUBXID;
					entry->subxid_references++;
				}
			}
			if (entry->flags
				& (CLUSTER_RECOVERY_RELMAP_FOLD_TOP_XID | CLUSTER_RECOVERY_RELMAP_FOLD_SUBXID)) {
				if (entry->kind == CLUSTER_RECOVERY_NATIVE_ASSIGNMENT)
					entry->flags |= CLUSTER_RECOVERY_RELMAP_FOLD_ASSIGNMENT;
				else if (entry->kind == CLUSTER_RECOVERY_NATIVE_PREPARE)
					entry->flags |= CLUSTER_RECOVERY_RELMAP_FOLD_PREPARE;
				else
					entry->flags |= (record->header.xl_info & XLOG_XACT_OPMASK) == XLOG_XACT_COMMIT
										? CLUSTER_RECOVERY_RELMAP_FOLD_COMMIT
										: CLUSTER_RECOVERY_RELMAP_FOLD_ABORT;
			}
		}
		inventory->relmap_cursor = inventory->relmap_cursor->next;
		if (!inventory_relmap_count(inventory, entry))
			return inventory_relmap_fail(inventory, CLUSTER_RECOVERY_RELMAP_FOLD_CAPACITY);
		if (entry->flags != 0) {
			*out = *entry;
			memset(entry, 0, sizeof(*entry));
			return CLUSTER_RECOVERY_RELMAP_FOLD_ENTRY;
		}
		memset(entry, 0, sizeof(*entry));
	}
}

ClusterRecoveryRelmapFoldResult
cluster_recovery_native_inventory_relmap_step(ClusterRecoveryNativeInventory *inventory,
											  const ClusterRelmapV2Image *pending,
											  ClusterRecoveryRelmapFoldEntry *out,
											  ClusterRecoveryRelmapFoldSummary *summary,
											  ClusterRecoveryNativeInventoryDiag *diag)
{
	ClusterRecoveryRelmapFoldResult result;
	uint8 wire[CLUSTER_RELMAP_V2_FILE_SIZE];

	if (out != NULL)
		memset(out, 0, sizeof(*out));
	if (summary != NULL)
		memset(summary, 0, sizeof(*summary));
	if (diag != NULL)
		memset(diag, 0, sizeof(*diag));
	if (inventory == NULL)
		return CLUSTER_RECOVERY_RELMAP_FOLD_INVALID;
	if (!inventory->complete || inventory->status != CLUSTER_RECOVERY_NATIVE_INVENTORY_OBSERVED)
		result = inventory_relmap_fail(inventory, CLUSTER_RECOVERY_RELMAP_FOLD_CHANGED);
	else if (inventory->sources == NULL || inventory->source_count == 0
			 || inventory->source_count > CLUSTER_WAL_INPUTS_MAX || out == NULL || summary == NULL
			 || pending == NULL || pending->pending_generation == 0
			 || cluster_relmap_v2_encode(pending, wire, sizeof(wire)) != CLUSTER_RELMAP_V2_VALID)
		result = inventory_relmap_fail(inventory, CLUSTER_RECOVERY_RELMAP_FOLD_INVALID);
	else if (inventory->relmap_active && memcmp(wire, inventory->relmap_wire, sizeof(wire)) != 0)
		result = inventory_relmap_fail(inventory, CLUSTER_RECOVERY_RELMAP_FOLD_INVALID);
	else {
		if (!inventory->relmap_active) {
			ClusterRecoveryRelmapFoldSummary *counts = &inventory->relmap_summary;
			memset(counts, 0, sizeof(*counts));
			counts->sources = inventory->source_count;
			memset(inventory->relmap_clog, 0, sizeof(inventory->relmap_clog));
			inventory->relmap_wal_source = UINT32_MAX;
			for (uint32 i = 0; i < inventory->source_count; ++i) {
				const ClusterControlRootIdentity *id = &inventory->sources[i].source.claim.identity;
				const ClusterRecoveryRelmapHistorySource *history = &inventory->sources[i].history;
				const ClusterRelmapCheckpointWindow *window = &history->window;
				uint64 query = pending->pending_owner.top_full_xid;
				if (id->system_identifier != pending->identity.system_identifier
					|| inventory->sources[i].source.claim.database_incarnation
						   != pending->identity.database_incarnation)
					return inventory_relmap_fail(inventory, CLUSTER_RECOVERY_RELMAP_FOLD_INVALID);
				if (id->origin_node_id != pending->pending_owner.node)
					continue;
				counts->origin_sources++;
				if (!inventory_source_equal(&history->input.source, &inventory->sources[i].source))
					continue;
				counts->checkpoint_sources += history->checkpoint_observed;
				counts->window_sources += history->window_observed;
				if (!history->window_observed || query < window->native_floor
					|| query > history->post_flush_issued
					|| history->post_flush_issued - window->native_floor >= UINT64_C(0x80000000))
					continue;
				counts->query_window_sources++;
				if (window->epoch == pending->pending_owner.membership_epoch
					&& window->admitted_incarnation == pending->pending_owner.admitted_incarnation
					&& window->boot_incarnation == pending->pending_owner.boot_incarnation)
					counts->owner_window_sources++;
			}
			if (counts->origin_sources == 0)
				return inventory_relmap_fail(inventory, CLUSTER_RECOVERY_RELMAP_FOLD_INVALID);
			inventory->relmap_query = *pending;
			memcpy(inventory->relmap_wire, wire, sizeof(wire));
			memset(&inventory->relmap_pending, 0, sizeof(inventory->relmap_pending));
			inventory->relmap_cursor = inventory->head;
			inventory->relmap_active = true;
			inventory->relmap_drained = false;
		}
		result = inventory_relmap_step(inventory, out, summary);
	}
	if (diag != NULL)
		*diag = inventory->diag;
	return result;
}

bool
cluster_recovery_native_inventory_relmap_source(const ClusterRecoveryNativeInventory *inventory,
												uint32 index,
												ClusterRecoveryRelmapHistorySource *out)
{
	if (out == NULL)
		return false;
	memset(out, 0, sizeof(*out));
	if (inventory == NULL || !inventory->complete
		|| inventory->status != CLUSTER_RECOVERY_NATIVE_INVENTORY_OBSERVED
		|| inventory->sources == NULL || index >= inventory->source_count
		|| !inventory_source_equal(&inventory->sources[index].history.input.source,
								   &inventory->sources[index].source))
		return false;
	memcpy(out, &inventory->sources[index].history, sizeof(*out));
	return true;
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

static ClusterRecoveryXactJoinResult
inventory_xact_fail(ClusterRecoveryNativeInventory *inventory, ClusterRecoveryXactJoinResult result)
{
	inventory->xact_status = result;
	inventory->status = CLUSTER_RECOVERY_NATIVE_INVENTORY_INVALID;
	inventory->complete = false;
	inventory_revoke_dependents(inventory);
	return result;
}

static ClusterRecoveryXactJoinResult
inventory_xact_step(ClusterRecoveryNativeInventory *inventory, ClusterRecoveryXactJoinEntry *out)
{
	unsigned visited = 0;

	if (inventory->xact_done)
		return CLUSTER_RECOVERY_XACT_JOIN_DRAINED;
	if (inventory->xact_join != NULL) {
		ClusterRecoveryXactJoinResult result
			= cluster_recovery_xact_join_next(inventory->xact_join, out);

		if (result == CLUSTER_RECOVERY_XACT_JOIN_ENTRY) {
			uint64 *count = out->is_parent ? &inventory->diag.xact_parents_compared
										   : &inventory->diag.xact_clog_compared;
			if (*count == UINT64_MAX
				|| (!out->equal && inventory->diag.xact_physical_mismatches == UINT64_MAX)) {
				memset(out, 0, sizeof(*out));
				return inventory_xact_fail(inventory, CLUSTER_RECOVERY_XACT_JOIN_CAPACITY);
			}
			(*count)++;
			if (!out->equal)
				inventory->diag.xact_physical_mismatches++;
		} else if (result == CLUSTER_RECOVERY_XACT_JOIN_DRAINED) {
			cluster_recovery_xact_join_destroy(inventory->xact_join);
			inventory->xact_join = NULL;
			inventory->xact_cursor = inventory->xact_cursor->next;
			return CLUSTER_RECOVERY_XACT_JOIN_MORE;
		} else if (result > CLUSTER_RECOVERY_XACT_JOIN_DRAINED)
			return inventory_xact_fail(inventory, result);
		return result;
	}
	while (inventory->xact_cursor != NULL && visited++ < INVENTORY_SEED_TICK) {
		ClusterRecoveryNativeEffect primary;
		ClusterRecoveryXactJoinResult result;

		if (cluster_recovery_native_scan_primary(inventory->xact_cursor->scan, &primary)
			!= CLUSTER_RECOVERY_NATIVE_EFFECT)
			return inventory_xact_fail(inventory, CLUSTER_RECOVERY_XACT_JOIN_CHANGED);
		if (primary.kind != CLUSTER_RECOVERY_NATIVE_TERMINAL
			&& primary.kind != CLUSTER_RECOVERY_NATIVE_ASSIGNMENT) {
			inventory->xact_cursor = inventory->xact_cursor->next;
			continue;
		}
		result
			= cluster_recovery_xact_join_begin(inventory->xact_roots, inventory->xact_cursor->scan,
											   inventory->xact_budget, &inventory->xact_join);
		if (result != CLUSTER_RECOVERY_XACT_JOIN_MORE)
			return inventory_xact_fail(inventory, result);
		return result;
	}
	if (inventory->xact_cursor == NULL) {
		inventory->xact_done = true;
		return CLUSTER_RECOVERY_XACT_JOIN_DRAINED;
	}
	return CLUSTER_RECOVERY_XACT_JOIN_MORE;
}

ClusterRecoveryXactJoinResult
cluster_recovery_native_inventory_xact_step(ClusterRecoveryNativeInventory *inventory,
											ClusterRecoveryRootSet *roots,
											size_t join_memory_budget,
											ClusterRecoveryXactJoinEntry *out,
											ClusterRecoveryNativeInventoryDiag *diag)
{
	ClusterRecoveryXactJoinResult result;
	uint8 digest[32];

	if (out != NULL)
		memset(out, 0, sizeof(*out));
	if (diag != NULL)
		memset(diag, 0, sizeof(*diag));
	if (inventory == NULL || roots == NULL || out == NULL || diag == NULL) {
		cluster_recovery_root_set_invalidate(roots);
		if (inventory != NULL)
			inventory_xact_fail(inventory, CLUSTER_RECOVERY_XACT_JOIN_INVALID);
		return CLUSTER_RECOVERY_XACT_JOIN_INVALID;
	}
	if (!inventory->complete || inventory->status != CLUSTER_RECOVERY_NATIVE_INVENTORY_OBSERVED) {
		cluster_recovery_root_set_invalidate(roots);
		result = inventory_xact_fail(inventory,
									 inventory->xact_status > CLUSTER_RECOVERY_XACT_JOIN_DRAINED
										 ? inventory->xact_status
										 : CLUSTER_RECOVERY_XACT_JOIN_CHANGED);
	} else if (!cluster_recovery_root_set_digest(roots, digest)) {
		/* This can precede binding xact_roots (e.g. during a ROOT reread). */
		cluster_recovery_root_set_invalidate(roots);
		result = inventory_xact_fail(inventory, CLUSTER_RECOVERY_XACT_JOIN_CHANGED);
	} else if (inventory->xact_roots != NULL
			   && (inventory->xact_roots != roots || inventory->xact_budget != join_memory_budget
				   || memcmp(inventory->xact_digest, digest, sizeof(digest)) != 0)) {
		cluster_recovery_root_set_invalidate(roots);
		result = inventory_xact_fail(inventory, CLUSTER_RECOVERY_XACT_JOIN_CHANGED);
	} else {
		if (inventory->xact_roots == NULL) {
			const ClusterWalSourceRef *source = &inventory->sources[0].source;

			inventory->xact_roots = roots;
			inventory->xact_budget = join_memory_budget;
			inventory->xact_cursor = inventory->head;
			memcpy(inventory->xact_digest, digest, sizeof(digest));
			if (!cluster_recovery_root_set_storage_matches(
					roots, source->claim.identity.system_identifier,
					source->claim.database_incarnation, source->claim.identity.storage_uuid)) {
				result = inventory_xact_fail(inventory, CLUSTER_RECOVERY_XACT_JOIN_CHANGED);
				*diag = inventory->diag;
				return result;
			}
		}
		PG_TRY();
		{
			result = inventory_xact_step(inventory, out);
		}
		PG_CATCH();
		{
			inventory_xact_fail(inventory, CLUSTER_RECOVERY_XACT_JOIN_INVALID);
			memset(out, 0, sizeof(*out));
			*diag = inventory->diag;
			PG_RE_THROW();
		}
		PG_END_TRY();
	}
	*diag = inventory->diag;
	return result;
}

static bool
inventory_digest_number(pg_cryptohash_ctx *hash, uint64 value)
{
	uint8 bytes[8];
	for (unsigned i = 0; i < sizeof(bytes); ++i)
		bytes[i] = (uint8)(value >> (8 * i));
	return pg_cryptohash_update(hash, bytes, sizeof(bytes)) == 0;
}

static bool
inventory_digest_source(pg_cryptohash_ctx *hash, const NativeInventorySource *saved)
{
	const ClusterWalSourceRef *s = &saved->source;
	const ClusterControlRootIdentity *id = &s->claim.identity;
	const RfContributorStreamCutV1 *cut = &saved->cut;
	const ClusterWalTailObservation *tail = &saved->observed;

#define INVENTORY_HASH(value_) inventory_digest_number(hash, (uint64)(value_))
#define INVENTORY_BYTES(value_)                                                                    \
	(pg_cryptohash_update(hash, (const uint8 *)(value_), sizeof(value_)) == 0)
	return INVENTORY_HASH(id->system_identifier) && INVENTORY_BYTES(id->storage_uuid)
		   && INVENTORY_BYTES(id->authority_uuid) && INVENTORY_HASH(id->origin_thread_id)
		   && INVENTORY_HASH(id->reserved42) && INVENTORY_HASH((uint32)id->origin_node_id)
		   && INVENTORY_HASH(id->thread_claim_created_at) && INVENTORY_HASH(id->thread_claim_crc32c)
		   && INVENTORY_HASH(id->reserved60) && INVENTORY_HASH(id->origin_owner_incarnation)
		   && INVENTORY_HASH(id->root_lineage_seq) && INVENTORY_HASH(s->claim.database_incarnation)
		   && INVENTORY_HASH(s->claim.max_config_generation)
		   && INVENTORY_BYTES(s->claim.claim_sha256) && INVENTORY_HASH(s->timeline)
		   && INVENTORY_HASH(cut->failed_thread) && INVENTORY_HASH(cut->flags)
		   && INVENTORY_HASH(cut->timeline_id) && INVENTORY_HASH(cut->scan_begin_inclusive)
		   && INVENTORY_HASH(cut->scan_end_exclusive) && INVENTORY_HASH(cut->contributor_count)
		   && INVENTORY_HASH(cut->component_count) && INVENTORY_HASH(cut->origin_owner_incarnation)
		   && INVENTORY_HASH(tail->complete_end) && INVENTORY_HASH(tail->last_record_start)
		   && INVENTORY_HASH(tail->last_record_crc) && INVENTORY_HASH(tail->records)
		   && INVENTORY_HASH(tail->database_incarnation) && INVENTORY_BYTES(saved->transcript);
#undef INVENTORY_HASH
#undef INVENTORY_BYTES
}

bool
cluster_recovery_native_inventory_digest(const ClusterRecoveryNativeInventory *inventory,
										 uint8 out[32])
{
	static const uint8 domain[] = "PGRAC-E1-INPUT-OBSERVATION-V1";
	pg_cryptohash_ctx *volatile hash = NULL;
	volatile bool ok = false;
	uint8 digest[32];

	if (out != NULL)
		memset(out, 0, 32);
	if (out == NULL || inventory == NULL || !inventory->complete
		|| inventory->status != CLUSTER_RECOVERY_NATIVE_INVENTORY_OBSERVED
		|| inventory->sources == NULL || inventory->source_count == 0
		|| inventory->source_count > CLUSTER_WAL_INPUTS_MAX)
		return false;
	if (!inventory_pages_current(inventory))
		return false;
	PG_TRY();
	{
		hash = pg_cryptohash_create(PG_SHA256);
		ok = hash != NULL && pg_cryptohash_init(hash) == 0
			 && pg_cryptohash_update(hash, domain, sizeof(domain)) == 0
			 && inventory_digest_number(hash, inventory->source_count);
		for (uint32 i = 0; ok && i < inventory->source_count; ++i)
			ok = inventory_digest_number(hash, i)
				 && inventory_digest_source(hash, &inventory->sources[i]);
		ok = ok && pg_cryptohash_final(hash, digest, sizeof(digest)) == 0;
	}
	PG_FINALLY();
	{
		pg_cryptohash_free(hash);
	}
	PG_END_TRY();
	if (ok)
		memcpy(out, digest, sizeof(digest));
	return ok;
}

bool
cluster_recovery_native_inventory_source_count_v1(ClusterRecoveryNativeInventory *inventory,
	uint32 *out)
{
	uint32 pages;

	if (out != NULL) *out = 0;
	if (out == NULL || inventory == NULL || !inventory->complete
		|| inventory->status != CLUSTER_RECOVERY_NATIVE_INVENTORY_OBSERVED)
		return false;
	/* Reuse the original complete/snapshot gate, without filtering sources
	 * by PAGE presence or changing any native-effect cursor. */
	if (inventory->sources == NULL || inventory->source_count == 0
		|| inventory->source_count > CLUSTER_WAL_INPUTS_MAX
		|| inventory->source_count != inventory->diag.sources) {
		inventory->complete = false;
		(void)inventory_refuse(inventory, CLUSTER_RECOVERY_NATIVE_INVENTORY_CHANGED,
			RF_PAGE_PROOF_DETAIL_SOURCE_GAP);
		return false;
	}
	if (!cluster_recovery_native_inventory_page_count_v1(inventory, &pages))
		return false;
	*out = inventory->source_count;
	return true;
}

bool
cluster_recovery_native_inventory_source_at_v1(ClusterRecoveryNativeInventory *inventory,
	uint32 index, ClusterRecoveryWalSourceObservationV1 *out)
{
	const NativeInventorySource *saved;
	uint32 count;

	if (out != NULL) memset(out, 0, sizeof(*out));
	if (out == NULL || !cluster_recovery_native_inventory_source_count_v1(inventory, &count)
		|| index >= count)
		return false;
	saved = &inventory->sources[index];
	out->source = saved->source;
	out->cut = saved->cut;
	out->observed = saved->observed;
	memcpy(out->transcript_sha256, saved->transcript, sizeof(out->transcript_sha256));
	out->source_index = index;
	return true;
}

bool
cluster_recovery_native_inventory_page_count_v1(ClusterRecoveryNativeInventory *inventory,
	uint32 *out)
{
	if (out != NULL) *out = 0;
	if (out == NULL || inventory == NULL || !inventory->complete
		|| inventory->status != CLUSTER_RECOVERY_NATIVE_INVENTORY_OBSERVED)
		return false;
	PG_TRY();
	{
		if (!inventory_pages_current(inventory)) {
			inventory->complete = false;
			(void)inventory_refuse(inventory, CLUSTER_RECOVERY_NATIVE_INVENTORY_CHANGED,
				RF_PAGE_PROOF_DETAIL_SOURCE_GAP);
		}
	}
	PG_CATCH();
	{
		inventory->complete = false;
		(void)inventory_refuse(inventory, CLUSTER_RECOVERY_NATIVE_INVENTORY_INVALID,
			RF_PAGE_PROOF_DETAIL_SOURCE_GAP);
		PG_RE_THROW();
	}
	PG_END_TRY();
	if (!inventory->complete) return false;
	*out = inventory->page_count;
	return true;
}

bool
cluster_recovery_native_inventory_page_at_v1(ClusterRecoveryNativeInventory *inventory,
	uint32 index, ClusterRecoveryPageWalV1 *out)
{
	NativeInventoryPage *page;
	uint32 count;

	if (out != NULL) memset(out, 0, sizeof(*out));
	if (out == NULL || !cluster_recovery_native_inventory_page_count_v1(inventory, &count)
		|| index >= count)
		return false;
	page = inventory->pages;
	for (uint32 i = 0; i < index; i++) page = page->next;
	out->source = inventory->sources[page->source_index].source;
	out->cut = inventory->sources[page->source_index].cut;
	out->source_index = page->source_index; out->ordinal = page->ordinal;
	out->reader = &page->reader;
	return true;
}

void
cluster_recovery_native_inventory_destroy(ClusterRecoveryNativeInventory *inventory)
{
	NativeInventoryRecord *record;
	NativeInventoryPage *page;

	if (inventory == NULL)
		return;
	inventory_revoke_dependents(inventory);
	cluster_recovery_multixact_join_destroy(inventory->mx_join);
	cluster_recovery_xact_join_destroy(inventory->xact_join);
	pg_cryptohash_free(inventory->hash);
	page = inventory->pages;
	while (page != NULL) {
		NativeInventoryPage *next = page->next;
		free(page); page = next;
	}
	record = inventory->head;
	while (record != NULL) {
		NativeInventoryRecord *next = record->next;
		cluster_recovery_native_scan_destroy(record->scan);
		free(record);
		record = next;
	}
	free(inventory);
}

/* Local input custody only. The original Start, simultaneous IR/WALR and
 * whole ROOT remain the sole episode owner; this handle releases none. */
struct ClusterRecoveryRelmapHistory {
	ClusterRecoveryRelmapSourceV1 original;
	ClusterRecoveryNativeInventory *inventory;
	ClusterRelmapClogBaseHandleV1 *native_base;
	bool failed;
};

static bool
history_overlap(const void *a, size_t an, const void *b, size_t bn)
{
	uintptr_t x = (uintptr_t)a, y = (uintptr_t)b;
	return a != NULL && b != NULL && (x <= y ? y - x < an : x - y < bn);
}

static size_t
history_owned_bytes(const ClusterRecoveryRelmapHistory *history)
{
	return sizeof(*history) + (history->native_base == NULL ? 0
		: cluster_clog_relmap_base_custody_bytes_v1());
}

/* Copied history/base accessors must not clear borrowed records/payloads or
 * original custody when rejecting an aliased caller output. */
static bool
history_storage_overlap(const ClusterRecoveryRelmapHistory *history, const void *p, size_t n)
{
	const ClusterRecoveryNativeInventory *inventory;
	const NativeInventoryRecord *record;
	const NativeInventoryPage *page;

	if (history == NULL)
		return false;
	if (history_overlap(p, n, history, sizeof(*history))
		|| history_overlap(p, n, history->native_base,
			cluster_clog_relmap_base_custody_bytes_v1()))
		return true;
	inventory = history->inventory;
	if (inventory == NULL)
		return false;
	if (history_overlap(p, n, inventory, MAXALIGN(sizeof(*inventory))
		+ inventory->source_count * sizeof(*inventory->sources)))
		return true;
	for (record = inventory->head; record != NULL; record = record->next)
		if (history_overlap(p, n, record, sizeof(*record))
			|| history_overlap(p, n, record->scan, cluster_recovery_native_scan_bytes(record->scan)))
			return true;
	for (page = inventory->pages; page != NULL; page = page->next)
		if (history_overlap(p, n, page, page->bytes))
			return true;
	return false;
}

/* Canonical bytes include both full map slots, not only the queried owner. */
static bool
history_pending_same(const ClusterRecoveryRelmapHistory *history, const ClusterRelmapV2Image *pending)
{
	uint8 wire[CLUSTER_RELMAP_V2_FILE_SIZE];
	return pending->pending_generation != 0
		&& cluster_relmap_v2_encode(pending, wire, sizeof(wire)) == CLUSTER_RELMAP_V2_VALID
		&& memcmp(wire, history->inventory->relmap_wire, sizeof(wire)) == 0;
}

static void
history_fail(ClusterRecoveryRelmapHistory *history)
{
	history->failed = true;
	if (history->inventory != NULL) {
		history->inventory->complete = false;
		history->inventory->status = CLUSTER_RECOVERY_NATIVE_INVENTORY_CHANGED;
		inventory_revoke_dependents(history->inventory);
	}
	cluster_wal_inputs_relmap_source_fail_v1(&history->original);
}

/* Copy the complete source baseline, including empty and non-PAGE streams.
 * Validate all original selected generations; a source ordinal is not the
 * selector index and a native-map prefix is not the full observed cut. */
static bool
history_source_observation(ClusterRecoveryRelmapHistory *history, uint32 ordinal,
	ClusterRecoveryWalSourceObservationV1 *out)
{
	ClusterRecoveryNativeInventory *inventory = history->inventory;

	if (!inventory_history_inputs(inventory, history->original.scope, false))
		return false;
	return cluster_recovery_native_inventory_source_at_v1(inventory, ordinal, out);
}

ClusterRecoveryNativeInventoryResult
cluster_recovery_relmap_history_build_v1(const ClusterRecoveryRelmapSourceV1 *source,
	size_t memory_budget, ClusterRecoveryRelmapHistory **out,
	ClusterRecoveryNativeInventoryDiag *diag)
{
	ClusterRecoveryRelmapSourceV1 original;
	ClusterRecoveryRelmapHistory *volatile history = NULL;
	volatile ClusterRecoveryNativeInventoryResult result = CLUSTER_RECOVERY_NATIVE_INVENTORY_INVALID;
	volatile bool entered = false;
	size_t native_bytes = cluster_clog_relmap_base_custody_bytes_v1();
	size_t reserved;

	if (out == NULL || diag == NULL || source == NULL
		|| history_overlap(out, sizeof(*out), source, sizeof(*source))
		|| history_overlap(diag, sizeof(*diag), source, sizeof(*source))
		|| history_overlap(out, sizeof(*out), diag, sizeof(*diag)))
		return result;
	*out = NULL;
	memset(diag, 0, sizeof(*diag));
	memcpy(&original, source, sizeof(original));
	if (CritSectionCount != 0 || cluster_cf_held(ShareLock) || cluster_cf_held(ExclusiveLock))
		return result;
	if (native_bytes > SIZE_MAX - sizeof(*history))
		return CLUSTER_RECOVERY_NATIVE_INVENTORY_CAPACITY;
	reserved = sizeof(*history) + native_bytes;
	if (memory_budget < reserved)
		return CLUSTER_RECOVERY_NATIVE_INVENTORY_CAPACITY;
	PG_TRY();
	{
		/* Even the first native all-holder check may raise ERROR. Keep the
		 * copied outside locator available to revoke the original episode. */
		if (cluster_wal_inputs_relmap_source_current_v1(&original)) {
			entered = true;
			history = calloc(1, sizeof(*history));
			if (history == NULL)
				result = CLUSTER_RECOVERY_NATIVE_INVENTORY_CAPACITY;
			else {
				memcpy(&history->original, &original, sizeof(original));
				result = inventory_build(original.scope, NULL, memory_budget - reserved,
					&history->inventory, diag, &history->original);
				diag->owned_bytes += history_owned_bytes(history);
				if (result == CLUSTER_RECOVERY_NATIVE_INVENTORY_OBSERVED
					&& (!cluster_wal_inputs_relmap_source_current_v1(&original)
						|| history->inventory->source_count != original.input_count
						|| memcmp(source, &original, sizeof(original)) != 0))
					result = CLUSTER_RECOVERY_NATIVE_INVENTORY_CHANGED;
			}
		}
	}
	PG_CATCH();
	{
		cluster_wal_inputs_relmap_source_fail_v1(&original);
		cluster_recovery_relmap_history_destroy_v1((ClusterRecoveryRelmapHistory **)&history);
		PG_RE_THROW();
	}
	PG_END_TRY();
	if (result != CLUSTER_RECOVERY_NATIVE_INVENTORY_OBSERVED) {
		if (entered)
			cluster_wal_inputs_relmap_source_fail_v1(&original);
		cluster_recovery_relmap_history_destroy_v1((ClusterRecoveryRelmapHistory **)&history);
		return result;
	}
	*out = history;
	return result;
}

bool
cluster_recovery_relmap_history_source_v1(ClusterRecoveryRelmapHistory *history, uint32 ordinal,
	ClusterRecoveryRelmapHistoryEntryV1 *out)
{
	ClusterRecoveryRelmapHistoryEntryV1 entry = {0};
	const NativeInventorySource *saved;
	volatile bool ok = false;

	if (out == NULL || history_storage_overlap(history, out, sizeof(*out)))
		return false;
	memset(out, 0, sizeof(*out));
	if (history == NULL || history->failed || history->inventory == NULL
		|| ordinal >= history->inventory->source_count || CritSectionCount != 0
		|| cluster_cf_held(ShareLock) || cluster_cf_held(ExclusiveLock))
		return false;
	saved = &history->inventory->sources[ordinal];
	PG_TRY();
	{
		ok = history->inventory->complete
			&& history->inventory->status == CLUSTER_RECOVERY_NATIVE_INVENTORY_OBSERVED
			&& cluster_wal_inputs_relmap_source_current_v1(&history->original);
		if (ok) {
			entry.source = cluster_wal_inputs_relmap_source_v1(history->original.scope,
				saved->history.input_index);
			ok = cluster_wal_inputs_relmap_source_current_v1(&entry.source)
				&& memcmp(&entry.source.selected, &saved->history.input,
					sizeof(saved->history.input)) == 0
				&& inventory_source_equal(&saved->source, &entry.source.selected.source)
				&& history_source_observation(history, ordinal, &entry.observation);
		}
		if (ok) {
			entry.checkpoint_end = saved->history.checkpoint_end;
			entry.window = saved->history.window;
			entry.post_flush_issued = saved->history.post_flush_issued;
			entry.checkpoint_observed = saved->history.checkpoint_observed;
			entry.window_observed = saved->history.window_observed;
			ok = cluster_wal_inputs_relmap_source_current_v1(&history->original)
				&& cluster_wal_inputs_relmap_source_current_v1(&entry.source)
				&& inventory_history_inputs(history->inventory, history->original.scope, false);
		}
	}
	PG_CATCH();
	{
		history_fail(history);
		PG_RE_THROW();
	}
	PG_END_TRY();
	if (!ok)
		history_fail(history);
	else
		memcpy(out, &entry, sizeof(entry));
	return ok;
}

static bool
history_pending_base_read(ClusterRecoveryRelmapHistory *history,
	ClusterRecoveryRelmapPendingBaseV1 *out)
{
	ClusterRecoveryNativeInventory *inventory = history->inventory;
	const ClusterRelmapV2Image *query = &inventory->relmap_query;
	const NativeInventorySource *saved;
	const NativeInventoryHistory *selected;
	const ClusterRelmapCheckpointWindow *window;
	uint64 xid = query->pending_owner.top_full_xid;

	if (!inventory->complete || inventory->status != CLUSTER_RECOVERY_NATIVE_INVENTORY_OBSERVED
		|| !inventory->relmap_drained || inventory->relmap_active
		|| inventory->relmap_summary.wal_matches != 1
		|| inventory->relmap_summary.query_window_sources != 1
		|| inventory->relmap_summary.owner_window_sources != 1
		|| inventory->relmap_wal_source >= inventory->source_count
		|| !inventory_pages_current(inventory)
		|| !inventory_history_inputs(inventory, history->original.scope, false))
		return false;
	saved = &inventory->sources[inventory->relmap_wal_source];
	selected = &saved->history;
	window = &selected->window;
	if (!selected->checkpoint_observed || !selected->window_observed
		|| window->origin != query->pending_owner.node
		|| window->epoch != query->pending_owner.membership_epoch
		|| window->admitted_incarnation != query->pending_owner.admitted_incarnation
		|| window->boot_incarnation != query->pending_owner.boot_incarnation
		|| xid < window->native_floor || xid > selected->post_flush_issued
		|| selected->post_flush_issued - window->native_floor >= UINT64_C(0x80000000))
		return false;
	out->history.source = cluster_wal_inputs_relmap_source_v1(history->original.scope,
		selected->input_index);
	if (!cluster_wal_inputs_relmap_source_current_v1(&out->history.source)
		|| memcmp(&out->history.source.selected, &selected->input, sizeof(selected->input)) != 0
		|| !inventory_source_equal(&saved->source, &out->history.source.selected.source)
		|| !cluster_clog_relmap_base_read_v1(&out->history.source, xid, &out->native,
			&history->native_base)
		|| history->native_base == NULL
		|| !cluster_wal_inputs_relmap_source_current_v1(&out->history.source)
		|| !inventory_pages_current(inventory)
		|| !history_source_observation(history, inventory->relmap_wal_source,
			&out->history.observation))
		return false;
	out->history.checkpoint_end = selected->checkpoint_end;
	out->history.window = selected->window;
	out->history.post_flush_issued = selected->post_flush_issued;
	out->history.checkpoint_observed = selected->checkpoint_observed;
	out->history.window_observed = selected->window_observed;
	out->fold = inventory->relmap_summary;
	return true;
}

ClusterRecoveryNativeInventoryResult
cluster_recovery_relmap_history_recheck_v1(ClusterRecoveryRelmapHistory *history,
	ClusterRecoveryNativeInventoryDiag *diag)
{
	volatile ClusterRecoveryNativeInventoryResult result = CLUSTER_RECOVERY_NATIVE_INVENTORY_CHANGED;

	if (diag == NULL || history_overlap(diag, sizeof(*diag), history, sizeof(*history)))
		return CLUSTER_RECOVERY_NATIVE_INVENTORY_INVALID;
	memset(diag, 0, sizeof(*diag));
	if (history == NULL || history->failed || history->inventory == NULL)
		return result;
	if (CritSectionCount != 0 || cluster_cf_held(ShareLock) || cluster_cf_held(ExclusiveLock))
		return CLUSTER_RECOVERY_NATIVE_INVENTORY_INVALID;
	/* Reentry or ERROR must never expose the old completed observation. */
	history->failed = true;
	PG_TRY();
	{
		if (cluster_wal_inputs_relmap_source_current_v1(&history->original)) {
			result = cluster_recovery_native_inventory_recheck(history->original.scope,
				history->inventory, diag);
			if (result == CLUSTER_RECOVERY_NATIVE_INVENTORY_OBSERVED && history->native_base != NULL) {
				ClusterRecoveryRelmapPendingBaseV1 base = {0};
				if (!history_pending_base_read(history, &base))
					result = CLUSTER_RECOVERY_NATIVE_INVENTORY_CHANGED;
			}
			if (result == CLUSTER_RECOVERY_NATIVE_INVENTORY_OBSERVED
				&& !cluster_wal_inputs_relmap_source_current_v1(&history->original))
				result = CLUSTER_RECOVERY_NATIVE_INVENTORY_CHANGED;
		}
	}
	PG_CATCH();
	{
		history_fail(history);
		PG_RE_THROW();
	}
	PG_END_TRY();
	diag->owned_bytes += history_owned_bytes(history);
	if (result == CLUSTER_RECOVERY_NATIVE_INVENTORY_OBSERVED)
		history->failed = false;
	else
		history_fail(history);
	return result;
}

ClusterRecoveryRelmapFoldResult
cluster_recovery_relmap_history_pending_step_v1(ClusterRecoveryRelmapHistory *history,
	const ClusterRelmapV2Image *pending, ClusterRecoveryRelmapFoldEntry *out,
	ClusterRecoveryRelmapFoldSummary *summary, ClusterRecoveryNativeInventoryDiag *diag)
{
	ClusterRecoveryRelmapFoldEntry entry = {0};
	ClusterRecoveryRelmapFoldSummary counts = {0};
	volatile ClusterRecoveryRelmapFoldResult result = CLUSTER_RECOVERY_RELMAP_FOLD_CHANGED;
	volatile bool current = false;

	/* Check aliases before clearing anything: the query and original custody
	 * must survive an ordinary caller precondition refusal unchanged. */
	if (out == NULL || summary == NULL || diag == NULL || pending == NULL
		|| history_overlap(out, sizeof(*out), pending, sizeof(*pending))
		|| history_overlap(summary, sizeof(*summary), pending, sizeof(*pending))
		|| history_overlap(diag, sizeof(*diag), pending, sizeof(*pending))
		|| history_overlap(out, sizeof(*out), summary, sizeof(*summary))
		|| history_overlap(out, sizeof(*out), diag, sizeof(*diag))
		|| history_overlap(summary, sizeof(*summary), diag, sizeof(*diag))
		|| history_overlap(out, sizeof(*out), history, sizeof(*history))
		|| history_overlap(summary, sizeof(*summary), history, sizeof(*history))
		|| history_overlap(diag, sizeof(*diag), history, sizeof(*history)))
		return CLUSTER_RECOVERY_RELMAP_FOLD_INVALID;
	memset(out, 0, sizeof(*out));
	memset(summary, 0, sizeof(*summary));
	memset(diag, 0, sizeof(*diag));
	if (history == NULL || history->inventory == NULL)
		return result;
	*diag = history->inventory->diag;
	diag->owned_bytes += history_owned_bytes(history);
	if (history->failed)
		return result;
	if (CritSectionCount != 0 || cluster_cf_held(ShareLock) || cluster_cf_held(ExclusiveLock))
		return CLUSTER_RECOVERY_RELMAP_FOLD_INVALID;

	/* Latch before even the first original all-holder check, which can ERROR.
	 * Never publish a partial tick or make failed observations current again. */
	history->failed = true;
	PG_TRY();
	{
		if (inventory_history_inputs(history->inventory, history->original.scope, false)) {
			if (history->native_base != NULL) {
				if (history->inventory->relmap_drained && history_pending_same(history, pending)) {
					counts = history->inventory->relmap_summary;
					result = CLUSTER_RECOVERY_RELMAP_FOLD_DRAINED;
				}
			} else
				result = cluster_recovery_native_inventory_relmap_step(history->inventory, pending,
					&entry, &counts, NULL);
			current = result <= CLUSTER_RECOVERY_RELMAP_FOLD_DRAINED;
			if (current && result == CLUSTER_RECOVERY_RELMAP_FOLD_ENTRY) {
				const NativeInventorySource *saved;
				current = entry.source_index < history->inventory->source_count;
				if (current) {
					saved = &history->inventory->sources[entry.source_index];
					current = entry.record != NULL && entry.history == &saved->history
						&& inventory_source_equal(&entry.record->source, &saved->source);
				}
			}
			if (current)
				current = inventory_history_inputs(history->inventory, history->original.scope,
					false);
			if (!current && result <= CLUSTER_RECOVERY_RELMAP_FOLD_DRAINED)
				result = CLUSTER_RECOVERY_RELMAP_FOLD_CHANGED;
		}
	}
	PG_CATCH();
	{
		history_fail(history);
		PG_RE_THROW();
	}
	PG_END_TRY();

	*diag = history->inventory->diag;
	diag->owned_bytes += history_owned_bytes(history);
	if (current) {
		history->failed = false;
		*out = entry;
		*summary = counts;
	} else
		history_fail(history);
	return result;
}

bool
cluster_recovery_relmap_history_pending_base_v1(ClusterRecoveryRelmapHistory *history,
	const ClusterRelmapV2Image *pending, ClusterRecoveryRelmapPendingBaseV1 *out,
	ClusterRecoveryNativeInventoryDiag *diag)
{
	ClusterRecoveryRelmapPendingBaseV1 base = {0};
	volatile bool ok = false;

	if (pending == NULL || out == NULL || diag == NULL
		|| history_overlap(out, sizeof(*out), pending, sizeof(*pending))
		|| history_overlap(diag, sizeof(*diag), pending, sizeof(*pending))
		|| history_overlap(out, sizeof(*out), diag, sizeof(*diag))
		|| history_storage_overlap(history, out, sizeof(*out))
		|| history_storage_overlap(history, diag, sizeof(*diag)))
		return false;
	memset(out, 0, sizeof(*out));
	memset(diag, 0, sizeof(*diag));
	if (history == NULL || history->inventory == NULL)
		return false;
	*diag = history->inventory->diag;
	diag->owned_bytes += history_owned_bytes(history);
	if (history->failed || CritSectionCount != 0
		|| cluster_cf_held(ShareLock) || cluster_cf_held(ExclusiveLock)
		|| !history->inventory->relmap_drained)
		return false;

	/* Latch before any original owner check or native read can raise ERROR.
	 * The native handle, if acquired, survives late refusal for local destroy. */
	history->failed = true;
	PG_TRY();
	{
		ok = history_pending_same(history, pending) && history_pending_base_read(history, &base);
	}
	PG_CATCH();
	{
		history_fail(history);
		PG_RE_THROW();
	}
	PG_END_TRY();
	*diag = history->inventory->diag;
	diag->owned_bytes += history_owned_bytes(history);
	if (ok) {
		history->failed = false;
		memcpy(out, &base, sizeof(base));
	} else
		history_fail(history);
	return ok;
}

/* Select only represented, unambiguous native carriers. Even this complete
 * negative screen cannot prove an earlier page birth or a FullXid era. */
static const NativeInventoryRecord *
history_pending_terminal_record(const ClusterRecoveryNativeInventory *inventory)
{
	const ClusterRecoveryRelmapFoldSummary *summary = &inventory->relmap_summary;
	const ClusterRelmapV2Image *query = &inventory->relmap_query;
	TransactionId raw = (TransactionId)query->pending_owner.top_full_xid;

	if (!inventory->complete || inventory->status != CLUSTER_RECOVERY_NATIVE_INVENTORY_OBSERVED
		|| !inventory->relmap_drained || inventory->relmap_active || inventory->relmap_terminal != NULL
		|| summary->wal_matches != 1 || summary->wal_aliases != 0
		|| summary->child_references != 0 || summary->assignments != 0 || summary->prepares != 0
		|| summary->clog_after_wal != 0 || summary->clog_unordered != 0
		|| !((summary->top_commits == 1 && summary->top_aborts == 0)
			|| (summary->top_commits == 0 && summary->top_aborts == 1)))
		return NULL;
	for (const NativeInventoryRecord *saved = inventory->head; saved != NULL; saved = saved->next) {
		ClusterRecoveryNativeEffect effect;
		const ClusterRecoveryNativeRecord *record;

		if (cluster_recovery_native_scan_primary(saved->scan, &effect) != CLUSTER_RECOVERY_NATIVE_EFFECT)
			return NULL;
		record = effect.record;
		if (effect.kind != CLUSTER_RECOVERY_NATIVE_TERMINAL || record == NULL
			|| record->source.claim.identity.origin_node_id != query->pending_owner.node
			|| record->top_xid != raw)
			continue;
		return saved->source_index == inventory->relmap_wal_source
			&& saved->source_index < inventory->source_count
			&& record->read_lsn >= query->pending_owner.wal_end
			&& (!TransactionIdIsValid(record->toplevel_xid) || record->toplevel_xid == raw)
			&& inventory_source_equal(&record->source, &inventory->sources[saved->source_index].source)
			? saved : NULL;
	}
	return NULL;
}

static bool history_pending_xid_begin(ClusterRecoveryRelmapHistory *,
	const ClusterRelmapV2Image *, NativeInventoryRelmapXid *, ClusterRecoveryRelmapPendingXidV1 *);
static bool history_pending_xid_finish(ClusterRecoveryRelmapHistory *,
	const ClusterRelmapV2Image *, NativeInventoryRelmapXid *, ClusterRecoveryRelmapPendingXidV1 *);
static bool history_pending_birth_match(const ClusterRecoveryRelmapPendingXidV1 *,
	const ClusterRelmapV2Image *, uint32 *);

/* Bind and re-observe every original physical generation independently.
 * Indexes below are physical inventory indexes, resolved through the saved
 * selector index. No cross-generation LSN order or lifetime is inferred. */
static bool
history_pending_clog_side_counts(ClusterRecoveryRelmapHistory *history,
	NativeInventoryRelmapXid *xid, bool finish)
{
	ClusterRecoveryNativeInventory *inventory = history->inventory;
	uint32 total = 0;

	if (xid == NULL || !xid->side_clog)
		return true;
	if (inventory->source_count == 0 || inventory->source_count > CLUSTER_WAL_INPUTS_MAX)
		return false;
	for (uint32 i = 0; i < inventory->source_count; i++) {
		const NativeInventorySource *saved = &inventory->sources[i];
		ClusterRecoveryRelmapSourceV1 source = cluster_wal_inputs_relmap_source_v1(
			history->original.scope, saved->history.input_index);
		uint32 count;

		if (!inventory_source_equal(&saved->source, &source.selected.source)
			|| memcmp(&saved->history.input, &source.selected, sizeof(source.selected)) != 0)
			return false;
		count = cluster_wal_inputs_relmap_clog_count_v1(&source);
		if (count == UINT32_MAX)
			return false;
		if (xid->all_native != NULL) {
			/* Bound the COMPLETE selected set before the first native read,
			 * not only the query origin or a prefix of the physical scan. */
			if (count > CLUSTER_RECOVERY_RELMAP_CLOG_HISTORY_MAX - total)
				return false;
			total += count;
		}
		if (finish) {
			if (count != xid->clog_expected[i] || xid->clog_matched[i] != count)
				return false;
		} else
			xid->clog_expected[i] = count;
	}
	return (xid->all_native == NULL || !finish || xid->all_native->clog_count == total)
		&& inventory_history_inputs(inventory, history->original.scope, false);
}

/* The original bounds already order every counter between checkpoint and
 * tail and keep that interval below half a raw cycle. The exact FULL page
 * first can precede checkpoint and query; include it in that same interval.
 * This necessary consistency check grants no native era or lifetime. */
static bool
history_pending_birth_halfspace(const ClusterRecoveryRelmapPendingBirthV1 *birth)
{
	const ClusterRecoveryRelmapPendingXidV1 *inputs = &birth->inputs;
	uint64 first;

	if (birth->birth_index >= inputs->clog_count
		|| birth->birth_index >= CLUSTER_RECOVERY_RELMAP_CLOG_HISTORY_MAX)
		return false;
	first = inputs->clog[birth->birth_index].full_birth.first_full_xid;
	return first <= inputs->tail_next
		&& inputs->tail_next - first < UINT64_C(0x80000000);
}

static bool
history_pending_terminal_read(ClusterRecoveryRelmapHistory *history,
	const ClusterRelmapV2Image *pending, ClusterRecoveryRelmapPendingTerminalV1 *out,
	ClusterRecoveryNativeInventoryDiag *diag, ClusterRecoveryRelmapPendingBirthV1 *birth,
	NativeInventoryRelmapXid *xid)
{
	ClusterRecoveryRelmapPendingTerminalV1 result = {0};
	NativeInventoryRelmapTerminal terminal = {0};
	const NativeInventoryRecord *saved;
	ClusterRecoveryNativeEffect effect;
	bool held = false;
	volatile bool ok = false;

	if (pending == NULL || out == NULL || diag == NULL
		|| history_overlap(out, sizeof(*out), pending, sizeof(*pending))
		|| history_overlap(diag, sizeof(*diag), pending, sizeof(*pending))
		|| history_overlap(out, sizeof(*out), diag, sizeof(*diag))
		|| history_storage_overlap(history, out, sizeof(*out))
		|| history_storage_overlap(history, diag, sizeof(*diag)))
		return false;
	memset(out, 0, sizeof(*out));
	memset(diag, 0, sizeof(*diag));
	if (history == NULL || history->inventory == NULL)
		return false;
	*diag = history->inventory->diag;
	diag->owned_bytes += history_owned_bytes(history);
	if (history->failed || CritSectionCount != 0
		|| cluster_cf_held(ShareLock) || cluster_cf_held(ExclusiveLock)
		|| !history->inventory->relmap_drained)
		return false;
	ForEachLWLockHeldByMe(inventory_page_lock_held, &held);
	if (held)
		return false;

	/* Latch before the first original owner/plan/native operation can ERROR.
	 * Custody survives late failure; only explicit local destroy closes it. */
	history->failed = true;
	PG_TRY();
	{
		saved = history_pending_same(history, pending)
			? history_pending_terminal_record(history->inventory) : NULL;
		ok = saved != NULL
			&& (birth == NULL || history_pending_xid_begin(history, pending, xid, &birth->inputs))
			&& history_pending_clog_side_counts(history, xid, false)
			&& history_pending_base_read(history, &result.base);
		if (ok) {
			ok = cluster_recovery_native_scan_primary(saved->scan, &effect)
				== CLUSTER_RECOVERY_NATIVE_EFFECT
				&& result.base.native.status == (history->inventory->relmap_summary.top_commits == 1
					? TRANSACTION_STATUS_COMMITTED : TRANSACTION_STATUS_ABORTED);
		}
		if (ok) {
			terminal.source = result.base.history.source;
			terminal.source_index = saved->source_index;
			terminal.expected = cluster_wal_inputs_relmap_terminal_count_v1(&terminal.source);
			ok = terminal.expected != 0 && terminal.expected != UINT32_MAX;
		}
		if (ok) {
			history->inventory->relmap_terminal = &terminal;
			if (xid != NULL)
				history->inventory->relmap_xid = xid;
			ok = cluster_recovery_native_inventory_recheck(history->original.scope,
				history->inventory, diag) == CLUSTER_RECOVERY_NATIVE_INVENTORY_OBSERVED;
			history->inventory->relmap_terminal = NULL;
			if (xid != NULL)
				history->inventory->relmap_xid = NULL;
			ok = ok && terminal.matched == terminal.expected
				&& cluster_wal_inputs_relmap_terminal_count_v1(&terminal.source) == terminal.expected
				&& history_pending_clog_side_counts(history, xid, true)
				&& history_pending_base_read(history, &result.base)
				&& cluster_wal_inputs_relmap_terminal_count_v1(&terminal.source) == terminal.expected
				&& history_pending_clog_side_counts(history, xid, true)
				&& inventory_history_inputs(history->inventory, history->original.scope, false);
		}
		if (ok && birth != NULL) {
			ok = history_pending_xid_finish(history, pending, xid, &birth->inputs)
				&& history_pending_birth_match(&birth->inputs, pending, &birth->birth_index)
				&& (!xid->side_clog || history_pending_birth_halfspace(birth))
				&& memcmp(&birth->inputs.history, &result.base.history,
					sizeof(result.base.history)) == 0
				&& saved->source_index == birth->inputs.history.observation.source_index
				&& saved->ordinal > birth->inputs.map_ordinal
				&& effect.record->read_lsn >= birth->inputs.map_end;
		}
		if (ok) {
			result.terminal_start = effect.record->read_lsn;
			result.terminal_end = effect.record->end_lsn;
			result.terminal_source_index = saved->source_index;
			result.terminal_ordinal = saved->ordinal;
		}
	}
	PG_CATCH();
	{
		history->inventory->relmap_terminal = NULL;
		if (xid != NULL)
			history->inventory->relmap_xid = NULL;
		history_fail(history);
		PG_RE_THROW();
	}
	PG_END_TRY();
	*diag = history->inventory->diag;
	diag->owned_bytes += history_owned_bytes(history);
	if (ok) {
		history->failed = false;
		memcpy(out, &result, sizeof(result));
	} else
		history_fail(history);
	return ok;
}

bool
cluster_recovery_relmap_history_pending_terminal_v1(ClusterRecoveryRelmapHistory *history,
	const ClusterRelmapV2Image *pending, ClusterRecoveryRelmapPendingTerminalV1 *out,
	ClusterRecoveryNativeInventoryDiag *diag)
{
	return history_pending_terminal_read(history, pending, out, diag, NULL, NULL);
}

/* Full counters only: never manufacture an era by attaching a current epoch
 * to a raw WAL xid. This is a necessary half-space consistency test on the
 * original counter candidate, not proof that a raw carrier belongs to it. */
static bool
history_pending_xid_bounds(uint64 query, uint64 checkpoint, uint64 before, uint64 after, uint64 tail)
{
	return TransactionIdIsNormal((TransactionId)query)
		&& TransactionIdIsNormal((TransactionId)checkpoint)
		&& TransactionIdIsNormal((TransactionId)before)
		&& TransactionIdIsNormal((TransactionId)after)
		&& TransactionIdIsNormal((TransactionId)tail)
		&& checkpoint <= before && before <= after && after <= tail
		&& query < after && after - query < UINT64_C(0x80000000)
		&& tail - query < UINT64_C(0x80000000)
		&& tail - checkpoint < UINT64_C(0x80000000);
}

/* Private read while the enclosing call has already latched history.failed.
 * Keep the public getter's full source/PAGE/metadata checks without clearing
 * that failure latch or temporarily making reentry available. */
static bool
history_pending_xid_source(ClusterRecoveryRelmapHistory *history, uint32 index,
	ClusterRecoveryRelmapHistoryEntryV1 *out)
{
	ClusterRecoveryNativeInventory *inventory = history->inventory;
	const NativeInventorySource *saved = &inventory->sources[index];

	if (!inventory->complete || inventory->status != CLUSTER_RECOVERY_NATIVE_INVENTORY_OBSERVED
		|| !cluster_wal_inputs_relmap_source_current_v1(&history->original))
		return false;
	out->source = cluster_wal_inputs_relmap_source_v1(history->original.scope,
		saved->history.input_index);
	if (!cluster_wal_inputs_relmap_source_current_v1(&out->source)
		|| memcmp(&out->source.selected, &saved->history.input, sizeof(saved->history.input)) != 0
		|| !inventory_source_equal(&saved->source, &out->source.selected.source)
		|| !history_source_observation(history, index, &out->observation))
		return false;
	out->checkpoint_end = saved->history.checkpoint_end;
	out->window = saved->history.window;
	out->post_flush_issued = saved->history.post_flush_issued;
	out->checkpoint_observed = saved->history.checkpoint_observed;
	out->window_observed = saved->history.window_observed;
	return cluster_wal_inputs_relmap_source_current_v1(&history->original)
		&& cluster_wal_inputs_relmap_source_current_v1(&out->source)
		&& inventory_history_inputs(inventory, history->original.scope, false);
}

/* Bind every original physical generation before the one full reread.
 * No order/continuity is inferred between distinct claims or timelines.
 * Explicit initializer terminals remain visible without an invented seed. */
static bool
history_pending_xid_generations(ClusterRecoveryRelmapHistory *history,
	NativeInventoryRelmapXid *xid, ClusterRecoveryRelmapPendingXidV1 *result, bool finish)
{
	ClusterRecoveryNativeInventory *inventory = history->inventory;

	if (inventory->source_count == 0 || inventory->source_count > CLUSTER_WAL_INPUTS_MAX
		|| (finish && result->generation_count != inventory->source_count))
		return false;
	for (uint32 i = 0; i < inventory->source_count; ++i) {
		ClusterRecoveryRelmapHistoryEntryV1 entry = {0};
		ClusterRecoveryRelmapXidGenerationV1 *generation = &result->generations[i];
		ClusterColdXidProgressV1 *progress = i == xid->source_index
			? &xid->progress : &xid->generations[i];
		ClusterColdParticipantV1 participant = {0};
		const NativeInventorySource *saved = &inventory->sources[i];

		if (!history_pending_xid_source(history, i, &entry))
			return false;
		if (finish) {
			if (memcmp(&generation->observation, &entry.observation, sizeof(entry.observation)) != 0
				|| generation->input_index != entry.source.input_index)
				return false;
			if (!progress->begun)
				continue; /* no checkpoint: no candidate or era */
			if (!cluster_cold_xid_candidate_v1(progress, &progress->source, &generation->tail_next)
				|| generation->tail_next < generation->checkpoint_next
				|| generation->tail_next - generation->checkpoint_next >= UINT64_C(0x80000000))
				return false;
			generation->records = progress->records;
			generation->counter_observed = true;
			continue;
		}
		generation->observation = entry.observation;
		generation->input_index = entry.source.input_index;
		if (entry.source.selected.kind == CLUSTER_WAL_INPUT_INITIALIZER_TERMINAL)
			continue;
		if (entry.source.selected.kind != CLUSTER_WAL_INPUT_CHECKPOINT || !entry.checkpoint_observed)
			return false;
		generation->native_redo = saved->history.input.native_checkpoint.redo;
		generation->checkpoint_next = U64FromFullTransactionId(saved->history.input.native_checkpoint.nextXid);
		participant.thread_id = saved->cut.failed_thread;
		participant.timeline = saved->cut.timeline_id;
		participant.owner_incarnation = saved->cut.origin_owner_incarnation;
		participant.physical_lower = saved->cut.scan_begin_inclusive;
		participant.native_redo = generation->native_redo;
		participant.tail_end = saved->cut.scan_end_exclusive;
		if (!cluster_cold_xid_begin_v1(progress, &participant,
			generation->checkpoint_next, saved->native_redo_records))
			return false;
		if (i == xid->source_index)
			xid->source = participant;
	}
	result->generation_count = inventory->source_count;
	return true;
}

static bool
history_pending_xid_begin(ClusterRecoveryRelmapHistory *history,
	const ClusterRelmapV2Image *pending, NativeInventoryRelmapXid *xid,
	ClusterRecoveryRelmapPendingXidV1 *result)
{
	ClusterRecoveryNativeInventory *inventory = history->inventory;
	bool ok;

	uint32 index = inventory->relmap_wal_source;
	ok = history_pending_same(history, pending) && !inventory->relmap_active
		&& inventory->relmap_xid == NULL && inventory->relmap_terminal == NULL
		&& inventory->relmap_summary.wal_matches == 1
		&& inventory->relmap_summary.wal_aliases == 0
		&& inventory->relmap_summary.owner_window_sources == 1
		&& inventory->relmap_summary.query_window_sources == 1
		&& index < inventory->source_count
		&& history_pending_xid_source(history, index, &result->history)
		&& result->history.checkpoint_observed && result->history.window_observed;
	if (ok) {
		const NativeInventorySource *saved = &inventory->sources[index];
		const ClusterRelmapCheckpointWindow *window = &result->history.window;
		uint64 query = pending->pending_owner.top_full_xid;
		result->checkpoint_next = U64FromFullTransactionId(
			saved->history.input.native_checkpoint.nextXid);
		xid->source_index = index;
		xid->result = result;
		ok = window->origin == pending->pending_owner.node
			&& window->epoch == pending->pending_owner.membership_epoch
			&& window->admitted_incarnation == pending->pending_owner.admitted_incarnation
			&& window->boot_incarnation == pending->pending_owner.boot_incarnation
			&& query >= window->native_floor && query <= result->history.post_flush_issued
			&& result->history.post_flush_issued - window->native_floor < UINT64_C(0x80000000)
			&& history_pending_xid_generations(history, xid, result, false);
	}
	return ok;
}

static bool
history_pending_xid_finish(ClusterRecoveryRelmapHistory *history,
	const ClusterRelmapV2Image *pending, NativeInventoryRelmapXid *xid,
	ClusterRecoveryRelmapPendingXidV1 *result)
{
	ClusterRecoveryNativeInventory *inventory = history->inventory;
	bool ok;

	ok = history_pending_xid_generations(history, xid, result, true)
		&& xid->map_end == pending->pending_owner.wal_end
		&& xid->map_start < xid->map_end
		&& cluster_cold_xid_candidate_v1(&xid->progress, &xid->source, &result->tail_next)
		&& history_pending_xid_bounds(pending->pending_owner.top_full_xid,
			result->checkpoint_next, xid->before_map, xid->after_map, result->tail_next)
		&& history_pending_xid_source(history, xid->source_index,
			&result->history)
		&& inventory_history_inputs(inventory, history->original.scope, false);
	if (ok) {
		result->before_map = xid->before_map;
		result->after_map = xid->after_map;
		result->records = xid->progress.records;
		result->map_ordinal = xid->map_ordinal;
		result->map_start = xid->map_start;
		result->map_end = xid->map_end;
	}
	return ok;
}

/* This correspondence is deliberately narrower than native lifetime proof.
 * Never order distinct physical generations, infer an era from raw ZERO, or
 * treat a matching page birth as an issued/terminal transaction. */
static bool
history_pending_birth_match(const ClusterRecoveryRelmapPendingXidV1 *inputs,
	const ClusterRelmapV2Image *pending, uint32 *birth_index)
{
	uint64 query = pending->pending_owner.top_full_xid;
	uint32 raw = (uint32)query, page = raw / CLUSTER_NATIVE_CLOG_PER_PAGE;
	uint64 first = query - raw + (page == 0 ? FirstNormalTransactionId
		: page * (uint64)CLUSTER_NATIVE_CLOG_PER_PAGE);
	uint32 source = inputs->history.observation.source_index;
	uint32 found = UINT32_MAX, origins = 0;
	const ClusterRecoveryRelmapClogEventV1 *birth;

	if (inputs->generation_count == 0 || inputs->generation_count > CLUSTER_WAL_INPUTS_MAX
		|| source >= inputs->generation_count
		|| inputs->clog_count > CLUSTER_RECOVERY_RELMAP_CLOG_HISTORY_MAX)
		return false;
	for (uint32 i = 0; i < inputs->generation_count; i++) {
		const ClusterRecoveryRelmapXidGenerationV1 *generation = &inputs->generations[i];
		if (generation->observation.source.claim.identity.origin_node_id != pending->pending_owner.node)
			continue;
		/* Other generations of this physical namespace have no proven order. */
		if (i != source || !generation->counter_observed
			|| memcmp(&generation->observation, &inputs->history.observation,
				sizeof(generation->observation)) != 0)
			return false;
		origins++;
	}
	if (origins != 1)
		return false;
	for (uint32 i = 0; i < inputs->clog_count; i++) {
		const ClusterRecoveryRelmapClogEventV1 *event = &inputs->clog[i];
		if (event->source_index != source)
			return false;
		if (event->kind == CLUSTER_RECOVERY_NATIVE_CLOG_ZERO && event->page == page
			&& event->full_birth_observed) {
			if (found != UINT32_MAX)
				return false;
			found = i;
		}
	}
	if (found == UINT32_MAX)
		return false;
	birth = &inputs->clog[found];
	if (birth->full_birth.first_full_xid != first || birth->full_birth.reserved_zero != 0
		|| birth->full_birth.pageno != page || !birth->counter_observed
		|| birth->read_lsn < inputs->generations[source].native_redo
		|| birth->read_lsn >= birth->end_lsn || birth->end_lsn > inputs->map_start
		|| birth->record_ordinal >= inputs->map_ordinal
		|| birth->counter_before < inputs->checkpoint_next
		|| birth->counter_before > birth->counter_after || birth->counter_after > inputs->before_map)
		return false;
	for (uint32 i = 0; i < inputs->clog_count; i++) {
		const ClusterRecoveryRelmapClogEventV1 *event = &inputs->clog[i];
		if (i == found)
			continue;
		/* A raw truncate has no independently ordered FullXid horizon. Keep
		 * it unresolved unless it is wholly before the observed new birth. */
		if (event->kind == CLUSTER_RECOVERY_NATIVE_CLOG_TRUNCATE
			|| (event->kind == CLUSTER_RECOVERY_NATIVE_CLOG_ZERO && event->page == page)) {
			if (event->record_ordinal >= birth->record_ordinal || event->end_lsn > birth->read_lsn)
				return false;
		}
	}
	*birth_index = found;
	return true;
}

static bool
history_pending_xid_read(ClusterRecoveryRelmapHistory *history,
	const ClusterRelmapV2Image *pending, ClusterRecoveryRelmapPendingXidV1 *out,
	ClusterRecoveryNativeInventoryDiag *diag, uint32 *birth_index)
{
	ClusterRecoveryRelmapPendingXidV1 result = {0};
	NativeInventoryRelmapXid xid = {0};
	ClusterRecoveryNativeInventory *inventory;
	bool held = false;
	volatile bool ok = false;

	if (pending == NULL || out == NULL || diag == NULL
		|| history_overlap(out, sizeof(*out), pending, sizeof(*pending))
		|| history_overlap(diag, sizeof(*diag), pending, sizeof(*pending))
		|| history_overlap(out, sizeof(*out), diag, sizeof(*diag))
		|| history_storage_overlap(history, out, sizeof(*out))
		|| history_storage_overlap(history, diag, sizeof(*diag)))
		return false;
	memset(out, 0, sizeof(*out));
	memset(diag, 0, sizeof(*diag));
	if (history == NULL || history->inventory == NULL)
		return false;
	inventory = history->inventory;
	*diag = inventory->diag;
	diag->owned_bytes += history_owned_bytes(history);
	if (history->failed || CritSectionCount != 0 || !inventory->relmap_drained
		|| cluster_cf_held(ShareLock) || cluster_cf_held(ExclusiveLock))
		return false;
	ForEachLWLockHeldByMe(inventory_page_lock_held, &held);
	if (held)
		return false;

	history->failed = true;
	PG_TRY();
	{
		ok = history_pending_xid_begin(history, pending, &xid, &result);
		if (ok) {
			inventory->relmap_xid = &xid;
			ok = cluster_recovery_native_inventory_recheck(history->original.scope,
				inventory, diag) == CLUSTER_RECOVERY_NATIVE_INVENTORY_OBSERVED;
			inventory->relmap_xid = NULL;
			ok = ok && history_pending_xid_finish(history, pending, &xid, &result);
		}
		if (ok && birth_index != NULL)
			ok = history_pending_birth_match(&result, pending, birth_index);
	}
	PG_CATCH();
	{
		inventory->relmap_xid = NULL;
		history_fail(history);
		PG_RE_THROW();
	}
	PG_END_TRY();
	*diag = inventory->diag;
	diag->owned_bytes += history_owned_bytes(history);
	if (ok) {
		history->failed = false;
		memcpy(out, &result, sizeof(result));
	} else
		history_fail(history);
	return ok;
}

bool
cluster_recovery_relmap_history_pending_xid_v1(ClusterRecoveryRelmapHistory *history,
	const ClusterRelmapV2Image *pending, ClusterRecoveryRelmapPendingXidV1 *out,
	ClusterRecoveryNativeInventoryDiag *diag)
{
	return history_pending_xid_read(history, pending, out, diag, NULL);
}

bool
cluster_recovery_relmap_history_pending_birth_v1(ClusterRecoveryRelmapHistory *history,
	const ClusterRelmapV2Image *pending, ClusterRecoveryRelmapPendingBirthV1 *out,
	ClusterRecoveryNativeInventoryDiag *diag)
{
	uint32 birth_index;

	if (pending == NULL || out == NULL || diag == NULL
		|| history_overlap(out, sizeof(*out), pending, sizeof(*pending))
		|| history_overlap(diag, sizeof(*diag), pending, sizeof(*pending))
		|| history_overlap(out, sizeof(*out), diag, sizeof(*diag))
		|| history_storage_overlap(history, out, sizeof(*out))
		|| history_storage_overlap(history, diag, sizeof(*diag)))
		return false;
	memset(out, 0, sizeof(*out));
	if (!history_pending_xid_read(history, pending, &out->inputs, diag, &birth_index))
		return false;
	out->birth_index = birth_index;
	return true;
}

static bool
history_pending_native_read(ClusterRecoveryRelmapHistory *history,
	const ClusterRelmapV2Image *pending, ClusterRecoveryRelmapPendingNativeV1 *out,
	ClusterRecoveryNativeInventoryDiag *diag, bool side_clog,
	ClusterRecoveryRelmapPendingAllNativeV1 *all_native)
{
	ClusterRecoveryRelmapPendingNativeV1 result = {0};
	NativeInventoryRelmapXid xid = {0};

	xid.side_clog = side_clog;
	xid.all_native = all_native;
	if (pending == NULL || out == NULL || diag == NULL
		|| history_overlap(out, sizeof(*out), pending, sizeof(*pending))
		|| history_overlap(diag, sizeof(*diag), pending, sizeof(*pending))
		|| history_overlap(out, sizeof(*out), diag, sizeof(*diag))
		|| history_storage_overlap(history, out, sizeof(*out))
		|| history_storage_overlap(history, diag, sizeof(*diag)))
		return false;
	memset(out, 0, sizeof(*out));
	if (!history_pending_terminal_read(history, pending, &result.terminal, diag,
		&result.birth, &xid))
		return false;
	memcpy(out, &result, sizeof(result));
	return true;
}

bool
cluster_recovery_relmap_history_pending_native_v1(ClusterRecoveryRelmapHistory *history,
	const ClusterRelmapV2Image *pending, ClusterRecoveryRelmapPendingNativeV1 *out,
	ClusterRecoveryNativeInventoryDiag *diag)
{
	return history_pending_native_read(history, pending, out, diag, false, NULL);
}

bool
cluster_recovery_relmap_history_pending_native_side_v1(ClusterRecoveryRelmapHistory *history,
	const ClusterRelmapV2Image *pending, ClusterRecoveryRelmapPendingNativeV1 *out,
	ClusterRecoveryNativeInventoryDiag *diag)
{
	return history_pending_native_read(history, pending, out, diag, true, NULL);
}

bool
cluster_recovery_relmap_history_pending_all_native_v1(ClusterRecoveryRelmapHistory *history,
	const ClusterRelmapV2Image *pending, ClusterRecoveryRelmapPendingAllNativeV1 *out,
	ClusterRecoveryNativeInventoryDiag *diag)
{
	ClusterRecoveryRelmapPendingAllNativeV1 result = {0};

	if (pending == NULL || out == NULL || diag == NULL
		|| history_overlap(out, sizeof(*out), pending, sizeof(*pending))
		|| history_overlap(diag, sizeof(*diag), pending, sizeof(*pending))
		|| history_overlap(out, sizeof(*out), diag, sizeof(*diag))
		|| history_storage_overlap(history, out, sizeof(*out))
		|| history_storage_overlap(history, diag, sizeof(*diag)))
		return false;
	memset(out, 0, sizeof(*out));
	if (!history_pending_native_read(history, pending, &result.query, diag, true, &result))
		return false;
	memcpy(out, &result, sizeof(result));
	return true;
}

void
cluster_recovery_relmap_history_destroy_v1(ClusterRecoveryRelmapHistory **custody)
{
	ClusterRecoveryRelmapHistory *history;

	if (custody == NULL || *custody == NULL)
		return;
	history = *custody;
	*custody = NULL;
	cluster_clog_relmap_base_release_v1(&history->native_base);
	cluster_recovery_native_inventory_destroy(history->inventory);
	free(history);
}
#endif
