/*-------------------------------------------------------------------------
 *
 * cluster_savepoint_reader.c
 *   Pure dual-identity validation for retained operation records.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *   src/backend/cluster/cluster_savepoint_reader.c
 *
 * NOTES
 *   Pgrac-original validation; original readers retain acquisition authority.
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "cluster/cluster_savepoint_reader.h"
#include "cluster/cluster_r4_operation.h"

#ifdef USE_PGRAC_CLUSTER

/* Pure validation only. The original landing owns transport, current-order,
 * retention, capability and exact dependency/worker qualification. */
bool
cluster_r4_peer_operation_extract(const ClusterR4OperationContextWire *request_wire,
								  const ClusterR4PeerOperationWire *proof_wire,
								  const ClusterTxLocator *locator, uint64 admission_generation,
								  const char page[BLCKSZ], char record_out[BLCKSZ],
								  size_t *length_out, ClusterTxLocator *canonical_out)
{
	ClusterR4OperationRequest request;
	ClusterR4PeerOperationProof peer;
	ClusterSavepointReaderIdentity identity = { 0 };
	ClusterSavepointReaderRecord decoded;
	ClusterTxLocator exact;
	if (length_out != NULL)
		*length_out = 0;
	if (canonical_out != NULL)
		memset(canonical_out, 0, sizeof(*canonical_out));
	if (locator == NULL || page == NULL || record_out == NULL || length_out == NULL
		|| canonical_out == NULL || !ClusterR4OperationContextDecode(request_wire, &request)
		|| !ClusterR4PeerOperationDecode(proof_wire, &peer)
		|| peer.admission_generation != admission_generation
		|| !cluster_undo_record_uba_equal(peer.uba, locator->uba)
		|| peer.identity.canonical.xid != locator->xid
		|| (locator->tt_wrap != TT_WRAP_INVALID
			&& locator->tt_wrap != peer.identity.canonical.wrap))
		return false;
	exact = *locator;
	exact.tt_wrap = peer.identity.canonical.wrap;
	if (!(request.retained
			  ? cluster_savepoint_reader_discover_retained_head(
					&request.identity, &request.head, &request.tag, &exact, page, &identity)
			  : cluster_savepoint_reader_discover_head(&request.head, &request.tag, &exact, page,
													   &identity))
		|| memcmp(&identity, &peer.identity, sizeof(identity)) != 0
		|| !cluster_savepoint_reader_extract_identity(&identity, &exact, page, record_out,
													  length_out, &decoded)
		|| (identity.operation_sequence != 0
				? (peer.record_format != 2 || peer.effect != decoded.operation.effect_state)
				: (peer.record_format != 1 || peer.effect != 0))) {
		*length_out = 0;
		return false;
	}
	*canonical_out = exact;
	return true;
}

static bool
savepoint_reader_binding_valid(const ClusterSavepointOperation *expected,
							   const ClusterTxLocator *locator)
{
	const ClusterCanonicalTxnBinding *top = &expected->top;

	return TransactionIdIsNormal(top->xid)
		   && top->xid == XidFromFullTransactionId(expected->top_fxid) && top->segment_id > 0
		   && top->segment_id <= PG_UINT16_MAX && top->segment_generation != PG_UINT32_MAX
		   && top->slot_offset < TT_SLOTS_PER_SEGMENT && top->slot_wrap != TT_WRAP_INVALID
		   && top->origin_instance > 0 && top->origin_instance <= UNDO_OWNER_INSTANCE_MAX
		   && (top->segment_id - 1) / CLUSTER_UNDO_SEGS_PER_INSTANCE == top->origin_instance - 1
		   && top->publish_state == CLUSTER_CANONICAL_TXN_PUBLISHED && top->reserved16 == 0
		   && !XLogRecPtrIsInvalid(top->active_lsn) && locator->xid == top->xid
		   && locator->tt_wrap == top->slot_wrap;
}

static bool
savepoint_reader_expected_valid(const ClusterSavepointOperation *expected,
								const ClusterTxLocator *locator)
{
	return savepoint_reader_binding_valid(expected, locator)
		   && TransactionIdIsNormal(XidFromFullTransactionId(expected->operation_fxid))
		   && U64FromFullTransactionId(expected->operation_fxid)
				  > U64FromFullTransactionId(expected->top_fxid)
		   && expected->operation_sequence != 0 && expected->command_id != InvalidCommandId
		   && expected->subid > TopSubTransactionId;
}

bool
cluster_savepoint_reader_identity_from_operation(const ClusterSavepointOperation *expected,
												 const ClusterTxLocator *locator,
												 ClusterSavepointReaderIdentity *out)
{
	ClusterSavepointReaderIdentity identity = { 0 };

	if (out == NULL)
		return false;
	memset(out, 0, sizeof(*out));
	if (expected == NULL || locator == NULL || !savepoint_reader_expected_valid(expected, locator))
		return false;
	identity.canonical = (ClusterTTSlotPhysicalLocator){ .segment_id = expected->top.segment_id,
														 .xid = expected->top.xid,
														 .slot_offset = expected->top.slot_offset,
														 .wrap = expected->top.slot_wrap };
	identity.canonical_generation = expected->top.segment_generation;
	identity.top_fxid = expected->top_fxid;
	identity.operation_fxid = expected->operation_fxid;
	identity.operation_sequence = expected->operation_sequence;
	identity.command_id = expected->command_id;
	identity.subid = expected->subid;
	*out = identity;
	return true;
}

static bool
savepoint_reader_identity_valid(const ClusterSavepointReaderIdentity *expected,
								const ClusterTxLocator *locator)
{
	const ClusterTTSlotPhysicalLocator *top = &expected->canonical;

	return TransactionIdIsNormal(top->xid)
		   && top->xid == XidFromFullTransactionId(expected->top_fxid) && top->segment_id > 0
		   && top->segment_id <= PG_UINT16_MAX
		   && (top->segment_id - 1) / CLUSTER_UNDO_SEGS_PER_INSTANCE < UNDO_OWNER_INSTANCE_MAX
		   && expected->canonical_generation != PG_UINT32_MAX
		   && top->slot_offset < TT_SLOTS_PER_SEGMENT && top->wrap <= TT_WRAP_MAX
		   && locator->xid == top->xid && locator->tt_wrap == top->wrap
		   && TransactionIdIsNormal(XidFromFullTransactionId(expected->operation_fxid))
		   && ((U64FromFullTransactionId(expected->operation_fxid)
					> U64FromFullTransactionId(expected->top_fxid)
				&& expected->operation_sequence != 0 && expected->command_id != InvalidCommandId
				&& expected->subid > TopSubTransactionId)
			   || (U64FromFullTransactionId(expected->operation_fxid)
					   == U64FromFullTransactionId(expected->top_fxid)
				   && expected->operation_sequence == 0 && expected->command_id == InvalidCommandId
				   && expected->subid == TopSubTransactionId));
}

/* A transaction predecessor need not be this page's ITL prior. Retain the
 * original owner/slot envelope and same-segment append order nevertheless. */
static bool
savepoint_reader_prev_address_valid(UBA current_uba, const UndoRecordHeader *record)
{
	uint32 current_segment, current_block, previous_segment, previous_block;
	uint16 current_tt, current_row, previous_tt, previous_row;

	if (UBA_is_invalid(record->prev_uba))
		return true;
	if (!cluster_undo_rollback_address_valid(record, record->prev_uba, false)
		|| !uba_decode_record(current_uba, &current_segment, &current_block, &current_tt,
							  &current_row)
		|| !uba_decode_record(record->prev_uba, &previous_segment, &previous_block, &previous_tt,
							  &previous_row))
		return false;
	return previous_segment != current_segment || previous_block < current_block
		   || (previous_block == current_block && previous_row < current_row);
}

bool
cluster_savepoint_reader_decode_identity(const ClusterSavepointReaderIdentity *expected,
										 const ClusterTxLocator *locator, UBA current_uba,
										 const UndoRecordHeader *record, const char *body,
										 size_t payload_length, ClusterSavepointReaderRecord *out)
{
	ClusterSavepointReaderRecord decoded = { 0 };
	UndoRecordHeader canonical;
	uint32 segment;
	uint32 block;
	uint16 tt_offset;
	uint16 row;

	if (out == NULL)
		return false;
	memset(out, 0, sizeof(*out));
	if (expected == NULL || locator == NULL || record == NULL || body == NULL
		|| !savepoint_reader_identity_valid(expected, locator)
		|| !uba_decode_record(current_uba, &segment, &block, &tt_offset, &row)
		|| tt_offset != expected->canonical.slot_offset
		|| record->tt_slot_segment_id != expected->canonical.segment_id
		|| record->origin_node_id
			   != (expected->canonical.segment_id - 1) / CLUSTER_UNDO_SEGS_PER_INSTANCE
		|| record->xid != XidFromFullTransactionId(expected->operation_fxid)
		|| cluster_undo_record_uba_equal(record->prev_uba, current_uba)
		|| (expected->operation_sequence == 0
				? (!(record->flags & UNDO_REC_FLAG_HAS_ITL_HISTORY)
				   || !cluster_undo_record_decode_payload(record, body, payload_length,
														  &decoded.body_length, &decoded.history))
				: !cluster_undo_record_decode_operation_payload(
					  record, body, payload_length, &decoded.body_length, &decoded.history,
					  &decoded.operation)))
		return false;
	if (expected->operation_sequence != 0
		&& (decoded.operation.canonical_top_fxid != U64FromFullTransactionId(expected->top_fxid)
			|| decoded.operation.operation_fxid
				   != U64FromFullTransactionId(expected->operation_fxid)
			|| decoded.operation.canonical_segment_generation != expected->canonical_generation
			|| decoded.operation.operation_sequence != expected->operation_sequence
			|| decoded.operation.command_id != expected->command_id
			|| decoded.operation.subid != expected->subid))
		return false;

	/* The old physical key validator remains top-only. Project only its
	 * private header input; tuple and operation validation keep the raw child. */
	canonical = *record;
	canonical.xid = expected->canonical.xid;
	if (!cluster_undo_record_validate_identity(locator, current_uba, &canonical, NULL)
		|| !savepoint_reader_prev_address_valid(current_uba, record))
		return false;
	*out = decoded;
	return true;
}

/* Locate bytes without interpreting an operation or granting authority. */
static bool
savepoint_reader_record(const ClusterTxLocator *locator, const char page[BLCKSZ],
						UndoRecordHeader *record_out, UndoSlotDirEntry *slot_out)
{
	UndoBlockHeader header;
	UndoSlotDirEntry slot;
	uint32 segment, block;
	uint16 tt_offset, row;
	uint32 directory;

	if (locator == NULL || page == NULL
		|| !uba_decode_record(locator->uba, &segment, &block, &tt_offset, &row))
		return false;
	memcpy(&header, page, sizeof(header));
	if (header.magic != PGRAC_UNDO_BLOCK_MAGIC || header.block_version != UNDO_BLOCK_VERSION_1
		|| header.slot_count == 0 || header.slot_count > (BLCKSZ - sizeof(header)) / sizeof(slot)
		|| row >= header.slot_count)
		return false;
	directory = BLCKSZ - (uint32)header.slot_count * sizeof(slot);
	if (header.free_offset < sizeof(header) || header.free_offset > directory)
		return false;
	memcpy(&slot, page + BLCKSZ - ((uint32)row + 1) * sizeof(slot), sizeof(slot));
	if (slot.record_offset < sizeof(header) || slot.record_length < sizeof(*record_out)
		|| slot.record_offset > header.free_offset
		|| slot.record_length > header.free_offset - slot.record_offset)
		return false;
	memcpy(record_out, page + slot.record_offset, sizeof(*record_out));
	if (slot.record_type != record_out->record_type || slot.flags != record_out->flags
		|| slot.record_length != sizeof(*record_out) + (size_t)record_out->payload_length)
		return false;
	*slot_out = slot;
	return true;
}

bool
cluster_savepoint_reader_extract_identity(const ClusterSavepointReaderIdentity *expected,
										  const ClusterTxLocator *locator, const char page[BLCKSZ],
										  char record_out[BLCKSZ], size_t *record_length_out,
										  ClusterSavepointReaderRecord *out)
{
	UndoRecordHeader record;
	UndoSlotDirEntry slot;
	ClusterSavepointReaderRecord decoded;

	if (record_length_out != NULL)
		*record_length_out = 0;
	if (out != NULL)
		memset(out, 0, sizeof(*out));
	if (expected == NULL || record_out == NULL || record_length_out == NULL || out == NULL
		|| !savepoint_reader_record(locator, page, &record, &slot)
		|| !cluster_savepoint_reader_decode_identity(expected, locator, locator->uba, &record,
													 page + slot.record_offset + sizeof(record),
													 record.payload_length, &decoded))
		return false;
	memmove(record_out, page + slot.record_offset, slot.record_length);
	*record_length_out = slot.record_length;
	*out = decoded;
	return true;
}

bool
cluster_savepoint_reader_decode(const ClusterSavepointOperation *expected,
								const ClusterTxLocator *locator, UBA current_uba,
								const UndoRecordHeader *record, const char *body,
								size_t payload_length, ClusterSavepointReaderRecord *out)
{
	ClusterSavepointReaderIdentity identity;

	if (out != NULL)
		memset(out, 0, sizeof(*out));
	return cluster_savepoint_reader_identity_from_operation(expected, locator, &identity)
		   && cluster_savepoint_reader_decode_identity(&identity, locator, current_uba, record,
													   body, payload_length, out);
}

bool
cluster_savepoint_reader_extract(const ClusterSavepointOperation *expected,
								 const ClusterTxLocator *locator, const char page[BLCKSZ],
								 char record_out[BLCKSZ], size_t *record_length_out,
								 ClusterSavepointReaderRecord *out)
{
	ClusterSavepointReaderIdentity identity;

	if (record_length_out != NULL)
		*record_length_out = 0;
	if (out != NULL)
		memset(out, 0, sizeof(*out));
	return cluster_savepoint_reader_identity_from_operation(expected, locator, &identity)
		   && cluster_savepoint_reader_extract_identity(&identity, locator, page, record_out,
														record_length_out, out);
}

/* Independently match the immutable page-slot incarnation and addressed head;
 * the tail is not allowed to choose the page owner or its after-image. */
static bool
savepoint_reader_head_matches(const ClusterItlSlotData *head, const BufferTag *tag,
							  const ClusterTxLocator *locator, const UndoItlHistoryTrailer *history)
{
	unsigned i;

	if (head->xid != locator->xid
		|| !cluster_undo_record_uba_equal(head->undo_segment_head, locator->uba))
		return false;
	for (i = 0; i < history->count; i++) {
		const UndoItlHistoryEntry *entry = &history->entries[i];
		uint16 wrap = entry->prior.wrap;
		bool role = entry->after_kind == ITL_FLAG_ACTIVE
						? head->flags >= ITL_FLAG_ACTIVE && head->flags <= ITL_FLAG_NEEDS_CLEANOUT
						: ITL_FLAG_IS_LOCK_ONLY(head->flags);
		bool request_role = entry->after_kind == ITL_FLAG_ACTIVE
								? locator->itl_kind >= ITL_FLAG_ACTIVE
									  && locator->itl_kind <= ITL_FLAG_NEEDS_CLEANOUT
								: ITL_FLAG_IS_LOCK_ONLY(locator->itl_kind);

		if (entry->block != tag->blockNum)
			continue;
		if (entry->prior.flags != ITL_FLAG_FREE
			&& !(entry->prior.flags == entry->after_kind && entry->prior.xid == head->xid))
			wrap++;
		return role && request_role && entry->slot_index == locator->itl_slot_index
			   && head->wrap == wrap && head->write_scn == entry->after_write_scn;
	}
	return false;
}

/* Produce a challenge from protected bytes, not a live-backend binding.
 * Author: SqlRush <sqlrush@gmail.com> */
bool
cluster_savepoint_reader_discover_head(const ClusterItlSlotData *head, const BufferTag *tag,
									   const ClusterTxLocator *locator, const char page[BLCKSZ],
									   ClusterSavepointReaderIdentity *out)
{
	ClusterSavepointReaderIdentity identity = { 0 };
	ClusterSavepointReaderRecord decoded;
	ClusterTxLocator canonical;
	UndoRecordHeader record;
	UndoSlotDirEntry slot;
	uint32 segment, block;
	uint16 tt, row;
	const char *body;

	if (out == NULL)
		return false;
	memset(out, 0, sizeof(*out));
	if (head == NULL || tag == NULL || locator == NULL
		|| !savepoint_reader_record(locator, page, &record, &slot))
		return false;
	body = page + slot.record_offset + sizeof(record);
	if (record.target_locator.spcOid != tag->spcOid || record.target_locator.dbOid != tag->dbOid
		|| record.target_locator.relNumber != tag->relNumber || record.target_fork != tag->forkNum
		|| !uba_decode_record(locator->uba, &segment, &block, &tt, &row)
		|| !cluster_undo_record_decode_operation_payload(&record, body, record.payload_length,
														 &decoded.body_length, &decoded.history,
														 &decoded.operation)
		|| !savepoint_reader_head_matches(head, tag, locator, &decoded.history))
		return false;
	identity.canonical
		= (ClusterTTSlotPhysicalLocator){ .segment_id = record.tt_slot_segment_id,
										  .xid = head->xid,
										  .slot_offset = tt,
										  .wrap = (uint16)(record.tt_wrap_plus1 - 1) };
	identity.canonical_generation = decoded.operation.canonical_segment_generation;
	identity.top_fxid = FullTransactionIdFromU64(decoded.operation.canonical_top_fxid);
	identity.operation_fxid = FullTransactionIdFromU64(decoded.operation.operation_fxid);
	identity.operation_sequence = decoded.operation.operation_sequence;
	identity.command_id = decoded.operation.command_id;
	identity.subid = decoded.operation.subid;
	canonical = *locator;
	if (canonical.tt_wrap == TT_WRAP_INVALID)
		canonical.tt_wrap = identity.canonical.wrap;
	if (!cluster_savepoint_reader_decode_identity(&identity, &canonical, locator->uba, &record,
												  body, record.payload_length, &decoded))
		return false;
	*out = identity;
	return true;
}

/* A retained qualified operation supplies only the compare identity missing
 * from V1. The original reader must still sample TT and reacquire DATA. */
bool
cluster_savepoint_reader_discover_retained_head(const ClusterSavepointReaderIdentity *owner,
												const ClusterItlSlotData *head,
												const BufferTag *tag,
												const ClusterTxLocator *locator,
												const char page[BLCKSZ],
												ClusterSavepointReaderIdentity *out)
{
	ClusterSavepointReaderIdentity identity;
	ClusterSavepointReaderRecord decoded;
	ClusterTxLocator exact;
	UndoRecordHeader record;
	UndoSlotDirEntry slot;

	if (out == NULL)
		return false;
	memset(out, 0, sizeof(*out));
	if (owner == NULL || head == NULL || tag == NULL || locator == NULL
		|| owner->operation_sequence != 0
		|| !savepoint_reader_record(locator, page, &record, &slot))
		return false;
	exact = *locator;
	if (exact.tt_wrap == TT_WRAP_INVALID)
		exact.tt_wrap = owner->canonical.wrap;
	if (!savepoint_reader_identity_valid(owner, &exact))
		return false;
	if (record.flags & UNDO_REC_FLAG_HAS_OPERATION) {
		if (!cluster_savepoint_reader_discover_head(head, tag, &exact, page, &identity)
			|| identity.canonical_generation != owner->canonical_generation
			|| !FullTransactionIdEquals(identity.top_fxid, owner->top_fxid)
			|| identity.canonical.segment_id != owner->canonical.segment_id
			|| identity.canonical.xid != owner->canonical.xid
			|| identity.canonical.slot_offset != owner->canonical.slot_offset
			|| identity.canonical.wrap != owner->canonical.wrap)
			return false;
	} else {
		identity = *owner;
		if (record.target_locator.spcOid != tag->spcOid || record.target_locator.dbOid != tag->dbOid
			|| record.target_locator.relNumber != tag->relNumber
			|| record.target_fork != tag->forkNum
			|| !cluster_savepoint_reader_decode_identity(&identity, &exact, locator->uba, &record,
														 page + slot.record_offset + sizeof(record),
														 record.payload_length, &decoded)
			|| !savepoint_reader_head_matches(head, tag, &exact, &decoded.history))
			return false;
	}
	*out = identity;
	return true;
}

/* Own-history callers must additionally prove the original publication.
 * Historical holders cannot fabricate that live-backend receipt. */
bool
cluster_savepoint_reader_discover_own_head(const ClusterSavepointOperation *owner,
										   const ClusterItlSlotData *head, const BufferTag *tag,
										   const ClusterTxLocator *locator, const char page[BLCKSZ],
										   ClusterSavepointReaderIdentity *out)
{
	ClusterSavepointReaderIdentity identity = { 0 };
	ClusterTxLocator exact;

	if (out == NULL)
		return false;
	memset(out, 0, sizeof(*out));
	if (owner == NULL || locator == NULL
		|| !FullTransactionIdEquals(owner->operation_fxid, owner->top_fxid)
		|| owner->operation_sequence != 0 || owner->command_id != InvalidCommandId
		|| owner->subid != TopSubTransactionId)
		return false;
	exact = *locator;
	if (exact.tt_wrap == TT_WRAP_INVALID)
		exact.tt_wrap = owner->top.slot_wrap;
	if (!savepoint_reader_binding_valid(owner, &exact))
		return false;
	identity.canonical = (ClusterTTSlotPhysicalLocator){ .segment_id = owner->top.segment_id,
														 .xid = owner->top.xid,
														 .slot_offset = owner->top.slot_offset,
														 .wrap = owner->top.slot_wrap };
	identity.canonical_generation = owner->top.segment_generation;
	identity.top_fxid = identity.operation_fxid = owner->top_fxid;
	identity.command_id = InvalidCommandId;
	identity.subid = TopSubTransactionId;
	return cluster_savepoint_reader_discover_retained_head(&identity, head, tag, &exact, page, out);
}

#endif
