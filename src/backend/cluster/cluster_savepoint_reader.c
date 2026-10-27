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

#ifdef USE_PGRAC_CLUSTER

static bool
savepoint_reader_expected_valid(const ClusterSavepointOperation *expected,
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
		   && locator->tt_wrap == top->slot_wrap
		   && TransactionIdIsNormal(XidFromFullTransactionId(expected->operation_fxid))
		   && U64FromFullTransactionId(expected->operation_fxid)
				  > U64FromFullTransactionId(expected->top_fxid)
		   && expected->operation_sequence != 0 && expected->command_id != InvalidCommandId
		   && expected->subid > TopSubTransactionId;
}

bool
cluster_savepoint_reader_decode(const ClusterSavepointOperation *expected,
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
		|| !savepoint_reader_expected_valid(expected, locator)
		|| !uba_decode_record(current_uba, &segment, &block, &tt_offset, &row)
		|| tt_offset != expected->top.slot_offset
		|| record->tt_slot_segment_id != expected->top.segment_id
		|| record->origin_node_id != expected->top.origin_instance - 1
		|| record->xid != XidFromFullTransactionId(expected->operation_fxid)
		|| cluster_undo_record_uba_equal(record->prev_uba, current_uba)
		|| !cluster_undo_record_decode_operation_payload(record, body, payload_length,
														 &decoded.body_length, &decoded.history,
														 &decoded.operation))
		return false;
	if (decoded.operation.canonical_top_fxid != U64FromFullTransactionId(expected->top_fxid)
		|| decoded.operation.operation_fxid != U64FromFullTransactionId(expected->operation_fxid)
		|| decoded.operation.canonical_segment_generation != expected->top.segment_generation
		|| decoded.operation.operation_sequence != expected->operation_sequence
		|| decoded.operation.command_id != expected->command_id
		|| decoded.operation.subid != expected->subid)
		return false;

	/* The old physical key validator remains top-only. Project only its
	 * private header input; tuple and operation validation keep the raw child. */
	canonical = *record;
	canonical.xid = expected->top.xid;
	if (!cluster_undo_record_validate_identity(locator, current_uba, &canonical, NULL))
		return false;
	*out = decoded;
	return true;
}

#endif
