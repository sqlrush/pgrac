/*-------------------------------------------------------------------------
 *
 * cluster_cold_recovery_tt_internal.h
 *    Shared immutable cold TT layout readers.
 *
 * Portions Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *    src/backend/cluster/cluster_cold_recovery_tt_internal.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef CLUSTER_COLD_RECOVERY_TT_INTERNAL_H
#define CLUSTER_COLD_RECOVERY_TT_INTERNAL_H

#include "cluster/cluster_cold_recovery.h"
#include "cluster/cluster_undo_segment_init.h"

/* Raw capture may retain damaged bytes for reconstruction. Typed readers
 * must first validate the complete immutable inventory, before interpreting
 * the version-dependent slots or generation. Legacy raw-slot rules remain. */
static bool
cold_tt_header_readable(const UndoSegmentHeaderData *header, uint8 owner, uint32 segment)
{
	const UndoSegmentHeaderDataV2 *v2 = (const UndoSegmentHeaderDataV2 *)header;

	if (cluster_undo_segment_header_integrity(header) == CLUSTER_UNDO_HEADER_CORRUPT)
		return false;
	if (UndoSegmentHeader_identity_matches((const char *)header, segment, owner))
		return true;
	if (!UndoSegmentHeaderV2_identity_matches((const char *)header, BLCKSZ, segment, owner)
		|| (v2->pd_flags & (~PD_VALID_FLAG_BITS | PD_HAS_ITL | PD_SPACE_METADATA)) != 0
		|| v2->pd_lower != SizeOfPageHeaderData || v2->pd_upper != BLCKSZ
		|| v2->pd_special != BLCKSZ || (v2->segment_flags & ~UNDO_SEGMENT_FLAG_FULL) != 0)
		return false;
	for (uint32 i = 0; i < TT_SLOTS_PER_SEGMENT; i++) {
		const TTSlotV2 *slot = &v2->tt_slots[i];

		if (slot->status > TT_SLOT_RECYCLABLE || (slot->flags & ~TT_SLOT_FLAGS_KNOWN) != 0
			|| slot->wrap == TT_WRAP_INVALID || slot->commit_scn == UINT64_MAX)
			return false;
		if (slot->status == TT_SLOT_UNUSED) {
			if (slot->original_full_xid != 0)
				return false;
		} else if (!TransactionIdIsNormal(slot->xid) || slot->original_full_xid == UINT64_MAX
			|| (TransactionId)slot->original_full_xid != slot->xid)
			return false;
	}
	return true;
}

/* The caller has validated this whole immutable header and slot bound. */
static uint32
cold_tt_header_generation(const UndoSegmentHeaderData *header)
{
	const UndoSegmentHeaderDataV2 *v2 = (const UndoSegmentHeaderDataV2 *)header;

	return v2->tt_layout_version == CLUSTER_UNDO_TT_LAYOUT_V2 ? v2->wrap_count : header->wrap_count;
}

static void
cold_tt_slot_copy(const UndoSegmentHeaderData *header, uint8 owner, uint32 segment,
				  uint32 slot_offset, ClusterColdTtSlotInputV1 *out)
{
	const UndoSegmentHeaderDataV2 *v2 = (const UndoSegmentHeaderDataV2 *)header;
	ClusterColdTtSlotInputV1 value = { 0 };

	if (v2->tt_layout_version == CLUSTER_UNDO_TT_LAYOUT_V2) {
		memcpy(&value.slot, &v2->tt_slots[slot_offset], sizeof(value.slot));
		value.original_full_xid = v2->tt_slots[slot_offset].original_full_xid;
		value.tt_layout_version = CLUSTER_UNDO_TT_LAYOUT_V2;
	} else
		value.slot = header->tt_slots[slot_offset];
	value.key.owner_instance = owner;
	value.key.segment_id = segment;
	value.key.segment_generation = cold_tt_header_generation(header);
	value.key.slot_offset = slot_offset;
	value.key.xid = value.slot.xid;
	value.key.slot_wrap = value.slot.wrap;
	*out = value;
}

#endif /* CLUSTER_COLD_RECOVERY_TT_INTERNAL_H */
