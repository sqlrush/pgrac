/*-------------------------------------------------------------------------
 * cluster_recovery_undo_scan.c
 *   Read-only undo header and TT candidates for cold recovery.
 *
 * Portions Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 *-------------------------------------------------------------------------
 */
#include "postgres.h"
#include "cluster/cluster_recovery_undo_scan.h"
#include "cluster/cluster_undo_segment_init.h"
#include "cluster/storage/cluster_undo_alloc.h"

#ifdef USE_PGRAC_CLUSTER

struct ClusterRecoveryUndoScan {
	UndoSegmentHeaderData header;
	uint16 next_slot;
};

static ClusterRecoveryUndoResult
recovery_undo_layout(const UndoSegmentHeaderData *header, uint32 segment, uint8 owner)
{
	Page page = (Page)header;

	if (cluster_undo_segment_header_integrity(header) == CLUSTER_UNDO_HEADER_CORRUPT)
		return CLUSTER_RECOVERY_UNDO_CORRUPT;
	if (PageGetPageSize(page) != BLCKSZ || PageGetPageLayoutVersion(page) != PG_PAGE_LAYOUT_VERSION
		|| (header->pd_flags & PD_UNDO_SEG_HEADER) == 0)
		return CLUSTER_RECOVERY_UNDO_UNSUPPORTED;
	if (header->segment_id != segment || header->owner_instance != owner)
		return CLUSTER_RECOVERY_UNDO_IDENTITY;
	if (!UndoSegmentHeader_identity_matches((const char *)header, segment, owner)
		|| (header->pd_flags & (~PD_VALID_FLAG_BITS | PD_HAS_ITL | PD_SPACE_METADATA)) != 0
		|| header->pd_lower != SizeOfPageHeaderData || header->pd_upper != BLCKSZ
		|| header->pd_special != BLCKSZ || (header->segment_flags & ~UNDO_SEGMENT_FLAG_FULL) != 0)
		return CLUSTER_RECOVERY_UNDO_CORRUPT;
	for (unsigned i = 0; i < TT_SLOTS_PER_SEGMENT; i++)
		if (header->tt_slots[i].status > TT_SLOT_RECYCLABLE
			|| (header->tt_slots[i].flags & ~TT_SLOT_FLAGS_KNOWN) != 0)
			return CLUSTER_RECOVERY_UNDO_CORRUPT;
	return CLUSTER_RECOVERY_UNDO_MORE;
}

ClusterRecoveryUndoResult
cluster_recovery_undo_scan_begin(const void *page, size_t length, uint32 expected_segment,
								 uint8 expected_owner, ClusterRecoveryUndoScan **out)
{
	ClusterRecoveryUndoScan *scan;
	ClusterRecoveryUndoResult result;

	if (out != NULL)
		*out = NULL;
	if (page == NULL || length != BLCKSZ || out == NULL || expected_owner == 0
		|| expected_owner > UNDO_OWNER_INSTANCE_MAX || expected_segment == 0
		|| (expected_segment - 1) / CLUSTER_UNDO_SEGS_PER_INSTANCE != expected_owner - 1)
		return CLUSTER_RECOVERY_UNDO_INVALID;
	scan = calloc(1, sizeof(*scan));
	if (scan == NULL)
		return CLUSTER_RECOVERY_UNDO_CAPACITY;
	memcpy(&scan->header, page, BLCKSZ);
	result = recovery_undo_layout(&scan->header, expected_segment, expected_owner);
	if (result != CLUSTER_RECOVERY_UNDO_MORE) {
		free(scan);
		return result;
	}
	*out = scan;
	return CLUSTER_RECOVERY_UNDO_MORE;
}

const UndoSegmentHeaderData *
cluster_recovery_undo_scan_header(const ClusterRecoveryUndoScan *scan)
{
	return scan == NULL ? NULL : &scan->header;
}

ClusterRecoveryUndoResult
cluster_recovery_undo_scan_next(ClusterRecoveryUndoScan *scan, ClusterRecoveryUndoEntry *out)
{
	if (out != NULL)
		memset(out, 0, sizeof(*out));
	if (scan == NULL || out == NULL)
		return CLUSTER_RECOVERY_UNDO_INVALID;
	if (scan->next_slot == TT_SLOTS_PER_SEGMENT)
		return CLUSTER_RECOVERY_UNDO_END;
	out->segment_id = scan->header.segment_id;
	out->segment_generation = scan->header.wrap_count;
	out->owner_instance = scan->header.owner_instance;
	out->slot_offset = scan->next_slot;
	out->slot = scan->header.tt_slots[scan->next_slot++];
	return CLUSTER_RECOVERY_UNDO_ENTRY;
}

void
cluster_recovery_undo_scan_destroy(ClusterRecoveryUndoScan *scan)
{
	free(scan);
}

ClusterRecoveryUndoResult
cluster_recovery_undo_record_read(const void *page, size_t length, uint32 expected_segment,
								  uint32 expected_block, uint8 expected_owner, UBA address,
								  ClusterRecoveryUndoRecord *out)
{
	UndoBlockHeader block;
	UndoSlotDirEntry selected = { 0 };
	UndoRecordHeader selected_header = { 0 };
	UndoItlHistoryTrailer selected_history = { 0 };
	uint16 selected_body = 0;
	uint32 segment, blockno;
	uint16 tt_slot, row;
	uint8 occupied[BLCKSZ / 8] = { 0 };
	const char *bytes = page;

	if (out != NULL)
		memset(out, 0, sizeof(*out));
	if (page == NULL || length != BLCKSZ || out == NULL || expected_owner == 0
		|| expected_owner > UNDO_OWNER_INSTANCE_MAX
		|| !uba_decode_record(address, &segment, &blockno, &tt_slot, &row))
		return CLUSTER_RECOVERY_UNDO_INVALID;
	if (segment != expected_segment || blockno != expected_block
		|| (segment - 1) / CLUSTER_UNDO_SEGS_PER_INSTANCE != expected_owner - 1)
		return CLUSTER_RECOVERY_UNDO_IDENTITY;

	memcpy(&block, page, sizeof(block));
	if (block.magic != PGRAC_UNDO_BLOCK_MAGIC || block.block_version != UNDO_BLOCK_VERSION_1)
		return CLUSTER_RECOVERY_UNDO_UNSUPPORTED;
	if (!cluster_undo_record_slot_index_valid(block.slot_count, row)
		|| block.free_offset < sizeof(block)
		|| block.free_offset > BLCKSZ - (uint32)block.slot_count * sizeof(UndoSlotDirEntry))
		return CLUSTER_RECOVERY_UNDO_CORRUPT;

	/* Validate the complete block before exporting any record. Directory and
	 * record structs may be unaligned; never dereference the caller's bytes.
	 * Each occupied byte is visited at most once, or overlap refuses the block. */
	for (uint16 i = 0; i < block.slot_count; i++) {
		UndoSlotDirEntry slot;
		UndoRecordHeader header;
		UndoItlHistoryTrailer history;
		uint16 body_length;
		uint32 end;

		memcpy(&slot, bytes + UNDO_SLOT_DIR_OFFSET(i), sizeof(slot));
		if (!cluster_undo_record_slot_range_valid(block.slot_count, i, slot.record_offset,
												  slot.record_length)
			|| slot.record_length < sizeof(header))
			return CLUSTER_RECOVERY_UNDO_CORRUPT;
		end = slot.record_offset + slot.record_length; /* bounded by the helper */
		if (end > block.free_offset)
			return CLUSTER_RECOVERY_UNDO_CORRUPT;
		for (uint32 offset = slot.record_offset; offset < end; offset++) {
			uint8 mask = (uint8)(1U << (offset & 7));

			if ((occupied[offset >> 3] & mask) != 0)
				return CLUSTER_RECOVERY_UNDO_CORRUPT;
			occupied[offset >> 3] |= mask;
		}
		memcpy(&header, bytes + slot.record_offset, sizeof(header));
		if (header.record_type < UNDO_RECORD_INSERT || header.record_type > UNDO_RECORD_ITL
			|| (header.flags & ~(UNDO_REC_FLAG_FIRST_IN_TX | UNDO_REC_FLAG_HAS_ITL_HISTORY)) != 0)
			return CLUSTER_RECOVERY_UNDO_UNSUPPORTED;
		if (header.payload_length != slot.record_length - sizeof(header)
			|| slot.record_type != header.record_type || slot.flags != header.flags)
			return CLUSTER_RECOVERY_UNDO_CORRUPT;
		if (header.origin_node_id != expected_owner - 1 || header.tt_slot_segment_id == 0
			|| (header.tt_slot_segment_id - 1) / CLUSTER_UNDO_SEGS_PER_INSTANCE
				   != expected_owner - 1
			|| header.tt_slot_id == 0 || header.tt_slot_id > TT_SLOTS_PER_SEGMENT
			|| (i == row && header.tt_slot_id != (uint32)tt_slot + 1))
			return CLUSTER_RECOVERY_UNDO_IDENTITY;
		if (!cluster_undo_record_decode_payload(&header,
												bytes + slot.record_offset + sizeof(header),
												header.payload_length, &body_length, &history))
			return CLUSTER_RECOVERY_UNDO_CORRUPT;
		if (i == row) {
			selected = slot;
			selected_header = header;
			selected_history = history;
			selected_body = body_length;
		}
	}

	/* Generation and terminal authority are deliberately absent. In particular,
	 * the original producer's crc64=0 and an unknown tt_wrap_plus1 stay raw. */
	out->address = address;
	out->block_header = block;
	out->directory = selected;
	out->header = selected_header;
	out->history = selected_history;
	out->body_length = selected_body;
	memcpy(out->payload, bytes + selected.record_offset + sizeof(selected_header),
		   selected_header.payload_length);
	return CLUSTER_RECOVERY_UNDO_ENTRY;
}

#endif
