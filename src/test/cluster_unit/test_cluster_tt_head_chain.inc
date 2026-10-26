/*-------------------------------------------------------------------------
 *
 * test_cluster_tt_head_chain.inc
 *      Literal retained-chain fixtures for the native operation-head consumer.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *      src/test/cluster_unit/test_cluster_tt_head_chain.inc
 *
 * NOTES
 *      Included by test_cluster_tt_durable.c; smgr I/O alone is replaced.
 *      Bytes are assembled independently of product encoders. No visibility,
 *      real filesystem, current-authority or crash-recovery claim is made.
 *-------------------------------------------------------------------------
 */

static PGAlignedBlock head_chain_pages[5];
static PGAlignedBlock head_chain_legacy_parent;
static bool head_chain_active;
static bool head_chain_long;
static bool head_chain_tt_drift;
static unsigned head_chain_header_reads;
static unsigned head_chain_generated_reads;
static unsigned head_chain_foreign_reads;

static UBA
head_chain_address(unsigned index)
{
	return uba_encode(index < 2 ? 1 : 2, 9 + index, 7, 0);
}

static UndoRecordHeader *
head_chain_header(unsigned index)
{
	return (UndoRecordHeader *)(head_chain_pages[index].data + 48);
}

static UndoSavepointRollbackPayload *
head_chain_control(unsigned index)
{
	return (UndoSavepointRollbackPayload *)(head_chain_pages[index].data + 112);
}

static UndoOperationTrailer *
head_chain_operation(unsigned index)
{
	return (UndoOperationTrailer *)(head_chain_pages[index].data
									+ (index == 0 || index == 2 ? 208 : 252));
}

static void
head_chain_page(PGAlignedBlock *page, const UndoRecordHeader *header, const void *payload)
{
	UndoBlockHeader *block = (UndoBlockHeader *)page->data;
	UndoSlotDirEntry *slot;

	memset(page->data, 0, BLCKSZ);
	block->magic = 0x55444f31;
	block->block_version = 1;
	block->block_lsn = 5000;
	block->slot_count = 1;
	block->free_offset = 48 + 64 + header->payload_length;
	memcpy(page->data + 48, header, 64);
	memcpy(page->data + 112, payload, header->payload_length);
	slot = UNDO_SLOT_DIR_PTR(page->data, 0);
	slot->record_offset = 48;
	slot->record_length = 64 + header->payload_length;
	slot->record_type = header->record_type;
	slot->flags = header->flags;
}

static void
head_chain_data(PGAlignedBlock *page, UBA previous, uint16 wrap,
				const UndoOperationTrailer *operation)
{
	UndoRecordHeader header = { 0 };
	UndoItlHistoryTrailer history = { 0 };
	char payload[188] = { 0 };

	header.record_type = UNDO_RECORD_INSERT;
	header.flags = UBA_is_invalid(previous) ? UNDO_REC_FLAG_FIRST_IN_TX : 0;
	header.payload_length = operation == NULL ? 4 : 188;
	header.xid = operation == NULL ? 778 : (uint32)operation->operation_fxid;
	header.origin_node_id = 0;
	header.tt_slot_segment_id = 1;
	header.tt_slot_id = 8;
	header.tt_wrap_plus1 = wrap + 1;
	header.write_scn = 201;
	header.prev_uba = previous;
	header.target_locator.spcOid = 1663;
	header.target_locator.dbOid = 5;
	header.target_locator.relNumber = 22000;
	header.target_block = 42;
	header.target_offset = 1;
	if (operation != NULL) {
		header.flags |= UNDO_REC_FLAG_HAS_OPERATION | UNDO_REC_FLAG_HAS_ITL_HISTORY;
		history.magic = 0x49544c48;
		history.version = 1;
		history.count = 1;
		history.entries[0].block = 42;
		history.entries[0].after_kind = ITL_FLAG_ACTIVE;
		history.entries[0].after_write_scn = 201;
		memcpy(payload + 4, &history, 136);
		memcpy(payload + 140, operation, 48);
	}
	head_chain_page(page, &header, payload);
}

static void
head_chain_seed_legacy_parent(uint16 wrap)
{
	UBA nil = InvalidUba_init;

	head_chain_data(&head_chain_legacy_parent, nil, wrap, NULL);
}

static bool
head_chain_fixture_read(uint32 segment, uint32 block, char *buf)
{
	unsigned i;

	if (buf == NULL)
		return false;
	if (head_chain_active) {
		if (segment > 256)
			head_chain_foreign_reads++;
		if (segment == 1 && block == 0 && ++head_chain_header_reads == 2 && head_chain_tt_drift)
			((UndoSegmentHeaderData *)g_canned_block)->wrap_count++;
		for (i = 0; i < 5; i++) {
			if (segment == (i < 2 ? 1 : 2) && block == 9 + i) {
				memcpy(buf, head_chain_pages[i].data, BLCKSZ);
				return true;
			}
		}
		if (head_chain_long && segment == 2 && block >= 101 && block <= 4197) {
			PGAlignedBlock page;
			UBA previous = InvalidUba_init;

			if (block != 101)
				previous = uba_encode(2, block - 1, 7, 0);
			head_chain_data(&page, previous, 5, NULL);
			memcpy(buf, page.data, BLCKSZ);
			head_chain_generated_reads++;
			return true;
		}
	}
	if (g_head_control_enabled && segment == 1 && block == 8) {
		memcpy(buf, head_chain_legacy_parent.data, BLCKSZ);
		return true;
	}
	return false;
}

/* Outer A is reserved before inner C, but A is published after C and DATA.
 * A -> D1 -> C(COMPLETE) -> D0 -> legacy top. Physical segments may roll. */
static TestUndoHeadOperation
head_chain_seed(void)
{
	TestUndoHeadOperation value = test_head_operation();
	UndoRecordHeader header = { 0 };
	UndoSavepointRollbackPayload body = { 0 };
	UndoOperationTrailer operation = value.operation;
	UBA nil = InvalidUba_init;
	char payload[144];
	unsigned i;

	seed_exact_abort_block(TT_SLOT_ACTIVE, 5, 4);
	g_head_control_enabled = false;
	head_chain_active = true;
	head_chain_long = head_chain_tt_drift = false;
	head_chain_header_reads = head_chain_generated_reads = head_chain_foreign_reads = 0;
	head_chain_data(&head_chain_pages[4], nil, 5, NULL);
	operation.operation_fxid++;
	operation.subid++;
	operation.operation_sequence = 33;
	head_chain_data(&head_chain_pages[3], head_chain_address(4), 5, &operation);
	operation = value.operation;
	operation.operation_sequence = 34;
	head_chain_data(&head_chain_pages[1], head_chain_address(2), 5, &operation);
	for (i = 0; i <= 2; i += 2) {
		operation = value.operation;
		if (i == 2) {
			operation.operation_sequence = 32;
			operation.operation_fxid++;
			operation.subid++;
		}
		header.record_type = UNDO_RECORD_SAVEPOINT_ROLLBACK;
		header.flags = UNDO_REC_FLAG_HAS_OPERATION;
		header.payload_length = 144;
		header.xid = (uint32)operation.operation_fxid;
		header.tt_slot_segment_id = 1;
		header.tt_slot_id = 8;
		header.tt_wrap_plus1 = 6;
		header.write_scn = 201;
		header.prev_uba = head_chain_address(i + 1);
		body.version = 1;
		body.length = 96;
		body.phase = i == 0 ? UNDO_SAVEPOINT_ROLLBACK_PENDING : UNDO_SAVEPOINT_ROLLBACK_COMPLETE;
		body.rollback_sequence = operation.operation_sequence;
		body.start_uba = header.prev_uba;
		body.stop_uba = head_chain_address(4);
		body.next_uba = i == 0 ? body.start_uba : body.stop_uba;
		body.scope_child_fxid = operation.operation_fxid;
		body.last_effect_wal_lsn = i == 0 ? 0 : 2000;
		body.visited_count = i == 0 ? 0 : 1;
		memcpy(payload, &body, 96);
		memcpy(payload + 96, &operation, 48);
		head_chain_page(&head_chain_pages[i], &header, payload);
	}
	return value;
}

static bool
head_chain_replay(const TestUndoHeadOperation *value)
{
	volatile bool caught = false;

	enable_sealed_header_io();
	g_write_hdr_ok = true;
	g_write_hdr_calls = g_fsync_segment_calls = g_present_durable_calls = 0;
	PG_TRY();
	{
		replay_operation_head(value);
	}
	PG_CATCH();
	{
		caught = true;
	}
	PG_END_TRY();
	head_chain_active = head_chain_long = head_chain_tt_drift = false;
	g_head_control_enabled = g_sealed_header_io = false;
	return caught;
}

UT_TEST(test_head_chain_forward_crosses_data_and_completed_control)
{
	unsigned i;

	for (i = 1; i < 5; i++) {
		TestUndoHeadOperation value = head_chain_seed();
		TTSlot *slot = &((UndoSegmentHeaderData *)g_canned_block)->tt_slots[7];
		TTSlot expected;

		slot->first_undo_block = head_chain_address(i);
		expected = *slot;
		expected.first_undo_block = value.head.first_undo_block;
		UT_ASSERT(!head_chain_replay(&value));
		UT_ASSERT_EQ(g_write_hdr_calls, 1);
		UT_ASSERT_EQ(g_fsync_segment_calls, 1);
		UT_ASSERT_EQ(g_present_durable_calls, 0);
		UT_ASSERT_EQ(memcmp(slot, &expected, sizeof(expected)), 0);
	}
}

UT_TEST(test_head_chain_stale_follows_ancestry_not_sequence)
{
	unsigned i;

	for (i = 0; i < 2; i++) {
		TestUndoHeadOperation value = head_chain_seed();
		TTSlot *slot = &((UndoSegmentHeaderData *)g_canned_block)->tt_slots[7];
		UBA later = head_chain_address(i);

		value.head.first_undo_block = head_chain_address(2);
		value.operation = *head_chain_operation(2);
		slot->first_undo_block = later;
		if (i == 1)
			head_chain_operation(1)->effect_state = CLUSTER_SAVEPOINT_EFFECT_DISCARDED;
		UT_ASSERT(!head_chain_replay(&value));
		UT_ASSERT_EQ(g_write_hdr_calls + g_fsync_segment_calls, 0);
		UT_ASSERT_EQ(g_present_durable_calls, 1);
		UT_ASSERT(cluster_undo_record_uba_equal(slot->first_undo_block, later));
	}
}

UT_TEST(test_head_chain_rejects_corrupt_or_unknown_members)
{
	unsigned i;

	for (i = 0; i < 14; i++) {
		TestUndoHeadOperation value = head_chain_seed();
		UndoRecordHeader *data = head_chain_header(1);
		TTSlot *slot = &((UndoSegmentHeaderData *)g_canned_block)->tt_slots[7];

		slot->first_undo_block = head_chain_address(2);
		switch (i) {
		case 0:
			data->prev_uba = uba_encode(1, 99, 7, 0);
			break;
		case 1:
			data->origin_node_id = 1;
			break;
		case 2:
			data->tt_slot_segment_id = 2;
			break;
		case 3:
			data->tt_wrap_plus1++;
			break;
		case 4:
			data->xid++;
			break;
		case 5:
			data->payload_length--;
			break;
		case 6:
			head_chain_operation(1)->canonical_top_fxid += UINT64_C(1) << 32;
			break;
		case 7:
			head_chain_operation(1)->canonical_segment_generation++;
			break;
		case 8:
			head_chain_operation(1)->operation_sequence = 31;
			break;
		case 9:
			((UndoBlockHeader *)head_chain_pages[1].data)->block_lsn = 0;
			break;
		case 10:
			UNDO_SLOT_DIR_PTR(head_chain_pages[1].data, 0)->record_offset = BLCKSZ - 2;
			break;
		case 11:
			((UndoItlHistoryTrailer *)(head_chain_pages[1].data + 116))->magic = 0;
			break;
		case 12:
			data->prev_uba = uba_encode(257, 99, 7, 0);
			break;
		case 13:
			data->record_type = 255;
			UNDO_SLOT_DIR_PTR(head_chain_pages[1].data, 0)->record_type = 255;
			break;
		}
		UT_ASSERT(head_chain_replay(&value));
		UT_ASSERT_EQ(g_write_hdr_calls + g_fsync_segment_calls + g_present_durable_calls, 0);
		UT_ASSERT_EQ(head_chain_foreign_reads, 0);
	}
}

UT_TEST(test_head_chain_rejects_cycles_and_unrelated_heads)
{
	unsigned i;

	for (i = 0; i < 4; i++) {
		TestUndoHeadOperation value = head_chain_seed();
		TTSlot *slot = &((UndoSegmentHeaderData *)g_canned_block)->tt_slots[7];

		slot->first_undo_block = head_chain_address(2);
		if (i == 0)
			head_chain_header(1)->prev_uba = head_chain_address(1);
		else if (i == 1)
			head_chain_header(3)->prev_uba = head_chain_address(1);
		else if (i == 2) {
			head_chain_header(3)->prev_uba = value.head.first_undo_block;
		} else {
			UBA nil = InvalidUba_init;
			head_chain_data(&head_chain_pages[1], nil, 5, NULL);
		}
		UT_ASSERT(head_chain_replay(&value));
		UT_ASSERT_EQ(g_write_hdr_calls + g_fsync_segment_calls + g_present_durable_calls, 0);
	}
}

UT_TEST(test_head_chain_legacy_top_is_not_an_unqualified_child)
{
	TestUndoHeadOperation value = head_chain_seed();
	TTSlot *slot = &((UndoSegmentHeaderData *)g_canned_block)->tt_slots[7];

	slot->first_undo_block = head_chain_address(4);
	head_chain_header(4)->xid = 779;
	UT_ASSERT(head_chain_replay(&value));
	UT_ASSERT_EQ(g_write_hdr_calls + g_fsync_segment_calls + g_present_durable_calls, 0);
	value = head_chain_seed();
	slot->first_undo_block = head_chain_address(4);
	head_chain_operation(3)->effect_state = CLUSTER_SAVEPOINT_EFFECT_DISCARDED;
	UT_ASSERT(!head_chain_replay(&value));
	UT_ASSERT_EQ(g_write_hdr_calls, 1);
}

UT_TEST(test_head_chain_has_no_small_traversal_ceiling)
{
	TestUndoHeadOperation value = head_chain_seed();
	TTSlot *slot = &((UndoSegmentHeaderData *)g_canned_block)->tt_slots[7];
	UBA first = uba_encode(2, 4197, 7, 0);
	UBA nil = InvalidUba_init;

	head_chain_long = true;
	head_chain_header(0)->prev_uba = first;
	head_chain_control(0)->start_uba = head_chain_control(0)->next_uba = first;
	head_chain_control(0)->stop_uba = nil;
	slot->first_undo_block = uba_encode(2, 101, 7, 0);
	UT_ASSERT(!head_chain_replay(&value));
	UT_ASSERT_EQ(g_write_hdr_calls, 1);
	UT_ASSERT_EQ(g_fsync_segment_calls, 1);
	UT_ASSERT(head_chain_generated_reads >= 4097);
}

UT_TEST(test_head_chain_rejects_tt_snapshot_drift)
{
	TestUndoHeadOperation value = head_chain_seed();

	head_chain_tt_drift = true;
	((UndoSegmentHeaderData *)g_canned_block)->tt_slots[7].first_undo_block = head_chain_address(2);
	UT_ASSERT(head_chain_replay(&value));
	UT_ASSERT_EQ(g_write_hdr_calls + g_fsync_segment_calls + g_present_durable_calls, 0);
	UT_ASSERT_EQ(head_chain_header_reads, 2);
}

UT_TEST(test_head_chain_nil_and_already_validate_the_older_chain)
{
	unsigned i;

	for (i = 0; i < 4; i++) {
		TestUndoHeadOperation value = head_chain_seed();
		TTSlot *slot = &((UndoSegmentHeaderData *)g_canned_block)->tt_slots[7];

		if (i & 1)
			slot->first_undo_block = value.head.first_undo_block;
		if (i >= 2)
			head_chain_header(3)->xid++;
		UT_ASSERT_EQ(head_chain_replay(&value), i >= 2);
		UT_ASSERT_EQ(g_write_hdr_calls, i == 0 ? 1 : 0);
		UT_ASSERT_EQ(g_present_durable_calls, i == 1 ? 1 : 0);
	}
}
