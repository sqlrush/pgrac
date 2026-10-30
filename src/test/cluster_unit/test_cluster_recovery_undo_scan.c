/* Cold recovery candidates from the original undo header producer.
 * Author: SqlRush <sqlrush@gmail.com>
 * Copyright (c) 2026, pgrac contributors. */
#include "postgres.h"
#include "cluster/cluster_recovery_undo_scan.h"
#include "cluster/cluster_undo_segment_init.h"
#include "unit_test.h"
#include "storage/checksum.h"

UT_DEFINE_GLOBALS();

void
ExceptionalCondition(const char *condition, const char *file, int line)
{
	fprintf(stderr, "%s at %s:%d\n", condition, file, line);
	abort();
}

#include "../../common/cluster_undo_segment_init.c"
#include "../../backend/cluster/cluster_uba.c"
#include "test_cluster_heap_small_scn.inc"
#include "../../backend/cluster/cluster_recovery_undo_scan.c"

UT_TEST(test_original_initializer_and_generation_zero)
{
	UndoSegmentHeaderData page;
	ClusterRecoveryUndoScan *scan = NULL;
	ClusterRecoveryUndoEntry entry;

	cluster_undo_segment_make_header_bytes(1, 1, (char *)&page);
	UT_ASSERT_EQ(cluster_recovery_undo_scan_begin(&page, sizeof(page), 1, 1, &scan),
				 CLUSTER_RECOVERY_UNDO_MORE);
	if (scan == NULL)
		return;
	UT_ASSERT_EQ(memcmp(cluster_recovery_undo_scan_header(scan), &page, sizeof(page)), 0);
	for (unsigned i = 0; i < TT_SLOTS_PER_SEGMENT; i++) {
		UT_ASSERT_EQ(cluster_recovery_undo_scan_next(scan, &entry), CLUSTER_RECOVERY_UNDO_ENTRY);
		UT_ASSERT_EQ(entry.slot_offset, i);
		UT_ASSERT_EQ(entry.segment_id, 1);
		UT_ASSERT_EQ(entry.owner_instance, 1);
		UT_ASSERT_EQ(entry.segment_generation, 0);
		UT_ASSERT_EQ(entry.slot.status, TT_SLOT_UNUSED);
	}
	UT_ASSERT_EQ(cluster_recovery_undo_scan_next(scan, &entry), CLUSTER_RECOVERY_UNDO_END);
	UT_ASSERT_EQ(cluster_recovery_undo_scan_next(scan, &entry), CLUSTER_RECOVERY_UNDO_END);
	UT_ASSERT_EQ(entry.segment_id, 0);
	cluster_recovery_undo_scan_destroy(scan);
}

UT_TEST(test_all_slots_and_raw_header_survive_without_terminal_decisions)
{
	UndoSegmentHeaderData page, expected;
	ClusterRecoveryUndoScan *scan = NULL;
	ClusterRecoveryUndoEntry entry;

	cluster_undo_segment_make_header_bytes(32768, 128, (char *)&page);
	page.segment_state = SEGMENT_RECYCLABLE;
	page.segment_flags = UNDO_SEGMENT_FLAG_FULL;
	page.wrap_count = UINT32_MAX;
	page.tt_active_count = 0; /* never a census bound */
	page.tt_next_free_slot = 0;
	page.head_block = 19;
	page.tail_block = 3;
	page.commit_horizon_scn = 911;
	page.free_block_bitmap[2] = 0xfe;
	for (unsigned i = 0; i < TT_SLOTS_PER_SEGMENT; i++) {
		TTSlot *slot = &page.tt_slots[i];

		slot->status = i % (TT_SLOT_RECYCLABLE + 1);
		slot->xid = 4195104 + i;
		slot->wrap = i;
		slot->commit_scn = i == 2 ? InvalidScn : 100 + i;
		slot->flags = i % 2 ? TT_SLOT_FLAG_CTRC_RELEASE_PROVEN : 0;
		slot->first_undo_block.raw[0] = UINT64CONST(0x1234567800000101) + i;
		slot->first_undo_block.raw[1] = UINT64CONST(0x1122334455667788) + i;
	}
	expected = page;
	UT_ASSERT_EQ(cluster_recovery_undo_scan_begin(&page, sizeof(page), 32768, 128, &scan),
				 CLUSTER_RECOVERY_UNDO_MORE);
	if (scan == NULL)
		return;
	memset(&page, 0xff, sizeof(page));
	UT_ASSERT_EQ(memcmp(cluster_recovery_undo_scan_header(scan), &expected, sizeof(expected)), 0);
	for (unsigned i = 0; i < TT_SLOTS_PER_SEGMENT; i++) {
		UT_ASSERT_EQ(cluster_recovery_undo_scan_next(scan, &entry), CLUSTER_RECOVERY_UNDO_ENTRY);
		UT_ASSERT_EQ(entry.segment_id, 32768);
		UT_ASSERT_EQ(entry.owner_instance, 128);
		UT_ASSERT_EQ(entry.segment_generation, UINT32_MAX);
		UT_ASSERT_EQ(entry.slot_offset, i);
		UT_ASSERT_EQ(memcmp(&entry.slot, &expected.tt_slots[i], sizeof(TTSlot)), 0);
	}
	UT_ASSERT_EQ(cluster_recovery_undo_scan_next(scan, &entry), CLUSTER_RECOVERY_UNDO_END);
	cluster_recovery_undo_scan_destroy(scan);
}

UT_TEST(test_mismatched_segment_and_owner_cannot_export_a_prefix)
{
	for (unsigned variant = 0; variant < 2; variant++) {
		UndoSegmentHeaderData page;
		ClusterRecoveryUndoScan *scan = (void *)1;

		cluster_undo_segment_make_header_bytes(257, 2, (char *)&page);
		if (variant == 0)
			page.segment_id++;
		else
			page.owner_instance = 1;
		UT_ASSERT_EQ(cluster_recovery_undo_scan_begin(&page, sizeof(page), 257, 2, &scan),
					 CLUSTER_RECOVERY_UNDO_IDENTITY);
		UT_ASSERT(scan == NULL);
	}
}

UT_TEST(test_unknown_or_corrupt_header_never_exports_partial_slots)
{
	for (unsigned variant = 0; variant < 12; variant++) {
		UndoSegmentHeaderData page;
		ClusterRecoveryUndoScan *scan = (void *)1;
		ClusterRecoveryUndoResult result;

		cluster_undo_segment_make_header_bytes(1, 1, (char *)&page);
		switch (variant) {
		case 0:
			page.pd_pagesize_version ^= 1;
			break;
		case 1:
			page.pd_flags = PD_HAS_ITL;
			break;
		case 2:
			page.pd_flags |= PD_SPACE_METADATA;
			break;
		case 3:
			page.pd_lower++;
			break;
		case 4:
			page.pd_upper--;
			break;
		case 5:
			page.pd_special--;
			break;
		case 6:
			page.segment_size_bytes--;
			break;
		case 7:
			page.segment_state = SEGMENT_INVALID;
			break;
		case 8:
			page.tt_slots_count--;
			break;
		case 9:
			page.segment_flags = 0x80;
			break;
		case 10:
			page.tt_slots[47].status = TT_SLOT_INVALID;
			break;
		case 11:
			page.tt_slots[47].flags = 0x80;
			break;
		}
		result = cluster_recovery_undo_scan_begin(&page, sizeof(page), 1, 1, &scan);
		UT_ASSERT(result == CLUSTER_RECOVERY_UNDO_UNSUPPORTED
				  || result == CLUSTER_RECOVERY_UNDO_CORRUPT);
		UT_ASSERT(scan == NULL);
	}
}

UT_TEST(test_invalid_arguments_clear_outputs)
{
	UndoSegmentHeaderData page;
	ClusterRecoveryUndoScan *scan = (void *)1;
	ClusterRecoveryUndoEntry entry, zero = { 0 };
	const uint32 segments[] = { 0, 32769, 1, 257, 1 };
	const uint8 owners[] = { 1, 128, 0, 1, 129 };

	cluster_undo_segment_make_header_bytes(1, 1, (char *)&page);
	for (unsigned i = 0; i < lengthof(segments); i++) {
		UT_ASSERT_EQ(
			cluster_recovery_undo_scan_begin(&page, sizeof(page), segments[i], owners[i], &scan),
			CLUSTER_RECOVERY_UNDO_INVALID);
		UT_ASSERT(scan == NULL);
	}
	UT_ASSERT_EQ(cluster_recovery_undo_scan_begin(NULL, sizeof(page), 1, 1, &scan),
				 CLUSTER_RECOVERY_UNDO_INVALID);
	UT_ASSERT_EQ(cluster_recovery_undo_scan_begin(&page, sizeof(page) - 1, 1, 1, &scan),
				 CLUSTER_RECOVERY_UNDO_INVALID);
	UT_ASSERT_EQ(cluster_recovery_undo_scan_begin(&page, sizeof(page), 1, 1, NULL),
				 CLUSTER_RECOVERY_UNDO_INVALID);
	memset(&entry, 0xff, sizeof(entry));
	UT_ASSERT_EQ(cluster_recovery_undo_scan_next(NULL, &entry), CLUSTER_RECOVERY_UNDO_INVALID);
	UT_ASSERT_EQ(memcmp(&entry, &zero, sizeof(entry)), 0);
	UT_ASSERT(cluster_recovery_undo_scan_header(NULL) == NULL);
	cluster_recovery_undo_scan_destroy(NULL);
}

static UndoRecordHeader
record_header(uint8 type)
{
	UndoRecordHeader header = { 0 };

	header.record_type = type;
	header.flags = UNDO_REC_FLAG_FIRST_IN_TX;
	header.xid = 42;
	header.origin_node_id = 1;
	header.tt_slot_segment_id = 257;
	header.tt_slot_id = 7;
	header.tt_wrap_plus1 = 2;
	header.write_scn = 100;
	header.target_locator = (RelFileLocator){ 1663, 5, 16385 };
	header.target_fork = MAIN_FORKNUM;
	header.target_block = 19;
	header.target_offset = 2;
	return header;
}

static void
record_page_init(char *page)
{
	UndoBlockHeader block = { 0 };

	memset(page, 0, BLCKSZ);
	block.magic = PGRAC_UNDO_BLOCK_MAGIC;
	block.block_version = UNDO_BLOCK_VERSION_1;
	block.free_offset = sizeof(block);
	block.first_change_scn = 100;
	block.block_lsn = UINT64CONST(0x1002000);
	memcpy(page, &block, sizeof(block));
}

/* Original producer's append-only bytes; the cold reader has no allocation,
 * online TT, CLOG, authority or terminal fixture to turn into a success. */
static void
record_append(char *page, UndoRecordHeader header, const void *payload, uint16 length)
{
	UndoBlockHeader block;
	UndoSlotDirEntry slot = { 0 };

	memcpy(&block, page, sizeof(block));
	header.payload_length = length;
	slot.record_offset = block.free_offset;
	slot.record_length = sizeof(header) + length;
	slot.record_type = header.record_type;
	slot.flags = header.flags;
	memcpy(page + slot.record_offset, &header, sizeof(header));
	memcpy(page + slot.record_offset + sizeof(header), payload, length);
	memcpy(page + UNDO_SLOT_DIR_OFFSET(block.slot_count), &slot, sizeof(slot));
	block.slot_count++;
	block.free_offset += slot.record_length;
	memcpy(page, &block, sizeof(block));
}

static void
record_page(char *page)
{
	UndoInsertPayload insert = { 32, 0 };

	record_page_init(page);
	record_append(page, record_header(UNDO_RECORD_INSERT), &insert, sizeof(insert));
}

UT_TEST(test_exact_record_keeps_cross_target_transaction_predecessor)
{
	char storage[BLCKSZ + 1];
	char *page = storage + 1; /* Input is not required to be aligned. */
	UndoRecordHeader header = record_header(UNDO_RECORD_INSERT);
	UndoInsertPayload insert = { 44, 0 };
	ClusterRecoveryUndoRecord out;
	UBA address = uba_encode(258, 9, 6, 1);

	record_page(page);
	header.flags = 0;
	header.prev_uba = uba_encode(257, 8, 6, 2);
	header.target_locator.relNumber = 18301;
	header.target_block = 27;
	header.tt_wrap_plus1 = 0; /* Unknown stays unknown, never a terminal proof. */
	record_append(page, header, &insert, sizeof(insert));
	UT_ASSERT_EQ(cluster_recovery_undo_record_read(page, BLCKSZ, 258, 9, 2, address, &out),
				 CLUSTER_RECOVERY_UNDO_ENTRY);
	header.payload_length = sizeof(insert);
	UT_ASSERT_EQ(memcmp(&out.header, &header, sizeof(header)), 0);
	UT_ASSERT_EQ(memcmp(out.payload, &insert, sizeof(insert)), 0);
	UT_ASSERT_EQ(memcmp(&out.address, &address, sizeof(address)), 0);
	UT_ASSERT_EQ(out.block_header.slot_count, 2);
	UT_ASSERT_EQ(out.body_length, sizeof(insert));
	UT_ASSERT_EQ(out.history.count, 0);
	memset(page, 0xff, BLCKSZ);
	UT_ASSERT_EQ(out.header.target_locator.relNumber, 18301);
	UT_ASSERT_EQ(out.header.tt_slot_segment_id, 257);
	UT_ASSERT_EQ(out.header.tt_wrap_plus1, 0);
	UT_ASSERT_EQ(out.block_header.crc64, 0); /* Not invented checksum evidence. */
}

UT_TEST(test_all_original_payload_types_are_preserved)
{
	for (unsigned type = UNDO_RECORD_INSERT; type <= UNDO_RECORD_ITL; type++) {
		char page[BLCKSZ], payload[64] = { 0 };
		UndoRecordHeader header = record_header(type);
		ClusterRecoveryUndoRecord out;
		uint16 length;

		record_page_init(page);
		if (type == UNDO_RECORD_INSERT) {
			UndoInsertPayload value = { 32, 0 };
			length = sizeof(value);
			memcpy(payload, &value, sizeof(value));
		} else if (type == UNDO_RECORD_UPDATE) {
			UndoUpdatePayload value = { 0 };
			value.old_tuple_length = 5;
			value.old_tuple_offset = sizeof(value);
			value.new_block = 28;
			value.new_offset = 4;
			length = sizeof(value) + 5;
			memcpy(payload, &value, sizeof(value));
			memcpy(payload + sizeof(value), "tuple", 5);
		} else if (type == UNDO_RECORD_DELETE) {
			UndoDeletePayload value = { 0 };
			value.full_tuple_length = 5;
			value.full_tuple_offset = sizeof(value);
			length = sizeof(value) + 5;
			memcpy(payload, &value, sizeof(value));
			memcpy(payload + sizeof(value), "tuple", 5);
		} else {
			UndoItlPayload value = { 0 };
			value.itl_slot_idx = 2;
			value.new_flags = ITL_FLAG_LOCK_ONLY_ACTIVE;
			value.lock_xid = header.xid;
			length = sizeof(value);
			memcpy(payload, &value, sizeof(value));
		}
		record_append(page, header, payload, length);
		UT_ASSERT_EQ(cluster_recovery_undo_record_read(page, BLCKSZ, 258, 9, 2,
													   uba_encode(258, 9, 6, 0), &out),
					 CLUSTER_RECOVERY_UNDO_ENTRY);
		UT_ASSERT_EQ(out.header.record_type, type);
		UT_ASSERT_EQ(out.body_length, length);
		UT_ASSERT_EQ(memcmp(out.payload, payload, length), 0);
	}
}

UT_TEST(test_page_history_is_distinct_from_transaction_predecessor)
{
	char page[BLCKSZ];
	char payload[sizeof(UndoInsertPayload) + sizeof(UndoItlHistoryTrailer)];
	UndoInsertPayload insert = { 32, 0 };
	UndoItlHistoryTrailer history = { 0 };
	UndoRecordHeader header = record_header(UNDO_RECORD_INSERT);
	ClusterRecoveryUndoRecord out;

	header.flags = UNDO_REC_FLAG_HAS_ITL_HISTORY;
	header.prev_uba = uba_encode(257, 8, 6, 2);
	history.magic = UNDO_ITL_HISTORY_MAGIC;
	history.version = UNDO_ITL_HISTORY_VERSION;
	history.count = 1;
	history.entries[0].block = header.target_block;
	history.entries[0].slot_index = 2;
	history.entries[0].after_kind = ITL_FLAG_ACTIVE;
	history.entries[0].after_write_scn = header.write_scn;
	history.entries[0].prior.flags = ITL_FLAG_ACTIVE;
	history.entries[0].prior.xid = 991;
	history.entries[0].prior.write_scn = 90;
	history.entries[0].prior.undo_segment_head = uba_encode(513, 5, 3, 0);
	memcpy(payload, &insert, sizeof(insert));
	memcpy(payload + sizeof(insert), &history, sizeof(history));
	record_page_init(page);
	record_append(page, header, payload, sizeof(payload));
	UT_ASSERT_EQ(
		cluster_recovery_undo_record_read(page, BLCKSZ, 258, 9, 2, uba_encode(258, 9, 6, 0), &out),
		CLUSTER_RECOVERY_UNDO_ENTRY);
	UT_ASSERT_EQ(memcmp(&out.history, &history, sizeof(history)), 0);
	UT_ASSERT_EQ(memcmp(&out.header.prev_uba, &header.prev_uba, sizeof(UBA)), 0);
	UT_ASSERT_EQ(out.body_length, sizeof(insert));
	UT_ASSERT_EQ(out.history.entries[0].prior.xid, 991);
}

static bool
record_zero(const ClusterRecoveryUndoRecord *out)
{
	const unsigned char *bytes = (const unsigned char *)out;

	for (size_t i = 0; i < sizeof(*out); i++)
		if (bytes[i] != 0)
			return false;
	return true;
}

UT_TEST(test_record_address_and_short_input_refuse)
{
	char page[BLCKSZ];
	ClusterRecoveryUndoRecord out;
	UBA addresses[] = { { { 0, 0 } },
						uba_encode(258, 0, 6, 0),
						uba_encode(257, 9, 6, 0),
						uba_encode(258, 8, 6, 0),
						uba_encode(258, 9, 6, 1),
						uba_encode(258, 9, 5, 0) };

	record_page(page);
	for (unsigned i = 0; i < lengthof(addresses); i++) {
		memset(&out, 0xff, sizeof(out));
		UT_ASSERT(cluster_recovery_undo_record_read(page, BLCKSZ, 258, 9, 2, addresses[i], &out)
				  != CLUSTER_RECOVERY_UNDO_ENTRY);
		UT_ASSERT(record_zero(&out));
	}
	UT_ASSERT_EQ(cluster_recovery_undo_record_read(page, BLCKSZ - 1, 258, 9, 2,
												   uba_encode(258, 9, 6, 0), &out),
				 CLUSTER_RECOVERY_UNDO_INVALID);
	UT_ASSERT_EQ(
		cluster_recovery_undo_record_read(NULL, BLCKSZ, 258, 9, 2, uba_encode(258, 9, 6, 0), &out),
		CLUSTER_RECOVERY_UNDO_INVALID);
	UT_ASSERT_EQ(
		cluster_recovery_undo_record_read(page, BLCKSZ, 258, 9, 2, uba_encode(258, 9, 6, 0), NULL),
		CLUSTER_RECOVERY_UNDO_INVALID);
}

UT_TEST(test_corrupt_block_or_directory_clears_record)
{
	for (unsigned variant = 0; variant < 11; variant++) {
		char page[BLCKSZ];
		UndoBlockHeader block;
		UndoSlotDirEntry slot;
		ClusterRecoveryUndoRecord out;

		record_page(page);
		memcpy(&block, page, sizeof(block));
		memcpy(&slot, page + UNDO_SLOT_DIR_OFFSET(0), sizeof(slot));
		switch (variant) {
		case 0:
			block.magic = 0;
			break;
		case 1:
			block.block_version++;
			break;
		case 2:
			block.slot_count = UINT16_MAX;
			break;
		case 3:
			block.free_offset = BLCKSZ;
			break;
		case 4:
			block.free_offset = sizeof(block) - 1;
			break;
		case 5:
			slot.record_offset = UINT32_MAX;
			break;
		case 6:
			slot.record_offset = sizeof(block) - 1;
			break;
		case 7:
			slot.record_length = sizeof(UndoRecordHeader) - 1;
			break;
		case 8:
			slot.record_length = UINT16_MAX;
			break;
		case 9:
			block.free_offset--;
			break;
		case 10:
			block.slot_count = 2;
			memcpy(page + UNDO_SLOT_DIR_OFFSET(1), &slot, sizeof(slot));
			break;
		}
		memcpy(page, &block, sizeof(block));
		memcpy(page + UNDO_SLOT_DIR_OFFSET(0), &slot, sizeof(slot));
		memset(&out, 0xff, sizeof(out));
		UT_ASSERT(cluster_recovery_undo_record_read(page, BLCKSZ, 258, 9, 2,
													uba_encode(258, 9, 6, 0), &out)
				  != CLUSTER_RECOVERY_UNDO_ENTRY);
		UT_ASSERT(record_zero(&out));
	}
}

UT_TEST(test_record_identity_must_match_the_independent_read_key)
{
	for (unsigned variant = 0; variant < 5; variant++) {
		char page[BLCKSZ];
		UndoRecordHeader header = record_header(UNDO_RECORD_INSERT);
		UndoInsertPayload insert = { 32, 0 };
		ClusterRecoveryUndoRecord out;

		if (variant == 0)
			header.origin_node_id = 0;
		if (variant == 1)
			header.tt_slot_segment_id = 1;
		if (variant == 2)
			header.tt_slot_id++;
		if (variant == 3)
			header.tt_slot_segment_id = 0;
		record_page_init(page);
		record_append(page, header, &insert, sizeof(insert));
		memset(&out, 0xff, sizeof(out));
		UT_ASSERT(cluster_recovery_undo_record_read(page, BLCKSZ, 258, 9, variant == 4 ? 1 : 2,
													uba_encode(258, 9, 6, 0), &out)
				  != CLUSTER_RECOVERY_UNDO_ENTRY);
		UT_ASSERT(record_zero(&out));
	}
}

UT_TEST(test_payload_unknown_flags_and_slot_header_disagreement_refuse)
{
	for (unsigned variant = 0; variant < 8; variant++) {
		char page[BLCKSZ];
		UndoRecordHeader header = record_header(UNDO_RECORD_INSERT);
		UndoInsertPayload insert = { 32, 0 };
		UndoSlotDirEntry slot;
		ClusterRecoveryUndoRecord out;

		switch (variant) {
		case 0:
			header.record_type = 99;
			break;
		case 1:
			header.flags |= UNDO_REC_FLAG_CONTINUED;
			break;
		case 2:
			header.flags |= UNDO_REC_FLAG_HAS_ITL_HISTORY;
			break;
		case 3:
			header.flags = 0;
			break; /* Missing predecessor, not FIRST. */
		case 4:
			insert.flags = 1;
			break;
		}
		record_page_init(page);
		record_append(page, header, &insert, sizeof(insert));
		memcpy(&slot, page + UNDO_SLOT_DIR_OFFSET(0), sizeof(slot));
		if (variant == 5)
			slot.record_type = UNDO_RECORD_UPDATE;
		if (variant == 6)
			slot.flags = 0;
		if (variant == 7) {
			memcpy(&header, page + slot.record_offset, sizeof(header));
			header.payload_length++;
			memcpy(page + slot.record_offset, &header, sizeof(header));
		}
		memcpy(page + UNDO_SLOT_DIR_OFFSET(0), &slot, sizeof(slot));
		memset(&out, 0xff, sizeof(out));
		UT_ASSERT(cluster_recovery_undo_record_read(page, BLCKSZ, 258, 9, 2,
													uba_encode(258, 9, 6, 0), &out)
				  != CLUSTER_RECOVERY_UNDO_ENTRY);
		UT_ASSERT(record_zero(&out));
	}
}

/* Use the upstream checksum object, independently of the undo writer. */
static void
checksum_fixture(PGAlignedBlock *page)
{
	((PageHeader)page->data)->pd_checksum = pg_checksum_page(page->data, 0);
}

UT_TEST(test_protected_header_rejects_single_byte_damage_before_export)
{
	PGAlignedBlock original, changed;
	const size_t offsets[] = { offsetof(UndoSegmentHeaderData, wrap_count),
		offsetof(UndoSegmentHeaderData, tt_slots) + offsetof(TTSlot, xid), BLCKSZ - 1 };
	ClusterRecoveryUndoScan *scan = NULL;

	cluster_undo_segment_make_header_bytes(1, 1, original.data);
	checksum_fixture(&original);
	UT_ASSERT_EQ(cluster_recovery_undo_scan_begin(original.data, BLCKSZ, 1, 1, &scan),
		CLUSTER_RECOVERY_UNDO_MORE);
	cluster_recovery_undo_scan_destroy(scan);
	for (unsigned i = 0; i < lengthof(offsets); i++) {
		changed = original;
		changed.data[offsets[i]] ^= 1;
		scan = NULL;
		UT_ASSERT_EQ(cluster_recovery_undo_scan_begin(changed.data, BLCKSZ, 1, 1, &scan),
			CLUSTER_RECOVERY_UNDO_CORRUPT);
		UT_ASSERT_NULL(scan);
		cluster_recovery_undo_scan_destroy(scan);
	}
}

UT_TEST(test_torn_header_is_not_a_stable_qualified_base)
{
	PGAlignedBlock old, newer, torn;
	ClusterRecoveryUndoScan *scan = NULL;

	cluster_undo_segment_make_header_bytes(1, 1, old.data);
	checksum_fixture(&old);
	newer = old;
	((UndoSegmentHeaderData *)newer.data)->tt_slots[0].xid = 123;
	newer.data[BLCKSZ - 1] = 42;
	checksum_fixture(&newer);
	for (int prefix = 0; prefix < 2; prefix++) {
		torn = prefix ? newer : old;
		memcpy(torn.data + BLCKSZ / 2,
			(prefix ? old.data : newer.data) + BLCKSZ / 2, BLCKSZ / 2);
		scan = NULL;
		UT_ASSERT_EQ(cluster_recovery_undo_scan_begin(torn.data, BLCKSZ, 1, 1, &scan),
			CLUSTER_RECOVERY_UNDO_CORRUPT);
		UT_ASSERT_NULL(scan);
		cluster_recovery_undo_scan_destroy(scan);
	}
	/* Legacy is unverified, not corruption. Raw scanning grants no top proof. */
	((PageHeader)old.data)->pd_checksum = 0;
	UT_ASSERT_EQ(cluster_recovery_undo_scan_begin(old.data, BLCKSZ, 1, 1, &scan),
		CLUSTER_RECOVERY_UNDO_MORE);
	cluster_recovery_undo_scan_destroy(scan);
}


static void
tt40_page(PGAlignedBlock *page)
{
	UndoSegmentHeaderDataV2 *header = (UndoSegmentHeaderDataV2 *)page->data;

	UT_ASSERT(cluster_undo_segment_make_header_v2_bytes(257, 2, page->data));
	header->wrap_count = 7;
	header->tt_slots[47].xid = 42;
	header->tt_slots[47].wrap = 6;
	header->tt_slots[47].status = TT_SLOT_ACTIVE;
	header->tt_slots[47].original_full_xid = (UINT64CONST(9) << 32) | 42;
	UT_ASSERT(cluster_undo_segment_header_seal(page->data));
}

UT_TEST(test_tt40_complete_inventory_preserves_original_identity_and_generation)
{
	PGAlignedBlock page, expected;
	UndoSegmentHeaderDataV2 *header = (UndoSegmentHeaderDataV2 *)page.data;
	const UndoSegmentHeaderDataV2 *saved = (const UndoSegmentHeaderDataV2 *)expected.data;
	ClusterRecoveryUndoScan *scan = NULL;
	ClusterRecoveryUndoEntry entry, zero = { 0 };

	tt40_page(&page);
	for (unsigned i = 1; i < TT_SLOTS_PER_SEGMENT; i++) {
		TTSlotV2 *slot = &header->tt_slots[i];

		slot->xid = 42 + i;
		slot->wrap = i;
		slot->status = 1 + i % TT_SLOT_RECYCLABLE;
		slot->commit_scn = slot->status == TT_SLOT_COMMITTED ? 100 + i : InvalidScn;
		slot->flags = slot->status == TT_SLOT_COMMITTED ? TT_SLOT_FLAG_CTRC_RELEASE_PROVEN : 0;
		slot->first_undo_block = uba_encode(258, 19, i, 0);
		slot->original_full_xid = ((uint64)i << 32) | slot->xid;
	}
	/* Neither counters nor bitmap are an enumeration shortcut. */
	header->tt_active_count = 0;
	header->tt_next_free_slot = TT_SLOTS_PER_SEGMENT;
	header->free_block_bitmap[17] = 0x43;
	header->oldest_active_scn = 17;
	UT_ASSERT(cluster_undo_segment_header_seal(page.data));
	expected = page;
	UT_ASSERT_EQ(cluster_recovery_undo_scan_begin(page.data, BLCKSZ, 257, 2, &scan),
		CLUSTER_RECOVERY_UNDO_MORE);
	if (scan == NULL)
		return;
	memset(page.data, 0xff, BLCKSZ);
	UT_ASSERT_EQ(memcmp(cluster_recovery_undo_scan_header(scan), expected.data, BLCKSZ), 0);
	for (unsigned i = 0; i < TT_SLOTS_PER_SEGMENT; i++) {
		UT_ASSERT_EQ(cluster_recovery_undo_scan_next(scan, &entry), CLUSTER_RECOVERY_UNDO_ENTRY);
		UT_ASSERT_EQ(entry.segment_id, 257);
		UT_ASSERT_EQ(entry.owner_instance, 2);
		UT_ASSERT_EQ(entry.segment_generation, 7);
		UT_ASSERT_EQ(entry.slot_offset, i);
		UT_ASSERT_EQ(entry.tt_layout_version, CLUSTER_UNDO_TT_LAYOUT_V2);
		UT_ASSERT_EQ(entry.original_full_xid, saved->tt_slots[i].original_full_xid);
		UT_ASSERT_EQ(memcmp(&entry.slot, &saved->tt_slots[i], sizeof(entry.slot)), 0);
	}
	UT_ASSERT_EQ(cluster_recovery_undo_scan_next(scan, &entry), CLUSTER_RECOVERY_UNDO_END);
	UT_ASSERT_EQ(memcmp(&entry, &zero, sizeof(entry)), 0);
	cluster_recovery_undo_scan_destroy(scan);
}

UT_TEST(test_tt40_unaligned_same_raw_different_epoch_is_never_widened)
{
	PGAlignedBlock page;
	UndoSegmentHeaderDataV2 *header = (UndoSegmentHeaderDataV2 *)page.data;
	char bytes[BLCKSZ + 1], before[BLCKSZ + 1];
	ClusterRecoveryUndoScan *scan = NULL;
	ClusterRecoveryUndoEntry entry;

	tt40_page(&page);
	header->tt_slots[0] = header->tt_slots[47];
	header->tt_slots[0].original_full_xid = 42; /* Epoch zero is a real identity. */
	UT_ASSERT(cluster_undo_segment_header_seal(page.data));
	memset(bytes, 0x52, sizeof(bytes));
	memcpy(bytes + 1, page.data, BLCKSZ);
	memcpy(before, bytes, sizeof(bytes));
	UT_ASSERT_EQ(cluster_recovery_undo_scan_begin(bytes + 1, BLCKSZ, 257, 2, &scan),
		CLUSTER_RECOVERY_UNDO_MORE);
	UT_ASSERT_EQ(memcmp(bytes, before, sizeof(bytes)), 0);
	if (scan == NULL)
		return;
	for (unsigned i = 0; i < TT_SLOTS_PER_SEGMENT; i++) {
		UT_ASSERT_EQ(cluster_recovery_undo_scan_next(scan, &entry), CLUSTER_RECOVERY_UNDO_ENTRY);
		if (i == 0 || i == 47) {
			UT_ASSERT_EQ(entry.slot.xid, 42);
			UT_ASSERT_EQ(entry.original_full_xid, i == 0 ? 42 : (UINT64CONST(9) << 32) | 42);
		}
	}
	cluster_recovery_undo_scan_destroy(scan);
}

UT_TEST(test_tt40_invalid_version_or_last_slot_refuses_before_any_prefix)
{
	for (unsigned variant = 0; variant < 14; variant++) {
		PGAlignedBlock page;
		UndoSegmentHeaderDataV2 *h = (UndoSegmentHeaderDataV2 *)page.data;
		ClusterRecoveryUndoScan *scan = (void *)1;
		ClusterRecoveryUndoResult result;

		tt40_page(&page);
		switch (variant) {
		case 0: h->tt_layout_version = 1; break;
		case 1: h->tt_layout_version = 3; break;
		case 2: h->tt_slot_bytes = sizeof(TTSlot); break;
		case 3: h->tt_slot_bytes++; break;
		case 4: h->tt_layout_reserved = 1; break;
		case 5: h->tt_slots[47].original_full_xid = 0; break;
		case 6: h->tt_slots[47].original_full_xid = UINT64_MAX; break;
		case 7: h->tt_slots[47].original_full_xid++; break;
		case 8: h->tt_slots[0].original_full_xid = 42; break;
		case 9: h->tt_slots[47].wrap = TT_WRAP_INVALID; break;
		case 10: h->tt_slots[47].flags = 0x80; break;
		case 11: h->tt_slots[47].status = TT_SLOT_INVALID; break;
		case 12: h->tt_slots[47].xid = FrozenTransactionId;
			h->tt_slots[47].original_full_xid = (UINT64CONST(9) << 32) | FrozenTransactionId; break;
		case 13: h->tt_slots[47].commit_scn = UINT64_MAX; break;
		}
		UT_ASSERT(cluster_undo_segment_header_seal(page.data));
		result = cluster_recovery_undo_scan_begin(page.data, BLCKSZ, 257, 2, &scan);
		UT_ASSERT(result == CLUSTER_RECOVERY_UNDO_UNSUPPORTED || result == CLUSTER_RECOVERY_UNDO_CORRUPT);
		UT_ASSERT_NULL(scan);
	}
}

UT_TEST(test_tt40_checksum_and_wrong_owner_never_export_inventory)
{
	PGAlignedBlock page;
	const size_t offsets[] = { offsetof(UndoSegmentHeaderDataV2, wrap_count),
		offsetof(UndoSegmentHeaderDataV2, tt_slots) + 47 * sizeof(TTSlotV2) + offsetof(TTSlotV2, original_full_xid),
		BLCKSZ - 1 };

	for (unsigned i = 0; i < lengthof(offsets); i++) {
		ClusterRecoveryUndoScan *scan = (void *)1;

		tt40_page(&page);
		page.data[offsets[i]] ^= 1;
		UT_ASSERT_EQ(cluster_recovery_undo_scan_begin(page.data, BLCKSZ, 257, 2, &scan),
			CLUSTER_RECOVERY_UNDO_CORRUPT);
		UT_ASSERT_NULL(scan);
	}
	for (unsigned i = 0; i < 2; i++) {
		UndoSegmentHeaderDataV2 *h = (UndoSegmentHeaderDataV2 *)page.data;
		ClusterRecoveryUndoScan *scan = (void *)1;

		tt40_page(&page);
		if (i == 0) h->owner_instance = 1;
		else h->segment_id++;
		UT_ASSERT(cluster_undo_segment_header_seal(page.data));
		UT_ASSERT_EQ(cluster_recovery_undo_scan_begin(page.data, BLCKSZ, 257, 2, &scan),
			CLUSTER_RECOVERY_UNDO_IDENTITY);
		UT_ASSERT_NULL(scan);
	}
}

UT_TEST(test_legacy_inventory_does_not_retrofit_original_fullxid)
{
	UndoSegmentHeaderData page;
	ClusterRecoveryUndoScan *scan = NULL;
	ClusterRecoveryUndoEntry entry;

	cluster_undo_segment_make_header_bytes(1, 1, (char *)&page);
	page.tt_slots[47].status = TT_SLOT_ACTIVE;
	page.tt_slots[47].xid = 42;
	page.tt_slots[47].wrap = 9;
	UT_ASSERT_EQ(cluster_recovery_undo_scan_begin(&page, BLCKSZ, 1, 1, &scan),
		CLUSTER_RECOVERY_UNDO_MORE);
	if (scan == NULL) return;
	for (unsigned i = 0; i < TT_SLOTS_PER_SEGMENT; i++) {
		UT_ASSERT_EQ(cluster_recovery_undo_scan_next(scan, &entry), CLUSTER_RECOVERY_UNDO_ENTRY);
		UT_ASSERT_EQ(entry.tt_layout_version, 0);
		UT_ASSERT_EQ(entry.original_full_xid, 0);
	}
	cluster_recovery_undo_scan_destroy(scan);
}

int
main(void)
{
	UT_PLAN(19);
	UT_RUN(test_protected_header_rejects_single_byte_damage_before_export);
	UT_RUN(test_torn_header_is_not_a_stable_qualified_base);
	UT_RUN(test_original_initializer_and_generation_zero);
	UT_RUN(test_all_slots_and_raw_header_survive_without_terminal_decisions);
	UT_RUN(test_mismatched_segment_and_owner_cannot_export_a_prefix);
	UT_RUN(test_unknown_or_corrupt_header_never_exports_partial_slots);
	UT_RUN(test_invalid_arguments_clear_outputs);
	UT_RUN(test_exact_record_keeps_cross_target_transaction_predecessor);
	UT_RUN(test_all_original_payload_types_are_preserved);
	UT_RUN(test_page_history_is_distinct_from_transaction_predecessor);
	UT_RUN(test_record_address_and_short_input_refuse);
	UT_RUN(test_corrupt_block_or_directory_clears_record);
	UT_RUN(test_record_identity_must_match_the_independent_read_key);
	UT_RUN(test_payload_unknown_flags_and_slot_header_disagreement_refuse);
	UT_RUN(test_tt40_complete_inventory_preserves_original_identity_and_generation);
	UT_RUN(test_tt40_unaligned_same_raw_different_epoch_is_never_widened);
	UT_RUN(test_tt40_invalid_version_or_last_slot_refuses_before_any_prefix);
	UT_RUN(test_tt40_checksum_and_wrong_owner_never_export_inventory);
	UT_RUN(test_legacy_inventory_does_not_retrofit_original_fullxid);
	UT_DONE();
	return ut_failed_count != 0;
}
