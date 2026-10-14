/* Cold recovery candidates from the original undo header producer.
 * Author: SqlRush <sqlrush@gmail.com>
 * Copyright (c) 2026, pgrac contributors. */
#include "postgres.h"
#include "cluster/cluster_recovery_undo_scan.h"
#include "cluster/cluster_undo_segment_init.h"
#include "unit_test.h"

UT_DEFINE_GLOBALS();

void
ExceptionalCondition(const char *condition, const char *file, int line)
{
	fprintf(stderr, "%s at %s:%d\n", condition, file, line);
	abort();
}

#include "../../common/cluster_undo_segment_init.c"
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

int
main(void)
{
	UT_PLAN(5);
	UT_RUN(test_original_initializer_and_generation_zero);
	UT_RUN(test_all_slots_and_raw_header_survive_without_terminal_decisions);
	UT_RUN(test_mismatched_segment_and_owner_cannot_export_a_prefix);
	UT_RUN(test_unknown_or_corrupt_header_never_exports_partial_slots);
	UT_RUN(test_invalid_arguments_clear_outputs);
	UT_DONE();
	return ut_failed_count != 0;
}
