/* Author: SqlRush <sqlrush@gmail.com> */
/* Execute the production normal-start reader. File ownership, checksum and
 * ROOT rechecks remain the surrounding loader's responsibility. */
#include "postgres.h"
#include "access/transam.h"
#include "cluster/cluster_uba.h"
#include "cluster/cluster_undo_segment_init.h"
#include "unit_test.h"

UT_DEFINE_GLOBALS();

void
ExceptionalCondition(const char *condition, const char *file, int line)
{
	fprintf(stderr, "%s at %s:%d\n", condition, file, line);
	abort();
}

#include "test_cluster_normal_cold_scn.inc"
#include "test_cluster_tt_normal_start.inc"

static PGAlignedBlock page;

static UndoSegmentHeaderDataV2 *
fresh_v2(void)
{
	if (!cluster_undo_segment_make_header_v2_bytes(1, 1, page.data))
		abort();
	return (UndoSegmentHeaderDataV2 *)page.data;
}

static void
terminal(TTSlotV2 *slot, uint32 xid, uint64 epoch, uint8 status, SCN commit)
{
	memset(slot, 0, sizeof(*slot));
	slot->xid = xid;
	slot->wrap = 7;
	slot->status = status;
	slot->commit_scn = commit;
	slot->original_full_xid = (epoch << 32) | xid;
}

UT_TEST(test_legacy_terminal_slots_keep_maximum)
{
	UndoSegmentHeaderData *h;
	SCN maximum = 30;

	cluster_undo_segment_make_header_bytes(1, 1, page.data);
	h = (UndoSegmentHeaderData *)page.data;
	h->wrap_count = 9;
	h->tt_slots[47].xid = 100;
	h->tt_slots[47].wrap = 7;
	h->tt_slots[47].status = TT_SLOT_COMMITTED;
	h->tt_slots[47].commit_scn = 80;
	UT_ASSERT(normal_start_terminal_header(&page, 1, 1, &maximum));
	UT_ASSERT_EQ(maximum, 80);
	h->tt_slots[0].status = TT_SLOT_ACTIVE;
	UT_ASSERT(!normal_start_terminal_header(&page, 1, 1, &maximum));
}

UT_TEST(test_v2_empty_terminal_namespace)
{
	SCN maximum = 30;

	(void)fresh_v2();
	UT_ASSERT(normal_start_terminal_header(&page, 1, 1, &maximum));
	UT_ASSERT_EQ(maximum, 30);
}

UT_TEST(test_v2_reads_every_terminal_slot_at_40_byte_stride)
{
	for (uint32 i = 0; i < TT_SLOTS_PER_SEGMENT; i++) {
		UndoSegmentHeaderDataV2 *h = fresh_v2();
		SCN maximum = 30;

		h->wrap_count = 19;
		terminal(&h->tt_slots[i], 100 + i, i, TT_SLOT_COMMITTED, 80 + i);
		UT_ASSERT(normal_start_terminal_header(&page, 1, 1, &maximum));
		UT_ASSERT_EQ(maximum, 80 + i);
	}
}

UT_TEST(test_v2_mixed_terminals_preserve_original_maximum_order)
{
	UndoSegmentHeaderDataV2 *h = fresh_v2();
	SCN maximum = 90;

	terminal(&h->tt_slots[0], 100, 0, TT_SLOT_COMMITTED, 70);
	terminal(&h->tt_slots[1], 101, 4, TT_SLOT_ABORTED, InvalidScn);
	terminal(&h->tt_slots[47], 102, 5, TT_SLOT_RECYCLABLE, InvalidScn);
	UT_ASSERT(normal_start_terminal_header(&page, 1, 1, &maximum));
	UT_ASSERT_EQ(maximum, 90);
	h->tt_slots[0].commit_scn = 120;
	UT_ASSERT(normal_start_terminal_header(&page, 1, 1, &maximum));
	UT_ASSERT_EQ(maximum, 120);
}

UT_TEST(test_unknown_or_incomplete_framing_is_not_legacy)
{
	for (unsigned variant = 0; variant < 8; variant++) {
		UndoSegmentHeaderDataV2 *h = fresh_v2();
		SCN maximum = 30;

		switch (variant) {
		case 0: h->tt_layout_version = 1; break;
		case 1: h->tt_layout_version = 3; break;
		case 2: h->tt_slot_bytes = 32; break;
		case 3: h->tt_slot_bytes = 0; break;
		case 4: h->tt_layout_reserved = 1; break;
		case 5: h->tt_slots_count = 47; break;
		case 6: h->tt_layout_version = 0; break;
		case 7: h->tt_layout_version = 0; h->tt_slot_bytes = 0;
			h->tt_layout_reserved = 1; break;
		}
		UT_ASSERT(!normal_start_terminal_header(&page, 1, 1, &maximum));
	}
}

UT_TEST(test_v2_original_fullxid_required_even_in_last_slot)
{
	for (unsigned variant = 0; variant < 5; variant++) {
		UndoSegmentHeaderDataV2 *h = fresh_v2();
		TTSlotV2 *slot = &h->tt_slots[47];
		SCN maximum = 30;

		terminal(slot, 100, 0, TT_SLOT_COMMITTED, 80);
		switch (variant) {
		case 0: slot->original_full_xid = 0; break;
		case 1: slot->original_full_xid = UINT64_MAX; break;
		case 2: slot->original_full_xid = (UINT64_C(4) << 32) | 101; break;
		case 3: slot->xid = FrozenTransactionId;
			slot->original_full_xid = (UINT64_C(4) << 32) | FrozenTransactionId; break;
		case 4: memset(slot, 0, sizeof(*slot)); slot->original_full_xid = 100; break;
		}
		UT_ASSERT(!normal_start_terminal_header(&page, 1, 1, &maximum));
	}
}

UT_TEST(test_v2_nonterminal_or_corrupt_slot_refuses_whole_header)
{
	for (unsigned variant = 0; variant < 8; variant++) {
		UndoSegmentHeaderDataV2 *h = fresh_v2();
		TTSlotV2 *slot = &h->tt_slots[47];
		SCN maximum = 30;

		terminal(slot, 100, 7, TT_SLOT_COMMITTED, 80);
		switch (variant) {
		case 0: slot->status = TT_SLOT_ACTIVE; break;
		case 1: slot->status = 255; break;
		case 2: slot->wrap = TT_WRAP_INVALID; break;
		case 3: slot->flags = 128; break;
		case 4: slot->commit_scn = InvalidScn; break;
		case 5: slot->commit_scn = UINT64_MAX; break;
		case 6: slot->status = TT_SLOT_ABORTED; break;
		case 7: slot->status = TT_SLOT_RECYCLABLE; break;
		}
		UT_ASSERT(!normal_start_terminal_header(&page, 1, 1, &maximum));
	}
}

UT_TEST(test_v2_original_uba_and_header_bounds_remain_required)
{
	for (unsigned variant = 0; variant < 11; variant++) {
		UndoSegmentHeaderDataV2 *h = fresh_v2();
		SCN maximum = 30;

		terminal(&h->tt_slots[47], 100, 7, TT_SLOT_ABORTED, InvalidScn);
		h->tt_slots[47].first_undo_block.raw[0] = (UINT64_C(1) << 32) | 1;
		switch (variant) {
		case 0: break; /* valid own-origin UBA */
		case 1: h->tt_slots[47].first_undo_block.raw[0] =
			((uint64)UNDO_BLOCKS_PER_SEGMENT << 32) | 1; break;
		case 2: h->tt_slots[47].first_undo_block.raw[1] = (uint64)BLCKSZ << 16; break;
		case 3: h->tt_slots[47].first_undo_block.raw[0] =
			(UINT64_C(1) << 32) | (CLUSTER_UNDO_SEGS_PER_INSTANCE + 1); break;
		case 4: h->wrap_count = UINT32_MAX; break;
		case 5: h->tt_active_count = TT_SLOTS_PER_SEGMENT + 1; break;
		case 6: h->tt_next_free_slot = TT_SLOTS_PER_SEGMENT; break;
		case 7: h->segment_flags = 128; break;
		case 8: h->owner_instance = 2; break;
		case 9: h->segment_id = 2; break;
		case 10: h->pd_flags = 0; break;
		}
		UT_ASSERT_EQ(normal_start_terminal_header(&page, 1, 1, &maximum), variant == 0);
	}
}

int
main(int argc, char **argv)
{
	UT_PLAN(8);
	UT_RUN(test_legacy_terminal_slots_keep_maximum);
	UT_RUN(test_v2_empty_terminal_namespace);
	UT_RUN(test_v2_reads_every_terminal_slot_at_40_byte_stride);
	UT_RUN(test_v2_mixed_terminals_preserve_original_maximum_order);
	UT_RUN(test_unknown_or_incomplete_framing_is_not_legacy);
	UT_RUN(test_v2_original_fullxid_required_even_in_last_slot);
	UT_RUN(test_v2_nonterminal_or_corrupt_slot_refuses_whole_header);
	UT_RUN(test_v2_original_uba_and_header_bounds_remain_required);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
