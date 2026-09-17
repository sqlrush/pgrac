/*-------------------------------------------------------------------------
 *
 * test_cluster_page_edge.c
 *    STOP-06 successor PageVersion-edge ABI and sole xloginsert encoder.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/xloginsert.h"

#include "unit_test.h"

UT_DEFINE_GLOBALS();

void
ExceptionalCondition(const char *condition_name, const char *file_name, int line_number)
{
	printf("# Assert failed: %s at %s:%d\n", condition_name, file_name, line_number);
	abort();
}

static void
set_incarnation(uint8 incarnation[16], uint8 seed)
{
	int i;

	for (i = 0; i < 16; i++)
		incarnation[i] = seed + i;
}

static void
make_ordinary(RfPageVersionEdgeEntryV1 *entry, uint8 block_id)
{
	memset(entry, 0, sizeof(*entry));
	entry->block_id = block_id;
	entry->page_class = RF_PAGE_CLASS_ORDINARY;
	entry->before_kind = RF_PAGE_STATE_PRESENT;
	entry->result_kind = RF_PAGE_STATE_PRESENT;
	entry->component_ordinal = block_id;
	set_incarnation(entry->before.segment_incarnation, 9);
	entry->before.mutation_token = 10;
	set_incarnation(entry->result_incarnation, 9);
}

static bool
encode_entries(uint64 result_token, RfPageVersionEdgeEntryV1 *entries, uint8 entry_count,
			   uint8 *wire, Size capacity, Size *wire_size)
{
	memset(wire, 0xa5, capacity);
	*wire_size = SIZE_MAX;
	return XLogEncodePageVersionEdgeV1(wire, capacity, result_token, entries, entry_count,
									   wire_size);
}

UT_TEST(test_successor_literals_and_layout)
{
	UT_ASSERT_EQ(XLR_BLOCK_ID_PAGE_VERSION_EDGE, 251);
	UT_ASSERT_EQ(XLR_PAGE_VERSION_EDGE_FORMAT_V1, 1);
	UT_ASSERT_EQ(XLR_PAGE_VERSION_EDGE_HEADER_SIZE, 16);
	UT_ASSERT_EQ(XLR_PAGE_VERSION_EDGE_ENTRY_SIZE, 48);
	UT_ASSERT_EQ(XLR_PAGE_VERSION_EDGE_MAX_ENTRIES, 33);
	UT_ASSERT_EQ(XLR_PAGE_VERSION_EDGE_MAX_SIZE, 1600);
	UT_ASSERT_EQ(sizeof(RfPageVersionV1), 24);
	UT_ASSERT_EQ(offsetof(RfPageVersionV1, mutation_token), 16);
	UT_ASSERT_EQ(sizeof(RfPageVersionEdgeEntryV1), 48);
	UT_ASSERT_EQ(offsetof(RfPageVersionEdgeEntryV1, before), 8);
	UT_ASSERT_EQ(offsetof(RfPageVersionEdgeEntryV1, result_incarnation), 32);
}

UT_TEST(test_encoder_writes_exact_native_endian_offsets)
{
	RfPageVersionEdgeEntryV1 entry;
	uint8 wire[XLR_PAGE_VERSION_EDGE_MAX_SIZE];
	uint64 token;
	uint64 before_token;
	Size wire_size;

	make_ordinary(&entry, 0);
	UT_ASSERT(
		encode_entries(UINT64_C(0x1122334455667788), &entry, 1, wire, sizeof(wire), &wire_size));
	UT_ASSERT_EQ(wire_size, 64);
	UT_ASSERT_EQ(wire[0], 251);
	UT_ASSERT_EQ(wire[1], 1);
	UT_ASSERT_EQ(wire[2], 1);
	UT_ASSERT_EQ(wire[3], 48);
	memcpy(&token, wire + 8, sizeof(token));
	memcpy(&before_token, wire + 16 + 24, sizeof(before_token));
	UT_ASSERT_EQ(token, UINT64_C(0x1122334455667788));
	UT_ASSERT_EQ(before_token, 10);
	UT_ASSERT(memcmp(wire + 16 + 8, entry.before.segment_incarnation, 16) == 0);
	UT_ASSERT(memcmp(wire + 16 + 32, entry.result_incarnation, 16) == 0);
}

UT_TEST(test_encoder_failure_leaves_outputs_untouched)
{
	RfPageVersionEdgeEntryV1 entry;
	uint8 wire[XLR_PAGE_VERSION_EDGE_MAX_SIZE];
	Size wire_size;

	make_ordinary(&entry, 0);
	UT_ASSERT(!encode_entries(0, &entry, 1, wire, sizeof(wire), &wire_size));
	UT_ASSERT_EQ(wire[0], 0xa5);
	UT_ASSERT_EQ(wire_size, SIZE_MAX);
	UT_ASSERT(!encode_entries(11, &entry, 1, wire, 63, &wire_size));
	UT_ASSERT_EQ(wire[0], 0xa5);
	UT_ASSERT_EQ(wire_size, SIZE_MAX);
}

UT_TEST(test_encoder_accepts_exact_33_entry_maximum)
{
	RfPageVersionEdgeEntryV1 entries[33];
	uint8 wire[XLR_PAGE_VERSION_EDGE_MAX_SIZE];
	Size wire_size;
	int i;

	for (i = 0; i < 33; i++)
		make_ordinary(&entries[i], i);
	UT_ASSERT(encode_entries(17, entries, 33, wire, sizeof(wire), &wire_size));
	UT_ASSERT_EQ(wire_size, XLR_PAGE_VERSION_EDGE_MAX_SIZE);
	UT_ASSERT(!encode_entries(17, entries, 34, wire, sizeof(wire), &wire_size));
}

UT_TEST(test_encoder_requires_sorted_unique_blocks_and_ordinals)
{
	RfPageVersionEdgeEntryV1 entries[2];
	uint8 wire[XLR_PAGE_VERSION_EDGE_MAX_SIZE];
	Size wire_size;

	make_ordinary(&entries[0], 0);
	make_ordinary(&entries[1], 1);
	UT_ASSERT(encode_entries(17, entries, 2, wire, sizeof(wire), &wire_size));
	entries[1].block_id = 0;
	UT_ASSERT(!encode_entries(17, entries, 2, wire, sizeof(wire), &wire_size));
	make_ordinary(&entries[1], 1);
	entries[1].component_ordinal = 0;
	UT_ASSERT(!encode_entries(17, entries, 2, wire, sizeof(wire), &wire_size));
}

UT_TEST(test_encoder_validates_ordinary_transitions)
{
	RfPageVersionEdgeEntryV1 entry;
	uint8 wire[XLR_PAGE_VERSION_EDGE_MAX_SIZE];
	Size wire_size;

	make_ordinary(&entry, 0);
	entry.result_incarnation[15] ^= UINT8_C(0xff);
	UT_ASSERT(!encode_entries(17, &entry, 1, wire, sizeof(wire), &wire_size));
	make_ordinary(&entry, 0);
	entry.before_kind = RF_PAGE_STATE_UNFORMATTED;
	entry.before.mutation_token = 0;
	UT_ASSERT(!encode_entries(17, &entry, 1, wire, sizeof(wire), &wire_size));
	entry.edge_flags = RF_PAGE_EDGE_WILL_INIT | RF_PAGE_EDGE_FULL_COVERAGE;
	UT_ASSERT(encode_entries(17, &entry, 1, wire, sizeof(wire), &wire_size));
	memset(entry.before.segment_incarnation, 0, 16);
	entry.before_kind = RF_PAGE_STATE_ABSENT;
	UT_ASSERT(encode_entries(17, &entry, 1, wire, sizeof(wire), &wire_size));
}

UT_TEST(test_encoder_accepts_only_exact_anchor_masks)
{
	RfPageVersionEdgeEntryV1 entry;
	uint8 wire[XLR_PAGE_VERSION_EDGE_MAX_SIZE];
	Size wire_size;
	uint16 valid[] = { UINT16_C(0x0005), UINT16_C(0x0006), UINT16_C(0x0007) };
	uint16 invalid[] = { UINT16_C(0x0001), UINT16_C(0x0002), UINT16_C(0x0004), UINT16_C(0x8000) };
	int i;

	for (i = 0; i < lengthof(valid); i++) {
		make_ordinary(&entry, 0);
		entry.edge_flags = valid[i];
		UT_ASSERT(encode_entries(17, &entry, 1, wire, sizeof(wire), &wire_size));
	}
	for (i = 0; i < lengthof(invalid); i++) {
		make_ordinary(&entry, 0);
		entry.edge_flags = invalid[i];
		UT_ASSERT(!encode_entries(17, &entry, 1, wire, sizeof(wire), &wire_size));
	}
}

UT_TEST(test_encoder_validates_rebuildable_and_routed_entries)
{
	RfPageVersionEdgeEntryV1 entry;
	uint8 wire[XLR_PAGE_VERSION_EDGE_MAX_SIZE];
	Size wire_size;

	memset(&entry, 0, sizeof(entry));
	entry.page_class = RF_PAGE_CLASS_REBUILDABLE_FSM;
	entry.before_kind = RF_PAGE_STATE_REBUILDABLE;
	entry.result_kind = RF_PAGE_STATE_REBUILDABLE;
	UT_ASSERT(encode_entries(17, &entry, 1, wire, sizeof(wire), &wire_size));
	entry.page_class = RF_PAGE_CLASS_ROUTED_SIDE;
	entry.before_kind = RF_PAGE_STATE_ROUTED;
	entry.result_kind = RF_PAGE_STATE_ROUTED;
	UT_ASSERT(encode_entries(17, &entry, 1, wire, sizeof(wire), &wire_size));
	entry.page_class = RF_PAGE_CLASS_TEMP_LOCAL;
	UT_ASSERT(!encode_entries(17, &entry, 1, wire, sizeof(wire), &wire_size));
}

UT_TEST(test_explicit_anchor_force_has_total_precedence)
{
	uint8 force_will_init = REGBUF_FORCE_IMAGE | REGBUF_WILL_INIT;

	UT_ASSERT(XLogPageVersionImageRequiredV1(force_will_init, false, InvalidXLogRecPtr,
											 InvalidXLogRecPtr));
	UT_ASSERT(XLogPageVersionImageRequiredV1(REGBUF_FORCE_IMAGE | REGBUF_NO_IMAGE, false,
											 UINT64_C(900), UINT64_C(1)));
	UT_ASSERT(!XLogPageVersionImageRequiredV1(REGBUF_NO_IMAGE, true, UINT64_C(1), UINT64_C(900)));
	UT_ASSERT(!XLogPageVersionImageRequiredV1(0, false, UINT64_C(1), UINT64_C(900)));
	UT_ASSERT(XLogPageVersionImageRequiredV1(0, true, UINT64_C(1), UINT64_C(900)));
}

int
main(void)
{
	UT_PLAN(9);
	UT_RUN(test_successor_literals_and_layout);
	UT_RUN(test_encoder_writes_exact_native_endian_offsets);
	UT_RUN(test_encoder_failure_leaves_outputs_untouched);
	UT_RUN(test_encoder_accepts_exact_33_entry_maximum);
	UT_RUN(test_encoder_requires_sorted_unique_blocks_and_ordinals);
	UT_RUN(test_encoder_validates_ordinary_transitions);
	UT_RUN(test_encoder_accepts_only_exact_anchor_masks);
	UT_RUN(test_encoder_validates_rebuildable_and_routed_entries);
	UT_RUN(test_explicit_anchor_force_has_total_precedence);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
