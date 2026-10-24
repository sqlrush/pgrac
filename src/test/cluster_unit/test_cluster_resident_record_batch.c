/* Exact metadata from one resident undo DATA image; no I/O or resolver doubles. */
#define USE_PGRAC_CLUSTER 1
#include "postgres.h"
#include "cluster/cluster_cr.h"
#include "cluster/cluster_uba.h"
#undef printf
#undef fprintf
#include "unit_test.h"

UT_DEFINE_GLOBALS();

static PGAlignedBlock image;
static ClusterTxLocator requests[8];

/* Eight 68-byte records. DATA segment 1 and canonical TT segment 2 are
 * deliberately distinct within origin 0; record slots remain physical. */
static void
fixture(void)
{
	UndoBlockHeader *block = (UndoBlockHeader *)image.data;
	uint32 i;

	memset(&image, 0, sizeof(image));
	memset(requests, 0, sizeof(requests));
	block->magic = PGRAC_UNDO_BLOCK_MAGIC;
	block->block_version = UNDO_BLOCK_VERSION_1;
	block->slot_count = 8;
	block->free_offset = 48 + 8 * 68;
	for (i = 0; i < 8; i++) {
		UndoRecordHeader record;
		UndoSlotDirEntry *slot = UNDO_SLOT_DIR_PTR(image.data, i);

		memset(&record, 0, sizeof(record));
		record.record_type = UNDO_RECORD_INSERT;
		record.flags = UNDO_REC_FLAG_FIRST_IN_TX;
		record.payload_length = 4;
		record.xid = 900; /* equal raw xids do not identify equal TT carriers */
		record.origin_node_id = 0;
		record.tt_slot_segment_id = 2;
		record.tt_slot_id = i + 1;
		record.tt_wrap_plus1 = i + 4;
		record.write_scn = 100 + i;
		record.target_locator = (RelFileLocator){1663, 77, 800};
		record.target_fork = MAIN_FORKNUM;
		record.target_block = 12;
		record.target_offset = i + 1;
		slot->record_offset = 48 + 68 * i;
		slot->record_length = 68;
		slot->record_type = UNDO_RECORD_INSERT;
		slot->flags = UNDO_REC_FLAG_FIRST_IN_TX;
		memcpy(image.data + slot->record_offset, &record, sizeof(record));
		memset(image.data + slot->record_offset + sizeof(record), 0x60 + i, 4);
		requests[i].uba = uba_encode(1, 7, i, i);
		requests[i].xid = 900;
		requests[i].tt_wrap = TT_WRAP_INVALID;
		requests[i].itl_kind = ITL_FLAG_ACTIVE;
		requests[i].itl_slot_index = i;
	}
}

static void
reject_unchanged(const char *bytes, const ClusterTxLocator *locators, uint32 count)
{
	ClusterR4ResidentRecordBatch out;
	ClusterR4ResidentRecordBatch before;

	memset(&out, 0xa5, sizeof(out));
	memcpy(&before, &out, sizeof(before));
	UT_ASSERT(!cluster_cr_r4_prepare_resident_records(bytes, locators, count, &out));
	UT_ASSERT_EQ(memcmp(&out, &before, sizeof(out)), 0);
}

UT_TEST(batch_preserves_eight_exact_carriers_and_targets)
{
	ClusterR4ResidentRecordBatch out;
	PGAlignedBlock before;
	ClusterTxLocator locators_before[8];
	uint32 i;
	bool prepared;

	fixture();
	before = image;
	memcpy(locators_before, requests, sizeof(requests));
	prepared = cluster_cr_r4_prepare_resident_records(image.data, requests, 8, &out);
	UT_ASSERT(prepared);
	if (!prepared)
		return;
	UT_ASSERT_EQ(out.count, 8);
	for (i = 0; i < 8; i++) {
		UT_ASSERT_EQ(out.records[i].record_offset, 48 + i * 68);
		UT_ASSERT_EQ(out.records[i].record_length, 68);
		UT_ASSERT_EQ(out.records[i].canonical_locator.tt_wrap, i + 3);
		UT_ASSERT_EQ(out.records[i].canonical_locator.uba.raw[0], requests[i].uba.raw[0]);
		UT_ASSERT_EQ(out.records[i].canonical_locator.uba.raw[1], requests[i].uba.raw[1]);
		UT_ASSERT_EQ(out.records[i].canonical_locator.itl_slot_index, i);
		UT_ASSERT_EQ(out.records[i].header.tt_slot_segment_id, 2);
		UT_ASSERT_EQ(out.records[i].header.tt_slot_id, i + 1);
		UT_ASSERT_EQ(out.records[i].header.target_offset, i + 1);
		UT_ASSERT_EQ(out.records[i].header.write_scn, 100 + i);
		UT_ASSERT_EQ(memcmp(&out.records[i].header, image.data + 48 + i * 68, 64), 0);
	}
	UT_ASSERT_EQ(memcmp(&image, &before, sizeof(image)), 0);
	UT_ASSERT_EQ(memcmp(requests, locators_before, sizeof(requests)), 0);
}

UT_TEST(batch_retains_order_and_duplicate_physical_requests)
{
	ClusterTxLocator selected[3];
	ClusterR4ResidentRecordBatch out;
	bool prepared;

	fixture();
	selected[0] = requests[7];
	selected[1] = requests[0];
	selected[2] = requests[7];
	prepared = cluster_cr_r4_prepare_resident_records(image.data, selected, 3, &out);
	UT_ASSERT(prepared);
	if (!prepared)
		return;
	UT_ASSERT_EQ(out.count, 3);
	UT_ASSERT_EQ(out.records[0].header.tt_slot_id, 8);
	UT_ASSERT_EQ(out.records[1].header.tt_slot_id, 1);
	UT_ASSERT_EQ(out.records[2].header.tt_slot_id, 8);
}

UT_TEST(batch_accepts_exact_known_wrap)
{
	ClusterR4ResidentRecordBatch out;
	bool prepared;

	fixture();
	requests[0].tt_wrap = 3;
	prepared = cluster_cr_r4_prepare_resident_records(image.data, requests, 1, &out);
	UT_ASSERT(prepared);
	if (!prepared)
		return;
	UT_ASSERT_EQ(out.count, 1);
	UT_ASSERT_EQ(out.records[0].canonical_locator.tt_wrap, 3);
}

UT_TEST(batch_rejects_bad_last_identity_without_publishing_prefix)
{
	fixture();
	requests[7].xid = 901;
	reject_unchanged(image.data, requests, 8);
}

UT_TEST(batch_rejects_known_wrap_mismatch)
{
	fixture();
	requests[7].tt_wrap = 9; /* addressed record has wrap 10 */
	reject_unchanged(image.data, requests, 8);
}

UT_TEST(batch_rejects_unknown_persisted_wrap)
{
	UndoRecordHeader record;

	fixture();
	memcpy(&record, image.data + 524, sizeof(record));
	record.tt_wrap_plus1 = 0;
	memcpy(image.data + 524, &record, sizeof(record));
	reject_unchanged(image.data, requests, 8);
}

UT_TEST(batch_rejects_mixed_data_blocks)
{
	fixture();
	requests[7].uba = uba_encode(1, 8, 7, 7);
	reject_unchanged(image.data, requests, 8);
}

UT_TEST(batch_rejects_mixed_data_segments)
{
	fixture();
	requests[7].uba = uba_encode(2, 7, 7, 7);
	reject_unchanged(image.data, requests, 8);
}

UT_TEST(batch_rejects_header_block_and_invalid_uba)
{
	fixture();
	requests[0].uba = uba_encode(1, 0, 0, 0);
	reject_unchanged(image.data, requests, 1);
	memset(&requests[0].uba, 0, sizeof(UBA));
	reject_unchanged(image.data, requests, 1);
}

UT_TEST(batch_rejects_row_beyond_directory)
{
	fixture();
	requests[7].uba = uba_encode(1, 7, 7, 8);
	reject_unchanged(image.data, requests, 8);
}

UT_TEST(batch_rejects_bad_header_or_directory_bound)
{
	UndoBlockHeader *block;

	fixture();
	block = (UndoBlockHeader *)image.data;
	block->magic = 0;
	reject_unchanged(image.data, requests, 8);
	fixture();
	block->block_version = 2;
	reject_unchanged(image.data, requests, 8);
	fixture();
	block->slot_count = 0;
	reject_unchanged(image.data, requests, 8);
	fixture();
	block->slot_count = UINT16_MAX;
	reject_unchanged(image.data, requests, 8);
	fixture();
	block->free_offset = BLCKSZ;
	reject_unchanged(image.data, requests, 8);
}

UT_TEST(batch_rejects_bad_record_span)
{
	UndoSlotDirEntry *slot;

	fixture();
	slot = UNDO_SLOT_DIR_PTR(image.data, 7);
	slot->record_offset = 47;
	reject_unchanged(image.data, requests, 8);
	fixture();
	slot->record_length = 63;
	reject_unchanged(image.data, requests, 8);
	fixture();
	slot->record_offset = UINT32_MAX;
	reject_unchanged(image.data, requests, 8);
	fixture();
	slot->record_length = 69;
	reject_unchanged(image.data, requests, 8);
}

UT_TEST(batch_rejects_directory_and_payload_mismatch)
{
	UndoRecordHeader record;

	fixture();
	UNDO_SLOT_DIR_PTR(image.data, 7)->record_type = UNDO_RECORD_DELETE;
	reject_unchanged(image.data, requests, 8);
	fixture();
	UNDO_SLOT_DIR_PTR(image.data, 7)->flags = 0;
	reject_unchanged(image.data, requests, 8);
	fixture();
	memcpy(&record, image.data + 524, sizeof(record));
	record.payload_length = 5;
	memcpy(image.data + 524, &record, sizeof(record));
	reject_unchanged(image.data, requests, 8);
}

UT_TEST(batch_rejects_wrong_canonical_tt_slot_or_origin)
{
	UndoRecordHeader record;

	fixture();
	memcpy(&record, image.data + 524, sizeof(record));
	record.tt_slot_id = 7;
	memcpy(image.data + 524, &record, sizeof(record));
	reject_unchanged(image.data, requests, 8);
	fixture();
	memcpy(&record, image.data + 524, sizeof(record));
	record.origin_node_id = 1;
	memcpy(image.data + 524, &record, sizeof(record));
	reject_unchanged(image.data, requests, 8);
}

UT_TEST(batch_rejects_bad_locator_kind_slot_or_xid)
{
	fixture();
	requests[7].itl_kind = 0xff;
	reject_unchanged(image.data, requests, 8);
	fixture();
	requests[7].itl_slot_index = 8;
	reject_unchanged(image.data, requests, 8);
	fixture();
	requests[7].xid = FrozenTransactionId;
	reject_unchanged(image.data, requests, 8);
}

UT_TEST(batch_checks_count_before_walking_inputs)
{
	/* Only one addressable locator; an oversize call must not read beyond it. */
	ClusterTxLocator one;

	fixture();
	one = requests[0];
	reject_unchanged(image.data, &one, 9);
	reject_unchanged(image.data, &one, UINT32_MAX);
	reject_unchanged(image.data, &one, 0);
}

UT_TEST(batch_rejects_null_inputs)
{
	fixture();
	reject_unchanged(NULL, requests, 1);
	reject_unchanged(image.data, NULL, 1);
	UT_ASSERT(!cluster_cr_r4_prepare_resident_records(image.data, requests, 1, NULL));
}

UT_TEST(single_extractor_retains_payload_and_failure_contract)
{
	PGAlignedBlock record;
	PGAlignedBlock before;
	ClusterTxLocator canonical;
	ClusterTxLocator canonical_before;
	size_t length = 777;

	fixture();
	memset(&record, 0xa5, sizeof(record));
	UT_ASSERT(cluster_cr_r4_extract_resident_record(image.data, &requests[7], record.data,
													&length, &canonical));
	UT_ASSERT_EQ(length, 68);
	UT_ASSERT_EQ(memcmp(record.data, image.data + 524, 68), 0);
	UT_ASSERT_EQ((unsigned char)record.data[68], 0xa5);
	UT_ASSERT_EQ(canonical.tt_wrap, 10);
	before = record;
	canonical_before = canonical;
	requests[7].xid = 901;
	UT_ASSERT(!cluster_cr_r4_extract_resident_record(image.data, &requests[7], record.data,
													 &length, &canonical));
	UT_ASSERT_EQ(length, 68);
	UT_ASSERT_EQ(memcmp(&record, &before, sizeof(record)), 0);
	UT_ASSERT_EQ(memcmp(&canonical, &canonical_before, sizeof(canonical)), 0);
}

int
main(void)
{
	UT_PLAN(18);
	UT_RUN(batch_preserves_eight_exact_carriers_and_targets);
	UT_RUN(batch_retains_order_and_duplicate_physical_requests);
	UT_RUN(batch_accepts_exact_known_wrap);
	UT_RUN(batch_rejects_bad_last_identity_without_publishing_prefix);
	UT_RUN(batch_rejects_known_wrap_mismatch);
	UT_RUN(batch_rejects_unknown_persisted_wrap);
	UT_RUN(batch_rejects_mixed_data_blocks);
	UT_RUN(batch_rejects_mixed_data_segments);
	UT_RUN(batch_rejects_header_block_and_invalid_uba);
	UT_RUN(batch_rejects_row_beyond_directory);
	UT_RUN(batch_rejects_bad_header_or_directory_bound);
	UT_RUN(batch_rejects_bad_record_span);
	UT_RUN(batch_rejects_directory_and_payload_mismatch);
	UT_RUN(batch_rejects_wrong_canonical_tt_slot_or_origin);
	UT_RUN(batch_rejects_bad_locator_kind_slot_or_xid);
	UT_RUN(batch_checks_count_before_walking_inputs);
	UT_RUN(batch_rejects_null_inputs);
	UT_RUN(single_extractor_retains_payload_and_failure_contract);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
