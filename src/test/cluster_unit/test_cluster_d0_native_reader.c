#include "postgres.h"
#include "unit_test.h"
UT_DEFINE_GLOBALS();
#include "test_cluster_d0_native_reader_fixture.h"

UT_TEST(test_completed_native_cross_page_d0_is_full_reference_input)
{
	NativeFixture f;
	ClusterRecoveryCTRCReferenceV2 reference;
	uint8 source[32]; const uint8 *raw; char *error = NULL;
	UT_ASSERT(open_fixture(&f, CLUSTER_RECOVERY_CTRC_WAL_INFO, 0, true));
	if (f.reader == NULL) return;
	UT_ASSERT(XLogReadRecord(f.reader, &error) != NULL);
	UT_ASSERT(error == NULL);
	raw = XLogReaderGetClusterD0RawRecord(f.reader);
	UT_ASSERT(raw != NULL);
	if (raw != NULL) {
		UT_ASSERT(memcmp(raw, f.raw[0], RAW_BYTES) == 0);
		UT_ASSERT(((uintptr_t)XLogRecGetData(f.reader) % MAXIMUM_ALIGNOF) == 0);
		UT_ASSERT(f.reader->EndRecPtr > f.reader->ReadRecPtr + RAW_BYTES);
		memset(source, 6, sizeof(source));
		UT_ASSERT(cluster_recovery_ctrc_reference_v2(f.reader, 17, source, raw, RAW_BYTES, &reference));
		UT_ASSERT_EQ(reference.start, f.reader->ReadRecPtr);
		UT_ASSERT_EQ(reference.end, f.reader->EndRecPtr);
		UT_ASSERT_EQ(reference.raw_length, RAW_BYTES);
	}
	close_fixture(&f);
}

UT_TEST(test_read_ahead_cannot_replace_current_d0_raw)
{
	NativeFixture f; const uint8 *raw; char *error = NULL;
	UT_ASSERT(open_fixture(&f, CLUSTER_RECOVERY_CTRC_WAL_INFO, 0, true));
	if (f.reader == NULL) return;
	UT_ASSERT(XLogReadRecord(f.reader, &error) != NULL);
	raw = XLogReaderGetClusterD0RawRecord(f.reader);
	UT_ASSERT(raw != NULL);
	if (raw != NULL) {
		UT_ASSERT(XLogReadAhead(f.reader, false) != NULL);
		UT_ASSERT(XLogReaderGetClusterD0RawRecord(f.reader) == raw);
		memset(f.reader->readRecordBuf, 0xEE, RAW_BYTES);
		UT_ASSERT(memcmp(raw, f.raw[0], RAW_BYTES) == 0);
		UT_ASSERT(XLogReadRecord(f.reader, &error) != NULL);
		raw = XLogReaderGetClusterD0RawRecord(f.reader);
		UT_ASSERT(raw != NULL);
		if (raw != NULL) UT_ASSERT(memcmp(raw, f.raw[1], RAW_BYTES) == 0);
	}
	close_fixture(&f);
}

UT_TEST(test_current_record_advance_and_begin_invalidate_old_view)
{
	NativeFixture f; const uint8 *raw; char *error = NULL;
	UT_ASSERT(open_fixture(&f, CLUSTER_RECOVERY_CTRC_WAL_INFO, 0, true));
	if (f.reader == NULL) return;
	UT_ASSERT(XLogReadRecord(f.reader, &error) != NULL);
	UT_ASSERT(XLogReadRecord(f.reader, &error) != NULL);
	raw = XLogReaderGetClusterD0RawRecord(f.reader);
	UT_ASSERT(raw != NULL);
	if (raw != NULL) UT_ASSERT(memcmp(raw, f.raw[1], RAW_BYTES) == 0);
	UT_ASSERT(XLogReadRecord(f.reader, &error) != NULL);
	UT_ASSERT(XLogReaderGetClusterD0RawRecord(f.reader) == NULL);
	XLogBeginRead(f.reader, f.first);
	UT_ASSERT(XLogReaderGetClusterD0RawRecord(f.reader) == NULL);
	close_fixture(&f);
}

UT_TEST(test_native_crc_failure_never_exposes_d0_raw)
{
	NativeFixture f; char *error = NULL; uint8 byte;
	UT_ASSERT(open_fixture(&f, CLUSTER_RECOVERY_CTRC_WAL_INFO, 0, true));
	if (f.reader == NULL) return;
	UT_ASSERT_EQ(pread(f.fd, &byte, 1, XLOG_BLCKSZ - 1), 1); byte ^= 1;
	UT_ASSERT_EQ(pwrite(f.fd, &byte, 1, XLOG_BLCKSZ - 1), 1);
	UT_ASSERT(XLogReadRecord(f.reader, &error) == NULL);
	UT_ASSERT(error != NULL);
	UT_ASSERT(XLogReaderGetClusterD0RawRecord(f.reader) == NULL);
	close_fixture(&f);
}

UT_TEST(test_selected_thread_mismatch_has_no_current_raw)
{
	NativeFixture f; char *error = NULL;
	UT_ASSERT(open_fixture(&f, CLUSTER_RECOVERY_CTRC_WAL_INFO, 0, true));
	if (f.reader == NULL) return;
	f.reader->cluster_expected_thread_id = 4;
	UT_ASSERT(XLogReadRecord(f.reader, &error) == NULL);
	UT_ASSERT(error != NULL);
	UT_ASSERT(XLogReaderGetClusterD0RawRecord(f.reader) == NULL);
	close_fixture(&f);
}

UT_TEST(test_other_info_and_outer_xid_never_receive_d0_view)
{
	for (unsigned mode = 0; mode < 2; mode++) {
		NativeFixture f; char *error = NULL;
		UT_ASSERT(open_fixture(&f, CLUSTER_RECOVERY_CTRC_WAL_INFO + (mode == 0), mode == 1, true));
		if (f.reader == NULL) continue;
		UT_ASSERT(XLogReadRecord(f.reader, &error) != NULL);
		UT_ASSERT(XLogReaderGetClusterD0RawRecord(f.reader) == NULL);
		close_fixture(&f);
	}
}

UT_TEST(test_inconsistent_current_metadata_is_readonly_refusal)
{
	NativeFixture f; char *error = NULL; const uint8 *raw;
	UT_ASSERT(open_fixture(&f, CLUSTER_RECOVERY_CTRC_WAL_INFO, 0, true));
	if (f.reader == NULL) return;
	UT_ASSERT(XLogReadRecord(f.reader, &error) != NULL);
	raw = XLogReaderGetClusterD0RawRecord(f.reader);
	UT_ASSERT(raw != NULL);
	if (raw != NULL) {
		DecodedXLogRecord saved;
		XLogRecPtr start = f.reader->ReadRecPtr, end = f.reader->EndRecPtr;
		memcpy(&saved, f.reader->record, sizeof(saved));
		for (unsigned mode = 0; mode < 8; mode++) {
			switch (mode) {
			case 0: f.reader->ReadRecPtr++; break;
			case 1: f.reader->EndRecPtr++; break;
			case 2: f.reader->record->next_lsn++; break;
			case 3: f.reader->record->main_data = (char *)f.reader->record + 1; break;
			case 4: f.reader->record->size = sizeof(saved); break;
			case 5: f.reader->record->header.xl_info ^= 1; break;
			case 6: f.reader->record->main_data_len--; break;
			case 7: f.reader->record->max_block_id = 0; break;
			}
			UT_ASSERT(XLogReaderGetClusterD0RawRecord(f.reader) == NULL);
			memcpy(f.reader->record, &saved, sizeof(saved));
			f.reader->ReadRecPtr = start; f.reader->EndRecPtr = end;
			UT_ASSERT(XLogReaderGetClusterD0RawRecord(f.reader) == raw);
			UT_ASSERT(memcmp(raw, f.raw[0], RAW_BYTES) == 0);
		}
	}
	close_fixture(&f);
}

UT_TEST(test_native_copy_is_not_semantic_d0_qualification)
{
	NativeFixture f; char *error = NULL; const uint8 *raw;
	uint8 source[32]; ClusterRecoveryCTRCReferenceV2 reference;
	UT_ASSERT(open_fixture(&f, CLUSTER_RECOVERY_CTRC_WAL_INFO, 0, false));
	if (f.reader == NULL) return;
	UT_ASSERT(XLogReadRecord(f.reader, &error) != NULL);
	raw = XLogReaderGetClusterD0RawRecord(f.reader);
	UT_ASSERT(raw != NULL);
	if (raw != NULL) {
		memset(source, 6, sizeof(source)); memset(&reference, 0xA5, sizeof(reference));
		UT_ASSERT(!cluster_recovery_ctrc_reference_v2(f.reader, 17, source, raw, RAW_BYTES, &reference));
		UT_ASSERT_EQ(reference.start, 0);
		UT_ASSERT_EQ(reference.raw_length, 0);
	}
	close_fixture(&f);
}

int
main(void)
{
	UT_PLAN(8);
	UT_RUN(test_completed_native_cross_page_d0_is_full_reference_input);
	UT_RUN(test_read_ahead_cannot_replace_current_d0_raw);
	UT_RUN(test_current_record_advance_and_begin_invalidate_old_view);
	UT_RUN(test_native_crc_failure_never_exposes_d0_raw);
	UT_RUN(test_selected_thread_mismatch_has_no_current_raw);
	UT_RUN(test_other_info_and_outer_xid_never_receive_d0_view);
	UT_RUN(test_inconsistent_current_metadata_is_readonly_refusal);
	UT_RUN(test_native_copy_is_not_semantic_d0_qualification);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
