/* CLOG record observations through the original descriptor. */
#ifdef FRONTEND
#include "postgres_fe.h"
#else
#include "postgres.h"
#endif
#include "access/clog.h"
#include "access/clog_file.h"
#include "unit_test.h"

UT_DEFINE_GLOBALS();

#ifndef FRONTEND
void
ExceptionalCondition(const char *condition, const char *file, int line)
{
	fprintf(stderr, "%s at %s:%d\n", condition, file, line);
	abort();
}
#endif

static uint8 bytes[8385];
static uint8 payload[8192];
static ClusterClogFileBindingV2 binding;
static ClusterClogPageIdentityV2 page;
static char description[1024];

static void
put_be(uint8 *out, uint64 value, unsigned width)
{
	for (unsigned i = 0; i < width; i++)
		out[i] = (uint8)(value >> (8 * (width - i - 1)));
}

static void
reset(void)
{
	memset(bytes, 0, sizeof(bytes));
	memset(&binding, 0, sizeof(binding));
	binding.identity.kind = CLUSTER_RECOVERY_FILE_CLOG_V2;
	binding.identity.format_id = CLUSTER_RECOVERY_FORMAT_ID_V2;
	binding.identity.format_version = CLUSTER_RECOVERY_FORMAT_VERSION_V2;
	binding.identity.system_identifier = 123;
	binding.identity.database_incarnation = 456;
	binding.identity.storage_uuid[0] = 1;
	binding.identity.redo_branch_uuid[0] = 2;
	binding.identity.redo_branch_generation = 2;
	binding.identity.redo_timeline = 1;
	binding.identity.creation_generation = 3;
	binding.identity.registration_generation = 4;
	binding.identity.object_id = binding.segment = 2;
	memset(&page, 0, sizeof(page));
	page.identity.logical_page = 64;
	page.identity.page_generation = 9;
	page.identity.birth_observed = true;
	page.identity.birth_full_xid = FullTransactionIdFromU64(UINT64CONST(30066868227));
	page.identity.page_lsn = 80;
	page.image_lsn = 100;
	memset(payload, 0x39, sizeof(payload));
}

static bool
record_image(uint8 info)
{
	reset();
	if (!cluster_recovery_file_identity_encode_v2(&binding.identity, bytes, 128))
		return false;
	if (info == 0x40)
		return cluster_clog_file_header_encode_v2(&binding, bytes + 128, 8192);
	if (info == 0x50)
		return cluster_clog_file_page_encode_v2(&binding, &page, payload, 8192, bytes + 128, 8256);
	put_be(bytes + 128, 64, 4);
	put_be(bytes + 132, 2097155, 4);
	put_be(bytes + 136, 17, 4);
	put_be(bytes + 144, UINT64CONST(30066868227), 8);
	return info == 0x60;
}

static const char *
describe(uint8 info, const void *data, Size length)
{
	XLogReaderState reader = {0};
	DecodedXLogRecord record = {0};
	StringInfoData output;

	record.header.xl_info = info;
	record.main_data = (char *)data;
	record.main_data_len = (uint32)length;
	reader.record = &record;
	initStringInfo(&output);
	clog_desc(&output, &reader);
	if ((Size)output.len >= sizeof(description))
		abort();
	memcpy(description, output.data, output.len + 1);
	/* This test links the real frontend StringInfo allocator in both modes. */
	free(output.data);
	return description;
}

UT_TEST(legacy_records_and_unknown_identifiers_remain_distinct)
{
	int zero_page = 42;
	xl_clog_truncate truncate = {64, 2097155, 17};
	UT_ASSERT_STR_EQ(clog_identify(CLOG_ZEROPAGE), "ZEROPAGE");
	UT_ASSERT_STR_EQ(clog_identify(CLOG_TRUNCATE), "TRUNCATE");
	UT_ASSERT_STR_EQ(describe(CLOG_ZEROPAGE, &zero_page, sizeof(zero_page)), "page 42");
	UT_ASSERT_STR_EQ(describe(CLOG_TRUNCATE, &truncate, sizeof(truncate)), "page 64; oldestXact 2097155");
	UT_ASSERT(clog_identify(0x20) == NULL);
	UT_ASSERT_STR_EQ(describe(0x20, NULL, 0), "");
}

UT_TEST(new_identifiers_use_explicit_opcode_family)
{
	UT_ASSERT_STR_EQ(clog_identify(0x40), "SEGMENT_V2");
	UT_ASSERT_STR_EQ(clog_identify(0x50), "PAGE_IMAGE_V2");
	UT_ASSERT_STR_EQ(clog_identify(0x60), "TRUNCATE_V2");
	UT_ASSERT_STR_EQ(clog_identify(0x41), "SEGMENT_V2");
}

UT_TEST(segment_observation_uses_complete_prefix_and_header)
{
	UT_ASSERT(record_image(0x40));
	UT_ASSERT_STR_CONTAINS(describe(0x40, bytes, 8320), "origin 0; segment 2; creation 3; registration 4");
}

UT_TEST(page_observation_keeps_birth_watermark_and_image_lsn_separate)
{
	UT_ASSERT(record_image(0x50));
	UT_ASSERT_STR_CONTAINS(describe(0x50, bytes, 8384), "page 64; generation 9; birth 30066868227");
	UT_ASSERT_STR_CONTAINS(description, "page_lsn 0/50; image_lsn 0/64");
}

UT_TEST(truncate_observation_does_not_infer_full_xid)
{
	UT_ASSERT(record_image(0x60));
	UT_ASSERT_STR_CONTAINS(describe(0x60, bytes, 152), "page 64; oldestXact 2097155; oldestFullXid 30066868227; database 17");
}

UT_TEST(short_trailing_and_missing_new_record_data_are_invalid)
{
	const uint8 kinds[] = {0x40, 0x50, 0x60};
	const Size lengths[] = {8320, 8384, 152};
	for (int i = 0; i < 3; i++) {
		UT_ASSERT(record_image(kinds[i]));
		UT_ASSERT_STR_EQ(describe(kinds[i], bytes, lengths[i] - 1), "invalid CLOG v2 record");
		UT_ASSERT_STR_EQ(describe(kinds[i], bytes, lengths[i] + 1), "invalid CLOG v2 record");
		UT_ASSERT_STR_EQ(describe(kinds[i], NULL, 0), "invalid CLOG v2 record");
	}
}

UT_TEST(complete_segment_and_page_corruption_is_not_printed_as_valid)
{
	const uint8 kinds[] = {0x40, 0x50};
	const Size lengths[] = {8320, 8384};
	for (int k = 0; k < 2; k++) {
		UT_ASSERT(record_image(kinds[k]));
		for (Size i = 0; i < lengths[k]; i++) {
			const char *actual;
			bytes[i] ^= 1;
			actual = describe(kinds[k], bytes, lengths[k]);
			UT_ASSERT_STR_EQ(actual, "invalid CLOG v2 record");
			if (strcmp(actual, "invalid CLOG v2 record") != 0)
				return;
			bytes[i] ^= 1;
		}
	}
}

UT_TEST(prefix_substitution_and_inconsistent_truncate_facts_refuse)
{
	UT_ASSERT(record_image(0x40));
	binding.identity.creation_generation++;
	UT_ASSERT(cluster_recovery_file_identity_encode_v2(&binding.identity, bytes, 128));
	UT_ASSERT_STR_EQ(describe(0x40, bytes, 8320), "invalid CLOG v2 record");
	UT_ASSERT(record_image(0x50));
	binding.identity.registration_generation++;
	UT_ASSERT(cluster_recovery_file_identity_encode_v2(&binding.identity, bytes, 128));
	UT_ASSERT_STR_EQ(describe(0x50, bytes, 8384), "invalid CLOG v2 record");
	UT_ASSERT(record_image(0x60));
	bytes[140] = 1;
	UT_ASSERT_STR_EQ(describe(0x60, bytes, 152), "invalid CLOG v2 record");
	bytes[140] = 0;
	bytes[151] ^= 1;
	UT_ASSERT_STR_EQ(describe(0x60, bytes, 152), "invalid CLOG v2 record");
	bytes[151] ^= 1;
	bytes[131] ^= 1;
	UT_ASSERT_STR_EQ(describe(0x60, bytes, 152), "invalid CLOG v2 record");
}

int
main(void)
{
	UT_PLAN(8);
	UT_RUN(legacy_records_and_unknown_identifiers_remain_distinct);
	UT_RUN(new_identifiers_use_explicit_opcode_family);
	UT_RUN(segment_observation_uses_complete_prefix_and_header);
	UT_RUN(page_observation_keeps_birth_watermark_and_image_lsn_separate);
	UT_RUN(truncate_observation_does_not_infer_full_xid);
	UT_RUN(short_trailing_and_missing_new_record_data_are_invalid);
	UT_RUN(complete_segment_and_page_corruption_is_not_printed_as_valid);
	UT_RUN(prefix_substitution_and_inconsistent_truncate_facts_refuse);
	UT_DONE();
	return ut_failed_count != 0;
}
