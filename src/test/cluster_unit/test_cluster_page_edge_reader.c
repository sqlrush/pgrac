/*-------------------------------------------------------------------------
 *
 * test_cluster_page_edge_reader.c
 *    STOP-06 PageVersion-edge integration with the sole core WAL decoder.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/xloginsert.h"
#include "access/xlogreader.h"

#include "unit_test.h"

UT_DEFINE_GLOBALS();

/* Matches xlogreader.c's private MAX_ERRORMSG_LEN plus its terminator. */
#define TEST_ERROR_BUFFER_SIZE 1001

void
ExceptionalCondition(const char *condition_name, const char *file_name, int line_number)
{
	printf("# Assert failed: %s at %s:%d\n", condition_name, file_name, line_number);
	abort();
}

int
errcode(int sqlerrcode pg_attribute_unused())
{
	return 0;
}

int
errmsg(const char *fmt pg_attribute_unused(), ...)
{
	return 0;
}

int
errmsg_internal(const char *fmt pg_attribute_unused(), ...)
{
	return 0;
}

bool
errstart_cold(int elevel pg_attribute_unused(), const char *domain pg_attribute_unused())
{
	return false;
}

void
errfinish(const char *filename pg_attribute_unused(), int lineno pg_attribute_unused(),
		  const char *funcname pg_attribute_unused())
{}

static void
set_incarnation(uint8 incarnation[16], uint8 seed)
{
	int i;

	for (i = 0; i < 16; i++)
		incarnation[i] = seed + i;
}

static void
make_edge(RfPageVersionEdgeV1 *edge)
{
	memset(edge, 0, sizeof(*edge));
	edge->entry_count = 1;
	edge->result_token = 99;
	edge->entries[0].block_id = 0;
	edge->entries[0].page_class = RF_PAGE_CLASS_ORDINARY;
	edge->entries[0].before_kind = RF_PAGE_STATE_PRESENT;
	edge->entries[0].result_kind = RF_PAGE_STATE_PRESENT;
	edge->entries[0].component_ordinal = 0;
	set_incarnation(edge->entries[0].before.segment_incarnation, 7);
	edge->entries[0].before.mutation_token = 41;
	set_incarnation(edge->entries[0].result_incarnation, 7);
}

static size_t
append_block_ref(uint8 *target, uint8 block_id)
{
	XLogRecordBlockHeader header;
	RelFileLocator locator = { 1663, 5, 17 };
	BlockNumber block = 23;
	uint8 *start = target;

	memset(&header, 0, sizeof(header));
	header.id = block_id;
	header.fork_flags = MAIN_FORKNUM;
	memcpy(target, &header, SizeOfXLogRecordBlockHeader);
	target += SizeOfXLogRecordBlockHeader;
	memcpy(target, &locator, sizeof(locator));
	target += sizeof(locator);
	memcpy(target, &block, sizeof(block));
	target += sizeof(block);
	return target - start;
}

static XLogRecord *
build_record(bool edge_first, bool edge_block_mismatch, size_t *record_size)
{
	RfPageVersionEdgeV1 edge;
	XLogRecord *record;
	uint8 edge_wire[XLR_PAGE_VERSION_EDGE_MAX_SIZE];
	uint8 *target;
	size_t edge_len;
	size_t block_len;

	make_edge(&edge);
	if (edge_block_mismatch)
		edge.entries[0].block_id = 1;
	UT_ASSERT(XLogEncodePageVersionEdgeV1(edge_wire, sizeof(edge_wire), edge.result_token,
										  edge.entries, edge.entry_count, &edge_len));
	block_len = SizeOfXLogRecordBlockHeader + sizeof(RelFileLocator) + sizeof(BlockNumber);
	*record_size = SizeOfXLogRecord + edge_len + block_len;
	record = calloc(1, *record_size);
	UT_ASSERT(record != NULL);
	if (record == NULL)
		abort();
	record->xl_tot_len = *record_size;
	target = (uint8 *)record + SizeOfXLogRecord;
	if (edge_first) {
		memcpy(target, edge_wire, edge_len);
		target += edge_len;
		target += append_block_ref(target, 0);
	} else {
		target += append_block_ref(target, 0);
		memcpy(target, edge_wire, edge_len);
		target += edge_len;
	}
	UT_ASSERT_EQ((size_t)(target - (uint8 *)record), *record_size);
	return record;
}

static XLogRecord *
build_block_only_record(size_t *record_size)
{
	XLogRecord *record;
	uint8 *target;

	*record_size = SizeOfXLogRecord + SizeOfXLogRecordBlockHeader + sizeof(RelFileLocator)
				   + sizeof(BlockNumber);
	record = calloc(1, *record_size);
	UT_ASSERT(record != NULL);
	if (record == NULL)
		abort();
	record->xl_tot_len = *record_size;
	target = (uint8 *)record + SizeOfXLogRecord;
	target += append_block_ref(target, 0);
	UT_ASSERT_EQ((size_t)(target - (uint8 *)record), *record_size);
	return record;
}

static bool
decode_record(XLogRecord *record, DecodedXLogRecord **decoded_out)
{
	XLogReaderState state;
	DecodedXLogRecord *decoded;
	char error_buffer[TEST_ERROR_BUFFER_SIZE];
	char *errormsg = NULL;

	memset(&state, 0, sizeof(state));
	state.errormsg_buf = error_buffer;
	error_buffer[0] = '\0';
	state.ReadRecPtr = UINT64_C(0x1000000);
	decoded = calloc(1, DecodeXLogRecordRequiredSpace(record->xl_tot_len));
	UT_ASSERT(decoded != NULL);
	if (!DecodeXLogRecord(&state, decoded, record, state.ReadRecPtr, &errormsg)) {
		free(decoded);
		*decoded_out = NULL;
		return false;
	}
	*decoded_out = decoded;
	return true;
}

UT_TEST(test_reader_decodes_first_fragment_page_edge)
{
	DecodedXLogRecord *decoded;
	XLogRecord *record;
	size_t record_size;

	record = build_record(true, false, &record_size);
	UT_ASSERT(decode_record(record, &decoded));
	UT_ASSERT(decoded->has_page_version_edge);
	UT_ASSERT_EQ(decoded->page_version_edge.entry_count, 1);
	UT_ASSERT_EQ(decoded->page_version_edge.result_token, 99);
	UT_ASSERT_EQ(decoded->blocks[0].component_ordinal, 0);
	free(decoded);
	free(record);
}

UT_TEST(test_reader_rejects_page_edge_after_block_ref)
{
	DecodedXLogRecord *decoded;
	XLogRecord *record;
	size_t record_size;

	record = build_record(false, false, &record_size);
	UT_ASSERT(!decode_record(record, &decoded));
	free(record);
}

UT_TEST(test_reader_rejects_edge_block_set_mismatch)
{
	DecodedXLogRecord *decoded;
	XLogRecord *record;
	size_t record_size;

	record = build_record(true, true, &record_size);
	UT_ASSERT(!decode_record(record, &decoded));
	free(record);
}

UT_TEST(test_reader_owns_full_page_version_equality)
{
	RfPageVersionV1 left;
	RfPageVersionV1 right;

	memset(&left, 0, sizeof(left));
	set_incarnation(left.segment_incarnation, 3);
	left.mutation_token = 9;
	right = left;
	UT_ASSERT(rf_page_version_equal_v1(&left, &right));
	right.segment_incarnation[15] ^= UINT8_C(0xff);
	UT_ASSERT(!rf_page_version_equal_v1(&left, &right));
	right = left;
	right.mutation_token++;
	UT_ASSERT(!rf_page_version_equal_v1(&left, &right));
}

UT_TEST(test_reader_rejects_stale_and_malformed_edge_headers)
{
	DecodedXLogRecord *decoded;
	XLogRecord *record;
	uint8 *edge_wire;
	size_t record_size;
	uint16 one = 1;

	record = build_record(true, false, &record_size);
	edge_wire = (uint8 *)record + SizeOfXLogRecord;
	edge_wire[3] = 32;
	UT_ASSERT(!decode_record(record, &decoded));
	free(record);

	record = build_record(true, false, &record_size);
	edge_wire = (uint8 *)record + SizeOfXLogRecord;
	memcpy(edge_wire + 4, &one, sizeof(one));
	UT_ASSERT(!decode_record(record, &decoded));
	free(record);

	record = build_record(true, false, &record_size);
	edge_wire = (uint8 *)record + SizeOfXLogRecord;
	memcpy(edge_wire + XLR_PAGE_VERSION_EDGE_HEADER_SIZE + 4, &one, sizeof(one));
	UT_ASSERT(!decode_record(record, &decoded));
	free(record);
}

UT_TEST(test_reader_rejects_init_flag_without_block_declaration)
{
	DecodedXLogRecord *decoded;
	XLogRecord *record;
	uint8 *entry_wire;
	uint16 init_flags = RF_PAGE_EDGE_WILL_INIT | RF_PAGE_EDGE_FULL_COVERAGE;
	size_t record_size;
	uint64 zero = 0;

	record = build_record(true, false, &record_size);
	entry_wire = (uint8 *)record + SizeOfXLogRecord + XLR_PAGE_VERSION_EDGE_HEADER_SIZE;
	entry_wire[2] = RF_PAGE_STATE_UNFORMATTED;
	memcpy(entry_wire + 4, &init_flags, sizeof(init_flags));
	memcpy(entry_wire + 24, &zero, sizeof(zero));
	UT_ASSERT(!decode_record(record, &decoded));
	free(record);
}

UT_TEST(test_reader_keeps_missing_edge_activation_neutral)
{
	DecodedXLogRecord *decoded;
	XLogRecord *record;
	size_t record_size;

	record = build_block_only_record(&record_size);
	UT_ASSERT(decode_record(record, &decoded));
	UT_ASSERT(!decoded->has_page_version_edge);
	free(decoded);
	free(record);
}

int
main(void)
{
	UT_PLAN(7);
	UT_RUN(test_reader_decodes_first_fragment_page_edge);
	UT_RUN(test_reader_rejects_page_edge_after_block_ref);
	UT_RUN(test_reader_rejects_edge_block_set_mismatch);
	UT_RUN(test_reader_owns_full_page_version_equality);
	UT_RUN(test_reader_rejects_stale_and_malformed_edge_headers);
	UT_RUN(test_reader_rejects_init_flag_without_block_declaration);
	UT_RUN(test_reader_keeps_missing_edge_activation_neutral);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
