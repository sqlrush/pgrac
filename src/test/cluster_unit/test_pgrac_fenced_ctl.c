#include "postgres.h"

#include <stdlib.h>

#undef printf
#undef fprintf
#undef snprintf

#include "unit_test.h"

#include "pgrac_fenced_ctl.h"

UT_DEFINE_GLOBALS();

void *
palloc(Size size)
{
	return malloc(size);
}

void
pfree(void *pointer)
{
	free(pointer);
}

void
ExceptionalCondition(const char *condition_name, const char *file_name, int line_number)
{
	printf("# Assert failed: %s at %s:%d\n", condition_name, file_name, line_number);
	abort();
}

static void
fill_nonzero(uint8 *bytes, size_t len, uint8 value)
{
	memset(bytes, value, len);
}

static void
make_journal_record(PgracFencedJournalRecordV1 *record, uint64 seq, uint8 marker)
{
	memset(record, 0, sizeof(*record));
	record->record_kind = PGRAC_FENCED_JOURNAL_KIND_CONFIG_LOADED;
	record->seq = seq;
	fill_nonzero(record->daemon_boot_id, sizeof(record->daemon_boot_id), marker);
	record->provider_result = PGRAC_FENCED_JOURNAL_PROVIDER_UNAVAILABLE;
	record->event_mono_ns = seq + 100;
	fill_nonzero(record->semantic_config_digest, sizeof(record->semantic_config_digest),
				 marker + 1);
}

UT_TEST(test_prepare_rejoin_request_is_exact_admin_frame)
{
	PgracFencedCtlPrepareRejoinV1 request = { .node_id = 7,
											  .old_incarnation = UINT64_C(0x100000001),
											  .candidate_incarnation = UINT64_C(0x100000002),
											  .timeout_ms = UINT32_C(120000) };
	PgracExternalFenceProtocolRejoinFrameV1 decoded;
	uint8 nonce[PGRAC_EXTERNAL_FENCE_NONCE_V1_BYTES];
	uint8 frame[PGRAC_EXTERNAL_FENCE_REJOIN_V1_BYTES];
	uint8 zero_digest[PGRAC_EXTERNAL_FENCE_PROTOCOL_DIGEST_BYTES] = { 0 };

	fill_nonzero(nonce, sizeof(nonce), 0x31);
	UT_ASSERT(pgrac_fenced_ctl_prepare_rejoin_frame(&request, nonce, frame));
	UT_ASSERT(pgrac_external_fence_rejoin_v1_decode(frame, sizeof(frame), &decoded));
	UT_ASSERT_EQ(decoded.opcode, PGRAC_EXTERNAL_FENCE_REJOIN_ADMIN_PREPARE);
	UT_ASSERT(memcmp(decoded.transport_nonce, nonce, sizeof(nonce)) == 0);
	UT_ASSERT_EQ(decoded.old_node_id, request.node_id);
	UT_ASSERT_EQ(decoded.old_incarnation, request.old_incarnation);
	UT_ASSERT_EQ(decoded.candidate_incarnation, request.candidate_incarnation);
	UT_ASSERT_EQ(decoded.timeout_ms, request.timeout_ms);
	UT_ASSERT_EQ(decoded.status, 0);
	UT_ASSERT_EQ(decoded.deny_reason, 0);
	UT_ASSERT(memcmp(decoded.rejoin_gate_digest, zero_digest, sizeof(zero_digest)) == 0);
	UT_ASSERT(memcmp(decoded.protected_set_digest, zero_digest, sizeof(zero_digest)) == 0);
	UT_ASSERT_EQ(decoded.provider_id, 0);
	UT_ASSERT_EQ(decoded.target_mapping_generation, 0);
}

UT_TEST(test_prepare_rejoin_rejects_invalid_tuple_timeout_and_nonce)
{
	PgracFencedCtlPrepareRejoinV1 request = { .node_id = 1,
											  .old_incarnation = UINT64_C(70),
											  .candidate_incarnation = UINT64_C(71),
											  .timeout_ms = UINT32_C(120000) };
	uint8 nonce[PGRAC_EXTERNAL_FENCE_NONCE_V1_BYTES];
	uint8 frame[PGRAC_EXTERNAL_FENCE_REJOIN_V1_BYTES];

	fill_nonzero(nonce, sizeof(nonce), 0x41);
	UT_ASSERT(pgrac_fenced_ctl_prepare_rejoin_valid(&request));
	request.node_id = -1;
	UT_ASSERT(!pgrac_fenced_ctl_prepare_rejoin_valid(&request));
	request.node_id = 128;
	UT_ASSERT(!pgrac_fenced_ctl_prepare_rejoin_valid(&request));
	request.node_id = 1;
	request.candidate_incarnation = request.old_incarnation;
	UT_ASSERT(!pgrac_fenced_ctl_prepare_rejoin_valid(&request));
	request.candidate_incarnation = request.old_incarnation + 1;
	request.timeout_ms = 0;
	UT_ASSERT(!pgrac_fenced_ctl_prepare_rejoin_valid(&request));
	request.timeout_ms = UINT32_C(600001);
	UT_ASSERT(!pgrac_fenced_ctl_prepare_rejoin_valid(&request));
	request.timeout_ms = UINT32_C(120000);
	memset(nonce, 0, sizeof(nonce));
	UT_ASSERT(!pgrac_fenced_ctl_prepare_rejoin_frame(&request, nonce, frame));
}

UT_TEST(test_prepare_rejoin_response_requires_exact_nonce_and_tuple)
{
	PgracFencedCtlPrepareRejoinV1 request = { .node_id = 1,
											  .old_incarnation = UINT64_C(70),
											  .candidate_incarnation = UINT64_C(77),
											  .timeout_ms = UINT32_C(120000) };
	PgracExternalFenceProtocolRejoinFrameV1 wire;
	PgracExternalFenceProtocolRejoinFrameV1 decoded;
	uint8 nonce[PGRAC_EXTERNAL_FENCE_NONCE_V1_BYTES];
	uint8 frame[PGRAC_EXTERNAL_FENCE_REJOIN_V1_BYTES];

	memset(&wire, 0, sizeof(wire));
	fill_nonzero(nonce, sizeof(nonce), 0x51);
	wire.opcode = PGRAC_EXTERNAL_FENCE_REJOIN_ADMIN_PREPARE_RESULT;
	memcpy(wire.transport_nonce, nonce, sizeof(nonce));
	fill_nonzero(wire.operation_id, sizeof(wire.operation_id), 0x52);
	wire.old_node_id = request.node_id;
	wire.old_incarnation = request.old_incarnation;
	wire.candidate_incarnation = request.candidate_incarnation;
	wire.status = 1;
	UT_ASSERT(pgrac_external_fence_rejoin_v1_encode(&wire, frame));
	UT_ASSERT(pgrac_fenced_ctl_prepare_rejoin_response(&request, nonce, frame, &decoded));
	UT_ASSERT_EQ(decoded.status, 1);

	frame[16] ^= UINT8_C(0x01);
	UT_ASSERT(!pgrac_fenced_ctl_prepare_rejoin_response(&request, nonce, frame, &decoded));
	frame[16] ^= UINT8_C(0x01);
	wire.candidate_incarnation++;
	UT_ASSERT(pgrac_external_fence_rejoin_v1_encode(&wire, frame));
	UT_ASSERT(!pgrac_fenced_ctl_prepare_rejoin_response(&request, nonce, frame, &decoded));
}

UT_TEST(test_prepare_rejoin_closed_negative_is_protocol_valid)
{
	PgracFencedCtlPrepareRejoinV1 request = { .node_id = 1,
											  .old_incarnation = UINT64_C(70),
											  .candidate_incarnation = UINT64_C(77),
											  .timeout_ms = UINT32_C(120000) };
	PgracExternalFenceProtocolRejoinFrameV1 wire;
	PgracExternalFenceProtocolRejoinFrameV1 decoded;
	uint8 nonce[PGRAC_EXTERNAL_FENCE_NONCE_V1_BYTES];
	uint8 frame[PGRAC_EXTERNAL_FENCE_REJOIN_V1_BYTES];

	memset(&wire, 0, sizeof(wire));
	fill_nonzero(nonce, sizeof(nonce), 0x61);
	wire.opcode = PGRAC_EXTERNAL_FENCE_REJOIN_ADMIN_PREPARE_RESULT;
	memcpy(wire.transport_nonce, nonce, sizeof(nonce));
	fill_nonzero(wire.operation_id, sizeof(wire.operation_id), 0x62);
	wire.old_node_id = request.node_id;
	wire.old_incarnation = request.old_incarnation;
	wire.candidate_incarnation = request.candidate_incarnation;
	wire.status = 7;
	wire.deny_reason = 10;
	UT_ASSERT(pgrac_external_fence_rejoin_v1_encode(&wire, frame));
	UT_ASSERT(pgrac_fenced_ctl_prepare_rejoin_response(&request, nonce, frame, &decoded));
	UT_ASSERT_EQ(decoded.status, 7);
	UT_ASSERT_EQ(decoded.deny_reason, 10);
}

UT_TEST(test_verify_journal_requires_exact_root_owned_regular_file)
{
	struct stat st;

	memset(&st, 0, sizeof(st));
	st.st_mode = S_IFREG | 0600;
	st.st_uid = 0;
	st.st_gid = 0;
	st.st_size = PGRAC_FENCED_JOURNAL_RECORD_BYTES;
	UT_ASSERT(pgrac_fenced_ctl_journal_stat_secure(&st));
	st.st_mode = S_IFREG | 0640;
	UT_ASSERT(!pgrac_fenced_ctl_journal_stat_secure(&st));
	st.st_mode = S_IFREG | 0600;
	st.st_uid = 1;
	UT_ASSERT(!pgrac_fenced_ctl_journal_stat_secure(&st));
	st.st_uid = 0;
	st.st_size = PGRAC_FENCED_JOURNAL_SEGMENT_BYTES + 1;
	UT_ASSERT(!pgrac_fenced_ctl_journal_stat_secure(&st));
	st.st_size = PGRAC_FENCED_JOURNAL_RECORD_BYTES + 1;
	UT_ASSERT(!pgrac_fenced_ctl_journal_stat_secure(&st));
}

UT_TEST(test_verify_journal_accepts_non_genesis_segment_and_checks_chain)
{
	PgracFencedJournalRecordV1 first;
	PgracFencedJournalRecordV1 second;
	PgracFencedCtlJournalSummaryV1 summary;
	uint8 frames[PGRAC_FENCED_JOURNAL_RECORD_BYTES * 2];
	uint8 expected_tail[PGRAC_FENCED_JOURNAL_DIGEST_BYTES];

	make_journal_record(&first, 41, 0x71);
	fill_nonzero(first.previous_record_digest, sizeof(first.previous_record_digest), 0x70);
	UT_ASSERT(pgrac_fenced_journal_record_encode(&first, frames));
	make_journal_record(&second, 42, 0x72);
	UT_ASSERT(pgrac_fenced_journal_record_digest(frames, second.previous_record_digest));
	UT_ASSERT(
		pgrac_fenced_journal_record_encode(&second, frames + PGRAC_FENCED_JOURNAL_RECORD_BYTES));
	UT_ASSERT(pgrac_fenced_journal_record_digest(frames + PGRAC_FENCED_JOURNAL_RECORD_BYTES,
												 expected_tail));

	UT_ASSERT(pgrac_fenced_ctl_journal_scan(frames, sizeof(frames), &summary));
	UT_ASSERT_EQ(summary.first_seq, 41);
	UT_ASSERT_EQ(summary.last_seq, 42);
	UT_ASSERT_EQ(summary.record_count, 2);
	UT_ASSERT(memcmp(summary.tail_digest, expected_tail, sizeof(expected_tail)) == 0);

	memset(second.previous_record_digest, 0, sizeof(second.previous_record_digest));
	UT_ASSERT(
		pgrac_fenced_journal_record_encode(&second, frames + PGRAC_FENCED_JOURNAL_RECORD_BYTES));
	UT_ASSERT(!pgrac_fenced_ctl_journal_scan(frames, sizeof(frames), &summary));
}

int
main(void)
{
	UT_PLAN(6);
	UT_RUN(test_prepare_rejoin_request_is_exact_admin_frame);
	UT_RUN(test_prepare_rejoin_rejects_invalid_tuple_timeout_and_nonce);
	UT_RUN(test_prepare_rejoin_response_requires_exact_nonce_and_tuple);
	UT_RUN(test_prepare_rejoin_closed_negative_is_protocol_valid);
	UT_RUN(test_verify_journal_requires_exact_root_owned_regular_file);
	UT_RUN(test_verify_journal_accepts_non_genesis_segment_and_checks_chain);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
