/*-------------------------------------------------------------------------
 *
 * test_pgrac_fenced_journal.c
 *	  RF-ROOT P4 exact local journal tests.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <fcntl.h>
#include <stdlib.h>
#include <sys/stat.h>
#include <unistd.h>

#include "pgrac_fenced_journal.h"
#include "pgrac_fenced_runtime.h"
#include "pgrac_fenced_ctl.h"
#include "common/pgrac_external_fence_protocol.h"
#include "port/pg_crc32c.h"

#undef printf
#undef fprintf
#undef snprintf

#include "unit_test.h"

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
make_config_record(PgracFencedJournalRecordV1 *record, uint64 seq)
{
	memset(record, 0, sizeof(*record));
	record->record_kind = PGRAC_FENCED_JOURNAL_KIND_CONFIG_LOADED;
	record->seq = seq;
	fill_nonzero(record->daemon_boot_id, sizeof(record->daemon_boot_id), 0x11);
	record->event_mono_ns = 100;
	record->provider_result = PGRAC_FENCED_JOURNAL_PROVIDER_UNAVAILABLE;
	fill_nonzero(record->semantic_config_digest, sizeof(record->semantic_config_digest), 0x22);
}

UT_TEST(test_journal_exact_codec_roundtrip)
{
	PgracFencedJournalRecordV1 record;
	PgracFencedJournalRecordV1 decoded;
	uint8 frame[PGRAC_FENCED_JOURNAL_RECORD_BYTES];

	make_config_record(&record, 1);
	UT_ASSERT(pgrac_fenced_journal_record_encode(&record, frame));
	UT_ASSERT_EQ(frame[0], 'P');
	UT_ASSERT_EQ(frame[1], 'F');
	UT_ASSERT_EQ(frame[2], 'G');
	UT_ASSERT_EQ(frame[3], 'J');
	UT_ASSERT_EQ(frame[248], 'P');
	UT_ASSERT_EQ(frame[249], 'F');
	UT_ASSERT_EQ(frame[250], 'G');
	UT_ASSERT_EQ(frame[251], 'Z');
	UT_ASSERT(pgrac_fenced_journal_record_decode(frame, sizeof(frame), &decoded));
	UT_ASSERT_EQ(decoded.record_kind, record.record_kind);
	UT_ASSERT_EQ(decoded.seq, record.seq);
	UT_ASSERT(memcmp(decoded.daemon_boot_id, record.daemon_boot_id, sizeof(record.daemon_boot_id))
			  == 0);
	UT_ASSERT(memcmp(decoded.semantic_config_digest, record.semantic_config_digest,
					 sizeof(record.semantic_config_digest))
			  == 0);
}

/* Author: SqlRush <sqlrush@gmail.com>
 * Independent envelope fixture: catches refusal/loss of the original need at
 * restart. The existing event/request codecs are not the new envelope codec.
 */
static void
fixture_u32(uint8 *out, uint32 value)
{
	out[0] = value;
	out[1] = value >> 8;
	out[2] = value >> 16;
	out[3] = value >> 24;
}

static void
fixture_crc(uint8 *frame, size_t crc_offset)
{
	pg_crc32c crc;

	INIT_CRC32C(crc);
	COMP_CRC32C(crc, frame, crc_offset);
	FIN_CRC32C(crc);
	fixture_u32(frame + crc_offset, crc);
}

static bool
make_intent_fixture(uint8 frame[768])
{
	PgracFencedJournalRecordV1 event;
	PgracExternalFenceProtocolRequestV1 request;
	PgracExternalFenceProtocolBindingV1 binding;

	make_config_record(&event, 1);
	event.record_kind = PGRAC_FENCED_JOURNAL_KIND_ACTUATION_ISSUED;
	event.provider_id = 257;
	event.provider_abi_version = 1;
	event.mapping_generation = 7;
	memset(event.operation_id, 0x33, 16);
	memset(&request, 0, sizeof(request));
	memset(request.request_nonce, 0x44, 16);
	request.need.system_identifier = 123456789;
	memset(request.need.canonical_duty_digest, 0x55, 32);
	request.need.victim_node_id = 2;
	request.need.victim_incarnation = 91;
	memset(request.need.protected_set_digest, 0x66, 32);
	request.need.predicate_id = 1;
	request.need.predicate_version = 1;
	request.timeout_ms = 30000;
	if (!pgrac_external_fence_binding_from_request_v1(&request.need, 7, &binding)
		|| !pgrac_external_fence_binding_digest_v1(&binding, event.binding_digest))
		return false;
	memset(frame, 0, 768);
	memcpy(frame, "PFG2", 4);
	frame[4] = 2;
	frame[6] = 1;
	fixture_u32(frame + 8, 768);
	if (!pgrac_fenced_journal_record_encode(&event, frame + 16)
		|| !pgrac_external_fence_request_v1_encode(&request, frame + 272))
		return false;
	memset(frame + 528, 0x77, 16);
	frame[544] = 3;
	fixture_u32(frame + 552, 123456789);
	memset(frame + 560, 0x66, 32);
	memcpy(frame + 760, "PF2Z", 4);
	fixture_crc(frame, 764);
	return true;
}

UT_TEST(test_restart_decodes_exact_persisted_intent)
{
	uint8 frame[768];
	PgracFencedJournalRecordV1 decoded;

	memset(&decoded, 0, sizeof(decoded));
	UT_ASSERT(make_intent_fixture(frame));
	UT_ASSERT(pgrac_fenced_journal_record_decode(frame, sizeof(frame), &decoded));
	UT_ASSERT_EQ(decoded.seq, 1);
	UT_ASSERT_EQ(decoded.mapping_generation, 7);
	UT_ASSERT_EQ(decoded.intent.kind, PGRAC_FENCED_JOURNAL_INTENT_ACQUIRE);
	UT_ASSERT_EQ(decoded.intent.attempt, 3);
	UT_ASSERT_EQ(decoded.intent.system_identifier, 123456789);
	UT_ASSERT_EQ(decoded.intent.request.acquire.need.victim_incarnation, 91);
	UT_ASSERT_EQ(decoded.intent.request.acquire.need.victim_node_id, 2);
	UT_ASSERT_EQ(decoded.intent.request.acquire.request_nonce[0], 0x44);
	UT_ASSERT_EQ(decoded.operation_id[0], 0x33);
}

UT_TEST(test_intent_codec_keeps_complete_identity_and_capacity)
{
	uint8 fixture[768], encoded[768], small[768];
	PgracFencedJournalRecordV1 record;
	size_t written = 99;

	UT_ASSERT(make_intent_fixture(fixture));
	UT_ASSERT(pgrac_fenced_journal_record_decode(fixture, sizeof(fixture), &record));
	UT_ASSERT(pgrac_fenced_journal_frame_encode(&record, encoded, sizeof(encoded), &written));
	UT_ASSERT_EQ(written, 768);
	UT_ASSERT(memcmp(encoded, fixture, 768) == 0);
	/* A legacy call must never silently discard the newly persisted need. */
	UT_ASSERT(!pgrac_fenced_journal_record_encode(&record, encoded));
	memset(small, 0xcc, sizeof(small));
	UT_ASSERT(!pgrac_fenced_journal_frame_encode(&record, small, 767, &written));
	UT_ASSERT_EQ(written, 0);
	for (size_t i = 0; i < sizeof(small); ++i)
		UT_ASSERT_EQ(small[i], 0xcc);
}

UT_TEST(test_intent_corruption_and_truncation_clear_output)
{
	uint8 frame[768];
	PgracFencedJournalRecordV1 record, zero = { 0 };

	UT_ASSERT(make_intent_fixture(frame));
	for (size_t i = 0; i < sizeof(frame); ++i) {
		memset(&record, 0xee, sizeof(record));
		UT_ASSERT(!pgrac_fenced_journal_record_decode(frame, i, &record));
		UT_ASSERT(memcmp(&record, &zero, sizeof(record)) == 0);
		frame[i] ^= 1;
		memset(&record, 0xee, sizeof(record));
		UT_ASSERT(!pgrac_fenced_journal_record_decode(frame, sizeof(frame), &record));
		UT_ASSERT(memcmp(&record, &zero, sizeof(record)) == 0);
		frame[i] ^= 1;
	}
}

UT_TEST(test_intent_identity_is_checked_beyond_crc)
{
	/* Each corruption has its CRC repaired; refusal must be semantic. */
	const size_t offsets[] = { 6, 12, 432, 544, 552, 560, 592 };
	uint8 frame[768];
	PgracFencedJournalRecordV1 record;

	for (size_t i = 0; i < lengthof(offsets); ++i) {
		UT_ASSERT(make_intent_fixture(frame));
		frame[offsets[i]] = offsets[i] == 544 ? 0 : 0xff;
		fixture_crc(frame, 764);
		UT_ASSERT(!pgrac_fenced_journal_record_decode(frame, sizeof(frame), &record));
	}
	UT_ASSERT(make_intent_fixture(frame));
	memset(frame + 528, 0, 16);
	fixture_crc(frame, 764);
	UT_ASSERT(!pgrac_fenced_journal_record_decode(frame, sizeof(frame), &record));
	UT_ASSERT(make_intent_fixture(frame));
	frame[16 + 152] = 8; /* event mapping disagrees with its binding digest */
	fixture_crc(frame + 16, 252);
	fixture_crc(frame, 764);
	UT_ASSERT(!pgrac_fenced_journal_record_decode(frame, sizeof(frame), &record));
}

static bool
make_rejoin_fixture(uint8 frame[768], uint16 opcode, int32 node, uint64 old_incarnation,
					uint64 candidate_incarnation)
{
	PgracExternalFenceProtocolRejoinFrameV1 request;
	PgracExternalFenceProtocolRejoinBindingV1 binding;
	PgracFencedJournalRecordV1 event;

	if (!make_intent_fixture(frame) || !pgrac_fenced_journal_record_decode(frame + 16, 256, &event))
		return false;
	memset(&request, 0, sizeof(request));
	memset(&binding, 0, sizeof(binding));
	request.opcode = opcode;
	memset(request.transport_nonce, 0x44, 16);
	request.old_node_id = binding.old_node_id = node;
	request.old_incarnation = binding.old_incarnation = old_incarnation;
	request.candidate_incarnation = binding.candidate_incarnation = candidate_incarnation;
	request.timeout_ms = 30000;
	binding.system_identifier = 123456789;
	binding.target_mapping_generation = 7;
	memset(binding.protected_set_digest, 0x66, 32);
	binding.predicate_id = 2;
	binding.predicate_version = 1;
	if (opcode != PGRAC_EXTERNAL_FENCE_REJOIN_ADMIN_PREPARE) {
		memset(request.operation_id, 0x33, 16);
		request.system_identifier = binding.system_identifier;
		memset(request.rejoin_gate_digest, 0x55, 32);
		memset(binding.rejoin_gate_digest, 0x55, 32);
		memset(request.protected_set_digest, 0x66, 32);
	}
	event.record_kind = PGRAC_FENCED_JOURNAL_KIND_REENABLE_REQUESTED;
	frame[6] = 2;
	if (!pgrac_external_fence_rejoin_binding_digest_v1(&binding, event.binding_digest)
		|| !pgrac_fenced_journal_record_encode(&event, frame + 16)
		|| !pgrac_external_fence_rejoin_v1_encode(&request, frame + 272))
		return false;
	fixture_crc(frame, 764);
	return true;
}

UT_TEST(test_intent_rejoin_preserves_admin_and_on_identities)
{
	const uint16 opcodes[] = { 1, 5, 7 };
	uint8 frame[768], encoded[768];
	PgracFencedJournalRecordV1 record;
	size_t written;

	for (size_t i = 0; i < lengthof(opcodes); ++i) {
		UT_ASSERT(make_rejoin_fixture(frame, opcodes[i], 2, 91, 92));
		UT_ASSERT(pgrac_fenced_journal_record_decode(frame, sizeof(frame), &record));
		UT_ASSERT_EQ(record.intent.request.rejoin.old_incarnation, 91);
		UT_ASSERT_EQ(record.intent.request.rejoin.candidate_incarnation, 92);
		UT_ASSERT_EQ(record.intent.request.rejoin.opcode, opcodes[i]);
		UT_ASSERT(pgrac_fenced_journal_frame_encode(&record, encoded, 768, &written));
		UT_ASSERT_EQ(written, 768);
		UT_ASSERT(memcmp(encoded, frame, 768) == 0);
		/* Correct CRC cannot let an ON authorization change operation or sysid. */
		record.intent.system_identifier++;
		UT_ASSERT(!pgrac_fenced_journal_frame_encode(&record, encoded, 768, &written));
	}
}

/* A legitimate new phase must not be mistaken for mutation of an old callback. */
static void
make_rejoin_phase(uint16 opcode, uint64 seq, uint64 attempt, PgracFencedJournalRecordV1 *record)
{
	uint8 frame[768];

	UT_ASSERT(make_rejoin_fixture(frame, opcode, 2, 91, 92));
	UT_ASSERT(pgrac_fenced_journal_record_decode(frame, sizeof(frame), record));
	record->seq = seq;
	record->intent.attempt = attempt;
	record->record_kind = PGRAC_FENCED_JOURNAL_KIND_REQUEST_ACCEPTED;
}

static void
rejoin_phase_proof(PgracFencedJournalRecordV1 *record, bool on)
{
	record->record_kind = on ? PGRAC_FENCED_JOURNAL_KIND_REENABLE_RESULT
							 : PGRAC_FENCED_JOURNAL_KIND_READBACK_RESULT;
	record->provider_result = PGRAC_FENCED_JOURNAL_PROVIDER_OK;
	record->target_state = on ? PGRAC_FENCED_JOURNAL_TARGET_ON : PGRAC_FENCED_JOURNAL_TARGET_OFF;
	record->io_drain_state = 1;
	record->deny_reason = 0;
	record->proof_generation = 7;
	record->fresh_until_mono_ns = record->event_mono_ns + 1000;
	UT_ASSERT(pgrac_external_fence_target_state_digest_v1(
		record->intent.target_uuid, record->target_state, record->io_drain_state,
		record->mapping_generation, record->proof_generation, record->target_state_digest));
}

UT_TEST(test_rejoin_successor_phases_preserve_one_owned_identity)
{
	PgracFencedJournalRecordV1 admin, authorize, refresh;

	make_rejoin_phase(PGRAC_EXTERNAL_FENCE_REJOIN_ADMIN_PREPARE, 1, 1, &admin);
	rejoin_phase_proof(&admin, false);
	make_rejoin_phase(PGRAC_EXTERNAL_FENCE_REJOIN_LMON_AUTHORIZE_ON, 2, 2, &authorize);
	authorize.intent.request.rejoin.transport_nonce[0] = 0x71;
	authorize.intent.request.rejoin.timeout_ms = 1000;
	UT_ASSERT(pgrac_fenced_journal_intent_continues(&admin, &authorize));
	rejoin_phase_proof(&authorize, true);
	make_rejoin_phase(PGRAC_EXTERNAL_FENCE_REJOIN_LMON_REFRESH_ON, 3, 3, &refresh);
	refresh.intent.request.rejoin.transport_nonce[0] = 0x72;
	refresh.intent.request.rejoin.timeout_ms = 500;
	UT_ASSERT(pgrac_fenced_journal_intent_continues(&authorize, &refresh));
	/* REFRESH creates new evidence; waiting for the joiner can outlive ON's proof. */
	refresh.event_mono_ns = authorize.fresh_until_mono_ns + 1;
	UT_ASSERT(pgrac_fenced_journal_intent_continues(&authorize, &refresh));
	/* AUTHORIZE still cannot actuate using an expired OFF offer. */
	authorize.record_kind = PGRAC_FENCED_JOURNAL_KIND_REQUEST_ACCEPTED;
	authorize.event_mono_ns = admin.fresh_until_mono_ns;
	UT_ASSERT(!pgrac_fenced_journal_intent_continues(&admin, &authorize));
}

UT_TEST(test_rejoin_phase_cannot_skip_or_replace_authority)
{
	PgracFencedJournalRecordV1 admin, authorize, refresh, bad;

	make_rejoin_phase(PGRAC_EXTERNAL_FENCE_REJOIN_ADMIN_PREPARE, 1, 1, &admin);
	rejoin_phase_proof(&admin, false);
	make_rejoin_phase(PGRAC_EXTERNAL_FENCE_REJOIN_LMON_AUTHORIZE_ON, 2, 2, &authorize);
	make_rejoin_phase(PGRAC_EXTERNAL_FENCE_REJOIN_LMON_REFRESH_ON, 2, 2, &refresh);
	UT_ASSERT(!pgrac_fenced_journal_intent_continues(&admin, &refresh));
	for (int variant = 0; variant < 8; ++variant) {
		bad = authorize;
		switch (variant) {
		case 0:
			bad.intent.attempt = 1;
			break;
		case 1:
			bad.intent.attempt = 3;
			break;
		case 2:
			bad.record_kind = PGRAC_FENCED_JOURNAL_KIND_ACTUATION_ISSUED;
			break;
		case 3:
			bad.daemon_boot_id[0] ^= 1;
			break;
		case 4:
			bad.intent.target_uuid[0] ^= 1;
			break;
		case 5:
			bad.intent.request.rejoin.candidate_incarnation++;
			break;
		case 6:
			bad.intent.request.rejoin.old_incarnation++;
			break;
		case 7:
			bad.semantic_config_digest[0] ^= 1;
			break;
		}
		UT_ASSERT(!pgrac_fenced_journal_intent_continues(&admin, &bad));
	}
	for (int variant = 0; variant < 6; ++variant) {
		bad = admin;
		switch (variant) {
		case 0:
			bad.provider_result = PGRAC_FENCED_JOURNAL_PROVIDER_UNKNOWN;
			break;
		case 1:
			bad.io_drain_state = 0;
			break;
		case 2:
			bad.deny_reason = 9;
			break;
		case 3:
			bad.proof_generation = 0;
			break;
		case 4:
			bad.target_state_digest[0] ^= 1;
			break;
		case 5:
			bad.record_kind = PGRAC_FENCED_JOURNAL_KIND_REQUEST_ACCEPTED;
			break;
		}
		UT_ASSERT(!pgrac_fenced_journal_intent_continues(&bad, &authorize));
	}
	rejoin_phase_proof(&authorize, true);
	refresh.seq = 3;
	refresh.intent.attempt = 3;
	bad = refresh;
	bad.intent.request.rejoin.rejoin_gate_digest[0] ^= 1;
	UT_ASSERT(!pgrac_fenced_journal_intent_continues(&authorize, &bad));
	bad = authorize;
	bad.seq++;
	bad.intent.request.rejoin.transport_nonce[0] ^= 1;
	UT_ASSERT(!pgrac_fenced_journal_intent_continues(&authorize, &bad));
}

UT_TEST(test_rejoin_admin_intent_rejects_invalid_identity)
{
	const struct {
		int32 node;
		uint64 old;
		uint64 candidate;
	} invalid[] = { { -1, 91, 92 }, { 128, 91, 92 }, { 2, 0, 1 },
					{ 2, 91, 0 },	{ 2, 91, 91 },	 { 2, 91, 90 } };
	uint8 frame[768];
	PgracFencedJournalRecordV1 record;

	for (size_t i = 0; i < lengthof(invalid); ++i) {
		UT_ASSERT(
			make_rejoin_fixture(frame, 1, invalid[i].node, invalid[i].old, invalid[i].candidate));
		UT_ASSERT(!pgrac_fenced_journal_record_decode(frame, 768, &record));
	}
}

UT_TEST(test_bootstrap_detects_byte_full_mixed_segment)
{
	PgracFencedJournalScanState state;

	pgrac_fenced_journal_scan_state_init(&state);
	state.segment_record_count = 87382;
	state.valid_bytes = UINT64_C(87381) * 768 + 256;
	/* A CONFIG_LOADED append happens before operation rotation is attached. */
	UT_ASSERT(!pgrac_fenced_journal_has_room(&state, 256));
	state.valid_bytes -= 256;
	UT_ASSERT(pgrac_fenced_journal_has_room(&state, 256));
	UT_ASSERT(!pgrac_fenced_journal_has_room(&state, 768));
}

UT_TEST(test_mixed_journal_hashes_the_entire_intent)
{
	uint8 frames[1024];
	PgracFencedJournalRecordV1 second;
	PgracFencedJournalScanState state;
	PgracFencedCtlJournalSummaryV1 summary;

	UT_ASSERT(make_intent_fixture(frames));
	make_config_record(&second, 2);
	UT_ASSERT(pgrac_fenced_journal_frame_digest(frames, 768, second.previous_record_digest));
	UT_ASSERT(pgrac_fenced_journal_record_encode(&second, frames + 768));
	pgrac_fenced_journal_scan_state_init(&state);
	UT_ASSERT_EQ(pgrac_fenced_journal_scan_bytes(frames, sizeof(frames), false, &state),
				 PGRAC_FENCED_JOURNAL_SCAN_OK);
	UT_ASSERT_EQ(state.next_seq, 3);
	UT_ASSERT_EQ(state.segment_record_count, 2);
	UT_ASSERT_EQ(state.valid_bytes, 1024);
	UT_ASSERT_EQ(state.last_record_bytes, 256);
	UT_ASSERT(pgrac_fenced_ctl_journal_scan(frames, sizeof(frames), &summary));
	UT_ASSERT_EQ(summary.record_count, 2);
	UT_ASSERT_EQ(summary.last_seq, 2);
	frames[528] ^= 1; /* valid but different target, covered by the next chain hash */
	fixture_crc(frames, 764);
	pgrac_fenced_journal_scan_state_init(&state);
	UT_ASSERT_EQ(pgrac_fenced_journal_scan_bytes(frames, sizeof(frames), false, &state),
				 PGRAC_FENCED_JOURNAL_SCAN_CORRUPT);
}

UT_TEST(test_intent_append_reopen_retains_the_original_need)
{
	char path[] = "/tmp/pgrac-fenced-intent.XXXXXX";
	uint8 frame[768];
	PgracFencedJournalRecordV1 record, last;
	PgracFencedJournalScanState state, loaded;
	PgracFencedJournalReconcileState reconcile;
	bool have_last = false;
	struct stat st;
	int fd = mkstemp(path);

	UT_ASSERT(fd >= 0);
	UT_ASSERT_EQ(fcntl(fd, F_SETFL, O_APPEND), 0);
	memset(&last, 0, sizeof(last));
	UT_ASSERT(make_intent_fixture(frame));
	UT_ASSERT(pgrac_fenced_journal_record_decode(frame, 768, &record));
	pgrac_fenced_journal_scan_state_init(&state);
	UT_ASSERT_EQ(pgrac_fenced_journal_append_fd(fd, &state, &record),
				 PGRAC_FENCED_JOURNAL_APPEND_OK);
	UT_ASSERT_EQ(fstat(fd, &st), 0);
	UT_ASSERT_EQ(st.st_size, 768);
	UT_ASSERT_EQ(close(fd), 0);
	fd = open(path, O_RDWR | O_APPEND);
	UT_ASSERT(fd >= 0);
	pgrac_fenced_journal_scan_state_init(&loaded);
	pgrac_fenced_journal_reconcile_state_init(&reconcile);
	UT_ASSERT(
		pgrac_fenced_journal_load_active_reconcile_fd(fd, &loaded, &last, &have_last, &reconcile));
	UT_ASSERT(have_last);
	UT_ASSERT_EQ(reconcile.pending_count, 1);
	UT_ASSERT_EQ(reconcile.pending[0].last_record.intent.request.acquire.need.victim_incarnation,
				 91);
	UT_ASSERT_EQ(last.intent.attempt, 3);
	UT_ASSERT_EQ(close(fd), 0);
	UT_ASSERT_EQ(unlink(path), 0);
}

UT_TEST(test_intent_partial_tail_is_not_a_completed_operation)
{
	char path[] = "/tmp/pgrac-fenced-intent-tail.XXXXXX";
	uint8 frame[768];
	PgracFencedJournalScanState state;
	struct stat st;
	int fd = mkstemp(path);

	UT_ASSERT(fd >= 0);
	UT_ASSERT_EQ(fcntl(fd, F_SETFL, O_APPEND), 0);
	UT_ASSERT(make_intent_fixture(frame));
	UT_ASSERT_EQ(write(fd, frame, 767), 767);
	pgrac_fenced_journal_scan_state_init(&state);
	UT_ASSERT_EQ(pgrac_fenced_journal_scan_bytes(frame, 767, true, &state),
				 PGRAC_FENCED_JOURNAL_SCAN_PARTIAL_TAIL);
	UT_ASSERT_EQ(state.valid_bytes, 0);
	UT_ASSERT_EQ(state.partial_record_bytes, 768);
	UT_ASSERT(pgrac_fenced_journal_repair_partial_tail_fd(fd, &state));
	UT_ASSERT_EQ(fstat(fd, &st), 0);
	UT_ASSERT_EQ(st.st_size, 0);
	frame[700] = 1;
	UT_ASSERT_EQ(write(fd, frame, 768), 768);
	pgrac_fenced_journal_scan_state_init(&state);
	UT_ASSERT_EQ(pgrac_fenced_journal_scan_bytes(frame, 768, true, &state),
				 PGRAC_FENCED_JOURNAL_SCAN_CORRUPT);
	UT_ASSERT(!pgrac_fenced_journal_repair_partial_tail_fd(fd, &state));
	UT_ASSERT_EQ(fstat(fd, &st), 0);
	UT_ASSERT_EQ(st.st_size, 768);
	UT_ASSERT_EQ(close(fd), 0);
	UT_ASSERT_EQ(unlink(path), 0);
}

UT_TEST(test_intent_rotation_honors_unchanged_byte_limit)
{
	char path[] = "/tmp/pgrac-fenced-intent-full.XXXXXX";
	uint8 frame[768];
	PgracFencedJournalRecordV1 record;
	PgracFencedJournalScanState state;
	struct stat st;
	int fd = mkstemp(path);

	UT_ASSERT(fd >= 0);
	UT_ASSERT_EQ(fcntl(fd, F_SETFL, O_APPEND), 0);
	UT_ASSERT(make_intent_fixture(frame));
	UT_ASSERT(pgrac_fenced_journal_record_decode(frame, 768, &record));
	pgrac_fenced_journal_scan_state_init(&state);
	state.valid_bytes = PGRAC_FENCED_JOURNAL_SEGMENT_BYTES - 512;
	UT_ASSERT_EQ(pgrac_fenced_journal_append_fd(fd, &state, &record),
				 PGRAC_FENCED_JOURNAL_APPEND_ROTATION_REQUIRED);
	UT_ASSERT(state.available);
	UT_ASSERT_EQ(fstat(fd, &st), 0);
	UT_ASSERT_EQ(st.st_size, 0);
	UT_ASSERT_EQ(close(fd), 0);
	UT_ASSERT_EQ(unlink(path), 0);
}

UT_TEST(test_intent_segment_seals_before_partial_record)
{
	char directory[] = "/tmp/pgrac-fenced-intent-rotate.XXXXXX";
	char sealed[PGRAC_FENCED_JOURNAL_SEALED_NAME_MAX];
	PgracFencedJournalScanState state;
	uint8 digest[32];
	uint64 first, last;
	struct stat st;
	int dirfd, fd;

	UT_ASSERT(mkdtemp(directory) != NULL);
	dirfd = open(directory, O_RDONLY | O_DIRECTORY);
	UT_ASSERT(dirfd >= 0);
	fd = openat(dirfd, "journal.active", O_RDWR | O_CREAT | O_EXCL | O_APPEND, 0600);
	UT_ASSERT(fd >= 0);
	UT_ASSERT_EQ(ftruncate(fd, 87381 * (off_t)768), 0);
	pgrac_fenced_journal_scan_state_init(&state);
	state.next_seq = 87382;
	state.segment_record_count = 87381;
	state.valid_bytes = 87381 * 768;
	memset(state.previous_record_digest, 0xab, 32);
	UT_ASSERT(pgrac_fenced_journal_rotate_at(dirfd, &fd, 0, &state, sealed, sizeof(sealed)));
	UT_ASSERT(pgrac_fenced_journal_sealed_name_parse(sealed, &first, &last, digest));
	UT_ASSERT_EQ(first, 1);
	UT_ASSERT_EQ(last, 87381);
	UT_ASSERT_EQ(state.valid_bytes, 0);
	UT_ASSERT_EQ(fstat(fd, &st), 0);
	UT_ASSERT_EQ(st.st_size, 0);
	UT_ASSERT_EQ(close(fd), 0);
	if (sealed[0] != '\0')
		UT_ASSERT_EQ(unlinkat(dirfd, sealed, 0), 0);
	UT_ASSERT_EQ(unlinkat(dirfd, "journal.active", 0), 0);
	UT_ASSERT_EQ(close(dirfd), 0);
	UT_ASSERT_EQ(rmdir(directory), 0);
}

UT_TEST(test_journal_rejects_crc_reserved_unknown_and_bad_proof)
{
	PgracFencedJournalRecordV1 record;
	uint8 frame[PGRAC_FENCED_JOURNAL_RECORD_BYTES];

	make_config_record(&record, 1);
	UT_ASSERT(pgrac_fenced_journal_record_encode(&record, frame));
	frame[240] = 1;
	UT_ASSERT(!pgrac_fenced_journal_record_decode(frame, sizeof(frame), &record));

	make_config_record(&record, 1);
	record.record_kind = 11;
	UT_ASSERT(!pgrac_fenced_journal_record_encode(&record, frame));

	make_config_record(&record, 1);
	record.record_kind = PGRAC_FENCED_JOURNAL_KIND_PROOF_SERVED;
	fill_nonzero(record.operation_id, sizeof(record.operation_id), 0x33);
	UT_ASSERT(!pgrac_fenced_journal_record_encode(&record, frame));
}

UT_TEST(test_journal_scan_verifies_seq_and_full_record_hash_chain)
{
	PgracFencedJournalRecordV1 first;
	PgracFencedJournalRecordV1 second;
	PgracFencedJournalScanState state;
	uint8 frames[PGRAC_FENCED_JOURNAL_RECORD_BYTES * 2];

	make_config_record(&first, 1);
	UT_ASSERT(pgrac_fenced_journal_record_encode(&first, frames));
	make_config_record(&second, 2);
	second.record_kind = PGRAC_FENCED_JOURNAL_KIND_REQUEST_ACCEPTED;
	fill_nonzero(second.operation_id, sizeof(second.operation_id), 0x44);
	UT_ASSERT(pgrac_fenced_journal_record_digest(frames, second.previous_record_digest));
	UT_ASSERT(
		pgrac_fenced_journal_record_encode(&second, frames + PGRAC_FENCED_JOURNAL_RECORD_BYTES));

	pgrac_fenced_journal_scan_state_init(&state);
	UT_ASSERT_EQ(pgrac_fenced_journal_scan_bytes(frames, sizeof(frames), false, &state),
				 PGRAC_FENCED_JOURNAL_SCAN_OK);
	UT_ASSERT_EQ(state.next_seq, 3);
	UT_ASSERT_EQ(state.segment_record_count, 2);
	UT_ASSERT_EQ(state.valid_bytes, sizeof(frames));

	frames[PGRAC_FENCED_JOURNAL_RECORD_BYTES + 56] ^= 1;
	pgrac_fenced_journal_scan_state_init(&state);
	UT_ASSERT_EQ(pgrac_fenced_journal_scan_bytes(frames, sizeof(frames), false, &state),
				 PGRAC_FENCED_JOURNAL_SCAN_CORRUPT);
	UT_ASSERT(!state.available);
}

UT_TEST(test_only_active_final_partial_record_is_truncatable)
{
	PgracFencedJournalRecordV1 record;
	PgracFencedJournalScanState state;
	uint8 bytes[PGRAC_FENCED_JOURNAL_RECORD_BYTES + 13];

	make_config_record(&record, 1);
	UT_ASSERT(pgrac_fenced_journal_record_encode(&record, bytes));
	memset(bytes + PGRAC_FENCED_JOURNAL_RECORD_BYTES, 0x55, 13);

	pgrac_fenced_journal_scan_state_init(&state);
	UT_ASSERT_EQ(pgrac_fenced_journal_scan_bytes(bytes, sizeof(bytes), true, &state),
				 PGRAC_FENCED_JOURNAL_SCAN_PARTIAL_TAIL);
	UT_ASSERT(state.available);
	UT_ASSERT_EQ(state.valid_bytes, PGRAC_FENCED_JOURNAL_RECORD_BYTES);
	UT_ASSERT_EQ(state.next_seq, 2);

	pgrac_fenced_journal_scan_state_init(&state);
	UT_ASSERT_EQ(pgrac_fenced_journal_scan_bytes(bytes, sizeof(bytes), false, &state),
				 PGRAC_FENCED_JOURNAL_SCAN_CORRUPT);
	UT_ASSERT(!state.available);
}

UT_TEST(test_append_advances_only_after_full_write_and_fsync)
{
	PgracFencedJournalRecordV1 record;
	PgracFencedJournalScanState state;
	PgracFencedJournalScanState scanned;
	uint8 frames[PGRAC_FENCED_JOURNAL_RECORD_BYTES * 2];
	char path[] = "/tmp/pgrac-fenced-journal.XXXXXX";
	int fd;

	fd = mkstemp(path);
	UT_ASSERT(fd >= 0);
	UT_ASSERT(pgrac_fenced_journal_filesystem_local(fd));
	UT_ASSERT_EQ(fcntl(fd, F_SETFL, fcntl(fd, F_GETFL) | O_APPEND), 0);
	pgrac_fenced_journal_scan_state_init(&state);

	make_config_record(&record, 0);
	UT_ASSERT_EQ(pgrac_fenced_journal_append_fd(fd, &state, &record),
				 PGRAC_FENCED_JOURNAL_APPEND_OK);
	UT_ASSERT_EQ(record.seq, 1);

	make_config_record(&record, 0);
	record.record_kind = PGRAC_FENCED_JOURNAL_KIND_REQUEST_ACCEPTED;
	fill_nonzero(record.operation_id, sizeof(record.operation_id), 0x66);
	UT_ASSERT_EQ(pgrac_fenced_journal_append_fd(fd, &state, &record),
				 PGRAC_FENCED_JOURNAL_APPEND_OK);
	UT_ASSERT_EQ(record.seq, 2);
	UT_ASSERT_EQ(state.next_seq, 3);

	UT_ASSERT_EQ(lseek(fd, 0, SEEK_SET), 0);
	UT_ASSERT_EQ(read(fd, frames, sizeof(frames)), sizeof(frames));
	pgrac_fenced_journal_scan_state_init(&scanned);
	UT_ASSERT_EQ(pgrac_fenced_journal_scan_bytes(frames, sizeof(frames), true, &scanned),
				 PGRAC_FENCED_JOURNAL_SCAN_OK);
	UT_ASSERT_EQ(scanned.next_seq, 3);

	(void)close(fd);
	(void)unlink(path);
}

UT_TEST(test_append_requests_rotation_at_exact_segment_limit)
{
	PgracFencedJournalRecordV1 record;
	PgracFencedJournalScanState state;

	pgrac_fenced_journal_scan_state_init(&state);
	state.segment_record_count = PGRAC_FENCED_JOURNAL_SEGMENT_RECORDS;
	make_config_record(&record, 0);
	UT_ASSERT_EQ(pgrac_fenced_journal_append_fd(-1, &state, &record),
				 PGRAC_FENCED_JOURNAL_APPEND_ROTATION_REQUIRED);
	UT_ASSERT(state.available);
	UT_ASSERT_EQ(state.next_seq, 1);
}

UT_TEST(test_reconcile_actions_cover_all_durable_record_kinds)
{
	PgracFencedJournalRecordV1 record;
	PgracFencedJournalRestartAction action;
	uint16 kind;

	for (kind = PGRAC_FENCED_JOURNAL_KIND_CONFIG_LOADED;
		 kind <= PGRAC_FENCED_JOURNAL_KIND_RECONCILED; kind++) {
		make_config_record(&record, 1);
		record.record_kind = kind;
		if (kind != PGRAC_FENCED_JOURNAL_KIND_CONFIG_LOADED)
			fill_nonzero(record.operation_id, sizeof(record.operation_id), 0x77);
		if (kind == PGRAC_FENCED_JOURNAL_KIND_REENABLE_RESULT)
			record.target_state = PGRAC_FENCED_JOURNAL_TARGET_ON;
		if (kind == PGRAC_FENCED_JOURNAL_KIND_RECONCILED)
			record.target_state = PGRAC_FENCED_JOURNAL_TARGET_UNKNOWN;
		UT_ASSERT(pgrac_fenced_journal_restart_action(&record, &action));
		UT_ASSERT_NE(action, PGRAC_FENCED_JOURNAL_RESTART_UNAVAILABLE);
	}

	make_config_record(&record, 1);
	record.record_kind = PGRAC_FENCED_JOURNAL_KIND_RECONCILED;
	fill_nonzero(record.operation_id, sizeof(record.operation_id), 0x88);
	record.target_state = PGRAC_FENCED_JOURNAL_TARGET_OFF;
	UT_ASSERT(pgrac_fenced_journal_restart_action(&record, &action));
	UT_ASSERT_EQ(action, PGRAC_FENCED_JOURNAL_RESTART_FRESH_READBACK);
	record.target_state = PGRAC_FENCED_JOURNAL_TARGET_ON;
	UT_ASSERT(pgrac_fenced_journal_restart_action(&record, &action));
	UT_ASSERT_EQ(action, PGRAC_FENCED_JOURNAL_RESTART_RETURN_OFF_BEFORE_REJOIN);
	record.target_state = PGRAC_FENCED_JOURNAL_TARGET_TRANSITIONING;
	UT_ASSERT(!pgrac_fenced_journal_restart_action(&record, &action));
	UT_ASSERT_EQ(action, PGRAC_FENCED_JOURNAL_RESTART_UNAVAILABLE);
}

UT_TEST(test_restart_reconcile_keeps_only_last_unfinished_operations)
{
	PgracFencedJournalReconcileState state;
	PgracFencedJournalRecordV1 record;

	pgrac_fenced_journal_reconcile_state_init(&state);
	make_config_record(&record, 1);
	UT_ASSERT(pgrac_fenced_journal_reconcile_observe(&state, &record));

	/* A completed proof supersedes this operation's uncertain actuation. */
	make_config_record(&record, 2);
	record.record_kind = PGRAC_FENCED_JOURNAL_KIND_ACTUATION_ISSUED;
	fill_nonzero(record.operation_id, sizeof(record.operation_id), 0x31);
	UT_ASSERT(pgrac_fenced_journal_reconcile_observe(&state, &record));
	record.record_kind = PGRAC_FENCED_JOURNAL_KIND_PROOF_SERVED;
	UT_ASSERT(pgrac_fenced_journal_reconcile_observe(&state, &record));

	/* This actuation remains uncertain at the crash cut. */
	make_config_record(&record, 3);
	record.record_kind = PGRAC_FENCED_JOURNAL_KIND_ACTUATION_RESULT;
	fill_nonzero(record.operation_id, sizeof(record.operation_id), 0x32);
	record.target_state = PGRAC_FENCED_JOURNAL_TARGET_UNKNOWN;
	UT_ASSERT(pgrac_fenced_journal_reconcile_observe(&state, &record));

	/* INVALIDATED supersedes a queued REQUEST_ACCEPTED. */
	make_config_record(&record, 4);
	record.record_kind = PGRAC_FENCED_JOURNAL_KIND_REQUEST_ACCEPTED;
	fill_nonzero(record.operation_id, sizeof(record.operation_id), 0x33);
	UT_ASSERT(pgrac_fenced_journal_reconcile_observe(&state, &record));
	record.record_kind = PGRAC_FENCED_JOURNAL_KIND_INVALIDATED;
	UT_ASSERT(pgrac_fenced_journal_reconcile_observe(&state, &record));

	/* ON after re-enable keeps the target write-disabled until fresh F. */
	make_config_record(&record, 5);
	record.record_kind = PGRAC_FENCED_JOURNAL_KIND_REENABLE_RESULT;
	fill_nonzero(record.operation_id, sizeof(record.operation_id), 0x34);
	record.target_state = PGRAC_FENCED_JOURNAL_TARGET_ON;
	UT_ASSERT(pgrac_fenced_journal_reconcile_observe(&state, &record));

	UT_ASSERT(pgrac_fenced_journal_reconcile_finish(&state));
	UT_ASSERT_EQ(state.pending_count, 2);
	UT_ASSERT(state.fresh_readback_required);
	UT_ASSERT(state.keep_write_disabled);
	UT_ASSERT(state.return_off_before_rejoin);
	UT_ASSERT(state.available);
}

static bool
observe_encoded_intent(PgracFencedJournalReconcileState *state,
					   const PgracFencedJournalRecordV1 *record)
{
	uint8 frame[768];
	size_t length;
	PgracFencedJournalRecordV1 decoded;
	bool encoded = pgrac_fenced_journal_frame_encode(record, frame, sizeof(frame), &length);

	UT_ASSERT(encoded);
	if (!encoded || !pgrac_fenced_journal_record_decode(frame, length, &decoded))
		return false;
	return pgrac_fenced_journal_reconcile_observe(state, &decoded);
}

UT_TEST(test_owned_intent_survives_accept_cancel_and_reconciliation)
{
	const uint16 kinds[] = { 2, 3, 4, 5, 7, 10 };
	PgracFencedJournalReconcileState state;
	PgracFencedJournalRecordV1 record;
	uint8 frame[768];

	UT_ASSERT(make_intent_fixture(frame));
	UT_ASSERT(pgrac_fenced_journal_record_decode(frame, sizeof(frame), &record));
	pgrac_fenced_journal_reconcile_state_init(&state);
	for (size_t i = 0; i < lengthof(kinds); ++i) {
		record.record_kind = kinds[i];
		record.seq = i + 1;
		record.target_state = 4;
		record.deny_reason = kinds[i] == 7 ? 16 : 9;
		UT_ASSERT(observe_encoded_intent(&state, &record));
		UT_ASSERT_EQ(state.pending_count, 1);
		UT_ASSERT_EQ(state.pending[0].last_record.intent.attempt, 3);
		UT_ASSERT(pgrac_fenced_journal_reconcile_finish(&state));
		UT_ASSERT(state.fresh_readback_required);
	}
	/* A durable exact proof retires work, not a cancellation or old timestamp. */
	record.seq++;
	record.record_kind = 6;
	record.provider_result = 0;
	record.target_state = 1;
	record.io_drain_state = 1;
	record.deny_reason = 0;
	record.fresh_until_mono_ns = 101;
	record.proof_generation = 1;
	memset(record.target_state_digest, 0x88, 32);
	UT_ASSERT(observe_encoded_intent(&state, &record));
	UT_ASSERT_EQ(state.pending_count, 0);
}

UT_TEST(test_owned_rejoin_keeps_writes_disabled_through_restart)
{
	const uint16 kinds[] = { 2, 3, 4, 5, 7, 8, 9, 10 };
	uint8 frame[768];
	PgracFencedJournalRecordV1 record;

	UT_ASSERT(make_rejoin_fixture(frame, PGRAC_EXTERNAL_FENCE_REJOIN_LMON_AUTHORIZE_ON, 2, 91, 92));
	UT_ASSERT(pgrac_fenced_journal_record_decode(frame, sizeof(frame), &record));
	for (size_t i = 0; i < lengthof(kinds); ++i) {
		PgracFencedJournalReconcileState state;

		pgrac_fenced_journal_reconcile_state_init(&state);
		record.record_kind = kinds[i];
		record.target_state
			= kinds[i] == 9 ? PGRAC_FENCED_JOURNAL_TARGET_ON : PGRAC_FENCED_JOURNAL_TARGET_UNKNOWN;
		UT_ASSERT(observe_encoded_intent(&state, &record));
		UT_ASSERT(pgrac_fenced_journal_reconcile_finish(&state));
		UT_ASSERT_EQ(state.pending_count, 1);
		UT_ASSERT(state.fresh_readback_required);
		UT_ASSERT(state.keep_write_disabled);
		UT_ASSERT_EQ(state.return_off_before_rejoin, kinds[i] == 9);
	}
}

UT_TEST(test_rejoin_terminal_is_provider_completion_not_database_open)
{
	for (int cleanup = 0; cleanup < 2; ++cleanup) {
		PgracFencedJournalRecordV1 accepted, result;
		PgracFencedJournalReconcileState replay;
		PgracFencedJournalRestartAction action;

		make_rejoin_phase(cleanup ? PGRAC_EXTERNAL_FENCE_REJOIN_LMON_AUTHORIZE_ON
								  : PGRAC_EXTERNAL_FENCE_REJOIN_LMON_REFRESH_ON,
						  1, 3, &accepted);
		result = accepted;
		result.seq++;
		rejoin_phase_proof(&result, !cleanup);
		if (cleanup)
			result.record_kind = PGRAC_FENCED_JOURNAL_KIND_RECONCILED;
		pgrac_fenced_journal_reconcile_state_init(&replay);
		UT_ASSERT(observe_encoded_intent(&replay, &accepted));
		UT_ASSERT(observe_encoded_intent(&replay, &result));
		UT_ASSERT_EQ(replay.pending_count, 0);
		UT_ASSERT(pgrac_fenced_journal_restart_action(&result, &action));
		UT_ASSERT_EQ(action, PGRAC_FENCED_JOURNAL_RESTART_NO_OPERATION);
		UT_ASSERT(pgrac_fenced_journal_reconcile_finish(&replay));
		UT_ASSERT(!replay.return_off_before_rejoin);
	}
}

UT_TEST(test_rejoin_near_success_stays_owned)
{
	for (int variant = 0; variant < 10; ++variant) {
		PgracFencedJournalRecordV1 accepted, result;
		PgracFencedJournalReconcileState replay;

		make_rejoin_phase(variant == 0 ? PGRAC_EXTERNAL_FENCE_REJOIN_LMON_AUTHORIZE_ON
									   : PGRAC_EXTERNAL_FENCE_REJOIN_LMON_REFRESH_ON,
						  1, 3, &accepted);
		result = accepted;
		result.seq++;
		rejoin_phase_proof(&result, true);
		switch (variant) {
		case 1:
			result.provider_result = PGRAC_FENCED_JOURNAL_PROVIDER_UNKNOWN;
			break;
		case 2:
			result.target_state = PGRAC_FENCED_JOURNAL_TARGET_OFF;
			break;
		case 3:
			result.io_drain_state = 0;
			break;
		case 4:
			result.deny_reason = 9;
			break;
		case 5:
			result.proof_generation = 0;
			break;
		case 6:
			result.fresh_until_mono_ns = result.event_mono_ns;
			break;
		case 7:
			result.target_state_digest[0] ^= 1;
			break;
		case 8:
			result.record_kind = PGRAC_FENCED_JOURNAL_KIND_INVALIDATED;
			break;
		case 9:
			result.record_kind = PGRAC_FENCED_JOURNAL_KIND_RECONCILED;
			break;
		}
		pgrac_fenced_journal_reconcile_state_init(&replay);
		UT_ASSERT(observe_encoded_intent(&replay, &accepted));
		UT_ASSERT(observe_encoded_intent(&replay, &result));
		UT_ASSERT_EQ(replay.pending_count, 1);
	}
}

UT_TEST(test_rejoin_replay_keeps_original_admin_not_derived_request)
{
	PgracFencedJournalRecordV1 admin, offer, authorize;
	PgracFencedJournalReconcileState replay;

	make_rejoin_phase(PGRAC_EXTERNAL_FENCE_REJOIN_ADMIN_PREPARE, 1, 1, &admin);
	offer = admin;
	offer.seq = 2;
	rejoin_phase_proof(&offer, false);
	make_rejoin_phase(PGRAC_EXTERNAL_FENCE_REJOIN_LMON_AUTHORIZE_ON, 3, 2, &authorize);
	authorize.intent.request.rejoin.transport_nonce[0] = 0xf1;
	authorize.intent.request.rejoin.timeout_ms = 2000;
	pgrac_fenced_journal_reconcile_state_init(&replay);
	UT_ASSERT(observe_encoded_intent(&replay, &admin));
	UT_ASSERT(observe_encoded_intent(&replay, &offer));
	UT_ASSERT(observe_encoded_intent(&replay, &authorize));
	UT_ASSERT_EQ(replay.pending_count, 1);
	UT_ASSERT_EQ(replay.pending[0].first_record.seq, 1);
	UT_ASSERT_EQ(replay.pending[0].first_record.intent.request.rejoin.opcode,
				 PGRAC_EXTERNAL_FENCE_REJOIN_ADMIN_PREPARE);
	UT_ASSERT_EQ(replay.pending[0].first_record.intent.request.rejoin.transport_nonce[0], 0x44);
	UT_ASSERT_EQ(replay.pending[0].first_record.intent.request.rejoin.timeout_ms, 30000);
	UT_ASSERT_EQ(replay.pending[0].last_record.intent.request.rejoin.transport_nonce[0], 0xf1);
}

UT_TEST(test_pending_intent_rejects_identity_drift_without_erasing_work)
{
	PgracFencedJournalRecordV1 original;
	uint8 frame[768];

	UT_ASSERT(make_intent_fixture(frame));
	UT_ASSERT(pgrac_fenced_journal_record_decode(frame, sizeof(frame), &original));
	for (int variant = 0; variant < 6; ++variant) {
		PgracFencedJournalReconcileState state;
		PgracFencedJournalRecordV1 changed = original;
		PgracExternalFenceProtocolBindingV1 binding;

		pgrac_fenced_journal_reconcile_state_init(&state);
		UT_ASSERT(observe_encoded_intent(&state, &original));
		changed.seq++;
		switch (variant) {
		case 0:
			changed.intent.target_uuid[0] ^= 1;
			break;
		case 1:
			changed.semantic_config_digest[0] ^= 1;
			break;
		case 2:
			changed.mapping_generation++;
			break;
		case 3:
			changed.intent.request.acquire.need.victim_incarnation++;
			break;
		case 4:
			changed.intent.request.acquire.request_nonce[0] ^= 1;
			break;
		case 5:
			changed.intent.request.acquire.timeout_ms++;
			break;
		}
		UT_ASSERT(pgrac_external_fence_binding_from_request_v1(
			&changed.intent.request.acquire.need, changed.mapping_generation, &binding));
		UT_ASSERT(pgrac_external_fence_binding_digest_v1(&binding, changed.binding_digest));
		UT_ASSERT(!observe_encoded_intent(&state, &changed));
		UT_ASSERT(!state.available);
		UT_ASSERT_EQ(state.pending_count, 1);
		UT_ASSERT(memcmp(&state.pending[0].last_record, &original, sizeof(original)) == 0);
	}
}

UT_TEST(test_pending_attempt_cannot_be_replaced_by_late_or_legacy_completion)
{
	PgracFencedJournalRecordV1 original;
	uint8 frame[768];

	UT_ASSERT(make_intent_fixture(frame));
	UT_ASSERT(pgrac_fenced_journal_record_decode(frame, sizeof(frame), &original));
	for (int variant = 0; variant < 4; ++variant) {
		PgracFencedJournalReconcileState state;
		PgracFencedJournalRecordV1 changed = original;

		pgrac_fenced_journal_reconcile_state_init(&state);
		UT_ASSERT(observe_encoded_intent(&state, &original));
		changed.seq++;
		if (variant == 3) {
			memset(&changed.intent, 0, sizeof(changed.intent));
			changed.record_kind = 7;
			UT_ASSERT(!pgrac_fenced_journal_reconcile_observe(&state, &changed));
		} else {
			changed.intent.attempt = variant == 0 ? 2 : variant == 1 ? 4 : 5;
			changed.record_kind = variant == 2 ? 2 : 4;
			UT_ASSERT(!observe_encoded_intent(&state, &changed));
		}
		UT_ASSERT_EQ(state.pending_count, 1);
		UT_ASSERT_EQ(state.pending[0].last_record.intent.attempt, 3);
		UT_ASSERT(!state.available);
	}
	{
		PgracFencedJournalReconcileState state;
		PgracFencedJournalRecordV1 next = original;

		pgrac_fenced_journal_reconcile_state_init(&state);
		UT_ASSERT(observe_encoded_intent(&state, &original));
		next.seq++;
		next.intent.attempt = 4;
		next.record_kind = 2;
		/* The new daemon owns a fresh attempt, not the previous callback. */
		next.daemon_boot_id[0] ^= 1;
		UT_ASSERT(observe_encoded_intent(&state, &next));
		UT_ASSERT_EQ(state.pending_count, 1);
		UT_ASSERT_EQ(state.pending[0].last_record.intent.attempt, 4);
	}
}

UT_TEST(test_rotation_seals_exact_name_and_creates_new_active)
{
	PgracFencedJournalScanState state;
	char dir_path[] = "/tmp/pgrac-fenced-rotate.XXXXXX";
	char sealed_name[PGRAC_FENCED_JOURNAL_SEALED_NAME_MAX];
	char sealed_path[MAXPGPATH];
	char active_path[MAXPGPATH];
	struct stat st;
	int dir_fd;
	int active_fd;

	UT_ASSERT_NOT_NULL(mkdtemp(dir_path));
	UT_ASSERT(snprintf(active_path, sizeof(active_path), "%s/%s", dir_path,
					   PGRAC_FENCED_JOURNAL_ACTIVE_NAME)
			  > 0);
	active_fd = open(active_path, O_RDWR | O_CREAT | O_EXCL | O_APPEND, 0600);
	UT_ASSERT(active_fd >= 0);
	UT_ASSERT_EQ(ftruncate(active_fd, (off_t)PGRAC_FENCED_JOURNAL_SEGMENT_BYTES), 0);
	dir_fd = open(dir_path, O_RDONLY | O_DIRECTORY);
	UT_ASSERT(dir_fd >= 0);

	pgrac_fenced_journal_scan_state_init(&state);
	state.segment_first_seq = 1;
	state.next_seq = UINT64_C(262145);
	state.segment_record_count = PGRAC_FENCED_JOURNAL_SEGMENT_RECORDS;
	state.valid_bytes = PGRAC_FENCED_JOURNAL_SEGMENT_BYTES;
	fill_nonzero(state.previous_record_digest, sizeof(state.previous_record_digest), 0xab);
	UT_ASSERT(pgrac_fenced_journal_rotate_at(dir_fd, &active_fd, 7, &state, sealed_name,
											 sizeof(sealed_name)));
	UT_ASSERT_EQ(strcmp(sealed_name, "journal.1-262144."
									 "abababababababababababababababab"
									 "abababababababababababababababab.sealed"),
				 0);
	UT_ASSERT_EQ(state.segment_record_count, 0);
	UT_ASSERT_EQ(state.segment_first_seq, UINT64_C(262145));
	UT_ASSERT_EQ(state.next_seq, UINT64_C(262145));
	UT_ASSERT(state.available);
	UT_ASSERT(active_fd >= 0);
	UT_ASSERT_EQ(fstat(active_fd, &st), 0);
	UT_ASSERT_EQ(st.st_size, 0);

	UT_ASSERT(snprintf(sealed_path, sizeof(sealed_path), "%s/%s", dir_path, sealed_name) > 0);
	UT_ASSERT_EQ(stat(sealed_path, &st), 0);
	UT_ASSERT_EQ(st.st_size, (off_t)PGRAC_FENCED_JOURNAL_SEGMENT_BYTES);

	(void)close(active_fd);
	(void)close(dir_fd);
	(void)unlink(active_path);
	(void)unlink(sealed_path);
	(void)rmdir(dir_path);
}

UT_TEST(test_ninth_rotation_fails_closed_without_rename)
{
	PgracFencedJournalScanState state;
	char sealed_name[PGRAC_FENCED_JOURNAL_SEALED_NAME_MAX];
	int active_fd = -1;

	pgrac_fenced_journal_scan_state_init(&state);
	state.segment_record_count = PGRAC_FENCED_JOURNAL_SEGMENT_RECORDS;
	UT_ASSERT(!pgrac_fenced_journal_rotate_at(-1, &active_fd, PGRAC_FENCED_JOURNAL_MAX_SEALED,
											  &state, sealed_name, sizeof(sealed_name)));
	UT_ASSERT(!state.available);
}

UT_TEST(test_semantic_config_digest_has_exact_domain_and_length)
{
	static const uint8 expected[PGRAC_FENCED_JOURNAL_DIGEST_BYTES]
		= { 0xd4, 0x40, 0x05, 0x32, 0x5d, 0x7a, 0x43, 0xf4, 0xb1, 0x81, 0xda,
			0x56, 0x53, 0x8f, 0xf2, 0xe3, 0xcc, 0x0a, 0x32, 0xa3, 0xf8, 0x50,
			0xfc, 0x66, 0xbe, 0x0f, 0x80, 0xcd, 0x4d, 0x0f, 0x3c, 0x63 };
	static const uint8 config[] = "x\n";
	uint8 digest[PGRAC_FENCED_JOURNAL_DIGEST_BYTES];

	UT_ASSERT(pgrac_fenced_journal_config_digest_v1(config, sizeof(config) - 1, digest));
	UT_ASSERT(memcmp(digest, expected, sizeof(expected)) == 0);
	UT_ASSERT(!pgrac_fenced_journal_config_digest_v1(NULL, 0, digest));
}

UT_TEST(test_partial_tail_repair_truncates_and_fsyncs_active)
{
	PgracFencedJournalRecordV1 record;
	PgracFencedJournalScanState state;
	uint8 bytes[PGRAC_FENCED_JOURNAL_RECORD_BYTES + 13];
	char path[] = "/tmp/pgrac-fenced-partial.XXXXXX";
	struct stat st;
	int fd;

	make_config_record(&record, 1);
	UT_ASSERT(pgrac_fenced_journal_record_encode(&record, bytes));
	memset(bytes + PGRAC_FENCED_JOURNAL_RECORD_BYTES, 0x55, 13);
	fd = mkstemp(path);
	UT_ASSERT(fd >= 0);
	UT_ASSERT_EQ(fcntl(fd, F_SETFL, fcntl(fd, F_GETFL) | O_APPEND), 0);
	UT_ASSERT_EQ(write(fd, bytes, sizeof(bytes)), sizeof(bytes));
	pgrac_fenced_journal_scan_state_init(&state);
	UT_ASSERT_EQ(pgrac_fenced_journal_scan_bytes(bytes, sizeof(bytes), true, &state),
				 PGRAC_FENCED_JOURNAL_SCAN_PARTIAL_TAIL);
	UT_ASSERT(pgrac_fenced_journal_repair_partial_tail_fd(fd, &state));
	UT_ASSERT_EQ(fstat(fd, &st), 0);
	UT_ASSERT_EQ(st.st_size, PGRAC_FENCED_JOURNAL_RECORD_BYTES);
	UT_ASSERT(state.available);
	(void)close(fd);
	(void)unlink(path);
}

UT_TEST(test_runtime_and_journal_stat_gates_are_exact)
{
	struct stat st;

	memset(&st, 0, sizeof(st));
	st.st_mode = S_IFDIR | 0750;
	st.st_uid = 0;
	st.st_gid = 44;
	UT_ASSERT(pgrac_fenced_runtime_dir_stat_secure(&st, 44));
	st.st_mode = S_IFDIR | 0770;
	UT_ASSERT(!pgrac_fenced_runtime_dir_stat_secure(&st, 44));
	st.st_mode = S_IFDIR | 0750;
	st.st_gid = 45;
	UT_ASSERT(!pgrac_fenced_runtime_dir_stat_secure(&st, 44));

	memset(&st, 0, sizeof(st));
	st.st_mode = S_IFDIR | 0700;
	st.st_uid = 0;
	st.st_gid = 0;
	UT_ASSERT(pgrac_fenced_journal_dir_stat_secure(&st));
	st.st_mode = S_IFDIR | 0750;
	UT_ASSERT(!pgrac_fenced_journal_dir_stat_secure(&st));

	memset(&st, 0, sizeof(st));
	st.st_mode = S_IFREG | 0600;
	st.st_uid = 0;
	st.st_gid = 0;
	st.st_size = PGRAC_FENCED_JOURNAL_RECORD_BYTES;
	UT_ASSERT(pgrac_fenced_journal_file_stat_secure(&st));
	st.st_mode = S_IFREG | 0640;
	UT_ASSERT(!pgrac_fenced_journal_file_stat_secure(&st));
	st.st_mode = S_IFREG | 0600;
	st.st_size = (off_t)PGRAC_FENCED_JOURNAL_SEGMENT_BYTES + 1;
	UT_ASSERT(!pgrac_fenced_journal_file_stat_secure(&st));
}

UT_TEST(test_runtime_loads_and_repairs_only_active_partial_tail)
{
	PgracFencedJournalRecordV1 record;
	PgracFencedJournalRecordV1 last;
	PgracFencedJournalScanState append_state;
	PgracFencedJournalScanState loaded_state;
	char path[] = "/tmp/pgrac-fenced-runtime-journal.XXXXXX";
	struct stat st;
	bool have_last = false;
	uint8 tail[13];
	int fd;

	fd = mkstemp(path);
	UT_ASSERT(fd >= 0);
	UT_ASSERT_EQ(fcntl(fd, F_SETFL, fcntl(fd, F_GETFL) | O_APPEND), 0);
	pgrac_fenced_journal_scan_state_init(&append_state);
	make_config_record(&record, 0);
	UT_ASSERT_EQ(pgrac_fenced_journal_append_fd(fd, &append_state, &record),
				 PGRAC_FENCED_JOURNAL_APPEND_OK);
	memset(tail, 0x5a, sizeof(tail));
	UT_ASSERT_EQ(write(fd, tail, sizeof(tail)), sizeof(tail));
	pgrac_fenced_journal_scan_state_init(&loaded_state);
	UT_ASSERT(pgrac_fenced_journal_load_active_fd(fd, &loaded_state, &last, &have_last));
	UT_ASSERT(have_last);
	UT_ASSERT_EQ(last.record_kind, PGRAC_FENCED_JOURNAL_KIND_CONFIG_LOADED);
	UT_ASSERT_EQ(loaded_state.next_seq, 2);
	UT_ASSERT_EQ(fstat(fd, &st), 0);
	UT_ASSERT_EQ(st.st_size, PGRAC_FENCED_JOURNAL_RECORD_BYTES);

	UT_ASSERT_EQ(pwrite(fd, "x", 1, 0), 1);
	UT_ASSERT_EQ(fsync(fd), 0);
	UT_ASSERT(!pgrac_fenced_journal_load_active_fd(fd, &loaded_state, &last, &have_last));
	UT_ASSERT(!loaded_state.available);
	(void)close(fd);
	(void)unlink(path);
}

UT_TEST(test_runtime_loads_sealed_then_active_as_one_hash_chain)
{
	PgracFencedJournalRecordV1 record;
	PgracFencedJournalRecordV1 last;
	PgracFencedJournalScanState append_state;
	PgracFencedJournalScanState loaded_state;
	char sealed_path[] = "/tmp/pgrac-fenced-sealed.XXXXXX";
	char active_path[] = "/tmp/pgrac-fenced-active.XXXXXX";
	bool have_last = false;
	int sealed_fd;
	int active_fd;

	sealed_fd = mkstemp(sealed_path);
	active_fd = mkstemp(active_path);
	UT_ASSERT(sealed_fd >= 0);
	UT_ASSERT(active_fd >= 0);
	UT_ASSERT_EQ(fcntl(sealed_fd, F_SETFL, fcntl(sealed_fd, F_GETFL) | O_APPEND), 0);
	UT_ASSERT_EQ(fcntl(active_fd, F_SETFL, fcntl(active_fd, F_GETFL) | O_APPEND), 0);
	pgrac_fenced_journal_scan_state_init(&append_state);
	make_config_record(&record, 0);
	UT_ASSERT_EQ(pgrac_fenced_journal_append_fd(sealed_fd, &append_state, &record),
				 PGRAC_FENCED_JOURNAL_APPEND_OK);
	append_state.segment_first_seq = append_state.next_seq;
	append_state.segment_record_count = 0;
	append_state.valid_bytes = 0;
	make_config_record(&record, 0);
	record.record_kind = PGRAC_FENCED_JOURNAL_KIND_REQUEST_ACCEPTED;
	fill_nonzero(record.operation_id, sizeof(record.operation_id), 0x33);
	UT_ASSERT_EQ(pgrac_fenced_journal_append_fd(active_fd, &append_state, &record),
				 PGRAC_FENCED_JOURNAL_APPEND_OK);

	pgrac_fenced_journal_scan_state_init(&loaded_state);
	UT_ASSERT(pgrac_fenced_journal_load_sealed_fd(sealed_fd, &loaded_state, &last, &have_last));
	UT_ASSERT(have_last);
	UT_ASSERT_EQ(last.seq, 1);
	UT_ASSERT(pgrac_fenced_journal_load_active_fd(active_fd, &loaded_state, &last, &have_last));
	UT_ASSERT(have_last);
	UT_ASSERT_EQ(last.seq, 2);
	UT_ASSERT_EQ(loaded_state.next_seq, 3);

	UT_ASSERT_EQ(write(sealed_fd, "x", 1), 1);
	pgrac_fenced_journal_scan_state_init(&loaded_state);
	UT_ASSERT(!pgrac_fenced_journal_load_sealed_fd(sealed_fd, &loaded_state, &last, &have_last));
	UT_ASSERT(!loaded_state.available);
	(void)close(active_fd);
	(void)close(sealed_fd);
	(void)unlink(active_path);
	(void)unlink(sealed_path);
}

UT_TEST(test_runtime_replays_verified_records_into_restart_reconcile)
{
	PgracFencedJournalReconcileState reconcile;
	PgracFencedJournalRecordV1 record;
	PgracFencedJournalRecordV1 last;
	PgracFencedJournalScanState append_state;
	PgracFencedJournalScanState loaded_state;
	char path[] = "/tmp/pgrac-fenced-reconcile.XXXXXX";
	bool have_last = false;
	int fd;

	fd = mkstemp(path);
	UT_ASSERT(fd >= 0);
	UT_ASSERT_EQ(fcntl(fd, F_SETFL, fcntl(fd, F_GETFL) | O_APPEND), 0);
	pgrac_fenced_journal_scan_state_init(&append_state);
	make_config_record(&record, 0);
	UT_ASSERT_EQ(pgrac_fenced_journal_append_fd(fd, &append_state, &record),
				 PGRAC_FENCED_JOURNAL_APPEND_OK);
	make_config_record(&record, 0);
	record.record_kind = PGRAC_FENCED_JOURNAL_KIND_ACTUATION_ISSUED;
	fill_nonzero(record.operation_id, sizeof(record.operation_id), 0x45);
	record.provider_result = PGRAC_FENCED_JOURNAL_PROVIDER_PENDING;
	UT_ASSERT_EQ(pgrac_fenced_journal_append_fd(fd, &append_state, &record),
				 PGRAC_FENCED_JOURNAL_APPEND_OK);

	pgrac_fenced_journal_scan_state_init(&loaded_state);
	pgrac_fenced_journal_reconcile_state_init(&reconcile);
	UT_ASSERT(pgrac_fenced_journal_load_active_reconcile_fd(fd, &loaded_state, &last, &have_last,
															&reconcile));
	UT_ASSERT(have_last);
	UT_ASSERT_EQ(last.record_kind, PGRAC_FENCED_JOURNAL_KIND_ACTUATION_ISSUED);
	UT_ASSERT(pgrac_fenced_journal_reconcile_finish(&reconcile));
	UT_ASSERT_EQ(reconcile.pending_count, 1);
	UT_ASSERT(reconcile.fresh_readback_required);
	(void)close(fd);
	(void)unlink(path);
}

UT_TEST(test_sealed_name_parser_is_canonical_and_full_segment_only)
{
	static const char valid[] = "journal.1-262144."
								"abababababababababababababababab"
								"abababababababababababababababab.sealed";
	char invalid[sizeof(valid)];
	uint8 digest[32];
	uint64 first;
	uint64 last;

	UT_ASSERT(pgrac_fenced_journal_sealed_name_parse(valid, &first, &last, digest));
	UT_ASSERT_EQ(first, 1);
	UT_ASSERT_EQ(last, UINT64_C(262144));
	UT_ASSERT_EQ(digest[0], 0xab);
	UT_ASSERT_EQ(digest[31], 0xab);
	strcpy(invalid, valid);
	invalid[8] = '0';
	UT_ASSERT(!pgrac_fenced_journal_sealed_name_parse(invalid, &first, &last, digest));
	strcpy(invalid, valid);
	invalid[25] = 'A';
	UT_ASSERT(!pgrac_fenced_journal_sealed_name_parse(invalid, &first, &last, digest));
	UT_ASSERT(!pgrac_fenced_journal_sealed_name_parse("journal.1-2."
													  "abababababababababababababababab"
													  "abababababababababababababababab.sealed",
													  &first, &last, digest));
	UT_ASSERT(!pgrac_fenced_journal_sealed_name_parse(NULL, &first, &last, digest));
}

int
main(void)
{
	UT_PLAN(38);
	UT_RUN(test_journal_exact_codec_roundtrip);
	UT_RUN(test_restart_decodes_exact_persisted_intent);
	UT_RUN(test_intent_codec_keeps_complete_identity_and_capacity);
	UT_RUN(test_intent_corruption_and_truncation_clear_output);
	UT_RUN(test_intent_identity_is_checked_beyond_crc);
	UT_RUN(test_intent_rejoin_preserves_admin_and_on_identities);
	UT_RUN(test_rejoin_successor_phases_preserve_one_owned_identity);
	UT_RUN(test_rejoin_phase_cannot_skip_or_replace_authority);
	UT_RUN(test_rejoin_admin_intent_rejects_invalid_identity);
	UT_RUN(test_bootstrap_detects_byte_full_mixed_segment);
	UT_RUN(test_mixed_journal_hashes_the_entire_intent);
	UT_RUN(test_intent_append_reopen_retains_the_original_need);
	UT_RUN(test_intent_partial_tail_is_not_a_completed_operation);
	UT_RUN(test_intent_rotation_honors_unchanged_byte_limit);
	UT_RUN(test_intent_segment_seals_before_partial_record);
	UT_RUN(test_journal_rejects_crc_reserved_unknown_and_bad_proof);
	UT_RUN(test_journal_scan_verifies_seq_and_full_record_hash_chain);
	UT_RUN(test_only_active_final_partial_record_is_truncatable);
	UT_RUN(test_append_advances_only_after_full_write_and_fsync);
	UT_RUN(test_append_requests_rotation_at_exact_segment_limit);
	UT_RUN(test_reconcile_actions_cover_all_durable_record_kinds);
	UT_RUN(test_restart_reconcile_keeps_only_last_unfinished_operations);
	UT_RUN(test_owned_intent_survives_accept_cancel_and_reconciliation);
	UT_RUN(test_owned_rejoin_keeps_writes_disabled_through_restart);
	UT_RUN(test_rejoin_terminal_is_provider_completion_not_database_open);
	UT_RUN(test_rejoin_near_success_stays_owned);
	UT_RUN(test_rejoin_replay_keeps_original_admin_not_derived_request);
	UT_RUN(test_pending_intent_rejects_identity_drift_without_erasing_work);
	UT_RUN(test_pending_attempt_cannot_be_replaced_by_late_or_legacy_completion);
	UT_RUN(test_rotation_seals_exact_name_and_creates_new_active);
	UT_RUN(test_ninth_rotation_fails_closed_without_rename);
	UT_RUN(test_semantic_config_digest_has_exact_domain_and_length);
	UT_RUN(test_partial_tail_repair_truncates_and_fsyncs_active);
	UT_RUN(test_runtime_and_journal_stat_gates_are_exact);
	UT_RUN(test_runtime_loads_and_repairs_only_active_partial_tail);
	UT_RUN(test_runtime_loads_sealed_then_active_as_one_hash_chain);
	UT_RUN(test_runtime_replays_verified_records_into_restart_reconcile);
	UT_RUN(test_sealed_name_parser_is_canonical_and_full_segment_only);
	UT_DONE();

	return ut_failed_count == 0 ? 0 : 1;
}
