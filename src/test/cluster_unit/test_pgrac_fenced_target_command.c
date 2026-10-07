/*-------------------------------------------------------------------------
 * test_pgrac_fenced_target_command.c
 *    Real C owner-to-target encoding and phase/polarity refusal.
 *
 * Author: SqlRush <sqlrush@gmail.com>
 * Portions Copyright (c) 2026, pgrac contributors
 * IDENTIFICATION
 *    src/test/cluster_unit/test_pgrac_fenced_target_command.c
 *-------------------------------------------------------------------------
 */
#include "postgres_fe.h"
#include "pgrac_fenced_target_command.h"

#undef printf
#undef fprintf
#undef snprintf
#include "unit_test.h"

UT_DEFINE_GLOBALS();

static uint8 boot[16], challenge[16];
static PgracFencedTargetV1 target;

static PgracFencedJournalRecordV1
make_record(uint16 opcode, uint16 event, uint32 state)
{
	PgracFencedJournalRecordV1 record;
	memset(&record, 0, sizeof(record));
	record.record_kind = event;
	record.seq = 9;
	record.event_mono_ns = 100;
	record.provider_id = 257;
	record.provider_abi_version = 1;
	record.mapping_generation = 7;
	record.target_state = state;
	memset(record.operation_id, 0x33, 16);
	memset(record.daemon_boot_id, 0x11, 16);
	memset(record.semantic_config_digest, 0x22, 32);
	record.intent.kind
		= opcode == 0 ? PGRAC_FENCED_JOURNAL_INTENT_ACQUIRE : PGRAC_FENCED_JOURNAL_INTENT_REJOIN;
	record.intent.attempt = 3;
	record.intent.system_identifier = UINT64_C(18446744073709551601);
	memset(record.intent.target_uuid, 0x77, 16);
	memset(record.intent.protected_set_digest, 0x66, 32);
	if (opcode == 0) {
		PgracExternalFenceProtocolRequestV1 *request = &record.intent.request.acquire;
		PgracExternalFenceProtocolBindingV1 binding;
		request->need.system_identifier = record.intent.system_identifier;
		request->need.victim_node_id = 2;
		request->need.victim_incarnation = 91;
		request->need.predicate_id = request->need.predicate_version = 1;
		request->timeout_ms = 30000;
		memset(request->request_nonce, 0x44, 16);
		memset(request->need.canonical_duty_digest, 0x55, 32);
		memset(request->need.protected_set_digest, 0x66, 32);
		UT_ASSERT(pgrac_external_fence_binding_from_request_v1(&request->need, 7, &binding));
		UT_ASSERT(pgrac_external_fence_binding_digest_v1(&binding, record.binding_digest));
	} else {
		PgracExternalFenceProtocolRejoinFrameV1 *request = &record.intent.request.rejoin;
		PgracExternalFenceProtocolRejoinBindingV1 binding;
		request->opcode = opcode;
		request->old_node_id = 2;
		request->old_incarnation = 91;
		request->candidate_incarnation = 92;
		request->timeout_ms = 30000;
		memset(request->transport_nonce, 0x44, 16);
		if (opcode != PGRAC_EXTERNAL_FENCE_REJOIN_ADMIN_PREPARE) {
			request->system_identifier = record.intent.system_identifier;
			memset(request->operation_id, 0x33, 16);
			memset(request->protected_set_digest, 0x66, 32);
			memset(request->rejoin_gate_digest, 0x99, 32);
		}
		memset(&binding, 0, sizeof(binding));
		binding.system_identifier = record.intent.system_identifier;
		binding.old_node_id = 2;
		binding.old_incarnation = 91;
		binding.candidate_incarnation = 92;
		binding.target_mapping_generation = 7;
		binding.predicate_id = 2;
		binding.predicate_version = 1;
		memcpy(binding.rejoin_gate_digest, request->rejoin_gate_digest, 32);
		memset(binding.protected_set_digest, 0x66, 32);
		UT_ASSERT(pgrac_external_fence_rejoin_binding_digest_v1(&binding, record.binding_digest));
	}
	memset(&target, 0, sizeof(target));
	memset(target.target_uuid, 0x77, 16);
	target.victim_node_id = 2;
	target.mapping_generation = 7;
	memset(boot, 0xcc, 16);
	memset(challenge, 0xdd, 16);
	return record;
}

static bool
encode(PgracFencedTargetCommand action, const PgracFencedJournalRecordV1 *record, char *out,
	   size_t capacity, size_t *length)
{
	return pgrac_fenced_target_command_encode(action, record, &target, boot, challenge, out,
											  capacity, length);
}

UT_TEST(test_read_only_identity_has_no_operation_or_target)
{
	char out[1024];
	size_t length;
	(void)make_record(0, 3, 0);
	UT_ASSERT(pgrac_fenced_target_command_encode(PGRAC_TARGET_IDENTITY, NULL, NULL, NULL, challenge,
												 out, sizeof(out), &length));
	UT_ASSERT(strcmp(out, "{\"action\":\"identity\",\"challenge\":"
						  "\"dddddddddddddddddddddddddddddddd\",\"version\":1}\n")
			  == 0);
	UT_ASSERT_EQ(length, strlen(out));
	UT_ASSERT(!encode(PGRAC_TARGET_IDENTITY, NULL, out, sizeof(out), &length));
}

UT_TEST(test_exact_acquire_command_keeps_uint64_and_canonical_bytes)
{
	PgracFencedJournalRecordV1 record = make_record(0, 3, 0);
	char out[1024];
	size_t length;
	const char *expected
		= "{\"action\":\"prepare_deny\",\"attempt\":3,"
		  "\"challenge\":\"dddddddddddddddddddddddddddddddd\","
		  "\"daemon_boot_id\":\"11111111111111111111111111111111\","
		  "\"mapping_generation\":7,\"node_id\":2,"
		  "\"operation_id\":\"33333333333333333333333333333333\","
		  "\"protected_set_digest\":"
		  "\"6666666666666666666666666666666666666666666666666666666666666666\","
		  "\"system_identifier\":18446744073709551601,"
		  "\"target_boot_id\":\"cccccccccccccccccccccccccccccccc\",\"version\":1}\n";
	UT_ASSERT(encode(PGRAC_TARGET_PREPARE_DENY, &record, out, sizeof(out), &length));
	UT_ASSERT(strcmp(out, expected) == 0);
	UT_ASSERT_EQ(length, strlen(expected));
	record.record_kind = 4;
	UT_ASSERT(encode(PGRAC_TARGET_COMPLETE_OFF, &record, out, sizeof(out), &length));
	UT_ASSERT(strstr(out, "\"action\":\"complete_off\"") != NULL);
}

UT_TEST(test_phase_matrix_is_exclusive_and_preserves_rejoin_intent)
{
	const struct {
		uint16 opcode, event;
		uint32 state;
		unsigned allowed;
	} cases[]
		= { { 0, 3, 0, 1U << PGRAC_TARGET_PREPARE_DENY },
			{ 0, 4, 0, 1U << PGRAC_TARGET_COMPLETE_OFF },
			{ 1, 2, 0, (1U << PGRAC_TARGET_PREPARE_DENY) | (1U << PGRAC_TARGET_COMPLETE_OFF) },
			{ 5, 3, 0, 1U << PGRAC_TARGET_REJOIN_RESTORE },
			{ 5, 4, 0, 1U << PGRAC_TARGET_REJOIN_RUNNING },
			{ 7, 2, 0, 1U << PGRAC_TARGET_REJOIN_RUNNING },
			{ 5, 3, 1, 1U << PGRAC_TARGET_REJOIN_PREPARE_REVOKE },
			{ 7, 3, 1, 1U << PGRAC_TARGET_REJOIN_PREPARE_REVOKE },
			{ 5, 4, 1, 1U << PGRAC_TARGET_REJOIN_COMPLETE_OFF },
			{ 7, 4, 1, 1U << PGRAC_TARGET_REJOIN_COMPLETE_OFF },
			{ 0, 2, 0, 0 },
			{ 0, 3, 1, 0 },
			{ 5, 2, 0, 0 },
			{ 7, 3, 0, 0 },
			{ 5, 5, 1, 0 },
			{ 7, 9, 2, 0 },
			{ 1, 4, 0, 0 } };
	for (size_t n = 0; n < lengthof(cases); n++) {
		PgracFencedJournalRecordV1 record
			= make_record(cases[n].opcode, cases[n].event, cases[n].state);
		for (unsigned action = 1; action <= PGRAC_TARGET_REJOIN_COMPLETE_OFF; action++) {
			char out[1024];
			size_t length = 99;
			bool allowed = (cases[n].allowed & (1U << action)) != 0;
			bool result = encode(action, &record, out, sizeof(out), &length);
			UT_ASSERT_EQ(result, allowed);
			if (!result) {
				UT_ASSERT_EQ(length, 0);
				UT_ASSERT_EQ(out[0], 0);
			} else if (action >= PGRAC_TARGET_REJOIN_RESTORE) {
				UT_ASSERT(strstr(out, "\"old_incarnation\":91") != NULL);
				UT_ASSERT(strstr(out, "\"candidate_incarnation\":92") != NULL);
				UT_ASSERT(strstr(out, "\"rejoin_gate_digest\":\"99999999") != NULL);
				if (action == PGRAC_TARGET_REJOIN_RUNNING)
					UT_ASSERT(strstr(out, cases[n].opcode == 5 ? "\"owner_phase\":\"authorize\""
															   : "\"owner_phase\":\"refresh\"")
							  != NULL);
			}
		}
	}
}

UT_TEST(test_wrong_target_or_malformed_durable_record_emits_nothing)
{
	for (unsigned n = 0; n < 12; n++) {
		PgracFencedJournalRecordV1 record = make_record(0, 3, 0);
		char out[1024];
		size_t length = 99;
		switch (n) {
		case 0:
			target.target_uuid[0]++;
			break;
		case 1:
			target.mapping_generation++;
			break;
		case 2:
			target.victim_node_id++;
			break;
		case 3:
			target.reserved0 = 1;
			break;
		case 4:
			record.intent.attempt = 0;
			break;
		case 5:
			record.provider_id = 1;
			break;
		case 6:
			record.intent.kind = 0;
			break;
		case 7:
			record.binding_digest[0]++;
			break;
		case 8:
			memset(boot, 0, 16);
			break;
		case 9:
			memset(challenge, 0, 16);
			break;
		case 10:
			record.intent.request.acquire.need.victim_incarnation++;
			break;
		case 11:
			record.seq = 0;
			break;
		}
		memset(out, 'x', sizeof(out));
		UT_ASSERT(!encode(PGRAC_TARGET_PREPARE_DENY, &record, out, sizeof(out), &length));
		UT_ASSERT_EQ(length, 0);
		UT_ASSERT_EQ(out[0], 0);
	}
}

UT_TEST(test_all_short_buffers_refuse_without_partial_command)
{
	PgracFencedJournalRecordV1 record = make_record(5, 3, 0);
	char out[1024];
	size_t full, length;
	UT_ASSERT(encode(PGRAC_TARGET_REJOIN_RESTORE, &record, out, sizeof(out), &full));
	if (full == 0)
		return;
	for (size_t capacity = 0; capacity <= full; capacity++) {
		memset(out, 'x', sizeof(out));
		length = 99;
		UT_ASSERT(!encode(PGRAC_TARGET_REJOIN_RESTORE, &record, out, capacity, &length));
		UT_ASSERT_EQ(length, 0);
		UT_ASSERT_EQ(out[capacity], 'x');
		if (capacity != 0)
			UT_ASSERT_EQ(out[0], 0);
	}
	UT_ASSERT(encode(PGRAC_TARGET_REJOIN_RESTORE, &record, out, full + 1, &length));
	UT_ASSERT_EQ(length, full);
	UT_ASSERT(!encode(99, &record, out, sizeof(out), &length));
	UT_ASSERT(!encode(PGRAC_TARGET_PREPARE_DENY, NULL, out, sizeof(out), &length));
}

int
main(void)
{
	UT_PLAN(5);
	UT_RUN(test_read_only_identity_has_no_operation_or_target);
	UT_RUN(test_exact_acquire_command_keeps_uint64_and_canonical_bytes);
	UT_RUN(test_phase_matrix_is_exclusive_and_preserves_rejoin_intent);
	UT_RUN(test_wrong_target_or_malformed_durable_record_emits_nothing);
	UT_RUN(test_all_short_buffers_refuse_without_partial_command);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
