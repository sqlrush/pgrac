/*-------------------------------------------------------------------------
 * test_pgrac_fenced_target_request.c
 *    Actual map, command and reply composition under an owned context.
 *
 * Author: SqlRush <sqlrush@gmail.com>
 * Portions Copyright (c) 2026, pgrac contributors
 * IDENTIFICATION
 *    src/test/cluster_unit/test_pgrac_fenced_target_request.c
 * NOTES
 *    Only entropy, callback context and external transport are test boundaries.
 *    All command/reply/map validation is production code, not a fake proof.
 *-------------------------------------------------------------------------
 */
#include "postgres_fe.h"
#include <time.h>
#include <unistd.h>
#include "pgrac_fenced_config.h"
#include "pgrac_fenced_target_client.h"
#include "pgrac_fenced_target_request.h"
#undef printf
#undef fprintf
#undef snprintf
#include "unit_test.h"
#include "data/pgrac_fence_map_v2_fixture.h"

UT_DEFINE_GLOBALS();

#define NONCE1 "dddddddddddddddddddddddddddddddd"
#define NONCE2 "dededededededededededededededede"
#define BOOT "cccccccccccccccccccccccccccccccc"
#define INVENTORY "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"
#define DIGEST "84dc62a01725f0361b3fa2de5615632a8fb9a95d07e564fbb3a661d51fd06828"
#define JOURNAL "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
#define OP "33333333333333333333333333333333"
#define DAEMON "11111111111111111111111111111111"
#define GATE "9999999999999999999999999999999999999999999999999999999999999999"

static PgracFencedConfigV1 config;
static PgracFencedJournalRecordV1 record;
static PgracFencedTargetV1 target;
static uint64 deadline;
static unsigned exchanges, random_calls, fault;
static bool have_context, have_record;
static PgracFencedTargetCommand requested;

const struct PgracFencedConfigV1 *
pgrac_fenced_provider_callback_config(void)
{
	return have_context ? &config : NULL;
}
const struct PgracFencedJournalRecordV1 *
pgrac_fenced_provider_callback_record(void)
{
	return have_record ? &record : NULL;
}
uint64_t
pgrac_fenced_provider_callback_deadline_mono_ns(void)
{
	return deadline;
}

bool
pg_strong_random(void *out, size_t length)
{
	random_calls++;
	UT_ASSERT_EQ(length, 16);
	if (fault == 1 || (fault == 2 && random_calls == 2))
		return false;
	memset(out, fault == 13 ? 0xdd : 0xdc + random_calls, length);
	return true;
}

static uint64
now_ns(void)
{
	struct timespec now;
	UT_ASSERT_EQ(clock_gettime(CLOCK_MONOTONIC, &now), 0);
	return (uint64)now.tv_sec * UINT64_C(1000000000) + now.tv_nsec;
}

static const char *names[]
	= { "identity",		  "prepare_deny",		   "complete_off",		 "rejoin_restore",
		"rejoin_running", "rejoin_prepare_revoke", "rejoin_complete_off" };
static const char *statuses[] = { "IDENTITY_ONLY",
								  "DENY_RECORDED",
								  "OFF_DRAIN_UNCERTIFIED",
								  "ACCESS_READY_UNCERTIFIED",
								  "REJOIN_RUNNING_UNCERTIFIED",
								  "REJOIN_REVOKED",
								  "OFF_DRAIN_UNCERTIFIED" };

/* Literal expectations, never produced by the command encoder under test. */
static void
action_frame(char *out, size_t size, bool response)
{
	bool rejoin = requested >= PGRAC_TARGET_REJOIN_RESTORE;
	char action[70], status[70];
	snprintf(action, sizeof(action), "\"action\":\"%s\",", names[requested]);
	snprintf(status, sizeof(status), "\"status\":\"%s\",", statuses[requested]);
	snprintf(out, size,
			 "{%s\"attempt\":3,%s\"challenge\":\"" NONCE2 "\",\"daemon_boot_id\":\"" DAEMON "\",%s"
			 "\"mapping_generation\":7,\"node_id\":2,%s\"operation_id\":\"" OP "\",%s"
			 "\"protected_set_digest\":\"" DIGEST "\",%s%s%s%s\"system_identifier\":123456789,"
			 "\"target_boot_id\":\"" BOOT "\",\"version\":1}\n",
			 response ? "" : action, rejoin ? "\"candidate_incarnation\":92," : "",
			 response ? "\"journal_digest\":\"" JOURNAL "\",\"journal_sequence\":4," : "",
			 rejoin ? "\"old_incarnation\":91," : "",
			 requested == PGRAC_TARGET_REJOIN_RUNNING ? "\"owner_phase\":\"authorize\"," : "",
			 rejoin ? "\"rejoin_gate_digest\":\"" GATE "\"," : "",
			 response
					 && (requested == PGRAC_TARGET_COMPLETE_OFF
						 || requested == PGRAC_TARGET_REJOIN_COMPLETE_OFF)
				 ? "\"route_phases\":[3,3,3,3],"
				 : "",
			 response && requested == PGRAC_TARGET_REJOIN_RUNNING ? "\"runtime_id\":12," : "",
			 response ? status : "");
}

bool
pgrac_fenced_target_client_exchange(const PgracFencedTargetClientPaths *paths, const char *command,
									size_t length, uint64 bound, char *output, size_t capacity,
									size_t *written)
{
	char expected[1024];
	exchanges++;
	UT_ASSERT_EQ(bound, deadline);
	UT_ASSERT_STR_EQ(paths->bundle_directory, "/protected/client");
	UT_ASSERT_STR_EQ(paths->config_file, "/protected/client.json");
	UT_ASSERT_EQ(length, strlen(command));
	UT_ASSERT(exchanges <= 2);
	if (exchanges == 1) {
		UT_ASSERT_STR_EQ(command,
						 "{\"action\":\"identity\",\"challenge\":\"" NONCE1 "\",\"version\":1}\n");
		snprintf(output, capacity,
				 "{\"challenge\":\"" NONCE1 "\",\"inventory_digest\":\"" INVENTORY
				 "\",\"status\":\"IDENTITY_ONLY\",\"target_boot_id\":\"" BOOT
				 "\",\"version\":1}\n");
	} else {
		action_frame(expected, sizeof(expected), false);
		UT_ASSERT_STR_EQ(command, expected);
		action_frame(output, capacity, true);
	}
	if ((fault == 3 && exchanges == 1) || (fault == 4 && exchanges == 2))
		return false;
	if (fault == 5 && exchanges == 1)
		strstr(output, INVENTORY)[0] = 'f';
	if (fault == 6 && exchanges == 2)
		strstr(output, BOOT)[0] = 'b';
	if (fault == 7 && exchanges == 2)
		strstr(output, NONCE2)[0] = 'a';
	if (fault == 8 && exchanges == 2)
		strstr(output, "[3,3,3,3]")[1] = '2';
	if (fault == 9 && exchanges == 2)
		strstr(output, OP)[0] = '4';
	if (fault == 10 && exchanges == 2)
		output[0] = 'x';
	if ((fault == 11 && exchanges == 1) || (fault == 12 && exchanges == 2))
		while (now_ns() < deadline)
			usleep(1000);
	*written = strlen(output);
	return true;
}

#ifdef USE_OPENSSL
static void
fixture(PgracFencedTargetCommand action)
{
	PgracExternalFenceProtocolBindingV1 binding;
	PgracExternalFenceProtocolRejoinBindingV1 rejoin = { 0 };
	UT_ASSERT_EQ(pgrac_fenced_config_parse((const uint8 *)fenced_config_v2,
										   strlen(fenced_config_v2), &config),
				 PGRAC_FENCED_CONFIG_OK);
	config.native.present = true;
	strcpy(config.native.bundle_directory, "/protected/client");
	strcpy(config.native.client_config, "/protected/client.json");
	memset(config.native.inventory_digest, 0xee, 32);
	memset(&target, 0, sizeof(target));
	target.victim_node_id = 2;
	target.mapping_generation = 7;
	memset(target.target_uuid, 5, 16);
	target.adapter_config = config.nodes[2].adapter_data;
	target.adapter_config_len = config.nodes[2].adapter_data_len;
	memset(&record, 0, sizeof(record));
	record.record_kind = (action == 2 || action == 4 || action == 6) ? 4 : 3;
	record.seq = 9;
	record.event_mono_ns = 100;
	record.provider_id = 257;
	record.provider_abi_version = 1;
	record.mapping_generation = 7;
	record.target_state = action >= 5 ? 1 : 0;
	memset(record.operation_id, 0x33, 16);
	memset(record.daemon_boot_id, 0x11, 16);
	memset(record.semantic_config_digest, 0x22, 32);
	record.intent.kind = action >= 3 ? 2 : 1;
	record.intent.attempt = 3;
	record.intent.system_identifier = 123456789;
	memset(record.intent.target_uuid, 5, 16);
	memcpy(record.intent.protected_set_digest, config.nodes[2].protected_set_digest, 32);
	if (action < 3) {
		PgracExternalFenceProtocolRequestV1 *request = &record.intent.request.acquire;
		request->need.system_identifier = 123456789;
		request->need.victim_node_id = 2;
		request->need.victim_incarnation = 91;
		request->need.predicate_id = request->need.predicate_version = 1;
		request->timeout_ms = 1000;
		memset(request->request_nonce, 0x44, 16);
		memset(request->need.canonical_duty_digest, 0x55, 32);
		memcpy(request->need.protected_set_digest, record.intent.protected_set_digest, 32);
		UT_ASSERT(pgrac_external_fence_binding_from_request_v1(&request->need, 7, &binding));
		UT_ASSERT(pgrac_external_fence_binding_digest_v1(&binding, record.binding_digest));
	} else {
		PgracExternalFenceProtocolRejoinFrameV1 *request = &record.intent.request.rejoin;
		request->opcode = 5;
		request->old_node_id = 2;
		request->old_incarnation = 91;
		request->candidate_incarnation = 92;
		request->timeout_ms = 1000;
		request->system_identifier = 123456789;
		memset(request->transport_nonce, 0x44, 16);
		memset(request->operation_id, 0x33, 16);
		memset(request->rejoin_gate_digest, 0x99, 32);
		memcpy(request->protected_set_digest, record.intent.protected_set_digest, 32);
		rejoin.system_identifier = 123456789;
		rejoin.old_node_id = 2;
		rejoin.old_incarnation = 91;
		rejoin.candidate_incarnation = 92;
		rejoin.target_mapping_generation = 7;
		rejoin.predicate_id = 2;
		rejoin.predicate_version = 1;
		memset(rejoin.rejoin_gate_digest, 0x99, 32);
		memcpy(rejoin.protected_set_digest, record.intent.protected_set_digest, 32);
		UT_ASSERT(pgrac_external_fence_rejoin_binding_digest_v1(&rejoin, record.binding_digest));
	}
	requested = action;
	exchanges = random_calls = fault = 0;
	have_context = true;
	have_record = action != PGRAC_TARGET_IDENTITY;
	deadline = now_ns() + UINT64_C(1000000000);
}
#endif

UT_TEST(test_two_leg_request_preserves_exact_context_and_returns_no_proof)
{
#ifdef USE_OPENSSL
	PgracFencedTargetRequestResult out;
	for (int action = 1; action <= 6; action++) {
		fixture(action);
		UT_ASSERT(pgrac_fenced_target_request(action, &target, &out));
		UT_ASSERT_EQ(exchanges, 2);
		UT_ASSERT_EQ(random_calls, 2);
		UT_ASSERT_EQ(out.observation.journal_sequence, 4);
		UT_ASSERT_EQ(out.expected.attempt, 3);
		UT_ASSERT_EQ(out.expected.mapping_generation, 7);
		UT_ASSERT_EQ(out.expected.route_count, 4);
		UT_ASSERT_EQ(out.expected.challenge[0], 0xde);
		UT_ASSERT_EQ(out.expected.target_boot_id[0], 0xcc);
		UT_ASSERT_EQ(out.expected.daemon_boot_id[0], 0x11);
		UT_ASSERT_EQ(out.expected.guest_uuid[0], 5);
	}
#endif
}

UT_TEST(test_identity_only_cannot_be_a_drain_expectation)
{
#ifdef USE_OPENSSL
	PgracFencedTargetRequestResult out;
	PgracFencedDrainIdentityV1 zero = { 0 };
	fixture(PGRAC_TARGET_IDENTITY);
	UT_ASSERT(pgrac_fenced_target_request(PGRAC_TARGET_IDENTITY, &target, &out));
	UT_ASSERT_EQ(exchanges, 1);
	UT_ASSERT_EQ(out.observation.target_boot_id[0], 0xcc);
	UT_ASSERT(memcmp(&out.expected, &zero, sizeof(zero)) == 0);
#endif
}

UT_TEST(test_invalid_context_or_phase_has_no_external_side_effect)
{
#ifdef USE_OPENSSL
	PgracFencedTargetRequestResult out, zero = { 0 };
	for (int variant = 0; variant < 17; variant++) {
		fixture(PGRAC_TARGET_COMPLETE_OFF);
		switch (variant) {
		case 0:
			have_context = false;
			break;
		case 1:
			have_record = false;
			break;
		case 2:
			config.native.present = false;
			break;
		case 3:
			target.victim_node_id = 128;
			break;
		case 4:
			target.target_uuid[0] ^= 1;
			break;
		case 5:
			config.nodes[2].adapter_data[80] ^= 1;
			break;
		case 6:
			record.record_kind = 3;
			break;
		case 7:
			deadline = now_ns() - 1;
			break;
		case 8:
			config.system_identifier++;
			break;
		case 9:
			record.intent.protected_set_digest[0] ^= 1;
			break;
		case 10:
			target.adapter_config_len--;
			break;
		case 11:
			config.provider_abi++;
			break;
		case 12:
			record.provider_id++;
			break;
		case 13:
			config.native.bundle_directory[0] = '\0';
			break;
		case 14:
			memset(config.native.inventory_digest, 0, 32);
			break;
		case 15:
			target.reserved0 = 1;
			break;
		case 16:
			requested = (PgracFencedTargetCommand)77;
			break;
		}
		memset(&out, 0x7f, sizeof(out));
		UT_ASSERT(!pgrac_fenced_target_request(requested, &target, &out));
		UT_ASSERT(memcmp(&out, &zero, sizeof(out)) == 0);
		UT_ASSERT_EQ(exchanges, 0);
	}
#endif
}

UT_TEST(test_reply_loss_changes_entropy_and_expiry_clear_output)
{
#ifdef USE_OPENSSL
	PgracFencedTargetRequestResult out, zero = { 0 };
	for (unsigned variant = 1; variant <= 13; variant++) {
		fixture(PGRAC_TARGET_COMPLETE_OFF);
		fault = variant;
		if (fault == 11 || fault == 12)
			deadline = now_ns() + UINT64_C(20000000);
		memset(&out, 0x7f, sizeof(out));
		UT_ASSERT(!pgrac_fenced_target_request(PGRAC_TARGET_COMPLETE_OFF, &target, &out));
		UT_ASSERT(memcmp(&out, &zero, sizeof(out)) == 0);
		UT_ASSERT(exchanges <= 2);
		if (fault == 1)
			UT_ASSERT_EQ(exchanges, 0);
		if (fault == 3 || fault == 5 || fault == 11)
			UT_ASSERT_EQ(exchanges, 1);
	}
#endif
}

UT_TEST(test_missing_config_never_uses_ambient_paths)
{
	PgracFencedTargetRequestResult out, zero = { 0 };
	have_context = false;
	exchanges = 0;
	memset(&out, 0x7f, sizeof(out));
	UT_ASSERT(!pgrac_fenced_target_request(PGRAC_TARGET_IDENTITY, &target, &out));
	UT_ASSERT(memcmp(&out, &zero, sizeof(out)) == 0);
	UT_ASSERT_EQ(exchanges, 0);
}

int
main(void)
{
	UT_PLAN(5);
	UT_RUN(test_two_leg_request_preserves_exact_context_and_returns_no_proof);
	UT_RUN(test_identity_only_cannot_be_a_drain_expectation);
	UT_RUN(test_invalid_context_or_phase_has_no_external_side_effect);
	UT_RUN(test_reply_loss_changes_entropy_and_expiry_clear_output);
	UT_RUN(test_missing_config_never_uses_ambient_paths);
	UT_DONE();
	return ut_failed_count ? 1 : 0;
}
