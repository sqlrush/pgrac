/*-------------------------------------------------------------------------
 * test_pgrac_fenced_target_reply.c
 *    Real management-observation decoder, including hostile reply boundaries.
 *
 * Author: SqlRush <sqlrush@gmail.com>
 * Portions Copyright (c) 2026, pgrac contributors
 * IDENTIFICATION
 *    src/test/cluster_unit/test_pgrac_fenced_target_reply.c
 *-------------------------------------------------------------------------
 */
#include "postgres_fe.h"
#include "pgrac_fenced_target_reply.h"
#undef printf
#undef fprintf
#undef snprintf
#include "unit_test.h"

UT_DEFINE_GLOBALS();

#define CHALLENGE "dddddddddddddddddddddddddddddddd"
#define BOOT "cccccccccccccccccccccccccccccccc"
#define DAEMON "11111111111111111111111111111111"
#define OPERATION "33333333333333333333333333333333"
#define DIGEST "6666666666666666666666666666666666666666666666666666666666666666"
#define INVENTORY "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"
#define JOURNAL "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
#define GATE "9999999999999999999999999999999999999999999999999999999999999999"

static uint8 boot[16], challenge[16], inventory[32];
static PgracFencedTargetV1 target;

static PgracFencedJournalRecordV1
make_record(PgracFencedTargetCommand action)
{
	PgracFencedJournalRecordV1 record = { 0 };
	record.record_kind
		= (action == PGRAC_TARGET_COMPLETE_OFF || action == PGRAC_TARGET_REJOIN_RUNNING
		   || action == PGRAC_TARGET_REJOIN_COMPLETE_OFF)
			  ? 4
			  : 3;
	record.seq = 9;
	record.event_mono_ns = 100;
	record.provider_id = 257;
	record.provider_abi_version = 1;
	record.mapping_generation = 7;
	record.target_state = action >= PGRAC_TARGET_REJOIN_PREPARE_REVOKE ? 1 : 0;
	memset(record.operation_id, 0x33, 16);
	memset(record.daemon_boot_id, 0x11, 16);
	memset(record.semantic_config_digest, 0x22, 32);
	record.intent.kind = action >= PGRAC_TARGET_REJOIN_RESTORE
							 ? PGRAC_FENCED_JOURNAL_INTENT_REJOIN
							 : PGRAC_FENCED_JOURNAL_INTENT_ACQUIRE;
	record.intent.attempt = 3;
	record.intent.system_identifier = UINT64_C(18446744073709551601);
	memset(record.intent.target_uuid, 0x77, 16);
	memset(record.intent.protected_set_digest, 0x66, 32);
	if (record.intent.kind == PGRAC_FENCED_JOURNAL_INTENT_ACQUIRE) {
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
		PgracExternalFenceProtocolRejoinBindingV1 binding = { 0 };
		request->opcode = 5;
		request->old_node_id = 2;
		request->old_incarnation = 91;
		request->candidate_incarnation = 92;
		request->timeout_ms = 30000;
		request->system_identifier = record.intent.system_identifier;
		memset(request->transport_nonce, 0x44, 16);
		memset(request->operation_id, 0x33, 16);
		memset(request->protected_set_digest, 0x66, 32);
		memset(request->rejoin_gate_digest, 0x99, 32);
		binding.system_identifier = record.intent.system_identifier;
		binding.old_node_id = 2;
		binding.old_incarnation = 91;
		binding.candidate_incarnation = 92;
		binding.target_mapping_generation = 7;
		binding.predicate_id = 2;
		binding.predicate_version = 1;
		memset(binding.rejoin_gate_digest, 0x99, 32);
		memset(binding.protected_set_digest, 0x66, 32);
		UT_ASSERT(pgrac_external_fence_rejoin_binding_digest_v1(&binding, record.binding_digest));
	}
	memset(&target, 0, sizeof(target));
	memset(target.target_uuid, 0x77, 16);
	target.victim_node_id = 2;
	target.mapping_generation = 7;
	memset(boot, 0xcc, 16);
	memset(challenge, 0xdd, 16);
	memset(inventory, 0xee, 32);
	return record;
}

static void
reply_fixture(PgracFencedTargetCommand action, char *out, size_t size)
{
	/* Literal golden fields, independent of the production encoder/decoder. */
	static const char *const statuses[] = { "IDENTITY_ONLY",
											"DENY_RECORDED",
											"OFF_DRAIN_UNCERTIFIED",
											"ACCESS_READY_UNCERTIFIED",
											"REJOIN_RUNNING_UNCERTIFIED",
											"REJOIN_REVOKED",
											"OFF_DRAIN_UNCERTIFIED" };
	bool rejoin = action >= PGRAC_TARGET_REJOIN_RESTORE;
	if (action == PGRAC_TARGET_IDENTITY) {
		snprintf(out, size,
				 "{\"challenge\":\"" CHALLENGE "\",\"inventory_digest\":\"" INVENTORY
				 "\",\"status\":\"IDENTITY_ONLY\",\"target_boot_id\":\"" BOOT
				 "\",\"version\":1}\n");
		return;
	}
	snprintf(out, size,
			 "{\"attempt\":3,%s\"challenge\":\"" CHALLENGE "\",\"daemon_boot_id\":\"" DAEMON
			 "\",\"journal_digest\":\"" JOURNAL
			 "\",\"journal_sequence\":4,\"mapping_generation\":7,\"node_id\":2,%s"
			 "\"operation_id\":\"" OPERATION "\",%s\"protected_set_digest\":\"" DIGEST
			 "\",%s%s%s\"status\":\"%s\",\"system_identifier\":18446744073709551601,"
			 "\"target_boot_id\":\"" BOOT "\",\"version\":1}\n",
			 rejoin ? "\"candidate_incarnation\":92," : "", rejoin ? "\"old_incarnation\":91," : "",
			 action == PGRAC_TARGET_REJOIN_RUNNING ? "\"owner_phase\":\"authorize\"," : "",
			 rejoin ? "\"rejoin_gate_digest\":\"" GATE "\"," : "",
			 action == PGRAC_TARGET_COMPLETE_OFF || action == PGRAC_TARGET_REJOIN_COMPLETE_OFF
				 ? "\"route_phases\":[3,3],"
				 : "",
			 action == PGRAC_TARGET_REJOIN_RUNNING ? "\"runtime_id\":12," : "", statuses[action]);
}

static bool
decode(PgracFencedTargetCommand action, const PgracFencedJournalRecordV1 *record, uint32 routes,
	   const char *bytes, size_t length, PgracFencedTargetObservation *out)
{
	return pgrac_fenced_target_reply_decode(action, action == PGRAC_TARGET_IDENTITY ? NULL : record,
											action == PGRAC_TARGET_IDENTITY ? NULL : &target,
											action == PGRAC_TARGET_IDENTITY ? NULL : boot,
											challenge, inventory, routes, bytes, length, out);
}

static void
replace_once(const char *source, const char *old, const char *replacement, char *out)
{
	const char *where = strstr(source, old);
	UT_ASSERT(where != NULL);
	if (where != NULL)
		snprintf(out, 8192, "%.*s%s%s", (int)(where - source), source, replacement,
				 where + strlen(old));
}

static void
refused(PgracFencedTargetCommand action, const PgracFencedJournalRecordV1 *record, uint32 routes,
		const char *bytes, size_t length)
{
	PgracFencedTargetObservation out, zero = { 0 };
	memset(&out, 0xa5, sizeof(out));
	UT_ASSERT(!decode(action, record, routes, bytes, length, &out));
	UT_ASSERT(memcmp(&out, &zero, sizeof(out)) == 0);
}

UT_TEST(test_exact_observations_all_actions_never_claim_proof)
{
	for (unsigned action = 0; action <= PGRAC_TARGET_REJOIN_COMPLETE_OFF; action++) {
		PgracFencedJournalRecordV1 record = make_record(action);
		PgracFencedTargetObservation out;
		char bytes[8192];
		reply_fixture(action, bytes, sizeof(bytes));
		UT_ASSERT(decode(action, &record, action == 0 ? 0 : 2, bytes, strlen(bytes), &out));
		UT_ASSERT(memcmp(out.target_boot_id, boot, 16) == 0);
		UT_ASSERT_EQ(out.journal_sequence, action == 0 ? 0 : 4);
		UT_ASSERT_EQ(out.runtime_id, action == PGRAC_TARGET_REJOIN_RUNNING ? 12 : 0);
		UT_ASSERT_EQ(out.flushed_routes, action == PGRAC_TARGET_COMPLETE_OFF
												 || action == PGRAC_TARGET_REJOIN_COMPLETE_OFF
											 ? 2
											 : 0);
	}
}

UT_TEST(test_each_echo_identity_must_be_exact)
{
	PgracFencedJournalRecordV1 record = make_record(PGRAC_TARGET_REJOIN_RUNNING);
	char bytes[8192], changed[8192];
	const struct {
		const char *old;
		const char *next;
	} cases[] = { { "\"attempt\":3", "\"attempt\":4" },
				  { "\"candidate_incarnation\":92", "\"candidate_incarnation\":93" },
				  { CHALLENGE, "ddddddddddddddddddddddddddddddde" },
				  { DAEMON, "11111111111111111111111111111112" },
				  { "\"mapping_generation\":7", "\"mapping_generation\":8" },
				  { "\"node_id\":2", "\"node_id\":3" },
				  { "\"old_incarnation\":91", "\"old_incarnation\":90" },
				  { OPERATION, "33333333333333333333333333333334" },
				  { "authorize", "refresh" },
				  { DIGEST, "7666666666666666666666666666666666666666666666666666666666666666" },
				  { GATE, "8999999999999999999999999999999999999999999999999999999999999999" },
				  { "18446744073709551601", "18446744073709551600" },
				  { BOOT, "cccccccccccccccccccccccccccccccd" },
				  { "\"version\":1", "\"version\":2" },
				  { "REJOIN_RUNNING_UNCERTIFIED", "PROVEN" } };
	reply_fixture(PGRAC_TARGET_REJOIN_RUNNING, bytes, sizeof(bytes));
	for (size_t n = 0; n < lengthof(cases); n++) {
		replace_once(bytes, cases[n].old, cases[n].next, changed);
		refused(PGRAC_TARGET_REJOIN_RUNNING, &record, 2, changed, strlen(changed));
	}
}

UT_TEST(test_malformed_and_noncanonical_document_is_not_a_reply)
{
	PgracFencedJournalRecordV1 record = make_record(PGRAC_TARGET_PREPARE_DENY);
	char bytes[8192], changed[8192];
	const struct {
		const char *old;
		const char *next;
	} cases[] = { { "{", " {" },
				  { "{", "{\"action\":\"prepare_deny\"," },
				  { "\"attempt\":3", "\"attempt\":3,\"attempt\":3" },
				  { "\"attempt\":3", "\"attempt\":3.0" },
				  { "\"attempt\":3", "\"attempt\":3e0" },
				  { "\"attempt\":3", "\"attempt\":true" },
				  { "\"attempt\":3", "\"attempt\":\"3\"" },
				  { "\"attempt\":3", "\"attempt\":null" },
				  { "\"attempt\":3", "\"attempt\":{\"attempt\":3}" },
				  { "\"attempt\":3", "\"attempt\":[3]" },
				  { "\"attempt\":3", "\"attempt\":03" },
				  { "\"attempt\":3", "\"attempt\": 3" },
				  { "\"attempt\":3,", "" },
				  { "\"attempt\":3,", "\"unknown\":3," },
				  { "\"attempt\"", "\"attem\\u0070t\"" },
				  { "}\n", "}\n\n" },
				  { "}\n", "}{}\n" },
				  { "\"version\":1", "\"version\":1,\"version\":1" },
				  { JOURNAL, "" },
				  { JOURNAL, "0000000000000000000000000000000000000000000000000000000000000000" },
				  { JOURNAL, "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA" },
				  { JOURNAL, "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaag" } };
	reply_fixture(PGRAC_TARGET_PREPARE_DENY, bytes, sizeof(bytes));
	for (size_t n = 0; n < lengthof(cases); n++) {
		replace_once(bytes, cases[n].old, cases[n].next, changed);
		refused(PGRAC_TARGET_PREPARE_DENY, &record, 2, changed, strlen(changed));
	}
	memcpy(changed, bytes, strlen(bytes));
	changed[10] = '\0';
	refused(PGRAC_TARGET_PREPARE_DENY, &record, 2, changed, strlen(bytes));
	memset(changed, '[', sizeof(changed));
	changed[4095] = '\n';
	refused(PGRAC_TARGET_PREPARE_DENY, &record, 2, changed, 4096);
}

UT_TEST(test_journal_and_runtime_exact_integer_boundaries)
{
	PgracFencedJournalRecordV1 record = make_record(PGRAC_TARGET_REJOIN_RUNNING);
	PgracFencedTargetObservation out;
	char bytes[8192], changed[8192];
	const char *bad[] = { "0", "-1", "4.0", "4e0", "\"4\"", "18446744073709551616" };
	const char *runtime[] = { "0", "4294967295", "4294967296", "-1", "12.0", "true" };
	reply_fixture(PGRAC_TARGET_REJOIN_RUNNING, bytes, sizeof(bytes));
	for (size_t n = 0; n < lengthof(bad); n++) {
		char pair[100];
		snprintf(pair, sizeof(pair), "\"journal_sequence\":%s", bad[n]);
		replace_once(bytes, "\"journal_sequence\":4", pair, changed);
		refused(PGRAC_TARGET_REJOIN_RUNNING, &record, 2, changed, strlen(changed));
	}
	replace_once(bytes, "\"journal_sequence\":4", "\"journal_sequence\":18446744073709551615",
				 changed);
	UT_ASSERT(decode(PGRAC_TARGET_REJOIN_RUNNING, &record, 2, changed, strlen(changed), &out));
	UT_ASSERT_EQ(out.journal_sequence, UINT64_MAX);
	for (size_t n = 0; n < lengthof(runtime); n++) {
		char pair[100];
		snprintf(pair, sizeof(pair), "\"runtime_id\":%s", runtime[n]);
		replace_once(bytes, "\"runtime_id\":12", pair, changed);
		refused(PGRAC_TARGET_REJOIN_RUNNING, &record, 2, changed, strlen(changed));
	}
	replace_once(bytes, "\"runtime_id\":12", "\"runtime_id\":4294967294", changed);
	UT_ASSERT(decode(PGRAC_TARGET_REJOIN_RUNNING, &record, 2, changed, strlen(changed), &out));
	UT_ASSERT_EQ(out.runtime_id, UINT32_MAX - 1);
}

UT_TEST(test_every_route_must_be_flushed_with_exact_map_cardinality)
{
	PgracFencedJournalRecordV1 record = make_record(PGRAC_TARGET_COMPLETE_OFF);
	PgracFencedTargetObservation out;
	char bytes[8192], changed[8192], routes[400];
	const char *bad[] = { "[]",		  "[3]",	 "[3,3,3]", "[2,3]",  "[3,0]",	   "[3,4]",
						  "[true,3]", "[3.0,3]", "[[3],3]", "[{},3]", "[\"3\",3]", "[3,3,]" };
	reply_fixture(PGRAC_TARGET_COMPLETE_OFF, bytes, sizeof(bytes));
	for (size_t n = 0; n < lengthof(bad); n++) {
		replace_once(bytes, "[3,3]", bad[n], changed);
		refused(PGRAC_TARGET_COMPLETE_OFF, &record, 2, changed, strlen(changed));
	}
	strcpy(routes, "[");
	for (unsigned n = 0; n < 128; n++)
		strcat(routes, n == 0 ? "3" : ",3");
	strcat(routes, "]");
	replace_once(bytes, "[3,3]", routes, changed);
	UT_ASSERT(decode(PGRAC_TARGET_COMPLETE_OFF, &record, 128, changed, strlen(changed), &out));
	UT_ASSERT_EQ(out.flushed_routes, 128);
	routes[strlen(routes) - 1] = '\0';
	strcat(routes, ",3]");
	replace_once(bytes, "[3,3]", routes, changed);
	refused(PGRAC_TARGET_COMPLETE_OFF, &record, 128, changed, strlen(changed));
}

UT_TEST(test_every_truncation_and_invalid_expectation_zeroes_output)
{
	PgracFencedJournalRecordV1 record = make_record(PGRAC_TARGET_PREPARE_DENY);
	char bytes[8192];
	reply_fixture(PGRAC_TARGET_PREPARE_DENY, bytes, sizeof(bytes));
	for (size_t n = 0; n < strlen(bytes); n++)
		refused(PGRAC_TARGET_PREPARE_DENY, &record, 2, bytes, n);
	refused(PGRAC_TARGET_PREPARE_DENY, &record, 2, bytes, sizeof(bytes));
	refused(PGRAC_TARGET_PREPARE_DENY, &record, 0, bytes, strlen(bytes));
	refused(PGRAC_TARGET_PREPARE_DENY, &record, 129, bytes, strlen(bytes));
	refused(PGRAC_TARGET_PREPARE_DENY, &record, 2, NULL, 1);
	record.record_kind = 4;
	refused(PGRAC_TARGET_PREPARE_DENY, &record, 2, bytes, strlen(bytes));
	record = make_record(PGRAC_TARGET_IDENTITY);
	reply_fixture(PGRAC_TARGET_IDENTITY, bytes, sizeof(bytes));
	inventory[0]++;
	refused(PGRAC_TARGET_IDENTITY, &record, 0, bytes, strlen(bytes));
	memset(inventory, 0, sizeof(inventory));
	refused(PGRAC_TARGET_IDENTITY, &record, 0, bytes, strlen(bytes));
	refused(PGRAC_TARGET_IDENTITY, &record, 1, bytes, strlen(bytes));
}

static int
channel_fixture(int argc, char **argv)
{
	unsigned action;
	PgracFencedJournalRecordV1 record;
	PgracFencedTargetObservation observation;
	char bytes[PGRAC_TARGET_REPLY_MAX_BYTES + 1];
	size_t length;
	if (argc != 4 || strlen(argv[2]) != 1 || argv[2][0] < '0' || argv[2][0] > '6'
		|| strlen(argv[3]) != 1 || argv[3][0] < '1' || argv[3][0] > '9')
		return 2;
	action = argv[2][0] - '0';
	record = make_record(action);
	record.intent.attempt = argv[3][0] - '0';
	if (strcmp(argv[1], "--command") == 0) {
		if (!pgrac_fenced_target_command_encode(
				action, action == 0 ? NULL : &record, action == 0 ? NULL : &target,
				action == 0 ? NULL : boot, challenge, bytes, sizeof(bytes), &length))
			return 2;
		return fwrite(bytes, 1, length, stdout) == length && fflush(stdout) == 0 ? 0 : 2;
	}
	if (strcmp(argv[1], "--decode") != 0)
		return 2;
	length = fread(bytes, 1, sizeof(bytes), stdin);
	return !ferror(stdin)
				   && decode(action, &record, action == 0 ? 0 : 4, bytes, length, &observation)
			   ? 0
			   : 1;
}

int
main(int argc, char **argv)
{
	if (argc != 1)
		return channel_fixture(argc, argv);
	UT_PLAN(6);
	UT_RUN(test_exact_observations_all_actions_never_claim_proof);
	UT_RUN(test_each_echo_identity_must_be_exact);
	UT_RUN(test_malformed_and_noncanonical_document_is_not_a_reply);
	UT_RUN(test_journal_and_runtime_exact_integer_boundaries);
	UT_RUN(test_every_route_must_be_flushed_with_exact_map_cardinality);
	UT_RUN(test_every_truncation_and_invalid_expectation_zeroes_output);
	UT_DONE();
	return ut_failed_count != 0;
}
