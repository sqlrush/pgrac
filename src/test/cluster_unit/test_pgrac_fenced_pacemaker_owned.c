/*-------------------------------------------------------------------------
 * test_pgrac_fenced_pacemaker_owned.c
 *    Real owner/map/phase validation with external actions at test boundaries.
 *
 * Author: SqlRush <sqlrush@gmail.com>
 * Portions Copyright (c) 2026, pgrac contributors
 * IDENTIFICATION
 *    src/test/cluster_unit/test_pgrac_fenced_pacemaker_owned.c
 * NOTES
 *    No live target or native power actions; no test result is a certificate.
 *-------------------------------------------------------------------------
 */
#include "postgres_fe.h"
#include <time.h>
#include <unistd.h>
#include "pgrac_fenced_cib.h"
#include "pgrac_fenced_config.h"
#include "pgrac_fenced_pacemaker.h"
#include "pgrac_fenced_pacemaker_owned.h"
#include "pgrac_fenced_target_request.h"
#undef printf
#undef fprintf
#undef snprintf
#include "unit_test.h"
#include "data/pgrac_fence_map_v2_fixture.h"

UT_DEFINE_GLOBALS();

static PgracFencedConfigV1 config;
static PgracFencedJournalRecordV1 record;
static PgracFencedTargetV1 target;
static uint64 deadline;
static bool have_config, have_record, requested_on, compensate;
static unsigned steps, fail_step, expire_step;
static char sequence[16];

const struct PgracFencedConfigV1 *
pgrac_fenced_provider_callback_config(void)
{
	return have_config ? &config : NULL;
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

static uint64
now_ns(void)
{
	struct timespec now;
	UT_ASSERT_EQ(clock_gettime(CLOCK_MONOTONIC, &now), 0);
	return (uint64)now.tv_sec * UINT64_C(1000000000) + now.tv_nsec;
}

static bool
event(char kind)
{
	UT_ASSERT(steps < 5);
	if (steps >= 5)
		return false;
	sequence[steps++] = kind;
	sequence[steps] = '\0';
	if (steps == expire_step)
		while (now_ns() < deadline)
			usleep(1000);
	return steps != fail_step;
}

PgracFencedProviderResult
pgrac_fenced_cib_check(uint64 bound, const PgracFencedCibPolicy *policy,
					   PgracFencedCibObservation *out)
{
	UT_ASSERT_EQ(bound, deadline);
	UT_ASSERT_STR_EQ(policy->resource, "pgrac-fence");
	UT_ASSERT_EQ(policy->node_count, 1);
	UT_ASSERT_STR_EQ(policy->nodes[0].name, "node-2");
	UT_ASSERT_EQ(policy->nodes[0].guest_uuid[0], 5);
	UT_ASSERT_EQ(policy->configuration_digest[0], 0xbb);
	memset(out, 0, sizeof(*out));
	memcpy(out->configuration_digest, policy->configuration_digest, 32);
	return event('C') ? PGRAC_FENCED_PROVIDER_OK : PGRAC_FENCED_PROVIDER_CONFIG_ERROR;
}

bool
pgrac_fenced_target_request(PgracFencedTargetCommand action, const PgracFencedTargetV1 *received,
							PgracFencedTargetRequestResult *out)
{
	UT_ASSERT(received == &target);
	UT_ASSERT_EQ(action, requested_on ? PGRAC_TARGET_REJOIN_RESTORE
						 : compensate ? PGRAC_TARGET_REJOIN_PREPARE_REVOKE
									  : PGRAC_TARGET_PREPARE_DENY);
	memset(out, 0, sizeof(*out));
	return event('T');
}

PgracFencedProviderResult
pgrac_fenced_pacemaker_action(const PgracFencedPacemakerActionV1 *action,
							  PgracFencedPacemakerActionResultV1 *out)
{
	UT_ASSERT_STR_EQ(action->node, "node-2");
	UT_ASSERT_EQ(action->attempt, 3);
	UT_ASSERT(memcmp(action->operation_id, record.operation_id, 16) == 0);
	UT_ASSERT_EQ(action->deadline_mono_ns, deadline);
	UT_ASSERT_EQ(action->turn_on, requested_on);
	memset(out, 0, sizeof(*out));
	return event('A') ? PGRAC_FENCED_PROVIDER_OK : PGRAC_FENCED_PROVIDER_UNKNOWN;
}

#ifdef USE_PACEMAKER
static void
fixture(unsigned mode)
{
	PgracExternalFenceProtocolBindingV1 binding;
	PgracExternalFenceProtocolRejoinBindingV1 rejoin = { 0 };
	UT_ASSERT_EQ(pgrac_fenced_config_parse((const uint8 *)fenced_config_v2,
										   strlen(fenced_config_v2), &config),
				 PGRAC_FENCED_CONFIG_OK);
	config.native.present = true;
	strcpy(config.native.resource, "pgrac-fence");
	strcpy(config.native.node_names[2], "node-2");
	memset(config.native.cib_digest, 0xbb, 32);
	memset(&target, 0, sizeof(target));
	target.victim_node_id = 2;
	target.mapping_generation = 7;
	memset(target.target_uuid, 5, 16);
	target.adapter_config = config.nodes[2].adapter_data;
	target.adapter_config_len = config.nodes[2].adapter_data_len;
	memset(&record, 0, sizeof(record));
	record.record_kind = PGRAC_FENCED_JOURNAL_KIND_ACTUATION_ISSUED;
	record.seq = 9;
	record.event_mono_ns = 100;
	record.provider_id = 257;
	record.provider_abi_version = 1;
	record.mapping_generation = 7;
	record.target_state
		= mode == 2 ? PGRAC_FENCED_JOURNAL_TARGET_OFF : PGRAC_FENCED_JOURNAL_TARGET_NONE;
	memset(record.operation_id, 0x33, 16);
	memset(record.daemon_boot_id, 0x11, 16);
	memset(record.semantic_config_digest, 0x22, 32);
	record.intent.kind
		= mode == 0 ? PGRAC_FENCED_JOURNAL_INTENT_ACQUIRE : PGRAC_FENCED_JOURNAL_INTENT_REJOIN;
	record.intent.attempt = 3;
	record.intent.system_identifier = 123456789;
	memset(record.intent.target_uuid, 5, 16);
	memcpy(record.intent.protected_set_digest, config.nodes[2].protected_set_digest, 32);
	if (mode == 0) {
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
		request->opcode = PGRAC_EXTERNAL_FENCE_REJOIN_LMON_AUTHORIZE_ON;
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
	requested_on = mode == 1;
	compensate = mode == 2;
	have_config = have_record = true;
	steps = fail_step = expire_step = 0;
	memset(sequence, 0, sizeof(sequence));
	deadline = now_ns() + UINT64_C(1000000000);
}

UT_TEST(test_exact_off_on_and_compensation_sequence)
{
	for (unsigned mode = 0; mode < 3; mode++) {
		fixture(mode);
		UT_ASSERT_EQ(pgrac_fenced_pacemaker_owned_actuate(&target, requested_on),
					 PGRAC_FENCED_PROVIDER_OK);
		UT_ASSERT_STR_EQ(sequence, "CTCAC");
	}
}

UT_TEST(test_each_failure_stops_without_implicit_compensation)
{
	for (unsigned mode = 0; mode < 3; mode++)
		for (unsigned failure = 1; failure <= 5; failure++) {
			fixture(mode);
			fail_step = failure;
			UT_ASSERT_NE(pgrac_fenced_pacemaker_owned_actuate(&target, requested_on),
						 PGRAC_FENCED_PROVIDER_OK);
			UT_ASSERT_EQ(steps, failure);
		}
}

UT_TEST(test_deadline_is_not_renewed_after_prepare_or_action)
{
	for (unsigned mode = 0; mode < 3; mode++)
		for (unsigned expiry = 2; expiry <= 4; expiry += 2) {
			fixture(mode);
			expire_step = expiry;
			deadline = now_ns() + UINT64_C(20000000);
			UT_ASSERT_NE(pgrac_fenced_pacemaker_owned_actuate(&target, requested_on),
						 PGRAC_FENCED_PROVIDER_OK);
			UT_ASSERT_EQ(steps, expiry);
		}
}

UT_TEST(test_invalid_owned_identity_or_phase_causes_no_external_action)
{
	for (unsigned variant = 0; variant < 13; variant++) {
		fixture(0);
		switch (variant) {
		case 0:
			have_config = false;
			break;
		case 1:
			have_record = false;
			break;
		case 2:
			config.native.present = false;
			break;
		case 3:
			record.record_kind = PGRAC_FENCED_JOURNAL_KIND_ACTUATION_RESULT;
			break;
		case 4:
			requested_on = true;
			break;
		case 5:
			target.victim_node_id = 128;
			break;
		case 6:
			target.target_uuid[0] ^= 1;
			break;
		case 7:
			config.system_identifier++;
			break;
		case 8:
			record.intent.protected_set_digest[0] ^= 1;
			break;
		case 9:
			target.adapter_config_len--;
			break;
		case 10:
			deadline = now_ns() - 1;
			break;
		case 11:
			record.binding_digest[0] ^= 1;
			break;
		case 12:
			config.nodes[2].adapter_data[80] ^= 1;
			break;
		}
		UT_ASSERT_NE(pgrac_fenced_pacemaker_owned_actuate(&target, requested_on),
					 PGRAC_FENCED_PROVIDER_OK);
		UT_ASSERT_EQ(steps, 0);
	}
}
#else
UT_TEST(test_disabled_build_cannot_send_preparation_or_power)
{
	UT_ASSERT_EQ(pgrac_fenced_pacemaker_owned_actuate(NULL, true),
				 PGRAC_FENCED_PROVIDER_UNAVAILABLE);
	UT_ASSERT_EQ(pgrac_fenced_pacemaker_owned_actuate(&target, false),
				 PGRAC_FENCED_PROVIDER_UNAVAILABLE);
	UT_ASSERT_EQ(steps, 0);
}
#endif

int
main(void)
{
#ifdef USE_PACEMAKER
	UT_PLAN(4);
	UT_RUN(test_exact_off_on_and_compensation_sequence);
	UT_RUN(test_each_failure_stops_without_implicit_compensation);
	UT_RUN(test_deadline_is_not_renewed_after_prepare_or_action);
	UT_RUN(test_invalid_owned_identity_or_phase_causes_no_external_action);
#else
	UT_PLAN(1);
	UT_RUN(test_disabled_build_cannot_send_preparation_or_power);
#endif
	UT_DONE();
	return ut_failed_count ? 1 : 0;
}
