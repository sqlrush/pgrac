/*-------------------------------------------------------------------------
 *
 * test_pgrac_fenced_rejoin.c
 *    Root-daemon PFRJ rejoin lifecycle tests.
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <fcntl.h>
#include <stdlib.h>
#include <sys/mman.h>
#include <time.h>
#include <unistd.h>

#include "pgrac_fenced_rejoin.h"

#undef printf
#undef fprintf
#undef snprintf

#include "unit_test.h"

UT_DEFINE_GLOBALS();

#include "data/pgrac_fence_map_v2_fixture.h"

static volatile uint32 *provider_state;
static const char *owned_journal_path;
static size_t read_records(int fd, PgracFencedJournalRecordV1 *records, size_t maximum);

static bool
callback_matches_durable_event(void)
{
	PgracFencedJournalRecordV1 records[30];
	const PgracFencedJournalRecordV1 *actual = pgrac_fenced_provider_callback_record();
	uint8 observed[768], durable[768];
	size_t observed_len, durable_len, count;
	int fd;

	if (owned_journal_path == NULL)
		return true;
	fd = open(owned_journal_path, O_RDONLY);
	if (fd < 0)
		return false;
	count = read_records(fd, records, lengthof(records));
	(void)close(fd);
	return actual != NULL && count != 0
		   && pgrac_fenced_journal_frame_encode(actual, observed, sizeof(observed), &observed_len)
		   && pgrac_fenced_journal_frame_encode(&records[count - 1], durable, sizeof(durable),
												&durable_len)
		   && observed_len == durable_len && memcmp(observed, durable, observed_len) == 0;
}

static PgracFencedProviderResult
test_resolve(const PgracFencedTargetV1 *configured, PgracFencedTargetV1 *resolved,
			 int32 *native_status)
{
	provider_state[0]++;
	if (provider_state[7] != 0) {
		*native_status = 71;
		return PGRAC_FENCED_PROVIDER_CONFIG_ERROR;
	}
	*resolved = *configured;
	*native_status = 0;
	return PGRAC_FENCED_PROVIDER_OK;
}

static PgracFencedProviderResult
test_actuate_off(const PgracFencedTargetV1 *target, uint64_t deadline_mono_ns, int32 *native_status)
{
	if (!callback_matches_durable_event())
		return PGRAC_FENCED_PROVIDER_CONFIG_ERROR;
	if (owned_journal_path != NULL) {
		PgracFencedJournalRecordV1 records[30];
		int fd = open(owned_journal_path, O_RDONLY);
		size_t count = fd < 0 ? 0 : read_records(fd, records, lengthof(records));

		if (count >= 2) {
			const PgracFencedJournalRecordV1 *accepted = &records[count - 2];
			const PgracFencedJournalRecordV1 *issued = &records[count - 1];

			provider_state[8] = accepted->record_kind == PGRAC_FENCED_JOURNAL_KIND_REQUEST_ACCEPTED
								&& issued->record_kind == PGRAC_FENCED_JOURNAL_KIND_ACTUATION_ISSUED
								&& issued->intent.kind == PGRAC_FENCED_JOURNAL_INTENT_REJOIN
								&& issued->intent.attempt >= 3
								&& issued->target_state == PGRAC_FENCED_TARGET_OFF
								&& issued->daemon_boot_id[0] == 0x82
								&& memcmp(issued->intent.target_uuid, target->target_uuid, 16) == 0
								&& memcmp(issued->operation_id, accepted->operation_id, 16) == 0;
		}
		if (fd >= 0)
			(void)close(fd);
	}
	(void)deadline_mono_ns;
	provider_state[1]++;
	provider_state[4] = PGRAC_FENCED_TARGET_OFF;
	*native_status = 0;
	return PGRAC_FENCED_PROVIDER_OK;
}

static PgracFencedProviderResult
test_readback(const PgracFencedTargetV1 *target, uint64_t deadline_mono_ns,
			  PgracFencedReadbackV1 *readback)
{
	if (!callback_matches_durable_event())
		return PGRAC_FENCED_PROVIDER_CONFIG_ERROR;
	(void)deadline_mono_ns;
	provider_state[2]++;
	if (provider_state[6] > 0) {
		provider_state[6]--;
		return PGRAC_FENCED_PROVIDER_UNKNOWN;
	}
	memset(readback, 0, sizeof(*readback));
	readback->state = provider_state[4];
	readback->io_drain_state = provider_state[5];
	memcpy(readback->observed_target_uuid, target->target_uuid,
		   sizeof(readback->observed_target_uuid));
	return PGRAC_FENCED_PROVIDER_OK;
}

static PgracFencedProviderResult
test_actuate_on(const PgracFencedTargetV1 *target, uint64_t deadline_mono_ns, int32 *native_status)
{
	if (!callback_matches_durable_event())
		return PGRAC_FENCED_PROVIDER_CONFIG_ERROR;
	if (owned_journal_path != NULL) {
		PgracFencedJournalRecordV1 records[12];
		int fd = open(owned_journal_path, O_RDONLY);
		size_t count = fd < 0 ? 0 : read_records(fd, records, lengthof(records));

		if (count >= 2) {
			const PgracFencedJournalRecordV1 *accepted = &records[count - 2];
			const PgracFencedJournalRecordV1 *issued = &records[count - 1];

			provider_state[8] = accepted->record_kind == PGRAC_FENCED_JOURNAL_KIND_REQUEST_ACCEPTED
								&& issued->record_kind == PGRAC_FENCED_JOURNAL_KIND_ACTUATION_ISSUED
								&& issued->intent.kind == PGRAC_FENCED_JOURNAL_INTENT_REJOIN
								&& issued->intent.attempt == 2
								&& issued->intent.request.rejoin.opcode
									   == PGRAC_EXTERNAL_FENCE_REJOIN_LMON_AUTHORIZE_ON
								&& memcmp(issued->intent.target_uuid, target->target_uuid, 16) == 0
								&& memcmp(issued->operation_id, accepted->operation_id, 16) == 0;
		}
		if (fd >= 0)
			(void)close(fd);
	}
	(void)target;
	(void)deadline_mono_ns;
	provider_state[3]++;
	provider_state[4] = PGRAC_FENCED_TARGET_ON;
	*native_status = 0;
	return PGRAC_FENCED_PROVIDER_OK;
}

static void
test_shutdown(void)
{}

static uint64
deadline_after_ms(uint64 milliseconds)
{
	struct timespec now;

	UT_ASSERT_EQ(clock_gettime(CLOCK_MONOTONIC, &now), 0);
	return (uint64)now.tv_sec * UINT64_C(1000000000) + (uint64)now.tv_nsec
		   + milliseconds * UINT64_C(1000000);
}

static void
make_config(PgracFencedConfigV1 *config)
{
	memset(config, 0, sizeof(*config));
	config->format_version = 1;
	config->mapping_generation = 17;
	config->system_identifier = 9001;
	config->storage_backend_id = 2;
	memset(config->storage_uuid, 0x31, sizeof(config->storage_uuid));
	config->provider_id = PGRAC_FENCED_PROVIDER_ID_TEST_ONLY;
	config->provider_abi = PGRAC_FENCED_PROVIDER_ABI_V1;
	config->node_count = 2;
	config->nodes[3].present = true;
	memset(config->nodes[3].target_uuid, 0x41, sizeof(config->nodes[3].target_uuid));
	config->nodes[5].present = true;
	memset(config->nodes[5].target_uuid, 0x51, sizeof(config->nodes[5].target_uuid));
}

static void
make_ops(PgracFencedProviderOpsV1 *ops)
{
	memset(ops, 0, sizeof(*ops));
	ops->abi_version = PGRAC_FENCED_PROVIDER_ABI_V1;
	ops->struct_size = sizeof(*ops);
	ops->provider_id = PGRAC_FENCED_PROVIDER_ID_TEST_ONLY;
	ops->provider_name = "rejoin-test";
	ops->resolve = test_resolve;
	ops->actuate_off = test_actuate_off;
	ops->readback = test_readback;
	ops->actuate_on = test_actuate_on;
	ops->shutdown = test_shutdown;
}

static int
open_context(PgracFencedOperationContextV1 *operation_context,
			 PgracFencedRejoinContextV1 *rejoin_context, PgracFencedJournalScanState *journal_state,
			 PgracFencedConfigV1 *config, PgracFencedProviderOpsV1 *ops, char path[64])
{
	uint8 config_digest[32];
	uint8 daemon_boot_id[16];
	int fd;

	memset(config_digest, 0x71, sizeof(config_digest));
	memset(daemon_boot_id, 0x81, sizeof(daemon_boot_id));
	strcpy(path, "/tmp/pgrac-fenced-rejoin.XXXXXX");
	fd = mkstemp(path);
	UT_ASSERT(fd >= 0);
	if (fd < 0)
		return -1;
	UT_ASSERT_EQ(fcntl(fd, F_SETFL, fcntl(fd, F_GETFL) | O_APPEND), 0);
	pgrac_fenced_journal_scan_state_init(journal_state);
	UT_ASSERT(pgrac_fenced_operation_context_init(
		operation_context, config, ops, true, config_digest, daemon_boot_id, fd, journal_state));
	UT_ASSERT(pgrac_fenced_rejoin_init(rejoin_context, operation_context));
	return fd;
}

static void
make_prepare(int node_id, uint64 old_incarnation, uint64 candidate_incarnation, uint8 nonce,
			 PgracExternalFenceProtocolRejoinFrameV1 *request)
{
	memset(request, 0, sizeof(*request));
	request->opcode = PGRAC_EXTERNAL_FENCE_REJOIN_ADMIN_PREPARE;
	memset(request->transport_nonce, nonce, sizeof(request->transport_nonce));
	request->old_node_id = node_id;
	request->old_incarnation = old_incarnation;
	request->candidate_incarnation = candidate_incarnation;
	request->timeout_ms = 1000;
}

static void
make_claim(uint8 nonce, PgracExternalFenceProtocolRejoinFrameV1 *request)
{
	memset(request, 0, sizeof(*request));
	request->opcode = PGRAC_EXTERNAL_FENCE_REJOIN_LMON_CLAIM_NEXT;
	memset(request->transport_nonce, nonce, sizeof(request->transport_nonce));
}

static void
make_bound_request(uint16 opcode, const PgracExternalFenceProtocolRejoinFrameV1 *source,
				   uint8 nonce, PgracExternalFenceProtocolRejoinFrameV1 *request)
{
	memset(request, 0, sizeof(*request));
	request->opcode = opcode;
	memset(request->transport_nonce, nonce, sizeof(request->transport_nonce));
	memcpy(request->operation_id, source->operation_id, sizeof(request->operation_id));
	request->system_identifier = source->system_identifier;
	memset(request->rejoin_gate_digest, 0x91, sizeof(request->rejoin_gate_digest));
	memcpy(request->protected_set_digest, source->protected_set_digest,
		   sizeof(request->protected_set_digest));
	request->old_node_id = source->old_node_id;
	request->old_incarnation = source->old_incarnation;
	request->candidate_incarnation = source->candidate_incarnation;
	request->timeout_ms = 1000;
}

static size_t
read_records(int fd, PgracFencedJournalRecordV1 *records, size_t maximum)
{
	uint8 frame[PGRAC_FENCED_JOURNAL_MAX_RECORD_BYTES];
	size_t count = 0;
	size_t length;
	ssize_t got;

	UT_ASSERT_EQ(lseek(fd, 0, SEEK_SET), 0);
	while (count < maximum) {
		do {
			got = read(fd, frame, 16);
		} while (got < 0 && errno == EINTR);
		if (got == 0)
			break;
		UT_ASSERT_EQ(got, 16);
		if (got != 16)
			break;
		length = pgrac_fenced_journal_frame_size(frame, 16);
		UT_ASSERT(length >= 16 && length <= sizeof(frame));
		if (length < 16 || length > sizeof(frame))
			break;
		UT_ASSERT_EQ(read(fd, frame + 16, length - 16), length - 16);
		UT_ASSERT(pgrac_fenced_journal_record_decode(frame, length, &records[count]));
		count++;
	}
	return count;
}

UT_TEST(test_admin_prepare_is_inert_durable_and_duplicate_stable)
{
	PgracFencedOperationContextV1 operation_context;
	PgracFencedRejoinContextV1 context;
	PgracFencedJournalScanState journal_state;
	PgracFencedJournalRecordV1 records[4];
	PgracFencedConfigV1 config;
	PgracFencedProviderOpsV1 ops;
	PgracExternalFenceProtocolRejoinFrameV1 request;
	PgracExternalFenceProtocolRejoinFrameV1 response;
	uint8 operation_id[16];
	uint8 duplicate_id[16];
	uint8 frame[PGRAC_EXTERNAL_FENCE_REJOIN_V1_BYTES];
	char path[64];
	int fd;

	make_config(&config);
	make_ops(&ops);
	memset((void *)provider_state, 0, sizeof(uint32) * 8);
	provider_state[4] = PGRAC_FENCED_TARGET_OFF;
	provider_state[5] = PGRAC_FENCED_IO_DRAIN_DRAINED;
	fd = open_context(&operation_context, &context, &journal_state, &config, &ops, path);
	if (fd < 0)
		return;
	make_prepare(3, 70, 77, 0x61, &request);
	memset(operation_id, 0xa1, sizeof(operation_id));
	UT_ASSERT(pgrac_fenced_rejoin_admin_prepare(&context, &request, operation_id,
												deadline_after_ms(1000), &response));
	UT_ASSERT_EQ(response.status, PGRAC_FENCED_REJOIN_STATUS_OFFERED);
	UT_ASSERT(memcmp(response.operation_id, operation_id, sizeof(operation_id)) == 0);
	UT_ASSERT(pgrac_external_fence_rejoin_v1_encode(&response, frame));
	UT_ASSERT_EQ(context.operation_count, 1);
	UT_ASSERT_EQ(provider_state[0], 1);
	UT_ASSERT_EQ(provider_state[1], 0);
	UT_ASSERT_EQ(provider_state[3], 0);
	UT_ASSERT_EQ(read_records(fd, records, lengthof(records)), 2);
	UT_ASSERT_EQ(records[1].record_kind, PGRAC_FENCED_JOURNAL_KIND_REENABLE_REQUESTED);

	memset(duplicate_id, 0xb1, sizeof(duplicate_id));
	UT_ASSERT(pgrac_fenced_rejoin_admin_prepare(&context, &request, duplicate_id,
												deadline_after_ms(1000), &response));
	UT_ASSERT(memcmp(response.operation_id, operation_id, sizeof(operation_id)) == 0);
	UT_ASSERT_EQ(provider_state[0], 1);
	UT_ASSERT_EQ(read_records(fd, records, lengthof(records)), 2);

	make_prepare(3, 70, 78, 0x62, &request);
	UT_ASSERT(pgrac_fenced_rejoin_admin_prepare(&context, &request, duplicate_id,
												deadline_after_ms(1000), &response));
	UT_ASSERT_EQ(response.status, PGRAC_FENCED_REJOIN_STATUS_REJECTED);
	UT_ASSERT_EQ(response.deny_reason, 23);
	UT_ASSERT(pgrac_external_fence_rejoin_v1_encode(&response, frame));
	UT_ASSERT_EQ(provider_state[0], 1);
	UT_ASSERT_EQ(context.operation_count, 1);
	(void)close(fd);
	(void)unlink(path);
}

UT_TEST(test_claim_authorize_refresh_is_exact_and_on_happens_once)
{
	PgracFencedOperationContextV1 operation_context;
	PgracFencedRejoinContextV1 context;
	PgracFencedJournalScanState journal_state;
	PgracFencedJournalRecordV1 records[10];
	PgracFencedConfigV1 config;
	PgracFencedProviderOpsV1 ops;
	PgracExternalFenceProtocolRejoinFrameV1 request;
	PgracExternalFenceProtocolRejoinFrameV1 offer;
	PgracExternalFenceProtocolRejoinFrameV1 on_result;
	PgracExternalFenceProtocolRejoinFrameV1 ready;
	uint8 operation_id[16];
	uint8 frame[PGRAC_EXTERNAL_FENCE_REJOIN_V1_BYTES];
	char path[64];
	int fd;
	size_t count;

	make_config(&config);
	make_ops(&ops);
	memset((void *)provider_state, 0, sizeof(uint32) * 8);
	provider_state[4] = PGRAC_FENCED_TARGET_OFF;
	provider_state[5] = PGRAC_FENCED_IO_DRAIN_DRAINED;
	fd = open_context(&operation_context, &context, &journal_state, &config, &ops, path);
	if (fd < 0)
		return;
	make_prepare(3, 70, 77, 0x63, &request);
	memset(operation_id, 0xa2, sizeof(operation_id));
	UT_ASSERT(pgrac_fenced_rejoin_admin_prepare(&context, &request, operation_id,
												deadline_after_ms(1000), &offer));
	make_claim(0x64, &request);
	UT_ASSERT(pgrac_fenced_rejoin_claim(&context, &request, deadline_after_ms(1000), &offer));
	UT_ASSERT_EQ(offer.status, PGRAC_FENCED_REJOIN_STATUS_OFFERED);
	UT_ASSERT_EQ(offer.proof_generation, 1);
	UT_ASSERT_EQ(offer.fresh_until_mono_ns - offer.verified_mono_ns, UINT64_C(5000000000));
	UT_ASSERT(pgrac_external_fence_rejoin_v1_encode(&offer, frame));
	UT_ASSERT_EQ(provider_state[1], 0);
	UT_ASSERT_EQ(provider_state[2], 1);

	make_bound_request(PGRAC_EXTERNAL_FENCE_REJOIN_LMON_AUTHORIZE_ON, &offer, 0x65, &request);
	UT_ASSERT(!pgrac_fenced_rejoin_authorize_on(&context, &request, false, deadline_after_ms(1000),
												&on_result));
	UT_ASSERT_EQ(provider_state[3], 0);
	UT_ASSERT(pgrac_fenced_rejoin_authorize_on(&context, &request, true, deadline_after_ms(1000),
											   &on_result));
	UT_ASSERT_EQ(on_result.status, PGRAC_FENCED_REJOIN_STATUS_WAITING_JOINER);
	UT_ASSERT_EQ(on_result.proof_generation, 2);
	UT_ASSERT(pgrac_external_fence_rejoin_v1_encode(&on_result, frame));
	UT_ASSERT_EQ(provider_state[3], 1);
	UT_ASSERT_EQ(provider_state[2], 2);

	make_bound_request(PGRAC_EXTERNAL_FENCE_REJOIN_LMON_REFRESH_ON, &on_result, 0x66, &request);
	UT_ASSERT(pgrac_fenced_rejoin_refresh_on(&context, &request, deadline_after_ms(1000), &ready));
	UT_ASSERT_EQ(ready.status, PGRAC_FENCED_REJOIN_STATUS_READY);
	UT_ASSERT_EQ(ready.proof_generation, 3);
	UT_ASSERT(pgrac_external_fence_rejoin_v1_encode(&ready, frame));
	UT_ASSERT_EQ(provider_state[3], 1);
	UT_ASSERT_EQ(provider_state[2], 3);
	count = read_records(fd, records, lengthof(records));
	UT_ASSERT_EQ(count, 7);
	UT_ASSERT_EQ(records[1].record_kind, PGRAC_FENCED_JOURNAL_KIND_REENABLE_REQUESTED);
	UT_ASSERT_EQ(records[2].record_kind, PGRAC_FENCED_JOURNAL_KIND_READBACK_RESULT);
	UT_ASSERT_EQ(records[3].record_kind, PGRAC_FENCED_JOURNAL_KIND_ACTUATION_ISSUED);
	UT_ASSERT_EQ(records[4].record_kind, PGRAC_FENCED_JOURNAL_KIND_ACTUATION_RESULT);
	UT_ASSERT_EQ(records[5].record_kind, PGRAC_FENCED_JOURNAL_KIND_REENABLE_RESULT);
	UT_ASSERT_EQ(records[6].record_kind, PGRAC_FENCED_JOURNAL_KIND_REENABLE_RESULT);
	UT_ASSERT_EQ(offer.journal_seq, records[2].seq);
	UT_ASSERT_EQ(on_result.journal_seq, records[5].seq);
	UT_ASSERT_EQ(ready.journal_seq, records[6].seq);
	(void)close(fd);
	(void)unlink(path);
}

UT_TEST(test_nonterminal_claim_releases_offer_and_cancel_discards_it)
{
	PgracFencedOperationContextV1 operation_context;
	PgracFencedRejoinContextV1 context;
	PgracFencedJournalScanState journal_state;
	PgracFencedConfigV1 config;
	PgracFencedProviderOpsV1 ops;
	PgracExternalFenceProtocolRejoinFrameV1 request;
	PgracExternalFenceProtocolRejoinFrameV1 response;
	uint8 operation_id[16];
	uint8 frame[PGRAC_EXTERNAL_FENCE_REJOIN_V1_BYTES];
	char path[64];
	int fd;

	make_config(&config);
	make_ops(&ops);
	memset((void *)provider_state, 0, sizeof(uint32) * 8);
	provider_state[4] = PGRAC_FENCED_TARGET_OFF;
	provider_state[5] = PGRAC_FENCED_IO_DRAIN_UNKNOWN;
	fd = open_context(&operation_context, &context, &journal_state, &config, &ops, path);
	if (fd < 0)
		return;
	make_prepare(3, 70, 77, 0x67, &request);
	memset(operation_id, 0xa3, sizeof(operation_id));
	UT_ASSERT(pgrac_fenced_rejoin_admin_prepare(&context, &request, operation_id,
												deadline_after_ms(1000), &response));
	make_claim(0x68, &request);
	UT_ASSERT(pgrac_fenced_rejoin_claim(&context, &request, deadline_after_ms(1000), &response));
	UT_ASSERT_EQ(response.status, PGRAC_FENCED_REJOIN_STATUS_UNKNOWN);
	UT_ASSERT_EQ(response.deny_reason, 9);
	UT_ASSERT_EQ(response.proof_generation, 0);
	UT_ASSERT(pgrac_external_fence_rejoin_v1_encode(&response, frame));
	provider_state[5] = PGRAC_FENCED_IO_DRAIN_DRAINED;
	make_claim(0x69, &request);
	UT_ASSERT(pgrac_fenced_rejoin_claim(&context, &request, deadline_after_ms(1000), &response));
	UT_ASSERT_EQ(response.status, PGRAC_FENCED_REJOIN_STATUS_OFFERED);
	memset(&request, 0, sizeof(request));
	request.opcode = PGRAC_EXTERNAL_FENCE_REJOIN_LMON_CANCEL;
	memset(request.transport_nonce, 0x6a, sizeof(request.transport_nonce));
	memcpy(request.operation_id, operation_id, sizeof(request.operation_id));
	UT_ASSERT(pgrac_fenced_rejoin_cancel(&context, &request));
	UT_ASSERT_EQ(context.operation_count, 0);
	UT_ASSERT(pgrac_fenced_rejoin_target(&context, operation_id) == NULL);
	(void)close(fd);
	(void)unlink(path);
}

UT_TEST(test_claim_retries_transient_readback_before_off_proof)
{
	PgracFencedOperationContextV1 operation_context;
	PgracFencedRejoinContextV1 context;
	PgracFencedJournalScanState journal_state;
	PgracFencedConfigV1 config;
	PgracFencedProviderOpsV1 ops;
	PgracExternalFenceProtocolRejoinFrameV1 request;
	PgracExternalFenceProtocolRejoinFrameV1 response;
	uint8 operation_id[16];
	char path[64];
	int fd;

	make_config(&config);
	make_ops(&ops);
	memset((void *)provider_state, 0, sizeof(uint32) * 8);
	provider_state[4] = PGRAC_FENCED_TARGET_OFF;
	provider_state[5] = PGRAC_FENCED_IO_DRAIN_DRAINED;
	provider_state[6] = 2;
	fd = open_context(&operation_context, &context, &journal_state, &config, &ops, path);
	if (fd < 0)
		return;
	make_prepare(3, 80, 81, 0x6b, &request);
	memset(operation_id, 0xa4, sizeof(operation_id));
	UT_ASSERT(pgrac_fenced_rejoin_admin_prepare(&context, &request, operation_id,
												deadline_after_ms(1000), &response));
	make_claim(0x6c, &request);
	UT_ASSERT(pgrac_fenced_rejoin_claim(&context, &request, deadline_after_ms(2000), &response));
	UT_ASSERT_EQ(response.status, PGRAC_FENCED_REJOIN_STATUS_OFFERED);
	UT_ASSERT_EQ(provider_state[2], 3);
	UT_ASSERT_EQ(provider_state[6], 0);
	(void)close(fd);
	(void)unlink(path);
}

UT_TEST(test_refresh_resolve_failure_returns_negative_and_discards_operation)
{
	PgracFencedOperationContextV1 operation_context;
	PgracFencedRejoinContextV1 context;
	PgracFencedJournalScanState journal_state;
	PgracFencedConfigV1 config;
	PgracFencedProviderOpsV1 ops;
	PgracExternalFenceProtocolRejoinFrameV1 request;
	PgracExternalFenceProtocolRejoinFrameV1 offer;
	PgracExternalFenceProtocolRejoinFrameV1 on_result;
	PgracExternalFenceProtocolRejoinFrameV1 response;
	uint8 operation_id[16];
	char path[64];
	int fd;

	make_config(&config);
	make_ops(&ops);
	memset((void *)provider_state, 0, sizeof(uint32) * 8);
	provider_state[4] = PGRAC_FENCED_TARGET_OFF;
	provider_state[5] = PGRAC_FENCED_IO_DRAIN_DRAINED;
	fd = open_context(&operation_context, &context, &journal_state, &config, &ops, path);
	if (fd < 0)
		return;
	make_prepare(3, 90, 91, 0x6d, &request);
	memset(operation_id, 0xa5, sizeof(operation_id));
	UT_ASSERT(pgrac_fenced_rejoin_admin_prepare(&context, &request, operation_id,
												deadline_after_ms(1000), &response));
	make_claim(0x6e, &request);
	UT_ASSERT(pgrac_fenced_rejoin_claim(&context, &request, deadline_after_ms(1000), &offer));
	make_bound_request(PGRAC_EXTERNAL_FENCE_REJOIN_LMON_AUTHORIZE_ON, &offer, 0x6f, &request);
	UT_ASSERT(pgrac_fenced_rejoin_authorize_on(&context, &request, true, deadline_after_ms(1000),
											   &on_result));
	UT_ASSERT_EQ(on_result.status, PGRAC_FENCED_REJOIN_STATUS_WAITING_JOINER);
	make_bound_request(PGRAC_EXTERNAL_FENCE_REJOIN_LMON_REFRESH_ON, &on_result, 0x70, &request);
	provider_state[7] = 1;
	UT_ASSERT(
		pgrac_fenced_rejoin_refresh_on(&context, &request, deadline_after_ms(1000), &response));
	UT_ASSERT_EQ(response.opcode, PGRAC_EXTERNAL_FENCE_REJOIN_LMON_REFRESH_RESULT);
	UT_ASSERT_EQ(response.status, PGRAC_FENCED_REJOIN_STATUS_UNAVAILABLE);
	UT_ASSERT_EQ(response.deny_reason, 10);
	UT_ASSERT_EQ(context.operation_count, 0);
	(void)close(fd);
	(void)unlink(path);
}

static bool
owned_config(PgracFencedConfigV1 *config, PgracFencedProviderOpsV1 *ops)
{
	PgracFencedConfigResult parsed = pgrac_fenced_config_parse(
		(const uint8 *)fenced_config_v2, sizeof(fenced_config_v2) - 1, config);

#ifndef USE_OPENSSL
	UT_ASSERT_NE(parsed, PGRAC_FENCED_CONFIG_OK);
	return false;
#else
	UT_ASSERT_EQ(parsed, PGRAC_FENCED_CONFIG_OK);
	make_ops(ops);
	ops->provider_id = PGRAC_FENCED_PROVIDER_ID_PACEMAKER_LIBVIRT_V1;
	memset((void *)provider_state, 0, sizeof(uint32) * 9);
	provider_state[4] = PGRAC_FENCED_TARGET_OFF;
	provider_state[5] = PGRAC_FENCED_IO_DRAIN_DRAINED;
	return parsed == PGRAC_FENCED_CONFIG_OK;
#endif
}

UT_TEST(test_owned_rejoin_journals_phase_before_on_and_retires_completed_work)
{
	PgracFencedOperationContextV1 operation_context;
	PgracFencedRejoinContextV1 context;
	PgracFencedJournalScanState journal_state;
	PgracFencedJournalReconcileState replay;
	PgracFencedJournalRecordV1 records[12];
	PgracFencedConfigV1 config;
	PgracFencedProviderOpsV1 ops;
	PgracExternalFenceProtocolRejoinFrameV1 request, response;
	uint8 operation_id[16];
	char path[64];
	int fd;
	size_t count;

	if (!owned_config(&config, &ops))
		return;
	fd = open_context(&operation_context, &context, &journal_state, &config, &ops, path);
	if (fd < 0)
		return;
	owned_journal_path = path;
	memset(operation_id, 0xa6, sizeof(operation_id));
	make_prepare(2, 91, 92, 0x81, &request);
	UT_ASSERT(pgrac_fenced_rejoin_admin_prepare(&context, &request, operation_id,
												deadline_after_ms(1000), &response));
	make_claim(0x82, &request);
	UT_ASSERT(pgrac_fenced_rejoin_claim(&context, &request, deadline_after_ms(1000), &response));
	make_bound_request(PGRAC_EXTERNAL_FENCE_REJOIN_LMON_AUTHORIZE_ON, &response, 0x83, &request);
	UT_ASSERT(pgrac_fenced_rejoin_authorize_on(&context, &request, true, deadline_after_ms(1000),
											   &response));
	UT_ASSERT_EQ(response.status, PGRAC_FENCED_REJOIN_STATUS_WAITING_JOINER);
	UT_ASSERT_EQ(provider_state[8], 1);
	count = read_records(fd, records, lengthof(records));
	pgrac_fenced_journal_reconcile_state_init(&replay);
	for (size_t i = 0; i < count; ++i)
		UT_ASSERT(pgrac_fenced_journal_reconcile_observe(&replay, &records[i]));
	UT_ASSERT_EQ(replay.pending_count, 1);
	make_bound_request(PGRAC_EXTERNAL_FENCE_REJOIN_LMON_REFRESH_ON, &response, 0x84, &request);
	UT_ASSERT(
		pgrac_fenced_rejoin_refresh_on(&context, &request, deadline_after_ms(1000), &response));
	UT_ASSERT_EQ(response.status, PGRAC_FENCED_REJOIN_STATUS_READY);
	count = read_records(fd, records, lengthof(records));
	UT_ASSERT_EQ(count, 9);
	pgrac_fenced_journal_reconcile_state_init(&replay);
	for (size_t i = 0; i < count; ++i) {
		UT_ASSERT(pgrac_fenced_journal_reconcile_observe(&replay, &records[i]));
		if (i == 0)
			continue;
		UT_ASSERT(memcmp(records[i].operation_id, operation_id, 16) == 0);
		UT_ASSERT_EQ(records[i].intent.kind, PGRAC_FENCED_JOURNAL_INTENT_REJOIN);
		UT_ASSERT_EQ(records[i].intent.attempt, i < 3 ? 1 : i < 7 ? 2 : 3);
		UT_ASSERT_EQ(records[i].intent.request.rejoin.transport_nonce[0], i < 3	  ? 0x81
																		  : i < 7 ? 0x83
																				  : 0x84);
	}
	UT_ASSERT_EQ(replay.pending_count, 0);
	/* A completed UUID cannot create a new replay root from REFRESH alone. */
	{
		PgracFencedRejoinOperationV1 completed = context.operations[0];
		uint64 generation = operation_context.next_proof_generation;
		uint32 calls[9];

		provider_state[7] = 1;
		memcpy(calls, (const void *)provider_state, sizeof(calls));
		UT_ASSERT(
			pgrac_fenced_rejoin_refresh_on(&context, &request, deadline_after_ms(1000), &response));
		UT_ASSERT_EQ(response.status, PGRAC_FENCED_REJOIN_STATUS_STALE);
		UT_ASSERT(memcmp(&completed, &context.operations[0], sizeof(completed)) == 0);
		UT_ASSERT_EQ(operation_context.next_proof_generation, generation);
		UT_ASSERT(memcmp(calls, (const void *)provider_state, sizeof(calls)) == 0);
		UT_ASSERT_EQ(read_records(fd, records, lengthof(records)), 9);
		provider_state[7] = 0;
	}
	memset(&request, 0, sizeof(request));
	request.opcode = PGRAC_EXTERNAL_FENCE_REJOIN_LMON_CANCEL;
	memset(request.transport_nonce, 0x85, sizeof(request.transport_nonce));
	memcpy(request.operation_id, operation_id, 16);
	UT_ASSERT(pgrac_fenced_rejoin_cancel(&context, &request));
	UT_ASSERT_EQ(context.operation_count, 0);
	UT_ASSERT(pgrac_fenced_rejoin_target(&context, operation_id) == NULL);
	UT_ASSERT_EQ(read_records(fd, records, lengthof(records)), 9);
	owned_journal_path = NULL;
	(void)close(fd);
	(void)unlink(path);
}

UT_TEST(test_owned_rejoin_negative_does_not_forget_the_target)
{
	for (int variant = 0; variant < 3; ++variant) {
		PgracFencedOperationContextV1 operation_context;
		PgracFencedRejoinContextV1 context;
		PgracFencedJournalScanState journal_state;
		PgracFencedConfigV1 config;
		PgracFencedProviderOpsV1 ops;
		PgracExternalFenceProtocolRejoinFrameV1 request, response;
		uint8 operation_id[16];
		char path[64];
		int fd;

		if (!owned_config(&config, &ops))
			return;
		fd = open_context(&operation_context, &context, &journal_state, &config, &ops, path);
		if (fd < 0)
			return;
		memset(operation_id, 0xa7, sizeof(operation_id));
		make_prepare(2, 91, 92, 0x86, &request);
		UT_ASSERT(pgrac_fenced_rejoin_admin_prepare(&context, &request, operation_id,
													deadline_after_ms(1000), &response));
		make_claim(0x87, &request);
		UT_ASSERT(
			pgrac_fenced_rejoin_claim(&context, &request, deadline_after_ms(1000), &response));
		make_bound_request(PGRAC_EXTERNAL_FENCE_REJOIN_LMON_AUTHORIZE_ON, &response, 0x88,
						   &request);
		if (variant == 0)
			provider_state[7] = 1;
		if (variant == 1)
			provider_state[5] = PGRAC_FENCED_IO_DRAIN_NOT_DRAINED;
		UT_ASSERT(pgrac_fenced_rejoin_authorize_on(&context, &request, true,
												   deadline_after_ms(1000), &response));
		if (variant == 2) {
			make_bound_request(PGRAC_EXTERNAL_FENCE_REJOIN_LMON_REFRESH_ON, &response, 0x89,
							   &request);
			provider_state[7] = 1;
			UT_ASSERT(pgrac_fenced_rejoin_refresh_on(&context, &request, deadline_after_ms(1000),
													 &response));
		}
		UT_ASSERT(response.status >= PGRAC_FENCED_REJOIN_STATUS_REJECTED);
		memset(&request, 0, sizeof(request));
		request.opcode = PGRAC_EXTERNAL_FENCE_REJOIN_LMON_CANCEL;
		memset(request.transport_nonce, 0x90, 16);
		memcpy(request.operation_id, operation_id, 16);
		UT_ASSERT(pgrac_fenced_rejoin_cancel(&context, &request));
		UT_ASSERT_EQ(context.operation_count, 1);
		UT_ASSERT(pgrac_fenced_rejoin_target(&context, operation_id) != NULL);
		(void)close(fd);
		(void)unlink(path);
	}
}

/* Restore reads real accepted phases; no provider action or old proof is inherited. */
UT_TEST(test_owned_restore_keeps_admin_identity_without_replaying_power)
{
	for (int phase = 0; phase < 3; ++phase) {
		PgracFencedOperationContextV1 operation_context;
		PgracFencedRejoinContextV1 context;
		PgracFencedJournalScanState journal_state;
		PgracFencedJournalReconcileState replay;
		PgracFencedJournalRecordV1 records[20];
		PgracFencedConfigV1 config;
		PgracFencedProviderOpsV1 ops;
		PgracExternalFenceProtocolRejoinFrameV1 admin, request, response;
		uint8 operation_id[16];
		uint8 digest[32], boot[16];
		uint32 counts[9];
		char path[64];
		size_t count;
		int fd;

		if (!owned_config(&config, &ops))
			return;
		fd = open_context(&operation_context, &context, &journal_state, &config, &ops, path);
		if (fd < 0)
			return;
		memset(operation_id, 0xb1, sizeof(operation_id));
		make_prepare(2, 91, 92, 0x91, &admin);
		UT_ASSERT(pgrac_fenced_rejoin_admin_prepare(&context, &admin, operation_id,
													deadline_after_ms(1000), &response));
		make_claim(0x92, &request);
		UT_ASSERT(
			pgrac_fenced_rejoin_claim(&context, &request, deadline_after_ms(1000), &response));
		if (phase > 0) {
			make_bound_request(PGRAC_EXTERNAL_FENCE_REJOIN_LMON_AUTHORIZE_ON, &response, 0x93,
							   &request);
			UT_ASSERT(pgrac_fenced_rejoin_authorize_on(&context, &request, true,
													   deadline_after_ms(1000), &response));
		}
		if (phase > 1) {
			make_bound_request(PGRAC_EXTERNAL_FENCE_REJOIN_LMON_REFRESH_ON, &response, 0x94,
							   &request);
			provider_state[7] = 1;
			UT_ASSERT(pgrac_fenced_rejoin_refresh_on(&context, &request, deadline_after_ms(1000),
													 &response));
			provider_state[7] = 0;
		}
		count = read_records(fd, records, lengthof(records));
		pgrac_fenced_journal_reconcile_state_init(&replay);
		for (size_t i = 0; i < count; ++i)
			UT_ASSERT(pgrac_fenced_journal_reconcile_observe(&replay, &records[i]));
		UT_ASSERT_EQ(replay.pending_count, 1);
		memcpy(counts, (const void *)provider_state, sizeof(counts));
		memcpy(digest, operation_context.semantic_config_digest, 32);
		memset(boot, 0x82, 16);
		UT_ASSERT(pgrac_fenced_operation_context_init(&operation_context, &config, &ops, true,
													  digest, boot, fd, &journal_state));
		/* Context startup writes its own BOOT record, not a replayed action. */
		count = read_records(fd, records, lengthof(records));
		UT_ASSERT(pgrac_fenced_rejoin_init(&context, &operation_context));
		UT_ASSERT(pgrac_fenced_rejoin_restore(&context, &replay));
		UT_ASSERT_EQ(context.operation_count, 1);
		UT_ASSERT_EQ(replay.pending_count, 0);
		UT_ASSERT_EQ(context.operations[0].state,
					 phase == 0 ? PGRAC_FENCED_REJOIN_OPERATION_OFFERED
								: PGRAC_FENCED_REJOIN_OPERATION_CLEANUP_REQUIRED);
		UT_ASSERT(memcmp(&context.operations[0].admin_request, &admin, sizeof(admin)) == 0);
		UT_ASSERT(memcmp(context.operations[0].operation_id, operation_id, 16) == 0);
		UT_ASSERT(memcmp(counts, (const void *)provider_state, sizeof(counts)) == 0);
		UT_ASSERT_EQ(context.operations[0].offer_result.proof_generation, 0);
		UT_ASSERT_EQ(context.operations[0].on_result.proof_generation, 0);
		UT_ASSERT_EQ(context.operations[0].ready_result.proof_generation, 0);
		UT_ASSERT_EQ(read_records(fd, records, lengthof(records)), count);
		if (phase == 0 && context.operation_count == 1) {
			make_claim(0x95, &request);
			UT_ASSERT(
				pgrac_fenced_rejoin_claim(&context, &request, deadline_after_ms(1000), &response));
			UT_ASSERT_EQ(response.status, PGRAC_FENCED_REJOIN_STATUS_OFFERED);
			UT_ASSERT_EQ(context.operations[0].last_record.intent.attempt, 2);
			UT_ASSERT(memcmp(response.daemon_boot_id, boot, 16) == 0);
			UT_ASSERT_EQ(provider_state[2], counts[2] + 1);
			UT_ASSERT_EQ(provider_state[1], counts[1]);
			UT_ASSERT_EQ(provider_state[3], counts[3]);
		}
		(void)close(fd);
		(void)unlink(path);
	}
}

UT_TEST(test_owned_restore_preflights_all_identities_before_attachment)
{
	PgracFencedOperationContextV1 operation_context;
	PgracFencedRejoinContextV1 context;
	PgracFencedJournalScanState journal_state;
	PgracFencedJournalReconcileState replay, original;
	PgracFencedJournalRecordV1 records[8];
	PgracFencedConfigV1 config;
	PgracFencedProviderOpsV1 ops;
	PgracExternalFenceProtocolRejoinFrameV1 request, response;
	uint8 operation_id[16];
	char path[64];
	int fd;
	size_t count;

	if (!owned_config(&config, &ops))
		return;
	fd = open_context(&operation_context, &context, &journal_state, &config, &ops, path);
	if (fd < 0)
		return;
	memset(operation_id, 0xb2, 16);
	make_prepare(2, 91, 92, 0x96, &request);
	UT_ASSERT(pgrac_fenced_rejoin_admin_prepare(&context, &request, operation_id,
												deadline_after_ms(1000), &response));
	count = read_records(fd, records, lengthof(records));
	pgrac_fenced_journal_reconcile_state_init(&original);
	for (size_t i = 0; i < count; ++i)
		UT_ASSERT(pgrac_fenced_journal_reconcile_observe(&original, &records[i]));
	for (int variant = 0; variant < 4; ++variant) {
		replay = original;
		/* The valid first entry must not be attached before rejecting the second. */
		replay.pending[1] = replay.pending[0];
		replay.pending_count = 2;
		if (variant == 0)
			memset(&replay.pending[1].first_record, 0, sizeof(replay.pending[1].first_record));
		if (variant == 1)
			replay.pending[1].last_record.intent.target_uuid[0] ^= 1;
		if (variant == 2)
			replay.pending[1].first_seq++;
		/* variant 3 is two owned operations for one target. */
		operation_context.available = true;
		UT_ASSERT(pgrac_fenced_rejoin_init(&context, &operation_context));
		UT_ASSERT(!pgrac_fenced_rejoin_restore(&context, &replay));
		UT_ASSERT_EQ(context.operation_count, 0);
		UT_ASSERT_EQ(replay.pending_count, 2);
		UT_ASSERT_EQ(provider_state[1], 0);
		UT_ASSERT_EQ(provider_state[3], 0);
	}
	(void)close(fd);
	(void)unlink(path);
}

UT_TEST(test_owned_cleanup_needs_new_exact_off_and_drain_not_old_on)
{
	PgracFencedOperationContextV1 operation_context;
	PgracFencedRejoinContextV1 context;
	PgracFencedJournalScanState journal_state;
	PgracFencedJournalReconcileState replay;
	PgracFencedJournalRecordV1 records[30];
	PgracFencedConfigV1 config;
	PgracFencedProviderOpsV1 ops;
	PgracExternalFenceProtocolRejoinFrameV1 request, response;
	uint8 operation_id[16], digest[32], boot[16];
	char path[64];
	int fd;
	size_t count;

	if (!owned_config(&config, &ops))
		return;
	fd = open_context(&operation_context, &context, &journal_state, &config, &ops, path);
	if (fd < 0)
		return;
	memset(operation_id, 0xb3, 16);
	make_prepare(2, 91, 92, 0x97, &request);
	UT_ASSERT(pgrac_fenced_rejoin_admin_prepare(&context, &request, operation_id,
												deadline_after_ms(1000), &response));
	make_claim(0x98, &request);
	UT_ASSERT(pgrac_fenced_rejoin_claim(&context, &request, deadline_after_ms(1000), &response));
	make_bound_request(PGRAC_EXTERNAL_FENCE_REJOIN_LMON_AUTHORIZE_ON, &response, 0x99, &request);
	UT_ASSERT(pgrac_fenced_rejoin_authorize_on(&context, &request, true, deadline_after_ms(1000),
											   &response));
	UT_ASSERT_EQ(response.status, PGRAC_FENCED_REJOIN_STATUS_WAITING_JOINER);
	count = read_records(fd, records, lengthof(records));
	pgrac_fenced_journal_reconcile_state_init(&replay);
	for (size_t i = 0; i < count; ++i)
		UT_ASSERT(pgrac_fenced_journal_reconcile_observe(&replay, &records[i]));
	memcpy(digest, operation_context.semantic_config_digest, 32);
	memset(boot, 0x82, 16);
	UT_ASSERT(pgrac_fenced_operation_context_init(&operation_context, &config, &ops, true, digest,
												  boot, fd, &journal_state));
	UT_ASSERT(pgrac_fenced_rejoin_init(&context, &operation_context));
	UT_ASSERT(pgrac_fenced_rejoin_restore(&context, &replay));
	owned_journal_path = path;
	provider_state[5] = PGRAC_FENCED_IO_DRAIN_NOT_DRAINED;
	UT_ASSERT(pgrac_fenced_rejoin_cleanup(&context, operation_id, deadline_after_ms(1000)));
	UT_ASSERT_EQ(provider_state[1], 1);
	UT_ASSERT_EQ(provider_state[3], 1);
	UT_ASSERT_EQ(provider_state[8], 1);
	UT_ASSERT_EQ(context.operations[0].last_record.intent.attempt, 3);
	UT_ASSERT_EQ(context.operations[0].state, PGRAC_FENCED_REJOIN_OPERATION_CLEANUP_REQUIRED);
	UT_ASSERT(!pgrac_fenced_journal_rejoin_terminal(&context.operations[0].last_record));
	UT_ASSERT_EQ(context.operation_count, 1);
	provider_state[5] = PGRAC_FENCED_IO_DRAIN_DRAINED;
	UT_ASSERT(pgrac_fenced_rejoin_cleanup(&context, operation_id, deadline_after_ms(1000)));
	UT_ASSERT_EQ(provider_state[1], 2);
	UT_ASSERT_EQ(provider_state[3], 1);
	UT_ASSERT_EQ(context.operations[0].last_record.intent.attempt, 4);
	UT_ASSERT_EQ(context.operations[0].last_record.record_kind,
				 PGRAC_FENCED_JOURNAL_KIND_RECONCILED);
	UT_ASSERT_EQ(context.operations[0].last_record.target_state, PGRAC_FENCED_TARGET_OFF);
	UT_ASSERT_EQ(context.operations[0].last_record.deny_reason, 0);
	UT_ASSERT(pgrac_fenced_journal_rejoin_terminal(&context.operations[0].last_record));
	count = read_records(fd, records, lengthof(records));
	pgrac_fenced_journal_reconcile_state_init(&replay);
	for (size_t i = 0; i < count; ++i)
		UT_ASSERT(pgrac_fenced_journal_reconcile_observe(&replay, &records[i]));
	UT_ASSERT_EQ(replay.pending_count, 0);
	/* Durable terminal is not permission to repeat the power operation. */
	UT_ASSERT(!pgrac_fenced_rejoin_cleanup(&context, operation_id, deadline_after_ms(1000)));
	UT_ASSERT_EQ(provider_state[1], 2);
	UT_ASSERT_EQ(provider_state[3], 1);
	owned_journal_path = NULL;
	(void)close(fd);
	(void)unlink(path);
}

int
main(void)
{
	provider_state
		= mmap(NULL, sizeof(uint32) * 9, PROT_READ | PROT_WRITE, MAP_SHARED | MAP_ANON, -1, 0);
	if (provider_state == MAP_FAILED)
		return 1;
	UT_PLAN(10);
	UT_RUN(test_admin_prepare_is_inert_durable_and_duplicate_stable);
	UT_RUN(test_claim_authorize_refresh_is_exact_and_on_happens_once);
	UT_RUN(test_nonterminal_claim_releases_offer_and_cancel_discards_it);
	UT_RUN(test_claim_retries_transient_readback_before_off_proof);
	UT_RUN(test_refresh_resolve_failure_returns_negative_and_discards_operation);
	UT_RUN(test_owned_rejoin_journals_phase_before_on_and_retires_completed_work);
	UT_RUN(test_owned_rejoin_negative_does_not_forget_the_target);
	UT_RUN(test_owned_restore_keeps_admin_identity_without_replaying_power);
	UT_RUN(test_owned_restore_preflights_all_identities_before_attachment);
	UT_RUN(test_owned_cleanup_needs_new_exact_off_and_drain_not_old_on);
	UT_DONE();
	(void)munmap((void *)provider_state, sizeof(uint32) * 9);
	return ut_failed_count == 0 ? 0 : 1;
}
