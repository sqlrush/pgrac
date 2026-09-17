/*-------------------------------------------------------------------------
 *
 * test_pgrac_fenced_rejoin.c
 *    Root-daemon PFRJ rejoin lifecycle tests.
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

static volatile uint32 *provider_state;

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
	(void)target;
	(void)deadline_mono_ns;
	provider_state[1]++;
	*native_status = 0;
	return PGRAC_FENCED_PROVIDER_OK;
}

static PgracFencedProviderResult
test_readback(const PgracFencedTargetV1 *target, uint64_t deadline_mono_ns,
			  PgracFencedReadbackV1 *readback)
{
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
	uint8 frame[PGRAC_FENCED_JOURNAL_RECORD_BYTES];
	size_t count = 0;
	ssize_t got;

	UT_ASSERT_EQ(lseek(fd, 0, SEEK_SET), 0);
	while (count < maximum) {
		do {
			got = read(fd, frame, sizeof(frame));
		} while (got < 0 && errno == EINTR);
		if (got == 0)
			break;
		UT_ASSERT_EQ(got, sizeof(frame));
		if (got != sizeof(frame))
			break;
		UT_ASSERT(pgrac_fenced_journal_record_decode(frame, sizeof(frame), &records[count]));
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

int
main(void)
{
	provider_state
		= mmap(NULL, sizeof(uint32) * 8, PROT_READ | PROT_WRITE, MAP_SHARED | MAP_ANON, -1, 0);
	if (provider_state == MAP_FAILED)
		return 1;
	UT_PLAN(5);
	UT_RUN(test_admin_prepare_is_inert_durable_and_duplicate_stable);
	UT_RUN(test_claim_authorize_refresh_is_exact_and_on_happens_once);
	UT_RUN(test_nonterminal_claim_releases_offer_and_cancel_discards_it);
	UT_RUN(test_claim_retries_transient_readback_before_off_proof);
	UT_RUN(test_refresh_resolve_failure_returns_negative_and_discards_operation);
	UT_DONE();
	(void)munmap((void *)provider_state, sizeof(uint32) * 8);
	return ut_failed_count == 0 ? 0 : 1;
}
