/*-------------------------------------------------------------------------
 *
 * test_pgrac_fenced_rejoin_async.c
 *    Parent-owned PFRJ state, proof generations and journal serialization.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <fcntl.h>
#include <poll.h>
#include <sys/mman.h>
#include <time.h>
#include <unistd.h>

#include "pgrac_fenced_rejoin_async.h"

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
	*resolved = *configured;
	*native_status = 0;
	provider_state[0]++;
	return PGRAC_FENCED_PROVIDER_OK;
}

static PgracFencedProviderResult
test_actuate(const PgracFencedTargetV1 *target, uint64_t deadline_mono_ns, int32 *native_status)
{
	(void)target;
	(void)deadline_mono_ns;
	*native_status = 0;
	provider_state[1]++;
	provider_state[2] = PGRAC_FENCED_TARGET_ON;
	return PGRAC_FENCED_PROVIDER_OK;
}

static PgracFencedProviderResult
test_readback(const PgracFencedTargetV1 *target, uint64_t deadline_mono_ns,
			  PgracFencedReadbackV1 *readback)
{
	(void)deadline_mono_ns;
	memset(readback, 0, sizeof(*readback));
	readback->state = provider_state[2];
	readback->io_drain_state = PGRAC_FENCED_IO_DRAIN_DRAINED;
	memcpy(readback->observed_target_uuid, target->target_uuid,
		   sizeof(readback->observed_target_uuid));
	provider_state[3]++;
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

static int
open_context(PgracFencedOperationContextV1 *operation_context,
			 PgracFencedRejoinContextV1 *rejoin_context, PgracFencedJournalScanState *journal_state,
			 PgracFencedConfigV1 *config, PgracFencedProviderOpsV1 *ops, char path[64])
{
	uint8 config_digest[32];
	uint8 daemon_boot_id[16];
	int fd;

	memset(config, 0, sizeof(*config));
	config->format_version = 1;
	config->mapping_generation = 17;
	config->system_identifier = 9001;
	config->storage_backend_id = 2;
	memset(config->storage_uuid, 0x31, sizeof(config->storage_uuid));
	config->provider_id = PGRAC_FENCED_PROVIDER_ID_TEST_ONLY;
	config->provider_abi = PGRAC_FENCED_PROVIDER_ABI_V1;
	config->node_count = 1;
	config->nodes[3].present = true;
	memset(config->nodes[3].target_uuid, 0x41, sizeof(config->nodes[3].target_uuid));
	memset(ops, 0, sizeof(*ops));
	ops->abi_version = PGRAC_FENCED_PROVIDER_ABI_V1;
	ops->struct_size = sizeof(*ops);
	ops->provider_id = PGRAC_FENCED_PROVIDER_ID_TEST_ONLY;
	ops->provider_name = "rejoin-async-test";
	ops->resolve = test_resolve;
	ops->actuate_off = test_actuate;
	ops->readback = test_readback;
	ops->actuate_on = test_actuate;
	ops->shutdown = test_shutdown;
	memset(config_digest, 0x71, sizeof(config_digest));
	memset(daemon_boot_id, 0x81, sizeof(daemon_boot_id));
	strcpy(path, "/tmp/pgrac-fenced-rejoin-async.XXXXXX");
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

static bool
finish_worker(PgracFencedOperationContextV1 *operation_context,
			  PgracFencedRejoinContextV1 *rejoin_context, PgracFencedRejoinAsyncWorkerV1 *worker,
			  PgracExternalFenceProtocolRejoinFrameV1 *response)
{
	PgracFencedRejoinAsyncEvent event;
	struct pollfd descriptor;
	int attempts = 0;

	while (attempts++ < 100) {
		descriptor.fd = pgrac_fenced_rejoin_async_fd(worker);
		descriptor.events = POLLIN | POLLHUP | POLLERR;
		descriptor.revents = 0;
		if (poll(&descriptor, 1, 100) <= 0)
			continue;
		if (!pgrac_fenced_rejoin_async_service(operation_context, rejoin_context, worker, &event,
											   response))
			return false;
		if (event == PGRAC_FENCED_REJOIN_ASYNC_COMPLETE)
			return true;
	}
	return false;
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

UT_TEST(test_parent_serializes_full_async_rejoin_lifecycle)
{
	PgracFencedOperationContextV1 operation_context;
	PgracFencedRejoinContextV1 rejoin_context;
	PgracFencedJournalScanState journal_state;
	PgracFencedConfigV1 config;
	PgracFencedProviderOpsV1 ops;
	PgracFencedRejoinAsyncWorkerV1 worker;
	PgracExternalFenceProtocolRejoinFrameV1 request;
	PgracExternalFenceProtocolRejoinFrameV1 offer;
	PgracExternalFenceProtocolRejoinFrameV1 on_result;
	PgracExternalFenceProtocolRejoinFrameV1 ready;
	uint8 operation_id[16];
	char path[64];
	int journal_fd;

	memset((void *)provider_state, 0, sizeof(uint32) * 4);
	provider_state[2] = PGRAC_FENCED_TARGET_OFF;
	journal_fd
		= open_context(&operation_context, &rejoin_context, &journal_state, &config, &ops, path);
	if (journal_fd < 0)
		return;
	memset(&request, 0, sizeof(request));
	request.opcode = PGRAC_EXTERNAL_FENCE_REJOIN_ADMIN_PREPARE;
	memset(request.transport_nonce, 0x61, sizeof(request.transport_nonce));
	request.old_node_id = 3;
	request.old_incarnation = 70;
	request.candidate_incarnation = 77;
	request.timeout_ms = 1000;
	memset(operation_id, 0xa1, sizeof(operation_id));
	UT_ASSERT(pgrac_fenced_rejoin_async_start(
		&operation_context, &rejoin_context, PGRAC_FENCED_REJOIN_ASYNC_ADMIN_PREPARE, &request,
		operation_id, false, deadline_after_ms(2000), &worker));
	UT_ASSERT(finish_worker(&operation_context, &rejoin_context, &worker, &offer));
	UT_ASSERT_EQ(rejoin_context.operation_count, 1);
	UT_ASSERT_EQ(operation_context.next_proof_generation, 1);

	memset(&request, 0, sizeof(request));
	request.opcode = PGRAC_EXTERNAL_FENCE_REJOIN_LMON_CLAIM_NEXT;
	memset(request.transport_nonce, 0x62, sizeof(request.transport_nonce));
	UT_ASSERT(pgrac_fenced_rejoin_async_start(&operation_context, &rejoin_context,
											  PGRAC_FENCED_REJOIN_ASYNC_CLAIM_NEXT, &request, NULL,
											  false, deadline_after_ms(2000), &worker));
	UT_ASSERT(finish_worker(&operation_context, &rejoin_context, &worker, &offer));
	UT_ASSERT_EQ(offer.status, PGRAC_FENCED_REJOIN_STATUS_OFFERED);
	UT_ASSERT_EQ(offer.proof_generation, 1);
	UT_ASSERT_EQ(operation_context.next_proof_generation, 2);

	make_bound_request(PGRAC_EXTERNAL_FENCE_REJOIN_LMON_AUTHORIZE_ON, &offer, 0x63, &request);
	UT_ASSERT(pgrac_fenced_rejoin_async_start(&operation_context, &rejoin_context,
											  PGRAC_FENCED_REJOIN_ASYNC_AUTHORIZE_ON, &request,
											  NULL, true, deadline_after_ms(2000), &worker));
	UT_ASSERT(finish_worker(&operation_context, &rejoin_context, &worker, &on_result));
	UT_ASSERT_EQ(on_result.status, PGRAC_FENCED_REJOIN_STATUS_WAITING_JOINER);
	UT_ASSERT_EQ(on_result.proof_generation, 2);
	UT_ASSERT_EQ(provider_state[1], 1);

	make_bound_request(PGRAC_EXTERNAL_FENCE_REJOIN_LMON_REFRESH_ON, &on_result, 0x64, &request);
	UT_ASSERT(pgrac_fenced_rejoin_async_start(&operation_context, &rejoin_context,
											  PGRAC_FENCED_REJOIN_ASYNC_REFRESH_ON, &request, NULL,
											  false, deadline_after_ms(2000), &worker));
	UT_ASSERT(finish_worker(&operation_context, &rejoin_context, &worker, &ready));
	UT_ASSERT_EQ(ready.status, PGRAC_FENCED_REJOIN_STATUS_READY);
	UT_ASSERT_EQ(ready.proof_generation, 3);
	UT_ASSERT_EQ(operation_context.next_proof_generation, 4);
	UT_ASSERT_EQ(provider_state[1], 1);
	UT_ASSERT_EQ(provider_state[3], 3);
	UT_ASSERT_EQ(pgrac_fenced_rejoin_async_fd(&worker), -1);
	(void)close(journal_fd);
	(void)unlink(path);
}

int
main(void)
{
	provider_state
		= mmap(NULL, sizeof(uint32) * 4, PROT_READ | PROT_WRITE, MAP_SHARED | MAP_ANON, -1, 0);
	if (provider_state == MAP_FAILED)
		return 1;
	UT_PLAN(1);
	UT_RUN(test_parent_serializes_full_async_rejoin_lifecycle);
	UT_DONE();
	(void)munmap((void *)provider_state, sizeof(uint32) * 4);
	return ut_failed_count == 0 ? 0 : 1;
}
