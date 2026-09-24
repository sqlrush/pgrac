/*-------------------------------------------------------------------------
 *
 * test_pgrac_fenced_rejoin_async.c
 *    Parent-owned PFRJ state, proof generations and journal serialization.
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <fcntl.h>
#include <poll.h>
#include <signal.h>
#include <sys/mman.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>

#include "pgrac_fenced_rejoin_async.h"
#include "common/pgrac_fence_map.h"

#ifdef USE_OPENSSL
#include <openssl/evp.h>
#endif

#undef printf
#undef fprintf
#undef snprintf

#include "unit_test.h"

UT_DEFINE_GLOBALS();

#include "data/pgrac_fence_map_v2_fixture.h"

static volatile uint32 *provider_state;
static void stop_test_worker(PgracFencedRejoinAsyncWorkerV1 *worker);

/* Two independently identified targets, signed with an ephemeral test-only key. */
static bool
add_second_test_target(PgracFencedConfigV1 *config)
{
#ifdef USE_OPENSSL
	PgracFencedNodeConfigV1 original = config->nodes[2];
	EVP_PKEY_CTX *key_context = EVP_PKEY_CTX_new_id(EVP_PKEY_ED25519, NULL);
	EVP_PKEY *key = NULL;
	EVP_MD_CTX *signer = EVP_MD_CTX_new();
	size_t key_length = sizeof(config->map_public_key);
	bool ok = false;

	if (key_context == NULL || signer == NULL || EVP_PKEY_keygen_init(key_context) != 1
		|| EVP_PKEY_keygen(key_context, &key) != 1
		|| EVP_PKEY_get_raw_public_key(key, config->map_public_key, &key_length) != 1)
		goto done;
	for (int node_id = 2; node_id <= 3; ++node_id) {
		PgracFencedNodeConfigV1 *node = &config->nodes[node_id];
		PgracProtectedSetDecodedV2 decoded;
		size_t payload_length, signature_length = PGRAC_FENCE_MAP_V2_SIGNATURE_BYTES;
		size_t old_payload = original.adapter_data_len - PGRAC_FENCE_MAP_V2_HEADER_BYTES
							 - PGRAC_FENCE_MAP_V2_SIGNATURE_BYTES;

		*node = original;
		if (!pgrac_protected_set_v2_decode(original.adapter_data + PGRAC_FENCE_MAP_V2_HEADER_BYTES,
										   old_payload, &decoded))
			goto done;
		if (node_id == 3) {
			memset(decoded.set.guest_uuid, 6, 16);
			for (uint32 route = 0; route < decoded.set.route_count; ++route) {
				decoded.routes[route].initiator.data = "iqn.2026-09.test:guest3";
				decoded.routes[route].initiator.length
					= strlen(decoded.routes[route].initiator.data);
			}
		}
		memcpy(node->target_uuid, decoded.set.guest_uuid, 16);
		if (!pgrac_external_fence_protected_set_digest_v2(&decoded.set, node->protected_set_digest)
			|| !pgrac_protected_set_v2_encode(
				&decoded.set, node->adapter_data + PGRAC_FENCE_MAP_V2_HEADER_BYTES,
				sizeof(node->adapter_data) - PGRAC_FENCE_MAP_V2_HEADER_BYTES - signature_length,
				&payload_length))
			goto done;
		for (int byte = 0; byte < 4; ++byte) {
			node->adapter_data[12 + byte] = (uint8)((uint32)node_id >> (byte * 8));
			node->adapter_data[24 + byte] = (uint8)((uint32)payload_length >> (byte * 8));
		}
		if (EVP_DigestSignInit(signer, NULL, NULL, NULL, key) != 1
			|| EVP_DigestSign(signer,
							  node->adapter_data + PGRAC_FENCE_MAP_V2_HEADER_BYTES + payload_length,
							  &signature_length, node->adapter_data,
							  PGRAC_FENCE_MAP_V2_HEADER_BYTES + payload_length)
				   != 1)
			goto done;
		node->adapter_data_len
			= PGRAC_FENCE_MAP_V2_HEADER_BYTES + payload_length + signature_length;
	}
	config->node_count = 2;
	ok = true;
done:
	EVP_MD_CTX_free(signer);
	EVP_PKEY_free(key);
	EVP_PKEY_CTX_free(key_context);
	return ok;
#else
	(void)config;
	return false;
#endif
}

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
test_actuate_off(const PgracFencedTargetV1 *target, uint64_t deadline_mono_ns, int32 *native_status)
{
	(void)target;
	(void)deadline_mono_ns;
	*native_status = 0;
	provider_state[4]++;
	provider_state[2] = PGRAC_FENCED_TARGET_OFF;
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
			 PgracFencedConfigV1 *config, PgracFencedProviderOpsV1 *ops, char path[64], bool owned)
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
	ops->actuate_off = test_actuate_off;
	ops->readback = test_readback;
	ops->actuate_on = test_actuate;
	ops->shutdown = test_shutdown;
	if (owned) {
		PgracFencedConfigResult parsed = pgrac_fenced_config_parse(
			(const uint8 *)fenced_config_v2, sizeof(fenced_config_v2) - 1, config);
#ifndef USE_OPENSSL
		UT_ASSERT_NE(parsed, PGRAC_FENCED_CONFIG_OK);
		return -1;
#else
		UT_ASSERT_EQ(parsed, PGRAC_FENCED_CONFIG_OK);
		if (parsed != PGRAC_FENCED_CONFIG_OK)
			return -1;
		ops->provider_id = PGRAC_FENCED_PROVIDER_ID_PACEMAKER_LIBVIRT_V1;
#endif
	}
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

static void
full_async_lifecycle(bool owned, bool restart, bool cleanup, int retirement_stage)
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

	memset((void *)provider_state, 0, sizeof(uint32) * 5);
	provider_state[2] = PGRAC_FENCED_TARGET_OFF;
	journal_fd = open_context(&operation_context, &rejoin_context, &journal_state, &config, &ops,
							  path, owned);
	if (journal_fd < 0)
		return;
	if (retirement_stage >= 0 && !add_second_test_target(&config)) {
		UT_ASSERT(false);
		(void)close(journal_fd);
		(void)unlink(path);
		return;
	}
	memset(&request, 0, sizeof(request));
	request.opcode = PGRAC_EXTERNAL_FENCE_REJOIN_ADMIN_PREPARE;
	memset(request.transport_nonce, 0x61, sizeof(request.transport_nonce));
	request.old_node_id = owned ? 2 : 3;
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
	if (restart) {
		PgracFencedJournalReconcileState replay;
		uint8 digest[32], boot[16];

		pgrac_fenced_journal_reconcile_state_init(&replay);
		UT_ASSERT(pgrac_fenced_journal_reconcile_observe(
			&replay, &rejoin_context.operations[0].last_record));
		memcpy(digest, operation_context.semantic_config_digest, 32);
		memset(boot, 0x82, 16);
		UT_ASSERT(pgrac_fenced_operation_context_init(&operation_context, &config, &ops, true,
													  digest, boot, journal_fd, &journal_state));
		UT_ASSERT(pgrac_fenced_rejoin_init(&rejoin_context, &operation_context));
		UT_ASSERT(pgrac_fenced_rejoin_restore(&rejoin_context, &replay));
	}

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
	if (cleanup) {
		bool started;

		rejoin_context.operations[0].state = PGRAC_FENCED_REJOIN_OPERATION_CLEANUP_REQUIRED;
		request = rejoin_context.operations[0].last_record.intent.request.rejoin;
		started = pgrac_fenced_rejoin_async_start(&operation_context, &rejoin_context,
												  PGRAC_FENCED_REJOIN_ASYNC_CLEANUP, &request, NULL,
												  false, deadline_after_ms(2000), &worker);
		UT_ASSERT(started);
		if (started) {
			UT_ASSERT(finish_worker(&operation_context, &rejoin_context, &worker, &ready));
			UT_ASSERT_EQ(ready.opcode, 0);
			UT_ASSERT(
				pgrac_fenced_journal_rejoin_terminal(&rejoin_context.operations[0].last_record));
			UT_ASSERT_EQ(rejoin_context.operations[0].last_record.target_state,
						 PGRAC_FENCED_TARGET_OFF);
			UT_ASSERT_EQ(provider_state[4], 1);
			UT_ASSERT_EQ(provider_state[1], 1);
			UT_ASSERT_EQ(operation_context.next_proof_generation, 4);
		}
		(void)close(journal_fd);
		(void)unlink(path);
		return;
	}

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
	if (owned) {
		uint64 next_seq = journal_state.next_seq;
		PgracFencedRejoinOperationV1 completed = rejoin_context.operations[0];

		/* An identical REFRESH must not create another durable attempt or proof. */
		UT_ASSERT(pgrac_fenced_rejoin_async_start(&operation_context, &rejoin_context,
												  PGRAC_FENCED_REJOIN_ASYNC_REFRESH_ON, &request,
												  NULL, false, deadline_after_ms(2000), &worker));
		UT_ASSERT(finish_worker(&operation_context, &rejoin_context, &worker, &ready));
		UT_ASSERT_EQ(ready.status, PGRAC_FENCED_REJOIN_STATUS_STALE);
		UT_ASSERT_EQ(journal_state.next_seq, next_seq);
		UT_ASSERT(memcmp(&completed, &rejoin_context.operations[0], sizeof(completed)) == 0);
	}
	if (retirement_stage >= 0) {
		uint8 next_operation[16];
		bool finished;

		memset(&request, 0, sizeof(request));
		request.opcode = PGRAC_EXTERNAL_FENCE_REJOIN_ADMIN_PREPARE;
		memset(request.transport_nonce, 0x67, 16);
		request.old_node_id = 3;
		request.old_incarnation = 80;
		request.candidate_incarnation = 81;
		request.timeout_ms = 1000;
		memset(next_operation, 0xa2, 16);
		UT_ASSERT(pgrac_fenced_rejoin_async_start(
			&operation_context, &rejoin_context, PGRAC_FENCED_REJOIN_ASYNC_ADMIN_PREPARE, &request,
			next_operation, false, deadline_after_ms(2000), &worker));
		if (retirement_stage == 1) {
			PgracFencedRejoinAsyncEvent event;
			struct pollfd descriptor = { worker.fd, POLLIN, 0 };
			UT_ASSERT_EQ(poll(&descriptor, 1, 2000), 1);
			UT_ASSERT(pgrac_fenced_rejoin_async_service(&operation_context, &rejoin_context,
														&worker, &event, &ready));
			UT_ASSERT_EQ(event, PGRAC_FENCED_REJOIN_ASYNC_JOURNAL);
		}
		UT_ASSERT(pgrac_fenced_rejoin_detach(&rejoin_context, operation_id));
		UT_ASSERT_EQ(rejoin_context.operation_count, 0);
		finished = finish_worker(&operation_context, &rejoin_context, &worker, &offer);
		UT_ASSERT(finished);
		if (!finished)
			stop_test_worker(&worker);
		UT_ASSERT_EQ(rejoin_context.operation_count, 1);
		UT_ASSERT_EQ(rejoin_context.operations[0].state, PGRAC_FENCED_REJOIN_OPERATION_UNUSED);
		UT_ASSERT_EQ(rejoin_context.operations[1].state, PGRAC_FENCED_REJOIN_OPERATION_OFFERED);
		UT_ASSERT(memcmp(rejoin_context.operations[1].operation_id, next_operation, 16) == 0);
	}
	(void)close(journal_fd);
	(void)unlink(path);
}

UT_TEST(test_parent_serializes_full_async_rejoin_lifecycle)
{
	full_async_lifecycle(false, false, false, -1);
}

UT_TEST(test_parent_serializes_owned_async_rejoin_lifecycle)
{
	full_async_lifecycle(true, false, false, -1);
}

UT_TEST(test_replayed_admin_claims_fresh_proof_in_new_boot)
{
	full_async_lifecycle(true, true, false, -1);
}

UT_TEST(test_cleanup_worker_uses_owned_identity_and_no_client_reply)
{
	full_async_lifecycle(true, false, true, -1);
}

UT_TEST(test_other_target_retirement_does_not_invalidate_owned_worker)
{
	full_async_lifecycle(true, false, false, 0);
	full_async_lifecycle(true, false, false, 1);
}

static void
stop_test_worker(PgracFencedRejoinAsyncWorkerV1 *worker)
{
	if (worker->fd >= 0)
		(void)close(worker->fd);
	if (worker->active) {
		(void)kill(worker->pid, SIGKILL);
		while (waitpid(worker->pid, NULL, 0) < 0 && errno == EINTR)
			;
	}
}

/* Real child messages must not overwrite a changed parent operation slot. */
UT_TEST(test_owned_worker_cannot_publish_after_parent_identity_changes)
{
	for (int after_journal = 0; after_journal < 3; ++after_journal) {
		PgracFencedOperationContextV1 operation_context;
		PgracFencedRejoinContextV1 context;
		PgracFencedJournalScanState journal_state;
		PgracFencedConfigV1 config;
		PgracFencedProviderOpsV1 ops;
		PgracFencedRejoinAsyncWorkerV1 worker;
		PgracExternalFenceProtocolRejoinFrameV1 request, response;
		PgracFencedRejoinAsyncEvent event;
		uint8 operation_id[16];
		uint64 next_seq;
		char path[64];
		int fd
			= open_context(&operation_context, &context, &journal_state, &config, &ops, path, true);

		if (fd < 0)
			return;
		memset((void *)provider_state, 0, sizeof(uint32) * 4);
		provider_state[2] = PGRAC_FENCED_TARGET_OFF;
		memset(&request, 0, sizeof(request));
		request.opcode = PGRAC_EXTERNAL_FENCE_REJOIN_ADMIN_PREPARE;
		memset(request.transport_nonce, 0x81, 16);
		request.old_node_id = 2;
		request.old_incarnation = 91;
		request.candidate_incarnation = 92;
		request.timeout_ms = 1000;
		memset(operation_id, 0xa8, 16);
		UT_ASSERT(pgrac_fenced_rejoin_async_start(
			&operation_context, &context, PGRAC_FENCED_REJOIN_ASYNC_ADMIN_PREPARE, &request,
			operation_id, false, deadline_after_ms(2000), &worker));
		if (after_journal) {
			struct pollfd poll_fd = { worker.fd, POLLIN, 0 };
			UT_ASSERT_EQ(poll(&poll_fd, 1, 2000), 1);
			UT_ASSERT(pgrac_fenced_rejoin_async_service(&operation_context, &context, &worker,
														&event, &response));
			UT_ASSERT_EQ(event, PGRAC_FENCED_REJOIN_ASYNC_JOURNAL);
		}
		next_seq = journal_state.next_seq;
		if (after_journal == 2)
			worker.last_record.seq++;
		else {
			context.operations[0].state = PGRAC_FENCED_REJOIN_OPERATION_OFFERED;
			memset(context.operations[0].operation_id, 0xdd, 16);
			context.operation_count = 1;
		}
		UT_ASSERT(!finish_worker(&operation_context, &context, &worker, &response));
		UT_ASSERT_EQ(context.operations[0].operation_id[0], after_journal == 2 ? 0 : 0xdd);
		UT_ASSERT_EQ(journal_state.next_seq, next_seq);
		UT_ASSERT_EQ(provider_state[1], 0);
		stop_test_worker(&worker);
		(void)close(fd);
		(void)unlink(path);
	}
}

int
main(void)
{
	provider_state
		= mmap(NULL, sizeof(uint32) * 5, PROT_READ | PROT_WRITE, MAP_SHARED | MAP_ANON, -1, 0);
	if (provider_state == MAP_FAILED)
		return 1;
	UT_PLAN(6);
	UT_RUN(test_parent_serializes_full_async_rejoin_lifecycle);
	UT_RUN(test_parent_serializes_owned_async_rejoin_lifecycle);
	UT_RUN(test_owned_worker_cannot_publish_after_parent_identity_changes);
	UT_RUN(test_replayed_admin_claims_fresh_proof_in_new_boot);
	UT_RUN(test_cleanup_worker_uses_owned_identity_and_no_client_reply);
	UT_RUN(test_other_target_retirement_does_not_invalidate_owned_worker);
	UT_DONE();
	(void)munmap((void *)provider_state, sizeof(uint32) * 5);
	return ut_failed_count == 0 ? 0 : 1;
}
