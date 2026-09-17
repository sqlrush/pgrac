/*-------------------------------------------------------------------------
 *
 * test_pgrac_fenced_session.c
 *    Authenticated exact-frame pgrac-fenced DB session tests.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <fcntl.h>
#include <stdlib.h>
#include <sys/socket.h>
#include <time.h>
#include <unistd.h>

#include "common/pgrac_external_fence_protocol.h"
#include "pgrac_fenced_dispatch.h"
#include "pgrac_fenced_session.h"

#undef printf
#undef fprintf
#undef snprintf

#include "unit_test.h"

UT_DEFINE_GLOBALS();

static PgracFencedProviderResult
test_resolve(const PgracFencedTargetV1 *configured, PgracFencedTargetV1 *resolved,
			 int32 *native_status)
{
	*resolved = *configured;
	*native_status = 0;
	return PGRAC_FENCED_PROVIDER_OK;
}

static PgracFencedProviderResult
test_actuate(const PgracFencedTargetV1 *target, uint64_t deadline_mono_ns, int32 *native_status)
{
	(void)target;
	(void)deadline_mono_ns;
	*native_status = 0;
	return PGRAC_FENCED_PROVIDER_OK;
}

static PgracFencedProviderResult
test_readback(const PgracFencedTargetV1 *target, uint64_t deadline_mono_ns,
			  PgracFencedReadbackV1 *out)
{
	(void)deadline_mono_ns;
	memset(out, 0, sizeof(*out));
	out->state = PGRAC_FENCED_TARGET_OFF;
	out->io_drain_state = PGRAC_FENCED_IO_DRAIN_DRAINED;
	memcpy(out->observed_target_uuid, target->target_uuid, sizeof(out->observed_target_uuid));
	return PGRAC_FENCED_PROVIDER_OK;
}

static void
test_shutdown(void)
{}

static uint64_t
deadline_after_ms(uint64_t milliseconds)
{
	struct timespec now;

	UT_ASSERT_EQ(clock_gettime(CLOCK_MONOTONIC, &now), 0);
	return (uint64_t)now.tv_sec * UINT64_C(1000000000) + (uint64_t)now.tv_nsec
		   + milliseconds * UINT64_C(1000000);
}

static int
open_context(PgracFencedOperationContextV1 *context, PgracFencedJournalScanState *journal_state,
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
	config->allowed_db_uid = (uint64)geteuid();
	config->allowed_db_gid = (uint64)getegid();
	config->provider_id = PGRAC_FENCED_PROVIDER_ID_TEST_ONLY;
	config->provider_abi = PGRAC_FENCED_PROVIDER_ABI_V1;
	config->node_count = 1;
	config->nodes[3].present = true;
	memset(config->nodes[3].target_uuid, 0x41, sizeof(config->nodes[3].target_uuid));
	memset(ops, 0, sizeof(*ops));
	ops->abi_version = PGRAC_FENCED_PROVIDER_ABI_V1;
	ops->struct_size = sizeof(*ops);
	ops->provider_id = PGRAC_FENCED_PROVIDER_ID_TEST_ONLY;
	ops->provider_name = "session-test";
	ops->resolve = test_resolve;
	ops->actuate_off = test_actuate;
	ops->readback = test_readback;
	ops->actuate_on = test_actuate;
	ops->shutdown = test_shutdown;
	memset(config_digest, 0x71, sizeof(config_digest));
	memset(daemon_boot_id, 0x81, sizeof(daemon_boot_id));
	strcpy(path, "/tmp/pgrac-fenced-session.XXXXXX");
	fd = mkstemp(path);
	UT_ASSERT(fd >= 0);
	if (fd < 0)
		return -1;
	UT_ASSERT_EQ(fcntl(fd, F_SETFL, fcntl(fd, F_GETFL) | O_APPEND), 0);
	pgrac_fenced_journal_scan_state_init(journal_state);
	UT_ASSERT(pgrac_fenced_operation_context_init(context, config, ops, true, config_digest,
												  daemon_boot_id, fd, journal_state));
	return fd;
}

static void
make_request(const PgracFencedConfigV1 *config, PgracExternalFenceProtocolRequestV1 *request)
{
	memset(request, 0, sizeof(*request));
	memset(request->request_nonce, 0x51, sizeof(request->request_nonce));
	request->need.system_identifier = config->system_identifier;
	memset(request->need.canonical_duty_digest, 0x61, sizeof(request->need.canonical_duty_digest));
	request->need.victim_node_id = 3;
	request->need.victim_incarnation = 23;
	UT_ASSERT(pgrac_external_fence_protected_set_digest_v1(
		config->storage_backend_id, config->storage_uuid, request->need.protected_set_digest));
	request->need.predicate_id = 1;
	request->need.predicate_version = 1;
	request->timeout_ms = 1000;
}

static bool
write_all(int fd, const uint8 *bytes, size_t len)
{
	size_t used = 0;

	while (used < len) {
		ssize_t written = write(fd, bytes + used, len - used);

		if (written < 0 && errno == EINTR)
			continue;
		if (written <= 0)
			return false;
		used += (size_t)written;
	}
	return true;
}

static bool
read_all(int fd, uint8 *bytes, size_t len)
{
	size_t used = 0;

	while (used < len) {
		ssize_t got = read(fd, bytes + used, len - used);

		if (got < 0 && errno == EINTR)
			continue;
		if (got <= 0)
			return false;
		used += (size_t)got;
	}
	return true;
}

static size_t
journal_record_count(int fd)
{
	uint8 frame[PGRAC_FENCED_JOURNAL_RECORD_BYTES];
	size_t count = 0;
	ssize_t got;

	UT_ASSERT_EQ(lseek(fd, 0, SEEK_SET), 0);
	for (;;) {
		do {
			got = read(fd, frame, sizeof(frame));
		} while (got < 0 && errno == EINTR);
		if (got == 0)
			break;
		UT_ASSERT_EQ(got, sizeof(frame));
		if (got != sizeof(frame))
			break;
		count++;
	}
	return count;
}

static bool
journal_record_at(int fd, size_t index, PgracFencedJournalRecordV1 *record)
{
	uint8 frame[PGRAC_FENCED_JOURNAL_RECORD_BYTES];
	ssize_t got;

	do {
		got = pread(fd, frame, sizeof(frame), (off_t)(index * sizeof(frame)));
	} while (got < 0 && errno == EINTR);
	return got == sizeof(frame) && pgrac_fenced_journal_record_decode(frame, sizeof(frame), record);
}

UT_TEST(test_exact_exchange_returns_wire_response_and_retains_positive_fd)
{
	PgracFencedOperationContextV1 context;
	PgracFencedJournalScanState journal_state;
	PgracExternalFenceProtocolRequestV1 request;
	PgracExternalFenceProtocolResponseV1 response;
	PgracExternalFenceProtocolResponseV1 decoded;
	PgracFencedProviderOpsV1 ops;
	PgracFencedConfigV1 config;
	uint8 request_frame[PGRAC_EXTERNAL_FENCE_REQUEST_V1_BYTES];
	uint8 response_frame[PGRAC_EXTERNAL_FENCE_RESPONSE_V1_BYTES];
	char path[64];
	int sockets[2];
	int journal_fd;

	journal_fd = open_context(&context, &journal_state, &config, &ops, path);
	if (journal_fd < 0)
		return;
	make_request(&config, &request);
	UT_ASSERT(pgrac_external_fence_request_v1_encode(&request, request_frame));
	UT_ASSERT_EQ(socketpair(AF_UNIX, SOCK_STREAM, 0, sockets), 0);
	UT_ASSERT(write_all(sockets[0], request_frame, sizeof(request_frame)));
	UT_ASSERT_EQ(pgrac_fenced_session_exchange(&context, sockets[1], true, deadline_after_ms(1000),
											   &response),
				 PGRAC_FENCED_SESSION_RETAINED);
	UT_ASSERT(read_all(sockets[0], response_frame, sizeof(response_frame)));
	UT_ASSERT(
		pgrac_external_fence_response_v1_decode(response_frame, sizeof(response_frame), &decoded));
	UT_ASSERT_EQ(decoded.verdict, 1);
	UT_ASSERT(pgrac_external_fence_affirmative_response_matches_request_v1(&request, &decoded));
	UT_ASSERT_EQ(journal_record_count(journal_fd), 6);
	(void)close(sockets[0]);
	(void)close(sockets[1]);
	(void)close(journal_fd);
	(void)unlink(path);
}

UT_TEST(test_nonaffirmative_response_closes_session_without_retention)
{
	PgracFencedOperationContextV1 context;
	PgracFencedJournalScanState journal_state;
	PgracExternalFenceProtocolRequestV1 request;
	PgracExternalFenceProtocolResponseV1 response;
	PgracExternalFenceProtocolResponseV1 decoded;
	PgracFencedProviderOpsV1 ops;
	PgracFencedConfigV1 config;
	uint8 request_frame[PGRAC_EXTERNAL_FENCE_REQUEST_V1_BYTES];
	uint8 response_frame[PGRAC_EXTERNAL_FENCE_RESPONSE_V1_BYTES];
	char path[64];
	int sockets[2];
	int journal_fd;

	journal_fd = open_context(&context, &journal_state, &config, &ops, path);
	if (journal_fd < 0)
		return;
	make_request(&config, &request);
	request.need.protected_set_digest[0] ^= 1;
	UT_ASSERT(pgrac_external_fence_request_v1_encode(&request, request_frame));
	UT_ASSERT_EQ(socketpair(AF_UNIX, SOCK_STREAM, 0, sockets), 0);
	UT_ASSERT(write_all(sockets[0], request_frame, sizeof(request_frame)));
	UT_ASSERT_EQ(pgrac_fenced_session_exchange(&context, sockets[1], true, deadline_after_ms(1000),
											   &response),
				 PGRAC_FENCED_SESSION_CLOSED);
	UT_ASSERT(read_all(sockets[0], response_frame, sizeof(response_frame)));
	UT_ASSERT(
		pgrac_external_fence_response_v1_decode(response_frame, sizeof(response_frame), &decoded));
	UT_ASSERT_EQ(decoded.verdict, 4);
	UT_ASSERT_EQ(decoded.deny_reason, 4);
	UT_ASSERT_EQ(journal_record_count(journal_fd), 1);
	(void)close(sockets[0]);
	(void)close(sockets[1]);
	(void)close(journal_fd);
	(void)unlink(path);
}

UT_TEST(test_peer_and_protocol_failures_do_not_reach_provider)
{
	PgracFencedOperationContextV1 context;
	PgracFencedJournalScanState journal_state;
	PgracExternalFenceProtocolRequestV1 request;
	PgracExternalFenceProtocolResponseV1 response;
	PgracFencedProviderOpsV1 ops;
	PgracFencedConfigV1 config;
	uint8 request_frame[PGRAC_EXTERNAL_FENCE_REQUEST_V1_BYTES];
	char path[64];
	int sockets[2];
	int journal_fd;

	journal_fd = open_context(&context, &journal_state, &config, &ops, path);
	if (journal_fd < 0)
		return;
	make_request(&config, &request);
	UT_ASSERT(pgrac_external_fence_request_v1_encode(&request, request_frame));
	UT_ASSERT_EQ(socketpair(AF_UNIX, SOCK_STREAM, 0, sockets), 0);
	UT_ASSERT(write_all(sockets[0], request_frame, sizeof(request_frame)));
	config.allowed_db_uid++;
	UT_ASSERT_EQ(pgrac_fenced_session_exchange(&context, sockets[1], true, deadline_after_ms(1000),
											   &response),
				 PGRAC_FENCED_SESSION_ERROR);
	config.allowed_db_uid--;
	(void)close(sockets[0]);
	(void)close(sockets[1]);

	memset(request_frame, 0, sizeof(request_frame));
	UT_ASSERT_EQ(socketpair(AF_UNIX, SOCK_STREAM, 0, sockets), 0);
	UT_ASSERT(write_all(sockets[0], request_frame, sizeof(request_frame)));
	UT_ASSERT_EQ(pgrac_fenced_session_exchange(&context, sockets[1], true, deadline_after_ms(1000),
											   &response),
				 PGRAC_FENCED_SESSION_ERROR);
	UT_ASSERT_EQ(journal_record_count(journal_fd), 1);
	(void)close(sockets[0]);
	(void)close(sockets[1]);
	(void)close(journal_fd);
	(void)unlink(path);
}

static void
run_retention_invalidation(bool expire)
{
	PgracFencedOperationContextV1 context;
	PgracFencedJournalScanState journal_state;
	PgracFencedJournalRecordV1 record;
	PgracExternalFenceProtocolRequestV1 request;
	PgracExternalFenceProtocolResponseV1 response;
	PgracFencedProviderOpsV1 ops;
	PgracFencedConfigV1 config;
	uint8 request_frame[PGRAC_EXTERNAL_FENCE_REQUEST_V1_BYTES];
	uint8 response_frame[PGRAC_EXTERNAL_FENCE_RESPONSE_V1_BYTES];
	uint32 reason = 0;
	char path[64];
	int sockets[2];
	int journal_fd;

	journal_fd = open_context(&context, &journal_state, &config, &ops, path);
	if (journal_fd < 0)
		return;
	make_request(&config, &request);
	UT_ASSERT(pgrac_external_fence_request_v1_encode(&request, request_frame));
	UT_ASSERT_EQ(socketpair(AF_UNIX, SOCK_STREAM, 0, sockets), 0);
	UT_ASSERT(write_all(sockets[0], request_frame, sizeof(request_frame)));
	UT_ASSERT_EQ(pgrac_fenced_session_exchange(&context, sockets[1], true, deadline_after_ms(1000),
											   &response),
				 PGRAC_FENCED_SESSION_RETAINED);
	UT_ASSERT(read_all(sockets[0], response_frame, sizeof(response_frame)));
	UT_ASSERT(!pgrac_fenced_session_retention_event(sockets[1], &response,
													response.verified_mono_ns, &reason));
	if (expire) {
		UT_ASSERT(pgrac_fenced_session_retention_event(sockets[1], &response,
													   response.fresh_until_mono_ns, &reason));
		UT_ASSERT_EQ(reason, 14);
	} else {
		UT_ASSERT(write_all(sockets[0], request_frame, 1));
		UT_ASSERT(pgrac_fenced_session_retention_event(sockets[1], &response,
													   response.verified_mono_ns, &reason));
		UT_ASSERT_EQ(reason, 16);
	}
	UT_ASSERT(pgrac_fenced_operation_invalidate(&context, &response, reason));
	UT_ASSERT_EQ(journal_record_count(journal_fd), 7);
	UT_ASSERT(journal_record_at(journal_fd, 6, &record));
	UT_ASSERT_EQ(record.record_kind, PGRAC_FENCED_JOURNAL_KIND_INVALIDATED);
	UT_ASSERT_EQ(record.deny_reason, reason);
	UT_ASSERT(memcmp(record.operation_id, request.request_nonce, sizeof(record.operation_id)) == 0);
	(void)close(sockets[0]);
	(void)close(sockets[1]);
	(void)close(journal_fd);
	(void)unlink(path);
}

UT_TEST(test_retained_extra_bytes_invalidate_live_proof)
{
	run_retention_invalidation(false);
}

UT_TEST(test_retained_expiry_invalidates_live_proof)
{
	run_retention_invalidation(true);
}

UT_TEST(test_dispatch_retains_then_reaps_client_event)
{
	PgracFencedOperationContextV1 context;
	PgracFencedJournalScanState journal_state;
	PgracExternalFenceProtocolRequestV1 request;
	PgracFencedProviderOpsV1 ops;
	PgracFencedConfigV1 config;
	PgracFencedDispatcherV1 dispatcher;
	uint8 request_frame[PGRAC_EXTERNAL_FENCE_REQUEST_V1_BYTES];
	uint8 response_frame[PGRAC_EXTERNAL_FENCE_RESPONSE_V1_BYTES];
	char path[64];
	int sockets[2];
	int journal_fd;

	journal_fd = open_context(&context, &journal_state, &config, &ops, path);
	if (journal_fd < 0)
		return;
	UT_ASSERT(pgrac_fenced_dispatch_init(&dispatcher, &context));
	make_request(&config, &request);
	UT_ASSERT(pgrac_external_fence_request_v1_encode(&request, request_frame));
	UT_ASSERT_EQ(socketpair(AF_UNIX, SOCK_STREAM, 0, sockets), 0);
	UT_ASSERT(write_all(sockets[0], request_frame, sizeof(request_frame)));
	UT_ASSERT(pgrac_fenced_dispatch_accept_fd(&dispatcher, sockets[1], deadline_after_ms(1000)));
	UT_ASSERT_EQ(dispatcher.client_count, 1);
	UT_ASSERT(read_all(sockets[0], response_frame, sizeof(response_frame)));
	UT_ASSERT(write_all(sockets[0], request_frame, 1));
	UT_ASSERT(
		pgrac_fenced_dispatch_reap(&dispatcher, dispatcher.clients[0].response.verified_mono_ns));
	UT_ASSERT_EQ(dispatcher.client_count, 0);
	UT_ASSERT_EQ(journal_record_count(journal_fd), 7);
	(void)close(sockets[0]);
	(void)close(journal_fd);
	(void)unlink(path);
}

UT_TEST(test_dispatch_capacity_denial_has_zero_operation_effect)
{
	PgracFencedOperationContextV1 context;
	PgracFencedJournalScanState journal_state;
	PgracExternalFenceProtocolRequestV1 request;
	PgracExternalFenceProtocolResponseV1 response;
	PgracFencedProviderOpsV1 ops;
	PgracFencedConfigV1 config;
	PgracFencedDispatcherV1 dispatcher;
	uint8 request_frame[PGRAC_EXTERNAL_FENCE_REQUEST_V1_BYTES];
	uint8 response_frame[PGRAC_EXTERNAL_FENCE_RESPONSE_V1_BYTES];
	char path[64];
	int sockets[2];
	int journal_fd;

	journal_fd = open_context(&context, &journal_state, &config, &ops, path);
	if (journal_fd < 0)
		return;
	UT_ASSERT(pgrac_fenced_dispatch_init(&dispatcher, &context));
	dispatcher.client_count = PGRAC_FENCED_MAX_CLIENTS;
	make_request(&config, &request);
	UT_ASSERT(pgrac_external_fence_request_v1_encode(&request, request_frame));
	UT_ASSERT_EQ(socketpair(AF_UNIX, SOCK_STREAM, 0, sockets), 0);
	UT_ASSERT(write_all(sockets[0], request_frame, sizeof(request_frame)));
	UT_ASSERT(pgrac_fenced_dispatch_accept_fd(&dispatcher, sockets[1], deadline_after_ms(1000)));
	UT_ASSERT(read_all(sockets[0], response_frame, sizeof(response_frame)));
	UT_ASSERT(
		pgrac_external_fence_response_v1_decode(response_frame, sizeof(response_frame), &response));
	UT_ASSERT_EQ(response.verdict, 4);
	UT_ASSERT_EQ(response.deny_reason, 10);
	UT_ASSERT_EQ(journal_record_count(journal_fd), 1);
	(void)close(sockets[0]);
	(void)close(journal_fd);
	(void)unlink(path);
}

UT_TEST(test_dispatch_journal_failure_closes_all_live_clients)
{
	PgracFencedOperationContextV1 context;
	PgracFencedJournalScanState journal_state;
	PgracExternalFenceProtocolRequestV1 request;
	PgracFencedProviderOpsV1 ops;
	PgracFencedConfigV1 config;
	PgracFencedDispatcherV1 dispatcher;
	uint8 request_frame[PGRAC_EXTERNAL_FENCE_REQUEST_V1_BYTES];
	uint8 response_frame[PGRAC_EXTERNAL_FENCE_RESPONSE_V1_BYTES];
	char path[64];
	int sockets[2];
	int journal_fd;

	journal_fd = open_context(&context, &journal_state, &config, &ops, path);
	if (journal_fd < 0)
		return;
	UT_ASSERT(pgrac_fenced_dispatch_init(&dispatcher, &context));
	make_request(&config, &request);
	UT_ASSERT(pgrac_external_fence_request_v1_encode(&request, request_frame));
	UT_ASSERT_EQ(socketpair(AF_UNIX, SOCK_STREAM, 0, sockets), 0);
	UT_ASSERT(write_all(sockets[0], request_frame, sizeof(request_frame)));
	UT_ASSERT(pgrac_fenced_dispatch_accept_fd(&dispatcher, sockets[1], deadline_after_ms(1000)));
	UT_ASSERT(read_all(sockets[0], response_frame, sizeof(response_frame)));
	UT_ASSERT_EQ(close(journal_fd), 0);
	UT_ASSERT(write_all(sockets[0], request_frame, 1));
	UT_ASSERT(
		!pgrac_fenced_dispatch_reap(&dispatcher, dispatcher.clients[0].response.verified_mono_ns));
	UT_ASSERT_EQ(dispatcher.client_count, 0);
	UT_ASSERT(!context.available);
	(void)close(sockets[0]);
	(void)unlink(path);
}

int
main(void)
{
	UT_PLAN(8);
	UT_RUN(test_exact_exchange_returns_wire_response_and_retains_positive_fd);
	UT_RUN(test_nonaffirmative_response_closes_session_without_retention);
	UT_RUN(test_peer_and_protocol_failures_do_not_reach_provider);
	UT_RUN(test_retained_extra_bytes_invalidate_live_proof);
	UT_RUN(test_retained_expiry_invalidates_live_proof);
	UT_RUN(test_dispatch_retains_then_reaps_client_event);
	UT_RUN(test_dispatch_capacity_denial_has_zero_operation_effect);
	UT_RUN(test_dispatch_journal_failure_closes_all_live_clients);
	UT_DONE();

	return ut_failed_count == 0 ? 0 : 1;
}
