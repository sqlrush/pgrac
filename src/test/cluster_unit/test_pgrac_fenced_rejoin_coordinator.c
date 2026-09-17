/*-------------------------------------------------------------------------
 *
 * test_pgrac_fenced_rejoin_coordinator.c
 *    PFRJ socket lifecycle and exact-target scalar exclusion.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <fcntl.h>
#include <sys/mman.h>
#include <sys/socket.h>
#include <time.h>
#include <unistd.h>

#include "pgrac_fenced_rejoin_coordinator.h"

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
	return PGRAC_FENCED_PROVIDER_OK;
}

static PgracFencedProviderResult
test_actuate_off(const PgracFencedTargetV1 *target, uint64_t deadline_mono_ns, int32 *native_status)
{
	(void)target;
	(void)deadline_mono_ns;
	*native_status = 0;
	provider_state[0]++;
	provider_state[2] = PGRAC_FENCED_TARGET_OFF;
	return PGRAC_FENCED_PROVIDER_OK;
}

static PgracFencedProviderResult
test_actuate_on(const PgracFencedTargetV1 *target, uint64_t deadline_mono_ns, int32 *native_status)
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
now_ns(void)
{
	struct timespec now;

	UT_ASSERT_EQ(clock_gettime(CLOCK_MONOTONIC, &now), 0);
	return (uint64)now.tv_sec * UINT64_C(1000000000) + (uint64)now.tv_nsec;
}

static uint64
deadline_after_ms(uint64 milliseconds)
{
	return now_ns() + milliseconds * UINT64_C(1000000);
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
	ops->provider_name = "rejoin-coordinator-test";
	ops->resolve = test_resolve;
	ops->actuate_off = test_actuate_off;
	ops->readback = test_readback;
	ops->actuate_on = test_actuate_on;
	ops->shutdown = test_shutdown;
	memset(config_digest, 0x71, sizeof(config_digest));
	memset(daemon_boot_id, 0x81, sizeof(daemon_boot_id));
	strcpy(path, "/tmp/pgrac-fenced-rejoin-coordinator.XXXXXX");
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

static bool
write_rejoin(int fd, const PgracExternalFenceProtocolRejoinFrameV1 *request)
{
	uint8 frame[PGRAC_EXTERNAL_FENCE_REJOIN_V1_BYTES];

	return pgrac_external_fence_rejoin_v1_encode(request, frame)
		   && write(fd, frame, sizeof(frame)) == sizeof(frame);
}

static bool
try_rejoin(int fd, uint8 frame[PGRAC_EXTERNAL_FENCE_REJOIN_V1_BYTES], size_t *used,
		   PgracExternalFenceProtocolRejoinFrameV1 *response)
{
	ssize_t got;

	got = recv(fd, frame + *used, PGRAC_EXTERNAL_FENCE_REJOIN_V1_BYTES - *used, MSG_DONTWAIT);
	if (got > 0)
		*used += (size_t)got;
	else if (got < 0 && (errno == EAGAIN || errno == EWOULDBLOCK))
		return false;
	else
		return false;
	if (*used != PGRAC_EXTERNAL_FENCE_REJOIN_V1_BYTES)
		return false;
	*used = 0;
	return pgrac_external_fence_rejoin_v1_decode(frame, PGRAC_EXTERNAL_FENCE_REJOIN_V1_BYTES,
												 response);
}

static bool
await_rejoin(PgracFencedCoordinatorV1 *scalar, PgracFencedRejoinCoordinatorV1 *rejoin, int peer_fd,
			 PgracExternalFenceProtocolRejoinFrameV1 *response)
{
	uint8 frame[PGRAC_EXTERNAL_FENCE_REJOIN_V1_BYTES];
	struct timespec pause = { 0, 1000000 };
	size_t used = 0;
	int loops;

	for (loops = 0; loops < 3000; loops++) {
		if (!pgrac_fenced_coordinator_service(scalar, now_ns())
			|| !pgrac_fenced_rejoin_coordinator_service(rejoin, now_ns()))
			return false;
		if (try_rejoin(peer_fd, frame, &used, response))
			return true;
		(void)nanosleep(&pause, NULL);
	}
	return false;
}

static void
make_scalar_request(const PgracFencedConfigV1 *config, uint8 nonce,
					PgracExternalFenceProtocolRequestV1 *request)
{
	memset(request, 0, sizeof(*request));
	memset(request->request_nonce, nonce, sizeof(request->request_nonce));
	request->need.system_identifier = config->system_identifier;
	memset(request->need.canonical_duty_digest, nonce + 1,
		   sizeof(request->need.canonical_duty_digest));
	request->need.victim_node_id = 3;
	request->need.victim_incarnation = 70;
	UT_ASSERT(pgrac_external_fence_protected_set_digest_v1(
		config->storage_backend_id, config->storage_uuid, request->need.protected_set_digest));
	request->need.predicate_id = 1;
	request->need.predicate_version = 1;
	request->timeout_ms = 2000;
}

static bool
write_scalar(int fd, const PgracExternalFenceProtocolRequestV1 *request)
{
	uint8 frame[PGRAC_EXTERNAL_FENCE_REQUEST_V1_BYTES];

	return pgrac_external_fence_request_v1_encode(request, frame)
		   && write(fd, frame, sizeof(frame)) == sizeof(frame);
}

static bool
try_scalar(int fd, PgracExternalFenceProtocolResponseV1 *response)
{
	uint8 frame[PGRAC_EXTERNAL_FENCE_RESPONSE_V1_BYTES];
	ssize_t got = recv(fd, frame, sizeof(frame), MSG_DONTWAIT);

	return got == sizeof(frame)
		   && pgrac_external_fence_response_v1_decode(frame, sizeof(frame), response);
}

static bool
await_scalar(PgracFencedCoordinatorV1 *scalar, int peer_fd,
			 PgracExternalFenceProtocolResponseV1 *response)
{
	struct timespec pause = { 0, 1000000 };
	int loops;

	for (loops = 0; loops < 3000; loops++) {
		if (!pgrac_fenced_coordinator_service(scalar, now_ns()))
			return false;
		if (try_scalar(peer_fd, response))
			return true;
		(void)nanosleep(&pause, NULL);
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
	request->timeout_ms = 2000;
}

UT_TEST(test_socket_rejoin_invalidates_exact_scalar_and_releases_after_close)
{
	PgracFencedOperationContextV1 operation_context;
	PgracFencedJournalScanState journal_state;
	PgracFencedConfigV1 config;
	PgracFencedProviderOpsV1 ops;
	PgracFencedCoordinatorV1 scalar;
	PgracFencedRejoinCoordinatorV1 rejoin;
	PgracExternalFenceProtocolRequestV1 scalar_request;
	PgracExternalFenceProtocolResponseV1 scalar_response;
	PgracExternalFenceProtocolRejoinFrameV1 request;
	PgracExternalFenceProtocolRejoinFrameV1 offer;
	PgracExternalFenceProtocolRejoinFrameV1 response;
	uint8 byte;
	char path[64];
	int journal_fd;
	int admin[2];
	int db[2];
	int old_scalar[2];
	int fresh_scalar[2];

	memset((void *)provider_state, 0, sizeof(uint32) * 4);
	provider_state[2] = PGRAC_FENCED_TARGET_OFF;
	journal_fd = open_context(&operation_context, &journal_state, &config, &ops, path);
	if (journal_fd < 0)
		return;
	UT_ASSERT(pgrac_fenced_coordinator_init(&scalar, &operation_context));
	UT_ASSERT(pgrac_fenced_rejoin_coordinator_init(&rejoin, &operation_context, &scalar));

	memset(&request, 0, sizeof(request));
	request.opcode = PGRAC_EXTERNAL_FENCE_REJOIN_ADMIN_PREPARE;
	memset(request.transport_nonce, 0x61, sizeof(request.transport_nonce));
	request.old_node_id = 3;
	request.old_incarnation = 70;
	request.candidate_incarnation = 77;
	request.timeout_ms = 2000;
	UT_ASSERT_EQ(socketpair(AF_UNIX, SOCK_STREAM, 0, admin), 0);
	UT_ASSERT(write_rejoin(admin[0], &request));
	UT_ASSERT(pgrac_fenced_rejoin_coordinator_accept_fd(&rejoin, admin[1], true,
														deadline_after_ms(3000)));
	UT_ASSERT(await_rejoin(&scalar, &rejoin, admin[0], &response));
	UT_ASSERT_EQ(response.opcode, PGRAC_EXTERNAL_FENCE_REJOIN_ADMIN_PREPARE_RESULT);
	UT_ASSERT_EQ(response.status, PGRAC_FENCED_REJOIN_STATUS_OFFERED);

	make_scalar_request(&config, 0x71, &scalar_request);
	UT_ASSERT_EQ(socketpair(AF_UNIX, SOCK_STREAM, 0, old_scalar), 0);
	UT_ASSERT(write_scalar(old_scalar[0], &scalar_request));
	UT_ASSERT(pgrac_fenced_coordinator_accept_fd(&scalar, old_scalar[1], deadline_after_ms(3000)));
	UT_ASSERT(await_scalar(&scalar, old_scalar[0], &scalar_response));
	UT_ASSERT_EQ(scalar_response.verdict, 1);
	UT_ASSERT_EQ(provider_state[0], 1);

	memset(&request, 0, sizeof(request));
	request.opcode = PGRAC_EXTERNAL_FENCE_REJOIN_LMON_CLAIM_NEXT;
	memset(request.transport_nonce, 0x62, sizeof(request.transport_nonce));
	UT_ASSERT_EQ(socketpair(AF_UNIX, SOCK_STREAM, 0, db), 0);
	UT_ASSERT(write_rejoin(db[0], &request));
	UT_ASSERT(
		pgrac_fenced_rejoin_coordinator_accept_fd(&rejoin, db[1], false, deadline_after_ms(3000)));
	UT_ASSERT(await_rejoin(&scalar, &rejoin, db[0], &offer));
	UT_ASSERT_EQ(offer.status, PGRAC_FENCED_REJOIN_STATUS_OFFERED);
	UT_ASSERT_EQ(recv(old_scalar[0], &byte, 1, MSG_DONTWAIT), -1);
	UT_ASSERT(errno == EAGAIN || errno == EWOULDBLOCK);

	make_bound_request(PGRAC_EXTERNAL_FENCE_REJOIN_LMON_AUTHORIZE_ON, &offer, 0x63, &request);
	UT_ASSERT(write_rejoin(db[0], &request));
	UT_ASSERT(await_rejoin(&scalar, &rejoin, db[0], &response));
	UT_ASSERT_EQ(response.status, PGRAC_FENCED_REJOIN_STATUS_WAITING_JOINER);
	UT_ASSERT_EQ(recv(old_scalar[0], &byte, 1, MSG_DONTWAIT), 0);
	UT_ASSERT_EQ(provider_state[1], 1);

	make_bound_request(PGRAC_EXTERNAL_FENCE_REJOIN_LMON_REFRESH_ON, &response, 0x64, &request);
	UT_ASSERT(write_rejoin(db[0], &request));
	UT_ASSERT(await_rejoin(&scalar, &rejoin, db[0], &response));
	UT_ASSERT_EQ(response.status, PGRAC_FENCED_REJOIN_STATUS_READY);
	(void)close(db[0]);
	UT_ASSERT(pgrac_fenced_rejoin_coordinator_service(&rejoin, now_ns()));

	make_scalar_request(&config, 0x72, &scalar_request);
	UT_ASSERT_EQ(socketpair(AF_UNIX, SOCK_STREAM, 0, fresh_scalar), 0);
	UT_ASSERT(write_scalar(fresh_scalar[0], &scalar_request));
	UT_ASSERT(
		pgrac_fenced_coordinator_accept_fd(&scalar, fresh_scalar[1], deadline_after_ms(3000)));
	UT_ASSERT(await_scalar(&scalar, fresh_scalar[0], &scalar_response));
	UT_ASSERT_EQ(scalar_response.verdict, 1);
	UT_ASSERT_EQ(provider_state[0], 2);
	UT_ASSERT_EQ(provider_state[1], 1);
	UT_ASSERT_EQ(pgrac_fenced_rejoin_coordinator_operation_count(&rejoin), 0);
	UT_ASSERT(pgrac_fenced_rejoin_coordinator_shutdown(&rejoin));
	UT_ASSERT(pgrac_fenced_coordinator_shutdown(&scalar, 16));
	(void)close(admin[0]);
	(void)close(old_scalar[0]);
	(void)close(fresh_scalar[0]);
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
	UT_RUN(test_socket_rejoin_invalidates_exact_scalar_and_releases_after_close);
	UT_DONE();
	(void)munmap((void *)provider_state, sizeof(uint32) * 4);
	return ut_failed_count == 0 ? 0 : 1;
}
