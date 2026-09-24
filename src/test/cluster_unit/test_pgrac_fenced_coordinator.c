/*-------------------------------------------------------------------------
 *
 * test_pgrac_fenced_coordinator.c
 *    Production socket coordinator join/FIFO/concurrency tests.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <fcntl.h>
#include <sys/mman.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <time.h>
#include <unistd.h>

#include "common/pgrac_external_fence_protocol.h"
#include "pgrac_fenced_coordinator.h"

#undef printf
#undef fprintf
#undef snprintf

#include "unit_test.h"
#include "data/pgrac_fence_map_v2_fixture.h"

UT_DEFINE_GLOBALS();

typedef struct SharedProviderState {
	volatile uint32 total_actions;
	volatile uint32 node_actions[PGRAC_FENCED_MAX_NODES];
	volatile uint32 hold_action;
	volatile uint32 release_action;
	volatile uint32 unknown_first_attempt;
} SharedProviderState;

static SharedProviderState *provider_state;

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
	struct timespec pause = { 0, 1000000 };
	uint32 action_number;
	uint32 loops = 0;

	*native_status = 0;
	action_number = __sync_add_and_fetch(&provider_state->total_actions, 1);
	(void)__sync_add_and_fetch(&provider_state->node_actions[target->victim_node_id], 1);
	if (provider_state->hold_action) {
		/* The reload test releases this exact action only after quiescing. */
		while (!provider_state->release_action) {
			struct timespec now;

			if (clock_gettime(CLOCK_MONOTONIC, &now) != 0
				|| (uint64_t)now.tv_sec * UINT64_C(1000000000) + (uint64_t)now.tv_nsec
					   >= deadline_mono_ns)
				return PGRAC_FENCED_PROVIDER_UNKNOWN;
			(void)nanosleep(&pause, NULL);
		}
		return PGRAC_FENCED_PROVIDER_OK;
	}
	while (action_number <= 2 && provider_state->total_actions < 2 && loops++ < 500)
		(void)nanosleep(&pause, NULL);
	return provider_state->total_actions >= 2 ? PGRAC_FENCED_PROVIDER_OK
											  : PGRAC_FENCED_PROVIDER_UNKNOWN;
}

static PgracFencedProviderResult
test_readback(const PgracFencedTargetV1 *target, uint64_t deadline_mono_ns,
			  PgracFencedReadbackV1 *out)
{
	(void)deadline_mono_ns;
	memset(out, 0, sizeof(*out));
	if (provider_state->unknown_first_attempt && provider_state->total_actions == 1)
		return PGRAC_FENCED_PROVIDER_UNKNOWN;
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
	config->node_count = 2;
	config->nodes[3].present = true;
	memset(config->nodes[3].target_uuid, 0x41, sizeof(config->nodes[3].target_uuid));
	config->nodes[4].present = true;
	memset(config->nodes[4].target_uuid, 0x42, sizeof(config->nodes[4].target_uuid));
	memset(ops, 0, sizeof(*ops));
	ops->abi_version = PGRAC_FENCED_PROVIDER_ABI_V1;
	ops->struct_size = sizeof(*ops);
	ops->provider_id = PGRAC_FENCED_PROVIDER_ID_TEST_ONLY;
	ops->provider_name = "coordinator-test";
	ops->resolve = test_resolve;
	ops->actuate_off = test_actuate;
	ops->readback = test_readback;
	ops->actuate_on = test_actuate;
	ops->shutdown = test_shutdown;
	memset(config_digest, 0x71, sizeof(config_digest));
	memset(daemon_boot_id, 0x81, sizeof(daemon_boot_id));
	strcpy(path, "/tmp/pgrac-fenced-coordinator.XXXXXX");
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
make_request(const PgracFencedConfigV1 *config, int32 node_id, uint8 nonce, uint8 duty,
			 PgracExternalFenceProtocolRequestV1 *request)
{
	memset(request, 0, sizeof(*request));
	memset(request->request_nonce, nonce, sizeof(request->request_nonce));
	request->need.system_identifier = config->system_identifier;
	memset(request->need.canonical_duty_digest, duty, sizeof(request->need.canonical_duty_digest));
	request->need.victim_node_id = node_id;
	request->need.victim_incarnation = (uint64)duty + 20;
	UT_ASSERT(pgrac_external_fence_protected_set_digest_v1(
		config->storage_backend_id, config->storage_uuid, request->need.protected_set_digest));
	request->need.predicate_id = 1;
	request->need.predicate_version = 1;
	request->timeout_ms = 2000;
}

static bool
write_request(int fd, const PgracExternalFenceProtocolRequestV1 *request)
{
	uint8 frame[PGRAC_EXTERNAL_FENCE_REQUEST_V1_BYTES];

	return pgrac_external_fence_request_v1_encode(request, frame)
		   && write(fd, frame, sizeof(frame)) == sizeof(frame);
}

static bool
try_response(int fd, PgracExternalFenceProtocolResponseV1 *response)
{
	uint8 frame[PGRAC_EXTERNAL_FENCE_RESPONSE_V1_BYTES];
	ssize_t got;

	do {
		got = recv(fd, frame, sizeof(frame), MSG_DONTWAIT);
	} while (got < 0 && errno == EINTR);
	if (got < 0 && (errno == EAGAIN || errno == EWOULDBLOCK))
		return false;
	UT_ASSERT_EQ(got, sizeof(frame));
	return got == sizeof(frame)
		   && pgrac_external_fence_response_v1_decode(frame, sizeof(frame), response);
}

static size_t
journal_record_count(int fd)
{
	struct stat st;

	UT_ASSERT_EQ(fstat(fd, &st), 0);
	return (size_t)st.st_size / PGRAC_FENCED_JOURNAL_RECORD_BYTES;
}

/* PGRAC: actual signed-map parser, disk journal, sockets and forked worker. */
static int
open_owned_context(PgracFencedOperationContextV1 *context, PgracFencedJournalScanState *journal,
				   PgracFencedConfigV1 *config, PgracFencedProviderOpsV1 *ops, char path[64])
{
	uint8 digest[32], boot[16];
	int fd = open_context(context, journal, config, ops, path);

	if (fd < 0)
		return fd;
	memcpy(digest, context->semantic_config_digest, sizeof(digest));
	memcpy(boot, context->daemon_boot_id, sizeof(boot));
	UT_ASSERT_EQ(pgrac_fenced_config_parse((const uint8 *)fenced_config_v2,
										   sizeof(fenced_config_v2) - 1, config),
				 PGRAC_FENCED_CONFIG_OK);
	config->allowed_db_uid = (uint64)geteuid();
	config->allowed_db_gid = (uint64)getegid();
	ops->provider_id = PGRAC_FENCED_PROVIDER_ID_PACEMAKER_LIBVIRT_V1;
	UT_ASSERT(
		pgrac_fenced_operation_context_init(context, config, ops, true, digest, boot, fd, journal));
	provider_state
		= mmap(NULL, sizeof(*provider_state), PROT_READ | PROT_WRITE, MAP_SHARED | MAP_ANON, -1, 0);
	UT_ASSERT(provider_state != MAP_FAILED);
	memset(provider_state, 0, sizeof(*provider_state));
	provider_state->hold_action = 1;
	return fd;
}

static void
make_owned_request(const PgracFencedConfigV1 *config, uint8 nonce, uint8 duty,
				   PgracExternalFenceProtocolRequestV1 *request)
{
	make_request(config, 2, nonce, duty, request);
	memcpy(request->need.protected_set_digest, config->nodes[2].protected_set_digest, 32);
}

static uint32
scan_owned_journal(int fd, PgracFencedJournalReconcileState *pending,
				   PgracFencedJournalRecordV1 *accepted)
{
	uint8 frame[PGRAC_FENCED_JOURNAL_MAX_RECORD_BYTES];
	PgracFencedJournalRecordV1 record;
	size_t length;
	off_t offset = 0;
	uint32 count = 0;

	pgrac_fenced_journal_reconcile_state_init(pending);
	while (pread(fd, frame, 16, offset) == 16) {
		length = pgrac_fenced_journal_frame_size(frame, 16);
		UT_ASSERT(length > 0 && length <= sizeof(frame));
		if (length == 0 || length > sizeof(frame))
			break;
		UT_ASSERT_EQ(pread(fd, frame, length, offset), length);
		UT_ASSERT(pgrac_fenced_journal_record_decode(frame, length, &record));
		UT_ASSERT(pgrac_fenced_journal_reconcile_observe(pending, &record));
		if (record.intent.kind == PGRAC_FENCED_JOURNAL_INTENT_ACQUIRE
			&& record.record_kind == PGRAC_FENCED_JOURNAL_KIND_REQUEST_ACCEPTED) {
			count++;
			*accepted = record;
		}
		offset += length;
	}
	return count;
}

static void
owned_service_step(PgracFencedCoordinatorV1 *coordinator)
{
	struct timespec pause = { 0, 1000000 };

	UT_ASSERT(pgrac_fenced_coordinator_service(coordinator, deadline_after_ms(0)));
	(void)nanosleep(&pause, NULL);
}

UT_TEST(test_owned_join_disconnect_and_rejoin_wait_for_background_off)
{
	PgracFencedOperationContextV1 context;
	PgracFencedJournalScanState journal;
	PgracFencedJournalReconcileState pending;
	PgracFencedJournalRecordV1 accepted;
	PgracFencedConfigV1 config;
	PgracFencedProviderOpsV1 ops;
	PgracFencedCoordinatorV1 coordinator;
	PgracExternalFenceProtocolRequestV1 request;
	int sockets[2][2], fd, loops;
	char path[64];

#ifndef USE_OPENSSL
	return;
#endif
	fd = open_owned_context(&context, &journal, &config, &ops, path);
	UT_ASSERT(pgrac_fenced_coordinator_init(&coordinator, &context));
	for (int i = 0; i < 2; i++) {
		make_owned_request(&config, 0x51 + i, 0x61, &request);
		UT_ASSERT_EQ(socketpair(AF_UNIX, SOCK_STREAM, 0, sockets[i]), 0);
		UT_ASSERT(write_request(sockets[i][0], &request));
		UT_ASSERT(pgrac_fenced_coordinator_accept_fd(&coordinator, sockets[i][1],
													 deadline_after_ms(2000)));
	}
	UT_ASSERT_EQ(scan_owned_journal(fd, &pending, &accepted), 1);
	UT_ASSERT_EQ(pending.pending_count, 1);
	for (loops = 0; loops < 1000 && provider_state->total_actions == 0; loops++)
		owned_service_step(&coordinator);
	UT_ASSERT_EQ(provider_state->total_actions, 1);
	(void)close(sockets[0][0]);
	(void)close(sockets[1][0]);
	owned_service_step(&coordinator);
	UT_ASSERT_EQ(coordinator.client_count, 0);
	UT_ASSERT_EQ(
		pgrac_fenced_coordinator_rejoin_acquire_target(&coordinator, config.nodes[2].target_uuid),
		PGRAC_FENCED_REJOIN_TARGET_WAITING);
	UT_ASSERT_EQ(pgrac_fenced_coordinator_rejoin_invalidate_target(&coordinator,
																   config.nodes[2].target_uuid, 19),
				 PGRAC_FENCED_REJOIN_TARGET_WAITING);
	provider_state->release_action = 1;
	for (loops = 0; loops < 2000 && pgrac_fenced_schedule_operation_count(&coordinator.schedule);
		 loops++)
		owned_service_step(&coordinator);
	UT_ASSERT_EQ(pgrac_fenced_schedule_operation_count(&coordinator.schedule), 0);
	UT_ASSERT_EQ(scan_owned_journal(fd, &pending, &accepted), 1);
	UT_ASSERT_EQ(pending.pending_count, 0);
	UT_ASSERT_EQ(provider_state->total_actions, 1);
	UT_ASSERT_EQ(
		pgrac_fenced_coordinator_rejoin_acquire_target(&coordinator, config.nodes[2].target_uuid),
		PGRAC_FENCED_REJOIN_TARGET_READY);
	UT_ASSERT(pgrac_fenced_coordinator_shutdown(&coordinator, 16));
	(void)munmap(provider_state, sizeof(*provider_state));
	(void)close(fd);
	(void)unlink(path);
}

UT_TEST(test_owned_unknown_redrives_without_caller_and_preserves_uuid)
{
	PgracFencedOperationContextV1 context;
	PgracFencedJournalScanState journal;
	PgracFencedJournalReconcileState pending;
	PgracFencedJournalRecordV1 first, last;
	PgracFencedConfigV1 config;
	PgracFencedProviderOpsV1 ops;
	PgracFencedCoordinatorV1 coordinator;
	PgracExternalFenceProtocolRequestV1 request;
	int sockets[2], fd, loops;
	char path[64];

#ifndef USE_OPENSSL
	return;
#endif
	fd = open_owned_context(&context, &journal, &config, &ops, path);
	provider_state->release_action = 1;
	provider_state->unknown_first_attempt = 1;
	UT_ASSERT(pgrac_fenced_coordinator_init(&coordinator, &context));
	make_owned_request(&config, 0x53, 0x63, &request);
	request.timeout_ms = 100;
	UT_ASSERT_EQ(socketpair(AF_UNIX, SOCK_STREAM, 0, sockets), 0);
	UT_ASSERT(write_request(sockets[0], &request));
	UT_ASSERT(
		pgrac_fenced_coordinator_accept_fd(&coordinator, sockets[1], deadline_after_ms(1000)));
	UT_ASSERT_EQ(scan_owned_journal(fd, &pending, &first), 1);
	(void)close(sockets[0]);
	for (loops = 0; loops < 2000 && pgrac_fenced_schedule_operation_count(&coordinator.schedule);
		 loops++)
		owned_service_step(&coordinator);
	UT_ASSERT_EQ(coordinator.client_count, 0);
	UT_ASSERT_EQ(provider_state->total_actions, 2);
	UT_ASSERT_EQ(scan_owned_journal(fd, &pending, &last), 2);
	UT_ASSERT_EQ(pending.pending_count, 0);
	UT_ASSERT_EQ(last.intent.attempt, 2);
	UT_ASSERT(memcmp(first.operation_id, last.operation_id, 16) == 0);
	UT_ASSERT(pgrac_fenced_coordinator_shutdown(&coordinator, 16));
	(void)munmap(provider_state, sizeof(*provider_state));
	(void)close(fd);
	(void)unlink(path);
}

UT_TEST(test_owned_joiners_share_one_proof_without_new_obligations)
{
	PgracFencedOperationContextV1 context;
	PgracFencedJournalScanState journal;
	PgracFencedJournalReconcileState pending;
	PgracFencedJournalRecordV1 accepted;
	PgracFencedConfigV1 config;
	PgracFencedProviderOpsV1 ops;
	PgracFencedCoordinatorV1 coordinator;
	PgracExternalFenceProtocolRequestV1 requests[2];
	PgracExternalFenceProtocolResponseV1 responses[2];
	bool received[2] = { false, false };
	int sockets[2][2], fd, loops;
	char path[64];

#ifndef USE_OPENSSL
	return;
#endif
	fd = open_owned_context(&context, &journal, &config, &ops, path);
	UT_ASSERT(pgrac_fenced_coordinator_init(&coordinator, &context));
	for (int i = 0; i < 2; i++) {
		make_owned_request(&config, 0x57 + i, 0x67, &requests[i]);
		UT_ASSERT_EQ(socketpair(AF_UNIX, SOCK_STREAM, 0, sockets[i]), 0);
		UT_ASSERT(write_request(sockets[i][0], &requests[i]));
		UT_ASSERT(pgrac_fenced_coordinator_accept_fd(&coordinator, sockets[i][1],
													 deadline_after_ms(2000)));
	}
	provider_state->release_action = 1;
	for (loops = 0; loops < 2000 && (!received[0] || !received[1]); loops++) {
		owned_service_step(&coordinator);
		for (int i = 0; i < 2; i++)
			if (!received[i])
				received[i] = try_response(sockets[i][0], &responses[i]);
	}
	for (int i = 0; i < 2; i++) {
		UT_ASSERT(received[i]);
		UT_ASSERT_EQ(responses[i].verdict, 1);
		UT_ASSERT(memcmp(responses[i].request_nonce, requests[i].request_nonce, 16) == 0);
		(void)close(sockets[i][0]);
	}
	UT_ASSERT_EQ(responses[0].journal_seq, responses[1].journal_seq);
	UT_ASSERT_EQ(responses[0].proof_generation, responses[1].proof_generation);
	UT_ASSERT_EQ(scan_owned_journal(fd, &pending, &accepted), 1);
	UT_ASSERT_EQ(pending.pending_count, 0);
	UT_ASSERT_EQ(provider_state->total_actions, 1);
	owned_service_step(&coordinator);
	UT_ASSERT(pgrac_fenced_coordinator_shutdown(&coordinator, 16));
	(void)munmap(provider_state, sizeof(*provider_state));
	(void)close(fd);
	(void)unlink(path);
}

UT_TEST(test_owned_replay_restores_original_fifo_without_clients_or_old_clock)
{
	PgracFencedOperationContextV1 context;
	PgracFencedJournalScanState journal;
	PgracFencedJournalReconcileState pending;
	PgracFencedJournalRecordV1 last, issued;
	PgracFencedPreparedAcquireV1 prepared[2];
	PgracFencedConfigV1 config;
	PgracFencedProviderOpsV1 ops;
	PgracFencedCoordinatorV1 coordinator;
	PgracExternalFenceProtocolRequestV1 request;
	PgracExternalFenceProtocolResponseV1 response;
	int fd, loops;
	char path[64];
	bool restored;

#ifndef USE_OPENSSL
	return;
#endif
	fd = open_owned_context(&context, &journal, &config, &ops, path);
	for (int i = 0; i < 2; i++) {
		make_owned_request(&config, 0x70 + i, 0x80 + i, &request);
		UT_ASSERT_EQ(pgrac_fenced_operation_accept(&context, &request, deadline_after_ms(1000),
												   &prepared[i], &response),
					 PGRAC_FENCED_OPERATION_READY);
	}
	/* First operation's later event must not put it behind the second accept. */
	issued = prepared[0].accepted_record;
	issued.record_kind = PGRAC_FENCED_JOURNAL_KIND_ACTUATION_ISSUED;
	UT_ASSERT(pgrac_fenced_operation_append_journal(&context, &issued));
	UT_ASSERT_EQ(scan_owned_journal(fd, &pending, &last), 2);
	UT_ASSERT_EQ(pending.pending[0].first_seq, prepared[0].accepted_journal_seq);
	context.daemon_boot_id[0] ^= 1;
	UT_ASSERT(pgrac_fenced_operation_reconcile_startup(&context, &pending));
	UT_ASSERT(pgrac_fenced_coordinator_init(&coordinator, &context));
	restored = pgrac_fenced_coordinator_restore(&coordinator, &pending);
	UT_ASSERT(restored);
	if (restored) {
		UT_ASSERT_EQ(pending.pending_count, 0);
		UT_ASSERT_EQ(coordinator.client_count, 0);
		UT_ASSERT_EQ(provider_state->total_actions, 0);
		UT_ASSERT_EQ(pgrac_fenced_schedule_operation_count(&coordinator.schedule), 2);
		for (loops = 0; loops < 1000 && provider_state->total_actions == 0; loops++)
			owned_service_step(&coordinator);
		UT_ASSERT_EQ(provider_state->total_actions, 1);
		UT_ASSERT_EQ(scan_owned_journal(fd, &pending, &last), 3);
		UT_ASSERT(memcmp(last.operation_id, prepared[0].accepted_record.operation_id, 16) == 0);
		UT_ASSERT_EQ(last.intent.attempt, 2);
		provider_state->release_action = 1;
		for (loops = 0;
			 loops < 2000 && pgrac_fenced_schedule_operation_count(&coordinator.schedule); loops++)
			owned_service_step(&coordinator);
		UT_ASSERT_EQ(provider_state->total_actions, 2);
		UT_ASSERT_EQ(scan_owned_journal(fd, &pending, &last), 4);
		UT_ASSERT_EQ(pending.pending_count, 0);
		UT_ASSERT(memcmp(last.operation_id, prepared[1].accepted_record.operation_id, 16) == 0);
		UT_ASSERT_EQ(last.intent.attempt, 2);
	}
	UT_ASSERT(pgrac_fenced_coordinator_shutdown(&coordinator, 16));
	(void)munmap(provider_state, sizeof(*provider_state));
	(void)close(fd);
	(void)unlink(path);
}

UT_TEST(test_owned_quiesce_keeps_work_and_refuses_mapping_reload)
{
	PgracFencedOperationContextV1 context;
	PgracFencedJournalScanState journal;
	PgracFencedJournalReconcileState pending;
	PgracFencedJournalRecordV1 last;
	PgracFencedPreparedAcquireV1 prepared;
	PgracFencedConfigV1 config, candidate;
	PgracFencedProviderOpsV1 ops;
	PgracFencedCoordinatorV1 coordinator;
	PgracExternalFenceProtocolRequestV1 request;
	PgracExternalFenceProtocolResponseV1 response;
	struct stat before, after;
	uint8 digest[32];
	int fd;
	char path[64];

#ifndef USE_OPENSSL
	return;
#endif
	fd = open_owned_context(&context, &journal, &config, &ops, path);
	make_owned_request(&config, 0x79, 0x89, &request);
	UT_ASSERT_EQ(pgrac_fenced_operation_accept(&context, &request, deadline_after_ms(1000),
											   &prepared, &response),
				 PGRAC_FENCED_OPERATION_READY);
	UT_ASSERT_EQ(scan_owned_journal(fd, &pending, &last), 1);
	UT_ASSERT(pgrac_fenced_coordinator_init(&coordinator, &context));
	/* Install the replayed obligation, but do not start a worker. */
	UT_ASSERT(pgrac_fenced_coordinator_restore(&coordinator, &pending));
	UT_ASSERT(pgrac_fenced_coordinator_quiesce(&coordinator, 17));
	candidate = config;
	candidate.mapping_generation++;
	memset(digest, 0x92, sizeof(digest));
	UT_ASSERT_EQ(fstat(fd, &before), 0);
	UT_ASSERT(
		!pgrac_fenced_coordinator_prepare_mapping_reload(&coordinator, &candidate, &ops, digest));
	UT_ASSERT(context.pending_config == NULL);
	UT_ASSERT_EQ(fstat(fd, &after), 0);
	UT_ASSERT_EQ(before.st_size, after.st_size);
	UT_ASSERT_EQ(provider_state->total_actions, 0);
	UT_ASSERT(pgrac_fenced_coordinator_shutdown(&coordinator, 16));
	UT_ASSERT_EQ(scan_owned_journal(fd, &pending, &last), 1);
	UT_ASSERT_EQ(pending.pending_count, 1);
	(void)munmap(provider_state, sizeof(*provider_state));
	(void)close(fd);
	(void)unlink(path);
}

UT_TEST(test_owned_expired_queue_and_reused_client_slot_do_not_lose_or_steal_work)
{
	PgracFencedOperationContextV1 context;
	PgracFencedJournalScanState journal;
	PgracFencedJournalReconcileState pending;
	PgracFencedJournalRecordV1 accepted;
	PgracFencedConfigV1 config;
	PgracFencedProviderOpsV1 ops;
	PgracFencedCoordinatorV1 coordinator;
	PgracExternalFenceProtocolRequestV1 requests[3];
	PgracExternalFenceProtocolResponseV1 response;
	struct timespec expire = { 0, 30000000 };
	int sockets[3][2], fd, loops;
	char path[64];
	bool received = false;

#ifndef USE_OPENSSL
	return;
#endif
	fd = open_owned_context(&context, &journal, &config, &ops, path);
	UT_ASSERT(pgrac_fenced_coordinator_init(&coordinator, &context));
	for (int i = 0; i < 2; i++) {
		make_owned_request(&config, 0x91 + i, 0xa1 + i, &requests[i]);
		UT_ASSERT_EQ(socketpair(AF_UNIX, SOCK_STREAM, 0, sockets[i]), 0);
		UT_ASSERT(write_request(sockets[i][0], &requests[i]));
		UT_ASSERT(pgrac_fenced_coordinator_accept_fd(&coordinator, sockets[i][1],
													 deadline_after_ms(i == 0 ? 2000 : 20)));
	}
	(void)close(sockets[0][0]);
	(void)nanosleep(&expire, NULL);
	owned_service_step(&coordinator);
	UT_ASSERT(try_response(sockets[1][0], &response));
	UT_ASSERT_EQ(response.deny_reason, 11);
	UT_ASSERT_EQ(coordinator.client_count, 0);
	UT_ASSERT_EQ(pgrac_fenced_schedule_operation_count(&coordinator.schedule), 2);
	make_owned_request(&config, 0x93, 0xa3, &requests[2]);
	UT_ASSERT_EQ(socketpair(AF_UNIX, SOCK_STREAM, 0, sockets[2]), 0);
	UT_ASSERT(write_request(sockets[2][0], &requests[2]));
	UT_ASSERT(
		pgrac_fenced_coordinator_accept_fd(&coordinator, sockets[2][1], deadline_after_ms(2000)));
	provider_state->release_action = 1;
	for (loops = 0; loops < 2000 && !received; loops++) {
		owned_service_step(&coordinator);
		received = try_response(sockets[2][0], &response);
	}
	UT_ASSERT(received);
	UT_ASSERT_EQ(response.verdict, 1);
	UT_ASSERT_EQ(response.binding.victim_incarnation, requests[2].need.victim_incarnation);
	UT_ASSERT(memcmp(response.request_nonce, requests[2].request_nonce, 16) == 0);
	UT_ASSERT_EQ(provider_state->total_actions, 3);
	UT_ASSERT_EQ(scan_owned_journal(fd, &pending, &accepted), 3);
	UT_ASSERT_EQ(pending.pending_count, 0);
	(void)close(sockets[1][0]);
	(void)close(sockets[2][0]);
	owned_service_step(&coordinator);
	UT_ASSERT(pgrac_fenced_coordinator_shutdown(&coordinator, 16));
	(void)munmap(provider_state, sizeof(*provider_state));
	(void)close(fd);
	(void)unlink(path);
}

UT_TEST(test_socket_join_fifo_and_different_target_concurrency)
{
	PgracFencedOperationContextV1 context;
	PgracFencedJournalScanState journal_state;
	PgracFencedProviderOpsV1 ops;
	PgracFencedConfigV1 config;
	PgracFencedCoordinatorV1 coordinator;
	PgracExternalFenceProtocolRequestV1 requests[4];
	PgracExternalFenceProtocolResponseV1 responses[4] = { 0 };
	bool received[4] = { false, false, false, false };
	struct timespec pause = { 0, 1000000 };
	int sockets[4][2];
	char path[64];
	int journal_fd;
	int loops;
	int i;

	journal_fd = open_context(&context, &journal_state, &config, &ops, path);
	if (journal_fd < 0)
		return;
	provider_state
		= mmap(NULL, sizeof(*provider_state), PROT_READ | PROT_WRITE, MAP_SHARED | MAP_ANON, -1, 0);
	UT_ASSERT(provider_state != MAP_FAILED);
	if (provider_state == MAP_FAILED)
		return;
	memset(provider_state, 0, sizeof(*provider_state));
	UT_ASSERT(pgrac_fenced_coordinator_init(&coordinator, &context));
	make_request(&config, 3, 0x51, 0x61, &requests[0]);
	requests[1] = requests[0];
	memset(requests[1].request_nonce, 0x52, sizeof(requests[1].request_nonce));
	make_request(&config, 3, 0x53, 0x62, &requests[2]);
	make_request(&config, 4, 0x54, 0x63, &requests[3]);
	for (i = 0; i < 4; i++) {
		UT_ASSERT_EQ(socketpair(AF_UNIX, SOCK_STREAM, 0, sockets[i]), 0);
		UT_ASSERT(write_request(sockets[i][0], &requests[i]));
		UT_ASSERT(pgrac_fenced_coordinator_accept_fd(&coordinator, sockets[i][1],
													 deadline_after_ms(2000)));
	}
	UT_ASSERT(!pgrac_fenced_coordinator_shutdown(&coordinator, 16));
	for (loops = 0; loops < 2000 && (!received[0] || !received[1] || !received[3]); loops++) {
		UT_ASSERT(pgrac_fenced_coordinator_service(&coordinator, deadline_after_ms(0)));
		for (i = 0; i < 4; i++) {
			if (!received[i])
				received[i] = try_response(sockets[i][0], &responses[i]);
		}
		(void)nanosleep(&pause, NULL);
	}
	UT_ASSERT(received[0]);
	UT_ASSERT(received[1]);
	UT_ASSERT(!received[2]);
	UT_ASSERT(received[3]);
	UT_ASSERT_EQ(responses[0].verdict, 1);
	UT_ASSERT_EQ(responses[1].verdict, 1);
	UT_ASSERT_EQ(responses[3].verdict, 1);
	UT_ASSERT_EQ(responses[0].proof_generation, responses[1].proof_generation);
	UT_ASSERT(memcmp(responses[0].target_state_digest, responses[1].target_state_digest,
					 sizeof(responses[0].target_state_digest))
			  == 0);
	UT_ASSERT_EQ(responses[0].verified_mono_ns, responses[1].verified_mono_ns);
	UT_ASSERT_EQ(close(sockets[0][0]), 0);
	UT_ASSERT_EQ(close(sockets[1][0]), 0);
	for (loops = 0; loops < 2000 && !received[2]; loops++) {
		UT_ASSERT(pgrac_fenced_coordinator_service(&coordinator, deadline_after_ms(0)));
		received[2] = try_response(sockets[2][0], &responses[2]);
		(void)nanosleep(&pause, NULL);
	}
	UT_ASSERT(received[2]);
	UT_ASSERT_EQ(responses[2].verdict, 1);
	UT_ASSERT_EQ(provider_state->node_actions[3], 2);
	UT_ASSERT_EQ(provider_state->node_actions[4], 1);
	UT_ASSERT_EQ(journal_record_count(journal_fd), 20);
	UT_ASSERT(pgrac_fenced_coordinator_shutdown(&coordinator, 16));
	(void)close(sockets[2][0]);
	(void)close(sockets[3][0]);
	UT_ASSERT_EQ(munmap(provider_state, sizeof(*provider_state)), 0);
	(void)close(journal_fd);
	(void)unlink(path);
}

UT_TEST(test_queued_deadline_is_durably_invalidated_without_action)
{
	PgracFencedOperationContextV1 context;
	PgracFencedJournalScanState journal_state;
	PgracFencedProviderOpsV1 ops;
	PgracFencedConfigV1 config;
	PgracFencedCoordinatorV1 coordinator;
	PgracExternalFenceProtocolRequestV1 owner_request;
	PgracExternalFenceProtocolRequestV1 queued_request;
	PgracExternalFenceProtocolResponseV1 owner_response = { 0 };
	PgracExternalFenceProtocolResponseV1 queued_response = { 0 };
	struct timespec pause = { 0, 1000000 };
	struct timespec expire = { 0, 30000000 };
	int owner_sockets[2];
	int queued_sockets[2];
	char path[64];
	int journal_fd;
	int loops;
	bool received = false;

	journal_fd = open_context(&context, &journal_state, &config, &ops, path);
	if (journal_fd < 0)
		return;
	provider_state
		= mmap(NULL, sizeof(*provider_state), PROT_READ | PROT_WRITE, MAP_SHARED | MAP_ANON, -1, 0);
	UT_ASSERT(provider_state != MAP_FAILED);
	if (provider_state == MAP_FAILED)
		return;
	memset(provider_state, 0, sizeof(*provider_state));
	provider_state->total_actions = 2;
	UT_ASSERT(pgrac_fenced_coordinator_init(&coordinator, &context));
	make_request(&config, 3, 0x71, 0x72, &owner_request);
	make_request(&config, 3, 0x73, 0x74, &queued_request);
	queued_request.timeout_ms = 20;
	UT_ASSERT_EQ(socketpair(AF_UNIX, SOCK_STREAM, 0, owner_sockets), 0);
	UT_ASSERT(write_request(owner_sockets[0], &owner_request));
	UT_ASSERT(pgrac_fenced_coordinator_accept_fd(&coordinator, owner_sockets[1],
												 deadline_after_ms(2000)));
	for (loops = 0; loops < 1000 && !received; loops++) {
		UT_ASSERT(pgrac_fenced_coordinator_service(&coordinator, deadline_after_ms(0)));
		received = try_response(owner_sockets[0], &owner_response);
		(void)nanosleep(&pause, NULL);
	}
	UT_ASSERT(received);
	UT_ASSERT_EQ(owner_response.verdict, 1);
	UT_ASSERT_EQ(socketpair(AF_UNIX, SOCK_STREAM, 0, queued_sockets), 0);
	UT_ASSERT(write_request(queued_sockets[0], &queued_request));
	UT_ASSERT(pgrac_fenced_coordinator_accept_fd(&coordinator, queued_sockets[1],
												 deadline_after_ms(2000)));
	(void)nanosleep(&expire, NULL);
	UT_ASSERT(pgrac_fenced_coordinator_service(&coordinator, deadline_after_ms(0)));
	UT_ASSERT(try_response(queued_sockets[0], &queued_response));
	UT_ASSERT_EQ(queued_response.verdict, 4);
	UT_ASSERT_EQ(queued_response.deny_reason, 11);
	UT_ASSERT_EQ(provider_state->node_actions[3], 1);
	UT_ASSERT_EQ(journal_record_count(journal_fd), 8);
	UT_ASSERT(pgrac_fenced_coordinator_shutdown(&coordinator, 16));
	(void)close(owner_sockets[0]);
	(void)close(queued_sockets[0]);
	UT_ASSERT_EQ(munmap(provider_state, sizeof(*provider_state)), 0);
	(void)close(journal_fd);
	(void)unlink(path);
}

UT_TEST(test_partial_ingress_never_blocks_other_clients_or_workers)
{
	PgracFencedOperationContextV1 context;
	PgracFencedJournalScanState journal_state;
	PgracFencedProviderOpsV1 ops;
	PgracFencedConfigV1 config;
	PgracFencedCoordinatorV1 coordinator;
	PgracExternalFenceProtocolRequestV1 request;
	PgracExternalFenceProtocolResponseV1 response = { 0 };
	uint8 frame[PGRAC_EXTERNAL_FENCE_REQUEST_V1_BYTES];
	struct timespec pause = { 0, 1000000 };
	struct timespec expire = { 0, 120000000 };
	int partial_sockets[2];
	int full_sockets[2];
	char path[64];
	int journal_fd;
	int loops;
	uint64 before;
	uint64 after;
	bool received = false;
	uint8 byte;

	journal_fd = open_context(&context, &journal_state, &config, &ops, path);
	if (journal_fd < 0)
		return;
	provider_state
		= mmap(NULL, sizeof(*provider_state), PROT_READ | PROT_WRITE, MAP_SHARED | MAP_ANON, -1, 0);
	UT_ASSERT(provider_state != MAP_FAILED);
	if (provider_state == MAP_FAILED)
		return;
	memset(provider_state, 0, sizeof(*provider_state));
	provider_state->total_actions = 2;
	UT_ASSERT(pgrac_fenced_coordinator_init(&coordinator, &context));
	make_request(&config, 3, 0x81, 0x82, &request);
	UT_ASSERT(pgrac_external_fence_request_v1_encode(&request, frame));
	UT_ASSERT_EQ(socketpair(AF_UNIX, SOCK_STREAM, 0, partial_sockets), 0);
	UT_ASSERT_EQ(write(partial_sockets[0], frame, 1), 1);
	before = deadline_after_ms(0);
	UT_ASSERT(pgrac_fenced_coordinator_accept_fd(&coordinator, partial_sockets[1],
												 deadline_after_ms(100)));
	after = deadline_after_ms(0);
	UT_ASSERT(after >= before && after - before < UINT64_C(50000000));

	UT_ASSERT_EQ(socketpair(AF_UNIX, SOCK_STREAM, 0, full_sockets), 0);
	UT_ASSERT(write_request(full_sockets[0], &request));
	UT_ASSERT(
		pgrac_fenced_coordinator_accept_fd(&coordinator, full_sockets[1], deadline_after_ms(2000)));
	for (loops = 0; loops < 2000 && !received; loops++) {
		UT_ASSERT(pgrac_fenced_coordinator_service(&coordinator, deadline_after_ms(0)));
		received = try_response(full_sockets[0], &response);
		(void)nanosleep(&pause, NULL);
	}
	UT_ASSERT(received);
	UT_ASSERT_EQ(response.verdict, 1);
	UT_ASSERT_EQ(journal_record_count(journal_fd), 6);
	(void)nanosleep(&expire, NULL);
	UT_ASSERT(pgrac_fenced_coordinator_service(&coordinator, deadline_after_ms(0)));
	UT_ASSERT_EQ(recv(partial_sockets[0], &byte, 1, MSG_DONTWAIT), 0);
	(void)close(full_sockets[0]);
	UT_ASSERT(pgrac_fenced_coordinator_service(&coordinator, deadline_after_ms(0)));
	UT_ASSERT(pgrac_fenced_coordinator_shutdown(&coordinator, 16));
	(void)close(partial_sockets[0]);
	UT_ASSERT_EQ(munmap(provider_state, sizeof(*provider_state)), 0);
	(void)close(journal_fd);
	(void)unlink(path);
}

UT_TEST(test_clean_shutdown_durably_invalidates_accepted_queue)
{
	PgracFencedOperationContextV1 context;
	PgracFencedJournalScanState journal_state;
	PgracFencedProviderOpsV1 ops;
	PgracFencedConfigV1 config;
	PgracFencedCoordinatorV1 coordinator;
	PgracExternalFenceProtocolRequestV1 owner_request;
	PgracExternalFenceProtocolRequestV1 queued_request;
	PgracExternalFenceProtocolResponseV1 owner_response = { 0 };
	PgracExternalFenceProtocolResponseV1 queued_response = { 0 };
	struct timespec pause = { 0, 1000000 };
	int owner_sockets[2];
	int queued_sockets[2];
	char path[64];
	int journal_fd;
	int loops;
	bool received = false;

	journal_fd = open_context(&context, &journal_state, &config, &ops, path);
	if (journal_fd < 0)
		return;
	provider_state
		= mmap(NULL, sizeof(*provider_state), PROT_READ | PROT_WRITE, MAP_SHARED | MAP_ANON, -1, 0);
	UT_ASSERT(provider_state != MAP_FAILED);
	if (provider_state == MAP_FAILED)
		return;
	memset(provider_state, 0, sizeof(*provider_state));
	provider_state->total_actions = 2;
	UT_ASSERT(pgrac_fenced_coordinator_init(&coordinator, &context));
	make_request(&config, 3, 0x91, 0x92, &owner_request);
	make_request(&config, 3, 0x93, 0x94, &queued_request);
	UT_ASSERT_EQ(socketpair(AF_UNIX, SOCK_STREAM, 0, owner_sockets), 0);
	UT_ASSERT(write_request(owner_sockets[0], &owner_request));
	UT_ASSERT(pgrac_fenced_coordinator_accept_fd(&coordinator, owner_sockets[1],
												 deadline_after_ms(2000)));
	for (loops = 0; loops < 2000 && !received; loops++) {
		UT_ASSERT(pgrac_fenced_coordinator_service(&coordinator, deadline_after_ms(0)));
		received = try_response(owner_sockets[0], &owner_response);
		(void)nanosleep(&pause, NULL);
	}
	UT_ASSERT(received);
	UT_ASSERT_EQ(owner_response.verdict, 1);
	UT_ASSERT_EQ(socketpair(AF_UNIX, SOCK_STREAM, 0, queued_sockets), 0);
	UT_ASSERT(write_request(queued_sockets[0], &queued_request));
	UT_ASSERT(pgrac_fenced_coordinator_accept_fd(&coordinator, queued_sockets[1],
												 deadline_after_ms(2000)));
	UT_ASSERT_EQ(provider_state->node_actions[3], 1);
	UT_ASSERT_EQ(journal_record_count(journal_fd), 7);
	UT_ASSERT(pgrac_fenced_coordinator_shutdown(&coordinator, 16));
	UT_ASSERT(try_response(queued_sockets[0], &queued_response));
	UT_ASSERT_EQ(queued_response.verdict, 4);
	UT_ASSERT_EQ(queued_response.deny_reason, 16);
	UT_ASSERT_EQ(queued_response.provider_result, PGRAC_FENCED_PROVIDER_UNAVAILABLE);
	UT_ASSERT_EQ(provider_state->node_actions[3], 1);
	UT_ASSERT_EQ(journal_record_count(journal_fd), 9);
	(void)close(owner_sockets[0]);
	(void)close(queued_sockets[0]);
	UT_ASSERT_EQ(munmap(provider_state, sizeof(*provider_state)), 0);
	(void)close(journal_fd);
	(void)unlink(path);
}

UT_TEST(test_mapping_reload_closes_old_admissions_before_new_mapping)
{
	PgracFencedOperationContextV1 context;
	PgracFencedJournalScanState journal_state;
	PgracFencedProviderOpsV1 ops;
	PgracFencedConfigV1 config;
	PgracFencedConfigV1 candidate;
	PgracFencedCoordinatorV1 coordinator;
	PgracExternalFenceProtocolRequestV1 owner_request;
	PgracExternalFenceProtocolRequestV1 queued_request;
	PgracExternalFenceProtocolRequestV1 fresh_request;
	PgracExternalFenceProtocolResponseV1 response = { 0 };
	uint8 candidate_digest[32];
	struct timespec pause = { 0, 1000000 };
	int owner_sockets[2];
	int queued_sockets[2];
	int fresh_sockets[2];
	char path[64];
	int journal_fd;
	int loops;
	bool received = false;

	journal_fd = open_context(&context, &journal_state, &config, &ops, path);
	if (journal_fd < 0)
		return;
	provider_state
		= mmap(NULL, sizeof(*provider_state), PROT_READ | PROT_WRITE, MAP_SHARED | MAP_ANON, -1, 0);
	UT_ASSERT(provider_state != MAP_FAILED);
	if (provider_state == MAP_FAILED)
		return;
	memset(provider_state, 0, sizeof(*provider_state));
	provider_state->total_actions = 2;
	UT_ASSERT(pgrac_fenced_coordinator_init(&coordinator, &context));
	make_request(&config, 3, 0xa1, 0xa2, &owner_request);
	make_request(&config, 3, 0xa3, 0xa4, &queued_request);
	UT_ASSERT_EQ(socketpair(AF_UNIX, SOCK_STREAM, 0, owner_sockets), 0);
	UT_ASSERT(write_request(owner_sockets[0], &owner_request));
	UT_ASSERT(pgrac_fenced_coordinator_accept_fd(&coordinator, owner_sockets[1],
												 deadline_after_ms(2000)));
	for (loops = 0; loops < 2000 && !received; loops++) {
		UT_ASSERT(pgrac_fenced_coordinator_service(&coordinator, deadline_after_ms(0)));
		received = try_response(owner_sockets[0], &response);
		(void)nanosleep(&pause, NULL);
	}
	UT_ASSERT(received);
	UT_ASSERT_EQ(response.verdict, 1);
	UT_ASSERT_EQ(socketpair(AF_UNIX, SOCK_STREAM, 0, queued_sockets), 0);
	UT_ASSERT(write_request(queued_sockets[0], &queued_request));
	UT_ASSERT(pgrac_fenced_coordinator_accept_fd(&coordinator, queued_sockets[1],
												 deadline_after_ms(2000)));
	candidate = config;
	candidate.mapping_generation++;
	candidate.nodes[3].target_uuid[0] ^= 1;
	memset(candidate_digest, 0x72, sizeof(candidate_digest));
	UT_ASSERT(pgrac_fenced_operation_prepare_mapping_reload(&context, &candidate, &ops,
															candidate_digest));
	UT_ASSERT_EQ(context.config->mapping_generation, 17);
	UT_ASSERT(pgrac_fenced_coordinator_shutdown(&coordinator, 17));
	UT_ASSERT(try_response(queued_sockets[0], &response));
	UT_ASSERT_EQ(response.verdict, 4);
	UT_ASSERT_EQ(response.deny_reason, 17);
	UT_ASSERT(pgrac_fenced_operation_activate_mapping_reload(&context));
	UT_ASSERT_EQ(context.config->mapping_generation, 18);
	UT_ASSERT(pgrac_fenced_coordinator_init(&coordinator, &context));
	make_request(&candidate, 3, 0xa5, 0xa6, &fresh_request);
	UT_ASSERT_EQ(socketpair(AF_UNIX, SOCK_STREAM, 0, fresh_sockets), 0);
	UT_ASSERT(write_request(fresh_sockets[0], &fresh_request));
	UT_ASSERT(pgrac_fenced_coordinator_accept_fd(&coordinator, fresh_sockets[1],
												 deadline_after_ms(2000)));
	received = false;
	for (loops = 0; loops < 2000 && !received; loops++) {
		UT_ASSERT(pgrac_fenced_coordinator_service(&coordinator, deadline_after_ms(0)));
		received = try_response(fresh_sockets[0], &response);
		(void)nanosleep(&pause, NULL);
	}
	UT_ASSERT(received);
	UT_ASSERT_EQ(response.verdict, 1);
	UT_ASSERT_EQ(response.binding.target_mapping_generation, 18);
	UT_ASSERT_EQ(provider_state->node_actions[3], 2);
	UT_ASSERT_EQ(journal_record_count(journal_fd), 15);
	UT_ASSERT(pgrac_fenced_coordinator_shutdown(&coordinator, 16));
	(void)close(owner_sockets[0]);
	(void)close(queued_sockets[0]);
	(void)close(fresh_sockets[0]);
	UT_ASSERT_EQ(munmap(provider_state, sizeof(*provider_state)), 0);
	(void)close(journal_fd);
	(void)unlink(path);
}

UT_TEST(test_reload_during_active_operation_invalidates_queue_without_abort)
{
	PgracFencedOperationContextV1 context;
	PgracFencedJournalScanState journal_state;
	PgracFencedProviderOpsV1 ops;
	PgracFencedConfigV1 config;
	PgracFencedCoordinatorV1 coordinator;
	PgracExternalFenceProtocolRequestV1 owner_request;
	PgracExternalFenceProtocolRequestV1 queued_request;
	PgracExternalFenceProtocolResponseV1 owner_response = { 0 };
	PgracExternalFenceProtocolResponseV1 queued_response = { 0 };
	struct timespec pause = { 0, 1000000 };
	int owner_sockets[2];
	int queued_sockets[2];
	char path[64];
	int journal_fd;
	int loops;
	bool owner_received = false;

	journal_fd = open_context(&context, &journal_state, &config, &ops, path);
	if (journal_fd < 0)
		return;
	provider_state
		= mmap(NULL, sizeof(*provider_state), PROT_READ | PROT_WRITE, MAP_SHARED | MAP_ANON, -1, 0);
	UT_ASSERT(provider_state != MAP_FAILED);
	if (provider_state == MAP_FAILED)
		return;
	memset(provider_state, 0, sizeof(*provider_state));
	UT_ASSERT(pgrac_fenced_coordinator_init(&coordinator, &context));
	make_request(&config, 3, 0xb1, 0xb2, &owner_request);
	make_request(&config, 3, 0xb3, 0xb4, &queued_request);
	provider_state->hold_action = 1;
	UT_ASSERT_EQ(socketpair(AF_UNIX, SOCK_STREAM, 0, owner_sockets), 0);
	UT_ASSERT(write_request(owner_sockets[0], &owner_request));
	UT_ASSERT(pgrac_fenced_coordinator_accept_fd(&coordinator, owner_sockets[1],
												 deadline_after_ms(2000)));
	UT_ASSERT_EQ(socketpair(AF_UNIX, SOCK_STREAM, 0, queued_sockets), 0);
	UT_ASSERT(write_request(queued_sockets[0], &queued_request));
	UT_ASSERT(pgrac_fenced_coordinator_accept_fd(&coordinator, queued_sockets[1],
												 deadline_after_ms(2000)));
	/*
	 * Prove the provider is in flight before reload, then release it explicitly.
	 * Counting 500 sleeps made this test depend on OS timer coalescing and could
	 * exhaust the unchanged two-second operation deadline on macOS.
	 */
	for (loops = 0; loops < 2000 && provider_state->node_actions[3] == 0; loops++) {
		UT_ASSERT(pgrac_fenced_coordinator_service(&coordinator, deadline_after_ms(0)));
		(void)nanosleep(&pause, NULL);
	}
	UT_ASSERT_EQ(provider_state->node_actions[3], 1);
	UT_ASSERT(!try_response(owner_sockets[0], &owner_response));
	UT_ASSERT_EQ(pgrac_fenced_coordinator_active_worker_count(&coordinator), 1);
	UT_ASSERT(pgrac_fenced_coordinator_quiesce(&coordinator, 17));
	UT_ASSERT(try_response(queued_sockets[0], &queued_response));
	UT_ASSERT_EQ(queued_response.verdict, 4);
	UT_ASSERT_EQ(queued_response.deny_reason, 17);
	provider_state->release_action = 1;
	for (loops = 0; loops < 2000 && pgrac_fenced_coordinator_active_worker_count(&coordinator) > 0;
		 loops++) {
		UT_ASSERT(pgrac_fenced_coordinator_service(&coordinator, deadline_after_ms(0)));
		if (!owner_received)
			owner_received = try_response(owner_sockets[0], &owner_response);
		(void)nanosleep(&pause, NULL);
	}
	UT_ASSERT_EQ(pgrac_fenced_coordinator_active_worker_count(&coordinator), 0);
	UT_ASSERT(owner_received);
	UT_ASSERT_EQ(owner_response.verdict, 1);
	UT_ASSERT_EQ(provider_state->node_actions[3], 1);
	UT_ASSERT(pgrac_fenced_coordinator_shutdown(&coordinator, 17));
	UT_ASSERT_EQ(journal_record_count(journal_fd), 9);
	(void)close(owner_sockets[0]);
	(void)close(queued_sockets[0]);
	UT_ASSERT_EQ(munmap(provider_state, sizeof(*provider_state)), 0);
	(void)close(journal_fd);
	(void)unlink(path);
}

UT_TEST(test_rejoin_reservation_invalidates_only_exact_target)
{
	PgracFencedOperationContextV1 context;
	PgracFencedJournalScanState journal_state;
	PgracFencedProviderOpsV1 ops;
	PgracFencedConfigV1 config;
	PgracFencedCoordinatorV1 coordinator;
	PgracExternalFenceProtocolRequestV1 request;
	PgracExternalFenceProtocolResponseV1 response;
	struct timespec pause = { 0, 1000000 };
	int first[2];
	int blocked[2];
	int other[2];
	int fresh[2];
	char path[64];
	int journal_fd;
	int loops;
	bool received;
	uint8 byte;

	journal_fd = open_context(&context, &journal_state, &config, &ops, path);
	if (journal_fd < 0)
		return;
	provider_state
		= mmap(NULL, sizeof(*provider_state), PROT_READ | PROT_WRITE, MAP_SHARED | MAP_ANON, -1, 0);
	UT_ASSERT(provider_state != MAP_FAILED);
	if (provider_state == MAP_FAILED)
		return;
	memset(provider_state, 0, sizeof(*provider_state));
	provider_state->total_actions = 2;
	UT_ASSERT(pgrac_fenced_coordinator_init(&coordinator, &context));
	make_request(&config, 3, 0xc1, 0xc2, &request);
	UT_ASSERT_EQ(socketpair(AF_UNIX, SOCK_STREAM, 0, first), 0);
	UT_ASSERT(write_request(first[0], &request));
	UT_ASSERT(pgrac_fenced_coordinator_accept_fd(&coordinator, first[1], deadline_after_ms(2000)));
	received = false;
	for (loops = 0; loops < 2000 && !received; loops++) {
		UT_ASSERT(pgrac_fenced_coordinator_service(&coordinator, deadline_after_ms(0)));
		received = try_response(first[0], &response);
		(void)nanosleep(&pause, NULL);
	}
	UT_ASSERT(received);
	UT_ASSERT_EQ(response.verdict, 1);
	UT_ASSERT_EQ(
		pgrac_fenced_coordinator_rejoin_acquire_target(&coordinator, config.nodes[3].target_uuid),
		PGRAC_FENCED_REJOIN_TARGET_READY);
	UT_ASSERT_EQ(recv(first[0], &byte, 1, MSG_DONTWAIT), -1);
	UT_ASSERT(errno == EAGAIN || errno == EWOULDBLOCK);
	UT_ASSERT_EQ(pgrac_fenced_coordinator_rejoin_invalidate_target(&coordinator,
																   config.nodes[3].target_uuid, 19),
				 PGRAC_FENCED_REJOIN_TARGET_READY);
	UT_ASSERT_EQ(recv(first[0], &byte, 1, MSG_DONTWAIT), 0);

	make_request(&config, 3, 0xc3, 0xc4, &request);
	UT_ASSERT_EQ(socketpair(AF_UNIX, SOCK_STREAM, 0, blocked), 0);
	UT_ASSERT(write_request(blocked[0], &request));
	UT_ASSERT(
		pgrac_fenced_coordinator_accept_fd(&coordinator, blocked[1], deadline_after_ms(2000)));
	UT_ASSERT(try_response(blocked[0], &response));
	UT_ASSERT_EQ(response.verdict, 4);
	UT_ASSERT_EQ(response.deny_reason, 19);
	UT_ASSERT_EQ(provider_state->node_actions[3], 1);

	make_request(&config, 4, 0xc5, 0xc6, &request);
	UT_ASSERT_EQ(socketpair(AF_UNIX, SOCK_STREAM, 0, other), 0);
	UT_ASSERT(write_request(other[0], &request));
	UT_ASSERT(pgrac_fenced_coordinator_accept_fd(&coordinator, other[1], deadline_after_ms(2000)));
	received = false;
	for (loops = 0; loops < 2000 && !received; loops++) {
		UT_ASSERT(pgrac_fenced_coordinator_service(&coordinator, deadline_after_ms(0)));
		received = try_response(other[0], &response);
		(void)nanosleep(&pause, NULL);
	}
	UT_ASSERT(received);
	UT_ASSERT_EQ(response.verdict, 1);
	UT_ASSERT_EQ(provider_state->node_actions[4], 1);

	UT_ASSERT(
		pgrac_fenced_coordinator_rejoin_release_target(&coordinator, config.nodes[3].target_uuid));
	make_request(&config, 3, 0xc7, 0xc8, &request);
	UT_ASSERT_EQ(socketpair(AF_UNIX, SOCK_STREAM, 0, fresh), 0);
	UT_ASSERT(write_request(fresh[0], &request));
	UT_ASSERT(pgrac_fenced_coordinator_accept_fd(&coordinator, fresh[1], deadline_after_ms(2000)));
	received = false;
	for (loops = 0; loops < 2000 && !received; loops++) {
		UT_ASSERT(pgrac_fenced_coordinator_service(&coordinator, deadline_after_ms(0)));
		received = try_response(fresh[0], &response);
		(void)nanosleep(&pause, NULL);
	}
	UT_ASSERT(received);
	UT_ASSERT_EQ(response.verdict, 1);
	UT_ASSERT_EQ(provider_state->node_actions[3], 2);
	UT_ASSERT(pgrac_fenced_coordinator_shutdown(&coordinator, 16));
	(void)close(first[0]);
	(void)close(blocked[0]);
	(void)close(other[0]);
	(void)close(fresh[0]);
	UT_ASSERT_EQ(munmap(provider_state, sizeof(*provider_state)), 0);
	(void)close(journal_fd);
	(void)unlink(path);
}

int
main(void)
{
	UT_PLAN(13);
	UT_RUN(test_socket_join_fifo_and_different_target_concurrency);
	UT_RUN(test_queued_deadline_is_durably_invalidated_without_action);
	UT_RUN(test_partial_ingress_never_blocks_other_clients_or_workers);
	UT_RUN(test_clean_shutdown_durably_invalidates_accepted_queue);
	UT_RUN(test_mapping_reload_closes_old_admissions_before_new_mapping);
	UT_RUN(test_reload_during_active_operation_invalidates_queue_without_abort);
	UT_RUN(test_rejoin_reservation_invalidates_only_exact_target);
	UT_RUN(test_owned_join_disconnect_and_rejoin_wait_for_background_off);
	UT_RUN(test_owned_unknown_redrives_without_caller_and_preserves_uuid);
	UT_RUN(test_owned_joiners_share_one_proof_without_new_obligations);
	UT_RUN(test_owned_replay_restores_original_fifo_without_clients_or_old_clock);
	UT_RUN(test_owned_quiesce_keeps_work_and_refuses_mapping_reload);
	UT_RUN(test_owned_expired_queue_and_reused_client_slot_do_not_lose_or_steal_work);
	UT_DONE();

	return ut_failed_count == 0 ? 0 : 1;
}
