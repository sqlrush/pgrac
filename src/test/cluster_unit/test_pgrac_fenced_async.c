/*-------------------------------------------------------------------------
 *
 * test_pgrac_fenced_async.c
 *    Concurrent operation workers with one parent-owned journal.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <fcntl.h>
#include <poll.h>
#include <signal.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>

#include "common/pgrac_external_fence_protocol.h"
#include "pgrac_fenced_async.h"

#undef printf
#undef fprintf
#undef snprintf

#include "unit_test.h"

UT_DEFINE_GLOBALS();

#include "data/pgrac_fence_map_v2_fixture.h"

static volatile uint32 *actuation_entries;
static long actuation_pause_ns;

static PgracFencedProviderResult
test_resolve(const PgracFencedTargetV1 *configured, PgracFencedTargetV1 *resolved,
			 int32 *native_status)
{
	*resolved = *configured;
	*native_status = 0;
	return PGRAC_FENCED_PROVIDER_OK;
}

static PgracFencedProviderResult
concurrent_actuate(const PgracFencedTargetV1 *target, uint64_t deadline_mono_ns,
				   int32 *native_status)
{
	struct timespec pause = { 0, 1000000 };
	uint32 loops = 0;

	(void)target;
	(void)deadline_mono_ns;
	*native_status = 0;
	(void)__sync_add_and_fetch(actuation_entries, 1);
	while (*actuation_entries < 2 && loops++ < 500)
		(void)nanosleep(&pause, NULL);
	if (actuation_pause_ns > 0) {
		pause.tv_nsec = actuation_pause_ns;
		(void)nanosleep(&pause, NULL);
	}
	return *actuation_entries >= 2 ? PGRAC_FENCED_PROVIDER_OK : PGRAC_FENCED_PROVIDER_UNKNOWN;
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
			 PgracFencedConfigV1 *config, PgracFencedProviderOpsV1 *ops, char path[64], bool v2)
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
	ops->provider_name = "async-test";
	ops->resolve = test_resolve;
	ops->actuate_off = concurrent_actuate;
	ops->readback = test_readback;
	ops->actuate_on = concurrent_actuate;
	ops->shutdown = test_shutdown;
	if (v2) {
		if (pgrac_fenced_config_parse((const uint8 *)fenced_config_v2, sizeof(fenced_config_v2) - 1,
									  config)
			!= PGRAC_FENCED_CONFIG_OK)
			return -1;
		ops->provider_id = PGRAC_FENCED_PROVIDER_ID_PACEMAKER_LIBVIRT_V1;
	}
	memset(config_digest, 0x71, sizeof(config_digest));
	memset(daemon_boot_id, 0x81, sizeof(daemon_boot_id));
	strcpy(path, "/tmp/pgrac-fenced-async.XXXXXX");
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
make_request(const PgracFencedConfigV1 *config, int32 node_id, uint8 nonce,
			 PgracExternalFenceProtocolRequestV1 *request)
{
	memset(request, 0, sizeof(*request));
	memset(request->request_nonce, nonce, sizeof(request->request_nonce));
	request->need.system_identifier = config->system_identifier;
	memset(request->need.canonical_duty_digest, (uint8)(nonce + 1),
		   sizeof(request->need.canonical_duty_digest));
	request->need.victim_node_id = node_id;
	request->need.victim_incarnation = (uint64)node_id + 20;
	UT_ASSERT(pgrac_fenced_config_protected_set_digest(config, node_id,
													   request->need.protected_set_digest));
	request->need.predicate_id = 1;
	request->need.predicate_version = 1;
	request->timeout_ms = 2000;
}

static size_t
journal_record_count(int fd)
{
	struct stat st;

	UT_ASSERT_EQ(fstat(fd, &st), 0);
	return (size_t)st.st_size / PGRAC_FENCED_JOURNAL_RECORD_BYTES;
}

static void
run_two_targets(long pause_ns)
{
	PgracFencedOperationContextV1 context;
	PgracFencedJournalScanState journal_state;
	PgracExternalFenceProtocolRequestV1 first_request;
	PgracExternalFenceProtocolRequestV1 second_request;
	PgracExternalFenceProtocolResponseV1 first_response;
	PgracExternalFenceProtocolResponseV1 second_response;
	PgracFencedPreparedAcquireV1 first_prepared;
	PgracFencedPreparedAcquireV1 second_prepared;
	PgracFencedProviderOpsV1 ops;
	PgracFencedConfigV1 config;
	PgracFencedAsyncWorkerV1 first;
	PgracFencedAsyncWorkerV1 second;
	PgracFencedAsyncEvent event;
	struct pollfd descriptors[2];
	char path[64];
	int journal_fd;
	int completed = 0;
	int attempts = 0;
	uint64_t deadline;

	actuation_pause_ns = pause_ns;
	journal_fd = open_context(&context, &journal_state, &config, &ops, path, false);
	if (journal_fd < 0)
		return;
	actuation_entries = mmap(NULL, sizeof(*actuation_entries), PROT_READ | PROT_WRITE,
							 MAP_SHARED | MAP_ANON, -1, 0);
	UT_ASSERT(actuation_entries != MAP_FAILED);
	if (actuation_entries == MAP_FAILED)
		return;
	*actuation_entries = 0;
	make_request(&config, 3, 0x51, &first_request);
	make_request(&config, 4, 0x61, &second_request);
	UT_ASSERT_EQ(pgrac_fenced_operation_accept(&context, &first_request, deadline_after_ms(2000),
											   &first_prepared, &first_response),
				 PGRAC_FENCED_OPERATION_READY);
	UT_ASSERT_EQ(pgrac_fenced_operation_accept(&context, &second_request, deadline_after_ms(2000),
											   &second_prepared, &second_response),
				 PGRAC_FENCED_OPERATION_READY);
	UT_ASSERT(pgrac_fenced_async_start_preaccepted(&context, &first_request, &first_prepared,
												   deadline_after_ms(2000), &first));
	UT_ASSERT(pgrac_fenced_async_start_preaccepted(&context, &second_request, &second_prepared,
												   deadline_after_ms(2000), &second));
	deadline = deadline_after_ms(2000);
	memset(&first_response, 0, sizeof(first_response));
	memset(&second_response, 0, sizeof(second_response));
	while (completed < 2 && attempts++ < 100 && deadline_after_ms(0) < deadline) {
		int ready;
		descriptors[0].fd = pgrac_fenced_async_fd(&first);
		descriptors[0].events = descriptors[0].fd >= 0 ? POLLIN : 0;
		descriptors[0].revents = 0;
		descriptors[1].fd = pgrac_fenced_async_fd(&second);
		descriptors[1].events = descriptors[1].fd >= 0 ? POLLIN : 0;
		descriptors[1].revents = 0;
		ready = poll(descriptors, 2, 100);
		UT_ASSERT(ready >= 0);
		if (ready < 0)
			break;
		if (descriptors[0].revents != 0) {
			bool ok = pgrac_fenced_async_service(&context, &first, &event, &first_response);

			UT_ASSERT(ok);
			if (!ok)
				completed++;
			if (event == PGRAC_FENCED_ASYNC_COMPLETE)
				completed++;
		}
		if (descriptors[1].revents != 0) {
			bool ok = pgrac_fenced_async_service(&context, &second, &event, &second_response);

			UT_ASSERT(ok);
			if (!ok)
				completed++;
			if (event == PGRAC_FENCED_ASYNC_COMPLETE)
				completed++;
		}
	}
	UT_ASSERT_EQ(completed, 2);
	UT_ASSERT(WIFEXITED(first.wait_status));
	UT_ASSERT_EQ(WEXITSTATUS(first.wait_status), 0);
	UT_ASSERT(WIFEXITED(second.wait_status));
	UT_ASSERT_EQ(WEXITSTATUS(second.wait_status), 0);
	UT_ASSERT_EQ(*actuation_entries, 2);
	UT_ASSERT_EQ(first_response.verdict, 1);
	UT_ASSERT_EQ(second_response.verdict, 1);
	UT_ASSERT_NE(first_response.proof_generation, second_response.proof_generation);
	UT_ASSERT_EQ(journal_record_count(journal_fd), 11);
	UT_ASSERT_EQ(pgrac_fenced_async_fd(&first), -1);
	UT_ASSERT_EQ(pgrac_fenced_async_fd(&second), -1);
	UT_ASSERT_EQ(munmap((void *)actuation_entries, sizeof(*actuation_entries)), 0);
	(void)close(journal_fd);
	(void)unlink(path);
}

UT_TEST(test_two_targets_execute_concurrently_with_parent_serial_journal)
{
	run_two_targets(0);
}

UT_TEST(test_two_targets_can_wait_between_parent_poll_ticks)
{
	/* A healthy callback may exceed one 100ms service tick, still far
	 * inside its unchanged two-second operation budget. */
	run_two_targets(150000000);
}

/* A real provider child opens the actual parent journal before its action. */
static char owned_journal_path[64];
static volatile uint32 *owned_action_observed;

static PgracFencedProviderResult
owned_actuate(const PgracFencedTargetV1 *target, uint64_t deadline, int32 *status)
{
	uint8 frame[768];
	PgracFencedJournalRecordV1 accepted, issued;
	int fd = open(owned_journal_path, O_RDONLY);
	bool valid = fd >= 0 && pread(fd, frame, sizeof(frame), 256) == sizeof(frame)
				 && pgrac_fenced_journal_record_decode(frame, sizeof(frame), &accepted)
				 && pread(fd, frame, sizeof(frame), 1024) == sizeof(frame)
				 && pgrac_fenced_journal_record_decode(frame, sizeof(frame), &issued)
				 && accepted.record_kind == PGRAC_FENCED_JOURNAL_KIND_REQUEST_ACCEPTED
				 && issued.record_kind == PGRAC_FENCED_JOURNAL_KIND_ACTUATION_ISSUED
				 && accepted.intent.kind == PGRAC_FENCED_JOURNAL_INTENT_ACQUIRE
				 && pgrac_fenced_journal_intent_continues(&accepted, &issued)
				 && memcmp(accepted.intent.target_uuid, target->target_uuid, 16) == 0;

	(void)deadline;
	if (fd >= 0)
		(void)close(fd);
	*owned_action_observed = valid ? 1 : 2;
	*status = 0;
	return valid ? PGRAC_FENCED_PROVIDER_OK : PGRAC_FENCED_PROVIDER_UNKNOWN;
}

UT_TEST(test_v2_parent_binds_worker_and_rejects_replaced_attempt_before_append)
{
#ifndef USE_OPENSSL
	return;
#endif
	owned_action_observed = mmap(NULL, sizeof(*owned_action_observed), PROT_READ | PROT_WRITE,
								 MAP_SHARED | MAP_ANON, -1, 0);
	UT_ASSERT(owned_action_observed != MAP_FAILED);
	if (owned_action_observed == MAP_FAILED)
		return;
	for (int variant = 0; variant < 5; ++variant) {
		PgracFencedOperationContextV1 context;
		PgracFencedJournalScanState journal;
		PgracFencedPreparedAcquireV1 prepared;
		PgracExternalFenceProtocolRequestV1 request;
		PgracExternalFenceProtocolResponseV1 response;
		PgracFencedAsyncWorkerV1 worker;
		PgracFencedAsyncEvent event = PGRAC_FENCED_ASYNC_NONE;
		PgracFencedProviderOpsV1 ops;
		PgracFencedConfigV1 config;
		struct stat before, after;
		struct pollfd descriptor;
		int fd = open_context(&context, &journal, &config, &ops, owned_journal_path, true);
		bool ok = true;

		UT_ASSERT(fd >= 0);
		if (fd < 0)
			break;
		ops.actuate_off = owned_actuate;
		*owned_action_observed = 0;
		make_request(&config, 2, 0x51, &request);
		UT_ASSERT_EQ(pgrac_fenced_operation_accept(&context, &request, deadline_after_ms(2000),
												   &prepared, &response),
					 PGRAC_FENCED_OPERATION_READY);
		UT_ASSERT(pgrac_fenced_async_start_preaccepted(&context, &request, &prepared,
													   deadline_after_ms(2000), &worker));
		UT_ASSERT_EQ(fstat(fd, &before), 0);
		UT_ASSERT_EQ(worker.accepted_record.intent.attempt, 1);
		/* Simulate the parent's current execution/config no longer being the child's. */
		if (variant == 1)
			worker.accepted_record.intent.attempt++;
		if (variant == 2)
			context.daemon_boot_id[0] ^= 1;
		if (variant == 3)
			context.semantic_config_digest[0] ^= 1;
		if (variant == 4)
			worker.accepted_record.intent.request.acquire.request_nonce[0] ^= 1;
		for (int step = 0; step < 12 && ok && event != PGRAC_FENCED_ASYNC_COMPLETE; ++step) {
			descriptor.fd = pgrac_fenced_async_fd(&worker);
			descriptor.events = POLLIN | POLLHUP;
			descriptor.revents = 0;
			UT_ASSERT(poll(&descriptor, 1, 2000) > 0);
			ok = pgrac_fenced_async_service(&context, &worker, &event, &response);
			if (variant != 0)
				break;
		}
		if (variant == 0) {
			UT_ASSERT(ok);
			UT_ASSERT_EQ(event, PGRAC_FENCED_ASYNC_COMPLETE);
			UT_ASSERT_EQ(response.verdict, 1);
			UT_ASSERT_EQ(*owned_action_observed, 1);
		} else {
			UT_ASSERT(!ok);
			UT_ASSERT_EQ(fstat(fd, &after), 0);
			UT_ASSERT_EQ(before.st_size, after.st_size);
			UT_ASSERT_EQ(*owned_action_observed, 0);
		}
		/* Test-owned child only; no live service or guest is stopped. */
		if (worker.active) {
			(void)close(worker.fd);
			(void)kill(worker.pid, SIGKILL);
			(void)waitpid(worker.pid, NULL, 0);
		}
		(void)close(fd);
		(void)unlink(owned_journal_path);
	}
	UT_ASSERT_EQ(munmap((void *)owned_action_observed, sizeof(*owned_action_observed)), 0);
}

static bool
replay_has_no_pending(int fd)
{
	uint8 bytes[8192];
	PgracFencedJournalReconcileState pending;
	PgracFencedJournalRecordV1 record;
	struct stat st;
	size_t offset = 0;

	if (fstat(fd, &st) != 0 || st.st_size <= 0 || st.st_size > sizeof(bytes)
		|| pread(fd, bytes, st.st_size, 0) != st.st_size)
		return false;
	pgrac_fenced_journal_reconcile_state_init(&pending);
	while (offset < (size_t)st.st_size) {
		size_t length = pgrac_fenced_journal_frame_size(bytes + offset, st.st_size - offset);

		if (length == 0 || length > st.st_size - offset
			|| !pgrac_fenced_journal_record_decode(bytes + offset, length, &record)
			|| !pgrac_fenced_journal_reconcile_observe(&pending, &record))
			return false;
		offset += length;
	}
	return pending.pending_count == 0;
}

UT_TEST(test_v2_late_completion_cannot_replace_parent_durable_proof)
{
#ifndef USE_OPENSSL
	return;
#endif
	owned_action_observed = mmap(NULL, sizeof(*owned_action_observed), PROT_READ | PROT_WRITE,
								 MAP_SHARED | MAP_ANON, -1, 0);
	UT_ASSERT(owned_action_observed != MAP_FAILED);
	if (owned_action_observed == MAP_FAILED)
		return;
	for (int variant = 0; variant < 5; ++variant) {
		PgracFencedOperationContextV1 context;
		PgracFencedJournalScanState journal;
		PgracFencedPreparedAcquireV1 prepared;
		PgracExternalFenceProtocolRequestV1 request;
		PgracExternalFenceProtocolResponseV1 response;
		PgracFencedAsyncWorkerV1 worker;
		PgracFencedAsyncEvent event;
		PgracFencedProviderOpsV1 ops;
		PgracFencedConfigV1 config;
		struct stat before, after;
		struct pollfd descriptor;
		int fd = open_context(&context, &journal, &config, &ops, owned_journal_path, true);
		bool ok = true;

		UT_ASSERT(fd >= 0);
		if (fd < 0)
			break;
		ops.actuate_off = owned_actuate;
		make_request(&config, 2, 0x51, &request);
		UT_ASSERT_EQ(pgrac_fenced_operation_accept(&context, &request, deadline_after_ms(2000),
												   &prepared, &response),
					 PGRAC_FENCED_OPERATION_READY);
		UT_ASSERT(pgrac_fenced_async_start_preaccepted(&context, &request, &prepared,
													   deadline_after_ms(2000), &worker));
		for (int step = 0; step < 8 && ok; ++step) {
			descriptor.fd = pgrac_fenced_async_fd(&worker);
			descriptor.events = POLLIN | POLLHUP;
			descriptor.revents = 0;
			UT_ASSERT(poll(&descriptor, 1, 2000) > 0);
			ok = pgrac_fenced_async_service(&context, &worker, &event, &response);
			if (worker.last_record_kind == PGRAC_FENCED_JOURNAL_KIND_PROOF_SERVED)
				break;
		}
		UT_ASSERT(ok);
		UT_ASSERT_EQ(worker.last_record_kind, PGRAC_FENCED_JOURNAL_KIND_PROOF_SERVED);
		if (variant == 4) {
			/* Exact race: journal ACK has happened, COMPLETE has not been consumed. */
			UT_ASSERT(replay_has_no_pending(fd));
			UT_ASSERT(pgrac_fenced_operation_cancel_preaccepted(&context, &request, &prepared, 16,
																&response));
			UT_ASSERT(replay_has_no_pending(fd));
		}
		UT_ASSERT_EQ(fstat(fd, &before), 0);
		if (variant == 0)
			worker.accepted_record.intent.attempt++;
		if (variant == 1)
			worker.proof_generation++;
		if (variant == 2)
			worker.last_record.proof_generation++;
		if (variant == 3)
			worker.last_record.target_state_digest[0] ^= 1;
		descriptor.fd = pgrac_fenced_async_fd(&worker);
		descriptor.events = POLLIN | POLLHUP;
		descriptor.revents = 0;
		UT_ASSERT(poll(&descriptor, 1, 2000) > 0);
		UT_ASSERT_EQ(pgrac_fenced_async_service(&context, &worker, &event, &response),
					 variant == 4);
		UT_ASSERT_EQ(event, variant == 4 ? PGRAC_FENCED_ASYNC_COMPLETE : PGRAC_FENCED_ASYNC_NONE);
		UT_ASSERT_EQ(fstat(fd, &after), 0);
		UT_ASSERT_EQ(before.st_size, after.st_size);
		if (worker.active) {
			(void)close(worker.fd);
			(void)kill(worker.pid, SIGKILL);
			(void)waitpid(worker.pid, NULL, 0);
		}
		(void)close(fd);
		(void)unlink(owned_journal_path);
	}
	UT_ASSERT_EQ(munmap((void *)owned_action_observed, sizeof(*owned_action_observed)), 0);
}

int
main(void)
{
	UT_PLAN(4);
	UT_RUN(test_two_targets_execute_concurrently_with_parent_serial_journal);
	UT_RUN(test_two_targets_can_wait_between_parent_poll_ticks);
	UT_RUN(test_v2_parent_binds_worker_and_rejects_replaced_attempt_before_append);
	UT_RUN(test_v2_late_completion_cannot_replace_parent_durable_proof);
	UT_DONE();

	return ut_failed_count == 0 ? 0 : 1;
}
