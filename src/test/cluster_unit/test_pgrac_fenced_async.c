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
#include <sys/mman.h>
#include <sys/stat.h>
#include <time.h>
#include <unistd.h>

#include "common/pgrac_external_fence_protocol.h"
#include "pgrac_fenced_async.h"

#undef printf
#undef fprintf
#undef snprintf

#include "unit_test.h"

UT_DEFINE_GLOBALS();

static volatile uint32 *actuation_entries;

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
	ops->provider_name = "async-test";
	ops->resolve = test_resolve;
	ops->actuate_off = concurrent_actuate;
	ops->readback = test_readback;
	ops->actuate_on = concurrent_actuate;
	ops->shutdown = test_shutdown;
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
	UT_ASSERT(pgrac_external_fence_protected_set_digest_v1(
		config->storage_backend_id, config->storage_uuid, request->need.protected_set_digest));
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

UT_TEST(test_two_targets_execute_concurrently_with_parent_serial_journal)
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

	journal_fd = open_context(&context, &journal_state, &config, &ops, path);
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
	memset(&first_response, 0, sizeof(first_response));
	memset(&second_response, 0, sizeof(second_response));
	while (completed < 2 && attempts++ < 100) {
		descriptors[0].fd = pgrac_fenced_async_fd(&first);
		descriptors[0].events = descriptors[0].fd >= 0 ? POLLIN : 0;
		descriptors[0].revents = 0;
		descriptors[1].fd = pgrac_fenced_async_fd(&second);
		descriptors[1].events = descriptors[1].fd >= 0 ? POLLIN : 0;
		descriptors[1].revents = 0;
		UT_ASSERT(poll(descriptors, 2, 100) > 0);
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

int
main(void)
{
	UT_PLAN(1);
	UT_RUN(test_two_targets_execute_concurrently_with_parent_serial_journal);
	UT_DONE();

	return ut_failed_count == 0 ? 0 : 1;
}
