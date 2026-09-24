/*-------------------------------------------------------------------------
 *
 * test_pgrac_fenced_operation.c
 *    Provider-neutral scalar ACQUIRE operation tests.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <dirent.h>
#include <fcntl.h>
#include <stdlib.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <time.h>
#include <unistd.h>

#include "common/pgrac_external_fence_protocol.h"
#include "pgrac_fenced_operation.h"

#undef printf
#undef fprintf
#undef snprintf

#include "unit_test.h"

UT_DEFINE_GLOBALS();

#include "data/pgrac_fence_map_v2_fixture.h"

typedef enum TestProviderMode {
	TEST_PROVIDER_EXACT = 0,
	TEST_PROVIDER_ACTION_UNKNOWN = 1,
	TEST_PROVIDER_READBACK_UNKNOWN = 2,
	TEST_PROVIDER_RESOLVE_MISMATCH = 3,
	TEST_PROVIDER_RESOLVE_SLOW = 4,
	TEST_PROVIDER_ACTION_SLOW = 5,
	TEST_PROVIDER_READBACK_SLOW = 6,
	TEST_PROVIDER_RESOLVE_UNAVAILABLE = 7,
	TEST_PROVIDER_RESOLVE_CONFIG_ERROR = 8,
	TEST_PROVIDER_READBACK_UNAVAILABLE = 9,
	TEST_PROVIDER_PREFLIGHT_ON_THEN_OFF = 10,
	TEST_PROVIDER_READBACK_TRANSIENT_TWICE = 11
} TestProviderMode;

static TestProviderMode provider_mode;
static unsigned int resolve_calls;
static volatile uint32 *shared_provider_counts;
static volatile uint32 *timeout_stage_entered;
static bool delay_before_timeout_stage;
static const char *owned_journal_path;
static size_t read_journal_records(int fd, PgracFencedJournalRecordV1 *records, size_t maximum);

static bool
callback_matches_durable_event(uint16 kind)
{
	PgracFencedJournalRecordV1 records[16];
	const PgracFencedJournalRecordV1 *actual = pgrac_fenced_provider_callback_record();
	const PgracFencedConfigV1 *config = pgrac_fenced_provider_callback_config();
	uint8 observed[768], durable[768];
	size_t observed_len, durable_len, count;
	int fd;

	if (owned_journal_path == NULL)
		return true;
	if (config == NULL || actual == NULL || config->format_version != 2
		|| config->system_identifier != actual->intent.system_identifier
		|| config->mapping_generation != actual->mapping_generation)
		return false;
	fd = open(owned_journal_path, O_RDONLY);
	if (fd < 0)
		return false;
	count = read_journal_records(fd, records, lengthof(records));
	(void)close(fd);
	return actual != NULL && count != 0 && actual->record_kind == kind
		   && pgrac_fenced_journal_frame_encode(actual, observed, sizeof(observed), &observed_len)
		   && pgrac_fenced_journal_frame_encode(&records[count - 1], durable, sizeof(durable),
												&durable_len)
		   && observed_len == durable_len && memcmp(observed, durable, observed_len) == 0;
}

static PgracFencedProviderResult
test_resolve(const PgracFencedTargetV1 *configured, PgracFencedTargetV1 *resolved,
			 int32 *native_status)
{
	resolve_calls++;
	if (owned_journal_path != NULL && pgrac_fenced_provider_callback_config() == NULL)
		return PGRAC_FENCED_PROVIDER_CONFIG_ERROR;
	/* Model setup/journal latency before the action/readback fault is reached. */
	if (delay_before_timeout_stage)
		(void)usleep(50000);
	if (provider_mode == TEST_PROVIDER_RESOLVE_SLOW)
		(void)usleep(200000);
	if (provider_mode == TEST_PROVIDER_RESOLVE_UNAVAILABLE)
		return PGRAC_FENCED_PROVIDER_UNAVAILABLE;
	if (provider_mode == TEST_PROVIDER_RESOLVE_CONFIG_ERROR)
		return PGRAC_FENCED_PROVIDER_CONFIG_ERROR;
	*resolved = *configured;
	if (provider_mode == TEST_PROVIDER_RESOLVE_MISMATCH)
		resolved->target_uuid[0] ^= 1;
	*native_status = 0;
	return PGRAC_FENCED_PROVIDER_OK;
}

static PgracFencedProviderResult
test_actuate(const PgracFencedTargetV1 *target, uint64_t deadline_mono_ns, int32 *native_status)
{
	if (!callback_matches_durable_event(PGRAC_FENCED_JOURNAL_KIND_ACTUATION_ISSUED))
		return PGRAC_FENCED_PROVIDER_CONFIG_ERROR;
	(void)target;
	(void)deadline_mono_ns;
	if (shared_provider_counts != NULL)
		shared_provider_counts[0]++;
	if (provider_mode == TEST_PROVIDER_ACTION_SLOW) {
		if (timeout_stage_entered != NULL)
			*timeout_stage_entered = TEST_PROVIDER_ACTION_SLOW;
		(void)usleep(2000000);
	}
	if (provider_mode == TEST_PROVIDER_ACTION_UNKNOWN) {
		*native_status = 71;
		return PGRAC_FENCED_PROVIDER_UNKNOWN;
	}
	*native_status = 0;
	return PGRAC_FENCED_PROVIDER_OK;
}

static PgracFencedProviderResult
test_readback(const PgracFencedTargetV1 *target, uint64_t deadline_mono_ns,
			  PgracFencedReadbackV1 *out)
{
	uint32 readback_call = 0;

	if (!callback_matches_durable_event(PGRAC_FENCED_JOURNAL_KIND_ACTUATION_RESULT))
		return PGRAC_FENCED_PROVIDER_CONFIG_ERROR;
	(void)deadline_mono_ns;
	memset(out, 0, sizeof(*out));
	if (shared_provider_counts != NULL)
		readback_call = ++shared_provider_counts[1];
	if (provider_mode == TEST_PROVIDER_READBACK_SLOW) {
		if (timeout_stage_entered != NULL)
			*timeout_stage_entered = TEST_PROVIDER_READBACK_SLOW;
		(void)usleep(2000000);
	}
	if (provider_mode == TEST_PROVIDER_READBACK_UNAVAILABLE)
		return PGRAC_FENCED_PROVIDER_UNAVAILABLE;
	if (provider_mode == TEST_PROVIDER_READBACK_UNKNOWN)
		return PGRAC_FENCED_PROVIDER_UNKNOWN;
	if (provider_mode == TEST_PROVIDER_READBACK_TRANSIENT_TWICE && readback_call <= 2)
		return PGRAC_FENCED_PROVIDER_UNKNOWN;
	if (provider_mode == TEST_PROVIDER_PREFLIGHT_ON_THEN_OFF && readback_call == 1) {
		out->state = PGRAC_FENCED_TARGET_ON;
		out->io_drain_state = PGRAC_FENCED_IO_DRAIN_UNKNOWN;
	} else {
		out->state = PGRAC_FENCED_TARGET_OFF;
		out->io_drain_state = PGRAC_FENCED_IO_DRAIN_DRAINED;
	}
	memcpy(out->observed_target_uuid, target->target_uuid, sizeof(out->observed_target_uuid));
	return PGRAC_FENCED_PROVIDER_OK;
}

static void
test_shutdown(void)
{}

static void
make_ops(PgracFencedProviderOpsV1 *ops)
{
	memset(ops, 0, sizeof(*ops));
	ops->abi_version = PGRAC_FENCED_PROVIDER_ABI_V1;
	ops->struct_size = sizeof(*ops);
	ops->provider_id = PGRAC_FENCED_PROVIDER_ID_TEST_ONLY;
	ops->provider_name = "operation-test";
	ops->resolve = test_resolve;
	ops->actuate_off = test_actuate;
	ops->readback = test_readback;
	ops->actuate_on = test_actuate;
	ops->shutdown = test_shutdown;
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
	config->node_count = 1;
	config->nodes[3].present = true;
	memset(config->nodes[3].target_uuid, 0x41, sizeof(config->nodes[3].target_uuid));
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

	memset(config_digest, 0x71, sizeof(config_digest));
	memset(daemon_boot_id, 0x81, sizeof(daemon_boot_id));
	strcpy(path, "/tmp/pgrac-fenced-operation.XXXXXX");
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

static size_t
read_journal_records(int fd, PgracFencedJournalRecordV1 *records, size_t maximum)
{
	uint8 frame[PGRAC_FENCED_JOURNAL_MAX_RECORD_BYTES];
	size_t count = 0;
	size_t length;
	ssize_t got;

	UT_ASSERT_EQ(lseek(fd, 0, SEEK_SET), 0);
	while (count < maximum) {
		do {
			got = read(fd, frame, PGRAC_FENCED_JOURNAL_RECORD_BYTES);
		} while (got < 0 && errno == EINTR);
		if (got == 0)
			break;
		UT_ASSERT_EQ(got, PGRAC_FENCED_JOURNAL_RECORD_BYTES);
		if (got != PGRAC_FENCED_JOURNAL_RECORD_BYTES)
			break;
		length = pgrac_fenced_journal_frame_size(frame, got);
		UT_ASSERT(length >= (size_t)got && length <= sizeof(frame));
		if (length < (size_t)got || length > sizeof(frame))
			break;
		if (length > (size_t)got) {
			ssize_t rest = read(fd, frame + got, length - got);
			UT_ASSERT_EQ(rest, length - got);
			if (rest != (ssize_t)(length - got))
				break;
		}
		UT_ASSERT(pgrac_fenced_journal_record_decode(frame, length, &records[count]));
		count++;
	}
	return count;
}

UT_TEST(test_scalar_acquire_fsyncs_exact_positive_sequence)
{
	PgracFencedOperationContextV1 context;
	PgracFencedJournalScanState journal_state;
	PgracFencedJournalRecordV1 records[8];
	PgracExternalFenceProtocolRequestV1 request;
	PgracExternalFenceProtocolResponseV1 response;
	PgracFencedProviderOpsV1 ops;
	PgracFencedConfigV1 config;
	char path[64];
	int fd;
	size_t count;

	provider_mode = TEST_PROVIDER_EXACT;
	make_config(&config);
	make_ops(&ops);
	make_request(&config, &request);
	fd = open_context(&context, &journal_state, &config, &ops, path);
	if (fd < 0)
		return;
	UT_ASSERT(
		pgrac_fenced_operation_acquire(&context, &request, deadline_after_ms(1000), &response));
	UT_ASSERT_EQ(response.verdict, 1);
	UT_ASSERT_EQ(response.deny_reason, 0);
	UT_ASSERT_EQ(response.proof_generation, 1);
	UT_ASSERT_EQ(response.binding.target_mapping_generation, 17);
	UT_ASSERT(pgrac_external_fence_affirmative_response_matches_request_v1(&request, &response));
	count = read_journal_records(fd, records, lengthof(records));
	UT_ASSERT_EQ(count, 6);
	UT_ASSERT_EQ(records[0].record_kind, PGRAC_FENCED_JOURNAL_KIND_CONFIG_LOADED);
	UT_ASSERT_EQ(records[1].record_kind, PGRAC_FENCED_JOURNAL_KIND_REQUEST_ACCEPTED);
	UT_ASSERT_EQ(records[2].record_kind, PGRAC_FENCED_JOURNAL_KIND_ACTUATION_ISSUED);
	UT_ASSERT_EQ(records[3].record_kind, PGRAC_FENCED_JOURNAL_KIND_ACTUATION_RESULT);
	UT_ASSERT_EQ(records[4].record_kind, PGRAC_FENCED_JOURNAL_KIND_READBACK_RESULT);
	UT_ASSERT_EQ(records[5].record_kind, PGRAC_FENCED_JOURNAL_KIND_PROOF_SERVED);
	UT_ASSERT_EQ(response.journal_seq, records[5].seq);
	(void)close(fd);
	(void)unlink(path);
}

UT_TEST(test_action_failure_still_accepts_independent_positive_readback)
{
	PgracFencedOperationContextV1 context;
	PgracFencedJournalScanState journal_state;
	PgracExternalFenceProtocolRequestV1 request;
	PgracExternalFenceProtocolResponseV1 response;
	PgracFencedProviderOpsV1 ops;
	PgracFencedConfigV1 config;
	char path[64];
	int fd;

	provider_mode = TEST_PROVIDER_ACTION_UNKNOWN;
	make_config(&config);
	make_ops(&ops);
	make_request(&config, &request);
	fd = open_context(&context, &journal_state, &config, &ops, path);
	if (fd < 0)
		return;
	UT_ASSERT(
		pgrac_fenced_operation_acquire(&context, &request, deadline_after_ms(1000), &response));
	UT_ASSERT_EQ(response.verdict, 1);
	UT_ASSERT_EQ(response.provider_result, PGRAC_FENCED_PROVIDER_OK);
	(void)close(fd);
	(void)unlink(path);
}

UT_TEST(test_scalar_readback_retries_transient_results_before_proof)
{
	PgracFencedOperationContextV1 context;
	PgracFencedJournalScanState journal_state;
	PgracExternalFenceProtocolRequestV1 request;
	PgracExternalFenceProtocolResponseV1 response;
	PgracFencedProviderOpsV1 ops;
	PgracFencedConfigV1 config;
	char path[64];
	int fd;

	provider_mode = TEST_PROVIDER_READBACK_TRANSIENT_TWICE;
	shared_provider_counts
		= mmap(NULL, sizeof(uint32) * 2, PROT_READ | PROT_WRITE, MAP_SHARED | MAP_ANON, -1, 0);
	UT_ASSERT(shared_provider_counts != MAP_FAILED);
	if (shared_provider_counts == MAP_FAILED) {
		shared_provider_counts = NULL;
		return;
	}
	memset((void *)shared_provider_counts, 0, sizeof(uint32) * 2);
	make_config(&config);
	make_ops(&ops);
	make_request(&config, &request);
	fd = open_context(&context, &journal_state, &config, &ops, path);
	if (fd < 0)
		return;
	UT_ASSERT(
		pgrac_fenced_operation_acquire(&context, &request, deadline_after_ms(2000), &response));
	UT_ASSERT_EQ(response.verdict, 1);
	UT_ASSERT_EQ(shared_provider_counts[1], 3);
	UT_ASSERT_EQ(response.proof_generation, 1);
	(void)close(fd);
	(void)unlink(path);
	UT_ASSERT_EQ(munmap((void *)shared_provider_counts, sizeof(uint32) * 2), 0);
	shared_provider_counts = NULL;
}

UT_TEST(test_resolve_mismatch_and_persistent_unknown_never_prove)
{
	PgracFencedOperationContextV1 context;
	PgracFencedJournalScanState journal_state;
	PgracExternalFenceProtocolRequestV1 request;
	PgracExternalFenceProtocolResponseV1 response;
	PgracFencedProviderOpsV1 ops;
	PgracFencedConfigV1 config;
	char path[64];
	int fd;

	provider_mode = TEST_PROVIDER_RESOLVE_MISMATCH;
	make_config(&config);
	make_ops(&ops);
	make_request(&config, &request);
	fd = open_context(&context, &journal_state, &config, &ops, path);
	if (fd < 0)
		return;
	UT_ASSERT(
		pgrac_fenced_operation_acquire(&context, &request, deadline_after_ms(1000), &response));
	UT_ASSERT_EQ(response.verdict, 2);
	UT_ASSERT_EQ(response.deny_reason, 8);
	UT_ASSERT_EQ(context.next_proof_generation, 1);
	(void)close(fd);
	(void)unlink(path);

	provider_mode = TEST_PROVIDER_READBACK_UNKNOWN;
	fd = open_context(&context, &journal_state, &config, &ops, path);
	if (fd < 0)
		return;
	UT_ASSERT(
		pgrac_fenced_operation_acquire(&context, &request, deadline_after_ms(1000), &response));
	UT_ASSERT_EQ(response.verdict, 4);
	UT_ASSERT_EQ(response.deny_reason, 11);
	UT_ASSERT_EQ(response.proof_generation, 0);
	UT_ASSERT_EQ(context.next_proof_generation, 1);
	(void)close(fd);
	(void)unlink(path);
}

UT_TEST(test_protected_set_mismatch_has_zero_request_journal_and_action)
{
	PgracFencedOperationContextV1 context;
	PgracFencedJournalScanState journal_state;
	PgracFencedJournalRecordV1 records[2];
	PgracExternalFenceProtocolRequestV1 request;
	PgracExternalFenceProtocolResponseV1 response;
	PgracFencedProviderOpsV1 ops;
	PgracFencedConfigV1 config;
	char path[64];
	int fd;

	provider_mode = TEST_PROVIDER_EXACT;
	make_config(&config);
	make_ops(&ops);
	make_request(&config, &request);
	request.need.protected_set_digest[0] ^= 1;
	fd = open_context(&context, &journal_state, &config, &ops, path);
	if (fd < 0)
		return;
	UT_ASSERT(
		pgrac_fenced_operation_acquire(&context, &request, deadline_after_ms(1000), &response));
	UT_ASSERT_EQ(response.verdict, 4);
	UT_ASSERT_EQ(response.deny_reason, 4);
	UT_ASSERT_EQ(read_journal_records(fd, records, lengthof(records)), 1);
	UT_ASSERT_EQ(context.next_proof_generation, 1);
	(void)close(fd);
	(void)unlink(path);
}

UT_TEST(test_deadline_owner_maps_slow_resolve_to_timeout)
{
	PgracFencedOperationContextV1 context;
	PgracFencedJournalScanState journal_state;
	PgracExternalFenceProtocolRequestV1 request;
	PgracExternalFenceProtocolResponseV1 response;
	PgracFencedProviderOpsV1 ops;
	PgracFencedConfigV1 config;
	char path[64];
	int fd;

	provider_mode = TEST_PROVIDER_RESOLVE_SLOW;
	make_config(&config);
	make_ops(&ops);
	make_request(&config, &request);
	fd = open_context(&context, &journal_state, &config, &ops, path);
	if (fd < 0)
		return;
	UT_ASSERT(pgrac_fenced_operation_acquire(&context, &request, deadline_after_ms(20), &response));
	UT_ASSERT_EQ(response.verdict, 4);
	UT_ASSERT_EQ(response.deny_reason, 11);
	UT_ASSERT_EQ(response.proof_generation, 0);
	(void)close(fd);
	(void)unlink(path);
}

UT_TEST(test_deadline_owner_maps_slow_action_and_readback_to_timeout)
{
	PgracFencedOperationContextV1 context;
	PgracFencedJournalScanState journal_state;
	PgracFencedJournalRecordV1 records[8];
	PgracExternalFenceProtocolRequestV1 request;
	PgracExternalFenceProtocolResponseV1 response;
	PgracFencedProviderOpsV1 ops;
	PgracFencedConfigV1 config;
	TestProviderMode modes[] = { TEST_PROVIDER_ACTION_SLOW, TEST_PROVIDER_READBACK_SLOW };
	char path[64];
	size_t i;

	make_config(&config);
	make_ops(&ops);
	make_request(&config, &request);
	timeout_stage_entered = mmap(NULL, sizeof(*timeout_stage_entered), PROT_READ | PROT_WRITE,
								 MAP_SHARED | MAP_ANON, -1, 0);
	UT_ASSERT(timeout_stage_entered != MAP_FAILED);
	if (timeout_stage_entered == MAP_FAILED) {
		timeout_stage_entered = NULL;
		return;
	}
	delay_before_timeout_stage = true;
	for (i = 0; i < lengthof(modes); i++) {
		int fd;
		size_t count;

		provider_mode = modes[i];
		*timeout_stage_entered = 0;
		fd = open_context(&context, &journal_state, &config, &ops, path);
		if (fd < 0)
			break;
		/*
		 * Use the fixture's one-second request budget to reach the target stage;
		 * its two-second injected stall must then hit the real deadline.  A
		 * 20ms whole-operation deadline could expire during setup, testing a
		 * different branch without ever executing the intended provider fault.
		 */
		UT_ASSERT(pgrac_fenced_operation_acquire(&context, &request,
												 deadline_after_ms(request.timeout_ms), &response));
		UT_ASSERT_EQ(response.verdict, 4);
		UT_ASSERT_EQ(response.deny_reason, 11);
		UT_ASSERT_EQ(response.proof_generation, 0);
		UT_ASSERT_EQ(*timeout_stage_entered, modes[i]);
		count = read_journal_records(fd, records, lengthof(records));
		UT_ASSERT(count > 0);
		if (count > 0) {
			UT_ASSERT_EQ(records[count - 1].record_kind, PGRAC_FENCED_JOURNAL_KIND_READBACK_RESULT);
			UT_ASSERT_EQ(records[count - 1].deny_reason, 11);
		}
		(void)close(fd);
		(void)unlink(path);
	}
	delay_before_timeout_stage = false;
	UT_ASSERT_EQ(munmap((void *)timeout_stage_entered, sizeof(*timeout_stage_entered)), 0);
	timeout_stage_entered = NULL;
}

UT_TEST(test_journal_failure_is_sticky_and_prevents_provider_calls)
{
	PgracFencedOperationContextV1 context;
	PgracFencedJournalScanState journal_state;
	PgracExternalFenceProtocolRequestV1 request;
	PgracExternalFenceProtocolResponseV1 response;
	PgracFencedProviderOpsV1 ops;
	PgracFencedConfigV1 config;
	char path[64];
	int fd;

	provider_mode = TEST_PROVIDER_EXACT;
	resolve_calls = 0;
	make_config(&config);
	make_ops(&ops);
	make_request(&config, &request);
	fd = open_context(&context, &journal_state, &config, &ops, path);
	if (fd < 0)
		return;
	UT_ASSERT_EQ(close(fd), 0);
	UT_ASSERT(
		pgrac_fenced_operation_acquire(&context, &request, deadline_after_ms(1000), &response));
	UT_ASSERT_EQ(response.verdict, 4);
	UT_ASSERT_EQ(response.deny_reason, 12);
	UT_ASSERT(!context.available);
	UT_ASSERT_EQ(resolve_calls, 0);
	UT_ASSERT(
		pgrac_fenced_operation_acquire(&context, &request, deadline_after_ms(1000), &response));
	UT_ASSERT_EQ(response.verdict, 4);
	UT_ASSERT_EQ(response.deny_reason, 12);
	UT_ASSERT_EQ(resolve_calls, 0);
	(void)unlink(path);
}

UT_TEST(test_malformed_request_is_bad_argument_without_side_effects)
{
	PgracFencedOperationContextV1 context;
	PgracFencedJournalScanState journal_state;
	PgracFencedJournalRecordV1 records[2];
	PgracExternalFenceProtocolRequestV1 request;
	PgracExternalFenceProtocolResponseV1 response;
	PgracFencedProviderOpsV1 ops;
	PgracFencedConfigV1 config;
	char path[64];
	int fd;

	provider_mode = TEST_PROVIDER_EXACT;
	resolve_calls = 0;
	make_config(&config);
	make_ops(&ops);
	make_request(&config, &request);
	fd = open_context(&context, &journal_state, &config, &ops, path);
	if (fd < 0)
		return;
	request.need.predicate_version = 0;
	UT_ASSERT(
		pgrac_fenced_operation_acquire(&context, &request, deadline_after_ms(1000), &response));
	UT_ASSERT_EQ(response.verdict, 4);
	UT_ASSERT_EQ(response.deny_reason, 1);
	make_request(&config, &request);
	request.timeout_ms = 0;
	UT_ASSERT(
		pgrac_fenced_operation_acquire(&context, &request, deadline_after_ms(1000), &response));
	UT_ASSERT_EQ(response.verdict, 4);
	UT_ASSERT_EQ(response.deny_reason, 1);
	UT_ASSERT_EQ(resolve_calls, 0);
	UT_ASSERT_EQ(read_journal_records(fd, records, lengthof(records)), 1);
	(void)close(fd);
	(void)unlink(path);
}

UT_TEST(test_provider_unavailable_mapping_is_exact_in_journal_and_response)
{
	PgracFencedOperationContextV1 context;
	PgracFencedJournalScanState journal_state;
	PgracFencedJournalRecordV1 records[8];
	PgracExternalFenceProtocolRequestV1 request;
	PgracExternalFenceProtocolResponseV1 response;
	PgracFencedProviderOpsV1 ops;
	PgracFencedConfigV1 config;
	TestProviderMode modes[]
		= { TEST_PROVIDER_RESOLVE_UNAVAILABLE, TEST_PROVIDER_RESOLVE_CONFIG_ERROR,
			TEST_PROVIDER_READBACK_UNAVAILABLE };
	uint16 expected_kinds[]
		= { PGRAC_FENCED_JOURNAL_KIND_ACTUATION_RESULT, PGRAC_FENCED_JOURNAL_KIND_ACTUATION_RESULT,
			PGRAC_FENCED_JOURNAL_KIND_READBACK_RESULT };
	char path[64];
	size_t i;

	make_config(&config);
	make_ops(&ops);
	make_request(&config, &request);
	for (i = 0; i < lengthof(modes); i++) {
		int fd;
		size_t count;

		provider_mode = modes[i];
		fd = open_context(&context, &journal_state, &config, &ops, path);
		if (fd < 0)
			return;
		UT_ASSERT(
			pgrac_fenced_operation_acquire(&context, &request, deadline_after_ms(1000), &response));
		UT_ASSERT_EQ(response.verdict, 4);
		UT_ASSERT_EQ(response.deny_reason, 10);
		count = read_journal_records(fd, records, lengthof(records));
		UT_ASSERT(count > 0);
		if (count > 0) {
			UT_ASSERT_EQ(records[count - 1].record_kind, expected_kinds[i]);
			UT_ASSERT_EQ(records[count - 1].deny_reason, 10);
		}
		(void)close(fd);
		(void)unlink(path);
	}
}

UT_TEST(test_operation_rotates_full_active_before_next_durable_record)
{
	PgracFencedOperationContextV1 context;
	PgracFencedJournalScanState journal_state;
	PgracExternalFenceProtocolRequestV1 request;
	PgracExternalFenceProtocolResponseV1 response;
	PgracFencedProviderOpsV1 ops;
	PgracFencedConfigV1 config;
	uint8 config_digest[32];
	uint8 daemon_boot_id[16];
	char directory_path[] = "/tmp/pgrac-fenced-operation-rotate.XXXXXX";
	char active_path[MAXPGPATH];
	DIR *directory;
	int directory_fd;
	int active_fd;

	provider_mode = TEST_PROVIDER_EXACT;
	make_config(&config);
	make_ops(&ops);
	make_request(&config, &request);
	memset(config_digest, 0x71, sizeof(config_digest));
	memset(daemon_boot_id, 0x81, sizeof(daemon_boot_id));
	UT_ASSERT_NOT_NULL(mkdtemp(directory_path));
	UT_ASSERT(snprintf(active_path, sizeof(active_path), "%s/%s", directory_path,
					   PGRAC_FENCED_JOURNAL_ACTIVE_NAME)
			  > 0);
	active_fd = open(active_path, O_RDWR | O_CREAT | O_EXCL | O_APPEND, 0600);
	UT_ASSERT(active_fd >= 0);
	directory_fd = open(directory_path, O_RDONLY | O_DIRECTORY);
	UT_ASSERT(directory_fd >= 0);
	pgrac_fenced_journal_scan_state_init(&journal_state);
	UT_ASSERT(pgrac_fenced_operation_context_init(&context, &config, &ops, true, config_digest,
												  daemon_boot_id, active_fd, &journal_state));
	UT_ASSERT(pgrac_fenced_operation_enable_rotation(&context, directory_fd, 0));
	UT_ASSERT_EQ(ftruncate(active_fd, (off_t)PGRAC_FENCED_JOURNAL_SEGMENT_BYTES), 0);
	journal_state.segment_first_seq = 1;
	journal_state.next_seq = UINT64_C(262145);
	journal_state.segment_record_count = PGRAC_FENCED_JOURNAL_SEGMENT_RECORDS;
	journal_state.valid_bytes = PGRAC_FENCED_JOURNAL_SEGMENT_BYTES;
	UT_ASSERT(
		pgrac_fenced_operation_acquire(&context, &request, deadline_after_ms(1000), &response));
	UT_ASSERT_EQ(response.verdict, 1);
	UT_ASSERT_EQ(context.sealed_count, 1);
	UT_ASSERT_EQ(journal_state.next_seq, UINT64_C(262150));
	UT_ASSERT(context.journal_fd >= 0);
	(void)close(context.journal_fd);
	(void)close(directory_fd);
	directory = opendir(directory_path);
	UT_ASSERT_NOT_NULL(directory);
	if (directory != NULL) {
		char entry_path[MAXPGPATH];
		const struct dirent *entry;

		while ((entry = readdir(directory)) != NULL) {
			if (strcmp(entry->d_name, ".") == 0 || strcmp(entry->d_name, "..") == 0)
				continue;
			UT_ASSERT(
				snprintf(entry_path, sizeof(entry_path), "%s/%s", directory_path, entry->d_name)
				> 0);
			UT_ASSERT_EQ(unlink(entry_path), 0);
		}
		(void)closedir(directory);
	}
	UT_ASSERT_EQ(rmdir(directory_path), 0);
}

UT_TEST(test_preaccepted_execution_has_one_request_record)
{
	PgracFencedOperationContextV1 context;
	PgracFencedJournalScanState journal_state;
	PgracExternalFenceProtocolRequestV1 request;
	PgracExternalFenceProtocolResponseV1 response;
	PgracFencedPreparedAcquireV1 prepared;
	PgracFencedJournalRecordV1 records[8];
	PgracFencedProviderOpsV1 ops;
	PgracFencedConfigV1 config;
	char path[64];
	int journal_fd;

	provider_mode = TEST_PROVIDER_EXACT;
	make_config(&config);
	make_ops(&ops);
	journal_fd = open_context(&context, &journal_state, &config, &ops, path);
	if (journal_fd < 0)
		return;
	make_request(&config, &request);
	UT_ASSERT_EQ(pgrac_fenced_operation_accept(&context, &request, deadline_after_ms(1000),
											   &prepared, &response),
				 PGRAC_FENCED_OPERATION_READY);
	UT_ASSERT_EQ(read_journal_records(journal_fd, records, lengthof(records)), 2);
	UT_ASSERT(pgrac_fenced_operation_execute_preaccepted(&context, &request, &prepared,
														 deadline_after_ms(1000), &response));
	UT_ASSERT_EQ(response.verdict, 1);
	UT_ASSERT_EQ(read_journal_records(journal_fd, records, lengthof(records)), 6);
	(void)close(journal_fd);
	(void)unlink(path);
}

UT_TEST(test_joiner_gets_shared_durable_proof_from_owner_readback)
{
	PgracFencedOperationContextV1 context;
	PgracFencedJournalScanState journal_state;
	PgracExternalFenceProtocolRequestV1 owner_request;
	PgracExternalFenceProtocolRequestV1 joiner_request;
	PgracExternalFenceProtocolResponseV1 owner_response;
	PgracExternalFenceProtocolResponseV1 joiner_response;
	PgracFencedPreparedAcquireV1 owner_prepared;
	PgracFencedPreparedAcquireV1 joiner_prepared;
	PgracFencedJournalRecordV1 records[10];
	PgracFencedProviderOpsV1 ops;
	PgracFencedConfigV1 config;
	uint64 deadline;
	char path[64];
	int journal_fd;

	provider_mode = TEST_PROVIDER_EXACT;
	make_config(&config);
	make_ops(&ops);
	journal_fd = open_context(&context, &journal_state, &config, &ops, path);
	if (journal_fd < 0)
		return;
	make_request(&config, &owner_request);
	joiner_request = owner_request;
	memset(joiner_request.request_nonce, 0x52, sizeof(joiner_request.request_nonce));
	deadline = deadline_after_ms(1000);
	UT_ASSERT_EQ(pgrac_fenced_operation_accept(&context, &owner_request, deadline, &owner_prepared,
											   &owner_response),
				 PGRAC_FENCED_OPERATION_READY);
	UT_ASSERT_EQ(pgrac_fenced_operation_accept(&context, &joiner_request, deadline,
											   &joiner_prepared, &joiner_response),
				 PGRAC_FENCED_OPERATION_READY);
	UT_ASSERT(memcmp(owner_prepared.binding_digest, joiner_prepared.binding_digest,
					 sizeof(owner_prepared.binding_digest))
			  == 0);
	UT_ASSERT(pgrac_fenced_operation_execute_preaccepted(&context, &owner_request, &owner_prepared,
														 deadline, &owner_response));
	UT_ASSERT_EQ(owner_response.verdict, 1);
	UT_ASSERT(pgrac_fenced_operation_serve_joiner(&context, &joiner_request, &joiner_prepared,
												  &owner_response, deadline, &joiner_response));
	UT_ASSERT_EQ(joiner_response.verdict, 1);
	UT_ASSERT_EQ(owner_response.proof_generation, joiner_response.proof_generation);
	UT_ASSERT(memcmp(owner_response.target_state_digest, joiner_response.target_state_digest,
					 sizeof(owner_response.target_state_digest))
			  == 0);
	UT_ASSERT_EQ(owner_response.verified_mono_ns, joiner_response.verified_mono_ns);
	UT_ASSERT(memcmp(joiner_response.request_nonce, joiner_request.request_nonce,
					 sizeof(joiner_response.request_nonce))
			  == 0);
	UT_ASSERT_EQ(read_journal_records(journal_fd, records, lengthof(records)), 8);
	(void)close(journal_fd);
	(void)unlink(path);
}

UT_TEST(test_restart_uncertainty_forces_readback_before_new_off_action)
{
	PgracFencedOperationContextV1 context;
	PgracFencedJournalScanState journal_state;
	PgracFencedJournalRecordV1 records[10];
	PgracExternalFenceProtocolRequestV1 request;
	PgracExternalFenceProtocolResponseV1 response;
	PgracFencedProviderOpsV1 ops;
	PgracFencedConfigV1 config;
	char path[64];
	int fd;

	provider_mode = TEST_PROVIDER_PREFLIGHT_ON_THEN_OFF;
	shared_provider_counts
		= mmap(NULL, sizeof(uint32) * 2, PROT_READ | PROT_WRITE, MAP_SHARED | MAP_ANON, -1, 0);
	UT_ASSERT(shared_provider_counts != MAP_FAILED);
	if (shared_provider_counts == MAP_FAILED) {
		shared_provider_counts = NULL;
		return;
	}
	memset((void *)shared_provider_counts, 0, sizeof(uint32) * 2);
	make_config(&config);
	make_ops(&ops);
	make_request(&config, &request);
	fd = open_context(&context, &journal_state, &config, &ops, path);
	if (fd < 0)
		return;
	context.restart_fresh_readback_required = true;
	UT_ASSERT(
		pgrac_fenced_operation_acquire(&context, &request, deadline_after_ms(1000), &response));
	UT_ASSERT_EQ(response.verdict, 1);
	UT_ASSERT_EQ(shared_provider_counts[0], 1);
	UT_ASSERT_EQ(shared_provider_counts[1], 2);
	UT_ASSERT_EQ(read_journal_records(fd, records, lengthof(records)), 7);
	UT_ASSERT_EQ(records[2].record_kind, PGRAC_FENCED_JOURNAL_KIND_READBACK_RESULT);
	UT_ASSERT_EQ(records[3].record_kind, PGRAC_FENCED_JOURNAL_KIND_ACTUATION_ISSUED);
	UT_ASSERT_EQ(records[5].record_kind, PGRAC_FENCED_JOURNAL_KIND_READBACK_RESULT);
	UT_ASSERT_EQ(records[6].record_kind, PGRAC_FENCED_JOURNAL_KIND_PROOF_SERVED);
	(void)close(fd);
	(void)unlink(path);
	UT_ASSERT_EQ(munmap((void *)shared_provider_counts, sizeof(uint32) * 2), 0);
	shared_provider_counts = NULL;
}

UT_TEST(test_startup_reconcile_fsyncs_diagnostic_without_provider_action)
{
	PgracFencedJournalReconcileState reconcile;
	PgracFencedOperationContextV1 context;
	PgracFencedJournalScanState journal_state;
	PgracFencedJournalRecordV1 records[4];
	PgracFencedJournalRecordV1 record;
	PgracFencedProviderOpsV1 ops;
	PgracFencedConfigV1 config;
	char path[64];
	int fd;

	provider_mode = TEST_PROVIDER_EXACT;
	make_config(&config);
	make_ops(&ops);
	fd = open_context(&context, &journal_state, &config, &ops, path);
	if (fd < 0)
		return;
	pgrac_fenced_journal_reconcile_state_init(&reconcile);
	memset(&record, 0, sizeof(record));
	record.record_kind = PGRAC_FENCED_JOURNAL_KIND_ACTUATION_ISSUED;
	memset(record.operation_id, 0x91, sizeof(record.operation_id));
	memset(record.binding_digest, 0x92, sizeof(record.binding_digest));
	record.provider_id = PGRAC_FENCED_PROVIDER_ID_TEST_ONLY;
	record.provider_abi_version = PGRAC_FENCED_PROVIDER_ABI_V1;
	record.provider_result = PGRAC_FENCED_PROVIDER_PENDING;
	record.target_state = PGRAC_FENCED_JOURNAL_TARGET_TRANSITIONING;
	UT_ASSERT(pgrac_fenced_journal_reconcile_observe(&reconcile, &record));
	UT_ASSERT(pgrac_fenced_journal_reconcile_finish(&reconcile));
	UT_ASSERT(pgrac_fenced_operation_reconcile_startup(&context, &reconcile));
	UT_ASSERT(context.restart_fresh_readback_required);
	UT_ASSERT_EQ(reconcile.pending_count, 0);
	UT_ASSERT_EQ(read_journal_records(fd, records, lengthof(records)), 2);
	UT_ASSERT_EQ(records[1].record_kind, PGRAC_FENCED_JOURNAL_KIND_RECONCILED);
	UT_ASSERT_EQ(records[1].target_state, PGRAC_FENCED_JOURNAL_TARGET_UNKNOWN);
	UT_ASSERT(memcmp(records[1].operation_id, record.operation_id, sizeof(record.operation_id))
			  == 0);
	(void)close(fd);
	(void)unlink(path);
}

UT_TEST(test_mapping_reload_is_durable_before_activation)
{
	PgracFencedOperationContextV1 context;
	PgracFencedJournalScanState journal_state;
	PgracFencedJournalRecordV1 records[4];
	PgracFencedProviderOpsV1 ops;
	PgracFencedConfigV1 current;
	PgracFencedConfigV1 candidate;
	uint8 candidate_digest[32];
	char path[64];
	int fd;

	provider_mode = TEST_PROVIDER_EXACT;
	make_config(&current);
	make_ops(&ops);
	fd = open_context(&context, &journal_state, &current, &ops, path);
	if (fd < 0)
		return;
	candidate = current;
	candidate.mapping_generation++;
	candidate.nodes[3].target_uuid[0] ^= 1;
	memset(candidate_digest, 0x72, sizeof(candidate_digest));
	UT_ASSERT(pgrac_fenced_operation_prepare_mapping_reload(&context, &candidate, &ops,
															candidate_digest));
	UT_ASSERT_EQ(context.config->mapping_generation, 17);
	UT_ASSERT_EQ(read_journal_records(fd, records, lengthof(records)), 2);
	UT_ASSERT_EQ(records[1].record_kind, PGRAC_FENCED_JOURNAL_KIND_CONFIG_LOADED);
	UT_ASSERT_EQ(records[1].mapping_generation, 18);
	UT_ASSERT(memcmp(records[1].semantic_config_digest, candidate_digest, sizeof(candidate_digest))
			  == 0);
	UT_ASSERT(!pgrac_fenced_operation_prepare_mapping_reload(&context, &candidate, &ops,
															 candidate_digest));
	UT_ASSERT(pgrac_fenced_operation_activate_mapping_reload(&context));
	UT_ASSERT_EQ(context.config->mapping_generation, 18);
	UT_ASSERT(memcmp(context.semantic_config_digest, candidate_digest, sizeof(candidate_digest))
			  == 0);
	UT_ASSERT(!pgrac_fenced_operation_activate_mapping_reload(&context));
	(void)close(fd);
	(void)unlink(path);
}

UT_TEST(test_v2_acquire_never_falls_back_to_storage_only_identity)
{
	PgracFencedOperationContextV1 context;
	PgracFencedJournalScanState journal_state;
	PgracFencedPreparedAcquireV1 prepared;
	PgracExternalFenceProtocolRequestV1 request;
	PgracExternalFenceProtocolResponseV1 response;
	PgracFencedProviderOpsV1 ops;
	PgracFencedConfigV1 config;
	char path[64];
	int fd;
	PgracFencedConfigResult parsed;

	parsed = pgrac_fenced_config_parse((const uint8 *)fenced_config_v2,
									   sizeof(fenced_config_v2) - 1, &config);
#ifndef USE_OPENSSL
	UT_ASSERT_NE(parsed, PGRAC_FENCED_CONFIG_OK);
	return;
#else
	UT_ASSERT_EQ(parsed, PGRAC_FENCED_CONFIG_OK);
	if (parsed != PGRAC_FENCED_CONFIG_OK)
		return;
#endif
	make_ops(&ops);
	/* An injected unit provider verifies the real accept boundary, not deployment. */
	ops.provider_id = PGRAC_FENCED_PROVIDER_ID_PACEMAKER_LIBVIRT_V1;
	fd = open_context(&context, &journal_state, &config, &ops, path);
	if (fd < 0)
		return;
	if (!context.available)
		goto done;
	make_request(&config, &request);
	request.need.victim_node_id = 2;
	memcpy(request.need.protected_set_digest, config.nodes[2].protected_set_digest, 32);
	UT_ASSERT_EQ(pgrac_fenced_operation_accept(&context, &request, deadline_after_ms(1000),
											   &prepared, &response),
				 PGRAC_FENCED_OPERATION_READY);
	UT_ASSERT(pgrac_external_fence_protected_set_digest_v1(
		config.storage_backend_id, config.storage_uuid, request.need.protected_set_digest));
	UT_ASSERT_EQ(pgrac_fenced_operation_accept(&context, &request, deadline_after_ms(1000),
											   &prepared, &response),
				 PGRAC_FENCED_OPERATION_COMPLETE);
	UT_ASSERT_EQ(response.deny_reason, 4);

done:
	(void)close(fd);
	(void)unlink(path);
}

/* Real file and production replay reducer; no provider callback is permitted. */
static bool
seed_owned_pending(PgracFencedOperationContextV1 *context,
				   PgracFencedJournalReconcileState *pending, PgracFencedJournalRecordV1 *record,
				   bool rejoin)
{
	PgracExternalFenceProtocolBindingV1 binding;
	uint8 frame[768];
	PgracFencedJournalRecordV1 decoded;

	memset(record, 0, sizeof(*record));
	record->record_kind = PGRAC_FENCED_JOURNAL_KIND_ACTUATION_ISSUED;
	memset(record->operation_id, 0x91, 16);
	memset(record->daemon_boot_id, 0x92, 16);
	record->provider_id = 257;
	record->provider_abi_version = 1;
	record->provider_result = PGRAC_FENCED_PROVIDER_PENDING;
	record->event_mono_ns = 1; /* Old daemon clock is not a deadline. */
	record->mapping_generation = context->config->mapping_generation;
	memcpy(record->semantic_config_digest, context->semantic_config_digest, 32);
	record->intent.kind = PGRAC_FENCED_JOURNAL_INTENT_ACQUIRE;
	record->intent.attempt = 7;
	record->intent.system_identifier = context->config->system_identifier;
	memcpy(record->intent.target_uuid, context->config->nodes[2].target_uuid, 16);
	memcpy(record->intent.protected_set_digest, context->config->nodes[2].protected_set_digest, 32);
	make_request(context->config, &record->intent.request.acquire);
	record->intent.request.acquire.need.victim_node_id = 2;
	memcpy(record->intent.request.acquire.need.protected_set_digest,
		   record->intent.protected_set_digest, 32);
	if (!pgrac_external_fence_binding_from_request_v1(&record->intent.request.acquire.need,
													  record->mapping_generation, &binding)
		|| !pgrac_external_fence_binding_digest_v1(&binding, record->binding_digest))
		return false;
	if (rejoin) {
		PgracExternalFenceProtocolRejoinFrameV1 *request = &record->intent.request.rejoin;
		PgracExternalFenceProtocolRejoinBindingV1 rejoin_binding;

		record->record_kind = PGRAC_FENCED_JOURNAL_KIND_INVALIDATED;
		record->intent.kind = PGRAC_FENCED_JOURNAL_INTENT_REJOIN;
		memset(&record->intent.request, 0, sizeof(record->intent.request));
		memset(&rejoin_binding, 0, sizeof(rejoin_binding));
		request->opcode = PGRAC_EXTERNAL_FENCE_REJOIN_ADMIN_PREPARE;
		memset(request->transport_nonce, 0x44, 16);
		request->timeout_ms = 1000;
		request->old_node_id = rejoin_binding.old_node_id = 2;
		request->old_incarnation = rejoin_binding.old_incarnation = 91;
		request->candidate_incarnation = rejoin_binding.candidate_incarnation = 92;
		rejoin_binding.system_identifier = context->config->system_identifier;
		rejoin_binding.target_mapping_generation = context->config->mapping_generation;
		rejoin_binding.predicate_id = 2;
		rejoin_binding.predicate_version = 1;
		memcpy(rejoin_binding.protected_set_digest, record->intent.protected_set_digest, 32);
		if (!pgrac_external_fence_rejoin_binding_digest_v1(&rejoin_binding, record->binding_digest))
			return false;
	}
	if (!pgrac_fenced_operation_append_journal(context, record)
		|| pread(context->journal_fd, frame, sizeof(frame), 256) != sizeof(frame)
		|| !pgrac_fenced_journal_record_decode(frame, sizeof(frame), &decoded))
		return false;
	pgrac_fenced_journal_reconcile_state_init(pending);
	return pgrac_fenced_journal_reconcile_observe(pending, &decoded);
}

UT_TEST(test_v2_startup_preserves_exact_work_and_refuses_mapping_drift)
{
	PgracFencedConfigV1 original;
	PgracFencedProviderOpsV1 ops;
	PgracFencedConfigResult parsed;

	parsed = pgrac_fenced_config_parse((const uint8 *)fenced_config_v2,
									   sizeof(fenced_config_v2) - 1, &original);
#ifndef USE_OPENSSL
	UT_ASSERT_NE(parsed, PGRAC_FENCED_CONFIG_OK);
	return;
#else
	UT_ASSERT_EQ(parsed, PGRAC_FENCED_CONFIG_OK);
	if (parsed != PGRAC_FENCED_CONFIG_OK)
		return;
#endif
	make_ops(&ops);
	ops.provider_id = 257;
	resolve_calls = 0;
	for (int variant = 0; variant < 7; ++variant) {
		PgracFencedConfigV1 config = original;
		PgracFencedOperationContextV1 context;
		PgracFencedJournalScanState journal;
		PgracFencedJournalReconcileState pending;
		PgracFencedJournalRecordV1 record;
		struct stat before, after;
		char path[64];
		int fd = open_context(&context, &journal, &config, &ops, path);

		if (fd < 0)
			return;
		UT_ASSERT(seed_owned_pending(&context, &pending, &record, variant == 6));
		UT_ASSERT_EQ(fstat(fd, &before), 0);
		switch (variant) {
		case 1:
			config.nodes[2].target_uuid[0] ^= 1;
			break;
		case 2:
			config.nodes[2].protected_set_digest[0] ^= 1;
			break;
		case 3:
			config.system_identifier++;
			break;
		case 4:
			config.mapping_generation++;
			break;
		case 5:
			context.semantic_config_digest[0] ^= 1;
			break;
		}
		UT_ASSERT_EQ(pgrac_fenced_operation_reconcile_startup(&context, &pending),
					 variant == 0 || variant == 6);
		UT_ASSERT_EQ(pending.pending_count, 1);
		UT_ASSERT(memcmp(&pending.pending[0].last_record, &record, sizeof(record)) == 0);
		UT_ASSERT_EQ(fstat(fd, &after), 0);
		UT_ASSERT_EQ(before.st_size, after.st_size);
		UT_ASSERT_EQ(resolve_calls, 0);
		if (variant == 0 || variant == 6) {
			UT_ASSERT(context.restart_fresh_readback_required);
			UT_ASSERT_EQ(context.restart_keep_write_disabled, variant == 6);
		} else
			UT_ASSERT(!context.available);
		(void)close(fd);
		(void)unlink(path);
	}
}

UT_TEST(test_v2_acquire_persists_one_complete_identity_for_all_events)
{
	PgracFencedConfigV1 config;
	PgracFencedProviderOpsV1 ops;
	PgracFencedConfigResult parsed;

	parsed = pgrac_fenced_config_parse((const uint8 *)fenced_config_v2,
									   sizeof(fenced_config_v2) - 1, &config);
#ifndef USE_OPENSSL
	UT_ASSERT_NE(parsed, PGRAC_FENCED_CONFIG_OK);
	return;
#else
	UT_ASSERT_EQ(parsed, PGRAC_FENCED_CONFIG_OK);
	if (parsed != PGRAC_FENCED_CONFIG_OK)
		return;
#endif
	make_ops(&ops);
	ops.provider_id = PGRAC_FENCED_PROVIDER_ID_PACEMAKER_LIBVIRT_V1;
	provider_mode = TEST_PROVIDER_EXACT;
	for (int cancel = 0; cancel < 2; ++cancel) {
		PgracFencedOperationContextV1 context;
		PgracFencedJournalScanState journal;
		PgracFencedJournalReconcileState pending;
		PgracFencedPreparedAcquireV1 prepared;
		PgracExternalFenceProtocolRequestV1 request;
		PgracExternalFenceProtocolResponseV1 response;
		PgracFencedJournalRecordV1 records[8];
		uint8 expected[160], actual[160];
		char path[64];
		size_t count;
		int fd = open_context(&context, &journal, &config, &ops, path);

		if (fd < 0)
			return;
		make_request(&config, &request);
		request.need.victim_node_id = 2;
		memcpy(request.need.protected_set_digest, config.nodes[2].protected_set_digest, 32);
		UT_ASSERT(pgrac_external_fence_request_v1_encode(&request, expected));
		UT_ASSERT_EQ(pgrac_fenced_operation_accept(&context, &request, deadline_after_ms(1000),
												   &prepared, &response),
					 PGRAC_FENCED_OPERATION_READY);
		count = read_journal_records(fd, records, lengthof(records));
		UT_ASSERT_EQ(count, 2);
		UT_ASSERT_EQ(records[1].intent.kind, PGRAC_FENCED_JOURNAL_INTENT_ACQUIRE);
		UT_ASSERT(memcmp(records[1].operation_id, request.request_nonce, 16) != 0);
		if (cancel)
			UT_ASSERT(pgrac_fenced_operation_cancel_preaccepted(&context, &request, &prepared, 16,
																&response));
		else {
			owned_journal_path = path;
			UT_ASSERT(pgrac_fenced_operation_execute_preaccepted(
				&context, &request, &prepared, deadline_after_ms(1000), &response));
			owned_journal_path = NULL;
			UT_ASSERT_EQ(response.verdict, 1);
		}
		count = read_journal_records(fd, records, lengthof(records));
		UT_ASSERT_EQ(count, cancel ? 3 : 6);
		pgrac_fenced_journal_reconcile_state_init(&pending);
		for (size_t i = 1; i < count; ++i) {
			if (cancel && i + 1 == count) {
				UT_ASSERT_EQ(records[i].intent.kind, PGRAC_FENCED_JOURNAL_INTENT_NONE);
				UT_ASSERT(memcmp(records[i].operation_id, request.request_nonce, 16) == 0);
				UT_ASSERT(pgrac_fenced_journal_reconcile_observe(&pending, &records[i]));
				continue;
			}
			UT_ASSERT_EQ(records[i].intent.kind, PGRAC_FENCED_JOURNAL_INTENT_ACQUIRE);
			UT_ASSERT_EQ(records[i].intent.attempt, 1);
			UT_ASSERT_EQ(records[i].intent.system_identifier, config.system_identifier);
			UT_ASSERT(memcmp(records[i].operation_id, records[1].operation_id, 16) == 0);
			UT_ASSERT(memcmp(records[i].intent.target_uuid, config.nodes[2].target_uuid, 16) == 0);
			UT_ASSERT(
				pgrac_external_fence_request_v1_encode(&records[i].intent.request.acquire, actual));
			UT_ASSERT(memcmp(actual, expected, sizeof(actual)) == 0);
			UT_ASSERT(pgrac_fenced_journal_reconcile_observe(&pending, &records[i]));
		}
		UT_ASSERT_EQ(pending.pending_count, cancel ? 1 : 0);
		(void)close(fd);
		(void)unlink(path);
	}
}

UT_TEST(test_v2_prepared_handle_rejects_changed_caller_identity_without_append)
{
	PgracFencedConfigV1 config;
	PgracFencedProviderOpsV1 ops;
	PgracFencedOperationContextV1 context;
	PgracFencedJournalScanState journal;
	PgracFencedPreparedAcquireV1 prepared;
	PgracExternalFenceProtocolRequestV1 request, changed;
	PgracExternalFenceProtocolResponseV1 response;
	struct stat before, after;
	char path[64];
	int fd;

#ifndef USE_OPENSSL
	return;
#endif
	UT_ASSERT_EQ(pgrac_fenced_config_parse((const uint8 *)fenced_config_v2,
										   sizeof(fenced_config_v2) - 1, &config),
				 PGRAC_FENCED_CONFIG_OK);
	make_ops(&ops);
	ops.provider_id = PGRAC_FENCED_PROVIDER_ID_PACEMAKER_LIBVIRT_V1;
	fd = open_context(&context, &journal, &config, &ops, path);
	if (fd < 0)
		return;
	make_request(&config, &request);
	request.need.victim_node_id = 2;
	memcpy(request.need.protected_set_digest, config.nodes[2].protected_set_digest, 32);
	UT_ASSERT_EQ(pgrac_fenced_operation_accept(&context, &request, deadline_after_ms(1000),
											   &prepared, &response),
				 PGRAC_FENCED_OPERATION_READY);
	UT_ASSERT_EQ(fstat(fd, &before), 0);
	resolve_calls = 0;
	for (int variant = 0; variant < 2; ++variant) {
		changed = request;
		if (variant == 0)
			changed.request_nonce[0] ^= 1;
		else
			changed.timeout_ms++;
		UT_ASSERT(!pgrac_fenced_operation_execute_preaccepted(&context, &changed, &prepared,
															  deadline_after_ms(1000), &response));
		UT_ASSERT_EQ(fstat(fd, &after), 0);
		UT_ASSERT_EQ(before.st_size, after.st_size);
		UT_ASSERT_EQ(resolve_calls, 0);
	}
	(void)close(fd);
	(void)unlink(path);
}

UT_TEST(test_v2_restart_persists_exact_successor_before_redrive)
{
	PgracFencedConfigV1 config;
	PgracFencedProviderOpsV1 ops;
	PgracFencedOperationContextV1 context;
	PgracFencedJournalScanState journal;
	PgracFencedJournalReconcileState pending;
	PgracFencedPreparedAcquireV1 prepared;
	PgracFencedJournalRecordV1 original, records[10];
	PgracExternalFenceProtocolResponseV1 response;
	struct stat before, after;
	char path[64];
	int fd;
	size_t count;

#ifndef USE_OPENSSL
	return;
#endif
	UT_ASSERT_EQ(pgrac_fenced_config_parse((const uint8 *)fenced_config_v2,
										   sizeof(fenced_config_v2) - 1, &config),
				 PGRAC_FENCED_CONFIG_OK);
	make_ops(&ops);
	ops.provider_id = PGRAC_FENCED_PROVIDER_ID_PACEMAKER_LIBVIRT_V1;
	fd = open_context(&context, &journal, &config, &ops, path);
	if (fd < 0)
		return;
	UT_ASSERT(seed_owned_pending(&context, &pending, &original, false));
	UT_ASSERT(pgrac_fenced_operation_reconcile_startup(&context, &pending));
	UT_ASSERT_EQ(fstat(fd, &before), 0);
	UT_ASSERT_EQ(pgrac_fenced_operation_prepare(&context, &original.intent.request.acquire,
												deadline_after_ms(1000), &prepared, &response),
				 PGRAC_FENCED_OPERATION_READY);
	UT_ASSERT_EQ(prepared.accepted_journal_seq, 0);
	UT_ASSERT_EQ(fstat(fd, &after), 0);
	UT_ASSERT_EQ(before.st_size, after.st_size);
	UT_ASSERT_EQ(pgrac_fenced_operation_resume_acquire(&context, &original, deadline_after_ms(1000),
													   &prepared, &response),
				 PGRAC_FENCED_OPERATION_READY);
	count = read_journal_records(fd, records, lengthof(records));
	UT_ASSERT_EQ(count, 3);
	UT_ASSERT_EQ(records[count - 1].intent.attempt, original.intent.attempt + 1);
	UT_ASSERT(memcmp(records[count - 1].operation_id, original.operation_id, 16) == 0);
	UT_ASSERT(memcmp(records[count - 1].daemon_boot_id, context.daemon_boot_id, 16) == 0);
	UT_ASSERT(pgrac_fenced_journal_intent_continues(&original, &records[count - 1]));
	shared_provider_counts
		= mmap(NULL, 2 * sizeof(uint32), PROT_READ | PROT_WRITE, MAP_SHARED | MAP_ANON, -1, 0);
	UT_ASSERT(shared_provider_counts != MAP_FAILED);
	if (shared_provider_counts != MAP_FAILED) {
		shared_provider_counts[0] = shared_provider_counts[1] = 0;
		provider_mode = TEST_PROVIDER_EXACT;
		owned_journal_path = path;
		UT_ASSERT(pgrac_fenced_operation_execute_preaccepted(
			&context, &original.intent.request.acquire, &prepared, deadline_after_ms(1000),
			&response));
		owned_journal_path = NULL;
		UT_ASSERT_EQ(response.verdict, 1);
		/* Old OFF is not an actuation for this attempt, even with exact readback. */
		UT_ASSERT_EQ(shared_provider_counts[0], 1);
		(void)munmap((void *)shared_provider_counts, 2 * sizeof(uint32));
	}
	shared_provider_counts = NULL;
	(void)close(fd);
	(void)unlink(path);
}

UT_TEST(test_v2_resume_refuses_wrong_target_completed_work_and_attempt_overflow)
{
	PgracFencedConfigV1 config;
	PgracFencedProviderOpsV1 ops;
	PgracFencedOperationContextV1 context;
	PgracFencedJournalScanState journal;
	PgracFencedJournalReconcileState pending;
	PgracFencedPreparedAcquireV1 prepared;
	PgracFencedJournalRecordV1 original, wrong;
	PgracExternalFenceProtocolResponseV1 response;
	struct stat before, after;
	char path[64];
	int fd;

#ifndef USE_OPENSSL
	return;
#endif
	UT_ASSERT_EQ(pgrac_fenced_config_parse((const uint8 *)fenced_config_v2,
										   sizeof(fenced_config_v2) - 1, &config),
				 PGRAC_FENCED_CONFIG_OK);
	make_ops(&ops);
	ops.provider_id = PGRAC_FENCED_PROVIDER_ID_PACEMAKER_LIBVIRT_V1;
	fd = open_context(&context, &journal, &config, &ops, path);
	if (fd < 0)
		return;
	UT_ASSERT(seed_owned_pending(&context, &pending, &original, false));
	UT_ASSERT_EQ(fstat(fd, &before), 0);
	resolve_calls = 0;
	for (int variant = 0; variant < 4; ++variant) {
		wrong = original;
		if (variant == 0)
			wrong.intent.target_uuid[0] ^= 1;
		if (variant == 1)
			wrong.intent.attempt = UINT64_MAX;
		if (variant == 2)
			wrong.record_kind = PGRAC_FENCED_JOURNAL_KIND_PROOF_SERVED;
		if (variant == 3)
			wrong.semantic_config_digest[0] ^= 1;
		memset(&prepared, 0xee, sizeof(prepared));
		UT_ASSERT_EQ(pgrac_fenced_operation_resume_acquire(
						 &context, &wrong, deadline_after_ms(1000), &prepared, &response),
					 PGRAC_FENCED_OPERATION_ERROR);
		UT_ASSERT_EQ(prepared.accepted_journal_seq, 0);
		UT_ASSERT_EQ(fstat(fd, &after), 0);
		UT_ASSERT_EQ(before.st_size, after.st_size);
		UT_ASSERT_EQ(resolve_calls, 0);
	}
	(void)close(fd);
	(void)unlink(path);
}

int
main(void)
{
	UT_PLAN(22);
	UT_RUN(test_scalar_acquire_fsyncs_exact_positive_sequence);
	UT_RUN(test_action_failure_still_accepts_independent_positive_readback);
	UT_RUN(test_scalar_readback_retries_transient_results_before_proof);
	UT_RUN(test_resolve_mismatch_and_persistent_unknown_never_prove);
	UT_RUN(test_protected_set_mismatch_has_zero_request_journal_and_action);
	UT_RUN(test_deadline_owner_maps_slow_resolve_to_timeout);
	UT_RUN(test_deadline_owner_maps_slow_action_and_readback_to_timeout);
	UT_RUN(test_journal_failure_is_sticky_and_prevents_provider_calls);
	UT_RUN(test_malformed_request_is_bad_argument_without_side_effects);
	UT_RUN(test_provider_unavailable_mapping_is_exact_in_journal_and_response);
	UT_RUN(test_operation_rotates_full_active_before_next_durable_record);
	UT_RUN(test_preaccepted_execution_has_one_request_record);
	UT_RUN(test_joiner_gets_shared_durable_proof_from_owner_readback);
	UT_RUN(test_restart_uncertainty_forces_readback_before_new_off_action);
	UT_RUN(test_startup_reconcile_fsyncs_diagnostic_without_provider_action);
	UT_RUN(test_mapping_reload_is_durable_before_activation);
	UT_RUN(test_v2_acquire_never_falls_back_to_storage_only_identity);
	UT_RUN(test_v2_startup_preserves_exact_work_and_refuses_mapping_drift);
	UT_RUN(test_v2_acquire_persists_one_complete_identity_for_all_events);
	UT_RUN(test_v2_prepared_handle_rejects_changed_caller_identity_without_append);
	UT_RUN(test_v2_restart_persists_exact_successor_before_redrive);
	UT_RUN(test_v2_resume_refuses_wrong_target_completed_work_and_attempt_overflow);
	UT_DONE();

	return ut_failed_count == 0 ? 0 : 1;
}
