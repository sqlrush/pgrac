/*-------------------------------------------------------------------------
 *
 * test_cluster_resource_x_retry.c
 *    Resource-X bounded retry state -- spec-8.7 D7-01.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "cluster/cluster_resource_x_retry.h"

#include "unit_test.h"

UT_DEFINE_GLOBALS();

void
ExceptionalCondition(const char *condition_name, const char *file_name, int line_number)
{
	printf("# Assert failed: %s at %s:%d\n", condition_name, file_name, line_number);
	abort();
}

static ResourceXAttemptWitness
make_attempt(uint64 base_generation)
{
	BufferTag tag;
	ResourceXAssertion assertion;
	ResourceXAttemptWitness attempt;

	tag.spcOid = 1663;
	tag.dbOid = 5;
	tag.relNumber = 9001;
	tag.forkNum = MAIN_FORKNUM;
	tag.blockNum = 42;
	UT_ASSERT(resource_x_assertion_init(&tag, 3, &assertion));
	UT_ASSERT(resource_x_attempt_init(&assertion, base_generation, &attempt));
	return attempt;
}

static ResourceXTransportWitness
make_transport(uint32 connection_generation)
{
	ResourceXTransportWitness transport;

	MemSet(&transport, 0, sizeof(transport));
	transport.cluster_epoch = 41;
	transport.peer_session_incarnation = 73;
	transport.connection_generation = connection_generation;
	transport.lane_id = 1;
	return transport;
}

UT_TEST(test_retry_state_layout_is_exact)
{
	UT_ASSERT_EQ(RESOURCE_X_RETRY_PRE_NO_RETURN, 1);
	UT_ASSERT_EQ(RESOURCE_X_RETRY_POST_NO_RETURN, 2);
	UT_ASSERT_EQ(RESOURCE_X_RETRY_TERMINAL, 3);
	UT_ASSERT_EQ(RESOURCE_X_RETRY_PHASE_RECOVERY_BLOCKED, 4);
	UT_ASSERT_EQ(sizeof(ResourceXRetryStateV1), 72);
	UT_ASSERT_EQ(offsetof(ResourceXRetryStateV1, attempt), 0);
	UT_ASSERT_EQ(offsetof(ResourceXRetryStateV1, first_submit_mono_us), 32);
	UT_ASSERT_EQ(offsetof(ResourceXRetryStateV1, next_retry_due_mono_us), 40);
	UT_ASSERT_EQ(offsetof(ResourceXRetryStateV1, terminal_deadline_mono_us), 48);
	UT_ASSERT_EQ(offsetof(ResourceXRetryStateV1, retry_count), 56);
	UT_ASSERT_EQ(offsetof(ResourceXRetryStateV1, terminal_errcode), 60);
	UT_ASSERT_EQ(offsetof(ResourceXRetryStateV1, last_phase), 64);
	UT_ASSERT_EQ(offsetof(ResourceXRetryStateV1, flags), 66);
	UT_ASSERT_EQ(offsetof(ResourceXRetryStateV1, state_generation), 68);
}

UT_TEST(test_retry_state_initialization_publishes_exact_attempt)
{
	ResourceXAttemptWitness attempt = make_attempt(17);
	ResourceXRetryStateV1 state;

	memset(&state, 0xa5, sizeof(state));
	UT_ASSERT(resource_x_retry_state_init(&attempt, 1000, 60000, 4, 10, 7, &state));
	UT_ASSERT(memcmp(&state.attempt, &attempt, sizeof(attempt)) == 0);
	UT_ASSERT_EQ(state.first_submit_mono_us, 1000);
	UT_ASSERT_EQ(state.next_retry_due_mono_us, 11000);
	UT_ASSERT_EQ(state.terminal_deadline_mono_us, 60000);
	UT_ASSERT_EQ(state.retry_count, 0);
	UT_ASSERT_EQ(state.terminal_errcode, 0);
	UT_ASSERT_EQ(state.last_phase, RESOURCE_X_RETRY_PRE_NO_RETURN);
	UT_ASSERT_EQ(state.flags, 20009);
	UT_ASSERT_EQ(state.state_generation, 7);
	UT_ASSERT(!resource_x_retry_state_is_clear(&state));
}

UT_TEST(test_retry_state_initialization_rejects_unpublishable_state)
{
	ResourceXAttemptWitness attempt = make_attempt(17);
	ResourceXRetryStateV1 state;
	ResourceXRetryStateV1 before;

	memset(&state, 0x5a, sizeof(state));
	before = state;
	UT_ASSERT(!resource_x_retry_state_init(NULL, 1000, 6000, 4, 10, 7, &state));
	UT_ASSERT(memcmp(&state, &before, sizeof(state)) == 0);
	UT_ASSERT(!resource_x_retry_state_init(&attempt, 0, 6000, 4, 10, 7, &state));
	UT_ASSERT(!resource_x_retry_state_init(&attempt, 1000, 999, 4, 10, 7, &state));
	UT_ASSERT(!resource_x_retry_state_init(&attempt, 1000, 6000, 9, 10, 7, &state));
	UT_ASSERT(!resource_x_retry_state_init(&attempt, 1000, 6000, 4, 0, 7, &state));
	UT_ASSERT(!resource_x_retry_state_init(&attempt, 1000, 6000, 4, 5001, 7, &state));
	UT_ASSERT(!resource_x_retry_state_init(&attempt, 1000, 6000, 4, 10, 0, &state));
	UT_ASSERT(!resource_x_retry_state_init(&attempt, 1000, 6000, 4, 10, 7, NULL));
	UT_ASSERT(memcmp(&state, &before, sizeof(state)) == 0);
}

UT_TEST(test_retry_state_clear_removes_all_attempt_state)
{
	ResourceXAttemptWitness attempt = make_attempt(17);
	ResourceXRetryStateV1 state;

	UT_ASSERT(resource_x_retry_state_init(&attempt, 1000, 60000, 4, 10, 7, &state));
	resource_x_retry_state_clear(&state);
	UT_ASSERT(resource_x_retry_state_is_clear(&state));
	state.flags = 1;
	UT_ASSERT(!resource_x_retry_state_is_clear(&state));
	resource_x_retry_state_clear(NULL);
	UT_ASSERT(!resource_x_retry_state_is_clear(NULL));
}

UT_TEST(test_classifier_stages_same_attempt_with_fresh_transport)
{
	ResourceXAttemptWitness attempt = make_attempt(17);
	ResourceXTransportWitness transport = make_transport(9);
	ResourceXRetryStateV1 state;
	ResourceXRetryStateV1 before;
	ResourceXRetryAction action;

	UT_ASSERT(resource_x_retry_state_init(&attempt, 1000, 60000, 4, 10, 7, &state));
	before = state;
	MemSet(&action, 0xa5, sizeof(action));
	UT_ASSERT_EQ(resource_x_retry_classify_exact(&state, &attempt, &transport, 11000, &action),
				 RESOURCE_X_RETRY_STAGE_EXACT);
	UT_ASSERT(memcmp(&action.attempt, &attempt, sizeof(attempt)) == 0);
	UT_ASSERT(memcmp(&action.transport, &transport, sizeof(transport)) == 0);
	UT_ASSERT_EQ(action.expected_state_generation, 7);
	UT_ASSERT_EQ(action.expected_retry_count, 0);
	UT_ASSERT(memcmp(&state, &before, sizeof(state)) == 0);

	transport.connection_generation++;
	UT_ASSERT_EQ(resource_x_retry_classify_exact(&state, &attempt, &transport, 11000, &action),
				 RESOURCE_X_RETRY_STAGE_EXACT);
	UT_ASSERT(memcmp(&action.attempt, &attempt, sizeof(attempt)) == 0);
	UT_ASSERT_EQ(action.transport.connection_generation, 10);
}

UT_TEST(test_classifier_never_retries_successor_attempt)
{
	ResourceXAttemptWitness attempt = make_attempt(17);
	ResourceXAttemptWitness successor = make_attempt(18);
	ResourceXTransportWitness transport = make_transport(9);
	ResourceXRetryStateV1 state;
	ResourceXRetryAction action;
	ResourceXRetryAction zero;

	UT_ASSERT(resource_x_retry_state_init(&attempt, 1000, 6000, 4, 1, 7, &state));
	MemSet(&action, 0xa5, sizeof(action));
	MemSet(&zero, 0, sizeof(zero));
	UT_ASSERT_EQ(resource_x_retry_classify_exact(&state, &successor, &transport, 2000, &action),
				 RESOURCE_X_RETRY_RECOVERY_BLOCKED);
	UT_ASSERT(memcmp(&action, &zero, sizeof(action)) == 0);
}

UT_TEST(test_classifier_rejects_unbound_transport_without_staging)
{
	ResourceXAttemptWitness attempt = make_attempt(17);
	ResourceXTransportWitness transport = make_transport(9);
	ResourceXRetryStateV1 state;
	ResourceXRetryAction action;
	ResourceXRetryAction zero;

	UT_ASSERT(resource_x_retry_state_init(&attempt, 1000, 6000, 4, 1, 7, &state));
	MemSet(&zero, 0, sizeof(zero));
	transport.connection_generation = 0;
	UT_ASSERT_EQ(resource_x_retry_classify_exact(&state, &attempt, &transport, 2000, &action),
				 RESOURCE_X_RETRY_WAIT_SCHEDULER);
	UT_ASSERT(memcmp(&action, &zero, sizeof(action)) == 0);
	transport = make_transport(9);
	transport.flags = 1;
	UT_ASSERT_EQ(resource_x_retry_classify_exact(&state, &attempt, &transport, 1010, &action),
				 RESOURCE_X_RETRY_RECOVERY_BLOCKED);
}

UT_TEST(test_retry_policy_snapshot_is_lossless_across_guc_domain)
{
	ResourceXAttemptWitness attempt = make_attempt(17);
	ResourceXRetryStateV1 state;
	uint32 initial_backoff_ms;
	uint32 max_retries;

	UT_ASSERT(
		resource_x_retry_state_init(&attempt, 1000, UINT64_C(6000000000), 8, 5000, 7, &state));
	UT_ASSERT_EQ(state.flags, 44999);
	UT_ASSERT(resource_x_retry_policy_exact(&state, &max_retries, &initial_backoff_ms));
	UT_ASSERT_EQ(max_retries, 8);
	UT_ASSERT_EQ(initial_backoff_ms, 5000);
	UT_ASSERT_EQ(state.next_retry_due_mono_us, UINT64_C(5001000));

	UT_ASSERT(resource_x_retry_state_init(&attempt, 1000, 6000, 0, 1, 7, &state));
	UT_ASSERT_EQ(state.flags, 0);
	UT_ASSERT(resource_x_retry_policy_exact(&state, &max_retries, &initial_backoff_ms));
	UT_ASSERT_EQ(max_retries, 0);
	UT_ASSERT_EQ(initial_backoff_ms, 1);
	UT_ASSERT_EQ(state.next_retry_due_mono_us, 2000);
}

UT_TEST(test_classifier_enforces_sampled_retry_budget)
{
	ResourceXAttemptWitness attempt = make_attempt(17);
	ResourceXTransportWitness transport = make_transport(9);
	ResourceXRetryStateV1 state;
	ResourceXRetryAction action;

	UT_ASSERT(resource_x_retry_state_init(&attempt, 1000, 6000, 4, 1, 7, &state));
	state.retry_count = 3;
	UT_ASSERT_EQ(resource_x_retry_classify_exact(&state, &attempt, &transport, 2000, &action),
				 RESOURCE_X_RETRY_STAGE_EXACT);
	state.retry_count = 4;
	UT_ASSERT_EQ(resource_x_retry_classify_exact(&state, &attempt, &transport, 2000, &action),
				 RESOURCE_X_RETRY_TERMINAL_EXHAUSTED);
	state.last_phase = RESOURCE_X_RETRY_POST_NO_RETURN;
	UT_ASSERT_EQ(resource_x_retry_classify_exact(&state, &attempt, &transport, 2000, &action),
				 RESOURCE_X_RETRY_ROLL_FORWARD);
	state.flags = UINT16_MAX;
	UT_ASSERT_EQ(resource_x_retry_classify_exact(&state, &attempt, &transport, 2000, &action),
				 RESOURCE_X_RETRY_RECOVERY_BLOCKED);
}

UT_TEST(test_exponential_backoff_saturates_and_clamps_to_deadline)
{
	ResourceXAttemptWitness attempt = make_attempt(17);
	ResourceXRetryStateV1 state;
	uint64 next_due;

	UT_ASSERT(resource_x_retry_state_init(&attempt, 1000, UINT64_C(1000000000), 8, 10, 7, &state));
	UT_ASSERT_EQ(state.next_retry_due_mono_us, 11000);
	UT_ASSERT(resource_x_retry_next_due_exact(&state, 20000, 1, &next_due));
	UT_ASSERT_EQ(next_due, 40000);
	UT_ASSERT(resource_x_retry_next_due_exact(&state, 20000, 2, &next_due));
	UT_ASSERT_EQ(next_due, 60000);

	UT_ASSERT(resource_x_retry_state_init(&attempt, 1000, 50000, 8, 10, 7, &state));
	UT_ASSERT(resource_x_retry_next_due_exact(&state, 40000, 1, &next_due));
	UT_ASSERT_EQ(next_due, 50000);
	UT_ASSERT(resource_x_retry_state_init(&attempt, 1000, UINT64_MAX, 8, 5000, 7, &state));
	UT_ASSERT(resource_x_retry_next_due_exact(&state, UINT64_MAX - 5, 8, &next_due));
	UT_ASSERT_EQ(next_due, UINT64_MAX);
	UT_ASSERT(!resource_x_retry_next_due_exact(&state, 20000, 9, &next_due));
}

UT_TEST(test_terminal_transition_is_attempt_generation_and_phase_exact)
{
	ResourceXAttemptWitness attempt = make_attempt(17);
	ResourceXRetryStateV1 current;
	ResourceXRetryStateV1 expected;
	ResourceXRetryStateV1 terminal;
	ResourceXRetryStateV1 before;
	uint32 errcode = ERRCODE_CLUSTER_GCS_BLOCK_RETRANSMIT_EXHAUSTED;

	UT_ASSERT(resource_x_retry_state_init(&attempt, 1000, 6000, 0, 1, 7, &current));
	expected = current;
	UT_ASSERT_EQ(resource_x_retry_terminalize_exact(&current, &expected, errcode, 2000, &terminal),
				 RESOURCE_X_RETRY_APPLY_APPLIED);
	UT_ASSERT_EQ(terminal.last_phase, RESOURCE_X_RETRY_TERMINAL);
	UT_ASSERT_EQ(terminal.terminal_errcode, errcode);
	UT_ASSERT_EQ(terminal.next_retry_due_mono_us, 2000);
	UT_ASSERT_EQ(terminal.terminal_deadline_mono_us, 2000);
	UT_ASSERT_EQ(terminal.state_generation, 8);
	UT_ASSERT_EQ(resource_x_retry_terminalize_exact(&terminal, &expected, errcode, 3000, &current),
				 RESOURCE_X_RETRY_APPLY_DUPLICATE);
	UT_ASSERT(memcmp(&current, &terminal, sizeof(current)) == 0);

	current = expected;
	current.last_phase = RESOURCE_X_RETRY_POST_NO_RETURN;
	before = current;
	UT_ASSERT_EQ(resource_x_retry_terminalize_exact(&current, &current, errcode, 2000, &terminal),
				 RESOURCE_X_RETRY_APPLY_ROLL_FORWARD);
	UT_ASSERT(memcmp(&current, &before, sizeof(current)) == 0);
	current = expected;
	current.state_generation = PG_UINT32_MAX;
	UT_ASSERT_EQ(resource_x_retry_terminalize_exact(&current, &current, errcode, 2000, &terminal),
				 RESOURCE_X_RETRY_APPLY_RECOVERY_BLOCKED);
	current = expected;
	UT_ASSERT_EQ(resource_x_retry_terminalize_exact(&current, &current, ERRCODE_DATA_CORRUPTED,
													2000, &terminal),
				 RESOURCE_X_RETRY_APPLY_RECOVERY_BLOCKED);
}

UT_TEST(test_terminal_reason_codec_is_closed)
{
	UT_ASSERT_EQ(resource_x_terminal_reason_decode(0), RESOURCE_X_TERMINAL_REASON_LEGACY_CANCEL);
	UT_ASSERT_EQ(resource_x_terminal_reason_decode(ERRCODE_CLUSTER_GCS_BLOCK_RETRANSMIT_EXHAUSTED),
				 RESOURCE_X_TERMINAL_REASON_RETRY_EXHAUSTED);
	UT_ASSERT_EQ(resource_x_terminal_reason_decode(ERRCODE_CLUSTER_GCS_BLOCK_INVALIDATE_TIMEOUT),
				 RESOURCE_X_TERMINAL_REASON_INVALIDATE_TIMEOUT);
	UT_ASSERT_EQ(resource_x_terminal_reason_decode(ERRCODE_CLUSTER_LOST_WRITE_DETECTED),
				 RESOURCE_X_TERMINAL_REASON_LOST_WRITE);
	UT_ASSERT_EQ(resource_x_terminal_reason_decode(1), RESOURCE_X_TERMINAL_REASON_INVALID);
	UT_ASSERT_EQ(resource_x_terminal_reason_decode(ERRCODE_DATA_CORRUPTED),
				 RESOURCE_X_TERMINAL_REASON_INVALID);
}

UT_TEST(test_terminal_record_replays_one_exact_attempt_byte_for_byte)
{
	ResourceXAttemptWitness attempt = make_attempt(17);
	ResourceXAttemptWitness successor = make_attempt(18);
	ResourceXRetryStateV1 current;
	ResourceXRetryStateV1 terminal;
	ResourceXRetryStateV1 replay;
	ResourceXTerminalRecordV1 record;

	UT_ASSERT_EQ(sizeof(ResourceXTerminalRecordV1), 72);
	UT_ASSERT(resource_x_retry_state_init(&attempt, 1000, 6000, 4, 1, 7, &current));
	current.retry_count = 3;
	UT_ASSERT_EQ(resource_x_retry_terminalize_exact(&current, &current,
													ERRCODE_CLUSTER_GCS_BLOCK_INVALIDATE_TIMEOUT,
													2000, &terminal),
				 RESOURCE_X_RETRY_APPLY_APPLIED);
	MemSet(&record, 0xa5, sizeof(record));
	UT_ASSERT(resource_x_terminal_record_publish(&terminal, &record));
	UT_ASSERT(resource_x_terminal_record_replay(&record, &attempt, &replay));
	UT_ASSERT(memcmp(&replay, &terminal, sizeof(replay)) == 0);
	UT_ASSERT(!resource_x_terminal_record_replay(&record, &successor, &replay));
	resource_x_terminal_record_clear(&record);
	UT_ASSERT(resource_x_terminal_record_is_clear(&record));
}

int
main(void)
{
	UT_PLAN(13);
	UT_RUN(test_retry_state_layout_is_exact);
	UT_RUN(test_retry_state_initialization_publishes_exact_attempt);
	UT_RUN(test_retry_state_initialization_rejects_unpublishable_state);
	UT_RUN(test_retry_state_clear_removes_all_attempt_state);
	UT_RUN(test_classifier_stages_same_attempt_with_fresh_transport);
	UT_RUN(test_classifier_never_retries_successor_attempt);
	UT_RUN(test_classifier_rejects_unbound_transport_without_staging);
	UT_RUN(test_retry_policy_snapshot_is_lossless_across_guc_domain);
	UT_RUN(test_classifier_enforces_sampled_retry_budget);
	UT_RUN(test_exponential_backoff_saturates_and_clamps_to_deadline);
	UT_RUN(test_terminal_transition_is_attempt_generation_and_phase_exact);
	UT_RUN(test_terminal_reason_codec_is_closed);
	UT_RUN(test_terminal_record_replays_one_exact_attempt_byte_for_byte);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
