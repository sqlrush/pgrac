/*-------------------------------------------------------------------------
 *
 * test_pgrac_fenced_core.c
 *	  RF-ROOT P4 daemon peer and total-FSM tests.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "pgrac_fenced_core.h"

#undef printf
#undef fprintf
#undef snprintf

#include "unit_test.h"

UT_DEFINE_GLOBALS();

UT_TEST(test_peer_policy_is_mutual_and_exact)
{
	PgracFencedPeerCredential peer;

	memset(&peer, 0, sizeof(peer));
	peer.uid = 0;
	peer.gid = 0;
	peer.pid = 17;
	peer.pid_known = true;
	UT_ASSERT(pgrac_fenced_peer_is_root(&peer));
	UT_ASSERT(!pgrac_fenced_peer_is_db(&peer, 501, 20));

	peer.uid = 501;
	peer.gid = 20;
	UT_ASSERT(!pgrac_fenced_peer_is_root(&peer));
	UT_ASSERT(pgrac_fenced_peer_is_db(&peer, 501, 20));
	peer.gid = 21;
	UT_ASSERT(!pgrac_fenced_peer_is_db(&peer, 501, 20));
	peer.gid = 20;
	peer.pid = 0;
	UT_ASSERT(!pgrac_fenced_peer_is_db(&peer, 501, 20));
	peer.pid_known = false;
	UT_ASSERT(pgrac_fenced_peer_is_db(&peer, 501, 20));
}

UT_TEST(test_capacity_rejects_129_without_evicting)
{
	UT_ASSERT(pgrac_fenced_capacity_available(0, 0));
	UT_ASSERT(pgrac_fenced_capacity_available(127, 127));
	UT_ASSERT(!pgrac_fenced_capacity_available(128, 0));
	UT_ASSERT(!pgrac_fenced_capacity_available(0, 128));
	UT_ASSERT(!pgrac_fenced_capacity_available(128, 128));
}

UT_TEST(test_recovery_operation_exact_happy_path)
{
	PgracFencedTransition transition;

	UT_ASSERT(pgrac_fenced_fsm_step(PGRAC_FENCED_STATE_IDLE, PGRAC_FENCED_EVENT_REQUEST_TARGET_FREE,
									&transition));
	UT_ASSERT_EQ(transition.next_state, PGRAC_FENCED_STATE_RESOLVING);
	UT_ASSERT_EQ(transition.journal_mask, PGRAC_FENCED_JOURNAL_REQUEST_ACCEPTED);
	UT_ASSERT_EQ(transition.outcome, PGRAC_FENCED_OUTCOME_PENDING);

	UT_ASSERT(pgrac_fenced_fsm_step(transition.next_state, PGRAC_FENCED_EVENT_RESOLVE_EXACT,
									&transition));
	UT_ASSERT_EQ(transition.next_state, PGRAC_FENCED_STATE_ACTUATING);
	UT_ASSERT_EQ(transition.journal_mask, PGRAC_FENCED_JOURNAL_ACTUATION_ISSUED);

	UT_ASSERT(pgrac_fenced_fsm_step(transition.next_state, PGRAC_FENCED_EVENT_ACTUATION_FINISHED,
									&transition));
	UT_ASSERT_EQ(transition.next_state, PGRAC_FENCED_STATE_VERIFYING);
	UT_ASSERT_EQ(transition.journal_mask, PGRAC_FENCED_JOURNAL_ACTUATION_RESULT);

	UT_ASSERT(pgrac_fenced_fsm_step(transition.next_state, PGRAC_FENCED_EVENT_READBACK_OFF_DRAINED,
									&transition));
	UT_ASSERT_EQ(transition.next_state, PGRAC_FENCED_STATE_PROVEN_DURABLE);
	UT_ASSERT_EQ(transition.journal_mask,
				 PGRAC_FENCED_JOURNAL_READBACK_RESULT | PGRAC_FENCED_JOURNAL_PROOF_SERVED);
	UT_ASSERT_EQ(transition.outcome, PGRAC_FENCED_OUTCOME_WRITE_EXCLUDED);
	UT_ASSERT_EQ(transition.deny_reason, PGRAC_FENCED_DENY_NONE);
}

UT_TEST(test_queue_and_negative_readback_are_closed)
{
	PgracFencedTransition transition;

	UT_ASSERT(pgrac_fenced_fsm_step(PGRAC_FENCED_STATE_ACTUATING,
									PGRAC_FENCED_EVENT_REQUEST_NONJOINABLE, &transition));
	UT_ASSERT_EQ(transition.next_state, PGRAC_FENCED_STATE_QUEUED);
	UT_ASSERT_EQ(transition.journal_mask, PGRAC_FENCED_JOURNAL_REQUEST_ACCEPTED);
	UT_ASSERT_EQ(transition.outcome, PGRAC_FENCED_OUTCOME_PENDING);

	UT_ASSERT(pgrac_fenced_fsm_step(PGRAC_FENCED_STATE_QUEUED, PGRAC_FENCED_EVENT_QUEUE_TIMEOUT,
									&transition));
	UT_ASSERT_EQ(transition.next_state, PGRAC_FENCED_STATE_INVALIDATED);
	UT_ASSERT_EQ(transition.outcome, PGRAC_FENCED_OUTCOME_UNKNOWN);
	UT_ASSERT_EQ(transition.deny_reason, PGRAC_FENCED_DENY_TIMEOUT);

	UT_ASSERT(pgrac_fenced_fsm_step(PGRAC_FENCED_STATE_VERIFYING,
									PGRAC_FENCED_EVENT_READBACK_OFF_NOT_DRAINED, &transition));
	UT_ASSERT_EQ(transition.next_state, PGRAC_FENCED_STATE_REJECTED);
	UT_ASSERT_EQ(transition.outcome, PGRAC_FENCED_OUTCOME_REJECTED);
	UT_ASSERT_EQ(transition.deny_reason, PGRAC_FENCED_DENY_IO_NOT_DRAINED);
	UT_ASSERT_EQ(transition.journal_mask, PGRAC_FENCED_JOURNAL_READBACK_RESULT);
}

UT_TEST(test_rejoin_needs_authorize_then_fresh_refresh)
{
	PgracFencedTransition transition;

	UT_ASSERT(pgrac_fenced_fsm_step(PGRAC_FENCED_STATE_IDLE, PGRAC_FENCED_EVENT_ADMIN_PREPARE,
									&transition));
	UT_ASSERT_EQ(transition.next_state, PGRAC_FENCED_STATE_REENABLING);
	UT_ASSERT_EQ(transition.outcome, PGRAC_FENCED_OUTCOME_ADMIN_OFFERED);
	UT_ASSERT_EQ(transition.journal_mask, PGRAC_FENCED_JOURNAL_REENABLE_REQUESTED);

	UT_ASSERT(pgrac_fenced_fsm_step(transition.next_state,
									PGRAC_FENCED_EVENT_LMON_CLAIM_OFF_DRAINED, &transition));
	UT_ASSERT_EQ(transition.outcome, PGRAC_FENCED_OUTCOME_LMON_OFFERED);
	UT_ASSERT_EQ(transition.journal_mask, PGRAC_FENCED_JOURNAL_READBACK_RESULT);

	UT_ASSERT(
		pgrac_fenced_fsm_step(transition.next_state, PGRAC_FENCED_EVENT_AUTHORIZE_ON, &transition));
	UT_ASSERT_EQ(transition.outcome, PGRAC_FENCED_OUTCOME_PENDING);
	UT_ASSERT_EQ(transition.journal_mask, PGRAC_FENCED_JOURNAL_INVALIDATED);

	UT_ASSERT(pgrac_fenced_fsm_step(transition.next_state, PGRAC_FENCED_EVENT_READBACK_ON_DRAINED,
									&transition));
	UT_ASSERT_EQ(transition.outcome, PGRAC_FENCED_OUTCOME_WAITING_JOINER);
	UT_ASSERT_EQ(transition.journal_mask, PGRAC_FENCED_JOURNAL_REENABLE_RESULT);

	UT_ASSERT(pgrac_fenced_fsm_step(transition.next_state, PGRAC_FENCED_EVENT_REFRESH_ON_DRAINED,
									&transition));
	UT_ASSERT_EQ(transition.outcome, PGRAC_FENCED_OUTCOME_READY);
	UT_ASSERT_EQ(transition.journal_mask, PGRAC_FENCED_JOURNAL_REENABLE_RESULT);
}

UT_TEST(test_illegal_state_event_is_fail_closed)
{
	PgracFencedTransition transition;

	UT_ASSERT(!pgrac_fenced_fsm_step(PGRAC_FENCED_STATE_IDLE,
									 PGRAC_FENCED_EVENT_READBACK_OFF_DRAINED, &transition));
	UT_ASSERT_EQ(transition.next_state, PGRAC_FENCED_STATE_UNAVAILABLE);
	UT_ASSERT_EQ(transition.outcome, PGRAC_FENCED_OUTCOME_UNAVAILABLE);
	UT_ASSERT_EQ(transition.deny_reason, PGRAC_FENCED_DENY_PROTOCOL);

	UT_ASSERT(pgrac_fenced_fsm_step(PGRAC_FENCED_STATE_PROVEN_DURABLE,
									PGRAC_FENCED_EVENT_PROOF_INVALIDATED, &transition));
	UT_ASSERT_EQ(transition.next_state, PGRAC_FENCED_STATE_INVALIDATED);
	UT_ASSERT_EQ(transition.outcome, PGRAC_FENCED_OUTCOME_NONE);
	UT_ASSERT_EQ(transition.journal_mask, PGRAC_FENCED_JOURNAL_INVALIDATED);

	UT_ASSERT(pgrac_fenced_fsm_step(PGRAC_FENCED_STATE_VERIFYING,
									PGRAC_FENCED_EVENT_INTEGRITY_FAILURE, &transition));
	UT_ASSERT_EQ(transition.next_state, PGRAC_FENCED_STATE_UNAVAILABLE);
	UT_ASSERT_EQ(transition.outcome, PGRAC_FENCED_OUTCOME_UNAVAILABLE);
	UT_ASSERT_EQ(transition.deny_reason, PGRAC_FENCED_DENY_JOURNAL);
}

int
main(void)
{
	UT_PLAN(6);
	UT_RUN(test_peer_policy_is_mutual_and_exact);
	UT_RUN(test_capacity_rejects_129_without_evicting);
	UT_RUN(test_recovery_operation_exact_happy_path);
	UT_RUN(test_queue_and_negative_readback_are_closed);
	UT_RUN(test_rejoin_needs_authorize_then_fresh_refresh);
	UT_RUN(test_illegal_state_event_is_fail_closed);
	UT_DONE();

	return ut_failed_count == 0 ? 0 : 1;
}
