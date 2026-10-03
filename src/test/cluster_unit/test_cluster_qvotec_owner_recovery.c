/* Reuse the real voting-disk fixture and product objects, without changing
 * the original suite or adding another product entry point. */
int qvotec_base_suite_main(void);
#define main qvotec_base_suite_main
#include "test_cluster_qvotec.c"
#undef main

static void
owner_recover_chosen_before_settle(bool different_request, bool cold_shmem)
{
	EpochPhase1Fixture f;
	ClusterQvotecWriterHandoff handoff;
	ClusterQvotecWriterIdentity old_owner, new_owner;
	ClusterQvotecMailboxRequest request, retry;
	ClusterQvotecMailboxCompletion done, untouched;
	ClusterEpochAuthorityValue decoded;
	ClusterQvotecBallotOwner *slot;
	uint64 writes;
	int tick;

	if (!epoch_phase1_open(&f))
		return;
	shmem_init_done = false;
	cluster_qvotec_shmem_init();
	handoff = owner_fixture_handoff();
	old_owner = handoff.next_owner;
	UT_ASSERT(cluster_qvotec_test_owner_handoff(&handoff));
	request = epoch_actor_request(&f);
	UT_ASSERT(owner_fixture_begin(&f, &old_owner, &request));
	memset(&done, 0xa5, sizeof(done));
	untouched = done;
	for (tick = 0; tick < 4; tick++)
		UT_ASSERT(!cluster_qvotec_test_owner_step(&old_owner, f.disks.fds, PGSA_TEST_DISKS,
												  f.admitted, &request, &done));
	slot = cluster_qvotec_test_ballot_owner();
	UT_ASSERT_EQ(slot->actor.phase, CLUSTER_QVOTEC_ACTOR_SETTLE_WRITE);
	UT_ASSERT_EQ(memcmp(&done, &untouched, sizeof(done)), 0);
	/* The full ACCEPT and post-write scan succeeded, but no SETTLED write ran. */
	epoch_expect_recovered(&f, CLUSTER_QVOTEC_MAILBOX_ADOPTED_OTHER, &f.requested);
	writes = epoch_write_calls;
	cluster_qvotec_shmem_init();
	UT_ASSERT_EQ(slot->actor.phase, CLUSTER_QVOTEC_ACTOR_SETTLE_WRITE);
	UT_ASSERT_EQ(epoch_write_calls, writes);
	handoff = owner_fixture_handoff();
	if (cold_shmem) {
		/* Model a NEW shared-memory allocation, not an attach or a live crash TAP.
		 * The external cold-start qualification is supplied by the fixture.
		 * Only the real voting files retain the chosen request. */
		shmem_init_done = false;
		cluster_qvotec_shmem_init();
		UT_ASSERT_EQ(slot->actor.phase, CLUSTER_QVOTEC_ACTOR_IDLE);
		UT_ASSERT_EQ(slot->identity.owner_generation, 0);
		UT_ASSERT(!cluster_qvotec_test_owner_handoff(&handoff));
		handoff = owner_fixture_handoff();
		handoff.next_owner.postmaster_generation++;
		handoff.next_owner.formation++;
	}
	handoff.next_owner.pid++;
	MyProcPid = handoff.next_owner.pid;
	new_owner = handoff.next_owner;
	UT_ASSERT(cluster_qvotec_test_owner_handoff(&handoff));
	UT_ASSERT_EQ(epoch_write_calls, writes);
	UT_ASSERT(!cluster_qvotec_test_owner_step(&old_owner, f.disks.fds, PGSA_TEST_DISKS, f.admitted,
											  &request, &done));
	UT_ASSERT_EQ(epoch_write_calls, writes);
	UT_ASSERT(!cluster_qvotec_test_owner_handoff(&handoff));

	retry = request;
	retry.request_seq += 2;
	if (different_request) {
		decoded = f.requested;
		decoded.request_nonce++;
		UT_ASSERT(cluster_epoch_authority_value_encode(
			&decoded, CLUSTER_EPOCH_BALLOT_GRAMMAR_FINGERPRINT, retry.request_value));
	}
	UT_ASSERT(owner_fixture_begin(&f, &new_owner, &retry));
	UT_ASSERT(owner_fixture_finish(&f, &new_owner, &retry, &done));
	UT_ASSERT_EQ(done.result, different_request ? CLUSTER_QVOTEC_MAILBOX_ADOPTED_OTHER
												: CLUSTER_QVOTEC_MAILBOX_CHOSEN);
	UT_ASSERT(cluster_epoch_authority_value_decode(
		done.completion_value, CLUSTER_EPOCH_BALLOT_GRAMMAR_FINGERPRINT, &decoded));
	UT_ASSERT_EQ(decoded.request_nonce, f.requested.request_nonce);
	UT_ASSERT_EQ(decoded.request_origin_node, f.requested.request_origin_node);
	UT_ASSERT_EQ(decoded.authority_generation, f.requested.authority_generation);
	epoch_expect_recovered(&f, CLUSTER_QVOTEC_MAILBOX_CHOSEN, &f.requested);
	MyBackendType = B_INVALID;
	MyProcPid = 0;
	pgsa_disk_set_close(&f.disks);
}

UT_TEST(test_selected_unsettled_request_survives_owner_handoff)
{
	owner_recover_chosen_before_settle(false, false);
}

UT_TEST(test_new_owner_cannot_replace_selected_unsettled_request)
{
	owner_recover_chosen_before_settle(true, false);
}

UT_TEST(test_new_shmem_recovers_selected_request_from_voting_disks)
{
	owner_recover_chosen_before_settle(false, true);
}

int
main(void)
{
	UT_PLAN(3);
	UT_RUN(test_selected_unsettled_request_survives_owner_handoff);
	UT_RUN(test_new_owner_cannot_replace_selected_unsettled_request);
	UT_RUN(test_new_shmem_recovers_selected_request_from_voting_disks);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
