/*-------------------------------------------------------------------------
 *
 * test_cluster_ko_ready_owner.c
 *    Exact READY evidence across the original native completion owner.
 *
 * Portions Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *    src/test/cluster_unit/test_cluster_ko_ready_owner.c
 *
 * NOTES
 *    PGRAC-original tests. The original KO producer, ACK handler and native
 *    handoff run here; GES, transport, SPACE IO and membership are fixtures.
 *
 *-------------------------------------------------------------------------
 */
int ko_stop_fixture_main(void);
#define main ko_stop_fixture_main
#include "test_cluster_ko_stop.c"
#undef main

UT_TEST(test_native_private_proof_keeps_exact_ready_set)
{
	ClusterKoCompletionV2 *owner = v3_native_structure_owner(CLUSTER_KO_SHARED_V3_OPERATION_DROP);
	ClusterKoSharedContext snapshot;

	UT_ASSERT_EQ(storage.contexts[owner->slot].request_done_peers, 2);
	UT_ASSERT(ko_completion_snapshot(owner, &snapshot));
	UT_ASSERT_EQ(snapshot.request_done_peers, 2);
	UT_ASSERT_EQ(snapshot.request_sent_peers, 2);
	UT_ASSERT_EQ(snapshot.structure_peers_accepted, 0);
	UT_ASSERT(memcmp(&snapshot.request_v3, &owner->barrier_request_v3, sizeof(snapshot.request_v3))
			  == 0);
	cluster_ko_shared_release_v2(&owner);
}

UT_TEST(test_native_owner_refuses_incomplete_or_failed_ready_set)
{
	for (unsigned fault = 0; fault < 7; fault++) {
		KoAxBinding binding = origin_marker_setup();
		ClusterKoCompletionV2 *owner = NULL;
		ClusterKoSharedContext *entry;

		UT_ASSERT(ko_shared_begin_v3(space_identity.key.locator, RELPERSISTENCE_PERMANENT,
									 CLUSTER_KO_SHARED_V3_OPERATION_DROP, &binding, &owner));
		owner->native_transaction = true;
		entry = &storage.contexts[owner->slot];
		UT_ASSERT(entry->complete && entry->request_done_peers == 2);
		switch (fault) {
		case 0:
			entry->request_done_peers = 0;
			break;
		case 1:
			entry->request_done_peers |= 4;
			break;
		case 2:
			entry->request_failed_peers = 2;
			break;
		case 3:
			entry->request_uncertain_peers = 2;
			break;
		case 4:
			entry->request_sending_peers = 2;
			break;
		case 5:
			entry->request_sent_peers = 0;
			break;
		case 6:
			entry->origin_detached = true;
			break;
		}
		UT_ASSERT(!ko_native_barrier_to_owner(owner));
		UT_ASSERT(!owner->barrier_private);
		UT_ASSERT(entry->used);
		cluster_ko_shared_release_v2(&owner);
	}
}

/* The original producer supplies both the full result and the typed record.
 * A fixture supplies durable SPACE observation, not the barrier evidence. */
static uint32
publish_native_ready(bool drop)
{
	ClusterKoCompletionV2 *owner = v3_native_structure_owner(
		drop ? CLUSTER_KO_SHARED_V3_OPERATION_DROP : CLUSTER_KO_SHARED_V3_OPERATION_TRUNCATE);
	ClusterKoCompletionV2 *claimed = NULL;
	ClusterPageWalBindingV1 terminal;
	uint8 wal[CLUSTER_SPACE_STRUCTURE_WAL_BYTES];
	uint32 selected = CLUSTER_KO_SHARED_CAPACITY;

	UT_ASSERT(cluster_ko_shared_claim_structure(
		&space_identity, drop ? CLUSTER_SPACE_WAL_TOMBSTONE : CLUSTER_SPACE_WAL_TRUNCATE,
		&claimed));
	UT_ASSERT(claimed == owner);
	v3_structure_record(drop, space_identity.operation, &terminal, wal);
	UT_ASSERT(cluster_ko_shared_observe_space_v2(claimed, &terminal, wal, sizeof(wal)));
	if (!drop)
		UT_ASSERT(cluster_ko_shared_observe_truncate_v2(claimed));
	native_commit_active = true;
	cluster_ko_shared_native_commit_v2();
	/* This direct handoff fixture now includes the real publication boundary.
	 * The origin-marker test separately executes the extracted native caller. */
	cluster_ko_shared_native_publication_v3();
	UT_ASSERT(owner->native_handed_off);
	if (drop)
		cluster_ko_shared_native_drop_buffers_released_v2(space_identity.key.locator);
	MyBackendType = B_CHECKPOINTER;
	UT_ASSERT_EQ(cluster_ko_shared_native_promote_v2(), CLUSTER_KO_STRUCTURE_PROGRESS);
	for (uint32 i = 0; i < CLUSTER_KO_SHARED_CAPACITY; i++)
		if (storage.contexts[i].used && storage.contexts[i].structure_owned) {
			UT_ASSERT_EQ(selected, CLUSTER_KO_SHARED_CAPACITY);
			selected = i;
		}
	UT_ASSERT(selected < CLUSTER_KO_SHARED_CAPACITY);
	return selected;
}

UT_TEST(test_commit_continuation_preserves_ready_without_legacy_accept)
{
	for (unsigned drop = 0; drop < 2; drop++) {
		uint32 slot = publish_native_ready(drop != 0);
		ClusterKoSharedContext *entry = &storage.contexts[slot];

		UT_ASSERT(entry->complete && entry->v3 && entry->structure_owned);
		UT_ASSERT_EQ(entry->request_done_peers, 2);
		UT_ASSERT_EQ(entry->request_sent_peers, 2);
		UT_ASSERT_EQ(entry->structure_peers_accepted, 0);
		UT_ASSERT_EQ(entry->structure_drop_pending, drop != 0);
		native_fixture_shared_memory_end();
	}
}

UT_TEST(test_drop_executor_cannot_consume_lost_ready_evidence)
{
	uint32 slot = publish_native_ready(true), cursor = 0;
	ClusterKoDropWorkV2 *work = NULL;

	storage.contexts[slot].request_done_peers = 0;
	UT_ASSERT(!cluster_ko_shared_drop_work_begin_v2(&cursor, 16, &work));
	UT_ASSERT(work == NULL);
	UT_ASSERT_EQ(storage.contexts[slot].drop_executor_pid, 0);
	UT_ASSERT(storage.contexts[slot].structure_drop_pending);
	/* Test cleanup is not product retirement of the retained obligation. */
	if (work != NULL) {
		ko_drop_work[slot] = NULL;
		pfree(work);
	}
	native_fixture_shared_memory_end();
}

UT_TEST(test_structure_consumer_cannot_replace_missing_ready_with_complete)
{
	uint32 slot = publish_native_ready(false), cursor = 0;
	uint64 serial = 0;
	ClusterPageWalBindingV1 terminal;

	storage.contexts[slot].request_done_peers = 0;
	UT_ASSERT(!cluster_ko_shared_structure_next_v2(&cursor, &serial, &terminal));
	UT_ASSERT(storage.contexts[slot].used && storage.contexts[slot].complete);
	native_fixture_shared_memory_end();
}

UT_TEST(test_sql_wait_notification_is_not_exact_ready_authority)
{
	KoAxBinding binding = origin_marker_setup();
	ClusterKoCompletionV2 *owner = NULL;
	volatile bool caught = false;

	/* The wait-table boundary says complete without delivering any KO result.
	 * Product must retain the operation and honor the original cancellation. */
	drive_v3_ack = false;
	barrier_complete = barrier_clock_wait = true;
	barrier_interrupt_at = 1;
	barrier_interrupt_sqlstate = ERRCODE_QUERY_CANCELED;
	PG_TRY();
	{
		(void)ko_shared_begin_v3(space_identity.key.locator, RELPERSISTENCE_PERMANENT,
								 CLUSTER_KO_SHARED_V3_OPERATION_DROP, &binding, &owner);
	}
	PG_CATCH();
	{
		caught = true;
	}
	PG_END_TRY();
	UT_ASSERT(caught);
	UT_ASSERT_EQ(reported_sqlstate, ERRCODE_QUERY_CANCELED);
	UT_ASSERT_EQ(record_count, 0);
	UT_ASSERT(!storage.contexts[0].complete);
	if (owner != NULL)
		cluster_ko_shared_release_v2(&owner);
}

UT_TEST(test_missing_remote_identity_keeps_original_refusal)
{
	KoAxBinding binding = origin_marker_setup();
	ClusterKoCompletionV2 *owner = NULL;
	volatile bool caught = false;

	formation.membership.membership_state[1] = CLUSTER_MEMBER_ABSENT;
	v3_route_master_node = binding.master_node = 0;
	expecting_error = true;
	PG_TRY();
	{
		(void)ko_shared_begin_v3(space_identity.key.locator, RELPERSISTENCE_PERMANENT,
								 CLUSTER_KO_SHARED_V3_OPERATION_DROP, &binding, &owner);
	}
	PG_CATCH();
	{
		caught = true;
	}
	PG_END_TRY();
	expecting_error = false;
	UT_ASSERT(caught && owner == NULL);
	UT_ASSERT_EQ(reported_sqlstate, ERRCODE_CLUSTER_OBJECT_FLUSH_UNAVAILABLE);
	UT_ASSERT_EQ(barrier_requests, 0);
	UT_ASSERT_EQ(record_count, 0);
}

int
main(void)
{
	UT_PLAN(7);
	UT_RUN(test_native_private_proof_keeps_exact_ready_set);
	UT_RUN(test_native_owner_refuses_incomplete_or_failed_ready_set);
	UT_RUN(test_commit_continuation_preserves_ready_without_legacy_accept);
	UT_RUN(test_drop_executor_cannot_consume_lost_ready_evidence);
	UT_RUN(test_structure_consumer_cannot_replace_missing_ready_with_complete);
	UT_RUN(test_sql_wait_notification_is_not_exact_ready_authority);
	UT_RUN(test_missing_remote_identity_keeps_original_refusal);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
