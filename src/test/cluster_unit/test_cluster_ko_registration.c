/*-------------------------------------------------------------------------
 * test_cluster_ko_registration.c
 *    Transaction-owned KO registration before physical barrier execution.
 *
 * Spec: 2026-10-16-pre2-p014-am05-shared-ddl-autocommit.md
 * Portions Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * The KO producer and cleanup are real. GES, SPACE reads, invalidation
 * registration and transport are explicit boundaries of the shared fixture.
 *-------------------------------------------------------------------------
 */
int ko_stop_fixture_main(void);
#define main ko_stop_fixture_main
#include "test_cluster_ko_stop.c"
#undef main

static ClusterKoCompletionV2 *
register_operation(KoAxBinding *binding, uint16 operation)
{
	ClusterKoCompletionV2 *owner = NULL;

	*binding = origin_marker_setup();
	cluster_hw_lease_install(space_identity.key.locator, MAIN_FORKNUM, 4, 4);
	UT_ASSERT(ko_shared_begin_v3(space_identity.key.locator, RELPERSISTENCE_PERMANENT,
		operation, binding, &owner));
	if (owner == NULL)
		abort();
	return owner;
}

static void
assert_no_object_work(void)
{
	UT_ASSERT_EQ(barrier_requests, 0);
	UT_ASSERT_EQ(record_count, 0);
	UT_ASSERT_EQ(send_calls, 0);
	UT_ASSERT_EQ(lock_calls, 0);
	UT_ASSERT_EQ(flush_count + sync_count + drop_count, 0);
	UT_ASSERT_EQ(origin_drain_calls + origin_flush_calls + origin_invalidate_calls, 0);
	UT_ASSERT_EQ(storage.send_v3_count, 0);
	UT_ASSERT(!barrier_active);
	UT_ASSERT_EQ(pg_atomic_read_u64(&leases.d_orphan_zero), 0);
	UT_ASSERT_EQ(cluster_hw_lease_next_block(space_identity.key.locator, MAIN_FORKNUM), 4);
}

UT_TEST(test_registration_does_not_execute_or_supply_a_completion)
{
	for (uint16 op = CLUSTER_KO_SHARED_V3_OPERATION_DROP;
		 op <= CLUSTER_KO_SHARED_V3_OPERATION_REWRITE_OLD; op++) {
		KoAxBinding binding;
		ClusterKoCompletionV2 *owner = register_operation(&binding, op);
		ClusterKoSharedContext before = storage.contexts[owner->slot], completed;

		assert_no_object_work();
		UT_ASSERT_EQ(marker_registrations, 1);
		UT_ASSERT_EQ(marker_batch, before.request_v3.base.batch_id);
		UT_ASSERT_EQ(space_reads, 1);
		UT_ASSERT_EQ(native_allocated, 1);
		UT_ASSERT(before.used && before.v3 && !before.complete && !before.origin_detached);
		UT_ASSERT_EQ(before.serial, owner->serial);
		UT_ASSERT(!owner->barrier_private && !owner->space_observed && !owner->origin_data_closed);
		UT_ASSERT(!ko_completion_snapshot(owner, &completed));
		UT_ASSERT(!cluster_ko_shared_covers_v2(owner, &space_identity.key,
			space_identity.incarnation));
		UT_ASSERT_EQ(before.request_sent_peers | before.request_sending_peers
			| before.request_uncertain_peers | before.request_done_peers, 0);
		cluster_ko_shared_release_v2(&owner);
		UT_ASSERT(owner == NULL && native_allocated == 0 && completion_allocations == 0);
	}
}

UT_TEST(test_execution_consumes_the_same_registration_once)
{
	KoAxBinding binding;
	ClusterKoCompletionV2 *owner = register_operation(&binding, CLUSTER_KO_SHARED_V3_OPERATION_DROP);
	ClusterKoSharedContext before = storage.contexts[owner->slot];
	unsigned slot = owner->slot;
	uint64 serial = storage.context_serial;

	UT_ASSERT(ko_run_shared_barrier_impl_v3(space_identity.key.locator,
		CLUSTER_KO_SHARED_V3_OPERATION_DROP, &binding, owner));
	UT_ASSERT_EQ(owner->slot, slot);
	UT_ASSERT_EQ(owner->serial, before.serial);
	UT_ASSERT_EQ(storage.context_serial, serial);
	UT_ASSERT_EQ(marker_registrations, 1);
	UT_ASSERT_EQ(barrier_requests, 1);
	UT_ASSERT_EQ(record_count, 1);
	UT_ASSERT_EQ(native_allocate_calls, 1);
	UT_ASSERT(memcmp(&storage.contexts[slot].request_v3, &before.request_v3,
		sizeof(before.request_v3)) == 0);
	UT_ASSERT(storage.contexts[slot].complete);
	UT_ASSERT(!ko_run_shared_barrier_impl_v3(space_identity.key.locator,
		CLUSTER_KO_SHARED_V3_OPERATION_DROP, &binding, owner));
	UT_ASSERT_EQ(storage.context_serial, serial);
	UT_ASSERT_EQ(barrier_requests, 1);
	UT_ASSERT_EQ(marker_registrations, 1);
	cluster_ko_shared_release_v2(&owner);
}

UT_TEST(test_changed_registration_is_not_refreshed_or_dispatched)
{
	for (unsigned fault = 0; fault < 12; fault++) {
		KoAxBinding binding;
		ClusterKoCompletionV2 *owner = register_operation(&binding, CLUSTER_KO_SHARED_V3_OPERATION_DROP);
		ClusterKoCompletionV2 saved = *owner;
		ClusterKoSharedContext before = storage.contexts[owner->slot];
		ClusterKoSharedContext *entry = &storage.contexts[owner->slot];
		uint64 serial = storage.context_serial;

		switch (fault) {
		case 0: entry->serial++; break;
		case 1: entry->request_v3.space_mutation_token++; break;
		case 2: entry->peer_boots[1]++; break;
		case 3: entry->origin_detached = true; break;
		case 4: entry->complete = true; break;
		case 5: entry->request_sending_peers = 2; break;
		case 6: entry->request_uncertain_peers = 2; break;
		case 7: owner->pid++; break;
		case 8: owner->owner = (ResourceOwner)2; break;
		case 9: owner->slot = CLUSTER_KO_SHARED_CAPACITY; break;
		case 10: binding.master_generation++; break;
		case 11: binding.holder.procno++; break;
		}
		UT_ASSERT(!ko_run_shared_barrier_impl_v3(space_identity.key.locator,
			CLUSTER_KO_SHARED_V3_OPERATION_DROP, &binding, owner));
		assert_no_object_work();
		UT_ASSERT_EQ(storage.context_serial, serial);
		UT_ASSERT_EQ(marker_registrations, 1);
		*owner = saved;
		*entry = before;
		cluster_ko_shared_release_v2(&owner);
	}
}

UT_TEST(test_current_authority_drift_does_not_create_a_new_registration)
{
	for (unsigned fault = 0; fault < 4; fault++) {
		KoAxBinding binding;
		ClusterKoCompletionV2 *owner = register_operation(&binding, CLUSTER_KO_SHARED_V3_OPERATION_DROP);
		uint64 serial = storage.context_serial;

		if (fault == 0) v3_local_ax = false;
		if (fault == 1) writer.claim.identity.origin_owner_incarnation++;
		if (fault == 2) formation.membership.last_admitted_incarnation[1]++;
		if (fault == 3) current_epoch++;
		UT_ASSERT(!ko_run_shared_barrier_impl_v3(space_identity.key.locator,
			CLUSTER_KO_SHARED_V3_OPERATION_DROP, &binding, owner));
		assert_no_object_work();
		UT_ASSERT_EQ(storage.context_serial, serial);
		UT_ASSERT_EQ(marker_registrations, 1);
		cluster_ko_shared_release_v2(&owner);
	}
}

UT_TEST(test_stale_registration_is_rejected_before_physical_execution)
{
	KoAxBinding binding;
	ClusterKoCompletionV2 *owner = register_operation(&binding, CLUSTER_KO_SHARED_V3_OPERATION_DROP);
	volatile bool caught = false;

	storage.contexts[owner->slot].request_v3.space_mutation_token++;
	expecting_error = true;
	PG_TRY();
	{
		ko_flush_and_wait_ack(space_identity.key.locator, RELPERSISTENCE_PERMANENT,
			owner, CLUSTER_KO_SHARED_V3_OPERATION_DROP, &binding);
	}
	PG_CATCH(); { caught = true; }
	PG_END_TRY();
	expecting_error = false;
	UT_ASSERT(caught);
	UT_ASSERT_EQ(reported_sqlstate, ERRCODE_CLUSTER_OBJECT_FLUSH_UNAVAILABLE);
	assert_no_object_work();
	cluster_ko_shared_release_v2(&owner);
}

UT_TEST(test_unqualified_single_member_registration_cannot_complete)
{
	KoAxBinding binding = origin_marker_setup();
	ClusterKoCompletionV2 *owner = NULL;
	uint64 serial;
	volatile bool caught = false;

	formation.membership.membership_state[1] = CLUSTER_MEMBER_DEAD;
	formation.membership.last_admitted_incarnation[1] = 0;
	expecting_error = true;
	PG_TRY();
	{
		UT_ASSERT(ko_shared_begin_v3(space_identity.key.locator, RELPERSISTENCE_PERMANENT,
			CLUSTER_KO_SHARED_V3_OPERATION_DROP, &binding, &owner));
	}
	PG_CATCH(); { caught = true; }
	PG_END_TRY();
	expecting_error = false;
	UT_ASSERT(!caught);
	if (owner == NULL)
		return;
	serial = owner->serial;
	UT_ASSERT(!storage.contexts[owner->slot].complete);
	UT_ASSERT_EQ(marker_registrations + barrier_requests + record_count + lock_calls, 0);
	/* This fixture has no qualified remote route. Zero peers must not
	 * bypass the original CONTROL gate or manufacture a completed barrier. */
	UT_ASSERT(!ko_shared_origin_current(&storage.contexts[owner->slot]));
	UT_ASSERT(!ko_run_shared_barrier_impl_v3(space_identity.key.locator,
		CLUSTER_KO_SHARED_V3_OPERATION_DROP, &binding, owner));
	UT_ASSERT_EQ(owner->serial, serial);
	UT_ASSERT(storage.contexts[owner->slot].used && !storage.contexts[owner->slot].complete);
	UT_ASSERT_EQ(marker_registrations + barrier_requests + record_count + lock_calls, 0);
	cluster_ko_shared_release_v2(&owner);
}

UT_TEST(test_unexecuted_registration_remains_owned_through_subcommit_and_abort)
{
	KoAxBinding binding;
	ClusterKoCompletionV2 *owner = register_operation(&binding, CLUSTER_KO_SHARED_V3_OPERATION_DROP);
	unsigned slot = owner->slot;
	uint64 serial = owner->serial;

	CurrentResourceOwner = CurTransactionResourceOwner = (ResourceOwner)2;
	owner->owner = CurrentResourceOwner;
	resource_callback(RESOURCE_RELEASE_BEFORE_LOCKS, true, false, NULL);
	UT_ASSERT_EQ(owner->owner, (ResourceOwner)1);
	UT_ASSERT_EQ(native_allocated, 1);
	CurrentResourceOwner = CurTransactionResourceOwner = (ResourceOwner)1;
	resource_callback(RESOURCE_RELEASE_BEFORE_LOCKS, false, true, NULL);
	UT_ASSERT(ko_completions == NULL);
	UT_ASSERT_EQ(native_allocated + completion_allocations, 0);
	UT_ASSERT_EQ(native_free_calls, 1);
	UT_ASSERT(storage.contexts[slot].used && storage.contexts[slot].origin_detached);
	UT_ASSERT_EQ(storage.contexts[slot].serial, serial);
	UT_ASSERT(!storage.contexts[slot].ax_release_requested);
	assert_no_object_work();
}

UT_TEST(test_original_caller_cleans_up_dispatch_error)
{
	RelationData relation;
	volatile bool caught = false;
	Oid saved_database = MyDatabaseId;

	v3_native_structure_setup(&relation);
	MyDatabaseId = relation.rd_locator.dbOid;
	drive_v3_failure = expecting_error = true;
	PG_TRY();
	{
		cluster_ko_flush_and_wait_ack_relation(&relation, RELPERSISTENCE_PERMANENT,
			CLUSTER_KO_SHARED_V3_OPERATION_DROP);
	}
	PG_CATCH(); { caught = true; }
	PG_END_TRY();
	expecting_error = false;
	MyDatabaseId = saved_database;
	UT_ASSERT(caught);
	UT_ASSERT_EQ(reported_sqlstate, ERRCODE_CLUSTER_OBJECT_FLUSH_UNAVAILABLE);
	UT_ASSERT_EQ(native_allocate_calls, 1);
	UT_ASSERT_EQ(native_free_calls, 1);
	UT_ASSERT_EQ(native_allocated + completion_allocations, 0);
	UT_ASSERT(ko_completions == NULL && !barrier_active);
	UT_ASSERT_EQ(barrier_removes, 1);
	UT_ASSERT(storage.contexts[0].used && storage.contexts[0].origin_detached);
	UT_ASSERT_EQ(storage.contexts[0].cancel_sqlerrcode, reported_sqlstate);
	UT_ASSERT_EQ(storage.contexts[0].request_sent_peers, 2);
	UT_ASSERT_EQ(storage.contexts[0].request_failed_peers, 2);
}

int
main(void)
{
	UT_PLAN(12);
	UT_RUN(test_registration_does_not_execute_or_supply_a_completion);
	UT_RUN(test_execution_consumes_the_same_registration_once);
	UT_RUN(test_changed_registration_is_not_refreshed_or_dispatched);
	UT_RUN(test_current_authority_drift_does_not_create_a_new_registration);
	UT_RUN(test_stale_registration_is_rejected_before_physical_execution);
	UT_RUN(test_unqualified_single_member_registration_cannot_complete);
	UT_RUN(test_unexecuted_registration_remains_owned_through_subcommit_and_abort);
	UT_RUN(test_original_caller_cleans_up_dispatch_error);
	UT_RUN(test_v3_native_reservation_precedes_publication_and_is_reused);
	UT_RUN(test_v3_native_allocation_failure_precedes_object_effects);
	UT_RUN(test_v3_native_presend_failure_releases_reserved_carrier);
	UT_RUN(test_v3_native_begin_requires_shared_state_before_reservation);
	UT_DONE();
	return ut_failed_count != 0;
}
