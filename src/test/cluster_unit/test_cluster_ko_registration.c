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

UT_TEST(test_registered_drop_can_prepare_record_without_a_completed_barrier)
{
	KoAxBinding binding;
	ClusterKoCompletionV2 *owner = register_operation(&binding, CLUSTER_KO_SHARED_V3_OPERATION_DROP);
	ClusterKoCompletionV2 *claimed = NULL, *again = NULL;
	ClusterKoSharedContext completed;

	UT_ASSERT(owner->native_transaction && owner->native_pending);
	UT_ASSERT(cluster_ko_shared_claim_structure(&space_identity, CLUSTER_SPACE_WAL_TOMBSTONE,
		&claimed));
	UT_ASSERT(claimed == owner && !owner->native_pending);
	UT_ASSERT(!cluster_ko_shared_claim_structure(&space_identity, CLUSTER_SPACE_WAL_TOMBSTONE,
		&again));
	UT_ASSERT(again == NULL);
	UT_ASSERT(!ko_completion_snapshot(owner, &completed));
	UT_ASSERT(!ko_shared_structure_current(owner, &space_identity, CLUSTER_SPACE_WAL_TOMBSTONE));
	UT_ASSERT(!cluster_ko_shared_drop_write_current(owner, &space_identity));
	UT_ASSERT(!owner->barrier_private && !owner->space_observed && !owner->origin_data_closed);
	assert_no_object_work();
	cluster_ko_shared_release_v2(&owner);
}

UT_TEST(test_registered_record_claim_requires_exact_drop_identity_and_owner)
{
	for (unsigned fault = 0; fault < 8; fault++) {
		KoAxBinding binding;
		ClusterKoCompletionV2 *owner = register_operation(&binding, CLUSTER_KO_SHARED_V3_OPERATION_DROP);
		ClusterKoCompletionV2 *claimed = NULL;
		ClusterSpaceIdentity identity = space_identity;
		ClusterSpaceWalAction action = CLUSTER_SPACE_WAL_TOMBSTONE;

		if (fault == 0) identity.operation++;
		if (fault == 1) identity.incarnation[0]++;
		if (fault == 2) identity.key.database_incarnation++;
		if (fault == 3) identity.key.locator.relNumber++;
		if (fault == 4) action = CLUSTER_SPACE_WAL_TRUNCATE;
		if (fault == 5) v3_local_ax = false;
		if (fault == 6) CritSectionCount = 1;
		if (fault == 7) CurrentResourceOwner = CurTransactionResourceOwner = (ResourceOwner)2;
		UT_ASSERT(!cluster_ko_shared_claim_structure(&identity, action, &claimed));
		UT_ASSERT(claimed == NULL);
		CritSectionCount = 0;
		CurrentResourceOwner = CurTransactionResourceOwner = (ResourceOwner)1;
		assert_no_object_work();
		cluster_ko_shared_release_v2(&owner);
	}
}

UT_TEST(test_registered_claim_abort_keeps_exact_unsent_s6_responsibility)
{
	KoAxBinding binding;
	ClusterKoCompletionV2 *owner = register_operation(&binding, CLUSTER_KO_SHARED_V3_OPERATION_DROP);
	ClusterKoCompletionV2 *claimed = NULL;
	unsigned slot = owner->slot;
	uint64 serial = owner->serial;

	UT_ASSERT(cluster_ko_shared_claim_structure(&space_identity, CLUSTER_SPACE_WAL_TOMBSTONE,
		&claimed));
	UT_ASSERT(claimed == owner);
	resource_callback(RESOURCE_RELEASE_BEFORE_LOCKS, false, true, NULL);
	UT_ASSERT(ko_completions == NULL && native_allocated == 0 && completion_allocations == 0);
	UT_ASSERT(storage.contexts[slot].used && storage.contexts[slot].origin_detached);
	UT_ASSERT_EQ(storage.contexts[slot].serial, serial);
	UT_ASSERT(!storage.contexts[slot].ax_release_requested && !storage.contexts[slot].complete);
	assert_no_object_work();
}

UT_TEST(test_registered_claim_keeps_legacy_truncate_and_ambiguity_refusals)
{
	KoAxBinding binding;
	ClusterKoCompletionV2 *owner = register_operation(&binding,
		CLUSTER_KO_SHARED_V3_OPERATION_REWRITE_OLD), *other = NULL, *claimed = NULL;

	UT_ASSERT(!cluster_ko_shared_claim_v2(&space_identity.key, space_identity.incarnation,
		&claimed));
	UT_ASSERT(claimed == NULL && owner->native_pending);
	UT_ASSERT(ko_shared_begin_v3(space_identity.key.locator, RELPERSISTENCE_PERMANENT,
		CLUSTER_KO_SHARED_V3_OPERATION_DROP, &binding, &other));
	UT_ASSERT(!cluster_ko_shared_claim_structure(&space_identity, CLUSTER_SPACE_WAL_TOMBSTONE,
		&claimed));
	UT_ASSERT(claimed == NULL && owner->native_pending && other->native_pending);
	cluster_ko_shared_release_v2(&other);
	UT_ASSERT(cluster_ko_shared_claim_structure(&space_identity, CLUSTER_SPACE_WAL_TOMBSTONE,
		&claimed));
	UT_ASSERT(claimed == owner);
	UT_ASSERT(!cluster_ko_shared_drop_write_current(owner, &space_identity));
	assert_no_object_work();
	cluster_ko_shared_release_v2(&owner);

	owner = register_operation(&binding, CLUSTER_KO_SHARED_V3_OPERATION_TRUNCATE);
	claimed = NULL;
	UT_ASSERT(!cluster_ko_shared_claim_structure(&space_identity, CLUSTER_SPACE_WAL_TRUNCATE,
		&claimed));
	UT_ASSERT(!cluster_ko_shared_claim_structure(&space_identity, CLUSTER_SPACE_WAL_TOMBSTONE,
		&claimed));
	UT_ASSERT(claimed == NULL && owner->native_pending);
	assert_no_object_work();
	cluster_ko_shared_release_v2(&owner);
}

UT_TEST(test_drop_write_permission_consumes_original_completed_owner)
{
	for (unsigned fault = 0; fault < 8; fault++) {
		ClusterKoCompletionV2 *owner = v3_native_structure_owner(
			CLUSTER_KO_SHARED_V3_OPERATION_DROP), *claimed = NULL;
		ClusterSpaceIdentity identity = space_identity;
		unsigned requests = barrier_requests, reads = space_reads, allocations = native_allocate_calls;

		UT_ASSERT(!cluster_ko_shared_drop_write_current(owner, &identity));
		UT_ASSERT(cluster_ko_shared_claim_structure(&identity, CLUSTER_SPACE_WAL_TOMBSTONE,
			&claimed));
		UT_ASSERT(claimed == owner);
		UT_ASSERT(cluster_ko_shared_drop_write_current(owner, &identity));
		if (fault == 0) owner->origin_data_closed = false;
		if (fault == 1) owner->native_pending = true;
		if (fault == 2) owner->space_observed = true;
		if (fault == 3) identity.operation++;
		if (fault == 4) v3_local_ax = false;
		if (fault == 5) owner->barrier_ready_peers = 0;
		if (fault == 6) CritSectionCount = 1;
		if (fault == 7) CurrentResourceOwner = CurTransactionResourceOwner = (ResourceOwner)2;
		UT_ASSERT(!cluster_ko_shared_drop_write_current(owner, &identity));
		UT_ASSERT_EQ(barrier_requests, requests);
		UT_ASSERT_EQ(space_reads, reads);
		UT_ASSERT_EQ(native_allocate_calls, allocations);
		CritSectionCount = 0;
		CurrentResourceOwner = CurTransactionResourceOwner = (ResourceOwner)1;
		cluster_ko_shared_release_v2(&owner);
	}
}

static ClusterKoCompletionV2 *
claimed_registration(uint16 operation)
{
	KoAxBinding binding;
	ClusterKoCompletionV2 *owner = register_operation(&binding, operation), *claimed = NULL;

	UT_ASSERT(cluster_ko_shared_claim_structure(&space_identity, CLUSTER_SPACE_WAL_TOMBSTONE,
		&claimed));
	UT_ASSERT(claimed == owner && !owner->native_pending);
	return owner;
}

/* The caller is the real irreversible native hook, not XACT_EVENT_COMMIT.
 * A caught baseline PANIC is a failed test, never a successful handoff. */
static bool
commit_registration(void)
{
	volatile bool caught = false;

	expecting_error = true;
	PG_TRY(); { cluster_ko_shared_native_commit_v2(); }
	PG_CATCH(); { caught = true; }
	PG_END_TRY();
	expecting_error = false;
	UT_ASSERT(!caught);
	if (caught) {
		native_fixture_shared_memory_end();
		return false;
	}
	return true;
}

UT_TEST(test_committed_registration_transfers_once_without_completion)
{
	const uint16 operations[] = { CLUSTER_KO_SHARED_V3_OPERATION_DROP,
		CLUSTER_KO_SHARED_V3_OPERATION_REWRITE_OLD };

	for (unsigned n = 0; n < lengthof(operations); n++) {
		ClusterKoCompletionV2 *owner = claimed_registration(operations[n]);
		ClusterKoSharedContext before = storage.contexts[owner->slot];
		dsa_pointer allocation = owner->native_allocation;
		ClusterKoNativeContinuation *work = owner->native_reserved;
		unsigned slot = owner->slot;

		UT_ASSERT(!owner->barrier_private && !owner->space_observed);
		if (!commit_registration())
			continue;
		UT_ASSERT(owner->native_handed_off && owner->postcommit);
		UT_ASSERT(!DsaPointerIsValid(owner->native_allocation) && owner->native_reserved == NULL);
		UT_ASSERT_EQ(owner->native_unready, allocation);
		UT_ASSERT_EQ(storage.native_incoming, allocation);
		UT_ASSERT_EQ(storage.native_waiting, 1);
		UT_ASSERT_EQ(native_allocate_calls, 1);
		UT_ASSERT_EQ(native_free_calls, 0);
		UT_ASSERT(!work->recovery_required && !storage.native_recovery_required);
		UT_ASSERT(!work->buffers_released && !work->predelete_ready);
		UT_ASSERT(work->context.used && work->context.v3 && !work->context.complete);
		UT_ASSERT(!work->context.structure_owned && !work->context.origin_data_closed);
		UT_ASSERT_EQ(work->context.pid, before.pid);
		UT_ASSERT_EQ(work->context.serial, before.serial);
		UT_ASSERT(memcmp(&work->context.request_v3, &before.request_v3,
			sizeof(before.request_v3)) == 0);
		UT_ASSERT(memcmp(work->context.peer_boots, before.peer_boots, sizeof(before.peer_boots)) == 0);
		UT_ASSERT_EQ(work->context.request_sent_peers | work->context.request_done_peers, 0);
		UT_ASSERT(storage.contexts[slot].used && !storage.contexts[slot].complete);
		UT_ASSERT(!ko_shared_ready_v3(&work->context));
		UT_ASSERT(!cluster_ko_shared_native_prepare_drop_v2(before.request.key.locator));
		UT_ASSERT(cluster_ko_shared_native_drop_deferred_v2(before.request.key.locator));
		UT_ASSERT(!cluster_ko_shared_drop_write_current(owner, &space_identity));
		cluster_ko_shared_native_commit_v2();
		UT_ASSERT_EQ(storage.native_waiting, 1);
		UT_ASSERT_EQ(native_allocate_calls, 1);
		assert_no_object_work();
		native_fixture_shared_memory_end();
	}
}

UT_TEST(test_committed_registration_cp_waits_without_live_proof_or_io)
{
	ClusterKoCompletionV2 *owner = claimed_registration(CLUSTER_KO_SHARED_V3_OPERATION_DROP);
	ClusterKoNativeContinuation *work = owner->native_reserved;
	dsa_pointer allocation = owner->native_allocation;
	unsigned reads = space_reads;
	const char *reason;

	if (!commit_registration())
		return;
	/* A committed pending owner must not be classified by the later live
	 * DATA/SPACE gate while its original producer is still doing cleanup. */
	current_epoch++;
	v3_local_ax = false;
	MyBackendType = B_CHECKPOINTER;
	for (unsigned n = 0; n < 3; n++) {
		UT_ASSERT_EQ(cluster_ko_shared_native_promote_v2(), CLUSTER_KO_STRUCTURE_PENDING);
		UT_ASSERT_EQ(storage.native_incoming, allocation);
		UT_ASSERT_EQ(storage.native_waiting, 1);
		UT_ASSERT(!DsaPointerIsValid(storage.native_failed));
		UT_ASSERT(!work->recovery_required && !storage.native_recovery_required);
	}
	UT_ASSERT_EQ(space_reads, reads);
	UT_ASSERT_EQ(native_free_calls, 0);
	UT_ASSERT_EQ(cluster_ko_shared_normal_stop_poll_v2(&reason), CLUSTER_NORMAL_STOP_PENDING);
	assert_no_object_work();
	MyBackendType = B_BACKEND;
	native_fixture_shared_memory_end();
}

UT_TEST(test_committed_registration_handoff_retains_original_cut_after_drift)
{
	for (unsigned fault = 0; fault < 4; fault++) {
		ClusterKoCompletionV2 *owner = claimed_registration(CLUSTER_KO_SHARED_V3_OPERATION_DROP);
		ClusterKoSharedMessageV3 request = owner->barrier_request_v3;
		ClusterKoNativeContinuation *work = owner->native_reserved;
		bool committed;

		if (fault == 0) current_epoch++;
		if (fault == 1) v3_local_ax = false;
		if (fault == 2) formation.membership.last_admitted_incarnation[1]++;
		if (fault == 3) cluster_interconnect_tier = CLUSTER_IC_TIER_2;
		committed = commit_registration();
		cluster_interconnect_tier = CLUSTER_IC_TIER_1;
		if (!committed)
			continue;
		UT_ASSERT(owner->native_handed_off);
		UT_ASSERT(memcmp(&work->context.request_v3, &request, sizeof(request)) == 0);
		UT_ASSERT(!work->context.complete && !work->context.structure_owned);
		UT_ASSERT(!work->recovery_required && !storage.native_recovery_required);
		assert_no_object_work();
		native_fixture_shared_memory_end();
	}
}

UT_TEST(test_committed_registration_survives_original_cleanup_and_blocks_s6)
{
	for (unsigned failing = 0; failing < 2; failing++) {
		ClusterKoCompletionV2 *owner = claimed_registration(CLUSTER_KO_SHARED_V3_OPERATION_DROP);
		ClusterKoNativeContinuation *work = owner->native_reserved;
		ClusterGrdHolderId holder = owner->barrier_request_v3.ax_holder;
		unsigned slot = owner->slot;
		LOCKTAG tag;
		const char *reason;
		volatile bool caught = false;

		SET_LOCKTAG_RELATION(tag, owner->barrier_request_v3.ax_resid.field1,
			owner->barrier_request_v3.ax_resid.field2);
		postcommit_owner = owner;
		memset(&postcommit_binding, 0, sizeof(postcommit_binding));
		postcommit_binding.identity.locator = owner->barrier_request_v3.base.key.locator;
		postcommit_drop = true;
		postcommit_error = postcommit_scope_changed = postcommit_buffer_error = false;
		postcommit_callback_error = failing != 0;
		postcommit_deletes = postcommit_phases = 0;
		postcommit_native_count = 1;
		postcommit_storage_consumer = NULL;
		native_fork_unlinks = native_fork_closes = native_smgr_invalidations = 0;
		native_commit_active = expecting_error = true;
		PG_TRY(); { run_native_postcommit(); }
		PG_CATCH(); { caught = true; }
		PG_END_TRY();
		expecting_error = false;
		UT_ASSERT_EQ(caught, failing != 0);
		if (!DsaPointerIsValid(storage.native_incoming)) {
			native_fixture_shared_memory_end();
			continue;
		}
		UT_ASSERT_EQ(storage.native_waiting, 1);
		UT_ASSERT_EQ(postcommit_deletes, failing ? 0 : 1);
		UT_ASSERT_EQ(native_fork_unlinks, 0);
		UT_ASSERT(!work->context.complete && !work->buffers_released);
		if (failing) {
			UT_ASSERT(!work->publication_observed);
			UT_ASSERT(!work->recovery_required);
			fixture_absent_ack_batch = work->context.request.batch_id;
			exit_callback(0, (Datum)0);
		}
		UT_ASSERT_EQ(fixture_absent_ack_removes, failing);
		UT_ASSERT_EQ(completion_allocations, 0);
		UT_ASSERT(work->recovery_required && storage.native_recovery_required);
		UT_ASSERT_EQ(native_free_calls, 0);
		UT_ASSERT(storage.contexts[slot].used && storage.contexts[slot].origin_detached);
		UT_ASSERT(cluster_ko_ax_release_handoff_v3(&tag, AccessExclusiveLock, &holder));
		UT_ASSERT(!ko_origin_release_ready(slot));
		storage.ax_release_cursor = slot;
		ko_origin_release_tick();
		UT_ASSERT_EQ(release_poll_calls, 0);
		UT_ASSERT(cluster_ko_ax_acquire_pending_v3(&work->context.request_v3.ax_resid,
			holder.procno, 0));
		UT_ASSERT_EQ(cluster_ko_shared_normal_stop_poll_v2(&reason), CLUSTER_NORMAL_STOP_INVALID);
		MyBackendType = B_CHECKPOINTER;
		UT_ASSERT_EQ(cluster_ko_shared_native_promote_v2(), CLUSTER_KO_STRUCTURE_INVALID);
		UT_ASSERT(DsaPointerIsValid(storage.native_failed));
		UT_ASSERT_EQ(storage.native_waiting, 0);
		UT_ASSERT(storage.contexts[slot].used && !ko_origin_release_ready(slot));
		MyBackendType = B_BACKEND;
		native_fixture_shared_memory_end();
	}
}

UT_TEST(test_commit_registration_rejects_changed_owner_or_executed_context)
{
	for (unsigned fault = 0; fault < 14; fault++) {
		ClusterKoCompletionV2 *owner = claimed_registration(CLUSTER_KO_SHARED_V3_OPERATION_DROP);
		ClusterKoSharedContext *entry = &storage.contexts[owner->slot];
		ClusterKoSharedContext before = *entry;
		ClusterKoCompletionV2 saved = *owner;

		owner->postcommit = true;
		switch (fault) {
		case 0: owner->postcommit = false; break;
		case 1: owner->native_pending = true; break;
		case 2: owner->owner = (ResourceOwner)2; break;
		case 3: owner->pid++; break;
		case 4: entry->serial++; break;
		case 5: entry->request_v3.space_mutation_token++; break;
		case 6: entry->peer_boots[1]++; break;
		case 7: entry->origin_detached = true; break;
		case 8: entry->complete = true; break;
		case 9: entry->request_sending_peers = 2; break;
		case 10: entry->request_uncertain_peers = 2; break;
		case 11: entry->request_sent_peers = 2; break;
		case 12: entry->request_failed_peers = 2; break;
		case 13: entry->structure_owned = true; break;
		}
		UT_ASSERT(!ko_native_publish(owner, true));
		UT_ASSERT_EQ(storage.native_waiting, 0);
		UT_ASSERT(!owner->native_handed_off);
		UT_ASSERT_EQ(native_allocated, 1);
		UT_ASSERT_EQ(native_free_calls, 0);
		*owner = saved;
		*entry = before;
		cluster_ko_shared_release_v2(&owner);
	}
}

UT_TEST(test_registered_handoff_cannot_be_started_by_abort_or_unclaimed_commit)
{
	KoAxBinding binding;
	ClusterKoCompletionV2 *owner = register_operation(&binding, CLUSTER_KO_SHARED_V3_OPERATION_DROP);
	unsigned slot = owner->slot;

	cluster_ko_shared_native_commit_v2();
	UT_ASSERT(!owner->native_handed_off && !owner->postcommit);
	UT_ASSERT_EQ(storage.native_waiting, 0);
	owner->native_pending = false;
	owner->postcommit = true;
	UT_ASSERT(!ko_native_publish(owner, false));
	owner->postcommit = false;
	resource_callback(RESOURCE_RELEASE_BEFORE_LOCKS, false, true, NULL);
	UT_ASSERT_EQ(native_free_calls, 1);
	UT_ASSERT_EQ(native_allocated, 0);
	UT_ASSERT(storage.contexts[slot].origin_detached);
	storage.contexts[slot].ax_release_requested = true;
	UT_ASSERT(ko_origin_release_ready(slot));
	assert_no_object_work();
}

/* Run through the real extracted native COMMIT sequence. The fixture stops
 * physical storage at its existing boundary, before original local cleanup. */
static bool
publication_keeps_committed_registration(RelFileLocator locator)
{
	ClusterKoNativeContinuation *work = dsa_get_address(ko_native_area,
		postcommit_owner->native_unready);
	unsigned slot = postcommit_owner->slot;

	UT_ASSERT(RelFileLocatorEquals(locator, work->context.request.key.locator));
	UT_ASSERT(!work->recovery_required && !storage.native_recovery_required);
	UT_ASSERT(work->publication_observed);
	UT_ASSERT(work->context.used && !work->context.complete);
	UT_ASSERT(!work->buffers_released && !work->predelete_ready);
	UT_ASSERT(!cluster_ko_directory_drop_committed_v3(locator));
	UT_ASSERT(!cluster_ko_shared_native_prepare_drop_v2(locator));
	UT_ASSERT(cluster_ko_shared_native_drop_deferred_v2(locator));
	UT_ASSERT(!ko_origin_release_ready(slot));
	MyBackendType = B_CHECKPOINTER;
	UT_ASSERT_EQ(cluster_ko_shared_native_promote_v2(), CLUSTER_KO_STRUCTURE_PENDING);
	MyBackendType = B_BACKEND;
	assert_no_object_work();
	return true;
}

UT_TEST(test_original_publication_does_not_fail_a_pending_commit_window)
{
	ClusterKoCompletionV2 *owner = claimed_registration(CLUSTER_KO_SHARED_V3_OPERATION_DROP);
	ClusterKoNativeContinuation *work = owner->native_reserved;

	postcommit_owner = owner;
	memset(&postcommit_binding, 0, sizeof(postcommit_binding));
	postcommit_binding.identity.locator = owner->barrier_request_v3.base.key.locator;
	postcommit_drop = true;
	postcommit_error = postcommit_scope_changed = postcommit_buffer_error = false;
	postcommit_callback_error = false;
	postcommit_deletes = postcommit_phases = 0;
	postcommit_native_count = 1;
	postcommit_storage_consumer = publication_keeps_committed_registration;
	native_fork_unlinks = native_fork_closes = native_smgr_invalidations = 0;
	native_commit_active = true;
	run_native_postcommit();
	postcommit_storage_consumer = NULL;
	UT_ASSERT_EQ(postcommit_deletes, 1);
	UT_ASSERT_EQ(native_fork_unlinks, 0);
	/* No successful drain is fabricated by this fixture. The original final
	 * cleanup still abandons the unready carrier for recovery. */
	UT_ASSERT(work->recovery_required && storage.native_recovery_required);
	UT_ASSERT_EQ(native_free_calls, 0);
	native_fixture_shared_memory_end();
}

UT_TEST(test_pending_publication_retains_immutable_cut_without_live_proofs)
{
	const uint16 operations[] = { CLUSTER_KO_SHARED_V3_OPERATION_DROP,
		CLUSTER_KO_SHARED_V3_OPERATION_REWRITE_OLD };

	for (unsigned op = 0; op < lengthof(operations); op++)
		for (unsigned fault = 0; fault < 5; fault++) {
			ClusterKoCompletionV2 *owner = claimed_registration(operations[op]);
			ClusterKoNativeContinuation *work = owner->native_reserved;
			ClusterKoSharedContext before;
			unsigned reads = space_reads;
			uint64 revision = pg_atomic_read_u64(&storage.peer_admission_revision);

			if (!commit_registration())
				continue;
			before = work->context;
			UT_ASSERT(!work->publication_observed);
			if (fault == 1) current_epoch++;
			if (fault == 2) v3_local_ax = false;
			if (fault == 3) formation.membership.last_admitted_incarnation[1]++;
			if (fault == 4) cluster_interconnect_tier = CLUSTER_IC_TIER_2;
			cluster_ko_shared_native_publication_v3();
			cluster_ko_shared_native_publication_v3();
			UT_ASSERT(!work->recovery_required && !storage.native_recovery_required);
			UT_ASSERT(work->publication_observed);
			UT_ASSERT(memcmp(&before, &work->context, sizeof(before)) == 0);
			UT_ASSERT_EQ(space_reads, reads);
			UT_ASSERT_EQ(pg_atomic_read_u64(&storage.peer_admission_revision), revision);
			UT_ASSERT_EQ(native_allocate_calls, 1);
			UT_ASSERT_EQ(native_free_calls, 0);
			UT_ASSERT(!work->buffers_released && !work->predelete_ready);
			assert_no_object_work();
			cluster_interconnect_tier = CLUSTER_IC_TIER_1;
			native_fixture_shared_memory_end();
		}
}

UT_TEST(test_pending_publication_rejects_changed_active_operation)
{
	for (unsigned fault = 0; fault < 14; fault++) {
		ClusterKoCompletionV2 *owner = claimed_registration(CLUSTER_KO_SHARED_V3_OPERATION_DROP);
		ClusterKoNativeContinuation *work = owner->native_reserved;
		ClusterKoSharedContext *entry = &storage.contexts[owner->slot];

		if (!commit_registration())
			continue;
		switch (fault) {
		case 0: entry->serial++; break;
		case 1: entry->request_v3.space_mutation_token++; break;
		case 2: entry->peer_boots[1]++; break;
		case 3: entry->origin_detached = true; break;
		case 4: entry->ax_release_requested = true; break;
		case 5: entry->request_sending_peers = 2; break;
		case 6: entry->request_sent_peers = 2; break;
		case 7: entry->request_uncertain_peers = 2; break;
		case 8: entry->request_done_peers = 2; break;
		case 9: entry->request_failed_peers = 2; break;
		case 10: entry->structure_owned = true; break;
		case 11: entry->complete = true; break;
		case 12: work->recovery_required = true; break;
		case 13: owner->slot = CLUSTER_KO_SHARED_CAPACITY; break;
		}
		cluster_ko_shared_native_publication_v3();
		UT_ASSERT(work->recovery_required && storage.native_recovery_required);
		UT_ASSERT(!work->publication_observed);
		UT_ASSERT(!work->buffers_released && !work->predelete_ready);
		UT_ASSERT(!cluster_ko_directory_drop_committed_v3(work->context.request.key.locator));
		UT_ASSERT_EQ(native_free_calls, 0);
		assert_no_object_work();
		native_fixture_shared_memory_end();
	}
}

UT_TEST(test_pending_publication_requires_original_committed_backend_owner)
{
	for (unsigned fault = 0; fault < 9; fault++) {
		ClusterKoCompletionV2 *owner = claimed_registration(CLUSTER_KO_SHARED_V3_OPERATION_DROP);
		ClusterKoNativeContinuation *work = owner->native_reserved;
		ClusterKoNativeContinuation before;
		ClusterKoCompletionV2 saved;

		if (!commit_registration())
			continue;
		before = *work;
		saved = *owner;
		switch (fault) {
		case 0: owner->native_handed_off = false; break;
		case 1: owner->postcommit = false; break;
		case 2: owner->native_pending = true; break;
		case 3: owner->pid++; break;
		case 4: owner->owner = (ResourceOwner)2; break;
		case 5: CritSectionCount = 1; break;
		case 6: CurrentResourceOwner = NULL; break;
		case 7: CurTransactionResourceOwner = (ResourceOwner)2; break;
		case 8: MyBackendType = B_CHECKPOINTER; break;
		}
		cluster_ko_shared_native_publication_v3();
		UT_ASSERT(memcmp(&before, work, sizeof(before)) == 0);
		UT_ASSERT(!work->publication_observed && !work->recovery_required);
		*owner = saved;
		CritSectionCount = 0;
		CurrentResourceOwner = CurTransactionResourceOwner = TopTransactionResourceOwner;
		MyBackendType = B_BACKEND;
		assert_no_object_work();
		native_fixture_shared_memory_end();
	}
}

UT_TEST(test_publication_retry_rechecks_identity_and_cannot_clear_failure)
{
	ClusterKoCompletionV2 *owner = claimed_registration(CLUSTER_KO_SHARED_V3_OPERATION_DROP);
	ClusterKoNativeContinuation *work = owner->native_reserved;
	unsigned slot = owner->slot;
	const char *reason;

	if (!commit_registration())
		return;
	cluster_ko_shared_native_publication_v3();
	UT_ASSERT(work->publication_observed && !work->recovery_required);
	storage.contexts[slot].serial++;
	cluster_ko_shared_native_publication_v3();
	UT_ASSERT(work->publication_observed && work->recovery_required);
	storage.contexts[slot].serial--;
	cluster_ko_shared_native_publication_v3();
	UT_ASSERT(work->publication_observed && work->recovery_required);
	UT_ASSERT_EQ(cluster_ko_shared_normal_stop_poll_v2(&reason), CLUSTER_NORMAL_STOP_INVALID);
	UT_ASSERT(!cluster_ko_shared_native_prepare_drop_v2(work->context.request.key.locator));
	UT_ASSERT(!ko_origin_release_ready(slot));
	UT_ASSERT_EQ(native_free_calls, 0);
	assert_no_object_work();
	native_fixture_shared_memory_end();
}

int
main(void)
{
	UT_PLAN(28);
	UT_RUN(test_pending_publication_requires_original_committed_backend_owner);
	UT_RUN(test_publication_retry_rechecks_identity_and_cannot_clear_failure);
	UT_RUN(test_original_publication_does_not_fail_a_pending_commit_window);
	UT_RUN(test_pending_publication_retains_immutable_cut_without_live_proofs);
	UT_RUN(test_pending_publication_rejects_changed_active_operation);
	UT_RUN(test_committed_registration_transfers_once_without_completion);
	UT_RUN(test_committed_registration_cp_waits_without_live_proof_or_io);
	UT_RUN(test_committed_registration_handoff_retains_original_cut_after_drift);
	UT_RUN(test_committed_registration_survives_original_cleanup_and_blocks_s6);
	UT_RUN(test_commit_registration_rejects_changed_owner_or_executed_context);
	UT_RUN(test_registered_handoff_cannot_be_started_by_abort_or_unclaimed_commit);
	UT_RUN(test_registered_drop_can_prepare_record_without_a_completed_barrier);
	UT_RUN(test_registered_record_claim_requires_exact_drop_identity_and_owner);
	UT_RUN(test_registered_claim_abort_keeps_exact_unsent_s6_responsibility);
	UT_RUN(test_registered_claim_keeps_legacy_truncate_and_ambiguity_refusals);
	UT_RUN(test_drop_write_permission_consumes_original_completed_owner);
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
