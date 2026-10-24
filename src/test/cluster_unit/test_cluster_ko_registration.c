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

static uint32 committed_origin_missing_caps;
static bool committed_origin_only_caps;
static bool committed_origin_revoke_after_release;
static bool committed_origin_production_caps;

static uint32
committed_origin_capabilities(void)
{
	uint32 caps = PGRAC_IC_HELLO_CAP_KO_COMMITTED_DISCARD_V1
		| PGRAC_IC_HELLO_CAP_KO_COMMIT_FENCE_V1;
	if (!committed_origin_only_caps)
		caps |= native_real_capability_word();
	if (committed_origin_revoke_after_release && release_poll_calls != 0)
		caps &= ~PGRAC_IC_HELLO_CAP_KO_COMMITTED_DISCARD_V1;
	return caps & ~committed_origin_missing_caps;
}

/* Actual origin constructor/claim/COMMIT consumers; only capability, GES,
 * SPACE identity, allocation and transport use the existing fixture. No
 * authenticated PKO4 context or completion is manufactured here. */
static KoAxBinding
committed_origin_setup(void)
{
	KoAxBinding binding = origin_marker_setup();
	committed_origin_missing_caps = 0;
	committed_origin_only_caps = false;
	committed_origin_revoke_after_release = false;
	native_capability_hook = committed_origin_production_caps
		? native_real_capability_word : committed_origin_capabilities;
	cluster_hw_lease_install(space_identity.key.locator, MAIN_FORKNUM, 4, 4);
	return binding;
}

static ClusterKoCompletionV2 *
committed_origin_register(uint16 operation, KoAxBinding *binding)
{
	ClusterKoCompletionV2 *owner = NULL;
	volatile bool caught = false;
	expecting_error = true;
	PG_TRY(); {
		UT_ASSERT(ko_shared_begin_v3(space_identity.key.locator, RELPERSISTENCE_PERMANENT,
			operation, binding, &owner));
	}
	PG_CATCH(); { caught = true; }
	PG_END_TRY();
	expecting_error = false;
	UT_ASSERT(!caught && owner != NULL);
	return owner;
}

UT_TEST(test_committed_native_entry_registers_without_precommit_object_work)
{
	const uint16 operations[] = { CLUSTER_KO_SHARED_V3_OPERATION_DROP,
		CLUSTER_KO_SHARED_V3_OPERATION_REWRITE_OLD };
	for (unsigned n = 0; n < lengthof(operations); n++) {
		RelationData relation;
		ClusterKoCompletionV2 *owner;
		ClusterKoSharedContext snapshot;
		Oid saved_database = MyDatabaseId;
		v3_native_structure_setup(&relation);
		committed_origin_missing_caps = 0;
		committed_origin_only_caps = false;
		committed_origin_revoke_after_release = false;
		native_capability_hook = committed_origin_capabilities;
		cluster_hw_lease_install(relation.rd_locator, MAIN_FORKNUM, 4, 4);
		MyDatabaseId = relation.rd_locator.dbOid;
		cluster_ko_flush_and_wait_ack_relation(&relation, RELPERSISTENCE_PERMANENT, operations[n]);
		MyDatabaseId = saved_database;
		owner = ko_completions;
		UT_ASSERT(owner != NULL);
		if (owner == NULL)
			continue;
		UT_ASSERT(owner->native_pending && owner->barrier_committed_discard);
		UT_ASSERT(!owner->barrier_private && !owner->origin_data_closed);
		UT_ASSERT(ko_shared_registration_snapshot_v3(owner, &snapshot));
		UT_ASSERT(storage.contexts[owner->slot].committed_discard);
		UT_ASSERT_EQ(marker_registrations, 1);
		UT_ASSERT_EQ(native_allocate_calls, 1);
		assert_no_object_work();
		cluster_ko_shared_release_v2(&owner);
		UT_ASSERT_EQ(native_allocated, 0);
	}
}

UT_TEST(test_committed_registration_accepts_new_only_capabilities)
{
	const uint16 operations[] = { CLUSTER_KO_SHARED_V3_OPERATION_DROP,
		CLUSTER_KO_SHARED_V3_OPERATION_REWRITE_OLD };
	for (unsigned n = 0; n < lengthof(operations); n++) {
		KoAxBinding binding = committed_origin_setup();
		ClusterKoCompletionV2 *owner;
		KoDropIdentity *identity;
		committed_origin_only_caps = true;
		object_wire_missing_capabilities = PGRAC_IC_HELLO_CAP_KO_OBJECT_DRAIN_V3;
		owner = committed_origin_register(operations[n], &binding);
		if (owner == NULL)
			continue;
		UT_ASSERT(owner->barrier_committed_discard);
		UT_ASSERT_EQ(storage.contexts[owner->slot].request_v3.operation, operations[n]);
		UT_ASSERT(storage.contexts[owner->slot].drop_identity != 0);
		if (storage.contexts[owner->slot].drop_identity != 0) {
			identity = &storage.drop_identities[storage.contexts[owner->slot].drop_identity - 1];
			UT_ASSERT(identity->committed_discard);
			UT_ASSERT_EQ(identity->state, KO_DROP_IDENTITY_RESERVED);
			UT_ASSERT(memcmp(&identity->request, &owner->barrier_request_v3,
				sizeof(identity->request)) == 0);
		}
		assert_no_object_work();
		cluster_ko_shared_release_v2(&owner);
		UT_ASSERT_EQ(native_allocated, 0);
	}
}

UT_TEST(test_new_origin_refusal_never_falls_back_to_live_v3)
{
	for (unsigned fault = 0; fault < 7; fault++) {
		KoAxBinding binding = committed_origin_setup();
		ClusterKoCompletionV2 *owner = NULL;
		volatile bool caught = false;
		uint16 operation = CLUSTER_KO_SHARED_V3_OPERATION_DROP;
		if (fault == 0) committed_origin_missing_caps = PGRAC_IC_HELLO_CAP_KO_COMMIT_FENCE_V1;
		if (fault == 1) object_wire_missing_capabilities = PGRAC_IC_HELLO_CAP_KO_COMMITTED_DISCARD_V1;
		if (fault == 2) object_wire_missing_capabilities = PGRAC_IC_HELLO_CAP_KO_COMMIT_FENCE_V1;
		if (fault == 3) cluster_interconnect_tier = CLUSTER_IC_TIER_2;
		if (fault == 4) cluster_interconnect_tier = CLUSTER_IC_TIER_3;
		if (fault == 5) operation = CLUSTER_KO_SHARED_V3_OPERATION_TRUNCATE;
		if (fault == 6) v3_local_ax = false;
		expecting_error = true;
		PG_TRY(); {
			(void)ko_shared_begin_v3(space_identity.key.locator, RELPERSISTENCE_PERMANENT,
				operation, &binding, &owner);
		}
		PG_CATCH(); { caught = true; }
		PG_END_TRY();
		expecting_error = false;
		UT_ASSERT(caught && owner == NULL);
		UT_ASSERT_EQ(marker_registrations, 0);
		UT_ASSERT_EQ(native_allocated, 0);
		assert_no_object_work();
		cluster_interconnect_tier = CLUSTER_IC_TIER_1;
		if (owner != NULL)
			cluster_ko_shared_release_v2(&owner);
	}
}

UT_TEST(test_committed_registration_cannot_enter_live_barrier)
{
	KoAxBinding binding = committed_origin_setup();
	ClusterKoCompletionV2 *owner = committed_origin_register(CLUSTER_KO_SHARED_V3_OPERATION_DROP,
		&binding);
	if (owner == NULL)
		return;
	UT_ASSERT(!ko_run_shared_barrier_impl_v3(space_identity.key.locator,
		CLUSTER_KO_SHARED_V3_OPERATION_DROP, &binding, owner));
	assert_no_object_work();
	UT_ASSERT(!owner->barrier_private && !storage.contexts[owner->slot].complete);
	cluster_ko_shared_release_v2(&owner);
}

UT_TEST(test_committed_registration_purpose_is_bound_before_claim)
{
	KoAxBinding binding = committed_origin_setup();
	ClusterKoCompletionV2 *owner = committed_origin_register(CLUSTER_KO_SHARED_V3_OPERATION_DROP,
		&binding);
	ClusterKoSharedContext snapshot;
	if (owner == NULL)
		return;
	storage.contexts[owner->slot].committed_discard = false;
	UT_ASSERT(!ko_shared_registration_snapshot_v3(owner, &snapshot));
	assert_no_object_work();
	cluster_ko_shared_release_v2(&owner);
}

UT_TEST(test_actual_new_registration_survives_commit_publication_without_completion)
{
	KoAxBinding binding = committed_origin_setup();
	ClusterKoCompletionV2 *owner = committed_origin_register(CLUSTER_KO_SHARED_V3_OPERATION_DROP,
		&binding), *claimed = NULL;
	ClusterKoNativeContinuation *work;
	if (owner == NULL)
		return;
	UT_ASSERT(cluster_ko_shared_claim_structure(&space_identity, CLUSTER_SPACE_WAL_TOMBSTONE,
		&claimed));
	UT_ASSERT(claimed == owner);
	work = owner->native_reserved;
	cluster_ko_shared_native_commit_v2();
	UT_ASSERT(owner->native_handed_off && work->context.committed_discard);
	cluster_ko_shared_native_publication_v3();
	UT_ASSERT(work->publication_observed && !work->recovery_required);
	UT_ASSERT(work->context.committed_drain_pending && !work->context.complete);
	UT_ASSERT(!work->buffers_released && !work->predelete_ready);
	UT_ASSERT_EQ(storage.native_waiting, 1);
	UT_ASSERT(!cluster_ko_shared_native_prepare_drop_v2(space_identity.key.locator));
	UT_ASSERT(!cluster_ko_directory_drop_committed_v3(space_identity.key.locator));
	assert_no_object_work();
	native_fixture_shared_memory_end();
}

/* Abort releases the private registration, but only the original AX handoff
 * and confirmed S6 result may retire the shared operation and reservation. */
static unsigned
committed_origin_abort_to_release(void)
{
	KoAxBinding binding = committed_origin_setup();
	ClusterKoCompletionV2 *owner;
	ClusterKoSharedContext before;
	LOCKTAG tag;
	unsigned slot;

	committed_origin_only_caps = true;
	object_wire_missing_capabilities = PGRAC_IC_HELLO_CAP_KO_OBJECT_DRAIN_V3;
	owner = committed_origin_register(CLUSTER_KO_SHARED_V3_OPERATION_DROP, &binding);
	if (owner == NULL)
		abort();
	slot = owner->slot;
	before = storage.contexts[slot];
	resource_callback(RESOURCE_RELEASE_BEFORE_LOCKS, false, true, NULL);
	UT_ASSERT(ko_completions == NULL);
	UT_ASSERT_EQ(native_allocated + completion_allocations, 0);
	UT_ASSERT(storage.contexts[slot].used && storage.contexts[slot].origin_detached);
	UT_ASSERT(storage.contexts[slot].committed_discard);
	UT_ASSERT_EQ(storage.contexts[slot].serial, before.serial);
	UT_ASSERT(!storage.contexts[slot].ax_release_requested);
	UT_ASSERT(memcmp(&storage.contexts[slot].request_v3, &before.request_v3,
		sizeof(before.request_v3)) == 0);
	memset(&tag, 0, sizeof(tag));
	SET_LOCKTAG_RELATION(tag, before.request_v3.ax_resid.field1, before.request_v3.ax_resid.field2);
	UT_ASSERT(cluster_ko_ax_release_handoff_v3(&tag, AccessExclusiveLock,
		&before.request_v3.ax_holder));
	UT_ASSERT(ko_origin_release_ready(slot));
	return slot;
}

UT_TEST(test_new_only_abort_waits_for_original_s6_then_reclaims)
{
	unsigned slot = committed_origin_abort_to_release();
	ClusterKoSharedContext before = storage.contexts[slot];
	KoDropIdentity *identity = &storage.drop_identities[before.drop_identity - 1];

	storage.ax_release_cursor = slot;
	shared_send_tick();
	UT_ASSERT_EQ(release_poll_calls, 1);
	UT_ASSERT(storage.contexts[slot].used && !storage.contexts[slot].ax_failed);
	UT_ASSERT_EQ(storage.contexts[slot].serial, before.serial);
	UT_ASSERT_EQ(identity->state, KO_DROP_IDENTITY_RESERVED);
	UT_ASSERT_EQ(storage.contexts[slot].ax_release.exchange.key.request_id,
		before.request_v3.ax_holder.request_id);
	release_poll_result = CLUSTER_GES_REDECLARE_CONFIRMED;
	storage.ax_release_cursor = slot;
	shared_send_tick();
	UT_ASSERT_EQ(release_poll_calls, 2);
	UT_ASSERT(!storage.contexts[slot].used);
	UT_ASSERT_EQ(identity->state, KO_DROP_IDENTITY_FREE);
	assert_no_object_work();
}

UT_TEST(test_new_abort_s6_preserves_owner_on_control_drift)
{
	for (unsigned fault = 0; fault < 3; fault++) {
		unsigned slot = committed_origin_abort_to_release();
		ClusterKoSharedContext before = storage.contexts[slot];
		KoDropIdentity *identity = &storage.drop_identities[before.drop_identity - 1];

		if (fault == 0)
			committed_origin_missing_caps = PGRAC_IC_HELLO_CAP_KO_COMMITTED_DISCARD_V1;
		if (fault == 1)
			object_wire_missing_capabilities |= PGRAC_IC_HELLO_CAP_KO_COMMITTED_DISCARD_V1;
		if (fault == 2)
			committed_origin_revoke_after_release = true;
		release_poll_result = CLUSTER_GES_REDECLARE_CONFIRMED;
		storage.ax_release_cursor = slot;
		shared_send_tick();
		UT_ASSERT_EQ(release_poll_calls, fault == 2 ? 1 : 0);
		UT_ASSERT(storage.contexts[slot].used && storage.contexts[slot].ax_failed);
		UT_ASSERT_EQ(storage.contexts[slot].serial, before.serial);
		UT_ASSERT_EQ(identity->state, KO_DROP_IDENTITY_RESERVED);
		UT_ASSERT(memcmp(&identity->request, &before.request_v3,
			sizeof(identity->request)) == 0);
		assert_no_object_work();
	}
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
		UT_ASSERT_EQ(cluster_ko_native_failure_count("abandon"), 1);
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
	for (unsigned fault = 0; fault < 15; fault++) {
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
		case 14: entry->committed_discard = true; break;
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
	UT_ASSERT_EQ(cluster_ko_native_failure_count("abandon"), 1);
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
	for (unsigned fault = 0; fault < 15; fault++) {
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
		case 14:
			entry->committed_discard = work->context.committed_discard = true;
			break;
		}
		cluster_ko_shared_native_publication_v3();
		UT_ASSERT(work->recovery_required && storage.native_recovery_required);
		UT_ASSERT_EQ(cluster_ko_native_failure_count(fault == 12
			? "producer_untracked" : "registration_publication"), 1);
		UT_ASSERT_EQ(work->first_failure.site, CLUSTER_KO_NATIVE_SITE_REGISTRATION);
		UT_ASSERT_EQ(work->first_failure.stage, CLUSTER_KO_NATIVE_STAGE_REGISTRATION);
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

/* The native commit tail and KO producer/LMON/ACK consumers are real.
 * WAL commit, local PG cleanup, transport and peer physical work are fixture
 * boundaries. Separate original local consumers follow READY, never final SPACE. */
static unsigned window_mode;
static bool window_expect_ready;
static bool window_expect_closed;
static unsigned window_origin_case;
static uint32 window_scan_capacity;
static unsigned window_final_case;
static unsigned window_close_case, window_close_samples;
/* Only the new directed diagnostics tests enable this observer. */
static const char *native_observation_reason;
static uint32 native_observation_stage;
static unsigned native_observation_multibyte_offset;

static ClusterKoDirectoryIoV3 window_entered;
static uint32 window_interrupt_holdoff, window_query_cancel_holdoff;
static ClusterKoSharedMessageV4 window_request;
static ClusterKoNativeContinuation *window_work;
static unsigned window_slot;

/* Inject only after the real final-SPACE consumer has saved its result and
 * released its entered call. No close fact or native carrier is fabricated. */
static void
window_close_fault(void)
{
	ClusterKoSharedContext *entry = &storage.contexts[window_slot];
	KoDropIdentity *identity;

	if (!postcommit_owner->space_observed)
		return;
	relation_gate_sample_hook = NULL;
	window_close_samples++;
	identity = &storage.drop_identities[window_work->context.drop_identity - 1];
	UT_ASSERT(!ko_directory_io_pending_locked(entry->request.key.locator));
	switch (window_close_case) {
	case 0: break;
	case 1:
		storage.directory_io[0].serial = ++storage.directory_io_serial;
		storage.directory_io[0].pid = MyProcPid + 1;
		storage.directory_io[0].locator = entry->request.key.locator;
		break;
	case 2:
		storage.cleaner_work[0].used = true;
		storage.cleaner_work[0].locator = entry->request.key.locator;
		break;
	case 3:
		storage.extend_work[0].used = true;
		storage.extend_work[0].locator = entry->request.key.locator;
		break;
	case 4: window_work->publication_observed = false; break;
	case 5: window_work->context.request_done_peers = 0; break;
	case 6: entry->serial++; break;
	case 7: writer.timeline++; break;
	case 8: window_work->context.terminal.record_end++; break;
	case 9: pg_atomic_fetch_add_u64(&storage.peer_admission_revision, 1); break;
	case 10: local_writeback_closed = false; break;
	case 11: relation_gate_ok = false; break;
	case 12: native_fixture_error(); break;
	case 13: postcommit_owner->terminal.record_end++; break;
	case 14: identity->fence_serial++; break;
	case 15: committed_origin_missing_caps = PGRAC_IC_HELLO_CAP_KO_COMMITTED_DISCARD_V1; break;
	case 16: window_work->recovery_required = true; break;
	case 17: window_work->context.committed_discard = false; break;
	case 18: postcommit_owner->drop_identity_serial = UINT64_MAX; break;
	}
}

static void
window_origin_discard(void)
{
	RelFileLocator locator = window_work->context.request.key.locator;
	ClusterKoDirectoryIoV3 call;

	UT_ASSERT_EQ(postcommit_phases, 1);
	UT_ASSERT_EQ(postcommit_cleanup_mask, 15);
	UT_ASSERT_EQ(postcommit_owner->barrier_ready_peers, 2);
	UT_ASSERT(cluster_ko_local_drop_discard_current_v4(locator));
	UT_ASSERT(!cluster_ko_directory_drop_retired_v3(locator));
	UT_ASSERT(!cluster_ko_directory_io_begin_v3(locator, &call));
	UT_ASSERT_EQ(flush_count + sync_count + drop_count + native_fork_unlinks, 0);
	switch (window_origin_case) {
	case 2:
		UT_ASSERT_EQ(committed_discard_input, (committed_discard_calls - 1) * 128);
		committed_discard_result = committed_discard_calls < (window_scan_capacity != 0
			? window_scan_capacity / 128 : 3)
			? CLUSTER_BUFFER_DRAIN_MORE : CLUSTER_BUFFER_DRAIN_EMPTY;
		break;
	case 7:
		if (committed_discard_calls == 1) {
			/* A prior consumer's tuple must not become cut-failure evidence. */
			ClusterKoDiscardDetailV1 stale = {
				.locator = locator, .source = CLUSTER_KO_DISCARD_WB, .valid = true
			};
			cluster_ko_origin_discard_detail_v1(&stale);
			(void)pg_atomic_fetch_add_u64(&storage.peer_admission_revision, 1);
		}
		else
			UT_ASSERT_EQ(committed_discard_input, 0);
		break;
	case 8: relation_gate_ok = false; break;
	case 9: storage.contexts[window_slot].serial++; break;
	case 10:
		InterruptHoldoffCount = QueryCancelHoldoffCount = CritSectionCount = 0;
		native_fixture_error();
		break;
	case 13: {
		ClusterKoCompletionV2 saved = *postcommit_owner;
		ClusterKoSharedContext context = storage.contexts[window_slot];
		for (unsigned fault = 0; fault < 9; fault++) {
			switch (fault) {
			case 0: postcommit_owner->pid++; break;
			case 1: postcommit_owner->owner = (ResourceOwner)2; break;
			case 2: postcommit_owner->postcommit = false; break;
			case 3: postcommit_owner->native_handed_off = false; break;
			case 4: window_work->recovery_required = true; break;
			case 5: window_work->publication_observed = false; break;
			case 6: storage.contexts[window_slot].request_v3.base.incarnation[0]++; break;
			case 7: storage.contexts[window_slot].request_done_peers = 0; break;
			case 8: InterruptHoldoffCount = 0; break;
			}
			UT_ASSERT(!cluster_ko_local_drop_discard_current_v4(locator));
			*postcommit_owner = saved;
			window_work->recovery_required = false;
			window_work->publication_observed = true;
			storage.contexts[window_slot] = context;
			InterruptHoldoffCount = window_interrupt_holdoff;
		}
		UT_ASSERT(cluster_ko_local_drop_discard_current_v4(locator));
		break;
	}
	}
}

static void
window_local_scan_continue(RelFileLocator locator pg_attribute_unused())
{
	uint32 end = window_scan_capacity != 0 ? window_scan_capacity : 256;
	local_relation_scan_advance = 128;
	local_relation_scan_result = local_relation_scan_input + 128 < end
		? CLUSTER_PCM_PI_RELATION_MORE : CLUSTER_PCM_PI_RELATION_EMPTY;
}

static void
window_wb_revision(void)
{
	local_writeback_revision++;
	local_writeback_sample_hook = NULL;
}

static void
window_deliver(uint16 status)
{
	ClusterKoSharedMessageV4 ack = window_request;
	ClusterICEnvelope env = { .source_node_id = ack.object.base.peer_node,
		.payload_length = CLUSTER_KO_SHARED_V4_BYTES };
	uint8 bytes[CLUSTER_KO_SHARED_V4_BYTES];

	ack.object.base.verb = CLUSTER_KO_SHARED_ACK;
	ack.object.base.status = status;
	UT_ASSERT(cluster_ko_shared_encode_v4(&ack, bytes, sizeof(bytes)));
	cluster_ko_flush_ack_handler(&env, bytes);
}

static ClusterICSendResult
window_send(uint8 type, int32 dest, const void *bytes, uint32 length)
{
	UT_ASSERT_EQ(type, PGRAC_IC_MSG_KO_FLUSH);
	UT_ASSERT_EQ(dest, 1);
	UT_ASSERT_EQ(length, CLUSTER_KO_SHARED_V4_BYTES);
	UT_ASSERT(cluster_ko_shared_decode_v4(bytes, length, &window_request));
	UT_ASSERT_EQ(postcommit_cleanup_mask, 15);
	UT_ASSERT_EQ(postcommit_phases, 1); /* Native LOCKS have not been released. */
	UT_ASSERT(InterruptHoldoffCount != 0 && window_work->publication_observed);
	UT_ASSERT(postcommit_owner->native_handed_off && postcommit_owner->postcommit);
	UT_ASSERT_EQ(postcommit_owner->barrier_ready_peers, 0);
	send_calls++;
	if (window_mode == 1 && send_calls == 1)
		return CLUSTER_IC_SEND_NOT_ADMITTED;
	if (window_mode == 4)
		return CLUSTER_IC_SEND_HARD_ERROR;
	if (window_mode == 5) {
		/* Native errfinish(ERROR) clears these before PG_RE_THROW. A plain
		 * fixture longjmp would miss the committed caller's restore duty. */
		InterruptHoldoffCount = QueryCancelHoldoffCount = CritSectionCount = 0;
		native_fixture_error();
	}
	if (window_mode == 2 || window_mode == 6) {
		/* A wait-table notification is not an exact operation result. */
		cluster_sinval_ack_wait_record(window_request.object.base.batch_id, dest);
		UT_ASSERT_EQ(storage.contexts[window_slot].request_done_peers, 0);
		return CLUSTER_IC_SEND_WOULD_BLOCK;
	}
	window_deliver(window_mode == 3 ? CLUSTER_KO_SHARED_FAILED : CLUSTER_KO_SHARED_DONE);
	UT_ASSERT_EQ(storage.contexts[window_slot].request_sending_peers, 2);
	UT_ASSERT(!window_work->context.complete);
	return CLUSTER_IC_SEND_DONE;
}

static bool window_observability;

static void
window_wait(void)
{
	UT_ASSERT_EQ(postcommit_cleanup_mask, 15);
	UT_ASSERT_EQ(postcommit_phases, 1);
	if (!barrier_active) {
		RelFileLocator locator = window_work->context.request.key.locator;

		if (window_observability && window_origin_case == 7)
			UT_ASSERT(!ko_origin_discard_detail.valid);
		UT_ASSERT(!postcommit_owner->origin_data_closed);
		UT_ASSERT(!cluster_ko_directory_drop_retired_v3(locator));
		if (window_origin_case == 1) {
			int32 origin_pid = MyProcPid;

			UT_ASSERT_EQ(committed_discard_calls + local_discard_calls, 0);
			UT_ASSERT(!cluster_ko_local_drop_discard_current_v4(locator));
			/* Only the other original owner can end this call. */
			MyProcPid = window_entered.pid;
			UT_ASSERT(cluster_ko_directory_io_end_v3(&window_entered));
			MyProcPid = origin_pid;
		}
		if (window_origin_case == 3)
			committed_discard_result = CLUSTER_BUFFER_DRAIN_EMPTY;
		if (window_origin_case == 4 || window_origin_case == 5) {
			UT_ASSERT_EQ(local_discard_calls, 1);
			local_relation_scan_result = CLUSTER_PCM_PI_RELATION_EMPTY;
		}
		if (window_origin_case == 6)
			local_writeback_closed = true;
		if (window_origin_case == 14 || window_origin_case == 15) {
			UT_ASSERT_EQ(committed_discard_calls + local_discard_calls, 0);
			UT_ASSERT(!cluster_ko_local_drop_discard_current_v4(locator));
			/* Explicit original finite-owner completion boundary. */
			storage.cleaner_work[0].used = false;
			storage.extend_work[0].used = false;
		}
		if (window_origin_case >= 16) {
			/* Exit a buggy wait deterministically; it may not claim that
			 * this original self/failed obligation can finish independently. */
			UT_ASSERT_EQ(origin_waits, 0);
			relation_gate_ok = false;
		}
		return;
	}
	if (window_mode == 6)
		current_epoch++;
	else if (window_mode == 2) {
		if (barrier_waits == 2)
			window_deliver(CLUSTER_KO_SHARED_DONE);
	} else
		shared_send_tick();
}

static bool
window_final_space_fixture(const ClusterKoCompletionV2 *owner,
	const ClusterKoDirectoryIoV3 *io, ClusterPageWalBindingV1 *terminal, void *wal)
{
	ClusterKoDirectoryIoV3 wrong = *io, ordinary;
	ClusterSpaceIdentity expected = space_identity;
	ClusterKoSharedContext *entry = &storage.contexts[window_slot];
	KoDropIdentity *identity = &storage.drop_identities[entry->drop_identity - 1];

	UT_ASSERT(owner == postcommit_owner && owner->origin_data_closed);
	UT_ASSERT_EQ(identity->state, KO_DROP_IDENTITY_CLOSING);
	UT_ASSERT(cluster_ko_native_space_current_v4(owner, io, &expected));
	UT_ASSERT(!cluster_ko_directory_io_begin_v3(expected.key.locator, &ordinary));
	UT_ASSERT(!cluster_ko_directory_drop_retired_v3(expected.key.locator));
	wrong.serial++;
	UT_ASSERT(!cluster_ko_native_space_current_v4(owner, &wrong, &expected));
	wrong = *io; wrong.pid++;
	UT_ASSERT(!cluster_ko_native_space_current_v4(owner, &wrong, &expected));
	expected.operation++;
	UT_ASSERT(!cluster_ko_native_space_current_v4(owner, io, &expected));
	v3_structure_record(true, space_identity.operation, terminal, wal);
	switch (window_final_case) {
	case 1: break;
	case 2: terminal->record_start = 0; break;
	case 3: terminal->flags = 0; break;
	case 4: terminal->identity.forknum = MAIN_FORKNUM; break;
	case 5: terminal->version.mutation_token++; break;
	case 6: terminal->source.timeline++; break;
	case 7: terminal->info = 0; break;
	case 8: entry->request_done_peers = 0; break;
	case 9: entry->origin_detached = true; break;
	case 10: window_work->recovery_required = true; break;
	case 11: identity->state = KO_DROP_IDENTITY_RETIRED; break;
	case 12: entry->serial++; break;
	case 13: v3_local_ax = false; break;
	case 14: relation_gate_ok = false; break;
	case 15: native_fixture_error(); break;
	case 16: ((uint8 *)wal)[0] ^= 1; break;
	case 17: terminal->identity.system_identifier++; break;
	case 18: terminal->identity.storage_uuid[0]++; break;
	}
	if (window_final_case == 1)
		relation_gate_sample_hook = window_close_fault;
	return true;
}

static bool
window_storage_observer(RelFileLocator locator)
{
	bool closed = window_expect_closed && window_final_case == 1 && window_close_case == 0;

	UT_ASSERT_EQ(InterruptHoldoffCount, window_interrupt_holdoff);
	UT_ASSERT_EQ(QueryCancelHoldoffCount, window_query_cancel_holdoff);
	UT_ASSERT(RelFileLocatorEquals(locator, window_work->context.request.key.locator));
	UT_ASSERT_EQ(postcommit_owner->barrier_ready_peers, window_expect_ready ? 2 : 0);
	UT_ASSERT_EQ(window_work->context.request_done_peers,
		window_expect_ready && window_close_case != 5 ? 2 : 0);
	UT_ASSERT_EQ(window_work->context.committed_drain_pending, !closed);
	UT_ASSERT_EQ(window_work->context.complete, closed);
	UT_ASSERT_EQ(window_work->buffers_released, closed);
	UT_ASSERT_EQ(window_work->predelete_ready, closed);
	UT_ASSERT(!postcommit_owner->barrier_private);
	UT_ASSERT_EQ(postcommit_owner->space_observed, window_expect_closed && window_final_case == 1);
	UT_ASSERT_EQ(window_work->context.structure_owned, window_expect_closed && window_final_case == 1);
	if (window_final_case == 1) {
		UT_ASSERT_EQ(memcmp(&postcommit_owner->terminal, &window_work->context.terminal,
			sizeof(postcommit_owner->terminal)) == 0,
			window_close_case != 8 && window_close_case != 13);
		UT_ASSERT(!cluster_ko_native_space_current_v4(postcommit_owner, &window_entered, &space_identity));
	}
	UT_ASSERT_EQ(postcommit_owner->origin_data_closed, window_expect_closed);
	UT_ASSERT_EQ(window_work->context.origin_data_closed, window_expect_closed);
	if (window_origin_case == 0) {
		UT_ASSERT_EQ(committed_discard_calls, window_expect_ready ? 1 : 0);
		UT_ASSERT_EQ(local_discard_calls, window_expect_ready ? 1 : 0);
	}
	UT_ASSERT_EQ(cluster_ko_directory_drop_retired_v3(locator), closed || window_final_case == 11);
	UT_ASSERT(!cluster_ko_shared_native_prepare_drop_v2(locator));
	UT_ASSERT(!ko_origin_release_ready(window_slot));
	UT_ASSERT_EQ(cluster_ko_local_drop_discard_current_v4(locator), closed || window_final_case == 11);
	if (closed) {
		ClusterKoSharedContext *origin = &storage.contexts[window_slot];
		UT_ASSERT(origin->used && origin->complete && !origin->committed_drain_pending);
		UT_ASSERT(!origin->structure_owned && !origin->structure_drop_pending);
		UT_ASSERT_EQ(origin->drop_identity, 0);
		UT_ASSERT_EQ(origin->drop_identity_serial, 0);
		UT_ASSERT(!DsaPointerIsValid(postcommit_owner->native_unready));
		UT_ASSERT_EQ(window_work->context.structure_peers_accepted, 0);
	}
	UT_ASSERT_EQ(native_fork_unlinks + flush_count + sync_count + drop_count, 0);
	return true;
}

static void
run_committed_window(uint16 operation, unsigned mode, bool held, unsigned origin_case)
{
	KoAxBinding binding = committed_origin_setup();
	ClusterKoCompletionV2 *owner, *claimed = NULL;
	volatile bool caught = false;

	committed_origin_only_caps = true;
	object_wire_missing_capabilities = PGRAC_IC_HELLO_CAP_KO_OBJECT_DRAIN_V3;
	if (window_final_case != 0) {
		space_identity.sequence = 1;
		writer.claim.identity.origin_thread_id = 1;
		writer.claim.identity.thread_claim_created_at = 123;
		writer.claim.identity.root_lineage_seq = 1;
		writer.claim.identity.authority_uuid[0] = 17;
		writer.claim.max_config_generation = 1;
		writer.claim.claim_sha256[0] = 19;
		writer.timeline = 1;
	}
	owner = committed_origin_register(operation, &binding);
	if (owner == NULL)
		return;
	UT_ASSERT(owner->barrier_committed_discard);
	if (!owner->barrier_committed_discard) {
		cluster_ko_shared_release_v2(&owner);
		return;
	}
	UT_ASSERT(cluster_ko_shared_claim_structure(&space_identity, CLUSTER_SPACE_WAL_TOMBSTONE,
		&claimed));
	UT_ASSERT(claimed == owner);
	window_work = owner->native_reserved;
	window_slot = owner->slot;
	window_close_samples = 0;
	window_mode = mode;
	window_origin_case = origin_case;
	if (window_scan_capacity != 0 || origin_case == 4)
		local_relation_scan_hook = window_local_scan_continue;
	if (window_scan_capacity != 0)
		native_window_origin_wait_limit = 64;
	native_window_space = mode == 7 ? NULL : (ClusterSpaceDropState *)&window_request;
	native_window_space_owner = mode == 8 ? NULL : owner;
	native_window_space_finish_hook = window_final_case == 0 ? NULL : window_final_space_fixture;
	native_fixture_error_multibyte_offset = native_observation_multibyte_offset;
	window_expect_ready = held && mode < 3;
	window_expect_closed = window_expect_ready
		&& (origin_case < 8 || (origin_case > 11 && origin_case < 16));
	postcommit_owner = owner;
	memset(&postcommit_binding, 0, sizeof(postcommit_binding));
	postcommit_binding.identity.locator = owner->barrier_request_v3.base.key.locator;
	postcommit_drop = true;
	postcommit_error = postcommit_scope_changed = postcommit_buffer_error = false;
	postcommit_callback_error = false;
	postcommit_deletes = postcommit_phases = postcommit_cleanup_mask = 0;
	postcommit_native_count = 1;
	postcommit_storage_consumer = window_storage_observer;
	native_fork_unlinks = native_fork_closes = native_smgr_invalidations = 0;
	native_commit_active = true;
	object_wire_send_hook = window_send;
	lmon_wakeup_hook = shared_send_tick;
	native_window_wait_hook = window_wait;
	committed_discard_hook = window_origin_discard;
	local_scan_rel_number = owner->barrier_request_v3.base.key.locator.relNumber;
	if (origin_case == 1 || origin_case == 16) {
		int32 origin_pid = MyProcPid;

		if (origin_case == 1)
			MyProcPid++;
		UT_ASSERT(cluster_ko_directory_io_begin_v3(space_identity.key.locator, &window_entered));
		MyProcPid = origin_pid;
	}
	if (origin_case == 3)
		committed_discard_result = CLUSTER_BUFFER_DRAIN_WAIT;
	if (origin_case == 4) {
		local_relation_scan_result = CLUSTER_PCM_PI_RELATION_MORE;
		local_relation_scan_advance = 128;
	}
	if (origin_case == 5)
		local_relation_scan_result = CLUSTER_PCM_PI_RELATION_PENDING;
	if (origin_case == 6)
		local_writeback_closed = false;
	if (origin_case == 11)
		local_relation_scan_drift = 4;
	if (origin_case == 12)
		local_writeback_sample_hook = window_wb_revision;
	if (origin_case == 14 || origin_case == 18) {
		storage.cleaner_work[0].used = true;
		storage.cleaner_work[0].locator = space_identity.key.locator;
		storage.cleaner_work[0].scope.pid = origin_case == 18 ? MyProcPid : MyProcPid + 1;
	}
	if (origin_case == 15 || origin_case == 17 || origin_case == 19) {
		storage.extend_work[0].used = true;
		storage.extend_work[0].locator = space_identity.key.locator;
		storage.extend_work[0].pid = origin_case == 17 ? MyProcPid : MyProcPid + 1;
		storage.extend_work[0].failed = origin_case == 19;
	}
	UT_ASSERT_EQ(send_calls, 0); /* No REQUEST escaped the real precommit producer. */
	window_interrupt_holdoff = held ? mode == 5 ? 3 : 1 : 0;
	window_query_cancel_holdoff = held && mode == 5 ? 2 : 0;
	InterruptHoldoffCount = window_interrupt_holdoff;
	QueryCancelHoldoffCount = window_query_cancel_holdoff;
	expecting_error = true;
	PG_TRY(); { run_native_postcommit(); }
	PG_CATCH(); { caught = true; }
	PG_END_TRY();
	expecting_error = false;
	UT_ASSERT_EQ(InterruptHoldoffCount, window_interrupt_holdoff);
	UT_ASSERT_EQ(QueryCancelHoldoffCount, window_query_cancel_holdoff);
	if (held && InterruptHoldoffCount != 0) {
		/* The original CommitTransaction tail owns this matching resume. */
		RESUME_INTERRUPTS();
		UT_ASSERT_EQ(InterruptHoldoffCount, window_interrupt_holdoff - 1);
	}
	InterruptHoldoffCount = 0;
	QueryCancelHoldoffCount = 0;
	postcommit_storage_consumer = NULL;
	UT_ASSERT(!caught);
	UT_ASSERT_EQ(postcommit_deletes, 1);
	UT_ASSERT_EQ(native_fork_unlinks, 0);
	UT_ASSERT_EQ(native_window_space_checks, held ? 1 : 0);
	UT_ASSERT_EQ(native_window_space_releases, mode == 7 ? 0 : 1);
	UT_ASSERT_EQ(native_window_space_finishes, window_expect_closed ? 1 : 0);
	UT_ASSERT_EQ(send_calls, !held || mode >= 7 ? 0 : mode == 1 ? 2 : 1);
	UT_ASSERT(!barrier_active);
	UT_ASSERT_EQ(native_window_errors_flushed,
		held && (mode == 5 || origin_case == 10 || origin_case == 11 || window_final_case == 15
			|| (window_final_case == 1 && window_close_case == 12)) ? 1 : 0);
	if (held && mode == 2)
		UT_ASSERT_EQ(barrier_waits, 2);
	UT_ASSERT_EQ(window_work->recovery_required,
		!(window_expect_closed && window_final_case == 1 && window_close_case == 0));
	UT_ASSERT_EQ(storage.native_recovery_required, window_work->recovery_required);
	UT_ASSERT(storage.contexts[window_slot].used && !ko_origin_release_ready(window_slot));
	UT_ASSERT_EQ(native_free_calls, 0);
	if (held && (mode == 4 || mode == 5))
		UT_ASSERT_EQ(storage.contexts[window_slot].request_uncertain_peers, 2);
	UT_ASSERT_EQ(origin_drain_calls + origin_flush_calls + origin_invalidate_calls, 0);
	if (window_expect_closed) {
		UT_ASSERT(committed_discard_calls > 0 && local_discard_calls > 0);
		UT_ASSERT_EQ(cluster_hw_lease_next_block(space_identity.key.locator, MAIN_FORKNUM),
			InvalidBlockNumber);
	}
	if (origin_case == 2) {
		UT_ASSERT_EQ(committed_discard_calls, window_scan_capacity != 0
			? window_scan_capacity / 128 : 3);
		UT_ASSERT_EQ(origin_waits, 0);
	}
	if (origin_case == 4)
		UT_ASSERT_EQ(local_relation_scan_input, 128);
	if (origin_case == 5 || origin_case == 7 || origin_case == 12)
		UT_ASSERT(committed_discard_calls >= 2);
	if (origin_case >= 16) {
		UT_ASSERT_EQ(origin_waits + committed_discard_calls + local_discard_calls, 0);
		if (origin_case == 16)
			UT_ASSERT_EQ(storage.directory_io[window_entered.slot].serial, window_entered.serial);
		else if (origin_case == 18)
			UT_ASSERT(storage.cleaner_work[0].used);
		else
			UT_ASSERT(storage.extend_work[0].used);
		if (origin_case == 19)
			UT_ASSERT(storage.extend_work[0].failed);
	}
	if (window_final_case == 1) {
		UT_ASSERT_EQ(window_close_samples, 1);
		/* The original backend caller cannot delete. Only after its buffer
		 * callback may CP move the same carrier into the original file owner. */
		if (window_close_case == 0) {
			ClusterKoSharedContext before = window_work->context;
			bool found = false;
			MyBackendType = B_CHECKPOINTER;
			UT_ASSERT_EQ(cluster_ko_shared_native_promote_v2(), CLUSTER_KO_STRUCTURE_PROGRESS);
			UT_ASSERT_EQ(storage.native_waiting, 0);
			for (unsigned i = 0; i < CLUSTER_KO_SHARED_CAPACITY; i++) {
				ClusterKoSharedContext *entry = &storage.contexts[i];
				ClusterKoSharedContext origin;
				ClusterKoDropWorkV2 *file_work = NULL;
				KoDropIdentity identity;
				ClusterKoDirectoryIoV3 denied;
				uint32 cursor = i;
				if (!entry->used || !entry->structure_owned)
					continue;
				UT_ASSERT(!found && i != window_slot);
				before.serial = entry->serial;
				UT_ASSERT(memcmp(&before, entry, sizeof(before)) == 0);
				origin = storage.contexts[window_slot];
				identity = storage.drop_identities[before.drop_identity - 1];
				/* Original file-completion consumer at its durable-provider
				 * boundary. Real provider effects are tested in drop_work;
				 * this fixture does not claim physical unlink or durability. */
				local_relation_scan_advance = 128;
				for (unsigned segment = 0; segment < 16; segment++) {
					cursor = i;
					UT_ASSERT(cluster_ko_shared_drop_work_begin_v2(&cursor, 1, &file_work));
					if (file_work == NULL || cluster_ko_shared_drop_work_revalidate_v2(file_work))
						break;
					file_work = NULL; /* Only a borrowed pointer; the original owner retains it. */
				}
				UT_ASSERT(file_work != NULL);
				if (file_work == NULL)
					break;
				UT_ASSERT(cluster_ko_shared_drop_work_finish_v2(&file_work));
				UT_ASSERT(file_work == NULL && !entry->used);
				UT_ASSERT(memcmp(&origin, &storage.contexts[window_slot], sizeof(origin)) == 0);
				UT_ASSERT(memcmp(&identity, &storage.drop_identities[before.drop_identity - 1],
					sizeof(identity)) == 0);
				UT_ASSERT(cluster_ko_directory_drop_retired_v3(before.request.key.locator));
				UT_ASSERT(!cluster_ko_directory_io_begin_v3(before.request.key.locator, &denied));
				found = true;
			}
			UT_ASSERT(found);
			UT_ASSERT_EQ(native_fork_unlinks, 0);
		} else {
			UT_ASSERT(window_work->context.committed_drain_pending);
			UT_ASSERT(!window_work->buffers_released);
		}
	}
	if (window_observability) {
		const char *reason = origin_case == 1 ? "directory_pending"
			: origin_case == 2 ? "buffer_more" : origin_case == 3 ? "buffer_wait"
			: origin_case == 4 ? "pi_more" : origin_case == 5 ? "pi_pending"
			: origin_case == 6 ? "wb_not_closed" : origin_case == 7 ? "buffer_cut"
			: origin_case == 14 ? "cleaner_pending" : "extend_pending";
		UT_ASSERT(cluster_ko_origin_discard_count(reason) > 0);
		UT_ASSERT_EQ(cluster_ko_origin_discard_count("unknown"), 0);
		UT_ASSERT_EQ(cluster_ko_origin_checkpoint_wait_count("drain_wait"), 0);
	}
	if (native_observation_reason != NULL) {
		ClusterKoNativeFailureV1 first, after;

		memset(&first, 0x5a, sizeof(first));
		after = first;
		if (strcmp(native_observation_reason, "none") == 0) {
			after = first;
			UT_ASSERT(!cluster_ko_native_failure_first("window_close", &first));
			UT_ASSERT(memcmp(&first, &after, sizeof(first)) == 0);
			for (int i = 1; i < CLUSTER_KO_NATIVE_FAIL_COUNT; i++)
				UT_ASSERT_EQ(cluster_ko_native_failure_count(
					cluster_ko_native_failure_reason_name(i)), 0);
		} else {
			const char *stop_reason = NULL;
			unsigned logs = native_failure_log_events;

			memset(&first, 0, sizeof(first));
			UT_ASSERT(cluster_ko_native_failure_first(native_observation_reason, &first));
			UT_ASSERT_EQ(first.site, CLUSTER_KO_NATIVE_SITE_WINDOW);
			UT_ASSERT_EQ(first.stage, native_observation_stage);
			UT_ASSERT_EQ(first.space_stage, native_observation_stage == CLUSTER_KO_NATIVE_STAGE_SPACE
				? (window_final_case == 2 ? CLUSTER_KO_NATIVE_SPACE_POSTCHECK
					: native_window_space_diagnostic) : CLUSTER_KO_NATIVE_SPACE_NONE);
			UT_ASSERT(RelFileLocatorEquals(first.locator, space_identity.key.locator));
			UT_ASSERT_EQ(first.serial, window_work->context.serial);
			UT_ASSERT_EQ(first.batch, window_work->context.request.batch_id);
			UT_ASSERT_EQ(first.producer_pid, window_work->context.pid);
			UT_ASSERT_EQ(cluster_ko_native_failure_count(native_observation_reason), 1);
			if (strcmp(native_observation_reason, "window_error") == 0) {
				UT_ASSERT_EQ(first.sqlerrcode, ERRCODE_INTERNAL_ERROR);
				UT_ASSERT_EQ(strlen(first.error_message), native_observation_multibyte_offset == 190
					? 190 : CLUSTER_KO_NATIVE_ERROR_SUMMARY_BYTES - 1);
				if (native_observation_multibyte_offset == 188)
					UT_ASSERT(memcmp(first.error_message + 188, "\xe4\xb8\xad", 3) == 0);
				UT_ASSERT(strncmp(first.error_message, "fixture native ERROR summary ", 28) == 0);
				UT_ASSERT(strchr(first.error_message, '\n') == NULL);
			} else
				UT_ASSERT_STR_EQ(first.error_message, "");
			/* Stop must carry the saved first failure even before CP runs. */
			UT_ASSERT_EQ(cluster_ko_shared_normal_stop_poll_v2(&stop_reason), CLUSTER_NORMAL_STOP_INVALID);
			UT_ASSERT_STR_EQ(stop_reason, "KO_SHARED_COMMITTED_RECOVERY_REQUIRED");
			UT_ASSERT_EQ(native_failure_log_events, logs + 1);
			UT_ASSERT(strstr(last_native_failure_log, "consumer=normal_stop") != NULL);
			UT_ASSERT(strstr(last_native_failure_log, "site=7") != NULL);
			UT_ASSERT(strstr(last_native_failure_log, "serial=") != NULL);
			UT_ASSERT(strstr(last_native_failure_log, "batch=") != NULL);
			{
				char field[32];
				snprintf(field, sizeof(field), "space_stage=%u ", first.space_stage);
				UT_ASSERT(strstr(last_native_failure_log, field) != NULL);
			}
			if (strcmp(native_observation_reason, "window_error") == 0)
				UT_ASSERT(strstr(last_native_failure_log,
					"error_summary=\"fixture native ERROR summary") != NULL);
			UT_ASSERT_EQ(cluster_ko_shared_normal_stop_poll_v2(&stop_reason), CLUSTER_NORMAL_STOP_INVALID);
			UT_ASSERT_EQ(native_failure_log_events, logs + 1);
			/* Secondary CP failure and producer exit must retain the original
			 * snapshot rather than naming promotion as the first cause. */
			MyBackendType = B_CHECKPOINTER;
			UT_ASSERT_EQ(cluster_ko_shared_native_promote_v2(), CLUSTER_KO_STRUCTURE_INVALID);
			UT_ASSERT_EQ(native_failure_log_events, logs + 2);
			UT_ASSERT(strstr(last_native_failure_log, "consumer=promote") != NULL);
			UT_ASSERT(cluster_ko_native_failure_first(native_observation_reason, &after));
			UT_ASSERT(memcmp(&first, &after, sizeof(first)) == 0);
			UT_ASSERT_EQ(cluster_ko_native_failure_count(native_observation_reason), 1);
			UT_ASSERT_EQ(cluster_ko_native_failure_count("promotion_cut"), 0);
			UT_ASSERT(window_work->recovery_required && storage.native_recovery_required);
		}
		UT_ASSERT_EQ(cluster_ko_native_failure_count(NULL), 0);
		UT_ASSERT_EQ(cluster_ko_native_failure_count("unknown"), 0);
		UT_ASSERT(!cluster_ko_native_failure_first("unknown", &after));
	}
	native_fixture_shared_memory_end();
}

UT_TEST(test_native_window_first_failure_has_exact_stage_and_work)
{
	native_observation_reason = "window_owner";
	native_observation_stage = CLUSTER_KO_NATIVE_STAGE_OWNER;
	run_committed_window(CLUSTER_KO_SHARED_V3_OPERATION_DROP, 7, true, 0);
	native_observation_reason = "window_wait";
	native_observation_stage = CLUSTER_KO_NATIVE_STAGE_WAIT;
	run_committed_window(CLUSTER_KO_SHARED_V3_OPERATION_DROP, 3, true, 0);
	native_observation_reason = "window_discard";
	native_observation_stage = CLUSTER_KO_NATIVE_STAGE_DISCARD;
	run_committed_window(CLUSTER_KO_SHARED_V3_OPERATION_DROP, 0, true, 8);
	native_observation_reason = "window_space";
	native_observation_stage = CLUSTER_KO_NATIVE_STAGE_SPACE;
	window_final_case = 2;
	run_committed_window(CLUSTER_KO_SHARED_V3_OPERATION_DROP, 0, true, 0);
	native_observation_reason = "window_close";
	native_observation_stage = CLUSTER_KO_NATIVE_STAGE_CLOSE;
	window_final_case = window_close_case = 1;
	run_committed_window(CLUSTER_KO_SHARED_V3_OPERATION_DROP, 0, true, 0);
	window_final_case = window_close_case = 0;
	/* A character crossing the byte limit must be omitted; one ending
	 * exactly at that limit must survive intact through work/log/dump. */
	native_observation_reason = "window_error";
	native_observation_stage = CLUSTER_KO_NATIVE_STAGE_SPACE;
	window_final_case = 15;
	native_observation_multibyte_offset = 190;
	run_committed_window(CLUSTER_KO_SHARED_V3_OPERATION_DROP, 0, true, 0);
	native_observation_multibyte_offset = 188;
	run_committed_window(CLUSTER_KO_SHARED_V3_OPERATION_DROP, 0, true, 0);
	native_observation_multibyte_offset = 0;
	window_final_case = 0;
	native_observation_reason = NULL;
}

UT_TEST(test_native_window_caught_error_retains_evaluating_stage)
{
	native_observation_reason = "window_error";
	native_observation_stage = CLUSTER_KO_NATIVE_STAGE_WAIT;
	run_committed_window(CLUSTER_KO_SHARED_V3_OPERATION_DROP, 5, true, 0);
	native_observation_stage = CLUSTER_KO_NATIVE_STAGE_DISCARD;
	run_committed_window(CLUSTER_KO_SHARED_V3_OPERATION_DROP, 0, true, 10);
	native_observation_stage = CLUSTER_KO_NATIVE_STAGE_SPACE;
	window_final_case = 15;
	run_committed_window(CLUSTER_KO_SHARED_V3_OPERATION_DROP, 0, true, 0);
	native_observation_stage = CLUSTER_KO_NATIVE_STAGE_CLOSE;
	window_final_case = 1; window_close_case = 12;
	run_committed_window(CLUSTER_KO_SHARED_V3_OPERATION_DROP, 0, true, 0);
	window_final_case = window_close_case = 0;
	native_observation_reason = NULL;
}

UT_TEST(test_native_space_substage_survives_work_promotion_and_stop)
{
	native_observation_reason = "window_space";
	native_observation_stage = CLUSTER_KO_NATIVE_STAGE_SPACE;
	for (native_window_space_diagnostic = CLUSTER_KO_NATIVE_SPACE_ENTERED_IO;
		native_window_space_diagnostic <= CLUSTER_KO_NATIVE_SPACE_IO_END;
		native_window_space_diagnostic++)
		run_committed_window(CLUSTER_KO_SHARED_V3_OPERATION_DROP, 0, true, 0);
	native_window_space_diagnostic = CLUSTER_KO_NATIVE_SPACE_PUBLICATION;
	native_observation_reason = NULL;
}

UT_TEST(test_native_healthy_window_has_no_first_failure)
{
	native_observation_reason = "none";
	window_final_case = 1;
	run_committed_window(CLUSTER_KO_SHARED_V3_OPERATION_DROP, 0, true, 0);
	window_final_case = 0;
	native_observation_reason = NULL;
}

UT_TEST(test_native_abandon_and_promotion_have_distinct_write_ids)
{
	ClusterKoCompletionV2 *owner = claimed_registration(CLUSTER_KO_SHARED_V3_OPERATION_DROP);
	ClusterKoNativeContinuation *work = owner->native_reserved;
	ClusterKoNativeFailureV1 first;

	UT_ASSERT(commit_registration());
	ko_native_abandon_unready(owner);
	UT_ASSERT(cluster_ko_native_failure_first("abandon", &first));
	UT_ASSERT_EQ(first.site, CLUSTER_KO_NATIVE_SITE_ABANDON);
	UT_ASSERT_EQ(first.stage, CLUSTER_KO_NATIVE_STAGE_ABANDON);
	UT_ASSERT(memcmp(&first, &work->first_failure, sizeof(first)) == 0);
	native_fixture_shared_memory_end();

	prepare_postcommit(false);
	UT_ASSERT(cluster_ko_shared_observe_truncate_v2(postcommit_owner));
	native_commit_active = true;
	cluster_ko_shared_native_commit_v2();
	current_epoch++;
	MyBackendType = B_CHECKPOINTER;
	UT_ASSERT_EQ(cluster_ko_shared_native_promote_v2(), CLUSTER_KO_STRUCTURE_INVALID);
	UT_ASSERT(cluster_ko_native_failure_first("promotion_cut", &first));
	UT_ASSERT_EQ(first.site, CLUSTER_KO_NATIVE_SITE_PROMOTE);
	UT_ASSERT_EQ(first.stage, CLUSTER_KO_NATIVE_STAGE_PROMOTE);
	native_fixture_shared_memory_end();
}

UT_TEST(test_committed_window_final_space_keeps_original_owner_and_negative_identity)
{
	for (window_final_case = 1; window_final_case <= 18; window_final_case++)
		run_committed_window(CLUSTER_KO_SHARED_V3_OPERATION_DROP, 0, true, 0);
	window_final_case = 0;
}

UT_TEST(test_committed_window_closes_same_native_owner_without_duplicate_file_work)
{
	window_final_case = 1;
	run_committed_window(CLUSTER_KO_SHARED_V3_OPERATION_DROP, 0, true, 0);
	run_committed_window(CLUSTER_KO_SHARED_V3_OPERATION_REWRITE_OLD, 0, true, 0);
	window_final_case = 0;
}

UT_TEST(test_production_capability_selects_complete_committed_window)
{
	committed_origin_production_caps = true;
	window_final_case = 1;
	run_committed_window(CLUSTER_KO_SHARED_V3_OPERATION_DROP, 0, true, 0);
	run_committed_window(CLUSTER_KO_SHARED_V3_OPERATION_REWRITE_OLD, 0, true, 0);
	window_final_case = 0;
	committed_origin_production_caps = false;
}

UT_TEST(test_production_origin_refuses_old_peer_before_publication)
{
	for (unsigned fault = 0; fault < 2; fault++) {
		KoAxBinding binding = committed_origin_setup();
		ClusterKoCompletionV2 *owner = NULL;
		volatile bool caught = false;

		native_capability_hook = native_real_capability_word;
		object_wire_missing_capabilities = fault == 0
			? PGRAC_IC_HELLO_CAP_KO_COMMITTED_DISCARD_V1
			: PGRAC_IC_HELLO_CAP_KO_COMMIT_FENCE_V1;
		expecting_error = true;
		PG_TRY(); {
			(void)ko_shared_begin_v3(space_identity.key.locator, RELPERSISTENCE_PERMANENT,
				CLUSTER_KO_SHARED_V3_OPERATION_DROP, &binding, &owner);
		}
		PG_CATCH(); { caught = true; }
		PG_END_TRY();
		expecting_error = false;
		UT_ASSERT(caught && owner == NULL);
		UT_ASSERT_EQ(marker_registrations, 0);
		if (owner != NULL)
			cluster_ko_shared_release_v2(&owner);
		UT_ASSERT_EQ(native_allocated, 0);
		assert_no_object_work();
	}
}

UT_TEST(test_production_origin_cannot_use_legacy_barrier_with_no_peers)
{
	KoAxBinding binding = origin_marker_setup();
	ClusterKoSharedMessageV3 request = { 0 };
	uint64 boots[CLUSTER_KO_SHARED_NODE_LIMIT];
	uint32 mask = 0;

	native_capability_hook = native_real_capability_word;
	formation.membership.membership_state[1] = CLUSTER_MEMBER_DEAD;
	formation.membership.last_admitted_incarnation[1] = 0;
	UT_ASSERT(!ko_shared_prepare_request_v3(space_identity.key.locator,
		CLUSTER_KO_SHARED_V3_OPERATION_DROP, &binding, &request, boots, &mask));
	UT_ASSERT_EQ(space_reads + marker_registrations + native_allocated + barrier_requests, 0);
}

UT_TEST(test_committed_window_close_rejects_changed_proof_and_unclosed_work)
{
	window_final_case = 1;
	for (window_close_case = 1; window_close_case <= 18; window_close_case++)
		run_committed_window(CLUSTER_KO_SHARED_V3_OPERATION_DROP, 0, true, 0);
	window_final_case = window_close_case = 0;
}

UT_TEST(test_actual_commit_window_collects_ready_after_local_cleanup)
{
	run_committed_window(CLUSTER_KO_SHARED_V3_OPERATION_DROP, 0, true, 0);
	run_committed_window(CLUSTER_KO_SHARED_V3_OPERATION_REWRITE_OLD, 0, true, 0);
}

UT_TEST(test_commit_window_keeps_backpressure_and_exact_result_waits)
{
	run_committed_window(CLUSTER_KO_SHARED_V3_OPERATION_DROP, 1, true, 0);
	run_committed_window(CLUSTER_KO_SHARED_V3_OPERATION_DROP, 2, true, 0);
}

UT_TEST(test_commit_window_failure_retains_original_owner_without_error)
{
	for (unsigned mode = 3; mode <= 8; mode++)
		run_committed_window(CLUSTER_KO_SHARED_V3_OPERATION_DROP, mode, true, 0);
}

UT_TEST(test_commit_window_never_enters_without_held_interrupts)
{
	run_committed_window(CLUSTER_KO_SHARED_V3_OPERATION_DROP, 0, false, 0);
}

UT_TEST(test_committed_origin_waits_for_original_io_and_finite_owners)
{
	run_committed_window(CLUSTER_KO_SHARED_V3_OPERATION_DROP, 0, true, 1);
	run_committed_window(CLUSTER_KO_SHARED_V3_OPERATION_DROP, 0, true, 14);
	run_committed_window(CLUSTER_KO_SHARED_V3_OPERATION_DROP, 0, true, 15);
}

UT_TEST(test_committed_origin_discard_counts_the_actual_wait_branch)
{
	const unsigned cases[] = {1, 2, 3, 4, 5, 6, 7, 14, 15};
	window_observability = true;
	for (unsigned i = 0; i < lengthof(cases); i++)
		run_committed_window(CLUSTER_KO_SHARED_V3_OPERATION_DROP, 0, true, cases[i]);
	window_observability = false;
}

UT_TEST(test_committed_origin_preserves_bounded_scan_and_wait_progress)
{
	for (unsigned origin_case = 2; origin_case <= 6; origin_case++)
		run_committed_window(CLUSTER_KO_SHARED_V3_OPERATION_DROP, 0, true, origin_case);
}

UT_TEST(test_committed_origin_rescans_changed_revisions_without_self_invalidation)
{
	run_committed_window(CLUSTER_KO_SHARED_V3_OPERATION_DROP, 0, true, 7);
	run_committed_window(CLUSTER_KO_SHARED_V3_OPERATION_DROP, 0, true, 12);
}

UT_TEST(test_committed_origin_drift_and_error_keep_native_obligation)
{
	for (unsigned origin_case = 8; origin_case <= 11; origin_case++)
		run_committed_window(CLUSTER_KO_SHARED_V3_OPERATION_DROP, 0, true, origin_case);
}

UT_TEST(test_committed_origin_discard_scope_is_not_generic_io_or_retirement)
{
	run_committed_window(CLUSTER_KO_SHARED_V3_OPERATION_DROP, 0, true, 13);
}

UT_TEST(test_committed_origin_self_or_failed_work_hands_off_without_waiting)
{
	for (unsigned origin_case = 16; origin_case <= 19; origin_case++)
		run_committed_window(CLUSTER_KO_SHARED_V3_OPERATION_DROP, 0, true, origin_case);
}

UT_TEST(test_prepared_mode_never_falls_back_from_changed_committed_identity)
{
	KoAxBinding binding = committed_origin_setup();
	ClusterKoCompletionV2 *owner = committed_origin_register(
		CLUSTER_KO_SHARED_V3_OPERATION_DROP, &binding), *claimed = NULL;
	bool mode = false;

	UT_ASSERT(!cluster_ko_shared_drop_prepare_mode_v4(owner, &mode));
	UT_ASSERT(!mode);
	UT_ASSERT(cluster_ko_shared_claim_structure(&space_identity, CLUSTER_SPACE_WAL_TOMBSTONE,
		&claimed));
	UT_ASSERT(claimed == owner);
	UT_ASSERT(cluster_ko_shared_drop_prepare_mode_v4(owner, &mode) && mode);
	UT_ASSERT(!cluster_ko_shared_drop_write_current(owner, &space_identity));
	owner->barrier_committed_discard = false;
	UT_ASSERT(!cluster_ko_shared_drop_prepare_mode_v4(owner, &mode));
	UT_ASSERT(mode); /* Failed classification leaves output unchanged. */
	owner->barrier_committed_discard = true;
	storage.contexts[owner->slot].serial++;
	UT_ASSERT(!cluster_ko_shared_drop_prepare_mode_v4(owner, &mode));
	UT_ASSERT(mode);
	storage.contexts[owner->slot].serial--;
	UT_ASSERT(cluster_ko_shared_drop_prepare_mode_v4(owner, &mode) && mode);
	owner->postcommit = true;
	UT_ASSERT(!cluster_ko_shared_drop_prepare_mode_v4(owner, &mode));
	owner->postcommit = false;
	assert_no_object_work();
	cluster_ko_shared_release_v2(&owner);
}

UT_TEST(test_prepared_mode_keeps_original_completed_legacy_owner)
{
	ClusterKoCompletionV2 *owner = v3_native_structure_owner(
		CLUSTER_KO_SHARED_V3_OPERATION_DROP), *claimed = NULL;
	bool mode = true;

	UT_ASSERT(cluster_ko_shared_claim_structure(&space_identity, CLUSTER_SPACE_WAL_TOMBSTONE,
		&claimed));
	UT_ASSERT(claimed == owner);
	UT_ASSERT(cluster_ko_shared_drop_prepare_mode_v4(owner, &mode) && !mode);
	UT_ASSERT(cluster_ko_shared_drop_write_current(owner, &space_identity));
	cluster_ko_shared_release_v2(&owner);
}

UT_TEST(test_two_object_16mb_discard_timer_budget)
{
	unsigned waits = 0;
	window_scan_capacity = 16 * 1024 * 1024 / BLCKSZ;
	window_final_case = 1;
	for (unsigned object = 0; object < 2; object++) {
		run_committed_window(CLUSTER_KO_SHARED_V3_OPERATION_DROP, 0, true, 2);
		waits += origin_waits;
		UT_ASSERT_EQ(committed_discard_calls, 16);
		UT_ASSERT(local_discard_calls >= 16);
	}
	window_scan_capacity = window_final_case = 0;
	printf("# two 16MB object scans: requested timed waits=%u ms\n", waits * 100);
	UT_ASSERT(waits * 100 <= 1000);
}

int
main(void)
{
	UT_PLAN(61);
	UT_RUN(test_two_object_16mb_discard_timer_budget);
	UT_RUN(test_native_abandon_and_promotion_have_distinct_write_ids);
	UT_RUN(test_native_window_first_failure_has_exact_stage_and_work);
	UT_RUN(test_native_space_substage_survives_work_promotion_and_stop);
	UT_RUN(test_native_window_caught_error_retains_evaluating_stage);
	UT_RUN(test_native_healthy_window_has_no_first_failure);
	UT_RUN(test_production_capability_selects_complete_committed_window);
	UT_RUN(test_production_origin_refuses_old_peer_before_publication);
	UT_RUN(test_production_origin_cannot_use_legacy_barrier_with_no_peers);
	UT_RUN(test_committed_window_closes_same_native_owner_without_duplicate_file_work);
	UT_RUN(test_committed_window_close_rejects_changed_proof_and_unclosed_work);
	UT_RUN(test_committed_origin_self_or_failed_work_hands_off_without_waiting);
	UT_RUN(test_committed_origin_waits_for_original_io_and_finite_owners);
	UT_RUN(test_committed_origin_discard_counts_the_actual_wait_branch);
	UT_RUN(test_committed_origin_preserves_bounded_scan_and_wait_progress);
	UT_RUN(test_committed_origin_rescans_changed_revisions_without_self_invalidation);
	UT_RUN(test_committed_origin_drift_and_error_keep_native_obligation);
	UT_RUN(test_committed_origin_discard_scope_is_not_generic_io_or_retirement);
	UT_RUN(test_prepared_mode_never_falls_back_from_changed_committed_identity);
	UT_RUN(test_prepared_mode_keeps_original_completed_legacy_owner);
	UT_RUN(test_committed_window_final_space_keeps_original_owner_and_negative_identity);
	UT_RUN(test_actual_commit_window_collects_ready_after_local_cleanup);
	UT_RUN(test_commit_window_keeps_backpressure_and_exact_result_waits);
	UT_RUN(test_commit_window_failure_retains_original_owner_without_error);
	UT_RUN(test_commit_window_never_enters_without_held_interrupts);
	UT_RUN(test_committed_native_entry_registers_without_precommit_object_work);
	UT_RUN(test_committed_registration_accepts_new_only_capabilities);
	UT_RUN(test_new_origin_refusal_never_falls_back_to_live_v3);
	UT_RUN(test_committed_registration_cannot_enter_live_barrier);
	UT_RUN(test_committed_registration_purpose_is_bound_before_claim);
	UT_RUN(test_actual_new_registration_survives_commit_publication_without_completion);
	UT_RUN(test_new_only_abort_waits_for_original_s6_then_reclaims);
	UT_RUN(test_new_abort_s6_preserves_owner_on_control_drift);
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
