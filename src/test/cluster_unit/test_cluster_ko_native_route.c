/* Legacy KO routing with explicit old-binary capabilities and real COMMIT/CP
 * callers. GES, SPACE durability, publication and buffer cleanup retain the
 * explicit standalone fixtures; no local cleanup is a physical IO proof.
 * Author: SqlRush <sqlrush@gmail.com> */
int ko_stop_fixture_main(void);
#define main ko_stop_fixture_main
#include "test_cluster_ko_stop.c"
#undef main

static uint32 missing_capabilities;
static uint32
native_incomplete_capabilities(void)
{
	return native_legacy_v3_capability_word() & ~missing_capabilities;
}

static uint32
native_legacy_capabilities(void)
{
	return PGRAC_IC_HELLO_CAP_PI_STRUCTURAL_V2;
}

static uint32
native_committed_capabilities(void)
{
	return (PGRAC_IC_HELLO_CAP_KO_COMMITTED_DISCARD_V1
			| PGRAC_IC_HELLO_CAP_KO_COMMIT_FENCE_V1) & ~missing_capabilities;
}

static void
prepare_native_v3(void)
{
	ClusterKoCompletionV2 *owner
		= v3_native_structure_owner(CLUSTER_KO_SHARED_V3_OPERATION_DROP), *claimed = NULL;

	UT_ASSERT(cluster_ko_shared_claim_structure(&space_identity,
		CLUSTER_SPACE_WAL_TOMBSTONE, &claimed));
	UT_ASSERT(claimed == owner);
	v3_structure_record(true, space_identity.operation, &postcommit_binding, postcommit_wal);
	UT_ASSERT(cluster_ko_shared_observe_space_v2(owner, &postcommit_binding,
		postcommit_wal, sizeof(postcommit_wal)));
	postcommit_owner = owner;
	postcommit_drop = true;
	postcommit_error = postcommit_scope_changed = postcommit_callback_error = false;
	postcommit_deletes = postcommit_phases = 0;
	postcommit_native_count = 1;
	postcommit_storage_consumer = NULL;
	postcommit_buffer_error = false;
	native_commit_active = true;
	native_capability_hook = native_legacy_v3_capability_word;
}

/* Supply an authenticated result at the old private-capture boundary. The
 * production PKO4 registration/window is covered by the registration tests;
 * native capture/COMMIT/publication/CP consumers below are real. Old DATA,
 * final SPACE and all-peer READY remain the existing explicit fixtures, not
 * evidence of the committed-discard producer or SQL completion. */
static void
prepare_committed_capture(void)
{
	ClusterKoSharedContext *context;

	prepare_native_v3();
	context = &storage.contexts[postcommit_owner->slot];
	context->committed_discard = true;
	storage.drop_identities[context->drop_identity - 1].committed_discard = true;
	postcommit_owner->barrier_private = false;
	missing_capabilities = 0;
	object_wire_missing_capabilities = PGRAC_IC_HELLO_CAP_KO_OBJECT_DRAIN_V3;
	native_capability_hook = native_committed_capabilities;
}

static bool
capture_committed_owner(void)
{
	bool captured;

	prepare_committed_capture();
	captured = ko_native_barrier_to_owner(postcommit_owner);
	UT_ASSERT(captured);
	if (!captured)
		native_fixture_shared_memory_end();
	return captured;
}

UT_TEST(test_committed_purpose_survives_private_snapshot_and_native_cp)
{
	ClusterKoSharedContext snapshot;
	ClusterKoNativeContinuation *work;
	bool found = false;

	if (!capture_committed_owner())
		return;
	UT_ASSERT(ko_completion_snapshot(postcommit_owner, &snapshot));
	UT_ASSERT(snapshot.committed_discard && snapshot.v3);
	run_native_postcommit();
	UT_ASSERT_EQ(storage.native_waiting, 1);
	work = dsa_get_address(ko_native_area, storage.native_incoming);
	UT_ASSERT(work->context.committed_discard);
	UT_ASSERT(!work->recovery_required && work->buffers_released);
	UT_ASSERT_EQ(native_fork_unlinks, 0);
	MyBackendType = B_CHECKPOINTER;
	UT_ASSERT(run_native_checkpointer());
	UT_ASSERT_EQ(storage.native_waiting, 0);
	for (unsigned i = 0; i < CLUSTER_KO_SHARED_CAPACITY; i++)
		found |= storage.contexts[i].used && storage.contexts[i].committed_discard
			&& storage.contexts[i].structure_drop_pending;
	UT_ASSERT(found);
	native_fixture_shared_memory_end();
}

UT_TEST(test_committed_capture_requires_its_own_control_not_live_v3)
{
	for (unsigned fault = 0; fault < 6; fault++) {
		ClusterKoSharedContext before;
		prepare_committed_capture();
		before = storage.contexts[postcommit_owner->slot];
		if (fault < 2)
			missing_capabilities = fault == 0 ? PGRAC_IC_HELLO_CAP_KO_COMMITTED_DISCARD_V1
				: PGRAC_IC_HELLO_CAP_KO_COMMIT_FENCE_V1;
		else if (fault < 4)
			cluster_interconnect_tier = fault == 2 ? CLUSTER_IC_TIER_2 : CLUSTER_IC_TIER_3;
		else if (fault == 4)
			native_capability_hook = native_legacy_v3_capability_word;
		else
			object_wire_missing_capabilities |= PGRAC_IC_HELLO_CAP_KO_COMMITTED_DISCARD_V1;
		UT_ASSERT(!ko_native_barrier_to_owner(postcommit_owner));
		UT_ASSERT(!postcommit_owner->barrier_private);
		UT_ASSERT(memcmp(&before, &storage.contexts[postcommit_owner->slot], sizeof(before)) == 0);
		UT_ASSERT_EQ(storage.native_waiting, 0);
		cluster_interconnect_tier = CLUSTER_IC_TIER_1;
		native_fixture_shared_memory_end();
	}
}

UT_TEST(test_committed_private_owner_cannot_downgrade_at_commit)
{
	if (!capture_committed_owner())
		return;
	native_capability_hook = native_legacy_v3_capability_word;
	object_wire_missing_capabilities = 0;
	cluster_ko_shared_native_commit_v2();
	UT_ASSERT(!postcommit_owner->native_handed_off);
	UT_ASSERT_EQ(storage.native_waiting, 0);
	UT_ASSERT(DsaPointerIsValid(postcommit_owner->native_allocation));
	native_fixture_shared_memory_end();
}

UT_TEST(test_committed_queued_owner_keeps_purpose_across_capability_loss)
{
	for (unsigned fault = 0; fault < 5; fault++) {
		if (!capture_committed_owner())
			return;
		run_native_postcommit();
		MyBackendType = B_CHECKPOINTER;
		if (fault < 2)
			missing_capabilities = fault == 0 ? PGRAC_IC_HELLO_CAP_KO_COMMITTED_DISCARD_V1
				: PGRAC_IC_HELLO_CAP_KO_COMMIT_FENCE_V1;
		else if (fault < 4)
			cluster_interconnect_tier = fault == 2 ? CLUSTER_IC_TIER_2 : CLUSTER_IC_TIER_3;
		else
			native_capability_hook = native_legacy_v3_capability_word;
		UT_ASSERT(!run_native_checkpointer());
		UT_ASSERT_EQ(storage.native_waiting, 1);
		UT_ASSERT(!storage.native_recovery_required);
		UT_ASSERT_EQ(native_fork_unlinks, 0);
		cluster_interconnect_tier = CLUSTER_IC_TIER_1;
		missing_capabilities = 0;
		native_capability_hook = native_committed_capabilities;
		UT_ASSERT(run_native_checkpointer());
		UT_ASSERT_EQ(storage.native_waiting, 0);
		native_fixture_shared_memory_end();
	}
}

UT_TEST(test_committed_capability_does_not_promote_live_or_legacy)
{
	for (unsigned v3 = 0; v3 < 2; v3++) {
		if (v3)
			prepare_native_v3();
		else {
			prepare_postcommit(false);
			UT_ASSERT(cluster_ko_shared_observe_truncate_v2(postcommit_owner));
			native_commit_active = true;
		}
		run_native_postcommit();
		MyBackendType = B_CHECKPOINTER;
		missing_capabilities = 0;
		native_capability_hook = native_committed_capabilities;
		UT_ASSERT(!run_native_checkpointer());
		UT_ASSERT_EQ(storage.native_waiting, 1);
		UT_ASSERT(!storage.native_recovery_required);
		UT_ASSERT_EQ(native_fork_unlinks, 0);
		native_fixture_shared_memory_end();
	}
}

UT_TEST(test_native_publication_rejects_changed_committed_purpose)
{
	ClusterKoNativeContinuation *work;
	KoDropIdentity *identity;

	if (!capture_committed_owner())
		return;
	cluster_ko_shared_native_commit_v2();
	UT_ASSERT(postcommit_owner->native_handed_off);
	work = dsa_get_address(ko_native_area, postcommit_owner->native_unready);
	identity = &storage.drop_identities[work->context.drop_identity - 1];
	/* Even a matching legacy carrier and reservation cannot rewrite the
	 * private owner's previously captured committed-discard purpose. */
	work->context.committed_discard = identity->committed_discard = false;
	native_capability_hook = native_legacy_v3_capability_word;
	object_wire_missing_capabilities = 0;
	cluster_ko_shared_native_publication_v3();
	UT_ASSERT(work->recovery_required && storage.native_recovery_required);
	UT_ASSERT_EQ(cluster_ko_native_failure_count("publication"), 1);
	UT_ASSERT_EQ(work->first_failure.site, CLUSTER_KO_NATIVE_SITE_PUBLICATION);
	UT_ASSERT_EQ(work->first_failure.stage, CLUSTER_KO_NATIVE_STAGE_PUBLICATION);
	UT_ASSERT_EQ(identity->state, KO_DROP_IDENTITY_RESERVED);
	UT_ASSERT(!work->buffers_released && !work->predelete_ready);
	native_fixture_shared_memory_end();
}

UT_TEST(test_native_private_publish_records_qualification_or_identity_first)
{
	for (unsigned fault = 0; fault < 2; fault++) {
		ClusterKoCompletionV2 *owner;
		ClusterKoNativeContinuation *work;
		ClusterKoNativeFailureV1 first = {0}, after = {0};
		const char *reason = fault == 0 ? "publish" : "publish_identity";

		prepare_native_v3();
		owner = postcommit_owner;
		work = owner->native_reserved;
		if (fault == 0)
			owner->space_observed = false;
		else
			owner->drop_identity_serial++;
		cluster_ko_shared_native_commit_v2();
		UT_ASSERT(owner->native_handed_off);
		UT_ASSERT(work->recovery_required && storage.native_recovery_required);
		UT_ASSERT(cluster_ko_native_failure_first(reason, &first));
		UT_ASSERT_EQ(first.site, fault == 0 ? CLUSTER_KO_NATIVE_SITE_PUBLISH
			: CLUSTER_KO_NATIVE_SITE_PUBLISH_IDENTITY);
		UT_ASSERT_EQ(first.stage, fault == 0 ? CLUSTER_KO_NATIVE_STAGE_PUBLISH
			: CLUSTER_KO_NATIVE_STAGE_PUBLISH_IDENTITY);
		UT_ASSERT_EQ(first.serial, work->context.serial);
		UT_ASSERT_EQ(first.batch, work->context.request.batch_id);
		UT_ASSERT(RelFileLocatorEquals(first.locator, work->context.request.key.locator));
		UT_ASSERT_EQ(cluster_ko_native_failure_count(reason), 1);
		ko_native_abandon_unready(owner);
		UT_ASSERT(cluster_ko_native_failure_first(reason, &after));
		UT_ASSERT(memcmp(&first, &after, sizeof(first)) == 0);
		UT_ASSERT_EQ(cluster_ko_native_failure_count("abandon"), 0);
		native_fixture_shared_memory_end();
	}
}

UT_TEST(test_native_identity_rejects_wrong_purpose_and_malformed_version)
{
	ClusterKoSharedContext before;
	KoDropIdentity *identity;

	prepare_committed_capture();
	before = storage.contexts[postcommit_owner->slot];
	identity = &storage.drop_identities[before.drop_identity - 1];
	identity->committed_discard = false;
	SpinLockAcquire(&storage.shared_lock);
	UT_ASSERT(ko_origin_drop_identity_locked(&before) == NULL);
	SpinLockRelease(&storage.shared_lock);
	before.v3 = false;
	object_wire_missing_capabilities = 0;
	UT_ASSERT(!ko_shared_origin_current(&before));
	native_fixture_shared_memory_end();
}

UT_TEST(test_legacy_v3_commit_transfers_before_callbacks_and_cp_promotes)
{
	bool found = false;

	prepare_native_v3();
	UT_ASSERT_EQ(native_legacy_v3_capability_word() & PGRAC_IC_HELLO_CAP_PI_STRUCTURAL_V2, 0);
	run_native_postcommit();
	UT_ASSERT_EQ(storage.native_waiting, 1);
	UT_ASSERT_EQ(completion_allocations, 0);
	UT_ASSERT_EQ(native_fork_unlinks, 0);
	MyBackendType = B_CHECKPOINTER;
	UT_ASSERT(run_native_checkpointer());
	UT_ASSERT_EQ(storage.native_waiting, 0);
	for (unsigned i = 0; i < CLUSTER_KO_SHARED_CAPACITY; i++)
		found |= storage.contexts[i].used && storage.contexts[i].v3
			&& storage.contexts[i].structure_drop_pending;
	UT_ASSERT(found);
	native_fixture_shared_memory_end();
}

UT_TEST(test_legacy_v3_commit_callback_error_retains_native_owner)
{
	volatile bool caught = false;
	prepare_native_v3();
	postcommit_callback_error = true;
	PG_TRY(); { run_native_postcommit(); }
	PG_CATCH(); { caught = true; }
	PG_END_TRY();
	UT_ASSERT(caught);
	UT_ASSERT_EQ(storage.native_waiting, 1);
	UT_ASSERT(postcommit_owner->native_handed_off);
	UT_ASSERT_EQ(postcommit_deletes, 0);
	MyBackendType = B_CHECKPOINTER;
	UT_ASSERT(!run_native_checkpointer());
	UT_ASSERT_EQ(storage.native_waiting, 1);
	native_fixture_shared_memory_end();
}

UT_TEST(test_v3_commit_requires_both_capabilities_and_tier1)
{
	for (unsigned fault = 0; fault < 5; fault++) {
		prepare_native_v3();
		if (fault < 2) {
			missing_capabilities = fault == 0 ? PGRAC_IC_HELLO_CAP_KO_OBJECT_DRAIN_V3
				: PGRAC_IC_HELLO_CAP_KO_COMMIT_FENCE_V1;
			native_capability_hook = native_incomplete_capabilities;
		} else if (fault < 4)
			cluster_interconnect_tier = fault == 2 ? CLUSTER_IC_TIER_2 : CLUSTER_IC_TIER_3;
		else
			native_capability_hook = native_legacy_capabilities;
		cluster_ko_shared_native_commit_v2();
		UT_ASSERT(!postcommit_owner->native_handed_off);
		UT_ASSERT_EQ(storage.native_waiting, 0);
		UT_ASSERT(DsaPointerIsValid(postcommit_owner->native_allocation));
		cluster_interconnect_tier = CLUSTER_IC_TIER_1;
		native_fixture_shared_memory_end();
	}
}

UT_TEST(test_queued_v3_keeps_its_original_capability_requirement)
{
	for (unsigned fault = 0; fault < 5; fault++) {
		prepare_native_v3();
		run_native_postcommit();
		MyBackendType = B_CHECKPOINTER;
		if (fault < 2) {
			missing_capabilities = fault == 0 ? PGRAC_IC_HELLO_CAP_KO_OBJECT_DRAIN_V3
				: PGRAC_IC_HELLO_CAP_KO_COMMIT_FENCE_V1;
			native_capability_hook = native_incomplete_capabilities;
		} else if (fault < 4)
			cluster_interconnect_tier = fault == 2 ? CLUSTER_IC_TIER_2 : CLUSTER_IC_TIER_3;
		else
			native_capability_hook = native_legacy_capabilities;
		UT_ASSERT(!run_native_checkpointer());
		UT_ASSERT_EQ(storage.native_waiting, 1);
		UT_ASSERT(!storage.native_recovery_required);
		UT_ASSERT_EQ(native_fork_unlinks, 0);
		cluster_interconnect_tier = CLUSTER_IC_TIER_1;
		native_capability_hook = native_legacy_v3_capability_word;
		UT_ASSERT(run_native_checkpointer());
		UT_ASSERT_EQ(storage.native_waiting, 0);
		native_fixture_shared_memory_end();
	}
}

UT_TEST(test_v3_capabilities_do_not_commit_legacy_work)
{
	prepare_postcommit(false);
	UT_ASSERT(cluster_ko_shared_observe_truncate_v2(postcommit_owner));
	native_capability_hook = native_legacy_v3_capability_word;
	cluster_ko_shared_native_commit_v2();
	UT_ASSERT(!postcommit_owner->native_handed_off);
	UT_ASSERT_EQ(storage.native_waiting, 0);
	native_fixture_shared_memory_end();
}

UT_TEST(test_v3_poll_cannot_promote_legacy_native_work)
{
	prepare_postcommit(false);
	UT_ASSERT(cluster_ko_shared_observe_truncate_v2(postcommit_owner));
	native_commit_active = true;
	cluster_ko_shared_native_commit_v2();
	UT_ASSERT_EQ(storage.native_waiting, 1);
	MyBackendType = B_CHECKPOINTER;
	native_capability_hook = native_legacy_v3_capability_word;
	UT_ASSERT(!cluster_ko_shared_native_poll_v2());
	UT_ASSERT_EQ(storage.native_waiting, 1);
	UT_ASSERT(!storage.native_recovery_required);
	/* The original legacy capability still resumes this exact work. */
	native_capability_hook = NULL;
	UT_ASSERT(cluster_ko_shared_native_poll_v2());
	UT_ASSERT_EQ(storage.native_waiting, 0);
	native_fixture_shared_memory_end();
}

UT_TEST(test_v3_capabilities_do_not_execute_published_legacy_drop)
{
	ClusterPageWalBindingV1 terminal, observed;
	uint8 wal[CLUSTER_SPACE_STRUCTURE_WAL_BYTES], copied[sizeof(wal)];
	uint32 slot = prepare_promoted_drop(&terminal, wal), cursor = slot;
	ClusterKoSharedContext before = storage.contexts[slot];
	ClusterKoDropWorkV2 *work = NULL;

	native_capability_hook = native_legacy_v3_capability_word;
	UT_ASSERT(!cluster_ko_shared_drop_work_begin_v2(&cursor, 16, &work));
	UT_ASSERT(work == NULL);
	UT_ASSERT(memcmp(&storage.contexts[slot], &before, sizeof(before)) == 0);
	/* Restore only this legacy fixture's capability, not v3 authority. */
	native_capability_hook = NULL;
	cursor = slot;
	if (work == NULL)
		UT_ASSERT(cluster_ko_shared_drop_work_begin_v2(&cursor, 16, &work));
	UT_ASSERT(cluster_ko_shared_drop_work_read_v2(work, &observed, copied, sizeof(copied)));
	UT_ASSERT(memcmp(&observed, &terminal, sizeof(terminal)) == 0);
	UT_ASSERT_EQ(native_fork_unlinks, 0);
	exit_callback(0, (Datum)0);
	native_fixture_shared_memory_end();
}

int
main(void)
{
	UT_PLAN(15);
	UT_RUN(test_native_private_publish_records_qualification_or_identity_first);
	UT_RUN(test_committed_purpose_survives_private_snapshot_and_native_cp);
	UT_RUN(test_committed_capture_requires_its_own_control_not_live_v3);
	UT_RUN(test_committed_private_owner_cannot_downgrade_at_commit);
	UT_RUN(test_committed_queued_owner_keeps_purpose_across_capability_loss);
	UT_RUN(test_committed_capability_does_not_promote_live_or_legacy);
	UT_RUN(test_native_publication_rejects_changed_committed_purpose);
	UT_RUN(test_native_identity_rejects_wrong_purpose_and_malformed_version);
	UT_RUN(test_legacy_v3_commit_transfers_before_callbacks_and_cp_promotes);
	UT_RUN(test_legacy_v3_commit_callback_error_retains_native_owner);
	UT_RUN(test_v3_commit_requires_both_capabilities_and_tier1);
	UT_RUN(test_queued_v3_keeps_its_original_capability_requirement);
	UT_RUN(test_v3_capabilities_do_not_commit_legacy_work);
	UT_RUN(test_v3_poll_cannot_promote_legacy_native_work);
	UT_RUN(test_v3_capabilities_do_not_execute_published_legacy_drop);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
