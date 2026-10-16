/* Native KO routing with the production capability producer and COMMIT/CP
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
	return native_real_capability_word() & ~missing_capabilities;
}

static uint32
native_legacy_capabilities(void)
{
	return PGRAC_IC_HELLO_CAP_PI_STRUCTURAL_V2;
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
	native_capability_hook = native_real_capability_word;
}

UT_TEST(test_real_v3_commit_transfers_before_callbacks_and_cp_promotes)
{
	bool found = false;

	prepare_native_v3();
	UT_ASSERT_EQ(native_real_capability_word() & PGRAC_IC_HELLO_CAP_PI_STRUCTURAL_V2, 0);
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

UT_TEST(test_real_v3_commit_callback_error_retains_native_owner)
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
		native_capability_hook = native_real_capability_word;
		UT_ASSERT(run_native_checkpointer());
		UT_ASSERT_EQ(storage.native_waiting, 0);
		native_fixture_shared_memory_end();
	}
}

UT_TEST(test_v3_capabilities_do_not_commit_legacy_work)
{
	prepare_postcommit(false);
	UT_ASSERT(cluster_ko_shared_observe_truncate_v2(postcommit_owner));
	native_capability_hook = native_real_capability_word;
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
	native_capability_hook = native_real_capability_word;
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

	native_capability_hook = native_real_capability_word;
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
	UT_PLAN(7);
	UT_RUN(test_real_v3_commit_transfers_before_callbacks_and_cp_promotes);
	UT_RUN(test_real_v3_commit_callback_error_retains_native_owner);
	UT_RUN(test_v3_commit_requires_both_capabilities_and_tier1);
	UT_RUN(test_queued_v3_keeps_its_original_capability_requirement);
	UT_RUN(test_v3_capabilities_do_not_commit_legacy_work);
	UT_RUN(test_v3_poll_cannot_promote_legacy_native_work);
	UT_RUN(test_v3_capabilities_do_not_execute_published_legacy_drop);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
