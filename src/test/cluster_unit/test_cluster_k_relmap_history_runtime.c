/* Actual runtime/owner source and original controlled transport leaves. */
int k_relmap_writer_original_main(void);
#define main k_relmap_writer_original_main
#include "test_cluster_wal_writer.c"
#undef main

UT_TEST(test_runtime_empty_source_uses_original_directory_and_lock)
{
	ClusterRelmapRetentionEmptyV1 observed;
	unsigned acquires, reads, assigned;

	relmap_runtime_fixture(true);
	UT_ASSERT(!cluster_relmap_retention_runtime_observe_empty_v1(0, &observed));
	relmap_runtime_qualified_fixture(0);
	acquires = relmap_retention_acquires;
	reads = relmap_binding_reads;
	assigned = relmap_assign_calls;
	UT_ASSERT(cluster_relmap_retention_runtime_observe_empty_v1(0, &observed));
	UT_ASSERT(observed.source == &relmap_runtime_shmem->directories[0]);
	UT_ASSERT(cluster_relmap_retention_runtime_empty_current_v1(&observed));
	UT_ASSERT_EQ(relmap_retention_acquires, acquires + 2);
	UT_ASSERT_EQ(relmap_binding_reads, reads);
	UT_ASSERT_EQ(relmap_assign_calls, assigned);
	UT_ASSERT(!relmap_retention_held);
	UT_ASSERT(!cluster_relmap_retention_runtime_observe_empty_v1(1, &observed));
	UT_ASSERT_EQ(observed.source_serial, 0);
}

UT_TEST(test_runtime_original_begin_cancel_invalidates_prior_empty)
{
	ClusterRelmapOwnerSampleV2 sample;
	ClusterRelmapV2Namespace identity;
	ClusterRelmapRetentionEmptyV1 observed, after;

	relmap_runtime_fixture(true);
	relmap_runtime_qualified_fixture(0);
	UT_ASSERT(cluster_relmap_retention_runtime_observe_empty_v1(0, &observed));
	UT_ASSERT(cluster_relmap_owner_capture_v2(&sample));
	identity = relmap_owner_namespace();
	UT_ASSERT_EQ(cluster_relmap_retention_runtime_begin_v2(&sample, &identity, 8),
		CLUSTER_RELMAP_RETENTION_OK);
	UT_ASSERT_EQ(cluster_relmap_retention_runtime_cancel_prepare_v2(&sample, &identity, 8),
		CLUSTER_RELMAP_RETENTION_OK);
	UT_ASSERT(!cluster_relmap_retention_runtime_empty_current_v1(&observed));
	UT_ASSERT(cluster_relmap_retention_runtime_observe_empty_v1(0, &after));
	UT_ASSERT(after.source_serial > observed.source_serial);
}

UT_TEST(test_runtime_attach_preserves_cut_and_recovery_revokes_it)
{
	ClusterRelmapRetentionEmptyV1 observed;

	relmap_runtime_fixture(true);
	relmap_runtime_qualified_fixture(0);
	UT_ASSERT(cluster_relmap_retention_runtime_observe_empty_v1(0, &observed));
	relmap_runtime_registered->init_fn();
	UT_ASSERT(cluster_relmap_retention_runtime_empty_current_v1(&observed));
	MyAuxProcType = StartupProcess;
	UT_ASSERT(cluster_relmap_retention_runtime_recovery_invalidate_v2());
	MyAuxProcType = NotAnAuxProcess;
	UT_ASSERT(!cluster_relmap_retention_runtime_empty_current_v1(&observed));
	UT_ASSERT(!cluster_relmap_retention_runtime_observe_empty_v1(0, &observed));
}

static void
k_runtime_context_refuses(const ClusterRelmapRetentionEmptyV1 *observed)
{
	ClusterRelmapRetentionEmptyV1 out;
	unsigned acquires = relmap_retention_acquires;
	bool held = relmap_retention_held;

	UT_ASSERT(!cluster_relmap_retention_runtime_observe_empty_v1(0, &out));
	UT_ASSERT(!cluster_relmap_retention_runtime_empty_current_v1(observed));
	UT_ASSERT_EQ(out.source_serial, 0);
	UT_ASSERT_EQ(relmap_retention_acquires, acquires);
	UT_ASSERT_EQ(relmap_retention_held, held);
}

UT_TEST(test_runtime_context_refusals_preserve_all_caller_locks)
{
	ClusterRelmapRetentionEmptyV1 observed;

	relmap_runtime_fixture(true);
	relmap_runtime_qualified_fixture(0);
	UT_ASSERT(cluster_relmap_retention_runtime_observe_empty_v1(0, &observed));
	CritSectionCount = 1;
	k_runtime_context_refuses(&observed);
	CritSectionCount = 0;
	ShutdownRequestPending = true;
	k_runtime_context_refuses(&observed);
	ShutdownRequestPending = false;
	relmap_local_lock = true;
	k_runtime_context_refuses(&observed);
	relmap_local_lock = false;
	LWLockAcquire(XidGenLock, LW_EXCLUSIVE);
	k_runtime_context_refuses(&observed);
	LWLockRelease(XidGenLock);
	LWLockAcquire(relmap_runtime_test_lock, LW_EXCLUSIVE);
	k_runtime_context_refuses(&observed);
	LWLockRelease(relmap_runtime_test_lock);
	UT_ASSERT(cluster_relmap_retention_runtime_empty_current_v1(&observed));
}

UT_TEST(test_runtime_missing_origin_and_output_alias_refuse_without_mutation)
{
	ClusterRelmapRetentionEmptyV1 observed, wrong;
	ClusterRelmapRetentionDirectory before;

	relmap_runtime_fixture(false);
	UT_ASSERT(!cluster_relmap_retention_runtime_observe_empty_v1(0, &observed));
	UT_ASSERT(!cluster_relmap_retention_runtime_empty_current_v1(&observed));
	relmap_runtime_fixture(true);
	relmap_runtime_qualified_fixture(0);
	UT_ASSERT(cluster_relmap_retention_runtime_observe_empty_v1(0, &observed));
	wrong = observed;
	wrong.origin = CLUSTER_MAX_NODES;
	UT_ASSERT(!cluster_relmap_retention_runtime_empty_current_v1(&wrong));
	UT_ASSERT(!cluster_relmap_retention_runtime_observe_empty_v1(CLUSTER_MAX_NODES, &wrong));
	before = relmap_runtime_shmem->directories[0];
	UT_ASSERT(!cluster_relmap_retention_runtime_observe_empty_v1(0,
		(ClusterRelmapRetentionEmptyV1 *)&relmap_runtime_shmem->directories[0].pins[2]));
	UT_ASSERT_EQ(memcmp(&before, &relmap_runtime_shmem->directories[0], sizeof(before)), 0);
	UT_ASSERT(!cluster_relmap_retention_runtime_observe_empty_v1(0, NULL));
	UT_ASSERT(!cluster_relmap_retention_runtime_empty_current_v1(NULL));
}

int
main(void)
{
	UT_PLAN(5);
	UT_RUN(test_runtime_empty_source_uses_original_directory_and_lock);
	UT_RUN(test_runtime_original_begin_cancel_invalidates_prior_empty);
	UT_RUN(test_runtime_attach_preserves_cut_and_recovery_revokes_it);
	UT_RUN(test_runtime_context_refusals_preserve_all_caller_locks);
	UT_RUN(test_runtime_missing_origin_and_output_alias_refuse_without_mutation);
	UT_DONE();
	return ut_failed_count != 0;
}
