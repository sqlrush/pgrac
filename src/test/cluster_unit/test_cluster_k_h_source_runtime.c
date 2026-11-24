/* Original runtime storage cannot acquire history provenance from a core
 * directory transition. No joint recovery completion is supplied here.
 * Author: SqlRush <sqlrush@gmail.com> */
int k_source_writer_main(void);
#define main k_source_writer_main
#include "test_cluster_wal_writer.c"
#undef main

static void
k_history_source_is_unpublished(void)
{
	ClusterRelmapHistoryCutV1 observed, zero = { 0 };
	RelmapRetentionRuntimeShmem *before = NULL;

	if (relmap_runtime_shmem != NULL) {
		before = malloc(sizeof(*before));
		if (before == NULL)
			abort();
		memcpy(before, relmap_runtime_shmem, sizeof(*before));
	}
	memset(&observed, 0xa5, sizeof(observed));
	UT_ASSERT(!cluster_relmap_retention_runtime_history_observe_v1(&observed));
	UT_ASSERT_EQ(memcmp(&observed, &zero, sizeof(observed)), 0);
	UT_ASSERT(!cluster_relmap_retention_runtime_history_current_v1(&observed));
	if (before != NULL) {
		/* The lock may change its internal accounting; all original directories
		 * and duties must remain byte-for-byte unchanged on refusal. */
		UT_ASSERT_EQ(memcmp(before->directories, relmap_runtime_shmem->directories,
			sizeof(before->directories)), 0);
		free(before);
	}
	UT_ASSERT(!relmap_retention_held);
}

UT_TEST(k_history_birth_and_attachment_do_not_publish_a_source)
{
	relmap_runtime_fixture(false);
	k_history_source_is_unpublished();
	relmap_runtime_registered->init_fn();
	k_history_source_is_unpublished();
}

UT_TEST(k_one_core_complete_origin_does_not_cover_historical_origins)
{
	ClusterRelmapRetentionEmptyV1 local;

	relmap_runtime_fixture(true);
	relmap_runtime_qualified_fixture(0);
	UT_ASSERT(cluster_relmap_retention_runtime_observe_empty_v1(0, &local));
	UT_ASSERT_EQ(relmap_runtime_shmem->directories[1].state,
		CLUSTER_RELMAP_RETENTION_UNKNOWN);
	k_history_source_is_unpublished();
}

UT_TEST(k_all_core_complete_origins_do_not_substitute_for_joint_completion)
{
	ClusterRelmapRetentionEmptyV1 local;

	relmap_runtime_fixture(true);
	for (unsigned i = 0; i < CLUSTER_MAX_NODES; i++) {
		relmap_runtime_qualified_fixture(i);
		UT_ASSERT(cluster_relmap_retention_runtime_observe_empty_v1(i, &local));
	}
	k_history_source_is_unpublished();
	/* Attaching those same bytes is no new reconstruction or provenance. */
	relmap_runtime_registered->init_fn();
	k_history_source_is_unpublished();
}

int
main(void)
{
	UT_PLAN(3);
	UT_RUN(k_history_birth_and_attachment_do_not_publish_a_source);
	UT_RUN(k_one_core_complete_origin_does_not_cover_historical_origins);
	UT_RUN(k_all_core_complete_origins_do_not_substitute_for_joint_completion);
	UT_DONE();
	free(relmap_runtime_allocation);
	return ut_failed_count != 0;
}
