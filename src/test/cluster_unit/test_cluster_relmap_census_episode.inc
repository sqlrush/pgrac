/* Same original episode -> original ROOT and real dual-copy census.
 * Only the original all-IR/WALR source/authority leaves are controlled here.
 * Author: SqlRush <sqlrush@gmail.com>
 * Spec: 2026-10-14-pre2-m5-e1-format-closure-v1.md */
#include "cluster/cluster_wal_inputs.h"

/* The parent control bypasses ONLY the missing episode adapter, keeping the
 * exact same physical scanner, directories, source fixture and assertions. */
#ifdef CENSUS_EPISODE_PARENT_CONTROL
static ClusterRelmapCensusResult
cluster_relmap_retention_census_begin_episode(
	const ClusterRecoveryRelmapSourceV1 *source, ClusterRecoveryRootSet *roots,
	ClusterRelmapRetentionDirectory *const directories[], size_t count,
	size_t budget, ClusterRelmapRetentionCensus **out)
{
	return cluster_relmap_retention_census_begin(roots,
		source->selected.source.claim.identity.system_identifier,
		source->selected.source.claim.database_incarnation,
		source->selected.source.claim.identity.storage_uuid, directories, count, budget, out);
}
#endif

static ClusterRecoveryRelmapSourceV1 episode_sources[4];
static uint32 episode_count;
static unsigned episode_owner_calls, episode_reads, episode_failures;
static bool episode_live, episode_error, episode_read_fault, episode_read_error;
static unsigned char episode_cookie;

uint32
cluster_wal_inputs_relmap_count_v1(ClusterWalInputsV1 *inputs)
{
	UT_ASSERT(inputs == (void *)&episode_cookie);
	return episode_count;
}

ClusterRecoveryRelmapSourceV1
cluster_wal_inputs_relmap_source_v1(ClusterWalInputsV1 *inputs, uint32 index)
{
	ClusterRecoveryRelmapSourceV1 empty = {0};
	UT_ASSERT(inputs == (void *)&episode_cookie);
	return index < episode_count && index < lengthof(episode_sources)
		? episode_sources[index] : empty;
}

bool
cluster_wal_inputs_relmap_source_current_v1(const ClusterRecoveryRelmapSourceV1 *source)
{
	episode_owner_calls++;
	if (episode_error)
		siglongjmp(*PG_exception_stack, 1);
	return episode_live && source->scope == (void *)&episode_cookie
		&& source->input_index < episode_count && source->input_index < lengthof(episode_sources)
		&& memcmp(source, &episode_sources[source->input_index], sizeof(*source)) == 0;
}

void
cluster_wal_inputs_relmap_source_fail_v1(const ClusterRecoveryRelmapSourceV1 *source)
{
	UT_ASSERT(source->scope == (void *)&episode_cookie);
	episode_failures++;
	episode_live = false;
}

static void
episode_read(void *bytes, size_t length, off_t offset)
{
	(void)bytes; (void)length; (void)offset;
	episode_reads++;
	if (episode_read_fault)
		episode_sources[0].selected.native_window[159] ^= 1;
	if (episode_read_error)
		episode_error = true;
}

static void
episode_setup(CensusFixture *f, bool pending)
{
	census_fixture(f, pending);
	UT_ASSERT_EQ(mkdirat(f->fd, "base/6", 0700), 0);
	census_pair(f->fd, 6, 7, false, pending, false);
	f->roots = begin_roots(f->fd, 4 * 1024 * 1024);
	UT_ASSERT_EQ(finish_roots(f->roots, NULL), CLUSTER_RECOVERY_ROOTS_OBSERVED);
	census_execute_active = census_execute_recovery = true;
	MyBackendType = B_STARTUP;
	CritSectionCount = 0;
	cluster_shared_catalog = census_merger_held = census_namespace_x = true;
	episode_count = 3;
	episode_owner_calls = episode_reads = episode_failures = 0;
	episode_live = true;
	episode_error = episode_read_fault = episode_read_error = false;
	memset(episode_sources, 0, sizeof(episode_sources));
	for (uint32 i = 0; i < lengthof(episode_sources); i++) {
		ClusterRecoveryRelmapSourceV1 *source = &episode_sources[i];
		source->scope = (void *)&episode_cookie;
		source->work_generation = 73;
		source->ir_request_id = i == 1 ? 903 : 907;
		source->input_index = i;
		source->input_count = episode_count;
		source->replay_index = i == 1 ? 0 : 1;
		source->replay_count = 2;
		source->selected.source.claim.identity.system_identifier = 817;
		source->selected.source.claim.database_incarnation = 91;
		source->selected.source.claim.identity.origin_node_id = i == 1 ? 3 : 7;
		source->selected.source.claim.identity.origin_thread_id = i == 1 ? 3 : 7;
		memcpy(source->selected.source.claim.identity.storage_uuid, root_uuid, 16);
		source->selected.checkpoint.root_publish_seq = 1000 + i;
		source->selected.native_window[159] = i;
		source->replay = episode_sources[i == 1 ? 1 : 2].selected;
	}
	namespace_read_transform = episode_read;
}

static ClusterRelmapCensusResult
episode_begin(CensusFixture *f, size_t count, size_t budget)
{
	return cluster_relmap_retention_census_begin_episode(&episode_sources[1], f->roots,
		f->directories, count, budget, &f->census);
}

static void
episode_cleanup(CensusFixture *f)
{
	episode_error = episode_read_fault = episode_read_error = false;
	namespace_read_transform = NULL;
	census_cleanup(f);
	census_execute_active = census_execute_recovery = false;
	MyBackendType = B_BG_WORKER;
}

UT_TEST(test_episode_census_all_origins_history_and_real_pairs)
{
	CensusFixture f;
	episode_setup(&f, true);
	UT_ASSERT_EQ(episode_begin(&f, 2, 1024 * 1024), CLUSTER_RELMAP_CENSUS_MORE);
	if (f.census != NULL) {
		UT_ASSERT_EQ(census_drain(&f), CLUSTER_RELMAP_CENSUS_DRAINED);
		UT_ASSERT(episode_reads > 0 && episode_owner_calls > 0);
		UT_ASSERT_EQ(episode_failures, 0);
		UT_ASSERT(census_pin(&f.origin[0], 5) != NULL);
		UT_ASSERT(census_pin(&f.origin[1], 6) != NULL);
		UT_ASSERT_EQ(f.origin[0].state, CLUSTER_RELMAP_RETENTION_BUILDING);
		UT_ASSERT_EQ(f.origin[1].state, CLUSTER_RELMAP_RETENTION_BUILDING);
		UT_ASSERT_NE(cluster_relmap_retention_allocation_check(&f.origin[0], CENSUS_FULL),
			CLUSTER_RELMAP_RETENTION_OK);
	}
	episode_cleanup(&f);
}

UT_TEST(test_episode_census_complete_origin_storage_and_budget_preflight)
{
	for (unsigned fault = 0; fault < 4; fault++) {
		CensusFixture f;
		episode_setup(&f, true);
		if (fault == 1) episode_sources[0].selected.source.claim.identity.storage_uuid[0] ^= 1;
		if (fault == 2) episode_sources[0].selected.source.claim.identity.origin_node_id = 9;
		UT_ASSERT(episode_begin(&f, fault == 0 ? 1 : 2, fault == 3 ? 1 : 1024 * 1024)
			> CLUSTER_RELMAP_CENSUS_DRAINED);
		UT_ASSERT(f.census == NULL);
		UT_ASSERT_EQ(episode_reads, 0);
		UT_ASSERT(episode_failures > 0);
		if (fault == 0) {
			uint8 digest[32];
			/* The omitted directory was never borrowed by this operation. */
			UT_ASSERT(!cluster_recovery_root_set_digest(f.roots, digest));
			UT_ASSERT_EQ(f.directories[0]->state, CLUSTER_RELMAP_RETENTION_UNKNOWN);
			UT_ASSERT_EQ(f.directories[1]->state, CLUSTER_RELMAP_RETENTION_BUILDING);
		} else
			census_refused(&f);
		episode_cleanup(&f);
	}
}

UT_TEST(test_episode_census_every_saved_generation_and_whole_set_are_fixed)
{
	for (unsigned fault = 0; fault < 4; fault++) {
		CensusFixture f;
		episode_setup(&f, false);
		UT_ASSERT_EQ(episode_begin(&f, 2, 1024 * 1024), CLUSTER_RELMAP_CENSUS_MORE);
		if (f.census != NULL) {
			if (fault == 0) episode_sources[0].selected.native_window[159]++;
			if (fault == 1) episode_sources[2].ir_request_id++;
			if (fault == 2) episode_count++;
			if (fault == 3) episode_live = false;
			UT_ASSERT_EQ(cluster_relmap_retention_census_next(f.census), CLUSTER_RELMAP_CENSUS_CHANGED);
			UT_ASSERT_EQ(episode_reads, 0);
			UT_ASSERT(episode_failures > 0);
			census_refused(&f);
		}
		episode_cleanup(&f);
	}
}

UT_TEST(test_episode_census_post_read_failure_preserves_original_pins)
{
	for (unsigned fault = 0; fault < 2; fault++) {
		CensusFixture f;
		ClusterRelmapRetentionPin saved[CLUSTER_RELMAP_RETENTION_CAPACITY];
		volatile bool caught = false;
		episode_setup(&f, true);
		UT_ASSERT_EQ(episode_begin(&f, 2, 1024 * 1024), CLUSTER_RELMAP_CENSUS_MORE);
		if (f.census != NULL && census_until_pin(&f)) {
			memcpy(saved, f.origin[0].pins, sizeof(saved));
			episode_read_fault = fault == 0;
			episode_read_error = fault == 1;
			PG_TRY();
			{
				UT_ASSERT_EQ(census_drain(&f), CLUSTER_RELMAP_CENSUS_CHANGED);
			}
			PG_CATCH(); { caught = true; }
			PG_END_TRY();
			UT_ASSERT_EQ(caught, fault == 1);
			UT_ASSERT(episode_failures > 0);
			UT_ASSERT_EQ(memcmp(saved, f.origin[0].pins, sizeof(saved)), 0);
			UT_ASSERT_EQ(cluster_relmap_retention_census_next(f.census), CLUSTER_RELMAP_CENSUS_CHANGED);
			census_refused(&f);
		}
		episode_cleanup(&f);
	}
}

UT_TEST(test_episode_census_first_owner_error_poison_is_not_retry)
{
	CensusFixture f;
	volatile bool caught = false;
	episode_setup(&f, true);
	episode_error = true;
	PG_TRY(); { (void)episode_begin(&f, 2, 1024 * 1024); }
	PG_CATCH(); { caught = true; }
	PG_END_TRY();
	UT_ASSERT(caught);
	UT_ASSERT(f.census == NULL);
	UT_ASSERT(episode_failures > 0);
	UT_ASSERT_EQ(episode_reads, 0);
	census_refused(&f);
	episode_cleanup(&f);
}

UT_TEST(test_episode_census_drained_reuse_and_interrupted_destroy_keep_debt)
{
	for (unsigned fault = 0; fault < 2; fault++) {
		CensusFixture f;
		episode_setup(&f, true);
		UT_ASSERT_EQ(episode_begin(&f, 2, 1024 * 1024), CLUSTER_RELMAP_CENSUS_MORE);
		if (f.census != NULL) {
			if (fault == 0) {
				UT_ASSERT_EQ(census_drain(&f), CLUSTER_RELMAP_CENSUS_DRAINED);
				episode_sources[0].selected.native_window[159]++;
				UT_ASSERT_EQ(cluster_relmap_retention_census_next(f.census), CLUSTER_RELMAP_CENSUS_CHANGED);
			} else {
				cluster_relmap_retention_census_destroy(f.census);
				f.census = NULL;
			}
			UT_ASSERT(episode_failures > 0);
			census_refused(&f);
		}
		episode_cleanup(&f);
	}
}

static void
census_episode_run(void)
{
	UT_RUN(test_episode_census_all_origins_history_and_real_pairs);
	UT_RUN(test_episode_census_complete_origin_storage_and_budget_preflight);
	UT_RUN(test_episode_census_every_saved_generation_and_whole_set_are_fixed);
	UT_RUN(test_episode_census_post_read_failure_preserves_original_pins);
	UT_RUN(test_episode_census_first_owner_error_poison_is_not_retry);
	UT_RUN(test_episode_census_drained_reuse_and_interrupted_destroy_keep_debt);
}
