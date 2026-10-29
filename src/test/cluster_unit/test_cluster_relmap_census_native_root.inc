/* Original scope namespace/WAL leaves are controlled; ROOT/files/pairs are real.
 * Author: SqlRush <sqlrush@gmail.com>
 * Spec: 2026-10-14-pre2-m5-e1-format-closure-v1.md */
#include "cluster/cluster_recovery_wal_targets.h"

static unsigned native_root_namespace_calls, native_root_visits, native_root_collects;
static unsigned native_root_fault;
static ClusterRecoveryRootSet *native_root_collected;
static ClusterRecoveryRootSet *native_root_parent;

#ifdef CENSUS_NATIVE_ROOT_PARENT_CONTROL
static ClusterRelmapCensusResult
cluster_relmap_retention_census_open_native_episode(
	const ClusterRecoveryRelmapSourceV1 *source, size_t root_budget,
	size_t memory_budget, size_t payload_budget, ClusterRelmapRetentionCensus **out)
{
	(void)root_budget; (void)payload_budget;
	return cluster_relmap_retention_census_begin_native_episode(source,
		native_root_parent, memory_budget, out);
}
#endif

ClusterControlRootResult
cluster_wal_inputs_ctrc_namespace_v1(ClusterWalInputsV1 *inputs, int *fd)
{
	UT_ASSERT(inputs == (void *)&episode_cookie);
	native_root_namespace_calls++;
	*fd = native_episode_fd;
	if (native_root_fault == 1) {
		*fd = -1;
		return CLUSTER_CONTROL_ROOT_STALE_TOKEN;
	}
	if (native_root_namespace_calls > 1 && native_root_fault == 8)
		*fd = native_episode_fd + 1000;
	return CLUSTER_CONTROL_ROOT_OK_PRIMARY;
}

bool
cluster_recovery_wal_targets_collect(ClusterRecoveryRootSet *roots, XLogReaderState *reader,
	const ClusterWalSourceRef *source, const RfContributorStreamCutV1 *cut,
	size_t budget, ClusterRecoveryWalTargetsDiag *diag)
{
	RfPageIdentityV1 page = target(90000 + source->claim.identity.origin_node_id,
		MAIN_FORKNUM, source->claim.identity.origin_node_id);
	(void)reader;
	UT_ASSERT(budget > 0 && cut->flags == RF_CONTRIBUTOR_CUT_COMPLETE);
	native_root_collects++;
	native_root_collected = roots;
	memset(diag, 0, sizeof(*diag));
	if (native_root_fault == 4) return false;
	if (native_root_fault == 5) siglongjmp(*PG_exception_stack, 1);
	if (native_root_fault == 6 && native_root_collects == 2)
		episode_sources[0].selected.native_window[159]++;
	/* The target decoder leaf is controlled. Indexing the actual missing
	 * physical path and preserving its extent uses the original ROOT core. */
	return cluster_recovery_root_set_add_target(roots, &page) == CLUSTER_RECOVERY_ROOTS_MORE;
}

ClusterControlRootResult
cluster_wal_inputs_cold_census_v1(ClusterWalInputsV1 *inputs,
	ClusterWalCensusVisitorV1 visitor, ClusterWalCensusSourceVisitorV1 source_visitor,
	void *arg, uint64 *records, RfPageProofDetailV1 *detail)
{
	UT_ASSERT(inputs == (void *)&episode_cookie);
	native_root_visits++;
	*records = 0;
	*detail = RF_PAGE_PROOF_DETAIL_OK;
	if (native_root_fault == 2) return CLUSTER_CONTROL_ROOT_STALE_TOKEN;
	for (unsigned i = 0; i < 2; i++) {
		/* Deliberately opposite to the selected-input carrier order. */
		ClusterWalSourceRef source = episode_sources[1 - i].selected.source;
		RfContributorStreamCutV1 cut = {0};
		ClusterWalTailObservation tail = {0};
		XLogReaderState reader = {0};
		cut.flags = RF_CONTRIBUTOR_CUT_COMPLETE;
		cut.failed_thread = source.claim.identity.origin_thread_id;
		cut.timeline_id = source.timeline;
		cut.origin_owner_incarnation = source.claim.identity.origin_owner_incarnation;
		cut.scan_begin_inclusive = 1000 + 100 * i;
		cut.scan_end_exclusive = cut.scan_begin_inclusive + 80;
		tail.complete_end = cut.scan_end_exclusive;
		tail.records = 1;
		if (native_root_fault == 7 && i == 1) cut.flags = 0;
		if ((*detail = source_visitor(&source, &cut, NULL, arg)) != RF_PAGE_PROOF_DETAIL_OK)
			return CLUSTER_CONTROL_ROOT_STALE_TOKEN;
		if ((*detail = visitor(&reader, &source, &cut, arg)) != RF_PAGE_PROOF_DETAIL_OK)
			return CLUSTER_CONTROL_ROOT_STALE_TOKEN;
		(*records)++;
		if (!(native_root_fault == 3 && i == 1)
			&& (*detail = source_visitor(&source, &cut, &tail, arg)) != RF_PAGE_PROOF_DETAIL_OK)
			return CLUSTER_CONTROL_ROOT_STALE_TOKEN;
	}
	return CLUSTER_CONTROL_ROOT_OK_PRIMARY;
}

static void
native_root_setup(CensusFixture *f, bool pending)
{
	native_episode_setup(f, pending);
	native_root_namespace_calls = native_root_visits = native_root_collects = native_root_fault = 0;
	native_root_collected = NULL;
	native_root_parent = f->roots;
	census_execute_closes = 0;
	for (unsigned i = 0; i < 2; i++) {
		episode_sources[i].selected.source.timeline = 1;
		episode_sources[i].selected.source.claim.identity.origin_owner_incarnation = 909;
		native_episode_inputs[i].source = episode_sources[i];
	}
}

static ClusterRelmapCensusResult
native_root_begin(CensusFixture *f, size_t root_budget, size_t memory_budget)
{
	return cluster_relmap_retention_census_open_native_episode(&episode_sources[1],
		root_budget, memory_budget, 128 * 1024, &f->census);
}

UT_TEST(test_native_root_census_owns_full_wal_union_and_borrows_original_fd)
{
	CensusFixture f;
	unsigned targets = 0;
	native_root_setup(&f, true);
	UT_ASSERT_EQ(native_root_begin(&f, 4 * 1024 * 1024, 1024 * 1024), CLUSTER_RELMAP_CENSUS_MORE);
	UT_ASSERT_EQ(native_root_visits, 1);
	UT_ASSERT_EQ(native_root_collects, 2);
	UT_ASSERT(native_root_namespace_calls >= 2);
	UT_ASSERT(native_root_collected != NULL && native_root_collected != f.roots);
	if (native_root_collected != NULL && f.census != NULL) {
		for (uint32 i = 0; i < cluster_recovery_root_set_count(native_root_collected); i++) {
			const ClusterRecoveryRootEntry *entry = cluster_recovery_root_set_entry(native_root_collected, i);
			if (strcmp(entry->file.path, "base/5/90003") == 0
				|| strcmp(entry->file.path, "base/5/90007") == 0) {
				UT_ASSERT_EQ(entry->origins, CLUSTER_RECOVERY_ROOT_FROM_WAL);
				UT_ASSERT_EQ(entry->required_blocks,
					strcmp(entry->file.path, "base/5/90003") == 0 ? 4 : 8);
				targets++;
			}
		}
		UT_ASSERT_EQ(targets, 2);
		UT_ASSERT_EQ(census_drain(&f), CLUSTER_RELMAP_CENSUS_DRAINED);
		cluster_relmap_retention_census_destroy(f.census);
		f.census = NULL;
	}
	UT_ASSERT_EQ(census_execute_closes, 0);
	UT_ASSERT(fcntl(f.fd, F_GETFD) >= 0);
	episode_cleanup(&f);
}

UT_TEST(test_native_root_census_rejects_whole_input_before_namespace_io)
{
	for (unsigned fault = 0; fault < 5; fault++) {
		CensusFixture f;
		native_root_setup(&f, false);
		if (fault == 0) episode_sources[0].selected.kind = CLUSTER_WAL_INPUT_INITIALIZER_TERMINAL;
		if (fault == 1) episode_sources[0].selected.source.claim.identity.origin_node_id = 3;
		if (fault == 2) episode_sources[0].selected.source.claim.identity.storage_uuid[0] ^= 1;
		if (fault == 3) episode_count = 3;
		UT_ASSERT(native_root_begin(&f, 4 * 1024 * 1024, fault == 4 ? 1 : 1024 * 1024)
			> CLUSTER_RELMAP_CENSUS_DRAINED);
		UT_ASSERT(f.census == NULL);
		UT_ASSERT_EQ(native_root_namespace_calls, 0);
		UT_ASSERT_EQ(native_root_visits, 0);
		UT_ASSERT_EQ(native_episode_calls, 0);
		UT_ASSERT(episode_failures > 0);
		episode_cleanup(&f);
	}
}

UT_TEST(test_native_root_census_whole_visit_failures_keep_original_fd_and_debt)
{
	for (unsigned fault = 1; fault <= 7; fault++) {
		CensusFixture f;
		volatile bool caught = false;
		native_root_setup(&f, false);
		native_root_fault = fault;
		PG_TRY(); { UT_ASSERT(native_root_begin(&f, 4 * 1024 * 1024, 1024 * 1024)
			> CLUSTER_RELMAP_CENSUS_DRAINED); }
		PG_CATCH(); { caught = true; }
		PG_END_TRY();
		UT_ASSERT_EQ(caught, fault == 5);
		UT_ASSERT(f.census == NULL);
		UT_ASSERT(episode_failures > 0);
		UT_ASSERT_EQ(native_episode_calls, 0);
		UT_ASSERT_EQ(census_execute_closes, 0);
		UT_ASSERT(fcntl(f.fd, F_GETFD) >= 0);
		episode_cleanup(&f);
	}
}

UT_TEST(test_native_root_census_checks_namespace_and_root_after_native_producer)
{
	for (unsigned fault = 0; fault < 3; fault++) {
		CensusFixture f;
		native_root_setup(&f, false);
		if (fault == 0) native_root_fault = 8;
		if (fault == 1) native_episode_fault = 6;
		if (fault == 2) native_episode_fault = 8;
		UT_ASSERT(native_root_begin(&f, 4 * 1024 * 1024, 1024 * 1024)
			> CLUSTER_RELMAP_CENSUS_DRAINED);
		UT_ASSERT(f.census == NULL);
		UT_ASSERT_EQ(native_root_visits, 1);
		UT_ASSERT_EQ(native_episode_calls, 2);
		UT_ASSERT(episode_failures > 0);
		UT_ASSERT_EQ(census_execute_closes, 0);
		UT_ASSERT(fcntl(f.fd, F_GETFD) >= 0);
		episode_cleanup(&f);
	}
}

UT_TEST(test_native_root_census_entry_and_error_destroy_preserve_original_owner)
{
	for (unsigned fault = 0; fault < 3; fault++) {
		CensusFixture f;
		volatile bool caught = false;
		native_root_setup(&f, false);
		if (fault == 0) {
			ClusterRecoveryRelmapSourceV1 saved = episode_sources[1];
			UT_ASSERT(cluster_relmap_retention_census_open_native_episode(&episode_sources[1],
				4 * 1024 * 1024, 1024 * 1024, 128 * 1024,
				(ClusterRelmapRetentionCensus **)&episode_sources[1]) > CLUSTER_RELMAP_CENSUS_DRAINED);
			UT_ASSERT_EQ(memcmp(&saved, &episode_sources[1], sizeof(saved)), 0);
			CritSectionCount = 1;
			UT_ASSERT(native_root_begin(&f, 4 * 1024 * 1024, 1024 * 1024) > CLUSTER_RELMAP_CENSUS_DRAINED);
			CritSectionCount = 0;
			UT_ASSERT_EQ(episode_failures, 0);
			UT_ASSERT_EQ(native_root_namespace_calls, 0);
		} else if (fault == 1) {
			episode_error = true;
			PG_TRY(); { (void)native_root_begin(&f, 4 * 1024 * 1024, 1024 * 1024); }
			PG_CATCH(); { caught = true; }
			PG_END_TRY();
			UT_ASSERT(caught && episode_failures > 0);
		} else {
			UT_ASSERT_EQ(native_root_begin(&f, 4 * 1024 * 1024, 1024 * 1024), CLUSTER_RELMAP_CENSUS_MORE);
			cluster_relmap_retention_census_destroy(f.census);
			f.census = NULL;
			UT_ASSERT(episode_failures > 0);
		}
		UT_ASSERT(f.census == NULL);
		UT_ASSERT_EQ(census_execute_closes, 0);
		UT_ASSERT(fcntl(f.fd, F_GETFD) >= 0);
		episode_cleanup(&f);
	}
}

static void
census_native_root_run(void)
{
	UT_RUN(test_native_root_census_owns_full_wal_union_and_borrows_original_fd);
	UT_RUN(test_native_root_census_rejects_whole_input_before_namespace_io);
	UT_RUN(test_native_root_census_whole_visit_failures_keep_original_fd_and_debt);
	UT_RUN(test_native_root_census_checks_namespace_and_root_after_native_producer);
	UT_RUN(test_native_root_census_entry_and_error_destroy_preserve_original_owner);
}
