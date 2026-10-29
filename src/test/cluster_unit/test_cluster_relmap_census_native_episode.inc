/* Original native checkpoint inputs -> owned BUILDING census directories.
 * Native source/authority leaves are controlled; ROOT readers and pairs are real.
 * Author: SqlRush <sqlrush@gmail.com>
 * Spec: 2026-10-14-pre2-m5-e1-format-closure-v1.md */
static ClusterRecoveryRelmapNativeInputV1 native_episode_inputs[3];
static unsigned native_episode_calls, native_episode_fault;
static int native_episode_fd;
static ClusterRelmapRetentionDirectory *const *native_episode_control_dirs;

#ifdef CENSUS_NATIVE_EPISODE_PARENT_CONTROL
static ClusterRelmapCensusResult
cluster_relmap_retention_census_begin_native_episode(
	const ClusterRecoveryRelmapSourceV1 *source, ClusterRecoveryRootSet *roots,
	size_t budget, ClusterRelmapRetentionCensus **out)
{
	return cluster_relmap_retention_census_begin_episode(source, roots,
		native_episode_control_dirs, 2, budget, out);
}
#endif

ClusterRecoveryRelmapNativeInputV1
cluster_wal_inputs_relmap_native_v1(ClusterWalInputsV1 *inputs, uint32 index)
{
	ClusterRecoveryRelmapNativeInputV1 value = {0};
	UT_ASSERT(inputs == (void *)&episode_cookie);
	native_episode_calls++;
	if (index < lengthof(native_episode_inputs))
		value = native_episode_inputs[index];
	if (index == 1) {
		if (native_episode_fault == 1) memset(&value, 0, sizeof(value));
		if (native_episode_fault == 2) value.source.selected.native_window[158] ^= 1;
		if (native_episode_fault == 3) value.post_flush_issued += UINT64CONST(0x80000000);
		if (native_episode_fault == 4) value.window.origin++;
		if (native_episode_fault == 5) siglongjmp(*PG_exception_stack, 1);
		if (native_episode_fault == 6) episode_sources[0].selected.native_window[158] ^= 1;
		if (native_episode_fault == 7) episode_count++;
		if (native_episode_fault == 8) root_file(native_episode_fd, "unrelated", "warm", 4);
	}
	return value;
}

static void
native_episode_setup(CensusFixture *f, bool pending)
{
	episode_setup(f, pending);
	/* One complete original checkpoint for EACH origin, in the opposite
	 * order from the original physical files and caller-built directories. */
	episode_sources[0] = episode_sources[2];
	episode_count = 2;
	for (uint32 i = 0; i < 2; i++) {
		ClusterRecoveryRelmapSourceV1 *s = &episode_sources[i];
		ClusterRecoveryRelmapNativeInputV1 *n = &native_episode_inputs[i];
		s->input_index = i; s->input_count = 2;
		s->selected.kind = CLUSTER_WAL_INPUT_CHECKPOINT;
		s->replay = s->selected;
		memset(n, 0, sizeof(*n));
		n->source = *s;
		n->window.system_identifier = 817;
		n->window.database_incarnation = 91;
		n->window.origin = s->selected.source.claim.identity.origin_node_id;
		n->window.epoch = 101; n->window.admitted_incarnation = 909;
		n->window.boot_incarnation = 202;
		n->window.native_floor = UINT64CONST(0x200000003);
		n->window.last_issued = CENSUS_FULL - 1;
		n->window.allocation_next = CENSUS_FULL + 32;
		n->post_flush_issued = CENSUS_FULL + 16;
		n->checkpoint_end = 1080;
	}
	native_episode_calls = native_episode_fault = 0;
	native_episode_fd = f->fd;
	native_episode_control_dirs = f->directories;
}

static ClusterRelmapCensusResult
native_episode_begin(CensusFixture *f, size_t budget)
{
	return cluster_relmap_retention_census_begin_native_episode(&episode_sources[1],
		f->roots, budget, &f->census);
}

UT_TEST(test_native_episode_uses_each_original_floor_and_post_flush_issued)
{
	for (unsigned fault = 0; fault < 3; fault++) {
		CensusFixture f;
		ClusterRelmapRetentionDirectory before[2];
		native_episode_setup(&f, true);
		memcpy(before, f.origin, sizeof(before));
		if (fault == 1) native_episode_inputs[1].post_flush_issued = CENSUS_FULL - 1;
		if (fault == 2) {
			native_episode_inputs[0].window.native_floor = CENSUS_FULL + 1;
			native_episode_inputs[0].window.last_issued = CENSUS_FULL + 2;
		}
		UT_ASSERT_EQ(native_episode_begin(&f, 1024 * 1024), CLUSTER_RELMAP_CENSUS_MORE);
		UT_ASSERT_EQ(native_episode_calls, 2);
		if (f.census != NULL)
			UT_ASSERT_EQ(census_drain(&f) == CLUSTER_RELMAP_CENSUS_DRAINED, fault == 0);
		/* The old caller-filled arrays were never borrowed or reset. */
		UT_ASSERT_EQ(memcmp(before, f.origin, sizeof(before)), 0);
		episode_cleanup(&f);
	}
}

UT_TEST(test_native_episode_whole_cohort_and_budget_precede_native_io)
{
	for (unsigned fault = 0; fault < 4; fault++) {
		CensusFixture f;
		uint8 digest[32];
		native_episode_setup(&f, false);
		if (fault == 0) { episode_count = 3; episode_sources[0].input_count = 3; episode_sources[1].input_count = 3; }
		if (fault == 1) episode_sources[0].selected.source.claim.identity.storage_uuid[0] ^= 1;
		if (fault == 2) episode_sources[0].selected.kind = CLUSTER_WAL_INPUT_INITIALIZER_TERMINAL;
		UT_ASSERT(native_episode_begin(&f, fault == 3 ? 1 : 1024 * 1024) > CLUSTER_RELMAP_CENSUS_DRAINED);
		UT_ASSERT(f.census == NULL);
		UT_ASSERT_EQ(native_episode_calls, 0);
		UT_ASSERT(episode_failures > 0);
		UT_ASSERT(!cluster_recovery_root_set_digest(f.roots, digest));
		episode_cleanup(&f);
	}
}

UT_TEST(test_native_episode_bad_native_inputs_and_error_never_publish_partial)
{
	for (unsigned fault = 1; fault <= 5; fault++) {
		CensusFixture f;
		volatile bool caught = false;
		native_episode_setup(&f, false);
		native_episode_fault = fault;
		PG_TRY(); {
			UT_ASSERT(native_episode_begin(&f, 1024 * 1024) > CLUSTER_RELMAP_CENSUS_DRAINED);
		}
		PG_CATCH(); { caught = true; }
		PG_END_TRY();
		UT_ASSERT_EQ(caught, fault == 5);
		UT_ASSERT(f.census == NULL);
		UT_ASSERT_EQ(native_episode_calls, 2);
		UT_ASSERT(episode_failures > 0);
		episode_cleanup(&f);
	}
}

UT_TEST(test_native_episode_rechecks_all_saved_sources_and_physical_root_after_io)
{
	for (unsigned fault = 6; fault <= 8; fault++) {
		CensusFixture f;
		uint8 digest[32];
		native_episode_setup(&f, false);
		native_episode_fault = fault;
		UT_ASSERT(native_episode_begin(&f, 1024 * 1024) > CLUSTER_RELMAP_CENSUS_DRAINED);
		UT_ASSERT(f.census == NULL);
		UT_ASSERT_EQ(native_episode_calls, 2);
		UT_ASSERT(episode_failures > 0);
		UT_ASSERT(!cluster_recovery_root_set_digest(f.roots, digest));
		episode_cleanup(&f);
	}
}

UT_TEST(test_native_episode_entry_aliases_and_first_owner_error_preserve_boundaries)
{
	CensusFixture f;
	ClusterRecoveryRelmapSourceV1 saved;
	volatile bool caught = false;
	native_episode_setup(&f, false);
	saved = episode_sources[1];
	UT_ASSERT(cluster_relmap_retention_census_begin_native_episode(&episode_sources[1], f.roots,
		1024 * 1024, (ClusterRelmapRetentionCensus **)&episode_sources[1]) > CLUSTER_RELMAP_CENSUS_DRAINED);
	UT_ASSERT_EQ(memcmp(&saved, &episode_sources[1], sizeof(saved)), 0);
	CritSectionCount = 1;
	UT_ASSERT(native_episode_begin(&f, 1024 * 1024) > CLUSTER_RELMAP_CENSUS_DRAINED);
	CritSectionCount = 0;
	UT_ASSERT_EQ(episode_failures, 0);
	UT_ASSERT_EQ(native_episode_calls, 0);
	episode_error = true;
	PG_TRY(); { (void)native_episode_begin(&f, 1024 * 1024); }
	PG_CATCH(); { caught = true; }
	PG_END_TRY();
	UT_ASSERT(caught);
	UT_ASSERT(f.census == NULL);
	UT_ASSERT(episode_failures > 0);
	UT_ASSERT_EQ(native_episode_calls, 0);
	episode_cleanup(&f);
}

UT_TEST(test_native_episode_owned_directories_keep_original_source_until_destroy)
{
	for (unsigned fault = 0; fault < 2; fault++) {
		CensusFixture f;
		ClusterRelmapRetentionDirectory before[2];
		native_episode_setup(&f, true);
		memcpy(before, f.origin, sizeof(before));
		UT_ASSERT_EQ(native_episode_begin(&f, 1024 * 1024), CLUSTER_RELMAP_CENSUS_MORE);
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
			UT_ASSERT_EQ(native_episode_calls, 2);
		}
		UT_ASSERT_EQ(memcmp(before, f.origin, sizeof(before)), 0);
		episode_cleanup(&f);
	}
}

static void
census_native_episode_run(void)
{
	UT_RUN(test_native_episode_uses_each_original_floor_and_post_flush_issued);
	UT_RUN(test_native_episode_whole_cohort_and_budget_precede_native_io);
	UT_RUN(test_native_episode_bad_native_inputs_and_error_never_publish_partial);
	UT_RUN(test_native_episode_rechecks_all_saved_sources_and_physical_root_after_io);
	UT_RUN(test_native_episode_entry_aliases_and_first_owner_error_preserve_boundaries);
	UT_RUN(test_native_episode_owned_directories_keep_original_source_until_destroy);
}
