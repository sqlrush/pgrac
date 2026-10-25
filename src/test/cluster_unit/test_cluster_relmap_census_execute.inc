/* Actual ROOT/pairs/census and local custody; original source/native leaves
 * are controlled, without native/all-origin completion qualification.
 * Author: SqlRush <sqlrush@gmail.com> */
static unsigned census_execute_post_fault, census_execute_opens;

bool
cluster_relmap_recovery_root_observe_cold_v1(const ClusterRelmapRecoverySourceObservationV1 *source,
											 size_t root_budget, size_t payload_budget,
											 ClusterRelmapRecoveryRootObservationV1 *out)
{
	ClusterRecoveryRootSet *volatile roots = NULL;
	volatile bool accepted = false;

	if (!census_execute_active)
		abort();
	UT_ASSERT_EQ(memcmp(source, &census_cold_selected, sizeof(*source)), 0);
	if (root_budget < 1024 || payload_budget == 0)
		return false;
	census_execute_fd = dup(census_native_fixture->fd);
	UT_ASSERT(census_execute_fd >= 0);
	census_execute_opens++;
	PG_TRY();
	{
		roots = begin_roots(census_execute_fd, root_budget);
		if (finish_roots((ClusterRecoveryRootSet *)roots, NULL)
			== CLUSTER_RECOVERY_ROOTS_OBSERVED) {
			out->roots = (ClusterRecoveryRootSet *)roots;
			out->root_fd = census_execute_fd;
			accepted = true;
		}
	}
	PG_FINALLY();
	{
		if (!accepted) {
			cluster_recovery_root_set_destroy((ClusterRecoveryRootSet *)roots);
			(void)CloseTransientFile(census_execute_fd);
		}
	}
	PG_END_TRY();
	return accepted;
}

static void
census_execute_after_native(void)
{
	if (census_execute_post_fault != 1) {
		census_native_identity = census_native_handles[census_arbitrations - 1].identity;
		census_cold_clear_pair_bytes();
	}
	if (census_arbitrations != 2)
		return;
	if (census_execute_post_fault == 2)
		census_restart_fault = 1;
	if (census_execute_post_fault == 3)
		census_restart_fault = 2;
	if (census_execute_post_fault == 4)
		UT_ASSERT_EQ(unlinkat(census_native_fixture->fd, "unrelated", 0), 0);
	if (census_execute_post_fault == 5)
		census_namespace_x = false;
	if (census_execute_post_fault == 6)
		census_cold_selected.scope_serial++;
}

static void
census_execute_setup(CensusFixture *f, bool pending)
{
	census_execute_active = census_execute_recovery = true;
	census_execute_fd = -1;
	census_execute_opens = census_execute_closes = census_execute_post_fault = 0;
	MyBackendType = B_STARTUP;
	CritSectionCount = 0;
	census_initial_setup(f, pending);
	UT_ASSERT_EQ(cluster_relmap_retention_census_begin(f->roots, 817, 91, root_uuid, f->directories,
													   2, CENSUS_BUDGET, &f->census),
				 CLUSTER_RELMAP_CENSUS_MORE);
	UT_ASSERT_EQ(census_drain(f), CLUSTER_RELMAP_CENSUS_DRAINED);
	/* These external original obligations must survive all local cleanup. */
	cluster_relmap_retention_invalidate(&f->origin[0]);
	cluster_relmap_retention_invalidate(&f->origin[1]);
	cluster_recovery_root_set_invalidate(f->roots);
	census_restart_fault = 0;
	census_batch_consuming = true;
	census_native_after_arbitration = census_execute_after_native;
}

static bool
census_execute_run(ClusterRelmapRetentionCensus **out)
{
	return cluster_relmap_retention_census_execute_cold(3, 2 * 1024 * 1024, CENSUS_BUDGET,
														1024 * 1024, out);
}

static void
census_execute_done(CensusFixture *f, ClusterRelmapRetentionCensus *cursor,
					const ClusterRelmapRetentionDirectory saved[2])
{
	cluster_relmap_retention_census_destroy(cursor);
	UT_ASSERT_EQ(census_execute_fd, -1);
	UT_ASSERT_EQ(census_execute_closes, census_execute_opens);
	UT_ASSERT_EQ(memcmp(saved, f->origin, sizeof(f->origin)), 0);
	UT_ASSERT(fcntl(f->fd, F_GETFD) >= 0);
	census_execution_no_local_handles();
	CritSectionCount = 0;
	MyBackendType = B_BG_WORKER;
	census_cold_consume_cleanup(f);
	census_execute_active = false;
}

UT_TEST(test_census_execute_clean_input_needs_no_native_attempt)
{
	CensusFixture f;
	ClusterRelmapRetentionCensus *out = NULL;
	ClusterRelmapRetentionDirectory saved[2];
	census_execute_setup(&f, false);
	memcpy(saved, f.origin, sizeof(saved));
	UT_ASSERT(census_execute_run(&out));
	UT_ASSERT(out != NULL && census_execute_fd >= 0 && census_execute_closes == 0);
	UT_ASSERT_EQ(cluster_relmap_retention_census_next(out), CLUSTER_RELMAP_CENSUS_DRAINED);
	UT_ASSERT_EQ(census_native_prepares + census_arbitrations, 0);
	census_execute_done(&f, out, saved);
}

UT_TEST(test_census_execute_all_pending_owns_entire_local_chain)
{
	CensusFixture f;
	ClusterRelmapRetentionCensus *out = NULL;
	ClusterRelmapRetentionDirectory saved[2];
	census_execute_setup(&f, true);
	memcpy(saved, f.origin, sizeof(saved));
	UT_ASSERT(census_execute_run(&out));
	UT_ASSERT(out != NULL && census_execute_fd >= 0 && census_execute_closes == 0);
	UT_ASSERT_EQ(cluster_relmap_retention_census_next(out), CLUSTER_RELMAP_CENSUS_DRAINED);
	UT_ASSERT_EQ(census_native_prepares, 2);
	UT_ASSERT_EQ(census_arbitrations, 2);
	UT_ASSERT_EQ(census_native_releases, 2);
	census_execute_done(&f, out, saved);
}

UT_TEST(test_census_execute_entry_gates_and_budgets_preserve_output)
{
	for (unsigned fault = 0; fault < 12; fault++) {
		CensusFixture f;
		ClusterRelmapRetentionCensus *out = NULL;
		ClusterRelmapRetentionDirectory saved[2];
		census_execute_setup(&f, false);
		memcpy(saved, f.origin, sizeof(saved));
		if (fault == 0)
			MyBackendType = B_BG_WORKER;
		if (fault == 1)
			census_execute_recovery = false;
		if (fault == 2)
			CritSectionCount = 1;
		if (fault == 3)
			cluster_shared_catalog = false;
		if (fault == 4)
			census_merger_held = false;
		if (fault == 5)
			census_namespace_x = false;
		if (fault == 6)
			out = (void *)1;
		UT_ASSERT(!cluster_relmap_retention_census_execute_cold(
			fault == 7 ? 128 : 3, fault == 8 ? 1 : 2 * 1024 * 1024, fault == 9 ? 1 : CENSUS_BUDGET,
			fault == 10 ? 0 : 1024 * 1024, fault == 11 ? NULL : &out));
		UT_ASSERT(out == (fault == 6 ? (void *)1 : NULL));
		UT_ASSERT_EQ(census_native_prepares + census_arbitrations, 0);
		if (fault == 6)
			out = NULL;
		census_execute_done(&f, out, saved);
	}
}

UT_TEST(test_census_execute_checkpoint_and_physical_failure_clean_local_custody)
{
	for (unsigned fault = 0; fault < 4; fault++) {
		CensusFixture f;
		ClusterRelmapRetentionCensus *volatile out = NULL;
		ClusterRelmapRetentionDirectory saved[2];
		volatile bool caught = false;
		census_execute_setup(&f, true);
		memcpy(saved, f.origin, sizeof(saved));
		if (fault < 2)
			census_initial_fault = fault + 1;
		if (fault >= 2)
			inject_pread_fault = fault - 1;
		PG_TRY();
		{
			UT_ASSERT(!census_execute_run((ClusterRelmapRetentionCensus **)&out));
		}
		PG_CATCH();
		{
			caught = true;
		}
		PG_END_TRY();
		UT_ASSERT_EQ(caught, fault == 1);
		UT_ASSERT(out == NULL && census_arbitrations == 0);
		inject_pread_fault = 0;
		census_execute_done(&f, (ClusterRelmapRetentionCensus *)out, saved);
	}
}

UT_TEST(test_census_execute_native_prepare_failure_releases_all_local_handles)
{
	for (unsigned attempt = 1; attempt <= 2; attempt++)
		for (unsigned fault = 1; fault <= 2; fault++) {
			CensusFixture f;
			ClusterRelmapRetentionCensus *volatile out = NULL;
			ClusterRelmapRetentionDirectory saved[2];
			volatile bool caught = false;
			census_execute_setup(&f, true);
			memcpy(saved, f.origin, sizeof(saved));
			census_native_fault = fault;
			census_native_fault_after = attempt;
			PG_TRY();
			{
				UT_ASSERT(!census_execute_run((ClusterRelmapRetentionCensus **)&out));
			}
			PG_CATCH();
			{
				caught = true;
			}
			PG_END_TRY();
			UT_ASSERT_EQ(caught, fault == 2);
			UT_ASSERT(out == NULL && census_arbitrations == 0);
			UT_ASSERT_EQ(census_native_prepares, attempt);
			UT_ASSERT_EQ(census_native_releases, attempt - 1);
			census_execute_done(&f, (ClusterRelmapRetentionCensus *)out, saved);
		}
}

UT_TEST(test_census_execute_unknown_or_error_stops_without_retry)
{
	for (unsigned attempt = 1; attempt <= 2; attempt++)
		for (unsigned fault = 1; fault <= 2; fault++) {
			CensusFixture f;
			ClusterRelmapRetentionCensus *volatile out = NULL;
			ClusterRelmapRetentionDirectory saved[2];
			volatile bool caught = false;
			census_execute_setup(&f, true);
			memcpy(saved, f.origin, sizeof(saved));
			census_arbitration_mode = fault;
			census_arbitration_after = attempt;
			PG_TRY();
			{
				UT_ASSERT(!census_execute_run((ClusterRelmapRetentionCensus **)&out));
			}
			PG_CATCH();
			{
				caught = true;
			}
			PG_END_TRY();
			UT_ASSERT_EQ(caught, fault == 2);
			UT_ASSERT(out == NULL && census_arbitrations == attempt);
			UT_ASSERT_EQ(census_native_prepares, 2);
			UT_ASSERT_EQ(census_native_releases, 2);
			census_execute_done(&f, (ClusterRelmapRetentionCensus *)out, saved);
		}
}

UT_TEST(test_census_execute_post_clear_failure_and_remaining_pending_refuse)
{
	for (unsigned fault = 1; fault <= 6; fault++) {
		CensusFixture f;
		ClusterRelmapRetentionCensus *volatile out = NULL;
		ClusterRelmapRetentionDirectory saved[2];
		volatile bool caught = false;
		census_execute_setup(&f, true);
		memcpy(saved, f.origin, sizeof(saved));
		census_execute_post_fault = fault;
		PG_TRY();
		{
			UT_ASSERT(!census_execute_run((ClusterRelmapRetentionCensus **)&out));
		}
		PG_CATCH();
		{
			caught = true;
		}
		PG_END_TRY();
		UT_ASSERT_EQ(caught, fault == 3);
		UT_ASSERT(out == NULL && census_arbitrations == 2 && census_native_releases == 2);
		census_execute_done(&f, (ClusterRelmapRetentionCensus *)out, saved);
	}
}

UT_TEST(test_census_execute_returned_chain_keeps_original_context_and_source)
{
	for (unsigned fault = 0; fault < 4; fault++) {
		CensusFixture f;
		ClusterRelmapRetentionCensus *out = NULL;
		ClusterRelmapRetentionDirectory saved[2];
		census_execute_setup(&f, true);
		memcpy(saved, f.origin, sizeof(saved));
		UT_ASSERT(census_execute_run(&out));
		if (fault == 0)
			census_cold_selected.scope_serial++;
		if (fault == 1)
			MyBackendType = B_BG_WORKER;
		if (fault == 2)
			CritSectionCount = 1;
		if (fault == 3)
			census_execute_recovery = false;
		UT_ASSERT_EQ(cluster_relmap_retention_census_next(out), CLUSTER_RELMAP_CENSUS_CHANGED);
		UT_ASSERT_EQ(census_execute_closes, 0);
		census_execute_done(&f, out, saved);
	}
}
