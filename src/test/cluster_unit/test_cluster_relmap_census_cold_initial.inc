/* Actual held ROOT and physical pairs; original checkpoint/source leaves are
 * controlled. No native lifetime or complete-origin qualification is inferred.
 * Author: SqlRush <sqlrush@gmail.com> */
static unsigned census_initial_fault, census_initial_builds;
static ClusterRelmapRetentionDirectory *census_initial_directory;

bool
cluster_relmap_recovery_retention_build_start_cold_source_v1(
	uint16 origin, ClusterRelmapRetentionDirectory *directory,
	ClusterRelmapRecoverySourceObservationV1 *source)
{
	ClusterRelmapRetentionDirectory zero_directory = { 0 };
	ClusterRelmapRecoverySourceObservationV1 zero_source = { 0 };
	UT_ASSERT_EQ(origin, 3);
	UT_ASSERT_EQ(memcmp(directory, &zero_directory, sizeof(*directory)), 0);
	UT_ASSERT_EQ(memcmp(source, &zero_source, sizeof(*source)), 0);
	census_initial_builds++;
	census_initial_directory = directory;
	if (census_initial_fault == 1)
		return false;
	if (census_initial_fault == 2)
		siglongjmp(*PG_exception_stack, 1);
	if (cluster_relmap_retention_build_start(directory, 817, 91, origin, 73,
											 UINT64CONST(0x200000003), UINT64CONST(0x200000400))
		!= CLUSTER_RELMAP_RETENTION_OK)
		return false;
	*source = census_cold_selected;
	if (census_initial_fault == 3)
		directory->system_identifier++;
	if (census_initial_fault == 4)
		directory->database_incarnation++;
	if (census_initial_fault == 5)
		directory->origin++;
	if (census_initial_fault == 6)
		directory->generation++;
	if (census_initial_fault == 7)
		directory->state = CLUSTER_RELMAP_RETENTION_COMPLETE;
	if (census_initial_fault == 8)
		source->source.claim.identity.storage_uuid[0]++;
	if (census_initial_fault == 9)
		source->scope_serial++;
	if (census_initial_fault == 10)
		source->checkpoint.root_publish_seq++;
	if (census_initial_fault == 11)
		source->native_redo++;
	if (census_initial_fault == 12)
		census_merger_held = false;
	if (census_initial_fault == 13)
		root_file(census_native_fixture->fd, "unrelated", "warm", 4);
	return true;
}

static void
census_initial_setup(CensusFixture *f, bool pending)
{
	census_fixture(f, pending);
	if (pending)
		census_pair(f->fd, InvalidOid, 3, false, true, false);
	census_native_reset(f);
	census_cold_reset();
	census_initial_fault = census_initial_builds = 0;
	census_initial_directory = NULL;
	memset(&census_cold_selected, 0, sizeof(census_cold_selected));
	census_cold_selected.source = census_native_sources[0].source;
	census_cold_selected.source.claim.identity.origin_node_id = 3;
	census_cold_selected.checkpoint.identity = census_cold_selected.source.claim.identity;
	census_cold_selected.checkpoint.lifecycle = CLUSTER_CONTROL_ROOT_LIFECYCLE_RECOVERY_REQUIRED;
	census_cold_selected.checkpoint.root_publish_seq = 8;
	census_cold_selected.native_redo = 512;
	census_cold_selected.scope_serial = 73;
	f->roots = begin_roots(f->fd, 4 * 1024 * 1024);
	UT_ASSERT_EQ(finish_roots(f->roots, NULL), CLUSTER_RECOVERY_ROOTS_OBSERVED);
}

static bool
census_initial_begin(CensusFixture *f)
{
	bool ok = cluster_relmap_retention_census_begin_cold(f->roots, 3, CENSUS_BUDGET, &f->census);
	UT_ASSERT(ok && f->census != NULL);
	return ok;
}

static void
census_initial_cleanup(CensusFixture *f)
{
	inject_pread_fault = 0;
	census_initial_fault = 0;
	census_cold_cleanup(f);
}

UT_TEST(test_census_initial_owner_inputs_restore_all_pending_without_old_directory)
{
	for (unsigned pending = 0; pending < 2; pending++) {
		CensusFixture f;
		ClusterRelmapRetentionDirectory saved[2];
		uint8 digest[32];
		census_initial_setup(&f, pending);
		memcpy(saved, f.origin, sizeof(saved));
		if (census_initial_begin(&f)) {
			UT_ASSERT_EQ(census_drain(&f), CLUSTER_RELMAP_CENSUS_DRAINED);
			UT_ASSERT_EQ(census_initial_builds, 1);
			UT_ASSERT_EQ(census_initial_directory->state, CLUSTER_RELMAP_RETENTION_BUILDING);
			UT_ASSERT_EQ(census_initial_directory->generation, 73);
			UT_ASSERT_EQ(census_pin(census_initial_directory, InvalidOid) != NULL, pending != 0);
			UT_ASSERT_EQ(census_pin(census_initial_directory, 5) != NULL, pending != 0);
			UT_ASSERT_EQ(memcmp(saved, f.origin, sizeof(saved)), 0);
			UT_ASSERT_EQ(census_native_prepares + census_arbitrations, 0);
			cluster_relmap_retention_census_destroy(f.census);
			f.census = NULL;
			UT_ASSERT(cluster_recovery_root_set_digest(f.roots, digest));
			UT_ASSERT(fcntl(f.fd, F_GETFD) >= 0); /* borrowed ROOT/fd survived */
		}
		census_initial_cleanup(&f);
	}
}

UT_TEST(test_census_initial_entry_gates_budget_aliases_and_null_preserve_borrowed_inputs)
{
	for (unsigned fault = 0; fault < 6; fault++) {
		CensusFixture f;
		uint8 before[32], after[32];
		ClusterRelmapRetentionCensus *out = (void *)1;
		census_initial_setup(&f, false);
		UT_ASSERT(cluster_recovery_root_set_digest(f.roots, before));
		if (fault == 0)
			cluster_shared_catalog = false;
		if (fault == 1)
			census_merger_held = false;
		if (fault == 2)
			census_namespace_x = false;
		UT_ASSERT(!cluster_relmap_retention_census_begin_cold(f.roots, fault == 3 ? 128 : 3,
															  CENSUS_BUDGET,
															  fault == 4   ? NULL
															  : fault == 5 ? (void *)f.roots
																		   : &out));
		if (fault != 4 && fault != 5)
			UT_ASSERT(out == NULL);
		UT_ASSERT(cluster_recovery_root_set_digest(f.roots, after));
		UT_ASSERT_EQ(memcmp(before, after, sizeof(before)), 0);
		UT_ASSERT_EQ(census_initial_builds, 0);
		census_merger_held = census_namespace_x = cluster_shared_catalog = true;
		if (census_initial_begin(&f))
			UT_ASSERT_EQ(census_drain(&f), CLUSTER_RELMAP_CENSUS_DRAINED);
		census_initial_cleanup(&f);
	}
}

UT_TEST(test_census_initial_producer_refusal_metadata_and_error_revoke_only_observation)
{
	for (unsigned fault = 0; fault <= 13; fault++) {
		CensusFixture f;
		ClusterRelmapRetentionDirectory saved[2];
		volatile bool threw = false;
		uint8 digest[32];
		census_initial_setup(&f, true);
		memcpy(saved, f.origin, sizeof(saved));
		census_initial_fault = fault;
		PG_TRY();
		{
			UT_ASSERT_EQ(
				cluster_relmap_retention_census_begin_cold(f.roots, 3, CENSUS_BUDGET, &f.census),
				fault == 0);
		}
		PG_CATCH();
		{
			threw = true;
		}
		PG_END_TRY();
		UT_ASSERT_EQ(threw, fault == 2);
		UT_ASSERT_EQ(f.census != NULL, fault == 0);
		if (fault != 0)
			UT_ASSERT(!cluster_recovery_root_set_digest(f.roots, digest));
		UT_ASSERT_EQ(census_initial_builds, 1);
		UT_ASSERT_EQ(memcmp(saved, f.origin, sizeof(saved)), 0);
		UT_ASSERT(fcntl(f.fd, F_GETFD) >= 0);
		census_initial_cleanup(&f);
	}
}

static unsigned census_initial_late_fault;
static void
census_initial_late_change(void *buffer, size_t length, off_t offset)
{
	(void)buffer;
	(void)length;
	(void)offset;
	namespace_read_transform = NULL;
	if (census_initial_late_fault == 1)
		census_cold_selected.scope_serial++;
	if (census_initial_late_fault == 2)
		census_namespace_x = false;
	if (census_initial_late_fault == 3)
		census_cold_selected.checkpoint.root_publish_seq++;
	if (census_initial_late_fault == 4)
		census_cold_fault = 8;
	if (census_initial_late_fault == 5)
		siglongjmp(*PG_exception_stack, 1);
}

UT_TEST(test_census_initial_full_root_io_and_post_io_original_checks_are_mandatory)
{
	for (unsigned fault = 0; fault < 9; fault++) {
		CensusFixture f;
		volatile bool threw = false;
		uint8 digest[32];
		census_initial_setup(&f, true);
		census_initial_late_fault = fault;
		if (fault >= 1 && fault <= 5)
			namespace_read_transform = census_initial_late_change;
		if (fault == 6)
			inject_pread_fault = 1;
		if (fault == 7)
			inject_pread_fault = 2;
		if (fault == 8)
			InterruptPending = true;
		PG_TRY();
		{
			UT_ASSERT_EQ(
				cluster_relmap_retention_census_begin_cold(f.roots, 3, CENSUS_BUDGET, &f.census),
				fault == 0);
		}
		PG_CATCH();
		{
			threw = true;
		}
		PG_END_TRY();
		UT_ASSERT_EQ(threw, fault == 4 || fault == 5 || fault == 8);
		UT_ASSERT_EQ(f.census != NULL, fault == 0);
		UT_ASSERT_EQ(census_initial_builds, 1);
		if (fault != 0)
			UT_ASSERT(!cluster_recovery_root_set_digest(f.roots, digest));
		census_initial_cleanup(&f);
	}
}

UT_TEST(test_census_initial_original_scope_is_rechecked_before_drained_and_on_reuse)
{
	for (unsigned fault = 0; fault < 5; fault++) {
		CensusFixture f;
		volatile bool threw = false;
		uint8 digest[32];
		census_initial_setup(&f, true);
		if (census_initial_begin(&f)) {
			if (fault >= 3)
				UT_ASSERT_EQ(census_drain(&f), CLUSTER_RELMAP_CENSUS_DRAINED);
			if (fault == 0 || fault == 3)
				census_cold_selected.scope_serial++;
			if (fault == 1)
				census_merger_held = false;
			if (fault == 2 || fault == 4)
				census_cold_fault = 8;
			PG_TRY();
			{
				UT_ASSERT_EQ(census_drain(&f), CLUSTER_RELMAP_CENSUS_CHANGED);
			}
			PG_CATCH();
			{
				threw = true;
			}
			PG_END_TRY();
			UT_ASSERT_EQ(threw, fault == 2 || fault == 4);
			UT_ASSERT(!cluster_recovery_root_set_digest(f.roots, digest));
			if (fault >= 3) {
				UT_ASSERT(census_pin(census_initial_directory, 5) != NULL);
				UT_ASSERT(census_pin(census_initial_directory, InvalidOid) != NULL);
			}
			UT_ASSERT_EQ(census_initial_directory->state, CLUSTER_RELMAP_RETENTION_UNKNOWN);
		}
		census_initial_cleanup(&f);
	}
}

UT_TEST(test_census_initial_budget_and_interrupted_cleanup_never_release_original_owner)
{
	for (unsigned sample = 0; sample < 2; sample++) {
		CensusFixture f;
		ClusterRelmapRetentionDirectory saved[2];
		uint8 digest[32];
		census_initial_setup(&f, true);
		memcpy(saved, f.origin, sizeof(saved));
		UT_ASSERT(!cluster_relmap_retention_census_begin_cold(
			f.roots, 3, sample == 0 ? 0 : sizeof(ClusterRelmapRetentionDirectory), &f.census));
		UT_ASSERT(f.census == NULL && census_initial_builds == 0);
		UT_ASSERT(!cluster_recovery_root_set_digest(f.roots, digest));
		UT_ASSERT_EQ(memcmp(saved, f.origin, sizeof(saved)), 0);
		census_initial_cleanup(&f);
	}
	{
		CensusFixture f;
		uint8 digest[32];
		census_initial_setup(&f, true);
		if (census_initial_begin(&f)) {
			UT_ASSERT_EQ(cluster_relmap_retention_census_next(f.census),
						 CLUSTER_RELMAP_CENSUS_MORE);
			cluster_relmap_retention_census_destroy(f.census);
			f.census = NULL;
			UT_ASSERT(!cluster_recovery_root_set_digest(f.roots, digest));
			UT_ASSERT(fcntl(f.fd, F_GETFD) >= 0 && census_native_scope_current);
		}
		census_initial_cleanup(&f);
	}
}
