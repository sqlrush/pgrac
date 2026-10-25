/* Actual original checkpoint/WAL -> local physical ROOT/fd -> cold census.
 * Original execution ownership and storage opener remain controlled leaves.
 * Author: SqlRush <sqlrush@gmail.com> */
static const char *const census_open_paths[]
	= { "global/pgrac_relmap_authority",	 "global/pgrac_relmap_authority.bak",
		"base/70001/pgrac_relmap_authority", "base/70001/pgrac_relmap_authority.bak",
		"base/42/pgrac_relmap_authority",	 "base/42/pgrac_relmap_authority.bak" };

static void
census_open_setup(void)
{
	ClusterRelmapRecoverySourceObservationV1 source = execution_observe_setup();
	ClusterRelmapV2Image image = { 0 };
	int fd = open(test_root, O_RDONLY | O_DIRECTORY);

	UT_ASSERT(fd >= 0);
	UT_ASSERT_EQ(mkdirat(fd, "base/42", 0700), 0);
	image.identity.system_identifier = source.source.claim.identity.system_identifier;
	image.identity.database_incarnation = source.source.claim.database_incarnation;
	image.committed_generation = 1;
	image.committed.magic = RELMAPPER_FILEMAGIC;
	image.committed.num_mappings = 1;
	image.committed.mappings[0].mapoid = 1247;
	image.committed.mappings[0].mapfilenumber = 81234;
	relmap_test_crc(&image.committed);
	for (unsigned i = 0; i < lengthof(census_open_paths); i++) {
		image.identity.shared = i < 2;
		image.identity.dbid = i < 2 ? InvalidOid : i < 4 ? 70001 : 42;
		relmap_census_file(fd, census_open_paths[i], &image);
	}
	UT_ASSERT_EQ(close(fd), 0);
}

static void
census_open_done(ClusterRelmapRetentionCensus *census)
{
	ClusterRelmapRecoveryRootObservationV1 empty = { 0 };
	int fd;

	cluster_relmap_retention_census_destroy(census);
	if (execution_root_fd >= 0)
		UT_ASSERT(fcntl(execution_root_fd, F_GETFD) < 0 && errno == EBADF);
	fd = open(test_root, O_RDONLY | O_DIRECTORY);
	UT_ASSERT(fd >= 0);
	for (unsigned i = 0; i < lengthof(census_open_paths); i++)
		UT_ASSERT(unlinkat(fd, census_open_paths[i], 0) == 0 || errno == ENOENT);
	UT_ASSERT_EQ(unlinkat(fd, "base/42", AT_REMOVEDIR), 0);
	UT_ASSERT_EQ(close(fd), 0);
	execution_observe_done(&empty);
}

static bool
census_open(ClusterRelmapRetentionCensus **out)
{
	return cluster_relmap_retention_census_open_cold(0, 2 * 1024 * 1024, 128 * 1024, 1024 * 1024,
													 out);
}

static ClusterRelmapCensusResult
census_open_drain(ClusterRelmapRetentionCensus *census, unsigned *images)
{
	ClusterRelmapCensusResult result = CLUSTER_RELMAP_CENSUS_MORE;
	*images = 0;
	for (unsigned i = 0; i < 100000 && result == CLUSTER_RELMAP_CENSUS_MORE; i++) {
		ClusterRelmapCensusCommitted committed;
		result = cluster_relmap_retention_census_next_committed(census, &committed);
		if (committed.present) {
			(*images)++;
			UT_ASSERT_EQ(committed.generation, 1);
			UT_ASSERT_EQ(committed.map.num_mappings, 1);
			UT_ASSERT_EQ(committed.map.mappings[0].mapoid, 1247);
			UT_ASSERT_EQ(committed.map.mappings[0].mapfilenumber, 81234);
		}
	}
	return result;
}

/* Real original checkpoint/WAL, target union, held fd and physical pairs;
 * the finished consumer obtains no caller-created source or map. */
UT_TEST(test_cold_committed_original_wal_factory_and_all_physical_namespaces)
{
	ClusterRelmapRetentionCensus *census = NULL;
	unsigned images;
	const Oid dbids[] = { InvalidOid, 70001, 42, InvalidOid };
	census_open_setup();
	UT_ASSERT(census_open(&census));
	UT_ASSERT_EQ(census_open_drain(census, &images), CLUSTER_RELMAP_CENSUS_DRAINED);
	for (unsigned i = 0; i < lengthof(dbids); i++) {
		ClusterRelmapCensusCommitted out;
		memset(&out, 0xa5, sizeof(out));
		UT_ASSERT(cluster_relmap_retention_census_committed_cold(census, dbids[i], &out));
		UT_ASSERT(out.present && out.identity.dbid == dbids[i]
				  && out.identity.shared == (dbids[i] == InvalidOid));
		UT_ASSERT_EQ(out.identity.system_identifier, TEST_SYSID);
		UT_ASSERT_EQ(out.identity.database_incarnation,
					 prefix_base_expected.source.claim.database_incarnation);
		UT_ASSERT_EQ(out.generation, 1);
		UT_ASSERT_EQ(out.map.num_mappings, 1);
		UT_ASSERT_EQ(out.map.mappings[0].mapoid, 1247);
		UT_ASSERT_EQ(out.map.mappings[0].mapfilenumber, 81234);
		UT_ASSERT(fcntl(execution_root_fd, F_GETFD) >= 0);
	}
	census_open_done(census);
}

UT_TEST(test_cold_committed_original_wal_source_and_other_namespace_drift_refuse)
{
	for (unsigned fault = 0; fault < 3; fault++) {
		ClusterRelmapRetentionCensus *census = NULL;
		ClusterRelmapCensusCommitted out, zero = { 0 };
		unsigned images;
		volatile bool accepted = false, threw = false;
		census_open_setup();
		UT_ASSERT(census_open(&census));
		UT_ASSERT_EQ(census_open_drain(census, &images), CLUSTER_RELMAP_CENSUS_DRAINED);
		if (fault == 0)
			custody_scope++;
		if (fault == 1) {
			int fd = open(test_root, O_RDONLY | O_DIRECTORY);
			UT_ASSERT(fd >= 0);
			UT_ASSERT_EQ(unlinkat(fd, census_open_paths[5], 0), 0);
			UT_ASSERT_EQ(close(fd), 0);
		}
		if (fault == 2)
			custody_scope_error_after = custody_scope_reads + 1;
		memset(&out, 0xa5, sizeof(out));
		test_capture_error_level = true;
		PG_TRY();
		{
			accepted = cluster_relmap_retention_census_committed_cold(census, InvalidOid, &out);
		}
		PG_CATCH();
		{
			threw = true;
		}
		PG_END_TRY();
		test_capture_error_level = false;
		custody_scope_error_after = 0; /* Also disarm when the parent has no reader. */
		UT_ASSERT(!accepted && memcmp(&out, &zero, sizeof(out)) == 0);
		UT_ASSERT_EQ(threw, fault == 2);
		UT_ASSERT_EQ(cluster_relmap_retention_census_next(census),
					 CLUSTER_RELMAP_CENSUS_UNAVAILABLE);
		UT_ASSERT(fcntl(execution_root_fd, F_GETFD) >= 0);
		census_open_done(census);
	}
}

/* Missing production composition or lost fd custody breaks real map/close. */
UT_TEST(test_census_open_original_wal_physical_pairs_and_owned_fd)
{
	ClusterRelmapRetentionCensus *census = NULL;
	unsigned images;
	census_open_setup();
	UT_ASSERT(census_open(&census));
	UT_ASSERT(census != NULL && fcntl(execution_root_fd, F_GETFD) >= 0);
	UT_ASSERT_EQ(observe_reads, 3);	   /* Two observations plus final verification. */
	UT_ASSERT_EQ(execution_visits, 2); /* Original bounds and complete targets. */
	UT_ASSERT_EQ(census_open_drain(census, &images), CLUSTER_RELMAP_CENSUS_DRAINED);
	UT_ASSERT_EQ(images, 3);
	UT_ASSERT(fcntl(execution_root_fd, F_GETFD) >= 0);
	census_open_done(census);
}

/* Wrong early gates/nonfresh output must not enter original storage/WAL. */
UT_TEST(test_census_open_initial_gates_output_and_budgets)
{
	for (unsigned fault = 0; fault < 14; fault++) {
		ClusterRelmapRetentionCensus *census = NULL;
		bool accepted;
		census_open_setup();
		if (fault == 1)
			relmap_root_merger = false;
		if (fault == 2)
			relmap_root_namespace_x = false;
		if (fault == 3)
			cluster_shared_catalog = false;
		if (fault == 4)
			MyBackendType = B_BG_WORKER;
		if (fault == 5)
			inputs_native_recovery = false;
		if (fault == 6)
			CritSectionCount = 1;
		if (fault == 7)
			custody_scope = 0;
		if (fault == 8)
			census = (ClusterRelmapRetentionCensus *)&execution_root;
		accepted = cluster_relmap_retention_census_open_cold(
			fault == 9 ? 128 : 0,
			fault == 10	  ? 0
			: fault == 11 ? 1
						  : 2 * 1024 * 1024,
			fault == 12 ? 1 : 128 * 1024, fault == 13 ? 1 : 1024 * 1024, &census);
		UT_ASSERT_EQ(accepted, fault == 0);
		if (fault == 8) {
			UT_ASSERT(census == (ClusterRelmapRetentionCensus *)&execution_root);
			census = NULL;
		} else if (fault != 0)
			UT_ASSERT(census == NULL);
		if (fault > 0 && fault < 11)
			UT_ASSERT_EQ(execution_root_opens, 0);
		UT_ASSERT(!census_open(NULL));
		census_open_done(census);
	}
}

/* Refused/mutated complete original checkpoint/WAL cannot seed a cursor. */
UT_TEST(test_census_open_checkpoint_refusal_and_error_cleanup)
{
	for (unsigned fault = 0; fault <= 9; fault++) {
		ClusterRelmapRetentionCensus *census = NULL;
		volatile bool accepted = false, threw = false;
		census_open_setup();
		execution_fault = fault;
		test_capture_error_level = true;
		PG_TRY();
		{
			accepted = census_open(&census);
		}
		PG_CATCH();
		{
			threw = true;
		}
		PG_END_TRY();
		test_capture_error_level = false;
		UT_ASSERT_EQ(accepted, fault == 0);
		UT_ASSERT_EQ(threw, fault == 3 || fault == 9);
		if (fault != 0)
			UT_ASSERT(census == NULL);
		census_open_done(census);
	}
}

/* Storage refusal/ERROR or changed source/owner cleans only its new fd. */
UT_TEST(test_census_open_storage_refusal_and_error_cleanup)
{
	for (unsigned fault = 0; fault <= 12; fault++) {
		ClusterRelmapRetentionCensus *census = NULL;
		volatile bool accepted = false, threw = false;
		census_open_setup();
		execution_root_fault = fault;
		test_capture_error_level = true;
		PG_TRY();
		{
			accepted = census_open(&census);
		}
		PG_CATCH();
		{
			threw = true;
		}
		PG_END_TRY();
		test_capture_error_level = false;
		UT_ASSERT_EQ(accepted, fault == 0);
		UT_ASSERT_EQ(threw, fault == 2 || fault == 5);
		if (fault != 0)
			UT_ASSERT(census == NULL);
		census_open_done(census);
	}
}

/* No partial publication/leaked fd after either observation or verification. */
UT_TEST(test_census_open_all_physical_passes_refuse_drift_and_errors)
{
	for (unsigned pass = 1; pass <= 3; pass++)
		for (unsigned fault = 0; fault <= 17; fault++) {
			ClusterRelmapRetentionCensus *census = NULL;
			volatile bool accepted = false, threw = false;
			if (fault == 10)
				continue; /* No caller-built source exists to mutate. */
			census_open_setup();
			observe_fault = fault;
			observe_on_read = pass;
			test_capture_error_level = true;
			PG_TRY();
			{
				accepted = census_open(&census);
			}
			PG_CATCH();
			{
				threw = true;
			}
			PG_END_TRY();
			test_capture_error_level = false;
			UT_ASSERT_EQ(accepted, fault == 0);
			UT_ASSERT_EQ(threw, fault == 3 || fault == 9 || fault == 14);
			UT_ASSERT(observe_reads >= pass);
			if (fault != 0)
				UT_ASSERT(census == NULL);
			census_open_done(census);
		}
}

/* A still-required WAL-only pair or changed original source blocks DRAINED. */
UT_TEST(test_census_open_later_missing_pair_source_or_error_keeps_fd_until_destroy)
{
	for (unsigned fault = 0; fault < 5; fault++) {
		ClusterRelmapRetentionCensus *census = NULL;
		volatile bool threw = false;
		ClusterRelmapCensusResult result = CLUSTER_RELMAP_CENSUS_MORE;
		unsigned images = 0;
		int fd;
		census_open_setup();
		fd = open(test_root, O_RDONLY | O_DIRECTORY);
		UT_ASSERT(fd >= 0);
		if (fault == 1) {
			UT_ASSERT_EQ(unlinkat(fd, census_open_paths[4], 0), 0);
			UT_ASSERT_EQ(unlinkat(fd, census_open_paths[5], 0), 0);
		}
		UT_ASSERT_EQ(close(fd), 0);
		UT_ASSERT(census_open(&census));
		UT_ASSERT(census != NULL);
		if (fault == 2)
			custody_scope++;
		if (fault == 3)
			relmap_root_merger = false;
		if (fault == 4)
			execution_pending_guard_fault = 4;
		test_capture_error_level = true;
		PG_TRY();
		{
			result = census_open_drain(census, &images);
		}
		PG_CATCH();
		{
			threw = true;
		}
		PG_END_TRY();
		test_capture_error_level = false;
		UT_ASSERT_EQ(threw, fault == 4);
		if (!threw)
			UT_ASSERT_EQ(result, fault == 0	  ? CLUSTER_RELMAP_CENSUS_DRAINED
								 : fault == 1 ? CLUSTER_RELMAP_CENSUS_UNAVAILABLE
											  : CLUSTER_RELMAP_CENSUS_CHANGED);
		UT_ASSERT(fcntl(execution_root_fd, F_GETFD) >= 0);
		census_open_done(census);
	}
}

/* Abandoning a provisional or failed cursor still destroys its local fd. */
UT_TEST(test_census_open_early_destroy_owns_only_new_custody)
{
	for (unsigned advanced = 0; advanced < 2; advanced++) {
		ClusterRelmapRetentionCensus *census = NULL;
		census_open_setup();
		UT_ASSERT(census_open(&census));
		if (advanced)
			UT_ASSERT_EQ(cluster_relmap_retention_census_next(census), CLUSTER_RELMAP_CENSUS_MORE);
		UT_ASSERT(fcntl(execution_root_fd, F_GETFD) >= 0);
		census_open_done(census);
	}
}
