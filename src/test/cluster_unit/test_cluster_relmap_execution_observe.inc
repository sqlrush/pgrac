/* Actual checkpoint/WAL/targets/physical ROOT/census composition. Original
 * execution ownership and storage opener remain controlled fixture leaves.
 * Author: SqlRush <sqlrush@gmail.com> */
static unsigned observe_fault, observe_reads, observe_on_read;
static ino_t observe_inode;
static ClusterRelmapRecoverySourceObservationV1 *observe_source;

ssize_t test_relmap_observation_pread(int fd, void *buffer, size_t length, off_t offset);
ssize_t
test_relmap_observation_pread(int fd, void *buffer, size_t length, off_t offset)
{
	struct stat held;
	ssize_t n = pread(fd, buffer, length, offset);

	if (fstat(fd, &held) != 0 || held.st_ino != observe_inode)
		return n;
	observe_reads++;
	if (observe_reads != observe_on_read)
		return n;
	if (observe_fault == 1) {
		errno = EIO;
		return -1;
	}
	if (observe_fault == 2)
		return n > 0 ? n - 1 : n;
	if (observe_fault == 3)
		ereport(ERROR, (errmsg("injected physical ROOT read ERROR")));
	if (observe_fault == 4)
		relmap_root_merger = false;
	if (observe_fault == 5)
		relmap_root_namespace_x = false;
	if (observe_fault == 6)
		cluster_shared_catalog = false;
	if (observe_fault == 7)
		custody_scope++;
	if (observe_fault == 8)
		execution_pending_guard_fault = 1;
	if (observe_fault == 9)
		execution_pending_guard_fault = 4;
	if (observe_fault == 10)
		observe_source->scope_serial++;
	if (observe_fault == 11)
		CritSectionCount = 1;
	if (observe_fault == 12)
		MyBackendType = B_BG_WORKER;
	if (observe_fault == 13)
		inputs_native_recovery = false;
	if (observe_fault == 14)
		InterruptPending = true;
	if (observe_fault == 15)
		v2_checkpoint_root_race();
	if (observe_fault == 16) {
		int file = open(targets_file, O_WRONLY);
		if (file < 0 || pwrite(file, "y", 1, 0) != 1 || close(file) != 0)
			abort();
	}
	if (observe_fault == 17 && unlink(targets_file) != 0)
		abort();
	return n;
}

static ClusterRelmapRecoverySourceObservationV1
execution_observe_setup(void)
{
	ClusterRelmapRecoverySourceObservationV1 source;
	ClusterRecoveryRootSet *unused = execution_targets_setup(&source);
	struct stat file;

	cluster_recovery_root_set_destroy(unused);
	UT_ASSERT_EQ(CloseTransientFile(targets_fd), 0);
	targets_fd = -1;
	if (stat(targets_file, &file) != 0)
		abort();
	observe_inode = file.st_ino;
	observe_fault = observe_reads = 0;
	observe_on_read = 1;
	observe_source = NULL;
	return source;
}

static void
execution_observe_release(ClusterRelmapRecoveryRootObservationV1 *out)
{
	if (out->roots != NULL) {
		cluster_recovery_root_set_destroy(out->roots);
		UT_ASSERT_EQ(CloseTransientFile(out->root_fd), 0);
	}
	memset(out, 0, sizeof(*out));
}

static void
execution_observe_done(ClusterRelmapRecoveryRootObservationV1 *out)
{
	execution_observe_release(out);
	if (execution_root_fd >= 0)
		UT_ASSERT(fcntl(execution_root_fd, F_GETFD) < 0 && errno == EBADF);
	if (unlink(targets_file) != 0 && errno != ENOENT)
		abort();
	if (rmdir(targets_database) != 0 || rmdir(targets_base) != 0)
		abort();
	UT_ASSERT(!inputs_pin_held && inputs_pin_refuse && test_actual_cf == NoLock);
	UT_ASSERT_EQ(prefix_base_reads + inputs_peer_begins + inputs_native_begin_calls, 0);
	observe_fault = observe_inode = 0;
	CritSectionCount = 0;
	InterruptPending = false;
	execution_root_done();
}

static bool
execution_observe(ClusterRelmapRecoverySourceObservationV1 *source,
				  ClusterRelmapRecoveryRootObservationV1 *out)
{
	observe_source = source;
	return cluster_relmap_recovery_root_observe_cold_v1(source, 2 * 1024 * 1024, 1024 * 1024, out);
}

UT_TEST(test_execution_observe_complete_wal_and_physical_union)
{
	ClusterRelmapRecoverySourceObservationV1 source = execution_observe_setup(), before = source;
	ClusterRelmapRecoveryRootObservationV1 out = { 0 };
	uint8 digest[32];
	bool physical = false, missing = false;

	UT_ASSERT(execution_observe(&source, &out));
	if (out.roots != NULL) {
		UT_ASSERT(out.root_fd == execution_root_fd && fcntl(out.root_fd, F_GETFD) >= 0);
		UT_ASSERT(out.targets.records >= 3 && out.targets.native_effects > 0);
		UT_ASSERT(out.targets.unowned_records > 0 && out.targets.other_native_effects > 0);
		UT_ASSERT_EQ(execution_visits, 1);
		UT_ASSERT_EQ(observe_reads, 2); /* Both full physical passes read the real file. */
		UT_ASSERT(cluster_recovery_root_set_digest(out.roots, digest));
		for (uint32 i = 0; i < cluster_recovery_root_set_count(out.roots); i++) {
			const ClusterRecoveryRootEntry *entry = cluster_recovery_root_set_entry(out.roots, i);
			if (strcmp(entry->file.path, "base/70001/81234") == 0)
				physical
					= entry->file.size == 1 && (entry->origins & CLUSTER_RECOVERY_ROOT_PHYSICAL);
			if ((entry->origins & CLUSTER_RECOVERY_ROOT_FROM_WAL)
				&& !(entry->origins & CLUSTER_RECOVERY_ROOT_PHYSICAL) && entry->required_blocks > 0)
				missing = true;
		}
		UT_ASSERT(physical && missing);
		UT_ASSERT_EQ(cluster_recovery_root_set_recheck_begin(out.roots),
					 CLUSTER_RECOVERY_ROOTS_MORE);
		UT_ASSERT_EQ(execution_targets_finish(out.roots), CLUSTER_RECOVERY_ROOTS_OBSERVED);
	}
	UT_ASSERT_EQ(memcmp(&source, &before, sizeof(source)), 0);
	execution_observe_done(&out);
}

UT_TEST(test_execution_observe_missing_gates_and_invalid_source)
{
	for (unsigned fault = 0; fault < 13; fault++) {
		ClusterRelmapRecoverySourceObservationV1 source = execution_observe_setup();
		ClusterRelmapRecoveryRootObservationV1 out = { 0 }, zero = { 0 };
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
			source.scope_serial++;
		if (fault == 8)
			source.checkpoint.root_publish_seq++;
		if (fault == 9)
			source.native_redo++;
		if (fault == 10)
			source.source.claim.identity.storage_uuid[0]++;
		if (fault == 11)
			execution_pending_guard_fault = 1;
		UT_ASSERT_EQ(cluster_relmap_recovery_root_observe_cold_v1(
						 fault == 12 ? NULL : &source, 2 * 1024 * 1024, 1024 * 1024, &out),
					 fault == 0);
		UT_ASSERT_EQ(execution_root_opens, fault == 0 ? 1 : 0);
		if (fault != 0)
			UT_ASSERT_EQ(memcmp(&out, &zero, sizeof(out)), 0);
		execution_observe_done(&out);
	}
}

UT_TEST(test_execution_observe_fresh_output_aliases_and_budgets)
{
	for (unsigned fault = 0; fault < 8; fault++) {
		ClusterRelmapRecoverySourceObservationV1 source = execution_observe_setup(),
												 before = source;
		ClusterRelmapRecoveryRootObservationV1 out = { 0 }, saved;
		if (fault == 1)
			out.root_fd = 7;
		if (fault == 2)
			out.targets.unowned_records = 1;
		saved = out;
		UT_ASSERT_EQ(cluster_relmap_recovery_root_observe_cold_v1(&source,
																  fault == 5   ? 0
																  : fault == 7 ? 1
																			   : 2 * 1024 * 1024,
																  fault == 6 ? 0 : 1024 * 1024,
																  fault == 3   ? NULL
																  : fault == 4 ? (void *)&source
																			   : &out),
					 fault == 0);
		if (fault != 0)
			UT_ASSERT_EQ(memcmp(&out, &saved, sizeof(out)), 0);
		UT_ASSERT_EQ(memcmp(&source, &before, sizeof(source)), 0);
		UT_ASSERT_EQ(execution_root_opens, fault == 0 || fault == 7 ? 1 : 0);
		execution_observe_done(&out);
	}
}

UT_TEST(test_execution_observe_storage_refusal_and_error_cleanup)
{
	for (unsigned fault = 0; fault <= 12; fault++) {
		ClusterRelmapRecoverySourceObservationV1 source = execution_observe_setup();
		ClusterRelmapRecoveryRootObservationV1 out = { 0 }, zero = { 0 };
		volatile bool accepted = false, threw = false;
		execution_root_fault = fault;
		test_capture_error_level = true;
		PG_TRY();
		{
			accepted = execution_observe(&source, &out);
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
			UT_ASSERT_EQ(memcmp(&out, &zero, sizeof(out)), 0);
		execution_observe_done(&out);
	}
}

UT_TEST(test_execution_observe_wal_refusal_and_error_cleanup)
{
	for (unsigned fault = 0; fault < 11; fault++) {
		ClusterRelmapRecoverySourceObservationV1 source = execution_observe_setup();
		ClusterRelmapRecoveryRootObservationV1 out = { 0 }, zero = { 0 };
		volatile bool accepted = false, threw = false;
		execution_fault = fault < 10 ? fault : 0;
		test_capture_error_level = true;
		PG_TRY();
		{
			accepted = cluster_relmap_recovery_root_observe_cold_v1(
				&source, 2 * 1024 * 1024, fault == 10 ? 1 : 1024 * 1024, &out);
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
			UT_ASSERT_EQ(memcmp(&out, &zero, sizeof(out)), 0);
		execution_observe_done(&out);
	}
}

UT_TEST(test_execution_observe_physical_read_and_owner_drift_cleanup)
{
	for (unsigned pass = 1; pass <= 2; pass++)
		for (unsigned fault = 0; fault <= 15; fault++) {
			ClusterRelmapRecoverySourceObservationV1 source = execution_observe_setup();
			ClusterRelmapRecoveryRootObservationV1 out = { 0 }, zero = { 0 };
			volatile bool accepted = false, threw = false;
			observe_fault = fault;
			observe_on_read = pass;
			test_capture_error_level = true;
			PG_TRY();
			{
				accepted = execution_observe(&source, &out);
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
				UT_ASSERT_EQ(memcmp(&out, &zero, sizeof(out)), 0);
			execution_observe_done(&out);
		}
}

UT_TEST(test_execution_observe_physical_change_never_publishes)
{
	for (unsigned pass = 1; pass <= 2; pass++)
		for (unsigned fault = 0; fault < 3; fault++) {
			ClusterRelmapRecoverySourceObservationV1 source = execution_observe_setup();
			ClusterRelmapRecoveryRootObservationV1 out = { 0 }, zero = { 0 };
			observe_fault = fault == 0 ? 0 : fault + 15;
			observe_on_read = pass;
			UT_ASSERT_EQ(execution_observe(&source, &out), fault == 0);
			UT_ASSERT(observe_reads >= pass);
			if (fault != 0)
				UT_ASSERT_EQ(memcmp(&out, &zero, sizeof(out)), 0);
			execution_observe_done(&out);
		}
}

UT_TEST(test_execution_observe_feeds_original_cold_census)
{
	for (unsigned complete = 0; complete < 2; complete++) {
		ClusterRelmapRecoverySourceObservationV1 source = execution_observe_setup();
		ClusterRelmapRecoveryRootObservationV1 out = { 0 };
		ClusterRelmapV2Image image = { 0 };
		ClusterRelmapRetentionCensus *census = NULL;
		ClusterRelmapCensusResult result = CLUSTER_RELMAP_CENSUS_MORE;
		unsigned images = 0;
		int fd = open(test_root, O_RDONLY | O_DIRECTORY);
		const char *paths[]
			= { "global/pgrac_relmap_authority",	 "global/pgrac_relmap_authority.bak",
				"base/70001/pgrac_relmap_authority", "base/70001/pgrac_relmap_authority.bak",
				"base/42/pgrac_relmap_authority",	 "base/42/pgrac_relmap_authority.bak" };
		unsigned pairs = complete ? lengthof(paths) : 4;

		UT_ASSERT(fd >= 0);
		image.identity.system_identifier = source.source.claim.identity.system_identifier;
		image.identity.database_incarnation = source.source.claim.database_incarnation;
		image.committed_generation = 1;
		image.committed.magic = RELMAPPER_FILEMAGIC;
		image.committed.num_mappings = 1;
		image.committed.mappings[0].mapoid = 1247;
		image.committed.mappings[0].mapfilenumber = 81234;
		relmap_test_crc(&image.committed);
		if (complete)
			UT_ASSERT_EQ(mkdirat(fd, "base/42", 0700), 0);
		for (unsigned i = 0; i < pairs; i++) {
			image.identity.shared = i < 2;
			image.identity.dbid = i < 2 ? InvalidOid : i < 4 ? 70001 : 42;
			relmap_census_file(fd, paths[i], &image);
		}
		UT_ASSERT(execution_observe(&source, &out));
		if (out.roots != NULL) {
			UT_ASSERT(
				cluster_relmap_retention_census_begin_cold(out.roots, 0, 128 * 1024, &census));
			if (census != NULL)
				for (unsigned i = 0; i < 100000 && result == CLUSTER_RELMAP_CENSUS_MORE; i++) {
					ClusterRelmapCensusCommitted committed;
					result = cluster_relmap_retention_census_next_committed(census, &committed);
					if (committed.present) {
						images++;
						UT_ASSERT_EQ(committed.map.mappings[0].mapfilenumber, 81234);
					}
				}
			/* WAL-only base/42 authority remains a missing obligation, even
		 * when all other physical namespaces have complete pairs. */
			UT_ASSERT_EQ(result, complete ? CLUSTER_RELMAP_CENSUS_DRAINED
										  : CLUSTER_RELMAP_CENSUS_UNAVAILABLE);
			UT_ASSERT_EQ(images, complete ? 3 : 0);
			cluster_relmap_retention_census_destroy(census);
			UT_ASSERT(fcntl(out.root_fd, F_GETFD) >= 0);
		}
		execution_observe_release(&out); /* All dependents are gone before fd close. */
		for (unsigned i = 0; i < pairs; i++)
			UT_ASSERT_EQ(unlinkat(fd, paths[i], 0), 0);
		if (complete)
			UT_ASSERT_EQ(unlinkat(fd, "base/42", AT_REMOVEDIR), 0);
		UT_ASSERT_EQ(close(fd), 0);
		execution_observe_done(&out);
	}
}
