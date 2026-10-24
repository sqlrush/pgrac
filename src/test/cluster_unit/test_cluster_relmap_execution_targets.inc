/* Actual original checkpoint/WAL reader, target parsers and physical ROOT.
 * Original execution scope/ownership remain controlled fixture leaves.
 * Author: SqlRush <sqlrush@gmail.com> */
static int targets_fd = -1;
static char targets_file[MAXPGPATH], targets_database[MAXPGPATH], targets_base[MAXPGPATH];

static ClusterRecoveryRootSet *
execution_targets_setup(ClusterRelmapRecoverySourceObservationV1 *source)
{
	ClusterRecoveryRootSet *roots = NULL;
	ClusterSpaceIdentityKey key = { 0 };
	int fd;
	*source = execution_root_setup();
	execution_checkpoint_after_hook = NULL;
	execution_visits = execution_records = 0;
	snprintf(targets_base, sizeof(targets_base), "%s/base", test_root);
	snprintf(targets_database, sizeof(targets_database), "%s/70001", targets_base);
	snprintf(targets_file, sizeof(targets_file), "%s/81234", targets_database);
	if ((mkdir(targets_base, 0700) != 0 && errno != EEXIST) || mkdir(targets_database, 0700) != 0)
		abort();
	fd = open(targets_file, O_WRONLY | O_CREAT | O_EXCL, 0600);
	if (fd < 0 || write(fd, "x", 1) != 1 || close(fd) != 0)
		abort();
	targets_fd = OpenTransientFile(test_root, O_RDONLY | O_DIRECTORY | O_CLOEXEC);
	UT_ASSERT_EQ(cluster_recovery_root_set_begin(
					 targets_fd, source->source.claim.identity.system_identifier,
					 source->source.claim.database_incarnation,
					 source->source.claim.identity.storage_uuid, 2 * 1024 * 1024, &roots),
				 CLUSTER_RECOVERY_ROOTS_MORE);
	key.system_identifier = source->source.claim.identity.system_identifier;
	key.database_incarnation = source->source.claim.database_incarnation;
	memcpy(key.storage_uuid, source->source.claim.identity.storage_uuid, 16);
	key.locator = (RelFileLocator){ DEFAULTTABLESPACE_OID, 70001, 81234 };
	UT_ASSERT_EQ(cluster_recovery_root_set_add_fork(roots, &key, MAIN_FORKNUM, 17),
				 CLUSTER_RECOVERY_ROOTS_MORE);
	return roots;
}

static bool
execution_targets_unstarted(ClusterRecoveryRootSet *roots,
							const ClusterRelmapRecoverySourceObservationV1 *source)
{
	return cluster_recovery_root_set_targets_match(
		roots, source->source.claim.identity.system_identifier,
		source->source.claim.database_incarnation, source->source.claim.identity.storage_uuid);
}

static ClusterRecoveryRootsResult
execution_targets_finish(ClusterRecoveryRootSet *roots)
{
	ClusterRecoveryRootsEvent event;
	ClusterRecoveryRootsResult result = CLUSTER_RECOVERY_ROOTS_MORE;
	for (unsigned i = 0; result < CLUSTER_RECOVERY_ROOTS_OBSERVED && i < 100000; i++)
		result = cluster_recovery_root_set_step(roots, &event);
	return result;
}

static void
execution_targets_done(ClusterRecoveryRootSet *roots)
{
	execution_checkpoint_after_hook = NULL;
	cluster_recovery_root_set_destroy(roots);
	UT_ASSERT_EQ(CloseTransientFile(targets_fd), 0);
	targets_fd = -1;
	if (unlink(targets_file) != 0 || rmdir(targets_database) != 0)
		abort();
	(void)rmdir(targets_base);
	UT_ASSERT(!inputs_pin_held && inputs_pin_refuse && test_actual_cf == NoLock);
	CritSectionCount = 0;
	execution_root_done();
}

UT_TEST(test_execution_targets_preserve_real_wal_missing_and_short_files)
{
	ClusterRelmapRecoverySourceObservationV1 source, before;
	ClusterRecoveryRootSet *roots = execution_targets_setup(&source);
	ClusterRelmapRecoveryRootTargetsV1 out;
	bool short_file = false, missing = false;
	before = source;
	UT_ASSERT(cluster_relmap_recovery_root_targets_cold_v1(roots, &source, 1024 * 1024, &out));
	UT_ASSERT(out.records >= 3 && out.native_effects > 0 && out.other_native_effects > 0);
	UT_ASSERT(out.unowned_records > 0 && execution_visits == 1);
	UT_ASSERT(execution_targets_unstarted(roots, &source));
	UT_ASSERT_EQ(execution_targets_finish(roots), CLUSTER_RECOVERY_ROOTS_OBSERVED);
	for (uint32 i = 0; i < cluster_recovery_root_set_count(roots); i++) {
		const ClusterRecoveryRootEntry *entry = cluster_recovery_root_set_entry(roots, i);
		if (strcmp(entry->file.path, "base/70001/81234") == 0)
			short_file = entry->required_blocks == 17 && entry->file.size == 1
						 && (entry->origins & CLUSTER_RECOVERY_ROOT_FROM_WAL)
						 && (entry->origins & CLUSTER_RECOVERY_ROOT_PHYSICAL);
		if ((entry->origins & CLUSTER_RECOVERY_ROOT_FROM_WAL)
			&& !(entry->origins & CLUSTER_RECOVERY_ROOT_PHYSICAL) && entry->required_blocks > 0)
			missing = true;
	}
	UT_ASSERT(short_file && missing);
	UT_ASSERT_EQ(memcmp(&source, &before, sizeof(source)), 0);
	UT_ASSERT_EQ(prefix_base_reads + inputs_peer_begins + inputs_native_begin_calls, 0);
	execution_targets_done(roots);
}

UT_TEST(test_execution_targets_missing_initial_gates_leave_root_unchanged)
{
	for (unsigned fault = 0; fault < 8; fault++) {
		ClusterRelmapRecoverySourceObservationV1 source;
		ClusterRecoveryRootSet *roots = execution_targets_setup(&source);
		ClusterRelmapRecoveryRootTargetsV1 out, zero = { 0 };
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
			execution_pending_guard_fault = 1;
		memset(&out, 0xa5, sizeof(out));
		UT_ASSERT_EQ(
			cluster_relmap_recovery_root_targets_cold_v1(roots, &source, 1024 * 1024, &out),
			fault == 0);
		UT_ASSERT_EQ(execution_visits, fault == 0 ? 1 : 0);
		if (fault != 0)
			UT_ASSERT_EQ(memcmp(&out, &zero, sizeof(out)), 0);
		UT_ASSERT(execution_targets_unstarted(roots, &source));
		execution_targets_done(roots);
	}
}

UT_TEST(test_execution_targets_invalid_source_leaves_unstarted_root)
{
	for (unsigned fault = 0; fault < 7; fault++) {
		ClusterRelmapRecoverySourceObservationV1 source, invalid;
		ClusterRecoveryRootSet *roots = execution_targets_setup(&source);
		ClusterRelmapRecoveryRootTargetsV1 out, zero = { 0 };
		invalid = source;
		if (fault == 1)
			invalid.scope_serial++;
		if (fault == 2)
			invalid.checkpoint.root_publish_seq++;
		if (fault == 3)
			invalid.native_redo++;
		if (fault == 4)
			invalid.source.claim.identity.storage_uuid[0]++;
		if (fault == 5)
			invalid.source.claim.database_incarnation++;
		memset(&out, 0xa5, sizeof(out));
		UT_ASSERT_EQ(cluster_relmap_recovery_root_targets_cold_v1(
						 roots, fault == 6 ? NULL : &invalid, 1024 * 1024, &out),
					 fault == 0);
		UT_ASSERT_EQ(execution_visits, fault == 0 ? 1 : 0);
		if (fault != 0)
			UT_ASSERT_EQ(memcmp(&out, &zero, sizeof(out)), 0);
		UT_ASSERT(execution_targets_unstarted(roots, &source));
		execution_targets_done(roots);
	}
}

UT_TEST(test_execution_targets_late_visit_refusal_or_error_revokes_partial_root)
{
	const unsigned faults[] = { 0, 2, 3, 4, 5, 6, 7, 8, 9 };
	for (unsigned i = 0; i < lengthof(faults); i++) {
		ClusterRelmapRecoverySourceObservationV1 source, before;
		ClusterRecoveryRootSet *roots = execution_targets_setup(&source);
		ClusterRelmapRecoveryRootTargetsV1 out, zero = { 0 };
		volatile bool threw = false, accepted = false;
		before = source;
		execution_fault = faults[i];
		memset(&out, 0xa5, sizeof(out));
		test_capture_error_level = true;
		PG_TRY();
		{
			accepted
				= cluster_relmap_recovery_root_targets_cold_v1(roots, &source, 1024 * 1024, &out);
		}
		PG_CATCH();
		{
			threw = true;
		}
		PG_END_TRY();
		test_capture_error_level = false;
		UT_ASSERT_EQ(accepted, i == 0);
		UT_ASSERT_EQ(threw, faults[i] == 3 || faults[i] == 9);
		if (i != 0) {
			UT_ASSERT_EQ(memcmp(&out, &zero, sizeof(out)), 0);
			UT_ASSERT(!execution_targets_unstarted(roots, &source));
			UT_ASSERT(execution_targets_finish(roots) > CLUSTER_RECOVERY_ROOTS_OBSERVED);
		}
		UT_ASSERT_EQ(memcmp(&source, &before, sizeof(source)), 0);
		execution_targets_done(roots);
	}
}

UT_TEST(test_execution_targets_payload_refusal_preserves_original_owner)
{
	for (unsigned fault = 0; fault < 2; fault++) {
		ClusterRelmapRecoverySourceObservationV1 source;
		ClusterRecoveryRootSet *roots = execution_targets_setup(&source);
		ClusterRelmapRecoveryRootTargetsV1 out, zero = { 0 };
		memset(&out, 0xa5, sizeof(out));
		UT_ASSERT_EQ(cluster_relmap_recovery_root_targets_cold_v1(roots, &source,
																  fault ? 1 : 1024 * 1024, &out),
					 fault == 0);
		if (fault) {
			UT_ASSERT_EQ(memcmp(&out, &zero, sizeof(out)), 0);
			UT_ASSERT(execution_targets_finish(roots) > CLUSTER_RECOVERY_ROOTS_OBSERVED);
		}
		UT_ASSERT_EQ(prefix_base_reads + inputs_peer_begins + inputs_native_begin_calls, 0);
		execution_targets_done(roots);
	}
}

static unsigned execution_targets_late_fault;
static ClusterRelmapRecoverySourceObservationV1 *execution_targets_borrowed;
static void
execution_targets_late_hook(void)
{
	if (execution_targets_late_fault == 1)
		relmap_root_merger = false;
	if (execution_targets_late_fault == 2)
		relmap_root_namespace_x = false;
	if (execution_targets_late_fault == 3)
		cluster_shared_catalog = false;
	if (execution_targets_late_fault == 4)
		CritSectionCount = 1;
	if (execution_targets_late_fault == 5)
		custody_scope++;
	if (execution_targets_late_fault == 6)
		execution_targets_borrowed->scope_serial++;
	if (execution_targets_late_fault == 7)
		execution_pending_guard_fault = 4;
}

UT_TEST(test_execution_targets_late_context_or_input_drift_revokes_root)
{
	for (unsigned fault = 0; fault < 8; fault++) {
		ClusterRelmapRecoverySourceObservationV1 source;
		ClusterRecoveryRootSet *roots = execution_targets_setup(&source);
		ClusterRelmapRecoveryRootTargetsV1 out, zero = { 0 };
		volatile bool threw = false, accepted = false;
		execution_targets_late_fault = fault;
		execution_targets_borrowed = &source;
		execution_checkpoint_after_hook = execution_targets_late_hook;
		memset(&out, 0xa5, sizeof(out));
		test_capture_error_level = true;
		PG_TRY();
		{
			accepted
				= cluster_relmap_recovery_root_targets_cold_v1(roots, &source, 1024 * 1024, &out);
		}
		PG_CATCH();
		{
			threw = true;
		}
		PG_END_TRY();
		test_capture_error_level = false;
		UT_ASSERT_EQ(accepted, fault == 0);
		UT_ASSERT_EQ(threw, fault == 7);
		if (fault != 0) {
			UT_ASSERT_EQ(memcmp(&out, &zero, sizeof(out)), 0);
			UT_ASSERT(execution_targets_finish(roots) > CLUSTER_RECOVERY_ROOTS_OBSERVED);
		}
		execution_targets_done(roots);
	}
}

UT_TEST(test_execution_targets_alias_and_started_root_refuse_safely)
{
	ClusterRelmapRecoverySourceObservationV1 source, before;
	ClusterRecoveryRootSet *roots = execution_targets_setup(&source);
	ClusterRelmapRecoveryRootTargetsV1 out, zero = { 0 };
	uint8 digest[32];
	before = source;
	UT_ASSERT(!cluster_relmap_recovery_root_targets_cold_v1(roots, &source, 1024 * 1024, NULL));
	UT_ASSERT(!cluster_relmap_recovery_root_targets_cold_v1(roots, &source, 1024 * 1024,
															(void *)&source));
	UT_ASSERT(!cluster_relmap_recovery_root_targets_cold_v1(
		roots, &source, 1024 * 1024, (void *)((char *)&source + sizeof(source) - 1)));
	UT_ASSERT(
		!cluster_relmap_recovery_root_targets_cold_v1(roots, &source, 1024 * 1024, (void *)roots));
	UT_ASSERT_EQ(memcmp(&source, &before, sizeof(source)), 0);
	UT_ASSERT(execution_targets_unstarted(roots, &source) && execution_visits == 0);
	UT_ASSERT(cluster_relmap_recovery_root_targets_cold_v1(roots, &source, 1024 * 1024, &out));
	UT_ASSERT_EQ(execution_targets_finish(roots), CLUSTER_RECOVERY_ROOTS_OBSERVED);
	memset(&out, 0xa5, sizeof(out));
	UT_ASSERT(!cluster_relmap_recovery_root_targets_cold_v1(roots, &source, 1024 * 1024, &out));
	UT_ASSERT_EQ(memcmp(&out, &zero, sizeof(out)), 0);
	UT_ASSERT(cluster_recovery_root_set_digest(roots, digest));
	execution_targets_done(roots);
}
