/* Original stage -> physical preservation -> local map installation.
 * The physical checker and preserve leaves are controlled here; the actual
 * filesystem checker is tested separately. No live/terminal qualification.
 * Author: SqlRush <sqlrush@gmail.com>
 */
enum V2PrepareFault
{
	PREPARE_OK,
	PREPARE_FIRST_FALSE, PREPARE_LAST_FALSE,
	PREPARE_FIRST_INVALIDATE, PREPARE_LAST_INVALIDATE,
	PREPARE_PRESERVE_DRIFT, PREPARE_PRESERVE_INVALIDATE,
	PREPARE_FIRST_ERROR, PREPARE_LAST_ERROR, PREPARE_PRESERVE_ERROR,
	PREPARE_STAGE_PREFLIGHT_FALSE, PREPARE_STAGE_WRITE_FALSE,
	PREPARE_FIRST_PIN_END, PREPARE_FIRST_PIN_UNMARK, PREPARE_LAST_PIN_END
};
static enum V2PrepareFault prepare_fault;
static unsigned prepare_checks;
static ClusterRelmapV2Image prepare_seen;

bool
cluster_relmap_authority_v2_check_staged(const ClusterRelmapV2Image *staged)
{
	ClusterRelmapRetentionDirectory *directory = &relmap_runtime_shmem->directories[cluster_node_id];
	uint8 wire[CLUSTER_RELMAP_V2_FILE_SIZE];

	UT_ASSERT(authority_held && local_held && !v2_retention_held && CritSectionCount == 0);
	UT_ASSERT_EQ(v2_stage_calls, 1);
	UT_ASSERT_EQ(v2_stage_writes, 2);
	UT_ASSERT_EQ(flushes, 1);
	UT_ASSERT_EQ(cluster_relmap_v2_encode(staged, wire, sizeof(wire)), CLUSTER_RELMAP_V2_VALID);
	UT_ASSERT(memcmp(&staged->pending, &v2_staged_map, sizeof(RelMapFile)) == 0);
	UT_ASSERT_EQ(staged->pending_owner.wal_end, v2_staged_owner.wal_end);
	prepare_seen = *staged;
	prepare_checks++;
	if ((prepare_checks == 1 && prepare_fault == PREPARE_FIRST_ERROR)
		|| (prepare_checks == 2 && prepare_fault == PREPARE_LAST_ERROR))
		ereport(ERROR, (errmsg("controlled exact-stage observation error")));
	if ((prepare_checks == 1 && prepare_fault == PREPARE_FIRST_FALSE)
		|| (prepare_checks == 2 && prepare_fault == PREPARE_LAST_FALSE))
		return false;
	if ((prepare_checks == 1 && prepare_fault == PREPARE_FIRST_INVALIDATE)
		|| (prepare_checks == 2 && prepare_fault == PREPARE_LAST_INVALIDATE))
		cluster_relmap_retention_invalidate(directory);
	if ((prepare_checks == 1 && prepare_fault == PREPARE_FIRST_PIN_END)
		|| (prepare_checks == 2 && prepare_fault == PREPARE_LAST_PIN_END))
		directory->pins[1].wal_end++;
	if (prepare_checks == 1 && prepare_fault == PREPARE_FIRST_PIN_UNMARK)
		directory->pins[1].may_be_staged = false;
	return true;
}

static void
prepare_preserve(RelFileLocator locator, bool at_commit)
{
	UT_ASSERT(authority_held && local_held && !v2_retention_held && CritSectionCount == 0);
	UT_ASSERT(!at_commit && preserves < 2);
	UT_ASSERT_EQ(locator.spcOid, prepare_seen.identity.shared ? GLOBALTABLESPACE_OID : MyDatabaseTableSpace);
	UT_ASSERT_EQ(locator.dbOid, prepare_seen.identity.dbid);
	UT_ASSERT_EQ(locator.relNumber, prepare_seen.pending.mappings[preserves].mapfilenumber);
	preserves++;
	if (preserves == 1)
	{
		if (prepare_fault == PREPARE_PRESERVE_DRIFT)
			change_original_owner();
		if (prepare_fault == PREPARE_PRESERVE_INVALIDATE)
			cluster_relmap_retention_invalidate(&relmap_runtime_shmem->directories[cluster_node_id]);
		if (prepare_fault == PREPARE_PRESERVE_ERROR)
			ereport(ERROR, (errmsg("controlled physical preservation error")));
	}
}

static ClusterRelmapRetentionDirectory *
prepare_fixture(bool preassigned, bool next_era)
{
	ClusterRelmapRetentionDirectory *directory;

	MyProcPid = 79;
	directory = v2_fixture(preassigned, next_era);
	prepare_checks = 0;
	prepare_fault = PREPARE_OK;
	memset(&prepare_seen, 0, sizeof(prepare_seen));
	v2_preserve_observer = prepare_preserve;
	shared_map = local_map = committed;
	allowSystemTableMods = true;
	updates.num_mappings = 2;
	updates.mappings[1].mapoid = 1247;
	updates.mappings[1].mapfilenumber = 502;
	return directory;
}

static void
prepare_original_terminal_untouched(void)
{
	UT_ASSERT(authority_held && !local_held && !v2_retention_held && CritSectionCount == 0);
	UT_ASSERT(!cluster_relmap_publish_arms[0].armed && !cluster_relmap_publish_arms[1].armed);
	UT_ASSERT_EQ(publish_calls[0] + publish_calls[1] + discard_calls[0] + discard_calls[1], 0);
	UT_ASSERT_EQ(unlock_calls, 0);
}

UT_TEST(v2_prepare_preserves_installs_and_forces_sync)
{
	for (unsigned which = 0; which < 8; which++)
	{
		bool shared = (which & 1) != 0;
		ClusterRelmapRetentionDirectory *directory = prepare_fixture((which & 2) != 0, (which & 4) != 0);
		ClusterRelmapRetentionPin old = directory->pins[0];
		ClusterRelmapOwnerSampleV2 sample;
		ClusterRelmapV2Image image;
		bool ok = cluster_relmap_producer_prepare_v2(shared, &updates, &sample, &image);

		UT_ASSERT(ok);
		if (!ok)
			continue;
		UT_ASSERT_EQ(full_requests, 1);
		UT_ASSERT_EQ(raw_requests + xid_under_lock + xid_in_crit, 0);
		UT_ASSERT_EQ(local_acquires, 2);
		UT_ASSERT_EQ(inserts, 1);
		UT_ASSERT_EQ(flushes, 1);
		UT_ASSERT_EQ(v2_stage_writes, 2);
		UT_ASSERT_EQ(prepare_checks, 2);
		UT_ASSERT_EQ(preserves, 2);
		UT_ASSERT_EQ(sync_commits, 1);
		UT_ASSERT_EQ(image.pending_generation, 8);
		UT_ASSERT_EQ(image.pending_owner.wal_end, 1001);
		UT_ASSERT_EQ(image.pending.num_mappings, 2);
		UT_ASSERT(memcmp(shared ? &shared_map : &local_map, &image.pending, sizeof(RelMapFile)) == 0);
		UT_ASSERT(memcmp(shared ? &local_map : &shared_map, &committed, sizeof(RelMapFile)) == 0);
		UT_ASSERT(memcmp(&directory->pins[0], &old, sizeof(old)) == 0);
		UT_ASSERT(directory->pins[1].occupied && directory->pins[1].may_be_staged);
		UT_ASSERT_EQ(directory->pins[1].wal_end, 1001);
		UT_ASSERT(cluster_relmap_owner_matches_v2(&sample));
		prepare_original_terminal_untouched();
	}
	v2_preserve_observer = NULL;
}

UT_TEST(v2_prepare_refusals_keep_local_map_and_debt)
{
	const enum V2PrepareFault faults[] = {
		PREPARE_FIRST_FALSE, PREPARE_LAST_FALSE,
		PREPARE_FIRST_INVALIDATE, PREPARE_LAST_INVALIDATE,
		PREPARE_PRESERVE_DRIFT, PREPARE_PRESERVE_INVALIDATE,
		PREPARE_STAGE_PREFLIGHT_FALSE, PREPARE_STAGE_WRITE_FALSE,
		PREPARE_FIRST_PIN_END, PREPARE_FIRST_PIN_UNMARK, PREPARE_LAST_PIN_END
	};

	for (unsigned which = 0; which < lengthof(faults) + 3; which++)
	{
		ClusterRelmapRetentionDirectory *directory = prepare_fixture(false, false);
		ClusterRelmapRetentionPin old = directory->pins[0];
		ClusterRelmapOwnerSampleV2 sample, before_sample;
		ClusterRelmapV2Image image, before_image;

		memset(&sample, 0xa5, sizeof(sample)); before_sample = sample;
		memset(&image, 0x5a, sizeof(image)); before_image = image;
		if (which < lengthof(faults)) prepare_fault = faults[which];
		if (prepare_fault == PREPARE_STAGE_PREFLIGHT_FALSE) v2_stage_mode = 1;
		if (prepare_fault == PREPARE_STAGE_WRITE_FALSE) v2_stage_mode = 2;
		if (which == lengthof(faults)) cluster_relmap_publish_arms[0].armed = true;
		UT_ASSERT(!cluster_relmap_producer_prepare_v2(true, &updates,
			which == lengthof(faults) + 1 ? NULL : &sample,
			which == lengthof(faults) + 2 ? NULL : &image));
		UT_ASSERT(memcmp(&sample, &before_sample, sizeof(sample)) == 0);
		UT_ASSERT(memcmp(&image, &before_image, sizeof(image)) == 0);
		UT_ASSERT(memcmp(&shared_map, &committed, sizeof(RelMapFile)) == 0);
		UT_ASSERT(memcmp(&local_map, &committed, sizeof(RelMapFile)) == 0);
		UT_ASSERT(memcmp(&directory->pins[0], &old, sizeof(old)) == 0);
		UT_ASSERT_EQ(sync_commits, 0);
		UT_ASSERT(authority_held && !local_held && !v2_retention_held);
		if (which < lengthof(faults))
		{
			bool late = prepare_fault == PREPARE_LAST_FALSE || prepare_fault == PREPARE_LAST_INVALIDATE
				|| prepare_fault == PREPARE_PRESERVE_DRIFT || prepare_fault == PREPARE_PRESERVE_INVALIDATE
				|| prepare_fault == PREPARE_LAST_PIN_END;
			UT_ASSERT_EQ(inserts, 1);
			UT_ASSERT_EQ(preserves, late ? 2 : 0);
			UT_ASSERT(directory->pins[1].occupied);
			UT_ASSERT_EQ(directory->pins[1].may_be_staged,
				prepare_fault != PREPARE_STAGE_PREFLIGHT_FALSE && prepare_fault != PREPARE_FIRST_PIN_UNMARK);
		}
		else
			UT_ASSERT_EQ(inserts + preserves + prepare_checks, 0);
	}
	v2_preserve_observer = NULL;
}

UT_TEST(v2_staged_runtime_check_requires_original_exact_pin)
{
	for (unsigned which = 0; which < 25; which++)
	{
		ClusterRelmapRetentionDirectory *directory = prepare_fixture(true, false);
		ClusterRelmapRetentionDirectory before;
		ClusterRelmapOwnerSampleV2 sample;
		ClusterRelmapV2Image image;
		unsigned assignments_before;

		UT_ASSERT(cluster_relmap_producer_stage_v2(true, &updates, &sample, &image));
		if (which == 3) image.identity.system_identifier++;
		if (which == 4) image.identity.database_incarnation++;
		if (which == 5) image.identity.dbid = 99;
		if (which == 6) image.pending_generation++;
		if (which == 7) image.pending_owner.node++;
		if (which == 8) image.pending_owner.top_full_xid += UINT64CONST(0x100000000);
		if (which == 9) image.pending_owner.membership_epoch++;
		if (which == 10) image.pending_owner.boot_incarnation++;
		if (which == 11) image.pending_owner.admitted_incarnation++;
		if (which == 12) image.pending_owner.wal_end++;
		if (which == 13) directory->origin++;
		if (which == 14) directory->state = CLUSTER_RELMAP_RETENTION_UNKNOWN;
		if (which == 15) directory->state = CLUSTER_RELMAP_RETENTION_BUILDING;
		if (which == 16) directory->pins[1].occupied = false;
		if (which == 17) directory->pins[1].may_be_staged = false;
		if (which == 18) directory->pins[1].wal_end++;
		if (which == 19) directory->pins[1].key.top_full_xid++;
		if (which == 20) v2_retention_held = true;
		if (which == 21) v2_xidgen_held = true;
		if (which == 22) writer_epoch++;
		if (which == 23) MyProcPid++;
		if (which == 24) local_held = true;
		before = *directory;
		assignments_before = full_requests;
		UT_ASSERT_EQ(cluster_relmap_retention_runtime_check_staged_v2(which == 1 ? NULL : &sample,
			which == 2 ? NULL : &image), which == 0 || which == 24);
		UT_ASSERT(memcmp(directory, &before, sizeof(before)) == 0);
		UT_ASSERT_EQ(full_requests, assignments_before);
		UT_ASSERT_EQ(inserts, 1);
		UT_ASSERT_EQ(preserves + sync_commits + prepare_checks, 0);
		v2_retention_held = v2_xidgen_held = local_held = false;
	}
	MyProcPid = 79;
	v2_preserve_observer = NULL;
}

static bool
prepare_raises(ClusterRelmapOwnerSampleV2 *sample, ClusterRelmapV2Image *image)
{
	error_expected = true;
	if (sigsetjmp(error_target, 0) == 0)
	{
		(void) cluster_relmap_producer_prepare_v2(true, &updates, sample, image);
		error_expected = false;
		return false;
	}
	error_expected = false;
	return true;
}

UT_TEST(v2_prepare_errors_keep_original_failure_duties)
{
	const enum V2PrepareFault faults[] = { PREPARE_FIRST_ERROR, PREPARE_LAST_ERROR, PREPARE_PRESERVE_ERROR };

	for (unsigned which = 0; which < lengthof(faults); which++)
	{
		ClusterRelmapRetentionDirectory *directory = prepare_fixture(false, false);
		ClusterRelmapRetentionPin old = directory->pins[0];
		ClusterRelmapOwnerSampleV2 sample, before_sample;
		ClusterRelmapV2Image image, before_image;

		memset(&sample, 0xa5, sizeof(sample)); before_sample = sample;
		memset(&image, 0x5a, sizeof(image)); before_image = image;
		prepare_fault = faults[which];
		UT_ASSERT(prepare_raises(&sample, &image));
		UT_ASSERT_EQ(error_level, ERROR);
		UT_ASSERT_EQ(preserves, which == 0 ? 0 : which == 1 ? 2 : 1);
		UT_ASSERT(memcmp(&sample, &before_sample, sizeof(sample)) == 0);
		UT_ASSERT(memcmp(&image, &before_image, sizeof(image)) == 0);
		UT_ASSERT(memcmp(&shared_map, &committed, sizeof(RelMapFile)) == 0);
		UT_ASSERT(memcmp(&directory->pins[0], &old, sizeof(old)) == 0);
		UT_ASSERT(directory->pins[1].occupied && directory->pins[1].may_be_staged);
		UT_ASSERT(authority_held && local_held && !v2_retention_held && CritSectionCount == 0);
		UT_ASSERT_EQ(sync_commits, 0);
		/* Original native transaction abort, represented only by this caller. */
		local_held = false;
		prepare_original_terminal_untouched();
	}
	v2_preserve_observer = NULL;
}
