/*-------------------------------------------------------------------------
 * test_cluster_recovery_managed_census.inc
 *    Managed pairs through the complete census and final manifest.
 *
 * Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Copyright (c) 1994, The Regents of the University of California
 * Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *    src/test/cluster_unit/test_cluster_recovery_managed_census.inc
 *
 * NOTES
 *    PGRAC-original tests included by the native-input census fixture.
 *    All helpers are static and use the managed_census_* namespace.
 *-------------------------------------------------------------------------
 */
static void
managed_census_pair_at(int fd, bool pending, const ClusterRecoveryFileIdentityV2 *identity)
{
	char primary[100], backup[100];
	const char *paths[] = { primary, backup };
	ClusterRelmapV2Image image = { 0 };
	uint8 bytes[CLUSTER_RELMAP_FILE_SIZE_V2];
	snprintf(primary, sizeof(primary), "base/%u/pgrac_relmap_authority", identity->database_oid);
	snprintf(backup, sizeof(backup), "base/%u/pgrac_relmap_authority.bak", identity->database_oid);

	image.identity
		= (ClusterRelmapV2Namespace){ identity->system_identifier, identity->database_incarnation,
									  identity->database_oid, false };
	image.committed_generation = 7;
	image.committed.magic = RELMAPPER_FILEMAGIC;
	image.committed.num_mappings = 1;
	image.committed.mappings[0] = (RelMapping){ 100, 16385 };
	INIT_CRC32C(image.committed.crc);
	COMP_CRC32C(image.committed.crc, &image.committed, offsetof(RelMapFile, crc));
	FIN_CRC32C(image.committed.crc);
	if (pending) {
		image.pending_generation = 8;
		image.pending_owner = (ClusterRelmapV2Owner){ 1, 0x200000020, 3, 4, 500, 6 };
		image.pending = image.committed;
	}
	UT_ASSERT(cluster_relmap_file_encode_v2(identity, &image, bytes, sizeof(bytes)));
	for (unsigned c = 0; c < 2; c++) {
		int f = openat(fd, paths[c], O_CREAT | O_TRUNC | O_WRONLY, 0600);
		UT_ASSERT(f >= 0 && write(f, bytes, sizeof(bytes)) == sizeof(bytes) && close(f) == 0);
	}
}
static void
managed_census_pair(int fd, bool pending)
{
	managed_census_pair_at(fd, pending, &managed_census_bank()->files[0]);
}
static ClusterRecoveryCensus *
managed_census_fixture(ClusterWalInputsV1 **inputs, int *fd, char path[MAXPGPATH])
{
	ClusterRecoveryCensus *census = owned_census_fixture(inputs, fd, path, false);
	const ClusterWalInputV1 *first = cluster_wal_inputs_at_v1(*inputs, 0);
	ClusterRecoveryFileIdentityV2 *id = &managed_census_bank()->files[0];
	memset(&managed_census_control, 0, sizeof(managed_census_control));
	memset(&managed_census_shared, 0, sizeof(managed_census_shared));
	SpinLockInit(&managed_census_shared.lock);
	native_files = &managed_census_shared;
	managed_census_bank()->info.mode = CLUSTER_NATIVE_FILES_MANAGED_V2;
	managed_census_bank()->info.table_generation = 7;
	managed_census_bank()->info.startup_generation = 9;
	managed_census_bank()->info.count = 1;
	managed_census_bank()->info.capacity = CLUSTER_NATIVE_FILES_CAPACITY_V2;
	managed_census_bank()->info.registry_byte_budget = 65536;
	managed_census_bank()->info.shared_bytes = sizeof(managed_census_shared);
	managed_census_control.current = true;
	managed_census_bank()->info.control.config.identity.system_identifier
		= first->source.claim.identity.system_identifier;
	managed_census_bank()->info.control.config.identity.database_incarnation
		= first->source.claim.database_incarnation;
	memcpy(managed_census_bank()->info.control.config.identity.storage_uuid,
		   first->source.claim.identity.storage_uuid, 16);
	managed_census_bank()->info.control.logical_count = 1;
	managed_census_bank()->info.control.source_count = cluster_wal_inputs_count_v1(*inputs);
	id->kind = CLUSTER_RECOVERY_FILE_RELMAP_V2;
	id->format_id = CLUSTER_RECOVERY_FORMAT_ID_V2;
	id->format_version = CLUSTER_RECOVERY_FORMAT_VERSION_V2;
	id->redo_timeline = 1;
	id->origin = first->source.claim.identity.origin_node_id;
	id->system_identifier = first->source.claim.identity.system_identifier;
	id->database_incarnation = first->source.claim.database_incarnation;
	memcpy(id->storage_uuid, first->source.claim.identity.storage_uuid, 16);
	memset(id->redo_branch_uuid, 0x31, 16);
	id->redo_branch_generation = 13;
	id->creation_generation = 15;
	id->registration_generation = 16;
	id->database_oid = 5;
	id->tablespace_oid = 1663;
	managed_census_pair(*fd, true);
	managed_census_expected = managed_census_bank()->info;
	return census;
}
static bool
managed_census_bind(ClusterRecoveryCensus *census)
{
#ifdef TEST_MANAGED_CENSUS_PARENT
	return true; /* Exact old product: no binding or managed dispatch exists. */
#else
	return cluster_recovery_census_bind_managed_relmap_v2(census, (void *)&managed_census_expected);
#endif
}
static void
managed_census_end(ClusterRecoveryCensus *census, ClusterWalInputsV1 **inputs, int fd,
				   const char *path)
{
	UT_ASSERT_EQ(managed_census_shared.borrowed, 0);
	UT_ASSERT_EQ(unlinkat(fd, "base/5/pgrac_relmap_authority", 0), 0);
	UT_ASSERT_EQ(unlinkat(fd, "base/5/pgrac_relmap_authority.bak", 0), 0);
	owned_census_done(census, inputs, fd, path);
}
UT_TEST(test_managed_census_keeps_full_pair_before_original_final_manifest)
{
	ClusterWalInputsV1 *inputs;
	ClusterRecoveryCensus *census;
	ClusterRecoveryCensusEntry out;
	ClusterRecoveryCensusManifest manifest;
	ClusterRecoveryCensusResult result = CLUSTER_RECOVERY_CENSUS_MORE;
	char path[MAXPGPATH];
	int fd;
	unsigned images = 0, mappings = 0, targets = 0;
	for (unsigned mode = 0; mode < 2; mode++) {
		images = mappings = targets = 0;
		census = managed_census_fixture(&inputs, &fd, path);
		if (mode == 1) {
			cluster_recovery_census_destroy(census);
			census = NULL;
			UT_ASSERT_EQ(
				cluster_recovery_census_begin_images_v1(inputs, fd, 8 * 1024 * 1024, &census),
				CLUSTER_RECOVERY_CENSUS_MORE);
		}
		UT_ASSERT(managed_census_bind(census));
		for (unsigned i = 0; i < 200000; i++) {
			result = cluster_recovery_census_next(census, &out);
			if (result >= CLUSTER_RECOVERY_CENSUS_DRAINED)
				break;
			if (result != CLUSTER_RECOVERY_CENSUS_ENTRY
				|| out.kind != CLUSTER_RECOVERY_CENSUS_RELMAP)
				continue;
			UT_ASSERT(out.value.relmap.file_identity && out.value.relmap.managed_image);
			images += out.value.relmap.kind == CLUSTER_RECOVERY_RELMAP_IMAGE;
			mappings += out.value.relmap.kind == CLUSTER_RECOVERY_RELMAP_MAPPING;
			targets += out.value.relmap.kind == CLUSTER_RECOVERY_RELMAP_TARGET;
		}
		UT_ASSERT_EQ(result, CLUSTER_RECOVERY_CENSUS_DRAINED);
		UT_ASSERT_EQ(images, 4);
		UT_ASSERT_EQ(mappings, 4);
		UT_ASSERT_EQ(targets, 16);
		UT_ASSERT(cluster_recovery_census_manifest(census, &manifest));
		UT_ASSERT(inputs_pin_current && fcntl(fd, F_GETFD) >= 0);
		managed_census_end(census, &inputs, fd, path);
	}
}

UT_TEST(test_managed_census_binding_is_once_before_any_scan)
{
	UT_ASSERT(!managed_census_scan_diag(NULL, NULL));
	for (unsigned mode = 0; mode < 7; mode++) {
		ClusterWalInputsV1 *inputs;
		char path[MAXPGPATH];
		int fd;
		ClusterRecoveryCensus *census = managed_census_fixture(&inputs, &fd, path);
		if (mode == 0) {
			UT_ASSERT(!cluster_recovery_census_bind_managed_relmap_v2(
				NULL, (void *)&managed_census_expected));
			UT_ASSERT(!cluster_recovery_census_bind_managed_relmap_v2(census, NULL));
			UT_ASSERT(managed_census_bind(census));
			UT_ASSERT(!managed_census_bind(census));
			UT_ASSERT_EQ(census->status, CLUSTER_RECOVERY_CENSUS_MORE);
		} else if (mode == 1) {
			ClusterRecoveryCensusEntry out;
			UT_ASSERT_EQ(cluster_recovery_census_next(census, &out), CLUSTER_RECOVERY_CENSUS_MORE);
			UT_ASSERT(!managed_census_bind(census));
			UT_ASSERT_EQ(managed_census_shared.borrowed, 0);
		} else {
			if (mode == 2)
				managed_census_expected.control.config.identity.system_identifier++;
			if (mode == 3)
				managed_census_expected.control.config.identity.database_incarnation++;
			if (mode == 4)
				managed_census_expected.control.config.identity.storage_uuid[0] ^= 1;
			if (mode == 5)
				managed_census_expected.control.logical_count = 0;
			if (mode == 6)
				managed_census_control.current = false;
			UT_ASSERT(!managed_census_bind(census));
			owned_census_sticky(census, CLUSTER_RECOVERY_CENSUS_REFUSED);
			managed_census_control.current = true;
			UT_ASSERT(!managed_census_bind(census));
		}
		UT_ASSERT(inputs_pin_current && fcntl(fd, F_GETFD) >= 0);
		managed_census_end(census, &inputs, fd, path);
	}
}
UT_TEST(test_managed_census_never_falls_back_to_raw_or_partial_pairs)
{
	for (unsigned mode = 0; mode < 3; mode++) {
		ClusterWalInputsV1 *inputs;
		char path[MAXPGPATH];
		int fd;
		ClusterRecoveryCensus *census = managed_census_fixture(&inputs, &fd, path);
		UT_ASSERT(managed_census_bind(census));
		if (mode == 0) {
			UT_ASSERT_EQ(unlinkat(fd, "base/5/pgrac_relmap_authority.bak", 0), 0);
		} else if (mode == 1) {
			int f = openat(fd, "base/5/pgrac_relmap_authority", O_WRONLY | O_TRUNC);
			UT_ASSERT(f >= 0 && write(f, "short", 5) == 5 && close(f) == 0);
		} else {
			owned_map_write(fd, "base/5/pg_filenode.map", 16385, false);
		}
		UT_ASSERT_EQ(owned_census_until(census, 0), CLUSTER_RECOVERY_CENSUS_REFUSED);
		owned_census_sticky(census, CLUSTER_RECOVERY_CENSUS_REFUSED);
		if (mode == 0)
			managed_census_pair(fd, true);
		if (mode == 2)
			UT_ASSERT_EQ(unlinkat(fd, "base/5/pg_filenode.map", 0), 0);
		managed_census_end(census, &inputs, fd, path);
	}
}
UT_TEST(test_managed_census_control_remains_bound_after_relmap_drain)
{
	const ClusterRecoveryCensusPhase phases[]
		= { CLUSTER_RECOVERY_CENSUS_FINAL_TT, CLUSTER_RECOVERY_CENSUS_FINAL_WAL,
			CLUSTER_RECOVERY_CENSUS_DONE };
	for (unsigned stage = 0; stage < 3; stage++)
		for (unsigned field = 0; field < 7; field++) {
			ClusterWalInputsV1 *inputs;
			char path[MAXPGPATH];
			int fd;
			ClusterRecoveryCensus *census = managed_census_fixture(&inputs, &fd, path);
			UT_ASSERT(managed_census_bind(census));
			UT_ASSERT_EQ(owned_census_until(census, phases[stage]), CLUSTER_RECOVERY_CENSUS_MORE);
			if (field == 0)
				managed_census_bank()->info.control.root_pair_sha256[0] ^= 1;
			if (field == 1)
				managed_census_bank()->info.control.registry_reference[3] ^= 1;
			if (field == 2)
				managed_census_bank()->info.control.logical_count++;
			if (field == 3)
				managed_census_bank()->info.control.source_count++;
			if (field == 4)
				managed_census_control.current = false;
			if (field == 5)
				managed_census_bank()->info.table_generation++;
			if (field == 6)
				managed_census_bank()->info.startup_generation++;
			UT_ASSERT_EQ(owned_census_until(census, 0), CLUSTER_RECOVERY_CENSUS_REFUSED);
			managed_census_control.current = true;
			owned_census_sticky(census, CLUSTER_RECOVERY_CENSUS_REFUSED);
			UT_ASSERT(inputs_pin_current && fcntl(fd, F_GETFD) >= 0);
			managed_census_end(census, &inputs, fd, path);
		}
}
UT_TEST(test_managed_census_original_final_physical_and_source_checks_remain_mandatory)
{
	for (unsigned mode = 0; mode < 3; mode++) {
		ClusterWalInputsV1 *inputs;
		char path[MAXPGPATH];
		int fd;
		ClusterRecoveryCensus *census = managed_census_fixture(&inputs, &fd, path);
		UT_ASSERT(managed_census_bind(census));
		UT_ASSERT_EQ(owned_census_until(census, CLUSTER_RECOVERY_CENSUS_FINAL_TT),
					 CLUSTER_RECOVERY_CENSUS_MORE);
		if (mode == 0) {
			int f = openat(fd, "base/5/pgrac_relmap_authority", O_WRONLY);
			UT_ASSERT(f >= 0 && pwrite(f, "x", 1, 300) == 1 && close(f) == 0);
		} else if (mode == 1)
			inputs_pin_current = false;
		else {
			inventory_fault = 0;
			inventory_drift();
		}
		UT_ASSERT_EQ(owned_census_until(census, 0), CLUSTER_RECOVERY_CENSUS_REFUSED);
		owned_census_sticky(census, CLUSTER_RECOVERY_CENSUS_REFUSED);
		UT_ASSERT(fcntl(fd, F_GETFD) >= 0);
		managed_census_end(census, &inputs, fd, path);
	}
}
UT_TEST(test_managed_census_first_error_is_sticky_without_releasing_original_owners)
{
	const ClusterRecoveryCensusPhase phases[]
		= { CLUSTER_RECOVERY_CENSUS_WAL, CLUSTER_RECOVERY_CENSUS_RELMAP_PHYSICAL,
			CLUSTER_RECOVERY_CENSUS_FINAL_WAL, CLUSTER_RECOVERY_CENSUS_DONE };
	for (unsigned stage = 0; stage < 4; stage++) {
		ClusterWalInputsV1 *inputs;
		char path[MAXPGPATH];
		int fd;
		ClusterRecoveryCensus *census = managed_census_fixture(&inputs, &fd, path);
		volatile bool caught = false;
		if (stage != 0) {
			UT_ASSERT(managed_census_bind(census));
			UT_ASSERT_EQ(owned_census_until(census, phases[stage]), CLUSTER_RECOVERY_CENSUS_MORE);
		}
		managed_census_control.error = true;
		PG_TRY();
		{
			if (stage == 0)
				(void)managed_census_bind(census);
			else {
				ClusterRecoveryCensusEntry out;
				(void)cluster_recovery_census_next(census, &out);
			}
		}
		PG_CATCH();
		{
			caught = true;
		}
		PG_END_TRY();
		managed_census_control.error = false;
		UT_ASSERT(caught && managed_census_control.current);
		owned_census_sticky(census, CLUSTER_RECOVERY_CENSUS_REFUSED);
		UT_ASSERT(census->roots == NULL && census->inventory == NULL && census->relmap == NULL);
		UT_ASSERT(inputs_pin_current && fcntl(fd, F_GETFD) >= 0);
		managed_census_end(census, &inputs, fd, path);
	}
}
UT_TEST(test_managed_census_outputs_never_clear_borrowed_control_or_local_state)
{
	for (unsigned mode = 0; mode < 8; mode++) {
		ClusterWalInputsV1 *inputs;
		char path[MAXPGPATH];
		int fd;
		ClusterRecoveryCensus *census = managed_census_fixture(&inputs, &fd, path);
		UT_ASSERT(managed_census_bind(census));
		if (mode == 0)
			UT_ASSERT_EQ(cluster_recovery_census_next(census, (void *)census),
						 CLUSTER_RECOVERY_CENSUS_INVALID);
		if (mode == 1)
			UT_ASSERT(!cluster_recovery_census_manifest(census, (void *)census));
		if (mode == 2)
			UT_ASSERT(!cluster_recovery_census_diag(census, (void *)census));
		if (mode == 3)
			UT_ASSERT_EQ(cluster_recovery_census_next(census, (void *)census),
						 CLUSTER_RECOVERY_CENSUS_INVALID);
		if (mode == 4)
			UT_ASSERT(!cluster_recovery_census_wal_source_count_v1(census, (void *)census));
		if (mode == 5)
			UT_ASSERT(!cluster_recovery_census_wal_source_at_v1(census, 0, (void *)census));
		if (mode == 6)
			UT_ASSERT(!cluster_recovery_census_page_wal_count_v1(census, (void *)census));
		if (mode == 7)
			UT_ASSERT(!cluster_recovery_census_page_wal_at_v1(census, 0, (void *)census));
		UT_ASSERT(inputs_pin_current && fcntl(fd, F_GETFD) >= 0);
		managed_census_end(census, &inputs, fd, path);
	}
}
UT_TEST(test_managed_census_shared_table_needs_no_cf_but_original_cf_guards_remain)
{
	ClusterWalInputsV1 *inputs;
	char path[MAXPGPATH];
	int fd, namespace_fd = -1;
	ClusterRecoveryCensus *census = managed_census_fixture(&inputs, &fd, path);
	test_actual_cf = test_cf_mode = ShareLock;
	test_inputs_startup_cf = true;
	UT_ASSERT_EQ(cluster_wal_inputs_revalidate_v1(inputs), CLUSTER_CONTROL_ROOT_LOCK_UNAVAILABLE);
	UT_ASSERT_EQ(cluster_wal_inputs_ctrc_namespace_v1(inputs, &namespace_fd),
				 CLUSTER_CONTROL_ROOT_LOCK_UNAVAILABLE);
	test_actual_cf = test_cf_mode = NoLock;
	UT_ASSERT(managed_census_bind(census));
	UT_ASSERT_EQ(owned_census_until(census, 0), CLUSTER_RECOVERY_CENSUS_DRAINED);
	UT_ASSERT(!cluster_cf_held(ShareLock) && !cluster_cf_held(ExclusiveLock));
	managed_census_end(census, &inputs, fd, path);
}

UT_TEST(test_managed_census_missing_table_and_unregistered_or_ambiguous_address_refuse)
{
	for (unsigned mode = 0; mode < 6; mode++) {
		ClusterWalInputsV1 *inputs;
		char path[MAXPGPATH];
		int fd;
		ClusterRecoveryCensus *census = managed_census_fixture(&inputs, &fd, path);
		if (mode == 0)
			native_files = NULL;
		if (mode == 1)
			managed_census_bank()->info.mode = CLUSTER_NATIVE_FILES_UNKNOWN_V2;
		if (mode == 2)
			managed_census_bank()->info.table_generation++;
		if (mode < 3)
			UT_ASSERT(!managed_census_bind(census));
		else {
			if (mode == 3)
				managed_census_bank()->files[0].kind = CLUSTER_RECOVERY_FILE_CLOG_V2;
			if (mode == 4) {
				managed_census_bank()->files[1] = managed_census_bank()->files[0];
				managed_census_bank()->files[1].origin++;
				managed_census_bank()->info.count = 2;
				managed_census_expected = managed_census_bank()->info;
			}
			if (mode == 5)
				managed_census_bank()->files[0].registration_generation++;
			UT_ASSERT(managed_census_bind(census));
			UT_ASSERT_EQ(owned_census_until(census, 0), CLUSTER_RECOVERY_CENSUS_REFUSED);
		}
		native_files = &managed_census_shared;
		owned_census_sticky(census, CLUSTER_RECOVERY_CENSUS_REFUSED);
		UT_ASSERT_EQ(managed_census_shared.borrowed, 0);
		UT_ASSERT(inputs_pin_current && fcntl(fd, F_GETFD) >= 0);
		managed_census_end(census, &inputs, fd, path);
	}
}
UT_TEST(test_managed_census_two_namespaces_retire_each_original_borrow)
{
	ClusterWalInputsV1 *inputs;
	char path[MAXPGPATH];
	int fd;
	ClusterRecoveryCensus *census = managed_census_fixture(&inputs, &fd, path);
	ClusterRecoveryCensusEntry out;
	ClusterRecoveryCensusResult result = CLUSTER_RECOVERY_CENSUS_MORE;
	unsigned images = 0;
	bool held = false;
	UT_ASSERT_EQ(mkdirat(fd, "base/6", 0700), 0);
	managed_census_bank()->files[1] = managed_census_bank()->files[0];
	managed_census_bank()->files[1].database_oid = 6;
	managed_census_bank()->info.count = 2;
	managed_census_bank()->info.control.logical_count = 2;
	managed_census_expected = managed_census_bank()->info;
	managed_census_pair_at(fd, true, &managed_census_bank()->files[1]);
	UT_ASSERT(managed_census_bind(census));
	for (unsigned tick = 0; tick < 200000; tick++) {
		result = cluster_recovery_census_next(census, &out);
		UT_ASSERT(managed_census_shared.borrowed <= 1);
		held |= managed_census_shared.borrowed != 0;
		if (result >= CLUSTER_RECOVERY_CENSUS_DRAINED)
			break;
		if (result == CLUSTER_RECOVERY_CENSUS_ENTRY && out.kind == CLUSTER_RECOVERY_CENSUS_RELMAP
			&& out.value.relmap.kind == CLUSTER_RECOVERY_RELMAP_IMAGE)
			images++;
	}
	UT_ASSERT_EQ(result, CLUSTER_RECOVERY_CENSUS_DRAINED);
	UT_ASSERT_EQ(images, 8);
	UT_ASSERT(held);
	UT_ASSERT_EQ(managed_census_shared.borrowed, 0);
	UT_ASSERT_EQ(unlinkat(fd, "base/6/pgrac_relmap_authority", 0), 0);
	UT_ASSERT_EQ(unlinkat(fd, "base/6/pgrac_relmap_authority.bak", 0), 0);
	UT_ASSERT_EQ(unlinkat(fd, "base/6", AT_REMOVEDIR), 0);
	managed_census_end(census, &inputs, fd, path);
}
static ClusterRecoveryRelmapScan *
managed_census_until_borrow(ClusterRecoveryCensus *census)
{
	ClusterRecoveryCensusEntry out;
	for (unsigned tick = 0; tick < 200000; tick++) {
		ClusterRecoveryCensusResult result = cluster_recovery_census_next(census, &out);
		if (result >= CLUSTER_RECOVERY_CENSUS_DRAINED)
			return NULL;
		if (managed_census_shared.borrowed)
			return census->relmap;
	}
	return NULL;
}
UT_TEST(test_managed_census_exported_scan_storage_cannot_be_output)
{
	for (unsigned mode = 0; mode < 4; mode++) {
		ClusterWalInputsV1 *inputs;
		char path[MAXPGPATH];
		int fd;
		ClusterRecoveryCensus *census = managed_census_fixture(&inputs, &fd, path);
		ClusterRecoveryRelmapScan *scan;
		UT_ASSERT(managed_census_bind(census));
		scan = managed_census_until_borrow(census);
		UT_ASSERT(scan != NULL);
		if (scan == NULL)
			return;
		if (mode == 0)
			UT_ASSERT_EQ(cluster_recovery_census_next(census, (void *)&scan->managed->identity),
						 CLUSTER_RECOVERY_CENSUS_INVALID);
		if (mode == 1)
			UT_ASSERT(!cluster_recovery_census_manifest(census, (void *)&scan->managed->image));
		if (mode == 2)
			UT_ASSERT(!cluster_recovery_census_wal_source_count_v1(census,
																   (void *)&scan->managed->native));
		if (mode == 3)
			UT_ASSERT(
				!cluster_recovery_census_page_wal_at_v1(census, 0, (void *)&scan->managed->native));
		UT_ASSERT_EQ(managed_census_shared.borrowed, 0);
		UT_ASSERT(inputs_pin_current && fcntl(fd, F_GETFD) >= 0);
		managed_census_end(census, &inputs, fd, path);
	}
}
UT_TEST(test_managed_census_failed_release_keeps_original_borrow_without_retry)
{
	ClusterWalInputsV1 *inputs;
	char path[MAXPGPATH];
	int fd;
	ClusterRecoveryCensus *census = managed_census_fixture(&inputs, &fd, path);
	ClusterRecoveryRelmapScan *scan;
	volatile bool caught = false;
	UT_ASSERT(managed_census_bind(census));
	scan = managed_census_until_borrow(census);
	UT_ASSERT(scan != NULL);
	if (scan == NULL)
		return;
	scan->managed->native.private_pid++;
	test_capture_error_level = true;
	PG_TRY();
	{
		ClusterRecoveryCensusEntry out;
		(void)cluster_recovery_census_next(census, &out);
	}
	PG_CATCH();
	{
		caught = true;
	}
	PG_END_TRY();
	UT_ASSERT(caught && scan->managed->release_attempted);
	UT_ASSERT_EQ(test_last_error_level, PANIC);
	UT_ASSERT_EQ(managed_census_shared.borrowed, 1);
	UT_ASSERT(census->relmap == scan && inputs_pin_current && fcntl(fd, F_GETFD) >= 0);
	caught = false;
	PG_TRY();
	{
		managed_census_scan_destroy(scan);
	}
	PG_CATCH();
	{
		caught = true;
	}
	PG_END_TRY();
	UT_ASSERT(caught && scan->managed->release_attempted);
	UT_ASSERT_EQ(managed_census_shared.borrowed, 1);
	test_capture_error_level = false;
	/* Test-only debt teardown, outside either refused production cleanup. */
	scan->managed->native.private_pid = MyProcPid;
	UT_ASSERT(managed_table_file_release_v2(&scan->managed->native));
	managed_census_end(census, &inputs, fd, path);
}
