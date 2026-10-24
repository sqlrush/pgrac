/* Physical maps under the original cold-input observation owner.
 * Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com> */
static void
owned_map_write(int fd, const char *path, unsigned rel, bool drift)
{
	char bytes[TEST_RELMAP_LENGTH];
	RelMapFile map;
	int file;
	test_relmap_payload(bytes, false, 1);
	memcpy(&map, bytes + MinSizeOfRelmapUpdate, sizeof(map));
	map.mappings[0].mapfilenumber = rel;
	if (drift)
		map.mappings[63].mapoid ^= 1;
	INIT_CRC32C(map.crc);
	COMP_CRC32C(map.crc, &map, offsetof(RelMapFile, crc));
	FIN_CRC32C(map.crc);
	file = openat(fd, path, O_CREAT | O_TRUNC | O_WRONLY, 0600);
	UT_ASSERT(file >= 0);
	UT_ASSERT_EQ(write(file, &map, sizeof(map)), sizeof(map));
	UT_ASSERT_EQ(close(file), 0);
}

UT_TEST(test_owned_census_maps_keep_all_copy_edges)
{
	ClusterWalInputsV1 *inputs;
	ClusterRecoveryCensus *census;
	ClusterRecoveryCensusEntry out;
	ClusterRecoveryCensusDiag diag;
	ClusterRecoveryCensusResult result = CLUSTER_RECOVERY_CENSUS_MORE;
	char path[MAXPGPATH];
	int fd;
	unsigned images = 0, mappings = 0, targets = 0, missing = 0;
	census = owned_census_fixture(&inputs, &fd, path, false);
	owned_map_write(fd, "base/5/pg_filenode.map", 16385, false);
	owned_map_write(fd, "base/5/pg_filenode.map.bak", 20000, false);
	for (unsigned i = 0; i < 200000; ++i) {
		result = cluster_recovery_census_next(census, &out);
		if (result >= CLUSTER_RECOVERY_CENSUS_DRAINED)
			break;
		if (result != CLUSTER_RECOVERY_CENSUS_ENTRY || out.kind != CLUSTER_RECOVERY_CENSUS_RELMAP)
			continue;
		UT_ASSERT_EQ(out.value.relmap.key.system_identifier,
					 cluster_wal_inputs_at_v1(inputs, 0)->source.claim.identity.system_identifier);
		if (out.value.relmap.kind == CLUSTER_RECOVERY_RELMAP_IMAGE)
			images++;
		if (out.value.relmap.kind == CLUSTER_RECOVERY_RELMAP_MAPPING)
			mappings++;
		if (out.value.relmap.kind == CLUSTER_RECOVERY_RELMAP_TARGET) {
			targets++;
			missing += !out.value.relmap.physical;
		}
	}
	UT_ASSERT_EQ(result, CLUSTER_RECOVERY_CENSUS_DRAINED);
	UT_ASSERT_EQ(images, 2);
	UT_ASSERT_EQ(mappings, 2);
	UT_ASSERT_EQ(targets, 8);
	UT_ASSERT_EQ(missing, 6);
	UT_ASSERT(cluster_recovery_census_diag(census, &diag));
	UT_ASSERT_EQ(diag.relmap_entries, 12);
	UT_ASSERT_EQ(diag.phase, CLUSTER_RECOVERY_CENSUS_DONE);
	UT_ASSERT_EQ(unlinkat(fd, "base/5/pg_filenode.map", 0), 0);
	UT_ASSERT_EQ(unlinkat(fd, "base/5/pg_filenode.map.bak", 0), 0);
	owned_census_done(census, &inputs, fd, path);
}

UT_TEST(test_owned_census_maps_final_root_and_wal_rechecks)
{
	for (unsigned mode = 0; mode < 3; ++mode) {
		ClusterWalInputsV1 *inputs;
		ClusterRecoveryCensus *census;
		ClusterRecoveryCensusEntry out, zero = { 0 };
		ClusterRecoveryCensusDiag diag;
		char path[MAXPGPATH];
		int fd;
		census = owned_census_fixture(&inputs, &fd, path, false);
		owned_map_write(fd, "base/5/pg_filenode.map", 16385, false);
		UT_ASSERT_EQ(owned_census_until(census, CLUSTER_RECOVERY_CENSUS_FINAL_TT),
					 CLUSTER_RECOVERY_CENSUS_MORE);
		UT_ASSERT(cluster_recovery_census_diag(census, &diag));
		UT_ASSERT_EQ(diag.relmap_entries, 6);
		if (mode == 0)
			owned_map_write(fd, "base/5/pg_filenode.map", 16385, true);
		else if (mode == 1) {
			inventory_fault = 0;
			inventory_drift();
		} else
			inputs_pin_current = false;
		UT_ASSERT_EQ(owned_census_until(census, 0), CLUSTER_RECOVERY_CENSUS_REFUSED);
		memset(&out, 0xa5, sizeof(out));
		UT_ASSERT_EQ(cluster_recovery_census_next(census, &out), CLUSTER_RECOVERY_CENSUS_REFUSED);
		UT_ASSERT_EQ(memcmp(&out, &zero, sizeof(out)), 0);
		UT_ASSERT_EQ(unlinkat(fd, "base/5/pg_filenode.map", 0), 0);
		owned_census_done(census, &inputs, fd, path);
	}
}

UT_TEST(test_owned_census_bad_map_revokes_all_children)
{
	ClusterWalInputsV1 *inputs;
	ClusterRecoveryCensus *census;
	ClusterRecoveryCensusDiag diag;
	char path[MAXPGPATH];
	int fd, file;
	census = owned_census_fixture(&inputs, &fd, path, false);
	file = openat(fd, "base/5/pg_filenode.map", O_CREAT | O_WRONLY, 0600);
	UT_ASSERT(file >= 0);
	UT_ASSERT_EQ(write(file, "short", 5), 5);
	UT_ASSERT_EQ(close(file), 0);
	UT_ASSERT_EQ(owned_census_until(census, 0), CLUSTER_RECOVERY_CENSUS_REFUSED);
	UT_ASSERT(cluster_recovery_census_diag(census, &diag));
	UT_ASSERT_EQ(diag.phase, CLUSTER_RECOVERY_CENSUS_RELMAP_PHYSICAL);
	UT_ASSERT_EQ(diag.relmap_result, CLUSTER_RECOVERY_RELMAP_BASE_REQUIRED);
	UT_ASSERT(census->roots == NULL && census->inventory == NULL && census->graph == NULL);
	UT_ASSERT_EQ(unlinkat(fd, "base/5/pg_filenode.map", 0), 0);
	owned_census_done(census, &inputs, fd, path);
}
