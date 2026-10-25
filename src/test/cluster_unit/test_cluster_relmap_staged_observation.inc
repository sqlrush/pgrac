/* Current exact stage observation, using the original real filesystem fixture.
 * This read-only result cannot certify durability or rehabilitate failed stage.
 * Author: SqlRush <sqlrush@gmail.com>
 */
static ClusterRelmapV2Image
staged_observation_fixture(const ClusterRelmapV2Namespace *ns)
{
	ClusterRelmapV2Image image = { 0 };
	unsigned char bytes[CLUSTER_RELMAP_V2_FILE_SIZE];

	setup(ns);
	UT_ASSERT(stage(ns));
	read_copy(0, bytes);
	UT_ASSERT_EQ(cluster_relmap_v2_decode(bytes, sizeof(bytes), ns, &image), CLUSTER_RELMAP_V2_VALID);
	reset_io();
	relmap_x_held = true;
	return image;
}

UT_TEST(test_staged_observation_reads_both_exact_copies_without_mutation)
{
	for (unsigned which = 0; which < 2; which++)
	{
		ClusterRelmapV2Namespace ns = identity;
		ClusterRelmapV2Image image;
		ClusterRelmapRetentionDirectory before;
		unsigned char a[CLUSTER_RELMAP_V2_FILE_SIZE], b[CLUSTER_RELMAP_V2_FILE_SIZE];

		if (which) { ns.shared = false; ns.dbid = 42; }
		image = staged_observation_fixture(&ns);
		before = writer_retention;
		read_copy(0, a); read_copy(1, b);
		UT_ASSERT(cluster_relmap_authority_v2_check_staged(&image));
		UT_ASSERT(reads >= 2);
		UT_ASSERT_EQ(writes + renames + file_syncs + dir_syncs, 0);
		UT_ASSERT_EQ(io_under_retention_lock, 0);
		UT_ASSERT(memcmp(&writer_retention, &before, sizeof(before)) == 0);
		read_copy(0, b); UT_ASSERT(memcmp(a, b, sizeof(a)) == 0);
		read_copy(1, b); UT_ASSERT(memcmp(a, b, sizeof(a)) == 0);
	}
}

UT_TEST(test_staged_observation_rejects_legal_union_cuts_and_changed_bytes)
{
	for (unsigned which = 0; which < 10; which++)
	{
		ClusterRelmapV2Image image = staged_observation_fixture(&identity);
		ClusterRelmapV2Image other = image;
		ClusterRelmapV2View view;
		ClusterRelmapRetentionDirectory before = writer_retention;
		unsigned char bytes[CLUSTER_RELMAP_V2_FILE_SIZE];
		int copy = which & 1;
		char path[MAXPGPATH];

		if (which < 6)
		{
			if (which == 2 || which == 3 || which == 5)
			{
				other.committed = other.pending;
				other.committed_generation = other.pending_generation;
			}
			other.pending_generation = 0;
			memset(&other.pending_owner, 0, sizeof(other.pending_owner));
			memset(&other.pending, 0, sizeof(other.pending));
		}
		if (which == 6) other.pending = map_image(40002);
		if (which == 7) other.pending_owner.boot_incarnation++;
		if (which < 8)
		{
			UT_ASSERT_EQ(cluster_relmap_v2_encode(&other, bytes, sizeof(bytes)), CLUSTER_RELMAP_V2_VALID);
			write_copy(copy, bytes, sizeof(bytes));
			if (which >= 4) write_copy(1 - copy, bytes, sizeof(bytes));
		}
		else
		{
			snprintf(path, sizeof(path), "%s/%s", directory, names[1]);
			if (which == 8) UT_ASSERT_EQ(unlink(path), 0);
			else write_copy(1, (const unsigned char *) "short", 5);
		}
		reset_io(); relmap_x_held = true;
		if (which < 3)
		{
			UT_ASSERT_EQ(cluster_relmap_authority_v2_read(&identity, &view), CLUSTER_RELMAP_V2_PAIR_VALID);
			UT_ASSERT(view.pending_complete && view.has_pending);
		}
		UT_ASSERT(!cluster_relmap_authority_v2_check_staged(&image));
		UT_ASSERT_EQ(writes + renames + file_syncs + dir_syncs, 0);
		UT_ASSERT(memcmp(&writer_retention, &before, sizeof(before)) == 0);
	}
}

UT_TEST(test_staged_observation_refuses_original_lock_path_and_close_failures)
{
	for (unsigned which = 0; which < 7; which++)
	{
		ClusterRelmapV2Image image = staged_observation_fixture(&identity);
		ClusterRelmapRetentionDirectory before = writer_retention;
		char path[MAXPGPATH];

		if (which == 0) enableFsync = false;
		if (which == 1) relmap_x_held = false;
		if (which == 2) drop_relmap_x_on_read = true;
		if (which == 3) replace_during_read = true;
		if (which == 4) fail_directory_close = true;
		if (which == 5)
		{
			snprintf(path, sizeof(path), "%s/%s", directory, names[1]);
			UT_ASSERT_EQ(unlink(path), 0);
			UT_ASSERT_EQ(symlink(names[0], path), 0);
		}
		UT_ASSERT(!cluster_relmap_authority_v2_check_staged(which == 6 ? NULL : &image));
		if (which < 2 || which == 6) UT_ASSERT_EQ(reads, 0);
		UT_ASSERT_EQ(writes + renames + file_syncs + dir_syncs, 0);
		UT_ASSERT_EQ(io_under_retention_lock, 0);
		UT_ASSERT(memcmp(&writer_retention, &before, sizeof(before)) == 0);
	}
}
