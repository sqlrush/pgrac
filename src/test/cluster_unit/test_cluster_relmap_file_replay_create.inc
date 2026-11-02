/*-------------------------------------------------------------------------
 * test_cluster_relmap_file_replay_create.inc
 *    Exact-image creation replay against real managed relmap files.
 * Portions Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 * IDENTIFICATION
 *    src/test/cluster_unit/test_cluster_relmap_file_replay_create.inc
 * NOTES
 *    Spec: 2026-11-01-pre2-e1-format-closure-v1.2-path-c.md
 *-------------------------------------------------------------------------
 */
UT_TEST(replay_missing_pair_creates_exact_generation_one)
{
	ClusterRelmapFilePairV2 pair;
	prepare();
	UT_ASSERT(cluster_relmap_file_pair_replay_create_v2(directory, &identity,
		&image.committed, &pair));
	UT_ASSERT_EQ(syncs, 3);
	UT_ASSERT_EQ(pair.view.committed_generation, 1);
	UT_ASSERT(!pair.view.has_pending && pair.view.pending_complete);
	UT_ASSERT_EQ(pair.view.committed.mappings[0].mapfilenumber, 9001);
	UT_ASSERT(!cluster_relmap_file_pair_create_v2(directory, &identity,
		&image.committed, &pair));
	cleanup();
}

UT_TEST(replay_exact_pair_only_syncs_and_preserves_both_inodes)
{
	ClusterRelmapFilePairV2 pair, first;
	struct stat before[2], after;
	const char *names[] = {"pgrac_relmap_authority", "pgrac_relmap_authority.bak"};
	prepare();
	create_pair();
	UT_ASSERT(cluster_relmap_file_pair_read_v2(directory, &identity, &first));
	for (unsigned i = 0; i < 2; ++i)
		UT_ASSERT(fstatat(directory, names[i], &before[i], AT_SYMLINK_NOFOLLOW) == 0);
	syncs = 0;
	UT_ASSERT(cluster_relmap_file_pair_replay_create_v2(directory, &identity,
		&image.committed, &pair));
	UT_ASSERT_EQ(syncs, 3);
	UT_ASSERT(memcmp(pair.bytes, first.bytes, sizeof(pair.bytes)) == 0);
	for (unsigned i = 0; i < 2; ++i) {
		UT_ASSERT(fstatat(directory, names[i], &after, AT_SYMLINK_NOFOLLOW) == 0);
		UT_ASSERT(before[i].st_dev == after.st_dev && before[i].st_ino == after.st_ino
			&& before[i].st_size == after.st_size && before[i].st_mtime == after.st_mtime
			&& before[i].st_ctime == after.st_ctime);
	}
	cleanup();
}

UT_TEST(replay_partial_pairs_keep_the_missing_name_and_original_output)
{
	const char *names[] = {"pgrac_relmap_authority", "pgrac_relmap_authority.bak"};
	for (unsigned missing = 0; missing < 2; ++missing) {
		ClusterRelmapFilePairV2 pair, saved;
		struct stat before, after;
		prepare();
		create_pair();
		UT_ASSERT(unlinkat(directory, names[missing], 0) == 0);
		UT_ASSERT(fstatat(directory, names[1-missing], &before, AT_SYMLINK_NOFOLLOW) == 0);
		memset(&pair, 0xa5, sizeof(pair)); saved = pair; syncs = 0;
		UT_ASSERT(!cluster_relmap_file_pair_replay_create_v2(directory, &identity,
			&image.committed, &pair));
		UT_ASSERT(syncs == 0 && memcmp(&pair, &saved, sizeof(pair)) == 0);
		UT_ASSERT(fstatat(directory, names[missing], &after, AT_SYMLINK_NOFOLLOW) < 0
			&& errno == ENOENT);
		UT_ASSERT(fstatat(directory, names[1-missing], &after, AT_SYMLINK_NOFOLLOW) == 0);
		UT_ASSERT(before.st_ino == after.st_ino && before.st_size == after.st_size);
		cleanup();
	}
}

UT_TEST(replay_changed_birth_map_generation_or_pending_never_rewrites)
{
	for (unsigned change = 0; change < 4; ++change) {
		ClusterRelmapFilePairV2 pair, saved;
		ClusterRecoveryFileIdentityV2 expected;
		RelMapFile requested;
		unsigned char bytes[CLUSTER_RELMAP_FILE_SIZE_V2], actual[CLUSTER_RELMAP_FILE_SIZE_V2];
		int fd;
		prepare();
		expected = identity; requested = image.committed;
		image.committed_generation = 1;
		if (change == 0) ++expected.creation_generation;
		if (change == 1) {
			image.committed.mappings[0].mapfilenumber = 9002;
			INIT_CRC32C(image.committed.crc);
			COMP_CRC32C(image.committed.crc, &image.committed, offsetof(RelMapFile, crc));
			FIN_CRC32C(image.committed.crc);
		}
		if (change == 2) image.committed_generation = 2;
		if (change == 3) {
			image.pending_generation = 2;
			image.pending_owner = (ClusterRelmapV2Owner){0,17,18,19,20,21};
			image.pending = image.committed;
		}
		UT_ASSERT(cluster_relmap_file_encode_v2(&identity, &image, bytes, sizeof(bytes)));
		for (unsigned i = 0; i < 2; ++i) {
			fd = openat(directory, managed_names[i], O_WRONLY|O_CREAT|O_EXCL, 0600);
			UT_ASSERT(fd >= 0 && write(fd, bytes, sizeof(bytes)) == sizeof(bytes) && close(fd) == 0);
		}
		memset(&pair, 0xa5, sizeof(pair)); saved = pair;
		UT_ASSERT(!cluster_relmap_file_pair_replay_create_v2(directory, &expected, &requested, &pair));
		UT_ASSERT(memcmp(&pair, &saved, sizeof(pair)) == 0);
		for (unsigned i = 0; i < 2; ++i) {
			fd = openat(directory, managed_names[i], O_RDONLY);
			UT_ASSERT(fd >= 0 && read(fd, actual, sizeof(actual)) == sizeof(actual) && close(fd) == 0);
			UT_ASSERT(memcmp(actual, bytes, sizeof(bytes)) == 0);
		}
		cleanup();
	}
}

UT_TEST(replay_sync_late_corruption_and_close_failure_keep_output)
{
	ClusterRelmapFilePairV2 pair, saved;
	prepare(); create_pair();
	memset(&pair, 0xa5, sizeof(pair)); saved = pair;
	for (unsigned fail = 1; fail <= 3; ++fail) {
		syncs = 0; fail_sync = fail;
		UT_ASSERT(!cluster_relmap_file_pair_replay_create_v2(directory, &identity, &image.committed, &pair));
		UT_ASSERT(memcmp(&pair, &saved, sizeof(pair)) == 0);
	}
	fail_sync = 0;
	for (unsigned fail = 1; fail <= 2; ++fail) {
		closes = 0; fail_close = fail;
		UT_ASSERT(!cluster_relmap_file_pair_replay_create_v2(directory, &identity, &image.committed, &pair));
		UT_ASSERT(memcmp(&pair, &saved, sizeof(pair)) == 0);
	}
	fail_close = 0; syncs = 0; corrupt_on_sync = true;
	UT_ASSERT(!cluster_relmap_file_pair_replay_create_v2(directory, &identity, &image.committed, &pair));
	UT_ASSERT(memcmp(&pair, &saved, sizeof(pair)) == 0);
	cleanup();
}

UT_TEST(replay_alias_invalid_image_and_linked_names_refuse)
{
	union { ClusterRelmapFilePairV2 pair; ClusterRecoveryFileIdentityV2 id; RelMapFile map; } alias;
	ClusterRelmapFilePairV2 pair;
	struct stat st;
	prepare();
	memset(&alias, 0xa5, sizeof(alias)); alias.id = identity;
	UT_ASSERT(!cluster_relmap_file_pair_replay_create_v2(directory, &alias.id, &image.committed, &alias.pair));
	alias.map = image.committed;
	UT_ASSERT(!cluster_relmap_file_pair_replay_create_v2(directory, &identity, &alias.map, &alias.pair));
	image.committed.magic = 0;
	UT_ASSERT(!cluster_relmap_file_pair_replay_create_v2(directory, &identity, &image.committed, &pair));
	UT_ASSERT(syncs == 0 && fstatat(directory, managed_names[0], &st, AT_SYMLINK_NOFOLLOW) < 0 && errno == ENOENT);
	cleanup();
	for (unsigned hard = 0; hard < 2; ++hard) {
		prepare(); create_pair();
		UT_ASSERT(unlinkat(directory, managed_names[1], 0) == 0);
		UT_ASSERT((hard ? linkat(directory, managed_names[0], directory, managed_names[1], 0)
			: symlinkat(managed_names[0], directory, managed_names[1])) == 0);
		UT_ASSERT(!cluster_relmap_file_pair_replay_create_v2(directory, &identity, &image.committed, &pair));
		UT_ASSERT(fstatat(directory, managed_names[1], &st, AT_SYMLINK_NOFOLLOW) == 0);
		UT_ASSERT(hard ? st.st_nlink == 2 : S_ISLNK(st.st_mode));
		cleanup();
	}
}

UT_TEST(replay_late_input_drift_cannot_publish_saved_output)
{
	for (unsigned existing = 0; existing < 2; ++existing)
		for (unsigned field = 0; field < 2; ++field) {
			ClusterRelmapFilePairV2 pair, saved;
			prepare();
			if (existing) create_pair();
			syncs = 0;
			memset(&pair, 0xa5, sizeof(pair)); saved = pair;
			if (field) drift_map_on_sync = &image.committed;
			else drift_identity_on_sync = &identity;
			UT_ASSERT(!cluster_relmap_file_pair_replay_create_v2(directory, &identity,
				&image.committed, &pair));
			UT_ASSERT(memcmp(&pair, &saved, sizeof(pair)) == 0);
			cleanup();
		}
}
