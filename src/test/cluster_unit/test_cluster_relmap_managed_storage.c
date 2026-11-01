/*-------------------------------------------------------------------------
 * test_cluster_relmap_managed_storage.c
 *    Original authority path and MappingLock gates for registered files.
 *
 * Portions Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 * IDENTIFICATION
 *    src/test/cluster_unit/test_cluster_relmap_managed_storage.c
 * NOTES
 *    Reuses the original storage fixture without executing its old test main.
 *-------------------------------------------------------------------------
 */
int original_relmap_storage_main(void);
#define main original_relmap_storage_main
#include "test_cluster_relmap_v2_storage.c"
#undef main

static ClusterRecoveryFileIdentityV2
registered_identity(void)
{
	ClusterRecoveryFileIdentityV2 id = {0};
	id.kind = CLUSTER_RECOVERY_FILE_RELMAP_V2;
	id.format_id = CLUSTER_RECOVERY_FORMAT_ID_V2;
	id.format_version = CLUSTER_RECOVERY_FORMAT_VERSION_V2;
	id.system_identifier = identity.system_identifier;
	id.database_incarnation = identity.database_incarnation;
	id.storage_uuid[0] = 1;
	id.redo_branch_uuid[0] = 2;
	id.redo_branch_generation = 3;
	id.redo_timeline = 4;
	id.tablespace_oid = GLOBALTABLESPACE_OID;
	id.creation_generation = 103;
	id.registration_generation = 19;
	return id;
}

static void
managed_setup(ClusterRecoveryFileIdentityV2 *id)
{
	ClusterRelmapV2Image image = {0};
	unsigned char bytes[CLUSTER_RELMAP_FILE_SIZE_V2];
	setup(&identity);
	*id = registered_identity();
	image.identity = identity;
	image.committed_generation = 7;
	image.committed = map_image(40000);
	UT_ASSERT(cluster_relmap_file_encode_v2(id, &image, bytes, sizeof(bytes)));
	write_copy(0, bytes, sizeof(bytes));
	write_copy(1, bytes, sizeof(bytes));
	reset_io();
}

UT_TEST(original_path_reads_and_syncs_complete_registered_pair)
{
	ClusterRecoveryFileIdentityV2 id;
	ClusterRelmapFilePairV2 pair;
	managed_setup(&id);
	UT_ASSERT(!cluster_relmap_authority_managed_read_v2(&id, &pair));
	mapping_held = true;
	UT_ASSERT(cluster_relmap_authority_managed_read_v2(&id, &pair));
	UT_ASSERT_EQ(pair.view.committed.mappings[0].mapfilenumber, 40000);
	UT_ASSERT(cluster_relmap_authority_managed_checkpoint_v2(&id, &pair));
	UT_ASSERT_EQ(file_syncs, 2);
	UT_ASSERT_EQ(dir_syncs, 1);
	UT_ASSERT_EQ(io_under_retention_lock, 0);
	cleanup();
}

static void
drop_mapping_lock(bool is_directory)
{
	if (is_directory)
		mapping_held = false;
}

UT_TEST(checkpoint_late_lock_or_close_failure_keeps_output_private)
{
	ClusterRecoveryFileIdentityV2 id;
	ClusterRelmapFilePairV2 pair, saved;
	managed_setup(&id);
	mapping_held = true;
	memset(&pair, 0xa5, sizeof(pair));
	saved = pair;
	post_sync_hook = drop_mapping_lock;
	UT_ASSERT(!cluster_relmap_authority_managed_checkpoint_v2(&id, &pair));
	UT_ASSERT(memcmp(&pair, &saved, sizeof(pair)) == 0);
	post_sync_hook = NULL;
	mapping_held = true;
	fail_directory_close = true;
	UT_ASSERT(!cluster_relmap_authority_managed_read_v2(&id, &pair));
	UT_ASSERT(memcmp(&pair, &saved, sizeof(pair)) == 0);
	cleanup();
}

UT_TEST(old_format_and_wrong_registration_never_fall_back)
{
	ClusterRecoveryFileIdentityV2 id;
	ClusterRelmapFilePairV2 pair;
	setup(&identity);
	id = registered_identity();
	mapping_held = true;
	UT_ASSERT(!cluster_relmap_authority_managed_read_v2(&id, &pair));
	managed_setup(&id);
	mapping_held = true;
	++id.creation_generation;
	UT_ASSERT(!cluster_relmap_authority_managed_read_v2(&id, &pair));
	--id.creation_generation;
	enableFsync = false;
	UT_ASSERT(!cluster_relmap_authority_managed_checkpoint_v2(&id, &pair));
	cleanup();
}

UT_TEST(outer_facade_rejects_identity_output_alias_before_io)
{
	ClusterRecoveryFileIdentityV2 id;
	union {
		ClusterRelmapFilePairV2 pair;
		ClusterRecoveryFileIdentityV2 identity;
	} alias;
	managed_setup(&id);
	mapping_held = true;
	memset(&alias, 0, sizeof(alias));
	alias.identity = id;
	UT_ASSERT(!cluster_relmap_authority_managed_read_v2(&alias.identity, &alias.pair));
	UT_ASSERT(!cluster_relmap_authority_managed_checkpoint_v2(&alias.identity, &alias.pair));
	UT_ASSERT_EQ(alias.identity.creation_generation, id.creation_generation);
	UT_ASSERT_EQ(file_syncs, 0);
	UT_ASSERT_EQ(dir_syncs, 0);
	cleanup();
}

int
main(void)
{
	UT_PLAN(4);
	UT_RUN(original_path_reads_and_syncs_complete_registered_pair);
	UT_RUN(checkpoint_late_lock_or_close_failure_keeps_output_private);
	UT_RUN(old_format_and_wrong_registration_never_fall_back);
	UT_RUN(outer_facade_rejects_identity_output_alias_before_io);
	UT_DONE();
	return ut_failed_count ? 1 : 0;
}
