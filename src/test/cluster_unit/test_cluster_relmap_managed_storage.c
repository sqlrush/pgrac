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


static bool
managed_stage(const ClusterRecoveryFileIdentityV2 *id)
{
	RelMapFile pending = map_image(40001);
	bool ok;
	observing_stage = true;
	ok = cluster_relmap_authority_managed_write_pending_v2(
		id, 8, &owner, &pending, &writer_retention, &retention_lock);
	observing_stage = false;
	return ok;
}

static ClusterRelmapV2Image
managed_staged(void)
{
	ClusterRelmapV2Image image = {0};
	image.identity = identity;
	image.committed_generation = 7;
	image.committed = map_image(40000);
	image.pending_generation = 8;
	image.pending_owner = owner;
	image.pending = map_image(40001);
	return image;
}

static void
managed_locks(void)
{
	relmap_x_held = mapping_held = true;
}

UT_TEST(managed_stage_publish_preserves_committed_until_finish_and_keeps_pin)
{
	ClusterRecoveryFileIdentityV2 id;
	ClusterRelmapFilePairV2 pair;
	ClusterRelmapV2Image staged = managed_staged();
	managed_setup(&id);
	managed_locks();
	UT_ASSERT(managed_stage(&id));
	UT_ASSERT(cluster_relmap_authority_managed_read_v2(&id, &pair));
	UT_ASSERT_EQ(pair.view.committed_generation, 7);
	UT_ASSERT_EQ(pair.view.committed.mappings[0].mapfilenumber, 40000);
	UT_ASSERT(pair.view.has_pending);
	UT_ASSERT_EQ(file_syncs, 2);
	UT_ASSERT_EQ(dir_syncs, 2);
	UT_ASSERT_EQ(writes_without_pin, 0);
	UT_ASSERT_EQ(io_under_retention_lock, 0);
	UT_ASSERT_EQ(cluster_relmap_authority_managed_publish_v2(&id, &staged),
		CLUSTER_RELMAP_V2_CLEARED_POSTREAD);
	UT_ASSERT(cluster_relmap_authority_managed_read_v2(&id, &pair));
	UT_ASSERT_EQ(pair.view.committed_generation, 8);
	UT_ASSERT_EQ(pair.view.committed.mappings[0].mapfilenumber, 40001);
	UT_ASSERT(!pair.view.has_pending);
	UT_ASSERT(writer_retention.pins[0].occupied && writer_retention.pins[0].may_be_staged);
	cleanup();
}

UT_TEST(managed_discard_keeps_original_map_and_requires_fresh_sync_on_clean_call)
{
	ClusterRecoveryFileIdentityV2 id;
	ClusterRelmapFilePairV2 pair;
	ClusterRelmapV2Image staged = managed_staged();
	managed_setup(&id);
	managed_locks();
	UT_ASSERT(managed_stage(&id));
	UT_ASSERT_EQ(cluster_relmap_authority_managed_discard_pending_v2(&id, &staged),
		CLUSTER_RELMAP_V2_CLEARED_POSTREAD);
	UT_ASSERT(cluster_relmap_authority_managed_read_v2(&id, &pair));
	UT_ASSERT_EQ(pair.view.committed_generation, 7);
	UT_ASSERT_EQ(pair.view.committed.mappings[0].mapfilenumber, 40000);
	UT_ASSERT(!pair.view.has_pending);
	reset_io(); managed_locks();
	UT_ASSERT_EQ(cluster_relmap_authority_managed_discard_pending_v2(&id, &staged),
		CLUSTER_RELMAP_V2_CLEARED_POSTREAD);
	UT_ASSERT_EQ(file_syncs, 2);
	UT_ASSERT_EQ(dir_syncs, 2);
	UT_ASSERT(writer_retention.pins[0].may_be_staged);
	cleanup();
}

UT_TEST(managed_mutations_require_both_original_locks_and_real_pin)
{
	ClusterRecoveryFileIdentityV2 id;
	ClusterRelmapV2Image staged = managed_staged();
	managed_setup(&id);
	mapping_held = true;
	UT_ASSERT(!managed_stage(&id));
	relmap_x_held = true; mapping_held = false;
	UT_ASSERT(!managed_stage(&id));
	UT_ASSERT_EQ(cluster_relmap_authority_managed_publish_v2(&id, &staged),
		CLUSTER_RELMAP_V2_RETAINED_UNKNOWN);
	managed_locks();
	memset(&writer_retention, 0, sizeof(writer_retention));
	UT_ASSERT(!managed_stage(&id));
	UT_ASSERT_EQ(writes, 0);
	cleanup();
}

UT_TEST(managed_identity_and_exact_original_stage_refuse_before_mutation)
{
	ClusterRecoveryFileIdentityV2 id, wrong;
	ClusterRelmapV2Image staged = managed_staged();
	managed_setup(&id); managed_locks(); wrong = id;
	++wrong.registration_generation;
	UT_ASSERT(!managed_stage(&wrong));
	UT_ASSERT_EQ(writes, 0);
	UT_ASSERT(!writer_retention.pins[0].may_be_staged);
	UT_ASSERT(managed_stage(&id));
	reset_io(); managed_locks();
	staged.pending = map_image(40002);
	UT_ASSERT_EQ(cluster_relmap_authority_managed_publish_v2(&id, &staged),
		CLUSTER_RELMAP_V2_RETAINED_UNKNOWN);
	UT_ASSERT_EQ(cluster_relmap_authority_managed_discard_pending_v2(&wrong, &staged),
		CLUSTER_RELMAP_V2_RETAINED_UNKNOWN);
	UT_ASSERT_EQ(writes, 0);
	cleanup();
}

static void
managed_fault(int fault)
{
	switch (fault) {
		case 0: fail_write = 2; break;
		case 1: fail_file_sync = 2; break;
		case 2: fail_rename = 2; break;
		case 3: fail_dir_sync = 2; break;
		case 4: fail_directory_close = true; break;
		case 5: corrupt_after_rename = true; fault_rename = 2; break;
		case 6: replace_after_rename = true; fault_rename = 2; break;
	}
}

UT_TEST(managed_partial_stage_failures_keep_original_retention_debt)
{
	ClusterRecoveryFileIdentityV2 id;
	for (int fault = 0; fault < 7; ++fault) {
		managed_setup(&id); managed_locks(); managed_fault(fault);
		UT_ASSERT(!managed_stage(&id));
		UT_ASSERT(writer_retention.pins[0].occupied && writer_retention.pins[0].may_be_staged);
		UT_ASSERT_EQ(writes_without_pin, 0);
		UT_ASSERT_EQ(io_under_retention_lock, 0);
		cleanup();
	}
}

static void
drop_mapping_after_file_sync(bool is_directory)
{
	if (!is_directory) mapping_held = false;
}

static ClusterRecoveryFileIdentityV2 *changing_registration;
static void
change_registration_after_file_sync(bool is_directory)
{
	if (!is_directory) ++changing_registration->creation_generation;
}

UT_TEST(managed_stage_late_lock_or_registration_change_stops_before_rename)
{
	ClusterRecoveryFileIdentityV2 id;
	managed_setup(&id); managed_locks();
	drop_relmap_x_on_read = true;
	UT_ASSERT(!managed_stage(&id));
	UT_ASSERT_EQ(writes, 0);
	managed_setup(&id); managed_locks();
	post_sync_hook = drop_mapping_after_file_sync;
	UT_ASSERT(!managed_stage(&id));
	UT_ASSERT_EQ(renames, 0);
	UT_ASSERT(writer_retention.pins[0].may_be_staged);
	managed_setup(&id); managed_locks();
	changing_registration = &id;
	post_sync_hook = change_registration_after_file_sync;
	UT_ASSERT(!managed_stage(&id));
	UT_ASSERT_EQ(renames, 0);
	UT_ASSERT(writer_retention.pins[0].may_be_staged);
	cleanup();
}

UT_TEST(managed_finish_failures_do_not_release_pin_or_claim_rollback)
{
	ClusterRecoveryFileIdentityV2 id;
	ClusterRelmapV2Image staged = managed_staged();
	for (int publish = 0; publish < 2; ++publish)
		for (int fault = 0; fault < 7; ++fault) {
			managed_setup(&id); managed_locks();
			UT_ASSERT(managed_stage(&id));
			reset_io(); managed_locks(); managed_fault(fault);
			UT_ASSERT_EQ(publish ? cluster_relmap_authority_managed_publish_v2(&id, &staged)
				: cluster_relmap_authority_managed_discard_pending_v2(&id, &staged),
				CLUSTER_RELMAP_V2_RETAINED_UNKNOWN);
			UT_ASSERT(writer_retention.pins[0].occupied && writer_retention.pins[0].may_be_staged);
			UT_ASSERT_EQ(io_under_retention_lock, 0);
			cleanup();
		}
}

UT_TEST(managed_finish_accepts_only_legal_exact_mixed_cuts)
{
	ClusterRecoveryFileIdentityV2 id;
	ClusterRelmapFilePairV2 pair;
	ClusterRelmapV2Image staged = managed_staged();
	for (int publish = 0; publish < 2; ++publish) {
		managed_setup(&id); managed_locks(); UT_ASSERT(managed_stage(&id));
		reset_io(); managed_locks(); fail_rename = 2;
		UT_ASSERT_EQ(publish ? cluster_relmap_authority_managed_publish_v2(&id, &staged)
			: cluster_relmap_authority_managed_discard_pending_v2(&id, &staged),
			CLUSTER_RELMAP_V2_RETAINED_UNKNOWN);
		reset_io(); managed_locks();
		UT_ASSERT_EQ(publish ? cluster_relmap_authority_managed_publish_v2(&id, &staged)
			: cluster_relmap_authority_managed_discard_pending_v2(&id, &staged),
			CLUSTER_RELMAP_V2_CLEARED_POSTREAD);
		UT_ASSERT(cluster_relmap_authority_managed_read_v2(&id, &pair));
		UT_ASSERT_EQ(pair.view.committed_generation, publish ? 8 : 7);
		UT_ASSERT_EQ(file_syncs, 2);
		cleanup();
	}
}

UT_TEST(managed_unsafe_modes_old_format_and_disabled_sync_never_write)
{
	ClusterRecoveryFileIdentityV2 id;
	char path[MAXPGPATH];
	setup(&identity); id = registered_identity(); managed_locks();
	UT_ASSERT(!managed_stage(&id));
	managed_setup(&id); managed_locks();
	snprintf(path, sizeof(path), "%s/%s", directory, names[0]);
	UT_ASSERT_EQ(chmod(path, 0666), 0);
	UT_ASSERT(!managed_stage(&id));
	UT_ASSERT_EQ(writes, 0);
	UT_ASSERT_EQ(chmod(path, 0600), 0);
	enableFsync = false;
	UT_ASSERT(!managed_stage(&id));
	UT_ASSERT_EQ(writes, 0);
	cleanup();
}

static int unsafe_sync_target;
static void
make_unsafe_after_file_sync(bool is_directory)
{
	char path[MAXPGPATH];
	if (is_directory) return;
	if (unsafe_sync_target == 2)
		snprintf(path, sizeof(path), "%s", directory);
	else
		snprintf(path, sizeof(path), "%s/%s", directory,
			names[unsafe_sync_target == 0 ? 0 : 2]);
	if (chmod(path, unsafe_sync_target == 2 ? 0777 : 0666) != 0) abort();
}

UT_TEST(managed_late_unsafe_file_temp_or_directory_refuses_before_rename)
{
	ClusterRecoveryFileIdentityV2 id;
	for (int target = 0; target < 3; ++target) {
		managed_setup(&id); managed_locks();
		unsafe_sync_target = target;
		post_sync_hook = make_unsafe_after_file_sync;
		UT_ASSERT(!managed_stage(&id));
		UT_ASSERT_EQ(renames, 0);
		UT_ASSERT(writer_retention.pins[0].may_be_staged);
		cleanup();
	}
}

UT_TEST(managed_local_database_uses_its_original_namespace_and_control_identity)
{
	const ClusterRelmapV2Namespace local = {12345, 67890, 42, false};
	ClusterRecoveryFileIdentityV2 id = registered_identity();
	ClusterRelmapV2Image image = {0};
	ClusterRelmapFilePairV2 pair;
	RelMapFile pending = map_image(40001);
	unsigned char bytes[CLUSTER_RELMAP_FILE_SIZE_V2];
	setup(&local);
	id.database_oid = 42;
	id.tablespace_oid = DEFAULTTABLESPACE_OID;
	image.identity = local;
	image.committed_generation = 7;
	image.committed = map_image(40000);
	UT_ASSERT(cluster_relmap_file_encode_v2(&id, &image, bytes, sizeof(bytes)));
	write_copy(0, bytes, sizeof(bytes)); write_copy(1, bytes, sizeof(bytes));
	reset_io(); managed_locks(); observing_stage = true;
	UT_ASSERT(cluster_relmap_authority_managed_write_pending_v2(
		&id, 8, &owner, &pending, &writer_retention, &retention_lock));
	observing_stage = false;
	image.pending_generation = 8; image.pending_owner = owner; image.pending = pending;
	UT_ASSERT_EQ(cluster_relmap_authority_managed_publish_v2(&id, &image),
		CLUSTER_RELMAP_V2_CLEARED_POSTREAD);
	UT_ASSERT(cluster_relmap_authority_managed_read_v2(&id, &pair));
	UT_ASSERT_EQ(pair.view.committed_generation, 8);
	UT_ASSERT_EQ(pair.view.committed.mappings[0].mapfilenumber, 40001);
	UT_ASSERT_EQ(writes_without_pin, 0);
	cleanup();
}

UT_TEST(managed_finish_lost_serialization_or_registration_keeps_debt)
{
	ClusterRecoveryFileIdentityV2 id;
	ClusterRelmapV2Image staged = managed_staged();
	for (int mode = 0; mode < 3; ++mode) {
		managed_setup(&id); managed_locks(); UT_ASSERT(managed_stage(&id));
		reset_io(); managed_locks();
		if (mode == 0) drop_relmap_x_on_read = true;
		else if (mode == 1) post_sync_hook = drop_mapping_after_file_sync;
		else { changing_registration = &id; post_sync_hook = change_registration_after_file_sync; }
		UT_ASSERT_EQ(cluster_relmap_authority_managed_publish_v2(&id, &staged),
			CLUSTER_RELMAP_V2_RETAINED_UNKNOWN);
		UT_ASSERT_EQ(renames, 0);
		UT_ASSERT(writer_retention.pins[0].may_be_staged);
		cleanup();
	}
}

UT_TEST(managed_missing_copy_and_unstaged_publish_never_repair_or_advance)
{
	ClusterRecoveryFileIdentityV2 id;
	ClusterRelmapV2Image staged = managed_staged();
	char path[MAXPGPATH];
	managed_setup(&id); managed_locks();
	UT_ASSERT_EQ(cluster_relmap_authority_managed_publish_v2(&id, &staged),
		CLUSTER_RELMAP_V2_RETAINED_UNKNOWN);
	snprintf(path, sizeof(path), "%s/%s", directory, names[1]);
	UT_ASSERT_EQ(unlink(path), 0);
	UT_ASSERT(!managed_stage(&id));
	UT_ASSERT_EQ(cluster_relmap_authority_managed_discard_pending_v2(&id, &staged),
		CLUSTER_RELMAP_V2_RETAINED_UNKNOWN);
	UT_ASSERT_EQ(writes, 0);
	UT_ASSERT(!writer_retention.pins[0].may_be_staged);
	cleanup();
}

int
main(void)
{
	UT_PLAN(17);
	UT_RUN(original_path_reads_and_syncs_complete_registered_pair);
	UT_RUN(checkpoint_late_lock_or_close_failure_keeps_output_private);
	UT_RUN(old_format_and_wrong_registration_never_fall_back);
	UT_RUN(outer_facade_rejects_identity_output_alias_before_io);
	UT_RUN(managed_stage_publish_preserves_committed_until_finish_and_keeps_pin);
	UT_RUN(managed_discard_keeps_original_map_and_requires_fresh_sync_on_clean_call);
	UT_RUN(managed_mutations_require_both_original_locks_and_real_pin);
	UT_RUN(managed_identity_and_exact_original_stage_refuse_before_mutation);
	UT_RUN(managed_partial_stage_failures_keep_original_retention_debt);
	UT_RUN(managed_stage_late_lock_or_registration_change_stops_before_rename);
	UT_RUN(managed_finish_failures_do_not_release_pin_or_claim_rollback);
	UT_RUN(managed_finish_accepts_only_legal_exact_mixed_cuts);
	UT_RUN(managed_unsafe_modes_old_format_and_disabled_sync_never_write);
	UT_RUN(managed_late_unsafe_file_temp_or_directory_refuses_before_rename);
	UT_RUN(managed_local_database_uses_its_original_namespace_and_control_identity);
	UT_RUN(managed_finish_lost_serialization_or_registration_keeps_debt);
	UT_RUN(managed_missing_copy_and_unstaged_publish_never_repair_or_advance);
	UT_DONE();
	return ut_failed_count ? 1 : 0;
}
