/* Real early-reader files and original capacity collector; no CF admission. */
#include "../../backend/cluster/cluster_control_root_registry_private.h"
#include "common/cluster_recovery_registry.h"
static uint8 bootstrap4_bytes[66560], bootstrap4_registry[8192];
static Size bootstrap4_registry_len;
static ClusterRecoveryRegistryV2 bootstrap4_reg;
static ClusterRecoveryCheckpointSourceV2 bootstrap4_sources[8];
static ClusterRecoveryCheckpointFileV2 bootstrap4_files[2];
static ClusterRecoveryCheckpointFileSourceV2 bootstrap4_intervals[2][8];
static char bootstrap4_path[MAXPGPATH];
static void
bootstrap4_source(const ControlRootImage *root, const ClusterControlRootSnapshot *s,
				  const ControlRootRecordRefsV2 *refs, unsigned at)
{
	ClusterRecoveryCheckpointSourceV2 *c = &bootstrap4_sources[at];
	ClusterRecoveryAnchorRefV2 ref = {0};
	ClusterRecoveryAnchorV2 anchor;
	uint8 bytes[512];
	char hex[65], path[MAXPGPATH];
	memset(c, 0, sizeof(*c));
	ref.identity = s->identity;
	ref.database_incarnation = root->header.v2.database_incarnation;
	ref.max_config_generation = root->header.v2.config_generation;
	ref.anchor_generation = refs->anchor_generation;
	memcpy(ref.anchor_sha256, refs->anchor_sha256, 32);
	memcpy(ref.claim_sha256, refs->claim_sha256, 32);
	bootstrap_hex(refs->anchor_sha256, hex);
	snprintf(path, sizeof(path),
			 "%s/global/anchor_images/thread_%u/generation_" UINT64_FORMAT "/anchor_" UINT64_FORMAT
			 "-%s.bin",
			 test_root, s->identity.origin_thread_id, s->identity.origin_owner_incarnation,
			 refs->anchor_generation, hex);
	read_all_or_abort(path, bytes, sizeof(bytes));
	if (cluster_recovery_anchor_v2_decode(bytes, sizeof(bytes), &ref, &anchor))
		abort();
	c->source.claim.identity = s->identity;
	c->source.claim.database_incarnation = ref.database_incarnation;
	c->source.claim.max_config_generation = ref.max_config_generation;
	memcpy(c->source.claim.claim_sha256, refs->claim_sha256, 32);
	c->source.timeline = s->checkpoint_tli;
	c->checkpoint = anchor.checkpoint_copy;
	c->checkpoint_start = anchor.checkpoint;
	c->checkpoint_end = anchor.checkpoint + 128;
	c->redo_start = anchor.checkpoint_copy.redo;
	c->replay_end = Max(c->checkpoint_end, s->validated_tail_lsn_exclusive);
}
static void
bootstrap4_write(ControlRootImage *root)
{
	uint8 ref[128];
	char path[MAXPGPATH];
	if (!cluster_recovery_registry_size_v2(&bootstrap4_reg, sizeof(bootstrap4_registry),
										   &bootstrap4_registry_len) ||
		!cluster_recovery_registry_encode_v2(&bootstrap4_reg, bootstrap4_registry,
											 bootstrap4_registry_len, ref) ||
		!cluster_control_root_registry_encode_v4(root, &bootstrap4_reg, ref, bootstrap4_bytes,
												 sizeof(bootstrap4_bytes)))
		abort();
	snprintf(path, sizeof(path), "%s/global/pgrac_recovery_registry", test_root);
	if (mkdir(path, 0700) && errno != EEXIST)
		abort();
	snprintf(bootstrap4_path, sizeof(bootstrap4_path),
			 "%s/0000000000000001-0000000000000001-0000000000000001.v2", path);
	write_all_or_abort(bootstrap4_path, bootstrap4_registry, bootstrap4_registry_len);
	path_for(path, sizeof(path), CLUSTER_CONTROL_ROOT_REL_PATH);
	write_all_or_abort(path, bootstrap4_bytes, sizeof(bootstrap4_bytes));
	path_for(path, sizeof(path), CLUSTER_CONTROL_ROOT_BAK_REL_PATH);
	write_all_or_abort(path, bootstrap4_bytes, sizeof(bootstrap4_bytes));
}
static void
bootstrap4_fixture(BootstrapFixture *f, ControlRootImage *root, bool history)
{
	char paths[3][MAXPGPATH];
	unsigned count = 0;
	bootstrap_read_fixture(f, 0);
	if (history)
		bootstrap_history_files(f, 127, 2, paths);
	root_fixture_version3(f->before);
	if (cluster_control_root_v3_decode(f->before, sizeof(f->before), v2_storage, TEST_SYSID, root))
		abort();
	/* Actual new-creation lineage, independently bound in local pgdata. */
	{
		PgracControlBinding binding;
		char path[MAXPGPATH];
		if (!pgrac_control_binding_decode(f->binding, sizeof(f->binding), &binding))
			abort();
		root->header.lineage_kind = binding.lineage_kind = PGRAC_CONTROL_LINEAGE_CREATION_V1;
		root->header.migration_prepare_generation = binding.migration_prepare_generation = 1;
		root->header.migration_transition_epoch = binding.migration_transition_epoch = 0;
		root->header.source_feature_bitmap = 0;
		root->header.target_feature_bitmap |= PGRAC_CONTROL_ROOT_FEATURE_SPACE_IDENTITY_V1 |
											  PGRAC_CONTROL_ROOT_FEATURE_SPACE_RESERVATION_V1;
		if (!pgrac_control_binding_encode(&binding, f->binding, sizeof(f->binding)))
			abort();
		snprintf(path, sizeof(path), "%s/global/%s", bootstrap_local, PGRAC_CONTROL_BINDING_NAME);
		write_all_or_abort(path, f->binding, sizeof(f->binding));
	}
	memset(&bootstrap4_reg, 0, sizeof(bootstrap4_reg));
	memset(bootstrap4_files, 0, sizeof(bootstrap4_files));
	memset(bootstrap4_intervals, 0, sizeof(bootstrap4_intervals));
	bootstrap4_reg.system_identifier = TEST_SYSID;
	bootstrap4_reg.database_incarnation = root->header.v2.database_incarnation;
	memcpy(bootstrap4_reg.storage_uuid, root->header.storage_uuid, 16);
	memset(bootstrap4_reg.redo_branch_uuid, 0x31, 16);
	bootstrap4_reg.redo_branch_generation = 1;
	bootstrap4_reg.coverage.control_generation = root->header.file_txn_seq;
	bootstrap4_reg.coverage.registration_generation = 1;
	bootstrap4_reg.coverage.checkpoint_generation = 1;
	bootstrap4_reg.coverage.commit_generation = 1;
	for (unsigned node = 0; node < 128; node++) {
		if (!root->present[node])
			continue;
		bootstrap4_source(root, &root->records[node], &root->refs[node], count++);
		if (root->refs[node].history_generation) {
			ClusterWalHistoryImage h;
			uint8 bytes[65604];
			struct stat st;
			if (stat(paths[0], &st))
				abort();
			read_all_or_abort(paths[0], bytes, st.st_size);
			if (cluster_control_root_v3_history_decode(bytes, st.st_size, root, node, &h))
				abort();
			for (unsigned i = 0; i < h.count; i++)
				bootstrap4_source(root, &h.records[i].snapshot, &h.records[i].refs, count++);
		}
	}
	bootstrap4_reg.coverage.sources = bootstrap4_sources;
	bootstrap4_reg.coverage.source_count = count;
	bootstrap4_reg.coverage.files = bootstrap4_files;
	bootstrap4_reg.coverage.file_count = 2;
	for (unsigned copy = 0; copy < 2; copy++) {
		ClusterRecoveryCheckpointFileV2 *file = &bootstrap4_files[copy];
		file->identity.kind = CLUSTER_RECOVERY_FILE_RELMAP_V2;
		file->identity.format_id = CLUSTER_RECOVERY_FORMAT_ID_V2;
		file->identity.format_version = 2;
		file->identity.system_identifier = TEST_SYSID;
		file->identity.database_incarnation = bootstrap4_reg.database_incarnation;
		memcpy(file->identity.storage_uuid, bootstrap4_reg.storage_uuid, 16);
		memcpy(file->identity.redo_branch_uuid, bootstrap4_reg.redo_branch_uuid, 16);
		file->identity.redo_branch_generation = 1;
		file->identity.redo_timeline = 1;
		file->identity.creation_generation = file->identity.registration_generation = 1;
		file->synced_bytes = 2304;
		file->page_count = 1;
		file->copy_count = 2;
		file->copy_index = copy;
		memset(file->image_digest, 0x61 + copy, 32);
		file->source_count = count;
		file->sources = bootstrap4_intervals[copy];
		for (unsigned i = 0; i < count; i++) {
			bootstrap4_intervals[copy][i].source_index = i;
			bootstrap4_intervals[copy][i].redo_start = bootstrap4_sources[i].redo_start;
			bootstrap4_intervals[copy][i].replay_end = bootstrap4_sources[i].replay_end;
		}
	}
	bootstrap4_write(root);
}
UT_TEST(test_bootstrap4_actual_files_and_complete_history)
{
	BootstrapFixture f;
	ControlRootImage root;
	ClusterControlBootstrapObservation out;
	uint8 hash[32];
	for (unsigned history = 0; history < 2; history++) {
		bootstrap4_fixture(&f, &root, history);
		UT_ASSERT_EQ(
			cluster_control_bootstrap_read(bootstrap_local, test_root, test_wal_root, 0, &out), 0);
		if (ut_current_failed)
			return;
		UT_ASSERT_EQ(out.snapshot.root_format_version, 4);
		UT_ASSERT_EQ(out.required.current_sources, 2);
		UT_ASSERT_EQ(out.required.history_sources, history ? 2 : 0);
		sha256_bytes(bootstrap4_bytes, sizeof(bootstrap4_bytes), hash);
		UT_ASSERT(memcmp(hash, out.snapshot.root_sha256, 32) == 0);
		UT_ASSERT_EQ(test_cf_lock_calls, 0);
		pfree(out.config_bytes);
	}
}
UT_TEST(test_bootstrap4_selected_registry_is_mandatory_and_exact)
{
	BootstrapFixture f;
	ControlRootImage root;
	for (unsigned fault = 0; fault < 5; fault++) {
		bootstrap4_fixture(&f, &root, false);
		if (fault == 0)
			UT_ASSERT_EQ(unlink(bootstrap4_path), 0);
		if (fault == 1) {
			bootstrap4_registry[300] ^= 1;
			write_all_or_abort(bootstrap4_path, bootstrap4_registry, bootstrap4_registry_len);
		}
		if (fault == 2)
			UT_ASSERT_EQ(chmod(bootstrap4_path, 0666), 0);
		if (fault == 3)
			UT_ASSERT_EQ(truncate(bootstrap4_path, bootstrap4_registry_len - 1), 0);
		if (fault == 4) {
			UT_ASSERT_EQ(unlink(bootstrap4_path), 0);
			UT_ASSERT_EQ(mkfifo(bootstrap4_path, 0600), 0);
		}
		bootstrap_read_refused(0);
		if (fault)
			UT_ASSERT_EQ(unlink(bootstrap4_path), 0);
	}
}
UT_TEST(test_bootstrap4_registry_cannot_substitute_selected_source)
{
	BootstrapFixture f;
	ControlRootImage root;
	for (unsigned fault = 0; fault < 3; fault++) {
		bootstrap4_fixture(&f, &root, true);
		if (fault == 0)
			bootstrap4_sources[3].source.claim.identity.root_lineage_seq++;
		if (fault == 1)
			bootstrap4_sources[0].source.claim.claim_sha256[0] ^= 1;
		if (fault == 2)
			bootstrap4_sources[0].checkpoint.nextOid++;
		bootstrap4_write(&root);
		bootstrap_read_refused(0);
	}
}
UT_TEST(test_bootstrap4_late_root_change_and_close_failure_refuse)
{
	BootstrapFixture f;
	ControlRootImage root;
	bootstrap4_fixture(&f, &root, true);
	memcpy(bootstrap_replacement, f.before, sizeof(f.before));
	bootstrap_race = 1;
	bootstrap_race_at = 3;
	bootstrap_read_refused(0);
	bootstrap4_fixture(&f, &root, false);
	bootstrap_close_fail_at = 3;
	bootstrap_read_refused(0);
	bootstrap_close_fail_at = 0;
}
UT_TEST(test_bootstrap4_composer_metadata_and_alias_preserve_full_identity)
{
	BootstrapFixture f;
	ControlRootImage root, metadata;
	ClusterControlBootstrapSnapshot out, saved;
	bootstrap4_fixture(&f, &root, false);
	f.input.root_before.data = f.input.root_after.data = bootstrap4_bytes;
	f.input.root_before.len = f.input.root_after.len = sizeof(bootstrap4_bytes);
	UT_ASSERT_EQ(cluster_control_bootstrap_decode_v4(&f.input, &bootstrap4_reg, &out), 0);
	UT_ASSERT_EQ(out.root_format_version, 4);
	UT_ASSERT(cluster_control_root_registry_decode_v4(bootstrap4_bytes, sizeof(bootstrap4_bytes),
													  v2_storage, TEST_SYSID, &metadata));
	UT_ASSERT(v2_zero(metadata.bytes, sizeof(metadata.bytes)));
	UT_ASSERT_EQ(metadata.header.format_version, 4);
	saved = out;
	UT_ASSERT(cluster_control_bootstrap_decode_v4(&f.input, &bootstrap4_reg,
												  (void *)&bootstrap4_sources[0]) != 0);
	UT_ASSERT_EQ(cluster_control_bootstrap_decode_v4(&f.input, &bootstrap4_reg, &out), 0);
	UT_ASSERT(memcmp(&saved, &out, sizeof(out)) == 0);
	/* The ordinary composer cannot accept v4 without the selected registry. */
	UT_ASSERT(cluster_control_bootstrap_decode(&f.input, &out) != 0);
	UT_ASSERT(v2_zero(&out, sizeof(out)));
}
static int
bootstrap_v4_tests(void)
{
	UT_PLAN(8);
	UT_RUN(test_bootstrap4_actual_files_and_complete_history);
	UT_RUN(test_bootstrap4_composer_metadata_and_alias_preserve_full_identity);
	UT_RUN(test_bootstrap4_selected_registry_is_mandatory_and_exact);
	UT_RUN(test_bootstrap4_registry_cannot_substitute_selected_source);
	UT_RUN(test_bootstrap4_late_root_change_and_close_failure_refuse);
	UT_RUN(test_bootstrap_v3_reads_exact_current_and_flat_history);
	UT_RUN(test_bootstrap_v3_pending_objects_and_reread_remain_exact);
	UT_RUN(test_bootstrap_read_exact_files_and_owned_config);
	UT_DONE();
	return ut_failed_count ? 1 : 0;
}
