/* Real early-reader files and original capacity collector; no CF admission. */
#include "../../backend/cluster/cluster_control_root_registry_private.h"
#include "common/cluster_recovery_registry.h"
#include "access/clog_file.h"
static uint8 bootstrap4_bytes[66560], bootstrap4_registry[8192];
static Size bootstrap4_registry_len;
static ClusterRecoveryRegistryV2 bootstrap4_reg;
static ClusterRecoveryCheckpointSourceV2 bootstrap4_sources[8];
static ClusterRecoveryCheckpointFileV2 bootstrap4_files[3];
static ClusterRecoveryCheckpointFileSourceV2 bootstrap4_intervals[3][8];
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
bootstrap4_register(ControlRootImage *root, const char *history_path)
{
	unsigned count = 0;
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
			if (stat(history_path, &st))
				abort();
			read_all_or_abort(history_path, bytes, st.st_size);
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
static void
bootstrap4_fixture(BootstrapFixture *f, ControlRootImage *root, bool history)
{
	char paths[3][MAXPGPATH];
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
	bootstrap4_register(root, history ? paths[0] : NULL);
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

UT_TEST(test_bootstrap4_initialized_creation_reads_original_reserved_inputs)
{
    BootstrapFixture f;
    ControlRootImage root;
    ClusterControlBootstrapObservation out;
    PgracControlBinding binding;
    char path[MAXPGPATH];

    bootstrap_read_fixture(&f, 0);
    first_start_fixture(f.before, &root);
    if (ut_current_failed) return;
    root.header.target_feature_bitmap |= PGRAC_CONTROL_ROOT_FEATURE_SPACE_IDENTITY_V1
        | PGRAC_CONTROL_ROOT_FEATURE_SPACE_RESERVATION_V1;
    UT_ASSERT(pgrac_control_binding_decode(f.binding, sizeof(f.binding), &binding));
    binding.lineage_kind = root.header.lineage_kind;
    binding.migration_prepare_generation = root.header.migration_prepare_generation;
    binding.migration_transition_epoch = root.header.migration_transition_epoch;
    memcpy(binding.migration_round_sha256, root.header.migration_round_sha256, 32);
    memcpy(binding.source_wal_state_sha256, root.header.source_wal_state_sha256, 32);
    UT_ASSERT(pgrac_control_binding_encode(&binding, f.binding, sizeof(f.binding)));
    snprintf(path, sizeof(path), "%s/global/%s", bootstrap_local, PGRAC_CONTROL_BINDING_NAME);
    write_all_or_abort(path, f.binding, sizeof(f.binding));
    snprintf(path, sizeof(path), "%s/native_side/origin_3", test_root);
    UT_ASSERT(mkdir(path, 0700) == 0 || errno == EEXIST);
    strlcat(path, "/pg_commit_ts", sizeof(path));
    UT_ASSERT(mkdir(path, 0700) == 0 || errno == EEXIST);
    bootstrap4_register(&root, NULL);
    UT_ASSERT_EQ(cluster_control_bootstrap_read(bootstrap_local, test_root, test_wal_root, 0, &out), 0);
    if (ut_current_failed) return;
    UT_ASSERT_EQ(out.required.current_sources, 2);
    UT_ASSERT_EQ(out.required.pending_sources, 2);
    UT_ASSERT(!out.snapshot.pending_wal_valid);
    pfree(out.config_bytes);
    MyBackendType = B_INVALID;
    test_reserve_mode = false;
}
UT_TEST(test_bootstrap4_ordinary_unfinished_pending_remains_unsupported)
{
    BootstrapFixture f;
    ControlRootImage root;
    ClusterControlBootstrapObservation out;
    char paths[3][MAXPGPATH];
    for (uint32 phase = CLUSTER_WAL_STARTUP_RESERVED;
         phase <= CLUSTER_WAL_STARTUP_INITIALIZING; phase++) {
        bootstrap_pending_fixture(&f, phase, paths);
        if (ut_current_failed) return;
        UT_ASSERT_EQ(cluster_control_root_v3_decode(f.before, sizeof(f.before), v2_storage,
            TEST_SYSID, &root), 0);
        /* Recover the original history path from the fixture's exact reference. */
        {
            char hex[65], history[MAXPGPATH];
            bootstrap_hex(root.refs[127].history_sha256, hex);
            snprintf(history, sizeof(history), "%s/global/wal_history/thread_128/history_"
                UINT64_FORMAT "-%s.bin", test_root, root.refs[127].history_generation, hex);
            bootstrap4_register(&root, history);
        }
        UT_ASSERT_EQ(cluster_control_bootstrap_read(bootstrap_local, test_root, test_wal_root,
            0, &out), CLUSTER_CONTROL_ROOT_PROFILE_UNSUPPORTED);
        UT_ASSERT(out.config_bytes == NULL);
    }
}

/* The catalog reader consumes the already selected early ROOT, not a new
 * registry selection. Preserve the original catalog files and bind the exact
 * complete v4 bytes through the original expected hash. */
static void
catalog_managed_clog_image(const ClusterRecoveryFileIdentityV2 *expected,
    FullTransactionId birth, uint8 *image)
{
    ClusterClogFileBindingV2 binding = {0};
    ClusterClogPageIdentityV2 page = {0};
    uint8 payload[BLCKSZ] = {0};
    binding.identity = *expected;
    page.identity.birth_observed = true;
    page.identity.birth_full_xid = birth;
    page.identity.page_generation = U64FromFullTransactionId(birth);
    if (!cluster_clog_file_header_encode_v2(&binding, image, CLUSTER_CLOG_FILE_HEADER_BYTES)
        || !cluster_clog_file_page_encode_v2(&binding, &page, payload, sizeof(payload),
            image + CLUSTER_CLOG_FILE_HEADER_BYTES, CLUSTER_CLOG_FRAME_BYTES))
        abort();
}

UT_TEST(test_bootstrap4_preserves_only_independently_selected_local_clog_identity)
{
    BootstrapFixture f;
    ControlRootImage root;
    ClusterControlBootstrapSnapshot out;
    ClusterRecoveryFileIdentityV2 saved;
    bootstrap4_fixture(&f, &root, false);
    bootstrap4_files[2] = bootstrap4_files[0];
    bootstrap4_files[2].identity.kind = CLUSTER_RECOVERY_FILE_CLOG_V2;
    bootstrap4_files[2].identity.origin = 0;
    bootstrap4_files[2].synced_bytes = CLUSTER_CLOG_FILE_HEADER_BYTES + CLUSTER_CLOG_FRAME_BYTES;
    bootstrap4_files[2].copy_count = 1;
    bootstrap4_reg.coverage.file_count = 3;
    bootstrap4_write(&root);
    f.input.root_before.data = f.input.root_after.data = bootstrap4_bytes;
    f.input.root_before.len = f.input.root_after.len = sizeof(bootstrap4_bytes);
    UT_ASSERT_EQ(cluster_control_bootstrap_decode_v4(&f.input, &bootstrap4_reg, &out), 0);
    UT_ASSERT(out.catalog_clog_valid);
    UT_ASSERT(memcmp(&out.catalog_clog, &bootstrap4_files[2].identity, sizeof(saved)) == 0);
    saved = out.catalog_clog;
    bootstrap4_files[2].identity.origin = 127;
    UT_ASSERT(memcmp(&out.catalog_clog, &saved, sizeof(saved)) == 0);
    bootstrap4_write(&root);
    UT_ASSERT_EQ(cluster_control_bootstrap_decode_v4(&f.input, &bootstrap4_reg, &out), 0);
    UT_ASSERT(!out.catalog_clog_valid);
    UT_ASSERT(v2_zero(&out.catalog_clog, sizeof(out.catalog_clog)));
}

static void
catalog_read_v4_fixture(CatalogReadFixture *f)
{
    BootstrapFixture bootstrap;
    ControlRootImage root, registry_root;
    uint8 legacy[CLUSTER_CONTROL_ROOT_FILE_BYTES], reference[128], bytes[66560];
    char path[MAXPGPATH];

    catalog_read_fixture(f);
    snprintf(path, sizeof(path), "%s/global/pgrac_control_root", f->path);
    read_all_or_abort(path, legacy, sizeof(legacy));
    UT_ASSERT_EQ(cluster_control_root_v3_decode(legacy, sizeof(legacy),
        f->expected.binding.storage_uuid, f->expected.binding.system_identifier, &root), 0);
    bootstrap4_fixture(&bootstrap, &registry_root, false);
    /* Original registry codec validates its complete source/coverage material;
     * only the catalog selection's common generation changes here. */
    bootstrap4_reg.coverage.control_generation = root.header.file_txn_seq;
    UT_ASSERT(cluster_recovery_registry_encode_v2(&bootstrap4_reg,
        bootstrap4_registry, bootstrap4_registry_len, reference));
    UT_ASSERT(cluster_control_root_registry_encode_v4(&root, &bootstrap4_reg,
        reference, bytes, sizeof(bytes)));
    write_all_or_abort(path, bytes, sizeof(bytes));
    sha256_bytes(bytes, sizeof(bytes), f->expected.root_sha256);
    f->expected.root_format_version = 4;
    f->expected.catalog_clog = bootstrap4_files[0].identity;
    f->expected.catalog_clog.kind = CLUSTER_RECOVERY_FILE_CLOG_V2;
    f->expected.catalog_clog.origin = f->expected.binding.node_id;
    f->expected.catalog_clog_valid = true;
    {
        uint8 image[CLUSTER_CLOG_FILE_HEADER_BYTES + CLUSTER_CLOG_FRAME_BYTES];
        catalog_managed_clog_image(&f->expected.catalog_clog, FirstNormalFullTransactionId, image);
        snprintf(path, sizeof(path), "%s/pgrac_initdb_native_side/pg_xact/0000", f->path);
        write_all_or_abort(path, image, sizeof(image));
    }
}

UT_TEST(test_catalog_v4_reads_exact_selected_root_and_rechecks_full_extension)
{
    CatalogReadFixture f;
    ClusterControlCatalogRead *read = NULL;
    ClusterCatalogStartupInput input;
    char path[MAXPGPATH];
    int fd;

    catalog_read_v4_fixture(&f);
    UT_ASSERT(cluster_control_catalog_read(f.path, f.path, &f.expected, &read, &input));
    if (ut_current_failed) {
        cluster_control_catalog_release(&read);
        UT_ASSERT(catalog_fixture_remove(f.path));
        return;
    }
    UT_ASSERT(cluster_catalog_startup_validate(&input));
    UT_ASSERT(cluster_control_catalog_current(read));
    snprintf(path, sizeof(path), "%s/global/pgrac_control_root", f.path);
    fd = open(path, O_WRONLY);
    UT_ASSERT(fd >= 0 && pwrite(fd, "X", 1, 639) == 1 && close(fd) == 0);
    UT_ASSERT(!cluster_control_catalog_current(read));
    cluster_control_catalog_release(&read);
    UT_ASSERT(catalog_fixture_remove(f.path));
}

UT_TEST(test_catalog_v4_refuses_wrong_selected_version_length_or_hash)
{
    for (unsigned fault = 0; fault < 3; fault++) {
        CatalogReadFixture f;
        ClusterControlCatalogRead *read = NULL;
        ClusterCatalogStartupInput input;
        char path[MAXPGPATH];

        catalog_read_v4_fixture(&f);
        if (fault == 0)
            f.expected.root_format_version = 3;
        else if (fault == 1) {
            snprintf(path, sizeof(path), "%s/global/pgrac_control_root", f.path);
            UT_ASSERT_EQ(truncate(path, CLUSTER_CONTROL_ROOT_FILE_BYTES), 0);
        } else
            f.expected.root_sha256[31] ^= 1;
        UT_ASSERT(!cluster_control_catalog_read(f.path, f.path, &f.expected, &read, &input));
        UT_ASSERT(v2_zero(&input, sizeof(input)));
        cluster_control_catalog_release(&read);
        UT_ASSERT(catalog_fixture_remove(f.path));
    }
}

UT_TEST(test_catalog_v4_requires_saved_identity_and_complete_managed_frame)
{
    for (unsigned fault = 0; fault < 8; fault++) {
        CatalogReadFixture f;
        ClusterControlCatalogRead *read = NULL;
        ClusterCatalogStartupInput input;
        uint8 image[CLUSTER_CLOG_FILE_HEADER_BYTES + CLUSTER_CLOG_FRAME_BYTES];
        char path[MAXPGPATH];
        catalog_read_v4_fixture(&f);
        snprintf(path, sizeof(path), "%s/pgrac_initdb_native_side/pg_xact/0000", f.path);
        if (fault == 0) f.expected.catalog_clog_valid = false;
        if (fault == 1) f.expected.catalog_clog.origin++;
        if (fault == 2) f.expected.catalog_clog.creation_generation++;
        if (fault == 3) {
            catalog_managed_clog_image(&f.expected.catalog_clog,
                FullTransactionIdFromU64(4), image);
            write_all_or_abort(path, image, sizeof(image));
        }
        if (fault == 4 || fault == 5) {
            read_all_or_abort(path, image, sizeof(image));
            image[fault == 4 ? 100 : sizeof(image)-1] ^= 1;
            write_all_or_abort(path, image, sizeof(image));
        }
        if (fault == 6) { uint8 raw[BLCKSZ] = {0}; write_all_or_abort(path, raw, sizeof(raw)); }
        if (fault == 7) UT_ASSERT_EQ(truncate(path, sizeof(image)-1), 0);
        UT_ASSERT(!cluster_control_catalog_read(f.path, f.path, &f.expected, &read, &input));
        UT_ASSERT(v2_zero(&input, sizeof(input)));
        cluster_control_catalog_release(&read);
        UT_ASSERT(catalog_fixture_remove(f.path));
    }
}

UT_TEST(test_catalog_v4_current_rejects_same_inode_reseal_and_identical_replacement)
{
    for (unsigned fault = 0; fault < 2; fault++) {
        CatalogReadFixture f;
        ClusterControlCatalogRead *read = NULL;
        ClusterCatalogStartupInput input;
        uint8 image[CLUSTER_CLOG_FILE_HEADER_BYTES + CLUSTER_CLOG_FRAME_BYTES];
        char path[MAXPGPATH], old[MAXPGPATH];
        catalog_read_v4_fixture(&f);
        UT_ASSERT(cluster_control_catalog_read(f.path, f.path, &f.expected, &read, &input));
        if (ut_current_failed) { cluster_control_catalog_release(&read); catalog_fixture_remove(f.path); return; }
        UT_ASSERT(cluster_control_catalog_current(read));
        snprintf(path, sizeof(path), "%s/pgrac_initdb_native_side/pg_xact/0000", f.path);
        read_all_or_abort(path, image, sizeof(image));
        if (fault == 0) {
            catalog_managed_clog_image(&f.expected.catalog_clog, FullTransactionIdFromU64(4), image);
        } else {
            snprintf(old, sizeof(old), "%s.old", path);
            UT_ASSERT_EQ(rename(path, old), 0);
        }
        write_all_or_abort(path, image, sizeof(image));
        UT_ASSERT(!cluster_control_catalog_current(read));
        cluster_control_catalog_release(&read);
        UT_ASSERT(catalog_fixture_remove(f.path));
    }
}

static int
bootstrap_v4_tests(void)
{
	UT_PLAN(18);
	UT_RUN(test_bootstrap4_preserves_only_independently_selected_local_clog_identity);
	UT_RUN(test_catalog_v4_requires_saved_identity_and_complete_managed_frame);
	UT_RUN(test_catalog_v4_current_rejects_same_inode_reseal_and_identical_replacement);
	UT_RUN(test_catalog_v4_reads_exact_selected_root_and_rechecks_full_extension);
	UT_RUN(test_catalog_v4_refuses_wrong_selected_version_length_or_hash);
	UT_RUN(test_catalog_read_requires_independent_original_and_exact_root);
	UT_RUN(test_catalog_recheck_rejects_replaced_root_and_changed_allocator);
	UT_RUN(test_catalog_read_refuses_missing_corrupt_and_unsafe_objects);
	UT_RUN(test_bootstrap4_initialized_creation_reads_original_reserved_inputs);
	UT_RUN(test_bootstrap4_ordinary_unfinished_pending_remains_unsupported);
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
