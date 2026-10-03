/* First startup consumes real immutable files; only live formation/owner
 * APIs use the enclosing suite's normal seams. Author: SqlRush <sqlrush@gmail.com> */
static void v3_startup_native_record(const ClusterWalStartupImage *op, ControlFileData *candidate);
static void history_stage_dirs(uint32 node);

static void
first_start_fixture(uint8 before[66048], ControlRootImage *root)
{
	static uint64 boot = 50000;
	ClusterPhase1FullStopPlan ignored;
	ClusterRecoveryAnchorV2 anchors[2];
	ClusterControlRootFileToken token;
	ClusterCfImageStage stage;
	ControlFileData common;
	ControlRootImage sources;
	ClusterSharedConfigIdentity config_id = {0};
	ClusterSharedConfigEntry entry = {-1, "cluster.enabled", "on"};
	uint8 ids[128][16] = {{0}}, inputs[128][1536], uuid[16] = {0};
	char path[MAXPGPATH], hex[65], config[1024];
	size_t length;

	test_reserve_mode = false;
	v2_close_fixture(before, &ignored);
	(void)v2_close_add_peer(before, 3, &ignored);
	UT_ASSERT_EQ(cluster_control_root_v2_decode(before, 66048, v2_storage, TEST_SYSID, root), 0);
	anchors[0] = v2_stop_clean_anchor(before, &root->records[0].identity);
	anchors[1] = v2_stop_clean_anchor(before, &root->records[3].identity);
	UT_ASSERT_EQ(cluster_control_root_v2_read_control_locked(v2_storage, TEST_SYSID,
		root, &common, &token), 0);
	root->header.format_version = 3;
	root->header.lineage_kind = PGRAC_CONTROL_LINEAGE_CREATION_V1;
	root->header.file_txn_seq = root->header.migration_prepare_generation = 1;
	root->header.migration_transition_epoch = root->header.source_feature_bitmap = 0;
	root->header.v2.database_state = CLUSTER_CONTROL_ROOT_DATABASE_MOUNTED;
	root->header.v2.formation_seq = root->header.v2.config_generation = 1;
	root->header.v2.control_image_generation = root->header.v2.catalog_manifest_generation = 1;
	root->header.v2.serving[0] = root->header.v2.serving[1] = 0;
	config_id.system_identifier = TEST_SYSID;
	config_id.database_incarnation = root->header.v2.database_incarnation;
	config_id.generation = 1;
	memcpy(config_id.configured, root->header.v2.configured, sizeof(config_id.configured));
	memcpy(config_id.storage_uuid, root->header.storage_uuid, 16);
	memcpy(config_id.authority_uuid, root->header.authority_uuid, 16);
	UT_ASSERT_EQ(cluster_shared_config_encode(&config_id, &entry, 1, config, sizeof(config),
		&length, root->header.v2.config_sha256), 0);
	for (unsigned i = 0; i < 32; i++)
		snprintf(hex + i * 2, 3, "%02x", root->header.v2.config_sha256[i]);
	snprintf(path, sizeof(path), "%s/global/config_images/1-%s.conf", test_root, hex);
	write_all_or_abort(path, (uint8 *)config, length);
	uuid[0] = ++boot; uuid[6] = 0x42; uuid[8] = 0x82;
	UT_ASSERT_EQ(cluster_cf_control_image_prepare(&common, 1, uuid, &stage), 0);
	UT_ASSERT_EQ(cluster_cf_control_image_install(&stage), 0);
	memcpy(root->header.v2.control_image_sha256, stage.image_sha256, 32);
	memset(&test_reserve_formation, 0, sizeof(test_reserve_formation));
	test_reserve_formation.local_epoch = test_epoch;
	/* Formation admits MEMBER before native startup installs this boot's writer. */
	test_reserve_formation.self_join_admitted = 0;
	test_reserve_formation.startup_formation_generation = boot;
	for (unsigned n = 0; n < 2; n++) {
		unsigned node = n == 0 ? 0 : 3;
		ClusterControlRootSnapshot *record = &root->records[node];
		ClusterWalThreadClaimV2 claim = {0};
		uint8 encoded[112], anchor_bytes[512];
		record->lifecycle = CLUSTER_CONTROL_ROOT_LIFECYCLE_CLOSED;
		record->root_publish_seq = record->identity.root_lineage_seq = 1;
		record->checkpoint_lower_lsn = record->tail_last_record_lsn;
		root->publisher_node[node] = node;
		root->publisher_incarnation[node] = record->identity.origin_owner_incarnation;
		root->refs[node].history_generation = 0;
		memset(root->refs[node].history_sha256, 0, 32);
		root->refs[node].anchor_generation = 1;
		claim.identity = record->identity;
		claim.identity.thread_claim_crc32c = 0;
		claim.database_incarnation = root->header.v2.database_incarnation;
		claim.config_generation = claim.claim_generation = 1;
		UT_ASSERT_EQ(cluster_wal_claim_v2_encode(&claim, encoded), 0);
		record->identity.thread_claim_crc32c = claim.identity.thread_claim_crc32c = image_crc(encoded, 104);
		sha256_bytes(encoded, sizeof(encoded), root->refs[node].claim_sha256);
		v2_claim_path(&claim.identity, path);
		write_all_or_abort(path, encoded, sizeof(encoded));
		anchors[n].identity = claim.identity;
		anchors[n].config_generation = anchors[n].anchor_generation = 1;
		memcpy(anchors[n].claim_sha256, root->refs[node].claim_sha256, 32);
		UT_ASSERT_EQ(cluster_recovery_anchor_v2_encode(&anchors[n], anchor_bytes), 0);
		sha256_bytes(anchor_bytes, sizeof(anchor_bytes), root->refs[node].anchor_sha256);
		for (unsigned i = 0; i < 32; i++)
			snprintf(hex + i * 2, 3, "%02x", root->refs[node].anchor_sha256[i]);
		snprintf(path, sizeof(path), "%s/global/anchor_images/thread_%u/generation_" UINT64_FORMAT
			"/anchor_1-%s.bin", test_root, node + 1, claim.identity.origin_owner_incarnation, hex);
		write_all_or_abort(path, anchor_bytes, sizeof(anchor_bytes));
		ids[node][0] = node + 1;
		test_reserve_formation.membership.membership_state[node] = CLUSTER_MEMBER_MEMBER;
		test_reserve_formation.membership.last_admitted_incarnation[node] = boot + node;
		test_reserve_cut.observer[node] = boot + node;
	}
	sources = *root;
	UT_ASSERT_EQ(cluster_control_root_v3_initialized_inputs(&sources, ids, wal_segment_size,
		root, inputs), 0);
	path_for(path, sizeof(path), "global/wal_startup");
	UT_ASSERT(mkdir(path, 0700) == 0 || errno == EEXIST);
	for (unsigned node = 0; node <= 3; node += 3) {
		snprintf(path, sizeof(path), "%s/global/wal_startup/thread_%u", test_root, node + 1);
		UT_ASSERT(mkdir(path, 0700) == 0 || errno == EEXIST);
		strlcat(path, "/.staging", sizeof(path));
		UT_ASSERT(mkdir(path, 0700) == 0 || errno == EEXIST);
		for (unsigned i = 0; i < 32; i++)
			snprintf(hex + i * 2, 3, "%02x", root->startup[node].sha256[i]);
		snprintf(path, sizeof(path), "%s/global/wal_startup/thread_%u/startup_1-%s.bin",
			test_root, node + 1, hex);
		write_all_or_abort(path, inputs[node], 1536);
	}
	memcpy(before, root->bytes, 66048); v2_write_roots(before);
	test_reserve_mode = test_reserve_quorum = true;
	test_reserve_provider = false;
	test_reserve_evidence = CLUSTER_STARTUP_EXIT_UNAVAILABLE;
	test_startup_exit_requests = 0;
	test_reserve_fault = 0;
	test_history_fail_sync = test_history_sync_count = 0;
	test_startup_sync_hook = NULL;
	test_cf_mode = test_actual_cf = NoLock;
	MyBackendType = B_STARTUP;
	ShutdownRequestPending = false;
	enableFsync = true;
}

UT_TEST(test_first_start_binds_original_inputs_without_clean_exit)
{
	uint8 before[66048], after[66048];
	ControlRootImage root, bound;
	ClusterWalStartupImage op, seed[2];
	ClusterWalSourceRef restart;
	char path[MAXPGPATH];
	first_start_fixture(before, &root);
	if (ut_current_failed) return;
	test_cf_mode = test_actual_cf = ShareLock;
	UT_ASSERT_EQ(cluster_wal_startup_read_locked(&root, 0, &seed[0]), 0);
	UT_ASSERT_EQ(cluster_wal_startup_read_locked(&root, 3, &seed[1]), 0);
	test_cf_mode = test_actual_cf = NoLock;
	restart = v3_driver_restart(&root, 3);
	UT_ASSERT_EQ(cluster_control_root_v3_startup_advance_clean(&restart, &op), CLUSTER_CONTROL_ROOT_RECONFIG_WAIT);
	v2_assert_primary_unchanged(before);
	restart = v3_driver_restart(&root, 0);
	UT_ASSERT_EQ(cluster_control_root_v3_startup_advance_clean(&restart, &op), CLUSTER_CONTROL_ROOT_RECONFIG_WAIT);
	UT_ASSERT(v2_zero(&op, sizeof(op)));
	if (ut_current_failed) return;
	path_for(path, sizeof(path), CLUSTER_CONTROL_ROOT_REL_PATH);
	read_all_or_abort(path, after, sizeof(after));
	UT_ASSERT_EQ(cluster_control_root_v3_decode(after, sizeof(after), v2_storage, TEST_SYSID, &bound), 0);
	UT_ASSERT_EQ(bound.header.file_txn_seq, 2);
	UT_ASSERT_EQ(bound.header.v2.serving[0], 0);
	UT_ASSERT_EQ(bound.header.v2.formation_seq, test_reserve_formation.startup_formation_generation);
	test_cf_mode = test_actual_cf = ShareLock;
	for (unsigned n = 0; n < 2; n++) {
		unsigned node = n == 0 ? 0 : 3;
		UT_ASSERT_EQ(cluster_wal_startup_read_locked(&bound, node, &op), 0);
		UT_ASSERT_EQ(op.input_kind, 4);
		UT_ASSERT_EQ(op.phase, CLUSTER_WAL_STARTUP_RESERVED);
		UT_ASSERT_EQ(op.formation_epoch, test_epoch);
		UT_ASSERT_EQ(op.claim.identity.origin_owner_incarnation, test_reserve_cut.observer[node]);
		UT_ASSERT_EQ(op.claim.identity.root_lineage_seq, 2);
		UT_ASSERT(memcmp(&op.predecessor, &seed[n].predecessor, sizeof(op.predecessor)) == 0);
		UT_ASSERT(memcmp(op.operation_uuid, seed[n].operation_uuid, 16) == 0);
		UT_ASSERT(memcmp(op.predecessor_evidence_sha256, seed[n].predecessor_evidence_sha256, 32) == 0);
		UT_ASSERT_EQ(cluster_wal_startup_read_locked(&root, node, &op), 0);
		UT_ASSERT_EQ(op.formation_epoch, 0);
	}
	test_cf_mode = test_actual_cf = NoLock;
	/* An equal epoch/roster does not bless a different accepted formation. */
	test_reserve_formation.startup_formation_generation++;
	UT_ASSERT(cluster_control_root_v3_startup_advance_clean(&restart, &op) != 0);
	UT_ASSERT(v2_zero(&op, sizeof(op)));
	v2_assert_primary_unchanged(after);
	test_reserve_formation.startup_formation_generation--;
	/* Both actual targets prepare; then the coordinator begins the cohort. */
	restart = v3_driver_restart(&root, 0);
	UT_ASSERT_EQ(cluster_control_root_v3_startup_advance_clean(&restart, &op), CLUSTER_CONTROL_ROOT_RECONFIG_WAIT);
	restart = v3_driver_restart(&root, 3);
	UT_ASSERT_EQ(cluster_control_root_v3_startup_advance_clean(&restart, &op), CLUSTER_CONTROL_ROOT_RECONFIG_WAIT);
	restart = v3_driver_restart(&root, 0);
	UT_ASSERT_EQ(cluster_control_root_v3_startup_advance_clean(&restart, &op), CLUSTER_CONTROL_ROOT_RECONFIG_WAIT);
	UT_ASSERT_EQ(cluster_control_root_v3_startup_advance_clean(&restart, &op), 0);
	UT_ASSERT_EQ(op.phase, CLUSTER_WAL_STARTUP_INITIALIZING);
	UT_ASSERT_EQ(op.input_kind, 4);
	UT_ASSERT_EQ(test_actual_cf, NoLock);
	for (unsigned node = 0; node <= 3; node += 3) {
		ControlFileData candidate;
		ClusterControlRootFileToken token;
		ClusterWalStartupImage durable;
		ClusterWalSourceRef writer;
		ClusterWalHistoryImage history;
		restart = v3_driver_restart(&root, node);
		UT_ASSERT_EQ(cluster_control_root_v3_startup_advance_clean(&restart, &op), 0);
		test_cf_mode = test_actual_cf = ShareLock;
		UT_ASSERT_EQ(cluster_control_root_v3_read_thread_locked(&op.predecessor.snapshot.identity,
			&bound, &candidate, &token), 0);
		test_cf_mode = test_actual_cf = NoLock;
		v3_startup_native_record(&op, &candidate);
		history_stage_dirs(node);
		UT_ASSERT_EQ(cluster_control_root_v3_startup_checkpoint(&op.claim.identity, op.operation_uuid,
			&candidate, test_checkpoint_end, &durable), 0);
		if (ut_current_failed) return;
		UT_ASSERT_EQ(cluster_control_root_v3_startup_install_writer(&durable, &writer), 0);
		if (ut_current_failed) return;
		UT_ASSERT(cluster_control_root_identity_equal(&writer.claim.identity, &op.claim.identity));
		UT_ASSERT_EQ(cluster_control_root_v3_startup_install_writer(&durable, &writer), 0);
		test_cf_mode = test_actual_cf = ShareLock;
		UT_ASSERT_EQ(cluster_control_root_v3_read_thread_locked(&op.claim.identity,
			&bound, &candidate, &token), 0);
		UT_ASSERT_EQ(cluster_wal_history_read_locked(&bound, node, &history), 0);
		UT_ASSERT_EQ(history.count, 1);
		UT_ASSERT(memcmp(&history.records[0].snapshot, &op.predecessor.snapshot,
			sizeof(op.predecessor.snapshot)) == 0);
		UT_ASSERT_EQ(bound.header.v2.serving[0], 0);
		UT_ASSERT_EQ(bound.startup[node].generation, 0);
		test_cf_mode = test_actual_cf = NoLock;
	}
	cluster_node_id = 0;
	UT_ASSERT_EQ(test_startup_exit_requests, 0);
	test_reserve_mode = false;
}

UT_TEST(test_first_start_rejects_incomplete_or_changed_formation)
{
	for (unsigned fault = 0; fault < 11; fault++) {
		uint8 before[66048];
		ControlRootImage root;
		ClusterWalStartupImage op;
		ClusterWalSourceRef restart;
		first_start_fixture(before, &root);
		if (ut_current_failed) return;
		restart = v3_driver_restart(&root, 0);
		switch (fault) {
		case 0: test_reserve_formation.startup_formation_generation = 0; break;
		case 1: test_reserve_formation.membership.membership_state[3] = CLUSTER_MEMBER_ABSENT; break;
		case 2: test_reserve_formation.membership.membership_state[2] = CLUSTER_MEMBER_MEMBER; break;
		case 3: test_reserve_formation.membership.last_admitted_incarnation[3] = 0; break;
		case 4: test_reserve_formation.membership.last_admitted_incarnation[3] = root.records[3].identity.origin_owner_incarnation; break;
		case 5: test_reserve_formation.pending_join_bitmap[0] = 8; break;
		case 6: test_reserve_formation.excluded_bitmap[0] = 8; break;
		case 7: test_reserve_formation.local_epoch++; break;
		case 8: test_reserve_quorum = false; break;
		case 9: test_reserve_formation.self_join_failed = 1; break;
		case 10: test_reserve_formation.self_join_admitted = 2; break;
		}
		UT_ASSERT(cluster_control_root_v3_startup_advance_clean(&restart, &op) != 0);
		UT_ASSERT(v2_zero(&op, sizeof(op)));
		UT_ASSERT_EQ(test_actual_cf, NoLock);
		v2_assert_primary_unchanged(before);
		test_reserve_mode = false;
	}
}

UT_TEST(test_first_start_d116_wal_refuses_before_any_root_write)
{
	for (unsigned node = 0; node <= 3; node += 3) {
		uint8 before[66048], backup[66048], after[66048];
		ControlRootImage root;
		ClusterWalStartupImage op;
		ClusterWalSourceRef restart;
		ClusterControlRootResult result;
		char path[MAXPGPATH];
		uint16 old_magic = UINT16_C(0xD116);
		int fd, renames;
		XLogRecPtr at;

		first_start_fixture(before, &root);
		if (ut_current_failed) return;
		restart = v3_driver_restart(&root, 0);
		at = root.records[node].tail_last_record_lsn;
		v2_checkpoint_wal_path(&root.records[node].identity, at,
			root.records[node].checkpoint_tli, path);
		fd = open(path, O_RDWR);
		UT_ASSERT(fd >= 0);
		UT_ASSERT_EQ(pwrite(fd, &old_magic, sizeof(old_magic),
			(at % wal_segment_size) - (at % XLOG_BLCKSZ)), sizeof(old_magic));
		UT_ASSERT_EQ(fsync(fd), 0);
		UT_ASSERT_EQ(close(fd), 0);
		path_for(path, sizeof(path), CLUSTER_CONTROL_ROOT_BAK_REL_PATH);
		read_all_or_abort(path, backup, sizeof(backup));
		renames = test_durable_rename_calls;
		result = cluster_control_root_v3_startup_advance_clean(&restart, &op);
		UT_ASSERT(result != CLUSTER_CONTROL_ROOT_OK_PRIMARY);
		UT_ASSERT(result != CLUSTER_CONTROL_ROOT_RECONFIG_WAIT);
		UT_ASSERT(v2_zero(&op, sizeof(op)));
		UT_ASSERT_EQ(test_durable_rename_calls, renames);
		UT_ASSERT_EQ(test_actual_cf, NoLock);
		UT_ASSERT_EQ(test_walr_begin_calls, test_walr_end_calls);
		v2_assert_primary_unchanged(before);
		read_all_or_abort(path, after, sizeof(after));
		UT_ASSERT(memcmp(backup, after, sizeof(backup)) == 0);
	}
}

static void
first_start_late_change(void)
{
	test_startup_sync_hook = NULL;
	test_reserve_formation.startup_formation_generation++;
}

UT_TEST(test_first_start_io_and_late_races_never_publish_authority)
{
	for (unsigned fault = 0; fault < 5; fault++) {
		uint8 before[66048];
		ControlRootImage root;
		ClusterWalStartupImage op;
		ClusterWalSourceRef restart;
		char path[MAXPGPATH], hex[65];
		first_start_fixture(before, &root);
		if (ut_current_failed) return;
		restart = v3_driver_restart(&root, 0);
		if (fault == 0) test_history_fail_sync = 1;
		if (fault == 1) test_startup_sync_hook = first_start_late_change;
		if (fault == 2) test_fail_primary_rename = true;
		if (fault == 3) {
			for (unsigned i = 0; i < 32; i++) snprintf(hex + 2 * i, 3, "%02x", root.startup[3].sha256[i]);
			snprintf(path, sizeof(path), "%s/global/wal_startup/thread_4/startup_1-%s.bin", test_root, hex);
			UT_ASSERT_EQ(unlink(path), 0);
		}
		if (fault == 4) {
			v2_claim_path(&root.records[3].identity, path);
			UT_ASSERT_EQ(unlink(path), 0);
		}
		UT_ASSERT(cluster_control_root_v3_startup_advance_clean(&restart, &op) != 0);
		UT_ASSERT(v2_zero(&op, sizeof(op)));
		UT_ASSERT_EQ(test_actual_cf, NoLock);
		UT_ASSERT_EQ(test_walr_begin_calls, test_walr_end_calls);
		v2_assert_primary_unchanged(before);
		test_fail_primary_rename = false;
		test_history_fail_sync = 0;
		test_startup_sync_hook = NULL;
		test_reserve_mode = false;
	}
}
