/* Fixed-cohort INSTALL -> serving -> normal-stop proof. All immutable files,
 * WAL, CF publication and readback use production code. Only the live semantic
 * owner is a fixture: it accepts the exact supplied carrier, never arbitrary
 * registered/OPEN-shaped inputs. Author: SqlRush <sqlrush@gmail.com> */
static ClusterSemanticActivationRecord serving_open;
static uint8 serving_descriptor[CLUSTER_UNDO_ROOT_DESCRIPTOR_BYTES];
static ClusterNormalStopPollResult serving_match = CLUSTER_NORMAL_STOP_READY;
static unsigned serving_match_calls;
static uint64 serving_current_epoch;
static ClusterNormalStopPollResult serving_epoch_result;
static void (*serving_match_hook)(void);

ClusterNormalStopPollResult
cluster_semantic_normal_stop_match(const ClusterSemanticActivationRecord *open,
								   const uint8 descriptor[CLUSTER_UNDO_ROOT_DESCRIPTOR_BYTES],
								   uint64 members[CLUSTER_PHASE1_FULL_STOP_MEMBER_COUNT],
								   const char **reason)
{
	memset(members, 0, sizeof(uint64) * CLUSTER_PHASE1_FULL_STOP_MEMBER_COUNT);
	if (reason != NULL)
		*reason = "FIXTURE_EXACT_OPEN_CARRIER";
	serving_match_calls++;
	if (serving_match_hook != NULL)
		serving_match_hook();
	if (open == NULL || descriptor == NULL || memcmp(open, &serving_open, sizeof(*open))
		|| memcmp(descriptor, serving_descriptor, sizeof(serving_descriptor)))
		return CLUSTER_NORMAL_STOP_INVALID;
	if (serving_match != CLUSTER_NORMAL_STOP_READY)
		return serving_match;
	for (unsigned node = 0; node < CLUSTER_PHASE1_FULL_STOP_MEMBER_COUNT; node++)
		if ((open->admitted_members_lo & (UINT64_C(1) << node)) != 0)
			members[node] = test_reserve_cut.observer[node];
	return CLUSTER_NORMAL_STOP_READY;
}

ClusterNormalStopPollResult
cluster_semantic_normal_stop_current_epoch(
	const ClusterSemanticActivationRecord *open,
	const uint8 descriptor[CLUSTER_UNDO_ROOT_DESCRIPTOR_BYTES], uint64 *epoch)
{
	*epoch = 0;
	if (memcmp(open, &serving_open, sizeof(*open)) != 0
		|| memcmp(descriptor, serving_descriptor, sizeof(serving_descriptor)) != 0)
		return CLUSTER_NORMAL_STOP_INVALID;
	if (serving_epoch_result == CLUSTER_NORMAL_STOP_READY)
		*epoch = serving_current_epoch;
	return serving_epoch_result;
}

static void
serving_fixture_installed(uint8 before[66048], ControlRootImage *root, bool all)
{
	ClusterWalSourceRef restart, writer;
	ClusterWalStartupImage op, durable;
	ControlFileData candidate;
	ClusterControlRootFileToken token;
	char path[MAXPGPATH];

	first_start_fixture(before, root);
	if (ut_current_failed)
		return;
	restart = v3_driver_restart(root, 0);
	UT_ASSERT_EQ(cluster_control_root_v3_startup_advance_clean(&restart, &op),
				 CLUSTER_CONTROL_ROOT_RECONFIG_WAIT);
	UT_ASSERT_EQ(cluster_control_root_v3_startup_advance_clean(&restart, &op),
				 CLUSTER_CONTROL_ROOT_RECONFIG_WAIT);
	restart = v3_driver_restart(root, 3);
	UT_ASSERT_EQ(cluster_control_root_v3_startup_advance_clean(&restart, &op),
				 CLUSTER_CONTROL_ROOT_RECONFIG_WAIT);
	restart = v3_driver_restart(root, 0);
	UT_ASSERT_EQ(cluster_control_root_v3_startup_advance_clean(&restart, &op),
				 CLUSTER_CONTROL_ROOT_RECONFIG_WAIT);
	for (unsigned node = 0; node <= 3; node += 3) {
		ControlRootImage observed;
		restart = v3_driver_restart(root, node);
		UT_ASSERT_EQ(cluster_control_root_v3_startup_advance_clean(&restart, &op), 0);
		test_cf_mode = test_actual_cf = ShareLock;
		UT_ASSERT_EQ(cluster_control_root_v3_read_thread_locked(&op.predecessor.snapshot.identity,
																&observed, &candidate, &token),
					 0);
		test_cf_mode = test_actual_cf = NoLock;
		v3_startup_native_record(&op, &candidate);
		history_stage_dirs(node);
		UT_ASSERT_EQ(cluster_control_root_v3_startup_checkpoint(&op.claim.identity,
																op.operation_uuid, &candidate,
																test_checkpoint_end, &durable),
					 0);
		if (all || node == 0)
			UT_ASSERT_EQ(cluster_control_root_v3_startup_install_writer(&durable, &writer), 0);
		if (ut_current_failed)
			return;
	}
	path_for(path, sizeof(path), CLUSTER_CONTROL_ROOT_REL_PATH);
	read_all_or_abort(path, before, 66048);
	UT_ASSERT_EQ(cluster_control_root_v3_decode(before, 66048, v2_storage, TEST_SYSID, root), 0);
	cluster_node_id = 0;
	test_self_incarnation = test_membership_incarnation = test_reserve_cut.observer[0];
	MyBackendType = B_LMON;
	test_reserve_formation.self_join_admitted = 1;
	memset(&serving_open, 0, sizeof(serving_open));
	serving_open.phase = CLUSTER_SEMANTIC_PHASE_OPEN;
	serving_open.record_generation = 6;
	serving_open.transition_epoch = test_epoch;
	serving_open.admitted_members_lo = 9;
	serving_open.coordinator_incarnation = test_self_incarnation;
	serving_open.capability_sample_digest = 41;
	serving_open.source_feature_bitmap = CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1;
	serving_open.target_feature_bitmap = CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1
										 | CLUSTER_SEMANTIC_FEATURE_R11_RESOURCE_X_D5_CUTOVER_V1;
	memset(serving_descriptor, 0x71, sizeof(serving_descriptor));
	serving_match = CLUSTER_NORMAL_STOP_READY;
	serving_match_calls = 0;
	serving_current_epoch = test_epoch;
	serving_epoch_result = CLUSTER_NORMAL_STOP_READY;
	serving_match_hook = NULL;
	UT_ASSERT_EQ(root->header.v2.database_state, CLUSTER_CONTROL_ROOT_DATABASE_MOUNTED);
	UT_ASSERT_EQ(root->header.v2.serving[0], 0);
}

static void
serving_fixture(uint8 before[66048], ControlRootImage *root)
{
	serving_fixture_installed(before, root, true);
}

UT_TEST(test_serving_requires_coordinator_publish_then_startup_is_active)
{
	uint8 before[66048], after[66048];
	ControlRootImage root, published;
	ClusterControlRootFileToken token, peer;
	ClusterControlRootStopObservation stop;
	char path[MAXPGPATH];
	int writes;
	serving_fixture(before, &root);
	if (ut_current_failed)
		return;
	cluster_node_id = 3;
	test_self_incarnation = test_membership_incarnation = test_reserve_cut.observer[3];
	UT_ASSERT_EQ(cluster_control_root_v3_serving_poll(&serving_open, serving_descriptor, &peer),
				 CLUSTER_CONTROL_ROOT_RECONFIG_WAIT);
	UT_ASSERT(v2_zero(&peer, sizeof(peer)));
	v2_assert_primary_unchanged(before);
	cluster_node_id = 0;
	test_self_incarnation = test_membership_incarnation = test_reserve_cut.observer[0];
	UT_ASSERT_EQ(cluster_control_root_v3_serving_poll(&serving_open, serving_descriptor, &token),
				 0);
	UT_ASSERT_EQ(test_actual_cf, NoLock);
	UT_ASSERT_EQ(token.file_txn_seq, root.header.file_txn_seq + 1);
	path_for(path, sizeof(path), CLUSTER_CONTROL_ROOT_REL_PATH);
	read_all_or_abort(path, after, sizeof(after));
	UT_ASSERT_EQ(
		cluster_control_root_v3_decode(after, sizeof(after), v2_storage, TEST_SYSID, &published),
		0);
	UT_ASSERT_EQ(published.header.v2.database_state, CLUSTER_CONTROL_ROOT_DATABASE_OPEN);
	UT_ASSERT_EQ(published.header.v2.serving[0], 9);
	UT_ASSERT(memcmp(root.bytes + 512, published.bytes + 512, 65536) == 0);
	UT_ASSERT_EQ(published.header.lineage_kind, root.header.lineage_kind);
	UT_ASSERT(
		memcmp(published.header.migration_round_sha256, root.header.migration_round_sha256, 32)
		== 0);
	writes = test_durable_rename_calls;
	for (unsigned node = 0; node <= 3; node += 3) {
		cluster_node_id = node;
		test_self_incarnation = test_membership_incarnation = test_reserve_cut.observer[node];
		UT_ASSERT_EQ(cluster_control_root_v3_serving_poll(&serving_open, serving_descriptor, &peer),
					 0);
		UT_ASSERT(memcmp(&token, &peer, sizeof(token)) == 0);
		UT_ASSERT_EQ(test_durable_rename_calls, writes);
		UT_ASSERT_EQ(
			cluster_control_root_v3_stop_phase_read(node + 1, test_self_incarnation, &stop), 0);
		UT_ASSERT_EQ(stop.phase, CLUSTER_CONTROL_ROOT_STOP_ACTIVE);
		UT_ASSERT_EQ(stop.members[0].phase, CLUSTER_CONTROL_ROOT_STOP_ACTIVE);
		UT_ASSERT_EQ(stop.members[3].phase, CLUSTER_CONTROL_ROOT_STOP_ACTIVE);
	}
	test_reserve_mode = false;
	cluster_node_id = 0;
}

UT_TEST(test_serving_refuses_changed_or_unqualified_live_cut)
{
	for (unsigned fault = 0; fault < 14; fault++) {
		uint8 before[66048], descriptor[CLUSTER_UNDO_ROOT_DESCRIPTOR_BYTES];
		ControlRootImage root;
		ClusterControlRootFileToken out;
		ClusterSemanticActivationRecord open;
		serving_fixture(before, &root);
		if (ut_current_failed)
			return;
		open = serving_open;
		memcpy(descriptor, serving_descriptor, sizeof(descriptor));
		switch (fault) {
		case 0:
			serving_match = CLUSTER_NORMAL_STOP_PENDING;
			break;
		case 1:
			serving_match = CLUSTER_NORMAL_STOP_INVALID;
			break;
		case 2:
			open.record_generation++;
			break;
		case 3:
			descriptor[100] ^= 1;
			break;
		case 4:
			test_reserve_formation.startup_formation_generation = 0;
			break;
		case 5:
			test_reserve_formation.self_join_admitted = 0;
			break;
		case 6:
			test_reserve_formation.local_epoch++;
			break;
		case 7:
			test_reserve_formation.pending_join_bitmap[0] = 2;
			break;
		case 8:
			test_reserve_formation.excluded_bitmap[0] = 8;
			break;
		case 9:
			test_reserve_formation.membership.last_admitted_incarnation[3]++;
			break;
		case 10:
			test_reserve_formation.membership.membership_state[1] = CLUSTER_MEMBER_MEMBER;
			break;
		case 11:
			test_reserve_quorum = false;
			break;
		case 12:
			test_fence = false;
			break;
		case 13:
			MyBackendType = B_STARTUP;
			break;
		}
		memset(&out, 0xff, sizeof(out));
		UT_ASSERT(cluster_control_root_v3_serving_poll(&open, descriptor, &out) != 0);
		UT_ASSERT(v2_zero(&out, sizeof(out)));
		UT_ASSERT_EQ(test_actual_cf, NoLock);
		v2_assert_primary_unchanged(before);
	}
	test_reserve_mode = false;
}

UT_TEST(test_serving_missing_inputs_never_publish)
{
	for (unsigned fault = 0; fault < 4; fault++) {
		uint8 before[66048];
		ControlRootImage root;
		ClusterControlRootFileToken out;
		char path[MAXPGPATH], moved[MAXPGPATH], hex[65];
		serving_fixture_installed(before, &root, fault != 3);
		if (ut_current_failed)
			return;
		if (fault == 0)
			v2_claim_path(&root.records[3].identity, path);
		if (fault == 1) {
			for (unsigned i = 0; i < 32; i++)
				snprintf(hex + i * 2, 3, "%02x", root.refs[3].anchor_sha256[i]);
			snprintf(path, sizeof(path),
					 "%s/global/anchor_images/thread_4/generation_" UINT64_FORMAT
					 "/anchor_" UINT64_FORMAT "-%s.bin",
					 test_root, root.records[3].identity.origin_owner_incarnation,
					 root.refs[3].anchor_generation, hex);
		}
		if (fault == 2) {
			for (unsigned i = 0; i < 32; i++)
				snprintf(hex + i * 2, 3, "%02x", root.header.v2.config_sha256[i]);
			snprintf(path, sizeof(path), "%s/global/config_images/1-%s.conf", test_root, hex);
		}
		if (fault < 3) {
			snprintf(moved, sizeof(moved), "%s.serving-test", path);
			UT_ASSERT_EQ(rename(path, moved), 0);
		}
		UT_ASSERT(cluster_control_root_v3_serving_poll(&serving_open, serving_descriptor, &out)
				  != 0);
		UT_ASSERT(v2_zero(&out, sizeof(out)));
		UT_ASSERT_EQ(test_actual_cf, NoLock);
		v2_assert_primary_unchanged(before);
		if (fault < 3)
			UT_ASSERT_EQ(rename(moved, path), 0);
	}
	test_reserve_mode = false;
}

UT_TEST(test_serving_pending_peer_installs_before_same_cut_publication)
{
	uint8 before[66048], after[66048];
	ControlRootImage root;
	ClusterControlRootFileToken out;
	ClusterWalStartupImage pending;
	ClusterWalSourceRef writer;
	char path[MAXPGPATH];

	serving_fixture_installed(before, &root, false);
	if (ut_current_failed)
		return;
	UT_ASSERT(root.startup[3].generation != 0);
	UT_ASSERT(root.records[3].identity.origin_owner_incarnation
			  != test_reserve_cut.observer[3]);
	memset(&out, 0xff, sizeof(out));
	UT_ASSERT_EQ(cluster_control_root_v3_serving_poll(&serving_open, serving_descriptor, &out),
				 CLUSTER_CONTROL_ROOT_RECONFIG_WAIT);
	UT_ASSERT(v2_zero(&out, sizeof(out)));
	UT_ASSERT_EQ(test_actual_cf, NoLock);
	v2_assert_primary_unchanged(before);

	/* Consume the actual pending checkpoint through its original INSTALL
	 * owner. Do not fabricate an installed ROOT to complete the test. */
	(void)v3_driver_restart(&root, 3);
	MyBackendType = B_STARTUP;
	test_cf_mode = test_actual_cf = ShareLock;
	UT_ASSERT_EQ(cluster_wal_startup_read_locked(&root, 3, &pending), 0);
	test_cf_mode = test_actual_cf = NoLock;
	UT_ASSERT_EQ(pending.phase, CLUSTER_WAL_STARTUP_DURABLE);
	UT_ASSERT_EQ(cluster_control_root_v3_startup_install_writer(&pending, &writer), 0);
	path_for(path, sizeof(path), CLUSTER_CONTROL_ROOT_REL_PATH);
	read_all_or_abort(path, after, sizeof(after));
	UT_ASSERT_EQ(cluster_control_root_v3_decode(after, sizeof(after), v2_storage, TEST_SYSID,
											  &root), 0);
	UT_ASSERT_EQ(root.startup[3].generation, 0);
	UT_ASSERT_EQ(root.records[3].identity.origin_owner_incarnation,
				 test_reserve_cut.observer[3]);
	cluster_node_id = 0;
	test_self_incarnation = test_membership_incarnation = test_reserve_cut.observer[0];
	MyBackendType = B_LMON;
	UT_ASSERT_EQ(cluster_control_root_v3_serving_poll(&serving_open, serving_descriptor, &out), 0);
	UT_ASSERT(!v2_zero(&out, sizeof(out)));
	UT_ASSERT_EQ(test_actual_cf, NoLock);

	/* A current formation with a different boot and no pending INSTALL is
	 * a contradiction, not another lawful installation wait. */
	test_reserve_cut.observer[3]++;
	test_reserve_formation.membership.last_admitted_incarnation[3]++;
	read_all_or_abort(path, before, sizeof(before));
	UT_ASSERT_EQ(cluster_control_root_v3_serving_poll(&serving_open, serving_descriptor, &out),
				 CLUSTER_CONTROL_ROOT_IDENTITY_MISMATCH);
	UT_ASSERT(v2_zero(&out, sizeof(out)));
	UT_ASSERT_EQ(test_actual_cf, NoLock);
	v2_assert_primary_unchanged(before);
	test_reserve_mode = false;
}

UT_TEST(test_serving_waits_for_own_release_and_revalidates_cut)
{
	for (unsigned fault = 0; fault < 4; fault++) {
		uint8 before[66048];
		ControlRootImage root;
		ClusterControlRootFileToken out;
		int writes;
		serving_fixture(before, &root);
		if (ut_current_failed)
			return;
		test_cf_release_confirmed = false;
		UT_ASSERT_EQ(cluster_control_root_v3_serving_poll(&serving_open, serving_descriptor, &out),
					 CLUSTER_CONTROL_ROOT_LOCK_UNAVAILABLE);
		UT_ASSERT(v2_zero(&out, sizeof(out)));
		UT_ASSERT_EQ(test_actual_cf, ExclusiveLock);
		writes = test_durable_rename_calls;
		UT_ASSERT_EQ(cluster_control_root_v3_serving_poll(&serving_open, serving_descriptor, &out),
					 CLUSTER_CONTROL_ROOT_LOCK_UNAVAILABLE);
		UT_ASSERT(v2_zero(&out, sizeof(out)));
		if (fault == 1)
			serving_open.record_generation++;
		if (fault == 2)
			test_reserve_formation.startup_formation_generation++;
		if (fault == 3)
			test_control_generation++;
		test_cf_release_confirmed = true;
		if (fault == 0) {
			UT_ASSERT_EQ(
				cluster_control_root_v3_serving_poll(&serving_open, serving_descriptor, &out), 0);
			UT_ASSERT_EQ(out.file_txn_seq, root.header.file_txn_seq + 1);
		} else {
			UT_ASSERT(cluster_control_root_v3_serving_poll(&serving_open, serving_descriptor, &out)
					  != 0);
			UT_ASSERT(v2_zero(&out, sizeof(out)));
		}
		UT_ASSERT_EQ(test_actual_cf, NoLock);
		UT_ASSERT_EQ(test_durable_rename_calls, writes);
	}
	test_reserve_mode = false;
}

static void
serving_corrupt_postread(void)
{
	char path[MAXPGPATH];
	uint8 byte = 0xff;
	int fd;
	test_checkpoint_published_hook = NULL;
	path_for(path, sizeof(path), CLUSTER_CONTROL_ROOT_REL_PATH);
	fd = open(path, O_RDWR);
	UT_ASSERT(fd >= 0);
	UT_ASSERT_EQ(pwrite(fd, &byte, 1, 512), 1);
	UT_ASSERT_EQ(fsync(fd), 0);
	UT_ASSERT_EQ(close(fd), 0);
}

UT_TEST(test_serving_io_failure_and_visible_rename_are_not_durability)
{
	for (unsigned fault = 0; fault < 4; fault++) {
		uint8 before[66048];
		ControlRootImage root;
		ClusterControlRootFileToken out;
		serving_fixture(before, &root);
		if (ut_current_failed)
			return;
		if (fault == 0)
			test_fail_primary_rename = true;
		if (fault == 1)
			test_fail_after_primary_rename = true;
		if (fault == 2)
			test_fail_global_sync = true;
		if (fault == 3)
			test_checkpoint_published_hook = serving_corrupt_postread;
		UT_ASSERT_EQ(cluster_control_root_v3_serving_poll(&serving_open, serving_descriptor, &out),
					 CLUSTER_CONTROL_ROOT_IO_ERROR);
		UT_ASSERT(v2_zero(&out, sizeof(out)));
		UT_ASSERT_EQ(test_actual_cf, NoLock);
		if (fault == 0 || fault == 2)
			v2_assert_primary_unchanged(before);
		test_fail_primary_rename = test_fail_after_primary_rename = test_fail_global_sync = false;
		if (fault == 3) {
			/* No valid primary remains: retry must not choose .bak as OPEN. */
			UT_ASSERT(cluster_control_root_v3_serving_poll(&serving_open, serving_descriptor, &out)
					  != 0);
			UT_ASSERT(v2_zero(&out, sizeof(out)));
			continue;
		}
		if (fault == 1) {
			/* Primary rename is visible, but neither coordinator nor peer may
			 * turn it into an ACK while its directory still cannot be synced. */
			test_fail_global_sync = true;
			for (unsigned node = 0; node <= 3; node += 3) {
				cluster_node_id = node;
				test_self_incarnation = test_membership_incarnation
					= test_reserve_cut.observer[node];
				UT_ASSERT_EQ(
					cluster_control_root_v3_serving_poll(&serving_open, serving_descriptor, &out),
					CLUSTER_CONTROL_ROOT_IO_ERROR);
				UT_ASSERT(v2_zero(&out, sizeof(out)));
			}
			test_fail_global_sync = false;
		}
		cluster_node_id = 0;
		test_self_incarnation = test_membership_incarnation = test_reserve_cut.observer[0];
		UT_ASSERT_EQ(cluster_control_root_v3_serving_poll(&serving_open, serving_descriptor, &out),
					 0);
		UT_ASSERT_EQ(out.file_txn_seq, root.header.file_txn_seq + 1);
	}
	test_reserve_mode = false;
}

static void
serving_change_during_io(void)
{
	if (serving_match_calls == 2)
		test_reserve_formation.startup_formation_generation++;
}

UT_TEST(test_serving_fresh_cut_and_original_owner_error_cleanup)
{
	for (unsigned fault = 0; fault < 4; fault++) {
		uint8 before[66048];
		ControlRootImage root;
		ClusterControlRootFileToken out;
		bool caught = false;
		static const char different_duty;
		serving_fixture(before, &root);
		if (ut_current_failed)
			return;
		if (fault == 0)
			serving_match_hook = serving_change_during_io;
		if (fault == 1)
			test_checkpoint_x_hook = v2_checkpoint_throw_on_x;
		if (fault == 2) {
			UT_ASSERT(cluster_cf_lock_poll_owned(ExclusiveLock, &different_duty));
		}
		if (fault == 3)
			test_config_release_throw = true;
		PG_TRY();
		{
			UT_ASSERT(cluster_control_root_v3_serving_poll(&serving_open, serving_descriptor, &out)
					  != 0);
		}
		PG_CATCH();
		{
			caught = true;
		}
		PG_END_TRY();
		UT_ASSERT(caught == (fault == 1 || fault == 3));
		UT_ASSERT(v2_zero(&out, sizeof(out)));
		if (fault == 2) {
			cluster_control_root_v3_serving_cancel();
			UT_ASSERT(cluster_cf_held_by(ExclusiveLock, &different_duty));
			UT_ASSERT_EQ(cluster_cf_unlock_owned(ExclusiveLock, &different_duty),
						 CLUSTER_CF_RELEASE_CONFIRMED);
		}
		UT_ASSERT_EQ(test_actual_cf, NoLock);
		if (fault != 3)
			v2_assert_primary_unchanged(before);
		else {
			/* Publication survived, but ERROR returned no ACK. The original
			 * owner was retired and an exact later observation can finish. */
			UT_ASSERT_EQ(
				cluster_control_root_v3_serving_poll(&serving_open, serving_descriptor, &out), 0);
			UT_ASSERT_EQ(out.file_txn_seq, root.header.file_txn_seq + 1);
		}
		serving_match_hook = NULL;
		test_checkpoint_x_hook = NULL;
	}
	test_reserve_mode = false;
}

static void
serving_shutdown_owner(const ControlRootImage *root, unsigned node)
{
	const ClusterControlRootSnapshot *record = &root->records[node];
	cluster_node_id = node;
	test_own_thread = node + 1;
	test_self_incarnation = test_membership_incarnation = record->identity.origin_owner_incarnation;
	MyBackendType = B_CHECKPOINTER;
	MyAuxProcType = CheckpointerProcess;
	ShutdownRequestPending = true;
	test_checkpoint_prefix_ref.claim.identity = record->identity;
	test_checkpoint_prefix_ref.claim.database_incarnation = root->header.v2.database_incarnation;
	test_checkpoint_prefix_ref.claim.max_config_generation = root->header.v2.config_generation;
	memcpy(test_checkpoint_prefix_ref.claim.claim_sha256, root->refs[node].claim_sha256, 32);
	test_checkpoint_prefix_ref.timeline = record->checkpoint_tli;
	snprintf(test_checkpoint_prefix_path, sizeof(test_checkpoint_prefix_path),
			 "%s/thread_%u/generation_" UINT64_FORMAT "/durable_prefix/current",
			 cluster_wal_threads_dir, node + 1, record->identity.origin_owner_incarnation);
	test_flush_tli = record->checkpoint_tli;
	test_flush = test_insert = record->validated_tail_lsn_exclusive;
	test_checkpoint_end = test_flush;
	test_checkpoint_crc = record->checkpoint_record_crc32c;
	test_checkpoint_prefix = (WalTestRecord){ 11, test_flush, record->tail_last_record_lsn,
											  record->tail_last_record_crc32c };
	v2_checkpoint_prefix_write();
}

/* Append a real shutdown record after the real startup record without
 * truncating its full-sized segment or replacing the earlier WAL bytes. */
static void
serving_append_shutdown(const ControlRootImage *root, unsigned node, ControlFileData *candidate)
{
	const ClusterControlRootSnapshot *prior = &root->records[node];
	uint8 bytes[SizeOfXLogRecord + 2 + sizeof(CheckPoint)] = { 0 };
	XLogRecord record = { 0 };
	char path[MAXPGPATH];
	int fd;
	candidate->state = DB_SHUTDOWNED;
	candidate->checkPoint = prior->validated_tail_lsn_exclusive;
	candidate->checkPointCopy.redo = candidate->checkPoint;
	candidate->checkPointCopy.time++;
	candidate->time++;
	INIT_CRC32C(candidate->crc);
	COMP_CRC32C(candidate->crc, candidate, offsetof(ControlFileData, crc));
	FIN_CRC32C(candidate->crc);
	record.xl_tot_len = sizeof(bytes);
	record.xl_prev = prior->tail_last_record_lsn;
	record.xl_rmid = RM_XLOG_ID;
	record.xl_info = XLOG_CHECKPOINT_SHUTDOWN;
	bytes[SizeOfXLogRecord] = XLR_BLOCK_ID_DATA_SHORT;
	bytes[SizeOfXLogRecord + 1] = sizeof(CheckPoint);
	memcpy(bytes + SizeOfXLogRecord + 2, &candidate->checkPointCopy, sizeof(CheckPoint));
	INIT_CRC32C(record.xl_crc);
	COMP_CRC32C(record.xl_crc, bytes + SizeOfXLogRecord, sizeof(bytes) - SizeOfXLogRecord);
	COMP_CRC32C(record.xl_crc, &record, offsetof(XLogRecord, xl_crc));
	FIN_CRC32C(record.xl_crc);
	memcpy(bytes, &record, SizeOfXLogRecord);
	UT_ASSERT(candidate->checkPoint % XLOG_BLCKSZ + sizeof(bytes) < XLOG_BLCKSZ);
	v2_checkpoint_wal_path(&prior->identity, candidate->checkPoint, prior->checkpoint_tli, path);
	fd = open(path, O_RDWR);
	UT_ASSERT(fd >= 0);
	UT_ASSERT_EQ(pwrite(fd, bytes, sizeof(bytes), candidate->checkPoint % wal_segment_size),
				 sizeof(bytes));
	UT_ASSERT_EQ(fsync(fd), 0);
	UT_ASSERT_EQ(close(fd), 0);
	test_checkpoint_end = test_flush = test_insert
		= MAXALIGN(candidate->checkPoint + sizeof(bytes));
	test_checkpoint_crc = record.xl_crc;
	test_checkpoint_prefix
		= (WalTestRecord){ 12, test_flush, candidate->checkPoint, record.xl_crc };
	v2_checkpoint_prefix_write();
}

static void
serving_append_live_record(const ControlRootImage *root, unsigned node)
{
	const ClusterControlRootSnapshot *prior = &root->records[node];
	XLogRecord record = { 0 };
	char path[MAXPGPATH];
	int fd;

	record.xl_tot_len = SizeOfXLogRecord;
	record.xl_prev = prior->tail_last_record_lsn;
	record.xl_rmid = RM_XLOG_ID;
	record.xl_info = XLOG_NOOP;
	INIT_CRC32C(record.xl_crc);
	COMP_CRC32C(record.xl_crc, &record, offsetof(XLogRecord, xl_crc));
	FIN_CRC32C(record.xl_crc);
	UT_ASSERT(prior->validated_tail_lsn_exclusive % XLOG_BLCKSZ + sizeof(record) < XLOG_BLCKSZ);
	v2_checkpoint_wal_path(&prior->identity, prior->validated_tail_lsn_exclusive,
						   prior->checkpoint_tli, path);
	fd = open(path, O_RDWR);
	UT_ASSERT(fd >= 0);
	UT_ASSERT_EQ(
		pwrite(fd, &record, sizeof(record), prior->validated_tail_lsn_exclusive % wal_segment_size),
		sizeof(record));
	UT_ASSERT_EQ(fsync(fd), 0);
	UT_ASSERT_EQ(close(fd), 0);
}

UT_TEST(test_serving_startup_checkpoint_remains_active_after_live_wal)
{
	uint8 before[66048], original, corrupt;
	ControlRootImage root;
	ClusterControlRootFileToken token;
	ClusterControlRootStopObservation stop;
	char path[MAXPGPATH];
	int fd;

	serving_fixture(before, &root);
	if (ut_current_failed)
		return;
	UT_ASSERT_EQ(cluster_control_root_v3_serving_poll(&serving_open, serving_descriptor, &token),
				 0);
	/* An actual full-sized live stream has grown since the installed startup
	 * checkpoint. A peer's stop observation must still classify it ACTIVE. */
	serving_append_live_record(&root, 0);
	if (ut_current_failed)
		return;
	for (unsigned node = 0; node <= 3; node += 3) {
		cluster_node_id = node;
		test_self_incarnation = test_membership_incarnation = test_reserve_cut.observer[node];
		UT_ASSERT_EQ(
			cluster_control_root_v3_stop_phase_read(node + 1, test_self_incarnation, &stop), 0);
		UT_ASSERT_EQ(stop.phase, CLUSTER_CONTROL_ROOT_STOP_ACTIVE);
		UT_ASSERT_EQ(stop.members[0].phase, CLUSTER_CONTROL_ROOT_STOP_ACTIVE);
		UT_ASSERT_EQ(stop.members[3].phase, CLUSTER_CONTROL_ROOT_STOP_ACTIVE);
	}
	/* Prefix qualification must still decode the selected checkpoint and CRC. */
	v2_checkpoint_wal_path(&root.records[0].identity, root.records[0].tail_last_record_lsn,
						   root.records[0].checkpoint_tli, path);
	fd = open(path, O_RDWR);
	UT_ASSERT(fd >= 0);
	UT_ASSERT_EQ(pread(fd, &original, 1, root.records[0].tail_last_record_lsn % wal_segment_size),
				 1);
	corrupt = original ^ 1;
	UT_ASSERT_EQ(pwrite(fd, &corrupt, 1, root.records[0].tail_last_record_lsn % wal_segment_size),
				 1);
	UT_ASSERT_EQ(fsync(fd), 0);
	memset(&stop, 0x71, sizeof(stop));
	UT_ASSERT(cluster_control_root_v3_stop_phase_read(4, test_self_incarnation, &stop) != 0);
	UT_ASSERT(v2_zero(&stop, sizeof(stop)));
	UT_ASSERT_EQ(pwrite(fd, &original, 1, root.records[0].tail_last_record_lsn % wal_segment_size),
				 1);
	UT_ASSERT_EQ(fsync(fd), 0);
	UT_ASSERT_EQ(close(fd), 0);
	test_reserve_mode = false;
	cluster_node_id = 0;
}

UT_TEST(test_serving_shutdown_checkpoint_still_requires_terminal_wal)
{
	uint8 before[66048];
	ControlRootImage root, observed;
	ClusterControlRootFileToken token;
	ClusterControlRootSnapshot snapshot;
	ClusterControlRootStopObservation stop;
	ControlFileData candidate, view;

	serving_fixture(before, &root);
	if (ut_current_failed)
		return;
	UT_ASSERT_EQ(cluster_control_root_v3_serving_poll(&serving_open, serving_descriptor, &token),
				 0);
	serving_shutdown_owner(&root, 0);
	test_cf_mode = test_actual_cf = ShareLock;
	UT_ASSERT_EQ(cluster_control_root_v3_read_thread_locked(&root.records[0].identity, &observed,
															&candidate, &token),
				 0);
	test_cf_mode = test_actual_cf = NoLock;
	serving_append_shutdown(&root, 0, &candidate);
	UT_ASSERT_EQ(
		cluster_control_root_v3_shutdown_checkpoint_publish(
			&root.records[0].identity, &candidate, test_checkpoint_end, &snapshot, &token, &view),
		0);
	UT_ASSERT_EQ(cluster_control_root_v3_stop_phase_read(1, test_self_incarnation, &stop), 0);
	UT_ASSERT_EQ(stop.phase, CLUSTER_CONTROL_ROOT_STOP_CHECKPOINT);
	root.records[0] = snapshot;
	serving_append_live_record(&root, 0);
	if (ut_current_failed)
		return;
	memset(&stop, 0x71, sizeof(stop));
	UT_ASSERT(cluster_control_root_v3_stop_phase_read(1, test_self_incarnation, &stop) != 0);
	UT_ASSERT(v2_zero(&stop, sizeof(stop)));
	test_reserve_mode = false;
}

UT_TEST(test_serving_startup_anchor_cannot_close_the_new_writer)
{
	uint8 before[66048];
	ControlRootImage root;
	ClusterControlRootFileToken token;
	ClusterControlRootSnapshot snapshot;
	ClusterPhase1FullStopPlan plan = { 0 };
	bool complete = true;
	char path[MAXPGPATH];
	serving_fixture(before, &root);
	if (ut_current_failed)
		return;
	UT_ASSERT_EQ(cluster_control_root_v3_serving_poll(&serving_open, serving_descriptor, &token),
				 0);
	serving_shutdown_owner(&root, 0);
	plan.valid = plan.pre2_root_observed = test_close_owned = true;
	plan.epoch = test_epoch;
	plan.member_incarnations[0] = test_reserve_cut.observer[0];
	plan.member_incarnations[3] = test_reserve_cut.observer[3];
	plan.own_wal_started_at = root.records[0].identity.thread_claim_created_at;
	path_for(path, sizeof(path), CLUSTER_CONTROL_ROOT_REL_PATH);
	read_all_or_abort(path, before, sizeof(before));
	UT_ASSERT_EQ(
		cluster_control_root_v3_shutdown_observe(&test_checkpoint_prefix_ref, &snapshot, &token),
		CLUSTER_CONTROL_ROOT_LIFECYCLE_INVALID);
	UT_ASSERT(v2_zero(&snapshot, sizeof(snapshot)) && v2_zero(&token, sizeof(token)));
	UT_ASSERT_EQ(cluster_control_root_v3_normal_stop_close(&plan, &complete),
				 CLUSTER_CONTROL_ROOT_LIFECYCLE_INVALID);
	UT_ASSERT(!complete);
	v2_assert_primary_unchanged(before);
	UT_ASSERT_EQ(test_actual_cf, NoLock);
	test_reserve_mode = false;
}

UT_TEST(test_serving_real_shutdown_checkpoints_close_installed_cohort)
{
	uint8 before[66048];
	ControlRootImage root, observed;
	ClusterControlRootFileToken token;
	ClusterControlRootSnapshot snapshot;
	ClusterControlRootStopObservation stop;
	ClusterPhase1FullStopPlan plan = { 0 };
	ControlFileData candidate, view;
	bool complete = false;
	serving_fixture(before, &root);
	if (ut_current_failed)
		return;
	UT_ASSERT_EQ(cluster_control_root_v3_serving_poll(&serving_open, serving_descriptor, &token),
				 0);
	plan.valid = plan.pre2_root_observed = test_close_owned = true;
	plan.epoch = test_epoch;
	plan.member_incarnations[0] = test_reserve_cut.observer[0];
	plan.member_incarnations[3] = test_reserve_cut.observer[3];
	for (unsigned node = 0; node <= 3; node += 3) {
		serving_shutdown_owner(&root, node);
		test_cf_mode = test_actual_cf = ShareLock;
		UT_ASSERT_EQ(cluster_control_root_v3_read_thread_locked(&root.records[node].identity,
																&observed, &candidate, &token),
					 0);
		test_cf_mode = test_actual_cf = NoLock;
		serving_append_shutdown(&root, node, &candidate);
		UT_ASSERT_EQ(cluster_control_root_v3_shutdown_checkpoint_publish(
						 &root.records[node].identity, &candidate, test_checkpoint_end, &snapshot,
						 &token, &view),
					 0);
		if (ut_current_failed)
			return;
		UT_ASSERT_EQ(
			cluster_control_root_v3_stop_phase_read(node + 1, test_self_incarnation, &stop), 0);
		UT_ASSERT_EQ(stop.phase, CLUSTER_CONTROL_ROOT_STOP_CHECKPOINT);
		UT_ASSERT_EQ(stop.snapshot.lifecycle_reason,
					 CLUSTER_CONTROL_ROOT_PUBLISH_CHECKPOINT_ADVANCE);
		UT_ASSERT_EQ(stop.snapshot.checkpoint_lower_lsn, root.records[node].checkpoint_lower_lsn);
		UT_ASSERT_EQ(stop.snapshot.tail_last_record_lsn, candidate.checkPoint);
		UT_ASSERT_EQ(cluster_control_root_v3_shutdown_observe(&test_checkpoint_prefix_ref,
															  &snapshot, &token),
					 0);
		plan.own_wal_started_at = root.records[node].identity.thread_claim_created_at;
		plan.pre2_member_phase[node] = CLUSTER_CONTROL_ROOT_STOP_CHECKPOINT;
		UT_ASSERT_EQ(cluster_control_root_v3_normal_stop_close(&plan, &complete), 0);
		UT_ASSERT(complete == (node == 3));
		UT_ASSERT_EQ(test_actual_cf, NoLock);
		UT_ASSERT_EQ(test_walr_begin_calls, test_walr_end_calls);
	}
	UT_ASSERT_EQ(cluster_control_root_v3_stop_phase_read(4, test_self_incarnation, &stop), 0);
	UT_ASSERT_EQ(stop.members[0].phase, CLUSTER_CONTROL_ROOT_STOP_CLOSED);
	UT_ASSERT_EQ(stop.members[3].phase, CLUSTER_CONTROL_ROOT_STOP_CLOSED);
	cluster_node_id = 0;
	/* Native clean restart consumes that exact closed cohort and the exit
	 * owner's matching proof. It must not reuse the earlier OPEN generation. */
	{
		ClusterWalSourceRef restart, writer;
		ClusterWalStartupImage op, durable;
		char path[MAXPGPATH];
		uint64 previous_formation = root.header.v2.formation_seq;
		path_for(path, sizeof(path), CLUSTER_CONTROL_ROOT_REL_PATH);
		read_all_or_abort(path, before, sizeof(before));
		UT_ASSERT_EQ(
			cluster_control_root_v3_decode(before, sizeof(before), v2_storage, TEST_SYSID, &root),
			0);
		UT_ASSERT_EQ(root.header.v2.database_state, CLUSTER_CONTROL_ROOT_DATABASE_CLOSED);
		test_reserve_formation.local_epoch = ++test_epoch;
		test_reserve_formation.startup_formation_generation++;
		for (unsigned node = 0; node <= 3; node += 3)
			test_reserve_formation.membership.last_admitted_incarnation[node] += 1000;
		UT_ASSERT_EQ(cluster_control_root_v3_clean_exit_cut(before, sizeof(before), v2_storage,
															TEST_SYSID, &test_reserve_formation,
															&test_reserve_cut),
					 0);
		test_reserve_provider = true;
		test_reserve_evidence = CLUSTER_STARTUP_EXIT_READY;
		MyBackendType = B_STARTUP;
		ShutdownRequestPending = false;
		restart = v3_driver_restart(&root, 0);
		UT_ASSERT_EQ(cluster_control_root_v3_startup_advance_clean(&restart, &op),
					 CLUSTER_CONTROL_ROOT_RECONFIG_WAIT);
		UT_ASSERT_EQ(cluster_control_root_v3_startup_advance_clean(&restart, &op),
					 CLUSTER_CONTROL_ROOT_RECONFIG_WAIT);
		restart = v3_driver_restart(&root, 3);
		UT_ASSERT_EQ(cluster_control_root_v3_startup_advance_clean(&restart, &op),
					 CLUSTER_CONTROL_ROOT_RECONFIG_WAIT);
		restart = v3_driver_restart(&root, 0);
		UT_ASSERT_EQ(cluster_control_root_v3_startup_advance_clean(&restart, &op),
					 CLUSTER_CONTROL_ROOT_RECONFIG_WAIT);
		for (unsigned node = 0; node <= 3; node += 3) {
			restart = v3_driver_restart(&root, node);
			UT_ASSERT_EQ(cluster_control_root_v3_startup_advance_clean(&restart, &op), 0);
			if (ut_current_failed)
				return;
			test_cf_mode = test_actual_cf = ShareLock;
			UT_ASSERT_EQ(cluster_control_root_v3_read_thread_locked(
							 &op.predecessor.snapshot.identity, &observed, &candidate, &token),
						 0);
			test_cf_mode = test_actual_cf = NoLock;
			v3_startup_native_record(&op, &candidate);
			history_stage_dirs(node);
			UT_ASSERT_EQ(cluster_control_root_v3_startup_checkpoint(&op.claim.identity,
																	op.operation_uuid, &candidate,
																	test_checkpoint_end, &durable),
						 0);
			UT_ASSERT_EQ(cluster_control_root_v3_startup_install_writer(&durable, &writer), 0);
		}
		cluster_node_id = 0;
		test_self_incarnation = test_membership_incarnation = test_reserve_cut.observer[0];
		MyBackendType = B_LMON;
		/* The live semantic owner completed OPEN_APPLIED for the new boots;
		 * the durable OPEN record still belongs to the original generation. */
		serving_current_epoch = test_epoch;
		UT_ASSERT_EQ(serving_open.transition_epoch, test_epoch - 1);
		UT_ASSERT_EQ(serving_open.coordinator_incarnation, test_self_incarnation - 1000);
		UT_ASSERT_EQ(
			cluster_control_root_v3_serving_poll(&serving_open, serving_descriptor, &token), 0);
		test_cf_mode = test_actual_cf = ShareLock;
		UT_ASSERT_EQ(cluster_control_root_v3_read_control_locked(v2_storage, TEST_SYSID, &observed,
																 &view, &token),
					 0);
		test_cf_mode = test_actual_cf = NoLock;
		UT_ASSERT_EQ(observed.header.v2.database_state, CLUSTER_CONTROL_ROOT_DATABASE_OPEN);
		UT_ASSERT_EQ(observed.header.v2.formation_seq, previous_formation + 1);
		UT_ASSERT_EQ(observed.header.v2.serving[0], 9);
		UT_ASSERT_EQ(cluster_control_root_v3_stop_phase_read(1, test_self_incarnation, &stop), 0);
		UT_ASSERT_EQ(stop.phase, CLUSTER_CONTROL_ROOT_STOP_ACTIVE);
	}
	test_reserve_mode = false;
}

UT_TEST(test_serving_clean_restart_keeps_old_open_with_current_epoch)
{
	uint8 before[66048];
	ControlRootImage root;
	ClusterControlRootFileToken token;
	ClusterSemanticActivationRecord immutable;
	serving_fixture(before, &root);
	if (ut_current_failed)
		return;
	serving_open.transition_epoch = test_epoch - 1;
	immutable = serving_open;
	UT_ASSERT_EQ(cluster_control_root_v3_serving_poll(&serving_open, serving_descriptor, &token),
				 0);
	UT_ASSERT(memcmp(&immutable, &serving_open, sizeof(immutable)) == 0);
	UT_ASSERT_EQ(token.file_txn_seq, root.header.file_txn_seq + 1);
	UT_ASSERT_EQ(test_actual_cf, NoLock);
	test_reserve_mode = false;
}

UT_TEST(test_serving_current_epoch_is_required_before_any_publication)
{
	for (unsigned bad = 0; bad < 4; bad++) {
		uint8 before[66048];
		ControlRootImage root;
		ClusterControlRootFileToken token;
		int writes;
		serving_fixture(before, &root);
		if (ut_current_failed)
			return;
		writes = test_durable_rename_calls;
		if (bad == 0)
			serving_epoch_result = CLUSTER_NORMAL_STOP_PENDING;
		if (bad == 1)
			serving_epoch_result = CLUSTER_NORMAL_STOP_INVALID;
		if (bad == 2)
			serving_current_epoch--;
		if (bad == 3)
			serving_current_epoch++;
		UT_ASSERT(cluster_control_root_v3_serving_poll(&serving_open, serving_descriptor, &token)
				  != 0);
		UT_ASSERT_EQ(test_durable_rename_calls, writes);
		UT_ASSERT(v2_zero(&token, sizeof(token)));
		v2_assert_primary_unchanged(before);
	}
	test_reserve_mode = false;
}
