/* Original founder advances its own native successor while every other
 * old cache-complete input remains selected and its pending writer RESERVED.
 * Native claim and writer counters are fixture leaves; ROOT/WAL are real.
 * Author: SqlRush <sqlrush@gmail.com> */
static void
recovered_founder_pending(uint8 before[66048], ControlRootImage *root,
						  ClusterControlRootFileToken *token, ClusterWalStartupImage ops[2],
						  unsigned founder, bool all)
{
	ClusterControlRootIdentity self;
	ClusterControlRootFileToken expected;
	ControlFileData common;

	v3_recovered_reserve_fixture(before, root, &self, &expected, all);
	if (ut_current_failed)
		return;
	(void)v3_recovered_driver_restart(root, founder);
	self = root->records[founder].identity;
	self.origin_owner_incarnation = test_self_incarnation;
	test_merge_claim_node = founder;
	UT_ASSERT_EQ(cluster_control_root_v3_reserve_recovered(&self, &expected, token), 0);
	if (ut_current_failed)
		return;
	test_walr_recovered_strict = false;
	test_actual_cf = test_cf_mode = ShareLock;
	UT_ASSERT_EQ(
		cluster_control_root_v3_read_control_locked(v2_storage, TEST_SYSID, root, &common, token),
		0);
	UT_ASSERT_EQ(cluster_wal_startup_read_locked(root, 0, &ops[0]), 0);
	UT_ASSERT_EQ(cluster_wal_startup_read_locked(root, 3, &ops[1]), 0);
	test_actual_cf = test_cf_mode = NoLock;
	memcpy(before, root->bytes, 66048);
}

static void
recovered_founder_running(uint8 before[66048], ControlRootImage *root,
						  ClusterWalStartupImage ops[2], unsigned founder, bool all,
						  ControlFileData *candidate)
{
	ClusterControlRootFileToken token, advanced;
	ClusterWalStartupImage out;
	unsigned i = founder == 0 ? 0 : 1;

	recovered_founder_pending(before, root, &token, ops, founder, all);
	if (ut_current_failed)
		return;
	UT_ASSERT_EQ(cluster_control_root_v3_startup_prepare_target(&ops[i].claim.identity,
																ops[i].operation_uuid, &out),
				 0);
	UT_ASSERT_EQ(cluster_control_root_v3_startup_begin_clean(
					 &ops[i].claim.identity, ops[i].operation_uuid, &token, &advanced),
				 0);
	if (ut_current_failed)
		return;
	test_actual_cf = test_cf_mode = ShareLock;
	UT_ASSERT_EQ(cluster_control_root_v3_read_thread_locked(&ops[i].predecessor.snapshot.identity,
															root, candidate, &token),
				 0);
	UT_ASSERT_EQ(cluster_wal_startup_read_locked(root, founder, &ops[i]), 0);
	test_actual_cf = test_cf_mode = NoLock;
	memcpy(before, root->bytes, 66048);
	v3_startup_native_record(&ops[i], candidate);
	history_stage_dirs(founder);
}

static void
recovered_founder_peer_unchanged(const ControlRootImage *before, const ControlRootImage *after,
								 const ClusterWalStartupImage *peer)
{
	unsigned node = peer->claim.identity.origin_node_id;
	ClusterWalStartupImage observed;
	char path[MAXPGPATH];
	struct stat st;

	UT_ASSERT_EQ(after->header.v2.configured[0], 9);
	UT_ASSERT_EQ(after->header.v2.configured[1], 0);
	UT_ASSERT_EQ(after->header.v2.database_state, CLUSTER_CONTROL_ROOT_DATABASE_MOUNTED);
	UT_ASSERT(v2_zero(after->header.v2.serving, sizeof(after->header.v2.serving)));
	UT_ASSERT(memcmp(&before->records[node], &after->records[node], sizeof(before->records[node]))
			  == 0);
	UT_ASSERT(memcmp(&before->refs[node], &after->refs[node], sizeof(before->refs[node])) == 0);
	UT_ASSERT(memcmp(&before->startup[node], &after->startup[node], sizeof(before->startup[node]))
			  == 0);
	test_actual_cf = test_cf_mode = ShareLock;
	UT_ASSERT_EQ(cluster_wal_startup_read_locked(after, node, &observed), 0);
	test_actual_cf = test_cf_mode = NoLock;
	UT_ASSERT(memcmp(&observed, peer, sizeof(observed)) == 0);
	UT_ASSERT_EQ(observed.phase, CLUSTER_WAL_STARTUP_RESERVED);
	v3_target_path(path, peer, "");
	UT_ASSERT(lstat(path, &st) < 0 && errno == ENOENT);
}

UT_TEST(test_recovered_founder_begin_preserves_unprepared_peer)
{
	for (unsigned variant = 0; variant < 3; variant++) {
		uint8 before[66048];
		ControlRootImage root, selected;
		ClusterControlRootFileToken token, advanced;
		ClusterWalStartupImage ops[2], out;
		unsigned i = variant == 0 ? 0 : 1, peer = 1 - i;

		recovered_founder_pending(before, &root, &token, ops, i * 3, variant == 2);
		if (ut_current_failed)
			goto done;
		UT_ASSERT_EQ(cluster_control_root_v3_startup_prepare_target(&ops[i].claim.identity,
																	ops[i].operation_uuid, &out),
					 0);
		UT_ASSERT_EQ(cluster_control_root_v3_startup_begin_clean(
						 &ops[i].claim.identity, ops[i].operation_uuid, &token, &advanced),
					 0);
		if (ut_current_failed)
			goto done;
		recovered_owner_read(&selected);
		UT_ASSERT_EQ(advanced.file_txn_seq, token.file_txn_seq + 1);
		recovered_founder_peer_unchanged(&root, &selected, &ops[peer]);
		UT_ASSERT_EQ(cluster_control_root_v3_startup_read_writer(&ops[i].claim.identity,
																 ops[i].operation_uuid, &out),
					 0);
		UT_ASSERT_EQ(out.phase, CLUSTER_WAL_STARTUP_INITIALIZING);
		UT_ASSERT_EQ(out.input_kind, ops[i].input_kind);
		UT_ASSERT(memcmp(&out.predecessor, &ops[i].predecessor, sizeof(out.predecessor)) == 0);
	done:
		cluster_node_id = 0;
		test_reserve_mode = false;
		if (ut_current_failed)
			return;
	}
}

UT_TEST(test_recovered_founder_driver_needs_only_own_target)
{
	uint8 before[66048];
	ControlRootImage root, selected;
	ClusterControlRootFileToken token;
	ClusterWalStartupImage ops[2], out;
	ClusterWalSourceRef restart;

	recovered_founder_pending(before, &root, &token, ops, 3, false);
	restart = v3_recovered_driver_restart(&root, 3);
	UT_ASSERT_EQ(cluster_control_root_v3_startup_advance_clean(&restart, &out),
				 CLUSTER_CONTROL_ROOT_RECONFIG_WAIT);
	UT_ASSERT(v2_zero(&out, sizeof(out)));
	recovered_owner_read(&selected);
	UT_ASSERT_EQ(selected.header.file_txn_seq, token.file_txn_seq + 1);
	if (ut_current_failed)
		goto done;
	recovered_founder_peer_unchanged(&root, &selected, &ops[0]);
	UT_ASSERT_EQ(cluster_control_root_v3_startup_advance_clean(&restart, &out), 0);
	UT_ASSERT_EQ(out.phase, CLUSTER_WAL_STARTUP_INITIALIZING);
	memcpy(before, selected.bytes, sizeof(before));
	restart = v3_recovered_driver_restart(&root, 0);
	UT_ASSERT_EQ(cluster_control_root_v3_startup_advance_clean(&restart, &out),
				 CLUSTER_CONTROL_ROOT_RECONFIG_WAIT);
	UT_ASSERT(v2_zero(&out, sizeof(out)));
	v2_assert_primary_unchanged(before);
	recovered_founder_peer_unchanged(&root, &selected, &ops[0]);
done:
	cluster_node_id = 0;
	test_reserve_mode = false;
}

UT_TEST(test_recovered_founder_native_eor_install_keeps_full_old_input)
{
	for (unsigned variant = 0; variant < 3; variant++) {
		uint8 before[66048];
		ControlRootImage root, selected;
		ControlFileData candidate;
		ClusterWalStartupImage ops[2], durable;
		ClusterWalSourceRef writer;
		ClusterWalHistoryImage history;
		unsigned i = variant == 0 ? 0 : 1, node = i * 3;

		recovered_founder_running(before, &root, ops, node, variant == 2, &candidate);
		if (ut_current_failed)
			goto done;
		UT_ASSERT_EQ(cluster_control_root_v3_startup_checkpoint(&ops[i].claim.identity,
																ops[i].operation_uuid, &candidate,
																test_checkpoint_end, &durable),
					 0);
		if (ut_current_failed)
			goto done;
		UT_ASSERT_EQ(durable.phase, CLUSTER_WAL_STARTUP_DURABLE);
		UT_ASSERT_EQ(cluster_control_root_v3_startup_install_writer(&durable, &writer), 0);
		if (ut_current_failed)
			goto done;
		recovered_owner_read(&selected);
		recovered_founder_peer_unchanged(&root, &selected, &ops[1 - i]);
		UT_ASSERT(v2_zero(&selected.startup[node], sizeof(selected.startup[node])));
		UT_ASSERT(memcmp(&selected.records[node], &durable.successor.snapshot,
						 sizeof(selected.records[node]))
				  == 0);
		UT_ASSERT(
			cluster_control_root_identity_equal(&writer.claim.identity, &ops[i].claim.identity));
		test_actual_cf = test_cf_mode = ShareLock;
		UT_ASSERT_EQ(cluster_wal_history_read_locked(&selected, node, &history), 0);
		test_actual_cf = test_cf_mode = NoLock;
		UT_ASSERT_EQ(history.count, 1);
		UT_ASSERT(memcmp(&history.records[0].snapshot, &ops[i].predecessor.snapshot,
						 sizeof(ops[i].predecessor.snapshot))
				  == 0);
		memcpy(before, selected.bytes, sizeof(before));
		UT_ASSERT_EQ(cluster_control_root_v3_startup_install_writer(&durable, &writer), 0);
		v2_assert_primary_unchanged(before);
		UT_ASSERT(cluster_recovery_merge_claim_owner_v1() != 0);
		/* A selected COMPLETE enum cannot replace its actual retained WAL,
         * including on an already-installed retry. Keep the immutable ROOT. */
		{
			const ClusterControlRootSnapshot *old = &ops[i].predecessor.snapshot;
			char path[MAXPGPATH];
			uint8 byte;
			off_t offset
				= old->tail_last_record_lsn % wal_segment_size + offsetof(XLogRecord, xl_crc);
			int fd;
			v2_checkpoint_wal_path(&old->identity, old->tail_last_record_lsn, old->tail_tli, path);
			fd = open(path, O_RDWR);
			UT_ASSERT(fd >= 0);
			UT_ASSERT_EQ(pread(fd, &byte, 1, offset), 1);
			byte ^= 1;
			UT_ASSERT_EQ(pwrite(fd, &byte, 1, offset), 1);
			UT_ASSERT_EQ(close(fd), 0);
			memset(&writer, 0xa5, sizeof(writer));
			UT_ASSERT_NE(cluster_control_root_v3_startup_install_writer(&durable, &writer), 0);
			UT_ASSERT(v2_zero(&writer, sizeof(writer)));
			v2_assert_primary_unchanged(before);
		}
	done:
		test_actual_cf = test_cf_mode = NoLock;
		cluster_node_id = 0;
		test_reserve_mode = false;
		if (ut_current_failed)
			return;
	}
}

UT_TEST(test_recovered_founder_writer_requires_original_claim)
{
	for (unsigned fault = 0; fault < 3; fault++) {
		uint8 before[66048];
		ControlRootImage root;
		ControlFileData candidate;
		ClusterWalStartupImage ops[2], out;

		recovered_founder_running(before, &root, ops, 3, false, &candidate);
		if (ut_current_failed)
			goto done;
		if (fault == 0)
			test_merge_claim_custody = false;
		if (fault == 1)
			test_merge_claim_node = 0;
		if (fault == 2)
			test_merge_claim_serial = 0;
		memset(&out, 0xa5, sizeof(out));
		UT_ASSERT_NE(cluster_control_root_v3_startup_read_writer(&ops[1].claim.identity,
																 ops[1].operation_uuid, &out),
					 0);
		UT_ASSERT(v2_zero(&out, sizeof(out)));
		v2_assert_primary_unchanged(before);
	done:
		cluster_node_id = 0;
		test_reserve_mode = false;
		if (ut_current_failed)
			return;
	}
}

static void
recovered_founder_replace_claim(void)
{
	recovered_owner_replace_claim();
	test_checkpoint_x_hook = NULL;
}

UT_TEST(test_recovered_founder_checkpoint_withholds_after_claim_drift)
{
	for (unsigned after = 0; after < 2; after++) {
		uint8 before[66048];
		ControlRootImage root, selected;
		ControlFileData candidate;
		ClusterWalStartupImage ops[2], out;

		recovered_founder_running(before, &root, ops, 3, false, &candidate);
		if (ut_current_failed)
			goto done;
		if (after)
			test_checkpoint_published_hook = recovered_founder_replace_claim;
		else
			test_checkpoint_x_hook = recovered_founder_replace_claim;
		memset(&out, 0xa5, sizeof(out));
		UT_ASSERT_NE(cluster_control_root_v3_startup_checkpoint(&ops[1].claim.identity,
																ops[1].operation_uuid, &candidate,
																test_checkpoint_end, &out),
					 0);
		UT_ASSERT(v2_zero(&out, sizeof(out)));
		UT_ASSERT(test_checkpoint_x_hook == NULL && test_checkpoint_published_hook == NULL);
		recovered_owner_read(&selected);
		recovered_founder_peer_unchanged(&root, &selected, &ops[0]);
		if (!after)
			v2_assert_primary_unchanged(before);
		else
			UT_ASSERT_EQ(selected.header.file_txn_seq, root.header.file_txn_seq + 1);
	done:
		test_checkpoint_x_hook = test_checkpoint_published_hook = NULL;
		cluster_node_id = 0;
		test_reserve_mode = false;
		if (ut_current_failed)
			return;
	}
}

UT_TEST(test_recovered_founder_install_withholds_after_claim_drift)
{
	for (unsigned after = 0; after < 2; after++) {
		uint8 before[66048];
		ControlRootImage root, selected;
		ControlFileData candidate;
		ClusterWalStartupImage ops[2], durable;
		ClusterWalSourceRef writer;

		recovered_founder_running(before, &root, ops, 3, false, &candidate);
		if (ut_current_failed)
			goto done;
		UT_ASSERT_EQ(cluster_control_root_v3_startup_checkpoint(&ops[1].claim.identity,
																ops[1].operation_uuid, &candidate,
																test_checkpoint_end, &durable),
					 0);
		if (ut_current_failed)
			goto done;
		recovered_owner_read(&root);
		memcpy(before, root.bytes, sizeof(before));
		if (after)
			test_checkpoint_published_hook = recovered_founder_replace_claim;
		else
			test_checkpoint_x_hook = recovered_founder_replace_claim;
		memset(&writer, 0xa5, sizeof(writer));
		UT_ASSERT_NE(cluster_control_root_v3_startup_install_writer(&durable, &writer), 0);
		UT_ASSERT(v2_zero(&writer, sizeof(writer)));
		UT_ASSERT(test_checkpoint_x_hook == NULL && test_checkpoint_published_hook == NULL);
		recovered_owner_read(&selected);
		recovered_founder_peer_unchanged(&root, &selected, &ops[0]);
		if (!after)
			v2_assert_primary_unchanged(before);
		else
			UT_ASSERT_EQ(selected.header.file_txn_seq, root.header.file_txn_seq + 1);
	done:
		test_checkpoint_x_hook = test_checkpoint_published_hook = NULL;
		cluster_node_id = 0;
		test_reserve_mode = false;
		if (ut_current_failed)
			return;
	}
}

UT_TEST(test_recovered_founder_checkpoint_rechecks_every_peer)
{
	for (unsigned fault = 0; fault < 4; fault++) {
		uint8 before[66048];
		ControlRootImage root;
		ControlFileData candidate;
		ClusterWalStartupImage ops[2], out;

		recovered_founder_running(before, &root, ops, 3, false, &candidate);
		if (ut_current_failed)
			goto done;
		if (fault == 0)
			test_reserve_formation.membership.last_admitted_incarnation[0]++;
		if (fault == 1)
			test_fence = false;
		if (fault == 2)
			test_reserve_formation.membership.membership_state[0] = CLUSTER_MEMBER_DEAD;
		if (fault == 3)
			test_reserve_formation.pending_join_bitmap[0] = 1;
		memset(&out, 0xa5, sizeof(out));
		UT_ASSERT_NE(cluster_control_root_v3_startup_checkpoint(&ops[1].claim.identity,
																ops[1].operation_uuid, &candidate,
																test_checkpoint_end, &out),
					 0);
		UT_ASSERT(v2_zero(&out, sizeof(out)));
		v2_assert_primary_unchanged(before);
	done:
		cluster_node_id = 0;
		test_reserve_mode = false;
		if (ut_current_failed)
			return;
	}
}

UT_TEST(test_recovered_founder_begin_requires_own_durable_target)
{
	uint8 before[66048];
	ControlRootImage root;
	ClusterControlRootFileToken token, advanced;
	ClusterWalStartupImage ops[2];

	recovered_founder_pending(before, &root, &token, ops, 3, false);
	if (ut_current_failed)
		goto done;
	memset(&advanced, 0xa5, sizeof(advanced));
	UT_ASSERT_NE(cluster_control_root_v3_startup_begin_clean(
					 &ops[1].claim.identity, ops[1].operation_uuid, &token, &advanced),
				 0);
	UT_ASSERT(v2_zero(&advanced, sizeof(advanced)));
	v2_assert_primary_unchanged(before);
done:
	cluster_node_id = 0;
	test_reserve_mode = false;
}
