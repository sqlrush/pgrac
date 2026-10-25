/* The original cold recoverer, including a nonlowest CLOSED member in a
 * mixed cohort, coordinates its existing successor transitions. This tests
 * the real ROOT/WAL readers and publishers; native claim authority is a leaf.
 * Author: SqlRush <sqlrush@gmail.com> */
static void
recovered_owner_read(ControlRootImage *root)
{
	char path[MAXPGPATH];
	uint8 bytes[CLUSTER_CONTROL_ROOT_FILE_BYTES];

	path_for(path, sizeof(path), CLUSTER_CONTROL_ROOT_REL_PATH);
	read_all_or_abort(path, bytes, sizeof(bytes));
	UT_ASSERT_EQ(cluster_control_root_v3_decode(bytes, sizeof(bytes), v2_storage,
											 TEST_SYSID, root), CLUSTER_CONTROL_ROOT_OK_PRIMARY);
}

static void
recovered_owner_replace_claim(void)
{
	test_merge_claim_serial++;
	test_startup_sync_hook = NULL;
	test_checkpoint_published_hook = NULL;
}

UT_TEST(test_recovered_reserve_uses_nonlowest_original_claim_owner)
{
	for (unsigned mixed = 0; mixed < 2; mixed++) {
		uint8 before[66048];
		ControlRootImage root, selected;
		ClusterControlRootIdentity self;
		ClusterControlRootFileToken expected, token;

		v3_recovered_reserve_fixture(before, &root, &self, &expected, mixed == 0);
		(void)v3_recovered_driver_restart(&root, 3);
		self = root.records[3].identity;
		self.origin_owner_incarnation = test_self_incarnation;
		test_merge_claim_node = 3;
		UT_ASSERT_EQ(cluster_control_root_v3_reserve_recovered(&self, &expected, &token), 0);
		if (ut_current_failed)
			goto done;
		recovered_owner_read(&selected);
		UT_ASSERT_EQ(selected.header.file_txn_seq, expected.file_txn_seq + 1);
		UT_ASSERT(v2_zero(selected.header.v2.serving, sizeof(selected.header.v2.serving)));
		for (unsigned node = 0; node <= 3; node += 3) {
			UT_ASSERT(selected.startup[node].generation != 0);
			UT_ASSERT(memcmp(&selected.records[node], &root.records[node],
							 sizeof(root.records[node])) == 0);
		}
		UT_ASSERT_EQ(test_actual_cf, NoLock);
		UT_ASSERT(!test_walr_owner_active);
done:
		cluster_node_id = 0;
		test_reserve_mode = false;
		if (ut_current_failed)
			return;
	}
}

UT_TEST(test_recovered_reserve_requires_native_claim_custody)
{
	for (unsigned fault = 0; fault < 3; fault++) {
		uint8 before[66048];
		ControlRootImage root;
		ClusterControlRootIdentity self;
		ClusterControlRootFileToken expected, token;

		v3_recovered_reserve_fixture(before, &root, &self, &expected, true);
		if (fault == 0)
			test_merge_claim_serial = 0;
		if (fault == 1)
			test_merge_claim_node = 3;
		if (fault == 2)
			test_merge_claim_custody = false;
		memset(&token, 0xa5, sizeof(token));
		UT_ASSERT_EQ(cluster_control_root_v3_reserve_recovered(&self, &expected, &token),
					 CLUSTER_CONTROL_ROOT_STALE_TOKEN);
		UT_ASSERT(v2_zero(&token, sizeof(token)));
		v2_assert_primary_unchanged(before);
		UT_ASSERT_EQ(test_walr_set_calls, 0);
		UT_ASSERT_EQ(test_actual_cf, NoLock);
		test_reserve_mode = false;
		if (ut_current_failed)
			return;
	}
}

UT_TEST(test_recovered_reserve_rejects_replaced_claim_before_cas)
{
	uint8 before[66048];
	ControlRootImage root;
	ClusterControlRootIdentity self;
	ClusterControlRootFileToken expected, token;

	v3_recovered_reserve_fixture(before, &root, &self, &expected, true);
	test_startup_sync_hook = recovered_owner_replace_claim;
	memset(&token, 0xa5, sizeof(token));
	UT_ASSERT_EQ(cluster_control_root_v3_reserve_recovered(&self, &expected, &token),
				 CLUSTER_CONTROL_ROOT_STALE_TOKEN);
	UT_ASSERT(test_startup_sync_hook == NULL);
	test_startup_sync_hook = NULL;
	UT_ASSERT(v2_zero(&token, sizeof(token)));
	v2_assert_primary_unchanged(before);
	UT_ASSERT(!test_walr_owner_active);
	UT_ASSERT_EQ(test_actual_cf, NoLock);
	test_reserve_mode = false;
}

UT_TEST(test_recovered_reserve_withholds_token_after_claim_loss)
{
	uint8 before[66048];
	ControlRootImage root, selected;
	ClusterControlRootIdentity self;
	ClusterControlRootFileToken expected, token;

	v3_recovered_reserve_fixture(before, &root, &self, &expected, true);
	test_checkpoint_published_hook = recovered_owner_replace_claim;
	memset(&token, 0xa5, sizeof(token));
	UT_ASSERT_EQ(cluster_control_root_v3_reserve_recovered(&self, &expected, &token),
				 CLUSTER_CONTROL_ROOT_STALE_TOKEN);
	UT_ASSERT(test_checkpoint_published_hook == NULL);
	test_checkpoint_published_hook = NULL;
	UT_ASSERT(v2_zero(&token, sizeof(token)));
	recovered_owner_read(&selected);
	UT_ASSERT_EQ(selected.header.file_txn_seq, expected.file_txn_seq + 1);
	UT_ASSERT(v2_zero(selected.header.v2.serving, sizeof(selected.header.v2.serving)));
	UT_ASSERT(!test_walr_owner_active);
	UT_ASSERT_EQ(test_actual_cf, NoLock);
	test_reserve_mode = false;
}

UT_TEST(test_recovered_native_driver_reserves_on_original_owner)
{
	for (unsigned mixed = 0; mixed < 2; mixed++) {
		uint8 before[66048];
		ControlRootImage root, selected;
		ClusterControlRootIdentity self;
		ClusterControlRootFileToken expected;
		ClusterWalStartupImage observed;
		ClusterWalSourceRef restart;

		v3_recovered_reserve_fixture(before, &root, &self, &expected, mixed == 0);
		test_merge_claim_node = 3;
		restart = v3_recovered_driver_restart(&root, 0);
		UT_ASSERT_EQ(cluster_control_root_v3_startup_advance_clean(&restart, &observed),
					 CLUSTER_CONTROL_ROOT_RECONFIG_WAIT);
		UT_ASSERT(v2_zero(&observed, sizeof(observed)));
		v2_assert_primary_unchanged(before);
		if (ut_current_failed)
			goto done;
		restart = v3_recovered_driver_restart(&root, 3);
		UT_ASSERT_EQ(cluster_control_root_v3_startup_advance_clean(&restart, &observed),
					 CLUSTER_CONTROL_ROOT_RECONFIG_WAIT);
		UT_ASSERT(v2_zero(&observed, sizeof(observed)));
		recovered_owner_read(&selected);
		UT_ASSERT_EQ(selected.header.file_txn_seq, expected.file_txn_seq + 1);
		UT_ASSERT(selected.startup[0].generation != 0);
		UT_ASSERT(selected.startup[3].generation != 0);
		UT_ASSERT(v2_zero(selected.header.v2.serving, sizeof(selected.header.v2.serving)));
done:
		cluster_node_id = 0;
		test_reserve_mode = false;
		if (ut_current_failed)
			return;
	}
}

/* Prepare both real targets with their respective Startup identities. This
 * does not grant follower admission: that production route is still pending. */
static void
recovered_owner_prepare_targets(const ClusterWalStartupImage ops[2])
{
	ClusterWalStartupImage observed;

	for (unsigned i = 0; i < 2; i++) {
		v3_recovered_begin_select(&ops[i]);
		UT_ASSERT_EQ(cluster_control_root_v3_startup_prepare_target(&ops[i].claim.identity,
																 ops[i].operation_uuid, &observed), 0);
		if (ut_current_failed)
			return;
	}
}

UT_TEST(test_recovered_native_driver_begins_on_original_owner)
{
	for (unsigned mixed = 0; mixed < 2; mixed++) {
		uint8 before[66048];
		ControlRootImage root, selected;
		ClusterControlRootFileToken expected;
		ClusterWalStartupImage ops[2], observed;
		ClusterWalSourceRef restart;

		v3_recovered_begin_fixture(before, &root, &expected, ops, mixed == 0);
		recovered_owner_prepare_targets(ops);
		if (ut_current_failed)
			goto done;
		test_merge_claim_node = 3;
		restart = v3_recovered_driver_restart(&root, 3);
		UT_ASSERT_EQ(cluster_control_root_v3_startup_advance_clean(&restart, &observed),
					 CLUSTER_CONTROL_ROOT_RECONFIG_WAIT);
		UT_ASSERT(v2_zero(&observed, sizeof(observed)));
		recovered_owner_read(&selected);
		UT_ASSERT_EQ(selected.header.file_txn_seq, expected.file_txn_seq + 1);
		UT_ASSERT_EQ(cluster_control_root_v3_startup_advance_clean(&restart, &observed), 0);
		UT_ASSERT_EQ(observed.phase, CLUSTER_WAL_STARTUP_INITIALIZING);
		UT_ASSERT_EQ(observed.input_kind, mixed ? CLUSTER_WAL_STARTUP_CLEAN
												  : CLUSTER_WAL_STARTUP_RECOVERED);
		UT_ASSERT(v2_zero(selected.header.v2.serving, sizeof(selected.header.v2.serving)));
done:
		cluster_node_id = 0;
		test_reserve_mode = false;
		if (ut_current_failed)
			return;
	}
}

UT_TEST(test_recovered_begin_lowest_node_cannot_replace_claim_owner)
{
	uint8 before[66048];
	ControlRootImage root;
	ClusterControlRootFileToken expected, token;
	ClusterWalStartupImage ops[2];

	v3_recovered_begin_fixture(before, &root, &expected, ops, true);
	recovered_owner_prepare_targets(ops);
	v3_recovered_begin_select(&ops[0]);
	test_merge_claim_node = 3;
	memset(&token, 0xa5, sizeof(token));
	UT_ASSERT_EQ(cluster_control_root_v3_startup_begin_clean(&ops[0].claim.identity,
														 ops[0].operation_uuid, &expected, &token),
				 CLUSTER_CONTROL_ROOT_STALE_TOKEN);
	UT_ASSERT(v2_zero(&token, sizeof(token)));
	v2_assert_primary_unchanged(before);
	UT_ASSERT_EQ(test_actual_cf, NoLock);
	test_reserve_mode = false;
}

UT_TEST(test_recovered_begin_revalidates_original_claim_at_publication)
{
	for (unsigned after = 0; after < 2; after++) {
		uint8 before[66048];
		ControlRootImage root, selected;
		ClusterControlRootFileToken expected, token;
		ClusterWalStartupImage ops[2];

		v3_recovered_begin_fixture(before, &root, &expected, ops, false);
		recovered_owner_prepare_targets(ops);
		v3_recovered_begin_select(&ops[0]);
		if (after)
			test_checkpoint_published_hook = recovered_owner_replace_claim;
		else
			test_startup_sync_hook = recovered_owner_replace_claim;
		memset(&token, 0xa5, sizeof(token));
		UT_ASSERT_EQ(cluster_control_root_v3_startup_begin_clean(&ops[0].claim.identity,
															 ops[0].operation_uuid, &expected, &token),
					 CLUSTER_CONTROL_ROOT_STALE_TOKEN);
		UT_ASSERT(test_startup_sync_hook == NULL && test_checkpoint_published_hook == NULL);
		test_startup_sync_hook = NULL;
		test_checkpoint_published_hook = NULL;
		UT_ASSERT(v2_zero(&token, sizeof(token)));
		if (after) {
			recovered_owner_read(&selected);
			UT_ASSERT_EQ(selected.header.file_txn_seq, expected.file_txn_seq + 1);
			UT_ASSERT(v2_zero(selected.header.v2.serving, sizeof(selected.header.v2.serving)));
		} else
			v2_assert_primary_unchanged(before);
		UT_ASSERT_EQ(test_actual_cf, NoLock);
		test_reserve_mode = false;
		if (ut_current_failed)
			return;
	}
}
