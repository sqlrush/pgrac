/*-------------------------------------------------------------------------
 * test_cluster_relmap_retained_producer.inc
 *     Original v2 stage dispatches to explicit retained transaction capture.
 *
 * Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 * IDENTIFICATION
 *     src/test/cluster_unit/test_cluster_relmap_retained_producer.inc
 * NOTES
 *     Actual producer/owner/writer/runtime; transaction allocation is controlled.
 *-------------------------------------------------------------------------
 */
UT_TEST(retained_stage_uses_native_writer_identity_before_mapping_and_wal)
{
	for (unsigned shared = 0; shared < 2; shared++) {
		ClusterRelmapRetentionDirectory *directory = v2_fixture(false, false);
		ClusterRelmapOwnerSampleV2 owner;
		ClusterRelmapV2Image image;

		UT_ASSERT(cluster_relmap_producer_stage_v2(shared, &updates, &owner, &image));
		UT_ASSERT_EQ(retained_requests, 1);
		UT_ASSERT_EQ(full_requests, 1);
		UT_ASSERT(retained_writer_reads > 0);
		UT_ASSERT(xid_order < lock_order && lock_order < wal_order && wal_order < stage_order);
		UT_ASSERT_EQ(xid_under_lock | xid_in_crit | raw_requests, 0);
		UT_ASSERT_EQ(image.pending_owner.top_full_xid, U64FromFullTransactionId(next_xid));
		UT_ASSERT(directory->pins[1].occupied && directory->pins[1].may_be_staged);
		UT_ASSERT_EQ(directory->pins[1].key.top_full_xid, image.pending_owner.top_full_xid);
		UT_ASSERT_EQ(directory->pins[1].wal_end, image.pending_owner.wal_end);
		UT_ASSERT(authority_held && !local_held);
	}
}

UT_TEST(retained_stage_missing_writer_never_assigns_or_stages)
{
	ClusterRelmapRetentionDirectory *directory = v2_fixture(false, false);
	ClusterRelmapRetentionDirectory saved = *directory;
	ClusterRelmapOwnerSampleV2 owner, old_owner;
	ClusterRelmapV2Image image, old_image;

	memset(&owner, 0x5a, sizeof(owner)); old_owner = owner;
	memset(&image, 0x3c, sizeof(image)); old_image = image;
	writer_available = false;
	UT_ASSERT(!cluster_relmap_producer_stage_v2(true, &updates, &owner, &image));
	UT_ASSERT_EQ(assignments | full_requests | retained_requests | inserts | local_acquires, 0);
	UT_ASSERT_EQ(memcmp(directory, &saved, sizeof(saved)), 0);
	UT_ASSERT_EQ(memcmp(&owner, &old_owner, sizeof(owner)), 0);
	UT_ASSERT_EQ(memcmp(&image, &old_image, sizeof(image)), 0);
	UT_ASSERT(authority_held && !local_held);
}

UT_TEST(retained_stage_allocator_error_preserves_outputs_and_pins)
{
	ClusterRelmapRetentionDirectory *directory = v2_fixture(false, false);
	ClusterRelmapRetentionDirectory saved = *directory;
	ClusterRelmapOwnerSampleV2 owner, old_owner;
	ClusterRelmapV2Image image, old_image;

	memset(&owner, 0x5a, sizeof(owner)); old_owner = owner;
	memset(&image, 0x3c, sizeof(image)); old_image = image;
	retained_assignment_error = true;
	UT_ASSERT(v2_stage_raises(&owner, &image));
	UT_ASSERT_EQ(error_level, ERROR);
	UT_ASSERT_EQ(retained_requests, 1);
	UT_ASSERT_EQ(assignments | inserts | local_acquires | v2_pair_reads, 0);
	UT_ASSERT_EQ(memcmp(directory, &saved, sizeof(saved)), 0);
	UT_ASSERT_EQ(memcmp(&owner, &old_owner, sizeof(owner)), 0);
	UT_ASSERT_EQ(memcmp(&image, &old_image, sizeof(image)), 0);
	UT_ASSERT(authority_held && !local_held);
}

UT_TEST(retained_stage_writer_drift_keeps_issuance_without_stage)
{
	ClusterRelmapRetentionDirectory *directory = v2_fixture(false, false);
	ClusterRelmapRetentionDirectory saved = *directory;
	ClusterRelmapOwnerSampleV2 owner, old_owner;
	ClusterRelmapV2Image image, old_image;

	memset(&owner, 0x5a, sizeof(owner)); old_owner = owner;
	memset(&image, 0x3c, sizeof(image)); old_image = image;
	retained_assignment_drift = true;
	UT_ASSERT(!cluster_relmap_producer_stage_v2(false, &updates, &owner, &image));
	UT_ASSERT_EQ(retained_requests, 1);
	UT_ASSERT_EQ(assignments, 1);
	UT_ASSERT_EQ(U64FromFullTransactionId(assigned_xid), U64FromFullTransactionId(next_xid));
	UT_ASSERT_EQ(inserts | local_acquires | v2_pair_reads | v2_stage_writes, 0);
	UT_ASSERT_EQ(memcmp(directory, &saved, sizeof(saved)), 0);
	UT_ASSERT_EQ(memcmp(&owner, &old_owner, sizeof(owner)), 0);
	UT_ASSERT_EQ(memcmp(&image, &old_image, sizeof(image)), 0);
	UT_ASSERT(authority_held && !local_held);
}
