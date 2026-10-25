/* Actual issued-publish and Checkpointer capture/finish feed the original
 * immutable anchor I/O. Native WAL flush, ROOT publication and CF/owner
 * providers remain controlled; these objects alone grant no authority. */
#include "test_cluster_cold_checkpoint_publisher.h"

static TestColdCheckpointPublisherResult
anchor_publish_window(ClusterRecoveryAnchorV2 *anchor)
{
	ClusterWalSourceRef source = { 0 };
	uint64 next = U64FromFullTransactionId(anchor->checkpoint_copy.nextXid);

	source.claim.identity = anchor->identity;
	source.claim.database_incarnation = anchor->database_incarnation;
	source.claim.max_config_generation = anchor->config_generation;
	memcpy(source.claim.claim_sha256, anchor->claim_sha256, 32);
	source.timeline = anchor->checkpoint_copy.ThisTimeLineID;
	return test_cold_checkpoint_publish(&source, &anchor->checkpoint_copy, anchor->checkpoint,
										anchor->checkpoint + 64, next + 9, 0,
										anchor->native_window);
}

UT_TEST(test_original_checkpointer_window_survives_immutable_anchor_readback)
{
	ClusterRecoveryAnchorV2 anchor;
	ClusterRecoveryAnchorStageV2 stage;
	ClusterRelmapCheckpointWindow window;
	TestColdCheckpointPublisherResult published;
	ControlFileData common, control, saved_common;
	uint8 uuid[16], bytes[160];
	uint64 observed = 0, next;
	char final[MAXPGPATH], temp[MAXPGPATH];

	v2_stage_fixture(&anchor, uuid, &common);
	published = anchor_publish_window(&anchor);
	UT_ASSERT(published.captured && published.finished);
	UT_ASSERT_EQ(published.locks_remaining, 0);
	UT_ASSERT_EQ(published.interrupt_errors, 0);
	UT_ASSERT_EQ(published.assertion_failed, 0);
	next = U64FromFullTransactionId(anchor.checkpoint_copy.nextXid);
	UT_ASSERT_EQ(published.before_issued, next - 1);
	UT_ASSERT_EQ(published.after_issued, next + 9);
	UT_ASSERT_EQ(published.allocation_next, next + 9 + 0x1000);
	saved_common = common;
	UT_ASSERT_EQ(cluster_recovery_anchor_v2_prepare(&anchor, uuid, &stage), 0);
	v2_stage_paths(&stage, final, temp);
	UT_ASSERT_EQ(access(temp, F_OK), 0);
	UT_ASSERT(access(final, F_OK) != 0);
	test_cf_x_held = true;
	UT_ASSERT_EQ(cluster_recovery_anchor_v2_install(&stage), 0);
	UT_ASSERT_EQ(access(final, F_OK), 0);
	UT_ASSERT(access(temp, F_OK) != 0);
	UT_ASSERT_EQ(
		cluster_recovery_anchor_v3_read_native_window_locked(&stage.ref, &common, &control, bytes),
		0);
	UT_ASSERT(memcmp(bytes, anchor.native_window, sizeof(bytes)) == 0);
	UT_ASSERT(
		memcmp(&control.checkPointCopy, &anchor.checkpoint_copy, sizeof(anchor.checkpoint_copy))
		== 0);
	UT_ASSERT_EQ(control.checkPoint, anchor.checkpoint);
	UT_ASSERT_EQ(control.system_identifier, anchor.identity.system_identifier);
	UT_ASSERT(memcmp(&common, &saved_common, sizeof(common)) == 0);
	UT_ASSERT(cluster_relmap_checkpoint_window_decode_v2(
		bytes, &control.checkPointCopy, control.checkPoint, control.checkPoint + 64,
		&stage.ref.identity, stage.ref.database_incarnation, &window, &observed));
	UT_ASSERT_EQ(observed, published.after_issued);
	UT_ASSERT_EQ(window.last_issued, published.before_issued);
	UT_ASSERT_EQ(window.allocation_next, next);
	UT_ASSERT(observed < published.allocation_next);
	test_cf_x_held = false;
	UT_ASSERT_EQ(cluster_recovery_anchor_v2_discard(&stage), 0);
}

UT_TEST(test_original_checkpointer_anchor_prepare_fsync_failure_exposes_no_stage)
{
	for (int cut = 1; cut <= 2; cut++) {
		ClusterRecoveryAnchorV2 anchor;
		ClusterRecoveryAnchorStageV2 stage;
		TestColdCheckpointPublisherResult published;
		ControlFileData common;
		uint8 uuid[16];
		char final[MAXPGPATH], temp[MAXPGPATH];

		v2_stage_fixture(&anchor, uuid, &common);
		published = anchor_publish_window(&anchor);
		UT_ASSERT(published.captured && published.finished);
		UT_ASSERT_EQ(published.assertion_failed, 0);
		UT_ASSERT_EQ(cluster_recovery_anchor_v2_prepare(&anchor, uuid, &stage), 0);
		v2_stage_paths(&stage, final, temp);
		UT_ASSERT_EQ(cluster_recovery_anchor_v2_discard(&stage), 0);
		test_fsync_calls = 0;
		test_fsync_fail_at = cut;
		memset(&stage, 0xa5, sizeof(stage));
		UT_ASSERT_EQ(cluster_recovery_anchor_v2_prepare(&anchor, uuid, &stage),
					 CLUSTER_CONTROL_ROOT_IO_ERROR);
		UT_ASSERT(v2_zero(&stage, sizeof(stage)));
		UT_ASSERT(access(temp, F_OK) != 0);
		UT_ASSERT(access(final, F_OK) != 0);
		test_fsync_fail_at = 0;
	}
}

UT_TEST(test_original_checkpointer_anchor_refuses_rebinding_without_replacing_original)
{
	ClusterRecoveryAnchorV2 anchor, changed;
	ClusterRecoveryAnchorStageV2 original, refused;
	TestColdCheckpointPublisherResult published;
	ControlFileData common, control;
	uint8 uuid[16], expected[512], actual[512], window[160];
	char final[MAXPGPATH], temp[MAXPGPATH];
	int fd;

	v2_stage_fixture(&anchor, uuid, &common);
	published = anchor_publish_window(&anchor);
	UT_ASSERT(published.captured && published.finished);
	UT_ASSERT_EQ(published.assertion_failed, 0);
	UT_ASSERT_EQ(cluster_recovery_anchor_v2_encode(&anchor, expected), 0);
	UT_ASSERT_EQ(cluster_recovery_anchor_v2_prepare(&anchor, uuid, &original), 0);
	v2_stage_paths(&original, final, temp);
	test_cf_x_held = true;
	UT_ASSERT_EQ(cluster_recovery_anchor_v2_install(&original), 0);
	for (unsigned fault = 0; fault < 4; fault++) {
		changed = anchor;
		if (fault == 0)
			changed.checkpoint_copy.nextOid++;
		if (fault == 1)
			changed.database_incarnation++;
		if (fault == 2)
			changed.identity.origin_owner_incarnation++;
		if (fault == 3)
			changed.checkpoint++;
		test_fsync_calls = 0;
		memset(&refused, 0xa5, sizeof(refused));
		UT_ASSERT_EQ(cluster_recovery_anchor_v2_prepare(&changed, uuid, &refused),
					 CLUSTER_CONTROL_ROOT_BAD_BODY_CRC);
		UT_ASSERT(v2_zero(&refused, sizeof(refused)));
		UT_ASSERT_EQ(test_fsync_calls, 0);
		UT_ASSERT(access(temp, F_OK) != 0);
		fd = open(final, O_RDONLY);
		UT_ASSERT(fd >= 0);
		UT_ASSERT_EQ(read(fd, actual, sizeof(actual)), sizeof(actual));
		UT_ASSERT_EQ(close(fd), 0);
		UT_ASSERT(memcmp(actual, expected, sizeof(actual)) == 0);
		UT_ASSERT_EQ(cluster_recovery_anchor_v3_read_native_window_locked(&original.ref, &common,
																		  &control, window),
					 0);
		UT_ASSERT(memcmp(window, anchor.native_window, sizeof(window)) == 0);
		UT_ASSERT(
			memcmp(&control.checkPointCopy, &anchor.checkpoint_copy, sizeof(anchor.checkpoint_copy))
			== 0);
	}
	test_cf_x_held = false;
	UT_ASSERT_EQ(cluster_recovery_anchor_v2_discard(&original), 0);
}
