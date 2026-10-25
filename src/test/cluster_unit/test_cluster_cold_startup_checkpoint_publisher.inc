/* Actual native issuance tracker and Checkpointer capture/finish feed the
 * original Startup consumer. ROOT/WAL visits and execution leaves remain
 * controlled; this does not qualify a native durable checkpoint or base. */
#include "test_cluster_cold_checkpoint_publisher.h"
static TestColdCheckpointPublisherResult checkpoint_publisher_result[6];

static ClusterColdTypedV1 *
checkpoint_publisher_fixture(uint32 sampled, uint64 native_next, uint64 post_issued, unsigned fault)
{
	ClusterColdTypedV1 *typed;

	fixture();
	history_generation(1, 8, 0x40, 0x80);
	history_generation(2, 9, 0x40, 0x80);
	history_generation(2, 10, 0x80, 0xc0);
	for (uint32 i = 1; i <= NROOTS; i++) {
		native_checkpoint[i].checkpoint.PrevTimeLineID = 1;
		native_checkpoint[i].checkpoint.nextXid = FullTransactionIdFromU64(native_next);
	}
	for (uint32 p = 0; p < 6; p++) {
		ClusterWalRecoveryCheckpointV1 checkpoint = { 0 };
		uint8 *bytes;

		if (p < 3) {
			UT_ASSERT_EQ(cluster_control_root_recovery_checkpoint_v1(&roots[p + 1], &tokens[p + 1],
																	 &checkpoint),
						 CLUSTER_CONTROL_ROOT_OK_PRIMARY);
			bytes = native_checkpoint[p + 1].native_window;
		} else {
			const ClusterWalInputV1 *input;

			census_refresh_inputs();
			input = &census_inputs[11 + p - 3];
			checkpoint.source = input->source;
			checkpoint.checkpoint_start = input->checkpoint_start;
			checkpoint.checkpoint = input->native_checkpoint;
			bytes = history_native_window[p - 3];
		}
		checkpoint_publisher_result[p] = test_cold_checkpoint_publish(
			&checkpoint.source, &checkpoint.checkpoint, checkpoint.checkpoint_start,
			checkpoint.checkpoint_start + 0x10, p == sampled ? post_issued : native_next - 1,
			p == sampled && fault < 8 ? fault : 0, bytes);
		UT_ASSERT_EQ(checkpoint_publisher_result[p].assertion_failed, 0);
		UT_ASSERT_EQ(checkpoint_publisher_result[p].locks_remaining, 0);
		UT_ASSERT_EQ(checkpoint_publisher_result[p].interrupt_errors, 0);
	}
	if (fault == 8)
		memcpy(history_native_window[2], native_checkpoint[1].native_window, 160);
	if (fault == 9)
		native_checkpoint[3].checkpoint.nextOid++;
	typed = prepare(0x800);
	UT_ASSERT_EQ(typed->refusal, CLUSTER_COLD_OK);
	cluster_cold_replay_window_enter_v1();
	UT_ASSERT(
		cluster_cold_typed_sync_sources_v1(typed, (ClusterRecoveryFencePlan *)&fixture_context));
	memset(endpoint_progress, 0, sizeof(endpoint_progress));
	memset(&endpoint_result, 0, sizeof(endpoint_result));
	endpoint_result.steps_done = cluster_cold_plan_step_count_v1(typed->plan);
	endpoint_result.space_relations_finished
		= cluster_cold_plan_space_relation_count_v1(typed->plan);
	for (uint32 p = 0; p < typed->participant_count; p++) {
		ClusterControlRootSnapshot root;
		ClusterWalRecoveryCheckpointV1 checkpoint;
		ClusterColdParticipantV1 source = typed->participants[p];
		ClusterColdXidProgressV1 *xid
			= p < typed->replay_count ? &endpoint_progress[p] : &typed->history_xid[p];
		ClusterColdRecordV1 record = { 0 };
		ClusterColdXidInputV1 input = { .complete = true };
		uint64 next;

		UT_ASSERT(cluster_cold_typed_checkpoint_v1(typed, p, &root, &checkpoint));
		source.native_redo = checkpoint.checkpoint.redo;
		UT_ASSERT(cluster_cold_xid_begin_v1(xid, &source, native_next, 1));
		record.read_rec_ptr = root.tail_last_record_lsn;
		record.end_rec_ptr = root.validated_tail_lsn_exclusive;
		record.record_crc = root.tail_last_record_crc32c;
		UT_ASSERT(cluster_cold_xid_record_v1(xid, &source, &record, &input));
		UT_ASSERT(cluster_cold_xid_candidate_v1(xid, &source, &next));
		if (p < typed->replay_count) {
			typed->xid_next_at_drain[p] = next;
			endpoint_result.last_read[p] = record.read_rec_ptr;
			endpoint_result.last_end[p] = record.end_rec_ptr;
			endpoint_result.last_crc[p] = record.record_crc;
		}
	}
	endpoint_result.own_read = endpoint_result.last_read[0];
	endpoint_result.own_end = endpoint_result.last_end[0];
	return typed;
}

UT_TEST(test_original_checkpointer_issued_window_reaches_every_startup_endpoint)
{
	for (uint32 sampled = 0; sampled < 6; sampled++) {
		ClusterColdTypedV1 *typed = checkpoint_publisher_fixture(sampled, UINT64_C(0x100000100),
																 UINT64_C(0x100001000), 0);
		uint64 maximum = 0;

		for (uint32 p = 0; p < 6; p++) {
			UT_ASSERT(checkpoint_publisher_result[p].captured);
			UT_ASSERT(checkpoint_publisher_result[p].finished);
			UT_ASSERT_EQ(checkpoint_publisher_result[p].before_issued, UINT64_C(0x1000000ff));
			UT_ASSERT_EQ(checkpoint_publisher_result[p].after_issued,
						 p == sampled ? UINT64_C(0x100001000) : UINT64_C(0x1000000ff));
		}
		UT_ASSERT(endpoint_finish(typed));
		UT_ASSERT(cluster_cold_typed_xid_maximum_v1(typed, &maximum));
		UT_ASSERT_EQ(maximum, UINT64_C(0x100001001));
		UT_ASSERT_EQ(typed->xid_receipt.next_xid[sampled], UINT64_C(0x100001001));
		UT_ASSERT_EQ(checkpoint_publisher_result[sampled].allocation_next, UINT64_C(0x100002000));
		UT_ASSERT(maximum < checkpoint_publisher_result[sampled].allocation_next);
		UT_ASSERT_EQ(endpoint_progress[0].next_xid, UINT64_C(0x100000100));
		endpoint_destroy(&typed);
	}
	{
		ClusterColdTypedV1 *typed
			= checkpoint_publisher_fixture(5, UINT64_C(0x1ffffff00), UINT64_C(0x1ffffffff), 0);

		UT_ASSERT(endpoint_finish(typed));
		UT_ASSERT_EQ(typed->xid_next_after_redo, UINT64_C(0x200000003));
		endpoint_destroy(&typed);
	}
}

UT_TEST(test_unknown_original_checkpointer_does_not_invent_a_later_endpoint)
{
	for (unsigned fault = 1; fault < 8; fault++) {
		ClusterColdTypedV1 *typed
			= checkpoint_publisher_fixture(5, UINT64_C(0x100000100), UINT64_C(0x100001000), fault);
		ClusterControlRootSnapshot root;
		ClusterWalRecoveryCheckpointV1 checkpoint;
		uint8 zero[160] = { 0 };

		UT_ASSERT_EQ(checkpoint_publisher_result[5].captured, fault >= 3);
		UT_ASSERT(!checkpoint_publisher_result[5].finished);
		UT_ASSERT(cluster_cold_typed_checkpoint_v1(typed, 5, &root, &checkpoint));
		UT_ASSERT(memcmp(checkpoint.native_window, zero, 160) == 0);
		/* Legacyzero still folds its original checkpoint/WAL only; it is not
		 * a native/base/lifetime or recovery-completion qualification. */
		UT_ASSERT(endpoint_finish(typed));
		UT_ASSERT_EQ(typed->xid_receipt.next_xid[5], UINT64_C(0x100000100));
		UT_ASSERT_EQ(typed->xid_next_after_redo, UINT64_C(0x100000100));
		endpoint_destroy(&typed);
	}
}

UT_TEST(test_original_publisher_window_cannot_move_to_another_source_or_checkpoint)
{
	for (unsigned fault = 8; fault <= 9; fault++) {
		ClusterColdTypedV1 *typed
			= checkpoint_publisher_fixture(5, UINT64_C(0x100000100), UINT64_C(0x100001000), fault);
		uint64 maximum = UINT64_MAX;

		UT_ASSERT(checkpoint_publisher_result[5].finished);
		typed->effects_receipt.plan = typed->plan;
		UT_ASSERT(!endpoint_finish(typed));
		UT_ASSERT_EQ(typed->refusal, CLUSTER_COLD_SOURCE_GAP);
		UT_ASSERT_NULL(typed->xid_receipt.plan);
		UT_ASSERT_NULL(typed->effects_receipt.plan);
		UT_ASSERT(!typed->census_root_captured);
		UT_ASSERT(!cluster_cold_typed_xid_maximum_v1(typed, &maximum));
		UT_ASSERT_EQ(maximum, 0);
		endpoint_destroy(&typed);
		UT_ASSERT_EQ(contexts_alive, 0);
	}
}
