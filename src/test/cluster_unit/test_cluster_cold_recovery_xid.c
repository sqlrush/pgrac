/* Ordered cold counter candidates never authorize replay or transaction state. */
#include "postgres.h"
#include "cluster/cluster_cold_recovery.h"
#include "unit_test.h"

UT_DEFINE_GLOBALS();

void
ExceptionalCondition(const char *condition_name, const char *file_name, int line_number)
{
	printf("# unexpected Assert: %s at %s:%d\n", condition_name, file_name, line_number);
	abort();
}

static ClusterColdParticipantV1
source(int count)
{
	ClusterColdParticipantV1 s = { 0 };
	s.thread_id = 3;
	s.timeline = 7;
	s.owner_incarnation = 19;
	s.physical_lower = 64;
	s.native_redo = 128;
	s.tail_end = 128 + count * 32;
	return s;
}

static uint64
full(uint32 epoch, TransactionId xid)
{
	return ((uint64)epoch << 32) | xid;
}

static ClusterColdRecordV1
record(int index)
{
	ClusterColdRecordV1 r = { 0 };
	r.read_rec_ptr = 128 + index * 32;
	r.end_rec_ptr = r.read_rec_ptr + 32;
	r.prev_rec_ptr = index > 0 ? r.read_rec_ptr - 32 : 64;
	return r;
}

static bool
feed(ClusterColdXidProgressV1 *p, const ClusterColdParticipantV1 *s, int index,
	 TransactionId header, TransactionId payload, uint64 checkpoint, bool shutdown)
{
	ClusterColdRecordV1 r = record(index);
	ClusterColdXidInputV1 input = { 0 };
	input.complete = true;
	input.record_xid = header;
	input.payload_xid = payload;
	input.checkpoint_next_xid = checkpoint;
	input.checkpoint_shutdown = shutdown;
	return cluster_cold_xid_record_v1(p, s, &r, &input);
}

UT_TEST(test_empty_cut_retains_checkpoint)
{
	ClusterColdParticipantV1 s = source(0);
	ClusterColdXidProgressV1 p;
	uint64 result = 99;
	UT_ASSERT(cluster_cold_xid_begin_v1(&p, &s, full(8, 100), 0));
	UT_ASSERT(cluster_cold_xid_candidate_v1(&p, &s, &result));
	UT_ASSERT_EQ(result, full(8, 100));
	UT_ASSERT_EQ(p.records, 0);
}

UT_TEST(test_ordered_header_and_payload)
{
	ClusterColdParticipantV1 s = source(3);
	ClusterColdXidProgressV1 p;
	uint64 result = 99;
	UT_ASSERT(cluster_cold_xid_begin_v1(&p, &s, full(8, 100), 3));
	UT_ASSERT(feed(&p, &s, 0, 105, 110, 0, false));
	UT_ASSERT(feed(&p, &s, 1, 103, 104, 0, false));
	UT_ASSERT(feed(&p, &s, 2, 112, 111, 0, false));
	UT_ASSERT(cluster_cold_xid_candidate_v1(&p, &s, &result));
	UT_ASSERT_EQ(result, full(8, 113));
}

UT_TEST(test_wrap_skips_reserved_xids)
{
	ClusterColdParticipantV1 s = source(2);
	ClusterColdXidProgressV1 p;
	uint64 result;
	UT_ASSERT(cluster_cold_xid_begin_v1(&p, &s, full(7, UINT32_MAX - 1), 2));
	UT_ASSERT(feed(&p, &s, 0, UINT32_MAX, 0, 0, false));
	UT_ASSERT_EQ(p.next_xid, full(8, FirstNormalTransactionId));
	UT_ASSERT(feed(&p, &s, 1, 4, 9, 0, false));
	UT_ASSERT(cluster_cold_xid_candidate_v1(&p, &s, &result));
	UT_ASSERT_EQ(result, full(8, 10));
}

UT_TEST(test_native_special_header_behavior)
{
	ClusterColdParticipantV1 s = source(2);
	ClusterColdXidProgressV1 p;
	uint64 result;
	/* Native special-xid comparison is numeric, including near wrap. */
	UT_ASSERT(cluster_cold_xid_begin_v1(&p, &s, full(7, UINT32_MAX), 2));
	UT_ASSERT(feed(&p, &s, 0, InvalidTransactionId, 0, 0, false));
	UT_ASSERT(feed(&p, &s, 1, FrozenTransactionId, 0, 0, false));
	UT_ASSERT(cluster_cold_xid_candidate_v1(&p, &s, &result));
	UT_ASSERT_EQ(result, full(7, UINT32_MAX));
}

UT_TEST(test_online_checkpoint_is_full_width_minimum)
{
	ClusterColdParticipantV1 s = source(3);
	ClusterColdXidProgressV1 p;
	uint64 result;
	UT_ASSERT(cluster_cold_xid_begin_v1(&p, &s, full(7, 100), 3));
	UT_ASSERT(feed(&p, &s, 0, 110, 112, full(7, 105), false));
	UT_ASSERT_EQ(p.next_xid, full(7, 113));
	UT_ASSERT(feed(&p, &s, 1, 100, 0, full(8, 90), false));
	UT_ASSERT(feed(&p, &s, 2, 89, 0, full(7, 200), false));
	UT_ASSERT(cluster_cold_xid_candidate_v1(&p, &s, &result));
	UT_ASSERT_EQ(result, full(8, 90));
}

UT_TEST(test_shutdown_checkpoint_assigns_after_inputs)
{
	ClusterColdParticipantV1 s = source(2);
	ClusterColdXidProgressV1 p;
	uint64 result;
	UT_ASSERT(cluster_cold_xid_begin_v1(&p, &s, full(8, 100), 2));
	UT_ASSERT(feed(&p, &s, 0, 110, 120, full(7, 90), true));
	UT_ASSERT_EQ(p.next_xid, full(7, 90));
	UT_ASSERT(feed(&p, &s, 1, 100, 0, 0, false));
	UT_ASSERT(cluster_cold_xid_candidate_v1(&p, &s, &result));
	UT_ASSERT_EQ(result, full(7, 101));
}

UT_TEST(test_missing_record_cannot_return_candidate)
{
	ClusterColdParticipantV1 s = source(2);
	ClusterColdXidProgressV1 p;
	uint64 result = 99;
	UT_ASSERT(cluster_cold_xid_begin_v1(&p, &s, full(8, 100), 2));
	UT_ASSERT(feed(&p, &s, 0, 105, 0, 0, false));
	UT_ASSERT(!cluster_cold_xid_candidate_v1(&p, &s, &result));
	UT_ASSERT_EQ(result, 0);
	UT_ASSERT(feed(&p, &s, 1, 106, 0, 0, false));
	UT_ASSERT(cluster_cold_xid_candidate_v1(&p, &s, &result));
	UT_ASSERT_EQ(result, full(8, 107));
}

UT_TEST(test_source_identity_and_cut_are_exact)
{
	for (int i = 0; i < 7; i++) {
		ClusterColdParticipantV1 s = source(1), changed = s;
		ClusterColdXidProgressV1 p;
		uint64 result = 99;
		if (i == 0) changed.thread_id++;
		if (i == 1) changed.timeline++;
		if (i == 2) changed.owner_incarnation++;
		if (i == 3) changed.physical_lower++;
		if (i == 4) changed.native_redo++;
		if (i == 5) changed.tail_end++;
		if (i == 6) changed.reserved_zero++;
		UT_ASSERT(cluster_cold_xid_begin_v1(&p, &s, full(8, 100), 1));
		UT_ASSERT(!feed(&p, &changed, 0, 105, 0, 0, false));
		UT_ASSERT(!cluster_cold_xid_candidate_v1(&p, &s, &result));
		UT_ASSERT_EQ(result, 0);
		UT_ASSERT(!feed(&p, &s, 0, 105, 0, 0, false));
	}
}

UT_TEST(test_record_chain_and_ranges_fail_closed)
{
	for (int i = 0; i < 7; i++) {
		ClusterColdParticipantV1 s = source(2);
		ClusterColdXidProgressV1 p;
		ClusterColdRecordV1 r = record(1);
		ClusterColdXidInputV1 input = { .record_xid = 106, .complete = true };
		uint64 result = 99;
		UT_ASSERT(cluster_cold_xid_begin_v1(&p, &s, full(8, 100), 2));
		UT_ASSERT(feed(&p, &s, 0, 105, 0, 0, false));
		if (i == 0) r.prev_rec_ptr++;
		if (i == 1) r = record(0);
		if (i == 2) r.end_rec_ptr = r.read_rec_ptr;
		if (i == 3) r.end_rec_ptr++;
		if (i == 4) r.read_rec_ptr--;
		if (i == 5) r.reserved_zero = 1;
		if (i == 6) r.record_flags = 0x80;
		UT_ASSERT(!cluster_cold_xid_record_v1(&p, &s, &r, &input));
		UT_ASSERT(!cluster_cold_xid_candidate_v1(&p, &s, &result));
		UT_ASSERT_EQ(result, 0);
		UT_ASSERT(!feed(&p, &s, 1, 106, 0, 0, false));
	}
}

UT_TEST(test_refused_or_incomplete_inputs_poison_progress)
{
	for (int i = 0; i < 6; i++) {
		ClusterColdParticipantV1 s = source(1);
		ClusterColdXidProgressV1 p;
		ClusterColdRecordV1 r = record(0);
		ClusterColdXidInputV1 input = { .record_xid = 105, .complete = true };
		uint64 result = 99;
		UT_ASSERT(cluster_cold_xid_begin_v1(&p, &s, full(8, 100), 1));
		if (i == 0) input.complete = false;
		if (i == 1) r.record_flags = CLUSTER_COLD_RECORD_UNSUPPORTED;
		if (i == 2) r.record_flags = CLUSTER_COLD_RECORD_STRUCTURAL;
		if (i == 3) input.payload_xid = FrozenTransactionId;
		if (i == 4) input.checkpoint_next_xid = full(8, 1);
		if (i == 5) input.checkpoint_shutdown = true;
		UT_ASSERT(!cluster_cold_xid_record_v1(&p, &s, &r, &input));
		UT_ASSERT(!cluster_cold_xid_candidate_v1(&p, &s, &result));
		UT_ASSERT_EQ(result, 0);
	}
}

UT_TEST(test_unowned_effects_remain_only_counter_inputs)
{
	ClusterColdParticipantV1 s = source(1);
	ClusterColdXidProgressV1 p;
	ClusterColdRecordV1 r = record(0);
	ClusterColdXidInputV1 input = { .record_xid = 105, .payload_xid = 109, .complete = true };
	uint64 result;
	r.record_flags = CLUSTER_COLD_RECORD_SIDE_UNOWNED;
	UT_ASSERT(cluster_cold_xid_begin_v1(&p, &s, full(8, 100), 1));
	UT_ASSERT(cluster_cold_xid_record_v1(&p, &s, &r, &input));
	UT_ASSERT(cluster_cold_xid_candidate_v1(&p, &s, &result));
	UT_ASSERT_EQ(result, full(8, 110));
	UT_ASSERT_EQ(r.record_flags, CLUSTER_COLD_RECORD_SIDE_UNOWNED);
}

UT_TEST(test_arithmetic_ambiguity_and_overflow_refuse)
{
	for (int i = 0; i < 4; i++) {
		ClusterColdParticipantV1 s = source(1);
		ClusterColdXidProgressV1 p;
		uint64 result = 99;
		uint64 seed = (i == 0 || i == 3) ? full(8, 100) : full(UINT32_MAX, UINT32_MAX - 1);
		TransactionId header = i == 0 ? UINT32_C(0x80000064)
			: i == 2 ? UINT32_MAX : i == 3 ? 99 : UINT32_MAX - 2;
		TransactionId payload = i == 1 ? UINT32_MAX : i == 3 ? UINT32_C(0x80000064) : 0;
		UT_ASSERT(cluster_cold_xid_begin_v1(&p, &s, seed, 1));
		UT_ASSERT(!feed(&p, &s, 0, header, payload, 0, false));
		UT_ASSERT(!cluster_cold_xid_candidate_v1(&p, &s, &result));
		UT_ASSERT_EQ(result, 0);
	}
}

UT_TEST(test_extra_record_invalidates_completed_candidate)
{
	ClusterColdParticipantV1 s = source(1);
	ClusterColdXidProgressV1 p;
	uint64 result = 99;
	UT_ASSERT(cluster_cold_xid_begin_v1(&p, &s, full(8, 100), 1));
	UT_ASSERT(feed(&p, &s, 0, 105, 0, 0, false));
	UT_ASSERT(cluster_cold_xid_candidate_v1(&p, &s, &result));
	UT_ASSERT(!feed(&p, &s, 1, 106, 0, 0, false));
	UT_ASSERT(!cluster_cold_xid_candidate_v1(&p, &s, &result));
	UT_ASSERT_EQ(result, 0);
}

UT_TEST(test_count_and_tail_both_required)
{
	ClusterColdParticipantV1 s = source(2);
	ClusterColdXidProgressV1 p;
	uint64 result = 99;
	UT_ASSERT(cluster_cold_xid_begin_v1(&p, &s, full(8, 100), 1));
	UT_ASSERT(feed(&p, &s, 0, 105, 0, 0, false));
	UT_ASSERT(!cluster_cold_xid_candidate_v1(&p, &s, &result));
	UT_ASSERT_EQ(result, 0);
	UT_ASSERT(cluster_cold_xid_begin_v1(&p, &s, full(8, 100), 3));
	UT_ASSERT(feed(&p, &s, 0, 105, 0, 0, false));
	UT_ASSERT(feed(&p, &s, 1, 106, 0, 0, false));
	result = 99;
	UT_ASSERT(!cluster_cold_xid_candidate_v1(&p, &s, &result));
	UT_ASSERT_EQ(result, 0);
}

UT_TEST(test_invalid_seed_and_source_do_not_leave_old_result)
{
	for (int i = 0; i < 11; i++) {
		ClusterColdParticipantV1 s = source(0);
		ClusterColdXidProgressV1 p;
		uint64 result = 99, seed = full(8, 100);
		UT_ASSERT(cluster_cold_xid_begin_v1(&p, &s, seed, 0));
		if (i == 0) seed = 0;
		if (i == 1) seed = full(8, 1);
		if (i == 2) s.thread_id = 0;
		if (i == 3) s.thread_id = 129;
		if (i == 4) s.timeline = 0;
		if (i == 5) s.owner_incarnation = 0;
		if (i == 6) s.physical_lower = 0;
		if (i == 7) s.native_redo = 63;
		if (i == 8) s.tail_end = 127;
		if (i == 9) s.reserved_zero = 1;
		if (i == 10) s.tail_end++;
		UT_ASSERT(!cluster_cold_xid_begin_v1(&p, &s, seed, 0));
		UT_ASSERT(!cluster_cold_xid_candidate_v1(&p, &s, &result));
		UT_ASSERT_EQ(result, 0);
	}
}

UT_TEST(test_null_and_uninitialized_inputs)
{
	ClusterColdParticipantV1 s = source(1);
	ClusterColdXidProgressV1 p = { 0 };
	ClusterColdRecordV1 r = record(0);
	ClusterColdXidInputV1 input = { .complete = true };
	uint64 result = 99;
	UT_ASSERT(!cluster_cold_xid_candidate_v1(&p, &s, &result));
	UT_ASSERT_EQ(result, 0);
	UT_ASSERT(!cluster_cold_xid_begin_v1(NULL, &s, full(8, 100), 1));
	UT_ASSERT(!cluster_cold_xid_begin_v1(&p, NULL, full(8, 100), 1));
	UT_ASSERT(!cluster_cold_xid_record_v1(NULL, &s, &r, &input));
	UT_ASSERT(cluster_cold_xid_begin_v1(&p, &s, full(8, 100), 1));
	UT_ASSERT(!cluster_cold_xid_record_v1(&p, &s, &r, NULL));
	UT_ASSERT(!cluster_cold_xid_candidate_v1(&p, &s, &result));
	UT_ASSERT_EQ(result, 0);
	UT_ASSERT(!cluster_cold_xid_candidate_v1(&p, &s, NULL));
}

#include "test_cluster_cold_xid_replay.h"

int
main(void)
{
	UT_PLAN(24);
	UT_RUN(test_empty_cut_retains_checkpoint);
	UT_RUN(test_ordered_header_and_payload);
	UT_RUN(test_wrap_skips_reserved_xids);
	UT_RUN(test_native_special_header_behavior);
	UT_RUN(test_online_checkpoint_is_full_width_minimum);
	UT_RUN(test_shutdown_checkpoint_assigns_after_inputs);
	UT_RUN(test_missing_record_cannot_return_candidate);
	UT_RUN(test_source_identity_and_cut_are_exact);
	UT_RUN(test_record_chain_and_ranges_fail_closed);
	UT_RUN(test_refused_or_incomplete_inputs_poison_progress);
	UT_RUN(test_unowned_effects_remain_only_counter_inputs);
	UT_RUN(test_arithmetic_ambiguity_and_overflow_refuse);
	UT_RUN(test_extra_record_invalidates_completed_candidate);
	UT_RUN(test_count_and_tail_both_required);
	UT_RUN(test_invalid_seed_and_source_do_not_leave_old_result);
	UT_RUN(test_null_and_uninitialized_inputs);
	UT_RUN(test_replay_xid_original_seeds_and_reset);
	UT_RUN(test_replay_xid_native_read_fold_and_drain);
	UT_RUN(test_replay_xid_no_endpoint_without_original_drain);
	UT_RUN(test_replay_xid_count_and_consumed_identity);
	UT_RUN(test_replay_xid_bad_record_cannot_publish);
	UT_RUN(test_replay_xid_history_cut_is_not_checkpoint_coverage);
	UT_RUN(test_replay_xid_empty_exact_checkpoint_cut);
	UT_RUN(test_replay_xid_bad_checkpoint_or_source);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
