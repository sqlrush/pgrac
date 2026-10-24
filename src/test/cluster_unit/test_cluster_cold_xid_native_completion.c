/* Actual native transaction parsers -> cold decoder -> ordered counter fold.
 * Reader envelopes, route selection and non-counter owner callbacks remain
 * fixture leaves. This is neither a raw WAL-reader/CRC test
 * nor a replay, allocator or recovery-authority proof. */
#define CLUSTER_COLD_NATIVE_XACT_PARSER 1
#define CLUSTER_COLD_NATIVE_XACT_SHAPE 1
int cold_decode_fixture_main(void);
#define main cold_decode_fixture_main
#include "test_cluster_cold_recovery_decode.c"
#undef main
#include "access/twophase.h"
#include "test_cluster_cold_xid_completion_children.inc"
#include "test_cluster_cold_xid_completion_shape.inc"

static void
native_completion(FakeRecord *r, bool commit, TransactionId top,
				  const TransactionId *children, int count)
{
	xl_xact_xinfo info = { .xinfo = XACT_XINFO_HAS_SUBXACTS };
	TimestampTz timestamp = 99;
	Size length = commit ? MinSizeOfXactCommit : MinSizeOfXactAbort;

	fake_record(r, RM_XACT_ID,
				(commit ? XLOG_XACT_COMMIT : XLOG_XACT_ABORT) | XLOG_XACT_HAS_INFO, 0);
	r->storage.decoded.header.xl_xid = top;
	memcpy(r->data, &timestamp, sizeof(timestamp));
	memcpy(r->data + length, &info, sizeof(info));
	length += sizeof(info);
	memcpy(r->data + length, &count, sizeof(count));
	length += MinSizeOfXactSubxacts;
	memcpy(r->data + length, children, count * sizeof(*children));
	r->storage.decoded.main_data_len = length + count * sizeof(*children);
}

static uint64
decoded_endpoint(FakeRecord *r, uint64 checkpoint)
{
	ClusterColdParticipantV1 source = { 0 };
	ClusterColdXidProgressV1 progress;
	ClusterColdDecodedV1 decoded = { 0 };
	ClusterColdDetailV1 detail;
	uint64 next = 0;

	detail = cluster_cold_recovery_decode_v1(&r->reader, 99, UUID, false, false, &decoded);
	UT_ASSERT_EQ(detail, CLUSTER_COLD_OK);
	if (detail != CLUSTER_COLD_OK)
		return 0;
	UT_ASSERT(decoded.xid_input.complete);
	source.thread_id = 3;
	source.timeline = 7;
	source.owner_incarnation = 19;
	source.physical_lower = r->storage.decoded.header.xl_prev;
	source.native_redo = r->reader.ReadRecPtr;
	source.tail_end = r->reader.EndRecPtr;
	UT_ASSERT(cluster_cold_xid_begin_v1(&progress, &source, checkpoint, 1));
	UT_ASSERT(cluster_cold_xid_record_v1(&progress, &source, &decoded.record,
									  &decoded.xid_input));
	UT_ASSERT(cluster_cold_xid_candidate_v1(&progress, &source, &next));
	cluster_cold_decoded_release_v1(&decoded);
	return next;
}

UT_TEST(test_native_commit_children_supply_counter_payload)
{
	FakeRecord r;
	TransactionId children[] = { 104, 110, 112 };

	native_completion(&r, true, 100, children, lengthof(children));
	UT_ASSERT_EQ(decoded_endpoint(&r, (UINT64_C(7) << 32) | 100),
				 (UINT64_C(7) << 32) | 113);
}

UT_TEST(test_native_abort_children_cross_counter_wrap)
{
	FakeRecord r;
	TransactionId children[] = { UINT32_MAX - 1, 5 };

	native_completion(&r, false, UINT32_MAX - 2, children, lengthof(children));
	UT_ASSERT_EQ(decoded_endpoint(&r, (UINT64_C(7) << 32) | (UINT32_MAX - 3)),
				 (UINT64_C(8) << 32) | 6);
}

UT_TEST(test_native_multixact_members_supply_counter_payload)
{
	FakeRecord r;
	xl_multixact_create header = { .mid = 9, .moff = 8, .nmembers = 3 };
	MultiXactMember members[] = {
		{ .xid = 101, .status = MultiXactStatusForKeyShare },
		{ .xid = 201, .status = MultiXactStatusUpdate },
		{ .xid = 109, .status = MultiXactStatusForShare }
	};

	fake_record(&r, RM_MULTIXACT_ID, XLOG_MULTIXACT_CREATE_ID, 0);
	r.storage.decoded.header.xl_xid = 100;
	memcpy(r.data, &header, SizeOfMultiXactCreate);
	memcpy(r.data + SizeOfMultiXactCreate, members, sizeof(members));
	r.storage.decoded.main_data_len = SizeOfMultiXactCreate + sizeof(members);
	UT_ASSERT_EQ(decoded_endpoint(&r, (UINT64_C(7) << 32) | 100),
				 (UINT64_C(7) << 32) | 202);
}

UT_TEST(test_truncated_native_completion_supplies_no_endpoint)
{
	FakeRecord r;
	TransactionId children[] = { 104, 110 };
	ClusterColdDecodedV1 decoded = { 0 };
	ClusterColdParticipantV1 source = { .thread_id = 3, .timeline = 7,
		.owner_incarnation = 19, .physical_lower = 0xf80, .native_redo = 0x1000,
		.tail_end = 0x1080 };
	ClusterColdXidProgressV1 progress;
	uint64 next = 99;

	native_completion(&r, true, 100, children, lengthof(children));
	r.storage.decoded.main_data_len--;
	UT_ASSERT(cluster_cold_recovery_decode_v1(&r.reader, 99, UUID, false, false, &decoded)
			  != CLUSTER_COLD_OK);
	UT_ASSERT(!decoded.xid_input.complete);
	UT_ASSERT(cluster_cold_xid_begin_v1(&progress, &source, (UINT64_C(7) << 32) | 100, 1));
	UT_ASSERT(!cluster_cold_xid_candidate_v1(&progress, &source, &next));
	UT_ASSERT_EQ(next, 0);
	cluster_cold_decoded_release_v1(&decoded);
}

UT_TEST(test_abort_shape_refusals_supply_no_endpoint)
{
	TransactionId children[] = { 104, 110 };

	for (int malformed = 0; malformed < 4; malformed++)
	{
		FakeRecord r;
		ClusterColdDecodedV1 decoded = { 0 };
		ClusterColdDetailV1 detail;
		ClusterColdParticipantV1 source = { .thread_id = 3, .timeline = 7,
			.owner_incarnation = 19, .physical_lower = 0xf80, .native_redo = 0x1000,
			.tail_end = 0x1080 };
		ClusterColdXidProgressV1 progress;
		uint64 next = 99;

		native_completion(&r, false, 100, children, lengthof(children));
		if (malformed == 0)
			r.storage.decoded.main_data_len--;
		else if (malformed == 1)
			r.storage.decoded.main_data_len = MinSizeOfXactAbort + sizeof(xl_xact_xinfo) - 1;
		else if (malformed == 2)
			r.storage.decoded.main_data_len++;
		else
		{
			xl_xact_xinfo info = { .xinfo = XACT_XINFO_HAS_SUBXACTS | UINT32_C(0x10000000) };

			memcpy(r.data + MinSizeOfXactAbort, &info, sizeof(info));
		}
		detail = cluster_cold_recovery_decode_v1(&r.reader, 99, UUID, false, false, &decoded);
		UT_ASSERT(detail != CLUSTER_COLD_OK);
		UT_ASSERT(!decoded.xid_input.complete);
		UT_ASSERT(cluster_cold_xid_begin_v1(&progress, &source,
										   (UINT64_C(7) << 32) | 100, 1));
		if (detail == CLUSTER_COLD_OK)
			UT_ASSERT(!cluster_cold_xid_record_v1(&progress, &source, &decoded.record,
												 &decoded.xid_input));
		UT_ASSERT(!cluster_cold_xid_candidate_v1(&progress, &source, &next));
		UT_ASSERT_EQ(next, 0);
		cluster_cold_decoded_release_v1(&decoded);
	}
}

int
main(void)
{
	UT_PLAN(5);
	UT_RUN(test_native_commit_children_supply_counter_payload);
	UT_RUN(test_native_abort_children_cross_counter_wrap);
	UT_RUN(test_native_multixact_members_supply_counter_payload);
	UT_RUN(test_truncated_native_completion_supplies_no_endpoint);
	UT_RUN(test_abort_shape_refusals_supply_no_endpoint);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
