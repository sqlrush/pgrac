/*-------------------------------------------------------------------------
 *
 * test_cluster_r4_multi_subx_2pc.c
 *    Stage 8 R4 native MultiXact snapshot and resolver tests.
 *
 *    This binary links the real cluster_tx_resolve.c resolver and replaces
 *    only its native member reader and semantic-admission boundaries.  The
 *    SubTrans and two-phase lifecycle seams remain covered by the dedicated
 *    TAP loadable module test_pgrac_r4_native_tx.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * IDENTIFICATION
 *    src/test/cluster_unit/test_cluster_r4_multi_subx_2pc.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/multixact.h"
#include "cluster/cluster_multixact.h"
#include "cluster/cluster_semantic_activation.h"
#include "cluster/cluster_tx_resolve.h"

#undef printf

#include "unit_test.h"

UT_DEFINE_GLOBALS();

#define TEST_NATIVE_READ_MAX 4
#define TEST_NATIVE_MEMBER_MAX 257

typedef struct TestNativeRead {
	MultiXactOffset start_offset;
	int count;
	bool null_members;
	MultiXactMember members[TEST_NATIVE_MEMBER_MAX];
} TestNativeRead;

static TestNativeRead test_native_reads[TEST_NATIVE_READ_MAX];
static int test_native_read_count;
static int test_native_read_calls;
static int test_enter_calls;
static int test_leave_calls;
static ClusterSemanticAdmissionResult test_admission_result = CLUSTER_SEMANTIC_ADMISSION_OK;

sigjmp_buf *PG_exception_stack = NULL;
ErrorContextCallback *error_context_stack = NULL;

void
pg_re_throw(void)
{
	if (PG_exception_stack != NULL)
		siglongjmp(*PG_exception_stack, 1);
	abort();
}

void
pfree(void *pointer)
{
	free(pointer);
}

ClusterSemanticAdmissionResult
cluster_semantic_activation_enter(uint64 feature_bit, ClusterSemanticAdmissionSide side,
								  ClusterSemanticAdmissionToken *token)
{
	test_enter_calls++;
	UT_ASSERT_EQ(feature_bit, CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1);
	UT_ASSERT_EQ(side, CLUSTER_SEMANTIC_TARGET_SIDE);
	if (test_admission_result == CLUSTER_SEMANTIC_ADMISSION_OK) {
		memset(token, 0, sizeof(*token));
		token->feature_bit = feature_bit;
		token->side = (uint8)side;
		token->entered = true;
	}
	return test_admission_result;
}

void
cluster_semantic_activation_leave(ClusterSemanticAdmissionToken *token)
{
	test_leave_calls++;
	UT_ASSERT(token->entered);
	token->entered = false;
}

int
GetMultiXactIdMembersWithOffset(MultiXactId multi, MultiXactMember **members, bool from_pgupgrade,
								bool isLockOnly, MultiXactOffset *start_offset_out)
{
	TestNativeRead *read;
	Size size;

	UT_ASSERT_EQ(multi, (MultiXactId)73);
	UT_ASSERT(!from_pgupgrade);
	UT_ASSERT(!isLockOnly);
	UT_ASSERT_NOT_NULL(members);
	UT_ASSERT_NOT_NULL(start_offset_out);
	UT_ASSERT(test_native_read_calls < test_native_read_count);

	read = &test_native_reads[test_native_read_calls++];
	*members = NULL;
	*start_offset_out = read->start_offset;
	if (read->null_members || read->count <= 0)
		return read->count;

	size = (Size)read->count * sizeof(MultiXactMember);
	*members = (MultiXactMember *)malloc(size);
	UT_ASSERT_NOT_NULL(*members);
	memcpy(*members, read->members, size);
	return read->count;
}

typedef int (*ClusterR4NativeMultiReadFn)(MultiXactId, MultiXactMember **, bool, bool,
										  MultiXactOffset *);

StaticAssertDecl(__builtin_types_compatible_p(__typeof__(&GetMultiXactIdMembersWithOffset),
											  ClusterR4NativeMultiReadFn),
				 "R4 native MultiXact reader signature must remain exact");

static void
fill_members(MultiXactMember *members, int count)
{
	int i;

	for (i = 0; i < count; i++) {
		members[i].xid = (TransactionId)(100 + i);
		members[i].status = (MultiXactStatus)(i % (MaxMultiXactStatus + 1));
	}
}

static void
reset_runtime_fixture(void)
{
	memset(test_native_reads, 0, sizeof(test_native_reads));
	test_native_read_count = 0;
	test_native_read_calls = 0;
	test_enter_calls = 0;
	test_leave_calls = 0;
	test_admission_result = CLUSTER_SEMANTIC_ADMISSION_OK;
}

static void
script_native_read(int index, MultiXactOffset start_offset, int count)
{
	UT_ASSERT(index >= 0 && index < TEST_NATIVE_READ_MAX);
	UT_ASSERT(count <= TEST_NATIVE_MEMBER_MAX);
	test_native_reads[index].start_offset = start_offset;
	test_native_reads[index].count = count;
	if (count > 0)
		fill_members(test_native_reads[index].members, count);
	if (index >= test_native_read_count)
		test_native_read_count = index + 1;
}

static bool
bytes_are_zero(const void *ptr, Size size)
{
	const unsigned char *bytes = (const unsigned char *)ptr;
	Size i;

	for (i = 0; i < size; i++) {
		if (bytes[i] != 0)
			return false;
	}
	return true;
}

UT_TEST(test_native_reader_signature_is_compile_time_only)
{
	UT_ASSERT(true);
}

UT_TEST(test_exact_two_member_snapshot_matches)
{
	MultiXactMember left[2];
	MultiXactMember right[2];

	fill_members(left, 2);
	memcpy(right, left, sizeof(left));
	UT_ASSERT(cluster_multixact_native_snapshot_equal(17, 2, left, 17, 2, right));
}

UT_TEST(test_exact_256_member_snapshot_matches)
{
	MultiXactMember left[256];
	MultiXactMember right[256];

	fill_members(left, 256);
	memcpy(right, left, sizeof(left));
	UT_ASSERT(cluster_multixact_native_snapshot_equal(29, 256, left, 29, 256, right));
}

UT_TEST(test_generation_change_refuses)
{
	MultiXactMember members[2];

	fill_members(members, 2);
	UT_ASSERT(!cluster_multixact_native_snapshot_equal(17, 2, members, 18, 2, members));
}

UT_TEST(test_zero_generation_refuses)
{
	MultiXactMember members[2];

	fill_members(members, 2);
	UT_ASSERT(!cluster_multixact_native_snapshot_equal(0, 2, members, 0, 2, members));
}

UT_TEST(test_zero_members_refuses)
{
	UT_ASSERT(!cluster_multixact_native_snapshot_equal(17, 0, NULL, 17, 0, NULL));
}

UT_TEST(test_one_member_refuses)
{
	MultiXactMember member;

	fill_members(&member, 1);
	UT_ASSERT(!cluster_multixact_native_snapshot_equal(17, 1, &member, 17, 1, &member));
}

UT_TEST(test_257_members_refuses)
{
	MultiXactMember members[257];

	fill_members(members, 257);
	UT_ASSERT(!cluster_multixact_native_snapshot_equal(17, 257, members, 17, 257, members));
}

UT_TEST(test_member_count_change_refuses)
{
	MultiXactMember members[3];

	fill_members(members, 3);
	UT_ASSERT(!cluster_multixact_native_snapshot_equal(17, 2, members, 17, 3, members));
}

UT_TEST(test_null_first_member_array_refuses)
{
	MultiXactMember members[2];

	fill_members(members, 2);
	UT_ASSERT(!cluster_multixact_native_snapshot_equal(17, 2, NULL, 17, 2, members));
}

UT_TEST(test_null_second_member_array_refuses)
{
	MultiXactMember members[2];

	fill_members(members, 2);
	UT_ASSERT(!cluster_multixact_native_snapshot_equal(17, 2, members, 17, 2, NULL));
}

UT_TEST(test_member_xid_change_refuses)
{
	MultiXactMember left[2];
	MultiXactMember right[2];

	fill_members(left, 2);
	memcpy(right, left, sizeof(left));
	right[1].xid++;
	UT_ASSERT(!cluster_multixact_native_snapshot_equal(17, 2, left, 17, 2, right));
}

UT_TEST(test_member_status_change_refuses)
{
	MultiXactMember left[2];
	MultiXactMember right[2];

	fill_members(left, 2);
	memcpy(right, left, sizeof(left));
	right[1].status = MultiXactStatusUpdate;
	UT_ASSERT(!cluster_multixact_native_snapshot_equal(17, 2, left, 17, 2, right));
}

UT_TEST(test_member_order_change_refuses)
{
	MultiXactMember left[2];
	MultiXactMember right[2];
	MultiXactMember swap;

	fill_members(left, 2);
	memcpy(right, left, sizeof(left));
	swap = right[0];
	right[0] = right[1];
	right[1] = swap;
	UT_ASSERT(!cluster_multixact_native_snapshot_equal(17, 2, left, 17, 2, right));
}

UT_TEST(test_invalid_member_xid_refuses)
{
	MultiXactMember members[2];

	fill_members(members, 2);
	members[0].xid = InvalidTransactionId;
	UT_ASSERT(!cluster_multixact_native_snapshot_equal(17, 2, members, 17, 2, members));
}

UT_TEST(test_invalid_member_status_refuses)
{
	MultiXactMember members[2];

	fill_members(members, 2);
	members[0].status = (MultiXactStatus)(MaxMultiXactStatus + 1);
	UT_ASSERT(!cluster_multixact_native_snapshot_equal(17, 2, members, 17, 2, members));
}

UT_TEST(test_target_disabled_refuses_before_native_read)
{
	ClusterMultiResolution resolution;
	ClusterTxResolveReason reason = CLUSTER_TX_RESOLVE_NONE;

	reset_runtime_fixture();
	test_admission_result = CLUSTER_SEMANTIC_ADMISSION_TARGET_DISABLED;
	memset(&resolution, 0xA5, sizeof(resolution));
	UT_ASSERT_EQ(cluster_tx_resolve_multixact((MultiXactId)73, &resolution, &reason),
				 CLUSTER_TX_UNKNOWN);
	UT_ASSERT_EQ(reason, CLUSTER_TX_RESOLVE_TARGET_DISABLED);
	UT_ASSERT_EQ(test_native_read_calls, 0);
	UT_ASSERT_EQ(test_enter_calls, 1);
	UT_ASSERT_EQ(test_leave_calls, 0);
	UT_ASSERT(bytes_are_zero(&resolution, sizeof(resolution)));
}

UT_TEST(test_rf_deferred_refuses_before_native_read)
{
	ClusterMultiResolution resolution;
	ClusterTxResolveReason reason = CLUSTER_TX_RESOLVE_NONE;

	reset_runtime_fixture();
	test_admission_result = CLUSTER_SEMANTIC_ADMISSION_CLOSED;
	memset(&resolution, 0xA5, sizeof(resolution));
	UT_ASSERT_EQ(cluster_tx_resolve_multixact((MultiXactId)73, &resolution, &reason),
				 CLUSTER_TX_UNKNOWN);
	UT_ASSERT_EQ(reason, CLUSTER_TX_RESOLVE_RF_DEFERRED);
	UT_ASSERT_EQ(test_native_read_calls, 0);
	UT_ASSERT_EQ(test_enter_calls, 1);
	UT_ASSERT_EQ(test_leave_calls, 0);
	UT_ASSERT(bytes_are_zero(&resolution, sizeof(resolution)));
}

UT_TEST(test_stable_native_sample_is_zero_coverage_gap)
{
	ClusterMultiResolution resolution;
	ClusterTxResolveReason reason = CLUSTER_TX_RESOLVE_NONE;

	reset_runtime_fixture();
	script_native_read(0, 17, 2);
	script_native_read(1, 17, 2);
	memset(&resolution, 0xA5, sizeof(resolution));
	UT_ASSERT_EQ(cluster_tx_resolve_multixact((MultiXactId)73, &resolution, &reason),
				 CLUSTER_TX_UNKNOWN);
	UT_ASSERT_EQ(reason, CLUSTER_TX_RESOLVE_COVERAGE_GAP);
	UT_ASSERT_EQ(test_native_read_calls, 2);
	UT_ASSERT_EQ(test_enter_calls, 1);
	UT_ASSERT_EQ(test_leave_calls, 1);
	UT_ASSERT(bytes_are_zero(&resolution, sizeof(resolution)));
}

UT_TEST(test_stable_256_member_sample_is_zero_coverage_gap)
{
	ClusterMultiResolution resolution;
	ClusterTxResolveReason reason = CLUSTER_TX_RESOLVE_NONE;

	reset_runtime_fixture();
	script_native_read(0, 29, 256);
	script_native_read(1, 29, 256);
	memset(&resolution, 0xA5, sizeof(resolution));
	UT_ASSERT_EQ(cluster_tx_resolve_multixact((MultiXactId)73, &resolution, &reason),
				 CLUSTER_TX_UNKNOWN);
	UT_ASSERT_EQ(reason, CLUSTER_TX_RESOLVE_COVERAGE_GAP);
	UT_ASSERT_EQ(test_native_read_calls, 2);
	UT_ASSERT_EQ(test_leave_calls, 1);
	UT_ASSERT(bytes_are_zero(&resolution, sizeof(resolution)));
}

UT_TEST(test_first_change_retries_one_whole_sample)
{
	ClusterMultiResolution resolution;
	ClusterTxResolveReason reason = CLUSTER_TX_RESOLVE_NONE;

	reset_runtime_fixture();
	script_native_read(0, 17, 2);
	script_native_read(1, 18, 2);
	script_native_read(2, 29, 2);
	script_native_read(3, 29, 2);
	memset(&resolution, 0xA5, sizeof(resolution));
	UT_ASSERT_EQ(cluster_tx_resolve_multixact((MultiXactId)73, &resolution, &reason),
				 CLUSTER_TX_UNKNOWN);
	UT_ASSERT_EQ(reason, CLUSTER_TX_RESOLVE_COVERAGE_GAP);
	UT_ASSERT_EQ(test_native_read_calls, 4);
	UT_ASSERT_EQ(test_leave_calls, 1);
	UT_ASSERT(bytes_are_zero(&resolution, sizeof(resolution)));
}

UT_TEST(test_second_change_is_zero_composition_changed)
{
	ClusterMultiResolution resolution;
	ClusterTxResolveReason reason = CLUSTER_TX_RESOLVE_NONE;

	reset_runtime_fixture();
	script_native_read(0, 17, 2);
	script_native_read(1, 18, 2);
	script_native_read(2, 29, 2);
	script_native_read(3, 30, 2);
	memset(&resolution, 0xA5, sizeof(resolution));
	UT_ASSERT_EQ(cluster_tx_resolve_multixact((MultiXactId)73, &resolution, &reason),
				 CLUSTER_TX_UNKNOWN);
	UT_ASSERT_EQ(reason, CLUSTER_TX_RESOLVE_COMPOSITION_CHANGED);
	UT_ASSERT_EQ(test_native_read_calls, 4);
	UT_ASSERT_EQ(test_leave_calls, 1);
	UT_ASSERT(bytes_are_zero(&resolution, sizeof(resolution)));
}

UT_TEST(test_invalid_native_sample_is_bad_composition)
{
	ClusterMultiResolution resolution;
	ClusterTxResolveReason reason = CLUSTER_TX_RESOLVE_NONE;

	reset_runtime_fixture();
	script_native_read(0, 17, 1);
	script_native_read(1, 17, 2);
	memset(&resolution, 0xA5, sizeof(resolution));
	UT_ASSERT_EQ(cluster_tx_resolve_multixact((MultiXactId)73, &resolution, &reason),
				 CLUSTER_TX_UNKNOWN);
	UT_ASSERT_EQ(reason, CLUSTER_TX_RESOLVE_BAD_COMPOSITION);
	UT_ASSERT_EQ(test_native_read_calls, 2);
	UT_ASSERT_EQ(test_leave_calls, 1);
	UT_ASSERT(bytes_are_zero(&resolution, sizeof(resolution)));
}

UT_TEST(test_257_member_native_sample_is_bad_composition)
{
	ClusterMultiResolution resolution;
	ClusterTxResolveReason reason = CLUSTER_TX_RESOLVE_NONE;

	reset_runtime_fixture();
	script_native_read(0, 17, 257);
	script_native_read(1, 17, 2);
	memset(&resolution, 0xA5, sizeof(resolution));
	UT_ASSERT_EQ(cluster_tx_resolve_multixact((MultiXactId)73, &resolution, &reason),
				 CLUSTER_TX_UNKNOWN);
	UT_ASSERT_EQ(reason, CLUSTER_TX_RESOLVE_BAD_COMPOSITION);
	UT_ASSERT_EQ(test_native_read_calls, 2);
	UT_ASSERT_EQ(test_leave_calls, 1);
	UT_ASSERT(bytes_are_zero(&resolution, sizeof(resolution)));
}

UT_TEST(test_invalid_native_member_domains_are_bad_composition)
{
	ClusterMultiResolution resolution;
	ClusterTxResolveReason reason = CLUSTER_TX_RESOLVE_NONE;

	reset_runtime_fixture();
	script_native_read(0, 17, 2);
	script_native_read(1, 17, 2);
	test_native_reads[0].members[1].xid = InvalidTransactionId;
	test_native_reads[1].members[0].status = (MultiXactStatus)(MaxMultiXactStatus + 1);
	memset(&resolution, 0xA5, sizeof(resolution));
	UT_ASSERT_EQ(cluster_tx_resolve_multixact((MultiXactId)73, &resolution, &reason),
				 CLUSTER_TX_UNKNOWN);
	UT_ASSERT_EQ(reason, CLUSTER_TX_RESOLVE_BAD_COMPOSITION);
	UT_ASSERT_EQ(test_native_read_calls, 2);
	UT_ASSERT_EQ(test_leave_calls, 1);
	UT_ASSERT(bytes_are_zero(&resolution, sizeof(resolution)));
}

UT_TEST(test_null_output_refuses_before_native_read)
{
	ClusterTxResolveReason reason = CLUSTER_TX_RESOLVE_NONE;

	reset_runtime_fixture();
	UT_ASSERT_EQ(cluster_tx_resolve_multixact((MultiXactId)73, NULL, &reason), CLUSTER_TX_UNKNOWN);
	UT_ASSERT_EQ(reason, CLUSTER_TX_RESOLVE_PROTOCOL);
	UT_ASSERT_EQ(test_native_read_calls, 0);
	UT_ASSERT_EQ(test_leave_calls, 1);
}

int
main(void)
{
	UT_PLAN(26);
	UT_RUN(test_native_reader_signature_is_compile_time_only);
	UT_RUN(test_exact_two_member_snapshot_matches);
	UT_RUN(test_exact_256_member_snapshot_matches);
	UT_RUN(test_generation_change_refuses);
	UT_RUN(test_zero_generation_refuses);
	UT_RUN(test_zero_members_refuses);
	UT_RUN(test_one_member_refuses);
	UT_RUN(test_257_members_refuses);
	UT_RUN(test_member_count_change_refuses);
	UT_RUN(test_null_first_member_array_refuses);
	UT_RUN(test_null_second_member_array_refuses);
	UT_RUN(test_member_xid_change_refuses);
	UT_RUN(test_member_status_change_refuses);
	UT_RUN(test_member_order_change_refuses);
	UT_RUN(test_invalid_member_xid_refuses);
	UT_RUN(test_invalid_member_status_refuses);
	UT_RUN(test_target_disabled_refuses_before_native_read);
	UT_RUN(test_rf_deferred_refuses_before_native_read);
	UT_RUN(test_stable_native_sample_is_zero_coverage_gap);
	UT_RUN(test_stable_256_member_sample_is_zero_coverage_gap);
	UT_RUN(test_first_change_retries_one_whole_sample);
	UT_RUN(test_second_change_is_zero_composition_changed);
	UT_RUN(test_invalid_native_sample_is_bad_composition);
	UT_RUN(test_257_member_native_sample_is_bad_composition);
	UT_RUN(test_invalid_native_member_domains_are_bad_composition);
	UT_RUN(test_null_output_refuses_before_native_read);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
