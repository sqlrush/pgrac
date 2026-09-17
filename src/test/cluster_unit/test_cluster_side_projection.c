/*-------------------------------------------------------------------------
 *
 * test_cluster_side_projection.c
 *    RF-SIDE D-SIDE-04 focused unit tests: the derived-projection
 *    judgements for CLOG / MULTIXACT / COMMIT_TS.
 *
 *    RED mapping (spec §5.1 U-SIDE-08/09/10 + §2.4):
 *      - verified requires canonical producer + coverage + integrity
 *        (U-SIDE-08: a local bit never overrides TT/redo);
 *      - a miss/UNKNOWN projection lookup FAILS CLOSED — never read the
 *        local ProcArray/CLOG to guess;
 *      - rebuildable: CLOG rebuilds from canonical truth (no redo
 *        retention needed); MULTIXACT/COMMIT_TS need the source redo
 *        retained (U-SIDE-09 missing-not-empty; U-SIDE-10 unknown
 *        timestamp never flips UNKNOWN to COMMITTED);
 *      - every rebuild needs the verified canonical producer.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "cluster/cluster_multixact.h"
#include "cluster/cluster_side_online_plan.h"
#include "cluster/cluster_side_projection.h"

#include "unit_test.h"

UT_DEFINE_GLOBALS();

void
ExceptionalCondition(const char *conditionName pg_attribute_unused(),
					 const char *fileName pg_attribute_unused(),
					 int lineNumber pg_attribute_unused())
{
	abort();
}

#include <stdio.h>

static ClusterSideProjectionVerifyInput
ut_full_verify(void)
{
	ClusterSideProjectionVerifyInput in;

	memset(&in, 0, sizeof(in));
	in.canonical_truth_ok = true;
	in.coverage_ok = true;
	in.integrity_ok = true;
	return in;
}

typedef struct ProjectionApplyCapture {
	uint32 reset_count;
	uint32 postread_count;
	uint32 truncate_count;
	uint32 multixact_mutate_count;
	uint32 multixact_verify_count;
	int origin_slot;
	TransactionId first_xid;
	uint32 xid_count;
	TransactionId oldest_xid;
	bool reset_ok;
	bool postread_ok;
	bool truncate_ok;
	bool multixact_mutate_ok;
	bool multixact_verify_ok;
} ProjectionApplyCapture;

static bool
capture_reset(void *arg, int origin_slot, TransactionId first_xid, uint32 xid_count)
{
	ProjectionApplyCapture *capture = (ProjectionApplyCapture *)arg;

	capture->reset_count++;
	capture->origin_slot = origin_slot;
	capture->first_xid = first_xid;
	capture->xid_count = xid_count;
	return capture->reset_ok;
}

static bool
capture_postread(void *arg, int origin_slot, TransactionId first_xid, uint32 xid_count)
{
	ProjectionApplyCapture *capture = (ProjectionApplyCapture *)arg;

	capture->postread_count++;
	UT_ASSERT_EQ(origin_slot, capture->origin_slot);
	UT_ASSERT_EQ(first_xid, capture->first_xid);
	UT_ASSERT_EQ(xid_count, capture->xid_count);
	return capture->postread_ok;
}

static bool
capture_truncate(void *arg, int origin_slot, TransactionId oldest_xid)
{
	ProjectionApplyCapture *capture = (ProjectionApplyCapture *)arg;

	capture->truncate_count++;
	capture->origin_slot = origin_slot;
	capture->oldest_xid = oldest_xid;
	return capture->truncate_ok;
}

static bool
capture_multixact(void *arg, int origin_slot, uint32 cluster_epoch,
				  const ClusterSideProjectionOperationV1 *operation, const uint8 *owned_payload,
				  uint32 owned_payload_length, XLogRecPtr source_lsn, XLogRecPtr source_end_lsn)
{
	ProjectionApplyCapture *capture = (ProjectionApplyCapture *)arg;
	const MultiXactMember *members = (const MultiXactMember *)owned_payload;

	UT_ASSERT_EQ(origin_slot, 2);
	UT_ASSERT_EQ(cluster_epoch, 19);
	UT_ASSERT_EQ(operation->multixact_id, 33);
	UT_ASSERT_EQ(operation->member_offset, 71);
	UT_ASSERT_EQ(operation->member_count, 2);
	UT_ASSERT_EQ(owned_payload_length, 2 * sizeof(MultiXactMember));
	UT_ASSERT_EQ(members[1].xid, 816);
	UT_ASSERT_EQ(source_lsn, 100);
	UT_ASSERT_EQ(source_end_lsn, 200);
	capture->multixact_mutate_count++;
	return capture->multixact_mutate_ok;
}

static bool
capture_multixact_postread(void *arg, int origin_slot, uint32 cluster_epoch,
						   const ClusterSideProjectionOperationV1 *operation,
						   const uint8 *owned_payload, uint32 owned_payload_length,
						   XLogRecPtr source_lsn, XLogRecPtr source_end_lsn)
{
	ProjectionApplyCapture *capture = (ProjectionApplyCapture *)arg;

	UT_ASSERT_EQ(origin_slot, 2);
	UT_ASSERT_EQ(cluster_epoch, 19);
	UT_ASSERT_EQ(operation->multixact_id, 33);
	UT_ASSERT(owned_payload != NULL);
	UT_ASSERT_EQ(owned_payload_length, 2 * sizeof(MultiXactMember));
	UT_ASSERT_EQ(source_lsn, 100);
	UT_ASSERT_EQ(source_end_lsn, 200);
	capture->multixact_verify_count++;
	return capture->multixact_verify_ok;
}

static ProjectionApplyCapture owner_capture;

bool
cluster_remote_xact_reset_range_v2(int origin_slot, TransactionId first_xid, uint32 xid_count)
{
	return capture_reset(&owner_capture, origin_slot, first_xid, xid_count);
}

bool
cluster_remote_xact_range_empty_v2(int origin_slot, TransactionId first_xid, uint32 xid_count)
{
	return capture_postread(&owner_capture, origin_slot, first_xid, xid_count);
}

bool
cluster_remote_xact_truncate_before_v2(int origin_slot, TransactionId oldest_xid)
{
	return capture_truncate(&owner_capture, origin_slot, oldest_xid);
}

bool
cluster_multixact_recovery_projection_apply(void *arg, int origin_slot, uint32 cluster_epoch,
											const ClusterSideProjectionOperationV1 *operation,
											const uint8 *owned_payload, uint32 owned_payload_length,
											XLogRecPtr source_lsn, XLogRecPtr source_end_lsn)
{
	UT_ASSERT(arg == NULL);
	return capture_multixact(&owner_capture, origin_slot, cluster_epoch, operation, owned_payload,
							 owned_payload_length, source_lsn, source_end_lsn);
}

bool
cluster_multixact_recovery_projection_verify(void *arg, int origin_slot, uint32 cluster_epoch,
											 const ClusterSideProjectionOperationV1 *operation,
											 const uint8 *owned_payload,
											 uint32 owned_payload_length, XLogRecPtr source_lsn,
											 XLogRecPtr source_end_lsn)
{
	UT_ASSERT(arg == NULL);
	return capture_multixact_postread(&owner_capture, origin_slot, cluster_epoch, operation,
									  owned_payload, owned_payload_length, source_lsn,
									  source_end_lsn);
}

UT_TEST(test_projection_verified_conjunction)
{
	ClusterSideProjectionVerifyInput in = ut_full_verify();
	ClusterSideProjectionKind kinds[]
		= { CLUSTER_SIDE_PROJECTION_CLOG, CLUSTER_SIDE_PROJECTION_MULTIXACT,
			CLUSTER_SIDE_PROJECTION_COMMIT_TS };
	int i;

	for (i = 0; i < (int)lengthof(kinds); i++) {
		UT_ASSERT(cluster_side_projection_verified(kinds[i], &in));
		in.canonical_truth_ok = false;
		UT_ASSERT(!cluster_side_projection_verified(kinds[i], &in));
		in = ut_full_verify();
		in.coverage_ok = false;
		UT_ASSERT(!cluster_side_projection_verified(kinds[i], &in));
		in = ut_full_verify();
		in.integrity_ok = false;
		UT_ASSERT(!cluster_side_projection_verified(kinds[i], &in));
		in = ut_full_verify();
	}
	UT_ASSERT(!cluster_side_projection_verified(CLUSTER_SIDE_PROJECTION_CLOG, NULL));
	/* Out-of-range kind fails closed. */
	UT_ASSERT(!cluster_side_projection_verified((ClusterSideProjectionKind)99, &in));
}

UT_TEST(test_projection_lookup_fail_closed)
{
	/* A miss/UNKNOWN projection NEVER answers from a local guess
	 * (U-SIDE-08: local bit cannot override TT/redo truth). */
	UT_ASSERT_EQ((int)cluster_side_projection_lookup(false),
				 (int)CLUSTER_SIDE_PROJECTION_LOOKUP_FAIL_CLOSED);
	UT_ASSERT_EQ((int)cluster_side_projection_lookup(true), (int)CLUSTER_SIDE_PROJECTION_LOOKUP_OK);
}

UT_TEST(test_projection_rebuildable_per_kind)
{
	/* CLOG rebuilds from the canonical transaction truth: no redo
	 * retention required. */
	UT_ASSERT(cluster_side_projection_rebuildable(CLUSTER_SIDE_PROJECTION_CLOG, false, true));

	/* MULTIXACT / COMMIT_TS rebuild from the retained redo: the source
	 * must still be retained (U-SIDE-09/10).  L9: a COMMIT_TS projection
	 * loss or unknown timestamp never changes the commit truth — the
	 * rebuild needs the source redo retained and the lookup fails
	 * closed until it is. */
	UT_ASSERT(cluster_side_projection_rebuildable(CLUSTER_SIDE_PROJECTION_MULTIXACT, true, true));
	UT_ASSERT(!cluster_side_projection_rebuildable(CLUSTER_SIDE_PROJECTION_MULTIXACT, false, true));
	UT_ASSERT(cluster_side_projection_rebuildable(CLUSTER_SIDE_PROJECTION_COMMIT_TS, true, true));
	UT_ASSERT(!cluster_side_projection_rebuildable(CLUSTER_SIDE_PROJECTION_COMMIT_TS, false, true));

	/* Every rebuild needs the verified canonical producer — without it,
	 * the rebuild is a guess and the resource stays BLOCKED. */
	UT_ASSERT(!cluster_side_projection_rebuildable(CLUSTER_SIDE_PROJECTION_CLOG, false, false));
	UT_ASSERT(!cluster_side_projection_rebuildable(CLUSTER_SIDE_PROJECTION_MULTIXACT, true, false));
	/* Out-of-range kind fails closed. */
	UT_ASSERT(!cluster_side_projection_rebuildable((ClusterSideProjectionKind)99, true, true));
}

UT_TEST(test_projection_zero_range_preflight_and_postread)
{
	ClusterSideProjectionOperationV1 operation;
	ClusterSideProjectionApplyInputV1 input;
	ClusterSideProjectionApplyOpsV1 ops;
	ProjectionApplyCapture capture;

	memset(&operation, 0, sizeof(operation));
	operation.kind = CLUSTER_SIDE_PROJECTION_CLOG;
	operation.action = CLUSTER_SIDE_PROJECTION_ACTION_ZERO_PAGE;
	operation.page_number = 7;
	memset(&input, 0, sizeof(input));
	input.operation = &operation;
	input.origin_thread = 3;
	UT_ASSERT_EQ(cluster_side_projection_target_preflight_v1(&input),
				 CLUSTER_SIDE_PROJECTION_APPLY_OK);
	memset(&capture, 0, sizeof(capture));
	capture.reset_ok = true;
	capture.postread_ok = true;
	memset(&ops, 0, sizeof(ops));
	ops.arg = &capture;
	ops.reset_remote_xact_range = capture_reset;
	ops.remote_xact_range_empty = capture_postread;
	ops.truncate_remote_xact_before = capture_truncate;
	UT_ASSERT_EQ(cluster_side_projection_apply_owned_v1(&input, &ops),
				 CLUSTER_SIDE_PROJECTION_APPLY_OK);
	UT_ASSERT_EQ(capture.reset_count, 1);
	UT_ASSERT_EQ(capture.postread_count, 1);
	UT_ASSERT_EQ(capture.origin_slot, 2);
	UT_ASSERT_EQ(capture.first_xid, 7 * BLCKSZ * 4);
	UT_ASSERT_EQ(capture.xid_count, BLCKSZ * 4);

	capture.postread_ok = false;
	UT_ASSERT_EQ(cluster_side_projection_apply_owned_v1(&input, &ops),
				 CLUSTER_SIDE_PROJECTION_APPLY_POST_READ_FAILED);
	UT_ASSERT_EQ(capture.reset_count, 2);
	UT_ASSERT_EQ(capture.postread_count, 2);
}

UT_TEST(test_projection_commit_ts_requires_retained_source_and_truncates)
{
	ClusterSideProjectionOperationV1 operation;
	ClusterSideProjectionApplyInputV1 input;
	ClusterSideProjectionApplyOpsV1 ops;
	ProjectionApplyCapture capture;

	memset(&operation, 0, sizeof(operation));
	operation.kind = CLUSTER_SIDE_PROJECTION_COMMIT_TS;
	operation.action = CLUSTER_SIDE_PROJECTION_ACTION_TRUNCATE;
	operation.page_number = 9;
	operation.oldest_xid = 800;
	memset(&input, 0, sizeof(input));
	input.operation = &operation;
	input.origin_thread = 3;
	UT_ASSERT_EQ(cluster_side_projection_target_preflight_v1(&input),
				 CLUSTER_SIDE_PROJECTION_APPLY_BLOCKED);
	input.source_retained = true;
	UT_ASSERT_EQ(cluster_side_projection_target_preflight_v1(&input),
				 CLUSTER_SIDE_PROJECTION_APPLY_OK);
	memset(&capture, 0, sizeof(capture));
	capture.truncate_ok = true;
	memset(&ops, 0, sizeof(ops));
	ops.arg = &capture;
	ops.reset_remote_xact_range = capture_reset;
	ops.remote_xact_range_empty = capture_postread;
	ops.truncate_remote_xact_before = capture_truncate;
	UT_ASSERT_EQ(cluster_side_projection_apply_owned_v1(&input, &ops),
				 CLUSTER_SIDE_PROJECTION_APPLY_OK);
	UT_ASSERT_EQ(capture.truncate_count, 1);
	UT_ASSERT_EQ(capture.origin_slot, 2);
	UT_ASSERT_EQ(capture.oldest_xid, 800);
	UT_ASSERT_EQ(capture.reset_count, 0);
}

UT_TEST(test_projection_multixact_requires_retained_exact_source_and_postread)
{
	ClusterSideProjectionOperationV1 operation;
	ClusterSideProjectionApplyInputV1 input;
	ClusterSideProjectionApplyOpsV1 ops;
	ProjectionApplyCapture capture;
	MultiXactMember members[2];

	memset(&operation, 0, sizeof(operation));
	operation.kind = CLUSTER_SIDE_PROJECTION_MULTIXACT;
	operation.action = CLUSTER_SIDE_PROJECTION_ACTION_CREATE;
	operation.normalized_info = XLOG_MULTIXACT_CREATE_ID;
	operation.multixact_id = 33;
	operation.member_offset = 71;
	operation.member_count = 2;
	memset(members, 0, sizeof(members));
	members[0].xid = 800;
	members[0].status = MultiXactStatusForKeyShare;
	members[1].xid = 816;
	members[1].status = MultiXactStatusUpdate;
	memset(&input, 0, sizeof(input));
	input.operation = &operation;
	input.owned_payload = (const uint8 *)members;
	input.owned_payload_length = sizeof(members);
	input.origin_thread = 3;
	input.cluster_epoch = 19;
	input.source_lsn = 100;
	input.source_end_lsn = 200;
	UT_ASSERT_EQ(cluster_side_projection_target_preflight_v1(&input),
				 CLUSTER_SIDE_PROJECTION_APPLY_BLOCKED);
	input.source_retained = true;
	UT_ASSERT_EQ(cluster_side_projection_target_preflight_v1(&input),
				 CLUSTER_SIDE_PROJECTION_APPLY_OK);
	memset(&capture, 0, sizeof(capture));
	capture.multixact_mutate_ok = true;
	capture.multixact_verify_ok = true;
	memset(&ops, 0, sizeof(ops));
	ops.arg = &capture;
	ops.apply_multixact_projection = capture_multixact;
	ops.verify_multixact_projection = capture_multixact_postread;
	UT_ASSERT_EQ(cluster_side_projection_apply_owned_v1(&input, &ops),
				 CLUSTER_SIDE_PROJECTION_APPLY_OK);
	UT_ASSERT_EQ(capture.multixact_mutate_count, 1);
	UT_ASSERT_EQ(capture.multixact_verify_count, 1);
	capture.multixact_verify_ok = false;
	UT_ASSERT_EQ(cluster_side_projection_apply_owned_v1(&input, &ops),
				 CLUSTER_SIDE_PROJECTION_APPLY_POST_READ_FAILED);
}

UT_TEST(test_online_owner_wires_retained_source_identity_and_real_callbacks)
{
	RfSideOnlineProjectionOwnerV1 owner;
	RfSideOnlineOperationV1 operation;
	MultiXactMember members[2];

	memset(&operation, 0, sizeof(operation));
	operation.kind = RF_SIDE_ONLINE_OPERATION_PROJECTION;
	operation.identity.record.origin_thread = 3;
	operation.identity.record.read_rec_ptr = 100;
	operation.identity.record.end_rec_ptr = 200;
	operation.projection.kind = CLUSTER_SIDE_PROJECTION_MULTIXACT;
	operation.projection.action = CLUSTER_SIDE_PROJECTION_ACTION_CREATE;
	operation.projection.normalized_info = XLOG_MULTIXACT_CREATE_ID;
	operation.projection.multixact_id = 33;
	operation.projection.member_offset = 71;
	operation.projection.member_count = 2;
	memset(members, 0, sizeof(members));
	members[0].xid = 800;
	members[0].status = MultiXactStatusForKeyShare;
	members[1].xid = 816;
	members[1].status = MultiXactStatusUpdate;
	operation.owned_payload = (const uint8 *)members;
	operation.owned_payload_length = sizeof(members);

	UT_ASSERT(rf_side_online_projection_owner_init_v1(&owner, 19, true));
	memset(&owner_capture, 0, sizeof(owner_capture));
	owner_capture.multixact_mutate_ok = true;
	owner_capture.multixact_verify_ok = true;
	UT_ASSERT(rf_side_online_projection_preflight_owned_v1(&owner, &operation));
	UT_ASSERT(rf_side_online_projection_apply_owned_v1(&owner, &operation));
	UT_ASSERT_EQ(owner_capture.multixact_mutate_count, 1);
	UT_ASSERT_EQ(owner_capture.multixact_verify_count, 1);

	UT_ASSERT(rf_side_online_projection_owner_init_v1(&owner, 19, false));
	UT_ASSERT(!rf_side_online_projection_preflight_owned_v1(&owner, &operation));
	operation.kind = RF_SIDE_ONLINE_OPERATION_XACT;
	UT_ASSERT(!rf_side_online_projection_apply_owned_v1(&owner, &operation));
}

int
main(void)
{
	UT_PLAN(7);

	UT_RUN(test_projection_verified_conjunction);
	UT_RUN(test_projection_lookup_fail_closed);
	UT_RUN(test_projection_rebuildable_per_kind);
	UT_RUN(test_projection_zero_range_preflight_and_postread);
	UT_RUN(test_projection_commit_ts_requires_retained_source_and_truncates);
	UT_RUN(test_projection_multixact_requires_retained_exact_source_and_postread);
	UT_RUN(test_online_owner_wires_retained_source_identity_and_real_callbacks);

	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
