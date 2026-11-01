/* Typed effects through native descriptor planning, PREPARE and APPLY.
 * Descriptor storage and receipt-table outcomes are controlled leaves.
 * Exact target qualification, physical publication and SQL are not simulated
 * successes here: they remain separate native integration requirements.
 * Portions Copyright (c) 2026, pgrac contributors */
int publication_unused_receipt_main(void);
#define RECEIPT_PUBLICATION_FIXTURE
#include "test_cluster_mx_operation_receipt.c"
#undef main
#undef RECEIPT_PUBLICATION_FIXTURE
#include "cluster/cluster_mxid_stripe.h"
#include "cluster/cluster_xid_stripe_boot.h"

static MultiXactMember stored_members[CLUSTER_CURRENT_MX_MAX_MEMBERS];
static unsigned stored_count, descriptor_creates, descriptor_reads;
static unsigned apply_calls, apply_fault_at;
static bool invalid_apply_token;
static ClusterCtrcApplyResult apply_fault;
static ClusterCtrcTargetV1 applied_target;
static ClusterCtrcTxnKeyV1 applied_key;
bool cluster_enabled = true;
bool cluster_xid_striping = true;

void cluster_mxid_stripe_lazy_latch(void) { abort(); }
int cluster_conf_node_count(void) { return 4; }
MultiXactId
MultiXactIdCreateLocalCurrentMembers(int count, MultiXactMember *members)
{
	UT_ASSERT(count > 0 && count <= lengthof(stored_members));
	memcpy(stored_members, members, count * sizeof(*members));
	stored_count = count; descriptor_creates++;
	return 32; /* Actual stripe derives local slot 0. */
}
int
GetMultiXactIdMembers(MultiXactId mxid, MultiXactMember **members, bool upgrade, bool lockonly)
{
	UT_ASSERT_EQ(mxid, 32); UT_ASSERT(!upgrade && !lockonly);
	*members = malloc(stored_count * sizeof(**members));
	UT_ASSERT(*members != NULL);
	memcpy(*members, stored_members, stored_count * sizeof(**members));
	descriptor_reads++;
	return stored_count;
}
ClusterCtrcApplyResult
cluster_ctrc_receipt_apply_shared(const ClusterCtrcReceiptHandle *handle,
	const ClusterCtrcTargetV1 *target, ClusterCtrcApplyToken *token)
{
	UT_ASSERT(handle->valid); UT_ASSERT_EQ(target->kind, CTRC_TARGET_EXACT_TID);
	apply_calls++; applied_target = *target; applied_key = handle->key;
	memset(token, 0, sizeof(*token));
	if (apply_fault_at == apply_calls) return apply_fault;
	token->valid = !invalid_apply_token;
	return CLUSTER_CTRC_APPLY_APPLIED;
}
static void
publication_setup(ReceiptFixture *f)
{
	receipt_setup(f);
	cluster_mxid_stripe_latch_runtime(true, 0, FirstMultiXactId);
	stored_count = descriptor_creates = descriptor_reads = apply_calls = apply_fault_at = 0;
	invalid_apply_token = false; apply_fault = CLUSTER_CTRC_APPLY_RETRY_REQUIRED;
	memset(stored_members, 0, sizeof(stored_members));
	memset(&applied_target, 0, sizeof(applied_target));
	memset(&applied_key, 0, sizeof(applied_key));
}
static bool
publication_prepare(TransactionId requester, int kind, MxPublicationReadout *out)
{
	ClusterCtrcTargetV1 pending = { 0 };
	pending.kind = CTRC_TARGET_PAGE_PENDING_OFFNUM;
	return mx_publication_prepare(requester, kind, LockTupleExclusive, &pending, out);
}
static int
publication_apply(bool prior_applied, MxPublicationReadout *out)
{
	ClusterCtrcTargetV1 exact = { 0 };
	exact.kind = CTRC_TARGET_EXACT_TID;
	exact.multixact_id = out->descriptor;
	exact.descriptor_hash = out->descriptor_hash;
	exact.block_number = 23; exact.offset_number = 3;
	return mx_publication_apply(&exact, prior_applied, out);
}
UT_TEST(publication_live_child_reaches_native_apply)
{
	ReceiptFixture f; MxPublicationReadout out;
	publication_setup(&f);
	UT_ASSERT(receipt_retain(&f, 1, test_operation_consumer_child));
	UT_ASSERT(publication_prepare(test_operation_consumer_child, 0, &out));
	UT_ASSERT_EQ(out.stage, CMX_HEAP_STAGE_RECEIPT_PREPARED);
	UT_ASSERT_EQ(out.receipt_count, 1); UT_ASSERT(out.active);
	UT_ASSERT_EQ(descriptor_creates, 1); UT_ASSERT_EQ(descriptor_reads, 1);
	UT_ASSERT_EQ(stored_members[0].xid, test_operation_consumer_child);
	UT_ASSERT_EQ(stored_members[0].status, MultiXactStatusUpdate);
	UT_ASSERT_EQ(test_undo_pending_key.xid, TEST_ORIGIN_XID);
	UT_ASSERT_EQ(test_undo_pending_publication.reference_kind, CTRC_REF_CURRENT_MX_UPDATER);
	UT_ASSERT_EQ(test_undo_pending_publication.descriptor_hash, out.descriptor_hash);
	UT_ASSERT_EQ(publication_apply(false, &out), 0);
	UT_ASSERT(out.any_applied && out.apply_started && out.active);
	UT_ASSERT_EQ(out.stage, CMX_HEAP_STAGE_RECEIPT_APPLIED);
	UT_ASSERT_EQ(applied_key.xid, TEST_ORIGIN_XID);
	UT_ASSERT_EQ(applied_target.multixact_id, out.descriptor);
	UT_ASSERT_EQ(applied_target.descriptor_hash, out.descriptor_hash);
	mx_publication_cancel(&out); UT_ASSERT(out.active);
	UT_ASSERT_EQ(test_undo_pending_cancel_calls, 0);
}
UT_TEST(publication_native_stamp_roles_and_survivors)
{
	for (unsigned kind = 0; kind < 3; kind++) {
		ReceiptFixture f; MxPublicationReadout out;
		publication_setup(&f); UT_ASSERT(receipt_retain(&f, 2, TEST_ORIGIN_XID));
		UT_ASSERT(publication_prepare(TEST_ORIGIN_XID, kind, &out));
		UT_ASSERT_EQ(out.receipt_count, kind == 2 ? 1 : 2);
		UT_ASSERT_EQ(stored_members[0].xid, test_operation_consumer_child);
		UT_ASSERT_EQ(stored_members[0].status, MultiXactStatusForShare);
		UT_ASSERT(out.infomask & HEAP_XMAX_IS_MULTI);
		if (kind < 2) {
			UT_ASSERT_EQ(stored_members[1].xid, TEST_ORIGIN_XID);
			UT_ASSERT_EQ(stored_members[1].status, kind ? MultiXactStatusForUpdate : MultiXactStatusUpdate);
		}
		UT_ASSERT_EQ(!!(out.infomask & HEAP_XMAX_LOCK_ONLY), kind != 0);
		UT_ASSERT_EQ(publication_apply(false, &out), 0);
		UT_ASSERT_EQ(apply_calls, kind == 2 ? 1 : 2);
	}
}
UT_TEST(publication_empty_survivors_allocate_nothing)
{
	ReceiptFixture f; MxPublicationReadout out;
	publication_setup(&f); receipt_discard(&f);
	UT_ASSERT(receipt_retain(&f, 1, TEST_ORIGIN_XID));
	UT_ASSERT(publication_prepare(TEST_ORIGIN_XID, 2, &out));
	UT_ASSERT(!out.active); UT_ASSERT_EQ(out.receipt_count, 0);
	UT_ASSERT_EQ(descriptor_creates, 0); UT_ASSERT_EQ(test_undo_pending_prepare_calls, 0);
	mx_publication_cancel(&out); UT_ASSERT_EQ(test_undo_pending_cancel_calls, 0);
}
UT_TEST(publication_prepare_refusal_cancels_only_new_handles)
{
	for (unsigned duplicate = 0; duplicate < 2; duplicate++) {
		ReceiptFixture f; MxPublicationReadout out;
		publication_setup(&f); UT_ASSERT(receipt_retain(&f, 2, TEST_ORIGIN_XID));
		receipt_refuse_at = 2; receipt_duplicate_at = duplicate ? 1 : 0;
		operation_worker_expected_logs = 1;
		UT_ASSERT(!publication_prepare(TEST_ORIGIN_XID, 0, &out));
		UT_ASSERT(!out.active); UT_ASSERT_EQ(out.receipt_count, 1);
		UT_ASSERT_EQ(test_undo_pending_cancel_calls, duplicate ? 0 : 1);
		UT_ASSERT_EQ(apply_calls, 0); mx_publication_cancel(&out);
		UT_ASSERT_EQ(test_undo_pending_cancel_calls, duplicate ? 0 : 1);
	}
}
UT_TEST(publication_first_retry_is_zero_apply)
{
	ReceiptFixture f; MxPublicationReadout out;
	publication_setup(&f); UT_ASSERT(receipt_retain(&f, 2, TEST_ORIGIN_XID));
	UT_ASSERT(publication_prepare(TEST_ORIGIN_XID, 0, &out));
	apply_fault_at = 1;
	UT_ASSERT_EQ(publication_apply(false, &out), 1);
	UT_ASSERT(!out.any_applied && !out.active); UT_ASSERT(out.apply_started);
	UT_ASSERT_EQ(apply_calls, 1); UT_ASSERT_EQ(test_undo_pending_cancel_calls, 2);
	UT_ASSERT_EQ(publication_apply(false, &out), 2); UT_ASSERT_EQ(apply_calls, 1);
}
UT_TEST(publication_second_retry_cannot_retry_attempt)
{
	ReceiptFixture f; MxPublicationReadout out;
	publication_setup(&f); UT_ASSERT(receipt_retain(&f, 2, TEST_ORIGIN_XID));
	UT_ASSERT(publication_prepare(TEST_ORIGIN_XID, 0, &out));
	apply_fault_at = 2;
	UT_ASSERT_EQ(publication_apply(false, &out), 2);
	UT_ASSERT(out.any_applied); UT_ASSERT(!out.active);
	UT_ASSERT_EQ(apply_calls, 2);
}
UT_TEST(publication_prior_undo_apply_forbids_retry)
{
	ReceiptFixture f; MxPublicationReadout out;
	publication_setup(&f); UT_ASSERT(receipt_retain(&f, 1, test_operation_consumer_child));
	UT_ASSERT(publication_prepare(test_operation_consumer_child, 0, &out));
	apply_fault_at = 1;
	UT_ASSERT_EQ(publication_apply(true, &out), 2);
	UT_ASSERT(out.any_applied); UT_ASSERT(!out.active); UT_ASSERT_EQ(apply_calls, 1);
}
UT_TEST(publication_missing_apply_token_never_claims_zero)
{
	ReceiptFixture f; MxPublicationReadout out;
	publication_setup(&f); UT_ASSERT(receipt_retain(&f, 1, test_operation_consumer_child));
	UT_ASSERT(publication_prepare(test_operation_consumer_child, 0, &out));
	invalid_apply_token = true;
	UT_ASSERT_EQ(publication_apply(false, &out), 2);
	UT_ASSERT(out.any_applied); UT_ASSERT(!out.active);
}
UT_TEST(publication_unknown_apply_never_becomes_retry)
{
	ReceiptFixture f; MxPublicationReadout out;
	publication_setup(&f); UT_ASSERT(receipt_retain(&f, 1, test_operation_consumer_child));
	UT_ASSERT(publication_prepare(test_operation_consumer_child, 0, &out));
	apply_fault_at = 1; apply_fault = CLUSTER_CTRC_APPLY_FAIL_CLOSED;
	UT_ASSERT_EQ(publication_apply(false, &out), 2);
	UT_ASSERT(!out.any_applied && !out.active);
}
UT_TEST(publication_cancel_prepared_is_idempotent)
{
	ReceiptFixture f; MxPublicationReadout out;
	publication_setup(&f); UT_ASSERT(receipt_retain(&f, 2, TEST_ORIGIN_XID));
	receipt_duplicate_at = 1;
	UT_ASSERT(publication_prepare(TEST_ORIGIN_XID, 0, &out));
	mx_publication_cancel(&out); mx_publication_cancel(&out);
	UT_ASSERT(!out.active); UT_ASSERT_EQ(out.stage, CMX_HEAP_STAGE_CANCELLED);
	UT_ASSERT_EQ(test_undo_pending_cancel_calls, 1); UT_ASSERT_EQ(apply_calls, 0);
}
int main(void)
{
	UT_PLAN(10);
	UT_RUN(publication_live_child_reaches_native_apply);
	UT_RUN(publication_native_stamp_roles_and_survivors);
	UT_RUN(publication_empty_survivors_allocate_nothing);
	UT_RUN(publication_prepare_refusal_cancels_only_new_handles);
	UT_RUN(publication_first_retry_is_zero_apply);
	UT_RUN(publication_second_retry_cannot_retry_attempt);
	UT_RUN(publication_prior_undo_apply_forbids_retry);
	UT_RUN(publication_missing_apply_token_never_claims_zero);
	UT_RUN(publication_unknown_apply_never_becomes_retry);
	UT_RUN(publication_cancel_prepared_is_idempotent);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
