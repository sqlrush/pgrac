/* Typed effects through real heap retention, runtime identity binder and
 * heap PREPARE/cancel.  Storage/capability/receipt-table leaves remain the
 * existing explicit outcome fixture; this is not native TT40/SQL evidence.
 * Portions Copyright (c) 2026, pgrac contributors */
int receipt_unused_outcome_main(int argc, char **argv);
#define cluster_ctrc_receipt_prepare_shared receipt_fixture_prepare_shared
#define main receipt_unused_outcome_main
#include "test_cluster_r4_tx_outcome.c"
#undef main
#undef cluster_ctrc_receipt_prepare_shared
#include "test_cluster_mx_operation_receipt.h"
/* Only the external receipt-table leaf is fault-injected.  The runtime
 * identity reconstruction and heap allocation/cancel bodies remain real. */
static unsigned receipt_prepare_attempts, receipt_refuse_at, receipt_duplicate_at;
ClusterCtrcPrepareResult cluster_ctrc_receipt_prepare_shared(
	const ClusterCtrcTxnKeyV1 *key, const ClusterCtrcParticipantIdentity *participant,
	uint32 grant, const ClusterCtrcPublicationIdV1 *publication,
	const ClusterCtrcTargetV1 *target, ClusterCtrcReceiptHandle *handle);
ClusterCtrcPrepareResult
cluster_ctrc_receipt_prepare_shared(const ClusterCtrcTxnKeyV1 *key,
	const ClusterCtrcParticipantIdentity *participant, uint32 grant,
	const ClusterCtrcPublicationIdV1 *publication,
	const ClusterCtrcTargetV1 *target, ClusterCtrcReceiptHandle *handle)
{
	ClusterCtrcPrepareResult result;
	receipt_prepare_attempts++;
	if (receipt_refuse_at == receipt_prepare_attempts)
		return CLUSTER_CTRC_PREPARE_REFUSED;
	result = receipt_fixture_prepare_shared(key, participant, grant, publication, target, handle);
	return receipt_duplicate_at == receipt_prepare_attempts
		? CLUSTER_CTRC_PREPARE_DUPLICATE : result;
}
typedef struct ReceiptFixture {
	ClusterCurrentMxKey mx;
	ClusterCurrentMxMemberDesc members[2];
	ClusterSavepointOperationKey keys[2];
	ClusterCurrentMemberOperationProof proofs[2];
	uint32 capabilities[2];
	MxReceiptReadout out;
} ReceiptFixture;
static void
receipt_setup(ReceiptFixture *f)
{
	reset_exact_origin_fixture();
	receipt_prepare_attempts = receipt_refuse_at = receipt_duplicate_at = 0;
	cluster_node_id = 0;
	test_operation_consumer_child = TEST_ORIGIN_XID + 2;
	memset(f, 0, sizeof(*f));
	f->mx.multixact_id = 17;
	f->mx.cluster_epoch = test_formation_epoch;
	for (uint16 i = 0; i < 2; i++) {
		ClusterCurrentMemberOperationProof *p = &f->proofs[i];
		f->members[i].xid = i == 0 ? test_operation_consumer_child : TEST_ORIGIN_XID;
		f->members[i].member_status = MultiXactStatusForShare;
		p->base.key = (ClusterCurrentMemberProofKey){ .origin_node_id = 0,
			.undo_segment_id = TEST_TT_SEGMENT, .tt_slot_id = TEST_TT_OFFSET + 1,
			.cluster_epoch = test_formation_epoch, .local_xid = TEST_ORIGIN_XID,
			.segment_generation = 23, .slot_wrap = TEST_ORIGIN_WRAP,
			.binding_version = CLUSTER_CURRENT_MEMBER_PROOF_BINDING_VERSION };
		p->base.member_xid = f->members[i].xid;
		p->base.member_status = f->members[i].member_status;
		p->base.member_ordinal = i;
		p->base.state = CCM_ACTIVE;
		ClusterCurrentMemberProofSetCtrcGrant(&p->base, test_ctrc_touch_grant);
		p->format_version = 2;
		f->capabilities[i] = 313;
	}
	f->keys[0] = (ClusterSavepointOperationKey){ .locator = test_origin_locator,
		.target = { .locator = { 1663, 42, 77 }, .forknum = MAIN_FORKNUM,
			.blockno = 23, .offnum = 3 },
		.top_fxid = FullTransactionIdFromEpochAndXid(9, TEST_ORIGIN_XID),
		.operation_fxid = FullTransactionIdFromEpochAndXid(9, test_operation_consumer_child) };
	f->proofs[0].operation_uba = f->keys[0].locator.uba;
	f->proofs[0].top_fxid = f->keys[0].top_fxid;
	f->proofs[0].operation_fxid = f->keys[0].operation_fxid;
	f->proofs[0].effect_state = CLUSTER_SAVEPOINT_EFFECT_LIVE;
	f->proofs[0].flags = CLUSTER_CURRENT_OPERATION_PROOF_EXACT;
}
static bool
receipt_retain(ReceiptFixture *f, uint16 count, TransactionId requester)
{
	return mx_receipt_retain(&f->mx, f->members, f->keys, f->proofs,
		f->capabilities, count, requester, MultiXactStatusForKeyShare, 1234, 2, &f->out);
}
static bool
receipt_prepare(TransactionId requester, uint16 *count)
{
	ClusterCtrcTargetV1 target = { 0 };
	target.kind = CTRC_TARGET_PAGE_PENDING_OFFNUM;
	return mx_receipt_prepare(requester, 9876, &target, count);
}
static void
receipt_discard(ReceiptFixture *f)
{
	f->proofs[0].effect_state = CLUSTER_SAVEPOINT_EFFECT_DISCARDED;
	ClusterCurrentMemberProofSetCtrcGrant(&f->proofs[0].base, 0);
	f->capabilities[0] = 0;
}
UT_TEST(receipt_live_child_keeps_raw_member_and_canonical_top)
{
	ReceiptFixture f;
	uint16 count = 0;
	receipt_setup(&f);
	UT_ASSERT(receipt_retain(&f, 1, test_operation_consumer_child));
	UT_ASSERT_EQ(f.out.normalized_count, 1);
	UT_ASSERT_EQ(f.out.proof_count, 1);
	UT_ASSERT_EQ(f.out.normalized[0].xid, test_operation_consumer_child);
	UT_ASSERT_EQ(f.out.proofs[0].member_xid, test_operation_consumer_child);
	UT_ASSERT_EQ(f.out.proofs[0].key.local_xid, TEST_ORIGIN_XID);
	UT_ASSERT_EQ(f.out.operation_id, 1234);
	UT_ASSERT_EQ(f.out.attempt_generation, 3);
	UT_ASSERT(receipt_prepare(test_operation_consumer_child, &count));
	UT_ASSERT_EQ(count, 1);
	UT_ASSERT_EQ(test_undo_pending_prepare_calls, 1);
	UT_ASSERT_EQ(test_undo_pending_key.xid, TEST_ORIGIN_XID);
	UT_ASSERT_EQ(test_undo_pending_key.segment_generation, 23);
	UT_ASSERT_EQ(test_undo_pending_key.slot_wrap, TEST_ORIGIN_WRAP);
	UT_ASSERT_EQ(test_undo_pending_participant.capability_record_generation, 313);
	UT_ASSERT_EQ(test_undo_pending_publication.operation_id, 1234);
	UT_ASSERT_EQ(test_undo_pending_publication.attempt_generation, 3);
	UT_ASSERT_EQ(test_undo_pending_publication.descriptor_hash, 9876);
	UT_ASSERT_EQ(test_candidate_acquire_calls, 0);
	UT_ASSERT_EQ(test_candidate_sample_calls, 0);
	UT_ASSERT_EQ(test_candidate_block0_copy_calls, 0);
}
UT_TEST(receipt_discarded_member_has_no_receipt_or_rewritten_top)
{
	ReceiptFixture f;
	uint16 count;
	receipt_setup(&f);
	receipt_discard(&f);
	UT_ASSERT(receipt_retain(&f, 2, TEST_ORIGIN_XID));
	UT_ASSERT_EQ(f.out.normalized_count, 1);
	UT_ASSERT_EQ(f.out.normalized[0].xid, TEST_ORIGIN_XID);
	UT_ASSERT_EQ(f.out.proof_count, 1);
	UT_ASSERT_EQ(f.out.proofs[0].member_ordinal, 1);
	UT_ASSERT(receipt_prepare(TEST_ORIGIN_XID, &count));
	UT_ASSERT_EQ(count, 1);
	UT_ASSERT_EQ(test_undo_pending_publication.member_ordinal, 0);
	UT_ASSERT_EQ(f.proofs[0].base.state, CCM_ACTIVE);
}
UT_TEST(receipt_terminal_locker_has_no_retained_grant)
{
	for (unsigned state = CCM_COMMITTED; state <= CCM_ABORTED; state++) {
		ReceiptFixture f;
		receipt_setup(&f);
		f.proofs[0].base.state = state;
		f.proofs[0].base.commit_scn = state == CCM_COMMITTED ? 500 : InvalidScn;
		ClusterCurrentMemberProofSetCtrcGrant(&f.proofs[0].base, 0);
		f.capabilities[0] = 0;
		UT_ASSERT(receipt_retain(&f, 2, TEST_ORIGIN_XID));
		UT_ASSERT_EQ(f.out.proof_count, 1);
		UT_ASSERT_EQ(f.out.proofs[0].member_xid, TEST_ORIGIN_XID);
	}
}
UT_TEST(receipt_invalid_set_or_capability_clears_all_output)
{
	for (unsigned fault = 0; fault < 9; fault++) {
		ReceiptFixture f;
		MxReceiptReadout zero = { 0 };
		receipt_setup(&f);
		switch (fault) {
		case 0: f.proofs[0].base.member_ordinal++; break;
		case 1: f.proofs[0].base.key.local_xid++; break;
		case 2: f.proofs[0].operation_fxid = FullTransactionIdFromEpochAndXid(10, test_operation_consumer_child); break;
		case 3: f.proofs[0].effect_state = 0; break;
		case 4: f.capabilities[0] = 0; break;
		case 5: f.proofs[1].base.state = CCM_UNKNOWN; break;
		case 6: f.proofs[0].base.state = CCM_SELF; break;
		case 7: f.proofs[1].base.member_xid++; break;
		case 8: ClusterCurrentMemberProofSetCtrcGrant(&f.proofs[0].base, 0); break;
		}
		UT_ASSERT(!receipt_retain(&f, 2, TEST_ORIGIN_XID));
		UT_ASSERT_EQ(memcmp(&f.out, &zero, sizeof(zero)), 0);
		UT_ASSERT_EQ(test_undo_pending_prepare_calls, 0);
	}
}
UT_TEST(receipt_attempt_overflow_or_zero_operation_clears_output)
{
	for (unsigned fault = 0; fault < 2; fault++) {
		ReceiptFixture f;
		MxReceiptReadout zero = { 0 };
		receipt_setup(&f);
		UT_ASSERT(!mx_receipt_retain(&f.mx, f.members, f.keys, f.proofs, f.capabilities,
			1, test_operation_consumer_child, MultiXactStatusForShare,
			fault == 0 ? 0 : 1234, fault == 0 ? 0 : UINT32_MAX, &f.out));
		UT_ASSERT_EQ(memcmp(&f.out, &zero, sizeof(zero)), 0);
	}
}
UT_TEST(receipt_revoked_admission_fails_original_identity_binder)
{
	ReceiptFixture f;
	uint16 count = 99;
	receipt_setup(&f);
	UT_ASSERT(receipt_retain(&f, 1, test_operation_consumer_child));
	test_admission_revoked = true;
	operation_worker_expected_logs = 1;
	UT_ASSERT(!receipt_prepare(test_operation_consumer_child, &count));
	UT_ASSERT_EQ(count, 0);
	UT_ASSERT_EQ(test_undo_pending_prepare_calls, 0);
}
UT_TEST(receipt_all_discarded_retains_no_authority_for_raw_child)
{
	ReceiptFixture f;
	receipt_setup(&f);
	receipt_discard(&f);
	UT_ASSERT(receipt_retain(&f, 1, TEST_ORIGIN_XID));
	UT_ASSERT_EQ(f.out.proof_count, 0);
	UT_ASSERT_EQ(f.out.normalized_count, 1);
	UT_ASSERT_EQ(f.out.normalized[0].xid, TEST_ORIGIN_XID);
}
UT_TEST(receipt_prepare_refusal_cancels_only_new_prior_receipts)
{
	for (unsigned duplicate = 0; duplicate < 2; duplicate++) {
		ReceiptFixture f;
		uint16 count = 99;
		receipt_setup(&f);
		UT_ASSERT(receipt_retain(&f, 2, TEST_ORIGIN_XID));
		receipt_refuse_at = 2;
		receipt_duplicate_at = duplicate ? 1 : 0;
		operation_worker_expected_logs = 1;
		UT_ASSERT(!receipt_prepare(TEST_ORIGIN_XID, &count));
		UT_ASSERT_EQ(receipt_prepare_attempts, 2);
		UT_ASSERT_EQ(count, 1);
		UT_ASSERT_EQ(test_undo_pending_cancel_calls, duplicate ? 0 : 1);
		UT_ASSERT_EQ(operation_worker_expected_logs, 0);
	}
}
UT_TEST(receipt_null_empty_and_over_limit_inputs_clear_output)
{
	for (unsigned fault = 0; fault < 7; fault++) {
		ReceiptFixture f;
		MxReceiptReadout zero = { 0 };
		receipt_setup(&f);
		UT_ASSERT(!mx_receipt_retain(fault == 0 ? NULL : &f.mx,
			fault == 1 ? NULL : f.members, fault == 2 ? NULL : f.keys,
			fault == 3 ? NULL : f.proofs, fault == 4 ? NULL : f.capabilities,
			fault == 5 ? 0 : fault == 6 ? 257 : 1,
			TEST_ORIGIN_XID, MultiXactStatusForShare, 1234, 0, &f.out));
		UT_ASSERT_EQ(memcmp(&f.out, &zero, sizeof(zero)), 0);
	}
}
UT_TEST(receipt_committed_live_updater_is_denied_not_silently_dropped)
{
	ReceiptFixture f;
	MxReceiptReadout zero = { 0 };
	receipt_setup(&f);
	f.members[0].member_status = f.proofs[0].base.member_status = MultiXactStatusUpdate;
	f.proofs[0].base.state = CCM_COMMITTED;
	f.proofs[0].base.commit_scn = 500;
	ClusterCurrentMemberProofSetCtrcGrant(&f.proofs[0].base, 0);
	UT_ASSERT(!receipt_retain(&f, 1, TEST_ORIGIN_XID));
	UT_ASSERT_EQ(memcmp(&f.out, &zero, sizeof(zero)), 0);
}
#include "test_cluster_mx_operation_heap.inc"
#ifdef RECEIPT_PUBLICATION_FIXTURE
#define main publication_unused_receipt_main
#endif
int main(void)
{
	UT_PLAN(23);
	UT_RUN(receipt_live_child_keeps_raw_member_and_canonical_top);
	UT_RUN(receipt_discarded_member_has_no_receipt_or_rewritten_top);
	UT_RUN(receipt_terminal_locker_has_no_retained_grant);
	UT_RUN(receipt_invalid_set_or_capability_clears_all_output);
	UT_RUN(receipt_attempt_overflow_or_zero_operation_clears_output);
	UT_RUN(receipt_revoked_admission_fails_original_identity_binder);
	UT_RUN(receipt_all_discarded_retains_no_authority_for_raw_child);
	UT_RUN(receipt_prepare_refusal_cancels_only_new_prior_receipts);
	UT_RUN(receipt_null_empty_and_over_limit_inputs_clear_output);
	UT_RUN(receipt_committed_live_updater_is_denied_not_silently_dropped);
	UT_RUN(heap_typed_continue_reaches_real_receipt_prepare);
	UT_RUN(heap_typed_conflict_policies_keep_canonical_wait_and_no_receipts);
	UT_RUN(heap_typed_exact_owner_maps_cid_without_retaining_self);
	UT_RUN(heap_typed_self_updated_retains_the_independent_successor);
	UT_RUN(heap_typed_raw_owner_collision_never_projects_self);
	UT_RUN(heap_typed_committed_updater_maps_independent_B_to_successor);
	UT_RUN(heap_typed_active_compatible_follow_retains_A_and_uses_B);
	UT_RUN(heap_typed_discarded_updater_never_exports_stale_successor);
	UT_RUN(heap_typed_B_required_even_for_nonfollowing_or_discarded_updater);
	UT_RUN(heap_typed_B_binding_drift_clears_entire_decision);
	UT_RUN(heap_typed_bad_member_or_capability_has_no_partial_TM_Ok);
	UT_RUN(heap_typed_invalid_attempt_or_missing_input_is_whole_unknown);
	UT_RUN(heap_typed_deleted_and_precheck_results_do_not_allocate_receipts);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
