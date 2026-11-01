/* Same qualified typed batch through the actual heap decision/receipt body.
 * The existing receipt fixture controls external storage/admission leaves. */
typedef struct HeapOperationFixture {
	ReceiptFixture receipt;
	ClusterCurrentMxRequestContext ctx;
	ClusterR4CrOwnerChallenge owner;
	ClusterCurrentUpdaterChallenge challenge;
	ClusterCurrentUpdaterProof updater;
	ItemPointerData tid;
	MxHeapReadout out;
} HeapOperationFixture;
static void
heap_operation_setup(HeapOperationFixture *f)
{
	memset(f, 0, sizeof(*f));
	receipt_setup(&f->receipt);
	f->ctx = (ClusterCurrentMxRequestContext){ .mxkey = f->receipt.mx,
		.top_xid = 1000, .current_member_xid = 1002, .curcid = 5,
		.tuple_cmax = InvalidCommandId, .precheck_result = TM_BeingModified,
		.desired_status = MultiXactStatusForKeyShare, .lock_mode = LockTupleKeyShare,
		.wait_policy = LockWaitBlock, .action = CCM_ACTION_LOCK,
		.tuple_shape = CCM_SHAPE_LOCK_ONLY, .wait_for_conflict = 1,
		.updater_origin_node_id = -1 };
	ItemPointerSet(&f->tid, 23, 4);
}
static ClusterCurrentMxDecision
heap_operation_consume(HeapOperationFixture *f, bool with_b)
{
	return mx_receipt_decide(f->receipt.members, f->receipt.keys, f->receipt.proofs,
		f->receipt.capabilities, 1, &f->ctx, &f->owner,
		with_b ? &f->challenge : NULL, with_b ? &f->updater : NULL,
		with_b ? &f->tid : NULL, 1234, 2, &f->out);
}
static void
heap_operation_updater(HeapOperationFixture *f, MultiXactStatus status)
{
	f->receipt.members[0].member_status = f->receipt.proofs[0].base.member_status = status;
	f->ctx.tuple_shape = CCM_SHAPE_UPDATED;
	f->ctx.updater_origin_node_id = 0;
	f->challenge = (ClusterCurrentUpdaterChallenge){
		.candidate_next_xmin_alias = { .origin_node_id = 0,
			.undo_record_segment_id = TEST_RECORD_SEGMENT, .tt_slot_id = TEST_TT_OFFSET + 1,
			.cluster_epoch = f->ctx.mxkey.cluster_epoch, .local_xid = test_operation_consumer_child },
		.candidate_next_xmin_locator = f->receipt.keys[0].locator,
		.updater_xid = test_operation_consumer_child, .member_ordinal = 0 };
	f->challenge.candidate_next_xmin_locator.xid = test_operation_consumer_child;
	f->challenge.candidate_next_xmin_locator.tt_wrap = TT_WRAP_INVALID;
	f->challenge.candidate_next_xmin_locator.uba = uba_encode(TEST_RECORD_SEGMENT, 29,
		TEST_TT_OFFSET, 0);
	f->updater = (ClusterCurrentUpdaterProof){ .mxkey = f->ctx.mxkey,
		.candidate_next_xmin_alias = f->challenge.candidate_next_xmin_alias,
		.candidate_next_xmin_locator = f->challenge.candidate_next_xmin_locator,
		.updater_xid = test_operation_consumer_child, .member_ordinal = 0,
		.verdict = CUCP_MATCH };
	f->updater.candidate_next_xmin_locator.tt_wrap = TEST_ORIGIN_WRAP;
}
static void
heap_operation_owner(HeapOperationFixture *f)
{
	f->ctx.top_xid = TEST_ORIGIN_XID;
	f->ctx.current_member_xid = test_operation_consumer_child;
	f->owner = (ClusterR4CrOwnerChallenge){ .top_fxid = f->receipt.keys[0].top_fxid,
		.active_lsn = 10, .segment_id = TEST_TT_SEGMENT, .segment_generation = 23,
		.top_xid = TEST_ORIGIN_XID, .slot_offset = TEST_TT_OFFSET,
		.slot_wrap = TEST_ORIGIN_WRAP, .origin_instance = 1,
		.publish_state = CLUSTER_CANONICAL_TXN_PUBLISHED, .flags = CLUSTER_R4_CR_OWNER_PRESENT };
}
static void
heap_operation_no_receipts(const MxHeapReadout *out)
{
	MxReceiptReadout zero = { 0 };
	UT_ASSERT_EQ(memcmp(&out->retained, &zero, sizeof(zero)), 0);
	UT_ASSERT_EQ(test_undo_pending_prepare_calls, 0);
}
static void
heap_operation_unknown(const MxHeapReadout *out)
{
	ClusterTTStatusKey zero = { 0 };
	heap_operation_no_receipts(out);
	UT_ASSERT_EQ(out->result, TM_BeingModified);
	UT_ASSERT_EQ(out->authoritative_updater, InvalidTransactionId);
	UT_ASSERT(!out->follow_updated_chain);
	UT_ASSERT(!out->successor.valid);
	UT_ASSERT_EQ(memcmp(&out->wait_key, &zero, sizeof(zero)), 0);
}
UT_TEST(heap_typed_continue_reaches_real_receipt_prepare)
{
	HeapOperationFixture f;
	uint16 count = 99;
	heap_operation_setup(&f);
	heap_operation_owner(&f);
	UT_ASSERT_EQ(heap_operation_consume(&f, false), CMDL_CONTINUE);
	UT_ASSERT_EQ(f.out.result, TM_Ok);
	UT_ASSERT_EQ(f.out.retained.normalized_count, 1);
	UT_ASSERT_EQ(f.out.retained.normalized[0].xid, test_operation_consumer_child);
	UT_ASSERT_EQ(f.out.retained.proof_count, 1);
	UT_ASSERT_EQ(f.out.retained.proofs[0].key.local_xid, TEST_ORIGIN_XID);
	UT_ASSERT_EQ(f.out.retained.capabilities[0], 313);
	UT_ASSERT_EQ(f.out.retained.operation_id, 1234);
	UT_ASSERT_EQ(f.out.retained.attempt_generation, 3);
	UT_ASSERT(receipt_prepare(test_operation_consumer_child, &count));
	UT_ASSERT_EQ(count, 1);
}
UT_TEST(heap_typed_conflict_policies_keep_canonical_wait_and_no_receipts)
{
	static const ClusterCurrentMxDecision decisions[] = {
		CMDL_WAIT_MEMBER, CMDL_WOULD_BLOCK, CMDL_LOCK_NOT_AVAILABLE, CMDL_BEING_MODIFIED };
	for (unsigned i = 0; i < lengthof(decisions); i++) {
		HeapOperationFixture f;
		heap_operation_setup(&f);
		f.ctx.lock_mode = LockTupleExclusive;
		f.ctx.desired_status = MultiXactStatusForUpdate;
		if (i == 1) f.ctx.wait_policy = LockWaitSkip;
		if (i == 2) f.ctx.wait_policy = LockWaitError;
		if (i == 3) {
			f.ctx.action = CCM_ACTION_UPDATE;
			f.ctx.desired_status = MultiXactStatusUpdate;
			f.ctx.wait_for_conflict = 0;
		}
		UT_ASSERT_EQ(heap_operation_consume(&f, false), decisions[i]);
		UT_ASSERT_EQ(f.out.result, i == 1 ? TM_WouldBlock : TM_BeingModified);
		UT_ASSERT_EQ(f.out.wait_key.local_xid, i == 0 ? TEST_ORIGIN_XID : InvalidTransactionId);
		if (i == 0) UT_ASSERT_EQ(f.out.wait_key.tt_slot_id, TEST_TT_OFFSET + 1);
		heap_operation_no_receipts(&f.out);
	}
}
UT_TEST(heap_typed_exact_owner_maps_cid_without_retaining_self)
{
	for (unsigned invisible = 0; invisible < 2; invisible++) {
		HeapOperationFixture f;
		heap_operation_setup(&f);
		heap_operation_owner(&f);
		f.receipt.members[0].member_status = f.receipt.proofs[0].base.member_status = MultiXactStatusUpdate;
		f.ctx.tuple_shape = CCM_SHAPE_DELETED;
		f.ctx.tuple_cmax = invisible ? 4 : 5;
		UT_ASSERT_EQ(heap_operation_consume(&f, false), invisible ? CMDL_INVISIBLE : CMDL_SELF_MODIFIED);
		UT_ASSERT_EQ(f.out.result, invisible ? TM_Invisible : TM_SelfModified);
		UT_ASSERT_EQ(f.receipt.proofs[0].base.state, CCM_ACTIVE);
		heap_operation_no_receipts(&f.out);
	}
}
UT_TEST(heap_typed_raw_owner_collision_never_projects_self)
{
	for (unsigned fault = 0; fault < 5; fault++) {
		HeapOperationFixture f;
		heap_operation_setup(&f);
		heap_operation_owner(&f);
		switch (fault) {
		case 0: f.owner.top_fxid = FullTransactionIdFromEpochAndXid(10, TEST_ORIGIN_XID); break;
		case 1: f.owner.segment_generation++; break;
		case 2: f.owner.slot_wrap++; break;
		case 3: memset(&f.owner, 0, sizeof(f.owner)); break;
		case 4: f.owner.slot_offset++; break;
		}
		UT_ASSERT_EQ(heap_operation_consume(&f, false), CMDL_UNKNOWN);
		heap_operation_unknown(&f.out);
	}
}
UT_TEST(heap_typed_committed_updater_maps_independent_B_to_successor)
{
	HeapOperationFixture f;
	heap_operation_setup(&f);
	heap_operation_updater(&f, MultiXactStatusUpdate);
	f.receipt.proofs[0].base.state = CCM_COMMITTED;
	f.receipt.proofs[0].base.commit_scn = 500;
	ClusterCurrentMemberProofSetCtrcGrant(&f.receipt.proofs[0].base, 0);
	f.ctx.follow_updates = 1;
	UT_ASSERT_EQ(heap_operation_consume(&f, true), CMDL_UPDATED);
	UT_ASSERT_EQ(f.out.result, TM_Updated);
	UT_ASSERT_EQ(f.out.authoritative_updater, test_operation_consumer_child);
	UT_ASSERT(f.out.successor.valid);
	if (f.out.successor.valid) {
		UT_ASSERT_EQ(ItemPointerGetBlockNumber(&f.out.successor.tid), 23);
		UT_ASSERT_EQ(ItemPointerGetOffsetNumber(&f.out.successor.tid), 4);
	}
	UT_ASSERT_EQ(f.out.successor.updater_xid, test_operation_consumer_child);
	UT_ASSERT_EQ(f.out.successor.locator_uba_raw[0], f.challenge.candidate_next_xmin_locator.uba.raw[0]);
	UT_ASSERT_EQ(f.out.successor.locator_uba_raw[1], f.challenge.candidate_next_xmin_locator.uba.raw[1]);
	UT_ASSERT(!f.out.follow_updated_chain);
	heap_operation_no_receipts(&f.out);
}
UT_TEST(heap_typed_active_compatible_follow_retains_A_and_uses_B)
{
	HeapOperationFixture f;
	heap_operation_setup(&f);
	heap_operation_updater(&f, MultiXactStatusNoKeyUpdate);
	f.ctx.follow_updates = 1;
	UT_ASSERT_EQ(heap_operation_consume(&f, true), CMDL_FOLLOW_UPDATED);
	UT_ASSERT_EQ(f.out.result, TM_Ok);
	UT_ASSERT(f.out.follow_updated_chain);
	UT_ASSERT(f.out.successor.valid);
	UT_ASSERT_EQ(f.out.retained.proof_count, 1);
	UT_ASSERT_EQ(f.out.retained.proofs[0].member_xid, test_operation_consumer_child);
	UT_ASSERT_EQ(f.out.retained.proofs[0].key.local_xid, TEST_ORIGIN_XID);
}
UT_TEST(heap_typed_discarded_updater_never_exports_stale_successor)
{
	HeapOperationFixture f;
	heap_operation_setup(&f);
	heap_operation_updater(&f, MultiXactStatusNoKeyUpdate);
	receipt_discard(&f.receipt);
	f.ctx.follow_updates = 1;
	UT_ASSERT_EQ(heap_operation_consume(&f, true), CMDL_CONTINUE);
	UT_ASSERT_EQ(f.out.result, TM_Ok);
	UT_ASSERT_EQ(f.out.authoritative_updater, InvalidTransactionId);
	UT_ASSERT_EQ(f.out.retained.proof_count, 0);
	UT_ASSERT_EQ(f.out.retained.normalized_count, 1);
	UT_ASSERT_EQ(f.out.retained.normalized[0].xid, 1002);
	UT_ASSERT(!f.out.successor.valid);
	UT_ASSERT(!f.out.follow_updated_chain);
	UT_ASSERT_EQ(f.receipt.proofs[0].base.state, CCM_ACTIVE);
}
UT_TEST(heap_typed_B_required_even_for_nonfollowing_or_discarded_updater)
{
	for (unsigned discarded = 0; discarded < 2; discarded++) {
		HeapOperationFixture f;
		heap_operation_setup(&f);
		heap_operation_updater(&f, MultiXactStatusNoKeyUpdate);
		if (discarded) receipt_discard(&f.receipt);
		UT_ASSERT_EQ(heap_operation_consume(&f, false), CMDL_UNKNOWN);
		heap_operation_unknown(&f.out);
	}
}
UT_TEST(heap_typed_B_binding_drift_clears_entire_decision)
{
	for (unsigned fault = 0; fault < 11; fault++) {
		HeapOperationFixture f;
		heap_operation_setup(&f);
		heap_operation_updater(&f, MultiXactStatusNoKeyUpdate);
		switch (fault) {
		case 0: f.updater.verdict = CUCP_UNKNOWN; break;
		case 1: f.updater.mxkey.multixact_id++; break;
		case 2: f.updater.member_ordinal++; break;
		case 3: f.updater.updater_xid++; break;
		case 4: f.updater.candidate_next_xmin_alias.tt_slot_id++; break;
		case 5: f.updater.candidate_next_xmin_locator.tt_wrap = TT_WRAP_INVALID; break;
		case 6: f.challenge.reserved16 = 1; break;
		case 7: f.updater.reserved8 = 1; break;
		case 8: f.ctx.updater_origin_node_id = 1; break;
		case 9: ItemPointerSetInvalid(&f.tid); break;
		case 10: f.updater.candidate_next_xmin_locator.uba = f.receipt.keys[0].locator.uba; break;
		}
		UT_ASSERT_EQ(heap_operation_consume(&f, true), CMDL_UNKNOWN);
		heap_operation_unknown(&f.out);
	}
}
UT_TEST(heap_typed_bad_member_or_capability_has_no_partial_TM_Ok)
{
	for (unsigned fault = 0; fault < 4; fault++) {
		HeapOperationFixture f;
		heap_operation_setup(&f);
		switch (fault) {
		case 0: f.receipt.proofs[0].base.member_ordinal++; break;
		case 1: f.receipt.capabilities[0] = 0; break;
		case 2: f.receipt.proofs[0].operation_fxid = FullTransactionIdFromEpochAndXid(10, test_operation_consumer_child); break;
		case 3: f.receipt.proofs[0].base.state = CCM_UNKNOWN; break;
		}
		UT_ASSERT_EQ(heap_operation_consume(&f, false), CMDL_UNKNOWN);
		heap_operation_unknown(&f.out);
	}
	for (unsigned fault = 0; fault < 2; fault++) {
		HeapOperationFixture f;
		heap_operation_setup(&f);
		if (fault == 0) f.receipt.proofs[1].base.member_ordinal++;
		else f.receipt.capabilities[1] = 0;
		UT_ASSERT_EQ(mx_receipt_decide(f.receipt.members, f.receipt.keys,
			f.receipt.proofs, f.receipt.capabilities, 2, &f.ctx, &f.owner,
			NULL, NULL, NULL, 1234, 2, &f.out), CMDL_UNKNOWN);
		heap_operation_unknown(&f.out);
	}
}
UT_TEST(heap_typed_invalid_attempt_or_missing_input_is_whole_unknown)
{
	for (unsigned fault = 0; fault < 9; fault++) {
		HeapOperationFixture f;
		heap_operation_setup(&f);
		UT_ASSERT_EQ(mx_receipt_decide(fault == 0 ? NULL : f.receipt.members,
			fault == 1 ? NULL : f.receipt.keys, fault == 2 ? NULL : f.receipt.proofs,
			fault == 3 ? NULL : f.receipt.capabilities, fault == 4 ? 257 : 1,
			fault == 5 ? NULL : &f.ctx, fault == 6 ? NULL : &f.owner, NULL, NULL, NULL,
			fault == 7 ? 0 : 1234, fault == 8 ? UINT32_MAX : 2, &f.out), CMDL_UNKNOWN);
		heap_operation_unknown(&f.out);
	}
}
UT_TEST(heap_typed_deleted_and_precheck_results_do_not_allocate_receipts)
{
	for (unsigned state = 0; state < 3; state++) {
		HeapOperationFixture f;
		heap_operation_setup(&f);
		if (state == 0) {
			f.receipt.members[0].member_status = f.receipt.proofs[0].base.member_status = MultiXactStatusUpdate;
			f.receipt.proofs[0].base.state = CCM_COMMITTED;
			f.receipt.proofs[0].base.commit_scn = 500;
			ClusterCurrentMemberProofSetCtrcGrant(&f.receipt.proofs[0].base, 0);
			f.ctx.tuple_shape = CCM_SHAPE_DELETED;
			f.ctx.lock_mode = LockTupleExclusive;
			f.ctx.desired_status = MultiXactStatusForUpdate;
		} else f.ctx.precheck_result = state == 1 ? TM_Invisible : TM_SelfModified;
		UT_ASSERT_EQ(heap_operation_consume(&f, false),
			state == 0 ? CMDL_DELETED : state == 1 ? CMDL_INVISIBLE : CMDL_SELF_MODIFIED);
		UT_ASSERT_EQ(f.out.result, state == 0 ? TM_Deleted : state == 1 ? TM_Invisible : TM_SelfModified);
		heap_operation_no_receipts(&f.out);
	}
}

UT_TEST(heap_typed_self_updated_retains_the_independent_successor)
{
	HeapOperationFixture f;
	heap_operation_setup(&f);
	heap_operation_updater(&f, MultiXactStatusNoKeyUpdate);
	heap_operation_owner(&f);
	f.ctx.tuple_cmax = f.ctx.curcid;
	f.ctx.follow_updates = 1;
	UT_ASSERT_EQ(heap_operation_consume(&f, true), CMDL_SELF_MODIFIED);
	UT_ASSERT_EQ(f.out.result, TM_SelfModified);
	UT_ASSERT(f.out.successor.valid);
	UT_ASSERT_EQ(f.out.successor.updater_xid, test_operation_consumer_child);
	if (f.out.successor.valid)
		UT_ASSERT_EQ(ItemPointerGetOffsetNumber(&f.out.successor.tid), 4);
	heap_operation_no_receipts(&f.out);
}
