/* Actual dormant HOT readers and both native successor consumers. Snapshot,
 * collector, typed validator and PCM/ITL checks execute; external history,
 * descriptor and scheduling inputs remain controlled. No native SQL claim.
 * Portions Copyright (c) 2026, pgrac contributors */
#define HISTORY_HOT_FIXTURE
#include "test_cluster_mx_history_authorize.c"
#undef main
static unsigned hot_fault, hot_promotions;
static bool hot_requalify, hot_stale_relock;
int cluster_conf_node_count(void) { return auth_conf.node_count; }
static void
hot_lock(Buffer buffer, int mode)
{
	UT_ASSERT_EQ(buffer, 1);
	if (mode == BUFFER_LOCK_UNLOCK) {
		UT_ASSERT(content_locked); UT_ASSERT(!exclusive);
		UT_ASSERT(remembered == 1 || remembered == 2);
		content_locked = false; bridge_unlocks++;
	} else {
		if (mode == BUFFER_LOCK_EXCLUSIVE) hot_promotions++;
		UT_ASSERT_EQ(mode, BUFFER_LOCK_SHARE); UT_ASSERT(!content_locked);
		content_locked = true; exclusive = false; bridge_relocks++;
		/* A one-shot SCUR image is newly installed on each SHARE relock. */
		if (pcm_source.pcm_state == PCM_STATE_READ_IMAGE
			&& !(remembered == 2 && hot_stale_relock)) {
			pcm_source.generation++; pcm_source.reservation_token++;
		}
		if (remembered == 1 && auth_descriptor_fault) {
			tuple.t_data->t_infomask2++; auth_descriptor_fault = 0;
		}
		if (remembered == 2) {
			if (hot_requalify) {
				pcm_source.pcm_state = PCM_STATE_READ_IMAGE;
				pcm_source.buffer_type = BUF_TYPE_CURRENT;
				pcm_source.generation++; pcm_source.reservation_token++;
				hot_requalify = false;
			}
			switch (hot_fault) {
			case 1: tuple.t_data->t_infomask2++; break;
			case 2: tuple.t_data->t_ctid.ip_posid++; break;
			case 3: ClusterPageGetItlSlots(BufferBlocks)[2].write_scn++; break;
			case 4: pg_atomic_write_u32(&history_gate.transition_closed, 1); break;
			case 5: CurrentResourceOwner = (ResourceOwner)2; break;
			default: break;
			}
			hot_fault = 0;
		}
	}
}
static Snapshot
hot_setup(ClusterTxOutcome outcome, bool scur)
{
	Snapshot s = auth_setup(outcome, false);
	PageHeader ph = (PageHeader)BufferBlocks; ItemId lp = &ph->pd_linp[1];
	HeapTupleHeader candidate;
	ph->pd_lower += sizeof(ItemIdData); ph->pd_upper -= MAXALIGN(SizeofHeapTupleHeader);
	ItemIdSetNormal(lp, ph->pd_upper, SizeofHeapTupleHeader);
	candidate = (HeapTupleHeader)PageGetItem(BufferBlocks, lp);
	memset(candidate, 0, SizeofHeapTupleHeader); candidate->t_hoff = SizeofHeapTupleHeader;
	HeapTupleHeaderSetXmin(candidate, 96); ItemPointerSet(&tuple.t_data->t_ctid, 3, 2);
	tuple.t_data->t_infomask &= ~HEAP_XMAX_LOCK_ONLY;
	members[0].member_status = MultiXactStatusNoKeyUpdate;
	finish_top = 96; finish_native_top_records = true;
	ClusterPageGetItlSlots(BufferBlocks)[0].xid = 96;
	ClusterPageGetItlSlots(BufferBlocks)[1].xid = 96;
	ClusterPageGetItlSlots(BufferBlocks)[2] = (ClusterItlSlotData){ .xid = 96,
		.flags = ITL_FLAG_ACTIVE, .wrap = 1, .undo_segment_head = uba_encode(1, 6, 3, 0), .write_scn = 79 };
	finish_successor_in_graph = finish_same_page_successor = true;
	exclusive = false; bridge_lock_override = hot_lock;
	if (scur) { pcm_source.pcm_state = PCM_STATE_READ_IMAGE; pcm_source.buffer_type = BUF_TYPE_CURRENT; }
	hot_fault = hot_promotions = 0; hot_requalify = hot_stale_relock = false;
	return s;
}
static int
hot_call(Snapshot s, const ClusterSemanticAdmissionToken *token, bool full, MxHotReadout *out)
{
	volatile int result = -1; error_code = 0;
	PG_TRY(); { result = mx_history_hot(&relation, 1, &tuple, s, token, full, out) ? 1 : 0; }
	PG_CATCH(); { result = -1; }
	PG_END_TRY(); return result;
}
static HeapTupleData
hot_candidate(void)
{
	HeapTupleData t = { 0 }; ItemId lp = PageGetItemId(BufferBlocks, 2);
	ItemPointerSet(&t.t_self, 3, 2); t.t_tableOid = relation.rd_id;
	t.t_data = (HeapTupleHeader)PageGetItem(BufferBlocks, lp); t.t_len = ItemIdGetLength(lp);
	return t;
}
static void hot_empty(const MxHotReadout *out)
{
	static ClusterHeapSuccessorProof zero;
	UT_ASSERT_EQ(out->updater, InvalidTransactionId);
	UT_ASSERT(memcmp(&out->successor, &zero, sizeof(zero)) == 0);
}
UT_TEST(typed_hot_validator_whole_set_and_effect_matrix)
{
	const ClusterTxOutcome outcomes[] = { CLUSTER_TX_IN_PROGRESS, CLUSTER_TX_COMMITTED, CLUSTER_TX_ABORTED };
	for (unsigned state = 0; state < lengthof(outcomes); ++state)
		for (unsigned discarded = 0; discarded < 2; ++discarded) {
			Snapshot s = roundtrip_setup(outcomes[state], discarded);
			ClusterMxHistoryCollector *c = NULL;
			ClusterCurrentUpdaterChallenge challenge = updater_challenge(6);
			ClusterCurrentUpdaterProof updater; uint32 capabilities[2];
			finish_successor_in_graph = true;
			UT_ASSERT(updater_begin(s, &challenge, &c));
			UT_ASSERT_EQ(finish_ready(c, false), CMXH_COMPLETE);
			UT_ASSERT_EQ(receipt_finish(c, capabilities, 2, &challenge, &updater), CMXH_COMPLETE);
			UT_ASSERT_EQ(cluster_multixact_current_validate_operation_updater(&mxkey, members,
				finish_keys, finish_proofs, 2, &challenge, &updater, 0), state == 1 && !discarded);
			if (state == 1 && !discarded) {
				ClusterCurrentMemberOperationProof bad[2]; ClusterCurrentUpdaterProof b = updater;
				memcpy(bad, finish_proofs, sizeof(bad)); bad[1].base.key.cluster_epoch++;
				UT_ASSERT(!cluster_multixact_current_validate_operation_updater(&mxkey, members,
					finish_keys, bad, 2, &challenge, &updater, 0));
				b.candidate_next_xmin_locator.itl_slot_index++;
				UT_ASSERT(!cluster_multixact_current_validate_operation_updater(&mxkey, members,
					finish_keys, finish_proofs, 2, &challenge, &b, 0));
				UT_ASSERT(!cluster_multixact_current_validate_operation_updater(&mxkey, members,
					finish_keys, finish_proofs, 2, &challenge, &updater, 1));
				UT_ASSERT(!cluster_multixact_current_validate_operation_updater(&mxkey, members,
					finish_keys, finish_proofs, 257, &challenge, &updater, 0));
			}
			UT_ASSERT_EQ(remembered, 2); UT_ASSERT_EQ(finish_reads, 3); clean(c, s);
		}
}
UT_TEST(hot_committed_successor_preserves_share_for_both_consumers)
{
	for (unsigned full = 0; full < 2; ++full)
		for (unsigned scur = 0; scur < 2; ++scur) {
			Snapshot s = hot_setup(CLUSTER_TX_COMMITTED, scur); MxHotReadout out;
			HeapTupleData candidate = hot_candidate();
			finish_pending = true;
			UT_ASSERT_EQ(hot_call(s, &admission, full, &out), 1);
			UT_ASSERT_EQ(out.updater, 96); UT_ASSERT(!out.restart); UT_ASSERT(out.successor.valid);
			if (out.successor.valid) UT_ASSERT_EQ(ItemPointerGetOffsetNumber(&out.successor.tid), 2);
			UT_ASSERT(mx_history_hot_matches(1, &candidate, full));
			UT_ASSERT_EQ(bridge_unlocks, 2); UT_ASSERT_EQ(bridge_relocks, 2);
			UT_ASSERT_EQ(auth_describes, 1); UT_ASSERT_EQ(auth_waits, 0);
			UT_ASSERT_EQ(finish_reads, 3); UT_ASSERT_EQ(remembered, 1);
			UT_ASSERT(content_locked && !exclusive); UT_ASSERT_EQ(hot_promotions, 0);
			auth_finished(); auth_clean(s);
		}
}
UT_TEST(hot_noncommitted_updater_is_not_a_committed_chain_proof)
{
	const ClusterTxOutcome states[] = { CLUSTER_TX_IN_PROGRESS, CLUSTER_TX_ABORTED };
	for (unsigned full = 0; full < 2; ++full)
		for (unsigned i = 0; i < lengthof(states); ++i) {
			Snapshot s = hot_setup(states[i], true); MxHotReadout out;
			UT_ASSERT_EQ(hot_call(s, &admission, full, &out), -1);
			UT_ASSERT_EQ(error_code, ERRCODE_CLUSTER_CROSS_NODE_WRITE_CONFLICT);
			hot_empty(&out); UT_ASSERT_EQ(remembered, 1);
			UT_ASSERT(content_locked && !exclusive); auth_finished(); auth_clean(s);
		}
}
UT_TEST(hot_relock_drift_restarts_whole_attempt)
{
	for (unsigned full = 0; full < 2; ++full)
		for (unsigned fault = 0; fault < 6; ++fault) {
			Snapshot s = hot_setup(CLUSTER_TX_COMMITTED, true); MxHotReadout out;
			MxAuthorizeState before, after;
			if (fault == 0) auth_descriptor_fault = 1; else hot_fault = fault;
			UT_ASSERT_EQ(hot_call(s, &admission, full, &out), 0);
			UT_ASSERT(out.restart); hot_empty(&out); UT_ASSERT(content_locked && !exclusive);
			UT_ASSERT_EQ(remembered, 1); mx_history_authorize_state(&before);
			UT_ASSERT(before.active && !before.finished); UT_ASSERT_EQ(before.restarts, 1);
			if (fault == 0) {
				UT_ASSERT_EQ(hot_call(s, &admission, full, &out), 1);
				mx_history_authorize_state(&after); UT_ASSERT_EQ(before.operation_id, after.operation_id);
				UT_ASSERT_EQ(auth_describes, 2); auth_finished();
			}
			CurrentResourceOwner = (ResourceOwner)1; auth_clean(s);
		}
}
UT_TEST(hot_scope_and_content_lock_are_not_replaced)
{
	for (unsigned full = 0; full < 2; ++full)
		for (unsigned fault = 0; fault < 6; ++fault) {
			Snapshot s = hot_setup(CLUSTER_TX_COMMITTED, true), input = s;
			SnapshotData forged = *s; ClusterSemanticAdmissionToken bad = admission; MxHotReadout out;
			if (fault == 0) input = NULL;
			if (fault == 1) input = &forged;
			if (fault == 2) memset(&bad, 0, sizeof(bad));
			if (fault == 3) exclusive = true;
			if (fault == 4) CritSectionCount = 1;
			if (fault == 5) oom = true;
			UT_ASSERT_EQ(hot_call(input, &bad, full, &out), -1);
			UT_ASSERT_EQ(error_code, ERRCODE_CLUSTER_CROSS_NODE_WRITE_CONFLICT);
			hot_empty(&out); UT_ASSERT_EQ(finish_requests, 0); UT_ASSERT_EQ(remembered, 1);
			if (fault == 3 || fault == 4) UT_ASSERT_EQ(auth_describes, 0);
			UT_ASSERT_EQ(hot_promotions, 0); CritSectionCount = 0; auth_clean(s);
		}
}
UT_TEST(hot_ERROR_cleans_snapshot_operation_and_outputs)
{
	for (unsigned full = 0; full < 2; ++full)
		for (unsigned fault = 0; fault < 2; ++fault) {
			Snapshot s = hot_setup(CLUSTER_TX_COMMITTED, true); MxHotReadout out;
			auth_throw_describe = fault == 0;
			if (fault == 1) { finish_pending = true; history_wait_fault = 1; }
			UT_ASSERT_EQ(hot_call(s, &admission, full, &out), -1);
			hot_empty(&out); UT_ASSERT(!content_locked); UT_ASSERT_EQ(remembered, 1);
			auth_finished(); auth_clean(s);
		}
}
UT_TEST(hot_read_requalification_and_successor_late_ABA)
{
	for (unsigned full = 0; full < 2; ++full) {
		Snapshot s = hot_setup(CLUSTER_TX_COMMITTED, true); MxHotReadout out;
		HeapTupleData candidate = hot_candidate();
		UT_ASSERT_EQ(hot_call(s, &admission, full, &out), 1);
		UT_ASSERT_EQ(pcm_source.pcm_state, PCM_STATE_READ_IMAGE);
		UT_ASSERT(mx_history_hot_matches(1, &candidate, full));
		ClusterPageGetItlSlots(BufferBlocks)[2].undo_segment_head = uba_encode(1, 7, 3, 0);
		UT_ASSERT(!mx_history_hot_matches(1, &candidate, full));
		UT_ASSERT_EQ(hot_promotions, 0); UT_ASSERT(content_locked && !exclusive); auth_clean(s);
		for (unsigned fault = 0; fault < 2; ++fault) {
			s = hot_setup(CLUSTER_TX_COMMITTED, fault != 0);
			hot_requalify = fault == 0; hot_stale_relock = fault == 1;
			UT_ASSERT_EQ(hot_call(s, &admission, full, &out), 0);
			UT_ASSERT(out.restart); hot_empty(&out); UT_ASSERT_EQ(remembered, 1);
			UT_ASSERT_EQ(hot_promotions, 0); auth_clean(s);
		}
	}
}
UT_TEST(hot_invalid_root_and_descriptor_fail_closed)
{
	for (unsigned full = 0; full < 2; ++full)
		for (unsigned fault = 0; fault < 7; ++fault) {
			Snapshot s = hot_setup(CLUSTER_TX_COMMITTED, true); MxHotReadout out;
			if (fault == 0) tuple.t_data->t_infomask &= ~HEAP_XMAX_IS_MULTI;
			if (fault == 1) tuple.t_data->t_infomask |= HEAP_XMAX_LOCK_ONLY;
			if (fault == 2) tuple.t_data->t_ctid = tuple.t_self;
			if (fault == 3) HeapTupleHeaderSetXmin(hot_candidate().t_data, 112);
			if (fault == 4) auth_describe_fault = 1;
			if (fault == 5) auth_describe_fault = 2;
			if (fault == 6) auth_describe_fault = 3;
			UT_ASSERT_EQ(hot_call(s, &admission, full, &out), -1);
			UT_ASSERT_EQ(error_code, fault == 4 ? ERRCODE_FEATURE_NOT_SUPPORTED :
				ERRCODE_CLUSTER_CROSS_NODE_WRITE_CONFLICT);
			hot_empty(&out); UT_ASSERT_EQ(remembered, 1); UT_ASSERT_EQ(auth_waits, 0);
			UT_ASSERT_EQ(hot_promotions, 0); auth_clean(s);
		}
}
static Snapshot
hot_child_setup(bool scur)
{
	Snapshot s = hot_setup(CLUSTER_TX_COMMITTED, scur);
	ClusterItlSlotData *slots = ClusterPageGetItlSlots(BufferBlocks);
	/* Physical ITLs retain the canonical top; xmin/member retain the child.
	 * A and independent LIVE INSERT B have exact operation tails in the
	 * controlled history source. No raw child is a TT identity. */
	finish_top = 80;
	finish_native_top_records = false;
	finish_live_child_successor = true;
	for (unsigned i = 0; i < 3; ++i)
		slots[i].xid = 80;
	return s;
}

UT_TEST(hot_child_history_inputs_form_a_typed_member_set)
{
	Snapshot s = hot_child_setup(false);
	ClusterMxHistoryCollector *collector = NULL;
	UT_ASSERT(begin(s, &collector));
	UT_ASSERT_EQ(finish_ready(collector, false), CMXH_COMPLETE);
	UT_ASSERT_EQ(finish_deliver(collector, 2), CMXH_COMPLETE);
	UT_ASSERT_EQ(finish_reads, 3);
	UT_ASSERT_EQ(finish_keys[0].locator.xid, 80);
	UT_ASSERT_EQ(XidFromFullTransactionId(finish_keys[0].top_fxid), 80);
	UT_ASSERT_EQ(XidFromFullTransactionId(finish_keys[0].operation_fxid), 96);
	UT_ASSERT_EQ(finish_proofs[0].base.member_xid, 96);
	UT_ASSERT_EQ(finish_proofs[0].base.key.local_xid, 80);
	UT_ASSERT_EQ(finish_proofs[0].effect_state, CLUSTER_SAVEPOINT_EFFECT_LIVE);
	UT_ASSERT_EQ(cluster_multixact_current_validate_operation_proof_set(&mxkey, members,
		finish_keys, finish_proofs, 2), CMX_RESOLVE_OK);
	clean(collector, s);
}

UT_TEST(hot_child_canonical_successor_reaches_both_native_consumers)
{
	for (unsigned full = 0; full < 2; ++full)
		for (unsigned scur = 0; scur < 2; ++scur) {
			Snapshot s = hot_child_setup(scur);
			MxHotReadout out;
			HeapTupleData candidate = hot_candidate();
			int result = hot_call(s, &admission, full, &out);
			printf("# child96/top80 full=%u scur=%u result=%d error=%d history_requests=%u reads=%u\n",
				full, scur, result, error_code, finish_requests, finish_reads);
			UT_ASSERT_EQ(result, 1);
			UT_ASSERT_EQ(error_code, 0);
			UT_ASSERT_EQ(out.updater, 96);
			UT_ASSERT(!out.restart);
			UT_ASSERT(out.successor.valid);
			UT_ASSERT(mx_history_hot_matches(1, &candidate, full));
			UT_ASSERT_EQ(finish_reads, 3);
			UT_ASSERT(content_locked && !exclusive);
			UT_ASSERT_EQ(hot_promotions, 0);
			auth_finished();
			auth_clean(s);
		}
}

#include "test_cluster_mx_history_typed_hot.inc"

#ifdef HISTORY_HOT_CALLER_FIXTURE
int history_hot_caller_unused_hot_main(void);
#define main history_hot_caller_unused_hot_main
#endif
int main(void)
{
	history_fixture_init(); UT_PLAN(16);
	UT_RUN(typed_hot_validator_whole_set_and_effect_matrix);
	UT_RUN(hot_committed_successor_preserves_share_for_both_consumers);
	UT_RUN(hot_noncommitted_updater_is_not_a_committed_chain_proof);
	UT_RUN(hot_relock_drift_restarts_whole_attempt);
	UT_RUN(hot_scope_and_content_lock_are_not_replaced);
	UT_RUN(hot_ERROR_cleans_snapshot_operation_and_outputs);
	UT_RUN(hot_read_requalification_and_successor_late_ABA);
	UT_RUN(hot_invalid_root_and_descriptor_fail_closed);
	UT_RUN(hot_child_history_inputs_form_a_typed_member_set);
	UT_RUN(hot_child_canonical_successor_reaches_both_native_consumers);
	UT_RUN(typed_hot_collector_delivers_once_and_rejects_legacy_finish);
	UT_RUN(typed_hot_collector_rechecks_candidate_and_challenge);
	UT_RUN(typed_hot_child_relock_drift_restarts_whole_attempt);
	UT_RUN(typed_hot_child_requires_live_independent_committed_B);
	UT_RUN(typed_hot_child_late_image_change_is_rejected);
	UT_RUN(typed_hot_validator_binds_whole_A_and_independent_B);
	free(procArray); UT_DONE(); return ut_failed_count ? 1 : 0;
}
