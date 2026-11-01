/* Actual collector/finish/heap typed read path. Native source/transport/PCM
 * remain controlled leaves; this does not prove TT40, SQL or physical I/O. */
static ClusterMxHistorySuccessorChallenge
typed_hot_challenge(void)
{
	return (ClusterMxHistorySuccessorChallenge){ .target = {
		.locator = { pcm_source.tag.spcOid, pcm_source.tag.dbOid, pcm_source.tag.relNumber },
		.forknum = MAIN_FORKNUM, .blockno = 3, .offnum = 2 }, .xmin = 96 };
}
static bool
typed_hot_begin(Snapshot s, ClusterMxHistoryCollector **c)
{
	ClusterMxHistorySuccessorChallenge challenge = typed_hot_challenge();
	bool ok = cluster_mx_history_capture_begin_with_successor(&relation, 1, &tuple, s,
		&mxkey, members, 2, &admission, &challenge, NULL, c);
	UT_ASSERT(ok); return ok;
}
static ClusterMxHistoryStep
typed_hot_finish(ClusterMxHistoryCollector *c, const ClusterMxHistorySuccessorChallenge *fresh,
	ClusterMxHistorySuccessorProof *b, uint32 capabilities[2])
{
	content_locked = true;
	return cluster_mx_history_recheck_and_finish_with_successor(c, &relation, 1, &tuple,
		&mxkey, members, 2, finish_keys, finish_proofs, capabilities, 2, fresh, NULL, NULL, b);
}
UT_TEST(typed_hot_collector_delivers_once_and_rejects_legacy_finish)
{
	for (unsigned legacy = 0; legacy < 2; ++legacy) {
		Snapshot s = hot_child_setup(false); ClusterMxHistoryCollector *c = NULL;
		ClusterMxHistorySuccessorChallenge fresh = typed_hot_challenge();
		ClusterMxHistorySuccessorProof b; uint32 capabilities[2];
		if (!typed_hot_begin(s, &c)) { clean(c, s); continue; }
		UT_ASSERT_EQ(finish_ready(c, false), CMXH_COMPLETE);
		UT_ASSERT_EQ(finish_reads, 3);
		if (legacy) {
			UT_ASSERT_EQ(finish_deliver(c, 2), CMXH_UNKNOWN);
			finish_unknown(2); UT_ASSERT_EQ(remembered, 1);
		} else {
			UT_ASSERT_EQ(typed_hot_finish(c, &fresh, &b, capabilities), CMXH_COMPLETE);
			UT_ASSERT_EQ(b.member.base.member_xid, 96); UT_ASSERT_EQ(b.key.locator.xid, 80);
			UT_ASSERT_EQ(b.member.effect_state, CLUSTER_SAVEPOINT_EFFECT_LIVE);
			UT_ASSERT_EQ(b.requester_capability_generation, 55);
			UT_ASSERT_EQ(remembered, 2);
			UT_ASSERT_EQ(typed_hot_finish(c, &fresh, &b, capabilities), CMXH_UNKNOWN);
			successor_unknown(&b); finish_unknown(2);
			UT_ASSERT_EQ(capabilities[0] | capabilities[1], 0);
		}
		clean(c, s);
	}
}
UT_TEST(typed_hot_collector_rechecks_candidate_and_challenge)
{
	for (unsigned fault = 0; fault < 4; ++fault) {
		Snapshot s = hot_child_setup(false); ClusterMxHistoryCollector *c = NULL;
		ClusterMxHistorySuccessorChallenge fresh = typed_hot_challenge();
		ClusterMxHistorySuccessorProof b; uint32 capabilities[2];
		if (!typed_hot_begin(s, &c)) { clean(c, s); continue; }
		UT_ASSERT_EQ(finish_ready(c, false), CMXH_COMPLETE);
		content_locked = true;
		if (fault == 0) hot_candidate().t_data->t_infomask2++;
		if (fault == 1) fresh.xmin++;
		if (fault == 2) PageGetItemId(BufferBlocks, 2)->lp_len++;
		if (fault == 3) CurrentResourceOwner = (ResourceOwner)2;
		UT_ASSERT_EQ(typed_hot_finish(c, &fresh, &b, capabilities), CMXH_REQUALIFY);
		successor_unknown(&b); finish_unknown(2);
		UT_ASSERT_EQ(capabilities[0] | capabilities[1], 0);
		UT_ASSERT_EQ(remembered, 1); CurrentResourceOwner = (ResourceOwner)1; clean(c, s);
	}
}
UT_TEST(typed_hot_child_relock_drift_restarts_whole_attempt)
{
	for (unsigned full = 0; full < 2; ++full)
		for (unsigned fault = 1; fault < 6; ++fault) {
			Snapshot s = hot_child_setup(true); MxHotReadout out;
			MxAuthorizeState state;
			hot_fault = fault;
			UT_ASSERT_EQ(hot_call(s, &admission, full, &out), 0);
			UT_ASSERT(out.restart); hot_empty(&out);
			UT_ASSERT_EQ(finish_reads, 3); UT_ASSERT_EQ(remembered, 1);
			UT_ASSERT(content_locked && !exclusive);
			mx_history_authorize_state(&state);
			UT_ASSERT(state.active && !state.finished); UT_ASSERT_EQ(state.restarts, 1);
			CurrentResourceOwner = (ResourceOwner)1; auth_clean(s);
		}
}
UT_TEST(typed_hot_child_requires_live_independent_committed_B)
{
	for (unsigned full = 0; full < 2; ++full)
		for (unsigned fault = 0; fault < 6; ++fault) {
			Snapshot s = hot_child_setup(true); MxHotReadout out;
			if (fault == 0) finish_outcome = CLUSTER_TX_IN_PROGRESS;
			if (fault == 1) finish_outcome = CLUSTER_TX_ABORTED;
			if (fault == 2) finish_successor_discarded = true;
			if (fault == 3) finish_successor_fxid_drift = true;
			if (fault == 4) finish_successor_body_kind = 2;
			if (fault == 5) finish_successor_body_kind = 3;
			UT_ASSERT_EQ(hot_call(s, &admission, full, &out), -1);
			UT_ASSERT_EQ(error_code, ERRCODE_CLUSTER_CROSS_NODE_WRITE_CONFLICT);
			hot_empty(&out); UT_ASSERT_EQ(finish_reads, 3);
			UT_ASSERT_EQ(remembered, 1); auth_finished(); auth_clean(s);
		}
}
UT_TEST(typed_hot_child_late_image_change_is_rejected)
{
	for (unsigned full = 0; full < 2; ++full)
		for (unsigned fault = 0; fault < 9; ++fault) {
			Snapshot s = hot_child_setup(true); MxHotReadout out;
			HeapTupleData candidate = hot_candidate();
			UT_ASSERT_EQ(hot_call(s, &admission, full, &out), 1);
			UT_ASSERT(mx_history_hot_matches(1, &candidate, full));
			if (fault == 0) candidate.t_data->t_infomask2++;
			if (fault == 1) ClusterPageGetItlSlots(BufferBlocks)[0].write_scn++;
			if (fault == 2) PageXLogRecPtrSet(((PageHeader)BufferBlocks)->pd_lsn, UINT64CONST(0x100000001));
			if (fault == 3) PageGetItemId(BufferBlocks, 2)->lp_len++;
			if (fault == 4) pcm_source.flags++;
			if (fault == 5) pcm_source.generation++;
			if (fault == 6) pcm_source.reservation_token++;
			if (fault == 7) HeapTupleHeaderSetXmin(candidate.t_data, 112);
			if (fault == 8) pcm_source.tag.blockNum++;
			UT_ASSERT(!mx_history_hot_matches(1, &candidate, full));
			UT_ASSERT_EQ(remembered, 1); auth_finished(); auth_clean(s);
		}
}

UT_TEST(typed_hot_validator_binds_whole_A_and_independent_B)
{
	Snapshot s = hot_child_setup(false); ClusterMxHistoryCollector *c = NULL;
	ClusterMxHistorySuccessorChallenge challenge = typed_hot_challenge();
	ClusterMxHistorySuccessorProof b; ClusterMxHistoryCapture capture; uint32 caps[2];
	if (!typed_hot_begin(s, &c)) { clean(c, s); return; }
	UT_ASSERT(cluster_mx_history_capture_read(c, &capture));
	UT_ASSERT_EQ(finish_ready(c, false), CMXH_COMPLETE);
	UT_ASSERT_EQ(typed_hot_finish(c, &challenge, &b, caps), CMXH_COMPLETE);
	UT_ASSERT(cluster_mx_history_validate_successor(&capture, finish_keys, finish_proofs, &challenge, &b));
	for (unsigned fault = 0; fault < 12; ++fault) {
		ClusterMxHistorySuccessorProof bad = b;
		ClusterMxHistorySuccessorChallenge target = challenge;
		ClusterSavepointOperationKey keys[2]; ClusterCurrentMemberOperationProof a[2];
		memcpy(keys, finish_keys, sizeof(keys)); memcpy(a, finish_proofs, sizeof(a));
		if (fault == 0) bad.member.top_fxid = FullTransactionIdFromEpochAndXid(2, 80);
		if (fault == 1) bad.member.operation_fxid = FullTransactionIdFromEpochAndXid(2, 96);
		if (fault == 2) bad.member.base.key.segment_generation++;
		if (fault == 3) bad.member.base.key.slot_wrap++;
		if (fault == 4) bad.member.effect_state = CLUSTER_SAVEPOINT_EFFECT_DISCARDED;
		if (fault == 5) bad.requester_capability_generation = 0;
		if (fault == 6) bad.key.target.offnum++;
		if (fault == 7) bad.member.base.member_ordinal++;
		if (fault == 8) a[1].base.state = CCM_UNKNOWN;
		if (fault == 9) { keys[0].target.offnum++; keys[1].target.offnum++; }
		if (fault == 10) target.target.offnum++;
		if (fault == 11) bad.member.base.commit_scn++;
		UT_ASSERT(!cluster_mx_history_validate_successor(&capture, keys, a, &target, &bad));
	}
	clean(c, s);
}
