/* Real capture/graph/record/peer assembly. Origin/transport inputs are
 * controlled; these are not TT40, native SQL or GCS landing witnesses. */
static Snapshot
successor_setup(ClusterMxHistorySuccessorChallenge *challenge)
{
	ClusterCurrentUpdaterChallenge old;
	Snapshot s = updater_remote_setup(&old);
	ClusterItlSlotData *slots = ClusterPageGetItlSlots(BufferBlocks);
	members[0].member_status = MultiXactStatusNoKeyUpdate;
	finish_successor_in_graph = finish_same_page_successor = true;
	finish_live_child_successor = true;
	slots[2] = (ClusterItlSlotData){ .xid = 80, .flags = ITL_FLAG_ACTIVE, .wrap = 1,
		.undo_segment_head = uba_encode(1, 6, 3, 0), .write_scn = 79 };
	ItemPointerSet(&tuple.t_data->t_ctid, 3, 2);
	*challenge = (ClusterMxHistorySuccessorChallenge){ .target = {
		.locator = { pcm_source.tag.spcOid, pcm_source.tag.dbOid, pcm_source.tag.relNumber },
		.forknum = MAIN_FORKNUM, .blockno = 3, .offnum = 2 }, .xmin = 96 };
	return s;
}
static const ClusterMxHistoryGraph *
successor_graph(Snapshot s, ClusterMxHistoryCollector **collector, ClusterMxHistoryCapture *capture)
{
	const ClusterMxHistoryGraph *g = NULL;
	UT_ASSERT(begin(s, collector));
	UT_ASSERT_EQ(finish_ready(*collector, true), CMXH_COMPLETE);
	content_locked = true;
	UT_ASSERT(cluster_mx_history_capture_read(*collector, capture));
	UT_ASSERT(cluster_mx_history_collect_recheck_graph(*collector, &relation, 1, &tuple,
		&mxkey, members, 2, &g));
	UT_ASSERT(cluster_mx_history_assemble_members(capture, g, finish_keys, finish_proofs, 2));
	return g;
}
static void
successor_unknown(const ClusterMxHistorySuccessorProof *proof)
{
	ClusterMxHistorySuccessorProof zero = { 0 };
	zero.member.base.state = CCM_UNKNOWN;
	zero.member.format_version = CLUSTER_CURRENT_OPERATION_PROOF_VERSION;
	UT_ASSERT_EQ(memcmp(proof, &zero, sizeof(zero)), 0);
}
static void
successor_child_top_independent_outcomes(void)
{
	const ClusterTxOutcome states[] = { CLUSTER_TX_IN_PROGRESS, CLUSTER_TX_COMMITTED, CLUSTER_TX_ABORTED };
	for (unsigned i = 0; i < lengthof(states); ++i) {
		ClusterMxHistorySuccessorChallenge challenge;
		Snapshot s = successor_setup(&challenge);
		ClusterMxHistoryCollector *c = NULL; ClusterMxHistoryCapture capture;
		ClusterMxHistorySuccessorProof b;
		const ClusterMxHistoryGraph *g;
		finish_outcome = states[i]; finish_distinct_capabilities = true;
		g = successor_graph(s, &c, &capture);
		UT_ASSERT_EQ(cluster_mx_history_assemble_successor(&capture, g, finish_keys,
			finish_proofs, &challenge, &b), CMXH_COMPLETE);
		UT_ASSERT_EQ(b.member.base.state, finish_proofs[0].base.state);
		UT_ASSERT_EQ(b.member.base.key.local_xid, 80);
		UT_ASSERT_EQ(b.member.base.member_xid, 96);
		UT_ASSERT_EQ(b.member.flags, CLUSTER_CURRENT_OPERATION_PROOF_EXACT);
		UT_ASSERT_EQ(b.member.effect_state, CLUSTER_SAVEPOINT_EFFECT_LIVE);
		UT_ASSERT_EQ(b.key.locator.itl_slot_index, 2);
		UT_ASSERT_EQ(b.key.target.offnum, 2);
		UT_ASSERT_EQ(b.requester_capability_generation, 55);
		UT_ASSERT(FullTransactionIdEquals(b.member.top_fxid, finish_proofs[0].top_fxid));
		UT_ASSERT(FullTransactionIdEquals(b.member.operation_fxid, finish_proofs[0].operation_fxid));
		UT_ASSERT_EQ(ClusterCurrentMemberProofGetCtrcGrant(&b.member.base), i == 0 ? 47 : 0);
		UT_ASSERT(!cluster_undo_record_uba_equal(b.key.locator.uba, finish_keys[0].locator.uba));
		UT_ASSERT_EQ(finish_reads, 3); UT_ASSERT_EQ(remembered, 2);
		clean(c, s);
	}
	members[0].member_status = MultiXactStatusForShare;
}
static void
successor_requires_creation_role(void)
{
	for (unsigned kind = 0; kind < 5; ++kind) {
		ClusterMxHistorySuccessorChallenge challenge;
		Snapshot s = successor_setup(&challenge);
		ClusterMxHistoryCollector *c = NULL; ClusterMxHistoryCapture capture;
		ClusterMxHistorySuccessorProof b;
		const ClusterMxHistoryGraph *g;
		finish_successor_body_kind = kind;
		g = successor_graph(s, &c, &capture);
		UT_ASSERT_EQ(cluster_mx_history_assemble_successor(&capture, g, finish_keys,
			finish_proofs, &challenge, &b), kind < 2 ? CMXH_COMPLETE : CMXH_UNKNOWN);
		if (kind >= 2) successor_unknown(&b);
		UT_ASSERT_EQ(finish_reads, 3); clean(c, s);
	}
	members[0].member_status = MultiXactStatusForShare;
}
static void
successor_missing_discarded_and_wrong_fullxid(void)
{
	for (unsigned fault = 0; fault < 3; ++fault) {
		ClusterMxHistorySuccessorChallenge challenge;
		Snapshot s = successor_setup(&challenge);
		ClusterMxHistoryCollector *c = NULL; ClusterMxHistoryCapture capture;
		ClusterMxHistorySuccessorProof b;
		const ClusterMxHistoryGraph *g;
		if (fault == 0) {
			finish_successor_in_graph = false;
			memset(&ClusterPageGetItlSlots(BufferBlocks)[2], 0, sizeof(ClusterItlSlotData));
		}
		if (fault == 1) finish_successor_discarded = true;
		if (fault == 2) finish_successor_fxid_drift = true;
		g = successor_graph(s, &c, &capture);
		UT_ASSERT_EQ(cluster_mx_history_assemble_successor(&capture, g, finish_keys,
			finish_proofs, &challenge, &b), fault == 0 ? CMXH_PENDING : CMXH_UNKNOWN);
		successor_unknown(&b); UT_ASSERT_EQ(updater_fetches, 0);
		clean(c, s);
	}
	members[0].member_status = MultiXactStatusForShare;
}
static void
successor_challenge_is_actual_captured_target(void)
{
	ClusterMxHistorySuccessorChallenge challenge;
	Snapshot s = successor_setup(&challenge);
	ClusterMxHistoryCollector *c = NULL; ClusterMxHistoryCapture capture;
	ClusterMxHistorySuccessorProof b;
	const ClusterMxHistoryGraph *g = successor_graph(s, &c, &capture);
	for (unsigned fault = 0; fault < 8; ++fault) {
		ClusterMxHistorySuccessorChallenge bad = challenge;
		if (fault == 0) bad.target.offnum++;
		if (fault == 1) bad.target.blockno++;
		if (fault == 2) bad.target.locator.relNumber++;
		if (fault == 3) bad.target._pad++;
		if (fault == 4) bad.xmin++;
		if (fault == 5) bad.member_ordinal = 1;
		if (fault == 6) bad.member_ordinal = 256;
		if (fault == 7) bad.reserved16++;
		memset(&b, 0xa5, sizeof(b));
		UT_ASSERT_EQ(cluster_mx_history_assemble_successor(&capture, g, finish_keys,
			finish_proofs, &bad, &b), CMXH_UNKNOWN);
		successor_unknown(&b);
	}
	clean(c, s); members[0].member_status = MultiXactStatusForShare;
}
static void
successor_rejects_partial_or_mixed_A(void)
{
	ClusterMxHistorySuccessorChallenge challenge;
	Snapshot s = successor_setup(&challenge);
	ClusterMxHistoryCollector *c = NULL; ClusterMxHistoryCapture capture;
	ClusterMxHistorySuccessorProof b;
	const ClusterMxHistoryGraph *g = successor_graph(s, &c, &capture);
	for (unsigned fault = 0; fault < 5; ++fault) {
		ClusterCurrentMemberOperationProof a[2]; ClusterSavepointOperationKey keys[2];
		memcpy(a, finish_proofs, sizeof(a)); memcpy(keys, finish_keys, sizeof(keys));
		if (fault == 0) a[1].base.state = CCM_UNKNOWN;
		if (fault == 1) a[0].base.key.segment_generation++;
		if (fault == 2) a[0].base.commit_scn++;
		if (fault == 3) a[0].effect_state = CLUSTER_SAVEPOINT_EFFECT_DISCARDED;
		if (fault == 4) keys[0].locator.uba = uba_encode(1, 6, 3, 0);
		UT_ASSERT_EQ(cluster_mx_history_assemble_successor(&capture, g, keys, a,
			&challenge, &b), CMXH_UNKNOWN);
		successor_unknown(&b);
	}
	clean(c, s); members[0].member_status = MultiXactStatusForShare;
}
static void
successor_failure_and_native_relock_cleanup(void)
{
	ClusterMxHistorySuccessorChallenge challenge;
	Snapshot s = successor_setup(&challenge);
	ClusterMxHistoryCollector *c = NULL; ClusterMxHistoryCapture capture;
	ClusterMxHistorySuccessorProof b;
	const ClusterMxHistoryGraph *g = successor_graph(s, &c, &capture);
	UT_ASSERT_EQ(cluster_mx_history_assemble_successor(&capture, g, finish_keys,
		finish_proofs, &challenge, NULL), CMXH_UNKNOWN);
	oom = true;
	UT_ASSERT_EQ(cluster_mx_history_assemble_successor(&capture, g, finish_keys,
		finish_proofs, &challenge, &b), CMXH_UNKNOWN);
	successor_unknown(&b); oom = false;
	UT_ASSERT_EQ(cluster_mx_history_assemble_successor(&capture, g, finish_keys,
		finish_proofs, &challenge, &b), CMXH_COMPLETE);
	UT_ASSERT_EQ(remembered, 2);
	ClusterPageGetItlSlots(BufferBlocks)[2].write_scn++;
	UT_ASSERT(!cluster_mx_history_collect_recheck_graph(c, &relation, 1, &tuple,
		&mxkey, members, 2, &g));
	UT_ASSERT(g == NULL); UT_ASSERT_EQ(remembered, 1);
	clean(c, s); members[0].member_status = MultiXactStatusForShare;
}

static void
successor_uses_proven_page_order_not_lsn_or_slot(void)
{
	for (unsigned ordered = 0; ordered < 2; ++ordered) {
		ClusterMxHistorySuccessorChallenge challenge;
		Snapshot s = successor_setup(&challenge);
		ClusterMxHistoryCollector *c = NULL; ClusterMxHistoryCapture capture;
		ClusterMxHistorySuccessorProof b;
		const ClusterMxHistoryGraph *g;
		ClusterItlSlotData *roots = ClusterPageGetItlSlots(BufferBlocks);
		finish_successor_order = ordered ? 2 : 1;
		roots[ordered ? 2 : 3] = (ClusterItlSlotData){ .xid = 80, .flags = ITL_FLAG_ACTIVE,
			.wrap = 1, .undo_segment_head = uba_encode(1, 5, 3, 0), .write_scn = ordered ? 82 : 78 };
		g = successor_graph(s, &c, &capture);
		UT_ASSERT_EQ(cluster_mx_history_graph_count(g), 4);
		UT_ASSERT_EQ(cluster_mx_history_assemble_successor(&capture, g, finish_keys,
			finish_proofs, &challenge, &b), ordered ? CMXH_COMPLETE : CMXH_UNKNOWN);
		if (ordered) UT_ASSERT(cluster_undo_record_uba_equal(b.key.locator.uba, uba_encode(1, 5, 3, 0)));
		else successor_unknown(&b);
		UT_ASSERT_EQ(finish_reads, 4); clean(c, s);
	}
	members[0].member_status = MultiXactStatusForShare;
}
