/* Native heap/capture/Snapshot and local combo array, controlled external
 * history landing/scheduling. No SQL, native TT40 or network claim. */
#include "../../backend/utils/time/combocid.c"
#include "../../backend/storage/page/itemptr.c"
Block *LocalBufferBlockPointers;
BlockNumber
BufferGetBlockNumber(Buffer buffer)
{
	UT_ASSERT_EQ(buffer, 1);
	return descriptor.bufferdesc.tag.blockNum;
}
static unsigned bridge_unlocks, bridge_relocks, bridge_fault;
static ComboCidKeyData bridge_combos[2];
static void (*bridge_lock_override)(Buffer, int);
void
LockBuffer(Buffer buffer, int mode)
{
	if (bridge_lock_override != NULL) {
		bridge_lock_override(buffer, mode);
		return;
	}
	UT_ASSERT_EQ(buffer, 1);
	if (mode == BUFFER_LOCK_UNLOCK) {
		UT_ASSERT(content_locked);
		UT_ASSERT_EQ(remembered, 2);
		content_locked = false;
		bridge_unlocks++;
	} else {
		UT_ASSERT_EQ(mode, BUFFER_LOCK_EXCLUSIVE);
		UT_ASSERT(!content_locked);
		content_locked = exclusive = true;
		bridge_relocks++;
		switch (bridge_fault) {
		case 1: tuple.t_data->t_infomask2++; break;
		case 2: pcm_source.generation++; break;
		case 3: ClusterPageGetItlSlots(BufferBlocks)[0].write_scn++; break;
		case 4: pg_atomic_write_u32(&history_gate.transition_closed, 1); break;
		case 5: CurrentResourceOwner = (ResourceOwner)2; break;
		case 6: canonical_source.slot_wrap++; break;
		case 7: tuple.t_data->t_ctid.ip_posid++; break;
		default: break;
		}
	}
}
static Snapshot
bridge_setup(ClusterTxOutcome outcome, bool discarded)
{
	Snapshot s;
	cluster_node_id = 1;
	s = roundtrip_setup(outcome, discarded);
	top_source = FullTransactionIdFromEpochAndXid(1, 129);
	current_command = 5;
	tuple.t_data->t_infomask |= HEAP_XMAX_LOCK_ONLY | HEAP_COMBOCID;
	tuple.t_data->t_choice.t_heap.t_field3.t_cid = 999; /* Foreign: never decode. */
	bridge_unlocks = bridge_relocks = bridge_fault = 0;
	comboCids = NULL;
	usedComboCids = sizeComboCids = 0;
	finish_distinct_capabilities = true;
	history_collect_wait_sources();
	return s;
}
static ClusterMxHistoryStep
bridge_collect(Snapshot s, ClusterCurrentMxRequestContext *ctx,
	ClusterCurrentMxDecision *decision, MxHeapReadout *out)
{
	return mx_history_collect(&relation, 1, &tuple, s, &admission, &mxkey,
		members, 2, ctx, decision, out);
}
static void
bridge_no_authority(const MxHeapReadout *out)
{
	static ClusterTTStatusKey zero;
	UT_ASSERT_EQ(out->retained.normalized_count, 0);
	UT_ASSERT_EQ(out->retained.proof_count, 0);
	UT_ASSERT_EQ(out->retained.operation_id, 0);
	UT_ASSERT(!out->successor.valid && !out->follow_updated_chain);
	UT_ASSERT_EQ(out->authoritative_updater, InvalidTransactionId);
	UT_ASSERT(memcmp(&out->wait_key, &zero, sizeof(zero)) == 0);
}
UT_TEST(heap_bridge_native_capture_wait_relock_receipts)
{
	for (unsigned discarded = 0; discarded < 2; ++discarded) {
		Snapshot s = bridge_setup(CLUSTER_TX_IN_PROGRESS, discarded);
		ClusterCurrentMxRequestContext ctx = heap_consumer_context(false);
		ClusterCurrentMxDecision d = CMDL_UNKNOWN;
		MxHeapReadout out;
		finish_pending = true;
		UT_ASSERT_EQ(bridge_collect(s, &ctx, &d, &out), CMXH_COMPLETE);
		UT_ASSERT_EQ(d, CMDL_CONTINUE);
		UT_ASSERT_EQ(out.result, TM_Ok);
		UT_ASSERT_EQ(out.retained.proof_count, discarded ? 0 : 1);
		UT_ASSERT_EQ(out.retained.normalized_count, discarded ? 1 : 2);
		if (!discarded) {
			UT_ASSERT_EQ(out.retained.capabilities[0], 21);
			UT_ASSERT_EQ(out.retained.proofs[0].key.local_xid, 80);
		}
		UT_ASSERT_EQ(bridge_unlocks, 1);
		UT_ASSERT_EQ(bridge_relocks, 1);
		UT_ASSERT(content_locked);
		UT_ASSERT_EQ(history_wait_events, 1);
		UT_ASSERT_EQ(remembered, 1);
		UT_ASSERT_EQ(finish_reads, 2);
		clean(NULL, s);
	}
}
UT_TEST(heap_bridge_native_conflict_policy_and_wait_key)
{
	const LockWaitPolicy policies[] = { LockWaitBlock, LockWaitSkip, LockWaitError };
	const ClusterCurrentMxDecision expected[] = {
		CMDL_WAIT_MEMBER, CMDL_WOULD_BLOCK, CMDL_LOCK_NOT_AVAILABLE };
	for (unsigned i = 0; i < lengthof(policies); ++i) {
		Snapshot s = bridge_setup(CLUSTER_TX_IN_PROGRESS, false);
		ClusterCurrentMxRequestContext ctx = heap_consumer_context(false);
		ClusterCurrentMxDecision d = CMDL_UNKNOWN;
		MxHeapReadout out;
		ctx.lock_mode = LockTupleExclusive;
		ctx.desired_status = MultiXactStatusForUpdate;
		ctx.wait_policy = policies[i];
		UT_ASSERT_EQ(bridge_collect(s, &ctx, &d, &out), CMXH_COMPLETE);
		UT_ASSERT_EQ(d, expected[i]);
		UT_ASSERT_EQ(out.retained.proof_count, 0);
		if (i == 0) UT_ASSERT_EQ(out.wait_key.local_xid, 80);
		UT_ASSERT(content_locked);
		UT_ASSERT_EQ(remembered, 1);
		clean(NULL, s);
	}
}
UT_TEST(heap_bridge_relock_drift_scrubs_whole_attempt)
{
	for (unsigned fault = 1; fault <= 5; ++fault) {
		Snapshot s = bridge_setup(CLUSTER_TX_IN_PROGRESS, false);
		ClusterCurrentMxRequestContext ctx = heap_consumer_context(false);
		ClusterCurrentMxDecision d = CMDL_CONTINUE;
		MxHeapReadout out;
		bridge_fault = fault;
		UT_ASSERT_EQ(bridge_collect(s, &ctx, &d, &out), CMXH_REQUALIFY);
		UT_ASSERT_EQ(d, CMDL_UNKNOWN);
		bridge_no_authority(&out);
		UT_ASSERT(content_locked);
		UT_ASSERT_EQ(bridge_relocks, 1);
		UT_ASSERT_EQ(remembered, 1);
		CurrentResourceOwner = (ResourceOwner)1;
		clean(NULL, s);
	}
}
UT_TEST(heap_bridge_invalid_scope_never_acquires_history)
{
	for (unsigned fault = 0; fault < 5; ++fault) {
		Snapshot s = bridge_setup(CLUSTER_TX_COMMITTED, false);
		SnapshotData forged = *s;
		ClusterCurrentMxRequestContext ctx = heap_consumer_context(false);
		ClusterCurrentMxDecision d = CMDL_CONTINUE;
		MxHeapReadout out;
		Snapshot input = fault == 0 ? NULL : fault == 1 ? &forged : s;
		if (fault == 2) content_locked = false;
		if (fault == 3) CritSectionCount = 1;
		if (fault == 4) oom = true;
		UT_ASSERT_EQ(bridge_collect(input, &ctx, &d, &out), CMXH_UNKNOWN);
		UT_ASSERT_EQ(d, CMDL_UNKNOWN);
		bridge_no_authority(&out);
		UT_ASSERT_EQ(finish_requests, 0);
		UT_ASSERT_EQ(bridge_unlocks, 0);
		UT_ASSERT_EQ(remembered, 1);
		CritSectionCount = 0;
		clean(NULL, s);
	}
}
UT_TEST(heap_bridge_wait_ERROR_releases_snapshot)
{
	Snapshot s = bridge_setup(CLUSTER_TX_IN_PROGRESS, false);
	ClusterCurrentMxRequestContext ctx = heap_consumer_context(false);
	static ClusterCurrentMxDecision d;
	MxHeapReadout out;
	volatile bool caught = false;
	d = CMDL_CONTINUE;
	finish_pending = true;
	history_wait_fault = 1;
	PG_TRY();
	{
		(void)bridge_collect(s, &ctx, &d, &out);
	}
	PG_CATCH();
	{
		caught = true;
	}
	PG_END_TRY();
	UT_ASSERT(caught);
	UT_ASSERT_EQ(d, CMDL_UNKNOWN);
	UT_ASSERT_EQ(remembered, 1);
	UT_ASSERT_EQ(finish_cancels, 1); /* native cancel asserts lease still held */
	UT_ASSERT_EQ(bridge_relocks, 0); /* ERROR uses original lock cleanup */
	clean(NULL, s);
}
static Snapshot
bridge_own_setup(void)
{
	Snapshot s = bridge_setup(CLUSTER_TX_IN_PROGRESS, false);
	cluster_node_id = 0;
	cluster_xid_stripe_latch_runtime(true, 0, FullTransactionIdFromEpochAndXid(1, 3));
	top_source = FullTransactionIdFromEpochAndXid(1, 80);
	canonical_source = (ClusterCanonicalTxnBinding){
		1, 0, 80, 3, 0, 1, CLUSTER_CANONICAL_TXN_PUBLISHED, 0, 123 };
	publication_source = CLUSTER_CANONICAL_TXN_PUBLISHED;
	members[0].member_status = MultiXactStatusNoKeyUpdate;
	tuple.t_data->t_infomask &= ~HEAP_XMAX_LOCK_ONLY;
	tuple.t_data->t_ctid = tuple.t_self;
	comboCids = bridge_combos;
	usedComboCids = sizeComboCids = 2;
	bridge_combos[0] = (ComboCidKeyData){ 1, 2 };
	bridge_combos[1] = (ComboCidKeyData){ 1, 5 };
	return s;
}
UT_TEST(heap_bridge_exact_owner_before_local_combo_decode)
{
	for (unsigned i = 0; i < 5; ++i) {
		Snapshot s = bridge_own_setup();
		ClusterCurrentMxRequestContext ctx = heap_consumer_context(false);
		ClusterCurrentMxDecision d = CMDL_UNKNOWN;
		MxHeapReadout out;
		ctx.current_member_xid = 112;
		ctx.top_xid = 999; ctx.curcid = 999; ctx.tuple_cmax = 999; /* ignored */
		tuple.t_data->t_choice.t_heap.t_field3.t_cid = i == 0 ? 0 : i == 1 ? 1 : 2;
		if (i == 3) bridge_fault = 6; /* owner changes across unlock */
		if (i == 4) canonical_source.segment_generation = 1; /* raw match != owner */
		UT_ASSERT_EQ(bridge_collect(s, &ctx, &d, &out), i < 2 ? CMXH_COMPLETE :
			i == 3 ? CMXH_REQUALIFY : CMXH_UNKNOWN);
		UT_ASSERT_EQ(d, i == 0 ? CMDL_INVISIBLE : i == 1 ? CMDL_SELF_MODIFIED : CMDL_UNKNOWN);
		UT_ASSERT_EQ(out.retained.proof_count, 0);
		UT_ASSERT_EQ(remembered, 1);
		clean(NULL, s);
		cluster_node_id = 1;
	}
}
UT_TEST(heap_bridge_checked_combo_bounds_and_plain_cmax)
{
	HeapTupleHeaderData t = { 0 };
	CommandId out = 55;
	comboCids = bridge_combos;
	usedComboCids = sizeComboCids = 2;
	bridge_combos[1].cmax = 9;
	t.t_infomask = HEAP_COMBOCID;
	t.t_choice.t_heap.t_field3.t_cid = 1;
	UT_ASSERT(cluster_combo_cmax_local(&t, &out)); UT_ASSERT_EQ(out, 9);
	t.t_choice.t_heap.t_field3.t_cid = 2;
	UT_ASSERT(!cluster_combo_cmax_local(&t, &out)); UT_ASSERT_EQ(out, InvalidCommandId);
	t.t_choice.t_heap.t_field3.t_cid = UINT32_MAX;
	UT_ASSERT(!cluster_combo_cmax_local(&t, &out));
	t.t_choice.t_heap.t_field3.t_cid = 0; comboCids = NULL;
	UT_ASSERT(!cluster_combo_cmax_local(&t, &out));
	comboCids = bridge_combos; sizeComboCids = 1;
	UT_ASSERT(!cluster_combo_cmax_local(&t, &out));
	t.t_infomask = 0; t.t_choice.t_heap.t_field3.t_cid = 4;
	UT_ASSERT(cluster_combo_cmax_local(&t, &out)); UT_ASSERT_EQ(out, 4);
	t.t_infomask = HEAP_MOVED_OFF;
	UT_ASSERT(!cluster_combo_cmax_local(&t, &out));
	UT_ASSERT(!cluster_combo_cmax_local(NULL, &out));
	UT_ASSERT(!cluster_combo_cmax_local(&t, NULL));
	comboCids = NULL; usedComboCids = sizeComboCids = 0;
}
UT_TEST(heap_bridge_native_successor_recheck)
{
	for (unsigned drift = 0; drift < 3; ++drift) {
		Snapshot s = bridge_setup(CLUSTER_TX_COMMITTED, false);
		PageHeader ph = (PageHeader)BufferBlocks;
		ItemId lp = &ph->pd_linp[1];
		HeapTupleHeader candidate;
		ClusterCurrentMxRequestContext ctx = heap_consumer_context(true);
		ClusterCurrentMxDecision d = CMDL_UNKNOWN;
		MxHeapReadout out;
		ClusterMxHistoryStep result;
		ph->pd_lower += sizeof(ItemIdData);
		ph->pd_upper -= MAXALIGN(SizeofHeapTupleHeader);
		ItemIdSetNormal(lp, ph->pd_upper, SizeofHeapTupleHeader);
		candidate = (HeapTupleHeader)PageGetItem(BufferBlocks, lp);
		memset(candidate, 0, SizeofHeapTupleHeader);
		candidate->t_hoff = SizeofHeapTupleHeader;
		HeapTupleHeaderSetXmin(candidate, 96);
		ItemPointerSet(&tuple.t_data->t_ctid, 3, 2);
		tuple.t_data->t_infomask &= ~HEAP_XMAX_LOCK_ONLY;
		members[0].member_status = MultiXactStatusNoKeyUpdate;
		/* Top-only updater uses the real native locator. A canonical top
		 * different from the raw child remains the known closed source seam. */
		if (drift != 2) {
			finish_top = 96;
			finish_native_top_records = true;
			ClusterPageGetItlSlots(BufferBlocks)[0].xid = 96;
			ClusterPageGetItlSlots(BufferBlocks)[1].xid = 96;
		}
		/* Actual original ITL/locator readers, with controlled physical input. */
		ClusterPageGetItlSlots(BufferBlocks)[2] = (ClusterItlSlotData){
			.xid = 96, .flags = ITL_FLAG_ACTIVE,
			.wrap = 1, .undo_segment_head = uba_encode(1, 6, 3, 0), .write_scn = 79 };
		finish_successor_in_graph = true;
		finish_same_page_successor = true;
		if (drift == 1) bridge_fault = 7;
		result = bridge_collect(s, &ctx, &d, &out);
		UT_ASSERT_EQ(result, drift == 2 ? CMXH_UNKNOWN :
			drift == 1 ? CMXH_REQUALIFY : CMXH_COMPLETE);
		UT_ASSERT_EQ(d, drift ? CMDL_UNKNOWN : CMDL_UPDATED);
		if (!drift && d == CMDL_UPDATED) {
			UT_ASSERT_EQ(out.result, TM_Updated);
			UT_ASSERT(out.successor.valid);
			UT_ASSERT_EQ(ItemPointerGetOffsetNumber(&out.successor.tid), 2);
		}
		UT_ASSERT_EQ(remembered, 1);
		UT_ASSERT(content_locked);
		clean(NULL, s);
	}
}
