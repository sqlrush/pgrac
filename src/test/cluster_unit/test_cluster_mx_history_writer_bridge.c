/* Actual dormant writer capture/finish/receipt and successor consumers.
 * Storage, transport, PCM and CTRC identity leaves are controlled; this is
 * not a native physical mutation or SQL test. */
#define HISTORY_WALKER_FIXTURE
#include "test_cluster_mx_history_hot_caller.c"
#undef main
#undef HISTORY_WALKER_FIXTURE
void *palloc0(Size size)
{
	void *p = MemoryContextAlloc(CurrentMemoryContext, size);
	memset(p, 0, size); return p;
}
static bool writer_grant_ok;
static unsigned writer_grant_calls, writer_relock_fault;
static uint32 writer_seen_capability;
bool
cluster_runtime_visibility_active_proof_ctrc_identity_exact(
	const ClusterCurrentMemberProofKey *key, uint32 grant, uint32 capability,
	ClusterCtrcTxnKeyV1 *ctrc, ClusterCtrcParticipantIdentity *participant)
{
	writer_grant_calls++; writer_seen_capability = capability;
	UT_ASSERT_EQ(key->local_xid, 80); UT_ASSERT_EQ(key->segment_generation, 0);
	UT_ASSERT_EQ(grant, 47);
	memset(ctrc, 0, sizeof(*ctrc)); memset(participant, 0, sizeof(*participant));
	return writer_grant_ok;
}
static void
writer_lock(Buffer buffer, int mode)
{
	auth_lock(buffer, mode);
	if (mode == BUFFER_LOCK_EXCLUSIVE && remembered == 2) {
		if (writer_relock_fault == 1) hot_candidate().t_data->t_infomask2++;
		if (writer_relock_fault == 2) PageGetItemId(BufferBlocks, 2)->lp_len++;
		if (writer_relock_fault == 3) ClusterPageGetItlSlots(BufferBlocks)[2].write_scn++;
		if (writer_relock_fault == 4) pg_atomic_write_u32(&history_gate.transition_closed, 1);
		writer_relock_fault = 0;
	}
}
static Snapshot
writer_setup(ClusterTxOutcome outcome, bool scoped)
{
	Snapshot s = hot_child_setup(false);
	finish_outcome = outcome; exclusive = true; bridge_lock_override = writer_lock;
	writer_grant_ok = true; writer_grant_calls = writer_seen_capability = writer_relock_fault = 0;
	caller_enters = caller_leaves = caller_inflight = 0; caller_closed = false;
	if (scoped) mx_history_recursive_open(s);
	return s;
}
static int
writer_scoped_call(MxHeapReadout *out)
{
	volatile int result = -1; error_code = 0;
	PG_TRY(); { result = mx_history_recursive_authorize(&relation, 1, &tuple,
		true, 145, LockTupleKeyShare, out); }
	PG_CATCH(); { result = -1; }
	PG_END_TRY(); return result;
}
static int
writer_match(HeapTuple candidate, const ClusterHeapSuccessorProof *proof, bool scoped)
{
	volatile int result = -1; error_code = 0;
	PG_TRY(); { result = mx_history_writer_matches(1, candidate, proof, scoped); }
	PG_CATCH(); { result = -1; }
	PG_END_TRY(); return result;
}
static void writer_clean(Snapshot s, bool scoped)
{
	if (scoped) {
		mx_history_recursive_close(); UT_ASSERT_EQ(caller_enters, 1);
		UT_ASSERT_EQ(caller_leaves, 1); UT_ASSERT_EQ(caller_inflight, 0);
	}
	auth_clean(s);
}
UT_TEST(writer_child_capture_delivers_A_receipts_and_independent_B)
{
	Snapshot s = writer_setup(CLUSTER_TX_IN_PROGRESS, false); MxHeapReadout out;
	ClusterCurrentMxRequestContext ctx = heap_consumer_context(true);
	UT_ASSERT_EQ(auth_call(s, &admission, &ctx, &out), 2);
	UT_ASSERT(out.follow_updated_chain && out.successor.valid && out.successor.writer_valid);
	UT_ASSERT(!out.successor.history_valid); UT_ASSERT_EQ(out.result, TM_Ok);
	UT_ASSERT_EQ(out.retained.proof_count, 1); UT_ASSERT_EQ(out.retained.capabilities[0], 21);
	UT_ASSERT_EQ(out.retained.proofs[0].key.local_xid, 80);
	UT_ASSERT_EQ(out.retained.proofs[0].member_xid, 96);
	UT_ASSERT_EQ(out.successor.writer.capability_generation, 55);
	UT_ASSERT_EQ(finish_reads, 3); UT_ASSERT_EQ(remembered, 1); writer_clean(s, false);
}
UT_TEST(writer_scoped_successor_consumes_exact_B_under_X)
{
	const ClusterTxOutcome states[] = { CLUSTER_TX_IN_PROGRESS, CLUSTER_TX_COMMITTED };
	for (unsigned state = 0; state < 2; ++state) {
		Snapshot s = writer_setup(states[state], true); MxHeapReadout out;
		HeapTupleData candidate = hot_candidate(); ClusterHeapSuccessorProof next;
		int result = writer_scoped_call(&out);
		UT_ASSERT_EQ(result, 2);
		if (result != 2) { writer_clean(s, true); continue; }
		UT_ASSERT(out.successor.valid && out.successor.writer_valid);
		UT_ASSERT_EQ(writer_match(&candidate, &out.successor, true), 1);
		UT_ASSERT_EQ(writer_grant_calls, state == 0 ? 1 : 0);
		if (state == 0) UT_ASSERT_EQ(writer_seen_capability, 55);
		UT_ASSERT(mx_history_recursive_next(&tuple, &next));
		UT_ASSERT(memcmp(&next, &out.successor, sizeof(next)) == 0);
		{
			volatile bool rejected = false;
			PG_TRY(); { (void)mx_history_recursive_next(&tuple, &next); }
			PG_CATCH(); { rejected = true; }
			PG_END_TRY(); UT_ASSERT(rejected);
		}
		UT_ASSERT_EQ(finish_reads, 3); UT_ASSERT_EQ(remembered, 1); writer_clean(s, true);
	}
}
UT_TEST(writer_successor_rejects_late_image_scope_or_B_drift)
{
	for (unsigned fault = 0; fault < 18; ++fault) {
		Snapshot s = writer_setup(CLUSTER_TX_IN_PROGRESS, true); MxHeapReadout out;
		HeapTupleData candidate = hot_candidate();
		int result = writer_scoped_call(&out);
		UT_ASSERT_EQ(result, 2);
		if (result != 2) { writer_clean(s, true); continue; }
		UT_ASSERT_EQ(finish_reads, 3);
		if (fault == 0) candidate.t_data->t_infomask2++;
		if (fault == 1) ClusterPageGetItlSlots(BufferBlocks)[0].write_scn++;
		if (fault == 2) PageXLogRecPtrSet(((PageHeader)BufferBlocks)->pd_lsn, UINT64CONST(129));
		if (fault == 3) PageGetItemId(BufferBlocks, 2)->lp_len++;
		if (fault == 4) pcm_source.flags++;
		if (fault == 5) pcm_source.generation++;
		if (fault == 6) pcm_source.pcm_state = PCM_STATE_S;
		if (fault == 7) writer_grant_ok = false;
		if (fault == 8) out.successor.writer.capability_generation = 0;
		if (fault == 9) out.successor.writer.member.operation_fxid = FullTransactionIdFromEpochAndXid(2,96);
		if (fault == 10) out.successor.writer.key.target.offnum++;
		if (fault == 11) out.successor.writer.member.effect_state = CLUSTER_SAVEPOINT_EFFECT_DISCARDED;
		if (fault == 12) out.successor.writer.snapshot_identity++;
		if (fault == 13) out.successor.writer.admission_generation++;
		if (fault == 14) CurrentResourceOwner = (ResourceOwner)2;
		if (fault == 15) out.successor.history_valid = true;
		if (fault == 16) out.successor.locator_uba_raw[0]++;
		UT_ASSERT(writer_match(&candidate, &out.successor, fault != 17) != 1);
		CurrentResourceOwner = (ResourceOwner)1; writer_clean(s, true);
	}
}
UT_TEST(writer_child_relock_drift_clears_whole_delivery)
{
	for (unsigned fault = 1; fault < 5; ++fault) {
		Snapshot s = writer_setup(CLUSTER_TX_IN_PROGRESS, false); MxHeapReadout out;
		ClusterCurrentMxRequestContext ctx = heap_consumer_context(true);
		writer_relock_fault = fault;
		UT_ASSERT(auth_call(s, &admission, &ctx, &out) != 2);
		bridge_no_authority(&out); UT_ASSERT_EQ(finish_reads, 3);
		UT_ASSERT_EQ(remembered, 1); UT_ASSERT(content_locked && exclusive); writer_clean(s, false);
	}
}
UT_TEST(writer_missing_or_discarded_B_never_yields_receipt)
{
	for (unsigned fault = 0; fault < 4; ++fault) {
		Snapshot s = writer_setup(CLUSTER_TX_IN_PROGRESS, false); MxHeapReadout out;
		ClusterCurrentMxRequestContext ctx = heap_consumer_context(true);
		if (fault == 0) finish_successor_discarded = true;
		if (fault == 1) finish_successor_fxid_drift = true;
		if (fault == 2) finish_successor_body_kind = 2;
		if (fault == 3) finish_successor_body_kind = 3;
		UT_ASSERT_EQ(auth_call(s, &admission, &ctx, &out), -1);
		bridge_no_authority(&out); UT_ASSERT_EQ(finish_reads, 3);
		UT_ASSERT_EQ(remembered, 1); writer_clean(s, false);
	}
}
UT_TEST(writer_WAIT_has_no_B_handoff_or_mutation_receipt)
{
	Snapshot s = writer_setup(CLUSTER_TX_IN_PROGRESS, false); MxHeapReadout out;
	ClusterCurrentMxRequestContext ctx = heap_consumer_context(true);
	ctx.lock_mode = LockTupleExclusive; ctx.desired_status = MultiXactStatusForUpdate;
	UT_ASSERT_EQ(auth_call(s, &admission, &ctx, &out), 1);
	bridge_no_authority(&out); UT_ASSERT_EQ(auth_wait_key.local_xid, 80);
	UT_ASSERT_EQ(finish_reads, 3); UT_ASSERT_EQ(remembered, 1); writer_clean(s, false);
}
UT_TEST(read_handoff_cannot_be_promoted_to_writer)
{
	Snapshot s = hot_child_setup(false); MxHotReadout out; HeapTupleData candidate = hot_candidate();
	UT_ASSERT_EQ(hot_call(s, &admission, false, &out), 1);
	exclusive = true; bridge_lock_override = auth_lock;
	caller_enters = caller_leaves = caller_inflight = 0; caller_closed = false;
	mx_history_recursive_open(s);
	UT_ASSERT(out.successor.history_valid && !out.successor.writer_valid);
	UT_ASSERT(writer_match(&candidate, &out.successor, true) != 1);
	mx_history_recursive_close(); auth_clean(s);
}
UT_TEST(writer_scope_close_expires_successor)
{
	Snapshot s = writer_setup(CLUSTER_TX_IN_PROGRESS, true); MxHeapReadout out;
	HeapTupleData candidate = hot_candidate();
	UT_ASSERT_EQ(writer_scoped_call(&out), 2);
	mx_history_recursive_close();
	UT_ASSERT(writer_match(&candidate, &out.successor, true) != 1);
	UT_ASSERT_EQ(caller_inflight, 0); auth_clean(s);
}
UT_TEST(writer_successor_cannot_cross_reopened_same_Snapshot_scope)
{
	Snapshot s = writer_setup(CLUSTER_TX_IN_PROGRESS, true); MxHeapReadout out;
	HeapTupleData candidate = hot_candidate();
	UT_ASSERT_EQ(writer_scoped_call(&out), 2);
	UT_ASSERT_EQ(writer_match(&candidate, &out.successor, true), 1);
	mx_history_recursive_close(); mx_history_recursive_open(s);
	UT_ASSERT(writer_match(&candidate, &out.successor, true) != 1);
	mx_history_recursive_close(); UT_ASSERT_EQ(caller_enters, 2);
	UT_ASSERT_EQ(caller_leaves, 2); UT_ASSERT_EQ(caller_inflight, 0); auth_clean(s);
}
int main(void)
{
	history_fixture_init(); UT_PLAN(9);
	UT_RUN(writer_child_capture_delivers_A_receipts_and_independent_B);
	UT_RUN(writer_scoped_successor_consumes_exact_B_under_X);
	UT_RUN(writer_successor_rejects_late_image_scope_or_B_drift);
	UT_RUN(writer_child_relock_drift_clears_whole_delivery);
	UT_RUN(writer_missing_or_discarded_B_never_yields_receipt);
	UT_RUN(writer_WAIT_has_no_B_handoff_or_mutation_receipt);
	UT_RUN(read_handoff_cannot_be_promoted_to_writer);
	UT_RUN(writer_scope_close_expires_successor);
	UT_RUN(writer_successor_cannot_cross_reopened_same_Snapshot_scope);
	free(procArray); UT_DONE(); return ut_failed_count ? 1 : 0;
}
