/* Actual dormant writer entry, descriptor validation, Snapshot collector,
 * typed compositor/receipt and native operation lifecycle. External descriptor
 * bytes, history landings and TX outcomes are controlled: no native SQL claim.
 * Portions Copyright (c) 2026, pgrac contributors */
#define HISTORY_AUTHORIZE_FIXTURE
#include "test_cluster_mx_history_roundtrip.c"
#undef main
#include "cluster/cluster_mxid_stripe.h"
#include "cluster/cluster_tx_enqueue.h"
static ClusterConf auth_conf;
ClusterConf *ClusterConfShmem = &auth_conf;
int cluster_ges_request_timeout_ms = 5000;
static unsigned auth_describes, auth_waits, auth_descriptor_fault, auth_history_fault;
static unsigned auth_describe_fault;
static bool auth_throw_wait, auth_throw_describe;
static ClusterTxwResult auth_tx_result;
static ClusterTTStatusKey auth_wait_key;
static uint64 *auth_deadline;
void cluster_mxid_stripe_lazy_latch(void) { abort(); }
int GetMultiXactIdMembers(MultiXactId id, MultiXactMember **out, bool a, bool b) { abort(); }
TransactionId GetTopTransactionId(void) { abort(); } /* Legacy decoder is forbidden. */
int errmsg_internal(const char *fmt, ...) { return 0; }
int errdetail_internal(const char *fmt, ...) { return 0; }
ClusterMxDescribeResult
cluster_gcs_current_mx_describe_fetch_and_wait(int32 origin, const ClusterCurrentMxKey *key,
	ClusterCurrentMxMemberDesc *out, uint16 capacity, uint16 *count, uint32 *total)
{
	UT_ASSERT(!content_locked); UT_ASSERT_EQ(remembered, 1);
	UT_ASSERT_EQ(origin, 3); UT_ASSERT_EQ(key->multixact_id, 99);
	UT_ASSERT_EQ(key->cluster_epoch, 7); UT_ASSERT(capacity >= 2);
	auth_describes++;
	if (auth_throw_describe) ereport(ERROR, (errcode(ERRCODE_QUERY_CANCELED), errmsg("cancel")));
	memcpy(out, members, sizeof(members)); *count = 2; *total = 2;
	if (auth_describe_fault == 1) { *total = 257; return CMX_DESC_SUPPORTED_LIMIT; }
	if (auth_describe_fault == 2) out[1] = out[0];
	if (auth_describe_fault == 3) (*total)++;
	if (auth_describe_fault == 4) return CMX_DESC_DENIED;
	return CMX_DESC_OK;
}
ClusterTxwResult
cluster_tx_enqueue_wait_current_mx(const ClusterTTStatusKey *key, int timeout, uint64 *deadline)
{
	UT_ASSERT(!content_locked); UT_ASSERT_EQ(remembered, 1);
	UT_ASSERT_EQ(timeout, 5000); UT_ASSERT(deadline != NULL);
	if (auth_deadline) UT_ASSERT(auth_deadline == deadline);
	auth_deadline = deadline; *deadline = 123456;
	auth_wait_key = *key; auth_waits++;
	if (auth_throw_wait) ereport(ERROR, (errcode(ERRCODE_QUERY_CANCELED), errmsg("cancel")));
	return auth_tx_result;
}
/* The shared native entry also contains the dormant legacy branch. None
 * of these legacy authority leaves may execute in the history mode. */
int cluster_gcs_reply_timeout_ms = 5000;
int cluster_subtrans_max_chain_depth = 64;
TimestampTz GetCurrentTimestamp(void) { abort(); }
bool TransactionIdIsCurrentTransactionId(TransactionId xid) { abort(); }
bool cluster_ctrc_origin_grant_publishable(const ClusterCtrcTxnKeyV1 *key,
	const ClusterCtrcParticipantIdentity *participant, uint32 grant) { abort(); }
bool cluster_runtime_visibility_current_owner_lookup_exact_ctrc_full(TransactionId xid,
	ClusterTTStatusKey *key, ClusterTTStatusResult *result, uint32 *grant,
	ClusterCtrcTxnKeyV1 *ctrc, ClusterCtrcParticipantIdentity *participant) { abort(); }
bool cluster_runtime_visibility_local_terminal_lookup_exact(TransactionId xid,
	ClusterTTStatusKey *key, ClusterTTStatusResult *result, bool *retry) { abort(); }
bool cluster_runtime_visibility_current_mx_updater_provenance_exact(const ClusterTxLocator *locator,
	TimestampTz deadline, ClusterTTStatusKey *key, ClusterTTStatusResult *result, uint32 *grant,
	uint32 *cap, ClusterCtrcTxnKeyV1 *ctrc, ClusterTxLocator *canonical, bool *cross, bool *retry)
{ abort(); }
ClusterMxResolveResult cluster_gcs_current_mx_member_proof_fetch_and_wait(int32 origin,
	struct ClusterCurrentMxProofForwardV2 *request, ClusterCurrentMemberProof *proofs,
	uint16 capacity, uint16 *count, ClusterCurrentUpdaterProof *updater, uint32 *cap,
	TimestampTz deadline) { abort(); }
static void
auth_lock(Buffer buffer, int mode)
{
	UT_ASSERT_EQ(buffer, 1);
	if (mode == BUFFER_LOCK_UNLOCK) {
		UT_ASSERT(content_locked); UT_ASSERT(remembered == 1 || remembered == 2);
		content_locked = false; bridge_unlocks++;
	} else {
		UT_ASSERT_EQ(mode, BUFFER_LOCK_EXCLUSIVE); UT_ASSERT(!content_locked);
		content_locked = exclusive = true; bridge_relocks++;
		if (remembered == 2 && auth_history_fault) {
			pcm_source.generation++; auth_history_fault = 0;
		} else if (remembered == 1 && auth_descriptor_fault) {
			pcm_source.generation++; auth_descriptor_fault = 0;
		}
	}
}
static Snapshot
auth_setup(ClusterTxOutcome outcome, bool discarded)
{
	Snapshot s = bridge_setup(outcome, discarded);
	mxkey.origin_node_id = 3; /* Actual raw 99 stripe; no fixture origin override. */
	cluster_mxid_stripe_latch_runtime(true, cluster_node_id, FirstMultiXactId);
	UT_ASSERT_EQ(cluster_mxid_origin_slot(99), 3);
	auth_conf.node_count = 4; bridge_lock_override = auth_lock;
	auth_describes = auth_waits = auth_descriptor_fault = auth_history_fault = auth_describe_fault = 0;
	auth_throw_wait = auth_throw_describe = false; auth_deadline = NULL;
	auth_tx_result = CLUSTER_TXW_RESOLVED; memset(&auth_wait_key, 0, sizeof(auth_wait_key));
	mx_history_authorize_reset(); error_code = 0;
	return s;
}
static void auth_clean(Snapshot s) { bridge_lock_override = NULL; clean(NULL, s); }
static int
auth_call(Snapshot actual, const ClusterSemanticAdmissionToken *token,
	const ClusterCurrentMxRequestContext *ctx, MxHeapReadout *out)
{
	volatile int result = -1;
	error_code = 0;
	PG_TRY(); { result = mx_history_authorize(&relation, 1, &tuple, actual, token, ctx, out); }
	PG_CATCH(); { result = -1; }
	PG_END_TRY();
	return result;
}
static void auth_finished(void)
{
	MxAuthorizeState state; mx_history_authorize_state(&state);
	UT_ASSERT(state.operation_id != 0); UT_ASSERT(state.finished); UT_ASSERT(!state.active);
}
UT_TEST(authorize_native_writer_terminal_receipts)
{
	const uint8 actions[] = { CCM_ACTION_LOCK, CCM_ACTION_UPDATE, CCM_ACTION_DELETE };
	for (unsigned i = 0; i < lengthof(actions); ++i) {
		Snapshot s = auth_setup(CLUSTER_TX_IN_PROGRESS, true);
		ClusterCurrentMxRequestContext ctx = heap_consumer_context(false); MxHeapReadout out;
		ctx.action = actions[i];
		if (i) ctx.lock_mode = LockTupleExclusive;
		UT_ASSERT_EQ(auth_call(s, &admission, &ctx, &out), 2);
		UT_ASSERT_EQ(out.result, TM_Ok); UT_ASSERT_EQ(out.retained.normalized_count, 1);
		UT_ASSERT_EQ(out.retained.proof_count, 0); UT_ASSERT_EQ(auth_describes, 1);
		UT_ASSERT_EQ(auth_waits, 0); UT_ASSERT_EQ(finish_reads, 2);
		UT_ASSERT_EQ(bridge_unlocks, 2); UT_ASSERT_EQ(bridge_relocks, 2);
		UT_ASSERT_EQ(remembered, 1); UT_ASSERT(content_locked); auth_finished(); auth_clean(s);
	}
}
UT_TEST(authorize_live_compatible_receipt_uses_original_capability)
{
	Snapshot s = auth_setup(CLUSTER_TX_IN_PROGRESS, false);
	ClusterCurrentMxRequestContext ctx = heap_consumer_context(false); MxHeapReadout out;
	UT_ASSERT_EQ(auth_call(s, &admission, &ctx, &out), 2);
	UT_ASSERT_EQ(out.result, TM_Ok); UT_ASSERT_EQ(out.retained.normalized_count, 2);
	UT_ASSERT_EQ(out.retained.proof_count, 1); UT_ASSERT_EQ(out.retained.capabilities[0], 21);
	UT_ASSERT_EQ(out.retained.proofs[0].key.local_xid, 80);
	auth_finished(); auth_clean(s);
}
UT_TEST(authorize_exact_tx_restart_reuses_only_immutable_descriptor)
{
	Snapshot s = auth_setup(CLUSTER_TX_IN_PROGRESS, false);
	ClusterCurrentMxRequestContext ctx = heap_consumer_context(false); MxHeapReadout out;
	MxAuthorizeState before, after;
	ctx.lock_mode = LockTupleExclusive;
	UT_ASSERT_EQ(auth_call(s, &admission, &ctx, &out), 1);
	mx_history_authorize_state(&before);
	UT_ASSERT(before.active && !before.finished); UT_ASSERT_EQ(before.restarts, 1);
	UT_ASSERT_EQ(auth_waits, 1); UT_ASSERT_EQ(auth_wait_key.local_xid, 80);
	UT_ASSERT_EQ(auth_wait_key.origin_node_id, 0); UT_ASSERT_EQ(auth_wait_key.undo_segment_id, 1);
	UT_ASSERT_EQ(auth_wait_key.tt_slot_id, 4); bridge_no_authority(&out);
	finish_outcome = CLUSTER_TX_COMMITTED;
	UT_ASSERT_EQ(auth_call(s, &admission, &ctx, &out), 2);
	mx_history_authorize_state(&after); UT_ASSERT_EQ(after.operation_id, before.operation_id);
	UT_ASSERT_EQ(after.restarts, 1); UT_ASSERT_EQ(auth_describes, 1);
	UT_ASSERT_EQ(finish_reads, 4); UT_ASSERT_EQ(out.result, TM_Ok);
	auth_finished(); auth_clean(s);
}
UT_TEST(authorize_all_native_tx_outcomes_preserve_polarity)
{
	const ClusterTxwResult outcomes[] = { CLUSTER_TXW_RESOLVED, CLUSTER_TXW_DEAD_HOLDER,
		CLUSTER_TXW_RETRY, CLUSTER_TXW_TIMEOUT, CLUSTER_TXW_UNPROVABLE, CLUSTER_TXW_DEADLOCK };
	const int codes[] = { 0, 0, 0, ERRCODE_CLUSTER_GES_TIMEOUT,
		ERRCODE_CLUSTER_CROSS_NODE_WRITE_CONFLICT, ERRCODE_T_R_DEADLOCK_DETECTED };
	for (unsigned i = 0; i < lengthof(outcomes); ++i) {
		Snapshot s = auth_setup(CLUSTER_TX_IN_PROGRESS, false);
		ClusterCurrentMxRequestContext ctx = heap_consumer_context(false); MxHeapReadout out;
		ctx.lock_mode = LockTupleExclusive; auth_tx_result = outcomes[i];
		UT_ASSERT_EQ(auth_call(s, &admission, &ctx, &out), i < 3 ? 1 : -1);
		UT_ASSERT_EQ(error_code, codes[i]); UT_ASSERT_EQ(auth_waits, 1);
		UT_ASSERT_EQ(remembered, 1); bridge_no_authority(&out);
		UT_ASSERT_EQ(content_locked, i < 3); if (i >= 3) auth_finished(); auth_clean(s);
	}
}
UT_TEST(authorize_conflict_skip_and_nowait)
{
	for (unsigned i = 0; i < 2; ++i) {
		Snapshot s = auth_setup(CLUSTER_TX_IN_PROGRESS, false);
		ClusterCurrentMxRequestContext ctx = heap_consumer_context(false); MxHeapReadout out;
		ctx.lock_mode = LockTupleExclusive; ctx.wait_policy = i ? LockWaitError : LockWaitSkip;
		UT_ASSERT_EQ(auth_call(s, &admission, &ctx, &out), i ? -1 : 2);
		UT_ASSERT_EQ(error_code, i ? ERRCODE_LOCK_NOT_AVAILABLE : 0);
		if (!i) UT_ASSERT_EQ(out.result, TM_WouldBlock);
		UT_ASSERT_EQ(auth_waits, 0); UT_ASSERT(content_locked); auth_finished(); auth_clean(s);
	}
}
UT_TEST(authorize_ERROR_finishes_operation_and_snapshot)
{
	for (unsigned i = 0; i < 3; ++i) {
		Snapshot s = auth_setup(CLUSTER_TX_IN_PROGRESS, false);
		ClusterCurrentMxRequestContext ctx = heap_consumer_context(false); MxHeapReadout out;
		ctx.lock_mode = LockTupleExclusive;
		auth_throw_describe = i == 0; auth_throw_wait = i == 2;
		if (i == 1) { finish_pending = true; history_wait_fault = 1; }
		UT_ASSERT_EQ(auth_call(s, &admission, &ctx, &out), -1);
		UT_ASSERT_EQ(remembered, 1); UT_ASSERT(!content_locked);
		bridge_no_authority(&out); auth_finished(); auth_clean(s);
	}
}
UT_TEST(authorize_drift_restarts_and_refetches_descriptor)
{
	for (unsigned i = 0; i < 2; ++i) {
		Snapshot s = auth_setup(CLUSTER_TX_COMMITTED, false);
		ClusterCurrentMxRequestContext ctx = heap_consumer_context(false); MxHeapReadout out;
		MxAuthorizeState before, after;
		if (i) auth_history_fault = 1; else auth_descriptor_fault = 1;
		UT_ASSERT_EQ(auth_call(s, &admission, &ctx, &out), 1);
		mx_history_authorize_state(&before); UT_ASSERT_EQ(before.restarts, 1);
		bridge_no_authority(&out); UT_ASSERT(content_locked);
		UT_ASSERT_EQ(auth_call(s, &admission, &ctx, &out), 2);
		mx_history_authorize_state(&after); UT_ASSERT_EQ(after.operation_id, before.operation_id);
		UT_ASSERT_EQ(auth_describes, 2); auth_finished(); auth_clean(s);
	}
}
UT_TEST(authorize_invalid_scope_or_descriptor_never_falls_back)
{
	for (unsigned i = 0; i < 7; ++i) {
		Snapshot s = auth_setup(CLUSTER_TX_COMMITTED, false);
		ClusterCurrentMxRequestContext ctx = heap_consumer_context(false); MxHeapReadout out;
		SnapshotData forged = *s; ClusterSemanticAdmissionToken bad = admission;
		Snapshot input = i == 0 ? NULL : i == 1 ? &forged : s;
		if (i == 2) memset(&bad, 0, sizeof(bad));
		if (i >= 3) auth_describe_fault = i - 2;
		UT_ASSERT_EQ(auth_call(input, i == 2 ? &bad : &admission, &ctx, &out), -1);
		UT_ASSERT_EQ(error_code, i == 3 ? ERRCODE_FEATURE_NOT_SUPPORTED :
			ERRCODE_CLUSTER_CROSS_NODE_WRITE_CONFLICT);
		UT_ASSERT_EQ(auth_waits, 0); bridge_no_authority(&out);
		UT_ASSERT_EQ(remembered, 1); auth_finished(); auth_clean(s);
	}
}
UT_TEST(authorize_follow_retains_active_operation_and_successor)
{
	Snapshot s = auth_setup(CLUSTER_TX_IN_PROGRESS, false);
	PageHeader ph = (PageHeader)BufferBlocks; ItemId lp = &ph->pd_linp[1];
	ClusterCurrentMxRequestContext ctx = heap_consumer_context(true); MxHeapReadout out;
	MxAuthorizeState state; HeapTupleHeader candidate;
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
	UT_ASSERT_EQ(auth_call(s, &admission, &ctx, &out), 2);
	mx_history_authorize_state(&state); UT_ASSERT(state.active && !state.finished);
	UT_ASSERT(out.follow_updated_chain && out.successor.valid); UT_ASSERT_EQ(out.result, TM_Ok);
	UT_ASSERT_EQ(out.retained.proof_count, 1); UT_ASSERT_EQ(finish_reads, 3);
	UT_ASSERT_EQ(remembered, 1); auth_clean(s);
}
UT_TEST(authorize_legacy_applicability_gate_is_preserved)
{
	for (unsigned i = 0; i < 4; ++i) {
		Snapshot s = auth_setup(CLUSTER_TX_COMMITTED, false);
		ClusterCurrentMxRequestContext ctx = heap_consumer_context(false); MxHeapReadout out;
		MxAuthorizeState state;
		if (i == 0) auth_conf.node_count = 1;
		if (i == 1) tuple.t_data->t_infomask &= ~HEAP_XMAX_IS_MULTI;
		if (i == 2) tuple.t_data->t_infomask |= HEAP_XMAX_INVALID;
		if (i == 3) tuple.t_data->t_infomask |= HEAP_XMAX_COMMITTED;
		UT_ASSERT_EQ(auth_call(s, &admission, &ctx, &out), 0);
		mx_history_authorize_state(&state); UT_ASSERT(!state.active); UT_ASSERT_EQ(state.operation_id, 0);
		UT_ASSERT_EQ(auth_describes, 0); UT_ASSERT_EQ(finish_requests, 0);
		bridge_no_authority(&out); auth_clean(s);
	}
}
int main(void)
{
	history_fixture_init(); UT_PLAN(10);
	UT_RUN(authorize_native_writer_terminal_receipts);
	UT_RUN(authorize_live_compatible_receipt_uses_original_capability);
	UT_RUN(authorize_exact_tx_restart_reuses_only_immutable_descriptor);
	UT_RUN(authorize_all_native_tx_outcomes_preserve_polarity);
	UT_RUN(authorize_conflict_skip_and_nowait);
	UT_RUN(authorize_ERROR_finishes_operation_and_snapshot);
	UT_RUN(authorize_drift_restarts_and_refetches_descriptor);
	UT_RUN(authorize_invalid_scope_or_descriptor_never_falls_back);
	UT_RUN(authorize_follow_retains_active_operation_and_successor);
	UT_RUN(authorize_legacy_applicability_gate_is_preserved);
	free(procArray); UT_DONE(); return ut_failed_count ? 1 : 0;
}
