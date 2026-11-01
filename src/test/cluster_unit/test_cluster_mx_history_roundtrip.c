/* Native Snapshot/collector, actual stripe derivation, dormant codecs and
 * operation consumers over the SAME private result set. Origin bytes,
 * outcomes and grants enter at the controlled external landing boundary;
 * this is not native TT40/CTRC, registered GCS transport or SQL evidence.
 * Portions Copyright (c) 2026, pgrac contributors */
#define HISTORY_ROUNDTRIP_FIXTURE
#include "test_cluster_mx_history_capture.c"
#include "cluster/cluster_multixact_operation_wire.h"
#include "cluster/cluster_xid_stripe_boot.h"
#include "test_cluster_mx_operation_receipt.h"

/* Only activation/nextXid leaves are controlled. The linked stripe arithmetic
 * derives raw child/top origins; no origin or proof verifier is stubbed. */
void
cluster_xid_stripe_lazy_latch(void)
{
	abort(); /* The fixture explicitly installs its original activation. */
}

static Snapshot
roundtrip_setup(ClusterTxOutcome outcome, bool discarded)
{
	Snapshot s = setup();
	ClusterItlSlotData *roots = ClusterPageGetItlSlots(BufferBlocks);
	vacuum_next_xid = U64FromFullTransactionId(FullTransactionIdFromEpochAndXid(1, 256));
	finish_sources();
	finish_top = 80;
	finish_outcome = outcome;
	finish_all_discarded = discarded;
	members[0].xid = 96;
	members[1].xid = 112;
	for (unsigned i = 0; i < 2; ++i) {
		roots[i].xid = finish_top;
		members[i].member_status = MultiXactStatusForShare;
	}
	cluster_xid_stripe_latch_runtime(true, cluster_node_id,
									 FullTransactionIdFromEpochAndXid(1, 3));
	UT_ASSERT_EQ(cluster_xid_origin_slot(finish_top), 0);
	UT_ASSERT_EQ(cluster_xid_origin_slot(members[0].xid), 0);
	UT_ASSERT_EQ(cluster_xid_origin_slot(members[1].xid), 0);
	return s;
}

static bool
roundtrip_collect(Snapshot s, ClusterMxHistoryCollector **c)
{
	if (!begin(s, c) || finish_ready(*c, false) != CMXH_COMPLETE
		|| finish_deliver(*c, 2) != CMXH_COMPLETE) {
		UT_ASSERT(false);
		return false;
	}
	UT_ASSERT_EQ(finish_reads, 2);
	UT_ASSERT_EQ(finish_requests, 2);
	UT_ASSERT_EQ(remembered, 2);
	return true;
}

static bool
roundtrip_plan(ClusterCurrentMxOperationRequestPlan *plan)
{
	uint16 origins[] = { 0, 0 }, n = 0;
	return cluster_multixact_operation_wire_build_requests(
			   &mxkey, members, origins, finish_keys, 2,
			   cluster_multixact_current_descriptor_hash(&mxkey, members, 2), NULL, 880, 7,
			   cluster_node_id, MyBackendId, plan, 1, &n)
			   == CMX_RESOLVE_OK
		   && n == 1;
}

static bool
roundtrip_decode(const ClusterCurrentMxOperationRequestPlan *plan,
				 const ClusterCurrentMxOperationReplyPageV6 *page,
				 ClusterCurrentMemberOperationProof out[2])
{
	ClusterMxResolveResult result;
	ClusterCurrentUpdaterProof updater;
	uint16 n;
	uint32 capability;
	bool ok = cluster_multixact_operation_wire_validate_reply(
		page, sizeof(*page), 0, 7, &plan->request, &result, out, 2, &n, &updater, &capability);
	if (ok) {
		UT_ASSERT_EQ(result, CMX_RESOLVE_OK);
		UT_ASSERT_EQ(n, 2);
		UT_ASSERT_EQ(capability, 12);
		UT_ASSERT_EQ(memcmp(out, finish_proofs, 2 * sizeof(*out)), 0);
	}
	return ok;
}

static ClusterCurrentMxDecision
roundtrip_decide(const ClusterCurrentMemberOperationProof proofs[2], ClusterTTStatusKey *wait)
{
	ClusterCurrentMxRequestContext ctx = { .mxkey = mxkey,
		.top_xid = 129, .current_member_xid = 145, .curcid = 5,
		.tuple_cmax = InvalidCommandId, .precheck_result = TM_BeingModified,
		.desired_status = MultiXactStatusForUpdate, .lock_mode = LockTupleExclusive,
		.wait_policy = LockWaitBlock, .action = CCM_ACTION_LOCK,
		.tuple_shape = CCM_SHAPE_LOCK_ONLY, .wait_for_conflict = 1,
		.updater_origin_node_id = -1 };
	ClusterR4CrOwnerChallenge owner = { 0 };
	return cluster_multixact_current_decide_operations(members, finish_keys, proofs, 2, &ctx,
													   &owner, NULL, NULL, wait, NULL);
}

UT_TEST(collector_wire_and_consumers_preserve_disposition)
{
	static const ClusterTxOutcome outcomes[]
		= { CLUSTER_TX_IN_PROGRESS, CLUSTER_TX_COMMITTED, CLUSTER_TX_ABORTED };
	for (unsigned state = 0; state < lengthof(outcomes); ++state)
		for (unsigned discarded = 0; discarded < 2; ++discarded) {
			Snapshot s = roundtrip_setup(outcomes[state], discarded);
			ClusterMxHistoryCollector *c = NULL;
			ClusterCurrentMxOperationRequestPlan plan = { 0 };
			ClusterCurrentMxOperationReplyPageV6 page = { 0 };
			ClusterCurrentMemberOperationProof out[2] = { 0 };
			ClusterTTStatusKey wait = { 0 };
			MultiXactMember normalized[3];
			uint16 count = 99;
			bool live_active = outcomes[state] == CLUSTER_TX_IN_PROGRESS && !discarded;
			if (!roundtrip_collect(s, &c)) { clean(c, s); continue; }
			UT_ASSERT_EQ(cluster_multixact_current_validate_operation_proof_set(
				&mxkey, members, finish_keys, finish_proofs, 2), CMX_RESOLVE_OK);
			UT_ASSERT(roundtrip_plan(&plan));
			if (!cluster_multixact_operation_wire_encode_reply(
					&plan.request, 0, 7, CMX_RESOLVE_OK, finish_proofs, 2, NULL, 12, &page)) {
				UT_ASSERT(false); clean(c, s); continue;
			}
			UT_ASSERT(roundtrip_decode(&plan, &page, out));
			UT_ASSERT_EQ(roundtrip_decide(out, &wait), live_active ? CMDL_WAIT_MEMBER : CMDL_CONTINUE);
			if (live_active) {
				UT_ASSERT_EQ(wait.local_xid, finish_top);
				UT_ASSERT_EQ(wait.origin_node_id, 0);
				UT_ASSERT_EQ(wait.undo_segment_id, 1);
				UT_ASSERT_EQ(wait.tt_slot_id, 4);
			}
			UT_ASSERT_EQ(cluster_multixact_current_recompose_operations(
				&mxkey, members, finish_keys, out, 2, 145, MultiXactStatusForUpdate,
				normalized, 3, &count), CMX_RECOMPOSE_OK);
			UT_ASSERT_EQ(count, live_active ? 2 : 1);
			UT_ASSERT_EQ(normalized[count - 1].xid, 145);
			if (live_active) UT_ASSERT_EQ(normalized[0].xid, 96);
			UT_ASSERT_EQ(finish_reads, 2); /* Never rerun member RPC/three stages. */
			UT_ASSERT_EQ(remembered, 2); /* Lease survives ALL consumer use. */
			clean(c, s);
		}
}

UT_TEST(late_grant_corruption_rejects_whole_collector_batch)
{
	Snapshot s = roundtrip_setup(CLUSTER_TX_IN_PROGRESS, false);
	ClusterMxHistoryCollector *c = NULL;
	ClusterCurrentMxOperationRequestPlan plan = { 0 };
	ClusterCurrentMxOperationReplyPageV6 page = { 0 };
	ClusterCurrentMemberOperationProof out[2] = { 0 };
	ClusterCurrentUpdaterProof updater;
	ClusterMxResolveResult result;
	uint16 count;
	uint32 capability;
	if (!roundtrip_collect(s, &c)) { clean(c, s); return; }
	UT_ASSERT(roundtrip_plan(&plan));
	UT_ASSERT(cluster_multixact_operation_wire_encode_reply(
		&plan.request, 0, 7, CMX_RESOLVE_OK, finish_proofs, 2, NULL, 12, &page));
	ClusterR4WireWriteU32(page.body + 88 + 40, 47); /* Discarded second member. */
	memset(out, 0xa5, sizeof(out));
	UT_ASSERT(!cluster_multixact_operation_wire_validate_reply(
		&page, sizeof(page), 0, 7, &plan.request, &result, out, 2, &count, &updater, &capability));
	UT_ASSERT_EQ(result, CMX_RESOLVE_UNKNOWN);
	UT_ASSERT_EQ(count, 0);
	UT_ASSERT_EQ(capability, 0);
	for (unsigned i = 0; i < 2; ++i) {
		UT_ASSERT_EQ(out[i].base.state, CCM_UNKNOWN);
		UT_ASSERT_EQ(out[i].flags, 0);
	}
	UT_ASSERT_EQ(remembered, 2);
	clean(c, s);
}

UT_TEST(original_relock_gate_prevents_wire_publication)
{
	Snapshot s = roundtrip_setup(CLUSTER_TX_IN_PROGRESS, false);
	ClusterMxHistoryCollector *c = NULL;
	UT_ASSERT(begin(s, &c));
	UT_ASSERT_EQ(finish_ready(c, false), CMXH_COMPLETE);
	pcm_source.generation++;
	UT_ASSERT_EQ(finish_deliver(c, 2), CMXH_REQUALIFY);
	finish_unknown(2);
	UT_ASSERT_EQ(cluster_multixact_current_validate_operation_proof_set(
		&mxkey, members, finish_keys, finish_proofs, 2), CMX_RESOLVE_UNKNOWN);
	UT_ASSERT_EQ(remembered, 1);
	clean(c, s);
}

static ClusterMxHistoryStep
receipt_finish(ClusterMxHistoryCollector *c, uint32 *capabilities, uint16 capacity,
	const ClusterCurrentUpdaterChallenge *challenge, ClusterCurrentUpdaterProof *updater)
{
	content_locked = true;
	return cluster_mx_history_recheck_and_finish_for_receipts(c, &relation, 1, &tuple,
		&mxkey, members, 2, finish_keys, finish_proofs, capabilities, capacity, challenge, updater);
}
UT_TEST(collector_original_capabilities_reach_real_heap_retainer)
{
	static const ClusterTxOutcome outcomes[] = {
		CLUSTER_TX_IN_PROGRESS, CLUSTER_TX_COMMITTED, CLUSTER_TX_ABORTED };
	for (unsigned state = 0; state < lengthof(outcomes); ++state)
		for (unsigned discarded = 0; discarded < 2; ++discarded) {
			Snapshot s = roundtrip_setup(outcomes[state], discarded);
			ClusterMxHistoryCollector *c = NULL;
			MxReceiptReadout retained;
			uint32 capabilities[3] = { 99, 99, 99 };
			bool live = outcomes[state] == CLUSTER_TX_IN_PROGRESS && !discarded;
			finish_distinct_capabilities = true;
			UT_ASSERT(begin(s, &c));
			UT_ASSERT_EQ(finish_ready(c, false), CMXH_COMPLETE);
			UT_ASSERT_EQ(receipt_finish(c, capabilities, 3, NULL, NULL), CMXH_COMPLETE);
			UT_ASSERT_EQ(capabilities[0], 21);
			UT_ASSERT_EQ(capabilities[1], 34);
			UT_ASSERT_EQ(capabilities[2], 0);
			UT_ASSERT(mx_receipt_retain(&mxkey, members, finish_keys, finish_proofs,
				capabilities, 2, 145, MultiXactStatusForKeyShare, 900, 4, &retained));
			UT_ASSERT_EQ(retained.proof_count, live ? 1 : 0);
			UT_ASSERT_EQ(retained.normalized_count, live ? 2 : 1);
			UT_ASSERT_EQ(retained.operation_id, 900);
			UT_ASSERT_EQ(retained.attempt_generation, 5);
			if (live) {
				UT_ASSERT_EQ(retained.proofs[0].member_xid, 96);
				UT_ASSERT_EQ(retained.proofs[0].key.local_xid, 80);
				UT_ASSERT_EQ(retained.capabilities[0], 21);
			}
			UT_ASSERT_EQ(finish_reads, 2);
			UT_ASSERT_EQ(finish_requests, 2);
			UT_ASSERT_EQ(remembered, 2);
			clean(c, s);
		}
}
UT_TEST(collector_receipt_delivery_is_once_and_keeps_snapshot)
{
	Snapshot s = roundtrip_setup(CLUSTER_TX_IN_PROGRESS, false);
	ClusterMxHistoryCollector *c = NULL;
	uint32 capabilities[3] = { 99, 99, 99 };
	UT_ASSERT(begin(s, &c));
	UT_ASSERT_EQ(finish_ready(c, false), CMXH_COMPLETE);
	UT_ASSERT_EQ(receipt_finish(c, capabilities, 3, NULL, NULL), CMXH_COMPLETE);
	UT_ASSERT_EQ(capabilities[0], 12);
	UT_ASSERT_EQ(capabilities[1], 12);
	UT_ASSERT_EQ(receipt_finish(c, capabilities, 3, NULL, NULL), CMXH_UNKNOWN);
	UT_ASSERT_EQ(capabilities[0] | capabilities[1] | capabilities[2], 0);
	finish_unknown(3);
	UT_ASSERT_EQ(remembered, 2);
	UT_ASSERT_EQ(finish_reads, 2);
	clean(c, s);
}
UT_TEST(collector_receipt_pending_has_no_partial_capability)
{
	Snapshot s = roundtrip_setup(CLUSTER_TX_IN_PROGRESS, false);
	ClusterMxHistoryCollector *c = NULL;
	uint32 capabilities[3] = { 99, 99, 99 };
	UT_ASSERT(begin(s, &c));
	UT_ASSERT_EQ(receipt_finish(c, capabilities, 3, NULL, NULL), CMXH_PENDING);
	UT_ASSERT_EQ(capabilities[0] | capabilities[1] | capabilities[2], 0);
	finish_unknown(3);
	UT_ASSERT_EQ(finish_reads, 0);
	UT_ASSERT_EQ(finish_ready(c, false), CMXH_COMPLETE);
	UT_ASSERT_EQ(receipt_finish(c, capabilities, 3, NULL, NULL), CMXH_COMPLETE);
	UT_ASSERT_EQ(capabilities[0], 12);
	UT_ASSERT_EQ(remembered, 2);
	clean(c, s);
}
UT_TEST(collector_receipt_relock_drift_clears_entire_batch)
{
	Snapshot s = roundtrip_setup(CLUSTER_TX_IN_PROGRESS, false);
	ClusterMxHistoryCollector *c = NULL;
	uint32 capabilities[3] = { 99, 99, 99 };
	UT_ASSERT(begin(s, &c));
	UT_ASSERT_EQ(finish_ready(c, false), CMXH_COMPLETE);
	pcm_source.generation++;
	UT_ASSERT_EQ(receipt_finish(c, capabilities, 3, NULL, NULL), CMXH_REQUALIFY);
	UT_ASSERT_EQ(capabilities[0] | capabilities[1] | capabilities[2], 0);
	finish_unknown(3);
	UT_ASSERT_EQ(remembered, 1);
	clean(c, s);
}
UT_TEST(collector_receipt_capability_output_is_required_and_bounded)
{
	for (unsigned fault = 0; fault < 3; ++fault) {
		Snapshot s = roundtrip_setup(CLUSTER_TX_IN_PROGRESS, false);
		ClusterMxHistoryCollector *c = NULL;
		uint32 capabilities[3] = { 99, 99, 99 };
		uint16 capacity = fault == 1 ? 1 : fault == 2 ? 257 : 3;
		UT_ASSERT(begin(s, &c));
		UT_ASSERT_EQ(finish_ready(c, false), CMXH_COMPLETE);
		UT_ASSERT_EQ(receipt_finish(c, fault == 0 ? NULL : capabilities, capacity, NULL, NULL),
			CMXH_UNKNOWN);
		if (fault == 1) { UT_ASSERT_EQ(capabilities[0], 0); UT_ASSERT_EQ(capabilities[1], 99); }
		else UT_ASSERT_EQ(capabilities[0], 99);
		UT_ASSERT_EQ(remembered, 1);
		clean(c, s);
	}
}
UT_TEST(collector_receipt_batch_keeps_A_capability_separate_from_B)
{
	Snapshot s = roundtrip_setup(CLUSTER_TX_IN_PROGRESS, false);
	ClusterMxHistoryCollector *c = NULL;
	ClusterCurrentUpdaterChallenge challenge = updater_challenge(6);
	ClusterCurrentUpdaterProof updater;
	uint32 capabilities[3] = { 99, 99, 99 };
	finish_successor_in_graph = finish_distinct_capabilities = true;
	UT_ASSERT(updater_begin(s, &challenge, &c));
	UT_ASSERT_EQ(finish_ready(c, false), CMXH_COMPLETE);
	UT_ASSERT_EQ(receipt_finish(c, capabilities, 3, &challenge, &updater), CMXH_COMPLETE);
	UT_ASSERT_EQ(updater.verdict, CUCP_MATCH);
	UT_ASSERT_EQ(capabilities[0], 21); /* Not B's independent request generation 55. */
	UT_ASSERT_EQ(capabilities[1], 34);
	UT_ASSERT_EQ(finish_reads, 3);
	UT_ASSERT_EQ(finish_requests, 3);
	UT_ASSERT_EQ(remembered, 2);
	clean(c, s);
}
UT_TEST(collector_receipt_B_challenge_drift_invalidates_all_outputs)
{
	Snapshot s = roundtrip_setup(CLUSTER_TX_IN_PROGRESS, false);
	ClusterMxHistoryCollector *c = NULL;
	ClusterCurrentUpdaterChallenge challenge = updater_challenge(7);
	ClusterCurrentUpdaterProof updater;
	uint32 capabilities[3] = { 99, 99, 99 };
	UT_ASSERT(updater_begin(s, &challenge, &c));
	UT_ASSERT_EQ(finish_ready(c, false), CMXH_COMPLETE);
	challenge.candidate_next_xmin_locator.itl_slot_index++;
	UT_ASSERT_EQ(receipt_finish(c, capabilities, 3, &challenge, &updater), CMXH_REQUALIFY);
	UT_ASSERT_EQ(updater.verdict, CUCP_UNKNOWN);
	UT_ASSERT_EQ(capabilities[0] | capabilities[1] | capabilities[2], 0);
	finish_unknown(3);
	UT_ASSERT_EQ(remembered, 1);
	clean(c, s);
}

static ClusterCurrentMxRequestContext
heap_consumer_context(bool updater)
{
	ClusterCurrentMxRequestContext ctx = { .mxkey = mxkey,
		.top_xid = 129, .current_member_xid = 145, .curcid = 5,
		.tuple_cmax = InvalidCommandId, .precheck_result = TM_BeingModified,
		.desired_status = MultiXactStatusForKeyShare, .lock_mode = LockTupleKeyShare,
		.wait_policy = LockWaitBlock, .action = CCM_ACTION_LOCK,
		.tuple_shape = updater ? CCM_SHAPE_UPDATED : CCM_SHAPE_LOCK_ONLY,
		.follow_updates = updater, .wait_for_conflict = 1,
		.updater_origin_node_id = updater ? 0 : -1 };
	return ctx;
}
UT_TEST(collector_batch_drives_heap_decision_and_original_capability_retention)
{
	for (unsigned conflict = 0; conflict < 2; ++conflict) {
		Snapshot s = roundtrip_setup(CLUSTER_TX_IN_PROGRESS, false);
		ClusterMxHistoryCollector *c = NULL;
		ClusterCurrentMxRequestContext ctx = heap_consumer_context(false);
		ClusterR4CrOwnerChallenge owner = { 0 };
		MxHeapReadout out;
		uint32 capabilities[2];
		if (conflict) {
			ctx.lock_mode = LockTupleExclusive;
			ctx.desired_status = MultiXactStatusForUpdate;
		}
		finish_distinct_capabilities = true;
		UT_ASSERT(begin(s, &c));
		UT_ASSERT_EQ(finish_ready(c, false), CMXH_COMPLETE);
		UT_ASSERT_EQ(receipt_finish(c, capabilities, 2, NULL, NULL), CMXH_COMPLETE);
		UT_ASSERT_EQ(mx_receipt_decide(members, finish_keys, finish_proofs, capabilities,
			2, &ctx, &owner, NULL, NULL, NULL, 900, 4, &out),
			conflict ? CMDL_WAIT_MEMBER : CMDL_CONTINUE);
		UT_ASSERT_EQ(out.result, conflict ? TM_BeingModified : TM_Ok);
		UT_ASSERT_EQ(out.retained.proof_count, conflict ? 0 : 1);
		UT_ASSERT_EQ(out.retained.normalized_count, conflict ? 0 : 2);
		if (conflict) UT_ASSERT_EQ(out.wait_key.local_xid, 80);
		else {
			UT_ASSERT_EQ(out.retained.proofs[0].member_xid, 96);
			UT_ASSERT_EQ(out.retained.proofs[0].key.local_xid, 80);
			UT_ASSERT_EQ(out.retained.capabilities[0], 21);
		}
		UT_ASSERT_EQ(remembered, 2);
		UT_ASSERT_EQ(finish_requests, 2);
		UT_ASSERT_EQ(finish_reads, 2);
		clean(c, s);
	}
}
UT_TEST(collector_independent_B_reaches_heap_follow_and_updated_decisions)
{
	static const ClusterTxOutcome outcomes[] = {
		CLUSTER_TX_IN_PROGRESS, CLUSTER_TX_COMMITTED, CLUSTER_TX_ABORTED };
	for (unsigned state = 0; state < lengthof(outcomes); ++state)
		for (unsigned discarded = 0; discarded < 2; ++discarded) {
			Snapshot s = roundtrip_setup(outcomes[state], discarded);
			ClusterMxHistoryCollector *c = NULL;
			ClusterCurrentUpdaterChallenge challenge = updater_challenge(6);
			ClusterCurrentUpdaterProof updater;
			ClusterCurrentMxRequestContext ctx = heap_consumer_context(true);
			ClusterR4CrOwnerChallenge owner = { 0 };
			ItemPointerData successor;
			MxHeapReadout out;
			uint32 capabilities[2];
			bool live = !discarded && outcomes[state] == CLUSTER_TX_IN_PROGRESS;
			bool committed = !discarded && outcomes[state] == CLUSTER_TX_COMMITTED;
			ItemPointerSet(&successor, 23, 4);
			finish_successor_in_graph = finish_distinct_capabilities = true;
			UT_ASSERT(updater_begin(s, &challenge, &c));
			UT_ASSERT_EQ(finish_ready(c, false), CMXH_COMPLETE);
			UT_ASSERT_EQ(receipt_finish(c, capabilities, 2, &challenge, &updater), CMXH_COMPLETE);
			UT_ASSERT_EQ(mx_receipt_decide(members, finish_keys, finish_proofs, capabilities,
				2, &ctx, &owner, &challenge, &updater, &successor, 900, 4, &out),
				live ? CMDL_FOLLOW_UPDATED : committed ? CMDL_UPDATED : CMDL_CONTINUE);
			UT_ASSERT_EQ(out.result, committed ? TM_Updated : TM_Ok);
			UT_ASSERT_EQ(out.follow_updated_chain, live);
			UT_ASSERT_EQ(out.successor.valid, live || committed);
			UT_ASSERT_EQ(out.authoritative_updater, discarded ? InvalidTransactionId : 96);
			UT_ASSERT_EQ(out.retained.proof_count, live ? 1 : 0);
			if (live || committed) {
				UT_ASSERT_EQ(out.successor.updater_xid, 96);
				UT_ASSERT_EQ(out.successor.locator_uba_raw[0], updater.candidate_next_xmin_locator.uba.raw[0]);
				UT_ASSERT_EQ(out.successor.locator_uba_raw[1], updater.candidate_next_xmin_locator.uba.raw[1]);
			}
			if (live) UT_ASSERT_EQ(out.retained.capabilities[0], 21);
			UT_ASSERT_EQ(remembered, 2);
			UT_ASSERT_EQ(finish_requests, 3);
			UT_ASSERT_EQ(finish_reads, 3);
			clean(c, s);
		}
}

#include "test_cluster_mx_history_heap_bridge.inc"

#ifdef HISTORY_AUTHORIZE_FIXTURE
int history_roundtrip_unused_main(void);
#define main history_roundtrip_unused_main
#endif
int
main(void)
{
	history_fixture_init();
	UT_PLAN(20);
	UT_RUN(collector_wire_and_consumers_preserve_disposition);
	UT_RUN(late_grant_corruption_rejects_whole_collector_batch);
	UT_RUN(original_relock_gate_prevents_wire_publication);
	UT_RUN(collector_original_capabilities_reach_real_heap_retainer);
	UT_RUN(collector_receipt_delivery_is_once_and_keeps_snapshot);
	UT_RUN(collector_receipt_pending_has_no_partial_capability);
	UT_RUN(collector_receipt_relock_drift_clears_entire_batch);
	UT_RUN(collector_receipt_capability_output_is_required_and_bounded);
	UT_RUN(collector_receipt_batch_keeps_A_capability_separate_from_B);
	UT_RUN(collector_receipt_B_challenge_drift_invalidates_all_outputs);
	UT_RUN(collector_batch_drives_heap_decision_and_original_capability_retention);
	UT_RUN(collector_independent_B_reaches_heap_follow_and_updated_decisions);
	free(procArray);
	UT_RUN(heap_bridge_native_capture_wait_relock_receipts);
	UT_RUN(heap_bridge_native_conflict_policy_and_wait_key);
	UT_RUN(heap_bridge_relock_drift_scrubs_whole_attempt);
	UT_RUN(heap_bridge_invalid_scope_never_acquires_history);
	UT_RUN(heap_bridge_wait_ERROR_releases_snapshot);
	UT_RUN(heap_bridge_exact_owner_before_local_combo_decode);
	UT_RUN(heap_bridge_checked_combo_bounds_and_plain_cmax);
	UT_RUN(heap_bridge_native_successor_recheck);
	UT_DONE();
	return ut_failed_count ? 1 : 0;
}
