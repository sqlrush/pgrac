/* Collector/writer scope -> native descriptor/PREPARE/APPLY -> native ITL
 * stamp -> destination matcher. Exact-target qualification, descriptor storage,
 * grant identity and receipt-table leaves are controlled. This does not run
 * complete heap recursion, the unified undo boundary, WAL durability or SQL. */
#define HISTORY_WRITER_PUBLICATION_FIXTURE
#define GetMultiXactIdMembers publication_forbidden_legacy_members
#define cluster_runtime_visibility_current_owner_lookup_exact_ctrc_full publication_forbidden_legacy_owner
#define cluster_runtime_visibility_active_proof_ctrc_identity_exact publication_unused_identity
#include "test_cluster_mx_history_writer_bridge.c"
#undef cluster_runtime_visibility_active_proof_ctrc_identity_exact
#undef cluster_runtime_visibility_current_owner_lookup_exact_ctrc_full
#undef GetMultiXactIdMembers
#undef HISTORY_WRITER_PUBLICATION_FIXTURE
#include "cluster/cluster_itl.h"

static MultiXactMember publication_members[2];
static unsigned publication_count, publication_prepares, publication_applies, publication_cancels;
static bool publication_retry;
static unsigned publication_retry_at;
static uint32 publication_A_capability;
static ClusterCtrcTargetV1 publication_applied_target;

/* Restore the original external names after renaming only the old fixture
 * abort leaves. The separately compiled product adapter uses these leaves. */
int GetMultiXactIdMembers(MultiXactId, MultiXactMember **, bool, bool);
bool cluster_runtime_visibility_current_owner_lookup_exact_ctrc_full(TransactionId,
	ClusterTTStatusKey *, ClusterTTStatusResult *, uint32 *, ClusterCtrcTxnKeyV1 *,
	ClusterCtrcParticipantIdentity *);
bool cluster_runtime_visibility_active_proof_ctrc_identity_exact(
	const ClusterCurrentMemberProofKey *, uint32, uint32,
	ClusterCtrcTxnKeyV1 *, ClusterCtrcParticipantIdentity *);

static void
publication_identity(TransactionId top, uint32 capability,
	ClusterCtrcTxnKeyV1 *key, ClusterCtrcParticipantIdentity *participant)
{
	*key = (ClusterCtrcTxnKeyV1){ .format_version = 1, .owner_instance = top == 80 ? 0 : 1,
		.origin_node_id = top == 80 ? 0 : 1, .segment_id = top == 80 ? 1 : 257,
		.slot_offset = 3, .xid = top, .cluster_epoch = 7, .system_identifier = 101,
		.origin_boot_incarnation = 11, .formation_epoch = 7, .admission_record_generation = 9,
		.root_descriptor_incarnation = 1, .root_id = 1, .root_generation = 1 };
	*participant = (ClusterCtrcParticipantIdentity){ .node_id = 1,
		.capability_record_generation = capability, .boot_incarnation = 19,
		.formation_epoch = 7, .admission_record_generation = 9 };
}
bool
cluster_runtime_visibility_active_proof_ctrc_identity_exact(
	const ClusterCurrentMemberProofKey *key, uint32 grant, uint32 capability,
	ClusterCtrcTxnKeyV1 *ctrc, ClusterCtrcParticipantIdentity *participant)
{
	UT_ASSERT_EQ(key->local_xid, 80); UT_ASSERT_EQ(grant, 47);
	UT_ASSERT(capability == 21 || capability == 55);
	publication_identity(80, capability, ctrc, participant);
	return writer_grant_ok;
}
bool
cluster_runtime_visibility_current_owner_lookup_exact_ctrc_full(TransactionId xid,
	ClusterTTStatusKey *key, ClusterTTStatusResult *result, uint32 *grant,
	ClusterCtrcTxnKeyV1 *ctrc, ClusterCtrcParticipantIdentity *participant)
{
	UT_ASSERT_EQ(xid, 145);
	memset(key, 0, sizeof(*key)); memset(result, 0, sizeof(*result));
	result->status = CLUSTER_TT_STATUS_IN_PROGRESS; *grant = 73;
	publication_identity(129, 66, ctrc, participant);
	return true;
}
MultiXactId
MultiXactIdCreateLocalCurrentMembers(int count, MultiXactMember *members)
{
	UT_ASSERT_EQ(count, 2);
	memcpy(publication_members, members, count * sizeof(*members));
	publication_count = count;
	return 33; /* Original stripe arithmetic derives requester node1. */
}
int
GetMultiXactIdMembers(MultiXactId id, MultiXactMember **out, bool upgrade, bool lockonly)
{
	UT_ASSERT_EQ(id, 33); UT_ASSERT(!upgrade && !lockonly);
	*out = malloc(publication_count * sizeof(**out));
	memcpy(*out, publication_members, publication_count * sizeof(**out));
	return publication_count;
}
ClusterCtrcPrepareResult
cluster_ctrc_receipt_prepare_shared(const ClusterCtrcTxnKeyV1 *key,
	const ClusterCtrcParticipantIdentity *participant, uint32 grant,
	const ClusterCtrcPublicationIdV1 *publication, const ClusterCtrcTargetV1 *target,
	ClusterCtrcReceiptHandle *handle)
{
	UT_ASSERT(!content_locked);
	UT_ASSERT_EQ(target->kind, CTRC_TARGET_PAGE_PENDING_OFFNUM);
	UT_ASSERT_EQ(participant->node_id, 1);
	UT_ASSERT_EQ(publication->grant_generation, grant);
	UT_ASSERT(publication->operation_id != 0 && publication->descriptor_hash != 0);
	UT_ASSERT_EQ(publication->capability_record_generation, participant->capability_record_generation);
	if (key->xid == 80) publication_A_capability = participant->capability_record_generation;
	memset(handle, 0, sizeof(*handle)); handle->key = *key; handle->valid = true;
	handle->receipt_index = ++publication_prepares;
	return CLUSTER_CTRC_PREPARE_READY;
}
bool
cluster_ctrc_receipt_cancel_shared(const ClusterCtrcReceiptHandle *handle)
{
	UT_ASSERT(handle->valid); publication_cancels++; return true;
}
ClusterCtrcApplyResult
cluster_ctrc_receipt_apply_shared(const ClusterCtrcReceiptHandle *handle,
	const ClusterCtrcTargetV1 *target, ClusterCtrcApplyToken *token)
{
	UT_ASSERT(content_locked && exclusive); UT_ASSERT(handle->valid);
	UT_ASSERT_EQ(target->kind, CTRC_TARGET_EXACT_TID);
	publication_applies++; publication_applied_target = *target;
	memset(token, 0, sizeof(*token));
	if (publication_retry || publication_retry_at == publication_applies)
		return CLUSTER_CTRC_APPLY_RETRY_REQUIRED;
	token->valid = true; return CLUSTER_CTRC_APPLY_APPLIED;
}
static Snapshot
publication_setup(MxHeapReadout *out)
{
	Snapshot s = writer_setup(CLUSTER_TX_IN_PROGRESS, true);
	publication_count = publication_prepares = publication_applies = publication_cancels = 0;
	publication_A_capability = 0; publication_retry = false; publication_retry_at = 0;
	memset(&publication_applied_target, 0, sizeof(publication_applied_target));
	UT_ASSERT_EQ(writer_scoped_call(out), 2);
	return s;
}
static bool
publication_prepare(MxPublicationReadout *plan)
{
	ClusterCtrcTargetV1 pending = { .kind = CTRC_TARGET_PAGE_PENDING_OFFNUM };
	volatile bool prepared = false;
	LockBuffer(1, BUFFER_LOCK_UNLOCK);
	PG_TRY(); { prepared = mx_publication_prepare(145, 1, LockTupleKeyShare, &pending, plan); }
	PG_CATCH(); { printf("# PREPARE rejected: code=%d prepares=%u\n", error_code, publication_prepares); }
	PG_END_TRY();
	LockBuffer(1, BUFFER_LOCK_EXCLUSIVE);
	return prepared;
}
static int
publication_apply(MxPublicationReadout *plan)
{
	/* Exact target qualification is a controlled boundary input here, as in
	 * the original publication fixture; no native target-capture claim. */
	ClusterCtrcTargetV1 target = { .kind = CTRC_TARGET_EXACT_TID, .block_number = 3,
		.offset_number = 1, .multixact_id = plan->descriptor,
		.descriptor_hash = plan->descriptor_hash };
	return mx_publication_apply(&target, false, plan);
}
/* Full boundary/undo qualification is controlled; PREPARE/APPLY and the
 * ITL publisher are actual product bodies. A real WAL-return input is
 * substituted here, never inferred from a post-write capture. */
static bool
transition_plan(MxHeapReadout *out, MxPublicationReadout *plan, bool rf)
{
	return mx_history_transition_plan(1, &tuple, &out->successor, plan, rf);
}
static void
transition_write(bool rf)
{
	mx_history_transition_write_header(&tuple, rf);
	cluster_itl_stamp_lock_active_with_history(1, 7, 129, 91, uba_encode(257, 12, 3, 0));
	PageSetLSN(BufferBlocks, UINT64CONST(129));
}
UT_TEST(child_A_receipts_and_B_remain_independent_through_APPLY)
{
	MxHeapReadout out; MxPublicationReadout plan;
	Snapshot s = publication_setup(&out); HeapTupleData candidate = hot_candidate();
	UT_ASSERT_EQ(writer_match(&candidate, &out.successor, true), 1);
	if (!publication_prepare(&plan)) {
		UT_ASSERT(false); writer_clean(s, true); return;
	}
	UT_ASSERT_EQ(publication_A_capability, 21);
	UT_ASSERT_EQ(out.successor.writer.capability_generation, 55);
	UT_ASSERT_EQ(publication_prepares, 2);
	UT_ASSERT_EQ(publication_apply(&plan), 0); UT_ASSERT(plan.any_applied);
	UT_ASSERT_EQ(publication_applies, 2); UT_ASSERT_EQ(publication_applied_target.offset_number, 1);
	UT_ASSERT_EQ(writer_match(&candidate, &out.successor, true), 1);
	mx_publication_cancel(&plan); UT_ASSERT_EQ(publication_cancels, 0);
	UT_ASSERT_EQ(finish_reads, 3); writer_clean(s, true);
}
UT_TEST(zero_APPLY_refusal_preserves_unpublished_successor)
{
	MxHeapReadout out; MxPublicationReadout plan;
	Snapshot s = publication_setup(&out); HeapTupleData candidate = hot_candidate();
	if (!publication_prepare(&plan)) {
		UT_ASSERT(false); writer_clean(s, true); return;
	}
	publication_retry = true;
	UT_ASSERT_EQ(publication_apply(&plan), 1); UT_ASSERT(!plan.any_applied);
	UT_ASSERT_EQ(publication_applies, 1); UT_ASSERT_EQ(publication_cancels, 2);
	UT_ASSERT_EQ(writer_match(&candidate, &out.successor, true), 1);
	UT_ASSERT_EQ(ClusterPageGetItlSlots(BufferBlocks)[7].flags, ITL_FLAG_FREE);
	UT_ASSERT_EQ(finish_reads, 3); writer_clean(s, true);
}
UT_TEST(child_same_page_successor_progress_after_own_publication)
{
	MxHeapReadout out; MxPublicationReadout plan; ClusterHeapSuccessorProof next;
	uint8 before[SizeofHeapTupleHeader];
	Snapshot s = publication_setup(&out); HeapTupleData candidate = hot_candidate();
	UT_ASSERT_EQ(writer_match(&candidate, &out.successor, true), 1);
	memcpy(before, candidate.t_data, sizeof(before));
	if (!publication_prepare(&plan)) {
		UT_ASSERT(false); writer_clean(s, true); return;
	}
	UT_ASSERT(mx_history_recursive_next(&tuple, &next)); /* Original pre-APPLY saved edge. */
	if (!transition_plan(&out, &plan, false)) {
		UT_ASSERT(false); mx_publication_cancel(&plan); writer_clean(s, true); return;
	}
	UT_ASSERT_EQ(publication_apply(&plan), 0); UT_ASSERT(plan.any_applied);
	mx_history_transition_write_header(&tuple, false);
	/* The actual native publisher changes another ITL slot on the same page.
	 * The candidate's complete tuple header and immutable B are unchanged. */
	cluster_itl_stamp_lock_active_with_history(1, 7, 129, 91, uba_encode(257, 12, 3, 0));
	PageSetLSN(BufferBlocks, UINT64CONST(129));
	UT_ASSERT_EQ(memcmp(before, candidate.t_data, sizeof(before)), 0);
	UT_ASSERT_EQ(ClusterPageGetItlSlots(BufferBlocks)[7].xid, 129);
	UT_ASSERT_EQ(ClusterPageGetItlSlots(BufferBlocks)[7].flags, ITL_FLAG_LOCK_ONLY_ACTIVE);
	UT_ASSERT_EQ(publication_prepares, 2); UT_ASSERT_EQ(publication_applies, 2);
	UT_ASSERT(mx_history_transition_finish(1, true, UINT64CONST(129)));
	UT_ASSERT_EQ(memcmp(&next, &out.successor, sizeof(next)), 0); /* Immutable B. */
	UT_ASSERT_EQ(writer_match(&candidate, &next, true), 1);
	UT_ASSERT_EQ(writer_match(&candidate, &next, true), 0); /* Consume once. */
	UT_ASSERT_EQ(finish_reads, 3); UT_ASSERT_EQ(publication_cancels, 0);
	mx_publication_cancel(&plan); writer_clean(s, true);
}
UT_TEST(unapplied_same_page_change_cannot_authorize_old_B)
{
	MxHeapReadout out;
	Snapshot s = publication_setup(&out); HeapTupleData candidate = hot_candidate();
	UT_ASSERT_EQ(writer_match(&candidate, &out.successor, true), 1);
	cluster_itl_stamp_lock_active_with_history(1, 7, 129, 91, uba_encode(257, 12, 3, 0));
	PageSetLSN(BufferBlocks, UINT64CONST(129));
	UT_ASSERT_EQ(writer_match(&candidate, &out.successor, true), 0);
	UT_ASSERT_EQ(publication_prepares, 0); UT_ASSERT_EQ(publication_applies, 0);
	UT_ASSERT_EQ(finish_reads, 3); writer_clean(s, true);
}

UT_TEST(transition_pre_APPLY_plan_and_receipt_bindings_cannot_drift)
{
	for (unsigned fault = 0; fault < 5; fault++) {
		MxHeapReadout out; MxPublicationReadout plan;
		Snapshot s = publication_setup(&out); HeapTupleData candidate = hot_candidate();
		if (!publication_prepare(&plan) || !transition_plan(&out, &plan, true)) {
			UT_ASSERT(false); mx_publication_cancel(&plan); writer_clean(s, true); continue;
		}
		UT_ASSERT_EQ(publication_apply(&plan), 0);
		transition_write(true);
		mx_history_transition_fault(fault);
		UT_ASSERT(!mx_history_transition_finish(1, true, 129));
		UT_ASSERT_EQ(writer_match(&candidate, &out.successor, true), 0);
		UT_ASSERT_EQ(publication_cancels, 0); writer_clean(s, true);
	}
}
UT_TEST(transition_no_or_partial_APPLY_has_no_continuation)
{
	for (unsigned fault = 0; fault < 3; fault++) {
		MxHeapReadout out; MxPublicationReadout plan;
		Snapshot s = publication_setup(&out); HeapTupleData candidate = hot_candidate();
		if (!publication_prepare(&plan) || !transition_plan(&out, &plan, false)) {
			UT_ASSERT(false); mx_publication_cancel(&plan); writer_clean(s, true); continue;
		}
		if (fault != 0) {
			publication_retry_at = 2;
			UT_ASSERT_EQ(publication_apply(&plan), 2);
			UT_ASSERT(plan.any_applied);
		}
		transition_write(false); /* An unqualified physical write proves nothing. */
		UT_ASSERT(!mx_history_transition_finish(1, fault != 2, 129));
		UT_ASSERT_EQ(writer_match(&candidate, &out.successor, true), 0);
		if (fault != 0) UT_ASSERT_EQ(publication_cancels, 0);
		writer_clean(s, true);
	}
}
UT_TEST(transition_exact_page_rejects_extra_changes_before_and_after_finish)
{
	for (unsigned when = 0; when < 2; when++)
		for (unsigned fault = 0; fault < 4; fault++) {
			MxHeapReadout out; MxPublicationReadout plan;
			Snapshot s = publication_setup(&out); HeapTupleData candidate = hot_candidate();
			if (!publication_prepare(&plan) || !transition_plan(&out, &plan, false)) {
				UT_ASSERT(false); mx_publication_cancel(&plan); writer_clean(s, true); continue;
			}
			UT_ASSERT_EQ(publication_apply(&plan), 0); transition_write(false);
			if (when) UT_ASSERT(mx_history_transition_finish(1, true, 129));
			if (fault == 0) candidate.t_data->t_infomask2++;
			if (fault == 1) ClusterPageGetItlSlots(BufferBlocks)[0].write_scn++;
			if (fault == 2) ((PageHeader)BufferBlocks)->pd_prune_xid++;
			if (fault == 3) PageXLogRecPtrSet(((PageHeader)BufferBlocks)->pd_lsn, UINT64CONST(130));
			if (!when) UT_ASSERT(!mx_history_transition_finish(1, true, 129));
			UT_ASSERT_EQ(writer_match(&candidate, &out.successor, true), 0);
			UT_ASSERT_EQ(publication_cancels, 0); writer_clean(s, true);
		}
}
UT_TEST(transition_live_scope_owner_grant_and_B_are_required)
{
	for (unsigned fault = 0; fault < 5; fault++) {
		MxHeapReadout out; MxPublicationReadout plan; int result;
		Snapshot s = publication_setup(&out); HeapTupleData candidate = hot_candidate();
		ResourceOwner original_owner = CurrentResourceOwner;
		if (!publication_prepare(&plan) || !transition_plan(&out, &plan, false)) {
			UT_ASSERT(false); mx_publication_cancel(&plan); writer_clean(s, true); continue;
		}
		UT_ASSERT_EQ(publication_apply(&plan), 0); transition_write(false);
		UT_ASSERT(mx_history_transition_finish(1, true, 129));
		if (fault == 0) CurrentResourceOwner = (ResourceOwner) 2;
		if (fault == 1) pcm_source.generation++;
		if (fault == 2) writer_grant_ok = false;
		if (fault == 3) out.successor.writer.capability_generation++;
		if (fault == 4) s->read_scn++;
		result = writer_match(&candidate, &out.successor, true);
		UT_ASSERT(result == 0 || result == -1);
		CurrentResourceOwner = original_owner;
		UT_ASSERT_EQ(publication_cancels, 0); writer_clean(s, true);
	}
}
UT_TEST(transition_late_plan_cross_page_and_reopened_scope_refuse)
{
	for (unsigned fault = 0; fault < 3; fault++) {
		MxHeapReadout out; MxPublicationReadout plan;
		Snapshot s = publication_setup(&out); HeapTupleData candidate = hot_candidate();
		UT_ASSERT(publication_prepare(&plan));
		if (fault == 0) {
			UT_ASSERT_EQ(publication_apply(&plan), 0);
			UT_ASSERT(!transition_plan(&out, &plan, false));
		} else {
			if (!transition_plan(&out, &plan, false)) {
				UT_ASSERT(false); mx_publication_cancel(&plan); writer_clean(s, true); continue;
			}
			UT_ASSERT_EQ(publication_apply(&plan), 0); transition_write(false);
			UT_ASSERT(mx_history_transition_finish(1, true, 129));
			if (fault == 1) {
				descriptor.bufferdesc.tag.blockNum++;
				candidate.t_self.ip_blkid.bi_lo++;
				UT_ASSERT_EQ(writer_match(&candidate, &out.successor, true), 0);
				descriptor.bufferdesc.tag.blockNum--;
			} else {
				mx_history_recursive_close(); mx_history_recursive_open(s);
				UT_ASSERT_EQ(writer_match(&candidate, &out.successor, true), 0);
				mx_history_recursive_close(); auth_clean(s);
				UT_ASSERT_EQ(caller_enters, 2); UT_ASSERT_EQ(caller_leaves, 2);
				continue;
			}
		}
		UT_ASSERT_EQ(publication_cancels, 0); writer_clean(s, true);
	}
}
UT_TEST(transition_RF_delta_is_planned_and_completion_cannot_be_replayed)
{
	MxHeapReadout out; MxPublicationReadout plan;
	Snapshot s = publication_setup(&out); HeapTupleData candidate = hot_candidate();
	if (!publication_prepare(&plan) || !transition_plan(&out, &plan, true)) {
		UT_ASSERT(false); mx_publication_cancel(&plan); writer_clean(s, true); return;
	}
	UT_ASSERT_EQ(publication_apply(&plan), 0); transition_write(true);
	UT_ASSERT(mx_history_transition_finish(1, true, 129));
	UT_ASSERT_EQ(writer_match(&candidate, &out.successor, true), 1);
	UT_ASSERT(!mx_history_transition_finish(1, true, 129));
	UT_ASSERT_EQ(writer_match(&candidate, &out.successor, true), 0);
	UT_ASSERT_EQ(finish_reads, 3); UT_ASSERT_EQ(publication_cancels, 0);
	printf("# local transition layout=%zu; no native two-hop/SQL claim\n", mx_history_transition_bytes());
	writer_clean(s, true);
}
/* Native PageSetLSN has an origin side effect absent from raw LSN fixtures. */
bool cluster_recmerge_window_active;
uint64 cluster_recmerge_window_scn, cluster_recmerge_window_own_lsn;
bool cluster_recmerge_apply_foreign;
UT_TEST(transition_native_WAL_origin_is_part_of_the_pre_APPLY_delta)
{
	MxHeapReadout out; MxPublicationReadout plan;
	Snapshot s = publication_setup(&out); HeapTupleData candidate = hot_candidate();
	if (!publication_prepare(&plan) || !transition_plan(&out, &plan, false)) {
		UT_ASSERT(false); writer_clean(s, true); return;
	}
	UT_ASSERT_EQ(publication_apply(&plan), 0); transition_write(false);
	PageSetLSN(BufferBlocks, UINT64CONST(129));
	UT_ASSERT(mx_history_transition_finish(1, true, 129));
	UT_ASSERT_EQ(writer_match(&candidate, &out.successor, true), 1);
	writer_clean(s, true);
}
UT_TEST(transition_foreign_WAL_coordinate_does_not_order_the_local_return)
{
	MxHeapReadout out; MxPublicationReadout plan;
	Snapshot s = publication_setup(&out); HeapTupleData candidate = hot_candidate();
	/* Independently requalify B before planning; this is input capture, not
	 * post-publication proof rebasing. Foreign LSN1000 precedes local129. */
	PageXLogRecPtrSet(((PageHeader)BufferBlocks)->pd_lsn, UINT64CONST(1000));
	UT_ASSERT(PageSetLSNOrigin(BufferBlocks, 0));
	UT_ASSERT_EQ(writer_scoped_call(&out), 2);
	if (!publication_prepare(&plan) || !transition_plan(&out, &plan, false)) {
		UT_ASSERT(false); writer_clean(s, true); return;
	}
	UT_ASSERT_EQ(publication_apply(&plan), 0); transition_write(false);
	PageSetLSN(BufferBlocks, UINT64CONST(129));
	UT_ASSERT(mx_history_transition_finish(1, true, 129));
	UT_ASSERT_EQ(writer_match(&candidate, &out.successor, true), 1);
	writer_clean(s, true);
}
UT_TEST(transition_extra_WAL_origin_cannot_be_laundered_at_completion)
{
	MxHeapReadout out; MxPublicationReadout plan;
	Snapshot s = publication_setup(&out); HeapTupleData candidate = hot_candidate();
	if (!publication_prepare(&plan) || !transition_plan(&out, &plan, false)) {
		UT_ASSERT(false); writer_clean(s, true); return;
	}
	UT_ASSERT_EQ(publication_apply(&plan), 0); transition_write(false);
	PageSetLSN(BufferBlocks, UINT64CONST(129));
	UT_ASSERT(PageSetLSNOrigin(BufferBlocks, 2));
	UT_ASSERT(!mx_history_transition_finish(1, true, 129));
	UT_ASSERT_EQ(writer_match(&candidate, &out.successor, true), 0);
	writer_clean(s, true);
}
UT_TEST(transition_WAL_return_requires_a_valid_coordinate_and_same_owner)
{
	const XLogRecPtr returned[] = {128, 127, 0, 129, 129};
	for (unsigned fault = 0; fault < lengthof(returned); fault++) {
		MxHeapReadout out; MxPublicationReadout plan;
		Snapshot s = publication_setup(&out); HeapTupleData candidate = hot_candidate();
		UT_ASSERT(PageSetLSNOrigin(BufferBlocks, 1));
		PageXLogRecPtrSet(((PageHeader)BufferBlocks)->pd_lsn, UINT64CONST(128));
		if (fault == 3) {
			PageClearLSNOrigin(BufferBlocks);
			PageXLogRecPtrSet(((PageHeader)BufferBlocks)->pd_lsn, UINT64CONST(1000));
		}
		UT_ASSERT_EQ(writer_scoped_call(&out), 2);
		if (!publication_prepare(&plan) || !transition_plan(&out, &plan, false)) {
			UT_ASSERT(false); writer_clean(s, true); continue;
		}
		UT_ASSERT_EQ(publication_apply(&plan), 0); transition_write(false);
		PageSetLSN(BufferBlocks, returned[fault]);
		if (fault == 4) cluster_node_id = 2;
		UT_ASSERT(!mx_history_transition_finish(1, true, returned[fault]));
		cluster_node_id = 1;
		UT_ASSERT_EQ(writer_match(&candidate, &out.successor, true), 0);
		writer_clean(s, true);
	}
}
int main(void)
{
	history_fixture_init(); UT_PLAN(14);
	UT_RUN(child_A_receipts_and_B_remain_independent_through_APPLY);
	UT_RUN(zero_APPLY_refusal_preserves_unpublished_successor);
	UT_RUN(child_same_page_successor_progress_after_own_publication);
	UT_RUN(unapplied_same_page_change_cannot_authorize_old_B);
	UT_RUN(transition_pre_APPLY_plan_and_receipt_bindings_cannot_drift);
	UT_RUN(transition_no_or_partial_APPLY_has_no_continuation);
	UT_RUN(transition_exact_page_rejects_extra_changes_before_and_after_finish);
	UT_RUN(transition_live_scope_owner_grant_and_B_are_required);
	UT_RUN(transition_late_plan_cross_page_and_reopened_scope_refuse);
	UT_RUN(transition_RF_delta_is_planned_and_completion_cannot_be_replayed);
	UT_RUN(transition_native_WAL_origin_is_part_of_the_pre_APPLY_delta);
	UT_RUN(transition_foreign_WAL_coordinate_does_not_order_the_local_return);
	UT_RUN(transition_extra_WAL_origin_cannot_be_laundered_at_completion);
	UT_RUN(transition_WAL_return_requires_a_valid_coordinate_and_same_owner);
	free(procArray); UT_DONE(); return ut_failed_count ? 1 : 0;
}
