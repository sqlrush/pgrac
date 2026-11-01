/* Manually selected original-heap acceptance RED. The real heap authorizer
 * and decision/recompose bodies are linked; descriptor/proof service and
 * backend leaves are controlled. Literal terminal/ACTIVE proof shapes do not
 * establish native TT/grant/H authority, capacity reservation or publication.
 * No G2 provisional proof is promoted to production authority by this test. */
int k_heap_full_component_main(void);
#define main k_heap_full_component_main
#define cluster_bufmgr_pcm_own_snapshot k_heap_fixture_pcm_snapshot
#define cluster_multixact_current_decide k_heap_fixture_decide
#define cluster_multixact_current_decide_observed k_heap_fixture_decide_observed
#define cluster_multixact_current_describe k_heap_fixture_describe
#define cluster_multixact_current_descriptor_hash k_heap_fixture_descriptor_hash
#define cluster_multixact_current_members_resolve k_heap_fixture_members_resolve
#define cluster_multixact_current_members_resolve_until k_heap_fixture_members_resolve_until
#define cluster_multixact_current_recompose k_heap_fixture_recompose
#define cluster_multixact_current_status_conflicts k_heap_fixture_status_conflicts
#define cluster_multixact_current_successor_provenance_well_formed k_heap_fixture_successor_provenance_well_formed
#define cluster_multixact_current_validate_descriptor k_heap_fixture_validate_descriptor
#define cluster_multixact_current_validate_updater_proof k_heap_fixture_validate_updater_proof
#include "test_cluster_r4_lock_order.c"
#undef main
#undef cluster_bufmgr_pcm_own_snapshot
#include "cluster/cluster_multixact_range.h"
#undef cluster_multixact_current_decide
#undef cluster_multixact_current_decide_observed
#undef cluster_multixact_current_describe
#undef cluster_multixact_current_descriptor_hash
#undef cluster_multixact_current_members_resolve
#undef cluster_multixact_current_members_resolve_until
#undef cluster_multixact_current_recompose
#undef cluster_multixact_current_status_conflicts
#undef cluster_multixact_current_successor_provenance_well_formed
#undef cluster_multixact_current_validate_descriptor
#undef cluster_multixact_current_validate_updater_proof

/* Only the service leaf and observation wrappers are renamed in the product
 * object. The decision/recompose implementations themselves are unchanged. */
extern ClusterCurrentMxDecision k_heap_product_decide_observed(
	const ClusterCurrentMxMemberDesc *, const ClusterCurrentMemberProof *, uint16,
	const ClusterCurrentMxRequestContext *, const ClusterCurrentUpdaterChallenge *,
	const ClusterCurrentUpdaterProof *, ClusterTTStatusKey *, ClusterCurrentMxDecisionTrace *);
extern ClusterMxRecomposeResult k_heap_product_recompose(
	const ClusterCurrentMxMemberDesc *, const ClusterCurrentMemberProof *, uint16,
	TransactionId, MultiXactStatus, MultiXactMember *, uint16, uint16 *);

extern uint64 cluster_multixact_current_descriptor_hash(const ClusterCurrentMxKey *,
	const ClusterCurrentMxMemberDesc *, uint16);
extern ClusterMxResolveResult cluster_multixact_current_members_resolve_until(
	const ClusterCurrentMxKey *, const ClusterCurrentMxMemberDesc *, uint16, uint64,
	const ClusterCurrentUpdaterChallenge *, ClusterCurrentMemberProof *,
	ClusterCurrentUpdaterProof *, uint32 *, TimestampTz *);
extern ClusterCurrentMxDecision cluster_multixact_current_decide_observed(
	const ClusterCurrentMxMemberDesc *, const ClusterCurrentMemberProof *, uint16,
	const ClusterCurrentMxRequestContext *, const ClusterCurrentUpdaterChallenge *,
	const ClusterCurrentUpdaterProof *, ClusterTTStatusKey *, ClusterCurrentMxDecisionTrace *);
extern ClusterMxRecomposeResult cluster_multixact_current_recompose(
	const ClusterCurrentMxMemberDesc *, const ClusterCurrentMemberProof *, uint16,
	TransactionId, MultiXactStatus, MultiXactMember *, uint16, uint16 *);

SnapshotData SnapshotAnyData = { .snapshot_type = SNAPSHOT_ANY };

static UtR4HotProductFixture full_fixture;
static HeapHotSearchResult full_hot;
static RelationData full_relation;
static FormData_pg_class full_form;
static HeapTupleData full_tuple;
/* All inspected outputs survive the controlled backend ERROR longjmp. */
static Buffer full_vm;
static TM_Result full_result;
static uint16 full_count;
static int full_disposition;
static uint32 full_total, full_seen, full_unknown;
static unsigned full_stage, full_drift_stage, full_decisions, full_recomposes;
static bool full_active;

typedef enum FullDrift {
	FULL_SAME, FULL_LSN, FULL_TID, FULL_LP, FULL_TUPLE,
	FULL_ITL_FIRST, FULL_ITL_LAST, FULL_PCM_TAG, FULL_PCM_GENERATION,
	FULL_PCM_RESERVATION, FULL_PCM_RESOURCE_X, FULL_PCM_FLAGS, FULL_PCM_STATE,
	FULL_PCM_UNAVAILABLE
} FullDrift;
static FullDrift full_drift;

extern ClusterPcmOwnResult cluster_bufmgr_pcm_own_snapshot(BufferDesc *, ClusterPcmOwnSnapshot *);
ClusterPcmOwnResult
cluster_bufmgr_pcm_own_snapshot(BufferDesc *buf, ClusterPcmOwnSnapshot *out)
{
	ClusterPcmOwnResult result = k_heap_fixture_pcm_snapshot(buf, out);
	if (!full_drift_stage || full_stage != full_drift_stage) return result;
	switch (full_drift) {
	case FULL_PCM_TAG: out->tag.blockNum++; break;
	case FULL_PCM_GENERATION: out->generation++; break;
	case FULL_PCM_RESERVATION: out->reservation_token++; break;
	case FULL_PCM_RESOURCE_X: out->resource_x_activation_generation++; break;
	case FULL_PCM_FLAGS: out->flags = PCM_OWN_FLAG_REVOKING; break;
	case FULL_PCM_STATE: out->pcm_state = (uint8)PCM_STATE_S; break;
	case FULL_PCM_UNAVAILABLE: return CLUSTER_PCM_OWN_STALE;
	default: break;
	}
	return result;
}

static void
full_change_heap(void)
{
	Page page = (Page)full_fixture.live_page;
	if (!full_drift_stage || full_stage != full_drift_stage) return;
	switch (full_drift) {
	case FULL_LSN: PageSetLSN(page, UINT64_C(0x998877)); break;
	case FULL_TID: ItemPointerSet(&full_tuple.t_self, UT_HOT_BLOCK, UT_HOT_ROOT_OFF + 1); break;
	case FULL_LP: PageGetItemId(page, UT_HOT_ROOT_OFF)->lp_len--; break;
	case FULL_TUPLE: full_tuple.t_data->t_infomask ^= HEAP_XMIN_COMMITTED; break;
	case FULL_ITL_FIRST: ClusterPageGetItlSlots(page)[0].xid++; break;
	case FULL_ITL_LAST: ClusterPageGetItlSlots(page)[CLUSTER_ITL_INITRANS_DEFAULT - 1].xid++; break;
	default: break;
	}
}

ClusterMxDescribeResult
cluster_multixact_current_describe_scoped(const ClusterCurrentMxKey *key,
	const BufferTag *target, Snapshot snapshot, ClusterCurrentMxMemberDesc *members,
	uint16 cap, uint16 *count, uint32 *total)
{
	UT_ASSERT(!ut_hot_content_lock_held);
	UT_ASSERT(BufferTagsEqual(target, &full_fixture.expected_tag));
	UT_ASSERT(snapshot == SnapshotAny);
	UT_ASSERT_EQ(key->origin_node_id, UT_HOT_CURRENT_MX_ORIGIN);
	UT_ASSERT_EQ(key->multixact_id, UT_HOT_FOREIGN_MXID);
	UT_ASSERT_EQ(key->cluster_epoch, 9);
	ut_hot_current_mx_describe_calls++;
	ut_mx_vm_service();
	full_stage = 1;
	full_change_heap();
	*count = 0; *total = full_total;
	/* Report the complete uint32 total, never a prefix or narrowed count.
	 * The original bounded entry cannot deliver a full-domain descriptor;
	 * an eventual full reader must leave this adapter bounded and bypass it. */
	if (full_total > cap) return CMX_DESC_SUPPORTED_LIMIT;
	memset(members, 0, (Size)cap * sizeof(*members));
	for (uint32 i = 0; i < full_total; i++) {
		members[i].xid = 10000 + i;
		members[i].member_status = MultiXactStatusForShare;
	}
	*count = (uint16)full_total;
	return CMX_DESC_OK;
}

ClusterMxResolveResult
cluster_multixact_current_members_resolve_until(const ClusterCurrentMxKey *key,
	const ClusterCurrentMxMemberDesc *members, uint16 count, uint64 hash,
	const ClusterCurrentUpdaterChallenge *challenge, ClusterCurrentMemberProof *proofs,
	ClusterCurrentUpdaterProof *updater, uint32 *generations, TimestampTz *deadline)
{
	UT_ASSERT(!ut_hot_content_lock_held);
	UT_ASSERT_NULL(challenge);
	UT_ASSERT_EQ(hash, cluster_multixact_current_descriptor_hash(key, members, count));
	UT_ASSERT_EQ(count, full_total);
	ut_hot_current_mx_resolve_calls++;
	ut_mx_vm_service();
	memset(proofs, 0, (Size)count * sizeof(*proofs));
	memset(updater, 0, sizeof(*updater));
	*deadline = 1;
	for (uint32 i = 0; i < count; i++) {
		ClusterCurrentMemberProof *q = &proofs[i];
		q->member_xid = members[i].xid; q->member_status = members[i].member_status;
		q->member_ordinal = i; q->state = i == full_unknown ? CCM_UNKNOWN : CCM_ABORTED;
		if (full_active) {
			q->state = CCM_ACTIVE;
			q->key.origin_node_id = 1; q->key.undo_segment_id = 1;
			q->key.tt_slot_id = 1; q->key.cluster_epoch = 9; q->key.local_xid = q->member_xid;
			ClusterCurrentMemberProofSetCtrcBinding(q, 17, 3);
			ClusterCurrentMemberProofSetCtrcGrant(q, 7);
			/* Structural V2 header only: no live grant authority is claimed. */
			q->origin_fullxid_v2[0] = CLUSTER_CTRC_ORIGIN_FULLXID_V2_VERSION;
			q->origin_fullxid_v2[2] = CLUSTER_CTRC_ORIGIN_FULLXID_V2_BYTES;
		}
		generations[i] = 41;
	}
	full_seen = count; full_stage = 2;
	full_change_heap();
	return CMX_RESOLVE_OK;
}

ClusterCurrentMxDecision
cluster_multixact_current_decide_observed(const ClusterCurrentMxMemberDesc *members,
	const ClusterCurrentMemberProof *proofs, uint16 count,
	const ClusterCurrentMxRequestContext *ctx, const ClusterCurrentUpdaterChallenge *challenge,
	const ClusterCurrentUpdaterProof *updater, ClusterTTStatusKey *wait,
	ClusterCurrentMxDecisionTrace *trace)
{
	UT_ASSERT(ut_hot_content_lock_held);
	full_decisions++;
	return k_heap_product_decide_observed(members, proofs, count, ctx, challenge, updater, wait, trace);
}

ClusterMxRecomposeResult
cluster_multixact_current_recompose(const ClusterCurrentMxMemberDesc *members,
	const ClusterCurrentMemberProof *proofs, uint16 count, TransactionId requester,
	MultiXactStatus status, MultiXactMember *out, uint16 cap, uint16 *nout)
{
	full_recomposes++;
	return k_heap_product_recompose(members, proofs, count, requester, status, out, cap, nout);
}

static void
full_begin(uint32 total)
{
	ut_itl_census_pcm_flags = 0;
	ut_itl_census_pcm_reservation_token = 31;
	ut_hot_current_mx_one_shot = false;
	ut_itl_census_second_round_drift = ut_itl_census_stale_first_round_full = false;
	ut_mx_vm_begin(&full_fixture, &full_hot, &full_relation, &full_form, &full_tuple, false);
	full_total = total; full_unknown = UINT32_MAX;
	full_seen = full_stage = full_drift_stage = full_decisions = full_recomposes = 0;
	full_active = false; full_drift = FULL_SAME;
	full_vm = 2; full_result = TM_BeingModified; full_count = 0; full_disposition = -1;
}

static bool
full_authorize(void)
{
	sigjmp_buf error;
	volatile bool caught = false;
	PG_exception_stack = &error;
	if (sigsetjmp(error, 0) == 0)
		full_disposition = cluster_heap_test_current_mx_with_vm(&full_relation, 1, &full_tuple,
			UT_ORDINARY_REQUESTER, false, LockTupleKeyShare, LockWaitBlock,
			&full_vm, &full_result, &full_count);
	else caught = true;
	PG_exception_stack = NULL;
	return caught;
}

static void
full_end(void)
{
	UT_ASSERT_EQ(ut_mx_vm_service_pins, 0);
	UT_ASSERT_EQ(ut_itl_census_dirty_hint_calls, 0);
	printf("# N=%u stage=%u resolved=%u decide=%u recompose=%u result=%d count=%u restarts=%u\n",
		full_total, full_stage, full_seen, full_decisions, full_recomposes,
		full_result, full_count, ut_mx_vm_recorded_restarts);
	ut_mx_vm_finish();
}

static void
full_expect_terminal(uint32 total)
{
	full_begin(total);
	UT_ASSERT(!full_authorize());
	UT_ASSERT_EQ(full_disposition, 2); UT_ASSERT_EQ(full_result, TM_Ok);
	UT_ASSERT_EQ(full_count, 1); UT_ASSERT_EQ(full_seen, total);
	UT_ASSERT_EQ(full_decisions, 1); UT_ASSERT_EQ(full_recomposes, 1);
	full_end();
}

UT_TEST(heap_terminal_two_and_256_controls)
{
	full_expect_terminal(2); full_expect_terminal(256);
}
UT_TEST(heap_full_257_terminal_members_reach_original_decision)
{
	full_expect_terminal(257);
}
UT_TEST(heap_full_65537_terminal_members_reach_original_decision)
{
	full_expect_terminal(65537);
}
UT_TEST(heap_unknown_tail_256_never_reaches_normalization)
{
	full_begin(256); full_unknown = 255;
	UT_ASSERT(full_authorize());
	UT_ASSERT_EQ(full_seen, 256); UT_ASSERT_EQ(full_decisions, 1);
	UT_ASSERT_EQ(full_recomposes, 0); UT_ASSERT_EQ(full_count, 0);
	UT_ASSERT_EQ(full_result, TM_BeingModified);
	full_end();
}
UT_TEST(heap_full_unknown_tail_is_checked_after_the_complete_descriptor)
{
	const uint32 sizes[] = {257, 65537};
	for (unsigned i = 0; i < lengthof(sizes); i++) {
		full_begin(sizes[i]); full_unknown = sizes[i] - 1;
		UT_ASSERT(full_authorize());
		/* A bounded feature-limit rejection is not evidence for this gate. */
		UT_ASSERT_EQ(full_seen, sizes[i]); UT_ASSERT_EQ(full_decisions, 1);
		UT_ASSERT_EQ(full_recomposes, 0); UT_ASSERT_EQ(full_count, 0);
		UT_ASSERT_EQ(full_result, TM_BeingModified);
		full_end();
	}
}

static void
full_expect_drift(uint32 total, unsigned stage)
{
	for (FullDrift drift = FULL_LSN; drift <= FULL_PCM_UNAVAILABLE; drift++) {
		full_begin(total); full_drift_stage = stage; full_drift = drift;
		UT_ASSERT(!full_authorize());
		UT_ASSERT_EQ(full_stage, stage);
		UT_ASSERT_EQ(full_disposition, 1); UT_ASSERT_EQ(full_result, TM_BeingModified);
		UT_ASSERT_EQ(full_count, 0); UT_ASSERT_EQ(full_decisions, 0);
		UT_ASSERT_EQ(full_recomposes, 0); UT_ASSERT_EQ(ut_mx_vm_recorded_restarts, 1);
		UT_ASSERT_EQ(ut_hot_current_mx_resolve_calls, stage == 2 ? 1 : 0);
		full_end();
	}
}
UT_TEST(heap_256_rechecks_fingerprints_after_descriptor)
{
	full_expect_drift(256, 1);
}
UT_TEST(heap_256_rechecks_fingerprints_after_proofs)
{
	full_expect_drift(256, 2);
}
UT_TEST(heap_full_rechecks_fingerprints_after_the_tail_proof)
{
	full_expect_drift(257, 2);
}
UT_TEST(heap_255_retained_plus_requester_control)
{
	full_begin(255); full_active = true;
	UT_ASSERT(!full_authorize());
	UT_ASSERT_EQ(full_seen, 255); UT_ASSERT_EQ(full_decisions, 1);
	UT_ASSERT_EQ(full_recomposes, 1); UT_ASSERT_EQ(full_count, 256);
	UT_ASSERT_EQ(full_result, TM_Ok);
	full_end();
}
UT_TEST(heap_256_retained_plus_requester_is_not_a_total_limit)
{
	full_begin(256); full_active = true;
	UT_ASSERT(!full_authorize());
	UT_ASSERT_EQ(full_seen, 256); UT_ASSERT_EQ(full_decisions, 1);
	UT_ASSERT_EQ(full_recomposes, 1); UT_ASSERT_EQ(full_count, 257);
	UT_ASSERT_EQ(full_result, TM_Ok);
	full_end();
}

int
main(void)
{
	UT_PLAN(10);
	UT_RUN(heap_terminal_two_and_256_controls);
	UT_RUN(heap_full_257_terminal_members_reach_original_decision);
	UT_RUN(heap_full_65537_terminal_members_reach_original_decision);
	UT_RUN(heap_unknown_tail_256_never_reaches_normalization);
	UT_RUN(heap_full_unknown_tail_is_checked_after_the_complete_descriptor);
	UT_RUN(heap_256_rechecks_fingerprints_after_descriptor);
	UT_RUN(heap_256_rechecks_fingerprints_after_proofs);
	UT_RUN(heap_full_rechecks_fingerprints_after_the_tail_proof);
	UT_RUN(heap_255_retained_plus_requester_control);
	UT_RUN(heap_256_retained_plus_requester_is_not_a_total_limit);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
