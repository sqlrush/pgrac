/* Real bounded heap consumer; metadata/proof and backend leaves controlled.
 * No full-width proof, receipt publication or native admission is implied. */
int k_heap_component_main(void);
#define main k_heap_component_main
#define cluster_bufmgr_pcm_own_snapshot k_component_pcm_snapshot
#define cluster_multixact_current_members_resolve_until k_component_resolve_until
#define cluster_multixact_current_decide_observed k_component_decide_observed
#define errcode k_component_errcode
/* This specialized consumer supplies its own scoped descriptor below. */
#define K_G5_SCOPED_PROVIDER 1
#include "test_cluster_r4_lock_order.c"
#undef K_G5_SCOPED_PROVIDER
#undef errcode
#undef cluster_multixact_current_decide_observed
#undef cluster_multixact_current_members_resolve_until
#undef cluster_bufmgr_pcm_own_snapshot
#undef main
#include "cluster/cluster_multixact_range.h"

SnapshotData SnapshotAnyData = { .snapshot_type = SNAPSHOT_ANY };

extern int errcode(int code);
extern ClusterPcmOwnResult cluster_bufmgr_pcm_own_snapshot(BufferDesc *, ClusterPcmOwnSnapshot *);
extern ClusterMxResolveResult cluster_multixact_current_members_resolve_until(
	const ClusterCurrentMxKey *, const ClusterCurrentMxMemberDesc *, uint16, uint64,
	const ClusterCurrentUpdaterChallenge *, ClusterCurrentMemberProof *,
	ClusterCurrentUpdaterProof *, uint32 *, TimestampTz *);
extern ClusterCurrentMxDecision cluster_multixact_current_decide_observed(
	const ClusterCurrentMxMemberDesc *, const ClusterCurrentMemberProof *, uint16,
	const ClusterCurrentMxRequestContext *, const ClusterCurrentUpdaterChallenge *,
	const ClusterCurrentUpdaterProof *, ClusterTTStatusKey *, ClusterCurrentMxDecisionTrace *);

typedef enum KHeapDrift
{
	K_SAME = 0, K_LSN, K_TID, K_LINE_POINTER, K_TUPLE,
	K_ITL_FIRST, K_ITL_LAST, K_PCM_TAG, K_PCM_GENERATION,
	K_PCM_RESERVATION, K_PCM_RESOURCE_X, K_PCM_FLAGS, K_PCM_STATE,
	K_PCM_UNAVAILABLE, K_DIAGNOSTIC_ROTATION
} KHeapDrift;

static UtR4HotProductFixture k_fixture;
static HeapHotSearchResult k_hot;
static RelationData k_relation;
static FormData_pg_class k_form;
static HeapTupleData k_tuple;
/* These outputs are also inspected after ERROR/cancel longjmp. */
static Buffer k_map;
static TM_Result k_result;
static uint16 k_count;
static int k_disposition;
static int k_error_code;
static unsigned k_stage, k_fault_stage, k_decisions, k_bad_descriptor;
static bool k_unknown_resolver;
static KHeapDrift k_drift;

int
errcode(int code)
{
	k_error_code = code;
	return k_component_errcode(code);
}

static bool
k_fault_active(void)
{
	return k_fault_stage != 0 && k_stage >= k_fault_stage;
}

static void
k_change_heap(void)
{
	Page page = (Page)k_fixture.live_page;
	ItemId lp = PageGetItemId(page, UT_HOT_ROOT_OFF);

	if (!k_fault_active()) return;
	switch (k_drift)
	{
		case K_LSN: PageSetLSN(page, UINT64_C(0x998877)); break;
		case K_TID: ItemPointerSet(&k_tuple.t_self, UT_HOT_BLOCK, UT_HOT_ROOT_OFF + 1); break;
		case K_LINE_POINTER: lp->lp_len--; break;
		case K_TUPLE: k_tuple.t_data->t_infomask ^= HEAP_XMIN_COMMITTED; break;
		case K_ITL_FIRST: ClusterPageGetItlSlots(page)[0].xid++; break;
		case K_ITL_LAST: ClusterPageGetItlSlots(page)[CLUSTER_ITL_INITRANS_DEFAULT - 1].xid++; break;
		default: break;
	}
}

ClusterPcmOwnResult
cluster_bufmgr_pcm_own_snapshot(BufferDesc *buf, ClusterPcmOwnSnapshot *out)
{
	ClusterPcmOwnResult result = k_component_pcm_snapshot(buf, out);

	if (!k_fault_active()) return result;
	switch (k_drift)
	{
		case K_PCM_TAG: out->tag.blockNum++; break;
		case K_PCM_GENERATION: out->generation++; break;
		case K_PCM_RESERVATION: out->reservation_token++; break;
		case K_PCM_RESOURCE_X: out->resource_x_activation_generation++; break;
		case K_PCM_FLAGS: out->flags = PCM_OWN_FLAG_REVOKING; break;
		case K_PCM_STATE: out->pcm_state = (uint8)PCM_STATE_S; break;
		case K_PCM_UNAVAILABLE: return CLUSTER_PCM_OWN_STALE;
		case K_DIAGNOSTIC_ROTATION: out->writer_activation_token = 99; break;
		default: break;
	}
	return result;
}

ClusterMxDescribeResult
cluster_multixact_current_describe_scoped(const ClusterCurrentMxKey *key,
	const BufferTag *target, Snapshot snapshot, ClusterCurrentMxMemberDesc *members,
	uint16 cap, uint16 *count, uint32 *total)
{
	ClusterMxDescribeResult result;

	UT_ASSERT(BufferTagsEqual(target, &k_fixture.expected_tag));
	UT_ASSERT(snapshot == SnapshotAny); /* Existing descriptor metadata contract. */
	result = cluster_multixact_current_describe(key, members, cap, count, total);
	k_stage = 1;
	k_change_heap();
	if (k_bad_descriptor == 1) return CMX_DESC_UNKNOWN;
	if (k_bad_descriptor == 2) *total = 2;
	if (k_bad_descriptor == 3) { *count = 0; *total = 257; return CMX_DESC_SUPPORTED_LIMIT; }
	return result;
}

ClusterMxResolveResult
cluster_multixact_current_members_resolve_until(
	const ClusterCurrentMxKey *key, const ClusterCurrentMxMemberDesc *members,
	uint16 count, uint64 hash, const ClusterCurrentUpdaterChallenge *challenge,
	ClusterCurrentMemberProof *proofs, ClusterCurrentUpdaterProof *updater,
	uint32 *generations, TimestampTz *deadline)
{
	ClusterMxResolveResult result = k_component_resolve_until(key, members, count,
		hash, challenge, proofs, updater, generations, deadline);

	k_stage = 2;
	k_change_heap();
	return k_unknown_resolver ? CMX_RESOLVE_UNKNOWN : result;
}

ClusterCurrentMxDecision
cluster_multixact_current_decide_observed(
	const ClusterCurrentMxMemberDesc *members, const ClusterCurrentMemberProof *proofs,
	uint16 count, const ClusterCurrentMxRequestContext *ctx,
	const ClusterCurrentUpdaterChallenge *challenge, const ClusterCurrentUpdaterProof *updater,
	ClusterTTStatusKey *wait_key, ClusterCurrentMxDecisionTrace *trace)
{
	k_decisions++;
	return k_component_decide_observed(members, proofs, count, ctx, challenge, updater, wait_key, trace);
}

static void
k_begin(unsigned stage, KHeapDrift drift)
{
	ut_itl_census_pcm_flags = 0;
	ut_itl_census_pcm_reservation_token = 31;
	ut_hot_current_mx_one_shot = false;
	ut_itl_census_second_round_drift = ut_itl_census_stale_first_round_full = false;
	ut_mx_vm_begin(&k_fixture, &k_hot, &k_relation, &k_form, &k_tuple, false);
	k_map = 2; k_result = TM_BeingModified; k_count = 0; k_disposition = -1;
	k_error_code = 0;
	k_stage = k_decisions = k_bad_descriptor = 0; k_unknown_resolver = false;
	k_fault_stage = stage; k_drift = drift;
}

static bool
k_authorize(void)
{
	sigjmp_buf error;
	volatile bool caught = false;

	PG_exception_stack = &error;
	if (sigsetjmp(error, 0) == 0)
		k_disposition = cluster_heap_test_current_mx_with_vm(&k_relation, 1, &k_tuple,
			UT_ORDINARY_REQUESTER, false, LockTupleKeyShare, LockWaitBlock,
			&k_map, &k_result, &k_count);
	else
		caught = true;
	PG_exception_stack = NULL;
	return caught;
}

static void
k_finish(void)
{
	UT_ASSERT_EQ(ut_mx_vm_service_pins, 0);
	UT_ASSERT_EQ(ut_itl_census_dirty_hint_calls, 0);
	ut_mx_vm_finish();
}

UT_TEST(heap_current_and_diagnostic_rotation_controls)
{
	for (unsigned stage = 1; stage <= 2; stage++)
		for (unsigned rotation = 0; rotation <= 1; rotation++)
		{
			k_begin(stage, rotation ? K_DIAGNOSTIC_ROTATION : K_SAME);
			UT_ASSERT(!k_authorize());
			UT_ASSERT_EQ(k_disposition, 2); UT_ASSERT_EQ(k_result, TM_Ok);
			UT_ASSERT_EQ(k_count, 2); UT_ASSERT_EQ(k_decisions, 1);
			UT_ASSERT_EQ(ut_mx_vm_recorded_restarts, 0);
			k_finish();
		}
}

static void
k_expect_window_restarts(unsigned stage)
{
	for (KHeapDrift drift = K_LSN; drift <= K_PCM_UNAVAILABLE; drift++)
	{
		k_begin(stage, drift);
		UT_ASSERT(!k_authorize());
		UT_ASSERT_EQ(k_disposition, 1); UT_ASSERT_EQ(k_count, 0);
		UT_ASSERT_EQ(k_result, TM_BeingModified); UT_ASSERT_EQ(k_decisions, 0);
		UT_ASSERT_EQ(ut_hot_current_mx_describe_calls, 1);
		UT_ASSERT_EQ(ut_hot_current_mx_resolve_calls, stage == 2 ? 1 : 0);
		UT_ASSERT_EQ(ut_mx_vm_recorded_restarts, 1);
		k_finish();
	}
}

UT_TEST(heap_restarts_after_describe_window_drift)
{
	k_expect_window_restarts(1);
}

UT_TEST(heap_restarts_after_proof_window_drift)
{
	k_expect_window_restarts(2);
}

UT_TEST(heap_bad_descriptor_never_enters_proof)
{
	for (unsigned kind = 1; kind <= 3; kind++)
	{
		k_begin(0, K_SAME); k_bad_descriptor = kind;
		UT_ASSERT(k_authorize());
		UT_ASSERT_EQ(k_stage, 1); UT_ASSERT_EQ(ut_hot_current_mx_describe_calls, 1);
		UT_ASSERT_EQ(k_error_code, kind == 3 ? ERRCODE_FEATURE_NOT_SUPPORTED
			: ERRCODE_CLUSTER_CROSS_NODE_WRITE_CONFLICT);
		UT_ASSERT_EQ(ut_hot_current_mx_resolve_calls, 0);
		UT_ASSERT_EQ(k_decisions, 0); UT_ASSERT_EQ(k_count, 0);
		k_finish();
	}
}

UT_TEST(heap_unknown_proof_never_enters_decision)
{
	k_begin(0, K_SAME); k_unknown_resolver = true;
	UT_ASSERT(k_authorize());
	UT_ASSERT_EQ(k_stage, 2); UT_ASSERT_EQ(ut_hot_current_mx_describe_calls, 1);
	UT_ASSERT_EQ(k_error_code, ERRCODE_CLUSTER_CROSS_NODE_WRITE_CONFLICT);
	UT_ASSERT_EQ(ut_hot_current_mx_resolve_calls, 1);
	UT_ASSERT_EQ(k_decisions, 0); UT_ASSERT_EQ(k_count, 0);
	k_finish();
}

int
main(void)
{
	UT_PLAN(5);
	UT_RUN(heap_current_and_diagnostic_rotation_controls);
	UT_RUN(heap_restarts_after_describe_window_drift);
	UT_RUN(heap_restarts_after_proof_window_drift);
	UT_RUN(heap_bad_descriptor_never_enters_proof);
	UT_RUN(heap_unknown_proof_never_enters_decision);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
