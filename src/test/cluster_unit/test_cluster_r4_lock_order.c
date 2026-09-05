/*-------------------------------------------------------------------------
 *
 * test_cluster_r4_lock_order.c
 *	  Control-plane wait-for and held-lock policy tests for R4 D13.
 *
 *-------------------------------------------------------------------------
 */
#define USE_CLUSTER_UNIT 1

#include "postgres.h"

#include "access/heapam.h"
#include "access/heaptoast.h"
#include "access/htup_details.h"
#include "access/multixact.h"
#include "access/xact.h"
#include "access/tableam.h"
#include "catalog/pg_class.h"
#include "cluster/cluster_cr.h"
#include "cluster/cluster_cr_server.h"
#include "cluster/cluster_epoch.h"
#include "cluster/cluster_itl.h"
#include "cluster/cluster_itl_slot.h"
#include "cluster/cluster_multixact_current.h"
#include "cluster/cluster_multixact_current_stats.h"
#include "cluster/cluster_mxid_stripe.h"
#include "cluster/cluster_pcm_x_bufmgr.h"
#include "cluster/cluster_semantic_activation.h"
#include "cluster/cluster_terminal_ref_census.h"
#include "cluster/cluster_tx_enqueue.h"
#include "cluster/cluster_tx_resolve.h"
#include "cluster/cluster_undo_record_api.h"
#include "cluster/cluster_uba.h"
#include "cluster/cluster_visibility_resolve.h"
#include "cluster/cluster_xid_stripe.h"
#include "common/hashfn.h"
#include "executor/tuptable.h"
#include "storage/buf_internals.h"
#include "storage/bufmgr.h"
#include "storage/bufpage.h"
#include "storage/shmem.h"
#include "utils/datum.h"
#include "utils/expandeddatum.h"
#include "utils/hsearch.h"
#include "utils/memutils.h"
#include "utils/rel.h"
#include "utils/snapmgr.h"

#include "cluster_r4_activation_test_stubs.h"
#include "../../backend/access/heap/heapam_r4_private.h"

void *
ShmemInitStruct(const char *name pg_attribute_unused(), Size size pg_attribute_unused(),
				bool *foundPtr pg_attribute_unused())
{
	return NULL;
}

/* Exercise the real product-local policy helpers without exporting a test API. */
#include "../../backend/cluster/cluster_semantic_activation.c"

#undef printf
#undef fprintf
#undef snprintf

#include "unit_test.h"

UT_DEFINE_GLOBALS();

int
errdetail_internal(const char *fmt pg_attribute_unused(), ...)
{
	return 0;
}

/* Minimal backend boundary for the real executor/heaptuple objects. */
static char ut_memory_context_storage;
MemoryContext CurrentMemoryContext = (MemoryContext)&ut_memory_context_storage;
MemoryContext TopMemoryContext = (MemoryContext)&ut_memory_context_storage;
int NBuffers = 2;
int NLocBuffer = 0;
char *BufferBlocks = NULL;
Block *LocalBufferBlockPointers = NULL;
static BufferDescPadded ut_buffer_descriptors[2];
BufferDescPadded *BufferDescriptors = ut_buffer_descriptors;
TransactionId RecentXmin = FirstNormalTransactionId;
int XactIsoLevel = XACT_READ_COMMITTED;
bool cluster_enabled = true;
int cluster_node_id = 0;
int cluster_ges_request_timeout_ms = 1000;

TransactionId
GetTopTransactionId(void)
{
	return (TransactionId) 905;
}

CommandId
GetCurrentCommandId(bool used)
{
	UT_ASSERT(!used);
	return (CommandId) 7;
}

CommandId
HeapTupleHeaderGetCmax(HeapTupleHeader tup pg_attribute_unused())
{
	UT_ASSERT(false);
	return InvalidCommandId;
}

ClusterTxwResult
cluster_tx_enqueue_wait_current_mx(
	const ClusterTTStatusKey *holder_key pg_attribute_unused(),
	int effective_timeout_ms pg_attribute_unused(),
	uint64 *absolute_deadline_mono_us pg_attribute_unused())
{
	UT_ASSERT(false);
	return CLUSTER_TXW_UNPROVABLE;
}

/* implementation (contract §C): cluster_semantic_activation.c now consults the
 * runtime census at the latch apply; this binary does not link
 * cluster_wal_state.o.  GREEN stub — the RED refusal path is covered in
 * test_cluster_r4_activation_fsm test_130. */
bool
cluster_wal_state_correctness_census_ok(void)
{
	return true;
}

/* Unrelated R4 readiness dependencies are closed in this lock-order binary;
 * the focused heap/ITL fixtures activate their own exact admission state. */
uint32
cluster_grd_recovery_state_value(void)
{
	return 0;
}

bool
cluster_reconfig_snapshot_initial_clean_formation(
	ClusterInitialCleanFormationSnapshot *out)
{
	if (out != NULL)
		memset(out, 0, sizeof(*out));
	return false;
}

bool
cluster_pcm_lock_resource_x_gate_snapshot(
	ResourceXGateSnapshot *snapshot_out)
{
	if (snapshot_out != NULL)
		memset(snapshot_out, 0, sizeof(*snapshot_out));
	return false;
}

bool
cluster_pcm_lock_resource_x_cutover_gate_snapshot_exact(
	ResourceXGateSnapshot *snapshot_out)
{
	if (snapshot_out != NULL)
		memset(snapshot_out, 0, sizeof(*snapshot_out));
	return false;
}

bool
cluster_pcm_lock_resource_x_cutover_current_proof_digest_exact(
	bool thawed pg_attribute_unused(), ResourceXReconfigToken *token_out,
	uint64 *digest_out)
{
	if (token_out != NULL)
		memset(token_out, 0, sizeof(*token_out));
	if (digest_out != NULL)
		*digest_out = 0;
	return false;
}

bool cluster_recmerge_window_active = false;
uint64 cluster_recmerge_window_scn = 0;
uint64 cluster_recmerge_window_own_lsn = 0;
bool cluster_recmerge_apply_foreign = false;
static ClusterConf ut_cluster_conf;
ClusterConf *ClusterConfShmem = &ut_cluster_conf;
sigjmp_buf *PG_exception_stack = NULL;
ErrorContextCallback *error_context_stack = NULL;

int
cluster_conf_node_count(void)
{
	return ClusterConfShmem == NULL ? 0 : ClusterConfShmem->node_count;
}

void
pg_re_throw(void)
{
	if (PG_exception_stack != NULL)
		siglongjmp(*PG_exception_stack, 1);
	abort();
}

bool
ItemPointerEquals(ItemPointer pointer1, ItemPointer pointer2)
{
	return ItemPointerGetBlockNumber(pointer1)
			   == ItemPointerGetBlockNumber(pointer2)
		&& ItemPointerGetOffsetNumber(pointer1)
			   == ItemPointerGetOffsetNumber(pointer2);
}

static int ut_alloc_calls;
static int ut_free_calls;
static int ut_invalid_free_calls;
static int ut_buffer_incr_calls;
static int ut_buffer_release_calls;
static void *ut_allocations[16];

/*
 * Authority-boundary fixture for the real scratch-only MVCC evaluator.
 * The product evaluator owns the visibility policy; these stubs provide only
 * the already-approved exact ITL ref and typed origin verdict.
 */
static Page ut_scratch_expected_page;
static Page ut_scratch_forbidden_live_page;
static ClusterUndoTTSlotRef ut_scratch_expected_ref;
static ClusterVisEvidence ut_scratch_resolve_evidence;
static ClusterTTStatus ut_scratch_resolve_status;
static bool ut_writer_bridge_fixture;
static int ut_writer_bridge_mutation;
static int ut_writer_bridge_tuple_pulls;
static SCN ut_scratch_resolve_scn;
static SCN ut_scratch_expected_read_scn;
static XLogRecPtr ut_scratch_expected_lsn;
static TransactionId ut_scratch_expected_xid;
static bool ut_scratch_ref_available;
static int ut_scratch_ref_calls;
static int ut_scratch_exact_resolve_calls;
static int ut_scratch_live_resolve_calls;
static int ut_scratch_cr_calls;
static int ut_scratch_ssi_calls;
static int ut_scratch_hint_calls;
static int ut_scratch_dirty_calls;
static int ut_live_visibility_calls;
static OffsetNumber ut_live_visible_offnum;
static int ut_native_multixact_decode_calls;
static TransactionId ut_native_multixact_updater;
static Page ut_hot_live_ref_page;
static ClusterUndoTTSlotRef ut_hot_live_ref;
static int ut_hot_live_ref_calls;
static bool ut_hot_content_lock_held;
static bool ut_hot_production_core_active;
static bool ut_hot_r4_target_reachable;
static bool ut_hot_current_mx_active;
static uint8 ut_hot_current_mx_pcm_state = (uint8) PCM_STATE_X;
static bool ut_hot_current_mx_one_shot;
static uint64 ut_hot_current_read_bracket;
static int ut_hot_current_read_acquire_calls;
static int ut_hot_current_read_clear_calls;
static uint16 ut_hot_current_mx_member_count = 2;
static bool ut_current_mx_ordinary_lock_only;
static ClusterUndoTTSlotRef ut_hot_successor_ref;
static int ut_hot_pcm_snapshot_calls;
static uint8 ut_hot_last_pcm_snapshot_state;
static bool ut_itl_census_force_pcm_n;
static bool ut_itl_census_change_writer_activation_projection;
static bool ut_itl_census_replace_current_page;
static uint64 ut_itl_census_pcm_reservation_token;
static uint32 ut_itl_census_pcm_flags;
static bool ut_itl_census_stale_first_round_full;
static bool ut_itl_census_second_round_drift;
static bool ut_itl_census_second_round_fresh_locator_seen;
static bool ut_itl_census_mutate_second_terminal_after_full_resolve;
static bool ut_itl_census_second_terminal_mutated;
static bool ut_itl_census_consume_allocated_slot;
static int ut_hot_current_mx_describe_calls;
static int ut_hot_current_mx_resolve_calls;
static int ut_hot_current_mx_validate_calls;
static int ut_hot_requester_requalify_barrier_calls;
static int ut_hot_requester_requalify_barrier_lock_calls;
static bool ut_itl_census_active;
static bool ut_itl_census_mutate_wrap;
static bool ut_itl_census_lock_only;
static int ut_itl_census_alloc_calls;
static int ut_itl_census_capacity_calls;
static int ut_itl_census_resolve_calls;
static int ut_itl_census_retained_resolve_calls;
static int ut_itl_census_preflight_calls;
static int ut_itl_census_dirty_hint_calls;
static int ut_itl_recycle_guard_arm_calls;
static int ut_itl_recycle_guard_unlock_calls;
static int ut_itl_recycle_guard_relock_calls;
static int ut_itl_recycle_guard_cancel_calls;
static bool ut_itl_recycle_guard_active;
static ClusterBufmgrItlRecycleGuardResult ut_itl_recycle_guard_arm_result;
static uint64 ut_itl_census_tt_generation;
static uint64 ut_itl_census_origin_tt_generation;
static bool ut_itl_census_mutate_activation;
static const ClusterSemanticAdmissionToken *ut_itl_census_admission;
static ClusterSemanticActivationShmem ut_itl_census_semantic;
static ClusterTxOutcome ut_itl_census_outcomes[CLUSTER_ITL_INITRANS_DEFAULT];
static BufferTag ut_itl_census_tag;
static int ut_itl_wait_calls;
static ClusterTxLocator ut_itl_wait_locator;
static int ut_itl_wait_budget_ms;
static uint64 ut_evidence_metrics[CLUSTER_VIS_METRIC_COUNT];

void
cluster_vis_evidence_note(ClusterVisEvidenceMetric metric)
{
	UT_ASSERT((unsigned)metric < CLUSTER_VIS_METRIC_COUNT);
	ut_evidence_metrics[metric]++;
}
static ClusterTxwResult ut_itl_wait_result = CLUSTER_TXW_RESOLVED;
static bool ut_itl_pair_active;
static bool ut_itl_pair_content_lock_held[2];
static Buffer ut_itl_pair_lock_buffers[4];
static int ut_itl_pair_lock_calls;

bool
LWLockHeldByMe(LWLock *lock)
{
	if (lock == BufferDescriptorGetContentLock(GetBufferDescriptor(0)))
		return ut_itl_pair_active ? ut_itl_pair_content_lock_held[0] : ut_hot_content_lock_held;
	if (lock == BufferDescriptorGetContentLock(GetBufferDescriptor(1)))
		return ut_itl_pair_content_lock_held[1];
	UT_ASSERT(false);
	return false;
}

ClusterTxwResult
cluster_tx_enqueue_wait_exact(const ClusterTxLocator *locator, int effective_timeout_ms,
							  ClusterTxResolveReason *reason_out)
{
	ut_itl_wait_calls++;
	ut_itl_wait_locator = *locator;
	ut_itl_wait_budget_ms = effective_timeout_ms;
	UT_ASSERT(!ut_hot_content_lock_held);
	UT_ASSERT(!ut_itl_pair_content_lock_held[0]);
	UT_ASSERT(!ut_itl_pair_content_lock_held[1]);
	UT_ASSERT(!ut_itl_recycle_guard_active);
	UT_ASSERT_EQ(semantic_activation_local_inflight[CLUSTER_SEMANTIC_TARGET_SIDE][0], 0);
	UT_ASSERT(effective_timeout_ms > 0);
	*reason_out = ut_itl_wait_result == CLUSTER_TXW_TIMEOUT ? CLUSTER_TX_RESOLVE_TIMEOUT
															: CLUSTER_TX_RESOLVE_NONE;
	return ut_itl_wait_result;
}
void
cluster_multixact_current_stats_bump(
	ClusterCurrentMxStatId stat pg_attribute_unused())
{}

void
cluster_multixact_current_stats_record_restarts(uint32 restarts)
{
	UT_ASSERT_EQ(restarts, 0);
}

bool
cluster_heap_test_r4_target_reachable(void)
{
	return ut_hot_r4_target_reachable;
}

#define UT_HOT_CURRENT_EPOCH UINT32_C(9)
#define UT_HOT_CURRENT_MX_ORIGIN UINT16_C(1)
#define UT_HOT_CURRENT_MX_HASH UINT64_C(0x91c0ffee)
#define UT_HOT_FOREIGN_MXID ((MultiXactId)17)
#define UT_HOT_AUTH_UPDATER ((TransactionId)901)
#define UT_ORDINARY_LOCKER ((TransactionId)897)
#define UT_ORDINARY_REQUESTER ((TransactionId)905)

bool
cluster_itl_find_data_slot_index_by_xid(Page page, TransactionId raw_xid,
									uint8 *slot_index_out)
{
	const ClusterItlSlotData *slots;
	int match = -1;
	uint16 winning_wrap = 0;
	bool ambiguous = false;
	uint8 i;

	if (page == NULL || slot_index_out == NULL)
		return false;
	*slot_index_out = CLUSTER_ITL_SLOT_UNALLOCATED;
	if (!PageHasItl(page) || !TransactionIdIsValid(raw_xid))
		return false;
	slots = ClusterPageGetItlSlots(page);
	for (i = 0; i < CLUSTER_ITL_INITRANS_DEFAULT; i++)
	{
		const ClusterItlSlotData *slot = &slots[i];
		bool data = slot->flags == ITL_FLAG_ACTIVE
			|| slot->flags == ITL_FLAG_COMMITTED
			|| slot->flags == ITL_FLAG_ABORTED
			|| slot->flags == ITL_FLAG_NEEDS_CLEANOUT;

		if (!data || slot->xid != raw_xid
			|| UBA_is_invalid(slot->undo_segment_head))
			continue;
		if (match < 0 || slot->wrap > winning_wrap)
		{
			match = i;
			winning_wrap = slot->wrap;
			ambiguous = false;
		}
		else if (slot->wrap == winning_wrap)
			ambiguous = true;
	}
	if (match < 0 || ambiguous)
		return false;
	*slot_index_out = (uint8) match;
	return true;
}

bool
cluster_itl_get_tt_ref(Page page, uint8 itl_slot_idx, ClusterUndoTTSlotRef *ref)
{
	if (ut_writer_bridge_fixture) {
		UT_ASSERT(ut_hot_content_lock_held);
		*ref = ut_scratch_expected_ref;
		return true;
	}
	if (page == ut_hot_live_ref_page)
	{
		ut_hot_live_ref_calls++;
		if (ut_hot_current_mx_active && itl_slot_idx == 3)
		{
			*ref = ut_hot_successor_ref;
			return true;
		}
		UT_ASSERT_EQ(itl_slot_idx, 2);
		*ref = ut_hot_live_ref;
		return true;
	}

	ut_scratch_ref_calls++;
	UT_ASSERT(page == ut_scratch_expected_page);
	UT_ASSERT(page != ut_scratch_forbidden_live_page);
	UT_ASSERT_EQ(itl_slot_idx, 1);
	if (!ut_scratch_ref_available)
		return false;
	*ref = ut_scratch_expected_ref;
	return true;
}

bool
cluster_itl_find_multixact_origin_by_xmax(Page page, MultiXactId multixact_id,
										 uint16 *origin_node_id)
{
	UT_ASSERT(page == ut_hot_live_ref_page);
	UT_ASSERT_EQ(multixact_id, UT_HOT_FOREIGN_MXID);
	UT_ASSERT_NOT_NULL(origin_node_id);
	UT_ASSERT(ut_hot_content_lock_held);
	*origin_node_id = 0;
	return false;
}

void
cluster_visibility_resolve_from_ref_scn(TransactionId raw_xid,
										const ClusterUndoTTSlotRef *ref,
										XLogRecPtr anchor_lsn, SCN read_scn,
										ClusterVisResolve *out)
{
	ut_scratch_exact_resolve_calls++;
	if (ut_hot_production_core_active)
		UT_ASSERT(!ut_hot_content_lock_held);
	UT_ASSERT_EQ(raw_xid, ut_scratch_expected_xid);
	UT_ASSERT(memcmp(ref, &ut_scratch_expected_ref, sizeof(*ref)) == 0);
	UT_ASSERT_EQ((uint64)anchor_lsn, (uint64)ut_scratch_expected_lsn);
	UT_ASSERT_EQ((uint64)read_scn, (uint64)ut_scratch_expected_read_scn);
	memset(out, 0, sizeof(*out));
	out->evidence = ut_scratch_resolve_evidence;
	out->status = ut_scratch_resolve_status;
	out->commit_scn = ut_scratch_resolve_scn;
	if (ut_writer_bridge_fixture && ut_writer_bridge_mutation == 1)
		PageSetLSN(ut_hot_live_ref_page, ut_scratch_expected_lsn + 1);
	if (ut_writer_bridge_fixture && ut_writer_bridge_mutation == 2)
		ClusterPageGetItlSlots(ut_hot_live_ref_page)[2].wrap++;
	if (ut_writer_bridge_fixture && ut_writer_bridge_mutation == 3)
		ut_scratch_expected_ref.cluster_epoch++;
}

void
cluster_visibility_resolve_tuple(Buffer buffer pg_attribute_unused(),
								 HeapTupleHeader tuple pg_attribute_unused(),
								 TransactionId xid pg_attribute_unused(),
								 ClusterVisXidKind kind pg_attribute_unused(),
								 ClusterVisResolve *out)
{
	ut_writer_bridge_tuple_pulls++;
	UT_ASSERT(!ut_hot_content_lock_held);
	memset(out, 0, sizeof(*out));
	out->evidence = CLUSTER_VIS_EVIDENCE_REMOTE;
	out->status = CLUSTER_TT_STATUS_COMMITTED;
	out->commit_scn = ut_scratch_resolve_scn;
}

bool cluster_crossnode_write_write = true;
bool cluster_tx_enqueue_wait_enabled = true;
ClusterTxwResult
cluster_tx_enqueue_wait(const ClusterTTStatusKey *key pg_attribute_unused(),
						int timeout_ms pg_attribute_unused())
{
	UT_ASSERT(false);
	return CLUSTER_TXW_UNPROVABLE;
}
void
cluster_vis_bump_vis_conflict_failclosed_count(void)
{}
void
cluster_vis_bump_writer_chain_resolved_count(void)
{}
void
cluster_vis_bump_writer_chain_failclosed_count(void)
{}
Buffer
ReadBuffer(Relation relation pg_attribute_unused(), BlockNumber block pg_attribute_unused())
{
	UT_ASSERT(false);
	return InvalidBuffer;
}
BlockNumber
RelationGetNumberOfBlocksInFork(Relation relation pg_attribute_unused(),
								ForkNumber fork pg_attribute_unused())
{
	return 100;
}
void
UnlockReleaseBuffer(Buffer buffer pg_attribute_unused())
{
	UT_ASSERT(false);
}
bool
cluster_itl_find_lock_tt_ref_by_xmax(Page page pg_attribute_unused(),
									 TransactionId xid pg_attribute_unused(),
									 ClusterUndoTTSlotRef *ref pg_attribute_unused())
{
	UT_ASSERT(false);
	return false;
}
ClusterSemanticAdmissionResult
cluster_tt_status_source_dispatch(ClusterTTStatusSourceOp op pg_attribute_unused(),
								  const ClusterTTStatusSourceRequest *request pg_attribute_unused(),
								  ClusterTTStatusSourceResult *result pg_attribute_unused())
{
	UT_ASSERT(false);
	return CLUSTER_SEMANTIC_ADMISSION_CLOSED;
}

void
cluster_visibility_resolve_tuple_scn(Buffer buffer pg_attribute_unused(),
								 HeapTupleHeader tuple pg_attribute_unused(),
								 TransactionId raw_xid pg_attribute_unused(),
								 ClusterVisXidKind which pg_attribute_unused(),
								 SCN read_scn pg_attribute_unused(),
								 ClusterVisResolve *out)
{
	ut_scratch_live_resolve_calls++;
	memset(out, 0, sizeof(*out));
}

ClusterCrVerdict
cluster_cr_satisfies_mvcc(HeapTuple htup pg_attribute_unused(),
						  Snapshot snapshot pg_attribute_unused(),
						  Buffer buffer pg_attribute_unused(),
						  bool *visible pg_attribute_unused())
{
	ut_scratch_cr_calls++;
	return CLUSTER_CR_FAILCLOSED;
}

void
cluster_heap_test_r4_conflict_out(bool visible pg_attribute_unused(),
								 Relation relation pg_attribute_unused(),
								 HeapTuple tuple pg_attribute_unused(),
								 Buffer buffer pg_attribute_unused(),
								 Snapshot snapshot pg_attribute_unused())
{
	ut_scratch_ssi_calls++;
}

void
PredicateLockTID(Relation relation pg_attribute_unused(),
				 ItemPointer tid pg_attribute_unused(),
				 Snapshot snapshot pg_attribute_unused(),
				 TransactionId xid pg_attribute_unused())
{
	ut_scratch_ssi_calls++;
}

bool
cluster_heap_test_r4_live_visibility(HeapTuple tuple pg_attribute_unused(),
									 Snapshot snapshot pg_attribute_unused(),
									 Buffer buffer pg_attribute_unused())
{
	ut_live_visibility_calls++;
	return !OffsetNumberIsValid(ut_live_visible_offnum)
		   || ItemPointerGetOffsetNumber(&tuple->t_self) == ut_live_visible_offnum;
}

void
HeapTupleSetHintBits(HeapTupleHeader tuple pg_attribute_unused(),
					 Buffer buffer pg_attribute_unused(),
					 uint16 infomask pg_attribute_unused(),
					 TransactionId xid pg_attribute_unused())
{
	ut_scratch_hint_calls++;
}

void
MarkBufferDirty(Buffer buffer pg_attribute_unused())
{
	ut_scratch_dirty_calls++;
}

GlobalVisState *
GlobalVisTestFor(Relation relation pg_attribute_unused())
{
	return NULL;
}

bool
cluster_vis_prune_must_defer(bool storage_mode pg_attribute_unused(),
							bool cluster_horizon_available pg_attribute_unused())
{
	return true;
}

bool
cluster_heap_test_r4_surely_dead(HeapTuple tuple pg_attribute_unused(),
								 GlobalVisState *vistest pg_attribute_unused())
{
	return false;
}

int
GetMultiXactIdMembers(MultiXactId multi pg_attribute_unused(),
					  MultiXactMember **members,
					  bool from_pgupgrade pg_attribute_unused(),
					  bool isLockOnly pg_attribute_unused())
{
	ut_native_multixact_decode_calls++;
	*members = palloc(sizeof(**members));
	(*members)[0].xid = ut_native_multixact_updater;
	(*members)[0].status = MultiXactStatusUpdate;
	return 1;
}

int
cluster_mxid_origin_slot(MultiXactId mxid)
{
	UT_ASSERT_EQ(mxid, UT_HOT_FOREIGN_MXID);
	return UT_HOT_CURRENT_MX_ORIGIN;
}

bool
cluster_mxid_is_mine(MultiXactId mxid)
{
	UT_ASSERT_EQ(mxid, UT_HOT_FOREIGN_MXID);
	return false;
}

int
cluster_xid_origin_slot(TransactionId xid)
{
	UT_ASSERT_EQ(xid, UT_HOT_AUTH_UPDATER);
	return UT_HOT_CURRENT_MX_ORIGIN;
}

ClusterPcmOwnResult
cluster_bufmgr_pcm_own_snapshot(BufferDesc *buf, ClusterPcmOwnSnapshot *out)
{
	UT_ASSERT(ut_hot_current_mx_active || ut_itl_census_active);
	if (ut_itl_pair_active)
	{
		int index = (int) (buf - &ut_buffer_descriptors[0].bufferdesc);

		UT_ASSERT(index >= 0 && index < 2);
		UT_ASSERT(ut_itl_pair_content_lock_held[index]);
	}
	else
	{
		UT_ASSERT(ut_hot_content_lock_held);
		UT_ASSERT(buf == &ut_buffer_descriptors[0].bufferdesc);
	}
	UT_ASSERT_NOT_NULL(out);
	ut_hot_pcm_snapshot_calls++;
	memset(out, 0, sizeof(*out));
	out->tag = ut_itl_census_tag;
	out->generation
		= (ut_itl_census_replace_current_page
		   || ut_itl_census_stale_first_round_full)
			&& ut_hot_pcm_snapshot_calls > 1 ? 18 : 17;
	out->reservation_token = ut_itl_census_pcm_reservation_token;
	out->flags = ut_itl_census_pcm_flags;
	if (ut_hot_current_mx_active && ut_hot_current_mx_one_shot)
	{
		UT_ASSERT_EQ(ut_hot_current_mx_pcm_state,
					 (uint8) PCM_STATE_READ_IMAGE);
		UT_ASSERT(ut_hot_current_read_bracket > 0);
		out->generation = 16 + ut_hot_current_read_bracket;
		out->reservation_token = ut_hot_current_read_bracket;
	}
	if (ut_itl_census_second_round_drift
		&& ut_hot_pcm_snapshot_calls > 3)
		out->generation = 19;
	out->writer_activation_token
		= ut_itl_census_change_writer_activation_projection
		  && ut_hot_pcm_snapshot_calls > 1 ? UINT64_C(99) : 0;
	if (ut_hot_current_mx_active)
		out->pcm_state = ut_hot_current_mx_pcm_state;
	else
		out->pcm_state = cluster_conf_has_peers() && !ut_itl_census_force_pcm_n
			? (uint8) PCM_STATE_X : (uint8) PCM_STATE_N;
	ut_hot_last_pcm_snapshot_state = out->pcm_state;
	return CLUSTER_PCM_OWN_OK;
}

ClusterBufmgrItlRecycleGuardResult
cluster_bufmgr_itl_recycle_guard_arm(
	Buffer buffer, const ClusterPcmOwnSnapshot *expected)
{
	UT_ASSERT_EQ(buffer, (Buffer) 1);
	UT_ASSERT_NOT_NULL(expected);
	UT_ASSERT(ut_hot_content_lock_held);
	UT_ASSERT(!ut_itl_recycle_guard_active);
	UT_ASSERT_EQ(expected->pcm_state, (uint8) PCM_STATE_X);
	ut_itl_recycle_guard_arm_calls++;
	if (ut_itl_recycle_guard_arm_result
		!= CLUSTER_BUFMGR_ITL_RECYCLE_ARMED)
		return ut_itl_recycle_guard_arm_result;
	ut_itl_recycle_guard_active = true;
	return CLUSTER_BUFMGR_ITL_RECYCLE_ARMED;
}

void
cluster_bufmgr_itl_recycle_guard_unlock(Buffer buffer)
{
	UT_ASSERT_EQ(buffer, (Buffer) 1);
	UT_ASSERT(ut_itl_recycle_guard_active);
	UT_ASSERT(ut_hot_content_lock_held);
	ut_itl_recycle_guard_unlock_calls++;
	LockBuffer(buffer, BUFFER_LOCK_UNLOCK);
}

bool
cluster_bufmgr_itl_recycle_guard_relock(Buffer buffer)
{
	UT_ASSERT_EQ(buffer, (Buffer) 1);
	UT_ASSERT(ut_itl_recycle_guard_active);
	UT_ASSERT(!ut_hot_content_lock_held);
	ut_itl_recycle_guard_relock_calls++;
	LockBuffer(buffer, BUFFER_LOCK_EXCLUSIVE);
	ut_itl_recycle_guard_active = false;
	return true;
}

void
cluster_bufmgr_itl_recycle_guard_cancel(Buffer buffer)
{
	UT_ASSERT_EQ(buffer, (Buffer) 1);
	UT_ASSERT(ut_itl_recycle_guard_active);
	UT_ASSERT(!ut_hot_content_lock_held);
	ut_itl_recycle_guard_cancel_calls++;
	ut_itl_recycle_guard_active = false;
}

static bool
ut_itl_census_alloc(Buffer buf, TransactionId xid,
					bool lock_only, uint8 *slot_index_out)
{
	ClusterItlSlotData *slots;
	uint8 i;

	UT_ASSERT(ut_itl_census_active);
	UT_ASSERT(ut_hot_content_lock_held);
	UT_ASSERT_EQ(buf, (Buffer) 1);
	UT_ASSERT_EQ(lock_only, ut_itl_census_lock_only);
	ut_itl_census_alloc_calls++;
	slots = ClusterPageGetItlSlots(BufferGetPage(buf));
	for (i = 0; i < CLUSTER_ITL_INITRANS_DEFAULT; i++)
	{
		if (slots[i].flags == ITL_FLAG_FREE
			|| slots[i].flags == ITL_FLAG_COMMITTED
			|| slots[i].flags == ITL_FLAG_ABORTED
			|| slots[i].flags == ITL_FLAG_LOCK_ONLY_COMMITTED
			|| slots[i].flags == ITL_FLAG_LOCK_ONLY_ABORTED)
		{
			*slot_index_out = i;
			if (ut_itl_census_consume_allocated_slot)
			{
				slots[i].xid = xid;
				slots[i].flags = lock_only
					? ITL_FLAG_LOCK_ONLY_ACTIVE : ITL_FLAG_ACTIVE;
				slots[i].commit_scn = InvalidScn;
			}
			return true;
		}
	}
	return false;
}

bool
cluster_itl_alloc_or_reuse_slot(Buffer buf, TransactionId xid,
								uint8 *slot_index_out)
{
	return ut_itl_census_alloc(buf, xid, false, slot_index_out);
}

bool
cluster_itl_alloc_or_reuse_lock_slot(Buffer buf, TransactionId xid,
									 uint8 *slot_index_out)
{
	return ut_itl_census_alloc(buf, xid, true, slot_index_out);
}

bool
cluster_itl_has_allocatable_slot(Buffer buf, TransactionId xid,
								 bool lock_only)
{
	ClusterItlSlotData *slots;
	uint8 i;

	UT_ASSERT(ut_itl_census_active);
	UT_ASSERT(ut_hot_content_lock_held);
	UT_ASSERT_EQ(buf, (Buffer) 1);
	ut_itl_census_capacity_calls++;
	slots = ClusterPageGetItlSlots(BufferGetPage(buf));
	for (i = 0; i < CLUSTER_ITL_INITRANS_DEFAULT; i++)
	{
		if ((!lock_only && slots[i].flags == ITL_FLAG_ACTIVE
			 && slots[i].xid == xid)
			|| (lock_only && slots[i].flags == ITL_FLAG_LOCK_ONLY_ACTIVE
				&& slots[i].xid == xid)
			|| slots[i].flags == ITL_FLAG_FREE
			|| slots[i].flags == ITL_FLAG_COMMITTED
			|| slots[i].flags == ITL_FLAG_ABORTED
			|| slots[i].flags == ITL_FLAG_LOCK_ONLY_COMMITTED
			|| slots[i].flags == ITL_FLAG_LOCK_ONLY_ABORTED)
			return true;
	}
	return false;
}

bool
cluster_tx_locator_from_itl(Page page, uint8 slot_index,
							ClusterTxLocator *out,
							ClusterTxResolveReason *reason_out)
{
	ClusterItlSlotData *slot = &ClusterPageGetItlSlots(page)[slot_index];

	memset(out, 0, sizeof(*out));
	out->uba = slot->undo_segment_head;
	out->xid = slot->xid;
	out->tt_wrap = slot->wrap;
	out->itl_kind = slot->flags;
	out->itl_slot_index = slot_index;
	*reason_out = CLUSTER_TX_RESOLVE_NONE;
	return true;
}

bool
cluster_tx_locator_from_itl_terminal_census(
	Page page, uint8 slot_index, ClusterTxLocator *out,
	ClusterTxResolveReason *reason_out)
{
	if (!cluster_tx_locator_from_itl(page, slot_index, out, reason_out))
		return false;
	out->tt_wrap = TT_WRAP_INVALID;
	return true;
}

bool
cluster_multixact_current_successor_provenance_well_formed(
	const ClusterCurrentMxSuccessorAlias *alias,
	const ClusterTxLocator *locator, TransactionId updater_xid,
	uint16 updater_origin_node, uint32 current_epoch)
{
	uint32 segment_id;
	uint32 block_no;
	uint16 slot_offset;
	uint16 row_offset;

	return alias != NULL && locator != NULL
		&& uba_decode(locator->uba, &segment_id, &block_no, &slot_offset,
					  &row_offset)
		&& alias->origin_node_id == updater_origin_node
		&& alias->undo_record_segment_id == segment_id
		&& alias->tt_slot_id == (uint32)slot_offset + 1
		&& alias->cluster_epoch == current_epoch
		&& alias->local_xid == updater_xid
		&& locator->xid == updater_xid
		&& locator->tt_wrap == TT_WRAP_INVALID;
}

ClusterTxOutcome
cluster_tx_resolve_exact(const ClusterTxLocator *locator,
					 ClusterTxResolveMode mode,
					 ClusterTxResolution *out,
					 ClusterTxResolveReason *reason_out)
{
	(void) locator;
	(void) mode;
	(void) out;
	(void) reason_out;
	UT_ASSERT(false);
	return CLUSTER_TX_UNKNOWN;
}

void cluster_tx_resolve_terminal_census_batch_preflight(void);

void
cluster_tx_resolve_terminal_census_batch_preflight(void)
{
	UT_ASSERT(ut_itl_census_active);
	UT_ASSERT(!ut_hot_content_lock_held);
	UT_ASSERT_EQ(ut_itl_census_resolve_calls,
				 ut_itl_census_preflight_calls
				 * CLUSTER_ITL_INITRANS_DEFAULT);
	ut_itl_census_preflight_calls++;
}

ClusterTxOutcome
cluster_tx_resolve_exact_admitted(
	const ClusterTxLocator *locator, ClusterTxResolveMode mode,
	const ClusterSemanticAdmissionToken *admission, ClusterTxResolution *out,
	ClusterTxResolveReason *reason_out)
{
	ClusterTxOutcome outcome;

	UT_ASSERT(ut_itl_census_active);
	UT_ASSERT_NOT_NULL(admission);
	UT_ASSERT(admission->entered);
	UT_ASSERT_EQ(admission->record_generation, UINT64_C(73));
	UT_ASSERT_EQ(pg_atomic_read_u32(
		&ut_itl_census_semantic.inflight[CLUSTER_SEMANTIC_TARGET_SIDE][0]), 1);
	UT_ASSERT_EQ(semantic_activation_local_inflight
		[CLUSTER_SEMANTIC_TARGET_SIDE][0], 1);
	if (ut_itl_census_admission == NULL)
		ut_itl_census_admission = admission;
	else
		UT_ASSERT(admission == ut_itl_census_admission);
	if (ut_itl_pair_active)
	{
		UT_ASSERT(!ut_itl_pair_content_lock_held[0]);
		UT_ASSERT(!ut_itl_pair_content_lock_held[1]);
	}
	else
		UT_ASSERT(!ut_hot_content_lock_held);
	UT_ASSERT_EQ(mode, CLUSTER_TX_RESOLVE_TERMINAL_CENSUS);
	UT_ASSERT(locator->itl_slot_index < CLUSTER_ITL_INITRANS_DEFAULT);
	UT_ASSERT_EQ(locator->tt_wrap, TT_WRAP_INVALID);
	if (ut_itl_census_mutate_wrap && ut_itl_census_resolve_calls == 0)
		ClusterPageGetItlSlots(BufferGetPage((Buffer) 1))[0].wrap++;
	if (ut_itl_census_mutate_activation
		&& ut_itl_census_resolve_calls == 0)
		pg_atomic_write_u64(&ut_itl_census_semantic.record_generation,
						 UINT64_C(74));
	if (ut_itl_census_replace_current_page
		&& ut_itl_census_resolve_calls == 0)
	{
		ClusterItlSlotData *current_slots = ClusterPageGetItlSlots(
			BufferGetPage((Buffer) 1));

		current_slots[4].flags = ITL_FLAG_COMMITTED;
		current_slots[4].commit_scn = (SCN) 8001;
		PageSetLSN(BufferGetPage((Buffer) 1),
				   (XLogRecPtr) UINT64_C(0x334456));
	}
	if (ut_itl_census_stale_first_round_full
		&& ut_itl_census_resolve_calls == 0)
	{
		ClusterItlSlotData *current_slots = ClusterPageGetItlSlots(
			BufferGetPage((Buffer) 1));

		current_slots[0].xid = (TransactionId) 1400;
		current_slots[0].wrap = (uint16) 40;
		current_slots[0].undo_segment_head = uba_encode(1, 40, 0, 0);
		PageSetLSN(BufferGetPage((Buffer) 1),
				   (XLogRecPtr) UINT64_C(0x334456));
	}
	else if (ut_itl_census_stale_first_round_full
			 && ut_itl_census_resolve_calls
				== CLUSTER_ITL_INITRANS_DEFAULT)
	{
		UBA expected_uba = uba_encode(1, 40, 0, 0);

		UT_ASSERT_EQ(locator->xid, (TransactionId) 1400);
		UT_ASSERT_EQ(locator->uba.raw[0], expected_uba.raw[0]);
		UT_ASSERT_EQ(locator->uba.raw[1], expected_uba.raw[1]);
		ut_itl_census_second_round_fresh_locator_seen = true;
		if (ut_itl_census_second_round_drift)
		{
			ClusterItlSlotData *current_slots = ClusterPageGetItlSlots(
				BufferGetPage((Buffer) 1));

			current_slots[1].wrap++;
			PageSetLSN(BufferGetPage((Buffer) 1),
					   (XLogRecPtr) UINT64_C(0x334457));
		}
	}
	if (ut_itl_census_mutate_second_terminal_after_full_resolve
		&& !ut_itl_census_second_terminal_mutated
		&& ut_itl_census_resolve_calls == 7)
	{
		ClusterItlSlotData *current_slots = ClusterPageGetItlSlots(
			BufferGetPage((Buffer) 1));

		current_slots[1].wrap++;
		current_slots[4].flags = ITL_FLAG_COMMITTED;
		current_slots[4].commit_scn = (SCN) 8001;
		ut_itl_census_second_terminal_mutated = true;
	}
	ut_itl_census_resolve_calls++;
	outcome = ut_itl_census_outcomes[locator->itl_slot_index];
	memset(out, 0, sizeof(*out));
	out->locator_echo = *locator;
	out->locator_echo.tt_wrap = 42;
	out->top_xid = locator->xid;
	out->outcome = outcome;
	out->proof_kind = CLUSTER_TX_PROOF_ORIGIN_DURABLE_TT_CLOG;
	out->commit_scn = outcome == CLUSTER_TX_COMMITTED ? (SCN) 9001 : InvalidScn;
	out->authority.origin_epoch = UINT32_C(9);
	out->authority.live_hwm_lsn = (XLogRecPtr) 1;
	out->authority.tt_generation = ut_itl_census_origin_tt_generation;
	out->authority.authority_scn = (SCN) 9002;
	*reason_out = outcome == CLUSTER_TX_UNKNOWN
		? CLUSTER_TX_RESOLVE_AUTHORITY_UNAVAILABLE
		: CLUSTER_TX_RESOLVE_NONE;
	return outcome;
}

ClusterTxOutcome
cluster_tx_resolve_terminal_census_retained_admitted(
	const ClusterTxLocator *locator, SCN retained_commit_scn,
	const ClusterSemanticAdmissionToken *admission, ClusterTxResolution *out,
	ClusterTxResolveReason *reason_out)
{
	UT_ASSERT_EQ(locator->itl_kind, ITL_FLAG_NEEDS_CLEANOUT);
	UT_ASSERT(SCN_VALID(retained_commit_scn));
	ut_itl_census_retained_resolve_calls++;
	return cluster_tx_resolve_exact_admitted(
		locator, CLUSTER_TX_RESOLVE_TERMINAL_CENSUS, admission, out,
		reason_out);
}

uint64
cluster_undo_tt_retention_rollover_count(void)
{
	return ut_itl_census_tt_generation;
}

bool
cluster_ctrc_shmem_ready(void)
{
	return true;
}

void
MarkBufferDirtyHint(Buffer buffer, bool buffer_std pg_attribute_unused())
{
	UT_ASSERT_EQ(buffer, (Buffer) 1);
	UT_ASSERT(ut_hot_content_lock_held);
	ut_itl_census_dirty_hint_calls++;
}

uint64
cluster_multixact_current_descriptor_hash(const ClusterCurrentMxKey *key,
										  const ClusterCurrentMxMemberDesc *members,
										  uint16 nmembers)
{
	UT_ASSERT_NOT_NULL(key);
	UT_ASSERT_NOT_NULL(members);
	UT_ASSERT_EQ(key->origin_node_id, UT_HOT_CURRENT_MX_ORIGIN);
	UT_ASSERT_EQ(key->multixact_id, UT_HOT_FOREIGN_MXID);
	UT_ASSERT_EQ(key->cluster_epoch, UT_HOT_CURRENT_EPOCH);
	UT_ASSERT_EQ(nmembers, ut_hot_current_mx_member_count);
	if (ut_current_mx_ordinary_lock_only)
	{
		UT_ASSERT_EQ(nmembers, 1);
		UT_ASSERT_EQ(members[0].xid, UT_ORDINARY_LOCKER);
		UT_ASSERT_EQ(members[0].member_status,
					 MultiXactStatusForKeyShare);
	}
	else if (nmembers == 1)
	{
		UT_ASSERT_EQ(members[0].xid, UT_HOT_AUTH_UPDATER);
		UT_ASSERT_EQ(members[0].member_status, MultiXactStatusUpdate);
	}
	else
	{
		UT_ASSERT_EQ(nmembers, 2);
		UT_ASSERT_EQ(members[0].member_status, MultiXactStatusForShare);
		UT_ASSERT_EQ(members[1].xid, UT_HOT_AUTH_UPDATER);
		UT_ASSERT_EQ(members[1].member_status, MultiXactStatusUpdate);
	}
	return UT_HOT_CURRENT_MX_HASH;
}

ClusterMxDescribeResult
cluster_multixact_current_validate_descriptor(
	const ClusterCurrentMxKey *key, uint16 source_node_id, uint32 current_epoch,
	const ClusterCurrentMxMemberDesc *members, uint16 nmembers,
	uint32 reported_total_members)
{
	UT_ASSERT_NOT_NULL(key);
	UT_ASSERT_NOT_NULL(members);
	UT_ASSERT_EQ(source_node_id, UT_HOT_CURRENT_MX_ORIGIN);
	UT_ASSERT_EQ(current_epoch, UT_HOT_CURRENT_EPOCH);
	UT_ASSERT_EQ(nmembers, ut_hot_current_mx_member_count);
	UT_ASSERT_EQ(reported_total_members, nmembers);
	if (ut_current_mx_ordinary_lock_only)
	{
		UT_ASSERT_EQ(members[0].xid, UT_ORDINARY_LOCKER);
		UT_ASSERT_EQ(members[0].member_status,
					 MultiXactStatusForKeyShare);
	}
	else
	{
		UT_ASSERT_EQ(members[nmembers - 1].xid, UT_HOT_AUTH_UPDATER);
		UT_ASSERT_EQ(members[nmembers - 1].member_status,
					 MultiXactStatusUpdate);
	}
	return CMX_DESC_OK;
}

ClusterMxDescribeResult
cluster_multixact_current_describe(const ClusterCurrentMxKey *key,
								   ClusterCurrentMxMemberDesc *members,
								   uint16 members_cap, uint16 *nmembers,
								   uint32 *reported_total_members)
{
	UT_ASSERT(ut_hot_current_mx_active);
	UT_ASSERT(!ut_hot_content_lock_held);
	UT_ASSERT_NOT_NULL(key);
	UT_ASSERT(members_cap >= ut_hot_current_mx_member_count);
	UT_ASSERT_EQ(key->origin_node_id, UT_HOT_CURRENT_MX_ORIGIN);
	UT_ASSERT_EQ(key->multixact_id, UT_HOT_FOREIGN_MXID);
	UT_ASSERT_EQ(key->cluster_epoch, UT_HOT_CURRENT_EPOCH);
	ut_hot_current_mx_describe_calls++;
	memset(members, 0, sizeof(*members) * members_cap);
	if (ut_current_mx_ordinary_lock_only)
	{
		UT_ASSERT_EQ(ut_hot_current_mx_member_count, 1);
		members[0].xid = UT_ORDINARY_LOCKER;
		members[0].member_status = MultiXactStatusForKeyShare;
	}
	else if (ut_hot_current_mx_member_count == 1)
	{
		members[0].xid = UT_HOT_AUTH_UPDATER;
		members[0].member_status = MultiXactStatusUpdate;
	}
	else
	{
		UT_ASSERT_EQ(ut_hot_current_mx_member_count, 2);
		members[0].xid = (TransactionId) 897;
		members[0].member_status = MultiXactStatusForShare;
		members[1].xid = UT_HOT_AUTH_UPDATER;
		members[1].member_status = MultiXactStatusUpdate;
	}
	*nmembers = ut_hot_current_mx_member_count;
	*reported_total_members = ut_hot_current_mx_member_count;
	return CMX_DESC_OK;
}

ClusterMxResolveResult
cluster_multixact_current_members_resolve(const ClusterCurrentMxKey *key,
										  const ClusterCurrentMxMemberDesc *members,
										  uint16 nmembers, uint64 descriptor_hash,
										  const ClusterCurrentUpdaterChallenge *challenge,
										  ClusterCurrentMemberProof *proofs,
										  ClusterCurrentUpdaterProof *updater_proof,
										  uint32 *proof_capability_generations)
{
	uint16 i;
	uint16 updater_ordinal = ut_hot_current_mx_member_count - 1;

	UT_ASSERT(ut_hot_current_mx_active);
	UT_ASSERT(!ut_hot_content_lock_held);
	UT_ASSERT_EQ(nmembers, ut_hot_current_mx_member_count);
	UT_ASSERT_EQ(descriptor_hash, UT_HOT_CURRENT_MX_HASH);
	if (ut_current_mx_ordinary_lock_only)
	{
		UT_ASSERT(challenge == NULL);
		UT_ASSERT_EQ(nmembers, 1);
		UT_ASSERT_EQ(members[0].xid, UT_ORDINARY_LOCKER);
		UT_ASSERT_EQ(members[0].member_status,
					 MultiXactStatusForKeyShare);
		ut_hot_current_mx_resolve_calls++;
		memset(proofs, 0, sizeof(*proofs));
		proofs[0].member_xid = members[0].xid;
		proofs[0].member_ordinal = 0;
		proofs[0].member_status = members[0].member_status;
		proofs[0].state = CCM_ACTIVE;
		proofs[0].key.origin_node_id = UT_HOT_CURRENT_MX_ORIGIN;
		proofs[0].key.undo_segment_id = 258;
		proofs[0].key.tt_slot_id = 7;
		proofs[0].key.cluster_epoch = UT_HOT_CURRENT_EPOCH;
		proofs[0].key.local_xid = UT_ORDINARY_LOCKER;
		proof_capability_generations[0] = 41;
		memset(updater_proof, 0, sizeof(*updater_proof));
		return CMX_RESOLVE_OK;
	}
	UT_ASSERT_EQ(challenge->updater_xid, UT_HOT_AUTH_UPDATER);
	UT_ASSERT_EQ(challenge->member_ordinal, updater_ordinal);
	UT_ASSERT_EQ(challenge->candidate_next_xmin_alias.origin_node_id,
				 UT_HOT_CURRENT_MX_ORIGIN);
	UT_ASSERT_EQ(challenge->candidate_next_xmin_alias.undo_record_segment_id,
				 ut_hot_successor_ref.undo_segment_id);
	UT_ASSERT_EQ(challenge->candidate_next_xmin_alias.tt_slot_id,
				 ut_hot_successor_ref.tt_slot_id);
	UT_ASSERT_EQ(challenge->candidate_next_xmin_alias.cluster_epoch,
				 UT_HOT_CURRENT_EPOCH);
	UT_ASSERT_EQ(challenge->candidate_next_xmin_alias.local_xid,
				 UT_HOT_AUTH_UPDATER);
	ut_hot_current_mx_resolve_calls++;
	UT_ASSERT_NOT_NULL(proof_capability_generations);
	memset(proofs, 0, sizeof(*proofs) * nmembers);
	for (i = 0; i < nmembers; i++)
	{
		proof_capability_generations[i] = 41;
		proofs[i].member_xid = members[i].xid;
		proofs[i].member_ordinal = i;
		proofs[i].member_status = members[i].member_status;
		proofs[i].state = i == updater_ordinal ? CCM_COMMITTED : CCM_ACTIVE;
		if (i == updater_ordinal)
			proofs[i].commit_scn = (SCN) 101;
	}
	memset(updater_proof, 0, sizeof(*updater_proof));
	updater_proof->mxkey = *key;
	updater_proof->candidate_next_xmin_alias
		= challenge->candidate_next_xmin_alias;
	updater_proof->candidate_next_xmin_locator
		= challenge->candidate_next_xmin_locator;
	/* The origin's exact undo record supplies the canonical TT wrap; make it
	 * deliberately distinct from the page ITL wrap in this fixture. */
	updater_proof->candidate_next_xmin_locator.tt_wrap = 42;
	updater_proof->updater_xid = challenge->updater_xid;
	updater_proof->member_ordinal = challenge->member_ordinal;
	updater_proof->verdict = CUCP_MATCH;
	return CMX_RESOLVE_OK;
}

ClusterMxResolveResult
cluster_multixact_current_members_resolve_until(
	const ClusterCurrentMxKey *key,
	const ClusterCurrentMxMemberDesc *members,
	uint16 nmembers, uint64 descriptor_hash,
	const ClusterCurrentUpdaterChallenge *challenge,
	ClusterCurrentMemberProof *proofs,
	ClusterCurrentUpdaterProof *updater_proof,
	uint32 *proof_capability_generations,
	TimestampTz *operation_deadline)
{
	UT_ASSERT_NOT_NULL(operation_deadline);
	if (*operation_deadline == 0)
		*operation_deadline = (TimestampTz) UINT64_C(123456789);
	else
		UT_ASSERT_EQ(*operation_deadline,
					 (TimestampTz) UINT64_C(123456789));
	return cluster_multixact_current_members_resolve(
		key, members, nmembers, descriptor_hash, challenge, proofs,
		updater_proof, proof_capability_generations);
}

bool
cluster_multixact_current_validate_updater_proof(
	const ClusterCurrentMxKey *key, const ClusterCurrentMxMemberDesc *members,
	const ClusterCurrentMemberProof *proofs, uint16 nmembers,
	const ClusterCurrentUpdaterChallenge *challenge,
	const ClusterCurrentUpdaterProof *updater_proof,
	uint16 updater_origin_node_id)
{
	uint16 updater_ordinal = ut_hot_current_mx_member_count - 1;

	UT_ASSERT(ut_hot_current_mx_active);
	UT_ASSERT(ut_hot_content_lock_held);
	UT_ASSERT_EQ(nmembers, ut_hot_current_mx_member_count);
	UT_ASSERT_EQ(updater_origin_node_id, UT_HOT_CURRENT_MX_ORIGIN);
	UT_ASSERT_EQ(members[updater_ordinal].xid, UT_HOT_AUTH_UPDATER);
	UT_ASSERT_EQ(proofs[updater_ordinal].state, CCM_COMMITTED);
	UT_ASSERT_EQ(challenge->member_ordinal, updater_ordinal);
	UT_ASSERT_EQ(updater_proof->verdict, CUCP_MATCH);
	UT_ASSERT(memcmp(key, &updater_proof->mxkey, sizeof(*key)) == 0);
	UT_ASSERT(memcmp(&challenge->candidate_next_xmin_alias,
				 &updater_proof->candidate_next_xmin_alias,
				 sizeof(ClusterCurrentMxSuccessorAlias)) == 0);
	UT_ASSERT_EQ(challenge->candidate_next_xmin_locator.tt_wrap,
				 TT_WRAP_INVALID);
	UT_ASSERT_EQ(updater_proof->candidate_next_xmin_locator.tt_wrap, 42);
	UT_ASSERT(cluster_tx_locator_reply_matches(
		&challenge->candidate_next_xmin_locator,
		&updater_proof->candidate_next_xmin_locator));
	ut_hot_current_mx_validate_calls++;
	return true;
}

ClusterCurrentMxDecision
cluster_multixact_current_decide(
	const ClusterCurrentMxMemberDesc *members,
	const ClusterCurrentMemberProof *proofs, uint16 nmembers,
	const ClusterCurrentMxRequestContext *ctx,
	const ClusterCurrentUpdaterChallenge *challenge,
	const ClusterCurrentUpdaterProof *updater_proof,
	ClusterTTStatusKey *wait_key)
{
	UT_ASSERT(ut_current_mx_ordinary_lock_only);
	UT_ASSERT_EQ(nmembers, 1);
	UT_ASSERT_EQ(members[0].xid, UT_ORDINARY_LOCKER);
	UT_ASSERT_EQ(members[0].member_status, MultiXactStatusForKeyShare);
	UT_ASSERT_EQ(proofs[0].state, CCM_ACTIVE);
	UT_ASSERT_EQ(proofs[0].key.local_xid, UT_ORDINARY_LOCKER);
	UT_ASSERT_NOT_NULL(ctx);
	UT_ASSERT_EQ(ctx->action, CCM_ACTION_LOCK);
	UT_ASSERT_EQ(ctx->lock_mode, LockTupleKeyShare);
	UT_ASSERT_EQ(ctx->desired_status, MultiXactStatusForKeyShare);
	UT_ASSERT_EQ(ctx->tuple_shape, CCM_SHAPE_LOCK_ONLY);
	UT_ASSERT(ctx->wait_for_conflict);
	UT_ASSERT(challenge == NULL);
	UT_ASSERT_NOT_NULL(updater_proof);
	UT_ASSERT_NOT_NULL(wait_key);
	return CMDL_CONTINUE;
}

ClusterCurrentMxDecision
cluster_multixact_current_decide_observed(
	const ClusterCurrentMxMemberDesc *members,
	const ClusterCurrentMemberProof *proofs, uint16 nmembers,
	const ClusterCurrentMxRequestContext *ctx,
	const ClusterCurrentUpdaterChallenge *challenge,
	const ClusterCurrentUpdaterProof *updater_proof,
	ClusterTTStatusKey *wait_key, ClusterCurrentMxDecisionTrace *trace)
{
	if (trace != NULL) {
		trace->unknown_reason = CMX_UNKNOWN_NONE;
		trace->member_ordinal = -1;
	}
	return cluster_multixact_current_decide(members, proofs, nmembers, ctx,
		challenge, updater_proof, wait_key);
}

ClusterUndoRecordConsumePreflightResult
cluster_undo_record_consume_preflight(ClusterUndoRecordPrepareReceipt *receipt,
									 uint16 payload_len)
{
	(void) receipt;
	(void) payload_len;
	return CLUSTER_UNDO_RECORD_CONSUME_PREFLIGHT_READY;
}

ClusterMxRecomposeResult
cluster_multixact_current_recompose(
	const ClusterCurrentMxMemberDesc *members,
	const ClusterCurrentMemberProof *proofs, uint16 nmembers,
	TransactionId requester_xid, MultiXactStatus requester_status,
	MultiXactMember *normalized_members, uint16 normalized_cap,
	uint16 *normalized_count)
{
	UT_ASSERT(ut_current_mx_ordinary_lock_only);
	UT_ASSERT_EQ(nmembers, 1);
	UT_ASSERT_EQ(members[0].xid, UT_ORDINARY_LOCKER);
	UT_ASSERT_EQ(proofs[0].state, CCM_ACTIVE);
	UT_ASSERT_EQ(requester_status, MultiXactStatusForKeyShare);
	UT_ASSERT(normalized_cap >= 2);
	UT_ASSERT_NOT_NULL(normalized_count);
	normalized_members[0].xid = members[0].xid;
	normalized_members[0].status = (MultiXactStatus) members[0].member_status;
	normalized_members[1].xid = requester_xid;
	normalized_members[1].status = requester_status;
	*normalized_count = 2;
	return CMX_RECOMPOSE_OK;
}

int
scn_time_cmp(SCN a, SCN b)
{
	return a < b ? -1 : a > b ? 1 : 0;
}

static void *
ut_alloc(Size size, bool zero)
{
	void *ptr = zero ? calloc(1, size) : malloc(size);

	if (ptr == NULL)
		abort();
	if (ut_alloc_calls >= lengthof(ut_allocations))
		abort();
	ut_allocations[ut_alloc_calls] = ptr;
	ut_alloc_calls++;
	return ptr;
}

void *
palloc(Size size)
{
	return ut_alloc(size, false);
}

void *
palloc0(Size size)
{
	return ut_alloc(size, true);
}

void *
MemoryContextAlloc(MemoryContext context pg_attribute_unused(), Size size)
{
	return ut_alloc(size, false);
}

void *
MemoryContextAllocZero(MemoryContext context pg_attribute_unused(), Size size)
{
	return ut_alloc(size, true);
}

void *
MemoryContextAllocZeroAligned(MemoryContext context pg_attribute_unused(), Size size)
{
	return ut_alloc(size, true);
}

void
pfree(void *pointer)
{
	if (pointer != NULL)
	{
		int i;

		for (i = 0; i < ut_alloc_calls; i++)
		{
			if (ut_allocations[i] == pointer)
			{
				ut_allocations[i] = NULL;
				ut_free_calls++;
				free(pointer);
				return;
			}
		}
		ut_invalid_free_calls++;
	}
}

void
IncrBufferRefCount(Buffer buffer)
{
	UT_ASSERT_EQ(buffer, 1);
	ut_buffer_incr_calls++;
}

void
ReleaseBuffer(Buffer buffer)
{
	UT_ASSERT_EQ(buffer, ut_itl_pair_active ? 2 : 1);
	if (ut_itl_pair_active) {
		UT_ASSERT(!ut_itl_pair_content_lock_held[0]);
		UT_ASSERT(!ut_itl_pair_content_lock_held[1]);
	}
	ut_buffer_release_calls++;
}

void
IncrTupleDescRefCount(TupleDesc tupdesc pg_attribute_unused())
{}

void
DecrTupleDescRefCount(TupleDesc tupdesc pg_attribute_unused())
{}

ExpandedObjectHeader *
DatumGetEOHP(Datum datum pg_attribute_unused())
{
	return NULL;
}

Size
EOH_get_flat_size(ExpandedObjectHeader *eohptr pg_attribute_unused())
{
	return 0;
}

void
EOH_flatten_into(ExpandedObjectHeader *eohptr pg_attribute_unused(),
				 void *result pg_attribute_unused(),
				 Size allocated_size pg_attribute_unused())
{}

Datum
datumCopy(Datum value, bool typByVal pg_attribute_unused(),
		  int typLen pg_attribute_unused())
{
	return value;
}

uint32
hash_bytes(const unsigned char *key pg_attribute_unused(),
		   int keylen pg_attribute_unused())
{
	return 0;
}

HTAB *
hash_create(const char *tabname pg_attribute_unused(),
			long nelem pg_attribute_unused(),
			const HASHCTL *info pg_attribute_unused(),
			int flags pg_attribute_unused())
{
	return NULL;
}

void *
hash_search(HTAB *hashp pg_attribute_unused(),
			const void *keyPtr pg_attribute_unused(),
			HASHACTION action pg_attribute_unused(),
			bool *foundPtr pg_attribute_unused())
{
	return NULL;
}

Datum
toast_flatten_tuple_to_datum(HeapTupleHeader tuple pg_attribute_unused(),
							 uint32 tuple_len pg_attribute_unused(),
							 TupleDesc tuple_desc pg_attribute_unused())
{
	return (Datum)0;
}

int
errmsg_internal(const char *fmt pg_attribute_unused(), ...)
{
	return 0;
}

int
errdetail(const char *fmt pg_attribute_unused(), ...)
{
	return 0;
}

const char *
cluster_cr_build_reason_name(ClusterCrBuildReason reason pg_attribute_unused())
{
	return "unit-test";
}

void
ExceptionalCondition(const char *condition_name pg_attribute_unused(),
					 const char *file_name pg_attribute_unused(),
					 int line_number pg_attribute_unused())
{
	abort();
}

#define DEFINE_WAIT_ALLOWED_TEST(test_name, edge_value)                                            \
	UT_TEST(test_name)                                                                             \
	{                                                                                              \
		UT_ASSERT(semantic_activation_control_wait_allowed((edge_value),                           \
														   SEMANTIC_ACTIVATION_HELD_NONE));        \
	}

#define DEFINE_WAIT_FORBIDDEN_TEST(test_name, edge_value, lock_value)                              \
	UT_TEST(test_name)                                                                             \
	{                                                                                              \
		UT_ASSERT(!semantic_activation_control_wait_allowed((edge_value), (lock_value)));          \
	}

UT_TEST(test_01_held_lock_bits_are_independent)
{
	UT_ASSERT_EQ(SEMANTIC_ACTIVATION_HELD_RESOURCE, UINT32_C(1));
	UT_ASSERT_EQ(SEMANTIC_ACTIVATION_HELD_BUFFER, UINT32_C(2));
	UT_ASSERT_EQ(SEMANTIC_ACTIVATION_HELD_SLRU, UINT32_C(4));
	UT_ASSERT_EQ(SEMANTIC_ACTIVATION_HELD_UNDO_IO, UINT32_C(8));
	UT_ASSERT_EQ(SEMANTIC_ACTIVATION_HELD_IC_DISPATCH, UINT32_C(16));
}

UT_TEST(test_02_wait_edge_values_are_closed)
{
	UT_ASSERT_EQ(SEMANTIC_ACTIVATION_WAIT_UTILITY_TO_LMON, 0);
	UT_ASSERT_EQ(SEMANTIC_ACTIVATION_WAIT_LMON_TO_QVOTEC, 1);
	UT_ASSERT_EQ(SEMANTIC_ACTIVATION_WAIT_LMON_TO_PEER_ACK, 2);
	UT_ASSERT_EQ(SEMANTIC_ACTIVATION_WAIT_LMON_TO_CONTROL_BARRIER, 3);
}

DEFINE_WAIT_ALLOWED_TEST(test_03_utility_to_lmon_wait_with_no_lock_is_allowed,
						 SEMANTIC_ACTIVATION_WAIT_UTILITY_TO_LMON)
DEFINE_WAIT_FORBIDDEN_TEST(test_04_utility_wait_rejects_resource_lock,
						   SEMANTIC_ACTIVATION_WAIT_UTILITY_TO_LMON,
						   SEMANTIC_ACTIVATION_HELD_RESOURCE)
DEFINE_WAIT_FORBIDDEN_TEST(test_05_utility_wait_rejects_buffer_lock,
						   SEMANTIC_ACTIVATION_WAIT_UTILITY_TO_LMON,
						   SEMANTIC_ACTIVATION_HELD_BUFFER)
DEFINE_WAIT_FORBIDDEN_TEST(test_06_utility_wait_rejects_slru_lock,
						   SEMANTIC_ACTIVATION_WAIT_UTILITY_TO_LMON, SEMANTIC_ACTIVATION_HELD_SLRU)
DEFINE_WAIT_FORBIDDEN_TEST(test_07_utility_wait_rejects_undo_io_ownership,
						   SEMANTIC_ACTIVATION_WAIT_UTILITY_TO_LMON,
						   SEMANTIC_ACTIVATION_HELD_UNDO_IO)
DEFINE_WAIT_FORBIDDEN_TEST(test_08_utility_wait_rejects_ic_dispatch_ownership,
						   SEMANTIC_ACTIVATION_WAIT_UTILITY_TO_LMON,
						   SEMANTIC_ACTIVATION_HELD_IC_DISPATCH)
DEFINE_WAIT_FORBIDDEN_TEST(test_09_utility_wait_rejects_combined_forbidden_locks,
						   SEMANTIC_ACTIVATION_WAIT_UTILITY_TO_LMON,
						   SEMANTIC_ACTIVATION_HELD_RESOURCE | SEMANTIC_ACTIVATION_HELD_BUFFER
							   | SEMANTIC_ACTIVATION_HELD_SLRU)

DEFINE_WAIT_ALLOWED_TEST(test_10_lmon_to_qvotec_wait_with_no_lock_is_allowed,
						 SEMANTIC_ACTIVATION_WAIT_LMON_TO_QVOTEC)
DEFINE_WAIT_FORBIDDEN_TEST(test_11_qvotec_wait_rejects_resource_lock,
						   SEMANTIC_ACTIVATION_WAIT_LMON_TO_QVOTEC,
						   SEMANTIC_ACTIVATION_HELD_RESOURCE)
DEFINE_WAIT_FORBIDDEN_TEST(test_12_qvotec_wait_rejects_buffer_lock,
						   SEMANTIC_ACTIVATION_WAIT_LMON_TO_QVOTEC, SEMANTIC_ACTIVATION_HELD_BUFFER)
DEFINE_WAIT_FORBIDDEN_TEST(test_13_qvotec_wait_rejects_slru_lock,
						   SEMANTIC_ACTIVATION_WAIT_LMON_TO_QVOTEC, SEMANTIC_ACTIVATION_HELD_SLRU)
DEFINE_WAIT_FORBIDDEN_TEST(test_14_qvotec_wait_rejects_undo_io_ownership,
						   SEMANTIC_ACTIVATION_WAIT_LMON_TO_QVOTEC,
						   SEMANTIC_ACTIVATION_HELD_UNDO_IO)
DEFINE_WAIT_FORBIDDEN_TEST(test_15_qvotec_wait_rejects_ic_dispatch_ownership,
						   SEMANTIC_ACTIVATION_WAIT_LMON_TO_QVOTEC,
						   SEMANTIC_ACTIVATION_HELD_IC_DISPATCH)
DEFINE_WAIT_FORBIDDEN_TEST(test_16_qvotec_wait_rejects_all_forbidden_locks,
						   SEMANTIC_ACTIVATION_WAIT_LMON_TO_QVOTEC,
						   SEMANTIC_ACTIVATION_HELD_ALL_FORBIDDEN)

DEFINE_WAIT_ALLOWED_TEST(test_17_peer_ack_wait_with_no_lock_is_allowed,
						 SEMANTIC_ACTIVATION_WAIT_LMON_TO_PEER_ACK)
DEFINE_WAIT_FORBIDDEN_TEST(test_18_peer_ack_wait_rejects_resource_lock,
						   SEMANTIC_ACTIVATION_WAIT_LMON_TO_PEER_ACK,
						   SEMANTIC_ACTIVATION_HELD_RESOURCE)
DEFINE_WAIT_FORBIDDEN_TEST(test_19_peer_ack_wait_rejects_buffer_lock,
						   SEMANTIC_ACTIVATION_WAIT_LMON_TO_PEER_ACK,
						   SEMANTIC_ACTIVATION_HELD_BUFFER)
DEFINE_WAIT_FORBIDDEN_TEST(test_20_peer_ack_wait_rejects_slru_lock,
						   SEMANTIC_ACTIVATION_WAIT_LMON_TO_PEER_ACK, SEMANTIC_ACTIVATION_HELD_SLRU)
DEFINE_WAIT_FORBIDDEN_TEST(test_21_peer_ack_wait_rejects_undo_io_ownership,
						   SEMANTIC_ACTIVATION_WAIT_LMON_TO_PEER_ACK,
						   SEMANTIC_ACTIVATION_HELD_UNDO_IO)
DEFINE_WAIT_FORBIDDEN_TEST(test_22_peer_ack_wait_rejects_ic_dispatch_ownership,
						   SEMANTIC_ACTIVATION_WAIT_LMON_TO_PEER_ACK,
						   SEMANTIC_ACTIVATION_HELD_IC_DISPATCH)

DEFINE_WAIT_ALLOWED_TEST(test_23_control_barrier_with_no_lock_is_allowed,
						 SEMANTIC_ACTIVATION_WAIT_LMON_TO_CONTROL_BARRIER)
DEFINE_WAIT_FORBIDDEN_TEST(test_24_control_barrier_rejects_resource_lock,
						   SEMANTIC_ACTIVATION_WAIT_LMON_TO_CONTROL_BARRIER,
						   SEMANTIC_ACTIVATION_HELD_RESOURCE)
DEFINE_WAIT_FORBIDDEN_TEST(test_25_control_barrier_rejects_buffer_lock,
						   SEMANTIC_ACTIVATION_WAIT_LMON_TO_CONTROL_BARRIER,
						   SEMANTIC_ACTIVATION_HELD_BUFFER)
DEFINE_WAIT_FORBIDDEN_TEST(test_26_control_barrier_rejects_slru_lock,
						   SEMANTIC_ACTIVATION_WAIT_LMON_TO_CONTROL_BARRIER,
						   SEMANTIC_ACTIVATION_HELD_SLRU)
DEFINE_WAIT_FORBIDDEN_TEST(test_27_control_barrier_rejects_undo_io_ownership,
						   SEMANTIC_ACTIVATION_WAIT_LMON_TO_CONTROL_BARRIER,
						   SEMANTIC_ACTIVATION_HELD_UNDO_IO)
DEFINE_WAIT_FORBIDDEN_TEST(test_28_control_barrier_rejects_ic_dispatch_ownership,
						   SEMANTIC_ACTIVATION_WAIT_LMON_TO_CONTROL_BARRIER,
						   SEMANTIC_ACTIVATION_HELD_IC_DISPATCH)

UT_TEST(test_29_process_utility_may_only_wait_on_lmon)
{
	UT_ASSERT(semantic_activation_actor_edge_allowed(SEMANTIC_ACTIVATION_ACTOR_PROCESS_UTILITY,
													 SEMANTIC_ACTIVATION_ACTOR_LMON));
}

UT_TEST(test_30_lmon_may_only_delegate_durable_io_to_qvotec)
{
	UT_ASSERT(semantic_activation_actor_edge_allowed(SEMANTIC_ACTIVATION_ACTOR_LMON,
													 SEMANTIC_ACTIVATION_ACTOR_QVOTEC));
}

UT_TEST(test_31_lmon_control_path_never_enters_holder_lms)
{
	UT_ASSERT(!semantic_activation_actor_edge_allowed(SEMANTIC_ACTIVATION_ACTOR_LMON,
													  SEMANTIC_ACTIVATION_ACTOR_LMS));
}

UT_TEST(test_32_qvotec_completion_never_enters_origin_data)
{
	UT_ASSERT(!semantic_activation_actor_edge_allowed(SEMANTIC_ACTIVATION_ACTOR_QVOTEC,
													  SEMANTIC_ACTIVATION_ACTOR_DATA));
}

#define UT_HOT_BLOCK ((BlockNumber)17)
#define UT_HOT_ROOT_OFF FirstOffsetNumber
#define UT_HOT_SUCCESSOR_OFF (FirstOffsetNumber + 1)
#define UT_HOT_TUPLE_LEN 64
#define UT_HOT_DATA_OFF (BLCKSZ - CLUSTER_ITL_SPECIAL_SIZE - UT_HOT_TUPLE_LEN)
#define UT_HOT_SUCCESSOR_DATA_OFF (UT_HOT_DATA_OFF - UT_HOT_TUPLE_LEN)
#define UT_HOT_CONTENT_SHARE UINT32_C(0x01)
#define UT_HOT_BUFFER_HEADER UINT32_C(0x02)
#define UT_HOT_GRD UINT32_C(0x04)
#define UT_HOT_SLRU UINT32_C(0x08)
#define UT_HOT_FORBIDDEN_LOCKS \
	(UT_HOT_CONTENT_SHARE | UT_HOT_BUFFER_HEADER | UT_HOT_GRD | UT_HOT_SLRU)
#define UT_HOT_BUFFER 1
#define UT_HOT_LIVE_XMIN ((TransactionId)900)
#define UT_HOT_FULL_XMIN ((TransactionId)700)
#define UT_HOT_READ_SCN ((SCN)UINT64_C(0x123456))
#define UT_HOT_TABLE_OID ((Oid)4242)
#define UT_HOT_PAYLOAD ((unsigned char)0x3c)

typedef struct UtR4HotLifecycleFixture {
	char live_page[BLCKSZ] pg_attribute_aligned(MAXIMUM_ALIGNOF);
	char full_source[BLCKSZ] pg_attribute_aligned(MAXIMUM_ALIGNOF);
	BufferTag expected_tag;
	ItemPointerData expected_root;
	SCN expected_read_scn;
	char *expected_scratch_page;
	HeapTuple expected_result_tuple;
	uint32 held_locks;
	bool live_poisoned;
	bool full_source_poisoned;
	int event;
	int unlock_calls;
	int fetch_calls;
	int scratch_search_calls;
	int relock_calls;
} UtR4HotLifecycleFixture;

typedef struct UtR4HotProductFixture
{
	char live_page[BLCKSZ] pg_attribute_aligned(MAXIMUM_ALIGNOF);
	char full_source[BLCKSZ] pg_attribute_aligned(MAXIMUM_ALIGNOF);
	BufferTag expected_tag;
	HeapHotSearchResult *expected_result;
	SCN expected_read_scn;
	int lock_modes[12];
	int lock_calls;
	int fetch_calls;
	bool mutate_non_target_itl;
	bool fail_first_after_mutation;
	bool live_poisoned;
	bool full_source_poisoned;
} UtR4HotProductFixture;

static UtR4HotProductFixture *ut_hot_product_fixture;

void
cluster_ctrc_test_barrier_wait(ClusterCtrcTestBarrierPhase phase)
{
	UT_ASSERT_EQ(phase, CTRC_TEST_BARRIER_REQUESTER_REQUALIFY);
	UT_ASSERT(ut_hot_current_mx_active);
	UT_ASSERT(!ut_hot_content_lock_held);
	UT_ASSERT_EQ(ut_hot_current_mx_resolve_calls, 1);
	UT_ASSERT_NOT_NULL(ut_hot_product_fixture);
	ut_hot_requester_requalify_barrier_calls++;
	ut_hot_requester_requalify_barrier_lock_calls
		= ut_hot_product_fixture->lock_calls;
}

static HeapTupleHeader ut_r4_hot_tuple_at(Page page, OffsetNumber offnum);

BlockNumber
BufferGetBlockNumber(Buffer buffer)
{
	UT_ASSERT_EQ(buffer, UT_HOT_BUFFER);
	UT_ASSERT_NOT_NULL(ut_hot_product_fixture);
	return UT_HOT_BLOCK;
}

void
BufferGetTag(Buffer buffer, RelFileLocator *rlocator,
			 ForkNumber *forknum, BlockNumber *blocknum)
{
	UT_ASSERT_EQ(buffer, UT_HOT_BUFFER);
	UT_ASSERT_NOT_NULL(ut_hot_product_fixture);
	rlocator->spcOid = ut_hot_product_fixture->expected_tag.spcOid;
	rlocator->dbOid = ut_hot_product_fixture->expected_tag.dbOid;
	rlocator->relNumber = ut_hot_product_fixture->expected_tag.relNumber;
	*forknum = ut_hot_product_fixture->expected_tag.forkNum;
	*blocknum = ut_hot_product_fixture->expected_tag.blockNum;
}

void
LockBuffer(Buffer buffer, int mode)
{
	UtR4HotProductFixture *fixture = ut_hot_product_fixture;

	UT_ASSERT_NOT_NULL(fixture);
	if (fixture == NULL)
		return;
	if (ut_itl_pair_active)
	{
		int index;

		UT_ASSERT(buffer == (Buffer) 1 || buffer == (Buffer) 2);
		index = buffer - 1;
		UT_ASSERT_EQ(mode, BUFFER_LOCK_UNLOCK);
		UT_ASSERT(ut_itl_pair_content_lock_held[index]);
		UT_ASSERT(ut_itl_pair_lock_calls
				  < lengthof(ut_itl_pair_lock_buffers));
		ut_itl_pair_content_lock_held[index] = false;
		ut_itl_pair_lock_buffers[ut_itl_pair_lock_calls++] = buffer;
		return;
	}
	UT_ASSERT_EQ(buffer, UT_HOT_BUFFER);
	UT_ASSERT(fixture->lock_calls < lengthof(fixture->lock_modes));
	if (fixture->lock_calls < lengthof(fixture->lock_modes))
		fixture->lock_modes[fixture->lock_calls] = mode;
	fixture->lock_calls++;

	if (mode == BUFFER_LOCK_UNLOCK)
	{
		UT_ASSERT(ut_hot_content_lock_held);
		ut_hot_content_lock_held = false;
		if (ut_hot_current_mx_active && ut_hot_current_mx_one_shot)
		{
			UT_ASSERT_EQ(ut_hot_current_mx_pcm_state,
						 (uint8) PCM_STATE_READ_IMAGE);
			ut_hot_current_mx_pcm_state = (uint8) PCM_STATE_N;
			ut_hot_current_read_clear_calls++;
		}
	}
	else if (mode == BUFFER_LOCK_SHARE)
	{
		UT_ASSERT(!ut_hot_content_lock_held);
		ut_hot_content_lock_held = true;
		if (ut_hot_current_mx_active && ut_hot_current_mx_one_shot)
		{
			UT_ASSERT_EQ(ut_hot_current_mx_pcm_state, (uint8) PCM_STATE_N);
			ut_hot_current_read_bracket++;
			ut_hot_current_mx_pcm_state = (uint8) PCM_STATE_READ_IMAGE;
			ut_hot_current_read_acquire_calls++;
		}
	}
	else if (mode == BUFFER_LOCK_EXCLUSIVE)
	{
		UT_ASSERT(!ut_hot_content_lock_held);
		ut_hot_content_lock_held = true;
	}
	else
		UT_ASSERT(false);
}

ClusterCrBuildResult
cluster_gcs_block_cr_fetch_and_wait(BufferTag tag, SCN read_scn,
									char dst_page[BLCKSZ],
									ClusterCrBuildReason *reason_out)
{
	UtR4HotProductFixture *fixture = ut_hot_product_fixture;

	UT_ASSERT_NOT_NULL(fixture);
	if (fixture == NULL)
		return CLUSTER_CR_BUILD_FAIL_CLOSED;
	fixture->fetch_calls++;
	UT_ASSERT(!ut_hot_content_lock_held);
	UT_ASSERT(BufferTagsEqual(&tag, &fixture->expected_tag));
	UT_ASSERT_EQ((uint64) read_scn, (uint64) fixture->expected_read_scn);
	UT_ASSERT(dst_page == fixture->expected_result->scratch_page);
	UT_ASSERT(dst_page != fixture->live_page);
	UT_ASSERT(dst_page != fixture->full_source);

	memcpy(dst_page, fixture->full_source, BLCKSZ);
	if (fixture->fetch_calls == 1)
	{
		if (fixture->mutate_non_target_itl)
		{
			ClusterItlSlotData *non_target_slot
				= &ClusterPageGetItlSlots((Page) fixture->live_page)[0];

			non_target_slot->wrap++;
		}
		else
		{
			HeapTupleHeader live_tuple = ut_r4_hot_tuple_at(
				(Page) fixture->live_page, UT_HOT_ROOT_OFF);

			live_tuple->t_infomask2 ^= HEAP_KEYS_UPDATED;
		}
		fixture->live_poisoned = true;
		if (fixture->fail_first_after_mutation)
		{
			*reason_out = CLUSTER_CR_BUILD_IO_ERROR;
			return CLUSTER_CR_BUILD_FAIL_CLOSED;
		}
	}
	else
	{
		memset(fixture->full_source, 0x5a, BLCKSZ);
		fixture->full_source_poisoned = true;
	}
	*reason_out = CLUSTER_CR_BUILD_NONE;
	return CLUSTER_CR_BUILD_FULL;
}

static HeapTupleHeader
ut_r4_hot_tuple_at(Page page, OffsetNumber offnum)
{
	ItemId item_id = PageGetItemId(page, offnum);

	return (HeapTupleHeader)PageGetItem(page, item_id);
}

static void
ut_r4_hot_set_tuple(HeapTupleHeader tuple, TransactionId xmin,
					uint8 itl_slot_index, unsigned char payload)
{
	memset(tuple, 0, UT_HOT_TUPLE_LEN);
	HeapTupleHeaderSetXmin(tuple, xmin);
	HeapTupleHeaderSetXmax(tuple, InvalidTransactionId);
	tuple->t_infomask = HEAP_XMIN_COMMITTED | HEAP_XMAX_INVALID;
	tuple->t_infomask2 = 0;
	tuple->t_hoff = SizeofHeapTupleHeader;
	tuple->t_itl_slot_idx = itl_slot_index;
	ItemPointerSet(&tuple->t_ctid, UT_HOT_BLOCK, UT_HOT_ROOT_OFF);
	memset((char *)tuple + tuple->t_hoff, payload,
		   UT_HOT_TUPLE_LEN - tuple->t_hoff);
}

static Page
ut_r4_hot_build_page(char storage[BLCKSZ], TransactionId xmin,
					 uint8 itl_slot_index, TransactionId itl_xid,
					 uint16 itl_wrap, unsigned char payload)
{
	Page page = (Page)storage;
	PageHeader header;
	ItemId item_id;
	ClusterItlSlotData *slot;

	memset(storage, 0, BLCKSZ);
	header = (PageHeader)page;
	header->pd_flags = PD_HAS_ITL;
	header->pd_special = (LocationIndex)(BLCKSZ - CLUSTER_ITL_SPECIAL_SIZE);
	header->pd_pagesize_version = BLCKSZ | PG_PAGE_LAYOUT_VERSION;
	header->pd_lower = SizeOfPageHeaderData + sizeof(ItemIdData);
	header->pd_upper = (LocationIndex)UT_HOT_DATA_OFF;
	item_id = PageGetItemId(page, UT_HOT_ROOT_OFF);
	ItemIdSetNormal(item_id, UT_HOT_DATA_OFF, UT_HOT_TUPLE_LEN);
	ut_r4_hot_set_tuple((HeapTupleHeader)(storage + UT_HOT_DATA_OFF),
						xmin, itl_slot_index, payload);
	slot = &ClusterPageGetItlSlots(page)[itl_slot_index];
	slot->xid = itl_xid;
	slot->wrap = itl_wrap;
	slot->flags = ITL_FLAG_ACTIVE;
	return page;
}

static void
ut_r4_hot_build_foreign_multixact_chain(char storage[BLCKSZ])
{
	Page page = (Page) storage;
	PageHeader header;
	HeapTupleHeader root;
	HeapTupleHeader successor;
	ClusterItlSlotData *slot;

	memset(storage, 0, BLCKSZ);
	header = (PageHeader) page;
	header->pd_flags = PD_HAS_ITL;
	header->pd_special = (LocationIndex) (BLCKSZ - CLUSTER_ITL_SPECIAL_SIZE);
	header->pd_pagesize_version = BLCKSZ | PG_PAGE_LAYOUT_VERSION;
	header->pd_lower = SizeOfPageHeaderData + 2 * sizeof(ItemIdData);
	header->pd_upper = (LocationIndex) UT_HOT_SUCCESSOR_DATA_OFF;
	ItemIdSetNormal(PageGetItemId(page, UT_HOT_ROOT_OFF),
					UT_HOT_DATA_OFF, UT_HOT_TUPLE_LEN);
	ItemIdSetNormal(PageGetItemId(page, UT_HOT_SUCCESSOR_OFF),
					UT_HOT_SUCCESSOR_DATA_OFF, UT_HOT_TUPLE_LEN);

	root = (HeapTupleHeader) (storage + UT_HOT_DATA_OFF);
	ut_r4_hot_set_tuple(root, UT_HOT_FULL_XMIN, 2, 0x41);
	HeapTupleHeaderSetXmax(root, UT_HOT_FOREIGN_MXID);
	root->t_infomask = HEAP_XMIN_COMMITTED | HEAP_XMAX_IS_MULTI
					   | HEAP_XMAX_EXCL_LOCK;
	root->t_infomask2 = HEAP_HOT_UPDATED;
	ItemPointerSet(&root->t_ctid, UT_HOT_BLOCK, UT_HOT_SUCCESSOR_OFF);

	successor = (HeapTupleHeader) (storage + UT_HOT_SUCCESSOR_DATA_OFF);
	ut_r4_hot_set_tuple(successor, UT_HOT_AUTH_UPDATER, 3, 0x42);
	successor->t_infomask2 = HEAP_ONLY_TUPLE;
	ItemPointerSet(&successor->t_ctid, UT_HOT_BLOCK, UT_HOT_SUCCESSOR_OFF);

	slot = &ClusterPageGetItlSlots(page)[2];
	slot->xid = UT_HOT_FULL_XMIN;
	slot->wrap = 4;
	slot->flags = ITL_FLAG_COMMITTED;
	slot = &ClusterPageGetItlSlots(page)[3];
	slot->xid = UT_HOT_AUTH_UPDATER;
	slot->wrap = 8;
	slot->flags = ITL_FLAG_ACTIVE;
	slot->undo_segment_head = uba_encode(258, 2, 7, 0);
}

static void
ut_r4_hot_init_product_fixture(UtR4HotProductFixture *fixture,
								HeapHotSearchResult *result)
{
	RelFileLocator locator = {
		.spcOid = 1663,
		.dbOid = 5,
		.relNumber = 9001,
	};
	HeapTupleHeader full_tuple;

	memset(fixture, 0, sizeof(*fixture));
	memset(result, 0, sizeof(*result));
	(void) ut_r4_hot_build_page(fixture->live_page, UT_HOT_LIVE_XMIN, 2,
									UT_HOT_LIVE_XMIN, 7, 0x77);
	(void) ut_r4_hot_build_page(fixture->full_source, UT_HOT_FULL_XMIN, 1,
									UT_HOT_FULL_XMIN, 4, UT_HOT_PAYLOAD);
	full_tuple = ut_r4_hot_tuple_at((Page) fixture->full_source,
									UT_HOT_ROOT_OFF);
	full_tuple->t_infomask = HEAP_XMAX_INVALID;
	PageSetLSN((Page) fixture->full_source, UINT64_C(0x123450));
	InitBufferTag(&fixture->expected_tag, &locator, MAIN_FORKNUM, UT_HOT_BLOCK);
	fixture->expected_result = result;
	fixture->expected_read_scn = UT_HOT_READ_SCN;

	memset(&ut_hot_live_ref, 0, sizeof(ut_hot_live_ref));
	ut_hot_live_ref.origin_node_id = 1;
	ut_hot_live_ref.undo_segment_id = 257;
	ut_hot_live_ref.tt_slot_id = 7;
	ut_hot_live_ref.cluster_epoch = 9;
	ut_hot_live_ref.local_xid = UT_HOT_LIVE_XMIN + 1;
	ut_hot_live_ref_page = (Page) fixture->live_page;
	ut_hot_live_ref_calls = 0;
	ut_hot_product_fixture = fixture;
	ut_hot_content_lock_held = true;
	ut_hot_production_core_active = true;
	ut_hot_r4_target_reachable = true;
	BufferBlocks = fixture->live_page;
}

static void
ut_r4_hot_poison_live(UtR4HotLifecycleFixture *fixture)
{
	memset(fixture->live_page, 0xa5, BLCKSZ);
	fixture->live_poisoned = true;
}

static void
ut_r4_hot_content_share(void *arg, bool acquire)
{
	UtR4HotLifecycleFixture *fixture = (UtR4HotLifecycleFixture *)arg;

	if (acquire)
	{
		UT_ASSERT_EQ(fixture->event, 3);
		UT_ASSERT_EQ(fixture->held_locks & UT_HOT_FORBIDDEN_LOCKS, 0);
		ut_r4_hot_poison_live(fixture);
		fixture->held_locks |= UT_HOT_CONTENT_SHARE;
		fixture->relock_calls++;
		fixture->event = 4;
	}
	else
	{
		UT_ASSERT_EQ(fixture->event, 0);
		UT_ASSERT_EQ(fixture->held_locks & UT_HOT_FORBIDDEN_LOCKS,
					 UT_HOT_CONTENT_SHARE);
		fixture->held_locks &= ~UT_HOT_CONTENT_SHARE;
		fixture->unlock_calls++;
		fixture->event = 1;
	}
}

static bool
ut_r4_hot_fetch_full(void *arg, const BufferTag *tag, SCN read_scn,
					 char dst_page[BLCKSZ])
{
	UtR4HotLifecycleFixture *fixture = (UtR4HotLifecycleFixture *)arg;

	UT_ASSERT_EQ(fixture->event, 1);
	fixture->fetch_calls++;
	UT_ASSERT_EQ(fixture->held_locks & UT_HOT_FORBIDDEN_LOCKS, 0);
	UT_ASSERT(BufferTagsEqual(tag, &fixture->expected_tag));
	UT_ASSERT_EQ((uint64)read_scn, (uint64)fixture->expected_read_scn);
	UT_ASSERT(dst_page == fixture->expected_scratch_page);
	UT_ASSERT(dst_page != fixture->full_source);
	UT_ASSERT(dst_page != fixture->live_page);

	memcpy(dst_page, fixture->full_source, BLCKSZ);
	memset(fixture->full_source, 0x5a, BLCKSZ);
	fixture->full_source_poisoned = true;
	ut_r4_hot_poison_live(fixture);
	fixture->event = 2;
	return true;
}

static bool
ut_r4_hot_search_scratch(void *arg,
						 const ClusterR4HotScratchTestContext *context,
						 HeapTuple scratch_tuple)
{
	UtR4HotLifecycleFixture *fixture = (UtR4HotLifecycleFixture *)arg;
	ItemId item_id;

	UT_ASSERT_EQ(fixture->event, 2);
	UT_ASSERT_EQ(fixture->held_locks & UT_HOT_FORBIDDEN_LOCKS, 0);
	UT_ASSERT(fixture->live_poisoned);
	UT_ASSERT(fixture->full_source_poisoned);
	UT_ASSERT(context->scratch_page == (Page)fixture->expected_scratch_page);
	UT_ASSERT(BufferTagsEqual(&context->tag, &fixture->expected_tag));
	UT_ASSERT_EQ(ItemPointerGetBlockNumber(&context->logical_root),
				 UT_HOT_BLOCK);
	UT_ASSERT_EQ(ItemPointerGetOffsetNumber(&context->logical_root),
				 UT_HOT_ROOT_OFF);
	UT_ASSERT_EQ((uint64)context->read_scn,
				 (uint64)fixture->expected_read_scn);
	UT_ASSERT(context->already_full);
	UT_ASSERT(!context->allow_hint);
	UT_ASSERT(!context->allow_cleanout);
	UT_ASSERT(scratch_tuple == fixture->expected_result_tuple);

	item_id = PageGetItemId(context->scratch_page, UT_HOT_ROOT_OFF);
	UT_ASSERT(ItemIdIsNormal(item_id));
	scratch_tuple->t_data =
		(HeapTupleHeader)PageGetItem(context->scratch_page, item_id);
	scratch_tuple->t_len = ItemIdGetLength(item_id);
	ItemPointerSet(&scratch_tuple->t_self, UT_HOT_BLOCK, UT_HOT_ROOT_OFF);
	scratch_tuple->t_tableOid = UT_HOT_TABLE_OID;
	UT_ASSERT_EQ(HeapTupleHeaderGetRawXmin(scratch_tuple->t_data),
				 UT_HOT_FULL_XMIN);
	UT_ASSERT_EQ(*((unsigned char *)scratch_tuple->t_data +
					 scratch_tuple->t_data->t_hoff), UT_HOT_PAYLOAD);

	fixture->scratch_search_calls++;
	fixture->event = 3;
	return true;
}

static void
ut_r4_hot_init_fixture(UtR4HotLifecycleFixture *fixture,
					   HeapHotSearchResult *result)
{
	RelFileLocator locator = {
		.spcOid = 1663,
		.dbOid = 5,
		.relNumber = 9001,
	};

	memset(fixture, 0, sizeof(*fixture));
	memset(result, 0, sizeof(*result));
	(void)ut_r4_hot_build_page(fixture->live_page, UT_HOT_LIVE_XMIN, 2,
							 UT_HOT_LIVE_XMIN, 7, 0x77);
	(void)ut_r4_hot_build_page(fixture->full_source, UT_HOT_FULL_XMIN, 1,
							 UT_HOT_FULL_XMIN, 4, UT_HOT_PAYLOAD);
	InitBufferTag(&fixture->expected_tag, &locator, MAIN_FORKNUM, UT_HOT_BLOCK);
	ItemPointerSet(&fixture->expected_root, UT_HOT_BLOCK, UT_HOT_ROOT_OFF);
	fixture->expected_read_scn = UT_HOT_READ_SCN;
	fixture->expected_scratch_page = result->scratch_page;
	fixture->expected_result_tuple = &result->tuple;
	fixture->held_locks = UT_HOT_CONTENT_SHARE;
}

static TupleTableSlot *
ut_r4_hot_make_slot(TupleDescData *tuple_desc)
{
	memset(tuple_desc, 0, sizeof(*tuple_desc));
	tuple_desc->natts = 0;
	tuple_desc->tdrefcount = -1;
	return MakeSingleTupleTableSlot(tuple_desc, &TTSOpsBufferHeapTuple);
}

static void
ut_r4_hot_reset_resources(void)
{
	memset(ut_allocations, 0, sizeof(ut_allocations));
	ut_alloc_calls = 0;
	ut_free_calls = 0;
	ut_invalid_free_calls = 0;
	ut_buffer_incr_calls = 0;
	ut_buffer_release_calls = 0;
}

UT_TEST(test_33_buffer_backed_result_keeps_real_buffer_pin)
{
	HeapHotSearchResult result;
	TupleDescData tuple_desc;
	TupleTableSlot *slot;
	BufferHeapTupleTableSlot *buffer_slot;
	char live_page[BLCKSZ] pg_attribute_aligned(MAXIMUM_ALIGNOF);
	HeapTupleHeader live_tuple_data;
	bool call_again = true;
	bool all_dead = true;
	int frees_before_clear;
	TableIndexFetchTupleResult stored;

	ut_r4_hot_reset_resources();
	memset(&result, 0, sizeof(result));
	(void)ut_r4_hot_build_page(live_page, UT_HOT_LIVE_XMIN, 2,
							 UT_HOT_LIVE_XMIN, 7, 0x77);
	result.kind = HEAP_HOT_SEARCH_BUFFER_BACKED;
	result.tuple.t_data = ut_r4_hot_tuple_at((Page)live_page, UT_HOT_ROOT_OFF);
	result.tuple.t_len = UT_HOT_TUPLE_LEN;
	ItemPointerSet(&result.tuple.t_self, UT_HOT_BLOCK, UT_HOT_ROOT_OFF);
	result.tuple.t_tableOid = UT_HOT_TABLE_OID;
	live_tuple_data = result.tuple.t_data;
	slot = ut_r4_hot_make_slot(&tuple_desc);
	buffer_slot = (BufferHeapTupleTableSlot *)slot;

	stored = cluster_heap_test_r4_store_hot_result(
		&result, slot, UT_HOT_BUFFER, &call_again, &all_dead);
	UT_ASSERT_EQ(stored, TABLE_INDEX_FETCH_FOUND);
	UT_ASSERT(!call_again);
	UT_ASSERT(!all_dead);
	UT_ASSERT(buffer_slot->base.tuple == &buffer_slot->base.tupdata);
	UT_ASSERT(buffer_slot->base.tuple != &result.tuple);
	UT_ASSERT(buffer_slot->base.tupdata.t_data == live_tuple_data);
	UT_ASSERT_EQ(buffer_slot->base.tupdata.t_len, UT_HOT_TUPLE_LEN);
	UT_ASSERT_EQ(buffer_slot->buffer, UT_HOT_BUFFER);
	UT_ASSERT(!TTS_SHOULDFREE(slot));
	UT_ASSERT_EQ(ut_buffer_incr_calls, 1);
	UT_ASSERT_EQ(ut_buffer_release_calls, 0);
	UT_ASSERT_EQ(ItemPointerGetBlockNumber(&slot->tts_tid), UT_HOT_BLOCK);
	UT_ASSERT_EQ(ItemPointerGetOffsetNumber(&slot->tts_tid), UT_HOT_ROOT_OFF);
	UT_ASSERT_EQ(slot->tts_tableOid, UT_HOT_TABLE_OID);

	/* The slot owns its embedded descriptor; the page bytes remain pin-backed. */
	memset(&result.tuple, 0, sizeof(result.tuple));
	UT_ASSERT(buffer_slot->base.tuple == &buffer_slot->base.tupdata);
	UT_ASSERT(buffer_slot->base.tupdata.t_data == live_tuple_data);
	UT_ASSERT_EQ(HeapTupleHeaderGetRawXmin(buffer_slot->base.tupdata.t_data),
				 UT_HOT_LIVE_XMIN);
	UT_ASSERT_EQ(*((unsigned char *)buffer_slot->base.tupdata.t_data +
					 buffer_slot->base.tupdata.t_data->t_hoff), 0x77);
	UT_ASSERT_EQ(ItemPointerGetBlockNumber(&buffer_slot->base.tupdata.t_self),
				 UT_HOT_BLOCK);
	UT_ASSERT_EQ(ItemPointerGetOffsetNumber(&buffer_slot->base.tupdata.t_self),
				 UT_HOT_ROOT_OFF);
	UT_ASSERT_EQ(buffer_slot->base.tupdata.t_tableOid, UT_HOT_TABLE_OID);

	frees_before_clear = ut_free_calls;
	ExecClearTuple(slot);
	UT_ASSERT_EQ(ut_free_calls, frees_before_clear);
	UT_ASSERT_EQ(ut_buffer_release_calls, 1);
	UT_ASSERT_EQ(buffer_slot->buffer, InvalidBuffer);
	ExecDropSingleTupleTableSlot(slot);
	UT_ASSERT_EQ(ut_alloc_calls, ut_free_calls);
	UT_ASSERT_EQ(ut_invalid_free_calls, 0);
}

UT_TEST(test_34_owned_scratch_result_survives_source_and_live_poison)
{
	UtR4HotLifecycleFixture fixture;
	HeapHotSearchResult result;
	TupleDescData tuple_desc;
	TupleTableSlot *slot;
	BufferHeapTupleTableSlot *buffer_slot;
	HeapTuple owned_tuple;
	bool call_again = true;
	bool all_dead = true;
	int frees_before_clear;
	int releases_before_clear;
	uintptr_t owned_data_addr;
	uintptr_t scratch_begin;
	uintptr_t scratch_end;
	HeapHotSearchResultKind kind;
	TableIndexFetchTupleResult stored;

	ut_r4_hot_reset_resources();
	ut_r4_hot_init_fixture(&fixture, &result);

	kind = cluster_heap_test_r4_hot_full_cycle(
		fixture.expected_tag, fixture.expected_root, fixture.expected_read_scn,
		&result, ut_r4_hot_content_share, ut_r4_hot_fetch_full,
		ut_r4_hot_search_scratch, &fixture, &call_again, &all_dead);
	UT_ASSERT_EQ(kind, HEAP_HOT_SEARCH_OWNED_SCRATCH);
	UT_ASSERT_EQ(result.kind, HEAP_HOT_SEARCH_OWNED_SCRATCH);
	UT_ASSERT(!call_again);
	UT_ASSERT(!all_dead);
	UT_ASSERT_EQ(fixture.event, 4);
	UT_ASSERT_EQ(fixture.unlock_calls, 1);
	UT_ASSERT_EQ(fixture.fetch_calls, 1);
	UT_ASSERT_EQ(fixture.scratch_search_calls, 1);
	UT_ASSERT_EQ(fixture.relock_calls, 1);
	UT_ASSERT_EQ(fixture.held_locks & UT_HOT_FORBIDDEN_LOCKS,
				 UT_HOT_CONTENT_SHARE);
	scratch_begin = (uintptr_t)result.scratch_page;
	scratch_end = scratch_begin + BLCKSZ;
	UT_ASSERT((uintptr_t)result.tuple.t_data >= scratch_begin);
	UT_ASSERT((uintptr_t)result.tuple.t_data + result.tuple.t_len <= scratch_end);

	slot = ut_r4_hot_make_slot(&tuple_desc);
	buffer_slot = (BufferHeapTupleTableSlot *)slot;
	call_again = true;
	all_dead = true;
	stored = cluster_heap_test_r4_store_hot_result(
		&result, slot, UT_HOT_BUFFER, &call_again, &all_dead);
	UT_ASSERT_EQ(stored, TABLE_INDEX_FETCH_FOUND);
	UT_ASSERT(!call_again);
	UT_ASSERT(!all_dead);
	UT_ASSERT_EQ(buffer_slot->buffer, InvalidBuffer);
	UT_ASSERT(TTS_SHOULDFREE(slot));
	UT_ASSERT_EQ(ut_buffer_incr_calls, 0);
	UT_ASSERT_EQ(ut_buffer_release_calls, 0);
	UT_ASSERT_EQ(ut_invalid_free_calls, 0);
	owned_tuple = buffer_slot->base.tuple;
	UT_ASSERT_NOT_NULL(owned_tuple);
	if (owned_tuple != NULL)
	{
		owned_data_addr = (uintptr_t)owned_tuple->t_data;
		UT_ASSERT(owned_tuple != &result.tuple);
		UT_ASSERT(owned_data_addr < scratch_begin || owned_data_addr >= scratch_end);
		UT_ASSERT_EQ(ItemPointerGetBlockNumber(&owned_tuple->t_self), UT_HOT_BLOCK);
		UT_ASSERT_EQ(ItemPointerGetOffsetNumber(&owned_tuple->t_self),
					 UT_HOT_ROOT_OFF);
		UT_ASSERT_EQ(owned_tuple->t_tableOid, UT_HOT_TABLE_OID);
	}
	UT_ASSERT_EQ(ItemPointerGetBlockNumber(&slot->tts_tid), UT_HOT_BLOCK);
	UT_ASSERT_EQ(ItemPointerGetOffsetNumber(&slot->tts_tid), UT_HOT_ROOT_OFF);
	UT_ASSERT_EQ(slot->tts_tableOid, UT_HOT_TABLE_OID);

	memset(result.scratch_page, 0x00, BLCKSZ);
	memset(fixture.live_page, 0x00, BLCKSZ);
	memset(fixture.full_source, 0x00, BLCKSZ);
	if (owned_tuple != NULL)
	{
		UT_ASSERT_EQ(HeapTupleHeaderGetRawXmin(owned_tuple->t_data),
					 UT_HOT_FULL_XMIN);
		UT_ASSERT_EQ(*((unsigned char *)owned_tuple->t_data +
						 owned_tuple->t_data->t_hoff), UT_HOT_PAYLOAD);
	}

	frees_before_clear = ut_free_calls;
	releases_before_clear = ut_buffer_release_calls;
	ExecClearTuple(slot);
	UT_ASSERT_EQ(ut_free_calls, frees_before_clear + 1);
	UT_ASSERT_EQ(ut_buffer_release_calls, releases_before_clear);
	UT_ASSERT(!TTS_SHOULDFREE(slot));
	UT_ASSERT_EQ(buffer_slot->buffer, InvalidBuffer);
	UT_ASSERT(TTS_EMPTY(slot));
	ExecDropSingleTupleTableSlot(slot);
	UT_ASSERT_EQ(ut_alloc_calls, ut_free_calls);
	UT_ASSERT_EQ(ut_invalid_free_calls, 0);
}

static void
ut_r4_hot_reset_scratch_authority(Page scratch_page, Page forbidden_live_page,
								  SCN read_scn, XLogRecPtr page_lsn)
{
	memset(&ut_scratch_expected_ref, 0, sizeof(ut_scratch_expected_ref));
	ut_scratch_expected_page = scratch_page;
	ut_scratch_forbidden_live_page = forbidden_live_page;
	ut_scratch_expected_ref.origin_node_id = 1;
	ut_scratch_expected_ref.undo_segment_id = 257;
	ut_scratch_expected_ref.tt_slot_id = 4;
	ut_scratch_expected_ref.cluster_epoch = 9;
	ut_scratch_expected_ref.local_xid = UT_HOT_FULL_XMIN;
	ut_scratch_expected_xid = UT_HOT_FULL_XMIN;
	ut_scratch_expected_read_scn = read_scn;
	ut_scratch_expected_lsn = page_lsn;
	ut_scratch_ref_available = true;
	ut_scratch_resolve_evidence = CLUSTER_VIS_EVIDENCE_REMOTE;
	ut_scratch_resolve_status = CLUSTER_TT_STATUS_COMMITTED;
	ut_scratch_resolve_scn = read_scn - 1;
	ut_scratch_ref_calls = 0;
	ut_scratch_exact_resolve_calls = 0;
	ut_scratch_live_resolve_calls = 0;
	ut_scratch_cr_calls = 0;
	ut_scratch_ssi_calls = 0;
	ut_scratch_hint_calls = 0;
	ut_scratch_dirty_calls = 0;
	ut_live_visibility_calls = 0;
	ut_live_visible_offnum = InvalidOffsetNumber;
}

UT_TEST(test_35_scratch_mvcc_uses_exact_ref_without_hints_or_live_page)
{
	char scratch_storage[BLCKSZ] pg_attribute_aligned(MAXIMUM_ALIGNOF);
	char live_storage[BLCKSZ] pg_attribute_aligned(MAXIMUM_ALIGNOF);
	char scratch_before[BLCKSZ] pg_attribute_aligned(MAXIMUM_ALIGNOF);
	Page scratch_page;
	Page live_page;
	HeapTupleData tuple;
	SnapshotData snapshot;
	ClusterR4HotScratchTestContext context;
	HeapTupleHeader tuple_header;
	XLogRecPtr page_lsn = UINT64_C(0x123450);
	bool visible;

	scratch_page = ut_r4_hot_build_page(scratch_storage, UT_HOT_FULL_XMIN, 1,
									 UT_HOT_FULL_XMIN, 4, UT_HOT_PAYLOAD);
	live_page = ut_r4_hot_build_page(live_storage, UT_HOT_LIVE_XMIN, 2,
								  UT_HOT_LIVE_XMIN, 7, 0x77);
	tuple_header = ut_r4_hot_tuple_at(scratch_page, UT_HOT_ROOT_OFF);
	/* No xmin hint: the exact resolver, never native CLOG/hinting, decides. */
	tuple_header->t_infomask = HEAP_XMAX_INVALID;
	PageSetLSN(scratch_page, page_lsn);
	memcpy(scratch_before, scratch_storage, BLCKSZ);

	memset(&tuple, 0, sizeof(tuple));
	tuple.t_data = tuple_header;
	tuple.t_len = UT_HOT_TUPLE_LEN;
	ItemPointerSet(&tuple.t_self, UT_HOT_BLOCK, UT_HOT_ROOT_OFF);
	tuple.t_tableOid = UT_HOT_TABLE_OID;
	memset(&snapshot, 0, sizeof(snapshot));
	snapshot.snapshot_type = SNAPSHOT_MVCC;
	snapshot.read_scn = UT_HOT_READ_SCN;
	snapshot.read_epoch = 9;
	snapshot.cluster_source = SNAPSHOT_SOURCE_CLUSTER;
	memset(&context, 0, sizeof(context));
	context.scratch_page = scratch_page;
	context.tag.blockNum = UT_HOT_BLOCK;
	context.logical_root = tuple.t_self;
	context.read_scn = snapshot.read_scn;
	context.already_full = true;
	context.allow_hint = false;
	context.allow_cleanout = false;

	ut_r4_hot_reset_scratch_authority(scratch_page, live_page,
									  snapshot.read_scn, page_lsn);
	visible = HeapTupleSatisfiesMVCCScratch(&tuple, &snapshot, &context);
	UT_ASSERT(visible);
	UT_ASSERT_EQ(ut_scratch_ref_calls, 1);
	UT_ASSERT_EQ(ut_scratch_exact_resolve_calls, 1);
	UT_ASSERT_EQ(ut_scratch_live_resolve_calls, 0);
	UT_ASSERT_EQ(ut_scratch_cr_calls, 0);
	UT_ASSERT_EQ(ut_scratch_ssi_calls, 0);
	UT_ASSERT(memcmp(scratch_storage, scratch_before, BLCKSZ) == 0);

	ut_scratch_resolve_scn = snapshot.read_scn + 1;
	ut_scratch_ref_calls = 0;
	ut_scratch_exact_resolve_calls = 0;
	visible = HeapTupleSatisfiesMVCCScratch(&tuple, &snapshot, &context);
	UT_ASSERT(!visible);
	UT_ASSERT_EQ(ut_scratch_ref_calls, 1);
	UT_ASSERT_EQ(ut_scratch_exact_resolve_calls, 1);
	UT_ASSERT_EQ(ut_scratch_live_resolve_calls, 0);
	UT_ASSERT_EQ(ut_scratch_cr_calls, 0);
	UT_ASSERT_EQ(ut_scratch_ssi_calls, 0);
	UT_ASSERT(memcmp(scratch_storage, scratch_before, BLCKSZ) == 0);

	ut_scratch_resolve_status = CLUSTER_TT_STATUS_ABORTED;
	ut_scratch_resolve_scn = InvalidScn;
	ut_scratch_ref_calls = 0;
	ut_scratch_exact_resolve_calls = 0;
	visible = HeapTupleSatisfiesMVCCScratch(&tuple, &snapshot, &context);
	UT_ASSERT(!visible);
	UT_ASSERT_EQ(ut_scratch_ref_calls, 1);
	UT_ASSERT_EQ(ut_scratch_exact_resolve_calls, 1);
	UT_ASSERT_EQ(ut_scratch_live_resolve_calls, 0);
	UT_ASSERT_EQ(ut_scratch_cr_calls, 0);
	UT_ASSERT_EQ(ut_scratch_ssi_calls, 0);
	UT_ASSERT(memcmp(scratch_storage, scratch_before, BLCKSZ) == 0);
}

UT_TEST(test_36_production_hot_core_full_result_is_owned)
{
	UtR4HotProductFixture fixture;
	HeapHotSearchResult result;
	RelationData relation;
	FormData_pg_class relation_form;
	SnapshotData snapshot;
	ItemPointerData tid;
	TupleDescData tuple_desc;
	TupleTableSlot *slot;
	BufferHeapTupleTableSlot *buffer_slot;
	HeapTuple owned_tuple;
	TableIndexFetchTupleResult stored;
	bool call_again = false;
	bool all_dead = true;
	int frees_before_clear;
	int releases_before_clear;
	uintptr_t scratch_begin;
	uintptr_t scratch_end;

	ut_r4_hot_reset_resources();
	ut_r4_hot_init_product_fixture(&fixture, &result);
	ut_r4_hot_reset_scratch_authority((Page) result.scratch_page,
									  (Page) fixture.live_page,
									  UT_HOT_READ_SCN, UINT64_C(0x123450));

	memset(&relation, 0, sizeof(relation));
	memset(&relation_form, 0, sizeof(relation_form));
	relation.rd_id = UT_HOT_TABLE_OID;
	relation.rd_rel = &relation_form;
	relation_form.relpersistence = RELPERSISTENCE_PERMANENT;
	memset(&snapshot, 0, sizeof(snapshot));
	snapshot.snapshot_type = SNAPSHOT_MVCC;
	snapshot.read_scn = UT_HOT_READ_SCN;
	snapshot.read_epoch = 9;
	snapshot.cluster_source = SNAPSHOT_SOURCE_CLUSTER;
	ItemPointerSet(&tid, UT_HOT_BLOCK, UT_HOT_ROOT_OFF);
	slot = ut_r4_hot_make_slot(&tuple_desc);
	buffer_slot = (BufferHeapTupleTableSlot *) slot;

	stored = cluster_heap_test_r4_index_hot_result(
		&tid, &relation, UT_HOT_BUFFER, &snapshot, &result, slot,
		&call_again, &all_dead);
	UT_ASSERT_EQ(stored, TABLE_INDEX_FETCH_FOUND);
	UT_ASSERT_EQ(result.kind, HEAP_HOT_SEARCH_OWNED_SCRATCH);
	UT_ASSERT(!call_again);
	UT_ASSERT(!all_dead);
	UT_ASSERT_EQ(fixture.lock_calls, 5);
	UT_ASSERT_EQ(fixture.lock_modes[0], BUFFER_LOCK_UNLOCK);
	UT_ASSERT_EQ(fixture.lock_modes[1], BUFFER_LOCK_SHARE);
	UT_ASSERT_EQ(fixture.lock_modes[2], BUFFER_LOCK_UNLOCK);
	UT_ASSERT_EQ(fixture.lock_modes[3], BUFFER_LOCK_SHARE);
	UT_ASSERT_EQ(fixture.lock_modes[4], BUFFER_LOCK_UNLOCK);
	UT_ASSERT(!ut_hot_content_lock_held);
	UT_ASSERT_EQ(fixture.fetch_calls, 2);
	UT_ASSERT(fixture.live_poisoned);
	UT_ASSERT(fixture.full_source_poisoned);
	UT_ASSERT_EQ(ut_hot_live_ref_calls, 2);
	UT_ASSERT_EQ(ut_scratch_ref_calls, 2);
	UT_ASSERT_EQ(ut_scratch_exact_resolve_calls, 2);
	UT_ASSERT_EQ(ut_live_visibility_calls, 0);
	UT_ASSERT_EQ(ut_scratch_live_resolve_calls, 0);
	UT_ASSERT_EQ(ut_scratch_cr_calls, 0);
	UT_ASSERT_EQ(ut_scratch_ssi_calls, 0);
	UT_ASSERT_EQ(ut_scratch_hint_calls, 0);
	UT_ASSERT_EQ(ut_scratch_dirty_calls, 0);

	scratch_begin = (uintptr_t) result.scratch_page;
	scratch_end = scratch_begin + BLCKSZ;
	UT_ASSERT((uintptr_t) result.tuple.t_data >= scratch_begin);
	UT_ASSERT((uintptr_t) result.tuple.t_data + result.tuple.t_len <= scratch_end);
	UT_ASSERT_EQ(buffer_slot->buffer, InvalidBuffer);
	UT_ASSERT(TTS_SHOULDFREE(slot));
	UT_ASSERT_EQ(ut_buffer_incr_calls, 0);
	UT_ASSERT_EQ(ut_buffer_release_calls, 0);
	owned_tuple = buffer_slot->base.tuple;
	UT_ASSERT_NOT_NULL(owned_tuple);
	if (owned_tuple != NULL)
	{
		UT_ASSERT(owned_tuple != &result.tuple);
		UT_ASSERT((uintptr_t) owned_tuple->t_data < scratch_begin
				  || (uintptr_t) owned_tuple->t_data >= scratch_end);
		UT_ASSERT_EQ(ItemPointerGetBlockNumber(&owned_tuple->t_self),
					 UT_HOT_BLOCK);
		UT_ASSERT_EQ(ItemPointerGetOffsetNumber(&owned_tuple->t_self),
					 UT_HOT_ROOT_OFF);
		UT_ASSERT_EQ(owned_tuple->t_tableOid, UT_HOT_TABLE_OID);
	}
	UT_ASSERT_EQ(ItemPointerGetBlockNumber(&slot->tts_tid), UT_HOT_BLOCK);
	UT_ASSERT_EQ(ItemPointerGetOffsetNumber(&slot->tts_tid), UT_HOT_ROOT_OFF);
	UT_ASSERT_EQ(slot->tts_tableOid, UT_HOT_TABLE_OID);

	memset(result.scratch_page, 0, BLCKSZ);
	memset(fixture.live_page, 0, BLCKSZ);
	memset(fixture.full_source, 0, BLCKSZ);
	if (owned_tuple != NULL)
	{
		UT_ASSERT_EQ(HeapTupleHeaderGetRawXmin(owned_tuple->t_data),
					 UT_HOT_FULL_XMIN);
		UT_ASSERT_EQ(*((unsigned char *) owned_tuple->t_data
						 + owned_tuple->t_data->t_hoff), UT_HOT_PAYLOAD);
	}

	ut_hot_production_core_active = false;
	ut_hot_product_fixture = NULL;
	ut_hot_live_ref_page = NULL;
	BufferBlocks = NULL;
	frees_before_clear = ut_free_calls;
	releases_before_clear = ut_buffer_release_calls;
	ExecClearTuple(slot);
	UT_ASSERT_EQ(ut_free_calls, frees_before_clear + 1);
	UT_ASSERT_EQ(ut_buffer_release_calls, releases_before_clear);
	UT_ASSERT(!TTS_SHOULDFREE(slot));
	UT_ASSERT_EQ(buffer_slot->buffer, InvalidBuffer);
	UT_ASSERT(TTS_EMPTY(slot));
	ExecDropSingleTupleTableSlot(slot);
	UT_ASSERT_EQ(ut_alloc_calls, ut_free_calls);
	UT_ASSERT_EQ(ut_invalid_free_calls, 0);
}

UT_TEST(test_37_full_input_recheck_catches_non_target_itl_with_stable_tuple_and_lsn)
{
	UtR4HotProductFixture fixture;
	HeapHotSearchResult result;
	RelationData relation;
	FormData_pg_class relation_form;
	SnapshotData snapshot;
	ItemPointerData tid;
	HeapHotSearchResultKind kind;
	ClusterItlSlotData non_target_before;
	char target_before[UT_HOT_TUPLE_LEN];
	XLogRecPtr stable_lsn = (XLogRecPtr) UINT64_C(0x445566);

	ut_r4_hot_init_product_fixture(&fixture, &result);
	fixture.mutate_non_target_itl = true;
	PageSetLSN((Page) fixture.live_page, stable_lsn);
	memcpy(target_before,
		   ut_r4_hot_tuple_at((Page) fixture.live_page, UT_HOT_ROOT_OFF),
		   sizeof(target_before));
	non_target_before = ClusterPageGetItlSlots((Page) fixture.live_page)[0];
	ut_r4_hot_reset_scratch_authority((Page) result.scratch_page,
									  (Page) fixture.live_page,
									  UT_HOT_READ_SCN, UINT64_C(0x123450));

	memset(&relation, 0, sizeof(relation));
	memset(&relation_form, 0, sizeof(relation_form));
	relation.rd_id = UT_HOT_TABLE_OID;
	relation.rd_rel = &relation_form;
	relation_form.relpersistence = RELPERSISTENCE_PERMANENT;
	memset(&snapshot, 0, sizeof(snapshot));
	snapshot.snapshot_type = SNAPSHOT_MVCC;
	snapshot.read_scn = UT_HOT_READ_SCN;
	snapshot.read_epoch = 9;
	snapshot.cluster_source = SNAPSHOT_SOURCE_CLUSTER;
	ItemPointerSet(&tid, UT_HOT_BLOCK, UT_HOT_ROOT_OFF);

	kind = heap_hot_search_buffer_result(&tid, &relation, UT_HOT_BUFFER,
										&snapshot, &result, NULL, true);
	UT_ASSERT_EQ(kind, HEAP_HOT_SEARCH_OWNED_SCRATCH);
	UT_ASSERT_EQ(fixture.fetch_calls, 2);
	UT_ASSERT_EQ(fixture.lock_calls, 4);
	UT_ASSERT(fixture.live_poisoned);
	UT_ASSERT_EQ(PageGetLSN((Page) fixture.live_page), stable_lsn);
	UT_ASSERT_EQ(memcmp(target_before,
					ut_r4_hot_tuple_at((Page) fixture.live_page,
										UT_HOT_ROOT_OFF),
					sizeof(target_before)), 0);
	UT_ASSERT(ClusterPageGetItlSlots((Page) fixture.live_page)[0].wrap
			  != non_target_before.wrap);
	UT_ASSERT_EQ(ut_hot_live_ref_calls, 2);
	UT_ASSERT_EQ(ut_scratch_ref_calls, 2);

	LockBuffer(UT_HOT_BUFFER, BUFFER_LOCK_UNLOCK);
	UT_ASSERT(!ut_hot_content_lock_held);
	ut_hot_production_core_active = false;
	ut_hot_product_fixture = NULL;
	ut_hot_live_ref_page = NULL;
	BufferBlocks = NULL;
}

UT_TEST(test_38_changed_input_discards_old_fetch_failure_before_error_mapping)
{
	UtR4HotProductFixture fixture;
	HeapHotSearchResult result;
	RelationData relation;
	FormData_pg_class relation_form;
	SnapshotData snapshot;
	ItemPointerData tid;
	HeapHotSearchResultKind kind;

	ut_r4_hot_init_product_fixture(&fixture, &result);
	fixture.mutate_non_target_itl = true;
	fixture.fail_first_after_mutation = true;
	ut_r4_hot_reset_scratch_authority((Page) result.scratch_page,
									  (Page) fixture.live_page,
									  UT_HOT_READ_SCN, UINT64_C(0x123450));

	memset(&relation, 0, sizeof(relation));
	memset(&relation_form, 0, sizeof(relation_form));
	relation.rd_id = UT_HOT_TABLE_OID;
	relation.rd_rel = &relation_form;
	relation_form.relpersistence = RELPERSISTENCE_PERMANENT;
	memset(&snapshot, 0, sizeof(snapshot));
	snapshot.snapshot_type = SNAPSHOT_MVCC;
	snapshot.read_scn = UT_HOT_READ_SCN;
	snapshot.read_epoch = 9;
	snapshot.cluster_source = SNAPSHOT_SOURCE_CLUSTER;
	ItemPointerSet(&tid, UT_HOT_BLOCK, UT_HOT_ROOT_OFF);

	kind = heap_hot_search_buffer_result(&tid, &relation, UT_HOT_BUFFER,
										&snapshot, &result, NULL, true);
	UT_ASSERT_EQ(kind, HEAP_HOT_SEARCH_OWNED_SCRATCH);
	UT_ASSERT_EQ(fixture.fetch_calls, 2);
	UT_ASSERT_EQ(fixture.lock_calls, 4);
	UT_ASSERT_EQ(ut_hot_live_ref_calls, 2);
	UT_ASSERT_EQ(ut_scratch_ref_calls, 1);

	LockBuffer(UT_HOT_BUFFER, BUFFER_LOCK_UNLOCK);
	UT_ASSERT(!ut_hot_content_lock_held);
	ut_hot_production_core_active = false;
	ut_hot_product_fixture = NULL;
	ut_hot_live_ref_page = NULL;
	BufferBlocks = NULL;
}

UT_TEST(test_peer_foreign_multixact_hot_chain_uses_shared_current_only)
{
	UtR4HotProductFixture fixture;
	HeapHotSearchResult result;
	RelationData relation;
	FormData_pg_class relation_form;
	SnapshotData snapshot;
	ItemPointerData tid;
	HeapHotSearchResultKind kind;

	ut_r4_hot_init_product_fixture(&fixture, &result);
	ut_r4_hot_build_foreign_multixact_chain(fixture.live_page);
	memset(&ut_cluster_conf, 0, sizeof(ut_cluster_conf));
	ut_cluster_conf.node_count = 2;
	ut_live_visible_offnum = UT_HOT_SUCCESSOR_OFF;
	ut_native_multixact_decode_calls = 0;
	ut_native_multixact_updater = UT_HOT_AUTH_UPDATER;
	cluster_r4_activation_test_current_epoch = UT_HOT_CURRENT_EPOCH;
	ut_hot_current_mx_active = true;
	ut_hot_current_mx_pcm_state = (uint8) PCM_STATE_S;
	memset(&ut_hot_successor_ref, 0, sizeof(ut_hot_successor_ref));
	ut_hot_successor_ref.origin_node_id = UT_HOT_CURRENT_MX_ORIGIN;
	ut_hot_successor_ref.undo_segment_id = 258;
	ut_hot_successor_ref.tt_slot_id = 8;
	ut_hot_successor_ref.cluster_epoch = UT_HOT_CURRENT_EPOCH;
	ut_hot_successor_ref.local_xid = UT_HOT_AUTH_UPDATER;
	ut_hot_pcm_snapshot_calls = 0;
	ut_hot_current_mx_describe_calls = 0;
	ut_hot_current_mx_resolve_calls = 0;
	ut_hot_current_mx_validate_calls = 0;
	ut_hot_requester_requalify_barrier_calls = 0;
	ut_hot_requester_requalify_barrier_lock_calls = -1;
	memset(&relation, 0, sizeof(relation));
	memset(&relation_form, 0, sizeof(relation_form));
	relation.rd_id = UT_HOT_TABLE_OID;
	relation.rd_rel = &relation_form;
	relation_form.relpersistence = RELPERSISTENCE_PERMANENT;
	memset(&snapshot, 0, sizeof(snapshot));
	snapshot.snapshot_type = SNAPSHOT_ANY;
	ItemPointerSet(&tid, UT_HOT_BLOCK, UT_HOT_ROOT_OFF);

	kind = heap_hot_search_buffer_result(&tid, &relation, UT_HOT_BUFFER,
										&snapshot, &result, NULL, true);
	UT_ASSERT_EQ(kind, HEAP_HOT_SEARCH_BUFFER_BACKED);
	UT_ASSERT_EQ(ItemPointerGetOffsetNumber(&tid), UT_HOT_SUCCESSOR_OFF);
	UT_ASSERT_EQ(HeapTupleHeaderGetRawXmin(result.tuple.t_data),
				 UT_HOT_AUTH_UPDATER);
	/* Root and successor are each evaluated once; no XCUR upgrade restart. */
	UT_ASSERT_EQ(ut_live_visibility_calls, 2);
	UT_ASSERT_EQ(fixture.fetch_calls, 0);
	UT_ASSERT_EQ(fixture.lock_calls, 4);
	UT_ASSERT_EQ(fixture.lock_modes[0], BUFFER_LOCK_UNLOCK);
	UT_ASSERT_EQ(fixture.lock_modes[1], BUFFER_LOCK_SHARE);
	UT_ASSERT_EQ(fixture.lock_modes[2], BUFFER_LOCK_UNLOCK);
	UT_ASSERT_EQ(fixture.lock_modes[3], BUFFER_LOCK_SHARE);
	UT_ASSERT_EQ(ut_hot_pcm_snapshot_calls, 5);
	UT_ASSERT_EQ(ut_hot_last_pcm_snapshot_state, (uint8) PCM_STATE_S);
	UT_ASSERT_EQ(ut_hot_live_ref_calls, 2);
	UT_ASSERT_EQ(ut_hot_current_mx_describe_calls, 1);
	UT_ASSERT_EQ(ut_hot_current_mx_resolve_calls, 1);
	UT_ASSERT_EQ(ut_hot_current_mx_validate_calls, 1);
	UT_ASSERT_EQ(ut_hot_requester_requalify_barrier_calls, 1);
	/* describe unlock/relock, then proof unlock; SHARE requalification follows */
	UT_ASSERT_EQ(ut_hot_requester_requalify_barrier_lock_calls, 3);
	UT_ASSERT_EQ(ut_native_multixact_decode_calls, 0);

	LockBuffer(UT_HOT_BUFFER, BUFFER_LOCK_UNLOCK);
	ut_hot_current_mx_pcm_state = (uint8) PCM_STATE_X;
	ut_hot_current_mx_active = false;
	ut_live_visible_offnum = InvalidOffsetNumber;
	ut_hot_production_core_active = false;
	ut_hot_product_fixture = NULL;
	ut_hot_live_ref_page = NULL;
	BufferBlocks = NULL;
}

UT_TEST(test_peer_foreign_multixact_hot_chain_revalidates_one_shot_current_images)
{
	UtR4HotProductFixture fixture;
	HeapHotSearchResult result;
	RelationData relation;
	FormData_pg_class relation_form;
	SnapshotData snapshot;
	ItemPointerData tid;
	HeapHotSearchResultKind kind;
	ClusterPcmOwnSnapshot one_shot_before;
	ClusterPcmOwnSnapshot one_shot_after;

	ut_r4_hot_init_product_fixture(&fixture, &result);
	memset(&one_shot_before, 0, sizeof(one_shot_before));
	one_shot_before.tag = fixture.expected_tag;
	one_shot_before.generation = 17;
	one_shot_before.reservation_token = 1;
	one_shot_before.pcm_state = (uint8) PCM_STATE_READ_IMAGE;
	one_shot_after = one_shot_before;
	one_shot_after.generation = 18;
	one_shot_after.reservation_token = 2;
	UT_ASSERT(cluster_heap_test_current_mx_read_requalification(
		&one_shot_before, &one_shot_after));
	one_shot_after.generation = one_shot_before.generation;
	UT_ASSERT(!cluster_heap_test_current_mx_read_requalification(
		&one_shot_before, &one_shot_after));
	one_shot_after.generation = 18;
	one_shot_after.reservation_token = one_shot_before.reservation_token;
	UT_ASSERT(!cluster_heap_test_current_mx_read_requalification(
		&one_shot_before, &one_shot_after));
	one_shot_after.reservation_token = 2;
	one_shot_after.pcm_state = (uint8) PCM_STATE_S;
	UT_ASSERT(!cluster_heap_test_current_mx_read_requalification(
		&one_shot_before, &one_shot_after));
	one_shot_after = one_shot_before;
	one_shot_after.flags = PCM_OWN_FLAG_GRANT_PENDING;
	UT_ASSERT(!cluster_heap_test_current_mx_read_requalification(
		&one_shot_before, &one_shot_after));
	ut_r4_hot_build_foreign_multixact_chain(fixture.live_page);
	memset(&ut_cluster_conf, 0, sizeof(ut_cluster_conf));
	ut_cluster_conf.node_count = 2;
	ut_live_visible_offnum = UT_HOT_SUCCESSOR_OFF;
	ut_native_multixact_decode_calls = 0;
	ut_native_multixact_updater = UT_HOT_AUTH_UPDATER;
	cluster_r4_activation_test_current_epoch = UT_HOT_CURRENT_EPOCH;
	ut_hot_current_mx_active = true;
	ut_hot_current_mx_one_shot = true;
	ut_hot_current_mx_pcm_state = (uint8) PCM_STATE_READ_IMAGE;
	ut_hot_current_read_bracket = 1;
	ut_hot_current_read_acquire_calls = 1;
	ut_hot_current_read_clear_calls = 0;
	memset(&ut_hot_successor_ref, 0, sizeof(ut_hot_successor_ref));
	ut_hot_successor_ref.origin_node_id = UT_HOT_CURRENT_MX_ORIGIN;
	ut_hot_successor_ref.undo_segment_id = 258;
	ut_hot_successor_ref.tt_slot_id = 8;
	ut_hot_successor_ref.cluster_epoch = UT_HOT_CURRENT_EPOCH;
	ut_hot_successor_ref.local_xid = UT_HOT_AUTH_UPDATER;
	ut_hot_pcm_snapshot_calls = 0;
	ut_hot_current_mx_describe_calls = 0;
	ut_hot_current_mx_resolve_calls = 0;
	ut_hot_current_mx_validate_calls = 0;
	memset(&relation, 0, sizeof(relation));
	memset(&relation_form, 0, sizeof(relation_form));
	relation.rd_id = UT_HOT_TABLE_OID;
	relation.rd_rel = &relation_form;
	relation_form.relpersistence = RELPERSISTENCE_PERMANENT;
	memset(&snapshot, 0, sizeof(snapshot));
	snapshot.snapshot_type = SNAPSHOT_ANY;
	ItemPointerSet(&tid, UT_HOT_BLOCK, UT_HOT_ROOT_OFF);

	kind = heap_hot_search_buffer_result(&tid, &relation, UT_HOT_BUFFER,
										&snapshot, &result, NULL, true);
	UT_ASSERT_EQ(kind, HEAP_HOT_SEARCH_BUFFER_BACKED);
	UT_ASSERT_EQ(ItemPointerGetOffsetNumber(&tid), UT_HOT_SUCCESSOR_OFF);
	UT_ASSERT_EQ(HeapTupleHeaderGetRawXmin(result.tuple.t_data),
				 UT_HOT_AUTH_UPDATER);
	UT_ASSERT_EQ(fixture.lock_calls, 4);
	UT_ASSERT_EQ(fixture.lock_modes[0], BUFFER_LOCK_UNLOCK);
	UT_ASSERT_EQ(fixture.lock_modes[1], BUFFER_LOCK_SHARE);
	UT_ASSERT_EQ(fixture.lock_modes[2], BUFFER_LOCK_UNLOCK);
	UT_ASSERT_EQ(fixture.lock_modes[3], BUFFER_LOCK_SHARE);
	UT_ASSERT_EQ(ut_hot_current_read_acquire_calls, 3);
	UT_ASSERT_EQ(ut_hot_current_read_clear_calls, 2);
	UT_ASSERT_EQ(ut_hot_current_read_bracket, UINT64_C(3));
	UT_ASSERT_EQ(ut_hot_pcm_snapshot_calls, 5);
	UT_ASSERT_EQ(ut_hot_last_pcm_snapshot_state,
				 (uint8) PCM_STATE_READ_IMAGE);
	UT_ASSERT_EQ(ut_hot_current_mx_describe_calls, 1);
	UT_ASSERT_EQ(ut_hot_current_mx_resolve_calls, 1);
	UT_ASSERT_EQ(ut_hot_current_mx_validate_calls, 1);
	UT_ASSERT_EQ(ut_native_multixact_decode_calls, 0);

	LockBuffer(UT_HOT_BUFFER, BUFFER_LOCK_UNLOCK);
	UT_ASSERT_EQ(ut_hot_current_read_clear_calls, 3);
	UT_ASSERT_EQ(ut_hot_current_mx_pcm_state, (uint8) PCM_STATE_N);
	ut_hot_current_mx_one_shot = false;
	ut_hot_current_mx_pcm_state = (uint8) PCM_STATE_X;
	ut_hot_current_read_bracket = 0;
	ut_hot_current_mx_active = false;
	ut_live_visible_offnum = InvalidOffsetNumber;
	ut_hot_production_core_active = false;
	ut_hot_product_fixture = NULL;
	ut_hot_live_ref_page = NULL;
	BufferBlocks = NULL;
}

UT_TEST(test_40_dormant_r4_does_not_intercept_live_hot_path)
{
	UtR4HotProductFixture fixture;
	HeapHotSearchResult result;
	RelationData relation;
	FormData_pg_class relation_form;
	SnapshotData snapshot;
	ItemPointerData tid;
	ClusterR4PrerequisiteSnapshot prerequisite;
	HeapHotSearchResultKind kind;

	ut_r4_hot_init_product_fixture(&fixture, &result);
	prerequisite = cluster_undo_block0_r4_prerequisite_snapshot();
	UT_ASSERT(!prerequisite.ready);
	UT_ASSERT_EQ(prerequisite.status,
				 CLUSTER_R4_PREREQUISITE_RF_DEFERRED);
	ut_hot_r4_target_reachable = false;
	ut_live_visibility_calls = 0;

	memset(&relation, 0, sizeof(relation));
	memset(&relation_form, 0, sizeof(relation_form));
	relation.rd_id = UT_HOT_TABLE_OID;
	relation.rd_rel = &relation_form;
	relation_form.relpersistence = RELPERSISTENCE_PERMANENT;
	memset(&snapshot, 0, sizeof(snapshot));
	snapshot.snapshot_type = SNAPSHOT_MVCC;
	snapshot.read_scn = UT_HOT_READ_SCN;
	snapshot.read_epoch = 9;
	snapshot.cluster_source = SNAPSHOT_SOURCE_CLUSTER;
	ItemPointerSet(&tid, UT_HOT_BLOCK, UT_HOT_ROOT_OFF);

	kind = heap_hot_search_buffer_result(&tid, &relation, UT_HOT_BUFFER,
										&snapshot, &result, NULL, true);
	UT_ASSERT_EQ(kind, HEAP_HOT_SEARCH_BUFFER_BACKED);
	UT_ASSERT_EQ(ItemPointerGetOffsetNumber(&tid), UT_HOT_ROOT_OFF);
	UT_ASSERT_EQ(fixture.fetch_calls, 0);
	UT_ASSERT_EQ(fixture.lock_calls, 0);
	UT_ASSERT_EQ(ut_live_visibility_calls, 1);
	UT_ASSERT(ut_hot_content_lock_held);

	LockBuffer(UT_HOT_BUFFER, BUFFER_LOCK_UNLOCK);
	ut_hot_r4_target_reachable = true;
	ut_hot_production_core_active = false;
	ut_hot_product_fixture = NULL;
	ut_hot_live_ref_page = NULL;
	BufferBlocks = NULL;
}

static void
ut_itl_census_begin(UtR4HotProductFixture *fixture,
					HeapHotSearchResult *result, bool lock_only)
{
	ClusterItlSlotData *slots;
	uint8 i;

	ut_r4_hot_init_product_fixture(fixture, result);
	slots = ClusterPageGetItlSlots((Page) fixture->live_page);
	memset(slots, 0,
		   sizeof(ClusterItlSlotData) * CLUSTER_ITL_INITRANS_DEFAULT);
	for (i = 0; i < CLUSTER_ITL_INITRANS_DEFAULT; i++)
	{
		slots[i].xid = (TransactionId) (1200 + i);
		slots[i].wrap = (uint16) (20 + i);
		slots[i].flags = lock_only
			? ITL_FLAG_LOCK_ONLY_ACTIVE : ITL_FLAG_ACTIVE;
		slots[i].undo_segment_head = uba_encode(1, i + 1, i, 0);
		ut_itl_census_outcomes[i] = CLUSTER_TX_UNKNOWN;
	}
	PageSetLSN((Page) fixture->live_page, (XLogRecPtr) UINT64_C(0x334455));
	ut_itl_census_active = true;
	ut_itl_census_mutate_wrap = false;
	ut_itl_census_lock_only = lock_only;
	ut_itl_census_alloc_calls = 0;
	ut_itl_census_capacity_calls = 0;
	ut_itl_census_resolve_calls = 0;
	ut_itl_census_retained_resolve_calls = 0;
	ut_itl_census_preflight_calls = 0;
	ut_itl_census_dirty_hint_calls = 0;
	ut_itl_recycle_guard_arm_calls = 0;
	ut_itl_recycle_guard_unlock_calls = 0;
	ut_itl_recycle_guard_relock_calls = 0;
	ut_itl_recycle_guard_cancel_calls = 0;
	ut_itl_recycle_guard_active = false;
	ut_itl_recycle_guard_arm_result = CLUSTER_BUFMGR_ITL_RECYCLE_ARMED;
	ut_itl_census_tt_generation = UINT64_C(77);
	ut_itl_census_origin_tt_generation = UINT64_C(77);
	ut_itl_census_mutate_activation = false;
	ut_itl_census_admission = NULL;
	memset(&ut_itl_census_semantic, 0,
		   sizeof(ut_itl_census_semantic));
	pg_atomic_init_u64(&ut_itl_census_semantic.admission_seq, 2);
	pg_atomic_init_u64(&ut_itl_census_semantic.active_bits, 0);
	pg_atomic_init_u64(&ut_itl_census_semantic.record_generation, 73);
	pg_atomic_init_u64(&ut_itl_census_semantic.formation_epoch, 9);
	pg_atomic_init_u32(&ut_itl_census_semantic.transition_closed, 0);
	for (i = 0; i < 2; i++)
	{
		int feature;

		for (feature = 0; feature < 64; feature++)
			pg_atomic_init_u32(
				&ut_itl_census_semantic.inflight[i][feature], 0);
	}
	memset(semantic_activation_local_inflight, 0,
		   sizeof(semantic_activation_local_inflight));
	semantic_activation_exit_hook_pid = 0;
	SemanticActivationShmem = &ut_itl_census_semantic;
	cluster_r4_activation_test_current_epoch = 9;
	ut_itl_census_tag = fixture->expected_tag;
	ut_cluster_conf.node_count = 2;
	ut_itl_pair_active = false;
	memset(ut_itl_pair_content_lock_held, 0,
		   sizeof(ut_itl_pair_content_lock_held));
	memset(ut_itl_pair_lock_buffers, 0,
		   sizeof(ut_itl_pair_lock_buffers));
	ut_itl_pair_lock_calls = 0;
	fixture->lock_calls = 0;
	ut_hot_pcm_snapshot_calls = 0;
	ut_hot_last_pcm_snapshot_state = UINT8_MAX;
	ut_itl_census_force_pcm_n = false;
	ut_itl_census_change_writer_activation_projection = false;
	ut_itl_census_replace_current_page = false;
	ut_itl_census_pcm_reservation_token = UINT64_C(17);
	ut_itl_census_pcm_flags = 0;
	ut_itl_census_stale_first_round_full = false;
	ut_itl_census_second_round_drift = false;
	ut_itl_census_second_round_fresh_locator_seen = false;
	ut_itl_census_mutate_second_terminal_after_full_resolve = false;
	ut_itl_census_second_terminal_mutated = false;
	ut_itl_census_consume_allocated_slot = false;
	ut_itl_wait_calls = 0;
	ut_itl_wait_budget_ms = 0;
	memset(ut_evidence_metrics, 0, sizeof(ut_evidence_metrics));
	memset(&ut_itl_wait_locator, 0, sizeof(ut_itl_wait_locator));
	ut_itl_wait_result = CLUSTER_TXW_RESOLVED;
}

static void
ut_itl_census_end(void)
{
	UT_ASSERT_EQ(pg_atomic_read_u32(
		&ut_itl_census_semantic.inflight[CLUSTER_SEMANTIC_TARGET_SIDE][0]), 0);
	UT_ASSERT_EQ(semantic_activation_local_inflight
		[CLUSTER_SEMANTIC_TARGET_SIDE][0], 0);
	if (ut_itl_pair_active)
	{
		UT_ASSERT(!ut_itl_pair_content_lock_held[0]);
		UT_ASSERT(!ut_itl_pair_content_lock_held[1]);
		ut_itl_pair_active = false;
		ut_hot_content_lock_held = false;
	}
	else
		LockBuffer(UT_HOT_BUFFER, BUFFER_LOCK_UNLOCK);
	ut_itl_census_active = false;
	ut_hot_production_core_active = false;
	ut_hot_product_fixture = NULL;
	ut_hot_live_ref_page = NULL;
	BufferBlocks = NULL;
	SemanticActivationShmem = NULL;
}

UT_TEST(test_41_data_itl_full_census_resolves_terminal_without_content_lock)
{
	UtR4HotProductFixture fixture;
	HeapHotSearchResult result;
	uint8 slot_index = CLUSTER_ITL_SLOT_UNALLOCATED;
	ClusterItlSlotData *slots;

	ut_itl_census_begin(&fixture, &result, false);
	ut_itl_census_outcomes[0] = CLUSTER_TX_COMMITTED;
	UT_ASSERT(cluster_heap_test_itl_alloc_with_terminal_census(
		UT_HOT_BUFFER, (TransactionId) 1300, false, &slot_index));
	slots = ClusterPageGetItlSlots((Page) fixture.live_page);
	UT_ASSERT_EQ(slot_index, 0);
	UT_ASSERT_EQ(slots[0].flags, ITL_FLAG_COMMITTED);
	UT_ASSERT_EQ((uint64) slots[0].commit_scn, UINT64_C(9001));
	UT_ASSERT_EQ(ut_itl_census_alloc_calls, 1);
	UT_ASSERT_EQ(ut_itl_census_resolve_calls,
				 CLUSTER_ITL_INITRANS_DEFAULT);
	UT_ASSERT_EQ(ut_itl_census_dirty_hint_calls, 1);
	UT_ASSERT_EQ(fixture.lock_calls, 2);
	UT_ASSERT_EQ(fixture.lock_modes[0], BUFFER_LOCK_UNLOCK);
	UT_ASSERT_EQ(fixture.lock_modes[1], BUFFER_LOCK_EXCLUSIVE);
	UT_ASSERT_EQ(ut_hot_pcm_snapshot_calls, 2);
	ut_itl_census_end();
}

UT_TEST(test_42_lock_only_itl_full_census_recycles_exact_abort)
{
	UtR4HotProductFixture fixture;
	HeapHotSearchResult result;
	uint8 slot_index = CLUSTER_ITL_SLOT_UNALLOCATED;
	ClusterItlSlotData *slots;

	ut_itl_census_begin(&fixture, &result, true);
	ut_itl_census_outcomes[0] = CLUSTER_TX_ABORTED;
	UT_ASSERT(cluster_heap_test_itl_alloc_with_terminal_census(
		UT_HOT_BUFFER, (TransactionId) 1301, true, &slot_index));
	slots = ClusterPageGetItlSlots((Page) fixture.live_page);
	UT_ASSERT_EQ(slot_index, 0);
	UT_ASSERT_EQ(slots[0].flags, ITL_FLAG_LOCK_ONLY_ABORTED);
	UT_ASSERT_EQ((uint64) slots[0].commit_scn, (uint64) InvalidScn);
	UT_ASSERT_EQ(ut_itl_census_alloc_calls, 1);
	UT_ASSERT_EQ(ut_itl_census_resolve_calls,
				 CLUSTER_ITL_INITRANS_DEFAULT);
	UT_ASSERT_EQ(ut_itl_census_dirty_hint_calls, 1);
	ut_itl_census_end();
}

UT_TEST(test_43_prepared_and_unknown_census_preserve_every_slot)
{
	UtR4HotProductFixture fixture;
	HeapHotSearchResult result;
	ClusterItlSlotData before[CLUSTER_ITL_INITRANS_DEFAULT];
	uint8 slot_index = CLUSTER_ITL_SLOT_UNALLOCATED;

	ut_itl_census_begin(&fixture, &result, false);
	ut_itl_census_outcomes[0] = CLUSTER_TX_PREPARED;
	memcpy(before, ClusterPageGetItlSlots((Page) fixture.live_page),
		   sizeof(before));
	UT_ASSERT(!cluster_heap_test_itl_alloc_with_terminal_census(
		UT_HOT_BUFFER, (TransactionId) 1302, false, &slot_index));
	UT_ASSERT_EQ(memcmp(before,
					ClusterPageGetItlSlots((Page) fixture.live_page),
					sizeof(before)), 0);
	UT_ASSERT_EQ(slot_index, CLUSTER_ITL_SLOT_UNALLOCATED);
	UT_ASSERT_EQ(ut_itl_census_alloc_calls, 0);
	UT_ASSERT_EQ(ut_itl_census_resolve_calls, 8);
	UT_ASSERT_EQ(ut_itl_census_dirty_hint_calls, 0);
	ut_itl_census_end();
}

UT_TEST(test_44_wrap_aba_recycles_only_after_fresh_second_census)
{
	UtR4HotProductFixture fixture;
	HeapHotSearchResult result;
	uint8 slot_index = CLUSTER_ITL_SLOT_UNALLOCATED;
	ClusterItlSlotData before;
	ClusterItlSlotData *slot;

	ut_itl_census_begin(&fixture, &result, false);
	ut_itl_census_outcomes[0] = CLUSTER_TX_COMMITTED;
	ut_itl_census_mutate_wrap = true;
	slot = &ClusterPageGetItlSlots((Page) fixture.live_page)[0];
	before = *slot;
	UT_ASSERT(cluster_heap_test_itl_alloc_with_terminal_census(
		UT_HOT_BUFFER, (TransactionId) 1303, false, &slot_index));
	UT_ASSERT_EQ(slot->wrap, (uint16) (before.wrap + 1));
	UT_ASSERT_EQ(slot->flags, ITL_FLAG_COMMITTED);
	UT_ASSERT_EQ(slot->xid, before.xid);
	UT_ASSERT_EQ(slot_index, 0);
	UT_ASSERT_EQ(ut_itl_census_alloc_calls, 1);
	UT_ASSERT_EQ(ut_itl_census_resolve_calls,
				 2 * CLUSTER_ITL_INITRANS_DEFAULT);
	UT_ASSERT_EQ(ut_itl_census_dirty_hint_calls, 1);
	ut_itl_census_end();
}

UT_TEST(test_45_cross_page_census_resolves_below_neither_content_lock)
{
	UtR4HotProductFixture fixture;
	HeapHotSearchResult result;

	ut_itl_census_begin(&fixture, &result, false);
	ut_itl_census_outcomes[0] = CLUSTER_TX_COMMITTED;
	ut_itl_pair_active = true;
	ut_itl_pair_content_lock_held[0] = true;
	ut_itl_pair_content_lock_held[1] = true;
	ut_hot_content_lock_held = false;

	UT_ASSERT(cluster_heap_test_itl_resolve_pair_terminal_census(
		(Buffer) 1, (Buffer) 2, (Buffer) 1));
	UT_ASSERT_EQ(ut_itl_census_resolve_calls,
				 CLUSTER_ITL_INITRANS_DEFAULT);
	UT_ASSERT(!ut_itl_pair_content_lock_held[0]);
	UT_ASSERT(!ut_itl_pair_content_lock_held[1]);
	UT_ASSERT_EQ(ut_itl_pair_lock_calls, 2);
	UT_ASSERT_EQ(ut_itl_pair_lock_buffers[0], (Buffer) 2);
	UT_ASSERT_EQ(ut_itl_pair_lock_buffers[1], (Buffer) 1);
	UT_ASSERT_EQ(ut_itl_census_dirty_hint_calls, 0);
	ut_itl_census_end();
}

UT_TEST(test_46_activation_generation_drift_preserves_terminal_candidate)
{
	UtR4HotProductFixture fixture;
	HeapHotSearchResult result;
	uint8 slot_index = CLUSTER_ITL_SLOT_UNALLOCATED;
	ClusterItlSlotData *slots;

	ut_itl_census_begin(&fixture, &result, false);
	ut_itl_census_outcomes[0] = CLUSTER_TX_COMMITTED;
	ut_itl_census_mutate_activation = true;
	UT_ASSERT(!cluster_heap_test_itl_alloc_with_terminal_census(
		UT_HOT_BUFFER, (TransactionId) 1304, false, &slot_index));
	slots = ClusterPageGetItlSlots((Page) fixture.live_page);
	UT_ASSERT_EQ(slot_index, CLUSTER_ITL_SLOT_UNALLOCATED);
	UT_ASSERT_EQ(slots[0].flags, ITL_FLAG_ACTIVE);
	UT_ASSERT_EQ(ut_itl_census_dirty_hint_calls, 0);
	ut_itl_census_end();
}

UT_TEST(test_47_origin_tt_generation_is_not_compared_to_requester_counter)
{
	UtR4HotProductFixture fixture;
	HeapHotSearchResult result;
	uint8 slot_index = CLUSTER_ITL_SLOT_UNALLOCATED;
	ClusterItlSlotData *slots;

	ut_itl_census_begin(&fixture, &result, false);
	ut_itl_census_outcomes[0] = CLUSTER_TX_COMMITTED;
	ut_itl_census_origin_tt_generation = UINT64_C(991);
	UT_ASSERT(cluster_heap_test_itl_alloc_with_terminal_census(
		UT_HOT_BUFFER, (TransactionId) 1305, false, &slot_index));
	slots = ClusterPageGetItlSlots((Page) fixture.live_page);
	UT_ASSERT_EQ(slot_index, 0);
	UT_ASSERT_EQ(slots[0].flags, ITL_FLAG_COMMITTED);
	UT_ASSERT_EQ(ut_itl_census_dirty_hint_calls, 1);
	UT_ASSERT_EQ(ut_itl_census_resolve_calls,
				 CLUSTER_ITL_INITRANS_DEFAULT);
	ut_itl_census_end();
}

UT_TEST(test_48_same_page_full_without_borrowed_census_never_leaves_token)
{
	UtR4HotProductFixture fixture;
	HeapHotSearchResult result;

	ut_itl_census_begin(&fixture, &result, false);
	UT_ASSERT_EQ(pg_atomic_read_u32(
		&ut_itl_census_semantic.inflight[CLUSTER_SEMANTIC_TARGET_SIDE][0]), 0);
	UT_ASSERT_EQ(semantic_activation_local_inflight
		[CLUSTER_SEMANTIC_TARGET_SIDE][0], 0);
	UT_ASSERT(!cluster_heap_test_itl_update_same_page_failure_cleanup());
	UT_ASSERT_EQ(pg_atomic_read_u32(
		&ut_itl_census_semantic.inflight[CLUSTER_SEMANTIC_TARGET_SIDE][0]), 0);
	UT_ASSERT_EQ(semantic_activation_local_inflight
		[CLUSTER_SEMANTIC_TARGET_SIDE][0], 0);
	ut_itl_census_end();
}

UT_TEST(test_49_known_single_node_pcm_n_resolves_and_recycles)
{
	UtR4HotProductFixture fixture;
	HeapHotSearchResult result;
	uint8 slot_index = CLUSTER_ITL_SLOT_UNALLOCATED;
	ClusterItlSlotData *slots;

	ut_itl_census_begin(&fixture, &result, false);
	ut_cluster_conf.node_count = 1;
	ut_itl_census_outcomes[0] = CLUSTER_TX_COMMITTED;
	UT_ASSERT(!cluster_conf_has_peers());
	UT_ASSERT(ut_hot_content_lock_held);
	UT_ASSERT(cluster_heap_test_itl_alloc_with_terminal_census(
		UT_HOT_BUFFER, (TransactionId) 1306, false, &slot_index));
	slots = ClusterPageGetItlSlots((Page) fixture.live_page);
	UT_ASSERT_EQ(slot_index, 0);
	UT_ASSERT_EQ(slots[0].flags, ITL_FLAG_COMMITTED);
	UT_ASSERT_EQ((uint64) slots[0].commit_scn, UINT64_C(9001));
	UT_ASSERT_EQ(ut_hot_pcm_snapshot_calls, 2);
	UT_ASSERT_EQ(ut_hot_last_pcm_snapshot_state, (uint8) PCM_STATE_N);
	UT_ASSERT_EQ(ut_itl_census_resolve_calls,
				 CLUSTER_ITL_INITRANS_DEFAULT);
	UT_ASSERT_EQ(ut_itl_census_alloc_calls, 1);
	UT_ASSERT_EQ(ut_itl_census_dirty_hint_calls, 1);
	UT_ASSERT_EQ(fixture.lock_calls, 2);
	UT_ASSERT(ut_hot_content_lock_held);
	ut_itl_census_end();
}

UT_TEST(test_50_peer_pcm_n_refuses_before_resolve)
{
	UtR4HotProductFixture fixture;
	HeapHotSearchResult result;
	uint8 slot_index = CLUSTER_ITL_SLOT_UNALLOCATED;

	ut_itl_census_begin(&fixture, &result, false);
	ut_itl_census_force_pcm_n = true;
	UT_ASSERT(cluster_conf_has_peers());
	UT_ASSERT(!cluster_heap_test_itl_alloc_with_terminal_census(
		UT_HOT_BUFFER, (TransactionId) 1307, false, &slot_index));
	UT_ASSERT_EQ(slot_index, CLUSTER_ITL_SLOT_UNALLOCATED);
	UT_ASSERT_EQ(ut_hot_pcm_snapshot_calls, 1);
	UT_ASSERT_EQ(ut_hot_last_pcm_snapshot_state, (uint8) PCM_STATE_N);
	UT_ASSERT_EQ(ut_itl_census_resolve_calls, 0);
	UT_ASSERT_EQ(ut_itl_census_alloc_calls, 0);
	UT_ASSERT_EQ(fixture.lock_calls, 0);
	UT_ASSERT(ut_hot_content_lock_held);
	ut_itl_census_end();
}

UT_TEST(test_51_terminal_census_resolves_and_stamps_complete_eight_slot_set)
{
	UtR4HotProductFixture fixture;
	HeapHotSearchResult result;
	uint8 slot_index = CLUSTER_ITL_SLOT_UNALLOCATED;
	ClusterItlSlotData *slots;
	uint8 locator_mask;
	uint8 attempted_mask;
	uint8 terminal_mask;
	uint8 terminal_count;
	uint8 i;

	ut_itl_census_begin(&fixture, &result, false);
	for (i = 0; i < CLUSTER_ITL_INITRANS_DEFAULT; i++)
		ut_itl_census_outcomes[i] = (i % 2) == 0
			? CLUSTER_TX_COMMITTED : CLUSTER_TX_ABORTED;
	UT_ASSERT(cluster_heap_test_itl_alloc_with_terminal_census(
		UT_HOT_BUFFER, (TransactionId) 1308, false, &slot_index));
	slots = ClusterPageGetItlSlots((Page) fixture.live_page);
	UT_ASSERT_EQ(slot_index, 0);
	for (i = 0; i < CLUSTER_ITL_INITRANS_DEFAULT; i++)
	{
		UT_ASSERT_EQ(slots[i].flags, (i % 2) == 0
			? ITL_FLAG_COMMITTED : ITL_FLAG_ABORTED);
		if ((i % 2) == 0)
			UT_ASSERT_EQ((uint64) slots[i].commit_scn, UINT64_C(9001));
		else
			UT_ASSERT(!SCN_VALID(slots[i].commit_scn));
	}
	UT_ASSERT_EQ(ut_itl_census_resolve_calls,
				 CLUSTER_ITL_INITRANS_DEFAULT);
	UT_ASSERT_EQ(ut_itl_census_preflight_calls, 1);
	cluster_heap_test_itl_last_census_stats(
		&locator_mask, &attempted_mask, &terminal_mask, &terminal_count);
	UT_ASSERT_EQ(locator_mask, UINT8_MAX);
	UT_ASSERT_EQ(attempted_mask, UINT8_MAX);
	UT_ASSERT_EQ(terminal_mask, UINT8_MAX);
	UT_ASSERT_EQ(terminal_count, CLUSTER_ITL_INITRANS_DEFAULT);
	UT_ASSERT_EQ(ut_itl_census_alloc_calls, 1);
	UT_ASSERT_EQ(ut_itl_census_dirty_hint_calls, 1);
	ut_itl_census_end();
}

UT_TEST(test_52_writer_activation_projection_drift_is_not_pcm_identity_drift)
{
	UtR4HotProductFixture fixture;
	HeapHotSearchResult result;
	uint8 slot_index = CLUSTER_ITL_SLOT_UNALLOCATED;

	ut_itl_census_begin(&fixture, &result, false);
	ut_itl_census_outcomes[0] = CLUSTER_TX_COMMITTED;
	ut_itl_census_change_writer_activation_projection = true;
	UT_ASSERT(cluster_heap_test_itl_alloc_with_terminal_census(
		UT_HOT_BUFFER, (TransactionId) 1309, false, &slot_index));
	UT_ASSERT_EQ(slot_index, 0);
	UT_ASSERT_EQ(ut_hot_pcm_snapshot_calls, 2);
	UT_ASSERT_EQ(ut_itl_census_resolve_calls,
				 CLUSTER_ITL_INITRANS_DEFAULT);
	ut_itl_census_end();
}

UT_TEST(test_53_stale_census_retries_only_the_relocked_current_page_once)
{
	UtR4HotProductFixture fixture;
	HeapHotSearchResult result;
	uint8 slot_index = CLUSTER_ITL_SLOT_UNALLOCATED;
	ClusterItlSlotData *slots;

	ut_itl_census_begin(&fixture, &result, false);
	ut_itl_census_outcomes[0] = CLUSTER_TX_COMMITTED;
	ut_itl_census_replace_current_page = true;
	UT_ASSERT(cluster_heap_test_itl_alloc_with_terminal_census(
		UT_HOT_BUFFER, (TransactionId) 1310, false, &slot_index));
	slots = ClusterPageGetItlSlots((Page) fixture.live_page);
	UT_ASSERT_EQ(slot_index, 4);
	UT_ASSERT_EQ(slots[0].flags, ITL_FLAG_ACTIVE);
	UT_ASSERT(!SCN_VALID(slots[0].commit_scn));
	UT_ASSERT_EQ(slots[4].flags, ITL_FLAG_COMMITTED);
	UT_ASSERT_EQ((uint64) slots[4].commit_scn, UINT64_C(8001));
	UT_ASSERT_EQ(ut_itl_census_alloc_calls, 1);
	UT_ASSERT_EQ(ut_itl_census_resolve_calls,
				 CLUSTER_ITL_INITRANS_DEFAULT);
	UT_ASSERT_EQ(ut_itl_census_dirty_hint_calls, 0);
	UT_ASSERT_EQ(ut_hot_pcm_snapshot_calls, 2);
	UT_ASSERT_EQ(fixture.lock_calls, 2);
	UT_ASSERT(ut_hot_content_lock_held);
	ut_itl_census_end();
}

UT_TEST(test_54_stale_single_node_pcm_n_does_not_retry_current_page)
{
	UtR4HotProductFixture fixture;
	HeapHotSearchResult result;
	uint8 slot_index = CLUSTER_ITL_SLOT_UNALLOCATED;
	ClusterItlSlotData *slots;

	ut_itl_census_begin(&fixture, &result, false);
	ut_cluster_conf.node_count = 1;
	ut_itl_census_outcomes[0] = CLUSTER_TX_COMMITTED;
	ut_itl_census_replace_current_page = true;
	UT_ASSERT(!cluster_heap_test_itl_alloc_with_terminal_census(
		UT_HOT_BUFFER, (TransactionId) 1311, false, &slot_index));
	slots = ClusterPageGetItlSlots((Page) fixture.live_page);
	UT_ASSERT_EQ(slot_index, CLUSTER_ITL_SLOT_UNALLOCATED);
	UT_ASSERT_EQ(slots[0].flags, ITL_FLAG_ACTIVE);
	UT_ASSERT(!SCN_VALID(slots[0].commit_scn));
	UT_ASSERT_EQ(ut_itl_census_alloc_calls, 0);
	UT_ASSERT_EQ(ut_itl_census_resolve_calls,
				 CLUSTER_ITL_INITRANS_DEFAULT);
	UT_ASSERT_EQ(ut_itl_census_dirty_hint_calls, 0);
	UT_ASSERT_EQ(ut_hot_pcm_snapshot_calls, 2);
	UT_ASSERT_EQ(ut_hot_last_pcm_snapshot_state, (uint8) PCM_STATE_N);
	UT_ASSERT(ut_hot_content_lock_held);
	ut_itl_census_end();
}

UT_TEST(test_55_second_census_recaptures_fresh_identity_after_current_page_full)
{
	UtR4HotProductFixture fixture;
	HeapHotSearchResult result;
	uint8 slot_index = CLUSTER_ITL_SLOT_UNALLOCATED;
	ClusterItlSlotData *slots;

	ut_itl_census_begin(&fixture, &result, false);
	ut_itl_census_outcomes[0] = CLUSTER_TX_COMMITTED;
	ut_itl_census_stale_first_round_full = true;
	UT_ASSERT(cluster_heap_test_itl_alloc_with_terminal_census(
		UT_HOT_BUFFER, (TransactionId) 1312, false, &slot_index));
	slots = ClusterPageGetItlSlots((Page) fixture.live_page);
	UT_ASSERT(ut_itl_census_second_round_fresh_locator_seen);
	UT_ASSERT_EQ(slot_index, 0);
	UT_ASSERT_EQ(slots[0].xid, (TransactionId) 1400);
	UT_ASSERT_EQ(slots[0].flags, ITL_FLAG_COMMITTED);
	UT_ASSERT_EQ((uint64) slots[0].commit_scn, UINT64_C(9001));
	UT_ASSERT_EQ(ut_itl_census_alloc_calls, 1);
	UT_ASSERT_EQ(ut_itl_census_resolve_calls,
				 2 * CLUSTER_ITL_INITRANS_DEFAULT);
	UT_ASSERT_EQ(ut_itl_census_dirty_hint_calls, 1);
	UT_ASSERT_EQ(ut_hot_pcm_snapshot_calls, 4);
	UT_ASSERT_EQ(fixture.lock_calls, 4);
	UT_ASSERT(ut_hot_content_lock_held);
	ut_itl_census_end();
}

UT_TEST(test_56_second_census_drift_overflows_without_third_retry)
{
	UtR4HotProductFixture fixture;
	HeapHotSearchResult result;
	uint8 slot_index = CLUSTER_ITL_SLOT_UNALLOCATED;
	ClusterItlSlotData *slots;

	ut_itl_census_begin(&fixture, &result, false);
	ut_itl_census_outcomes[0] = CLUSTER_TX_COMMITTED;
	ut_itl_census_stale_first_round_full = true;
	ut_itl_census_second_round_drift = true;
	UT_ASSERT(!cluster_heap_test_itl_alloc_with_terminal_census(
		UT_HOT_BUFFER, (TransactionId) 1313, false, &slot_index));
	slots = ClusterPageGetItlSlots((Page) fixture.live_page);
	UT_ASSERT(ut_itl_census_second_round_fresh_locator_seen);
	UT_ASSERT_EQ(slot_index, CLUSTER_ITL_SLOT_UNALLOCATED);
	UT_ASSERT_EQ(slots[0].xid, (TransactionId) 1400);
	UT_ASSERT_EQ(slots[0].flags, ITL_FLAG_ACTIVE);
	UT_ASSERT(!SCN_VALID(slots[0].commit_scn));
	UT_ASSERT_EQ(ut_itl_census_alloc_calls, 0);
	UT_ASSERT_EQ(ut_itl_census_resolve_calls,
				 2 * CLUSTER_ITL_INITRANS_DEFAULT);
	UT_ASSERT_EQ(ut_itl_census_dirty_hint_calls, 0);
	UT_ASSERT_EQ(ut_hot_pcm_snapshot_calls, 4);
	UT_ASSERT_EQ(fixture.lock_calls, 4);
	UT_ASSERT(ut_hot_content_lock_held);
	ut_itl_census_end();
}

UT_TEST(test_57_terminal_census_validates_all_before_mutating_any)
{
	UtR4HotProductFixture fixture;
	HeapHotSearchResult result;
	uint8 slot_index = CLUSTER_ITL_SLOT_UNALLOCATED;
	ClusterItlSlotData *slots;

	ut_itl_census_begin(&fixture, &result, false);
	ut_itl_census_outcomes[0] = CLUSTER_TX_COMMITTED;
	ut_itl_census_outcomes[1] = CLUSTER_TX_COMMITTED;
	ut_itl_census_mutate_second_terminal_after_full_resolve = true;
	UT_ASSERT(cluster_heap_test_itl_alloc_with_terminal_census(
		UT_HOT_BUFFER, (TransactionId) 1314, false, &slot_index));
	slots = ClusterPageGetItlSlots((Page) fixture.live_page);
	UT_ASSERT(ut_itl_census_second_terminal_mutated);
	UT_ASSERT_EQ(slot_index, 4);
	UT_ASSERT_EQ(slots[0].flags, ITL_FLAG_ACTIVE);
	UT_ASSERT_EQ(slots[1].flags, ITL_FLAG_ACTIVE);
	UT_ASSERT_EQ(slots[4].flags, ITL_FLAG_COMMITTED);
	UT_ASSERT_EQ((uint64) slots[4].commit_scn, UINT64_C(8001));
	UT_ASSERT_EQ(ut_itl_census_resolve_calls,
				 CLUSTER_ITL_INITRANS_DEFAULT);
	UT_ASSERT_EQ(ut_itl_census_alloc_calls, 1);
	UT_ASSERT_EQ(ut_itl_census_dirty_hint_calls, 0);
	ut_itl_census_end();
}

UT_TEST(test_58_terminal_census_continues_past_nonterminal_outcomes)
{
	UtR4HotProductFixture fixture;
	HeapHotSearchResult result;
	uint8 slot_index = CLUSTER_ITL_SLOT_UNALLOCATED;
	ClusterItlSlotData *slots;

	ut_itl_census_begin(&fixture, &result, false);
	ut_itl_census_outcomes[1] = CLUSTER_TX_PREPARED;
	ut_itl_census_outcomes[2] = CLUSTER_TX_IN_PROGRESS;
	ut_itl_census_outcomes[3] = CLUSTER_TX_COMMITTED;
	ut_itl_census_outcomes[4] = CLUSTER_TX_ABORTED;
	ut_itl_census_outcomes[6] = CLUSTER_TX_PREPARED;
	ut_itl_census_outcomes[7] = CLUSTER_TX_COMMITTED;
	UT_ASSERT(cluster_heap_test_itl_alloc_with_terminal_census(
		UT_HOT_BUFFER, (TransactionId) 1315, false, &slot_index));
	slots = ClusterPageGetItlSlots((Page) fixture.live_page);
	UT_ASSERT_EQ(slot_index, 3);
	UT_ASSERT_EQ(slots[0].flags, ITL_FLAG_ACTIVE);
	UT_ASSERT_EQ(slots[1].flags, ITL_FLAG_ACTIVE);
	UT_ASSERT_EQ(slots[2].flags, ITL_FLAG_ACTIVE);
	UT_ASSERT_EQ(slots[3].flags, ITL_FLAG_COMMITTED);
	UT_ASSERT_EQ(slots[4].flags, ITL_FLAG_ABORTED);
	UT_ASSERT_EQ(slots[5].flags, ITL_FLAG_ACTIVE);
	UT_ASSERT_EQ(slots[6].flags, ITL_FLAG_ACTIVE);
	UT_ASSERT_EQ(slots[7].flags, ITL_FLAG_COMMITTED);
	UT_ASSERT_EQ(ut_itl_census_resolve_calls,
				 CLUSTER_ITL_INITRANS_DEFAULT);
	UT_ASSERT_EQ(ut_itl_census_dirty_hint_calls, 1);
	ut_itl_census_end();
}

UT_TEST(test_59_one_batch_leaves_capacity_for_three_stale_followers)
{
	UtR4HotProductFixture fixture;
	HeapHotSearchResult result;
	ClusterItlSlotData *slots;
	uint8 slot_index = CLUSTER_ITL_SLOT_UNALLOCATED;
	uint8 reusable_count = 0;
	uint8 i;

	ut_itl_census_begin(&fixture, &result, false);
	for (i = 0; i < CLUSTER_ITL_INITRANS_DEFAULT; i++)
		ut_itl_census_outcomes[i] = (i % 2) == 0
			? CLUSTER_TX_COMMITTED : CLUSTER_TX_ABORTED;
	ut_itl_census_consume_allocated_slot = true;
	for (i = 0; i < 4; i++)
	{
		UT_ASSERT(cluster_heap_test_itl_alloc_with_terminal_census(
			UT_HOT_BUFFER, (TransactionId) (1316 + i), false, &slot_index));
		UT_ASSERT_EQ(slot_index, i);
	}
	slots = ClusterPageGetItlSlots((Page) fixture.live_page);
	for (i = 0; i < CLUSTER_ITL_INITRANS_DEFAULT; i++)
	{
		if (slots[i].flags == ITL_FLAG_COMMITTED
			|| slots[i].flags == ITL_FLAG_ABORTED)
			reusable_count++;
		if (i < 4)
		{
			UT_ASSERT_EQ(slots[i].flags, ITL_FLAG_ACTIVE);
			UT_ASSERT_EQ(slots[i].xid, (TransactionId) (1316 + i));
		}
	}
	UT_ASSERT_EQ(reusable_count, 4);
	UT_ASSERT_EQ(ut_itl_census_resolve_calls,
				 CLUSTER_ITL_INITRANS_DEFAULT);
	UT_ASSERT_EQ(ut_itl_census_alloc_calls, 4);
	UT_ASSERT_EQ(ut_itl_census_dirty_hint_calls, 1);
	ut_itl_census_end();
}

UT_TEST(test_60_same_page_peer_census_uses_exact_holder_singleflight)
{
	UtR4HotProductFixture fixture;
	HeapHotSearchResult result;
	uint8 slot_index = CLUSTER_ITL_SLOT_UNALLOCATED;

	ut_itl_census_begin(&fixture, &result, false);
	ut_itl_census_outcomes[0] = CLUSTER_TX_COMMITTED;
	UT_ASSERT(cluster_heap_test_itl_alloc_with_terminal_census(
		UT_HOT_BUFFER, (TransactionId) 1320, false, &slot_index));
	UT_ASSERT_EQ(slot_index, 0);
	UT_ASSERT_EQ(ut_itl_recycle_guard_arm_calls, 1);
	UT_ASSERT_EQ(ut_itl_recycle_guard_unlock_calls, 1);
	UT_ASSERT_EQ(ut_itl_recycle_guard_relock_calls, 1);
	UT_ASSERT_EQ(ut_itl_recycle_guard_cancel_calls, 0);
	UT_ASSERT(!ut_itl_recycle_guard_active);
	UT_ASSERT(ut_hot_content_lock_held);
	ut_itl_census_end();
}

UT_TEST(test_61_precommit_cleanout_requires_exact_terminal_census)
{
	UtR4HotProductFixture fixture;
	HeapHotSearchResult result;
	uint8 slot_index = CLUSTER_ITL_SLOT_UNALLOCATED;
	ClusterItlSlotData *slots;

	ut_itl_census_begin(&fixture, &result, false);
	slots = ClusterPageGetItlSlots((Page) fixture.live_page);
	slots[0].flags = ITL_FLAG_NEEDS_CLEANOUT;
	slots[0].commit_scn = (SCN) UINT64_C(9001);
	ut_itl_census_outcomes[0] = CLUSTER_TX_COMMITTED;

	UT_ASSERT(cluster_heap_test_itl_alloc_with_terminal_census(
		UT_HOT_BUFFER, (TransactionId) 1321, false, &slot_index));
	UT_ASSERT_EQ(slot_index, 0);
	UT_ASSERT_EQ(slots[0].flags, ITL_FLAG_COMMITTED);
	UT_ASSERT_EQ((uint64) slots[0].commit_scn, UINT64_C(9001));
	UT_ASSERT_EQ(ut_itl_census_resolve_calls,
				 CLUSTER_ITL_INITRANS_DEFAULT);
	UT_ASSERT_EQ(ut_itl_census_dirty_hint_calls, 1);
	ut_itl_census_end();
}

UT_TEST(test_62_in_progress_cleanout_evidence_is_never_stamped)
{
	UtR4HotProductFixture fixture;
	HeapHotSearchResult result;
	uint8 slot_index = CLUSTER_ITL_SLOT_UNALLOCATED;
	ClusterItlSlotData *slots;

	ut_itl_census_begin(&fixture, &result, false);
	slots = ClusterPageGetItlSlots((Page) fixture.live_page);
	slots[0].flags = ITL_FLAG_NEEDS_CLEANOUT;
	slots[0].commit_scn = (SCN) UINT64_C(8001);
	ut_itl_census_outcomes[0] = CLUSTER_TX_IN_PROGRESS;

	UT_ASSERT(!cluster_heap_test_itl_alloc_with_terminal_census(
		UT_HOT_BUFFER, (TransactionId) 1322, false, &slot_index));
	UT_ASSERT_EQ(slot_index, CLUSTER_ITL_SLOT_UNALLOCATED);
	UT_ASSERT_EQ(slots[0].flags, ITL_FLAG_NEEDS_CLEANOUT);
	UT_ASSERT_EQ((uint64) slots[0].commit_scn, UINT64_C(8001));
	UT_ASSERT_EQ(ut_itl_census_resolve_calls,
				 CLUSTER_ITL_INITRANS_DEFAULT);
	UT_ASSERT_EQ(ut_itl_census_dirty_hint_calls, 0);
	ut_itl_census_end();
}

UT_TEST(test_63_cleanout_evidence_scn_drift_is_never_stamped)
{
	UtR4HotProductFixture fixture;
	HeapHotSearchResult result;
	uint8 slot_index = CLUSTER_ITL_SLOT_UNALLOCATED;
	ClusterItlSlotData *slots;

	ut_itl_census_begin(&fixture, &result, false);
	slots = ClusterPageGetItlSlots((Page) fixture.live_page);
	slots[0].flags = ITL_FLAG_NEEDS_CLEANOUT;
	slots[0].commit_scn = (SCN) UINT64_C(8001);
	ut_itl_census_outcomes[0] = CLUSTER_TX_COMMITTED;

	UT_ASSERT(!cluster_heap_test_itl_alloc_with_terminal_census(
		UT_HOT_BUFFER, (TransactionId) 1323, false, &slot_index));
	UT_ASSERT_EQ(slot_index, CLUSTER_ITL_SLOT_UNALLOCATED);
	UT_ASSERT_EQ(slots[0].flags, ITL_FLAG_NEEDS_CLEANOUT);
	UT_ASSERT_EQ((uint64) slots[0].commit_scn, UINT64_C(8001));
	UT_ASSERT_EQ(ut_itl_census_dirty_hint_calls, 0);
	ut_itl_census_end();
}

static void
ut_dml_guard_advance_hint_lsn(Buffer buffer, HeapTuple tuple pg_attribute_unused(),
							 void *arg)
{
	int saved_node_id = cluster_node_id;

	/* A shipped page can carry another node's WAL origin.  The terminal
	 * census hint FPI legitimately advances both pd_lsn and its origin to
	 * this writer's stream. */
	cluster_node_id = saved_node_id == 3 ? 2 : 3;
	PageSetLSN(BufferGetPage(buffer), *(XLogRecPtr *) arg);
	cluster_node_id = saved_node_id;
}

static void
ut_dml_guard_drift_pcm(Buffer buffer pg_attribute_unused(),
					  HeapTuple tuple pg_attribute_unused(),
					  void *arg pg_attribute_unused())
{
	ut_itl_census_replace_current_page = true;
}

static void
ut_dml_guard_drift_tuple(Buffer buffer pg_attribute_unused(), HeapTuple tuple,
						 void *arg pg_attribute_unused())
{
	tuple->t_data->t_infomask ^= HEAP_XMAX_INVALID;
}

static void
ut_dml_guard_begin_revoke(Buffer buffer pg_attribute_unused(),
	HeapTuple tuple pg_attribute_unused(), void *arg pg_attribute_unused())
{
	ut_itl_census_pcm_reservation_token++;
	ut_itl_census_pcm_flags = PCM_OWN_FLAG_REVOKING;
}

static void
ut_dml_guard_abort_revoke(Buffer buffer pg_attribute_unused(),
	HeapTuple tuple pg_attribute_unused(), void *arg pg_attribute_unused())
{
	ut_itl_census_pcm_flags = 0;
}

static void
ut_dml_guard_occupy_selected_slot(Buffer buffer,
	HeapTuple tuple pg_attribute_unused(), void *arg)
{
	uint8 slot_index = *(uint8 *) arg;
	ClusterItlSlotData *slot
		= &ClusterPageGetItlSlots(BufferGetPage(buffer))[slot_index];

	slot->xid = (TransactionId) 9911;
	slot->wrap++;
	slot->flags = ITL_FLAG_ACTIVE;
	slot->lock_count = 3;
	slot->undo_segment_head = uba_encode(9, 8, 7, 6);
	slot->write_scn = (SCN) UINT64_C(0x99112233);
}

UT_TEST(test_64_dml_guard_allows_terminal_hint_lsn_but_rejects_authority_drift)
{
	UtR4HotProductFixture fixture;
	HeapHotSearchResult result;
	HeapTupleData tuple;
	XLogRecPtr hint_lsn = (XLogRecPtr) UINT64_C(0x334466);

	ut_itl_census_begin(&fixture, &result, false);
	memset(&tuple, 0, sizeof(tuple));
	tuple.t_data = ut_r4_hot_tuple_at((Page) fixture.live_page,
									 UT_HOT_ROOT_OFF);
	tuple.t_len = ItemIdGetLength(
		PageGetItemId((Page) fixture.live_page, UT_HOT_ROOT_OFF));
	ItemPointerSet(&tuple.t_self, UT_HOT_BLOCK, UT_HOT_ROOT_OFF);

	/* A terminal-census hint FPI may advance the page LSN and its coupled
	 * origin qualifier; neither is DML authority. */
	UT_ASSERT(cluster_heap_test_dml_authority_guard_recheck_with_hook(
		UT_HOT_BUFFER, &tuple, ut_dml_guard_advance_hint_lsn, &hint_lsn));

	/* The grant-to-content writer token rotates on a legal unlock/relock.  It
	 * is diagnostic projection, not Resource-X/PCM identity. */
	ut_hot_pcm_snapshot_calls = 0;
	ut_itl_census_change_writer_activation_projection = true;
	UT_ASSERT(cluster_heap_test_dml_authority_guard_recheck_with_hook(
		UT_HOT_BUFFER, &tuple, NULL, NULL));
	ut_itl_census_change_writer_activation_projection = false;

	/* A27: a type-17 drain can publish or exact-abort REVOKING after this
	 * backend already owns content-X.  Both directions, including the
	 * monotonic reservation-token advance, retain the same DML authority. */
	ut_hot_pcm_snapshot_calls = 0;
	ut_itl_census_pcm_reservation_token = UINT64_C(17);
	ut_itl_census_pcm_flags = 0;
	UT_ASSERT(cluster_heap_test_dml_authority_guard_recheck_with_hook(
		UT_HOT_BUFFER, &tuple, ut_dml_guard_begin_revoke, NULL));
	ut_hot_pcm_snapshot_calls = 0;
	ut_itl_census_pcm_reservation_token = UINT64_C(18);
	ut_itl_census_pcm_flags = PCM_OWN_FLAG_REVOKING;
	UT_ASSERT(cluster_heap_test_dml_authority_guard_recheck_with_hook(
		UT_HOT_BUFFER, &tuple, ut_dml_guard_abort_revoke, NULL));

	/* Exact PCM and target-tuple drift remain fail closed. */
	ut_hot_pcm_snapshot_calls = 0;
	UT_ASSERT(!cluster_heap_test_dml_authority_guard_recheck_with_hook(
		UT_HOT_BUFFER, &tuple, ut_dml_guard_drift_pcm, NULL));
	ut_itl_census_replace_current_page = false;
	ut_hot_pcm_snapshot_calls = 0;
	UT_ASSERT(!cluster_heap_test_dml_authority_guard_recheck_with_hook(
		UT_HOT_BUFFER, &tuple, ut_dml_guard_drift_tuple, NULL));
	ut_itl_census_end();
}

UT_TEST(test_81_terminal_census_transient_lifecycle_is_typed_retry)
{
	UtR4HotProductFixture fixture;
	HeapHotSearchResult result;
	ClusterHeapItlCapacityResult capacity_result;

	/* HANDOFF can become visible between capture and the exact RECYCLING
	 * claim.  The bufmgr seam reports that collision without arming debt. */
	ut_itl_census_begin(&fixture, &result, false);
	ut_itl_recycle_guard_arm_result
		= CLUSTER_BUFMGR_ITL_RECYCLE_RETRY_REQUALIFY;
	capacity_result = cluster_heap_test_itl_capacity_outcome(
		UT_HOT_BUFFER, (TransactionId) 1381, false);
	UT_ASSERT_EQ(capacity_result,
				 CLUSTER_HEAP_ITL_CAPACITY_RETRY_REQUALIFY);
	UT_ASSERT_EQ(ut_itl_recycle_guard_arm_calls, 1);
	UT_ASSERT_EQ(ut_itl_recycle_guard_unlock_calls, 0);
	UT_ASSERT_EQ(ut_itl_recycle_guard_relock_calls, 0);
	UT_ASSERT_EQ(ut_itl_recycle_guard_cancel_calls, 0);
	UT_ASSERT_EQ(ut_itl_census_resolve_calls, 0);
	UT_ASSERT(!ut_itl_recycle_guard_active);
	UT_ASSERT(ut_hot_content_lock_held);
	ut_itl_census_end();

	/* REVOKING can already be present at the first PCM capture.  It is the
	 * same typed retry and never reaches the recycle-arm or resolver. */
	ut_itl_census_begin(&fixture, &result, false);
	ut_itl_census_pcm_reservation_token = UINT64_C(18);
	ut_itl_census_pcm_flags = PCM_OWN_FLAG_REVOKING;
	capacity_result = cluster_heap_test_itl_capacity_outcome(
		UT_HOT_BUFFER, (TransactionId) 1382, false);
	UT_ASSERT_EQ(capacity_result,
				 CLUSTER_HEAP_ITL_CAPACITY_RETRY_REQUALIFY);
	UT_ASSERT_EQ(ut_hot_pcm_snapshot_calls, 1);
	UT_ASSERT_EQ(ut_itl_recycle_guard_arm_calls, 0);
	UT_ASSERT_EQ(ut_itl_census_resolve_calls, 0);
	UT_ASSERT(ut_hot_content_lock_held);
	ut_itl_census_end();
}

UT_TEST(test_82_cross_page_transient_census_releases_both_locks)
{
	UtR4HotProductFixture fixture;
	HeapHotSearchResult result;

	ut_itl_census_begin(&fixture, &result, false);
	ut_itl_pair_active = true;
	ut_itl_pair_content_lock_held[0] = true;
	ut_itl_pair_content_lock_held[1] = true;
	ut_hot_content_lock_held = false;
	ut_itl_census_pcm_reservation_token = UINT64_C(18);
	ut_itl_census_pcm_flags = PCM_OWN_FLAG_REVOKING;

	UT_ASSERT(!cluster_heap_test_itl_resolve_pair_terminal_census(
		(Buffer) 1, (Buffer) 2, (Buffer) 1));
	UT_ASSERT_EQ(ut_itl_census_resolve_calls, 0);
	UT_ASSERT_EQ(ut_itl_pair_lock_calls, 2);
	UT_ASSERT_EQ(ut_itl_pair_lock_buffers[0], (Buffer) 2);
	UT_ASSERT_EQ(ut_itl_pair_lock_buffers[1], (Buffer) 1);
	UT_ASSERT(!ut_itl_pair_content_lock_held[0]);
	UT_ASSERT(!ut_itl_pair_content_lock_held[1]);
	ut_itl_census_end();
}

UT_TEST(test_69_dml_guard_rejects_selected_itl_slot_only_aba)
{
	UtR4HotProductFixture fixture;
	HeapHotSearchResult result;
	HeapTupleData tuple;
	uint8 slot_index = 0;
	uint16 original_infomask;

	ut_itl_census_begin(&fixture, &result, false);
	memset(&tuple, 0, sizeof(tuple));
	tuple.t_data = ut_r4_hot_tuple_at((Page) fixture.live_page,
		UT_HOT_ROOT_OFF);
	tuple.t_len = ItemIdGetLength(
		PageGetItemId((Page) fixture.live_page, UT_HOT_ROOT_OFF));
	ItemPointerSet(&tuple.t_self, UT_HOT_BLOCK, UT_HOT_ROOT_OFF);
	original_infomask = tuple.t_data->t_infomask;
	UT_ASSERT(!cluster_heap_test_dml_authority_guard_slot_recheck_with_hook(
		UT_HOT_BUFFER, &tuple, slot_index,
		ut_dml_guard_occupy_selected_slot, &slot_index));
	UT_ASSERT_EQ(tuple.t_data->t_infomask, original_infomask);
	ut_itl_census_end();
}

UT_TEST(test_70_all_heap_callers_retry_only_from_zero_apply_drift)
{
	ClusterHeapNoRetryTestCaller caller;

	for (caller = CLUSTER_HEAP_NO_RETRY_TEST_INSERT;
		 caller <= CLUSTER_HEAP_NO_RETRY_TEST_UPDATE_CHAIN; caller++)
	{
		ClusterHeapNoRetryTestReport report;

		UT_ASSERT(cluster_heap_test_no_retry_boundary(
			caller, true, false, &report));
		UT_ASSERT_EQ(report.outcome,
			CLUSTER_HEAP_NO_RETRY_TEST_ZERO_APPLY_RETRY);
		UT_ASSERT_EQ(report.apply_calls, 0);
		UT_ASSERT_EQ(report.consume_calls, 0);
		UT_ASSERT(report.retry_edge);
	}
}

UT_TEST(test_71_all_heap_callers_refuse_partial_apply_without_retry_edge)
{
	ClusterHeapNoRetryTestCaller caller;

	for (caller = CLUSTER_HEAP_NO_RETRY_TEST_INSERT;
		 caller <= CLUSTER_HEAP_NO_RETRY_TEST_UPDATE_CHAIN; caller++)
	{
		ClusterHeapNoRetryTestReport report;

		UT_ASSERT(cluster_heap_test_no_retry_boundary(
			caller, false, true, &report));
		UT_ASSERT_EQ(report.outcome,
			CLUSTER_HEAP_NO_RETRY_TEST_REFUSED);
		UT_ASSERT(report.preflight_calls > 0);
		UT_ASSERT(report.apply_calls > 0);
		UT_ASSERT_EQ(report.consume_calls, 0);
		UT_ASSERT(!report.retry_edge);
	}
}

/* A successful publication crosses one caller-owned boundary.  Every exact
 * target is rechecked before the first receipt is applied, and undo is
 * consumed only after the final receipt has become APPLIED. */
UT_TEST(test_76_all_heap_callers_preflight_apply_then_consume)
{
	static const uint8 expected_undo_plans[] = {1, 1, 2, 0, 1, 1};
	static const uint8 expected_mx_publications[] = {0, 1, 2, 1, 1, 0};
	ClusterHeapNoRetryTestCaller caller;

	for (caller = CLUSTER_HEAP_NO_RETRY_TEST_INSERT;
		 caller <= CLUSTER_HEAP_NO_RETRY_TEST_UPDATE_CHAIN; caller++)
	{
		ClusterHeapNoRetryTestReport report;
		uint8 expected_total = expected_undo_plans[caller]
			+ expected_mx_publications[caller];

		UT_ASSERT(cluster_heap_test_no_retry_boundary(
			caller, false, false, &report));
		UT_ASSERT_EQ(report.outcome, CLUSTER_HEAP_NO_RETRY_TEST_APPLIED);
		UT_ASSERT_EQ(report.preflight_calls, expected_total);
		UT_ASSERT_EQ(report.apply_calls, expected_total);
		UT_ASSERT_EQ(report.consume_calls,
			expected_undo_plans[caller] == 0 ? 0 : 1);
		UT_ASSERT_EQ(report.retained_undo_handle_count,
			expected_undo_plans[caller]);
		UT_ASSERT(report.last_preflight_event > 0);
		UT_ASSERT(report.first_apply_event > report.last_preflight_event);
		if (report.consume_calls != 0)
			UT_ASSERT(report.consume_event > report.last_apply_event);
		UT_ASSERT(!report.retry_edge);
	}
}

/* The outer prepare loop freezes one absolute deadline.  Transient misses
 * retry with that exact value; READY succeeds and REFUSED terminates without
 * manufacturing a new deadline or another attempt. */
UT_TEST(test_77_heap_prepare_retries_with_one_frozen_deadline)
{
	const ClusterUndoRecordPrepareResult results[] = {
		CLUSTER_UNDO_RECORD_PREPARE_RETRY_REQUIRED,
		CLUSTER_UNDO_RECORD_PREPARE_RETRY_REQUIRED,
		CLUSTER_UNDO_RECORD_PREPARE_READY,
	};
	ClusterHeapPrepareRetryTestReport report;

	UT_ASSERT(cluster_heap_test_prepare_retry_sequence(results,
		lengthof(results), UINT64_C(987654321), &report));
	UT_ASSERT_EQ(report.prepare_calls, 3);
	UT_ASSERT(report.deadline_stable);
	UT_ASSERT_EQ(report.observed_deadline_us, UINT64_C(987654321));
	UT_ASSERT_EQ(report.terminal_result, CLUSTER_UNDO_RECORD_PREPARE_READY);
}

UT_TEST(test_78_heap_prepare_refusal_stops_without_extra_attempt)
{
	const ClusterUndoRecordPrepareResult results[] = {
		CLUSTER_UNDO_RECORD_PREPARE_RETRY_REQUIRED,
		CLUSTER_UNDO_RECORD_PREPARE_REFUSED,
		CLUSTER_UNDO_RECORD_PREPARE_READY,
	};
	ClusterHeapPrepareRetryTestReport report;

	UT_ASSERT(!cluster_heap_test_prepare_retry_sequence(results,
		lengthof(results), UINT64_C(123456789), &report));
	UT_ASSERT_EQ(report.prepare_calls, 2);
	UT_ASSERT(report.deadline_stable);
	UT_ASSERT_EQ(report.observed_deadline_us, UINT64_C(123456789));
	UT_ASSERT_EQ(report.terminal_result, CLUSTER_UNDO_RECORD_PREPARE_REFUSED);
}

UT_TEST(test_72_itl_reference_admission_requires_current_receipt_identity)
{
	UT_ASSERT(cluster_heap_test_itl_receipt_identity_admitted(
		FirstNormalTransactionId, 1));
	UT_ASSERT(cluster_heap_test_itl_receipt_identity_admitted(
		FirstNormalTransactionId, UINT16_MAX));
	UT_ASSERT(!cluster_heap_test_itl_receipt_identity_admitted(
		InvalidTransactionId, 1));
	UT_ASSERT(!cluster_heap_test_itl_receipt_identity_admitted(
		FirstNormalTransactionId, 0));
	UT_ASSERT(!cluster_heap_test_itl_receipt_identity_admitted(
		FirstNormalTransactionId, ((uint32) UINT16_MAX) + 1));
}

UT_TEST(test_73_current_multi_insert_delegates_to_receipt_safe_heap_insert)
{
	UT_ASSERT_EQ(cluster_heap_test_multi_insert_route(false),
		CLUSTER_HEAP_MULTI_INSERT_NATIVE_BATCH);
	UT_ASSERT_EQ(cluster_heap_test_multi_insert_route(true),
		CLUSTER_HEAP_MULTI_INSERT_RECEIPT_SAFE_PER_TUPLE);
}

UT_TEST(test_75_update_predicts_successor_only_for_receipt_consumers)
{
	UT_ASSERT(!cluster_heap_test_update_needs_successor_prediction(
		false, false));
	UT_ASSERT(cluster_heap_test_update_needs_successor_prediction(
		true, false));
	UT_ASSERT(cluster_heap_test_update_needs_successor_prediction(
		false, true));
	UT_ASSERT(cluster_heap_test_update_needs_successor_prediction(
		true, true));
}

/* A local catalog page has no PCM generation and therefore cannot produce a
 * current CTRC receipt.  The heap gate must follow the same relation-class
 * boundary as shared SMGR/PCM, while shared catalogs and user relations keep
 * the cluster path. */
UT_TEST(test_80_itl_route_matches_shared_relation_boundary)
{
	UT_ASSERT(!cluster_heap_test_itl_relation_route(
		false, false, false, (RelFileNumber) FirstNormalObjectId));
	UT_ASSERT(!cluster_heap_test_itl_relation_route(
		true, true, true, (RelFileNumber) FirstNormalObjectId));
	UT_ASSERT(!cluster_heap_test_itl_relation_route(
		true, false, false, (RelFileNumber) (FirstNormalObjectId - 1)));
	UT_ASSERT(cluster_heap_test_itl_relation_route(
		true, false, true, (RelFileNumber) (FirstNormalObjectId - 1)));
	UT_ASSERT(cluster_heap_test_itl_relation_route(
		true, false, false, (RelFileNumber) FirstNormalObjectId));
}

UT_TEST(test_65_batch_cleanout_routes_every_retained_scn_to_exact_c1b_pair)
{
	UtR4HotProductFixture fixture;
	HeapHotSearchResult result;
	ClusterItlSlotData *slots;
	uint8 slot_index = CLUSTER_ITL_SLOT_UNALLOCATED;
	uint8 i;

	ut_itl_census_begin(&fixture, &result, false);
	slots = ClusterPageGetItlSlots((Page) fixture.live_page);
	for (i = 0; i < CLUSTER_ITL_INITRANS_DEFAULT; i++)
	{
		slots[i].flags = ITL_FLAG_NEEDS_CLEANOUT;
		slots[i].commit_scn = (SCN) UINT64_C(9001);
		ut_itl_census_outcomes[i] = CLUSTER_TX_COMMITTED;
	}

	UT_ASSERT(cluster_heap_test_itl_alloc_with_terminal_census(
		UT_HOT_BUFFER, (TransactionId) 1324, false, &slot_index));
	UT_ASSERT_EQ(slot_index, 0);
	UT_ASSERT_EQ(ut_itl_census_retained_resolve_calls,
				 CLUSTER_ITL_INITRANS_DEFAULT);
	for (i = 0; i < CLUSTER_ITL_INITRANS_DEFAULT; i++)
	{
		UT_ASSERT_EQ(slots[i].flags, ITL_FLAG_COMMITTED);
		UT_ASSERT_EQ((uint64) slots[i].commit_scn, UINT64_C(9001));
	}
	ut_itl_census_end();
}

UT_TEST(test_66_one_member_current_mx_reaches_real_hot_companion)
{
	UtR4HotProductFixture fixture;
	HeapHotSearchResult result;
	RelationData relation;
	FormData_pg_class relation_form;
	SnapshotData snapshot;
	ItemPointerData tid;
	HeapHotSearchResultKind kind;

	ut_r4_hot_init_product_fixture(&fixture, &result);
	ut_r4_hot_build_foreign_multixact_chain(fixture.live_page);
	memset(&ut_cluster_conf, 0, sizeof(ut_cluster_conf));
	ut_cluster_conf.node_count = 2;
	ut_live_visible_offnum = UT_HOT_SUCCESSOR_OFF;
	ut_native_multixact_decode_calls = 0;
	cluster_r4_activation_test_current_epoch = UT_HOT_CURRENT_EPOCH;
	ut_hot_current_mx_active = true;
	ut_hot_current_mx_pcm_state = (uint8) PCM_STATE_S;
	ut_hot_current_mx_member_count = 1;
	memset(&ut_hot_successor_ref, 0, sizeof(ut_hot_successor_ref));
	ut_hot_successor_ref.origin_node_id = UT_HOT_CURRENT_MX_ORIGIN;
	ut_hot_successor_ref.undo_segment_id = 258;
	ut_hot_successor_ref.tt_slot_id = 8;
	ut_hot_successor_ref.cluster_epoch = UT_HOT_CURRENT_EPOCH;
	ut_hot_successor_ref.local_xid = UT_HOT_AUTH_UPDATER;
	ut_hot_pcm_snapshot_calls = 0;
	ut_hot_current_mx_describe_calls = 0;
	ut_hot_current_mx_resolve_calls = 0;
	ut_hot_current_mx_validate_calls = 0;

	memset(&relation, 0, sizeof(relation));
	memset(&relation_form, 0, sizeof(relation_form));
	relation.rd_id = UT_HOT_TABLE_OID;
	relation.rd_rel = &relation_form;
	relation_form.relpersistence = RELPERSISTENCE_PERMANENT;
	memset(&snapshot, 0, sizeof(snapshot));
	snapshot.snapshot_type = SNAPSHOT_ANY;
	ItemPointerSet(&tid, UT_HOT_BLOCK, UT_HOT_ROOT_OFF);

	kind = heap_hot_search_buffer_result(&tid, &relation, UT_HOT_BUFFER,
										&snapshot, &result, NULL, true);
	UT_ASSERT_EQ(kind, HEAP_HOT_SEARCH_BUFFER_BACKED);
	UT_ASSERT_EQ(ItemPointerGetOffsetNumber(&tid), UT_HOT_SUCCESSOR_OFF);
	UT_ASSERT_EQ(HeapTupleHeaderGetRawXmin(result.tuple.t_data),
				 UT_HOT_AUTH_UPDATER);
	UT_ASSERT_EQ(ut_hot_current_mx_describe_calls, 1);
	UT_ASSERT_EQ(ut_hot_current_mx_resolve_calls, 1);
	UT_ASSERT_EQ(ut_hot_current_mx_validate_calls, 1);
	UT_ASSERT_EQ(ut_native_multixact_decode_calls, 0);

	LockBuffer(UT_HOT_BUFFER, BUFFER_LOCK_UNLOCK);
	ut_hot_current_mx_pcm_state = (uint8) PCM_STATE_X;
	ut_hot_current_mx_member_count = 2;
	ut_hot_current_mx_active = false;
	ut_live_visible_offnum = InvalidOffsetNumber;
	ut_hot_production_core_active = false;
	ut_hot_product_fixture = NULL;
	ut_hot_live_ref_page = NULL;
	BufferBlocks = NULL;
}

UT_TEST(test_67_one_member_current_mx_reaches_standard_hot_consumer)
{
	UtR4HotProductFixture fixture;
	HeapHotSearchResult fixture_result;
	HeapTupleData tuple;
	RelationData relation;
	FormData_pg_class relation_form;
	SnapshotData snapshot;
	ItemPointerData tid;
	bool found;

	ut_r4_hot_init_product_fixture(&fixture, &fixture_result);
	ut_r4_hot_build_foreign_multixact_chain(fixture.live_page);
	memset(&ut_cluster_conf, 0, sizeof(ut_cluster_conf));
	ut_cluster_conf.node_count = 2;
	ut_native_multixact_decode_calls = 0;
	cluster_r4_activation_test_current_epoch = UT_HOT_CURRENT_EPOCH;
	ut_hot_current_mx_active = true;
	ut_hot_current_mx_pcm_state = (uint8) PCM_STATE_S;
	ut_hot_current_mx_member_count = 1;
	memset(&ut_hot_successor_ref, 0, sizeof(ut_hot_successor_ref));
	ut_hot_successor_ref.origin_node_id = UT_HOT_CURRENT_MX_ORIGIN;
	ut_hot_successor_ref.undo_segment_id = 258;
	ut_hot_successor_ref.tt_slot_id = 8;
	ut_hot_successor_ref.cluster_epoch = UT_HOT_CURRENT_EPOCH;
	ut_hot_successor_ref.local_xid = UT_HOT_AUTH_UPDATER;
	ut_hot_pcm_snapshot_calls = 0;
	ut_hot_current_mx_describe_calls = 0;
	ut_hot_current_mx_resolve_calls = 0;
	ut_hot_current_mx_validate_calls = 0;

	memset(&relation, 0, sizeof(relation));
	memset(&relation_form, 0, sizeof(relation_form));
	relation.rd_id = UT_HOT_TABLE_OID;
	relation.rd_rel = &relation_form;
	relation_form.relpersistence = RELPERSISTENCE_PERMANENT;
	memset(&snapshot, 0, sizeof(snapshot));
	snapshot.snapshot_type = SNAPSHOT_ANY;
	memset(&tuple, 0, sizeof(tuple));
	ItemPointerSet(&tid, UT_HOT_BLOCK, UT_HOT_ROOT_OFF);

	/* first_call=false makes the real consumer traverse past the known root. */
	found = heap_hot_search_buffer(&tid, &relation, UT_HOT_BUFFER,
								   &snapshot, &tuple, NULL, false);
	UT_ASSERT(found);
	UT_ASSERT_EQ(ItemPointerGetOffsetNumber(&tid), UT_HOT_SUCCESSOR_OFF);
	UT_ASSERT_EQ(HeapTupleHeaderGetRawXmin(tuple.t_data), UT_HOT_AUTH_UPDATER);
	UT_ASSERT_EQ(ut_hot_current_mx_describe_calls, 1);
	UT_ASSERT_EQ(ut_hot_current_mx_resolve_calls, 1);
	UT_ASSERT_EQ(ut_hot_current_mx_validate_calls, 1);
	UT_ASSERT_EQ(ut_native_multixact_decode_calls, 0);

	LockBuffer(UT_HOT_BUFFER, BUFFER_LOCK_UNLOCK);
	ut_hot_current_mx_pcm_state = (uint8) PCM_STATE_X;
	ut_hot_current_mx_member_count = 2;
	ut_hot_current_mx_active = false;
	ut_hot_production_core_active = false;
	ut_hot_product_fixture = NULL;
	ut_hot_live_ref_page = NULL;
	BufferBlocks = NULL;
}

UT_TEST(test_68_one_member_current_mx_reaches_ordinary_heap_consumer)
{
	UtR4HotProductFixture fixture;
	HeapHotSearchResult fixture_result;
	HeapTupleData tuple;
	RelationData relation;
	FormData_pg_class relation_form;
	HeapTupleHeader root;
	MultiXactMember normalized[2];
	TM_Result result = TM_BeingModified;
	uint16 normalized_count = 0;

	ut_r4_hot_init_product_fixture(&fixture, &fixture_result);
	ut_r4_hot_build_foreign_multixact_chain(fixture.live_page);
	root = ut_r4_hot_tuple_at((Page) fixture.live_page, UT_HOT_ROOT_OFF);
	root->t_infomask = HEAP_XMIN_COMMITTED | HEAP_XMAX_IS_MULTI
		| HEAP_XMAX_KEYSHR_LOCK | HEAP_XMAX_LOCK_ONLY;
	root->t_infomask2 = 0;
	ItemPointerSet(&root->t_ctid, UT_HOT_BLOCK, UT_HOT_ROOT_OFF);

	memset(&ut_cluster_conf, 0, sizeof(ut_cluster_conf));
	ut_cluster_conf.node_count = 2;
	cluster_r4_activation_test_current_epoch = UT_HOT_CURRENT_EPOCH;
	ut_hot_current_mx_active = true;
	ut_hot_current_mx_member_count = 1;
	ut_current_mx_ordinary_lock_only = true;
	/* Releasing content authority for DESCRIBE/RESOLVE may rotate only the
	 * diagnostic writer-activation projection.  It is not PCM identity and
	 * must not manufacture an ABA restart. */
	ut_itl_census_change_writer_activation_projection = true;
	ut_hot_pcm_snapshot_calls = 0;
	ut_hot_current_mx_describe_calls = 0;
	ut_hot_current_mx_resolve_calls = 0;
	ut_hot_current_mx_validate_calls = 0;

	memset(&relation, 0, sizeof(relation));
	memset(&relation_form, 0, sizeof(relation_form));
	relation.rd_id = UT_HOT_TABLE_OID;
	relation.rd_rel = &relation_form;
	relation_form.relpersistence = RELPERSISTENCE_PERMANENT;
	memset(&tuple, 0, sizeof(tuple));
	tuple.t_data = root;
	tuple.t_len = ItemIdGetLength(
		PageGetItemId((Page) fixture.live_page, UT_HOT_ROOT_OFF));
	tuple.t_tableOid = UT_HOT_TABLE_OID;
	ItemPointerSet(&tuple.t_self, UT_HOT_BLOCK, UT_HOT_ROOT_OFF);
	memset(normalized, 0, sizeof(normalized));

	UT_ASSERT(cluster_heap_test_current_mx_authorize_keyshare(
		&relation, UT_HOT_BUFFER, &tuple, UT_ORDINARY_REQUESTER,
		&result, normalized, lengthof(normalized), &normalized_count));
	UT_ASSERT_EQ(result, TM_Ok);
	UT_ASSERT_EQ(normalized_count, 2);
	UT_ASSERT_EQ(normalized[0].xid, UT_ORDINARY_LOCKER);
	UT_ASSERT_EQ(normalized[0].status, MultiXactStatusForKeyShare);
	UT_ASSERT_EQ(normalized[1].xid, UT_ORDINARY_REQUESTER);
	UT_ASSERT_EQ(normalized[1].status, MultiXactStatusForKeyShare);
	UT_ASSERT_EQ(ut_hot_current_mx_describe_calls, 1);
	UT_ASSERT_EQ(ut_hot_current_mx_resolve_calls, 1);
	UT_ASSERT_EQ(ut_hot_pcm_snapshot_calls, 3);
	UT_ASSERT_EQ(ut_native_multixact_decode_calls, 0);

	LockBuffer(UT_HOT_BUFFER, BUFFER_LOCK_UNLOCK);
	ut_current_mx_ordinary_lock_only = false;
	ut_itl_census_change_writer_activation_projection = false;
	ut_hot_current_mx_member_count = 2;
	ut_hot_current_mx_active = false;
	ut_hot_production_core_active = false;
	ut_hot_product_fixture = NULL;
	ut_hot_live_ref_page = NULL;
	BufferBlocks = NULL;
}

UT_TEST(test_79_current_mx_epoch_zero_requires_clean_four_node_formation)
{
	memset(&ut_cluster_conf, 0, sizeof(ut_cluster_conf));
	ut_cluster_conf.node_count = 4;
	UT_ASSERT(cluster_heap_test_current_mx_epoch_supported(0));
	UT_ASSERT(cluster_heap_test_current_mx_epoch_supported(1));
	UT_ASSERT(cluster_heap_test_current_mx_epoch_supported(UINT32_MAX));
	UT_ASSERT(!cluster_heap_test_current_mx_epoch_supported(
		UINT64_C(1) + UINT32_MAX));

	ut_cluster_conf.node_count = 2;
	UT_ASSERT(!cluster_heap_test_current_mx_epoch_supported(0));
	UT_ASSERT(cluster_heap_test_current_mx_epoch_supported(1));
}

UT_TEST(test_recycled_writer_uses_existing_authority_outside_content_lock)
{
	int leg;

	for (leg = 0; leg < 4; leg++) {
		UtR4HotProductFixture fixture;
		HeapHotSearchResult result;
		ClusterVisResolve resolved;
		ClusterVisEvidence evidence;
		PGAlignedBlock before;

		ut_itl_census_begin(&fixture, &result, false);
		memcpy(before.data, fixture.live_page, BLCKSZ);
		memset(&ut_scratch_expected_ref, 0, sizeof(ut_scratch_expected_ref));
		ut_scratch_expected_ref.origin_node_id = 1;
		ut_scratch_expected_ref.tt_slot_id = 2;
		ut_scratch_expected_ref.local_xid = 1500;
		ut_scratch_expected_xid = 1200;
		ut_scratch_expected_lsn = PageGetLSN((Page)fixture.live_page);
		ut_scratch_expected_read_scn = InvalidScn;
		ut_scratch_resolve_evidence
			= leg == 3 ? CLUSTER_VIS_EVIDENCE_STALE_OR_AMBIGUOUS : CLUSTER_VIS_EVIDENCE_REMOTE;
		ut_scratch_resolve_status = leg < 2	   ? CLUSTER_TT_STATUS_COMMITTED
									: leg == 2 ? CLUSTER_TT_STATUS_ABORTED
											   : CLUSTER_TT_STATUS_UNKNOWN;
		ut_scratch_resolve_scn = leg < 2 ? (SCN)9001 : InvalidScn;
		ut_scratch_exact_resolve_calls = 0;
		evidence = cluster_heap_test_resolve_recycled_writer_ref(
			UT_HOT_BUFFER, 1200, &ut_scratch_expected_ref, false, &resolved);
		UT_ASSERT_EQ(evidence, ut_scratch_resolve_evidence);
		UT_ASSERT_EQ(ut_scratch_exact_resolve_calls, 1);
		UT_ASSERT(!ut_hot_content_lock_held);
		UT_ASSERT_EQ(memcmp(before.data, fixture.live_page, BLCKSZ), 0);
		if (!ut_hot_content_lock_held)
			LockBuffer(UT_HOT_BUFFER, BUFFER_LOCK_EXCLUSIVE);
		ut_itl_census_end();
	}
}

UT_TEST(test_lock_only_xid_breach_never_asks_fallback_or_changes_page)
{
	UtR4HotProductFixture fixture;
	HeapHotSearchResult result;
	ClusterVisResolve resolved;
	ClusterUndoTTSlotRef ref;
	PGAlignedBlock before;

	ut_itl_census_begin(&fixture, &result, false);
	memcpy(before.data, fixture.live_page, BLCKSZ);
	memset(&ref, 0, sizeof(ref));
	ref.origin_node_id = 1;
	ref.tt_slot_id = 2;
	ref.local_xid = 1500;
	ut_scratch_exact_resolve_calls = 0;
	UT_ASSERT_EQ(
		cluster_heap_test_resolve_recycled_writer_ref(UT_HOT_BUFFER, 1200, &ref, true, &resolved),
		CLUSTER_VIS_EVIDENCE_STALE_OR_AMBIGUOUS);
	UT_ASSERT_EQ(ut_scratch_exact_resolve_calls, 0);
	UT_ASSERT(ut_hot_content_lock_held);
	UT_ASSERT_EQ(memcmp(before.data, fixture.live_page, BLCKSZ), 0);
	ut_itl_census_end();
}

UT_TEST(test_recycled_writer_terminal_consumes_proof_only_after_fresh_recheck)
{
	int leg;

	for (leg = 0; leg < 5; leg++) {
		UtR4HotProductFixture fixture;
		HeapHotSearchResult hot_result;
		HeapTupleData tuple = { 0 };
		TM_Result result = TM_Invisible;
		PGAlignedBlock before;

		ut_itl_census_begin(&fixture, &hot_result, false);
		tuple.t_data = ut_r4_hot_tuple_at((Page)fixture.live_page, UT_HOT_ROOT_OFF);
		tuple.t_len = UT_HOT_TUPLE_LEN;
		tuple.t_tableOid = UT_HOT_TABLE_OID;
		ItemPointerSet(&tuple.t_self, UT_HOT_BLOCK, UT_HOT_ROOT_OFF);
		tuple.t_data->t_ctid = tuple.t_self;
		tuple.t_data->t_infomask = HEAP_XMIN_COMMITTED;
		tuple.t_data->t_itl_slot_idx = 2;
		HeapTupleHeaderSetXmax(tuple.t_data, 1200);
		memset(&ut_scratch_expected_ref, 0, sizeof(ut_scratch_expected_ref));
		ut_scratch_expected_ref.origin_node_id = 1;
		ut_scratch_expected_ref.tt_slot_id = 2;
		ut_scratch_expected_ref.local_xid = 1500;
		ut_scratch_expected_xid = 1200;
		ut_scratch_expected_lsn = PageGetLSN((Page)fixture.live_page);
		ut_scratch_expected_read_scn = InvalidScn;
		ut_scratch_resolve_evidence = CLUSTER_VIS_EVIDENCE_REMOTE;
		ut_scratch_resolve_status
			= leg == 1 ? CLUSTER_TT_STATUS_ABORTED : CLUSTER_TT_STATUS_COMMITTED;
		ut_scratch_resolve_scn = leg == 1 ? InvalidScn : (SCN)9001;
		ut_scratch_exact_resolve_calls = 0;
		ut_writer_bridge_fixture = true;
		ut_writer_bridge_mutation = leg >= 2 ? leg - 1 : 0;
		ut_writer_bridge_tuple_pulls = 0;
		memcpy(before.data, fixture.live_page, BLCKSZ);
		UT_ASSERT(cluster_heap_test_writer_wait(NULL, 1, &tuple, 1200, tuple.t_data->t_infomask,
												&result));
		UT_ASSERT_EQ(result, leg >= 2 ? TM_BeingModified : leg == 1 ? TM_Ok : TM_Deleted);
		UT_ASSERT_EQ(ut_scratch_exact_resolve_calls, 1);
		UT_ASSERT_EQ(ut_writer_bridge_tuple_pulls, 0);
		UT_ASSERT(ut_hot_content_lock_held);
		if (leg < 2)
			UT_ASSERT_EQ(memcmp(before.data, fixture.live_page, BLCKSZ), 0);
		ut_writer_bridge_fixture = false;
		ut_itl_census_end();
	}
}

UT_TEST(test_itl_capacity_deadline_is_once_only_checked_and_ceil_rounded)
{
	uint64 deadline = 0;
	uint64 saved;

	UT_ASSERT_EQ(cluster_heap_test_itl_remaining_wait_ms(&deadline, 1000, 5000), 5000);
	UT_ASSERT_EQ(deadline, UINT64_C(5001000));
	saved = deadline;
	UT_ASSERT_EQ(cluster_heap_test_itl_remaining_wait_ms(&deadline, 5000001, 30000), 1);
	UT_ASSERT_EQ(deadline, saved);
	UT_ASSERT_EQ(cluster_heap_test_itl_remaining_wait_ms(&deadline, saved, 30000), 0);
	UT_ASSERT_EQ(deadline, saved);
	deadline = 0;
	UT_ASSERT_EQ(cluster_heap_test_itl_remaining_wait_ms(&deadline, 1000, 0), 0);
	UT_ASSERT_EQ(deadline, 0);
	UT_ASSERT_EQ(cluster_heap_test_itl_remaining_wait_ms(&deadline, 1000, -1), 0);
	UT_ASSERT_EQ(deadline, 0);
	UT_ASSERT_EQ(cluster_heap_test_itl_remaining_wait_ms(&deadline, UINT64_MAX - 10, 1), 0);
	UT_ASSERT_EQ(deadline, 0);
}

UT_TEST(test_itl_full_wait_uses_lowest_exact_blocker_after_pair_unwind)
{
	int pair;

	for (pair = 0; pair < 2; pair++) {
		UtR4HotProductFixture fixture;
		HeapHotSearchResult result;
		uint64 deadline = 0;
		const char *reason;
		uint8 i;
		int releases;

		ut_itl_census_begin(&fixture, &result, false);
		for (i = 0; i < CLUSTER_ITL_INITRANS_DEFAULT; i++)
			ut_itl_census_outcomes[i] = CLUSTER_TX_IN_PROGRESS;
		if (pair) {
			ut_itl_pair_active = true;
			ut_itl_pair_content_lock_held[0] = true;
			ut_itl_pair_content_lock_held[1] = true;
			ut_hot_content_lock_held = false;
		}
		releases = ut_buffer_release_calls;
		UT_ASSERT_EQ(cluster_heap_test_itl_wait_capacity(1, pair ? 2 : 1, 1, &deadline, &reason),
					 CLUSTER_TXW_RESOLVED);
		UT_ASSERT_EQ(ut_itl_wait_calls, 1);
		UT_ASSERT_EQ(ut_itl_wait_locator.itl_slot_index, 0);
		UT_ASSERT_EQ(ut_itl_wait_locator.xid, 1200);
		UT_ASSERT(ut_itl_wait_locator.tt_wrap != TT_WRAP_INVALID);
		UT_ASSERT(deadline > 0);
		UT_ASSERT(ut_itl_wait_budget_ms <= cluster_ges_request_timeout_ms);
		UT_ASSERT_EQ(ut_itl_census_dirty_hint_calls, 0);
		UT_ASSERT_EQ(ut_buffer_release_calls - releases, pair);
		UT_ASSERT_EQ(ut_evidence_metrics[CLUSTER_VIS_METRIC_ITL_SELECTED], 1);
		UT_ASSERT_EQ(ut_evidence_metrics[CLUSTER_VIS_METRIC_ITL_WAIT_STARTED], 1);
		UT_ASSERT_EQ(ut_evidence_metrics[CLUSTER_VIS_METRIC_ITL_WAIT_TERMINAL], 1);
		UT_ASSERT_EQ(ut_evidence_metrics[CLUSTER_VIS_METRIC_ITL_DEADLINE_INIT], 1);
		if (pair) {
			/* Restore fixture only after recording a RED; production must
			 * have performed both unlocks already. */
			UT_ASSERT(!ut_itl_pair_content_lock_held[0]);
			UT_ASSERT(!ut_itl_pair_content_lock_held[1]);
			ut_itl_pair_content_lock_held[0] = false;
			ut_itl_pair_content_lock_held[1] = false;
		} else if (!ut_hot_content_lock_held)
			LockBuffer(1, BUFFER_LOCK_EXCLUSIVE);
		ut_itl_census_end();
	}
}

UT_TEST(test_itl_wait_negative_boundaries_preserve_page_and_close_owners)
{
	int leg;

	for (leg = 0; leg < 7; leg++) {
		UtR4HotProductFixture fixture;
		HeapHotSearchResult result;
		PGAlignedBlock before;
		uint64 deadline = leg == 1 || leg == 6 ? 1 : 0;
		const char *reason;
		ClusterTxwResult expected = CLUSTER_TXW_UNPROVABLE;
		uint8 i;

		ut_itl_census_begin(&fixture, &result, false);
		for (i = 0; i < CLUSTER_ITL_INITRANS_DEFAULT; i++)
			ut_itl_census_outcomes[i] = CLUSTER_TX_IN_PROGRESS;
		if (leg == 0)
			ut_itl_census_outcomes[0] = CLUSTER_TX_UNKNOWN;
		if (leg == 1 || leg == 2) {
			expected = CLUSTER_TXW_TIMEOUT;
			ut_itl_wait_result = CLUSTER_TXW_TIMEOUT;
		}
		if (leg == 3)
			ut_itl_census_mutate_activation = true;
		if (leg == 4) {
			for (i = 0; i < CLUSTER_ITL_INITRANS_DEFAULT; i++)
				ClusterPageGetItlSlots((Page)fixture.live_page)[i].flags = ITL_FLAG_COMMITTED;
		}
		if (leg == 5 || leg == 6) {
			ut_itl_census_pcm_flags = PCM_OWN_FLAG_REVOKING;
			expected = leg == 6 ? CLUSTER_TXW_TIMEOUT : CLUSTER_TXW_RETRY;
		}
		memcpy(before.data, fixture.live_page, BLCKSZ);
		UT_ASSERT_EQ(cluster_heap_test_itl_wait_capacity(1, 1, 1, &deadline, &reason), expected);
		UT_ASSERT_EQ(ut_itl_wait_calls, leg == 2 ? 1 : 0);
		UT_ASSERT_EQ(memcmp(before.data, fixture.live_page, BLCKSZ), 0);
		UT_ASSERT_EQ(ut_itl_census_dirty_hint_calls, 0);
		UT_ASSERT(!ut_itl_recycle_guard_active);
		UT_ASSERT(!ut_hot_content_lock_held);
		if (leg == 1 || leg == 6) {
			UT_ASSERT_EQ(deadline, 1);
			UT_ASSERT(strcmp(reason, "ITL_CAPACITY_WAIT_EXPIRED") == 0);
			UT_ASSERT_EQ(ut_itl_census_resolve_calls, 0);
		}
		if (leg == 4)
			UT_ASSERT(strcmp(reason, "ITL_PROTECTED_HISTORY_CAPACITY") == 0);
		LockBuffer(1, BUFFER_LOCK_EXCLUSIVE);
		ut_itl_census_end();
	}
}

int
main(void)
{
	UT_PLAN(88);
	UT_RUN(test_recycled_writer_terminal_consumes_proof_only_after_fresh_recheck);
	UT_RUN(test_itl_wait_negative_boundaries_preserve_page_and_close_owners);
	UT_RUN(test_itl_capacity_deadline_is_once_only_checked_and_ceil_rounded);
	UT_RUN(test_itl_full_wait_uses_lowest_exact_blocker_after_pair_unwind);
	UT_RUN(test_recycled_writer_uses_existing_authority_outside_content_lock);
	UT_RUN(test_lock_only_xid_breach_never_asks_fallback_or_changes_page);
	UT_RUN(test_01_held_lock_bits_are_independent);
	UT_RUN(test_02_wait_edge_values_are_closed);
	UT_RUN(test_03_utility_to_lmon_wait_with_no_lock_is_allowed);
	UT_RUN(test_04_utility_wait_rejects_resource_lock);
	UT_RUN(test_05_utility_wait_rejects_buffer_lock);
	UT_RUN(test_06_utility_wait_rejects_slru_lock);
	UT_RUN(test_07_utility_wait_rejects_undo_io_ownership);
	UT_RUN(test_08_utility_wait_rejects_ic_dispatch_ownership);
	UT_RUN(test_09_utility_wait_rejects_combined_forbidden_locks);
	UT_RUN(test_10_lmon_to_qvotec_wait_with_no_lock_is_allowed);
	UT_RUN(test_11_qvotec_wait_rejects_resource_lock);
	UT_RUN(test_12_qvotec_wait_rejects_buffer_lock);
	UT_RUN(test_13_qvotec_wait_rejects_slru_lock);
	UT_RUN(test_14_qvotec_wait_rejects_undo_io_ownership);
	UT_RUN(test_15_qvotec_wait_rejects_ic_dispatch_ownership);
	UT_RUN(test_16_qvotec_wait_rejects_all_forbidden_locks);
	UT_RUN(test_17_peer_ack_wait_with_no_lock_is_allowed);
	UT_RUN(test_18_peer_ack_wait_rejects_resource_lock);
	UT_RUN(test_19_peer_ack_wait_rejects_buffer_lock);
	UT_RUN(test_20_peer_ack_wait_rejects_slru_lock);
	UT_RUN(test_21_peer_ack_wait_rejects_undo_io_ownership);
	UT_RUN(test_22_peer_ack_wait_rejects_ic_dispatch_ownership);
	UT_RUN(test_23_control_barrier_with_no_lock_is_allowed);
	UT_RUN(test_24_control_barrier_rejects_resource_lock);
	UT_RUN(test_25_control_barrier_rejects_buffer_lock);
	UT_RUN(test_26_control_barrier_rejects_slru_lock);
	UT_RUN(test_27_control_barrier_rejects_undo_io_ownership);
	UT_RUN(test_28_control_barrier_rejects_ic_dispatch_ownership);
	UT_RUN(test_29_process_utility_may_only_wait_on_lmon);
	UT_RUN(test_30_lmon_may_only_delegate_durable_io_to_qvotec);
	UT_RUN(test_31_lmon_control_path_never_enters_holder_lms);
	UT_RUN(test_32_qvotec_completion_never_enters_origin_data);
	UT_RUN(test_33_buffer_backed_result_keeps_real_buffer_pin);
	UT_RUN(test_34_owned_scratch_result_survives_source_and_live_poison);
	UT_RUN(test_35_scratch_mvcc_uses_exact_ref_without_hints_or_live_page);
	UT_RUN(test_36_production_hot_core_full_result_is_owned);
	UT_RUN(test_37_full_input_recheck_catches_non_target_itl_with_stable_tuple_and_lsn);
	UT_RUN(test_38_changed_input_discards_old_fetch_failure_before_error_mapping);
	UT_RUN(test_peer_foreign_multixact_hot_chain_uses_shared_current_only);
	UT_RUN(test_peer_foreign_multixact_hot_chain_revalidates_one_shot_current_images);
	UT_RUN(test_40_dormant_r4_does_not_intercept_live_hot_path);
	UT_RUN(test_41_data_itl_full_census_resolves_terminal_without_content_lock);
	UT_RUN(test_42_lock_only_itl_full_census_recycles_exact_abort);
	UT_RUN(test_43_prepared_and_unknown_census_preserve_every_slot);
	UT_RUN(test_44_wrap_aba_recycles_only_after_fresh_second_census);
	UT_RUN(test_45_cross_page_census_resolves_below_neither_content_lock);
	UT_RUN(test_46_activation_generation_drift_preserves_terminal_candidate);
	UT_RUN(test_47_origin_tt_generation_is_not_compared_to_requester_counter);
	UT_RUN(test_48_same_page_full_without_borrowed_census_never_leaves_token);
	UT_RUN(test_49_known_single_node_pcm_n_resolves_and_recycles);
	UT_RUN(test_50_peer_pcm_n_refuses_before_resolve);
	UT_RUN(test_51_terminal_census_resolves_and_stamps_complete_eight_slot_set);
	UT_RUN(test_52_writer_activation_projection_drift_is_not_pcm_identity_drift);
	UT_RUN(test_53_stale_census_retries_only_the_relocked_current_page_once);
	UT_RUN(test_54_stale_single_node_pcm_n_does_not_retry_current_page);
	UT_RUN(test_55_second_census_recaptures_fresh_identity_after_current_page_full);
	UT_RUN(test_56_second_census_drift_overflows_without_third_retry);
	UT_RUN(test_57_terminal_census_validates_all_before_mutating_any);
	UT_RUN(test_58_terminal_census_continues_past_nonterminal_outcomes);
	UT_RUN(test_59_one_batch_leaves_capacity_for_three_stale_followers);
	UT_RUN(test_60_same_page_peer_census_uses_exact_holder_singleflight);
	UT_RUN(test_61_precommit_cleanout_requires_exact_terminal_census);
	UT_RUN(test_62_in_progress_cleanout_evidence_is_never_stamped);
	UT_RUN(test_63_cleanout_evidence_scn_drift_is_never_stamped);
	UT_RUN(test_64_dml_guard_allows_terminal_hint_lsn_but_rejects_authority_drift);
	UT_RUN(test_81_terminal_census_transient_lifecycle_is_typed_retry);
	UT_RUN(test_82_cross_page_transient_census_releases_both_locks);
	UT_RUN(test_69_dml_guard_rejects_selected_itl_slot_only_aba);
	UT_RUN(test_70_all_heap_callers_retry_only_from_zero_apply_drift);
	UT_RUN(test_71_all_heap_callers_refuse_partial_apply_without_retry_edge);
	UT_RUN(test_76_all_heap_callers_preflight_apply_then_consume);
	UT_RUN(test_77_heap_prepare_retries_with_one_frozen_deadline);
	UT_RUN(test_78_heap_prepare_refusal_stops_without_extra_attempt);
	UT_RUN(test_72_itl_reference_admission_requires_current_receipt_identity);
	UT_RUN(test_73_current_multi_insert_delegates_to_receipt_safe_heap_insert);
	UT_RUN(test_75_update_predicts_successor_only_for_receipt_consumers);
	UT_RUN(test_80_itl_route_matches_shared_relation_boundary);
	UT_RUN(test_65_batch_cleanout_routes_every_retained_scn_to_exact_c1b_pair);
	UT_RUN(test_68_one_member_current_mx_reaches_ordinary_heap_consumer);
	UT_RUN(test_66_one_member_current_mx_reaches_real_hot_companion);
	UT_RUN(test_67_one_member_current_mx_reaches_standard_hot_consumer);
	UT_RUN(test_79_current_mx_epoch_zero_requires_clean_four_node_formation);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
