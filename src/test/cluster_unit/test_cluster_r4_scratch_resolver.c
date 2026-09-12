/*-------------------------------------------------------------------------
 *
 * test_cluster_r4_scratch_resolver.c
 *	  Unit37 receipt for the real exact-ref visibility resolver.
 *
 * The test links a function-sectioned cluster_visibility_resolve.c and drives
 * its peer-origin, still-bound ITL-ref path into an exact-key memo hit.  Only
 * the backend-local memo is a fixture boundary.  Every overlay, wire, native
 * CLOG and durable-recovery alternative is trapped and must remain unused.
 *
 *-------------------------------------------------------------------------
 */
#define USE_CLUSTER_UNIT 1

#include "postgres.h"

#include "access/clog.h"
#include "access/heapam.h"
#include "access/htup_details.h"
#include "access/multixact.h"
#include "access/xact.h"
#include "access/transam.h"
#include "access/xlog.h"
#include "cluster/cluster_cr.h"
#include "cluster/cluster_epoch.h"
#include "cluster/cluster_guc.h"
#include "cluster/cluster_itl.h"
#include "cluster/cluster_itl_cleanout.h"
#include "cluster/cluster_catalog_stats.h"
#include "cluster/cluster_mxid_stripe.h"
#include "cluster/cluster_visibility_inject.h"
#include "cluster/cluster_recovery_merge.h"
#include "cluster/cluster_remote_xact.h"
#include "cluster/cluster_runtime_visibility.h"
#include "cluster/cluster_subtrans.h"
#include "cluster/cluster_touched_peers.h"
#include "cluster/cluster_tt_durable.h"
#include "cluster/cluster_tt_status.h"
#include "cluster/cluster_tx_resolve.h"
#include "cluster/cluster_undo_verdict.h"
#include "cluster/cluster_visibility_resolve.h"
#include "cluster/cluster_xid_authority.h"
#include "cluster/cluster_xid_stripe.h"
#include "cluster/cluster_xnode_lever.h"
#include "cluster/cluster_xnode_profile.h"
#include "storage/lwlock.h"
#include "storage/buf_internals.h"
#include "storage/procarray.h"
#include "utils/combocid.h"
#include "utils/snapmgr.h"
#include "utils/wait_event.h"
#include "../../backend/access/heap/heapam_r4_private.h"

#include "unit_test.h"

/* Exercise the real lossy-hint sender/receiver alongside the real resolver.
 * Function sections discard unrelated registration/shmem entry points. */
#include "../../backend/cluster/cluster_tt_status_hint.c"

UT_DEFINE_GLOBALS();

sigjmp_buf *PG_exception_stack;
ErrorContextCallback *error_context_stack;

void
pg_re_throw(void)
{
	if (PG_exception_stack != NULL)
		siglongjmp(*PG_exception_stack, 1);
	abort();
}

/* This fixture keeps its original non-writer HTSU entry. The new page-only
 * writer route is exercised with real UBA decoding in r4_lock_order. */
NodeId
uba_origin_node_id(UBA uba pg_attribute_unused())
{
	UT_ASSERT(false);
	return InvalidNodeId;
}

/* These scratch pages describe only the selected ref; there is no alternate
 * on-page DATA/lock selector. Keep that absence explicit at the fixture edge. */
bool
cluster_itl_find_data_slot_index_by_xid(Page page pg_attribute_unused(),
										TransactionId xid pg_attribute_unused(), uint8 *slot_index)
{
	*slot_index = CLUSTER_ITL_SLOT_UNALLOCATED;
	return false;
}

bool
cluster_itl_find_lock_slot_index_by_xmax(Page page pg_attribute_unused(),
										 TransactionId xid pg_attribute_unused(), uint8 *slot_index)
{
	*slot_index = CLUSTER_ITL_SLOT_UNALLOCATED;
	return false;
}

#define UT_SELF_NODE 3
#define UT_PEER_NODE 11
#define UT_UNDO_SEGMENT UINT16_C(0x1234)
#define UT_TT_SLOT UINT32_C(17)
#define UT_CLUSTER_EPOCH UINT32_C(0x10203040)
#define UT_RAW_XID ((TransactionId)UINT32_C(0x24681357))
#define UT_ANCHOR_LSN ((XLogRecPtr)UINT64_C(0x0102030405060708))
#define UT_READ_SCN ((SCN)UINT64_C(0x0011223344556677))
#define UT_COMMIT_SCN ((SCN)UINT64_C(0x0000000200000700))
#define UT_NATIVE_XID ((TransactionId)UINT32_C(798))
#define UT_NATIVE_HW UINT64_C(816)
#define UT_NEXT_FULL_XID UINT64_C(4195136)

/* Globals read by the retained real-resolver call graph. */
int cluster_node_id = UT_SELF_NODE;
int cluster_dg_role = CLUSTER_DG_ROLE_PRIMARY;
int cluster_subtrans_max_chain_depth = 32;
bool cluster_enable_adg = false;
bool cluster_crossnode_runtime_visibility = false;
bool cluster_crossnode_write_write = false;
bool cluster_cf_terminal_authority = false;
bool cluster_xnode_profile_enabled = false;
ClusterXnodeProfileShared *ClusterXnodeProfileCtl = NULL;
static LWLockPadded ut_main_lwlocks[45];
static VariableCacheData ut_variable_cache;
LWLockPadded *MainLWLockArray = ut_main_lwlocks;
VariableCache ShmemVariableCache = &ut_variable_cache;

typedef struct ResolverReceiptCalls {
	int memo_probe;
	int memo_install;
	int resolve_note;
	int peer_stamp;
	int overlay;
	int wire;
	int clog;
	int durable;
	int pair_resolve;
	int exact_resolve;
	int prehistory_probe;
	int prehistory_lock;
	int prehistory_unlock;
	int prehistory_status;
	int prehistory_note;
	int truncation_lock;
	int truncation_unlock;
} ResolverReceiptCalls;

static ResolverReceiptCalls ut_calls;
static ClusterTTStatusKey ut_seen_key;
static ClusterTTStatusKey ut_installed_key;
static ClusterTTStatus ut_memo_status;
static SCN ut_memo_scn;
static uint8 ut_installed_status;
static SCN ut_installed_scn;
static ClusterUndoVerdictResult ut_pair_verdict;
static int32 ut_stamped_node;
static ClusterTouchKind ut_stamped_kind;
static bool ut_memo_saw_resolve_scope;
static bool ut_memo_hit;
static bool ut_native_prehistory_armed;
static bool ut_slotless_bound_armed;
static bool ut_overlay_hit;
static ClusterTTStatus ut_overlay_status;
static ClusterUndoVerdictResult ut_origin_verdict;
static int ut_origin_asks;
static int ut_recycled_asks;
static bool ut_recycled_proven;
static bool ut_recycled_committed;
static bool ut_recycled_bound;
static uint64 ut_evidence_metrics[CLUSTER_VIS_METRIC_COUNT];
bool cluster_enabled = true;
int cluster_tt_status_hint_outbound_capacity = 2;
int cluster_tt_status_hint_emit_mode = CLUSTER_TT_STATUS_HINT_EMIT_ALL_STATUS;
int cluster_multixact_member_overlay_max_members = 256;
int cluster_multixact_hint_outbound_slots = 2;
BackendType MyBackendType = B_LMON;
static bool ut_hint_install_succeeds;
static int ut_hint_installs;
static int ut_hint_wakes;
static int ut_hint_sends;
static ClusterNodeInfo ut_hint_nodes[4];
static ClusterConf ut_hint_conf = { .node_count = 4 };
ClusterConf *ClusterConfShmem = &ut_hint_conf;
static PGAlignedBlock ut_visibility_page;
static BufferDescPadded ut_visibility_buffer;
int NBuffers = 1;
int NLocBuffer = 0;
char *BufferBlocks = ut_visibility_page.data;
Block *LocalBufferBlockPointers;
BufferDescPadded *BufferDescriptors = &ut_visibility_buffer;
bool cluster_shared_catalog = false;
bool cluster_multi_xmax_remote_resolve = true;
bool cluster_test_force_visibility_cluster_path = false;
bool cluster_recmerge_window_active = false;
bool cluster_recmerge_apply_foreign = false;
uint64 cluster_recmerge_window_scn;
uint64 cluster_recmerge_window_own_lsn;
static uint32 ut_wait_event;
uint32 *my_wait_event_info = &ut_wait_event;
static bool ut_exit_fixture;
static ClusterUndoTTSlotRef ut_exit_ref;
static bool ut_exit_exact_proof;
static bool ut_full_scratch_fixture;
static int ut_full_scratch_scenario;
static int ut_history_origin;
static uint64 ut_current_epoch;
static int ut_native_calls;
static int ut_hint_mutations;
static int ut_live_cr_gate_calls;
static sigjmp_buf ut_error_jump;
static bool ut_error_armed;
static int ut_error_level;
static int ut_error_code;
static char ut_error_detail[512];
static char ut_error_message[256];

int
cluster_conf_node_count(void)
{
	return 4;
}
const ClusterNodeInfo *
cluster_conf_lookup_node(int32 node_id)
{
	return node_id >= 0 && node_id < 4 ? &ut_hint_nodes[node_id] : NULL;
}
void
cluster_lmon_wakeup(void)
{}
void
cluster_lmon_duty_mark_dirty(ClusterLmonDuty duty pg_attribute_unused())
{}

void
cluster_ic_send_envelope_fanout(uint8 type, const void *payload pg_attribute_unused(),
								uint32 len pg_attribute_unused(), ClusterICFanoutResult per_peer[])
{
	int i;

	UT_ASSERT_EQ(type, PGRAC_IC_MSG_TT_STATUS_HINT);
	ut_hint_sends++;
	for (i = 0; i < CLUSTER_MAX_NODES; i++)
		per_peer[i] = CLUSTER_IC_FANOUT_PEER_DOWN;
	per_peer[0] = CLUSTER_IC_FANOUT_WOULD_BLOCK; /* admitted, not a drop */
	per_peer[1] = CLUSTER_IC_FANOUT_NOT_ADMITTED;
	per_peer[2] = CLUSTER_IC_FANOUT_HARD_ERROR;
}

void
cluster_txw_wake_waiters(const ClusterTTStatusKey *key pg_attribute_unused())
{
	UT_ASSERT(ut_hint_install_succeeds);
	UT_ASSERT(ut_hint_installs > 0);
	ut_hint_wakes++;
}

ClusterSemanticAdmissionResult
cluster_multixact_source_dispatch(ClusterMultiXactSourceOp op pg_attribute_unused(),
								  const ClusterMultiXactSourceRequest *request
									  pg_attribute_unused(),
								  ClusterMultiXactSourceResult *result pg_attribute_unused())
{
	return CLUSTER_SEMANTIC_ADMISSION_CLOSED;
}

int
errcode(int code)
{
	ut_error_code = code;
	return 0;
}
int
errmsg(const char *fmt, ...)
{
	va_list args;
	va_start(args, fmt);
	vsnprintf(ut_error_message, sizeof(ut_error_message), fmt, args);
	va_end(args);
	return 0;
}
int
errdetail(const char *fmt, ...)
{
	va_list args;
	va_start(args, fmt);
	vsnprintf(ut_error_detail, sizeof(ut_error_detail), fmt, args);
	va_end(args);
	return 0;
}
int
errhint(const char *fmt pg_attribute_unused(), ...)
{
	return 0;
}
bool
errstart_cold(int level, const char *domain pg_attribute_unused())
{
	ut_error_level = level;
	return true;
}

void
cluster_vis_evidence_note(ClusterVisEvidenceMetric metric)
{
	UT_ASSERT((unsigned)metric < CLUSTER_VIS_METRIC_COUNT);
	ut_evidence_metrics[metric]++;
}

static ClusterUndoTTSlotRef
ut_exact_peer_ref(void)
{
	ClusterUndoTTSlotRef ref;

	memset(&ref, 0, sizeof(ref));
	ref.origin_node_id = UT_PEER_NODE;
	ref.undo_segment_id = UT_UNDO_SEGMENT;
	ref.tt_slot_id = UT_TT_SLOT;
	ref.cluster_epoch = UT_CLUSTER_EPOCH;
	ref.local_xid = UT_RAW_XID;
	ref.cached_commit_scn = InvalidScn;
	ref.has_cached_status = false;
	return ref;
}

static void
ut_reset(ClusterTTStatus status, SCN scn)
{
	memset(&ut_calls, 0, sizeof(ut_calls));
	memset(&ut_seen_key, 0xA5, sizeof(ut_seen_key));
	memset(&ut_installed_key, 0xA5, sizeof(ut_installed_key));
	ut_memo_status = status;
	ut_memo_scn = scn;
	ut_installed_status = UINT8_MAX;
	ut_installed_scn = InvalidScn;
	memset(&ut_pair_verdict, 0, sizeof(ut_pair_verdict));
	ut_pair_verdict.kind = CLUSTER_UNDO_VERDICT_UNKNOWN_FAIL_CLOSED;
	ut_pair_verdict.commit_scn = InvalidScn;
	ut_stamped_node = -1;
	ut_stamped_kind = CLUSTER_TOUCH_KIND_COUNT;
	ut_memo_saw_resolve_scope = false;

	cluster_node_id = UT_SELF_NODE;
	cluster_dg_role = CLUSTER_DG_ROLE_PRIMARY;
	cluster_enable_adg = false;
	cluster_crossnode_runtime_visibility = false;
	cluster_crossnode_write_write = false;
	cluster_cf_terminal_authority = false;
	cluster_xnode_profile_enabled = false;
	ClusterXnodeProfileCtl = NULL;
	ut_memo_hit = true;
	ut_native_prehistory_armed = false;
	ut_slotless_bound_armed = false;
	ut_overlay_hit = false;
	ut_overlay_status = CLUSTER_TT_STATUS_UNKNOWN;
	memset(&ut_origin_verdict, 0, sizeof(ut_origin_verdict));
	ut_origin_verdict.kind = CLUSTER_UNDO_VERDICT_UNKNOWN_FAIL_CLOSED;
	ut_origin_asks = 0;
	ut_recycled_asks = 0;
	ut_recycled_proven = false;
	ut_recycled_committed = true;
	ut_recycled_bound = false;
	ut_exit_fixture = false;
	ut_exit_exact_proof = false;
	ut_full_scratch_fixture = false;
	ut_full_scratch_scenario = 0;
	ut_history_origin = UT_PEER_NODE;
	ut_current_epoch = UT_CLUSTER_EPOCH;
	ut_native_calls = 0;
	ut_hint_mutations = 0;
	ut_live_cr_gate_calls = 0;
	ut_error_armed = false;
	ut_error_level = 0;
	ut_error_code = 0;
	ut_error_detail[0] = '\0';
	ut_error_message[0] = '\0';
	memset(ut_evidence_metrics, 0, sizeof(ut_evidence_metrics));
	memset(&ut_variable_cache, 0, sizeof(ut_variable_cache));
	ut_variable_cache.oldestClogXid = FirstNormalTransactionId;
}

/* Expected boundaries on the memo-hit path. */
bool
cluster_touched_peers_stamp(int32 node_id, ClusterTouchKind kind)
{
	ut_calls.peer_stamp++;
	ut_stamped_node = node_id;
	ut_stamped_kind = kind;
	return true;
}

void
cluster_lever_c_note_resolve(void)
{
	ut_calls.resolve_note++;
}

bool
cluster_vis_memo_probe(const ClusterTTStatusKey *key, uint8 *status_out, SCN *scn_out)
{
	ut_calls.memo_probe++;
	ut_seen_key = *key;
	ut_memo_saw_resolve_scope = cluster_vis_resolve_in_flight();
	if (!ut_memo_hit)
		return false;
	*status_out = (uint8)ut_memo_status;
	*scn_out = ut_memo_scn;
	return true;
}

/* Overlay/source fallbacks: none may execute after an exact memo hit. */
ClusterSemanticAdmissionResult
cluster_tt_status_source_dispatch(ClusterTTStatusSourceOp op pg_attribute_unused(),
								  const ClusterTTStatusSourceRequest *request pg_attribute_unused(),
								  ClusterTTStatusSourceResult *result pg_attribute_unused())
{
	ut_calls.overlay++;
	if (op == CLUSTER_TT_SOURCE_INSTALL_LOCAL || op == CLUSTER_TT_SOURCE_INSTALL_SUBCOMMITTED) {
		ut_hint_installs++;
		memset(result, 0, sizeof(*result));
		result->bool_value = ut_hint_install_succeeds;
		return CLUSTER_SEMANTIC_ADMISSION_OK;
	}
	if (ut_overlay_hit) {
		UT_ASSERT_EQ(op, CLUSTER_TT_SOURCE_LOOKUP);
		UT_ASSERT_EQ(request->key->local_xid, UT_RAW_XID);
		memset(result, 0, sizeof(*result));
		result->bool_value = true;
		result->lookup.authoritative = true;
		result->lookup.status = ut_overlay_status;
		result->lookup.commit_scn = InvalidScn;
		return CLUSTER_SEMANTIC_ADMISSION_OK;
	}
	return CLUSTER_SEMANTIC_ADMISSION_CLOSED;
}

ClusterTTStatusResult
cluster_subtrans_lookup_parent(const ClusterTTStatusResult *child_result pg_attribute_unused(),
							   int depth_remaining pg_attribute_unused())
{
	ClusterTTStatusResult result;

	ut_calls.overlay++;
	memset(&result, 0, sizeof(result));
	result.status = CLUSTER_TT_STATUS_UNKNOWN;
	return result;
}

void
cluster_vis_memo_install(const ClusterTTStatusKey *key, uint8 status,
						 SCN commit_scn)
{
	ut_calls.memo_install++;
	ut_installed_key = *key;
	ut_installed_status = status;
	ut_installed_scn = commit_scn;
	ut_memo_status = (ClusterTTStatus)status;
	ut_memo_scn = commit_scn;
	ut_memo_hit = true;
}

void
cluster_lever_c_note_tt_lookup(bool stamp_cached_present pg_attribute_unused(),
							   bool stamp_contradicted pg_attribute_unused())
{
	ut_calls.overlay++;
}

void
cluster_vis_bump_overlay_refresh_count(void)
{
	ut_calls.overlay++;
}

/* Runtime/wire fallbacks, including the synthetic UNKNOWN widening leg. */
bool
cluster_runtime_visibility_try_resolve_remote(int origin_node pg_attribute_unused(),
											  uint32 undo_segment_id pg_attribute_unused(),
											  TransactionId raw_xid pg_attribute_unused(),
											  SCN read_scn pg_attribute_unused(),
											  bool authoritative pg_attribute_unused(),
											  bool *out_committed, SCN *out_commit_scn,
											  bool *out_commit_scn_is_bound)
{
	ut_calls.wire++;
	ut_recycled_asks++;
	if (ut_recycled_proven) {
		UT_ASSERT_EQ(origin_node, UT_PEER_NODE);
		UT_ASSERT_EQ(raw_xid, UT_RAW_XID);
		UT_ASSERT(!authoritative);
		*out_committed = ut_recycled_committed;
		*out_commit_scn = ut_recycled_committed ? UT_COMMIT_SCN : InvalidScn;
		*out_commit_scn_is_bound = ut_recycled_committed && ut_recycled_bound;
		return true;
	}
	if (ut_slotless_bound_armed) {
		UT_ASSERT_EQ(origin_node, UT_PEER_NODE);
		UT_ASSERT_EQ(undo_segment_id, UT_UNDO_SEGMENT);
		UT_ASSERT_EQ(raw_xid, UT_RAW_XID);
		UT_ASSERT_EQ(read_scn, UT_READ_SCN);
		UT_ASSERT(!authoritative);
		*out_committed = true;
		*out_commit_scn = UT_COMMIT_SCN;
		*out_commit_scn_is_bound = true;
		return true;
	}
	if (out_committed != NULL)
		*out_committed = false;
	if (out_commit_scn != NULL)
		*out_commit_scn = InvalidScn;
	if (out_commit_scn_is_bound != NULL)
		*out_commit_scn_is_bound = false;
	return false;
}

ClusterUndoVerdictResult
cluster_undo_verdict_resolve(int origin_node pg_attribute_unused(),
							 uint32 undo_segment_id pg_attribute_unused(),
							 TransactionId raw_xid pg_attribute_unused(),
							 uint32 expected_tt_slot_id pg_attribute_unused(),
							 SCN read_scn pg_attribute_unused(),
							 bool authoritative pg_attribute_unused())
{
	ut_calls.wire++;
	ut_origin_asks++;
	if (ut_full_scratch_fixture && ut_full_scratch_scenario >= 30) {
		UT_ASSERT(cluster_vis_resolve_in_flight());
		UT_ASSERT_EQ(origin_node, ut_history_origin);
		UT_ASSERT(origin_node != ut_exit_ref.origin_node_id);
		UT_ASSERT_EQ(raw_xid, UT_RAW_XID);
		UT_ASSERT_EQ(undo_segment_id, UT_UNDO_SEGMENT);
		UT_ASSERT_EQ(expected_tt_slot_id, 0);
		UT_ASSERT_EQ(read_scn, UT_READ_SCN);
		UT_ASSERT(!authoritative);
		if (ut_full_scratch_scenario == 38)
			ut_current_epoch++;
		if (ut_full_scratch_scenario == 40)
			pg_re_throw();
		return ut_origin_verdict;
	}
	UT_ASSERT_EQ(origin_node, UT_PEER_NODE);
	UT_ASSERT_EQ(raw_xid, UT_RAW_XID);
	UT_ASSERT_EQ(expected_tt_slot_id, UT_TT_SLOT);
	UT_ASSERT(authoritative);
	return ut_origin_verdict;
}

ClusterUndoVerdictResult
cluster_undo_verdict_resolve_freshref_c1b_pair(
	int origin_node, uint32 undo_segment_id, TransactionId raw_xid,
	TransactionId ref_xid, uint32 expected_tt_slot_id, uint32 ref_epoch,
	SCN cached_commit_scn, SCN read_scn)
{
	ut_calls.wire++;
	ut_calls.pair_resolve++;
	if (ut_full_scratch_fixture && ut_full_scratch_scenario >= 12) {
		UT_ASSERT_EQ(origin_node, ut_exit_ref.origin_node_id);
		UT_ASSERT_EQ(raw_xid, UT_RAW_XID);
		UT_ASSERT_EQ(ref_xid, UT_RAW_XID);
		UT_ASSERT_EQ(undo_segment_id, UT_UNDO_SEGMENT);
		UT_ASSERT_EQ(expected_tt_slot_id, UT_TT_SLOT);
		UT_ASSERT_EQ(ref_epoch, ut_current_epoch);
		UT_ASSERT_EQ(cached_commit_scn, ut_exit_ref.cached_commit_scn);
		UT_ASSERT_EQ(read_scn, UT_READ_SCN);
		UT_ASSERT(cluster_vis_resolve_in_flight());
		if (ut_full_scratch_scenario == 19)
			ut_current_epoch++;
		if (ut_full_scratch_scenario == 20)
			pg_re_throw();
		return ut_pair_verdict;
	}
	UT_ASSERT_EQ(origin_node, UT_PEER_NODE);
	UT_ASSERT_EQ(undo_segment_id, UT_UNDO_SEGMENT);
	UT_ASSERT_EQ(raw_xid, UT_RAW_XID);
	UT_ASSERT_EQ(ref_xid, UT_RAW_XID);
	UT_ASSERT_EQ(expected_tt_slot_id, UT_TT_SLOT);
	UT_ASSERT_EQ(ref_epoch, UT_CLUSTER_EPOCH);
	UT_ASSERT_EQ(cached_commit_scn, UT_COMMIT_SCN);
	UT_ASSERT_EQ(read_scn, UT_READ_SCN);
	return ut_pair_verdict;
}

bool
cluster_vis_freshref_c1b_pair_request_eligible(
	TransactionId raw_xid, TransactionId ref_xid, bool has_cached_status,
	SCN cached_commit_scn, uint32 ref_epoch, uint64 current_epoch,
	int32 origin_node, int32 local_node, uint32 segment_id,
	uint32 expected_tt_slot_id)
{
	if (ut_full_scratch_fixture && ut_full_scratch_scenario >= 12)
		return raw_xid == UT_RAW_XID && ref_xid == UT_RAW_XID && has_cached_status
			   && SCN_VALID(cached_commit_scn) && ref_epoch == ut_current_epoch
			   && current_epoch == ut_current_epoch && origin_node == UT_PEER_NODE
			   && local_node == UT_SELF_NODE && segment_id == UT_UNDO_SEGMENT
			   && expected_tt_slot_id == UT_TT_SLOT;
	return raw_xid == UT_RAW_XID && ref_xid == UT_RAW_XID
		   && has_cached_status && cached_commit_scn == UT_COMMIT_SCN
		   && ref_epoch == UT_CLUSTER_EPOCH
		   && current_epoch == UT_CLUSTER_EPOCH
		   && origin_node == UT_PEER_NODE && local_node == UT_SELF_NODE
		   && segment_id == UT_UNDO_SEGMENT
		   && expected_tt_slot_id == UT_TT_SLOT;
}

int
cluster_xid_origin_slot(TransactionId xid pg_attribute_unused())
{
	ut_calls.wire++;
	if (ut_full_scratch_fixture && ut_full_scratch_scenario >= 30) {
		UT_ASSERT_EQ(xid, UT_RAW_XID);
		return ut_full_scratch_scenario == 39 ? -1 : ut_history_origin;
	}
	return UT_PEER_NODE;
}

void
cluster_vis_freshref_verdict_note_resolved(void)
{
	ut_calls.wire++;
}

void
cluster_vis_freshref_verdict_note_failclosed(void)
{
	ut_calls.wire++;
}

void
cluster_rtvis_note_underivable_failclosed(void)
{
	ut_calls.wire++;
}

void
cluster_vis_bump_vis_variant_unknown_failclosed_count(void)
{
	ut_calls.wire++;
}

/* Native/ADG CLOG alternatives: the peer exact-ref leg must avoid all. */
bool
RecoveryInProgress(void)
{
	ut_calls.clog++;
	return false;
}

bool
TransactionIdDidCommit(TransactionId xid pg_attribute_unused())
{
	if (ut_exit_fixture)
		ut_native_calls++;
	ut_calls.clog++;
	return false;
}

XidStatus
TransactionIdGetStatus(TransactionId xid pg_attribute_unused(), XLogRecPtr *lsn)
{
	ut_calls.clog++;
	ut_calls.prehistory_status++;
	if (lsn != NULL)
		*lsn = InvalidXLogRecPtr;
	return ut_native_prehistory_armed ? TRANSACTION_STATUS_COMMITTED
									  : TRANSACTION_STATUS_IN_PROGRESS;
}

bool
TransactionIdPrecedes(TransactionId id1 pg_attribute_unused(),
					  TransactionId id2 pg_attribute_unused())
{
	ut_calls.clog++;
	return false;
}

FullTransactionId
ReadNextFullTransactionId(void)
{
	ut_calls.clog++;
	return FullTransactionIdFromU64(UT_NEXT_FULL_XID);
}

uint64
cluster_cr_native_prehistory_covered_hw(void)
{
	ut_calls.prehistory_probe++;
	return ut_native_prehistory_armed ? UT_NATIVE_HW : 0;
}

void
cluster_cr_native_prehistory_reader_lock(void)
{
	ut_calls.clog++;
	ut_calls.prehistory_lock++;
}

void
cluster_cr_native_prehistory_reader_unlock(void)
{
	ut_calls.clog++;
	ut_calls.prehistory_unlock++;
}

bool
cluster_xid_native_prehistory_provable_full(uint64 next_full_xid pg_attribute_unused(),
											uint64 covered_hw_full pg_attribute_unused(),
											TransactionId xid pg_attribute_unused())
{
	ut_calls.clog++;
	return ut_native_prehistory_armed && next_full_xid == UT_NEXT_FULL_XID
		   && covered_hw_full == UT_NATIVE_HW && xid == UT_NATIVE_XID;
}

bool
cluster_xid_provably_foreign(TransactionId xid pg_attribute_unused())
{
	ut_calls.clog++;
	return false;
}

void
cluster_rtvis_note_native_prehistory_local(void)
{
	ut_calls.clog++;
	ut_calls.prehistory_note++;
}

bool
LWLockAcquire(LWLock *lock pg_attribute_unused(), LWLockMode mode pg_attribute_unused())
{
	ut_calls.clog++;
	ut_calls.truncation_lock++;
	return true;
}

void
LWLockRelease(LWLock *lock pg_attribute_unused())
{
	ut_calls.clog++;
	ut_calls.truncation_unlock++;
}

/* Materialized/durable authority alternatives: also forbidden here. */
bool
cluster_merged_instance_is_materialized(int origin_node pg_attribute_unused())
{
	ut_calls.durable++;
	return false;
}

uint64
cluster_merged_instance_recovered_through(int origin_node pg_attribute_unused())
{
	ut_calls.durable++;
	return 0;
}

bool
cluster_tt_recovery_remote_authority_covers(uint64 recovered_through pg_attribute_unused(),
											uint64 anchor_lsn pg_attribute_unused())
{
	ut_calls.durable++;
	return false;
}

uint64
cluster_epoch_get_current(void)
{
	ut_calls.durable++;
	return ut_current_epoch;
}

ClusterRemoteXactOutcome
cluster_remote_outcome_terminal_authorized(int origin_node pg_attribute_unused(),
										   TransactionId xid pg_attribute_unused(),
										   uint64 observed_epoch pg_attribute_unused(),
										   uint64 current_epoch pg_attribute_unused(),
										   bool retention_required pg_attribute_unused(),
										   bool retention_proven pg_attribute_unused(),
										   SCN *out_scn pg_attribute_unused())
{
	ut_calls.durable++;
	return CLUSTER_REMOTE_XACT_INDOUBT;
}

ClusterRemoteXactOutcome
cluster_remote_outcome_durable_checked(int origin_node pg_attribute_unused(),
									   TransactionId xid pg_attribute_unused(),
									   SCN *out_scn pg_attribute_unused())
{
	ut_calls.durable++;
	return CLUSTER_REMOTE_XACT_INDOUBT;
}

void
cluster_tt_recovery_count_remote_active_failclosed(void)
{
	ut_calls.durable++;
}

ClusterTxOutcome
cluster_tx_resolve_exact(const ClusterTxLocator *locator pg_attribute_unused(),
	ClusterTxResolveMode mode pg_attribute_unused(),
	ClusterTxResolution *out, ClusterTxResolveReason *reason_out)
{
	ut_calls.exact_resolve++;
	if (ut_full_scratch_fixture && ut_full_scratch_scenario >= 12) {
		memset(out, 0, sizeof(*out));
		UT_ASSERT(cluster_vis_resolve_in_flight());
		UT_ASSERT_EQ(locator->xid, UT_RAW_XID);
		if (ut_full_scratch_scenario == 21 || ut_full_scratch_scenario == 24) {
			out->outcome = CLUSTER_TX_IN_PROGRESS;
			out->locator_echo = *locator;
			*reason_out = CLUSTER_TX_RESOLVE_NONE;
			return CLUSTER_TX_IN_PROGRESS;
		}
		*reason_out = CLUSTER_TX_RESOLVE_XID_MISMATCH;
		return CLUSTER_TX_UNKNOWN; /* canonical TT already reused */
	}
	if (ut_exit_exact_proof) {
		UT_ASSERT_EQ(mode, CLUSTER_TX_RESOLVE_VISIBILITY);
		UT_ASSERT_EQ(locator->xid, UT_RAW_XID);
		UT_ASSERT_EQ(locator->tt_wrap, TT_WRAP_INVALID);
		if (ut_full_scratch_fixture) {
			const ClusterItlSlotData *slot = ClusterPageGetItlSlots(ut_visibility_page.data);

			UT_ASSERT_EQ(locator->uba.raw[0], slot->undo_segment_head.raw[0]);
			UT_ASSERT_EQ(locator->uba.raw[1], slot->undo_segment_head.raw[1]);
			UT_ASSERT_EQ(locator->itl_kind, ITL_FLAG_ACTIVE);
			UT_ASSERT_EQ(locator->itl_slot_index, 0);
			UT_ASSERT(cluster_vis_resolve_in_flight());
			if (ut_full_scratch_scenario == 7)
				pg_re_throw();
		}
		memset(out, 0, sizeof(*out));
		out->outcome = CLUSTER_TX_COMMITTED;
		out->proof_kind = CLUSTER_TX_PROOF_ORIGIN_DURABLE_TT_CLOG;
		out->commit_scn = UT_COMMIT_SCN;
		out->locator_echo = *locator;
		out->locator_echo.tt_wrap = 7;
		*reason_out = CLUSTER_TX_RESOLVE_NONE;
		if (ut_full_scratch_fixture) {
			if (ut_full_scratch_scenario == 1 || ut_full_scratch_scenario == 2) {
				out->outcome
					= ut_full_scratch_scenario == 1 ? CLUSTER_TX_ABORTED : CLUSTER_TX_IN_PROGRESS;
				out->commit_scn = InvalidScn;
			}
			if (ut_full_scratch_scenario == 3)
				out->commit_scn = UT_READ_SCN + 1;
			if (ut_full_scratch_scenario == 5)
				out->commit_scn = InvalidScn;
			if (ut_full_scratch_scenario == 6) {
				out->outcome = CLUSTER_TX_PREPARED;
				out->proof_kind = CLUSTER_TX_PROOF_ORIGIN_TWOPHASE;
				out->commit_scn = InvalidScn;
			}
			return out->outcome;
		}
		return CLUSTER_TX_COMMITTED;
	}
	if (out != NULL)
		memset(out, 0, sizeof(*out));
	if (reason_out != NULL)
		*reason_out = CLUSTER_TX_RESOLVE_PROTOCOL;
	return CLUSTER_TX_UNKNOWN;
}

const char *
cluster_tx_resolve_reason_name(ClusterTxResolveReason reason pg_attribute_unused())
{
	return "PROTOCOL";
}

bool
errstart(int elevel, const char *domain pg_attribute_unused())
{
	ut_error_level = elevel;
	return true;
}

int
errmsg_internal(const char *fmt pg_attribute_unused(), ...)
{
	return 0;
}

void
errfinish(const char *filename pg_attribute_unused(), int lineno pg_attribute_unused(),
		  const char *funcname pg_attribute_unused())
{
	if (ut_error_level >= ERROR) {
		UT_ASSERT(ut_error_armed);
		if (PG_exception_stack != NULL)
			pg_re_throw();
		if (ut_error_armed)
			siglongjmp(ut_error_jump, 1);
		abort();
	}
}

bool
cluster_itl_get_tt_ref(Page page, uint8 index, ClusterUndoTTSlotRef *out)
{
	UT_ASSERT(ut_exit_fixture);
	UT_ASSERT(page == ut_visibility_page.data);
	if (index != 0)
		return false;
	*out = ut_exit_ref;
	return true;
}

bool
cluster_itl_find_data_tt_ref_by_xid(Page page pg_attribute_unused(),
									TransactionId xid pg_attribute_unused(),
									ClusterUndoTTSlotRef *out pg_attribute_unused())
{
	return false;
}
bool
cluster_itl_find_lock_tt_ref_by_xmax(Page page pg_attribute_unused(),
									 TransactionId xid pg_attribute_unused(),
									 ClusterUndoTTSlotRef *out pg_attribute_unused())
{
	return false;
}
bool
cluster_itl_find_multixact_origin_by_xmax(Page page pg_attribute_unused(),
										  MultiXactId xid pg_attribute_unused(),
										  uint16 *out pg_attribute_unused())
{
	return false;
}
bool
cluster_tx_locator_from_itl(Page page, uint8 index, ClusterTxLocator *out,
							ClusterTxResolveReason *reason)
{
	UT_ASSERT(page == ut_visibility_page.data);
	UT_ASSERT_EQ(index, 0);
	if (ut_full_scratch_fixture
		&& (ut_full_scratch_scenario == 8 || ut_full_scratch_scenario == 42)) {
		*reason = CLUSTER_TX_RESOLVE_BAD_UBA;
		return false;
	}
	memset(out, 0, sizeof(*out));
	out->xid = UT_RAW_XID;
	out->tt_wrap = 7;
	if (ut_full_scratch_fixture) {
		const ClusterItlSlotData *slot = ClusterPageGetItlSlots(page);

		out->uba = slot->undo_segment_head;
		out->itl_kind = slot->flags;
		out->itl_slot_index = index;
		if (ut_full_scratch_scenario >= 30)
			out->xid = slot->xid;
	}
	*reason = CLUSTER_TX_RESOLVE_NONE;
	return true;
}
void
BufferGetTag(Buffer buffer pg_attribute_unused(), RelFileLocator *rlocator, ForkNumber *fork,
			 BlockNumber *block)
{
	memset(rlocator, 0, sizeof(*rlocator));
	*fork = MAIN_FORKNUM;
	*block = 0;
}
void
cluster_catalog_stats_vis_resolve_inc(void)
{}
void
cluster_catalog_stats_vis_unknown_inc(void)
{}

/* Native transaction and page-mutation boundaries are traps in these legs.
 * The real resolver above supplies the status; these never manufacture it. */
bool
TransactionIdIsCurrentTransactionId(TransactionId xid pg_attribute_unused())
{
	return false;
}
bool
TransactionIdIsInProgress(TransactionId xid pg_attribute_unused())
{
	ut_native_calls++;
	return false;
}
bool
XidInMVCCSnapshot(TransactionId xid pg_attribute_unused(), Snapshot snapshot pg_attribute_unused())
{
	ut_native_calls++;
	return false;
}
bool
MultiXactIdIsRunning(MultiXactId xid pg_attribute_unused(), bool lock_only pg_attribute_unused())
{
	ut_native_calls++;
	return false;
}
TransactionId
HeapTupleGetUpdateXid(HeapTupleHeader header pg_attribute_unused())
{
	ut_native_calls++;
	return InvalidTransactionId;
}
CommandId
HeapTupleHeaderGetCmin(HeapTupleHeader header pg_attribute_unused())
{
	ut_native_calls++;
	return InvalidCommandId;
}
CommandId
HeapTupleHeaderGetCmax(HeapTupleHeader header pg_attribute_unused())
{
	ut_native_calls++;
	return InvalidCommandId;
}
XLogRecPtr
TransactionIdGetCommitLSN(TransactionId xid pg_attribute_unused())
{
	ut_native_calls++;
	return InvalidXLogRecPtr;
}
bool
BufferIsPermanent(Buffer buffer pg_attribute_unused())
{
	ut_hint_mutations++;
	return false;
}
XLogRecPtr
BufferGetLSNAtomic(Buffer buffer pg_attribute_unused())
{
	ut_hint_mutations++;
	return InvalidXLogRecPtr;
}
bool
XLogNeedsFlush(XLogRecPtr lsn pg_attribute_unused())
{
	ut_hint_mutations++;
	return false;
}
void
MarkBufferDirtyHint(Buffer buffer pg_attribute_unused(), bool standard pg_attribute_unused())
{
	ut_hint_mutations++;
}
bool
cluster_bufmgr_block_write_permitted(Buffer buffer pg_attribute_unused())
{
	UT_ASSERT(false); /* these read/committed-writer fixtures never normalize xmax */
	return false;
}
bool
cluster_itl_cleanout_lazy(Buffer buffer pg_attribute_unused(), uint8 index pg_attribute_unused(),
						  TransactionId xid pg_attribute_unused(), SCN scn pg_attribute_unused())
{
	ut_hint_mutations++;
	return false;
}
bool
ItemPointerEquals(ItemPointer a, ItemPointer b)
{
	return ItemPointerGetBlockNumber(a) == ItemPointerGetBlockNumber(b)
		   && ItemPointerGetOffsetNumber(a) == ItemPointerGetOffsetNumber(b);
}
bool
cluster_cr_no_peer_fastpath_eligible(Snapshot snapshot pg_attribute_unused())
{
	return false;
}
ClusterCrVerdict
cluster_cr_satisfies_mvcc(HeapTuple tuple pg_attribute_unused(),
						  Snapshot snapshot pg_attribute_unused(),
						  Buffer buffer pg_attribute_unused(), bool *visible pg_attribute_unused())
{
	ut_live_cr_gate_calls++;
	return CLUSTER_CR_NOT_APPLICABLE;
}
bool
cluster_xid_foreign_class_cheap(TransactionId xid)
{
	return xid == UT_RAW_XID;
}
bool
cluster_xid_is_mine(TransactionId xid pg_attribute_unused())
{
	return false;
}
int
cluster_mxid_origin_slot(MultiXactId mxid pg_attribute_unused())
{
	ut_native_calls++;
	return -1;
}
ClusterSemanticAdmissionResult
cluster_multixact_remote_xmax_visibility_dispatch(
	const ClusterMultiXactSourceRequest *request pg_attribute_unused(),
	ClusterMultiXactSourceResult *result pg_attribute_unused())
{
	ut_native_calls++;
	return CLUSTER_SEMANTIC_ADMISSION_CLOSED;
}
bool
cluster_test_lookup_visibility_inject(TransactionId xid pg_attribute_unused(),
									  ClusterUndoTTSlotRef *ref pg_attribute_unused())
{
	return false;
}
void
cluster_vis53r97_note_multi_unresolvable(void)
{}
void
cluster_vis53r97_note_xmax_unprovable(void)
{}
void
cluster_vis53r97_note_xmin_overlay_verdict_ask(void)
{}
void
cluster_vis53r97_note_xmin_overlay_verdict_hit(void)
{}
void
cluster_vis_bump_vis_update_fork_count(void)
{}
void
cluster_vis_bump_xmax_resolved_count(void)
{}
int
scn_time_cmp(SCN a, SCN b)
{
	return scn_local(a) < scn_local(b) ? -1 : scn_local(a) > scn_local(b) ? 1 : 0;
}

/* Execute the actual MVCC body, not the HOT test's counted visibility seam.
 * A complete tuple proof must not ask CR to inspect a recycled DATA slot. */
static void
ut_complete_frozen_live_case(uint16 mask, uint64 epoch_delta, bool complete)
{
	HeapTupleData tuple = { 0 };
	SnapshotData snapshot = { 0 };
	HeapTupleHeader header;
	PGAlignedBlock before;
	volatile bool caught = false;
	volatile bool visible = false;

	ut_reset(CLUSTER_TT_STATUS_UNKNOWN, InvalidScn);
	ut_exit_fixture = true;
	ut_exit_ref = ut_exact_peer_ref();
	ut_exit_ref.local_xid++;
	ut_memo_hit = false;
	cluster_crossnode_runtime_visibility = true;
	memset(ut_visibility_page.data, 0, BLCKSZ);
	((PageHeader)ut_visibility_page.data)->pd_flags = PD_HAS_ITL;
	((PageHeader)ut_visibility_page.data)->pd_special = BLCKSZ - CLUSTER_ITL_SPECIAL_SIZE;
	((PageHeader)ut_visibility_page.data)->pd_upper = BLCKSZ - CLUSTER_ITL_SPECIAL_SIZE;
	((PageHeader)ut_visibility_page.data)->pd_lower = SizeOfPageHeaderData;
	((PageHeader)ut_visibility_page.data)->pd_pagesize_version = BLCKSZ | PG_PAGE_LAYOUT_VERSION;
	PageSetLSN(ut_visibility_page.data, UT_ANCHOR_LSN);
	header = (HeapTupleHeader)(ut_visibility_page.data + 1024);
	header->t_hoff = SizeofHeapTupleHeader;
	header->t_itl_slot_idx = 0;
	header->t_infomask = mask;
	HeapTupleHeaderSetXmin(header, UT_RAW_XID);
	HeapTupleHeaderSetXmax(header, (mask & HEAP_XMAX_INVALID) ? InvalidTransactionId : UT_RAW_XID);
	ItemPointerSet(&tuple.t_self, 0, 1);
	header->t_ctid = tuple.t_self;
	tuple.t_data = header;
	tuple.t_len = SizeofHeapTupleHeader;
	tuple.t_tableOid = FirstNormalObjectId;
	snapshot.snapshot_type = SNAPSHOT_MVCC;
	snapshot.cluster_source = SNAPSHOT_SOURCE_CLUSTER;
	snapshot.read_epoch = UT_CLUSTER_EPOCH + epoch_delta;
	snapshot.read_scn = UT_READ_SCN;
	memcpy(before.data, ut_visibility_page.data, BLCKSZ);
	ut_error_armed = true;
	if (sigsetjmp(ut_error_jump, 0) == 0)
		visible = cluster_heap_test_satisfies_mvcc(&tuple, &snapshot, 1);
	else
		caught = true;
	ut_error_armed = false;
	UT_ASSERT_EQ(caught, epoch_delta != 0 || !complete);
	UT_ASSERT_EQ(visible, epoch_delta == 0 && complete);
	UT_ASSERT_EQ(ut_live_cr_gate_calls, complete ? 0 : 1);
	if (epoch_delta != 0) {
		UT_ASSERT_EQ(ut_error_code, ERRCODE_CLUSTER_TT_STATUS_UNKNOWN);
		UT_ASSERT(strstr(ut_error_message, "snapshot stale across reconfig") != NULL);
	}
	UT_ASSERT_EQ(ut_native_calls, 0);
	UT_ASSERT_EQ(ut_hint_mutations, 0);
	UT_ASSERT_EQ(memcmp(before.data, ut_visibility_page.data, BLCKSZ), 0);
	ut_exit_fixture = false;
}

UT_TEST(test_complete_frozen_live_proof_precedes_legacy_cr)
{
	ut_complete_frozen_live_case(HEAP_XMIN_FROZEN | HEAP_XMAX_INVALID, 0, true);
}

UT_TEST(test_complete_frozen_live_proof_keeps_full_epoch_check)
{
	ut_complete_frozen_live_case(HEAP_XMIN_FROZEN | HEAP_XMAX_INVALID, 1, true);
	ut_complete_frozen_live_case(HEAP_XMIN_FROZEN | HEAP_XMAX_INVALID, UINT64_C(1) << 32, true);
}

UT_TEST(test_incomplete_live_proof_keeps_cr_and_authority_refusal)
{
	ut_complete_frozen_live_case(HEAP_XMIN_COMMITTED | HEAP_XMAX_INVALID, 0, false);
	ut_complete_frozen_live_case(HEAP_XMIN_FROZEN, 0, false);
}

UT_TEST(test_three_real_visibility_exits_route_recycle_and_unproven)
{
	static const char *exits[] = { "VIS_RECYCLED_XMIN", "VIS_RECYCLED_XMAX", "VIS_RECYCLED_XID" };
	volatile int exit_index;
	volatile int leg;

	/* Execute the real heap caller AND its real tuple/ref resolver.  The
	 * fixture substitutes only physical TT/read/transport boundaries. */
	for (exit_index = 0; exit_index < 3; exit_index++) {
		for (leg = 0; leg < 3; leg++) {
			HeapTupleData tuple = { 0 };
			HeapTupleHeader header;
			SnapshotData snapshot = { 0 };
			PGAlignedBlock before;
			volatile bool caught = false;

			ut_reset(CLUSTER_TT_STATUS_UNKNOWN, InvalidScn);
			ut_exit_fixture = true;
			ut_exit_ref = ut_exact_peer_ref();
			ut_exit_ref.local_xid += leg != 0;
			ut_exit_exact_proof = leg == 0;
			ut_recycled_proven = leg == 1;
			ut_memo_hit = false;
			cluster_crossnode_runtime_visibility = true;
			memset(ut_visibility_page.data, 0, BLCKSZ);
			((PageHeader)ut_visibility_page.data)->pd_flags = PD_HAS_ITL;
			((PageHeader)ut_visibility_page.data)->pd_lower = SizeOfPageHeaderData;
			((PageHeader)ut_visibility_page.data)->pd_special = BLCKSZ - CLUSTER_ITL_SPECIAL_SIZE;
			((PageHeader)ut_visibility_page.data)->pd_upper = BLCKSZ - CLUSTER_ITL_SPECIAL_SIZE;
			((PageHeader)ut_visibility_page.data)->pd_pagesize_version
				= BLCKSZ | PG_PAGE_LAYOUT_VERSION;
			PageSetLSN(ut_visibility_page.data, UT_ANCHOR_LSN);
			header = (HeapTupleHeader)(ut_visibility_page.data + 1024);
			header->t_hoff = SizeofHeapTupleHeader;
			header->t_itl_slot_idx = 0;
			header->t_infomask = exit_index == 1 ? HEAP_XMIN_FROZEN : HEAP_XMAX_INVALID;
			HeapTupleHeaderSetXmin(header, UT_RAW_XID);
			HeapTupleHeaderSetXmax(header, exit_index == 1 ? UT_RAW_XID : InvalidTransactionId);
			ItemPointerSet(&tuple.t_self, 0, 1);
			header->t_ctid = tuple.t_self;
			tuple.t_data = header;
			tuple.t_len = SizeofHeapTupleHeader;
			tuple.t_tableOid = FirstNormalObjectId;
			snapshot.snapshot_type = SNAPSHOT_MVCC;
			snapshot.cluster_source = SNAPSHOT_SOURCE_CLUSTER;
			snapshot.read_epoch = UT_CLUSTER_EPOCH;
			snapshot.read_scn = UT_READ_SCN;
			memcpy(before.data, ut_visibility_page.data, BLCKSZ);
			ut_error_armed = true;
			if (sigsetjmp(ut_error_jump, 0) == 0) {
				if (exit_index == 2)
					UT_ASSERT(cluster_heap_test_satisfies_mvcc(&tuple, &snapshot, 1));
				else
					UT_ASSERT_EQ(HeapTupleSatisfiesUpdate(&tuple, 1, 1),
								 exit_index == 1 ? TM_Deleted : TM_Ok);
			} else
				caught = true;
			ut_error_armed = false;
			UT_ASSERT_EQ(caught, leg == 2);
			if (leg == 2) {
				UT_ASSERT_EQ(ut_error_code, ERRCODE_CLUSTER_TT_STATUS_UNKNOWN);
				UT_ASSERT(strstr(ut_error_detail, exits[exit_index]) != NULL);
				UT_ASSERT(strstr(ut_error_detail, "RECYCLED_AUTHORITY_UNPROVABLE") != NULL);
			} else if (leg == 0) {
				UT_ASSERT_EQ(ut_calls.exact_resolve, 1);
				UT_ASSERT_EQ(ut_evidence_metrics[CLUSTER_VIS_METRIC_DURABLE_ROUTE_GAP], 1);
			} else
				UT_ASSERT_EQ(ut_evidence_metrics[CLUSTER_VIS_METRIC_RECYCLED_TERMINAL], 1);
			UT_ASSERT_EQ(ut_native_calls, 0);
			UT_ASSERT_EQ(ut_hint_mutations, 0);
			UT_ASSERT_EQ(memcmp(before.data, ut_visibility_page.data, BLCKSZ), 0);
		}
	}
	ut_exit_fixture = false;
}

void
ExceptionalCondition(const char *condition_name, const char *file_name, int line_number)
{
	fprintf(stderr, "unexpected Assert: %s at %s:%d\n", condition_name, file_name, line_number);
	abort();
}

static void
ut_assert_exact_key(void)
{
	UT_ASSERT_EQ(ut_seen_key.origin_node_id, UT_PEER_NODE);
	UT_ASSERT_EQ(ut_seen_key.undo_segment_id, UT_UNDO_SEGMENT);
	UT_ASSERT_EQ(ut_seen_key.tt_slot_id, UT_TT_SLOT);
	UT_ASSERT_EQ(ut_seen_key.cluster_epoch, UT_CLUSTER_EPOCH);
	UT_ASSERT_EQ(ut_seen_key.local_xid, UT_RAW_XID);
	UT_ASSERT_EQ(ut_seen_key._reserved, 0);
	UT_ASSERT_EQ(ut_seen_key._reserved2, 0);
}

static void
ut_assert_no_fallback(void)
{
	UT_ASSERT_EQ(ut_calls.overlay, 0);
	UT_ASSERT_EQ(ut_calls.memo_install, 0);
	UT_ASSERT_EQ(ut_calls.wire, 0);
	UT_ASSERT_EQ(ut_calls.clog, 0);
	UT_ASSERT_EQ(ut_calls.durable, 0);
}

static void
ut_run_memo_hit(ClusterTTStatus status, SCN memo_scn)
{
	ClusterUndoTTSlotRef ref = ut_exact_peer_ref();
	ClusterVisResolve out;

	ut_reset(status, memo_scn);
	memset(&out, 0xA5, sizeof(out));

	UT_ASSERT(!cluster_vis_resolve_in_flight());
	cluster_visibility_resolve_from_ref_scn(UT_RAW_XID, &ref, UT_ANCHOR_LSN, UT_READ_SCN, &out);
	UT_ASSERT(!cluster_vis_resolve_in_flight());

	UT_ASSERT_EQ(ut_calls.peer_stamp, 1);
	UT_ASSERT_EQ(ut_stamped_node, UT_PEER_NODE);
	UT_ASSERT_EQ(ut_stamped_kind, CLUSTER_TOUCH_VISIBILITY);
	UT_ASSERT_EQ(ut_calls.resolve_note, 1);
	UT_ASSERT_EQ(ut_calls.memo_probe, 1);
	UT_ASSERT(ut_memo_saw_resolve_scope);
	ut_assert_exact_key();

	UT_ASSERT_EQ(out.evidence, CLUSTER_VIS_EVIDENCE_REMOTE);
	UT_ASSERT_EQ(out.status, status);
	UT_ASSERT_EQ(out.commit_scn, memo_scn);
	UT_ASSERT(!out.commit_scn_is_bound);
	UT_ASSERT(memcmp(&out.ref, &ref, sizeof(ref)) == 0);
	ut_assert_no_fallback();
}

UT_TEST(test_peer_exact_memo_hit_propagates_committed)
{
	ut_run_memo_hit(CLUSTER_TT_STATUS_COMMITTED, UT_COMMIT_SCN);
}

UT_TEST(test_peer_exact_memo_hit_propagates_aborted)
{
	ut_run_memo_hit(CLUSTER_TT_STATUS_ABORTED, InvalidScn);
}

/*
 * The production memo accepts terminal values only.  This synthetic hit pins
 * the resolver's UNKNOWN propagation and, critically, proves that disabling
 * runtime visibility prevents UNKNOWN from re-entering the fresh-ref wire leg.
 */
UT_TEST(test_peer_exact_synthetic_unknown_hit_stays_failclosed_without_wire)
{
	ut_run_memo_hit(CLUSTER_TT_STATUS_UNKNOWN, InvalidScn);
}

/*
 * Formal 4x1x3 at 0c507acf first failed on native seed xid 798: a peer
 * acquired a still-bound (fresh) ITL ref, skipped the recycled-ref-only
 * native-prehistory gate, then missed TT authority that cannot exist for an
 * enabled=off seed transaction.  Verified prehistory is the local terminal
 * authority for every provably native xid regardless of ref freshness.
 */
UT_TEST(test_peer_fresh_native_ref_uses_covered_prehistory_before_overlay)
{
	ClusterUndoTTSlotRef ref = ut_exact_peer_ref();
	ClusterVisResolve out;

	ref.local_xid = UT_NATIVE_XID;
	ut_reset(CLUSTER_TT_STATUS_UNKNOWN, InvalidScn);
	ut_memo_hit = false;
	ut_native_prehistory_armed = true;
	cluster_crossnode_runtime_visibility = true;
	memset(&out, 0xA5, sizeof(out));

	cluster_visibility_resolve_from_ref_scn(UT_NATIVE_XID, &ref, UT_ANCHOR_LSN, UT_READ_SCN, &out);

	UT_ASSERT_EQ(out.evidence, CLUSTER_VIS_EVIDENCE_REMOTE);
	UT_ASSERT_EQ(out.status, CLUSTER_TT_STATUS_COMMITTED);
	UT_ASSERT_EQ(out.commit_scn, (SCN)1);
	UT_ASSERT(out.commit_scn_is_bound);
	UT_ASSERT_EQ(ut_calls.peer_stamp, 1);
	UT_ASSERT(ut_calls.prehistory_probe >= 2);
	UT_ASSERT_EQ(ut_calls.prehistory_lock, 1);
	UT_ASSERT_EQ(ut_calls.prehistory_unlock, 1);
	UT_ASSERT_EQ(ut_calls.prehistory_status, 1);
	UT_ASSERT_EQ(ut_calls.prehistory_note, 1);
	UT_ASSERT_EQ(ut_calls.truncation_lock, 1);
	UT_ASSERT_EQ(ut_calls.truncation_unlock, 1);
	UT_ASSERT_EQ(ut_calls.overlay, 0);
	UT_ASSERT_EQ(ut_calls.wire, 0);
}

static void
ut_run_freshref_terminal_memo(ClusterUndoVerdictKind kind,
	ClusterTTStatus expected_status, SCN verdict_scn)
{
	ClusterUndoTTSlotRef ref = ut_exact_peer_ref();
	ClusterVisResolve out;
	int first_pair_resolve;

	ref.has_cached_status = true;
	ref.cached_commit_scn = UT_COMMIT_SCN;
	ut_reset(CLUSTER_TT_STATUS_UNKNOWN, InvalidScn);
	ut_memo_hit = false;
	ut_pair_verdict.kind = kind;
	ut_pair_verdict.commit_scn = verdict_scn;
	cluster_crossnode_runtime_visibility = true;

	memset(&out, 0xA5, sizeof(out));
	cluster_visibility_resolve_from_ref_scn(
		UT_RAW_XID, &ref, UT_ANCHOR_LSN, UT_READ_SCN, &out);
	UT_ASSERT_EQ(out.evidence, CLUSTER_VIS_EVIDENCE_REMOTE);
	UT_ASSERT_EQ(out.status, expected_status);
	UT_ASSERT_EQ(out.commit_scn, verdict_scn);
	UT_ASSERT(!out.commit_scn_is_bound);
	UT_ASSERT_EQ(ut_calls.memo_install, 1);
	UT_ASSERT_EQ(ut_installed_status, expected_status);
	UT_ASSERT_EQ(ut_installed_scn, verdict_scn);
	ut_assert_exact_key();
	UT_ASSERT_EQ(memcmp(&ut_installed_key, &ut_seen_key,
						 sizeof(ut_installed_key)), 0);
	first_pair_resolve = ut_calls.pair_resolve;
	UT_ASSERT_EQ(first_pair_resolve, 1);

	memset(&out, 0xA5, sizeof(out));
	cluster_visibility_resolve_from_ref_scn(
		UT_RAW_XID, &ref, UT_ANCHOR_LSN, UT_READ_SCN, &out);
	UT_ASSERT_EQ(out.evidence, CLUSTER_VIS_EVIDENCE_REMOTE);
	UT_ASSERT_EQ(out.status, expected_status);
	UT_ASSERT_EQ(out.commit_scn, verdict_scn);
	UT_ASSERT_EQ(ut_calls.pair_resolve, first_pair_resolve);
	UT_ASSERT_EQ(ut_calls.memo_install, 1);
	UT_ASSERT_EQ(ut_calls.memo_probe, 2);
}

UT_TEST(test_freshref_committed_exact_installs_then_hits_terminal_memo)
{
	ut_run_freshref_terminal_memo(
		CLUSTER_UNDO_VERDICT_COMMITTED_EXACT,
		CLUSTER_TT_STATUS_COMMITTED, UT_COMMIT_SCN);
}

/*
 * A pair-eligible fresh ref must enter the frozen ordinary exact-slot -> C1b
 * resolver.  The older slotless overlay-miss pull can only return a
 * snapshot-relative bound and must not shadow that exact path.
 */
UT_TEST(test_pair_eligible_freshref_bypasses_slotless_bound)
{
	ClusterUndoTTSlotRef ref = ut_exact_peer_ref();
	ClusterVisResolve out;

	ref.has_cached_status = true;
	ref.cached_commit_scn = UT_COMMIT_SCN;
	ut_reset(CLUSTER_TT_STATUS_UNKNOWN, InvalidScn);
	ut_memo_hit = false;
	ut_slotless_bound_armed = true;
	ut_pair_verdict.kind = CLUSTER_UNDO_VERDICT_COMMITTED_EXACT;
	ut_pair_verdict.commit_scn = UT_COMMIT_SCN;
	cluster_crossnode_runtime_visibility = true;
	cluster_crossnode_write_write = true;
	memset(&out, 0xA5, sizeof(out));

	cluster_visibility_resolve_from_ref_scn(
		UT_RAW_XID, &ref, UT_ANCHOR_LSN, UT_READ_SCN, &out);
	UT_ASSERT_EQ(out.evidence, CLUSTER_VIS_EVIDENCE_REMOTE);
	UT_ASSERT_EQ(out.status, CLUSTER_TT_STATUS_COMMITTED);
	UT_ASSERT_EQ(out.commit_scn, UT_COMMIT_SCN);
	UT_ASSERT(!out.commit_scn_is_bound);
	UT_ASSERT_EQ(ut_calls.pair_resolve, 1);
	UT_ASSERT_EQ(ut_calls.memo_install, 1);
}

UT_TEST(test_freshref_aborted_installs_then_hits_terminal_memo)
{
	ut_run_freshref_terminal_memo(
		CLUSTER_UNDO_VERDICT_ABORTED,
		CLUSTER_TT_STATUS_ABORTED, InvalidScn);
}

UT_TEST(test_freshref_nonexact_verdicts_never_enter_terminal_memo)
{
	static const struct {
		ClusterUndoVerdictKind kind;
		SCN scn;
		ClusterTTStatus status;
		bool bound;
	} cases[] = {
		{ CLUSTER_UNDO_VERDICT_COMMITTED_BOUND, UT_COMMIT_SCN,
		  CLUSTER_TT_STATUS_COMMITTED, true },
		{ CLUSTER_UNDO_VERDICT_IN_PROGRESS, InvalidScn,
		  CLUSTER_TT_STATUS_IN_PROGRESS, false },
		{ CLUSTER_UNDO_VERDICT_UNKNOWN_FAIL_CLOSED, InvalidScn,
		  CLUSTER_TT_STATUS_UNKNOWN, false }
	};
	uint32 i;

	for (i = 0; i < lengthof(cases); i++) {
		ClusterUndoTTSlotRef ref = ut_exact_peer_ref();
		ClusterVisResolve out;

		ref.has_cached_status = true;
		ref.cached_commit_scn = UT_COMMIT_SCN;
		ut_reset(CLUSTER_TT_STATUS_UNKNOWN, InvalidScn);
		ut_memo_hit = false;
		ut_pair_verdict.kind = cases[i].kind;
		ut_pair_verdict.commit_scn = cases[i].scn;
		cluster_crossnode_runtime_visibility = true;
		memset(&out, 0xA5, sizeof(out));

		cluster_visibility_resolve_from_ref_scn(
			UT_RAW_XID, &ref, UT_ANCHOR_LSN, UT_READ_SCN, &out);
		UT_ASSERT_EQ(out.status, cases[i].status);
		UT_ASSERT_EQ(out.commit_scn_is_bound, cases[i].bound);
		UT_ASSERT_EQ(ut_calls.memo_install, 0);
		UT_ASSERT(!ut_memo_hit);
	}
}

UT_TEST(test_provisional_overlay_cannot_hide_origin_terminal)
{
	static const ClusterTTStatus provisional[]
		= { CLUSTER_TT_STATUS_IN_PROGRESS, CLUSTER_TT_STATUS_SUBCOMMITTED };
	uint32 i;

	for (i = 0; i < lengthof(provisional); i++) {
		ClusterUndoTTSlotRef ref = ut_exact_peer_ref();
		ClusterVisResolve out;

		ut_reset(CLUSTER_TT_STATUS_UNKNOWN, InvalidScn);
		ut_memo_hit = false;
		ut_overlay_hit = true;
		ut_overlay_status = provisional[i];
		ut_origin_verdict.kind = CLUSTER_UNDO_VERDICT_COMMITTED_EXACT;
		ut_origin_verdict.commit_scn = UT_COMMIT_SCN;
		cluster_crossnode_runtime_visibility = true;
		cluster_visibility_resolve_from_ref_scn(UT_RAW_XID, &ref, UT_ANCHOR_LSN, UT_READ_SCN, &out);
		UT_ASSERT_EQ(out.evidence, CLUSTER_VIS_EVIDENCE_REMOTE);
		UT_ASSERT_EQ(out.status, CLUSTER_TT_STATUS_COMMITTED);
		UT_ASSERT_EQ(out.commit_scn, UT_COMMIT_SCN);
		UT_ASSERT_EQ(ut_origin_asks, 1);
		UT_ASSERT_EQ(ut_calls.clog, 0);
		UT_ASSERT_EQ(ut_installed_status, CLUSTER_TT_STATUS_COMMITTED);
		UT_ASSERT_EQ(ut_evidence_metrics[CLUSTER_VIS_METRIC_OVERLAY_LIVE], 1);
		UT_ASSERT_EQ(ut_evidence_metrics[CLUSTER_VIS_METRIC_ORIGIN_ASK], 1);
		UT_ASSERT_EQ(ut_evidence_metrics[CLUSTER_VIS_METRIC_ORIGIN_TERMINAL], 1);
	}
}

UT_TEST(test_hint_miss_uses_existing_origin_and_accepts_only_proven_live)
{
	static const ClusterUndoVerdictKind outcomes[]
		= { CLUSTER_UNDO_VERDICT_COMMITTED_EXACT, CLUSTER_UNDO_VERDICT_ABORTED,
			CLUSTER_UNDO_VERDICT_IN_PROGRESS, CLUSTER_UNDO_VERDICT_UNKNOWN_FAIL_CLOSED };
	static const ClusterTTStatus statuses[]
		= { CLUSTER_TT_STATUS_COMMITTED, CLUSTER_TT_STATUS_ABORTED, CLUSTER_TT_STATUS_IN_PROGRESS,
			CLUSTER_TT_STATUS_UNKNOWN };
	uint32 i;

	for (i = 0; i < lengthof(outcomes); i++) {
		ClusterUndoTTSlotRef ref = ut_exact_peer_ref();
		ClusterVisResolve out;

		ut_reset(CLUSTER_TT_STATUS_UNKNOWN, InvalidScn);
		ut_memo_hit = false;
		ut_origin_verdict.kind = outcomes[i];
		ut_origin_verdict.commit_scn = i == 0 ? UT_COMMIT_SCN : InvalidScn;
		cluster_crossnode_runtime_visibility = true;
		cluster_visibility_resolve_from_ref_scn(UT_RAW_XID, &ref, UT_ANCHOR_LSN, UT_READ_SCN, &out);
		UT_ASSERT_EQ(out.status, statuses[i]);
		UT_ASSERT_EQ(ut_origin_asks, 1);
		UT_ASSERT_EQ(ut_calls.clog, 0);
		UT_ASSERT_EQ(ut_calls.memo_install, i < 2 ? 1 : 0);
	}
}

UT_TEST(test_stale_live_hint_without_origin_proof_stays_unknown)
{
	ClusterUndoTTSlotRef ref = ut_exact_peer_ref();
	ClusterVisResolve out;

	ut_reset(CLUSTER_TT_STATUS_UNKNOWN, InvalidScn);
	ut_memo_hit = false;
	ut_overlay_hit = true;
	ut_overlay_status = CLUSTER_TT_STATUS_IN_PROGRESS;
	cluster_crossnode_runtime_visibility = true;
	cluster_visibility_resolve_from_ref_scn(UT_RAW_XID, &ref, UT_ANCHOR_LSN, UT_READ_SCN, &out);
	UT_ASSERT_EQ(out.status, CLUSTER_TT_STATUS_UNKNOWN);
	UT_ASSERT_EQ(ut_origin_asks, 1);
	UT_ASSERT_EQ(ut_calls.memo_install, 0);
	UT_ASSERT_EQ(ut_calls.clog, 0);
}

UT_TEST(test_recycled_data_ref_uses_derived_origin_not_current_slot_owner)
{
	uint32 leg;

	/* Exact commit, retention bound, abort, and no durable proof.  These
	 * drive the same product resolver used by all three visibility exits. */
	for (leg = 0; leg < 4; leg++) {
		ClusterUndoTTSlotRef ref = ut_exact_peer_ref();
		ClusterVisResolve out;

		ut_reset(CLUSTER_TT_STATUS_UNKNOWN, InvalidScn);
		ref.local_xid++;
		ref.origin_node_id = UT_PEER_NODE + 1;
		ut_memo_hit = false;
		ut_recycled_proven = leg < 3;
		ut_recycled_committed = leg < 2;
		ut_recycled_bound = leg == 1;
		cluster_crossnode_runtime_visibility = true;
		cluster_visibility_resolve_from_ref_scn(UT_RAW_XID, &ref, UT_ANCHOR_LSN, UT_READ_SCN, &out);
		UT_ASSERT_EQ(ut_recycled_asks, 1);
		UT_ASSERT_EQ(ut_calls.clog, 0);
		UT_ASSERT_EQ(ut_calls.memo_install, 0);
		UT_ASSERT_EQ(out.evidence, leg < 3 ? CLUSTER_VIS_EVIDENCE_REMOTE
										   : CLUSTER_VIS_EVIDENCE_STALE_OR_AMBIGUOUS);
		UT_ASSERT_EQ(out.status, leg < 2	? CLUSTER_TT_STATUS_COMMITTED
								 : leg == 2 ? CLUSTER_TT_STATUS_ABORTED
											: CLUSTER_TT_STATUS_UNKNOWN);
		UT_ASSERT_EQ(out.commit_scn_is_bound, leg == 1);
		UT_ASSERT_EQ(ut_evidence_metrics[CLUSTER_VIS_METRIC_RECYCLED_TERMINAL], leg < 3 ? 1 : 0);
		UT_ASSERT_EQ(ut_evidence_metrics[CLUSTER_VIS_METRIC_RECYCLED_UNPROVABLE], leg == 3 ? 1 : 0);
	}
}

static void
ut_hint_reset(void)
{
	static ClusterTTStatusHintState counters;
	Size bytes = offsetof(ClusterTTStatusHintOutboundRing, slots)
				 + 2 * sizeof(ClusterTTStatusHintOutboundEntry);

	ut_reset(CLUSTER_TT_STATUS_UNKNOWN, InvalidScn);
	memset(&counters, 0, sizeof(counters));
	ClusterTTHintCounters = &counters;
	ClusterTTHintOutbound = calloc(1, bytes);
	ClusterTTHintOutbound->capacity = 2;
	ClusterMultiXactHintOutbound = NULL;
	ut_hint_installs = ut_hint_wakes = ut_hint_sends = 0;
	ut_hint_install_succeeds = false;
}

UT_TEST(test_real_hint_full_and_fanout_keep_loss_observable_without_retry)
{
	ClusterTTStatusKey key;

	ut_hint_reset();
	memset(&key, 0, sizeof(key));
	key.origin_node_id = UT_SELF_NODE;
	key.cluster_epoch = UT_CLUSTER_EPOCH;
	key.local_xid = UT_RAW_XID;
	cluster_tt_status_hint_emit_raw(&key, CLUSTER_TT_STATUS_COMMITTED, UT_COMMIT_SCN);
	cluster_tt_status_hint_emit_raw(&key, CLUSTER_TT_STATUS_COMMITTED, UT_COMMIT_SCN);
	UT_ASSERT_EQ(cluster_tt_status_hint_get_emit_count(), 1);
	UT_ASSERT_EQ(cluster_tt_status_hint_get_drop_invalid_count(), 1);
	UT_ASSERT_EQ(ut_evidence_metrics[CLUSTER_VIS_METRIC_HINT_FULL], 1);
	cluster_tt_status_hint_drain_outbound_raw();
	UT_ASSERT_EQ(ut_hint_sends, 1);
	UT_ASSERT_EQ(ut_evidence_metrics[CLUSTER_VIS_METRIC_HINT_SEND_FAILED], 2);
	cluster_tt_status_hint_drain_outbound_raw();
	UT_ASSERT_EQ(ut_hint_sends, 1);
	free(ClusterTTHintOutbound);
	ClusterTTHintOutbound = NULL;
}

UT_TEST(test_real_hint_receiver_wakes_only_after_successful_exact_apply)
{
	ClusterTTStatusHintMsgV2 msg;
	ClusterICEnvelope env;

	ut_hint_reset();
	memset(&msg, 0, sizeof(msg));
	memset(&env, 0, sizeof(env));
	msg.msg_version = CLUSTER_TT_STATUS_HINT_V2;
	msg.status = CLUSTER_TT_STATUS_COMMITTED;
	msg.key.origin_node_id = UT_PEER_NODE;
	msg.key.cluster_epoch = UT_CLUSTER_EPOCH;
	msg.key.local_xid = UT_RAW_XID;
	msg.commit_scn = UT_COMMIT_SCN;
	env.source_node_id = UT_PEER_NODE;
	env.payload_length = sizeof(msg);
	cluster_tt_status_hint_handle_envelope_raw(&env, &msg);
	UT_ASSERT_EQ(ut_hint_installs, 1);
	UT_ASSERT_EQ(ut_hint_wakes, 0);
	UT_ASSERT_EQ(cluster_tt_status_hint_get_install_count(), 0);
	ut_hint_install_succeeds = true;
	cluster_tt_status_hint_handle_envelope_raw(&env, &msg);
	UT_ASSERT_EQ(ut_hint_wakes, 1);
	UT_ASSERT_EQ(cluster_tt_status_hint_get_install_count(), 1);
	msg.key.cluster_epoch++;
	cluster_tt_status_hint_handle_envelope_raw(&env, &msg);
	UT_ASSERT_EQ(ut_hint_wakes, 1);
	UT_ASSERT_EQ(ut_evidence_metrics[CLUSTER_VIS_METRIC_HINT_STALE], 1);
	msg.key.cluster_epoch--;
	msg.key._reserved = 1;
	cluster_tt_status_hint_handle_envelope_raw(&env, &msg);
	UT_ASSERT_EQ(ut_hint_wakes, 1);
	UT_ASSERT_EQ(ut_evidence_metrics[CLUSTER_VIS_METRIC_HINT_INVALID], 1);
	free(ClusterTTHintOutbound);
	ClusterTTHintOutbound = NULL;
}

/* Real FULL consumer and real legacy resolver, with only the exact origin
 * service substituted. A complete DATA locator must reach that service;
 * a reduced-key miss must not prevent it, for either origin or tuple side. */
static void
ut_full_scratch_case_with_role(bool deleting, bool local_origin, int scenario, int replacement_role)
{
	HeapTupleData tuple = { 0 };
	SnapshotData snapshot = { 0 };
	ClusterR4HotScratchTestContext context = { 0 };
	HeapTupleHeader header;
	ClusterItlSlotData *slot;
	PageHeader page_header;
	PGAlignedBlock before;
	volatile bool caught = false;
	volatile bool visible = false;

	ut_reset(CLUSTER_TT_STATUS_UNKNOWN, InvalidScn);
	ut_exit_fixture = true;
	ut_full_scratch_fixture = true;
	ut_full_scratch_scenario = scenario;
	ut_exit_exact_proof = scenario != 4;
	ut_exit_ref = ut_exact_peer_ref();
	ut_exit_ref.origin_node_id = local_origin ? UT_SELF_NODE : UT_PEER_NODE;
	ut_exit_ref.cluster_epoch = 0;
	ut_memo_hit = false;
	cluster_crossnode_runtime_visibility = true;
	memset(ut_visibility_page.data, 0, BLCKSZ);
	page_header = (PageHeader)ut_visibility_page.data;
	page_header->pd_flags = PD_HAS_ITL;
	page_header->pd_lower = SizeOfPageHeaderData;
	page_header->pd_upper = 1024;
	page_header->pd_special = BLCKSZ - CLUSTER_ITL_SPECIAL_SIZE;
	page_header->pd_pagesize_version = BLCKSZ | PG_PAGE_LAYOUT_VERSION;
	PageSetLSN(ut_visibility_page.data, UT_ANCHOR_LSN);
	slot = ClusterPageGetItlSlots(ut_visibility_page.data);
	slot->flags = ITL_FLAG_ACTIVE;
	slot->xid = UT_RAW_XID;
	slot->wrap = 7;
	slot->undo_segment_head.raw[0] = UINT64_C(0x1234010203040506);
	slot->undo_segment_head.raw[1] = UINT64_C(0x0708090a0b0c0d0e);
	if (scenario >= 12) {
		slot->flags = ITL_FLAG_COMMITTED;
		slot->commit_scn = UT_COMMIT_SCN;
		ut_exit_ref.has_cached_status = true;
		ut_exit_ref.cached_commit_scn = UT_COMMIT_SCN;
		ut_exit_ref.cluster_epoch = UT_CLUSTER_EPOCH;
		ut_pair_verdict.kind = CLUSTER_UNDO_VERDICT_COMMITTED_EXACT;
		ut_pair_verdict.commit_scn = UT_COMMIT_SCN;
		if (scenario == 13 || scenario == 21 || scenario == 24)
			ut_pair_verdict.kind = CLUSTER_UNDO_VERDICT_UNKNOWN_FAIL_CLOSED;
		if (scenario == 14)
			ut_pair_verdict.commit_scn++;
		if (scenario == 15)
			ut_pair_verdict.kind = CLUSTER_UNDO_VERDICT_ABORTED;
		if (scenario == 16)
			ut_pair_verdict.kind = CLUSTER_UNDO_VERDICT_COMMITTED_BOUND;
		if (scenario == 17)
			ut_exit_ref.has_cached_status = false;
		if (scenario == 18)
			ut_exit_ref.cached_commit_scn = InvalidScn;
		if (scenario == 22) {
			slot->commit_scn = UT_READ_SCN + 1;
			ut_exit_ref.cached_commit_scn = UT_READ_SCN + 1;
			ut_pair_verdict.commit_scn = UT_READ_SCN + 1;
		}
		if (scenario == 23)
			ut_exit_ref.cluster_epoch = ut_current_epoch = 0;
		if (scenario == 24)
			slot->flags = ITL_FLAG_ACTIVE; /* precommit stamp is not terminal */
		if (scenario == 25 || scenario == 26) {
			ut_pair_verdict.kind = CLUSTER_UNDO_VERDICT_COMMITTED_BOUND;
			ut_pair_verdict.commit_scn = scenario == 25 ? UT_READ_SCN + 1 : UT_COMMIT_SCN;
		}
	}
	if (scenario == 9)
		slot->flags = ITL_FLAG_LOCK_ONLY_ACTIVE;
	if (scenario == 10)
		ut_exit_ref.local_xid++;
	if (scenario == 11)
		ut_exit_ref.tt_slot_id = 0;
	if (scenario >= 30) {
		ut_history_origin = local_origin ? UT_SELF_NODE : UT_PEER_NODE;
		ut_exit_ref.origin_node_id = local_origin ? UT_PEER_NODE : UT_SELF_NODE;
		ut_exit_ref.local_xid = UT_RAW_XID + 4;
		slot->xid = ut_exit_ref.local_xid;
		ut_origin_verdict.kind = CLUSTER_UNDO_VERDICT_COMMITTED_EXACT;
		ut_origin_verdict.commit_scn = UT_COMMIT_SCN;
		if (scenario == 31)
			ut_origin_verdict.commit_scn = UT_READ_SCN + 1;
		if (scenario == 32)
			ut_origin_verdict.kind = CLUSTER_UNDO_VERDICT_ABORTED;
		if (scenario == 33 || scenario == 34 || scenario == 47)
			ut_origin_verdict.kind = CLUSTER_UNDO_VERDICT_COMMITTED_BOUND;
		if (scenario == 34)
			ut_origin_verdict.commit_scn = UT_READ_SCN + 1;
		if (scenario == 35)
			ut_origin_verdict.kind = CLUSTER_UNDO_VERDICT_UNKNOWN_FAIL_CLOSED;
		if (scenario == 36)
			ut_origin_verdict.kind = CLUSTER_UNDO_VERDICT_IN_PROGRESS;
		if (scenario == 37 || scenario == 47)
			ut_origin_verdict.commit_scn = InvalidScn;
		if (scenario == 41)
			ut_exit_ref.cluster_epoch--;
		if (scenario == 43)
			ut_exit_ref.tt_slot_id = 0;
		if (scenario == 44)
			slot->flags = ITL_FLAG_LOCK_ONLY_COMMITTED;
		if (scenario == 45)
			slot->xid++;
		if (scenario == 46)
			ut_exit_ref.cluster_epoch = ut_current_epoch = 0;
		if (scenario == 48)
			ut_current_epoch = UINT64_C(1) << 32;
		if (replacement_role >= 0)
			slot->flags = (uint8)replacement_role;
	}
	header = (HeapTupleHeader)(ut_visibility_page.data + 1024);
	header->t_hoff = SizeofHeapTupleHeader;
	header->t_itl_slot_idx = 0;
	header->t_infomask = deleting ? HEAP_XMIN_FROZEN : HEAP_XMAX_INVALID;
	HeapTupleHeaderSetXmin(header, UT_RAW_XID);
	HeapTupleHeaderSetXmax(header, deleting ? UT_RAW_XID : InvalidTransactionId);
	ItemPointerSet(&tuple.t_self, 0, 1);
	header->t_ctid = tuple.t_self;
	tuple.t_data = header;
	tuple.t_len = SizeofHeapTupleHeader;
	tuple.t_tableOid = FirstNormalObjectId;
	snapshot.snapshot_type = SNAPSHOT_MVCC;
	snapshot.cluster_source = SNAPSHOT_SOURCE_CLUSTER;
	snapshot.read_epoch = UT_CLUSTER_EPOCH;
	snapshot.read_scn = UT_READ_SCN;
	context.scratch_page = ut_visibility_page.data;
	context.already_full = true;
	context.read_scn = snapshot.read_scn;
	context.logical_root = tuple.t_self;
	context.tag.blockNum = 0;
	memcpy(before.data, ut_visibility_page.data, BLCKSZ);
	ut_error_armed = true;
	PG_TRY();
	{
		visible = HeapTupleSatisfiesMVCCScratch(&tuple, &snapshot, &context);
	}
	PG_CATCH();
	{
		caught = true;
	}
	PG_END_TRY();
	ut_error_armed = false;
	if (scenario >= 30) {
		bool invalid_role
			= replacement_role == ITL_FLAG_FREE || replacement_role > ITL_FLAG_LOCK_ONLY_ABORTED;
		bool expected_error = invalid_role || (scenario >= 34 && scenario != 44 && scenario != 46);
		bool expected_call = !invalid_role
							 && (scenario < 39 || scenario == 40 || scenario == 44 || scenario == 46
								 || scenario == 47);

		UT_ASSERT_EQ(caught, expected_error);
		if (!caught)
			UT_ASSERT_EQ(visible, scenario == 31 || scenario == 32 ? deleting : !deleting);
		UT_ASSERT_EQ(ut_origin_asks, expected_call ? 1 : 0);
		UT_ASSERT_EQ(ut_calls.exact_resolve, 0);
		UT_ASSERT_EQ(ut_calls.pair_resolve, 0);
	} else if (scenario >= 12) {
		bool expected_error = (scenario >= 13 && scenario <= 20) || scenario == 25;
		bool expected_exact = scenario == 13 || scenario == 17 || scenario == 18 || scenario == 21
							  || scenario == 24;

		UT_ASSERT_EQ(caught, expected_error);
		if (!caught)
			UT_ASSERT_EQ(visible,
						 scenario == 12 || scenario == 23 || scenario == 26 ? !deleting : deleting);
		UT_ASSERT_EQ(ut_calls.exact_resolve, expected_exact ? 1 : 0);
		UT_ASSERT_EQ(ut_calls.pair_resolve, scenario == 17 || scenario == 18 ? 0 : 1);
	} else {
		UT_ASSERT_EQ(caught, scenario >= 4);
		if (!caught)
			UT_ASSERT_EQ(visible, scenario == 0 ? !deleting : deleting);
		UT_ASSERT_EQ(ut_calls.exact_resolve, scenario < 8 ? 1 : 0);
	}
	UT_ASSERT(!cluster_vis_resolve_in_flight());
	UT_ASSERT(PG_exception_stack == NULL);
	UT_ASSERT_EQ(ut_calls.clog, 0);
	UT_ASSERT_EQ(ut_native_calls, 0);
	UT_ASSERT_EQ(ut_hint_mutations, 0);
	UT_ASSERT_EQ(memcmp(before.data, ut_visibility_page.data, BLCKSZ), 0);
}

static void
ut_full_scratch_exact_case(bool deleting, bool local_origin, int scenario)
{
	ut_full_scratch_case_with_role(deleting, local_origin, scenario, -1);
}

UT_TEST(test_full_scratch_remote_xmax_retains_exact_data_locator)
{
	ut_full_scratch_exact_case(true, false, 0);
}
UT_TEST(test_full_scratch_remote_xmin_retains_exact_data_locator)
{
	ut_full_scratch_exact_case(false, false, 0);
}
UT_TEST(test_full_scratch_local_xmax_uses_exact_origin_not_native_clog)
{
	ut_full_scratch_exact_case(true, true, 0);
}
UT_TEST(test_full_scratch_local_xmin_uses_exact_origin_not_native_clog)
{
	ut_full_scratch_exact_case(false, true, 0);
}

UT_TEST(test_full_scratch_exact_status_and_scn_polarity_both_origins)
{
	int scenario;
	int origin;
	int deleting;

	for (scenario = 1; scenario <= 3; scenario++)
		for (origin = 0; origin < 2; origin++)
			for (deleting = 0; deleting < 2; deleting++)
				ut_full_scratch_exact_case(deleting, origin, scenario);
}

UT_TEST(test_full_scratch_unknown_prepared_bad_scn_and_locator_stay_protective)
{
	int scenario;
	int origin;
	int deleting;

	for (scenario = 4; scenario <= 11; scenario++) {
		if (scenario == 7)
			continue; /* the owned-error cleanup leg is tested separately */
		for (origin = 0; origin < 2; origin++)
			for (deleting = 0; deleting < 2; deleting++)
				ut_full_scratch_exact_case(deleting, origin, scenario);
	}
}

UT_TEST(test_full_scratch_local_exact_error_releases_resolution_scope)
{
	ut_full_scratch_exact_case(false, true, 7);
	ut_full_scratch_exact_case(true, true, 0);
}

UT_TEST(test_full_scratch_local_reused_tt_retained_xmin)
{
	ut_full_scratch_exact_case(false, true, 12);
}

UT_TEST(test_full_scratch_local_reused_tt_retained_xmax)
{
	ut_full_scratch_exact_case(true, true, 12);
}

UT_TEST(test_full_scratch_retained_scn_both_origins_and_tuple_sides)
{
	for (int origin = 0; origin < 2; origin++)
		for (int deleting = 0; deleting < 2; deleting++) {
			ut_full_scratch_exact_case(deleting, origin, 12);
			ut_full_scratch_exact_case(deleting, origin, 22);
			ut_full_scratch_exact_case(deleting, origin, 23);
		}
}

UT_TEST(test_full_scratch_retained_unknown_malformed_missing_and_epoch_change)
{
	for (int scenario = 13; scenario <= 19; scenario++)
		for (int deleting = 0; deleting < 2; deleting++)
			ut_full_scratch_exact_case(deleting, true, scenario);
}

UT_TEST(test_full_scratch_retained_pair_error_unwinds_then_exact_live_works)
{
	ut_full_scratch_exact_case(false, true, 20);
	for (int deleting = 0; deleting < 2; deleting++) {
		ut_full_scratch_exact_case(deleting, true, 21);
		ut_full_scratch_exact_case(deleting, true, 24);
	}
}

UT_TEST(test_full_scratch_recycled_creator_uses_original_origin_terminal_proof)
{
	ut_full_scratch_exact_case(false, false, 30);
	ut_full_scratch_exact_case(false, true, 30);
}

UT_TEST(test_full_scratch_recycled_deleter_uses_original_origin_terminal_proof)
{
	ut_full_scratch_exact_case(true, false, 30);
	ut_full_scratch_exact_case(true, true, 30);
}

UT_TEST(test_full_scratch_history_terminal_polarity_and_bound_both_origins)
{
	const int scenarios[] = { 31, 32, 33, 44, 46 };

	for (int deleting = 0; deleting <= 1; deleting++)
		for (int origin = 0; origin <= 1; origin++)
			for (int i = 0; i < lengthof(scenarios); i++)
				ut_full_scratch_exact_case(deleting, origin, scenarios[i]);
}

UT_TEST(test_full_scratch_history_cannot_rescue_unknown_malformed_or_epoch_drift)
{
	const int scenarios[] = { 34, 35, 36, 37, 38, 39, 41, 42, 43, 45, 47, 48 };

	for (int deleting = 0; deleting <= 1; deleting++)
		for (int origin = 0; origin <= 1; origin++)
			for (int i = 0; i < lengthof(scenarios); i++)
				ut_full_scratch_exact_case(deleting, origin, scenarios[i]);
}

UT_TEST(test_full_scratch_history_error_releases_scope_before_next_exact_proof)
{
	for (int origin = 0; origin <= 1; origin++) {
		ut_full_scratch_exact_case(false, origin, 40);
		ut_full_scratch_exact_case(true, origin, 0);
	}
}

UT_TEST(test_full_scratch_consumer_does_not_turn_an_upper_bound_into_after_read_commit)
{
	for (int deleting = 0; deleting <= 1; deleting++) {
		ut_full_scratch_exact_case(deleting, false, 25);
		ut_full_scratch_exact_case(deleting, false, 26);
	}
}

UT_TEST(test_full_scratch_recycled_lock_roles_use_only_original_terminal_proof)
{
	const int scenarios[]
		= { 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 45, 46, 47, 48 };

	for (int role = ITL_FLAG_LOCK_ONLY_ACTIVE; role <= ITL_FLAG_LOCK_ONLY_ABORTED; role++)
		for (int deleting = 0; deleting <= 1; deleting++)
			for (int origin = 0; origin <= 1; origin++)
				for (int i = 0; i < lengthof(scenarios); i++)
					ut_full_scratch_case_with_role(deleting, origin, scenarios[i], role);
}

UT_TEST(test_full_scratch_history_never_uses_free_or_multixact_role)
{
	const int roles[] = { ITL_FLAG_FREE, ITL_FLAG_LOCK_ONLY_XMAX_IS_MULTI, 255 };

	for (int deleting = 0; deleting <= 1; deleting++)
		for (int origin = 0; origin <= 1; origin++)
			for (int i = 0; i < lengthof(roles); i++)
				ut_full_scratch_case_with_role(deleting, origin, 30, roles[i]);
}

int
main(void)
{
	UT_PLAN(38);
	UT_RUN(test_full_scratch_recycled_lock_roles_use_only_original_terminal_proof);
	UT_RUN(test_full_scratch_history_never_uses_free_or_multixact_role);
	UT_RUN(test_full_scratch_consumer_does_not_turn_an_upper_bound_into_after_read_commit);
	UT_RUN(test_full_scratch_history_terminal_polarity_and_bound_both_origins);
	UT_RUN(test_full_scratch_history_cannot_rescue_unknown_malformed_or_epoch_drift);
	UT_RUN(test_full_scratch_history_error_releases_scope_before_next_exact_proof);
	UT_RUN(test_full_scratch_recycled_creator_uses_original_origin_terminal_proof);
	UT_RUN(test_full_scratch_recycled_deleter_uses_original_origin_terminal_proof);
	UT_RUN(test_peer_exact_memo_hit_propagates_committed);
	UT_RUN(test_peer_exact_memo_hit_propagates_aborted);
	UT_RUN(test_peer_exact_synthetic_unknown_hit_stays_failclosed_without_wire);
	UT_RUN(test_peer_fresh_native_ref_uses_covered_prehistory_before_overlay);
	UT_RUN(test_freshref_committed_exact_installs_then_hits_terminal_memo);
	UT_RUN(test_pair_eligible_freshref_bypasses_slotless_bound);
	UT_RUN(test_freshref_aborted_installs_then_hits_terminal_memo);
	UT_RUN(test_freshref_nonexact_verdicts_never_enter_terminal_memo);
	UT_RUN(test_provisional_overlay_cannot_hide_origin_terminal);
	UT_RUN(test_hint_miss_uses_existing_origin_and_accepts_only_proven_live);
	UT_RUN(test_stale_live_hint_without_origin_proof_stays_unknown);
	UT_RUN(test_recycled_data_ref_uses_derived_origin_not_current_slot_owner);
	UT_RUN(test_real_hint_full_and_fanout_keep_loss_observable_without_retry);
	UT_RUN(test_real_hint_receiver_wakes_only_after_successful_exact_apply);
	UT_RUN(test_three_real_visibility_exits_route_recycle_and_unproven);
	UT_RUN(test_complete_frozen_live_proof_precedes_legacy_cr);
	UT_RUN(test_complete_frozen_live_proof_keeps_full_epoch_check);
	UT_RUN(test_incomplete_live_proof_keeps_cr_and_authority_refusal);
	UT_RUN(test_full_scratch_remote_xmax_retains_exact_data_locator);
	UT_RUN(test_full_scratch_remote_xmin_retains_exact_data_locator);
	UT_RUN(test_full_scratch_local_xmax_uses_exact_origin_not_native_clog);
	UT_RUN(test_full_scratch_local_xmin_uses_exact_origin_not_native_clog);
	UT_RUN(test_full_scratch_exact_status_and_scn_polarity_both_origins);
	UT_RUN(test_full_scratch_unknown_prepared_bad_scn_and_locator_stay_protective);
	UT_RUN(test_full_scratch_local_exact_error_releases_resolution_scope);
	UT_RUN(test_full_scratch_local_reused_tt_retained_xmin);
	UT_RUN(test_full_scratch_local_reused_tt_retained_xmax);
	UT_RUN(test_full_scratch_retained_scn_both_origins_and_tuple_sides);
	UT_RUN(test_full_scratch_retained_unknown_malformed_missing_and_epoch_change);
	UT_RUN(test_full_scratch_retained_pair_error_unwinds_then_exact_live_works);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
