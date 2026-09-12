/*-------------------------------------------------------------------------
 *
 * test_cluster_r4_tx_outcome.c
 *    Stage 8 R4 closed outcome/proof compatibility table.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * IDENTIFICATION
 *    src/test/cluster_unit/test_cluster_r4_tx_outcome.c
 *
 * NOTES
 *    This is a pgrac-original file.
 *
 *-------------------------------------------------------------------------
 */
#define USE_PGRAC_CLUSTER 1

#include "postgres.h"

#include "access/clog.h"
#include "access/subtrans.h"
#include "access/twophase.h"
#include "access/xlog.h"
#include "cluster/cluster_epoch.h"
#include "cluster/cluster_runtime_visibility.h"
#include "cluster/cluster_tt_durable.h"
#include "cluster/cluster_tt_slot.h"
#include "cluster/cluster_tx_resolve.h"
#include "cluster/cluster_uba.h"
#include "cluster/cluster_undo_record.h"
#include "cluster/cluster_undo_record_api.h"
#include "cluster/cluster_semantic_activation.h"
#include "cluster/cluster_terminal_ref_census.h"
#include "cluster/cluster_undo_segment.h"
#include "cluster/cluster_tt_local.h"
#include "cluster/cluster_tt_status.h"
#include "cluster/cluster_xid_stripe.h"
#include "cluster/storage/cluster_undo_block0_current.h"
#include "cluster/storage/cluster_undo_buf.h"
#include "cluster/cluster_cr.h"
#include "storage/ipc.h"
#include "storage/lwlock.h"
#include "storage/procarray.h"
#include "utils/snapmgr.h"

#undef printf

#include "unit_test.h"

UT_DEFINE_GLOBALS();

#define TEST_ORIGIN_XID ((TransactionId)797)
#define TEST_ORIGIN_WRAP ((uint16)7)
#define TEST_RECORD_SEGMENT ((uint32)1)
#define TEST_TT_SEGMENT ((uint32)2)
#define TEST_TT_OFFSET ((uint16)3)

int cluster_node_id = 0;
bool cluster_crossnode_runtime_visibility = true;
TransactionId TransactionXmin = FirstNormalTransactionId;
volatile sig_atomic_t InterruptPending = false;
volatile uint32 InterruptHoldoffCount = 0;
sigjmp_buf *PG_exception_stack = NULL;
ErrorContextCallback *error_context_stack = NULL;

static LWLockPadded test_lwlocks[64];
LWLockPadded *MainLWLockArray = test_lwlocks;
static VariableCacheData test_variable_cache;
VariableCache ShmemVariableCache = &test_variable_cache;

static ClusterTxLocator test_origin_locator;
static UndoRecordHeader test_origin_record;
static int test_undo_read_calls;
static int test_tt_exact_calls;
static int test_tt_snapshot_calls;
static int test_native_status_calls;
static int test_by_xid_scan_calls;
static bool test_durable_locator_available;
static uint32 test_tt_segment_seen;
static uint16 test_tt_slot_seen;
static TransactionId test_tt_xid_seen;
static uint16 test_tt_wrap_seen;
static XidStatus test_native_status;
static TransactionId test_native_script_xids[8];
static XidStatus test_native_script_statuses[8];
static int test_native_script_count;
static int test_native_script_pos;
static TransactionId test_subtrans_chain[CLUSTER_R4_SUBTRANS_MAX_DEPTH + 1];
static int test_subtrans_chain_count;
static int test_subtrans_parent_calls;
static bool test_subtrans_mutate_recheck;
static bool test_subtrans_cycle;
static bool test_twophase_prepared;
static TransactionId test_twophase_xid;
static int test_twophase_calls;
static TTSlot test_tt_slot;
static SCN test_commit_scn;
static uint64 test_formation_epoch;
static XLogRecPtr test_flush_lsn;
static uint64 test_tt_generation;
static SCN test_authority_scn;
static int test_candidate_acquire_calls;
static int test_candidate_sample_calls;
static int test_candidate_block0_copy_calls;
static int test_candidate_data_copy_calls;
static int test_candidate_release_calls;
static int test_candidate_cancel_calls;
static int test_candidate_poll_calls;
static int test_candidate_wait_calls;
static int test_candidate_wakes_before_terminal;
static uint64 test_vis_evidence[CLUSTER_VIS_METRIC_COUNT];
static int test_candidate_exit_hooks_ensure_calls;
static uint32 test_candidate_acquire_segments[8];
static int test_candidate_held_count;
static int test_candidate_max_held_count;
static int test_candidate_extract_calls;
static bool test_candidate_mutate_record_on_recheck;
static ClusterUndoBlock0Result test_candidate_sample_result;
static uint32 test_candidate_data_generation;
static uint32 test_candidate_tt_generation;
static bool test_candidate_copy_physical_slot;
static bool test_current_owner_available;
static bool test_current_owner_drift_on_recheck;
static int test_current_owner_calls;
static ClusterTTSlotCurrentOwner test_current_owner;
static bool test_local_binding_available;
static bool test_local_binding_drift_on_recheck;
static int test_local_binding_calls;
static ClusterCanonicalTxnBinding test_local_binding;
static int test_ctrc_touch_calls;
static uint32 test_ctrc_touch_grant;
static uint32 test_current_segment;
static bool test_current_segment_drift_on_recheck;
static int test_current_segment_calls;
static bool test_no_raw_reuse_window;
static bool test_no_raw_reuse_drift_on_recheck;
static bool test_xid_is_mine;
static bool test_native_origin_provable;
static bool test_native_throw;
static bool test_procarray_live;
static int test_native_fence_depth;
static int test_native_fence_lock_calls;
static int test_native_fence_unlock_calls;
static int test_procarray_calls;
static int test_xact_lock_depth;
static int test_regular_admission_recheck_calls;
static int test_terminal_census_recheck_calls;
static int test_regular_root_resolve_calls;
static int test_terminal_census_root_resolve_calls;
static bool test_local_freshref_pair_exact;
static int test_local_freshref_pair_calls;
static ClusterUndoBlock0CurrentStep test_candidate_acquire_step
	= CLUSTER_UNDO_BLOCK0_CURRENT_HELD;
static ClusterUndoBlock0CurrentStep test_candidate_poll_step
	= CLUSTER_UNDO_BLOCK0_CURRENT_FAILED;
static ClusterUndoBlock0CurrentStep test_candidate_after_wait_step
	= CLUSTER_UNDO_BLOCK0_CURRENT_FAILED;

void
cluster_vis_evidence_note(ClusterVisEvidenceMetric metric)
{
	UT_ASSERT(metric >= 0 && metric < CLUSTER_VIS_METRIC_COUNT);
	test_vis_evidence[metric]++;
}

void
ExceptionalCondition(const char *conditionName pg_attribute_unused(),
					 const char *fileName pg_attribute_unused(), int lineNumber pg_attribute_unused())
{
	abort();
}

void
ProcessInterrupts(void)
{}

void
pg_re_throw(void)
{
	if (PG_exception_stack != NULL)
		siglongjmp(*PG_exception_stack, 1);
	abort();
}

void
before_shmem_exit(pg_on_exit_callback function pg_attribute_unused(),
				  Datum arg pg_attribute_unused())
{}

void
cancel_before_shmem_exit(pg_on_exit_callback function pg_attribute_unused(),
						 Datum arg pg_attribute_unused())
{}

void
cluster_undo_block0_current_ensure_exit_hooks(void)
{
	test_candidate_exit_hooks_ensure_calls++;
}

void
cluster_rtvis_resolve_note_committed(void)
{}

void
cluster_rtvis_resolve_note_failclosed(void)
{}

static bool
test_uba_equal(UBA left, UBA right)
{
	return left.raw[0] == right.raw[0] && left.raw[1] == right.raw[1];
}

size_t
cluster_undo_get_record(UBA uba, void *out_buffer, size_t buffer_size)
{
	test_undo_read_calls++;
	UT_ASSERT(test_uba_equal(uba, test_origin_locator.uba));
	UT_ASSERT(out_buffer != NULL);
	UT_ASSERT(buffer_size >= sizeof(test_origin_record));
	memcpy(out_buffer, &test_origin_record, sizeof(test_origin_record));
	return sizeof(test_origin_record);
}

bool
cluster_tt_slot_durable_lookup_committed_stable(
	uint32 segment_id, uint16 slot_offset, TransactionId xid, uint32 expected_wrap,
	ClusterTTDurableXidCommitCheck xid_committed, SCN *commit_scn)
{
	test_tt_exact_calls++;
	test_tt_segment_seen = segment_id;
	test_tt_slot_seen = slot_offset;
	test_tt_xid_seen = xid;
	test_tt_wrap_seen = (uint16)expected_wrap;
	if (test_tt_slot.status != TT_SLOT_COMMITTED || test_tt_slot.xid != xid
		|| test_tt_slot.wrap != (uint16)expected_wrap || !SCN_VALID(test_tt_slot.commit_scn)
		|| xid_committed == NULL || commit_scn == NULL || !xid_committed(xid))
		return false;
	*commit_scn = test_tt_slot.commit_scn;
	return true;
}

bool
cluster_tt_slot_durable_read_exact_stable(uint32 segment_id, uint16 slot_offset,
											   TransactionId xid, uint16 expected_wrap,
											   TTSlot *slot_out)
{
	test_tt_snapshot_calls++;
	test_tt_segment_seen = segment_id;
	test_tt_slot_seen = slot_offset;
	test_tt_xid_seen = xid;
	test_tt_wrap_seen = expected_wrap;
	if (slot_out == NULL || test_tt_slot.status > TT_SLOT_RECYCLABLE
		|| test_tt_slot.xid != xid || test_tt_slot.wrap != expected_wrap)
		return false;
	*slot_out = test_tt_slot;
	return true;
}

ClusterTTDurableResolve
cluster_tt_slot_durable_resolve_by_xid_origin(int origin_node pg_attribute_unused(),
											  TransactionId xid pg_attribute_unused(),
											  uint32 expected_wrap pg_attribute_unused(),
											  SCN *commit_scn pg_attribute_unused(),
											  uint16 *out_seg pg_attribute_unused(),
											  uint16 *out_slot pg_attribute_unused(),
											  uint16 *out_wrap pg_attribute_unused())
{
	test_by_xid_scan_calls++;
	return CLUSTER_TT_DURABLE_SCAN_UNAVAILABLE;
}

ClusterTTDurableLocate
cluster_tt_slot_durable_locate_any_by_xid_origin(int origin_node,
	TransactionId xid, uint16 *out_seg, uint16 *out_slot,
	uint16 *out_wrap, uint8 *out_status)
{
	UT_ASSERT_EQ(origin_node, 0);
	UT_ASSERT_EQ((int)xid, (int)TEST_ORIGIN_XID);
	if (!test_durable_locator_available)
		return CLUSTER_TT_DURABLE_LOCATE_MISSING;
	UT_ASSERT_NOT_NULL(out_seg);
	UT_ASSERT_NOT_NULL(out_slot);
	UT_ASSERT_NOT_NULL(out_wrap);
	*out_seg = TEST_TT_SEGMENT;
	*out_slot = TEST_TT_OFFSET;
	*out_wrap = TEST_ORIGIN_WRAP;
	if (out_status != NULL)
		*out_status = test_tt_slot.status;
	return CLUSTER_TT_DURABLE_LOCATE_FOUND;
}

int
cluster_xid_origin_slot(TransactionId xid)
{
	UT_ASSERT_EQ((int)xid, (int)TEST_ORIGIN_XID);
	return 0;
}

uint64
GetSystemIdentifier(void)
{
	return UINT64_C(0x1122334455667788);
}

TimestampTz
GetCurrentTimestamp(void)
{
	return 0;
}

uint64
cluster_qvotec_get_self_incarnation(void)
{
	return UINT64_C(9001);
}

bool
cluster_reconfig_get_observed_slot(int32 node_id pg_attribute_unused(),
	uint64 *incarnation, uint64 *generation)
{
	if (incarnation != NULL)
		*incarnation = 0;
	if (generation != NULL)
		*generation = 0;
	return false;
}

uint64
cluster_reconfig_get_observed_epoch(int32 node_id pg_attribute_unused())
{
	return 0;
}

uint64
cluster_membership_get_last_admitted_incarnation(
	int32 node_id pg_attribute_unused())
{
	return 0;
}

ClusterSemanticAdmissionResult
cluster_semantic_activation_enter(uint64 feature_bit,
	ClusterSemanticAdmissionSide side, ClusterSemanticAdmissionToken *token)
{
	UT_ASSERT_NOT_NULL(token);
	UT_ASSERT_EQ(feature_bit, CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1);
	UT_ASSERT_EQ(side, CLUSTER_SEMANTIC_TARGET_SIDE);
	memset(token, 0, sizeof(*token));
	token->feature_bit = feature_bit;
	token->record_generation = 77;
	token->formation_epoch = test_formation_epoch;
	token->side = side;
	token->entered = true;
	return CLUSTER_SEMANTIC_ADMISSION_OK;
}

void
cluster_semantic_activation_leave(ClusterSemanticAdmissionToken *token)
{
	UT_ASSERT_NOT_NULL(token);
	token->entered = false;
}

XidStatus
TransactionIdGetStatus(TransactionId xid, XLogRecPtr *lsn)
{
	XidStatus status;

	test_native_status_calls++;
	if (test_native_throw)
		pg_re_throw();
	if (lsn != NULL)
		*lsn = test_flush_lsn;
	if (test_native_script_pos < test_native_script_count) {
		UT_ASSERT_EQ((int)xid, (int)test_native_script_xids[test_native_script_pos]);
		status = test_native_script_statuses[test_native_script_pos++];
	} else {
		UT_ASSERT_EQ((int)xid, (int)TEST_ORIGIN_XID);
		status = test_native_status;
	}
	return status;
}

bool
TransactionIdPrecedes(TransactionId id1, TransactionId id2)
{
	int32 diff;

	if (!TransactionIdIsNormal(id1) || !TransactionIdIsNormal(id2))
		return id1 < id2;
	diff = (int32)(id1 - id2);
	return diff < 0;
}

TransactionId
SubTransGetParent(TransactionId xid)
{
	int i;
	int phase;

	test_subtrans_parent_calls++;
	if (test_subtrans_cycle)
		return xid;
	for (i = 0; i < test_subtrans_chain_count; i++) {
		if (test_subtrans_chain[i] != xid)
			continue;
		phase = (test_subtrans_parent_calls - 1) / test_subtrans_chain_count;
		if (test_subtrans_mutate_recheck && phase > 0 && i == 0)
			return test_subtrans_chain[i] - 2;
		return i + 1 < test_subtrans_chain_count ? test_subtrans_chain[i + 1]
											 : InvalidTransactionId;
	}
	UT_ASSERT(false);
	return InvalidTransactionId;
}

bool
TwoPhaseTransactionIdIsPrepared(TransactionId xid)
{
	test_twophase_calls++;
	UT_ASSERT_EQ((int)xid, (int)test_twophase_xid);
	return test_twophase_prepared;
}

uint64
cluster_epoch_get_current(void)
{
	return test_formation_epoch;
}

XLogRecPtr
GetFlushRecPtr(TimeLineID *insertTLI)
{
	if (insertTLI != NULL)
		*insertTLI = 1;
	return test_flush_lsn;
}

uint64
cluster_undo_tt_retention_rollover_count(void)
{
	return test_tt_generation;
}

SCN
cluster_scn_current(void)
{
	return test_authority_scn;
}

static bool
test_admission_token_exact(const ClusterSemanticAdmissionToken *token)
{
	return token != NULL && token->entered
		   && token->feature_bit == CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1
		   && token->side == CLUSTER_SEMANTIC_TARGET_SIDE
		   && token->formation_epoch == test_formation_epoch;
}

bool
cluster_semantic_activation_recheck(
	const ClusterSemanticAdmissionToken *token)
{
	test_regular_admission_recheck_calls++;
	return test_admission_token_exact(token);
}

bool
cluster_semantic_activation_recheck_r4_terminal_census(
	const ClusterSemanticAdmissionToken *token)
{
	test_terminal_census_recheck_calls++;
	return test_admission_token_exact(token);
}

static bool
test_resolve_shared_undo_root(const ClusterSemanticAdmissionToken *token,
	ClusterUndoPathIntent intent, uint32 owner_instance, uint32 segment_id,
	ClusterUndoBlock0ResolvedRoot *out)
{
	UT_ASSERT(test_admission_token_exact(token));
	UT_ASSERT_EQ(intent, CLUSTER_UNDO_PATH_RUNTIME_SHARED);
	UT_ASSERT_EQ(owner_instance, 1);
	UT_ASSERT(segment_id == TEST_RECORD_SEGMENT || segment_id == TEST_TT_SEGMENT);
	out->intent = intent;
	out->root_id = segment_id == TEST_RECORD_SEGMENT ? 91 : 92;
	out->root_generation = 7;
	return true;
}

bool
cluster_semantic_activation_resolve_shared_undo_root(
	const ClusterSemanticAdmissionToken *token, ClusterUndoPathIntent intent,
	uint32 owner_instance, uint32 segment_id,
	ClusterUndoBlock0ResolvedRoot *out)
{
	test_regular_root_resolve_calls++;
	return test_resolve_shared_undo_root(token, intent, owner_instance,
		segment_id, out);
}

bool
cluster_semantic_activation_resolve_shared_undo_root_r4_terminal_census(
	const ClusterSemanticAdmissionToken *token, ClusterUndoPathIntent intent,
	uint32 owner_instance, uint32 segment_id,
	ClusterUndoBlock0ResolvedRoot *out)
{
	test_terminal_census_root_resolve_calls++;
	return test_resolve_shared_undo_root(token, intent, owner_instance,
		segment_id, out);
}

ClusterUndoBlock0CurrentStep
cluster_undo_block0_current_acquire_begin_admitted(
	const ClusterUndoBlock0LogicalKey *key, ClusterUndoBlock0CurrentMode mode,
	int timeout_ms pg_attribute_unused(),
	const ClusterSemanticAdmissionToken *admission,
	ClusterUndoBlock0CurrentGuard *guard pg_attribute_unused(),
	ClusterUndoBlock0Result *failure pg_attribute_unused())
{
	UT_ASSERT(test_candidate_acquire_calls
			  < (int)lengthof(test_candidate_acquire_segments));
	test_candidate_acquire_segments[test_candidate_acquire_calls]
		= key->segment_id;
	test_candidate_acquire_calls++;
	UT_ASSERT_EQ(key->owner_instance, 1);
	UT_ASSERT(key->segment_id == TEST_RECORD_SEGMENT
			  || key->segment_id == TEST_TT_SEGMENT);
	UT_ASSERT_EQ(mode, CLUSTER_UNDO_BLOCK0_SCUR);
	UT_ASSERT(test_admission_token_exact(admission));
	if (test_candidate_acquire_step == CLUSTER_UNDO_BLOCK0_CURRENT_HELD) {
		test_candidate_held_count++;
		if (test_candidate_max_held_count < test_candidate_held_count)
			test_candidate_max_held_count = test_candidate_held_count;
	}
	return test_candidate_acquire_step;
}

ClusterUndoBlock0CurrentStep
cluster_undo_block0_current_acquire_poll(
	ClusterUndoBlock0CurrentGuard *guard pg_attribute_unused(),
	ClusterUndoBlock0Result *failure pg_attribute_unused())
{
	test_candidate_poll_calls++;
	if (test_candidate_poll_step == CLUSTER_UNDO_BLOCK0_CURRENT_HELD) {
		test_candidate_held_count++;
		test_candidate_max_held_count
			= Max(test_candidate_max_held_count, test_candidate_held_count);
	}
	return test_candidate_poll_step;
}

bool
cluster_undo_block0_current_wait_reply(ClusterUndoBlock0CurrentGuard *guard pg_attribute_unused(),
									   ClusterUndoBlock0ReplyWaitSite site)
{
	UT_ASSERT(site == CLUSTER_UNDO_BLOCK0_WAIT_RUNTIME_VIS_ACQUIRE
			  || site == CLUSTER_UNDO_BLOCK0_WAIT_RUNTIME_VIS_RELEASE);
	test_candidate_wait_calls++;
	UT_ASSERT_EQ(test_candidate_poll_calls, test_candidate_wait_calls);
	/* A wake alone is not a grant: the product must repoll each time. */
	if (test_candidate_wait_calls >= test_candidate_wakes_before_terminal)
		test_candidate_poll_step = test_candidate_after_wait_step;
	return true;
}

ClusterUndoBlock0Result
cluster_undo_block0_current_sample_generation(
	ClusterUndoBlock0CurrentGuard *guard pg_attribute_unused(),
	const ClusterUndoBlock0ResolvedRoot *root,
	ClusterUndoBlock0Generation *observed)
{
	test_candidate_sample_calls++;
	UT_ASSERT(root->root_id == 91 || root->root_id == 92);
	if (test_candidate_sample_result == CLUSTER_UNDO_BLOCK0_OK)
		*observed = (ClusterUndoBlock0Generation){ true, root->root_id == 91
															 ? test_candidate_data_generation
															 : test_candidate_tt_generation };
	return test_candidate_sample_result;
}

ClusterUndoBlock0Result
cluster_undo_block0_current_copy_resident(
	ClusterUndoBlock0CurrentGuard *guard pg_attribute_unused(),
	const ClusterUndoBlock0ResolvedRoot *root,
	const ClusterUndoBlock0Generation *expected, char private_page[BLCKSZ])
{
	UndoSegmentHeaderData *header = (UndoSegmentHeaderData *)private_page;

	test_candidate_block0_copy_calls++;
	UT_ASSERT(expected->known);
	UT_ASSERT_EQ(expected->value, root->root_id == 91 ? test_candidate_data_generation
													  : test_candidate_tt_generation);
	memset(private_page, 0, BLCKSZ);
	if (test_candidate_copy_physical_slot)
		header->tt_slots[TEST_TT_OFFSET] = test_tt_slot;
	return CLUSTER_UNDO_BLOCK0_OK;
}

bool
cluster_tt_slot_current_owner_by_xid(int node_id, TransactionId xid,
								 ClusterTTSlotCurrentOwner *out)
{
	ClusterTTSlotCurrentOwner sampled;

	test_current_owner_calls++;
	UT_ASSERT_EQ(node_id, cluster_node_id);
	UT_ASSERT_EQ((int)xid, (int)TEST_ORIGIN_XID);
	UT_ASSERT(out != NULL);
	memset(out, 0, sizeof(*out));
	if (!test_current_owner_available)
		return false;
	sampled = test_current_owner;
	if (test_current_owner_drift_on_recheck && test_current_owner_calls > 2)
		sampled.wrap++;
	*out = sampled;
	return true;
}

bool
cluster_tt_local_get_published_binding(TransactionId xid,
								   ClusterCanonicalTxnBinding *out)
{
	ClusterCanonicalTxnBinding sampled;

	test_local_binding_calls++;
	UT_ASSERT_EQ((int)xid, (int)TEST_ORIGIN_XID);
	UT_ASSERT_NOT_NULL(out);
	memset(out, 0, sizeof(*out));
	if (!test_local_binding_available)
		return false;
	sampled = test_local_binding;
	if (test_local_binding_drift_on_recheck && test_local_binding_calls > 1)
		sampled.slot_wrap++;
	*out = sampled;
	return true;
}

ClusterCtrcTouchResult
cluster_ctrc_origin_touch_exact(const ClusterCtrcTxnKeyV1 *key,
	const ClusterCtrcParticipantIdentity *participant,
	ClusterCtrcProofClass proof_class, uint32 *grant_out)
{
	test_ctrc_touch_calls++;
	UT_ASSERT_NOT_NULL(key);
	UT_ASSERT_NOT_NULL(participant);
	UT_ASSERT_NOT_NULL(grant_out);
	UT_ASSERT_EQ(key->segment_id, TEST_TT_SEGMENT);
	UT_ASSERT_EQ(key->segment_generation, (uint32)23);
	UT_ASSERT_EQ(key->slot_offset, TEST_TT_OFFSET);
	UT_ASSERT_EQ(key->slot_wrap, TEST_ORIGIN_WRAP);
	UT_ASSERT_EQ((int)key->xid, (int)TEST_ORIGIN_XID);
	UT_ASSERT_EQ(participant->node_id, (uint16)cluster_node_id);
	UT_ASSERT_EQ(proof_class, CTRC_PROOF_ACTIVE);
	*grant_out = test_ctrc_touch_grant;
	return test_ctrc_touch_grant == 0
		? CLUSTER_CTRC_TOUCH_REFUSED : CLUSTER_CTRC_TOUCH_RECORDED;
}

uint32
cluster_tt_slot_current_segment(int node_id)
{
	test_current_segment_calls++;
	UT_ASSERT_EQ(node_id, cluster_node_id);
	if (test_current_segment_drift_on_recheck
		&& test_current_segment_calls > 1)
		return test_current_segment + 1;
	return test_current_segment;
}

bool
cluster_xid_is_mine(TransactionId xid)
{
	UT_ASSERT_EQ((int)xid, (int)TEST_ORIGIN_XID);
	return test_xid_is_mine;
}

bool
cluster_cr_server_local_freshref_c1b_pair_exact(
	TransactionId xid, uint32 expected_segment_id,
	uint32 expected_tt_slot_id, SCN proposed_scn, uint16 *out_wrap)
{
	test_local_freshref_pair_calls++;
	UT_ASSERT_EQ((int) xid, (int) TEST_ORIGIN_XID);
	UT_ASSERT_EQ(expected_segment_id, TEST_RECORD_SEGMENT);
	UT_ASSERT_EQ(expected_tt_slot_id,
				 (uint32) TEST_TT_OFFSET + 1);
	UT_ASSERT_EQ((uint64) proposed_scn, (uint64) test_commit_scn);
	UT_ASSERT_NOT_NULL(out_wrap);
	if (!test_local_freshref_pair_exact)
		return false;
	*out_wrap = TEST_ORIGIN_WRAP;
	return true;
}

void
cluster_cr_native_prehistory_reader_lock(void)
{
	test_native_fence_lock_calls++;
	test_native_fence_depth++;
	if (test_no_raw_reuse_drift_on_recheck
		&& test_native_fence_lock_calls > 1)
		test_no_raw_reuse_window = false;
}

void
cluster_cr_native_prehistory_reader_unlock(void)
{
	test_native_fence_unlock_calls++;
	UT_ASSERT_EQ(test_native_fence_depth, 1);
	test_native_fence_depth--;
}

bool
cluster_cr_native_prehistory_disabled(void)
{
	UT_ASSERT_EQ(test_native_fence_depth, 1);
	return !test_no_raw_reuse_window;
}

bool
cluster_cr_native_origin_epoch0_provable(TransactionId xid)
{
	UT_ASSERT_EQ(test_native_fence_depth, 1);
	UT_ASSERT_EQ((int)xid, (int)TEST_ORIGIN_XID);
	return test_native_origin_provable && test_no_raw_reuse_window;
}

bool
TransactionIdIsInProgress(TransactionId xid)
{
	UT_ASSERT_EQ((int)xid, (int)TEST_ORIGIN_XID);
	UT_ASSERT_EQ(test_native_fence_depth, 1);
	UT_ASSERT_EQ(test_xact_lock_depth, 0);
	test_procarray_calls++;
	return test_procarray_live;
}

bool
LWLockAcquire(LWLock *lock, LWLockMode mode)
{
	UT_ASSERT(lock == XactTruncationLock);
	UT_ASSERT_EQ(mode, LW_SHARED);
	test_xact_lock_depth++;
	return true;
}

void
LWLockRelease(LWLock *lock)
{
	UT_ASSERT(lock == XactTruncationLock);
	UT_ASSERT_EQ(test_xact_lock_depth, 1);
	test_xact_lock_depth--;
}

void
LWLockReleaseAll(void)
{
	test_xact_lock_depth = 0;
	test_native_fence_depth = 0;
}

bool
cluster_undo_buf_copy_resident(uint32 segment_id, uint8 owner,
							   uint32 block_no, char dst[BLCKSZ])
{
	test_candidate_data_copy_calls++;
	UT_ASSERT_EQ(segment_id, TEST_RECORD_SEGMENT);
	UT_ASSERT_EQ(owner, 1);
	UT_ASSERT_EQ(block_no, 7);
	memset(dst, 0x5a, BLCKSZ);
	return true;
}

bool
cluster_cr_r4_extract_resident_record(
	const char resident_undo_page[BLCKSZ] pg_attribute_unused(),
	const ClusterTxLocator *request_locator, char record_out[BLCKSZ],
	size_t *record_length_out, ClusterTxLocator *canonical_locator_out)
{
	test_candidate_extract_calls++;
	UT_ASSERT_EQ(request_locator->tt_wrap, TT_WRAP_INVALID);
	if (test_candidate_mutate_record_on_recheck
		&& test_candidate_extract_calls == 2)
		test_origin_record.payload_length++;
	memcpy(record_out, &test_origin_record, sizeof(test_origin_record));
	*record_length_out = sizeof(test_origin_record);
	*canonical_locator_out = *request_locator;
	canonical_locator_out->tt_wrap = TEST_ORIGIN_WRAP;
	return true;
}

ClusterUndoBlock0CurrentStep
cluster_undo_block0_current_release_begin(
	ClusterUndoBlock0CurrentGuard *guard pg_attribute_unused(),
	ClusterUndoBlock0Result *failure pg_attribute_unused())
{
	test_candidate_release_calls++;
	UT_ASSERT(test_candidate_held_count > 0);
	test_candidate_held_count--;
	return CLUSTER_UNDO_BLOCK0_CURRENT_RELEASED;
}

ClusterUndoBlock0CurrentStep
cluster_undo_block0_current_release_poll(
	ClusterUndoBlock0CurrentGuard *guard pg_attribute_unused(),
	ClusterUndoBlock0Result *failure pg_attribute_unused())
{
	UT_ASSERT(false);
	return CLUSTER_UNDO_BLOCK0_CURRENT_FAILED;
}

void
cluster_undo_block0_current_cancel(
	ClusterUndoBlock0CurrentGuard *guard pg_attribute_unused())
{
	test_candidate_cancel_calls++;
}

ClusterTxOutcome
cluster_gcs_block_r4_tx_resolve_fetch_and_wait(
	int32 origin_node pg_attribute_unused(),
	const ClusterTxLocator *locator pg_attribute_unused(),
	uint32 expected_physical_generation pg_attribute_unused(),
	uint64 formation_epoch pg_attribute_unused(),
	ClusterTxResolution *out pg_attribute_unused(),
	ClusterTxResolveReason *reason_out pg_attribute_unused())
{
	UT_ASSERT(false);
	return CLUSTER_TX_UNKNOWN;
}

static void
reset_exact_origin_fixture(void)
{
	memset(&test_origin_locator, 0, sizeof(test_origin_locator));
	test_origin_locator.uba = uba_encode(TEST_RECORD_SEGMENT, 7, TEST_TT_OFFSET, 400);
	test_origin_locator.xid = TEST_ORIGIN_XID;
	test_origin_locator.tt_wrap = TEST_ORIGIN_WRAP;
	test_origin_locator.itl_kind = ITL_FLAG_ACTIVE;
	test_origin_locator.itl_slot_index = 2;

	memset(&test_origin_record, 0, sizeof(test_origin_record));
	test_origin_record.record_type = UNDO_RECORD_UPDATE;
	test_origin_record.xid = TEST_ORIGIN_XID;
	test_origin_record.origin_node_id = 0;
	test_origin_record.tt_slot_segment_id = TEST_TT_SEGMENT;
	test_origin_record.tt_slot_id = cluster_tt_slot_offset_to_id(TEST_TT_OFFSET);
	test_origin_record.tt_wrap_plus1 = (uint16)(TEST_ORIGIN_WRAP + 1);

	test_undo_read_calls = 0;
	test_tt_exact_calls = 0;
	test_tt_snapshot_calls = 0;
	test_native_status_calls = 0;
	test_by_xid_scan_calls = 0;
	test_durable_locator_available = true;
	test_tt_segment_seen = 0;
	test_tt_slot_seen = 0;
	test_tt_xid_seen = InvalidTransactionId;
	test_tt_wrap_seen = TT_WRAP_INVALID;
	test_native_status = TRANSACTION_STATUS_COMMITTED;
	test_native_script_count = 0;
	test_native_script_pos = 0;
	test_subtrans_chain_count = 0;
	test_subtrans_parent_calls = 0;
	test_subtrans_mutate_recheck = false;
	test_subtrans_cycle = false;
	test_twophase_prepared = false;
	test_twophase_xid = InvalidTransactionId;
	test_twophase_calls = 0;
	TransactionXmin = FirstNormalTransactionId;
	test_commit_scn = scn_encode(0, 80);
	memset(&test_tt_slot, 0, sizeof(test_tt_slot));
	test_tt_slot.status = TT_SLOT_COMMITTED;
	test_tt_slot.xid = TEST_ORIGIN_XID;
	test_tt_slot.wrap = TEST_ORIGIN_WRAP;
	test_tt_slot.commit_scn = test_commit_scn;
	test_formation_epoch = 42;
	test_flush_lsn = (XLogRecPtr)UINT64CONST(0x12345678);
	test_tt_generation = 9;
	test_authority_scn = scn_encode(0, 100);
	test_candidate_acquire_calls = 0;
	test_candidate_sample_calls = 0;
	test_candidate_block0_copy_calls = 0;
	test_candidate_data_copy_calls = 0;
	test_candidate_release_calls = 0;
	test_candidate_cancel_calls = 0;
	test_candidate_poll_calls = 0;
	test_candidate_wait_calls = 0;
	test_candidate_wakes_before_terminal = 1;
	memset(test_vis_evidence, 0, sizeof(test_vis_evidence));
	test_candidate_exit_hooks_ensure_calls = 0;
	memset(test_candidate_acquire_segments, 0,
		   sizeof(test_candidate_acquire_segments));
	test_candidate_held_count = 0;
	test_candidate_max_held_count = 0;
	test_candidate_extract_calls = 0;
	test_candidate_mutate_record_on_recheck = false;
	test_candidate_sample_result = CLUSTER_UNDO_BLOCK0_OK;
	test_candidate_data_generation = 17;
	test_candidate_tt_generation = 23;
	test_candidate_copy_physical_slot = true;
	test_current_owner_available = false;
	test_current_owner_drift_on_recheck = false;
	test_current_owner_calls = 0;
	memset(&test_current_owner, 0, sizeof(test_current_owner));
	test_current_owner.segment_id = TEST_TT_SEGMENT;
	test_current_owner.xid = TEST_ORIGIN_XID;
	test_current_owner.slot_offset = TEST_TT_OFFSET;
	test_current_owner.wrap = TEST_ORIGIN_WRAP;
	test_current_owner.status = CTS_ACTIVE;
	test_local_binding_available = false;
	test_local_binding_drift_on_recheck = false;
	test_local_binding_calls = 0;
	memset(&test_local_binding, 0, sizeof(test_local_binding));
	test_local_binding.segment_id = TEST_TT_SEGMENT;
	test_local_binding.segment_generation = 23;
	test_local_binding.xid = TEST_ORIGIN_XID;
	test_local_binding.slot_offset = TEST_TT_OFFSET;
	test_local_binding.slot_wrap = TEST_ORIGIN_WRAP;
	test_local_binding.origin_instance = 1;
	test_local_binding.publish_state = CLUSTER_CANONICAL_TXN_PUBLISHED;
	test_local_binding.active_lsn = test_flush_lsn;
	test_ctrc_touch_calls = 0;
	test_ctrc_touch_grant = 41;
	test_current_segment = TEST_TT_SEGMENT + 1;
	test_current_segment_drift_on_recheck = false;
	test_current_segment_calls = 0;
	test_no_raw_reuse_window = true;
	test_no_raw_reuse_drift_on_recheck = false;
	test_xid_is_mine = true;
	test_native_origin_provable = false;
	test_native_throw = false;
	test_procarray_live = true;
	test_native_fence_depth = 0;
	test_native_fence_lock_calls = 0;
	test_native_fence_unlock_calls = 0;
	test_procarray_calls = 0;
	test_xact_lock_depth = 0;
	memset(&test_variable_cache, 0, sizeof(test_variable_cache));
	test_variable_cache.oldestClogXid = FirstNormalTransactionId;
	test_regular_admission_recheck_calls = 0;
	test_terminal_census_recheck_calls = 0;
	test_regular_root_resolve_calls = 0;
	test_terminal_census_root_resolve_calls = 0;
	test_local_freshref_pair_exact = false;
	test_local_freshref_pair_calls = 0;
	test_candidate_acquire_step = CLUSTER_UNDO_BLOCK0_CURRENT_HELD;
	test_candidate_poll_step = CLUSTER_UNDO_BLOCK0_CURRENT_FAILED;
	test_candidate_after_wait_step = CLUSTER_UNDO_BLOCK0_CURRENT_FAILED;
}

static const bool expected[5][8] = {
	[CLUSTER_TX_UNKNOWN] = {
		[CLUSTER_TX_PROOF_NONE] = true,
		[CLUSTER_TX_PROOF_RECYCLED_BELOW_HORIZON] = true,
	},
	[CLUSTER_TX_IN_PROGRESS] = {
		[CLUSTER_TX_PROOF_ORIGIN_DURABLE_TT_CLOG] = true,
		[CLUSTER_TX_PROOF_ORIGIN_SUBTRANS_TOP] = true,
		[CLUSTER_TX_PROOF_ORIGIN_MULTIXACT] = true,
	},
	[CLUSTER_TX_PREPARED] = {
		[CLUSTER_TX_PROOF_ORIGIN_SUBTRANS_TOP] = true,
		[CLUSTER_TX_PROOF_ORIGIN_TWOPHASE] = true,
		[CLUSTER_TX_PROOF_ORIGIN_MULTIXACT] = true,
	},
	[CLUSTER_TX_COMMITTED] = {
		[CLUSTER_TX_PROOF_ITL_CLEANOUT] = true,
		[CLUSTER_TX_PROOF_ORIGIN_DURABLE_TT_CLOG] = true,
		[CLUSTER_TX_PROOF_ORIGIN_SUBTRANS_TOP] = true,
		[CLUSTER_TX_PROOF_ORIGIN_MULTIXACT] = true,
		[CLUSTER_TX_PROOF_RECOVERY_MATERIALIZED] = true,
	},
	[CLUSTER_TX_ABORTED] = {
		[CLUSTER_TX_PROOF_ITL_CLEANOUT] = true,
		[CLUSTER_TX_PROOF_ORIGIN_DURABLE_TT_CLOG] = true,
		[CLUSTER_TX_PROOF_ORIGIN_SUBTRANS_TOP] = true,
		[CLUSTER_TX_PROOF_ORIGIN_MULTIXACT] = true,
		[CLUSTER_TX_PROOF_RECOVERY_MATERIALIZED] = true,
	},
};

/* Current-MultiXact member proofs must
 * consume the R4 TARGET canonical physical TT slot.  The allocator snapshot
 * is only a locator/corroboration input; no SOURCE overlay participates. */
extern bool cluster_runtime_visibility_current_owner_sample_held(
	TransactionId xid, const ClusterTTSlotCurrentOwner *expected_owner,
	const ClusterSemanticAdmissionToken *admission,
	ClusterUndoBlock0CurrentGuard *guard,
	const ClusterUndoBlock0ResolvedRoot *root,
	ClusterTTStatusKey *key_out, ClusterTTStatusResult *result_out,
	bool *ctrc_physical_active_out);

static void
run_pair(unsigned int pair)
{
	ClusterTxOutcome outcome = (ClusterTxOutcome)(pair / 8);
	ClusterTxProofKind proof = (ClusterTxProofKind)(pair % 8);

	UT_ASSERT_EQ(cluster_tx_outcome_proof_is_valid(outcome, proof), expected[outcome][proof]);
}

#define DEFINE_PAIR_TEST(n) \
	UT_TEST(test_outcome_proof_pair_##n) { run_pair(n); }

#define RUN_PAIR_TEST(n) UT_RUN(test_outcome_proof_pair_##n)

DEFINE_PAIR_TEST(0)
DEFINE_PAIR_TEST(1)
DEFINE_PAIR_TEST(2)
DEFINE_PAIR_TEST(3)
DEFINE_PAIR_TEST(4)
DEFINE_PAIR_TEST(5)
DEFINE_PAIR_TEST(6)
DEFINE_PAIR_TEST(7)
DEFINE_PAIR_TEST(8)
DEFINE_PAIR_TEST(9)
DEFINE_PAIR_TEST(10)
DEFINE_PAIR_TEST(11)
DEFINE_PAIR_TEST(12)
DEFINE_PAIR_TEST(13)
DEFINE_PAIR_TEST(14)
DEFINE_PAIR_TEST(15)
DEFINE_PAIR_TEST(16)
DEFINE_PAIR_TEST(17)
DEFINE_PAIR_TEST(18)
DEFINE_PAIR_TEST(19)
DEFINE_PAIR_TEST(20)
DEFINE_PAIR_TEST(21)
DEFINE_PAIR_TEST(22)
DEFINE_PAIR_TEST(23)
DEFINE_PAIR_TEST(24)
DEFINE_PAIR_TEST(25)
DEFINE_PAIR_TEST(26)
DEFINE_PAIR_TEST(27)
DEFINE_PAIR_TEST(28)
DEFINE_PAIR_TEST(29)
DEFINE_PAIR_TEST(30)
DEFINE_PAIR_TEST(31)
DEFINE_PAIR_TEST(32)
DEFINE_PAIR_TEST(33)
DEFINE_PAIR_TEST(34)
DEFINE_PAIR_TEST(35)
DEFINE_PAIR_TEST(36)
DEFINE_PAIR_TEST(37)
DEFINE_PAIR_TEST(38)
DEFINE_PAIR_TEST(39)

UT_TEST(test_out_of_domain_values_fail_closed)
{
	UT_ASSERT(!cluster_tx_outcome_proof_is_valid((ClusterTxOutcome)-1, CLUSTER_TX_PROOF_NONE));
	UT_ASSERT(!cluster_tx_outcome_proof_is_valid((ClusterTxOutcome)5, CLUSTER_TX_PROOF_NONE));
	UT_ASSERT(!cluster_tx_outcome_proof_is_valid(CLUSTER_TX_UNKNOWN, (ClusterTxProofKind)-1));
	UT_ASSERT(!cluster_tx_outcome_proof_is_valid(CLUSTER_TX_UNKNOWN, (ClusterTxProofKind)8));
}

UT_TEST(test_current_member_target_canonical_active_requires_exact_physical_slot)
{
	ClusterSemanticAdmissionToken admission;
	ClusterUndoBlock0CurrentGuard guard;
	ClusterUndoBlock0ResolvedRoot root;
	ClusterTTStatusKey key;
	ClusterTTStatusResult result;

	reset_exact_origin_fixture();
	test_native_status = TRANSACTION_STATUS_IN_PROGRESS;
	test_twophase_xid = TEST_ORIGIN_XID;
	test_tt_slot.status = TT_SLOT_ACTIVE;
	test_tt_slot.xid = TEST_ORIGIN_XID;
	test_tt_slot.wrap = TEST_ORIGIN_WRAP;
	test_tt_slot.commit_scn = InvalidScn;
	test_current_owner_available = true;
	test_current_owner.status = CTS_ACTIVE;
	memset(&admission, 0, sizeof(admission));
	admission.feature_bit = CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1;
	admission.record_generation = 5;
	admission.formation_epoch = test_formation_epoch;
	admission.side = CLUSTER_SEMANTIC_TARGET_SIDE;
	admission.entered = true;
	memset(&guard, 0, sizeof(guard));
	memset(&root, 0, sizeof(root));
	root.intent = CLUSTER_UNDO_PATH_RUNTIME_SHARED;
	root.root_id = 92;
	root.root_generation = 7;
	memset(&key, 0xa5, sizeof(key));
	memset(&result, 0xa5, sizeof(result));

	UT_ASSERT(cluster_runtime_visibility_current_owner_sample_held(
		TEST_ORIGIN_XID, &test_current_owner, &admission, &guard, &root,
		&key, &result, NULL));
	UT_ASSERT_EQ(key.origin_node_id, (uint16)cluster_node_id);
	UT_ASSERT_EQ(key.undo_segment_id, (uint16)TEST_TT_SEGMENT);
	UT_ASSERT_EQ(key.tt_slot_id,
		cluster_tt_slot_offset_to_id(TEST_TT_OFFSET));
	UT_ASSERT_EQ(key.cluster_epoch, (uint32)test_formation_epoch);
	UT_ASSERT_EQ(key.local_xid, TEST_ORIGIN_XID);
	UT_ASSERT_EQ(result.status, CLUSTER_TT_STATUS_IN_PROGRESS);
	UT_ASSERT_EQ(result.commit_scn, InvalidScn);
	UT_ASSERT_EQ(result.status_epoch, (uint32)test_formation_epoch);
	UT_ASSERT(result.authoritative);
	UT_ASSERT_EQ(test_candidate_block0_copy_calls, 1);
	UT_ASSERT_EQ(test_current_owner_calls, 1);

	/* Empty physical bytes cannot be promoted by the allocator snapshot. */
	test_candidate_copy_physical_slot = false;
	test_current_owner_calls = 0;
	memset(&key, 0xa5, sizeof(key));
	memset(&result, 0xa5, sizeof(result));
	UT_ASSERT(!cluster_runtime_visibility_current_owner_sample_held(
		TEST_ORIGIN_XID, &test_current_owner, &admission, &guard, &root,
		&key, &result, NULL));
	UT_ASSERT_EQ(result.status, CLUSTER_TT_STATUS_UNKNOWN);
	UT_ASSERT(!result.authoritative);

	/* A physical wrap mismatch and a post-sample allocator drift both remain
	 * fail closed; neither can be repaired from raw xid/native state. */
	test_candidate_copy_physical_slot = true;
	test_tt_slot.wrap++;
	UT_ASSERT(!cluster_runtime_visibility_current_owner_sample_held(
		TEST_ORIGIN_XID, &test_current_owner, &admission, &guard, &root,
		&key, &result, NULL));
	test_tt_slot.wrap = TEST_ORIGIN_WRAP;
	/* The shared fixture mutates on its third owner sample. */
	test_current_owner_calls = 2;
	test_current_owner_drift_on_recheck = true;
	UT_ASSERT(!cluster_runtime_visibility_current_owner_sample_held(
		TEST_ORIGIN_XID, &test_current_owner, &admission, &guard, &root,
		&key, &result, NULL));
}

/* MXA-T19 x MXA-T20: the visibility bracket deliberately keeps a physical
 * COMMITTED/native-live predecessor conservative.  That result is not the
 * exact physical ACTIVE sample required to issue a CTRC grant. */
UT_TEST(test_ctrc_physical_active_sample_excludes_precommit_committed_window)
{
	ClusterSemanticAdmissionToken admission;
	ClusterUndoBlock0CurrentGuard guard;
	ClusterUndoBlock0ResolvedRoot root;
	ClusterTTStatusKey key;
	ClusterTTStatusResult result;
	bool physical_active = true;

	reset_exact_origin_fixture();
	test_native_status = TRANSACTION_STATUS_IN_PROGRESS;
	test_twophase_xid = TEST_ORIGIN_XID;
	test_current_owner_available = true;
	memset(&admission, 0, sizeof(admission));
	admission.feature_bit = CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1;
	admission.record_generation = 5;
	admission.formation_epoch = test_formation_epoch;
	admission.side = CLUSTER_SEMANTIC_TARGET_SIDE;
	admission.entered = true;
	memset(&guard, 0, sizeof(guard));
	memset(&root, 0, sizeof(root));
	root.intent = CLUSTER_UNDO_PATH_RUNTIME_SHARED;
	root.root_id = 92;
	root.root_generation = 7;

	UT_ASSERT(cluster_runtime_visibility_current_owner_sample_held(
		TEST_ORIGIN_XID, &test_current_owner, &admission, &guard, &root,
		&key, &result, &physical_active));
	UT_ASSERT_EQ(result.status, CLUSTER_TT_STATUS_IN_PROGRESS);
	UT_ASSERT_EQ(result.commit_scn, InvalidScn);
	UT_ASSERT(!physical_active);

	test_tt_slot.status = TT_SLOT_ACTIVE;
	test_tt_slot.commit_scn = InvalidScn;
	test_current_owner_calls = 0;
	UT_ASSERT(cluster_runtime_visibility_current_owner_sample_held(
		TEST_ORIGIN_XID, &test_current_owner, &admission, &guard, &root,
		&key, &result, &physical_active));
	UT_ASSERT(physical_active);
}

/* A backend-local canonical binding remains the exact owner locator for the
 * whole xid lifetime even after retention pressure rolls CURRENT forward.
 * CTRC PREPARE must therefore sample the binding's old physical segment,
 * validate its captured generation, and never substitute a fresh CURRENT
 * allocator scan. */
UT_TEST(test_ctrc_current_owner_uses_published_binding_across_rollover)
{
	ClusterTTStatusKey status_key;
	ClusterTTStatusResult status;
	ClusterCtrcTxnKeyV1 ctrc_key;
	ClusterCtrcParticipantIdentity participant;
	uint32 grant = 0;

	reset_exact_origin_fixture();
	test_native_status = TRANSACTION_STATUS_IN_PROGRESS;
	test_twophase_xid = TEST_ORIGIN_XID;
	test_tt_slot.status = TT_SLOT_ACTIVE;
	test_tt_slot.xid = TEST_ORIGIN_XID;
	test_tt_slot.wrap = TEST_ORIGIN_WRAP;
	test_tt_slot.commit_scn = InvalidScn;
	test_local_binding_available = true;
	test_current_owner_available = false;
	test_current_segment = TEST_TT_SEGMENT + 1;
	memset(&status_key, 0xa5, sizeof(status_key));
	memset(&status, 0xa5, sizeof(status));
	memset(&ctrc_key, 0xa5, sizeof(ctrc_key));
	memset(&participant, 0xa5, sizeof(participant));

	UT_ASSERT(cluster_runtime_visibility_current_owner_lookup_exact_ctrc_full(
		TEST_ORIGIN_XID, &status_key, &status, &grant, &ctrc_key,
		&participant));
	UT_ASSERT_EQ(status.status, CLUSTER_TT_STATUS_IN_PROGRESS);
	UT_ASSERT(status.authoritative);
	UT_ASSERT_EQ(grant, test_ctrc_touch_grant);
	UT_ASSERT_EQ(status_key.undo_segment_id, (uint16)TEST_TT_SEGMENT);
	UT_ASSERT_EQ(ctrc_key.segment_id, TEST_TT_SEGMENT);
	UT_ASSERT_EQ(ctrc_key.segment_generation,
		test_local_binding.segment_generation);
	UT_ASSERT_EQ(ctrc_key.slot_offset, TEST_TT_OFFSET);
	UT_ASSERT_EQ(ctrc_key.slot_wrap, TEST_ORIGIN_WRAP);
	UT_ASSERT_EQ(test_local_binding_calls, 2);
	UT_ASSERT_EQ(test_current_owner_calls, 0);
	UT_ASSERT_EQ(test_ctrc_touch_calls, 1);

	/* A stale backend receipt cannot authorize a recycled segment generation. */
	reset_exact_origin_fixture();
	test_native_status = TRANSACTION_STATUS_IN_PROGRESS;
	test_twophase_xid = TEST_ORIGIN_XID;
	test_tt_slot.status = TT_SLOT_ACTIVE;
	test_tt_slot.xid = TEST_ORIGIN_XID;
	test_tt_slot.wrap = TEST_ORIGIN_WRAP;
	test_tt_slot.commit_scn = InvalidScn;
	test_local_binding_available = true;
	test_local_binding.segment_generation = 22;
	UT_ASSERT(!cluster_runtime_visibility_current_owner_lookup_exact_ctrc_full(
		TEST_ORIGIN_XID, &status_key, &status, &grant, &ctrc_key,
		&participant));
	UT_ASSERT_EQ(test_ctrc_touch_calls, 0);

	/* Binding drift during the physical sample is equally fail closed. */
	reset_exact_origin_fixture();
	test_native_status = TRANSACTION_STATUS_IN_PROGRESS;
	test_twophase_xid = TEST_ORIGIN_XID;
	test_tt_slot.status = TT_SLOT_ACTIVE;
	test_tt_slot.xid = TEST_ORIGIN_XID;
	test_tt_slot.wrap = TEST_ORIGIN_WRAP;
	test_tt_slot.commit_scn = InvalidScn;
	test_local_binding_available = true;
	test_local_binding_drift_on_recheck = true;
	UT_ASSERT(!cluster_runtime_visibility_current_owner_lookup_exact_ctrc_full(
		TEST_ORIGIN_XID, &status_key, &status, &grant, &ctrc_key,
		&participant));
	UT_ASSERT_EQ(test_ctrc_touch_calls, 0);
}

UT_TEST(test_current_mx_active_proof_reconstructs_exact_ctrc_identity)
{
	ClusterCurrentMemberProofKey proof_key;
	ClusterCtrcTxnKeyV1 ctrc_key;
	ClusterCtrcParticipantIdentity participant;

	reset_exact_origin_fixture();
	test_tt_slot.status = TT_SLOT_ACTIVE;
	test_tt_slot.commit_scn = InvalidScn;
	memset(&proof_key, 0, sizeof(proof_key));
	proof_key.origin_node_id = 0;
	proof_key.undo_segment_id = TEST_TT_SEGMENT;
	proof_key.tt_slot_id = cluster_tt_slot_offset_to_id(TEST_TT_OFFSET);
	proof_key.cluster_epoch = (uint32)test_formation_epoch;
	proof_key.local_xid = TEST_ORIGIN_XID;
	proof_key.segment_generation = 23;
	proof_key.slot_wrap = TEST_ORIGIN_WRAP;
	proof_key.binding_version = CLUSTER_CURRENT_MEMBER_PROOF_BINDING_VERSION;
	memset(&ctrc_key, 0xa5, sizeof(ctrc_key));
	memset(&participant, 0xa5, sizeof(participant));

	UT_ASSERT(cluster_runtime_visibility_active_proof_ctrc_identity_exact(
		&proof_key, 17, 313, &ctrc_key, &participant));
	UT_ASSERT_EQ(ctrc_key.format_version, CLUSTER_CTRC_FORMAT_VERSION);
	UT_ASSERT_EQ(ctrc_key.origin_node_id, 0);
	UT_ASSERT_EQ(ctrc_key.owner_instance, 1);
	UT_ASSERT_EQ(ctrc_key.segment_id, TEST_TT_SEGMENT);
	UT_ASSERT_EQ(ctrc_key.segment_generation, 23);
	UT_ASSERT_EQ(ctrc_key.slot_offset, TEST_TT_OFFSET);
	UT_ASSERT_EQ(ctrc_key.slot_wrap, TEST_ORIGIN_WRAP);
	UT_ASSERT_EQ(ctrc_key.xid, TEST_ORIGIN_XID);
	UT_ASSERT_EQ(ctrc_key.cluster_epoch, test_formation_epoch);
	UT_ASSERT_EQ(participant.node_id, 0);
	UT_ASSERT(participant.boot_incarnation != 0);
	UT_ASSERT_EQ(participant.capability_record_generation, (uint32)313);
	UT_ASSERT_EQ(participant.formation_epoch, test_formation_epoch);
	UT_ASSERT_EQ(participant.admission_record_generation, (uint64)77);
	UT_ASSERT_EQ(participant.admission_record_generation,
		ctrc_key.admission_record_generation);

	memset(&ctrc_key, 0xa5, sizeof(ctrc_key));
	memset(&participant, 0xa5, sizeof(participant));
	UT_ASSERT(!cluster_runtime_visibility_active_proof_ctrc_identity_exact(
		&proof_key, 17, 0, &ctrc_key, &participant));
	UT_ASSERT_EQ(participant.capability_record_generation, (uint32)0);
}

/* MXA-T41: the member origin, not the requester, owns the canonical SCUR
 * sample.  Its proof must carry the exact generation/wrap needed to rebuild
 * the CTRC receipt identity; consuming that private proof must perform no
 * requester-side block-0 acquire, sample, copy, or publication. */
UT_TEST(test_current_mx_active_proof_uses_origin_binding_without_foreign_storage)
{
	ClusterCurrentMemberProofKey proof_key;
	ClusterCtrcTxnKeyV1 ctrc_key;
	ClusterCtrcParticipantIdentity participant;

	reset_exact_origin_fixture();
	test_tt_slot.status = TT_SLOT_ACTIVE;
	test_tt_slot.commit_scn = InvalidScn;
	test_durable_locator_available = false;
	memset(&proof_key, 0, sizeof(proof_key));
	proof_key.origin_node_id = 0;
	proof_key.undo_segment_id = TEST_TT_SEGMENT;
	proof_key.tt_slot_id = cluster_tt_slot_offset_to_id(TEST_TT_OFFSET);
	proof_key.cluster_epoch = (uint32)test_formation_epoch;
	proof_key.local_xid = TEST_ORIGIN_XID;
	proof_key.segment_generation = 23;
	proof_key.slot_wrap = TEST_ORIGIN_WRAP;
	proof_key.binding_version = CLUSTER_CURRENT_MEMBER_PROOF_BINDING_VERSION;
	test_candidate_sample_result = CLUSTER_UNDO_BLOCK0_NOT_PUBLISHED;
	memset(&ctrc_key, 0xa5, sizeof(ctrc_key));
	memset(&participant, 0xa5, sizeof(participant));

	UT_ASSERT(cluster_runtime_visibility_active_proof_ctrc_identity_exact(
		&proof_key, 17, 313, &ctrc_key, &participant));
	UT_ASSERT_EQ(test_candidate_acquire_calls, 0);
	UT_ASSERT_EQ(test_candidate_sample_calls, 0);
	UT_ASSERT_EQ(test_candidate_block0_copy_calls, 0);
	UT_ASSERT_EQ(test_candidate_release_calls, 0);
	UT_ASSERT_EQ(ctrc_key.segment_id, TEST_TT_SEGMENT);
	UT_ASSERT_EQ(ctrc_key.segment_generation, (uint32)23);
	UT_ASSERT_EQ(ctrc_key.slot_offset, TEST_TT_OFFSET);
	UT_ASSERT_EQ(ctrc_key.slot_wrap, TEST_ORIGIN_WRAP);
	UT_ASSERT_EQ(ctrc_key.xid, TEST_ORIGIN_XID);
	UT_ASSERT_EQ(participant.capability_record_generation, (uint32)313);
}

UT_TEST(test_current_member_rolled_terminal_uses_locator_then_canonical_scur)
{
	ClusterSemanticAdmissionToken admission;
	ClusterUndoBlock0CurrentGuard guard;
	ClusterUndoBlock0ResolvedRoot root;
	ClusterTTSlotPhysicalLocator locator;
	ClusterTTStatusKey key;
	ClusterTTStatusResult result;

	reset_exact_origin_fixture();
	test_native_status = TRANSACTION_STATUS_ABORTED;
	test_tt_slot.status = TT_SLOT_ABORTED;
	test_tt_slot.xid = TEST_ORIGIN_XID;
	test_tt_slot.wrap = TEST_ORIGIN_WRAP;
	test_tt_slot.commit_scn = InvalidScn;
	test_current_owner_available = false;
	test_current_segment = TEST_TT_SEGMENT + 1;
	memset(&admission, 0, sizeof(admission));
	admission.feature_bit = CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1;
	admission.record_generation = 5;
	admission.formation_epoch = test_formation_epoch;
	admission.side = CLUSTER_SEMANTIC_TARGET_SIDE;
	admission.entered = true;
	memset(&guard, 0, sizeof(guard));
	memset(&root, 0, sizeof(root));
	root.intent = CLUSTER_UNDO_PATH_RUNTIME_SHARED;
	root.root_id = 92;
	root.root_generation = 7;
	memset(&locator, 0, sizeof(locator));
	locator.segment_id = TEST_TT_SEGMENT;
	locator.xid = TEST_ORIGIN_XID;
	locator.slot_offset = TEST_TT_OFFSET;
	locator.wrap = TEST_ORIGIN_WRAP;

	UT_ASSERT(cluster_runtime_visibility_physical_locator_sample_held(
		&locator, &admission, &guard, &root, &key, &result, NULL));
	UT_ASSERT_EQ(result.status, CLUSTER_TT_STATUS_ABORTED);
	UT_ASSERT(result.authoritative);
	UT_ASSERT_EQ(key.undo_segment_id, (uint16)TEST_TT_SEGMENT);
	UT_ASSERT_EQ(key.tt_slot_id,
		cluster_tt_slot_offset_to_id(TEST_TT_OFFSET));
	UT_ASSERT_EQ(test_current_owner_calls, 0);
	UT_ASSERT_EQ(test_current_segment_calls, 2);

	/* Locator status is never a verdict: missing/mismatched canonical bytes
	 * and allocator rollover drift remain UNKNOWN. */
	test_candidate_copy_physical_slot = false;
	UT_ASSERT(!cluster_runtime_visibility_physical_locator_sample_held(
		&locator, &admission, &guard, &root, &key, &result, NULL));
	UT_ASSERT_EQ(result.status, CLUSTER_TT_STATUS_UNKNOWN);
	UT_ASSERT(!result.authoritative);
	test_candidate_copy_physical_slot = true;
	test_tt_slot.xid++;
	UT_ASSERT(!cluster_runtime_visibility_physical_locator_sample_held(
		&locator, &admission, &guard, &root, &key, &result, NULL));
	test_tt_slot.xid = TEST_ORIGIN_XID;
	test_current_segment_calls = 0;
	test_current_segment_drift_on_recheck = true;
	UT_ASSERT(!cluster_runtime_visibility_physical_locator_sample_held(
		&locator, &admission, &guard, &root, &key, &result, NULL));
}

UT_TEST(test_current_member_terminal_on_current_segment_survives_retired_owner_index)
{
	ClusterSemanticAdmissionToken admission;
	ClusterUndoBlock0CurrentGuard guard;
	ClusterUndoBlock0ResolvedRoot root;
	ClusterTTSlotPhysicalLocator locator;
	ClusterTTStatusKey key;
	ClusterTTStatusResult result;

	reset_exact_origin_fixture();
	test_native_status = TRANSACTION_STATUS_COMMITTED;
	test_tt_slot.status = TT_SLOT_COMMITTED;
	test_tt_slot.xid = TEST_ORIGIN_XID;
	test_tt_slot.wrap = TEST_ORIGIN_WRAP;
	test_tt_slot.commit_scn = test_commit_scn;
	test_current_segment = TEST_TT_SEGMENT;
	test_current_owner_available = false;
	memset(&admission, 0, sizeof(admission));
	admission.feature_bit = CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1;
	admission.record_generation = 5;
	admission.formation_epoch = test_formation_epoch;
	admission.side = CLUSTER_SEMANTIC_TARGET_SIDE;
	admission.entered = true;
	memset(&guard, 0, sizeof(guard));
	memset(&root, 0, sizeof(root));
	root.intent = CLUSTER_UNDO_PATH_RUNTIME_SHARED;
	root.root_id = 92;
	root.root_generation = 7;
	memset(&locator, 0, sizeof(locator));
	locator.segment_id = TEST_TT_SEGMENT;
	locator.xid = TEST_ORIGIN_XID;
	locator.slot_offset = TEST_TT_OFFSET;
	locator.wrap = TEST_ORIGIN_WRAP;

	/* A terminal canonical slot remains authoritative after its allocator
	 * owner entry retires, even while the allocator still uses the segment. */
	UT_ASSERT(cluster_runtime_visibility_physical_locator_sample_held(
		&locator, &admission, &guard, &root, &key, &result, NULL));
	UT_ASSERT_EQ(result.status, CLUSTER_TT_STATUS_COMMITTED);
	UT_ASSERT_EQ(result.commit_scn, test_commit_scn);
	UT_ASSERT(result.authoritative);
	UT_ASSERT_EQ(test_current_owner_calls, 2);
	UT_ASSERT_EQ(test_current_segment_calls, 2);
	UT_ASSERT_EQ(test_candidate_block0_copy_calls, 1);
}

UT_TEST(test_current_member_local_terminal_accepts_clean_formation_epoch_zero)
{
	ClusterTTStatusKey key;
	ClusterTTStatusResult result;

	reset_exact_origin_fixture();
	test_formation_epoch = 0;
	test_native_status = TRANSACTION_STATUS_COMMITTED;
	test_tt_slot.status = TT_SLOT_COMMITTED;
	test_tt_slot.xid = TEST_ORIGIN_XID;
	test_tt_slot.wrap = TEST_ORIGIN_WRAP;
	test_tt_slot.commit_scn = test_commit_scn;
	test_current_segment = TEST_TT_SEGMENT;
	test_current_owner_available = false;
	memset(&key, 0xa5, sizeof(key));
	memset(&result, 0xa5, sizeof(result));

	UT_ASSERT(cluster_runtime_visibility_local_terminal_lookup_exact(
		TEST_ORIGIN_XID, &key, &result));
	UT_ASSERT_EQ(key.origin_node_id, (uint16)cluster_node_id);
	UT_ASSERT_EQ(key.undo_segment_id, (uint16)TEST_TT_SEGMENT);
	UT_ASSERT_EQ(key.tt_slot_id,
		cluster_tt_slot_offset_to_id(TEST_TT_OFFSET));
	UT_ASSERT_EQ(key.cluster_epoch, (uint32)0);
	UT_ASSERT_EQ(key.local_xid, TEST_ORIGIN_XID);
	UT_ASSERT_EQ(result.status, CLUSTER_TT_STATUS_COMMITTED);
	UT_ASSERT_EQ(result.status_epoch, (uint32)0);
	UT_ASSERT_EQ(result.commit_scn, test_commit_scn);
	UT_ASSERT(result.authoritative);
	UT_ASSERT_EQ(test_candidate_acquire_calls, 1);
	UT_ASSERT_EQ(test_candidate_block0_copy_calls, 1);
	UT_ASSERT_EQ(test_candidate_release_calls, 1);
}

UT_TEST(test_current_member_active_on_current_segment_requires_live_owner_index)
{
	ClusterSemanticAdmissionToken admission;
	ClusterUndoBlock0CurrentGuard guard;
	ClusterUndoBlock0ResolvedRoot root;
	ClusterTTSlotPhysicalLocator locator;
	ClusterTTStatusKey key;
	ClusterTTStatusResult result;

	reset_exact_origin_fixture();
	test_native_status = TRANSACTION_STATUS_IN_PROGRESS;
	test_tt_slot.status = TT_SLOT_ACTIVE;
	test_tt_slot.xid = TEST_ORIGIN_XID;
	test_tt_slot.wrap = TEST_ORIGIN_WRAP;
	test_tt_slot.commit_scn = InvalidScn;
	test_twophase_xid = TEST_ORIGIN_XID;
	test_current_segment = TEST_TT_SEGMENT;
	test_current_owner_available = false;
	memset(&admission, 0, sizeof(admission));
	admission.feature_bit = CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1;
	admission.record_generation = 5;
	admission.formation_epoch = test_formation_epoch;
	admission.side = CLUSTER_SEMANTIC_TARGET_SIDE;
	admission.entered = true;
	memset(&guard, 0, sizeof(guard));
	memset(&root, 0, sizeof(root));
	root.intent = CLUSTER_UNDO_PATH_RUNTIME_SHARED;
	root.root_id = 92;
	root.root_generation = 7;
	memset(&locator, 0, sizeof(locator));
	locator.segment_id = TEST_TT_SEGMENT;
	locator.xid = TEST_ORIGIN_XID;
	locator.slot_offset = TEST_TT_OFFSET;
	locator.wrap = TEST_ORIGIN_WRAP;

	UT_ASSERT(!cluster_runtime_visibility_physical_locator_sample_held(
		&locator, &admission, &guard, &root, &key, &result, NULL));
	UT_ASSERT_EQ(result.status, CLUSTER_TT_STATUS_UNKNOWN);
	UT_ASSERT(!result.authoritative);
}

UT_TEST(test_exact_origin_committed_uses_canonical_tt_identity_and_direct_clog)
{
	ClusterTxResolution resolution;
	ClusterTxResolveReason reason = CLUSTER_TX_RESOLVE_PROTOCOL;
	ClusterTxOutcome outcome;

	reset_exact_origin_fixture();
	memset(&resolution, 0xa5, sizeof(resolution));
	outcome = cluster_runtime_visibility_resolve_exact_origin(
		&test_origin_locator, CLUSTER_TX_RESOLVE_VISIBILITY, test_formation_epoch, &resolution,
		&reason);

	UT_ASSERT_EQ(outcome, CLUSTER_TX_COMMITTED);
	UT_ASSERT_EQ(reason, CLUSTER_TX_RESOLVE_NONE);
	UT_ASSERT_EQ(memcmp(&resolution.locator_echo, &test_origin_locator,
							 sizeof(test_origin_locator)),
				 0);
	UT_ASSERT_EQ((int)resolution.top_xid, (int)TEST_ORIGIN_XID);
	UT_ASSERT_EQ(resolution.outcome, CLUSTER_TX_COMMITTED);
	UT_ASSERT_EQ(resolution.proof_kind, CLUSTER_TX_PROOF_ORIGIN_DURABLE_TT_CLOG);
	UT_ASSERT_EQ(resolution.commit_scn, test_commit_scn);
	UT_ASSERT_EQ(resolution.authority.origin_epoch, test_formation_epoch);
	UT_ASSERT_EQ(resolution.authority.live_hwm_lsn, test_flush_lsn);
	UT_ASSERT_EQ(resolution.authority.tt_generation, test_tt_generation);
	UT_ASSERT_EQ(resolution.authority.authority_scn, test_authority_scn);
	UT_ASSERT_EQ(test_undo_read_calls, 1);
	UT_ASSERT_EQ(test_tt_exact_calls, 1);
	UT_ASSERT_EQ(test_tt_snapshot_calls, 0);
	UT_ASSERT_EQ(test_native_status_calls, 1);
	UT_ASSERT_EQ(test_by_xid_scan_calls, 0);
	UT_ASSERT_EQ(test_tt_segment_seen, TEST_TT_SEGMENT);
	UT_ASSERT_EQ(test_tt_slot_seen, TEST_TT_OFFSET);
	UT_ASSERT_EQ((int)test_tt_xid_seen, (int)TEST_ORIGIN_XID);
	UT_ASSERT_EQ(test_tt_wrap_seen, TEST_ORIGIN_WRAP);
}

UT_TEST(test_terminal_census_local_origin_uses_resident_candidate2_and_canonical_upgrade)
{
	ClusterTxResolution resolution;
	ClusterTxResolveReason reason = CLUSTER_TX_RESOLVE_PROTOCOL;
	ClusterSemanticAdmissionToken admission;

	reset_exact_origin_fixture();
	test_origin_locator.tt_wrap = TT_WRAP_INVALID;
	test_origin_record.tt_slot_segment_id = TEST_RECORD_SEGMENT;
	memset(&admission, 0, sizeof(admission));
	admission.feature_bit = CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1;
	admission.record_generation = 5;
	admission.formation_epoch = test_formation_epoch;
	admission.side = CLUSTER_SEMANTIC_TARGET_SIDE;
	admission.entered = true;

	UT_ASSERT_EQ(cluster_runtime_visibility_resolve_exact_origin_admitted(
		&test_origin_locator, CLUSTER_TX_RESOLVE_TERMINAL_CENSUS, &admission,
		&resolution, &reason), CLUSTER_TX_COMMITTED);
	UT_ASSERT_EQ(reason, CLUSTER_TX_RESOLVE_NONE);
	UT_ASSERT_EQ(resolution.locator_echo.tt_wrap, TEST_ORIGIN_WRAP);
	UT_ASSERT_EQ(test_candidate_acquire_calls, 1);
	UT_ASSERT_EQ(test_candidate_exit_hooks_ensure_calls, 1);
	UT_ASSERT_EQ(test_candidate_sample_calls, 2);
	UT_ASSERT_EQ(test_candidate_block0_copy_calls, 1);
	UT_ASSERT_EQ(test_candidate_data_copy_calls, 1);
	UT_ASSERT_EQ(test_candidate_release_calls, 1);
	UT_ASSERT_EQ(test_undo_read_calls, 0);
	UT_ASSERT_EQ(test_tt_exact_calls, 0);
	UT_ASSERT_EQ(test_tt_snapshot_calls, 0);
}

UT_TEST(test_terminal_census_nonresident_cleanout_uses_exact_local_c1b_pair)
{
	ClusterTxResolution resolution;
	ClusterTxResolveReason reason = CLUSTER_TX_RESOLVE_PROTOCOL;
	ClusterSemanticAdmissionToken admission;

	reset_exact_origin_fixture();
	test_origin_locator.tt_wrap = TT_WRAP_INVALID;
	test_origin_locator.itl_kind = ITL_FLAG_NEEDS_CLEANOUT;
	test_local_freshref_pair_exact = true;
	memset(&admission, 0, sizeof(admission));
	admission.feature_bit = CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1;
	admission.record_generation = 5;
	admission.formation_epoch = test_formation_epoch;
	admission.side = CLUSTER_SEMANTIC_TARGET_SIDE;
	admission.entered = true;

	UT_ASSERT_EQ(
		cluster_runtime_visibility_resolve_terminal_census_retained_local_exact(
			&test_origin_locator, test_commit_scn, &admission,
			&resolution, &reason),
		CLUSTER_TX_COMMITTED);
	UT_ASSERT_EQ(reason, CLUSTER_TX_RESOLVE_NONE);
	UT_ASSERT_EQ(resolution.outcome, CLUSTER_TX_COMMITTED);
	UT_ASSERT_EQ(resolution.proof_kind,
				 CLUSTER_TX_PROOF_ORIGIN_DURABLE_TT_CLOG);
	UT_ASSERT_EQ((uint64) resolution.commit_scn,
				 (uint64) test_commit_scn);
	UT_ASSERT_EQ(memcmp(&resolution.locator_echo, &test_origin_locator,
						 sizeof(test_origin_locator)), 0);
	UT_ASSERT_EQ(test_local_freshref_pair_calls, 1);
	UT_ASSERT_EQ(test_candidate_acquire_calls, 0);
	UT_ASSERT_EQ(test_candidate_data_copy_calls, 0);
}

UT_TEST(test_terminal_census_retained_pair_negative_matrix_fails_closed)
{
	ClusterTxResolution resolution;
	ClusterTxResolveReason reason;
	ClusterSemanticAdmissionToken admission;

	reset_exact_origin_fixture();
	test_origin_locator.tt_wrap = TT_WRAP_INVALID;
	test_origin_locator.itl_kind = ITL_FLAG_NEEDS_CLEANOUT;
	memset(&admission, 0, sizeof(admission));
	admission.feature_bit = CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1;
	admission.record_generation = 5;
	admission.formation_epoch = test_formation_epoch;
	admission.side = CLUSTER_SEMANTIC_TARGET_SIDE;
	admission.entered = true;

	/* Exact origin pair denial cannot be converted into a terminal stamp. */
	reason = CLUSTER_TX_RESOLVE_NONE;
	UT_ASSERT_EQ(
		cluster_runtime_visibility_resolve_terminal_census_retained_local_exact(
			&test_origin_locator, test_commit_scn, &admission,
			&resolution, &reason),
		CLUSTER_TX_UNKNOWN);
	UT_ASSERT_EQ(reason, CLUSTER_TX_RESOLVE_AUTHORITY_UNAVAILABLE);

	/* The retained path is closed to ACTIVE and invalid-SCN carriers. */
	test_local_freshref_pair_exact = true;
	test_origin_locator.itl_kind = ITL_FLAG_ACTIVE;
	reason = CLUSTER_TX_RESOLVE_NONE;
	UT_ASSERT_EQ(
		cluster_runtime_visibility_resolve_terminal_census_retained_local_exact(
			&test_origin_locator, test_commit_scn, &admission,
			&resolution, &reason),
		CLUSTER_TX_UNKNOWN);
	test_origin_locator.itl_kind = ITL_FLAG_NEEDS_CLEANOUT;
	UT_ASSERT_EQ(
		cluster_runtime_visibility_resolve_terminal_census_retained_local_exact(
			&test_origin_locator, InvalidScn, &admission,
			&resolution, &reason),
		CLUSTER_TX_UNKNOWN);
	UT_ASSERT_EQ(test_local_freshref_pair_calls, 1);
}

UT_TEST(test_terminal_census_same_owner_cross_segment_is_sequential_and_exact)
{
	ClusterTxResolution resolution;
	ClusterTxResolveReason reason = CLUSTER_TX_RESOLVE_PROTOCOL;
	ClusterSemanticAdmissionToken admission;

	reset_exact_origin_fixture();
	test_origin_locator.tt_wrap = TT_WRAP_INVALID;
	memset(&admission, 0, sizeof(admission));
	admission.feature_bit = CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1;
	admission.record_generation = 5;
	admission.formation_epoch = test_formation_epoch;
	admission.side = CLUSTER_SEMANTIC_TARGET_SIDE;
	admission.entered = true;

	UT_ASSERT_EQ(cluster_runtime_visibility_resolve_exact_origin_admitted(
		&test_origin_locator, CLUSTER_TX_RESOLVE_TERMINAL_CENSUS, &admission,
		&resolution, &reason), CLUSTER_TX_COMMITTED);
	UT_ASSERT_EQ(reason, CLUSTER_TX_RESOLVE_NONE);
	UT_ASSERT_EQ(resolution.locator_echo.tt_wrap, TEST_ORIGIN_WRAP);
	UT_ASSERT_EQ(test_candidate_acquire_calls, 3);
	UT_ASSERT_EQ(test_candidate_acquire_segments[0], TEST_RECORD_SEGMENT);
	UT_ASSERT_EQ(test_candidate_acquire_segments[1], TEST_TT_SEGMENT);
	UT_ASSERT_EQ(test_candidate_acquire_segments[2], TEST_RECORD_SEGMENT);
	UT_ASSERT_EQ(test_candidate_release_calls, 3);
	UT_ASSERT_EQ(test_candidate_held_count, 0);
	UT_ASSERT_EQ(test_candidate_max_held_count, 1);
	UT_ASSERT_EQ(test_candidate_extract_calls, 2);
	UT_ASSERT_EQ(test_candidate_data_copy_calls, 2);
	UT_ASSERT_EQ(test_candidate_block0_copy_calls, 2);
	UT_ASSERT_EQ(test_native_status_calls, 1);
	UT_ASSERT_EQ(test_by_xid_scan_calls, 0);
	UT_ASSERT_EQ(test_regular_admission_recheck_calls, 0);
	UT_ASSERT(test_terminal_census_recheck_calls > 0);
	UT_ASSERT_EQ(test_regular_root_resolve_calls, 0);
	UT_ASSERT(test_terminal_census_root_resolve_calls > 0);
}

UT_TEST(test_visibility_same_owner_cross_segment_is_sequential_and_exact)
{
	ClusterTxResolution resolution;
	ClusterTxResolveReason reason = CLUSTER_TX_RESOLVE_PROTOCOL;
	ClusterSemanticAdmissionToken admission;

	reset_exact_origin_fixture();
	test_origin_locator.tt_wrap = TT_WRAP_INVALID;
	memset(&admission, 0, sizeof(admission));
	admission.feature_bit = CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1;
	admission.record_generation = 5;
	admission.formation_epoch = test_formation_epoch;
	admission.side = CLUSTER_SEMANTIC_TARGET_SIDE;
	admission.entered = true;

	UT_ASSERT_EQ(cluster_runtime_visibility_resolve_exact_origin_admitted(
		&test_origin_locator, CLUSTER_TX_RESOLVE_VISIBILITY, &admission,
		&resolution, &reason), CLUSTER_TX_COMMITTED);
	UT_ASSERT_EQ(reason, CLUSTER_TX_RESOLVE_NONE);
	UT_ASSERT_EQ(resolution.locator_echo.tt_wrap, TEST_ORIGIN_WRAP);
	UT_ASSERT_EQ(test_candidate_acquire_calls, 3);
	UT_ASSERT_EQ(test_candidate_acquire_segments[0], TEST_RECORD_SEGMENT);
	UT_ASSERT_EQ(test_candidate_acquire_segments[1], TEST_TT_SEGMENT);
	UT_ASSERT_EQ(test_candidate_acquire_segments[2], TEST_RECORD_SEGMENT);
	UT_ASSERT_EQ(test_candidate_release_calls, 3);
	UT_ASSERT_EQ(test_candidate_held_count, 0);
	UT_ASSERT_EQ(test_candidate_max_held_count, 1);
	UT_ASSERT_EQ(test_candidate_extract_calls, 2);
	UT_ASSERT_EQ(test_native_status_calls, 1);
	UT_ASSERT(test_regular_admission_recheck_calls > 0);
	UT_ASSERT_EQ(test_terminal_census_recheck_calls, 0);
	UT_ASSERT(test_regular_root_resolve_calls > 0);
	UT_ASSERT_EQ(test_terminal_census_root_resolve_calls, 0);
}

UT_TEST(test_visibility_precommit_committed_slot_with_live_origin_stays_in_progress)
{
	ClusterTxResolution resolution;
	ClusterTxResolveReason reason = CLUSTER_TX_RESOLVE_PROTOCOL;
	ClusterSemanticAdmissionToken admission;

	reset_exact_origin_fixture();
	test_origin_locator.tt_wrap = TT_WRAP_INVALID;
	test_native_status = TRANSACTION_STATUS_IN_PROGRESS;
	test_twophase_xid = TEST_ORIGIN_XID;
	memset(&admission, 0, sizeof(admission));
	admission.feature_bit = CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1;
	admission.record_generation = 5;
	admission.formation_epoch = test_formation_epoch;
	admission.side = CLUSTER_SEMANTIC_TARGET_SIDE;
	admission.entered = true;
	memset(&resolution, 0xa5, sizeof(resolution));

	UT_ASSERT_EQ(cluster_runtime_visibility_resolve_exact_origin_admitted(
		&test_origin_locator, CLUSTER_TX_RESOLVE_VISIBILITY, &admission,
		&resolution, &reason), CLUSTER_TX_IN_PROGRESS);
	UT_ASSERT_EQ(reason, CLUSTER_TX_RESOLVE_NONE);
	UT_ASSERT_EQ(resolution.outcome, CLUSTER_TX_IN_PROGRESS);
	UT_ASSERT_EQ(resolution.proof_kind,
				 CLUSTER_TX_PROOF_ORIGIN_DURABLE_TT_CLOG);
	UT_ASSERT_EQ(resolution.commit_scn, InvalidScn);
	UT_ASSERT_EQ(test_candidate_acquire_calls, 3);
	UT_ASSERT_EQ(test_candidate_release_calls, 3);
	UT_ASSERT_EQ(test_candidate_max_held_count, 1);
	UT_ASSERT_EQ(test_candidate_extract_calls, 2);
	UT_ASSERT_EQ(test_native_status_calls, 2);
}

static void
reset_recycled_abort_fixture(ClusterSemanticAdmissionToken *admission)
{
	reset_exact_origin_fixture();
	test_origin_locator.tt_wrap = TT_WRAP_INVALID;
	test_tt_slot.xid += 384;
	test_tt_slot.wrap++;
	test_native_status = TRANSACTION_STATUS_ABORTED;
	test_native_origin_provable = true;
	memset(admission, 0, sizeof(*admission));
	admission->feature_bit = CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1;
	admission->record_generation = 5;
	admission->formation_epoch = test_formation_epoch;
	admission->side = CLUSTER_SEMANTIC_TARGET_SIDE;
	admission->entered = true;
}

UT_TEST(test_recycled_canonical_abort_rechecks_exact_data_and_origin)
{
	ClusterSemanticAdmissionToken admission;
	ClusterTxResolution resolution;
	ClusterTxResolveReason reason;

	reset_recycled_abort_fixture(&admission);
	UT_ASSERT_EQ(
		cluster_runtime_visibility_resolve_exact_origin_admitted(
			&test_origin_locator, CLUSTER_TX_RESOLVE_VISIBILITY, &admission, &resolution, &reason),
		CLUSTER_TX_ABORTED);
	UT_ASSERT_EQ(reason, CLUSTER_TX_RESOLVE_NONE);
	UT_ASSERT_EQ(resolution.locator_echo.xid, TEST_ORIGIN_XID);
	UT_ASSERT_EQ(resolution.locator_echo.tt_wrap, TEST_ORIGIN_WRAP);
	UT_ASSERT_EQ(resolution.top_xid, TEST_ORIGIN_XID);
	UT_ASSERT_EQ(resolution.commit_scn, InvalidScn);
	UT_ASSERT_EQ(resolution.proof_kind, CLUSTER_TX_PROOF_ORIGIN_DURABLE_TT_CLOG);
	UT_ASSERT_EQ(test_candidate_extract_calls, 2);
	UT_ASSERT_EQ(test_native_status_calls, 2);
	UT_ASSERT_EQ(test_native_fence_lock_calls, 2);
	UT_ASSERT_EQ(test_native_fence_unlock_calls, 2);
	UT_ASSERT_EQ(test_native_fence_depth, 0);
	UT_ASSERT_EQ(test_xact_lock_depth, 0);
	UT_ASSERT_EQ(test_current_owner_calls, 0);
}

UT_TEST(test_recycled_canonical_abort_same_segment)
{
	ClusterSemanticAdmissionToken admission;
	ClusterTxResolution resolution;
	ClusterTxResolveReason reason;

	reset_recycled_abort_fixture(&admission);
	test_origin_record.tt_slot_segment_id = TEST_RECORD_SEGMENT;
	UT_ASSERT_EQ(
		cluster_runtime_visibility_resolve_exact_origin_admitted(
			&test_origin_locator, CLUSTER_TX_RESOLVE_VISIBILITY, &admission, &resolution, &reason),
		CLUSTER_TX_ABORTED);
	UT_ASSERT_EQ(resolution.locator_echo.tt_wrap, TEST_ORIGIN_WRAP);
	UT_ASSERT_EQ(resolution.commit_scn, InvalidScn);
	UT_ASSERT_EQ(test_candidate_acquire_calls, 1);
	UT_ASSERT_EQ(test_native_status_calls, 1);
	UT_ASSERT_EQ(test_native_fence_depth, 0);
}

/* A whole-segment rebirth clears the physical slot; it does not erase the
 * old origin's independently protected literal ABORT or its exact DATA. */
UT_TEST(test_reborn_empty_canonical_preserves_exact_origin_abort)
{
	ClusterSemanticAdmissionToken admission;
	ClusterTxResolution resolution;
	ClusterTxResolveReason reason;

	for (int same_segment = 0; same_segment < 2; same_segment++) {
		reset_recycled_abort_fixture(&admission);
		memset(&test_tt_slot, 0, sizeof(test_tt_slot));
		if (same_segment)
			test_origin_record.tt_slot_segment_id = TEST_RECORD_SEGMENT;
		UT_ASSERT_EQ(cluster_runtime_visibility_resolve_exact_origin_admitted(
						 &test_origin_locator, CLUSTER_TX_RESOLVE_VISIBILITY, &admission,
						 &resolution, &reason),
					 CLUSTER_TX_ABORTED);
		UT_ASSERT_EQ(reason, CLUSTER_TX_RESOLVE_NONE);
		UT_ASSERT_EQ(resolution.locator_echo.xid, TEST_ORIGIN_XID);
		UT_ASSERT_EQ(resolution.locator_echo.tt_wrap, TEST_ORIGIN_WRAP);
		UT_ASSERT_EQ(resolution.top_xid, TEST_ORIGIN_XID);
		UT_ASSERT_EQ(resolution.commit_scn, InvalidScn);
		UT_ASSERT_EQ(resolution.proof_kind, CLUSTER_TX_PROOF_ORIGIN_DURABLE_TT_CLOG);
		UT_ASSERT_EQ(test_native_status_calls, same_segment ? 1 : 2);
		UT_ASSERT_EQ(test_candidate_extract_calls, same_segment ? 1 : 2);
		UT_ASSERT_EQ(test_current_owner_calls, 0);
		UT_ASSERT_EQ(test_native_fence_depth, 0);
		UT_ASSERT_EQ(test_xact_lock_depth, 0);
		UT_ASSERT_EQ(test_candidate_held_count, 0);
	}
}

/* Allocation after a whole-segment rebirth starts its per-slot wrap again.
 * The later occupant's SCN must never become the old transaction's status. */
UT_TEST(test_reborn_allocated_canonical_preserves_exact_origin_abort)
{
	const uint16 wraps[] = {0, TEST_ORIGIN_WRAP - 1, TEST_ORIGIN_WRAP};
	ClusterSemanticAdmissionToken admission;
	ClusterTxResolution resolution;
	ClusterTxResolveReason reason;

	for (unsigned i = 0; i < lengthof(wraps); i++) {
		for (uint8 status = TT_SLOT_ACTIVE; status <= TT_SLOT_RECYCLABLE; status++) {
			reset_recycled_abort_fixture(&admission);
			test_tt_slot.wrap = wraps[i];
			test_tt_slot.status = status;
			test_tt_slot.commit_scn = status == TT_SLOT_COMMITTED ? 9999 : InvalidScn;
			UT_ASSERT_EQ(cluster_runtime_visibility_resolve_exact_origin_admitted(
							 &test_origin_locator, CLUSTER_TX_RESOLVE_VISIBILITY, &admission,
							 &resolution, &reason),
						 CLUSTER_TX_ABORTED);
			UT_ASSERT_EQ(reason, CLUSTER_TX_RESOLVE_NONE);
			UT_ASSERT_EQ(resolution.locator_echo.xid, TEST_ORIGIN_XID);
			UT_ASSERT_EQ(resolution.locator_echo.tt_wrap, TEST_ORIGIN_WRAP);
			UT_ASSERT_EQ(resolution.commit_scn, InvalidScn);
			UT_ASSERT_EQ(test_native_status_calls, 2);
			UT_ASSERT_EQ(test_candidate_extract_calls, 2);
			UT_ASSERT_EQ(test_native_fence_depth, 0);
			UT_ASSERT_EQ(test_xact_lock_depth, 0);
		}
	}
}

static void
reset_reborn_abort_fixture(ClusterSemanticAdmissionToken *admission, int shape)
{
	reset_recycled_abort_fixture(admission);
	if (shape == 0)
		memset(&test_tt_slot, 0, sizeof(test_tt_slot));
	else
		test_tt_slot.wrap = 0;
}

UT_TEST(test_reborn_canonical_nonabort_and_census_refuse)
{
	const XidStatus states[] = {TRANSACTION_STATUS_IN_PROGRESS, TRANSACTION_STATUS_COMMITTED,
							   TRANSACTION_STATUS_SUB_COMMITTED};
	ClusterSemanticAdmissionToken admission;
	ClusterTxResolution resolution;
	ClusterTxResolution zero = {0};
	ClusterTxResolveReason reason;

	for (int shape = 0; shape < 2; shape++) {
		for (unsigned i = 0; i <= lengthof(states); i++) {
			ClusterTxResolveMode mode = i == lengthof(states)
				? CLUSTER_TX_RESOLVE_TERMINAL_CENSUS : CLUSTER_TX_RESOLVE_VISIBILITY;

			reset_reborn_abort_fixture(&admission, shape);
			if (i < lengthof(states))
				test_native_status = states[i];
			memset(&resolution, 0xa5, sizeof(resolution));
			UT_ASSERT_EQ(cluster_runtime_visibility_resolve_exact_origin_admitted(
				&test_origin_locator, mode, &admission, &resolution, &reason), CLUSTER_TX_UNKNOWN);
			UT_ASSERT_EQ(memcmp(&resolution, &zero, sizeof(zero)), 0);
			UT_ASSERT_EQ(test_native_status_calls, i == lengthof(states) ? 0 : 1);
			UT_ASSERT_EQ(test_current_owner_calls, 0);
			UT_ASSERT_EQ(test_twophase_calls, 0);
			UT_ASSERT_EQ(test_subtrans_parent_calls, 0);
			UT_ASSERT_EQ(test_procarray_calls, 0);
			UT_ASSERT_EQ(test_native_fence_depth, 0);
			UT_ASSERT_EQ(test_xact_lock_depth, 0);
			UT_ASSERT_EQ(test_candidate_held_count, 0);
		}
	}
}

UT_TEST(test_reborn_canonical_malformed_or_initial_generation_refuses)
{
	ClusterSemanticAdmissionToken admission;
	ClusterTxResolution resolution;
	ClusterTxResolution zero = {0};
	ClusterTxResolveReason reason;

	/* Even an UNUSED slot's flags, UBA and otherwise unused bytes must be
	 * empty. A shape check may not silently accept a torn reset. */
	for (size_t byte = 0; byte < sizeof(TTSlot) + 4; byte++) {
		reset_reborn_abort_fixture(&admission, 0);
		if (byte < sizeof(TTSlot))
			((unsigned char *)&test_tt_slot)[byte] = 1;
		else {
			if ((byte - sizeof(TTSlot)) / 2 != 0)
				reset_reborn_abort_fixture(&admission, 1);
			test_candidate_tt_generation = byte % 2 == 0 ? 0 : UINT32_MAX;
		}
		memset(&resolution, 0xa5, sizeof(resolution));
		UT_ASSERT_EQ(cluster_runtime_visibility_resolve_exact_origin_admitted(
			&test_origin_locator, CLUSTER_TX_RESOLVE_VISIBILITY, &admission,
			&resolution, &reason), CLUSTER_TX_UNKNOWN);
		UT_ASSERT_EQ(memcmp(&resolution, &zero, sizeof(zero)), 0);
		UT_ASSERT_EQ(test_native_status_calls, 0);
		UT_ASSERT_EQ(test_native_fence_lock_calls, 0);
		UT_ASSERT_EQ(test_candidate_held_count, 0);
	}
}

UT_TEST(test_reborn_canonical_revocation_truncation_and_data_drift_refuse)
{
	ClusterSemanticAdmissionToken admission;
	ClusterTxResolution resolution;
	ClusterTxResolution zero = {0};
	ClusterTxResolveReason reason;

	for (int shape = 0; shape < 2; shape++) {
		for (int failure = 0; failure < 4; failure++) {
			reset_reborn_abort_fixture(&admission, shape);
			switch (failure) {
			case 0:
				test_native_origin_provable = false;
				break;
			case 1:
				test_variable_cache.oldestClogXid = TEST_ORIGIN_XID + 1;
				break;
			case 2:
				test_no_raw_reuse_drift_on_recheck = true;
				break;
			case 3:
				test_candidate_mutate_record_on_recheck = true;
				break;
			}
			memset(&resolution, 0xa5, sizeof(resolution));
			UT_ASSERT_EQ(cluster_runtime_visibility_resolve_exact_origin_admitted(
				&test_origin_locator, CLUSTER_TX_RESOLVE_VISIBILITY, &admission,
				&resolution, &reason), CLUSTER_TX_UNKNOWN);
			UT_ASSERT_EQ(memcmp(&resolution, &zero, sizeof(zero)), 0);
			UT_ASSERT_EQ(test_native_status_calls, failure < 2 ? 0 : 1);
			UT_ASSERT_EQ(test_native_fence_depth, 0);
			UT_ASSERT_EQ(test_xact_lock_depth, 0);
			UT_ASSERT_EQ(test_candidate_held_count, 0);
		}
	}
}

/* Exercise the production continuation used by the LMS adapter, rather
 * than just the synchronous wrapper. Its transport/SCUR seams are fixtures;
 * freeze, canonical classification and final DATA publication are real C. */
UT_TEST(test_reborn_split_plan_rechecks_before_publication)
{
	for (int shape = 0; shape < 2; shape++) {
		for (int failure = 0; failure < 5; failure++) {
			ClusterSemanticAdmissionToken admission;
			ClusterRuntimeVisibilityOriginPlan plan;
			ClusterRuntimeVisibilityCanonicalDiagnostic diagnostic;
			ClusterTxResolution resolution;
			ClusterTxResolution zero = {0};
			ClusterTxResolveReason reason;
			ClusterUndoBlock0CurrentGuard data_guard = {0};
			ClusterUndoBlock0CurrentGuard tt_guard = {0};
			ClusterUndoBlock0ResolvedRoot data_root = {0};
			ClusterUndoBlock0ResolvedRoot tt_root = {0};
			PGAlignedBlock copied;
			PGAlignedBlock expected;

			reset_reborn_abort_fixture(&admission, shape);
			data_root.intent = tt_root.intent = CLUSTER_UNDO_PATH_RUNTIME_SHARED;
			data_root.root_id = 91;
			tt_root.root_id = 92;
			data_root.root_generation = tt_root.root_generation = 7;
			UT_ASSERT_EQ(cluster_runtime_visibility_origin_plan_freeze_data_held(
				&test_origin_locator, CLUSTER_TX_RESOLVE_VISIBILITY, &admission, NULL,
				&data_guard, &data_root, &plan, &resolution, &reason),
				CLUSTER_RUNTIME_VISIBILITY_ORIGIN_NEEDS_CANONICAL);
			UT_ASSERT(cluster_runtime_visibility_origin_plan_sample_canonical_held(
				&plan, CLUSTER_TX_RESOLVE_VISIBILITY, &admission, &tt_guard, &tt_root, &reason));
			UT_ASSERT(cluster_runtime_visibility_origin_plan_canonical_diagnostic(&plan, &diagnostic));
			UT_ASSERT(diagnostic.native_sampled);
			UT_ASSERT_EQ(diagnostic.native_status, TRANSACTION_STATUS_ABORTED);
			UT_ASSERT_EQ(diagnostic.first_failure, CLUSTER_RUNTIME_VISIBILITY_CANONICAL_FAILURE_NONE);
			if (failure == 1)
				test_candidate_mutate_record_on_recheck = true;
			else if (failure == 2)
				test_no_raw_reuse_window = false;
			else if (failure == 3)
				data_root.root_generation++;
			else if (failure == 4)
				test_native_status = TRANSACTION_STATUS_COMMITTED;
			memset(&resolution, 0xa5, sizeof(resolution));
			memset(&copied, 0xa5, sizeof(copied));
			memset(&expected, failure == 0 ? 0x5a : 0xa5, sizeof(expected));
			UT_ASSERT_EQ(cluster_runtime_visibility_origin_plan_copy_data_held(
				&plan, CLUSTER_TX_RESOLVE_VISIBILITY, &admission, &data_guard,
				&data_root, &resolution, copied.data, &reason),
				failure == 0 ? CLUSTER_TX_ABORTED : CLUSTER_TX_UNKNOWN);
			UT_ASSERT_EQ(memcmp(copied.data, expected.data, BLCKSZ), 0);
			if (failure == 0) {
				UT_ASSERT_EQ(reason, CLUSTER_TX_RESOLVE_NONE);
				UT_ASSERT_EQ(resolution.locator_echo.xid, TEST_ORIGIN_XID);
				UT_ASSERT_EQ(resolution.locator_echo.tt_wrap, TEST_ORIGIN_WRAP);
				UT_ASSERT_EQ(resolution.commit_scn, InvalidScn);
				UT_ASSERT_EQ(test_native_status_calls, 2);
			} else
				UT_ASSERT_EQ(memcmp(&resolution, &zero, sizeof(zero)), 0);
			UT_ASSERT_EQ(test_native_fence_depth, 0);
			UT_ASSERT_EQ(test_xact_lock_depth, 0);
		}
	}
}

UT_TEST(test_recycled_canonical_nonabort_never_borrows_successor_status)
{
	const XidStatus states[] = { TRANSACTION_STATUS_IN_PROGRESS, TRANSACTION_STATUS_COMMITTED,
								 TRANSACTION_STATUS_SUB_COMMITTED };
	ClusterSemanticAdmissionToken admission;
	ClusterTxResolution resolution;
	ClusterTxResolution zero = { 0 };
	ClusterTxResolveReason reason;

	for (unsigned i = 0; i < lengthof(states); i++) {
		reset_recycled_abort_fixture(&admission);
		test_native_status = states[i];
		UT_ASSERT_EQ(cluster_runtime_visibility_resolve_exact_origin_admitted(
						 &test_origin_locator, CLUSTER_TX_RESOLVE_VISIBILITY, &admission,
						 &resolution, &reason),
					 CLUSTER_TX_UNKNOWN);
		UT_ASSERT_EQ(memcmp(&resolution, &zero, sizeof(zero)), 0);
		UT_ASSERT_EQ(test_twophase_calls, 0);
		UT_ASSERT_EQ(test_subtrans_parent_calls, 0);
		UT_ASSERT_EQ(test_procarray_calls, 0);
		UT_ASSERT_EQ(test_native_fence_depth, 0);
		UT_ASSERT_EQ(test_xact_lock_depth, 0);
	}
}

UT_TEST(test_recycled_canonical_invalid_successor_never_reads_native)
{
	ClusterSemanticAdmissionToken admission;
	ClusterTxResolution resolution;
	ClusterTxResolveReason reason;

	for (int bad = 0; bad < 9; bad++) {
		reset_recycled_abort_fixture(&admission);
		switch (bad) {
		case 0:
			test_tt_slot.xid = TEST_ORIGIN_XID;
			break;
		case 1:
			test_tt_slot.xid = TEST_ORIGIN_XID - 16;
			break;
		case 2:
			test_tt_slot.xid++;
			break;
		case 3:
			test_tt_slot.wrap = TEST_ORIGIN_WRAP;
			/* A reset wrap without a post-initial canonical generation is
			 * still ineligible; the reborn positive is tested separately. */
			test_candidate_tt_generation = 0;
			break;
		case 4:
			test_tt_slot.wrap = TT_WRAP_INVALID;
			break;
		case 5:
			test_tt_slot.status = TT_SLOT_UNUSED;
			break;
		case 6:
			test_tt_slot.status = TT_SLOT_RECYCLABLE + 1;
			break;
		case 7:
			test_tt_slot.commit_scn = InvalidScn;
			break;
		case 8:
			test_tt_slot.status = TT_SLOT_ABORTED;
			break;
		}
		UT_ASSERT_EQ(cluster_runtime_visibility_resolve_exact_origin_admitted(
						 &test_origin_locator, CLUSTER_TX_RESOLVE_VISIBILITY, &admission,
						 &resolution, &reason),
					 CLUSTER_TX_UNKNOWN);
		UT_ASSERT_EQ(test_native_status_calls, 0);
		UT_ASSERT_EQ(test_native_fence_lock_calls, 0);
	}
}

UT_TEST(test_recycled_canonical_missing_or_revoked_window_refuses)
{
	ClusterSemanticAdmissionToken admission;
	ClusterTxResolution resolution;
	ClusterTxResolution zero = { 0 };
	ClusterTxResolveReason reason;

	for (int revoked = 0; revoked < 2; revoked++) {
		reset_recycled_abort_fixture(&admission);
		test_native_origin_provable = revoked != 0;
		test_no_raw_reuse_drift_on_recheck = revoked != 0;
		UT_ASSERT_EQ(cluster_runtime_visibility_resolve_exact_origin_admitted(
						 &test_origin_locator, CLUSTER_TX_RESOLVE_VISIBILITY, &admission,
						 &resolution, &reason),
					 CLUSTER_TX_UNKNOWN);
		UT_ASSERT_EQ(memcmp(&resolution, &zero, sizeof(zero)), 0);
		UT_ASSERT_EQ(test_native_status_calls, revoked);
		UT_ASSERT_EQ(test_native_fence_depth, 0);
		UT_ASSERT_EQ(test_xact_lock_depth, 0);
	}
}

UT_TEST(test_recycled_canonical_truncation_and_data_drift_refuse)
{
	ClusterSemanticAdmissionToken admission;
	ClusterTxResolution resolution;
	ClusterTxResolution zero = { 0 };
	ClusterTxResolveReason reason;

	for (int drift = 0; drift < 2; drift++) {
		reset_recycled_abort_fixture(&admission);
		if (drift)
			test_candidate_mutate_record_on_recheck = true;
		else
			test_variable_cache.oldestClogXid = TEST_ORIGIN_XID + 1;
		UT_ASSERT_EQ(cluster_runtime_visibility_resolve_exact_origin_admitted(
						 &test_origin_locator, CLUSTER_TX_RESOLVE_VISIBILITY, &admission,
						 &resolution, &reason),
					 CLUSTER_TX_UNKNOWN);
		UT_ASSERT_EQ(memcmp(&resolution, &zero, sizeof(zero)), 0);
		UT_ASSERT_EQ(test_native_status_calls, drift);
		UT_ASSERT_EQ(test_native_fence_depth, 0);
		UT_ASSERT_EQ(test_xact_lock_depth, 0);
	}
}

UT_TEST(test_recycled_canonical_clog_error_rethrows_and_releases_own_locks)
{
	ClusterSemanticAdmissionToken admission;
	ClusterTxResolution resolution;
	ClusterTxResolveReason reason;
	for (int shape = 0; shape < 3; shape++) {
		volatile bool caught = false;

		reset_recycled_abort_fixture(&admission);
		if (shape < 2)
			reset_reborn_abort_fixture(&admission, shape);
		test_native_throw = true;
		PG_TRY();
		{
			(void)cluster_runtime_visibility_resolve_exact_origin_admitted(
				&test_origin_locator, CLUSTER_TX_RESOLVE_VISIBILITY, &admission, &resolution, &reason);
		}
		PG_CATCH();
		{
			caught = true;
		}
		PG_END_TRY();
		UT_ASSERT(caught);
		UT_ASSERT_EQ(test_native_fence_unlock_calls, 1);
		UT_ASSERT_EQ(test_native_fence_depth, 0);
		UT_ASSERT_EQ(test_xact_lock_depth, 0);
	}
}

UT_TEST(test_canonical_sample_reports_first_failed_predicate)
{
	ClusterRuntimeVisibilityOriginPlan plan;
	ClusterRuntimeVisibilityCanonicalDiagnostic diagnostic;
	ClusterTxResolution resolution;
	ClusterTxResolveReason reason = CLUSTER_TX_RESOLVE_PROTOCOL;
	ClusterSemanticAdmissionToken admission;
	ClusterUndoBlock0CurrentGuard data_guard;
	ClusterUndoBlock0CurrentGuard tt_guard;
	ClusterUndoBlock0ResolvedRoot data_root;
	ClusterUndoBlock0ResolvedRoot tt_root;
	ClusterUndoBlock0LogicalKey canonical_logical;

	reset_exact_origin_fixture();
	test_origin_locator.tt_wrap = TT_WRAP_INVALID;
	memset(&admission, 0, sizeof(admission));
	admission.feature_bit = CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1;
	admission.record_generation = 5;
	admission.formation_epoch = test_formation_epoch;
	admission.side = CLUSTER_SEMANTIC_TARGET_SIDE;
	admission.entered = true;
	memset(&data_guard, 0, sizeof(data_guard));
	memset(&tt_guard, 0, sizeof(tt_guard));
	memset(&data_root, 0, sizeof(data_root));
	data_root.intent = CLUSTER_UNDO_PATH_RUNTIME_SHARED;
	data_root.root_id = 91;
	data_root.root_generation = 7;
	memset(&tt_root, 0, sizeof(tt_root));
	tt_root.intent = CLUSTER_UNDO_PATH_RUNTIME_SHARED;
	tt_root.root_id = 92;
	tt_root.root_generation = 7;

	UT_ASSERT_EQ(cluster_runtime_visibility_origin_plan_freeze_data_held(
		&test_origin_locator, CLUSTER_TX_RESOLVE_VISIBILITY, &admission, NULL,
		&data_guard, &data_root, &plan, &resolution, &reason),
		CLUSTER_RUNTIME_VISIBILITY_ORIGIN_NEEDS_CANONICAL);
	UT_ASSERT(cluster_runtime_visibility_origin_plan_canonical_logical(
		&plan, &canonical_logical));
	UT_ASSERT_EQ(canonical_logical.segment_id, TEST_TT_SEGMENT);
	test_candidate_sample_result = CLUSTER_UNDO_BLOCK0_NOT_PUBLISHED;
	UT_ASSERT(!cluster_runtime_visibility_origin_plan_sample_canonical_held(
		&plan, CLUSTER_TX_RESOLVE_VISIBILITY, &admission, &tt_guard, &tt_root,
		&reason));
	UT_ASSERT_EQ(reason, CLUSTER_TX_RESOLVE_AUTHORITY_UNAVAILABLE);
	memset(&diagnostic, 0xa5, sizeof(diagnostic));
	UT_ASSERT(cluster_runtime_visibility_origin_plan_canonical_diagnostic(
		&plan, &diagnostic));
	UT_ASSERT(diagnostic.valid);
	UT_ASSERT_EQ(diagnostic.first_failure,
		CLUSTER_RUNTIME_VISIBILITY_CANONICAL_FAILURE_GENERATION_SAMPLE);
	UT_ASSERT_EQ(diagnostic.generation_result,
		CLUSTER_UNDO_BLOCK0_NOT_PUBLISHED);
	UT_ASSERT(!diagnostic.generation_known);
	UT_ASSERT_EQ(diagnostic.locator_xid, TEST_ORIGIN_XID);
	UT_ASSERT_EQ(diagnostic.locator_wrap, TEST_ORIGIN_WRAP);
	UT_ASSERT_EQ(diagnostic.tt_slot_offset, TEST_TT_OFFSET);
	UT_ASSERT_EQ(diagnostic.root_id, tt_root.root_id);
	UT_ASSERT_EQ(diagnostic.root_generation, tt_root.root_generation);
	UT_ASSERT(diagnostic.initial_admission_current);
	UT_ASSERT_EQ(diagnostic.resident_copy_result, -1);
	UT_ASSERT(!diagnostic.native_sampled);
}

UT_TEST(test_empty_physical_slot_remains_unknown_despite_current_allocator_owner)
{
	ClusterTxResolution resolution;
	ClusterTxResolution zero = { 0 };
	ClusterTxResolveReason reason = CLUSTER_TX_RESOLVE_PROTOCOL;
	ClusterSemanticAdmissionToken admission;

	reset_exact_origin_fixture();
	test_origin_locator.tt_wrap = TT_WRAP_INVALID;
	test_candidate_copy_physical_slot = false;
	test_current_owner_available = true;
	test_native_status = TRANSACTION_STATUS_IN_PROGRESS;
	test_twophase_xid = TEST_ORIGIN_XID;
	memset(&admission, 0, sizeof(admission));
	admission.feature_bit = CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1;
	admission.record_generation = 5;
	admission.formation_epoch = test_formation_epoch;
	admission.side = CLUSTER_SEMANTIC_TARGET_SIDE;
	admission.entered = true;
	memset(&resolution, 0xa5, sizeof(resolution));

	UT_ASSERT_EQ(cluster_runtime_visibility_resolve_exact_origin_admitted(
		&test_origin_locator, CLUSTER_TX_RESOLVE_VISIBILITY, &admission,
		&resolution, &reason), CLUSTER_TX_UNKNOWN);
	UT_ASSERT_EQ(reason, CLUSTER_TX_RESOLVE_AUTHORITY_UNAVAILABLE);
	UT_ASSERT_EQ(memcmp(&resolution, &zero, sizeof(resolution)), 0);
	UT_ASSERT_EQ(test_current_owner_calls, 0);
	UT_ASSERT_EQ(test_native_status_calls, 0);
	UT_ASSERT_EQ(test_candidate_acquire_calls, 2);
	UT_ASSERT_EQ(test_candidate_release_calls, 2);
}

UT_TEST(test_allocator_identity_never_overrides_empty_physical_slot)
{
	ClusterTxResolution resolution;
	ClusterTxResolution zero = { 0 };
	ClusterTxResolveReason reason;
	ClusterSemanticAdmissionToken admission;
	int mismatch;

	for (mismatch = 0; mismatch < 5; mismatch++) {
		reset_exact_origin_fixture();
		test_origin_locator.tt_wrap = TT_WRAP_INVALID;
		test_candidate_copy_physical_slot = false;
		test_current_owner_available = true;
		test_native_status = TRANSACTION_STATUS_IN_PROGRESS;
		test_twophase_xid = TEST_ORIGIN_XID;
		switch (mismatch) {
			case 0:
				test_current_owner.segment_id++;
				break;
			case 1:
				test_current_owner.slot_offset++;
				break;
			case 2:
				test_current_owner.xid++;
				break;
			case 3:
				test_current_owner.wrap++;
				break;
			default:
				test_current_owner.status = CTS_COMMITTED;
				break;
		}
		memset(&admission, 0, sizeof(admission));
		admission.feature_bit = CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1;
		admission.record_generation = 5;
		admission.formation_epoch = test_formation_epoch;
		admission.side = CLUSTER_SEMANTIC_TARGET_SIDE;
		admission.entered = true;
		memset(&resolution, 0xa5, sizeof(resolution));
		reason = CLUSTER_TX_RESOLVE_PROTOCOL;

		UT_ASSERT_EQ(cluster_runtime_visibility_resolve_exact_origin_admitted(
			&test_origin_locator, CLUSTER_TX_RESOLVE_VISIBILITY, &admission,
			&resolution, &reason), CLUSTER_TX_UNKNOWN);
		UT_ASSERT_EQ(memcmp(&resolution, &zero, sizeof(resolution)), 0);
		UT_ASSERT_EQ(reason, CLUSTER_TX_RESOLVE_AUTHORITY_UNAVAILABLE);
		UT_ASSERT_EQ(test_current_owner_calls, 0);
		UT_ASSERT_EQ(test_native_status_calls, 0);
	}

	reset_exact_origin_fixture();
	test_origin_locator.tt_wrap = TT_WRAP_INVALID;
	test_candidate_copy_physical_slot = false;
	test_current_segment = TEST_TT_SEGMENT;
	memset(&admission, 0, sizeof(admission));
	admission.feature_bit = CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1;
	admission.record_generation = 5;
	admission.formation_epoch = test_formation_epoch;
	admission.side = CLUSTER_SEMANTIC_TARGET_SIDE;
	admission.entered = true;
	memset(&resolution, 0xa5, sizeof(resolution));
	reason = CLUSTER_TX_RESOLVE_PROTOCOL;
	UT_ASSERT_EQ(cluster_runtime_visibility_resolve_exact_origin_admitted(
		&test_origin_locator, CLUSTER_TX_RESOLVE_VISIBILITY, &admission,
		&resolution, &reason), CLUSTER_TX_UNKNOWN);
	UT_ASSERT_EQ(memcmp(&resolution, &zero, sizeof(resolution)), 0);
	UT_ASSERT_EQ(reason, CLUSTER_TX_RESOLVE_AUTHORITY_UNAVAILABLE);
	UT_ASSERT_EQ(test_current_owner_calls, 0);
	UT_ASSERT_EQ(test_native_status_calls, 0);

	reset_exact_origin_fixture();
	test_origin_locator.tt_wrap = TT_WRAP_INVALID;
	test_candidate_copy_physical_slot = false;
	test_current_owner_available = true;
	test_native_status = TRANSACTION_STATUS_IN_PROGRESS;
	test_twophase_xid = TEST_ORIGIN_XID;
	test_twophase_prepared = true;
	memset(&admission, 0, sizeof(admission));
	admission.feature_bit = CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1;
	admission.record_generation = 5;
	admission.formation_epoch = test_formation_epoch;
	admission.side = CLUSTER_SEMANTIC_TARGET_SIDE;
	admission.entered = true;
	memset(&resolution, 0xa5, sizeof(resolution));
	reason = CLUSTER_TX_RESOLVE_PROTOCOL;
	UT_ASSERT_EQ(cluster_runtime_visibility_resolve_exact_origin_admitted(
		&test_origin_locator, CLUSTER_TX_RESOLVE_VISIBILITY, &admission,
		&resolution, &reason), CLUSTER_TX_UNKNOWN);
	UT_ASSERT_EQ(reason, CLUSTER_TX_RESOLVE_AUTHORITY_UNAVAILABLE);
	UT_ASSERT_EQ(memcmp(&resolution, &zero, sizeof(resolution)), 0);
	UT_ASSERT_EQ(test_current_owner_calls, 0);
	UT_ASSERT_EQ(test_native_status_calls, 0);
	UT_ASSERT_EQ(test_twophase_calls, 0);

	reset_exact_origin_fixture();
	test_origin_locator.tt_wrap = TT_WRAP_INVALID;
	test_candidate_copy_physical_slot = false;
	test_current_owner_available = true;
	test_current_owner_drift_on_recheck = true;
	test_native_status = TRANSACTION_STATUS_IN_PROGRESS;
	test_twophase_xid = TEST_ORIGIN_XID;
	memset(&admission, 0, sizeof(admission));
	admission.feature_bit = CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1;
	admission.record_generation = 5;
	admission.formation_epoch = test_formation_epoch;
	admission.side = CLUSTER_SEMANTIC_TARGET_SIDE;
	admission.entered = true;
	memset(&resolution, 0xa5, sizeof(resolution));
	reason = CLUSTER_TX_RESOLVE_PROTOCOL;

	UT_ASSERT_EQ(cluster_runtime_visibility_resolve_exact_origin_admitted(
		&test_origin_locator, CLUSTER_TX_RESOLVE_VISIBILITY, &admission,
		&resolution, &reason), CLUSTER_TX_UNKNOWN);
	UT_ASSERT_EQ(reason, CLUSTER_TX_RESOLVE_AUTHORITY_UNAVAILABLE);
	UT_ASSERT_EQ(memcmp(&resolution, &zero, sizeof(resolution)), 0);
	UT_ASSERT_EQ(test_current_owner_calls, 0);
}

UT_TEST(test_empty_physical_slot_remains_unknown_despite_rolled_live_xid)
{
	ClusterTxResolution resolution;
	ClusterTxResolution zero = { 0 };
	ClusterTxResolveReason reason = CLUSTER_TX_RESOLVE_PROTOCOL;
	ClusterSemanticAdmissionToken admission;

	reset_exact_origin_fixture();
	test_origin_locator.tt_wrap = TT_WRAP_INVALID;
	test_candidate_copy_physical_slot = false;
	test_native_status = TRANSACTION_STATUS_IN_PROGRESS;
	test_twophase_xid = TEST_ORIGIN_XID;
	memset(&admission, 0, sizeof(admission));
	admission.feature_bit = CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1;
	admission.record_generation = 5;
	admission.formation_epoch = test_formation_epoch;
	admission.side = CLUSTER_SEMANTIC_TARGET_SIDE;
	admission.entered = true;
	memset(&resolution, 0xa5, sizeof(resolution));

	UT_ASSERT_EQ(cluster_runtime_visibility_resolve_exact_origin_admitted(
		&test_origin_locator, CLUSTER_TX_RESOLVE_VISIBILITY, &admission,
		&resolution, &reason), CLUSTER_TX_UNKNOWN);
	UT_ASSERT_EQ(reason, CLUSTER_TX_RESOLVE_AUTHORITY_UNAVAILABLE);
	UT_ASSERT_EQ(memcmp(&resolution, &zero, sizeof(resolution)), 0);
	UT_ASSERT_EQ(test_current_owner_calls, 0);
	UT_ASSERT_EQ(test_current_segment_calls, 0);
	/* The origin qualification is checked under the drain, but it fails:
	 * neither a native status nor allocator/live-owner evidence is read. */
	UT_ASSERT_EQ(test_native_fence_lock_calls, 1);
	UT_ASSERT_EQ(test_native_fence_unlock_calls, 1);
	UT_ASSERT_EQ(test_native_status_calls, 0);
	UT_ASSERT_EQ(test_native_fence_depth, 0);
	UT_ASSERT_EQ(test_procarray_calls, 0);
	UT_ASSERT_EQ(test_xact_lock_depth, 0);
}

UT_TEST(test_rolled_live_evidence_never_overrides_mismatched_physical_slot)
{
	ClusterTxResolution resolution;
	ClusterTxResolution zero = { 0 };
	ClusterTxResolveReason reason;
	ClusterSemanticAdmissionToken admission;
	int physical_kind;

	for (physical_kind = 0; physical_kind < 3; physical_kind++) {
		reset_exact_origin_fixture();
		test_origin_locator.tt_wrap = TT_WRAP_INVALID;
		test_tt_slot.xid = TEST_ORIGIN_XID + 1;
		test_tt_slot.wrap = TEST_ORIGIN_WRAP + 1;
		test_tt_slot.status = physical_kind == 0
			? TT_SLOT_COMMITTED
			: (physical_kind == 1 ? TT_SLOT_ABORTED : TT_SLOT_ACTIVE);
		test_tt_slot.commit_scn = physical_kind == 0
			? scn_encode(0, 79) : InvalidScn;
		test_twophase_xid = TEST_ORIGIN_XID;
		test_native_status = TRANSACTION_STATUS_IN_PROGRESS;
		memset(&admission, 0, sizeof(admission));
		admission.feature_bit = CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1;
		admission.record_generation = 5;
		admission.formation_epoch = test_formation_epoch;
		admission.side = CLUSTER_SEMANTIC_TARGET_SIDE;
		admission.entered = true;
		memset(&resolution, 0xa5, sizeof(resolution));
		reason = CLUSTER_TX_RESOLVE_PROTOCOL;

		UT_ASSERT_EQ(cluster_runtime_visibility_resolve_exact_origin_admitted(
			&test_origin_locator, CLUSTER_TX_RESOLVE_VISIBILITY, &admission,
			&resolution, &reason), CLUSTER_TX_UNKNOWN);
		UT_ASSERT_EQ(reason, CLUSTER_TX_RESOLVE_AUTHORITY_UNAVAILABLE);
		UT_ASSERT_EQ(memcmp(&resolution, &zero, sizeof(resolution)), 0);
		UT_ASSERT_EQ(test_current_segment_calls, 0);
		UT_ASSERT_EQ(test_procarray_calls, 0);
	}
}

UT_TEST(test_empty_physical_slot_never_consults_rolled_native_axes)
{
	ClusterTxResolution resolution;
	ClusterTxResolution zero = { 0 };
	ClusterTxResolveReason reason;
	ClusterSemanticAdmissionToken admission;
	int mismatch;

	for (mismatch = 0; mismatch < 6; mismatch++) {
		reset_exact_origin_fixture();
		test_origin_locator.tt_wrap = TT_WRAP_INVALID;
		test_candidate_copy_physical_slot = false;
		test_native_status = TRANSACTION_STATUS_IN_PROGRESS;
		test_twophase_xid = TEST_ORIGIN_XID;
		switch (mismatch) {
		case 0:
			test_current_segment = TEST_TT_SEGMENT;
			break;
		case 1:
			test_no_raw_reuse_window = false;
			break;
		case 2:
			test_xid_is_mine = false;
			break;
		case 3:
			test_native_status = TRANSACTION_STATUS_COMMITTED;
			break;
		case 4:
			test_procarray_live = false;
			break;
		default:
			test_twophase_prepared = true;
			break;
		}
		memset(&admission, 0, sizeof(admission));
		admission.feature_bit = CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1;
		admission.record_generation = 5;
		admission.formation_epoch = test_formation_epoch;
		admission.side = CLUSTER_SEMANTIC_TARGET_SIDE;
		admission.entered = true;
		memset(&resolution, 0xa5, sizeof(resolution));
		reason = CLUSTER_TX_RESOLVE_PROTOCOL;

		UT_ASSERT_EQ(cluster_runtime_visibility_resolve_exact_origin_admitted(
			&test_origin_locator, CLUSTER_TX_RESOLVE_VISIBILITY, &admission,
			&resolution, &reason), CLUSTER_TX_UNKNOWN);
		UT_ASSERT_EQ(reason, CLUSTER_TX_RESOLVE_AUTHORITY_UNAVAILABLE);
		UT_ASSERT_EQ(memcmp(&resolution, &zero, sizeof(resolution)), 0);
		UT_ASSERT_EQ(test_current_segment_calls, 0);
		UT_ASSERT_EQ(test_native_fence_lock_calls, 1);
		UT_ASSERT_EQ(test_native_fence_unlock_calls, 1);
		UT_ASSERT_EQ(test_native_status_calls, 0);
		UT_ASSERT_EQ(test_procarray_calls, 0);
	}
}

UT_TEST(test_empty_physical_slot_cannot_reach_rolled_recheck)
{
	ClusterTxResolution resolution;
	ClusterTxResolution zero = { 0 };
	ClusterTxResolveReason reason;
	ClusterSemanticAdmissionToken admission;
	int drift;

	for (drift = 0; drift < 2; drift++) {
		reset_exact_origin_fixture();
		test_origin_locator.tt_wrap = TT_WRAP_INVALID;
		test_candidate_copy_physical_slot = false;
		test_native_status = TRANSACTION_STATUS_IN_PROGRESS;
		test_twophase_xid = TEST_ORIGIN_XID;
		if (drift == 0)
			test_current_segment_drift_on_recheck = true;
		else
			test_no_raw_reuse_drift_on_recheck = true;
		memset(&admission, 0, sizeof(admission));
		admission.feature_bit = CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1;
		admission.record_generation = 5;
		admission.formation_epoch = test_formation_epoch;
		admission.side = CLUSTER_SEMANTIC_TARGET_SIDE;
		admission.entered = true;
		memset(&resolution, 0xa5, sizeof(resolution));
		reason = CLUSTER_TX_RESOLVE_PROTOCOL;

		UT_ASSERT_EQ(cluster_runtime_visibility_resolve_exact_origin_admitted(
			&test_origin_locator, CLUSTER_TX_RESOLVE_VISIBILITY, &admission,
			&resolution, &reason), CLUSTER_TX_UNKNOWN);
		UT_ASSERT_EQ(reason, CLUSTER_TX_RESOLVE_AUTHORITY_UNAVAILABLE);
		UT_ASSERT_EQ(memcmp(&resolution, &zero, sizeof(resolution)), 0);
		UT_ASSERT_EQ(test_current_segment_calls, 0);
		UT_ASSERT_EQ(test_native_fence_lock_calls, 1);
		UT_ASSERT_EQ(test_native_fence_unlock_calls, 1);
		UT_ASSERT_EQ(test_native_status_calls, 0);
		UT_ASSERT_EQ(test_procarray_calls, 0);
	}
}

UT_TEST(test_terminal_census_never_projects_current_active_owner)
{
	ClusterTxResolution resolution;
	ClusterTxResolution zero = { 0 };
	ClusterTxResolveReason reason = CLUSTER_TX_RESOLVE_PROTOCOL;
	ClusterSemanticAdmissionToken admission;

	reset_exact_origin_fixture();
	test_origin_locator.tt_wrap = TT_WRAP_INVALID;
	test_candidate_copy_physical_slot = false;
	test_current_owner_available = true;
	test_current_segment = TEST_TT_SEGMENT + 1;
	test_no_raw_reuse_window = true;
	test_procarray_live = true;
	test_native_status = TRANSACTION_STATUS_IN_PROGRESS;
	test_twophase_xid = TEST_ORIGIN_XID;
	memset(&admission, 0, sizeof(admission));
	admission.feature_bit = CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1;
	admission.record_generation = 5;
	admission.formation_epoch = test_formation_epoch;
	admission.side = CLUSTER_SEMANTIC_TARGET_SIDE;
	admission.entered = true;
	memset(&resolution, 0xa5, sizeof(resolution));

	UT_ASSERT_EQ(cluster_runtime_visibility_resolve_exact_origin_admitted(
		&test_origin_locator, CLUSTER_TX_RESOLVE_TERMINAL_CENSUS, &admission,
		&resolution, &reason), CLUSTER_TX_UNKNOWN);
	UT_ASSERT_EQ(reason, CLUSTER_TX_RESOLVE_AUTHORITY_UNAVAILABLE);
	UT_ASSERT_EQ(memcmp(&resolution, &zero, sizeof(resolution)), 0);
	UT_ASSERT_EQ(test_current_owner_calls, 0);
	UT_ASSERT_EQ(test_current_segment_calls, 0);
	UT_ASSERT_EQ(test_native_fence_lock_calls, 0);
	UT_ASSERT_EQ(test_procarray_calls, 0);
	UT_ASSERT_EQ(test_native_status_calls, 0);
}

UT_TEST(test_terminal_census_rejects_precommit_committed_slot_with_live_origin)
{
	ClusterTxResolution resolution;
	ClusterTxResolution zero = { 0 };
	ClusterTxResolveReason reason = CLUSTER_TX_RESOLVE_PROTOCOL;
	ClusterSemanticAdmissionToken admission;

	reset_exact_origin_fixture();
	test_origin_locator.tt_wrap = TT_WRAP_INVALID;
	test_native_status = TRANSACTION_STATUS_IN_PROGRESS;
	test_twophase_xid = TEST_ORIGIN_XID;
	memset(&admission, 0, sizeof(admission));
	admission.feature_bit = CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1;
	admission.record_generation = 5;
	admission.formation_epoch = test_formation_epoch;
	admission.side = CLUSTER_SEMANTIC_TARGET_SIDE;
	admission.entered = true;
	memset(&resolution, 0xa5, sizeof(resolution));

	UT_ASSERT_EQ(cluster_runtime_visibility_resolve_exact_origin_admitted(
		&test_origin_locator, CLUSTER_TX_RESOLVE_TERMINAL_CENSUS, &admission,
		&resolution, &reason), CLUSTER_TX_UNKNOWN);
	UT_ASSERT_EQ(reason, CLUSTER_TX_RESOLVE_AUTHORITY_UNAVAILABLE);
	UT_ASSERT_EQ(memcmp(&resolution, &zero, sizeof(resolution)), 0);
	UT_ASSERT_EQ(test_candidate_acquire_calls, 2);
	UT_ASSERT_EQ(test_candidate_release_calls, 2);
	UT_ASSERT_EQ(test_candidate_max_held_count, 1);
	UT_ASSERT_EQ(test_native_status_calls, 2);
}

UT_TEST(test_terminal_census_cross_segment_data_drift_fails_closed)
{
	ClusterTxResolution resolution;
	ClusterTxResolution zero = { 0 };
	ClusterTxResolveReason reason = CLUSTER_TX_RESOLVE_PROTOCOL;
	ClusterSemanticAdmissionToken admission;

	reset_exact_origin_fixture();
	test_origin_locator.tt_wrap = TT_WRAP_INVALID;
	test_candidate_mutate_record_on_recheck = true;
	memset(&admission, 0, sizeof(admission));
	admission.feature_bit = CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1;
	admission.record_generation = 5;
	admission.formation_epoch = test_formation_epoch;
	admission.side = CLUSTER_SEMANTIC_TARGET_SIDE;
	admission.entered = true;
	memset(&resolution, 0xa5, sizeof(resolution));

	UT_ASSERT_EQ(cluster_runtime_visibility_resolve_exact_origin_admitted(
		&test_origin_locator, CLUSTER_TX_RESOLVE_TERMINAL_CENSUS, &admission,
		&resolution, &reason), CLUSTER_TX_UNKNOWN);
	UT_ASSERT_EQ(reason, CLUSTER_TX_RESOLVE_AUTHORITY_STALE);
	UT_ASSERT_EQ(memcmp(&resolution, &zero, sizeof(zero)), 0);
	UT_ASSERT_EQ(test_candidate_acquire_calls, 3);
	UT_ASSERT_EQ(test_candidate_release_calls, 3);
	UT_ASSERT_EQ(test_candidate_max_held_count, 1);
	UT_ASSERT_EQ(test_candidate_extract_calls, 2);
	UT_ASSERT_EQ(test_native_status_calls, 1);
}

UT_TEST(test_terminal_census_cross_owner_tt_alias_fails_closed)
{
	ClusterTxResolution resolution;
	ClusterTxResolution zero = { 0 };
	ClusterTxResolveReason reason = CLUSTER_TX_RESOLVE_PROTOCOL;
	ClusterSemanticAdmissionToken admission;

	reset_exact_origin_fixture();
	test_origin_locator.tt_wrap = TT_WRAP_INVALID;
	/* Segment 257 belongs to owner instance 2, while the DATA UBA belongs
	 * to owner instance 1.  It must not trigger a second current acquire. */
	test_origin_record.tt_slot_segment_id = 257;
	memset(&admission, 0, sizeof(admission));
	admission.feature_bit = CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1;
	admission.record_generation = 5;
	admission.formation_epoch = test_formation_epoch;
	admission.side = CLUSTER_SEMANTIC_TARGET_SIDE;
	admission.entered = true;
	memset(&resolution, 0xa5, sizeof(resolution));

	UT_ASSERT_EQ(cluster_runtime_visibility_resolve_exact_origin_admitted(
		&test_origin_locator, CLUSTER_TX_RESOLVE_TERMINAL_CENSUS, &admission,
		&resolution, &reason), CLUSTER_TX_UNKNOWN);
	UT_ASSERT_EQ(reason, CLUSTER_TX_RESOLVE_AUTHORITY_UNAVAILABLE);
	UT_ASSERT_EQ(memcmp(&resolution, &zero, sizeof(zero)), 0);
	UT_ASSERT_EQ(test_candidate_acquire_calls, 1);
	UT_ASSERT_EQ(test_candidate_release_calls, 1);
	UT_ASSERT_EQ(test_candidate_max_held_count, 1);
	UT_ASSERT_EQ(test_candidate_extract_calls, 1);
	UT_ASSERT_EQ(test_native_status_calls, 0);
}

UT_TEST(test_terminal_census_pending_acquire_failure_cancels_candidate_guard)
{
	ClusterTxResolution resolution;
	ClusterTxResolution zero = { 0 };
	ClusterTxResolveReason reason = CLUSTER_TX_RESOLVE_PROTOCOL;
	ClusterSemanticAdmissionToken admission;

	reset_exact_origin_fixture();
	test_origin_locator.tt_wrap = TT_WRAP_INVALID;
	test_origin_record.tt_slot_segment_id = TEST_RECORD_SEGMENT;
	test_candidate_acquire_step = CLUSTER_UNDO_BLOCK0_CURRENT_PENDING;
	test_candidate_poll_step = CLUSTER_UNDO_BLOCK0_CURRENT_PENDING;
	memset(&admission, 0, sizeof(admission));
	admission.feature_bit = CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1;
	admission.record_generation = 5;
	admission.formation_epoch = test_formation_epoch;
	admission.side = CLUSTER_SEMANTIC_TARGET_SIDE;
	admission.entered = true;
	memset(&resolution, 0xa5, sizeof(resolution));

	UT_ASSERT_EQ(cluster_runtime_visibility_resolve_exact_origin_admitted(
		&test_origin_locator, CLUSTER_TX_RESOLVE_TERMINAL_CENSUS,
		&admission, &resolution, &reason), CLUSTER_TX_UNKNOWN);
	UT_ASSERT_EQ(reason, CLUSTER_TX_RESOLVE_AUTHORITY_UNAVAILABLE);
	UT_ASSERT_EQ(memcmp(&resolution, &zero, sizeof(zero)), 0);
	UT_ASSERT_EQ(test_candidate_acquire_calls, 1);
	UT_ASSERT_EQ(test_candidate_poll_calls, 2);
	UT_ASSERT_EQ(test_candidate_wait_calls, 1);
	UT_ASSERT_EQ(test_candidate_cancel_calls, 1);
	UT_ASSERT_EQ(test_candidate_sample_calls, 0);
	UT_ASSERT_EQ(test_candidate_release_calls, 0);
	UT_ASSERT_EQ(test_vis_evidence[CLUSTER_VIS_METRIC_TRANSIENT_PENDING], 2);
	UT_ASSERT_EQ(test_vis_evidence[CLUSTER_VIS_METRIC_PREMATURE_EXIT], 0);
}

UT_TEST(test_terminal_census_pending_wakes_repoll_until_exact_terminal)
{
	ClusterTxResolution resolution;
	ClusterTxResolveReason reason = CLUSTER_TX_RESOLVE_PROTOCOL;
	ClusterSemanticAdmissionToken admission = { 0 };

	reset_exact_origin_fixture();
	test_origin_locator.tt_wrap = TT_WRAP_INVALID;
	test_origin_record.tt_slot_segment_id = TEST_RECORD_SEGMENT;
	test_candidate_acquire_step = CLUSTER_UNDO_BLOCK0_CURRENT_PENDING;
	test_candidate_poll_step = CLUSTER_UNDO_BLOCK0_CURRENT_PENDING;
	test_candidate_wakes_before_terminal = 3;
	test_candidate_after_wait_step = CLUSTER_UNDO_BLOCK0_CURRENT_HELD;
	admission.feature_bit = CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1;
	admission.record_generation = 5;
	admission.formation_epoch = test_formation_epoch;
	admission.side = CLUSTER_SEMANTIC_TARGET_SIDE;
	admission.entered = true;

	UT_ASSERT_EQ(cluster_runtime_visibility_resolve_exact_origin_admitted(
					 &test_origin_locator, CLUSTER_TX_RESOLVE_TERMINAL_CENSUS, &admission,
					 &resolution, &reason),
				 CLUSTER_TX_COMMITTED);
	UT_ASSERT_EQ(reason, CLUSTER_TX_RESOLVE_NONE);
	UT_ASSERT_EQ(resolution.commit_scn, test_commit_scn);
	UT_ASSERT_EQ(resolution.locator_echo.xid, TEST_ORIGIN_XID);
	UT_ASSERT_EQ(resolution.locator_echo.tt_wrap, TEST_ORIGIN_WRAP);
	UT_ASSERT_EQ(test_candidate_acquire_calls, 1);
	UT_ASSERT_EQ(test_candidate_poll_calls, 4);
	UT_ASSERT_EQ(test_candidate_wait_calls, 3);
	UT_ASSERT_EQ(test_candidate_max_held_count, 1);
	UT_ASSERT_EQ(test_candidate_held_count, 0);
	UT_ASSERT_EQ(test_candidate_release_calls, 1);
	UT_ASSERT_EQ(test_candidate_cancel_calls, 0);
	UT_ASSERT_EQ(test_vis_evidence[CLUSTER_VIS_METRIC_TRANSIENT_PENDING], 4);
	UT_ASSERT_EQ(test_vis_evidence[CLUSTER_VIS_METRIC_PREMATURE_EXIT], 0);
	UT_ASSERT_EQ(test_vis_evidence[CLUSTER_VIS_METRIC_AUTHORITY_TIMEOUT], 0);
}

UT_TEST(test_exact_origin_bad_record_wrap_fails_before_tt_or_clog)
{
	ClusterTxResolution resolution;
	ClusterTxResolution zero = {0};
	ClusterTxResolveReason reason = CLUSTER_TX_RESOLVE_PROTOCOL;

	reset_exact_origin_fixture();
	test_origin_record.tt_wrap_plus1++;
	memset(&resolution, 0xa5, sizeof(resolution));

	UT_ASSERT_EQ(cluster_runtime_visibility_resolve_exact_origin(
					 &test_origin_locator, CLUSTER_TX_RESOLVE_VISIBILITY, test_formation_epoch,
					 &resolution, &reason),
				 CLUSTER_TX_UNKNOWN);
	UT_ASSERT_EQ(reason, CLUSTER_TX_RESOLVE_WRAP_MISMATCH);
	UT_ASSERT_EQ(memcmp(&resolution, &zero, sizeof(resolution)), 0);
	UT_ASSERT_EQ(test_undo_read_calls, 1);
	UT_ASSERT_EQ(test_tt_exact_calls, 0);
	UT_ASSERT_EQ(test_tt_snapshot_calls, 0);
	UT_ASSERT_EQ(test_native_status_calls, 0);
	UT_ASSERT_EQ(test_by_xid_scan_calls, 0);
}

UT_TEST(test_exact_origin_aborted_uses_exact_tt_and_direct_clog)
{
	ClusterTxResolution resolution;
	ClusterTxResolveReason reason = CLUSTER_TX_RESOLVE_PROTOCOL;

	reset_exact_origin_fixture();
	test_tt_slot.status = TT_SLOT_ABORTED;
	test_tt_slot.commit_scn = InvalidScn;
	test_native_status = TRANSACTION_STATUS_ABORTED;
	memset(&resolution, 0xa5, sizeof(resolution));

	UT_ASSERT_EQ(cluster_runtime_visibility_resolve_exact_origin(
					 &test_origin_locator, CLUSTER_TX_RESOLVE_VISIBILITY, test_formation_epoch,
					 &resolution, &reason),
				 CLUSTER_TX_ABORTED);
	UT_ASSERT_EQ(reason, CLUSTER_TX_RESOLVE_NONE);
	UT_ASSERT_EQ(resolution.outcome, CLUSTER_TX_ABORTED);
	UT_ASSERT_EQ(resolution.proof_kind, CLUSTER_TX_PROOF_ORIGIN_DURABLE_TT_CLOG);
	UT_ASSERT_EQ(resolution.commit_scn, InvalidScn);
	UT_ASSERT_EQ(memcmp(&resolution.locator_echo, &test_origin_locator,
							 sizeof(test_origin_locator)),
				 0);
	UT_ASSERT_EQ(test_undo_read_calls, 1);
	UT_ASSERT_EQ(test_tt_exact_calls, 1);
	UT_ASSERT_EQ(test_tt_snapshot_calls, 1);
	UT_ASSERT_EQ(test_native_status_calls, 1);
	UT_ASSERT_EQ(test_by_xid_scan_calls, 0);
	UT_ASSERT_EQ(test_tt_segment_seen, TEST_TT_SEGMENT);
	UT_ASSERT_EQ(test_tt_slot_seen, TEST_TT_OFFSET);
}

UT_TEST(test_exact_origin_conflicting_terminal_evidence_fails_closed)
{
	ClusterTxResolution resolution;
	ClusterTxResolution zero = {0};
	ClusterTxResolveReason reason = CLUSTER_TX_RESOLVE_PROTOCOL;

	reset_exact_origin_fixture();
	test_native_status = TRANSACTION_STATUS_ABORTED;
	memset(&resolution, 0xa5, sizeof(resolution));

	UT_ASSERT_EQ(cluster_runtime_visibility_resolve_exact_origin(
					 &test_origin_locator, CLUSTER_TX_RESOLVE_VISIBILITY, test_formation_epoch,
					 &resolution, &reason),
				 CLUSTER_TX_UNKNOWN);
	UT_ASSERT_EQ(reason, CLUSTER_TX_RESOLVE_AUTHORITY_CONFLICT);
	UT_ASSERT_EQ(memcmp(&resolution, &zero, sizeof(resolution)), 0);
	UT_ASSERT_EQ(test_undo_read_calls, 1);
	UT_ASSERT_EQ(test_tt_exact_calls, 1);
	UT_ASSERT_EQ(test_tt_snapshot_calls, 1);
	UT_ASSERT_EQ(test_native_status_calls, 2);
	UT_ASSERT_EQ(test_by_xid_scan_calls, 0);
}

UT_TEST(test_exact_origin_active_and_native_in_progress_stays_live)
{
	ClusterTxResolution resolution;
	ClusterTxResolveReason reason = CLUSTER_TX_RESOLVE_PROTOCOL;

	reset_exact_origin_fixture();
	test_tt_slot.status = TT_SLOT_ACTIVE;
	test_tt_slot.commit_scn = InvalidScn;
	test_native_status = TRANSACTION_STATUS_IN_PROGRESS;
	test_twophase_xid = TEST_ORIGIN_XID;
	memset(&resolution, 0xa5, sizeof(resolution));

	UT_ASSERT_EQ(cluster_runtime_visibility_resolve_exact_origin(
					 &test_origin_locator, CLUSTER_TX_RESOLVE_ROW_WAIT, test_formation_epoch,
					 &resolution, &reason),
				 CLUSTER_TX_IN_PROGRESS);
	UT_ASSERT_EQ(reason, CLUSTER_TX_RESOLVE_NONE);
	UT_ASSERT_EQ(resolution.outcome, CLUSTER_TX_IN_PROGRESS);
	UT_ASSERT_EQ(resolution.proof_kind, CLUSTER_TX_PROOF_ORIGIN_DURABLE_TT_CLOG);
	UT_ASSERT_EQ(resolution.commit_scn, InvalidScn);
	UT_ASSERT_EQ(resolution.horizon_scn, InvalidScn);
	UT_ASSERT_EQ(memcmp(&resolution.locator_echo, &test_origin_locator,
							 sizeof(test_origin_locator)),
				 0);
	UT_ASSERT_EQ(test_undo_read_calls, 1);
	UT_ASSERT_EQ(test_tt_exact_calls, 1);
	UT_ASSERT_EQ(test_tt_snapshot_calls, 1);
	UT_ASSERT_EQ(test_native_status_calls, 2);
	UT_ASSERT_EQ(test_twophase_calls, 1);
	UT_ASSERT_EQ(test_by_xid_scan_calls, 0);
}

static void
set_native_status_sample(int index, TransactionId xid, XidStatus status)
{
	UT_ASSERT(index >= 0 && index < (int)lengthof(test_native_script_xids));
	test_native_script_xids[index] = xid;
	test_native_script_statuses[index] = status;
	if (test_native_script_count <= index)
		test_native_script_count = index + 1;
}

static void
set_subtrans_chain3(TransactionId child, TransactionId parent, TransactionId top)
{
	test_subtrans_chain[0] = child;
	test_subtrans_chain[1] = parent;
	test_subtrans_chain[2] = top;
	test_subtrans_chain_count = 3;
}

UT_TEST(test_exact_origin_prepared_is_distinct_live_outcome)
{
	ClusterTxResolution resolution;
	ClusterTxResolveReason reason = CLUSTER_TX_RESOLVE_PROTOCOL;

	reset_exact_origin_fixture();
	test_tt_slot.status = TT_SLOT_ACTIVE;
	test_tt_slot.commit_scn = InvalidScn;
	set_native_status_sample(0, TEST_ORIGIN_XID, TRANSACTION_STATUS_IN_PROGRESS);
	set_native_status_sample(1, TEST_ORIGIN_XID, TRANSACTION_STATUS_IN_PROGRESS);
	test_twophase_prepared = true;
	test_twophase_xid = TEST_ORIGIN_XID;
	memset(&resolution, 0xa5, sizeof(resolution));

	UT_ASSERT_EQ(cluster_runtime_visibility_resolve_exact_origin(
					 &test_origin_locator, CLUSTER_TX_RESOLVE_ROW_WAIT, test_formation_epoch,
					 &resolution, &reason),
				 CLUSTER_TX_PREPARED);
	UT_ASSERT_EQ(reason, CLUSTER_TX_RESOLVE_NONE);
	UT_ASSERT_EQ(resolution.outcome, CLUSTER_TX_PREPARED);
	UT_ASSERT_EQ(resolution.proof_kind, CLUSTER_TX_PROOF_ORIGIN_TWOPHASE);
	UT_ASSERT_EQ((int)resolution.top_xid, (int)TEST_ORIGIN_XID);
	UT_ASSERT_EQ(test_native_status_calls, 2);
	UT_ASSERT_EQ(test_twophase_calls, 1);
}

UT_TEST(test_exact_origin_prepared_finish_abort_terminal_wins)
{
	ClusterTxResolution resolution;
	ClusterTxResolveReason reason = CLUSTER_TX_RESOLVE_PROTOCOL;

	reset_exact_origin_fixture();
	test_tt_slot.status = TT_SLOT_ACTIVE;
	test_tt_slot.commit_scn = InvalidScn;
	set_native_status_sample(0, TEST_ORIGIN_XID, TRANSACTION_STATUS_IN_PROGRESS);
	set_native_status_sample(1, TEST_ORIGIN_XID, TRANSACTION_STATUS_ABORTED);
	test_twophase_prepared = true;
	test_twophase_xid = TEST_ORIGIN_XID;
	memset(&resolution, 0xa5, sizeof(resolution));

	UT_ASSERT_EQ(cluster_runtime_visibility_resolve_exact_origin(
					 &test_origin_locator, CLUSTER_TX_RESOLVE_ROW_WAIT, test_formation_epoch,
					 &resolution, &reason),
				 CLUSTER_TX_ABORTED);
	UT_ASSERT_EQ(reason, CLUSTER_TX_RESOLVE_NONE);
	UT_ASSERT_EQ(resolution.outcome, CLUSTER_TX_ABORTED);
	UT_ASSERT_EQ(resolution.proof_kind, CLUSTER_TX_PROOF_ORIGIN_DURABLE_TT_CLOG);
	UT_ASSERT_EQ(test_native_status_calls, 2);
	UT_ASSERT_EQ(test_twophase_calls, 1);
}

UT_TEST(test_exact_origin_prepared_finish_commit_without_exact_scn_fails_closed)
{
	ClusterTxResolution resolution;
	ClusterTxResolution zero = {0};
	ClusterTxResolveReason reason = CLUSTER_TX_RESOLVE_PROTOCOL;

	reset_exact_origin_fixture();
	test_tt_slot.status = TT_SLOT_ACTIVE;
	test_tt_slot.commit_scn = InvalidScn;
	set_native_status_sample(0, TEST_ORIGIN_XID, TRANSACTION_STATUS_IN_PROGRESS);
	set_native_status_sample(1, TEST_ORIGIN_XID, TRANSACTION_STATUS_COMMITTED);
	test_twophase_prepared = true;
	test_twophase_xid = TEST_ORIGIN_XID;
	memset(&resolution, 0xa5, sizeof(resolution));

	UT_ASSERT_EQ(cluster_runtime_visibility_resolve_exact_origin(
					 &test_origin_locator, CLUSTER_TX_RESOLVE_ROW_WAIT, test_formation_epoch,
					 &resolution, &reason),
				 CLUSTER_TX_UNKNOWN);
	UT_ASSERT_EQ(reason, CLUSTER_TX_RESOLVE_COVERAGE_GAP);
	UT_ASSERT_EQ(memcmp(&resolution, &zero, sizeof(resolution)), 0);
	UT_ASSERT_EQ(test_native_status_calls, 2);
	UT_ASSERT_EQ(test_twophase_calls, 1);
}

UT_TEST(test_exact_origin_tt_aborted_does_not_beat_direct_prepared_owner)
{
	ClusterTxResolution resolution;
	ClusterTxResolveReason reason = CLUSTER_TX_RESOLVE_PROTOCOL;

	reset_exact_origin_fixture();
	test_tt_slot.status = TT_SLOT_ABORTED;
	test_tt_slot.commit_scn = InvalidScn;
	set_native_status_sample(0, TEST_ORIGIN_XID, TRANSACTION_STATUS_IN_PROGRESS);
	set_native_status_sample(1, TEST_ORIGIN_XID, TRANSACTION_STATUS_IN_PROGRESS);
	test_twophase_prepared = true;
	test_twophase_xid = TEST_ORIGIN_XID;
	memset(&resolution, 0xa5, sizeof(resolution));

	UT_ASSERT_EQ(cluster_runtime_visibility_resolve_exact_origin(
					 &test_origin_locator, CLUSTER_TX_RESOLVE_ROW_WAIT, test_formation_epoch,
					 &resolution, &reason),
				 CLUSTER_TX_PREPARED);
	UT_ASSERT_EQ(reason, CLUSTER_TX_RESOLVE_NONE);
	UT_ASSERT_EQ(resolution.outcome, CLUSTER_TX_PREPARED);
	UT_ASSERT_EQ(resolution.proof_kind, CLUSTER_TX_PROOF_ORIGIN_TWOPHASE);
	UT_ASSERT_EQ(test_native_status_calls, 2);
	UT_ASSERT_EQ(test_twophase_calls, 1);
}

UT_TEST(test_exact_origin_tt_aborted_without_direct_native_terminal_fails_closed)
{
	ClusterTxResolution resolution;
	ClusterTxResolution zero = {0};
	ClusterTxResolveReason reason = CLUSTER_TX_RESOLVE_PROTOCOL;

	reset_exact_origin_fixture();
	test_tt_slot.status = TT_SLOT_ABORTED;
	test_tt_slot.commit_scn = InvalidScn;
	set_native_status_sample(0, TEST_ORIGIN_XID, TRANSACTION_STATUS_IN_PROGRESS);
	set_native_status_sample(1, TEST_ORIGIN_XID, TRANSACTION_STATUS_IN_PROGRESS);
	test_twophase_xid = TEST_ORIGIN_XID;
	memset(&resolution, 0xa5, sizeof(resolution));

	UT_ASSERT_EQ(cluster_runtime_visibility_resolve_exact_origin(
					 &test_origin_locator, CLUSTER_TX_RESOLVE_ROW_WAIT, test_formation_epoch,
					 &resolution, &reason),
				 CLUSTER_TX_UNKNOWN);
	UT_ASSERT_EQ(reason, CLUSTER_TX_RESOLVE_AUTHORITY_UNAVAILABLE);
	UT_ASSERT_EQ(memcmp(&resolution, &zero, sizeof(resolution)), 0);
	UT_ASSERT_EQ(test_native_status_calls, 2);
	UT_ASSERT_EQ(test_twophase_calls, 1);
}

UT_TEST(test_exact_origin_tt_aborted_direct_second_clog_abort_is_terminal)
{
	ClusterTxResolution resolution;
	ClusterTxResolveReason reason = CLUSTER_TX_RESOLVE_PROTOCOL;

	reset_exact_origin_fixture();
	test_tt_slot.status = TT_SLOT_ABORTED;
	test_tt_slot.commit_scn = InvalidScn;
	set_native_status_sample(0, TEST_ORIGIN_XID, TRANSACTION_STATUS_IN_PROGRESS);
	set_native_status_sample(1, TEST_ORIGIN_XID, TRANSACTION_STATUS_ABORTED);
	test_twophase_prepared = true;
	test_twophase_xid = TEST_ORIGIN_XID;
	memset(&resolution, 0xa5, sizeof(resolution));

	UT_ASSERT_EQ(cluster_runtime_visibility_resolve_exact_origin(
					 &test_origin_locator, CLUSTER_TX_RESOLVE_ROW_WAIT, test_formation_epoch,
					 &resolution, &reason),
				 CLUSTER_TX_ABORTED);
	UT_ASSERT_EQ(reason, CLUSTER_TX_RESOLVE_NONE);
	UT_ASSERT_EQ(resolution.outcome, CLUSTER_TX_ABORTED);
	UT_ASSERT_EQ(test_native_status_calls, 2);
	UT_ASSERT_EQ(test_twophase_calls, 1);
}

UT_TEST(test_exact_origin_tt_aborted_direct_second_clog_commit_conflicts)
{
	ClusterTxResolution resolution;
	ClusterTxResolution zero = {0};
	ClusterTxResolveReason reason = CLUSTER_TX_RESOLVE_PROTOCOL;

	reset_exact_origin_fixture();
	test_tt_slot.status = TT_SLOT_ABORTED;
	test_tt_slot.commit_scn = InvalidScn;
	set_native_status_sample(0, TEST_ORIGIN_XID, TRANSACTION_STATUS_IN_PROGRESS);
	set_native_status_sample(1, TEST_ORIGIN_XID, TRANSACTION_STATUS_COMMITTED);
	test_twophase_xid = TEST_ORIGIN_XID;
	memset(&resolution, 0xa5, sizeof(resolution));

	UT_ASSERT_EQ(cluster_runtime_visibility_resolve_exact_origin(
					 &test_origin_locator, CLUSTER_TX_RESOLVE_ROW_WAIT, test_formation_epoch,
					 &resolution, &reason),
				 CLUSTER_TX_UNKNOWN);
	UT_ASSERT_EQ(reason, CLUSTER_TX_RESOLVE_AUTHORITY_CONFLICT);
	UT_ASSERT_EQ(memcmp(&resolution, &zero, sizeof(resolution)), 0);
	UT_ASSERT_EQ(test_native_status_calls, 2);
	UT_ASSERT_EQ(test_twophase_calls, 1);
}

UT_TEST(test_exact_origin_tt_aborted_does_not_beat_subtrans_prepared_owner)
{
	ClusterTxResolution resolution;
	ClusterTxResolveReason reason = CLUSTER_TX_RESOLVE_PROTOCOL;
	TransactionId top = TEST_ORIGIN_XID - 1;

	reset_exact_origin_fixture();
	test_tt_slot.status = TT_SLOT_ABORTED;
	test_tt_slot.commit_scn = InvalidScn;
	test_subtrans_chain[0] = TEST_ORIGIN_XID;
	test_subtrans_chain[1] = top;
	test_subtrans_chain_count = 2;
	set_native_status_sample(0, TEST_ORIGIN_XID, TRANSACTION_STATUS_SUB_COMMITTED);
	set_native_status_sample(1, top, TRANSACTION_STATUS_IN_PROGRESS);
	set_native_status_sample(2, top, TRANSACTION_STATUS_IN_PROGRESS);
	test_twophase_prepared = true;
	test_twophase_xid = top;
	memset(&resolution, 0xa5, sizeof(resolution));

	UT_ASSERT_EQ(cluster_runtime_visibility_resolve_exact_origin(
					 &test_origin_locator, CLUSTER_TX_RESOLVE_ROW_WAIT, test_formation_epoch,
					 &resolution, &reason),
				 CLUSTER_TX_PREPARED);
	UT_ASSERT_EQ(reason, CLUSTER_TX_RESOLVE_NONE);
	UT_ASSERT_EQ(resolution.outcome, CLUSTER_TX_PREPARED);
	UT_ASSERT_EQ(resolution.proof_kind, CLUSTER_TX_PROOF_ORIGIN_SUBTRANS_TOP);
	UT_ASSERT_EQ((int)resolution.top_xid, (int)top);
	UT_ASSERT_EQ(test_native_status_calls, 3);
	UT_ASSERT_EQ(test_subtrans_parent_calls, 4);
	UT_ASSERT_EQ(test_twophase_calls, 1);
}

UT_TEST(test_exact_origin_tt_aborted_without_subtrans_native_terminal_fails_closed)
{
	ClusterTxResolution resolution;
	ClusterTxResolution zero = {0};
	ClusterTxResolveReason reason = CLUSTER_TX_RESOLVE_PROTOCOL;
	TransactionId top = TEST_ORIGIN_XID - 1;

	reset_exact_origin_fixture();
	test_tt_slot.status = TT_SLOT_ABORTED;
	test_tt_slot.commit_scn = InvalidScn;
	test_subtrans_chain[0] = TEST_ORIGIN_XID;
	test_subtrans_chain[1] = top;
	test_subtrans_chain_count = 2;
	set_native_status_sample(0, TEST_ORIGIN_XID, TRANSACTION_STATUS_SUB_COMMITTED);
	set_native_status_sample(1, top, TRANSACTION_STATUS_IN_PROGRESS);
	set_native_status_sample(2, top, TRANSACTION_STATUS_IN_PROGRESS);
	test_twophase_xid = top;
	memset(&resolution, 0xa5, sizeof(resolution));

	UT_ASSERT_EQ(cluster_runtime_visibility_resolve_exact_origin(
					 &test_origin_locator, CLUSTER_TX_RESOLVE_ROW_WAIT, test_formation_epoch,
					 &resolution, &reason),
				 CLUSTER_TX_UNKNOWN);
	UT_ASSERT_EQ(reason, CLUSTER_TX_RESOLVE_AUTHORITY_UNAVAILABLE);
	UT_ASSERT_EQ(memcmp(&resolution, &zero, sizeof(resolution)), 0);
	UT_ASSERT_EQ(test_native_status_calls, 3);
	UT_ASSERT_EQ(test_subtrans_parent_calls, 4);
	UT_ASSERT_EQ(test_twophase_calls, 1);
}

UT_TEST(test_exact_origin_tt_aborted_subtrans_second_clog_abort_is_terminal)
{
	ClusterTxResolution resolution;
	ClusterTxResolveReason reason = CLUSTER_TX_RESOLVE_PROTOCOL;
	TransactionId top = TEST_ORIGIN_XID - 1;

	reset_exact_origin_fixture();
	test_tt_slot.status = TT_SLOT_ABORTED;
	test_tt_slot.commit_scn = InvalidScn;
	test_subtrans_chain[0] = TEST_ORIGIN_XID;
	test_subtrans_chain[1] = top;
	test_subtrans_chain_count = 2;
	set_native_status_sample(0, TEST_ORIGIN_XID, TRANSACTION_STATUS_SUB_COMMITTED);
	set_native_status_sample(1, top, TRANSACTION_STATUS_IN_PROGRESS);
	set_native_status_sample(2, top, TRANSACTION_STATUS_ABORTED);
	test_twophase_prepared = true;
	test_twophase_xid = top;
	memset(&resolution, 0xa5, sizeof(resolution));

	UT_ASSERT_EQ(cluster_runtime_visibility_resolve_exact_origin(
					 &test_origin_locator, CLUSTER_TX_RESOLVE_ROW_WAIT, test_formation_epoch,
					 &resolution, &reason),
				 CLUSTER_TX_ABORTED);
	UT_ASSERT_EQ(reason, CLUSTER_TX_RESOLVE_NONE);
	UT_ASSERT_EQ(resolution.outcome, CLUSTER_TX_ABORTED);
	UT_ASSERT_EQ(resolution.proof_kind, CLUSTER_TX_PROOF_ORIGIN_SUBTRANS_TOP);
	UT_ASSERT_EQ(test_native_status_calls, 3);
	UT_ASSERT_EQ(test_subtrans_parent_calls, 4);
	UT_ASSERT_EQ(test_twophase_calls, 1);
}

UT_TEST(test_exact_origin_tt_aborted_subtrans_second_clog_commit_conflicts)
{
	ClusterTxResolution resolution;
	ClusterTxResolution zero = {0};
	ClusterTxResolveReason reason = CLUSTER_TX_RESOLVE_PROTOCOL;
	TransactionId top = TEST_ORIGIN_XID - 1;

	reset_exact_origin_fixture();
	test_tt_slot.status = TT_SLOT_ABORTED;
	test_tt_slot.commit_scn = InvalidScn;
	test_subtrans_chain[0] = TEST_ORIGIN_XID;
	test_subtrans_chain[1] = top;
	test_subtrans_chain_count = 2;
	set_native_status_sample(0, TEST_ORIGIN_XID, TRANSACTION_STATUS_SUB_COMMITTED);
	set_native_status_sample(1, top, TRANSACTION_STATUS_IN_PROGRESS);
	set_native_status_sample(2, top, TRANSACTION_STATUS_COMMITTED);
	test_twophase_xid = top;
	memset(&resolution, 0xa5, sizeof(resolution));

	UT_ASSERT_EQ(cluster_runtime_visibility_resolve_exact_origin(
					 &test_origin_locator, CLUSTER_TX_RESOLVE_ROW_WAIT, test_formation_epoch,
					 &resolution, &reason),
				 CLUSTER_TX_UNKNOWN);
	UT_ASSERT_EQ(reason, CLUSTER_TX_RESOLVE_AUTHORITY_CONFLICT);
	UT_ASSERT_EQ(memcmp(&resolution, &zero, sizeof(resolution)), 0);
	UT_ASSERT_EQ(test_native_status_calls, 3);
	UT_ASSERT_EQ(test_subtrans_parent_calls, 4);
	UT_ASSERT_EQ(test_twophase_calls, 1);
}

UT_TEST(test_exact_origin_nested_subcommitted_top_commit_without_locator_fails_closed)
{
	ClusterTxResolution resolution;
	ClusterTxResolution zero = {0};
	ClusterTxResolveReason reason = CLUSTER_TX_RESOLVE_PROTOCOL;
	TransactionId parent = TEST_ORIGIN_XID - 1;
	TransactionId top = TEST_ORIGIN_XID - 2;

	reset_exact_origin_fixture();
	set_subtrans_chain3(TEST_ORIGIN_XID, parent, top);
	set_native_status_sample(0, TEST_ORIGIN_XID, TRANSACTION_STATUS_SUB_COMMITTED);
	set_native_status_sample(1, TEST_ORIGIN_XID, TRANSACTION_STATUS_SUB_COMMITTED);
	set_native_status_sample(2, top, TRANSACTION_STATUS_COMMITTED);
	memset(&resolution, 0xa5, sizeof(resolution));

	UT_ASSERT_EQ(cluster_runtime_visibility_resolve_exact_origin(
					 &test_origin_locator, CLUSTER_TX_RESOLVE_VISIBILITY, test_formation_epoch,
					 &resolution, &reason),
				 CLUSTER_TX_UNKNOWN);
	UT_ASSERT_EQ(reason, CLUSTER_TX_RESOLVE_COVERAGE_GAP);
	UT_ASSERT_EQ(memcmp(&resolution, &zero, sizeof(resolution)), 0);
	UT_ASSERT_EQ(test_subtrans_parent_calls, 6);
	UT_ASSERT_EQ(test_twophase_calls, 0);
}

UT_TEST(test_exact_origin_subcommitted_top_aborted_is_terminal)
{
	ClusterTxResolution resolution;
	ClusterTxResolveReason reason = CLUSTER_TX_RESOLVE_PROTOCOL;
	TransactionId top = TEST_ORIGIN_XID - 1;

	reset_exact_origin_fixture();
	test_tt_slot.status = TT_SLOT_ACTIVE;
	test_tt_slot.commit_scn = InvalidScn;
	test_subtrans_chain[0] = TEST_ORIGIN_XID;
	test_subtrans_chain[1] = top;
	test_subtrans_chain_count = 2;
	set_native_status_sample(0, TEST_ORIGIN_XID, TRANSACTION_STATUS_SUB_COMMITTED);
	set_native_status_sample(1, top, TRANSACTION_STATUS_ABORTED);
	memset(&resolution, 0xa5, sizeof(resolution));

	UT_ASSERT_EQ(cluster_runtime_visibility_resolve_exact_origin(
					 &test_origin_locator, CLUSTER_TX_RESOLVE_VISIBILITY, test_formation_epoch,
					 &resolution, &reason),
				 CLUSTER_TX_ABORTED);
	UT_ASSERT_EQ(reason, CLUSTER_TX_RESOLVE_NONE);
	UT_ASSERT_EQ((int)resolution.top_xid, (int)top);
	UT_ASSERT_EQ(resolution.outcome, CLUSTER_TX_ABORTED);
	UT_ASSERT_EQ(resolution.proof_kind, CLUSTER_TX_PROOF_ORIGIN_SUBTRANS_TOP);
	UT_ASSERT_EQ(resolution.commit_scn, InvalidScn);
	UT_ASSERT_EQ(test_native_status_calls, 2);
	UT_ASSERT_EQ(test_subtrans_parent_calls, 4);
	UT_ASSERT_EQ(test_twophase_calls, 0);
}

UT_TEST(test_exact_origin_subcommitted_top_in_progress_stays_live)
{
	ClusterTxResolution resolution;
	ClusterTxResolveReason reason = CLUSTER_TX_RESOLVE_PROTOCOL;
	TransactionId top = TEST_ORIGIN_XID - 1;

	reset_exact_origin_fixture();
	test_tt_slot.status = TT_SLOT_ACTIVE;
	test_tt_slot.commit_scn = InvalidScn;
	test_subtrans_chain[0] = TEST_ORIGIN_XID;
	test_subtrans_chain[1] = top;
	test_subtrans_chain_count = 2;
	set_native_status_sample(0, TEST_ORIGIN_XID, TRANSACTION_STATUS_SUB_COMMITTED);
	set_native_status_sample(1, top, TRANSACTION_STATUS_IN_PROGRESS);
	set_native_status_sample(2, top, TRANSACTION_STATUS_IN_PROGRESS);
	test_twophase_xid = top;
	memset(&resolution, 0xa5, sizeof(resolution));

	UT_ASSERT_EQ(cluster_runtime_visibility_resolve_exact_origin(
					 &test_origin_locator, CLUSTER_TX_RESOLVE_ROW_WAIT, test_formation_epoch,
					 &resolution, &reason),
				 CLUSTER_TX_IN_PROGRESS);
	UT_ASSERT_EQ(reason, CLUSTER_TX_RESOLVE_NONE);
	UT_ASSERT_EQ((int)resolution.top_xid, (int)top);
	UT_ASSERT_EQ(resolution.outcome, CLUSTER_TX_IN_PROGRESS);
	UT_ASSERT_EQ(resolution.proof_kind, CLUSTER_TX_PROOF_ORIGIN_SUBTRANS_TOP);
	UT_ASSERT_EQ(resolution.commit_scn, InvalidScn);
	UT_ASSERT_EQ(test_native_status_calls, 3);
	UT_ASSERT_EQ(test_subtrans_parent_calls, 4);
	UT_ASSERT_EQ(test_twophase_calls, 1);
}

UT_TEST(test_exact_origin_subcommitted_top_prepared_stays_live)
{
	ClusterTxResolution resolution;
	ClusterTxResolveReason reason = CLUSTER_TX_RESOLVE_PROTOCOL;
	TransactionId top = TEST_ORIGIN_XID - 1;

	reset_exact_origin_fixture();
	test_tt_slot.status = TT_SLOT_ACTIVE;
	test_tt_slot.commit_scn = InvalidScn;
	test_subtrans_chain[0] = TEST_ORIGIN_XID;
	test_subtrans_chain[1] = top;
	test_subtrans_chain_count = 2;
	set_native_status_sample(0, TEST_ORIGIN_XID, TRANSACTION_STATUS_SUB_COMMITTED);
	set_native_status_sample(1, top, TRANSACTION_STATUS_IN_PROGRESS);
	set_native_status_sample(2, top, TRANSACTION_STATUS_IN_PROGRESS);
	test_twophase_prepared = true;
	test_twophase_xid = top;
	memset(&resolution, 0xa5, sizeof(resolution));

	UT_ASSERT_EQ(cluster_runtime_visibility_resolve_exact_origin(
					 &test_origin_locator, CLUSTER_TX_RESOLVE_ROW_WAIT, test_formation_epoch,
					 &resolution, &reason),
				 CLUSTER_TX_PREPARED);
	UT_ASSERT_EQ(reason, CLUSTER_TX_RESOLVE_NONE);
	UT_ASSERT_EQ((int)resolution.top_xid, (int)top);
	UT_ASSERT_EQ(resolution.proof_kind, CLUSTER_TX_PROOF_ORIGIN_SUBTRANS_TOP);
	UT_ASSERT_EQ(test_subtrans_parent_calls, 4);
	UT_ASSERT_EQ(test_twophase_calls, 1);
}

UT_TEST(test_exact_origin_subtrans_edge_change_fails_closed)
{
	ClusterTxResolution resolution;
	ClusterTxResolution zero = {0};
	ClusterTxResolveReason reason = CLUSTER_TX_RESOLVE_PROTOCOL;
	TransactionId top = TEST_ORIGIN_XID - 1;

	reset_exact_origin_fixture();
	test_tt_slot.status = TT_SLOT_ACTIVE;
	test_tt_slot.commit_scn = InvalidScn;
	test_subtrans_chain[0] = TEST_ORIGIN_XID;
	test_subtrans_chain[1] = top;
	test_subtrans_chain_count = 2;
	test_subtrans_mutate_recheck = true;
	set_native_status_sample(0, TEST_ORIGIN_XID, TRANSACTION_STATUS_SUB_COMMITTED);
	set_native_status_sample(1, top, TRANSACTION_STATUS_IN_PROGRESS);
	set_native_status_sample(2, top, TRANSACTION_STATUS_IN_PROGRESS);
	test_twophase_xid = top;
	memset(&resolution, 0xa5, sizeof(resolution));

	UT_ASSERT_EQ(cluster_runtime_visibility_resolve_exact_origin(
					 &test_origin_locator, CLUSTER_TX_RESOLVE_VISIBILITY, test_formation_epoch,
					 &resolution, &reason),
				 CLUSTER_TX_UNKNOWN);
	UT_ASSERT_EQ(reason, CLUSTER_TX_RESOLVE_SUBTRANS_CHANGED);
	UT_ASSERT_EQ(memcmp(&resolution, &zero, sizeof(resolution)), 0);
}

UT_TEST(test_exact_origin_subtrans_cycle_fails_closed)
{
	ClusterTxResolution resolution;
	ClusterTxResolution zero = {0};
	ClusterTxResolveReason reason = CLUSTER_TX_RESOLVE_PROTOCOL;

	reset_exact_origin_fixture();
	test_tt_slot.status = TT_SLOT_ACTIVE;
	test_tt_slot.commit_scn = InvalidScn;
	test_subtrans_cycle = true;
	set_native_status_sample(0, TEST_ORIGIN_XID, TRANSACTION_STATUS_SUB_COMMITTED);
	memset(&resolution, 0xa5, sizeof(resolution));

	UT_ASSERT_EQ(cluster_runtime_visibility_resolve_exact_origin(
					 &test_origin_locator, CLUSTER_TX_RESOLVE_VISIBILITY, test_formation_epoch,
					 &resolution, &reason),
				 CLUSTER_TX_UNKNOWN);
	UT_ASSERT_EQ(reason, CLUSTER_TX_RESOLVE_SUBTRANS_CYCLE);
	UT_ASSERT_EQ(memcmp(&resolution, &zero, sizeof(resolution)), 0);
	UT_ASSERT_EQ(test_twophase_calls, 0);
}

UT_TEST(test_exact_origin_subcommitted_before_transaction_xmin_fails_closed)
{
	ClusterTxResolution resolution;
	ClusterTxResolution zero = {0};
	ClusterTxResolveReason reason = CLUSTER_TX_RESOLVE_PROTOCOL;

	reset_exact_origin_fixture();
	test_tt_slot.status = TT_SLOT_ACTIVE;
	test_tt_slot.commit_scn = InvalidScn;
	TransactionXmin = TEST_ORIGIN_XID + 1;
	set_native_status_sample(0, TEST_ORIGIN_XID, TRANSACTION_STATUS_SUB_COMMITTED);
	memset(&resolution, 0xa5, sizeof(resolution));

	UT_ASSERT_EQ(cluster_runtime_visibility_resolve_exact_origin(
					 &test_origin_locator, CLUSTER_TX_RESOLVE_VISIBILITY, test_formation_epoch,
					 &resolution, &reason),
				 CLUSTER_TX_UNKNOWN);
	UT_ASSERT_EQ(reason, CLUSTER_TX_RESOLVE_COVERAGE_GAP);
	UT_ASSERT_EQ(memcmp(&resolution, &zero, sizeof(resolution)), 0);
	UT_ASSERT_EQ(test_native_status_calls, 1);
	UT_ASSERT_EQ(test_subtrans_parent_calls, 0);
	UT_ASSERT_EQ(test_twophase_calls, 0);
}

UT_TEST(test_exact_origin_subtrans_depth_fails_closed)
{
	ClusterTxResolution resolution;
	ClusterTxResolution zero = {0};
	ClusterTxResolveReason reason = CLUSTER_TX_RESOLVE_PROTOCOL;
	TransactionId child = 5000;
	int i;

	reset_exact_origin_fixture();
	test_tt_slot.status = TT_SLOT_ACTIVE;
	test_tt_slot.commit_scn = InvalidScn;
	test_origin_locator.xid = child;
	test_origin_record.xid = child;
	test_tt_slot.xid = child;
	for (i = 0; i <= CLUSTER_R4_SUBTRANS_MAX_DEPTH; i++)
		test_subtrans_chain[i] = child - i;
	test_subtrans_chain_count = CLUSTER_R4_SUBTRANS_MAX_DEPTH + 1;
	set_native_status_sample(0, child, TRANSACTION_STATUS_SUB_COMMITTED);
	memset(&resolution, 0xa5, sizeof(resolution));

	UT_ASSERT_EQ(cluster_runtime_visibility_resolve_exact_origin(
					 &test_origin_locator, CLUSTER_TX_RESOLVE_VISIBILITY, test_formation_epoch,
					 &resolution, &reason),
				 CLUSTER_TX_UNKNOWN);
	UT_ASSERT_EQ(reason, CLUSTER_TX_RESOLVE_SUBTRANS_DEPTH);
	UT_ASSERT_EQ(memcmp(&resolution, &zero, sizeof(resolution)), 0);
	UT_ASSERT_EQ(test_subtrans_parent_calls, CLUSTER_R4_SUBTRANS_MAX_DEPTH);
}

UT_TEST(test_exact_origin_subtrans_max_chain_is_rechecked_once_per_edge)
{
	ClusterTxResolution resolution;
	ClusterTxResolveReason reason = CLUSTER_TX_RESOLVE_PROTOCOL;
	TransactionId child = 5000;
	TransactionId top;
	int i;

	reset_exact_origin_fixture();
	test_tt_slot.status = TT_SLOT_ACTIVE;
	test_tt_slot.commit_scn = InvalidScn;
	test_origin_locator.xid = child;
	test_origin_record.xid = child;
	test_tt_slot.xid = child;
	for (i = 0; i < CLUSTER_R4_SUBTRANS_MAX_DEPTH; i++)
		test_subtrans_chain[i] = child - i;
	test_subtrans_chain_count = CLUSTER_R4_SUBTRANS_MAX_DEPTH;
	top = test_subtrans_chain[CLUSTER_R4_SUBTRANS_MAX_DEPTH - 1];
	set_native_status_sample(0, child, TRANSACTION_STATUS_SUB_COMMITTED);
	set_native_status_sample(1, top, TRANSACTION_STATUS_ABORTED);
	memset(&resolution, 0xa5, sizeof(resolution));

	UT_ASSERT_EQ(cluster_runtime_visibility_resolve_exact_origin(
					 &test_origin_locator, CLUSTER_TX_RESOLVE_VISIBILITY, test_formation_epoch,
					 &resolution, &reason),
				 CLUSTER_TX_ABORTED);
	UT_ASSERT_EQ(reason, CLUSTER_TX_RESOLVE_NONE);
	UT_ASSERT_EQ((int)resolution.top_xid, (int)top);
	UT_ASSERT_EQ(resolution.proof_kind, CLUSTER_TX_PROOF_ORIGIN_SUBTRANS_TOP);
	UT_ASSERT_EQ(test_subtrans_parent_calls, 2 * CLUSTER_R4_SUBTRANS_MAX_DEPTH);
}

UT_TEST(test_origin_export_copies_only_the_final_revalidated_resident_data_page)
{
	int variant;
	for (variant = 0; variant < 8; variant++) {
		ClusterRuntimeVisibilityOriginPlan plan;
		ClusterTxResolution resolution;
		ClusterTxResolveReason reason;
		ClusterSemanticAdmissionToken admission;
		ClusterUndoBlock0CurrentGuard data_guard = { 0 }, tt_guard = { 0 };
		ClusterUndoBlock0ResolvedRoot data_root = { 0 }, tt_root = { 0 };
		PGAlignedBlock page, expected;
		bool negative = variant >= 2;

		reset_exact_origin_fixture();
		test_origin_locator.tt_wrap = TT_WRAP_INVALID;
		memset(&admission, 0, sizeof(admission));
		admission.feature_bit = CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1;
		admission.record_generation = 5;
		admission.formation_epoch = test_formation_epoch;
		admission.side = CLUSTER_SEMANTIC_TARGET_SIDE;
		admission.entered = true;
		data_root.intent = tt_root.intent = CLUSTER_UNDO_PATH_RUNTIME_SHARED;
		data_root.root_id = 91;
		tt_root.root_id = 92;
		data_root.root_generation = tt_root.root_generation = 7;
		if (variant == 0)
			test_origin_record.tt_slot_segment_id = TEST_RECORD_SEGMENT;
		UT_ASSERT_EQ(cluster_runtime_visibility_origin_plan_freeze_data_held(
						 &test_origin_locator, CLUSTER_TX_RESOLVE_VISIBILITY, &admission, NULL,
						 &data_guard, &data_root, &plan, &resolution, &reason),
					 variant == 0 ? CLUSTER_RUNTIME_VISIBILITY_ORIGIN_COMPLETE
								  : CLUSTER_RUNTIME_VISIBILITY_ORIGIN_NEEDS_CANONICAL);
		if (variant != 0 && variant != 6)
			UT_ASSERT(cluster_runtime_visibility_origin_plan_sample_canonical_held(
				&plan, CLUSTER_TX_RESOLVE_VISIBILITY, &admission, &tt_guard, &tt_root, &reason));
		switch (variant) {
		case 2:
			test_candidate_mutate_record_on_recheck = true;
			break;
		case 3:
			test_candidate_sample_result = CLUSTER_UNDO_BLOCK0_NOT_PUBLISHED;
			break;
		case 4:
			admission.formation_epoch++;
			break;
		case 5:
			data_root.root_generation++;
			break;
		case 7:
			/* The same root/UBA after physical reuse cannot satisfy the plan
			 * frozen at generation17, even if the record bytes still match. */
			test_candidate_data_generation++;
			break;
		default:
			break;
		}
		memset(page.data, 0xa5, BLCKSZ);
		memset(expected.data, negative ? 0xa5 : 0x5a, BLCKSZ);
		UT_ASSERT_EQ(cluster_runtime_visibility_origin_plan_copy_data_held(
						 &plan, CLUSTER_TX_RESOLVE_VISIBILITY, &admission, &data_guard, &data_root,
						 &resolution, page.data, &reason),
					 negative ? CLUSTER_TX_UNKNOWN : CLUSTER_TX_COMMITTED);
		UT_ASSERT_EQ(memcmp(page.data, expected.data, BLCKSZ), 0);
		if (!negative) {
			UT_ASSERT_EQ(resolution.locator_echo.tt_wrap, TEST_ORIGIN_WRAP);
			UT_ASSERT_EQ(reason, CLUSTER_TX_RESOLVE_NONE);
			/* Export consumes the frozen plan; it cannot be replayed. */
			memset(page.data, 0xa5, BLCKSZ);
			memset(expected.data, 0xa5, BLCKSZ);
			UT_ASSERT_EQ(cluster_runtime_visibility_origin_plan_copy_data_held(
							 &plan, CLUSTER_TX_RESOLVE_VISIBILITY, &admission, &data_guard,
							 &data_root, &resolution, page.data, &reason),
						 CLUSTER_TX_UNKNOWN);
			UT_ASSERT_EQ(memcmp(page.data, expected.data, BLCKSZ), 0);
		}
	}
}

int
main(void)
{
	UT_PLAN(108);
	RUN_PAIR_TEST(0);
	RUN_PAIR_TEST(1);
	RUN_PAIR_TEST(2);
	RUN_PAIR_TEST(3);
	RUN_PAIR_TEST(4);
	RUN_PAIR_TEST(5);
	RUN_PAIR_TEST(6);
	RUN_PAIR_TEST(7);
	RUN_PAIR_TEST(8);
	RUN_PAIR_TEST(9);
	RUN_PAIR_TEST(10);
	RUN_PAIR_TEST(11);
	RUN_PAIR_TEST(12);
	RUN_PAIR_TEST(13);
	RUN_PAIR_TEST(14);
	RUN_PAIR_TEST(15);
	RUN_PAIR_TEST(16);
	RUN_PAIR_TEST(17);
	RUN_PAIR_TEST(18);
	RUN_PAIR_TEST(19);
	RUN_PAIR_TEST(20);
	RUN_PAIR_TEST(21);
	RUN_PAIR_TEST(22);
	RUN_PAIR_TEST(23);
	RUN_PAIR_TEST(24);
	RUN_PAIR_TEST(25);
	RUN_PAIR_TEST(26);
	RUN_PAIR_TEST(27);
	RUN_PAIR_TEST(28);
	RUN_PAIR_TEST(29);
	RUN_PAIR_TEST(30);
	RUN_PAIR_TEST(31);
	RUN_PAIR_TEST(32);
	RUN_PAIR_TEST(33);
	RUN_PAIR_TEST(34);
	RUN_PAIR_TEST(35);
	RUN_PAIR_TEST(36);
	RUN_PAIR_TEST(37);
	RUN_PAIR_TEST(38);
	RUN_PAIR_TEST(39);
	UT_RUN(test_out_of_domain_values_fail_closed);
	UT_RUN(test_current_member_target_canonical_active_requires_exact_physical_slot);
	UT_RUN(test_ctrc_physical_active_sample_excludes_precommit_committed_window);
	UT_RUN(test_ctrc_current_owner_uses_published_binding_across_rollover);
	UT_RUN(test_current_mx_active_proof_reconstructs_exact_ctrc_identity);
	UT_RUN(test_current_mx_active_proof_uses_origin_binding_without_foreign_storage);
	UT_RUN(test_current_member_rolled_terminal_uses_locator_then_canonical_scur);
	UT_RUN(test_current_member_terminal_on_current_segment_survives_retired_owner_index);
	UT_RUN(test_current_member_local_terminal_accepts_clean_formation_epoch_zero);
	UT_RUN(test_current_member_active_on_current_segment_requires_live_owner_index);
	UT_RUN(test_exact_origin_committed_uses_canonical_tt_identity_and_direct_clog);
	UT_RUN(test_terminal_census_local_origin_uses_resident_candidate2_and_canonical_upgrade);
	UT_RUN(test_terminal_census_nonresident_cleanout_uses_exact_local_c1b_pair);
	UT_RUN(test_terminal_census_retained_pair_negative_matrix_fails_closed);
	UT_RUN(test_terminal_census_same_owner_cross_segment_is_sequential_and_exact);
	UT_RUN(test_visibility_same_owner_cross_segment_is_sequential_and_exact);
	UT_RUN(test_visibility_precommit_committed_slot_with_live_origin_stays_in_progress);
	UT_RUN(test_canonical_sample_reports_first_failed_predicate);
	UT_RUN(test_recycled_canonical_abort_rechecks_exact_data_and_origin);
	UT_RUN(test_recycled_canonical_abort_same_segment);
	UT_RUN(test_reborn_empty_canonical_preserves_exact_origin_abort);
	UT_RUN(test_reborn_allocated_canonical_preserves_exact_origin_abort);
	UT_RUN(test_reborn_canonical_nonabort_and_census_refuse);
	UT_RUN(test_reborn_canonical_malformed_or_initial_generation_refuses);
	UT_RUN(test_reborn_canonical_revocation_truncation_and_data_drift_refuse);
	UT_RUN(test_reborn_split_plan_rechecks_before_publication);
	UT_RUN(test_recycled_canonical_nonabort_never_borrows_successor_status);
	UT_RUN(test_recycled_canonical_invalid_successor_never_reads_native);
	UT_RUN(test_recycled_canonical_missing_or_revoked_window_refuses);
	UT_RUN(test_recycled_canonical_truncation_and_data_drift_refuse);
	UT_RUN(test_recycled_canonical_clog_error_rethrows_and_releases_own_locks);
	UT_RUN(test_empty_physical_slot_remains_unknown_despite_current_allocator_owner);
	UT_RUN(test_allocator_identity_never_overrides_empty_physical_slot);
	UT_RUN(test_empty_physical_slot_remains_unknown_despite_rolled_live_xid);
	UT_RUN(test_rolled_live_evidence_never_overrides_mismatched_physical_slot);
	UT_RUN(test_empty_physical_slot_never_consults_rolled_native_axes);
	UT_RUN(test_empty_physical_slot_cannot_reach_rolled_recheck);
	UT_RUN(test_terminal_census_never_projects_current_active_owner);
	UT_RUN(test_terminal_census_rejects_precommit_committed_slot_with_live_origin);
	UT_RUN(test_terminal_census_cross_segment_data_drift_fails_closed);
	UT_RUN(test_terminal_census_cross_owner_tt_alias_fails_closed);
	UT_RUN(test_terminal_census_pending_acquire_failure_cancels_candidate_guard);
	UT_RUN(test_terminal_census_pending_wakes_repoll_until_exact_terminal);
	UT_RUN(test_exact_origin_bad_record_wrap_fails_before_tt_or_clog);
	UT_RUN(test_exact_origin_aborted_uses_exact_tt_and_direct_clog);
	UT_RUN(test_exact_origin_conflicting_terminal_evidence_fails_closed);
	UT_RUN(test_exact_origin_active_and_native_in_progress_stays_live);
	UT_RUN(test_exact_origin_prepared_is_distinct_live_outcome);
	UT_RUN(test_exact_origin_prepared_finish_abort_terminal_wins);
	UT_RUN(test_exact_origin_prepared_finish_commit_without_exact_scn_fails_closed);
	UT_RUN(test_exact_origin_tt_aborted_does_not_beat_direct_prepared_owner);
	UT_RUN(test_exact_origin_tt_aborted_without_direct_native_terminal_fails_closed);
	UT_RUN(test_exact_origin_tt_aborted_direct_second_clog_abort_is_terminal);
	UT_RUN(test_exact_origin_tt_aborted_direct_second_clog_commit_conflicts);
	UT_RUN(test_exact_origin_tt_aborted_does_not_beat_subtrans_prepared_owner);
	UT_RUN(test_exact_origin_tt_aborted_without_subtrans_native_terminal_fails_closed);
	UT_RUN(test_exact_origin_tt_aborted_subtrans_second_clog_abort_is_terminal);
	UT_RUN(test_exact_origin_tt_aborted_subtrans_second_clog_commit_conflicts);
	UT_RUN(test_exact_origin_nested_subcommitted_top_commit_without_locator_fails_closed);
	UT_RUN(test_exact_origin_subcommitted_top_aborted_is_terminal);
	UT_RUN(test_exact_origin_subcommitted_top_in_progress_stays_live);
	UT_RUN(test_exact_origin_subcommitted_top_prepared_stays_live);
	UT_RUN(test_exact_origin_subtrans_edge_change_fails_closed);
	UT_RUN(test_exact_origin_subtrans_cycle_fails_closed);
	UT_RUN(test_exact_origin_subcommitted_before_transaction_xmin_fails_closed);
	UT_RUN(test_exact_origin_subtrans_depth_fails_closed);
	UT_RUN(test_exact_origin_subtrans_max_chain_is_rechecked_once_per_edge);
	UT_RUN(test_origin_export_copies_only_the_final_revalidated_resident_data_page);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
