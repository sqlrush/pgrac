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
#include "access/xact.h"
#include "access/subtrans.h"
#include "access/twophase.h"
#include "access/xlog.h"
#include "cluster/cluster_epoch.h"
#include "cluster/cluster_multixact_current_stats.h"
#include "cluster/cluster_multixact_current_wire.h"
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
#include "cluster/cluster_cr_server.h"
#include "storage/ipc.h"
#include "storage/lwlock.h"
#include "storage/procarray.h"
#include "utils/snapmgr.h"

#undef printf

#include "unit_test.h"

UT_DEFINE_GLOBALS();

#include "test_cluster_current_mx_local_sample.inc"

#define TEST_ORIGIN_XID ((TransactionId)797)
#define TEST_ORIGIN_WRAP ((uint16)7)
#define TEST_RECORD_SEGMENT ((uint32)1)
#define TEST_TT_SEGMENT ((uint32)2)
#define TEST_TT_OFFSET ((uint16)3)

int cluster_node_id = 0;
BackendId MyBackendId = 7;
int cluster_gcs_reply_timeout_ms = 1000;
int cluster_subtrans_max_chain_depth = 32;
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
static char test_origin_payload[64];
static size_t test_origin_payload_bytes;
static int test_undo_read_calls;
static int test_tt_exact_calls;
static int test_tt_snapshot_calls;
static int test_native_status_calls;
static int test_by_xid_scan_calls;
static bool test_durable_locator_available;
static int test_durable_locator_calls;
static ClusterTTDurableLocate test_durable_locator_result;
static uint16 test_durable_locator_segment;
static uint16 test_durable_locator_slot;
static uint16 test_durable_locator_wrap;
static uint8 test_durable_locator_status;
static FullTransactionId test_next_full_xid;
static FullTransactionId test_next_full_on_locate;
static TransactionId test_oldest_clog_on_locate;
static int test_release_scope_drift;
static int test_release_identity_drift;
static uint64 test_system_identifier;
static uint64 test_boot_incarnation;
static bool test_original_available;
static FullTransactionId test_original_full_xid;
static ClusterCtrcTxnKeyV1 test_original_key;
static int test_original_calls;
static int test_original_drift;
static bool test_candidate_copy_generation_drift;
static bool test_candidate_root_drift;
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
static unsigned test_candidate_cold_copy_mask;
static int test_candidate_data_pin_calls;
static int test_candidate_data_unpin_calls;
static bool test_candidate_data_pool_disabled;
static bool test_candidate_data_io_throw;
static bool test_candidate_data_io_generation_drift;
static PGAlignedBlock test_candidate_io_page;
static int test_candidate_release_calls;
static bool test_candidate_release_failed;
static bool test_candidate_release_epoch_drift;
static bool test_candidate_release_admission_drift;
static int test_candidate_release_fault_call;
static bool test_admission_revoked;
static const bool *test_updater_retry_observer;
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
static ClusterCtrcTouchResult test_ctrc_touch_result = CLUSTER_CTRC_TOUCH_RECORDED;
static uint32 test_ctrc_touch_segment;
static uint32 test_ctrc_touch_generation;
static ClusterCtrcTxnKeyV1 test_ctrc_touched_key;
static ClusterCtrcParticipantIdentity test_ctrc_touched_participant;
static bool test_ctrc_touch_generation_drift;
static bool test_ctrc_revoke_on_release;
static bool test_ctrc_grant_revoked;
static TimestampTz test_now;
static bool test_mx_batch_enabled;
static int test_mx_remote_calls;
static bool test_mx_remote_failed;
static bool test_mx_remote_revokes_grant;
static bool test_mx_remote_epoch_drift;
static bool test_mx_remote_identity_mismatch;
static bool test_mx_remote_omits_proof;
static bool test_mx_remote_invalid_commit_scn;
static bool test_mx_remote_committed_with_grant;
static int test_undo_pending_prepare_calls;
static int test_undo_pending_cancel_calls;
static ClusterCtrcTxnKeyV1 test_undo_pending_key;
static ClusterCtrcParticipantIdentity test_undo_pending_participant;
static ClusterCtrcPublicationIdV1 test_undo_pending_publication;
static ClusterCtrcTargetV1 test_undo_pending_target;
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
static SCN test_local_origin_bound;
static SCN test_local_origin_exact;
static bool test_local_ordinary_abort;
static bool test_local_last_ordinary_single;
static int test_local_origin_calls;
static bool test_local_origin_epoch_drift;

extern ClusterUndoVerdictResult
cluster_runtime_visibility_test_local_retained_read(TransactionId xid, uint32 segment, uint32 slot,
													SCN retained_scn, SCN read_scn);
static bool test_remote_origin_enabled;
static int test_remote_origin_calls;
static uint32 test_remote_origin_generation;
static ClusterTxOutcome test_remote_origin_outcome;
static bool test_remote_origin_revoke_admission;
static const uint8 *test_remote_origin_verdict_page;
static bool test_receiver_registered;
static ClusterTxResolution test_receiver_retained;
static uint64 test_receiver_peer_boot, test_receiver_peer_generation, test_receiver_peer_epoch;
static uint64 test_receiver_self_admitted;
static unsigned test_receiver_checks, test_receiver_drift;
static ClusterUndoBlock0CurrentStep test_candidate_acquire_step = CLUSTER_UNDO_BLOCK0_CURRENT_HELD;
static ClusterUndoBlock0CurrentStep test_candidate_poll_step = CLUSTER_UNDO_BLOCK0_CURRENT_FAILED;
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
					 const char *fileName pg_attribute_unused(),
					 int lineNumber pg_attribute_unused())
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

void
cluster_rtvis_resolve_note_aborted(void)
{}

void
cluster_scn_observe(SCN remote_scn pg_attribute_unused())
{}

int
scn_time_cmp(SCN a, SCN b)
{
	return scn_local(a) == scn_local(b) ? 0 : (scn_local(a) > scn_local(b) ? 1 : -1);
}

bool
cluster_lms_undo_verdict_fill_page(TransactionId xid, bool authoritative, bool ordinary_single,
								   ClusterGcsUndoVerdictPage *page)
{
	test_local_origin_calls++;
	test_local_last_ordinary_single = ordinary_single;
	UT_ASSERT_EQ(xid, TEST_ORIGIN_XID);
	UT_ASSERT(!authoritative);
	if (test_local_origin_epoch_drift)
		test_formation_epoch++;
	if (SCN_VALID(test_local_origin_exact)) {
		memset(page, 0, sizeof(*page));
		page->magic = CLUSTER_GCS_UNDO_VERDICT_MAGIC;
		page->version = CLUSTER_GCS_UNDO_VERDICT_VERSION;
		page->xid_echo = xid;
		page->verdict = CLUSTER_GCS_UNDO_VERDICT_COMMITTED_EXACT;
		page->commit_scn = test_local_origin_exact;
		page->wrap = TEST_ORIGIN_WRAP;
		return true;
	}
	if (test_local_ordinary_abort) {
		if (!ordinary_single)
			return false;
		memset(page, 0, sizeof(*page));
		page->magic = CLUSTER_GCS_UNDO_VERDICT_MAGIC;
		page->version = CLUSTER_GCS_UNDO_VERDICT_VERSION;
		page->xid_echo = xid;
		page->verdict = CLUSTER_GCS_UNDO_VERDICT_ABORTED;
		return true;
	}
	if (!SCN_VALID(test_local_origin_bound))
		return false;
	memset(page, 0, sizeof(*page));
	page->magic = CLUSTER_GCS_UNDO_VERDICT_MAGIC;
	page->version = CLUSTER_GCS_UNDO_VERDICT_VERSION;
	page->xid_echo = xid;
	page->verdict = CLUSTER_GCS_UNDO_VERDICT_COMMITTED_BELOW_HORIZON;
	page->horizon_scn = test_local_origin_bound;
	return true;
}

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
cluster_tt_slot_durable_lookup_committed_stable(uint32 segment_id, uint16 slot_offset,
												TransactionId xid, uint32 expected_wrap,
												ClusterTTDurableXidCommitCheck xid_committed,
												SCN *commit_scn)
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
cluster_tt_slot_durable_read_exact_stable(uint32 segment_id, uint16 slot_offset, TransactionId xid,
										  uint16 expected_wrap, TTSlot *slot_out)
{
	test_tt_snapshot_calls++;
	test_tt_segment_seen = segment_id;
	test_tt_slot_seen = slot_offset;
	test_tt_xid_seen = xid;
	test_tt_wrap_seen = expected_wrap;
	if (slot_out == NULL || test_tt_slot.status > TT_SLOT_RECYCLABLE || test_tt_slot.xid != xid
		|| test_tt_slot.wrap != expected_wrap)
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
cluster_tt_slot_durable_locate_any_by_xid_origin(int origin_node, TransactionId xid,
												 uint16 *out_seg, uint16 *out_slot,
												 uint16 *out_wrap, uint8 *out_status)
{
	test_durable_locator_calls++;
	UT_ASSERT_EQ(test_xact_lock_depth, 0);
	UT_ASSERT_EQ(origin_node, 0);
	UT_ASSERT_EQ((int)xid, (int)TEST_ORIGIN_XID);
	if (FullTransactionIdIsValid(test_next_full_on_locate))
		test_next_full_xid = test_next_full_on_locate;
	if (TransactionIdIsValid(test_oldest_clog_on_locate))
		test_variable_cache.oldestClogXid = test_oldest_clog_on_locate;
	if (!test_durable_locator_available)
		return CLUSTER_TT_DURABLE_LOCATE_MISSING;
	if (test_durable_locator_result != CLUSTER_TT_DURABLE_LOCATE_FOUND)
		return test_durable_locator_result;
	UT_ASSERT_NOT_NULL(out_seg);
	UT_ASSERT_NOT_NULL(out_slot);
	UT_ASSERT_NOT_NULL(out_wrap);
	*out_seg = test_durable_locator_segment;
	*out_slot = test_durable_locator_slot;
	*out_wrap = test_durable_locator_wrap;
	if (out_status != NULL)
		*out_status = test_durable_locator_status;
	return CLUSTER_TT_DURABLE_LOCATE_FOUND;
}

int
cluster_xid_origin_slot(TransactionId xid)
{
	if (test_mx_batch_enabled && xid == TEST_ORIGIN_XID + 1)
		return 1;
	UT_ASSERT_EQ((int)xid, (int)TEST_ORIGIN_XID);
	return 0;
}

FullTransactionId
ReadNextFullTransactionId(void)
{
	UT_ASSERT_EQ(test_xact_lock_depth, 0);
	return test_next_full_xid;
}

uint64
GetSystemIdentifier(void)
{
	return test_system_identifier;
}

TimestampTz
GetCurrentTimestamp(void)
{
	return test_now;
}

bool
TransactionIdIsCurrentTransactionId(TransactionId xid)
{
	UT_ASSERT(test_mx_batch_enabled);
	UT_ASSERT_EQ(xid, TEST_ORIGIN_XID);
	return false;
}

void
cluster_multixact_current_stats_bump(ClusterCurrentMxStatId stat pg_attribute_unused())
{}

/* Only the foreign transport boundary is supplied here. Local members and
 * updater provenance execute the real runtime and batch implementations. */
ClusterMxResolveResult
cluster_gcs_current_mx_member_proof_fetch_and_wait(
	int32 origin_node, ClusterCurrentMxProofForwardV2 *request, ClusterCurrentMemberProof *proofs,
	uint16 proofs_cap, uint16 *proof_count, ClusterCurrentUpdaterProof *updater_proof,
	uint32 *requester_capability_generation_out, TimestampTz deadline)
{
	ClusterCurrentMxProofForwardV2 decoded;
	ClusterCurrentMemberProof *proof = &proofs[0];

	UT_ASSERT(test_mx_batch_enabled);
	UT_ASSERT_EQ(origin_node, 1);
	UT_ASSERT_EQ(test_candidate_held_count, 0);
	UT_ASSERT_EQ(test_ctrc_touch_calls, 1);
	UT_ASSERT(deadline > test_now);
	UT_ASSERT(proofs_cap > 0);
	UT_ASSERT(cluster_multixact_current_wire_validate_proof_forward(
		request, sizeof(*request), cluster_node_id, origin_node, test_formation_epoch, &decoded));
	UT_ASSERT_EQ(decoded.prefix.body_kind, CLUSTER_CURRENT_MX_PROOF_BODY_MEMBER_ASKS);
	UT_ASSERT_EQ(decoded.prefix.entry_count, 1);
	UT_ASSERT_EQ(decoded.trailer.body.asks[0].xid, TEST_ORIGIN_XID + 1);
	test_mx_remote_calls++;
	*proof_count = 0;
	*requester_capability_generation_out = 0;
	memset(updater_proof, 0, sizeof(*updater_proof));
	updater_proof->verdict = CUCP_UNKNOWN;
	if (test_mx_remote_failed)
		return CMX_RESOLVE_UNKNOWN;
	if (test_mx_remote_omits_proof)
		return CMX_RESOLVE_OK;
	memset(proof, 0, sizeof(*proof));
	proof->member_xid = decoded.trailer.body.asks[0].xid;
	proof->member_ordinal = decoded.trailer.body.asks[0].member_ordinal;
	proof->member_status = decoded.trailer.body.asks[0].member_status;
	proof->state = CCM_COMMITTED;
	proof->commit_scn = scn_encode(1, 50);
	if (test_mx_remote_invalid_commit_scn)
		proof->commit_scn = InvalidScn;
	if (test_mx_remote_committed_with_grant)
		ClusterCurrentMemberProofSetCtrcGrant(proof, 99);
	if (test_mx_remote_identity_mismatch)
		proof->member_xid = TEST_ORIGIN_XID;
	*proof_count = 1;
	if (test_mx_remote_revokes_grant)
		test_ctrc_grant_revoked = true;
	if (test_mx_remote_epoch_drift)
		test_formation_epoch++;
	return CMX_RESOLVE_OK;
}

uint64
cluster_qvotec_get_self_incarnation(void)
{
	return test_boot_incarnation;
}

bool
cluster_reconfig_get_observed_slot(int32 node_id, uint64 *incarnation,
								   uint64 *generation)
{
	if (incarnation != NULL)
		*incarnation = test_receiver_registered && node_id == 0 ? test_receiver_peer_boot : 0;
	if (generation != NULL)
		*generation = test_receiver_registered && node_id == 0 ? test_receiver_peer_generation : 0;
	return test_receiver_registered && node_id == 0;
}

uint64
cluster_reconfig_get_observed_epoch(int32 node_id)
{
	return test_receiver_registered && node_id == 0 ? test_receiver_peer_epoch : 0;
}

uint64
cluster_membership_get_last_admitted_incarnation(int32 node_id)
{
	return !test_receiver_registered ? 0
		: node_id == 0 ? test_receiver_peer_boot : node_id == 1 ? test_receiver_self_admitted : 0;
}

/* The shared owner is exercised in test_cluster_ctrc_cleaner. This boundary
 * is an explicitly installed exact receiver record, never codec success. */
bool
cluster_ctrc_itl_original_full_xid_matches_shared(
	const ClusterCtrcTxnKeyV1 *key, FullTransactionId original,
	const ClusterTxLocator *locator, const ClusterCtrcParticipantIdentity *receiver)
{
	const ClusterCtrcTxnKeyV1 *expected = &test_receiver_retained.origin_companion.key;
	bool matched = test_receiver_registered && key != NULL && locator != NULL && receiver != NULL
		&& memcmp(key, expected, sizeof(*key)) == 0
		&& FullTransactionIdEquals(original, test_receiver_retained.origin_companion.original_full_xid)
		&& memcmp(locator, &test_receiver_retained.locator_echo, sizeof(*locator)) == 0
		&& receiver->node_id == 1 && receiver->boot_incarnation == test_receiver_self_admitted
		&& receiver->capability_record_generation == expected->admission_record_generation
		&& receiver->formation_epoch == expected->formation_epoch
		&& receiver->admission_record_generation == expected->admission_record_generation;

	test_receiver_checks++;
	switch (test_receiver_drift) {
	case 1: return false;
	case 2: test_boot_incarnation++; break;
	case 3: test_receiver_peer_boot++; break;
	case 4: test_receiver_peer_generation++; break;
	case 5: test_receiver_peer_epoch++; break;
	case 6: test_system_identifier++; break;
	case 7: test_candidate_root_drift = true; test_regular_root_resolve_calls = 2; break;
	case 8: test_admission_revoked = true; break;
	case 9: test_receiver_self_admitted++; break;
	}
	return matched;
}

ClusterSemanticAdmissionResult
cluster_semantic_activation_enter(uint64 feature_bit, ClusterSemanticAdmissionSide side,
								  ClusterSemanticAdmissionToken *token)
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
	if (test_updater_retry_observer != NULL)
		UT_ASSERT(!*test_updater_retry_observer);
	return !test_admission_revoked && token != NULL && token->entered
		   && token->feature_bit == CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1
		   && token->side == CLUSTER_SEMANTIC_TARGET_SIDE
		   && token->formation_epoch == test_formation_epoch;
}

bool
cluster_semantic_activation_recheck(const ClusterSemanticAdmissionToken *token)
{
	test_regular_admission_recheck_calls++;
	return test_admission_token_exact(token);
}

bool
cluster_semantic_activation_recheck_r4_terminal_census(const ClusterSemanticAdmissionToken *token)
{
	test_terminal_census_recheck_calls++;
	return test_admission_token_exact(token);
}

static bool
test_resolve_shared_undo_root(const ClusterSemanticAdmissionToken *token,
							  ClusterUndoPathIntent intent, uint32 owner_instance,
							  uint32 segment_id, ClusterUndoBlock0ResolvedRoot *out)
{
	if (!test_admission_token_exact(token))
		return false;
	UT_ASSERT_EQ(intent, CLUSTER_UNDO_PATH_RUNTIME_SHARED);
	UT_ASSERT_EQ(owner_instance, 1);
	UT_ASSERT(segment_id == TEST_RECORD_SEGMENT || segment_id == TEST_TT_SEGMENT);
	out->intent = intent;
	out->root_id = segment_id == TEST_RECORD_SEGMENT ? 91 : 92;
	out->root_generation = test_candidate_root_drift && test_regular_root_resolve_calls > 1 ? 8 : 7;
	return true;
}

bool
cluster_semantic_activation_resolve_shared_undo_root(const ClusterSemanticAdmissionToken *token,
													 ClusterUndoPathIntent intent,
													 uint32 owner_instance, uint32 segment_id,
													 ClusterUndoBlock0ResolvedRoot *out)
{
	test_regular_root_resolve_calls++;
	return test_resolve_shared_undo_root(token, intent, owner_instance, segment_id, out);
}

bool
cluster_semantic_activation_resolve_shared_undo_root_r4_terminal_census(
	const ClusterSemanticAdmissionToken *token, ClusterUndoPathIntent intent, uint32 owner_instance,
	uint32 segment_id, ClusterUndoBlock0ResolvedRoot *out)
{
	test_terminal_census_root_resolve_calls++;
	return test_resolve_shared_undo_root(token, intent, owner_instance, segment_id, out);
}

ClusterUndoBlock0CurrentStep
cluster_undo_block0_current_acquire_begin_admitted(
	const ClusterUndoBlock0LogicalKey *key, ClusterUndoBlock0CurrentMode mode,
	int timeout_ms pg_attribute_unused(), const ClusterSemanticAdmissionToken *admission,
	ClusterUndoBlock0CurrentGuard *guard pg_attribute_unused(),
	ClusterUndoBlock0Result *failure pg_attribute_unused())
{
	UT_ASSERT(test_candidate_acquire_calls < (int)lengthof(test_candidate_acquire_segments));
	test_candidate_acquire_segments[test_candidate_acquire_calls] = key->segment_id;
	test_candidate_acquire_calls++;
	UT_ASSERT_EQ(key->owner_instance, 1);
	UT_ASSERT(key->segment_id == TEST_RECORD_SEGMENT || key->segment_id == TEST_TT_SEGMENT);
	UT_ASSERT_EQ(mode, CLUSTER_UNDO_BLOCK0_SCUR);
	UT_ASSERT_EQ(test_xact_lock_depth, 0);
	UT_ASSERT(test_admission_token_exact(admission));
	if (test_candidate_acquire_step == CLUSTER_UNDO_BLOCK0_CURRENT_HELD) {
		test_candidate_held_count++;
		if (test_candidate_max_held_count < test_candidate_held_count)
			test_candidate_max_held_count = test_candidate_held_count;
	}
	return test_candidate_acquire_step;
}

ClusterUndoBlock0CurrentStep
cluster_undo_block0_current_acquire_poll(ClusterUndoBlock0CurrentGuard *guard pg_attribute_unused(),
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
	const ClusterUndoBlock0ResolvedRoot *root, ClusterUndoBlock0Generation *observed)
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
	const ClusterUndoBlock0ResolvedRoot *root, const ClusterUndoBlock0Generation *expected,
	char private_page[BLCKSZ])
{
	UndoSegmentHeaderData *header = (UndoSegmentHeaderData *)private_page;

	test_candidate_block0_copy_calls++;
	UT_ASSERT(expected->known);
	UT_ASSERT_EQ(expected->value, root->root_id == 91 ? test_candidate_data_generation
													  : test_candidate_tt_generation);
	memset(private_page, 0, BLCKSZ);
	if (test_candidate_copy_physical_slot)
		header->tt_slots[TEST_TT_OFFSET] = test_tt_slot;
	if (test_candidate_copy_generation_drift)
		test_candidate_tt_generation++;
	return CLUSTER_UNDO_BLOCK0_OK;
}

bool
cluster_tt_slot_current_owner_by_xid(int node_id, TransactionId xid, ClusterTTSlotCurrentOwner *out)
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
cluster_tt_local_get_published_binding(TransactionId xid, ClusterCanonicalTxnBinding *out)
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

/* Controlled retained-slot source; canonical resolver bodies remain real. */
static bool g_retained_origin_available;
static FullTransactionId g_retained_origin_full;
static int g_retained_origin_reads;
static int g_retained_origin_drift;
static ClusterCtrcTxnKeyV1 g_retained_origin_key;

bool
cluster_tt_slot_origin_identity_read(const ClusterCtrcTxnKeyV1 *key, FullTransactionId *out)
{
	g_retained_origin_reads++;
	if (key != NULL)
		g_retained_origin_key = *key;
	if (out == NULL)
		return false;
	*out = InvalidFullTransactionId;
	if (!g_retained_origin_available || key == NULL
		|| memcmp(key, &test_original_key, sizeof(*key)) != 0)
		return false;
	UT_ASSERT_EQ(test_candidate_held_count, 1);
	if (g_retained_origin_drift == 1)
		test_candidate_tt_generation = ++test_candidate_data_generation;
	else if (g_retained_origin_drift == 2)
		test_candidate_root_drift = true;
	else if (g_retained_origin_drift == 3)
		test_admission_revoked = true;
	else if (g_retained_origin_drift == 4)
		test_boot_incarnation++;
	*out = g_retained_origin_full;
	return true;
}

/* Shared source/locking is a leaf; its real engine has separate coverage. */
bool
cluster_ctrc_origin_original_full_xid_shared(const ClusterCtrcTxnKeyV1 *key,
	FullTransactionId *out)
{
	bool exact;

	test_original_calls++;
	*out = InvalidFullTransactionId;
	if (!test_original_available)
		return false;
	UT_ASSERT_EQ(test_candidate_held_count, 1);
	exact = memcmp(key, &test_original_key, sizeof(*key)) == 0
		&& FullTransactionIdIsNormal(test_original_full_xid)
		&& XidFromFullTransactionId(test_original_full_xid) == key->xid;
	if (test_original_drift == 1)
		test_candidate_tt_generation = ++test_candidate_data_generation;
	else if (test_original_drift == 2)
		test_candidate_root_drift = true;
	else if (test_original_drift == 3)
		test_admission_revoked = true;
	else if (test_original_drift == 4)
		test_boot_incarnation++;
	if (exact)
		*out = test_original_full_xid;
	return exact;
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
	UT_ASSERT_EQ(key->segment_id, test_ctrc_touch_segment);
	UT_ASSERT_EQ(key->segment_generation, test_ctrc_touch_generation);
	UT_ASSERT_EQ(key->slot_offset, TEST_TT_OFFSET);
	UT_ASSERT_EQ(key->slot_wrap, TEST_ORIGIN_WRAP);
	UT_ASSERT_EQ((int)key->xid, (int)TEST_ORIGIN_XID);
	UT_ASSERT_EQ(participant->node_id, (uint16)cluster_node_id);
	UT_ASSERT_EQ(proof_class, CTRC_PROOF_ACTIVE);
	test_ctrc_touched_key = *key;
	test_ctrc_touched_participant = *participant;
	*grant_out = test_ctrc_touch_grant;
	if (test_ctrc_touch_generation_drift) {
		if (key->segment_id == TEST_RECORD_SEGMENT)
			test_candidate_data_generation++;
		else
			test_candidate_tt_generation++;
	}
	return test_ctrc_touch_grant == 0 ? CLUSTER_CTRC_TOUCH_REFUSED : test_ctrc_touch_result;
}

bool
cluster_ctrc_origin_grant_publishable(const ClusterCtrcTxnKeyV1 *key,
									  const ClusterCtrcParticipantIdentity *participant,
									  uint32 grant_generation)
{
	return !test_ctrc_grant_revoked && key != NULL && participant != NULL && grant_generation != 0
		   && grant_generation == test_ctrc_touch_grant && key->xid == TEST_ORIGIN_XID
		   && key->formation_epoch == test_formation_epoch
		   && participant->formation_epoch == test_formation_epoch;
}

TransactionId
GetTopTransactionIdIfAny(void)
{
	return TEST_ORIGIN_XID;
}

ClusterCtrcPrepareResult
cluster_ctrc_receipt_prepare_shared(const ClusterCtrcTxnKeyV1 *key,
									const ClusterCtrcParticipantIdentity *participant, uint32 grant,
									const ClusterCtrcPublicationIdV1 *publication,
									const ClusterCtrcTargetV1 *target,
									ClusterCtrcReceiptHandle *handle)
{
	UT_ASSERT_EQ(test_candidate_held_count, 0);
	UT_ASSERT_EQ(grant, test_ctrc_touch_grant);
	test_undo_pending_prepare_calls++;
	test_undo_pending_key = *key;
	test_undo_pending_participant = *participant;
	test_undo_pending_publication = *publication;
	test_undo_pending_target = *target;
	memset(handle, 0, sizeof(*handle));
	handle->key = *key;
	handle->valid = true;
	return CLUSTER_CTRC_PREPARE_READY;
}

/* Controlled original144 leaf for the already-real pending producer. The
 * unit observes exact key/receiver/grant continuity; shared locking/encoding
 * has its own original-source tests. No arbitrary receiver is accepted. */
bool
cluster_ctrc_origin_fullxid_capture_v2(const ClusterCtrcTxnKeyV1 *key,
	const ClusterCtrcParticipantIdentity *receiver, uint32 grant,
	uint8 bytes[CLUSTER_CTRC_ORIGIN_FULLXID_V2_BYTES])
{
	memset(bytes, 0, CLUSTER_CTRC_ORIGIN_FULLXID_V2_BYTES);
	if (grant == 0 || grant != test_ctrc_touch_grant
		|| memcmp(key, &test_ctrc_touched_key, sizeof(*key)) != 0
		|| memcmp(receiver, &test_ctrc_touched_participant, sizeof(*receiver)) != 0)
		return false;
	memcpy(bytes, key, sizeof(*key));
	memcpy(bytes + sizeof(*key), receiver, sizeof(*receiver));
	memcpy(bytes + sizeof(*key) + sizeof(*receiver), &grant, sizeof(grant));
	return true;
}

ClusterCtrcPrepareResult
cluster_ctrc_receipt_prepare_origin_v2(const ClusterCtrcTxnKeyV1 *key,
	const ClusterCtrcParticipantIdentity *receiver, uint32 grant,
	const ClusterCtrcPublicationIdV1 *publication, const ClusterCtrcTargetV1 *target,
	const uint8 *bytes, Size length, ClusterCtrcReceiptHandle *handle)
{
	uint8 expected[CLUSTER_CTRC_ORIGIN_FULLXID_V2_BYTES];

	if (length != sizeof(expected) || bytes == NULL
		|| !cluster_ctrc_origin_fullxid_capture_v2(key, receiver, grant, expected)
		|| memcmp(bytes, expected, sizeof(expected)) != 0)
		return CLUSTER_CTRC_PREPARE_REFUSED;
	return cluster_ctrc_receipt_prepare_shared(key, receiver, grant, publication,
		target, handle);
}

bool
cluster_ctrc_receipt_cancel_shared(const ClusterCtrcReceiptHandle *handle)
{
	UT_ASSERT(handle->valid);
	test_undo_pending_cancel_calls++;
	return true;
}

uint32
cluster_tt_slot_current_segment(int node_id)
{
	test_current_segment_calls++;
	UT_ASSERT_EQ(node_id, cluster_node_id);
	/* Every fresh attempt must still observe the injected rollover race. */
	if (test_current_segment_drift_on_recheck && test_current_segment_calls > 1)
		return test_current_segment + (uint32)test_current_segment_calls - 1;
	return test_current_segment;
}

bool
cluster_xid_is_mine(TransactionId xid)
{
	UT_ASSERT_EQ((int)xid, (int)TEST_ORIGIN_XID);
	return test_xid_is_mine;
}

bool
cluster_cr_server_local_freshref_c1b_pair_exact(TransactionId xid, uint32 expected_segment_id,
												uint32 expected_tt_slot_id, SCN proposed_scn,
												uint16 *out_wrap)
{
	test_local_freshref_pair_calls++;
	UT_ASSERT_EQ((int)xid, (int)TEST_ORIGIN_XID);
	UT_ASSERT_EQ(expected_segment_id, TEST_RECORD_SEGMENT);
	UT_ASSERT_EQ(expected_tt_slot_id, (uint32)TEST_TT_OFFSET + 1);
	UT_ASSERT_EQ((uint64)proposed_scn, (uint64)test_commit_scn);
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
	if (test_no_raw_reuse_drift_on_recheck && test_native_fence_lock_calls > 1)
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
cluster_undo_buf_copy_resident(uint32 segment_id, uint8 owner, uint32 block_no, char dst[BLCKSZ])
{
	test_candidate_data_copy_calls++;
	UT_ASSERT_EQ(segment_id, TEST_RECORD_SEGMENT);
	UT_ASSERT_EQ(owner, 1);
	UT_ASSERT_EQ(block_no, 7);
	if (test_candidate_data_copy_calls <= 2
		&& (test_candidate_cold_copy_mask & (1u << (test_candidate_data_copy_calls - 1))) != 0)
		return false;
	memset(dst, 0x5a, BLCKSZ);
	return true;
}

char *
cluster_undo_buf_pin(uint32 segment_id, uint8 owner, uint32 block_no, ClusterUndoBufMode mode,
					 ClusterUndoBufPin *pin)
{
	test_candidate_data_pin_calls++;
	UT_ASSERT_EQ(cluster_node_id, 0);
	UT_ASSERT_EQ(segment_id, TEST_RECORD_SEGMENT);
	UT_ASSERT_EQ(owner, 1);
	UT_ASSERT_EQ(block_no, 7);
	UT_ASSERT_EQ(mode, CLUSTER_UNDO_BUF_SHARED);
	UT_ASSERT_EQ(test_candidate_held_count, 1);
	pin->slot = -1;
	if (test_candidate_data_io_throw)
		pg_re_throw();
	if (test_candidate_data_pool_disabled)
		return NULL;
	pin->slot = 0;
	pin->mode = mode;
	memset(test_candidate_io_page.data, 0x5a, BLCKSZ);
	if (test_candidate_data_io_generation_drift && test_candidate_data_pin_calls == 2)
		test_candidate_data_generation++;
	return test_candidate_io_page.data;
}

void
cluster_undo_buf_unpin(ClusterUndoBufPin *pin)
{
	UT_ASSERT_EQ(pin->slot, 0);
	UT_ASSERT_EQ(pin->mode, CLUSTER_UNDO_BUF_SHARED);
	UT_ASSERT_EQ(test_candidate_held_count, 1);
	test_candidate_data_unpin_calls++;
	pin->slot = -1;
}

bool
cluster_cr_r4_extract_resident_record(const char resident_undo_page[BLCKSZ] pg_attribute_unused(),
									  const ClusterTxLocator *request_locator,
									  char record_out[BLCKSZ], size_t *record_length_out,
									  ClusterTxLocator *canonical_locator_out)
{
	test_candidate_extract_calls++;
	UT_ASSERT_EQ(request_locator->tt_wrap, TT_WRAP_INVALID);
	if (test_candidate_mutate_record_on_recheck && test_candidate_extract_calls == 2)
		test_origin_record.payload_length++;
	memcpy(record_out, &test_origin_record, sizeof(test_origin_record));
	memcpy(record_out + sizeof(test_origin_record), test_origin_payload, test_origin_payload_bytes);
	*record_length_out = sizeof(test_origin_record) + test_origin_payload_bytes;
	*canonical_locator_out = *request_locator;
	canonical_locator_out->tt_wrap = TEST_ORIGIN_WRAP;
	return true;
}

ClusterUndoBlock0CurrentStep
cluster_undo_block0_current_release_begin(ClusterUndoBlock0CurrentGuard *guard
											  pg_attribute_unused(),
										  ClusterUndoBlock0Result *failure pg_attribute_unused())
{
	test_candidate_release_calls++;
	if (test_updater_retry_observer != NULL)
		UT_ASSERT(!*test_updater_retry_observer);
	UT_ASSERT(test_candidate_held_count > 0);
	test_candidate_held_count--;
	if (test_candidate_release_fault_call != 0
		&& test_candidate_release_calls != test_candidate_release_fault_call)
		return CLUSTER_UNDO_BLOCK0_CURRENT_RELEASED;
	if (test_ctrc_revoke_on_release)
		test_ctrc_grant_revoked = true;
	if (test_candidate_release_admission_drift)
		test_admission_revoked = true;
	if (test_candidate_release_epoch_drift)
		test_formation_epoch++;
	switch (test_release_scope_drift) {
	case 1:
		test_variable_cache.oldestClogXid = TEST_ORIGIN_XID + 1;
		break;
	case 2:
		test_next_full_xid = FullTransactionIdFromEpochAndXid(1, 1000);
		break;
	case 3:
		test_boot_incarnation++;
		break;
	case 4:
		test_system_identifier++;
		break;
	case 5:
		test_current_segment++;
		break;
	case 6:
		test_candidate_root_drift = true;
		break;
	default:
		break;
	}
	switch (test_release_identity_drift) {
	case 1:
		test_current_owner_available = false;
		break;
	case 2:
		test_current_owner.wrap++;
		break;
	case 3:
		test_current_owner.segment_id++;
		break;
	case 4:
		test_current_owner.status = CTS_COMMITTED;
		test_current_owner.commit_scn = test_commit_scn;
		break;
	case 5:
		test_local_binding_available = false;
		break;
	case 6:
		test_local_binding.slot_wrap++;
		break;
	case 7:
		test_local_binding.segment_generation++;
		break;
	case 8:
		test_local_binding.active_lsn++;
		break;
	default:
		break;
	}
	if (test_candidate_release_failed)
		return CLUSTER_UNDO_BLOCK0_CURRENT_FAILED;
	return CLUSTER_UNDO_BLOCK0_CURRENT_RELEASED;
}

ClusterUndoBlock0CurrentStep
cluster_undo_block0_current_release_poll(ClusterUndoBlock0CurrentGuard *guard pg_attribute_unused(),
										 ClusterUndoBlock0Result *failure pg_attribute_unused())
{
	UT_ASSERT(false);
	return CLUSTER_UNDO_BLOCK0_CURRENT_FAILED;
}

void
cluster_undo_block0_current_cancel(ClusterUndoBlock0CurrentGuard *guard pg_attribute_unused())
{
	test_candidate_cancel_calls++;
}

ClusterTxOutcome
cluster_gcs_block_r4_tx_resolve_fetch_and_wait(int32 origin_node, const ClusterTxLocator *locator,
											   uint32 expected_physical_generation,
											   uint64 formation_epoch, ClusterTxResolution *out,
											   ClusterTxResolveReason *reason_out)
{
	UT_ASSERT(test_remote_origin_enabled);
	UT_ASSERT_EQ(origin_node, 0);
	UT_ASSERT_EQ(cluster_node_id, 1);
	UT_ASSERT_EQ(formation_epoch, test_formation_epoch);
	UT_ASSERT_EQ(memcmp(locator, &test_origin_locator, sizeof(*locator)), 0);
	test_remote_origin_calls++;
	test_remote_origin_generation = expected_physical_generation;
	if (test_remote_origin_verdict_page != NULL) {
		bool decoded = ClusterR4TxVerdictPageDecode(test_remote_origin_verdict_page, locator, out);

		UT_ASSERT(decoded);
		*reason_out = decoded ? CLUSTER_TX_RESOLVE_NONE : CLUSTER_TX_RESOLVE_PROTOCOL;
		if (test_remote_origin_revoke_admission)
			test_formation_epoch++;
		return decoded ? out->outcome : CLUSTER_TX_UNKNOWN;
	}
	/* Transport/proof validation is covered by the real GCS suite. This seam
	 * records what the real admitted consumer asks the exact origin to do. */
	memset(out, 0, sizeof(*out));
	out->locator_echo = *locator;
	out->outcome = test_remote_origin_outcome;
	/* A peer must never supply an in-process creation projection. */
	out->local_creation_target.valid = true;
	out->local_aborted_xmax_target.valid = true;
	out->local_original_full_xid = FullTransactionIdFromEpochAndXid(9, TEST_ORIGIN_XID);
	*reason_out = test_remote_origin_outcome == CLUSTER_TX_UNKNOWN
					  ? CLUSTER_TX_RESOLVE_AUTHORITY_UNAVAILABLE
					  : CLUSTER_TX_RESOLVE_NONE;
	if (test_remote_origin_revoke_admission)
		test_formation_epoch++;
	return test_remote_origin_outcome;
}

static void
reset_exact_origin_fixture(void)
{
	cluster_runtime_visibility_exact_counters_reset();
	memset(&test_origin_locator, 0, sizeof(test_origin_locator));
	test_origin_locator.uba = uba_encode(TEST_RECORD_SEGMENT, 7, TEST_TT_OFFSET, 400);
	test_origin_locator.xid = TEST_ORIGIN_XID;
	test_origin_locator.tt_wrap = TEST_ORIGIN_WRAP;
	test_origin_locator.itl_kind = ITL_FLAG_ACTIVE;
	test_origin_locator.itl_slot_index = 2;

	memset(&test_origin_record, 0, sizeof(test_origin_record));
	memset(test_origin_payload, 0, sizeof(test_origin_payload));
	test_origin_payload_bytes = 0;
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
	test_durable_locator_calls = 0;
	test_durable_locator_result = CLUSTER_TT_DURABLE_LOCATE_FOUND;
	test_durable_locator_segment = TEST_TT_SEGMENT;
	test_durable_locator_slot = TEST_TT_OFFSET;
	test_durable_locator_wrap = TEST_ORIGIN_WRAP;
	test_durable_locator_status = TT_SLOT_COMMITTED;
	test_next_full_xid = FullTransactionIdFromEpochAndXid(0, 1000);
	test_next_full_on_locate = InvalidFullTransactionId;
	test_oldest_clog_on_locate = InvalidTransactionId;
	test_release_scope_drift = 0;
	test_release_identity_drift = 0;
	test_system_identifier = UINT64_C(0x1122334455667788);
	test_boot_incarnation = UINT64_C(9001);
	test_original_available = false;
	test_original_full_xid = InvalidFullTransactionId;
	memset(&test_original_key, 0, sizeof(test_original_key));
	test_original_calls = 0;
	test_original_drift = 0;
	g_retained_origin_available = false;
	g_retained_origin_full = InvalidFullTransactionId;
	g_retained_origin_reads = 0;
	g_retained_origin_drift = 0;
	memset(&g_retained_origin_key, 0, sizeof(g_retained_origin_key));
	test_candidate_copy_generation_drift = false;
	test_candidate_root_drift = false;
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
	test_candidate_cold_copy_mask = 0;
	test_candidate_data_pin_calls = 0;
	test_candidate_data_unpin_calls = 0;
	test_candidate_data_pool_disabled = false;
	test_candidate_data_io_throw = false;
	test_candidate_data_io_generation_drift = false;
	test_candidate_release_calls = 0;
	test_candidate_release_failed = false;
	test_candidate_release_epoch_drift = false;
	test_candidate_release_admission_drift = false;
	test_candidate_release_fault_call = 0;
	test_admission_revoked = false;
	test_updater_retry_observer = NULL;
	test_candidate_cancel_calls = 0;
	test_candidate_poll_calls = 0;
	test_candidate_wait_calls = 0;
	test_candidate_wakes_before_terminal = 1;
	memset(test_vis_evidence, 0, sizeof(test_vis_evidence));
	test_candidate_exit_hooks_ensure_calls = 0;
	memset(test_candidate_acquire_segments, 0, sizeof(test_candidate_acquire_segments));
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
	test_ctrc_touch_result = CLUSTER_CTRC_TOUCH_RECORDED;
	test_ctrc_touch_segment = TEST_TT_SEGMENT;
	test_ctrc_touch_generation = 23;
	memset(&test_ctrc_touched_key, 0, sizeof(test_ctrc_touched_key));
	memset(&test_ctrc_touched_participant, 0, sizeof(test_ctrc_touched_participant));
	test_ctrc_touch_generation_drift = false;
	test_ctrc_revoke_on_release = false;
	test_ctrc_grant_revoked = false;
	test_now = 0;
	test_mx_batch_enabled = false;
	test_mx_remote_calls = 0;
	test_mx_remote_failed = false;
	test_mx_remote_revokes_grant = false;
	test_mx_remote_epoch_drift = false;
	test_mx_remote_identity_mismatch = false;
	test_mx_remote_omits_proof = false;
	test_mx_remote_invalid_commit_scn = false;
	test_mx_remote_committed_with_grant = false;
	test_undo_pending_prepare_calls = 0;
	test_undo_pending_cancel_calls = 0;
	memset(&test_undo_pending_key, 0, sizeof(test_undo_pending_key));
	memset(&test_undo_pending_participant, 0, sizeof(test_undo_pending_participant));
	memset(&test_undo_pending_publication, 0, sizeof(test_undo_pending_publication));
	memset(&test_undo_pending_target, 0, sizeof(test_undo_pending_target));
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
	test_local_origin_bound = InvalidScn;
	test_local_origin_exact = InvalidScn;
	test_local_ordinary_abort = false;
	test_local_last_ordinary_single = false;
	test_local_origin_calls = 0;
	test_local_origin_epoch_drift = false;
	test_remote_origin_enabled = false;
	test_remote_origin_calls = 0;
	test_remote_origin_generation = 0;
	test_remote_origin_outcome = CLUSTER_TX_IN_PROGRESS;
	test_remote_origin_revoke_admission = false;
	test_remote_origin_verdict_page = NULL;
	test_receiver_registered = false;
	memset(&test_receiver_retained, 0, sizeof(test_receiver_retained));
	test_receiver_peer_boot = test_receiver_peer_generation = test_receiver_peer_epoch = 0;
	test_receiver_self_admitted = 0;
	test_receiver_checks = test_receiver_drift = 0;
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
	const ClusterSemanticAdmissionToken *admission, ClusterUndoBlock0CurrentGuard *guard,
	const ClusterUndoBlock0ResolvedRoot *root, ClusterTTStatusKey *key_out,
	ClusterTTStatusResult *result_out, bool *ctrc_physical_active_out);

static void
run_pair(unsigned int pair)
{
	ClusterTxOutcome outcome = (ClusterTxOutcome)(pair / 8);
	ClusterTxProofKind proof = (ClusterTxProofKind)(pair % 8);

	UT_ASSERT_EQ(cluster_tx_outcome_proof_is_valid(outcome, proof), expected[outcome][proof]);
}

#define DEFINE_PAIR_TEST(n)                                                                        \
	UT_TEST(test_outcome_proof_pair_##n)                                                           \
	{                                                                                              \
		run_pair(n);                                                                               \
	}

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
		TEST_ORIGIN_XID, &test_current_owner, &admission, &guard, &root, &key, &result, NULL));
	UT_ASSERT_EQ(key.origin_node_id, (uint16)cluster_node_id);
	UT_ASSERT_EQ(key.undo_segment_id, (uint16)TEST_TT_SEGMENT);
	UT_ASSERT_EQ(key.tt_slot_id, cluster_tt_slot_offset_to_id(TEST_TT_OFFSET));
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
		TEST_ORIGIN_XID, &test_current_owner, &admission, &guard, &root, &key, &result, NULL));
	UT_ASSERT_EQ(result.status, CLUSTER_TT_STATUS_UNKNOWN);
	UT_ASSERT(!result.authoritative);

	/* A physical wrap mismatch and a post-sample allocator drift both remain
	 * fail closed; neither can be repaired from raw xid/native state. */
	test_candidate_copy_physical_slot = true;
	test_tt_slot.wrap++;
	UT_ASSERT(!cluster_runtime_visibility_current_owner_sample_held(
		TEST_ORIGIN_XID, &test_current_owner, &admission, &guard, &root, &key, &result, NULL));
	test_tt_slot.wrap = TEST_ORIGIN_WRAP;
	/* The shared fixture mutates on its third owner sample. */
	test_current_owner_calls = 2;
	test_current_owner_drift_on_recheck = true;
	UT_ASSERT(!cluster_runtime_visibility_current_owner_sample_held(
		TEST_ORIGIN_XID, &test_current_owner, &admission, &guard, &root, &key, &result, NULL));
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
		TEST_ORIGIN_XID, &test_current_owner, &admission, &guard, &root, &key, &result,
		&physical_active));
	UT_ASSERT_EQ(result.status, CLUSTER_TT_STATUS_IN_PROGRESS);
	UT_ASSERT_EQ(result.commit_scn, InvalidScn);
	UT_ASSERT(!physical_active);

	test_tt_slot.status = TT_SLOT_ACTIVE;
	test_tt_slot.commit_scn = InvalidScn;
	test_current_owner_calls = 0;
	UT_ASSERT(cluster_runtime_visibility_current_owner_sample_held(
		TEST_ORIGIN_XID, &test_current_owner, &admission, &guard, &root, &key, &result,
		&physical_active));
	UT_ASSERT(physical_active);
}

/* The old-segment physical sampler exposes the exact precommit observation to
 * its consumer only when canonical bytes carry a valid commit SCN while the
 * native xid remains live.  A true ACTIVE slot is the CTRC-ready proof and
 * must clear that retry hint. */
UT_TEST(test_physical_locator_precommit_retry_marks_committed_window)
{
	ClusterSemanticAdmissionToken admission;
	ClusterUndoBlock0CurrentGuard guard;
	ClusterUndoBlock0ResolvedRoot root;
	ClusterTTSlotPhysicalLocator locator;
	ClusterTTStatusKey key;
	ClusterTTStatusResult result;
	bool physical_active = true;
	bool precommit_retry = false;

	reset_exact_origin_fixture();
	test_native_status = TRANSACTION_STATUS_IN_PROGRESS;
	test_twophase_xid = TEST_ORIGIN_XID;
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
		&locator, &admission, &guard, &root, &key, &result, &physical_active,
		&precommit_retry));
	UT_ASSERT_EQ(result.status, CLUSTER_TT_STATUS_IN_PROGRESS);
	UT_ASSERT(!physical_active);
	UT_ASSERT(precommit_retry);
	UT_ASSERT_EQ(result.commit_scn, InvalidScn);

	test_tt_slot.status = TT_SLOT_ACTIVE;
	test_tt_slot.commit_scn = InvalidScn;
	physical_active = false;
	precommit_retry = true;
	UT_ASSERT(cluster_runtime_visibility_physical_locator_sample_held(
		&locator, &admission, &guard, &root, &key, &result, &physical_active,
		&precommit_retry));
	UT_ASSERT_EQ(result.status, CLUSTER_TT_STATUS_IN_PROGRESS);
	UT_ASSERT(physical_active);
	UT_ASSERT(!precommit_retry);
}

/* A backend-local canonical binding remains the exact owner locator for the
 * whole xid lifetime even after retention pressure rolls CURRENT forward.
 * CTRC PREPARE must therefore sample the binding's old physical segment,
 * validate its captured generation, and never substitute a fresh CURRENT
 * allocator scan. */
UT_TEST(test_current_mx_malformed_active_is_not_precommit_retry)
{
	ClusterTTStatusKey key;
	ClusterTTStatusResult result;
	ClusterCtrcTxnKeyV1 ctrc_key;
	uint32 grant = 0;
	uint32 capability = 0;

	reset_exact_origin_fixture();
	test_native_status = TRANSACTION_STATUS_IN_PROGRESS;
	test_twophase_xid = TEST_ORIGIN_XID;
	test_tt_slot.status = TT_SLOT_ACTIVE;
	/* Retain the invalid ACTIVE/commit-SCN combination in a rolled slot. */
	test_current_segment = TEST_TT_SEGMENT + 1;
	UT_ASSERT(current_mx_local_member_sample_exact(TEST_ORIGIN_XID, &key, &result, &grant,
												   &capability, &ctrc_key)
			  != CMX_RESOLVE_RETRY);
	UT_ASSERT_EQ(test_candidate_held_count, 0);
	UT_ASSERT_EQ(result.status, CLUSTER_TT_STATUS_UNKNOWN);
	UT_ASSERT(!result.authoritative);
	UT_ASSERT_EQ(grant, 0);
	UT_ASSERT_EQ(test_ctrc_touch_calls, 0);
}

UT_TEST(test_current_mx_local_precommit_returns_retry_after_guard_release)
{
	ClusterTTStatusKey key;
	ClusterTTStatusResult result;
	ClusterCtrcTxnKeyV1 ctrc_key;
	uint32 grant = 0;
	uint32 capability = 0;

	reset_exact_origin_fixture();
	test_native_status = TRANSACTION_STATUS_IN_PROGRESS;
	test_twophase_xid = TEST_ORIGIN_XID;
	test_current_owner_available = true;
	UT_ASSERT_EQ(current_mx_local_member_sample_exact(TEST_ORIGIN_XID, &key, &result, &grant,
													  &capability, &ctrc_key),
				 CMX_RESOLVE_RETRY);
	UT_ASSERT_EQ(test_candidate_held_count, 0);
	UT_ASSERT_EQ(test_candidate_max_held_count, 1);
	UT_ASSERT_EQ(test_candidate_release_calls, test_candidate_acquire_calls);
	UT_ASSERT_EQ(test_ctrc_touch_calls, 0);
	UT_ASSERT_EQ(grant, 0);
	UT_ASSERT_EQ(capability, 0);
	UT_ASSERT_EQ(result.status, CLUSTER_TT_STATUS_UNKNOWN);
	UT_ASSERT(!result.authoritative);
}

UT_TEST(test_current_mx_precommit_resamples_exact_terminal_without_grant)
{
	ClusterTTStatusKey key;
	ClusterTTStatusResult result;
	ClusterCtrcTxnKeyV1 ctrc_key;
	uint32 grant;
	uint32 capability;
	int pass;

	for (pass = 0; pass < 2; pass++) {
		reset_exact_origin_fixture();
		test_native_status = TRANSACTION_STATUS_IN_PROGRESS;
		test_twophase_xid = TEST_ORIGIN_XID;
		test_current_segment = TEST_TT_SEGMENT + 1;
		UT_ASSERT_EQ(current_mx_local_member_sample_exact(TEST_ORIGIN_XID, &key, &result, &grant,
														  &capability, &ctrc_key),
					 CMX_RESOLVE_RETRY);
		/* Writer progress needs X: change no TT/CLOG evidence until SCUR retired. */
		UT_ASSERT_EQ(test_candidate_held_count, 0);
		if (pass == 0)
			test_native_status = TRANSACTION_STATUS_COMMITTED;
		else {
			/* CLOG-only abort conflicts with COMMITTED TT; it is not retryable. */
			test_native_status = TRANSACTION_STATUS_ABORTED;
			UT_ASSERT_EQ(current_mx_local_member_sample_exact(TEST_ORIGIN_XID, &key, &result,
															  &grant, &capability, &ctrc_key),
						 CMX_RESOLVE_UNKNOWN);
			test_tt_slot.status = TT_SLOT_ABORTED;
			test_tt_slot.commit_scn = InvalidScn;
		}
		UT_ASSERT_EQ(current_mx_local_member_sample_exact(TEST_ORIGIN_XID, &key, &result, &grant,
														  &capability, &ctrc_key),
					 CMX_RESOLVE_OK);
		UT_ASSERT_EQ(result.status,
					 pass == 0 ? CLUSTER_TT_STATUS_COMMITTED : CLUSTER_TT_STATUS_ABORTED);
		UT_ASSERT(result.authoritative);
		UT_ASSERT_EQ(grant, 0);
		UT_ASSERT_EQ(capability, 0);
		UT_ASSERT_EQ(test_ctrc_touch_calls, 0);
		UT_ASSERT_EQ(test_candidate_held_count, 0);
	}
}

UT_TEST(test_current_mx_precommit_keeps_unknown_and_release_fences)
{
	ClusterTTStatusKey key;
	ClusterTTStatusResult result;
	ClusterCtrcTxnKeyV1 ctrc_key;
	uint32 grant;
	uint32 capability;
	bool retry;
	int variant;

	for (variant = 0; variant < 6; variant++) {
		reset_exact_origin_fixture();
		test_native_status = TRANSACTION_STATUS_IN_PROGRESS;
		test_twophase_xid = TEST_ORIGIN_XID;
		test_current_segment = TEST_TT_SEGMENT + 1;
		switch (variant) {
		case 0:
			test_twophase_prepared = true;
			break;
		case 1:
			test_tt_slot.commit_scn = InvalidScn;
			break;
		case 2:
			test_tt_slot.wrap++;
			break;
		case 3:
			test_current_segment_drift_on_recheck = true;
			break;
		case 4:
			test_candidate_sample_result = CLUSTER_UNDO_BLOCK0_AUTHORITY_DENIED;
			break;
		case 5:
			test_tt_slot.status = TT_SLOT_ACTIVE;
			test_tt_slot.commit_scn = InvalidScn;
			test_current_owner_available = true;
			test_current_segment = TEST_TT_SEGMENT;
			test_ctrc_touch_grant = 0;
			break;
		}
		UT_ASSERT_EQ(current_mx_local_member_sample_exact(TEST_ORIGIN_XID, &key, &result, &grant,
														  &capability, &ctrc_key),
					 CMX_RESOLVE_UNKNOWN);
		UT_ASSERT_EQ(result.status, CLUSTER_TT_STATUS_UNKNOWN);
		UT_ASSERT(!result.authoritative);
		UT_ASSERT_EQ(test_candidate_held_count, 0);
	}
	for (variant = 0; variant < 2; variant++) {
		reset_exact_origin_fixture();
		test_native_status = TRANSACTION_STATUS_IN_PROGRESS;
		test_twophase_xid = TEST_ORIGIN_XID;
		test_current_segment = TEST_TT_SEGMENT + 1;
		test_candidate_release_failed = variant == 0;
		test_candidate_release_epoch_drift = variant == 1;
		retry = true;
		UT_ASSERT(!cluster_runtime_visibility_local_terminal_lookup_exact(TEST_ORIGIN_XID, &key,
																		  &result, &retry));
		UT_ASSERT(!retry);
		UT_ASSERT_EQ(result.status, CLUSTER_TT_STATUS_UNKNOWN);
		UT_ASSERT_EQ(test_candidate_held_count, 0);
		UT_ASSERT_EQ(test_candidate_cancel_calls, variant == 0 ? 1 : 0);
	}
}

static void
check_local_updater_sample(bool expected_proof, bool expected_retry, bool optional_flag)
{
	ClusterTTStatusKey key;
	ClusterTTStatusResult result;
	ClusterCtrcTxnKeyV1 ctrc_key;
	ClusterTxLocator canonical;
	uint32 grant = 99;
	uint32 capability = 99;
	bool cross_segment = true;
	bool retry = true;
	bool sampled;

	memset(&key, 0xa5, sizeof(key));
	memset(&result, 0xa5, sizeof(result));
	memset(&ctrc_key, 0xa5, sizeof(ctrc_key));
	memset(&canonical, 0xa5, sizeof(canonical));
	test_updater_retry_observer = optional_flag ? NULL : &retry;
	sampled = cluster_runtime_visibility_current_mx_updater_provenance_exact(
		&test_origin_locator, 0, &key, &result, &grant, &capability, &ctrc_key, &canonical,
		&cross_segment, optional_flag ? NULL : &retry);
	test_updater_retry_observer = NULL;
	UT_ASSERT_EQ(sampled, expected_proof);
	if (!optional_flag)
		UT_ASSERT_EQ(retry, expected_retry);
	UT_ASSERT_EQ(test_candidate_held_count, 0);
	UT_ASSERT_EQ(grant, 0);
	UT_ASSERT_EQ(capability, 0);
	UT_ASSERT_EQ(test_ctrc_touch_calls, 0);
	UT_ASSERT_EQ(ctrc_key.xid, 0);
	if (sampled) {
		UT_ASSERT(result.authoritative);
		UT_ASSERT_EQ(result.status, test_native_status == TRANSACTION_STATUS_COMMITTED
										? CLUSTER_TT_STATUS_COMMITTED
										: CLUSTER_TT_STATUS_ABORTED);
		UT_ASSERT_EQ(canonical.xid, TEST_ORIGIN_XID);
		UT_ASSERT_EQ(canonical.tt_wrap, TEST_ORIGIN_WRAP);
		UT_ASSERT_EQ(cross_segment, test_origin_record.tt_slot_segment_id != TEST_RECORD_SEGMENT);
	} else {
		UT_ASSERT_EQ(result.status, CLUSTER_TT_STATUS_UNKNOWN);
		UT_ASSERT(!result.authoritative);
		UT_ASSERT_EQ(result.commit_scn, InvalidScn);
		UT_ASSERT_EQ(key.local_xid, 0);
		UT_ASSERT_EQ(canonical.xid, 0);
		UT_ASSERT_EQ(canonical.uba.raw[0], 0);
		UT_ASSERT_EQ(canonical.uba.raw[1], 0);
		UT_ASSERT(!cross_segment);
	}
}

UT_TEST(test_local_updater_precommit_retries_only_after_guard_release)
{
	int cross_segment;
	int optional_flag;

	for (cross_segment = 0; cross_segment < 2; cross_segment++) {
		for (optional_flag = 0; optional_flag < 2; optional_flag++) {
			reset_exact_origin_fixture();
			test_origin_locator.tt_wrap = TT_WRAP_INVALID;
			test_origin_record.tt_slot_segment_id
				= cross_segment ? TEST_TT_SEGMENT : TEST_RECORD_SEGMENT;
			test_native_status = TRANSACTION_STATUS_IN_PROGRESS;
			test_twophase_xid = TEST_ORIGIN_XID;
			check_local_updater_sample(false, true, optional_flag != 0);
			UT_ASSERT_EQ(test_candidate_acquire_calls, cross_segment ? 2 : 1);
			UT_ASSERT_EQ(test_candidate_release_calls, test_candidate_acquire_calls);
			UT_ASSERT_EQ(test_candidate_max_held_count, 1);
			UT_ASSERT_EQ(test_by_xid_scan_calls, 0);
		}
	}
}

UT_TEST(test_local_updater_precommit_release_and_admission_failure_stay_unknown)
{
	int cross_segment;
	int fault;

	for (cross_segment = 0; cross_segment < 2; cross_segment++) {
		for (fault = 0; fault < 3; fault++) {
			reset_exact_origin_fixture();
			test_origin_locator.tt_wrap = TT_WRAP_INVALID;
			test_origin_record.tt_slot_segment_id
				= cross_segment ? TEST_TT_SEGMENT : TEST_RECORD_SEGMENT;
			test_native_status = TRANSACTION_STATUS_IN_PROGRESS;
			test_twophase_xid = TEST_ORIGIN_XID;
			test_candidate_release_fault_call = cross_segment ? 2 : 1;
			test_candidate_release_failed = fault == 0;
			test_candidate_release_epoch_drift = fault == 1;
			test_candidate_release_admission_drift = fault == 2;
			check_local_updater_sample(false, false, false);
			UT_ASSERT_EQ(test_candidate_release_calls, cross_segment ? 2 : 1);
			UT_ASSERT_EQ(test_candidate_cancel_calls, fault == 0 ? 1 : 0);
		}
	}
}

UT_TEST(test_local_updater_does_not_retry_unproven_samples)
{
	int cross_segment;
	int fault;

	for (cross_segment = 0; cross_segment < 2; cross_segment++) {
		for (fault = 0; fault < 8; fault++) {
			reset_exact_origin_fixture();
			test_origin_locator.tt_wrap = TT_WRAP_INVALID;
			test_origin_record.tt_slot_segment_id
				= cross_segment ? TEST_TT_SEGMENT : TEST_RECORD_SEGMENT;
			test_native_status = TRANSACTION_STATUS_IN_PROGRESS;
			test_twophase_xid = TEST_ORIGIN_XID;
			switch (fault) {
			case 0:
				test_twophase_prepared = true;
				break;
			case 1:
				test_tt_slot.commit_scn = InvalidScn;
				break;
			case 2:
				test_tt_slot.wrap++;
				break;
			case 3:
				test_tt_slot.status = TT_SLOT_ACTIVE;
				break;
			case 4:
				test_native_status = TRANSACTION_STATUS_ABORTED;
				break;
			case 5:
				test_candidate_sample_result = CLUSTER_UNDO_BLOCK0_AUTHORITY_DENIED;
				break;
			case 6:
				test_origin_locator.tt_wrap = TEST_ORIGIN_WRAP;
				break;
			case 7:
				test_tt_slot.xid++;
				break;
			}
			check_local_updater_sample(false, false, false);
		}
	}
}

UT_TEST(test_local_updater_precommit_resamples_terminal_without_grant)
{
	int cross_segment;
	int aborted;

	for (cross_segment = 0; cross_segment < 2; cross_segment++) {
		for (aborted = 0; aborted < 2; aborted++) {
			reset_exact_origin_fixture();
			test_origin_locator.tt_wrap = TT_WRAP_INVALID;
			test_origin_record.tt_slot_segment_id
				= cross_segment ? TEST_TT_SEGMENT : TEST_RECORD_SEGMENT;
			test_native_status = TRANSACTION_STATUS_IN_PROGRESS;
			test_twophase_xid = TEST_ORIGIN_XID;
			check_local_updater_sample(false, true, false);
			/* SCUR is gone: model the writer's exact terminal publication. */
			test_native_status
				= aborted ? TRANSACTION_STATUS_ABORTED : TRANSACTION_STATUS_COMMITTED;
			if (aborted) {
				test_tt_slot.status = TT_SLOT_ABORTED;
				test_tt_slot.commit_scn = InvalidScn;
			}
			check_local_updater_sample(true, false, false);
			UT_ASSERT_EQ(test_candidate_release_calls, test_candidate_acquire_calls);
		}
	}
}

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
		TEST_ORIGIN_XID, &status_key, &status, &grant, &ctrc_key, &participant));
	UT_ASSERT_EQ(status.status, CLUSTER_TT_STATUS_IN_PROGRESS);
	UT_ASSERT(status.authoritative);
	UT_ASSERT_EQ(grant, test_ctrc_touch_grant);
	UT_ASSERT_EQ(status_key.undo_segment_id, (uint16)TEST_TT_SEGMENT);
	UT_ASSERT_EQ(ctrc_key.segment_id, TEST_TT_SEGMENT);
	UT_ASSERT_EQ(ctrc_key.segment_generation, test_local_binding.segment_generation);
	UT_ASSERT_EQ(ctrc_key.slot_offset, TEST_TT_OFFSET);
	UT_ASSERT_EQ(ctrc_key.slot_wrap, TEST_ORIGIN_WRAP);
	UT_ASSERT_EQ(test_local_binding_calls, 3); /* Initial, held, then released. */
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
		TEST_ORIGIN_XID, &status_key, &status, &grant, &ctrc_key, &participant));
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
		TEST_ORIGIN_XID, &status_key, &status, &grant, &ctrc_key, &participant));
	UT_ASSERT_EQ(test_ctrc_touch_calls, 0);
}

static void
reset_rolled_active_fixture(void)
{
	reset_exact_origin_fixture();
	test_native_status = TRANSACTION_STATUS_IN_PROGRESS;
	test_twophase_xid = TEST_ORIGIN_XID;
	test_tt_slot.status = TT_SLOT_ACTIVE;
	test_tt_slot.commit_scn = InvalidScn;
	/* The querying backend has no published binding or CURRENT index entry. */
	test_local_binding_available = false;
	test_current_owner_available = false;
	test_current_segment = TEST_TT_SEGMENT + 1;
}

static void
assert_rolled_lookup_unknown(void)
{
	ClusterTTStatusKey key;
	ClusterTTStatusResult result;
	ClusterCtrcTxnKeyV1 ctrc_key;
	ClusterCtrcParticipantIdentity participant;
	ClusterTTStatusKey empty_key = { 0 };
	ClusterCtrcTxnKeyV1 empty_ctrc_key = { 0 };
	ClusterCtrcParticipantIdentity empty_participant = { 0 };
	uint32 grant = 99;

	memset(&key, 0xa5, sizeof(key));
	memset(&result, 0xa5, sizeof(result));
	memset(&ctrc_key, 0xa5, sizeof(ctrc_key));
	memset(&participant, 0xa5, sizeof(participant));
	UT_ASSERT(!cluster_runtime_visibility_current_owner_lookup_exact_ctrc_full(
		TEST_ORIGIN_XID, &key, &result, &grant, &ctrc_key, &participant));
	UT_ASSERT_EQ(result.status, CLUSTER_TT_STATUS_UNKNOWN);
	UT_ASSERT_EQ(result.commit_scn, InvalidScn);
	UT_ASSERT(!result.authoritative);
	UT_ASSERT_EQ(grant, 0);
	UT_ASSERT(memcmp(&key, &empty_key, sizeof(key)) == 0);
	UT_ASSERT(memcmp(&ctrc_key, &empty_ctrc_key, sizeof(ctrc_key)) == 0);
	UT_ASSERT(memcmp(&participant, &empty_participant, sizeof(participant)) == 0);
	UT_ASSERT_EQ(test_candidate_held_count, 0);
	UT_ASSERT_EQ(test_xact_lock_depth, 0);
}

static void
reset_rolled_updater_fixture(bool cross_segment)
{
	reset_rolled_active_fixture();
	test_origin_locator.tt_wrap = TT_WRAP_INVALID;
	test_origin_record.tt_slot_segment_id = cross_segment ? TEST_TT_SEGMENT : TEST_RECORD_SEGMENT;
	test_ctrc_touch_segment = cross_segment ? TEST_TT_SEGMENT : TEST_RECORD_SEGMENT;
	test_ctrc_touch_generation = cross_segment ? 23 : 17;
}

void undo_pending_test_reserve(ClusterUndoRecordPrepareReceipt *receipt, uint16 segment,
							   uint16 offset, uint8 pending_mask);
bool undo_pending_test_receipt_matches(const ClusterUndoRecordPrepareReceipt *receipt);

static void
assert_undo_pending_refused(ClusterUndoRecordPrepareReceipt *receipt)
{
	ClusterUndoRecordPrepareReceipt original = *receipt;

	UT_ASSERT(!cluster_undo_record_ctrc_prepare_pending(receipt, 0));
	UT_ASSERT(memcmp(receipt, &original, sizeof(*receipt)) == 0);
	UT_ASSERT_EQ(test_undo_pending_prepare_calls, 0);
	UT_ASSERT_EQ(test_undo_pending_cancel_calls, 0);
	UT_ASSERT_EQ(test_candidate_held_count, 0);
	UT_ASSERT_EQ(test_xact_lock_depth, 0);
	UT_ASSERT_EQ(test_native_fence_depth, 0);
}

UT_TEST(test_retained_active_identity_reaches_real_undo_pending)
{
	int binding;

	for (binding = 0; binding < 2; binding++) {
		ClusterUndoRecordPrepareReceipt receipt;
		uint8 ordinal;

		reset_rolled_active_fixture();
		test_local_binding_available = binding != 0;
		undo_pending_test_reserve(&receipt, TEST_TT_SEGMENT, TEST_TT_OFFSET, 3);
		for (ordinal = 0; ordinal < 2; ordinal++) {
			uint8 bit = UINT8_C(1) << ordinal;

			UT_ASSERT(cluster_undo_record_ctrc_prepare_pending(&receipt, ordinal));
			UT_ASSERT(undo_pending_test_receipt_matches(&receipt));
			UT_ASSERT(receipt.ctrc_handles[ordinal].valid);
			UT_ASSERT((receipt.ctrc_prepared_mask & bit) != 0);
			UT_ASSERT(memcmp(&test_undo_pending_key, &test_ctrc_touched_key,
							 sizeof(test_undo_pending_key))
					  == 0);
			UT_ASSERT(memcmp(&test_undo_pending_participant, &test_ctrc_touched_participant,
							 sizeof(test_undo_pending_participant))
					  == 0);
			UT_ASSERT_EQ(test_undo_pending_key.segment_id, TEST_TT_SEGMENT);
			UT_ASSERT_EQ(test_undo_pending_key.slot_offset, TEST_TT_OFFSET);
			UT_ASSERT_EQ(test_undo_pending_key.slot_wrap, TEST_ORIGIN_WRAP);
			UT_ASSERT_EQ(test_undo_pending_publication.requester_node_id, 0);
			UT_ASSERT_EQ(test_undo_pending_publication.requester_boot_incarnation,
						 test_boot_incarnation);
			UT_ASSERT_EQ(test_undo_pending_publication.capability_record_generation, 77);
			UT_ASSERT_EQ(test_undo_pending_publication.requester_backend_id, MyBackendId);
			UT_ASSERT_EQ(test_undo_pending_publication.wire_request_id,
						 receipt.reservation_sequence);
			UT_ASSERT_EQ(test_undo_pending_publication.operation_id,
						 receipt.reservation_sequence + ordinal);
			UT_ASSERT_EQ(test_undo_pending_publication.attempt_generation, 3);
			UT_ASSERT_EQ(test_undo_pending_publication.reference_kind, CTRC_REF_HEAP_ITL_UBA);
			UT_ASSERT_EQ(test_undo_pending_publication.target_kind,
						 CTRC_TARGET_PAGE_PENDING_ITL_SLOT);
			UT_ASSERT_EQ(test_undo_pending_publication.grant_generation, test_ctrc_touch_grant);
			UT_ASSERT_EQ(test_undo_pending_target.block_number, 37 + ordinal);
			UT_ASSERT_EQ(receipt.absolute_deadline_us, 1000);
		}
		UT_ASSERT_EQ(receipt.ctrc_prepared_mask, 3);
		UT_ASSERT_EQ(receipt.ctrc_applied_mask, 0);
		UT_ASSERT_EQ(test_undo_pending_prepare_calls, 2);
		UT_ASSERT_EQ(test_undo_pending_cancel_calls, 0);
		UT_ASSERT_EQ(test_durable_locator_calls, binding ? 0 : 2);
		UT_ASSERT(!test_current_owner_available);
		UT_ASSERT_EQ(test_candidate_acquire_calls, test_candidate_release_calls);
		UT_ASSERT_EQ(test_candidate_held_count, 0);
		UT_ASSERT_EQ(test_xact_lock_depth, 0);
	}
}

UT_TEST(test_retained_undo_pending_requires_matching_active_tt_receipt)
{
	int fault;

	for (fault = 0; fault < 8; fault++) {
		ClusterUndoRecordPrepareReceipt receipt;

		reset_rolled_active_fixture();
		undo_pending_test_reserve(&receipt, TEST_TT_SEGMENT + (fault == 0),
								  TEST_TT_OFFSET + (fault == 1), 1);
		switch (fault) {
		case 2:
			test_ctrc_touch_grant = 0;
			break;
		case 3:
		case 4:
			test_tt_slot.status = TT_SLOT_COMMITTED;
			test_tt_slot.commit_scn = test_commit_scn;
			if (fault == 4)
				test_native_status = TRANSACTION_STATUS_COMMITTED;
			break;
		case 5:
			test_tt_slot.status = TT_SLOT_ABORTED;
			test_native_status = TRANSACTION_STATUS_ABORTED;
			break;
		case 6:
			test_twophase_prepared = true;
			break;
		case 7:
			receipt.reservation_sequence++;
			break;
		default:
			break;
		}
		assert_undo_pending_refused(&receipt);
		UT_ASSERT_EQ(test_ctrc_touch_calls, fault <= 2 ? 1 : 0);
		UT_ASSERT_EQ(test_candidate_acquire_calls, test_candidate_release_calls);
	}
}

UT_TEST(test_retained_undo_pending_guard_drift_cannot_publish)
{
	int fault;

	for (fault = 0; fault < 12; fault++) {
		ClusterUndoRecordPrepareReceipt receipt;

		reset_rolled_active_fixture();
		undo_pending_test_reserve(&receipt, TEST_TT_SEGMENT, TEST_TT_OFFSET, 1);
		switch (fault) {
		case 0:
			test_tt_slot.wrap++;
			break;
		case 1:
			test_candidate_copy_generation_drift = true;
			break;
		case 2:
			test_ctrc_touch_generation_drift = true;
			break;
		case 3:
			test_candidate_release_admission_drift = true;
			break;
		case 4:
			test_candidate_release_failed = true;
			break;
		case 5:
			test_candidate_release_epoch_drift = true;
			break;
		default:
			test_release_scope_drift = fault - 5;
			break;
		}
		assert_undo_pending_refused(&receipt);
		UT_ASSERT(undo_pending_test_receipt_matches(&receipt));
		UT_ASSERT_EQ(test_candidate_acquire_calls, test_candidate_release_calls);
		UT_ASSERT_EQ(test_candidate_cancel_calls, fault == 4 ? 1 : 0);
	}
}

static ClusterMxResolveResult
resolve_rolled_mx_batch(bool updater, bool foreign_member, ClusterCurrentMemberProof *proofs,
						ClusterCurrentUpdaterProof *updater_proof, uint32 *capabilities,
						TimestampTz *deadline)
{
	ClusterCurrentMxKey key = { 0 };
	ClusterCurrentMxMemberDesc members[2] = { 0 };
	ClusterCurrentUpdaterChallenge challenge = { 0 };
	uint16 count = foreign_member ? 2 : 1;
	uint64 hash;

	UT_ASSERT(!updater || !foreign_member);
	test_mx_batch_enabled = true;
	key.multixact_id = 17;
	key.cluster_epoch = (uint32)test_formation_epoch;
	members[0].xid = TEST_ORIGIN_XID;
	members[0].member_status = updater ? MultiXactStatusNoKeyUpdate : MultiXactStatusForShare;
	if (foreign_member) {
		members[1].xid = TEST_ORIGIN_XID + 1;
		members[1].member_status = MultiXactStatusForKeyShare;
	}
	challenge.candidate_next_xmin_alias.undo_record_segment_id = TEST_RECORD_SEGMENT;
	challenge.candidate_next_xmin_alias.tt_slot_id = cluster_tt_slot_offset_to_id(TEST_TT_OFFSET);
	challenge.candidate_next_xmin_alias.cluster_epoch = key.cluster_epoch;
	challenge.candidate_next_xmin_alias.local_xid = TEST_ORIGIN_XID;
	challenge.candidate_next_xmin_locator = test_origin_locator;
	challenge.updater_xid = TEST_ORIGIN_XID;
	hash = cluster_multixact_current_descriptor_hash(&key, members, count);
	memset(proofs, 0xa5, sizeof(*proofs) * count);
	memset(updater_proof, 0xa5, sizeof(*updater_proof));
	memset(capabilities, 0xa5, sizeof(*capabilities) * count);
	return cluster_multixact_current_members_resolve_until(&key, members, count, hash,
														   updater ? &challenge : NULL, proofs,
														   updater_proof, capabilities, deadline);
}

static void
assert_mx_batch_unknown(const ClusterCurrentMemberProof *proofs, uint16 count,
						const ClusterCurrentUpdaterProof *updater, const uint32 *capabilities)
{
	ClusterCurrentMemberProof empty_proof = { 0 };
	ClusterCurrentUpdaterProof empty_updater = { 0 };
	uint16 i;

	empty_proof.state = CCM_UNKNOWN;
	empty_updater.verdict = CUCP_UNKNOWN;
	for (i = 0; i < count; i++) {
		UT_ASSERT(memcmp(&proofs[i], &empty_proof, sizeof(empty_proof)) == 0);
		UT_ASSERT_EQ(capabilities[i], 0);
	}
	UT_ASSERT(memcmp(updater, &empty_updater, sizeof(empty_updater)) == 0);
	UT_ASSERT_EQ(test_candidate_held_count, 0);
	UT_ASSERT_EQ(test_xact_lock_depth, 0);
	UT_ASSERT_EQ(test_native_fence_depth, 0);
}

UT_TEST(test_retained_active_identity_reaches_real_mx_batch)
{
	int updater;
	int cross_segment;

	for (updater = 0; updater < 2; updater++) {
		for (cross_segment = 0; cross_segment < (updater ? 2 : 1); cross_segment++) {
			ClusterCurrentMemberProof proof;
			ClusterCurrentUpdaterProof updater_proof;
			ClusterTTStatusKey status_key;
			uint32 capability;
			uint32 generation;
			uint16 wrap;
			TimestampTz deadline = 0;

			if (updater)
				reset_rolled_updater_fixture(cross_segment != 0);
			else
				reset_rolled_active_fixture();
			UT_ASSERT_EQ(resolve_rolled_mx_batch(updater != 0, false, &proof, &updater_proof,
												 &capability, &deadline),
						 CMX_RESOLVE_OK);
			UT_ASSERT_EQ(proof.state, CCM_ACTIVE);
			UT_ASSERT_EQ(proof.member_xid, TEST_ORIGIN_XID);
			UT_ASSERT_EQ(proof.member_ordinal, 0);
			UT_ASSERT_EQ(proof.commit_scn, InvalidScn);
			UT_ASSERT_EQ(ClusterCurrentMemberProofGetCtrcGrant(&proof), test_ctrc_touch_grant);
			UT_ASSERT(
				ClusterCurrentMemberProofGetStatusKey(&proof, &status_key, &generation, &wrap));
			UT_ASSERT_EQ(status_key.origin_node_id, 0);
			UT_ASSERT_EQ(status_key.undo_segment_id, test_ctrc_touch_segment);
			UT_ASSERT_EQ(status_key.tt_slot_id, cluster_tt_slot_offset_to_id(TEST_TT_OFFSET));
			UT_ASSERT_EQ(status_key.cluster_epoch, test_formation_epoch);
			UT_ASSERT_EQ(status_key.local_xid, TEST_ORIGIN_XID);
			UT_ASSERT_EQ(generation, test_ctrc_touch_generation);
			UT_ASSERT_EQ(wrap, TEST_ORIGIN_WRAP);
			UT_ASSERT_EQ(capability, 77);
			UT_ASSERT_EQ(updater_proof.verdict, updater ? CUCP_MATCH : CUCP_UNKNOWN);
			if (updater) {
				UT_ASSERT_EQ(updater_proof.candidate_next_xmin_locator.tt_wrap, TEST_ORIGIN_WRAP);
				UT_ASSERT(memcmp(&updater_proof.candidate_next_xmin_locator.uba,
								 &test_origin_locator.uba, sizeof(test_origin_locator.uba))
						  == 0);
				UT_ASSERT_EQ(test_durable_locator_calls, 0);
			} else
				UT_ASSERT_EQ(test_durable_locator_calls, 1);
			UT_ASSERT_EQ(test_ctrc_touch_calls, 1);
			UT_ASSERT_EQ(test_mx_remote_calls, 0);
			UT_ASSERT_EQ(test_candidate_held_count, 0);
			UT_ASSERT_EQ(test_candidate_acquire_calls, test_candidate_release_calls);
			UT_ASSERT_EQ(test_native_fence_depth, 0);
			UT_ASSERT_EQ(test_xact_lock_depth, 0);
		}
	}
}

UT_TEST(test_retained_active_mx_batch_scrubs_earlier_local_proof)
{
	int failure;

	for (failure = 0; failure < 3; failure++) {
		ClusterCurrentMemberProof proofs[2];
		ClusterCurrentUpdaterProof updater;
		uint32 capabilities[2];
		TimestampTz deadline = 0;

		reset_rolled_active_fixture();
		test_mx_remote_failed = failure == 1;
		test_mx_remote_revokes_grant = failure == 2;
		UT_ASSERT_EQ(
			resolve_rolled_mx_batch(false, true, proofs, &updater, capabilities, &deadline),
			failure == 0 ? CMX_RESOLVE_OK
						 : (failure == 1 ? CMX_RESOLVE_UNKNOWN : CMX_RESOLVE_RETRY));
		UT_ASSERT_EQ(test_mx_remote_calls, 1);
		if (failure != 0)
			assert_mx_batch_unknown(proofs, 2, &updater, capabilities);
		else {
			UT_ASSERT_EQ(proofs[0].state, CCM_ACTIVE);
			UT_ASSERT_EQ(ClusterCurrentMemberProofGetCtrcGrant(&proofs[0]), test_ctrc_touch_grant);
			UT_ASSERT_EQ(capabilities[0], 77);
			UT_ASSERT_EQ(proofs[1].state, CCM_COMMITTED);
			UT_ASSERT_EQ(ClusterCurrentMemberProofGetCtrcGrant(&proofs[1]), 0);
			UT_ASSERT_EQ(capabilities[1], 0);
		}
		UT_ASSERT_EQ(test_candidate_acquire_calls, test_candidate_release_calls);
	}
}

UT_TEST(test_retained_active_mx_batch_rechecks_remote_epoch)
{
	ClusterCurrentMemberProof proofs[2];
	ClusterCurrentUpdaterProof updater;
	uint32 capabilities[2];
	TimestampTz deadline = 0;

	/* A remote member proof is provisional until the batch has confirmed that
	 * the formation epoch is still current.  If the remote response crosses an
	 * epoch change, the local ACTIVE proof must be scrubbed with it. */
	reset_rolled_active_fixture();
	test_mx_remote_epoch_drift = true;
	UT_ASSERT_EQ(
		resolve_rolled_mx_batch(false, true, proofs, &updater, capabilities, &deadline),
		CMX_RESOLVE_UNKNOWN);
	UT_ASSERT_EQ(test_mx_remote_calls, 1);
	assert_mx_batch_unknown(proofs, 2, &updater, capabilities);
	UT_ASSERT_EQ(test_formation_epoch, 43);
	UT_ASSERT_EQ(test_ctrc_touch_calls, 1);
	UT_ASSERT_EQ(test_durable_locator_calls, 1);
	UT_ASSERT_EQ(test_candidate_acquire_calls, test_candidate_release_calls);
}

UT_TEST(test_retained_active_mx_batch_rejects_remote_identity_mismatch)
{
	ClusterCurrentMemberProof proofs[2];
	ClusterCurrentUpdaterProof updater;
	uint32 capabilities[2];
	TimestampTz deadline = 0;

	/* A remote committed proof remains provisional until its member identity
	 * matches the descriptor that the requester sent for that ordinal. */
	reset_rolled_active_fixture();
	test_mx_remote_identity_mismatch = true;
	UT_ASSERT_EQ(
		resolve_rolled_mx_batch(false, true, proofs, &updater, capabilities, &deadline),
		CMX_RESOLVE_UNKNOWN);
	UT_ASSERT_EQ(test_mx_remote_calls, 1);
	assert_mx_batch_unknown(proofs, 2, &updater, capabilities);
	UT_ASSERT_EQ(test_formation_epoch, 42);
	UT_ASSERT_EQ(test_ctrc_touch_calls, 1);
	UT_ASSERT_EQ(test_durable_locator_calls, 1);
	UT_ASSERT_EQ(test_candidate_acquire_calls, test_candidate_release_calls);
}

UT_TEST(test_retained_active_mx_batch_rejects_missing_remote_proof)
{
	ClusterCurrentMemberProof proofs[2];
	ClusterCurrentUpdaterProof updater;
	uint32 capabilities[2];
	TimestampTz deadline = 0;

	/* A successful transport response without the requested member proof is
	 * incomplete and cannot publish the earlier local proof either. */
	reset_rolled_active_fixture();
	test_mx_remote_omits_proof = true;
	UT_ASSERT_EQ(
		resolve_rolled_mx_batch(false, true, proofs, &updater, capabilities, &deadline),
		CMX_RESOLVE_UNKNOWN);
	UT_ASSERT_EQ(test_mx_remote_calls, 1);
	assert_mx_batch_unknown(proofs, 2, &updater, capabilities);
	UT_ASSERT_EQ(test_formation_epoch, 42);
	UT_ASSERT_EQ(test_ctrc_touch_calls, 1);
	UT_ASSERT_EQ(test_durable_locator_calls, 1);
	UT_ASSERT_EQ(test_candidate_acquire_calls, test_candidate_release_calls);
}

UT_TEST(test_retained_active_mx_batch_rejects_invalid_remote_commit_scn)
{
	ClusterCurrentMemberProof proofs[2];
	ClusterCurrentUpdaterProof updater;
	uint32 capabilities[2];
	TimestampTz deadline = 0;

	/* A committed remote proof without a valid commit SCN is semantically
	 * incomplete and cannot publish the earlier local proof. */
	reset_rolled_active_fixture();
	test_mx_remote_invalid_commit_scn = true;
	UT_ASSERT_EQ(
		resolve_rolled_mx_batch(false, true, proofs, &updater, capabilities, &deadline),
		CMX_RESOLVE_UNKNOWN);
	UT_ASSERT_EQ(test_mx_remote_calls, 1);
	assert_mx_batch_unknown(proofs, 2, &updater, capabilities);
	UT_ASSERT_EQ(test_formation_epoch, 42);
	UT_ASSERT_EQ(test_ctrc_touch_calls, 1);
	UT_ASSERT_EQ(test_durable_locator_calls, 1);
	UT_ASSERT_EQ(test_candidate_acquire_calls, test_candidate_release_calls);
}

UT_TEST(test_retained_active_mx_batch_rejects_remote_committed_grant)
{
	ClusterCurrentMemberProof proofs[2];
	ClusterCurrentUpdaterProof updater;
	uint32 capabilities[2];
	TimestampTz deadline = 0;

	/* A committed remote proof carrying a live CTRC grant mixes terminal and
	 * active authority, so it cannot publish the earlier local proof. */
	reset_rolled_active_fixture();
	test_mx_remote_committed_with_grant = true;
	UT_ASSERT_EQ(
		resolve_rolled_mx_batch(false, true, proofs, &updater, capabilities, &deadline),
		CMX_RESOLVE_UNKNOWN);
	UT_ASSERT_EQ(test_mx_remote_calls, 1);
	assert_mx_batch_unknown(proofs, 2, &updater, capabilities);
	UT_ASSERT_EQ(test_formation_epoch, 42);
	UT_ASSERT_EQ(test_ctrc_touch_calls, 1);
	UT_ASSERT_EQ(test_durable_locator_calls, 1);
	UT_ASSERT_EQ(test_candidate_acquire_calls, test_candidate_release_calls);
}

UT_TEST(test_retained_mx_precommit_retries_preserve_batch_deadline)
{
	int updater;

	for (updater = 0; updater < 2; updater++) {
		ClusterCurrentMemberProof proof;
		ClusterCurrentUpdaterProof updater_proof;
		uint32 capability;
		TimestampTz deadline = 0;
		int acquire_calls;

		if (updater)
			reset_rolled_updater_fixture(true);
		else
			reset_rolled_active_fixture();
		test_tt_slot.status = TT_SLOT_COMMITTED;
		test_tt_slot.commit_scn = test_commit_scn;
		test_now = 1000000;
		UT_ASSERT_EQ(resolve_rolled_mx_batch(updater != 0, false, &proof, &updater_proof,
											 &capability, &deadline),
					 CMX_RESOLVE_RETRY);
		assert_mx_batch_unknown(&proof, 1, &updater_proof, &capability);
		UT_ASSERT_EQ(deadline, 2000000);
		test_now = 1500000;
		UT_ASSERT_EQ(resolve_rolled_mx_batch(updater != 0, false, &proof, &updater_proof,
											 &capability, &deadline),
					 CMX_RESOLVE_RETRY);
		assert_mx_batch_unknown(&proof, 1, &updater_proof, &capability);
		UT_ASSERT_EQ(deadline, 2000000);
		acquire_calls = test_candidate_acquire_calls;
		test_now = deadline;
		UT_ASSERT_EQ(resolve_rolled_mx_batch(updater != 0, false, &proof, &updater_proof,
											 &capability, &deadline),
					 CMX_RESOLVE_TIMEOUT);
		assert_mx_batch_unknown(&proof, 1, &updater_proof, &capability);
		UT_ASSERT_EQ(deadline, 2000000);
		UT_ASSERT_EQ(test_candidate_acquire_calls, acquire_calls);
		UT_ASSERT_EQ(test_ctrc_touch_calls, 0);
	}
}

static void
check_rolled_active_updater_sample(bool expected_proof)
{
	ClusterTTStatusKey key;
	ClusterTTStatusResult result;
	ClusterCtrcTxnKeyV1 ctrc_key;
	ClusterTxLocator canonical;
	ClusterTTStatusKey empty_key = { 0 };
	ClusterCtrcTxnKeyV1 empty_ctrc_key = { 0 };
	ClusterTxLocator empty_locator = { 0 };
	uint32 grant = 99;
	uint32 capability = 99;
	bool cross_segment = true;
	bool retry = true;

	memset(&key, 0xa5, sizeof(key));
	memset(&result, 0xa5, sizeof(result));
	memset(&ctrc_key, 0xa5, sizeof(ctrc_key));
	memset(&canonical, 0xa5, sizeof(canonical));
	test_updater_retry_observer = &retry;
	UT_ASSERT_EQ(cluster_runtime_visibility_current_mx_updater_provenance_exact(
					 &test_origin_locator, 0, &key, &result, &grant, &capability, &ctrc_key,
					 &canonical, &cross_segment, &retry),
				 expected_proof);
	test_updater_retry_observer = NULL;
	UT_ASSERT(!retry);
	UT_ASSERT_EQ(test_candidate_held_count, 0);
	UT_ASSERT_EQ(test_candidate_max_held_count, 1);
	UT_ASSERT_EQ(test_xact_lock_depth, 0);
	UT_ASSERT_EQ(test_native_fence_depth, 0);
	UT_ASSERT_EQ(test_by_xid_scan_calls, 0);
	UT_ASSERT_EQ(test_durable_locator_calls, 0);
	if (expected_proof) {
		UT_ASSERT(result.authoritative);
		UT_ASSERT_EQ(result.status, CLUSTER_TT_STATUS_IN_PROGRESS);
		UT_ASSERT_EQ(result.commit_scn, InvalidScn);
		UT_ASSERT_EQ(key.undo_segment_id, test_ctrc_touch_segment);
		UT_ASSERT_EQ(key.tt_slot_id, cluster_tt_slot_offset_to_id(TEST_TT_OFFSET));
		UT_ASSERT_EQ(key.cluster_epoch, test_formation_epoch);
		UT_ASSERT_EQ(key.local_xid, TEST_ORIGIN_XID);
		UT_ASSERT_EQ(grant, test_ctrc_touch_grant);
		UT_ASSERT_EQ(capability, 77);
		UT_ASSERT(memcmp(&ctrc_key, &test_ctrc_touched_key, sizeof(ctrc_key)) == 0);
		UT_ASSERT_EQ(ctrc_key.system_identifier, test_system_identifier);
		UT_ASSERT_EQ(ctrc_key.origin_boot_incarnation, test_boot_incarnation);
		UT_ASSERT_EQ(ctrc_key.admission_record_generation, 77);
		UT_ASSERT_EQ(test_ctrc_touched_participant.capability_record_generation, capability);
		UT_ASSERT_EQ(test_ctrc_touched_participant.boot_incarnation, test_boot_incarnation);
		UT_ASSERT_EQ(test_ctrc_touched_participant.formation_epoch, test_formation_epoch);
		UT_ASSERT_EQ(canonical.xid, TEST_ORIGIN_XID);
		UT_ASSERT_EQ(canonical.tt_wrap, TEST_ORIGIN_WRAP);
		UT_ASSERT(memcmp(&canonical.uba, &test_origin_locator.uba, sizeof(canonical.uba)) == 0);
		UT_ASSERT_EQ(cross_segment, test_ctrc_touch_segment != TEST_RECORD_SEGMENT);
	} else {
		UT_ASSERT(!result.authoritative);
		UT_ASSERT_EQ(result.status, CLUSTER_TT_STATUS_UNKNOWN);
		UT_ASSERT_EQ(result.commit_scn, InvalidScn);
		UT_ASSERT_EQ(grant, 0);
		UT_ASSERT_EQ(capability, 0);
		UT_ASSERT(memcmp(&key, &empty_key, sizeof(key)) == 0);
		UT_ASSERT(memcmp(&ctrc_key, &empty_ctrc_key, sizeof(ctrc_key)) == 0);
		UT_ASSERT(memcmp(&canonical, &empty_locator, sizeof(canonical)) == 0);
		UT_ASSERT(!cross_segment);
	}
}

UT_TEST(test_local_updater_rolled_active_preserves_original_identity)
{
	int cross_segment;
	int epoch_zero;

	for (cross_segment = 0; cross_segment < 2; cross_segment++) {
		for (epoch_zero = 0; epoch_zero < 2; epoch_zero++) {
			reset_rolled_updater_fixture(cross_segment != 0);
			if (epoch_zero)
				test_formation_epoch = 0;
			/* The real updater consumer must follow UndoRecord, not CURRENT. */
			check_rolled_active_updater_sample(true);
			UT_ASSERT_EQ(test_ctrc_touch_calls, 1);
			UT_ASSERT_EQ(test_candidate_acquire_calls, cross_segment ? 3 : 1);
			UT_ASSERT_EQ(test_candidate_release_calls, test_candidate_acquire_calls);
			UT_ASSERT_EQ(test_candidate_acquire_segments[0], TEST_RECORD_SEGMENT);
			if (cross_segment) {
				UT_ASSERT_EQ(test_candidate_acquire_segments[1], TEST_TT_SEGMENT);
				UT_ASSERT_EQ(test_candidate_acquire_segments[2], TEST_RECORD_SEGMENT);
			}
		}
	}
}

UT_TEST(test_local_updater_rolled_active_accepts_duplicate_ctrc_touch)
{
	int cross_segment;

	for (cross_segment = 0; cross_segment < 2; cross_segment++) {
		reset_rolled_updater_fixture(cross_segment != 0);
		test_ctrc_touch_result = CLUSTER_CTRC_TOUCH_DUPLICATE;
		check_rolled_active_updater_sample(true);
		UT_ASSERT_EQ(test_ctrc_touch_calls, 1);
		UT_ASSERT_EQ(test_candidate_acquire_calls, cross_segment ? 3 : 1);
		UT_ASSERT_EQ(test_candidate_release_calls, test_candidate_acquire_calls);
		UT_ASSERT_EQ(test_candidate_held_count, 0);
	}
}

UT_TEST(test_local_updater_rolled_active_requires_canonical_slot)
{
	int cross_segment;
	int fault;

	for (cross_segment = 0; cross_segment < 2; cross_segment++) {
		for (fault = 0; fault < 4; fault++) {
			reset_rolled_updater_fixture(cross_segment != 0);
			switch (fault) {
			case 0:
				test_tt_slot.wrap++;
				break;
			case 1:
				test_tt_slot.xid++;
				break;
			case 2:
				test_twophase_prepared = true;
				break;
			case 3:
				test_tt_slot.commit_scn = test_commit_scn;
				break;
			}
			check_rolled_active_updater_sample(false);
			UT_ASSERT_EQ(test_ctrc_touch_calls, 0);
			UT_ASSERT_EQ(test_candidate_release_calls, test_candidate_acquire_calls);
		}
	}
}

UT_TEST(test_local_updater_rolled_active_rechecks_touch_and_original_data)
{
	int cross_segment;
	int fault;

	for (cross_segment = 0; cross_segment < 2; cross_segment++) {
		for (fault = 0; fault < 3; fault++) {
			reset_rolled_updater_fixture(cross_segment != 0);
			if (fault == 0)
				test_ctrc_touch_grant = 0;
			else if (fault == 1)
				test_ctrc_touch_generation_drift = true;
			else
				test_candidate_mutate_record_on_recheck = true;
			check_rolled_active_updater_sample(false);
			UT_ASSERT_EQ(test_ctrc_touch_calls, 1);
			UT_ASSERT_EQ(test_candidate_release_calls, test_candidate_acquire_calls);
		}
	}
}

UT_TEST(test_local_updater_rolled_active_final_release_fences)
{
	int cross_segment;
	int fault;

	for (cross_segment = 0; cross_segment < 2; cross_segment++) {
		for (fault = 0; fault < 4; fault++) {
			reset_rolled_updater_fixture(cross_segment != 0);
			test_candidate_release_fault_call = cross_segment ? 3 : 1;
			test_candidate_release_failed = fault == 0;
			test_candidate_release_epoch_drift = fault == 1;
			test_candidate_release_admission_drift = fault == 2;
			test_ctrc_revoke_on_release = fault == 3;
			check_rolled_active_updater_sample(false);
			UT_ASSERT_EQ(test_ctrc_touch_calls, 1);
			UT_ASSERT_EQ(test_candidate_release_calls, cross_segment ? 3 : 1);
			UT_ASSERT_EQ(test_candidate_cancel_calls, fault == 0 ? 1 : 0);
		}
	}
}

UT_TEST(test_other_backend_rolled_active_uses_locator_and_ctrc)
{
	ClusterTTStatusKey key;
	ClusterTTStatusResult result;
	ClusterCtrcTxnKeyV1 ctrc_key;
	ClusterCtrcParticipantIdentity participant;
	uint32 grant;
	int mirror;

	for (mirror = 0; mirror < 3; mirror++) {
		reset_rolled_active_fixture();
		test_durable_locator_status = mirror == 0	? TT_SLOT_ACTIVE
									  : mirror == 1 ? TT_SLOT_COMMITTED
													: TT_SLOT_ABORTED;
		/* Epoch zero is also a legal clean-formation admission. */
		if (mirror == 2)
			test_formation_epoch = 0;
		UT_ASSERT(cluster_runtime_visibility_current_owner_lookup_exact_ctrc_full(
			TEST_ORIGIN_XID, &key, &result, &grant, &ctrc_key, &participant));
		UT_ASSERT_EQ(result.status, CLUSTER_TT_STATUS_IN_PROGRESS);
		UT_ASSERT(result.authoritative);
		UT_ASSERT_EQ(result.commit_scn, InvalidScn);
		UT_ASSERT_EQ(key.undo_segment_id, TEST_TT_SEGMENT);
		UT_ASSERT_EQ(key.cluster_epoch, test_formation_epoch);
		UT_ASSERT_EQ(ctrc_key.segment_generation, 23);
		UT_ASSERT_EQ(ctrc_key.slot_wrap, TEST_ORIGIN_WRAP);
		UT_ASSERT_EQ(grant, test_ctrc_touch_grant);
		UT_ASSERT_EQ(participant.boot_incarnation, test_boot_incarnation);
		UT_ASSERT_EQ(test_ctrc_touch_calls, 1);
		UT_ASSERT_EQ(test_durable_locator_calls, 1);
		UT_ASSERT_EQ(test_current_owner_calls, 1);
		UT_ASSERT_EQ(test_candidate_acquire_calls, 1);
		UT_ASSERT_EQ(test_candidate_release_calls, 1);
		UT_ASSERT_EQ(test_candidate_held_count, 0);
	}
}

UT_TEST(test_other_backend_rolled_active_accepts_duplicate_ctrc_touch)
{
	ClusterTTStatusKey key;
	ClusterTTStatusResult result;
	ClusterCtrcTxnKeyV1 ctrc_key;
	ClusterCtrcParticipantIdentity participant;
	uint32 grant = 0;

	reset_rolled_active_fixture();
	test_ctrc_touch_result = CLUSTER_CTRC_TOUCH_DUPLICATE;
	UT_ASSERT(cluster_runtime_visibility_current_owner_lookup_exact_ctrc_full(
		TEST_ORIGIN_XID, &key, &result, &grant, &ctrc_key, &participant));
	UT_ASSERT_EQ(result.status, CLUSTER_TT_STATUS_IN_PROGRESS);
	UT_ASSERT(result.authoritative);
	UT_ASSERT_EQ(grant, test_ctrc_touch_grant);
	UT_ASSERT_EQ(test_ctrc_touch_calls, 1);
	UT_ASSERT_EQ(test_durable_locator_calls, 1);
	UT_ASSERT_EQ(test_current_owner_calls, 1);
	UT_ASSERT_EQ(test_candidate_acquire_calls, 1);
	UT_ASSERT_EQ(test_candidate_release_calls, 1);
	UT_ASSERT_EQ(test_candidate_held_count, 0);
	UT_ASSERT_EQ(key.undo_segment_id, TEST_TT_SEGMENT);
	UT_ASSERT_EQ(ctrc_key.segment_id, TEST_TT_SEGMENT);
	UT_ASSERT_EQ(ctrc_key.segment_generation, test_ctrc_touch_generation);
	UT_ASSERT_EQ(ctrc_key.slot_offset, TEST_TT_OFFSET);
	UT_ASSERT_EQ(ctrc_key.slot_wrap, TEST_ORIGIN_WRAP);
}

UT_TEST(test_current_mx_other_backend_rolled_active_records_original_touch)
{
	ClusterTTStatusKey key;
	ClusterTTStatusResult result;
	ClusterCtrcTxnKeyV1 ctrc_key;
	uint32 grant = 0;
	uint32 capability = 0;

	reset_rolled_active_fixture();
	UT_ASSERT_EQ(current_mx_local_member_sample_exact(TEST_ORIGIN_XID, &key, &result, &grant,
													  &capability, &ctrc_key),
				 CMX_RESOLVE_OK);
	UT_ASSERT_EQ(result.status, CLUSTER_TT_STATUS_IN_PROGRESS);
	UT_ASSERT_EQ(grant, test_ctrc_touch_grant);
	UT_ASSERT(capability > 0);
	UT_ASSERT_EQ(ctrc_key.segment_id, TEST_TT_SEGMENT);
	UT_ASSERT_EQ(test_durable_locator_calls, 1); /* No terminal-only fallback. */
	UT_ASSERT_EQ(test_ctrc_touch_calls, 1);
	UT_ASSERT_EQ(test_candidate_held_count, 0);
}

UT_TEST(test_retained_active_mx_batch_accepts_duplicate_ctrc_touch)
{
	ClusterCurrentMemberProof proof;
	ClusterCurrentUpdaterProof updater_proof;
	uint32 capability;
	TimestampTz deadline = 0;

	/* The batch consumer must preserve a valid old-segment ACTIVE proof when
	 * the idempotent CTRC touch reports that another attempt already recorded
	 * the same receipt. */
	reset_rolled_active_fixture();
	test_ctrc_touch_result = CLUSTER_CTRC_TOUCH_DUPLICATE;
	UT_ASSERT_EQ(resolve_rolled_mx_batch(false, false, &proof, &updater_proof, &capability,
								 &deadline),
				 CMX_RESOLVE_OK);
	UT_ASSERT_EQ(proof.state, CCM_ACTIVE);
	UT_ASSERT_EQ(proof.member_xid, TEST_ORIGIN_XID);
	UT_ASSERT_EQ(proof.member_ordinal, 0);
	UT_ASSERT_EQ(ClusterCurrentMemberProofGetCtrcGrant(&proof), test_ctrc_touch_grant);
	UT_ASSERT_EQ(capability, 77);
	UT_ASSERT_EQ(updater_proof.verdict, CUCP_UNKNOWN);
	UT_ASSERT_EQ(test_ctrc_touch_calls, 1);
	UT_ASSERT_EQ(test_durable_locator_calls, 1);
	UT_ASSERT_EQ(test_candidate_held_count, 0);
	UT_ASSERT_EQ(test_candidate_acquire_calls, test_candidate_release_calls);
}

UT_TEST(test_retained_active_mx_batch_refuses_ctrc_touch)
{
	ClusterCurrentMemberProof proof;
	ClusterCurrentUpdaterProof updater_proof;
	uint32 capability;
	TimestampTz deadline = 0;

	/* An old-segment ACTIVE sample without a publishable CTRC receipt cannot
	 * authorize a member proof.  The batch consumer must scrub the partial
	 * result and release its candidate guard. */
	reset_rolled_active_fixture();
	test_ctrc_touch_grant = 0;
	UT_ASSERT_EQ(resolve_rolled_mx_batch(false, false, &proof, &updater_proof, &capability,
									 &deadline),
				 CMX_RESOLVE_UNKNOWN);
	assert_mx_batch_unknown(&proof, 1, &updater_proof, &capability);
	UT_ASSERT_EQ(test_ctrc_touch_calls, 1);
	/* The failed ACTIVE proof falls through the terminal-only fallback, which
	 * performs its own exact locator scan before returning UNKNOWN. */
	UT_ASSERT_EQ(test_durable_locator_calls, 2);
	UT_ASSERT_EQ(test_candidate_acquire_calls, test_candidate_release_calls);
}

UT_TEST(test_retained_active_mx_batch_rechecks_local_grant_after_release)
{
	ClusterCurrentMemberProof proof;
	ClusterCurrentUpdaterProof updater_proof;
	uint32 capability;
	TimestampTz deadline = 0;

	/* A local ACTIVE proof is provisional until the candidate guard has been
	 * released.  Revoking its CTRC grant at that boundary must force a retry
	 * and scrub the entire batch before publication. */
	reset_rolled_active_fixture();
	test_ctrc_revoke_on_release = true;
	UT_ASSERT_EQ(resolve_rolled_mx_batch(false, false, &proof, &updater_proof, &capability,
									 &deadline),
				 CMX_RESOLVE_RETRY);
	assert_mx_batch_unknown(&proof, 1, &updater_proof, &capability);
	UT_ASSERT_EQ(test_ctrc_touch_calls, 1);
	UT_ASSERT_EQ(test_durable_locator_calls, 1);
	UT_ASSERT_EQ(test_candidate_acquire_calls, test_candidate_release_calls);
}

UT_TEST(test_retained_active_mx_batch_rechecks_admission_and_epoch_after_release)
{
	int fault;

	for (fault = 0; fault < 2; fault++) {
		ClusterCurrentMemberProof proof;
		ClusterCurrentUpdaterProof updater_proof;
		uint32 capability;
		TimestampTz deadline = 0;

		/* The local ACTIVE proof remains provisional until its candidate guard
		 * is released.  Either admission revocation or a formation epoch change
		 * must clear the proof and leave the terminal fallback unproven. */
		reset_rolled_active_fixture();
		if (fault == 0)
			test_candidate_release_admission_drift = true;
		else
			test_candidate_release_epoch_drift = true;
		UT_ASSERT_EQ(resolve_rolled_mx_batch(false, false, &proof, &updater_proof, &capability,
									 &deadline),
						 CMX_RESOLVE_UNKNOWN);
		assert_mx_batch_unknown(&proof, 1, &updater_proof, &capability);
		UT_ASSERT_EQ(test_ctrc_touch_calls, 1);
		UT_ASSERT_EQ(test_durable_locator_calls, fault == 0 ? 1 : 2);
		UT_ASSERT_EQ(test_candidate_acquire_calls, test_candidate_release_calls);
	}
}

UT_TEST(test_other_backend_rolled_terminal_uses_canonical_status)
{
	ClusterTTStatusKey key;
	ClusterTTStatusResult result;
	int aborted;

	for (aborted = 0; aborted < 2; aborted++) {
		reset_exact_origin_fixture();
		test_durable_locator_status = TT_SLOT_ACTIVE;
		if (aborted) {
			test_native_status = TRANSACTION_STATUS_ABORTED;
			test_tt_slot.status = TT_SLOT_ABORTED;
			test_tt_slot.commit_scn = InvalidScn;
		}
		UT_ASSERT(
			cluster_runtime_visibility_current_owner_lookup_exact(TEST_ORIGIN_XID, &key, &result));
		UT_ASSERT_EQ(result.status,
					 aborted ? CLUSTER_TT_STATUS_ABORTED : CLUSTER_TT_STATUS_COMMITTED);
		UT_ASSERT_EQ(result.commit_scn, aborted ? InvalidScn : test_commit_scn);
		UT_ASSERT(result.authoritative);
		UT_ASSERT_EQ(test_durable_locator_calls, 1);
		UT_ASSERT_EQ(test_ctrc_touch_calls, 0);
		UT_ASSERT_EQ(test_candidate_held_count, 0);
	}
}

UT_TEST(test_other_backend_rolled_locator_failures_do_not_acquire)
{
	int fault;

	for (fault = 0; fault < 6; fault++) {
		reset_rolled_active_fixture();
		switch (fault) {
		case 0:
			test_durable_locator_result = CLUSTER_TT_DURABLE_LOCATE_MISSING;
			break;
		case 1:
			test_durable_locator_result = CLUSTER_TT_DURABLE_LOCATE_AMBIGUOUS;
			break;
		case 2:
			test_durable_locator_result = CLUSTER_TT_DURABLE_LOCATE_SCAN_UNAVAILABLE;
			break;
		case 3:
			test_durable_locator_segment = 0;
			break;
		case 4:
			test_durable_locator_slot = TT_SLOTS_PER_SEGMENT;
			break;
		case 5:
			test_durable_locator_wrap = TT_WRAP_INVALID;
			break;
		}
		assert_rolled_lookup_unknown();
		UT_ASSERT_EQ(test_durable_locator_calls, 1);
		UT_ASSERT_EQ(test_candidate_acquire_calls, 0);
		UT_ASSERT_EQ(test_ctrc_touch_calls, 0);
	}
}

UT_TEST(test_other_backend_rolled_locator_requires_native_retained_era)
{
	int fault;

	for (fault = 0; fault < 6; fault++) {
		reset_rolled_active_fixture();
		switch (fault) {
		case 0:
			test_next_full_xid = InvalidFullTransactionId;
			break;
		case 1:
			test_next_full_xid = FullTransactionIdFromEpochAndXid(1, 1000);
			break;
		case 2:
			test_next_full_xid = FullTransactionIdFromEpochAndXid(0, TEST_ORIGIN_XID);
			break;
		case 3:
			test_next_full_xid = FullTransactionIdFromEpochAndXid(0, TEST_ORIGIN_XID - 1);
			break;
		case 4:
			test_variable_cache.oldestClogXid = TEST_ORIGIN_XID + 1;
			break;
		case 5:
			test_variable_cache.oldestClogXid = InvalidTransactionId;
			break;
		}
		assert_rolled_lookup_unknown();
		UT_ASSERT_EQ(test_durable_locator_calls, 0);
		UT_ASSERT_EQ(test_candidate_acquire_calls, 0);
		UT_ASSERT_EQ(test_ctrc_touch_calls, 0);
	}
	reset_rolled_active_fixture();
	test_variable_cache.oldestClogXid = TEST_ORIGIN_XID;
	{
		ClusterTTStatusKey key;
		ClusterTTStatusResult result;
		uint32 grant;
		UT_ASSERT(cluster_runtime_visibility_current_owner_lookup_exact_ctrc(TEST_ORIGIN_XID, &key,
																			 &result, &grant));
	}
}

UT_TEST(test_other_backend_rolled_locator_rechecks_scope_after_scan)
{
	int fault;

	for (fault = 0; fault < 2; fault++) {
		reset_rolled_active_fixture();
		if (fault == 0)
			test_oldest_clog_on_locate = TEST_ORIGIN_XID + 1;
		else
			test_next_full_on_locate = FullTransactionIdFromEpochAndXid(1, 1000);
		assert_rolled_lookup_unknown();
		UT_ASSERT_EQ(test_durable_locator_calls, 1);
		UT_ASSERT_EQ(test_candidate_acquire_calls, 0);
		UT_ASSERT_EQ(test_ctrc_touch_calls, 0);
	}
}

UT_TEST(test_other_backend_rolled_locator_requires_exact_canonical_proof)
{
	int fault;

	for (fault = 0; fault < 9; fault++) {
		reset_rolled_active_fixture();
		switch (fault) {
		case 0:
			test_tt_slot.xid++;
			break;
		case 1:
			test_tt_slot.wrap++;
			break;
		case 2:
			test_tt_slot.commit_scn = test_commit_scn;
			break;
		case 3:
			test_candidate_copy_physical_slot = false;
			break;
		case 4:
			test_candidate_sample_result = CLUSTER_UNDO_BLOCK0_AUTHORITY_DENIED;
			break;
		case 5:
			test_candidate_copy_generation_drift = true;
			break;
		case 6:
			test_candidate_root_drift = true;
			break;
		case 7:
			test_twophase_prepared = true;
			break;
		case 8: /* Precommit COMMITTED bytes cannot grant an ACTIVE touch. */
			test_tt_slot.status = TT_SLOT_COMMITTED;
			test_tt_slot.commit_scn = test_commit_scn;
			break;
		}
		assert_rolled_lookup_unknown();
		UT_ASSERT_EQ(test_durable_locator_calls, 1);
		UT_ASSERT_EQ(test_candidate_release_calls, 1);
		UT_ASSERT_EQ(test_ctrc_touch_calls, 0);
	}
}

UT_TEST(test_other_backend_rolled_lookup_rechecks_after_release)
{
	int fault;

	for (fault = 0; fault < 8; fault++) {
		reset_rolled_active_fixture();
		if (fault == 0)
			test_candidate_release_failed = true;
		else if (fault == 1)
			test_candidate_release_epoch_drift = true;
		else if (fault == 2)
			test_candidate_release_admission_drift = true;
		else
			test_release_scope_drift = fault - 2;
		assert_rolled_lookup_unknown();
		UT_ASSERT_EQ(test_durable_locator_calls, 1);
		UT_ASSERT_EQ(test_candidate_release_calls, 1);
	}
}

UT_TEST(test_other_backend_locator_does_not_expand_fast_paths)
{
	ClusterTTStatusKey key;
	ClusterTTStatusResult result;
	uint32 grant;
	int binding;

	for (binding = 0; binding < 2; binding++) {
		reset_rolled_active_fixture();
		test_next_full_xid = InvalidFullTransactionId;
		test_local_binding_available = binding != 0;
		test_current_owner_available = binding == 0;
		UT_ASSERT(cluster_runtime_visibility_current_owner_lookup_exact_ctrc(TEST_ORIGIN_XID, &key,
																			 &result, &grant));
		UT_ASSERT_EQ(test_durable_locator_calls, 0);
		UT_ASSERT_EQ(test_ctrc_touch_calls, 1);
	}
	/* A malformed index hit must not be laundered through a locator retry. */
	reset_rolled_active_fixture();
	test_current_owner_available = true;
	test_current_owner.wrap = TT_WRAP_INVALID;
	assert_rolled_lookup_unknown();
	UT_ASSERT_EQ(test_durable_locator_calls, 0);
}

UT_TEST(test_current_owner_fast_paths_recheck_released_identity)
{
	int binding;
	int fault;

	for (binding = 0; binding < 2; binding++) {
		for (fault = 1; fault <= 4; fault++) {
			reset_rolled_active_fixture();
			test_local_binding_available = binding != 0;
			test_current_owner_available = binding == 0;
			test_release_identity_drift = binding * 4 + fault;
			assert_rolled_lookup_unknown();
			/* Discard the sampled grant without repeating block acquisition or
			 * replacing a changed fast-path identity through the disk locator. */
			UT_ASSERT_EQ(test_ctrc_touch_calls, 1);
			UT_ASSERT_EQ(test_durable_locator_calls, 0);
			UT_ASSERT_EQ(test_candidate_acquire_calls, 1);
			UT_ASSERT_EQ(test_candidate_block0_copy_calls, 1);
			UT_ASSERT_EQ(test_candidate_release_calls, 1);
		}
	}
}

UT_TEST(test_current_owner_fast_paths_recheck_release_authority)
{
	int binding;
	int fault;

	for (binding = 0; binding < 2; binding++) {
		for (fault = 0; fault < 5; fault++) {
			reset_rolled_active_fixture();
			test_local_binding_available = binding != 0;
			test_current_owner_available = binding == 0;
			switch (fault) {
			case 0:
				test_candidate_release_epoch_drift = true;
				break;
			case 1:
				test_candidate_release_admission_drift = true;
				break;
			case 2:
				test_release_scope_drift = 6; /* Root generation. */
				break;
			case 3:
				test_release_scope_drift = 3; /* Origin boot incarnation. */
				break;
			case 4:
				test_candidate_release_failed = true;
				break;
			}
			assert_rolled_lookup_unknown();
			UT_ASSERT_EQ(test_ctrc_touch_calls, 1);
			UT_ASSERT_EQ(test_durable_locator_calls, 0);
			UT_ASSERT_EQ(test_candidate_acquire_calls, 1);
			UT_ASSERT_EQ(test_candidate_release_calls, 1);
		}
	}
}

UT_TEST(test_current_mx_fast_paths_reject_released_identity)
{
	ClusterTTStatusKey key;
	ClusterTTStatusResult result;
	ClusterCtrcTxnKeyV1 ctrc_key;
	ClusterCtrcTxnKeyV1 empty_ctrc_key = { 0 };
	ClusterTTStatusKey empty_key = { 0 };
	uint32 grant;
	uint32 capability;
	int binding;
	int changed;

	for (binding = 0; binding < 2; binding++) {
		for (changed = 0; changed < 2; changed++) {
			reset_rolled_active_fixture();
			test_local_binding_available = binding != 0;
			test_current_owner_available = binding == 0;
			if (changed)
				test_release_identity_drift = binding ? 6 : 2; /* Exact wrap. */
			memset(&key, 0xa5, sizeof(key));
			memset(&result, 0xa5, sizeof(result));
			memset(&ctrc_key, 0xa5, sizeof(ctrc_key));
			grant = capability = 99;
			UT_ASSERT_EQ(current_mx_local_member_sample_exact(TEST_ORIGIN_XID, &key, &result,
															  &grant, &capability, &ctrc_key),
						 changed ? CMX_RESOLVE_UNKNOWN : CMX_RESOLVE_OK);
			if (changed) {
				UT_ASSERT_EQ(result.status, CLUSTER_TT_STATUS_UNKNOWN);
				UT_ASSERT_EQ(result.commit_scn, InvalidScn);
				UT_ASSERT(!result.authoritative);
				UT_ASSERT_EQ(grant, 0);
				UT_ASSERT_EQ(capability, 0);
				UT_ASSERT(memcmp(&key, &empty_key, sizeof(key)) == 0);
				UT_ASSERT(memcmp(&ctrc_key, &empty_ctrc_key, sizeof(ctrc_key)) == 0);
			} else {
				UT_ASSERT_EQ(result.status, CLUSTER_TT_STATUS_IN_PROGRESS);
				UT_ASSERT(result.authoritative);
				UT_ASSERT_EQ(grant, test_ctrc_touch_grant);
				UT_ASSERT(capability > 0);
				UT_ASSERT_EQ(ctrc_key.slot_wrap, TEST_ORIGIN_WRAP);
				UT_ASSERT_EQ(test_durable_locator_calls, 0);
				UT_ASSERT_EQ(test_candidate_acquire_calls, 1);
				UT_ASSERT_EQ(test_candidate_block0_copy_calls, 1);
			}
			UT_ASSERT_EQ(test_candidate_held_count, 0);
			UT_ASSERT_EQ(test_candidate_release_calls, test_candidate_acquire_calls);
		}
	}
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

	UT_ASSERT(cluster_runtime_visibility_active_proof_ctrc_identity_exact(&proof_key, 17, 313,
																		  &ctrc_key, &participant));
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
	UT_ASSERT_EQ(participant.admission_record_generation, ctrc_key.admission_record_generation);

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

	UT_ASSERT(cluster_runtime_visibility_active_proof_ctrc_identity_exact(&proof_key, 17, 313,
																		  &ctrc_key, &participant));
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
		&locator, &admission, &guard, &root, &key, &result, NULL, NULL));
	UT_ASSERT_EQ(result.status, CLUSTER_TT_STATUS_ABORTED);
	UT_ASSERT(result.authoritative);
	UT_ASSERT_EQ(key.undo_segment_id, (uint16)TEST_TT_SEGMENT);
	UT_ASSERT_EQ(key.tt_slot_id, cluster_tt_slot_offset_to_id(TEST_TT_OFFSET));
	UT_ASSERT_EQ(test_current_owner_calls, 0);
	UT_ASSERT_EQ(test_current_segment_calls, 2);

	/* Locator status is never a verdict: missing/mismatched canonical bytes
	 * and allocator rollover drift remain UNKNOWN. */
	test_candidate_copy_physical_slot = false;
	UT_ASSERT(!cluster_runtime_visibility_physical_locator_sample_held(
		&locator, &admission, &guard, &root, &key, &result, NULL, NULL));
	UT_ASSERT_EQ(result.status, CLUSTER_TT_STATUS_UNKNOWN);
	UT_ASSERT(!result.authoritative);
	test_candidate_copy_physical_slot = true;
	test_tt_slot.xid++;
	UT_ASSERT(!cluster_runtime_visibility_physical_locator_sample_held(
		&locator, &admission, &guard, &root, &key, &result, NULL, NULL));
	test_tt_slot.xid = TEST_ORIGIN_XID;
	test_current_segment_calls = 0;
	test_current_segment_drift_on_recheck = true;
	UT_ASSERT(!cluster_runtime_visibility_physical_locator_sample_held(
		&locator, &admission, &guard, &root, &key, &result, NULL, NULL));
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
		&locator, &admission, &guard, &root, &key, &result, NULL, NULL));
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

	UT_ASSERT(cluster_runtime_visibility_local_terminal_lookup_exact(TEST_ORIGIN_XID, &key, &result,
																	 NULL));
	UT_ASSERT_EQ(key.origin_node_id, (uint16)cluster_node_id);
	UT_ASSERT_EQ(key.undo_segment_id, (uint16)TEST_TT_SEGMENT);
	UT_ASSERT_EQ(key.tt_slot_id, cluster_tt_slot_offset_to_id(TEST_TT_OFFSET));
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

static void
assert_local_terminal_unknown(void)
{
	ClusterTTStatusKey key;
	ClusterTTStatusKey zero_key = { 0 };
	ClusterTTStatusResult result;
	bool retry = true;

	memset(&key, 0xa5, sizeof(key));
	memset(&result, 0xa5, sizeof(result));
	UT_ASSERT(!cluster_runtime_visibility_local_terminal_lookup_exact(TEST_ORIGIN_XID, &key,
																	  &result, &retry));
	UT_ASSERT_EQ(memcmp(&key, &zero_key, sizeof(key)), 0);
	UT_ASSERT_EQ(result.status, CLUSTER_TT_STATUS_UNKNOWN);
	UT_ASSERT_EQ(result.commit_scn, InvalidScn);
	UT_ASSERT(!result.authoritative);
	UT_ASSERT(!retry);
	UT_ASSERT_EQ(test_candidate_held_count, 0);
	UT_ASSERT_EQ(test_xact_lock_depth, 0);
	UT_ASSERT_EQ(test_ctrc_touch_calls, 0);
}

UT_TEST(test_local_terminal_locator_rejects_unproven_native_era_before_io)
{
	int fault;

	for (fault = 0; fault < 8; fault++) {
		reset_exact_origin_fixture();
		switch (fault) {
		case 0:
			test_next_full_xid = FullTransactionIdFromEpochAndXid(1, 1000);
			break;
		case 1:
			test_next_full_xid = InvalidFullTransactionId;
			break;
		case 2:
			test_next_full_xid = FullTransactionIdFromEpochAndXid(0, TEST_ORIGIN_XID);
			break;
		case 3:
			test_next_full_xid = FullTransactionIdFromEpochAndXid(0, TEST_ORIGIN_XID - 1);
			break;
		case 4:
			test_variable_cache.oldestClogXid = TEST_ORIGIN_XID + 1;
			break;
		case 5:
			test_variable_cache.oldestClogXid = InvalidTransactionId;
			break;
		case 6:
			test_system_identifier = 0;
			break;
		case 7:
			test_boot_incarnation = 0;
			break;
		}
		assert_local_terminal_unknown();
		UT_ASSERT_EQ(test_durable_locator_calls, 0);
		UT_ASSERT_EQ(test_candidate_acquire_calls, 0);
	}
}

UT_TEST(test_local_terminal_locator_rechecks_native_scope_after_scan)
{
	int fault;

	for (fault = 0; fault < 2; fault++) {
		reset_exact_origin_fixture();
		if (fault == 0)
			test_oldest_clog_on_locate = TEST_ORIGIN_XID + 1;
		else
			test_next_full_on_locate = FullTransactionIdFromEpochAndXid(1, 1000);
		assert_local_terminal_unknown();
		UT_ASSERT_EQ(test_durable_locator_calls, 1);
		UT_ASSERT_EQ(test_candidate_acquire_calls, 0);
	}
}

UT_TEST(test_local_terminal_rechecks_scope_and_root_after_release)
{
	int aborted;
	int fault;

	for (aborted = 0; aborted < 2; aborted++) {
		for (fault = 1; fault <= 6; fault++) {
			reset_exact_origin_fixture();
			if (aborted) {
				test_native_status = TRANSACTION_STATUS_ABORTED;
				test_tt_slot.status = TT_SLOT_ABORTED;
				test_tt_slot.commit_scn = InvalidScn;
			}
			test_release_scope_drift = fault;
			assert_local_terminal_unknown();
			UT_ASSERT_EQ(test_durable_locator_calls, 1);
			UT_ASSERT_EQ(test_candidate_release_calls, 1);
		}
	}
}

UT_TEST(test_local_terminal_scope_drift_clears_precommit_retry)
{
	int fault;

	for (fault = 1; fault <= 6; fault++) {
		reset_exact_origin_fixture();
		test_native_status = TRANSACTION_STATUS_IN_PROGRESS;
		test_twophase_xid = TEST_ORIGIN_XID;
		test_release_scope_drift = fault;
		assert_local_terminal_unknown();
		UT_ASSERT_EQ(test_candidate_release_calls, 1);
	}
}

UT_TEST(test_local_terminal_precommit_window_requests_retry)
{
	ClusterTTStatusKey key;
	ClusterTTStatusResult result;
	bool retry = false;

	reset_exact_origin_fixture();
	test_native_status = TRANSACTION_STATUS_IN_PROGRESS;
	test_twophase_xid = TEST_ORIGIN_XID;

	/* A retained TT COMMITTED stamp with a live native xid is a precommit
	 * window, not a terminal result.  Preserve the retry handoff after the
	 * physical bracket releases so the caller can observe publication. */
	UT_ASSERT(!cluster_runtime_visibility_local_terminal_lookup_exact(TEST_ORIGIN_XID, &key,
																		 &result, &retry));
	UT_ASSERT(retry);
	UT_ASSERT_EQ(result.status, CLUSTER_TT_STATUS_UNKNOWN);
	UT_ASSERT(!result.authoritative);
	UT_ASSERT_EQ(test_durable_locator_calls, 1);
	UT_ASSERT_EQ(test_candidate_acquire_calls, 1);
	UT_ASSERT_EQ(test_candidate_release_calls, 1);
	UT_ASSERT_EQ(test_candidate_held_count, 0);
}

UT_TEST(test_local_terminal_retained_boundary_and_counter_advance_stay_valid)
{
	ClusterTTStatusKey key;
	ClusterTTStatusResult result;
	bool retry;
	int aborted;

	for (aborted = 0; aborted < 2; aborted++) {
		reset_exact_origin_fixture();
		test_variable_cache.oldestClogXid = TEST_ORIGIN_XID;
		test_next_full_on_locate = FullTransactionIdFromEpochAndXid(0, 1200);
		/* The locator's status is deliberately contrary to canonical bytes. */
		test_durable_locator_status = aborted ? TT_SLOT_COMMITTED : TT_SLOT_ABORTED;
		if (aborted) {
			test_native_status = TRANSACTION_STATUS_ABORTED;
			test_tt_slot.status = TT_SLOT_ABORTED;
			test_tt_slot.commit_scn = InvalidScn;
		}
		retry = true;
		UT_ASSERT(cluster_runtime_visibility_local_terminal_lookup_exact(TEST_ORIGIN_XID, &key,
																		 &result, &retry));
		UT_ASSERT_EQ(key.undo_segment_id, TEST_TT_SEGMENT);
		UT_ASSERT_EQ(key.tt_slot_id, (uint32)4);
		UT_ASSERT_EQ(key.local_xid, TEST_ORIGIN_XID);
		UT_ASSERT_EQ(result.status,
					 aborted ? CLUSTER_TT_STATUS_ABORTED : CLUSTER_TT_STATUS_COMMITTED);
		UT_ASSERT_EQ(result.commit_scn, aborted ? InvalidScn : test_commit_scn);
		UT_ASSERT(result.authoritative);
		UT_ASSERT(!retry);
		UT_ASSERT_EQ(test_durable_locator_calls, 1);
		UT_ASSERT_EQ(test_candidate_acquire_calls, 1);
		UT_ASSERT_EQ(test_candidate_release_calls, 1);
		UT_ASSERT_EQ(test_candidate_held_count, 0);
		UT_ASSERT_EQ(test_xact_lock_depth, 0);
	}
}

UT_TEST(test_current_mx_terminal_fallback_cannot_bypass_native_era)
{
	ClusterTTStatusKey key;
	ClusterTTStatusResult result;
	ClusterCtrcTxnKeyV1 ctrc_key;
	uint32 grant;
	uint32 capability;
	int fault;

	for (fault = 0; fault < 2; fault++) {
		reset_exact_origin_fixture();
		test_xid_is_mine = false;
		if (fault == 0)
			test_next_full_xid = FullTransactionIdFromEpochAndXid(1, 1000);
		else
			test_variable_cache.oldestClogXid = TEST_ORIGIN_XID + 1;
		UT_ASSERT_EQ(current_mx_local_member_sample_exact(TEST_ORIGIN_XID, &key, &result, &grant,
														  &capability, &ctrc_key),
					 CMX_RESOLVE_UNKNOWN);
		UT_ASSERT_EQ(result.status, CLUSTER_TT_STATUS_UNKNOWN);
		UT_ASSERT(!result.authoritative);
		UT_ASSERT_EQ(grant, 0);
		UT_ASSERT_EQ(capability, 0);
		UT_ASSERT_EQ(test_durable_locator_calls, 0);
		UT_ASSERT_EQ(test_candidate_acquire_calls, 0);
	}
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
		&locator, &admission, &guard, &root, &key, &result, NULL, NULL));
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
	UT_ASSERT_EQ(
		memcmp(&resolution.locator_echo, &test_origin_locator, sizeof(test_origin_locator)), 0);
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

static void
test_remote_origin_does_not_depend_on_foreign_residency(ClusterTxResolveMode mode)
{
	int variant;
	int saved_node = cluster_node_id;

	for (variant = 0; variant < 3; variant++) {
		ClusterTxResolution resolution;
		ClusterTxResolveReason reason = CLUSTER_TX_RESOLVE_PROTOCOL;
		ClusterSemanticAdmissionToken admission = { 0 };
		ClusterTxOutcome outcome;

		reset_exact_origin_fixture();
		cluster_node_id = 1;
		test_remote_origin_enabled = true;
		/* Cold foreign header (the P56 candidate), known local first
		 * generation, and another locally observed generation all route
		 * without treating requester cache contents as origin authority. */
		test_candidate_sample_result
			= variant == 0 ? CLUSTER_UNDO_BLOCK0_NOT_PUBLISHED : CLUSTER_UNDO_BLOCK0_OK;
		test_candidate_data_generation = variant == 1 ? 0 : 3;
		admission.feature_bit = CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1;
		admission.record_generation = 5;
		admission.formation_epoch = test_formation_epoch;
		admission.side = CLUSTER_SEMANTIC_TARGET_SIDE;
		admission.entered = true;
		outcome = cluster_runtime_visibility_resolve_exact_origin_admitted(
			&test_origin_locator, mode, &admission, &resolution, &reason);
		cluster_node_id = saved_node;
		UT_ASSERT_EQ(outcome, CLUSTER_TX_IN_PROGRESS);
		UT_ASSERT_EQ(reason, CLUSTER_TX_RESOLVE_NONE);
		UT_ASSERT_EQ(test_remote_origin_calls, 1);
		UT_ASSERT_EQ(test_remote_origin_generation, UINT32_MAX);
		UT_ASSERT(!FullTransactionIdIsValid(resolution.local_original_full_xid));
		UT_ASSERT(!resolution.local_creation_target.valid);
		UT_ASSERT(!resolution.local_aborted_xmax_target.valid);
		UT_ASSERT_EQ(test_regular_root_resolve_calls, 0);
		UT_ASSERT_EQ(test_terminal_census_root_resolve_calls, 0);
		UT_ASSERT_EQ(test_candidate_acquire_calls, 0);
		UT_ASSERT_EQ(test_candidate_sample_calls, 0);
		UT_ASSERT_EQ(test_candidate_release_calls, 0);
		UT_ASSERT_EQ(test_candidate_cancel_calls, 0);
		UT_ASSERT_EQ(test_candidate_block0_copy_calls, 0);
		UT_ASSERT_EQ(test_candidate_data_copy_calls, 0);
		UT_ASSERT_EQ(test_undo_read_calls, 0);
		UT_ASSERT_EQ(test_tt_exact_calls, 0);
		UT_ASSERT_EQ(test_native_status_calls, 0);
	}
}

UT_TEST(test_visibility_remote_origin_selects_generation_without_foreign_scur)
{
	test_remote_origin_does_not_depend_on_foreign_residency(CLUSTER_TX_RESOLVE_VISIBILITY);
}

UT_TEST(test_terminal_census_remote_origin_selects_generation_without_foreign_scur)
{
	test_remote_origin_does_not_depend_on_foreign_residency(CLUSTER_TX_RESOLVE_TERMINAL_CENSUS);
}

UT_TEST(test_remote_origin_failed_proof_or_revoked_admission_clears_output)
{
	int variant;
	int saved_node = cluster_node_id;

	for (variant = 0; variant < 3; variant++) {
		ClusterTxResolution resolution;
		ClusterTxResolution zero = { 0 };
		ClusterTxResolveReason reason = CLUSTER_TX_RESOLVE_NONE;
		ClusterSemanticAdmissionToken admission = { 0 };
		ClusterTxOutcome outcome;

		reset_exact_origin_fixture();
		cluster_node_id = 1;
		test_remote_origin_enabled = true;
		test_remote_origin_revoke_admission = variant == 1;
		if (variant == 0)
			test_remote_origin_outcome = CLUSTER_TX_UNKNOWN;
		admission.feature_bit = CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1;
		admission.record_generation = 5;
		admission.formation_epoch = test_formation_epoch;
		admission.side = CLUSTER_SEMANTIC_TARGET_SIDE;
		admission.entered = variant != 2;
		memset(&resolution, 0xa5, sizeof(resolution));
		outcome = cluster_runtime_visibility_resolve_exact_origin_admitted(
			&test_origin_locator, CLUSTER_TX_RESOLVE_VISIBILITY, &admission, &resolution, &reason);
		cluster_node_id = saved_node;
		UT_ASSERT_EQ(outcome, CLUSTER_TX_UNKNOWN);
		UT_ASSERT_EQ(reason, variant == 0 ? CLUSTER_TX_RESOLVE_AUTHORITY_UNAVAILABLE
										  : CLUSTER_TX_RESOLVE_AUTHORITY_STALE);
		UT_ASSERT_EQ(memcmp(&resolution, &zero, sizeof(zero)), 0);
		UT_ASSERT_EQ(test_remote_origin_calls, variant == 2 ? 0 : 1);
		UT_ASSERT_EQ(test_candidate_acquire_calls, 0);
		UT_ASSERT_EQ(test_native_status_calls, 0);
	}
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
					 &resolution, &reason),
				 CLUSTER_TX_COMMITTED);
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

	UT_ASSERT_EQ(cluster_runtime_visibility_resolve_terminal_census_retained_local_exact(
					 &test_origin_locator, test_commit_scn, &admission, &resolution, &reason),
				 CLUSTER_TX_COMMITTED);
	UT_ASSERT_EQ(reason, CLUSTER_TX_RESOLVE_NONE);
	UT_ASSERT_EQ(resolution.outcome, CLUSTER_TX_COMMITTED);
	UT_ASSERT_EQ(resolution.proof_kind, CLUSTER_TX_PROOF_ORIGIN_DURABLE_TT_CLOG);
	UT_ASSERT_EQ((uint64)resolution.commit_scn, (uint64)test_commit_scn);
	UT_ASSERT_EQ(
		memcmp(&resolution.locator_echo, &test_origin_locator, sizeof(test_origin_locator)), 0);
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
	UT_ASSERT_EQ(cluster_runtime_visibility_resolve_terminal_census_retained_local_exact(
					 &test_origin_locator, test_commit_scn, &admission, &resolution, &reason),
				 CLUSTER_TX_UNKNOWN);
	UT_ASSERT_EQ(reason, CLUSTER_TX_RESOLVE_AUTHORITY_UNAVAILABLE);

	/* The retained path is closed to ACTIVE and invalid-SCN carriers. */
	test_local_freshref_pair_exact = true;
	test_origin_locator.itl_kind = ITL_FLAG_ACTIVE;
	reason = CLUSTER_TX_RESOLVE_NONE;
	UT_ASSERT_EQ(cluster_runtime_visibility_resolve_terminal_census_retained_local_exact(
					 &test_origin_locator, test_commit_scn, &admission, &resolution, &reason),
				 CLUSTER_TX_UNKNOWN);
	test_origin_locator.itl_kind = ITL_FLAG_NEEDS_CLEANOUT;
	UT_ASSERT_EQ(cluster_runtime_visibility_resolve_terminal_census_retained_local_exact(
					 &test_origin_locator, InvalidScn, &admission, &resolution, &reason),
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
					 &resolution, &reason),
				 CLUSTER_TX_COMMITTED);
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

	UT_ASSERT_EQ(
		cluster_runtime_visibility_resolve_exact_origin_admitted(
			&test_origin_locator, CLUSTER_TX_RESOLVE_VISIBILITY, &admission, &resolution, &reason),
		CLUSTER_TX_COMMITTED);
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

static void
test_local_data_readthrough_case(int variant)
{
	ClusterTxResolution resolution, zero = { 0 };
	ClusterTxResolveReason reason = CLUSTER_TX_RESOLVE_PROTOCOL;
	ClusterSemanticAdmissionToken admission = { 0 };
	volatile bool caught = false;
	ClusterTxOutcome outcome = CLUSTER_TX_UNKNOWN;
	bool positive = variant < 2 || variant == 5;
	int expected_pins = variant == 5 ? 0 : variant == 1 || variant == 4 || variant == 6 ? 1 : 2;

	reset_exact_origin_fixture();
	test_origin_locator.tt_wrap = TT_WRAP_INVALID;
	test_candidate_cold_copy_mask = variant == 5 ? 0 : variant == 1 ? 2 : 3;
	test_candidate_mutate_record_on_recheck = variant == 2;
	test_candidate_data_io_generation_drift = variant == 3;
	test_candidate_data_pool_disabled = variant == 4;
	test_candidate_data_io_throw = variant == 6;
	admission.feature_bit = CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1;
	admission.record_generation = 5;
	admission.formation_epoch = test_formation_epoch;
	admission.side = CLUSTER_SEMANTIC_TARGET_SIDE;
	admission.entered = true;
	memset(&resolution, 0xa5, sizeof(resolution));
	PG_TRY();
	{
		outcome = cluster_runtime_visibility_resolve_exact_origin_admitted(
			&test_origin_locator, CLUSTER_TX_RESOLVE_VISIBILITY, &admission, &resolution, &reason);
	}
	PG_CATCH();
	{
		caught = true;
	}
	PG_END_TRY();
	UT_ASSERT_EQ(caught, variant == 6);
	UT_ASSERT_EQ(test_candidate_data_pin_calls, expected_pins);
	UT_ASSERT_EQ(test_candidate_data_unpin_calls, variant == 4 || variant == 6 ? 0 : expected_pins);
	UT_ASSERT_EQ(test_candidate_max_held_count, 1);
	if (variant == 6) {
		UT_ASSERT_EQ(test_candidate_cancel_calls, 1);
		UT_ASSERT_EQ(test_candidate_release_calls, 0);
		UT_ASSERT_EQ(memcmp(&resolution, &zero, sizeof(zero)), 0);
		return;
	}
	UT_ASSERT_EQ(outcome, positive ? CLUSTER_TX_COMMITTED : CLUSTER_TX_UNKNOWN);
	UT_ASSERT_EQ(test_candidate_held_count, 0);
	UT_ASSERT_EQ(test_candidate_acquire_calls, variant == 4 ? 1 : 3);
	UT_ASSERT_EQ(test_candidate_release_calls, variant == 4 ? 1 : 3);
	if (positive) {
		UT_ASSERT_EQ(reason, CLUSTER_TX_RESOLVE_NONE);
		UT_ASSERT_EQ(resolution.commit_scn, test_commit_scn);
		UT_ASSERT_EQ(resolution.locator_echo.tt_wrap, TEST_ORIGIN_WRAP);
	} else {
		UT_ASSERT(reason != CLUSTER_TX_RESOLVE_NONE);
		UT_ASSERT_EQ(memcmp(&resolution, &zero, sizeof(zero)), 0);
	}
}

UT_TEST(test_local_origin_cold_first_and_recheck_keep_exact_tt_proof)
{
	test_local_data_readthrough_case(0);
}

UT_TEST(test_local_origin_cold_only_after_tt_phase_still_revalidates)
{
	test_local_data_readthrough_case(1);
}

UT_TEST(test_local_origin_cold_record_change_is_not_hidden)
{
	test_local_data_readthrough_case(2);
}

UT_TEST(test_local_origin_cold_generation_change_is_not_hidden)
{
	test_local_data_readthrough_case(3);
}

UT_TEST(test_local_origin_disabled_pool_is_unknown_not_success)
{
	test_local_data_readthrough_case(4);
}

UT_TEST(test_local_origin_warm_data_adds_no_readthrough)
{
	test_local_data_readthrough_case(5);
}

UT_TEST(test_local_origin_readthrough_exception_cancels_guard)
{
	test_local_data_readthrough_case(6);
}

UT_TEST(test_split_origin_cold_first_and_recheck_never_start_io)
{
	for (int recheck = 0; recheck < 2; recheck++) {
		ClusterRuntimeVisibilityOriginPlan plan;
		ClusterTxResolution resolution;
		ClusterTxResolveReason reason;
		ClusterSemanticAdmissionToken admission = { 0 };
		ClusterUndoBlock0CurrentGuard guard = { 0 };
		ClusterUndoBlock0ResolvedRoot data_root = { 0 }, tt_root = { 0 };
		ClusterUndoBlock0Generation expected = { true, 17 };

		reset_exact_origin_fixture();
		test_origin_locator.tt_wrap = TT_WRAP_INVALID;
		test_candidate_cold_copy_mask = recheck ? 2 : 1;
		admission.feature_bit = CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1;
		admission.record_generation = 5;
		admission.formation_epoch = test_formation_epoch;
		admission.side = CLUSTER_SEMANTIC_TARGET_SIDE;
		admission.entered = true;
		data_root.intent = tt_root.intent = CLUSTER_UNDO_PATH_RUNTIME_SHARED;
		data_root.root_id = 91;
		tt_root.root_id = 92;
		data_root.root_generation = tt_root.root_generation = 7;
		UT_ASSERT_EQ(cluster_runtime_visibility_origin_plan_freeze_data_held(
						 &test_origin_locator, CLUSTER_TX_RESOLVE_VISIBILITY, &admission, &expected,
						 &guard, &data_root, &plan, &resolution, &reason),
					 recheck ? CLUSTER_RUNTIME_VISIBILITY_ORIGIN_NEEDS_CANONICAL
							 : CLUSTER_RUNTIME_VISIBILITY_ORIGIN_FAILED);
		if (recheck) {
			UT_ASSERT(cluster_runtime_visibility_origin_plan_sample_canonical_held(
				&plan, CLUSTER_TX_RESOLVE_VISIBILITY, &admission, &guard, &tt_root, &reason));
			UT_ASSERT_EQ(cluster_runtime_visibility_origin_plan_recheck_data_held(
							 &plan, CLUSTER_TX_RESOLVE_VISIBILITY, &admission, &guard, &data_root,
							 &resolution, &reason),
						 CLUSTER_TX_UNKNOWN);
		}
		UT_ASSERT_EQ(test_candidate_data_pin_calls, 0);
		UT_ASSERT_EQ(test_candidate_data_unpin_calls, 0);
	}
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

	UT_ASSERT_EQ(
		cluster_runtime_visibility_resolve_exact_origin_admitted(
			&test_origin_locator, CLUSTER_TX_RESOLVE_VISIBILITY, &admission, &resolution, &reason),
		CLUSTER_TX_IN_PROGRESS);
	UT_ASSERT_EQ(reason, CLUSTER_TX_RESOLVE_NONE);
	UT_ASSERT_EQ(resolution.outcome, CLUSTER_TX_IN_PROGRESS);
	UT_ASSERT_EQ(resolution.proof_kind, CLUSTER_TX_PROOF_ORIGIN_DURABLE_TT_CLOG);
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
	const uint16 wraps[] = { 0, TEST_ORIGIN_WRAP - 1, TEST_ORIGIN_WRAP };
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

UT_TEST(test_reborn_canonical_nonabort_refuses_in_both_modes)
{
	const XidStatus states[] = { TRANSACTION_STATUS_IN_PROGRESS, TRANSACTION_STATUS_COMMITTED,
								 TRANSACTION_STATUS_SUB_COMMITTED };
	ClusterSemanticAdmissionToken admission;
	ClusterTxResolution resolution;
	ClusterTxResolution zero = { 0 };
	ClusterTxResolveReason reason;

	for (int shape = 0; shape < 2; shape++) {
		for (unsigned i = 0; i < 2 * lengthof(states); i++) {
			ClusterTxResolveMode mode = i >= lengthof(states) ? CLUSTER_TX_RESOLVE_TERMINAL_CENSUS
															  : CLUSTER_TX_RESOLVE_VISIBILITY;

			reset_reborn_abort_fixture(&admission, shape);
			test_native_status = states[i % lengthof(states)];
			memset(&resolution, 0xa5, sizeof(resolution));
			UT_ASSERT_EQ(cluster_runtime_visibility_resolve_exact_origin_admitted(
							 &test_origin_locator, mode, &admission, &resolution, &reason),
						 CLUSTER_TX_UNKNOWN);
			UT_ASSERT_EQ(memcmp(&resolution, &zero, sizeof(zero)), 0);
			UT_ASSERT_EQ(test_native_status_calls, 1);
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
	ClusterTxResolution zero = { 0 };
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
						 &resolution, &reason),
					 CLUSTER_TX_UNKNOWN);
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
	ClusterTxResolution zero = { 0 };
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
							 &resolution, &reason),
						 CLUSTER_TX_UNKNOWN);
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
static void
check_reborn_split_plan_rechecks_before_publication(ClusterTxResolveMode mode)
{
	for (int shape = 0; shape < 2; shape++) {
		for (int failure = 0; failure < 5; failure++) {
			ClusterSemanticAdmissionToken admission;
			ClusterRuntimeVisibilityOriginPlan plan;
			ClusterRuntimeVisibilityCanonicalDiagnostic diagnostic;
			ClusterTxResolution resolution;
			ClusterTxResolution zero = { 0 };
			ClusterTxResolveReason reason;
			ClusterUndoBlock0CurrentGuard data_guard = { 0 };
			ClusterUndoBlock0CurrentGuard tt_guard = { 0 };
			ClusterUndoBlock0ResolvedRoot data_root = { 0 };
			ClusterUndoBlock0ResolvedRoot tt_root = { 0 };
			PGAlignedBlock copied;
			PGAlignedBlock expected;

			reset_reborn_abort_fixture(&admission, shape);
			data_root.intent = tt_root.intent = CLUSTER_UNDO_PATH_RUNTIME_SHARED;
			data_root.root_id = 91;
			tt_root.root_id = 92;
			data_root.root_generation = tt_root.root_generation = 7;
			UT_ASSERT_EQ(cluster_runtime_visibility_origin_plan_freeze_data_held(
							 &test_origin_locator, mode, &admission, NULL, &data_guard, &data_root,
							 &plan, &resolution, &reason),
						 CLUSTER_RUNTIME_VISIBILITY_ORIGIN_NEEDS_CANONICAL);
			UT_ASSERT(cluster_runtime_visibility_origin_plan_sample_canonical_held(
				&plan, mode, &admission, &tt_guard, &tt_root, &reason));
			UT_ASSERT(
				cluster_runtime_visibility_origin_plan_canonical_diagnostic(&plan, &diagnostic));
			UT_ASSERT(diagnostic.native_sampled);
			UT_ASSERT_EQ(diagnostic.native_status, TRANSACTION_STATUS_ABORTED);
			UT_ASSERT_EQ(diagnostic.first_failure,
						 CLUSTER_RUNTIME_VISIBILITY_CANONICAL_FAILURE_NONE);
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
							 &plan, mode, &admission, &data_guard, &data_root, &resolution,
							 copied.data, &reason),
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

UT_TEST(test_reborn_split_plan_rechecks_before_publication)
{
	check_reborn_split_plan_rechecks_before_publication(CLUSTER_TX_RESOLVE_VISIBILITY);
}

UT_TEST(test_terminal_census_split_plan_rechecks_before_publication)
{
	check_reborn_split_plan_rechecks_before_publication(CLUSTER_TX_RESOLVE_TERMINAL_CENSUS);
}

/* Execute the real origin provider with terminal-only admission. Physical
 * TT reuse must not hide an independently proved old ABORT, but neither a
 * successor's outcome nor an unproved/changed native byte can clean a page. */
UT_TEST(test_terminal_census_recycled_abort_proof_and_boundaries)
{
	for (int shape = 0; shape < 3; shape++) {
		for (int same_segment = 0; same_segment < 2; same_segment++) {
			for (int fault = 0; fault < 9; fault++) {
				ClusterSemanticAdmissionToken admission;
				ClusterTxResolution resolution;
				ClusterTxResolution zero = { 0 };
				ClusterTxResolveReason reason;
				bool rejected = fault != 0;

				reset_recycled_abort_fixture(&admission);
				if (shape < 2)
					reset_reborn_abort_fixture(&admission, shape);
				if (same_segment)
					test_origin_record.tt_slot_segment_id = TEST_RECORD_SEGMENT;
				switch (fault) {
				case 1:
					test_native_status = TRANSACTION_STATUS_IN_PROGRESS;
					break;
				case 2:
					test_native_status = TRANSACTION_STATUS_COMMITTED;
					break;
				case 3:
					test_native_status = TRANSACTION_STATUS_SUB_COMMITTED;
					break;
				case 4:
					test_native_origin_provable = false;
					break;
				case 5:
					test_variable_cache.oldestClogXid = TEST_ORIGIN_XID + 1;
					break;
				case 6:
					test_no_raw_reuse_drift_on_recheck = true;
					rejected = !same_segment;
					break;
				case 7:
					test_candidate_mutate_record_on_recheck = true;
					rejected = !same_segment;
					break;
				case 8:
					test_tt_slot.wrap = TT_WRAP_INVALID;
					break;
				}
				memset(&resolution, 0xa5, sizeof(resolution));
				UT_ASSERT_EQ(cluster_runtime_visibility_resolve_exact_origin_admitted(
								 &test_origin_locator, CLUSTER_TX_RESOLVE_TERMINAL_CENSUS,
								 &admission, &resolution, &reason),
							 rejected ? CLUSTER_TX_UNKNOWN : CLUSTER_TX_ABORTED);
				if (rejected)
					UT_ASSERT_EQ(memcmp(&resolution, &zero, sizeof(zero)), 0);
				else {
					UT_ASSERT_EQ(reason, CLUSTER_TX_RESOLVE_NONE);
					UT_ASSERT_EQ(resolution.locator_echo.xid, TEST_ORIGIN_XID);
					UT_ASSERT_EQ(resolution.locator_echo.tt_wrap, TEST_ORIGIN_WRAP);
					UT_ASSERT_EQ(resolution.top_xid, TEST_ORIGIN_XID);
					UT_ASSERT_EQ(resolution.commit_scn, InvalidScn);
					UT_ASSERT_EQ(resolution.proof_kind, CLUSTER_TX_PROOF_ORIGIN_DURABLE_TT_CLOG);
					UT_ASSERT_EQ(test_native_status_calls, same_segment ? 1 : 2);
				}
				UT_ASSERT_EQ(test_native_fence_depth, 0);
				UT_ASSERT_EQ(test_xact_lock_depth, 0);
				UT_ASSERT_EQ(test_candidate_held_count, 0);
			}
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
	for (int variant = 0; variant < 6; variant++) {
		int shape = variant % 3;
		ClusterTxResolveMode mode
			= variant < 3 ? CLUSTER_TX_RESOLVE_VISIBILITY : CLUSTER_TX_RESOLVE_TERMINAL_CENSUS;
		volatile bool caught = false;

		reset_recycled_abort_fixture(&admission);
		if (shape < 2)
			reset_reborn_abort_fixture(&admission, shape);
		test_native_throw = true;
		PG_TRY();
		{
			(void)cluster_runtime_visibility_resolve_exact_origin_admitted(
				&test_origin_locator, mode, &admission, &resolution, &reason);
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
	UT_ASSERT(cluster_runtime_visibility_origin_plan_canonical_logical(&plan, &canonical_logical));
	UT_ASSERT_EQ(canonical_logical.segment_id, TEST_TT_SEGMENT);
	test_candidate_sample_result = CLUSTER_UNDO_BLOCK0_NOT_PUBLISHED;
	UT_ASSERT(!cluster_runtime_visibility_origin_plan_sample_canonical_held(
		&plan, CLUSTER_TX_RESOLVE_VISIBILITY, &admission, &tt_guard, &tt_root, &reason));
	UT_ASSERT_EQ(reason, CLUSTER_TX_RESOLVE_AUTHORITY_UNAVAILABLE);
	memset(&diagnostic, 0xa5, sizeof(diagnostic));
	UT_ASSERT(cluster_runtime_visibility_origin_plan_canonical_diagnostic(&plan, &diagnostic));
	UT_ASSERT(diagnostic.valid);
	UT_ASSERT_EQ(diagnostic.first_failure,
				 CLUSTER_RUNTIME_VISIBILITY_CANONICAL_FAILURE_GENERATION_SAMPLE);
	UT_ASSERT_EQ(diagnostic.generation_result, CLUSTER_UNDO_BLOCK0_NOT_PUBLISHED);
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

	UT_ASSERT_EQ(
		cluster_runtime_visibility_resolve_exact_origin_admitted(
			&test_origin_locator, CLUSTER_TX_RESOLVE_VISIBILITY, &admission, &resolution, &reason),
		CLUSTER_TX_UNKNOWN);
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
						 &resolution, &reason),
					 CLUSTER_TX_UNKNOWN);
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
	UT_ASSERT_EQ(
		cluster_runtime_visibility_resolve_exact_origin_admitted(
			&test_origin_locator, CLUSTER_TX_RESOLVE_VISIBILITY, &admission, &resolution, &reason),
		CLUSTER_TX_UNKNOWN);
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
	UT_ASSERT_EQ(
		cluster_runtime_visibility_resolve_exact_origin_admitted(
			&test_origin_locator, CLUSTER_TX_RESOLVE_VISIBILITY, &admission, &resolution, &reason),
		CLUSTER_TX_UNKNOWN);
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

	UT_ASSERT_EQ(
		cluster_runtime_visibility_resolve_exact_origin_admitted(
			&test_origin_locator, CLUSTER_TX_RESOLVE_VISIBILITY, &admission, &resolution, &reason),
		CLUSTER_TX_UNKNOWN);
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

	UT_ASSERT_EQ(
		cluster_runtime_visibility_resolve_exact_origin_admitted(
			&test_origin_locator, CLUSTER_TX_RESOLVE_VISIBILITY, &admission, &resolution, &reason),
		CLUSTER_TX_UNKNOWN);
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
		test_tt_slot.commit_scn = physical_kind == 0 ? scn_encode(0, 79) : InvalidScn;
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
						 &resolution, &reason),
					 CLUSTER_TX_UNKNOWN);
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
						 &resolution, &reason),
					 CLUSTER_TX_UNKNOWN);
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
						 &resolution, &reason),
					 CLUSTER_TX_UNKNOWN);
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
					 &resolution, &reason),
				 CLUSTER_TX_UNKNOWN);
	UT_ASSERT_EQ(reason, CLUSTER_TX_RESOLVE_AUTHORITY_UNAVAILABLE);
	UT_ASSERT_EQ(memcmp(&resolution, &zero, sizeof(resolution)), 0);
	UT_ASSERT_EQ(test_current_owner_calls, 0);
	UT_ASSERT_EQ(test_current_segment_calls, 0);
	/* The terminal abort probe checks its origin window but cannot project
	 * this allocator/live-owner fixture or sample an unproved native xid. */
	UT_ASSERT_EQ(test_native_fence_lock_calls, 1);
	UT_ASSERT_EQ(test_native_fence_unlock_calls, 1);
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
					 &resolution, &reason),
				 CLUSTER_TX_UNKNOWN);
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
					 &resolution, &reason),
				 CLUSTER_TX_UNKNOWN);
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
					 &resolution, &reason),
				 CLUSTER_TX_UNKNOWN);
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
					 &test_origin_locator, CLUSTER_TX_RESOLVE_TERMINAL_CENSUS, &admission,
					 &resolution, &reason),
				 CLUSTER_TX_UNKNOWN);
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
	ClusterTxResolution zero = { 0 };
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
	UT_ASSERT_EQ(
		memcmp(&resolution.locator_echo, &test_origin_locator, sizeof(test_origin_locator)), 0);
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
	ClusterTxResolution zero = { 0 };
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
	UT_ASSERT_EQ(
		memcmp(&resolution.locator_echo, &test_origin_locator, sizeof(test_origin_locator)), 0);
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
	ClusterTxResolution zero = { 0 };
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
	ClusterTxResolution zero = { 0 };
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
	ClusterTxResolution zero = { 0 };
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
	ClusterTxResolution zero = { 0 };
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
	ClusterTxResolution zero = { 0 };
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
	ClusterTxResolution zero = { 0 };
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
	ClusterTxResolution zero = { 0 };
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
	ClusterTxResolution zero = { 0 };
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
	ClusterTxResolution zero = { 0 };
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
	ClusterTxResolution zero = { 0 };
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

/* Exercise the guarded canonical DATA/TT/DATA consumer, including its
 * final release/admission checks, rather than the legacy C1b entry. */
static ClusterTxOutcome
test_resolve_memo_origin(ClusterTxResolveMode mode, ClusterTxResolution *out,
						 ClusterTxResolveReason *reason)
{
	ClusterSemanticAdmissionToken admission = { 0 };

	admission.feature_bit = CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1;
	admission.record_generation = 5;
	admission.formation_epoch = test_formation_epoch;
	admission.side = CLUSTER_SEMANTIC_TARGET_SIDE;
	admission.entered = true;
	return cluster_runtime_visibility_resolve_exact_origin_admitted(&test_origin_locator, mode,
																	&admission, out, reason);
}

UT_TEST(test_exact_origin_memo_reuses_only_proven_terminal_outcomes)
{
	int variant;

	for (variant = 0; variant < 6; variant++) {
		ClusterTxResolution resolution;
		ClusterTxResolveReason reason;
		ClusterRuntimeVisibilityCounters counters;
		ClusterTxOutcome expected = variant < 2 ? CLUSTER_TX_COMMITTED : CLUSTER_TX_ABORTED;
		int data_copies;
		int native_calls = variant < 4 ? 1 : 2;
		TransactionId top_xid = variant < 4 ? TEST_ORIGIN_XID : TEST_ORIGIN_XID - 1;

		reset_exact_origin_fixture();
		test_origin_locator.tt_wrap = TT_WRAP_INVALID;
		if (variant % 2 == 0)
			test_origin_record.tt_slot_segment_id = TEST_RECORD_SEGMENT;
		if (expected == CLUSTER_TX_ABORTED) {
			test_native_status = TRANSACTION_STATUS_ABORTED;
			test_tt_slot.status = TT_SLOT_ABORTED;
			test_tt_slot.commit_scn = InvalidScn;
		}
		if (variant >= 4) {
			test_tt_slot.status = TT_SLOT_ACTIVE;
			test_subtrans_chain[0] = TEST_ORIGIN_XID;
			test_subtrans_chain[1] = top_xid;
			test_subtrans_chain_count = 2;
			set_native_status_sample(0, TEST_ORIGIN_XID, TRANSACTION_STATUS_SUB_COMMITTED);
			set_native_status_sample(1, top_xid, TRANSACTION_STATUS_ABORTED);
		}
		UT_ASSERT_EQ(test_resolve_memo_origin(CLUSTER_TX_RESOLVE_VISIBILITY, &resolution, &reason),
					 expected);
		UT_ASSERT_EQ(test_native_status_calls, native_calls);
		data_copies = test_candidate_data_copy_calls;
		/* Terminal evidence survives a later demand SCN, but the response
		 * must still stamp the authority of the new guarded observation. */
		test_authority_scn++;
		test_flush_lsn++;
		UT_ASSERT_EQ(
			test_resolve_memo_origin(CLUSTER_TX_RESOLVE_TERMINAL_CENSUS, &resolution, &reason),
			expected);
		UT_ASSERT_EQ(test_native_status_calls, native_calls);
		UT_ASSERT_EQ(test_candidate_data_copy_calls, 2 * data_copies);
		UT_ASSERT_EQ(resolution.top_xid, top_xid);
		UT_ASSERT_EQ(resolution.proof_kind, variant < 4 ? CLUSTER_TX_PROOF_ORIGIN_DURABLE_TT_CLOG
														: CLUSTER_TX_PROOF_ORIGIN_SUBTRANS_TOP);
		UT_ASSERT_EQ(test_subtrans_parent_calls, variant < 4 ? 0 : 4);
		UT_ASSERT_EQ(resolution.commit_scn,
					 expected == CLUSTER_TX_COMMITTED ? test_commit_scn : InvalidScn);
		UT_ASSERT_EQ(resolution.authority.live_hwm_lsn, test_flush_lsn);
		UT_ASSERT_EQ(resolution.authority.authority_scn, test_authority_scn);
		UT_ASSERT_EQ(test_candidate_held_count, 0);
		UT_ASSERT_EQ(test_candidate_acquire_calls, test_candidate_release_calls);
		cluster_runtime_visibility_exact_counters_snapshot(&counters);
		UT_ASSERT_EQ(counters.local_memo_hits, 1);
		UT_ASSERT_EQ(counters.local_memo_publishes, 1);
	}
}

UT_TEST(test_local_terminal_consumes_exact_origin_memo)
{
	int aborted;

	for (aborted = 0; aborted < 2; aborted++) {
		ClusterTxResolution resolution;
		ClusterTxResolveReason reason;
		ClusterTTStatusKey key;
		ClusterTTStatusResult result;
		ClusterRuntimeVisibilityCounters counters;
		bool retry = true;
		int copies;

		reset_exact_origin_fixture();
		test_origin_locator.tt_wrap = TT_WRAP_INVALID;
		if (aborted) {
			test_native_status = TRANSACTION_STATUS_ABORTED;
			test_tt_slot.status = TT_SLOT_ABORTED;
			test_tt_slot.commit_scn = InvalidScn;
		} else
			test_current_segment = TEST_TT_SEGMENT;
		UT_ASSERT_EQ(test_resolve_memo_origin(CLUSTER_TX_RESOLVE_VISIBILITY, &resolution, &reason),
					 aborted ? CLUSTER_TX_ABORTED : CLUSTER_TX_COMMITTED);
		UT_ASSERT_EQ(test_native_status_calls, 1);
		copies = test_candidate_block0_copy_calls;
		UT_ASSERT(cluster_runtime_visibility_local_terminal_lookup_exact(TEST_ORIGIN_XID, &key,
																		 &result, &retry));
		UT_ASSERT_EQ(result.status,
					 aborted ? CLUSTER_TT_STATUS_ABORTED : CLUSTER_TT_STATUS_COMMITTED);
		UT_ASSERT_EQ(result.commit_scn, aborted ? InvalidScn : test_commit_scn);
		UT_ASSERT(result.authoritative);
		UT_ASSERT(!retry);
		UT_ASSERT_EQ(key.undo_segment_id, TEST_TT_SEGMENT);
		UT_ASSERT_EQ(key.tt_slot_id, cluster_tt_slot_offset_to_id(TEST_TT_OFFSET));
		UT_ASSERT_EQ(test_native_status_calls, 1);
		UT_ASSERT_EQ(test_candidate_block0_copy_calls, copies + 1);
		UT_ASSERT_EQ(test_ctrc_touch_calls, 0);
		UT_ASSERT_EQ(test_candidate_held_count, 0);
		UT_ASSERT_EQ(test_candidate_acquire_calls, test_candidate_release_calls);
		cluster_runtime_visibility_exact_counters_snapshot(&counters);
		UT_ASSERT_EQ(counters.local_memo_hits, 1);
		UT_ASSERT_EQ(counters.local_memo_publishes, 1);
	}
}

UT_TEST(test_physical_terminal_populates_shared_exact_memo)
{
	int variant;

	for (variant = 0; variant < 4; variant++) {
		ClusterTxResolution resolution;
		ClusterTxResolveReason reason;
		ClusterTTStatusKey key;
		ClusterTTStatusResult result;
		ClusterRuntimeVisibilityCounters counters;
		bool aborted = variant >= 2;
		bool retry = true;
		int attempt;

		reset_exact_origin_fixture();
		test_origin_locator.tt_wrap = TT_WRAP_INVALID;
		if (variant % 2 == 0)
			test_current_segment = TEST_TT_SEGMENT;
		if (aborted) {
			test_native_status = TRANSACTION_STATUS_ABORTED;
			test_tt_slot.status = TT_SLOT_ABORTED;
			test_tt_slot.commit_scn = InvalidScn;
		}
		for (attempt = 0; attempt < 2; attempt++) {
			UT_ASSERT(cluster_runtime_visibility_local_terminal_lookup_exact(TEST_ORIGIN_XID, &key,
																			 &result, &retry));
			UT_ASSERT_EQ(result.status,
						 aborted ? CLUSTER_TT_STATUS_ABORTED : CLUSTER_TT_STATUS_COMMITTED);
			UT_ASSERT_EQ(result.commit_scn, aborted ? InvalidScn : test_commit_scn);
			UT_ASSERT(result.authoritative);
			UT_ASSERT(!retry);
			UT_ASSERT_EQ(test_native_status_calls, 1);
			UT_ASSERT_EQ(test_candidate_block0_copy_calls, attempt + 1);
		}
		/* The DATA consumer can reuse the physical proof only after its own
		 * exact DATA/TT/DATA checks; the memo contains no row locator. */
		UT_ASSERT_EQ(test_resolve_memo_origin(CLUSTER_TX_RESOLVE_VISIBILITY, &resolution, &reason),
					 aborted ? CLUSTER_TX_ABORTED : CLUSTER_TX_COMMITTED);
		UT_ASSERT_EQ(test_native_status_calls, 1);
		/* DATA's own segment header and the canonical TT header are both
		 * still sampled by the existing cross-segment continuation. */
		UT_ASSERT_EQ(test_candidate_block0_copy_calls, 4);
		UT_ASSERT_EQ(test_candidate_data_copy_calls, 2);
		UT_ASSERT_EQ(test_ctrc_touch_calls, 0);
		UT_ASSERT_EQ(test_candidate_held_count, 0);
		UT_ASSERT_EQ(test_candidate_acquire_calls, test_candidate_release_calls);
		cluster_runtime_visibility_exact_counters_snapshot(&counters);
		UT_ASSERT_EQ(counters.local_memo_hits, 2);
		UT_ASSERT_EQ(counters.local_memo_publishes, 1);
	}
}

UT_TEST(test_physical_terminal_publication_waits_for_final_sample_fences)
{
	int fault;

	for (fault = 0; fault < 4; fault++) {
		ClusterSemanticAdmissionToken admission = { 0 };
		ClusterUndoBlock0CurrentGuard guard = { 0 };
		ClusterUndoBlock0ResolvedRoot root
			= { .intent = CLUSTER_UNDO_PATH_RUNTIME_SHARED, .root_id = 92, .root_generation = 7 };
		ClusterTTSlotPhysicalLocator locator = { .segment_id = TEST_TT_SEGMENT,
												 .xid = TEST_ORIGIN_XID,
												 .slot_offset = TEST_TT_OFFSET,
												 .wrap = TEST_ORIGIN_WRAP };
		ClusterTTStatusKey key;
		ClusterTTStatusResult result;
		ClusterRuntimeVisibilityCounters counters;

		reset_exact_origin_fixture();
		admission.feature_bit = CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1;
		admission.record_generation = 5;
		admission.formation_epoch = test_formation_epoch;
		admission.side = CLUSTER_SEMANTIC_TARGET_SIDE;
		admission.entered = true;
		/* The caller has already resolved the supplied root. Call the held
		 * sampler directly so each fault lands in its final recheck. */
		test_regular_root_resolve_calls = 1;
		switch (fault) {
		case 0:
			test_candidate_copy_generation_drift = true;
			break;
		case 1:
			test_current_segment_drift_on_recheck = true;
			break;
		case 2:
			test_current_segment = TEST_TT_SEGMENT;
			test_current_owner_available = true;
			test_current_owner.status = CTS_COMMITTED;
			test_current_owner.commit_scn = test_commit_scn;
			test_current_owner_calls = 1;
			test_current_owner_drift_on_recheck = true;
			break;
		case 3:
			test_candidate_root_drift = true;
			break;
		}
		UT_ASSERT(!cluster_runtime_visibility_physical_locator_sample_held(
			&locator, &admission, &guard, &root, &key, &result, NULL, NULL));
		UT_ASSERT_EQ(result.status, CLUSTER_TT_STATUS_UNKNOWN);
		UT_ASSERT(!result.authoritative);
		UT_ASSERT_EQ(test_native_status_calls, 1);
		cluster_runtime_visibility_exact_counters_snapshot(&counters);
		UT_ASSERT_EQ(counters.local_memo_hits, 0);
		UT_ASSERT_EQ(counters.local_memo_publishes, 0);

		/* A later successful sample must still obtain native proof. */
		test_candidate_copy_generation_drift = false;
		test_current_segment_drift_on_recheck = false;
		test_current_owner_drift_on_recheck = false;
		test_candidate_root_drift = false;
		test_current_owner_calls = 0;
		test_current_segment_calls = 0;
		test_regular_root_resolve_calls = 0;
		UT_ASSERT(cluster_runtime_visibility_local_terminal_lookup_exact(TEST_ORIGIN_XID, &key,
																		 &result, NULL));
		UT_ASSERT_EQ(result.status, CLUSTER_TT_STATUS_COMMITTED);
		UT_ASSERT_EQ(test_native_status_calls, 2);
		cluster_runtime_visibility_exact_counters_snapshot(&counters);
		UT_ASSERT_EQ(counters.local_memo_hits, 0);
		UT_ASSERT_EQ(counters.local_memo_publishes, 1);
	}
}

UT_TEST(test_physical_terminal_never_memoizes_unproven_outcomes)
{
	int variant;

	for (variant = 0; variant < 5; variant++) {
		ClusterTTStatusKey key;
		ClusterTTStatusResult result;
		ClusterRuntimeVisibilityCounters counters;
		int attempt;

		reset_exact_origin_fixture();
		if (variant < 2) {
			test_native_status = TRANSACTION_STATUS_IN_PROGRESS;
			test_twophase_xid = TEST_ORIGIN_XID;
			test_twophase_prepared = variant == 1;
		} else if (variant == 2) {
			test_tt_slot.status = TT_SLOT_ABORTED;
			test_tt_slot.commit_scn = InvalidScn;
		} else {
			test_native_status = TRANSACTION_STATUS_ABORTED;
			if (variant == 4) {
				test_tt_slot.status = TT_SLOT_ACTIVE;
				test_tt_slot.commit_scn = InvalidScn;
			}
		}
		for (attempt = 0; attempt < 2; attempt++) {
			UT_ASSERT(!cluster_runtime_visibility_local_terminal_lookup_exact(TEST_ORIGIN_XID, &key,
																			  &result, NULL));
			UT_ASSERT_EQ(result.status, CLUSTER_TT_STATUS_UNKNOWN);
			UT_ASSERT(!result.authoritative);
		}
		UT_ASSERT_EQ(test_candidate_block0_copy_calls, 2);
		UT_ASSERT_EQ(test_candidate_held_count, 0);
		UT_ASSERT_EQ(test_candidate_acquire_calls, test_candidate_release_calls);
		cluster_runtime_visibility_exact_counters_snapshot(&counters);
		UT_ASSERT_EQ(counters.local_memo_hits, 0);
		UT_ASSERT_EQ(counters.local_memo_publishes, 0);
	}
}

UT_TEST(test_local_terminal_memo_requires_current_physical_identity)
{
	int variant;

	for (variant = 0; variant < 7; variant++) {
		ClusterTxResolution resolution;
		ClusterTxResolveReason reason;
		ClusterTTStatusKey key;
		ClusterTTStatusResult result;
		ClusterRuntimeVisibilityCounters counters;
		bool retry = true;
		bool valid = variant != 3 && variant != 4;

		reset_exact_origin_fixture();
		test_origin_locator.tt_wrap = TT_WRAP_INVALID;
		UT_ASSERT_EQ(test_resolve_memo_origin(CLUSTER_TX_RESOLVE_VISIBILITY, &resolution, &reason),
					 CLUSTER_TX_COMMITTED);
		switch (variant) {
		case 0:
			test_formation_epoch++;
			break;
		case 1:
			test_candidate_tt_generation++;
			break;
		case 2:
			test_candidate_root_drift = true;
			break;
		case 3:
			test_tt_slot.wrap++;
			break;
		case 4:
			test_tt_slot.xid++;
			break;
		case 5:
			test_tt_slot.commit_scn++;
			break;
		case 6:
			test_tt_slot.status = TT_SLOT_ABORTED;
			test_tt_slot.commit_scn = InvalidScn;
			test_native_status = TRANSACTION_STATUS_ABORTED;
			break;
		}
		UT_ASSERT_EQ(cluster_runtime_visibility_local_terminal_lookup_exact(TEST_ORIGIN_XID, &key,
																			&result, &retry),
					 valid);
		UT_ASSERT_EQ(result.status, !valid		   ? CLUSTER_TT_STATUS_UNKNOWN
									: variant == 6 ? CLUSTER_TT_STATUS_ABORTED
												   : CLUSTER_TT_STATUS_COMMITTED);
		UT_ASSERT_EQ(result.commit_scn,
					 valid && variant != 6 ? test_tt_slot.commit_scn : InvalidScn);
		UT_ASSERT_EQ(result.authoritative, valid);
		UT_ASSERT(!retry);
		UT_ASSERT_EQ(test_native_status_calls, valid ? 2 : 1);
		UT_ASSERT_EQ(test_candidate_held_count, 0);
		UT_ASSERT_EQ(test_candidate_acquire_calls, test_candidate_release_calls);
		cluster_runtime_visibility_exact_counters_snapshot(&counters);
		UT_ASSERT_EQ(counters.local_memo_hits, 0);
		UT_ASSERT_EQ(counters.local_memo_publishes, valid ? 2 : 1);
	}
}

UT_TEST(test_local_terminal_memo_hit_preserves_final_fences)
{
	int fault;

	for (fault = 0; fault < 4; fault++) {
		ClusterTxResolution resolution;
		ClusterTxResolveReason reason;
		ClusterRuntimeVisibilityCounters counters;

		reset_exact_origin_fixture();
		test_origin_locator.tt_wrap = TT_WRAP_INVALID;
		UT_ASSERT_EQ(test_resolve_memo_origin(CLUSTER_TX_RESOLVE_VISIBILITY, &resolution, &reason),
					 CLUSTER_TX_COMMITTED);
		if (fault == 0)
			test_candidate_copy_generation_drift = true;
		else if (fault == 1)
			test_candidate_release_admission_drift = true;
		else
			test_release_scope_drift = fault == 2 ? 6 : 2;
		assert_local_terminal_unknown();
		UT_ASSERT_EQ(test_native_status_calls, 1);
		UT_ASSERT_EQ(test_candidate_acquire_calls, test_candidate_release_calls);
		cluster_runtime_visibility_exact_counters_snapshot(&counters);
		UT_ASSERT_EQ(counters.local_memo_hits, 1);
		UT_ASSERT_EQ(counters.local_memo_publishes, 1);
	}
}

UT_TEST(test_exact_origin_memo_never_promotes_precommit_or_prepared)
{
	int variant;
	ClusterSemanticAdmissionToken admission;
	ClusterTxResolution resolution;
	ClusterTxResolveReason reason;
	ClusterRuntimeVisibilityCounters counters;

	for (variant = 0; variant < 3; variant++) {
		ClusterTxOutcome expected = variant == 0 ? CLUSTER_TX_IN_PROGRESS : CLUSTER_TX_PREPARED;
		int native_calls;

		reset_exact_origin_fixture();
		test_origin_locator.tt_wrap = TT_WRAP_INVALID;
		test_native_status = TRANSACTION_STATUS_IN_PROGRESS;
		test_twophase_xid = TEST_ORIGIN_XID;
		test_twophase_prepared = variant != 0;
		if (variant != 0) {
			test_tt_slot.status = variant == 1 ? TT_SLOT_ACTIVE : TT_SLOT_ABORTED;
			test_tt_slot.commit_scn = InvalidScn;
		}
		UT_ASSERT_EQ(test_resolve_memo_origin(CLUSTER_TX_RESOLVE_VISIBILITY, &resolution, &reason),
					 expected);
		native_calls = test_native_status_calls;
		UT_ASSERT_EQ(test_resolve_memo_origin(CLUSTER_TX_RESOLVE_VISIBILITY, &resolution, &reason),
					 expected);
		UT_ASSERT(test_native_status_calls > native_calls);
		cluster_runtime_visibility_exact_counters_snapshot(&counters);
		UT_ASSERT_EQ(counters.local_memo_hits, 0);
		UT_ASSERT_EQ(counters.local_memo_publishes, 0);
	}
	/* A recycled-abort proof is scoped to its native retention window,
	 * rather than an exact current TT carrier, so it must not enter memo. */
	reset_recycled_abort_fixture(&admission);
	for (variant = 0; variant < 2; variant++)
		UT_ASSERT_EQ(cluster_runtime_visibility_resolve_exact_origin_admitted(
						 &test_origin_locator, CLUSTER_TX_RESOLVE_VISIBILITY, &admission,
						 &resolution, &reason),
					 CLUSTER_TX_ABORTED);
	UT_ASSERT_EQ(test_native_status_calls, 4);
	cluster_runtime_visibility_exact_counters_snapshot(&counters);
	UT_ASSERT_EQ(counters.local_memo_hits, 0);
	UT_ASSERT_EQ(counters.local_memo_publishes, 0);
}

UT_TEST(test_exact_origin_memo_isolates_epoch_generation_and_slot_identity)
{
	int variant;

	for (variant = 0; variant < 7; variant++) {
		ClusterTxResolution resolution;
		ClusterTxResolveReason reason;
		ClusterRuntimeVisibilityCounters counters;

		reset_exact_origin_fixture();
		test_origin_locator.tt_wrap = TT_WRAP_INVALID;
		UT_ASSERT_EQ(test_resolve_memo_origin(CLUSTER_TX_RESOLVE_VISIBILITY, &resolution, &reason),
					 CLUSTER_TX_COMMITTED);
		switch (variant) {
		case 0:
			test_formation_epoch++;
			break;
		case 1:
			test_candidate_tt_generation++;
			break;
		case 2:
			test_tt_slot.xid++;
			break;
		case 3:
			test_tt_slot.wrap++;
			break;
		case 4:
			test_candidate_copy_physical_slot = false;
			break;
		case 5:
			test_tt_slot.status = TT_SLOT_ABORTED;
			test_tt_slot.commit_scn = InvalidScn;
			break;
		case 6:
			test_origin_record.tt_slot_segment_id = TEST_RECORD_SEGMENT;
			break;
		}
		UT_ASSERT_EQ(test_resolve_memo_origin(CLUSTER_TX_RESOLVE_VISIBILITY, &resolution, &reason),
					 variant < 2 || variant == 6 ? CLUSTER_TX_COMMITTED : CLUSTER_TX_UNKNOWN);
		cluster_runtime_visibility_exact_counters_snapshot(&counters);
		UT_ASSERT_EQ(counters.local_memo_hits, 0);
		UT_ASSERT_EQ(test_native_status_calls, variant >= 2 && variant <= 4 ? 1 : 2);
		UT_ASSERT_EQ(test_candidate_held_count, 0);
	}
}

UT_TEST(test_exact_origin_memo_rechecks_each_data_carrier)
{
	ClusterTxResolution resolution;
	ClusterTxResolveReason reason;
	ClusterRuntimeVisibilityCounters counters;

	reset_exact_origin_fixture();
	test_origin_locator.tt_wrap = TT_WRAP_INVALID;
	UT_ASSERT_EQ(test_resolve_memo_origin(CLUSTER_TX_RESOLVE_VISIBILITY, &resolution, &reason),
				 CLUSTER_TX_COMMITTED);
	test_origin_locator.uba = uba_encode(TEST_RECORD_SEGMENT, 7, TEST_TT_OFFSET, 500);
	test_origin_locator.itl_slot_index++;
	UT_ASSERT_EQ(test_resolve_memo_origin(CLUSTER_TX_RESOLVE_VISIBILITY, &resolution, &reason),
				 CLUSTER_TX_COMMITTED);
	UT_ASSERT_EQ(memcmp(&resolution.locator_echo.uba, &test_origin_locator.uba, sizeof(UBA)), 0);
	UT_ASSERT_EQ(resolution.locator_echo.itl_slot_index, test_origin_locator.itl_slot_index);
	UT_ASSERT_EQ(test_native_status_calls, 1);
	UT_ASSERT_EQ(test_candidate_extract_calls, 4);
	cluster_runtime_visibility_exact_counters_snapshot(&counters);
	UT_ASSERT_EQ(counters.local_memo_hits, 1);
}

UT_TEST(test_exact_origin_memo_publishes_after_final_data_recheck)
{
	int variant;

	for (variant = 0; variant < 3; variant++) {
		ClusterRuntimeVisibilityOriginPlan plan;
		ClusterTxResolution resolution;
		ClusterTxResolveReason reason;
		ClusterRuntimeVisibilityCounters counters;
		ClusterSemanticAdmissionToken admission = { 0 };
		ClusterUndoBlock0CurrentGuard data_guard = { 0 }, tt_guard = { 0 };
		ClusterUndoBlock0ResolvedRoot data_root = { 0 }, tt_root = { 0 };

		reset_exact_origin_fixture();
		test_origin_locator.tt_wrap = TT_WRAP_INVALID;
		admission.feature_bit = CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1;
		admission.record_generation = 5;
		admission.formation_epoch = test_formation_epoch;
		admission.side = CLUSTER_SEMANTIC_TARGET_SIDE;
		admission.entered = true;
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
		cluster_runtime_visibility_exact_counters_snapshot(&counters);
		UT_ASSERT_EQ(counters.local_memo_publishes, 0);
		if (variant == 1)
			test_candidate_data_generation++;
		if (variant == 2)
			test_candidate_mutate_record_on_recheck = true;
		UT_ASSERT_EQ(cluster_runtime_visibility_origin_plan_recheck_data_held(
						 &plan, CLUSTER_TX_RESOLVE_VISIBILITY, &admission, &data_guard, &data_root,
						 &resolution, &reason),
					 variant == 0 ? CLUSTER_TX_COMMITTED : CLUSTER_TX_UNKNOWN);
		cluster_runtime_visibility_exact_counters_snapshot(&counters);
		UT_ASSERT_EQ(counters.local_memo_publishes, variant == 0 ? 1 : 0);
	}
}

UT_TEST(test_exact_origin_memo_hit_preserves_final_data_and_generation_fences)
{
	int variant;

	for (variant = 0; variant < 2; variant++) {
		ClusterTxResolution resolution;
		ClusterTxResolveReason reason;
		ClusterRuntimeVisibilityCounters counters;

		reset_exact_origin_fixture();
		test_origin_locator.tt_wrap = TT_WRAP_INVALID;
		UT_ASSERT_EQ(test_resolve_memo_origin(CLUSTER_TX_RESOLVE_VISIBILITY, &resolution, &reason),
					 CLUSTER_TX_COMMITTED);
		test_candidate_extract_calls = 0;
		if (variant == 0)
			test_candidate_mutate_record_on_recheck = true;
		else
			test_candidate_copy_generation_drift = true;
		UT_ASSERT_EQ(test_resolve_memo_origin(CLUSTER_TX_RESOLVE_VISIBILITY, &resolution, &reason),
					 CLUSTER_TX_UNKNOWN);
		UT_ASSERT_EQ(resolution.outcome, CLUSTER_TX_UNKNOWN);
		/* The DATA copy already advances the TT generation in variant 1:
		 * it must miss the memo and then fail the TT generation recheck. */
		UT_ASSERT_EQ(test_native_status_calls, variant == 0 ? 1 : 2);
		UT_ASSERT_EQ(test_candidate_held_count, 0);
		cluster_runtime_visibility_exact_counters_snapshot(&counters);
		UT_ASSERT_EQ(counters.local_memo_hits, variant == 0 ? 1 : 0);
		UT_ASSERT_EQ(counters.local_memo_publishes, 1);
	}
}

UT_TEST(test_origin_tx_plan_preserves_strict_zero_and_final_generation_recheck)
{
	int variant;

	for (variant = 0; variant < 4; variant++) {
		ClusterRuntimeVisibilityOriginPlan plan;
		ClusterTxResolution resolution;
		ClusterTxResolveReason reason;
		ClusterSemanticAdmissionToken admission = { 0 };
		ClusterUndoBlock0CurrentGuard data_guard = { 0 }, tt_guard = { 0 };
		ClusterUndoBlock0ResolvedRoot data_root = { 0 }, tt_root = { 0 };
		ClusterUndoBlock0Generation expected = { true, variant == 0 ? 0 : 3 };

		reset_exact_origin_fixture();
		test_candidate_data_generation = 3;
		test_origin_locator.tt_wrap = TT_WRAP_INVALID;
		admission.feature_bit = CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1;
		admission.record_generation = 5;
		admission.formation_epoch = test_formation_epoch;
		admission.side = CLUSTER_SEMANTIC_TARGET_SIDE;
		admission.entered = true;
		data_root.intent = tt_root.intent = CLUSTER_UNDO_PATH_RUNTIME_SHARED;
		data_root.root_id = 91;
		tt_root.root_id = 92;
		data_root.root_generation = tt_root.root_generation = 7;
		UT_ASSERT_EQ(cluster_runtime_visibility_origin_plan_freeze_data_held(
						 &test_origin_locator, CLUSTER_TX_RESOLVE_VISIBILITY, &admission, &expected,
						 &data_guard, &data_root, &plan, &resolution, &reason),
					 variant == 0 ? CLUSTER_RUNTIME_VISIBILITY_ORIGIN_FAILED
								  : CLUSTER_RUNTIME_VISIBILITY_ORIGIN_NEEDS_CANONICAL);
		if (variant == 0) {
			UT_ASSERT_EQ(test_candidate_data_copy_calls, 0);
			UT_ASSERT_EQ(test_tt_exact_calls, 0);
			continue;
		}
		UT_ASSERT(cluster_runtime_visibility_origin_plan_sample_canonical_held(
			&plan, CLUSTER_TX_RESOLVE_VISIBILITY, &admission, &tt_guard, &tt_root, &reason));
		if (variant == 2)
			test_candidate_data_generation++;
		if (variant == 3)
			test_candidate_mutate_record_on_recheck = true;
		UT_ASSERT_EQ(cluster_runtime_visibility_origin_plan_recheck_data_held(
						 &plan, CLUSTER_TX_RESOLVE_VISIBILITY, &admission, &data_guard, &data_root,
						 &resolution, &reason),
					 variant == 1 ? CLUSTER_TX_COMMITTED : CLUSTER_TX_UNKNOWN);
		if (variant == 1) {
			UT_ASSERT(resolution.local_canonical_valid);
			UT_ASSERT_EQ(resolution.local_canonical.segment_id, TEST_TT_SEGMENT);
			UT_ASSERT(resolution.local_canonical.segment_id != TEST_RECORD_SEGMENT);
			UT_ASSERT_EQ(resolution.local_canonical.xid, resolution.locator_echo.xid);
			UT_ASSERT_EQ(resolution.local_canonical.wrap, resolution.locator_echo.tt_wrap);
			UT_ASSERT_EQ(resolution.local_canonical_generation, test_candidate_tt_generation);
		}
	}
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

/* Author: PGRAC. The real local consumer must not discard an already-proven
 * exact terminal merely because a later physical-alias pair would refuse. */
UT_TEST(test_local_retained_read_preserves_same_stamp_origin_exact)
{
	ClusterUndoVerdictResult result;

	reset_exact_origin_fixture();
	test_local_origin_exact = test_commit_scn;
	result = cluster_runtime_visibility_test_local_retained_read(
		TEST_ORIGIN_XID, TEST_RECORD_SEGMENT, TEST_TT_OFFSET + 1, test_commit_scn,
		test_commit_scn + 1);
	UT_ASSERT_EQ(result.kind, CLUSTER_UNDO_VERDICT_COMMITTED_EXACT);
	UT_ASSERT_EQ(result.commit_scn, test_commit_scn);
	UT_ASSERT_EQ(result.wrap, TEST_ORIGIN_WRAP);
	UT_ASSERT_EQ(test_local_origin_calls, 1);
	UT_ASSERT_EQ(test_local_freshref_pair_calls, 0);
}

UT_TEST(test_local_retained_exact_does_not_replace_the_snapshot_or_census_role)
{
	ClusterUndoVerdictResult result;

	reset_exact_origin_fixture();
	test_local_origin_exact = test_commit_scn;
	result = cluster_runtime_visibility_test_local_retained_read(
		TEST_ORIGIN_XID, TEST_RECORD_SEGMENT, TEST_TT_OFFSET + 1, test_commit_scn,
		test_commit_scn - 1);
	UT_ASSERT_EQ(result.kind, CLUSTER_UNDO_VERDICT_COMMITTED_EXACT);
	UT_ASSERT_EQ(result.commit_scn, test_commit_scn);
	UT_ASSERT_EQ(test_local_freshref_pair_calls, 0);

	reset_exact_origin_fixture();
	test_local_origin_exact = test_commit_scn;
	result = cluster_runtime_visibility_test_local_retained_read(
		TEST_ORIGIN_XID, TEST_RECORD_SEGMENT, TEST_TT_OFFSET + 1, test_commit_scn, InvalidScn);
	UT_ASSERT_EQ(result.kind, CLUSTER_UNDO_VERDICT_UNKNOWN_FAIL_CLOSED);
	UT_ASSERT_EQ(test_local_origin_calls, 0);
	UT_ASSERT_EQ(test_local_freshref_pair_calls, 1);
}

UT_TEST(test_local_retained_exact_mismatch_and_epoch_drift_stay_unproven)
{
	ClusterUndoVerdictResult result;

	reset_exact_origin_fixture();
	test_local_origin_exact = test_commit_scn + 1;
	result = cluster_runtime_visibility_test_local_retained_read(
		TEST_ORIGIN_XID, TEST_RECORD_SEGMENT, TEST_TT_OFFSET + 1, test_commit_scn,
		test_commit_scn + 2);
	UT_ASSERT_EQ(result.kind, CLUSTER_UNDO_VERDICT_UNKNOWN_FAIL_CLOSED);
	UT_ASSERT_EQ(test_local_freshref_pair_calls, 1);

	reset_exact_origin_fixture();
	test_local_origin_exact = test_commit_scn;
	test_local_origin_epoch_drift = true;
	result = cluster_runtime_visibility_test_local_retained_read(
		TEST_ORIGIN_XID, TEST_RECORD_SEGMENT, TEST_TT_OFFSET + 1, test_commit_scn,
		test_commit_scn + 1);
	UT_ASSERT_EQ(result.kind, CLUSTER_UNDO_VERDICT_UNKNOWN_FAIL_CLOSED);
	UT_ASSERT_EQ(test_local_freshref_pair_calls, 0);
}

UT_TEST(test_local_retained_read_bound_precedes_exact_pair_without_promoting_it)
{
	ClusterUndoVerdictResult result;
	reset_exact_origin_fixture();
	test_local_origin_bound = 100;
	result = cluster_runtime_visibility_test_local_retained_read(
		TEST_ORIGIN_XID, TEST_RECORD_SEGMENT, TEST_TT_OFFSET + 1, test_commit_scn, 120);
	UT_ASSERT_EQ(result.kind, CLUSTER_UNDO_VERDICT_COMMITTED_BOUND);
	UT_ASSERT_EQ(result.commit_scn, (SCN)100);
	UT_ASSERT_EQ(test_local_origin_calls, 1);
	UT_ASSERT_EQ(test_local_freshref_pair_calls, 0);

	reset_exact_origin_fixture();
	test_local_origin_bound = 100;
	result = cluster_runtime_visibility_test_local_retained_read(
		TEST_ORIGIN_XID, TEST_RECORD_SEGMENT, TEST_TT_OFFSET + 1, test_commit_scn, 99);
	UT_ASSERT_EQ(result.kind, CLUSTER_UNDO_VERDICT_UNKNOWN_FAIL_CLOSED);
	UT_ASSERT_EQ(test_local_freshref_pair_calls, 1);

	reset_exact_origin_fixture();
	test_local_origin_bound = 100;
	test_local_freshref_pair_exact = true;
	result = cluster_runtime_visibility_test_local_retained_read(
		TEST_ORIGIN_XID, TEST_RECORD_SEGMENT, TEST_TT_OFFSET + 1, test_commit_scn, InvalidScn);
	UT_ASSERT_EQ(result.kind, CLUSTER_UNDO_VERDICT_COMMITTED_EXACT);
	UT_ASSERT_EQ(result.commit_scn, test_commit_scn);
	UT_ASSERT_EQ(test_local_origin_calls, 0); /* terminal census stays exact-only */

	reset_exact_origin_fixture();
	test_local_origin_bound = 100;
	test_local_origin_epoch_drift = true;
	result = cluster_runtime_visibility_test_local_retained_read(
		TEST_ORIGIN_XID, TEST_RECORD_SEGMENT, TEST_TT_OFFSET + 1, test_commit_scn, 120);
	UT_ASSERT_EQ(result.kind, CLUSTER_UNDO_VERDICT_UNKNOWN_FAIL_CLOSED);
	UT_ASSERT_EQ(test_local_freshref_pair_calls, 0);
}

UT_TEST(test_self_fresh_role_does_not_inherit_ordinary_abort_permission)
{
	ClusterUndoVerdictResult result;
	extern ClusterUndoVerdictResult cluster_runtime_visibility_test_self_verdict_role(
		TransactionId xid, uint32 segment, uint32 slot, SCN read_scn, bool authoritative,
		SCN retained_scn);

	reset_exact_origin_fixture();
	test_local_ordinary_abort = true;
	result = cluster_runtime_visibility_test_self_verdict_role(TEST_ORIGIN_XID, TEST_RECORD_SEGMENT,
															   0, 120, false, InvalidScn);
	UT_ASSERT_EQ(result.kind, CLUSTER_UNDO_VERDICT_ABORTED);
	UT_ASSERT(test_local_last_ordinary_single);
	result = cluster_runtime_visibility_test_self_verdict_role(
		TEST_ORIGIN_XID, TEST_RECORD_SEGMENT, TEST_TT_OFFSET + 1, 120, true, InvalidScn);
	UT_ASSERT_EQ(result.kind, CLUSTER_UNDO_VERDICT_UNKNOWN_FAIL_CLOSED);
	UT_ASSERT(!test_local_last_ordinary_single);
	result = cluster_runtime_visibility_test_self_verdict_role(
		TEST_ORIGIN_XID, TEST_RECORD_SEGMENT, TEST_TT_OFFSET + 1, 120, true, test_commit_scn);
	UT_ASSERT_EQ(result.kind, CLUSTER_UNDO_VERDICT_UNKNOWN_FAIL_CLOSED);
	UT_ASSERT(!test_local_last_ordinary_single);
}


static void
init_creation_record(uint16 type, bool same_segment)
{
	UndoInsertPayload insert = {0};
	UndoUpdatePayload update = {0};
	UndoDeletePayload deleted = {0};

	reset_exact_origin_fixture();
	test_origin_locator.tt_wrap = TT_WRAP_INVALID;
	test_origin_record.record_type = type;
	test_origin_record.flags = UNDO_REC_FLAG_FIRST_IN_TX;
	test_origin_record.target_locator = (RelFileLocator){1663, 42, 77};
	test_origin_record.target_fork = MAIN_FORKNUM;
	test_origin_record.target_block = 23;
	test_origin_record.target_offset = 3;
	if (same_segment)
		test_origin_record.tt_slot_segment_id = TEST_RECORD_SEGMENT;
	if (type == UNDO_RECORD_INSERT) {
		insert.inserted_tuple_len = 60;
		memcpy(test_origin_payload, &insert, sizeof(insert));
		test_origin_payload_bytes = sizeof(insert);
	} else if (type == UNDO_RECORD_UPDATE) {
		update.new_block = 24;
		update.new_offset = 4;
		update.old_tuple_length = 1;
		update.old_tuple_offset = sizeof(update);
		memcpy(test_origin_payload, &update, sizeof(update));
		test_origin_payload_bytes = sizeof(update) + 1;
	} else {
		deleted.full_tuple_length = 1;
		deleted.full_tuple_offset = sizeof(deleted);
		memcpy(test_origin_payload, &deleted, sizeof(deleted));
		test_origin_payload_bytes = sizeof(deleted) + 1;
	}
	test_origin_record.payload_length = test_origin_payload_bytes;
}

/* Actual held-plan/data-recheck bodies; locks, storage and C1b remain doubles. */
static void
resolve_creation_record(bool same_segment, int drift, ClusterTxResolution *resolution)
{
	ClusterRuntimeVisibilityOriginPlan plan;
	ClusterTxResolveReason reason;
	ClusterSemanticAdmissionToken admission = {0};
	ClusterUndoBlock0CurrentGuard data_guard = {0}, tt_guard = {0};
	ClusterUndoBlock0ResolvedRoot data_root = {0}, tt_root = {0};
	PGAlignedBlock page;
	const ClusterTxOutcome expected = test_native_status == TRANSACTION_STATUS_ABORTED
		? CLUSTER_TX_ABORTED : CLUSTER_TX_COMMITTED;

	admission.feature_bit = CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1;
	admission.record_generation = 5;
	admission.formation_epoch = test_formation_epoch;
	admission.side = CLUSTER_SEMANTIC_TARGET_SIDE;
	admission.entered = true;
	data_root.intent = tt_root.intent = CLUSTER_UNDO_PATH_RUNTIME_SHARED;
	data_root.root_id = 91;
	tt_root.root_id = 92;
	data_root.root_generation = tt_root.root_generation = 7;
	UT_ASSERT_EQ(cluster_runtime_visibility_origin_plan_freeze_data_held(
		&test_origin_locator, CLUSTER_TX_RESOLVE_VISIBILITY, &admission, NULL,
		&data_guard, &data_root, &plan, resolution, &reason),
		same_segment ? CLUSTER_RUNTIME_VISIBILITY_ORIGIN_COMPLETE
					 : CLUSTER_RUNTIME_VISIBILITY_ORIGIN_NEEDS_CANONICAL);
	if (!same_segment) {
		UT_ASSERT(!resolution->local_creation_target.valid);
		UT_ASSERT(cluster_runtime_visibility_origin_plan_sample_canonical_held(
			&plan, CLUSTER_TX_RESOLVE_VISIBILITY, &admission, &tt_guard, &tt_root, &reason));
	}
	switch (drift) {
	case 1: test_candidate_mutate_record_on_recheck = true; break;
	case 2: test_candidate_data_generation++; break;
	case 3: data_root.root_generation++; break;
	case 4: admission.formation_epoch++; break;
	default: break;
	}
	UT_ASSERT_EQ(cluster_runtime_visibility_origin_plan_copy_data_held(
		&plan, CLUSTER_TX_RESOLVE_VISIBILITY, &admission, &data_guard,
		&data_root, resolution, page.data, &reason),
		drift == 0 ? expected : CLUSTER_TX_UNKNOWN);
}

static void
init_aborted_modification_record(uint16 type, bool same_segment)
{
	const uint16 old_length = SizeofHeapTupleHeader + 8;
	uint16 old_offset;
	uint8 old_hoff = SizeofHeapTupleHeader;

	init_creation_record(type, same_segment);
	test_native_status = TRANSACTION_STATUS_ABORTED;
	test_tt_slot.status = test_durable_locator_status = TT_SLOT_ABORTED;
	test_tt_slot.commit_scn = InvalidScn;
	memset(test_origin_payload, 0, sizeof(test_origin_payload));
	if (type == UNDO_RECORD_UPDATE) {
		UndoUpdatePayload update = {0};
		update.new_block = 24; update.new_offset = 4;
		update.old_tuple_length = old_length;
		update.old_tuple_offset = old_offset = sizeof(update);
		memcpy(test_origin_payload, &update, sizeof(update));
	} else {
		UndoDeletePayload deleted = {0};
		deleted.full_tuple_length = old_length;
		deleted.full_tuple_offset = old_offset = sizeof(deleted);
		memcpy(test_origin_payload, &deleted, sizeof(deleted));
	}
	UT_ASSERT(old_offset + old_length <= sizeof(test_origin_payload));
	memcpy(test_origin_payload + old_offset + offsetof(HeapTupleHeaderData, t_hoff),
		&old_hoff, sizeof(old_hoff));
	test_origin_payload_bytes = old_offset + old_length;
	test_origin_record.payload_length = test_origin_payload_bytes;
}

UT_TEST(test_aborted_delete_target_comes_from_final_exact_record)
{
	for (int same = 0; same < 2; same++) {
		ClusterTxResolution resolution;
		init_aborted_modification_record(UNDO_RECORD_DELETE, same);
		if (same) test_origin_record.target_locator.dbOid = InvalidOid;
		resolve_creation_record(same, 0, &resolution);
		UT_ASSERT_EQ(resolution.outcome, CLUSTER_TX_ABORTED);
		UT_ASSERT(resolution.local_aborted_xmax_target.valid);
		UT_ASSERT(RelFileLocatorEquals(resolution.local_aborted_xmax_target.locator,
			test_origin_record.target_locator));
		UT_ASSERT_EQ(resolution.local_aborted_xmax_target.fork, MAIN_FORKNUM);
		UT_ASSERT_EQ(resolution.local_aborted_xmax_target.block, 23);
		UT_ASSERT_EQ(resolution.local_aborted_xmax_target.offset, 3);
		UT_ASSERT_EQ(resolution.local_aborted_xmax_target.tuple_length, SizeofHeapTupleHeader + 8);
		UT_ASSERT(!resolution.local_creation_target.valid);
	}
}

UT_TEST(test_aborted_update_target_names_old_tuple_not_replacement)
{
	for (int same = 0; same < 2; same++) {
		ClusterTxResolution resolution;
		init_aborted_modification_record(UNDO_RECORD_UPDATE, same);
		resolve_creation_record(same, 0, &resolution);
		UT_ASSERT(resolution.local_aborted_xmax_target.valid);
		UT_ASSERT_EQ(resolution.local_aborted_xmax_target.block, 23);
		UT_ASSERT_EQ(resolution.local_aborted_xmax_target.offset, 3);
		UT_ASSERT_EQ(resolution.local_aborted_xmax_target.tuple_length, SizeofHeapTupleHeader + 8);
		UT_ASSERT(!resolution.local_creation_target.valid);
	}
}

UT_TEST(test_modification_projection_never_upgrades_committed_or_insert_record)
{
	for (unsigned variant = 0; variant < 3; variant++) {
		ClusterTxResolution resolution;
		if (variant == 2) {
			init_creation_record(UNDO_RECORD_INSERT, false);
			test_native_status = TRANSACTION_STATUS_ABORTED;
			test_tt_slot.status = TT_SLOT_ABORTED; test_tt_slot.commit_scn = InvalidScn;
		} else {
			init_aborted_modification_record(variant ? UNDO_RECORD_UPDATE : UNDO_RECORD_DELETE, false);
			test_native_status = TRANSACTION_STATUS_COMMITTED;
			test_tt_slot.status = test_durable_locator_status = TT_SLOT_COMMITTED;
			test_tt_slot.commit_scn = test_commit_scn;
		}
		resolve_creation_record(false, 0, &resolution);
		UT_ASSERT(!resolution.local_aborted_xmax_target.valid);
		UT_ASSERT_EQ(resolution.local_creation_target.valid, variant == 1);
	}
}

UT_TEST(test_aborted_modification_projection_rejects_malformed_record_image)
{
	for (unsigned update = 0; update < 2; update++) {
		for (unsigned variant = 0; variant < 10; variant++) {
			ClusterTxResolution resolution;
			uint16 old_offset;
			init_aborted_modification_record(update ? UNDO_RECORD_UPDATE : UNDO_RECORD_DELETE, false);
			old_offset = update ? sizeof(UndoUpdatePayload) : sizeof(UndoDeletePayload);
			switch (variant) {
			case 0: test_origin_record.target_locator.relNumber = InvalidRelFileNumber; break;
			case 1: test_origin_record.target_fork = FSM_FORKNUM; break;
			case 2: test_origin_record.target_block = InvalidBlockNumber; break;
			case 3: test_origin_record.target_offset = MaxHeapTuplesPerPage + 1; break;
			case 4: test_origin_record.payload_length++; break;
			case 5:
				if (update) ((UndoUpdatePayload *)test_origin_payload)->old_tuple_length = 1;
				else ((UndoDeletePayload *)test_origin_payload)->full_tuple_length = 1;
				break;
			case 6:
				if (update) ((UndoUpdatePayload *)test_origin_payload)->old_tuple_offset = BLCKSZ;
				else ((UndoDeletePayload *)test_origin_payload)->full_tuple_offset = BLCKSZ;
				break;
			case 7: test_origin_payload[old_offset + offsetof(HeapTupleHeaderData, t_hoff)] = 0; break;
			case 8: test_origin_payload[old_offset + offsetof(HeapTupleHeaderData, t_hoff)] = UINT8_MAX; break;
			case 9: test_origin_record.flags = 0; break;
			}
			resolve_creation_record(false, 0, &resolution);
			UT_ASSERT_EQ(resolution.outcome, CLUSTER_TX_ABORTED);
			UT_ASSERT(!resolution.local_aborted_xmax_target.valid);
			UT_ASSERT(!resolution.local_creation_target.valid);
		}
	}
}

UT_TEST(test_aborted_modification_target_not_exported_after_final_data_drift)
{
	for (int drift = 1; drift <= 4; drift++) {
		ClusterTxResolution resolution;
		init_aborted_modification_record(UNDO_RECORD_UPDATE, false);
		resolve_creation_record(false, drift, &resolution);
		UT_ASSERT(!resolution.local_aborted_xmax_target.valid);
	}
}

UT_TEST(test_later_creation_result_clears_aborted_modification_projection)
{
	ClusterTxResolution resolution;
	init_aborted_modification_record(UNDO_RECORD_DELETE, false);
	resolve_creation_record(false, 0, &resolution);
	UT_ASSERT(resolution.local_aborted_xmax_target.valid);
	init_creation_record(UNDO_RECORD_INSERT, false);
	resolve_creation_record(false, 0, &resolution);
	UT_ASSERT(!resolution.local_aborted_xmax_target.valid);
	UT_ASSERT(resolution.local_creation_target.valid);
}

UT_TEST(test_insert_creation_target_comes_from_final_exact_record)
{
	for (int same = 0; same < 2; same++) {
		ClusterTxResolution resolution;
		init_creation_record(UNDO_RECORD_INSERT, same);
		/* Global relations legitimately have database OID zero. */
		if (same)
			test_origin_record.target_locator.dbOid = InvalidOid;
		resolve_creation_record(same, 0, &resolution);
		UT_ASSERT(resolution.local_creation_target.valid);
		UT_ASSERT(RelFileLocatorEquals(resolution.local_creation_target.locator,
									 test_origin_record.target_locator));
		UT_ASSERT_EQ(resolution.local_creation_target.fork, MAIN_FORKNUM);
		UT_ASSERT_EQ(resolution.local_creation_target.block, 23);
		UT_ASSERT_EQ(resolution.local_creation_target.offset, 3);
		UT_ASSERT_EQ(resolution.local_creation_target.tuple_length, 60);
	}
}

UT_TEST(test_update_creation_target_names_replacement_not_old_tuple)
{
	for (int same = 0; same < 2; same++) {
		ClusterTxResolution resolution;
		init_creation_record(UNDO_RECORD_UPDATE, same);
		resolve_creation_record(same, 0, &resolution);
		UT_ASSERT(resolution.local_creation_target.valid);
		UT_ASSERT_EQ(resolution.local_creation_target.block, 24);
		UT_ASSERT_EQ(resolution.local_creation_target.offset, 4);
		UT_ASSERT_EQ(resolution.local_creation_target.tuple_length, 0);
	}
}

UT_TEST(test_noncreation_or_malformed_payload_keeps_verdict_without_target)
{
	for (int variant = 0; variant < 11; variant++) {
		ClusterTxResolution resolution;
		init_creation_record(variant == 0 ? UNDO_RECORD_DELETE : UNDO_RECORD_UPDATE, false);
		switch (variant) {
		case 1: {
			UndoUpdatePayload update;
			memcpy(&update, test_origin_payload, sizeof(update));
			update.new_block = InvalidBlockNumber;
			update.new_offset = InvalidOffsetNumber;
			memcpy(test_origin_payload, &update, sizeof(update));
			break;
		}
		case 2: test_origin_record.payload_length++; break;
		case 3: test_origin_record.target_locator.relNumber = InvalidRelFileNumber; break;
		case 4: test_origin_record.target_fork = FSM_FORKNUM; break;
		case 5: test_origin_record.target_offset = MaxHeapTuplesPerPage + 1; break;
		case 6: test_origin_record.target_block = InvalidBlockNumber; break;
		case 7: {
			UndoUpdatePayload update;
			memcpy(&update, test_origin_payload, sizeof(update));
			update.new_offset = MaxHeapTuplesPerPage + 1;
			memcpy(test_origin_payload, &update, sizeof(update));
			break;
		}
		case 8: test_origin_record.flags = 0; break;
		case 9:
			init_creation_record(UNDO_RECORD_INSERT, false);
			((UndoInsertPayload *)test_origin_payload)->inserted_tuple_len = 1;
			break;
		case 10:
			init_creation_record(UNDO_RECORD_INSERT, false);
			((UndoInsertPayload *)test_origin_payload)->inserted_tuple_len = BLCKSZ;
			break;
		default: break;
		}
		resolve_creation_record(false, 0, &resolution);
		UT_ASSERT_EQ(resolution.outcome, CLUSTER_TX_COMMITTED);
		UT_ASSERT(!resolution.local_creation_target.valid);
	}
}

UT_TEST(test_creation_target_is_not_exported_after_final_data_fence_drift)
{
	for (int drift = 1; drift <= 4; drift++) {
		ClusterTxResolution resolution;
		init_creation_record(UNDO_RECORD_INSERT, false);
		resolve_creation_record(false, drift, &resolution);
		UT_ASSERT(!resolution.local_creation_target.valid);
	}
}

static void
setup_original_full_xid(bool same_segment)
{
	init_creation_record(UNDO_RECORD_INSERT, same_segment);
	test_original_available = true;
	/* Deliberately outside the local nextXid epoch: this is retained origin
	 * provenance, never the current raw-XID interpretation window. */
	test_original_full_xid = FullTransactionIdFromEpochAndXid(9, TEST_ORIGIN_XID);
	test_original_key = (ClusterCtrcTxnKeyV1){
		.format_version = CLUSTER_CTRC_FORMAT_VERSION,
		.owner_instance = 1,
		.origin_node_id = 0,
		.segment_id = same_segment ? TEST_RECORD_SEGMENT : TEST_TT_SEGMENT,
		.segment_generation = same_segment ? 17 : 23,
		.slot_offset = TEST_TT_OFFSET,
		.slot_wrap = TEST_ORIGIN_WRAP,
		.xid = TEST_ORIGIN_XID,
		.cluster_epoch = 42,
		.system_identifier = UINT64_C(0x1122334455667788),
		.origin_boot_incarnation = 9001,
		.formation_epoch = 42,
		.admission_record_generation = 5,
		.root_descriptor_incarnation = 7,
		.root_id = same_segment ? 91 : 92,
		.root_generation = 7
	};
}

static ClusterTxOutcome
resolve_original_full_xid(ClusterTxResolveMode mode, ClusterTxResolution *resolution)
{
	ClusterTxResolveReason reason;
	ClusterSemanticAdmissionToken admission = {
		.feature_bit = CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1,
		.record_generation = 5, .formation_epoch = 42,
		.side = CLUSTER_SEMANTIC_TARGET_SIDE, .entered = true
	};

	return cluster_runtime_visibility_resolve_exact_origin_admitted(
		&test_origin_locator, mode, &admission, resolution, &reason);
}

UT_TEST(test_original_full_xid_reaches_same_and_cross_segment_census_result)
{
	for (int same = 0; same < 2; same++) {
		ClusterTxResolution resolution;

		setup_original_full_xid(same);
		UT_ASSERT_EQ(resolve_original_full_xid(CLUSTER_TX_RESOLVE_TERMINAL_CENSUS,
			&resolution), CLUSTER_TX_COMMITTED);
		UT_ASSERT(FullTransactionIdEquals(resolution.local_original_full_xid,
			test_original_full_xid));
		UT_ASSERT(resolution.local_creation_target.valid);
		UT_ASSERT_EQ(test_original_calls, 1);
		UT_ASSERT_EQ(g_retained_origin_reads, 0);
		UT_ASSERT_EQ(test_candidate_max_held_count, 1);
		UT_ASSERT_EQ(test_candidate_held_count, 0);
		UT_ASSERT_EQ(test_candidate_acquire_calls, same ? 1 : 3);
	}
}

UT_TEST(test_original_full_xid_missing_or_key_alias_keeps_verdict_without_identity)
{
	for (int variant = 0; variant < 15; variant++) {
		ClusterTxResolution resolution;

		setup_original_full_xid(false);
		switch (variant) {
		case 0: test_original_available = false; break;
		case 1: test_original_full_xid = InvalidFullTransactionId; break;
		case 2: test_original_full_xid = FullTransactionIdFromU64(2); break;
		case 3: test_original_full_xid = FullTransactionIdFromEpochAndXid(9, TEST_ORIGIN_XID+1); break;
		case 4: test_original_key.segment_id++; break;
		case 5: test_original_key.segment_generation++; break;
		case 6: test_original_key.slot_offset++; break;
		case 7: test_original_key.slot_wrap++; break;
		case 8: test_original_key.root_id++; break;
		case 9: test_original_key.root_generation++; break;
		case 10: test_original_key.root_descriptor_incarnation++; break;
		case 11: test_original_key.origin_boot_incarnation++; break;
		case 12: test_original_key.system_identifier++; break;
		case 13: test_original_key.formation_epoch++; break;
		case 14: test_original_key.admission_record_generation++; break;
		}
		UT_ASSERT_EQ(resolve_original_full_xid(CLUSTER_TX_RESOLVE_TERMINAL_CENSUS,
			&resolution), CLUSTER_TX_COMMITTED);
		UT_ASSERT(!FullTransactionIdIsValid(resolution.local_original_full_xid));
		UT_ASSERT_EQ(test_original_calls, 1);
		UT_ASSERT_EQ(test_candidate_held_count, 0);
	}
}

UT_TEST(test_original_full_xid_not_queried_for_other_roles_or_abort)
{
	for (int variant = 0; variant < 2; variant++) {
		ClusterTxResolution resolution;
		ClusterTxResolveMode mode = variant == 0 ? CLUSTER_TX_RESOLVE_VISIBILITY
			: CLUSTER_TX_RESOLVE_TERMINAL_CENSUS;

		setup_original_full_xid(false);
		if (variant == 1) {
			test_tt_slot.status = TT_SLOT_ABORTED;
			test_tt_slot.commit_scn = InvalidScn;
			test_native_status = TRANSACTION_STATUS_ABORTED;
		}
		UT_ASSERT_EQ(resolve_original_full_xid(mode, &resolution),
			variant == 0 ? CLUSTER_TX_COMMITTED : CLUSTER_TX_ABORTED);
		UT_ASSERT(!FullTransactionIdIsValid(resolution.local_original_full_xid));
		UT_ASSERT_EQ(test_original_calls, 0);
		UT_ASSERT_EQ(g_retained_origin_reads, 0);
	}
}

UT_TEST(test_original_full_xid_does_not_escape_final_fence_drift)
{
	for (int same = 0; same < 2; same++) {
		for (int drift = 1; drift <= 4; drift++) {
			ClusterTxResolution resolution;
			ClusterTxOutcome outcome;

			setup_original_full_xid(same);
			/* The original final root check will observe this drift on either
			 * mode; the root provider itself remains unchanged. */
			if (drift == 2)
				test_regular_root_resolve_calls = 2;
			test_original_drift = drift;
			outcome = resolve_original_full_xid(CLUSTER_TX_RESOLVE_TERMINAL_CENSUS,
				&resolution);
			UT_ASSERT_EQ(outcome, drift == 4 ? CLUSTER_TX_COMMITTED : CLUSTER_TX_UNKNOWN);
			UT_ASSERT(!FullTransactionIdIsValid(resolution.local_original_full_xid));
			UT_ASSERT_EQ(test_original_calls, 1);
			UT_ASSERT_EQ(test_candidate_held_count, 0);
		}
	}
}

UT_TEST(test_retired_origin_uses_retained_slot_identity_without_extra_acquisition)
{
	for (int same = 0; same < 2; same++) {
		ClusterTxResolution resolution;

		setup_original_full_xid(same);
		test_original_available = false;
		g_retained_origin_available = true;
		g_retained_origin_full = test_original_full_xid;
		UT_ASSERT_EQ(resolve_original_full_xid(CLUSTER_TX_RESOLVE_TERMINAL_CENSUS,
			&resolution), CLUSTER_TX_COMMITTED);
		UT_ASSERT(FullTransactionIdEquals(resolution.local_original_full_xid,
			g_retained_origin_full));
		UT_ASSERT_EQ(test_original_calls, 1);
		UT_ASSERT_EQ(g_retained_origin_reads, 1);
		UT_ASSERT_EQ(memcmp(&g_retained_origin_key, &test_original_key,
			sizeof(test_original_key)), 0);
		UT_ASSERT_EQ(test_candidate_max_held_count, 1);
		UT_ASSERT_EQ(test_candidate_held_count, 0);
		UT_ASSERT_EQ(test_candidate_acquire_calls, same ? 1 : 3);
		UT_ASSERT(resolution.local_creation_target.valid);
	}
}

UT_TEST(test_retained_slot_unknown_or_mismatched_source_preserves_verdict_only)
{
	for (int variant = 0; variant < 8; variant++) {
		ClusterTxResolution resolution;

		setup_original_full_xid(false);
		test_original_available = false;
		g_retained_origin_available = true;
		g_retained_origin_full = test_original_full_xid;
		switch (variant) {
		case 0: g_retained_origin_available = false; break;
		case 1: g_retained_origin_full = InvalidFullTransactionId; break;
		case 2: g_retained_origin_full = FullTransactionIdFromEpochAndXid(9, FrozenTransactionId); break;
		case 3: g_retained_origin_full = FullTransactionIdFromEpochAndXid(9, TEST_ORIGIN_XID+1); break;
		case 4: test_original_key.root_generation++; break;
		case 5: test_original_key.segment_generation++; break;
		case 6: test_original_key.origin_boot_incarnation++; break;
		case 7: test_original_key.admission_record_generation++; break;
		}
		UT_ASSERT_EQ(resolve_original_full_xid(CLUSTER_TX_RESOLVE_TERMINAL_CENSUS,
			&resolution), CLUSTER_TX_COMMITTED);
		UT_ASSERT(!FullTransactionIdIsValid(resolution.local_original_full_xid));
		UT_ASSERT_EQ(test_original_calls, 1);
		UT_ASSERT_EQ(g_retained_origin_reads, 1);
		UT_ASSERT_EQ(test_candidate_held_count, 0);
	}
}

UT_TEST(test_retained_slot_identity_does_not_escape_original_final_fences)
{
	for (int same = 0; same < 2; same++) {
		for (int drift = 1; drift <= 4; drift++) {
			ClusterTxResolution resolution;
			ClusterTxOutcome outcome;

			setup_original_full_xid(same);
			test_original_available = false;
			g_retained_origin_available = true;
			g_retained_origin_full = test_original_full_xid;
			if (drift == 2)
				test_regular_root_resolve_calls = 2;
			g_retained_origin_drift = drift;
			outcome = resolve_original_full_xid(CLUSTER_TX_RESOLVE_TERMINAL_CENSUS,
				&resolution);
			UT_ASSERT_EQ(outcome, drift == 4 ? CLUSTER_TX_COMMITTED : CLUSTER_TX_UNKNOWN);
			UT_ASSERT(!FullTransactionIdIsValid(resolution.local_original_full_xid));
			UT_ASSERT_EQ(test_original_calls, 1);
			UT_ASSERT_EQ(g_retained_origin_reads, 1);
			UT_ASSERT_EQ(test_candidate_held_count, 0);
		}
	}
}

/* Actual origin DATA/canonical guard path, followed by the existing reply
 * encoder. The new version must carry identity, never reuse V3's zero tail. */
UT_TEST(test_original_canonical_creation_selects_explicit_wire_version)
{
	for (int same = 0; same < 2; same++) {
		ClusterTxResolution resolution;
		uint8 page[BLCKSZ];

		setup_original_full_xid(same);
		UT_ASSERT_EQ(resolve_original_full_xid(CLUSTER_TX_RESOLVE_TERMINAL_CENSUS,
			&resolution), CLUSTER_TX_COMMITTED);
		UT_ASSERT(resolution.local_canonical_valid);
		UT_ASSERT(resolution.local_creation_target.valid);
		UT_ASSERT(FullTransactionIdEquals(resolution.local_original_full_xid,
			test_original_full_xid));
		UT_ASSERT(ClusterR4TxVerdictPageEncode(page, &resolution));
		UT_ASSERT_EQ(ClusterR4WireReadU16(page + 4), 4);
		UT_ASSERT_EQ(ClusterR4WireReadU16(page + 6), 224);
	}
}

UT_TEST(test_origin_companion_missing_source_stays_v3_and_fence_drift_clears_candidate)
{
	for (int same = 0; same < 2; same++) {
		ClusterTxResolution resolution, decoded;
		uint8 page[BLCKSZ];

		setup_original_full_xid(same);
		test_original_available = false;
		UT_ASSERT_EQ(resolve_original_full_xid(CLUSTER_TX_RESOLVE_TERMINAL_CENSUS,
			&resolution), CLUSTER_TX_COMMITTED);
		UT_ASSERT(!resolution.origin_companion.present);
		UT_ASSERT(ClusterR4TxVerdictPageEncode(page, &resolution));
		UT_ASSERT_EQ(ClusterR4WireReadU16(page + 4), 3);
		UT_ASSERT(ClusterR4TxVerdictPageDecode(page, &resolution.locator_echo, &decoded));
		UT_ASSERT(!decoded.origin_companion.present);
		for (int drift = 1; drift <= 4; drift++) {
			setup_original_full_xid(same);
			if (drift == 2)
				test_regular_root_resolve_calls = 2;
			test_original_drift = drift;
			resolve_original_full_xid(CLUSTER_TX_RESOLVE_TERMINAL_CENSUS, &resolution);
			UT_ASSERT(!resolution.origin_companion.present);
			UT_ASSERT(!resolution.origin_companion.creation_target.valid);
			UT_ASSERT_EQ(test_candidate_held_count, 0);
		}
	}
}

UT_TEST(test_foreign_v4_remains_separate_and_original_final_admission_clears_all)
{
	int saved_node = cluster_node_id;

	for (int revoke = 0; revoke < 2; revoke++) {
		ClusterTxResolution origin, received, zero = {0};
		uint8 page[BLCKSZ];

		setup_original_full_xid(false);
		UT_ASSERT_EQ(resolve_original_full_xid(CLUSTER_TX_RESOLVE_TERMINAL_CENSUS,
			&origin), CLUSTER_TX_COMMITTED);
		UT_ASSERT(ClusterR4TxVerdictPageEncode(page, &origin));
		test_receiver_registered = true;
		test_receiver_retained = origin;
		test_receiver_peer_boot = origin.origin_companion.key.origin_boot_incarnation;
		test_receiver_peer_generation = 3;
		test_receiver_peer_epoch = origin.origin_companion.key.formation_epoch;
		test_receiver_self_admitted = test_boot_incarnation;
		test_remote_origin_enabled = true;
		test_remote_origin_verdict_page = page;
		test_remote_origin_revoke_admission = revoke;
		cluster_node_id = 1;
		UT_ASSERT_EQ(resolve_original_full_xid(CLUSTER_TX_RESOLVE_TERMINAL_CENSUS,
			&received), revoke ? CLUSTER_TX_UNKNOWN : CLUSTER_TX_COMMITTED);
		cluster_node_id = saved_node;
		UT_ASSERT_EQ(test_remote_origin_calls, 1);
		UT_ASSERT(!received.local_canonical_valid);
		UT_ASSERT(!FullTransactionIdIsValid(received.local_original_full_xid));
		UT_ASSERT(!received.local_creation_target.valid);
		if (revoke)
			UT_ASSERT_EQ(memcmp(&received, &zero, sizeof(zero)), 0);
		else {
			UT_ASSERT(received.origin_companion.present);
			UT_ASSERT_EQ(memcmp(&received.origin_companion, &origin.origin_companion,
				sizeof(origin.origin_companion)), 0);
		}
		test_remote_origin_verdict_page = NULL;
	}
}

UT_TEST(test_foreign_v4_without_receiver_retention_discards_only_companion)
{
	ClusterTxResolution origin, received;
	ClusterTxCanonicalCompanionV1 zero = {0};
	uint8 page[BLCKSZ];
	int saved_node = cluster_node_id;

	setup_original_full_xid(false);
	UT_ASSERT_EQ(resolve_original_full_xid(CLUSTER_TX_RESOLVE_TERMINAL_CENSUS,
		&origin), CLUSTER_TX_COMMITTED);
	UT_ASSERT(ClusterR4TxVerdictPageEncode(page, &origin));
	test_remote_origin_enabled = true;
	test_remote_origin_verdict_page = page;
	cluster_node_id = 1;
	UT_ASSERT_EQ(resolve_original_full_xid(CLUSTER_TX_RESOLVE_TERMINAL_CENSUS,
		&received), CLUSTER_TX_COMMITTED);
	cluster_node_id = saved_node;
	UT_ASSERT_EQ(test_remote_origin_calls, 1);
	UT_ASSERT_EQ(memcmp(&received.origin_companion, &zero, sizeof(zero)), 0);
	UT_ASSERT_EQ(memcmp(&received.locator_echo, &origin.locator_echo,
		sizeof(origin.locator_echo)), 0);
	UT_ASSERT_EQ(received.commit_scn, origin.commit_scn);
	UT_ASSERT(!received.local_canonical_valid);
	UT_ASSERT(!FullTransactionIdIsValid(received.local_original_full_xid));
	UT_ASSERT(!received.local_creation_target.valid);
	test_remote_origin_verdict_page = NULL;
}

UT_TEST(test_foreign_retained_companion_cannot_escape_source_or_root_drift)
{
	for (unsigned drift = 1; drift <= 17; drift++) {
		ClusterTxResolution origin, received;
		ClusterTxCanonicalCompanionV1 zero = {0};
		uint8 page[BLCKSZ];
		int saved_node = cluster_node_id;

		setup_original_full_xid(false);
		UT_ASSERT_EQ(resolve_original_full_xid(CLUSTER_TX_RESOLVE_TERMINAL_CENSUS,
			&origin), CLUSTER_TX_COMMITTED);
		UT_ASSERT(ClusterR4TxVerdictPageEncode(page, &origin));
		test_receiver_registered = true;
		test_receiver_retained = origin;
		test_receiver_peer_boot = origin.origin_companion.key.origin_boot_incarnation;
		test_receiver_peer_generation = 3;
		test_receiver_peer_epoch = origin.origin_companion.key.formation_epoch;
		test_receiver_self_admitted = test_boot_incarnation;
		test_remote_origin_enabled = true;
		test_remote_origin_verdict_page = page;
		if (drift <= 9) test_receiver_drift = drift;
		switch (drift) {
		case 10: test_receiver_peer_boot++; break;
		case 11: test_receiver_peer_generation = 0; break;
		case 12: test_receiver_peer_epoch++; break;
		case 13: test_receiver_self_admitted++; break;
		case 14: test_system_identifier++; break;
		case 15: test_receiver_retained.origin_companion.original_full_xid
			= FullTransactionIdFromEpochAndXid(10, TEST_ORIGIN_XID); break;
		case 16: test_receiver_retained.origin_companion.key.segment_generation++; break;
		case 17: test_receiver_retained.locator_echo.uba = uba_encode(TEST_RECORD_SEGMENT, 9, 1, 8); break;
		}
		cluster_node_id = 1;
		UT_ASSERT_EQ(resolve_original_full_xid(CLUSTER_TX_RESOLVE_TERMINAL_CENSUS,
			&received), drift == 8 ? CLUSTER_TX_UNKNOWN : CLUSTER_TX_COMMITTED);
		cluster_node_id = saved_node;
		UT_ASSERT_EQ(memcmp(&received.origin_companion, &zero, sizeof(zero)), 0);
		UT_ASSERT(!received.local_canonical_valid);
		UT_ASSERT(!FullTransactionIdIsValid(received.local_original_full_xid));
		UT_ASSERT(!received.local_creation_target.valid);
		UT_ASSERT_EQ(test_remote_origin_calls, 1);
		UT_ASSERT_EQ(test_candidate_held_count, 0);
	}
}

int
main(void)
{
	UT_PLAN(210);
	UT_RUN(test_original_full_xid_reaches_same_and_cross_segment_census_result);
	UT_RUN(test_original_full_xid_missing_or_key_alias_keeps_verdict_without_identity);
	UT_RUN(test_original_full_xid_not_queried_for_other_roles_or_abort);
	UT_RUN(test_original_full_xid_does_not_escape_final_fence_drift);
	UT_RUN(test_retired_origin_uses_retained_slot_identity_without_extra_acquisition);
	UT_RUN(test_retained_slot_unknown_or_mismatched_source_preserves_verdict_only);
	UT_RUN(test_retained_slot_identity_does_not_escape_original_final_fences);
	UT_RUN(test_retained_active_identity_reaches_real_undo_pending);
	UT_RUN(test_retained_undo_pending_requires_matching_active_tt_receipt);
	UT_RUN(test_retained_undo_pending_guard_drift_cannot_publish);
	UT_RUN(test_retained_active_identity_reaches_real_mx_batch);
	UT_RUN(test_retained_active_mx_batch_scrubs_earlier_local_proof);
	UT_RUN(test_retained_active_mx_batch_rechecks_remote_epoch);
	UT_RUN(test_retained_active_mx_batch_rejects_remote_identity_mismatch);
	UT_RUN(test_retained_active_mx_batch_rejects_missing_remote_proof);
	UT_RUN(test_retained_active_mx_batch_rejects_invalid_remote_commit_scn);
	UT_RUN(test_retained_active_mx_batch_rejects_remote_committed_grant);
	UT_RUN(test_retained_mx_precommit_retries_preserve_batch_deadline);
	UT_RUN(test_local_updater_rolled_active_preserves_original_identity);
	UT_RUN(test_local_updater_rolled_active_accepts_duplicate_ctrc_touch);
	UT_RUN(test_local_updater_rolled_active_requires_canonical_slot);
	UT_RUN(test_local_updater_rolled_active_rechecks_touch_and_original_data);
	UT_RUN(test_local_updater_rolled_active_final_release_fences);
	UT_RUN(test_local_updater_precommit_retries_only_after_guard_release);
	UT_RUN(test_local_updater_precommit_release_and_admission_failure_stay_unknown);
	UT_RUN(test_local_updater_does_not_retry_unproven_samples);
	UT_RUN(test_local_updater_precommit_resamples_terminal_without_grant);
	UT_RUN(test_current_mx_precommit_resamples_exact_terminal_without_grant);
	UT_RUN(test_current_mx_precommit_keeps_unknown_and_release_fences);
	UT_RUN(test_current_mx_malformed_active_is_not_precommit_retry);
	UT_RUN(test_current_mx_local_precommit_returns_retry_after_guard_release);
	UT_RUN(test_self_fresh_role_does_not_inherit_ordinary_abort_permission);
	UT_RUN(test_local_retained_read_bound_precedes_exact_pair_without_promoting_it);
	UT_RUN(test_local_retained_read_preserves_same_stamp_origin_exact);
	UT_RUN(test_local_retained_exact_does_not_replace_the_snapshot_or_census_role);
	UT_RUN(test_local_retained_exact_mismatch_and_epoch_drift_stay_unproven);
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
	UT_RUN(test_physical_locator_precommit_retry_marks_committed_window);
	UT_RUN(test_ctrc_current_owner_uses_published_binding_across_rollover);
	UT_RUN(test_other_backend_rolled_active_uses_locator_and_ctrc);
	UT_RUN(test_other_backend_rolled_active_accepts_duplicate_ctrc_touch);
	UT_RUN(test_current_mx_other_backend_rolled_active_records_original_touch);
	UT_RUN(test_retained_active_mx_batch_accepts_duplicate_ctrc_touch);
	UT_RUN(test_retained_active_mx_batch_refuses_ctrc_touch);
	UT_RUN(test_retained_active_mx_batch_rechecks_local_grant_after_release);
	UT_RUN(test_retained_active_mx_batch_rechecks_admission_and_epoch_after_release);
	UT_RUN(test_other_backend_rolled_terminal_uses_canonical_status);
	UT_RUN(test_other_backend_rolled_locator_failures_do_not_acquire);
	UT_RUN(test_other_backend_rolled_locator_requires_native_retained_era);
	UT_RUN(test_other_backend_rolled_locator_rechecks_scope_after_scan);
	UT_RUN(test_other_backend_rolled_locator_requires_exact_canonical_proof);
	UT_RUN(test_other_backend_rolled_lookup_rechecks_after_release);
	UT_RUN(test_other_backend_locator_does_not_expand_fast_paths);
	UT_RUN(test_current_owner_fast_paths_recheck_released_identity);
	UT_RUN(test_current_owner_fast_paths_recheck_release_authority);
	UT_RUN(test_current_mx_fast_paths_reject_released_identity);
	UT_RUN(test_current_mx_active_proof_reconstructs_exact_ctrc_identity);
	UT_RUN(test_current_mx_active_proof_uses_origin_binding_without_foreign_storage);
	UT_RUN(test_current_member_rolled_terminal_uses_locator_then_canonical_scur);
	UT_RUN(test_current_member_terminal_on_current_segment_survives_retired_owner_index);
	UT_RUN(test_current_member_local_terminal_accepts_clean_formation_epoch_zero);
	UT_RUN(test_local_terminal_locator_rejects_unproven_native_era_before_io);
	UT_RUN(test_local_terminal_locator_rechecks_native_scope_after_scan);
	UT_RUN(test_local_terminal_rechecks_scope_and_root_after_release);
	UT_RUN(test_local_terminal_scope_drift_clears_precommit_retry);
	UT_RUN(test_local_terminal_precommit_window_requests_retry);
	UT_RUN(test_local_terminal_retained_boundary_and_counter_advance_stay_valid);
	UT_RUN(test_current_mx_terminal_fallback_cannot_bypass_native_era);
	UT_RUN(test_current_member_active_on_current_segment_requires_live_owner_index);
	UT_RUN(test_exact_origin_committed_uses_canonical_tt_identity_and_direct_clog);
	UT_RUN(test_visibility_remote_origin_selects_generation_without_foreign_scur);
	UT_RUN(test_terminal_census_remote_origin_selects_generation_without_foreign_scur);
	UT_RUN(test_remote_origin_failed_proof_or_revoked_admission_clears_output);
	UT_RUN(test_terminal_census_local_origin_uses_resident_candidate2_and_canonical_upgrade);
	UT_RUN(test_terminal_census_nonresident_cleanout_uses_exact_local_c1b_pair);
	UT_RUN(test_terminal_census_retained_pair_negative_matrix_fails_closed);
	UT_RUN(test_terminal_census_same_owner_cross_segment_is_sequential_and_exact);
	UT_RUN(test_visibility_same_owner_cross_segment_is_sequential_and_exact);
	UT_RUN(test_local_origin_cold_first_and_recheck_keep_exact_tt_proof);
	UT_RUN(test_local_origin_cold_only_after_tt_phase_still_revalidates);
	UT_RUN(test_local_origin_cold_record_change_is_not_hidden);
	UT_RUN(test_local_origin_cold_generation_change_is_not_hidden);
	UT_RUN(test_local_origin_disabled_pool_is_unknown_not_success);
	UT_RUN(test_local_origin_warm_data_adds_no_readthrough);
	UT_RUN(test_local_origin_readthrough_exception_cancels_guard);
	UT_RUN(test_split_origin_cold_first_and_recheck_never_start_io);
	UT_RUN(test_visibility_precommit_committed_slot_with_live_origin_stays_in_progress);
	UT_RUN(test_canonical_sample_reports_first_failed_predicate);
	UT_RUN(test_recycled_canonical_abort_rechecks_exact_data_and_origin);
	UT_RUN(test_recycled_canonical_abort_same_segment);
	UT_RUN(test_reborn_empty_canonical_preserves_exact_origin_abort);
	UT_RUN(test_reborn_allocated_canonical_preserves_exact_origin_abort);
	UT_RUN(test_reborn_canonical_nonabort_refuses_in_both_modes);
	UT_RUN(test_reborn_canonical_malformed_or_initial_generation_refuses);
	UT_RUN(test_reborn_canonical_revocation_truncation_and_data_drift_refuse);
	UT_RUN(test_reborn_split_plan_rechecks_before_publication);
	UT_RUN(test_terminal_census_split_plan_rechecks_before_publication);
	UT_RUN(test_terminal_census_recycled_abort_proof_and_boundaries);
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
	UT_RUN(test_origin_tx_plan_preserves_strict_zero_and_final_generation_recheck);
	UT_RUN(test_exact_origin_memo_reuses_only_proven_terminal_outcomes);
	UT_RUN(test_local_terminal_consumes_exact_origin_memo);
	UT_RUN(test_physical_terminal_populates_shared_exact_memo);
	UT_RUN(test_physical_terminal_publication_waits_for_final_sample_fences);
	UT_RUN(test_physical_terminal_never_memoizes_unproven_outcomes);
	UT_RUN(test_local_terminal_memo_requires_current_physical_identity);
	UT_RUN(test_local_terminal_memo_hit_preserves_final_fences);
	UT_RUN(test_exact_origin_memo_never_promotes_precommit_or_prepared);
	UT_RUN(test_exact_origin_memo_isolates_epoch_generation_and_slot_identity);
	UT_RUN(test_exact_origin_memo_rechecks_each_data_carrier);
	UT_RUN(test_exact_origin_memo_publishes_after_final_data_recheck);
	UT_RUN(test_exact_origin_memo_hit_preserves_final_data_and_generation_fences);
	UT_RUN(test_aborted_delete_target_comes_from_final_exact_record);
	UT_RUN(test_aborted_update_target_names_old_tuple_not_replacement);
	UT_RUN(test_modification_projection_never_upgrades_committed_or_insert_record);
	UT_RUN(test_aborted_modification_projection_rejects_malformed_record_image);
	UT_RUN(test_aborted_modification_target_not_exported_after_final_data_drift);
	UT_RUN(test_later_creation_result_clears_aborted_modification_projection);
	UT_RUN(test_insert_creation_target_comes_from_final_exact_record);
	UT_RUN(test_update_creation_target_names_replacement_not_old_tuple);
	UT_RUN(test_noncreation_or_malformed_payload_keeps_verdict_without_target);
	UT_RUN(test_creation_target_is_not_exported_after_final_data_fence_drift);
	UT_RUN(test_original_canonical_creation_selects_explicit_wire_version);
	UT_RUN(test_origin_companion_missing_source_stays_v3_and_fence_drift_clears_candidate);
	UT_RUN(test_foreign_v4_remains_separate_and_original_final_admission_clears_all);
	UT_RUN(test_foreign_v4_without_receiver_retention_discards_only_companion);
	UT_RUN(test_foreign_retained_companion_cannot_escape_source_or_root_drift);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
