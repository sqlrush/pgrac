/*-------------------------------------------------------------------------
 *
 * cluster_runtime_visibility.c
 *	  pgrac spec-6.12 wave 6.12i (缺口 A) — D-i1 undo-block CF fetch
 *	  orchestration + the runtime live-authority gate wrapper.
 *
 *	  Active runtime has no materialized marker to admit the by-xid
 *	  durable-TT resolution of a recycled remote ITL slot, so the origin's
 *	  LMS co-samples a live authority triple {origin_epoch, live_hwm_lsn,
 *	  tt_generation} into the very undo-block reply that carries its TT
 *	  (spec-6.12 §2.11 "live authority source").  This file is the
 *	  requester-side consumer: it rides the spec-6.12b CR-server wire
 *	  (cluster_gcs_block_undo_tt_fetch_and_wait), caches the fetched
 *	  block + authority PAIR (L2 CR pool bytes + a per-backend authority
 *	  memo, Q-i5), and exposes the runtime covers() gate.
 *
 *	  The bytes and the authority are only ever served TOGETHER, exactly
 *	  as co-sampled: a cache hit returns the authority sampled with the
 *	  cached bytes, never a newer one — a scan over the bytes may only
 *	  claim the coverage window of ITS OWN sample, or a 0-match inside a
 *	  later window would be mistaken for proof (D-i2 condition (c)).
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/backend/cluster/cluster_runtime_visibility.c
 *
 * NOTES
 *	  This is a pgrac-original file.  Compiled only in --enable-cluster
 *	  builds.  Every miss path returns false so the caller keeps the
 *	  pre-existing 53R97 fail-closed boundary (规则 8.A: this wave only
 *	  widens "resolve when provable", never "resolve when unprovable").
 *	  Spec: spec-6.12-crossnode-cache-fusion-perf-optimization.md (wave i)
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#ifdef USE_PGRAC_CLUSTER

#include "access/clog.h"
#include "access/subtrans.h"
#include "access/transam.h"
#include "access/twophase.h"
#include "access/xlog.h"
#include "cluster/cluster_cr.h"
#include "cluster/cluster_cr_cache.h"
#include "cluster/cluster_cr_pool.h"
#include "cluster/cluster_cr_server.h"
#include "cluster/cluster_conf.h"
#include "cluster/cluster_cssd.h" /* cluster_cssd_get_peer_state (D3-3 Q9 serve-gate) */
#include "cluster/cluster_elog.h" /* cluster_node_id */
#include "cluster/cluster_epoch.h"
#include "cluster/cluster_gcs_block.h"
#include "cluster/cluster_guc.h"
#include "cluster/cluster_mode.h" /* cluster_peer_mode_enabled (D3-2) */
#include "cluster/cluster_membership.h"
#include "cluster/cluster_multixact_current.h"
#include "cluster/cluster_reconfig.h"
#include "cluster/cluster_runtime_visibility.h"
#include "cluster/cluster_recovery_merge.h"
#include "cluster/cluster_qvotec.h"
#include "cluster/cluster_semantic_activation.h"
#include "cluster/cluster_sf_dep.h" /* peer HELLO D4-capability gate (D4-6) */
#include "cluster/cluster_terminal_ref_census.h"
#include "cluster/cluster_tt_durable.h"
#include "cluster/cluster_tt_local.h"
#include "cluster/cluster_tt_slot.h"
#include "cluster/cluster_tt_status.h"
#include "cluster/cluster_tx_resolve.h"
#include "cluster/cluster_uba.h"
#include "cluster/cluster_undo_authority.h" /* dead-owner serve authority (D4-4) */
#include "cluster/cluster_undo_gcs.h"		/* cluster_undo_block_acquire_shared (D3-2) */
#include "cluster/cluster_undo_resid.h"		/* cluster_undo_resid_encode (D3-2) */
#include "cluster/cluster_undo_record.h"
#include "cluster/cluster_undo_record_api.h"
#include "cluster/cluster_undo_segment.h"
#include "cluster/cluster_undo_verdict.h"	/* verdict taxonomy + entry (D3-3/D3-4) */
#include "cluster/cluster_undo_horizon.h"	/* D5-8 read admission (spec-5.22e) */
#include "cluster/cluster_xid_stripe.h"
#include "cluster/storage/cluster_undo_block0_current.h"
#include "cluster/storage/cluster_undo_buf.h"
#include "storage/ipc.h"
#include "storage/lwlock.h"
#include "storage/procarray.h"
#include "utils/snapmgr.h"

static XidStatus
cluster_runtime_visibility_direct_xid_status(TransactionId xid)
{
	XLogRecPtr xid_lsn;

	return TransactionIdGetStatus(xid, &xid_lsn);
}

void
cluster_runtime_visibility_ensure_exit_hooks(void)
{
	cluster_undo_block0_current_ensure_exit_hooks();
}

/* The Stage-8 clean four-node formation legitimately keeps epoch zero.  A
 * fresh-ref pair at that epoch is usable only while the ordinary R4 TARGET
 * admission is exact-current; the token is held across the complete wire or
 * origin proof so a cutover cannot turn an epoch-zero syntax match into
 * authority.  The caller owns cluster_semantic_activation_leave(). */
bool
cluster_runtime_visibility_zero_epoch_pair_admission_enter(
	ClusterSemanticAdmissionToken *token)
{
	ClusterSemanticAdmissionResult result;

	if (token != NULL)
		memset(token, 0, sizeof(*token));
	if (token == NULL || cluster_conf_node_count() != 4
		|| !cluster_storage_mode_enabled() || cluster_recmerge_window_active
		|| cluster_epoch_get_current() != 0)
		return false;
	result = cluster_semantic_activation_enter(
		CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1,
		CLUSTER_SEMANTIC_TARGET_SIDE, token);
	if (result != CLUSTER_SEMANTIC_ADMISSION_OK)
		return false;
	if (token->record_generation == 0 || token->formation_epoch != 0
		|| !cluster_semantic_activation_recheck(token)) {
		cluster_semantic_activation_leave(token);
		memset(token, 0, sizeof(*token));
		return false;
	}
	return true;
}

static bool
cluster_runtime_visibility_direct_xid_committed(TransactionId xid)
{
	return cluster_runtime_visibility_direct_xid_status(xid) == TRANSACTION_STATUS_COMMITTED;
}

typedef struct ClusterRuntimeSubtransSample {
	TransactionId xids[CLUSTER_R4_SUBTRANS_MAX_DEPTH];
	int count;
} ClusterRuntimeSubtransSample;

/*
 * Complete the native CLOG/pg_twophase/CLOG bracket for a live xid.  A
 * terminal second CLOG observation always wins over the prepared predicate;
 * callers still need an exact durable TT commit SCN before publishing a
 * COMMITTED result.
 */
static XidStatus
cluster_runtime_visibility_recheck_prepared(TransactionId xid, XidStatus first_status,
											bool *prepared_out)
{
	bool prepared;
	XidStatus second_status;

	Assert(prepared_out != NULL);
	*prepared_out = false;
	if (first_status != TRANSACTION_STATUS_IN_PROGRESS)
		return first_status;

	prepared = TwoPhaseTransactionIdIsPrepared(xid);
	second_status = cluster_runtime_visibility_direct_xid_status(xid);
	if (second_status == TRANSACTION_STATUS_IN_PROGRESS)
		*prepared_out = prepared;
	return second_status;
}

/*
 * Sample every immediate pg_subtrans edge, including the top->Invalid edge.
 * SubTransGetParent() owns native SLRU locking and ERROR behavior; deliberately
 * do not wrap it in PG_TRY, so physical I/O/corruption failures propagate.
 */
static bool
cluster_runtime_visibility_sample_subtrans(TransactionId child,
										   ClusterRuntimeSubtransSample *sample,
										   ClusterTxResolveReason *reason_out)
{
	TransactionId current = child;
	uint32 previous_distance = 0;
	int depth;

	Assert(sample != NULL);
	Assert(reason_out != NULL);
	memset(sample, 0, sizeof(*sample));

	for (depth = 0; depth < CLUSTER_R4_SUBTRANS_MAX_DEPTH; depth++) {
		TransactionId parent;
		uint32 parent_distance;

		if (TransactionIdPrecedes(current, TransactionXmin)) {
			*reason_out = CLUSTER_TX_RESOLVE_COVERAGE_GAP;
			return false;
		}

		parent = SubTransGetParent(current);
		sample->xids[depth] = current;
		sample->count = depth + 1;

		if (!TransactionIdIsValid(parent))
			return true;
		if (!TransactionIdIsNormal(parent)) {
			*reason_out = CLUSTER_TX_RESOLVE_COVERAGE_GAP;
			return false;
		}
		parent_distance = (uint32)(child - parent);
		if (parent_distance == 0 || parent_distance >= ((uint32)0x80000000U)
			|| parent_distance <= previous_distance) {
			*reason_out = CLUSTER_TX_RESOLVE_SUBTRANS_CYCLE;
			return false;
		}
		previous_distance = parent_distance;
		current = parent;
	}

	*reason_out = CLUSTER_TX_RESOLVE_SUBTRANS_DEPTH;
	return false;
}

static bool
cluster_runtime_visibility_recheck_subtrans(const ClusterRuntimeSubtransSample *sample,
											ClusterTxResolveReason *reason_out)
{
	int i;

	Assert(sample != NULL);
	Assert(reason_out != NULL);
	for (i = 0; i < sample->count; i++) {
		TransactionId expected_parent;
		TransactionId parent;

		if (TransactionIdPrecedes(sample->xids[i], TransactionXmin)) {
			*reason_out = CLUSTER_TX_RESOLVE_COVERAGE_GAP;
			return false;
		}
		parent = SubTransGetParent(sample->xids[i]);
		expected_parent = i + 1 < sample->count ? sample->xids[i + 1] : InvalidTransactionId;
		if (!TransactionIdEquals(parent, expected_parent)) {
			*reason_out = CLUSTER_TX_RESOLVE_SUBTRANS_CHANGED;
			return false;
		}
	}
	return true;
}

typedef struct ClusterRuntimeCandidateCleanup {
	ClusterUndoBlock0CurrentGuard *guard;
	bool active;
} ClusterRuntimeCandidateCleanup;

static void
cluster_runtime_visibility_candidate_cleanup(int code, Datum arg)
{
	ClusterRuntimeCandidateCleanup *cleanup
		= (ClusterRuntimeCandidateCleanup *)DatumGetPointer(arg);

	(void)code;
	if (cleanup != NULL && cleanup->active && cleanup->guard != NULL) {
		cluster_undo_block0_current_cancel(cleanup->guard);
		cleanup->active = false;
	}
}

static ClusterUndoBlock0CurrentStep
cluster_runtime_visibility_candidate_acquire_until(
	const ClusterUndoBlock0LogicalKey *logical,
	const ClusterSemanticAdmissionToken *admission,
	ClusterUndoBlock0CurrentGuard *guard,
	ClusterRuntimeCandidateCleanup *cleanup,
	ClusterUndoBlock0Result *failure, TimestampTz deadline)
{
	ClusterUndoBlock0CurrentStep step;
	int timeout_ms = 0;

	if (deadline != 0) {
		TimestampTz now = GetCurrentTimestamp();
		TimestampTz remaining_us;

		if (now >= deadline) {
			cluster_vis_evidence_note(CLUSTER_VIS_METRIC_AUTHORITY_TIMEOUT);
			return CLUSTER_UNDO_BLOCK0_CURRENT_FAILED;
		}
		remaining_us = deadline - now;
		timeout_ms = remaining_us / 1000 >= INT_MAX
			? INT_MAX : (int)Max((remaining_us + 999) / 1000, 1);
	}

	step = cluster_undo_block0_current_acquire_begin_admitted(
		logical, CLUSTER_UNDO_BLOCK0_SCUR, timeout_ms, admission, guard,
		failure);
	if (cleanup != NULL
		&& (step == CLUSTER_UNDO_BLOCK0_CURRENT_PENDING
			|| step == CLUSTER_UNDO_BLOCK0_CURRENT_HELD))
		cleanup->active = true;
	while (step == CLUSTER_UNDO_BLOCK0_CURRENT_PENDING) {
		cluster_vis_evidence_note(CLUSTER_VIS_METRIC_TRANSIENT_PENDING);
		CHECK_FOR_INTERRUPTS();
		if (deadline != 0 && GetCurrentTimestamp() >= deadline) {
			cluster_vis_evidence_note(CLUSTER_VIS_METRIC_AUTHORITY_TIMEOUT);
			cluster_undo_block0_current_cancel(guard);
			if (cleanup != NULL)
				cleanup->active = false;
			return CLUSTER_UNDO_BLOCK0_CURRENT_FAILED;
		}
		step = cluster_undo_block0_current_acquire_poll(guard, failure);
		if (step == CLUSTER_UNDO_BLOCK0_CURRENT_PENDING
			&& !cluster_undo_block0_current_wait_reply(
				guard, CLUSTER_UNDO_BLOCK0_WAIT_RUNTIME_VIS_ACQUIRE))
			pg_usleep(1000L);
	}
	if (step != CLUSTER_UNDO_BLOCK0_CURRENT_HELD
		&& cleanup != NULL && cleanup->active) {
		cluster_undo_block0_current_cancel(guard);
		cleanup->active = false;
	}
	/* A synchronous candidate must never leak an unfinished observation. */
	if (step == CLUSTER_UNDO_BLOCK0_CURRENT_PENDING) {
		cluster_vis_evidence_note(CLUSTER_VIS_METRIC_PREMATURE_EXIT);
		Assert(false);
	}
	return step;
}

static ClusterUndoBlock0CurrentStep
cluster_runtime_visibility_candidate_acquire(
	const ClusterUndoBlock0LogicalKey *logical,
	const ClusterSemanticAdmissionToken *admission,
	ClusterUndoBlock0CurrentGuard *guard,
	ClusterRuntimeCandidateCleanup *cleanup,
	ClusterUndoBlock0Result *failure)
{
	return cluster_runtime_visibility_candidate_acquire_until(
		logical, admission, guard, cleanup, failure, 0);
}

static ClusterUndoBlock0CurrentStep
cluster_runtime_visibility_candidate_release_until(
	ClusterUndoBlock0CurrentGuard *guard, ClusterUndoBlock0Result *failure,
	TimestampTz deadline)
{
	ClusterUndoBlock0CurrentStep step;

	step = cluster_undo_block0_current_release_begin(guard, failure);
	while (step == CLUSTER_UNDO_BLOCK0_CURRENT_PENDING) {
		cluster_vis_evidence_note(CLUSTER_VIS_METRIC_TRANSIENT_PENDING);
		CHECK_FOR_INTERRUPTS();
		if (deadline != 0 && GetCurrentTimestamp() >= deadline) {
			cluster_vis_evidence_note(CLUSTER_VIS_METRIC_AUTHORITY_TIMEOUT);
			cluster_undo_block0_current_cancel(guard);
			return CLUSTER_UNDO_BLOCK0_CURRENT_FAILED;
		}
		step = cluster_undo_block0_current_release_poll(guard, failure);
		if (step == CLUSTER_UNDO_BLOCK0_CURRENT_PENDING
			&& !cluster_undo_block0_current_wait_reply(
				guard, CLUSTER_UNDO_BLOCK0_WAIT_RUNTIME_VIS_RELEASE))
			pg_usleep(1000L);
	}
	if (step == CLUSTER_UNDO_BLOCK0_CURRENT_PENDING) {
		cluster_vis_evidence_note(CLUSTER_VIS_METRIC_PREMATURE_EXIT);
		Assert(false);
	}
	return step;
}

static ClusterUndoBlock0CurrentStep
cluster_runtime_visibility_candidate_release(
	ClusterUndoBlock0CurrentGuard *guard, ClusterUndoBlock0Result *failure)
{
	return cluster_runtime_visibility_candidate_release_until(
		guard, failure, 0);
}

static bool
cluster_runtime_visibility_candidate_root_matches(
	const ClusterUndoBlock0ResolvedRoot *left,
	const ClusterUndoBlock0ResolvedRoot *right)
{
	return left != NULL && right != NULL && left->intent == right->intent
		   && left->root_id == right->root_id
		   && left->root_generation == right->root_generation;
}

static ClusterTxOutcome
cluster_runtime_visibility_candidate_decide(
	const ClusterTxLocator *locator, ClusterTxResolveMode mode,
	const TTSlot *exact_slot,
	TransactionId *top_xid_out, ClusterTxProofKind *proof_kind_out,
	SCN *commit_scn_out, ClusterTxResolveReason *reason_out,
	ClusterRuntimeVisibilityCanonicalDiagnostic *diagnostic)
{
	XidStatus native_status;
	TransactionId top_xid = locator->xid;
	ClusterTxProofKind proof_kind = CLUSTER_TX_PROOF_ORIGIN_DURABLE_TT_CLOG;
	SCN commit_scn = InvalidScn;
	bool prepared = false;

	if (diagnostic != NULL) {
		diagnostic->locator_xid = locator->xid;
		diagnostic->locator_wrap = locator->tt_wrap;
		diagnostic->slot_status = exact_slot->status;
		diagnostic->slot_xid = exact_slot->xid;
		diagnostic->slot_wrap = exact_slot->wrap;
		diagnostic->slot_commit_scn = exact_slot->commit_scn;
	}
	if (exact_slot->status > TT_SLOT_RECYCLABLE) {
		if (diagnostic != NULL)
			diagnostic->first_failure
				= CLUSTER_RUNTIME_VISIBILITY_CANONICAL_FAILURE_SLOT_DOMAIN;
		*reason_out = CLUSTER_TX_RESOLVE_AUTHORITY_UNAVAILABLE;
		return CLUSTER_TX_UNKNOWN;
	}
	if (exact_slot->xid != locator->xid
		|| exact_slot->wrap != locator->tt_wrap) {
		if (diagnostic != NULL)
			diagnostic->first_failure
				= CLUSTER_RUNTIME_VISIBILITY_CANONICAL_FAILURE_SLOT_IDENTITY;
		*reason_out = CLUSTER_TX_RESOLVE_AUTHORITY_UNAVAILABLE;
		return CLUSTER_TX_UNKNOWN;
	}

	native_status = cluster_runtime_visibility_direct_xid_status(locator->xid);
	if (diagnostic != NULL) {
		diagnostic->native_sampled = true;
		diagnostic->native_status = native_status;
	}
	if (native_status == TRANSACTION_STATUS_SUB_COMMITTED) {
		ClusterRuntimeSubtransSample subtrans;

		if (!cluster_runtime_visibility_sample_subtrans(
				locator->xid, &subtrans, reason_out))
			return CLUSTER_TX_UNKNOWN;
		top_xid = subtrans.xids[subtrans.count - 1];
		native_status = cluster_runtime_visibility_direct_xid_status(top_xid);
		native_status = cluster_runtime_visibility_recheck_prepared(
			top_xid, native_status, &prepared);
		if (diagnostic != NULL) {
			diagnostic->prepared_sampled = true;
			diagnostic->prepared = prepared;
			diagnostic->native_status = native_status;
		}
		if (!cluster_runtime_visibility_recheck_subtrans(&subtrans, reason_out))
			return CLUSTER_TX_UNKNOWN;
		proof_kind = CLUSTER_TX_PROOF_ORIGIN_SUBTRANS_TOP;
		if (native_status == TRANSACTION_STATUS_COMMITTED) {
			*reason_out = exact_slot->status == TT_SLOT_ABORTED
				? CLUSTER_TX_RESOLVE_AUTHORITY_CONFLICT
				: CLUSTER_TX_RESOLVE_COVERAGE_GAP;
			return CLUSTER_TX_UNKNOWN;
		}
	} else if (native_status == TRANSACTION_STATUS_IN_PROGRESS) {
		native_status = cluster_runtime_visibility_recheck_prepared(
			locator->xid, native_status, &prepared);
		if (diagnostic != NULL) {
			diagnostic->prepared_sampled = true;
			diagnostic->prepared = prepared;
			diagnostic->native_status = native_status;
		}
		if (prepared)
			proof_kind = CLUSTER_TX_PROOF_ORIGIN_TWOPHASE;
	}

	if (native_status == TRANSACTION_STATUS_ABORTED) {
		if (exact_slot->status == TT_SLOT_COMMITTED) {
			*reason_out = CLUSTER_TX_RESOLVE_AUTHORITY_CONFLICT;
			return CLUSTER_TX_UNKNOWN;
		}
		*top_xid_out = top_xid;
		*proof_kind_out = proof_kind;
		*commit_scn_out = InvalidScn;
		return CLUSTER_TX_ABORTED;
	}
	if (native_status == TRANSACTION_STATUS_COMMITTED) {
		if (exact_slot->status == TT_SLOT_ABORTED) {
			*reason_out = CLUSTER_TX_RESOLVE_AUTHORITY_CONFLICT;
			return CLUSTER_TX_UNKNOWN;
		}
		if (exact_slot->status != TT_SLOT_COMMITTED
			|| !SCN_VALID(exact_slot->commit_scn)) {
			*reason_out = CLUSTER_TX_RESOLVE_COVERAGE_GAP;
			return CLUSTER_TX_UNKNOWN;
		}
		commit_scn = exact_slot->commit_scn;
		*top_xid_out = top_xid;
		*proof_kind_out = proof_kind;
		*commit_scn_out = commit_scn;
		return CLUSTER_TX_COMMITTED;
	}
	if (native_status == TRANSACTION_STATUS_IN_PROGRESS) {
		if (prepared
			&& (exact_slot->status == TT_SLOT_ACTIVE
				|| exact_slot->status == TT_SLOT_ABORTED)) {
			*top_xid_out = top_xid;
			*proof_kind_out = proof_kind;
			*commit_scn_out = InvalidScn;
			return CLUSTER_TX_PREPARED;
		}
		if (!prepared && mode == CLUSTER_TX_RESOLVE_VISIBILITY
			&& (exact_slot->status == TT_SLOT_ACTIVE
				|| (exact_slot->status == TT_SLOT_COMMITTED
					&& SCN_VALID(exact_slot->commit_scn)))) {
			*top_xid_out = top_xid;
			*proof_kind_out = proof_kind;
			*commit_scn_out = InvalidScn;
			return CLUSTER_TX_IN_PROGRESS;
		}
	}

	if (diagnostic != NULL)
		diagnostic->first_failure
			= CLUSTER_RUNTIME_VISIBILITY_CANONICAL_FAILURE_NATIVE_OUTCOME;
	*reason_out = CLUSTER_TX_RESOLVE_AUTHORITY_UNAVAILABLE;
	return CLUSTER_TX_UNKNOWN;
}

static bool
cluster_runtime_visibility_admission_current(
	ClusterTxResolveMode mode,
	const ClusterSemanticAdmissionToken *admission)
{
	if (mode == CLUSTER_TX_RESOLVE_VISIBILITY)
		return cluster_semantic_activation_recheck(admission);
	if (mode == CLUSTER_TX_RESOLVE_TERMINAL_CENSUS)
		return cluster_semantic_activation_recheck_r4_terminal_census(admission);
	return false;
}

static bool
cluster_runtime_visibility_resolve_root_admitted(
	ClusterTxResolveMode mode,
	const ClusterSemanticAdmissionToken *admission, ClusterUndoPathIntent intent,
	uint32 owner_instance, uint32 segment_id,
	ClusterUndoBlock0ResolvedRoot *out)
{
	if (mode == CLUSTER_TX_RESOLVE_VISIBILITY)
		return cluster_semantic_activation_resolve_shared_undo_root(
			admission, intent, owner_instance, segment_id, out);
	if (mode == CLUSTER_TX_RESOLVE_TERMINAL_CENSUS)
		return cluster_semantic_activation_resolve_shared_undo_root_r4_terminal_census(
			admission, intent, owner_instance, segment_id, out);
	return false;
}

static bool
cluster_runtime_visibility_current_owner_shape_valid(
	TransactionId xid, const ClusterTTSlotCurrentOwner *owner)
{
	if (owner == NULL || cluster_node_id < 0
		|| cluster_node_id >= CLUSTER_MAX_NODES
		|| !TransactionIdIsNormal(xid) || owner->xid != xid
		|| owner->segment_id == 0 || owner->segment_id > UINT16_MAX
		|| owner->slot_offset >= TT_SLOTS_PER_SEGMENT
		|| owner->wrap == TT_WRAP_INVALID || owner->reserved8[0] != 0
		|| owner->reserved8[1] != 0 || owner->reserved8[2] != 0)
		return false;
	switch ((ClusterTTSlotAllocStatus)owner->status) {
	case CTS_ACTIVE:
	case CTS_ABORTED:
		return owner->commit_scn == InvalidScn;
	case CTS_COMMITTED:
		return SCN_VALID(owner->commit_scn);
	case CTS_FREE:
		break;
	}
	return false;
}

static bool
cluster_runtime_visibility_current_owner_slot_matches(
	const ClusterTTSlotCurrentOwner *owner, const TTSlot *slot,
	ClusterTxOutcome outcome, SCN commit_scn)
{
	if (owner == NULL || slot == NULL || slot->xid != owner->xid
		|| slot->wrap != owner->wrap)
		return false;
	switch ((ClusterTTSlotAllocStatus)owner->status) {
	case CTS_ACTIVE:
		return outcome == CLUSTER_TX_IN_PROGRESS
			&& (slot->status == TT_SLOT_ACTIVE
				|| (slot->status == TT_SLOT_COMMITTED
					&& SCN_VALID(slot->commit_scn)))
			&& commit_scn == InvalidScn;
	case CTS_COMMITTED:
		return outcome == CLUSTER_TX_COMMITTED
			&& slot->status == TT_SLOT_COMMITTED
			&& SCN_VALID(commit_scn)
			&& commit_scn == slot->commit_scn
			&& commit_scn == owner->commit_scn;
	case CTS_ABORTED:
		return outcome == CLUSTER_TX_ABORTED
			&& slot->status == TT_SLOT_ABORTED
			&& commit_scn == InvalidScn;
	case CTS_FREE:
		break;
	}
	return false;
}

static bool
cluster_runtime_visibility_physical_locator_shape_valid(
	const ClusterTTSlotPhysicalLocator *locator)
{
	return locator != NULL && cluster_node_id >= 0
		&& cluster_node_id < CLUSTER_MAX_NODES
		&& locator->segment_id > 0 && locator->segment_id <= UINT16_MAX
		&& TransactionIdIsNormal(locator->xid)
		&& locator->slot_offset < TT_SLOTS_PER_SEGMENT
		&& locator->wrap != TT_WRAP_INVALID && locator->reserved32 == 0;
}

static bool
cluster_runtime_visibility_physical_locator_owner_matches(
	const ClusterTTSlotPhysicalLocator *locator,
	const ClusterTTSlotCurrentOwner *owner)
{
	return locator != NULL && owner != NULL
		&& locator->segment_id == owner->segment_id
		&& locator->xid == owner->xid
		&& locator->slot_offset == owner->slot_offset
		&& locator->wrap == owner->wrap
		&& cluster_runtime_visibility_current_owner_shape_valid(
			locator->xid, owner);
}

static bool
cluster_runtime_visibility_physical_locator_slot_matches(
	const ClusterTTSlotPhysicalLocator *locator, const TTSlot *slot,
	ClusterTxOutcome outcome, SCN commit_scn)
{
	if (locator == NULL || slot == NULL || slot->xid != locator->xid
		|| slot->wrap != locator->wrap)
		return false;
	switch (outcome) {
	case CLUSTER_TX_IN_PROGRESS:
		return commit_scn == InvalidScn
			&& (slot->status == TT_SLOT_ACTIVE
				|| (slot->status == TT_SLOT_COMMITTED
					&& SCN_VALID(slot->commit_scn)));
	case CLUSTER_TX_COMMITTED:
		return slot->status == TT_SLOT_COMMITTED
			&& SCN_VALID(commit_scn) && commit_scn == slot->commit_scn;
	case CLUSTER_TX_ABORTED:
		return slot->status == TT_SLOT_ABORTED
			&& commit_scn == InvalidScn;
	case CLUSTER_TX_UNKNOWN:
	case CLUSTER_TX_PREPARED:
		break;
	}
	return false;
}

bool
cluster_runtime_visibility_physical_locator_sample_held(
	const ClusterTTSlotPhysicalLocator *locator,
	const ClusterSemanticAdmissionToken *admission,
	ClusterUndoBlock0CurrentGuard *guard,
	const ClusterUndoBlock0ResolvedRoot *root,
	ClusterTTStatusKey *key_out, ClusterTTStatusResult *result_out,
	bool *ctrc_physical_active_out)
{
	ClusterUndoBlock0Generation generation = { false, 0 };
	ClusterUndoBlock0Generation final_generation = { false, 0 };
	ClusterUndoBlock0ResolvedRoot final_root;
	ClusterTTSlotCurrentOwner current_owner;
	ClusterTTSlotCurrentOwner final_owner;
	ClusterTxLocator tx_locator;
	ClusterTxResolveReason reason = CLUSTER_TX_RESOLVE_AUTHORITY_UNAVAILABLE;
	ClusterTxOutcome outcome;
	ClusterTxProofKind proof_kind = CLUSTER_TX_PROOF_NONE;
	TransactionId top_xid = InvalidTransactionId;
	SCN commit_scn = InvalidScn;
	PGAlignedBlock block0;
	const UndoSegmentHeaderData *header;
	TTSlot exact_slot;
	uint32 current_segment;
	uint32 final_current_segment;
	uint64 epoch;
	bool current_allocator_exact;
	bool current_owner_found = false;
	bool final_owner_found = false;

	if (key_out != NULL)
		memset(key_out, 0, sizeof(*key_out));
	if (result_out != NULL) {
		memset(result_out, 0, sizeof(*result_out));
		result_out->status = CLUSTER_TT_STATUS_UNKNOWN;
		result_out->commit_scn = InvalidScn;
	}
	if (ctrc_physical_active_out != NULL)
		*ctrc_physical_active_out = false;
	memset(&final_root, 0, sizeof(final_root));
	memset(&current_owner, 0, sizeof(current_owner));
	memset(&final_owner, 0, sizeof(final_owner));
	memset(&tx_locator, 0, sizeof(tx_locator));
	if (key_out == NULL || result_out == NULL || admission == NULL
		|| guard == NULL || root == NULL
		|| root->intent != CLUSTER_UNDO_PATH_RUNTIME_SHARED
		|| !cluster_runtime_visibility_physical_locator_shape_valid(locator)
		|| !cluster_runtime_visibility_admission_current(
			CLUSTER_TX_RESOLVE_VISIBILITY, admission))
		return false;
	epoch = cluster_epoch_get_current();
	if (epoch > UINT32_MAX || admission->formation_epoch != epoch)
		return false;

	/* The allocator is a locator/reuse corroborator only.  An old physical
	 * segment is admissible when the allocator has stably rolled elsewhere;
	 * if this is the current segment, its exact owner must still match. */
	current_segment = cluster_tt_slot_current_segment(cluster_node_id);
	if (current_segment == 0)
		return false;
	current_allocator_exact = current_segment == locator->segment_id;
	if (current_allocator_exact) {
		current_owner_found = cluster_tt_slot_current_owner_by_xid(
			cluster_node_id, locator->xid, &current_owner);
		if (current_owner_found
			&& !cluster_runtime_visibility_physical_locator_owner_matches(
				locator, &current_owner))
			return false;
	}

	if (cluster_undo_block0_current_sample_generation(
			guard, root, &generation) != CLUSTER_UNDO_BLOCK0_OK
		|| !generation.known || generation.value == UINT32_MAX
		|| cluster_undo_block0_current_copy_resident(
			guard, root, &generation, block0.data) != CLUSTER_UNDO_BLOCK0_OK)
		return false;
	header = (const UndoSegmentHeaderData *)block0.data;
	exact_slot = header->tt_slots[locator->slot_offset];
	tx_locator.xid = locator->xid;
	tx_locator.tt_wrap = locator->wrap;
	outcome = cluster_runtime_visibility_candidate_decide(
		&tx_locator, CLUSTER_TX_RESOLVE_VISIBILITY, &exact_slot, &top_xid,
		&proof_kind, &commit_scn, &reason, NULL);
	if (outcome == CLUSTER_TX_UNKNOWN || outcome == CLUSTER_TX_PREPARED
		|| top_xid != locator->xid
		|| !cluster_tx_outcome_proof_is_valid(outcome, proof_kind)
		|| !cluster_runtime_visibility_physical_locator_slot_matches(
			locator, &exact_slot, outcome, commit_scn)
		|| (current_allocator_exact
			&& ((outcome == CLUSTER_TX_IN_PROGRESS && !current_owner_found)
				|| (current_owner_found
					&& !cluster_runtime_visibility_current_owner_slot_matches(
						&current_owner, &exact_slot, outcome, commit_scn))))
		|| cluster_undo_block0_current_sample_generation(
			guard, root, &final_generation) != CLUSTER_UNDO_BLOCK0_OK
		|| !final_generation.known
		|| final_generation.value != generation.value)
		return false;

	final_current_segment = cluster_tt_slot_current_segment(cluster_node_id);
	if (final_current_segment != current_segment)
		return false;
	if (current_allocator_exact) {
		final_owner_found = cluster_tt_slot_current_owner_by_xid(
			cluster_node_id, locator->xid, &final_owner);
		if (final_owner_found != current_owner_found
			|| (current_owner_found
				&& memcmp(&final_owner, &current_owner, sizeof(final_owner)) != 0))
			return false;
	}
	if (!cluster_runtime_visibility_resolve_root_admitted(
			CLUSTER_TX_RESOLVE_VISIBILITY, admission,
			CLUSTER_UNDO_PATH_RUNTIME_SHARED,
			(uint32)cluster_node_id + 1, locator->segment_id, &final_root)
		|| !cluster_runtime_visibility_candidate_root_matches(root, &final_root)
		|| !cluster_runtime_visibility_admission_current(
			CLUSTER_TX_RESOLVE_VISIBILITY, admission))
		return false;

	key_out->origin_node_id = (uint16)cluster_node_id;
	key_out->undo_segment_id = (uint16)locator->segment_id;
	key_out->tt_slot_id
		= cluster_tt_slot_offset_to_id(locator->slot_offset);
	key_out->cluster_epoch = (uint32)epoch;
	key_out->local_xid = locator->xid;
	result_out->status_epoch = (uint32)epoch;
	result_out->authoritative = true;
	switch (outcome) {
	case CLUSTER_TX_IN_PROGRESS:
		result_out->status = CLUSTER_TT_STATUS_IN_PROGRESS;
		break;
	case CLUSTER_TX_COMMITTED:
		result_out->status = CLUSTER_TT_STATUS_COMMITTED;
		result_out->commit_scn = commit_scn;
		break;
	case CLUSTER_TX_ABORTED:
		result_out->status = CLUSTER_TT_STATUS_ABORTED;
		break;
	case CLUSTER_TX_UNKNOWN:
	case CLUSTER_TX_PREPARED:
		return false;
	}
	if (ctrc_physical_active_out != NULL)
		*ctrc_physical_active_out = exact_slot.xid == locator->xid
			&& exact_slot.wrap == locator->wrap
			&& exact_slot.status == TT_SLOT_ACTIVE
			&& exact_slot.commit_scn == InvalidScn;
	return true;
}

static bool
cluster_runtime_visibility_current_owner_from_local_binding(
	TransactionId xid, const ClusterCanonicalTxnBinding *binding,
	ClusterTTSlotCurrentOwner *owner)
{
	if (binding == NULL || owner == NULL || cluster_node_id < 0
		|| cluster_node_id >= CLUSTER_MAX_NODES
		|| binding->segment_id == 0 || binding->segment_id > UINT16_MAX
		|| binding->segment_generation == UINT32_MAX
		|| binding->xid != xid
		|| binding->slot_offset >= TT_SLOTS_PER_SEGMENT
		|| binding->slot_wrap == TT_WRAP_INVALID
		|| binding->origin_instance != (uint8)((uint32)cluster_node_id + 1)
		|| binding->publish_state != CLUSTER_CANONICAL_TXN_PUBLISHED
		|| binding->reserved16 != 0
		|| XLogRecPtrIsInvalid(binding->active_lsn))
		return false;

	memset(owner, 0, sizeof(*owner));
	owner->segment_id = binding->segment_id;
	owner->xid = binding->xid;
	owner->commit_scn = InvalidScn;
	owner->slot_offset = binding->slot_offset;
	owner->wrap = binding->slot_wrap;
	owner->status = CTS_ACTIVE;
	return true;
}

static bool
cluster_runtime_visibility_current_owner_recheck(
	TransactionId xid, const ClusterTTSlotCurrentOwner *expected_owner,
	const ClusterCanonicalTxnBinding *expected_binding)
{
	ClusterTTSlotCurrentOwner final_owner;

	memset(&final_owner, 0, sizeof(final_owner));
	if (expected_binding != NULL)
	{
		ClusterCanonicalTxnBinding final_binding;

		memset(&final_binding, 0, sizeof(final_binding));
		if (!cluster_tt_local_get_published_binding(xid, &final_binding)
			|| final_binding.segment_id != expected_binding->segment_id
			|| final_binding.segment_generation
			   != expected_binding->segment_generation
			|| final_binding.xid != expected_binding->xid
			|| final_binding.slot_offset != expected_binding->slot_offset
			|| final_binding.slot_wrap != expected_binding->slot_wrap
			|| final_binding.origin_instance
			   != expected_binding->origin_instance
			|| final_binding.publish_state != expected_binding->publish_state
			|| final_binding.reserved16 != expected_binding->reserved16
			|| final_binding.active_lsn != expected_binding->active_lsn
			|| !cluster_runtime_visibility_current_owner_from_local_binding(
				xid, &final_binding, &final_owner))
			return false;
	}
	else if (!cluster_tt_slot_current_owner_by_xid(
			 cluster_node_id, xid, &final_owner))
		return false;

	return memcmp(&final_owner, expected_owner, sizeof(final_owner)) == 0;
}

static bool
cluster_runtime_visibility_current_owner_sample_held_internal(
	TransactionId xid, const ClusterTTSlotCurrentOwner *expected_owner,
	const ClusterCanonicalTxnBinding *expected_binding,
	const ClusterSemanticAdmissionToken *admission,
	ClusterUndoBlock0CurrentGuard *guard,
	const ClusterUndoBlock0ResolvedRoot *root,
	ClusterTTStatusKey *key_out, ClusterTTStatusResult *result_out,
	bool *ctrc_physical_active_out)
{
	ClusterUndoBlock0Generation generation;
	ClusterUndoBlock0Generation final_generation;
	ClusterUndoBlock0ResolvedRoot final_root;
	ClusterTxLocator locator;
	ClusterTxResolveReason reason = CLUSTER_TX_RESOLVE_AUTHORITY_UNAVAILABLE;
	ClusterTxOutcome outcome;
	ClusterTxProofKind proof_kind = CLUSTER_TX_PROOF_NONE;
	TransactionId top_xid = InvalidTransactionId;
	SCN commit_scn = InvalidScn;
	PGAlignedBlock block0;
	const UndoSegmentHeaderData *header;
	TTSlot exact_slot;
	uint64 epoch;

	if (key_out != NULL)
		memset(key_out, 0, sizeof(*key_out));
	if (result_out != NULL) {
		memset(result_out, 0, sizeof(*result_out));
		result_out->status = CLUSTER_TT_STATUS_UNKNOWN;
		result_out->commit_scn = InvalidScn;
	}
	if (ctrc_physical_active_out != NULL)
		*ctrc_physical_active_out = false;
	memset(&generation, 0, sizeof(generation));
	memset(&final_generation, 0, sizeof(final_generation));
	memset(&final_root, 0, sizeof(final_root));
	memset(&locator, 0, sizeof(locator));
	if (key_out == NULL || result_out == NULL || admission == NULL
		|| guard == NULL || root == NULL
		|| root->intent != CLUSTER_UNDO_PATH_RUNTIME_SHARED
		|| !cluster_runtime_visibility_current_owner_shape_valid(
			xid, expected_owner)
		|| !cluster_runtime_visibility_admission_current(
			CLUSTER_TX_RESOLVE_VISIBILITY, admission))
		return false;
	epoch = cluster_epoch_get_current();
	if (epoch > UINT32_MAX || admission->formation_epoch != epoch)
		return false;
	if (cluster_undo_block0_current_sample_generation(
			guard, root, &generation) != CLUSTER_UNDO_BLOCK0_OK
		|| !generation.known || generation.value == UINT32_MAX
		|| (expected_binding != NULL
			&& generation.value != expected_binding->segment_generation)
		|| cluster_undo_block0_current_copy_resident(
			guard, root, &generation, block0.data) != CLUSTER_UNDO_BLOCK0_OK)
		return false;
	header = (const UndoSegmentHeaderData *)block0.data;
	exact_slot = header->tt_slots[expected_owner->slot_offset];
	locator.xid = xid;
	locator.tt_wrap = expected_owner->wrap;
	outcome = cluster_runtime_visibility_candidate_decide(
		&locator, CLUSTER_TX_RESOLVE_VISIBILITY, &exact_slot, &top_xid,
		&proof_kind, &commit_scn, &reason, NULL);
	if (outcome == CLUSTER_TX_UNKNOWN || outcome == CLUSTER_TX_PREPARED
		|| top_xid != xid
		|| !cluster_tx_outcome_proof_is_valid(outcome, proof_kind)
		|| !cluster_runtime_visibility_current_owner_slot_matches(
			expected_owner, &exact_slot, outcome, commit_scn)
		|| cluster_undo_block0_current_sample_generation(
			guard, root, &final_generation) != CLUSTER_UNDO_BLOCK0_OK
		|| !final_generation.known
		|| final_generation.value != generation.value
		|| !cluster_runtime_visibility_current_owner_recheck(
			xid, expected_owner, expected_binding)
		|| !cluster_runtime_visibility_resolve_root_admitted(
			CLUSTER_TX_RESOLVE_VISIBILITY, admission,
			CLUSTER_UNDO_PATH_RUNTIME_SHARED,
			(uint32)cluster_node_id + 1, expected_owner->segment_id,
			&final_root)
		|| !cluster_runtime_visibility_candidate_root_matches(root, &final_root)
		|| !cluster_runtime_visibility_admission_current(
			CLUSTER_TX_RESOLVE_VISIBILITY, admission))
		return false;

	key_out->origin_node_id = (uint16)cluster_node_id;
	key_out->undo_segment_id = (uint16)expected_owner->segment_id;
	key_out->tt_slot_id
		= cluster_tt_slot_offset_to_id(expected_owner->slot_offset);
	key_out->cluster_epoch = (uint32)epoch;
	key_out->local_xid = xid;
	result_out->status_epoch = (uint32)epoch;
	result_out->authoritative = true;
	switch (outcome) {
	case CLUSTER_TX_IN_PROGRESS:
		result_out->status = CLUSTER_TT_STATUS_IN_PROGRESS;
		break;
	case CLUSTER_TX_COMMITTED:
		result_out->status = CLUSTER_TT_STATUS_COMMITTED;
		result_out->commit_scn = commit_scn;
		break;
	case CLUSTER_TX_ABORTED:
		result_out->status = CLUSTER_TT_STATUS_ABORTED;
		break;
	case CLUSTER_TX_UNKNOWN:
	case CLUSTER_TX_PREPARED:
		return false;
	}
	if (ctrc_physical_active_out != NULL)
		*ctrc_physical_active_out = exact_slot.xid == xid
			&& exact_slot.wrap == expected_owner->wrap
			&& exact_slot.status == TT_SLOT_ACTIVE
			&& exact_slot.commit_scn == InvalidScn;
	return true;
}

bool
cluster_runtime_visibility_current_owner_sample_held(
	TransactionId xid, const ClusterTTSlotCurrentOwner *expected_owner,
	const ClusterSemanticAdmissionToken *admission,
	ClusterUndoBlock0CurrentGuard *guard,
	const ClusterUndoBlock0ResolvedRoot *root,
	ClusterTTStatusKey *key_out, ClusterTTStatusResult *result_out,
	bool *ctrc_physical_active_out)
{
	return cluster_runtime_visibility_current_owner_sample_held_internal(
		xid, expected_owner, NULL, admission, guard, root, key_out, result_out,
		ctrc_physical_active_out);
}

static bool
cluster_runtime_visibility_ctrc_key_fill(
	const ClusterTTSlotCurrentOwner *owner,
	const ClusterUndoBlock0Generation *generation,
	const ClusterUndoBlock0ResolvedRoot *root,
	const ClusterSemanticAdmissionToken *admission,
	ClusterCtrcTxnKeyV1 *key)
{
	uint64 epoch;
	uint64 boot_incarnation;
	uint64 system_identifier;

	if (owner == NULL || generation == NULL || root == NULL
		|| admission == NULL || key == NULL || cluster_node_id < 0
		|| cluster_node_id >= CLUSTER_CTRC_MAX_PARTICIPANTS
		|| !generation->known || generation->value == UINT32_MAX
		|| admission->record_generation == 0
		|| root->root_id == 0 || root->root_generation == 0)
		return false;
	epoch = cluster_epoch_get_current();
	boot_incarnation = cluster_qvotec_get_self_incarnation();
	system_identifier = GetSystemIdentifier();
	if (epoch > UINT32_MAX || boot_incarnation == 0
		|| system_identifier == 0
		|| admission->formation_epoch != epoch)
		return false;

	memset(key, 0, sizeof(*key));
	key->format_version = CLUSTER_CTRC_FORMAT_VERSION;
	key->owner_instance = (uint8)((uint32)cluster_node_id + 1);
	key->origin_node_id = (uint16)cluster_node_id;
	key->segment_id = owner->segment_id;
	key->segment_generation = generation->value;
	key->slot_offset = owner->slot_offset;
	key->slot_wrap = owner->wrap;
	key->xid = owner->xid;
	key->cluster_epoch = (uint32)epoch;
	key->system_identifier = system_identifier;
	key->origin_boot_incarnation = boot_incarnation;
	key->formation_epoch = admission->formation_epoch;
	key->admission_record_generation = admission->record_generation;
	key->root_descriptor_incarnation = root->root_generation;
	key->root_id = root->root_id;
	key->root_generation = root->root_generation;
	return true;
}

static bool
cluster_runtime_visibility_current_owner_lookup_internal(
	TransactionId xid, ClusterTTStatusKey *key_out,
	ClusterTTStatusResult *result_out, bool ctrc_required,
	uint32 *ctrc_grant_out, ClusterCtrcTxnKeyV1 *ctrc_key_out,
	ClusterCtrcParticipantIdentity *participant_out)
{
	ClusterSemanticAdmissionToken admission;
	ClusterTTSlotCurrentOwner owner;
	ClusterUndoBlock0LogicalKey logical;
	ClusterUndoBlock0ResolvedRoot root;
	ClusterUndoBlock0ResolvedRoot final_root;
	ClusterUndoBlock0Generation ctrc_generation = { false, 0 };
	ClusterUndoBlock0Generation final_generation = { false, 0 };
	ClusterUndoBlock0CurrentGuard guard = { 0 };
	ClusterRuntimeCandidateCleanup cleanup = { &guard, false };
	ClusterCtrcTxnKeyV1 ctrc_key;
	ClusterCtrcParticipantIdentity participant;
	ClusterCanonicalTxnBinding local_binding;
	const ClusterCanonicalTxnBinding *expected_binding = NULL;
	ClusterUndoBlock0Result current_result = CLUSTER_UNDO_BLOCK0_AUTHORITY_DENIED;
	bool sampled = false;
	bool ctrc_physical_active = false;
	uint32 grant = 0;

	if (key_out != NULL)
		memset(key_out, 0, sizeof(*key_out));
	if (result_out != NULL) {
		memset(result_out, 0, sizeof(*result_out));
		result_out->status = CLUSTER_TT_STATUS_UNKNOWN;
		result_out->commit_scn = InvalidScn;
	}
	if (ctrc_grant_out != NULL)
		*ctrc_grant_out = 0;
	if (ctrc_key_out != NULL)
		memset(ctrc_key_out, 0, sizeof(*ctrc_key_out));
	if (participant_out != NULL)
		memset(participant_out, 0, sizeof(*participant_out));
	memset(&admission, 0, sizeof(admission));
	memset(&owner, 0, sizeof(owner));
	memset(&logical, 0, sizeof(logical));
	memset(&root, 0, sizeof(root));
	memset(&final_root, 0, sizeof(final_root));
	memset(&ctrc_key, 0, sizeof(ctrc_key));
	memset(&participant, 0, sizeof(participant));
	memset(&local_binding, 0, sizeof(local_binding));
	if (key_out == NULL || result_out == NULL || cluster_node_id < 0
		|| cluster_node_id >= CLUSTER_MAX_NODES
		|| (ctrc_required && ctrc_grant_out == NULL)
		|| cluster_xid_origin_slot(xid) != cluster_node_id
		|| cluster_semantic_activation_enter(
			CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1,
			CLUSTER_SEMANTIC_TARGET_SIDE, &admission)
			!= CLUSTER_SEMANTIC_ADMISSION_OK)
		return false;
	if (cluster_tt_local_get_published_binding(xid, &local_binding))
	{
		if (!cluster_runtime_visibility_current_owner_from_local_binding(
				xid, &local_binding, &owner))
			goto done;
		expected_binding = &local_binding;
	}
	else if (!cluster_tt_slot_current_owner_by_xid(
			 cluster_node_id, xid, &owner)
			 || !cluster_runtime_visibility_current_owner_shape_valid(xid, &owner))
		goto done;
	logical.owner_instance = (uint8)((uint32)cluster_node_id + 1);
	logical.segment_id = owner.segment_id;
	if (!cluster_runtime_visibility_resolve_root_admitted(
			CLUSTER_TX_RESOLVE_VISIBILITY, &admission,
			CLUSTER_UNDO_PATH_RUNTIME_SHARED, logical.owner_instance,
			logical.segment_id, &root)
		|| !cluster_runtime_visibility_admission_current(
			CLUSTER_TX_RESOLVE_VISIBILITY, &admission))
		goto done;

	cluster_runtime_visibility_ensure_exit_hooks();
	PG_ENSURE_ERROR_CLEANUP(cluster_runtime_visibility_candidate_cleanup,
						  PointerGetDatum(&cleanup));
	{
		if (cluster_runtime_visibility_candidate_acquire(
				&logical, &admission, &guard, &cleanup, &current_result)
			== CLUSTER_UNDO_BLOCK0_CURRENT_HELD)
			sampled = cluster_runtime_visibility_current_owner_sample_held_internal(
				xid, &owner, expected_binding, &admission, &guard, &root,
				key_out, result_out, &ctrc_physical_active);
		if (sampled && ctrc_required && !ctrc_physical_active)
			sampled = false;
		if (sampled && ctrc_required
			&& result_out->status == CLUSTER_TT_STATUS_IN_PROGRESS)
		{
			ClusterCtrcTouchResult touch_result;

			if (admission.record_generation > UINT32_MAX
				|| cluster_undo_block0_current_sample_generation(
					&guard, &root, &ctrc_generation) != CLUSTER_UNDO_BLOCK0_OK
				|| !cluster_runtime_visibility_ctrc_key_fill(
					&owner, &ctrc_generation, &root, &admission, &ctrc_key))
				sampled = false;
			else
			{
				participant.node_id = (uint16)cluster_node_id;
				participant.capability_record_generation
					= (uint32)admission.record_generation;
				participant.boot_incarnation
					= ctrc_key.origin_boot_incarnation;
				participant.formation_epoch = admission.formation_epoch;
				participant.admission_record_generation
					= admission.record_generation;
				touch_result = cluster_ctrc_origin_touch_exact(
					&ctrc_key, &participant, CTRC_PROOF_ACTIVE, &grant);
				sampled = touch_result == CLUSTER_CTRC_TOUCH_RECORDED
					|| touch_result == CLUSTER_CTRC_TOUCH_DUPLICATE;
				if (sampled
					&& (grant == 0
						|| cluster_undo_block0_current_sample_generation(
							&guard, &root, &final_generation)
						   != CLUSTER_UNDO_BLOCK0_OK
						|| !final_generation.known
						|| final_generation.value != ctrc_generation.value
						|| !cluster_runtime_visibility_resolve_root_admitted(
							CLUSTER_TX_RESOLVE_VISIBILITY, &admission,
							CLUSTER_UNDO_PATH_RUNTIME_SHARED,
							(uint32)cluster_node_id + 1, owner.segment_id,
							&final_root)
						|| !cluster_runtime_visibility_candidate_root_matches(
							&root, &final_root)
						|| !cluster_runtime_visibility_admission_current(
							CLUSTER_TX_RESOLVE_VISIBILITY, &admission)
						|| cluster_qvotec_get_self_incarnation()
						   != ctrc_key.origin_boot_incarnation))
					sampled = false;
			}
		}
		if (cleanup.active) {
			if (cluster_runtime_visibility_candidate_release(
					&guard, &current_result)
				!= CLUSTER_UNDO_BLOCK0_CURRENT_RELEASED) {
				cluster_undo_block0_current_cancel(&guard);
				sampled = false;
			}
			cleanup.active = false;
		}
	}
	PG_END_ENSURE_ERROR_CLEANUP(cluster_runtime_visibility_candidate_cleanup,
							  PointerGetDatum(&cleanup));

done:
	cluster_semantic_activation_leave(&admission);
	if (!sampled) {
		memset(key_out, 0, sizeof(*key_out));
		memset(result_out, 0, sizeof(*result_out));
		result_out->status = CLUSTER_TT_STATUS_UNKNOWN;
		result_out->commit_scn = InvalidScn;
		grant = 0;
	}
	if (ctrc_grant_out != NULL)
		*ctrc_grant_out = grant;
	if (sampled && ctrc_required)
	{
		if (ctrc_key_out != NULL)
			*ctrc_key_out = ctrc_key;
		if (participant_out != NULL)
			*participant_out = participant;
	}
	return sampled;
}

bool
cluster_runtime_visibility_current_owner_lookup_exact(
	TransactionId xid, ClusterTTStatusKey *key_out,
	ClusterTTStatusResult *result_out)
{
	return cluster_runtime_visibility_current_owner_lookup_internal(
		xid, key_out, result_out, false, NULL, NULL, NULL);
}

bool
cluster_runtime_visibility_current_owner_lookup_exact_ctrc(
	TransactionId xid, ClusterTTStatusKey *key_out,
	ClusterTTStatusResult *result_out, uint32 *ctrc_grant_out)
{
	return cluster_runtime_visibility_current_owner_lookup_internal(
		xid, key_out, result_out, true, ctrc_grant_out, NULL, NULL);
}

bool
cluster_runtime_visibility_current_owner_lookup_exact_ctrc_full(
	TransactionId xid, ClusterTTStatusKey *key_out,
	ClusterTTStatusResult *result_out, uint32 *ctrc_grant_out,
	ClusterCtrcTxnKeyV1 *ctrc_key_out,
	ClusterCtrcParticipantIdentity *participant_out)
{
	if (ctrc_key_out == NULL || participant_out == NULL)
		return false;
	return cluster_runtime_visibility_current_owner_lookup_internal(
		xid, key_out, result_out, true, ctrc_grant_out,
		ctrc_key_out, participant_out);
}


/*
 * Sample a local current-MX terminal member from its exact physical TT slot.
 *
 * The current allocator is only a locator/reuse corroborator.  A terminal
 * owner may already have retired from the current index or may live in a
 * rolled segment, so locate the unique durable physical identity and then
 * apply the same resident block0/generation/root/admission bracket used by
 * the remote origin path.  This function deliberately rejects IN_PROGRESS:
 * positive ACTIVE/SELF must use _current_owner_lookup_exact_ctrc_full(),
 * which records the participant touch and returns a nonzero grant.
 */
bool
cluster_runtime_visibility_local_terminal_lookup_exact(
	TransactionId xid, ClusterTTStatusKey *key_out,
	ClusterTTStatusResult *result_out)
{
	ClusterSemanticAdmissionToken admission;
	ClusterUndoBlock0LogicalKey logical;
	ClusterUndoBlock0ResolvedRoot root;
	ClusterUndoBlock0CurrentGuard guard = { 0 };
	ClusterRuntimeCandidateCleanup cleanup = { &guard, false };
	ClusterUndoBlock0Result current_result
		= CLUSTER_UNDO_BLOCK0_AUTHORITY_DENIED;
	ClusterTTSlotPhysicalLocator locator;
	ClusterTTDurableLocate locate_result;
	uint16 segment_id = 0;
	uint16 slot_offset = 0;
	uint16 slot_wrap = TT_WRAP_INVALID;
	uint64 epoch;
	bool sampled = false;
	bool physical_active = false;

	if (key_out != NULL)
		MemSet(key_out, 0, sizeof(*key_out));
	if (result_out != NULL)
	{
		MemSet(result_out, 0, sizeof(*result_out));
		result_out->status = CLUSTER_TT_STATUS_UNKNOWN;
		result_out->commit_scn = InvalidScn;
	}
	MemSet(&admission, 0, sizeof(admission));
	MemSet(&logical, 0, sizeof(logical));
	MemSet(&root, 0, sizeof(root));
	MemSet(&locator, 0, sizeof(locator));
	if (key_out == NULL || result_out == NULL
		|| cluster_node_id < 0 || cluster_node_id >= CLUSTER_MAX_NODES
		|| !TransactionIdIsNormal(xid)
		|| cluster_xid_origin_slot(xid) != cluster_node_id)
		return false;
	epoch = cluster_epoch_get_current();
	/* Epoch zero is the valid clean-formation epoch.  The exact TARGET
	 * admission token below, not a nonzero sentinel test, fences this sample
	 * against activation drift. */
	if (epoch > UINT32_MAX)
		return false;
	locate_result = cluster_tt_slot_durable_locate_any_by_xid_origin(
		cluster_node_id, xid, &segment_id, &slot_offset, &slot_wrap, NULL);
	if (locate_result != CLUSTER_TT_DURABLE_LOCATE_FOUND
		|| segment_id == 0 || slot_offset >= TT_SLOTS_PER_SEGMENT
		|| slot_wrap == TT_WRAP_INVALID)
		return false;
	locator.segment_id = segment_id;
	locator.xid = xid;
	locator.slot_offset = slot_offset;
	locator.wrap = slot_wrap;

	if (cluster_semantic_activation_enter(
			CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1,
			CLUSTER_SEMANTIC_TARGET_SIDE, &admission)
		!= CLUSTER_SEMANTIC_ADMISSION_OK
		|| admission.formation_epoch != epoch)
		goto done;
	logical.owner_instance = (uint8)((uint32)cluster_node_id + 1);
	logical.segment_id = segment_id;
	if (!cluster_runtime_visibility_resolve_root_admitted(
			CLUSTER_TX_RESOLVE_VISIBILITY, &admission,
			CLUSTER_UNDO_PATH_RUNTIME_SHARED, logical.owner_instance,
			logical.segment_id, &root)
		|| !cluster_runtime_visibility_admission_current(
			CLUSTER_TX_RESOLVE_VISIBILITY, &admission))
		goto done;

	cluster_runtime_visibility_ensure_exit_hooks();
	PG_ENSURE_ERROR_CLEANUP(cluster_runtime_visibility_candidate_cleanup,
						  PointerGetDatum(&cleanup));
	{
		if (cluster_runtime_visibility_candidate_acquire(
				&logical, &admission, &guard, &cleanup, &current_result)
			== CLUSTER_UNDO_BLOCK0_CURRENT_HELD)
			sampled = cluster_runtime_visibility_physical_locator_sample_held(
				&locator, &admission, &guard, &root, key_out, result_out,
				&physical_active);
		if (sampled
			&& (physical_active
				|| (result_out->status != CLUSTER_TT_STATUS_COMMITTED
					&& result_out->status != CLUSTER_TT_STATUS_ABORTED)))
			sampled = false;
		if (cleanup.active)
		{
			if (cluster_runtime_visibility_candidate_release(
					&guard, &current_result)
				!= CLUSTER_UNDO_BLOCK0_CURRENT_RELEASED)
			{
				cluster_undo_block0_current_cancel(&guard);
				sampled = false;
			}
			cleanup.active = false;
		}
	}
	PG_END_ENSURE_ERROR_CLEANUP(cluster_runtime_visibility_candidate_cleanup,
							  PointerGetDatum(&cleanup));
	if (sampled
		&& (cluster_epoch_get_current() != epoch
			|| !cluster_runtime_visibility_admission_current(
				CLUSTER_TX_RESOLVE_VISIBILITY, &admission)))
		sampled = false;

done:
	if (admission.entered)
		cluster_semantic_activation_leave(&admission);
	if (!sampled)
	{
		MemSet(key_out, 0, sizeof(*key_out));
		MemSet(result_out, 0, sizeof(*result_out));
		result_out->status = CLUSTER_TT_STATUS_UNKNOWN;
		result_out->commit_scn = InvalidScn;
	}
	return sampled;
}

bool
cluster_runtime_visibility_active_proof_ctrc_identity_exact(
	const ClusterCurrentMemberProofKey *proof_key, uint32 ctrc_grant,
	uint32 requester_capability_generation,
	ClusterCtrcTxnKeyV1 *ctrc_key_out,
	ClusterCtrcParticipantIdentity *participant_out)
{
	ClusterSemanticAdmissionToken admission;
	ClusterUndoBlock0LogicalKey logical;
	ClusterUndoBlock0ResolvedRoot root;
	ClusterUndoBlock0ResolvedRoot final_root;
	ClusterCtrcTxnKeyV1 key;
	ClusterCtrcParticipantIdentity participant;
	uint16 segment_id = 0;
	uint16 slot_offset = 0;
	uint64 origin_boot = 0;
	uint64 observed_generation = 0;
	uint64 participant_boot;
	uint64 system_identifier;
	uint64 epoch;
	bool entered = false;
	bool valid = false;

	if (ctrc_key_out != NULL)
		memset(ctrc_key_out, 0, sizeof(*ctrc_key_out));
	if (participant_out != NULL)
		memset(participant_out, 0, sizeof(*participant_out));
	memset(&admission, 0, sizeof(admission));
	memset(&logical, 0, sizeof(logical));
	memset(&root, 0, sizeof(root));
	memset(&final_root, 0, sizeof(final_root));
	memset(&key, 0, sizeof(key));
	memset(&participant, 0, sizeof(participant));
	if (proof_key == NULL || ctrc_key_out == NULL || participant_out == NULL
		|| ctrc_grant == 0 || requester_capability_generation == 0
		|| cluster_node_id < 0
		|| cluster_node_id >= CLUSTER_CTRC_MAX_PARTICIPANTS
		|| proof_key->origin_node_id >= CLUSTER_CTRC_MAX_PARTICIPANTS
		|| proof_key->undo_segment_id == 0 || proof_key->tt_slot_id == 0
		|| !TransactionIdIsNormal(proof_key->local_xid)
		|| proof_key->binding_version
		   != CLUSTER_CURRENT_MEMBER_PROOF_BINDING_VERSION
		|| proof_key->segment_generation == UINT32_MAX
		|| proof_key->slot_wrap > TT_WRAP_MAX
		|| cluster_xid_origin_slot(proof_key->local_xid)
		   != proof_key->origin_node_id)
		return false;
	epoch = cluster_epoch_get_current();
	if (epoch > UINT32_MAX || proof_key->cluster_epoch != (uint32)epoch)
		return false;

	/* The member origin sampled canonical block-0 under SCUR and bound its
	 * exact incarnation into this private proof.  The requester rechecks the
	 * surrounding admission/root/membership identities, but never acquires or
	 * installs a foreign live-owner block-0 image. */
	segment_id = proof_key->undo_segment_id;
	if (proof_key->tt_slot_id > TT_SLOTS_PER_SEGMENT)
		return false;
	slot_offset = cluster_tt_slot_id_to_offset(proof_key->tt_slot_id);

	if (cluster_semantic_activation_enter(
			CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1,
			CLUSTER_SEMANTIC_TARGET_SIDE, &admission)
		!= CLUSTER_SEMANTIC_ADMISSION_OK)
		return false;
	entered = true;
	if (admission.record_generation == 0
		|| admission.formation_epoch != epoch)
		goto done;
	logical.owner_instance = (uint8)(proof_key->origin_node_id + 1);
	logical.segment_id = segment_id;
	if (!cluster_runtime_visibility_resolve_root_admitted(
			CLUSTER_TX_RESOLVE_VISIBILITY, &admission,
			CLUSTER_UNDO_PATH_RUNTIME_SHARED, logical.owner_instance,
			logical.segment_id, &root))
		goto done;

	participant_boot = cluster_qvotec_get_self_incarnation();
	system_identifier = GetSystemIdentifier();
	if (proof_key->origin_node_id == (uint16)cluster_node_id)
		origin_boot = participant_boot;
	else if (!cluster_reconfig_get_observed_slot(proof_key->origin_node_id,
			&origin_boot, &observed_generation)
		|| origin_boot == 0 || observed_generation == 0
		|| cluster_reconfig_get_observed_epoch(proof_key->origin_node_id)
		   != epoch
		|| cluster_membership_get_last_admitted_incarnation(
			proof_key->origin_node_id) != origin_boot)
		goto done;
	if (participant_boot == 0 || origin_boot == 0 || system_identifier == 0)
		goto done;

	if (root.root_id == 0 || root.root_generation == 0)
		goto done;
	key.format_version = CLUSTER_CTRC_FORMAT_VERSION;
	key.owner_instance = logical.owner_instance;
	key.origin_node_id = proof_key->origin_node_id;
	key.segment_id = segment_id;
	key.segment_generation = proof_key->segment_generation;
	key.slot_offset = slot_offset;
	key.slot_wrap = proof_key->slot_wrap;
	key.xid = proof_key->local_xid;
	key.cluster_epoch = (uint32)epoch;
	key.system_identifier = system_identifier;
	key.origin_boot_incarnation = origin_boot;
	key.formation_epoch = admission.formation_epoch;
	key.admission_record_generation = admission.record_generation;
	key.root_descriptor_incarnation = root.root_generation;
	key.root_id = root.root_id;
	key.root_generation = root.root_generation;
	participant.node_id = (uint16)cluster_node_id;
	participant.capability_record_generation
		= requester_capability_generation;
	participant.boot_incarnation = participant_boot;
	participant.formation_epoch = admission.formation_epoch;
	participant.admission_record_generation
		= admission.record_generation;
	if (!cluster_runtime_visibility_resolve_root_admitted(
			CLUSTER_TX_RESOLVE_VISIBILITY, &admission,
			CLUSTER_UNDO_PATH_RUNTIME_SHARED, logical.owner_instance,
			logical.segment_id, &final_root)
		|| !cluster_runtime_visibility_candidate_root_matches(
			&root, &final_root)
		|| cluster_epoch_get_current() != epoch
		|| cluster_qvotec_get_self_incarnation() != participant_boot
		|| !cluster_runtime_visibility_admission_current(
			CLUSTER_TX_RESOLVE_VISIBILITY, &admission))
		goto done;
	valid = true;
	if (proof_key->origin_node_id != (uint16)cluster_node_id)
	{
		uint64 final_origin_boot = 0;
		uint64 final_observed_generation = 0;

		if (!cluster_reconfig_get_observed_slot(proof_key->origin_node_id,
				&final_origin_boot, &final_observed_generation)
			|| final_origin_boot != origin_boot
			|| final_observed_generation != observed_generation
			|| cluster_reconfig_get_observed_epoch(proof_key->origin_node_id)
			   != epoch
			|| cluster_membership_get_last_admitted_incarnation(
				proof_key->origin_node_id) != origin_boot)
			valid = false;
	}

done:
	if (entered)
		cluster_semantic_activation_leave(&admission);
	if (!valid)
		return false;
	*ctrc_key_out = key;
	*participant_out = participant;
	return true;
}

#define CLUSTER_RUNTIME_VISIBILITY_ORIGIN_PLAN_MAGIC UINT32_C(0x52564f50)

typedef struct ClusterRuntimeVisibilityOriginPlanData {
	uint32 magic;
	bool valid;
	bool candidate_valid;
	bool candidate_recycled_abort;
	ClusterTxResolveMode mode;
	ClusterTxLocator locator;
	ClusterTxLocator canonical_locator;
	ClusterUndoBlock0LogicalKey data_logical;
	ClusterUndoBlock0LogicalKey tt_logical;
	ClusterUndoBlock0ResolvedRoot data_root;
	ClusterUndoBlock0Generation data_generation;
	uint32 data_block_no;
	uint16 tt_slot_offset;
	size_t record_length;
	ClusterRuntimeVisibilityCanonicalDiagnostic canonical_diagnostic;
	ClusterTxResolution candidate;
	PGAlignedBlock record_buf;
} ClusterRuntimeVisibilityOriginPlanData;

StaticAssertDecl(sizeof(ClusterRuntimeVisibilityOriginPlanData)
				 <= CLUSTER_RUNTIME_VISIBILITY_ORIGIN_PLAN_BYTES,
				 "exact origin continuation must fit its process-local envelope");

static ClusterRuntimeVisibilityOriginPlanData *
cluster_runtime_visibility_origin_plan_data(
	ClusterRuntimeVisibilityOriginPlan *plan)
{
	return plan == NULL
		? NULL : (ClusterRuntimeVisibilityOriginPlanData *)plan->opaque;
}

static const ClusterRuntimeVisibilityOriginPlanData *
cluster_runtime_visibility_origin_plan_data_const(
	const ClusterRuntimeVisibilityOriginPlan *plan)
{
	return plan == NULL
		? NULL : (const ClusterRuntimeVisibilityOriginPlanData *)plan->opaque;
}

static void
cluster_runtime_visibility_origin_candidate_stamp(
	ClusterTxResolution *candidate,
	const ClusterSemanticAdmissionToken *admission)
{
	candidate->authority.origin_epoch = cluster_epoch_get_current();
	candidate->authority.live_hwm_lsn = GetFlushRecPtr(NULL);
	candidate->authority.tt_generation
		= cluster_undo_tt_retention_rollover_count();
	candidate->authority.authority_scn = cluster_scn_current();
	(void)admission;
}

/* Only the origin's literal, untruncated ABORTED byte is admitted. The
 * existing drain prevents raw-xid reuse through this proof's selection;
 * epoch zero and the latched own stripe exclude adopted/foreign aliases.
 * Never infer an outcome from a missing ProcArray entry or recursive CLOG. */
static bool
cluster_runtime_visibility_origin_recycled_abort_sample(
	const ClusterTxLocator *locator, ClusterRuntimeVisibilityCanonicalDiagnostic *diagnostic)
{
	volatile bool native_locked = false;
	volatile bool xact_locked = false;
	volatile bool aborted = false;

	if (cluster_node_id < 0 || uba_origin_node_id(locator->uba) != (NodeId)cluster_node_id)
		return false;
	PG_TRY();
	{
		cluster_cr_native_prehistory_reader_lock();
		native_locked = true;
		if (cluster_cr_native_origin_epoch0_provable(locator->xid)) {
			LWLockAcquire(XactTruncationLock, LW_SHARED);
			xact_locked = true;
			if (!TransactionIdPrecedes(locator->xid, ShmemVariableCache->oldestClogXid)) {
				XidStatus status = cluster_runtime_visibility_direct_xid_status(locator->xid);

				if (diagnostic != NULL) {
					diagnostic->native_sampled = true;
					diagnostic->native_status = status;
				}
				aborted = status == TRANSACTION_STATUS_ABORTED;
			}
			LWLockRelease(XactTruncationLock);
			xact_locked = false;
			aborted = aborted && cluster_cr_native_origin_epoch0_provable(locator->xid);
		}
	}
	PG_FINALLY();
	{
		if (xact_locked)
			LWLockRelease(XactTruncationLock);
		if (native_locked)
			cluster_cr_native_prehistory_reader_unlock();
	}
	PG_END_TRY();
	return aborted;
}

/* This is a terminal-only extension of the origin DATA/physical-TT proof,
 * not a replacement for candidate_decide's exact identity guard. Whole-
 * segment rebirth clears slots and restarts their wraps. Neither an empty
 * slot nor a newer occupant proves the old transaction's status: the old
 * ABORT must be proved independently at its origin. */
static bool
cluster_runtime_visibility_origin_recycled_abort_candidate(
	const ClusterTxLocator *locator, ClusterTxResolveMode mode, const TTSlot *slot,
	uint32 sampled_generation,
	ClusterRuntimeVisibilityCanonicalDiagnostic *diagnostic)
{
	if (mode != CLUSTER_TX_RESOLVE_VISIBILITY || !TransactionIdIsNormal(locator->xid)
		|| locator->tt_wrap > TT_WRAP_MAX)
		return false;
	if (slot->status == TT_SLOT_UNUSED) {
		const TTSlot empty = {0};

		if (sampled_generation == 0 || sampled_generation == UINT32_MAX
			|| memcmp(slot, &empty, sizeof(empty)) != 0)
			return false;
		return cluster_runtime_visibility_origin_recycled_abort_sample(locator, diagnostic);
	}
	if (!TransactionIdIsNormal(slot->xid) || slot->xid <= locator->xid
		|| slot->xid % CLUSTER_XID_STRIDE != locator->xid % CLUSTER_XID_STRIDE
		|| slot->wrap > TT_WRAP_MAX || slot->status < TT_SLOT_ACTIVE
		|| slot->status > TT_SLOT_RECYCLABLE
		|| (slot->status == TT_SLOT_COMMITTED ? !SCN_VALID(slot->commit_scn)
											  : slot->commit_scn != InvalidScn))
		return false;
	if (slot->wrap <= locator->tt_wrap
		&& (sampled_generation == 0 || sampled_generation == UINT32_MAX))
		return false;
	return cluster_runtime_visibility_origin_recycled_abort_sample(locator, diagnostic);
}

/* The held DATA SCUR and the caller's generation checks bracket this read.
 * A synchronous local consumer may fill cold DATA; a split LMS callback
 * cannot. No pool pin/content lock survives into the canonical TT phase. */
static bool
cluster_runtime_visibility_origin_data_copy(uint32 segment_id, uint8 owner, uint32 block_no,
											char data_out[BLCKSZ], bool allow_readthrough)
{
	ClusterUndoBufPin pin;
	char *page;

	if (cluster_undo_buf_copy_resident(segment_id, owner, block_no, data_out))
		return true;
	if (!allow_readthrough)
		return false;
	page = cluster_undo_buf_pin(segment_id, owner, block_no, CLUSTER_UNDO_BUF_SHARED, &pin);
	if (page == NULL)
		return false;
	memcpy(data_out, page, BLCKSZ);
	cluster_undo_buf_unpin(&pin);
	return true;
}

static ClusterRuntimeVisibilityOriginStep
cluster_runtime_visibility_origin_plan_freeze_data_internal(
	const ClusterTxLocator *locator, ClusterTxResolveMode mode,
	const ClusterSemanticAdmissionToken *admission,
	const ClusterUndoBlock0Generation *expected_generation, ClusterUndoBlock0CurrentGuard *guard,
	const ClusterUndoBlock0ResolvedRoot *root, ClusterRuntimeVisibilityOriginPlan *plan,
	ClusterTxResolution *out, ClusterTxResolveReason *reason_out, bool allow_readthrough)
{
	ClusterRuntimeVisibilityOriginPlanData *plan_data;
	ClusterUndoBlock0ResolvedRoot final_root;
	ClusterUndoBlock0Generation generation;
	ClusterUndoBlock0Generation final_generation;
	ClusterTxLocator canonical_locator;
	ClusterTxResolveReason reason = CLUSTER_TX_RESOLVE_AUTHORITY_UNAVAILABLE;
	ClusterTxOutcome outcome;
	ClusterTxProofKind proof_kind = CLUSTER_TX_PROOF_ORIGIN_DURABLE_TT_CLOG;
	PGAlignedBlock block0;
	PGAlignedBlock data_page;
	PGAlignedBlock record_buf;
	const UndoSegmentHeaderData *header;
	const UndoRecordHeader *record;
	TTSlot exact_slot;
	TransactionId top_xid = InvalidTransactionId;
	SCN commit_scn = InvalidScn;
	NodeId origin = InvalidNodeId;
	uint32 segment_id = 0;
	uint32 block_no = 0;
	uint16 tt_slot_offset = 0;
	uint16 row_offset = 0;
	size_t record_length = 0;
	uint32 tt_segment_id = 0;

	if (out != NULL)
		memset(out, 0, sizeof(*out));
	if (plan != NULL)
		memset(plan, 0, sizeof(*plan));
	if (reason_out != NULL)
		*reason_out = reason;
	memset(&final_root, 0, sizeof(final_root));
	memset(&generation, 0, sizeof(generation));
	memset(&final_generation, 0, sizeof(final_generation));
	memset(&canonical_locator, 0, sizeof(canonical_locator));

	if (locator == NULL || out == NULL || admission == NULL || guard == NULL
		|| root == NULL || plan == NULL
		|| (mode != CLUSTER_TX_RESOLVE_VISIBILITY
			&& mode != CLUSTER_TX_RESOLVE_TERMINAL_CENSUS)) {
		reason = locator == NULL ? CLUSTER_TX_RESOLVE_BAD_LOCATOR
								 : CLUSTER_TX_RESOLVE_PROTOCOL;
		goto failed;
	}
	if (!cluster_runtime_visibility_admission_current(mode, admission)) {
		reason = CLUSTER_TX_RESOLVE_AUTHORITY_STALE;
		goto failed;
	}
	if (!uba_decode(locator->uba, &segment_id, &block_no, &tt_slot_offset,
					&row_offset) || block_no == 0) {
		reason = CLUSTER_TX_RESOLVE_BAD_UBA;
		goto failed;
	}
	origin = uba_origin_node_id(locator->uba);
	if (origin == InvalidNodeId) {
		reason = CLUSTER_TX_RESOLVE_BAD_UBA;
		goto failed;
	}
	if (cluster_node_id < 0 || origin != (NodeId)cluster_node_id)
		goto failed;

	plan_data = cluster_runtime_visibility_origin_plan_data(plan);
	plan_data->data_logical.owner_instance
		= (uint8)((uint32)origin + 1);
	plan_data->data_logical.segment_id = segment_id;
	if (cluster_undo_block0_current_sample_generation(
			guard, root, &generation) != CLUSTER_UNDO_BLOCK0_OK
		|| !generation.known || generation.value == UINT32_MAX)
		goto failed;
	if (expected_generation != NULL
		&& (!expected_generation->known
			|| expected_generation->value == UINT32_MAX
			|| expected_generation->value != generation.value))
		goto failed;
	if (cluster_undo_block0_current_copy_resident(
			guard, root, &generation, block0.data) != CLUSTER_UNDO_BLOCK0_OK)
		goto failed;
	if (!cluster_runtime_visibility_origin_data_copy(segment_id,
													 plan_data->data_logical.owner_instance,
													 block_no, data_page.data, allow_readthrough))
		goto failed;
	if (!cluster_cr_r4_extract_resident_record(
			data_page.data, locator, record_buf.data, &record_length,
			&canonical_locator)
		|| record_length < sizeof(UndoRecordHeader))
		goto failed;

	record = (const UndoRecordHeader *)record_buf.data;
	tt_segment_id = record->tt_slot_segment_id;
	if (tt_segment_id == 0 || tt_segment_id > UINT16_MAX
		|| ((tt_segment_id - 1) / CLUSTER_UNDO_SEGS_PER_INSTANCE) + 1
			   != plan_data->data_logical.owner_instance
		|| tt_slot_offset >= TT_SLOTS_PER_SEGMENT)
		goto failed;

	plan_data->magic = CLUSTER_RUNTIME_VISIBILITY_ORIGIN_PLAN_MAGIC;
	plan_data->valid = true;
	plan_data->mode = mode;
	plan_data->locator = *locator;
	plan_data->canonical_locator = canonical_locator;
	plan_data->data_root = *root;
	plan_data->data_generation = generation;
	plan_data->data_block_no = block_no;
	plan_data->tt_slot_offset = tt_slot_offset;
	plan_data->record_length = record_length;
	memcpy(plan_data->record_buf.data, record_buf.data, record_length);
	plan_data->tt_logical.owner_instance
		= plan_data->data_logical.owner_instance;
	plan_data->tt_logical.segment_id = tt_segment_id;
	if (tt_segment_id != segment_id) {
		if (reason_out != NULL)
			*reason_out = CLUSTER_TX_RESOLVE_NONE;
		return CLUSTER_RUNTIME_VISIBILITY_ORIGIN_NEEDS_CANONICAL;
	}

	header = (const UndoSegmentHeaderData *)block0.data;
	exact_slot = header->tt_slots[tt_slot_offset];
	outcome = cluster_runtime_visibility_candidate_decide(
		&canonical_locator, mode, &exact_slot, &top_xid, &proof_kind,
		&commit_scn, &reason, NULL);
	if (outcome == CLUSTER_TX_UNKNOWN
		&& cluster_runtime_visibility_origin_recycled_abort_candidate(&canonical_locator, mode,
																	  &exact_slot, generation.value, NULL)) {
		outcome = CLUSTER_TX_ABORTED;
		top_xid = canonical_locator.xid;
		proof_kind = CLUSTER_TX_PROOF_ORIGIN_DURABLE_TT_CLOG;
		commit_scn = InvalidScn;
		plan_data->candidate_recycled_abort = true;
	}
	if (outcome == CLUSTER_TX_UNKNOWN)
		goto failed;
	plan_data->candidate.locator_echo = canonical_locator;
	plan_data->candidate.top_xid = top_xid;
	plan_data->candidate.outcome = outcome;
	plan_data->candidate.proof_kind = proof_kind;
	plan_data->candidate.commit_scn = commit_scn;
	cluster_runtime_visibility_origin_candidate_stamp(
		&plan_data->candidate, admission);
	if (plan_data->candidate.authority.origin_epoch
			!= admission->formation_epoch
		|| XLogRecPtrIsInvalid(
			plan_data->candidate.authority.live_hwm_lsn)
		|| !SCN_VALID(plan_data->candidate.authority.authority_scn)
		|| cluster_undo_block0_current_sample_generation(
			guard, root, &final_generation) != CLUSTER_UNDO_BLOCK0_OK
		|| !final_generation.known
		|| final_generation.value != generation.value
		|| !cluster_runtime_visibility_resolve_root_admitted(
			mode, admission, CLUSTER_UNDO_PATH_RUNTIME_SHARED,
			plan_data->data_logical.owner_instance,
			plan_data->data_logical.segment_id, &final_root)
		|| !cluster_runtime_visibility_candidate_root_matches(root, &final_root)
		|| !cluster_runtime_visibility_admission_current(mode, admission)) {
		reason = CLUSTER_TX_RESOLVE_AUTHORITY_STALE;
		goto failed;
	}
	*out = plan_data->candidate;
	plan_data->candidate_valid = true;
	if (reason_out != NULL)
		*reason_out = CLUSTER_TX_RESOLVE_NONE;
	return CLUSTER_RUNTIME_VISIBILITY_ORIGIN_COMPLETE;

failed:
	if (plan != NULL)
		memset(plan, 0, sizeof(*plan));
	if (reason_out != NULL)
		*reason_out = reason;
	return CLUSTER_RUNTIME_VISIBILITY_ORIGIN_FAILED;
}

ClusterRuntimeVisibilityOriginStep
cluster_runtime_visibility_origin_plan_freeze_data_held(
	const ClusterTxLocator *locator, ClusterTxResolveMode mode,
	const ClusterSemanticAdmissionToken *admission,
	const ClusterUndoBlock0Generation *expected_generation, ClusterUndoBlock0CurrentGuard *guard,
	const ClusterUndoBlock0ResolvedRoot *root, ClusterRuntimeVisibilityOriginPlan *plan,
	ClusterTxResolution *out, ClusterTxResolveReason *reason_out)
{
	return cluster_runtime_visibility_origin_plan_freeze_data_internal(
		locator, mode, admission, expected_generation, guard, root, plan, out, reason_out, false);
}

bool
cluster_runtime_visibility_origin_plan_canonical_logical(
	const ClusterRuntimeVisibilityOriginPlan *plan,
	ClusterUndoBlock0LogicalKey *logical_out)
{
	const ClusterRuntimeVisibilityOriginPlanData *plan_data
		= cluster_runtime_visibility_origin_plan_data_const(plan);

	if (plan_data == NULL || logical_out == NULL
		|| plan_data->magic != CLUSTER_RUNTIME_VISIBILITY_ORIGIN_PLAN_MAGIC
		|| !plan_data->valid || plan_data->candidate_valid
		|| plan_data->tt_logical.segment_id
			   == plan_data->data_logical.segment_id)
		return false;
	*logical_out = plan_data->tt_logical;
	return true;
}

bool
cluster_runtime_visibility_origin_plan_canonical_physical(
	const ClusterRuntimeVisibilityOriginPlan *plan,
	ClusterTTSlotPhysicalLocator *locator_out, bool *same_segment_out)
{
	const ClusterRuntimeVisibilityOriginPlanData *plan_data
		= cluster_runtime_visibility_origin_plan_data_const(plan);

	if (locator_out != NULL)
		memset(locator_out, 0, sizeof(*locator_out));
	if (same_segment_out != NULL)
		*same_segment_out = false;
	if (plan_data == NULL || locator_out == NULL || same_segment_out == NULL
		|| plan_data->magic != CLUSTER_RUNTIME_VISIBILITY_ORIGIN_PLAN_MAGIC
		|| !plan_data->valid || plan_data->tt_logical.segment_id == 0
		|| !TransactionIdIsNormal(plan_data->canonical_locator.xid)
		|| plan_data->canonical_locator.tt_wrap == TT_WRAP_INVALID
		|| plan_data->canonical_locator.tt_wrap > TT_WRAP_MAX
		|| plan_data->tt_slot_offset >= TT_SLOTS_PER_SEGMENT)
		return false;

	locator_out->segment_id = plan_data->tt_logical.segment_id;
	locator_out->xid = plan_data->canonical_locator.xid;
	locator_out->slot_offset = plan_data->tt_slot_offset;
	locator_out->wrap = plan_data->canonical_locator.tt_wrap;
	*same_segment_out = plan_data->tt_logical.segment_id
		== plan_data->data_logical.segment_id;
	return true;
}

bool
cluster_runtime_visibility_origin_plan_sample_canonical_held(
	ClusterRuntimeVisibilityOriginPlan *plan, ClusterTxResolveMode mode,
	const ClusterSemanticAdmissionToken *admission,
	ClusterUndoBlock0CurrentGuard *guard,
	const ClusterUndoBlock0ResolvedRoot *root,
	ClusterTxResolveReason *reason_out)
{
	ClusterRuntimeVisibilityOriginPlanData *plan_data
		= cluster_runtime_visibility_origin_plan_data(plan);
	ClusterUndoBlock0ResolvedRoot final_root;
	ClusterUndoBlock0Generation generation;
	ClusterUndoBlock0Generation final_generation;
	ClusterTxResolveReason reason = CLUSTER_TX_RESOLVE_AUTHORITY_UNAVAILABLE;
	ClusterTxOutcome outcome;
	ClusterTxProofKind proof_kind = CLUSTER_TX_PROOF_ORIGIN_DURABLE_TT_CLOG;
	PGAlignedBlock block0;
	const UndoSegmentHeaderData *header;
	TTSlot exact_slot;
	TransactionId top_xid = InvalidTransactionId;
	SCN commit_scn = InvalidScn;
	ClusterUndoBlock0Result generation_result;
	ClusterUndoBlock0Result resident_copy_result;
	bool root_sampled;
	bool admission_current;

	if (reason_out != NULL)
		*reason_out = reason;
	memset(&final_root, 0, sizeof(final_root));
	memset(&generation, 0, sizeof(generation));
	memset(&final_generation, 0, sizeof(final_generation));
	if (plan_data == NULL || admission == NULL || guard == NULL || root == NULL
		|| plan_data->magic != CLUSTER_RUNTIME_VISIBILITY_ORIGIN_PLAN_MAGIC
		|| !plan_data->valid || plan_data->candidate_valid
		|| plan_data->mode != mode
		|| plan_data->tt_logical.segment_id
			   == plan_data->data_logical.segment_id) {
		reason = CLUSTER_TX_RESOLVE_AUTHORITY_STALE;
		goto failed;
	}
	memset(&plan_data->canonical_diagnostic, 0,
		   sizeof(plan_data->canonical_diagnostic));
	plan_data->canonical_diagnostic.valid = true;
	plan_data->canonical_diagnostic.resident_copy_result = -1;
	plan_data->canonical_diagnostic.locator_xid
		= plan_data->canonical_locator.xid;
	plan_data->canonical_diagnostic.locator_wrap
		= plan_data->canonical_locator.tt_wrap;
	plan_data->canonical_diagnostic.tt_slot_offset
		= plan_data->tt_slot_offset;
	plan_data->canonical_diagnostic.root_id = root->root_id;
	plan_data->canonical_diagnostic.root_generation = root->root_generation;
	admission_current
		= cluster_runtime_visibility_admission_current(mode, admission);
	plan_data->canonical_diagnostic.initial_admission_current
		= admission_current;
	if (!admission_current) {
		plan_data->canonical_diagnostic.first_failure
			= CLUSTER_RUNTIME_VISIBILITY_CANONICAL_FAILURE_PLAN_CURRENT;
		reason = CLUSTER_TX_RESOLVE_AUTHORITY_STALE;
		goto failed;
	}
	generation_result = cluster_undo_block0_current_sample_generation(
		guard, root, &generation);
	plan_data->canonical_diagnostic.generation_result = generation_result;
	plan_data->canonical_diagnostic.generation_known = generation.known;
	plan_data->canonical_diagnostic.generation_value = generation.value;
	if (generation_result != CLUSTER_UNDO_BLOCK0_OK) {
		plan_data->canonical_diagnostic.first_failure
			= CLUSTER_RUNTIME_VISIBILITY_CANONICAL_FAILURE_GENERATION_SAMPLE;
		goto failed;
	}
	if (!generation.known) {
		plan_data->canonical_diagnostic.first_failure
			= CLUSTER_RUNTIME_VISIBILITY_CANONICAL_FAILURE_GENERATION_UNKNOWN;
		goto failed;
	}
	if (generation.value == UINT32_MAX) {
		plan_data->canonical_diagnostic.first_failure
			= CLUSTER_RUNTIME_VISIBILITY_CANONICAL_FAILURE_GENERATION_OVERFLOW;
		goto failed;
	}
	resident_copy_result = cluster_undo_block0_current_copy_resident(
		guard, root, &generation, block0.data);
	plan_data->canonical_diagnostic.resident_copy_result
		= resident_copy_result;
	if (resident_copy_result != CLUSTER_UNDO_BLOCK0_OK) {
		plan_data->canonical_diagnostic.first_failure
			= CLUSTER_RUNTIME_VISIBILITY_CANONICAL_FAILURE_RESIDENT_COPY;
		goto failed;
	}
	header = (const UndoSegmentHeaderData *)block0.data;
	exact_slot = header->tt_slots[plan_data->tt_slot_offset];
	outcome = cluster_runtime_visibility_candidate_decide(
		&plan_data->canonical_locator, mode, &exact_slot, &top_xid, &proof_kind,
		&commit_scn, &reason, &plan_data->canonical_diagnostic);
	if (outcome == CLUSTER_TX_UNKNOWN
		&& cluster_runtime_visibility_origin_recycled_abort_candidate(
			&plan_data->canonical_locator, mode, &exact_slot, generation.value,
			&plan_data->canonical_diagnostic)) {
		outcome = CLUSTER_TX_ABORTED;
		top_xid = plan_data->canonical_locator.xid;
		proof_kind = CLUSTER_TX_PROOF_ORIGIN_DURABLE_TT_CLOG;
		commit_scn = InvalidScn;
		plan_data->candidate_recycled_abort = true;
	}
	if (outcome == CLUSTER_TX_UNKNOWN)
		goto failed;
	memset(&plan_data->candidate, 0, sizeof(plan_data->candidate));
	plan_data->candidate.locator_echo = plan_data->canonical_locator;
	plan_data->candidate.top_xid = top_xid;
	plan_data->candidate.outcome = outcome;
	plan_data->candidate.proof_kind = proof_kind;
	plan_data->candidate.commit_scn = commit_scn;
	cluster_runtime_visibility_origin_candidate_stamp(
		&plan_data->candidate, admission);
	if (plan_data->candidate.authority.origin_epoch
			!= admission->formation_epoch
		|| XLogRecPtrIsInvalid(
			plan_data->candidate.authority.live_hwm_lsn)
		|| !SCN_VALID(plan_data->candidate.authority.authority_scn)) {
		plan_data->canonical_diagnostic.first_failure
			= CLUSTER_RUNTIME_VISIBILITY_CANONICAL_FAILURE_AUTHORITY_STAMP;
		reason = CLUSTER_TX_RESOLVE_AUTHORITY_STALE;
		goto failed;
	}
	generation_result = cluster_undo_block0_current_sample_generation(
		guard, root, &final_generation);
	if (generation_result != CLUSTER_UNDO_BLOCK0_OK
		|| !final_generation.known
		|| final_generation.value != generation.value) {
		plan_data->canonical_diagnostic.first_failure
			= CLUSTER_RUNTIME_VISIBILITY_CANONICAL_FAILURE_GENERATION_RECHECK;
		reason = CLUSTER_TX_RESOLVE_AUTHORITY_STALE;
		goto failed;
	}
	root_sampled = cluster_runtime_visibility_resolve_root_admitted(
		mode, admission, CLUSTER_UNDO_PATH_RUNTIME_SHARED,
		plan_data->tt_logical.owner_instance,
		plan_data->tt_logical.segment_id, &final_root);
	plan_data->canonical_diagnostic.final_root_sampled = root_sampled;
	if (root_sampled) {
		plan_data->canonical_diagnostic.final_root_id = final_root.root_id;
		plan_data->canonical_diagnostic.final_root_generation
			= final_root.root_generation;
	}
	if (!root_sampled
		|| !cluster_runtime_visibility_candidate_root_matches(root, &final_root)) {
		plan_data->canonical_diagnostic.first_failure
			= CLUSTER_RUNTIME_VISIBILITY_CANONICAL_FAILURE_ROOT_RECHECK;
		reason = CLUSTER_TX_RESOLVE_AUTHORITY_STALE;
		goto failed;
	}
	admission_current
		= cluster_runtime_visibility_admission_current(mode, admission);
	plan_data->canonical_diagnostic.final_admission_current
		= admission_current;
	if (!admission_current) {
		plan_data->canonical_diagnostic.first_failure
			= CLUSTER_RUNTIME_VISIBILITY_CANONICAL_FAILURE_ADMISSION_RECHECK;
		reason = CLUSTER_TX_RESOLVE_AUTHORITY_STALE;
		goto failed;
	}
	plan_data->canonical_diagnostic.first_failure
		= CLUSTER_RUNTIME_VISIBILITY_CANONICAL_FAILURE_NONE;
	plan_data->candidate_valid = true;
	if (reason_out != NULL)
		*reason_out = CLUSTER_TX_RESOLVE_NONE;
	return true;

failed:
	if (reason_out != NULL)
		*reason_out = reason;
	return false;
}

bool
cluster_runtime_visibility_origin_plan_canonical_diagnostic(
	const ClusterRuntimeVisibilityOriginPlan *plan,
	ClusterRuntimeVisibilityCanonicalDiagnostic *out)
{
	const ClusterRuntimeVisibilityOriginPlanData *plan_data
		= cluster_runtime_visibility_origin_plan_data_const(plan);

	if (out != NULL)
		memset(out, 0, sizeof(*out));
	if (plan_data == NULL || out == NULL
		|| plan_data->magic != CLUSTER_RUNTIME_VISIBILITY_ORIGIN_PLAN_MAGIC
		|| !plan_data->valid || !plan_data->canonical_diagnostic.valid)
		return false;
	*out = plan_data->canonical_diagnostic;
	return true;
}

static ClusterTxOutcome
cluster_runtime_visibility_origin_plan_recheck_data_internal(
	ClusterRuntimeVisibilityOriginPlan *plan, ClusterTxResolveMode mode,
	const ClusterSemanticAdmissionToken *admission, ClusterUndoBlock0CurrentGuard *guard,
	const ClusterUndoBlock0ResolvedRoot *root, ClusterTxResolution *out, char *data_out,
	ClusterTxResolveReason *reason_out, bool allow_readthrough)
{
	ClusterRuntimeVisibilityOriginPlanData *plan_data
		= cluster_runtime_visibility_origin_plan_data(plan);
	ClusterUndoBlock0ResolvedRoot final_root;
	ClusterUndoBlock0Generation generation;
	ClusterUndoBlock0Generation final_generation;
	ClusterTxLocator rechecked_locator;
	ClusterTxResolveReason reason = CLUSTER_TX_RESOLVE_AUTHORITY_UNAVAILABLE;
	PGAlignedBlock data_page;
	PGAlignedBlock record_buf;
	size_t record_length = 0;

	if (out != NULL)
		memset(out, 0, sizeof(*out));
	if (reason_out != NULL)
		*reason_out = reason;
	memset(&final_root, 0, sizeof(final_root));
	memset(&generation, 0, sizeof(generation));
	memset(&final_generation, 0, sizeof(final_generation));
	memset(&rechecked_locator, 0, sizeof(rechecked_locator));
	if (plan_data == NULL || admission == NULL || guard == NULL || root == NULL
		|| out == NULL
		|| plan_data->magic != CLUSTER_RUNTIME_VISIBILITY_ORIGIN_PLAN_MAGIC
		|| !plan_data->valid || !plan_data->candidate_valid
		|| plan_data->mode != mode
		|| !cluster_runtime_visibility_candidate_root_matches(
			&plan_data->data_root, root)
		|| !cluster_runtime_visibility_admission_current(mode, admission)) {
		reason = CLUSTER_TX_RESOLVE_AUTHORITY_STALE;
		goto failed;
	}
	if (cluster_undo_block0_current_sample_generation(guard, root, &generation)
			!= CLUSTER_UNDO_BLOCK0_OK
		|| !generation.known || generation.value != plan_data->data_generation.value
		|| !cluster_runtime_visibility_origin_data_copy(
			plan_data->data_logical.segment_id, plan_data->data_logical.owner_instance,
			plan_data->data_block_no, data_page.data, allow_readthrough)
		|| !cluster_cr_r4_extract_resident_record(data_page.data, &plan_data->locator,
												  record_buf.data, &record_length,
												  &rechecked_locator)
		|| record_length != plan_data->record_length
		|| memcmp(record_buf.data, plan_data->record_buf.data, record_length) != 0
		|| memcmp(&rechecked_locator, &plan_data->canonical_locator, sizeof(rechecked_locator)) != 0
		|| cluster_undo_block0_current_sample_generation(guard, root, &final_generation)
			   != CLUSTER_UNDO_BLOCK0_OK
		|| !final_generation.known || final_generation.value != plan_data->data_generation.value
		|| !cluster_runtime_visibility_resolve_root_admitted(
			mode, admission, CLUSTER_UNDO_PATH_RUNTIME_SHARED,
			plan_data->data_logical.owner_instance, plan_data->data_logical.segment_id, &final_root)
		|| !cluster_runtime_visibility_candidate_root_matches(root, &final_root)
		|| !cluster_runtime_visibility_admission_current(mode, admission)) {
		reason = CLUSTER_TX_RESOLVE_AUTHORITY_STALE;
		goto failed;
	}
	if (plan_data->candidate_recycled_abort
		&& !cluster_runtime_visibility_origin_recycled_abort_sample(
			&plan_data->canonical_locator, &plan_data->canonical_diagnostic))
		goto failed;
	*out = plan_data->candidate;
	if (data_out != NULL)
		memcpy(data_out, data_page.data, BLCKSZ);
	plan_data->valid = false;
	if (reason_out != NULL)
		*reason_out = CLUSTER_TX_RESOLVE_NONE;
	return out->outcome;

failed:
	if (reason_out != NULL)
		*reason_out = reason;
	return CLUSTER_TX_UNKNOWN;
}

ClusterTxOutcome
cluster_runtime_visibility_origin_plan_recheck_data_held(
	ClusterRuntimeVisibilityOriginPlan *plan, ClusterTxResolveMode mode,
	const ClusterSemanticAdmissionToken *admission, ClusterUndoBlock0CurrentGuard *guard,
	const ClusterUndoBlock0ResolvedRoot *root, ClusterTxResolution *out,
	ClusterTxResolveReason *reason_out)
{
	return cluster_runtime_visibility_origin_plan_recheck_data_internal(
		plan, mode, admission, guard, root, out, NULL, reason_out, false);
}

ClusterTxOutcome
cluster_runtime_visibility_origin_plan_copy_data_held(
	ClusterRuntimeVisibilityOriginPlan *plan, ClusterTxResolveMode mode,
	const ClusterSemanticAdmissionToken *admission, ClusterUndoBlock0CurrentGuard *guard,
	const ClusterUndoBlock0ResolvedRoot *root, ClusterTxResolution *out, char data_out[BLCKSZ],
	ClusterTxResolveReason *reason_out)
{
	if (data_out == NULL) {
		if (out != NULL)
			memset(out, 0, sizeof(*out));
		if (reason_out != NULL)
			*reason_out = CLUSTER_TX_RESOLVE_PROTOCOL;
		return CLUSTER_TX_UNKNOWN;
	}
	return cluster_runtime_visibility_origin_plan_recheck_data_internal(
		plan, mode, admission, guard, root, out, data_out, reason_out, false);
}

bool
cluster_runtime_visibility_current_mx_updater_provenance_exact(
	const ClusterTxLocator *locator, TimestampTz deadline,
	ClusterTTStatusKey *key_out, ClusterTTStatusResult *result_out,
	uint32 *ctrc_grant_out,
	uint32 *participant_capability_generation_out,
	ClusterCtrcTxnKeyV1 *ctrc_key_out,
	ClusterTxLocator *canonical_locator_out,
	bool *cross_segment_out)
{
	ClusterSemanticAdmissionToken admission;
	ClusterUndoBlock0LogicalKey data_logical;
	ClusterUndoBlock0LogicalKey tt_logical;
	ClusterUndoBlock0ResolvedRoot data_root;
	ClusterUndoBlock0ResolvedRoot phase_root;
	ClusterUndoBlock0ResolvedRoot final_data_root;
	ClusterUndoBlock0ResolvedRoot final_tt_root;
	ClusterUndoBlock0CurrentGuard guard = { 0 };
	ClusterRuntimeCandidateCleanup cleanup = { &guard, false };
	ClusterRuntimeVisibilityOriginPlan plan;
	ClusterTTSlotPhysicalLocator physical;
	ClusterTTSlotCurrentOwner owner;
	ClusterTTStatusKey sampled_key;
	ClusterTTStatusResult sampled_result;
	ClusterTxResolution resolution;
	ClusterTxResolveReason reason = CLUSTER_TX_RESOLVE_AUTHORITY_UNAVAILABLE;
	ClusterRuntimeVisibilityOriginStep origin_step;
	ClusterUndoBlock0Generation generation = { false, 0 };
	ClusterUndoBlock0Generation final_generation = { false, 0 };
	ClusterUndoBlock0Result current_result
		= CLUSTER_UNDO_BLOCK0_AUTHORITY_DENIED;
	ClusterCtrcTxnKeyV1 ctrc_key;
	ClusterCtrcParticipantIdentity participant;
	ClusterCtrcTouchResult touch_result;
	ClusterTxOutcome outcome = CLUSTER_TX_UNKNOWN;
	NodeId origin;
	uint32 data_segment;
	uint32 block_no;
	uint16 tt_slot_offset;
	uint16 row_offset;
	uint64 epoch;
	uint32 grant = 0;
	bool same_segment = false;
	bool physical_active = false;
	bool sampled = false;
	bool entered = false;

	if (key_out != NULL)
		memset(key_out, 0, sizeof(*key_out));
	if (result_out != NULL) {
		memset(result_out, 0, sizeof(*result_out));
		result_out->status = CLUSTER_TT_STATUS_UNKNOWN;
		result_out->commit_scn = InvalidScn;
	}
	if (ctrc_grant_out != NULL)
		*ctrc_grant_out = 0;
	if (participant_capability_generation_out != NULL)
		*participant_capability_generation_out = 0;
	if (ctrc_key_out != NULL)
		memset(ctrc_key_out, 0, sizeof(*ctrc_key_out));
	if (canonical_locator_out != NULL)
		memset(canonical_locator_out, 0, sizeof(*canonical_locator_out));
	if (cross_segment_out != NULL)
		*cross_segment_out = false;
	memset(&admission, 0, sizeof(admission));
	memset(&data_logical, 0, sizeof(data_logical));
	memset(&tt_logical, 0, sizeof(tt_logical));
	memset(&data_root, 0, sizeof(data_root));
	memset(&phase_root, 0, sizeof(phase_root));
	memset(&final_data_root, 0, sizeof(final_data_root));
	memset(&final_tt_root, 0, sizeof(final_tt_root));
	memset(&plan, 0, sizeof(plan));
	memset(&physical, 0, sizeof(physical));
	memset(&owner, 0, sizeof(owner));
	memset(&sampled_key, 0, sizeof(sampled_key));
	memset(&sampled_result, 0, sizeof(sampled_result));
	sampled_result.status = CLUSTER_TT_STATUS_UNKNOWN;
	sampled_result.commit_scn = InvalidScn;
	memset(&resolution, 0, sizeof(resolution));
	memset(&ctrc_key, 0, sizeof(ctrc_key));
	memset(&participant, 0, sizeof(participant));

	if (locator == NULL || key_out == NULL || result_out == NULL
		|| ctrc_grant_out == NULL
		|| participant_capability_generation_out == NULL
		|| ctrc_key_out == NULL
		|| canonical_locator_out == NULL
		|| cross_segment_out == NULL
		|| cluster_node_id < 0 || cluster_node_id >= CLUSTER_MAX_NODES
		|| !TransactionIdIsNormal(locator->xid)
		|| locator->tt_wrap != TT_WRAP_INVALID
		|| !uba_decode(locator->uba, &data_segment, &block_no,
					   &tt_slot_offset, &row_offset)
		|| block_no == 0 || tt_slot_offset >= TT_SLOTS_PER_SEGMENT
		|| (origin = uba_origin_node_id(locator->uba)) == InvalidNodeId
		|| origin != (NodeId)cluster_node_id
		|| cluster_xid_origin_slot(locator->xid) != cluster_node_id
		|| (deadline != 0 && GetCurrentTimestamp() >= deadline))
		return false;
	epoch = cluster_epoch_get_current();
	if (epoch > UINT32_MAX
		|| cluster_semantic_activation_enter(
			CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1,
			CLUSTER_SEMANTIC_TARGET_SIDE, &admission)
			!= CLUSTER_SEMANTIC_ADMISSION_OK)
		return false;
	entered = true;
	if (admission.formation_epoch != epoch)
		goto done;

	data_logical.owner_instance = (uint8)((uint32)origin + 1);
	data_logical.segment_id = data_segment;
	if (!cluster_runtime_visibility_resolve_root_admitted(
			CLUSTER_TX_RESOLVE_VISIBILITY, &admission,
			CLUSTER_UNDO_PATH_RUNTIME_SHARED, data_logical.owner_instance,
			data_logical.segment_id, &data_root)
		|| !cluster_runtime_visibility_admission_current(
			CLUSTER_TX_RESOLVE_VISIBILITY, &admission))
		goto done;

	cluster_runtime_visibility_ensure_exit_hooks();
	PG_ENSURE_ERROR_CLEANUP(cluster_runtime_visibility_candidate_cleanup,
						  PointerGetDatum(&cleanup));
	{
		if (cluster_runtime_visibility_candidate_acquire_until(
				&data_logical, &admission, &guard, &cleanup,
				&current_result, deadline)
			!= CLUSTER_UNDO_BLOCK0_CURRENT_HELD)
			goto protected_done;
		phase_root = data_root;
		origin_step = cluster_runtime_visibility_origin_plan_freeze_data_held(
			locator, CLUSTER_TX_RESOLVE_VISIBILITY, &admission, NULL,
			&guard, &phase_root, &plan, &resolution, &reason);
		if (origin_step == CLUSTER_RUNTIME_VISIBILITY_ORIGIN_FAILED
			|| !cluster_runtime_visibility_origin_plan_canonical_physical(
				&plan, &physical, &same_segment)
			|| (origin_step == CLUSTER_RUNTIME_VISIBILITY_ORIGIN_COMPLETE
				&& !same_segment)
			|| (origin_step
					== CLUSTER_RUNTIME_VISIBILITY_ORIGIN_NEEDS_CANONICAL
				&& same_segment))
			goto protected_done;

		if (!same_segment) {
			if (cluster_runtime_visibility_candidate_release_until(
					&guard, &current_result, deadline)
				!= CLUSTER_UNDO_BLOCK0_CURRENT_RELEASED)
				goto protected_done;
			cleanup.active = false;
			memset(&guard, 0, sizeof(guard));
			if (!cluster_runtime_visibility_origin_plan_canonical_logical(
					&plan, &tt_logical)
				|| !cluster_runtime_visibility_resolve_root_admitted(
					CLUSTER_TX_RESOLVE_VISIBILITY, &admission,
					CLUSTER_UNDO_PATH_RUNTIME_SHARED,
					tt_logical.owner_instance, tt_logical.segment_id,
					&phase_root)
				|| cluster_runtime_visibility_candidate_acquire_until(
					&tt_logical, &admission, &guard, &cleanup,
					&current_result, deadline)
					!= CLUSTER_UNDO_BLOCK0_CURRENT_HELD
				|| !cluster_runtime_visibility_origin_plan_sample_canonical_held(
					&plan, CLUSTER_TX_RESOLVE_VISIBILITY, &admission, &guard,
					&phase_root, &reason))
				goto protected_done;
		}

		if (!cluster_runtime_visibility_physical_locator_sample_held(
				&physical, &admission, &guard, &phase_root, &sampled_key,
				&sampled_result, &physical_active))
			goto protected_done;
		if (sampled_result.status == CLUSTER_TT_STATUS_IN_PROGRESS) {
			owner.segment_id = physical.segment_id;
			owner.xid = physical.xid;
			owner.slot_offset = physical.slot_offset;
			owner.wrap = physical.wrap;
			if (!physical_active || admission.record_generation > UINT32_MAX
				|| cluster_undo_block0_current_sample_generation(
					&guard, &phase_root, &generation)
					!= CLUSTER_UNDO_BLOCK0_OK
				|| !cluster_runtime_visibility_ctrc_key_fill(
					&owner, &generation, &phase_root, &admission, &ctrc_key))
				goto protected_done;
			participant.node_id = (uint16)cluster_node_id;
			participant.capability_record_generation
				= (uint32)admission.record_generation;
			participant.boot_incarnation = ctrc_key.origin_boot_incarnation;
			participant.formation_epoch = admission.formation_epoch;
			participant.admission_record_generation
				= admission.record_generation;
			touch_result = cluster_ctrc_origin_touch_exact(
				&ctrc_key, &participant, CTRC_PROOF_ACTIVE, &grant);
			if ((touch_result != CLUSTER_CTRC_TOUCH_RECORDED
				 && touch_result != CLUSTER_CTRC_TOUCH_DUPLICATE)
				|| grant == 0
				|| cluster_undo_block0_current_sample_generation(
					&guard, &phase_root, &final_generation)
					!= CLUSTER_UNDO_BLOCK0_OK
				|| !final_generation.known
				|| final_generation.value != generation.value
				|| !cluster_runtime_visibility_resolve_root_admitted(
					CLUSTER_TX_RESOLVE_VISIBILITY, &admission,
					CLUSTER_UNDO_PATH_RUNTIME_SHARED,
					same_segment ? data_logical.owner_instance
								 : tt_logical.owner_instance,
					physical.segment_id, &final_tt_root)
				|| !cluster_runtime_visibility_candidate_root_matches(
					&phase_root, &final_tt_root)
				|| cluster_qvotec_get_self_incarnation()
					!= ctrc_key.origin_boot_incarnation)
				goto protected_done;
		} else if (physical_active
				   || (sampled_result.status != CLUSTER_TT_STATUS_COMMITTED
					   && sampled_result.status != CLUSTER_TT_STATUS_ABORTED))
			goto protected_done;

		if (!same_segment) {
			if (cluster_runtime_visibility_candidate_release_until(
					&guard, &current_result, deadline)
				!= CLUSTER_UNDO_BLOCK0_CURRENT_RELEASED)
				goto protected_done;
			cleanup.active = false;
			memset(&guard, 0, sizeof(guard));
			if (!cluster_runtime_visibility_resolve_root_admitted(
					CLUSTER_TX_RESOLVE_VISIBILITY, &admission,
					CLUSTER_UNDO_PATH_RUNTIME_SHARED,
					data_logical.owner_instance, data_logical.segment_id,
					&final_data_root)
				|| !cluster_runtime_visibility_candidate_root_matches(
					&data_root, &final_data_root)
				|| cluster_runtime_visibility_candidate_acquire_until(
					&data_logical, &admission, &guard, &cleanup,
					&current_result, deadline)
					!= CLUSTER_UNDO_BLOCK0_CURRENT_HELD)
				goto protected_done;
			phase_root = final_data_root;
		}
		outcome = cluster_runtime_visibility_origin_plan_recheck_data_held(
			&plan, CLUSTER_TX_RESOLVE_VISIBILITY, &admission, &guard,
			&phase_root, &resolution, &reason);
		if (outcome == CLUSTER_TX_UNKNOWN
			|| !cluster_tx_locator_reply_matches(locator,
				&resolution.locator_echo)
			|| (outcome == CLUSTER_TX_IN_PROGRESS
				&& sampled_result.status != CLUSTER_TT_STATUS_IN_PROGRESS)
			|| (outcome == CLUSTER_TX_COMMITTED
				&& sampled_result.status != CLUSTER_TT_STATUS_COMMITTED)
			|| (outcome == CLUSTER_TX_ABORTED
				&& sampled_result.status != CLUSTER_TT_STATUS_ABORTED)
			|| (grant != 0
				&& !cluster_ctrc_origin_grant_publishable(
					&ctrc_key, &participant, grant)))
			goto protected_done;
		sampled = true;

protected_done:
		if (cleanup.active) {
			if (cluster_runtime_visibility_candidate_release_until(
					&guard, &current_result, deadline)
				!= CLUSTER_UNDO_BLOCK0_CURRENT_RELEASED) {
				cluster_undo_block0_current_cancel(&guard);
				sampled = false;
			}
			cleanup.active = false;
		}
	}
	PG_END_ENSURE_ERROR_CLEANUP(cluster_runtime_visibility_candidate_cleanup,
							  PointerGetDatum(&cleanup));

	if (sampled
		&& (cluster_epoch_get_current() != epoch
			|| !cluster_runtime_visibility_admission_current(
				CLUSTER_TX_RESOLVE_VISIBILITY, &admission)
			|| (grant != 0
				&& !cluster_ctrc_origin_grant_publishable(
					&ctrc_key, &participant, grant))))
		sampled = false;

done:
	if (entered)
		cluster_semantic_activation_leave(&admission);
	if (!sampled)
		return false;
	*key_out = sampled_key;
	*result_out = sampled_result;
	*ctrc_grant_out = grant;
	*participant_capability_generation_out = grant == 0
		? 0 : participant.capability_record_generation;
	if (grant != 0)
		*ctrc_key_out = ctrc_key;
	*canonical_locator_out = resolution.locator_echo;
	*cross_segment_out = !same_segment;
	return true;
}

ClusterTxOutcome
cluster_runtime_visibility_resolve_exact_origin_held(
	const ClusterTxLocator *locator, ClusterTxResolveMode mode,
	const ClusterSemanticAdmissionToken *admission,
	const ClusterUndoBlock0Generation *expected_generation,
	ClusterUndoBlock0CurrentGuard *guard,
	const ClusterUndoBlock0ResolvedRoot *root, ClusterTxResolution *out,
	ClusterTxResolveReason *reason_out)
{
	ClusterUndoBlock0LogicalKey logical;
	ClusterUndoBlock0LogicalKey tt_logical;
	ClusterUndoBlock0ResolvedRoot final_root;
	ClusterUndoBlock0ResolvedRoot tt_root;
	ClusterRuntimeCandidateCleanup phase_cleanup;
	ClusterRuntimeVisibilityOriginPlan plan;
	ClusterTxResolveReason reason = CLUSTER_TX_RESOLVE_AUTHORITY_UNAVAILABLE;
	ClusterTxOutcome outcome = CLUSTER_TX_UNKNOWN;
	ClusterRuntimeVisibilityOriginStep origin_step;
	ClusterUndoBlock0Result current_result;
	NodeId origin;
	uint32 segment_id;
	uint32 block_no;
	uint16 tt_slot_offset;
	uint16 row_offset;

	if (out != NULL)
		memset(out, 0, sizeof(*out));
	if (reason_out != NULL)
		*reason_out = reason;
	memset(&final_root, 0, sizeof(final_root));
	memset(&tt_root, 0, sizeof(tt_root));
	memset(&plan, 0, sizeof(plan));
	phase_cleanup.guard = guard;
	phase_cleanup.active = false;

	if (locator == NULL || out == NULL || admission == NULL || guard == NULL
		|| root == NULL
		|| (mode != CLUSTER_TX_RESOLVE_VISIBILITY
			&& mode != CLUSTER_TX_RESOLVE_TERMINAL_CENSUS)) {
		reason = locator == NULL ? CLUSTER_TX_RESOLVE_BAD_LOCATOR
								 : CLUSTER_TX_RESOLVE_PROTOCOL;
		goto done;
	}
	if (!cluster_runtime_visibility_admission_current(mode, admission)) {
		reason = CLUSTER_TX_RESOLVE_AUTHORITY_STALE;
		goto done;
	}
	if (!uba_decode(locator->uba, &segment_id, &block_no, &tt_slot_offset,
					&row_offset) || block_no == 0) {
		reason = CLUSTER_TX_RESOLVE_BAD_UBA;
		goto done;
	}
	origin = uba_origin_node_id(locator->uba);
	if (origin == InvalidNodeId) {
		reason = CLUSTER_TX_RESOLVE_BAD_UBA;
		goto done;
	}
	if (cluster_node_id < 0 || origin != (NodeId)cluster_node_id) {
		reason = CLUSTER_TX_RESOLVE_AUTHORITY_UNAVAILABLE;
		goto done;
	}
	logical.owner_instance = (uint8)((uint32)origin + 1);
	logical.segment_id = segment_id;
	origin_step = cluster_runtime_visibility_origin_plan_freeze_data_internal(
		locator, mode, admission, expected_generation, guard, root, &plan, out, &reason, true);
	if (origin_step == CLUSTER_RUNTIME_VISIBILITY_ORIGIN_COMPLETE) {
		if (reason_out != NULL)
			*reason_out = CLUSTER_TX_RESOLVE_NONE;
		return out->outcome;
	}
	if (origin_step != CLUSTER_RUNTIME_VISIBILITY_ORIGIN_NEEDS_CANONICAL)
		goto done;

	/* Synchronous backend callers retain the original wrapper.  The LMS
	 * status-22 event loop uses the split functions directly and never enters
	 * these polling waits. */
	if (cluster_runtime_visibility_candidate_release(guard, &current_result)
		!= CLUSTER_UNDO_BLOCK0_CURRENT_RELEASED)
		goto done;
	memset(guard, 0, sizeof(*guard));
	if (!cluster_runtime_visibility_origin_plan_canonical_logical(
			&plan, &tt_logical)
		|| !cluster_runtime_visibility_resolve_root_admitted(
			mode, admission, CLUSTER_UNDO_PATH_RUNTIME_SHARED,
			tt_logical.owner_instance, tt_logical.segment_id, &tt_root)
		|| !cluster_runtime_visibility_admission_current(mode, admission)
		|| cluster_runtime_visibility_candidate_acquire(
			&tt_logical, admission, guard, &phase_cleanup, &current_result)
			   != CLUSTER_UNDO_BLOCK0_CURRENT_HELD)
		goto done;
	if (!cluster_runtime_visibility_origin_plan_sample_canonical_held(
			&plan, mode, admission, guard, &tt_root, &reason))
		goto done;
	if (cluster_runtime_visibility_candidate_release(guard, &current_result)
		!= CLUSTER_UNDO_BLOCK0_CURRENT_RELEASED)
		goto done;
	phase_cleanup.active = false;
	memset(guard, 0, sizeof(*guard));
	if (!cluster_runtime_visibility_resolve_root_admitted(
			mode, admission, CLUSTER_UNDO_PATH_RUNTIME_SHARED,
			logical.owner_instance, logical.segment_id, &final_root)
		|| !cluster_runtime_visibility_candidate_root_matches(root, &final_root)
		|| cluster_runtime_visibility_candidate_acquire(
			&logical, admission, guard, &phase_cleanup, &current_result)
			   != CLUSTER_UNDO_BLOCK0_CURRENT_HELD)
		goto done;
	outcome = cluster_runtime_visibility_origin_plan_recheck_data_internal(
		&plan, mode, admission, guard, &final_root, out, NULL, &reason, true);
	if (outcome != CLUSTER_TX_UNKNOWN) {
		if (reason_out != NULL)
			*reason_out = CLUSTER_TX_RESOLVE_NONE;
		return outcome;
	}

done:
	if (reason_out != NULL)
		*reason_out = reason;
	return CLUSTER_TX_UNKNOWN;
}

ClusterTxOutcome
cluster_runtime_visibility_resolve_exact_origin_admitted(
	const ClusterTxLocator *locator, ClusterTxResolveMode mode,
	const ClusterSemanticAdmissionToken *admission, ClusterTxResolution *out,
	ClusterTxResolveReason *reason_out)
{
	ClusterUndoBlock0CurrentGuard guard = { 0 };
	ClusterRuntimeCandidateCleanup cleanup = { &guard, false };
	ClusterUndoBlock0LogicalKey logical;
	ClusterUndoBlock0ResolvedRoot root;
	ClusterUndoBlock0Result current_result = CLUSTER_UNDO_BLOCK0_AUTHORITY_DENIED;
	ClusterTxResolveReason reason = CLUSTER_TX_RESOLVE_AUTHORITY_UNAVAILABLE;
	ClusterTxOutcome outcome = CLUSTER_TX_UNKNOWN;
	NodeId origin;
	uint32 segment_id;
	uint32 block_no;
	uint16 tt_slot_offset;
	uint16 row_offset;
	bool held = false;

	if (out != NULL)
		memset(out, 0, sizeof(*out));
	if (reason_out != NULL)
		*reason_out = reason;
	memset(&root, 0, sizeof(root));
	if (locator == NULL || out == NULL || admission == NULL
		|| (mode != CLUSTER_TX_RESOLVE_VISIBILITY
			&& mode != CLUSTER_TX_RESOLVE_TERMINAL_CENSUS)) {
		reason = locator == NULL ? CLUSTER_TX_RESOLVE_BAD_LOCATOR
								 : CLUSTER_TX_RESOLVE_PROTOCOL;
		goto done;
	}
	if (!cluster_runtime_visibility_admission_current(mode, admission)) {
		reason = CLUSTER_TX_RESOLVE_AUTHORITY_STALE;
		goto done;
	}
	if (!uba_decode(locator->uba, &segment_id, &block_no, &tt_slot_offset,
					&row_offset) || block_no == 0) {
		reason = CLUSTER_TX_RESOLVE_BAD_UBA;
		goto done;
	}
	origin = uba_origin_node_id(locator->uba);
	if (origin == InvalidNodeId || cluster_node_id < 0) {
		reason = CLUSTER_TX_RESOLVE_BAD_UBA;
		goto done;
	}
	/* A foreign SCUR grant does not publish a local header image. Ask the
	 * exact origin to select and freeze DATA generation under its own guard;
	 * never transmit a failed local sample's zero as a known generation. */
	if (origin != (NodeId)cluster_node_id) {
		outcome = cluster_gcs_block_r4_tx_resolve_fetch_and_wait(
			(int32)origin, locator, UINT32_MAX, admission->formation_epoch, out, &reason);
		goto recheck;
	}
	logical.owner_instance = (uint8)((uint32)origin + 1);
	logical.segment_id = segment_id;
	if (!cluster_runtime_visibility_resolve_root_admitted(
			mode, admission, CLUSTER_UNDO_PATH_RUNTIME_SHARED,
			logical.owner_instance, logical.segment_id, &root))
		goto done;

	cluster_runtime_visibility_ensure_exit_hooks();
	PG_ENSURE_ERROR_CLEANUP(cluster_runtime_visibility_candidate_cleanup,
							PointerGetDatum(&cleanup));
	{
		if (cluster_runtime_visibility_candidate_acquire(
				&logical, admission, &guard, &cleanup, &current_result)
			!= CLUSTER_UNDO_BLOCK0_CURRENT_HELD)
			goto candidate_done;
		held = true;
		outcome = cluster_runtime_visibility_resolve_exact_origin_held(
			locator, mode, admission, NULL, &guard, &root, out, &reason);

candidate_done:
		if (cleanup.active) {
			if (cluster_runtime_visibility_candidate_release(
					&guard, &current_result)
				!= CLUSTER_UNDO_BLOCK0_CURRENT_RELEASED) {
				cluster_undo_block0_current_cancel(&guard);
				outcome = CLUSTER_TX_UNKNOWN;
				reason = CLUSTER_TX_RESOLVE_AUTHORITY_UNAVAILABLE;
				held = false;
			}
			cleanup.active = false;
		}
	}
	PG_END_ENSURE_ERROR_CLEANUP(cluster_runtime_visibility_candidate_cleanup,
							  PointerGetDatum(&cleanup));

	if (!held)
		goto done;

recheck:
	if (outcome != CLUSTER_TX_UNKNOWN) {
		if (!cluster_runtime_visibility_admission_current(mode, admission)) {
			reason = CLUSTER_TX_RESOLVE_AUTHORITY_STALE;
			goto done;
		}
		if (reason_out != NULL)
			*reason_out = CLUSTER_TX_RESOLVE_NONE;
		return outcome;
	}

done:
	if (out != NULL)
		memset(out, 0, sizeof(*out));
	if (reason_out != NULL)
		*reason_out = reason;
	return CLUSTER_TX_UNKNOWN;
}

/* Retained NEEDS_CLEANOUT terminal proof.  The page supplies the exact
 * physical {origin, segment, slot, xid} and pre-commit SCN; the origin's
 * existing C1b/no-raw-reuse pair is the only fallback when the DATA record
 * is no longer resident.  A bound, live result, mismatched SCN, or any
 * admission drift remains UNKNOWN. */
static ClusterTxOutcome
cluster_runtime_visibility_resolve_terminal_census_retained_remote_exact(
	const ClusterTxLocator *locator, SCN retained_commit_scn,
	const ClusterSemanticAdmissionToken *admission, ClusterTxResolution *out,
	ClusterTxResolveReason *reason_out)
{
	ClusterUndoVerdictResult verdict;
	ClusterTxResolveReason reason = CLUSTER_TX_RESOLVE_AUTHORITY_UNAVAILABLE;
	NodeId origin;
	uint32 segment_id;
	uint32 block_no;
	uint16 tt_slot_offset;
	uint16 row_offset;

	if (out != NULL)
		memset(out, 0, sizeof(*out));
	if (reason_out != NULL)
		*reason_out = reason;
	if (locator == NULL || out == NULL || admission == NULL
		|| locator->itl_kind != ITL_FLAG_NEEDS_CLEANOUT
		|| locator->tt_wrap != TT_WRAP_INVALID
		|| !TransactionIdIsNormal(locator->xid)
		|| !SCN_VALID(retained_commit_scn)
		|| admission->formation_epoch > UINT32_MAX)
	{
		reason = locator == NULL ? CLUSTER_TX_RESOLVE_BAD_LOCATOR
								 : CLUSTER_TX_RESOLVE_PROTOCOL;
		goto failed;
	}
	if (!cluster_runtime_visibility_admission_current(
			CLUSTER_TX_RESOLVE_TERMINAL_CENSUS, admission))
	{
		reason = CLUSTER_TX_RESOLVE_AUTHORITY_STALE;
		goto failed;
	}
	if (!uba_decode(locator->uba, &segment_id, &block_no, &tt_slot_offset,
					&row_offset)
		|| block_no == 0 || segment_id == 0 || segment_id > UINT16_MAX
		|| tt_slot_offset >= TT_SLOTS_PER_SEGMENT)
	{
		reason = CLUSTER_TX_RESOLVE_BAD_UBA;
		goto failed;
	}
	origin = uba_origin_node_id(locator->uba);
	if (origin == InvalidNodeId || cluster_node_id < 0
		|| origin == (NodeId) cluster_node_id)
	{
		reason = CLUSTER_TX_RESOLVE_BAD_UBA;
		goto failed;
	}

	verdict = cluster_undo_verdict_resolve_freshref_c1b_pair(
		(int) origin, segment_id, locator->xid, locator->xid,
		(uint32) tt_slot_offset + 1,
		(uint32) admission->formation_epoch, retained_commit_scn,
		InvalidScn);
	if (verdict.kind != CLUSTER_UNDO_VERDICT_COMMITTED_EXACT
		|| verdict.commit_scn != retained_commit_scn)
		goto failed;
	if (!cluster_runtime_visibility_admission_current(
			CLUSTER_TX_RESOLVE_TERMINAL_CENSUS, admission))
	{
		reason = CLUSTER_TX_RESOLVE_AUTHORITY_STALE;
		goto failed;
	}

	out->locator_echo = *locator;
	out->top_xid = locator->xid;
	out->outcome = CLUSTER_TX_COMMITTED;
	out->proof_kind = CLUSTER_TX_PROOF_ORIGIN_DURABLE_TT_CLOG;
	out->commit_scn = retained_commit_scn;
	cluster_runtime_visibility_origin_candidate_stamp(out, admission);
	if (out->authority.origin_epoch != admission->formation_epoch
		|| XLogRecPtrIsInvalid(out->authority.live_hwm_lsn)
		|| !SCN_VALID(out->authority.authority_scn)
		|| !cluster_runtime_visibility_admission_current(
			CLUSTER_TX_RESOLVE_TERMINAL_CENSUS, admission))
	{
		reason = CLUSTER_TX_RESOLVE_AUTHORITY_STALE;
		goto failed;
	}
	if (reason_out != NULL)
		*reason_out = CLUSTER_TX_RESOLVE_NONE;
	return CLUSTER_TX_COMMITTED;

failed:
	memset(out, 0, sizeof(*out));
	if (reason_out != NULL)
		*reason_out = reason;
	return CLUSTER_TX_UNKNOWN;
}

/*
 * R4 exact durable-origin provider boundary.  The consumer is now wired to
 * this exact-locator surface, but target semantics remain fail-closed until
 * the origin-side exact undo/TT/native-state producer and its V3 remote arm
 * land.  The legacy xid-scan verdict path is intentionally not used here: it
 * cannot prove or echo the caller-selected UBA/slot/wrap identity.
 */
ClusterTxOutcome
cluster_runtime_visibility_resolve_exact_origin(const ClusterTxLocator *locator,
											 ClusterTxResolveMode mode, uint64 formation_epoch,
											 ClusterTxResolution *out,
											 ClusterTxResolveReason *reason_out)
{
	PGAlignedBlock record_buf;
	const UndoRecordHeader *record;
	ClusterTxResolution candidate;
	ClusterTxResolveReason reason = CLUSTER_TX_RESOLVE_AUTHORITY_UNAVAILABLE;
	ClusterTxOutcome outcome = CLUSTER_TX_UNKNOWN;
	ClusterTxProofKind proof_kind = CLUSTER_TX_PROOF_ORIGIN_DURABLE_TT_CLOG;
	ClusterLiveAuthority authority;
	TTSlot exact_slot;
	XidStatus native_status;
	NodeId origin_node;
	TransactionId top_xid;
	size_t record_size;
	SCN commit_scn = InvalidScn;
	uint16 tt_slot_offset;

	if (out != NULL)
		memset(out, 0, sizeof(*out));
	if (reason_out != NULL)
		*reason_out = CLUSTER_TX_RESOLVE_AUTHORITY_UNAVAILABLE;
	memset(&candidate, 0, sizeof(candidate));
	memset(&authority, 0, sizeof(authority));
	memset(&exact_slot, 0, sizeof(exact_slot));

	if (locator == NULL) {
		reason = CLUSTER_TX_RESOLVE_BAD_LOCATOR;
		goto unknown;
	}
	if (out == NULL
		|| (unsigned int)mode
			   > (unsigned int)CLUSTER_TX_RESOLVE_TERMINAL_CENSUS) {
		reason = CLUSTER_TX_RESOLVE_PROTOCOL;
		goto unknown;
	}
	if (cluster_epoch_get_current() != formation_epoch) {
		reason = CLUSTER_TX_RESOLVE_AUTHORITY_STALE;
		goto unknown;
	}

	origin_node = uba_origin_node_id(locator->uba);
	if (origin_node == InvalidNodeId) {
		reason = CLUSTER_TX_RESOLVE_BAD_UBA;
		goto unknown;
	}
	if (cluster_node_id < 0 || origin_node != (NodeId)cluster_node_id) {
		reason = CLUSTER_TX_RESOLVE_AUTHORITY_UNAVAILABLE;
		goto unknown;
	}

	record_size = cluster_undo_get_record(locator->uba, record_buf.data, sizeof(record_buf.data));
	if (record_size < sizeof(UndoRecordHeader)) {
		reason = CLUSTER_TX_RESOLVE_IO_ERROR;
		goto unknown;
	}
	record = (const UndoRecordHeader *)record_buf.data;
	if (!cluster_undo_record_validate_identity(locator, locator->uba, record, &reason))
		goto unknown;

	tt_slot_offset = cluster_tt_slot_id_to_offset(record->tt_slot_id);
	top_xid = locator->xid;
	if (cluster_tt_slot_durable_lookup_committed_stable(
			record->tt_slot_segment_id, tt_slot_offset, locator->xid, locator->tt_wrap,
			cluster_runtime_visibility_direct_xid_committed, &commit_scn)
		&& SCN_VALID(commit_scn)) {
		outcome = CLUSTER_TX_COMMITTED;
	} else {
		if (!cluster_tt_slot_durable_read_exact_stable(record->tt_slot_segment_id,
													  tt_slot_offset, locator->xid,
													  locator->tt_wrap, &exact_slot)) {
			reason = CLUSTER_TX_RESOLVE_AUTHORITY_UNAVAILABLE;
			goto unknown;
		}

		native_status = cluster_runtime_visibility_direct_xid_status(locator->xid);
		if ((exact_slot.status == TT_SLOT_COMMITTED
			 && native_status == TRANSACTION_STATUS_ABORTED)
			|| (exact_slot.status == TT_SLOT_ABORTED
				&& native_status == TRANSACTION_STATUS_COMMITTED)) {
			reason = CLUSTER_TX_RESOLVE_AUTHORITY_CONFLICT;
			goto unknown;
		}
		if (native_status == TRANSACTION_STATUS_ABORTED) {
			outcome = CLUSTER_TX_ABORTED;
			commit_scn = InvalidScn;
		} else if (native_status == TRANSACTION_STATUS_SUB_COMMITTED) {
			ClusterRuntimeSubtransSample subtrans;
			bool prepared = false;

			if (!cluster_runtime_visibility_sample_subtrans(locator->xid, &subtrans, &reason))
				goto unknown;
			top_xid = subtrans.xids[subtrans.count - 1];
			native_status = cluster_runtime_visibility_direct_xid_status(top_xid);
			native_status = cluster_runtime_visibility_recheck_prepared(top_xid,
															native_status, &prepared);
			if (!cluster_runtime_visibility_recheck_subtrans(&subtrans, &reason))
				goto unknown;

			proof_kind = CLUSTER_TX_PROOF_ORIGIN_SUBTRANS_TOP;
			commit_scn = InvalidScn;
			if (native_status == TRANSACTION_STATUS_COMMITTED) {
				if (exact_slot.status == TT_SLOT_ABORTED) {
					reason = CLUSTER_TX_RESOLVE_AUTHORITY_CONFLICT;
					goto unknown;
				}
				/* No exact top locator/TT commit SCN is carried in this batch. */
				reason = CLUSTER_TX_RESOLVE_COVERAGE_GAP;
				goto unknown;
			}
			if (native_status == TRANSACTION_STATUS_ABORTED) {
				if (exact_slot.status == TT_SLOT_COMMITTED) {
					reason = CLUSTER_TX_RESOLVE_AUTHORITY_CONFLICT;
					goto unknown;
				}
				outcome = CLUSTER_TX_ABORTED;
			} else if (native_status == TRANSACTION_STATUS_IN_PROGRESS) {
				if (prepared
					&& (exact_slot.status == TT_SLOT_ACTIVE
						|| exact_slot.status == TT_SLOT_ABORTED))
					outcome = CLUSTER_TX_PREPARED;
				else if (!prepared && exact_slot.status == TT_SLOT_ACTIVE)
					outcome = CLUSTER_TX_IN_PROGRESS;
				else {
					reason = CLUSTER_TX_RESOLVE_AUTHORITY_UNAVAILABLE;
					goto unknown;
				}
			} else {
				reason = CLUSTER_TX_RESOLVE_AUTHORITY_UNAVAILABLE;
				goto unknown;
			}
		} else if (native_status == TRANSACTION_STATUS_IN_PROGRESS) {
			bool prepared = false;

			native_status = cluster_runtime_visibility_recheck_prepared(locator->xid, native_status,
																		&prepared);
			commit_scn = InvalidScn;
			if (native_status == TRANSACTION_STATUS_COMMITTED) {
				if (exact_slot.status == TT_SLOT_ABORTED) {
					reason = CLUSTER_TX_RESOLVE_AUTHORITY_CONFLICT;
					goto unknown;
				}
				/* Retry will acquire a stable exact TT/CLOG/TT commit sample. */
				reason = CLUSTER_TX_RESOLVE_COVERAGE_GAP;
				goto unknown;
			}
			if (native_status == TRANSACTION_STATUS_ABORTED) {
				if (exact_slot.status == TT_SLOT_COMMITTED) {
					reason = CLUSTER_TX_RESOLVE_AUTHORITY_CONFLICT;
					goto unknown;
				}
				outcome = CLUSTER_TX_ABORTED;
			}
			else if (native_status == TRANSACTION_STATUS_IN_PROGRESS) {
				if (prepared
					&& (exact_slot.status == TT_SLOT_ACTIVE
						|| exact_slot.status == TT_SLOT_ABORTED)) {
					outcome = CLUSTER_TX_PREPARED;
					proof_kind = CLUSTER_TX_PROOF_ORIGIN_TWOPHASE;
				} else if (!prepared && exact_slot.status == TT_SLOT_ACTIVE)
					outcome = CLUSTER_TX_IN_PROGRESS;
				else {
					reason = CLUSTER_TX_RESOLVE_AUTHORITY_UNAVAILABLE;
					goto unknown;
				}
			} else {
				reason = CLUSTER_TX_RESOLVE_AUTHORITY_UNAVAILABLE;
				goto unknown;
			}
		} else if (native_status == TRANSACTION_STATUS_COMMITTED) {
			/* A matching valid commit SCN was not proved by the stable helper. */
			reason = CLUSTER_TX_RESOLVE_COVERAGE_GAP;
			goto unknown;
		} else {
			reason = CLUSTER_TX_RESOLVE_AUTHORITY_UNAVAILABLE;
			goto unknown;
		}
	}

	authority.origin_epoch = cluster_epoch_get_current();
	authority.live_hwm_lsn = GetFlushRecPtr(NULL);
	authority.tt_generation = cluster_undo_tt_retention_rollover_count();
	authority.authority_scn = cluster_scn_current();
	if (authority.origin_epoch != formation_epoch
		|| cluster_epoch_get_current() != authority.origin_epoch) {
		reason = CLUSTER_TX_RESOLVE_AUTHORITY_STALE;
		goto unknown;
	}
	if (XLogRecPtrIsInvalid(authority.live_hwm_lsn) || !SCN_VALID(authority.authority_scn)) {
		reason = CLUSTER_TX_RESOLVE_AUTHORITY_UNAVAILABLE;
		goto unknown;
	}

	candidate.locator_echo = *locator;
	candidate.top_xid = top_xid;
	candidate.outcome = outcome;
	candidate.proof_kind = proof_kind;
	candidate.commit_scn = commit_scn;
	candidate.authority = authority;
	*out = candidate;
	if (reason_out != NULL)
		*reason_out = CLUSTER_TX_RESOLVE_NONE;
	return outcome;

unknown:
	if (reason_out != NULL)
		*reason_out = reason;
	return CLUSTER_TX_UNKNOWN;
}

/*
 * cluster_vis_live_authority_covers
 *
 * Runtime D-i2 window gate: the pure policy under the CURRENT membership
 * epoch.  An epoch bump (origin fail-stop / any reconfig, D-i3) makes every
 * previously sampled authority fail this gate immediately — the in-flight
 * recycled resolution degrades to the unchanged 53R97 fail-closed.
 */
bool
cluster_vis_live_authority_covers(SCN demand_scn, ClusterLiveAuthority auth)
{
	return cluster_vis_live_authority_covers_policy(demand_scn, auth, cluster_epoch_get_current());
}


/*
 * Per-backend memo of the authority CO-SAMPLED with the block bytes last
 * installed into the L2 CR pool for one (origin, segment) — the pool stores
 * only page bytes, and a hit is unusable without the authority of exactly
 * those bytes (banner).  Direct-mapped, exact-match validated; a collision
 * only costs a re-fetch.  Backend-local by design: no locks, no torn
 * triples, and the fetch path is backend-context anyway.
 */
#define RTVIS_AUTH_MEMO_SLOTS 8

typedef struct RtvisAuthMemo {
	bool valid;
	int origin_node;
	uint32 segment_id;
	ClusterLiveAuthority auth;
} RtvisAuthMemo;

static RtvisAuthMemo rtvis_auth_memo[RTVIS_AUTH_MEMO_SLOTS];

static inline RtvisAuthMemo *
rtvis_auth_memo_slot(int origin_node, uint32 segment_id)
{
	uint32 h = (uint32)origin_node * 31u + segment_id;

	return &rtvis_auth_memo[h % RTVIS_AUTH_MEMO_SLOTS];
}

/*
 * Pool key for the cached undo header block.  Field reuse is deliberate and
 * documented: the CR pool key is an opaque discriminator tuple, so the
 * synthetic locator carries (magic, origin, segment) and the two identity
 * tails carry the epoch and TT generation the bytes were sampled under — a
 * reconfig or a TT rollover changes the key, leaving stale entries
 * unreachable (fail-closed by construction; LRU reclaims them).  The magic
 * spcOid keeps synthetic keys disjoint from every real-relation CR key.
 */
static ClusterCRCacheKey
rtvis_pool_key(int origin_node, uint32 segment_id, uint32 block_no, ClusterLiveAuthority auth)
{
	ClusterCRCacheKey key;

	memset(&key, 0, sizeof(key));
	key.rlocator.spcOid = GCS_BLOCK_UNDO_FETCH_TAG_MAGIC;
	key.rlocator.dbOid = (Oid)origin_node;
	key.rlocator.relNumber = (RelFileNumber)segment_id;
	key.forknum = MAIN_FORKNUM;
	key.blockno = (BlockNumber)block_no;
	key.read_scn = (SCN)auth.origin_epoch;				/* epoch discriminator */
	key.base_page_lsn = (XLogRecPtr)auth.tt_generation; /* generation discriminator */
	return key;
}

/*
 * cluster_undo_block_fetch_for_visibility — D-i1 (spec-6.12i CP2).
 *
 * See cluster_runtime_visibility.h for the contract.  Order of refusal
 * checks mirrors the origin-side serve so both ends agree on the slice
 * boundary (block 0 / TT header only).
 */
bool
cluster_undo_block_fetch_for_visibility(int origin_node, UBA uba, char *out_page,
										ClusterLiveAuthority *auth_out)
{
	uint32 segment_id = 0;
	uint32 block_no = 0;
	uint16 tt_slot_offset = 0;
	uint16 row_offset = 0;
	uint64 local_epoch;
	RtvisAuthMemo *memo;
	ClusterCRPoolHandle handle;

	if (out_page == NULL || auth_out == NULL)
		return false;
	memset(auth_out, 0, sizeof(*auth_out));

	if (!cluster_crossnode_runtime_visibility || origin_node < 0 || origin_node == cluster_node_id
		|| !uba_decode(uba, &segment_id, &block_no, &tt_slot_offset, &row_offset)
		|| block_no != 0) {
		cluster_rtvis_undo_fetch_note_failclosed();
		return false;
	}

	/*
	 * Cache leg (Q-i5): serve the pool bytes ONLY together with the
	 * same-epoch authority they were installed under.  The epoch equality
	 * check here is the D-i3 crash-shrink: after a reconfig the memo (and
	 * with it the pool key) is dead, so every path below re-fetches under
	 * the new epoch.
	 */
	local_epoch = cluster_epoch_get_current();
	memo = rtvis_auth_memo_slot(origin_node, segment_id);
	if (memo->valid && memo->origin_node == origin_node && memo->segment_id == segment_id
		&& memo->auth.origin_epoch == local_epoch) {
		ClusterCRCacheKey key = rtvis_pool_key(origin_node, segment_id, block_no, memo->auth);

		if (cluster_cr_pool_lookup_copy(&key, out_page)) {
			*auth_out = memo->auth;
			cluster_rtvis_undo_fetch_note_cache_hit();
			return true;
		}
	}

	/* Wire leg: fetch block + co-sampled authority from the origin. */
	if (!cluster_gcs_block_undo_tt_fetch_and_wait((int32)origin_node, segment_id, block_no,
												  out_page, auth_out)) {
		cluster_rtvis_undo_fetch_note_failclosed();
		return false;
	}
	cluster_rtvis_undo_fetch_note_wire();

	/*
	 * Install the PAIR: memo takes the reply authority verbatim (never a
	 * max-merge — the authority must stay the one sampled with these
	 * bytes), the pool takes the bytes under that authority's key.  A
	 * reserve refusal (pool off / full / already present) only loses the
	 * cache, never the result.
	 */
	memo->valid = true;
	memo->origin_node = origin_node;
	memo->segment_id = segment_id;
	memo->auth = *auth_out;

	{
		ClusterCRCacheKey key = rtvis_pool_key(origin_node, segment_id, block_no, *auth_out);

		if (cluster_cr_pool_reserve(&key, &handle))
			cluster_cr_pool_publish(&handle, out_page);
	}

	return true;
}

/*
 * rtvis_try_origin_verdict — CP5 (D-i4 / spec-6.15 D4) verdict fallback leg.
 *
 *	Reached when the single-block positive proof came back NONE (0-match /
 *	ambiguity) or COMMITTED (spec-7.1a hardening: a COMMITTED stamp is
 *	pre-commit evidence and only the origin can run the C1b CLOG cross-
 *	check on it): ask the ORIGIN for a complete own-TT by-xid verdict (the
 *	spec-3.22 retention theorem served cross-instance; see the header and
 *	cluster_cr_server.c lms_undo_verdict_serve for the origin-side legs).
 *
 *	The shipped horizon_scn / commit_scn are Lamport-observed BEFORE any
 *	admissibility decision (AD-008: an SCN that crossed the wire advances
 *	the local clock) — so even a leg-(e) refusal (read_scn behind the
 *	shipped horizon, the t/346 clock-skew case) self-heals: the NEXT
 *	snapshot's read_scn is at/after the observed horizon.
 *
 *	true only on a proven terminal verdict or an exact fresh-ref live proof;
 *	*out_commit_scn_is_bound marks a BELOW_HORIZON commit whose scn field is
 *	the horizon BOUND (valid against THIS read_scn only — never stamp/cache
 *	it).  false = fail-closed.
 */
static bool
rtvis_try_origin_verdict(int origin_node, uint32 undo_segment_id, TransactionId raw_xid,
						 uint32 expected_tt_slot_id, uint32 ref_epoch,
						 SCN freshref_pair_scn, SCN demand_scn, SCN read_scn,
						 bool authoritative, bool *out_committed, bool *out_in_progress,
						 SCN *out_commit_scn, bool *out_commit_scn_is_bound)
{
	ClusterGcsUndoVerdictPage verdict;
	ClusterLiveAuthority auth;
	bool freshref_pair = SCN_VALID(freshref_pair_scn);

	/*
	 * Q9: owner-as-master serve-gate precheck (coherent regime only).  A dead
	 * owner or an in-flight remaster fails closed early — with no doomed
	 * verdict request on the wire — symmetric with the D2-5 acquire_shared
	 * serve-gate (keyed on the OWNER under owner-as-master, not a hash static
	 * master).  Dead-owner SERVE from shared storage is D4, never a D2/D3
	 * false-resolve (Rule 8.A).  Off the coherent gate the 6.12i best-effort
	 * verdict path is unchanged (回归安全).
	 */
	if (cluster_undo_gcs_coherence && cluster_peer_mode_enabled()
		&& !cluster_undo_serve_allowed(cluster_cssd_get_peer_state((int32)origin_node)
										   != CLUSTER_CSSD_PEER_DEAD,
									   cluster_grd_recovery_in_progress())) {
		cluster_rtvis_verdict_note_failclosed();
		return false;
	}

	cluster_rtvis_verdict_note_wire();
	if (freshref_pair
		? !cluster_gcs_block_undo_freshref_c1b_pair_fetch_and_wait(
			  (int32)origin_node, undo_segment_id, expected_tt_slot_id, raw_xid,
			  ref_epoch, freshref_pair_scn, &verdict, &auth)
		: !cluster_gcs_block_undo_verdict_fetch_and_wait(
			  (int32)origin_node, undo_segment_id, expected_tt_slot_id, raw_xid,
			  authoritative, &verdict, &auth)) {
		cluster_rtvis_verdict_note_failclosed();
		return false;
	}

	/* Lamport-observe every SCN that crossed the wire (AD-008), before any
	 * gate can refuse — the observe is what makes a refusal self-heal. */
	cluster_scn_observe((SCN)verdict.horizon_scn);
	cluster_scn_observe((SCN)verdict.commit_scn);
	cluster_scn_observe(auth.authority_scn);

	if (!cluster_vis_live_authority_covers(
			freshref_pair ? freshref_pair_scn : demand_scn, auth)) {
		cluster_vis_bump_covers_scn_refuse_count(); /* spec-7.1a D6 */
		cluster_vis53r97_note_covers_refuse();		/* spec-7.1 D0 census (union) */
		cluster_rtvis_verdict_note_failclosed();
		return false;
	}
	if (freshref_pair
		&& (verdict.verdict
				!= (uint8)CLUSTER_GCS_UNDO_VERDICT_COMMITTED_EXACT
			|| verdict.commit_scn != freshref_pair_scn)) {
		cluster_rtvis_verdict_note_failclosed();
		return false;
	}

	switch (verdict.verdict) {
	case (uint8)CLUSTER_GCS_UNDO_VERDICT_IN_PROGRESS:
		/*
		 * S3-P0-13: positive, non-terminal origin liveness proof.  It is
		 * usable only as REMOTE/IN_PROGRESS by the fresh-ref current-DML
		 * consumer; there is no SCN/bound and no memo/hint side effect.
		 */
		if (!authoritative || expected_tt_slot_id < 1
			|| expected_tt_slot_id > TT_SLOTS_PER_SEGMENT) {
			cluster_rtvis_verdict_note_failclosed();
			return false;
		}
		*out_in_progress = true;
		elog(DEBUG1,
			 "rtvis verdict: xid %u origin %d IN_PROGRESS exact segment %u slot %u",
			 raw_xid, origin_node, undo_segment_id, expected_tt_slot_id);
		return true;

	case (uint8)CLUSTER_GCS_UNDO_VERDICT_COMMITTED_EXACT:
		*out_committed = true;
		*out_commit_scn = (SCN)verdict.commit_scn;
		cluster_rtvis_verdict_note_exact();
		elog(DEBUG1,
			 "rtvis verdict: xid %u origin %d COMMITTED_EXACT scn " UINT64_FORMAT " wrap %u",
			 raw_xid, origin_node, (uint64)verdict.commit_scn, (unsigned)verdict.wrap);
		return true;

	case (uint8)CLUSTER_GCS_UNDO_VERDICT_ABORTED:
		*out_committed = false;
		cluster_rtvis_verdict_note_exact();
		elog(DEBUG1, "rtvis verdict: xid %u origin %d ABORTED", raw_xid, origin_node);
		return true;

	case (uint8)CLUSTER_GCS_UNDO_VERDICT_COMMITTED_BELOW_HORIZON:
		if (freshref_pair) {
			cluster_rtvis_verdict_note_failclosed();
			return false;
		}
		/*
		 * Requester leg (e) of the retention proof: the bound commit_scn <=
		 * horizon decides against read_scn only when the horizon is not
		 * newer than read_scn.  A terminal-state-only caller (read_scn
		 * invalid) consumes only the origin+CLOG-proven COMMITTED fact; the
		 * is_bound marker below still forbids stamping/caching the horizon.
		 */
		if (!cluster_vis_committed_bound_admissible((SCN)verdict.horizon_scn, read_scn)) {
			cluster_rtvis_verdict_note_inadmissible();
			elog(DEBUG1,
				 "rtvis verdict: xid %u origin %d BELOW_HORIZON " UINT64_FORMAT
				 " inadmissible for read_scn " UINT64_FORMAT " (observed; next snapshot heals)",
				 raw_xid, origin_node, (uint64)verdict.horizon_scn, (uint64)read_scn);
			return false;
		}
		*out_committed = true;
		*out_commit_scn = (SCN)verdict.horizon_scn;
		*out_commit_scn_is_bound = true;
		cluster_rtvis_verdict_note_below_horizon();
		elog(DEBUG1,
			 "rtvis verdict: xid %u origin %d COMMITTED_BELOW_HORIZON " UINT64_FORMAT
			 " admissible for read_scn " UINT64_FORMAT,
			 raw_xid, origin_node, (uint64)verdict.horizon_scn, (uint64)read_scn);
		return true;

	default:
		/* page_usable() already fenced the kind range; defense in depth. */
		cluster_rtvis_verdict_note_failclosed();
		return false;
	}
}

/*
 * rtvis_try_resolve_remote_internal — CP3 + CP5 (D-i2/D-i4 wiring body).
 *
 *	Active-runtime terminal resolution of a RECYCLED remote ITL ref:
 *	  fetch (D-i1, block 0 of the ref's segment)  ->  covers gate (D-i2,
 *	  co-sampled authority vs this tuple's page LSN)  ->  positive proof
 *	  (exact xid+wrap slot match on the SHIPPED bytes)  ->  on a proof NONE
 *	  or a proof COMMITTED (evidence only -- the stamp lands at pre-commit
 *	  and needs the origin's C1b CLOG cross-check, see the fast-leg banner
 *	  below), the CP5 origin-verdict leg (complete scan at the origin);
 *	  only a proof ABORTED short-circuits.
 *
 *	The proof scans the very bytes the authority was co-sampled with (also
 *	on a cache hit — the pool/memo only ever serve the pair as installed),
 *	so D-i2 condition (c) generation consistency is structural: there is no
 *	second read whose generation could diverge.  auth.tt_generation is kept
 *	for observability and any future cross-source check.
 *
 *	A nonzero expected_tt_slot_id lets the authoritative fresh-ref consumer
 *	carry an exact 1-based slot binding and receive a proven IN_PROGRESS
 *	answer through out_in_progress.  Slot 0 retains the terminal-only legacy
 *	contract.  Every unproven outcome returns false so classify_ref keeps the
 *	pre-existing STALE_OR_AMBIGUOUS -> 53R97 fail-closed (Rule 8.A: only
 *	"resolve when provable" is widened).
 */
static bool
rtvis_try_resolve_remote_internal(int origin_node, uint32 undo_segment_id,
								  uint32 expected_tt_slot_id, TransactionId raw_xid,
								  uint32 ref_epoch, SCN freshref_pair_scn,
								  SCN read_scn, bool authoritative, bool *out_committed,
								  bool *out_in_progress, SCN *out_commit_scn,
								  bool *out_commit_scn_is_bound)
{
	PGAlignedBlock page;
	ClusterLiveAuthority auth;
	SCN demand_scn;
	bool got_block = false;

	if (out_committed != NULL)
		*out_committed = false;
	if (out_in_progress != NULL)
		*out_in_progress = false;
	if (out_commit_scn != NULL)
		*out_commit_scn = InvalidScn;

	/* spec-5.22e D5-8: inherently remote -- same admission as the verdict
	 * entry (53R60 on not-member/pre-join; false = mixed-cap fail-closed). */
	if (!cluster_undo_horizon_read_admission_enforce(read_scn))
		return false;
	if (out_commit_scn_is_bound != NULL)
		*out_commit_scn_is_bound = false;
	if (out_committed == NULL || out_in_progress == NULL || out_commit_scn == NULL
		|| out_commit_scn_is_bound == NULL)
		return false;

	if (!cluster_crossnode_runtime_visibility)
		return false;
	if (origin_node < 0 || origin_node == cluster_node_id)
		return false;

	/*
	 * uba_encode contract pre-validation (segment 0 is bootstrap-only and
	 * Assert-fenced there; a ref carrying it is not resolvable evidence).
	 */
	if (undo_segment_id == 0 || undo_segment_id > UINT16_MAX)
		return false;

	/*
	 * PGRAC: spec-7.1a D3 -- the covers demand.  Snapshot legs demand
	 * conclusiveness for their read_scn; no-snapshot legs (writer terminal
	 * resolution, read_scn = InvalidScn) demand the local clock AS OF NOW.
	 * Sampled BEFORE any fetch: the reply's SCNs are Lamport-observed below,
	 * so sampling after would trivially satisfy the gate and void it.
	 */
	demand_scn = SCN_VALID(read_scn) ? read_scn : cluster_scn_current();

	/*
	 * Single-block fast leg (CP3), entirely opportunistic since CP5: the
	 * ref's segment id names the CURRENT slot owner's segment, which under
	 * the spec-6.15 D4 xid-derived origin may not even exist on the node we
	 * are asking — a fetch miss there is a routing artifact, not evidence.
	 * Only an exact ABORTED proof still short-circuits the wire (an abort
	 * stamp is terminal and irreversible); a COMMITTED proof is EVIDENCE
	 * that must be finalized by the origin's C1b CLOG cross-check on the
	 * verdict leg, and a fetch miss or a proof NONE prove nothing — all
	 * three fall to the origin-verdict leg.
	 *
	 * Block0 acquisition (spec-5.22c D3-2): under cluster.undo_gcs_coherence
	 * + peer-mode the read goes through the D2 owner-as-master coherent
	 * S-grant (cluster_undo_block_acquire_shared -- owner-as-master routing +
	 * D2-5 serve-gate + epoch/generation admissibility; the owner ships the
	 * image and this peer never opens the foreign undo file, invariant #8).
	 * Off that gate it keeps the 6.12i best-effort authority-less fetch
	 * verbatim, so coherence off is byte-for-byte the old path (回归安全).
	 *
	 * Amendment-1 (generation): the requester has no independent source for
	 * the owner's current segment generation, so it encodes rid.field3 == gen
	 * (its own known value: a carried ITL-ref generation if any, else a
	 * neutral 0).  D2's generation_matches(rid, gen) then compares two equal
	 * caller values and is structurally true -- it is NOT the anti-ABA gate on
	 * this path.  The real anti-ABA is the slot-level xid+wrap positive proof
	 * below (content-based on the shipped bytes; a recycled segment changes
	 * the slot wrap, so the proof fails NONE -> fail-closed).
	 */
	if (cluster_undo_gcs_coherence && cluster_peer_mode_enabled()) {
		ClusterResId rid;
		ClusterUndoGrantResult res;
		uint32 gen = 0; /* Amendment-1: rid.field3 == gen, self-consistent */

		cluster_undo_resid_encode((int32)origin_node, undo_segment_id, 0 /* block0 */, gen, &rid);
		if (cluster_undo_block_acquire_shared(&rid, gen, page.data, &res)) {
			/* Reconstruct the co-sampled authority the SCN-covers gate below
			 * needs (D2 kept only the triple, not the struct).  The D2 grant
			 * wire predates the spec-7.1a authority_scn co-sample, so the
			 * clock rides as InvalidScn -- the covers gate then refuses
			 * conservatively and this leg falls to the verdict leg (fail-
			 * closed; the coherent fast leg re-arms once the grant wire
			 * carries the clock, merge-window follow-up). */
			auth.origin_epoch = res.origin_epoch;
			auth.live_hwm_lsn = res.live_hwm_lsn;
			auth.tt_generation = res.tt_generation;
			auth.authority_scn = InvalidScn;
			got_block = true;
		}
	} else
		got_block = cluster_undo_block_fetch_for_visibility(
			origin_node, uba_encode(undo_segment_id, 0, 0, 0), page.data, &auth);

	if (got_block) {
		/* Lamport-observe the co-sampled clock BEFORE the gate can refuse
		 * (AD-008) -- the observe is what makes a refusal self-heal.
		 * (InvalidScn from the S-grant leg is a no-op observe.) */
		cluster_scn_observe(auth.authority_scn);
		if (!cluster_vis_live_authority_covers(demand_scn, auth))
			cluster_vis_bump_covers_scn_refuse_count(); /* spec-7.1a D6 */
		else {
			switch (cluster_vis_tt_block_positive_proof(
				page.data, undo_segment_id, (uint8)(origin_node + 1), raw_xid, NULL, NULL)) {
			case CLUSTER_VIS_TT_PROOF_COMMITTED:

				/*
				 * PGRAC: spec-7.1a hardening (C1b) -- a shipped COMMITTED
				 * stamp is evidence, NOT a verdict.  The durable slot is
				 * stamped at pre-commit (2PC COMMIT PREPARED stamps before
				 * the commit record -- cluster_tt_durable.h), so an owner
				 * crash in that window leaves a COMMITTED stamp on an
				 * in-doubt xid; concluding committed here is a
				 * false-committed (Rule 8.A).  Only the origin can run the
				 * C1b CLOG cross-check (the foreign xid has no meaning in
				 * the LOCAL clog -- AD-012 例外 9), and it does so on the
				 * verdict leg (cluster_cr_server.c lms_undo_verdict_serve
				 * refuses !TransactionIdDidCommit): fall through to it.
				 */
				break;
			case CLUSTER_VIS_TT_PROOF_ABORTED:
				cluster_rtvis_resolve_note_aborted();
				return true;
			case CLUSTER_VIS_TT_PROOF_NONE:
			default:
				break; /* 0-match / ambiguity: fall to the verdict leg */
			}
		}
	}

	/*
	 * CP5 (D-i4) verdict leg: a single fetched TT block cannot prove
	 * recycled-or-aborted (the slot may live in another segment), and a
	 * fetch/covers miss proves nothing either — ask the origin for the
	 * complete own-TT verdict instead of failing closed outright.
	 */
	if (rtvis_try_origin_verdict(
			origin_node, undo_segment_id, raw_xid, expected_tt_slot_id, ref_epoch,
			freshref_pair_scn, demand_scn, read_scn, authoritative, out_committed,
			out_in_progress, out_commit_scn, out_commit_scn_is_bound)) {
		if (*out_in_progress) {
			/* Non-terminal positive proof: do not fold it into either of the
			 * terminal committed/aborted census buckets. */
		} else if (*out_committed)
			cluster_rtvis_resolve_note_committed();
		else
			cluster_rtvis_resolve_note_aborted();
		return true;
	}
	cluster_rtvis_resolve_note_failclosed();
	return false;
}

/*
 * Legacy terminal-only entry.  It carries no physical slot binding, so even
 * an authoritative caller cannot request the S3-P0-13 live widening.
 */
bool
cluster_runtime_visibility_try_resolve_remote(int origin_node, uint32 undo_segment_id,
											  TransactionId raw_xid, SCN read_scn,
											  bool authoritative, bool *out_committed,
											  SCN *out_commit_scn,
											  bool *out_commit_scn_is_bound)
{
	bool in_progress = false;

	if (!rtvis_try_resolve_remote_internal(
			origin_node, undo_segment_id, 0, raw_xid, 0, InvalidScn,
			read_scn, authoritative,
			out_committed, &in_progress, out_commit_scn, out_commit_scn_is_bound))
		return false;
	if (in_progress) {
		cluster_rtvis_resolve_note_failclosed();
		return false;
	}
	return true;
}

/*
 * rtvis_resolve_own_xid — D3-4 master==self local verdict resolve (Q5).
 *
 *	The owner resolving its OWN xid needs no network grant: its own durable TT
 *	+ own CLOG are the authority.  It must NOT take acquire_shared, which is
 *	fail-closed-forward-D6 for master==self (cluster_undo_gcs_grant.c:97).  The
 *	resolution reuses cluster_lms_undo_verdict_fill_page — the very function
 *	the origin serves foreign requesters with — so a node's self answer and
 *	its served answer over one xid can never diverge (Rule 8.A).  A
 *	COMMITTED_BOUND then applies the same admission as the foreign path:
 *	observe the horizon (Lamport self-heal); a snapshot admits only when
 *	read_scn is at/after it, while a terminal-state-only caller consumes only
 *	the proven COMMITTED fact and keeps the bound marker.
 */
static ClusterUndoVerdictResult
rtvis_resolve_own_xid(TransactionId raw_xid, SCN read_scn, bool ordinary_single)
{
	ClusterGcsUndoVerdictPage v;
	ClusterUndoVerdictResult r;
	ClusterUndoVerdictResult unknown
		= { .kind = CLUSTER_UNDO_VERDICT_UNKNOWN_FAIL_CLOSED, .commit_scn = InvalidScn, .wrap = 0 };

	memset(&v, 0, sizeof(v));
	/* master==self keeps the stripe self-check (authoritative=false): D6-7's
	 * physical-binding relaxation is for the FOREIGN fresh-ref serve only.
	 * That stripe check is separate from the ordinary/fresh consumer role. */
	if (!cluster_lms_undo_verdict_fill_page(raw_xid, false, ordinary_single, &v)) {
		cluster_rtvis_resolve_note_failclosed(); /* D3-6: self-path observability */
		return unknown;							 /* in-doubt / ambiguous / not-own -> fail-closed */
	}

	r = cluster_undo_verdict_from_wire_page(&v, raw_xid);

	if (r.kind == CLUSTER_UNDO_VERDICT_COMMITTED_BOUND) {
		cluster_scn_observe(r.commit_scn);
		if (!cluster_vis_committed_bound_admissible(r.commit_scn, read_scn)) {
			cluster_rtvis_resolve_note_failclosed();
			return unknown; /* bound inadmissible for this consumer; snapshot heals next */
		}
	}

	/* D3-6: count the self-path terminal outcome on the same rtvis resolve
	 * counters the foreign path uses (total verdict resolutions by outcome). */
	if (r.kind == CLUSTER_UNDO_VERDICT_COMMITTED_EXACT
		|| r.kind == CLUSTER_UNDO_VERDICT_COMMITTED_BOUND)
		cluster_rtvis_resolve_note_committed();
	else if (r.kind == CLUSTER_UNDO_VERDICT_ABORTED)
		cluster_rtvis_resolve_note_aborted();
	else
		cluster_rtvis_resolve_note_failclosed();
	return r;
}

static bool
rtvis_local_freshref_c1b_pair_eligible(
	int origin_node, uint32 undo_segment_id, TransactionId raw_xid,
	TransactionId ref_xid, uint32 expected_tt_slot_id, uint32 ref_epoch,
	SCN retained_commit_scn)
{
	uint64 current_epoch = cluster_epoch_get_current();

	return origin_node == cluster_node_id && cluster_node_id >= 0
		&& cluster_node_id < CLUSTER_MAX_NODES
		&& TransactionIdIsNormal(raw_xid) && ref_xid == raw_xid
		&& SCN_VALID(retained_commit_scn) && current_epoch <= UINT32_MAX
		&& ref_epoch == (uint32) current_epoch
		&& undo_segment_id > 0 && undo_segment_id <= UINT16_MAX
		&& expected_tt_slot_id >= 1
		&& expected_tt_slot_id <= TT_SLOTS_PER_SEGMENT;
}

static ClusterUndoVerdictResult
rtvis_resolve_own_xid_freshref_c1b_pair(TransactionId raw_xid, uint32 undo_segment_id,
										uint32 expected_tt_slot_id, SCN retained_commit_scn,
										SCN read_scn)
{
	ClusterUndoVerdictResult result = {
		.kind = CLUSTER_UNDO_VERDICT_UNKNOWN_FAIL_CLOSED,
		.commit_scn = InvalidScn,
		.wrap = 0};
	uint16 wrap = 0;

	/* A local immutable read can consume the same snapshot-relative origin
	 * bound as a foreign read. The terminal-census caller passes no read SCN
	 * and must remain exact-only. A bound is never the retained exact stamp. */
	if (SCN_VALID(read_scn)) {
		uint64 epoch = cluster_epoch_get_current();
		ClusterUndoVerdictResult ordinary = rtvis_resolve_own_xid(raw_xid, read_scn, false);

		if (cluster_epoch_get_current() != epoch)
			return result;
		if (ordinary.kind == CLUSTER_UNDO_VERDICT_COMMITTED_BOUND)
			return ordinary;
	}

	if (!cluster_cr_server_local_freshref_c1b_pair_exact(
			raw_xid, undo_segment_id, expected_tt_slot_id,
			retained_commit_scn, &wrap))
	{
		cluster_rtvis_resolve_note_failclosed();
		return result;
	}
	result.kind = CLUSTER_UNDO_VERDICT_COMMITTED_EXACT;
	result.commit_scn = retained_commit_scn;
	result.wrap = wrap;
	cluster_rtvis_resolve_note_committed();
	return result;
}
/* The self arm keeps the request role even though it needs no wire exchange. */
static ClusterUndoVerdictResult
rtvis_resolve_self_verdict(TransactionId raw_xid, uint32 undo_segment_id,
						   uint32 expected_tt_slot_id, SCN read_scn, bool authoritative,
						   SCN freshref_pair_scn)
{
	if (SCN_VALID(freshref_pair_scn))
		return rtvis_resolve_own_xid_freshref_c1b_pair(
			raw_xid, undo_segment_id, expected_tt_slot_id, freshref_pair_scn, read_scn);
	return rtvis_resolve_own_xid(raw_xid, read_scn, !authoritative);
}

#ifdef USE_CLUSTER_UNIT
ClusterUndoVerdictResult
cluster_runtime_visibility_test_local_retained_read(TransactionId xid, uint32 segment, uint32 slot,
													SCN retained_scn, SCN read_scn);
ClusterUndoVerdictResult
cluster_runtime_visibility_test_local_retained_read(TransactionId xid, uint32 segment, uint32 slot,
													SCN retained_scn, SCN read_scn)
{
	return rtvis_resolve_own_xid_freshref_c1b_pair(xid, segment, slot, retained_scn, read_scn);
}
ClusterUndoVerdictResult
cluster_runtime_visibility_test_self_verdict_role(TransactionId xid, uint32 segment, uint32 slot,
												  SCN read_scn, bool authoritative,
												  SCN retained_scn);
ClusterUndoVerdictResult
cluster_runtime_visibility_test_self_verdict_role(TransactionId xid, uint32 segment, uint32 slot,
												  SCN read_scn, bool authoritative,
												  SCN retained_scn)
{
	return rtvis_resolve_self_verdict(xid, segment, slot, read_scn, authoritative, retained_scn);
}
#endif

/* Local-origin retained proof used by terminal census after its DATA record
 * has left the resident undo cache.  This consumes only the already-frozen
 * page identity and retained SCN; the origin C1b sampler holds the canonical
 * TT/no-raw-reuse bracket. */
ClusterTxOutcome
cluster_runtime_visibility_resolve_terminal_census_retained_local_exact(
	const ClusterTxLocator *locator, SCN retained_commit_scn,
	const ClusterSemanticAdmissionToken *admission, ClusterTxResolution *out,
	ClusterTxResolveReason *reason_out)
{
	ClusterUndoVerdictResult verdict;
	ClusterTxResolveReason reason = CLUSTER_TX_RESOLVE_AUTHORITY_UNAVAILABLE;
	NodeId origin;
	uint32 segment_id;
	uint32 block_no;
	uint16 tt_slot_offset;
	uint16 row_offset;

	if (out != NULL)
		memset(out, 0, sizeof(*out));
	if (reason_out != NULL)
		*reason_out = reason;
	if (locator == NULL || out == NULL || admission == NULL
		|| locator->itl_kind != ITL_FLAG_NEEDS_CLEANOUT
		|| locator->tt_wrap != TT_WRAP_INVALID
		|| !TransactionIdIsNormal(locator->xid)
		|| !SCN_VALID(retained_commit_scn)
		|| admission->formation_epoch > UINT32_MAX)
	{
		reason = locator == NULL ? CLUSTER_TX_RESOLVE_BAD_LOCATOR
								 : CLUSTER_TX_RESOLVE_PROTOCOL;
		goto failed;
	}
	if (!cluster_runtime_visibility_admission_current(
			CLUSTER_TX_RESOLVE_TERMINAL_CENSUS, admission))
	{
		reason = CLUSTER_TX_RESOLVE_AUTHORITY_STALE;
		goto failed;
	}
	if (!uba_decode(locator->uba, &segment_id, &block_no, &tt_slot_offset,
					&row_offset)
		|| block_no == 0 || segment_id == 0 || segment_id > UINT16_MAX
		|| tt_slot_offset >= TT_SLOTS_PER_SEGMENT)
	{
		reason = CLUSTER_TX_RESOLVE_BAD_UBA;
		goto failed;
	}
	origin = uba_origin_node_id(locator->uba);
	if (origin == InvalidNodeId || cluster_node_id < 0
		|| origin != (NodeId) cluster_node_id
		|| !cluster_crossnode_runtime_visibility
		|| !rtvis_local_freshref_c1b_pair_eligible(
			(int) origin, segment_id, locator->xid, locator->xid,
			(uint32) tt_slot_offset + 1,
			(uint32) admission->formation_epoch, retained_commit_scn))
	{
		reason = CLUSTER_TX_RESOLVE_BAD_UBA;
		goto failed;
	}

	verdict = rtvis_resolve_own_xid_freshref_c1b_pair(
		locator->xid, segment_id, (uint32)tt_slot_offset + 1, retained_commit_scn, InvalidScn);
	if (verdict.kind != CLUSTER_UNDO_VERDICT_COMMITTED_EXACT
		|| verdict.commit_scn != retained_commit_scn)
		goto failed;
	if (!cluster_runtime_visibility_admission_current(
			CLUSTER_TX_RESOLVE_TERMINAL_CENSUS, admission))
	{
		reason = CLUSTER_TX_RESOLVE_AUTHORITY_STALE;
		goto failed;
	}

	out->locator_echo = *locator;
	out->top_xid = locator->xid;
	out->outcome = CLUSTER_TX_COMMITTED;
	out->proof_kind = CLUSTER_TX_PROOF_ORIGIN_DURABLE_TT_CLOG;
	out->commit_scn = retained_commit_scn;
	cluster_runtime_visibility_origin_candidate_stamp(out, admission);
	if (out->authority.origin_epoch != admission->formation_epoch
		|| XLogRecPtrIsInvalid(out->authority.live_hwm_lsn)
		|| !SCN_VALID(out->authority.authority_scn)
		|| !cluster_runtime_visibility_admission_current(
			CLUSTER_TX_RESOLVE_TERMINAL_CENSUS, admission))
	{
		reason = CLUSTER_TX_RESOLVE_AUTHORITY_STALE;
		goto failed;
	}
	if (reason_out != NULL)
		*reason_out = CLUSTER_TX_RESOLVE_NONE;
	return CLUSTER_TX_COMMITTED;

failed:
	if (out != NULL)
		memset(out, 0, sizeof(*out));
	if (reason_out != NULL)
		*reason_out = reason;
	return CLUSTER_TX_UNKNOWN;
}

ClusterTxOutcome
cluster_runtime_visibility_resolve_terminal_census_retained_exact(
	const ClusterTxLocator *locator, SCN retained_commit_scn,
	const ClusterSemanticAdmissionToken *admission, ClusterTxResolution *out,
	ClusterTxResolveReason *reason_out)
{
	NodeId origin;

	if (locator == NULL)
	{
		if (out != NULL)
			memset(out, 0, sizeof(*out));
		if (reason_out != NULL)
			*reason_out = CLUSTER_TX_RESOLVE_BAD_LOCATOR;
		return CLUSTER_TX_UNKNOWN;
	}
	origin = uba_origin_node_id(locator->uba);
	if (cluster_node_id >= 0 && origin == (NodeId) cluster_node_id)
		return cluster_runtime_visibility_resolve_terminal_census_retained_local_exact(
			locator, retained_commit_scn, admission, out, reason_out);
	return cluster_runtime_visibility_resolve_terminal_census_retained_remote_exact(
		locator, retained_commit_scn, admission, out, reason_out);
}

/*
 * rtvis_authority_serve_block0 — D4-4 self-authority dead-owner block0 serve
 * (spec-5.22d §2.4, Route B), requester-leg wrapper.
 *
 *	Self IS the elected serve authority for a dead/absent owner's undo
 *	resource.  The read + coverage + proof runs in the SHARED prove core
 *	(cluster_undo_authority_block0_prove, D4-5) — the same core the
 *	wire-served LMS authority leg runs, so the self answer and the served
 *	answer over one (owner, segment, xid) can never diverge (Rule 8.A, the
 *	same discipline as cluster_lms_undo_verdict_fill_page on the live-owner
 *	path).  This wrapper only folds the outcome into the rtvis resolve
 *	counters; the prove core owns the undo_authority_* counters.
 */
static ClusterUndoVerdictResult
rtvis_authority_serve_block0(int origin_node, uint32 undo_segment_id, TransactionId raw_xid,
							 const ClusterUndoServeRoute *route)
{
	ClusterUndoVerdictResult r;

	/* serve_decide only emits SELF_BLOCK0 on an OK route */
	Assert(route->status == CLUSTER_UNDO_AUTHORITY_OK);

	r = cluster_undo_authority_block0_prove((int32)origin_node, undo_segment_id, raw_xid,
											route->reconfig_epoch);
	if (r.kind == CLUSTER_UNDO_VERDICT_COMMITTED_EXACT)
		cluster_rtvis_resolve_note_committed();
	else if (r.kind == CLUSTER_UNDO_VERDICT_ABORTED)
		cluster_rtvis_resolve_note_aborted();
	else
		cluster_rtvis_resolve_note_failclosed();
	return r;
}

/*
 * cluster_undo_verdict_resolve — D3 cross-node xid -> commit_scn verdict entry
 * (D3-3).  The D6 consumer calls this to decide a seed / fresh-ref tuple.
 *
 *	master==self (owner reads its own xid) routes to the local durable resolve
 *	(rtvis_resolve_own_xid); master!=self (a peer reading a foreign owner — the
 *	seed/joiner main scene) routes to the CP3 owner-as-master S-grant + CP5
 *	origin verdict (cluster_runtime_visibility_try_resolve_remote), whose
 *	outcome is folded into the taxonomy (Amendment-2 collapses is_bound into
 *	the kind, so a consumer never sees the side axis).  Every unproven path
 *	returns UNKNOWN_FAIL_CLOSED so the caller keeps its 53R97 boundary
 *	(Rule 8.A — never a false-visible edge).
 *
 *	The foreign-path result carries wrap == 0: the wrap is the anti-ABA
 *	evidence already consumed inside try_resolve_remote (proof + wrap-suspect
 *	gate), not a consumer output; the self path carries it verbatim from the
 *	verdict page.
 */
static ClusterUndoVerdictResult
cluster_undo_verdict_resolve_internal(
	int origin_node, uint32 undo_segment_id, TransactionId raw_xid,
	uint32 expected_tt_slot_id, SCN read_scn, bool authoritative,
	TransactionId ref_xid, uint32 ref_epoch, SCN freshref_pair_scn)
{
	ClusterUndoVerdictResult unknown
		= { .kind = CLUSTER_UNDO_VERDICT_UNKNOWN_FAIL_CLOSED, .commit_scn = InvalidScn, .wrap = 0 };
	bool committed = false;
	bool in_progress = false;
	bool is_bound = false;
	SCN commit_scn = InvalidScn;
	bool freshref_pair = SCN_VALID(freshref_pair_scn);

	if (!cluster_crossnode_runtime_visibility)
		return unknown;
	if (!TransactionIdIsNormal(raw_xid) || origin_node < 0)
		return unknown;
	if (freshref_pair
		&& (!authoritative
			|| !(origin_node == cluster_node_id
					 ? rtvis_local_freshref_c1b_pair_eligible(
						 origin_node, undo_segment_id, raw_xid, ref_xid,
						 expected_tt_slot_id, ref_epoch, freshref_pair_scn)
					 : cluster_vis_freshref_c1b_pair_request_eligible(
						 raw_xid, ref_xid, true, freshref_pair_scn,
						 ref_epoch, cluster_epoch_get_current(), origin_node,
						 cluster_node_id, undo_segment_id,
						 expected_tt_slot_id))))
		return unknown;

	/*
	 * spec-5.22e D5-8 read admission, FOREIGN arm only (own-instance reads
	 * below are untouched): a non-MEMBER or pre-join snapshot must not
	 * consult foreign undo (53R60 inside), and a mixed-capability cluster
	 * fails closed here (false return keeps the UNKNOWN/53R97 shape).
	 */
	if (origin_node != cluster_node_id && !cluster_undo_horizon_read_admission_enforce(read_scn))
		return unknown;

	/* master==self: own CLOG + own durable TT authority (Q5/D3-4). */
	if (origin_node == cluster_node_id) {
		return rtvis_resolve_self_verdict(raw_xid, undo_segment_id, expected_tt_slot_id, read_scn,
										  authoritative, freshref_pair_scn);
	}

	/*
	 * D4-4 precision chain (spec-5.22d §2.4): under the armed data plane,
	 * route by owner liveness BEFORE asking the owner.  A dead/absent owner
	 * cannot serve (its S-grant / verdict wire has nobody behind it); the
	 * elected survivor authority serves its block0 verdict instead.  Off the
	 * arm gate the chain is inert and the pre-D4 path below runs verbatim
	 * (回归安全, §2.6).
	 */
	if (cluster_undo_gcs_coherence && cluster_peer_mode_enabled()) {
		ClusterResId rid;
		ClusterUndoServeRoute route;

		cluster_undo_resid_encode((int32)origin_node, undo_segment_id, 0 /* block0 */, 0, &rid);
		route = cluster_undo_serve_authority(&rid, cluster_epoch_get_current());
		switch (cluster_undo_authority_serve_decide(&route, cluster_node_id)) {
		case CLUSTER_UNDO_AUTHORITY_SERVE_SELF_BLOCK0:
			if (freshref_pair)
				return unknown;
			/* self IS the elected authority for the dead owner */
			return rtvis_authority_serve_block0(origin_node, undo_segment_id, raw_xid, &route);
		case CLUSTER_UNDO_AUTHORITY_SERVE_PEER_BLOCK0: {
			ClusterUndoVerdictResult r;

			if (freshref_pair)
				return unknown;

			/*
			 * D4-6: a PEER is the elected authority — kind-4 wire serve.
			 * Capability gate first (约束/A.1③): never route kind 4 to a
			 * peer that did not advertise the D4 protocol bit; the election
			 * is deterministic and NOT re-run against a different node, so
			 * a non-capable authority means fail closed.  The fetch itself
			 * fails closed on timeout / DENIED / wrong sender / epoch moved
			 * / malformed or v1 page (8.A amend binding inside).
			 */
			if (!cluster_peer_supports_undo_authority_serve(route.destination_node)
				|| !cluster_gcs_block_undo_authority_verdict_fetch_and_wait(
					route.destination_node, origin_node, undo_segment_id, raw_xid, &r)) {
				cluster_undo_authority_note_failclosed();
				cluster_rtvis_resolve_note_failclosed();
				return unknown;
			}

			/* Lamport-observe the SCN that crossed the wire (AD-008);
			 * observe of InvalidScn (ABORTED) is a no-op. */
			cluster_scn_observe(r.commit_scn);
			if (r.kind == (uint8)CLUSTER_UNDO_VERDICT_COMMITTED_EXACT)
				cluster_rtvis_resolve_note_committed();
			else
				cluster_rtvis_resolve_note_aborted();
			return r;
		}
		case CLUSTER_UNDO_AUTHORITY_SERVE_OWNER_LIVE:
			break; /* live owner: unchanged CP3 + CP5 path below */
		case CLUSTER_UNDO_AUTHORITY_SERVE_FAIL_CLOSED:
		default:
			/*
			 * Owner liveness unproven / no derivable authority: fail
			 * closed, NEVER the native CLOG/hint path (Rule 8.A).
			 */
			cluster_undo_authority_note_failclosed();
			cluster_rtvis_resolve_note_failclosed();
			return unknown;
		}
	}

	/* master!=self, owner live (or data plane unarmed): CP3 S-grant + CP5
	 * origin verdict, byte-for-byte the pre-D4 path. */
	if (freshref_pair) {
		/*
		 * S8 happy path: freshref exact-live verdict precedes terminal C1b pair.
		 *
		 * A pre-commit ITL stamp can already carry COMMITTED + cached SCN while
		 * the same exact origin {xid, segment, slot} remains in ProcArray.  The
		 * C1b pair is deliberately terminal-only and must keep rejecting its
		 * IN_PROGRESS CLOG row; routing directly to it would therefore turn a
		 * proven live writer into 53R97.  Ask the existing ordinary authoritative
		 * exact-slot verdict first.  Only an unproved result falls through to the
		 * unchanged pair request below.  This creates no new proof kind, authority
		 * or state, and a generic bound remains a bound.
		 */
		if (rtvis_try_resolve_remote_internal(
				origin_node, undo_segment_id, expected_tt_slot_id, raw_xid,
				0, InvalidScn, read_scn, true, &committed, &in_progress,
				&commit_scn, &is_bound)) {
			if (in_progress) {
				ClusterUndoVerdictResult live
					= { .kind = CLUSTER_UNDO_VERDICT_IN_PROGRESS,
						.commit_scn = InvalidScn,
						.wrap = 0 };

				return live;
			}
			return cluster_undo_verdict_from_resolve(true, committed, commit_scn,
											 is_bound);
		}
	}
	if (rtvis_try_resolve_remote_internal(
			origin_node, undo_segment_id, expected_tt_slot_id, raw_xid,
			ref_epoch, freshref_pair_scn, read_scn, authoritative, &committed,
			&in_progress, &commit_scn, &is_bound)) {
		if (in_progress) {
			ClusterUndoVerdictResult live
				= { .kind = CLUSTER_UNDO_VERDICT_IN_PROGRESS,
					.commit_scn = InvalidScn,
					.wrap = 0 };

			return live;
		}
		return cluster_undo_verdict_from_resolve(true, committed, commit_scn, is_bound);
	}
	return unknown;
}

ClusterUndoVerdictResult
cluster_undo_verdict_resolve(int origin_node, uint32 undo_segment_id,
							 TransactionId raw_xid, uint32 expected_tt_slot_id,
							 SCN read_scn, bool authoritative)
{
	return cluster_undo_verdict_resolve_internal(
		origin_node, undo_segment_id, raw_xid, expected_tt_slot_id, read_scn,
		authoritative, InvalidTransactionId, 0, InvalidScn);
}

ClusterUndoVerdictResult
cluster_undo_verdict_resolve_freshref_c1b_pair(
	int origin_node, uint32 undo_segment_id, TransactionId raw_xid,
	TransactionId ref_xid, uint32 expected_tt_slot_id, uint32 ref_epoch,
	SCN cached_commit_scn, SCN read_scn)
{
	return cluster_undo_verdict_resolve_internal(
		origin_node, undo_segment_id, raw_xid, expected_tt_slot_id, read_scn,
		true, ref_xid, ref_epoch, cached_commit_scn);
}

#endif /* USE_PGRAC_CLUSTER */
