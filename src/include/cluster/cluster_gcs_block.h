/*-------------------------------------------------------------------------
 *
 * cluster_gcs_block.h
 *	  pgrac cluster GCS block-shipping substrate (Cache Fusion data plane).
 *
 *	  spec-2.33 activates cross-node 8KB block shipping on top of the
 *	  spec-2.32 GCS control plane (request/reply framework).  Wire opcodes
 *	  PGRAC_IC_MSG_GCS_BLOCK_REQUEST=14 / PGRAC_IC_MSG_GCS_BLOCK_REPLY=15
 *	  carry a 64B request and a 48B header + 8192B page payload, gated by
 *	  the I-WAL-before-ship invariant (master XLogFlush(page_lsn) before
 *	  shipping bytes).
 *
 *	  Scope (FROZEN v0.4):
 *	    - Wire ABI definition (GcsBlockRequestPayload 64B /
 *	      GcsBlockReplyHeader 48B + 8192B block_data)
 *	    - GcsBlockReplyStatus enum (GRANTED / STORAGE_FALLBACK / 4 DENIED /
 *	      DENIED_MASTER_NOT_HOLDER)
 *	    - Sender API cluster_gcs_send_block_request_and_wait (BufferDesc-aware)
 *	    - Master-side handler cluster_gcs_handle_block_request_envelope
 *	      (XLogFlush(page_lsn) before ship + revalidate + memcpy 8192B)
 *	    - Sender-side handler cluster_gcs_handle_block_reply_envelope
 *	      (checksum verify + memcpy + PageSetLSN)
 *	    - postmaster-once registration of msg_type 14/15
 *	    - 4 NEW wait events (BLOCK_REQUEST / BLOCK_REPLY / BLOCK_CHECKSUM_FAIL
 *	      / BLOCK_TIMEOUT) + cluster.gcs_reply_timeout_ms PGC_SUSET GUC
 *
 *	  Forward-link spec-2.34+:
 *	    - Retransmit + reconfig epoch cascading invalidation
 *	    - PI buffer copy + dirty-downgrade-with-writeback (spec-2.35)
 *	    - CF 2-way S-to-S read sharing (spec-2.35)
 *	    - CR / MVCC visibility coupling (spec-2.37+ AD-006 round 5)
 *
 *	  HC contracts in this header (HC79-HC89 11 NEW):
 *	    HC79 NEW msg_type 14/15;  spec-2.32 12/13 untouched
 *	    HC80 wire sizes 64B / 48B / 8192B;  reply key = (backend_id, request_id)
 *	    HC81 deterministic hash mod-N over declared node_id array (sparse safe)
 *	    HC82 master-side XLogFlush(page_lsn) BEFORE block bytes ship
 *	    HC83 CRC32C checksum mandatory; fail-closed; receiver must verify
 *	    HC84 PageSetLSN(page, reply.page_lsn) under content_lock EXCLUSIVE
 *	    HC85 reply timeout via cluster.gcs_reply_timeout_ms PGC_SUSET
 *	    HC86 retransmit deferred to spec-2.34
 *	    HC87 reconfig cascading invalidation deferred to spec-2.34
 *	    HC88 master-not-holder state=N → GRANTED_STORAGE_FALLBACK;
 *	         state != N → DENIED_MASTER_NOT_HOLDER fail-closed;
 *	         transition mutation must NOT precede this decision
 *	    HC89 revalidation single-retry; retry exhausted → fail-closed;
 *	         unbounded loop forbidden (hot-page starvation defense);
 *	         0-retry fail-closed forbidden (normal LSN drift false positive)
 *
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/include/cluster/cluster_gcs_block.h
 *
 * NOTES
 *	  This is a pgrac-original file.
 *	  Spec: spec-2.33-gcs-block-shipping-substrate.md (FROZEN v0.4)
 *	  Design: docs/cache-fusion-protocol-design.md
 *	  AD-005 (Cache Fusion full) + AD-002 (PCM lock state machine)
 *
 *-------------------------------------------------------------------------
 */
#ifndef CLUSTER_GCS_BLOCK_H
#define CLUSTER_GCS_BLOCK_H

#include "c.h"
#include "cluster/cluster_gcs_reqid.h"
#include "cluster/cluster_pcm_lock.h" /* PcmLockTransition */
#include "cluster/cluster_pcm_x_bufmgr.h"
#include "cluster/cluster_semantic_activation.h"
#include "cluster/cluster_sf_dep.h" /* ClusterSfDepVec / max origins */
#include "cluster/cluster_terminal_ref_census.h"
#include "cluster/cluster_tx_resolve.h"
#include "storage/block.h"			/* BLCKSZ */
#include "storage/buf_internals.h"	/* BufferTag, BufferDesc */

#ifdef USE_PGRAC_CLUSTER

/* One bounded, local requester-delivery callback on the existing LMS loop.
 * No wire ownership or foreground success is implied by this result. */
extern ResourceXApplyResult
cluster_gcs_block_resource_x_delivery_tick(const ResourceXAcquisitionRef *ref);

/* Stage 8 8.15-PRE-CAP D1: diagnostic-only failure axes.  These values are
 * process-internal observability, not wire outcomes or a second authority.
 * NONE is retained so successful/unknown observations are never fabricated
 * as one of the four frozen failure domains. */
typedef enum ResourceXRemoteSStage {
	RESOURCE_X_REMOTE_S_STAGE_NONE = 0,
	RESOURCE_X_REMOTE_S_STAGE_VALIDATE,
	RESOURCE_X_REMOTE_S_STAGE_SNAPSHOT,
	RESOURCE_X_REMOTE_S_STAGE_REVOKE_HELD,
	RESOURCE_X_REMOTE_S_STAGE_PENDING_STAGED,
	RESOURCE_X_REMOTE_S_STAGE_COMMITTED_N,
	RESOURCE_X_REMOTE_S_STAGE_READY_PUBLISHED
} ResourceXRemoteSStage;

typedef enum ResourceXFailureDomain {
	RESOURCE_X_FAIL_NONE = 0,
	RESOURCE_X_FAIL_PRE_MUTATION_BACKPRESSURE,
	RESOURCE_X_FAIL_AUTHORITY_DRIFT,
	RESOURCE_X_FAIL_POST_MUTATION_AMBIGUITY,
	RESOURCE_X_FAIL_INTERNAL_CORRUPTION
} ResourceXFailureDomain;

typedef struct ResourceXRemoteSFailureDecision {
	ResourceXFailureDomain domain;
	bool global_fail_closed;
	bool discard_old_round;
	bool rollback_complete;
} ResourceXRemoteSFailureDecision;

/* A clean requester N snapshot can race the same-node fan-in executor that
 * installs the canonical terminal X between two BufferDesc header-lock
 * acquisitions.  This predicate grants nothing: it permits only a fresh
 * resample when the independently validated terminal-round snapshot names
 * that exact new X generation.  The driver must loop and revalidate both
 * lock domains before returning the terminal reference. */
static inline bool
cluster_gcs_resource_x_target_terminal_resample_exact(
	ClusterPcmOwnResult candidate_result,
	const ClusterPcmOwnSnapshot *before_n,
	const ClusterPcmOwnSnapshot *live_x,
	ResourceXApplyResult round_snapshot_result,
	const ResourceXBootstrapRoundFailureSnapshot *round)
{
	if (candidate_result != CLUSTER_PCM_OWN_STALE
		|| before_n == NULL || live_x == NULL || round == NULL
		|| round_snapshot_result != RESOURCE_X_APPLY_APPLIED
		|| round->terminal != 1)
		return false;
	if (!BufferTagsEqual(&before_n->tag, &live_x->tag)
		|| !BufferTagsEqual(&live_x->tag,
			&round->ref.assertion.resource)
		|| before_n->pcm_state != (uint8) PCM_STATE_N
		|| before_n->flags != 0
		|| before_n->generation == UINT64_MAX
		|| before_n->reservation_token == UINT64_MAX
		|| before_n->writer_activation_token != 0
		|| before_n->resource_x_activation_generation != 0
		|| live_x->pcm_state != (uint8) PCM_STATE_X
		|| live_x->flags != 0
		|| live_x->generation == 0
		|| live_x->generation == UINT64_MAX
		|| live_x->generation == before_n->generation
		|| live_x->reservation_token == 0
		|| live_x->reservation_token == UINT64_MAX
		|| live_x->writer_activation_token != 0
		|| live_x->resource_x_activation_generation != 0)
		return false;
	return round->buffer_ownership_generation == live_x->generation
		&& round->ref.assertion.requester_node >= 0
		&& round->ref.formation != 0
		&& round->ref.formation != UINT64_MAX
		&& round->ref.acquisition_generation != 0
		&& round->ref.acquisition_generation != UINT64_MAX
		&& round->base_authority_generation != 0
		&& round->base_authority_generation != UINT64_MAX
		&& round->authority_generation
			> round->base_authority_generation
		&& round->authority_generation != UINT64_MAX
		&& round->r4_record_generation != 0
		&& round->r4_record_generation != UINT64_MAX
		&& round->master_session_incarnation != 0
		&& round->master_session_incarnation != UINT64_MAX
		&& round->absolute_deadline_us != 0
		&& round->absolute_deadline_us != UINT64_MAX
		&& round->retired_acquisition_generation
			== round->ref.acquisition_generation
		&& round->progress_flags == 0;
}

/* The same-node T1 executor can finish its exact N+GRANT_PENDING install and
 * publish TERMINAL_X_CACHED after a follower sampled the BufferDesc but before
 * the follower inspected the requester round.  The pending snapshot is not a
 * second authority.  Permit only a fresh driver iteration when one ownership
 * generation, the unchanged reservation token, and the independently frozen
 * terminal round all identify the same completed install. */
#define RESOURCE_X_PENDING_TERMINAL_MISMATCH_NONE UINT64_C(0)
#define RESOURCE_X_PENDING_TERMINAL_MISMATCH_INPUT (UINT64_C(1) << 0)
#define RESOURCE_X_PENDING_TERMINAL_MISMATCH_ROUND_RESULT (UINT64_C(1) << 1)
#define RESOURCE_X_PENDING_TERMINAL_MISMATCH_ROUND_TERMINAL (UINT64_C(1) << 2)
#define RESOURCE_X_PENDING_TERMINAL_MISMATCH_TAG (UINT64_C(1) << 3)
#define RESOURCE_X_PENDING_TERMINAL_MISMATCH_BEFORE_STATE (UINT64_C(1) << 4)
#define RESOURCE_X_PENDING_TERMINAL_MISMATCH_BEFORE_FLAGS (UINT64_C(1) << 5)
#define RESOURCE_X_PENDING_TERMINAL_MISMATCH_BEFORE_GENERATION (UINT64_C(1) << 6)
#define RESOURCE_X_PENDING_TERMINAL_MISMATCH_BEFORE_TOKEN (UINT64_C(1) << 7)
#define RESOURCE_X_PENDING_TERMINAL_MISMATCH_BEFORE_ACTIVATION (UINT64_C(1) << 8)
#define RESOURCE_X_PENDING_TERMINAL_MISMATCH_LIVE_STATE (UINT64_C(1) << 9)
#define RESOURCE_X_PENDING_TERMINAL_MISMATCH_LIVE_FLAGS (UINT64_C(1) << 10)
#define RESOURCE_X_PENDING_TERMINAL_MISMATCH_LIVE_GENERATION (UINT64_C(1) << 11)
#define RESOURCE_X_PENDING_TERMINAL_MISMATCH_LIVE_TOKEN (UINT64_C(1) << 12)
#define RESOURCE_X_PENDING_TERMINAL_MISMATCH_LIVE_ACTIVATION (UINT64_C(1) << 13)
#define RESOURCE_X_PENDING_TERMINAL_MISMATCH_ROUND_BUFFER_GENERATION (UINT64_C(1) << 14)
#define RESOURCE_X_PENDING_TERMINAL_MISMATCH_ROUND_REQUESTER (UINT64_C(1) << 15)
#define RESOURCE_X_PENDING_TERMINAL_MISMATCH_ROUND_FORMATION (UINT64_C(1) << 16)
#define RESOURCE_X_PENDING_TERMINAL_MISMATCH_ROUND_ACQUISITION (UINT64_C(1) << 17)
#define RESOURCE_X_PENDING_TERMINAL_MISMATCH_ROUND_BASE_AUTHORITY (UINT64_C(1) << 18)
#define RESOURCE_X_PENDING_TERMINAL_MISMATCH_ROUND_AUTHORITY (UINT64_C(1) << 19)
#define RESOURCE_X_PENDING_TERMINAL_MISMATCH_ROUND_R4 (UINT64_C(1) << 20)
#define RESOURCE_X_PENDING_TERMINAL_MISMATCH_ROUND_SESSION (UINT64_C(1) << 21)
#define RESOURCE_X_PENDING_TERMINAL_MISMATCH_ROUND_DEADLINE (UINT64_C(1) << 22)
#define RESOURCE_X_PENDING_TERMINAL_MISMATCH_ROUND_RETIRED (UINT64_C(1) << 23)
#define RESOURCE_X_PENDING_TERMINAL_MISMATCH_ROUND_PROGRESS (UINT64_C(1) << 24)

/* Diagnostic decomposition of the admission predicate below.  It is neither
 * a wire value nor a retry policy: callers may log the mask, but only a zero
 * mask is the exact stable cover that permits a fresh driver iteration. */
static inline uint64
cluster_gcs_resource_x_pending_terminal_resample_mismatch(
	const ClusterPcmOwnSnapshot *before_pending,
	const ClusterPcmOwnSnapshot *live_x,
	ResourceXApplyResult round_snapshot_result,
	const ResourceXBootstrapRoundFailureSnapshot *round)
{
	uint64 mismatch = RESOURCE_X_PENDING_TERMINAL_MISMATCH_NONE;

	if (round_snapshot_result != RESOURCE_X_APPLY_APPLIED)
		mismatch |= RESOURCE_X_PENDING_TERMINAL_MISMATCH_ROUND_RESULT;
	if (before_pending == NULL || live_x == NULL || round == NULL)
		return mismatch | RESOURCE_X_PENDING_TERMINAL_MISMATCH_INPUT;
	if (round->terminal != 1)
		mismatch |= RESOURCE_X_PENDING_TERMINAL_MISMATCH_ROUND_TERMINAL;
	if (!BufferTagsEqual(&before_pending->tag, &live_x->tag)
		|| !BufferTagsEqual(&live_x->tag, &round->ref.assertion.resource))
		mismatch |= RESOURCE_X_PENDING_TERMINAL_MISMATCH_TAG;
	if (before_pending->pcm_state != (uint8) PCM_STATE_N)
		mismatch |= RESOURCE_X_PENDING_TERMINAL_MISMATCH_BEFORE_STATE;
	if (before_pending->flags != PCM_OWN_FLAG_GRANT_PENDING)
		mismatch |= RESOURCE_X_PENDING_TERMINAL_MISMATCH_BEFORE_FLAGS;
	if (before_pending->generation >= UINT64_MAX - 1)
		mismatch |= RESOURCE_X_PENDING_TERMINAL_MISMATCH_BEFORE_GENERATION;
	if (before_pending->reservation_token == 0
		|| before_pending->reservation_token == UINT64_MAX)
		mismatch |= RESOURCE_X_PENDING_TERMINAL_MISMATCH_BEFORE_TOKEN;
	if (before_pending->writer_activation_token != 0
		|| before_pending->resource_x_activation_generation != 0)
		mismatch |= RESOURCE_X_PENDING_TERMINAL_MISMATCH_BEFORE_ACTIVATION;
	if (live_x->pcm_state != (uint8) PCM_STATE_X)
		mismatch |= RESOURCE_X_PENDING_TERMINAL_MISMATCH_LIVE_STATE;
	if (live_x->flags != 0)
		mismatch |= RESOURCE_X_PENDING_TERMINAL_MISMATCH_LIVE_FLAGS;
	if (before_pending->generation >= UINT64_MAX - 1
		|| live_x->generation != before_pending->generation + 1)
		mismatch |= RESOURCE_X_PENDING_TERMINAL_MISMATCH_LIVE_GENERATION;
	if (live_x->reservation_token != before_pending->reservation_token)
		mismatch |= RESOURCE_X_PENDING_TERMINAL_MISMATCH_LIVE_TOKEN;
	if (live_x->writer_activation_token != 0
		|| live_x->resource_x_activation_generation != 0)
		mismatch |= RESOURCE_X_PENDING_TERMINAL_MISMATCH_LIVE_ACTIVATION;
	if (round->buffer_ownership_generation != live_x->generation)
		mismatch |= RESOURCE_X_PENDING_TERMINAL_MISMATCH_ROUND_BUFFER_GENERATION;
	if (round->ref.assertion.requester_node < 0)
		mismatch |= RESOURCE_X_PENDING_TERMINAL_MISMATCH_ROUND_REQUESTER;
	if (round->ref.formation == 0 || round->ref.formation == UINT64_MAX)
		mismatch |= RESOURCE_X_PENDING_TERMINAL_MISMATCH_ROUND_FORMATION;
	if (round->ref.acquisition_generation == 0
		|| round->ref.acquisition_generation == UINT64_MAX)
		mismatch |= RESOURCE_X_PENDING_TERMINAL_MISMATCH_ROUND_ACQUISITION;
	if (round->base_authority_generation == 0
		|| round->base_authority_generation == UINT64_MAX)
		mismatch |= RESOURCE_X_PENDING_TERMINAL_MISMATCH_ROUND_BASE_AUTHORITY;
	if (round->authority_generation <= round->base_authority_generation
		|| round->authority_generation == UINT64_MAX)
		mismatch |= RESOURCE_X_PENDING_TERMINAL_MISMATCH_ROUND_AUTHORITY;
	if (round->r4_record_generation == 0
		|| round->r4_record_generation == UINT64_MAX)
		mismatch |= RESOURCE_X_PENDING_TERMINAL_MISMATCH_ROUND_R4;
	if (round->master_session_incarnation == 0
		|| round->master_session_incarnation == UINT64_MAX)
		mismatch |= RESOURCE_X_PENDING_TERMINAL_MISMATCH_ROUND_SESSION;
	if (round->absolute_deadline_us == 0
		|| round->absolute_deadline_us == UINT64_MAX)
		mismatch |= RESOURCE_X_PENDING_TERMINAL_MISMATCH_ROUND_DEADLINE;
	if (round->retired_acquisition_generation
		!= round->ref.acquisition_generation)
		mismatch |= RESOURCE_X_PENDING_TERMINAL_MISMATCH_ROUND_RETIRED;
	if (round->progress_flags != 0)
		mismatch |= RESOURCE_X_PENDING_TERMINAL_MISMATCH_ROUND_PROGRESS;
	return mismatch;
}

static inline bool
cluster_gcs_resource_x_target_pending_terminal_resample_exact(
	const ClusterPcmOwnSnapshot *before_pending,
	const ClusterPcmOwnSnapshot *live_x,
	ResourceXApplyResult round_snapshot_result,
	const ResourceXBootstrapRoundFailureSnapshot *round)
{
	return cluster_gcs_resource_x_pending_terminal_resample_mismatch(
		before_pending, live_x, round_snapshot_result, round)
		== RESOURCE_X_PENDING_TERMINAL_MISMATCH_NONE;
}

/* A legacy local acquire and the target-only Resource-X driver serialize
 * through the same BufferDesc reservation word.  The driver can sample any
 * edge of one unrelated reversible N reservation: clean->GRANT_PENDING,
 * GRANT_PENDING->clean, or a complete clean->clean token advance.  None
 * grants authority or carries page proof.  Accept only an unchanged N
 * generation, monotone token and zero activation fields, then permit one
 * bounded sleep/reprobe under the caller's original R7 deadline.  The next
 * iteration validates the successor through its canonical predicate. */
static inline bool
cluster_gcs_resource_x_target_local_n_reservation_retry_exact(
	const ClusterPcmOwnSnapshot *before,
	ClusterPcmOwnResult live_result,
	const ClusterPcmOwnSnapshot *live,
	uint64 now_us, uint64 absolute_deadline_us)
{
	bool before_pending;

	if (before == NULL || live == NULL
		|| now_us == 0 || now_us == UINT64_MAX
		|| absolute_deadline_us == 0
		|| absolute_deadline_us == UINT64_MAX
		|| now_us >= absolute_deadline_us)
		return false;
	before_pending = before->flags == PCM_OWN_FLAG_GRANT_PENDING;
	if ((!before_pending && before->flags != 0)
		|| live_result != (before_pending
			? CLUSTER_PCM_OWN_OK : CLUSTER_PCM_OWN_STALE)
		|| !BufferTagsEqual(&before->tag, &live->tag)
		|| before->pcm_state != (uint8) PCM_STATE_N
		|| live->pcm_state != (uint8) PCM_STATE_N
		|| (live->flags != 0
			&& live->flags != PCM_OWN_FLAG_GRANT_PENDING)
		|| before->generation == UINT64_MAX
		|| live->generation != before->generation
		|| before->reservation_token == UINT64_MAX
		|| live->reservation_token == UINT64_MAX
		|| before->writer_activation_token != 0
		|| live->writer_activation_token != 0
		|| before->resource_x_activation_generation != 0
		|| live->resource_x_activation_generation != 0)
		return false;
	if (before_pending)
		return before->reservation_token != 0
			&& live->reservation_token >= before->reservation_token
			&& (live->flags == 0 || live->reservation_token != 0);
	return live->reservation_token > before->reservation_token
		&& (live->flags == 0 || live->reservation_token != 0);
}

/* A direct-init reservation remains non-authoritative.  Durable storage is
 * the ordinary known-new proof; the sole auxiliary exception permits an
 * already-joined remote carrier only for VM/FSM, whose exact retained image
 * is installed by the existing Resource-X target path before X commit. */
static inline bool
GcsBlockResourceXDirectInitProofAllowedExact(
	const BufferTag *tag, bool durable_proof, bool remote_proof)
{
	if (tag == NULL || durable_proof == remote_proof)
		return false;
	if (durable_proof)
		return true;
	return remote_proof
		&& (tag->forkNum == VISIBILITYMAP_FORKNUM
			|| tag->forkNum == FSM_FORKNUM);
}

/* A clean cached X observation can race one complete type-17 X-to-N
 * settlement before this requester creates a bootstrap round.  This
 * predicate grants no authority and preserves no evidence: it permits only
 * one fresh driver iteration when the current descriptor is the exact
 * one-generation/one-token clean N successor, the round is still absent,
 * the sampled authority tuple is still current, and the original deadline
 * remains live. */
static inline bool
cluster_gcs_resource_x_target_empty_round_drift_retry_exact(
	ResourceXBootstrapRoundAction action,
	bool direct_init, bool join_only,
	const ClusterPcmOwnSnapshot *before_x,
	ClusterPcmOwnResult live_result,
	const ClusterPcmOwnSnapshot *live_n,
	ResourceXApplyResult round_snapshot_result,
	bool authority_current,
	uint64 now_us, uint64 absolute_deadline_us)
{
	if (action != RESOURCE_X_BOOTSTRAP_ROUND_FAIL_CLOSED
		|| direct_init || join_only
		|| before_x == NULL || live_n == NULL
		|| live_result != CLUSTER_PCM_OWN_OK
		|| round_snapshot_result != RESOURCE_X_APPLY_NOT_FOUND
		|| !authority_current
		|| now_us == 0 || now_us == UINT64_MAX
		|| absolute_deadline_us == 0
		|| absolute_deadline_us == UINT64_MAX
		|| now_us >= absolute_deadline_us)
		return false;
	if (!BufferTagsEqual(&before_x->tag, &live_n->tag)
		|| before_x->pcm_state != (uint8) PCM_STATE_X
		|| before_x->flags != 0
		|| before_x->generation == 0
		|| before_x->generation >= UINT64_MAX - 1
		|| before_x->reservation_token == 0
		|| before_x->reservation_token >= UINT64_MAX - 1
		|| before_x->writer_activation_token != 0
		|| before_x->resource_x_activation_generation != 0
		|| live_n->pcm_state != (uint8) PCM_STATE_N
		|| live_n->flags != 0
		|| live_n->generation != before_x->generation + 1
		|| live_n->reservation_token
			!= before_x->reservation_token + 1
		|| live_n->writer_activation_token != 0
		|| live_n->resource_x_activation_generation != 0)
		return false;
	return true;
}

/* A cached-X observation may race the physical half of an exact predecessor
 * conversion.  The retained pair and N+PI carrier are independent current
 * witnesses; together they authorize no transition, only one fresh driver
 * iteration under the original deadline. */
static inline bool
cluster_gcs_resource_x_target_retained_predecessor_retry_exact(
	ResourceXBootstrapRoundAction action,
	bool direct_init, bool join_only,
	const ClusterPcmOwnSnapshot *before_x,
	ClusterPcmOwnResult live_result,
	const ClusterPcmOwnSnapshot *live_n,
	bool retained_pair_exact, bool retained_buffer_exact,
	bool authority_current,
	uint64 now_us, uint64 absolute_deadline_us)
{
	if (action != RESOURCE_X_BOOTSTRAP_ROUND_FAIL_CLOSED
		|| direct_init || join_only
		|| before_x == NULL || live_n == NULL
		|| live_result != CLUSTER_PCM_OWN_OK
		|| !retained_pair_exact || !retained_buffer_exact
		|| !authority_current
		|| now_us == 0 || now_us == UINT64_MAX
		|| absolute_deadline_us == 0
		|| absolute_deadline_us == UINT64_MAX
		|| now_us >= absolute_deadline_us)
		return false;
	if (!BufferTagsEqual(&before_x->tag, &live_n->tag)
		|| before_x->pcm_state != (uint8) PCM_STATE_X
		|| before_x->flags != 0
		|| before_x->generation == 0
		|| before_x->generation >= UINT64_MAX - 1
		|| before_x->reservation_token == 0
		|| before_x->reservation_token == UINT64_MAX
		|| before_x->writer_activation_token != 0
		|| before_x->resource_x_activation_generation != 0
		|| live_n->pcm_state != (uint8) PCM_STATE_N
		|| (live_n->flags != PCM_OWN_FLAG_REVOKING
			&& live_n->flags != 0)
		|| live_n->generation != before_x->generation + 1
		|| live_n->reservation_token == 0
		|| live_n->reservation_token == UINT64_MAX
		|| live_n->reservation_token <= before_x->reservation_token
		|| live_n->writer_activation_token != 0
		|| live_n->resource_x_activation_generation != 0)
		return false;
	return true;
}

/* An exact undrained retained pair may coexist briefly with the clean
 * N+CURRENT successor produced by VM/FSM DROP.  This classifier grants no
 * authority and does not consume the pair: it only permits the ordinary
 * empty bootstrap-round step to observe the predecessor and return
 * PREDECESSOR_WAIT.  N+PI remains owned by SourceSettlement's physical
 * retained-release interlock. */
static inline bool
cluster_gcs_resource_x_target_undrained_current_predecessor_exact(
	bool retained_pair_exact, bool retained_buffer_exact,
	ClusterPcmOwnResult current_candidate_result,
	const ClusterPcmOwnSnapshot *current_n)
{
	if (!retained_pair_exact || retained_buffer_exact
		|| current_candidate_result != CLUSTER_PCM_OWN_OK
		|| current_n == NULL
		|| current_n->pcm_state != (uint8) PCM_STATE_N
		|| current_n->flags != 0
		|| current_n->generation == 0
		|| current_n->generation == UINT64_MAX
		|| current_n->reservation_token == 0
		|| current_n->reservation_token == UINT64_MAX
		|| current_n->writer_activation_token != 0
		|| current_n->resource_x_activation_generation != 0)
		return false;
	return true;
}

/* D1 observability for the final requester-side dispatch fence.  Each bit
 * names one already-frozen predicate; the mask changes no retry or failure
 * policy and carries no authority. */
#define RESOURCE_X_DISPATCH_RECHECK_OK UINT32_C(0)
#define RESOURCE_X_DISPATCH_RECHECK_ADMISSION UINT32_C(0x00000001)
#define RESOURCE_X_DISPATCH_RECHECK_GATE_SESSION UINT32_C(0x00000002)
#define RESOURCE_X_DISPATCH_RECHECK_REQUESTER_SAMPLE UINT32_C(0x00000004)
#define RESOURCE_X_DISPATCH_RECHECK_MASTER_SAMPLE UINT32_C(0x00000008)
#define RESOURCE_X_DISPATCH_RECHECK_REQUESTER_GENERATION UINT32_C(0x00000010)
#define RESOURCE_X_DISPATCH_RECHECK_MASTER_GENERATION UINT32_C(0x00000020)

/* The type-17 DATA handler may run without an ordinary backend PGPROC in an
 * auxiliary event loop.  The local-owner field is an opaque,
 * process-local equality token only: prefer the backend slot when present,
 * otherwise use the current positive PID.  It is never wire or authority. */
static inline int32
cluster_gcs_resource_x_event_owner_identity(int32 procno, int32 process_pid)
{
	if (procno >= 0)
		return procno;
	return process_pid > 0 ? process_pid : -1;
}

static inline uint32
cluster_gcs_resource_x_dispatch_recheck_failure_mask(
	bool admission_current, bool gate_session_current,
	bool requester_sampled, bool master_sampled,
	uint32 expected_requester_generation,
	uint32 expected_master_generation,
	uint32 observed_requester_generation,
	uint32 observed_master_generation)
{
	uint32 mask = RESOURCE_X_DISPATCH_RECHECK_OK;

	if (!admission_current)
		mask |= RESOURCE_X_DISPATCH_RECHECK_ADMISSION;
	if (!gate_session_current)
		mask |= RESOURCE_X_DISPATCH_RECHECK_GATE_SESSION;
	if (!requester_sampled)
		mask |= RESOURCE_X_DISPATCH_RECHECK_REQUESTER_SAMPLE;
	if (!master_sampled)
		mask |= RESOURCE_X_DISPATCH_RECHECK_MASTER_SAMPLE;
	if (observed_requester_generation != expected_requester_generation)
		mask |= RESOURCE_X_DISPATCH_RECHECK_REQUESTER_GENERATION;
	if (observed_master_generation != expected_master_generation)
		mask |= RESOURCE_X_DISPATCH_RECHECK_MASTER_GENERATION;
	return mask;
}

/* D6 keeps result mapping separate from the safety policy.  The caller first
 * performs every rollback operation required by the frozen remote-S stage,
 * then feeds the observed outcomes here.  Missing or failed rollback after a
 * reversible holder mutation is ambiguous; once S has committed to N, every
 * publication failure is ambiguous.  This helper owns no state and grants no
 * authority. */
static inline ResourceXRemoteSFailureDecision
cluster_gcs_resource_x_remote_s_failure_decide(
	ResourceXRemoteSStage stage, ResourceXFailureDomain cause_domain,
	bool cancel_attempted, bool cancel_ok,
	bool abort_attempted, bool abort_ok)
{
	ResourceXRemoteSFailureDecision decision;

	decision.domain = RESOURCE_X_FAIL_INTERNAL_CORRUPTION;
	decision.global_fail_closed = true;
	decision.discard_old_round = false;
	decision.rollback_complete = false;
	if (cause_domain <= RESOURCE_X_FAIL_NONE
		|| cause_domain > RESOURCE_X_FAIL_INTERNAL_CORRUPTION)
		return decision;

	switch (stage) {
	case RESOURCE_X_REMOTE_S_STAGE_VALIDATE:
	case RESOURCE_X_REMOTE_S_STAGE_SNAPSHOT:
		if (cancel_attempted || abort_attempted)
			return decision;
		decision.rollback_complete = true;
		break;
	case RESOURCE_X_REMOTE_S_STAGE_REVOKE_HELD:
		if (cancel_attempted || !abort_attempted)
			return decision;
		if (!abort_ok) {
			decision.domain
				= RESOURCE_X_FAIL_POST_MUTATION_AMBIGUITY;
			return decision;
		}
		decision.rollback_complete = true;
		break;
	case RESOURCE_X_REMOTE_S_STAGE_PENDING_STAGED:
		if (!cancel_attempted || !abort_attempted)
			return decision;
		if (!cancel_ok || !abort_ok) {
			decision.domain
				= RESOURCE_X_FAIL_POST_MUTATION_AMBIGUITY;
			return decision;
		}
		decision.rollback_complete = true;
		break;
	case RESOURCE_X_REMOTE_S_STAGE_COMMITTED_N:
		if (cancel_attempted || abort_attempted)
			return decision;
		decision.domain = RESOURCE_X_FAIL_POST_MUTATION_AMBIGUITY;
		return decision;
	case RESOURCE_X_REMOTE_S_STAGE_NONE:
	case RESOURCE_X_REMOTE_S_STAGE_READY_PUBLISHED:
	default:
		return decision;
	}

	decision.domain = cause_domain;
	decision.global_fail_closed
		= cause_domain == RESOURCE_X_FAIL_POST_MUTATION_AMBIGUITY
		  || cause_domain == RESOURCE_X_FAIL_INTERNAL_CORRUPTION;
	decision.discard_old_round
		= cause_domain == RESOURCE_X_FAIL_AUTHORITY_DRIFT;
	return decision;
}

/* One lock-free authority observation.  The qvotec slot tuple and the
 * connection capability record have different publishers, so consumers must
 * sample both ends and reject a change as retryable rather than accepting a
 * half-old identity.  Zero is a valid value for INITIAL epoch and for the
 * registered RDMA connection generation; the explicit *_valid bits carry
 * presence. */
typedef struct ClusterGcsPcmXAuthSample {
	uint64 session_before;
	uint64 session_after;
	uint64 slot_generation_before;
	uint64 slot_generation_after;
	uint64 observed_epoch_before;
	uint64 observed_epoch_after;
	uint32 connection_generation_before;
	uint32 connection_generation_after;
	bool connection_before_valid;
	bool connection_after_valid;
	bool slot_before_valid;
	bool slot_after_valid;
	bool fresh_before;
	bool fresh_after;
} ClusterGcsPcmXAuthSample;

typedef enum PcmXSessionAuthResult {
	PCM_X_SESSION_AUTH_INVALID = 0,
	PCM_X_SESSION_AUTH_OK,
	PCM_X_SESSION_AUTH_CONNECTION_NOT_READY,
	PCM_X_SESSION_AUTH_SLOT_NOT_READY,
	PCM_X_SESSION_AUTH_EPOCH_NOT_READY,
	PCM_X_SESSION_AUTH_FRESH_NOT_READY,
	PCM_X_SESSION_AUTH_SLOT_TORN,
	PCM_X_SESSION_AUTH_EPOCH_TORN,
	PCM_X_SESSION_AUTH_CONNECTION_TORN
} PcmXSessionAuthResult;

static inline PcmXSessionAuthResult
cluster_gcs_pcm_x_auth_sample_classify(const ClusterGcsPcmXAuthSample *sample,
									   uint64 expected_epoch)
{
	if (sample == NULL)
		return PCM_X_SESSION_AUTH_INVALID;
	if (!sample->connection_before_valid || !sample->connection_after_valid)
		return PCM_X_SESSION_AUTH_CONNECTION_NOT_READY;
	if (!sample->slot_before_valid || !sample->slot_after_valid || sample->session_before == 0
		|| sample->session_after == 0 || sample->slot_generation_before == 0
		|| sample->slot_generation_after == 0)
		return PCM_X_SESSION_AUTH_SLOT_NOT_READY;
	if (sample->observed_epoch_before != expected_epoch
		|| sample->observed_epoch_after != expected_epoch)
		return sample->observed_epoch_before == sample->observed_epoch_after
				   ? PCM_X_SESSION_AUTH_EPOCH_NOT_READY
				   : PCM_X_SESSION_AUTH_EPOCH_TORN;
	if (!sample->fresh_before || !sample->fresh_after)
		return PCM_X_SESSION_AUTH_FRESH_NOT_READY;
	if (sample->session_before != sample->session_after
		|| sample->slot_generation_before != sample->slot_generation_after)
		return PCM_X_SESSION_AUTH_SLOT_TORN;
	if (sample->observed_epoch_before != sample->observed_epoch_after)
		return PCM_X_SESSION_AUTH_EPOCH_TORN;
	if (sample->connection_generation_before != sample->connection_generation_after)
		return PCM_X_SESSION_AUTH_CONNECTION_TORN;
	return PCM_X_SESSION_AUTH_OK;
}

/* A complete non-OK sample is retryable only before a Resource-X round has
 * been created or any holder mutation has started.  The caller retains the
 * first absolute deadline and must still recheck admission, gate, master,
 * session and transport identity on every iteration. */
static inline bool
cluster_gcs_pcm_x_auth_result_retryable(PcmXSessionAuthResult result)
{
	return result >= PCM_X_SESSION_AUTH_CONNECTION_NOT_READY
		&& result <= PCM_X_SESSION_AUTH_CONNECTION_TORN;
}

/* ============================================================
 * GCS_BLOCK_DATA_SIZE -- block bytes carried in every reply.
 *
 *  Locked to BLCKSZ at compile time; StaticAssertDecl in cluster_gcs_block.c
 *  enforces equality.  HC80 anchors this at 8192B per spec-2.33 v0.4.
 * ============================================================ */
#define GCS_BLOCK_DATA_SIZE 8192

/*
 * Keep the per-backend outstanding-block cap visible to the RDMA direct-land
 * lane: wr_id/sidecar arm ids are derived from backend_idx * cap + slot_idx
 * so LMON can demux a completion without scanning every backend.
 */
#define CLUSTER_GCS_BLOCK_MAX_OUTSTANDING_PER_BACKEND 8

/*
 * spec-2.35 HC108/HC109: forwarding_master_node_bytes stores the master that
 * authorized a holder-to-requester direct ship.  Node 0 is a valid cluster
 * node, so the direct-from-master sentinel must be outside the legal node-id
 * range.
 */
#define GCS_BLOCK_REPLY_NO_FORWARDING_MASTER (-1)

typedef enum ClusterGcsBlockDirectState {
	GCS_BLOCK_DIRECT_UNARMED = 0,
	GCS_BLOCK_DIRECT_ARMING,
	GCS_BLOCK_DIRECT_ARMED,
	GCS_BLOCK_DIRECT_LANDED,
	GCS_BLOCK_DIRECT_INSTALLED,
	GCS_BLOCK_DIRECT_ABORTING,
	GCS_BLOCK_DIRECT_ABORTED,
} ClusterGcsBlockDirectState;

typedef enum ClusterGcsBlockDirectTargetKind {
	GCS_BLOCK_DIRECT_TARGET_NONE = 0,
	GCS_BLOCK_DIRECT_TARGET_SHARED_BUFFER,
	GCS_BLOCK_DIRECT_TARGET_STAGING_PAGE,
} ClusterGcsBlockDirectTargetKind;

typedef enum ClusterGcsBlockDirectAbortReason {
	GCS_BLOCK_DIRECT_ABORT_NONE = 0,
	GCS_BLOCK_DIRECT_ABORT_ARM_FAILED,
	GCS_BLOCK_DIRECT_ABORT_CQE_ERROR,
	GCS_BLOCK_DIRECT_ABORT_BAD_LENGTH,
	GCS_BLOCK_DIRECT_ABORT_BAD_SIDECAR,
	GCS_BLOCK_DIRECT_ABORT_BAD_STATUS,
	GCS_BLOCK_DIRECT_ABORT_BAD_IDENTITY,
	GCS_BLOCK_DIRECT_ABORT_BAD_CHECKSUM,
	GCS_BLOCK_DIRECT_ABORT_TIMEOUT,
	GCS_BLOCK_DIRECT_ABORT_PEER_DOWN,
} ClusterGcsBlockDirectAbortReason;

static inline bool
cluster_gcs_block_direct_state_transition_ok(ClusterGcsBlockDirectState from,
											 ClusterGcsBlockDirectState to)
{
	switch (from) {
	case GCS_BLOCK_DIRECT_UNARMED:
		return to == GCS_BLOCK_DIRECT_ARMING;
	case GCS_BLOCK_DIRECT_ARMING:
		return to == GCS_BLOCK_DIRECT_ARMED || to == GCS_BLOCK_DIRECT_UNARMED
			   || to == GCS_BLOCK_DIRECT_ABORTED;
	case GCS_BLOCK_DIRECT_ARMED:
		return to == GCS_BLOCK_DIRECT_LANDED || to == GCS_BLOCK_DIRECT_ABORTING
			   || to == GCS_BLOCK_DIRECT_ABORTED;
	case GCS_BLOCK_DIRECT_LANDED:
		return to == GCS_BLOCK_DIRECT_INSTALLED || to == GCS_BLOCK_DIRECT_ABORTED;
	case GCS_BLOCK_DIRECT_ABORTING:
		return to == GCS_BLOCK_DIRECT_ABORTED;
	case GCS_BLOCK_DIRECT_INSTALLED:
	case GCS_BLOCK_DIRECT_ABORTED:
		return to == GCS_BLOCK_DIRECT_UNARMED;
	}
	return false;
}

/*
 * A current-master INVALIDATE that meets a node-local mirror-N
 * GRANT_PENDING is also an authoritative reason for the one exact N->S
 * attempt to stand down: that reader cannot be granted while this X transfer
 * is invalidating the node.  Keep the delivery predicate attempt-exact and
 * refuse any live direct-land target.  The caller only synthesizes a local
 * DENIED_PENDING_X; the INVALIDATE itself still returns RETRYABLE_BUSY until
 * the owning backend aborts its reservation, so no holder bit is credited.
 */
static inline bool
GcsBlockLocalPendingSDenialMatches(bool in_use, bool reply_received, bool stale,
								   uint8 transition_id, const BufferTag *slot_tag,
								   uint64 request_epoch, int32 expected_master_node,
								   ClusterGcsBlockDirectState direct_state,
								   bool direct_target_prepared, const BufferTag *invalidate_tag,
								   uint64 invalidate_epoch, int32 invalidate_master_node)
{
	return in_use && !reply_received && !stale && slot_tag != NULL && invalidate_tag != NULL
		   && transition_id == (uint8)PCM_TRANS_N_TO_S && BufferTagsEqual(slot_tag, invalidate_tag)
		   && request_epoch == invalidate_epoch && expected_master_node == invalidate_master_node
		   && !direct_target_prepared
		   && (direct_state == GCS_BLOCK_DIRECT_UNARMED
			   || direct_state == GCS_BLOCK_DIRECT_ABORTED);
}


/* ============================================================
 * GcsBlockReplyStatus -- reply status code carried in
 * GcsBlockReplyHeader.status (HC83 + HC88).
 *
 *  GRANTED                     transition applied, block bytes valid
 *  GRANTED_STORAGE_FALLBACK    master state=N, no holder; requester keeps
 *                              shared-storage page (HC88 N_TO_S/N_TO_X only;
 *                              cross-node X→N→evict dirty deferred to spec-2.35)
 *  DENIED_INCOMPATIBLE         transition apply rejected (state conflict)
 *  DENIED_VALIDATOR_REJECT     HC75 transition_id illegal
 *  DENIED_EPOCH_STALE          request epoch < current cluster_epoch
 *  DENIED_CHECKSUM_FAIL        (sender-side derived; not master-emitted)
 *  DENIED_MASTER_NOT_HOLDER    master state != N and no buffer (HC88) OR
 *                              HC89 revalidation single-retry exhausted
 * ============================================================ */
typedef enum GcsBlockReplyStatus {
	GCS_BLOCK_REPLY_GRANTED = 0,
	GCS_BLOCK_REPLY_GRANTED_STORAGE_FALLBACK = 1,
	GCS_BLOCK_REPLY_DENIED_INCOMPATIBLE = 2,
	GCS_BLOCK_REPLY_DENIED_VALIDATOR_REJECT = 3,
	GCS_BLOCK_REPLY_DENIED_EPOCH_STALE = 4,
	GCS_BLOCK_REPLY_DENIED_CHECKSUM_FAIL = 5,
	GCS_BLOCK_REPLY_DENIED_MASTER_NOT_HOLDER = 6,
	GCS_BLOCK_REPLY_DENIED_DEDUP_FULL = 7,			/* PGRAC: spec-2.34 D1 NEW;
											 * HC96 transient — sender 走 retry
											 * path 同 timeout 语义,budget 耗尽
											 * 才 ereport 53R90 */
	GCS_BLOCK_REPLY_GRANTED_FROM_HOLDER = 8,		/* PGRAC: spec-2.35 D1 NEW;
												 * holder ships block directly to
												 * original requester (2-way CF read
												 * sharing).  Sender HC108
												 * authorized chain validates that
												 * hdr.forwarding_master_node ==
												 * slot.expected_master_node. */
	GCS_BLOCK_REPLY_X_GRANTED_FROM_HOLDER = 9,		/* PGRAC: spec-2.36 D1 NEW;
												 * X-flavored holder direct ship for
												 * 3-way CF writer transfer.  HC115
												 * + HC118 — same HC108 authorized
												 * chain semantics as GRANTED_FROM_
												 * HOLDER but maps to X transition. */
	GCS_BLOCK_REPLY_DENIED_PENDING_X = 10,			/* PGRAC: spec-2.36 D1 NEW;
												 * HC117 reader starvation guard —
												 * N→S request denied because a
												 * pending X requester is registered;
												 * sender backs off + retries per
												 * cluster.gcs_block_starvation_*. */
	GCS_BLOCK_REPLY_DENIED_INVALIDATE_TIMEOUT = 11, /* PGRAC: spec-2.36 D1 NEW;
													 * master could not collect all
													 * S/X holder invalidate ACKs
													 * within retransmit budget;
													 * sender maps to 53R91. */
	GCS_BLOCK_REPLY_DENIED_LOST_WRITE = 12,			/* PGRAC: spec-2.37 D1 / spec-2.41 D1;
													 * master direct ship self-check OR
													 * holder forward validate fail-closed
													 * via gcs_block_lost_write_verdict():
													 * shipped page pd_block_scn STALE
													 * (< pi_watermark_scn) or ANOMALY
													 * (tracked block, pd_block_scn
													 * InvalidScn).  Cross-node version is
													 * the global SCN, NOT page_lsn (§0).
													 * sender maps to 53R93 terminal denial
													 * — not retried because lost-write is a
													 * data integrity issue that must
													 * surface, not be papered over. */
	GCS_BLOCK_REPLY_READ_IMAGE_FROM_XHOLDER = 13	/* PGRAC: spec-5.2 D2 NEW;
													 * X-holder ships the CURRENT block
													 * image for a one-shot cross-node
													 * read (node1 must see node0's
													 * uncommitted ITL row-lock bits) and
													 * KEEPS its X — no ownership transfer,
													 * no downgrade.  The requester
													 * installs the bytes for this read
													 * only, does NOT send a transition-ack
													 * (never registers as an S holder),
													 * and leaves buf->pcm_state == N so
													 * the next access re-fetches (Rule
													 * 8.A: a cached copy with no
													 * invalidation path would go stale).
													 * Reuses HC103 copy-ship + HC127
													 * watermark. */
	,
	GCS_BLOCK_REPLY_DENIED_RESOURCE_RECOVERING = 14,  /* PGRAC: spec-5.16 D3b NEW;
													 * master-side hard gate (INV-R8/R14)
													 * — the master (a rejoining node) is
													 * NOT yet a quorum MEMBER, or the
													 * requested block's joiner-home view
													 * is still being rebuilt (survivors
													 * not all re-declared).  Default-deny
													 * BEFORE dedup/grant so a stale-view
													 * requester routed here never gets a
													 * cold grant.  sender maps to 53R9L
													 * (retry-safe, Class 53). */
	GCS_BLOCK_REPLY_S_GRANTED_XHOLDER_DOWNGRADE = 15, /* PGRAC: spec-6.12a ㉕ NEW;
													 * the remote X holder accepted the
													 * downgrade request: it flushed the
													 * quiescent page, flipped its own
													 * copy X→S, fired the master
													 * PCM_TRANS_X_TO_S_DOWNGRADE notify,
													 * and ships this DURABLE S grant.
													 * The requester installs the bytes,
													 * then registers as an S holder via
													 * the normal N→S transition (wire
													 * try-ACK for the 3-corner path;
													 * local apply when requester ==
													 * master).  If that registration is
													 * denied (notify raced or lost) the
													 * requester DEGRADES to the one-shot
													 * read-image semantics: pcm_state
													 * stays N, no S copy is retained
													 * (Rule 8.A fail-closed — never a
													 * durable copy the master does not
													 * track).  HC108 authorized chain
													 * applies as for GRANTED_FROM_HOLDER. */
	GCS_BLOCK_REPLY_CR_RESULT_FULL = 16,			  /* PGRAC: spec-6.12b NEW; the
													 * origin's LMS constructed the
													 * COMPLETE CR page at the carried
													 * read_scn (every candidate chain
													 * was origin-home).  The page is a
													 * consistent-read result: NEVER
													 * installed as current, never
													 * flushed, consumed only by the CR
													 * waiter into the CR cache slot /
													 * scratch (Rule 8.A hard
													 * invariant). */
	GCS_BLOCK_REPLY_CR_RESULT_PARTIAL = 17,			  /* PGRAC: spec-6.12b NEW; the
													 * origin's LMS applied the
													 * write_scn-DESC PREFIX of the
													 * candidate chains (all
													 * origin-home) and stopped at the
													 * first foreign chain — which in
													 * the 2-node topology is
													 * requester-home.  The requester
													 * CONTINUES the construction
													 * locally on the shipped page (the
													 * remaining candidates re-derive
													 * from the page's ITL state); any
													 * still-foreign chain there hits
													 * the class-③ walk backstop ->
													 * 53R9G (Rule 8.A). */
	GCS_BLOCK_REPLY_UNDO_TT_FETCH_RESULT = 18,		  /* PGRAC: spec-6.12i NEW; the
													 * origin's LMS read its own
													 * TT-bearing undo header block
													 * (D-i1) and CO-SAMPLED the live
													 * authority triple into the same
													 * reply: hdr.epoch = LMS-sampled
													 * origin epoch, hdr.page_lsn =
													 * live_hwm_lsn, and a 16-byte
													 * ClusterGcsUndoAuthTrailer after
													 * the page carries tt_generation.
													 * The page is undo METADATA: never
													 * installed as a current heap
													 * block, never flushed; consumed
													 * only by the runtime-visibility
													 * fetch (Rule 8.A). */
	GCS_BLOCK_REPLY_UNDO_VERDICT_RESULT = 19		  /* PGRAC: spec-6.12i D-i4 /
													 * spec-6.15 D4 NEW; the origin's
													 * LMS ran the COMPLETE own-TT
													 * by-xid scan + CLOG cross-check
													 * + retention origin legs and
													 * ships a ClusterGcsUndoVerdictPage
													 * (in the BLCKSZ area) under the
													 * same co-sampled authority
													 * carriage as status 18 (epoch /
													 * live_hwm_lsn / trailer).  Every
													 * unprovable outcome is a DENIED
													 * reply instead — the requester
													 * keeps 53R97 (Rule 8.A). */
	,
	GCS_BLOCK_REPLY_UNDO_MULTI_VERDICT_RESULT = 20, /* PGRAC: spec-7.1 D3-b NEW; the
												 * origin's LMS enumerated a foreign
													 * multixact's members and served a
													 * per-updater-member batch verdict
													 * (ClusterGcsUndoMultiVerdictPage in
													 * the BLCKSZ area) under the same
													 * co-sampled authority carriage as
													 * statuses 18/19.  Shipped ONLY when
													 * every updater member is proven
													 * (status SERVED); any unprovable
												 * multi is a DENIED reply — the
												 * requester keeps 53R97 (Rule 8.A). */
	GCS_BLOCK_REPLY_R4_CR_FULL = 21,
	GCS_BLOCK_REPLY_R4_TX_RESOLVE_RESULT = 22,
	GCS_BLOCK_REPLY_R4_MULTI_RESOLVE_RESULT = 23,
	GCS_BLOCK_REPLY_R4_UNDO_DATA_RESULT = 24,
	GCS_BLOCK_REPLY_R4_RETRYABLE_HOLDER_MOVED = 25,
	GCS_BLOCK_REPLY_R4_DENIED = 26,
	GCS_BLOCK_REPLY_CURRENT_MX_DESCRIBE_RESULT = 27,
	GCS_BLOCK_REPLY_CURRENT_MX_MEMBER_PROOF_RESULT = 28,
	GCS_BLOCK_REPLY_CURRENT_MX_STATS_RESULT = 29,
	GCS_BLOCK_REPLY_CURRENT_MX_CTRC_SEAL_RESULT = 30
} GcsBlockReplyStatus;

/*
 * The block-request wire validator admits only legal transition ids, but the
 * master's outer S/X decision and its final entry-lock transition apply are
 * deliberately separated by buffer probing/copying.  A concurrent handoff can
 * therefore make an otherwise valid N->S/N->X request incompatible at that
 * final apply.  That is authority drift, not a client-terminal protocol error:
 * reuse DENIED_PENDING_X's established fresh-request/token retry boundary.
 * Keep every other transition's incompatibility terminal so malformed or
 * structurally illegal control-plane transitions are never papered over.
 */
static inline GcsBlockReplyStatus
GcsBlockApplyRefusalStatus(PcmGcsTransitionApplyResult apply_result,
						   PcmLockTransition transition_id)
{
	if (apply_result == PCM_GCS_TRANSITION_PENDING_X
		|| (apply_result == PCM_GCS_TRANSITION_INCOMPATIBLE
			&& (transition_id == PCM_TRANS_N_TO_S || transition_id == PCM_TRANS_N_TO_X)))
		return GCS_BLOCK_REPLY_DENIED_PENDING_X;
	return GCS_BLOCK_REPLY_DENIED_INCOMPATIBLE;
}

/* spec-5.16 D3b / r4 (spec-6.12a ㉕ extends) — every new reply status MUST be
 * appended as the tail value (no collision with any shipped status; r3 mis-read
 * a truncated enum as max 8, the real shipped max before spec-5.16 was
 * READ_IMAGE_FROM_XHOLDER=13). */
StaticAssertDecl(GCS_BLOCK_REPLY_DENIED_RESOURCE_RECOVERING
					 == GCS_BLOCK_REPLY_READ_IMAGE_FROM_XHOLDER + 1,
				 "GCS_BLOCK_REPLY_DENIED_RESOURCE_RECOVERING must follow READ_IMAGE_FROM_XHOLDER");
StaticAssertDecl(
	GCS_BLOCK_REPLY_S_GRANTED_XHOLDER_DOWNGRADE == GCS_BLOCK_REPLY_DENIED_RESOURCE_RECOVERING + 1,
	"GCS_BLOCK_REPLY_S_GRANTED_XHOLDER_DOWNGRADE must follow DENIED_RESOURCE_RECOVERING");
StaticAssertDecl(GCS_BLOCK_REPLY_CR_RESULT_PARTIAL == GCS_BLOCK_REPLY_CR_RESULT_FULL + 1
					 && GCS_BLOCK_REPLY_CR_RESULT_FULL
							== GCS_BLOCK_REPLY_S_GRANTED_XHOLDER_DOWNGRADE + 1,
				 "spec-6.12b CR result statuses must be the tail enum values");
StaticAssertDecl(GCS_BLOCK_REPLY_UNDO_TT_FETCH_RESULT == GCS_BLOCK_REPLY_CR_RESULT_PARTIAL + 1,
				 "spec-6.12i undo-TT fetch status must precede the verdict status");
StaticAssertDecl(GCS_BLOCK_REPLY_UNDO_VERDICT_RESULT == GCS_BLOCK_REPLY_UNDO_TT_FETCH_RESULT + 1,
				 "spec-6.12i undo-verdict status must follow the undo-TT fetch status");
StaticAssertDecl(GCS_BLOCK_REPLY_UNDO_MULTI_VERDICT_RESULT
					 == GCS_BLOCK_REPLY_UNDO_VERDICT_RESULT + 1,
				 "spec-7.1 D3-b undo-multi-verdict status must follow undo-verdict");
StaticAssertDecl(GCS_BLOCK_REPLY_R4_CR_FULL == 21,
				 "R4 reply status ABI must begin at 21");
StaticAssertDecl(GCS_BLOCK_REPLY_R4_DENIED == 26,
				 "R4 reply status ABI must end at 26");
StaticAssertDecl(GCS_BLOCK_REPLY_CURRENT_MX_DESCRIBE_RESULT
					 == GCS_BLOCK_REPLY_R4_DENIED + 1,
				 "current MX describe result must follow the closed R4 domain");
StaticAssertDecl(GCS_BLOCK_REPLY_CURRENT_MX_MEMBER_PROOF_RESULT
					 == GCS_BLOCK_REPLY_CURRENT_MX_DESCRIBE_RESULT + 1,
				 "current MX member-proof result must follow describe");
StaticAssertDecl(GCS_BLOCK_REPLY_CURRENT_MX_STATS_RESULT
					 == GCS_BLOCK_REPLY_CURRENT_MX_MEMBER_PROOF_RESULT + 1,
				 "current MX stats result must follow member proof");
StaticAssertDecl(GCS_BLOCK_REPLY_CURRENT_MX_CTRC_SEAL_RESULT
					 == GCS_BLOCK_REPLY_CURRENT_MX_STATS_RESULT + 1,
				 "CTRC seal result must remain the appended tail status");

/* PGRAC adaptation: R4 owns one closed status suffix.  Keep the domain
 * predicates numeric so legacy and R4 decoders cannot accept each other's
 * frames merely because both use the 48+BLCKSZ envelope shape. */
static inline bool
GcsBlockReplyStatusIsLegacy(GcsBlockReplyStatus status)
{
	return status >= GCS_BLOCK_REPLY_GRANTED
		   && status <= GCS_BLOCK_REPLY_UNDO_MULTI_VERDICT_RESULT;
}

static inline bool
GcsBlockReplyStatusIsR4(GcsBlockReplyStatus status)
{
	return status >= GCS_BLOCK_REPLY_R4_CR_FULL && status <= GCS_BLOCK_REPLY_R4_DENIED;
}

static inline bool
GcsBlockReplyStatusIsR4Refusal(GcsBlockReplyStatus status)
{
	return status == GCS_BLOCK_REPLY_R4_RETRYABLE_HOLDER_MOVED
		   || status == GCS_BLOCK_REPLY_R4_DENIED;
}

/* PGRAC: spec-6.12i / spec-7.1 — every undo-plane reply kind (TT-header fetch,
 * single-xid verdict, batched multi-member verdict, R4 undo-data result) ships
 * the BLCKSZ page plus a co-sampled ClusterGcsUndoAuthTrailer and overrides the
 * reply header's epoch / page_lsn with the LMS-sampled live authority.
 * Centralised so every ship/parse site treats the four identically (D-i3
 * authority carriage). */
static inline bool
GcsBlockReplyStatusCarriesUndoAuthTrailer(GcsBlockReplyStatus status)
{
	return status == GCS_BLOCK_REPLY_UNDO_TT_FETCH_RESULT
		   || status == GCS_BLOCK_REPLY_UNDO_VERDICT_RESULT
		   || status == GCS_BLOCK_REPLY_UNDO_MULTI_VERDICT_RESULT
		   || status == GCS_BLOCK_REPLY_R4_UNDO_DATA_RESULT;
}

static inline bool
GcsBlockReplyStatusAllowsDirectLandInstall(GcsBlockReplyStatus status)
{
	return status == GCS_BLOCK_REPLY_GRANTED || status == GCS_BLOCK_REPLY_GRANTED_FROM_HOLDER
		   || status == GCS_BLOCK_REPLY_S_GRANTED_XHOLDER_DOWNGRADE;
}

static inline bool
GcsBlockReplyStatusIsDirectLandSendable(GcsBlockReplyStatus status)
{
	if (GcsBlockReplyStatusAllowsDirectLandInstall(status))
		return true;
	switch (status) {
	case GCS_BLOCK_REPLY_DENIED_INCOMPATIBLE:
	case GCS_BLOCK_REPLY_DENIED_VALIDATOR_REJECT:
	case GCS_BLOCK_REPLY_DENIED_EPOCH_STALE:
	case GCS_BLOCK_REPLY_DENIED_CHECKSUM_FAIL:
	case GCS_BLOCK_REPLY_DENIED_MASTER_NOT_HOLDER:
	case GCS_BLOCK_REPLY_DENIED_DEDUP_FULL:
	case GCS_BLOCK_REPLY_DENIED_PENDING_X:
	case GCS_BLOCK_REPLY_DENIED_INVALIDATE_TIMEOUT:
	case GCS_BLOCK_REPLY_DENIED_LOST_WRITE:
	case GCS_BLOCK_REPLY_DENIED_RESOURCE_RECOVERING:
		return true;
	default:
		break;
	}
	return false;
}

static inline bool
GcsBlockReplyStatusAllowsDirectLandNoForwardIdentity(GcsBlockReplyStatus status)
{
	if (status == GCS_BLOCK_REPLY_GRANTED)
		return true;
	return !GcsBlockReplyStatusAllowsDirectLandInstall(status)
		   && GcsBlockReplyStatusIsDirectLandSendable(status);
}

static inline bool
GcsBlockDirectCanArmExpectedPeer(int32 holder_node, int32 expected_peer)
{
	return holder_node < 0 || holder_node == expected_peer;
}

/* ============================================================
 * GcsBlockInvalidatePayload — spec-2.36 D1 NEW.
 *
 *   Wire-ABI for PGRAC_IC_MSG_GCS_BLOCK_INVALIDATE (master → S/X holder).
 *   Carried inside a ClusterICEnvelope; sender backend (which is the
 *   master responding to a foreign X request) emits one per current
 *   holder enumerated from s_holders_bitmap / x_holder_node.
 *
 *   Layout (64B fixed; HC83 CRC32C @ offset 48; pad to 64 with
 *   reserved_1[12]):
 *     [  0,   8) request_id              uint64  — master-side allocator
 *     [  8,  16) epoch                   uint64  — HC73 freshness
 *     [ 16,  36) tag                     BufferTag (PG-fact 20B)
 *     [ 36,  40) master_node             int32   — sender of invalidate
 *     [ 40,  41) invalidating_for_x_node uint8   — original X requester
 *                                                  (observability;
 *                                                  HC117 starvation trace)
 *     [ 41,  48) reserved_0[7]                   — pad to checksum align
 *     [ 48,  52) checksum                uint32  — HC83 CRC32C
 *     [ 52,  64) reserved_1[12]                  — pad to 64B
 * ============================================================ */
typedef struct GcsBlockInvalidatePayload {
	uint64 request_id;			   /*  8B [  0,   8) */
	uint64 epoch;				   /*  8B [  8,  16) */
	BufferTag tag;				   /* 20B [ 16,  36) PG-fact */
	int32 master_node;			   /*  4B [ 36,  40) */
	uint8 invalidating_for_x_node; /*  1B [ 40,  41) HC117 */
	uint8 reserved_0[7];		   /*  7B [ 41,  48) */
	uint32 checksum;			   /*  4B [ 48,  52) HC83 CRC32C */
	uint8 reserved_1[12];		   /* 12B [ 52,  64) */
} GcsBlockInvalidatePayload;

/* ============================================================
 * GcsBlockInvalidateAckPayload — spec-2.36 D1 NEW.
 *
 *   Wire-ABI for PGRAC_IC_MSG_GCS_BLOCK_INVALIDATE_ACK (holder → master).
 *   Distinct msg_type from INVALIDATE; same size but separate dispatch
 *   keying (codereview F1 P0).  ack_status field encodes:
 *
 *     0 = OK (holder evicted buffer + applied PCM transition)
 *     1 = epoch_stale (HC100 reject before mutation)
 *     2 = already_invalidated (race: buffer not resident)
 *
 *   Layout (64B fixed; same offsets as request payload to keep header
 *   parsing symmetric through checksum).  spec-2.37 carried the holder
 *   page_lsn here; spec-2.41 D3 REINTERPRETS the same 8B slot as the holder
 *   page's pd_block_scn (the cross-node version) so the master advances the
 *   lost-write detector's SCN watermark after a successful invalidate ACK.
 *   The slot is covered by the ACK checksum (all-bytes-except-checksum), so
 *   the reinterpretation is checksum-neutral.  Mixed-version incompatible —
 *   gated by the spec-2.41 catversion/protocol bump (D8):
 *     [ 52,  60) page_scn_bytes[8]      -- little-endian SCN (was page_lsn)
 *     [ 60,  64) reserved_1[4]          -- pad to 64B
 * ============================================================ */
typedef struct GcsBlockInvalidateAckPayload {
	uint64 request_id;		 /*  8B [  0,   8) */
	uint64 epoch;			 /*  8B [  8,  16) */
	BufferTag tag;			 /* 20B [ 16,  36) PG-fact */
	int32 sender_node;		 /*  4B [ 36,  40) */
	uint8 ack_status;		 /*  1B [ 40,  41) 0/1/2 */
	uint8 reserved_0[7];	 /*  7B [ 41,  48) */
	uint32 checksum;		 /*  4B [ 48,  52) HC83 CRC32C */
	uint8 page_scn_bytes[8]; /*  8B [ 52,  60) spec-2.41 D3 (was page_lsn_bytes) */
	uint8 reserved_1[4];	 /*  4B [ 60,  64) */
} GcsBlockInvalidateAckPayload;

StaticAssertDecl(sizeof(GcsBlockInvalidateAckPayload) == 64,
				 "spec-2.36 D1 / spec-2.41 D3 GcsBlockInvalidateAckPayload wire ABI 64B");

StaticAssertDecl(offsetof(GcsBlockInvalidateAckPayload, page_scn_bytes) == 52,
				 "spec-2.41 D3 — invalidate ACK page_scn_bytes[8] must land at offset 52");

static inline void
GcsBlockInvalidateAckPayloadSetPageScn(GcsBlockInvalidateAckPayload *p, SCN scn)
{
	uint64 v = (uint64)scn;

	p->page_scn_bytes[0] = (uint8)(v & 0xff);
	p->page_scn_bytes[1] = (uint8)((v >> 8) & 0xff);
	p->page_scn_bytes[2] = (uint8)((v >> 16) & 0xff);
	p->page_scn_bytes[3] = (uint8)((v >> 24) & 0xff);
	p->page_scn_bytes[4] = (uint8)((v >> 32) & 0xff);
	p->page_scn_bytes[5] = (uint8)((v >> 40) & 0xff);
	p->page_scn_bytes[6] = (uint8)((v >> 48) & 0xff);
	p->page_scn_bytes[7] = (uint8)((v >> 56) & 0xff);
}

static inline SCN
GcsBlockInvalidateAckPayloadGetPageScn(const GcsBlockInvalidateAckPayload *p)
{
	uint64 v = 0;

	v |= (uint64)p->page_scn_bytes[0];
	v |= (uint64)p->page_scn_bytes[1] << 8;
	v |= (uint64)p->page_scn_bytes[2] << 16;
	v |= (uint64)p->page_scn_bytes[3] << 24;
	v |= (uint64)p->page_scn_bytes[4] << 32;
	v |= (uint64)p->page_scn_bytes[5] << 40;
	v |= (uint64)p->page_scn_bytes[6] << 48;
	v |= (uint64)p->page_scn_bytes[7] << 56;
	return (SCN)v;
}

/* ============================================================
 * PGRAC: spec-6.12h D-h2 — reserved-byte / status overlays on the
 * INVALIDATE / INVALIDATE_ACK wire pair (§3.6 discipline: no new msg_type;
 * the "copy hygiene" channel carries the PI-discard protocol).  All four
 * rides are covered by the existing checksums (invalidate: bytes [0,48);
 * ACK: all-bytes-except-checksum).
 *
 *   INVALIDATE.reserved_0[0] == KIND_PI_DISCARD:  master → holder "drop
 *     your Past Image of this tag" directive.  Unsolicited (request_id 0),
 *     fire-and-forget, NEVER ACKed; the holder drops strictly PI-typed
 *     buffers only (a live current copy is never touched).  0 keeps the
 *     legacy S-invalidate semantics byte-identical.
 *
 *   ACK.reserved_0[0] == ACK_KEPT_PI (on a solicited status-0 ack):  the
 *     invalidated holder's drop converted to a D-h1 Past Image; the master
 *     records the sender in pi_holders_bitmap.
 *
 *   ACK.ack_status == PI_DURABLE_NOTE (unsolicited):  a writer reports the
 *     block's CURRENT copy durable on shared storage (Q25-A trigger fired);
 *     page_scn_bytes@52 carries the written pd_block_scn (the only
 *     cross-node comparable version unit — per-thread WAL keeps LSNs in
 *     per-node spaces, so no LSN rides).  request_id stays 0; no slot
 *     matching (diverted before the HC100 slot logic); no reply.
 *
 *   ACK.ack_status == PI_KEPT_NOTE (unsolicited):  a forwarded holder's
 *     destructive drop kept a Past Image; master records the sender.
 * ============================================================ */
#define GCS_BLOCK_INVALIDATE_KIND_PI_DISCARD 1
#define GCS_BLOCK_INVALIDATE_ACK_KEPT_PI 1
#define GCS_BLOCK_INVALIDATE_ACK_STATUS_PI_DURABLE_NOTE 3
#define GCS_BLOCK_INVALIDATE_ACK_STATUS_PI_KEPT_NOTE 4
/* PGRAC ownership-generation wave (ruling ②) — solicited negative ACK: the
 * holder cannot invalidate RIGHT NOW (GRANT_PENDING in-flight grant, or a
 * pinned copy) and did NOT change any local state.  The master must not
 * credit acked_bm / clear the holder bit / advance watermarks / grant X; it
 * aborts the round immediately (pending_x cleared, slot released) and
 * retries with a NEW round identity after a short backoff.  Values 3/4 are
 * taken by the PI note rides above; 5 is the next free value.  Send-side
 * gated on PGRAC_IC_HELLO_CAP_GCS_INVAL_BUSY_V1 (an old master drops
 * status>2 as stale and would burn its timeout; the holder then falls back
 * to the round-5 park). */
#define GCS_BLOCK_INVALIDATE_ACK_STATUS_RETRYABLE_BUSY 5

/* Slot occupancy, not epoch value, proves that an outstanding request exists.
 * The first legal reconfiguration advances epoch 0 to 1 and must wake those
 * initial-formation requests just like every later epoch transition. */
static inline bool
cluster_gcs_block_epoch_advance_stales_slot(bool in_use, uint64 request_epoch, uint64 new_epoch)
{
	return in_use && request_epoch < new_epoch;
}

/* ============================================================
 *   GcsBlockRedeclarePayload -- wire ABI for PGRAC_IC_MSG_GCS_BLOCK_REDECLARE
 *   (spec-4.7 D2;  survivor → remastered master).
 *
 *   After a reconfiguration, each survivor's P5 chunked scan (in the LMON
 *   reconfig tick) re-declares every locally-held S/X buffer to the block's
 *   current GCS master so the master can rebuild the minimal block-resource
 *   view (holder bitmap / mode / PI watermark — D3).  Fire-and-forget
 *   announce (no ACK):  the master is authoritative once rebuilt and a lost
 *   announce just leaves a holder unrecorded (re-sent next tick until the
 *   barrier completes).  64B fixed.  cluster_epoch is the episode epoch (L235
 *   coherence gate; the master drops a re-declare whose epoch != its accepted
 *   episode epoch).
 *
 *   DUAL version carriers (spec-2.41 D3): page_lsn_bytes@28 keeps the
 *   per-stream replay position for the spec-4.7 D5 redo-coverage serve-gate
 *   (required_lsn); page_scn_bytes@52 (carved from the old reserved_1) carries
 *   the cross-node pd_block_scn for the lost-write detector's SCN watermark.
 *   The rebuild advances BOTH.  Because page_scn@52 falls AFTER checksum@48,
 *   the checksum was extended to all-bytes-except-checksum (D3 mandatory) so a
 *   corrupted holder page_lsn OR page_scn cannot poison the rebuilt watermarks.
 * ============================================================ */
typedef struct GcsBlockRedeclarePayload {
	uint64 cluster_epoch;	 /*  8B [  0,   8) episode epoch (L235) */
	BufferTag tag;			 /* 20B [  8,  28) PG-fact */
	uint8 page_lsn_bytes[8]; /*  8B [ 28,  36) LE XLogRecPtr (redo-coverage required_lsn) */
	int32 holder_node_id;	 /*  4B [ 36,  40) = sender node */
	uint8 held_mode;		 /*  1B [ 40,  41) PcmState: PCM_STATE_S / PCM_STATE_X */
	uint8 reserved_0[7];	 /*  7B [ 41,  48) */
	uint32 checksum;		 /*  4B [ 48,  52) */
	uint8 page_scn_bytes[8]; /*  8B [ 52,  60) spec-2.41 D3 LE SCN (detector watermark) */
	uint8 reserved_1[4];	 /*  4B [ 60,  64) pad to 64B */
} GcsBlockRedeclarePayload;

StaticAssertDecl(sizeof(GcsBlockRedeclarePayload) == 64,
				 "spec-4.7 D2 / spec-2.41 D3 GcsBlockRedeclarePayload wire ABI 64B");
StaticAssertDecl(offsetof(GcsBlockRedeclarePayload, page_lsn_bytes) == 28,
				 "spec-4.7 D2 GcsBlockRedeclarePayload page_lsn_bytes must land at offset 28");
StaticAssertDecl(offsetof(GcsBlockRedeclarePayload, checksum) == 48,
				 "spec-4.7 D2 GcsBlockRedeclarePayload checksum must land at offset 48");
StaticAssertDecl(offsetof(GcsBlockRedeclarePayload, page_scn_bytes) == 52,
				 "spec-2.41 D3 GcsBlockRedeclarePayload page_scn_bytes must land at offset 52");

static inline void
GcsBlockRedeclarePayloadSetPageLsn(GcsBlockRedeclarePayload *p, XLogRecPtr lsn)
{
	uint64 v = (uint64)lsn;

	p->page_lsn_bytes[0] = (uint8)(v & 0xff);
	p->page_lsn_bytes[1] = (uint8)((v >> 8) & 0xff);
	p->page_lsn_bytes[2] = (uint8)((v >> 16) & 0xff);
	p->page_lsn_bytes[3] = (uint8)((v >> 24) & 0xff);
	p->page_lsn_bytes[4] = (uint8)((v >> 32) & 0xff);
	p->page_lsn_bytes[5] = (uint8)((v >> 40) & 0xff);
	p->page_lsn_bytes[6] = (uint8)((v >> 48) & 0xff);
	p->page_lsn_bytes[7] = (uint8)((v >> 56) & 0xff);
}

static inline XLogRecPtr
GcsBlockRedeclarePayloadGetPageLsn(const GcsBlockRedeclarePayload *p)
{
	uint64 v = 0;

	v |= (uint64)p->page_lsn_bytes[0];
	v |= (uint64)p->page_lsn_bytes[1] << 8;
	v |= (uint64)p->page_lsn_bytes[2] << 16;
	v |= (uint64)p->page_lsn_bytes[3] << 24;
	v |= (uint64)p->page_lsn_bytes[4] << 32;
	v |= (uint64)p->page_lsn_bytes[5] << 40;
	v |= (uint64)p->page_lsn_bytes[6] << 48;
	v |= (uint64)p->page_lsn_bytes[7] << 56;
	return (XLogRecPtr)v;
}

/* PGRAC: spec-2.41 D3 — REDECLARE page_scn carrier (@52, detector watermark).
 * Distinct unit from page_lsn@28 (redo-coverage); the rebuild advances both. */
static inline void
GcsBlockRedeclarePayloadSetPageScn(GcsBlockRedeclarePayload *p, SCN scn)
{
	uint64 v = (uint64)scn;

	p->page_scn_bytes[0] = (uint8)(v & 0xff);
	p->page_scn_bytes[1] = (uint8)((v >> 8) & 0xff);
	p->page_scn_bytes[2] = (uint8)((v >> 16) & 0xff);
	p->page_scn_bytes[3] = (uint8)((v >> 24) & 0xff);
	p->page_scn_bytes[4] = (uint8)((v >> 32) & 0xff);
	p->page_scn_bytes[5] = (uint8)((v >> 40) & 0xff);
	p->page_scn_bytes[6] = (uint8)((v >> 48) & 0xff);
	p->page_scn_bytes[7] = (uint8)((v >> 56) & 0xff);
}

static inline SCN
GcsBlockRedeclarePayloadGetPageScn(const GcsBlockRedeclarePayload *p)
{
	uint64 v = 0;

	v |= (uint64)p->page_scn_bytes[0];
	v |= (uint64)p->page_scn_bytes[1] << 8;
	v |= (uint64)p->page_scn_bytes[2] << 16;
	v |= (uint64)p->page_scn_bytes[3] << 24;
	v |= (uint64)p->page_scn_bytes[4] << 32;
	v |= (uint64)p->page_scn_bytes[5] << 40;
	v |= (uint64)p->page_scn_bytes[6] << 48;
	v |= (uint64)p->page_scn_bytes[7] << 56;
	return (SCN)v;
}


/* ============================================================
 * GcsBlockRequestPayload -- wire ABI for PGRAC_IC_MSG_GCS_BLOCK_REQUEST.
 *
 *  Layout (64B; HC80; Sprint A Step 1 PG-fact discovery: struct natural
 *  alignment is 8B because of uint64 request_id / epoch, so the trailing
 *  pad rounds 60B claim up to 64B.  Reserved_0 bumped 15 → 19 to make
 *  the size explicit at the declaration and lock the wire ABI to 64B):
 *    [  0,   8) request_id              -- per-sender-backend monotone
 *    [  8,  16) epoch                   -- cluster_epoch snapshot at send
 *    [ 16,  36) tag                     -- BufferTag (PG-fact 20B)
 *    [ 36,  40) sender_node             -- int32 cluster_node_id of sender
 *    [ 40,  44) requester_backend_id    -- int32 backend slot index;
 *                                          compound reply key (HC80)
 *    [ 44,  45) transition_id           -- PcmLockTransition (1..9)
 *    [ 45,  64) reserved_0[19]          -- pad + future fields
 * ============================================================ */
typedef struct GcsBlockRequestPayload {
	uint64 request_id;			/*  8B [  0,   8) */
	uint64 epoch;				/*  8B [  8,  16) */
	BufferTag tag;				/* 20B [ 16,  36) */
	int32 sender_node;			/*  4B [ 36,  40) */
	int32 requester_backend_id; /* 4B [ 40,  44) */
	uint8 transition_id;		/*  1B [ 44,  45) */
	uint8 reserved_0[19];		/* 19B [ 45,  64) */
} GcsBlockRequestPayload;

StaticAssertDecl(sizeof(GcsBlockRequestPayload) == 64,
				 "spec-2.33 D1 GcsBlockRequestPayload wire ABI 64B "
				 "(request_id 8 + epoch 8 + tag 20 + sender_node 4 + "
				 "requester_backend_id 4 + transition_id 1 + reserved 19;"
				 " 64B = natural 8-aligned struct size)");

/* PGRAC: spec-5.2a D1 — clean-page X-transfer eligibility flag carried in the
 * REQUEST payload's reserved_0[0].
 *
 *	The REQUEST and FORWARD payloads are DISTINCT structs, so request[0] is
 *	free even though forward[0] is the spec-5.2 read-image flag (the eligible
 *	flag on the forward wire uses reserved_0[2] instead — see
 *	GcsBlockForwardPayloadSetCleanEligible).  The requesting backend sets this
 *	when its NEXT cluster PCM X acquire was deliberately armed for a clean
 *	(no active ITL / MVCC) page — sequence refill, spec-5.2a D5 — so the GCS
 *	master takes the dedicated clean-page X-transfer path (spec-5.2a D3)
 *	instead of the conservative HG7 fail-closed DENY.  A normal heap request
 *	leaves it 0 → existing conservative path unchanged (inv ①).  ABI stays
 *	64B (reserved-byte overlay). */
static inline void
GcsBlockRequestPayloadSetCleanEligible(GcsBlockRequestPayload *p, bool eligible)
{
	p->reserved_0[0] = eligible ? (uint8)1 : (uint8)0;
}

static inline bool
GcsBlockRequestPayloadIsCleanEligible(const GcsBlockRequestPayload *p)
{
	return p->reserved_0[0] != 0;
}

/* PGRAC: spec-6.13 D6 — direct-land arming flag carried in REQUEST
 * reserved_0[1].  REQUEST bytes are independent from FORWARD bytes; [0] is
 * the clean-page X-transfer eligibility flag above and [1] was previously
 * unused. */
static inline void
GcsBlockRequestPayloadSetDirectLandArmed(GcsBlockRequestPayload *p, bool armed)
{
	p->reserved_0[1] = armed ? (uint8)1 : (uint8)0;
}

static inline bool
GcsBlockRequestPayloadIsDirectLandArmed(const GcsBlockRequestPayload *p)
{
	return p->reserved_0[1] != 0;
}

/* PGRAC: GCS-race round-2 RC-F — requester legal-lifetime hint carried in
 * REQUEST reserved_0[2..5] (uint32 ms, little-endian byte overlay; [0] is
 * clean-eligible, [1] direct-land above).  0 = no hint (older wire peer):
 * the master pins the entry TTL from its own GUCs at registration. */
static inline void
GcsBlockRequestPayloadSetLifetimeHintMs(GcsBlockRequestPayload *p, uint32 lifetime_ms)
{
	p->reserved_0[2] = (uint8)(lifetime_ms & 0xFF);
	p->reserved_0[3] = (uint8)((lifetime_ms >> 8) & 0xFF);
	p->reserved_0[4] = (uint8)((lifetime_ms >> 16) & 0xFF);
	p->reserved_0[5] = (uint8)((lifetime_ms >> 24) & 0xFF);
}

static inline uint32
GcsBlockRequestPayloadGetLifetimeHintMs(const GcsBlockRequestPayload *p)
{
	return (uint32)p->reserved_0[2] | ((uint32)p->reserved_0[3] << 8)
		   | ((uint32)p->reserved_0[4] << 16) | ((uint32)p->reserved_0[5] << 24);
}


/* ============================================================
 * GcsBlockDonePayload -- wire ABI for PGRAC_IC_MSG_GCS_BLOCK_DONE
 *                        (GCS-race round-2 RC-F completion proof).
 *
 *	Sent by the requester AFTER it has accepted a terminal reply
 *	(status verified, CRC passed, image installed/consumed).  The master
 *	verifies the FULL identity against its dedup entry and stamps the
 *	completion proof (cluster_gcs_block_dedup_mark_done); every mismatch
 *	is counted and dropped -- DONE is advisory, the pinned TTL remains
 *	the loss backstop.
 *
 *	epoch carries the REQUEST epoch (slot->request_epoch): the master's
 *	dedup key was built from req->epoch, so a reply-time epoch would
 *	never match the entry.
 *
 *	Layout mirrors GcsBlockRequestPayload (64B fixed):
 *	  [  0,   8) request_id
 *	  [  8,  16) epoch                   -- REQUEST epoch (key match)
 *	  [ 16,  36) tag                     -- shard routing + identity
 *	  [ 36,  40) sender_node             -- requester (key origin)
 *	  [ 40,  44) requester_backend_id
 *	  [ 44,  45) transition_id
 *	  [ 45,  64) reserved_0[19]          -- zero
 * ============================================================ */
typedef struct GcsBlockDonePayload {
	uint64 request_id;			/*  8B [  0,   8) */
	uint64 epoch;				/*  8B [  8,  16) — REQUEST epoch */
	BufferTag tag;				/* 20B [ 16,  36) */
	int32 sender_node;			/*  4B [ 36,  40) */
	int32 requester_backend_id; /* 4B [ 40,  44) */
	uint8 transition_id;		/*  1B [ 44,  45) */
	uint8 reserved_0[19];		/* 19B [ 45,  64) */
} GcsBlockDonePayload;

StaticAssertDecl(sizeof(GcsBlockDonePayload) == 64,
				 "GCS-race round-2 GcsBlockDonePayload wire ABI 64B (mirrors request)");


/* ============================================================
 * GcsBlockReplyHeader -- wire ABI for PGRAC_IC_MSG_GCS_BLOCK_REPLY
 *                        (header portion; followed by 8192B block_data).
 *
 *  Total reply envelope payload = sizeof(GcsBlockReplyHeader) +
 *                                 GCS_BLOCK_DATA_SIZE = 48 + 8192 = 8240B.
 *  Receiver decodes header in-place then reads block_data directly out
 *  of the envelope buffer (no separate alloc).
 *
 *  Layout (48B; HC80 + HC83 + HC84 + spec-2.35 HC109):
 *    [  0,   8) request_id              -- match outstanding
 *    [  8,  16) page_lsn                -- PageGetLSN(page) at ship time;
 *                                          receiver MUST PageSetLSN(page,
 *                                          page_lsn) under content_lock
 *                                          EXCLUSIVE (HC84)
 *    [ 16,  24) epoch                   -- cluster_epoch at reply
 *    [ 24,  28) checksum                -- CRC32C(block_data, 8192) (HC83)
 *    [ 28,  32) sender_node             -- int32 of replying node
 *                                          (master for direct, holder for
 *                                          forwarded-from-holder)
 *    [ 32,  36) requester_backend_id    -- compound key match (HC80)
 *    [ 36,  37) transition_id           -- echo from request
 *    [ 37,  38) status                  -- GcsBlockReplyStatus (HC83)
 *    [ 38,  42) forwarding_master_node_bytes[4]
 *                                       -- spec-2.35 HC109 reserved 重解读:
 *                                          stored as uint8[4] (NOT int32) so
 *                                          the compiler does not insert
 *                                          padding before this field;  use
 *                                          GcsBlockReplyHeaderGet/Set
 *                                          ForwardingMasterNode() helpers to
 *                                          encode/decode int32 little-endian.
 *                                          -1 == direct from master;
 *                                          >= 0 == forwarded by this master
 *                                          (sender 走 HC108 authorized chain).
 *                                          Node 0 is a valid cluster node;
 *                                          never use 0 as the direct sentinel.
 *    [ 42,  48) reserved_0[6]           -- align + future fields
 * ============================================================ */
typedef struct GcsBlockReplyHeader {
	uint64 request_id;					   /*  8B [  0,   8) */
	uint64 page_lsn;					   /*  8B [  8,  16) HC84 */
	uint64 epoch;						   /*  8B [ 16,  24) */
	uint32 checksum;					   /*  4B [ 24,  28) HC83 CRC32C */
	int32 sender_node;					   /*  4B [ 28,  32) */
	int32 requester_backend_id;			   /*  4B [ 32,  36) */
	uint8 transition_id;				   /*  1B [ 36,  37) */
	uint8 status;						   /*  1B [ 37,  38) GcsBlockReplyStatus */
	uint8 forwarding_master_node_bytes[4]; /* 4B [ 38,  42) HC109 spec-2.35 */
	uint8 reserved_0[6];				   /*  6B [ 42,  48) */
} GcsBlockReplyHeader;

StaticAssertDecl(sizeof(GcsBlockReplyHeader) == 48,
				 "spec-2.33 D1 + spec-2.35 HC109 GcsBlockReplyHeader wire ABI 48B "
				 "(request_id 8 + page_lsn 8 + epoch 8 + checksum 4 + "
				 "sender_node 4 + requester_backend_id 4 + transition_id 1 + "
					 "status 1 + forwarding_master_node_bytes 4 + reserved 6)");

/* Status 24 alone reuses the six-byte reply tail as
 * {physical_generation:u32_le, reserved:u16=0}.  Generation zero is valid;
 * UINT32_MAX is exhausted and cannot be published. */
static inline bool
GcsBlockReplyHeaderSetR4UndoGeneration(GcsBlockReplyHeader *header,
									   uint32 physical_generation)
{
	if (header != NULL)
		memset(header->reserved_0, 0, sizeof(header->reserved_0));
	if (header == NULL || physical_generation == UINT32_MAX)
		return false;
	header->reserved_0[0] = (uint8)physical_generation;
	header->reserved_0[1] = (uint8)(physical_generation >> 8);
	header->reserved_0[2] = (uint8)(physical_generation >> 16);
	header->reserved_0[3] = (uint8)(physical_generation >> 24);
	return true;
}

static inline bool
GcsBlockReplyHeaderGetR4UndoGeneration(const GcsBlockReplyHeader *header,
									   uint32 *physical_generation_out)
{
	uint32 generation;

	if (physical_generation_out != NULL)
		*physical_generation_out = 0;
	if (header == NULL || physical_generation_out == NULL
		|| header->reserved_0[4] != 0 || header->reserved_0[5] != 0)
		return false;
	generation = (uint32)header->reserved_0[0]
				 | ((uint32)header->reserved_0[1] << 8)
				 | ((uint32)header->reserved_0[2] << 16)
				 | ((uint32)header->reserved_0[3] << 24);
	if (generation == UINT32_MAX)
		return false;
	*physical_generation_out = generation;
	return true;
}

#define GCS_BLOCK_REPLY_PROTOCOL_V1 1
#define GCS_BLOCK_REPLY_PROTOCOL_V2 2
#define GCS_BLOCK_REPLY_SF_EARLY_TRANSFER 0x01
#define GCS_BLOCK_REPLY_SF_HAS_DEP_VEC 0x02
#define GCS_BLOCK_REPLY_SF_KNOWN_FLAGS                                                             \
	(GCS_BLOCK_REPLY_SF_EARLY_TRANSFER | GCS_BLOCK_REPLY_SF_HAS_DEP_VEC)

typedef struct GcsBlockReplySfDep {
	int32 origin_node;
	uint32 reserved_0;
	uint64 required_redo_lsn;
} GcsBlockReplySfDep;

/*
 * spec-6.2 D5: block-reply v2 header.  It is never sent to peers that have not
 * negotiated GCS_BLOCK_REPLY_PROTOCOL_V2 in HELLO; mixed-version peers continue
 * to receive the 48-byte v1 header and the HC82 WAL-before-ship path.
 */
typedef struct GcsBlockReplyHeaderV2 {
	GcsBlockReplyHeader v1; /* [0,48) byte-identical v1 prefix */
	uint8 sf_flags;
	uint8 sf_dep_count;
	uint8 reserved_0[6];
	GcsBlockReplySfDep sf_dep[CLUSTER_SF_DEP_MAX_ORIGINS];
} GcsBlockReplyHeaderV2;

StaticAssertDecl(offsetof(GcsBlockReplyHeaderV2, sf_flags) == sizeof(GcsBlockReplyHeader),
				 "spec-6.2 D5 GcsBlockReplyHeaderV2 sf_flags must follow v1 header");
StaticAssertDecl(offsetof(GcsBlockReplyHeaderV2, sf_dep) == sizeof(GcsBlockReplyHeader) + 8,
				 "spec-6.2 D5 GcsBlockReplyHeaderV2 dependency vector offset");
StaticAssertDecl(sizeof(GcsBlockReplySfDep) == 16,
				 "spec-6.2 D5 Smart Fusion dependency wire entry is 16 bytes");
StaticAssertDecl(sizeof(GcsBlockReplyHeaderV2)
					 == sizeof(GcsBlockReplyHeader) + 8
							+ CLUSTER_SF_DEP_MAX_ORIGINS * sizeof(GcsBlockReplySfDep),
				 "spec-6.2 D5 GcsBlockReplyHeaderV2 max-size ABI");

static inline bool
cluster_gcs_block_reply_v2_extract_dep_vec(const GcsBlockReplyHeaderV2 *hdr,
										   ClusterSfDepVec *out_vec)
{
	bool seen[CLUSTER_SF_DEP_MAX_ORIGINS];
	bool has_dep;
	int i;

	if (out_vec != NULL)
		cluster_sf_dep_vec_reset(out_vec);
	if (hdr == NULL)
		return false;
	if ((hdr->sf_flags & ~GCS_BLOCK_REPLY_SF_KNOWN_FLAGS) != 0)
		return false;
	for (i = 0; i < (int)sizeof(hdr->reserved_0); i++) {
		if (hdr->reserved_0[i] != 0)
			return false;
	}
	if (hdr->sf_dep_count > CLUSTER_SF_DEP_MAX_ORIGINS)
		return false;

	has_dep = (hdr->sf_flags & GCS_BLOCK_REPLY_SF_HAS_DEP_VEC) != 0;
	if (!has_dep)
		return hdr->sf_dep_count == 0;
	if ((hdr->sf_flags & GCS_BLOCK_REPLY_SF_EARLY_TRANSFER) == 0 || hdr->sf_dep_count == 0)
		return false;

	memset(seen, 0, sizeof(seen));
	for (i = 0; i < hdr->sf_dep_count; i++) {
		int32 origin = hdr->sf_dep[i].origin_node;
		XLogRecPtr required_lsn = (XLogRecPtr)hdr->sf_dep[i].required_redo_lsn;

		if (hdr->sf_dep[i].reserved_0 != 0)
			return false;
		if (!cluster_sf_dep_origin_valid(origin) || seen[origin]
			|| XLogRecPtrIsInvalid(required_lsn))
			return false;
		seen[origin] = true;
		if (out_vec != NULL)
			out_vec->required[origin] = required_lsn;
	}
	for (; i < CLUSTER_SF_DEP_MAX_ORIGINS; i++) {
		if (hdr->sf_dep[i].origin_node != 0 || hdr->sf_dep[i].reserved_0 != 0
			|| !XLogRecPtrIsInvalid((XLogRecPtr)hdr->sf_dep[i].required_redo_lsn))
			return false;
	}
	return true;
}


/* ============================================================
 * Helpers for the spec-2.35 HC109 forwarding_master_node_bytes[4] field.
 *
 *	The field is stored as uint8[4] so the C compiler does not insert
 *	alignment padding before it (placing an int32 at offset 38 would
 *	otherwise require a 2-byte gap and expand the header from 48 to 56
 *	bytes — that would silently break the wire ABI lock above).  Wire
 *	encoding is little-endian, matching every other multi-byte field in
 *	the envelope (cluster_ic_envelope.h uses LE for magic / payload_crc
 *	/ etc).  GCS_BLOCK_REPLY_NO_FORWARDING_MASTER marks "direct from
 *	master, not forwarded"; node 0 is a valid forwarding master.
 * ============================================================ */
static inline int32
GcsBlockReplyHeaderGetForwardingMasterNode(const GcsBlockReplyHeader *hdr)
{
	int32 v;

	memcpy(&v, hdr->forwarding_master_node_bytes, sizeof(int32));
	return v;
}

static inline void
GcsBlockReplyHeaderSetForwardingMasterNode(GcsBlockReplyHeader *hdr, int32 node_id)
{
	memcpy(hdr->forwarding_master_node_bytes, &node_id, sizeof(int32));
}


/* ============================================================
 * GcsBlockForwardPayload -- wire ABI for PGRAC_IC_MSG_GCS_BLOCK_FORWARD
 *                          (spec-2.35 D2; HC102; master→holder direction).
 *
 *	When master decides to forward a GCS_BLOCK_REQUEST to an authorized
 *	holder (HC101: state==S + master not local-resident + bitmap has the
 *	holder bit), it emits this 64B payload to that holder.  Holder reads
 *	original_requester_node + requester_backend_id to direct-ship the
 *	GCS_BLOCK_REPLY (with status GRANTED_FROM_HOLDER + holder's node id
 *	as sender_node + forwarding_master_node = master_node) back to the
 *	original sender (skipping a proxy round-trip through master).
 *
 *	Layout (64B; same size as GcsBlockRequestPayload for ring slot
 *	commonality, but with independent field semantics):
 *	  [  0,   8) request_id            -- echo from original request
 *	  [  8,  16) epoch                 -- master's epoch at forward time
 *	  [ 16,  36) tag                   -- BufferTag (PG-fact 20B)
 *	  [ 36,  40) original_requester_node -- "ship reply back to whom"
 *	  [ 40,  44) requester_backend_id  -- HC80 compound key
 *	  [ 44,  48) master_node           -- "this forward authorized by me"
 *	                                      (holder copies into reply.
 *	                                      forwarding_master_node)
 *	  [ 48,  49) transition_id         -- PcmLockTransition (1..9)
 *	  [ 49,  57) expected_pi_watermark_scn_bytes[8] -- spec-2.41 D1/D3
 *	                                      little-endian SCN (was page_lsn under
 *	                                      spec-2.37 HC127).  Master stamps
 *	                                      pi_watermark_scn(tag) so the holder can
 *	                                      validate the shipped page pd_block_scn
 *	                                      via gcs_block_lost_write_verdict()
 *	                                      before shipping;  InvalidScn = not
 *	                                      SCN-tracked.  Mixed-version incompatible
 *	                                      → gated by the spec-2.41 catversion bump.
 *	  [ 57,  64) reserved_0[7]         -- pad + future fields
 *
 *	HC109 pattern (same as GcsBlockReplyHeader.forwarding_master_node_bytes):
 *	use uint8[8] + memcpy helpers to encode the value little-endian; never
 *	declare `SCN expected_pi_watermark_scn` directly because struct padding
 *	rules would silently expand sizeof past 64B (codereview F1 P0 defense
 *	pattern from spec-2.35).
 * ============================================================ */
typedef struct GcsBlockForwardPayload {
	uint64 request_id;						  /*  8B [  0,   8) */
	uint64 epoch;							  /*  8B [  8,  16) */
	BufferTag tag;							  /* 20B [ 16,  36) */
	int32 original_requester_node;			  /*  4B [ 36,  40) */
	int32 requester_backend_id;				  /*  4B [ 40,  44) */
	int32 master_node;						  /*  4B [ 44,  48) */
	uint8 transition_id;					  /*  1B [ 48,  49) */
	uint8 expected_pi_watermark_scn_bytes[8]; /*  8B [ 49,  57) spec-2.41 D1/D3 (was lsn) */
	uint8 reserved_0[7];					  /*  7B [ 57,  64) */
} GcsBlockForwardPayload;

StaticAssertDecl(sizeof(GcsBlockForwardPayload) == 64,
				 "spec-2.35 D2 / spec-2.41 D1 GcsBlockForwardPayload wire ABI 64B "
				 "(request_id 8 + epoch 8 + tag 20 + original_requester_node 4 + "
				 "requester_backend_id 4 + master_node 4 + transition_id 1 + "
				 "expected_pi_watermark_scn_bytes[8] @ offset 49 + reserved_0[7] @ offset 57;  "
				 "sizeof 64B unchanged — same HC109 pattern as forwarding_master_node_bytes[4])");

StaticAssertDecl(offsetof(GcsBlockForwardPayload, expected_pi_watermark_scn_bytes) == 49,
				 "spec-2.41 D1 — expected_pi_watermark_scn_bytes[8] must land at "
				 "offset 49 immediately after transition_id byte at offset 48");

/* PGRAC: spec-2.41 D1/D3 — little-endian SCN helpers (the @49 carrier now holds
 * the detector's expected pi_watermark_scn, NOT a page_lsn). */
static inline void
GcsBlockForwardPayloadSetExpectedPiWatermarkScn(GcsBlockForwardPayload *p, SCN scn)
{
	uint64 v = (uint64)scn;

	p->expected_pi_watermark_scn_bytes[0] = (uint8)(v & 0xff);
	p->expected_pi_watermark_scn_bytes[1] = (uint8)((v >> 8) & 0xff);
	p->expected_pi_watermark_scn_bytes[2] = (uint8)((v >> 16) & 0xff);
	p->expected_pi_watermark_scn_bytes[3] = (uint8)((v >> 24) & 0xff);
	p->expected_pi_watermark_scn_bytes[4] = (uint8)((v >> 32) & 0xff);
	p->expected_pi_watermark_scn_bytes[5] = (uint8)((v >> 40) & 0xff);
	p->expected_pi_watermark_scn_bytes[6] = (uint8)((v >> 48) & 0xff);
	p->expected_pi_watermark_scn_bytes[7] = (uint8)((v >> 56) & 0xff);
}

static inline SCN
GcsBlockForwardPayloadGetExpectedPiWatermarkScn(const GcsBlockForwardPayload *p)
{
	uint64 v = 0;

	v |= (uint64)p->expected_pi_watermark_scn_bytes[0];
	v |= (uint64)p->expected_pi_watermark_scn_bytes[1] << 8;
	v |= (uint64)p->expected_pi_watermark_scn_bytes[2] << 16;
	v |= (uint64)p->expected_pi_watermark_scn_bytes[3] << 24;
	v |= (uint64)p->expected_pi_watermark_scn_bytes[4] << 32;
	v |= (uint64)p->expected_pi_watermark_scn_bytes[5] << 40;
	v |= (uint64)p->expected_pi_watermark_scn_bytes[6] << 48;
	v |= (uint64)p->expected_pi_watermark_scn_bytes[7] << 56;
	return (SCN)v;
}

/* Stage 8 R4 keeps the legacy 64-byte prefixes and appends these exact
 * byte-array extensions.  Multibyte fields are encoded explicitly little
 * endian; no packed/native cast is a wire authority. */
#define CLUSTER_R4_WIRE_VERSION ((uint8)1)
#define CLUSTER_R4_FORWARD_EXTENDED ((uint8)6)
#define GCS_BLOCK_FORWARD_KIND_CURRENT_MX_MEMBER_PROOF ((uint8)7)
#define GCS_BLOCK_FORWARD_KIND_CURRENT_MX_STATS ((uint8)8)
#define GCS_BLOCK_FORWARD_KIND_CURRENT_MX_DESCRIBE ((uint8)9)
#define CLUSTER_GCS_BLOCK_R4_INTERNAL_ENDPOINT ((int32)-2)

StaticAssertDecl(CLUSTER_R4_FORWARD_EXTENDED
					 < GCS_BLOCK_FORWARD_KIND_CURRENT_MX_MEMBER_PROOF,
				 "current MX request domain must follow the closed R4 kind");
StaticAssertDecl(GCS_BLOCK_FORWARD_KIND_CURRENT_MX_STATS
					 == GCS_BLOCK_FORWARD_KIND_CURRENT_MX_MEMBER_PROOF + 1
					 && GCS_BLOCK_FORWARD_KIND_CURRENT_MX_DESCRIBE
							== GCS_BLOCK_FORWARD_KIND_CURRENT_MX_STATS + 1,
				 "current MX request kind allocation changed");

static inline bool
GcsBlockForwardPayloadIsCurrentMxMemberProof(const GcsBlockForwardPayload *payload)
{
	return payload != NULL
		   && payload->reserved_0[6] == GCS_BLOCK_FORWARD_KIND_CURRENT_MX_MEMBER_PROOF;
}

static inline bool
GcsBlockForwardPayloadIsCurrentMxDescribe(const GcsBlockForwardPayload *payload)
{
	return payload != NULL
		   && payload->reserved_0[6] == GCS_BLOCK_FORWARD_KIND_CURRENT_MX_DESCRIBE;
}

static inline bool
GcsBlockForwardPayloadIsCurrentMxRuntime(const GcsBlockForwardPayload *payload)
{
	return GcsBlockForwardPayloadIsCurrentMxMemberProof(payload)
		   || GcsBlockForwardPayloadIsCurrentMxDescribe(payload);
}

/* Current-MX overlays the old BufferTag.  DATA sharding therefore uses only
 * the preserved request identity; this synthetic tag is never authority. */
static inline BufferTag
GcsBlockCurrentMxRouteTagMake(uint64 request_id, uint64 epoch,
							  int32 requester_node,
							  int32 requester_backend_id)
{
	BufferTag tag;

	memset(&tag, 0, sizeof(tag));
	tag.spcOid = (Oid)((epoch >> 32) ^ (uint64)(uint32)requester_node);
	tag.dbOid = (Oid)epoch;
	tag.relNumber = (RelFileNumber)requester_backend_id;
	tag.forkNum = MAIN_FORKNUM;
	tag.blockNum = (BlockNumber)(request_id ^ (request_id >> 32));
	return tag;
}

typedef enum ClusterCrBuildResult {
	CLUSTER_CR_BUILD_FULL = 0,
	CLUSTER_CR_BUILD_RETRYABLE = 1,
	CLUSTER_CR_BUILD_FAIL_CLOSED = 2
} ClusterCrBuildResult;

typedef enum ClusterCrBuildReason {
	CLUSTER_CR_BUILD_NONE = 0,
	CLUSTER_CR_BUILD_TARGET_DISABLED = 1,
	CLUSTER_CR_BUILD_RF_DEFERRED = 2,
	CLUSTER_CR_BUILD_WRONG_MASTER = 3,
	CLUSTER_CR_BUILD_NO_HOLDER = 4,
	CLUSTER_CR_BUILD_HOLDER_AMBIGUOUS = 5,
	CLUSTER_CR_BUILD_HOLDER_MOVED = 6,
	CLUSTER_CR_BUILD_RECOVERING = 7,
	CLUSTER_CR_BUILD_GENERATION_MISMATCH = 8,
	CLUSTER_CR_BUILD_CAPACITY = 9,
	CLUSTER_CR_BUILD_BAD_LOCATOR = 10,
	CLUSTER_CR_BUILD_BAD_UNDO = 11,
	CLUSTER_CR_BUILD_CHAIN_LIMIT = 12,
	CLUSTER_CR_BUILD_SNAPSHOT_TOO_OLD = 13,
	CLUSTER_CR_BUILD_EPOCH_MISMATCH = 14,
	CLUSTER_CR_BUILD_CANCELLED = 15,
	CLUSTER_CR_BUILD_IO_ERROR = 16,
	CLUSTER_CR_BUILD_PROTOCOL = 17
} ClusterCrBuildReason;

/*
 * Closed build-reason polarity.  Retryable reasons describe an operation
 * whose durable block/transaction authority was not changed; data/protocol
 * failures never acquire retry polarity by falling through an unknown enum.
 */
static inline ClusterCrBuildResult
cluster_cr_build_result_for_reason(ClusterCrBuildReason reason)
{
	switch (reason) {
		case CLUSTER_CR_BUILD_NONE:
			return CLUSTER_CR_BUILD_FULL;
		case CLUSTER_CR_BUILD_TARGET_DISABLED:
		case CLUSTER_CR_BUILD_RF_DEFERRED:
		case CLUSTER_CR_BUILD_WRONG_MASTER:
		case CLUSTER_CR_BUILD_NO_HOLDER:
		case CLUSTER_CR_BUILD_HOLDER_AMBIGUOUS:
		case CLUSTER_CR_BUILD_HOLDER_MOVED:
		case CLUSTER_CR_BUILD_RECOVERING:
		case CLUSTER_CR_BUILD_GENERATION_MISMATCH:
		case CLUSTER_CR_BUILD_CAPACITY:
		case CLUSTER_CR_BUILD_EPOCH_MISMATCH:
			return CLUSTER_CR_BUILD_RETRYABLE;
		case CLUSTER_CR_BUILD_BAD_LOCATOR:
		case CLUSTER_CR_BUILD_BAD_UNDO:
		case CLUSTER_CR_BUILD_CHAIN_LIMIT:
		case CLUSTER_CR_BUILD_SNAPSHOT_TOO_OLD:
		case CLUSTER_CR_BUILD_CANCELLED:
		case CLUSTER_CR_BUILD_IO_ERROR:
		case CLUSTER_CR_BUILD_PROTOCOL:
			return CLUSTER_CR_BUILD_FAIL_CLOSED;
	}
	return CLUSTER_CR_BUILD_FAIL_CLOSED;
}

typedef struct ClusterR4CrRouteProof {
	BufferTag tag;
	SCN read_scn;
	uint64 formation_epoch;
	uint64 activation_generation;
	uint64 master_authority_generation;
	uint64 master_resource_transition_count;
	SCN expected_page_scn;
	int32 real_master_node;
	int32 selected_holder_node;
} ClusterR4CrRouteProof;

StaticAssertDecl(sizeof(ClusterR4CrRouteProof) == 80,
				 "R4 CR route proof must remain 80 bytes");

/*
 * R4 D3 master-side policy over one coherent PCM snapshot.  NONE means one
 * canonical current holder was selected.  This helper neither queries nor
 * mutates authority; the production route wrapper supplies the one snapshot
 * and consumes the typed refusal.
 */
static inline ClusterCrBuildReason
cluster_r4_route_policy_classify(const PcmAuthoritySnapshot *authority, uint64 current_epoch,
								 uint64 master_authority_generation,
								 int32 *selected_holder_out)
{
	uint32 master_node;

	if (selected_holder_out != NULL)
		*selected_holder_out = -1;
	if (authority == NULL || selected_holder_out == NULL)
		return CLUSTER_CR_BUILD_PROTOCOL;
	if (authority->reserved[0] != 0 || authority->reserved[1] != 0)
		return CLUSTER_CR_BUILD_HOLDER_AMBIGUOUS;
	if (authority->pending_x_requester_node >= 0)
		return CLUSTER_CR_BUILD_RECOVERING;
	if (authority->pending_x_requester_node != -1 || authority->pending_x_since_lsn != 0)
		return CLUSTER_CR_BUILD_HOLDER_AMBIGUOUS;

	master_node = authority->master_holder.node_id;
	if (authority->state == PCM_STATE_N) {
		if (authority->x_holder_node != -1 || authority->s_holders_bitmap != 0
			|| master_node != UINT32_MAX)
			return CLUSTER_CR_BUILD_HOLDER_AMBIGUOUS;
		return CLUSTER_CR_BUILD_NO_HOLDER;
	}
	if (authority->state == PCM_STATE_X) {
		if (authority->x_holder_node < 0
			|| authority->x_holder_node >= RESOURCE_X_PROTOCOL_NODE_LIMIT
			|| authority->s_holders_bitmap != 0
			|| master_node != (uint32)authority->x_holder_node)
			return CLUSTER_CR_BUILD_HOLDER_AMBIGUOUS;
		*selected_holder_out = authority->x_holder_node;
	} else if (authority->state == PCM_STATE_S) {
		if (authority->x_holder_node != -1 || authority->s_holders_bitmap == 0
			|| master_node >= RESOURCE_X_PROTOCOL_NODE_LIMIT
			|| (authority->s_holders_bitmap & (UINT32_C(1) << master_node)) == 0)
			return CLUSTER_CR_BUILD_HOLDER_AMBIGUOUS;
		*selected_holder_out = (int32)master_node;
	} else
		return CLUSTER_CR_BUILD_HOLDER_AMBIGUOUS;

	if ((uint32)master_authority_generation == 0
		|| (uint32)(master_authority_generation >> 32) != (uint32)current_epoch
		|| authority->transition_count == 0 || authority->transition_count == UINT64_MAX) {
		*selected_holder_out = -1;
		return CLUSTER_CR_BUILD_GENERATION_MISMATCH;
	}
	return CLUSTER_CR_BUILD_NONE;
}

/* Duplicate route work is reusable only while every master proof scalar and
 * the selected canonical holder remain exact. */
static inline bool
cluster_r4_route_proof_matches(const ClusterR4CrRouteProof *armed, uint64 formation_epoch,
							   uint64 master_authority_generation,
							   int32 selected_holder_node,
							   uint64 master_resource_transition_count,
							   SCN expected_page_scn)
{
	return armed != NULL && selected_holder_node >= 0
		   && selected_holder_node < RESOURCE_X_PROTOCOL_NODE_LIMIT
		   && armed->formation_epoch == formation_epoch
		   && armed->master_authority_generation == master_authority_generation
		   && armed->selected_holder_node == selected_holder_node
		   && armed->master_resource_transition_count == master_resource_transition_count
		   && armed->expected_page_scn == expected_page_scn;
}

/* R4 §3.8 closed operation policy.  These are process-local policy values,
 * not wire, disk, shared-memory or SQL-visible ABI. */
typedef enum ClusterR4OperationState {
	CLUSTER_R4_STATE_INVALID = -1,
	CLUSTER_R4_STATE_EMPTY = 0,
	CLUSTER_R4_STATE_ROUTING,
	CLUSTER_R4_STATE_FORWARDED,
	CLUSTER_R4_STATE_BUILDING,
	CLUSTER_R4_STATE_WAIT_UNDO,
	CLUSTER_R4_STATE_REPLIED,
	CLUSTER_R4_STATE_CONSUMED,
	CLUSTER_R4_STATE_RETRYABLE,
	CLUSTER_R4_STATE_FAIL_CLOSED,
	CLUSTER_R4_STATE_CANCELLED,
	CLUSTER_R4_STATE_COUNT
} ClusterR4OperationState;

typedef enum ClusterR4OperationEvent {
	CLUSTER_R4_EVENT_VALID = 0,
	CLUSTER_R4_EVENT_DUPLICATE,
	CLUSTER_R4_EVENT_STALE,
	CLUSTER_R4_EVENT_TIMEOUT,
	CLUSTER_R4_EVENT_CANCEL,
	CLUSTER_R4_EVENT_DEATH,
	CLUSTER_R4_EVENT_RECONFIG,
	CLUSTER_R4_EVENT_CAPACITY,
	CLUSTER_R4_EVENT_MALFORMED,
	CLUSTER_R4_EVENT_COUNT
} ClusterR4OperationEvent;

typedef enum ClusterR4TransitionOwner {
	CLUSTER_R4_OWNER_REQUESTER = UINT32_C(1) << 0,
	CLUSTER_R4_OWNER_REAL_MASTER = UINT32_C(1) << 1,
	CLUSTER_R4_OWNER_HOLDER_LMON = UINT32_C(1) << 2,
	CLUSTER_R4_OWNER_HOLDER_LMS = UINT32_C(1) << 3,
	CLUSTER_R4_OWNER_ORIGIN = UINT32_C(1) << 4
} ClusterR4TransitionOwner;

typedef enum ClusterR4TransitionAction {
	CLUSTER_R4_ACTION_ARM_BEFORE_SEND = 0,
	CLUSTER_R4_ACTION_DROP,
	CLUSTER_R4_ACTION_NO_SEND,
	CLUSTER_R4_ACTION_RETRY,
	CLUSTER_R4_ACTION_FAIL_PROTOCOL,
	CLUSTER_R4_ACTION_MASTER_FORWARD,
	CLUSTER_R4_ACTION_REPLAY_ROUTE,
	CLUSTER_R4_ACTION_CLOSE_ATTEMPT,
	CLUSTER_R4_ACTION_CANCEL_ROUTE,
	CLUSTER_R4_ACTION_STALE_ROUTE,
	CLUSTER_R4_ACTION_HOLDER_STABLE_COPY,
	CLUSTER_R4_ACTION_DROP_OR_REPLAY,
	CLUSTER_R4_ACTION_CANCEL_SLOT,
	CLUSTER_R4_ACTION_STALE_SLOT,
	CLUSTER_R4_ACTION_REQUEST_UNDO_OR_PUBLISH,
	CLUSTER_R4_ACTION_ABORT_SCRATCH,
	CLUSTER_R4_ACTION_FAIL_DATA_PROTOCOL,
	CLUSTER_R4_ACTION_EXACT_UNDO_REPLY,
	CLUSTER_R4_ACTION_CONSUMER_CAS,
	CLUSTER_R4_ACTION_DROP_RESULT,
	CLUSTER_R4_ACTION_NEW_REQUEST,
	CLUSTER_R4_ACTION_OVERALL_DEADLINE,
	CLUSTER_R4_ACTION_CLEANUP,
	CLUSTER_R4_ACTION_WAIT_TOPOLOGY,
	CLUSTER_R4_ACTION_WAIT_ADMISSION,
	CLUSTER_R4_ACTION_BACKOFF
} ClusterR4TransitionAction;

typedef struct ClusterR4TransitionCell {
	ClusterR4OperationState next_state;
	ClusterR4OperationState alternate_state;
	uint32 owner_mask;
	ClusterR4TransitionAction action;
	const char *spec_action;
} ClusterR4TransitionCell;

#define CLUSTER_R4_ROUTE_OWNERS                                                                 \
	(CLUSTER_R4_OWNER_REQUESTER | CLUSTER_R4_OWNER_REAL_MASTER)
#define CLUSTER_R4_FORWARD_OWNERS                                                               \
	(CLUSTER_R4_OWNER_REAL_MASTER | CLUSTER_R4_OWNER_HOLDER_LMON)
#define CLUSTER_R4_UNDO_OWNERS                                                                  \
	(CLUSTER_R4_OWNER_HOLDER_LMON | CLUSTER_R4_OWNER_HOLDER_LMS | CLUSTER_R4_OWNER_ORIGIN)
#define CLUSTER_R4_CELL(next, alternate, owner, action, text)                                    \
	{ (next), (alternate), (owner), (action), (text) }

static inline const ClusterR4TransitionCell *
cluster_r4_transition_lookup(ClusterR4OperationState state, ClusterR4OperationEvent event)
{
	static const ClusterR4TransitionCell
		cluster_r4_transition_manifest[CLUSTER_R4_STATE_COUNT][CLUSTER_R4_EVENT_COUNT] = {
			{
				CLUSTER_R4_CELL(CLUSTER_R4_STATE_ROUTING, CLUSTER_R4_STATE_INVALID,
					CLUSTER_R4_OWNER_REQUESTER, CLUSTER_R4_ACTION_ARM_BEFORE_SEND,
					"R/arm-before-send"),
				CLUSTER_R4_CELL(CLUSTER_R4_STATE_EMPTY, CLUSTER_R4_STATE_INVALID,
					CLUSTER_R4_OWNER_REQUESTER, CLUSTER_R4_ACTION_DROP, "E/drop"),
				CLUSTER_R4_CELL(CLUSTER_R4_STATE_EMPTY, CLUSTER_R4_STATE_INVALID,
					CLUSTER_R4_OWNER_REQUESTER, CLUSTER_R4_ACTION_DROP, "E/drop"),
				CLUSTER_R4_CELL(CLUSTER_R4_STATE_EMPTY, CLUSTER_R4_STATE_INVALID,
					CLUSTER_R4_OWNER_REQUESTER, CLUSTER_R4_ACTION_DROP, "E/drop"),
				CLUSTER_R4_CELL(CLUSTER_R4_STATE_CANCELLED, CLUSTER_R4_STATE_INVALID,
					CLUSTER_R4_OWNER_REQUESTER, CLUSTER_R4_ACTION_NO_SEND, "K/no-send"),
				CLUSTER_R4_CELL(CLUSTER_R4_STATE_EMPTY, CLUSTER_R4_STATE_INVALID,
					CLUSTER_R4_OWNER_REQUESTER, CLUSTER_R4_ACTION_DROP, "E/drop"),
				CLUSTER_R4_CELL(CLUSTER_R4_STATE_EMPTY, CLUSTER_R4_STATE_INVALID,
					CLUSTER_R4_OWNER_REQUESTER, CLUSTER_R4_ACTION_DROP, "E/drop"),
				CLUSTER_R4_CELL(CLUSTER_R4_STATE_RETRYABLE, CLUSTER_R4_STATE_INVALID,
					CLUSTER_R4_OWNER_REQUESTER, CLUSTER_R4_ACTION_RETRY, "T/retry"),
				CLUSTER_R4_CELL(CLUSTER_R4_STATE_FAIL_CLOSED, CLUSTER_R4_STATE_INVALID,
					CLUSTER_R4_OWNER_REQUESTER, CLUSTER_R4_ACTION_FAIL_PROTOCOL,
					"X/FC(protocol)"),
			},
			{
				CLUSTER_R4_CELL(CLUSTER_R4_STATE_FORWARDED, CLUSTER_R4_STATE_INVALID,
					CLUSTER_R4_ROUTE_OWNERS, CLUSTER_R4_ACTION_MASTER_FORWARD,
					"F/master selects+forwards"),
				CLUSTER_R4_CELL(CLUSTER_R4_STATE_ROUTING, CLUSTER_R4_STATE_INVALID,
					CLUSTER_R4_ROUTE_OWNERS, CLUSTER_R4_ACTION_REPLAY_ROUTE,
					"R/replay same route"),
				CLUSTER_R4_CELL(CLUSTER_R4_STATE_RETRYABLE, CLUSTER_R4_STATE_INVALID,
					CLUSTER_R4_ROUTE_OWNERS, CLUSTER_R4_ACTION_RETRY, "T/retry"),
				CLUSTER_R4_CELL(CLUSTER_R4_STATE_RETRYABLE, CLUSTER_R4_STATE_INVALID,
					CLUSTER_R4_ROUTE_OWNERS, CLUSTER_R4_ACTION_CLOSE_ATTEMPT,
					"T/close attempt"),
				CLUSTER_R4_CELL(CLUSTER_R4_STATE_CANCELLED, CLUSTER_R4_STATE_INVALID,
					CLUSTER_R4_ROUTE_OWNERS, CLUSTER_R4_ACTION_CANCEL_ROUTE,
					"K/cancel route"),
				CLUSTER_R4_CELL(CLUSTER_R4_STATE_RETRYABLE, CLUSTER_R4_STATE_INVALID,
					CLUSTER_R4_ROUTE_OWNERS, CLUSTER_R4_ACTION_RETRY, "T/retry"),
				CLUSTER_R4_CELL(CLUSTER_R4_STATE_RETRYABLE, CLUSTER_R4_STATE_INVALID,
					CLUSTER_R4_ROUTE_OWNERS, CLUSTER_R4_ACTION_STALE_ROUTE,
					"T/stale route"),
				CLUSTER_R4_CELL(CLUSTER_R4_STATE_RETRYABLE, CLUSTER_R4_STATE_INVALID,
					CLUSTER_R4_ROUTE_OWNERS, CLUSTER_R4_ACTION_RETRY, "T/retry"),
				CLUSTER_R4_CELL(CLUSTER_R4_STATE_FAIL_CLOSED, CLUSTER_R4_STATE_INVALID,
					CLUSTER_R4_ROUTE_OWNERS, CLUSTER_R4_ACTION_FAIL_PROTOCOL,
					"X/FC(protocol)"),
			},
			{
				CLUSTER_R4_CELL(CLUSTER_R4_STATE_BUILDING, CLUSTER_R4_STATE_INVALID,
					CLUSTER_R4_FORWARD_OWNERS, CLUSTER_R4_ACTION_HOLDER_STABLE_COPY,
					"B/holder stable copy"),
				CLUSTER_R4_CELL(CLUSTER_R4_STATE_FORWARDED, CLUSTER_R4_STATE_INVALID,
					CLUSTER_R4_FORWARD_OWNERS, CLUSTER_R4_ACTION_DROP_OR_REPLAY,
					"F/drop or replay"),
				CLUSTER_R4_CELL(CLUSTER_R4_STATE_RETRYABLE, CLUSTER_R4_STATE_INVALID,
					CLUSTER_R4_FORWARD_OWNERS, CLUSTER_R4_ACTION_RETRY, "T/retry"),
				CLUSTER_R4_CELL(CLUSTER_R4_STATE_RETRYABLE, CLUSTER_R4_STATE_INVALID,
					CLUSTER_R4_FORWARD_OWNERS, CLUSTER_R4_ACTION_CLOSE_ATTEMPT,
					"T/close attempt"),
				CLUSTER_R4_CELL(CLUSTER_R4_STATE_CANCELLED, CLUSTER_R4_STATE_INVALID,
					CLUSTER_R4_FORWARD_OWNERS, CLUSTER_R4_ACTION_CANCEL_SLOT,
					"K/cancel slot"),
				CLUSTER_R4_CELL(CLUSTER_R4_STATE_RETRYABLE, CLUSTER_R4_STATE_INVALID,
					CLUSTER_R4_FORWARD_OWNERS, CLUSTER_R4_ACTION_RETRY, "T/retry"),
				CLUSTER_R4_CELL(CLUSTER_R4_STATE_RETRYABLE, CLUSTER_R4_STATE_INVALID,
					CLUSTER_R4_FORWARD_OWNERS, CLUSTER_R4_ACTION_STALE_SLOT,
					"T/stale slot"),
				CLUSTER_R4_CELL(CLUSTER_R4_STATE_RETRYABLE, CLUSTER_R4_STATE_INVALID,
					CLUSTER_R4_FORWARD_OWNERS, CLUSTER_R4_ACTION_RETRY, "T/retry"),
				CLUSTER_R4_CELL(CLUSTER_R4_STATE_FAIL_CLOSED, CLUSTER_R4_STATE_INVALID,
					CLUSTER_R4_FORWARD_OWNERS, CLUSTER_R4_ACTION_FAIL_PROTOCOL,
					"X/FC(protocol)"),
			},
			{
				CLUSTER_R4_CELL(CLUSTER_R4_STATE_WAIT_UNDO, CLUSTER_R4_STATE_REPLIED,
					CLUSTER_R4_OWNER_HOLDER_LMS, CLUSTER_R4_ACTION_REQUEST_UNDO_OR_PUBLISH,
					"U/request foreign undo or P/publish result"),
				CLUSTER_R4_CELL(CLUSTER_R4_STATE_BUILDING, CLUSTER_R4_STATE_INVALID,
					CLUSTER_R4_OWNER_HOLDER_LMS, CLUSTER_R4_ACTION_DROP, "B/drop"),
				CLUSTER_R4_CELL(CLUSTER_R4_STATE_RETRYABLE, CLUSTER_R4_STATE_INVALID,
					CLUSTER_R4_OWNER_HOLDER_LMS, CLUSTER_R4_ACTION_RETRY, "T/retry"),
				CLUSTER_R4_CELL(CLUSTER_R4_STATE_RETRYABLE, CLUSTER_R4_STATE_INVALID,
					CLUSTER_R4_OWNER_HOLDER_LMS, CLUSTER_R4_ACTION_ABORT_SCRATCH,
					"T/abort scratch"),
				CLUSTER_R4_CELL(CLUSTER_R4_STATE_CANCELLED, CLUSTER_R4_STATE_INVALID,
					CLUSTER_R4_OWNER_HOLDER_LMS, CLUSTER_R4_ACTION_ABORT_SCRATCH,
					"K/abort scratch"),
				CLUSTER_R4_CELL(CLUSTER_R4_STATE_RETRYABLE, CLUSTER_R4_STATE_INVALID,
					CLUSTER_R4_OWNER_HOLDER_LMS, CLUSTER_R4_ACTION_RETRY, "T/retry"),
				CLUSTER_R4_CELL(CLUSTER_R4_STATE_RETRYABLE, CLUSTER_R4_STATE_INVALID,
					CLUSTER_R4_OWNER_HOLDER_LMS, CLUSTER_R4_ACTION_ABORT_SCRATCH,
					"T/abort scratch"),
				CLUSTER_R4_CELL(CLUSTER_R4_STATE_RETRYABLE, CLUSTER_R4_STATE_INVALID,
					CLUSTER_R4_OWNER_HOLDER_LMS, CLUSTER_R4_ACTION_RETRY, "T/retry"),
				CLUSTER_R4_CELL(CLUSTER_R4_STATE_FAIL_CLOSED, CLUSTER_R4_STATE_INVALID,
					CLUSTER_R4_OWNER_HOLDER_LMS, CLUSTER_R4_ACTION_FAIL_DATA_PROTOCOL,
					"X/FC(data/protocol)"),
			},
			{
				CLUSTER_R4_CELL(CLUSTER_R4_STATE_BUILDING, CLUSTER_R4_STATE_INVALID,
					CLUSTER_R4_UNDO_OWNERS, CLUSTER_R4_ACTION_EXACT_UNDO_REPLY,
					"B/exact undo reply"),
				CLUSTER_R4_CELL(CLUSTER_R4_STATE_WAIT_UNDO, CLUSTER_R4_STATE_INVALID,
					CLUSTER_R4_UNDO_OWNERS, CLUSTER_R4_ACTION_DROP, "U/drop"),
				CLUSTER_R4_CELL(CLUSTER_R4_STATE_RETRYABLE, CLUSTER_R4_STATE_INVALID,
					CLUSTER_R4_UNDO_OWNERS, CLUSTER_R4_ACTION_RETRY, "T/retry"),
				CLUSTER_R4_CELL(CLUSTER_R4_STATE_RETRYABLE, CLUSTER_R4_STATE_INVALID,
					CLUSTER_R4_UNDO_OWNERS, CLUSTER_R4_ACTION_ABORT_SCRATCH,
					"T/abort scratch"),
				CLUSTER_R4_CELL(CLUSTER_R4_STATE_CANCELLED, CLUSTER_R4_STATE_INVALID,
					CLUSTER_R4_UNDO_OWNERS, CLUSTER_R4_ACTION_ABORT_SCRATCH,
					"K/abort scratch"),
				CLUSTER_R4_CELL(CLUSTER_R4_STATE_RETRYABLE, CLUSTER_R4_STATE_INVALID,
					CLUSTER_R4_UNDO_OWNERS, CLUSTER_R4_ACTION_RETRY, "T/retry"),
				CLUSTER_R4_CELL(CLUSTER_R4_STATE_RETRYABLE, CLUSTER_R4_STATE_INVALID,
					CLUSTER_R4_UNDO_OWNERS, CLUSTER_R4_ACTION_ABORT_SCRATCH,
					"T/abort scratch"),
				CLUSTER_R4_CELL(CLUSTER_R4_STATE_RETRYABLE, CLUSTER_R4_STATE_INVALID,
					CLUSTER_R4_UNDO_OWNERS, CLUSTER_R4_ACTION_RETRY, "T/retry"),
				CLUSTER_R4_CELL(CLUSTER_R4_STATE_FAIL_CLOSED, CLUSTER_R4_STATE_INVALID,
					CLUSTER_R4_UNDO_OWNERS, CLUSTER_R4_ACTION_FAIL_DATA_PROTOCOL,
					"X/FC(data/protocol)"),
			},
			{
				CLUSTER_R4_CELL(CLUSTER_R4_STATE_CONSUMED, CLUSTER_R4_STATE_INVALID,
					CLUSTER_R4_OWNER_REQUESTER, CLUSTER_R4_ACTION_CONSUMER_CAS,
					"C/exact consumer CAS"),
				CLUSTER_R4_CELL(CLUSTER_R4_STATE_REPLIED, CLUSTER_R4_STATE_INVALID,
					CLUSTER_R4_OWNER_REQUESTER, CLUSTER_R4_ACTION_DROP, "P/drop"),
				CLUSTER_R4_CELL(CLUSTER_R4_STATE_REPLIED, CLUSTER_R4_STATE_INVALID,
					CLUSTER_R4_OWNER_REQUESTER, CLUSTER_R4_ACTION_DROP, "P/drop"),
				CLUSTER_R4_CELL(CLUSTER_R4_STATE_REPLIED, CLUSTER_R4_STATE_INVALID,
					CLUSTER_R4_OWNER_REQUESTER, CLUSTER_R4_ACTION_DROP, "P/drop"),
				CLUSTER_R4_CELL(CLUSTER_R4_STATE_CANCELLED, CLUSTER_R4_STATE_INVALID,
					CLUSTER_R4_OWNER_REQUESTER, CLUSTER_R4_ACTION_DROP_RESULT,
					"K/drop result"),
				CLUSTER_R4_CELL(CLUSTER_R4_STATE_REPLIED, CLUSTER_R4_STATE_INVALID,
					CLUSTER_R4_OWNER_REQUESTER, CLUSTER_R4_ACTION_DROP, "P/drop"),
				CLUSTER_R4_CELL(CLUSTER_R4_STATE_REPLIED, CLUSTER_R4_STATE_INVALID,
					CLUSTER_R4_OWNER_REQUESTER, CLUSTER_R4_ACTION_DROP, "P/drop"),
				CLUSTER_R4_CELL(CLUSTER_R4_STATE_REPLIED, CLUSTER_R4_STATE_INVALID,
					CLUSTER_R4_OWNER_REQUESTER, CLUSTER_R4_ACTION_DROP, "P/drop"),
				CLUSTER_R4_CELL(CLUSTER_R4_STATE_REPLIED, CLUSTER_R4_STATE_INVALID,
					CLUSTER_R4_OWNER_REQUESTER, CLUSTER_R4_ACTION_DROP, "P/drop"),
			},
			{
				CLUSTER_R4_CELL(CLUSTER_R4_STATE_CONSUMED, CLUSTER_R4_STATE_INVALID,
					CLUSTER_R4_OWNER_REQUESTER, CLUSTER_R4_ACTION_DROP, "C/drop"),
				CLUSTER_R4_CELL(CLUSTER_R4_STATE_CONSUMED, CLUSTER_R4_STATE_INVALID,
					CLUSTER_R4_OWNER_REQUESTER, CLUSTER_R4_ACTION_DROP, "C/drop"),
				CLUSTER_R4_CELL(CLUSTER_R4_STATE_CONSUMED, CLUSTER_R4_STATE_INVALID,
					CLUSTER_R4_OWNER_REQUESTER, CLUSTER_R4_ACTION_DROP, "C/drop"),
				CLUSTER_R4_CELL(CLUSTER_R4_STATE_CONSUMED, CLUSTER_R4_STATE_INVALID,
					CLUSTER_R4_OWNER_REQUESTER, CLUSTER_R4_ACTION_DROP, "C/drop"),
				CLUSTER_R4_CELL(CLUSTER_R4_STATE_CONSUMED, CLUSTER_R4_STATE_INVALID,
					CLUSTER_R4_OWNER_REQUESTER, CLUSTER_R4_ACTION_DROP, "C/drop"),
				CLUSTER_R4_CELL(CLUSTER_R4_STATE_CONSUMED, CLUSTER_R4_STATE_INVALID,
					CLUSTER_R4_OWNER_REQUESTER, CLUSTER_R4_ACTION_DROP, "C/drop"),
				CLUSTER_R4_CELL(CLUSTER_R4_STATE_CONSUMED, CLUSTER_R4_STATE_INVALID,
					CLUSTER_R4_OWNER_REQUESTER, CLUSTER_R4_ACTION_DROP, "C/drop"),
				CLUSTER_R4_CELL(CLUSTER_R4_STATE_CONSUMED, CLUSTER_R4_STATE_INVALID,
					CLUSTER_R4_OWNER_REQUESTER, CLUSTER_R4_ACTION_DROP, "C/drop"),
				CLUSTER_R4_CELL(CLUSTER_R4_STATE_CONSUMED, CLUSTER_R4_STATE_INVALID,
					CLUSTER_R4_OWNER_REQUESTER, CLUSTER_R4_ACTION_DROP, "C/drop"),
			},
			{
				CLUSTER_R4_CELL(CLUSTER_R4_STATE_ROUTING, CLUSTER_R4_STATE_INVALID,
					CLUSTER_R4_OWNER_REQUESTER, CLUSTER_R4_ACTION_NEW_REQUEST,
					"R/new request id after typed close"),
				CLUSTER_R4_CELL(CLUSTER_R4_STATE_RETRYABLE, CLUSTER_R4_STATE_INVALID,
					CLUSTER_R4_OWNER_REQUESTER, CLUSTER_R4_ACTION_DROP, "T/drop"),
				CLUSTER_R4_CELL(CLUSTER_R4_STATE_RETRYABLE, CLUSTER_R4_STATE_INVALID,
					CLUSTER_R4_OWNER_REQUESTER, CLUSTER_R4_ACTION_DROP, "T/drop"),
				CLUSTER_R4_CELL(CLUSTER_R4_STATE_FAIL_CLOSED, CLUSTER_R4_STATE_INVALID,
					CLUSTER_R4_OWNER_REQUESTER, CLUSTER_R4_ACTION_OVERALL_DEADLINE,
					"X/overall deadline"),
				CLUSTER_R4_CELL(CLUSTER_R4_STATE_CANCELLED, CLUSTER_R4_STATE_INVALID,
					CLUSTER_R4_OWNER_REQUESTER, CLUSTER_R4_ACTION_CLEANUP, "K/cleanup"),
				CLUSTER_R4_CELL(CLUSTER_R4_STATE_RETRYABLE, CLUSTER_R4_STATE_INVALID,
					CLUSTER_R4_OWNER_REQUESTER, CLUSTER_R4_ACTION_WAIT_TOPOLOGY,
					"T/wait topology"),
				CLUSTER_R4_CELL(CLUSTER_R4_STATE_RETRYABLE, CLUSTER_R4_STATE_INVALID,
					CLUSTER_R4_OWNER_REQUESTER, CLUSTER_R4_ACTION_WAIT_ADMISSION,
					"T/wait admission"),
				CLUSTER_R4_CELL(CLUSTER_R4_STATE_RETRYABLE, CLUSTER_R4_STATE_INVALID,
					CLUSTER_R4_OWNER_REQUESTER, CLUSTER_R4_ACTION_BACKOFF, "T/backoff"),
				CLUSTER_R4_CELL(CLUSTER_R4_STATE_FAIL_CLOSED, CLUSTER_R4_STATE_INVALID,
					CLUSTER_R4_OWNER_REQUESTER, CLUSTER_R4_ACTION_FAIL_PROTOCOL,
					"X/FC(protocol)"),
			},
			{
				CLUSTER_R4_CELL(CLUSTER_R4_STATE_FAIL_CLOSED, CLUSTER_R4_STATE_INVALID,
					CLUSTER_R4_OWNER_REQUESTER, CLUSTER_R4_ACTION_DROP, "X/drop"),
				CLUSTER_R4_CELL(CLUSTER_R4_STATE_FAIL_CLOSED, CLUSTER_R4_STATE_INVALID,
					CLUSTER_R4_OWNER_REQUESTER, CLUSTER_R4_ACTION_DROP, "X/drop"),
				CLUSTER_R4_CELL(CLUSTER_R4_STATE_FAIL_CLOSED, CLUSTER_R4_STATE_INVALID,
					CLUSTER_R4_OWNER_REQUESTER, CLUSTER_R4_ACTION_DROP, "X/drop"),
				CLUSTER_R4_CELL(CLUSTER_R4_STATE_FAIL_CLOSED, CLUSTER_R4_STATE_INVALID,
					CLUSTER_R4_OWNER_REQUESTER, CLUSTER_R4_ACTION_DROP, "X/drop"),
				CLUSTER_R4_CELL(CLUSTER_R4_STATE_FAIL_CLOSED, CLUSTER_R4_STATE_INVALID,
					CLUSTER_R4_OWNER_REQUESTER, CLUSTER_R4_ACTION_CLEANUP, "X/cleanup"),
				CLUSTER_R4_CELL(CLUSTER_R4_STATE_FAIL_CLOSED, CLUSTER_R4_STATE_INVALID,
					CLUSTER_R4_OWNER_REQUESTER, CLUSTER_R4_ACTION_DROP, "X/drop"),
				CLUSTER_R4_CELL(CLUSTER_R4_STATE_FAIL_CLOSED, CLUSTER_R4_STATE_INVALID,
					CLUSTER_R4_OWNER_REQUESTER, CLUSTER_R4_ACTION_DROP, "X/drop"),
				CLUSTER_R4_CELL(CLUSTER_R4_STATE_FAIL_CLOSED, CLUSTER_R4_STATE_INVALID,
					CLUSTER_R4_OWNER_REQUESTER, CLUSTER_R4_ACTION_DROP, "X/drop"),
				CLUSTER_R4_CELL(CLUSTER_R4_STATE_FAIL_CLOSED, CLUSTER_R4_STATE_INVALID,
					CLUSTER_R4_OWNER_REQUESTER, CLUSTER_R4_ACTION_DROP, "X/drop"),
			},
			{
				CLUSTER_R4_CELL(CLUSTER_R4_STATE_CANCELLED, CLUSTER_R4_STATE_INVALID,
					CLUSTER_R4_OWNER_REQUESTER, CLUSTER_R4_ACTION_DROP, "K/drop"),
				CLUSTER_R4_CELL(CLUSTER_R4_STATE_CANCELLED, CLUSTER_R4_STATE_INVALID,
					CLUSTER_R4_OWNER_REQUESTER, CLUSTER_R4_ACTION_DROP, "K/drop"),
				CLUSTER_R4_CELL(CLUSTER_R4_STATE_CANCELLED, CLUSTER_R4_STATE_INVALID,
					CLUSTER_R4_OWNER_REQUESTER, CLUSTER_R4_ACTION_DROP, "K/drop"),
				CLUSTER_R4_CELL(CLUSTER_R4_STATE_CANCELLED, CLUSTER_R4_STATE_INVALID,
					CLUSTER_R4_OWNER_REQUESTER, CLUSTER_R4_ACTION_DROP, "K/drop"),
				CLUSTER_R4_CELL(CLUSTER_R4_STATE_CANCELLED, CLUSTER_R4_STATE_INVALID,
					CLUSTER_R4_OWNER_REQUESTER, CLUSTER_R4_ACTION_DROP, "K/drop"),
				CLUSTER_R4_CELL(CLUSTER_R4_STATE_CANCELLED, CLUSTER_R4_STATE_INVALID,
					CLUSTER_R4_OWNER_REQUESTER, CLUSTER_R4_ACTION_DROP, "K/drop"),
				CLUSTER_R4_CELL(CLUSTER_R4_STATE_CANCELLED, CLUSTER_R4_STATE_INVALID,
					CLUSTER_R4_OWNER_REQUESTER, CLUSTER_R4_ACTION_DROP, "K/drop"),
				CLUSTER_R4_CELL(CLUSTER_R4_STATE_CANCELLED, CLUSTER_R4_STATE_INVALID,
					CLUSTER_R4_OWNER_REQUESTER, CLUSTER_R4_ACTION_DROP, "K/drop"),
				CLUSTER_R4_CELL(CLUSTER_R4_STATE_CANCELLED, CLUSTER_R4_STATE_INVALID,
					CLUSTER_R4_OWNER_REQUESTER, CLUSTER_R4_ACTION_DROP, "K/drop"),
			},
		};

	if (state < CLUSTER_R4_STATE_EMPTY || state >= CLUSTER_R4_STATE_COUNT
		|| event < CLUSTER_R4_EVENT_VALID || event >= CLUSTER_R4_EVENT_COUNT)
		return NULL;
	return &cluster_r4_transition_manifest[state][event];
}

#undef CLUSTER_R4_CELL
#undef CLUSTER_R4_UNDO_OWNERS
#undef CLUSTER_R4_FORWARD_OWNERS
#undef CLUSTER_R4_ROUTE_OWNERS

extern const char *cluster_cr_build_reason_name(ClusterCrBuildReason reason);

typedef enum ClusterR4WireKind {
	CLUSTER_R4_WIRE_CR_BUILD = 1,
	CLUSTER_R4_WIRE_TX_RESOLVE = 2,
	CLUSTER_R4_WIRE_MULTI_RESOLVE = 3,
	CLUSTER_R4_WIRE_UNDO_DATA_FETCH = 4
} ClusterR4WireKind;

typedef struct ClusterR4RequestExtension {
	uint8 r4_version;
	uint8 r4_kind;
	uint8 flags_le[2];
	uint8 read_scn_le[8];
	uint8 reserved[4];
} ClusterR4RequestExtension;

typedef union ClusterR4ForwardKindUnion {
	uint8 locator_bytes[24];
	struct {
		uint8 master_authority_generation_le[8];
		uint8 master_resource_transition_count_le[8];
		uint8 expected_page_scn_le[8];
	} cr;
} ClusterR4ForwardKindUnion;

typedef struct ClusterR4ForwardExtension {
	uint8 r4_version;
	uint8 r4_kind;
	uint8 flags_le[2];
	ClusterR4ForwardKindUnion kind;
	uint8 subject_id_le[4];
} ClusterR4ForwardExtension;

StaticAssertDecl(sizeof(ClusterR4RequestExtension) == 16,
				 "R4 request extension must remain 16 bytes");
StaticAssertDecl(sizeof(ClusterR4ForwardKindUnion) == 24,
				 "R4 forward kind union must remain 24 bytes");
StaticAssertDecl(sizeof(ClusterR4ForwardExtension) == 32,
				 "R4 forward extension must remain 32 bytes");
StaticAssertDecl(offsetof(ClusterR4ForwardExtension,
						 kind.cr.master_resource_transition_count_le)
					 == 12,
				 "R4 FORWARD96 transition count must occupy absolute bytes 76..83");

typedef struct ClusterR4CrRequestPayload {
	GcsBlockRequestPayload base;
	ClusterR4RequestExtension extension;
} ClusterR4CrRequestPayload;

typedef struct ClusterR4CrForwardPayload {
	GcsBlockForwardPayload base;
	ClusterR4ForwardExtension extension;
} ClusterR4CrForwardPayload;

StaticAssertDecl(sizeof(ClusterR4CrRequestPayload) == 80,
				 "R4 CR request payload must remain 80 bytes");
StaticAssertDecl(sizeof(ClusterR4CrForwardPayload) == 96,
				 "R4 CR forward payload must remain 96 bytes");

struct ClusterICEnvelope;
extern ClusterCrBuildResult cluster_gcs_block_r4_route_cr(
	const struct ClusterICEnvelope *env, const ClusterR4CrRequestPayload *request,
	ClusterCrBuildReason *reason_out);
/* LMON close census: live requester slots in the exact R4_CR domain. */
extern uint64 cluster_gcs_block_r4_requester_count(void);
extern ClusterTxOutcome cluster_gcs_block_r4_tx_resolve_fetch_and_wait(
	int32 origin_node, const ClusterTxLocator *locator,
	uint32 expected_physical_generation, uint64 formation_epoch,
	ClusterTxResolution *out, ClusterTxResolveReason *reason_out);
extern void cluster_gcs_block_r4_tx_resolve_drain(void);
extern bool cluster_gcs_block_r4_tx_resolve_active(void);
#define CLUSTER_GCS_BLOCK_R4_TX_ORIGIN_PENDING_WAIT_MS 1
static inline long
cluster_gcs_block_r4_tx_resolve_wait_timeout_for_count(long idle_timeout_ms,
													int active_contexts)
{
	if (idle_timeout_ms <= 0 || active_contexts <= 0)
		return idle_timeout_ms;
	return idle_timeout_ms < CLUSTER_GCS_BLOCK_R4_TX_ORIGIN_PENDING_WAIT_MS
		? idle_timeout_ms
		: CLUSTER_GCS_BLOCK_R4_TX_ORIGIN_PENDING_WAIT_MS;
}
extern long cluster_gcs_block_r4_tx_resolve_wait_timeout(long idle_timeout_ms);
#ifdef USE_CLUSTER_UNIT
extern bool cluster_gcs_block_test_r4_request80(const struct ClusterICEnvelope *env,
											 const void *payload);
extern bool cluster_gcs_block_test_r4_forward96(const struct ClusterICEnvelope *env,
												 const void *payload);
extern int cluster_gcs_block_test_r4_tx_origin_context_count(void);
extern void cluster_gcs_block_test_r4_tx_origin_drain(void);
extern bool cluster_gcs_block_test_current_mx_forward128(
	const struct ClusterICEnvelope *env, const void *payload);
extern bool cluster_gcs_block_test_r4_refusal_status(ClusterCrBuildResult result,
											  ClusterCrBuildReason reason,
											  bool admitted_forward,
											  GcsBlockReplyStatus *status_out);
extern bool cluster_gcs_block_test_decode_r4_reply(
	const struct ClusterICEnvelope *env, const void *payload, uint64 expected_request_id,
	uint64 expected_epoch, int32 expected_requester_backend_id, uint8 expected_transition_id,
	int32 expected_sender_node, int32 expected_forwarding_master_node,
	uint8 expected_reply_domain);
extern bool cluster_gcs_block_test_arm_r4_reply_slot(uint64 request_id,
												 uint64 request_epoch,
												 int32 requester_backend_id,
												 uint8 transition_id,
												 int32 expected_master_node);
extern bool cluster_gcs_block_test_arm_legacy_reply_slot(uint64 request_id,
													 uint64 request_epoch,
													 int32 requester_backend_id,
													 uint8 transition_id,
													 int32 expected_master_node);
extern bool cluster_gcs_block_test_snapshot_r4_reply_slot(
	GcsBlockReplyHeader *header_out, char block_out[GCS_BLOCK_DATA_SIZE],
	bool *reply_received_out, uint64 *stale_drop_count_out);
extern bool cluster_gcs_block_test_r4_requester_arm(
	BufferTag tag, uint64 request_epoch, int32 expected_master_node,
	uint64 next_sequence, uint64 *request_id_out);
extern bool cluster_gcs_block_test_snapshot_r4_requester_slot(
	bool *in_use_out, uint8 *reply_domain_out, uint64 *request_id_out,
	uint8 *transition_id_out, BufferTag *tag_out, uint64 *request_epoch_out,
	int32 *expected_master_node_out, ClusterGcsBlockDirectState *direct_state_out,
	bool *direct_target_prepared_out);
extern bool cluster_gcs_block_test_release_r4_requester_slot(void);
extern bool cluster_gcs_block_test_r4_fetch_and_wait(
	BufferTag tag, SCN read_scn, int32 real_master_node,
	char dst_page[GCS_BLOCK_DATA_SIZE]);
#endif

static inline void
ClusterR4WireWriteU16(uint8 out[2], uint16 value)
{
	out[0] = (uint8)value;
	out[1] = (uint8)(value >> 8);
}

static inline uint16
ClusterR4WireReadU16(const uint8 in[2])
{
	return (uint16)((uint16)in[0] | ((uint16)in[1] << 8));
}

static inline void
ClusterR4WireWriteU32(uint8 out[4], uint32 value)
{
	int i;

	for (i = 0; i < 4; i++)
		out[i] = (uint8)(value >> (i * 8));
}

static inline uint32
ClusterR4WireReadU32(const uint8 in[4])
{
	uint32 value = 0;
	int i;

	for (i = 0; i < 4; i++)
		value |= (uint32)in[i] << (i * 8);
	return value;
}

static inline void
ClusterR4WireWriteU64(uint8 out[8], uint64 value)
{
	int i;

	for (i = 0; i < 8; i++)
		out[i] = (uint8)(value >> (i * 8));
}

static inline uint64
ClusterR4WireReadU64(const uint8 in[8])
{
	uint64 value = 0;
	int i;

	for (i = 0; i < 8; i++)
		value |= (uint64)in[i] << (i * 8);
	return value;
}

static inline bool
ClusterR4RequestExtensionSetCr(ClusterR4RequestExtension *extension, SCN read_scn)
{
	if (extension == NULL)
		return false;
	memset(extension, 0, sizeof(*extension));
	if (!SCN_VALID(read_scn))
		return false;
	extension->r4_version = CLUSTER_R4_WIRE_VERSION;
	extension->r4_kind = CLUSTER_R4_WIRE_CR_BUILD;
	ClusterR4WireWriteU64(extension->read_scn_le, (uint64)read_scn);
	return true;
}

static inline bool
ClusterR4RequestExtensionGetCr(const ClusterR4RequestExtension *extension, SCN *read_scn_out)
{
	static const uint8 zero_flags[2] = { 0, 0 };
	static const uint8 zero_reserved[4] = { 0, 0, 0, 0 };
	SCN read_scn;

	if (read_scn_out != NULL)
		*read_scn_out = InvalidScn;
	if (extension == NULL || read_scn_out == NULL
		|| extension->r4_version != CLUSTER_R4_WIRE_VERSION
		|| extension->r4_kind != CLUSTER_R4_WIRE_CR_BUILD
		|| memcmp(extension->flags_le, zero_flags, sizeof(zero_flags)) != 0
		|| memcmp(extension->reserved, zero_reserved, sizeof(zero_reserved)) != 0)
		return false;

	read_scn = (SCN)ClusterR4WireReadU64(extension->read_scn_le);
	if (!SCN_VALID(read_scn))
		return false;
	*read_scn_out = read_scn;
	return true;
}

static inline void
ClusterR4ForwardExtensionSetCrProof(ClusterR4ForwardExtension *extension,
									uint64 master_authority_generation,
									uint64 master_resource_transition_count,
									SCN expected_page_scn)
{
	if (extension == NULL)
		return;
	memset(extension, 0, sizeof(*extension));
	extension->r4_version = CLUSTER_R4_WIRE_VERSION;
	extension->r4_kind = CLUSTER_R4_WIRE_CR_BUILD;
	ClusterR4WireWriteU64(extension->kind.cr.master_authority_generation_le,
						  master_authority_generation);
	ClusterR4WireWriteU64(extension->kind.cr.master_resource_transition_count_le,
						  master_resource_transition_count);
	ClusterR4WireWriteU64(extension->kind.cr.expected_page_scn_le,
						  (uint64)expected_page_scn);
}

static inline bool
ClusterR4ForwardExtensionGetCrProof(const ClusterR4ForwardExtension *extension,
									uint64 formation_epoch,
									uint64 *master_authority_generation_out,
									uint64 *master_resource_transition_count_out,
									SCN *expected_page_scn_out)
{
	static const uint8 zero_flags[2] = { 0, 0 };
	static const uint8 zero_subject[4] = { 0, 0, 0, 0 };
	uint64 master_generation;
	uint64 transition_count;

	if (master_authority_generation_out != NULL)
		*master_authority_generation_out = 0;
	if (master_resource_transition_count_out != NULL)
		*master_resource_transition_count_out = 0;
	if (expected_page_scn_out != NULL)
		*expected_page_scn_out = InvalidScn;
	if (extension == NULL || master_authority_generation_out == NULL
		|| master_resource_transition_count_out == NULL || expected_page_scn_out == NULL
		|| extension->r4_version != CLUSTER_R4_WIRE_VERSION
		|| extension->r4_kind != CLUSTER_R4_WIRE_CR_BUILD
		|| memcmp(extension->flags_le, zero_flags, sizeof(zero_flags)) != 0
		|| memcmp(extension->subject_id_le, zero_subject, sizeof(zero_subject)) != 0)
		return false;

	master_generation
		= ClusterR4WireReadU64(extension->kind.cr.master_authority_generation_le);
	transition_count
		= ClusterR4WireReadU64(extension->kind.cr.master_resource_transition_count_le);
	if ((uint32)master_generation == 0
		|| (uint32)(master_generation >> 32) != (uint32)formation_epoch
		|| transition_count == 0 || transition_count == UINT64_MAX)
		return false;

	*master_authority_generation_out = master_generation;
	*master_resource_transition_count_out = transition_count;
	*expected_page_scn_out
		= (SCN)ClusterR4WireReadU64(extension->kind.cr.expected_page_scn_le);
	return true;
}

static inline bool
ClusterR4ForwardExtensionSetLocator(ClusterR4ForwardExtension *extension,
									ClusterR4WireKind kind,
									const ClusterTxLocator *locator)
{
	if (extension != NULL)
		memset(extension, 0, sizeof(*extension));
	if (extension == NULL || locator == NULL
		|| (kind != CLUSTER_R4_WIRE_TX_RESOLVE
			&& kind != CLUSTER_R4_WIRE_UNDO_DATA_FETCH))
		return false;

	extension->r4_version = CLUSTER_R4_WIRE_VERSION;
	extension->r4_kind = (uint8)kind;
	ClusterR4WireWriteU64(&extension->kind.locator_bytes[0], locator->uba.raw[0]);
	ClusterR4WireWriteU64(&extension->kind.locator_bytes[8], locator->uba.raw[1]);
	ClusterR4WireWriteU32(&extension->kind.locator_bytes[16], (uint32)locator->xid);
	ClusterR4WireWriteU16(&extension->kind.locator_bytes[20], locator->tt_wrap);
	extension->kind.locator_bytes[22] = locator->itl_kind;
	extension->kind.locator_bytes[23] = locator->itl_slot_index;
	return true;
}

/* Kind-2/kind-4 bind the locator to the physical segment generation sampled
 * under BLOCK0_CURRENT SCUR.  Zero is a valid first generation; UINT32_MAX is
 * exhausted and cannot be published. */
static inline bool
ClusterR4ForwardExtensionSetLocatorGeneration(ClusterR4ForwardExtension *extension,
										   ClusterR4WireKind kind,
										   const ClusterTxLocator *locator,
										   uint32 physical_generation)
{
	if (extension != NULL)
		memset(extension, 0, sizeof(*extension));
	if (extension == NULL || locator == NULL || physical_generation == UINT32_MAX
		|| !ClusterR4ForwardExtensionSetLocator(extension, kind, locator))
		return false;
	ClusterR4WireWriteU32(extension->subject_id_le, physical_generation);
	return true;
}

static inline bool
ClusterR4ForwardExtensionGetLocator(const ClusterR4ForwardExtension *extension,
									ClusterR4WireKind expected_kind,
									ClusterTxLocator *locator_out)
{
	static const uint8 zero_flags[2] = { 0, 0 };
	static const uint8 zero_subject[4] = { 0, 0, 0, 0 };
	ClusterTxLocator decoded;

	if (locator_out != NULL)
		memset(locator_out, 0, sizeof(*locator_out));
	if (extension == NULL || locator_out == NULL
		|| (expected_kind != CLUSTER_R4_WIRE_TX_RESOLVE
			&& expected_kind != CLUSTER_R4_WIRE_UNDO_DATA_FETCH)
		|| extension->r4_version != CLUSTER_R4_WIRE_VERSION
		|| extension->r4_kind != (uint8)expected_kind
		|| memcmp(extension->flags_le, zero_flags, sizeof(zero_flags)) != 0
		|| memcmp(extension->subject_id_le, zero_subject, sizeof(zero_subject)) != 0)
		return false;

	memset(&decoded, 0, sizeof(decoded));
	decoded.uba.raw[0] = ClusterR4WireReadU64(&extension->kind.locator_bytes[0]);
	decoded.uba.raw[1] = ClusterR4WireReadU64(&extension->kind.locator_bytes[8]);
	decoded.xid = (TransactionId)ClusterR4WireReadU32(&extension->kind.locator_bytes[16]);
	decoded.tt_wrap = ClusterR4WireReadU16(&extension->kind.locator_bytes[20]);
	decoded.itl_kind = extension->kind.locator_bytes[22];
	decoded.itl_slot_index = extension->kind.locator_bytes[23];
	*locator_out = decoded;
	return true;
}

static inline bool
ClusterR4ForwardExtensionGetLocatorGeneration(const ClusterR4ForwardExtension *extension,
										   ClusterR4WireKind expected_kind,
										   ClusterTxLocator *locator_out,
										   uint32 *physical_generation_out)
{
	uint32 generation;
	ClusterR4ForwardExtension copy;

	if (locator_out != NULL)
		memset(locator_out, 0, sizeof(*locator_out));
	if (physical_generation_out != NULL)
		*physical_generation_out = 0;
	if (extension == NULL || locator_out == NULL || physical_generation_out == NULL)
		return false;
	generation = ClusterR4WireReadU32(extension->subject_id_le);
	if (generation == UINT32_MAX)
		return false;
	copy = *extension;
	memset(copy.subject_id_le, 0, sizeof(copy.subject_id_le));
	if (!ClusterR4ForwardExtensionGetLocator(&copy, expected_kind, locator_out))
		return false;
	*physical_generation_out = generation;
	return true;
}

#define CLUSTER_R4_TX_VERDICT_MAGIC UINT32_C(0x50475556)
#define CLUSTER_R4_TX_VERDICT_VERSION UINT16_C(3)
#define CLUSTER_R4_TX_VERDICT_HEADER_LEN UINT16_C(80)

/*
 * Exact V3 transaction-verdict payload carried at the head of a BLCKSZ reply
 * page.  The page is encoded bytewise so compiler packing and host endian are
 * never wire authority.  Bytes 80..BLCKSZ-1 are part of the canonical form
 * and must remain zero.
 */
static inline bool
ClusterR4TxVerdictPageEncode(uint8 page_out[BLCKSZ], const ClusterTxResolution *resolution)
{
	if (page_out != NULL)
		memset(page_out, 0, BLCKSZ);
	if (page_out == NULL || resolution == NULL
		|| !cluster_tx_outcome_proof_is_valid(resolution->outcome, resolution->proof_kind))
		return false;

	ClusterR4WireWriteU32(&page_out[0], CLUSTER_R4_TX_VERDICT_MAGIC);
	ClusterR4WireWriteU16(&page_out[4], CLUSTER_R4_TX_VERDICT_VERSION);
	ClusterR4WireWriteU16(&page_out[6], CLUSTER_R4_TX_VERDICT_HEADER_LEN);
	page_out[8] = (uint8)resolution->outcome;
	page_out[9] = (uint8)resolution->proof_kind;
	ClusterR4WireWriteU64(&page_out[12], resolution->locator_echo.uba.raw[0]);
	ClusterR4WireWriteU64(&page_out[20], resolution->locator_echo.uba.raw[1]);
	ClusterR4WireWriteU32(&page_out[28], (uint32)resolution->locator_echo.xid);
	ClusterR4WireWriteU16(&page_out[32], resolution->locator_echo.tt_wrap);
	page_out[34] = resolution->locator_echo.itl_kind;
	page_out[35] = resolution->locator_echo.itl_slot_index;
	ClusterR4WireWriteU32(&page_out[36], (uint32)resolution->top_xid);
	ClusterR4WireWriteU64(&page_out[40], (uint64)resolution->commit_scn);
	ClusterR4WireWriteU64(&page_out[48], (uint64)resolution->horizon_scn);
	ClusterR4WireWriteU64(&page_out[56], resolution->authority.origin_epoch);
	ClusterR4WireWriteU64(&page_out[64], resolution->authority.tt_generation);
	ClusterR4WireWriteU64(&page_out[72], (uint64)resolution->authority.authority_scn);
	return true;
}

static inline bool
ClusterR4TxVerdictPageDecode(const uint8 page[BLCKSZ], const ClusterTxLocator *expected_locator,
							 ClusterTxResolution *resolution_out)
{
	static const uint8 zero_tail[BLCKSZ - CLUSTER_R4_TX_VERDICT_HEADER_LEN] = { 0 };
	ClusterTxResolution decoded;

	if (resolution_out != NULL)
		memset(resolution_out, 0, sizeof(*resolution_out));
	if (page == NULL || expected_locator == NULL || resolution_out == NULL
		|| ClusterR4WireReadU32(&page[0]) != CLUSTER_R4_TX_VERDICT_MAGIC
		|| ClusterR4WireReadU16(&page[4]) != CLUSTER_R4_TX_VERDICT_VERSION
		|| ClusterR4WireReadU16(&page[6]) != CLUSTER_R4_TX_VERDICT_HEADER_LEN
		|| page[10] != 0 || page[11] != 0
		|| memcmp(&page[CLUSTER_R4_TX_VERDICT_HEADER_LEN], zero_tail, sizeof(zero_tail)) != 0)
		return false;

	memset(&decoded, 0, sizeof(decoded));
	decoded.outcome = (ClusterTxOutcome)page[8];
	decoded.proof_kind = (ClusterTxProofKind)page[9];
	if (!cluster_tx_outcome_proof_is_valid(decoded.outcome, decoded.proof_kind))
		return false;

	decoded.locator_echo.uba.raw[0] = ClusterR4WireReadU64(&page[12]);
	decoded.locator_echo.uba.raw[1] = ClusterR4WireReadU64(&page[20]);
	decoded.locator_echo.xid = (TransactionId)ClusterR4WireReadU32(&page[28]);
	decoded.locator_echo.tt_wrap = ClusterR4WireReadU16(&page[32]);
	decoded.locator_echo.itl_kind = page[34];
	decoded.locator_echo.itl_slot_index = page[35];
	if (!cluster_tx_locator_reply_matches(expected_locator,
									   &decoded.locator_echo))
		return false;

	decoded.top_xid = (TransactionId)ClusterR4WireReadU32(&page[36]);
	decoded.commit_scn = (SCN)ClusterR4WireReadU64(&page[40]);
	decoded.horizon_scn = (SCN)ClusterR4WireReadU64(&page[48]);
	decoded.authority.origin_epoch = ClusterR4WireReadU64(&page[56]);
	decoded.authority.live_hwm_lsn = InvalidXLogRecPtr;
	decoded.authority.tt_generation = ClusterR4WireReadU64(&page[64]);
	decoded.authority.authority_scn = (SCN)ClusterR4WireReadU64(&page[72]);
	*resolution_out = decoded;
	return true;
}

/* PGRAC: spec-2.41 D1 — pure lost-write verdict (the detector's SCN decision).
 *
 *	Compares a master's expected pi_watermark_scn(tag) against a shipped page's
 *	pd_block_scn (§2.6 three-branch).  Pure (no shmem / no locks) so the
 *	master-direct and holder-forward detectors share ONE decision and the unit
 *	tests can exercise every branch directly. */
typedef enum GcsLostWriteVerdict {
	GCS_LOST_WRITE_SKIP,		 /* expected InvalidScn: block not SCN-tracked (no fire) */
	GCS_LOST_WRITE_PASS,		 /* shipped >= expected: current version */
	GCS_LOST_WRITE_FAIL_STALE,	 /* both valid, shipped < expected: stale page */
	GCS_LOST_WRITE_FAIL_ANOMALY, /* expected valid, shipped InvalidScn: tracked-but-unstamped */
} GcsLostWriteVerdict;

/*
 * The single SCN lost-write decision shared by the master-direct and
 * holder-forward detectors (§2.6 three-branch).  `expected_scn` is the
 * master's pi_watermark_scn(tag);  `shipped_scn` is the pd_block_scn of the
 * page about to be shipped.  Cross-node version order is the global Lamport
 * SCN (AD-008), NEVER page_lsn (per-node WAL position; §0).  static inline so
 * it is pure (no shmem / no locks), inlinable in both detector paths, and
 * unit-testable from the header-only test binary.
 *
 *	expected InvalidScn                 -> SKIP    (block not SCN-tracked; no fire)
 *	expected valid, shipped InvalidScn  -> ANOMALY (tracked block ships an
 *	                                                 unstamped page — never PASS)
 *	both valid, shipped < expected      -> STALE   (true lost write)
 *	shipped >= expected                 -> PASS    (current)
 */
static inline GcsLostWriteVerdict
gcs_block_lost_write_verdict(SCN expected_scn, SCN shipped_scn)
{
	if (!SCN_VALID(expected_scn))
		return GCS_LOST_WRITE_SKIP;
	if (!SCN_VALID(shipped_scn))
		return GCS_LOST_WRITE_FAIL_ANOMALY;
	/* Compare by local_scn (the Lamport time order) — this IS scn_time_cmp's
	 * "only local_scn matters" contract.  A raw uint64 compare would be wrong:
	 * the SCN encodes node_id in the high 8 bits (cluster_scn.h), so raw `<`
	 * would let a higher-node_id watermark falsely flag a lower-node_id node's
	 * newer write as stale (the very cross-stream false-fire spec-2.41 fixes).
	 * scn_local() is extracted inline so the verdict stays pure / header-only
	 * testable; both operands are valid SCNs here (branches above). */
	if (scn_local(shipped_scn)
		< scn_local(expected_scn)) /* SCN_CMP_OK: scn_time_cmp via scn_local */
		return GCS_LOST_WRITE_FAIL_STALE;
	return GCS_LOST_WRITE_PASS;
}

/*
 * fix 2 (crash-rejoin re-declare barrier, defense in depth) — cold-GRD
 * watermark verdict.
 *
 * The storage-fallback / local-master freshness gate normally SKIPs when the
 * master pi_watermark_scn is InvalidScn (an old-binary master, a holder
 * re-ack whose requester copy is authoritative, or a block that is simply not
 * SCN-tracked).  But a crash-rejoined node's LOCAL GRD watermark was WIPED by
 * the restart, so within an active self-fence an InvalidScn watermark can mask
 * a stale home block whose peer holds a newer version — a SKIP there is a
 * silent fail-OPEN.  This is a second line behind the phase-gate boot barrier,
 * which already fences self-home blocks RECOVERING before the acquire reaches
 * the freshness gate; it exists so any future path that reaches the freshness
 * gate with a wiped watermark still fails closed.
 *
 * Pure truth table (header-only, unit-testable — no shmem, no I/O):
 *	expected_scn_valid                       -> PROVE       (run the normal verdict)
 *	!valid, no self-fence                    -> SKIP        (legit never-tracked / re-ack)
 *	!valid, self-fence, extension block      -> SKIP        (genuine new block, never
 *	                                                         cross-node written -> Invalid
 *	                                                         is correct; the storage refresh
 *	                                                         would read past EOF otherwise)
 *	!valid, self-fence, NOT an extension     -> FAIL_CLOSED (wiped/cold GRD watermark on a
 *	                                                         pre-existing block — ambiguous,
 *	                                                         must not serve, Rule 8.A)
 */
typedef enum ClusterColdGrdVerdict {
	CLUSTER_COLD_GRD_PROVE,		  /* watermark valid: run the lost-write verdict */
	CLUSTER_COLD_GRD_SKIP,		  /* Invalid watermark, provably safe to keep local */
	CLUSTER_COLD_GRD_FAIL_CLOSED, /* Invalid watermark under a self-fence: refuse */
} ClusterColdGrdVerdict;

static inline ClusterColdGrdVerdict
cluster_gcs_cold_grd_watermark_verdict(bool expected_scn_valid, bool self_fence_active,
									   bool is_extension_block)
{
	if (expected_scn_valid)
		return CLUSTER_COLD_GRD_PROVE;
	if (!self_fence_active)
		return CLUSTER_COLD_GRD_SKIP;
	if (is_extension_block)
		return CLUSTER_COLD_GRD_SKIP;
	return CLUSTER_COLD_GRD_FAIL_CLOSED;
}

/* PGRAC: spec-5.2 D2 — read-image intent flag carried in reserved_0[0].
 *
 *	When the master forwards an N→S read request to a node that holds the
 *	block in X, it sets this flag so the holder ships a one-shot read image
 *	(status READ_IMAGE_FROM_XHOLDER) and KEEPS its X, instead of the
 *	2-way-share GRANTED_FROM_HOLDER.  Reuses the existing 64B forward wire
 *	(no size change) — same reserved-byte-overlay pattern as HC127. */
static inline void
GcsBlockForwardPayloadSetReadImage(GcsBlockForwardPayload *p, bool read_image)
{
	p->reserved_0[0] = read_image ? (uint8)1 : (uint8)0;
}

static inline bool
GcsBlockForwardPayloadIsReadImage(const GcsBlockForwardPayload *p)
{
	return p->reserved_0[0] != 0;
}

/* PGRAC: spec-5.2 D11 — X-transfer (writer-transfer-revoke) intent flag carried
 * in reserved_0[1].
 *
 *	When THIS node is the GCS master for a block held in X by a REMOTE node and
 *	a LOCAL writer needs X (cross-node TX row-lock wait), the master forwards an
 *	N→X request to the holder with this flag set.  Unlike the 3-way
 *	X_GRANTED_FROM_HOLDER path (master is a third node; holder retains its X
 *	until the requester's post-install transition ACK reaches the master), the
 *	2-node local-master case has no separate ACK round-trip — the master IS the
 *	requester — so the holder must RELEASE its own X as it ships (invalidating
 *	its local copy so it can never flush a stale page; Rule 8.A no-stale-flush).
 *	The brief no-holder window is safe (no double-X);  the local master records
 *	itself as the new x_holder on install.  Reuses the existing 64B forward wire
 *	(no size change) — same reserved-byte-overlay pattern as read-image / HC127. */
static inline void
GcsBlockForwardPayloadSetXTransfer(GcsBlockForwardPayload *p, bool x_transfer)
{
	p->reserved_0[1] = x_transfer ? (uint8)1 : (uint8)0;
}

static inline bool
GcsBlockForwardPayloadIsXTransfer(const GcsBlockForwardPayload *p)
{
	return p->reserved_0[1] != 0;
}

/* PGRAC: spec-5.2a D1 — clean-page X-transfer eligibility flag carried in the
 * FORWARD payload's reserved_0[2].
 *
 *	v0.3 P0 FIX (reserved-byte collision):  reserved_0[0] is the spec-5.2 D2
 *	read-image flag and reserved_0[1] is the spec-5.2 D11 X-transfer flag
 *	(above).  The clean-page eligibility flag on the FORWARD wire therefore
 *	MUST NOT reuse [0]/[1] — it uses reserved_0[2] (the [2..6] range is free;
 *	reserved_0 is 7B at offset 57).  Set by the master when forwarding an
 *	eligible (sequence-refill) N→X to the holder so the holder uses the
 *	flush-data-before-drop path (spec-5.2a D4) rather than the no-data
 *	drop_no_wire path: the shared data file must reflect the current value
 *	after the drop so a later storage-fallback (stale-holder recovery) reads
 *	the current page, not a stale one (inv③, F0-11).  A heap / non-eligible
 *	forward leaves this 0 → existing behaviour unchanged (inv①). */
static inline void
GcsBlockForwardPayloadSetCleanEligible(GcsBlockForwardPayload *p, bool eligible)
{
	p->reserved_0[2] = eligible ? (uint8)1 : (uint8)0;
}

static inline bool
GcsBlockForwardPayloadIsCleanEligible(const GcsBlockForwardPayload *p)
{
	return p->reserved_0[2] != 0;
}

/* PGRAC: spec-6.12a ㉕ — remote-holder downgrade request flag carried in
 * reserved_0[3] ([0]=read-image, [1]=X-transfer, [2]=clean-eligible above;
 * [4..6] remain free).
 *
 *	Set by the master ALONGSIDE the read-image flag when forwarding an N→S
 *	read to a remote X holder and cluster.read_scache is on: the holder
 *	should TRY the quiescent X→S self-downgrade (flush + local flip + master
 *	notify) and, on success, ship a durable S grant
 *	(S_GRANTED_XHOLDER_DOWNGRADE) instead of the one-shot read image.
 *	Refusal (active ITL / raced / flush unavailable / notify send failure)
 *	falls back to the read-image ship — the flag is a request, never a
 *	command (Rule 8.A: the holder alone can judge quiescence).  A holder
 *	running with cluster.read_scache=off ignores the flag entirely
 *	(off-path byte-identical). */
static inline void
GcsBlockForwardPayloadSetDowngradeRequest(GcsBlockForwardPayload *p, bool downgrade)
{
	p->reserved_0[3] = downgrade ? (uint8)1 : (uint8)0;
}

static inline bool
GcsBlockForwardPayloadIsDowngradeRequest(const GcsBlockForwardPayload *p)
{
	/* PGRAC: spec-6.12e2 — value 2 in the same byte is the BAST-nudge
	 * variant (below); the ㉕ downgrade-with-ship request is exactly 1.
	 * Existing senders only ever wrote 0/1, so the narrowed predicate is
	 * wire-compatible. */
	return p->reserved_0[3] == 1;
}

/* PGRAC: spec-6.12e2 (㉔) — BAST nudge carried as VALUE 2 in the same
 * reserved_0[3] byte (all seven reserved bytes are taken; the ㉕ request
 * uses value 1, so the byte becomes a tiny enum: 0=none, 1=downgrade-
 * with-ship, 2=nudge-only).
 *
 *	Sent by the MASTER, fire-and-forget (request_id 0, no reply of any
 *	kind), when it must DENY an X request because another LIVE node
 *	holds X (the HG7 conservative deny): the holder's LMON should TRY
 *	the quiescent X→S self-downgrade NOW instead of waiting for a
 *	natural release, so the requester's bounded retry finds an
 *	S-invalidate-able holder and the grant proceeds (Oracle BAST → LMS
 *	background yield; never interrupts a foreground session).  Refusal
 *	(active ITL / pinned / raced / flush unavailable) leaves today's
 *	deny-retry path untouched — the nudge is advisory, never a command,
 *	and the e1 release-side path remains the fallback (§3.4b). */
static inline void
GcsBlockForwardPayloadSetBastNudge(GcsBlockForwardPayload *p)
{
	p->reserved_0[3] = (uint8)2;
}

static inline bool
GcsBlockForwardPayloadIsBastNudge(const GcsBlockForwardPayload *p)
{
	return p->reserved_0[3] == 2;
}

/* PGRAC: spec-6.12b — cross-instance CR request flag carried in reserved_0[4]
 * ([0]=read-image, [1]=X-transfer, [2]=clean-eligible, [3]=downgrade-request
 * above; [5..6] remain free).
 *
 *	Sent REQUESTER -> ORIGIN (the foreign undo home derived from the chain
 *	head UBA), riding the same 64B forward wire the sub-case B read-image
 *	path already sends requester->holder.  With this flag set the
 *	expected_pi_watermark_scn_bytes[8] carrier is REINTERPRETED as the
 *	requester's snapshot read_scn (both are SCN carriers; a CR result is
 *	historical by intent so the lost-write watermark verdict does not apply
 *	on this path).  master_node = the requester itself, so the HC108
 *	authorized chain on the direct-shipped reply validates exactly like the
 *	sub-case B flow.  The origin's LMON only VALIDATES + parks the request
 *	for LMS (light-work rule: construction never runs in the dispatch
 *	loop); LMS constructs, LMON ships CR_RESULT_FULL / CR_RESULT_PARTIAL,
 *	or a DENIED status which the requester maps to the unchanged 53R9G
 *	fail-closed (Rule 8.A). */
static inline void
GcsBlockForwardPayloadSetCrRequest(GcsBlockForwardPayload *p, bool cr_request)
{
	p->reserved_0[4] = cr_request ? (uint8)1 : (uint8)0;
}

static inline bool
GcsBlockForwardPayloadIsCrRequest(const GcsBlockForwardPayload *p)
{
	return p->reserved_0[4] != 0;
}

/* PGRAC: spec-6.13 D6 — direct-land arming flag on FORWARD uses
 * reserved_0[5].  The frozen spec text mentioned [3], but spec-6.12a already
 * uses [3] for downgrade-request and spec-6.12b uses [4] for CR request; [5]
 * is the first remaining free byte in the 7-byte FORWARD reserved area. */
static inline void
GcsBlockForwardPayloadSetDirectLandArmed(GcsBlockForwardPayload *p, bool armed)
{
	p->reserved_0[5] = armed ? (uint8)1 : (uint8)0;
}

static inline bool
GcsBlockForwardPayloadIsDirectLandArmed(const GcsBlockForwardPayload *p)
{
	return p->reserved_0[5] != 0;
}

/* PGRAC: spec-6.12i D-i1 — undo-TT fetch request carried in reserved_0[6]
 * VALUE 1 ([0]=read-image, [1]=X-transfer, [2]=clean-eligible,
 * [3]=downgrade-request/nudge, [4]=CR-request, [5]=spec-6.13 direct-land
 * above; [6] is the last byte, value-multiplexed like [3]: value 1 =
 * undo-TT fetch, values 2/3 = spec-6.15 D4 / spec-5.22f D6-7 xid verdict
 * sub-kinds, value 4 = spec-5.22d D4-6 dead-owner authority verdict — see
 * the per-kind banners below).
 *
 *	Sent REQUESTER -> ORIGIN, riding the same 64B forward wire as the
 *	spec-6.12b CR request.  With this flag set the BufferTag is a SYNTHETIC
 *	undo address (see GcsBlockUndoFetchTagMake below), NOT a heap block
 *	identity — the origin-side handler branches on this flag BEFORE any GRD
 *	/ holder logic can interpret the tag, exactly like the CR branch.  The
 *	origin's LMON validates + parks the request for LMS; LMS reads its OWN
 *	TT-bearing undo header block and co-samples the live authority triple
 *	{origin_epoch, live_hwm_lsn, tt_generation} (spec-6.12 §2.11 "live
 *	authority source") into the reply; LMON ships UNDO_TT_FETCH_RESULT with
 *	the ClusterGcsUndoAuthTrailer appended, or a DENIED status which the
 *	requester maps to the unchanged 53R97 fail-closed (Rule 8.A). */
static inline void
GcsBlockForwardPayloadSetUndoTtFetchRequest(GcsBlockForwardPayload *p, bool undo_fetch)
{
	p->reserved_0[6] = undo_fetch ? (uint8)1 : (uint8)0;
}

static inline bool
GcsBlockForwardPayloadIsUndoTtFetchRequest(const GcsBlockForwardPayload *p)
{
	return p->reserved_0[6] == (uint8)1;
}

/*
 * spec-6.13 D6 safety gate: a master must not propagate the requester's
 * direct-land flag to a forwarded holder unless the requester armed that exact
 * holder as the expected block-reply peer.  Until redirect/exact-holder arm
 * lands, all current forward paths pass exact_holder_arm=false.
 */
static inline void
GcsBlockForwardPayloadSetDirectLandFromRequest(GcsBlockForwardPayload *fwd,
											   const GcsBlockRequestPayload *req,
											   bool exact_holder_arm)
{
	GcsBlockForwardPayloadSetDirectLandArmed(
		fwd, exact_holder_arm && GcsBlockRequestPayloadIsDirectLandArmed(req));
}

/* PGRAC: spec-6.12i D-i4 / spec-6.15 D4 — undo-verdict request carried in
 * reserved_0[6] VALUE 2 (the byte is value-multiplexed with the undo-TT
 * fetch, value 1 — see the allocation list on the undo-fetch flag above;
 * one FORWARD is only ever one of the two request kinds).
 *
 *	Sent REQUESTER -> ORIGIN when the single-block positive proof came back
 *	NONE (0-match / ambiguity): ask the origin for a COMPLETE own-TT by-xid
 *	verdict instead.  The asked-for xid rides the expected-PI-watermark SCN
 *	carrier (widened to uint64; the upper 32 bits MUST be zero — the origin
 *	validates on decode), and the BufferTag stays the synthetic undo address
 *	of the ref's segment (kept for tag validity + observability only: the
 *	verdict scan is complete over ALL of the origin's own segments, so the
 *	segment field does not scope the answer).  The origin's LMON validates +
 *	parks for LMS; LMS serves ONLY xids the spec-6.15 stripe derivation
 *	proves its own (cluster_xid_is_mine — the D4 self-check), runs the
 *	complete durable-TT scan + CLOG cross-check + retention origin legs and
 *	ships UNDO_VERDICT_RESULT, or a DENIED status which the requester maps
 *	to the unchanged 53R97 fail-closed (Rule 8.A). */
/*
 * PGRAC: spec-5.22f D6-7 — reserved_0[6] value-multiplexes the verdict request
 * into two sub-kinds: VALUE 2 = a DERIVED verdict (the spec-6.15 D4 recycled
 * path, whose origin was derived from the xid value; the serve keeps the
 * cluster_xid_is_mine self-check that guards the 6.12i P0 wrong-origin match),
 * VALUE 5 = an AUTHORITATIVE verdict (the spec-5.22f fresh-ref path, whose
 * origin is the tuple page's PHYSICAL ITL binding — the requester already
 * proved this is the correct owner, so the serve skips the stripe pre-filter
 * and answers underivable own xids over its own durable-TT + CLOG authority;
 * the positive-proof gates are unchanged, Rule 8.A).
 *
 * spec-5.22f Hardening (RC#1 integration review): the AUTHORITATIVE sub-kind
 * originally reused VALUE 3, which COLLIDES with the spec-7.1 D3-b
 * undo-MULTI-verdict request (also VALUE 3 below).  IsUndoVerdictRequest then
 * matched a multi request and the forward handler's single-verdict branch stole
 * it before the multi branch, so a cross-node multixact member serve refused and
 * the requester fail-closed (t/359_mxid G5 red on the branch, green on main).
 * The byte legend is now 0=none, 1=undo-TT fetch, 2=derived verdict, 3=MULTI
 * verdict (7.1 D3-b, unchanged), 4=dead-owner authority verdict (5.22d D4-6),
 * 5=authoritative single verdict (moved off the multi value).  Multi keeps its
 * shipped value 3; only this unshipped-on-main sub-kind moves.
 */
#define GCS_BLOCK_FORWARD_KIND_UNDO_FRESHREF_C1B_PAIR ((uint8)10)
#define GCS_BLOCK_FORWARD_KIND_CURRENT_MX_CTRC_SEAL ((uint8)11)

StaticAssertDecl(GCS_BLOCK_FORWARD_KIND_UNDO_FRESHREF_C1B_PAIR
					 > GCS_BLOCK_FORWARD_KIND_CURRENT_MX_DESCRIBE,
				 "fresh-ref C1b pair kind must not collide with kinds 1..9");
StaticAssertDecl(GCS_BLOCK_FORWARD_KIND_CURRENT_MX_CTRC_SEAL
					 == GCS_BLOCK_FORWARD_KIND_UNDO_FRESHREF_C1B_PAIR + 1,
				 "CTRC seal request kind must remain appended after fresh-ref");

static inline void
GcsBlockForwardPayloadSetUndoVerdictRequest(GcsBlockForwardPayload *p, bool authoritative)
{
	p->reserved_0[6] = authoritative ? (uint8)5 : (uint8)2;
}

static inline void
GcsBlockForwardPayloadSetUndoFreshRefC1bPairRequest(GcsBlockForwardPayload *p)
{
	p->reserved_0[6] = GCS_BLOCK_FORWARD_KIND_UNDO_FRESHREF_C1B_PAIR;
}

static inline bool
GcsBlockForwardPayloadIsUndoFreshRefC1bPairRequest(const GcsBlockForwardPayload *p)
{
	return p->reserved_0[6] == GCS_BLOCK_FORWARD_KIND_UNDO_FRESHREF_C1B_PAIR;
}

static inline bool
GcsBlockForwardPayloadIsUndoVerdictRequest(const GcsBlockForwardPayload *p)
{
	return p->reserved_0[6] == (uint8)2 || p->reserved_0[6] == (uint8)5
		   || GcsBlockForwardPayloadIsUndoFreshRefC1bPairRequest(p);
}

static inline bool
GcsBlockForwardPayloadIsUndoVerdictAuthoritative(const GcsBlockForwardPayload *p)
{
	return p->reserved_0[6] == (uint8)5
		   || GcsBlockForwardPayloadIsUndoFreshRefC1bPairRequest(p);
}

/* PGRAC: spec-5.22d D4-6 — reserved_0[6] VALUE 4 = dead-owner AUTHORITY
 * verdict request (the byte's fourth value; 1 = undo-TT fetch, 2 = derived
 * verdict, 3 = authoritative verdict above).
 *
 *	Sent REQUESTER -> elected serve AUTHORITY (a live survivor, NOT the
 *	owner) when the undo OWNER of the asked-for xid is dead/absent: the
 *	owner's verdict wire has nobody behind it, so the deterministically
 *	elected survivor authority answers from the owner's durable shared
 *	block0 instead (spec-5.22d Route B).  Identity vs destination are
 *	deliberately separate layers: the request's OWNER (whose xid / whose
 *	undo segment) never changes and rides in tag.relNumber (see
 *	GcsBlockUndoAuthorityFetchTagMake below); only the serve DESTINATION
 *	moves to the authority.  The requester sends this kind ONLY to a peer
 *	that advertised PGRAC_IC_HELLO_CAP_UNDO_AUTHORITY_SERVE_V1 (an old
 *	binary never sees kind 4).  The serve side NEVER blind-trusts the wire:
 *	it re-checks request epoch == its current epoch, re-derives the
 *	authority for (owner, current epoch) and only serves when that is
 *	itself, then proves the verdict on the owner's block0 bytes
 *	(cluster_undo_authority_block0_prove — the same core the requester's
 *	self-authority leg runs).  Any failed check is a DENIED and the
 *	requester keeps the 53R97 fail-closed (Rule 8.A). */
static inline void
GcsBlockForwardPayloadSetUndoAuthorityVerdictRequest(GcsBlockForwardPayload *p)
{
	p->reserved_0[6] = (uint8)4;
}

static inline bool
GcsBlockForwardPayloadIsUndoAuthorityVerdictRequest(const GcsBlockForwardPayload *p)
{
	return p->reserved_0[6] == (uint8)4;
}

/* PGRAC: spec-7.1 D3-b — undo-MULTI-verdict request carried in reserved_0[6]
 * VALUE 3 (the byte is value-multiplexed with the undo-TT fetch (1) and the
 * single-xid verdict (2); one FORWARD is only ever one request kind).
 *
 *	Sent REQUESTER -> ORIGIN when a FOREIGN multixact xmax structurally misses
 *	the requester's member overlay (the updater has no compose-time TT
 *	binding, spec-7.1 IN-12): ask the origin, which alone owns the multi's
 *	members, for a batched per-member verdict.  The asked-for MXID rides the
 *	expected-PI-watermark SCN carrier (widened to uint64; upper 32 bits MUST
 *	be zero — MultiXactId is 32-bit like TransactionId, Q-D3b1) and the
 *	BufferTag stays the synthetic undo address for tag validity + routing
 *	observability only.  The origin's LMON parks for LMS; LMS gates on
 *	cluster_mxid_is_mine, enumerates the members, resolves each updater's
 *	terminal via the single-xid verdict path and ships
 *	UNDO_MULTI_VERDICT_RESULT (status SERVED) or a DENIED status the requester
 *	maps to the unchanged 53R97 fail-closed (Rule 8.A). */
static inline void
GcsBlockForwardPayloadSetUndoMultiVerdictRequest(GcsBlockForwardPayload *p, bool undo_multi_verdict)
{
	p->reserved_0[6] = undo_multi_verdict ? (uint8)3 : (uint8)0;
}

static inline bool
GcsBlockForwardPayloadIsUndoMultiVerdictRequest(const GcsBlockForwardPayload *p)
{
	return p->reserved_0[6] == (uint8)3;
}

/* PGRAC: spec-6.12i D-i1 — synthetic undo-address tag for the fetch wire.
 *
 *	An undo block has no BufferTag (undo segment files live outside shared
 *	buffers, addressed by (segment_id, owner_instance, block_no) through
 *	cluster_undo_smgr).  The fetch REUSES the forward payload's 20B tag
 *	field to carry that address: spcOid holds a magic discriminator (so a
 *	synthetic tag can never be confused with a real relation tag anywhere
 *	it might leak into observability), dbOid carries the segment_id and
 *	blockNum the block_no.  On the owner-served kinds (reserved_0[6] values
 *	1/2/3) the OWNER is deliberately NOT carried — the origin only ever
 *	serves its own undo (owner_instance derives from the serving node's own
 *	id), so a forged owner field cannot redirect the read (fail-closed by
 *	construction), and relNumber stays 0 (the LMON submit refuses a
 *	non-zero relNumber on those kinds).  The spec-5.22d D4-6 AUTHORITY kind
 *	(value 4) is the ONE exception: the dead OWNER's node id rides in the
 *	otherwise-empty relNumber as owner+1 (0 stays "absent"), because the
 *	serve destination is NOT the owner.  The serve side still never trusts
 *	it: the authority re-derivation + epoch check + block0 positive proof
 *	must all pass before a byte is answered (a forged owner is refused,
 *	never redirected-to). */
#define GCS_BLOCK_UNDO_FETCH_TAG_MAGIC ((Oid)0x50475549) /* "PGUI" */

static inline BufferTag
GcsBlockUndoFetchTagMake(uint32 segment_id, uint32 block_no)
{
	BufferTag tag;

	tag.spcOid = GCS_BLOCK_UNDO_FETCH_TAG_MAGIC;
	tag.dbOid = (Oid)segment_id;
	tag.relNumber = (RelFileNumber)0;
	tag.forkNum = MAIN_FORKNUM;
	tag.blockNum = (BlockNumber)block_no;
	return tag;
}

static inline bool
GcsBlockUndoFetchTagDecode(BufferTag tag, uint32 *segment_id, uint32 *block_no)
{
	if (tag.spcOid != GCS_BLOCK_UNDO_FETCH_TAG_MAGIC)
		return false;
	if (segment_id != NULL)
		*segment_id = (uint32)tag.dbOid;
	if (block_no != NULL)
		*block_no = (uint32)tag.blockNum;
	return true;
}

/* PGRAC: spec-5.22d D4-6 — authority-kind tag: the dead OWNER rides in the
 * otherwise-empty relNumber as owner+1 (see the banner above).  Decode is
 * shape-only (magic + non-zero owner carrier); range and self-exclusion are
 * the LMON submit's job, and TRUST is nobody's until the serve-side triple
 * check passes. */
static inline BufferTag
GcsBlockUndoAuthorityFetchTagMake(uint32 segment_id, uint32 block_no, int32 owner_node)
{
	BufferTag tag = GcsBlockUndoFetchTagMake(segment_id, block_no);

	tag.relNumber = (RelFileNumber)(owner_node + 1);
	return tag;
}

static inline bool
GcsBlockUndoAuthorityFetchTagDecodeOwner(BufferTag tag, int32 *owner_node)
{
	if (tag.spcOid != GCS_BLOCK_UNDO_FETCH_TAG_MAGIC)
		return false;
	if (tag.relNumber == (RelFileNumber)0)
		return false; /* owner absent: an owner-served-kind tag */
	if (owner_node != NULL)
		*owner_node = (int32)tag.relNumber - 1;
	return true;
}

/* S8-815PRE-FRESHREF-C1B-01: kind-10-only exact pairing tag.  The ordinary
 * owner-served kinds require relNumber == 0; kind 10 instead binds the raw xid
 * there while dbOid and blockNum retain the exact segment and 1-based TT slot.
 * The discriminator is validated separately before this decoder is called. */
static inline BufferTag
GcsBlockUndoFreshRefC1bTagMake(uint32 segment_id, TransactionId xid,
									 uint32 expected_tt_slot_id)
{
	BufferTag tag = GcsBlockUndoFetchTagMake(segment_id, expected_tt_slot_id);

	tag.relNumber = (RelFileNumber)xid;
	return tag;
}

static inline bool
GcsBlockUndoFreshRefC1bTagDecode(BufferTag tag, uint32 *segment_id,
									TransactionId *xid, uint32 *expected_tt_slot_id)
{
	TransactionId decoded_xid = (TransactionId)tag.relNumber;

	if (tag.spcOid != GCS_BLOCK_UNDO_FETCH_TAG_MAGIC || tag.forkNum != MAIN_FORKNUM
		|| tag.dbOid == (Oid)0 || (uint64)tag.dbOid > (uint64)UINT16_MAX
		|| !TransactionIdIsNormal(decoded_xid) || (RelFileNumber)decoded_xid != tag.relNumber
		|| tag.blockNum < 1 || tag.blockNum > TT_SLOTS_PER_SEGMENT)
		return false;
	if (segment_id != NULL)
		*segment_id = (uint32)tag.dbOid;
	if (xid != NULL)
		*xid = decoded_xid;
	if (expected_tt_slot_id != NULL)
		*expected_tt_slot_id = (uint32)tag.blockNum;
	return true;
}

/* PGRAC: spec-6.12i D-i1 — live-authority trailer appended after the BLCKSZ
 * page on UNDO_TT_FETCH_RESULT replies (wire size = 48B v1 header + 8192B
 * page + 16B trailer = 8256B; distinct from both the 8240B v1 and the 8504B
 * spec-6.2 v2 sizes, so the reply decoder discriminates by length exactly
 * like the v2 precedent).  Only tt_generation rides here: origin_epoch
 * reuses the header's epoch field (which the HC100 stale-reply check already
 * validates against the request epoch — a mid-reconfig reply drops, which IS
 * the D-i3 fail-closed) and live_hwm_lsn reuses page_lsn (zero on every
 * other served-page path).  Little-endian byte carrier, same pattern as
 * expected_pi_watermark_scn_bytes. */
typedef struct ClusterGcsUndoAuthTrailer {
	uint8 tt_generation_bytes[8]; /* origin TT retention-rollover generation */
	uint8 authority_scn_bytes[8]; /* PGRAC: spec-7.1a D3 -- origin SCN clock
								   * co-sampled with the content (LE); zero
								   * (InvalidScn) = absent (older peer) ->
								   * the covers gate refuses fail-closed */
} ClusterGcsUndoAuthTrailer;

StaticAssertDecl(sizeof(ClusterGcsUndoAuthTrailer) == 16,
				 "spec-6.12i ClusterGcsUndoAuthTrailer wire ABI is 16 bytes");

static inline void
ClusterGcsUndoAuthTrailerSetTtGeneration(ClusterGcsUndoAuthTrailer *t, uint64 tt_generation)
{
	t->tt_generation_bytes[0] = (uint8)(tt_generation & 0xff);
	t->tt_generation_bytes[1] = (uint8)((tt_generation >> 8) & 0xff);
	t->tt_generation_bytes[2] = (uint8)((tt_generation >> 16) & 0xff);
	t->tt_generation_bytes[3] = (uint8)((tt_generation >> 24) & 0xff);
	t->tt_generation_bytes[4] = (uint8)((tt_generation >> 32) & 0xff);
	t->tt_generation_bytes[5] = (uint8)((tt_generation >> 40) & 0xff);
	t->tt_generation_bytes[6] = (uint8)((tt_generation >> 48) & 0xff);
	t->tt_generation_bytes[7] = (uint8)((tt_generation >> 56) & 0xff);
}

static inline uint64
ClusterGcsUndoAuthTrailerGetTtGeneration(const ClusterGcsUndoAuthTrailer *t)
{
	return ((uint64)t->tt_generation_bytes[0]) | (((uint64)t->tt_generation_bytes[1]) << 8)
		   | (((uint64)t->tt_generation_bytes[2]) << 16)
		   | (((uint64)t->tt_generation_bytes[3]) << 24)
		   | (((uint64)t->tt_generation_bytes[4]) << 32)
		   | (((uint64)t->tt_generation_bytes[5]) << 40)
		   | (((uint64)t->tt_generation_bytes[6]) << 48)
		   | (((uint64)t->tt_generation_bytes[7]) << 56);
}

/* PGRAC: spec-7.1a D3 -- same little-endian carrier for the co-sampled
 * origin SCN clock (rides the former must-be-zero trailer bytes, so the
 * wire size is unchanged and an older peer's zero reads as InvalidScn). */
static inline void
ClusterGcsUndoAuthTrailerSetAuthorityScn(ClusterGcsUndoAuthTrailer *t, uint64 v)
{
	t->authority_scn_bytes[0] = (uint8)(v & 0xff);
	t->authority_scn_bytes[1] = (uint8)((v >> 8) & 0xff);
	t->authority_scn_bytes[2] = (uint8)((v >> 16) & 0xff);
	t->authority_scn_bytes[3] = (uint8)((v >> 24) & 0xff);
	t->authority_scn_bytes[4] = (uint8)((v >> 32) & 0xff);
	t->authority_scn_bytes[5] = (uint8)((v >> 40) & 0xff);
	t->authority_scn_bytes[6] = (uint8)((v >> 48) & 0xff);
	t->authority_scn_bytes[7] = (uint8)((v >> 56) & 0xff);
}

static inline uint64
ClusterGcsUndoAuthTrailerGetAuthorityScn(const ClusterGcsUndoAuthTrailer *t)
{
	uint64 v = (uint64)t->authority_scn_bytes[0];

	v |= (uint64)t->authority_scn_bytes[1] << 8;
	v |= (uint64)t->authority_scn_bytes[2] << 16;
	v |= (uint64)t->authority_scn_bytes[3] << 24;
	v |= (uint64)t->authority_scn_bytes[4] << 32;
	v |= (uint64)t->authority_scn_bytes[5] << 40;
	v |= (uint64)t->authority_scn_bytes[6] << 48;
	v |= (uint64)t->authority_scn_bytes[7] << 56;
	return v;
}

/* PGRAC: spec-6.12i D-i4 / spec-6.15 D4 — verdict page carried in the BLCKSZ
 * area of an UNDO_VERDICT_RESULT reply (rest of the page is zero; the reply
 * checksum covers the whole BLCKSZ area exactly like every other reply).
 *
 *	The verdict is the origin's answer over its COMPLETE own durable TT
 *	(cluster_tt_slot_durable_resolve_by_xid) cross-checked against its own
 *	CLOG (AD-006: CLOG is the committed-ness authority; the TT carries
 *	commit_scn), under the same co-sampled live authority carriage as the
 *	single-block fetch (hdr.epoch / hdr.page_lsn / auth trailer):
 *
 *	  COMMITTED_EXACT          exactly one COMMITTED slot match with a valid
 *	                           commit_scn, CLOG-confirmed, wrap-suspect gate
 *	                           passed (cluster_cr_accept_resolved_scn).
 *	  COMMITTED_BELOW_HORIZON  complete-scan 0-match + CLOG COMMITTED + the
 *	                           spec-3.22 retention origin legs: the xact's
 *	                           slot was horizon-gated-recycled, so its (lost)
 *	                           commit_scn is provably <= horizon_scn.  The
 *	                           exact value is gone — the requester may use
 *	                           the bound ONLY for a read_scn at/after the
 *	                           horizon (requester leg (e)), and must never
 *	                           cache/stamp the bound as an exact scn.
 *	  ABORTED                  terminal abort: either an exact ABORTED-slot
 *	                           match or complete-scan 0-match + explicit
 *	                           CLOG ABORTED.
 *
 *	horizon_scn doubles as the Lamport-observe carrier: an SCN that crossed
 *	the wire MUST be observed by the receiver (AD-008) so a horizon ahead of
 *	the requester's clock makes the NEXT snapshot admissible instead of
 *	failing leg (e) forever. */
#define CLUSTER_GCS_UNDO_VERDICT_MAGIC ((uint32)0x50475556) /* "PGUV" */
#define CLUSTER_GCS_UNDO_VERDICT_VERSION ((uint32)1)
/* PGRAC: spec-5.22d D4-6 — version 2 marks "dead-owner AUTHORITY-served"
 * provenance (Route B block0 prove).  An old requester's strict ==1 gate
 * refuses it (fail-closed, never mistaken for owner-served), and the new
 * authority leg accepts ONLY version 2 (an owner-served v1 page can never
 * masquerade as an authority serve).  Same 48-byte layout. */
#define CLUSTER_GCS_UNDO_VERDICT_VERSION_AUTHORITY ((uint32)2)

typedef enum ClusterGcsUndoVerdictKind {
	CLUSTER_GCS_UNDO_VERDICT_COMMITTED_EXACT = 1,
	CLUSTER_GCS_UNDO_VERDICT_COMMITTED_BELOW_HORIZON = 2,
	CLUSTER_GCS_UNDO_VERDICT_ABORTED = 3,
	/*
	 * S3-P0-13: positive NON-terminal proof.  The origin emits this only
	 * after exact fresh-ref segment/slot identity, own-stripe, stable
	 * RESOLVED_SCN, and ProcArray-live gates.  Canonical payload has no SCN
	 * and no wrap; it is never memoized or hint-stamped.
	 */
	CLUSTER_GCS_UNDO_VERDICT_IN_PROGRESS = 4
} ClusterGcsUndoVerdictKind;

typedef struct ClusterGcsUndoVerdictPage {
	uint32 magic;		 /* CLUSTER_GCS_UNDO_VERDICT_MAGIC */
	uint32 version;		 /* CLUSTER_GCS_UNDO_VERDICT_VERSION */
	uint64 xid_echo;	 /* asked-for xid widened to u64 (upper 32 bits zero) */
	uint8 verdict;		 /* ClusterGcsUndoVerdictKind */
	uint8 reserved_0[7]; /* must be zero */
	uint64 commit_scn;	 /* COMMITTED_EXACT only, else InvalidScn */
	uint64 horizon_scn;	 /* COMMITTED_BELOW_HORIZON bound, else InvalidScn */
	uint16 wrap;		 /* COMMITTED_EXACT slot wrap evidence */
	uint8 reserved_1[6]; /* must be zero */
} ClusterGcsUndoVerdictPage;

StaticAssertDecl(sizeof(ClusterGcsUndoVerdictPage) == 48,
				 "spec-6.12i ClusterGcsUndoVerdictPage wire ABI is 48 bytes");

/* PGRAC: spec-7.1 D3-b — batched multixact member-verdict page carried in the
 * BLCKSZ area of a GCS_BLOCK_REPLY_UNDO_MULTI_VERDICT_RESULT reply.
 *
 *	A foreign multixact xmax the requester cannot resolve locally (its member
 *	overlay structurally misses — the updater has no TT binding at compose
 *	time, spec-7.1 IN-12) is answered by the ORIGIN, which alone owns the
 *	multi's pg_multixact members: the origin enumerates the members
 *	(GetMultiXactIdMembers) and resolves each UPDATER member's terminal via
 *	the SAME by-xid verdict path as the single-xid serve (A1/A2).  lock-only
 *	members (status <= MultiXactStatusForUpdate) never gate visibility and
 *	carry no verdict.  The requester feeds the per-member terminals to the
 *	pure combination resolver cluster_multixact_resolve_visibility_served.
 *
 *	8.A (positive proof only): the origin ships this page ONLY when EVERY
 *	updater member has a proven terminal (status == SERVED).  A multi with an
 *	unprovable updater / not-mine mxid / unreadable member set is refused with
 *	a DENIED reply (no page) exactly like the single-verdict serve — the
 *	requester keeps its unchanged 53R97.  The status field is carried for
 *	defence-in-depth (the requester re-checks SERVED) and future
 *	observability.  Each member mirrors one ClusterGcsUndoVerdictPage's
 *	{commit_scn, horizon_scn, wrap, verdict}; horizon_scn crossings are
 *	Lamport-observed by the requester exactly as the single verdict's are. */
#define CLUSTER_GCS_UNDO_MULTI_VERDICT_MAGIC ((uint32)0x50474D56) /* "PGMV" */
#define CLUSTER_GCS_UNDO_MULTI_VERDICT_VERSION ((uint32)1)
#define CLUSTER_GCS_UNDO_MULTI_VERDICT_MAX_MEMBERS 256

/* Whole-multi serve status (A2 / Q-D3b3: origin never sends a partial set). */
typedef enum ClusterGcsUndoMultiVerdictStatus {
	CLUSTER_GCS_UNDO_MULTI_VERDICT_SERVED = 1,	   /* every updater member proven */
	CLUSTER_GCS_UNDO_MULTI_VERDICT_UNPROVABLE = 2, /* an updater member unprovable */
	CLUSTER_GCS_UNDO_MULTI_VERDICT_NOT_MINE = 3,   /* mxid not origin-derived-own */
	CLUSTER_GCS_UNDO_MULTI_VERDICT_NO_MEMBERS = 4  /* < 2 / unreadable member set */
} ClusterGcsUndoMultiVerdictStatus;

typedef struct ClusterGcsUndoMultiVerdictMember {
	uint64 commit_scn;	 /* COMMITTED_EXACT only, else InvalidScn */
	uint64 horizon_scn;	 /* COMMITTED_BELOW_HORIZON bound, else InvalidScn */
	TransactionId xid;	 /* member xid (uint32; NOT full-xid) */
	uint16 wrap;		 /* COMMITTED_EXACT slot wrap evidence */
	uint8 verdict;		 /* ClusterGcsUndoVerdictKind (1/2/3); 0 = lock-only none */
	uint8 member_status; /* MultiXactStatus: updater(4-5) vs lock-only(0-3) */
} ClusterGcsUndoMultiVerdictMember;

StaticAssertDecl(sizeof(ClusterGcsUndoMultiVerdictMember) == 24,
				 "spec-7.1 D3-b ClusterGcsUndoMultiVerdictMember wire ABI is 24 bytes");

typedef struct ClusterGcsUndoMultiVerdictPage {
	uint32 magic;		 /* CLUSTER_GCS_UNDO_MULTI_VERDICT_MAGIC */
	uint32 version;		 /* CLUSTER_GCS_UNDO_MULTI_VERDICT_VERSION */
	uint64 mxid_echo;	 /* asked-for mxid widened to u64 (upper 32 bits zero) */
	uint16 nmembers;	 /* 1..CLUSTER_GCS_UNDO_MULTI_VERDICT_MAX_MEMBERS */
	uint8 status;		 /* ClusterGcsUndoMultiVerdictStatus */
	uint8 reserved_0[5]; /* must be zero (pads members[] to 8-byte alignment) */
	ClusterGcsUndoMultiVerdictMember members[FLEXIBLE_ARRAY_MEMBER];
} ClusterGcsUndoMultiVerdictPage;

StaticAssertDecl(offsetof(ClusterGcsUndoMultiVerdictPage, members) == 24,
				 "spec-7.1 D3-b multi-verdict header is 24 bytes (members 8-aligned)");
StaticAssertDecl(offsetof(ClusterGcsUndoMultiVerdictPage, members)
						 + CLUSTER_GCS_UNDO_MULTI_VERDICT_MAX_MEMBERS
							   * sizeof(ClusterGcsUndoMultiVerdictMember)
					 <= BLCKSZ,
				 "spec-7.1 D3-b multi-verdict page (header + max members) must fit BLCKSZ");

/* PGRAC: spec-5.2 D2 — pure master-side decision for an N→S read request
 * when the block is held in X.  Kept pure (no shmem / no I/O) so the gate
 * truth table is unit-tested standalone (U3). */
typedef enum GcsXheldReadShipDecision {
	GCS_XHELD_READ_NOT_APPLICABLE = 0, /* not an X-held N→S read — existing logic */
	GCS_XHELD_READ_DIRECT_FROM_MASTER, /* master itself holds X resident → ship its image */
	GCS_XHELD_READ_FORWARD_TO_HOLDER,  /* a remote node holds X → forward read-image */
	GCS_XHELD_READ_DENY				   /* cannot satisfy safely → fail-closed (unchanged) */
} GcsXheldReadShipDecision;

static inline GcsXheldReadShipDecision
gcs_block_xheld_read_ship_decision(uint8 transition_id, int pre_state, int32 holder_node,
									   int32 requester_node, int32 master_node, bool master_resident)
{
	/* Only plain cross-node reads (N→S) on an X-held block are in scope. */
	if (transition_id != (uint8)PCM_TRANS_N_TO_S || pre_state != (int)PCM_LOCK_MODE_X)
		return GCS_XHELD_READ_NOT_APPLICABLE;

	/* A valid live holder must exist and it must not be the requester itself
	 * (a node never read-ships to itself). */
	if (holder_node < 0 || holder_node == requester_node)
		return GCS_XHELD_READ_DENY;

	/* The master holds X and the buffer is resident here → it can copy and
	 * ship its own current image directly. */
	if (holder_node == master_node && master_resident)
		return GCS_XHELD_READ_DIRECT_FROM_MASTER;

	/* A different live node holds X → forward a read-image request to it. */
	if (holder_node != master_node)
		return GCS_XHELD_READ_FORWARD_TO_HOLDER;

	/* Master is recorded as holder but the buffer is not resident (evicted /
	 * race) — cannot ship safely (Rule 8.A: never a silent stale read). */
	return GCS_XHELD_READ_DENY;
}

/* A native Resource-X head bars durable S admission, not Oracle-style
 * current-block shipping.  Permit that one-shot read only when two complete
 * authority samples are byte-exact and name one valid current X holder other
 * than the requester.  The returned fact is not authority and cannot be
 * widened into an X->S downgrade or S registration. */
static inline bool
gcs_block_xheld_read_barrier_bypass_exact(const PcmAuthoritySnapshot *before,
										  const PcmAuthoritySnapshot *after,
										  int32 requester_node)
{
	int32 holder_node;

	if (before == NULL || after == NULL || requester_node < 0
		|| requester_node >= RESOURCE_X_PROTOCOL_NODE_LIMIT
		|| memcmp(before, after, sizeof(*before)) != 0
		|| before->reserved[0] != 0 || before->reserved[1] != 0
		|| before->state != PCM_STATE_X || before->transition_count == 0
		|| before->transition_count == UINT64_MAX || before->s_holders_bitmap != 0)
		return false;
	holder_node = before->x_holder_node;
	return holder_node >= 0 && holder_node < RESOURCE_X_PROTOCOL_NODE_LIMIT
		   && holder_node != requester_node
		   && before->master_holder.node_id == (uint32)holder_node;
}

typedef enum GcsBlockSBarrierReadAction {
	GCS_BLOCK_S_BARRIER_NONE = 0,
	GCS_BLOCK_S_BARRIER_DENY,
	GCS_BLOCK_S_BARRIER_IMAGE_ONLY
} GcsBlockSBarrierReadAction;

static inline GcsBlockSBarrierReadAction
gcs_block_s_barrier_read_action_exact(bool queue_before, bool queue_after,
									  bool resource_x_before, bool resource_x_after,
									  const PcmAuthoritySnapshot *authority_before,
									  bool authority_before_valid,
									  const PcmAuthoritySnapshot *authority_after,
									  bool authority_after_valid, int32 requester_node)
{
	if (!queue_before && !queue_after && !resource_x_before && !resource_x_after)
		return GCS_BLOCK_S_BARRIER_NONE;
	if (queue_before || queue_after || (!resource_x_before && !resource_x_after)
		|| !authority_before_valid || !authority_after_valid)
		return GCS_BLOCK_S_BARRIER_DENY;
	return gcs_block_xheld_read_barrier_bypass_exact(
		authority_before, authority_after, requester_node)
		? GCS_BLOCK_S_BARRIER_IMAGE_ONLY
		: GCS_BLOCK_S_BARRIER_DENY;
}

/* A FORWARDED_IN_FLIGHT dedup record caches routing, never authority.  Reuse
 * it only while one coherent master snapshot still names the same canonical
 * holder and the same request class remains live.  In particular, an ordered
 * writer barrier invalidates an older read route before the X holder changes;
 * a committed Resource-X handoff invalidates it through the holder fields.
 * No PI bitmap participates in this decision. */
static inline bool
gcs_block_forward_replay_authority_exact(GcsBlockReplyStatus cached_status,
										 uint8 transition_id, int32 cached_holder_node,
										 int32 requester_node,
										 const PcmAuthoritySnapshot *authority)
{
	uint32 holder_bit;

	if (authority == NULL || cached_holder_node < 0
		|| cached_holder_node >= RESOURCE_X_PROTOCOL_NODE_LIMIT || requester_node < 0
		|| requester_node >= RESOURCE_X_PROTOCOL_NODE_LIMIT || cached_holder_node == requester_node
		|| authority->reserved[0] != 0 || authority->reserved[1] != 0)
		return false;
	holder_bit = UINT32_C(1) << cached_holder_node;

	switch (cached_status) {
	case GCS_BLOCK_REPLY_READ_IMAGE_FROM_XHOLDER:
		return transition_id == (uint8)PCM_TRANS_N_TO_S
			   && authority->state == PCM_STATE_X
			   && authority->x_holder_node == cached_holder_node
			   && authority->s_holders_bitmap == 0
			   && authority->master_holder.node_id == (uint32)cached_holder_node
			   && authority->pending_x_requester_node == -1
			   && authority->pending_x_since_lsn == 0;
	case GCS_BLOCK_REPLY_GRANTED_FROM_HOLDER:
		return transition_id == (uint8)PCM_TRANS_N_TO_S
			   && authority->state == PCM_STATE_S && authority->x_holder_node == -1
			   && (authority->s_holders_bitmap & holder_bit) != 0
			   && authority->master_holder.node_id == (uint32)cached_holder_node
			   && authority->pending_x_requester_node == -1
			   && authority->pending_x_since_lsn == 0;
	case GCS_BLOCK_REPLY_X_GRANTED_FROM_HOLDER:
		return (transition_id == (uint8)PCM_TRANS_N_TO_X
				|| transition_id == (uint8)PCM_TRANS_S_TO_X_UPGRADE)
			   && authority->state == PCM_STATE_X
			   && authority->x_holder_node == cached_holder_node
			   && authority->s_holders_bitmap == 0
			   && authority->master_holder.node_id == (uint32)cached_holder_node
			   && authority->pending_x_requester_node == requester_node
			   && authority->pending_x_since_lsn != 0;
	default:
		return false;
	}
}

/* A forwarded holder refusal invalidates the master's cached forwarding
 * marker.  The master intentionally answers the same request's first
 * re-entry with one direct refusal after removing that marker; only that
 * immediately following cleanup response is retryable.  No direct refusal
 * can start a retry round by itself. */
static inline bool
gcs_block_holder_refusal_retry_exact(int32 forwarding_master,
									 bool *awaiting_master_cleanup,
									 int retry_attempt, int max_retries)
{
	if (awaiting_master_cleanup == NULL || retry_attempt >= max_retries) {
		if (awaiting_master_cleanup != NULL)
			*awaiting_master_cleanup = false;
		return false;
	}
	if (forwarding_master != GCS_BLOCK_REPLY_NO_FORWARDING_MASTER) {
		*awaiting_master_cleanup = true;
		return true;
	}
	if (*awaiting_master_cleanup) {
		*awaiting_master_cleanup = false;
		return true;
	}
	return false;
}

/* A forwarded N->S refusal is a pre-mutation carrier drift, not a terminal
 * reader error.  Return through the existing outer retry boundary so bufmgr
 * first aborts the exact GRANT_PENDING reservation and the next attempt uses
 * a fresh request identity.  Direct master denials and writer transitions do
 * not qualify. */
static inline bool
gcs_block_forwarded_s_refusal_requires_fresh_retry(
	GcsBlockReplyStatus status, uint8 transition_id,
	int32 forwarding_master)
{
	return status == GCS_BLOCK_REPLY_DENIED_MASTER_NOT_HOLDER
		&& transition_id == (uint8)PCM_TRANS_N_TO_S
		&& forwarding_master >= 0 && forwarding_master < 32;
}

/* PGRAC: spec-5.2a D3 — pure master-side decision for an eligible clean-page
 * (sequence) X request.  Kept pure (no shmem / no I/O) so the 5-branch truth
 * table is unit-tested standalone (U3).  The handler runs ON the GCS master,
 * so `master` == cluster_node_id; `requester` is req->sender_node; `x_holder`
 * is the GRD-recorded X holder (or < 0 for none). */
typedef enum GcsCleanXferDecision {
	GCS_CLEAN_XFER_IDEMPOTENT = 0,	  /* x_holder == requester — already holds X */
	GCS_CLEAN_XFER_STORAGE_FALLBACK,  /* no holder — grant + read storage */
	GCS_CLEAN_XFER_SELF_SHIP,		  /* x_holder == master — path-B self-ship */
	GCS_CLEAN_XFER_FORWARD_TO_HOLDER, /* x_holder is other live, master == requester */
	GCS_CLEAN_XFER_THIRD_PARTY_DENY /* x_holder is other live, master ∉ {req,holder} (≥3 nodes) */
} GcsCleanXferDecision;

static inline GcsCleanXferDecision
gcs_block_clean_xfer_master_decision(int32 x_holder, int32 requester, int32 master)
{
	if (x_holder == requester)
		return GCS_CLEAN_XFER_IDEMPOTENT;
	if (x_holder < 0)
		return GCS_CLEAN_XFER_STORAGE_FALLBACK;
	if (x_holder == master)
		return GCS_CLEAN_XFER_SELF_SHIP;
	if (master == requester)
		return GCS_CLEAN_XFER_FORWARD_TO_HOLDER;
	return GCS_CLEAN_XFER_THIRD_PARTY_DENY;
}

/* PGRAC: spec-5.2a D3 — pure stale-holder predicate (U4).  True when an
 * eligible clean-page X-transfer got a holder DENIED_MASTER_NOT_HOLDER reply:
 * the holder is LIVE but no longer resident (it dropped to N), yet the master
 * still records it — the F0-4 stale-holder window.  Q3 amended 2026-06-21: the
 * action is now FAIL CLOSED (53R9X retryable), NOT storage-fallback recovery —
 * Stage-5 shared storage is not cross-instance coherent, so reading the page
 * from storage on the recovering node returns a stale view and reissues
 * sequence values (Rule 8.A violation, proven by t/284 L5).  The normal CF
 * image-ship path self-heals; a sound storage-fallback lands in Stage 6.  A
 * timeout (got_reply == false) is NOT this case (it cannot prove the holder
 * dropped) and stays fail-closed via the generic path. */
static inline bool
gcs_block_clean_xfer_should_stale_break(bool clean_eligible, bool got_reply, uint8 reply_status)
{
	return clean_eligible && got_reply
		   && reply_status == (uint8)GCS_BLOCK_REPLY_DENIED_MASTER_NOT_HOLDER;
}

/* Compile-time assertion that block size matches PG BLCKSZ.  HC80. */
StaticAssertDecl(GCS_BLOCK_DATA_SIZE == BLCKSZ,
				 "spec-2.33 D1 GCS_BLOCK_DATA_SIZE must equal BLCKSZ "
				 "(reply payload = header 48B + BLCKSZ block_data)");


/* ============================================================
 * Bufmgr helpers (implemented in src/backend/storage/buffer/bufmgr.c).
 *
 *	D4 lives in bufmgr.c because BufferDesc / partition lock internals are
 *	static there.  Declared here so cluster_gcs_block.c can call them and
 *	bufmgr.c sees a prototype for its definitions.
 * ============================================================ */
#include "access/xlogdefs.h"		  /* XLogRecPtr */
#include "cluster/cluster_pcm_lock.h" /* PcmLockMode for invalidate helper */
extern bool cluster_bufmgr_probe_block_for_gcs(BufferTag tag);
extern bool cluster_bufmgr_read_storage_scn_for_gcs(BufferTag tag, SCN *out_page_scn);
/* Process-local diagnostic returned by the nonblocking holder-copy helper.
 * This is observation only: the caller's bool success/deny contract and wire
 * reply status remain unchanged. */
typedef enum ClusterBufmgrGcsCopyRefusal {
	CLUSTER_BUFMGR_GCS_COPY_REFUSAL_NONE = 0,
	CLUSTER_BUFMGR_GCS_COPY_REFUSAL_INVALID_ARGUMENT,
	CLUSTER_BUFMGR_GCS_COPY_REFUSAL_NOT_RESIDENT,
	CLUSTER_BUFMGR_GCS_COPY_REFUSAL_CURRENT_INVALID,
	CLUSTER_BUFMGR_GCS_COPY_REFUSAL_CONTENT_LOCK_FIRST,
	CLUSTER_BUFMGR_GCS_COPY_REFUSAL_CONTENT_LOCK_SECOND,
	CLUSTER_BUFMGR_GCS_COPY_REFUSAL_OWNERSHIP_REVOKE_BUSY,
	CLUSTER_BUFMGR_GCS_COPY_REFUSAL_HC89_LSN_DRIFT,
	CLUSTER_BUFMGR_GCS_COPY_REFUSAL_SMART_FUSION_UNCLASSIFIED,
	CLUSTER_BUFMGR_GCS_COPY_REFUSAL_INJECTED_EVICT
} ClusterBufmgrGcsCopyRefusal;

/* A DATA worker cannot wait for BufferContent: its owner may itself be waiting
 * for that worker to deliver a reply.  This mapping is shared by master-direct
 * and holder-forward DATA replies.  Only the two conditional-lock misses are
 * retryable through the established fresh reservation/request boundary.
 * Residency/current-image failures remain structural, while HC89 keeps its
 * explicit one-retry hot-page bound. */
static inline GcsBlockReplyStatus
GcsBlockMasterDirectCopyRefusalStatus(ClusterBufmgrGcsCopyRefusal refusal)
{
	if (refusal == CLUSTER_BUFMGR_GCS_COPY_REFUSAL_CONTENT_LOCK_FIRST
		|| refusal == CLUSTER_BUFMGR_GCS_COPY_REFUSAL_CONTENT_LOCK_SECOND)
		return GCS_BLOCK_REPLY_DENIED_PENDING_X;
	return GCS_BLOCK_REPLY_DENIED_MASTER_NOT_HOLDER;
}

extern const char *cluster_bufmgr_gcs_copy_refusal_name(ClusterBufmgrGcsCopyRefusal refusal);
extern bool cluster_bufmgr_copy_block_for_gcs(BufferTag tag, XLogRecPtr *out_page_lsn, char *dst,
										  ClusterBufmgrGcsCopyRefusal *out_refusal);
extern bool cluster_bufmgr_copy_block_for_r4_cr(BufferTag tag, SCN expected_page_scn,
										XLogRecPtr *page_lsn_out, SCN *page_scn_out,
										char *dst,
										ClusterBufmgrGcsCopyRefusal *refusal_out);
extern bool cluster_bufmgr_borrow_block_for_gcs_live_sge(BufferTag tag, XLogRecPtr *out_page_lsn,
														 void **out_page_addr,
														 BufferDesc **out_buf);
extern void cluster_bufmgr_release_block_for_gcs_live_sge(BufferDesc *buf);
extern bool cluster_bufmgr_prepare_direct_land_target_for_gcs(BufferDesc *buf, BufferTag tag,
															  void **out_page_addr);
extern void cluster_bufmgr_finish_direct_land_target_for_gcs(BufferDesc *buf, bool valid,
															 XLogRecPtr page_lsn);
extern uint32 cluster_gcs_block_compute_checksum(const char *block_data);
extern bool cluster_bufmgr_copy_block_for_gcs_smart_fusion(BufferTag tag, XLogRecPtr *out_page_lsn,
														   char *dst, ClusterSfDepVec *out_dep_vec);
/* PGRAC: spec-2.36 D4 (HC118 / HC123) — by-tag invalidate wrapper for
 * holder-side INVALIDATE handler.  XLogFlush+InvalidateBuffer. */
extern PcmLockMode cluster_bufmgr_block_pcm_state(BufferTag tag);
/* PGRAC ownership-generation wave (W3): is a grant for this tag in flight to
 * install (GRANT_PENDING) on this node?  The invalidate handler consults it
 * before treating a pcm_state==N block as already-invalidated. */
extern bool cluster_bufmgr_block_grant_pending(BufferTag tag);
/* PGRAC ownership-generation wave (W3) test-only delivery shim: drive the real
 * invalidate handler with a synthetic same-tag directive from inside the
 * grant-finalize window (armed inject only; see cluster_gcs_block.c). */
extern bool cluster_gcs_block_test_deliver_self_invalidate(BufferTag tag);
/* PGRAC: spec-6.12g — no-fetch resident-buffer acquire for the commit-time
 * ITL stamp; residency proves ownership (a self-contained transfer drops the
 * copy).  InvalidBuffer -> block transferred away -> skip the stamp.
 * Superseded by the exact-proof helper below (spec-8.3); retained as the
 * rollback target until the formal acceptance runs pass. */
extern Buffer cluster_bufmgr_lock_resident_for_stamp(RelFileLocator rlocator, ForkNumber forknum,
													 BlockNumber blocknum);
extern void cluster_bufmgr_unlock_resident_stamp(Buffer buffer);

/*
 * PGRAC: spec-8.3 — exact-authority ITL terminal stamp acquisition.
 *
 *	ClusterItlStampSkipReason enumerates every safe no-stamp outcome of the
 *	no-fetch helper.  All values are hint skips, never errors: the slot's
 *	terminal state stays with TT/CLOG/undo.  A malformed proof or an
 *	impossible local ownership tuple is fail-closed inside the helper and
 *	never surfaces as one of these reasons.
 */
typedef enum ClusterItlStampSkipReason {
	CLUSTER_ITL_STAMP_SKIP_NONE = 0,
	CLUSTER_ITL_STAMP_SKIP_INVALID_PROOF,
	CLUSTER_ITL_STAMP_SKIP_NOT_RESIDENT,
	CLUSTER_ITL_STAMP_SKIP_TAG_CHANGED,
	CLUSTER_ITL_STAMP_SKIP_NOT_VALID,
	CLUSTER_ITL_STAMP_SKIP_NOT_CURRENT_IMAGE,
	CLUSTER_ITL_STAMP_SKIP_NOT_LOCAL_X,
	CLUSTER_ITL_STAMP_SKIP_OWNERSHIP_FLAGS_BUSY,
	CLUSTER_ITL_STAMP_SKIP_OWNERSHIP_GENERATION_CHANGED,
	CLUSTER_ITL_STAMP_SKIP_CLUSTER_EPOCH_CHANGED,
	CLUSTER_ITL_STAMP_SKIP_NO_ITL,
	CLUSTER_ITL_STAMP_SKIP_SLOT_OUT_OF_RANGE,
	CLUSTER_ITL_STAMP_SKIP_XID_CHANGED,
	CLUSTER_ITL_STAMP_SKIP_WRAP_CHANGED,
	CLUSTER_ITL_STAMP_SKIP_CLASS_CHANGED,
	CLUSTER_ITL_STAMP_SKIP_ALREADY_TERMINAL
} ClusterItlStampSkipReason;

/* PGRAC: spec-8.3 — read-only terminal-stamp authority projection.  Caller
 * must hold the exact buffer's content lock EXCLUSIVE.  Peer mode preserves
 * the process-local ACTIVE PCM-X holder-ledger contract unchanged.  Known
 * single-node storage mode may instead project only a quiescent PCM-N tuple
 * for the exact expected tag while recovery merge is inactive.  Returns the
 * captured generation, acquisition epoch and PCM state; creates no authority
 * or lifecycle state. */
extern bool cluster_bufmgr_terminal_stamp_authority(Buffer buffer,
												const BufferTag *expected_tag,
												uint64 *own_generation,
												uint64 *acquisition_epoch,
												uint8 *pcm_state);

/* PGRAC: spec-8.3 — no-fetch exact-proof acquire for the commit-time ITL
 * stamp.  Replaces the residency-only semantic: under mapping authority,
 * pin, content EXCLUSIVE and the buffer-header lock it revalidates the
 * exact tag, current image, local X, quiescent ownership tuple, captured
 * generation/epoch and the slot's xid/wrap/class before returning a
 * stampable buffer.  On any normal mismatch it unlocks/unpins and returns
 * InvalidBuffer with a typed skip reason; it never reads storage, never
 * invokes Cache Fusion, never retries and never ERRORs on a normal skip.
 * Release with cluster_bufmgr_unlock_resident_stamp(). */
struct ClusterItlTouchRecord; /* cluster/cluster_itl_touch.h */
extern Buffer
cluster_bufmgr_lock_resident_for_exact_itl_stamp(const struct ClusterItlTouchRecord *record,
												 ClusterItlStampSkipReason *out_reason);

/*
 * PGRAC: GCS serve-stall round-5 (A2) — bounded drop result.
 *
 *	The GCS drop/invalidate wrappers run in the LMS / LMON IC-dispatch
 *	context and must NEVER wait on a foreign buffer pin: the pin's holder
 *	may itself be waiting on a GCS reply only this dispatch loop can
 *	deliver (a circular wait resolved only by the reply-wait timeout —
 *	the measured 33-96s S3 serve-stall wall, R-state stack samples all
 *	parked in InvalidateBuffer's pin-wait retry loop).
 *
 *	  DROPPED       copy invalidated (or kept as a Past Image — the
 *	                caller checks cluster_bufmgr_block_is_pi as before);
 *	                page_lsn/page_scn outputs valid;  WAL flushed when
 *	                the page was dirty (HC123).
 *	  NOT_RESIDENT  no validly-resident copy (BufTable miss / tag moved /
 *	                !BM_VALID) — the pre-round-5 `false`.
 *	  PINNED        a foreign pin holds the buffer;  NOTHING was changed
 *	                (no state cleared, no flush relied upon).  The caller
 *	                parks the job and retries, or fail-closes with a
 *	                retryable deny — never spins.
 *	  STALE         GCS serve-stall round-6:  a local writer committed to
 *	                the page between the ship-image copy and this drop (the
 *	                page LSN advanced past the caller's expected copy-time
 *	                LSN), so the already-captured ship image is stale.
 *	                NOTHING was changed;  the caller MUST NOT grant the
 *	                stale image — it fail-closes with a retryable deny so
 *	                the re-serve copies the current page (the generation
 *	                binding that closes the copy->drop silent-lost-write
 *	                window, gaps (a)+(b)).
 */
typedef enum ClusterBufmgrGcsDropResult {
	CLUSTER_BUFMGR_GCS_DROP_DROPPED = 0,
	CLUSTER_BUFMGR_GCS_DROP_NOT_RESIDENT,
	CLUSTER_BUFMGR_GCS_DROP_PINNED,
	CLUSTER_BUFMGR_GCS_DROP_STALE,
} ClusterBufmgrGcsDropResult;

extern ClusterBufmgrGcsDropResult cluster_bufmgr_invalidate_block_for_gcs(BufferTag tag,
																		  PcmLockMode expected_mode,
																		  XLogRecPtr *out_page_lsn,
																		  SCN *out_page_scn);

/* PGRAC: GCS-race round-4c FUNC-1 — storage-fallback SCN verify / refresh.
 * read_block_scn: snapshot pd_block_scn under content_lock SHARED (page-
 * header contents are content-lock protected; a raw read could tear the
 * 8-byte SCN against a concurrent EXCLUSIVE writer).
 * refresh_block_from_storage: discard the CLEAN local bytes and re-read the
 * shared-storage page under content_lock EXCLUSIVE; returns false WITHOUT
 * touching the bytes when the buffer is dirty (caller fail-closes — dirt
 * could be a newer local version and must never be overwritten or flushed
 * over the newer storage copy).  Caller holds a pin (LockBuffer contract).
 * fallback_verify_refresh: the requester-side decision (verdict on the
 * local copy → PASS keep / stale → refresh + re-verdict → 53R93). */
extern SCN cluster_bufmgr_read_block_scn_for_gcs(BufferDesc *buf);
extern bool cluster_bufmgr_refresh_block_from_storage_for_gcs(BufferDesc *buf, SCN *out_page_scn);
/* fix 2 (crash-rejoin cold-GRD watermark) extension-block whitelist input. */
extern bool cluster_bufmgr_block_is_extension_for_gcs(BufferTag tag);
extern void cluster_gcs_block_fallback_verify_refresh(BufferDesc *buf, BufferTag tag,
													  SCN expected_scn);
/* PGRAC: spec-5.2 D11 (writer-transfer-revoke) — by-tag local buffer drop
 * with NO GCS release wire, for the holder-side X-transfer branch running in
 * the §3.5 IC-dispatch (LMON) context.  XLogFlush+InvalidateBuffer, with the
 * cache-eviction release wire suppressed (clears pcm_state=N first). */
extern ClusterBufmgrGcsDropResult
cluster_bufmgr_drop_block_for_gcs_no_wire(BufferTag tag, XLogRecPtr expected_lsn,
										  XLogRecPtr *out_page_lsn);

/* PGRAC: spec-6.12h D-h2 — Past Image discard helpers.
 * block_is_pi: does this tag's resident buffer hold a D-h1 Past Image
 * (BUF_TYPE_PI)?  Conversion sites use it to report kept-PI to the master.
 * discard_pi_block: drop the tag's buffer iff it is a real unpinned Past
 * Image (strictly type PI + !BM_VALID + refcount 0 — a current copy is
 * NEVER touched); false = no droppable PI (already implicitly discarded,
 * pinned by a racing re-reader, or never kept). */
extern bool cluster_bufmgr_block_is_pi(BufferTag tag);
extern bool cluster_bufmgr_discard_pi_block(BufferTag tag);

/* PGRAC: spec-6.12h D-h3b — copy a Past Image's frozen bytes + its D-h3a
 * ship-SCN stamp out of the buffer pool for a detached recovery rebuild.
 * True only when the tag maps to a stamped PI whose bytes provably did not
 * change during the copy (the D-h3a StartBufferIO reset seam makes the
 * post-copy shape recheck sufficient); false = no usable PI, the caller
 * falls back to storage + full redo (fail-safe, never an error). */
extern bool cluster_bufmgr_snapshot_pi_block(BufferTag tag, char *dst, SCN *out_ship_scn);

/* PGRAC: spec-6.12a — LOCAL-master S->X upgrade with remote-S invalidate.
 * Backend-context path for a writer on the master node whose block was
 * quiescent-downgraded: pending_x barrier + INVALIDATE broadcast via the
 * backend outbound ring + ack-certified bit clearing + S_TO_X_UPGRADE.
 * False = slot busy / ack timeout / raced state (caller stays on the
 * pre-6.12a bounded fail-closed, Rule 8.A). */
extern bool cluster_gcs_block_local_x_upgrade(BufferTag tag);
extern bool cluster_gcs_block_local_x_upgrade_ext(BufferTag tag, bool *out_busy);

/* PGRAC: spec-6.12a — master==holder quiescent X->S self-downgrade.  Flushes
 * a dirty page to shared storage first (every S copy stays storage-
 * consistent), applies PCM_TRANS_X_TO_S_DOWNGRADE, flips the local
 * pcm_state cache X->S.  False = not quiescent / not X / buffer gone /
 * master refused; caller falls back to the one-shot read-image ship. */
typedef enum ClusterBufmgrGcsDowngradeOutcome {
	CLUSTER_BUFMGR_GCS_DOWNGRADE_REFUSED_PRE_NOTIFY = 0,
	CLUSTER_BUFMGR_GCS_DOWNGRADE_COMMITTED,
	CLUSTER_BUFMGR_GCS_DOWNGRADE_FAILCLOSED_POST_NOTIFY
} ClusterBufmgrGcsDowngradeOutcome;
extern bool cluster_bufmgr_downgrade_x_to_s_for_gcs(BufferTag tag);
extern ClusterBufmgrGcsDowngradeOutcome cluster_bufmgr_downgrade_x_to_s_for_gcs_prepare_image(
	BufferTag tag, XLogRecPtr *out_page_lsn, char *dst, ClusterBufmgrGcsCopyRefusal *out_refusal);
extern bool cluster_bufmgr_downgrade_x_to_s_remote_for_gcs(BufferTag tag, int32 master_node);
extern ClusterBufmgrGcsDowngradeOutcome
cluster_bufmgr_downgrade_x_to_s_remote_for_gcs_prepare_image(
	BufferTag tag, int32 master_node, XLogRecPtr *out_page_lsn, char *dst,
	ClusterBufmgrGcsCopyRefusal *out_refusal);

/* PGRAC: GCS-race round-4c P1 — re-send the (idempotent) X->S downgrade
 * notify when a BAST nudge arrives for a block this node already holds in
 * S: the original fire-and-forget yield notify may have been lost on the
 * wire, leaving the master recording X@us and nudging forever. */
extern bool cluster_bufmgr_renotify_s_for_gcs(BufferTag tag, int32 master_node);

/* PGRAC: spec-5.2a D4 (backend eager flush) — flush a cluster sequence page to
 * shared storage from the BACKEND that just wrote it.  Caller holds a pin and
 * the buffer content lock (any mode; nextval/setval hold EXCLUSIVE).  Runs
 * FlushOneBuffer -> FlushBuffer (XLogFlush(page_lsn) WAL-before-data + smgrwrite
 * to shared storage), which is safe HERE because the backend's own WAL insert
 * is complete and flushable.  After this returns the page is clean and
 * storage-current, so a later cross-node clean X-transfer (LMON) only has to
 * drop a clean page (drop_block_for_gcs_clean_only) and a stale-holder
 * storage-fallback reads the current value.  Fails closed (ereport) on write
 * error via the underlying smgr path. */
extern void cluster_bufmgr_flush_seq_page_to_storage(Buffer buffer);

/* PGRAC: spec-5.2 §3.5 D11 (writer-transfer-revoke) — false
 * ONLY when this buffer is a deferred-writer read-image of a remote-X-held
 * block (pcm_state == PCM_STATE_READ_IMAGE); true otherwise.  The cluster_itl
 * forward-write path fails closed (retryable) on false so a writer never
 * mutates a non-owned copy (Rule 8.A multi-row fail-closed leg). */
extern bool cluster_bufmgr_block_write_permitted(Buffer buffer);

/* PGRAC: spec-5.13 D5b (clean-leave GCS flush seam) — a leaving node force-
 * persists every dirty block it holds X on to shared storage and releases that
 * X (pcm_state X -> N).  FlushBuffer is a bufmgr private static, so this seam
 * lives in bufmgr.c.  Runs in the leaving node's own backend/checkpointer
 * (CL-I9), never LMON.  Fail-closed (ereport) on write error.  Returns the
 * count of blocks flushed + X-released (CL-I5 / §0.3 命门). */
extern uint32 cluster_bufmgr_flush_and_release_x_for_leave(void);

/* PGRAC: spec-4.7 D2 (Q6-A' worker-centric) — bounded chunked scan of the
 * shared buffer pool that re-declares each locally-held S/X buffer.  The
 * callback receives (tag, held_mode, page_lsn, arg) per qualifying buffer;
 * cluster_bufmgr_redeclare_scan_chunk returns the next cursor (== NBuffers
 * once the whole pool has been scanned) so the LMON reconfig tick can drive
 * it in bounded chunks without blocking the heartbeat. */
typedef void (*ClusterGcsRedeclareCallback)(BufferTag tag, uint8 held_mode, XLogRecPtr page_lsn,
											SCN page_scn, void *arg); /* spec-2.41 D3 +page_scn */
extern int cluster_bufmgr_redeclare_scan_chunk(int start_buf, int max_scan,
											   ClusterGcsRedeclareCallback cb, void *arg);


/* ============================================================
 * Public API.
 * ============================================================ */

/*
 * cluster_gcs_send_block_request_and_wait -- request a block from the
 * deterministic master and block until the reply arrives (or timeout).
 *
 *  Caller boundary (spec-2.33 v0.2 F1):
 *    caller holds buffer pin on `buf` but MUST NOT hold content_lock
 *    when calling.  On GRANTED, the helper takes content_lock EXCLUSIVE
 *    to install block bytes + PageSetLSN (HC84) before returning.
 *
 *  Steps (HC80 + HC83 + HC84 + HC85):
 *    1. Reserve outstanding-slot (spec-2.32 D6 helper reuse)
 *    2. Build GcsBlockRequestPayload (request_id + requester_backend_id key)
 *    3. cluster_ic_send_envelope(master_node, GCS_BLOCK_REQUEST, ...)
 *    4. ConditionVariableTimedSleep(slot.reply_cv,
 *                                   cluster.gcs_reply_timeout_ms,
 *                                   WAIT_EVENT_GCS_BLOCK_SHIP_WAIT)
 *    5. On wake:
 *       GRANTED:
 *         - Verify checksum (HC83);  fail-closed on mismatch
 *         - LWLockAcquire(buf->content_lock, LW_EXCLUSIVE)
 *         - memcpy reply.block_data → BufferGetPage(buf)
 *         - PageSetLSN(BufferGetPage(buf), reply.page_lsn)  (HC84)
 *         - LWLockRelease(buf->content_lock)
 *         - Update buf->pcm_state + buf->buffer_type
 *         - Return success
 *       GRANTED_STORAGE_FALLBACK:
 *         - Do not memcpy;  requester keeps ReadBuffer() page from shared
 *           storage because master state was N when granting (HC88).
 *         - Update buf->pcm_state + buf->buffer_type
 *         - Return success
 *       DENIED_*: cleanup + ereport
 *       Timeout: cleanup + ereport ERRCODE_QUERY_CANCELED + errhint
 *                "spec-2.34 retransmit"
 *    6. Release slot
 */
/*
 * Returns true if a DURABLE PCM grant was acquired (GRANTED / STORAGE_
 * FALLBACK — the caller mirrors PCM ownership into buf->pcm_state).  Returns
 * false for a spec-5.2 D2 one-shot READ_IMAGE, or for an authoritative
 * DENIED_PENDING_X with *out_retry_denied set.  In the latter case the caller
 * must exact-abort its GRANT_PENDING reservation before waiting/re-entering.
 * Terminal denials ereport(ERROR) and do not return.
 */
extern bool cluster_gcs_send_block_request_and_wait(BufferDesc *buf,
													PcmLockTransition transition_id,
													int master_node, bool clean_eligible,
													bool *out_retry_denied);

/*
 * spec-5.2 D2 (sub-case B) — local-master read-image forward.  Used by
 * cluster_pcm_lock_acquire_buffer when THIS node is the GCS master for a
 * block a REMOTE node holds in X and a local reader needs an N→S image.
 * Forwards a read-image request to the holder and installs the shipped
 * current image for one read.  Returns false (non-durable; caller leaves
 * buf->pcm_state == N); fails closed (ereport) if no image is obtained while
 * expected remains exact.  Authority drift instead sets out_retry_denied so
 * bufmgr aborts/rearms GRANT_PENDING and selects a fresh holder identity.
 */
extern bool cluster_gcs_local_master_read_image_and_wait(BufferDesc *buf,
												 const PcmAuthoritySnapshot *expected,
												 bool force_one_shot,
												 bool *out_retry_denied);
/*
 * R10/A' PGRAC adaptation: a master-local legacy N->S acquisition bypasses
 * the data-plane dedup table, so consult the existing exact PCM-X active-head
 * locator or approved pre-ASSERT late-bind head before publishing local S.
 * This is a retry barrier only: it grants
 * no PCM/PI authority and never reads or reconstructs page bytes.
 */
extern bool cluster_gcs_block_resource_x_local_s_barrier_active(BufferTag tag);
/* PGRAC: spec-5.2 D11 — local-master writer-transfer (revoke); durable X grant.
 * spec-5.2a D2/D3: clean_eligible routes a clean (sequence) page through the
 * flush-data-before-drop holder path + stale-holder storage-fallback recovery.
 * P0-26: expected is the entry-lock authority token; authority drift returns
 * retry_denied so bufmgr aborts/rearms a fresh ownership/request identity. */
extern bool cluster_gcs_local_master_x_transfer_and_wait(BufferDesc *buf,
														 const PcmAuthoritySnapshot *expected,
														 bool clean_eligible,
														 bool *out_retry_denied);

/*
 * spec-4.7 D1 — GCS/PCM block resource recovery phase.
 *
 *	AD-002 资源级 {GRANTED, CONVERTING, RECOVERING} 的 RECOVERING 兑现.
 *	A block resource is RECOVERING when its GCS master is being recovered
 *	after a reconfiguration: the master node is DEAD, and block-protocol
 *	state (holders / mode / PI watermark) is volatile shmem with no
 *	transition log, so it must be REBUILT (spec-4.7 D2/D3), not recovered.
 *	cluster_gcs_lookup_master hashes over the STATIC declared node list
 *	(cluster_gcs.c), so a dead master still routes here;  spec-4.6 GRD/GES
 *	remaster rebuilds only the logical-lock layer, NOT block/PCM state.
 *
 *	The bufmgr acquire gate (cluster_pcm_lock_acquire_buffer) fail-closes
 *	53R9L (ERRCODE_CLUSTER_GCS_BLOCK_RESOURCE_RECOVERING) for a RECOVERING
 *	block after a bounded cluster.gcs_block_recovery_wait_ms wait — never a
 *	stale local / old-master fallback.  master == self (own master or
 *	single-node fallback) is NOT RECOVERING (it is the clean-restart
 *	lazy-rebuild path landed by spec-4.7 D3).
 */
typedef enum ClusterGcsBlockPhase {
	GCS_BLOCK_NORMAL = 0,
	GCS_BLOCK_RECOVERING = 1,
} ClusterGcsBlockPhase;

extern ClusterGcsBlockPhase cluster_gcs_block_phase_for_tag(BufferTag tag);

/*
 * spec-5.16 D3 — online-join PCM block snap-back fence predicates (impl in
 * cluster_grd.c;  declared here because BufferTag is in scope and both the
 * requester-side phase gate and the master-side envelope handler consume them).
 *
 *	cluster_grd_join_remaster_active_for_shard:  the block's STATIC PCM home
 *	    (cluster_gcs_lookup_master_static) is a rejoining RECIPIENT of the current
 *	    fence episode (join_pcm_fence_member_epoch[home] == join_pcm_fence_epoch;
 *	    bound to online_join, INDEPENDENT of any GRD master[] movement — so
 *	    join_remaster_enabled=off still fences, r2 P1-①).  false when the fence is
 *	    not armed (join_pcm_fence_epoch == 0) or the home is a steady member.
 *	cluster_grd_block_view_rebuilt:  the joiner-home view is rebuilt — i.e.
 *	    EVERY declared member's recovery_done_epoch >= join_pcm_fence_epoch
 *	    (Hardening v1.1:  the all-members all_done barrier, NOT the joiner's own
 *	    done-epoch, which advances before survivors finish re-declaring → 8.A).
 *	    true when the fence is not armed.
 */
extern bool cluster_grd_join_remaster_active_for_shard(BufferTag tag);
extern bool cluster_grd_block_view_rebuilt(BufferTag tag);

/*
 * spec-4.7 D5 — redo-before-unfreeze gate (Q5):  true iff the dead origin's
 * merged WAL recovery on this node reached >= required_lsn (the survivor's
 * observed max page_lsn).  Below that → lost-write risk → fail-closed 53R9M.
 */
extern bool cluster_gcs_block_redo_lsn_covered(int dead_origin, XLogRecPtr required_lsn);

/*
 * spec-4.7 D2 — survivor block re-declare wire (PGRAC_IC_MSG_GCS_BLOCK_REDECLARE).
 *	cluster_gcs_block_send_redeclare:  the P5 chunked scan sends one
 *		fire-and-forget announce per locally-held S/X buffer to the block's
 *		current (remastered) master.
 *	cluster_gcs_handle_block_redeclare_envelope:  master-side receive —
 *		validate checksum + episode epoch (L235/L236), then rebuild the
 *		minimal block-resource view via
 *		cluster_gcs_block_master_rebuild_from_redeclare (cluster_pcm_lock.c).
 */
extern void cluster_gcs_block_send_redeclare(BufferTag tag, uint8 held_mode, XLogRecPtr page_lsn,
											 SCN page_scn, uint64 cluster_epoch, int master_node);
extern void cluster_gcs_handle_block_redeclare_envelope(const struct ClusterICEnvelope *env,
														const void *payload);

/*
 * cluster_gcs_register_block_msg_types -- postmaster-once registration of
 * GCS_BLOCK_REQUEST + GCS_BLOCK_REPLY in cluster_ic dispatch table.  Called
 * from the same phase as cluster_gcs_register_msg_types (spec-2.32).
 *
 *  broadcast_ok = false (point-to-point only).
 */
extern void cluster_gcs_register_block_msg_types(void);

/*
 * Shmem registry for outstanding block-request table + LWLock.
 */
extern Size cluster_gcs_block_shmem_size(void);
extern void cluster_gcs_block_shmem_init(void);
extern void cluster_gcs_block_module_init(void);


/* ============================================================
 * Receiver handlers -- installed into cluster_ic dispatch table.
 * Exposed for cluster_unit tests to exercise dispatch directly.
 * ============================================================ */

/* Forward decl -- definition lives in cluster_ic_envelope.h */
struct ClusterICEnvelope;

extern void cluster_gcs_handle_block_request_envelope(const struct ClusterICEnvelope *env,
													  const void *payload);
extern void cluster_gcs_handle_block_reply_envelope(const struct ClusterICEnvelope *env,
													const void *payload);
/* PGRAC: spec-2.35 D7 — holder-side forward handler.  Receives
 * PGRAC_IC_MSG_GCS_BLOCK_FORWARD, copies the page bytes, direct-ships
 * the GCS_BLOCK_REPLY (status GRANTED_FROM_HOLDER) to the original
 * requester carried in fwd.original_requester_node.  HC103 + HC104 +
 * HC105 (evict race fallback). */
extern void cluster_gcs_handle_block_forward_envelope(const struct ClusterICEnvelope *env,
													  const void *payload);


/* ============================================================
 * GCS serve-stall round-5 — per-family send admission accounting.
 *
 *	One shared funnel for every block-family send site (replies incl.
 *	cached resends and cluster_cr_server's direct REPLY sends, FORWARD,
 *	INVALIDATE + acks + redeclare) under the four-state ownership
 *	contract (ClusterICSendResult in cluster_ic.h).  WOULD_BLOCK =
 *	admitted into the tier1 per-peer FIFO (queued counter);
 *	NOT_ADMITTED = refused, retransmit self-heals (red-flag counter).
 * ============================================================ */
#include "cluster/cluster_ic.h" /* ClusterICSendResult */

typedef enum GcsBlockSendFamily {
	GCS_BLOCK_SEND_FAMILY_REPLY = 0,
	GCS_BLOCK_SEND_FAMILY_FORWARD,
	GCS_BLOCK_SEND_FAMILY_INVALIDATE,
} GcsBlockSendFamily;

extern void cluster_gcs_block_note_send_outcome(GcsBlockSendFamily family, ClusterICSendResult rc);
extern ClusterICSendResult
cluster_gcs_block_send_direct_zero_reply(int32 dest_node, const GcsBlockReplyHeader *header);

extern uint64 cluster_gcs_get_reply_send_queued_count(void);
extern uint64 cluster_gcs_get_reply_send_not_admitted_count(void);
extern uint64 cluster_gcs_get_forward_send_queued_count(void);
extern uint64 cluster_gcs_get_forward_send_not_admitted_count(void);
extern uint64 cluster_gcs_get_invalidate_send_queued_count(void);
extern uint64 cluster_gcs_get_invalidate_send_not_admitted_count(void);

/* PGRAC: GCS serve-stall round-5 A2 — bounded-drop machinery.  The LMS
 * worker loop retries PINNED invalidate directives parked by the handler
 * (per-worker process-local lot;  see the invalidate handler notes). */
extern void cluster_gcs_block_invalidate_park_tick(void);
extern uint64 cluster_gcs_get_invalidate_parked_count(void);
extern uint64 cluster_gcs_get_invalidate_park_expired_count(void);
extern uint64 cluster_gcs_get_invalidate_busy_sent_count(void);
extern uint64 cluster_gcs_get_invalidate_busy_received_count(void);
extern uint64 cluster_gcs_get_invalidate_passive_s_release_count(void);
extern uint64 cluster_gcs_get_pcm_x_self_handoff_count(void);
extern uint64 cluster_gcs_get_pcm_x_self_handoff_drain_count(void);
extern uint64 cluster_gcs_get_invalidate_park_overflow_count(void);
extern uint64 cluster_gcs_get_drop_pinned_deny_count(void);
extern uint64 cluster_gcs_get_xfer_stale_deny_count(void);

/* ============================================================
 * Observability accessors (dump_gcs +8 NEW rows for block plane).
 *
 *  Each accessor returns a uint64 counter.  Returns 0 when module is
 *  not initialized (cluster_pcm_is_active false at startup).
 * ============================================================ */
extern uint64 cluster_gcs_get_block_request_count(void);
extern uint64 cluster_gcs_get_block_reply_count(void);
extern uint64 cluster_gcs_get_block_timeout_count(void);
extern uint64 cluster_gcs_get_block_checksum_fail_count(void);
extern uint64 cluster_gcs_get_block_storage_fallback_count(void);
extern uint64 cluster_gcs_get_block_master_not_holder_count(void);
extern uint64 cluster_gcs_get_block_wal_flush_before_ship_count(void);
extern uint64 cluster_gcs_get_block_ship_bytes_total(void);

/*
 * PGRAC: spec-7.2 D6 — requester-side block-ship latency histogram.
 *
 *	16 log-scale buckets in microseconds;  bucket b counts completions
 *	with elapsed <= bound(b), last bucket is the +inf overflow.  Samples
 *	are recorded at the single normal-exit funnel of
 *	cluster_gcs_send_block_request_and_wait (GRANTED / STORAGE_FALLBACK /
 *	READ_IMAGE);  terminal-ereport exits lose the sample.  This is the
 *	ruler for the spec-7.2 value gate (p99 < 20ms, p50 < 5ms) and the
 *	7.7/7.8 wait-closure legs.  dump category 'gcs', keys
 *	ship_hist_us_le_<bound> + ship_hist_us_inf.
 */
#define CLUSTER_GCS_SHIP_HIST_BUCKETS 16
extern uint64 cluster_gcs_block_ship_hist_bound_us(int bucket);
extern uint64 cluster_gcs_block_ship_hist_count(int bucket);

/* PGRAC: spec-7.2 D3/D4 — registry probe for the atomic plane flip. */
extern bool cluster_gcs_block_family_on_data_plane(void);

/* PGRAC: spec-6.13 D8 — RDMA tier3/direct-land copy observability. */
extern uint64 cluster_gcs_get_scratch_copy_count(void);
extern uint64 cluster_gcs_get_live_sge_send_count(void);
extern uint64 cluster_gcs_get_live_sge_fallback_count(void);
extern uint64 cluster_gcs_get_direct_install_count(void);
extern uint64 cluster_gcs_get_direct_install_abort_count(void);
extern uint64 cluster_gcs_get_install_copy_count(void);

/* ============================================================
 * spec-2.34 D1 — reliability hardening counter accessors (9 NEW).
 *
 *	dump_gcs rows 22→31:
 *	  retransmit_attempt_count       — # of retry attempts entered
 *	  retransmit_send_count          — # of resend envelopes emitted
 *	  retransmit_exhausted_count     — # of budget-exhausted 53R90 ereports
 *	  dedup_hit_count                — # of CACHED_REPLY hits on master
 *	  dedup_miss_count               — # of MISS_REGISTERED on master
 *	  dedup_collision_count          — # of HC91 tag/transition mismatch
 *	  dedup_full_count               — # of HC92 cap-full DENIED_DEDUP_FULL
 *	  epoch_invalidate_wake_count    — # of CV signals from eager wake hook
 *	  stale_reply_drop_count         — # of HC100 stale-reply drops
 *	  done_sent_count                — # of GCS_BLOCK_DONE proofs sent (RC-F)
 *	  dedup_done_marked_count        — # of DONE proofs stamped on master (RC-F)
 *	  dedup_done_mismatch_count      — # of DONE proofs dropped on master (RC-F)
 * ============================================================ */
extern uint64 cluster_gcs_get_block_retransmit_attempt_count(void);
extern uint64 cluster_gcs_get_block_retransmit_send_count(void);
extern uint64 cluster_gcs_get_block_retransmit_exhausted_count(void);
extern uint64 cluster_gcs_get_block_dedup_hit_count(void);
extern uint64 cluster_gcs_get_block_dedup_miss_count(void);
extern uint64 cluster_gcs_get_block_dedup_collision_count(void);
extern uint64 cluster_gcs_get_block_dedup_full_count(void);
extern uint64 cluster_gcs_get_block_dedup_entry_count(void); /* spec-7.2a D5 */
extern uint64 cluster_gcs_get_block_dedup_evict_count(void); /* spec-7.2a D5 */
extern uint64 cluster_gcs_get_block_dedup_max_entries(void); /* spec-7.2a D5 */
extern uint64 cluster_gcs_get_block_epoch_invalidate_wake_count(void);
extern uint64 cluster_gcs_get_block_stale_reply_drop_count(void);
extern uint64 cluster_gcs_get_block_done_sent_count(void);			  /* RC-F DONE */
extern uint64 cluster_gcs_get_block_done_enqueue_drop_count(void);	  /* review F7 */
extern uint64 cluster_gcs_get_block_dedup_done_marked_count(void);	  /* RC-F DONE */
extern uint64 cluster_gcs_get_block_dedup_done_mismatch_count(void);  /* RC-F DONE */
extern uint64 cluster_gcs_get_block_dedup_hint_violation_count(void); /* review F5 */
extern uint64 cluster_gcs_get_block_dedup_legacy_pin_count(void);	  /* review F5 */
extern uint64 cluster_gcs_get_fallback_scn_verify_pass_count(void); /* round-4c FUNC-1 */
extern uint64 cluster_gcs_get_fallback_scn_refresh_count(void);		/* round-4c FUNC-1 */
extern uint64 cluster_gcs_get_fallback_scn_failclosed_count(void);	/* round-4c FUNC-1 */

/*
 * PGRAC: spec-2.35 D12 — 7 NEW reliability/lifecycle counter accessors
 * for CF 2-way read sharing.  Mirrors ClusterGcsBlockShared fields.
 *
 *	block_forward_sent_count            — master sent GCS_BLOCK_FORWARD
 *	block_forward_received_count        — holder received FORWARD
 *	block_from_holder_ship_count        — holder shipped GRANTED_FROM_HOLDER
 *	block_forward_holder_evicted_count  — holder evict race DENIED reply
 *	s_holders_bitmap_redirect_count     — master chose forward over fallback
 *	master_holder_lifecycle_count       — HC110 update events
 *	forward_replay_count                — dedup FORWARDED re-forward
 */
extern uint64 cluster_gcs_get_block_forward_sent_count(void);
extern uint64 cluster_gcs_get_block_forward_received_count(void);
extern uint64 cluster_gcs_get_block_from_holder_ship_count(void);
extern uint64 cluster_gcs_get_block_forward_holder_evicted_count(void);
extern uint64 cluster_gcs_get_block_s_holders_bitmap_redirect_count(void);
extern uint64 cluster_gcs_get_block_master_holder_lifecycle_count(void);
extern uint64 cluster_gcs_get_block_forward_replay_count(void);

/* PGRAC: spec-2.36 D10 — 6 NEW counter accessors for CF 3-way protocol. */
extern uint64 cluster_gcs_get_block_invalidate_broadcast_count(void);
extern uint64 cluster_gcs_get_block_invalidate_ack_received_count(void);
extern uint64 cluster_gcs_get_block_invalidate_timeout_count(void);
extern uint64 cluster_gcs_get_block_x_forward_sent_count(void);
extern uint64 cluster_gcs_get_block_x_granted_from_holder_count(void);
extern uint64 cluster_gcs_get_starvation_denied_pending_x_count(void);

/* PGRAC: spec-6.14a D5 — 3 NEW counter accessors for the X-vs-S arms. */
extern uint64 cluster_gcs_get_local_s_upgrade_grant_count(void);
extern uint64 cluster_gcs_get_x_vs_s_nonholder_grant_count(void);
extern uint64 cluster_gcs_get_x_vs_s_no_carrier_denied_count(void);

/* PGRAC: spec-2.37 D12 — 4 NEW counter accessors for PI watermark + lost-write. */
extern uint64 cluster_gcs_get_pi_watermark_advance_count(void);
extern uint64 cluster_gcs_get_pi_watermark_retire_count(void);
extern uint64 cluster_gcs_get_pi_durable_note_apply_count(void);
extern uint64 cluster_gcs_get_lost_write_detected_count(void);
extern uint64 cluster_gcs_get_lost_write_avoid_count(void);
/* PGRAC: spec-2.41 D7 — SCN detector + redo-coverage observability accessors. */
extern uint64 cluster_gcs_get_lost_write_invalidscn_failclosed_count(void);
/* PGRAC: branch-1 master-direct storage-fallback rescue accessor. */
extern uint64 cluster_gcs_get_lost_write_master_direct_storage_fallback_count(void);
extern uint64 cluster_gcs_get_lost_write_not_scn_tracked_skip_count(void);
extern uint64 cluster_gcs_get_redo_coverage_required_lsn_zero_count(void);
extern uint64 cluster_gcs_get_redo_coverage_gate_block_count(void);

/* PGRAC: spec-5.2 D2 — X-holder read-image ship counter accessor. */
extern uint64 cluster_gcs_get_cf_xheld_read_ship_count(void);
/* PGRAC: spec-5.2a D6 — clean-page X-transfer enabler counters (5). */
extern uint64 cluster_gcs_get_clean_page_xfer_count(void);
extern uint64 cluster_gcs_get_clean_page_xfer_storage_fallback_count(void);
extern uint64 cluster_gcs_get_clean_page_xfer_fail_closed_count(void);
extern uint64 cluster_gcs_get_clean_page_xfer_stale_holder_recover_count(void);
extern uint64 cluster_gcs_get_clean_page_xfer_third_party_denied_count(void);
/* PGRAC: spec-5.2 D11 — writer-transfer-revoke ship counters (A: path-A
 * forward-to-holder revoke; B: master==holder self-ship). */
extern uint64 cluster_gcs_get_block_x_transfer_ship_count(void);
extern uint64 cluster_gcs_get_block_x_self_ship_count(void);

/* ============================================================
 * PGRAC: spec-6.12h D-h2 — PI-holder discard protocol (Q25-A dual trigger).
 *
 *	The "current copy written durable" proof pipeline: FlushBuffer records
 *	every tracked-block write into a small shmem ring (pi_write_note = the
 *	"写盘成功" face); the checkpointer brackets ProcessSyncRequests with
 *	presync_snapshot/confirm (the "checkpoint 推进" face — everything noted
 *	before the sync phase is durable once it returns, exactly Oracle's
 *	"a PI may be discarded only after a newer version is persisted"); the
 *	LMON tick drains confirmed notes and routes each to the block's master
 *	(locally, or as an unsolicited INVALIDATE_ACK status-3 durable-note ride:
 *	page_scn_bytes@52 carries the written pd_block_scn — the only cross-node
 *	comparable version unit under per-thread WAL, so the page LSN is
 *	deliberately not part of the protocol).  The master retires the
 *	watermarks + PI bitmap
 *	(cluster_pcm_lock_pi_discard_collect) and sends each PI holder a
 *	PI_DISCARD (INVALIDATE ride, reserved_0[0] = 1, no ACK).  Every hop is
 *	fire-and-forget fail-safe: a lost note/notify only leaves a PI lingering
 *	until buffer pressure or the implicit-discard reread.
 * ============================================================ */
extern void cluster_gcs_block_pi_write_note(BufferTag tag, SCN page_scn);
extern uint64 cluster_gcs_block_pi_note_presync_snapshot(void);
extern void cluster_gcs_block_pi_note_confirm(uint64 presync_seq);
extern void cluster_gcs_block_pi_discard_drain(void);
/* spec-5.22b D2-4 — single-target PI_DISCARD send, reused by the shared-undo
 * owner-as-master data plane (LMON-context; caller owns self/range guard). */
extern void cluster_gcs_block_send_pi_discard_invalidate(BufferTag tag, int32 target_node);

/* PGRAC: spec-4.7 D6 — 8 warm-recovery observability accessors. */
extern uint64 cluster_gcs_get_recovery_block_resources_recovering(void);
extern uint64 cluster_gcs_get_recovery_buffers_redeclared(void);
extern uint64 cluster_gcs_get_recovery_block_state_rebuilt(void);
extern uint64 cluster_gcs_get_recovery_redo_boundary_waits(void);
extern uint64 cluster_gcs_get_recovery_redo_boundary_reached(void);
extern uint64 cluster_gcs_get_recovery_stale_block_drop(void);
extern uint64 cluster_gcs_get_recovery_ambiguous_owner_failclosed(void);
extern uint64 cluster_gcs_get_recovery_before_boundary_failclosed(void);

/*
 * PGRAC: spec-2.35 D3 (HC110) — counter bump invoked from cluster_pcm_
 *	transition_apply each time master_holder is mutated.  Keeping the
 *	bump logic in cluster_gcs_block.c avoids exposing the atomic field
 *	of ClusterGcsBlockShared to other translation units.
 */
extern void cluster_gcs_block_bump_master_holder_lifecycle(void);

/*
 * spec-6.13 D6 direct-land hooks.
 *
 * LMON calls prepare_outbound_request after dequeuing a backend-produced
 * GCS_BLOCK_REQUEST but before sending it on the wire.  The hook posts the
 * two-SGE block-reply receive when the slot is in ARMING state and sets the
 * request direct-land flag only after post_recv succeeds.
 *
 * The RDMA block-reply lane calls handle_direct_land_completion for receive
 * CQEs.  `sidecar` points at exactly
 * CLUSTER_IC_RDMA_DIRECT_LAND_SIDECAR_BYTES bytes containing
 * ClusterICEnvelope + GcsBlockReplyHeader; the landed page is already in the
 * slot target.
 */
extern void cluster_gcs_block_lmon_prepare_outbound_request(GcsBlockRequestPayload *req,
															int32 dest_node);

/*
 * spec-7.3 D4 — DATA worker for a staged block-family frame (hash of its
 * BufferTag).  Only REQUEST / FORWARD / INVALIDATE carry a routable tag;
 * returns [0, n_workers) or -1 (8.A fail-closed: refuse to stage, never
 * default a worker).  See cluster_gcs_block.c for the direct-send rationale.
 */
extern int cluster_gcs_block_payload_shard(uint8 msg_type, const void *payload, uint16 payload_len,
										   int n_workers);
extern void cluster_gcs_block_lmon_handle_direct_land_completion(int32 peer_node, uint64 wr_id,
																 bool cqe_success, uint32 byte_len,
																 const void *sidecar);
extern void cluster_gcs_block_lmon_abort_direct_land_peer(int32 peer_node,
														  ClusterGcsBlockDirectAbortReason reason);
extern int cluster_gcs_block_lmon_drain_direct_land_aborts(void);
typedef struct ResourceXWriterUseContext {
	ResourceXAcquisitionRef ref;
	uint64 r4_record_generation;
	uint64 buffer_ownership_generation;
	uint64 writer_activation_token;
	uint64 resource_x_activation_generation;
} ResourceXWriterUseContext;

StaticAssertDecl(sizeof(ResourceXWriterUseContext) == 72,
	"ResourceXWriterUseContext layout must remain 72 bytes");

/* Process-local TARGET cached-X eviction plan.  It is never serialized or
 * stored in shared memory: PREPARE freezes the exact existing kind-4 bytes
 * while BufferDesc is still X+REVOKING, LOCAL COMMIT is recorded by the
 * caller, and PUBLISH transfers those same bytes before closing the exact
 * EVICTING owner and terminal cover. */
typedef struct ResourceXTargetEvictionPlan {
	ResourceXDecodedFrame release;
	ResourceXLocalOwnerHandle owner;
	ResourceXGateSnapshot gate;
	BufferTag tag;
	uint64 cached_ownership_generation;
	uint64 r4_record_generation;
	uint32 sender_connection_generation;
	int32 master_node;
	uint16 payload_bytes;
	bool prepared;
	bool local_n_committed;
	bool release_admitted;
	uint8 reserved[3];
	uint8 release_payload[RESOURCE_X_CONTROL_V1_BYTES];
} ResourceXTargetEvictionPlan;

/* TARGET-only requester: join/create the per-resource bootstrap round and
 * return only its exact post-T3 retained acquisition ref. */
/* Diagnostics never infer expiry from BAD_STATE or from a later clock read.
 * Only the exact failing branch may supply either expiry fact. */
static inline const char *
cluster_gcs_resource_x_acquire_failure_reason(bool head_expired, bool caller_expired,
											  ResourceXApplyResult result)
{
	if (head_expired)
		return "HEAD_NO_PROGRESS_EXPIRED";
	if (caller_expired)
		return "FOLLOWER_DEADLINE_EXPIRED";
	if (result == RESOURCE_X_APPLY_STALE)
		return "ACQUIRE_IDENTITY_STALE";
	return "ACQUIRE_FAILURE_CAUSE_UNPROVEN";
}

extern const char *cluster_gcs_resource_x_take_acquire_failure_reason(int buffer_id,
																	  ResourceXApplyResult result,
																	  uint64 *attempt_out);

extern ResourceXApplyResult cluster_gcs_resource_x_target_acquire_exact(
	BufferDesc *buf, uint64 r4_record_generation,
	ResourceXAcquisitionRef *ref_out);
/* Stack-only retry variant for the bufmgr pre-use handoff.  A zero input
 * freezes the ordinary R7 absolute deadline; subsequent calls must present
 * the same nonzero value and can never refresh it. */
extern ResourceXApplyResult
cluster_gcs_resource_x_target_acquire_until_exact(
	BufferDesc *buf, const BufferTag *expected_resource,
	uint64 r4_record_generation,
	uint64 *absolute_deadline_us_io, ResourceXAcquisitionRef *ref_out);
extern ResourceXApplyResult
cluster_gcs_resource_x_target_evict_prepare_exact(
	const BufferTag *tag, const ClusterPcmOwnSnapshot *exact_x,
	uint64 r4_record_generation, uint64 reservation_token,
	ResourceXTargetEvictionPlan *plan_out);
extern ResourceXApplyResult
cluster_gcs_resource_x_target_evict_publish_exact(
	ResourceXTargetEvictionPlan *plan);
extern ResourceXApplyResult
cluster_gcs_resource_x_target_evict_abort_exact(
	ResourceXTargetEvictionPlan *plan);
extern ResourceXApplyResult
cluster_gcs_resource_x_target_direct_init_acquire_exact(
	BufferDesc *buf, const BufferTag *expected_resource,
	uint64 r4_record_generation,
	uint64 direct_init_ownership_generation,
	uint64 direct_init_reservation_token,
	ResourceXAcquisitionRef *ref_out);
extern ResourceXApplyResult
cluster_gcs_resource_x_target_direct_init_join_exact(
	BufferDesc *buf, const BufferTag *expected_resource,
	uint64 direct_init_ownership_generation,
	uint64 direct_init_reservation_token,
	ResourceXAcquisitionRef *ref_out);
extern bool cluster_gcs_resource_x_target_context_recheck_exact(
	const ResourceXWriterUseContext *context);
extern ResourceXApplyResult
cluster_gcs_resource_x_target_itl_recycle_begin_exact(
	const ResourceXWriterUseContext *context,
	const ClusterPcmOwnSnapshot *observed,
	ResourceXLocalOwnerHandle *handle_out);
extern ResourceXApplyResult
cluster_gcs_resource_x_target_itl_recycle_finish_exact(
	const ResourceXWriterUseContext *context,
	const ClusterPcmOwnSnapshot *observed,
	const ResourceXLocalOwnerHandle *handle);
extern ResourceXApplyResult
cluster_gcs_resource_x_target_itl_recycle_cancel_exact(
	const ResourceXLocalOwnerHandle *handle);
/* ============================================================
 * spec-2.34 D4 — eager wake on epoch advance.
 *
 *	Called by spec-2.29 reconfig coordinator inside
 *	cluster_reconfig_apply_epoch_bump_as_coordinator() AFTER
 *	cluster_epoch_advance_for_reconfig() + cluster_epoch_set_changed_at_lsn()
 *	and BEFORE cluster_reconfig_publish_event() (HC95 ordering).
 *
 *	Action: sweep all per-backend block-outstanding slots; mark slots whose
 *	request_epoch < new_epoch as stale + ConditionVariableBroadcast their
 *	reply_cv so the sender wakes immediately rather than waiting for the
 *	reply timeout safety net.
 * ============================================================ */
extern void cluster_gcs_block_on_epoch_advance(uint64 new_epoch);
extern void cluster_gcs_block_on_epoch_advance_exact(
	uint64 new_epoch, const uint8 *dead_bitmap);
extern bool cluster_gcs_block_resource_x_cutover_tick(void);
extern bool cluster_gcs_ctrc_dispatch_close(
	const ClusterCtrcCloseDispatch *dispatch);


/* ============================================================
 * spec-5.13 D5 — clean-leave GCS data-plane drain.
 *
 *	flush_all_dirty: leaving node, thin orchestration over the bufmgr D5b
 *	seam (runs in the leaving node's backend/checkpointer, CL-I9).
 *	invalidate_for: survivor, POST-epoch cache invalidate of the leaving
 *	node's blocks (reuses on_epoch_advance; CL-I5 happens-before boundary).
 * ============================================================ */
extern uint32 cluster_gcs_block_clean_leave_flush_all_dirty(void);
extern void cluster_gcs_block_clean_leave_invalidate_for(int32 leaving_node, uint64 new_epoch);


/* ============================================================
 * Test-only injection (cluster_unit / TAP harness builds only).
 * ============================================================ */
#ifdef USE_CLUSTER_UNIT

/*
 * Spy hooks for HC82 / HC83 / HC84 / HC89 unit tests.  When non-NULL the
 * helper invokes the hook at the documented point in its flow (after
 * page_lsn read but before XLogFlush, after checksum verify, etc).  The
 * hook may set static state for retry / fail-closed scenarios.
 *
 *  cluster_gcs_block_test_xlog_flush_hook   -- HC82 invocation order spy
 *  cluster_gcs_block_test_lsn_drift_hook    -- HC89 single-retry simulation
 *                                              (returns count of drift events
 *                                              to inject before stabilizing)
 */
extern void (*cluster_gcs_block_test_xlog_flush_hook)(uint64 page_lsn);
extern int (*cluster_gcs_block_test_lsn_drift_hook)(void);

#endif /* USE_CLUSTER_UNIT */


/* ============================================================
 * Internal constants.
 * ============================================================ */

/* Reply envelope payload total size = header + block_data. */
#define GCS_BLOCK_REPLY_PAYLOAD_TOTAL_SIZE (sizeof(GcsBlockReplyHeader) + GCS_BLOCK_DATA_SIZE)


#endif /* USE_PGRAC_CLUSTER */

#endif /* CLUSTER_GCS_BLOCK_H */
