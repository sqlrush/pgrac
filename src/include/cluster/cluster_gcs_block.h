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
#include "cluster/cluster_lmd_wait_state.h"
#include "cluster/cluster_pcm_lock.h" /* PcmLockTransition */
#include "cluster/cluster_pcm_x_bufmgr.h"
#include "cluster/cluster_pcm_x_convert.h"
#include "cluster/cluster_sf_dep.h" /* ClusterSfDepVec / max origins */
#include "storage/block.h"			/* BLCKSZ */
#include "storage/buf_internals.h"	/* BufferTag, BufferDesc */

#ifdef USE_PGRAC_CLUSTER

/* A RETIRE wake is only a latency hint, but it still must never target a
 * recycled PGPROC wait generation.  Keep the tuple classifier pure so the
 * seqlock reader and GCS adapter share one executable definition of exact. */
static inline bool
cluster_gcs_pcm_x_wait_identity_matches(const PcmXWaitIdentity *identity, int32 local_node,
										uint32 all_proc_count,
										ClusterLmdWaitStateReadResult read_result,
										const ClusterLmdWaitStateSnapshot *snapshot)
{
	return identity != NULL && snapshot != NULL && local_node >= 0
		   && local_node < PCM_X_PROTOCOL_NODE_LIMIT && identity->node_id == local_node
		   && identity->procno < all_proc_count && identity->request_id != 0
		   && identity->wait_seq != 0 && read_result == CLUSTER_LMD_WAIT_STATE_READ_ACTIVE
		   && snapshot->active && snapshot->kind == CLUSTER_LMD_WAIT_PCM_CONVERT
		   && snapshot->request_id == identity->request_id
		   && snapshot->cluster_epoch == identity->cluster_epoch
		   && snapshot->wait_seq == identity->wait_seq && snapshot->xid == identity->xid;
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

typedef enum GcsBlockPcmXRequesterSite {
	GCS_BLOCK_PCM_X_RETRY_SITE_JOIN = 0,
	GCS_BLOCK_PCM_X_RETRY_SITE_LEADER_REKEY,
	GCS_BLOCK_PCM_X_RETRY_SITE_ROLE_REFRESH,
	GCS_BLOCK_PCM_X_RETRY_SITE_CLAIM,
	GCS_BLOCK_PCM_X_RETRY_SITE_CUTOFF,
	GCS_BLOCK_PCM_X_RETRY_SITE_FOLLOWER_SNAPSHOT,
	GCS_BLOCK_PCM_X_RETRY_SITE_WFG_COMMIT,
	GCS_BLOCK_PCM_X_RETRY_SITE_WFG_CLEAR,
	GCS_BLOCK_PCM_X_RETRY_SITE_PROGRESS,
	GCS_BLOCK_PCM_X_RETRY_SITE_PRECOMMIT_ARM,
	GCS_BLOCK_PCM_X_RETRY_SITE_IMAGE_FETCH,
	GCS_BLOCK_PCM_X_RETRY_SITE_POSTCOMMIT_ARM,
	GCS_BLOCK_PCM_X_RETRY_SITE_POSTCOMMIT_REPLAY_ARM,
	GCS_BLOCK_PCM_X_RETRY_SITE_RESERVATION_PREFLIGHT
} GcsBlockPcmXRequesterSite;

typedef enum GcsBlockPcmXRetryAction {
	GCS_BLOCK_PCM_X_RETRY_FAIL_CLOSED = 0,
	GCS_BLOCK_PCM_X_RETRY_WAIT,
	GCS_BLOCK_PCM_X_RETRY_RELOAD_PROGRESS,
	GCS_BLOCK_PCM_X_RETRY_RESNAPSHOT_WFG,
	GCS_BLOCK_PCM_X_RETRY_REFRESH_ROLE
} GcsBlockPcmXRetryAction;

/*
 * Drive-tick anomaly damping.  A master ticket can legally be observed
 * CLAIMED with its GRD pending-X cookie absent while another actor sits
 * between the two halves of a claimed cancel (the cookie is cleared before
 * the ticket finalizes) or of an identity-keyed serve-path clear.  One such
 * observation is therefore not corruption evidence;  the same per-ticket
 * observation persisting across consecutive periodic drive ticks is.  The
 * table lives in process-local memory:  LMON's retry tick is the guaranteed
 * periodic observer, so a real invariant violation still reaches the
 * fail-closed verdict after GCS_BLOCK_PCM_X_DRIVE_ANOMALY_FUSE observations
 * of its tag (the retry tick sweeps one pending tag per pass, so wall-clock
 * latency scales with the number of concurrently pending tags).
 */
typedef struct GcsBlockPcmXDriveAnomaly {
	BufferTag tag;
	uint64 ticket_id;
	uint32 streak;
	bool in_use;
} GcsBlockPcmXDriveAnomaly;

#define GCS_BLOCK_PCM_X_DRIVE_ANOMALY_SLOTS 64
#define GCS_BLOCK_PCM_X_DRIVE_ANOMALY_FUSE 8

/* Returns true once the anomaly has persisted long enough to fail closed. */
static inline bool
cluster_gcs_pcm_x_drive_anomaly_note(GcsBlockPcmXDriveAnomaly *table, int nslots,
									 const BufferTag *tag, uint64 ticket_id)
{
	int victim_slot = -1;
	bool victim_in_use = true;
	uint32 victim_streak = 0;
	int i;

	if (table == NULL || nslots <= 0 || tag == NULL)
		return true; /* no tracking substrate -- keep fail-closed */
	for (i = 0; i < nslots; i++) {
		if (table[i].in_use && table[i].ticket_id == ticket_id
			&& BufferTagsEqual(&table[i].tag, tag)) {
			table[i].streak++;
			return table[i].streak >= GCS_BLOCK_PCM_X_DRIVE_ANOMALY_FUSE;
		}
		/* Prefer a free slot;  under pressure recycle the lowest streak.
		 * Resolved tickets leave stale entries behind (settle fires only on
		 * definite progress for the tag), while a truly persisting anomaly
		 * keeps growing its streak and therefore survives the recycling. */
		if (!table[i].in_use) {
			if (victim_in_use) {
				victim_slot = i;
				victim_in_use = false;
			}
		} else if (victim_in_use && (victim_slot < 0 || table[i].streak < victim_streak)) {
			victim_slot = i;
			victim_streak = table[i].streak;
		}
	}
	table[victim_slot].tag = *tag;
	table[victim_slot].ticket_id = ticket_id;
	table[victim_slot].streak = 1;
	table[victim_slot].in_use = true;
	return false;
}

/* Any non-anomalous drive completion for the tag settles its streaks. */
static inline void
cluster_gcs_pcm_x_drive_anomaly_settle(GcsBlockPcmXDriveAnomaly *table, int nslots,
									   const BufferTag *tag)
{
	int i;

	if (table == NULL || nslots <= 0 || tag == NULL)
		return;
	for (i = 0; i < nslots; i++) {
		if (table[i].in_use && BufferTagsEqual(&table[i].tag, tag)) {
			table[i].in_use = false;
			table[i].streak = 0;
		}
	}
}

typedef enum GcsBlockPcmXFormationAction {
	GCS_BLOCK_PCM_X_FORMATION_FAIL_CLOSED = 0,
	GCS_BLOCK_PCM_X_FORMATION_WAIT,
	GCS_BLOCK_PCM_X_FORMATION_PROCEED
} GcsBlockPcmXFormationAction;

/* A pristine startup has never published a queue authority token, so a
 * writer may wait interruptibly for LMON formation.  Once any generation or
 * session has been published, a non-ACTIVE state is fail-stop evidence and
 * must never be mistaken for startup or routed through the legacy protocol. */
static inline GcsBlockPcmXFormationAction
cluster_gcs_pcm_x_requester_formation_action(const PcmXRuntimeSnapshot *runtime)
{
	if (runtime == NULL)
		return GCS_BLOCK_PCM_X_FORMATION_FAIL_CLOSED;
	if (runtime->state == PCM_X_RUNTIME_ACTIVE && runtime->gate_generation != 0
		&& runtime->master_session_incarnation != 0)
		return GCS_BLOCK_PCM_X_FORMATION_PROCEED;
	if (runtime->state == PCM_X_RUNTIME_RECOVERY_BLOCKED && runtime->gate_generation == 0
		&& runtime->master_session_incarnation == 0)
		return GCS_BLOCK_PCM_X_FORMATION_WAIT;
	return GCS_BLOCK_PCM_X_FORMATION_FAIL_CLOSED;
}

/* Every retry in one admitted request remains subordinate to the exact
 * formation token captured before publishing PCM_CONVERT. */
static inline bool
cluster_gcs_pcm_x_requester_runtime_exact(const PcmXRuntimeSnapshot *start,
										  const PcmXRuntimeSnapshot *current)
{
	return start != NULL && current != NULL && start->state == PCM_X_RUNTIME_ACTIVE
		   && current->state == PCM_X_RUNTIME_ACTIVE && start->gate_generation != 0
		   && start->gate_generation == current->gate_generation
		   && start->master_session_incarnation != 0
		   && start->master_session_incarnation == current->master_session_incarnation;
}

/* A generation/locator handoff while validating already-held content locks is
 * a normal transient.  It is safe to retry only while the requester's
 * PCM_CONVERT wait identity remains published.  BARRIER_CLOSED is deliberately
 * excluded: a barrier frozen before this wait cannot acquire the nested edge
 * retroactively, so the requester must unwind without sleeping. */
static inline bool
cluster_gcs_pcm_x_nested_guard_retryable(PcmXQueueResult result)
{
	return result == PCM_X_QUEUE_BUSY || result == PCM_X_QUEUE_GATE_RETRY
		   || result == PCM_X_QUEUE_STALE;
}

/* A local promotion changes only the role byte.  Every locator and immutable
 * waiter field must still name the exact same membership; accepting a fresh
 * handle with any other change would turn an alias/ABA into writer authority. */
static inline bool
cluster_gcs_pcm_x_role_refresh_exact(const PcmXLocalHandle *follower,
									 const PcmXLocalHandle *promoted)
{
	return follower != NULL && promoted != NULL && follower->flags == 0 && promoted->flags == 0
		   && follower->role == PCM_X_LOCAL_ROLE_FOLLOWER
		   && promoted->role == PCM_X_LOCAL_ROLE_NODE_LEADER
		   && BufferTagsEqual(&follower->identity.tag, &promoted->identity.tag)
		   && follower->identity.node_id == promoted->identity.node_id
		   && follower->identity.procno == promoted->identity.procno
		   && follower->identity.xid == promoted->identity.xid
		   && follower->identity.cluster_epoch == promoted->identity.cluster_epoch
		   && follower->identity.request_id == promoted->identity.request_id
		   && follower->identity.wait_seq == promoted->identity.wait_seq
		   && follower->identity.base_own_generation == promoted->identity.base_own_generation
		   && follower->tag_slot.slot_index == promoted->tag_slot.slot_index
		   && follower->tag_slot.slot_generation == promoted->tag_slot.slot_generation
		   && follower->membership_slot.slot_index == promoted->membership_slot.slot_index
		   && follower->membership_slot.slot_generation == promoted->membership_slot.slot_generation
		   && follower->local_sequence == promoted->local_sequence
		   && follower->local_round == promoted->local_round;
}

/* A queue result is meaningful only at the operation that produced it.
 * Immutable-token failures (especially STALE) must never enter a generic
 * retry loop.  An arm may race a newer progress publication; a WFG commit
 * may race its exact blocker; both have an explicit recovery action. */
static inline GcsBlockPcmXRetryAction
cluster_gcs_pcm_x_requester_retry_action(GcsBlockPcmXRequesterSite site, PcmXQueueResult result)
{
	switch (site) {
	case GCS_BLOCK_PCM_X_RETRY_SITE_JOIN:
		if (result == PCM_X_QUEUE_NOT_READY || result == PCM_X_QUEUE_BUSY
			|| result == PCM_X_QUEUE_GATE_RETRY || result == PCM_X_QUEUE_NO_CAPACITY)
			return GCS_BLOCK_PCM_X_RETRY_WAIT;
		break;
	case GCS_BLOCK_PCM_X_RETRY_SITE_LEADER_REKEY:
	case GCS_BLOCK_PCM_X_RETRY_SITE_ROLE_REFRESH:
	case GCS_BLOCK_PCM_X_RETRY_SITE_CLAIM:
		if (result == PCM_X_QUEUE_NOT_READY || result == PCM_X_QUEUE_BUSY
			|| result == PCM_X_QUEUE_GATE_RETRY || result == PCM_X_QUEUE_BARRIER_CLOSED)
			return GCS_BLOCK_PCM_X_RETRY_WAIT;
		break;
	case GCS_BLOCK_PCM_X_RETRY_SITE_FOLLOWER_SNAPSHOT:
		/* STALE proves the handle no longer byte-matches its membership slot.
		 * That is the same promotion / round-advance churn the claim site
		 * already recovers from with a refresh lookup, so route it to the
		 * exact re-dispatch instead of the kill path. */
		if (result == PCM_X_QUEUE_STALE)
			return GCS_BLOCK_PCM_X_RETRY_REFRESH_ROLE;
		if (result == PCM_X_QUEUE_NOT_READY || result == PCM_X_QUEUE_BUSY
			|| result == PCM_X_QUEUE_GATE_RETRY || result == PCM_X_QUEUE_BARRIER_CLOSED)
			return GCS_BLOCK_PCM_X_RETRY_WAIT;
		break;
	case GCS_BLOCK_PCM_X_RETRY_SITE_CUTOFF:
		if (result == PCM_X_QUEUE_NOT_READY || result == PCM_X_QUEUE_BUSY
			|| result == PCM_X_QUEUE_GATE_RETRY)
			return GCS_BLOCK_PCM_X_RETRY_WAIT;
		break;
	case GCS_BLOCK_PCM_X_RETRY_SITE_WFG_COMMIT:
		if (result == PCM_X_QUEUE_STALE || result == PCM_X_QUEUE_NOT_READY
			|| result == PCM_X_QUEUE_BUSY || result == PCM_X_QUEUE_GATE_RETRY
			|| result == PCM_X_QUEUE_BARRIER_CLOSED)
			return GCS_BLOCK_PCM_X_RETRY_RESNAPSHOT_WFG;
		break;
	case GCS_BLOCK_PCM_X_RETRY_SITE_PROGRESS:
		/* local_progress reports BUSY while a promoted leader's admission
		 * rekey owns the cross-lock gate.  The resident identity is deliberately
		 * unreadable in that window; wait and reload instead of treating the
		 * fail-closed snapshot discipline itself as corruption. */
		if (result == PCM_X_QUEUE_NOT_READY || result == PCM_X_QUEUE_BUSY)
			return GCS_BLOCK_PCM_X_RETRY_WAIT;
		break;
	case GCS_BLOCK_PCM_X_RETRY_SITE_PRECOMMIT_ARM:
		if (result == PCM_X_QUEUE_BAD_STATE)
			return GCS_BLOCK_PCM_X_RETRY_RELOAD_PROGRESS;
		if (result == PCM_X_QUEUE_NOT_READY || result == PCM_X_QUEUE_BUSY
			|| result == PCM_X_QUEUE_GATE_RETRY || result == PCM_X_QUEUE_BARRIER_CLOSED)
			return GCS_BLOCK_PCM_X_RETRY_WAIT;
		break;
	case GCS_BLOCK_PCM_X_RETRY_SITE_IMAGE_FETCH:
		if (result == PCM_X_QUEUE_NOT_READY || result == PCM_X_QUEUE_BUSY
			|| result == PCM_X_QUEUE_GATE_RETRY || result == PCM_X_QUEUE_NO_CAPACITY)
			return GCS_BLOCK_PCM_X_RETRY_WAIT;
		break;
	case GCS_BLOCK_PCM_X_RETRY_SITE_POSTCOMMIT_REPLAY_ARM:
		if (result == PCM_X_QUEUE_BAD_STATE)
			return GCS_BLOCK_PCM_X_RETRY_RELOAD_PROGRESS;
		break;
	case GCS_BLOCK_PCM_X_RETRY_SITE_RESERVATION_PREFLIGHT:
		/* BUSY is a transient own-slot lifecycle flag (a revoke/grant on this
		 * node mid-flight); wait and re-snapshot.  STALE means an interleaved
		 * revoke consumed the queued identity's base_own_generation, which the
		 * grant/final-ack chain cannot absorb without a master-visible rebase;
		 * keep the fail-closed verdict until that amendment lands. */
		if (result == PCM_X_QUEUE_BUSY)
			return GCS_BLOCK_PCM_X_RETRY_WAIT;
		break;
	case GCS_BLOCK_PCM_X_RETRY_SITE_WFG_CLEAR:
	case GCS_BLOCK_PCM_X_RETRY_SITE_POSTCOMMIT_ARM:
		break;
	}
	return GCS_BLOCK_PCM_X_RETRY_FAIL_CLOSED;
}

static inline uint32
cluster_gcs_pcm_x_requester_wait_index_advance(uint32 wait_index)
{
	const uint32 maximum = (uint32)CLUSTER_PCM_X_HOLDER_RETRY_BATCH_WAITS - 1;

	return wait_index < maximum ? wait_index + 1 : maximum;
}

typedef enum GcsBlockPcmXCleanupAction {
	GCS_BLOCK_PCM_X_CLEANUP_NONE = 0,
	GCS_BLOCK_PCM_X_CLEANUP_CANCEL_LOCAL,
	GCS_BLOCK_PCM_X_CLEANUP_PRESERVE_FAIL_CLOSED
} GcsBlockPcmXCleanupAction;

/* Before cohort freeze/PREPARE, an exact local membership (and its active
 * writer claim) can be unwound without inventing remote state.  Once an
 * irreversible local or ownership boundary exists, only recovery resolves it. */
static inline GcsBlockPcmXCleanupAction
cluster_gcs_pcm_x_requester_cleanup_action(bool handle_live, bool claim_live,
										   bool irreversible_started, bool ownership_committed)
{
	if (irreversible_started || ownership_committed || (claim_live && !handle_live))
		return GCS_BLOCK_PCM_X_CLEANUP_PRESERVE_FAIL_CLOSED;
	if (handle_live)
		return GCS_BLOCK_PCM_X_CLEANUP_CANCEL_LOCAL;
	return GCS_BLOCK_PCM_X_CLEANUP_NONE;
}

/* PREPARE may publish GRANT_PENDING only from the exact clean N tuple named
 * by the active queue identity.  A nonzero idle reservation token is valid;
 * only live flags denote an overlapping ownership lifecycle. */
static inline PcmXQueueResult
cluster_gcs_pcm_x_remote_reservation_preflight(const ClusterPcmOwnSnapshot *live,
											   const PcmXWaitIdentity *identity)
{
	ClusterPcmOwnResult live_result;

	if (live == NULL || identity == NULL)
		return PCM_X_QUEUE_INVALID;
	if (!BufferTagsEqual(&live->tag, &identity->tag)
		|| live->generation != identity->base_own_generation)
		return PCM_X_QUEUE_STALE;
	if (live->generation == UINT64_MAX)
		return PCM_X_QUEUE_COUNTER_EXHAUSTED;
	live_result = cluster_pcm_own_classify_live_flags(live->flags, live->reservation_token);
	if (live_result == CLUSTER_PCM_OWN_CORRUPT)
		return PCM_X_QUEUE_CORRUPT;
	if (live_result == CLUSTER_PCM_OWN_BUSY)
		return PCM_X_QUEUE_BUSY;
	if (live->pcm_state != (uint8)PCM_STATE_N)
		return PCM_X_QUEUE_STALE;
	return PCM_X_QUEUE_OK;
}

/* A requester-domain id carries the authenticated backend ordinal used by
 * the existing per-backend block-reply table. */
static inline bool
cluster_gcs_requester_id_decode(uint64 request_id, int32 *node_out, int32 *backend_id_out,
								uint64 *sequence_out)
{
	uint64 sequence;
	int32 node;
	int32 backend_id;

	if ((request_id & GCS_REQID_LOCAL_DOMAIN_FLAG) != 0)
		return false;
	sequence = request_id & GCS_REQID_REQUESTER_SEQ_MASK;
	if (sequence == 0)
		return false;
	node = (int32)((request_id >> GCS_REQID_NODE_SHIFT) & GCS_REQID_NODE_MASK);
	backend_id = (int32)(((request_id >> GCS_REQID_BACKEND_SHIFT) & GCS_REQID_BACKEND_MASK) + 1);
	if (node < 0 || node >= PCM_X_PROTOCOL_NODE_LIMIT || backend_id <= 0)
		return false;
	if (node_out != NULL)
		*node_out = node;
	if (backend_id_out != NULL)
		*backend_id_out = backend_id;
	if (sequence_out != NULL)
		*sequence_out = sequence;
	return true;
}

typedef struct GcsBlockPcmXImageIdentity {
	PcmXTicketRef ref;
	PcmXImageToken image;
} GcsBlockPcmXImageIdentity;

typedef struct GcsBlockPcmXImageBinding {
	GcsBlockPcmXImageIdentity identity;
	uint64 master_session;
	uint64 required_page_scn;
} GcsBlockPcmXImageBinding;

StaticAssertDecl(sizeof(GcsBlockPcmXImageIdentity) == 128,
				 "PCM-X image identity must fit the existing dedup 128-byte metadata cell");
StaticAssertDecl(sizeof(GcsBlockPcmXImageBinding) == 144,
				 "PCM-X image binding includes session and required source floor");

static inline bool
GcsBlockPcmXImageIdentityEqual(const GcsBlockPcmXImageIdentity *left,
							   const GcsBlockPcmXImageIdentity *right)
{
	return left != NULL && right != NULL && memcmp(left, right, sizeof(*left)) == 0;
}

static inline bool
GcsBlockPcmXImageBindingEqual(const GcsBlockPcmXImageBinding *left,
							  const GcsBlockPcmXImageBinding *right)
{
	return left != NULL && right != NULL && left->master_session == right->master_session
		   && left->required_page_scn == right->required_page_scn
		   && GcsBlockPcmXImageIdentityEqual(&left->identity, &right->identity);
}

/*
 * A formation tick may compare the active runtime only after two complete,
 * byte-identical authority samples.  A membership/session read that changes
 * while either sample is collected is transient evidence, not proof of drift;
 * the caller leaves ACTIVE unchanged and samples again on the next tick.
 */
static inline bool
cluster_gcs_pcm_x_formation_samples_stable(bool before_complete,
										   const PcmXPeerBinding before[PCM_X_PROTOCOL_NODE_LIMIT],
										   bool after_complete,
										   const PcmXPeerBinding after[PCM_X_PROTOCOL_NODE_LIMIT])
{
	return before_complete && after_complete && before != NULL && after != NULL
		   && memcmp(before, after, sizeof(PcmXPeerBinding) * PCM_X_PROTOCOL_NODE_LIMIT) == 0;
}


/* Derive the complete holder set and one image source from a coherent GRD
 * snapshot.  The active queue requester must still own the pending-X gate.
 * A canonical N authority and an ordered self-X round use the requester as a
 * synthetic source.  They still materialize a normal immutable A-record and
 * therefore do not introduce a no-image wire arm or bypass the FIFO ticket. */
static inline PcmXQueueResult
cluster_gcs_pcm_x_authority_holders_exact(const PcmAuthoritySnapshot *authority,
										  int32 requester_node, uint64 ticket_id,
										  uint32 *holders_out, int32 *source_out)
{
	uint64 pending_x_value;

	if (holders_out != NULL)
		*holders_out = 0;
	if (source_out != NULL)
		*source_out = -1;
	if (authority == NULL || holders_out == NULL || source_out == NULL || requester_node < 0
		|| requester_node >= PCM_X_PROTOCOL_NODE_LIMIT)
		return PCM_X_QUEUE_INVALID;
	if (!PcmPendingXQueueValue(ticket_id, &pending_x_value))
		return PCM_X_QUEUE_INVALID;
	if (authority->reserved[0] != 0 || authority->reserved[1] != 0)
		return PCM_X_QUEUE_CORRUPT;
	if (authority->pending_x_requester_node != requester_node
		|| authority->pending_x_since_lsn != pending_x_value)
		return PCM_X_QUEUE_STALE;
	if (authority->state == PCM_STATE_N) {
		if (authority->x_holder_node != -1 || authority->s_holders_bitmap != 0
			|| authority->master_holder.node_id != UINT32_MAX)
			return PCM_X_QUEUE_CORRUPT;
		*holders_out = UINT32_C(1) << requester_node;
		*source_out = requester_node;
		return PCM_X_QUEUE_OK;
	}
	if (authority->state == PCM_STATE_X) {
		if (authority->x_holder_node < 0 || authority->x_holder_node >= PCM_X_PROTOCOL_NODE_LIMIT
			|| authority->s_holders_bitmap != 0
			|| authority->master_holder.node_id != (uint32)authority->x_holder_node)
			return PCM_X_QUEUE_CORRUPT;
		*holders_out = UINT32_C(1) << authority->x_holder_node;
		*source_out = authority->x_holder_node;
		return PCM_X_QUEUE_OK;
	}
	if (authority->state != PCM_STATE_S)
		return PCM_X_QUEUE_CORRUPT;
	if (authority->x_holder_node != -1 || authority->s_holders_bitmap == 0
		|| authority->master_holder.node_id >= PCM_X_PROTOCOL_NODE_LIMIT
		|| (authority->s_holders_bitmap & (UINT32_C(1) << authority->master_holder.node_id)) == 0)
		return PCM_X_QUEUE_CORRUPT;

	*holders_out = authority->s_holders_bitmap;
	/* A requester that already owns S must be the image source.  Invalidating
	 * its resident S mirror would bump the immutable identity base before the
	 * later X commit, while FINAL_ACK is exact at base+1.  The source handoff
	 * instead reuses S+REVOKING as GRANT_PENDING and performs the sole bump at
	 * COMMIT_X.  A non-holder requester uses the canonical GRD master_holder. */
	if ((authority->s_holders_bitmap & (UINT32_C(1) << requester_node)) != 0)
		*source_out = requester_node;
	else
		*source_out = (int32)authority->master_holder.node_id;
	return PCM_X_QUEUE_OK;
}


/* Select one deterministic holder while preserving an exact bitmap ledger. */
static inline PcmXQueueResult
cluster_gcs_pcm_x_next_unacked_holder(uint32 pending, uint32 acked, int32 *holder_out)
{
	uint32 unacked;
	int32 node;

	if (holder_out == NULL)
		return PCM_X_QUEUE_INVALID;
	*holder_out = -1;
	if ((acked & ~pending) != 0)
		return PCM_X_QUEUE_CORRUPT;
	unacked = pending & ~acked;
	if (unacked == 0)
		return PCM_X_QUEUE_NOT_FOUND;
	for (node = 0; node < PCM_X_PROTOCOL_NODE_LIMIT; node++) {
		if ((unacked & (UINT32_C(1) << node)) != 0) {
			*holder_out = node;
			return PCM_X_QUEUE_OK;
		}
	}
	return PCM_X_QUEUE_CORRUPT;
}


/* Build the process-local GRD commit token from an already validated engine
 * FINAL_ACK barrier.  No wire or shared-memory layout is changed here. */
static inline bool
cluster_gcs_pcm_x_grd_handoff_token_build(const PcmXMasterFinalAckToken *final,
										  const PcmAuthoritySnapshot *authority,
										  PcmXGrdHandoffToken *handoff_out)
{
	const PcmXWaitIdentity *identity;

	if (handoff_out != NULL)
		memset(handoff_out, 0, sizeof(*handoff_out));
	if (final == NULL || authority == NULL || handoff_out == NULL)
		return false;
	identity = &final->final_ack.ref.identity;
	if (identity->node_id < 0 || identity->node_id >= PCM_X_PROTOCOL_NODE_LIMIT
		|| identity->request_id == 0 || final->final_ack.ref.handle.ticket_id == 0
		|| final->final_ack.ref.grant_generation == 0 || final->final_ack.image_id == 0
		|| final->final_ack.committed_own_generation == 0 || final->image.image_id == 0
		|| final->image.image_id != final->final_ack.image_id
		|| final->image.source_node >= PCM_X_PROTOCOL_NODE_LIMIT)
		return false;
	handoff_out->tag = identity->tag;
	handoff_out->authority = *authority;
	handoff_out->cluster_epoch = identity->cluster_epoch;
	handoff_out->request_id = identity->request_id;
	handoff_out->ticket_id = final->final_ack.ref.handle.ticket_id;
	handoff_out->grant_generation = final->final_ack.ref.grant_generation;
	handoff_out->image_id = final->image.image_id;
	handoff_out->source_own_generation = final->image.source_own_generation;
	handoff_out->page_scn = (SCN) final->image.page_scn;
	handoff_out->page_lsn = final->image.page_lsn;
	handoff_out->requester_node = identity->node_id;
	handoff_out->source_node = (int32) final->image.source_node;
	handoff_out->requester_procno = identity->procno;
	handoff_out->page_checksum = final->image.page_checksum;
	return true;
}


static inline bool
cluster_gcs_pcm_x_holder_image_identity_exact(const PcmXLocalHolderProgress *progress,
											  const PcmXTicketRef *ref, uint64 image_id,
											  int32 source_node)
{
	return progress != NULL && ref != NULL && image_id != 0 && source_node >= 0
		   && source_node < PCM_X_PROTOCOL_NODE_LIMIT
		   && memcmp(&progress->ref, ref, sizeof(*ref)) == 0 && progress->image.image_id == image_id
		   && progress->image.source_node == (uint32)source_node;
}


/* image_id exists from REVOKE onward.  The reliable leg, not generation,
 * proves that the rest of the image token has been published. */
static inline bool
cluster_gcs_pcm_x_holder_image_ready_exact(const PcmXLocalHolderProgress *progress,
										   const PcmXTicketRef *ref, uint64 image_id,
										   int32 source_node)
{
	return cluster_gcs_pcm_x_holder_image_identity_exact(progress, ref, image_id, source_node)
		   && progress->pending_opcode == PGRAC_IC_MSG_PCM_X_IMAGE_READY
		   && progress->phase == PGRAC_IC_MSG_PCM_X_IMAGE_READY
		   && progress->last_response_opcode == PGRAC_IC_MSG_PCM_X_REVOKE
		   && (progress->flags & PCM_X_LOCAL_TAG_F_HOLDER_TERMINAL_MASK) == 0;
}


/* DRAIN clears the reliable leg but retains the exact holder image until
 * RETIRE.  This is the post-transition capture used to release the dedup and
 * retained-image substrates without treating generation zero as absence. */
static inline bool
cluster_gcs_pcm_x_holder_image_drained_exact(const PcmXLocalHolderProgress *progress,
											 const PcmXTicketRef *ref, uint64 image_id,
											 int32 source_node)
{
	return cluster_gcs_pcm_x_holder_image_identity_exact(progress, ref, image_id, source_node)
		   && progress->pending_opcode == 0 && progress->phase == 0
		   && progress->last_response_opcode == PGRAC_IC_MSG_PCM_X_DRAIN_POLL
		   && (progress->flags & PCM_X_LOCAL_TAG_F_HOLDER_TERMINAL_MASK)
				  == PCM_X_LOCAL_TAG_F_HOLDER_TERMINAL_MASK;
}


/* Before the GRD handoff, 0/REVOKE is the only transfer phase that still
 * depends on the pending-X cookie.  Later reliable phases carry their own
 * exact replay evidence after the handoff has atomically cleared that cookie. */
static inline bool
cluster_gcs_pcm_x_transfer_pre_handoff_phase(uint16 pending_opcode)
{
	return pending_opcode == 0 || pending_opcode == PGRAC_IC_MSG_PCM_X_REVOKE;
}


/* A graph published for this confirm must be removed by the full waiter
 * identity if engine revalidation does not accept that exact publication. */
static inline bool
cluster_gcs_pcm_x_confirm_compensation_required(uint64 graph_generation,
												PcmXQueueResult revalidate_result)
{
	return graph_generation != 0 && revalidate_result != PCM_X_QUEUE_OK
		   && revalidate_result != PCM_X_QUEUE_DUPLICATE;
}


/*
 * Authenticate a tag-bearing PCM-X admission request before it can touch the
 * master queue.  Session/frontier and process-capacity checks remain in the
 * queue engine; this boundary binds the immutable identity to the DATA
 * connection, current epoch, and authoritative tag master.
 */
static inline bool
cluster_gcs_pcm_x_enqueue_ingress_valid(const PcmXEnqueuePayload *request, Size payload_length,
										int32 authenticated_node, uint64 current_epoch,
										int32 tag_master, int32 local_node)
{
	return request != NULL && payload_length == sizeof(*request) && authenticated_node >= 0
		   && authenticated_node < PCM_X_PROTOCOL_NODE_LIMIT
		   && request->identity.node_id == authenticated_node
		   && request->identity.cluster_epoch == current_epoch && tag_master == local_node
		   && local_node >= 0 && local_node < PCM_X_PROTOCOL_NODE_LIMIT
		   && request->identity.request_id != 0 && request->identity.wait_seq != 0
		   && request->prehandle.sender_session_incarnation != 0
		   && request->prehandle.prehandle_sequence != 0;
}

static inline bool
cluster_gcs_pcm_x_admit_confirm_ingress_valid(const PcmXPhasePayload *request, Size payload_length,
											  int32 authenticated_node, uint64 current_epoch,
											  int32 tag_master, int32 local_node)
{
	return request != NULL && payload_length == sizeof(*request) && authenticated_node >= 0
		   && authenticated_node < PCM_X_PROTOCOL_NODE_LIMIT
		   && request->ref.identity.node_id == authenticated_node
		   && request->ref.identity.cluster_epoch == current_epoch && tag_master == local_node
		   && local_node >= 0 && local_node < PCM_X_PROTOCOL_NODE_LIMIT
		   && request->ref.identity.request_id != 0 && request->ref.identity.wait_seq != 0
		   && request->ref.handle.ticket_id != 0 && request->ref.handle.queue_generation != 0
		   && request->ref.grant_generation == 0 && request->reason == 0
		   && request->phase == PCM_X_LOCAL_RELIABLE_PHASE_ADMIT_CONFIRM && request->flags == 0;
}

static inline bool
cluster_gcs_pcm_x_admit_confirm_ack_ingress_valid(const PcmXPhasePayload *ack, Size payload_length,
												  int32 authenticated_node, uint64 current_epoch,
												  int32 tag_master, int32 local_node)
{
	return ack != NULL && payload_length == sizeof(*ack) && authenticated_node >= 0
		   && authenticated_node < PCM_X_PROTOCOL_NODE_LIMIT && local_node >= 0
		   && local_node < PCM_X_PROTOCOL_NODE_LIMIT && ack->ref.identity.node_id == local_node
		   && ack->ref.identity.cluster_epoch == current_epoch && tag_master == authenticated_node
		   && ack->ref.identity.request_id != 0 && ack->ref.identity.wait_seq != 0
		   && ack->ref.handle.ticket_id != 0 && ack->ref.handle.queue_generation != 0
		   && ack->ref.grant_generation == 0 && ack->reason == 0
		   && ack->phase == PCM_X_LOCAL_RELIABLE_PHASE_ADMIT_CONFIRM && ack->flags == 0;
}

static inline bool
cluster_gcs_pcm_x_ticket_ref_wire_valid(const PcmXTicketRef *ref, uint64 current_epoch)
{
	return ref != NULL && ref->identity.node_id >= 0
		   && ref->identity.node_id < PCM_X_PROTOCOL_NODE_LIMIT
		   && ref->identity.cluster_epoch == current_epoch && ref->identity.request_id != 0
		   && ref->identity.wait_seq != 0 && ref->handle.ticket_id != 0
		   && ref->handle.queue_generation != 0 && ref->grant_generation == 0;
}

/* BLOCKER_SET originates at the probed holder, not at the ticket requester. */
static inline bool
cluster_gcs_pcm_x_blocker_header_ingress_valid(const PcmXBlockerSetHeaderPayload *header,
											   Size payload_length, int32 authenticated_node,
											   uint64 current_epoch, int32 tag_master,
											   int32 local_node)
{
	return header != NULL && payload_length == sizeof(*header) && authenticated_node >= 0
		   && authenticated_node < PCM_X_PROTOCOL_NODE_LIMIT && local_node >= 0
		   && local_node < PCM_X_PROTOCOL_NODE_LIMIT && tag_master == local_node
		   && cluster_gcs_pcm_x_ticket_ref_wire_valid(&header->ref, current_epoch)
		   && header->set_generation != 0 && header->set_generation != UINT64_MAX
		   && header->nblockers <= INT_MAX;
}

static inline bool
cluster_gcs_pcm_x_blocker_edge_ingress_valid(const PcmXBlockerChunkPayload *edge,
											 Size payload_length, int32 authenticated_node,
											 uint64 current_epoch, int32 tag_master,
											 int32 local_node)
{
	return edge != NULL && payload_length == sizeof(*edge) && authenticated_node >= 0
		   && authenticated_node < PCM_X_PROTOCOL_NODE_LIMIT && local_node >= 0
		   && local_node < PCM_X_PROTOCOL_NODE_LIMIT && tag_master == local_node
		   && edge->requester_node >= 0 && edge->requester_node < PCM_X_PROTOCOL_NODE_LIMIT
		   && edge->cluster_epoch == current_epoch && edge->request_id != 0
		   && edge->handle.ticket_id != 0 && edge->handle.queue_generation != 0
		   && edge->grant_generation == 0 && edge->set_generation != 0
		   && edge->set_generation != UINT64_MAX && edge->blocker.node_id == authenticated_node
		   && edge->blocker.cluster_epoch == current_epoch && edge->blocker.wait_seq != 0
		   && (edge->blocker.request_id != 0 || TransactionIdIsValid(edge->blocker.xid));
}

/*
 * PcmXPhasePayload has exactly 64 non-ref bits at bytes [88,96).  Type 48
 * uses all of them as one lossless set_generation: zero is the
 * master-to-holder PROBE arm; nonzero is the generation-exact ACK arm, so a
 * delayed ACK can never acknowledge a newer set on the same ticket.
 */
static inline void
cluster_gcs_pcm_x_blocker_ack_set_generation(PcmXPhasePayload *ack, uint64 set_generation)
{
	if (ack == NULL)
		return;
	ack->reason = (uint32)set_generation;
	ack->phase = (uint16)(set_generation >> 32);
	ack->flags = (uint16)(set_generation >> 48);
}

static inline uint64
cluster_gcs_pcm_x_blocker_ack_generation(const PcmXPhasePayload *ack)
{
	if (ack == NULL)
		return 0;
	return (uint64)ack->reason | ((uint64)ack->phase << 32) | ((uint64)ack->flags << 48);
}

/* Build only the nonzero ACK arm.  Invalid generations leave a zeroed
 * payload, so production callers cannot accidentally emit the reserved
 * all-zero PROBE encoding in non-assert builds. */
static inline bool
cluster_gcs_pcm_x_blocker_ack_build(const PcmXTicketRef *ref, uint64 set_generation,
									PcmXPhasePayload *ack)
{
	if (ack == NULL)
		return false;
	memset(ack, 0, sizeof(*ack));
	if (ref == NULL || set_generation == 0 || set_generation == UINT64_MAX)
		return false;
	ack->ref = *ref;
	cluster_gcs_pcm_x_blocker_ack_set_generation(ack, set_generation);
	return true;
}

static inline bool
cluster_gcs_pcm_x_blocker_control_ingress_valid(const PcmXPhasePayload *control,
												Size payload_length, int32 authenticated_node,
												uint64 current_epoch, int32 tag_master,
												int32 local_node)
{
	return control != NULL && payload_length == sizeof(*control) && authenticated_node >= 0
		   && authenticated_node < PCM_X_PROTOCOL_NODE_LIMIT && local_node >= 0
		   && local_node < PCM_X_PROTOCOL_NODE_LIMIT && tag_master == authenticated_node
		   && cluster_gcs_pcm_x_ticket_ref_wire_valid(&control->ref, current_epoch);
}

static inline bool
cluster_gcs_pcm_x_blocker_probe_ingress_valid(const PcmXPhasePayload *probe, Size payload_length,
											  int32 authenticated_node, uint64 current_epoch,
											  int32 tag_master, int32 local_node)
{
	return cluster_gcs_pcm_x_blocker_control_ingress_valid(
			   probe, payload_length, authenticated_node, current_epoch, tag_master, local_node)
		   && cluster_gcs_pcm_x_blocker_ack_generation(probe) == 0;
}

static inline bool
cluster_gcs_pcm_x_blocker_ack_ingress_valid(const PcmXPhasePayload *ack, Size payload_length,
											int32 authenticated_node, uint64 current_epoch,
											int32 tag_master, int32 local_node)
{
	uint64 set_generation = cluster_gcs_pcm_x_blocker_ack_generation(ack);

	return cluster_gcs_pcm_x_blocker_control_ingress_valid(ack, payload_length, authenticated_node,
														   current_epoch, tag_master, local_node)
		   && set_generation != 0 && set_generation != UINT64_MAX;
}

/* Transfer messages are legal only after the master assigns a grant generation. */
static inline bool
cluster_gcs_pcm_x_transfer_ref_wire_valid(const PcmXTicketRef *ref, uint64 current_epoch)
{
	return ref != NULL && ref->identity.node_id >= 0
		   && ref->identity.node_id < PCM_X_PROTOCOL_NODE_LIMIT
		   && ref->identity.cluster_epoch == current_epoch && ref->identity.request_id != 0
		   && ref->identity.wait_seq != 0 && ref->handle.ticket_id != 0
		   && ref->handle.queue_generation != 0 && ref->grant_generation != 0;
}

static inline bool
cluster_gcs_pcm_x_image_id_master_wire_valid(uint64 image_id, int32 expected_master_node)
{
	int32 encoded_master_node;

	return expected_master_node >= 0 && expected_master_node < PCM_X_PROTOCOL_NODE_LIMIT
		   && cluster_pcm_x_image_id_decode(image_id, &encoded_master_node, NULL)
		   && encoded_master_node == expected_master_node;
}

static inline bool
cluster_gcs_pcm_x_image_token_wire_valid(const PcmXImageToken *image, int32 expected_master_node)
{
	return image != NULL && image->source_node < PCM_X_PROTOCOL_NODE_LIMIT
		   && cluster_gcs_pcm_x_image_id_master_wire_valid(image->image_id, expected_master_node);
}

/* REVOKE is master -> current holder; the requester identity is third-party. */
static inline bool
cluster_gcs_pcm_x_revoke_ingress_valid(const PcmXRevokePayload *request, Size payload_length,
									   int32 authenticated_node, uint64 current_epoch,
									   int32 tag_master, int32 local_node)
{
	return request != NULL
		   && (payload_length == sizeof(*request) || payload_length == sizeof(PcmXRevokePayloadV2))
		   && authenticated_node >= 0
		   && authenticated_node < PCM_X_PROTOCOL_NODE_LIMIT && local_node >= 0
		   && local_node < PCM_X_PROTOCOL_NODE_LIMIT && tag_master == authenticated_node
		   && cluster_gcs_pcm_x_transfer_ref_wire_valid(&request->ref, current_epoch)
		   && cluster_gcs_pcm_x_image_id_master_wire_valid(request->image_id, authenticated_node);
}

/* IMAGE_READY is current holder -> master and binds the image to that holder. */
static inline bool
cluster_gcs_pcm_x_image_ready_ingress_valid(const PcmXGrantPayload *ready, Size payload_length,
											int32 authenticated_node, uint64 current_epoch,
											int32 tag_master, int32 local_node)
{
	return ready != NULL && payload_length == sizeof(*ready) && authenticated_node >= 0
		   && authenticated_node < PCM_X_PROTOCOL_NODE_LIMIT && local_node >= 0
		   && local_node < PCM_X_PROTOCOL_NODE_LIMIT && tag_master == local_node
		   && cluster_gcs_pcm_x_transfer_ref_wire_valid(&ready->ref, current_epoch)
		   && cluster_gcs_pcm_x_image_token_wire_valid(&ready->image, local_node)
		   && ready->image.source_node == (uint32)authenticated_node;
}

/* PREPARE_GRANT is master -> exact requester and may carry a third-party image. */
static inline bool
cluster_gcs_pcm_x_prepare_grant_ingress_valid(const PcmXGrantPayload *grant, Size payload_length,
											  int32 authenticated_node, uint64 current_epoch,
											  int32 tag_master, int32 local_node)
{
	return grant != NULL && payload_length == sizeof(*grant) && authenticated_node >= 0
		   && authenticated_node < PCM_X_PROTOCOL_NODE_LIMIT && local_node >= 0
		   && local_node < PCM_X_PROTOCOL_NODE_LIMIT && tag_master == authenticated_node
		   && cluster_gcs_pcm_x_transfer_ref_wire_valid(&grant->ref, current_epoch)
		   && grant->ref.identity.node_id == local_node
		   && cluster_gcs_pcm_x_image_token_wire_valid(&grant->image, authenticated_node);
}

/* INSTALL_READY is one canonical requester -> master application ACK.  Both
 * the V1 (104-byte, no rebase) and V2 (112-byte) exact frames are legal; the
 * caller normalizes a V1 frame to rebased_own_generation == 0 before this
 * check.  A V1 frame carrying a nonzero rebase is impossible by construction
 * and a V2 rebase must be strictly newer than the immutable identity base. */
static inline bool
cluster_gcs_pcm_x_install_ready_ingress_valid(const PcmXInstallReadyPayload *ready,
											  Size payload_length, int32 authenticated_node,
											  uint64 current_epoch, int32 tag_master,
											  int32 local_node, bool rebase_wire_active,
											  bool source_supports_rebase)
{
	return ready != NULL
		   && (payload_length == sizeof(*ready) || payload_length == PCM_X_INSTALL_READY_V1_LEN)
		   && (payload_length == sizeof(*ready) || ready->rebased_own_generation == 0)
		   && (payload_length != sizeof(*ready) || (rebase_wire_active && source_supports_rebase))
		   && (ready->rebased_own_generation == 0
			   || (ready->rebased_own_generation != UINT64_MAX
				   && ready->rebased_own_generation > ready->ref.identity.base_own_generation))
		   && authenticated_node >= 0 && authenticated_node < PCM_X_PROTOCOL_NODE_LIMIT
		   && local_node >= 0 && local_node < PCM_X_PROTOCOL_NODE_LIMIT && tag_master == local_node
		   && cluster_gcs_pcm_x_transfer_ref_wire_valid(&ready->ref, current_epoch)
		   && ready->ref.identity.node_id == authenticated_node
		   && cluster_gcs_pcm_x_image_id_master_wire_valid(ready->image_id, local_node)
		   && ready->result == PCM_X_QUEUE_OK && ready->phase == PGRAC_IC_MSG_PCM_X_INSTALL_READY
		   && ready->flags == 0;
}

/* COMMIT_X is one canonical master -> exact requester application command. */
static inline bool
cluster_gcs_pcm_x_commit_x_ingress_valid(const PcmXPhasePayload *commit, Size payload_length,
										 int32 authenticated_node, uint64 current_epoch,
										 int32 tag_master, int32 local_node)
{
	return commit != NULL && payload_length == sizeof(*commit) && authenticated_node >= 0
		   && authenticated_node < PCM_X_PROTOCOL_NODE_LIMIT && local_node >= 0
		   && local_node < PCM_X_PROTOCOL_NODE_LIMIT && tag_master == authenticated_node
		   && cluster_gcs_pcm_x_transfer_ref_wire_valid(&commit->ref, current_epoch)
		   && commit->ref.identity.node_id == local_node && commit->reason == 0
		   && commit->phase == PGRAC_IC_MSG_PCM_X_COMMIT_X && commit->flags == 0;
}

/* FINAL_ACK proves the requester committed exactly one ownership generation.
 * The wire check is only a monotonic floor against the immutable identity
 * base: a published INSTALL_READY rebase may have moved the effective grant
 * base, and the exact "+1" proof runs under the master ticket lock. */
static inline bool
cluster_gcs_pcm_x_final_ack_ingress_valid(const PcmXFinalAckPayload *ack, Size payload_length,
										  int32 authenticated_node, uint64 current_epoch,
										  int32 tag_master, int32 local_node)
{
	return ack != NULL && payload_length == sizeof(*ack) && authenticated_node >= 0
		   && authenticated_node < PCM_X_PROTOCOL_NODE_LIMIT && local_node >= 0
		   && local_node < PCM_X_PROTOCOL_NODE_LIMIT && tag_master == local_node
		   && cluster_gcs_pcm_x_transfer_ref_wire_valid(&ack->ref, current_epoch)
		   && ack->ref.identity.node_id == authenticated_node
		   && cluster_gcs_pcm_x_image_id_master_wire_valid(ack->image_id, local_node)
		   && ack->ref.identity.base_own_generation != UINT64_MAX
		   && ack->committed_own_generation > ack->ref.identity.base_own_generation;
}

/* FINAL_COMMIT_ACK is the canonical master -> requester application ACK. */
static inline bool
cluster_gcs_pcm_x_final_commit_ack_ingress_valid(const PcmXPhasePayload *ack, Size payload_length,
												 int32 authenticated_node, uint64 current_epoch,
												 int32 tag_master, int32 local_node)
{
	return ack != NULL && payload_length == sizeof(*ack) && authenticated_node >= 0
		   && authenticated_node < PCM_X_PROTOCOL_NODE_LIMIT && local_node >= 0
		   && local_node < PCM_X_PROTOCOL_NODE_LIMIT && tag_master == authenticated_node
		   && cluster_gcs_pcm_x_transfer_ref_wire_valid(&ack->ref, current_epoch)
		   && ack->ref.identity.node_id == local_node && ack->reason == 0
		   && ack->phase == PGRAC_IC_MSG_PCM_X_FINAL_COMMIT_ACK && ack->flags == 0;
}

/* FINAL_CONFIRM is the canonical requester -> master terminal confirmation. */
static inline bool
cluster_gcs_pcm_x_final_confirm_ingress_valid(const PcmXPhasePayload *confirm, Size payload_length,
											  int32 authenticated_node, uint64 current_epoch,
											  int32 tag_master, int32 local_node)
{
	return confirm != NULL && payload_length == sizeof(*confirm) && authenticated_node >= 0
		   && authenticated_node < PCM_X_PROTOCOL_NODE_LIMIT && local_node >= 0
		   && local_node < PCM_X_PROTOCOL_NODE_LIMIT && tag_master == local_node
		   && cluster_gcs_pcm_x_transfer_ref_wire_valid(&confirm->ref, current_epoch)
		   && confirm->ref.identity.node_id == authenticated_node && confirm->reason == 0
		   && confirm->phase == PGRAC_IC_MSG_PCM_X_FINAL_CONFIRM && confirm->flags == 0;
}

static inline bool
cluster_gcs_pcm_x_prehandle_cancel_ingress_valid(const PcmXPrehandleCancelPayload *request,
												 Size payload_length, int32 authenticated_node,
												 uint64 current_epoch, int32 tag_master,
												 int32 local_node)
{
	return request != NULL && payload_length == sizeof(*request) && authenticated_node >= 0
		   && authenticated_node < PCM_X_PROTOCOL_NODE_LIMIT
		   && request->identity.node_id == authenticated_node
		   && request->identity.cluster_epoch == current_epoch && tag_master == local_node
		   && local_node >= 0 && local_node < PCM_X_PROTOCOL_NODE_LIMIT
		   && request->identity.request_id != 0 && request->identity.wait_seq != 0
		   && request->prehandle.sender_session_incarnation != 0
		   && request->prehandle.prehandle_sequence != 0;
}

static inline bool
cluster_gcs_pcm_x_prehandle_cancel_ack_ingress_valid(const PcmXAdmitAckPayload *ack,
													 Size payload_length, int32 authenticated_node,
													 uint64 current_epoch, int32 tag_master,
													 int32 local_node)
{
	return ack != NULL && payload_length == sizeof(*ack) && authenticated_node >= 0
		   && authenticated_node < PCM_X_PROTOCOL_NODE_LIMIT && local_node >= 0
		   && local_node < PCM_X_PROTOCOL_NODE_LIMIT && ack->ref.identity.node_id == local_node
		   && ack->ref.identity.cluster_epoch == current_epoch && tag_master == authenticated_node
		   && ack->ref.identity.request_id != 0 && ack->ref.identity.wait_seq != 0
		   && ack->ref.handle.ticket_id != 0 && ack->ref.handle.queue_generation != 0
		   && ack->ref.grant_generation == 0 && ack->prehandle.sender_session_incarnation != 0
		   && ack->prehandle.prehandle_sequence != 0 && ack->result == PCM_X_QUEUE_OK
		   && ack->phase == PCM_X_LOCAL_RELIABLE_PHASE_PREHANDLE_CANCEL && ack->flags == 0;
}

static inline bool
cluster_gcs_pcm_x_cancel_ingress_valid(const PcmXPhasePayload *request, Size payload_length,
									   int32 authenticated_node, uint64 current_epoch,
									   int32 tag_master, int32 local_node)
{
	return request != NULL && payload_length == sizeof(*request) && authenticated_node >= 0
		   && authenticated_node < PCM_X_PROTOCOL_NODE_LIMIT
		   && request->ref.identity.node_id == authenticated_node
		   && request->ref.identity.cluster_epoch == current_epoch && tag_master == local_node
		   && local_node >= 0 && local_node < PCM_X_PROTOCOL_NODE_LIMIT
		   && request->ref.identity.request_id != 0 && request->ref.identity.wait_seq != 0
		   && request->ref.handle.ticket_id != 0 && request->ref.handle.queue_generation != 0
		   && request->ref.grant_generation == 0 && request->reason == 0
		   && request->phase == PCM_X_LOCAL_RELIABLE_PHASE_CANCEL && request->flags == 0;
}

static inline bool
cluster_gcs_pcm_x_cancel_ack_ingress_valid(const PcmXPhasePayload *ack, Size payload_length,
										   int32 authenticated_node, uint64 current_epoch,
										   int32 tag_master, int32 local_node)
{
	return ack != NULL && payload_length == sizeof(*ack) && authenticated_node >= 0
		   && authenticated_node < PCM_X_PROTOCOL_NODE_LIMIT && local_node >= 0
		   && local_node < PCM_X_PROTOCOL_NODE_LIMIT && ack->ref.identity.node_id == local_node
		   && ack->ref.identity.cluster_epoch == current_epoch && tag_master == authenticated_node
		   && ack->ref.identity.request_id != 0 && ack->ref.identity.wait_seq != 0
		   && ack->ref.handle.ticket_id != 0 && ack->ref.handle.queue_generation != 0
		   && ack->ref.grant_generation == 0 && ack->reason == 0
		   && ack->phase == PCM_X_LOCAL_RELIABLE_PHASE_CANCEL && ack->flags == 0;
}

/* Terminal messages bind the final, non-sentinel grant generation as well. */
static inline bool
cluster_gcs_pcm_x_terminal_ref_wire_valid(const PcmXTicketRef *ref, uint64 current_epoch)
{
	return ref != NULL && ref->identity.node_id >= 0
		   && ref->identity.node_id < PCM_X_PROTOCOL_NODE_LIMIT
		   && ref->identity.cluster_epoch == current_epoch && ref->identity.request_id != 0
		   && ref->identity.wait_seq != 0 && ref->handle.ticket_id != 0
		   && ref->handle.queue_generation != 0 && ref->grant_generation != UINT64_MAX;
}

/* DRAIN_POLL is authoritative only from the current master of this tag. */
static inline bool
cluster_gcs_pcm_x_drain_poll_ingress_valid(const PcmXDrainPollPayload *poll, Size payload_length,
										   int32 authenticated_node, uint64 current_epoch,
										   int32 tag_master, int32 local_node)
{
	return poll != NULL && payload_length == sizeof(*poll) && authenticated_node >= 0
		   && authenticated_node < PCM_X_PROTOCOL_NODE_LIMIT && local_node >= 0
		   && local_node < PCM_X_PROTOCOL_NODE_LIMIT && tag_master == authenticated_node
		   && cluster_gcs_pcm_x_terminal_ref_wire_valid(&poll->ref, current_epoch)
		   && poll->drain_generation != 0 && poll->drain_generation != UINT64_MAX;
}

/* DRAIN_ACK may come from any involved participant, but only to this master. */
static inline bool
cluster_gcs_pcm_x_drain_ack_ingress_valid(const PcmXPhasePayload *ack, Size payload_length,
										  int32 authenticated_node, uint64 current_epoch,
										  int32 tag_master, int32 local_node)
{
	return ack != NULL && payload_length == sizeof(*ack) && authenticated_node >= 0
		   && authenticated_node < PCM_X_PROTOCOL_NODE_LIMIT && local_node >= 0
		   && local_node < PCM_X_PROTOCOL_NODE_LIMIT && tag_master == local_node
		   && cluster_gcs_pcm_x_terminal_ref_wire_valid(&ack->ref, current_epoch)
		   && ack->reason == 0 && ack->phase == 0 && ack->flags == 0;
}

/* RETIRE_UP_TO is tagless; bind it to the DATA peer session and local target. */
static inline bool
cluster_gcs_pcm_x_retire_request_ingress_valid(const PcmXRetirePayload *request,
											   Size payload_length, int32 authenticated_node,
											   uint64 authenticated_session, uint64 current_epoch,
											   int32 local_node)
{
	return request != NULL && payload_length == sizeof(*request) && authenticated_node >= 0
		   && authenticated_node < PCM_X_PROTOCOL_NODE_LIMIT && local_node >= 0
		   && local_node < PCM_X_PROTOCOL_NODE_LIMIT && authenticated_session != 0
		   && request->cluster_epoch == current_epoch
		   && request->master_session_incarnation == authenticated_session
		   && request->retire_through_ticket_id != 0 && request->sender_node == local_node
		   && request->flags == 0;
}

/* RETIRE_ACK names its authenticated responder and this master's session. */
static inline bool
cluster_gcs_pcm_x_retire_ack_ingress_valid(const PcmXRetirePayload *ack, Size payload_length,
										   int32 authenticated_node, uint64 current_epoch,
										   uint64 master_session, int32 local_node)
{
	return ack != NULL && payload_length == sizeof(*ack) && authenticated_node >= 0
		   && authenticated_node < PCM_X_PROTOCOL_NODE_LIMIT && local_node >= 0
		   && local_node < PCM_X_PROTOCOL_NODE_LIMIT && ack->cluster_epoch == current_epoch
		   && master_session != 0 && ack->master_session_incarnation == master_session
		   && ack->retire_through_ticket_id != 0 && ack->sender_node == authenticated_node
		   && ack->flags == 0;
}

static inline void
cluster_gcs_pcm_x_vertex_from_identity(const PcmXWaitIdentity *identity, ClusterLmdVertex *vertex)
{
	if (vertex == NULL)
		return;
	memset(vertex, 0, sizeof(*vertex));
	if (identity == NULL)
		return;
	vertex->node_id = identity->node_id;
	vertex->procno = identity->procno;
	vertex->cluster_epoch = identity->cluster_epoch;
	vertex->request_id = identity->request_id;
	vertex->xid = identity->xid;
	vertex->wait_seq = identity->wait_seq;
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
								   bool direct_target_prepared,
								   const BufferTag *invalidate_tag, uint64 invalidate_epoch,
								   int32 invalidate_master_node)
{
	return in_use && !reply_received && !stale && slot_tag != NULL && invalidate_tag != NULL
		   && transition_id == (uint8)PCM_TRANS_N_TO_S
		   && BufferTagsEqual(slot_tag, invalidate_tag) && request_epoch == invalidate_epoch
		   && expected_master_node == invalidate_master_node && !direct_target_prepared
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
	GCS_BLOCK_REPLY_UNDO_MULTI_VERDICT_RESULT = 20 /* PGRAC: spec-7.1 D3-b NEW; the
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
				 "spec-7.1 D3-b undo-multi-verdict status must be the tail enum value");

/* PGRAC: spec-6.12i / spec-7.1 — every undo-plane reply kind (TT-header fetch,
 * single-xid verdict, batched multi-member verdict) ships the BLCKSZ page plus
 * a co-sampled ClusterGcsUndoAuthTrailer and overrides the reply header's
 * epoch / page_lsn with the LMS-sampled live authority.  Centralised so every
 * ship/parse site treats the three identically (D-i3 authority carriage). */
static inline bool
GcsBlockReplyStatusCarriesUndoAuthTrailer(GcsBlockReplyStatus status)
{
	return status == GCS_BLOCK_REPLY_UNDO_TT_FETCH_RESULT
		   || status == GCS_BLOCK_REPLY_UNDO_VERDICT_RESULT
		   || status == GCS_BLOCK_REPLY_UNDO_MULTI_VERDICT_RESULT;
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

/*
 * Classify an INVALIDATE_ACK against one exact queue transfer round.  A
 * nonmatching tag/epoch/request belongs to the legacy invalidate machinery;
 * once that identity matches, however, an unexpected or already-credited
 * sender is consumed here so it cannot mutate GRD through the legacy
 * slotless path.  The caller authenticates the transport/session separately.
 */
static inline PcmXQueueResult
cluster_gcs_pcm_x_invalidate_ack_match_exact(const PcmXMasterDriveSnapshot *snapshot,
											 const GcsBlockInvalidateAckPayload *ack,
											 uint64 current_epoch, int32 authenticated_source)
{
	uint32 source_bit;

	if (snapshot == NULL || ack == NULL || authenticated_source < 0
		|| authenticated_source >= PCM_X_PROTOCOL_NODE_LIMIT)
		return PCM_X_QUEUE_INVALID;
	if (snapshot->ticket_state != PCM_XT_ACTIVE_TRANSFER
		|| snapshot->ref.identity.cluster_epoch != current_epoch || ack->epoch != current_epoch
		|| ack->request_id != snapshot->ref.identity.request_id
		|| !BufferTagsEqual(&ack->tag, &snapshot->ref.identity.tag))
		return PCM_X_QUEUE_NOT_FOUND;
	if (ack->sender_node != authenticated_source)
		return PCM_X_QUEUE_STALE;
	source_bit = UINT32_C(1) << (uint32)authenticated_source;
	if ((snapshot->pending_s_holders_bitmap & source_bit) == 0)
		return PCM_X_QUEUE_STALE;
	if ((snapshot->acked_s_holders_bitmap & source_bit) != 0)
		return PCM_X_QUEUE_DUPLICATE;
	if (ack->ack_status == GCS_BLOCK_INVALIDATE_ACK_STATUS_RETRYABLE_BUSY)
		return PCM_X_QUEUE_BUSY;
	if (ack->ack_status == 0 || ack->ack_status == 2)
		return PCM_X_QUEUE_OK;
	return PCM_X_QUEUE_BAD_STATE;
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
static inline void
GcsBlockForwardPayloadSetUndoVerdictRequest(GcsBlockForwardPayload *p, bool authoritative)
{
	p->reserved_0[6] = authoritative ? (uint8)5 : (uint8)2;
}

static inline bool
GcsBlockForwardPayloadIsUndoVerdictRequest(const GcsBlockForwardPayload *p)
{
	return p->reserved_0[6] == (uint8)2 || p->reserved_0[6] == (uint8)5;
}

static inline bool
GcsBlockForwardPayloadIsUndoVerdictAuthoritative(const GcsBlockForwardPayload *p)
{
	return p->reserved_0[6] == (uint8)5;
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
	CLUSTER_GCS_UNDO_VERDICT_ABORTED = 3
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
 * for that worker to deliver a reply.  Only the two conditional-lock misses
 * are therefore retryable through the established fresh reservation/request
 * boundary.  Residency/current-image failures remain structural, while HC89
 * keeps its explicit one-retry hot-page bound. */
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
 * copy).  InvalidBuffer -> block transferred away -> skip the stamp. */
extern Buffer cluster_bufmgr_lock_resident_for_stamp(RelFileLocator rlocator, ForkNumber forknum,
													 BlockNumber blocknum);
extern void cluster_bufmgr_unlock_resident_stamp(Buffer buffer);

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
extern ClusterBufmgrGcsDowngradeOutcome
cluster_bufmgr_downgrade_x_to_s_for_gcs_prepare_image(
	BufferTag tag, XLogRecPtr *out_page_lsn, char *dst,
	ClusterBufmgrGcsCopyRefusal *out_refusal);
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
													 bool *out_retry_denied);
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
extern ClusterICSendResult cluster_gcs_block_send_direct_zero_reply(
	int32 dest_node, const GcsBlockReplyHeader *header);

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
extern uint64 cluster_gcs_get_block_dedup_pcm_x_stage_count(void);
extern uint64 cluster_gcs_get_block_dedup_pcm_x_replay_count(void);
extern uint64 cluster_gcs_get_block_dedup_pcm_x_release_count(void);
extern uint64 cluster_gcs_get_block_dedup_pcm_x_failclosed_count(void);
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
extern uint64 cluster_gcs_get_pcm_x_image_fetch_recovering_retry_count(void);
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
extern void cluster_gcs_block_pcm_x_formation_tick(void);
extern void cluster_gcs_block_pcm_x_owner_start(int worker_id);
extern void cluster_gcs_block_pcm_x_image_pump_tick(int worker_id);
extern void cluster_gcs_pcm_x_terminal_kick(const PcmXTicketRef *ref);
/* Type-48 zero-generation arm.  One call stages at most one exact PROBE;
 * duplicate calls replay the same durable master leg. */
extern void cluster_gcs_pcm_x_blocker_probe_kick(const PcmXTicketRef *ref, int32 holder_node);

/* Stage one PCM-X application frame onto the BufferTag-owning DATA worker.
 * This is also the loopback path when the target is this node: the LMS worker
 * dispatches the frame locally instead of relying on the IC self-send no-op. */
extern bool cluster_gcs_pcm_x_stage_frame(uint8 msg_type, int32 dest_node_id, const void *payload,
										  uint16 payload_len);
/* Join the node-local FIFO, drive the sole remote queue request when this
 * backend is leader, and return one exact writer claim.  claim_handed_off is
 * published before requester cleanup authority is dropped, closing the
 * owner-exit gap while bufmgr adopts the claim.  The caller must keep the
 * claim through content-lock ownership and release it after UNLOCK. */
extern PcmXQueueResult cluster_gcs_pcm_x_acquire_writer(BufferDesc *buf,
														PcmXLocalWriterClaim *claim_out,
														bool *claim_handed_off);
/* Diagnostic: source line of this backend's most recent non-OK
 * acquire-writer exit (0 when it never failed). */
extern int cluster_gcs_pcm_x_requester_last_fail_line(void);
extern PcmXQueueResult
cluster_gcs_pcm_x_writer_claim_release_and_wake_exact(const PcmXLocalWriterClaim *claim);
/* Error/owner-exit cleanup adapter: never propagates ERROR.  A caught ERROR
 * closes the runtime and returns CORRUPT while leaving the exact claim in the
 * caller's ledger for fail-closed evidence/retry. */
extern PcmXQueueResult
cluster_gcs_pcm_x_writer_claim_cleanup_and_wake_noexcept(const PcmXLocalWriterClaim *claim);


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
