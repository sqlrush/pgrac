/*-------------------------------------------------------------------------
 *
 * cluster_pcm_x_bufmgr.h
 *	  Opaque buffer-manager boundary for PCM-X ownership reservations.
 *
 * Queue JOIN/WAIT code must not call begin_x_reservation.  Only the
 * ACTIVE_TRANSFER PREPARE leg may begin the short GRANT_PENDING interval;
 * the returned token is then mandatory for exact finish or abort.  A remote
 * source S->X conversion must first complete the protocol's exact S->N + ACK
 * and capture a fresh N tuple; begin_x_reservation deliberately rejects an S
 * snapshot.  A requester acting as its own N/S/X source instead hands the
 * existing exact REVOKING token to GRANT_PENDING at PREPARE, so it still has
 * only one ownership lifecycle and one committed generation bump.  All
 * exported functions acquire the BufferDesc header spinlock themselves.  No
 * caller may access ClusterPcmOwnEntry directly across this boundary.
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 *-------------------------------------------------------------------------
 */
#ifndef CLUSTER_PCM_X_BUFMGR_H
#define CLUSTER_PCM_X_BUFMGR_H

#include "access/xlogdefs.h"
#include "cluster/cluster_pcm_own.h"
#include "cluster/cluster_pcm_x_convert.h"
#include "storage/buf_internals.h"

/* A retry batch is bounded and cancellable.  Registration may begin another
 * batch while the runtime remains healthy because bypassing a closed holder
 * barrier would expose untracked page bytes.  Post-content-lock unregister
 * instead defers its exact handle after one batch so ordinary UNLOCK never
 * becomes a transaction ERROR merely because RETIRE owns the short gate. */
#define CLUSTER_PCM_X_HOLDER_RETRY_BATCH_WAITS 5

typedef enum ClusterPcmXHolderRetryAction {
	CLUSTER_PCM_X_HOLDER_RETRY_COMPLETE = 0,
	CLUSTER_PCM_X_HOLDER_RETRY_WAIT,
	CLUSTER_PCM_X_HOLDER_RETRY_DEFER,
	CLUSTER_PCM_X_HOLDER_RETRY_FAIL
} ClusterPcmXHolderRetryAction;

typedef enum ClusterPcmXWriterRetryAction {
	CLUSTER_PCM_X_WRITER_RETRY_COMPLETE = 0,
	CLUSTER_PCM_X_WRITER_RETRY_WAIT,
	CLUSTER_PCM_X_WRITER_RETRY_DEFER,
	CLUSTER_PCM_X_WRITER_RETRY_FAIL
} ClusterPcmXWriterRetryAction;

typedef enum ClusterPcmXOwnerExitAction {
	CLUSTER_PCM_X_OWNER_EXIT_COMPLETE = 0,
	CLUSTER_PCM_X_OWNER_EXIT_RETRY,
	CLUSTER_PCM_X_OWNER_EXIT_PRESERVE
} ClusterPcmXOwnerExitAction;

/* Exit callbacks have no later safe entrance: a short admission/retire gate
 * must be waited out while this exact runtime is still active.  Once the
 * runtime/incarnation changes, evidence is preserved behind RECOVERY_BLOCKED
 * rather than being retried against a different authority generation. */
static inline ClusterPcmXOwnerExitAction
cluster_pcm_x_owner_exit_action(PcmXQueueResult result, bool not_found_is_complete,
								bool runtime_active)
{
	if (result == PCM_X_QUEUE_OK || (not_found_is_complete && result == PCM_X_QUEUE_NOT_FOUND))
		return CLUSTER_PCM_X_OWNER_EXIT_COMPLETE;
	if (runtime_active && (result == PCM_X_QUEUE_GATE_RETRY || result == PCM_X_QUEUE_BUSY))
		return CLUSTER_PCM_X_OWNER_EXIT_RETRY;
	return CLUSTER_PCM_X_OWNER_EXIT_PRESERVE;
}

static inline ClusterPcmXHolderRetryAction
cluster_pcm_x_holder_register_retry_action(PcmXQueueResult result, bool runtime_active)
{
	if (result == PCM_X_QUEUE_OK || result == PCM_X_QUEUE_DUPLICATE)
		return CLUSTER_PCM_X_HOLDER_RETRY_COMPLETE;
	if (result == PCM_X_QUEUE_GATE_RETRY || result == PCM_X_QUEUE_BARRIER_CLOSED
		|| (result == PCM_X_QUEUE_NOT_READY && runtime_active))
		return CLUSTER_PCM_X_HOLDER_RETRY_WAIT;
	return CLUSTER_PCM_X_HOLDER_RETRY_FAIL;
}

static inline ClusterPcmXHolderRetryAction
cluster_pcm_x_holder_unregister_retry_action(PcmXQueueResult result, uint32 waits_used)
{
	if (result == PCM_X_QUEUE_OK || result == PCM_X_QUEUE_NOT_FOUND)
		return CLUSTER_PCM_X_HOLDER_RETRY_COMPLETE;
	if (result != PCM_X_QUEUE_GATE_RETRY && result != PCM_X_QUEUE_BUSY)
		return CLUSTER_PCM_X_HOLDER_RETRY_FAIL;
	return waits_used < CLUSTER_PCM_X_HOLDER_RETRY_BATCH_WAITS ? CLUSTER_PCM_X_HOLDER_RETRY_WAIT
															   : CLUSTER_PCM_X_HOLDER_RETRY_DEFER;
}

static inline ClusterPcmXWriterRetryAction
cluster_pcm_x_writer_release_retry_action(PcmXQueueResult result, uint32 waits_used)
{
	if (result == PCM_X_QUEUE_OK)
		return CLUSTER_PCM_X_WRITER_RETRY_COMPLETE;
	if (result != PCM_X_QUEUE_GATE_RETRY && result != PCM_X_QUEUE_BUSY)
		return CLUSTER_PCM_X_WRITER_RETRY_FAIL;
	return waits_used < CLUSTER_PCM_X_HOLDER_RETRY_BATCH_WAITS ? CLUSTER_PCM_X_WRITER_RETRY_WAIT
															   : CLUSTER_PCM_X_WRITER_RETRY_DEFER;
}

static inline long
cluster_pcm_x_holder_retry_delay_ms(uint32 wait_index)
{
	uint32 bounded_index = Min(wait_index, (uint32)CLUSTER_PCM_X_HOLDER_RETRY_BATCH_WAITS - 1);

	return 2L << bounded_index;
}

typedef struct ClusterPcmOwnSnapshot {
	BufferTag tag;
	uint64 generation;
	uint64 reservation_token;
	uint32 flags;
	uint8 pcm_state;
	uint8 _reserved[3];
} ClusterPcmOwnSnapshot;

StaticAssertDecl(sizeof(ClusterPcmOwnSnapshot) == 48, "ClusterPcmOwnSnapshot must remain 48 bytes");

/* The queue returns execution authority before bufmgr takes content EXCLUSIVE.
 * Bind that claim to the one committed ownership generation and require the
 * same complete tuple again after the content-lock window. */
static inline bool
cluster_pcm_x_writer_grant_snapshot_exact(const PcmXLocalWriterClaim *claim,
										  const ClusterPcmOwnSnapshot *granted,
										  const ClusterPcmOwnSnapshot *live)
{
	return claim != NULL && granted != NULL && live != NULL && claim->flags == 0
		   && claim->writer.flags == 0 && claim->claim_generation != 0
		   && claim->writer.identity.base_own_generation != UINT64_MAX
		   && claim->grant_base_own_generation != UINT64_MAX
		   /* A' rebase: the requester copies the published effective grant
			* base into the claim; zero keeps the enqueue-time identity base. */
		   && granted->generation
				  == (claim->grant_base_own_generation != 0
						  ? claim->grant_base_own_generation
						  : claim->writer.identity.base_own_generation)
						 + 1
		   && granted->reservation_token != 0 && granted->flags == 0
		   && granted->pcm_state == (uint8)PCM_STATE_X
		   && BufferTagsEqual(&granted->tag, &claim->writer.identity.tag)
		   && claim->active_slot.slot_index == claim->writer.membership_slot.slot_index
		   && claim->active_slot.slot_generation == claim->writer.membership_slot.slot_generation
		   && claim->local_round == claim->writer.local_round && claim->role == claim->writer.role
		   && (claim->role == PCM_X_LOCAL_ROLE_NODE_LEADER
			   || claim->role == PCM_X_LOCAL_ROLE_FOLLOWER)
		   && BufferTagsEqual(&live->tag, &granted->tag) && live->generation == granted->generation
		   && live->reservation_token == granted->reservation_token && live->flags == granted->flags
		   && live->pcm_state == granted->pcm_state;
}

/* A queue-managed X remains node-owned until its exact DRAIN/RETIRE lane
 * releases it.  The legacy cache-off unlock is legal only when no queue claim
 * governed this content-lock interval. */
static inline bool
cluster_pcm_x_should_release_legacy_on_unlock(bool local_cache, bool queue_managed)
{
	return !local_cache && !queue_managed;
}

/* A committed node X is cache residency authority, not a fresh writer
 * conversion.  Consult the coherent ownership tuple before JOIN so repeated
 * local LockBuffer(X) calls keep the spec-4.7a hold-until-revoked fast path.
 * Any transition flag forces the caller through normal arbitration/recheck. */
static inline bool
cluster_pcm_x_cached_cover_bypasses_queue(bool local_cache, bool requested_x, uint8 pcm_state,
										  uint32 flags)
{
	return local_cache && requested_x && pcm_state == (uint8)PCM_STATE_X && flags == 0;
}

/* ConditionalLockBuffer cannot initiate a PCM conversion.  Preserve native
 * PostgreSQL behavior while PCM is inactive and for relations outside the
 * coherence domain; an active tracked page must already hold exact X.  Live
 * transition/retained evidence remains closed regardless of runtime state. */
static inline bool
cluster_pcm_x_conditional_lock_allowed(bool runtime_active, bool tracked, bool retained_image,
									   uint8 pcm_state, uint32 flags)
{
	return !retained_image && flags == 0
		   && (!runtime_active || !tracked || pcm_state == (uint8)PCM_STATE_X);
}

typedef ClusterPcmOwnSnapshot ClusterPcmOwnEvictionCapture;

typedef enum ClusterPcmXGrantReservationKind {
	CLUSTER_PCM_X_GRANT_RESERVATION_INVALID = 0,
	CLUSTER_PCM_X_GRANT_RESERVATION_N_NEW,
	CLUSTER_PCM_X_GRANT_RESERVATION_N_REVOKE_HANDOFF,
	CLUSTER_PCM_X_GRANT_RESERVATION_S_REVOKE_HANDOFF,
	CLUSTER_PCM_X_GRANT_RESERVATION_X_REVOKE_HANDOFF
} ClusterPcmXGrantReservationKind;

/* Classify every legal PREPARE reservation shape.  Ordinary remote-image N
 * acquisition allocates the next monotonic token.  A requester acting as its
 * own N/S/X image source instead reuses the already-live revoke token and
 * changes only its role.  Every shape retains tag/generation/pcm_state until
 * the single grant commit. */
static inline ClusterPcmXGrantReservationKind
cluster_pcm_x_grant_reservation_kind(const ClusterPcmOwnSnapshot *live,
									 const ClusterPcmOwnSnapshot *base, uint64 reservation_token)
{
	if (live == NULL || base == NULL || reservation_token == 0 || base->generation == UINT64_MAX
		|| !BufferTagsEqual(&live->tag, &base->tag) || live->generation != base->generation
		|| live->reservation_token != reservation_token || live->flags != PCM_OWN_FLAG_GRANT_PENDING
		|| live->pcm_state != base->pcm_state)
		return CLUSTER_PCM_X_GRANT_RESERVATION_INVALID;

	if (base->pcm_state == (uint8)PCM_STATE_N && base->flags == 0
		&& base->reservation_token != UINT64_MAX
		&& reservation_token == base->reservation_token + 1)
		return CLUSTER_PCM_X_GRANT_RESERVATION_N_NEW;
	if (base->flags == PCM_OWN_FLAG_REVOKING && base->reservation_token != 0
		&& reservation_token == base->reservation_token) {
		if (base->pcm_state == (uint8)PCM_STATE_N)
			return CLUSTER_PCM_X_GRANT_RESERVATION_N_REVOKE_HANDOFF;
		if (base->pcm_state == (uint8)PCM_STATE_S)
			return CLUSTER_PCM_X_GRANT_RESERVATION_S_REVOKE_HANDOFF;
		if (base->pcm_state == (uint8)PCM_STATE_X)
			return CLUSTER_PCM_X_GRANT_RESERVATION_X_REVOKE_HANDOFF;
	}
	return CLUSTER_PCM_X_GRANT_RESERVATION_INVALID;
}

/* VM and FSM callers may consume a pinned buffer without taking its content
 * lock.  They therefore cannot coexist with the PI+BM_VALID retained shape:
 * an already-pinned or newly-pinned reader could otherwise observe source
 * bytes after ownership moved to N.  Main/init forks keep the passive-pin
 * tolerant retained path that breaks the S3 writer pin ring. */
typedef enum ClusterPcmXRevokeFinishMode {
	CLUSTER_PCM_X_REVOKE_FINISH_INVALID = 0,
	CLUSTER_PCM_X_REVOKE_FINISH_RETAIN,
	CLUSTER_PCM_X_REVOKE_FINISH_DROP,
	CLUSTER_PCM_X_REVOKE_FINISH_BUSY
} ClusterPcmXRevokeFinishMode;

static inline ClusterPcmXRevokeFinishMode
cluster_pcm_x_revoke_finish_mode(const BufferTag *tag, uint32 shared_refcount)
{
	if (tag == NULL || tag->forkNum < MAIN_FORKNUM || tag->forkNum > MAX_FORKNUM)
		return CLUSTER_PCM_X_REVOKE_FINISH_INVALID;
	if (tag->forkNum == FSM_FORKNUM || tag->forkNum == VISIBILITYMAP_FORKNUM)
		return shared_refcount == 0 ? CLUSTER_PCM_X_REVOKE_FINISH_DROP
									: CLUSTER_PCM_X_REVOKE_FINISH_BUSY;
	return CLUSTER_PCM_X_REVOKE_FINISH_RETAIN;
}

/* buffer_type is a monotone last-grant hint, not the live PCM authority.
 * X->S yield therefore legitimately leaves S+XCUR.  X acquisition always
 * publishes XCUR, so X+SCUR remains malformed.  N and PI/CURRENT/CR are never
 * shippable even when BM_VALID remains set. */
static inline bool
cluster_pcm_x_current_image_shape(uint8 pcm_state, uint8 buffer_type, bool valid)
{
	return valid
		   && ((pcm_state == (uint8)PCM_STATE_S
				&& (buffer_type == (uint8)BUF_TYPE_SCUR || buffer_type == (uint8)BUF_TYPE_XCUR))
			   || (pcm_state == (uint8)PCM_STATE_X && buffer_type == (uint8)BUF_TYPE_XCUR));
}

/* Token zero is valid before the first reservation; a completed reservation
 * leaves a nonzero monotonic token idle.  In both cases flags alone say
 * whether a lifecycle is currently active. */
static inline bool
cluster_pcm_own_eviction_reuse_allowed(const ClusterPcmOwnEvictionCapture *capture)
{
	return capture != NULL && capture->generation != UINT64_MAX && capture->flags == 0;
}

extern ClusterPcmOwnResult cluster_bufmgr_pcm_own_snapshot(BufferDesc *buf,
														   ClusterPcmOwnSnapshot *out_snapshot);
/* Resolve one resident descriptor and snapshot its ownership tuple while the
 * mapping partition and buffer header still bind the same BufferTag.  The
 * returned buffer id is only a locator; every later lifecycle call rechecks
 * the complete snapshot and therefore remains safe across descriptor reuse. */
extern ClusterPcmOwnResult
cluster_bufmgr_pcm_own_snapshot_by_tag(const BufferTag *tag, int *out_buffer_id,
									   ClusterPcmOwnSnapshot *out_snapshot);
/* Backstop direct content-lock mutation entrances that do not pass through
 * LockBuffer/W1.  GRANT_PENDING image installation is permitted; a live
 * source REVOKING lifecycle or retained PI+VALID descriptor is not. */
extern bool cluster_bufmgr_pcm_x_content_write_permitted(BufferDesc *buf);
/* Called only after the requester has proved the exact remote master's S->N
 * RELEASE application ACK.  Atomically normalizes the matching descriptor
 * tuple and returns the fresh N snapshot used by the later PREPARE leg. */
extern ClusterPcmOwnResult
cluster_bufmgr_pcm_own_finish_s_release_to_n(BufferDesc *buf,
											 const ClusterPcmOwnSnapshot *expected_s,
											 ClusterPcmOwnSnapshot *out_n_snapshot);
/* A queue INVALIDATE may target a MAIN/INIT S mirror that has passive PG
 * pins, including the waiting writer's own pin.  This by-tag boundary drains
 * content authority, snapshots/flushes its page evidence, and atomically
 * normalizes the exact S tuple to a clean BM_VALID N mirror.  VM/FSM remain
 * pin-intolerant because their readers may consume bytes without LockBuffer. */
extern ClusterPcmOwnResult cluster_bufmgr_pcm_own_release_pinned_s_for_gcs(const BufferTag *tag,
																		   XLogRecPtr *out_page_lsn,
																		   uint64 *out_page_scn);
/* Publish installed bytes only while content EXCLUSIVE and the exact
 * N+GRANT_PENDING reservation still bind this descriptor. */
extern ClusterPcmOwnResult cluster_bufmgr_pcm_own_publish_installed_x_image(
	BufferDesc *buf, const ClusterPcmOwnSnapshot *expected, uint64 reservation_token);
extern ClusterPcmOwnResult
cluster_bufmgr_pcm_own_begin_x_reservation(BufferDesc *buf, const ClusterPcmOwnSnapshot *expected,
										   uint64 *out_token);
/* Reclassify an exact requester-as-source N/S/X revoke without allocating a
 * second token or advancing generation.  The S-named entry remains as a
 * compatibility wrapper for callers that have not yet generalized. */
extern ClusterPcmOwnResult cluster_bufmgr_pcm_own_handoff_revoke_to_x_reservation(
	BufferDesc *buf, const ClusterPcmOwnSnapshot *expected_revoking, uint64 *out_token);
extern ClusterPcmOwnResult cluster_bufmgr_pcm_own_handoff_s_revoke_to_x_reservation(
	BufferDesc *buf, const ClusterPcmOwnSnapshot *expected_revoking, uint64 *out_token);
extern ClusterPcmOwnResult
cluster_bufmgr_pcm_own_finish_x_commit(BufferDesc *buf, const ClusterPcmOwnSnapshot *expected,
									   uint64 reservation_token, uint64 *out_committed_generation);
extern ClusterPcmOwnResult
cluster_bufmgr_pcm_own_abort_x_reservation(BufferDesc *buf, const ClusterPcmOwnSnapshot *expected,
										   uint64 reservation_token);

/* An ordinary remote source-holder revoke is separate from the requester
 * grant lifecycle, but uses the same ownership generation/token substrate.
 * Begin accepts passive PG pins: they are not PCM holders.  Finish is legal
 * only after the caller has staged the immutable source bytes identified by
 * expected_lsn.  It changes exact S/X to N and bumps generation once.  Main
 * and init forks keep a valid PI-shaped descriptor plus the exact REVOKING
 * token; the matching DRAIN releases that retained copy.  VM/FSM instead
 * require refcount zero under exclusive mapping authority and drop the
 * mapping at commit, because their pin-only readers do not take a content
 * lock and could otherwise observe stale retained bytes. */
extern ClusterPcmOwnResult
cluster_bufmgr_pcm_own_begin_x_revoke(BufferDesc *buf, const ClusterPcmOwnSnapshot *expected_x,
									  ClusterPcmOwnSnapshot *out_revoking);
extern ClusterPcmOwnResult
cluster_bufmgr_pcm_own_abort_x_revoke(BufferDesc *buf,
									  const ClusterPcmOwnSnapshot *expected_revoking);

/* Build an immutable requester-as-source image from a clean N descriptor and
 * shared storage.  Success leaves the exact same-generation REVOKING token
 * live, installs the verified scratch page into the resident descriptor, and
 * returns byte/LSN/SCN evidence from that one scratch image.  It never emits
 * protocol READY; the caller computes the GCS checksum and publishes READY
 * only after its dedup record is complete. */
extern ClusterPcmOwnResult cluster_bufmgr_pcm_own_prepare_n_source_image(
	BufferDesc *buf, const ClusterPcmOwnSnapshot *expected_n, ClusterPcmOwnSnapshot *out_revoking,
	char block_data[BLCKSZ], XLogRecPtr *out_page_lsn, uint64 *out_page_scn);
extern ClusterPcmOwnResult
cluster_bufmgr_pcm_own_abort_n_revoke(BufferDesc *buf,
									  const ClusterPcmOwnSnapshot *expected_revoking);

/* A shared source uses the same generation/token reservation and is never
 * promoted to, or represented as, X. */
extern ClusterPcmOwnResult
cluster_bufmgr_pcm_own_begin_s_revoke(BufferDesc *buf, const ClusterPcmOwnSnapshot *expected_s,
									  ClusterPcmOwnSnapshot *out_revoking);
extern ClusterPcmOwnResult
cluster_bufmgr_pcm_own_abort_s_revoke(BufferDesc *buf,
									  const ClusterPcmOwnSnapshot *expected_revoking);
extern ClusterPcmOwnResult cluster_bufmgr_pcm_own_finish_revoke_retain(
	BufferDesc *buf, const ClusterPcmOwnSnapshot *expected_revoking, XLogRecPtr expected_lsn,
	ClusterPcmOwnSnapshot *out_retained);
extern ClusterPcmOwnResult cluster_bufmgr_pcm_own_release_retained_image(const BufferTag *tag,
																		 uint64 source_generation);
/* Process-local descriptor evidence captured by the DRAIN-time probe below
 * so the caller can log the observed shape as diagnostics.  The fields are
 * one header-locked snapshot; never persisted or sent on wire. */
typedef struct ClusterPcmOwnSelfHandoffSample {
	uint64 live_generation;
	uint64 live_token;
	uint32 own_flags;
	uint8 pcm_state;
	uint8 buffer_type;
	bool bm_valid;
	bool buffer_found;
} ClusterPcmOwnSelfHandoffSample;

/* Read-only corruption probe for a delayed sole-requester source DRAIN.  The
 * release authority is the protocol completion certificate; the descriptor
 * may legitimately have moved on or been evicted, so this only reports a
 * structurally malformed flags/token shape (and never mutates anything). */
extern ClusterPcmOwnResult
cluster_bufmgr_pcm_own_self_handoff_probe(const BufferTag *tag,
										  ClusterPcmOwnSelfHandoffSample *sample_out);

#endif /* CLUSTER_PCM_X_BUFMGR_H */
