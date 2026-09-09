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

#include "access/transam.h"
#include "access/xlogdefs.h"
#include "cluster/cluster_pcm_own.h"
#include "cluster/cluster_pcm_lock.h"
#include "storage/buf_internals.h"

/* Compiler-visible identities consumed by the Stage-8 closed-world AST gate.
 * Non-Clang product builds keep identical behavior; certification itself
 * requires a compiler that exposes annotate attributes in its AST. */
#if __has_attribute(annotate)
#define PGRAC_PCM_X_FENCE_TERMINAL_OWNER(kind, parameter, proof)                                   \
	__attribute__((annotate("pgrac_pcm_x_terminal_owner:" #kind ":" #parameter ":" #proof)))
#define PGRAC_PCM_X_FENCE_DOMINATED(proof)                                                         \
	__attribute__((annotate("pgrac_pcm_x_fence_dominated:" #proof)))
#else
#define PGRAC_PCM_X_FENCE_TERMINAL_OWNER(kind, parameter, proof)
#define PGRAC_PCM_X_FENCE_DOMINATED(proof)
#endif

/* Keep the relation-class decision shared by SMGR-adjacent heap gates and
 * BufferTag PCM tracking.  With shared_catalog off, catalog/system relations
 * remain node-local and cannot have a real Resource-X ownership episode. */
static inline bool
cluster_pcm_x_relation_number_tracked(RelFileNumber rel_number, bool shared_catalog)
{
	return shared_catalog || rel_number >= (RelFileNumber)FirstNormalObjectId;
}

/* FSM pages are advisory and may be consumed without a content lock, so they
 * are outside PCM/PCM-X ownership.  Every other fork keeps the relation-level
 * user/shared-catalog tracking policy. */
static inline bool
cluster_pcm_x_buffer_tag_tracked(const BufferTag *tag, bool shared_catalog)
{
	if (tag == NULL || tag->forkNum == FSM_FORKNUM)
		return false;
	return cluster_pcm_x_relation_number_tracked(tag->relNumber, shared_catalog);
}

/* Only authority/image-shape bits belong in coherent observation.  Pin count,
 * usage count, waiter and header-lock bookkeeping must not force RESAMPLE. */
#define CLUSTER_PCM_OWN_SEMANTIC_BUF_STATE_MASK                                                    \
	(BM_TAG_VALID | BM_VALID | BM_DIRTY | BM_JUST_DIRTIED | BM_CHECKPOINT_NEEDED                   \
	 | BM_IO_IN_PROGRESS | BM_IO_ERROR)

typedef struct ClusterPcmOwnSnapshot {
	BufferTag tag;
	uint32 semantic_buf_state;
	uint64 generation;
	uint64 reservation_token;
	/* Diagnostic projection of the grant->content activation fence.  It is
	 * sampled under the same BufferDesc header lock as the authoritative
	 * ownership tuple and participates in local coherent-observation equality,
	 * but is not wire state or an independent authority. */
	uint64 writer_activation_token;
	uint64 resource_x_activation_generation;
	uint32 flags;
	uint8 pcm_state;
	uint8 buffer_type;
	uint8 _reserved[2];
} ClusterPcmOwnSnapshot;

/* Local descriptor residency only.  The Resource-X entry owns the attempt
 * binding; these wrappers prove tag/generation under the header lock. */
extern ClusterPcmOwnResult cluster_bufmgr_pcm_own_delivery_hold_begin_exact(
	BufferDesc *buf, const ClusterPcmOwnSnapshot *expected_n, uint64 delivery_attempt);
extern ClusterPcmOwnResult
cluster_bufmgr_pcm_own_delivery_snapshot_exact(BufferDesc *buf, const BufferTag *tag,
											   uint64 delivery_attempt,
											   ClusterPcmOwnSnapshot *snapshot_out);
extern ClusterPcmOwnResult cluster_bufmgr_pcm_own_delivery_hold_release_exact(
	BufferDesc *buf, const ClusterPcmOwnSnapshot *terminal_x, uint64 delivery_attempt,
	uint64 original_install_token);

StaticAssertDecl(sizeof(ClusterPcmOwnSnapshot) == 64, "ClusterPcmOwnSnapshot must remain 64 bytes");
StaticAssertDecl(offsetof(ClusterPcmOwnSnapshot, semantic_buf_state) == 20,
				 "semantic buffer state must use former padding");
StaticAssertDecl(offsetof(ClusterPcmOwnSnapshot, buffer_type) == 61,
				 "buffer type must use a former reserved byte");

/* Both operands come from a constructor that clears the complete object
 * before assigning its fields.  Whole-object equality therefore includes
 * BufferTag padding and reserved bytes as future-proof observation axes. */
static inline bool
cluster_pcm_own_snapshot_equal_exact(const ClusterPcmOwnSnapshot *left,
									 const ClusterPcmOwnSnapshot *right)
{
	return left != NULL && right != NULL && memcmp(left, right, sizeof(*left)) == 0;
}

/* Revalidate an already-owned operation fence across ordinary image/I/O
 * transitions.  This is not coherent-observation equality or image validity:
 * the owner must still check the current image under its required locks.
 * Copy the complete initialized object so every identity/reserved byte is
 * compared; only these two physical-image fields are outside fence identity. */
static inline bool
cluster_pcm_own_fence_equal_exact(const ClusterPcmOwnSnapshot *left,
								  const ClusterPcmOwnSnapshot *right)
{
	ClusterPcmOwnSnapshot left_fence;
	ClusterPcmOwnSnapshot right_fence;

	if (left == NULL || right == NULL)
		return false;
	memcpy(&left_fence, left, sizeof(left_fence));
	memcpy(&right_fence, right, sizeof(right_fence));
	left_fence.semantic_buf_state = right_fence.semantic_buf_state = 0;
	left_fence.buffer_type = right_fence.buffer_type = 0;
	return cluster_pcm_own_snapshot_equal_exact(&left_fence, &right_fence);
}

/* Resolve one raw entry result only after the caller has obtained both
 * BufferDesc snapshots.  A changed projection wins over every raw state;
 * neither authority-bearing output may escape that observation window. */
static inline ResourceXTargetInstallFollowState
cluster_pcm_x_target_install_follow_adjudicate_exact(
	const ClusterPcmOwnSnapshot *before, const ClusterPcmOwnSnapshot *after,
	ResourceXTargetInstallFollowState raw_state, ResourceXAcquisitionRef *terminal_ref,
	ResourceXTargetInstallContinuation *new_continuation)
{
	ResourceXTargetInstallFollowState result = raw_state;
	bool retain_continuation;

	if (before == NULL || after == NULL)
		result = RESOURCE_X_TARGET_INSTALL_INVALID;
	else if (!cluster_pcm_own_snapshot_equal_exact(before, after))
		result = RESOURCE_X_TARGET_INSTALL_RESAMPLE;
	else if (raw_state < RESOURCE_X_TARGET_INSTALL_INVALID
			 || raw_state > RESOURCE_X_TARGET_INSTALL_RESAMPLE)
		result = RESOURCE_X_TARGET_INSTALL_INVALID;

	if (result != RESOURCE_X_TARGET_INSTALL_TERMINAL && terminal_ref != NULL)
		memset(terminal_ref, 0, sizeof(*terminal_ref));
	retain_continuation = result == RESOURCE_X_TARGET_INSTALL_INFLIGHT
						  || result == RESOURCE_X_TARGET_INSTALL_TERMINAL
						  || result == RESOURCE_X_TARGET_INSTALL_PREUSE_RETRY;
	if (!retain_continuation && new_continuation != NULL)
		memset(new_continuation, 0, sizeof(*new_continuation));
	return result;
}

/* A27 terminal-census admission is intentionally tri-state.  A valid
 * Resource-X HANDOFF/REVOKING collision is caller-owned requalification, not
 * physical ITL exhaustion and not a successfully armed recycle owner. */
typedef enum ClusterBufmgrItlRecycleGuardResult {
	CLUSTER_BUFMGR_ITL_RECYCLE_REFUSED = 0,
	CLUSTER_BUFMGR_ITL_RECYCLE_ARMED,
	CLUSTER_BUFMGR_ITL_RECYCLE_RETRY_REQUALIFY
} ClusterBufmgrItlRecycleGuardResult;

/* A remote-S type-17 can race a local grant installation before the local
 * descriptor has committed its S tuple.  This exact N+GRANT_PENDING shape is
 * coherent but is not holder evidence: the caller must make zero mutation
 * and let the existing Resource-X retry deadline resample it.  Malformed
 * reservation/activation evidence remains a hard failure. */
static inline ClusterPcmOwnResult
cluster_pcm_x_remote_s_holder_pending_grant_result(const ClusterPcmOwnSnapshot *snapshot)
{
	if (snapshot == NULL || snapshot->pcm_state != (uint8)PCM_STATE_N
		|| snapshot->flags != PCM_OWN_FLAG_GRANT_PENDING)
		return CLUSTER_PCM_OWN_INVALID;
	if (snapshot->generation == UINT64_MAX || snapshot->reservation_token == 0
		|| snapshot->reservation_token == UINT64_MAX || snapshot->writer_activation_token != 0
		|| snapshot->resource_x_activation_generation != 0)
		return CLUSTER_PCM_OWN_CORRUPT;
	return CLUSTER_PCM_OWN_BUSY;
}

/* An authenticated type-17 may be replayed after its first exact S->N has
 * already published the retained status into reliable DATA transport.  This
 * predicate recognizes only the resulting idle, post-transition N tuple; it
 * does not manufacture attempt identity or grant authority.  A coherent live
 * reservation is ordinary backpressure, while residual/illegal tuple axes
 * remain corruption. */
static inline ClusterPcmOwnResult
cluster_pcm_x_remote_s_holder_stable_n_result(const ClusterPcmOwnSnapshot *snapshot)
{
	ClusterPcmOwnResult live_result;

	if (snapshot == NULL || snapshot->pcm_state != (uint8)PCM_STATE_N)
		return CLUSTER_PCM_OWN_INVALID;
	if (snapshot->generation == 0)
		return CLUSTER_PCM_OWN_STALE;
	if (snapshot->generation == UINT64_MAX)
		return CLUSTER_PCM_OWN_CORRUPT;
	live_result = cluster_pcm_own_classify_live_flags(snapshot->flags, snapshot->reservation_token);
	if (live_result != CLUSTER_PCM_OWN_OK)
		return live_result;
	/* Every successful S->N revoke keeps its exact reservation token as a
	 * monotonic ABA fence after clearing REVOKING.  Zero therefore cannot
	 * identify this replay shape, while a finite nonzero idle token does. */
	if (snapshot->reservation_token == 0)
		return CLUSTER_PCM_OWN_STALE;
	if (snapshot->reservation_token == UINT64_MAX || snapshot->writer_activation_token != 0
		|| snapshot->resource_x_activation_generation != 0)
		return CLUSTER_PCM_OWN_CORRUPT;
	return CLUSTER_PCM_OWN_OK;
}

#define CLUSTER_PCM_OWN_HELD_X_REVOKE_PIN_HELD UINT32_C(0x00000001)
#define CLUSTER_PCM_OWN_HELD_X_REVOKE_STARTED UINT32_C(0x00000002)
#define CLUSTER_PCM_OWN_HELD_X_REVOKE_KNOWN_MASK                                                   \
	(CLUSTER_PCM_OWN_HELD_X_REVOKE_PIN_HELD | CLUSTER_PCM_OWN_HELD_X_REVOKE_STARTED)

/* Opaque process-local holder for the post-L3 X-source path.  The raw pin is
 * acquired under mapping authority before REVOKING begins and remains live
 * through copy, pair retention, and the finish attempt.  It is never copied
 * to shared memory, wire, WAL, or Resource-X identity. */
typedef struct ClusterPcmOwnHeldXRevoke {
	ClusterPcmOwnSnapshot revoking;
	int32 buffer_id;
	uint32 flags;
} ClusterPcmOwnHeldXRevoke;

StaticAssertDecl(sizeof(ClusterPcmOwnHeldXRevoke) == 72,
				 "ClusterPcmOwnHeldXRevoke layout must remain 72 bytes");

typedef struct ResourceXCurrentImage {
	const char *page_bytes;
	XLogRecPtr page_lsn;
	SCN page_scn;
	uint32 page_checksum;
	uint32 image_length;
} ResourceXCurrentImage;

typedef enum ResourceXBufferActivationResult {
	RESOURCE_X_BUFFER_T2_INSTALLED = 0,
	RESOURCE_X_BUFFER_ALREADY_INSTALLED,
	RESOURCE_X_BUFFER_BUSY,
	RESOURCE_X_BUFFER_ABSENT,
	RESOURCE_X_BUFFER_STALE,
	RESOURCE_X_BUFFER_CORRUPT
} ResourceXBufferActivationResult;

typedef enum ResourceXSidecarNeutralizeResult {
	RESOURCE_X_SIDECAR_NEUTRALIZED = 0,
	RESOURCE_X_SIDECAR_ALREADY_CLEAR,
	RESOURCE_X_SIDECAR_SUCCESSOR,
	RESOURCE_X_SIDECAR_BUSY,
	RESOURCE_X_SIDECAR_STALE,
	RESOURCE_X_SIDECAR_CORRUPT
} ResourceXSidecarNeutralizeResult;

StaticAssertDecl(sizeof(ResourceXCurrentImage) == 32,
				 "ResourceXCurrentImage process-local layout must remain 32 bytes");

static inline bool
cluster_pcm_x_resource_x_t2_snapshot_exact(const ResourceXAcquisitionRef *ref,
										   const ClusterPcmOwnSnapshot *live)
{
	return ref != NULL && live != NULL && ref->formation != 0 && ref->acquisition_generation != 0
		   && BufferTagsEqual(&ref->assertion.resource, &live->tag)
		   && live->pcm_state == (uint8)PCM_STATE_X && live->flags == 0
		   && live->reservation_token != 0
		   && live->writer_activation_token == live->reservation_token
		   && (live->resource_x_activation_generation == 0
			   || live->resource_x_activation_generation == ref->acquisition_generation);
}

static inline bool
cluster_pcm_x_resource_x_t3_snapshot_exact(const ResourceXAcquisitionRef *ref,
										   const ClusterPcmOwnSnapshot *live)
{
	return cluster_pcm_x_resource_x_t2_snapshot_exact(ref, live)
		   && live->resource_x_activation_generation == ref->acquisition_generation;
}

/* The physical claim survives image installation.  This shape is not a
 * grant: callers must separately hold the exact joined image/ref and claim.
 * It only prevents a replay from treating installed G+1 as a new base G. */
static inline bool
cluster_pcm_x_resource_x_claim_installed_exact(const ResourceXAcquisitionRef *ref,
											   const ClusterPcmOwnSnapshot *live,
											   uint64 pending_generation, uint64 reservation_token)
{
	return pending_generation < UINT64_MAX - 1 && reservation_token != 0
		   && reservation_token != UINT64_MAX
		   && cluster_pcm_x_resource_x_t2_snapshot_exact(ref, live)
		   && live->generation == pending_generation + 1
		   && live->reservation_token == reservation_token;
}

static inline bool
cluster_pcm_x_resource_x_r8_snapshot_exact(const BufferTag *tag, uint64 old_formation,
										   uint64 acquisition_generation,
										   const ClusterPcmOwnSnapshot *live)
{
	return tag != NULL && live != NULL && old_formation != 0 && old_formation != UINT64_MAX
		   && acquisition_generation != 0 && acquisition_generation != UINT64_MAX
		   && BufferTagsEqual(tag, &live->tag) && live->pcm_state == (uint8)PCM_STATE_X
		   && live->flags == 0 && live->reservation_token != 0
		   && live->writer_activation_token == live->reservation_token
		   && live->resource_x_activation_generation == acquisition_generation;
}

/*
 * Process-local evidence for a reversible finish-revoke refusal.  The DATA
 * worker fills this from the same header-lock sample that chose BUSY, so GCS
 * can correlate the exact ticket/image with the actual VM/FSM pin, I/O, or
 * ownership-lifecycle branch.  This is diagnostics only; it is never sent on
 * wire or persisted.
 */
typedef enum ClusterPcmOwnFinishRefusalReason {
	CLUSTER_PCM_OWN_FINISH_REFUSAL_NONE = 0,
	CLUSTER_PCM_OWN_FINISH_REFUSAL_VM_FSM_PINNED,
	CLUSTER_PCM_OWN_FINISH_REFUSAL_IO_IN_PROGRESS,
	CLUSTER_PCM_OWN_FINISH_REFUSAL_LIVE_FLAGS,
	CLUSTER_PCM_OWN_FINISH_REFUSAL_CONTENT_LOCK
} ClusterPcmOwnFinishRefusalReason;

typedef struct ClusterPcmOwnFinishRefusal {
	uint64 live_token;
	uint32 shared_refcount;
	uint32 live_flags;
	ClusterPcmOwnFinishRefusalReason reason;
	bool bm_io_in_progress;
} ClusterPcmOwnFinishRefusal;

/* Process-local reason for a retryable S-source image preparation refusal.
 * DIRTY_FLUSHED records forward progress; the other values identify the
 * exact nonblocking gate that asked the image pump to retry. */
typedef enum ClusterPcmOwnSourcePrepareRefusal {
	CLUSTER_PCM_OWN_SOURCE_PREPARE_REFUSAL_NONE = 0,
	CLUSTER_PCM_OWN_SOURCE_PREPARE_REFUSAL_BEGIN_REVOKE,
	CLUSTER_PCM_OWN_SOURCE_PREPARE_REFUSAL_CONTENT_LOCK,
	CLUSTER_PCM_OWN_SOURCE_PREPARE_REFUSAL_DIRTY_FLUSHED,
	CLUSTER_PCM_OWN_SOURCE_PREPARE_REFUSAL_DIRTY_RACED,
	CLUSTER_PCM_OWN_SOURCE_PREPARE_REFUSAL_IO_IN_PROGRESS
} ClusterPcmOwnSourcePrepareRefusal;

/* Process-local disposition returned by the header-locked grant-begin
 * classifier.  READ_IMAGE is a node-wide one-shot content bracket, not a
 * cached S cover and not a live grant reservation; keeping it distinct lets
 * the waiter obtain content-X before attempting successor-safe reclamation. */
typedef enum ClusterPcmGrantBeginWaitReason {
	CLUSTER_PCM_GRANT_WAIT_NONE = 0,
	CLUSTER_PCM_GRANT_WAIT_LIVE_RESERVATION,
	CLUSTER_PCM_GRANT_WAIT_READ_IMAGE_BRACKET,
	CLUSTER_PCM_GRANT_WAIT_RESOURCE_X_BARRIER
} ClusterPcmGrantBeginWaitReason;

/* Pure half of the header-locked begin decision.  INVALID means that the
 * sampled tuple is not READ_IMAGE and the caller should continue through the
 * ordinary cover/reservation classifier.  Every other result is terminal for
 * this sample and must occur before reservation_begin_exact. */
static inline ClusterPcmOwnResult
cluster_pcm_x_read_image_begin_disposition(const ClusterPcmOwnSnapshot *live, uint32 buf_state,
										   uint8 buffer_type,
										   ClusterPcmGrantBeginWaitReason *wait_reason)
{
	if (wait_reason == NULL)
		return CLUSTER_PCM_OWN_INVALID;
	*wait_reason = CLUSTER_PCM_GRANT_WAIT_NONE;
	if (live == NULL || live->pcm_state != (uint8)PCM_STATE_READ_IMAGE)
		return CLUSTER_PCM_OWN_INVALID;
	if (live->generation == UINT64_MAX || live->reservation_token == UINT64_MAX)
		return CLUSTER_PCM_OWN_EXHAUSTED;
	if (live->generation == 0 || live->reservation_token == 0 || live->flags != 0
		|| live->writer_activation_token != 0 || live->resource_x_activation_generation != 0
		|| (buf_state & BM_VALID) == 0 || (buf_state & BM_IO_IN_PROGRESS) != 0
		|| buffer_type != (uint8)BUF_TYPE_CURRENT)
		return CLUSTER_PCM_OWN_CORRUPT;
	*wait_reason = CLUSTER_PCM_GRANT_WAIT_READ_IMAGE_BRACKET;
	return CLUSTER_PCM_OWN_BUSY;
}

/* A tracked current-X page is not an ordinary writable entrance until both
 * the legacy grant->content activation and Resource-X T2->T3 activation have
 * opened.  Every fast/conditional/direct write gate delegates to this one
 * predicate so a newly added entrance cannot accidentally check only one
 * sidecar field. */
static inline bool
cluster_pcm_x_activation_fence_open(uint64 writer_activation_token,
									uint64 resource_x_activation_generation)
{
	return writer_activation_token == 0 && resource_x_activation_generation == 0;
}

/* VM/FSM readers may consume bytes under a pin alone.  Once an exact revoke
 * is published, a first passive pin must therefore serialize with that
 * publication under the BufferDesc header lock and retry without changing
 * refcount.  Existing pins predate the fence and are drained separately
 * before retained intent; no new ownership state is introduced here. */
static inline bool
cluster_pcm_x_aux_pin_admission_allowed(bool runtime_active, bool tracked, ForkNumber forknum,
										uint32 flags)
{
	bool auxiliary = forknum == VISIBILITYMAP_FORKNUM || forknum == FSM_FORKNUM;

	return !runtime_active || !tracked || !auxiliary || (flags & PCM_OWN_FLAG_REVOKING) == 0;
}

/* A committed node X is cache residency authority, not a fresh writer
 * conversion.  Consult the coherent ownership tuple before JOIN so repeated
 * local LockBuffer(X) calls keep the spec-4.7a hold-until-revoked fast path.
 * Any transition flag forces the caller through normal arbitration/recheck. */
static inline bool
cluster_pcm_x_cached_cover_bypasses_queue(bool local_cache, bool requested_x, uint8 pcm_state,
										  uint32 flags, uint64 writer_activation_token,
										  uint64 resource_x_activation_generation)
{
	return local_cache && requested_x && pcm_state == (uint8)PCM_STATE_X && flags == 0
		   && cluster_pcm_x_activation_fence_open(writer_activation_token,
												  resource_x_activation_generation);
}

/* Revalidate a cached cover after the caller has acquired content authority.
 * A stable current S/X is the node-level authority for an S read even when a
 * complete ownership round advanced the generation while this backend waited:
 * the page-image install/transition is serialized by the content lock already
 * held by the caller.  Treating that successor S as stale would open a fresh
 * legacy reservation from S (the forbidden S_NEW shape).  X writers retain
 * the stricter generation-exact rule and re-enter the convert queue after any
 * ownership round. */
static inline bool
cluster_pcm_x_cached_cover_reverify_accepts(uint8 requested_state, uint64 captured_generation,
											uint64 current_generation, uint8 current_state,
											uint32 current_flags, uint64 writer_activation_token,
											uint64 resource_x_activation_generation)
{
	bool covers;

	if (requested_state != (uint8)PCM_STATE_S && requested_state != (uint8)PCM_STATE_X)
		return false;
	if (current_flags != 0
		|| !cluster_pcm_x_activation_fence_open(writer_activation_token,
												resource_x_activation_generation))
		return false;
	covers = current_state == (uint8)PCM_STATE_X
			 || (requested_state == (uint8)PCM_STATE_S && current_state == (uint8)PCM_STATE_S);
	return covers
		   && (requested_state == (uint8)PCM_STATE_S || current_generation == captured_generation);
}

/* A TARGET grant can be revoked after T3 but before this backend returns
 * writable content authority.  Re-probing is safe only while the caller has
 * not exposed that content interval and the enclosing R4/gate/tag domain is
 * unchanged.  This predicate grants nothing: it merely validates that the
 * fresh BufferDesc tuple is a legal stable or in-flight ownership shape that
 * the ordinary Resource-X acquire may classify again under the same absolute
 * deadline.  Corrupt tuples and identity drift retain their fail-closed
 * polarity. */
static inline bool
cluster_pcm_x_target_preuse_drift_retryable(const ClusterPcmOwnSnapshot *live,
											bool target_path_current, bool gate_current,
											bool tag_current)
{
	ClusterPcmOwnResult shape;

	if (live == NULL || !target_path_current || !gate_current || !tag_current
		|| live->generation == 0 || live->generation == UINT64_MAX
		|| live->reservation_token == UINT64_MAX || live->writer_activation_token == UINT64_MAX
		|| live->resource_x_activation_generation == UINT64_MAX
		|| (live->pcm_state != (uint8)PCM_STATE_N && live->pcm_state != (uint8)PCM_STATE_S
			&& live->pcm_state != (uint8)PCM_STATE_X))
		return false;
	shape = cluster_pcm_own_classify_live_flags(live->flags, live->reservation_token);
	return shape == CLUSTER_PCM_OWN_OK || shape == CLUSTER_PCM_OWN_BUSY;
}

/* ConditionalLockBuffer cannot initiate a PCM conversion.  Preserve native
 * PostgreSQL behavior while PCM is inactive and for relations outside the
 * coherence domain; an active tracked page must already hold exact X.  Live
 * transition/retained evidence remains closed regardless of runtime state. */
static inline bool
cluster_pcm_x_ordinary_mutation_allowed(bool runtime_active, bool tracked, bool retained_image,
										uint8 pcm_state, uint32 flags,
										uint64 writer_activation_token,
										uint64 resource_x_activation_generation)
{
	return !retained_image && flags == 0
		   && (!runtime_active || !tracked
			   || (pcm_state == (uint8)PCM_STATE_X
				   && cluster_pcm_x_activation_fence_open(writer_activation_token,
														  resource_x_activation_generation)));
}

/* A type-17 pre-retention drain first marks the exact current X REVOKING and
 * then probes BufferContent with a conditional exclusive acquire.  A caller
 * that already owns BufferContent predates that fence and must be allowed to
 * finish its current mutation; its lock makes the drain return BUSY, after
 * which the event-loop path exact-aborts REVOKING before replay.  New content
 * entrants continue to use ordinary_mutation_allowed() and therefore cannot
 * cross the fence.  Retained images and activation-sidecar debt are never
 * covered by this narrow predecessor exception. */
static inline bool
cluster_pcm_x_content_holder_mutation_allowed(bool runtime_active, bool tracked,
											  bool retained_image, uint8 pcm_state, uint32 flags,
											  uint64 writer_activation_token,
											  uint64 resource_x_activation_generation)
{
	return cluster_pcm_x_ordinary_mutation_allowed(runtime_active, tracked, retained_image,
												   pcm_state, flags, writer_activation_token,
												   resource_x_activation_generation)
		   || (runtime_active && tracked && !retained_image && pcm_state == (uint8)PCM_STATE_X
			   && flags == PCM_OWN_FLAG_REVOKING
			   && cluster_pcm_x_activation_fence_open(writer_activation_token,
													  resource_x_activation_generation));
}

/* A27: BufferDesc REVOKING is a reversible drain fence, not a second
 * current-block authority.  A backend that continuously owns content-X may
 * therefore see the fence published or exact-aborted while it finishes the
 * already-admitted mutation.  Reservation tokens are lifecycle identities and
 * may advance across those attempts; tag, X ownership generation and both
 * activation sidecars remain strict.  This predicate grants no write access:
 * the caller must independently prove that it retained content-X throughout. */
static inline bool
cluster_pcm_x_content_holder_dml_authority_equivalent(const ClusterPcmOwnSnapshot *captured,
													  const ClusterPcmOwnSnapshot *live)
{
	bool captured_lifecycle_valid;
	bool live_lifecycle_valid;

	if (captured == NULL || live == NULL || !BufferTagsEqual(&captured->tag, &live->tag)
		|| captured->generation == 0 || captured->generation == UINT64_MAX
		|| live->generation != captured->generation || captured->pcm_state != (uint8)PCM_STATE_X
		|| live->pcm_state != (uint8)PCM_STATE_X || captured->writer_activation_token != 0
		|| live->writer_activation_token != 0 || captured->resource_x_activation_generation != 0
		|| live->resource_x_activation_generation != 0)
		return false;
	captured_lifecycle_valid
		= (captured->flags == 0 && captured->reservation_token != UINT64_MAX)
		  || (captured->flags == PCM_OWN_FLAG_REVOKING && captured->reservation_token != 0
			  && captured->reservation_token != UINT64_MAX);
	live_lifecycle_valid = (live->flags == 0 && live->reservation_token != UINT64_MAX)
						   || (live->flags == PCM_OWN_FLAG_REVOKING && live->reservation_token != 0
							   && live->reservation_token != UINT64_MAX);
	return captured_lifecycle_valid && live_lifecycle_valid;
}

static inline bool
cluster_pcm_x_conditional_lock_allowed(bool runtime_active, bool tracked, bool retained_image,
									   uint8 pcm_state, uint32 flags,
									   uint64 writer_activation_token,
									   uint64 resource_x_activation_generation)
{
	return cluster_pcm_x_ordinary_mutation_allowed(runtime_active, tracked, retained_image,
												   pcm_state, flags, writer_activation_token,
												   resource_x_activation_generation);
}

static inline bool
cluster_pcm_x_flush_fence_consistent(bool dirty, uint64 writer_activation_token,
									 uint64 resource_x_activation_generation)
{
	return !dirty
		   || cluster_pcm_x_activation_fence_open(writer_activation_token,
												  resource_x_activation_generation);
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
 * the single grant commit.
 *
 * A fresh-token S base (an S holder entering the legacy master acquire, the
 * loop9/loop10b "S_NEW" tuple) is deliberately NOT a legal finish shape:
 * writer conversions are ordered by the convert queue's FIFO/WFG, and
 * legalizing the legacy S-base short cut here would let a stale-cover
 * fallback bypass that arbitration entirely (the original S3 unordered
 * multi-writer defect).  The stale-cover path must re-enter the queue
 * instead; this classifier keeps refusing the bypass.  A fresh-token X base
 * is equally unenumerated: a live X cover never re-acquires through this
 * path (cluster_pcm_x_cached_cover_bypasses_queue). */
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

/* A requester-local N descriptor contributes no page proof to ASSERT_X.
 * CURRENT is the ordinary clean-N residency; PI+BM_VALID is the passive
 * retained mirror kept byte-exact under a pre-existing pin after exact
 * DRAIN.  Both may identify the requester as a non-holder, but neither is
 * read or shipped here.  All other descriptor shapes remain closed. */
static inline ClusterPcmOwnResult
cluster_pcm_x_n_assertion_shape(uint8 pcm_state, uint8 buffer_type, uint32 buf_state)
{
	if (pcm_state != (uint8)PCM_STATE_N)
		return CLUSTER_PCM_OWN_STALE;
	if ((buf_state & BM_VALID) == 0 || (buf_state & BM_IO_ERROR) != 0)
		return CLUSTER_PCM_OWN_CORRUPT;
	if (buffer_type != (uint8)BUF_TYPE_CURRENT && buffer_type != (uint8)BUF_TYPE_PI)
		return CLUSTER_PCM_OWN_CORRUPT;
	if ((buf_state & (BM_DIRTY | BM_JUST_DIRTIED | BM_CHECKPOINT_NEEDED | BM_IO_IN_PROGRESS)) != 0)
		return CLUSTER_PCM_OWN_BUSY;
	return CLUSTER_PCM_OWN_OK;
}

/* Post-release retag of a retained PI whose exact write-fence token was just
 * released, applied under the same header-lock hold.  With no pins the image
 * is dropped -- !BM_VALID plus a BUF_TYPE_CURRENT retag makes the next
 * ordinary read reload the current page bytes.  With a pre-existing pin the
 * frozen bytes must neither vanish nor change under the pin holder
 * (PostgreSQL pin contract): keep the established PI+BM_VALID
 * never-write/never-serve N mirror (the passive-pin invalidate release
 * shape).  The next S acquire begins an exact GRANT_PENDING install that may
 * overwrite it and republish CURRENT, an X convert rides the convert queue,
 * and eviction retags once the last pin drains.  Returns true when the image
 * was dropped. */
static inline bool
cluster_pcm_x_retained_release_retag(uint8 *buffer_type, uint32 *buf_state)
{
	if (BUF_STATE_GET_REFCOUNT(*buf_state) == 0) {
		*buf_state &= ~BM_VALID;
		*buffer_type = (uint8)BUF_TYPE_CURRENT;
		return true;
	}
	return false;
}

/* A frozen PI mirror kept for pre-existing pins regains CURRENT only while
 * the exact legacy grant lifecycle that just proved its bytes (a shipped
 * install, a storage refresh, or an SCN PASS proof) is still open: pcm N +
 * live GRANT_PENDING + nonzero token + BM_VALID + PI.  Every other shape
 * stays frozen so the grant-finish valid-image gate keeps failing closed on
 * an unproven stale cover. */
static inline bool
cluster_pcm_x_grant_pending_republish_shape(uint8 pcm_state, uint32 flags, uint64 token, bool valid,
											uint8 buffer_type)
{
	return pcm_state == (uint8)PCM_STATE_N && flags == PCM_OWN_FLAG_GRANT_PENDING && token != 0
		   && valid && buffer_type == (uint8)BUF_TYPE_PI;
}

/* Token zero is valid before the first reservation; a completed reservation
 * leaves a nonzero monotonic token idle.  In both cases flags alone say
 * whether a lifecycle is currently active. */
static inline bool
cluster_pcm_own_eviction_reuse_allowed(const ClusterPcmOwnEvictionCapture *capture)
{
	return capture != NULL && capture->generation != UINT64_MAX && capture->flags == 0
		   && capture->writer_activation_token == 0
		   && capture->resource_x_activation_generation == 0;
}

extern ClusterPcmOwnResult cluster_bufmgr_pcm_own_snapshot(BufferDesc *buf,
														   ClusterPcmOwnSnapshot *out_snapshot);
/* A non-durable S acquisition publishes a node-exclusive read bracket by
 * consuming its exact N+GRANT_PENDING reservation and changing pcm_state
 * under the same BufferDesc header-lock hold.  Normal release and abandoned
 * recovery both clear only the exact published snapshot and bump generation
 * once; the recovery form additionally requires content EXCLUSIVE. */
extern ClusterPcmOwnResult
cluster_pcm_own_publish_read_image_exact(BufferDesc *buf, const ClusterPcmOwnSnapshot *base,
										 uint64 reservation_token,
										 uint64 *published_generation_out);
extern ClusterPcmOwnResult cluster_pcm_own_release_read_image_exact(BufferDesc *buf,
																	uint64 *cleared_generation_out);
extern ClusterPcmOwnResult
cluster_pcm_own_reclaim_read_image_exact(BufferDesc *buf, const ClusterPcmOwnSnapshot *published,
										 uint64 *cleared_generation_out);
/* Exact requester-side N assertion preflight.  A passive retained PI mirror
 * is accepted only as a no-local-current shape; no page bytes or proof are
 * returned, and the master still owns proof selection. */
extern ClusterPcmOwnResult cluster_bufmgr_pcm_own_n_assertion_candidate_exact(
	BufferDesc *buf, const ClusterPcmOwnSnapshot *expected_n, ClusterPcmOwnSnapshot *observed_out);
/* Exact physical half of the former-source reacquire interlock.  It proves
 * only a clean, valid N+PI descriptor held by one REVOKING token; the caller
 * must independently bind that generation to the retained Resource-X pair. */
extern bool
cluster_bufmgr_pcm_own_n_retained_release_inflight_exact(BufferDesc *buf,
														 const ClusterPcmOwnSnapshot *expected_n);
/* A passive N descriptor is eligible for a durable-storage assertion only
 * while the exact ownership tuple still names clean, valid, non-IO CURRENT
 * residency.  The page bytes are deliberately not returned as proof. */
extern ClusterPcmOwnResult
cluster_bufmgr_pcm_own_n_storage_candidate_exact(BufferDesc *buf,
												 const ClusterPcmOwnSnapshot *expected_n);
/* The only TARGET durable-proof exception to the ordinary BM_VALID/no-IO N
 * preflight.  An exact direct-init proof has already published this existing
 * N_NEW reservation before any Resource-X frame; T2 revalidates its known-new
 * shared descriptor shape without treating page bytes as proof. */
extern ClusterPcmOwnResult cluster_bufmgr_pcm_own_n_direct_init_candidate_exact(
	BufferDesc *buf, const ClusterPcmOwnSnapshot *expected_pending_n);
/* Current-slice remote-S holder evidence.  This is a local BufferDesc
 * predicate only: it neither creates nor consults a GRD/Resource-X authority
 * record.  The caller holds content EXCLUSIVE across its final check and the
 * atomic retained-ring publication that commits S->N. */
extern ClusterPcmOwnResult
cluster_bufmgr_pcm_own_s_holder_candidate_exact(BufferDesc *buf,
												const ClusterPcmOwnSnapshot *expected_s);
/* Read one verified shared-storage page into actor-private aligned staging.
 * This function takes no BufferDesc pin/content lock and publishes no
 * ownership; callers bind the result with Resource-X snapshot/revalidation. */
extern bool cluster_bufmgr_read_storage_image_for_resource_x(BufferTag tag, char block_data[BLCKSZ],
															 XLogRecPtr *out_page_lsn,
															 uint64 *out_page_scn);
/* Same-page terminal-census slow path.  D11 retains the exact off-lock
 * occupancy in the existing per-resource Resource-X entry, not the deleted
 * ticket holder ledger. */
extern ClusterBufmgrItlRecycleGuardResult
cluster_bufmgr_itl_recycle_guard_arm(Buffer buffer, const ClusterPcmOwnSnapshot *expected);
extern void cluster_bufmgr_itl_recycle_guard_unlock(Buffer buffer);
extern bool cluster_bufmgr_itl_recycle_guard_relock(Buffer buffer);
extern void cluster_bufmgr_itl_recycle_guard_cancel(Buffer buffer);
extern ResourceXBufferActivationResult
cluster_bufmgr_pcm_own_activate_x_by_tag(const ResourceXAcquisitionRef *ref,
										 const ResourceXCurrentImage *image,
										 ResourceXBufferInstallProof *out_proof);
extern ResourceXBufferActivationResult cluster_bufmgr_pcm_own_writer_activation_clear_by_tag_exact(
	const ResourceXAcquisitionRef *ref, ResourceXBufferActivationProof *out_proof);
/* Exact known-new TARGET adaptation.  These calls bind/clear only the
 * Resource-X sidecar around an already committed local X reservation; they
 * never install durable-storage bytes into the direct-init descriptor. */
extern ResourceXBufferActivationResult cluster_bufmgr_pcm_own_direct_init_bind_x_by_tag_exact(
	const ResourceXAcquisitionRef *ref, uint64 expected_generation,
	uint64 expected_reservation_token, ResourceXBufferInstallProof *out_proof);
extern ResourceXBufferActivationResult cluster_bufmgr_pcm_own_direct_init_clear_x_by_tag_exact(
	const ResourceXAcquisitionRef *ref, uint64 expected_generation,
	uint64 expected_reservation_token, ResourceXBufferActivationProof *out_proof);
extern ResourceXSidecarNeutralizeResult
cluster_bufmgr_resource_x_neutralize_exact(const BufferTag *tag, uint64 old_formation,
										   uint64 acquisition_generation);
/* Resolve one resident descriptor and snapshot its ownership tuple while the
 * mapping partition and buffer header still bind the same BufferTag.  The
 * returned buffer id is only a locator; every later lifecycle call rechecks
 * the complete snapshot and therefore remains safe across descriptor reuse. */
extern ClusterPcmOwnResult
cluster_bufmgr_pcm_own_snapshot_by_tag(const BufferTag *tag, int *out_buffer_id,
									   ClusterPcmOwnSnapshot *out_snapshot);
extern ClusterPcmOwnResult cluster_bufmgr_pcm_own_direct_init_snapshot_by_tag_exact(
	const BufferTag *tag, uint64 expected_generation, uint64 expected_reservation_token,
	int *out_buffer_id, ClusterPcmOwnSnapshot *out_snapshot);
/* Backstop direct content-lock mutation entrances that do not pass through
 * LockBuffer/W1.  GRANT_PENDING image installation is permitted; a live
 * source REVOKING lifecycle or retained PI+VALID descriptor is not. */
extern bool cluster_bufmgr_pcm_x_content_write_permitted(BufferDesc *buf);
extern bool cluster_bufmgr_pcm_x_ordinary_content_write_permitted(BufferDesc *buf);
extern void cluster_bufmgr_pcm_own_republish_grant_pending_image(BufferDesc *buf);
/* Called only after the requester has proved the exact remote master's S->N
 * RELEASE application ACK.  Atomically normalizes the matching descriptor
 * tuple and returns the fresh N snapshot used by the later PREPARE leg. */
extern ClusterPcmOwnResult
cluster_bufmgr_pcm_own_finish_s_release_to_n(BufferDesc *buf,
											 const ClusterPcmOwnSnapshot *expected_s,
											 ClusterPcmOwnSnapshot *out_n_snapshot);
/* Current-slice remote-S Resource-X adaptation.  The caller must hold
 * content EXCLUSIVE and present the exact REVOKING S tuple whose immutable
 * type-18 status is already retained in the existing LMS DATA ring. */
extern ClusterPcmOwnResult
cluster_bufmgr_pcm_own_finish_remote_s_block_to_n(BufferDesc *buf,
												  const ClusterPcmOwnSnapshot *expected_revoking,
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
extern ClusterPcmOwnResult
cluster_bufmgr_pcm_own_delivery_begin_x_reservation(BufferDesc *buf,
													const ClusterPcmOwnSnapshot *expected,
													uint64 delivery_attempt, uint64 *out_token);
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
extern ClusterPcmOwnResult
cluster_bufmgr_pcm_own_begin_x_revoke_held_by_tag(const BufferTag *tag,
												  const ClusterPcmOwnSnapshot *expected_x,
												  ClusterPcmOwnHeldXRevoke *held_out);
extern ClusterPcmOwnResult
cluster_bufmgr_pcm_own_abort_held_x_revoke(ClusterPcmOwnHeldXRevoke *held);
extern ClusterPcmOwnResult
cluster_bufmgr_pcm_own_validate_held_x_revoke(const ClusterPcmOwnHeldXRevoke *held);
extern ClusterPcmOwnResult
cluster_bufmgr_pcm_own_adopt_held_x_revoke(int buffer_id, const ResourceXLocalOwnerHandle *owner,
										   ClusterPcmOwnHeldXRevoke *held_out);
extern ClusterPcmOwnResult
cluster_bufmgr_pcm_own_try_drain_held_x_revoke(const ClusterPcmOwnHeldXRevoke *held);
extern ClusterPcmOwnResult
cluster_bufmgr_pcm_own_try_drain_drop_x_revoke(BufferDesc *buf,
											   const ClusterPcmOwnSnapshot *expected_revoking);
extern ClusterPcmOwnResult cluster_bufmgr_pcm_own_finish_held_x_revoke_retain(
	ClusterPcmOwnHeldXRevoke *held, XLogRecPtr expected_lsn, ClusterPcmOwnSnapshot *out_retained,
	ClusterPcmOwnFinishRefusal *out_refusal);
extern ClusterPcmOwnResult
cluster_bufmgr_pcm_own_abandon_held_x_revoke_after_fail_closed(ClusterPcmOwnHeldXRevoke *held);

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
/* Freeze one exact S source and choose the newest safe bytes from its clean
 * current image and verified shared storage.  A valid required_page_scn is a
 * hard floor; V1's zero floor still prefers the newer observed copy. */
extern ClusterPcmOwnResult cluster_bufmgr_pcm_own_prepare_s_source_image(
	BufferDesc *buf, const ClusterPcmOwnSnapshot *expected_s, SCN required_page_scn,
	ClusterPcmOwnSnapshot *out_revoking, char block_data[BLCKSZ], XLogRecPtr *out_page_lsn,
	uint64 *out_page_scn, ClusterPcmOwnSourcePrepareRefusal *out_refusal);
extern ClusterPcmOwnResult
cluster_bufmgr_pcm_own_abort_s_revoke(BufferDesc *buf,
									  const ClusterPcmOwnSnapshot *expected_revoking);
extern ClusterPcmOwnResult cluster_bufmgr_pcm_own_finish_revoke_retain(
	BufferDesc *buf, const ClusterPcmOwnSnapshot *expected_revoking, XLogRecPtr expected_lsn,
	ClusterPcmOwnSnapshot *out_retained, ClusterPcmOwnFinishRefusal *out_refusal);
extern ClusterPcmOwnResult cluster_bufmgr_pcm_own_release_retained_fence_preserve_pi(
	const BufferTag *tag, uint64 source_generation, bool *release_applied_out);
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
