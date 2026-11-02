/* Recovery creation borrows the original typed episode, never a runtime creator. */
#ifndef CLUSTER_CONTROL_RELMAP_RECOVERY_H
#define CLUSTER_CONTROL_RELMAP_RECOVERY_H
#include "cluster/cluster_control_relmap_delta.h"
#include "cluster/cluster_control_native_files.h"
#include "cluster/cluster_cold_recovery.h"

typedef struct ClusterControlRelmapRecoveryCreateV2 ClusterControlRelmapRecoveryCreateV2;
/* Same original CF-X, complete IR/WALR and isolated-source custody span begin,
 * target I/O and confirm. Call before MappingLock; this never acquires CF.
 * source and actual record positions must match the original sealed plan.
 * expected/batch must independently match durable control registration.
 * Fresh *out is NULL; refusal/ERROR never publishes an authorization.
 * AVAILABLE requires the original batch retained in its coverage. */
extern ClusterControlNativeResultV2 cluster_control_relmap_recovery_create_begin_locked_v2(
	ClusterColdTypedV1 *, struct ClusterRecoveryFencePlan *, uint32 participant,
	const ClusterWalSourceRef *, XLogRecPtr read_rec_ptr, XLogRecPtr end_rec_ptr,
	const ClusterRecoveryFileIdentityV2 *expected, const uint8 batch_uuid[16],
	ClusterControlRelmapRecoveryCreateV2 **out);
/* Creation-only suspended episode. The first authorization still belongs to
 * this exact CF-X acquisition. The original generic/serial entry is unchanged. */
extern ClusterControlNativeResultV2 cluster_control_relmap_recovery_create_begin_barrier_locked_v2(
	ClusterColdTypedV1 *, struct ClusterRecoveryFencePlan *, uint64 episode_id,
	uint32 participant, const ClusterWalSourceRef *, XLogRecPtr, XLogRecPtr,
	const ClusterRecoveryFileIdentityV2 *, const uint8 batch_uuid[16],
	ClusterControlRelmapRecoveryCreateV2 **out);
/* Independent read-only repetition in a fresh CF-X acquisition of the SAME
 * typed/H episode. It selects current persistent facts only for comparison
 * against original batch/complete receipts; it never revives the old selection
 * or allows the original authorization/current/confirm to run again. */
extern bool cluster_control_relmap_recovery_create_confirmed_barrier_current_v2(
	ClusterControlRelmapRecoveryCreateV2 *, uint64 episode_id,
	const ClusterRecoveryCheckpointFileV2 *, uint64 count);
extern bool cluster_cold_typed_relmap_create_barrier_current_v2(
	ClusterColdTypedV1 *, struct ClusterRecoveryFencePlan *, uint64 episode_id);
extern bool cluster_cold_typed_relmap_create_barrier_record_current_v2(
	ClusterColdTypedV1 *, struct ClusterRecoveryFencePlan *, uint64 episode_id,
	uint32 participant, const ClusterWalSourceRef *, XLogRecPtr, XLogRecPtr);
extern bool cluster_cold_typed_relmap_create_barrier_record_held_v2(
	ClusterColdTypedV1 *, struct ClusterRecoveryFencePlan *, uint64 episode_id,
	uint32 participant, const ClusterWalSourceRef *, XLogRecPtr, XLogRecPtr);
/* Local-only check under the original MappingLock/I/O lifetime; no ROOT I/O.
 * A copy of the opaque carrier is never authority. */
extern bool
cluster_control_relmap_recovery_create_current_v2(ClusterControlRelmapRecoveryCreateV2 *);
/* After original MappingLock release, consume exactly two complete real pair
 * receipts once. Only the existing immutable registry writer changes RESERVED
 * to AVAILABLE; no scope/pin is created or released. False/ERROR retains debt
 * and revokes this original episode. Repeating an AVAILABLE record needs the
 * exact durable batch plus unchanged complete receipts, never current-file
 * backfill. A confirmation grants no recovery/OPEN/lifetime qualification. */
extern ClusterControlNativeResultV2 cluster_control_relmap_recovery_create_confirm_locked_v2(
	ClusterControlRelmapRecoveryCreateV2 *, const ClusterRecoveryCheckpointFileV2 *, uint64 count);
/* Read-only after successful confirmation, with the SAME original CF-X,
 * complete typed/fence sources and continuous IR/WALR. Call only after releasing
 * MappingLock and other native family locks. The original nonzero batch and
 * both complete saved pair receipts must match persistent AVAILABLE coverage;
 * the exact published native table is borrowed/current-checked across the
 * original source/ROOT rechecks. No CF acquisition, target-pair read/sync/create,
 * registration, confirmation, new scope/pin or completion is performed.
 * false/ERROR revokes this episode and retains the original creation debt.
 * A successful call does not consume the receipt; repetitions recheck it. */
extern bool cluster_control_relmap_recovery_create_confirmed_current_v2(
	ClusterControlRelmapRecoveryCreateV2 *, const ClusterRecoveryCheckpointFileV2 *, uint64 count);
/* Fixed original Startup owner check for this one confirmation's physical
 * ROOT phase. Complete source/cut/frame checks remain outside MappingLock. */
extern bool cluster_cold_typed_relmap_create_barrier_confirmation_current_v2(
	ClusterColdTypedV1 *, struct ClusterRecoveryFencePlan *, uint64 episode_id,
	const ClusterControlRelmapRecoveryCreateV2 *, uint32 participant,
	const ClusterWalSourceRef *, XLogRecPtr, XLogRecPtr);
/* Match the original saved baseline and return only this completed confirm's
 * exact control/table delta. The full original pair/source receipts stay
 * owned by the same carrier. Borrow counts are point observations only.
 * No selection, I/O, mutation, current permission or rebase. */
extern bool cluster_control_relmap_recovery_create_control_delta_v2(
	const ClusterControlRelmapRecoveryCreateV2 *, const ClusterColdTypedV1 *,
	const struct ClusterRecoveryFencePlan *, uint64 episode_id,
	const ClusterRecoveryFileIdentityV2 *expected, const uint8 batch_uuid[16],
	const ClusterControlRelmapReferenceV2 *before,
	const ClusterControlNativeTableInfoV2 *table_before,
	ClusterControlRelmapReferenceV2 *after, ClusterControlNativeTableInfoV2 *table_after);
/* Original typed owner records a completed confirmation in preallocated
 * local custody. It retains the prepare baseline and accepts only a complete
 * chain of exact same-episode control/table deltas. No ROOT publication. */
extern bool cluster_cold_typed_relmap_create_accept_delta_v2(
	ClusterColdTypedV1 *, struct ClusterRecoveryFencePlan *, uint64 episode_id,
	uint32 original_declaration_index, const ClusterControlRelmapRecoveryCreateV2 *);
/* The plan's execution index is not a registry index. Match the original
 * expected/batch uniquely in the saved prepare set before accepting its link. */
extern bool cluster_cold_typed_relmap_create_accept_confirmation_v2(
	ClusterColdTypedV1 *, struct ClusterRecoveryFencePlan *, uint64 episode_id,
	const ClusterRecoveryFileIdentityV2 *, const uint8 batch_uuid[16],
	const ClusterControlRelmapRecoveryCreateV2 *);
/* Pure storage check for borrowed outputs, including failed/unconfirmed
 * carriers and their entire owned selection. No current/CF or I/O check. */
extern bool cluster_control_relmap_recovery_create_output_distinct_v2(
	const ClusterControlRelmapRecoveryCreateV2 *, const void *, Size);
/* Failure/destroy retire local metadata only; no cancellation or file removal.
 * Destroy an unconfirmed authorization also fails the original episode. */
extern void cluster_control_relmap_recovery_create_fail_v2(ClusterControlRelmapRecoveryCreateV2 *);
extern void
cluster_control_relmap_recovery_create_destroy_v2(ClusterControlRelmapRecoveryCreateV2 **);
/* Original Startup owner checks, not caller-provided authority callbacks. */
extern bool cluster_cold_typed_relmap_create_record_current_v2(ClusterColdTypedV1 *,
															   struct ClusterRecoveryFencePlan *,
															   uint32 participant,
															   const ClusterWalSourceRef *,
															   XLogRecPtr, XLogRecPtr);
extern bool cluster_cold_typed_relmap_create_record_held_v2(ClusterColdTypedV1 *,
															struct ClusterRecoveryFencePlan *,
															uint32 participant,
															const ClusterWalSourceRef *, XLogRecPtr,
															XLogRecPtr);
extern void cluster_cold_typed_relmap_create_failed_v2(ClusterColdTypedV1 *);
#endif
