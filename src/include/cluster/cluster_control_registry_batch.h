/* Batch registration by the original CF-X control/creation owner. */
#ifndef CLUSTER_CONTROL_REGISTRY_BATCH_H
#define CLUSTER_CONTROL_REGISTRY_BATCH_H
#include "cluster/cluster_control_file_identity.h"
#include "common/cluster_recovery_reservations.h"
typedef enum ClusterControlRegistryOperationV4 {
	CLUSTER_CONTROL_REGISTRY_RESERVE_V4 = 1,
	CLUSTER_CONTROL_REGISTRY_COMPLETE_V4,
	CLUSTER_CONTROL_REGISTRY_CANCEL_V4
} ClusterControlRegistryOperationV4;
typedef struct ClusterControlRegistryBatchV4 {
	ClusterControlRegistryOperationV4 operation;
	uint8 batch_uuid[16];
	uint64 identity_count;
	const ClusterRecoveryFileIdentityV2 *identities;
	uint64 receipt_count;
	const ClusterRecoveryCheckpointFileV2 *receipts;
} ClusterControlRegistryBatchV4;
/* SAME original CF-X throughout; no acquire/reentry. The control owner supplies
 * independently preallocated identities, or the original file owner's complete
 * sync/readback receipts under continuous file/I/O custody. Numbers alone do
 * not certify checkpoint/source/era/lifetime or OPEN. Existing source/cut and
 * checkpoint/commit generations remain exact. No native caller activation.
 * Complete replaces the WHOLE batch; cancel removes only this owner's batch.
 * Existing AVAILABLE and other batches cannot be deleted/reidentified.
 * Output is a selection reference. Refusal preserves it. Entered failure/ERROR
 * poisons the selection: no retry/rollback/unlink, even if registry/ROOT may be
 * durable. Original CF/file/pin/creation duty remains. Success also requires a
 * NEW selection; destroy local selection before the original owner exits. */
extern ClusterControlRootResult cluster_control_root_registry_batch_locked_v4(
	ClusterControlRelmapSelectionV2 *, const ClusterControlRegistryBatchV4 *, uint8 reference[128]);
/* Control observations only, never ordinary native file admission. */
extern uint64 cluster_control_root_registry_reservation_count_v4(ClusterControlRelmapSelectionV2 *);
extern bool cluster_control_root_registry_reservation_at_v4(ClusterControlRelmapSelectionV2 *,
															uint64 index,
															ClusterRecoveryFileReservationV1 *out);
/* Exact original creator + batch + full preallocated expected required.
 * By-value observation; no FD, pin or file permission is created/transferred. */
extern bool cluster_control_root_reserved_file_locked_v4(
	ClusterControlRelmapSelectionV2 *, const uint8 batch_uuid[16],
	const ClusterRecoveryFileIdentityV2 *expected, ClusterRecoveryFileIdentityV2 *out);
#endif
