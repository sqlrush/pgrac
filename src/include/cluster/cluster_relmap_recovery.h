/*-------------------------------------------------------------------------
 * cluster_relmap_recovery.h
 *    Original native issuance inputs for relmap recovery.
 *-------------------------------------------------------------------------
 */
#ifndef CLUSTER_RELMAP_RECOVERY_H
#define CLUSTER_RELMAP_RECOVERY_H
#include "access/transam.h"
#ifdef USE_PGRAC_CLUSTER
struct ClusterRelmapOwnerSampleV2;
typedef struct ClusterRelmapRecoveryNativeBounds {
	uint64 system_identifier;
	uint64 database_incarnation;
	uint16 origin;
	uint64 membership_epoch;
	uint64 admitted_incarnation;
	uint64 boot_incarnation;
	uint64 native_floor;
	uint64 last_issued;
} ClusterRelmapRecoveryNativeBounds;

/* Original allocator, XidGenLock held, AFTER native ProcArray publication.
 * Shared volatile provenance only: no WAL, allocation, lock or ERROR. Never
 * call for a candidate, lease/JUMP limit, checkpoint or replay endpoint. */
extern void cluster_relmap_recovery_issued_publish(FullTransactionId issued);

/* Dormant original-backend input, while its sampled top FullXid and global
 * relmap X still belong to this exact native writer. Neither XidGenLock nor
 * XactTruncationLock may already be held; refusal never releases caller locks.
 * Actual native CLOG floor
 * and actual published last-issued FullXid are sampled under the original
 * native locks; no widening from nextXid or a stripe/legal ceiling.
 * This is a current-boot observation, not a persisted checkpoint base,
 * terminal, cold/restart certificate, COMPLETE or release permission.
 * Nonalias refusal clears output. Original input overlap is unchanged. */
extern bool cluster_relmap_recovery_native_bounds(const struct ClusterRelmapOwnerSampleV2 *owner,
												  ClusterRelmapRecoveryNativeBounds *out);
#endif
#endif
