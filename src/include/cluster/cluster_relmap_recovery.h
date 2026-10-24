/*-------------------------------------------------------------------------
 * cluster_relmap_recovery.h
 *    Original native issuance inputs for relmap recovery.
 *-------------------------------------------------------------------------
 */
#ifndef CLUSTER_RELMAP_RECOVERY_H
#define CLUSTER_RELMAP_RECOVERY_H
#include "access/transam.h"
#include "access/clog.h"
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

/* Current native CLOG fact under its original truncation lock. status is
 * COMMITTED or ABORTED, derived by the native reader, never supplied by caller.
 * status_lsn is the native group hint (possibly zero), NOT a durability receipt.
 * This value does not certify a persisted checkpoint/base or sealed redo. */
typedef struct ClusterRelmapRecoveryNativeTerminal {
	ClusterRelmapRecoveryNativeBounds bounds;
	uint64 top_full_xid;
	XidStatus status;
	XLogRecPtr status_lsn;
} ClusterRelmapRecoveryNativeTerminal;

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
/* Same exact current owner as native_bounds. Native floor/issuance are sampled
 * while XactTruncationLock spans TransactionIdGetStatus; XidGenLock is released
 * before native CLOG I/O. Recheck original owner after unlocking; publish last.
 * IN_PROGRESS/SUB_COMMITTED/unknown refuse. Current native observation only,
 * not the frozen opaque cold terminal_read or COMPLETE/release permission.
 * Neither native lock nor XactSLRULock may be held by caller. Nonalias refusal clears output;
 * original input overlap stays unchanged. Native ERROR propagates unchanged. */
extern bool
cluster_relmap_recovery_native_terminal_capture(const struct ClusterRelmapOwnerSampleV2 *owner,
												ClusterRelmapRecoveryNativeTerminal *out);
/* Dormant original-owner terminal persistence. Uses the same native floor and
 * actual-issued window as capture, but writes the original native CLOG slot
 * through its SLRU owner, then directly syncs and reads back the same native
 * file. Success is that terminal slot's native durability observation only;
 * not a persisted FullXid base/source seal or cold recovery authority.
 * Caller lock/alias/refusal/ERROR contracts are the same as capture. */
extern bool
cluster_relmap_recovery_native_terminal_sync(const struct ClusterRelmapOwnerSampleV2 *owner,
											 ClusterRelmapRecoveryNativeTerminal *out);
#endif
#endif
