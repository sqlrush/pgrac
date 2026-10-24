/*-------------------------------------------------------------------------
 * cluster_relmap_recovery.h
 *    Original native issuance inputs for relmap recovery.
 *-------------------------------------------------------------------------
 */
#ifndef CLUSTER_RELMAP_RECOVERY_H
#define CLUSTER_RELMAP_RECOVERY_H
#include "access/transam.h"
#include "access/clog.h"
#include "common/cluster_relmap_v2.h"
#ifdef USE_PGRAC_CLUSTER
struct ClusterRelmapOwnerSampleV2;

/* Original-round recovery custody; no caller-supplied terminal authority. */
typedef struct ClusterRelmapRecoveryHandle ClusterRelmapRecoveryHandle;
struct ClusterWalInputsV1;
/* Dormant pending custody, not a terminal result. Borrow the original cold
 * scope and complete inputs for this process/round. Validate the full view,
 * select its owner origin internally, and bind the exact pending native WAL
 * before the checkpoint-prefix terminal and its owner window. No supplied
 * status/qualification boolean, new scope, source pin or native base grant.
 * Internally bind the original native durable page observation fingerprint;
 * original source/round checks bracket its native sync/readback. This does
 * not certify the FullXid era or retained lifetime.
 * Inputs must outlive the handle. Nonoverlap refusal sets *out NULL; overlap
 * is unchanged. ERROR frees only local memory and propagates. */
extern bool cluster_relmap_recovery_prepare_v1(struct ClusterWalInputsV1 *inputs,
											   const ClusterRelmapV2Namespace *identity,
											   const ClusterRelmapV2View *pending,
											   ClusterRelmapRecoveryHandle **out);
/* Exact full namespace/view, original-round current checks and an unchanged
 * internally reread native base fingerprint. Page drift (even another slot)
 * conservatively refuses this snapshot; false is not rollback/retry permission.
 * Refusal
 * or ERROR after entry is sticky; overlapping inputs are unchanged and do
 * not invalidate custody. No terminal/COMPLETE/release permission. */
extern bool cluster_relmap_recovery_handle_matches_v1(ClusterRelmapRecoveryHandle *handle,
													  const ClusterRelmapV2Namespace *identity,
													  const ClusterRelmapV2View *pending);
/* Frees local custody only; never the original borrowed scope/pins. */
extern void cluster_relmap_recovery_handle_release_v1(ClusterRelmapRecoveryHandle **handle);

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
