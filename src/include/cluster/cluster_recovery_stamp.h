/* PGRAC: exact recovery stamp/checkpoint interlock, not write authority.
 * Author: SqlRush <sqlrush@gmail.com>
 * Spec: 2026-10-14-pre2-m5-e1-format-closure-v1.md */
#ifndef CLUSTER_RECOVERY_STAMP_H
#define CLUSTER_RECOVERY_STAMP_H

#include "cluster/cluster_wal_writer.h"
#include "cluster/cluster_undo_smgr.h"
#include "cluster/cluster_recovery_tt_record.h"
#include "storage/spin.h"

#define CLUSTER_RECOVERY_STAMP_PAYLOAD_BYTES 320
#define CLUSTER_RECOVERY_STAMP_SLOTS 128

typedef enum ClusterRecoveryStampKindV1 {
	CLUSTER_RECOVERY_STAMP_C0 = 1,
	CLUSTER_RECOVERY_STAMP_FPI = 2,
	CLUSTER_RECOVERY_STAMP_D0 = 3
} ClusterRecoveryStampKindV1;

typedef enum ClusterRecoveryStampPhaseV1 {
	CLUSTER_RECOVERY_STAMP_EMPTY = 0,
	CLUSTER_RECOVERY_STAMP_PRE_INSERT,
	CLUSTER_RECOVERY_STAMP_INSERTING,
	CLUSTER_RECOVERY_STAMP_INSERTED,
	CLUSTER_RECOVERY_STAMP_DURABLE
} ClusterRecoveryStampPhaseV1;

typedef enum ClusterRecoveryStampPollV1 {
	CLUSTER_RECOVERY_STAMP_INVALID = 0,
	CLUSTER_RECOVERY_STAMP_PENDING,
	CLUSTER_RECOVERY_STAMP_COMPLETE
} ClusterRecoveryStampPollV1;

typedef struct ClusterRecoveryStampOwnerV1 {
	ClusterWalWriterToken writer;
	ClusterWalThreadClaimRefV2 target;
	uint64 executor_boot;
	uint64 recovery_serial;
	int32 executor_pid;
} ClusterRecoveryStampOwnerV1;

typedef struct ClusterRecoveryStampKeyV1 {
	uint64 slot_generation;
	uint64 window_seq;
	uint16 target_thread;
} ClusterRecoveryStampKeyV1;

/* Returned by prepare, copied unchanged by the one original executor. */
typedef struct ClusterRecoveryStampHandleV1 {
	ClusterRecoveryStampKeyV1 key;
	ClusterRecoveryStampOwnerV1 owner;
	ClusterRecoveryStampKindV1 kind;
	uint32 payload_length;
	uint8 payload[CLUSTER_RECOVERY_STAMP_PAYLOAD_BYTES];
} ClusterRecoveryStampHandleV1;

/* Embedded in the original per-thread replay slot. No pointers or callbacks. */
typedef struct ClusterRecoveryStampSlotV1 {
	slock_t lock;
	ClusterRecoveryStampHandleV1 handle;
	ClusterRecoveryStampPhaseV1 phase;
	XLogRecPtr record_start;
	XLogRecPtr record_end;
	bool failed;
} ClusterRecoveryStampSlotV1;

typedef struct ClusterRecoveryStampCaptureV1 {
	bool captured;
	bool caller_owned;
	uint32 count;
	ClusterRecoveryStampKeyV1 keys[CLUSTER_RECOVERY_STAMP_SLOTS];
} ClusterRecoveryStampCaptureV1;

/* Call only after original authority, reservation and held-FD preparation.
 * Admits exact C0 or D0 bound to the original writer/target/boot/serial;
 * FPI and unknown shapes refuse. ROOT/graph/retention remain caller premises.
 * Complete codec validation occurs before taking any slot lock. No WAL or IO.
 * Inputs/output must not alias. Output is zero on refusal. */
extern bool cluster_recovery_stamp_prepare_v1(const ClusterRecoveryStampOwnerV1 *owner,
											  ClusterRecoveryStampKindV1 kind, const uint8 *payload,
											  Size length, ClusterRecoveryStampHandleV1 *out);
/* Both insertion transitions require the original executor's critical section.
 * INSERTED records the actual XLogInsert start/end; no insertion lock is held.
 * All transitions match the unchanged prepared operation, without allocation,
 * hashing or codec calls inside the critical section or slot spinlock. */
extern bool cluster_recovery_stamp_inserting_v1(const ClusterRecoveryStampHandleV1 *handle);
extern bool cluster_recovery_stamp_inserted_v1(const ClusterRecoveryStampHandleV1 *handle,
											   XLogRecPtr start, XLogRecPtr end);
/* Dormant D0 adapter: immediately after the original executor's XLogInsert,
 * bind its returned end and original last-insert header/actual start to this
 * exact INSERTING window. Refusal leaves the window unchanged and cannot
 * cancel post-insert duty. The header checks do not prove payload identity,
 * full raw-record SHA/CRC, owner/graph authority, flush or target durability;
 * the original inserter and the remaining independent proofs are required. */
extern bool cluster_recovery_stamp_ctrc_inserted_v2(const ClusterRecoveryStampHandleV1 *handle,
													XLogRecPtr end);
/* The original executor's dormant D0 emitter. The caller must have completed
 * all readiness/authority/capacity checks and hold the immutable prepared
 * payload while in its critical section. This performs exactly one native
 * XLogBeginInsert/RegisterData/XLogInsert and immediately binds the original
 * native header and positions. A live native XID, disallowed insertion, any
 * pre-insert mismatch, native error, or post-insert mismatch refuses; after
 * insertion the window is sticky failed and cannot be cancelled or completed.
 * It proves neither WAL flush nor target IO durability, graph/owner authority,
 * full raw-record identity, nor any live production caller. */
extern XLogRecPtr
cluster_recovery_stamp_ctrc_insert_v2(const ClusterRecoveryStampHandleV1 *handle);
/* Dormant original-startup D0 flush step, in the executor's critical section.
 * Flushes the exact INSERTED window's actual end with its original writer,
 * then independently checks native startup flush coverage and revalidates the
 * writer/window. No slot lock is held across those calls. A matched failure
 * is sticky; native errors retain the original post-insert responsibility.
 * Success leaves INSERTED unchanged: target sync/postread and all other
 * readiness/authority proofs are still required before DURABLE or clear.
 * This is not a generic worker flush receipt or a live production caller. */
extern bool cluster_recovery_stamp_ctrc_flush_v2(const ClusterRecoveryStampHandleV1 *handle);
/* Dormant startup-only finish, inside the original insertion critical section.
 * Flushes this exact INSERTED native end, writes only the held-FD stage bound
 * to all 320B of this payload, directly syncs/reads back, and rechecks the
 * original writer/flush/window before DURABLE and clear. A matched refusal
 * stays failed; native errors retain post-insert duty. The caller retains
 * graph/source/terminal/ROOT/IR/WALR/target reservation throughout. This does
 * not establish those proofs, close the ResourceOwner token or add a live caller. */
extern bool cluster_recovery_stamp_ctrc_finish_v2(const ClusterRecoveryStampHandleV1 *handle,
												  ClusterUndoSmgrRecoveryStampV1 stamp);
/* Only the original executor calls durable after real WAL flush and exact
 * target sync/postread. These APIs record completion, never prove it. */
extern bool cluster_recovery_stamp_durable_v1(const ClusterRecoveryStampHandleV1 *handle);
extern bool cluster_recovery_stamp_clear_v1(const ClusterRecoveryStampHandleV1 *handle);
/* Cancel only before INSERTING, when no WAL insertion/target mutation began. */
extern bool cluster_recovery_stamp_cancel_v1(const ClusterRecoveryStampHandleV1 *handle);
extern bool cluster_recovery_stamp_fail_v1(const ClusterRecoveryStampHandleV1 *handle);
/* Capture after fixing redo and releasing native insertion locks. No caller
 * lock, IO or wait is retained. Poll never adds later windows or clears one. */
extern bool cluster_recovery_stamp_capture_v1(ClusterRecoveryStampCaptureV1 *out);
extern ClusterRecoveryStampPollV1
cluster_recovery_stamp_poll_v1(const ClusterRecoveryStampCaptureV1 *capture);
#endif
