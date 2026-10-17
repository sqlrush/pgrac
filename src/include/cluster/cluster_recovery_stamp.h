/* PGRAC: exact recovery stamp/checkpoint interlock, not write authority.
 * Author: SqlRush <sqlrush@gmail.com>
 * Spec: 2026-10-14-pre2-m5-e1-format-closure-v1.md */
#ifndef CLUSTER_RECOVERY_STAMP_H
#define CLUSTER_RECOVERY_STAMP_H

#include "cluster/cluster_wal_writer.h"
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
} ClusterRecoveryStampHandleV1;

/* Embedded in the original per-thread replay slot. No pointers or callbacks. */
typedef struct ClusterRecoveryStampSlotV1 {
	slock_t lock;
	ClusterRecoveryStampHandleV1 handle;
	ClusterRecoveryStampPhaseV1 phase;
	ClusterRecoveryStampKindV1 kind;
	uint32 payload_length;
	uint8 payload[CLUSTER_RECOVERY_STAMP_PAYLOAD_BYTES];
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
 * Currently admits exact C0; FPI/D0 and unknown shapes refuse. No WAL or IO.
 * Inputs/output must not alias. Output is zero on refusal. */
extern bool cluster_recovery_stamp_prepare_v1(const ClusterRecoveryStampOwnerV1 *owner,
											  ClusterRecoveryStampKindV1 kind, const uint8 *payload,
											  Size length, ClusterRecoveryStampHandleV1 *out);
/* Both insertion transitions require the original executor's critical section.
 * INSERTED records the actual XLogInsert start/end; no insertion lock is held. */
extern bool cluster_recovery_stamp_inserting_v1(const ClusterRecoveryStampHandleV1 *handle);
extern bool cluster_recovery_stamp_inserted_v1(const ClusterRecoveryStampHandleV1 *handle,
											   XLogRecPtr start, XLogRecPtr end);
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
