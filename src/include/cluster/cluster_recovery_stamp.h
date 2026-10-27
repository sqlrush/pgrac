/* PGRAC: exact recovery stamp/checkpoint interlock, not write authority.
 * Author: SqlRush <sqlrush@gmail.com>
 * Spec: 2026-10-14-pre2-m5-e1-format-closure-v1.md */
#ifndef CLUSTER_RECOVERY_STAMP_H
#define CLUSTER_RECOVERY_STAMP_H

#include "cluster/cluster_wal_writer.h"
#include "cluster/cluster_recovery_tt_record.h"
#include "cluster/cluster_page_authority.h"
#include "access/generic_xlog.h"
#include "storage/spin.h"

#define CLUSTER_RECOVERY_STAMP_PAYLOAD_BYTES 320
#define CLUSTER_RECOVERY_STAMP_SLOTS 128
#define CLUSTER_RECOVERY_STAMP_EXIT_V1 1

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
	CLUSTER_RECOVERY_STAMP_DURABLE,
	CLUSTER_RECOVERY_STAMP_ADOPT_PENDING
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

/* Returned by prepare, copied unchanged by the one original executor.
 * The complete operation is value-owned and immutable until retirement. */
typedef struct ClusterRecoveryStampHandleV1 {
	ClusterRecoveryStampKeyV1 key;
	ClusterRecoveryStampOwnerV1 owner;
	ClusterRecoveryStampKindV1 kind;
	uint32 payload_length;
	uint8 payload[CLUSTER_RECOVERY_STAMP_PAYLOAD_BYTES];
} ClusterRecoveryStampHandleV1;

/* Local image comparison plus references to the original held protection.
 * The locator is the typed SPACE decoder's expected key; the namespace is
 * the immutable operation owner's. No image bytes or object pointers. */
typedef struct ClusterRecoveryStampFpiV1 {
	GenericXLogRecoveryImageDescriptorV1 image;
	RelFileLocator locator;
	uint32 reserved_zero;
	RfPageRecoveryReferenceV1 protection;
} ClusterRecoveryStampFpiV1;
StaticAssertDecl(sizeof(ClusterRecoveryStampFpiV1) <= CLUSTER_RECOVERY_STAMP_PAYLOAD_BYTES,
	"recovery FPI envelope must fit the existing stamp payload");

/* Process-local WAL-leg result, never embedded in shared memory. The exact
 * image copy remains available for subsequent typed installation. The caller
 * retains the original preflight/serial/pin and this promoted guard on success,
 * refusal after promotion, or ERROR. WAL flush does not retire the window. */
typedef struct ClusterRecoveryFpiInsertV1 {
	GenericXLogRecoveryImageV1 image;
	RfPageAuthorityGuardV1 *guard;
	ClusterRecoveryStampHandleV1 window;
	XLogRecPtr record_start;
	XLogRecPtr record_end;
	bool wal_flushed;
	/* Local target I/O progress only; original discharge/window remain pending. */
	bool target_synced;
} ClusterRecoveryFpiInsertV1;

/* Embedded in the original per-thread replay slot. No pointers or callbacks. */
typedef struct ClusterRecoveryStampSlotV1 {
	slock_t lock;
	ClusterRecoveryStampHandleV1 handle;
	ClusterRecoveryStampPhaseV1 phase;
	ClusterRecoveryStampPhaseV1 adopt_from;
	uint64 executor_attempt_stamp;
	uint64 bound_worker_attempt;
	int32 bound_worker_pid;
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

/* Original validated BGW entry registers its scheduler attempt. Only the
 * original LMON, after exact native-handle BGWH_STOPPED, reports its exit.
 * This preserves an unfinished window; it grants no adoption/write authority. */
extern bool cluster_recovery_stamp_worker_begin_v1(uint16 thread, uint64 attempt);
extern bool cluster_recovery_stamp_worker_stopped_v1(uint16 thread, uint64 attempt);

/* Call only after original authority, reservation and held-FD preparation.
 * Admits exact C0/D0 bound to the original writer/target/boot/serial;
 * Raw FPI and unknown shapes refuse. Complete codec validation is outside every
 * slot lock; transitions compare the prepared operation without decoding.
 * No WAL or IO and no authority, graph-completion or release qualification.
 * Inputs/output must not alias. Output is zero on refusal. */
extern bool cluster_recovery_stamp_prepare_v1(const ClusterRecoveryStampOwnerV1 *owner,
											  ClusterRecoveryStampKindV1 kind, const uint8 *payload,
											  Size length, ClusterRecoveryStampHandleV1 *out);
/* Original ordinary-RR FPI admission path. Describe the local image before promotion;
 * consume its exact typed target and original promoted PAGE/serial/pin here.
 * This only records PRE_INSERT, never insertion, installation or completion.
 * Retain all borrowed objects and the local image through the actual work.
 * Inputs/output must not alias. Output is zero on ordinary refusal. */
extern bool cluster_recovery_stamp_prepare_fpi_v1(const ClusterRecoveryStampOwnerV1 *owner,
	const RfPageIdentityV1 *target, const GenericXLogRecoveryImageDescriptorV1 *image,
	const RfPageAuthorityGuardV1 *guard, ClusterRecoverySerialGuard *serial_guard,
	ClusterWalRetentionPin *retention_pin, ClusterRecoveryStampHandleV1 *out);
struct ClusterRecoveryStartV1;
struct ClusterColdCtrcWorkV1;
struct ClusterRecoveryCtrcReplaySetSourceV1;
struct RfPageCtrcAuthorityV1;
/* Original post-DONE Startup, accepted TX result and exact native all-replay
 * PAGE custody. Derive the owner from that original checkpoint owner and the
 * selected old claim; retain its original Start serial. Record only PRE_INSERT
 * through the unchanged F4 state machine/envelope. The descriptor must already
 * describe the exact immutable full frame (hash before PAGE promotion); this
 * API checks its canonical metadata, not the full image or its hash producer.
 * Later insertion must revalidate that frame and live native custody again.
 * All inputs/backing storage stay alive and immutable. Return a handle by
 * value, zero on refusal, stored separately; no caller output is written.
 * subject_count is the caller-retained original complete entry count; it must
 * match the live work before any array read. memory_budget bounds one private
 * full-work snapshot, captured before writer
 * observation and freed on all normal/ERROR paths. No I/O/new acquisition,
 * WAL/flush/install/discharge or borrowed-owner
 * release. A returned window remains pending until original exact completion
 * or valid pre-insertion cancellation, including after subsequent failure. */
extern ClusterRecoveryStampHandleV1 cluster_recovery_stamp_prepare_ctrc_fpi_v1(
	const struct ClusterRecoveryStartV1 *start, const struct ClusterColdCtrcWorkV1 *work,
	const struct ClusterRecoveryCtrcReplaySetSourceV1 *source,
	const struct RfPageCtrcAuthorityV1 *authority, const RfPageIdentityV1 *target,
	const GenericXLogRecoveryImageDescriptorV1 *image, Size subject_count, Size memory_budget);
/* Both insertion transitions require the original executor's critical section.
 * INSERTED records the actual XLogInsert start/end; no insertion lock is held. */
extern bool cluster_recovery_stamp_inserting_v1(const ClusterRecoveryStampHandleV1 *handle);
extern bool cluster_recovery_stamp_inserted_v1(const ClusterRecoveryStampHandleV1 *handle,
											   XLogRecPtr start, XLogRecPtr end);
/* Read-only exact original FPI INSERTED/range observation. No flush, PAGE
 * authority, transition or completion is supplied by this observation. */
extern bool cluster_recovery_stamp_fpi_inserted_current_v1(
	const ClusterRecoveryStampHandleV1 *handle, XLogRecPtr start, XLogRecPtr end);
/* Same read-only exact observation, requiring original nonfailed DURABLE.
 * This snapshot supplies no FD lifetime, discharge or service authority. */
extern bool cluster_recovery_stamp_fpi_durable_current_v1(
	const ClusterRecoveryStampHandleV1 *handle, XLogRecPtr start, XLogRecPtr end);
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
