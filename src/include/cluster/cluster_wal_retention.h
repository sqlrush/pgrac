/*-------------------------------------------------------------------------
 *
 * cluster_wal_retention.h
 *	  Control-root-aware WAL retention and reuse fencing (RF-ROOT P6).
 *
 * The declarations in this first slice are process-local only.  They do not
 * define disk, wire, shared-memory, SQL, or cross-version ABI.
 *
 *-------------------------------------------------------------------------
 */
#ifndef CLUSTER_WAL_RETENTION_H
#define CLUSTER_WAL_RETENTION_H

#include "c.h"

#include "access/xlog_internal.h"
#include "cluster/cluster_control_root.h"
#include "cluster/cluster_external_fence.h"
#include "cluster/cluster_grd.h"
#include "cluster/cluster_ir.h"
#include "cluster/cluster_lock_acquire.h"
#include "utils/resowner.h"

#define CLUSTER_WAL_RETENTION_MAX_THREADS UINT16_C(128)
#define CLUSTER_WAL_RETENTION_RESID_TYPE UINT8_C(0xfa)

#define CLUSTER_WAL_REUSE_GUARD_MAGIC UINT32_C(0x57414c47)
#define CLUSTER_WAL_REUSE_GUARD_VERSION UINT16_C(1)
#define CLUSTER_WAL_E1_CONTEXT_MAGIC UINT32_C(0x57453143)
#define CLUSTER_WAL_REUSE_FENCE_TIMEOUT_MS 1000

#define CLUSTER_WAL_GUARD_F_PRIMARY_L3 UINT8_C(0x01)
#define CLUSTER_WAL_GUARD_F_PRIMARY_RECYCLE UINT8_C(0x02)
#define CLUSTER_WAL_GUARD_F_PRIMARY_ZERO UINT8_C(0x04)
#define CLUSTER_WAL_GUARD_F_FALLBACK_L3 UINT8_C(0x08)
#define CLUSTER_WAL_GUARD_F_FALLBACK_ZERO UINT8_C(0x10)
#define CLUSTER_WAL_GUARD_F_TARGET_ABSENT UINT8_C(0x20)
#define CLUSTER_WAL_GUARD_F_KNOWN_MASK UINT8_C(0x3f)

typedef enum ClusterWalFileKind {
	CLUSTER_WAL_FILE_INVALID = 0,
	CLUSTER_WAL_FILE_NORMAL = 1,
	CLUSTER_WAL_FILE_PARTIAL = 2
} ClusterWalFileKind;

typedef struct ClusterWalFileIdentity {
	uint16 thread_id;
	uint8 kind;
	uint8 reserved_zero;
	TimeLineID tli;
	XLogSegNo segno;
} ClusterWalFileIdentity;

typedef struct ClusterWalRetentionInterval {
	uint16 thread_id;
	uint16 reserved_zero;
	TimeLineID tli;
	XLogRecPtr start_lsn;
	XLogRecPtr end_lsn;
} ClusterWalRetentionInterval;

typedef enum ClusterWalReuseEntry {
	CLUSTER_WAL_REUSE_E1_CHECKPOINT_RESTARTPOINT = 1,
	CLUSTER_WAL_REUSE_E2_APPLY_TIMELINE_SWITCH = 2,
	CLUSTER_WAL_REUSE_E3_ARCHIVE_END = 3,
	CLUSTER_WAL_REUSE_E4_PARTIAL_RENAME = 4,
	CLUSTER_WAL_REUSE_E5_RESTORE_STAGING = 5,
	CLUSTER_WAL_REUSE_E6_ARCHIVE_KEEP = 6,
	CLUSTER_WAL_REUSE_E7_EXTERNAL_CLEANUP = 7
} ClusterWalReuseEntry;

typedef enum ClusterWalFileObjectPlatform {
	CLUSTER_WAL_OBJECT_POSIX = 1,
	CLUSTER_WAL_OBJECT_WIN32 = 2
} ClusterWalFileObjectPlatform;

typedef struct ClusterWalFileObjectStamp {
	uint8 platform;
	uint8 reserved1[7];
	uint64 directory_device;
	uint64 directory_file_id_hi;
	uint64 directory_file_id_lo;
	uint64 volume_serial;
	uint64 file_device;
	uint64 file_id_hi;
	uint64 file_id_lo;
	uint64 size_bytes;
	int64 mtime_ns;
	int64 ctime_ns;
	uint32 mode_bits;
	uint32 reserved92;
	ClusterWalFileIdentity parsed_identity;
	XLogLongPageHeaderData long_header;
} ClusterWalFileObjectStamp;

typedef struct ClusterWalrLock {
	ClusterResId resid;
	LOCKMODE mode;
	bool held;
	bool coordinated;
	bool converted_from_pin;
	bool release_uncertain;
	ClusterLockAcquireRequest acquire_request;
	ResourceOwner owner;
} ClusterWalrLock;

typedef struct ClusterWalRetentionE1Context {
	uint32 magic;
	uint16 thread_id;
	uint8 state;
	uint8 reserved;
	int32 owner_pid;
	ResourceOwner owner;
	ClusterRecoveryDutyKey duty;
	ClusterControlRootReadToken root_read;
	ClusterFormationWitnessV1 *formation;
	ClusterWalrLock coarse_walr;
	uint64 provider_deadline_us;
} ClusterWalRetentionE1Context;

typedef enum ClusterWalTerminalOutcome {
	CLUSTER_WAL_TERMINAL_UNCHANGED = 0,
	CLUSTER_WAL_TERMINAL_REMOVED = 1,
	CLUSTER_WAL_TERMINAL_RECYCLED = 2,
	CLUSTER_WAL_TERMINAL_RENAMED_PARTIAL = 3,
	CLUSTER_WAL_TERMINAL_REPLACED = 4,
	CLUSTER_WAL_TERMINAL_CREATED = 5
} ClusterWalTerminalOutcome;

typedef enum ClusterWalReuseGuardResult {
	CLUSTER_WAL_GUARD_OK = 0,
	CLUSTER_WAL_GUARD_BLOCKED = 1,
	CLUSTER_WAL_GUARD_INVALID = 2,
	CLUSTER_WAL_GUARD_RELEASE_UNCERTAIN = 3
} ClusterWalReuseGuardResult;

typedef enum ClusterWalReuseDenyReason {
	CLUSTER_WAL_DENY_NONE = 0,
	CLUSTER_WAL_DENY_INVALID_IDENTITY = 1,
	CLUSTER_WAL_DENY_ACTIVATION = 2,
	CLUSTER_WAL_DENY_THREAD_SCOPE = 3,
	CLUSTER_WAL_DENY_ROOT_UNAVAILABLE = 4,
	CLUSTER_WAL_DENY_ROOT_REQUIRED = 5,
	CLUSTER_WAL_DENY_ROOT_STALE = 6,
	CLUSTER_WAL_DENY_DUTY_STALE = 7,
	CLUSTER_WAL_DENY_SERIAL_STALE = 8,
	CLUSTER_WAL_DENY_FENCE_UNAVAILABLE = 9,
	CLUSTER_WAL_DENY_PINNED = 10,
	CLUSTER_WAL_DENY_INSTALL_SOURCE_UNPROVEN = 11,
	CLUSTER_WAL_DENY_GES_UNAVAILABLE = 12,
	CLUSTER_WAL_DENY_OBJECT_STALE = 13,
	CLUSTER_WAL_DENY_EXTERNAL_CLEANUP_DISABLED = 14,
	CLUSTER_WAL_DENY_OFFLINE_PROVENANCE = 15,
	CLUSTER_WAL_DENY_FORMATION_STALE = 16,
	CLUSTER_WAL_DENY_FENCE_STALE = 17,
	CLUSTER_WAL_DENY_GUARD_STATE = 18,
	CLUSTER_WAL_DENY_RELEASE_UNCERTAIN = 19
} ClusterWalReuseDenyReason;

typedef enum ClusterWalReusePhysicalAction {
	CLUSTER_WAL_PHYSICAL_REMOVE = 1,
	CLUSTER_WAL_PHYSICAL_RECYCLE = 2,
	CLUSTER_WAL_PHYSICAL_REPLACE = 3,
	CLUSTER_WAL_PHYSICAL_RENAME_PARTIAL = 4,
	CLUSTER_WAL_PHYSICAL_CREATE_ABSENT = 5
} ClusterWalReusePhysicalAction;

typedef enum ClusterWalInstallSourceKind {
	CLUSTER_WAL_INSTALL_SOURCE_NONE = 0,
	CLUSTER_WAL_INSTALL_SOURCE_FORK_COPY_TEMP = 1,
	CLUSTER_WAL_INSTALL_SOURCE_ARCHIVE_STAGE = 2
} ClusterWalInstallSourceKind;

typedef enum ClusterWalReuseGuardState {
	CLUSTER_WAL_GUARD_EMPTY = 0,
	CLUSTER_WAL_GUARD_PREFLIGHTED = 1,
	CLUSTER_WAL_GUARD_FENCE_ADMITTED_OR_NA = 2,
	CLUSTER_WAL_GUARD_WALR_X_HELD = 3,
	CLUSTER_WAL_GUARD_ARMED = 4,
	CLUSTER_WAL_GUARD_TERMINAL_DURABLE = 5,
	CLUSTER_WAL_GUARD_BOOKKEPT = 6
} ClusterWalReuseGuardState;

typedef enum ClusterWalReuseAction {
	CLUSTER_WAL_ACTION_RETIRE_RECYCLE_OR_REMOVE = 1,
	CLUSTER_WAL_ACTION_FORCE_REPLACE = 2,
	CLUSTER_WAL_ACTION_RENAME_PARTIAL = 3,
	CLUSTER_WAL_ACTION_ARCHIVE_REPLACE = 4,
	CLUSTER_WAL_ACTION_CREATE_ABSENT = 5
} ClusterWalReuseAction;

typedef enum ClusterWalRootFoldResult {
	CLUSTER_WAL_FOLD_UNKNOWN = 0,
	CLUSTER_WAL_FOLD_UNCONSTRAINED = 1,
	CLUSTER_WAL_FOLD_BOUNDED = 2
} ClusterWalRootFoldResult;

/* One stable configured-membership sample feeds all slots.  configured=false
 * is the sole EMPTY proof.  A configured slot may be consumed only when its
 * owner read returned a primary-authoritative result. */
typedef struct ClusterWalRootFoldInput {
	bool configured;
	ClusterControlRootResult read_result;
	ClusterControlRootSnapshot snapshot;
} ClusterWalRootFoldInput;

typedef struct ClusterWalRootFold {
	ClusterWalRootFoldResult result;
	uint32 nintervals;
	ClusterWalRetentionInterval intervals[CLUSTER_WAL_RETENTION_MAX_THREADS];
	XLogSegNo floor_by_thread[CLUSTER_WAL_RETENTION_MAX_THREADS];
} ClusterWalRootFold;

typedef enum ClusterWalPinResult {
	CLUSTER_WAL_PIN_OK = 0,
	CLUSTER_WAL_PIN_INVALID = 1,
	CLUSTER_WAL_PIN_UNAVAILABLE = 2,
	CLUSTER_WAL_PIN_STALE = 3,
	CLUSTER_WAL_PIN_CAPACITY = 4,
	CLUSTER_WAL_PIN_RELEASE_UNCERTAIN = 5
} ClusterWalPinResult;

typedef enum ClusterWalrReleaseResult {
	CLUSTER_WALR_RELEASE_NOT_HELD = 0,
	CLUSTER_WALR_RELEASE_CONFIRMED = 1,
	CLUSTER_WALR_RELEASE_UNCONFIRMED = 2,
	CLUSTER_WALR_RELEASE_INVALID = 3
} ClusterWalrReleaseResult;

typedef struct ClusterWalRetentionPin ClusterWalRetentionPin;
typedef struct ClusterWalRootPublishGuard ClusterWalRootPublishGuard;

typedef struct ClusterWalRetentionPinThreadRequest {
	const ClusterWalRetentionInterval *intervals;
	uint32 nintervals;
	ClusterRecoveryDutyKey duty;
	ClusterControlRootReadToken root_read;
	const ClusterFormationWitnessV1 *formation;
	const PgracExternalFenceNeedSetV1 *needs;
	const PgracExternalFenceAdmissionSetV1 *admissions;
} ClusterWalRetentionPinThreadRequest;

typedef struct ClusterWalReuseActionGuard {
	uint32 magic;
	uint16 version;
	uint8 state;
	uint8 flags;
	int32 owner_pid;
	uintptr_t self_address;
	ClusterWalReuseEntry entry;
	ClusterWalReuseAction action;
	ClusterWalTerminalOutcome outcome;
	ClusterWalFileIdentity file;
	ClusterWalFileObjectStamp pre_action_stamp;
	ClusterWalInstallSourceKind source_kind;
	intptr_t source_dir_handle;
	intptr_t source_handle;
	intptr_t fork_source_handle;
	ClusterWalFileIdentity source_carrier;
	XLogRecPtr fork_lsn;
	ClusterWalFileObjectStamp source_stamp;
	ClusterWalFileObjectStamp fork_source_stamp;
	XLogRecPtr source_coverage_start;
	XLogRecPtr source_coverage_end;
	uint8 source_sha256[32];
	ClusterRecoveryDutyKey duty;
	ClusterControlRootReadToken root_read;
	const ClusterFormationWitnessV1 *formation;
	ClusterRecoverySerialGuard *serial_or_null;
	ClusterWalRetentionPin *pin_or_null;
	const PgracExternalFenceNeedSetV1 *needs_or_null;
	const PgracExternalFenceAdmissionSetV1 *admissions_or_null;
	ClusterWalrLock walr;
	ResourceOwner owner;
} ClusterWalReuseActionGuard;

typedef struct ClusterWalReuseGuardRequest {
	ClusterWalFileIdentity file;
	ClusterWalReuseEntry entry;
	ClusterWalReuseAction action;
	ClusterWalInstallSourceKind source_kind;
	ClusterWalFileIdentity source_carrier;
	XLogRecPtr fork_lsn;
	XLogRecPtr source_coverage_start;
	XLogRecPtr source_coverage_end;
	ClusterRecoveryDutyKey duty;
	ClusterControlRootReadToken root_read;
	const ClusterFormationWitnessV1 *formation;
} ClusterWalReuseGuardRequest;

StaticAssertDecl(sizeof(ClusterWalFileIdentity) == 16, "ClusterWalFileIdentity must be 16 bytes");
StaticAssertDecl(offsetof(ClusterWalFileIdentity, thread_id) == 0,
				 "ClusterWalFileIdentity thread offset");
StaticAssertDecl(offsetof(ClusterWalFileIdentity, kind) == 2, "ClusterWalFileIdentity kind offset");
StaticAssertDecl(offsetof(ClusterWalFileIdentity, reserved_zero) == 3,
				 "ClusterWalFileIdentity reserved offset");
StaticAssertDecl(offsetof(ClusterWalFileIdentity, tli) == 4, "ClusterWalFileIdentity TLI offset");
StaticAssertDecl(offsetof(ClusterWalFileIdentity, segno) == 8,
				 "ClusterWalFileIdentity segment offset");
StaticAssertDecl(sizeof(ClusterWalRetentionInterval) == 24,
				 "ClusterWalRetentionInterval must be 24 bytes");
StaticAssertDecl(offsetof(ClusterWalRetentionInterval, thread_id) == 0,
				 "ClusterWalRetentionInterval thread offset");
StaticAssertDecl(offsetof(ClusterWalRetentionInterval, tli) == 4,
				 "ClusterWalRetentionInterval TLI offset");
StaticAssertDecl(offsetof(ClusterWalRetentionInterval, start_lsn) == 8,
				 "ClusterWalRetentionInterval start offset");
StaticAssertDecl(offsetof(ClusterWalRetentionInterval, end_lsn) == 16,
				 "ClusterWalRetentionInterval end offset");

extern bool cluster_wal_thread_directory_parse(const char *basename, uint16 *out_thread_id);
extern bool cluster_wal_file_identity_parse(const char *basename, uint16 thread_id,
											int wal_segsz_bytes,
											ClusterWalFileIdentity *out_identity);
extern bool cluster_wal_file_identity_valid(const ClusterWalFileIdentity *identity,
											int wal_segsz_bytes);
extern bool cluster_wal_file_long_header_matches(const ClusterWalFileIdentity *identity,
												 const XLogLongPageHeaderData *header,
												 uint64 system_identifier, int wal_segsz_bytes);
extern bool
cluster_wal_retention_interval_segment_bounds(const ClusterWalRetentionInterval *interval,
											  int wal_segsz_bytes, XLogSegNo *out_first,
											  XLogSegNo *out_last);
extern bool
cluster_wal_retention_interval_intersects_file(const ClusterWalRetentionInterval *interval,
											   const ClusterWalFileIdentity *identity,
											   int wal_segsz_bytes);
extern bool cluster_wal_retention_resid_encode(uint16 thread_id, ClusterResId *out_resid);
extern ClusterWalPinResult
cluster_wal_retention_pin_acquire(const ClusterWalRetentionPinThreadRequest *requests,
								  uint16 nthreads, ClusterWalRetentionPin **out_pin);
extern ClusterWalPinResult
cluster_wal_retention_pin_bind_one(ClusterWalRetentionPin *pin,
								   ClusterRecoverySerialGuard *held_serial);
extern ClusterWalPinResult
cluster_wal_retention_pin_bind_set(ClusterWalRetentionPin *pin,
								   ClusterRecoverySerialGuardSet *held_set);
extern ClusterWalPinResult
cluster_wal_retention_pin_preflight_revalidate_wait_v1(ClusterWalRetentionPin *pin);
extern ClusterWalPinResult cluster_wal_retention_pin_revalidate(ClusterWalRetentionPin *pin);
extern ClusterWalPinResult
cluster_wal_retention_pin_seal_for_root_publish(ClusterWalRetentionPin *pin);
extern ClusterWalPinResult cluster_wal_retention_pin_adopt_root_readback_v1(
	ClusterWalRetentionPin *pin, const ClusterControlRootSnapshot *expected_snapshot,
	const ClusterControlRootReadToken *expected_token,
	const ClusterControlRootSnapshot *observed_snapshot,
	const ClusterControlRootReadToken *observed_token);
extern ClusterWalrReleaseResult cluster_wal_retention_pin_release(ClusterWalRetentionPin **pin);
extern ClusterWalPinResult
cluster_wal_retention_root_publish_begin_exact(const ClusterControlRootReadToken *expected_root,
											   bool require_sealed_pin,
											   ClusterWalRootPublishGuard **out_guard);
extern ClusterWalrReleaseResult
cluster_wal_retention_root_publish_end(ClusterWalRootPublishGuard **guard);
extern ClusterWalReuseGuardResult
cluster_wal_retention_e1_coarse_begin(ClusterWalRetentionE1Context *context, uint16 thread_id,
									  ClusterWalRootFoldResult *out_fold_result,
									  XLogSegNo *out_floor_segno,
									  ClusterWalReuseDenyReason *out_reason);
extern ClusterWalrReleaseResult
cluster_wal_retention_e1_coarse_release(ClusterWalRetentionE1Context *context,
										ClusterWalReuseDenyReason *out_reason);
extern ClusterWalReuseGuardResult cluster_wal_retention_e1_preflight(
	ClusterWalRetentionE1Context *context, const ClusterWalFileIdentity *file,
	ClusterWalReuseActionGuard *guard, PgracExternalFenceNeedSetV1 **out_needs,
	ClusterWalReuseDenyReason *out_reason);
extern bool cluster_wal_retention_active_pin_present(void);
extern ClusterWalReuseGuardResult
cluster_wal_retention_action_begin(ClusterWalRetentionE1Context *context, uint16 thread_id,
								   ClusterWalReuseDenyReason *out_reason);
extern ClusterWalReuseGuardResult cluster_wal_retention_action_preflight(
	ClusterWalRetentionE1Context *context, const ClusterWalFileIdentity *file,
	ClusterWalReuseEntry entry, ClusterWalReuseActionGuard *guard,
	PgracExternalFenceNeedSetV1 **out_needs, ClusterWalReuseDenyReason *out_reason);
extern void cluster_wal_retention_action_finish(ClusterWalRetentionE1Context *context);
extern ClusterWalReuseGuardResult cluster_wal_retention_e1_fence_wait(
	ClusterWalRetentionE1Context *context, ClusterWalReuseActionGuard *guard,
	PgracExternalFenceNeedSetV1 *needs, PgracExternalFenceAdmissionSetV1 **out_admissions,
	ClusterWalReuseDenyReason *out_reason);
extern void cluster_wal_retention_e1_finish(ClusterWalRetentionE1Context *context);
extern ClusterWalReuseGuardResult
cluster_wal_reuse_guard_init(ClusterWalReuseActionGuard *guard,
							 ClusterWalReuseDenyReason *out_reason);
extern ClusterWalReuseGuardResult cluster_wal_reuse_guard_preflight(
	ClusterWalReuseActionGuard *guard, const ClusterWalReuseGuardRequest *request,
	PgracExternalFenceNeedSetV1 **out_needs, ClusterWalReuseDenyReason *out_reason);
extern ClusterWalReuseGuardResult cluster_wal_reuse_guard_preflight_active_recovery(
	ClusterWalReuseActionGuard *guard, const ClusterWalFileIdentity *file,
	ClusterWalReuseEntry entry, ClusterRecoverySerialGuard **out_serial,
	ClusterWalRetentionPin **out_pin, ClusterWalReuseDenyReason *out_reason);
extern ClusterWalReuseGuardResult cluster_wal_reuse_guard_fence_admitted_nowait(
	ClusterWalReuseActionGuard *guard, const PgracExternalFenceAdmissionSetV1 *admissions_or_null,
	ClusterWalReuseDenyReason *out_reason);
extern ClusterWalReuseGuardResult cluster_wal_reuse_guard_arm(
	ClusterWalReuseActionGuard *guard, ClusterRecoverySerialGuard *held_serial_or_null,
	ClusterWalRetentionPin *held_pin_or_null, ClusterWalReuseDenyReason *out_reason);
extern ClusterWalReuseGuardResult
cluster_wal_reuse_guard_l3_begin(ClusterWalReuseActionGuard *guard,
								 ClusterWalReusePhysicalAction physical,
								 ClusterWalReuseDenyReason *out_reason);
extern ClusterWalReuseGuardResult
cluster_wal_reuse_guard_note_zero_mutation(ClusterWalReuseActionGuard *guard,
										   ClusterWalReusePhysicalAction physical,
										   ClusterWalReuseDenyReason *out_reason);
extern ClusterWalReuseGuardResult
cluster_wal_reuse_guard_confirm_zero_mutation(ClusterWalReuseActionGuard *guard,
											  ClusterWalReusePhysicalAction physical,
											  ClusterWalReuseDenyReason *out_reason);
extern ClusterWalReuseGuardResult
cluster_wal_reuse_guard_terminal_durable(ClusterWalReuseActionGuard *guard,
										 ClusterWalTerminalOutcome outcome,
										 ClusterWalReuseDenyReason *out_reason);
extern ClusterWalReuseGuardResult
cluster_wal_reuse_guard_remove(ClusterWalReuseActionGuard *guard,
							   ClusterWalReuseDenyReason *out_reason);
extern ClusterWalReuseGuardResult
cluster_wal_reuse_guard_recycle(ClusterWalReuseActionGuard *guard,
								const ClusterWalFileIdentity *destination,
								ClusterWalReuseDenyReason *out_reason);
extern ClusterWalReuseGuardResult
cluster_wal_reuse_guard_bookkeep(ClusterWalReuseActionGuard *guard,
								 ClusterWalReuseDenyReason *out_reason);
extern ClusterWalrReleaseResult
cluster_wal_reuse_guard_finish(ClusterWalReuseActionGuard *guard,
							   ClusterWalTerminalOutcome *out_outcome,
							   ClusterWalReuseDenyReason *out_reason);
extern bool cluster_wal_retention_fold_validated_roots(const ClusterWalRootFoldInput *inputs,
													   uint16 nslots, int wal_segsz_bytes,
													   ClusterWalRootFold *out_fold);

#endif /* CLUSTER_WAL_RETENTION_H */
