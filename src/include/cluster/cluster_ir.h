/*-------------------------------------------------------------------------
 *
 * cluster_ir.h
 *	  STOP03 recovery serialization over the existing IR enqueue class.
 *
 *	  RF-ROOT P3 binds the compact IR(X) resource to
 *	  {origin_thread_id, root_lineage_seq}; every request and held guard also
 *	  retains the exact full ClusterRecoveryDutyKey plus the borrowed formation,
 *	  NeedSet, and AdmissionSet evidence.  Compact-key collisions are conservative:
 *	  full-duty revalidation is still mandatory before and while mutating.
 *
 *	  Exclusive/dontwait competition admits only an actual GES holder.  There is
 *	  no native, node-count, coordinator, local-epoch, or deterministic-survivor
 *	  success path.  A guard covers mutation, durability, and immutable proof
 *	  publication; confirmed typed release is required before root finalization.
 *	  Uncertain release keeps the guard live for process/GRD cleanup.
 *
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/include/cluster/cluster_ir.h
 *
 * NOTES
 *	  This is a pgrac-original file (no derivation from PostgreSQL).
 *	  Spec: spec-s8-stop-03-root-serialization.md §17 (frozen)
 *
 *-------------------------------------------------------------------------
 */
#ifndef CLUSTER_IR_H
#define CLUSTER_IR_H

#include "cluster/cluster_control_root.h" /* ClusterRecoveryDutyKey */
#include "cluster/cluster_grd.h"		  /* ClusterResId */
#include "cluster/cluster_hw.h"			  /* CLUSTER_HW_RESID_TYPE (collision check) */
#include "cluster/cluster_dl.h"			  /* CLUSTER_DL_RESID_TYPE (collision check) */
#include "cluster/cluster_xlog.h"		  /* CLUSTER_WAL_THREAD_MAX */
#include "storage/lock.h"				  /* LOCKTAG_LAST_TYPE */

/*
 * CLUSTER_IR_RESID_TYPE -- IR resource-id namespace marker.  Above every PG
 * LockTagType, distinct from SQ (0xF0) / CF (0xF1) / HW (0xF2) / DL (0xF3) /
 * TT (0xF4).
 */
#define CLUSTER_IR_RESID_TYPE 0xF5

StaticAssertDecl(CLUSTER_IR_RESID_TYPE > LOCKTAG_LAST_TYPE,
				 "IR resid namespace must not collide with any PG LockTagType");
StaticAssertDecl(CLUSTER_IR_RESID_TYPE != CLUSTER_HW_RESID_TYPE,
				 "IR and HW resid namespaces must be distinct");
StaticAssertDecl(CLUSTER_IR_RESID_TYPE != CLUSTER_DL_RESID_TYPE,
				 "IR and DL resid namespaces must be distinct");
/* The other siblings -- SQ (0xF0), CF (0xF1), and TT (0xF4) -- are defined in
 * headers this one does not pull in.  test_cluster_recovery_serial asserts the
 * full SQ/CF/HW/DL distinctness at unit-test link time. */

/* RF-ROOT P3 / STOP03 §17.2: compact IR(X) key derived only from a valid
 * full recovery-duty identity.  Invalid input preserves *out. */
extern bool cluster_recovery_serial_resid_encode(const ClusterRecoveryDutyKey *duty,
												 ClusterResId *out);

#ifndef FRONTEND

#include "cluster/cluster_lock_acquire.h" /* ClusterLockAcquireRequest */

typedef struct ClusterFormationWitnessV1 ClusterFormationWitnessV1;
typedef struct PgracExternalFenceNeedSetV1 PgracExternalFenceNeedSetV1;
typedef struct PgracExternalFenceAdmissionSetV1 PgracExternalFenceAdmissionSetV1;

typedef enum ClusterRecoverySerialMode {
	CLUSTER_RECOVERY_SERIAL_ONLINE = 1,
	CLUSTER_RECOVERY_SERIAL_COLD_FORMED = 2
} ClusterRecoverySerialMode;

typedef enum ClusterRecoverySerialAcquireResult {
	CLUSTER_RECOVERY_SERIAL_GRANTED = 0,
	CLUSTER_RECOVERY_SERIAL_BUSY = 1,
	CLUSTER_RECOVERY_SERIAL_RETRY = 2,
	CLUSTER_RECOVERY_SERIAL_STALE = 3,
	CLUSTER_RECOVERY_SERIAL_ROOT_UNAVAILABLE = 4,
	CLUSTER_RECOVERY_SERIAL_FENCE_DENIED = 5,
	CLUSTER_RECOVERY_SERIAL_CAPABILITY_UNAVAILABLE = 6,
	CLUSTER_RECOVERY_SERIAL_INTERNAL_FAILURE = 7
} ClusterRecoverySerialAcquireResult;

typedef enum ClusterRecoverySerialRevalidateResult {
	CLUSTER_RECOVERY_SERIAL_CURRENT = 0,
	CLUSTER_RECOVERY_SERIAL_NOT_HELD = 1,
	CLUSTER_RECOVERY_SERIAL_MEMBERSHIP_STALE = 2,
	CLUSTER_RECOVERY_SERIAL_FENCE_STALE = 3,
	CLUSTER_RECOVERY_SERIAL_CAPABILITY_STALE = 4,
	CLUSTER_RECOVERY_SERIAL_RELEASE_UNCERTAIN = 5
} ClusterRecoverySerialRevalidateResult;

typedef enum ClusterRecoverySerialReleaseResult {
	CLUSTER_RECOVERY_SERIAL_RELEASE_NOT_HELD = 0,
	CLUSTER_RECOVERY_SERIAL_RELEASE_CONFIRMED = 1,
	CLUSTER_RECOVERY_SERIAL_RELEASE_UNCONFIRMED = 2,
	CLUSTER_RECOVERY_SERIAL_RELEASE_INVALID = 3
} ClusterRecoverySerialReleaseResult;

typedef struct ClusterRecoverySerialRequest {
	ClusterRecoverySerialMode mode;
	ClusterRecoveryDutyKey duty;
	ClusterControlRootReadToken expected_root_token;
	const ClusterFormationWitnessV1 *formation;
	const PgracExternalFenceNeedSetV1 *fence_need_set;
	const PgracExternalFenceAdmissionSetV1 *fence_admission_set;
	int32 acquire_timeout_ms;
	int32 release_timeout_ms;
} ClusterRecoverySerialRequest;

typedef struct ClusterRecoverySerialGuard {
	bool held;
	bool release_uncertain;
	ClusterRecoverySerialMode mode;
	ClusterResId resid;
	ClusterRecoveryDutyKey duty;
	ClusterControlRootReadToken root_read_token;
	const ClusterFormationWitnessV1 *formation;
	const PgracExternalFenceNeedSetV1 *fence_need_set;
	const PgracExternalFenceAdmissionSetV1 *fence_admission_set;
	ClusterLockAcquireRequest lock_request;
	int32 release_timeout_ms;
} ClusterRecoverySerialGuard;

#define CLUSTER_RECOVERY_SERIAL_SET_MAX CLUSTER_WAL_THREAD_MAX
#define CLUSTER_RECOVERY_SERIAL_SET_FAILED_NONE UINT16_MAX

typedef struct ClusterRecoverySerialGuardSet {
	uint16 count;
	int32 release_timeout_ms;
	ClusterRecoverySerialGuard guards[CLUSTER_RECOVERY_SERIAL_SET_MAX];
} ClusterRecoverySerialGuardSet;

extern ClusterRecoverySerialAcquireResult
cluster_recovery_serial_acquire(const ClusterRecoverySerialRequest *request,
								ClusterRecoverySerialGuard *guard);
extern ClusterRecoverySerialRevalidateResult
cluster_recovery_serial_revalidate(ClusterRecoverySerialGuard *guard);
extern ClusterRecoverySerialReleaseResult
cluster_recovery_serial_release(ClusterRecoverySerialGuard *guard);
extern ClusterRecoverySerialAcquireResult
cluster_recovery_serial_acquire_set(const ClusterRecoverySerialRequest *requests, uint16 count,
									int overall_acquire_timeout_ms,
									ClusterRecoverySerialGuardSet *set, uint16 *failed_index);
extern ClusterRecoverySerialReleaseResult
cluster_recovery_serial_release_set(ClusterRecoverySerialGuardSet *set);

/* Minimal shmem region for the ten STOP03 §10.3 volatile counters. */
extern Size cluster_ir_shmem_size(void);
extern void cluster_ir_shmem_init(void);
extern void cluster_ir_shmem_register(void);

/* Observability only; never authority (dump_ir / pg_cluster_state). */
extern uint64 cluster_recovery_serial_grant_count(void);
extern uint64 cluster_recovery_serial_busy_count(void);
extern uint64 cluster_recovery_serial_retry_count(void);
extern uint64 cluster_recovery_serial_revalidate_reject_count(void);
extern uint64 cluster_recovery_serial_node_cleanup_wait_count(void);
extern uint64 cluster_recovery_serial_release_confirmed_count(void);
extern uint64 cluster_recovery_serial_release_unconfirmed_count(void);
extern uint64 cluster_recovery_serial_cold_set_grant_count(void);
extern uint64 cluster_recovery_serial_capability_denied_count(void);
extern uint64 cluster_recovery_serial_native_result_rejected_count(void);

#endif /* !FRONTEND */

#endif /* CLUSTER_IR_H */
