/*-------------------------------------------------------------------------
 *
 * test_cluster_reconfig.c
 *	  spec-2.29 Sprint A Step 1 unit tests — cluster_reconfig foundation.
 *
 *	  Step 1 cases (this binary):
 *	    T-reconfig-1  ReconfigEvent + ClusterReconfigState sizeof bounds
 *	                  (P2.8 — natural-aligned, StaticAssertDecl ≤ 96 ≥ 64);
 *	                  cluster_reconfig_shmem_size > 0 + shmem_init succeeds
 *	                  + idempotent (init twice safe via found-flag);
 *	                  CLUSTER_RECONFIG_DEAD_BITMAP_BYTES == 16
 *	    T-reconfig-9  cluster_epoch_observe_remote CAS-loop semantics:
 *	                  - initial epoch=0, observe_remote(7) → epoch=7, returns true
 *	                  - observe_remote(7) again → epoch stays 7, returns false
 *	                  - observe_remote(3) (stale) → epoch stays 7, returns false
 *	                  - observe_remote(10) → epoch=10, returns true
 *	                  - CLUSTER_EPOCH_OBSERVE_MAX_JUMP == 16 constant
 *
 *	  Step 2 / Step 3 add T-reconfig-2..8 + T-reconfig-10/11
 *	  (event_id dedup / Q2 A'' rule / mid-tick rotation / PROCSIG handler
 *	  triplet / broadcast-vs-epoch++ split / I6 commit-durable guard /
 *	  envelope tri-branch / declared-peer filter end-to-end).
 *
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/test/cluster_unit/test_cluster_reconfig.c
 *
 * NOTES
 *	  pgrac-original file.  Spec:  spec-2.29-reconfig-coordinator-
 *	  internal.md (DRAFT v0.3).
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <stddef.h>
#include <string.h>
#include <sys/wait.h>
#include <unistd.h>

#include "cluster/cluster_reconfig.h"
#include "cluster/cluster_clean_leave.h"
#include "cluster/cluster_external_fence.h"
#include "cluster/cluster_recovery_duty.h"
#include "cluster/cluster_thread_recovery.h"
#include "cluster/cluster_epoch.h"
#include "cluster/cluster_qvotec.h"
#include "cluster/cluster_ic.h"
#include "cluster/cluster_ic_router.h"
#include "cluster/cluster_ic_tier1.h"
#include "cluster/cluster_lms.h"
#include "cluster/cluster_sf_dep.h"
#include "cluster/cluster_startup_phase.h"
#include "cluster/cluster_wal_state.h"
#include "cluster/cluster_write_fence.h" /* spec-4.12 D4 marker submit stubs */

#undef printf
#undef fprintf
#undef snprintf
#undef sprintf
#undef vsnprintf
#undef vfprintf
#undef vprintf
#undef vsprintf
#undef strerror
#undef strerror_r

#include "unit_test.h"

UT_DEFINE_GLOBALS();

static ClusterReplacementEpisode ut_admitted_replacement_episode(
	int32 target_node_id);
static bool jb_test(const uint8 *bmp, int i);
static void ut_join_setup(void);
static void ut_set_self_incarnation_sequence(uint64 first, uint64 second,
											 uint64 later);
static bool ut_join_qvotec_poll_write_pending(int32 *target_node_out,
											  void *write_slot512_out);
static void ut_join_qvotec_complete_write(bool acked);

extern bool cluster_reconfig_qvotec_lifecycle_transition(
	ClusterQvotecMailbox *authority_mailbox,
	pg_atomic_uint32 *qvotec_status, ClusterQvotecStatus next_status);

void
cluster_qvotec_mailbox_restart_reset(ClusterQvotecMailbox *mailbox)
{
	if (mailbox == NULL)
		return;
	memset(mailbox, 0, sizeof(*mailbox));
	pg_atomic_init_u64(&mailbox->request_seq, 0);
	pg_atomic_init_u64(&mailbox->completion_seq, 0);
	pg_atomic_init_u32(&mailbox->request_opcode,
					   CLUSTER_QVOTEC_MAILBOX_NONE);
	pg_atomic_init_u32(&mailbox->completion_result,
					   CLUSTER_QVOTEC_MAILBOX_RESULT_NONE);
}

/* spec-5.22e D5-8 stub: cluster_membership_set_state notes self-admission
 * into the undo horizon shmem (cluster_undo_horizon_ic.c not linked here);
 * also satisfies the cluster_node_id extern via cluster_guc.o linkage or
 * local definition in this binary. */
static bool ut_self_member_callback_seen;
static ClusterJoinGateVerdict ut_self_member_callback_gate;

void cluster_undo_horizon_note_self_member(void);
void
cluster_undo_horizon_note_self_member(void)
{
	ut_self_member_callback_seen = true;
	ut_self_member_callback_gate = cluster_reconfig_self_join_gate_verdict();
}

/* spec-2.29a: cluster_reconfig.c gates the async marker stage on
 * MyBackendType == B_LMON.  The fixture exercises the pre-2.29a bounded-wait
 * semantics (write_fence submit stub returns FAILED synchronously), so run as
 * a non-LMON backend; the async FSM itself is covered by
 * test_cluster_marker_async.c. */
#include "miscadmin.h"
BackendType MyBackendType = B_INVALID;
bool IsUnderPostmaster = false;

/* Spec-5.15A phase-3 target-LMON sender controls.  The prerequisite provider
 * stays a production dependency of cluster_reconfig.c; this standalone unit
 * supplies its current instantaneous value without retaining it in product
 * state. */
static ClusterR4PrerequisiteSnapshot ut_r4_prerequisite_snapshot;
static bool ut_r4_snapshot_use_reconfig;
static ClusterICSendResult ut_phase3_send_result = CLUSTER_IC_SEND_DONE;
static int ut_phase3_send_calls;
static uint8 ut_phase3_send_msg_type;
static int32 ut_phase3_send_dest;
static uint32 ut_phase3_send_length;
static uint8 ut_phase3_send_bytes[CLUSTER_REPLACEMENT_WIRE_BYTES];
static int ut_phase3_close_calls;
static int32 ut_phase3_close_peer;
static bool ut_candidate2_capable[CLUSTER_MAX_NODES];
static bool ut_owner_rejoin_result = true;
static int ut_owner_rejoin_calls;
static int32 ut_owner_rejoin_node;
static uint64 ut_owner_rejoin_incarnation;
static ClusterControlRootResult ut_recovery_root_result;
static ClusterControlRootIdentity ut_recovery_root_identity;
static bool ut_rejoin_root_complete;
static bool ut_external_fence_active;
static bool ut_authority_managed;
static ClusterAuthorityReadiness ut_authority_readiness
	= CLUSTER_AUTHORITY_OFF;
static uint64 ut_handoff_identity_incarnation;
static uint64 ut_handoff_identity_predecessor_floor;
static bool ut_serving_ready;
static uint64 ut_lms_generation = UINT64_C(1);
static bool ut_rejoin_grd_clear_ready;
static int ut_rejoin_start_calls;
static int ut_rejoin_poll_calls;
static int ut_rejoin_clear_build_calls;
static int ut_rejoin_authorize_calls;
static int ut_rejoin_refresh_calls;
static int ut_rejoin_root_revalidate_calls;
static int ut_rejoin_consume_calls;
static int ut_rejoin_release_calls;
static bool ut_rejoin_consume_saw_prior_marker_submit;
static PgracExternalFenceRejoinStatus ut_rejoin_poll_status;
static PgracExternalFenceRejoinOfferV1 ut_rejoin_offer;
static ClusterReconfigRejoinFailureSnapshotV1 ut_rejoin_failure_seen;
static ClusterReconfigRejoinPendingSnapshotV1 ut_rejoin_pending_seen;
static ClusterJoinCommitMarker ut_rejoin_committed_seen;
static char ut_rejoin_op_storage;
static char ut_rejoin_clear_storage;

ClusterControlRootResult
cluster_control_root_lookup_owner_by_node_runtime(
	int32 old_node_id pg_attribute_unused(),
	ClusterControlRootIdentity *out_identity,
	ClusterControlRootSnapshot *out_snapshot,
	ClusterControlRootReadToken *out_token)
{
	if (out_identity != NULL)
		*out_identity = ut_recovery_root_identity;
	if (out_snapshot != NULL)
	{
		memset(out_snapshot, 0, sizeof(*out_snapshot));
		out_snapshot->identity = ut_recovery_root_identity;
		out_snapshot->lifecycle = ut_rejoin_root_complete
			? CLUSTER_CONTROL_ROOT_LIFECYCLE_RECOVERY_COMPLETE
			: CLUSTER_CONTROL_ROOT_LIFECYCLE_RECOVERY_REQUIRED;
	}
	if (out_token != NULL)
		memset(out_token, 0, sizeof(*out_token));
	return ut_recovery_root_result;
}

bool
cluster_external_fence_runtime_active(void)
{
	return ut_external_fence_active;
}

bool
cluster_authority_readiness_managed(void)
{
	return ut_authority_managed;
}

ClusterAuthorityReadiness
cluster_authority_readiness_get(void)
{
	return ut_authority_readiness;
}

bool
cluster_authority_handoff_identity_current(
	uint64 expected_self_incarnation, uint64 expected_predecessor_floor)
{
	return ut_authority_managed
		&& ut_authority_readiness == CLUSTER_AUTHORITY_OFF
		&& expected_self_incarnation == ut_handoff_identity_incarnation
		&& expected_predecessor_floor
			   == ut_handoff_identity_predecessor_floor;
}

bool
cluster_serving_ready_is_current(void)
{
	return ut_serving_ready;
}

bool
cluster_authority_serving_rebind_lmon(void)
{
	return ut_serving_ready;
}

/* RF-ROOT P6 (L5 leaver serving rebind): cluster_reconfig.o references the
 * leaver-side authority rebind wrapper; the pure unit pins it to the same
 * serving-readiness gate as the LMON rebind above. */
bool
cluster_authority_serving_rebind_leaver(void)
{
	return ut_serving_ready;
}

uint64
cluster_lms_get_lms_restart_generation(void)
{
	return ut_lms_generation;
}

bool
cluster_external_fence_rejoin_protected_set_digest(uint8 out[32])
{
	if (out == NULL)
		return false;
	memset(out, 0x5a, 32);
	return true;
}

PgracExternalFenceRejoinStatus
cluster_external_fence_rejoin_start_async(
	int timeout_ms, PgracExternalFenceRejoinOpV1 **out_op)
{
	ut_rejoin_start_calls++;
	if (ut_rejoin_start_calls > 1 ||
		timeout_ms != PGRAC_EXTERNAL_FENCE_ACQUIRE_TIMEOUT_DEFAULT_MS ||
		out_op == NULL || *out_op != NULL)
		return PGRAC_EXTERNAL_FENCE_REJOIN_UNAVAILABLE;
	*out_op = (PgracExternalFenceRejoinOpV1 *) &ut_rejoin_op_storage;
	ut_rejoin_poll_status = PGRAC_EXTERNAL_FENCE_REJOIN_OFFERED;
	return PGRAC_EXTERNAL_FENCE_REJOIN_PENDING;
}

PgracExternalFenceRejoinStatus
cluster_external_fence_rejoin_poll_nowait(
	PgracExternalFenceRejoinOpV1 *op, PgracExternalFenceDenyReason *reason)
{
	ut_rejoin_poll_calls++;
	if (reason != NULL)
		*reason = PGRAC_EXTERNAL_FENCE_DENY_NONE;
	if (op != (PgracExternalFenceRejoinOpV1 *) &ut_rejoin_op_storage)
		return PGRAC_EXTERNAL_FENCE_REJOIN_UNAVAILABLE;
	return ut_rejoin_poll_status;
}

const PgracExternalFenceRejoinOfferV1 *
cluster_external_fence_rejoin_offer(
	const PgracExternalFenceRejoinOpV1 *op)
{
	return op == (const PgracExternalFenceRejoinOpV1 *) &ut_rejoin_op_storage
		? &ut_rejoin_offer : NULL;
}

bool
cluster_grd_rejoin_clear_snapshot(
	const ClusterReconfigRejoinFailureSnapshotV1 *failure,
	ClusterGrdRejoinClearSnapshotV1 *out_clear)
{
	if (!ut_rejoin_grd_clear_ready || failure == NULL || out_clear == NULL)
		return false;
	memset(out_clear, 0, sizeof(*out_clear));
	out_clear->episode_epoch = failure->new_epoch;
	out_clear->dead_bitmap_hash = UINT64_C(0x1122334455667788);
	memcpy(out_clear->survivor_bitmap, failure->survivor_bitmap,
		   sizeof(out_clear->survivor_bitmap));
	return true;
}

PgracExternalFenceRejoinStatus
cluster_external_fence_rejoin_authority_clear_build(
	const PgracExternalFenceRejoinOpV1 *op,
	const ClusterReconfigRejoinFailureSnapshotV1 *failure,
	const ClusterGrdRejoinClearSnapshotV1 *grd_clear,
	PgracExternalFenceRejoinAuthorityClearV1 **out_clear,
	PgracExternalFenceDenyReason *reason)
{
	ut_rejoin_clear_build_calls++;
	if (reason != NULL)
		*reason = PGRAC_EXTERNAL_FENCE_DENY_NONE;
	if (op != (const PgracExternalFenceRejoinOpV1 *) &ut_rejoin_op_storage ||
		failure == NULL || grd_clear == NULL || out_clear == NULL ||
		*out_clear != NULL)
		return PGRAC_EXTERNAL_FENCE_REJOIN_UNAVAILABLE;
	ut_rejoin_failure_seen = *failure;
	*out_clear = (PgracExternalFenceRejoinAuthorityClearV1 *)
		&ut_rejoin_clear_storage;
	return PGRAC_EXTERNAL_FENCE_REJOIN_WAITING_ROOT;
}

PgracExternalFenceRejoinStatus
cluster_external_fence_rejoin_authorize_on_async(
	PgracExternalFenceRejoinOpV1 *op,
	PgracExternalFenceRejoinAuthorityClearV1 **clear,
	const ClusterControlRootIdentity *identity,
	const ClusterControlRootSnapshot *snapshot,
	const ClusterControlRootReadToken *token,
	const uint8 protected_set_digest[32],
	PgracExternalFenceDenyReason *reason)
{
	ut_rejoin_authorize_calls++;
	if (reason != NULL)
		*reason = PGRAC_EXTERNAL_FENCE_DENY_NONE;
	if (op != (PgracExternalFenceRejoinOpV1 *) &ut_rejoin_op_storage ||
		clear == NULL ||
		*clear != (PgracExternalFenceRejoinAuthorityClearV1 *)
			&ut_rejoin_clear_storage || identity == NULL || snapshot == NULL ||
		token == NULL || protected_set_digest == NULL)
		return PGRAC_EXTERNAL_FENCE_REJOIN_UNAVAILABLE;
	*clear = NULL;
	ut_rejoin_poll_status = PGRAC_EXTERNAL_FENCE_REJOIN_WAITING_JOINER;
	return PGRAC_EXTERNAL_FENCE_REJOIN_PENDING;
}

PgracExternalFenceRejoinStatus
cluster_external_fence_rejoin_refresh_on_async(
	PgracExternalFenceRejoinOpV1 *op,
	const ClusterReconfigRejoinPendingSnapshotV1 *pending,
	PgracExternalFenceDenyReason *reason)
{
	ut_rejoin_refresh_calls++;
	if (reason != NULL)
		*reason = PGRAC_EXTERNAL_FENCE_DENY_NONE;
	if (op != (PgracExternalFenceRejoinOpV1 *) &ut_rejoin_op_storage ||
		pending == NULL)
		return PGRAC_EXTERNAL_FENCE_REJOIN_UNAVAILABLE;
	ut_rejoin_pending_seen = *pending;
	ut_rejoin_poll_status = PGRAC_EXTERNAL_FENCE_REJOIN_READY;
	return PGRAC_EXTERNAL_FENCE_REJOIN_PENDING;
}

bool
cluster_external_fence_rejoin_revalidate_root(
	PgracExternalFenceRejoinOpV1 *op,
	ClusterControlRootSnapshot *out_snapshot,
	PgracExternalFenceDenyReason *reason)
{
	ut_rejoin_root_revalidate_calls++;
	if (reason != NULL)
		*reason = PGRAC_EXTERNAL_FENCE_DENY_NONE;
	if (op != (PgracExternalFenceRejoinOpV1 *) &ut_rejoin_op_storage ||
		out_snapshot == NULL)
		return false;
	memset(out_snapshot, 0, sizeof(*out_snapshot));
	out_snapshot->lifecycle = CLUSTER_CONTROL_ROOT_LIFECYCLE_RECOVERY_COMPLETE;
	return true;
}

bool
cluster_external_fence_rejoin_consume_nowait(
	PgracExternalFenceRejoinOpV1 *op,
	const ClusterReconfigRejoinPendingSnapshotV1 *pending,
	const ClusterJoinCommitMarker *marker,
	PgracExternalFenceDenyReason *reason)
{
	ClusterJoinMarkerMailboxOperationV1 operation;
	int32 target = -1;
	uint8 slot[512];

	ut_rejoin_consume_calls++;
	if (reason != NULL)
		*reason = PGRAC_EXTERNAL_FENCE_DENY_NONE;
	ut_rejoin_consume_saw_prior_marker_submit =
		cluster_reconfig_join_qvotec_poll_pending(&operation, &target, slot);
	if (op != (PgracExternalFenceRejoinOpV1 *) &ut_rejoin_op_storage ||
		pending == NULL || marker == NULL)
		return false;
	ut_rejoin_pending_seen = *pending;
	ut_rejoin_committed_seen = *marker;
	return true;
}

void
cluster_external_fence_rejoin_release(PgracExternalFenceRejoinOpV1 **op)
{
	if (op != NULL && *op != NULL)
	{
		ut_rejoin_release_calls++;
		*op = NULL;
	}
}

void
cluster_external_fence_rejoin_authority_clear_release(
	PgracExternalFenceRejoinAuthorityClearV1 **clear)
{
	if (clear != NULL)
		*clear = NULL;
}

bool
cluster_recovery_owner_rejoin_v1(int32 node_id, uint64 admitted_incarnation)
{
	ut_owner_rejoin_calls++;
	ut_owner_rejoin_node = node_id;
	ut_owner_rejoin_incarnation = admitted_incarnation;
	return ut_owner_rejoin_result;
}

ClusterR4PrerequisiteSnapshot
cluster_undo_block0_r4_prerequisite_snapshot(void)
{
	if (ut_r4_snapshot_use_reconfig)
		return cluster_reconfig_r4_prerequisite_snapshot();
	return ut_r4_prerequisite_snapshot;
}

ClusterICSendResult
cluster_ic_send_envelope(uint8 msg_type, int32 dest_node_id, const void *payload,
						 uint32 payload_len)
{
	ut_phase3_send_calls++;
	ut_phase3_send_msg_type = msg_type;
	ut_phase3_send_dest = dest_node_id;
	ut_phase3_send_length = payload_len;
	if (payload != NULL && payload_len == sizeof(ut_phase3_send_bytes))
		memcpy(ut_phase3_send_bytes, payload, payload_len);
	return ut_phase3_send_result;
}

void
cluster_ic_tier1_close_peer(int32 peer_id, const char *reason pg_attribute_unused())
{
	ut_phase3_close_calls++;
	ut_phase3_close_peer = peer_id;
}

bool
cluster_sf_peer_capability_family_sample(
	int32 peer_id, uint32 required_capabilities,
	uint32 optional_capabilities, bool *optional_supported_out,
	uint32 *generation_out)
{
	if (optional_supported_out != NULL)
		*optional_supported_out = false;
	if (generation_out != NULL)
		*generation_out = 0;
	if (peer_id < 0 || peer_id >= CLUSTER_MAX_NODES
		|| required_capabilities
			   != PGRAC_IC_HELLO_CAP_CANDIDATE2_CORRECTED_A1_V1
		|| optional_capabilities != 0 || !ut_candidate2_capable[peer_id])
		return false;
	if (generation_out != NULL)
		*generation_out = UINT32_C(7);
	return true;
}


/* ============================================================
 * Stubs — link cluster_reconfig.o + cluster_epoch.o standalone.
 *
 *	cluster_reconfig.c (Step 2 body) now references:
 *	  - cluster_conf_lookup_node (declared-peer filter F11)
 *	  - cluster_cssd_get_peer_state (CSSD survivor SSOT P1.1)
 *	  - cluster_cssd_get_dead_generation (P1.2 hash input)
 *	  - cluster_qvotec_in_quorum (I2 in_quorum gate)
 *	  - cluster_node_id (extern int)
 *	  - cluster_enabled (extern bool)
 *	  - IsTransactionState (D4 I6 absorb)
 *	  - GetTopTransactionIdIfAny (D4 writable-tx guard)
 *	  - GetXLogInsertRecPtr (epoch_changed_at_lsn stamp)
 *	  - GetCurrentTimestamp (event applied_at)
 *	  - BackendIdGetProc / SendProcSignal / MaxBackends / MyProcPid
 *	  - cluster_reconfig_start_pending (handler-set sig_atomic_t)
 *	  - cluster_injection_* (D10 injection point callsites)
 *
 *	Unit-test scope: T-2 (compute_event_id determinism), T-3 (publish
 *	dedup via lmon_tick gated path), T-7 (broadcast vs epoch++ split
 *	semantics — verified at compute layer), T-8 (D4 I6 IsTransactionState
 *	absorb path).  T-4/4b/5/5b/6 are best covered by TAP 099 (Step 5)
 *	+ cluster_signal unit T6 (existing).
 * ============================================================ */

#include "storage/shmem.h"
/* spec-5.13 D3 grew ClusterReconfigState with clean_departed_epoch[CLUSTER_MAX_NODES]
 * (1 KiB) + clean_departed_bitmap + counter; spec-5.15 D2 added the membership SSOT
 * table (last_admitted_incarnation[CLUSTER_MAX_NODES] 1 KiB + membership_state[] +
 * pending_join_bitmap + self_join_admitted) plus the D1 observed-slot snapshot
 * (observed_incarnation[] + observed_generation[], 2 KiB).  spec-5.15 H1.3 added
 * observed_fresh_alive[] and spec-5.16 added observed_committed_join_incarnation[] +
 * observed_committed_join_epoch[] (3 more pg_atomic_uint64[CLUSTER_MAX_NODES=128]
 * arrays, +3 KiB).  Bump the mock backing store to fit the grown state struct. */
static char reconfig_shmem_storage[16384] __attribute__((aligned(64)));
static char epoch_shmem_storage[64] __attribute__((aligned(64)));
static bool reconfig_init_done = false;
static bool epoch_init_done = false;

void *
ShmemInitStruct(const char *name, Size size pg_attribute_unused(), bool *foundPtr)
{
	if (strcmp(name, "pgrac cluster reconfig") == 0) {
		*foundPtr = reconfig_init_done;
		reconfig_init_done = true;
		return reconfig_shmem_storage;
	} else if (strcmp(name, "pgrac cluster epoch") == 0) {
		*foundPtr = epoch_init_done;
		epoch_init_done = true;
		return epoch_shmem_storage;
	}
	*foundPtr = false;
	return NULL;
}

#include "storage/lwlock.h"
static bool ut_lwlock_conditional_result = true;
static int ut_lwlock_blocking_calls = 0;
static int ut_lwlock_conditional_calls = 0;
static int ut_lwlock_conditional_fail_call = 0;

void
LWLockInitialize(LWLock *lock pg_attribute_unused(), int tranche_id pg_attribute_unused())
{}
bool
LWLockAcquire(LWLock *lock pg_attribute_unused(), LWLockMode mode pg_attribute_unused())
{
	ut_lwlock_blocking_calls++;
	return true;
}
bool
LWLockConditionalAcquire(LWLock *lock pg_attribute_unused(),
						 LWLockMode mode pg_attribute_unused())
{
	ut_lwlock_conditional_calls++;
	return ut_lwlock_conditional_result
		&& (ut_lwlock_conditional_fail_call == 0
			|| ut_lwlock_conditional_calls
				   != ut_lwlock_conditional_fail_call);
}
void
LWLockRelease(LWLock *lock pg_attribute_unused())
{}

#include "cluster/cluster_shmem.h"
void
cluster_shmem_register_region(const ClusterShmemRegion *region pg_attribute_unused())
{}

void
ExceptionalCondition(const char *conditionName pg_attribute_unused(),
					 const char *fileName pg_attribute_unused(),
					 int lineNumber pg_attribute_unused())
{
	abort();
}

/* errmsg / errhint / errcode helpers — actual errstart / errstart_cold /
 * errfinish stubs are defined below alongside setjmp catcher state. */
int
errmsg(const char *f pg_attribute_unused(), ...)
{
	return 0;
}
int
errmsg_internal(const char *f pg_attribute_unused(), ...)
{
	return 0;
}
int
errhint(const char *f pg_attribute_unused(), ...)
{
	return 0;
}
int
errdetail(const char *f pg_attribute_unused(), ...) /* spec-5.14 D4 40R01 detail */
{
	return 0;
}
int
errcode(int s pg_attribute_unused())
{
	return 0;
}

/* Step 2 deps — cluster_reconfig.c lmon_tick body + ProcessInterrupts. */
#include "cluster/cluster_conf.h"
#include "cluster/cluster_cssd.h"
#include "cluster/cluster_qvotec.h"
#include "cluster/cluster_signal.h"

bool cluster_enabled = false;
int cluster_node_id = 0;
bool cluster_touched_peers_trace = false; /* spec-5.14 D4/D6 diag GUC stub */
volatile sig_atomic_t cluster_reconfig_start_pending = 0;
volatile sig_atomic_t InterruptPending = 0;

/* Mocked CSSD / QVOTEC / conf state — tests override via globals. */
static bool ut_in_quorum_value = false;
static int ut_qvotec_status = CLUSTER_QVOTEC_READY;
static uint64 ut_self_incarnation_first = UINT64_C(77);
static uint64 ut_self_incarnation_second = UINT64_C(77);
static uint64 ut_self_incarnation_later = UINT64_C(77);
static int ut_self_incarnation_calls = 0;
bool
cluster_qvotec_in_quorum(void)
{
	return ut_in_quorum_value;
}

uint64
cluster_qvotec_get_self_incarnation(void)
{
	int call = ut_self_incarnation_calls++;

	if (call == 0)
		return ut_self_incarnation_first;
	if (call == 1)
		return ut_self_incarnation_second;
	return ut_self_incarnation_later;
}

static void
ut_set_self_incarnation_sequence(uint64 first, uint64 second, uint64 later)
{
	ut_self_incarnation_first = first;
	ut_self_incarnation_second = second;
	ut_self_incarnation_later = later;
	ut_self_incarnation_calls = 0;
}

int
cluster_qvotec_get_status(void)
{
	return ut_qvotec_status;
}

static ClusterQvotecMailboxSubmitStatus ut_authority_submit_status
	= CLUSTER_QVOTEC_MAILBOX_SUBMIT_ACCEPTED;
static uint64 ut_authority_next_request_seq = UINT64_C(2);
static int ut_authority_submit_calls = 0;
static bool ut_authority_completion_ready = false;
static ClusterQvotecMailboxCompletion ut_authority_completion;

ClusterQvotecMailboxSubmitStatus
cluster_qvotec_authority_lmon_submit(
	ClusterQvotecMailboxOpcode opcode,
	const uint8 request_value[CLUSTER_QVOTEC_AUTHORITY_VALUE_BYTES],
	uint64 *request_seq_out)
{
	static const uint8 zero_value[CLUSTER_QVOTEC_AUTHORITY_VALUE_BYTES] = { 0 };

	if (opcode != CLUSTER_QVOTEC_MAILBOX_RECOVER_HEAD
		|| request_value == NULL || request_seq_out == NULL
		|| memcmp(request_value, zero_value, sizeof(zero_value)) != 0)
		return CLUSTER_QVOTEC_MAILBOX_SUBMIT_INVALID;
	ut_authority_submit_calls++;
	if (ut_authority_submit_status != CLUSTER_QVOTEC_MAILBOX_SUBMIT_ACCEPTED)
		return ut_authority_submit_status;
	*request_seq_out = ut_authority_next_request_seq;
	ut_authority_next_request_seq += UINT64_C(2);
	return CLUSTER_QVOTEC_MAILBOX_SUBMIT_ACCEPTED;
}

bool
cluster_qvotec_authority_lmon_poll_completion(
	uint64 request_seq, ClusterQvotecMailboxCompletion *completion_out)
{
	if (!ut_authority_completion_ready || completion_out == NULL
		|| ut_authority_completion.request_seq != request_seq)
		return false;
	*completion_out = ut_authority_completion;
	return true;
}

static ClusterCssdPeerState ut_peer_state[CLUSTER_MAX_NODES];
static uint64 ut_dead_generation = 0;
ClusterCssdPeerState
cluster_cssd_get_peer_state(int32 peer_id)
{
	if (peer_id < 0 || peer_id >= CLUSTER_MAX_NODES)
		return CLUSTER_CSSD_PEER_ALIVE;
	return ut_peer_state[peer_id];
}
uint64
cluster_cssd_get_dead_generation(void)
{
	return ut_dead_generation;
}

/* Hardening v1.0.4 stub: settable for the classify-before-own regression. */
static bool ut_clean_leave_in_progress;
bool
cluster_clean_leave_in_progress(void)
{
	return ut_clean_leave_in_progress;
}

/* RF-ROOT P6 (L5 wiring): unit stubs for symbols cluster_reconfig.o pulls in
 * that the pure unit harness does not link. */
volatile sig_atomic_t ShutdownRequestPending = false;

uint64
cluster_grd_recovery_done_epoch_for(int32 node pg_attribute_unused())
{
	return 0;
}

bool
cluster_lmon_reconfig_suppressed(void)
{
	return false;
}

/* declared-peer set:  bit i set → node i is declared in cluster.conf. */
static bool ut_declared_set[CLUSTER_MAX_NODES];
static ClusterNodeInfo ut_dummy_node;
const ClusterNodeInfo *
cluster_conf_lookup_node(int32 node_id)
{
	if (node_id < 0 || node_id >= CLUSTER_MAX_NODES)
		return NULL;
	return ut_declared_set[node_id] ? &ut_dummy_node : NULL;
}

#include "access/transam.h"
#include "access/xact.h"
/* IsTransactionState stub.  D4 ProcessInterrupts I6 absorb path. */
static bool ut_in_tx_state = false;
static TransactionId ut_top_xid = InvalidTransactionId;
bool
IsTransactionState(void)
{
	return ut_in_tx_state;
}
TransactionId
GetTopTransactionIdIfAny(void)
{
	return ut_top_xid;
}

/* Injection framework stubs for D10 callsites in cluster_reconfig.c. */
#include "cluster/cluster_inject.h"
int cluster_injection_armed_count = 0;
void
cluster_injection_run(const char *name pg_attribute_unused())
{}
bool
cluster_injection_should_skip(const char *name pg_attribute_unused())
{
	return false;
}

/* GetCurrentTimestamp + GetXLogInsertRecPtr stubs. */
#include "datatype/timestamp.h"
TimestampTz
GetCurrentTimestamp(void)
{
	return 1700000000000000LL;
}
#include "access/xlogdefs.h"
XLogRecPtr
GetXLogInsertRecPtr(void)
{
	return (XLogRecPtr)0x10000000;
}

/* SRF stubs (Step 3 D5b) — test never invokes cluster_get_reconfig_state but
 * the symbol must link.  Mirrors test_cluster_views.c pattern. */
#include "funcapi.h"
void
InitMaterializedSRF(FunctionCallInfo fcinfo pg_attribute_unused(),
					bits32 flags pg_attribute_unused())
{}
void
tuplestore_putvalues(Tuplestorestate *state pg_attribute_unused(),
					 TupleDesc tdesc pg_attribute_unused(), Datum *values pg_attribute_unused(),
					 bool *isnull pg_attribute_unused())
{}
text *
cstring_to_text(const char *s pg_attribute_unused())
{
	return NULL;
}

/* ProcArray / signal stubs. */
#include "storage/proc.h"
#include "storage/procsignal.h"
int MaxBackends = 0;
int MyProcPid = 99999;
PGPROC *MyProc = NULL;
PGPROC *
BackendIdGetProc(BackendId beid pg_attribute_unused())
{
	return NULL;
}
int
SendProcSignal(pid_t pid pg_attribute_unused(), ProcSignalReason r pg_attribute_unused(),
			   BackendId beid pg_attribute_unused())
{
	return 0;
}

/* setjmp-based ereport catcher (mirrors test_cluster_fence pattern). */
#include <setjmp.h>
static sigjmp_buf ut_ereport_jump;
static bool ut_ereport_jump_armed = false;
static int ut_ereport_fired_count = 0;
#undef errstart
#undef errstart_cold
#undef errfinish
bool
errstart(int elevel, const char *d pg_attribute_unused())
{
	return elevel >= 21; /* ERROR threshold */
}
bool
errstart_cold(int elevel, const char *d)
{
	return errstart(elevel, d);
}
void
errfinish(const char *f pg_attribute_unused(), int l pg_attribute_unused(),
		  const char *fn pg_attribute_unused())
{
	ut_ereport_fired_count++;
	if (ut_ereport_jump_armed)
		siglongjmp(ut_ereport_jump, 1);
}

/* spec-2.34 D4 stub: cluster_reconfig_apply_epoch_bump_as_coordinator
 * calls cluster_gcs_block_on_epoch_advance.  Fixture has no GCS shmem
 * state; stub is a no-op. */
void
cluster_gcs_block_on_epoch_advance(uint64 new_epoch pg_attribute_unused())
{}

void
cluster_gcs_block_on_epoch_advance_exact(
	uint64 new_epoch pg_attribute_unused(), const uint8 *dead_bitmap pg_attribute_unused())
{}

/* spec-2.39 D14 stub: cluster_reconfig_apply_epoch_bump_as_coordinator
 * calls cluster_sinval_reset_all_on_reconfig.  Fixture has no sinval shmem;
 * stub no-op. */
void
cluster_sinval_reset_all_on_reconfig(void)
{}

/* spec-4.12 D4 stubs: the coordinator's marker-before-publish gate references
 * the enforcement GUC + the submit entry.  Enforcement OFF here so the gate is a
 * no-op (reconfig behaves as pre-4.12 in this unit harness). */
int cluster_write_fence_enforcement = CLUSTER_WRITE_FENCE_ENFORCE_OFF;
static uint64 ut_authority_cache_invalidate_count = 0;
void
cluster_write_fence_authority_cache_invalidate(void)
{
	ut_authority_cache_invalidate_count++;
}
uint64
cluster_write_fence_authority_cache_mutation_begin(void)
{
	return UINT64_C(1);
}
void
cluster_write_fence_authority_cache_mutation_end(uint64 odd_sequence pg_attribute_unused())
{}
ClusterFenceMarkerSubmitResult cluster_write_fence_submit_marker(const ClusterFenceMarker *m);
ClusterFenceMarkerSubmitResult
cluster_write_fence_submit_marker(const ClusterFenceMarker *m pg_attribute_unused())
{
	return CLUSTER_FENCE_MARKER_SUBMIT_FAILED;
}
/* Link-only stub for the joiner-admission event helper (RF-ROOT P6); the real
 * durable-authority reader is exercised by test_cluster_write_fence_durable. */
ClusterFenceAuthorityReadResult
cluster_write_fence_read_durable_authority(ClusterFenceAuthorityProof *out pg_attribute_unused())
{
	return CLUSTER_FENCE_AUTHORITY_IO_UNAVAILABLE;
}

/* spec-2.29a review r1 P1-c: controllable async-marker stubs so the tick-level
 * P1-1 invariants (bump-once while PENDING, node-remove zero-false-contest,
 * publish deferred to ACK) can be hard-asserted.  Defaults preserve the
 * pre-existing inert behavior (submit rejected, poll idle). */
static bool ut_fence_async_submit_ok = false;
static ClusterMarkerPollResult ut_fence_async_poll_pr = CLUSTER_MARKER_POLL_IDLE;
static uint32 ut_fence_async_poll_result = CLUSTER_FENCE_MARKER_SUBMIT_FAILED;
static int ut_fence_async_submit_calls = 0;
static int ut_fence_async_poll_calls = 0;
static bool ut_fence_async_marker_captured = false;
static ClusterFenceMarker ut_fence_async_marker;

bool
cluster_write_fence_submit_marker_async(ClusterMarkerAsync *a,
										const ClusterFenceMarker *m,
										ClusterMarkerAsyncKind kind, int32 target_node,
										TimestampTz now pg_attribute_unused())
{
	ut_fence_async_submit_calls++;
	if (m != NULL) {
		ut_fence_async_marker = *m;
		ut_fence_async_marker_captured = true;
	}
	if (!ut_fence_async_submit_ok)
		return false;
	/* mirror the real wrapper's FSM effect so is_submitted() sees SUBMITTED */
	a->state = CLUSTER_MARKER_ASYNC_SUBMITTED;
	a->kind = kind;
	a->target_node = target_node;
	return true;
}
ClusterMarkerPollResult
cluster_write_fence_poll_marker_async(ClusterMarkerAsync *a, TimestampTz now pg_attribute_unused(),
									  uint32 *out_result, uint64 *out_elapsed_us)
{
	ut_fence_async_poll_calls++;
	if (out_result != NULL)
		*out_result = ut_fence_async_poll_result;
	if (out_elapsed_us != NULL)
		*out_elapsed_us = 0;
	if (ut_fence_async_poll_pr == CLUSTER_MARKER_POLL_ACKED
		|| ut_fence_async_poll_pr == CLUSTER_MARKER_POLL_TIMEOUT)
		a->state = CLUSTER_MARKER_ASYNC_IDLE;
	return ut_fence_async_poll_pr;
}
void
cluster_lmon_marker_complete_wakeup(void)
{}

/* spec-3.1 D7 stub: cluster_reconfig_apply_epoch_bump_as_coordinator
 * calls cluster_tt_status_flush_all.  Fixture has no TT overlay shmem;
 * stub no-op. */
void
cluster_tt_status_flush_all(uint32 new_epoch pg_attribute_unused())
{}

/* spec-5.15 D4 stubs: the join-marker handshake + seed reference these.  The
 * join path is gated by cluster_online_join (off by default in this fixture), so
 * apply/commit/submit are never exercised at runtime here; the symbols just need
 * a definition for the standalone link. */
bool cluster_online_join = false;
bool cluster_controlfile_shared_authority = false;
int cluster_quorum_poll_interval_ms = 100;
int cluster_join_convergence_timeout_ms = 30000;
/* spec-5.16 D6 GUC + joiner-home PCM fence arm referenced by the reconfig lmon tick /
 * note_self_admitted; the join-remaster path is off by default in this fixture, so
 * stub for the standalone link. */
bool cluster_join_remaster_enabled = false;
void cluster_grd_arm_join_pcm_fence(const uint8 *rejoining_set);
void
cluster_grd_arm_join_pcm_fence(const uint8 *rejoining_set pg_attribute_unused())
{}

/* Shape A (crash-rejoin re-declare barrier) stubs — the off-path rejoin tick
 * references these; the reconfig unit legs do not exercise the crash-rejoin
 * arm, so inert stubs suffice. */
static bool ut_offpath_boot_decided = true;
static bool ut_prior_unclean_death = false;
static bool ut_join_view_rebuilt = true;

int
cluster_conf_node_count(void)
{
	int n = 0;
	int i;

	for (i = 0; i < CLUSTER_MAX_NODES; i++)
		if (ut_declared_set[i])
			n++;
	return n;
}
bool
cluster_qvotec_prior_unclean_death(void)
{
	return ut_prior_unclean_death;
}
void
cluster_grd_set_offpath_boot_decided(void)
{
	ut_offpath_boot_decided = true;
}
bool
cluster_grd_offpath_boot_decided(void)
{
	return ut_offpath_boot_decided;
}
bool
cluster_grd_join_view_rebuilt(void)
{
	return ut_join_view_rebuilt;
}
void
cluster_grd_inc_offpath_crash_rejoin_fenced(void)
{}
/* spec-6.15 D5b: stripe joiner gate.  Most tests keep the legacy PROCEED
 * fixture; the AD-023 cold-boot ordering test drives the HOLD edge. */
#include "cluster/cluster_xid_stripe_boot.h"
static ClusterXidStripeJoinVerdict ut_xid_stripe_verdict
	= CLUSTER_XID_STRIPE_JOIN_PROCEED;
static int ut_xid_stripe_join_gate_calls;
static bool ut_xid_stripe_last_may_seed;
static ClusterXidStripeJoinProgress ut_xid_stripe_progress
	= STRIPE_JOIN_PROCEED;
static int ut_xid_stripe_join_progress_calls;
static bool ut_xid_stripe_progress_last_may_seed;
static ClusterXidStripeDiskState ut_xid_stripe_disk_state
	= CLUSTER_XID_STRIPE_DISK_UNKNOWN;
static ClusterWalSlotVerdict ut_wal_slot_verdict[CLUSTER_MAX_NODES];
static ClusterWalStateSlot ut_wal_slot[CLUSTER_MAX_NODES];
static bool ut_recovery_in_progress = false;
ClusterXidStripeJoinVerdict
cluster_xid_stripe_join_gate(bool self_may_seed)
{
	ut_xid_stripe_join_gate_calls++;
	ut_xid_stripe_last_may_seed = self_may_seed;
	return ut_xid_stripe_verdict;
}
ClusterXidStripeJoinProgress
cluster_xid_stripe_join_progress(bool self_may_seed)
{
	ut_xid_stripe_join_progress_calls++;
	ut_xid_stripe_progress_last_may_seed = self_may_seed;
	return ut_xid_stripe_progress;
}
ClusterXidStripeDiskState
cluster_xid_stripe_disk_state(void)
{
	return ut_xid_stripe_disk_state;
}
ClusterWalSlotVerdict
cluster_wal_state_read_slot(uint16 thread_id, ClusterWalStateSlot *slot_out)
{
	int node_id = (int)thread_id - 1;

	if (node_id < 0 || node_id >= CLUSTER_MAX_NODES)
		return CLUSTER_WAL_SLOT_CORRUPT;
	if (slot_out != NULL)
		*slot_out = ut_wal_slot[node_id];
	return ut_wal_slot_verdict[node_id];
}
bool
RecoveryInProgress(void)
{
	return ut_recovery_in_progress;
}
/* pgstat backend global referenced by pgstat_report_wait_start/end (the D4 join
 * marker submit wait); provide a file-static fake so the standalone link works. */
static uint32 ut_wait_event_info_storage;
uint32 *my_wait_event_info = &ut_wait_event_info_storage;
#include "storage/latch.h"
void
SetLatch(Latch *latch pg_attribute_unused())
{}
#include "storage/ipc.h"
void
on_shmem_exit(pg_on_exit_callback function pg_attribute_unused(), Datum arg pg_attribute_unused())
{}
void
before_shmem_exit(pg_on_exit_callback function pg_attribute_unused(), Datum arg pg_attribute_unused())
{}
#include "cluster/cluster_voting_disk_io.h"
static bool ut_join_disk_readable[CLUSTER_MAX_VOTING_DISKS];
static uint8 ut_join_disk_images[CLUSTER_MAX_VOTING_DISKS][CLUSTER_VOTING_SLOT_BYTES];
ClusterVotingDiskIoState
cluster_voting_disk_read_join_slot(int fd,
								   uint32 node_id pg_attribute_unused(),
								   void *out_slot512)
{
	if (fd >= 0 && fd < CLUSTER_MAX_VOTING_DISKS
		&& ut_join_disk_readable[fd] && out_slot512 != NULL) {
		memcpy(out_slot512, ut_join_disk_images[fd], CLUSTER_VOTING_SLOT_BYTES);
		return CLUSTER_VOTING_DISK_IO_OK;
	}
	return CLUSTER_VOTING_DISK_IO_FAILED; /* no marker -> seed is a no-op */
}

/* Reset helper for between-test mock state. */
static void
ut_reset_mocks(void)
{
	int i;
	for (i = 0; i < CLUSTER_MAX_NODES; i++) {
		ut_peer_state[i] = CLUSTER_CSSD_PEER_ALIVE;
		ut_declared_set[i] = false;
		ut_candidate2_capable[i] = true;
	}
	ut_in_quorum_value = false;
	ut_qvotec_status = CLUSTER_QVOTEC_READY;
	ut_set_self_incarnation_sequence(UINT64_C(77), UINT64_C(77), UINT64_C(77));
	ut_offpath_boot_decided = true;
	ut_prior_unclean_death = false;
	ut_join_view_rebuilt = true;
	ut_xid_stripe_verdict = CLUSTER_XID_STRIPE_JOIN_PROCEED;
	ut_xid_stripe_join_gate_calls = 0;
	ut_xid_stripe_last_may_seed = false;
	ut_xid_stripe_progress = STRIPE_JOIN_PROCEED;
	ut_xid_stripe_join_progress_calls = 0;
	ut_xid_stripe_progress_last_may_seed = false;
	ut_xid_stripe_disk_state = CLUSTER_XID_STRIPE_DISK_UNKNOWN;
	for (i = 0; i < CLUSTER_MAX_NODES; i++) {
		ut_wal_slot_verdict[i] = CLUSTER_WAL_SLOT_EMPTY;
		memset(&ut_wal_slot[i], 0, sizeof(ut_wal_slot[i]));
	}
	ut_recovery_in_progress = false;
	ut_clean_leave_in_progress = false;
	ut_self_member_callback_seen = false;
	ut_self_member_callback_gate = CLUSTER_JOIN_GATE_ALLOW;
	ut_dead_generation = 0;
	ut_in_tx_state = false;
	ut_top_xid = InvalidTransactionId;
	cluster_enabled = true;
	cluster_node_id = 0;
	cluster_controlfile_shared_authority = false;
	cluster_reconfig_start_pending = 0;
	InterruptPending = 0;
	ut_ereport_fired_count = 0;
	ut_ereport_jump_armed = false;
	ut_authority_submit_status = CLUSTER_QVOTEC_MAILBOX_SUBMIT_ACCEPTED;
	ut_authority_next_request_seq = UINT64_C(2);
	ut_authority_submit_calls = 0;
	ut_authority_completion_ready = false;
	memset(&ut_authority_completion, 0, sizeof(ut_authority_completion));
	memset(ut_join_disk_readable, 0, sizeof(ut_join_disk_readable));
	memset(ut_join_disk_images, 0, sizeof(ut_join_disk_images));
	memset(&ut_r4_prerequisite_snapshot, 0,
		   sizeof(ut_r4_prerequisite_snapshot));
	ut_r4_prerequisite_snapshot.target_node_id = -1;
	ut_r4_snapshot_use_reconfig = false;
	ut_phase3_send_result = CLUSTER_IC_SEND_DONE;
	ut_phase3_send_calls = 0;
	ut_phase3_send_msg_type = 0;
	ut_phase3_send_dest = -1;
	ut_phase3_send_length = 0;
	memset(ut_phase3_send_bytes, 0, sizeof(ut_phase3_send_bytes));
	ut_phase3_close_calls = 0;
	ut_phase3_close_peer = -1;
	ut_owner_rejoin_result = true;
	ut_owner_rejoin_calls = 0;
	ut_owner_rejoin_node = -1;
	ut_owner_rejoin_incarnation = 0;
	ut_rejoin_root_complete = false;
	ut_external_fence_active = false;
	ut_authority_managed = false;
	ut_authority_readiness = CLUSTER_AUTHORITY_OFF;
	ut_handoff_identity_incarnation = 0;
	ut_handoff_identity_predecessor_floor = 0;
	ut_serving_ready = false;
	ut_lms_generation = UINT64_C(1);
	ut_rejoin_grd_clear_ready = true;
	ut_rejoin_start_calls = 0;
	ut_rejoin_poll_calls = 0;
	ut_rejoin_clear_build_calls = 0;
	ut_rejoin_authorize_calls = 0;
	ut_rejoin_refresh_calls = 0;
	ut_rejoin_root_revalidate_calls = 0;
	ut_rejoin_consume_calls = 0;
	ut_rejoin_release_calls = 0;
	ut_rejoin_consume_saw_prior_marker_submit = false;
	ut_lwlock_conditional_result = true;
	ut_lwlock_conditional_fail_call = 0;
	ut_lwlock_blocking_calls = 0;
	ut_lwlock_conditional_calls = 0;
	ut_rejoin_poll_status = PGRAC_EXTERNAL_FENCE_REJOIN_PENDING;
	memset(&ut_rejoin_offer, 0, sizeof(ut_rejoin_offer));
	memset(&ut_rejoin_failure_seen, 0, sizeof(ut_rejoin_failure_seen));
	memset(&ut_rejoin_pending_seen, 0, sizeof(ut_rejoin_pending_seen));
	memset(&ut_rejoin_committed_seen, 0, sizeof(ut_rejoin_committed_seen));
	ut_fence_async_marker_captured = false;
	memset(&ut_fence_async_marker, 0, sizeof(ut_fence_async_marker));
	MyBackendType = B_INVALID;
}


/* ============================================================
 * T-reconfig-1 — Foundation: sizeof bounds + shmem layout.
 * ============================================================ */

UT_TEST(test_reconfig_dead_bitmap_bytes_eq_16)
{
	/* P2.8 fix:  dead_bitmap must be uint8[16] = 128 bits for 128
	 * declared nodes (CLUSTER_MAX_NODES).  v0.1's uint64 (64 bits)
	 * was rejected — verify the constant is 16. */
	UT_ASSERT_EQ(CLUSTER_RECONFIG_DEAD_BITMAP_BYTES, 16);
}


UT_TEST(test_reconfig_event_sizeof_bounds)
{
	/* P2.8 fix:  natural-aligned, NOT pg_attribute_packed.  Lower bound
	 * 64 catches accidental field removal;upper bound widened to 112 by
	 * spec-5.15 (join_bitmap[16]) catches accidental field bloat. */
	UT_ASSERT(sizeof(ReconfigEvent) >= 64);
	UT_ASSERT(sizeof(ReconfigEvent) <= 112);

	/* Field-level sanity:  88 bytes through spec-5.14 (8+4+4 + 8+8+16 + 8+4+4 +
	 * 8+8 = 80, plus reconfig_kind uint8 + _pad2[7] = 8), plus spec-5.15's
	 * join_bitmap[16] -> 104. */
	UT_ASSERT_EQ(sizeof(ReconfigEvent), 104);
}


UT_TEST(test_reconfig_shmem_size_positive)
{
	Size s = cluster_reconfig_shmem_size();
	/* MAXALIGN(sizeof(ClusterReconfigState)) — must be > sizeof
	 * ReconfigEvent because state struct wraps event + lock + 3
	 * atomic counters. */
	UT_ASSERT(s > sizeof(ReconfigEvent));
	UT_ASSERT(s <= sizeof(reconfig_shmem_storage));
}


UT_TEST(test_reconfig_shmem_init_idempotent)
{
	ReconfigEvent evt;

	reconfig_init_done = false;

	/* First init — found = false branch. */
	cluster_reconfig_shmem_init();
	UT_ASSERT(reconfig_init_done);

	/* get_last_event should populate with never-applied sentinel
	 * (event_id = 0, observer_role = NONE). */
	cluster_reconfig_get_last_event(&evt);
	UT_ASSERT_EQ((unsigned long long)evt.event_id, 0ULL);
	UT_ASSERT_EQ(evt.observer_role, CLUSTER_RECONFIG_OBSERVER_NONE);
	UT_ASSERT_EQ((long long)evt.applied_at, 0LL);

	/* Second init — found = true branch.  Should NOT re-zero state
	 * (postmaster restart preserves shmem on the same shmem segment
	 * for the same process — the found-flag prevents double init). */
	cluster_reconfig_shmem_init();
	cluster_reconfig_get_last_event(&evt);
	UT_ASSERT_EQ((unsigned long long)evt.event_id, 0ULL);
}


UT_TEST(test_formation_snapshot_no_pgproc_never_blocks_on_reconfig_lock)
{
	ClusterFormationSnapshotV1 snapshot;
	ClusterFormationSnapshotV1 zero;
	PGPROC fake_proc;

	reconfig_init_done = false;
	cluster_reconfig_shmem_init();
	memset(&zero, 0, sizeof(zero));

	/* Postmaster phase 3 has no PGPROC.  Contention must return unavailable
	 * without entering LWLockQueueSelf, leaving no partial snapshot bytes. */
	MyProc = NULL;
	ut_lwlock_conditional_result = false;
	ut_lwlock_blocking_calls = 0;
	ut_lwlock_conditional_calls = 0;
	memset(&snapshot, 0xa5, sizeof(snapshot));
	UT_ASSERT(!cluster_reconfig_capture_formation_snapshot_v1(1, &snapshot));
	UT_ASSERT_EQ(ut_lwlock_conditional_calls, 1);
	UT_ASSERT_EQ(ut_lwlock_blocking_calls, 0);
	UT_ASSERT_EQ(memcmp(&snapshot, &zero, sizeof(snapshot)), 0);

	/* The same no-PGPROC caller may take an immediately available shared
	 * lock, still without using the blocking acquisition primitive. */
	ut_lwlock_conditional_result = true;
	ut_lwlock_blocking_calls = 0;
	ut_lwlock_conditional_calls = 0;
	UT_ASSERT(cluster_reconfig_capture_formation_snapshot_v1(1, &snapshot));
	UT_ASSERT_EQ(ut_lwlock_conditional_calls, 1);
	UT_ASSERT_EQ(ut_lwlock_blocking_calls, 0);

	/* Ordinary processes retain the existing blocking snapshot semantics. */
	memset(&fake_proc, 0, sizeof(fake_proc));
	MyProc = &fake_proc;
	ut_lwlock_blocking_calls = 0;
	ut_lwlock_conditional_calls = 0;
	UT_ASSERT(cluster_reconfig_capture_formation_snapshot_v1(1, &snapshot));
	UT_ASSERT_EQ(ut_lwlock_conditional_calls, 0);
	UT_ASSERT_EQ(ut_lwlock_blocking_calls, 1);
	MyProc = NULL;
}


UT_TEST(test_self_join_admitted_no_pgproc_never_blocks_on_reconfig_lock)
{
	ClusterReconfigState *state;
	PGPROC fake_proc;

	reconfig_init_done = false;
	cluster_reconfig_shmem_init();
	state = (ClusterReconfigState *)reconfig_shmem_storage;

	/* Postmaster phase 3 has no PGPROC (AD-023 A1, STOP-01 contract):
	 * a contended reconfig lock must read not-yet-admitted without
	 * entering LWLockQueueSelf. */
	MyProc = NULL;
	ut_lwlock_conditional_result = false;
	ut_lwlock_blocking_calls = 0;
	ut_lwlock_conditional_calls = 0;
	UT_ASSERT(!cluster_reconfig_self_join_admitted());
	UT_ASSERT_EQ(ut_lwlock_conditional_calls, 1);
	UT_ASSERT_EQ(ut_lwlock_blocking_calls, 0);

	/* The same no-PGPROC caller may take an immediately available shared
	 * lock, still without using the blocking acquisition primitive. */
	state->self_join_admitted = 1;
	ut_lwlock_conditional_result = true;
	ut_lwlock_blocking_calls = 0;
	ut_lwlock_conditional_calls = 0;
	UT_ASSERT(cluster_reconfig_self_join_admitted());
	UT_ASSERT_EQ(ut_lwlock_conditional_calls, 1);
	UT_ASSERT_EQ(ut_lwlock_blocking_calls, 0);
	state->self_join_admitted = 0;

	/* Ordinary processes retain the existing blocking read semantics. */
	memset(&fake_proc, 0, sizeof(fake_proc));
	MyProc = &fake_proc;
	ut_lwlock_blocking_calls = 0;
	ut_lwlock_conditional_calls = 0;
	UT_ASSERT(!cluster_reconfig_self_join_admitted());
	UT_ASSERT_EQ(ut_lwlock_conditional_calls, 0);
	UT_ASSERT_EQ(ut_lwlock_blocking_calls, 1);
	MyProc = NULL;
}


/* spec-5.15A: the node-local replacement episode is part of the existing
 * reconfig region and starts as the exact canonical empty image.  Together
 * with the v3 mailbox widening plus P04's volatile fast-rejoin evidence this
 * is the frozen 10,968-byte state shape. */
UT_TEST(test_reconfig_replacement_episode_is_embedded_and_zero_initialized)
{
	ClusterReconfigState *state;
	ClusterReplacementEpisode empty_episode;

	reconfig_init_done = false;
	cluster_reconfig_shmem_init();
	state = (ClusterReconfigState *)reconfig_shmem_storage;
	memset(&empty_episode, 0, sizeof(empty_episode));

	UT_ASSERT_EQ(sizeof(ClusterReconfigState), 12640);
	UT_ASSERT_EQ(memcmp(&state->replacement_episode, &empty_episode,
						sizeof(empty_episode)),
				 0);
}


UT_TEST(test_reconfig_publish_increments_apply_counter)
{
	ReconfigEvent evt;
	ReconfigEvent in;

	reconfig_init_done = false;
	cluster_enabled = true;
	cluster_reconfig_shmem_init();

	memset(&in, 0, sizeof(in));
	in.event_id = 0xABCDEF;
	in.coordinator_node_id = 0;
	in.old_epoch = 5;
	in.new_epoch = 6;
	in.observer_role = CLUSTER_RECONFIG_OBSERVER_COORDINATOR;
	in.event_seq = 42; /* publish path owns the final monotonic value. */
	in.cssd_dead_generation = 3;

	cluster_reconfig_publish_event(&in);

	cluster_reconfig_get_last_event(&evt);
	UT_ASSERT_EQ((unsigned long long)evt.event_id, 0xABCDEFULL);
	UT_ASSERT_EQ(evt.coordinator_node_id, 0);
	UT_ASSERT_EQ((unsigned long long)evt.new_epoch, 6ULL);
	UT_ASSERT_EQ(evt.observer_role, CLUSTER_RECONFIG_OBSERVER_COORDINATOR);
	UT_ASSERT_EQ((unsigned long long)evt.event_seq, 1ULL);
	UT_ASSERT_EQ((unsigned long long)evt.cssd_dead_generation, 3ULL);
}


UT_TEST(test_reconfig_publish_overwrites_event_seq_monotonically)
{
	ReconfigEvent evt;
	ReconfigEvent in;

	reconfig_init_done = false;
	cluster_reconfig_shmem_init();

	memset(&in, 0, sizeof(in));
	in.event_id = 1;
	in.event_seq = 99;
	in.observer_role = CLUSTER_RECONFIG_OBSERVER_COORDINATOR;
	cluster_reconfig_publish_event(&in);
	cluster_reconfig_get_last_event(&evt);
	UT_ASSERT_EQ((unsigned long long)evt.event_seq, 1ULL);

	in.event_id = 2;
	in.event_seq = 99;
	cluster_reconfig_publish_event(&in);
	cluster_reconfig_get_last_event(&evt);
	UT_ASSERT_EQ((unsigned long long)evt.event_seq, 2ULL);
}


UT_TEST(test_reconfig_broadcast_increments_counter)
{
	reconfig_init_done = false;
	cluster_reconfig_shmem_init();

	/* Real body walks ProcArray; MaxBackends=0 in this unit harness, so
	 * the loop is empty but the invocation counter still advances. */
	cluster_reconfig_broadcast_local_procsig();
	cluster_reconfig_broadcast_local_procsig();

	UT_ASSERT_EQ((unsigned long long)cluster_reconfig_get_procsig_broadcast_count(), 2ULL);
}


/* ============================================================
 * T-reconfig-9 — cluster_epoch_observe_remote CAS-loop semantics
 *                + CLUSTER_EPOCH_OBSERVE_MAX_JUMP constant.
 * ============================================================ */

UT_TEST(test_epoch_observe_remote_advance_from_zero)
{
	bool advanced;

	epoch_init_done = false;
	cluster_epoch_shmem_init();
	UT_ASSERT_EQ((unsigned long long)cluster_epoch_get_current(), 0ULL);

	/* Advance from 0 → 7. */
	advanced = cluster_epoch_observe_remote(7);
	UT_ASSERT(advanced);
	UT_ASSERT_EQ((unsigned long long)cluster_epoch_get_current(), 7ULL);
}


UT_TEST(test_epoch_observe_remote_no_op_equal)
{
	bool advanced;

	epoch_init_done = false;
	cluster_epoch_shmem_init();
	(void)cluster_epoch_observe_remote(7); /* establish baseline */

	/* observe_remote(7) again — local already at 7, no advance. */
	advanced = cluster_epoch_observe_remote(7);
	UT_ASSERT(!advanced);
	UT_ASSERT_EQ((unsigned long long)cluster_epoch_get_current(), 7ULL);
}


UT_TEST(test_epoch_observe_remote_no_retreat)
{
	bool advanced;

	epoch_init_done = false;
	cluster_epoch_shmem_init();
	(void)cluster_epoch_observe_remote(7);

	/* observe_remote(3) — stale, must NOT retreat. */
	advanced = cluster_epoch_observe_remote(3);
	UT_ASSERT(!advanced);
	UT_ASSERT_EQ((unsigned long long)cluster_epoch_get_current(), 7ULL);
}


UT_TEST(test_epoch_observe_remote_monotonic_chain)
{
	epoch_init_done = false;
	cluster_epoch_shmem_init();

	/* Apply a chain of advances + no-ops + retreats;final must be
	 * the max observed, not the last observed. */
	UT_ASSERT(cluster_epoch_observe_remote(5));	  /* 0 → 5 */
	UT_ASSERT(!cluster_epoch_observe_remote(3));  /* stale */
	UT_ASSERT(cluster_epoch_observe_remote(10));  /* 5 → 10 */
	UT_ASSERT(!cluster_epoch_observe_remote(8));  /* stale */
	UT_ASSERT(!cluster_epoch_observe_remote(10)); /* no-op */
	UT_ASSERT(cluster_epoch_observe_remote(11));  /* 10 → 11 */

	UT_ASSERT_EQ((unsigned long long)cluster_epoch_get_current(), 11ULL);
}


UT_TEST(test_epoch_advance_for_reconfig_pre_post_snapshots)
{
	uint64 old_v, new_v;

	epoch_init_done = false;
	cluster_epoch_shmem_init();

	/* From 0 → 1. */
	cluster_epoch_advance_for_reconfig(&old_v, &new_v);
	UT_ASSERT_EQ((unsigned long long)old_v, 0ULL);
	UT_ASSERT_EQ((unsigned long long)new_v, 1ULL);
	UT_ASSERT_EQ((unsigned long long)cluster_epoch_get_current(), 1ULL);

	/* Idempotent — each call advances by exactly 1. */
	cluster_epoch_advance_for_reconfig(&old_v, &new_v);
	UT_ASSERT_EQ((unsigned long long)old_v, 1ULL);
	UT_ASSERT_EQ((unsigned long long)new_v, 2ULL);
	UT_ASSERT_EQ((unsigned long long)cluster_epoch_get_current(), 2ULL);
}


UT_TEST(test_epoch_observe_max_jump_constant)
{
	/* spec-2.29 D18b — bounded jump defense against hostile-spoof
	 * envelope frames.  Caller (D20 envelope verify path) checks
	 * remote - my <= MAX_JUMP before calling observe_remote;
	 * constant must be exactly 16 per spec §3.7-bis + §6 R11. */
	UT_ASSERT_EQ((unsigned long long)CLUSTER_EPOCH_OBSERVE_MAX_JUMP, 16ULL);
}


UT_TEST(test_epoch_changed_at_lsn_set_and_get)
{
	uint64 lsn;

	epoch_init_done = false;
	cluster_epoch_shmem_init();

	UT_ASSERT_EQ((unsigned long long)cluster_epoch_get_changed_at_lsn(), 0ULL);

	cluster_epoch_set_changed_at_lsn(0xDEADBEEFCAFEBABEULL);
	lsn = cluster_epoch_get_changed_at_lsn();
	UT_ASSERT_EQ((unsigned long long)lsn, 0xDEADBEEFCAFEBABEULL);
}


/* ============================================================
 * T-reconfig-2 — compute_event_id deterministic + P1.2 invariants.
 * ============================================================ */

UT_TEST(test_reconfig_compute_event_id_deterministic)
{
	uint8 bmp[CLUSTER_RECONFIG_DEAD_BITMAP_BYTES] = { 0 };
	uint64 id1, id2;

	bmp[0] = 0x02; /* node 1 dead */

	id1 = cluster_reconfig_compute_event_id(bmp, 7);
	id2 = cluster_reconfig_compute_event_id(bmp, 7);
	UT_ASSERT_EQ((unsigned long long)id1, (unsigned long long)id2);
	/* sanity: hash output != 0 (probabilistically); 0 reserved sentinel. */
	UT_ASSERT(id1 != 0);
}


UT_TEST(test_reconfig_compute_event_id_dead_bitmap_sensitivity)
{
	uint8 bmp1[CLUSTER_RECONFIG_DEAD_BITMAP_BYTES] = { 0 };
	uint8 bmp2[CLUSTER_RECONFIG_DEAD_BITMAP_BYTES] = { 0 };
	uint64 id1, id2;

	bmp1[0] = 0x02; /* node 1 dead */
	bmp2[0] = 0x06; /* nodes 1 + 2 dead */

	id1 = cluster_reconfig_compute_event_id(bmp1, 5);
	id2 = cluster_reconfig_compute_event_id(bmp2, 5);
	UT_ASSERT(id1 != id2);
}


UT_TEST(test_reconfig_compute_event_id_dead_gen_sensitivity)
{
	uint8 bmp[CLUSTER_RECONFIG_DEAD_BITMAP_BYTES] = { 0 };
	uint64 id_gen5, id_gen6;

	bmp[0] = 0x02;

	/* P1.2 invariant: same dead_bitmap with different cssd_dead_generation
	 * MUST produce different event_id, so rejoin-then-redeath fires fresh
	 * reconfig event even when bitmap unchanged. */
	id_gen5 = cluster_reconfig_compute_event_id(bmp, 5);
	id_gen6 = cluster_reconfig_compute_event_id(bmp, 6);
	UT_ASSERT(id_gen5 != id_gen6);
}

UT_TEST(test_reconfig_builds_exact_replacement_committed_event)
{
	ClusterReplacementEpisode episode = ut_admitted_replacement_episode(3);
	ClusterReplacementEpisode changed;
	ReconfigEvent event;
	ReconfigEvent changed_event;
	ReconfigEvent sentinel;
	int i;

	episode.phase = CLUSTER_REPLACEMENT_EPISODE_COMMITTED_CLOSED;
	episode.readiness_flags = 0;
	memset(&event, 0, sizeof(event));
	UT_ASSERT(cluster_reconfig_build_replacement_committed_event(
		&episode, CLUSTER_RECONFIG_OBSERVER_SURVIVOR,
		(TimestampTz)UINT64_C(1234), &event));
	UT_ASSERT_EQ((int)RECONFIG_KIND_REPLACEMENT_COMMITTED, 6);
	UT_ASSERT_EQ((int)event.reconfig_kind,
		(int)RECONFIG_KIND_REPLACEMENT_COMMITTED);
	UT_ASSERT(event.event_id != 0);
	UT_ASSERT_EQ(event.coordinator_node_id, episode.coordinator_node_id);
	UT_ASSERT_EQ(event.old_epoch, episode.baseline_epoch);
	UT_ASSERT_EQ(event.new_epoch, episode.reserved_or_committed_epoch);
	UT_ASSERT_EQ(event.applied_at, (TimestampTz)UINT64_C(1234));
	UT_ASSERT_EQ(event.observer_role, CLUSTER_RECONFIG_OBSERVER_SURVIVOR);
	UT_ASSERT_EQ(event.cssd_dead_generation, 0);
	for (i = 0; i < CLUSTER_RECONFIG_DEAD_BITMAP_BYTES; i++)
		UT_ASSERT_EQ((int)event.dead_bitmap[i], 0);
	UT_ASSERT(jb_test(event.join_bitmap, episode.target_node_id));
	for (i = 0; i < CLUSTER_MAX_NODES; i++) {
		if (i != episode.target_node_id)
			UT_ASSERT(!jb_test(event.join_bitmap, i));
	}

	changed = episode;
	changed.request_nonce++;
	UT_ASSERT(cluster_reconfig_build_replacement_committed_event(
		&changed, CLUSTER_RECONFIG_OBSERVER_SURVIVOR,
		(TimestampTz)UINT64_C(1234), &changed_event));
	UT_ASSERT(changed_event.event_id != event.event_id);
	changed = episode;
	changed.expected_survivors[0] ^= UINT8_C(0x10);
	changed.acknowledgements[0] = changed.expected_survivors[0];
	UT_ASSERT(cluster_reconfig_build_replacement_committed_event(
		&changed, CLUSTER_RECONFIG_OBSERVER_SURVIVOR,
		(TimestampTz)UINT64_C(1234), &changed_event));
	UT_ASSERT(changed_event.event_id != event.event_id);

	memset(&sentinel, 0xa5, sizeof(sentinel));
	changed_event = sentinel;
	changed = episode;
	changed.acknowledgements[0] &= (uint8)~UINT8_C(0x04);
	UT_ASSERT(!cluster_reconfig_build_replacement_committed_event(
		&changed, CLUSTER_RECONFIG_OBSERVER_SURVIVOR,
		(TimestampTz)UINT64_C(1234), &changed_event));
	UT_ASSERT_EQ(memcmp(&changed_event, &sentinel, sizeof(sentinel)), 0);

	changed = episode;
	changed.phase = CLUSTER_REPLACEMENT_EPISODE_PURGING;
	UT_ASSERT(!cluster_reconfig_build_replacement_committed_event(
		&changed, CLUSTER_RECONFIG_OBSERVER_SURVIVOR,
		(TimestampTz)UINT64_C(1234), &changed_event));
	UT_ASSERT_EQ(memcmp(&changed_event, &sentinel, sizeof(sentinel)), 0);
	UT_ASSERT(!cluster_reconfig_build_replacement_committed_event(
		&episode, CLUSTER_RECONFIG_OBSERVER_NONE,
		(TimestampTz)UINT64_C(1234), &changed_event));
	UT_ASSERT_EQ(memcmp(&changed_event, &sentinel, sizeof(sentinel)), 0);
}

/* Replacement post-epoch GRD may derive its direction set only from the
 * exact durable PREPARE/COMMITTED lineage, never from declared membership or
 * the kind-6 target bit.  This gate is pure and leaves outputs untouched on
 * every mismatch. */
UT_TEST(test_reconfig_replacement_grd_basis_uses_immutable_survivors)
{
	ClusterReconfigState *state;
	ClusterReplacementEpisode episode = ut_admitted_replacement_episode(3);
	ClusterReplacementCommitMarkerV3 committed;
	ReconfigEvent event;
	uint8 committed_image[CLUSTER_JCMK_REPLACEMENT_BYTES];
	uint8 survivors[CLUSTER_RECONFIG_DEAD_BITMAP_BYTES];
	uint8 sentinel[CLUSTER_RECONFIG_DEAD_BITMAP_BYTES];
	uint64 epoch = UINT64_C(0xa5a5a5a5a5a5a5a5);
	uint64 epoch_sentinel = epoch;

	episode.phase = CLUSTER_REPLACEMENT_EPISODE_COMMITTED_CLOSED;
	episode.readiness_flags = 0;
	memset(&committed, 0, sizeof(committed));
	committed.magic = CLUSTER_JCMK_MAGIC;
	committed.version = CLUSTER_JCMK_REPLACEMENT_VERSION;
	committed.target_node_id = episode.target_node_id;
	committed.phase = CLUSTER_JCMK_REPLACEMENT_PHASE_COMMITTED_CLOSED;
	committed.generation = UINT64_C(41);
	committed.old_admitted_incarnation = episode.old_admitted_incarnation;
	committed.fresh_incarnation = episode.fresh_incarnation;
	committed.baseline_epoch = episode.baseline_epoch;
	committed.reserved_or_committed_epoch
		= episode.reserved_or_committed_epoch;
	committed.request_nonce = episode.request_nonce;
	memcpy(committed.expected_purge_survivors, episode.expected_survivors,
		   sizeof(committed.expected_purge_survivors));
	committed.grammar_fingerprint = episode.grammar_fingerprint;
	UT_ASSERT(cluster_reconfig_build_replacement_committed_event(
		&episode, CLUSTER_RECONFIG_OBSERVER_SURVIVOR,
		(TimestampTz)UINT64_C(1234), &event));
	event.event_seq = UINT64_C(7); /* proves local publication, not a builder */
	memset(survivors, 0xa5, sizeof(survivors));
	memcpy(sentinel, survivors, sizeof(sentinel));

	UT_ASSERT(cluster_reconfig_replacement_grd_basis_authorized(
		&event, &episode, &committed, 2, survivors, &epoch));
	UT_ASSERT_EQ(memcmp(survivors, episode.expected_survivors,
					 sizeof(survivors)), 0);
	UT_ASSERT_EQ(epoch, episode.reserved_or_committed_epoch);

	memcpy(survivors, sentinel, sizeof(survivors));
	epoch = epoch_sentinel;
	event.reconfig_kind = RECONFIG_KIND_JOIN_COMMITTED;
	UT_ASSERT(!cluster_reconfig_replacement_grd_basis_authorized(
		&event, &episode, &committed, 2, survivors, &epoch));
	UT_ASSERT_EQ(memcmp(survivors, sentinel, sizeof(survivors)), 0);
	UT_ASSERT_EQ(epoch, epoch_sentinel);
	event.reconfig_kind = RECONFIG_KIND_REPLACEMENT_COMMITTED;

	committed.expected_purge_survivors[0] ^= UINT8_C(0x02);
	UT_ASSERT(!cluster_reconfig_replacement_grd_basis_authorized(
		&event, &episode, &committed, 2, survivors, &epoch));
	UT_ASSERT_EQ(memcmp(survivors, sentinel, sizeof(survivors)), 0);
	UT_ASSERT_EQ(epoch, epoch_sentinel);
	committed.expected_purge_survivors[0] ^= UINT8_C(0x02);

	UT_ASSERT(!cluster_reconfig_replacement_grd_basis_authorized(
		&event, &episode, &committed, episode.target_node_id,
		survivors, &epoch));
	UT_ASSERT_EQ(memcmp(survivors, sentinel, sizeof(survivors)), 0);
	UT_ASSERT_EQ(epoch, epoch_sentinel);

	/* The LMON snapshot composes the same gate only after local kind-6 and
	 * the exact QVOTEC-completed marker are co-sampled. */
	ut_join_setup();
	cluster_node_id = 2;
	state = (ClusterReconfigState *)reconfig_shmem_storage;
	state->replacement_episode = episode;
	epoch_init_done = false;
	cluster_epoch_shmem_init();
	UT_ASSERT(cluster_epoch_observe_remote(
		episode.reserved_or_committed_epoch));
	UT_ASSERT(cluster_replacement_marker_v3_encode(
		&committed, committed_image));
	memcpy(state->join_pending_marker, committed_image,
		   sizeof(committed_image));
	pg_atomic_write_u64(&state->join_marker_request_seq, UINT64_C(11));
	pg_atomic_write_u64(&state->join_marker_completion_seq, UINT64_C(11));
	pg_atomic_write_u32(&state->join_marker_result,
						CLUSTER_JOIN_MARKER_SUBMIT_ACK);
	UT_ASSERT(cluster_reconfig_build_replacement_committed_event(
		&episode, CLUSTER_RECONFIG_OBSERVER_SURVIVOR,
		(TimestampTz)UINT64_C(4321), &event));
	cluster_reconfig_publish_event(&event);
	memset(survivors, 0xa5, sizeof(survivors));
	epoch = epoch_sentinel;
	UT_ASSERT(cluster_reconfig_lmon_snapshot_replacement_grd_basis(
		survivors, &epoch));
	UT_ASSERT_EQ(memcmp(survivors, episode.expected_survivors,
					 sizeof(survivors)), 0);
	UT_ASSERT_EQ(epoch, episode.reserved_or_committed_epoch);

	memcpy(survivors, sentinel, sizeof(survivors));
	epoch = epoch_sentinel;
	state->join_pending_marker[24] ^= UINT8_C(0x01);
	UT_ASSERT(!cluster_reconfig_lmon_snapshot_replacement_grd_basis(
		survivors, &epoch));
	UT_ASSERT_EQ(memcmp(survivors, sentinel, sizeof(survivors)), 0);
	UT_ASSERT_EQ(epoch, epoch_sentinel);
}


/* ============================================================
 * T-reconfig-3 — lmon_tick dedup (same event_id → skip).
 * ============================================================ */

UT_TEST(test_reconfig_lmon_tick_dedups_same_event_id)
{
	uint64 first_apply, second_apply;

	ut_reset_mocks();
	reconfig_init_done = false;
	epoch_init_done = false;
	cluster_reconfig_shmem_init();
	cluster_epoch_shmem_init();

	/* Set up: node 0 self in_quorum, node 1 declared + DEAD, node 0 declared. */
	cluster_node_id = 0;
	ut_in_quorum_value = true;
	ut_declared_set[0] = true;
	ut_declared_set[1] = true;
	ut_peer_state[1] = CLUSTER_CSSD_PEER_DEAD;
	ut_dead_generation = 1;

	/* First tick: should fire (apply_counter 0 → 1). */
	cluster_reconfig_lmon_tick();
	first_apply = cluster_reconfig_get_apply_counter();

	/* Second tick: same dead_bitmap + same dead_gen → dedup skip. */
	cluster_reconfig_lmon_tick();
	second_apply = cluster_reconfig_get_apply_counter();

	UT_ASSERT_EQ((unsigned long long)first_apply, 1ULL);
	UT_ASSERT_EQ((unsigned long long)second_apply, 1ULL); /* unchanged */
}


UT_TEST(test_reconfig_lmon_tick_refires_on_dead_gen_bump)
{
	uint64 apply1, apply2;

	ut_reset_mocks();
	reconfig_init_done = false;
	epoch_init_done = false;
	cluster_reconfig_shmem_init();
	cluster_epoch_shmem_init();

	cluster_node_id = 0;
	ut_in_quorum_value = true;
	ut_declared_set[0] = true;
	ut_declared_set[1] = true;
	ut_peer_state[1] = CLUSTER_CSSD_PEER_DEAD;
	ut_dead_generation = 1;

	cluster_reconfig_lmon_tick();
	apply1 = cluster_reconfig_get_apply_counter();

	/* Rejoin-then-redeath:  dead_generation bumps;same dead_bitmap → new
	 * event_id → re-fire (P1.2). */
	ut_dead_generation = 2;
	cluster_reconfig_lmon_tick();
	apply2 = cluster_reconfig_get_apply_counter();

	UT_ASSERT_EQ((unsigned long long)apply1, 1ULL);
	UT_ASSERT_EQ((unsigned long long)apply2, 2ULL);
}


/* ============================================================
 * T-reconfig-4 + 4b — Q2 A'' rule + in_quorum gate.
 * ============================================================ */

UT_TEST(test_reconfig_lmon_tick_skips_when_not_in_quorum)
{
	uint64 apply_before;

	ut_reset_mocks();
	reconfig_init_done = false;
	cluster_reconfig_shmem_init();

	cluster_node_id = 0;
	ut_in_quorum_value = false; /* I2:  not in_quorum */
	ut_declared_set[0] = true;
	ut_declared_set[1] = true;
	ut_peer_state[1] = CLUSTER_CSSD_PEER_DEAD;

	apply_before = cluster_reconfig_get_apply_counter();
	cluster_reconfig_lmon_tick();
	UT_ASSERT_EQ((unsigned long long)cluster_reconfig_get_apply_counter(),
				 (unsigned long long)apply_before);
}


UT_TEST(test_reconfig_lmon_tick_skips_when_disabled)
{
	uint64 apply_before;

	ut_reset_mocks();
	reconfig_init_done = false;
	cluster_reconfig_shmem_init();

	cluster_enabled = false; /* L20: disable-cluster runtime gate */
	cluster_node_id = 0;
	ut_in_quorum_value = true;
	ut_declared_set[0] = true;
	ut_declared_set[1] = true;
	ut_peer_state[1] = CLUSTER_CSSD_PEER_DEAD;

	apply_before = cluster_reconfig_get_apply_counter();
	cluster_reconfig_lmon_tick();
	UT_ASSERT_EQ((unsigned long long)cluster_reconfig_get_apply_counter(),
				 (unsigned long long)apply_before);
}


UT_TEST(test_reconfig_lmon_tick_skips_on_empty_dead_bitmap)
{
	uint64 apply_before;

	ut_reset_mocks();
	reconfig_init_done = false;
	cluster_reconfig_shmem_init();

	cluster_node_id = 0;
	ut_in_quorum_value = true;
	ut_declared_set[0] = true;
	ut_declared_set[1] = true;
	/* All peers ALIVE — no dead_bitmap bits set. */

	apply_before = cluster_reconfig_get_apply_counter();
	cluster_reconfig_lmon_tick();
	UT_ASSERT_EQ((unsigned long long)cluster_reconfig_get_apply_counter(),
				 (unsigned long long)apply_before);
}


UT_TEST(test_reconfig_lmon_tick_undeclared_peer_ignored_F11)
{
	uint64 apply_before;

	ut_reset_mocks();
	reconfig_init_done = false;
	cluster_reconfig_shmem_init();

	cluster_node_id = 0;
	ut_in_quorum_value = true;
	ut_declared_set[0] = true;
	/* node 1 NOT declared.  CSSD peer_state defaults ALIVE (per
	 * cluster_cssd_get_peer_state shmem-NULL-safe behavior).  But our
	 * mock returns ALIVE anyway. */
	ut_declared_set[1] = false;
	ut_peer_state[1] = CLUSTER_CSSD_PEER_DEAD; /* irrelevant — filtered out */

	apply_before = cluster_reconfig_get_apply_counter();
	cluster_reconfig_lmon_tick();
	UT_ASSERT_EQ((unsigned long long)cluster_reconfig_get_apply_counter(),
				 (unsigned long long)apply_before);
}


/* ============================================================
 * T-reconfig-7 — broadcast vs epoch++ split (P1.3 I7).
 *
 *	When self is the coordinator: epoch advances.
 *	When self is NOT the coordinator: epoch stays (only piggyback via D20
 *	receive path would advance it, which is not exercised here).
 * ============================================================ */

UT_TEST(test_reconfig_lmon_tick_coordinator_advances_epoch)
{
	uint64 epoch_before, epoch_after;
	ReconfigEvent evt;

	ut_reset_mocks();
	reconfig_init_done = false;
	epoch_init_done = false;
	cluster_reconfig_shmem_init();
	cluster_epoch_shmem_init();

	/* self = node 0, node 1 dead → survivor_set = {0} → coordinator = 0 = self */
	cluster_node_id = 0;
	ut_in_quorum_value = true;
	ut_declared_set[0] = true;
	ut_declared_set[1] = true;
	ut_peer_state[1] = CLUSTER_CSSD_PEER_DEAD;
	ut_dead_generation = 1;

	epoch_before = cluster_epoch_get_current();
	cluster_reconfig_lmon_tick();
	epoch_after = cluster_epoch_get_current();

	UT_ASSERT_EQ((unsigned long long)epoch_before, 0ULL);
	UT_ASSERT_EQ((unsigned long long)epoch_after, 1ULL); /* coordinator bumped */

	cluster_reconfig_get_last_event(&evt);
	UT_ASSERT_EQ(evt.coordinator_node_id, 0);
	UT_ASSERT_EQ(evt.observer_role, CLUSTER_RECONFIG_OBSERVER_COORDINATOR);
	UT_ASSERT_EQ((unsigned long long)evt.new_epoch, 1ULL);
}


UT_TEST(test_reconfig_lmon_tick_survivor_does_not_advance_epoch)
{
	uint64 epoch_before, epoch_after;
	ReconfigEvent evt;

	ut_reset_mocks();
	reconfig_init_done = false;
	epoch_init_done = false;
	cluster_reconfig_shmem_init();
	cluster_epoch_shmem_init();

	/* self = node 1, node 2 dead → alive = {0, 1}, coord = 0, self != coord */
	cluster_node_id = 1;
	ut_in_quorum_value = true;
	ut_declared_set[0] = true;
	ut_declared_set[1] = true;
	ut_declared_set[2] = true;
	ut_peer_state[0] = CLUSTER_CSSD_PEER_ALIVE;
	ut_peer_state[2] = CLUSTER_CSSD_PEER_DEAD;
	ut_dead_generation = 1;
	/*
	 * RF-ROOT P9 verification / cold-formation ruling: an ABSENT declared
	 * peer at the INITIAL epoch is admitted only by the WHOLE-round QVOTEC
	 * bootstrap proof — its fresh co-boot slot at INITIAL + the same-round
	 * in-quorum snapshot (never from live CSSD alone).  Mock the qvotec
	 * publication for node 0 so its ABSENT -> MEMBER admission lands and
	 * the survivor/coordinator math stays node-0-as-coordinator.
	 */
	cluster_reconfig_record_observed_slot(0, UINT64_C(77), UINT64_C(1), 0);
	cluster_reconfig_record_observed_fresh_alive(0, true);
	cluster_reconfig_bootstrap_publish_in_quorum(true);

	epoch_before = cluster_epoch_get_current();
	cluster_reconfig_lmon_tick();
	epoch_after = cluster_epoch_get_current();

	UT_ASSERT_EQ((unsigned long long)epoch_before, 0ULL);
	/* I7:  non-coord survivor MUST NOT advance epoch — that's coord's job. */
	UT_ASSERT_EQ((unsigned long long)epoch_after, 0ULL);

	cluster_reconfig_get_last_event(&evt);
	UT_ASSERT_EQ(evt.coordinator_node_id, 0);
	UT_ASSERT_EQ(evt.observer_role, CLUSTER_RECONFIG_OBSERVER_SURVIVOR);
}


/* ============================================================
 * T-reconfig-2.29a — P1-1 staged-record hard assertions (spec-2.29a
 * review r1 P1-c).
 *
 *	(a) fail-stop fence stage: while the marker is PENDING across ticks
 *	    the epoch is bumped EXACTLY ONCE and nothing publishes; the
 *	    staged event publishes only on ACKED+ACK.
 *	(b) node-remove stage: a tick re-entry while the fence marker is
 *	    PENDING returns 0 with *out_contest == false (no false contest
 *	    against our own already-advanced baseline) and no re-bump.
 * ============================================================ */

UT_TEST(test_reconfig_failstop_fence_stage_bump_once_while_pending)
{
	uint64 epoch_after_bump;
	uint64 apply_before;

	ut_reset_mocks();
	reconfig_init_done = false;
	epoch_init_done = false;
	cluster_reconfig_shmem_init();
	cluster_epoch_shmem_init();

	cluster_node_id = 0;
	ut_in_quorum_value = true;
	ut_declared_set[0] = true;
	ut_declared_set[1] = true;
	ut_peer_state[1] = CLUSTER_CSSD_PEER_DEAD;
	ut_dead_generation = 1;

	cluster_write_fence_enforcement = CLUSTER_WRITE_FENCE_ENFORCE_ON;
	MyBackendType = B_LMON;
	ut_fence_async_submit_ok = true;
	ut_fence_async_poll_pr = CLUSTER_MARKER_POLL_PENDING;
	ut_fence_async_poll_result = CLUSTER_FENCE_MARKER_SUBMIT_FAILED;

	apply_before = cluster_reconfig_get_apply_counter();

	/* tick #1: stage-entry — bump once, submit, DO NOT publish. */
	cluster_reconfig_lmon_tick();
	epoch_after_bump = cluster_epoch_get_current();
	UT_ASSERT(epoch_after_bump > 0);
	UT_ASSERT_EQ((unsigned long long)cluster_reconfig_get_apply_counter(),
				 (unsigned long long)apply_before);

	/* ticks #2/#3: marker PENDING — the bump path must NOT re-enter
	 * (P1-1 invariant 0: zero re-bump, zero publish, zero side-effects). */
	cluster_reconfig_lmon_tick();
	cluster_reconfig_lmon_tick();
	UT_ASSERT_EQ((unsigned long long)cluster_epoch_get_current(),
				 (unsigned long long)epoch_after_bump);
	UT_ASSERT_EQ((unsigned long long)cluster_reconfig_get_apply_counter(),
				 (unsigned long long)apply_before);

	/* tick #4: ACKED + ACK — the STAGED event publishes; still no re-bump. */
	ut_fence_async_poll_pr = CLUSTER_MARKER_POLL_ACKED;
	ut_fence_async_poll_result = CLUSTER_FENCE_MARKER_SUBMIT_ACK;
	cluster_reconfig_lmon_tick();
	UT_ASSERT_EQ((unsigned long long)cluster_epoch_get_current(),
				 (unsigned long long)epoch_after_bump);
	UT_ASSERT_EQ((unsigned long long)cluster_reconfig_get_apply_counter(),
				 (unsigned long long)(apply_before + 1));

	/* restore fixture defaults for the neighboring tests */
	MyBackendType = B_INVALID;
	cluster_write_fence_enforcement = CLUSTER_WRITE_FENCE_ENFORCE_OFF;
	ut_fence_async_submit_ok = false;
	ut_fence_async_poll_pr = CLUSTER_MARKER_POLL_IDLE;
	ut_fence_async_poll_result = CLUSTER_FENCE_MARKER_SUBMIT_FAILED;
}

UT_TEST(test_reconfig_failstop_marker_unions_prior_excluded_with_new_delta)
{
	ReconfigEvent prior;
	ReconfigEvent applied;

	ut_reset_mocks();
	reconfig_init_done = false;
	epoch_init_done = false;
	cluster_reconfig_shmem_init();
	cluster_epoch_shmem_init();

	cluster_node_id = 0;
	ut_in_quorum_value = true;
	ut_declared_set[0] = true;
	ut_declared_set[1] = true;
	ut_declared_set[2] = true;
	ut_peer_state[1] = CLUSTER_CSSD_PEER_ALIVE;
	ut_peer_state[2] = CLUSTER_CSSD_PEER_DEAD;
	ut_dead_generation = UINT64_C(2);

	/* Node 1 is still excluded even though its process is visible ALIVE: no
	 * COMMITTED JCMK + ROOT gate has cleared it.  Node 2 is the new delta. */
	memset(&prior, 0, sizeof(prior));
	prior.event_id = UINT64_C(401);
	prior.coordinator_node_id = 0;
	prior.new_epoch = cluster_epoch_get_current();
	prior.dead_bitmap[0] = UINT8_C(0x02);
	prior.cssd_dead_generation = UINT64_C(1);
	prior.reconfig_kind = RECONFIG_KIND_FAIL_STOP;
	cluster_reconfig_publish_event(&prior);

	cluster_write_fence_enforcement = CLUSTER_WRITE_FENCE_ENFORCE_ON;
	MyBackendType = B_LMON;
	ut_fence_async_submit_ok = true;
	ut_fence_async_poll_pr = CLUSTER_MARKER_POLL_PENDING;
	ut_fence_async_poll_result = CLUSTER_FENCE_MARKER_SUBMIT_FAILED;

	cluster_reconfig_lmon_tick();
	UT_ASSERT(ut_fence_async_marker_captured);
	UT_ASSERT_EQ(ut_fence_async_marker.fenced_dead_bitmap[0], UINT8_C(0x06));

	ut_fence_async_poll_pr = CLUSTER_MARKER_POLL_ACKED;
	ut_fence_async_poll_result = CLUSTER_FENCE_MARKER_SUBMIT_ACK;
	cluster_reconfig_lmon_tick();
	cluster_reconfig_get_last_event(&applied);
	UT_ASSERT_EQ((int)applied.reconfig_kind, (int)RECONFIG_KIND_FAIL_STOP);
	UT_ASSERT_EQ(applied.dead_bitmap[0], UINT8_C(0x06));

	MyBackendType = B_INVALID;
	cluster_write_fence_enforcement = CLUSTER_WRITE_FENCE_ENFORCE_OFF;
	ut_fence_async_submit_ok = false;
	ut_fence_async_poll_pr = CLUSTER_MARKER_POLL_IDLE;
	ut_fence_async_poll_result = CLUSTER_FENCE_MARKER_SUBMIT_FAILED;
}


UT_TEST(test_reconfig_node_remove_stage_no_false_contest)
{
	uint64 baseline;
	uint64 epoch_after_bump;
	uint64 ret;
	bool contest;

	ut_reset_mocks();
	reconfig_init_done = false;
	epoch_init_done = false;
	cluster_reconfig_shmem_init();
	cluster_epoch_shmem_init();

	cluster_node_id = 0;
	ut_in_quorum_value = true;
	ut_declared_set[0] = true;
	ut_declared_set[1] = true;

	cluster_write_fence_enforcement = CLUSTER_WRITE_FENCE_ENFORCE_ON;
	MyBackendType = B_LMON;
	ut_fence_async_submit_ok = true;
	ut_fence_async_poll_pr = CLUSTER_MARKER_POLL_PENDING;
	ut_fence_async_poll_result = CLUSTER_FENCE_MARKER_SUBMIT_FAILED;

	baseline = cluster_epoch_get_current();

	/* call #1: guarded CAS advances baseline→baseline+1, stages the fence
	 * marker, returns 0 (not yet committed), contest MUST be false. */
	contest = true;
	ret = cluster_reconfig_apply_node_removed_as_coordinator(1, baseline, 42, 7, &contest);
	UT_ASSERT_EQ((unsigned long long)ret, 0ULL);
	UT_ASSERT(!contest);
	epoch_after_bump = cluster_epoch_get_current();
	UT_ASSERT_EQ((unsigned long long)epoch_after_bump, (unsigned long long)(baseline + 1));

	/* call #2 (tick re-entry with the SAME stale baseline while PENDING):
	 * without the staged record this would re-run the baseline CAS against
	 * our own advanced epoch and report a FALSE contest.  P1-1: it must
	 * poll the stage instead — 0, contest==false, no second bump. */
	contest = true;
	ret = cluster_reconfig_apply_node_removed_as_coordinator(1, baseline, 42, 7, &contest);
	UT_ASSERT_EQ((unsigned long long)ret, 0ULL);
	UT_ASSERT(!contest);
	UT_ASSERT_EQ((unsigned long long)cluster_epoch_get_current(),
				 (unsigned long long)epoch_after_bump);

	/* call #3: ACKED + ACK — removal publishes at the STAGED epoch. */
	ut_fence_async_poll_pr = CLUSTER_MARKER_POLL_ACKED;
	ut_fence_async_poll_result = CLUSTER_FENCE_MARKER_SUBMIT_ACK;
	contest = true;
	ret = cluster_reconfig_apply_node_removed_as_coordinator(1, baseline, 42, 7, &contest);
	UT_ASSERT_EQ((unsigned long long)ret, (unsigned long long)epoch_after_bump);
	UT_ASSERT(!contest);
	UT_ASSERT_EQ((unsigned long long)cluster_epoch_get_current(),
				 (unsigned long long)epoch_after_bump);

	MyBackendType = B_INVALID;
	cluster_write_fence_enforcement = CLUSTER_WRITE_FENCE_ENFORCE_OFF;
	ut_fence_async_submit_ok = false;
	ut_fence_async_poll_pr = CLUSTER_MARKER_POLL_IDLE;
	ut_fence_async_poll_result = CLUSTER_FENCE_MARKER_SUBMIT_FAILED;
}


UT_TEST(test_reconfig_node_remove_marker_carries_full_excluded_set)
{
	ReconfigEvent applied;
	uint64 baseline;
	uint64 ret;
	bool contest;

	ut_reset_mocks();
	reconfig_init_done = false;
	epoch_init_done = false;
	cluster_reconfig_shmem_init();
	cluster_epoch_shmem_init();

	cluster_node_id = 0;
	ut_in_quorum_value = true;
	ut_declared_set[0] = true;
	ut_declared_set[1] = true;
	ut_declared_set[2] = true;
	ut_declared_set[3] = true;

	/* Frozen §17.6 producer rule: removal of node 1 must retain the
	 * current applied DEAD node 2 and the already REMOVED node 3. */
	memset(&applied, 0, sizeof(applied));
	applied.event_id = UINT64_C(101);
	applied.coordinator_node_id = 0;
	applied.old_epoch = 0;
	applied.new_epoch = cluster_epoch_get_current();
	applied.dead_bitmap[2 / 8] |= (uint8)(1u << (2 % 8));
	applied.cssd_dead_generation = UINT64_C(7);
	applied.reconfig_kind = RECONFIG_KIND_FAIL_STOP;
	cluster_reconfig_publish_event(&applied);
	cluster_reconfig_record_removed(3, applied.new_epoch, false);

	cluster_write_fence_enforcement = CLUSTER_WRITE_FENCE_ENFORCE_ON;
	MyBackendType = B_LMON;
	ut_fence_async_submit_ok = true;
	ut_fence_async_poll_pr = CLUSTER_MARKER_POLL_PENDING;
	ut_fence_async_poll_result = CLUSTER_FENCE_MARKER_SUBMIT_FAILED;

	baseline = cluster_epoch_get_current();
	contest = true;
	ret = cluster_reconfig_apply_node_removed_as_coordinator(1, baseline, 42, 7, &contest);
	UT_ASSERT_EQ((unsigned long long)ret, 0ULL);
	UT_ASSERT(!contest);
	UT_ASSERT(ut_fence_async_marker_captured);
	UT_ASSERT((ut_fence_async_marker.fenced_dead_bitmap[0] & UINT8_C(0x02)) != 0);
	UT_ASSERT((ut_fence_async_marker.fenced_dead_bitmap[0] & UINT8_C(0x04)) != 0);
	UT_ASSERT((ut_fence_async_marker.fenced_dead_bitmap[0] & UINT8_C(0x08)) != 0);

	/* After the marker ACK, the applied event must retain the pre-existing
	 * DEAD set so the next steady-state baseline cannot release node 2. */
	ut_fence_async_poll_pr = CLUSTER_MARKER_POLL_ACKED;
	ut_fence_async_poll_result = CLUSTER_FENCE_MARKER_SUBMIT_ACK;
	contest = true;
	ret = cluster_reconfig_apply_node_removed_as_coordinator(1, baseline, 42, 7,
												 &contest);
	UT_ASSERT(ret > baseline);
	UT_ASSERT(!contest);
	cluster_reconfig_get_last_event(&applied);
	UT_ASSERT_EQ((int)applied.reconfig_kind, (int)RECONFIG_KIND_NODE_REMOVED);
	UT_ASSERT((applied.dead_bitmap[0] & UINT8_C(0x04)) != 0);

	MyBackendType = B_INVALID;
	cluster_write_fence_enforcement = CLUSTER_WRITE_FENCE_ENFORCE_OFF;
	ut_fence_async_submit_ok = false;
	ut_fence_async_poll_pr = CLUSTER_MARKER_POLL_IDLE;
	ut_fence_async_poll_result = CLUSTER_FENCE_MARKER_SUBMIT_FAILED;
}

UT_TEST(test_clean_leave_preserves_prior_excluded_set)
{
	ReconfigEvent prior;
	ReconfigEvent applied;
	uint64 baseline;
	uint64 committed;

	ut_reset_mocks();
	reconfig_init_done = false;
	epoch_init_done = false;
	cluster_reconfig_shmem_init();
	cluster_epoch_shmem_init();
	cluster_node_id = 0;

	memset(&prior, 0, sizeof(prior));
	prior.event_id = UINT64_C(451);
	prior.coordinator_node_id = 0;
	prior.new_epoch = cluster_epoch_get_current();
	prior.dead_bitmap[0] = UINT8_C(0x04); /* already-excluded node 2 */
	prior.cssd_dead_generation = UINT64_C(1);
	prior.reconfig_kind = RECONFIG_KIND_FAIL_STOP;
	cluster_reconfig_publish_event(&prior);

	baseline = cluster_epoch_get_current();
	committed = cluster_reconfig_apply_clean_leave_as_coordinator(1, baseline);
	UT_ASSERT_EQ(committed, baseline + 1);
	cluster_reconfig_get_last_event(&applied);
	UT_ASSERT_EQ((int)applied.reconfig_kind, (int)RECONFIG_KIND_CLEAN_LEAVE);
	UT_ASSERT_EQ(applied.dead_bitmap[0], UINT8_C(0x06));
}


/* ============================================================
 * T-reconfig-8 — ProcessInterrupts I6 guard (D4).
 *
 *	Verify: when pending=true but IsTransactionState()=false (idle/
 *	post-commit cleanup), no ereport fires.  Pending flag is cleared.
 *	When pending=true AND IsTransactionState()=true, ereport fires
 *	(verified via setjmp catcher).
 * ============================================================ */

UT_TEST(test_reconfig_check_pending_disabled_silent)
{
	ut_reset_mocks();
	cluster_enabled = false;
	cluster_reconfig_start_pending = 1;
	ut_in_tx_state = true;

	cluster_reconfig_check_pending_in_proc_interrupts();

	UT_ASSERT_EQ(ut_ereport_fired_count, 0);
	/* pending NOT cleared when cluster.enabled=off — early return before
	 * read-clear (matches cluster_fence pattern). */
	UT_ASSERT_EQ((int)cluster_reconfig_start_pending, 1);
}


UT_TEST(test_reconfig_check_pending_no_pending_fast_path)
{
	ut_reset_mocks();
	cluster_reconfig_start_pending = 0;
	ut_in_tx_state = true;

	cluster_reconfig_check_pending_in_proc_interrupts();

	UT_ASSERT_EQ(ut_ereport_fired_count, 0);
	UT_ASSERT_EQ((int)cluster_reconfig_start_pending, 0);
}


UT_TEST(test_reconfig_check_pending_idle_absorbs_I6)
{
	ut_reset_mocks();
	cluster_reconfig_start_pending = 1;
	ut_in_tx_state = false; /* idle / post-commit cleanup tail */

	cluster_reconfig_check_pending_in_proc_interrupts();

	UT_ASSERT_EQ(ut_ereport_fired_count, 0);
	/* I6:  pending cleared (read-clear-FIRST) even though we absorbed. */
	UT_ASSERT_EQ((int)cluster_reconfig_start_pending, 0);
}


UT_TEST(test_reconfig_check_pending_read_only_xact_absorbs)
{
	ut_reset_mocks();
	cluster_reconfig_start_pending = 1;
	ut_in_tx_state = true;
	ut_top_xid = InvalidTransactionId; /* no writes yet */
	ut_in_quorum_value = true;

	cluster_reconfig_check_pending_in_proc_interrupts();

	UT_ASSERT_EQ(ut_ereport_fired_count, 0);
	UT_ASSERT_EQ((int)cluster_reconfig_start_pending, 0);
}


UT_TEST(test_reconfig_check_pending_in_tx_quorum_lost_errors)
{
	ut_reset_mocks();
	cluster_reconfig_start_pending = 1;
	ut_in_tx_state = true;
	ut_top_xid = 42;			/* writable tx */
	ut_in_quorum_value = false; /* quorum lost → 53R50 branch */

	if (sigsetjmp(ut_ereport_jump, 1) == 0) {
		ut_ereport_jump_armed = true;
		cluster_reconfig_check_pending_in_proc_interrupts();
		UT_ASSERT(false); /* should have ereport ERROR */
	} else {
		ut_ereport_jump_armed = false;
		UT_ASSERT_EQ(ut_ereport_fired_count, 1);
	}
}


UT_TEST(test_reconfig_check_pending_in_tx_in_quorum_53R60_errors)
{
	ut_reset_mocks();
	cluster_reconfig_start_pending = 1;
	ut_in_tx_state = true;
	ut_top_xid = 42;		   /* writable tx */
	ut_in_quorum_value = true; /* in_quorum → 53R60 reconfig_in_progress */

	if (sigsetjmp(ut_ereport_jump, 1) == 0) {
		ut_ereport_jump_armed = true;
		cluster_reconfig_check_pending_in_proc_interrupts();
		UT_ASSERT(false);
	} else {
		ut_ereport_jump_armed = false;
		UT_ASSERT_EQ(ut_ereport_fired_count, 1);
	}
}


/* ============================================================
 * spec-5.14 U6 — reconfig_kind field round-trip + enum boundary.
 * ============================================================ */
UT_TEST(test_reconfig_kind_field_roundtrip)
{
	ReconfigEvent evt;
	ReconfigEvent got;

	/* enum constants are the on-the-wire-free shmem contract. */
	UT_ASSERT_EQ((int)RECONFIG_KIND_NONE, 0);
	UT_ASSERT_EQ((int)RECONFIG_KIND_FAIL_STOP, 1);
	UT_ASSERT_EQ((int)RECONFIG_KIND_CLEAN_LEAVE, 2);

	cluster_reconfig_shmem_init();

	memset(&evt, 0, sizeof(evt));
	evt.event_id = 999;
	evt.reconfig_kind = RECONFIG_KIND_FAIL_STOP;
	cluster_reconfig_publish_event(&evt);

	cluster_reconfig_get_last_event(&got);
	UT_ASSERT_EQ((int)got.reconfig_kind, (int)RECONFIG_KIND_FAIL_STOP);

	/* CLEAN_LEAVE field also round-trips (defensive D4 path reachability). */
	memset(&evt, 0, sizeof(evt));
	evt.event_id = 1000;
	evt.reconfig_kind = RECONFIG_KIND_CLEAN_LEAVE;
	cluster_reconfig_publish_event(&evt);
	cluster_reconfig_get_last_event(&got);
	UT_ASSERT_EQ((int)got.reconfig_kind, (int)RECONFIG_KIND_CLEAN_LEAVE);
}


/* ============================================================
 * spec-5.14 U8 — classify_verdict decision matrix (§3.2), pure.
 *	4 quadrants (read/write × touched/non-touched) × quorum.
 * ============================================================ */
UT_TEST(test_reconfig_classify_verdict_matrix)
{
	/* touched (read OR write) + in quorum -> 40R01 (ABORT_TOUCHED) */
	UT_ASSERT_EQ((int)cluster_reconfig_classify_verdict(true, false, true),
				 (int)RECONFIG_VERDICT_ABORT_TOUCHED);
	UT_ASSERT_EQ((int)cluster_reconfig_classify_verdict(true, true, true),
				 (int)RECONFIG_VERDICT_ABORT_TOUCHED);

	/* touched + lost quorum -> 53R50 (ABORT_QUORUM, terminal) */
	UT_ASSERT_EQ((int)cluster_reconfig_classify_verdict(true, false, false),
				 (int)RECONFIG_VERDICT_ABORT_QUORUM);
	UT_ASSERT_EQ((int)cluster_reconfig_classify_verdict(true, true, false),
				 (int)RECONFIG_VERDICT_ABORT_QUORUM);

	/* non-touched read-only -> ABSORB (INV-TP5), regardless of quorum */
	UT_ASSERT_EQ((int)cluster_reconfig_classify_verdict(false, false, true),
				 (int)RECONFIG_VERDICT_ABSORB);
	UT_ASSERT_EQ((int)cluster_reconfig_classify_verdict(false, false, false),
				 (int)RECONFIG_VERDICT_ABSORB);

	/* non-touched writable + in quorum -> 53R60 (ABORT_RECONFIG) */
	UT_ASSERT_EQ((int)cluster_reconfig_classify_verdict(false, true, true),
				 (int)RECONFIG_VERDICT_ABORT_RECONFIG);

	/* non-touched writable + lost quorum -> 53R50 (ABORT_QUORUM) */
	UT_ASSERT_EQ((int)cluster_reconfig_classify_verdict(false, true, false),
				 (int)RECONFIG_VERDICT_ABORT_QUORUM);
}


/* ============================================================
 * spec-5.15 D1 — cluster_reconfig_compute_join_bitmap (join-edge detection).
 *
 *	These exercise the join-edge detector against the membership SSOT + the
 *	qvotec-published observed slots (U6-U9 of the spec §4.1 list; they live here
 *	rather than test_cluster_membership.c because the detector reads
 *	ClusterReconfigState + the CSSD/conf mocks this fixture already provides).
 * ============================================================ */

/* test a bit in a returned join_bitmap (mirrors the module-private helper). */
static bool
jb_test(const uint8 *bmp, int i)
{
	return (bmp[i / 8] & (uint8)(1u << (i % 8))) != 0;
}

/* fresh reconfig shmem (membership all ABSENT, observed all 0) + clean mocks. */
static void
ut_join_setup(void)
{
	ClusterQvotecMailbox authority_mailbox;
	pg_atomic_uint32 qvotec_status;

	ut_reset_mocks();
	reconfig_init_done = false;
	cluster_reconfig_shmem_init(); /* memset clean + attach membership table */
	/*
	 * RF-ROOT P9 verification / cold-formation ruling: the founding
	 * bootstrap proof (cluster_reconfig_bootstrap_quorum_at_initial) and the
	 * cold-formation ABSENT admission now require the LOCAL epoch to still be
	 * CLUSTER_EPOCH_INITIAL — a cold co-boot of an EXISTING cluster recovers
	 * epoch > INITIAL from the voting disk and must never self-admit through
	 * the founding proof (that is the exact bug B′ fixes).  The shared epoch
	 * storage carries a stale non-INITIAL value across tests (earlier
	 * coordinator bumps), so reset it here for a clean baseline; tests that
	 * need a specific epoch advance it explicitly afterwards.
	 */
	epoch_init_done = false;
	cluster_epoch_shmem_init();
	cluster_qvotec_mailbox_restart_reset(&authority_mailbox);
	pg_atomic_init_u32(&qvotec_status, CLUSTER_QVOTEC_READY);
	UT_ASSERT(cluster_reconfig_qvotec_lifecycle_transition(
		&authority_mailbox, &qvotec_status, CLUSTER_QVOTEC_STARTING));
	UT_ASSERT(cluster_reconfig_qvotec_lifecycle_transition(
		&authority_mailbox, &qvotec_status, CLUSTER_QVOTEC_READY));
	cluster_node_id = 0;		   /* self */
	ut_declared_set[0] = true;	   /* self declared */
}

static ClusterReplacementEpisode
ut_admitted_replacement_episode(int32 target_node_id)
{
	ClusterReplacementEpisode episode;

	memset(&episode, 0, sizeof(episode));
	episode.request_nonce = UINT64_C(0x1112131415161718);
	episode.baseline_epoch = UINT64_C(40);
	episode.reserved_or_committed_epoch = UINT64_C(41);
	episode.old_admitted_incarnation = UINT64_C(70);
	episode.fresh_incarnation = UINT64_C(71);
	episode.grammar_fingerprint = CLUSTER_REPLACEMENT_EPISODE_GRAMMAR_FINGERPRINT;
	episode.expected_survivors[0] = UINT8_C(0x06); /* nodes 1 and 2 */
	episode.acknowledgements[0] = UINT8_C(0x06);
	episode.target_node_id = target_node_id;
	episode.coordinator_node_id = 1;
	episode.state_generation = UINT32_C(9);
	episode.phase = CLUSTER_REPLACEMENT_EPISODE_ADMITTED;
	episode.readiness_flags = CLUSTER_REPLACEMENT_EPISODE_READINESS_MASK;
	return episode;
}

static void
ut_prepare_exact_r4_membership(void)
{
	int node;

	ut_join_setup();
	ut_in_quorum_value = true;
	ut_set_self_incarnation_sequence(UINT64_C(100), UINT64_C(100),
								 UINT64_C(100));
	for (node = 0; node < 4; node++) {
		uint64 incarnation = UINT64_C(100) + (uint64)node;

		ut_declared_set[node] = true;
		cluster_membership_record_admitted(node, incarnation);
		cluster_membership_set_state(node, CLUSTER_MEMBER_MEMBER);
		cluster_reconfig_record_observed_slot(
			node, incarnation, UINT64_C(20) + (uint64)node,
			CLUSTER_EPOCH_INITIAL);
		cluster_reconfig_record_observed_fresh_alive(node, true);
	}
}

UT_TEST(test_r4_membership_snapshot_captures_exact_current_four_node_view)
{
	ClusterR4MembershipSnapshot snapshot;
	int node;

	ut_prepare_exact_r4_membership();
	memset(&snapshot, 0xa5, sizeof(snapshot));
	UT_ASSERT(cluster_reconfig_lmon_snapshot_r4_membership(&snapshot));
	UT_ASSERT_EQ(snapshot.formation_epoch, CLUSTER_EPOCH_INITIAL);
	UT_ASSERT_EQ(snapshot.admitted_members_lo, UINT64_C(0x0f));
	UT_ASSERT_EQ(snapshot.admitted_members_hi, 0);
	UT_ASSERT_EQ(snapshot.local_self_boot_incarnation, UINT64_C(100));
	for (node = 0; node < 4; node++) {
		UT_ASSERT_EQ(snapshot.admitted_incarnation[node],
					 UINT64_C(100) + (uint64)node);
		UT_ASSERT_EQ(snapshot.observed_generation[node],
					 UINT64_C(20) + (uint64)node);
	}
	for (; node < CLUSTER_MAX_NODES; node++) {
		UT_ASSERT_EQ(snapshot.admitted_incarnation[node], 0);
		UT_ASSERT_EQ(snapshot.observed_generation[node], 0);
	}
}

UT_TEST(test_r4_membership_snapshot_fails_closed_on_inexact_member_evidence)
{
	ClusterR4MembershipSnapshot snapshot;

	ut_prepare_exact_r4_membership();
	cluster_membership_set_state(3, CLUSTER_MEMBER_JOINING);
	UT_ASSERT(cluster_reconfig_lmon_snapshot_r4_membership(&snapshot));
	UT_ASSERT_EQ(snapshot.admitted_members_lo, UINT64_C(0x07));
	UT_ASSERT_EQ(snapshot.admitted_incarnation[3], 0);

	ut_prepare_exact_r4_membership();
	cluster_reconfig_record_observed_slot(
		3, UINT64_C(999), UINT64_C(23), CLUSTER_EPOCH_INITIAL);
	UT_ASSERT(!cluster_reconfig_lmon_snapshot_r4_membership(&snapshot));
	UT_ASSERT_EQ(snapshot.admitted_members_lo, 0);

	ut_prepare_exact_r4_membership();
	cluster_reconfig_record_observed_fresh_alive(2, false);
	UT_ASSERT(!cluster_reconfig_lmon_snapshot_r4_membership(&snapshot));
	UT_ASSERT_EQ(snapshot.admitted_members_lo, 0);

	ut_prepare_exact_r4_membership();
	ut_set_self_incarnation_sequence(UINT64_C(101), UINT64_C(101),
								 UINT64_C(101));
	UT_ASSERT(!cluster_reconfig_lmon_snapshot_r4_membership(&snapshot));
	UT_ASSERT_EQ(snapshot.admitted_members_lo, 0);
}

/* A decoded phase-1 frame is only a purge candidate after the current local
 * state exact-binds it to the settled RESERVE, its acting ballot proposer and
 * the majority-durable PREPARE.  Authorization itself must mutate nothing. */
UT_TEST(test_replacement_purge_request_requires_exact_reserve_prepare_authority)
{
	ClusterReconfigState *state;
	ClusterReplacementEpisode episode;
	ClusterReplacementEpisode before;
	ClusterReplacementCommitMarkerV3 prepare;
	ClusterEpochAuthorityValue reserve;
	ClusterEpochBallotId ballot;
	ClusterReplacementWireMessage request;
	ClusterReplacementWireMessage observed;
	ClusterReplacementWireMessage sentinel;
	ClusterICEnvelope envelope;
	uint8 request_bytes[CLUSTER_REPLACEMENT_WIRE_BYTES];

	ut_join_setup();
	cluster_node_id = 2;
	state = (ClusterReconfigState *)reconfig_shmem_storage;
	episode = ut_admitted_replacement_episode(3);
	episode.phase = CLUSTER_REPLACEMENT_EPISODE_PREPARE_DURABLE;
	episode.readiness_flags = 0;
	memset(episode.acknowledgements, 0,
		   sizeof(episode.acknowledgements));
	state->replacement_episode = episode;
	cluster_membership_set_state(1, CLUSTER_MEMBER_MEMBER);
	cluster_membership_set_state(2, CLUSTER_MEMBER_MEMBER);
	cluster_membership_record_admitted(1, UINT64_C(111));
	cluster_membership_record_admitted(
		episode.target_node_id, episode.old_admitted_incarnation);
	cluster_membership_record_admitted(2, UINT64_C(222));
	epoch_init_done = false;
	cluster_epoch_shmem_init();
	UT_ASSERT(cluster_epoch_observe_remote(episode.baseline_epoch));

	memset(&prepare, 0, sizeof(prepare));
	prepare.magic = CLUSTER_JCMK_MAGIC;
	prepare.version = CLUSTER_JCMK_REPLACEMENT_VERSION;
	prepare.target_node_id = episode.target_node_id;
	prepare.phase = CLUSTER_JCMK_REPLACEMENT_PHASE_PREPARE;
	prepare.generation = UINT64_C(41);
	prepare.old_admitted_incarnation = episode.old_admitted_incarnation;
	prepare.fresh_incarnation = episode.fresh_incarnation;
	prepare.baseline_epoch = episode.baseline_epoch;
	prepare.reserved_or_committed_epoch
		= episode.reserved_or_committed_epoch;
	prepare.request_nonce = episode.request_nonce;
	memcpy(prepare.expected_purge_survivors, episode.expected_survivors,
		   sizeof(prepare.expected_purge_survivors));
	prepare.grammar_fingerprint = episode.grammar_fingerprint;

	memset(&reserve, 0, sizeof(reserve));
	reserve.value_version = CLUSTER_EPOCH_AUTHORITY_VALUE_VERSION;
	reserve.transition = CLUSTER_EPOCH_AUTHORITY_RESERVE;
	reserve.event_kind = CLUSTER_EPOCH_EVENT_SAME_NODE_REPLACEMENT;
	reserve.request_origin_node = episode.target_node_id;
	reserve.target_node_id = episode.target_node_id;
	reserve.authority_generation = UINT64_C(101);
	reserve.baseline_epoch = episode.baseline_epoch;
	reserve.reserved_epoch = episode.reserved_or_committed_epoch;
	reserve.old_incarnation = episode.old_admitted_incarnation;
	reserve.fresh_incarnation = episode.fresh_incarnation;
	reserve.request_nonce = episode.request_nonce;
	memcpy(reserve.authority_member_bitmap, episode.expected_survivors,
		   sizeof(reserve.authority_member_bitmap));
	reserve.event_subject_bitmap[episode.target_node_id / 8]
		= (uint8)(1u << (episode.target_node_id % 8));
	reserve.grammar_fingerprint = episode.grammar_fingerprint;
	memset(reserve.predecessor_digest, 0x5a,
		   sizeof(reserve.predecessor_digest));

	memset(&ballot, 0, sizeof(ballot));
	ballot.counter = UINT64_C(7);
	ballot.proposer_node_id = episode.coordinator_node_id;
	ballot.proposer_admitted_incarnation = UINT64_C(111);
	ballot.nonce = UINT64_C(0x5152535455565758);

	memset(&request, 0, sizeof(request));
	request.phase = CLUSTER_REPLACEMENT_WIRE_PHASE_PURGE_REQUEST;
	request.target_node_id = episode.target_node_id;
	request.epoch = episode.baseline_epoch;
	request.request_nonce = episode.request_nonce;
	request.identity0 = episode.old_admitted_incarnation;
	request.identity1 = episode.fresh_incarnation;
	memcpy(request.body.bitmap, episode.expected_survivors,
		   sizeof(request.body.bitmap));
	request.grammar_fingerprint = episode.grammar_fingerprint;
	before = state->replacement_episode;

	UT_ASSERT(cluster_reconfig_replacement_purge_request_authorized(
		&request, episode.coordinator_node_id, cluster_node_id,
		&reserve, &ballot, &prepare));
	UT_ASSERT_EQ(memcmp(&state->replacement_episode, &before,
					  sizeof(before)), 0);

	UT_ASSERT(cluster_replacement_wire_encode(&request, request_bytes));
	memset(&envelope, 0, sizeof(envelope));
	envelope.msg_type = PGRAC_IC_MSG_GES_REQUEST;
	envelope.source_node_id = (uint32)episode.coordinator_node_id;
	envelope.dest_node_id = (uint32)cluster_node_id;
	envelope.epoch = episode.baseline_epoch;
	envelope.payload_length = CLUSTER_REPLACEMENT_WIRE_BYTES;
	memset(&observed, 0, sizeof(observed));
	UT_ASSERT(cluster_reconfig_replacement_purge_request_ingress_authorized(
		&envelope, request_bytes, sizeof(request_bytes),
		episode.coordinator_node_id, cluster_node_id,
		&reserve, &ballot, &prepare, &observed));
	UT_ASSERT_EQ(memcmp(&observed, &request, sizeof(request)), 0);

	memset(&sentinel, 0xa5, sizeof(sentinel));
	observed = sentinel;
	envelope.source_node_id = (uint32)cluster_node_id;
	UT_ASSERT(!cluster_reconfig_replacement_purge_request_ingress_authorized(
		&envelope, request_bytes, sizeof(request_bytes),
		episode.coordinator_node_id, cluster_node_id,
		&reserve, &ballot, &prepare, &observed));
	UT_ASSERT_EQ(memcmp(&observed, &sentinel, sizeof(sentinel)), 0);
	envelope.source_node_id = (uint32)episode.coordinator_node_id;
	envelope.epoch++;
	UT_ASSERT(!cluster_reconfig_replacement_purge_request_ingress_authorized(
		&envelope, request_bytes, sizeof(request_bytes),
		episode.coordinator_node_id, cluster_node_id,
		&reserve, &ballot, &prepare, &observed));
	UT_ASSERT_EQ(memcmp(&observed, &sentinel, sizeof(sentinel)), 0);
	envelope.epoch--;
	UT_ASSERT(!cluster_reconfig_replacement_purge_request_ingress_authorized(
		&envelope, request_bytes, sizeof(request_bytes) - 1,
		episode.coordinator_node_id, cluster_node_id,
		&reserve, &ballot, &prepare, &observed));
	UT_ASSERT_EQ(memcmp(&observed, &sentinel, sizeof(sentinel)), 0);

	ballot.proposer_node_id = cluster_node_id;
	UT_ASSERT(!cluster_reconfig_replacement_purge_request_authorized(
		&request, episode.coordinator_node_id, cluster_node_id,
		&reserve, &ballot, &prepare));
	ballot.proposer_node_id = episode.coordinator_node_id;
	prepare.phase = CLUSTER_JCMK_REPLACEMENT_PHASE_COMMITTED_CLOSED;
	UT_ASSERT(!cluster_reconfig_replacement_purge_request_authorized(
		&request, episode.coordinator_node_id, cluster_node_id,
		&reserve, &ballot, &prepare));
	prepare.phase = CLUSTER_JCMK_REPLACEMENT_PHASE_PREPARE;
	request.body.bitmap[0] ^= UINT8_C(0x02);
	UT_ASSERT(!cluster_reconfig_replacement_purge_request_authorized(
		&request, episode.coordinator_node_id, cluster_node_id,
		&reserve, &ballot, &prepare));
	request.body.bitmap[0] ^= UINT8_C(0x02);
	ut_candidate2_capable[episode.target_node_id] = false;
	UT_ASSERT(!cluster_reconfig_replacement_purge_request_authorized(
		&request, episode.coordinator_node_id, cluster_node_id,
		&reserve, &ballot, &prepare));
	UT_ASSERT_EQ(memcmp(&state->replacement_episode, &before,
					  sizeof(before)), 0);

	/* Phase 2 derives the ACK node only from the authenticated endpoint. The
	 * wire bitmap is identity, never a self-asserted ACK selector. A duplicate
	 * ACK remains an idempotent candidate and this predicate mutates nothing. */
	ut_candidate2_capable[episode.target_node_id] = true;
	state->replacement_episode.phase = CLUSTER_REPLACEMENT_EPISODE_PURGING;
	cluster_node_id = episode.coordinator_node_id;
	request.phase = CLUSTER_REPLACEMENT_WIRE_PHASE_PURGE_ACK;
	before = state->replacement_episode;
	{
		int32 ack_node = -1;

		UT_ASSERT(cluster_reconfig_replacement_purge_ack_authorized(
			&request, 2, cluster_node_id, &reserve, &ballot, &prepare,
			&ack_node));
		UT_ASSERT_EQ(ack_node, 2);
		UT_ASSERT_EQ(memcmp(&state->replacement_episode, &before,
						  sizeof(before)), 0);

		UT_ASSERT(cluster_replacement_wire_encode(&request, request_bytes));
		memset(&envelope, 0, sizeof(envelope));
		envelope.msg_type = PGRAC_IC_MSG_GES_REQUEST;
		envelope.source_node_id = UINT32_C(2);
		envelope.dest_node_id = (uint32)cluster_node_id;
		envelope.epoch = episode.baseline_epoch;
		envelope.payload_length = CLUSTER_REPLACEMENT_WIRE_BYTES;
		ack_node = -1;
		UT_ASSERT(
			cluster_reconfig_replacement_purge_ack_ingress_authorized(
				&envelope, request_bytes, sizeof(request_bytes), 2,
				cluster_node_id, &reserve, &ballot, &prepare, &ack_node));
		UT_ASSERT_EQ(ack_node, 2);
		UT_ASSERT_EQ(memcmp(&state->replacement_episode, &before,
						  sizeof(before)), 0);
		ack_node = -1;
		envelope.dest_node_id = UINT32_C(2);
		UT_ASSERT(
			!cluster_reconfig_replacement_purge_ack_ingress_authorized(
				&envelope, request_bytes, sizeof(request_bytes), 2,
				cluster_node_id, &reserve, &ballot, &prepare, &ack_node));
		UT_ASSERT_EQ(ack_node, -1);
		envelope.dest_node_id = (uint32)cluster_node_id;
		UT_ASSERT(
			!cluster_reconfig_replacement_purge_ack_ingress_authorized(
				&envelope, request_bytes, sizeof(request_bytes) - 1, 2,
				cluster_node_id, &reserve, &ballot, &prepare, &ack_node));
		UT_ASSERT_EQ(ack_node, -1);

		state->replacement_episode.acknowledgements[0] |= UINT8_C(0x04);
		before = state->replacement_episode;
		ack_node = -1;
		UT_ASSERT(cluster_reconfig_replacement_purge_ack_authorized(
			&request, 2, cluster_node_id, &reserve, &ballot, &prepare,
			&ack_node));
		UT_ASSERT_EQ(ack_node, 2);
		UT_ASSERT_EQ(memcmp(&state->replacement_episode, &before,
						  sizeof(before)), 0);

		ack_node = -1;
		ballot.proposer_node_id = 2;
		UT_ASSERT(!cluster_reconfig_replacement_purge_ack_authorized(
			&request, 2, cluster_node_id, &reserve, &ballot, &prepare,
			&ack_node));
		UT_ASSERT_EQ(ack_node, -1);
		ballot.proposer_node_id = episode.coordinator_node_id;
		UT_ASSERT(!cluster_reconfig_replacement_purge_ack_authorized(
			&request, episode.target_node_id, cluster_node_id,
			&reserve, &ballot, &prepare, &ack_node));
		UT_ASSERT_EQ(ack_node, -1);
		UT_ASSERT_EQ(memcmp(&state->replacement_episode, &before,
						  sizeof(before)), 0);
	}
}

/* U6 — declared-peer filter: an un-declared CSSD-ALIVE peer is never a join edge. */
UT_TEST(test_join_bitmap_declared_peer_filter)
{
	uint8 jb[CLUSTER_RECONFIG_DEAD_BITMAP_BYTES];
	int n;

	ut_join_setup();
	/* Never-seen runtime admission is past INITIAL; initial founders use
	 * the exact bootstrap path tested separately below. */
	UT_ASSERT(cluster_epoch_observe_remote(UINT64_C(7)));
	/* node 3 declared, ALIVE, fresh slot, membership ABSENT -> join edge */
	ut_declared_set[3] = true;
	ut_peer_state[3] = CLUSTER_CSSD_PEER_ALIVE;
	cluster_reconfig_record_observed_slot(3, 10, 1, 0);
	/* node 5 NOT declared, ALIVE, fresh slot -> filtered out */
	ut_peer_state[5] = CLUSTER_CSSD_PEER_ALIVE;
	cluster_reconfig_record_observed_slot(5, 10, 1, 0);

	n = cluster_reconfig_compute_join_bitmap(jb);
	UT_ASSERT_EQ(n, 1);
	UT_ASSERT(jb_test(jb, 3));
	UT_ASSERT(!jb_test(jb, 5));
}

/* A new founding peer can be published by QVOTEC after the membership loop
 * has sampled it, but before the same tick computes runtime join edges.
 * INITIAL ABSENT remains owned by bootstrap on both sides of that window;
 * a transient missing bootstrap proof must not create JOIN_PENDING. */
UT_TEST(test_initial_absent_peer_is_not_a_runtime_join_edge)
{
	uint8 jb[CLUSTER_RECONFIG_DEAD_BITMAP_BYTES];
	uint64 inc = 0;
	int i;

	ut_join_setup();
	ut_in_quorum_value = true;
	for (i = 0; i < 4; i++) {
		ut_declared_set[i] = true;
		ut_peer_state[i] = CLUSTER_CSSD_PEER_ALIVE;
		if (i == 3)
			continue;
		cluster_reconfig_record_observed_slot(i, 100 + i, 1, 0);
		cluster_reconfig_record_observed_fresh_alive(i, true);
		cluster_membership_record_admitted(i, 100 + i);
		cluster_membership_set_state(i, CLUSTER_MEMBER_MEMBER);
	}
	UT_ASSERT_EQ(cluster_reconfig_compute_join_bitmap(jb), 0);
	cluster_reconfig_bootstrap_publish_begin();
	cluster_reconfig_record_observed_slot(3, 103, 1, 0);
	UT_ASSERT(!cluster_reconfig_bootstrap_proof_node(3, &inc));
	UT_ASSERT_EQ(cluster_reconfig_compute_join_bitmap(jb), 0);
	UT_ASSERT(!jb_test(jb, 3));
	cluster_reconfig_record_observed_fresh_alive(3, true);
	cluster_reconfig_bootstrap_publish_in_quorum(true);
	cluster_reconfig_bootstrap_publish_end();
	UT_ASSERT(cluster_reconfig_bootstrap_proof_node(3, &inc));
	UT_ASSERT_EQ(inc, UINT64_C(103));
	UT_ASSERT_EQ(cluster_reconfig_compute_join_bitmap(jb), 0);
	UT_ASSERT(!jb_test(jb, 3));
	UT_ASSERT_EQ((int)cluster_membership_get_state(3),
				 (int)CLUSTER_MEMBER_ABSENT);
	UT_ASSERT_EQ(cluster_membership_get_last_admitted_incarnation(3), 0);

	/* A real past-INITIAL cluster still owns ordinary never-seen admission. */
	UT_ASSERT(cluster_epoch_observe_remote(UINT64_C(7)));
	UT_ASSERT_EQ(cluster_reconfig_compute_join_bitmap(jb), 1);
	UT_ASSERT(jb_test(jb, 3));
}

/* U7 — DEAD->ALIVE edge is a join; a steady-state MEMBER is not. */
UT_TEST(test_join_bitmap_dead_edge_not_member)
{
	uint8 jb[CLUSTER_RECONFIG_DEAD_BITMAP_BYTES];
	int n;

	ut_join_setup();
	/* node 1: MEMBER + ALIVE -> already a member, NOT a join edge */
	ut_declared_set[1] = true;
	ut_peer_state[1] = CLUSTER_CSSD_PEER_ALIVE;
	cluster_membership_set_state(1, CLUSTER_MEMBER_MEMBER);
	cluster_reconfig_record_observed_slot(1, 10, 1, 0);
	/* node 2: DEAD + ALIVE + fresh -> join edge */
	ut_declared_set[2] = true;
	ut_peer_state[2] = CLUSTER_CSSD_PEER_ALIVE;
	cluster_membership_set_state(2, CLUSTER_MEMBER_DEAD);
	cluster_reconfig_record_observed_slot(2, 10, 1, 0);

	n = cluster_reconfig_compute_join_bitmap(jb);
	UT_ASSERT_EQ(n, 1);
	UT_ASSERT(!jb_test(jb, 1));
	UT_ASSERT(jb_test(jb, 2));
}

/* U8 — multiple simultaneous joiners. */
UT_TEST(test_join_bitmap_multi_joiner)
{
	uint8 jb[CLUSTER_RECONFIG_DEAD_BITMAP_BYTES];
	int n;

	ut_join_setup();
	ut_declared_set[1] = true;
	ut_peer_state[1] = CLUSTER_CSSD_PEER_ALIVE;
	cluster_membership_set_state(1, CLUSTER_MEMBER_DEAD);
	cluster_reconfig_record_observed_slot(1, 10, 1, 0);
	ut_declared_set[2] = true;
	ut_peer_state[2] = CLUSTER_CSSD_PEER_ALIVE;
	cluster_membership_set_state(2, CLUSTER_MEMBER_DEAD);
	cluster_reconfig_record_observed_slot(2, 10, 1, 0);

	n = cluster_reconfig_compute_join_bitmap(jb);
	UT_ASSERT_EQ(n, 2);
	UT_ASSERT(jb_test(jb, 1));
	UT_ASSERT(jb_test(jb, 2));
}

/* U9 — a stale incarnation (<= floor) and a not-ready (generation 0) peer are
 * both excluded; only the fresh+ready DEAD->ALIVE peer is a join edge. */
UT_TEST(test_join_bitmap_stale_and_notready_excluded)
{
	uint8 jb[CLUSTER_RECONFIG_DEAD_BITMAP_BYTES];
	int n;

	ut_join_setup();
	/* node 1: stale — observed incarnation 10 <= admitted floor 10 */
	ut_declared_set[1] = true;
	ut_peer_state[1] = CLUSTER_CSSD_PEER_ALIVE;
	cluster_membership_set_state(1, CLUSTER_MEMBER_DEAD);
	cluster_membership_record_admitted(1, 10);
	cluster_reconfig_record_observed_slot(1, 10, 1, 0);
	/* node 2: not ready — observed generation 0 (no valid published slot) */
	ut_declared_set[2] = true;
	ut_peer_state[2] = CLUSTER_CSSD_PEER_ALIVE;
	cluster_membership_set_state(2, CLUSTER_MEMBER_DEAD);
	cluster_reconfig_record_observed_slot(2, 99, 0, 0);
	/* node 3: fresh (11 > floor 10) + ready -> join edge */
	ut_declared_set[3] = true;
	ut_peer_state[3] = CLUSTER_CSSD_PEER_ALIVE;
	cluster_membership_set_state(3, CLUSTER_MEMBER_DEAD);
	cluster_membership_record_admitted(3, 10);
	cluster_reconfig_record_observed_slot(3, 11, 1, 0);

	n = cluster_reconfig_compute_join_bitmap(jb);
	UT_ASSERT_EQ(n, 1);
	UT_ASSERT(!jb_test(jb, 1));
	UT_ASSERT(!jb_test(jb, 2));
	UT_ASSERT(jb_test(jb, 3));
}

/* P04 approved fast-rejoin slice: a shared-CF founding peer must acquire an
 * exact incarnation floor before MEMBER.  A later fresh slot for the same live
 * node is therefore an incarnation rollover, not a second bootstrap member;
 * evict the prior incarnation through the ordinary FAIL_STOP path first. */
UT_TEST(test_shared_cf_fast_rejoin_evicts_prior_live_incarnation)
{
	ClusterReconfigState *state;
	ReconfigEvent event;

	cluster_online_join = false;
	ut_join_setup();
	cluster_controlfile_shared_authority = true;
	epoch_init_done = false;
	cluster_epoch_shmem_init();
	state = (ClusterReconfigState *)reconfig_shmem_storage;
	state->self_join_admitted = 1;
	ut_in_quorum_value = true;
	ut_declared_set[1] = true;
	ut_peer_state[1] = CLUSTER_CSSD_PEER_ALIVE;
	/*
	 * RF-ROOT P9 verification / cold-formation ruling: the ABSENT founding
	 * admission is the WHOLE-round QVOTEC bootstrap proof (fresh co-boot
	 * slot at INITIAL + same-round in-quorum snapshot + exact non-zero
	 * incarnation).  Provisioning may leave a valid but clean/stale prior
	 * slot before the final two-node co-boot: it is not a MEMBER
	 * incarnation floor and — being stale — it is not identity authority
	 * either, so the peer stays ABSENT (fail-closed) until a fresh slot
	 * proves the co-boot.
	 */
	cluster_reconfig_record_observed_slot(1, UINT64_C(69), UINT64_C(1), 0);
	cluster_reconfig_record_observed_fresh_alive(1, false);
	cluster_reconfig_bootstrap_publish_in_quorum(true);

	cluster_reconfig_lmon_tick();
	UT_ASSERT_EQ((int)cluster_membership_get_state(1),
				 (int)CLUSTER_MEMBER_ABSENT);
	UT_ASSERT_EQ(cluster_membership_get_last_admitted_incarnation(1),
				 UINT64_C(0));
	UT_ASSERT_EQ(state->fast_rejoin_incarnation[1], UINT64_C(0));
	UT_ASSERT_EQ(cluster_reconfig_get_apply_counter(), UINT64_C(0));

	/* The first fresh-alive slot is the founding peer identity: the ABSENT
	 * branch proves it through the bootstrap window and record_admitted's
	 * the EXACT observed incarnation as the D13 floor before MEMBER. */
	cluster_reconfig_record_observed_slot(1, UINT64_C(70), UINT64_C(2), 0);
	cluster_reconfig_record_observed_fresh_alive(1, true);
	cluster_reconfig_lmon_tick();
	UT_ASSERT_EQ((int)cluster_membership_get_state(1),
				 (int)CLUSTER_MEMBER_MEMBER);
	UT_ASSERT_EQ(cluster_membership_get_last_admitted_incarnation(1),
				 UINT64_C(70));
	UT_ASSERT_EQ(state->fast_rejoin_incarnation[1], UINT64_C(0));
	UT_ASSERT_EQ(cluster_reconfig_get_apply_counter(), UINT64_C(0));

	/* A steady-state tick latches the volatile rollover baseline: the
	 * already-formed peer's fresh slot establishes fast_rejoin_incarnation
	 * (a zero floor is bootstrap, never rollover evidence). */
	cluster_reconfig_lmon_tick();
	UT_ASSERT_EQ(state->fast_rejoin_incarnation[1],
				 UINT64_C(70));
	UT_ASSERT_EQ(cluster_membership_get_last_admitted_incarnation(1),
				 UINT64_C(70));
	UT_ASSERT_EQ(cluster_reconfig_get_apply_counter(), UINT64_C(0));

	/* A later fresh slot for the same live node is an incarnation rollover,
	 * not a second bootstrap member; evict the prior incarnation through
	 * the ordinary FAIL_STOP path first. */
	cluster_reconfig_record_observed_slot(1, UINT64_C(77), UINT64_C(3), 0);
	cluster_reconfig_lmon_tick();
	cluster_reconfig_get_last_event(&event);
	UT_ASSERT_EQ((int)cluster_membership_get_state(1),
				 (int)CLUSTER_MEMBER_DEAD);
	UT_ASSERT_EQ((int)event.reconfig_kind, (int)RECONFIG_KIND_FAIL_STOP);
	UT_ASSERT((event.dead_bitmap[0] & UINT8_C(0x02)) != 0);
	UT_ASSERT_EQ(event.new_epoch, UINT64_C(1));
	cluster_controlfile_shared_authority = false;
}

static ClusterReconfigState *
ut_fast_rejoin_to_join_pending(void)
{
	ClusterReconfigState *state;
	ClusterJoinCommitMarker marker;
	ReconfigEvent event;
	uint8 slot[CLUSTER_VOTING_SLOT_BYTES];
	int32 target = -1;

	cluster_online_join = false;
	ut_join_setup();
	MyBackendType = B_LMON;
	epoch_init_done = false;
	cluster_epoch_shmem_init();
	state = (ClusterReconfigState *)reconfig_shmem_storage;
	state->self_join_admitted = 1;
	ut_in_quorum_value = true;
	ut_authority_managed = true;
	ut_serving_ready = true;
	cluster_controlfile_shared_authority = true;
	ut_declared_set[1] = true;
	ut_peer_state[1] = CLUSTER_CSSD_PEER_ALIVE;
	cluster_membership_set_state(0, CLUSTER_MEMBER_MEMBER);
	cluster_membership_set_state(1, CLUSTER_MEMBER_MEMBER);

	/* Establish the fresh-alive founding incarnation, then roll it over. */
	cluster_reconfig_record_observed_slot(1, UINT64_C(70), UINT64_C(1), 0);
	cluster_reconfig_record_observed_fresh_alive(1, true);
	cluster_reconfig_lmon_tick();
	cluster_reconfig_record_observed_slot(1, UINT64_C(77), UINT64_C(2), 0);
	cluster_reconfig_lmon_tick();
	cluster_reconfig_get_last_event(&event);
	UT_ASSERT_EQ((int)event.reconfig_kind, (int)RECONFIG_KIND_FAIL_STOP);
	UT_ASSERT((event.dead_bitmap[0] & UINT8_C(0x02)) != 0);

	/* Current serving authority starts Phase 1 through the ordinary driver. */
	cluster_reconfig_lmon_tick();
	memset(slot, 0, sizeof(slot));
	UT_ASSERT(ut_join_qvotec_poll_write_pending(&target, slot));
	UT_ASSERT_EQ(target, 1);
	memcpy(&marker, slot, sizeof(marker));
	UT_ASSERT_EQ((int)marker.phase, (int)CLUSTER_JCMK_PHASE_PREPARE);
	ut_join_qvotec_complete_write(true);
	cluster_reconfig_lmon_tick();
	cluster_reconfig_get_last_event(&event);
	UT_ASSERT_EQ((int)event.reconfig_kind, (int)RECONFIG_KIND_JOIN_PENDING);
	UT_ASSERT(jb_test(event.join_bitmap, 1));
	return state;
}

/* P04 A2 Scheme B: once the exact fast-rejoin FAIL_STOP has started the
 * existing singleton JOIN transaction, formation invalidation may close
 * SERVING_READY between JOIN_PENDING and JOIN_COMMITTED.  The episode-bound
 * control capability must carry only that already-authorized target through
 * Phase 2; a newly eligible peer must not be smuggled into the transaction. */
UT_TEST(test_fast_rejoin_control_episode_carries_only_bound_join_to_terminal)
{
	ClusterReconfigState *state;
	ClusterJoinCommitMarker marker;
	ReconfigEvent event;
	uint8 slot[CLUSTER_VOTING_SLOT_BYTES];
	int32 target = -1;

	state = ut_fast_rejoin_to_join_pending();

	/* JOIN_PENDING made the managed serving formation stale.  Also introduce an
	 * unrelated eligible peer: only the bound node 1 may reach Phase 2. */
	ut_serving_ready = false;
	ut_declared_set[2] = true;
	ut_peer_state[2] = CLUSTER_CSSD_PEER_ALIVE;
	cluster_membership_set_state(2, CLUSTER_MEMBER_DEAD);
	cluster_reconfig_record_observed_slot(2, UINT64_C(88), UINT64_C(1), 0);
	cluster_reconfig_record_observed_fresh_alive(2, true);
	cluster_reconfig_lmon_tick();
	memset(slot, 0, sizeof(slot));
	target = -1;
	UT_ASSERT(ut_join_qvotec_poll_write_pending(&target, slot));
	UT_ASSERT_EQ(target, 1);
	memcpy(&marker, slot, sizeof(marker));
	UT_ASSERT_EQ((int)marker.phase, (int)CLUSTER_JCMK_PHASE_COMMITTED);
	UT_ASSERT_EQ((int)cluster_membership_get_state(2),
				 (int)CLUSTER_MEMBER_DEAD);

	/* Complete the bound terminal so no process-local stage leaks to later tests. */
	ut_join_qvotec_complete_write(true);
	cluster_reconfig_lmon_tick();
	cluster_reconfig_get_last_event(&event);
	UT_ASSERT_EQ((int)event.reconfig_kind, (int)RECONFIG_KIND_JOIN_COMMITTED);
	UT_ASSERT_EQ((int)cluster_membership_get_state(1),
				 (int)CLUSTER_MEMBER_MEMBER);
	UT_ASSERT(!jb_test(state->fast_rejoin_bitmap, 1));

	ut_authority_managed = false;
	ut_serving_ready = false;
	cluster_controlfile_shared_authority = false;
	MyBackendType = B_INVALID;
}

/* The capability is bound to the exact LMS generation.  A generation change
 * invalidates it PERMANENTLY: restoring the old scalar must not re-arm it,
 * and an unrelated newly eligible peer must not be smuggled into the episode.
 * (The COMMITTED marker, however, was staged while the generation still
 * matched and is a majority-durable decision; the ungated commit drain
 * (increments 5/16) only completes that already-authorized publication — its
 * re-vet re-reads the joiner's current observed slot, so nothing is committed
 * on stale evidence.) */
UT_TEST(test_fast_rejoin_control_episode_lms_generation_loss_fails_closed)
{
	ClusterJoinCommitMarker marker;
	ReconfigEvent event;
	uint8 slot[CLUSTER_VOTING_SLOT_BYTES];
	int32 target = -1;

	(void)ut_fast_rejoin_to_join_pending();
	ut_serving_ready = false;
	ut_lms_generation = UINT64_C(2);
	cluster_reconfig_lmon_tick();

	/* The generation change cleared the control capability (no new arming),
	 * but the already-staged COMMITTED marker is still pending: the staged
	 * publication completes once the marker reaches a majority (the joiner
	 * stays JOINING until then — nothing is published on stale evidence). */
	UT_ASSERT(ut_join_qvotec_poll_write_pending(&target, slot));
	UT_ASSERT_EQ(target, 1);
	memcpy(&marker, slot, sizeof(marker));
	UT_ASSERT_EQ((int)marker.phase, (int)CLUSTER_JCMK_PHASE_COMMITTED);
	UT_ASSERT_EQ(marker.admitted_incarnation, UINT64_C(77));
	UT_ASSERT_EQ((int)cluster_membership_get_state(1),
				 (int)CLUSTER_MEMBER_JOINING);

	/* Complete the already-authorized terminal so no stage leaks to later
	 * tests: the drain publishes JOIN_COMMITTED after the majority ACK. */
	ut_join_qvotec_complete_write(true);
	cluster_reconfig_lmon_tick();
	cluster_reconfig_get_last_event(&event);
	UT_ASSERT_EQ((int)event.reconfig_kind, (int)RECONFIG_KIND_JOIN_COMMITTED);
	UT_ASSERT_EQ((int)cluster_membership_get_state(1),
				 (int)CLUSTER_MEMBER_MEMBER);

	/* Fail-closed half: restoring the old generation must NOT resurrect the
	 * capability — an unrelated newly eligible peer stays out of the episode
	 * (no new JOIN_PENDING, node 2 remains DEAD). */
	ut_declared_set[2] = true;
	ut_peer_state[2] = CLUSTER_CSSD_PEER_ALIVE;
	cluster_membership_set_state(2, CLUSTER_MEMBER_DEAD);
	cluster_reconfig_record_observed_slot(2, UINT64_C(88), UINT64_C(1), 0);
	cluster_reconfig_record_observed_fresh_alive(2, true);
	ut_lms_generation = UINT64_C(1);
	cluster_reconfig_lmon_tick();
	cluster_reconfig_get_last_event(&event);
	UT_ASSERT_EQ((int)event.reconfig_kind, (int)RECONFIG_KIND_JOIN_COMMITTED);
	UT_ASSERT(!jb_test(event.join_bitmap, 2));
	UT_ASSERT_EQ((int)cluster_membership_get_state(2),
				 (int)CLUSTER_MEMBER_DEAD);

	ut_authority_managed = false;
	ut_serving_ready = false;
	cluster_controlfile_shared_authority = false;
	MyBackendType = B_INVALID;
}

/* Legacy shared-nothing authority=off remains legal: a live MEMBER's newer
 * observed slot is not an implicit admission request when the P04 shared-CF
 * authority scope is absent. */
UT_TEST(test_legacy_online_join_off_does_not_evict_live_member)
{
	uint64 apply_before;

	cluster_online_join = false;
	ut_join_setup();
	cluster_controlfile_shared_authority = false;
	ut_in_quorum_value = true;
	ut_declared_set[1] = true;
	ut_peer_state[1] = CLUSTER_CSSD_PEER_ALIVE;
	cluster_membership_set_state(1, CLUSTER_MEMBER_MEMBER);
	cluster_membership_record_admitted(1, UINT64_C(70));
	cluster_reconfig_record_observed_slot(1, UINT64_C(77), UINT64_C(2), 0);
	apply_before = cluster_reconfig_get_apply_counter();

	cluster_reconfig_lmon_tick();
	UT_ASSERT_EQ((int)cluster_membership_get_state(1),
				 (int)CLUSTER_MEMBER_MEMBER);
	UT_ASSERT_EQ(cluster_reconfig_get_apply_counter(), apply_before);
}

/* U12 (D3, INV-J11) — event_id_v2: the 4 real kinds are mutually distinct under
 * their actual non-collision bases; FAIL_STOP stays byte-compatible with the
 * legacy hash; CLEAN_LEAVE also uses legacy (5.13 marker binding, RC-1) and is
 * distinguished from FAIL_STOP by cssd_dead_generation; the two JOIN phases fold
 * the kind; NONE is the 0 sentinel. */
UT_TEST(test_event_id_v2_kind_distinctness)
{
	uint8 dead[CLUSTER_RECONFIG_DEAD_BITMAP_BYTES];
	uint8 join[CLUSTER_RECONFIG_DEAD_BITMAP_BYTES];
	uint64 incs[CLUSTER_MAX_NODES];
	uint64 incs2[CLUSTER_MAX_NODES];
	uint64 gen = 7;
	uint64 legacy, id_fs, id_cl, id_jp, id_jc;

	memset(dead, 0, sizeof(dead));
	memset(join, 0, sizeof(join));
	memset(incs, 0, sizeof(incs));
	dead[0] = 0x02;	 /* node 1 in the leave set */
	join[0] = 0x04;	 /* node 2 in the join set */
	incs[2] = 12345; /* joiner 2 incarnation */

	/* NONE is the never-applied sentinel. */
	UT_ASSERT_EQ(cluster_reconfig_compute_event_id_v2(RECONFIG_KIND_NONE, dead, join, incs, gen),
				 0);

	/* FAIL_STOP == the legacy hash over (dead, gen). */
	legacy = cluster_reconfig_compute_event_id(dead, gen);
	id_fs = cluster_reconfig_compute_event_id_v2(RECONFIG_KIND_FAIL_STOP, dead, join, incs, gen);
	UT_ASSERT_EQ(id_fs, legacy);

	/* CLEAN_LEAVE also uses legacy (RC-1): equal to FAIL_STOP for identical
	 * (dead, gen); the real-world distinction is cssd_dead_generation. */
	id_cl = cluster_reconfig_compute_event_id_v2(RECONFIG_KIND_CLEAN_LEAVE, dead, join, incs, gen);
	UT_ASSERT_EQ(id_cl, legacy);
	UT_ASSERT(
		cluster_reconfig_compute_event_id_v2(RECONFIG_KIND_CLEAN_LEAVE, dead, join, incs, gen + 1)
		!= id_fs);

	/* JOIN_PENDING != JOIN_COMMITTED (kind folded); both != the leave ids, != 0. */
	id_jp = cluster_reconfig_compute_event_id_v2(RECONFIG_KIND_JOIN_PENDING, dead, join, incs, gen);
	id_jc
		= cluster_reconfig_compute_event_id_v2(RECONFIG_KIND_JOIN_COMMITTED, dead, join, incs, gen);
	UT_ASSERT(id_jp != id_jc);
	UT_ASSERT(id_jp != id_fs);
	UT_ASSERT(id_jc != id_fs);
	UT_ASSERT(id_jp != 0);
	UT_ASSERT(id_jc != 0);

	/* distinct joiner incarnations -> distinct join ids. */
	memcpy(incs2, incs, sizeof(incs2));
	incs2[2] = 99999;
	UT_ASSERT(
		cluster_reconfig_compute_event_id_v2(RECONFIG_KIND_JOIN_PENDING, dead, join, incs2, gen)
		!= id_jp);
}

/* U14 (D4, INV-J10) — clearing clean_departed (what commit_member does at
 * JOIN_COMMITTED) re-enables a node's later fail-stop: a clean-left node that
 * rejoins must have clean_departed[N] cleared so effective_dead = cssd_dead &
 * ~clean_departed once again includes N's later real CSSD DEAD.  The full
 * marker-durable commit path is covered e2e by TAP L6; here we exercise the
 * clear primitive the commit uses + that it is JOIN_COMMITTED-only (Phase-1 /
 * apply does NOT clear). */
UT_TEST(test_clean_departed_clear_for_rejoin)
{
	ut_join_setup();
	ut_declared_set[2] = true;

	/* node 2 cleanly departed at epoch 5 -> masked from effective_dead */
	cluster_reconfig_record_clean_departed(2, 5, false);
	UT_ASSERT(cluster_reconfig_is_clean_departed(2));
	UT_ASSERT_EQ(cluster_reconfig_get_clean_departed_epoch(2), 5);

	/* Phase-1 (apply_join) sets JOINING + pending but does NOT clear (only a real
	 * MEMBER commit may) — assert the suppression still holds at JOINING. */
	cluster_membership_set_state(2, CLUSTER_MEMBER_JOINING);
	UT_ASSERT(cluster_reconfig_is_clean_departed(2));

	/* JOIN_COMMITTED clears it (commit_member calls clear_clean_departed). */
	cluster_reconfig_clear_clean_departed(2);
	UT_ASSERT(!cluster_reconfig_is_clean_departed(2));
	UT_ASSERT_EQ(cluster_reconfig_get_clean_departed_epoch(2), 0);
}

/* D5 (INV-J9) — joiner write-gate lifecycle: a fresh node defaults ALLOW; when
 * it detects a running cluster (a peer at epoch > 0) the tick closes the gate
 * (53R60, retry-safe); note_self_admitted reopens it (ALLOW). */
UT_TEST(test_cold_bootstrap_records_exact_floor_before_opening_gate)
{
	pid_t pid;
	int status = 0;

	/* joiner_gate_decided is intentionally process-local and one-shot.  Exercise
	 * the cold edge in a child so this witness cannot perturb the parent binary's
	 * later rejoin lifecycle cases. */
	fflush(NULL);
	pid = fork();
	UT_ASSERT(pid >= 0);
	if (pid < 0)
		return;
	if (pid == 0) {
		ClusterReconfigState *state;

		ut_current_failed = 0;
		cluster_online_join = true;
		ut_join_setup();
		ut_in_quorum_value = true;
		state = (ClusterReconfigState *)reconfig_shmem_storage;

		/* A zero incarnation cannot latch the bootstrap decision or publish
		 * MEMBER.  The same path retries once QVOTEC exposes this formation. */
		ut_set_self_incarnation_sequence(0, 0, 0);
		cluster_reconfig_lmon_tick();
		UT_ASSERT_EQ(state->self_join_admitted, 0);
		UT_ASSERT_EQ((int)cluster_membership_get_state(cluster_node_id),
					 (int)CLUSTER_MEMBER_JOINING);
		UT_ASSERT_EQ(
			cluster_membership_get_last_admitted_incarnation(cluster_node_id),
			UINT64_C(0));

		ut_set_self_incarnation_sequence(UINT64_C(61), UINT64_C(61),
									 UINT64_C(61));
		cluster_reconfig_lmon_tick();
		UT_ASSERT_EQ(
			cluster_membership_get_last_admitted_incarnation(cluster_node_id),
			UINT64_C(61));
		UT_ASSERT_EQ((int)cluster_membership_get_state(cluster_node_id),
					 (int)CLUSTER_MEMBER_MEMBER);
		UT_ASSERT_EQ(state->self_join_admitted, 1);
		UT_ASSERT(ut_self_member_callback_seen);
		UT_ASSERT_EQ((int)ut_self_member_callback_gate,
					 (int)CLUSTER_JOIN_GATE_BLOCK_53R60);

		cluster_reconfig_lmon_tick();
		UT_ASSERT_EQ(
			cluster_membership_get_last_admitted_incarnation(cluster_node_id),
			UINT64_C(61));
		UT_ASSERT_EQ(state->self_join_admitted, 1);
		fflush(stdout);
		_exit(ut_current_failed == 0 ? 0 : 1);
	}

	UT_ASSERT_EQ(waitpid(pid, &status, 0), pid);
	UT_ASSERT(WIFEXITED(status));
	UT_ASSERT_EQ(WEXITSTATUS(status), 0);
}

/* AD-023 recovery-control/serving split: a pre-StartupXLOG stripe HOLD must
 * not leave phase 3 without an exact MEMBER formation.  The floor is current,
 * but the ordinary write byte stays closed until the unchanged stripe gate
 * later returns PROCEED. */
UT_TEST(test_initial_bootstrap_stripe_gate_publishes_control_member_only)
{
	pid_t pid;
	int status = 0;

	fflush(NULL);
	pid = fork();
	UT_ASSERT(pid >= 0);
	if (pid < 0)
		return;
	if (pid == 0) {
		ClusterReconfigState *state;

		ut_current_failed = 0;
		cluster_online_join = true;
		ut_join_setup();
		ut_in_quorum_value = true;
		ut_set_self_incarnation_sequence(UINT64_C(61), UINT64_C(61),
									 UINT64_C(61));
		state = (ClusterReconfigState *)reconfig_shmem_storage;

		ut_xid_stripe_verdict = CLUSTER_XID_STRIPE_JOIN_HOLD;
		cluster_reconfig_lmon_tick();
		UT_ASSERT_EQ((int)cluster_membership_get_state(cluster_node_id),
					 (int)CLUSTER_MEMBER_MEMBER);
		UT_ASSERT_EQ(
			cluster_membership_get_last_admitted_incarnation(cluster_node_id),
			UINT64_C(61));
		UT_ASSERT_EQ(state->self_join_admitted, 0);

		/* HOLD remains idempotent and cannot latch the one-shot decision. */
		cluster_reconfig_lmon_tick();
		UT_ASSERT_EQ((int)cluster_membership_get_state(cluster_node_id),
					 (int)CLUSTER_MEMBER_MEMBER);
		UT_ASSERT_EQ(state->self_join_admitted, 0);

		ut_xid_stripe_verdict = CLUSTER_XID_STRIPE_JOIN_PROCEED;
		cluster_reconfig_lmon_tick();
		UT_ASSERT_EQ((int)cluster_membership_get_state(cluster_node_id),
					 (int)CLUSTER_MEMBER_MEMBER);
		UT_ASSERT_EQ(
			cluster_membership_get_last_admitted_incarnation(cluster_node_id),
			UINT64_C(61));
		UT_ASSERT_EQ(state->self_join_admitted, 1);
		fflush(stdout);
		_exit(ut_current_failed == 0 ? 0 : 1);
	}

	UT_ASSERT_EQ(waitpid(pid, &status, 0), pid);
	UT_ASSERT(WIFEXITED(status));
	UT_ASSERT_EQ(WEXITSTATUS(status), 0);
}

/* Oracle-first startup classification: once any current, fresh peer has
 * formed past INITIAL, a slower database instance is an ordinary online
 * joiner.  An earlier xid-stripe HOLD at INITIAL is not retained as hidden
 * bootstrap authority. */
UT_TEST(test_late_initial_node_becomes_online_joiner)
{
	pid_t pid;
	int status = 0;

	fflush(NULL);
	pid = fork();
	UT_ASSERT(pid >= 0);
	if (pid < 0)
		return;
	if (pid == 0) {
		ClusterReconfigState *state;
		int i;

		ut_current_failed = 0;
		cluster_online_join = true;
		ut_join_setup();
		cluster_node_id = 1;
		ut_in_quorum_value = true;
		ut_set_self_incarnation_sequence(UINT64_C(61), UINT64_C(61),
									 UINT64_C(61));
		state = (ClusterReconfigState *)reconfig_shmem_storage;
		for (i = 0; i < 4; i++) {
			ut_declared_set[i] = true;
			if (i == cluster_node_id)
				continue;
			cluster_reconfig_record_observed_slot(
				i, UINT64_C(100) + (uint64)i, UINT64_C(1),
				CLUSTER_EPOCH_INITIAL);
			cluster_reconfig_record_observed_fresh_alive(i, true);
		}

		ut_xid_stripe_verdict = CLUSTER_XID_STRIPE_JOIN_HOLD;
		cluster_reconfig_lmon_tick();
		UT_ASSERT_EQ(state->self_join_admitted, 0);
		UT_ASSERT_EQ(state->self_join_deadline_us, 0);

		/* Same peer incarnation, but current durable evidence now says the
		 * cluster is formed.  The slow node may cease to be in the current
		 * local quorum view at exactly this edge because the peers have already
		 * advanced their formation.  Their fresh QVOTEC-authenticated slot is
		 * still sufficient to close this node's gate and classify it as an
		 * ordinary JCMK joiner; it is never authority to admit the node. */
		cluster_reconfig_record_observed_slot(
			0, UINT64_C(100), UINT64_C(2), UINT64_C(7));
		cluster_reconfig_record_observed_fresh_alive(0, true);
		ut_in_quorum_value = false;
		cluster_reconfig_lmon_tick();
		UT_ASSERT_EQ((int)cluster_membership_get_state(cluster_node_id),
					 (int)CLUSTER_MEMBER_JOINING);
		UT_ASSERT_EQ(state->self_join_admitted, 0);
		UT_ASSERT_EQ(state->self_join_deadline_us, 0);
		UT_ASSERT_EQ(ut_xid_stripe_join_progress_calls, 0);
		UT_ASSERT_EQ((int)cluster_reconfig_self_join_gate_verdict(),
					 (int)CLUSTER_JOIN_GATE_BLOCK_53R60);
		fflush(stdout);
		_exit(ut_current_failed == 0 ? 0 : 1);
	}

	UT_ASSERT_EQ(waitpid(pid, &status, 0), pid);
	UT_ASSERT(WIFEXITED(status));
	UT_ASSERT_EQ(WEXITSTATUS(status), 0);
}

/* Transport epoch catch-up is permitted only from the same current, fresh
 * QVOTEC slot evidence that can classify a running peer.  A durable slot whose
 * heartbeat is no longer fresh is residue, not authority to move this node out
 * of the founding epoch. */
UT_TEST(test_late_joiner_does_not_adopt_stale_peer_epoch)
{
	pid_t pid;
	int status = 0;

	fflush(NULL);
	pid = fork();
	UT_ASSERT(pid >= 0);
	if (pid < 0)
		return;
	if (pid == 0) {
		ClusterReconfigState *state;

		ut_current_failed = 0;
		cluster_online_join = true;
		ut_join_setup();
		cluster_node_id = 1;
		ut_declared_set[0] = true;
		ut_declared_set[1] = true;
		ut_in_quorum_value = false;
		state = (ClusterReconfigState *)reconfig_shmem_storage;
		cluster_reconfig_record_observed_slot(
			0, UINT64_C(100), UINT64_C(2), UINT64_C(7));
		cluster_reconfig_record_observed_fresh_alive(0, false);

		cluster_reconfig_lmon_tick();
		UT_ASSERT_EQ(cluster_epoch_get_current(), CLUSTER_EPOCH_INITIAL);
		UT_ASSERT_EQ((int)cluster_membership_get_state(cluster_node_id),
					 (int)CLUSTER_MEMBER_ABSENT);
		UT_ASSERT_EQ(state->self_join_admitted, 0);
		UT_ASSERT_EQ(state->self_join_deadline_us, 0);
		UT_ASSERT_EQ(ut_xid_stripe_join_progress_calls, 0);
		fflush(stdout);
		_exit(ut_current_failed == 0 ? 0 : 1);
	}

	UT_ASSERT_EQ(waitpid(pid, &status, 0), pid);
	UT_ASSERT(WIFEXITED(status));
	UT_ASSERT_EQ(WEXITSTATUS(status), 0);
}

/* A qvotec slot update is not current until its whole publish window closes.
 * In particular, a new epoch written between begin/end must not be combined
 * with the previous round's fresh bit or generation and adopted early. */
UT_TEST(test_late_joiner_epoch_adoption_waits_for_exact_publish_window)
{
	pid_t pid;
	int status = 0;

	fflush(NULL);
	pid = fork();
	UT_ASSERT(pid >= 0);
	if (pid < 0)
		return;
	if (pid == 0) {
		ClusterReconfigState *state;

		ut_current_failed = 0;
		cluster_online_join = true;
		ut_join_setup();
		cluster_node_id = 1;
		ut_declared_set[0] = true;
		ut_declared_set[1] = true;
		ut_in_quorum_value = false;
		state = (ClusterReconfigState *)reconfig_shmem_storage;

		cluster_reconfig_bootstrap_publish_begin();
		cluster_reconfig_record_observed_slot(
			0, UINT64_C(101), UINT64_C(2), UINT64_C(7));
		cluster_reconfig_record_observed_fresh_alive(0, true);
		cluster_reconfig_lmon_tick();
		UT_ASSERT_EQ(cluster_epoch_get_current(), CLUSTER_EPOCH_INITIAL);
		UT_ASSERT_EQ((int)cluster_membership_get_state(cluster_node_id),
					 (int)CLUSTER_MEMBER_ABSENT);
		UT_ASSERT_EQ(state->self_join_deadline_us, 0);

		cluster_reconfig_bootstrap_publish_end();
		cluster_reconfig_lmon_tick();
		UT_ASSERT_EQ(cluster_epoch_get_current(), UINT64_C(7));
		UT_ASSERT_EQ((int)cluster_membership_get_state(cluster_node_id),
					 (int)CLUSTER_MEMBER_JOINING);
		UT_ASSERT_EQ(state->self_join_admitted, 0);
		UT_ASSERT_EQ(state->self_join_deadline_us, 0);
		UT_ASSERT_EQ(ut_xid_stripe_join_progress_calls, 0);
		fflush(stdout);
		_exit(ut_current_failed == 0 ? 0 : 1);
	}

	UT_ASSERT_EQ(waitpid(pid, &status, 0), pid);
	UT_ASSERT(WIFEXITED(status));
	UT_ASSERT_EQ(WEXITSTATUS(status), 0);
}

UT_TEST(test_late_joiner_deadline_starts_only_after_stripe_claim_owned)
{
	pid_t pid;
	int status = 0;

	fflush(NULL);
	pid = fork();
	UT_ASSERT(pid >= 0);
	if (pid < 0)
		return;
	if (pid == 0) {
		ClusterReconfigState *state;
		uint64 first_deadline;

		ut_current_failed = 0;
		cluster_online_join = true;
		ut_join_setup();
		cluster_node_id = 1;
		ut_declared_set[0] = true;
		ut_declared_set[1] = true;
		ut_set_self_incarnation_sequence(UINT64_C(71), UINT64_C(71),
									 UINT64_C(71));
		state = (ClusterReconfigState *)reconfig_shmem_storage;
		cluster_reconfig_record_observed_slot(
			0, UINT64_C(100), UINT64_C(2), UINT64_C(7));
		cluster_reconfig_record_observed_fresh_alive(0, true);

		/* Classification is allowed without local quorum, but ownership and
		 * its absolute deadline are not. */
		ut_in_quorum_value = false;
		ut_xid_stripe_progress = STRIPE_JOIN_WAIT_EVIDENCE;
		cluster_reconfig_lmon_tick();
		UT_ASSERT_EQ((int)cluster_membership_get_state(cluster_node_id),
					 (int)CLUSTER_MEMBER_JOINING);
		UT_ASSERT_EQ(state->self_join_deadline_us, 0);
		UT_ASSERT_EQ(state->self_join_failed, 0);
		UT_ASSERT_EQ(ut_xid_stripe_join_progress_calls, 0);

		/* Quorum without driveable disk evidence still owns no deadline. */
		ut_in_quorum_value = true;
		cluster_reconfig_lmon_tick();
		UT_ASSERT_EQ(ut_xid_stripe_join_progress_calls, 1);
		UT_ASSERT(!ut_xid_stripe_progress_last_may_seed);
		UT_ASSERT_EQ(state->self_join_deadline_us, 0);
		UT_ASSERT_EQ(state->self_join_failed, 0);

		/* The exact staged/pending own-slot claim starts the deadline once. */
		ut_xid_stripe_progress = STRIPE_JOIN_CLAIM_OWNED;
		cluster_reconfig_lmon_tick();
		UT_ASSERT_EQ(ut_xid_stripe_join_progress_calls, 2);
		UT_ASSERT(state->self_join_deadline_us != 0);
		first_deadline = state->self_join_deadline_us;
		cluster_reconfig_lmon_tick();
		UT_ASSERT_EQ(ut_xid_stripe_join_progress_calls, 3);
		UT_ASSERT_EQ(state->self_join_deadline_us, first_deadline);
		UT_ASSERT_EQ(state->self_join_failed, 0);
		fflush(stdout);
		_exit(ut_current_failed == 0 ? 0 : 1);
	}

	UT_ASSERT_EQ(waitpid(pid, &status, 0), pid);
	UT_ASSERT(WIFEXITED(status));
	UT_ASSERT_EQ(WEXITSTATUS(status), 0);
}

UT_TEST(test_clean_leave_suppresses_late_joiner_classification_and_progress)
{
	pid_t pid;
	int status = 0;

	fflush(NULL);
	pid = fork();
	UT_ASSERT(pid >= 0);
	if (pid < 0)
		return;
	if (pid == 0) {
		ClusterReconfigState *state;
		uint64 first_deadline;

		ut_current_failed = 0;
		cluster_online_join = true;
		ut_join_setup();
		cluster_node_id = 1;
		ut_declared_set[0] = true;
		ut_declared_set[1] = true;
		ut_in_quorum_value = true;
		ut_set_self_incarnation_sequence(UINT64_C(73), UINT64_C(73),
									 UINT64_C(73));
		state = (ClusterReconfigState *)reconfig_shmem_storage;
		cluster_reconfig_record_observed_slot(
			0, UINT64_C(100), UINT64_C(2), UINT64_C(7));
		cluster_reconfig_record_observed_fresh_alive(0, true);
		ut_xid_stripe_progress = STRIPE_JOIN_CLAIM_OWNED;

		ut_clean_leave_in_progress = true;
		cluster_reconfig_lmon_tick();
		UT_ASSERT_EQ(ut_xid_stripe_join_progress_calls, 0);
		UT_ASSERT_EQ(state->self_join_deadline_us, 0);
		UT_ASSERT_EQ(state->self_join_failed, 0);

		ut_clean_leave_in_progress = false;
		cluster_reconfig_lmon_tick();
		UT_ASSERT_EQ(ut_xid_stripe_join_progress_calls, 1);
		UT_ASSERT(state->self_join_deadline_us != 0);
		first_deadline = state->self_join_deadline_us;
		UT_ASSERT_EQ(state->self_join_failed, 0);

		/* An already-classified joiner still must not drive a second
		 * prerequisite owner while clean leave is active.  The original
		 * absolute deadline remains armed and is never refreshed. */
		ut_clean_leave_in_progress = true;
		cluster_reconfig_lmon_tick();
		UT_ASSERT_EQ(ut_xid_stripe_join_progress_calls, 1);
		UT_ASSERT_EQ(state->self_join_deadline_us, first_deadline);
		UT_ASSERT_EQ(state->self_join_failed, 0);
		fflush(stdout);
		_exit(ut_current_failed == 0 ? 0 : 1);
	}

	UT_ASSERT_EQ(waitpid(pid, &status, 0), pid);
	UT_ASSERT(WIFEXITED(status));
	UT_ASSERT_EQ(WEXITSTATUS(status), 0);
}

UT_TEST(test_epoch0_late_founder_classifies_only_after_recovery)
{
	pid_t pid;
	int status = 0;

	fflush(NULL);
	pid = fork();
	UT_ASSERT(pid >= 0);
	if (pid < 0)
		return;
	if (pid == 0) {
		ClusterReconfigState *state;

		ut_current_failed = 0;
		cluster_online_join = true;
		ut_join_setup();
		cluster_node_id = 3;
		ut_declared_set[0] = true;
		ut_declared_set[3] = true;
		ut_in_quorum_value = true;
		ut_set_self_incarnation_sequence(UINT64_C(83), UINT64_C(83),
									 UINT64_C(83));
		state = (ClusterReconfigState *)reconfig_shmem_storage;
		cluster_reconfig_record_observed_slot(
			0, UINT64_C(80), UINT64_C(7), CLUSTER_EPOCH_INITIAL);
		cluster_reconfig_record_observed_fresh_alive(0, true);
		cluster_membership_record_admitted(0, UINT64_C(80));
		cluster_membership_set_state(0, CLUSTER_MEMBER_MEMBER);
	ut_xid_stripe_disk_state = CLUSTER_XID_STRIPE_DISK_PUBLISHED;
		ut_wal_slot_verdict[0] = CLUSTER_WAL_SLOT_OK;
		ut_wal_slot[0].thread_id = 1;
		ut_wal_slot[0].node_id = 0;
		ut_wal_slot[0].state = CLUSTER_WAL_SLOT_STATE_ACTIVE;
		ut_wal_slot[0].started_at = INT64_C(81);
		ut_wal_slot[0].last_updated = INT64_C(81);
		ut_xid_stripe_verdict = CLUSTER_XID_STRIPE_JOIN_HOLD;
		ut_xid_stripe_progress = STRIPE_JOIN_CLAIM_OWNED;

		/* The exact peer evidence can authorize only the existing
		 * JOIN_READONLY deferral while recovery is active.  It must not
		 * classify, claim, or start the ordinary deadline yet. */
		ut_recovery_in_progress = true;
		cluster_reconfig_lmon_tick();
		UT_ASSERT_EQ(ut_xid_stripe_join_progress_calls, 0);
		UT_ASSERT_EQ(state->self_join_deadline_us, 0);
		UT_ASSERT_EQ(state->self_join_admitted, 0);

		/* After StartupXLOG, the same fresh/current admitted epoch-0 peer
		 * classifies self as an ordinary JOINING node.  The existing stripe
		 * owner stages the exact claim and owns the unrefreshed deadline. */
		ut_recovery_in_progress = false;
		cluster_reconfig_lmon_tick();
		UT_ASSERT_EQ(ut_xid_stripe_join_progress_calls, 1);
		UT_ASSERT(!ut_xid_stripe_progress_last_may_seed);
		UT_ASSERT_EQ((int)cluster_membership_get_state(cluster_node_id),
					 (int)CLUSTER_MEMBER_JOINING);
		UT_ASSERT(state->self_join_deadline_us != 0);
		UT_ASSERT_EQ(state->self_join_failed, 0);
		fflush(stdout);
		_exit(ut_current_failed == 0 ? 0 : 1);
	}

	UT_ASSERT_EQ(waitpid(pid, &status, 0), pid);
	UT_ASSERT(WIFEXITED(status));
	UT_ASSERT_EQ(WEXITSTATUS(status), 0);
}

/* A node that already bound the existing recovery authority before
 * StartupXLOG is a normal founder, not a JOIN_READONLY late founder.  Exact
 * epoch-0 peer evidence becoming visible after recovery must not demote that
 * founder into ordinary JOINING or transfer its stripe progress to JCMK. */
UT_TEST(test_epoch0_current_authority_is_not_reclassified_as_late_founder)
{
	pid_t pid;
	int status = 0;

	fflush(NULL);
	pid = fork();
	UT_ASSERT(pid >= 0);
	if (pid < 0)
		return;
	if (pid == 0) {
		ClusterReconfigState *state;

		ut_current_failed = 0;
		cluster_online_join = true;
		ut_join_setup();
		cluster_node_id = 3;
		ut_declared_set[0] = true;
		ut_declared_set[3] = true;
		ut_in_quorum_value = true;
		ut_set_self_incarnation_sequence(UINT64_C(83), UINT64_C(83),
								 UINT64_C(83));
		state = (ClusterReconfigState *)reconfig_shmem_storage;
		cluster_reconfig_record_observed_slot(
			0, UINT64_C(80), UINT64_C(7), CLUSTER_EPOCH_INITIAL);
		cluster_reconfig_record_observed_fresh_alive(0, true);
		cluster_membership_record_admitted(0, UINT64_C(80));
		cluster_membership_set_state(0, CLUSTER_MEMBER_MEMBER);
		ut_xid_stripe_disk_state = CLUSTER_XID_STRIPE_DISK_PUBLISHED;
		ut_xid_stripe_verdict = CLUSTER_XID_STRIPE_JOIN_PROCEED;
		ut_xid_stripe_progress = STRIPE_JOIN_CLAIM_OWNED;
		ut_recovery_in_progress = false;
		ut_authority_managed = true;

		cluster_reconfig_lmon_tick();

		UT_ASSERT_EQ(ut_xid_stripe_join_progress_calls, 0);
		UT_ASSERT_EQ(ut_xid_stripe_join_gate_calls, 1);
		UT_ASSERT_EQ(state->self_join_admitted, 1);
		UT_ASSERT_EQ((int)cluster_membership_get_state(cluster_node_id),
					 (int)CLUSTER_MEMBER_MEMBER);
		fflush(stdout);
		_exit(ut_current_failed == 0 ? 0 : 1);
	}

	UT_ASSERT_EQ(waitpid(pid, &status, 0), pid);
	UT_ASSERT(WIFEXITED(status));
	UT_ASSERT_EQ(WEXITSTATUS(status), 0);
}

static ClusterReconfigState *
ut_prepare_epoch0_managed_handoff(void)
{
	ClusterReconfigState *state;

	cluster_online_join = true;
	ut_join_setup();
	cluster_node_id = 3;
	ut_declared_set[0] = true;
	ut_declared_set[3] = true;
	ut_in_quorum_value = true;
	ut_set_self_incarnation_sequence(UINT64_C(83), UINT64_C(83),
								 UINT64_C(83));
	state = (ClusterReconfigState *)reconfig_shmem_storage;
	cluster_reconfig_record_observed_slot(
		0, UINT64_C(80), UINT64_C(7), CLUSTER_EPOCH_INITIAL);
	cluster_reconfig_record_observed_fresh_alive(0, true);
	cluster_membership_record_admitted(0, UINT64_C(80));
	cluster_membership_set_state(0, CLUSTER_MEMBER_MEMBER);
	/* This boot has not been admitted yet.  The retained floor is the exact
	 * predecessor, strictly older than the presented boot incarnation. */
	cluster_membership_record_admitted(cluster_node_id, UINT64_C(82));
	cluster_membership_set_state(cluster_node_id, CLUSTER_MEMBER_MEMBER);
	state->self_join_admitted = 0;
	state->self_join_failed = 0;
	state->self_join_deadline_us = 0;
	ut_xid_stripe_disk_state = CLUSTER_XID_STRIPE_DISK_PUBLISHED;
	ut_wal_slot_verdict[0] = CLUSTER_WAL_SLOT_OK;
	ut_wal_slot[0].thread_id = 1;
	ut_wal_slot[0].node_id = 0;
	ut_wal_slot[0].state = CLUSTER_WAL_SLOT_STATE_ACTIVE;
	ut_wal_slot[0].started_at = INT64_C(81);
	ut_wal_slot[0].last_updated = INT64_C(81);
	ut_recovery_in_progress = false;
	ut_authority_managed = true;
	ut_authority_readiness = CLUSTER_AUTHORITY_OFF;
	ut_handoff_identity_incarnation = UINT64_C(83);
	ut_handoff_identity_predecessor_floor = UINT64_C(82);
	MyProc = NULL;
	return state;
}

/* Phase 3 has no PGPROC.  The exact handoff must use conditional acquisition,
 * leave MEMBER untouched when the reconfig lock is busy, and accept only the
 * exact predecessor floor.  Once staged, the byte-identical JOINING
 * tuple is an idempotent success and may not refresh any field. */
UT_TEST(test_pre_publish_join_handoff_is_exact_idempotent_and_nonblocking)
{
	ClusterReconfigState *state = ut_prepare_epoch0_managed_handoff();
	uint64 deadline_before;
	uint8 admitted_before;
	uint8 failed_before;

	ut_lwlock_conditional_calls = 0;
	ut_lwlock_blocking_calls = 0;
	/* Two exact peer captures precede the handoff's EXCLUSIVE claim. */
	ut_lwlock_conditional_fail_call = 3;
	UT_ASSERT(!cluster_reconfig_stage_pre_publish_join_handoff(
		UINT64_C(83), UINT64_C(82)));
	UT_ASSERT_EQ(ut_lwlock_conditional_calls, 3);
	UT_ASSERT_EQ(ut_lwlock_blocking_calls, 0);
	UT_ASSERT_EQ((int)cluster_membership_get_state(cluster_node_id),
				 (int)CLUSTER_MEMBER_MEMBER);

	ut_set_self_incarnation_sequence(UINT64_C(83), UINT64_C(83),
								 UINT64_C(83));
	ut_lwlock_conditional_fail_call = 0;
	ut_lwlock_conditional_calls = 0;
	ut_lwlock_blocking_calls = 0;
	UT_ASSERT(cluster_reconfig_stage_pre_publish_join_handoff(
		UINT64_C(83), UINT64_C(82)));
	UT_ASSERT_EQ(ut_lwlock_blocking_calls, 0);
	UT_ASSERT_EQ((int)cluster_membership_get_state(cluster_node_id),
				 (int)CLUSTER_MEMBER_JOINING);
	UT_ASSERT(cluster_reconfig_pre_publish_join_handoff_current());

	admitted_before = state->self_join_admitted;
	failed_before = state->self_join_failed;
	deadline_before = state->self_join_deadline_us;
	ut_set_self_incarnation_sequence(UINT64_C(83), UINT64_C(83),
								 UINT64_C(83));
	UT_ASSERT(cluster_reconfig_stage_pre_publish_join_handoff(
		UINT64_C(83), UINT64_C(82)));
	UT_ASSERT_EQ(state->self_join_admitted, admitted_before);
	UT_ASSERT_EQ(state->self_join_failed, failed_before);
	UT_ASSERT_EQ(state->self_join_deadline_us, deadline_before);
	UT_ASSERT_EQ(cluster_membership_get_last_admitted_incarnation(
		cluster_node_id), UINT64_C(82));
}

/* Every conjunct is fail-closed: no generic managed+OFF+JOINING state may be
 * promoted into the ordinary owner, and a post-write identity drift leaves
 * only the write-closed staged tuple for a later exact revalidation. */
UT_TEST(test_pre_publish_join_handoff_rejects_nonexact_state)
{
	ClusterReconfigState *state;

	state = ut_prepare_epoch0_managed_handoff();
	ut_authority_readiness = CLUSTER_AUTHORITY_RECOVERY_READY;
	UT_ASSERT(!cluster_reconfig_stage_pre_publish_join_handoff(
		UINT64_C(83), UINT64_C(82)));
	UT_ASSERT_EQ((int)cluster_membership_get_state(cluster_node_id),
				 (int)CLUSTER_MEMBER_MEMBER);

	state = ut_prepare_epoch0_managed_handoff();
	UT_ASSERT(!cluster_reconfig_stage_pre_publish_join_handoff(
		UINT64_C(84), UINT64_C(82)));

	state = ut_prepare_epoch0_managed_handoff();
	UT_ASSERT(!cluster_reconfig_stage_pre_publish_join_handoff(
		UINT64_C(83), UINT64_C(81)));

	state = ut_prepare_epoch0_managed_handoff();
	cluster_membership_record_admitted(cluster_node_id, UINT64_C(83));
	UT_ASSERT(!cluster_reconfig_stage_pre_publish_join_handoff(
		UINT64_C(83), UINT64_C(83)));
	UT_ASSERT_EQ((int)cluster_membership_get_state(cluster_node_id),
				 (int)CLUSTER_MEMBER_MEMBER);

	state = ut_prepare_epoch0_managed_handoff();
	cluster_membership_record_admitted(cluster_node_id, UINT64_C(84));
	UT_ASSERT(!cluster_reconfig_stage_pre_publish_join_handoff(
		UINT64_C(83), UINT64_C(84)));

	state = ut_prepare_epoch0_managed_handoff();
	state->replacement_episode = ut_admitted_replacement_episode(cluster_node_id);
	UT_ASSERT(!cluster_reconfig_stage_pre_publish_join_handoff(
		UINT64_C(83), UINT64_C(82)));

	state = ut_prepare_epoch0_managed_handoff();
	ut_clean_leave_in_progress = true;
	UT_ASSERT(!cluster_reconfig_stage_pre_publish_join_handoff(
		UINT64_C(83), UINT64_C(82)));

	state = ut_prepare_epoch0_managed_handoff();
	state->removed_bitmap[cluster_node_id / 8]
		|= (uint8)(1u << (cluster_node_id % 8));
	UT_ASSERT(!cluster_reconfig_stage_pre_publish_join_handoff(
		UINT64_C(83), UINT64_C(82)));

	state = ut_prepare_epoch0_managed_handoff();
	state->self_join_failed = 1;
	cluster_membership_set_state(cluster_node_id, CLUSTER_MEMBER_JOINING);
	UT_ASSERT(!cluster_reconfig_pre_publish_join_handoff_current());

	state = ut_prepare_epoch0_managed_handoff();
	state->self_join_deadline_us = UINT64_C(9001);
	cluster_membership_set_state(cluster_node_id, CLUSTER_MEMBER_JOINING);
	UT_ASSERT(!cluster_reconfig_pre_publish_join_handoff_current());

	state = ut_prepare_epoch0_managed_handoff();
	ut_set_self_incarnation_sequence(UINT64_C(83), UINT64_C(84),
								 UINT64_C(84));
	UT_ASSERT(!cluster_reconfig_stage_pre_publish_join_handoff(
		UINT64_C(83), UINT64_C(82)));
	UT_ASSERT_EQ((int)cluster_membership_get_state(cluster_node_id),
				 (int)CLUSTER_MEMBER_JOINING);
	UT_ASSERT_EQ(state->self_join_admitted, 0);
	UT_ASSERT_EQ(state->self_join_failed, 0);
	UT_ASSERT_EQ(state->self_join_deadline_us, 0);
	/* The failed call cannot publish admission or reopen writes.  A later
	 * classifier starts from a fresh current incarnation/floor sample; the
	 * handoff does not retain a second cross-boot identity authority. */

	state = ut_prepare_epoch0_managed_handoff();
	UT_ASSERT(cluster_reconfig_stage_pre_publish_join_handoff(
		UINT64_C(83), UINT64_C(82)));
	cluster_membership_record_admitted(cluster_node_id, UINT64_C(83));
	UT_ASSERT(!cluster_reconfig_pre_publish_join_handoff_current());
	UT_ASSERT_EQ(state->self_join_admitted, 0);
}

/* The approved pre-publish pivot retains the boot-lifetime managed latch but
 * exposes its exact JOIN_READONLY edge to the ordinary classifier.  A forked
 * LMON-side owner must consume that shared tuple, drive the non-seeding stripe
 * prerequisite, reject an early JCMK callback, and publish terminal admission
 * only after the existing stripe/JOIN-WAL gate reports PROCEED. */
UT_TEST(test_epoch0_managed_pivot_drives_ordinary_join_owner)
{
	pid_t pid;
	int status = 0;
	ClusterReconfigState *state = ut_prepare_epoch0_managed_handoff();

	UT_ASSERT(cluster_reconfig_stage_pre_publish_join_handoff(
		UINT64_C(83), UINT64_C(82)));
	UT_ASSERT_EQ((int)cluster_membership_get_state(cluster_node_id),
				 (int)CLUSTER_MEMBER_JOINING);

	fflush(NULL);
	pid = fork();
	UT_ASSERT(pid >= 0);
	if (pid < 0)
		return;
	if (pid == 0) {
		ut_current_failed = 0;
		ut_set_self_incarnation_sequence(UINT64_C(83), UINT64_C(83),
									 UINT64_C(83));
		ut_xid_stripe_verdict = CLUSTER_XID_STRIPE_JOIN_HOLD;
		ut_xid_stripe_progress = STRIPE_JOIN_CLAIM_OWNED;
		ut_recovery_in_progress = false;
		ut_authority_managed = true;
		ut_authority_readiness = CLUSTER_AUTHORITY_OFF;

		cluster_reconfig_lmon_tick();

		UT_ASSERT_EQ(ut_xid_stripe_join_progress_calls, 1);
		UT_ASSERT(!ut_xid_stripe_progress_last_may_seed);
		UT_ASSERT_EQ((int)cluster_membership_get_state(cluster_node_id),
					 (int)CLUSTER_MEMBER_JOINING);
		UT_ASSERT_EQ(state->self_join_admitted, 0);
		UT_ASSERT(state->self_join_deadline_us != 0);

		/* Exact JCMK may arrive before PGXS/JOIN WAL.  The existing admission
		 * callback must keep the tuple write-closed until the unified stripe
		 * gate is terminal-current. */
		cluster_reconfig_note_self_admitted(UINT64_C(7));
		UT_ASSERT_EQ(state->self_join_admitted, 0);
		UT_ASSERT_EQ((int)cluster_membership_get_state(cluster_node_id),
						 (int)CLUSTER_MEMBER_JOINING);
		ut_xid_stripe_verdict = CLUSTER_XID_STRIPE_JOIN_PROCEED;
		cluster_reconfig_note_self_admitted(UINT64_C(7));
		UT_ASSERT_EQ(state->self_join_admitted, 1);
		UT_ASSERT_EQ((int)cluster_membership_get_state(cluster_node_id),
						 (int)CLUSTER_MEMBER_MEMBER);
		UT_ASSERT_EQ(cluster_membership_get_last_admitted_incarnation(
			cluster_node_id), UINT64_C(83));
		fflush(stdout);
		_exit(ut_current_failed == 0 ? 0 : 1);
	}

	UT_ASSERT_EQ(waitpid(pid, &status, 0), pid);
	UT_ASSERT(WIFEXITED(status));
	UT_ASSERT_EQ(WEXITSTATUS(status), 0);
}

UT_TEST(test_epoch0_late_founder_evidence_requires_exact_peer_and_pgxa)
{
	ut_join_setup();
	cluster_online_join = true;
	cluster_node_id = 3;
	ut_declared_set[0] = true;
	ut_declared_set[3] = true;
	ut_in_quorum_value = true;
	cluster_reconfig_record_observed_slot(
		0, UINT64_C(80), UINT64_C(7), CLUSTER_EPOCH_INITIAL);
	cluster_reconfig_record_observed_fresh_alive(0, true);
	cluster_membership_record_admitted(0, UINT64_C(80));
	cluster_membership_set_state(0, CLUSTER_MEMBER_MEMBER);

	/* A current admitted peer is insufficient until the existing durable
	 * activation face proves that some founder completed recovery and seeded
	 * PGXA. */
	ut_xid_stripe_disk_state = CLUSTER_XID_STRIPE_DISK_UNKNOWN;
	UT_ASSERT(!cluster_reconfig_epoch0_late_founder_evidence_current());
	ut_xid_stripe_disk_state = CLUSTER_XID_STRIPE_DISK_PUBLISHED;
	UT_ASSERT(!cluster_reconfig_epoch0_late_founder_evidence_current());

	/* A Phase-1 ACTIVE slot predating this peer's current QVOTEC boot is stale
	 * for the Phase-2 bridge.  Only a current-boot ACTIVE pair proves that a
	 * distinct database instance actually completed StartupXLOG. */
	ut_wal_slot_verdict[0] = CLUSTER_WAL_SLOT_OK;
	ut_wal_slot[0].thread_id = 1;
	ut_wal_slot[0].node_id = 0;
	ut_wal_slot[0].state = CLUSTER_WAL_SLOT_STATE_ACTIVE;
	ut_wal_slot[0].started_at = INT64_C(79);
	ut_wal_slot[0].last_updated = INT64_C(79);
	UT_ASSERT(!cluster_reconfig_epoch0_late_founder_evidence_current());
	ut_wal_slot[0].started_at = INT64_C(81);
	ut_wal_slot[0].last_updated = INT64_C(81);
	UT_ASSERT(cluster_reconfig_epoch0_late_founder_evidence_current());

	cluster_reconfig_record_observed_fresh_alive(0, false);
	UT_ASSERT(!cluster_reconfig_epoch0_late_founder_evidence_current());
	cluster_reconfig_record_observed_fresh_alive(0, true);
	cluster_reconfig_record_observed_slot(
		0, UINT64_C(81), UINT64_C(8), CLUSTER_EPOCH_INITIAL);
	UT_ASSERT(!cluster_reconfig_epoch0_late_founder_evidence_current());
	cluster_membership_record_admitted(0, UINT64_C(81));
	UT_ASSERT(cluster_reconfig_epoch0_late_founder_evidence_current());

	/* An in-progress QVOTEC publication and a non-initial peer both remain
	 * outside this narrow epoch-0 bridge. */
	cluster_reconfig_bootstrap_publish_begin();
	UT_ASSERT(!cluster_reconfig_epoch0_late_founder_evidence_current());
	cluster_reconfig_bootstrap_publish_end();
	cluster_reconfig_record_observed_slot(
		0, UINT64_C(81), UINT64_C(9), UINT64_C(1));
	UT_ASSERT(!cluster_reconfig_epoch0_late_founder_evidence_current());
}

UT_TEST(test_late_joiner_stripe_refuse_is_terminal_fail_closed)
{
	pid_t pid;
	int status = 0;

	fflush(NULL);
	pid = fork();
	UT_ASSERT(pid >= 0);
	if (pid < 0)
		return;
	if (pid == 0) {
		ClusterReconfigState *state;

		ut_current_failed = 0;
		cluster_online_join = true;
		ut_join_setup();
		cluster_node_id = 1;
		ut_declared_set[0] = true;
		ut_declared_set[1] = true;
		ut_in_quorum_value = true;
		ut_set_self_incarnation_sequence(UINT64_C(72), UINT64_C(72),
									 UINT64_C(72));
		state = (ClusterReconfigState *)reconfig_shmem_storage;
		cluster_reconfig_record_observed_slot(
			0, UINT64_C(100), UINT64_C(2), UINT64_C(7));
		cluster_reconfig_record_observed_fresh_alive(0, true);
		ut_xid_stripe_progress = STRIPE_JOIN_REFUSE;

		cluster_reconfig_lmon_tick();
		UT_ASSERT_EQ(ut_xid_stripe_join_progress_calls, 1);
		UT_ASSERT_EQ(state->self_join_admitted, 0);
		UT_ASSERT_EQ(state->self_join_failed, 1);
		UT_ASSERT_EQ(state->self_join_deadline_us, 0);
		UT_ASSERT_EQ((int)cluster_membership_get_state(cluster_node_id),
					 (int)CLUSTER_MEMBER_REJECTED);
		UT_ASSERT_EQ((int)cluster_reconfig_self_join_gate_verdict(),
					 (int)CLUSTER_JOIN_GATE_BLOCK_53R61);
		fflush(stdout);
		_exit(ut_current_failed == 0 ? 0 : 1);
	}

	UT_ASSERT_EQ(waitpid(pid, &status, 0), pid);
	UT_ASSERT(WIFEXITED(status));
	UT_ASSERT_EQ(WEXITSTATUS(status), 0);
}

/* A durable ordinary JOIN_COMMITTED proof cannot open service while the
 * existing xid-stripe gate is still staging the current node's own PGXS
 * claim.  The callback is retried by QVOTEC and must never seed PGXA. */
UT_TEST(test_online_join_waits_for_own_stripe_claim)
{
	ClusterReconfigState *state;

	cluster_online_join = true;
	ut_join_setup();
	cluster_node_id = 1;
	ut_declared_set[1] = true;
	ut_set_self_incarnation_sequence(UINT64_C(81), UINT64_C(81),
								 UINT64_C(81));
	state = (ClusterReconfigState *)reconfig_shmem_storage;
	cluster_membership_set_state(cluster_node_id, CLUSTER_MEMBER_JOINING);
	ut_xid_stripe_verdict = CLUSTER_XID_STRIPE_JOIN_HOLD;

	cluster_reconfig_note_self_admitted(UINT64_C(7));
	UT_ASSERT_EQ(ut_xid_stripe_join_gate_calls, 1);
	UT_ASSERT(!ut_xid_stripe_last_may_seed);
	UT_ASSERT_EQ(state->self_join_admitted, 0);
	UT_ASSERT_EQ((int)cluster_membership_get_state(cluster_node_id),
				 (int)CLUSTER_MEMBER_JOINING);
	UT_ASSERT_EQ(cluster_membership_get_last_admitted_incarnation(
		cluster_node_id), UINT64_C(0));

	ut_xid_stripe_verdict = CLUSTER_XID_STRIPE_JOIN_PROCEED;
	cluster_reconfig_note_self_admitted(UINT64_C(7));
	UT_ASSERT_EQ(ut_xid_stripe_join_gate_calls, 2);
	UT_ASSERT(!ut_xid_stripe_last_may_seed);
	UT_ASSERT_EQ(state->self_join_admitted, 1);
	UT_ASSERT_EQ((int)cluster_membership_get_state(cluster_node_id),
				 (int)CLUSTER_MEMBER_MEMBER);
	UT_ASSERT_EQ(cluster_membership_get_last_admitted_incarnation(
		cluster_node_id), UINT64_C(81));
	cluster_online_join = false;
}

/* PGXA=PUBLISHED and PGXS=MINE are still insufficient when the per-thread
 * JOIN WAL record is not insertable.  The same ordinary admission callback
 * must remain closed until the unified stripe gate reports PROCEED. */
UT_TEST(test_online_join_waits_for_join_wal)
{
	ClusterReconfigState *state;

	cluster_online_join = true;
	ut_join_setup();
	cluster_node_id = 2;
	ut_declared_set[2] = true;
	ut_set_self_incarnation_sequence(UINT64_C(82), UINT64_C(82),
								 UINT64_C(82));
	state = (ClusterReconfigState *)reconfig_shmem_storage;
	cluster_membership_set_state(cluster_node_id, CLUSTER_MEMBER_JOINING);
	ut_xid_stripe_verdict = CLUSTER_XID_STRIPE_JOIN_HOLD;

	cluster_reconfig_note_self_admitted(UINT64_C(9));
	UT_ASSERT_EQ(ut_xid_stripe_join_gate_calls, 1);
	UT_ASSERT(!ut_xid_stripe_last_may_seed);
	UT_ASSERT_EQ(state->self_join_admitted, 0);
	UT_ASSERT_EQ((int)cluster_membership_get_state(cluster_node_id),
				 (int)CLUSTER_MEMBER_JOINING);

	ut_xid_stripe_verdict = CLUSTER_XID_STRIPE_JOIN_PROCEED;
	cluster_reconfig_note_self_admitted(UINT64_C(9));
	UT_ASSERT_EQ(ut_xid_stripe_join_gate_calls, 2);
	UT_ASSERT(!ut_xid_stripe_last_may_seed);
	UT_ASSERT_EQ(state->self_join_admitted, 1);
	UT_ASSERT_EQ((int)cluster_membership_get_state(cluster_node_id),
				 (int)CLUSTER_MEMBER_MEMBER);
	UT_ASSERT_EQ(cluster_membership_get_last_admitted_incarnation(
		cluster_node_id), UINT64_C(82));
	cluster_online_join = false;
}

UT_TEST(test_shared_cf_prior_unclean_rejoin_cannot_fall_back_to_cold_bootstrap)
{
	pid_t pid;
	int status = 0;

	/* Both early-boot decisions are process-local; isolate the exact fresh
	 * incarnation just as a real postmaster restart does. */
	fflush(NULL);
	pid = fork();
	UT_ASSERT(pid >= 0);
	if (pid < 0)
		return;
	if (pid == 0) {
		ut_current_failed = 0;
		cluster_online_join = false;
		ut_join_setup();
		cluster_controlfile_shared_authority = true;
		ut_in_quorum_value = true;
		ut_declared_set[1] = true;
		ut_offpath_boot_decided = false;
		ut_prior_unclean_death = true;
		ut_join_view_rebuilt = true;
		cluster_reconfig_record_observed_slot(0, UINT64_C(77), UINT64_C(1), 0);
		cluster_reconfig_record_observed_slot(1, UINT64_C(70), UINT64_C(1), 0);
		cluster_reconfig_record_observed_fresh_alive(0, true);
		cluster_reconfig_record_observed_fresh_alive(1, true);

		/* First tick arms the off-path fence.  The next tick must retain the
		 * admission gate even though the two fresh epoch-0 slots also satisfy
		 * the generic cold-bootstrap proof. */
		cluster_reconfig_lmon_tick();
		UT_ASSERT_EQ((int)cluster_reconfig_self_join_gate_verdict(),
					 (int)CLUSTER_JOIN_GATE_BLOCK_53R60);
		cluster_reconfig_lmon_tick();
		UT_ASSERT_EQ((int)cluster_reconfig_self_join_gate_verdict(),
					 (int)CLUSTER_JOIN_GATE_BLOCK_53R60);
		UT_ASSERT(!ut_offpath_boot_decided);
		fflush(stdout);
		_exit(ut_current_failed == 0 ? 0 : 1);
	}

	UT_ASSERT_EQ(waitpid(pid, &status, 0), pid);
	UT_ASSERT(WIFEXITED(status));
	UT_ASSERT_EQ(WEXITSTATUS(status), 0);
}

UT_TEST(test_offpath_member_requires_decided_boot_and_exact_current_floor)
{
	ClusterReconfigState *state;
	uint64 invalidations_after_publish;

	/* A decided single-node off-path formation may publish the exact current
	 * floor, but the shmem default self_join_admitted=1 is not authority alone. */
	cluster_online_join = false;
	ut_join_setup();
	ut_in_quorum_value = true;
	state = (ClusterReconfigState *)reconfig_shmem_storage;
	ut_offpath_boot_decided = true;
	ut_set_self_incarnation_sequence(UINT64_C(101), UINT64_C(101),
								 UINT64_C(101));
	cluster_reconfig_lmon_tick();
	UT_ASSERT_EQ(state->self_join_admitted, 1);
	UT_ASSERT_EQ(cluster_membership_get_last_admitted_incarnation(cluster_node_id),
				 UINT64_C(101));
	UT_ASSERT_EQ((int)cluster_membership_get_state(cluster_node_id),
				 (int)CLUSTER_MEMBER_MEMBER);
	invalidations_after_publish = ut_authority_cache_invalidate_count;
	cluster_reconfig_lmon_tick();
	UT_ASSERT_EQ(ut_authority_cache_invalidate_count,
				 invalidations_after_publish);

	/* With multiple declared nodes and no fresh bootstrap quorum, Shape A is
	 * UNDECIDED.  The ordinary maintenance choke must withhold both floor and
	 * MEMBER even though the config-off shmem byte was initialized to one. */
	ut_join_setup();
	ut_in_quorum_value = true;
	state = (ClusterReconfigState *)reconfig_shmem_storage;
	ut_declared_set[1] = true;
	ut_declared_set[2] = true;
	ut_offpath_boot_decided = false;
	ut_set_self_incarnation_sequence(UINT64_C(102), UINT64_C(102),
								 UINT64_C(102));
	cluster_reconfig_lmon_tick();
	UT_ASSERT_EQ(state->self_join_admitted, 1);
	UT_ASSERT_EQ(cluster_membership_get_last_admitted_incarnation(cluster_node_id),
				 UINT64_C(0));
	UT_ASSERT_EQ((int)cluster_membership_get_state(cluster_node_id),
				 (int)CLUSTER_MEMBER_JOINING);
}

UT_TEST(test_ordinary_self_floor_drift_and_high_water_fail_closed_then_retry)
{
	/* Zero is not a floor; a later stable current incarnation retries cleanly. */
	cluster_online_join = false;
	ut_join_setup();
	ut_in_quorum_value = true;
	ut_offpath_boot_decided = true;
	ut_set_self_incarnation_sequence(0, 0, 0);
	cluster_reconfig_lmon_tick();
	UT_ASSERT_EQ((int)cluster_membership_get_state(cluster_node_id),
				 (int)CLUSTER_MEMBER_JOINING);
	UT_ASSERT_EQ(cluster_membership_get_last_admitted_incarnation(cluster_node_id),
				 UINT64_C(0));
	ut_set_self_incarnation_sequence(UINT64_C(31), UINT64_C(31), UINT64_C(31));
	cluster_reconfig_lmon_tick();
	UT_ASSERT_EQ((int)cluster_membership_get_state(cluster_node_id),
				 (int)CLUSTER_MEMBER_MEMBER);
	UT_ASSERT_EQ(cluster_membership_get_last_admitted_incarnation(cluster_node_id),
				 UINT64_C(31));

	/* A change between the pre-lock and lock-held samples records nothing. */
	ut_join_setup();
	ut_in_quorum_value = true;
	ut_offpath_boot_decided = true;
	ut_set_self_incarnation_sequence(UINT64_C(40), UINT64_C(41), UINT64_C(41));
	cluster_reconfig_lmon_tick();
	UT_ASSERT_EQ((int)cluster_membership_get_state(cluster_node_id),
				 (int)CLUSTER_MEMBER_JOINING);
	UT_ASSERT_EQ(cluster_membership_get_last_admitted_incarnation(cluster_node_id),
				 UINT64_C(0));
	ut_set_self_incarnation_sequence(UINT64_C(41), UINT64_C(41), UINT64_C(41));
	cluster_reconfig_lmon_tick();
	UT_ASSERT_EQ(cluster_membership_get_last_admitted_incarnation(cluster_node_id),
				 UINT64_C(41));
	UT_ASSERT_EQ((int)cluster_membership_get_state(cluster_node_id),
				 (int)CLUSTER_MEMBER_MEMBER);

	/* A post-record incarnation change leaves the raised floor but withholds
	 * MEMBER until a later stable incarnation raises it again. */
	ut_join_setup();
	ut_in_quorum_value = true;
	ut_offpath_boot_decided = true;
	ut_set_self_incarnation_sequence(UINT64_C(60), UINT64_C(60), UINT64_C(61));
	cluster_reconfig_lmon_tick();
	UT_ASSERT_EQ(cluster_membership_get_last_admitted_incarnation(cluster_node_id),
				 UINT64_C(60));
	UT_ASSERT_EQ((int)cluster_membership_get_state(cluster_node_id),
				 (int)CLUSTER_MEMBER_JOINING);
	ut_set_self_incarnation_sequence(UINT64_C(61), UINT64_C(61), UINT64_C(61));
	cluster_reconfig_lmon_tick();
	UT_ASSERT_EQ(cluster_membership_get_last_admitted_incarnation(cluster_node_id),
				 UINT64_C(61));
	UT_ASSERT_EQ((int)cluster_membership_get_state(cluster_node_id),
				 (int)CLUSTER_MEMBER_MEMBER);

	/* Monotonic record_admitted cannot lower a prior floor.  Exact equality is
	 * still required before MEMBER, then the matching formation retries. */
	ut_join_setup();
	ut_in_quorum_value = true;
	ut_offpath_boot_decided = true;
	cluster_membership_record_admitted(cluster_node_id, UINT64_C(70));
	ut_set_self_incarnation_sequence(UINT64_C(69), UINT64_C(69), UINT64_C(69));
	cluster_reconfig_lmon_tick();
	UT_ASSERT_EQ(cluster_membership_get_last_admitted_incarnation(cluster_node_id),
				 UINT64_C(70));
	UT_ASSERT_EQ((int)cluster_membership_get_state(cluster_node_id),
				 (int)CLUSTER_MEMBER_JOINING);
	ut_set_self_incarnation_sequence(UINT64_C(70), UINT64_C(70), UINT64_C(70));
	cluster_reconfig_lmon_tick();
	UT_ASSERT_EQ((int)cluster_membership_get_state(cluster_node_id),
				 (int)CLUSTER_MEMBER_MEMBER);
	UT_ASSERT_EQ(cluster_membership_get_last_admitted_incarnation(cluster_node_id),
				 UINT64_C(70));
}

UT_TEST(test_self_join_gate_lifecycle)
{
	ut_join_setup();
	cluster_node_id = 2;
	ut_declared_set[2] = true;
	ut_declared_set[0] = true;
	ut_in_quorum_value = true;
	cluster_enabled = true;
	cluster_online_join = true;

	/* default gate open */
	UT_ASSERT_EQ((int)cluster_reconfig_self_join_gate_verdict(), (int)CLUSTER_JOIN_GATE_ALLOW);

	/* a peer observed at epoch > 0 => cluster running => this node is rejoining;
	 * the tick's joiner self-tick closes the gate. */
	cluster_reconfig_record_observed_slot(0, 100, 1, 7); /* node 0 at epoch 7 */
	cluster_reconfig_lmon_tick();
	UT_ASSERT_EQ((int)cluster_reconfig_self_join_gate_verdict(),
				 (int)CLUSTER_JOIN_GATE_BLOCK_53R60);

	/* admission (note_self_admitted) reopens the gate. */
	cluster_reconfig_note_self_admitted(7);
	UT_ASSERT_EQ((int)cluster_reconfig_self_join_gate_verdict(), (int)CLUSTER_JOIN_GATE_ALLOW);

	cluster_online_join = false; /* leave global off for other tests */
}

/* Replacement ADMITTED publishes only MEMBER metadata.  It stores one exact
 * validated local episode and closes the write gate even when the ordinary
 * join gate was open.  A later ordinary v2 admission callback cannot bypass
 * that replacement-only closed lane. */
UT_TEST(test_replacement_member_publish_is_exact_and_write_closed)
{
	ClusterReplacementEpisode episode;
	ClusterReplacementEpisode wrong;
	ClusterReconfigState *state;

	cluster_online_join = false;
	ut_join_setup();
	state = (ClusterReconfigState *)reconfig_shmem_storage;
	episode = ut_admitted_replacement_episode(cluster_node_id);

	wrong = episode;
	wrong.phase = CLUSTER_REPLACEMENT_EPISODE_COMMITTED_CLOSED;
	UT_ASSERT(!cluster_reconfig_publish_replacement_member_closed(&wrong));
	UT_ASSERT_EQ((int)cluster_membership_get_state(cluster_node_id),
				 (int)CLUSTER_MEMBER_ABSENT);
	UT_ASSERT_EQ((int)cluster_reconfig_self_join_gate_verdict(),
				 (int)CLUSTER_JOIN_GATE_ALLOW);

	wrong = episode;
	wrong.target_node_id = 3;
	UT_ASSERT(!cluster_reconfig_publish_replacement_member_closed(&wrong));

	UT_ASSERT(cluster_reconfig_publish_replacement_member_closed(&episode));
	UT_ASSERT_EQ((int)cluster_membership_get_state(cluster_node_id),
				 (int)CLUSTER_MEMBER_MEMBER);
	UT_ASSERT_EQ((int)cluster_reconfig_self_join_gate_verdict(),
				 (int)CLUSTER_JOIN_GATE_BLOCK_53R60);
	UT_ASSERT_EQ(memcmp(&state->replacement_episode, &episode, sizeof(episode)), 0);

	/* A different, individually valid episode cannot replace the exact local
	 * ADMITTED identity through the MEMBER publisher. */
	wrong = episode;
	wrong.request_nonce++;
	UT_ASSERT(!cluster_reconfig_publish_replacement_member_closed(&wrong));
	UT_ASSERT_EQ(memcmp(&state->replacement_episode, &episode, sizeof(episode)), 0);

	/* The ordinary v2 callback remains unable to open a replacement episode. */
	cluster_reconfig_note_self_admitted(episode.reserved_or_committed_epoch);
	UT_ASSERT_EQ((int)cluster_reconfig_self_join_gate_verdict(),
				 (int)CLUSTER_JOIN_GATE_BLOCK_53R60);
	UT_ASSERT_EQ((int)cluster_membership_get_state(cluster_node_id),
				 (int)CLUSTER_MEMBER_MEMBER);
}

/* The ordinary LMON self-state maintenance must not infer JOINING solely from
 * the closed write byte once the exact local replacement episode is ADMITTED:
 * replacement MEMBER is deliberately a service-closed state. */
UT_TEST(test_lmon_preserves_replacement_admitted_member_while_write_closed)
{
	ClusterReplacementEpisode episode;

	cluster_online_join = false;
	ut_join_setup();
	ut_in_quorum_value = true;
	cluster_enabled = true;
	episode = ut_admitted_replacement_episode(cluster_node_id);

	UT_ASSERT(cluster_reconfig_publish_replacement_member_closed(&episode));
	cluster_reconfig_lmon_tick();

	UT_ASSERT_EQ((int)cluster_membership_get_state(cluster_node_id),
				 (int)CLUSTER_MEMBER_MEMBER);
	UT_ASSERT_EQ((int)cluster_reconfig_self_join_gate_verdict(),
				 (int)CLUSTER_JOIN_GATE_BLOCK_53R60);
	UT_ASSERT_EQ(cluster_membership_get_last_admitted_incarnation(cluster_node_id),
				 UINT64_C(0));
}

/* With online join enabled, the ordinary cold-bootstrap classifier also runs
 * inside LMON.  It must not reinterpret a stored replacement ADMITTED episode
 * as a fresh v2 bootstrap and set the shared write byte. */
UT_TEST(test_ordinary_bootstrap_cannot_open_replacement_admitted_member)
{
	ClusterReplacementEpisode episode;

	cluster_online_join = true;
	ut_join_setup();
	ut_in_quorum_value = true;
	cluster_enabled = true;
	episode = ut_admitted_replacement_episode(cluster_node_id);

	UT_ASSERT(cluster_reconfig_publish_replacement_member_closed(&episode));
	cluster_reconfig_lmon_tick();

	UT_ASSERT_EQ((int)cluster_reconfig_self_join_gate_verdict(),
				 (int)CLUSTER_JOIN_GATE_BLOCK_53R60);
	UT_ASSERT_EQ((int)cluster_membership_get_state(cluster_node_id),
				 (int)CLUSTER_MEMBER_MEMBER);
	cluster_online_join = false;
}

/* A torn/corrupt but nonempty replacement mirror is not equivalent to the
 * canonical empty ordinary-join state.  Both ordinary bootstrap and the v2
 * marker callback must fail closed rather than treating validation failure as
 * permission to open. */
UT_TEST(test_nonempty_invalid_replacement_episode_blocks_ordinary_openers)
{
	ClusterReconfigState *state;

	cluster_online_join = true;
	ut_join_setup();
	ut_in_quorum_value = true;
	cluster_enabled = true;
	state = (ClusterReconfigState *)reconfig_shmem_storage;
	state->replacement_episode.request_nonce = UINT64_C(1); /* nonempty, invalid */
	state->self_join_admitted = 0;

	cluster_reconfig_lmon_tick();
	UT_ASSERT_EQ((int)cluster_reconfig_self_join_gate_verdict(),
				 (int)CLUSTER_JOIN_GATE_BLOCK_53R60);
	cluster_reconfig_note_self_admitted(UINT64_C(41));
	UT_ASSERT_EQ((int)cluster_reconfig_self_join_gate_verdict(),
				 (int)CLUSTER_JOIN_GATE_BLOCK_53R60);
	cluster_online_join = false;
}

/* Uniform OPEN releases the replacement gate only for the byte-identical
 * stored ADMITTED episode, its explicit generation token, and an already
 * published local MEMBER.  Wrong generation/identity/member state stays
 * closed; the exact call is retry-safe. */
UT_TEST(test_uniform_open_requires_exact_replacement_episode_and_generation)
{
	ClusterReplacementEpisode episode;
	ClusterReplacementEpisode wrong;

	cluster_online_join = false;
	ut_join_setup();
	episode = ut_admitted_replacement_episode(cluster_node_id);

	UT_ASSERT(!cluster_reconfig_open_replacement_admission(
		&episode, episode.state_generation));
	UT_ASSERT(cluster_reconfig_publish_replacement_member_closed(&episode));

	UT_ASSERT(!cluster_reconfig_open_replacement_admission(
		&episode, episode.state_generation + 1));
	wrong = episode;
	wrong.request_nonce++;
	UT_ASSERT(!cluster_reconfig_open_replacement_admission(
		&wrong, wrong.state_generation));

	cluster_membership_set_state(cluster_node_id, CLUSTER_MEMBER_JOINING);
	UT_ASSERT(!cluster_reconfig_open_replacement_admission(
		&episode, episode.state_generation));
	UT_ASSERT_EQ((int)cluster_reconfig_self_join_gate_verdict(),
				 (int)CLUSTER_JOIN_GATE_BLOCK_53R60);
	cluster_membership_set_state(cluster_node_id, CLUSTER_MEMBER_MEMBER);

	UT_ASSERT(cluster_reconfig_open_replacement_admission(
		&episode, episode.state_generation));
	UT_ASSERT_EQ((int)cluster_reconfig_self_join_gate_verdict(),
				 (int)CLUSTER_JOIN_GATE_ALLOW);
	UT_ASSERT(cluster_reconfig_open_replacement_admission(
		&episode, episode.state_generation));

	/* A delayed closed-MEMBER duplicate cannot re-close a completed OPEN. */
	UT_ASSERT(!cluster_reconfig_publish_replacement_member_closed(&episode));
	UT_ASSERT_EQ((int)cluster_reconfig_self_join_gate_verdict(),
				 (int)CLUSTER_JOIN_GATE_ALLOW);
}

/* ======================================================================
 * U18 (HF-1 / INV-J9) -- the publish-proof is true only when a MAJORITY of the
 * current MEMBER survivors have reached admitted_epoch: i.e. the coordinator's
 * JOIN_COMMITTED publish actually propagated.  A marker-durable-but-unpublished
 * state (survivors still behind) is NOT proven -> the joiner gate stays closed
 * (the half-publish window, P1-1).
 * ====================================================================== */
UT_TEST(test_reconfig_join_publish_proven_member_quorum)
{
	ut_join_setup();		   /* self = node 0, the joiner */
	ut_declared_set[1] = true; /* peers 1 and 2 are MEMBER survivors */
	ut_declared_set[2] = true;
	cluster_membership_set_state(1, CLUSTER_MEMBER_MEMBER);
	cluster_membership_set_state(2, CLUSTER_MEMBER_MEMBER);

	/* admitted_epoch 5; nobody advanced yet -> not proven (half-publish). */
	cluster_reconfig_record_observed_slot(1, 1, 1, 0);
	cluster_reconfig_record_observed_slot(2, 1, 1, 0);
	UT_ASSERT(!cluster_reconfig_join_publish_proven(5));

	/* one of two members reached it -> still below majority(2) -> not proven. */
	cluster_reconfig_record_observed_slot(1, 1, 1, 5);
	UT_ASSERT(!cluster_reconfig_join_publish_proven(5));

	/* both members reached it -> proven. */
	cluster_reconfig_record_observed_slot(2, 1, 1, 5);
	UT_ASSERT(cluster_reconfig_join_publish_proven(5));

	/* admitted_epoch 0 is never a real epoch -> fail-closed. */
	UT_ASSERT(!cluster_reconfig_join_publish_proven(0));
}

/* ======================================================================
 * U18b (HF-1) -- zero visible MEMBER survivor -> cannot prove -> fail-closed
 * (a peer at a high epoch that is NOT a member does not count).
 * ====================================================================== */
UT_TEST(test_reconfig_join_publish_proven_no_member_failclosed)
{
	ut_join_setup();
	ut_declared_set[1] = true;
	cluster_membership_set_state(1, CLUSTER_MEMBER_JOINING); /* not a member */
	cluster_reconfig_record_observed_slot(1, 1, 1, 9);
	UT_ASSERT(!cluster_reconfig_join_publish_proven(5));
}

/* ======================================================================
 * U19 (HF-2 / INV-J14) -- bootstrap is a POSITIVE epoch proof, not a timing
 * grace: quorum of declared CSSD-alive AND no peer past INITIAL.  A running
 * cluster (any peer past INITIAL) is NOT a bootstrap -> false (a slow rejoiner
 * stays fail-closed, P1-2).  Too few alive -> undecided -> false.
 * ====================================================================== */
UT_TEST(test_reconfig_bootstrap_quorum_epoch_proof)
{
	ut_join_setup();		   /* self = node 0 */
	ut_declared_set[1] = true; /* 3 declared nodes */
	ut_declared_set[2] = true;

	/* quorum of declared FRESH-ALIVE co-boot slots at INITIAL -> bootstrap proven
	 * (v1.3: durable voting-disk heartbeat freshness + valid slot, not live CSSD). */
	cluster_reconfig_record_observed_slot(1, 1, 1, 0);
	cluster_reconfig_record_observed_slot(2, 1, 1, 0);
	cluster_reconfig_record_observed_fresh_alive(1, true);
	cluster_reconfig_record_observed_fresh_alive(2, true);
	UT_ASSERT(cluster_reconfig_bootstrap_quorum_at_initial());

	/* a peer past INITIAL (running cluster) -> NOT a bootstrap (fail-closed). */
	cluster_reconfig_record_observed_slot(1, 1, 1, 4);
	UT_ASSERT(!cluster_reconfig_bootstrap_quorum_at_initial());

	/* no valid co-boot slot on either peer (generation 0) -> only self proven ->
	 * below quorum -> false (never latch on a default-0 placeholder). */
	cluster_reconfig_record_observed_slot(1, 0, 0, 0);
	cluster_reconfig_record_observed_slot(2, 0, 0, 0);
	UT_ASSERT(!cluster_reconfig_bootstrap_quorum_at_initial());
}

/* ======================================================================
 * U19b (RF-ROOT P9 verification / cold-formation ruling) — the ABSENT
 * founding admission reads the WHOLE-round QVOTEC bootstrap proof
 * (cluster_reconfig_bootstrap_proof_node) under the bootstrap publication
 * seqlock:
 *   - an ODD seq (writer in progress) is NEVER accepted, even when every
 *     datum is consistent — this is what makes the "new incarnation +
 *     stale fresh-alive" intermediate state (reachable only inside an open
 *     window) unable to form an admission proof;
 *   - a CHANGED seq between the getter's two reads is rejected by the
 *     same code path (the getter retries); the change can only be
 *     observed concurrently, so the unit test proves the window contract
 *     instead: data written between begin()/end() is visible only when
 *     seq is even AND unchanged;
 *   - a stable even-seq snapshot admits the target only when in-quorum +
 *     fresh co-boot peers at INITIAL + non-zero target incarnation all
 *     hold (every fail-closed require is covered below).
 * ====================================================================== */
UT_TEST(test_reconfig_bootstrap_proof_seqlock)
{
	ClusterReconfigState *state;
	uint64		inc = 0;

	ut_join_setup();			/* self = node 0, fresh shmem, seq 0 even */
	ut_declared_set[1] = true;
	ut_declared_set[2] = true;
	state = (ClusterReconfigState *) reconfig_shmem_storage;

	/* --- A. ODD seq (writer in progress) is never accepted. --- */
	cluster_reconfig_record_observed_slot(1, 7, 1, 0);
	cluster_reconfig_record_observed_slot(2, 7, 1, 0);
	cluster_reconfig_record_observed_fresh_alive(1, true);
	cluster_reconfig_record_observed_fresh_alive(2, true);
	pg_atomic_write_u64(&state->bootstrap_in_quorum, 1);
	pg_atomic_write_u64(&state->observed_bootstrap_seq, 3); /* odd */
	UT_ASSERT(!cluster_reconfig_bootstrap_proof_node(1, &inc));
	pg_atomic_write_u64(&state->observed_bootstrap_seq, 4); /* even */

	/* --- B. stable even snapshot + full quorum -> admitted with the
	 *        exact target incarnation. --- */
	UT_ASSERT(cluster_reconfig_bootstrap_proof_node(1, &inc));
	UT_ASSERT(inc == UINT64_C(7));

	/* --- C. the window contract: data written while the window is open
	 *        (odd seq) is never read as a proof.  Simulate the publisher
	 *        updating the incarnation for the NEXT round while the
	 *        fresh-alive bit still reflects the CURRENT round — the mixed
	 *        view is rejected while seq is odd.  Once the writer closes
	 *        the window (even seq), the mixed view would be a coherent
	 *        round in this simulation; in production the writer always
	 *        publishes fresh for the same round inside the same window,
	 *        so the mixed state cannot survive end(). --- */
	pg_atomic_write_u64(&state->observed_bootstrap_seq, 5); /* begin: odd */
	cluster_reconfig_record_observed_slot(1, 9, 2, 0);	   /* NEW incarnation */
	/* fresh_alive(1) is still true from the OLD round — the mixed view */
	UT_ASSERT(!cluster_reconfig_bootstrap_proof_node(1, &inc)); /* odd: reject */
	pg_atomic_write_u64(&state->observed_bootstrap_seq, 6); /* end: even */
	UT_ASSERT(cluster_reconfig_bootstrap_proof_node(1, &inc));
	UT_ASSERT(inc == UINT64_C(9));

	/* --- D. missing same-round in-quorum -> fail closed. --- */
	pg_atomic_write_u64(&state->bootstrap_in_quorum, 0);
	pg_atomic_write_u64(&state->observed_bootstrap_seq, 8);
	UT_ASSERT(!cluster_reconfig_bootstrap_proof_node(1, &inc));
	pg_atomic_write_u64(&state->bootstrap_in_quorum, 1);
	pg_atomic_write_u64(&state->observed_bootstrap_seq, 10);

	/* --- E. stale target (fresh=false) -> fail closed. --- */
	cluster_reconfig_record_observed_fresh_alive(1, false);
	pg_atomic_write_u64(&state->observed_bootstrap_seq, 12);
	UT_ASSERT(!cluster_reconfig_bootstrap_proof_node(1, &inc));
	cluster_reconfig_record_observed_fresh_alive(1, true);
	pg_atomic_write_u64(&state->observed_bootstrap_seq, 14);

	/* --- F. zero target incarnation -> fail closed. --- */
	cluster_reconfig_record_observed_slot(1, 0, 1, 0);
	pg_atomic_write_u64(&state->observed_bootstrap_seq, 16);
	UT_ASSERT(!cluster_reconfig_bootstrap_proof_node(1, &inc));
	cluster_reconfig_record_observed_slot(1, 7, 1, 0);
	pg_atomic_write_u64(&state->observed_bootstrap_seq, 18);

	/* --- G. any declared peer past INITIAL -> running cluster, NOT a
	 *        founding bootstrap -> fail closed (this is also the guard
	 *        that keeps a DEAD-then-restarted peer out of the founding
	 *        branch: its observed epoch is past INITIAL). --- */
	cluster_reconfig_record_observed_slot(2, 7, 1, 4);
	pg_atomic_write_u64(&state->observed_bootstrap_seq, 20);
	UT_ASSERT(!cluster_reconfig_bootstrap_proof_node(1, &inc));
	cluster_reconfig_record_observed_slot(2, 7, 1, 0);
	pg_atomic_write_u64(&state->observed_bootstrap_seq, 22);

	/* --- H. no valid co-boot slot for the TARGET (gen 0) while the other
	 *        peer is fresh: quorum holds but target_ok fails -> target 1
	 *        fail-closes; target 2 still admits from the same snapshot. --- */
	cluster_reconfig_record_observed_slot(1, 0, 0, 0);
	pg_atomic_write_u64(&state->observed_bootstrap_seq, 24);
	UT_ASSERT(!cluster_reconfig_bootstrap_proof_node(1, &inc));
	UT_ASSERT(cluster_reconfig_bootstrap_proof_node(2, &inc));
	UT_ASSERT(inc == UINT64_C(7));
}


/* ======================================================================
 * U20 (spec-5.15 Hardening v1.2 / INV-J14 self-join-gate race) -- the
 * cold-bootstrap proof must rest on a VALID durable co-boot slot
 * (cluster_reconfig_get_observed_slot true, generation > 0, observed_epoch
 * == INITIAL), NOT on live CSSD state and NOT on a default-0 placeholder.
 *
 * Root cause it guards: a founding survivor that has durable proof of co-
 * booting at INITIAL but whose peers' live CSSD is momentarily DOWN (IC /
 * heartbeat churn) was denied bootstrap by the v1.1 CSSD-quorum proof, so it
 * stayed UNDECIDED; a later UNRELATED node fail-stop then advanced the epoch
 * and reclassified this genuine member as a rejoiner -> 53R61 (refused its own
 * writes).  Anchoring the proof on the durable voting-disk slot lets the member
 * latch reliably during formation (immune to CSSD churn), closing the window.
 * ====================================================================== */
UT_TEST(test_reconfig_bootstrap_proof_valid_slot_not_cssd)
{
	/* --- A. FRESH-ALIVE co-boot slots at INITIAL but peers CSSD-DEAD -> still a
	 *        proven bootstrap (durable voting-disk heartbeat, not live CSSD).  v1.1
	 *        returned false here (the race window); v1.3 returns true because the
	 *        liveness is the voting-disk fresh-alive signal, immune to CSSD churn. --- */
	ut_join_setup();		   /* self = node 0 */
	ut_declared_set[1] = true; /* 3 declared nodes */
	ut_declared_set[2] = true;
	cluster_reconfig_record_observed_slot(1, 7, 1, 0);	   /* valid slot, INITIAL */
	cluster_reconfig_record_observed_slot(2, 7, 1, 0);	   /* valid slot, INITIAL */
	cluster_reconfig_record_observed_fresh_alive(1, true); /* voting-disk fresh */
	cluster_reconfig_record_observed_fresh_alive(2, true);
	ut_peer_state[1] = CLUSTER_CSSD_PEER_DEAD; /* live CSSD churned down */
	ut_peer_state[2] = CLUSTER_CSSD_PEER_DEAD;
	UT_ASSERT(cluster_reconfig_bootstrap_quorum_at_initial());

	/* --- B. CSSD-alive but NO valid slot (generation 0 placeholder) must
	 *        NOT prove bootstrap — never latch on a default-0 epoch. --- */
	ut_join_setup();
	ut_declared_set[1] = true;
	ut_declared_set[2] = true;
	ut_peer_state[1] = CLUSTER_CSSD_PEER_ALIVE;
	ut_peer_state[2] = CLUSTER_CSSD_PEER_ALIVE;
	/* no record_observed_slot -> generation 0 -> not a valid co-boot proof */
	UT_ASSERT(!cluster_reconfig_bootstrap_quorum_at_initial());

	/* --- C. a peer observed past INITIAL is a running cluster, never a
	 *        bootstrap (rejoiner fail-closed) — unchanged from v1.1. --- */
	ut_join_setup();
	ut_declared_set[1] = true;
	ut_declared_set[2] = true;
	cluster_reconfig_record_observed_slot(1, 7, 1, 0); /* valid, INITIAL */
	cluster_reconfig_record_observed_slot(2, 7, 1, 5); /* valid, past INITIAL */
	UT_ASSERT(!cluster_reconfig_bootstrap_quorum_at_initial());
}


/* ======================================================================
 * U21 (spec-5.15 Hardening v1.3 / INV-J14 stale-slot fail-open) -- a valid
 * generation > 0 slot at epoch INITIAL is NOT proof of co-booting: it may be a
 * CRASHED peer's stale leftover (decide_quorum_view's P2.1 freshness gate marks
 * it not-fresh).  The cold-bootstrap proof must additionally require the per-node
 * FRESH-ALIVE signal (durable voting-disk heartbeat), not slot existence alone —
 * else a node with self + a stale peer slot fail-opens (latches BOOTSTRAP without
 * a live co-boot quorum).  v1.2 counted such a stale slot (the regression this
 * guards); v1.3 fail-closes on it.
 * ====================================================================== */
UT_TEST(test_reconfig_bootstrap_proof_stale_slot_failclosed)
{
	/* --- A. valid slots at INITIAL but STALE heartbeat (fresh_alive=false) must
	 *        NOT count -> only self proven -> below quorum -> false.  v1.2 (no
	 *        freshness) counted them = fail-open; v1.3 fail-closes. --- */
	ut_join_setup();		   /* self = node 0 */
	ut_declared_set[1] = true; /* 3 declared nodes */
	ut_declared_set[2] = true;
	cluster_reconfig_record_observed_slot(1, 7, 1, 0);		/* valid slot, INITIAL */
	cluster_reconfig_record_observed_slot(2, 7, 1, 0);		/* valid slot, INITIAL */
	cluster_reconfig_record_observed_fresh_alive(1, false); /* crashed peer: stale hb */
	cluster_reconfig_record_observed_fresh_alive(2, false);
	UT_ASSERT(!cluster_reconfig_bootstrap_quorum_at_initial());

	/* --- B. the same slots but FRESH-ALIVE -> genuine co-boot -> proven. --- */
	cluster_reconfig_record_observed_fresh_alive(1, true);
	cluster_reconfig_record_observed_fresh_alive(2, true);
	UT_ASSERT(cluster_reconfig_bootstrap_quorum_at_initial());

	/* --- C. one fresh + one stale -> self + the one fresh = quorum (3-node) ->
	 *        proven; the stale peer simply does not contribute (no over-reject). --- */
	cluster_reconfig_record_observed_fresh_alive(2, false);
	UT_ASSERT(cluster_reconfig_bootstrap_quorum_at_initial());
}

static bool
ut_join_qvotec_poll_write_pending(int32 *target_node_out,
								  void *write_slot512_out)
{
	ClusterJoinMarkerMailboxOperationV1 operation;
	bool pending;

	pending = cluster_reconfig_join_qvotec_poll_pending(
		&operation, target_node_out, write_slot512_out);
	return pending && operation == CLUSTER_JOIN_MARKER_MAILBOX_WRITE_EXACT;
}

static void
ut_join_qvotec_complete_write(bool acked)
{
	cluster_reconfig_join_qvotec_complete(
		CLUSTER_JOIN_MARKER_MAILBOX_WRITE_EXACT, acked, NULL);
}

static void
ut_stage_join_commit_with_prior_excluded(bool owner_gate)
{
	ClusterReconfigState *state;
	ReconfigEvent applied;
	int32 target = -1;
	uint8 slot[CLUSTER_VOTING_SLOT_BYTES];

	cluster_online_join = true;
	ut_join_setup();
	epoch_init_done = false;
	cluster_epoch_shmem_init();
	state = (ClusterReconfigState *)reconfig_shmem_storage;
	state->self_join_admitted = 1;
	cluster_node_id = 0;
	ut_in_quorum_value = true;
	ut_declared_set[0] = true;
	ut_declared_set[1] = true;
	cluster_membership_set_state(0, CLUSTER_MEMBER_MEMBER);
	cluster_membership_set_state(1, CLUSTER_MEMBER_JOINING);
	cluster_membership_record_admitted(1, UINT64_C(70));
	state->pending_join_bitmap[0] = UINT8_C(0x02);
	cluster_reconfig_record_observed_slot(1, UINT64_C(77), UINT64_C(1), 0);
	ut_dead_generation = UINT64_C(9);

	memset(&applied, 0, sizeof(applied));
	applied.event_id = UINT64_C(501);
	applied.coordinator_node_id = 0;
	applied.new_epoch = cluster_epoch_get_current();
	applied.dead_bitmap[0] = UINT8_C(0x06); /* joining node 1 + unrelated node 2 */
	applied.cssd_dead_generation = UINT64_C(9);
	applied.reconfig_kind = RECONFIG_KIND_FAIL_STOP;
	cluster_reconfig_publish_event(&applied);

	ut_owner_rejoin_result = owner_gate;
	cluster_write_fence_enforcement = CLUSTER_WRITE_FENCE_ENFORCE_ON;
	MyBackendType = B_LMON;
	ut_fence_async_submit_ok = true;
	ut_fence_async_poll_pr = CLUSTER_MARKER_POLL_PENDING;
	ut_fence_async_poll_result = CLUSTER_FENCE_MARKER_SUBMIT_FAILED;
	UT_ASSERT(!cluster_reconfig_commit_member(1, UINT64_C(77)));
	memset(slot, 0, sizeof(slot));
	UT_ASSERT(ut_join_qvotec_poll_write_pending(&target, slot));
	UT_ASSERT_EQ(target, 1);
	ut_join_qvotec_complete_write(true);
}

UT_TEST(test_join_commit_clears_only_owner_after_root_rejoin_gate)
{
	ReconfigEvent applied;

	ut_stage_join_commit_with_prior_excluded(true);
	cluster_reconfig_lmon_tick();
	UT_ASSERT_EQ(ut_owner_rejoin_calls, 1);
	UT_ASSERT_EQ(ut_owner_rejoin_node, 1);
	UT_ASSERT_EQ(ut_owner_rejoin_incarnation, UINT64_C(77));
	UT_ASSERT(ut_fence_async_marker_captured);
	UT_ASSERT(cluster_fence_marker_valid_v1(&ut_fence_async_marker));
	UT_ASSERT_EQ((int)ut_fence_async_marker.marker_kind,
				 (int)CLUSTER_FENCE_MARKER_KIND_BASELINE);
	UT_ASSERT_EQ(ut_fence_async_marker.fenced_dead_bitmap[0], UINT8_C(0x04));
	cluster_reconfig_get_last_event(&applied);
	UT_ASSERT_EQ(applied.event_id, UINT64_C(501));
	UT_ASSERT_EQ((int)cluster_membership_get_state(1),
				 (int)CLUSTER_MEMBER_JOINING);

	ut_fence_async_poll_pr = CLUSTER_MARKER_POLL_ACKED;
	ut_fence_async_poll_result = CLUSTER_FENCE_MARKER_SUBMIT_ACK;
	cluster_reconfig_lmon_tick();
	cluster_reconfig_get_last_event(&applied);
	UT_ASSERT_EQ((int)applied.reconfig_kind, (int)RECONFIG_KIND_JOIN_COMMITTED);
	UT_ASSERT_EQ(ut_fence_async_marker.fence_epoch, applied.new_epoch);
	UT_ASSERT_EQ(ut_fence_async_marker.fence_event_id, applied.event_id);
	UT_ASSERT_EQ(ut_fence_async_marker.fence_generation,
				 applied.cssd_dead_generation);
	UT_ASSERT_EQ(ut_fence_async_marker.issuer_node_id,
				 applied.coordinator_node_id);
	UT_ASSERT((applied.dead_bitmap[0] & UINT8_C(0x02)) == 0);
	UT_ASSERT((applied.dead_bitmap[0] & UINT8_C(0x04)) != 0);
	MyBackendType = B_INVALID;
	cluster_write_fence_enforcement = CLUSTER_WRITE_FENCE_ENFORCE_OFF;
	cluster_online_join = false;
}

UT_TEST(test_join_commit_root_gate_failure_keeps_owner_excluded)
{
	ReconfigEvent applied;

	ut_stage_join_commit_with_prior_excluded(false);
	cluster_reconfig_lmon_tick();
	UT_ASSERT_EQ(ut_owner_rejoin_calls, 1);
	cluster_reconfig_get_last_event(&applied);
	UT_ASSERT_EQ(applied.event_id, UINT64_C(501));
	UT_ASSERT((applied.dead_bitmap[0] & UINT8_C(0x02)) != 0);
	UT_ASSERT_EQ((int)cluster_membership_get_state(1),
				 (int)CLUSTER_MEMBER_JOINING);
	MyBackendType = B_INVALID;
	cluster_write_fence_enforcement = CLUSTER_WRITE_FENCE_ENFORCE_OFF;
	cluster_online_join = false;
}

UT_TEST(test_join_commit_fence_ack_failure_keeps_owner_excluded)
{
	ReconfigEvent applied;

	ut_stage_join_commit_with_prior_excluded(true);
	cluster_reconfig_lmon_tick();
	UT_ASSERT(ut_fence_async_marker_captured);
	ut_fence_async_poll_pr = CLUSTER_MARKER_POLL_ACKED;
	ut_fence_async_poll_result = CLUSTER_FENCE_MARKER_SUBMIT_FAILED;
	cluster_reconfig_lmon_tick();
	cluster_reconfig_get_last_event(&applied);
	UT_ASSERT_EQ(applied.event_id, UINT64_C(501));
	UT_ASSERT_EQ(applied.dead_bitmap[0], UINT8_C(0x06));
	UT_ASSERT_EQ((int)cluster_membership_get_state(1),
				 (int)CLUSTER_MEMBER_JOINING);
	MyBackendType = B_INVALID;
	cluster_write_fence_enforcement = CLUSTER_WRITE_FENCE_ENFORCE_OFF;
	cluster_online_join = false;
}

UT_TEST(test_join_pending_preserves_full_prior_excluded_set)
{
	ClusterReconfigState *state;
	ReconfigEvent applied;
	uint8 join_bitmap[CLUSTER_RECONFIG_DEAD_BITMAP_BYTES] = { 0 };
	uint64 incarnations[CLUSTER_MAX_NODES] = { 0 };
	uint8 slot[CLUSTER_VOTING_SLOT_BYTES];
	int32 target = -1;

	cluster_online_join = true;
	ut_join_setup();
	epoch_init_done = false;
	cluster_epoch_shmem_init();
	state = (ClusterReconfigState *)reconfig_shmem_storage;
	state->self_join_admitted = 1;
	cluster_node_id = 0;
	ut_in_quorum_value = true;
	ut_declared_set[0] = true;
	ut_declared_set[1] = true;
	cluster_membership_set_state(0, CLUSTER_MEMBER_MEMBER);
	cluster_membership_set_state(1, CLUSTER_MEMBER_DEAD);

	memset(&applied, 0, sizeof(applied));
	applied.event_id = UINT64_C(551);
	applied.coordinator_node_id = 0;
	applied.new_epoch = cluster_epoch_get_current();
	applied.dead_bitmap[0] = UINT8_C(0x06); /* joiner 1 + unrelated origin 2 */
	applied.cssd_dead_generation = UINT64_C(9);
	applied.reconfig_kind = RECONFIG_KIND_FAIL_STOP;
	cluster_reconfig_publish_event(&applied);

	join_bitmap[0] = UINT8_C(0x02);
	incarnations[1] = UINT64_C(77);
	cluster_reconfig_apply_join_as_coordinator(join_bitmap, 0, incarnations);
	memset(slot, 0, sizeof(slot));
	UT_ASSERT(ut_join_qvotec_poll_write_pending(&target, slot));
	UT_ASSERT_EQ(target, 1);
	ut_join_qvotec_complete_write(true);
	cluster_reconfig_lmon_tick();

	cluster_reconfig_get_last_event(&applied);
	UT_ASSERT_EQ((int)applied.reconfig_kind, (int)RECONFIG_KIND_JOIN_PENDING);
	UT_ASSERT_EQ(applied.dead_bitmap[0], UINT8_C(0x06));
	UT_ASSERT_EQ(ut_owner_rejoin_calls, 0);
	cluster_online_join = false;
}

static void
ut_stage_survivor_join_observation(bool owner_gate)
{
	ClusterReconfigState *state;
	ReconfigEvent applied;

	cluster_online_join = true;
	ut_join_setup();
	epoch_init_done = false;
	cluster_epoch_shmem_init();
	state = (ClusterReconfigState *)reconfig_shmem_storage;
	state->self_join_admitted = 1;
	cluster_node_id = 1;
	ut_in_quorum_value = true;
	ut_declared_set[0] = true;
	ut_declared_set[1] = true;
	ut_declared_set[2] = true;
	ut_peer_state[0] = CLUSTER_CSSD_PEER_ALIVE;
	ut_peer_state[2] = CLUSTER_CSSD_PEER_ALIVE;
	cluster_membership_set_state(0, CLUSTER_MEMBER_MEMBER);
	cluster_membership_set_state(1, CLUSTER_MEMBER_MEMBER);
	cluster_membership_set_state(2, CLUSTER_MEMBER_DEAD);
	cluster_membership_record_admitted(2, UINT64_C(70));
	cluster_reconfig_record_observed_committed_join(2, UINT64_C(77), UINT64_C(1));

	memset(&applied, 0, sizeof(applied));
	applied.event_id = UINT64_C(601);
	applied.coordinator_node_id = 0;
	applied.new_epoch = cluster_epoch_get_current();
	applied.dead_bitmap[0] = UINT8_C(0x0c); /* joining node 2 + unrelated node 3 */
	applied.cssd_dead_generation = UINT64_C(10);
	applied.reconfig_kind = RECONFIG_KIND_FAIL_STOP;
	cluster_reconfig_publish_event(&applied);
	ut_owner_rejoin_result = owner_gate;
}

UT_TEST(test_survivor_join_observation_requires_root_rejoin_gate)
{
	ReconfigEvent applied;

	ut_stage_survivor_join_observation(false);
	cluster_reconfig_lmon_tick();
	UT_ASSERT_EQ(ut_owner_rejoin_calls, 1);
	UT_ASSERT_EQ(ut_owner_rejoin_node, 2);
	UT_ASSERT_EQ(ut_owner_rejoin_incarnation, UINT64_C(77));
	UT_ASSERT_EQ((int)cluster_membership_get_state(2),
				 (int)CLUSTER_MEMBER_DEAD);
	cluster_reconfig_get_last_event(&applied);
	UT_ASSERT_EQ(applied.event_id, UINT64_C(601));
	UT_ASSERT_EQ(applied.dead_bitmap[0], UINT8_C(0x0c));
	cluster_online_join = false;
}

UT_TEST(test_survivor_join_observation_clears_only_gated_origin)
{
	ReconfigEvent applied;

	ut_stage_survivor_join_observation(true);
	cluster_reconfig_lmon_tick();
	UT_ASSERT_EQ(ut_owner_rejoin_calls, 1);
	UT_ASSERT_EQ((int)cluster_membership_get_state(2),
				 (int)CLUSTER_MEMBER_MEMBER);
	cluster_reconfig_get_last_event(&applied);
	UT_ASSERT_EQ((int)applied.reconfig_kind,
				 (int)RECONFIG_KIND_JOIN_COMMITTED);
	UT_ASSERT((applied.dead_bitmap[0] & UINT8_C(0x04)) == 0);
	UT_ASSERT((applied.dead_bitmap[0] & UINT8_C(0x08)) != 0);
	cluster_online_join = false;
}

UT_TEST(test_survivor_join_observation_waits_for_serving_ready)
{
	ReconfigEvent applied;

	ut_stage_survivor_join_observation(true);
	ut_authority_managed = true;
	ut_serving_ready = false;
	cluster_reconfig_lmon_tick();
	UT_ASSERT_EQ(ut_owner_rejoin_calls, 0);
	UT_ASSERT_EQ((int)cluster_membership_get_state(2),
				 (int)CLUSTER_MEMBER_DEAD);
	cluster_reconfig_get_last_event(&applied);
	UT_ASSERT_EQ(applied.event_id, UINT64_C(601));

	ut_serving_ready = true;
	cluster_reconfig_lmon_tick();
	UT_ASSERT_EQ(ut_owner_rejoin_calls, 1);
	UT_ASSERT_EQ((int)cluster_membership_get_state(2),
				 (int)CLUSTER_MEMBER_MEMBER);
	cluster_online_join = false;
}

/* The region-3 mailbox is one raw 96-byte payload carrier.  V2 retains its
 * exact native 64-byte image; v3 is codec-produced and exact; qvotec sees only
 * one zero-padded 512-byte slot.  A v3 value the codec rejects must not publish
 * a request sequence or overwrite the last completed payload. */
UT_TEST(test_reconfig_region3_mailbox_preserves_v2_and_canonical_v3)
{
	ClusterJoinCommitMarker v2;
	ClusterReplacementCommitMarkerV3 v3;
	ClusterMarkerAsync async;
	ClusterReconfigState *state;
	uint8 expected_v3[CLUSTER_JCMK_REPLACEMENT_BYTES];
	uint8 slot[CLUSTER_VOTING_SLOT_BYTES];
	uint64 seq_before;
	int32 target = -1;
	int i;

	ut_join_setup();
	state = (ClusterReconfigState *)reconfig_shmem_storage;

	memset(&v2, 0, sizeof(v2));
	v2.magic = CLUSTER_JCMK_MAGIC;
	v2.version = CLUSTER_JCMK_VERSION;
	v2.node_id = 2;
	v2.phase = CLUSTER_JCMK_PHASE_PREPARE;
	v2.generation = UINT64_C(11);
	v2.admitted_incarnation = UINT64_C(22);
	v2.admitted_epoch = UINT64_C(33);
	v2.commit_nonce = UINT64_C(44);
	cluster_join_marker_compute_crc(&v2);
	cluster_marker_async_init(&async);
	UT_ASSERT(cluster_reconfig_submit_join_marker_async(
		&async, 2, &v2, CLUSTER_MARKER_KIND_JOIN_PREPARE,
		(TimestampTz)UINT64_C(1000)));
	memset(slot, 0xa5, sizeof(slot));
	UT_ASSERT(ut_join_qvotec_poll_write_pending(&target, slot));
	UT_ASSERT_EQ(target, 2);
	UT_ASSERT_EQ(memcmp(slot, &v2, sizeof(v2)), 0);
	for (i = sizeof(v2); i < CLUSTER_VOTING_SLOT_BYTES; i++)
		UT_ASSERT_EQ(slot[i], 0);
	ut_join_qvotec_complete_write(true);

	memset(&v3, 0, sizeof(v3));
	v3.magic = CLUSTER_JCMK_MAGIC;
	v3.version = CLUSTER_JCMK_REPLACEMENT_VERSION;
	v3.target_node_id = 3;
	v3.phase = CLUSTER_JCMK_REPLACEMENT_PHASE_ADMITTED;
	v3.generation = UINT64_C(0x0102030405060708);
	v3.old_admitted_incarnation = UINT64_C(0x1112131415161718);
	v3.fresh_incarnation = UINT64_C(0x2122232425262728);
	v3.baseline_epoch = UINT64_C(0x3132333435363738);
	v3.reserved_or_committed_epoch = UINT64_C(0x4142434445464748);
	v3.request_nonce = UINT64_C(0x5152535455565758);
	for (i = 0; i < 16; i++)
		v3.expected_purge_survivors[i] = (uint8)(0x80 + i);
	v3.grammar_fingerprint = UINT64_C(0x6162636465666768);
	v3.ready_state_generation = UINT32_C(0x71727374);
	UT_ASSERT(cluster_replacement_marker_v3_encode(&v3, expected_v3));

	cluster_marker_async_init(&async);
	UT_ASSERT(cluster_reconfig_submit_replacement_marker_v3_async(
		&async, 3, &v3, CLUSTER_MARKER_KIND_JOIN_COMMITTED,
		(TimestampTz)UINT64_C(2000)));
	memset(slot, 0xa5, sizeof(slot));
	target = -1;
	UT_ASSERT(ut_join_qvotec_poll_write_pending(&target, slot));
	UT_ASSERT_EQ(target, 3);
	UT_ASSERT_EQ(memcmp(slot, expected_v3, sizeof(expected_v3)), 0);
	for (i = CLUSTER_JCMK_REPLACEMENT_BYTES;
		 i < CLUSTER_VOTING_SLOT_BYTES; i++)
		UT_ASSERT_EQ(slot[i], 0);
	ut_join_qvotec_complete_write(true);

	/* Codec-invalid v3 and a target/payload mismatch are zero-publication. */
	seq_before = pg_atomic_read_u64(&state->join_marker_request_seq);
	v3.reserved0[0] = 1;
	cluster_marker_async_init(&async);
	UT_ASSERT(!cluster_reconfig_submit_replacement_marker_v3_async(
		&async, 3, &v3, CLUSTER_MARKER_KIND_JOIN_COMMITTED,
		(TimestampTz)UINT64_C(3000)));
	UT_ASSERT(!cluster_marker_async_is_submitted(&async));
	UT_ASSERT_EQ(pg_atomic_read_u64(&state->join_marker_request_seq), seq_before);
	UT_ASSERT(!ut_join_qvotec_poll_write_pending(&target, slot));
	v3.reserved0[0] = 0;
	UT_ASSERT(!cluster_reconfig_submit_replacement_marker_v3_async(
		&async, 4, &v3, CLUSTER_MARKER_KIND_JOIN_COMMITTED,
		(TimestampTz)UINT64_C(3001)));
	UT_ASSERT(!cluster_marker_async_is_submitted(&async));
	UT_ASSERT_EQ(pg_atomic_read_u64(&state->join_marker_request_seq), seq_before);
	UT_ASSERT(!ut_join_qvotec_poll_write_pending(&target, slot));
}

/* The existing four-byte target slot is the exact operation/target request
 * word, and the existing 96-byte payload is a duplex VERIFY
 * result.  VERIFY returns no caller image to QVOTEC, while successful
 * completion publishes the canonical majority-selected COMMITTED_CLOSED image.
 * An operation mismatch must fail and clear the duplex payload. */
UT_TEST(test_reconfig_region3_mailbox_request_word_is_exact_duplex)
{
	ClusterMarkerAsync async;
	ClusterReconfigState *state;
	ClusterReplacementCommitMarkerV3 marker;
	ClusterJoinMarkerMailboxOperationV1 operation;
	uint8 image[CLUSTER_JCMK_REPLACEMENT_BYTES];
	uint8 slot[CLUSTER_VOTING_SLOT_BYTES];
	int32 target;
	int i;

	ut_join_setup();
	state = (ClusterReconfigState *)reconfig_shmem_storage;
	UT_ASSERT_EQ(sizeof(ClusterReconfigState), 12640);
	UT_ASSERT_EQ(CLUSTER_JOIN_MARKER_REQUEST_TARGET_MASK,
				 UINT32_C(0x0000007f));
	UT_ASSERT_EQ(CLUSTER_JOIN_MARKER_REQUEST_RESERVED_MASK,
				 UINT32_C(0x7fffff80));
	UT_ASSERT_EQ(CLUSTER_JOIN_MARKER_REQUEST_VERIFY_COMMITTED_CLOSED,
				 UINT32_C(0x80000000));

	memset(&marker, 0, sizeof(marker));
	marker.magic = CLUSTER_JCMK_MAGIC;
	marker.version = CLUSTER_JCMK_REPLACEMENT_VERSION;
	marker.target_node_id = 5;
	marker.phase = CLUSTER_JCMK_REPLACEMENT_PHASE_COMMITTED_CLOSED;
	marker.generation = UINT64_C(0x0102030405060708);
	marker.old_admitted_incarnation = UINT64_C(0x1112131415161718);
	marker.fresh_incarnation = UINT64_C(0x2122232425262728);
	marker.baseline_epoch = UINT64_C(0x3132333435363738);
	marker.reserved_or_committed_epoch = UINT64_C(0x4142434445464748);
	marker.request_nonce = UINT64_C(0x5152535455565758);
	marker.expected_purge_survivors[0] = UINT8_C(0x06);
	marker.grammar_fingerprint = UINT64_C(0x6162636465666768);
	UT_ASSERT(cluster_replacement_marker_v3_encode(&marker, image));

	cluster_marker_async_init(&async);
	UT_ASSERT(cluster_reconfig_verify_replacement_committed_closed_async(
		&async, marker.target_node_id, (TimestampTz)UINT64_C(4000)));
	UT_ASSERT_EQ(state->join_marker_request_word,
				 CLUSTER_JOIN_MARKER_REQUEST_VERIFY_COMMITTED_CLOSED
				 | (uint32)marker.target_node_id);
	for (i = 0; i < CLUSTER_JCMK_REPLACEMENT_BYTES; i++)
		UT_ASSERT_EQ(state->join_pending_marker[i], 0);

	memset(slot, 0xa5, sizeof(slot));
	operation = CLUSTER_JOIN_MARKER_MAILBOX_WRITE_EXACT;
	target = -1;
	UT_ASSERT(cluster_reconfig_join_qvotec_poll_pending(
		&operation, &target, slot));
	UT_ASSERT_EQ((int)operation,
				 (int)CLUSTER_JOIN_MARKER_MAILBOX_VERIFY_COMMITTED_CLOSED);
	UT_ASSERT_EQ(target, marker.target_node_id);
	for (i = 0; i < CLUSTER_VOTING_SLOT_BYTES; i++)
		UT_ASSERT_EQ(slot[i], 0);
	cluster_reconfig_join_qvotec_complete(operation, true, image);
	UT_ASSERT_EQ(pg_atomic_read_u32(&state->join_marker_result),
				 CLUSTER_JOIN_MARKER_SUBMIT_ACK);
	UT_ASSERT_EQ(pg_atomic_read_u64(&state->join_marker_completion_seq),
				 async.inflight_seq);
	UT_ASSERT_EQ(memcmp(state->join_pending_marker, image, sizeof(image)), 0);

	/* The next VERIFY deliberately completes through the WRITE branch. */
	cluster_marker_async_init(&async);
	UT_ASSERT(cluster_reconfig_verify_replacement_committed_closed_async(
		&async, marker.target_node_id, (TimestampTz)UINT64_C(5000)));
	memset(slot, 0xa5, sizeof(slot));
	UT_ASSERT(cluster_reconfig_join_qvotec_poll_pending(
		&operation, &target, slot));
	cluster_reconfig_join_qvotec_complete(
		CLUSTER_JOIN_MARKER_MAILBOX_WRITE_EXACT, true, NULL);
	UT_ASSERT_EQ(pg_atomic_read_u32(&state->join_marker_result),
				 CLUSTER_JOIN_MARKER_SUBMIT_FAILED);
	for (i = 0; i < CLUSTER_JCMK_REPLACEMENT_BYTES; i++)
		UT_ASSERT_EQ(state->join_pending_marker[i], 0);
}

UT_TEST(test_reconfig_qvotec_lifecycle_double_invalidates_mailboxes)
{
	ClusterReconfigState *state;
	ClusterQvotecMailbox authority_mailbox;
	ClusterJoinMarkerMailboxOperationV1 operation;
	pg_atomic_uint32 qvotec_status;
	uint8 authority_zero[sizeof(authority_mailbox)];
	uint8 payload_before[CLUSTER_JCMK_REPLACEMENT_BYTES];
	uint8 slot[CLUSTER_VOTING_SLOT_BYTES];
	int32 target;

	ut_join_setup();
	state = (ClusterReconfigState *)reconfig_shmem_storage;
	memset(&authority_mailbox, 0xa5, sizeof(authority_mailbox));
	memset(authority_zero, 0, sizeof(authority_zero));
	pg_atomic_init_u32(&qvotec_status, CLUSTER_QVOTEC_READY);
	state->join_marker_request_word = UINT32_C(5);
	memset(state->join_pending_marker, 0x5a,
		   sizeof(state->join_pending_marker));
	memcpy(payload_before, state->join_pending_marker,
		   sizeof(payload_before));
	pg_atomic_write_u64(&state->join_marker_request_seq, UINT64_C(701));
	pg_atomic_write_u64(&state->join_marker_completion_seq, UINT64_C(701));
	pg_atomic_write_u32(&state->join_marker_result,
						CLUSTER_JOIN_MARKER_SUBMIT_ACK);

	/* Poll transfers the persistent request to the old QVOTEC process. */
	UT_ASSERT(cluster_reconfig_join_qvotec_poll_pending(
		&operation, &target, slot));
	UT_ASSERT_EQ((int)operation,
				 (int)CLUSTER_JOIN_MARKER_MAILBOX_WRITE_EXACT);
	UT_ASSERT_EQ(target, 5);

	UT_ASSERT(cluster_reconfig_qvotec_lifecycle_transition(
		&authority_mailbox, &qvotec_status,
		CLUSTER_QVOTEC_SHUTTING_DOWN));
	UT_ASSERT_EQ(pg_atomic_read_u32(&qvotec_status),
				 CLUSTER_QVOTEC_SHUTTING_DOWN);
	UT_ASSERT_EQ(memcmp(&authority_mailbox, authority_zero,
					 sizeof(authority_mailbox)), 0);
	UT_ASSERT_EQ(pg_atomic_read_u64(&state->join_marker_request_seq),
				 UINT64_C(701));
	UT_ASSERT_EQ(pg_atomic_read_u64(&state->join_marker_completion_seq),
				 UINT64_C(0));
	UT_ASSERT_EQ(pg_atomic_read_u32(&state->join_marker_result),
				 CLUSTER_JOIN_MARKER_SUBMIT_FAILED);
	UT_ASSERT_EQ(state->join_marker_request_word, UINT32_C(5));
	UT_ASSERT_EQ(memcmp(state->join_pending_marker, payload_before,
					 sizeof(payload_before)), 0);

	/* The successor repeats the invalidation before publishing a latch. */
	memset(&authority_mailbox, 0xa5, sizeof(authority_mailbox));
	pg_atomic_write_u64(&state->join_marker_completion_seq, UINT64_C(701));
	pg_atomic_write_u32(&state->join_marker_result,
						CLUSTER_JOIN_MARKER_SUBMIT_ACK);
	UT_ASSERT(cluster_reconfig_qvotec_lifecycle_transition(
		&authority_mailbox, &qvotec_status, CLUSTER_QVOTEC_STARTING));
	UT_ASSERT_EQ(pg_atomic_read_u32(&qvotec_status),
				 CLUSTER_QVOTEC_STARTING);
	UT_ASSERT_EQ(memcmp(&authority_mailbox, authority_zero,
					 sizeof(authority_mailbox)), 0);
	UT_ASSERT_EQ(pg_atomic_read_u64(&state->join_marker_completion_seq),
				 UINT64_C(0));

	UT_ASSERT(cluster_reconfig_qvotec_lifecycle_transition(
		&authority_mailbox, &qvotec_status, CLUSTER_QVOTEC_READY));
	UT_ASSERT_EQ(pg_atomic_read_u32(&qvotec_status), CLUSTER_QVOTEC_READY);
	UT_ASSERT(cluster_reconfig_join_qvotec_poll_pending(
		&operation, &target, slot));
	UT_ASSERT_EQ(target, 5);
	UT_ASSERT_EQ(memcmp(slot, payload_before, sizeof(payload_before)), 0);
	cluster_reconfig_join_qvotec_complete(operation, false, NULL);
}

UT_TEST(test_reconfig_target_refuses_ready_without_startup_closure_proof)
{
	ClusterReconfigState *state;
	ClusterReplacementEpisode episode;
	ClusterReplacementCommitMarkerV3 marker;
	ClusterR4PrerequisiteSnapshot expected;
	ClusterR4PrerequisiteSnapshot observed;
	uint8 marker_image[CLUSTER_JCMK_REPLACEMENT_BYTES];

	ut_join_setup();
	cluster_node_id = 3;
	state = (ClusterReconfigState *)reconfig_shmem_storage;
	episode = ut_admitted_replacement_episode(cluster_node_id);
	episode.phase = CLUSTER_REPLACEMENT_EPISODE_COMMITTED_CLOSED;
	episode.state_generation = UINT32_C(17);
	episode.readiness_flags
		= CLUSTER_REPLACEMENT_EPISODE_GRD_POSTEPOCH_READY
		  | CLUSTER_REPLACEMENT_EPISODE_INTENT_CLEARED;
	state->replacement_episode = episode;
	state->self_join_admitted = 0;
	cluster_membership_set_state(1, CLUSTER_MEMBER_MEMBER);
	cluster_membership_set_state(2, CLUSTER_MEMBER_MEMBER);
	epoch_init_done = false;
	cluster_epoch_shmem_init();
	UT_ASSERT(cluster_epoch_observe_remote(
		episode.reserved_or_committed_epoch));

	memset(&marker, 0, sizeof(marker));
	marker.magic = CLUSTER_JCMK_MAGIC;
	marker.version = CLUSTER_JCMK_REPLACEMENT_VERSION;
	marker.target_node_id = episode.target_node_id;
	marker.phase = CLUSTER_JCMK_REPLACEMENT_PHASE_COMMITTED_CLOSED;
	marker.generation = UINT64_C(41);
	marker.old_admitted_incarnation = episode.old_admitted_incarnation;
	marker.fresh_incarnation = episode.fresh_incarnation;
	marker.baseline_epoch = episode.baseline_epoch;
	marker.reserved_or_committed_epoch
		= episode.reserved_or_committed_epoch;
	marker.request_nonce = episode.request_nonce;
	memcpy(marker.expected_purge_survivors, episode.expected_survivors,
		   sizeof(marker.expected_purge_survivors));
	marker.grammar_fingerprint = episode.grammar_fingerprint;
	UT_ASSERT(cluster_replacement_marker_v3_encode(&marker, marker_image));
	memcpy(state->join_pending_marker, marker_image, sizeof(marker_image));
	pg_atomic_write_u64(&state->join_marker_request_seq, UINT64_C(900));
	pg_atomic_write_u64(&state->join_marker_completion_seq, UINT64_C(900));
	pg_atomic_write_u32(&state->join_marker_result,
						CLUSTER_JOIN_MARKER_SUBMIT_ACK);

	memset(&expected, 0, sizeof(expected));
	expected.status = CLUSTER_R4_PREREQUISITE_R4A_READY;
	expected.ready = true;
	expected.target_node_id = episode.target_node_id;
	expected.episode_state_generation = UINT32_C(18);
	expected.jcmk_generation = marker.generation;
	expected.request_nonce = episode.request_nonce;
	expected.old_admitted_incarnation = episode.old_admitted_incarnation;
	expected.fresh_incarnation = episode.fresh_incarnation;
	expected.committed_epoch = episode.reserved_or_committed_epoch;
	expected.grammar_fingerprint = episode.grammar_fingerprint;

	UT_ASSERT(!cluster_reconfig_r4_publish_ready(&expected));
	UT_ASSERT_EQ(state->replacement_episode.state_generation,
				 episode.state_generation);
	UT_ASSERT_EQ(state->replacement_episode.readiness_flags,
				 episode.readiness_flags);
	observed = cluster_reconfig_r4_prerequisite_snapshot();
	UT_ASSERT_EQ(observed.status, CLUSTER_R4_PREREQUISITE_RF_DEFERRED);
	UT_ASSERT(!observed.ready);
	UT_ASSERT_EQ(observed.target_node_id, -1);
	UT_ASSERT(!cluster_reconfig_r4_publish_ready(&expected));
}

/* Break caught: formation LMON must be able to turn one authenticated phase-3
 * item into the existing R4A_TARGET_READY bit only when the current local
 * episode and the completed exact JCMK COMMITTED_CLOSED receipt are identical.
 * The handoff alone must not advance the episode phase or admission. */
UT_TEST(test_reconfig_phase3_observation_sets_only_existing_ready_bit)
{
	ClusterReconfigState *state;
	ClusterReplacementEpisode episode;
	ClusterReplacementCommitMarkerV3 marker;
	ClusterReplacementPhase3HandoffItem item;
	ClusterMarkerAsync async;
	ClusterJoinGateVerdict gate_before;
	uint8 slot[CLUSTER_VOTING_SLOT_BYTES];
	int32 target = -1;

	ut_join_setup();
	cluster_node_id = 1;
	gate_before = cluster_reconfig_self_join_gate_verdict();
	state = (ClusterReconfigState *)reconfig_shmem_storage;
	episode = ut_admitted_replacement_episode(3);
	episode.phase = CLUSTER_REPLACEMENT_EPISODE_COMMITTED_CLOSED;
	episode.state_generation = UINT32_C(17);
	episode.readiness_flags
		= CLUSTER_REPLACEMENT_EPISODE_GRD_POSTEPOCH_READY
		  | CLUSTER_REPLACEMENT_EPISODE_INTENT_CLEARED;
	state->replacement_episode = episode;
	epoch_init_done = false;
	cluster_epoch_shmem_init();
	UT_ASSERT(cluster_epoch_observe_remote(
		episode.reserved_or_committed_epoch));

	memset(&marker, 0, sizeof(marker));
	marker.magic = CLUSTER_JCMK_MAGIC;
	marker.version = CLUSTER_JCMK_REPLACEMENT_VERSION;
	marker.target_node_id = episode.target_node_id;
	marker.phase = CLUSTER_JCMK_REPLACEMENT_PHASE_COMMITTED_CLOSED;
	marker.generation = UINT64_C(41);
	marker.old_admitted_incarnation = episode.old_admitted_incarnation;
	marker.fresh_incarnation = episode.fresh_incarnation;
	marker.baseline_epoch = episode.baseline_epoch;
	marker.reserved_or_committed_epoch = episode.reserved_or_committed_epoch;
	marker.request_nonce = episode.request_nonce;
	memcpy(marker.expected_purge_survivors, episode.expected_survivors,
		   sizeof(marker.expected_purge_survivors));
	marker.grammar_fingerprint = episode.grammar_fingerprint;
	cluster_marker_async_init(&async);
	UT_ASSERT(cluster_reconfig_submit_replacement_marker_v3_async(
		&async, marker.target_node_id, &marker,
		CLUSTER_MARKER_KIND_JOIN_COMMITTED, (TimestampTz)UINT64_C(2000)));
	UT_ASSERT(ut_join_qvotec_poll_write_pending(&target, slot));
	UT_ASSERT_EQ(target, marker.target_node_id);
	ut_join_qvotec_complete_write(true);

	memset(&item, 0, sizeof(item));
	item.message.phase
		= CLUSTER_REPLACEMENT_WIRE_PHASE_TARGET_RECOVERY_READY;
	item.message.target_node_id = episode.target_node_id;
	item.message.epoch = episode.baseline_epoch;
	item.message.request_nonce = episode.request_nonce;
	item.message.identity0 = episode.old_admitted_incarnation;
	item.message.identity1 = episode.fresh_incarnation;
	item.message.body.phase3.jcmk_generation = marker.generation;
	item.message.body.phase3.episode_state_generation
		= episode.state_generation;
	item.message.grammar_fingerprint = episode.grammar_fingerprint;
	item.authenticated_source_node_id = episode.target_node_id;
	item.local_receiver_node_id = episode.coordinator_node_id;
	item.control_connection_generation = UINT32_C(9);

	UT_ASSERT(cluster_reconfig_lmon_observe_replacement_ready(&item));
	UT_ASSERT_EQ(state->replacement_episode.phase,
				 CLUSTER_REPLACEMENT_EPISODE_COMMITTED_CLOSED);
	UT_ASSERT_EQ(state->replacement_episode.state_generation,
				 episode.state_generation);
	UT_ASSERT_EQ(state->replacement_episode.readiness_flags,
				 CLUSTER_REPLACEMENT_EPISODE_READINESS_MASK);
	UT_ASSERT_EQ((int)cluster_reconfig_self_join_gate_verdict(),
				 (int)gate_before);
}

/* Building ADMITTED is a zero-mutation derivation from three exact durable
 * inputs: the current fully-ready episode, its completed COMMITTED_CLOSED
 * JCMK receipt, and the matching common-ballot COMMIT_CLOSED head. */
UT_TEST(test_reconfig_builds_admitted_only_from_terminal_same_episode)
{
	ClusterReconfigState *state;
	ClusterReplacementEpisode episode;
	ClusterReplacementCommitMarkerV3 committed;
	ClusterReplacementCommitMarkerV3 admitted;
	ClusterReplacementCommitMarkerV3 built;
	ClusterEpochAuthorityValue head;
	uint8 committed_image[CLUSTER_JCMK_REPLACEMENT_BYTES];

	ut_join_setup();
	cluster_node_id = 1;
	state = (ClusterReconfigState *)reconfig_shmem_storage;
	episode = ut_admitted_replacement_episode(3);
	episode.phase = CLUSTER_REPLACEMENT_EPISODE_COMMITTED_CLOSED;
	episode.state_generation = UINT32_C(17);
	episode.readiness_flags = CLUSTER_REPLACEMENT_EPISODE_READINESS_MASK;
	state->replacement_episode = episode;
	cluster_membership_set_state(1, CLUSTER_MEMBER_MEMBER);
	cluster_membership_set_state(2, CLUSTER_MEMBER_MEMBER);
	epoch_init_done = false;
	cluster_epoch_shmem_init();
	UT_ASSERT(cluster_epoch_observe_remote(
		episode.reserved_or_committed_epoch));

	memset(&committed, 0, sizeof(committed));
	committed.magic = CLUSTER_JCMK_MAGIC;
	committed.version = CLUSTER_JCMK_REPLACEMENT_VERSION;
	committed.target_node_id = episode.target_node_id;
	committed.phase = CLUSTER_JCMK_REPLACEMENT_PHASE_COMMITTED_CLOSED;
	committed.generation = UINT64_C(41);
	committed.old_admitted_incarnation = episode.old_admitted_incarnation;
	committed.fresh_incarnation = episode.fresh_incarnation;
	committed.baseline_epoch = episode.baseline_epoch;
	committed.reserved_or_committed_epoch
		= episode.reserved_or_committed_epoch;
	committed.request_nonce = episode.request_nonce;
	memcpy(committed.expected_purge_survivors, episode.expected_survivors,
		   sizeof(committed.expected_purge_survivors));
	committed.grammar_fingerprint = episode.grammar_fingerprint;
	UT_ASSERT(cluster_replacement_marker_v3_encode(
		&committed, committed_image));
	memcpy(state->join_pending_marker, committed_image,
		   sizeof(committed_image));
	pg_atomic_write_u64(&state->join_marker_request_seq, UINT64_C(9));
	pg_atomic_write_u64(&state->join_marker_completion_seq, UINT64_C(9));
	pg_atomic_write_u32(&state->join_marker_result,
						CLUSTER_JOIN_MARKER_SUBMIT_ACK);

	memset(&head, 0, sizeof(head));
	head.value_version = CLUSTER_EPOCH_AUTHORITY_VALUE_VERSION;
	head.transition = CLUSTER_EPOCH_AUTHORITY_COMMIT_CLOSED;
	head.event_kind = CLUSTER_EPOCH_EVENT_SAME_NODE_REPLACEMENT;
	head.request_origin_node = episode.target_node_id;
	head.target_node_id = episode.target_node_id;
	head.authority_generation = UINT64_C(103);
	head.baseline_epoch = episode.baseline_epoch;
	head.reserved_epoch = episode.reserved_or_committed_epoch;
	head.old_incarnation = episode.old_admitted_incarnation;
	head.fresh_incarnation = episode.fresh_incarnation;
	head.request_nonce = episode.request_nonce;
	memcpy(head.authority_member_bitmap, episode.expected_survivors,
		   sizeof(head.authority_member_bitmap));
	head.event_subject_bitmap[episode.target_node_id / 8]
		= (uint8)(1u << (episode.target_node_id % 8));
	head.grammar_fingerprint = episode.grammar_fingerprint;
	memset(head.predecessor_digest, 0x5a,
		   sizeof(head.predecessor_digest));

	cluster_membership_set_state(2, CLUSTER_MEMBER_ABSENT);
	memset(&admitted, 0, sizeof(admitted));
	UT_ASSERT(!cluster_reconfig_lmon_build_replacement_admitted(
		&head, &admitted));
	UT_ASSERT_EQ((int)admitted.phase, 0);
	cluster_membership_set_state(2, CLUSTER_MEMBER_MEMBER);
	cluster_membership_set_state(episode.target_node_id,
							 CLUSTER_MEMBER_MEMBER);
	UT_ASSERT(!cluster_reconfig_lmon_build_replacement_admitted(
		&head, &admitted));
	UT_ASSERT_EQ((int)admitted.phase, 0);
	cluster_membership_set_state(episode.target_node_id,
							 CLUSTER_MEMBER_ABSENT);
	cluster_membership_set_state(4, CLUSTER_MEMBER_MEMBER);
	UT_ASSERT(!cluster_reconfig_lmon_build_replacement_admitted(
		&head, &admitted));
	UT_ASSERT_EQ((int)admitted.phase, 0);
	cluster_membership_set_state(4, CLUSTER_MEMBER_ABSENT);

	ut_candidate2_capable[episode.target_node_id] = false;
	memset(&admitted, 0, sizeof(admitted));
	UT_ASSERT(!cluster_reconfig_lmon_build_replacement_admitted(
		&head, &admitted));
	UT_ASSERT_EQ((int)admitted.phase, 0);
	ut_candidate2_capable[episode.target_node_id] = true;
	ut_candidate2_capable[2] = false;
	UT_ASSERT(!cluster_reconfig_lmon_build_replacement_admitted(
		&head, &admitted));
	UT_ASSERT_EQ((int)admitted.phase, 0);
	ut_candidate2_capable[2] = true;

	memset(&admitted, 0, sizeof(admitted));
	UT_ASSERT(cluster_reconfig_lmon_build_replacement_admitted(
		&head, &admitted));
	UT_ASSERT_EQ((int)admitted.phase,
				 CLUSTER_JCMK_REPLACEMENT_PHASE_ADMITTED);
	UT_ASSERT_EQ(admitted.generation, committed.generation + 1);
	UT_ASSERT_EQ(admitted.ready_state_generation,
				 episode.state_generation);
	built = admitted;
	UT_ASSERT_EQ(state->replacement_episode.phase,
				 CLUSTER_REPLACEMENT_EPISODE_COMMITTED_CLOSED);
	UT_ASSERT_EQ((int)cluster_reconfig_self_join_gate_verdict(),
				 (int)CLUSTER_JOIN_GATE_ALLOW);

	head.request_nonce++;
	memset(&admitted, 0, sizeof(admitted));
	UT_ASSERT(!cluster_reconfig_lmon_build_replacement_admitted(
		&head, &admitted));
	UT_ASSERT_EQ((int)admitted.phase, 0);

	head.request_nonce--;
	UT_ASSERT(cluster_replacement_marker_v3_encode(
		&built, committed_image));
	memcpy(state->join_pending_marker, committed_image,
		   sizeof(committed_image));
	pg_atomic_write_u64(&state->join_marker_request_seq, UINT64_C(10));
	pg_atomic_write_u64(&state->join_marker_completion_seq, UINT64_C(10));
	pg_atomic_write_u32(&state->join_marker_result,
						CLUSTER_JOIN_MARKER_SUBMIT_ACK);
	ut_candidate2_capable[episode.target_node_id] = false;
	UT_ASSERT(!cluster_reconfig_lmon_finalize_replacement_admitted(
		&head, &built));
	UT_ASSERT_EQ(state->replacement_episode.phase,
				 CLUSTER_REPLACEMENT_EPISODE_COMMITTED_CLOSED);
	UT_ASSERT_EQ((int)cluster_membership_get_state(episode.target_node_id),
				 (int)CLUSTER_MEMBER_ABSENT);
	ut_candidate2_capable[episode.target_node_id] = true;
	ut_candidate2_capable[2] = false;
	UT_ASSERT(!cluster_reconfig_lmon_finalize_replacement_admitted(
		&head, &built));
	UT_ASSERT_EQ(state->replacement_episode.phase,
				 CLUSTER_REPLACEMENT_EPISODE_COMMITTED_CLOSED);
	UT_ASSERT_EQ((int)cluster_membership_get_state(episode.target_node_id),
				 (int)CLUSTER_MEMBER_ABSENT);
	ut_candidate2_capable[2] = true;
	UT_ASSERT(cluster_reconfig_lmon_finalize_replacement_admitted(
		&head, &built));
	UT_ASSERT_EQ(state->replacement_episode.phase,
				 CLUSTER_REPLACEMENT_EPISODE_ADMITTED);
	UT_ASSERT_EQ((int)cluster_membership_get_state(episode.target_node_id),
				 (int)CLUSTER_MEMBER_MEMBER);
	UT_ASSERT_EQ(cluster_membership_get_last_admitted_incarnation(
					 episode.target_node_id),
				 episode.fresh_incarnation);
	UT_ASSERT_EQ((int)cluster_reconfig_self_join_gate_verdict(),
				 (int)CLUSTER_JOIN_GATE_ALLOW);
}

/* The formation LMON actor may publish ADMITTED/MEMBER only between two exact
 * RECOVER_HEAD observations of the same settled COMMIT_CLOSED value+ballot.
 * This drives the real region-3 mailbox in between those authority reads and
 * proves that MEMBER publication leaves the local write gate untouched. */
UT_TEST(test_reconfig_lmon_admits_between_identical_terminal_head_reads)
{
	ClusterReconfigState *state;
	ClusterReplacementEpisode episode;
	ClusterReplacementCommitMarkerV3 committed;
	ClusterReplacementCommitMarkerV3 durable_admitted;
	ClusterEpochAuthorityValue head;
	ClusterEpochBallotId ballot;
	uint8 committed_image[CLUSTER_JCMK_REPLACEMENT_BYTES];
	uint8 head_image[CLUSTER_EPOCH_AUTHORITY_VALUE_BYTES];
	uint8 ballot_image[CLUSTER_EPOCH_BALLOT_ID_BYTES];
	uint8 slot[CLUSTER_VOTING_SLOT_BYTES];
	int32 target = -1;

	ut_join_setup();
	cluster_node_id = 1;
	cluster_membership_record_admitted(cluster_node_id, UINT64_C(111));
	state = (ClusterReconfigState *)reconfig_shmem_storage;
	episode = ut_admitted_replacement_episode(3);
	episode.phase = CLUSTER_REPLACEMENT_EPISODE_COMMITTED_CLOSED;
	episode.state_generation = UINT32_C(17);
	episode.readiness_flags = CLUSTER_REPLACEMENT_EPISODE_READINESS_MASK;
	state->replacement_episode = episode;
	cluster_membership_set_state(1, CLUSTER_MEMBER_MEMBER);
	cluster_membership_set_state(2, CLUSTER_MEMBER_MEMBER);
	epoch_init_done = false;
	cluster_epoch_shmem_init();
	UT_ASSERT(cluster_epoch_observe_remote(
		episode.reserved_or_committed_epoch));

	memset(&committed, 0, sizeof(committed));
	committed.magic = CLUSTER_JCMK_MAGIC;
	committed.version = CLUSTER_JCMK_REPLACEMENT_VERSION;
	committed.target_node_id = episode.target_node_id;
	committed.phase = CLUSTER_JCMK_REPLACEMENT_PHASE_COMMITTED_CLOSED;
	committed.generation = UINT64_C(41);
	committed.old_admitted_incarnation = episode.old_admitted_incarnation;
	committed.fresh_incarnation = episode.fresh_incarnation;
	committed.baseline_epoch = episode.baseline_epoch;
	committed.reserved_or_committed_epoch
		= episode.reserved_or_committed_epoch;
	committed.request_nonce = episode.request_nonce;
	memcpy(committed.expected_purge_survivors, episode.expected_survivors,
		   sizeof(committed.expected_purge_survivors));
	committed.grammar_fingerprint = episode.grammar_fingerprint;
	UT_ASSERT(cluster_replacement_marker_v3_encode(
		&committed, committed_image));
	memcpy(state->join_pending_marker, committed_image,
		   sizeof(committed_image));
	pg_atomic_write_u64(&state->join_marker_request_seq, UINT64_C(9));
	pg_atomic_write_u64(&state->join_marker_completion_seq, UINT64_C(9));
	pg_atomic_write_u32(&state->join_marker_result,
						CLUSTER_JOIN_MARKER_SUBMIT_ACK);

	memset(&head, 0, sizeof(head));
	head.value_version = CLUSTER_EPOCH_AUTHORITY_VALUE_VERSION;
	head.transition = CLUSTER_EPOCH_AUTHORITY_COMMIT_CLOSED;
	head.event_kind = CLUSTER_EPOCH_EVENT_SAME_NODE_REPLACEMENT;
	head.request_origin_node = episode.target_node_id;
	head.target_node_id = episode.target_node_id;
	head.authority_generation = UINT64_C(103);
	head.baseline_epoch = episode.baseline_epoch;
	head.reserved_epoch = episode.reserved_or_committed_epoch;
	head.old_incarnation = episode.old_admitted_incarnation;
	head.fresh_incarnation = episode.fresh_incarnation;
	head.request_nonce = episode.request_nonce;
	memcpy(head.authority_member_bitmap, episode.expected_survivors,
		   sizeof(head.authority_member_bitmap));
	head.event_subject_bitmap[episode.target_node_id / 8]
		= (uint8)(1u << (episode.target_node_id % 8));
	head.grammar_fingerprint = episode.grammar_fingerprint;
	memset(head.predecessor_digest, 0x5a,
		   sizeof(head.predecessor_digest));
	UT_ASSERT(cluster_epoch_authority_value_encode(
		&head, CLUSTER_EPOCH_BALLOT_GRAMMAR_FINGERPRINT, head_image));

	memset(&ballot, 0, sizeof(ballot));
	ballot.counter = UINT64_C(7);
	ballot.proposer_node_id = cluster_node_id;
	ballot.proposer_admitted_incarnation = UINT64_C(111);
	ballot.nonce = UINT64_C(0xabcdef);
	UT_ASSERT(cluster_epoch_ballot_id_encode(&ballot, ballot_image));

	memset(&ut_authority_completion, 0, sizeof(ut_authority_completion));
	ut_authority_completion.request_seq = UINT64_C(2);
	ut_authority_completion.result = CLUSTER_QVOTEC_MAILBOX_CHOSEN;
	memcpy(ut_authority_completion.completion_value, head_image,
		   sizeof(head_image));
	memcpy(ut_authority_completion.completion_ballot, ballot_image,
		   sizeof(ballot_image));
	ut_authority_completion.actor_phase
		= CLUSTER_QVOTEC_ACTOR_RECOVER_SCAN_B;

	cluster_reconfig_lmon_replacement_admit_tick(); /* submit pre-read */
	UT_ASSERT_EQ(ut_authority_submit_calls, 1);
	ut_authority_completion_ready = true;
	cluster_reconfig_lmon_replacement_admit_tick(); /* consume pre-read */
	ut_candidate2_capable[episode.target_node_id] = false;
	cluster_reconfig_lmon_replacement_admit_tick(); /* refuse stale candidate */
	memset(slot, 0, sizeof(slot));
	if (ut_join_qvotec_poll_write_pending(&target, slot)) {
		ClusterReplacementCommitMarkerV3 observed;

		UT_ASSERT(cluster_replacement_marker_v3_decode(
			slot, episode.target_node_id, &observed));
		UT_ASSERT(observed.phase
				  != CLUSTER_JCMK_REPLACEMENT_PHASE_ADMITTED);
		ut_join_qvotec_complete_write(true);
	}
	UT_ASSERT_EQ((int)state->replacement_episode.phase,
				 (int)CLUSTER_REPLACEMENT_EPISODE_COMMITTED_CLOSED);

	ut_candidate2_capable[episode.target_node_id] = true;
	cluster_reconfig_lmon_replacement_admit_tick(); /* resubmit pre-read */
	UT_ASSERT_EQ(ut_authority_submit_calls, 2);
	ut_authority_completion.request_seq = UINT64_C(4);
	ut_authority_completion_ready = true;
	cluster_reconfig_lmon_replacement_admit_tick(); /* consume pre-read */
	cluster_reconfig_lmon_replacement_admit_tick(); /* submit ADMITTED */
	memset(slot, 0, sizeof(slot));
	UT_ASSERT(ut_join_qvotec_poll_write_pending(&target, slot));
	UT_ASSERT_EQ(target, episode.target_node_id);
	UT_ASSERT(cluster_replacement_marker_v3_decode(
		slot, episode.target_node_id, &durable_admitted));
	UT_ASSERT_EQ((int)durable_admitted.phase,
				 (int)CLUSTER_JCMK_REPLACEMENT_PHASE_ADMITTED);
	UT_ASSERT_EQ(durable_admitted.generation, committed.generation + 1);
	UT_ASSERT_EQ(durable_admitted.ready_state_generation,
				 episode.state_generation);
	ut_join_qvotec_complete_write(true);

	cluster_reconfig_lmon_replacement_admit_tick(); /* observe ADMITTED ACK */
	cluster_reconfig_lmon_replacement_admit_tick(); /* submit post-read */
	UT_ASSERT_EQ(ut_authority_submit_calls, 3);
	ut_authority_completion.request_seq = UINT64_C(6);
	ut_authority_completion_ready = true;
	cluster_reconfig_lmon_replacement_admit_tick(); /* consume + publish */

	UT_ASSERT_EQ((int)state->replacement_episode.phase,
				 (int)CLUSTER_REPLACEMENT_EPISODE_ADMITTED);
	UT_ASSERT_EQ((int)cluster_membership_get_state(episode.target_node_id),
				 (int)CLUSTER_MEMBER_MEMBER);
	UT_ASSERT_EQ(cluster_membership_get_last_admitted_incarnation(
					 episode.target_node_id),
				 episode.fresh_incarnation);
	UT_ASSERT_EQ((int)cluster_reconfig_self_join_gate_verdict(),
				 (int)CLUSTER_JOIN_GATE_ALLOW);
}

UT_TEST(test_reconfig_lmon_closed_apply_consumes_recovered_verify_pair)
{
	ClusterReconfigState *state;
	ClusterReplacementEpisode episode;
	ClusterReplacementCommitMarkerV3 committed;
	ClusterEpochAuthorityValue head;
	ClusterEpochBallotId ballot;
	ClusterJoinMarkerMailboxOperationV1 operation;
	ClusterMarkerAsync conflicting_marker;
	ReconfigEvent event;
	uint8 committed_image[CLUSTER_JCMK_REPLACEMENT_BYTES];
	uint8 head_image[CLUSTER_EPOCH_AUTHORITY_VALUE_BYTES];
	uint8 ballot_image[CLUSTER_EPOCH_BALLOT_ID_BYTES];
	uint8 slot[CLUSTER_VOTING_SLOT_BYTES];
	int32 target = -1;

	ut_join_setup();
	cluster_node_id = 2;
	MyBackendType = B_LMON;
	ut_in_quorum_value = true;
	state = (ClusterReconfigState *)reconfig_shmem_storage;
	episode = ut_admitted_replacement_episode(3);
	episode.phase = CLUSTER_REPLACEMENT_EPISODE_PURGE_COMPLETE;
	episode.state_generation = UINT32_C(17);
	episode.readiness_flags = 0;
	state->replacement_episode = episode;
	cluster_membership_set_state(1, CLUSTER_MEMBER_MEMBER);
	cluster_membership_set_state(2, CLUSTER_MEMBER_MEMBER);
	cluster_membership_record_admitted(1, UINT64_C(111));
	cluster_membership_record_admitted(
		episode.target_node_id, episode.old_admitted_incarnation);
	epoch_init_done = false;
	cluster_epoch_shmem_init();
	UT_ASSERT(cluster_epoch_observe_remote(episode.baseline_epoch));

	memset(&committed, 0, sizeof(committed));
	committed.magic = CLUSTER_JCMK_MAGIC;
	committed.version = CLUSTER_JCMK_REPLACEMENT_VERSION;
	committed.target_node_id = episode.target_node_id;
	committed.phase = CLUSTER_JCMK_REPLACEMENT_PHASE_COMMITTED_CLOSED;
	committed.generation = UINT64_C(41);
	committed.old_admitted_incarnation = episode.old_admitted_incarnation;
	committed.fresh_incarnation = episode.fresh_incarnation;
	committed.baseline_epoch = episode.baseline_epoch;
	committed.reserved_or_committed_epoch
		= episode.reserved_or_committed_epoch;
	committed.request_nonce = episode.request_nonce;
	memcpy(committed.expected_purge_survivors, episode.expected_survivors,
		   sizeof(committed.expected_purge_survivors));
	committed.grammar_fingerprint = episode.grammar_fingerprint;
	UT_ASSERT(cluster_replacement_marker_v3_encode(
		&committed, committed_image));

	memset(&head, 0, sizeof(head));
	head.value_version = CLUSTER_EPOCH_AUTHORITY_VALUE_VERSION;
	head.transition = CLUSTER_EPOCH_AUTHORITY_COMMIT_CLOSED;
	head.event_kind = CLUSTER_EPOCH_EVENT_SAME_NODE_REPLACEMENT;
	head.request_origin_node = episode.target_node_id;
	head.target_node_id = episode.target_node_id;
	head.authority_generation = UINT64_C(103);
	head.baseline_epoch = episode.baseline_epoch;
	head.reserved_epoch = episode.reserved_or_committed_epoch;
	head.old_incarnation = episode.old_admitted_incarnation;
	head.fresh_incarnation = episode.fresh_incarnation;
	head.request_nonce = episode.request_nonce;
	memcpy(head.authority_member_bitmap, episode.expected_survivors,
		   sizeof(head.authority_member_bitmap));
	head.event_subject_bitmap[episode.target_node_id / 8]
		= (uint8)(1u << (episode.target_node_id % 8));
	head.grammar_fingerprint = episode.grammar_fingerprint;
	memset(head.predecessor_digest, 0x5a,
		   sizeof(head.predecessor_digest));
	UT_ASSERT(cluster_epoch_authority_value_encode(
		&head, CLUSTER_EPOCH_BALLOT_GRAMMAR_FINGERPRINT, head_image));

	memset(&ballot, 0, sizeof(ballot));
	ballot.counter = UINT64_C(7);
	ballot.proposer_node_id = episode.coordinator_node_id;
	ballot.proposer_admitted_incarnation = UINT64_C(111);
	ballot.nonce = UINT64_C(0xabcdef);
	UT_ASSERT(cluster_epoch_ballot_id_encode(&ballot, ballot_image));

	cluster_reconfig_lmon_replacement_closed_tick();
	UT_ASSERT_EQ(ut_authority_submit_calls, 1);
	UT_ASSERT(cluster_reconfig_join_qvotec_poll_pending(
		&operation, &target, slot));
	UT_ASSERT_EQ((int)operation,
				 (int)CLUSTER_JOIN_MARKER_MAILBOX_VERIFY_COMMITTED_CLOSED);
	UT_ASSERT_EQ(target, episode.target_node_id);
	cluster_reconfig_join_qvotec_complete(operation, true, committed_image);
	cluster_marker_async_init(&conflicting_marker);
	UT_ASSERT(!cluster_reconfig_submit_replacement_marker_v3_async(
		&conflicting_marker, episode.target_node_id, &committed,
		CLUSTER_MARKER_KIND_JOIN_COMMITTED,
		(TimestampTz)UINT64_C(8000)));

	memset(&ut_authority_completion, 0,
		   sizeof(ut_authority_completion));
	ut_authority_completion.request_seq = UINT64_C(2);
	ut_authority_completion.result = CLUSTER_QVOTEC_MAILBOX_CHOSEN;
	memcpy(ut_authority_completion.completion_value, head_image,
		   sizeof(head_image));
	memcpy(ut_authority_completion.completion_ballot, ballot_image,
		   sizeof(ballot_image));
	ut_authority_completion.actor_phase
		= CLUSTER_QVOTEC_ACTOR_RECOVER_SCAN_B;
	ut_authority_completion_ready = true;

	cluster_reconfig_lmon_replacement_closed_tick();
	UT_ASSERT_EQ((int)state->replacement_episode.phase,
				 (int)CLUSTER_REPLACEMENT_EPISODE_COMMITTED_CLOSED);
	UT_ASSERT_EQ(state->replacement_episode.state_generation,
				 UINT32_C(18));
	UT_ASSERT_EQ(cluster_epoch_get_current(),
				 episode.reserved_or_committed_epoch);
	cluster_reconfig_get_last_event(&event);
	UT_ASSERT_EQ((int)event.reconfig_kind,
				 (int)RECONFIG_KIND_REPLACEMENT_COMMITTED);
	UT_ASSERT_EQ(event.old_epoch, episode.baseline_epoch);
	UT_ASSERT_EQ(event.new_epoch, episode.reserved_or_committed_epoch);
	UT_ASSERT_EQ((int)event.observer_role,
				 (int)CLUSTER_RECONFIG_OBSERVER_SURVIVOR);
	UT_ASSERT_EQ((int)cluster_reconfig_lmon_publish_replacement_committed_closed(
		UINT64_C(2), pg_atomic_read_u64(&state->join_marker_request_seq)),
		(int)CLUSTER_REPLACEMENT_CLOSED_RETRY);
}

/* Target QVOTEC must not infer replacement membership from one disk, a v2
 * marker, or a stale incarnation.  Two identical ADMITTED v3 images on the
 * configured three-disk set plus the exact local episode/publish proof may
 * publish only closed MEMBER metadata. */
UT_TEST(test_reconfig_qvotec_majority_admitted_publishes_member_closed)
{
	ClusterReconfigState *state;
	ClusterReplacementEpisode episode;
	ClusterReplacementCommitMarkerV3 admitted;
	uint8 admitted_image[CLUSTER_JCMK_REPLACEMENT_BYTES];
	int fds[3] = { 0, 1, 2 };

	ut_join_setup();
	state = (ClusterReconfigState *)reconfig_shmem_storage;
	episode = ut_admitted_replacement_episode(cluster_node_id);
	episode.phase = CLUSTER_REPLACEMENT_EPISODE_POST_EPOCH;
	state->replacement_episode = episode;
	epoch_init_done = false;
	cluster_epoch_shmem_init();
	UT_ASSERT(cluster_epoch_observe_remote(
		episode.reserved_or_committed_epoch));

	/* The immutable expected survivor set {1,2} has published E+1. */
	ut_declared_set[1] = true;
	ut_declared_set[2] = true;
	cluster_membership_set_state(1, CLUSTER_MEMBER_MEMBER);
	cluster_membership_set_state(2, CLUSTER_MEMBER_MEMBER);
	cluster_reconfig_record_observed_slot(
		1, UINT64_C(101), UINT64_C(1), episode.reserved_or_committed_epoch);
	cluster_reconfig_record_observed_slot(
		2, UINT64_C(102), UINT64_C(1), episode.reserved_or_committed_epoch);

	memset(&admitted, 0, sizeof(admitted));
	admitted.magic = CLUSTER_JCMK_MAGIC;
	admitted.version = CLUSTER_JCMK_REPLACEMENT_VERSION;
	admitted.target_node_id = episode.target_node_id;
	admitted.phase = CLUSTER_JCMK_REPLACEMENT_PHASE_ADMITTED;
	admitted.generation = UINT64_C(42);
	admitted.old_admitted_incarnation = episode.old_admitted_incarnation;
	admitted.fresh_incarnation = episode.fresh_incarnation;
	admitted.baseline_epoch = episode.baseline_epoch;
	admitted.reserved_or_committed_epoch
		= episode.reserved_or_committed_epoch;
	admitted.request_nonce = episode.request_nonce;
	memcpy(admitted.expected_purge_survivors, episode.expected_survivors,
		   sizeof(admitted.expected_purge_survivors));
	admitted.grammar_fingerprint = episode.grammar_fingerprint;
	admitted.ready_state_generation = episode.state_generation;
	UT_ASSERT(cluster_replacement_marker_v3_encode(
		&admitted, admitted_image));
	memcpy(ut_join_disk_images[0], admitted_image, sizeof(admitted_image));
	memcpy(ut_join_disk_images[1], admitted_image, sizeof(admitted_image));
	ut_join_disk_readable[0] = true;

	UT_ASSERT(!cluster_reconfig_qvotec_observe_replacement_admitted(
		fds, 3, episode.fresh_incarnation));
	UT_ASSERT_EQ((int)state->replacement_episode.phase,
				 (int)CLUSTER_REPLACEMENT_EPISODE_POST_EPOCH);
	UT_ASSERT_EQ((int)cluster_membership_get_state(cluster_node_id),
				 (int)CLUSTER_MEMBER_ABSENT);

	ut_join_disk_readable[1] = true;
	UT_ASSERT(!cluster_reconfig_qvotec_observe_replacement_admitted(
		fds, 3, episode.fresh_incarnation + 1));
	ut_candidate2_capable[2] = false;
	UT_ASSERT(!cluster_reconfig_qvotec_observe_replacement_admitted(
		fds, 3, episode.fresh_incarnation));
	UT_ASSERT_EQ((int)state->replacement_episode.phase,
				 (int)CLUSTER_REPLACEMENT_EPISODE_POST_EPOCH);
	UT_ASSERT_EQ((int)cluster_membership_get_state(cluster_node_id),
				 (int)CLUSTER_MEMBER_ABSENT);
	ut_candidate2_capable[2] = true;
	UT_ASSERT(cluster_reconfig_qvotec_observe_replacement_admitted(
		fds, 3, episode.fresh_incarnation));
	UT_ASSERT_EQ((int)state->replacement_episode.phase,
				 (int)CLUSTER_REPLACEMENT_EPISODE_ADMITTED);
	UT_ASSERT_EQ((int)cluster_membership_get_state(cluster_node_id),
				 (int)CLUSTER_MEMBER_MEMBER);
	UT_ASSERT_EQ((int)cluster_reconfig_self_join_gate_verdict(),
				 (int)CLUSTER_JOIN_GATE_BLOCK_53R60);
}

/* Formation LMON must consume one co-sampled durable ADMITTED certificate,
 * not the episode phase by itself.  Drift or an extra MEMBER leaves caller
 * outputs untouched so it cannot seed a PGSA request. */
UT_TEST(test_reconfig_lmon_snapshots_only_exact_admitted_certificate)
{
	ClusterReconfigState *state;
	ClusterReplacementEpisode episode;
	ClusterReplacementEpisode observed_episode;
	ClusterReplacementEpisode before_episode;
	ClusterReplacementCommitMarkerV3 admitted;
	ClusterReplacementCommitMarkerV3 observed_marker;
	ClusterReplacementCommitMarkerV3 before_marker;
	uint8 admitted_image[CLUSTER_JCMK_REPLACEMENT_BYTES];

	ut_join_setup();
	cluster_node_id = 1;
	state = (ClusterReconfigState *)reconfig_shmem_storage;
	episode = ut_admitted_replacement_episode(3);
	state->replacement_episode = episode;
	epoch_init_done = false;
	cluster_epoch_shmem_init();
	UT_ASSERT(cluster_epoch_observe_remote(
		episode.reserved_or_committed_epoch));
	cluster_membership_set_state(1, CLUSTER_MEMBER_MEMBER);
	cluster_membership_set_state(2, CLUSTER_MEMBER_MEMBER);
	cluster_membership_set_state(episode.target_node_id, CLUSTER_MEMBER_MEMBER);
	cluster_membership_record_admitted(episode.target_node_id,
								 episode.fresh_incarnation);

	memset(&admitted, 0, sizeof(admitted));
	admitted.magic = CLUSTER_JCMK_MAGIC;
	admitted.version = CLUSTER_JCMK_REPLACEMENT_VERSION;
	admitted.target_node_id = episode.target_node_id;
	admitted.phase = CLUSTER_JCMK_REPLACEMENT_PHASE_ADMITTED;
	admitted.generation = UINT64_C(42);
	admitted.old_admitted_incarnation = episode.old_admitted_incarnation;
	admitted.fresh_incarnation = episode.fresh_incarnation;
	admitted.baseline_epoch = episode.baseline_epoch;
	admitted.reserved_or_committed_epoch
		= episode.reserved_or_committed_epoch;
	admitted.request_nonce = episode.request_nonce;
	memcpy(admitted.expected_purge_survivors, episode.expected_survivors,
		   sizeof(admitted.expected_purge_survivors));
	admitted.grammar_fingerprint = episode.grammar_fingerprint;
	admitted.ready_state_generation = episode.state_generation;
	UT_ASSERT(cluster_replacement_marker_v3_encode(
		&admitted, admitted_image));
	memcpy(state->join_pending_marker, admitted_image,
		   sizeof(admitted_image));
	pg_atomic_write_u64(&state->join_marker_request_seq, UINT64_C(10));
	pg_atomic_write_u64(&state->join_marker_completion_seq, UINT64_C(10));
	pg_atomic_write_u32(&state->join_marker_result,
						CLUSTER_JOIN_MARKER_SUBMIT_ACK);

	memset(&observed_episode, 0xa5, sizeof(observed_episode));
	memset(&observed_marker, 0x5a, sizeof(observed_marker));
	UT_ASSERT(cluster_reconfig_lmon_snapshot_replacement_admitted(
		&observed_episode, &observed_marker));
	UT_ASSERT_EQ(memcmp(&observed_episode, &episode, sizeof(episode)), 0);
	UT_ASSERT(cluster_replacement_marker_v3_same_image(
		&observed_marker, &admitted));

	memset(&observed_episode, 0xa5, sizeof(observed_episode));
	memset(&observed_marker, 0x5a, sizeof(observed_marker));
	before_episode = observed_episode;
	before_marker = observed_marker;
	cluster_membership_set_state(4, CLUSTER_MEMBER_MEMBER);
	UT_ASSERT(!cluster_reconfig_lmon_snapshot_replacement_admitted(
		&observed_episode, &observed_marker));
	UT_ASSERT_EQ(memcmp(&observed_episode, &before_episode,
					 sizeof(observed_episode)), 0);
	UT_ASSERT_EQ(memcmp(&observed_marker, &before_marker,
					 sizeof(observed_marker)), 0);
	cluster_membership_set_state(4, CLUSTER_MEMBER_ABSENT);

	admitted.ready_state_generation++;
	UT_ASSERT(cluster_replacement_marker_v3_encode(
		&admitted, admitted_image));
	memcpy(state->join_pending_marker, admitted_image,
		   sizeof(admitted_image));
	UT_ASSERT(!cluster_reconfig_lmon_snapshot_replacement_admitted(
		&observed_episode, &observed_marker));
}

/* The target LMON is the CONTROL-plane sender for Startup's instantaneous
 * phase-3 value.  It must route only the exact current target episode/JCMK
 * tuple, retransmit byte-identically without an ACK, and stop when that
 * episode leaves its pre-ADMITTED phase. */
UT_TEST(test_reconfig_target_lmon_retransmits_exact_phase3_until_admitted)
{
	ClusterReconfigState *state;
	ClusterReplacementEpisode episode;
	ClusterReplacementCommitMarkerV3 committed;
	ClusterReplacementWireMessage decoded;
	ClusterEpochAuthorityValue head;
	ClusterEpochBallotId ballot;
	ClusterQvotecMailbox lifecycle_authority_mailbox;
	ClusterJoinMarkerMailboxOperationV1 operation;
	ClusterMarkerAsync conflicting_marker;
	pg_atomic_uint32 lifecycle_status;
	uint8 committed_image[CLUSTER_JCMK_REPLACEMENT_BYTES];
	uint8 head_image[CLUSTER_EPOCH_AUTHORITY_VALUE_BYTES];
	uint8 ballot_image[CLUSTER_EPOCH_BALLOT_ID_BYTES];
	uint8 slot[CLUSTER_VOTING_SLOT_BYTES];
	uint8 first_image[CLUSTER_REPLACEMENT_WIRE_BYTES];
	int32 target = -1;

	ut_join_setup();
	cluster_qvotec_mailbox_restart_reset(&lifecycle_authority_mailbox);
	pg_atomic_init_u32(&lifecycle_status, CLUSTER_QVOTEC_DOWN);
	UT_ASSERT(cluster_reconfig_qvotec_lifecycle_transition(
		&lifecycle_authority_mailbox, &lifecycle_status,
		CLUSTER_QVOTEC_STARTING));
	UT_ASSERT(cluster_reconfig_qvotec_lifecycle_transition(
		&lifecycle_authority_mailbox, &lifecycle_status,
		CLUSTER_QVOTEC_READY));
	cluster_node_id = 3;
	MyBackendType = B_LMON;
	ut_in_quorum_value = true;
	state = (ClusterReconfigState *)reconfig_shmem_storage;
	episode = ut_admitted_replacement_episode(cluster_node_id);
	episode.phase = CLUSTER_REPLACEMENT_EPISODE_COMMITTED_CLOSED;
	episode.state_generation = UINT32_C(17);
	episode.readiness_flags = CLUSTER_REPLACEMENT_EPISODE_R4A_TARGET_READY;
	state->replacement_episode = episode;
	state->self_join_admitted = 0;
	state->self_join_failed = 0;
	cluster_membership_set_state(1, CLUSTER_MEMBER_MEMBER);
	cluster_membership_set_state(2, CLUSTER_MEMBER_MEMBER);
	cluster_membership_record_admitted(1, UINT64_C(111));
	cluster_membership_record_admitted(
		episode.target_node_id, episode.old_admitted_incarnation);
	epoch_init_done = false;
	cluster_epoch_shmem_init();
	UT_ASSERT(cluster_epoch_observe_remote(
		episode.reserved_or_committed_epoch));

	memset(&committed, 0, sizeof(committed));
	committed.magic = CLUSTER_JCMK_MAGIC;
	committed.version = CLUSTER_JCMK_REPLACEMENT_VERSION;
	committed.target_node_id = episode.target_node_id;
	committed.phase = CLUSTER_JCMK_REPLACEMENT_PHASE_COMMITTED_CLOSED;
	committed.generation = UINT64_C(41);
	committed.old_admitted_incarnation = episode.old_admitted_incarnation;
	committed.fresh_incarnation = episode.fresh_incarnation;
	committed.baseline_epoch = episode.baseline_epoch;
	committed.reserved_or_committed_epoch
		= episode.reserved_or_committed_epoch;
	committed.request_nonce = episode.request_nonce;
	memcpy(committed.expected_purge_survivors, episode.expected_survivors,
		   sizeof(committed.expected_purge_survivors));
	committed.grammar_fingerprint = episode.grammar_fingerprint;
	UT_ASSERT(cluster_replacement_marker_v3_encode(
		&committed, committed_image));

	memset(&head, 0, sizeof(head));
	head.value_version = CLUSTER_EPOCH_AUTHORITY_VALUE_VERSION;
	head.transition = CLUSTER_EPOCH_AUTHORITY_COMMIT_CLOSED;
	head.event_kind = CLUSTER_EPOCH_EVENT_SAME_NODE_REPLACEMENT;
	head.request_origin_node = episode.target_node_id;
	head.target_node_id = episode.target_node_id;
	head.authority_generation = UINT64_C(103);
	head.baseline_epoch = episode.baseline_epoch;
	head.reserved_epoch = episode.reserved_or_committed_epoch;
	head.old_incarnation = episode.old_admitted_incarnation;
	head.fresh_incarnation = episode.fresh_incarnation;
	head.request_nonce = episode.request_nonce;
	memcpy(head.authority_member_bitmap, episode.expected_survivors,
		   sizeof(head.authority_member_bitmap));
	head.event_subject_bitmap[episode.target_node_id / 8]
		= (uint8)(1u << (episode.target_node_id % 8));
	head.grammar_fingerprint = episode.grammar_fingerprint;
	memset(head.predecessor_digest, 0x5a,
		   sizeof(head.predecessor_digest));
	UT_ASSERT(cluster_epoch_authority_value_encode(
		&head, CLUSTER_EPOCH_BALLOT_GRAMMAR_FINGERPRINT, head_image));

	memset(&ballot, 0, sizeof(ballot));
	ballot.counter = UINT64_C(7);
	ballot.proposer_node_id = episode.coordinator_node_id;
	ballot.proposer_admitted_incarnation = UINT64_C(111);
	ballot.nonce = UINT64_C(0xabcdef);
	UT_ASSERT(cluster_epoch_ballot_id_encode(&ballot, ballot_image));
	ut_r4_snapshot_use_reconfig = true;

	cluster_reconfig_lmon_replacement_ready_tick();
	UT_ASSERT_EQ(ut_phase3_send_calls, 0);
	UT_ASSERT_EQ(ut_authority_submit_calls, 1);
	UT_ASSERT(cluster_reconfig_join_qvotec_poll_pending(
		&operation, &target, slot));
	UT_ASSERT_EQ((int)operation,
				 (int)CLUSTER_JOIN_MARKER_MAILBOX_VERIFY_COMMITTED_CLOSED);
	UT_ASSERT_EQ(target, episode.target_node_id);
	cluster_reconfig_join_qvotec_complete(operation, true, committed_image);
	cluster_marker_async_init(&conflicting_marker);
	UT_ASSERT(!cluster_reconfig_submit_replacement_marker_v3_async(
		&conflicting_marker, episode.target_node_id, &committed,
		CLUSTER_MARKER_KIND_JOIN_COMMITTED,
		(TimestampTz)UINT64_C(9000)));

	memset(&ut_authority_completion, 0,
		   sizeof(ut_authority_completion));
	ut_authority_completion.request_seq = UINT64_C(2);
	ut_authority_completion.result = CLUSTER_QVOTEC_MAILBOX_CHOSEN;
	memcpy(ut_authority_completion.completion_value, head_image,
		   sizeof(head_image));
	memcpy(ut_authority_completion.completion_ballot, ballot_image,
		   sizeof(ballot_image));
	ut_authority_completion.actor_phase
		= CLUSTER_QVOTEC_ACTOR_RECOVER_SCAN_B;
	ut_authority_completion_ready = true;

	cluster_reconfig_lmon_replacement_ready_tick();
	UT_ASSERT_EQ(ut_phase3_send_calls, 1);
	UT_ASSERT_EQ((int)ut_phase3_send_msg_type,
				 (int)PGRAC_IC_MSG_GES_REQUEST);
	UT_ASSERT_EQ(ut_phase3_send_dest, episode.coordinator_node_id);
	UT_ASSERT_EQ((int)ut_phase3_send_length,
				 CLUSTER_REPLACEMENT_WIRE_BYTES);
	memcpy(first_image, ut_phase3_send_bytes, sizeof(first_image));
	UT_ASSERT(cluster_replacement_wire_decode(first_image, &decoded));
	UT_ASSERT_EQ((int)decoded.phase,
				 CLUSTER_REPLACEMENT_WIRE_PHASE_TARGET_RECOVERY_READY);
	UT_ASSERT_EQ(decoded.target_node_id, episode.target_node_id);
	UT_ASSERT(decoded.body.phase3.jcmk_generation == committed.generation);
	UT_ASSERT_EQ((int)decoded.body.phase3.episode_state_generation,
				 (int)episode.state_generation);

	ut_phase3_send_result = CLUSTER_IC_SEND_NOT_ADMITTED;
	cluster_reconfig_lmon_replacement_ready_tick();
	UT_ASSERT_EQ(ut_phase3_send_calls, 2);
	UT_ASSERT_EQ(memcmp(first_image, ut_phase3_send_bytes,
					 sizeof(first_image)), 0);

	ut_phase3_send_result = CLUSTER_IC_SEND_HARD_ERROR;
	cluster_reconfig_lmon_replacement_ready_tick();
	UT_ASSERT_EQ(ut_phase3_send_calls, 3);
	UT_ASSERT_EQ(ut_phase3_close_calls, 1);
	UT_ASSERT_EQ(ut_phase3_close_peer, episode.coordinator_node_id);

	state->replacement_episode.phase = CLUSTER_REPLACEMENT_EPISODE_ADMITTED;
	cluster_reconfig_lmon_replacement_ready_tick();
	UT_ASSERT_EQ(ut_phase3_send_calls, 3);
}


/* ============================================================
 * Main — register + run all tests.
 * ============================================================ */

UT_TEST(test_thread_recovery_eligibility_uses_current_failstop_and_root_duty)
{
	ClusterThreadRecLaunchEligibility eligibility;
	ClusterWalThreadClaim claim;
	ReconfigEvent event;

	cluster_reconfig_shmem_init();
	memset(&ut_recovery_root_identity, 0,
		   sizeof(ut_recovery_root_identity));
	ut_recovery_root_identity.system_identifier = UINT64_C(0x1234);
	memset(ut_recovery_root_identity.storage_uuid, 0x11, 16);
	memset(ut_recovery_root_identity.authority_uuid, 0x22, 16);
	ut_recovery_root_identity.authority_uuid[6] = 0x42;
	ut_recovery_root_identity.authority_uuid[8] = 0x82;
	ut_recovery_root_identity.origin_thread_id = 1;
	ut_recovery_root_identity.origin_node_id = 0;
	ut_recovery_root_identity.thread_claim_created_at = 77;
	cluster_wal_thread_claim_fill(&claim, 1, 0, 77);
	ut_recovery_root_identity.thread_claim_crc32c = claim.crc;
	ut_recovery_root_identity.origin_owner_incarnation = 9;
	ut_recovery_root_identity.root_lineage_seq = 10;
	ut_recovery_root_result = CLUSTER_CONTROL_ROOT_OK_PRIMARY;

	memset(&event, 0, sizeof(event));
	event.event_id = 8;
	event.new_epoch = 7;
	event.reconfig_kind = RECONFIG_KIND_FAIL_STOP;
	event.dead_bitmap[0] = 0x01;
	cluster_reconfig_publish_event(&event);

	memset(&eligibility, 0xA5, sizeof(eligibility));
	UT_ASSERT(cluster_reconfig_thread_recovery_eligibility_consume(
		1, &eligibility));
	UT_ASSERT_EQ(eligibility.origin_thread, 1);
	UT_ASSERT_EQ(eligibility.attempt_stamp, 7);
	UT_ASSERT(memcmp(&eligibility.duty, &ut_recovery_root_identity,
				 sizeof(eligibility.duty)) == 0);

	event.dead_bitmap[0] = 0;
	cluster_reconfig_publish_event(&event);
	memset(&eligibility, 0xA5, sizeof(eligibility));
	UT_ASSERT(!cluster_reconfig_thread_recovery_eligibility_consume(
		1, &eligibility));
	UT_ASSERT(memcmp(&eligibility,
				 &(ClusterThreadRecLaunchEligibility){ 0 },
				 sizeof(eligibility)) == 0);
	UT_ASSERT(!cluster_reconfig_thread_recovery_eligibility_consume(1, NULL));
}

UT_TEST(test_rejoin_observed_slot_getter_is_coherent)
{
	uint64 incarnation = UINT64_MAX;
	uint64 generation = UINT64_MAX;

	ut_join_setup();
	UT_ASSERT(!cluster_reconfig_get_observed_slot_coherent(
		1, &incarnation, &generation));
	UT_ASSERT_EQ(incarnation, 0);
	UT_ASSERT_EQ(generation, 0);
	cluster_reconfig_record_observed_slot(1, UINT64_C(77), UINT64_C(9),
										  UINT64_C(12));
	UT_ASSERT(cluster_reconfig_get_observed_slot_coherent(
		1, &incarnation, &generation));
	UT_ASSERT_EQ(incarnation, UINT64_C(77));
	UT_ASSERT_EQ(generation, UINT64_C(9));
}

UT_TEST(test_rejoin_failure_snapshot_requires_exact_nonempty_survivors_and_floor)
{
	ClusterReconfigRejoinFailureSnapshotV1 failure;
	ClusterReconfigRejoinFailureSnapshotV1 zero;
	ReconfigEvent event;

	ut_join_setup();
	ut_declared_set[1] = true;
	ut_declared_set[2] = true;
	cluster_membership_set_state(0, CLUSTER_MEMBER_MEMBER);
	cluster_membership_set_state(1, CLUSTER_MEMBER_DEAD);
	cluster_membership_set_state(2, CLUSTER_MEMBER_MEMBER);
	cluster_membership_record_admitted(1, UINT64_C(70));
	memset(&event, 0, sizeof(event));
	event.event_id = UINT64_C(501);
	event.new_epoch = UINT64_C(9);
	event.cssd_dead_generation = UINT64_C(4);
	event.dead_bitmap[0] = UINT8_C(0x02);
	event.reconfig_kind = RECONFIG_KIND_FAIL_STOP;
	cluster_reconfig_publish_event(&event);

	UT_ASSERT(cluster_reconfig_rejoin_failure_snapshot(
		1, UINT64_C(70), &failure));
	UT_ASSERT_EQ(failure.reconfig_kind, RECONFIG_KIND_FAIL_STOP);
	UT_ASSERT_EQ(failure.event_id, UINT64_C(501));
	UT_ASSERT_EQ(failure.new_epoch, UINT64_C(9));
	UT_ASSERT_EQ(failure.cssd_dead_generation, UINT64_C(4));
	UT_ASSERT_EQ(failure.dead_bitmap[0], UINT8_C(0x02));
	UT_ASSERT_EQ(failure.survivor_bitmap[0], UINT8_C(0x05));
	UT_ASSERT_EQ(failure.old_node_id, 1);
	UT_ASSERT_EQ(failure.old_incarnation, UINT64_C(70));

	memset(&failure, 0xa5, sizeof(failure));
	memset(&zero, 0, sizeof(zero));
	UT_ASSERT(!cluster_reconfig_rejoin_failure_snapshot(
		1, UINT64_C(69), &failure));
	UT_ASSERT(memcmp(&failure, &zero, sizeof(failure)) == 0);
}

UT_TEST(test_rejoin_pending_snapshot_requires_exact_singleton_lineage)
{
	ClusterReconfigState *state;
	ClusterReconfigRejoinFailureSnapshotV1 failure;
	ClusterReconfigRejoinPendingSnapshotV1 pending;
	ClusterReconfigRejoinPendingSnapshotV1 zero;
	ReconfigEvent event;
	uint64 incarnations[CLUSTER_MAX_NODES];

	ut_join_setup();
	state = (ClusterReconfigState *)reconfig_shmem_storage;
	ut_declared_set[1] = true;
	cluster_membership_set_state(0, CLUSTER_MEMBER_MEMBER);
	cluster_membership_set_state(1, CLUSTER_MEMBER_DEAD);
	cluster_membership_record_admitted(1, UINT64_C(70));
	memset(&event, 0, sizeof(event));
	event.event_id = UINT64_C(501);
	event.new_epoch = UINT64_C(9);
	event.cssd_dead_generation = UINT64_C(4);
	event.dead_bitmap[0] = UINT8_C(0x02);
	event.reconfig_kind = RECONFIG_KIND_FAIL_STOP;
	cluster_reconfig_publish_event(&event);
	UT_ASSERT(cluster_reconfig_rejoin_failure_snapshot(
		1, UINT64_C(70), &failure));

	cluster_membership_set_state(1, CLUSTER_MEMBER_JOINING);
	state->pending_join_bitmap[0] = UINT8_C(0x02);
	cluster_reconfig_record_observed_slot(1, UINT64_C(77), UINT64_C(11),
										  UINT64_C(10));
	memset(incarnations, 0, sizeof(incarnations));
	incarnations[1] = UINT64_C(77);
	memset(&event, 0, sizeof(event));
	event.old_epoch = UINT64_C(9);
	event.new_epoch = UINT64_C(10);
	event.cssd_dead_generation = UINT64_C(5);
	event.join_bitmap[0] = UINT8_C(0x02);
	event.reconfig_kind = RECONFIG_KIND_JOIN_PENDING;
	event.event_id = cluster_reconfig_compute_event_id_v2(
		RECONFIG_KIND_JOIN_PENDING, event.dead_bitmap, event.join_bitmap,
		incarnations, event.cssd_dead_generation);
	cluster_reconfig_publish_event(&event);

	UT_ASSERT(cluster_reconfig_rejoin_pending_snapshot(
		&failure, UINT64_C(77), &pending));
	UT_ASSERT_EQ(pending.reconfig_kind, RECONFIG_KIND_JOIN_PENDING);
	UT_ASSERT_EQ(pending.old_epoch, UINT64_C(9));
	UT_ASSERT_EQ(pending.new_epoch, UINT64_C(10));
	UT_ASSERT_EQ(pending.join_bitmap[0], UINT8_C(0x02));
	UT_ASSERT_EQ(pending.node_id, 1);
	UT_ASSERT_EQ(pending.candidate_incarnation, UINT64_C(77));
	UT_ASSERT_EQ(pending.observed_slot_generation, UINT64_C(11));

	event.dead_bitmap[0] = UINT8_C(0x02);
	cluster_reconfig_publish_event(&event);
	memset(&pending, 0xa5, sizeof(pending));
	memset(&zero, 0, sizeof(zero));
	UT_ASSERT(!cluster_reconfig_rejoin_pending_snapshot(
		&failure, UINT64_C(77), &pending));
	UT_ASSERT(memcmp(&pending, &zero, sizeof(pending)) == 0);
}

UT_TEST(test_external_rejoin_consumes_exact_candidate_before_jcmk_submit)
{
	ClusterReconfigState *state;
	ReconfigEvent event;
	ReconfigEvent pending_event;
	ClusterReconfigRejoinPendingSnapshotV1 exact_pending;
	ClusterJoinCommitMarker submitted;
	int32 target = -1;
	uint8 slot[CLUSTER_VOTING_SLOT_BYTES];

	ut_join_setup();
	cluster_online_join = true;
	ut_external_fence_active = true;
	ut_in_quorum_value = true;
	ut_dead_generation = UINT64_C(9);
	ut_declared_set[1] = true;
	ut_peer_state[1] = CLUSTER_CSSD_PEER_DEAD;
	state = (ClusterReconfigState *) reconfig_shmem_storage;
	state->self_join_admitted = 1;
	cluster_membership_set_state(0, CLUSTER_MEMBER_MEMBER);
	cluster_membership_set_state(1, CLUSTER_MEMBER_DEAD);
	cluster_membership_record_admitted(1, UINT64_C(70));
	/* implementation review (2026-08-20): the UNDECIDED preservation is a
	 * conjunction — self must be a genuine formed member whose admitted
	 * floor exactly equals the current self incarnation (77). */
	cluster_membership_record_admitted(0, UINT64_C(77));

	epoch_init_done = false;
	cluster_epoch_shmem_init();
	UT_ASSERT(cluster_epoch_observe_remote(UINT64_C(9)));
	memset(&event, 0, sizeof(event));
	event.coordinator_node_id = 0;
	event.old_epoch = UINT64_C(8);
	event.new_epoch = UINT64_C(9);
	event.cssd_dead_generation = UINT64_C(9);
	event.dead_bitmap[0] = UINT8_C(0x02);
	event.reconfig_kind = RECONFIG_KIND_FAIL_STOP;
	event.event_id = cluster_reconfig_compute_event_id(
		event.dead_bitmap, event.cssd_dead_generation);
	cluster_reconfig_publish_event(&event);

	memset(&ut_rejoin_offer, 0, sizeof(ut_rejoin_offer));
	memset(ut_rejoin_offer.operation_id, 0x33,
		   sizeof(ut_rejoin_offer.operation_id));
	ut_rejoin_offer.old_node_id = 1;
	ut_rejoin_offer.old_incarnation = UINT64_C(70);
	ut_rejoin_offer.candidate_incarnation = UINT64_C(77);
	memset(&ut_recovery_root_identity, 0,
		   sizeof(ut_recovery_root_identity));
	ut_recovery_root_identity.system_identifier = UINT64_C(0x1234);
	ut_recovery_root_identity.origin_thread_id = 2;
	ut_recovery_root_identity.origin_node_id = 1;
	ut_recovery_root_identity.origin_owner_incarnation = UINT64_C(70);
	ut_recovery_root_identity.root_lineage_seq = UINT64_C(8);
	ut_recovery_root_result = CLUSTER_CONTROL_ROOT_OK_PRIMARY;
	ut_rejoin_root_complete = false;
	ut_rejoin_grd_clear_ready = false;
	MyBackendType = B_LMON;

	/* Claim and attach the offered operation to fixed slot[1].  Incomplete G
	 * is a retry cut: it cannot destroy the offer, authorize ON or submit JCMK. */
	cluster_reconfig_lmon_tick();
	UT_ASSERT_EQ(ut_rejoin_start_calls, 1);
	UT_ASSERT_EQ(cluster_epoch_get_current(), UINT64_C(9));
	cluster_reconfig_lmon_tick();
	UT_ASSERT_EQ(cluster_epoch_get_current(), UINT64_C(9));
	UT_ASSERT_EQ(ut_rejoin_clear_build_calls, 0);
	UT_ASSERT_EQ(ut_rejoin_authorize_calls, 0);
	UT_ASSERT_EQ(ut_rejoin_start_calls, 1);

	/* Exact G now becomes available and produces A. */
	ut_rejoin_grd_clear_ready = true;
	cluster_reconfig_lmon_tick();
	UT_ASSERT_EQ(cluster_epoch_get_current(), UINT64_C(9));
	UT_ASSERT_EQ(ut_rejoin_clear_build_calls, 1);
	UT_ASSERT_EQ(ut_rejoin_failure_seen.old_node_id, 1);
	UT_ASSERT_EQ(ut_rejoin_failure_seen.old_incarnation, UINT64_C(70));

	/* B-own root is still not COMPLETE: preserve A/offer and keep ON/JCMK at
	 * zero.  The same slot advances only after the exact root becomes COMPLETE. */
	cluster_reconfig_lmon_tick();
	UT_ASSERT_EQ(ut_rejoin_authorize_calls, 0);
	memset(slot, 0, sizeof(slot));
	UT_ASSERT(!ut_join_qvotec_poll_write_pending(&target, slot));
	ut_rejoin_root_complete = true;
	cluster_reconfig_lmon_tick();
	UT_ASSERT_EQ(ut_rejoin_authorize_calls, 1);

	/* The candidate becomes visible only after physical ON.  The active path
	 * may now publish exact singleton P, whose dead bitmap excludes B-old while
	 * membership remains JOINING and the durable fence marker still retains it. */
	ut_peer_state[1] = CLUSTER_CSSD_PEER_ALIVE;
	cluster_reconfig_record_observed_slot(1, UINT64_C(77), UINT64_C(11),
										  UINT64_C(9));
	cluster_reconfig_lmon_tick();
	UT_ASSERT_EQ(cluster_epoch_get_current(), UINT64_C(10));
	memset(slot, 0, sizeof(slot));
	UT_ASSERT(ut_join_qvotec_poll_write_pending(&target, slot));
	UT_ASSERT_EQ(target, 1);
	ut_join_qvotec_complete_write(true);
	cluster_reconfig_lmon_tick();
	UT_ASSERT_EQ(cluster_epoch_get_current(), UINT64_C(10));
	cluster_reconfig_get_last_event(&pending_event);
	UT_ASSERT_EQ((int) pending_event.reconfig_kind,
				 (int) RECONFIG_KIND_JOIN_PENDING);
	UT_ASSERT_EQ(pending_event.old_epoch,
				 ut_rejoin_failure_seen.new_epoch);
	UT_ASSERT_EQ(pending_event.dead_bitmap[0], 0);
	UT_ASSERT_EQ(pending_event.join_bitmap[0], UINT8_C(0x02));
	memset(&exact_pending, 0, sizeof(exact_pending));
	UT_ASSERT(cluster_reconfig_rejoin_pending_snapshot(
		&ut_rejoin_failure_seen, UINT64_C(77), &exact_pending));
	UT_ASSERT(cluster_reconfig_rejoin_pending_ready(&exact_pending));

	/* Refresh reaches READY on a later poll.  The final root revalidate and
	 * one-shot consume happen after C is fully built but before qvotec can see
	 * any COMMITTED marker request. */
	cluster_reconfig_lmon_tick();
	UT_ASSERT_EQ(ut_rejoin_refresh_calls, 1);
	cluster_reconfig_lmon_tick();
	UT_ASSERT_EQ(ut_rejoin_root_revalidate_calls, 1);
	UT_ASSERT_EQ(ut_rejoin_consume_calls, 1);
	UT_ASSERT(!ut_rejoin_consume_saw_prior_marker_submit);
	UT_ASSERT_EQ(ut_rejoin_pending_seen.node_id, 1);
	UT_ASSERT_EQ(ut_rejoin_pending_seen.candidate_incarnation,
				 UINT64_C(77));
	UT_ASSERT_EQ(ut_rejoin_committed_seen.node_id, 1);
	UT_ASSERT_EQ(ut_rejoin_committed_seen.phase,
				 CLUSTER_JCMK_PHASE_COMMITTED);
	UT_ASSERT_EQ(ut_rejoin_committed_seen.admitted_incarnation,
				 UINT64_C(77));
	UT_ASSERT_EQ(ut_rejoin_committed_seen.generation, UINT64_C(77));
	UT_ASSERT_EQ(ut_rejoin_committed_seen.admitted_epoch,
				 pending_event.new_epoch + 1);
	UT_ASSERT(ut_rejoin_committed_seen.commit_nonce != 0);
	memset(slot, 0, sizeof(slot));
	UT_ASSERT(ut_join_qvotec_poll_write_pending(&target, slot));
	memcpy(&submitted, slot, sizeof(submitted));
	UT_ASSERT(memcmp(&submitted, &ut_rejoin_committed_seen,
				 sizeof(submitted)) == 0);
}

/* ======================================================================
 * RF-ROOT P9 verification / cold-formation cold-formation — three negative
 * legs (implementation acceptance order, after the focused formation unit + t243 +
 * t274): the state machine is driven directly (cluster_reconfig_cold_
 * formation_tick is the lmon_tick sub-step, extern) with qvotec's
 * observation/mailbox publications mocked, so each failure mode is
 * deterministic.
 *
 *   leg 1: marker WRITER crashes mid-write — the surviving non-arbiter
 *          admits from the COMMITTED marker already durable in its own
 *          region-7 slot, with zero dependence on the dead writer's
 *          completion (takeover of the admission, not of the write).
 *   leg 2: a LIVE SURVIVOR (fresh slot past INITIAL) refuses cold
 *          formation — the window stays SURVIVOR, no marker is ever
 *          submitted, self never admits through this path.
 *   leg 3: a DIVERGENT marker (self incarnation mismatch) is rejected
 *          by the non-arbiter, and an arbiter whose marker write FAILED
 *          to reach quorum-majority readback never admits — it resets
 *          and re-submits with a fresh nonce until the readback proves.
 * ====================================================================== */

extern void cluster_reconfig_test_reset_cold_formation(void);

/* Both declared nodes fresh at INITIAL (A-semantics co-boot), local epoch
 * recovered past INITIAL: the observation-window precondition. */
static void
ut_cold_formation_setup_coboot(void)
{
	ut_join_setup();		   /* self = 0, epoch reset to INITIAL */
	cluster_node_id = 1;	   /* the higher node: never the arbiter */
	ut_declared_set[0] = true; /* both declared: {0, 1} */
	ut_in_quorum_value = true;
	UT_ASSERT(cluster_epoch_observe_remote(UINT64_C(5))); /* past INITIAL */
	cluster_reconfig_record_observed_slot(0, UINT64_C(66), UINT64_C(1), 0);
	cluster_reconfig_record_observed_fresh_alive(0, true);
	cluster_reconfig_test_reset_cold_formation();
}

/* A quorum-majority formation marker is membership evidence, but it cannot
 * bypass the shared PGXA/own-PGXS/JOIN-WAL admission prerequisite.  HOLD must
 * leave both the membership floor and ordinary write gate untouched; the same
 * exact marker may be consumed only after the gate reaches PROCEED. */
UT_TEST(test_formation_marker_admission_waits_for_stripe)
{
	ClusterReconfigState *state;
	ClusterFormationCommitMarker marker;
	uint64 incs[CLUSTER_MAX_NODES] = {0};
	int t;

	cluster_online_join = false;
	ut_cold_formation_setup_coboot();
	state = (ClusterReconfigState *)reconfig_shmem_storage;
	state->self_join_admitted = 0;
	memset(&marker, 0, sizeof(marker));
	marker.magic = CLUSTER_FORMATION_MARKER_MAGIC;
	marker.version = CLUSTER_FORMATION_MARKER_VERSION;
	marker.phase = CLUSTER_FORMATION_MARKER_PHASE_COMMITTED;
	marker.formation_generation = UINT64_C(4);
	marker.formation_epoch = UINT64_C(5);
	marker.arbiter_node = 0;
	marker.arbiter_incarnation = UINT64_C(66);
	marker.commit_nonce = UINT64_C(1);
	marker.n_admitted = 2;
	marker.admitted_nodes[0] = UINT8_C(0x03);
	incs[0] = UINT64_C(66);
	incs[1] = UINT64_C(77);
	cluster_reconfig_formation_qvotec_publish_observed(&marker, incs);

	ut_xid_stripe_verdict = CLUSTER_XID_STRIPE_JOIN_HOLD;
	for (t = 0; t < 3; t++)
		cluster_reconfig_cold_formation_tick();
	UT_ASSERT_EQ(ut_xid_stripe_join_gate_calls, 1);
	UT_ASSERT(!ut_xid_stripe_last_may_seed);
	UT_ASSERT_EQ((int)cluster_membership_get_state(1),
				 (int)CLUSTER_MEMBER_ABSENT);
	UT_ASSERT_EQ(cluster_membership_get_last_admitted_incarnation(1),
				 UINT64_C(0));
	UT_ASSERT_EQ(state->self_join_admitted, 0);

	ut_xid_stripe_verdict = CLUSTER_XID_STRIPE_JOIN_PROCEED;
	cluster_reconfig_cold_formation_tick();
	UT_ASSERT_EQ(ut_xid_stripe_join_gate_calls, 2);
	UT_ASSERT(!ut_xid_stripe_last_may_seed);
	UT_ASSERT_EQ((int)cluster_membership_get_state(1),
				 (int)CLUSTER_MEMBER_MEMBER);
	UT_ASSERT_EQ(cluster_membership_get_last_admitted_incarnation(1),
				 UINT64_C(77));
	UT_ASSERT_EQ(state->self_join_admitted, 1);
}

UT_TEST(test_cold_formation_leg1_writer_crash_midwrite_survivor_admits)
{
	ClusterReconfigState *state;
	ClusterFormationCommitMarker marker;
	uint64		incs[CLUSTER_MAX_NODES] = {0};
	int			t;

	ut_cold_formation_setup_coboot();
	state = (ClusterReconfigState *) reconfig_shmem_storage;

	/*
	 * The arbiter (node 0, lowest co-boot node) submitted the COMMITTED
	 * marker and its qvotec wrote it into EVERY target member's region-7
	 * slot; then node 0 crashed BEFORE its own completion/admission.
	 * qvotec on THIS node already read the marker back from OUR OWN slot
	 * and published the observation (the exact marker the dead writer
	 * built — including OUR current incarnation 77).
	 */
	marker.magic = CLUSTER_FORMATION_MARKER_MAGIC;
	marker.version = CLUSTER_FORMATION_MARKER_VERSION;
	marker.phase = CLUSTER_FORMATION_MARKER_PHASE_COMMITTED;
	marker.formation_generation = UINT64_C(4);
	marker.formation_epoch = UINT64_C(5);
	marker.arbiter_node = 0;
	marker.arbiter_incarnation = UINT64_C(66);
	marker.commit_nonce = UINT64_C(1);
	marker.n_admitted = 2;
	marker.admitted_nodes[0] = UINT8_C(0x03); /* nodes 0 and 1 */
	incs[0] = UINT64_C(66);
	incs[1] = UINT64_C(77); /* EXACTLY this node's current incarnation */
	cluster_reconfig_formation_qvotec_publish_observed(&marker, incs);

	/* Window warm-up (2 ticks) then pass (tick 3) — the dead writer's slot
	 * is still fresh, so the window stays COBOOT and the marker-wait
	 * branch is reached.  No re-write by the survivor: it was never the
	 * arbiter and must not need to be. */
	for (t = 0; t < 3; t++)
		cluster_reconfig_cold_formation_tick();

	UT_ASSERT_EQ((int)cluster_membership_get_state(1),
				 (int)CLUSTER_MEMBER_MEMBER);
	UT_ASSERT_EQ(cluster_membership_get_last_admitted_incarnation(1),
				 UINT64_C(77)); /* D13 exact floor, from the marker */
	UT_ASSERT_EQ(cluster_membership_get_last_admitted_incarnation(0),
				 UINT64_C(66)); /* the dead arbiter's exact incarnation */
	UT_ASSERT_EQ(state->self_join_admitted, 1);
	/* The survivor never submitted anything (no takeover write): the
	 * durable marker alone carried the admission. */
	UT_ASSERT_EQ(pg_atomic_read_u64(&state->formation_marker_request_seq),
				 UINT64_C(0));

	/* The dead writer's slot ages out: the window goes PENDING, but the
	 * admission is already durable — self stays MEMBER with the exact
	 * floor across further ticks. */
	cluster_reconfig_record_observed_fresh_alive(0, false);
	for (t = 0; t < 5; t++)
		cluster_reconfig_cold_formation_tick();
	UT_ASSERT_EQ((int)cluster_membership_get_state(1),
				 (int)CLUSTER_MEMBER_MEMBER);
	UT_ASSERT_EQ(cluster_membership_get_last_admitted_incarnation(1),
				 UINT64_C(77));
}

UT_TEST(test_initial_clean_snapshot_requires_exact_four_node_marker_and_empty_replacement)
{
	ClusterReconfigState *state;
	ClusterFormationCommitMarker marker;
	ClusterInitialCleanFormationSnapshot snapshot;
	ClusterReplacementEpisode replacement;
	uint64 incarnations[CLUSTER_MAX_NODES] = {0};
	int node;

	ut_join_setup();
	state = (ClusterReconfigState *)reconfig_shmem_storage;
	state->self_join_admitted = 1;
	memset(&marker, 0, sizeof(marker));
	marker.magic = CLUSTER_FORMATION_MARKER_MAGIC;
	marker.version = CLUSTER_FORMATION_MARKER_VERSION;
	marker.phase = CLUSTER_FORMATION_MARKER_PHASE_COMMITTED;
	marker.formation_generation = UINT64_C(3);
	marker.formation_epoch = cluster_epoch_get_current();
	marker.arbiter_node = 0;
	marker.arbiter_incarnation = UINT64_C(60);
	marker.commit_nonce = UINT64_C(71);
	marker.n_admitted = 4;
	marker.admitted_nodes[0] = UINT8_C(0x0f);
	for (node = 0; node < 4; node++) {
		ut_declared_set[node] = true;
		incarnations[node] = UINT64_C(60) + (uint64)node;
		cluster_membership_record_admitted(node, incarnations[node]);
		cluster_membership_set_state(node, CLUSTER_MEMBER_MEMBER);
	}
	cluster_reconfig_formation_qvotec_publish_observed(
		&marker, incarnations);

	memset(&snapshot, 0xa5, sizeof(snapshot));
	UT_ASSERT(cluster_reconfig_snapshot_initial_clean_formation(&snapshot));
	UT_ASSERT_EQ(snapshot.formation_marker_generation, UINT64_C(3));
	UT_ASSERT_EQ(snapshot.formation_epoch, cluster_epoch_get_current());
	UT_ASSERT_EQ(snapshot.members_lo, UINT64_C(0x0f));
	UT_ASSERT_EQ(snapshot.members_hi, UINT64_C(0));
	UT_ASSERT_EQ(snapshot.arbiter_node, UINT64_C(0));
	UT_ASSERT_EQ(snapshot.arbiter_incarnation, UINT64_C(60));
	for (node = 0; node < 4; node++)
		UT_ASSERT_EQ(snapshot.admitted_incarnation[node],
					 incarnations[node]);

	replacement = ut_admitted_replacement_episode(3);
	state->replacement_episode = replacement;
	memset(&snapshot, 0xa5, sizeof(snapshot));
	UT_ASSERT(!cluster_reconfig_snapshot_initial_clean_formation(&snapshot));
	for (node = 0; node < (int)sizeof(snapshot); node++)
		UT_ASSERT_EQ(((uint8 *)&snapshot)[node], UINT8_C(0));
}

UT_TEST(test_initial_clean_snapshot_accepts_exact_initial_quorum_without_marker)
{
	ClusterReconfigState *state;
	ClusterInitialCleanFormationSnapshot snapshot;
	uint64 incarnations[4];
	int node;

	ut_join_setup();
	state = (ClusterReconfigState *)reconfig_shmem_storage;
	state->self_join_admitted = 1;
	ut_in_quorum_value = true;
	ut_set_self_incarnation_sequence(
		UINT64_C(80), UINT64_C(80), UINT64_C(80));
	for (node = 0; node < 4; node++) {
		incarnations[node] = UINT64_C(80) + (uint64)node;
		ut_declared_set[node] = true;
		cluster_reconfig_record_observed_slot(
			node, incarnations[node], UINT64_C(1),
			CLUSTER_EPOCH_INITIAL);
		cluster_reconfig_record_observed_fresh_alive(node, true);
		cluster_membership_record_admitted(node, incarnations[node]);
		cluster_membership_set_state(node, CLUSTER_MEMBER_MEMBER);
	}
	cluster_reconfig_bootstrap_publish_begin();
	cluster_reconfig_bootstrap_publish_in_quorum(true);
	cluster_reconfig_bootstrap_publish_end();

	memset(&snapshot, 0xa5, sizeof(snapshot));
	UT_ASSERT(cluster_reconfig_snapshot_initial_clean_formation(&snapshot));
	UT_ASSERT_EQ(snapshot.formation_marker_generation, UINT64_C(0));
	UT_ASSERT_EQ(snapshot.formation_epoch, CLUSTER_EPOCH_INITIAL);
	UT_ASSERT_EQ(snapshot.members_lo, UINT64_C(0x0f));
	for (node = 0; node < 4; node++)
		UT_ASSERT_EQ(snapshot.admitted_incarnation[node],
					 incarnations[node]);

	cluster_reconfig_bootstrap_publish_begin();
	cluster_reconfig_record_observed_slot(
		2, incarnations[2] + 1, UINT64_C(2),
		CLUSTER_EPOCH_INITIAL);
	cluster_reconfig_bootstrap_publish_in_quorum(true);
	cluster_reconfig_bootstrap_publish_end();
	memset(&snapshot, 0xa5, sizeof(snapshot));
	UT_ASSERT(!cluster_reconfig_snapshot_initial_clean_formation(&snapshot));
	for (node = 0; node < (int)sizeof(snapshot); node++)
		UT_ASSERT_EQ(((uint8 *)&snapshot)[node], UINT8_C(0));
}

UT_TEST(test_cold_formation_leg2_live_survivor_refuses)
{
	ClusterReconfigState *state;
	int			t;

	ut_join_setup();
	cluster_node_id = 1;
	ut_declared_set[0] = true;
	ut_declared_set[1] = true;
	ut_in_quorum_value = true;
	UT_ASSERT(cluster_epoch_observe_remote(UINT64_C(5)));
	/* node 0 is a LIVE survivor: fresh slot publishing the LIVE epoch
	 * (A-semantics: a formed node publishes the real epoch). */
	cluster_reconfig_record_observed_slot(0, UINT64_C(66), UINT64_C(1),
										  UINT64_C(7));
	cluster_reconfig_record_observed_fresh_alive(0, true);
	cluster_reconfig_test_reset_cold_formation();
	state = (ClusterReconfigState *) reconfig_shmem_storage;

	for (t = 0; t < 12; t++)
		cluster_reconfig_cold_formation_tick();

	/* The window must fail-closed as SURVIVOR every tick: no marker was
	 * ever staged, and this node never admitted through the cold-formation
	 * path (a live cluster has no cold formation — the ordinary join/JCMK
	 * chain owns this node). */
	UT_ASSERT_EQ(pg_atomic_read_u64(&state->formation_marker_request_seq),
				 UINT64_C(0));
	UT_ASSERT_EQ((int)cluster_membership_get_state(1),
				 (int)CLUSTER_MEMBER_ABSENT);
	UT_ASSERT_EQ(cluster_membership_get_last_admitted_incarnation(1),
				 UINT64_C(0));
}

UT_TEST(test_cold_formation_leg3_divergent_marker_rejected_no_majority)
{
	ClusterReconfigState *state;
	ClusterFormationCommitMarker marker;
	uint64		incs[CLUSTER_MAX_NODES] = {0};
	int			t;

	/* ---- A. non-arbiter: a DIVERGENT marker (names an OLD incarnation
	 * for self — a stale/divergent generation's image) is rejected; the
	 * node keeps waiting and never admits. ---- */
	ut_cold_formation_setup_coboot();
	state = (ClusterReconfigState *) reconfig_shmem_storage;
	marker.magic = CLUSTER_FORMATION_MARKER_MAGIC;
	marker.version = CLUSTER_FORMATION_MARKER_VERSION;
	marker.phase = CLUSTER_FORMATION_MARKER_PHASE_COMMITTED;
	marker.formation_generation = UINT64_C(2);
	marker.formation_epoch = UINT64_C(5);
	marker.arbiter_node = 0;
	marker.arbiter_incarnation = UINT64_C(66);
	marker.commit_nonce = UINT64_C(1);
	marker.n_admitted = 2;
	marker.admitted_nodes[0] = UINT8_C(0x03);
	incs[0] = UINT64_C(66);
	incs[1] = UINT64_C(55); /* != current self incarnation 77: divergent */
	cluster_reconfig_formation_qvotec_publish_observed(&marker, incs);
	for (t = 0; t < 6; t++)
		cluster_reconfig_cold_formation_tick();
	UT_ASSERT_EQ((int)cluster_membership_get_state(1),
				 (int)CLUSTER_MEMBER_ABSENT);
	UT_ASSERT_EQ(cluster_membership_get_last_admitted_incarnation(1),
				 UINT64_C(0));
	UT_ASSERT_EQ(cluster_membership_get_last_admitted_incarnation(0),
				 UINT64_C(0));

	/* ---- B. arbiter: the marker write did NOT reach quorum-majority
	 * readback (qvotec completes FAILED) — no admission happens; the
	 * arbiter resets and re-submits with a fresh nonce, and only a
	 * later PROVEN write admits. ---- */
	ut_join_setup();		   /* self = 0 = the LOWEST co-boot node */
	cluster_node_id = 0;
	ut_declared_set[0] = true;
	ut_declared_set[1] = true;
	ut_in_quorum_value = true;
	UT_ASSERT(cluster_epoch_observe_remote(UINT64_C(5)));
	cluster_reconfig_record_observed_slot(1, UINT64_C(70), UINT64_C(1), 0);
	cluster_reconfig_record_observed_fresh_alive(1, true);
	cluster_reconfig_test_reset_cold_formation();
	state = (ClusterReconfigState *) reconfig_shmem_storage;

	for (t = 0; t < 3; t++)
		cluster_reconfig_cold_formation_tick();
	UT_ASSERT_EQ(pg_atomic_read_u64(&state->formation_marker_request_seq),
				 UINT64_C(1)); /* submitted once */

	/* qvotec read back fewer than a strict majority of exact images. */
	cluster_reconfig_formation_qvotec_complete(false);
	cluster_reconfig_cold_formation_tick();
	UT_ASSERT_EQ((int)cluster_membership_get_state(0),
				 (int)CLUSTER_MEMBER_ABSENT);
	UT_ASSERT_EQ(cluster_membership_get_last_admitted_incarnation(0),
				 UINT64_C(0));

	/* The failed submission is retried (fresh nonce, same generation
	 * lineage: max observed generation + 1 stays 1). */
	cluster_reconfig_cold_formation_tick();
	UT_ASSERT_EQ(pg_atomic_read_u64(&state->formation_marker_request_seq),
				 UINT64_C(2)); /* re-submitted */
	UT_ASSERT_EQ((int)cluster_membership_get_state(0),
				 (int)CLUSTER_MEMBER_ABSENT);
	UT_ASSERT_EQ(cluster_membership_get_last_admitted_incarnation(0),
				 UINT64_C(0));

	/* The retried write proves on a majority: admission lands with the
	 * exact per-member floors. */
	cluster_reconfig_formation_qvotec_complete(true);
	cluster_reconfig_cold_formation_tick();
	UT_ASSERT_EQ((int)cluster_membership_get_state(0),
				 (int)CLUSTER_MEMBER_MEMBER);
	UT_ASSERT_EQ(cluster_membership_get_last_admitted_incarnation(0),
				 UINT64_C(77));
	UT_ASSERT_EQ(cluster_membership_get_last_admitted_incarnation(1),
				 UINT64_C(70));
	UT_ASSERT_EQ(state->self_join_admitted, 1);
}

static void
stop_reconfig_shared_owners_body(void)
{
	ClusterReconfigState *s;
	const char *domain, *reason;
	uint64 key;
	ut_join_setup();
	s = (ClusterReconfigState *)reconfig_shmem_storage;
	IsUnderPostmaster = true;
	MyBackendType = B_LMON;
	UT_ASSERT_EQ(CLUSTER_NORMAL_STOP_READY, cluster_reconfig_normal_stop_poll(NULL, NULL, NULL));
	MyBackendType = B_CHECKPOINTER;
	UT_ASSERT_EQ(CLUSTER_NORMAL_STOP_INVALID, cluster_reconfig_normal_stop_poll(NULL, NULL, NULL));
	MyBackendType = B_LMON;
	pg_atomic_write_u32(&s->prebump_sync_active, 1);
	UT_ASSERT_EQ(CLUSTER_NORMAL_STOP_PENDING, cluster_reconfig_normal_stop_poll(NULL, NULL, NULL));
	s->pending_join_bitmap[CLUSTER_RECONFIG_DEAD_BITMAP_BYTES - 1] = 128;
	pg_atomic_write_u32(&s->prebump_sync_active, 0);
	UT_ASSERT_EQ(CLUSTER_NORMAL_STOP_PENDING,
				 cluster_reconfig_normal_stop_poll(&domain, &key, &reason));
	UT_ASSERT_EQ(CLUSTER_MAX_NODES - 1, key);
	s->pending_join_bitmap[CLUSTER_RECONFIG_DEAD_BITMAP_BYTES - 1] = 0;
	UT_ASSERT_EQ(CLUSTER_NORMAL_STOP_READY, cluster_reconfig_normal_stop_poll(NULL, NULL, NULL));
	/* Historical cached counters/marker bytes must not be reset to be idle. */
	pg_atomic_write_u64(&s->formation_marker_request_seq, 7);
	pg_atomic_write_u64(&s->formation_marker_completion_seq, 7);
	pg_atomic_write_u64(&s->join_marker_request_seq, 9);
	pg_atomic_write_u64(&s->join_marker_completion_seq, 9);
	UT_ASSERT_EQ(CLUSTER_NORMAL_STOP_READY, cluster_reconfig_normal_stop_poll(NULL, NULL, NULL));
	pg_atomic_write_u32(&s->prebump_sync_active, 1);
	pg_atomic_write_u64(&s->join_marker_completion_seq, 10);
	UT_ASSERT_EQ(CLUSTER_NORMAL_STOP_INVALID,
				 cluster_reconfig_normal_stop_poll(&domain, &key, &reason));
	UT_ASSERT_EQ(0, strcmp(domain, "JOIN_MAILBOX"));
	UT_ASSERT_EQ(10, pg_atomic_read_u64(&s->join_marker_completion_seq));
	ut_join_setup();
	MyBackendType = B_INVALID;
	IsUnderPostmaster = false;
}

static void
stop_reconfig_actual_formation_owner_body(void)
{
	ClusterReconfigState *s;
	ClusterFormationMarkerSubmitRequest request;
	ut_join_setup();
	cluster_node_id = 0;
	ut_declared_set[0] = ut_declared_set[1] = true;
	ut_in_quorum_value = true;
	UT_ASSERT(cluster_epoch_observe_remote(UINT64_C(5)));
	cluster_reconfig_record_observed_slot(1, UINT64_C(70), UINT64_C(1), 0);
	cluster_reconfig_record_observed_fresh_alive(1, true);
	cluster_reconfig_test_reset_cold_formation();
	s = (ClusterReconfigState *)reconfig_shmem_storage;
	IsUnderPostmaster = true;
	MyBackendType = B_LMON;
	for (int i = 0; i < 3; i++)
		cluster_reconfig_cold_formation_tick();
	UT_ASSERT_EQ(1, pg_atomic_read_u64(&s->formation_marker_request_seq));
	UT_ASSERT_EQ(CLUSTER_NORMAL_STOP_PENDING, cluster_reconfig_normal_stop_poll(NULL, NULL, NULL));
	UT_ASSERT(cluster_reconfig_formation_qvotec_poll_pending(&request));
	UT_ASSERT(request.active);
	cluster_reconfig_formation_qvotec_complete(true);
	/* QVOTEC completion is not the arbiter's admission/retirement. */
	UT_ASSERT_EQ(CLUSTER_NORMAL_STOP_PENDING, cluster_reconfig_normal_stop_poll(NULL, NULL, NULL));
	cluster_reconfig_cold_formation_tick();
	UT_ASSERT_EQ(CLUSTER_MEMBER_MEMBER, cluster_membership_get_state(0));
	UT_ASSERT_EQ(1, s->self_join_admitted);
	UT_ASSERT_EQ(CLUSTER_NORMAL_STOP_READY, cluster_reconfig_normal_stop_poll(NULL, NULL, NULL));
	UT_ASSERT_EQ(1, pg_atomic_read_u64(&s->formation_marker_request_seq));
	cluster_reconfig_test_reset_cold_formation();
	ut_join_setup();
	MyBackendType = B_INVALID;
	IsUnderPostmaster = false;
}

static void
stop_reconfig_isolated(void (*body)(void))
{
	pid_t pid;
	int status;
	/* Admission intentionally leaves boot-private evidence. Do not reset
	 * that evidence merely to run another simulated postmaster in this suite. */
	fflush(NULL);
	pid = fork();
	UT_ASSERT(pid >= 0);
	if (pid < 0)
		return;
	if (pid == 0) {
		body();
		fflush(NULL);
		_exit(ut_current_failed ? 1 : 0);
	}
	UT_ASSERT_EQ(pid, waitpid(pid, &status, 0));
	UT_ASSERT(WIFEXITED(status));
	UT_ASSERT_EQ(0, WIFEXITED(status) ? WEXITSTATUS(status) : -1);
}
UT_TEST(test_stop_reconfig_shared_owners)
{
	stop_reconfig_isolated(stop_reconfig_shared_owners_body);
}
UT_TEST(test_stop_reconfig_actual_formation_owner)
{
	stop_reconfig_isolated(stop_reconfig_actual_formation_owner_body);
}

int
main(void)
{
	UT_PLAN(119);
	UT_RUN(test_stop_reconfig_shared_owners);
	UT_RUN(test_stop_reconfig_actual_formation_owner);

	UT_RUN(test_shared_cf_prior_unclean_rejoin_cannot_fall_back_to_cold_bootstrap);

	UT_RUN(test_thread_recovery_eligibility_uses_current_failstop_and_root_duty);
	UT_RUN(test_rejoin_observed_slot_getter_is_coherent);
	UT_RUN(test_rejoin_failure_snapshot_requires_exact_nonempty_survivors_and_floor);
	UT_RUN(test_rejoin_pending_snapshot_requires_exact_singleton_lineage);

	/* T-reconfig-1 */
	UT_RUN(test_reconfig_dead_bitmap_bytes_eq_16);
	UT_RUN(test_reconfig_event_sizeof_bounds);
	UT_RUN(test_reconfig_shmem_size_positive);
	UT_RUN(test_reconfig_shmem_init_idempotent);
	UT_RUN(test_formation_snapshot_no_pgproc_never_blocks_on_reconfig_lock);
	UT_RUN(test_self_join_admitted_no_pgproc_never_blocks_on_reconfig_lock);
	UT_RUN(test_r4_membership_snapshot_captures_exact_current_four_node_view);
	UT_RUN(test_r4_membership_snapshot_fails_closed_on_inexact_member_evidence);
	UT_RUN(test_reconfig_replacement_episode_is_embedded_and_zero_initialized);
	UT_RUN(test_reconfig_publish_increments_apply_counter);
	UT_RUN(test_reconfig_publish_overwrites_event_seq_monotonically);
	UT_RUN(test_reconfig_broadcast_increments_counter);

	/* T-reconfig-9 */
	UT_RUN(test_epoch_observe_remote_advance_from_zero);
	UT_RUN(test_epoch_observe_remote_no_op_equal);
	UT_RUN(test_epoch_observe_remote_no_retreat);
	UT_RUN(test_epoch_observe_remote_monotonic_chain);
	UT_RUN(test_epoch_advance_for_reconfig_pre_post_snapshots);
	UT_RUN(test_epoch_observe_max_jump_constant);
	UT_RUN(test_epoch_changed_at_lsn_set_and_get);

	/* T-reconfig-2 — compute_event_id pure-function (P1.2). */
	UT_RUN(test_reconfig_compute_event_id_deterministic);
	UT_RUN(test_reconfig_compute_event_id_dead_bitmap_sensitivity);
	UT_RUN(test_reconfig_compute_event_id_dead_gen_sensitivity);
	UT_RUN(test_reconfig_builds_exact_replacement_committed_event);
	UT_RUN(test_replacement_purge_request_requires_exact_reserve_prepare_authority);

	/* T-reconfig-3 — lmon_tick dedup. */
	UT_RUN(test_reconfig_lmon_tick_dedups_same_event_id);
	UT_RUN(test_reconfig_lmon_tick_refires_on_dead_gen_bump);
	UT_RUN(test_reconfig_failstop_fence_stage_bump_once_while_pending);
	UT_RUN(test_reconfig_failstop_marker_unions_prior_excluded_with_new_delta);
	UT_RUN(test_reconfig_node_remove_stage_no_false_contest);
	UT_RUN(test_reconfig_node_remove_marker_carries_full_excluded_set);
	UT_RUN(test_clean_leave_preserves_prior_excluded_set);

	/* T-reconfig-4 / 4b — Q2 A'' + I2 + L20 + F11 + empty-dead-set gates. */
	UT_RUN(test_reconfig_lmon_tick_skips_when_not_in_quorum);
	UT_RUN(test_reconfig_lmon_tick_skips_when_disabled);
	UT_RUN(test_reconfig_lmon_tick_skips_on_empty_dead_bitmap);
	UT_RUN(test_reconfig_lmon_tick_undeclared_peer_ignored_F11);

	/* T-reconfig-7 — broadcast vs epoch++ split (P1.3 I7). */
	UT_RUN(test_reconfig_lmon_tick_coordinator_advances_epoch);
	UT_RUN(test_reconfig_lmon_tick_survivor_does_not_advance_epoch);

	/* T-reconfig-8 — ProcessInterrupts D4 I6 guard. */
	UT_RUN(test_reconfig_check_pending_disabled_silent);
	UT_RUN(test_reconfig_check_pending_no_pending_fast_path);
	UT_RUN(test_reconfig_check_pending_idle_absorbs_I6);
	UT_RUN(test_reconfig_check_pending_read_only_xact_absorbs);
	UT_RUN(test_reconfig_check_pending_in_tx_quorum_lost_errors);
	UT_RUN(test_reconfig_check_pending_in_tx_in_quorum_53R60_errors);

	/* spec-5.14 — reconfig_kind round-trip (U6) + verdict matrix (U8). */
	UT_RUN(test_reconfig_kind_field_roundtrip);
	UT_RUN(test_reconfig_classify_verdict_matrix);

	/* spec-5.15 D1 — join-edge detection (U6-U9). */
	UT_RUN(test_join_bitmap_declared_peer_filter);
	UT_RUN(test_initial_absent_peer_is_not_a_runtime_join_edge);
	UT_RUN(test_join_bitmap_dead_edge_not_member);
	UT_RUN(test_join_bitmap_multi_joiner);
	UT_RUN(test_join_bitmap_stale_and_notready_excluded);
	UT_RUN(test_shared_cf_fast_rejoin_evicts_prior_live_incarnation);
	UT_RUN(test_fast_rejoin_control_episode_carries_only_bound_join_to_terminal);
	UT_RUN(test_fast_rejoin_control_episode_lms_generation_loss_fails_closed);
	UT_RUN(test_legacy_online_join_off_does_not_evict_live_member);

	/* spec-5.15 D3 — event_id_v2 kind distinctness (U12). */
	UT_RUN(test_event_id_v2_kind_distinctness);
	UT_RUN(test_reconfig_replacement_grd_basis_uses_immutable_survivors);

	/* spec-5.15 D4 — clean-departed clear for rejoin (U14). */
	UT_RUN(test_clean_departed_clear_for_rejoin);

	/* spec-5.15A replacement MEMBER stays closed even while the ordinary
	 * online-join classifier is live.  Run this before the ordinary lifecycle
	 * test so its process-local first-decision latch is still pristine. */
	UT_RUN(test_ordinary_bootstrap_cannot_open_replacement_admitted_member);
	UT_RUN(test_nonempty_invalid_replacement_episode_blocks_ordinary_openers);
	UT_RUN(test_cold_bootstrap_records_exact_floor_before_opening_gate);
	UT_RUN(test_initial_bootstrap_stripe_gate_publishes_control_member_only);
	UT_RUN(test_late_initial_node_becomes_online_joiner);
	UT_RUN(test_late_joiner_does_not_adopt_stale_peer_epoch);
	UT_RUN(test_late_joiner_epoch_adoption_waits_for_exact_publish_window);
	UT_RUN(test_late_joiner_deadline_starts_only_after_stripe_claim_owned);
	UT_RUN(test_clean_leave_suppresses_late_joiner_classification_and_progress);
	UT_RUN(test_epoch0_late_founder_classifies_only_after_recovery);
	UT_RUN(test_epoch0_current_authority_is_not_reclassified_as_late_founder);
	UT_RUN(test_pre_publish_join_handoff_is_exact_idempotent_and_nonblocking);
	UT_RUN(test_pre_publish_join_handoff_rejects_nonexact_state);
	UT_RUN(test_epoch0_managed_pivot_drives_ordinary_join_owner);
	UT_RUN(test_epoch0_late_founder_evidence_requires_exact_peer_and_pgxa);
	UT_RUN(test_late_joiner_stripe_refuse_is_terminal_fail_closed);
	UT_RUN(test_offpath_member_requires_decided_boot_and_exact_current_floor);
	UT_RUN(test_ordinary_self_floor_drift_and_high_water_fail_closed_then_retry);

	/* spec-5.15 D5 — joiner write-gate lifecycle (INV-J9). */
	UT_RUN(test_self_join_gate_lifecycle);
	UT_RUN(test_online_join_waits_for_own_stripe_claim);
	UT_RUN(test_online_join_waits_for_join_wal);
	UT_RUN(test_replacement_member_publish_is_exact_and_write_closed);
	UT_RUN(test_lmon_preserves_replacement_admitted_member_while_write_closed);
	UT_RUN(test_uniform_open_requires_exact_replacement_episode_and_generation);

	/* spec-5.15 Hardening v1.1 — HF-1 publish-proof + HF-2 bootstrap epoch-proof. */
	UT_RUN(test_reconfig_join_publish_proven_member_quorum);
	UT_RUN(test_reconfig_join_publish_proven_no_member_failclosed);
	UT_RUN(test_reconfig_bootstrap_quorum_epoch_proof);
	UT_RUN(test_reconfig_bootstrap_proof_seqlock);
	UT_RUN(test_reconfig_bootstrap_proof_valid_slot_not_cssd);
	UT_RUN(test_reconfig_bootstrap_proof_stale_slot_failclosed);
	UT_RUN(test_reconfig_region3_mailbox_preserves_v2_and_canonical_v3);
	UT_RUN(test_join_commit_clears_only_owner_after_root_rejoin_gate);
	UT_RUN(test_join_commit_root_gate_failure_keeps_owner_excluded);
	UT_RUN(test_join_commit_fence_ack_failure_keeps_owner_excluded);
	UT_RUN(test_join_pending_preserves_full_prior_excluded_set);
	UT_RUN(test_survivor_join_observation_requires_root_rejoin_gate);
	UT_RUN(test_survivor_join_observation_clears_only_gated_origin);
	UT_RUN(test_survivor_join_observation_waits_for_serving_ready);
	UT_RUN(test_reconfig_region3_mailbox_request_word_is_exact_duplex);
	UT_RUN(test_reconfig_qvotec_lifecycle_double_invalidates_mailboxes);
	UT_RUN(test_reconfig_target_refuses_ready_without_startup_closure_proof);
	UT_RUN(test_reconfig_phase3_observation_sets_only_existing_ready_bit);
	UT_RUN(test_reconfig_builds_admitted_only_from_terminal_same_episode);
	UT_RUN(test_reconfig_lmon_admits_between_identical_terminal_head_reads);
	UT_RUN(test_reconfig_lmon_closed_apply_consumes_recovered_verify_pair);
	UT_RUN(test_reconfig_qvotec_majority_admitted_publishes_member_closed);
	UT_RUN(test_reconfig_lmon_snapshots_only_exact_admitted_certificate);
	UT_RUN(test_reconfig_target_lmon_retransmits_exact_phase3_until_admitted);
	UT_RUN(test_external_rejoin_consumes_exact_candidate_before_jcmk_submit);

	/* RF-ROOT P9 verification / cold-formation cold-formation negative
	 * legs (run last: leg admissions latch process-local state). */
	UT_RUN(test_formation_marker_admission_waits_for_stripe);
	UT_RUN(test_cold_formation_leg1_writer_crash_midwrite_survivor_admits);
	UT_RUN(test_initial_clean_snapshot_requires_exact_four_node_marker_and_empty_replacement);
	UT_RUN(test_initial_clean_snapshot_accepts_exact_initial_quorum_without_marker);
	UT_RUN(test_cold_formation_leg2_live_survivor_refuses);
	UT_RUN(test_cold_formation_leg3_divergent_marker_rejected_no_majority);

	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
