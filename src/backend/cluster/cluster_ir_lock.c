/*-------------------------------------------------------------------------
 *
 * cluster_ir_lock.c
 *	  STOP03 recovery-serialization backend: volatile counters and the typed
 *	  IR release boundary.  The pure full-duty resid encoder lives in
 *	  cluster_ir.c and is standalone-linkable for the unit test.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/backend/cluster/cluster_ir_lock.c
 *
 * NOTES
 *	  This is a pgrac-original file (no derivation from PostgreSQL).
 *	  Spec: spec-s8-stop-03-root-serialization.md §17 (frozen)
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "cluster/cluster_guc.h" /* cluster_ges_request_timeout_ms */
#include "cluster/cluster_external_fence.h"
#include "cluster/cluster_ir.h"
#include "cluster/cluster_lock_acquire.h"
#include "cluster/cluster_recovery_duty.h"
#include "cluster/cluster_shmem.h"
#include "miscadmin.h" /* IsUnderPostmaster */
#include "storage/lock.h"
#include "storage/shmem.h"
#include "utils/timestamp.h"
#include "utils/wait_event.h"
#include "port/atomics.h"
#include "portability/instr_time.h"

/* STOP03 §10.3: volatile observability only; reset at postmaster init. */

typedef struct ClusterIrShared {
	pg_atomic_uint64 grant_count;
	pg_atomic_uint64 busy_count;
	pg_atomic_uint64 retry_count;
	pg_atomic_uint64 revalidate_reject_count;
	pg_atomic_uint64 node_cleanup_wait_count;
	pg_atomic_uint64 release_confirmed_count;
	pg_atomic_uint64 release_unconfirmed_count;
	pg_atomic_uint64 cold_set_grant_count;
	pg_atomic_uint64 capability_denied_count;
	pg_atomic_uint64 native_result_rejected_count;
} ClusterIrShared;

static ClusterIrShared *ir_state = NULL;
static int ir_base_timeout_ms = 600000;

Size
cluster_ir_shmem_size(void)
{
	return MAXALIGN(sizeof(ClusterIrShared));
}

void
cluster_ir_shmem_init(void)
{
	bool found;

	ir_state = (ClusterIrShared *)ShmemInitStruct("pgrac cluster ir",
												  MAXALIGN(sizeof(ClusterIrShared)), &found);
	if (!IsUnderPostmaster) {
		ir_base_timeout_ms
			= (cluster_ges_request_timeout_ms >= 1 && cluster_ges_request_timeout_ms <= 600000)
				  ? cluster_ges_request_timeout_ms
				  : 600000;
		pg_atomic_init_u64(&ir_state->grant_count, 0);
		pg_atomic_init_u64(&ir_state->busy_count, 0);
		pg_atomic_init_u64(&ir_state->retry_count, 0);
		pg_atomic_init_u64(&ir_state->revalidate_reject_count, 0);
		pg_atomic_init_u64(&ir_state->node_cleanup_wait_count, 0);
		pg_atomic_init_u64(&ir_state->release_confirmed_count, 0);
		pg_atomic_init_u64(&ir_state->release_unconfirmed_count, 0);
		pg_atomic_init_u64(&ir_state->cold_set_grant_count, 0);
		pg_atomic_init_u64(&ir_state->capability_denied_count, 0);
		pg_atomic_init_u64(&ir_state->native_result_rejected_count, 0);
	}
}

static const ClusterShmemRegion cluster_ir_region = {
	.name = "pgrac cluster ir",
	.size_fn = cluster_ir_shmem_size,
	.init_fn = cluster_ir_shmem_init,
	.lwlock_count = 0,
	.owner_subsys = "spec-5.7 IR instance-recovery owner",
	.reserved_flags = 0,
};

void
cluster_ir_shmem_register(void)
{
	cluster_shmem_register_region(&cluster_ir_region);
}

#define IR_BUMP(field)                                                                             \
	do {                                                                                           \
		if (ir_state != NULL)                                                                      \
			pg_atomic_fetch_add_u64(&ir_state->field, 1);                                          \
	} while (0)

uint64
cluster_recovery_serial_grant_count(void)
{
	return ir_state != NULL ? pg_atomic_read_u64(&ir_state->grant_count) : 0;
}
uint64
cluster_recovery_serial_busy_count(void)
{
	return ir_state != NULL ? pg_atomic_read_u64(&ir_state->busy_count) : 0;
}
uint64
cluster_recovery_serial_retry_count(void)
{
	return ir_state != NULL ? pg_atomic_read_u64(&ir_state->retry_count) : 0;
}
uint64
cluster_recovery_serial_revalidate_reject_count(void)
{
	return ir_state != NULL ? pg_atomic_read_u64(&ir_state->revalidate_reject_count) : 0;
}
uint64
cluster_recovery_serial_node_cleanup_wait_count(void)
{
	return ir_state != NULL ? pg_atomic_read_u64(&ir_state->node_cleanup_wait_count) : 0;
}
uint64
cluster_recovery_serial_release_confirmed_count(void)
{
	return ir_state != NULL ? pg_atomic_read_u64(&ir_state->release_confirmed_count) : 0;
}
uint64
cluster_recovery_serial_release_unconfirmed_count(void)
{
	return ir_state != NULL ? pg_atomic_read_u64(&ir_state->release_unconfirmed_count) : 0;
}
uint64
cluster_recovery_serial_cold_set_grant_count(void)
{
	return ir_state != NULL ? pg_atomic_read_u64(&ir_state->cold_set_grant_count) : 0;
}
uint64
cluster_recovery_serial_capability_denied_count(void)
{
	return ir_state != NULL ? pg_atomic_read_u64(&ir_state->capability_denied_count) : 0;
}
uint64
cluster_recovery_serial_native_result_rejected_count(void)
{
	return ir_state != NULL ? pg_atomic_read_u64(&ir_state->native_result_rejected_count) : 0;
}

static bool
recovery_serial_root_token_matches_duty(const ClusterControlRootReadToken *token,
										const ClusterRecoveryDutyKey *duty)
{
	uint32 required_flags
		= CLUSTER_CONTROL_ROOT_FLAG_CLAIM_VALID | CLUSTER_CONTROL_ROOT_FLAG_CHECKPOINT_VALID
		  | CLUSTER_CONTROL_ROOT_FLAG_TAIL_VALID | CLUSTER_CONTROL_ROOT_FLAG_RECOVERED_VALID;

	return token != NULL && duty != NULL && token->origin_thread_id == duty->origin_thread_id
		   && token->root_lineage_seq == duty->root_lineage_seq
		   && memcmp(token->authority_uuid, duty->authority_uuid, sizeof(token->authority_uuid))
				  == 0
		   && token->lifecycle == CLUSTER_CONTROL_ROOT_LIFECYCLE_RECOVERY_REQUIRED
		   && token->reserved20 == 0 && token->reserved32 == 0 && token->file_txn_seq != 0
		   && token->root_publish_seq != 0 && token->record_crc32c != 0
		   && (token->root_flags & required_flags) == required_flags;
}

static bool
recovery_serial_request_valid(const ClusterRecoverySerialRequest *request)
{
	return request != NULL
		   && (request->mode == CLUSTER_RECOVERY_SERIAL_ONLINE
			   || request->mode == CLUSTER_RECOVERY_SERIAL_COLD_FORMED)
		   && cluster_recovery_duty_key_valid_v1(&request->duty)
		   && recovery_serial_root_token_matches_duty(&request->expected_root_token, &request->duty)
		   && request->formation != NULL && request->fence_need_set != NULL
		   && request->fence_admission_set != NULL && request->acquire_timeout_ms >= 1
		   && request->acquire_timeout_ms <= 600000 && request->release_timeout_ms >= 1
		   && request->release_timeout_ms <= 600000;
}

static bool
recovery_serial_release_guard_valid(const ClusterRecoverySerialGuard *guard)
{
	ClusterResId expected_resid;

	if (guard == NULL || !guard->held
		|| (guard->mode != CLUSTER_RECOVERY_SERIAL_ONLINE
			&& guard->mode != CLUSTER_RECOVERY_SERIAL_COLD_FORMED)
		|| guard->formation == NULL || guard->fence_need_set == NULL
		|| guard->fence_admission_set == NULL || guard->release_timeout_ms < 1
		|| guard->release_timeout_ms > 600000 || !cluster_recovery_duty_key_valid_v1(&guard->duty)
		|| !cluster_recovery_serial_resid_encode(&guard->duty, &expected_resid)
		|| memcmp(&guard->resid, &expected_resid, sizeof(expected_resid)) != 0
		|| memcmp(&guard->lock_request.resid, &guard->resid, sizeof(guard->resid)) != 0
		|| guard->lock_request.lockmode != ExclusiveLock
		|| guard->lock_request.op != CLUSTER_LOCK_OP_REQUEST
		|| guard->lock_request.current_mode != NoLock
		|| guard->lock_request.lockmethod_id != DEFAULT_LOCKMETHOD || !guard->lock_request.dontwait
		|| guard->lock_request.sessionLock || guard->lock_request.request_id == 0
		|| guard->lock_request.holder.request_id != guard->lock_request.request_id
		|| guard->lock_request.holder.cluster_epoch == 0
		|| !recovery_serial_root_token_matches_duty(&guard->root_read_token, &guard->duty))
		return false;
	return true;
}

static ClusterRecoverySerialReleaseResult
recovery_serial_release_with_timeout(ClusterRecoverySerialGuard *guard, int timeout_ms);

static ClusterRecoverySerialAcquireResult
recovery_serial_preflight(const ClusterRecoverySerialRequest *request)
{
	ClusterControlRootSnapshot snapshot;
	ClusterControlRootReadToken token;
	ClusterControlRootResult root_result;
	PgracExternalFenceDenyReason reason;
	uint32 required_flags
		= CLUSTER_CONTROL_ROOT_FLAG_CLAIM_VALID | CLUSTER_CONTROL_ROOT_FLAG_CHECKPOINT_VALID
		  | CLUSTER_CONTROL_ROOT_FLAG_TAIL_VALID | CLUSTER_CONTROL_ROOT_FLAG_RECOVERED_VALID;

	root_result
		= cluster_control_root_read_canonical(request->duty.origin_thread_id, &request->duty,
											  CLUSTER_CONTROL_ROOT_READ_STRONG, &snapshot, &token);
	if (root_result != CLUSTER_CONTROL_ROOT_OK_PRIMARY
		&& root_result != CLUSTER_CONTROL_ROOT_OK_PRIMARY_DEGRADED)
		return CLUSTER_RECOVERY_SERIAL_ROOT_UNAVAILABLE;
	if (cluster_recovery_duty_key_compare(&snapshot.identity, &request->duty)
			!= CLUSTER_RECOVERY_DUTY_COMPARE_EXACT
		|| snapshot.lifecycle != CLUSTER_CONTROL_ROOT_LIFECYCLE_RECOVERY_REQUIRED
		|| (snapshot.root_flags & required_flags) != required_flags
		|| memcmp(&token, &request->expected_root_token, sizeof(token)) != 0)
		return CLUSTER_RECOVERY_SERIAL_STALE;
	if (cluster_formation_witness_revalidate_nowait(request->formation)
		!= CLUSTER_FORMATION_WITNESS_READY)
		return CLUSTER_RECOVERY_SERIAL_STALE;
	if (!cluster_external_fence_need_set_revalidate_nowait(request->fence_need_set,
														   request->formation, &reason)
		|| !cluster_external_fence_revalidate_set_nowait(
			request->fence_admission_set, request->fence_need_set, request->formation, &reason))
		return CLUSTER_RECOVERY_SERIAL_FENCE_DENIED;
	return CLUSTER_RECOVERY_SERIAL_GRANTED;
}

ClusterRecoverySerialAcquireResult
cluster_recovery_serial_acquire(const ClusterRecoverySerialRequest *request,
								ClusterRecoverySerialGuard *guard)
{
	ClusterRecoverySerialAcquireResult preflight;
	ClusterLockAcquireRequest lock_request;
	ClusterLockAcquireResult lock_result;
	ClusterResId resid;
	PgracExternalFenceDenyReason reason;
	ClusterRecoverySerialRevalidateResult stale_result = CLUSTER_RECOVERY_SERIAL_CURRENT;

	if (guard == NULL)
		return CLUSTER_RECOVERY_SERIAL_INTERNAL_FAILURE;
	memset(guard, 0, sizeof(*guard));
	if (!recovery_serial_request_valid(request))
		return CLUSTER_RECOVERY_SERIAL_INTERNAL_FAILURE;
	preflight = recovery_serial_preflight(request);
	if (preflight != CLUSTER_RECOVERY_SERIAL_GRANTED) {
		if (preflight == CLUSTER_RECOVERY_SERIAL_STALE
			|| preflight == CLUSTER_RECOVERY_SERIAL_ROOT_UNAVAILABLE
			|| preflight == CLUSTER_RECOVERY_SERIAL_CAPABILITY_UNAVAILABLE)
			IR_BUMP(capability_denied_count);
		return preflight;
	}
	if (!cluster_recovery_serial_resid_encode(&request->duty, &resid))
		return CLUSTER_RECOVERY_SERIAL_INTERNAL_FAILURE;

	memset(&lock_request, 0, sizeof(lock_request));
	lock_request.resid = resid;
	lock_request.lockmode = ExclusiveLock;
	lock_request.op = CLUSTER_LOCK_OP_REQUEST;
	lock_request.current_mode = NoLock;
	lock_request.lockmethod_id = DEFAULT_LOCKMETHOD;
	lock_request.dontwait = true;
	lock_request.sessionLock = false;
	lock_request.recovery_bootstrap = true;
	lock_request.caller_local_start_ts_ms = (uint64)(GetCurrentTimestamp() / 1000);
	lock_request.timeout_ms = Min(request->acquire_timeout_ms, ir_base_timeout_ms);
	lock_request.wait_event = WAIT_EVENT_CLUSTER_GES_REPLY_WAIT;
	lock_result = cluster_lock_acquire_seven_step(&lock_request);

	if (lock_result == CLUSTER_LOCK_ACQUIRE_NOT_AVAIL) {
		IR_BUMP(busy_count);
		return CLUSTER_RECOVERY_SERIAL_BUSY;
	}
	if (lock_result == CLUSTER_LOCK_ACQUIRE_OK_NATIVE) {
		IR_BUMP(native_result_rejected_count);
		IR_BUMP(retry_count);
		return CLUSTER_RECOVERY_SERIAL_RETRY;
	}
	if (lock_result == CLUSTER_LOCK_ACQUIRE_NEED_PG_NATIVE_LOCK)
		lock_result = cluster_lock_acquire_s5_promote(&lock_request);
	if (lock_result != CLUSTER_LOCK_ACQUIRE_OK_GRANTED
		&& lock_result != CLUSTER_LOCK_ACQUIRE_OK_CONVERTED) {
		IR_BUMP(retry_count);
		return CLUSTER_RECOVERY_SERIAL_RETRY;
	}

	guard->held = true;
	guard->release_uncertain = false;
	guard->mode = request->mode;
	guard->resid = resid;
	guard->duty = request->duty;
	guard->root_read_token = request->expected_root_token;
	guard->formation = request->formation;
	guard->fence_need_set = request->fence_need_set;
	guard->fence_admission_set = request->fence_admission_set;
	guard->lock_request = lock_request;
	guard->release_timeout_ms = request->release_timeout_ms;

	/* Post-grant checks are strictly no-wait: the authority holder must not
	 * cross its first mutation gate on a proof that expired while IR was being
	 * registered. */
	if (cluster_formation_witness_revalidate_nowait(guard->formation)
			!= CLUSTER_FORMATION_WITNESS_READY
		|| !cluster_external_fence_need_set_revalidate_nowait(guard->fence_need_set,
															  guard->formation, &reason))
		stale_result = CLUSTER_RECOVERY_SERIAL_MEMBERSHIP_STALE;
	else if (!cluster_external_fence_revalidate_set_nowait(
				 guard->fence_admission_set, guard->fence_need_set, guard->formation, &reason))
		stale_result = CLUSTER_RECOVERY_SERIAL_FENCE_STALE;
	if (stale_result != CLUSTER_RECOVERY_SERIAL_CURRENT) {
		ClusterRecoverySerialReleaseResult release_result
			= recovery_serial_release_with_timeout(guard, guard->release_timeout_ms);

		IR_BUMP(revalidate_reject_count);
		if (release_result != CLUSTER_RECOVERY_SERIAL_RELEASE_CONFIRMED)
			return CLUSTER_RECOVERY_SERIAL_INTERNAL_FAILURE;
		memset(guard, 0, sizeof(*guard));
		return stale_result == CLUSTER_RECOVERY_SERIAL_MEMBERSHIP_STALE
				   ? CLUSTER_RECOVERY_SERIAL_STALE
				   : CLUSTER_RECOVERY_SERIAL_FENCE_DENIED;
	}
	IR_BUMP(grant_count);
	return CLUSTER_RECOVERY_SERIAL_GRANTED;
}

ClusterRecoverySerialRevalidateResult
cluster_recovery_serial_revalidate(ClusterRecoverySerialGuard *guard)
{
	if (guard != NULL && guard->release_uncertain) {
		IR_BUMP(revalidate_reject_count);
		return CLUSTER_RECOVERY_SERIAL_RELEASE_UNCERTAIN;
	}
	if (guard == NULL || !guard->held)
		return CLUSTER_RECOVERY_SERIAL_NOT_HELD;
	if (!recovery_serial_release_guard_valid(guard)) {
		IR_BUMP(revalidate_reject_count);
		return CLUSTER_RECOVERY_SERIAL_FENCE_STALE;
	}

	if (cluster_formation_witness_revalidate_nowait(guard->formation)
			!= CLUSTER_FORMATION_WITNESS_READY
		|| !cluster_external_fence_need_set_revalidate_nowait(
			guard->fence_need_set, guard->formation,
			&(PgracExternalFenceDenyReason){ PGRAC_EXTERNAL_FENCE_DENY_NONE })) {
		IR_BUMP(revalidate_reject_count);
		return CLUSTER_RECOVERY_SERIAL_MEMBERSHIP_STALE;
	}
	if (!cluster_external_fence_revalidate_set_nowait(
			guard->fence_admission_set, guard->fence_need_set, guard->formation,
			&(PgracExternalFenceDenyReason){ PGRAC_EXTERNAL_FENCE_DENY_NONE })) {
		IR_BUMP(revalidate_reject_count);
		return CLUSTER_RECOVERY_SERIAL_FENCE_STALE;
	}
	return CLUSTER_RECOVERY_SERIAL_CURRENT;
}

ClusterRecoverySerialAcquireResult
cluster_recovery_serial_acquire_set(const ClusterRecoverySerialRequest *requests, uint16 count,
									int overall_acquire_timeout_ms,
									ClusterRecoverySerialGuardSet *set, uint16 *failed_index)
{
	instr_time started;
	uint16 i;

	if (failed_index != NULL)
		*failed_index = CLUSTER_RECOVERY_SERIAL_SET_FAILED_NONE;
	if (set == NULL)
		return CLUSTER_RECOVERY_SERIAL_INTERNAL_FAILURE;
	memset(set, 0, sizeof(*set));
	if (failed_index == NULL || requests == NULL || count == 0
		|| count > CLUSTER_RECOVERY_SERIAL_SET_MAX || overall_acquire_timeout_ms < 1
		|| overall_acquire_timeout_ms > 600000)
		return CLUSTER_RECOVERY_SERIAL_INTERNAL_FAILURE;
	for (i = 0; i < count; i++) {
		if (!recovery_serial_request_valid(&requests[i])
			|| (i > 0 && requests[i - 1].duty.origin_thread_id >= requests[i].duty.origin_thread_id)
			|| (i > 0 && requests[i - 1].release_timeout_ms != requests[i].release_timeout_ms))
			return CLUSTER_RECOVERY_SERIAL_INTERNAL_FAILURE;
	}

	INSTR_TIME_SET_CURRENT(started);
	for (i = 0; i < count; i++) {
		ClusterRecoverySerialRequest bounded_request = requests[i];
		ClusterRecoverySerialGuard guard;
		ClusterRecoverySerialAcquireResult result;
		instr_time now;
		double elapsed_ms;
		int elapsed_ms_ceil;
		int remaining_ms;

		INSTR_TIME_SET_CURRENT(now);
		INSTR_TIME_SUBTRACT(now, started);
		elapsed_ms = INSTR_TIME_GET_MILLISEC(now);
		elapsed_ms_ceil = (int)elapsed_ms;
		if ((double)elapsed_ms_ceil < elapsed_ms)
			elapsed_ms_ceil++;
		remaining_ms = overall_acquire_timeout_ms - elapsed_ms_ceil;
		if (remaining_ms <= 0) {
			result = CLUSTER_RECOVERY_SERIAL_RETRY;
		} else {
			bounded_request.acquire_timeout_ms
				= Min(bounded_request.acquire_timeout_ms, remaining_ms);
			result = cluster_recovery_serial_acquire(&bounded_request, &guard);
		}
		if (result != CLUSTER_RECOVERY_SERIAL_GRANTED) {
			ClusterRecoverySerialReleaseResult rollback_result;

			if (set->count == 0) {
				memset(set, 0, sizeof(*set));
				return result;
			}
			*failed_index = i;
			rollback_result = cluster_recovery_serial_release_set(set);
			if (rollback_result != CLUSTER_RECOVERY_SERIAL_RELEASE_CONFIRMED)
				return CLUSTER_RECOVERY_SERIAL_INTERNAL_FAILURE;
			return result;
		}

		if (set->count == 0)
			set->release_timeout_ms = requests[0].release_timeout_ms;
		set->guards[set->count] = guard;
		set->count++;
	}

	return CLUSTER_RECOVERY_SERIAL_GRANTED;
}

static ClusterRecoverySerialReleaseResult
recovery_serial_release_with_timeout(ClusterRecoverySerialGuard *guard, int timeout_ms)
{
	ClusterLockAcquireRequest release_request;
	ClusterLockAcquireResult result;

	if (guard == NULL)
		return CLUSTER_RECOVERY_SERIAL_RELEASE_INVALID;
	if (!guard->held && !guard->release_uncertain)
		return CLUSTER_RECOVERY_SERIAL_RELEASE_NOT_HELD;
	if (!recovery_serial_release_guard_valid(guard) || timeout_ms < 1 || timeout_ms > 600000)
		return CLUSTER_RECOVERY_SERIAL_RELEASE_INVALID;

	release_request = guard->lock_request;
	release_request.timeout_ms = Min(timeout_ms, ir_base_timeout_ms);
	result = cluster_lock_acquire_s6_release(&release_request);
	if (result != CLUSTER_LOCK_ACQUIRE_OK_GRANTED) {
		guard->release_uncertain = true;
		IR_BUMP(release_unconfirmed_count);
		return CLUSTER_RECOVERY_SERIAL_RELEASE_UNCONFIRMED;
	}

	guard->held = false;
	guard->release_uncertain = false;
	IR_BUMP(release_confirmed_count);
	return CLUSTER_RECOVERY_SERIAL_RELEASE_CONFIRMED;
}

ClusterRecoverySerialReleaseResult
cluster_recovery_serial_release(ClusterRecoverySerialGuard *guard)
{
	if (guard == NULL)
		return CLUSTER_RECOVERY_SERIAL_RELEASE_INVALID;
	return recovery_serial_release_with_timeout(guard, guard->release_timeout_ms);
}

ClusterRecoverySerialReleaseResult
cluster_recovery_serial_release_set(ClusterRecoverySerialGuardSet *set)
{
	instr_time started;
	uint16 original_count;
	uint16 survivors = 0;
	uint16 i;
	bool unconfirmed = false;

	if (set == NULL)
		return CLUSTER_RECOVERY_SERIAL_RELEASE_INVALID;
	if (set->count == 0)
		return CLUSTER_RECOVERY_SERIAL_RELEASE_NOT_HELD;
	if (set->count > CLUSTER_RECOVERY_SERIAL_SET_MAX || set->release_timeout_ms < 1
		|| set->release_timeout_ms > 600000)
		return CLUSTER_RECOVERY_SERIAL_RELEASE_INVALID;
	for (i = 0; i < set->count; i++) {
		if (!recovery_serial_release_guard_valid(&set->guards[i])
			|| set->guards[i].release_timeout_ms != set->release_timeout_ms
			|| (i > 0
				&& set->guards[i - 1].duty.origin_thread_id
					   >= set->guards[i].duty.origin_thread_id))
			return CLUSTER_RECOVERY_SERIAL_RELEASE_INVALID;
	}

	original_count = set->count;
	INSTR_TIME_SET_CURRENT(started);
	for (i = original_count; i > 0; i--) {
		ClusterRecoverySerialGuard *guard = &set->guards[i - 1];
		ClusterRecoverySerialReleaseResult result;
		instr_time now;
		double elapsed_ms;
		int elapsed_ms_ceil;
		int remaining_ms;

		INSTR_TIME_SET_CURRENT(now);
		INSTR_TIME_SUBTRACT(now, started);
		elapsed_ms = INSTR_TIME_GET_MILLISEC(now);
		elapsed_ms_ceil = (int)elapsed_ms;
		if ((double)elapsed_ms_ceil < elapsed_ms)
			elapsed_ms_ceil++;
		remaining_ms = set->release_timeout_ms - elapsed_ms_ceil;
		if (remaining_ms <= 0) {
			guard->release_uncertain = true;
			IR_BUMP(release_unconfirmed_count);
			unconfirmed = true;
			continue;
		}
		result = recovery_serial_release_with_timeout(guard, remaining_ms);
		if (result != CLUSTER_RECOVERY_SERIAL_RELEASE_CONFIRMED)
			unconfirmed = true;
	}

	for (i = 0; i < original_count; i++) {
		if (set->guards[i].held || set->guards[i].release_uncertain) {
			if (survivors != i)
				set->guards[survivors] = set->guards[i];
			survivors++;
		}
	}
	memset(&set->guards[survivors], 0, sizeof(set->guards) - sizeof(set->guards[0]) * survivors);
	set->count = survivors;
	if (unconfirmed || survivors != 0)
		return CLUSTER_RECOVERY_SERIAL_RELEASE_UNCONFIRMED;

	memset(set, 0, sizeof(*set));
	return CLUSTER_RECOVERY_SERIAL_RELEASE_CONFIRMED;
}
