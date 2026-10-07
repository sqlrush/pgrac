/*-------------------------------------------------------------------------
 * cluster_lock_owner.c -- stable manual GES ownership and reconstruction.
 *
 * Portions Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "cluster/cluster_epoch.h"
#include "access/xact.h"
#include "cluster/cluster_cancel_token.h"
#include "cluster/cluster_cf_enqueue.h"
#include "cluster/cluster_lmd_wait_state.h"
#include "cluster/cluster_lock_owner.h"
#include "cluster/cluster_lmon.h"
#include "cluster/cluster_wal_retention.h"
#include "miscadmin.h"
#include "postmaster/startup.h"
#include "storage/ipc.h"
#include "storage/latch.h"
#include "storage/proc.h"
#include "utils/memutils.h"
#include "utils/timestamp.h"
#include "utils/wait_event.h"

static ClusterLockOwner *private_owners;
static bool private_walk_active;
static uint64 owner_sequence;
static ClusterControlRequestOwner control_creator;
static bool lock_owner_release_wait(ClusterLockOwner *owner, int timeout_ms, uint32 event);

static void
lock_owner_exit(int code pg_attribute_unused(), Datum argument pg_attribute_unused())
{
	/* Explicit transfer survives the process. No network wait in exit. */
	cluster_control_request_owner_exit(&control_creator);
	cluster_lmon_wakeup();
}

static bool
lock_owner_creator(void)
{
	if (MyProc == NULL || MyProcPid <= 0)
		return false;
	if (control_creator.incarnation != 0)
		return control_creator.pid == MyProcPid
			   && control_creator.procno == (uint32)MyProc->pgprocno;
	if (!cluster_control_request_owner_init((uint32)MyProc->pgprocno, MyProcPid, &control_creator))
		return false;
	before_shmem_exit(lock_owner_exit, (Datum)0);
	return true;
}

static ClusterLockOwner *
lock_owner_find(uint64 cookie)
{
	ClusterLockOwner *owner;

	if (cookie == 0)
		return NULL;
	for (owner = private_owners; owner != NULL; owner = owner->next)
		if (owner->shared && owner->request.control_owner_id == cookie)
			return owner;
	return NULL;
}

static bool
lock_owner_mark_held(const ClusterControlRequestHandle *handle, const ClusterResId *resid,
					 const ClusterGrdHolderId *holder, LOCKMODE mode)
{
	ClusterControlRequestView view;

	return cluster_control_request_snapshot(handle, &view)
		   && memcmp(&view.message.key.resid, resid, sizeof(*resid)) == 0
		   && view.message.key.holder.node_id == holder->node_id
		   && view.message.key.holder.procno == holder->procno
		   && view.message.key.holder.cluster_epoch == holder->cluster_epoch
		   && view.message.key.holder.request_id == holder->request_id && view.mode == mode
		   && cluster_control_request_mark_held(handle, &control_creator);
}

/* S3 and the conversion bypass call this on the stable request itself.
 * Unknown/copy-only producers cannot publish a control request. */
bool
cluster_lock_owner_request_prepare(ClusterLockAcquireRequest *request)
{
	ClusterLockOwner *owner;
	ClusterControlRequestKey key;
	ClusterControlRequestView view;

	if (request == NULL)
		return false;
	if (!cluster_shared_config || !cluster_control_request_resid_valid(&request->resid))
		return true;
	owner = lock_owner_find(request->control_owner_id);
	if (owner == NULL || request != &owner->request || owner->state != CLUSTER_LOCK_OWNER_ACQUIRING
		|| !lock_owner_creator())
		return false;
	memset(&key, 0, sizeof(key));
	key.resid = request->resid;
	key.holder = request->holder;
	/* Conversion has already registered its exact identity before pausing
	 * the old hold. This is validation, never reactivation of an old key. */
	if (owner->attempt.generation != 0)
		return owner->conversion != CLUSTER_LOCK_OWNER_NO_CONVERSION
			   && cluster_control_request_snapshot(&owner->attempt, &view)
			   && view.state == CLUSTER_CONTROL_REQUEST_ACTIVE && !view.owner_exited
			   && memcmp(&view.message.key, &key, sizeof(key)) == 0
			   && view.mode == request->lockmode
			   && view.message.previous_request == request->convert_old_request_id;
	return cluster_control_request_register(
		&key, request->lockmode, &control_creator, request->convert_old_request_id,
		request->convert_old_request_id ? request->current_mode : NoLock, &owner->attempt);
}

static void
lock_owner_declare_debt(const ClusterControlRequestHandle *handle)
{
	if (cluster_control_request_abandon(handle, &control_creator))
		cluster_lmon_wakeup();
}

static void
lock_owner_abandon(ClusterLockOwner *owner)
{
	if (owner->shared
		&& (owner->conversion == CLUSTER_LOCK_OWNER_UPGRADING
			|| owner->conversion == CLUSTER_LOCK_OWNER_RESTORING)) {
		/* One logical release surrenders both S and X. An earlier in-flight
		 * failed-X retirement must not recreate S after S's own retirement. */
		if (!cluster_control_request_surrender_upgrade(&owner->secondary, &owner->attempt,
													   &control_creator))
			return; /* Keep the conversion owned; no terminal claim. */
		owner->conversion = CLUSTER_LOCK_OWNER_NO_CONVERSION;
	}
	if (owner->shared && owner->conversion == CLUSTER_LOCK_OWNER_REBUILD_RESTORING)
		owner->conversion
			= CLUSTER_LOCK_OWNER_NO_CONVERSION; /* Restore-S intent already revoked. */
	owner->state = CLUSTER_LOCK_OWNER_RETIRING;
	owner->reconstructable = false;
	if (!owner->shared)
		return;
	if (owner->attempt.generation != 0)
		lock_owner_declare_debt(&owner->attempt);
	if (owner->secondary.generation != 0) {
		lock_owner_declare_debt(&owner->secondary);
		owner->secondary_retiring = true;
	}
	if (owner->redeclare.wait_registered)
		cluster_ges_reply_wait_delete(&owner->redeclare.key);
	memset(&owner->redeclare, 0, sizeof(owner->redeclare));
	memset(&owner->target, 0, sizeof(owner->target));
	if (owner->acquisition.exchange.wait_registered)
		cluster_ges_reply_wait_delete(&owner->acquisition.exchange.key);
	memset(&owner->acquisition, 0, sizeof(owner->acquisition));
	if (owner->cooperative_acquire && MyProc != NULL)
		cluster_lmd_wait_state_clear(&MyProc->cluster_lmd_wait);
}

static bool
lock_owner_cut(const ClusterResId *resid, ClusterControlRequestCut *cut)
{
	memset(cut, 0, sizeof(*cut));
	cut->epoch = cluster_epoch_get_current();
	cut->master = cluster_grd_lookup_master_gen(resid, &cut->generation);
	return cut->epoch != 0 && cut->generation != 0 && cut->master >= 0
		   && cut->master < CLUSTER_MAX_NODES && cluster_epoch_get_current() == cut->epoch;
}

static bool
lock_owner_retire_shadow(const ClusterControlRequestHandle *handle,
						 ClusterControlRequestCut *current)
{
	ClusterControlRequestView view;
	ClusterControlRequestCut before, after;
	int result;

	if (!cluster_control_request_snapshot(handle, &view)
		|| view.state != CLUSTER_CONTROL_REQUEST_TERMINAL
		|| !lock_owner_cut(&view.message.key.resid, &before) || before.epoch != view.cut.epoch
		|| before.generation != view.cut.generation || before.master != view.cut.master)
		return false;
	/* Clear only the requester shadow, never drain grants from that shadow. */
	result = cluster_grd_retire_request_and_drain(&view.message.key.resid, &view.message.key.holder,
												  view.message.previous_request,
												  view.message.previous_mode, NULL, 0);
	if ((result < 0 && result != CLUSTER_GRD_RELEASE_NOT_FOUND)
		|| !lock_owner_cut(&view.message.key.resid, &after) || after.epoch != before.epoch
		|| after.generation != before.generation || after.master != before.master)
		return false;
	*current = after;
	return true;
}

static bool
lock_owner_retire_handle(ClusterControlRequestHandle *handle)
{
	ClusterControlRequestCut current;

	if (handle->generation == 0)
		return true;
	if (!lock_owner_retire_shadow(handle, &current)
		|| !cluster_control_request_forget(handle, &control_creator, &current))
		return false;
	memset(handle, 0, sizeof(*handle));
	return true;
}

static bool
lock_owner_retire_poll(ClusterLockOwner *owner)
{
	bool first, second;

	lock_owner_abandon(owner);
	if (owner->conversion == CLUSTER_LOCK_OWNER_UPGRADING
		|| owner->conversion == CLUSTER_LOCK_OWNER_RESTORING)
		return false;
	first = lock_owner_retire_handle(&owner->attempt);
	second = lock_owner_retire_handle(&owner->secondary);
	return first && second;
}

static bool
lock_owner_register(ClusterLockOwner *owner, ClusterLockOwnerState state)
{
	owner->shared = cluster_shared_config;
	if (owner->shared) {
		if (owner_sequence == UINT64_MAX)
			return false;
		owner->request.control_owner_id = ++owner_sequence;
	}
	owner->state = state;
	owner->generation = cluster_grd_redeclare_generation();
	owner->next = private_owners;
	private_owners = owner;
	pg_atomic_write_u64(&MyProc->cluster_grd_redeclare_acked, 0);
	pg_atomic_fetch_add_u32(&MyProc->cluster_grd_registered_count, 1);
	return true;
}

static bool
lock_owner_forget(ClusterLockOwner *owner)
{
	ClusterLockOwner **link = &private_owners;
	bool heap_owned = owner->heap_owned;

	while (*link != NULL && *link != owner)
		link = &(*link)->next;
	if (*link != owner)
		return false;
	*link = owner->next;
	pg_atomic_fetch_sub_u32(&MyProc->cluster_grd_registered_count, 1);
	memset(owner, 0, sizeof(*owner));
	if (heap_owned)
		pfree(owner);
	return true;
}

/* Unpublished acquisitions remain stable until exact terminal evidence.
 * They have no caller guard to come back and reap them, so normal owner
 * entry/census does that work. The shared LMON service owns wire progress. */
static void
lock_owner_reap(void)
{
	ClusterLockOwner *owner = private_owners;

	while (owner != NULL) {
		ClusterLockOwner *next = owner->next;

		if (owner->shared && owner->heap_owned && owner->auto_reap
			&& owner->state == CLUSTER_LOCK_OWNER_RETIRING && lock_owner_retire_poll(owner))
			(void)lock_owner_forget(owner);
		owner = next;
	}
}

ClusterLockAcquireResult
cluster_lock_owner_request_acquire(ClusterLockAcquireRequest *request,
								   ClusterControlNativeFinish finish, void *argument)
{
	ClusterLockOwner *owner;
	ClusterLockAcquireResult result;

	if (request == NULL || !cluster_shared_config || MyProc == NULL
		|| !cluster_control_request_resid_valid(&request->resid)
		|| request->op != CLUSTER_LOCK_OP_REQUEST || request->control_owner_id != 0)
		return CLUSTER_LOCK_ACQUIRE_FAIL_INTERNAL;
	lock_owner_reap();
	owner = MemoryContextAllocZero(TopMemoryContext, sizeof(*owner));
	owner->request = *request;
	owner->heap_owned = true;
	if (!lock_owner_register(owner, CLUSTER_LOCK_OWNER_ACQUIRING)) {
		pfree(owner);
		return CLUSTER_LOCK_ACQUIRE_FAIL_INTERNAL;
	}
	PG_TRY();
	{
		result = cluster_lock_acquire_seven_step(&owner->request);
		if (result == CLUSTER_LOCK_ACQUIRE_NEED_PG_NATIVE_LOCK) {
			owner->state = CLUSTER_LOCK_OWNER_INSTALLING;
			result = finish != NULL ? finish(&owner->request, argument)
									: cluster_lock_acquire_s5_promote(&owner->request);
		}
		if ((result == CLUSTER_LOCK_ACQUIRE_OK_GRANTED
			 || result == CLUSTER_LOCK_ACQUIRE_OK_CONVERTED)
			&& owner->request.holder.cluster_epoch == cluster_epoch_get_current()
			&& owner->generation == cluster_grd_redeclare_generation()
			&& lock_owner_mark_held(&owner->attempt, &owner->request.resid, &owner->request.holder,
									owner->request.lockmode)) {
			owner->reconstructable = true;
			owner->state = CLUSTER_LOCK_OWNER_HELD;
		} else {
			if (result == CLUSTER_LOCK_ACQUIRE_OK_GRANTED
				|| result == CLUSTER_LOCK_ACQUIRE_OK_CONVERTED)
				result = CLUSTER_LOCK_ACQUIRE_FAIL_INTERNAL;
			lock_owner_abandon(owner);
			owner->auto_reap = true;
		}
		*request = owner->request;
	}
	PG_CATCH();
	{
		lock_owner_abandon(owner);
		owner->auto_reap = true;
		*request = owner->request;
		PG_RE_THROW();
	}
	PG_END_TRY();
	lock_owner_reap();
	return result;
}

static ClusterLockOwner *
lock_owner_from_guard(const ClusterLockAcquireRequest *request)
{
	ClusterLockOwner *owner;

	if (request == NULL || !cluster_shared_config)
		return NULL;
	owner = lock_owner_find(request->control_owner_id);
	if (owner == NULL || memcmp(&request->resid, &owner->request.resid, sizeof(request->resid)) != 0
		|| (request->lockmode != owner->request.lockmode
			&& !(owner->state == CLUSTER_LOCK_OWNER_RETIRING
				 && request->lockmode == owner->original.lockmode))
		|| request->holder.node_id != owner->request.holder.node_id
		|| request->holder.procno != owner->request.holder.procno)
		return NULL;
	return owner;
}

static bool
lock_owner_restore_poll(ClusterLockOwner *owner)
{
	ClusterControlRequestCut cut;
	uint64 epoch = cluster_epoch_get_current();
	uint64 generation = cluster_grd_redeclare_generation();
	bool rebuilding = owner->conversion == CLUSTER_LOCK_OWNER_REBUILD_RESTORING;

	if (!rebuilding && owner->conversion != CLUSTER_LOCK_OWNER_RESTORING)
		return false;
	if (rebuilding || epoch != owner->original.holder.cluster_epoch
		|| generation != owner->generation) {
		if (!cluster_grd_control_rebuild_frozen(epoch, generation))
			return false;
		if (!rebuilding) {
			if (!cluster_control_request_rebuild_upgrade_begin(&owner->secondary, &owner->attempt,
															   &control_creator))
				return false;
			owner->conversion = CLUSTER_LOCK_OWNER_REBUILD_RESTORING;
			cluster_lmon_wakeup();
		}
		rebuilding = true;
	}
	if (!lock_owner_retire_shadow(&owner->attempt, &cut))
		return false;
	if (rebuilding) {
		if (!cluster_grd_control_rebuild_frozen(epoch, generation)
			|| !cluster_control_request_rebuild_upgrade_finish(&owner->secondary, &owner->attempt,
															   &control_creator, &cut))
			return false;
	} else if (!cluster_control_request_restore_upgrade(&owner->secondary, &owner->attempt,
														&control_creator, &cut))
		return false;
	/* The registry consumed the failed X and restored the known S in one
	 * critical section. No second forget can race that restoration proof. */
	owner->attempt = owner->secondary;
	memset(&owner->secondary, 0, sizeof(owner->secondary));
	owner->secondary_retiring = false;
	owner->request = owner->original;
	memset(&owner->original, 0, sizeof(owner->original));
	owner->conversion = CLUSTER_LOCK_OWNER_NO_CONVERSION;
	owner->state = CLUSTER_LOCK_OWNER_HELD;
	owner->reconstructable = true;
	return !rebuilding; /* Rebuild still owes the old S's actual REDECLARE. */
}

ClusterLockAcquireResult
cluster_lock_owner_request_convert(ClusterLockAcquireRequest *request, uint64 preserve_request_id,
								   ClusterControlNativeFinish finish, void *argument)
{
	ClusterLockOwner *owner;
	ClusterLockAcquireRequest next;
	ClusterControlRequestKey key;
	ClusterControlRequestHandle target = { 0 };
	ClusterControlRequestCut cut;
	ClusterLockAcquireResult result;
	bool upgrade;

	if (request == NULL || !cluster_shared_config || finish == NULL
		|| request->op != CLUSTER_LOCK_OP_CONVERT
		|| request->resid.type != CLUSTER_WAL_RETENTION_RESID_TYPE)
		return CLUSTER_LOCK_ACQUIRE_FAIL_INTERNAL;
	owner = lock_owner_find(request->control_owner_id);
	if (!cluster_lock_owner_is_usable(owner)
		|| memcmp(&request->resid, &owner->request.resid, sizeof(request->resid)) != 0
		|| request->current_mode != owner->request.lockmode
		|| request->holder.node_id != owner->request.holder.node_id
		|| request->holder.procno != owner->request.holder.procno)
		return CLUSTER_LOCK_ACQUIRE_FAIL_INTERNAL;
	upgrade = request->current_mode == ShareLock && request->lockmode == ExclusiveLock;
	if ((!upgrade && (request->current_mode != ExclusiveLock || request->lockmode != ShareLock))
		|| (upgrade && preserve_request_id != 0)
		|| (!upgrade
			&& (preserve_request_id == 0 || preserve_request_id != request->request_id
				|| request->holder.request_id != request->request_id)))
		return CLUSTER_LOCK_ACQUIRE_FAIL_INTERNAL;
	if (owner->secondary_retiring && lock_owner_retire_handle(&owner->secondary))
		owner->secondary_retiring = false;
	if (upgrade && owner->secondary.generation != 0)
		return CLUSTER_LOCK_ACQUIRE_PENDING;
	next = *request;
	next.holder = owner->request.holder;
	/* A copied guard identifies the stable owner, not its current wire id.
	 * Downgrade preserves that owner's confirmed, possibly rebound X id. */
	next.request_id
		= upgrade ? cluster_ges_reply_wait_next_request_id() : owner->request.request_id;
	next.holder.request_id = next.request_id;
	next.convert_old_request_id = upgrade ? owner->request.request_id : 0;
	if (upgrade) {
		memset(&key, 0, sizeof(key));
		key.resid = next.resid;
		key.holder = next.holder;
		if (!cluster_control_request_register(&key, ExclusiveLock, &control_creator,
											  owner->request.request_id, ShareLock, &target))
			return CLUSTER_LOCK_ACQUIRE_FAIL_INTERNAL;
		if (!cluster_control_request_pause_upgrade(&owner->attempt, &control_creator)) {
			owner->secondary = target;
			owner->secondary_retiring = true;
			lock_owner_declare_debt(&target);
			return CLUSTER_LOCK_ACQUIRE_FAIL_INTERNAL;
		}
		owner->secondary = owner->attempt;
		owner->attempt = target;
	} else if (!cluster_control_request_begin_downgrade(&owner->attempt, &control_creator))
		return CLUSTER_LOCK_ACQUIRE_FAIL_INTERNAL;
	owner->original = owner->request;
	owner->request = next;
	owner->reconstructable = false;
	owner->state = CLUSTER_LOCK_OWNER_ACQUIRING;
	owner->conversion = upgrade ? CLUSTER_LOCK_OWNER_UPGRADING : CLUSTER_LOCK_OWNER_DOWNGRADING;
	PG_TRY();
	{
		result = cluster_lock_acquire_seven_step(&owner->request);
		if (result == CLUSTER_LOCK_ACQUIRE_NEED_PG_NATIVE_LOCK) {
			owner->state = CLUSTER_LOCK_OWNER_INSTALLING;
			result = finish(&owner->request, argument);
		}
		if (result == CLUSTER_LOCK_ACQUIRE_OK_CONVERTED
			&& owner->generation == cluster_grd_redeclare_generation()
			&& lock_owner_cut(&owner->request.resid, &cut)
			&& cut.epoch == owner->request.holder.cluster_epoch
			&& lock_owner_mark_held(&owner->attempt, &owner->request.resid, &owner->request.holder,
									owner->request.lockmode)
			&& (!upgrade
				|| cluster_control_request_confirm_upgrade(&owner->secondary, &owner->attempt,
														   &control_creator, &cut))) {
			owner->state = CLUSTER_LOCK_OWNER_HELD;
			owner->reconstructable = true;
			owner->request.convert_old_request_id = 0;
			owner->conversion = CLUSTER_LOCK_OWNER_NO_CONVERSION;
			if (upgrade) {
				owner->secondary_retiring = true;
				cluster_lmon_wakeup();
			}
			memset(&owner->original, 0, sizeof(owner->original));
		} else {
			if (result == CLUSTER_LOCK_ACQUIRE_OK_GRANTED
				|| result == CLUSTER_LOCK_ACQUIRE_OK_CONVERTED)
				result = CLUSTER_LOCK_ACQUIRE_FAIL_INTERNAL;
			lock_owner_declare_debt(&owner->attempt);
			owner->state = CLUSTER_LOCK_OWNER_RETIRING;
			if (upgrade) {
				owner->conversion = CLUSTER_LOCK_OWNER_RESTORING;
				/* NOT_AVAIL can promise the old S only after an exact ACK. */
				if (!lock_owner_restore_poll(owner))
					result = CLUSTER_LOCK_ACQUIRE_FAIL_INTERNAL;
			} else {
				lock_owner_abandon(owner);
				result = CLUSTER_LOCK_ACQUIRE_FAIL_INTERNAL;
			}
		}
		*request = owner->request;
	}
	PG_CATCH();
	{
		/* ERROR abandons the operation and both logical holds. The original
		 * caller guard remains sufficient to find its stable cleanup owner. */
		lock_owner_abandon(owner);
		*request = owner->request;
		PG_RE_THROW();
	}
	PG_END_TRY();
	return result;
}

bool
cluster_lock_owner_request_usable(const ClusterLockAcquireRequest *request)
{
	return cluster_lock_owner_is_usable(lock_owner_from_guard(request));
}

bool
cluster_lock_owner_request_refresh(ClusterLockAcquireRequest *request)
{
	ClusterLockOwner *owner = lock_owner_from_guard(request);

	if (!cluster_lock_owner_is_usable(owner))
		return false;
	*request = owner->request;
	return true;
}

ClusterLockAcquireResult
cluster_lock_owner_request_release(const ClusterLockAcquireRequest *request)
{
	ClusterLockOwner *owner = lock_owner_from_guard(request);

	if (owner == NULL)
		return CLUSTER_LOCK_ACQUIRE_FAIL_INTERNAL;
	return lock_owner_release_wait(owner, request->timeout_ms, request->wait_event)
			   ? CLUSTER_LOCK_ACQUIRE_OK_GRANTED
			   : CLUSTER_LOCK_ACQUIRE_PENDING;
}

static ClusterLockAcquireResult lock_owner_install_result(ClusterLockOwner *owner);

static ClusterLockAcquireResult
lock_owner_cf_poll_step(ClusterLockOwner *owner)
{
	ClusterLockAcquireResult result;
	ClusterGesAcquireResult exchange;

	if (owner->state == CLUSTER_LOCK_OWNER_EMPTY) {
		/* No publication yet. An unavailable prerequisite owns no remote
		 * request and must not register a phantom reconstruction blocker. */
		if (cluster_lock_acquire_s1_entry(&owner->request) != CLUSTER_LOCK_ACQUIRE_OK_GRANTED
			|| (cluster_grd_shard_phase(cluster_grd_shard_for_resource(&owner->request.resid))
					!= GRD_SHARD_NORMAL
				&& !cluster_grd_control_recovery_ready(&owner->request.resid,
													   owner->request.lockmode)))
			return CLUSTER_LOCK_ACQUIRE_PENDING;
		if (cluster_lock_acquire_s2_identity(&owner->request) != CLUSTER_LOCK_ACQUIRE_OK_GRANTED
			|| !lock_owner_register(owner, CLUSTER_LOCK_OWNER_ACQUIRING))
			return CLUSTER_LOCK_ACQUIRE_FAIL_INTERNAL;
		owner->cooperative_acquire = true;
		result = cluster_lock_acquire_s3_partition_reservation(&owner->request);
		if (result != CLUSTER_LOCK_ACQUIRE_OK_GRANTED)
			return result;
		(void)cluster_lmd_wait_state_publish(
			&MyProc->cluster_lmd_wait, CLUSTER_LMD_WAIT_GES, owner->request.request_id,
			owner->request.holder.cluster_epoch, GetTopTransactionIdIfAny());
	}
	if (owner->request.holder.cluster_epoch != cluster_epoch_get_current()
		|| owner->generation != cluster_grd_redeclare_generation())
		return CLUSTER_LOCK_ACQUIRE_FAIL_STALE_GENERATION;
	if (cluster_cancel_token_consume())
		return CLUSTER_LOCK_ACQUIRE_FAIL_DEADLOCK;
	/* A pending S5 retains this grant, including any exact local promotion.
	 * Do not reconstruct/zero it through another S4 result. */
	exchange = owner->request.hw_grant.grant_observed
				   ? CLUSTER_GES_ACQUIRE_GRANTED
				   : cluster_ges_cf_request_poll(&owner->acquisition, &owner->request.resid,
												 owner->request.lockmode, &owner->request.holder,
												 &owner->request.hw_grant);
	if (exchange == CLUSTER_GES_ACQUIRE_PENDING)
		return CLUSTER_LOCK_ACQUIRE_PENDING;
	if (exchange != CLUSTER_GES_ACQUIRE_GRANTED)
		return exchange == CLUSTER_GES_ACQUIRE_CUT_CHANGED
				   ? CLUSTER_LOCK_ACQUIRE_FAIL_STALE_GENERATION
				   : CLUSTER_LOCK_ACQUIRE_FAIL_INTERNAL;
	cluster_lmd_wait_state_clear(&MyProc->cluster_lmd_wait);
	return lock_owner_install_result(owner);
}

ClusterLockAcquireResult
cluster_lock_owner_acquire_poll(ClusterLockOwner *owner)
{
	ClusterLockAcquireResult result;

	if (owner == NULL || MyProc == NULL || !cluster_shared_config
		|| (MyBackendType != B_LMON && MyBackendType != B_LMS)
		|| owner->request.resid.type != CLUSTER_CF_RESID_TYPE
		|| !cluster_control_request_resid_valid(&owner->request.resid)
		|| owner->request.op != CLUSTER_LOCK_OP_REQUEST
		|| (owner->state != CLUSTER_LOCK_OWNER_EMPTY
			&& (owner->state != CLUSTER_LOCK_OWNER_ACQUIRING || !owner->cooperative_acquire))
		|| owner->acquire_poll_active)
		return CLUSTER_LOCK_ACQUIRE_FAIL_INTERNAL;
	owner->acquire_poll_active = true;
	PG_TRY();
	{
		result = lock_owner_cf_poll_step(owner);
	}
	PG_CATCH();
	{
		owner->acquire_poll_active = false;
		if (owner->state != CLUSTER_LOCK_OWNER_EMPTY)
			lock_owner_abandon(owner);
		PG_RE_THROW();
	}
	PG_END_TRY();
	owner->acquire_poll_active = false;
	if (result != CLUSTER_LOCK_ACQUIRE_OK_GRANTED && result != CLUSTER_LOCK_ACQUIRE_PENDING
		&& owner->state != CLUSTER_LOCK_OWNER_EMPTY)
		lock_owner_abandon(owner);
	return result;
}

/* The real S1-S4 producer writes directly into process-owned storage. A
 * failed/throwing request with an allocated identity retains cleanup, even
 * when S5 was never reached. No temporary acquisition stack is enumerated. */
ClusterLockAcquireResult
cluster_lock_owner_acquire(ClusterLockOwner *owner)
{
	ClusterLockAcquireResult result;

	if (owner == NULL || owner->state != CLUSTER_LOCK_OWNER_EMPTY)
		return CLUSTER_LOCK_ACQUIRE_FAIL_INTERNAL;
	if (MyProc == NULL) {
		/* Before PGPROC initialization only an explicit S1 native decision
		 * may proceed. Never send a request without a census owner. */
		result = cluster_lock_acquire_s1_entry(&owner->request);
		return result == CLUSTER_LOCK_ACQUIRE_OK_NATIVE ? result
														: CLUSTER_LOCK_ACQUIRE_FAIL_INTERNAL;
	}
	if (!lock_owner_register(owner, CLUSTER_LOCK_OWNER_ACQUIRING))
		return CLUSTER_LOCK_ACQUIRE_FAIL_INTERNAL;
	PG_TRY();
	{
		result = cluster_lock_acquire_seven_step(&owner->request);
	}
	PG_CATCH();
	{
		lock_owner_abandon(owner);
		if (owner->request.request_id == 0)
			(void)lock_owner_forget(owner);
		PG_RE_THROW();
	}
	PG_END_TRY();
	owner->state = CLUSTER_LOCK_OWNER_RETIRING;
	if (result == CLUSTER_LOCK_ACQUIRE_OK_NATIVE) {
		(void)lock_owner_forget(owner);
		return result;
	}
	if (owner->request.request_id == 0) {
		(void)lock_owner_forget(owner);
		return result == CLUSTER_LOCK_ACQUIRE_NEED_PG_NATIVE_LOCK
					   || result == CLUSTER_LOCK_ACQUIRE_OK_GRANTED
					   || result == CLUSTER_LOCK_ACQUIRE_OK_CONVERTED
				   ? CLUSTER_LOCK_ACQUIRE_FAIL_INTERNAL
				   : result;
	}
	if (result != CLUSTER_LOCK_ACQUIRE_NEED_PG_NATIVE_LOCK
		&& result != CLUSTER_LOCK_ACQUIRE_OK_GRANTED
		&& result != CLUSTER_LOCK_ACQUIRE_OK_CONVERTED) {
		lock_owner_abandon(owner);
		return result;
	}
	owner->state = CLUSTER_LOCK_OWNER_ACQUIRING;
	return cluster_lock_owner_install(owner) ? CLUSTER_LOCK_ACQUIRE_OK_GRANTED
											 : CLUSTER_LOCK_ACQUIRE_FAIL_INTERNAL;
}

/* The scope precedes S5 publication, including any CFI inside S5.  A longjmp
 * retains a retiring owner, rather than a pointer into the caller's stack. */
static ClusterLockAcquireResult
lock_owner_install_result(ClusterLockOwner *owner)
{
	uint64 epoch = cluster_epoch_get_current();
	uint64 generation;
	ClusterLockAcquireResult result;

	if (owner == NULL || MyProc == NULL
		|| (owner->state != CLUSTER_LOCK_OWNER_EMPTY
			&& owner->state != CLUSTER_LOCK_OWNER_ACQUIRING))
		return CLUSTER_LOCK_ACQUIRE_FAIL_INTERNAL;
	if (owner->request.holder.cluster_epoch != epoch
		|| owner->request.holder.node_id != cluster_node_id
		|| owner->request.holder.procno != (uint32)MyProc->pgprocno
		|| owner->request.request_id == 0
		|| owner->request.holder.request_id != owner->request.request_id) {
		if (owner->state == CLUSTER_LOCK_OWNER_ACQUIRING)
			lock_owner_abandon(owner);
		return CLUSTER_LOCK_ACQUIRE_FAIL_INTERNAL;
	}
	if (owner->state == CLUSTER_LOCK_OWNER_EMPTY) {
		/* PRE2 requires the owner/attempt before S3, not a post-grant claim. */
		if (cluster_shared_config || !lock_owner_register(owner, CLUSTER_LOCK_OWNER_INSTALLING))
			return CLUSTER_LOCK_ACQUIRE_FAIL_INTERNAL;
	} else
		owner->state = CLUSTER_LOCK_OWNER_INSTALLING;
	generation = owner->generation;
	PG_TRY();
	{
		result = cluster_lock_acquire_s5_promote(&owner->request);
	}
	PG_CATCH();
	{
		lock_owner_abandon(owner);
		PG_RE_THROW();
	}
	PG_END_TRY();
	if (result == CLUSTER_LOCK_ACQUIRE_PENDING) {
		owner->state = CLUSTER_LOCK_OWNER_ACQUIRING;
		return result;
	}
	owner->state = CLUSTER_LOCK_OWNER_RETIRING;
	owner->reconstructable = result == CLUSTER_LOCK_ACQUIRE_OK_GRANTED;
	if (result != CLUSTER_LOCK_ACQUIRE_OK_GRANTED || owner->request.holder.cluster_epoch != epoch
		|| cluster_epoch_get_current() != epoch || cluster_grd_redeclare_generation() != generation
		|| (owner->shared
			&& !lock_owner_mark_held(&owner->attempt, &owner->request.resid, &owner->request.holder,
									 owner->request.lockmode))) {
		if (owner->shared)
			lock_owner_abandon(owner);
		return CLUSTER_LOCK_ACQUIRE_FAIL_INTERNAL;
	}
	owner->state = CLUSTER_LOCK_OWNER_HELD;
	return CLUSTER_LOCK_ACQUIRE_OK_GRANTED;
}

bool
cluster_lock_owner_install(ClusterLockOwner *owner)
{
	return lock_owner_install_result(owner) == CLUSTER_LOCK_ACQUIRE_OK_GRANTED;
}

bool
cluster_lock_owner_is_usable(const ClusterLockOwner *owner)
{
	ClusterControlRequestView view;

	if (owner != NULL && owner->shared
		&& (!cluster_control_request_snapshot(&owner->attempt, &view)
			|| view.state != CLUSTER_CONTROL_REQUEST_HELD || view.owner_exited))
		return false;
	return owner != NULL && owner->state == CLUSTER_LOCK_OWNER_HELD && !owner->redeclare.initialized
		   && owner->request.holder.cluster_epoch == cluster_epoch_get_current()
		   && owner->generation == cluster_grd_redeclare_generation();
}

/* Discard only an older-epoch wire waiter, never the still-owned hold.
 * The drain rejects obsolete epochs. A sender-local restart counter
 * alone cannot retire a remote exchange at the same epoch. */
static bool
lock_owner_obsolete_exchange(ClusterLockOwner *owner, uint64 epoch)
{
	ClusterGesRedeclareAttempt *attempt = &owner->redeclare;
	uint64 generation;
	int32 master;

	if (!attempt->initialized) {
		/* Unknown routing can precede the first enqueue.  No wire/table
		 * obligation exists yet, but the old target id must not strand us. */
		memset(&owner->target, 0, sizeof(owner->target));
		return true;
	}
	master = cluster_grd_lookup_master_gen(&owner->request.resid, &generation);
	if (master < 0 || epoch <= attempt->key.cluster_epoch)
		return false;
	if (attempt->wait_registered)
		cluster_ges_reply_wait_delete(&attempt->key);
	memset(attempt, 0, sizeof(*attempt));
	memset(&owner->target, 0, sizeof(owner->target));
	return true;
}

static void
lock_owner_abandon_target(ClusterLockOwner *owner)
{
	if (owner->secondary.generation != 0) {
		lock_owner_declare_debt(&owner->secondary);
		owner->secondary_retiring = true;
	}
	if (owner->redeclare.wait_registered)
		cluster_ges_reply_wait_delete(&owner->redeclare.key);
	memset(&owner->redeclare, 0, sizeof(owner->redeclare));
	memset(&owner->target, 0, sizeof(owner->target));
}

static bool
lock_owner_shared_redeclare(ClusterLockOwner *owner, uint64 epoch, uint64 generation)
{
	ClusterControlRequestKey key;
	ClusterControlRequestHandle old;
	ClusterControlRequestView view;
	ClusterGesRedeclareResult result;

	if (owner->secondary_retiring) {
		if (!lock_owner_retire_handle(&owner->secondary))
			return false;
		owner->secondary_retiring = false;
	}
	if (owner->state != CLUSTER_LOCK_OWNER_HELD || !owner->reconstructable || epoch == 0
		|| owner->request.holder.cluster_epoch > epoch
		|| !cluster_control_request_snapshot(&owner->attempt, &view)
		|| view.state != CLUSTER_CONTROL_REQUEST_HELD || view.owner_exited)
		return false;
	if (!owner->redeclare.initialized && owner->request.holder.cluster_epoch == epoch
		&& owner->generation == generation)
		return true;
	if (owner->target.request_id == 0) {
		owner->target = owner->request.holder;
		owner->target.cluster_epoch = epoch;
		owner->target.request_id = cluster_ges_reply_wait_next_request_id();
	}
	if (owner->secondary.generation == 0) {
		memset(&key, 0, sizeof(key));
		key.resid = owner->request.resid;
		key.holder = owner->target;
		if (!cluster_control_request_register(&key, owner->request.lockmode, &control_creator, 0,
											  NoLock, &owner->secondary))
			return false;
	}
	result = cluster_ges_redeclare_poll(&owner->redeclare, &owner->request.resid,
										owner->request.lockmode, &owner->target);
	if (result == CLUSTER_GES_REDECLARE_PENDING)
		return false;
	if (result != CLUSTER_GES_REDECLARE_CONFIRMED || cluster_epoch_get_current() != epoch
		|| cluster_grd_redeclare_generation() != generation
		|| !lock_owner_mark_held(&owner->secondary, &owner->request.resid, &owner->target,
								 owner->request.lockmode)) {
		/* Even at the same epoch a sent target has its own retirement duty.
		 * Deleting its reply waiter cannot erase a queued/late grant. */
		lock_owner_abandon_target(owner);
		return false;
	}
	old = owner->attempt;
	owner->attempt = owner->secondary;
	owner->secondary = old;
	owner->secondary_retiring = true;
	lock_owner_declare_debt(&owner->secondary);
	owner->request.holder = owner->target;
	owner->request.request_id = owner->target.request_id;
	owner->generation = generation;
	memset(&owner->redeclare, 0, sizeof(owner->redeclare));
	memset(&owner->target, 0, sizeof(owner->target));
	/* The new holder is known. Census acknowledgement additionally requires
	 * the old exact attempt to be terminal at this cut. */
	if (!lock_owner_retire_handle(&owner->secondary))
		return false;
	owner->secondary_retiring = false;
	return true;
}

static bool
lock_owner_redeclare(ClusterLockOwner *owner)
{
	uint64 epoch = cluster_epoch_get_current();
	uint64 generation = cluster_grd_redeclare_generation();
	ClusterGesRedeclareResult result;

	if (owner->shared && owner->cooperative_acquire && owner->state == CLUSTER_LOCK_OWNER_ACQUIRING
		&& !owner->acquire_poll_active) {
		ClusterControlRequestCut cut;
		ClusterGesRedeclareAttempt *exchange = &owner->acquisition.exchange;

		if (epoch != owner->request.holder.cluster_epoch || generation != owner->generation
			|| (exchange->initialized && lock_owner_cut(&owner->request.resid, &cut)
				&& (cut.master != exchange->master
					|| cut.generation != exchange->master_generation)))
			lock_owner_abandon(owner);
		else
			return false; /* In flight, not a reconstructed holder. */
	}

	if (owner->shared
		&& (owner->conversion == CLUSTER_LOCK_OWNER_RESTORING
			|| owner->conversion == CLUSTER_LOCK_OWNER_REBUILD_RESTORING))
		return lock_owner_restore_poll(owner);
	if (owner->shared && owner->state == CLUSTER_LOCK_OWNER_RETIRING)
		return lock_owner_retire_poll(owner);
	if (owner->shared)
		return lock_owner_shared_redeclare(owner, epoch, generation);

	if (owner->state != CLUSTER_LOCK_OWNER_HELD && owner->state != CLUSTER_LOCK_OWNER_RETIRING)
		return false;
	if (!owner->redeclare.initialized && owner->request.holder.cluster_epoch == epoch
		&& owner->generation == generation)
		return true;
	if (epoch == 0 || owner->request.holder.cluster_epoch > epoch)
		return false;
	/* A failed/throwing S5 owns cleanup, not evidence that a grant existed.
	 * Never rebuild such an uncertain attempt into a current holder. */
	if (!owner->reconstructable)
		return false;
	if (owner->target.request_id == 0) {
		owner->target = owner->request.holder;
		owner->target.cluster_epoch = epoch;
		owner->target.request_id = cluster_ges_reply_wait_next_request_id();
	}
	result = cluster_ges_redeclare_poll(&owner->redeclare, &owner->request.resid,
										owner->request.lockmode, &owner->target);
	if (result == CLUSTER_GES_REDECLARE_CUT_CHANGED) {
		(void)lock_owner_obsolete_exchange(owner, epoch);
		return false; /* Another tick owns the new cut, not this observation. */
	}
	if (result != CLUSTER_GES_REDECLARE_CONFIRMED || cluster_epoch_get_current() != epoch
		|| cluster_grd_redeclare_generation() != generation)
		return false;
	owner->request.holder = owner->target;
	owner->request.request_id = owner->target.request_id;
	owner->generation = generation;
	memset(&owner->redeclare, 0, sizeof(owner->redeclare));
	memset(&owner->target, 0, sizeof(owner->target));
	return true;
}

static bool
lock_owner_release_poll(ClusterLockOwner *owner)
{
	ClusterLockAcquireResult result;

	if (owner == NULL || owner->state == CLUSTER_LOCK_OWNER_EMPTY || MyProc == NULL
		|| (owner->state == CLUSTER_LOCK_OWNER_ACQUIRING
			&& (!owner->cooperative_acquire || owner->acquire_poll_active))
		|| owner->state == CLUSTER_LOCK_OWNER_INSTALLING
		|| owner->state == CLUSTER_LOCK_OWNER_RELEASING)
		return false;
	owner->state = CLUSTER_LOCK_OWNER_RETIRING;
	if (owner->shared) {
		if (!lock_owner_retire_poll(owner))
			return false;
		return lock_owner_forget(owner);
	}
	if (!lock_owner_redeclare(owner))
		return false;
	/* No CFI inside S6 may replace the identity that S6 is retiring. */
	owner->state = CLUSTER_LOCK_OWNER_RELEASING;
	PG_TRY();
	{
		result = cluster_lock_acquire_s6_release(&owner->request);
	}
	PG_CATCH();
	{
		owner->state = CLUSTER_LOCK_OWNER_RETIRING;
		PG_RE_THROW();
	}
	PG_END_TRY();
	owner->state = CLUSTER_LOCK_OWNER_RETIRING;
	if (result != CLUSTER_LOCK_ACQUIRE_OK_GRANTED)
		return false;
	return lock_owner_forget(owner);
}

/* Preserve the caller's existing release budget, not a retirement deadline.
 * Expiry leaves the exact shared obligation owned and unusable. CONTROL
 * services never sleep on work they themselves must dispatch. */
static bool
lock_owner_release_wait(ClusterLockOwner *owner, int timeout_ms, uint32 event)
{
	TimestampTz deadline;
	int effective = timeout_ms > 0 ? timeout_ms : cluster_ges_request_timeout_ms;

	if (lock_owner_release_poll(owner))
		return true;
	if (owner == NULL || !owner->shared || owner->state != CLUSTER_LOCK_OWNER_RETIRING
		|| MyBackendType == B_LMON || MyBackendType == B_LMS || MyLatch == NULL
		|| proc_exit_inprogress)
		return false;
	if (effective == 0)
		effective = 600000; /* Same fallback as the existing S6 adapter. */
	deadline = effective < 0 ? 0 : TimestampTzPlusMilliseconds(GetCurrentTimestamp(), effective);
	for (;;) {
		TimestampTz now;
		long wait_ms = 100;

		ResetLatch(MyLatch);
		if (lock_owner_release_poll(owner))
			return true;
		now = GetCurrentTimestamp();
		if (deadline != 0 && now >= deadline)
			return false;
		if (deadline != 0)
			wait_ms = Min(wait_ms, Max(1L, (long)((deadline - now) / 1000)));
		CHECK_FOR_INTERRUPTS();
		/* Startup's SIGTERM sets a private flag, not InterruptPending. The
		 * original exit callbacks transfer shared retirement and release local
		 * locks; an exit callback must never re-enter this waiting loop. */
		if (AmStartupProcess())
			HandleStartupProcInterrupts();
		(void)WaitLatch(MyLatch, WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH, wait_ms,
						event != 0 ? event : WAIT_EVENT_CLUSTER_GES_REPLY_WAIT);
		if (AmStartupProcess())
			HandleStartupProcInterrupts();
	}
}

bool
cluster_lock_owner_release(ClusterLockOwner *owner)
{
	return owner != NULL
		   && lock_owner_release_wait(owner, owner->request.timeout_ms, owner->request.wait_event);
}

bool
cluster_lock_owner_release_poll(ClusterLockOwner *owner)
{
	return lock_owner_release_poll(owner);
}

/* Latch-only auxiliary loops cannot depend on a backend ProcSignal handler.
 * No synchronous remote operation is introduced: private owners use poll,
 * and the common walker publishes the existing complete-census ACK. */
void
cluster_lock_owners_service_poll(void)
{
	uint64 enumerated;

	if (!cluster_shared_config || MyProc == NULL || private_owners == NULL || private_walk_active)
		return;
	(void)cluster_lock_owners_redeclare(&enumerated);
	cluster_grd_redeclare_all_registered();
}

bool
cluster_lock_owners_redeclare(uint64 *enumerated)
{
	ClusterLockOwner *owner;
	volatile bool complete = true;

	if (enumerated == NULL)
		return false;
	*enumerated = 0;
	if (private_walk_active)
		return false;
	private_walk_active = true;
	PG_TRY();
	{
		lock_owner_reap();
		for (owner = private_owners; owner != NULL; owner = owner->next) {
			(*enumerated)++;
			if (!lock_owner_redeclare(owner))
				complete = false;
		}
	}
	PG_CATCH();
	{
		FlushErrorState();
		complete = false;
	}
	PG_END_TRY();
	private_walk_active = false;
	return complete;
}
