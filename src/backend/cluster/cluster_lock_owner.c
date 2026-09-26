/*-------------------------------------------------------------------------
 * cluster_lock_owner.c -- stable manual GES ownership and reconstruction.
 *
 * Portions Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "cluster/cluster_epoch.h"
#include "cluster/cluster_lock_owner.h"
#include "miscadmin.h"
#include "storage/proc.h"

static ClusterLockOwner *private_owners;
static bool private_walk_active;

/* The scope precedes S5 publication, including any CFI inside S5.  A longjmp
 * retains a retiring owner, rather than a pointer into the caller's stack. */
bool
cluster_lock_owner_install(ClusterLockOwner *owner)
{
	uint64 epoch = cluster_epoch_get_current();
	uint64 generation = cluster_grd_redeclare_generation();
	ClusterLockAcquireResult result;

	if (owner == NULL || owner->state != CLUSTER_LOCK_OWNER_EMPTY || MyProc == NULL
		|| owner->request.holder.node_id != cluster_node_id
		|| owner->request.holder.procno != (uint32)MyProc->pgprocno
		|| owner->request.request_id == 0
		|| owner->request.holder.request_id != owner->request.request_id)
		return false;
	owner->state = CLUSTER_LOCK_OWNER_INSTALLING;
	owner->generation = generation;
	owner->next = private_owners;
	private_owners = owner;
	pg_atomic_write_u64(&MyProc->cluster_grd_redeclare_acked, 0);
	pg_atomic_fetch_add_u32(&MyProc->cluster_grd_registered_count, 1);
	PG_TRY();
	{
		result = cluster_lock_acquire_s5_promote(&owner->request);
	}
	PG_CATCH();
	{
		owner->state = CLUSTER_LOCK_OWNER_RETIRING;
		PG_RE_THROW();
	}
	PG_END_TRY();
	owner->state = CLUSTER_LOCK_OWNER_RETIRING;
	owner->reconstructable = result == CLUSTER_LOCK_ACQUIRE_OK_GRANTED;
	if (result != CLUSTER_LOCK_ACQUIRE_OK_GRANTED || owner->request.holder.cluster_epoch != epoch
		|| cluster_epoch_get_current() != epoch || cluster_grd_redeclare_generation() != generation)
		return false;
	owner->state = CLUSTER_LOCK_OWNER_HELD;
	return true;
}

bool
cluster_lock_owner_is_usable(const ClusterLockOwner *owner)
{
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

static bool
lock_owner_redeclare(ClusterLockOwner *owner)
{
	uint64 epoch = cluster_epoch_get_current();
	uint64 generation = cluster_grd_redeclare_generation();
	ClusterGesRedeclareResult result;

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

bool
cluster_lock_owner_release(ClusterLockOwner *owner)
{
	ClusterLockOwner **link = &private_owners;
	ClusterLockAcquireResult result;

	if (owner == NULL || owner->state == CLUSTER_LOCK_OWNER_EMPTY || MyProc == NULL
		|| owner->state == CLUSTER_LOCK_OWNER_INSTALLING
		|| owner->state == CLUSTER_LOCK_OWNER_RELEASING)
		return false;
	owner->state = CLUSTER_LOCK_OWNER_RETIRING;
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
	while (*link != NULL && *link != owner)
		link = &(*link)->next;
	if (*link != owner)
		return false;
	*link = owner->next;
	pg_atomic_fetch_sub_u32(&MyProc->cluster_grd_registered_count, 1);
	memset(owner, 0, sizeof(*owner));
	return true;
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
