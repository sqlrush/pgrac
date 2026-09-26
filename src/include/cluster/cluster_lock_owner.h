/*-------------------------------------------------------------------------
 * cluster_lock_owner.h -- process-private ownership of manual GES holds.
 *
 * Portions Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 *-------------------------------------------------------------------------
 */
#ifndef CLUSTER_LOCK_OWNER_H
#define CLUSTER_LOCK_OWNER_H

#include "cluster/cluster_lock_acquire.h"

typedef enum ClusterLockOwnerState {
	CLUSTER_LOCK_OWNER_EMPTY = 0,
	CLUSTER_LOCK_OWNER_ACQUIRING,
	CLUSTER_LOCK_OWNER_INSTALLING,
	CLUSTER_LOCK_OWNER_HELD,
	CLUSTER_LOCK_OWNER_RETIRING,
	CLUSTER_LOCK_OWNER_RELEASING
} ClusterLockOwnerState;

/* Embedded in stable process-owned storage, never in an acquisition stack. */
typedef struct ClusterLockOwner {
	ClusterLockAcquireRequest request;
	ClusterGesRedeclareAttempt redeclare;
	ClusterGrdHolderId target;
	uint64 generation;
	bool reconstructable;
	struct ClusterLockOwner *next;
	ClusterLockOwnerState state;
} ClusterLockOwner;

extern bool cluster_lock_owner_install(ClusterLockOwner *owner);
extern ClusterLockAcquireResult cluster_lock_owner_acquire(ClusterLockOwner *owner);
extern bool cluster_lock_owner_is_usable(const ClusterLockOwner *owner);
extern bool cluster_lock_owner_release(ClusterLockOwner *owner);
extern bool cluster_lock_owners_redeclare(uint64 *enumerated);

#endif
