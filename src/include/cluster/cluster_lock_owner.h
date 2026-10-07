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
#include "cluster/cluster_control_request.h"

typedef enum ClusterLockOwnerState {
	CLUSTER_LOCK_OWNER_EMPTY = 0,
	CLUSTER_LOCK_OWNER_ACQUIRING,
	CLUSTER_LOCK_OWNER_INSTALLING,
	CLUSTER_LOCK_OWNER_HELD,
	CLUSTER_LOCK_OWNER_RETIRING,
	CLUSTER_LOCK_OWNER_RELEASING
} ClusterLockOwnerState;

typedef enum ClusterLockOwnerConversion {
	CLUSTER_LOCK_OWNER_NO_CONVERSION = 0,
	CLUSTER_LOCK_OWNER_UPGRADING,
	CLUSTER_LOCK_OWNER_RESTORING,
	CLUSTER_LOCK_OWNER_REBUILD_RESTORING,
	CLUSTER_LOCK_OWNER_DOWNGRADING
} ClusterLockOwnerConversion;

/* Embedded in stable process-owned storage, never in an acquisition stack. */
typedef struct ClusterLockOwner {
	ClusterLockAcquireRequest request;
	ClusterGesRedeclareAttempt redeclare;
	ClusterGesAcquireAttempt acquisition;
	ClusterGrdHolderId target;
	uint64 generation;
	bool reconstructable;
	bool shared;
	bool secondary_retiring;
	bool heap_owned;
	bool auto_reap;
	bool cooperative_acquire;
	bool acquire_poll_active;
	ClusterControlRequestHandle attempt;
	ClusterControlRequestHandle secondary;
	ClusterLockAcquireRequest original;
	ClusterLockOwnerConversion conversion;
	struct ClusterLockOwner *next;
	ClusterLockOwnerState state;
} ClusterLockOwner;

extern bool cluster_lock_owner_install(ClusterLockOwner *owner);
extern ClusterLockAcquireResult cluster_lock_owner_acquire(ClusterLockOwner *owner);
extern ClusterLockAcquireResult cluster_lock_owner_acquire_poll(ClusterLockOwner *owner);
extern bool cluster_lock_owner_is_usable(const ClusterLockOwner *owner);
extern bool cluster_lock_owner_release(ClusterLockOwner *owner);
extern bool cluster_lock_owner_release_poll(ClusterLockOwner *owner);
extern void cluster_lock_owners_service_poll(void);
extern bool cluster_lock_owners_redeclare(uint64 *enumerated);
/* Called after identity minting, before the first reservation/wire mutation. */
extern bool cluster_lock_owner_request_prepare(ClusterLockAcquireRequest *request);
typedef ClusterLockAcquireResult (*ClusterControlNativeFinish)(ClusterLockAcquireRequest *request,
															   void *argument);
extern ClusterLockAcquireResult
cluster_lock_owner_request_acquire(ClusterLockAcquireRequest *request,
								   ClusterControlNativeFinish finish, void *argument);
extern ClusterLockAcquireResult
cluster_lock_owner_request_convert(ClusterLockAcquireRequest *request, uint64 preserve_request_id,
								   ClusterControlNativeFinish finish, void *argument);
extern bool cluster_lock_owner_request_refresh(ClusterLockAcquireRequest *request);
extern bool cluster_lock_owner_request_usable(const ClusterLockAcquireRequest *request);
extern ClusterLockAcquireResult
cluster_lock_owner_request_release(const ClusterLockAcquireRequest *request);

#endif
