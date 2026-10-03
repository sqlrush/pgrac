/*-------------------------------------------------------------------------
 * cluster_control_request.h -- exact control-request cleanup ownership.
 * Portions Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 *-------------------------------------------------------------------------
 */
#ifndef CLUSTER_CONTROL_REQUEST_H
#define CLUSTER_CONTROL_REQUEST_H

#include "cluster/cluster_grd.h"

#define CLUSTER_CONTROL_REQUEST_CAPACITY 4096
#define CLUSTER_CONTROL_RETIRE_OPCODE UINT32_C(19)
#define CLUSTER_CONTROL_RETIRE_BYTES 80

typedef struct ClusterControlRequestKey {
	ClusterResId resid;
	ClusterGrdHolderId holder;
} ClusterControlRequestKey;

typedef struct ClusterControlRequestOwner {
	uint64 incarnation;
	uint32 procno;
	int32 pid;
} ClusterControlRequestOwner;

typedef struct ClusterControlRequestHandle {
	uint64 generation;
	uint32 slot;
} ClusterControlRequestHandle;

typedef struct ClusterControlRequestCut {
	uint64 epoch;
	uint64 generation;
	int32 master;
} ClusterControlRequestCut;

typedef enum ClusterControlRequestState {
	CLUSTER_CONTROL_REQUEST_FREE = 0,
	CLUSTER_CONTROL_REQUEST_ACTIVE,
	CLUSTER_CONTROL_REQUEST_HELD,
	CLUSTER_CONTROL_REQUEST_ABANDONED,
	CLUSTER_CONTROL_REQUEST_TERMINAL,
	CLUSTER_CONTROL_REQUEST_INVALID,
	/* Known original S remains owned, but cannot send/redeclare in flight. */
	CLUSTER_CONTROL_REQUEST_QUIESCED
} ClusterControlRequestState;

typedef enum ClusterControlRetireVerb {
	CLUSTER_CONTROL_RETIRE = 1,
	CLUSTER_CONTROL_RETIRED,
	CLUSTER_CONTROL_RETIRE_RETRY,
	CLUSTER_CONTROL_RETIRE_INVALID
} ClusterControlRetireVerb;

/* Decoded values only. Never send this native structure on the wire. */
typedef struct ClusterControlRetireMessage {
	ClusterControlRequestKey key;
	uint64 cleanup_epoch;
	uint64 exchange_id;
	uint64 previous_request;
	LOCKMODE previous_mode;
	ClusterControlRetireVerb verb;
} ClusterControlRetireMessage;

typedef struct ClusterControlRequestView {
	ClusterControlRequestHandle handle;
	ClusterControlRequestOwner owner;
	ClusterControlRequestCut cut;
	ClusterControlRetireMessage message;
	uint64 rebuild_previous_request; /* Private pair binding, never a wire grant. */
	uint64 driver;
	LOCKMODE mode;
	ClusterControlRequestState state;
	bool owner_exited;
} ClusterControlRequestView;

extern bool cluster_control_request_resid_valid(const ClusterResId *resid);
extern bool cluster_control_retire_encode(const ClusterControlRetireMessage *message,
										  uint8 out[CLUSTER_CONTROL_RETIRE_BYTES]);
extern bool cluster_control_retire_decode(const void *bytes, Size length,
										  ClusterControlRetireMessage *out);
extern void cluster_control_request_shmem_register(void);
extern Size cluster_control_request_shmem_size(void);
extern void cluster_control_request_shmem_init(void);

/* Call once per process, not once per private copy of a guard. */
extern bool cluster_control_request_owner_init(uint32 procno, int32 pid,
											   ClusterControlRequestOwner *out);
extern bool cluster_control_request_register(const ClusterControlRequestKey *key, LOCKMODE mode,
											 const ClusterControlRequestOwner *owner,
											 uint64 previous_request, LOCKMODE previous_mode,
											 ClusterControlRequestHandle *out);
extern bool cluster_control_request_mark_held(const ClusterControlRequestHandle *handle,
											  const ClusterControlRequestOwner *owner);
extern bool cluster_control_request_abandon(const ClusterControlRequestHandle *handle,
											const ClusterControlRequestOwner *owner);
extern void cluster_control_request_owner_exit(const ClusterControlRequestOwner *owner);
extern bool cluster_control_request_send_allowed(const ClusterControlRequestKey *key);
extern bool cluster_control_request_send_mode_allowed(const ClusterControlRequestKey *key,
													  LOCKMODE mode);
extern bool cluster_control_request_pause_upgrade(const ClusterControlRequestHandle *original,
												  const ClusterControlRequestOwner *owner);
extern bool cluster_control_request_confirm_upgrade(const ClusterControlRequestHandle *original,
													const ClusterControlRequestHandle *replacement,
													const ClusterControlRequestOwner *owner,
													const ClusterControlRequestCut *cut);
extern bool cluster_control_request_restore_upgrade(const ClusterControlRequestHandle *original,
													const ClusterControlRequestHandle *failed,
													const ClusterControlRequestOwner *owner,
													const ClusterControlRequestCut *cut);
extern bool
cluster_control_request_surrender_upgrade(const ClusterControlRequestHandle *original,
										  const ClusterControlRequestHandle *replacement,
										  const ClusterControlRequestOwner *owner);
/* Caller must prove the common rebuild gate before each step, outside the
 * registry LWLock. Finish restores only old ownership, never a new GRD grant. */
extern bool
cluster_control_request_rebuild_upgrade_begin(const ClusterControlRequestHandle *original,
											  const ClusterControlRequestHandle *failed,
											  const ClusterControlRequestOwner *owner);
extern bool cluster_control_request_rebuild_upgrade_finish(
	const ClusterControlRequestHandle *original, const ClusterControlRequestHandle *failed,
	const ClusterControlRequestOwner *owner, const ClusterControlRequestCut *cut);
extern bool cluster_control_request_begin_downgrade(const ClusterControlRequestHandle *handle,
													const ClusterControlRequestOwner *owner);

/* Only the LMON startup/service adapter owns these calls. A fresh shared
 * incarnation invalidates old driver snapshots; elapsed time never does. */
extern uint64 cluster_control_request_driver_start(void);
extern bool cluster_control_request_claim(const ClusterControlRequestHandle *handle, uint64 driver,
										  const ClusterControlRequestCut *cut,
										  ClusterControlRetireMessage *out);
extern bool cluster_control_request_ack(const ClusterControlRetireMessage *message, int32 source,
										uint64 driver, const ClusterControlRequestCut *cut);
extern bool cluster_control_request_ack_owner(const ClusterControlRetireMessage *message,
											  int32 source, uint64 driver,
											  const ClusterControlRequestCut *cut,
											  ClusterControlRequestOwner *notify_owner);
extern bool cluster_control_request_forget(const ClusterControlRequestHandle *handle,
										   const ClusterControlRequestOwner *owner,
										   const ClusterControlRequestCut *cut);
extern bool cluster_control_request_snapshot(const ClusterControlRequestHandle *handle,
											 ClusterControlRequestView *out);
extern bool cluster_control_request_next(uint32 *cursor, ClusterControlRequestView *out);

/* Read-only shared census, including creators no longer present in PGPROC.
 * A nonzero version may be checked again after the private-owner census.
 * At initial formation epoch zero, only an initialized empty registry passes;
 * this observation does not authorize epoch-zero control requests. */
extern bool cluster_control_request_census(uint64 epoch, uint64 *version);
extern bool cluster_control_request_census_unchanged(uint64 epoch, uint64 version);
extern bool cluster_control_request_empty(void);

#endif
