/*-------------------------------------------------------------------------
 *
 * pgrac_fenced_rejoin_coordinator.h
 *    Nonblocking PFRJ socket and exact-target reservation owner.
 *
 *-------------------------------------------------------------------------
 */
#ifndef PGRAC_FENCED_REJOIN_COORDINATOR_H
#define PGRAC_FENCED_REJOIN_COORDINATOR_H

#include "c.h"

#include "pgrac_fenced_coordinator.h"
#include "pgrac_fenced_rejoin_async.h"

#define PGRAC_FENCED_REJOIN_MAX_CLIENTS UINT32_C(128)

typedef enum PgracFencedRejoinClientState
{
	PGRAC_FENCED_REJOIN_CLIENT_UNUSED = 0,
	PGRAC_FENCED_REJOIN_CLIENT_INGRESS = 1,
	PGRAC_FENCED_REJOIN_CLIENT_READY = 2,
	PGRAC_FENCED_REJOIN_CLIENT_WAIT_RESERVATION = 3,
	PGRAC_FENCED_REJOIN_CLIENT_WORKER = 4,
	PGRAC_FENCED_REJOIN_CLIENT_EGRESS = 5
} PgracFencedRejoinClientState;

typedef struct PgracFencedRejoinClientV1
{
	int fd;
	PgracFencedRejoinClientState state;
	bool is_admin;
	bool abandoned;
	bool owns_operation;
	bool target_reserved;
	uint64 transport_deadline_mono_ns;
	uint64 operation_deadline_mono_ns;
	size_t request_frame_used;
	size_t response_frame_sent;
	uint8 request_frame[PGRAC_EXTERNAL_FENCE_REJOIN_V1_BYTES];
	uint8 response_frame[PGRAC_EXTERNAL_FENCE_REJOIN_V1_BYTES];
	uint8 operation_id[PGRAC_EXTERNAL_FENCE_NONCE_V1_BYTES];
	uint8 reserved_target[PGRAC_EXTERNAL_FENCE_NONCE_V1_BYTES];
	PgracExternalFenceProtocolRejoinFrameV1 request;
	PgracExternalFenceProtocolRejoinFrameV1 response;
	PgracFencedRejoinAsyncAction action;
} PgracFencedRejoinClientV1;

typedef struct PgracFencedRejoinCoordinatorV1
{
	PgracFencedOperationContextV1 *operation_context;
	PgracFencedCoordinatorV1 *scalar_coordinator;
	PgracFencedRejoinContextV1 rejoin_context;
	PgracFencedRejoinAsyncWorkerV1 worker;
	int worker_owner;
	uint32 cleanup_cursor;
	uint32 client_count;
	bool quiescing;
	PgracFencedRejoinClientV1 clients[PGRAC_FENCED_REJOIN_MAX_CLIENTS];
} PgracFencedRejoinCoordinatorV1;

extern bool pgrac_fenced_rejoin_coordinator_init(
	PgracFencedRejoinCoordinatorV1 *coordinator,
	PgracFencedOperationContextV1 *operation_context,
	PgracFencedCoordinatorV1 *scalar_coordinator);
extern bool pgrac_fenced_rejoin_coordinator_restore(
	PgracFencedRejoinCoordinatorV1 *coordinator,
	PgracFencedJournalReconcileState *pending);
extern bool pgrac_fenced_rejoin_coordinator_accept_fd(
	PgracFencedRejoinCoordinatorV1 *coordinator,
	int client_fd,
	bool is_admin,
	uint64 transport_deadline_mono_ns);
extern bool pgrac_fenced_rejoin_coordinator_service(
	PgracFencedRejoinCoordinatorV1 *coordinator,
	uint64 now_mono_ns);
extern bool pgrac_fenced_rejoin_coordinator_quiesce(
	PgracFencedRejoinCoordinatorV1 *coordinator);
extern bool pgrac_fenced_rejoin_coordinator_shutdown(
	PgracFencedRejoinCoordinatorV1 *coordinator);
extern uint32 pgrac_fenced_rejoin_coordinator_active_worker_count(
	const PgracFencedRejoinCoordinatorV1 *coordinator);
extern uint32 pgrac_fenced_rejoin_coordinator_operation_count(
	const PgracFencedRejoinCoordinatorV1 *coordinator);

#endif /* PGRAC_FENCED_REJOIN_COORDINATOR_H */
