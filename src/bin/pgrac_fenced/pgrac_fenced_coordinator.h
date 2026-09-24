/*-------------------------------------------------------------------------
 *
 * pgrac_fenced_coordinator.h
 *    Socket, target scheduler and async-operation owner.
 *
 *-------------------------------------------------------------------------
 */
#ifndef PGRAC_FENCED_COORDINATOR_H
#define PGRAC_FENCED_COORDINATOR_H

#include "c.h"

#include "pgrac_fenced_async.h"
#include "pgrac_fenced_schedule.h"

typedef enum PgracFencedCoordinatorClientState
{
	PGRAC_FENCED_COORDINATOR_CLIENT_UNUSED = 0,
	PGRAC_FENCED_COORDINATOR_CLIENT_INGRESS = 1,
	PGRAC_FENCED_COORDINATOR_CLIENT_WAITING = 2,
	PGRAC_FENCED_COORDINATOR_CLIENT_RETAINED = 3
} PgracFencedCoordinatorClientState;

typedef struct PgracFencedCoordinatorClientV1
{
	int fd;
	PgracFencedCoordinatorClientState state;
	uint64 deadline_mono_ns;
	size_t request_frame_used;
	uint8 request_frame[PGRAC_EXTERNAL_FENCE_REQUEST_V1_BYTES];
	PgracFencedScheduleTicketV1 ticket;
	PgracExternalFenceProtocolRequestV1 request;
	PgracFencedPreparedAcquireV1 prepared;
	PgracExternalFenceProtocolResponseV1 response;
} PgracFencedCoordinatorClientV1;

typedef struct PgracFencedCoordinatorOperationV1
{
	bool worker_active;
	/* PGRAC: queue ownership survives the last caller and worker attempt. */
	bool owned_pending;
	bool retry_required;
	int owner_client_id;
	PgracFencedScheduleTicketV1 ticket;
	PgracExternalFenceProtocolRequestV1 request;
	PgracFencedPreparedAcquireV1 prepared;
	PgracFencedAsyncWorkerV1 worker;
} PgracFencedCoordinatorOperationV1;

typedef enum PgracFencedCoordinatorRejoinTargetResult
{
	PGRAC_FENCED_REJOIN_TARGET_ERROR = 0,
	PGRAC_FENCED_REJOIN_TARGET_WAITING = 1,
	PGRAC_FENCED_REJOIN_TARGET_READY = 2
} PgracFencedCoordinatorRejoinTargetResult;

typedef struct PgracFencedCoordinatorV1
{
	PgracFencedOperationContextV1 *context;
	PgracFencedScheduleV1 schedule;
	uint32 client_count;
	bool quiescing;
	uint32 rejoin_target_count;
	bool rejoin_target_used[PGRAC_FENCED_MAX_OPERATIONS];
	uint8 rejoin_targets[PGRAC_FENCED_MAX_OPERATIONS]
		[PGRAC_EXTERNAL_FENCE_NONCE_V1_BYTES];
	PgracFencedCoordinatorClientV1 clients[PGRAC_FENCED_MAX_CLIENTS];
	PgracFencedCoordinatorOperationV1 operations[PGRAC_FENCED_MAX_OPERATIONS];
} PgracFencedCoordinatorV1;

extern bool pgrac_fenced_coordinator_init(
	PgracFencedCoordinatorV1 *coordinator,
	PgracFencedOperationContextV1 *context);
extern bool pgrac_fenced_coordinator_restore(
	PgracFencedCoordinatorV1 *coordinator,
	PgracFencedJournalReconcileState *pending);
extern bool pgrac_fenced_coordinator_prepare_mapping_reload(
	PgracFencedCoordinatorV1 *coordinator,
	const PgracFencedConfigV1 *config,
	const PgracFencedProviderOpsV1 *provider,
	const uint8 semantic_config_digest[PGRAC_FENCED_CONFIG_DIGEST_BYTES]);
extern bool pgrac_fenced_coordinator_accept_fd(
	PgracFencedCoordinatorV1 *coordinator,
	int client_fd,
	uint64 transport_deadline_mono_ns);
extern bool pgrac_fenced_coordinator_service(
	PgracFencedCoordinatorV1 *coordinator,
	uint64 now_mono_ns);
extern bool pgrac_fenced_coordinator_quiesce(
	PgracFencedCoordinatorV1 *coordinator,
	uint32 deny_reason);
extern bool pgrac_fenced_coordinator_shutdown(
	PgracFencedCoordinatorV1 *coordinator,
	uint32 deny_reason);
extern uint32 pgrac_fenced_coordinator_active_worker_count(
	const PgracFencedCoordinatorV1 *coordinator);
extern PgracFencedCoordinatorRejoinTargetResult
pgrac_fenced_coordinator_rejoin_acquire_target(
	PgracFencedCoordinatorV1 *coordinator,
	const uint8 target_uuid[PGRAC_EXTERNAL_FENCE_NONCE_V1_BYTES]);
extern PgracFencedCoordinatorRejoinTargetResult
pgrac_fenced_coordinator_rejoin_invalidate_target(
	PgracFencedCoordinatorV1 *coordinator,
	const uint8 target_uuid[PGRAC_EXTERNAL_FENCE_NONCE_V1_BYTES],
	uint32 deny_reason);
extern bool pgrac_fenced_coordinator_rejoin_release_target(
	PgracFencedCoordinatorV1 *coordinator,
	const uint8 target_uuid[PGRAC_EXTERNAL_FENCE_NONCE_V1_BYTES]);

#endif /* PGRAC_FENCED_COORDINATOR_H */
