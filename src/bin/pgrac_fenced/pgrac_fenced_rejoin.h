/*-------------------------------------------------------------------------
 *
 * pgrac_fenced_rejoin.h
 *    Root-daemon PFRJ rejoin operation core.
 *
 *-------------------------------------------------------------------------
 */
#ifndef PGRAC_FENCED_REJOIN_H
#define PGRAC_FENCED_REJOIN_H

#include "c.h"

#include "pgrac_fenced_operation.h"

#define PGRAC_FENCED_REJOIN_MAX_OPERATIONS UINT32_C(128)

#define PGRAC_FENCED_REJOIN_STATUS_PENDING UINT32_C(0)
#define PGRAC_FENCED_REJOIN_STATUS_OFFERED UINT32_C(1)
#define PGRAC_FENCED_REJOIN_STATUS_WAITING_JOINER UINT32_C(3)
#define PGRAC_FENCED_REJOIN_STATUS_READY UINT32_C(4)
#define PGRAC_FENCED_REJOIN_STATUS_REJECTED UINT32_C(5)
#define PGRAC_FENCED_REJOIN_STATUS_UNKNOWN UINT32_C(6)
#define PGRAC_FENCED_REJOIN_STATUS_UNAVAILABLE UINT32_C(7)
#define PGRAC_FENCED_REJOIN_STATUS_STALE UINT32_C(8)

typedef enum PgracFencedRejoinOperationState
{
	PGRAC_FENCED_REJOIN_OPERATION_UNUSED = 0,
	PGRAC_FENCED_REJOIN_OPERATION_OFFERED = 1,
	PGRAC_FENCED_REJOIN_OPERATION_CLAIMED = 2,
	PGRAC_FENCED_REJOIN_OPERATION_WAITING_JOINER = 3,
	PGRAC_FENCED_REJOIN_OPERATION_READY = 4
} PgracFencedRejoinOperationState;

typedef struct PgracFencedRejoinOperationV1
{
	PgracFencedRejoinOperationState state;
	PgracExternalFenceProtocolRejoinFrameV1 admin_request;
	uint8 operation_id[PGRAC_EXTERNAL_FENCE_NONCE_V1_BYTES];
	PgracFencedTargetV1 target;
	PgracExternalFenceProtocolRejoinFrameV1 offer_result;
	PgracExternalFenceProtocolRejoinFrameV1 on_result;
	PgracExternalFenceProtocolRejoinFrameV1 ready_result;
	/* PGRAC: current durable phase; transport withdrawal cannot erase it. */
	PgracFencedJournalRecordV1 last_record;
} PgracFencedRejoinOperationV1;

typedef struct PgracFencedRejoinContextV1
{
	PgracFencedOperationContextV1 *operation_context;
	uint32 operation_count;
	PgracFencedRejoinOperationV1
		operations[PGRAC_FENCED_REJOIN_MAX_OPERATIONS];
} PgracFencedRejoinContextV1;

extern bool pgrac_fenced_rejoin_init(
	PgracFencedRejoinContextV1 *context,
	PgracFencedOperationContextV1 *operation_context);
extern bool pgrac_fenced_rejoin_admin_prepare(
	PgracFencedRejoinContextV1 *context,
	const PgracExternalFenceProtocolRejoinFrameV1 *request,
	const uint8 operation_id[PGRAC_EXTERNAL_FENCE_NONCE_V1_BYTES],
	uint64 deadline_mono_ns,
	PgracExternalFenceProtocolRejoinFrameV1 *response);
extern bool pgrac_fenced_rejoin_claim(
	PgracFencedRejoinContextV1 *context,
	const PgracExternalFenceProtocolRejoinFrameV1 *request,
	uint64 deadline_mono_ns,
	PgracExternalFenceProtocolRejoinFrameV1 *response);
extern bool pgrac_fenced_rejoin_authorize_on(
	PgracFencedRejoinContextV1 *context,
	const PgracExternalFenceProtocolRejoinFrameV1 *request,
	bool target_admissions_invalidated,
	uint64 deadline_mono_ns,
	PgracExternalFenceProtocolRejoinFrameV1 *response);
extern bool pgrac_fenced_rejoin_refresh_on(
	PgracFencedRejoinContextV1 *context,
	const PgracExternalFenceProtocolRejoinFrameV1 *request,
	uint64 deadline_mono_ns,
	PgracExternalFenceProtocolRejoinFrameV1 *response);
extern bool pgrac_fenced_rejoin_cancel(
	PgracFencedRejoinContextV1 *context,
	const PgracExternalFenceProtocolRejoinFrameV1 *request);
extern const PgracFencedTargetV1 *pgrac_fenced_rejoin_target(
	const PgracFencedRejoinContextV1 *context,
	const uint8 operation_id[PGRAC_EXTERNAL_FENCE_NONCE_V1_BYTES]);
extern const PgracFencedTargetV1 *pgrac_fenced_rejoin_claim_target(
	const PgracFencedRejoinContextV1 *context);
extern const PgracFencedTargetV1 *pgrac_fenced_rejoin_request_target(
	const PgracFencedRejoinContextV1 *context,
	const PgracExternalFenceProtocolRejoinFrameV1 *request);

#endif /* PGRAC_FENCED_REJOIN_H */
