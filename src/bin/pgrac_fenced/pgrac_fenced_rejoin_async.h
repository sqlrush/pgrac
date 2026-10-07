/*-------------------------------------------------------------------------
 *
 * pgrac_fenced_rejoin_async.h
 *    Forked PFRJ phases with parent-owned state and durable sequencing.
 *
 *-------------------------------------------------------------------------
 */
#ifndef PGRAC_FENCED_REJOIN_ASYNC_H
#define PGRAC_FENCED_REJOIN_ASYNC_H

#include "c.h"

#include <sys/types.h>

#include "pgrac_fenced_rejoin.h"

typedef enum PgracFencedRejoinAsyncAction
{
	PGRAC_FENCED_REJOIN_ASYNC_ADMIN_PREPARE = 1,
	PGRAC_FENCED_REJOIN_ASYNC_CLAIM_NEXT = 2,
	PGRAC_FENCED_REJOIN_ASYNC_AUTHORIZE_ON = 3,
	PGRAC_FENCED_REJOIN_ASYNC_REFRESH_ON = 4,
	/* PGRAC: internal owner work, not a PFRJ client opcode. */
	PGRAC_FENCED_REJOIN_ASYNC_CLEANUP = 5
} PgracFencedRejoinAsyncAction;

typedef enum PgracFencedRejoinAsyncEvent
{
	PGRAC_FENCED_REJOIN_ASYNC_NONE = 0,
	PGRAC_FENCED_REJOIN_ASYNC_JOURNAL = 1,
	PGRAC_FENCED_REJOIN_ASYNC_PROOF = 2,
	PGRAC_FENCED_REJOIN_ASYNC_COMPLETE = 3
} PgracFencedRejoinAsyncEvent;

typedef struct PgracFencedRejoinAsyncWorkerV1
{
	pid_t pid;
	int fd;
	int wait_status;
	PgracFencedRejoinAsyncAction action;
	bool active;
	/* PGRAC: parent-owned snapshot, never taken from a completion message. */
	bool owned;
	uint32 operation_slot;
	uint32 operation_count;
	uint64 proof_generation;
	PgracExternalFenceProtocolRejoinFrameV1 request;
	PgracFencedRejoinOperationV1 before;
	PgracFencedJournalRecordV1 identity;
	PgracFencedJournalRecordV1 last_record;
} PgracFencedRejoinAsyncWorkerV1;

extern bool pgrac_fenced_rejoin_async_start(
	PgracFencedOperationContextV1 *operation_context,
	const PgracFencedRejoinContextV1 *rejoin_context,
	PgracFencedRejoinAsyncAction action,
	const PgracExternalFenceProtocolRejoinFrameV1 *request,
	const uint8 operation_id[PGRAC_EXTERNAL_FENCE_NONCE_V1_BYTES],
	bool target_admissions_invalidated,
	uint64 deadline_mono_ns,
	PgracFencedRejoinAsyncWorkerV1 *worker);
extern int pgrac_fenced_rejoin_async_fd(
	const PgracFencedRejoinAsyncWorkerV1 *worker);
extern bool pgrac_fenced_rejoin_async_service(
	PgracFencedOperationContextV1 *operation_context,
	PgracFencedRejoinContextV1 *rejoin_context,
	PgracFencedRejoinAsyncWorkerV1 *worker,
	PgracFencedRejoinAsyncEvent *event,
	PgracExternalFenceProtocolRejoinFrameV1 *response);

#endif /* PGRAC_FENCED_REJOIN_ASYNC_H */
