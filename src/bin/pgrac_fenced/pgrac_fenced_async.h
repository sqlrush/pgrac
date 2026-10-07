/*-------------------------------------------------------------------------
 *
 * pgrac_fenced_async.h
 *    Forked operation execution with parent-owned journal serialization.
 *
 *-------------------------------------------------------------------------
 */
#ifndef PGRAC_FENCED_ASYNC_H
#define PGRAC_FENCED_ASYNC_H

#include "c.h"

#include <sys/types.h>

#include "pgrac_fenced_operation.h"

typedef enum PgracFencedAsyncEvent
{
	PGRAC_FENCED_ASYNC_NONE = 0,
	PGRAC_FENCED_ASYNC_JOURNAL = 1,
	PGRAC_FENCED_ASYNC_COMPLETE = 2
} PgracFencedAsyncEvent;

typedef struct PgracFencedAsyncWorkerV1
{
	pid_t pid;
	int fd;
	int wait_status;
	uint16 last_record_kind;
	bool active;
	/* PGRAC: parent-owned execution identity, never a reused client pointer. */
	PgracFencedJournalRecordV1 accepted_record;
	PgracFencedJournalRecordV1 last_record;
	uint64 proof_generation;
} PgracFencedAsyncWorkerV1;

extern bool pgrac_fenced_async_start(
	PgracFencedOperationContextV1 *context,
	const PgracExternalFenceProtocolRequestV1 *request,
	uint64 deadline_mono_ns,
	PgracFencedAsyncWorkerV1 *worker);
extern bool pgrac_fenced_async_start_preaccepted(
	PgracFencedOperationContextV1 *context,
	const PgracExternalFenceProtocolRequestV1 *request,
	const PgracFencedPreparedAcquireV1 *prepared,
	uint64 deadline_mono_ns,
	PgracFencedAsyncWorkerV1 *worker);
extern int pgrac_fenced_async_fd(const PgracFencedAsyncWorkerV1 *worker);
extern bool pgrac_fenced_async_service(
	PgracFencedOperationContextV1 *context,
	PgracFencedAsyncWorkerV1 *worker,
	PgracFencedAsyncEvent *event,
	PgracExternalFenceProtocolResponseV1 *response);

#endif /* PGRAC_FENCED_ASYNC_H */
