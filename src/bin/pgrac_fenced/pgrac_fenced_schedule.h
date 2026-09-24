/*-------------------------------------------------------------------------
 *
 * pgrac_fenced_schedule.h
 *    Bounded per-target operation scheduling for pgrac-fenced.
 *
 *-------------------------------------------------------------------------
 */
#ifndef PGRAC_FENCED_SCHEDULE_H
#define PGRAC_FENCED_SCHEDULE_H

#include "c.h"

#include "pgrac_fenced_core.h"

typedef enum PgracFencedScheduleResult
{
	PGRAC_FENCED_SCHEDULE_DENY = 0,
	PGRAC_FENCED_SCHEDULE_START = 1,
	PGRAC_FENCED_SCHEDULE_JOIN = 2,
	PGRAC_FENCED_SCHEDULE_QUEUE = 3
} PgracFencedScheduleResult;

typedef struct PgracFencedScheduleTicketV1
{
	uint32 slot;
	uint64 serial;
} PgracFencedScheduleTicketV1;

typedef struct PgracFencedScheduleSnapshotV1
{
	PgracFencedState state;
	bool active;
	uint8 target_uuid[16];
	uint8 binding_digest[32];
	uint64 deadline_mono_ns;
	uint32 client_count;
} PgracFencedScheduleSnapshotV1;

typedef struct PgracFencedScheduledClientV1
{
	int client_id;
	uint64 deadline_mono_ns;
} PgracFencedScheduledClientV1;

typedef struct PgracFencedScheduledOperationV1
{
	bool used;
	bool active;
	PgracFencedState state;
	uint8 target_uuid[16];
	uint8 binding_digest[32];
	uint64 deadline_mono_ns;
	uint64 serial;
	uint64 queue_order;
	uint32 client_count;
	PgracFencedScheduledClientV1 clients[PGRAC_FENCED_MAX_CLIENTS];
} PgracFencedScheduledOperationV1;

typedef struct PgracFencedScheduleV1
{
	bool caller_independent;
	uint64 next_serial;
	uint64 next_queue_order;
	uint32 operation_count;
	uint32 active_count;
	uint32 client_count;
	PgracFencedScheduledOperationV1 operations[PGRAC_FENCED_MAX_OPERATIONS];
} PgracFencedScheduleV1;

extern bool pgrac_fenced_schedule_init(PgracFencedScheduleV1 *schedule);
extern bool pgrac_fenced_schedule_init_owned(PgracFencedScheduleV1 *schedule);
extern PgracFencedScheduleResult pgrac_fenced_schedule_submit(
	PgracFencedScheduleV1 *schedule,
	int client_id,
	const uint8 target_uuid[16],
	const uint8 binding_digest[32],
	uint64 deadline_mono_ns,
	PgracFencedScheduleTicketV1 *ticket);
extern bool pgrac_fenced_schedule_set_state(
	PgracFencedScheduleV1 *schedule,
	const PgracFencedScheduleTicketV1 *ticket,
	PgracFencedState state);
extern bool pgrac_fenced_schedule_snapshot(
	const PgracFencedScheduleV1 *schedule,
	const PgracFencedScheduleTicketV1 *ticket,
	PgracFencedScheduleSnapshotV1 *snapshot);
extern bool pgrac_fenced_schedule_release(
	PgracFencedScheduleV1 *schedule,
	const PgracFencedScheduleTicketV1 *ticket,
	PgracFencedScheduleTicketV1 *started);
extern bool pgrac_fenced_schedule_cancel_client(
	PgracFencedScheduleV1 *schedule,
	int client_id,
	PgracFencedScheduleTicketV1 *started);
extern uint32 pgrac_fenced_schedule_operation_count(
	const PgracFencedScheduleV1 *schedule);
extern uint32 pgrac_fenced_schedule_active_count(
	const PgracFencedScheduleV1 *schedule);
extern bool pgrac_fenced_schedule_copy_clients(
	const PgracFencedScheduleV1 *schedule,
	const PgracFencedScheduleTicketV1 *ticket,
	PgracFencedScheduledClientV1 *clients,
	uint32 maximum,
	uint32 *client_count);

#endif /* PGRAC_FENCED_SCHEDULE_H */
