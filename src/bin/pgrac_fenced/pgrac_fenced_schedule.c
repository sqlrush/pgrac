/*-------------------------------------------------------------------------
 *
 * pgrac_fenced_schedule.c
 *    Bounded per-target operation scheduling for pgrac-fenced.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres_fe.h"

#include "pgrac_fenced_schedule.h"

static bool
bytes_nonzero(const uint8 *bytes, size_t len)
{
	size_t i;

	for (i = 0; i < len; i++)
	{
		if (bytes[i] != 0)
			return true;
	}
	return false;
}

static bool
state_joinable(PgracFencedState state)
{
	return state == PGRAC_FENCED_STATE_RESOLVING ||
		state == PGRAC_FENCED_STATE_ACTUATING ||
		state == PGRAC_FENCED_STATE_VERIFYING;
}

static PgracFencedScheduledOperationV1 *
ticket_operation(PgracFencedScheduleV1 *schedule,
				 const PgracFencedScheduleTicketV1 *ticket)
{
	PgracFencedScheduledOperationV1 *operation;

	if (schedule == NULL || ticket == NULL || ticket->serial == 0 ||
		ticket->slot >= PGRAC_FENCED_MAX_OPERATIONS)
		return NULL;
	operation = &schedule->operations[ticket->slot];
	if (!operation->used || operation->serial != ticket->serial)
		return NULL;
	return operation;
}

static const PgracFencedScheduledOperationV1 *
const_ticket_operation(const PgracFencedScheduleV1 *schedule,
					   const PgracFencedScheduleTicketV1 *ticket)
{
	const PgracFencedScheduledOperationV1 *operation;

	if (schedule == NULL || ticket == NULL || ticket->serial == 0 ||
		ticket->slot >= PGRAC_FENCED_MAX_OPERATIONS)
		return NULL;
	operation = &schedule->operations[ticket->slot];
	if (!operation->used || operation->serial != ticket->serial)
		return NULL;
	return operation;
}

static bool
client_present(const PgracFencedScheduleV1 *schedule, int client_id)
{
	uint32 i;
	uint32 j;

	for (i = 0; i < PGRAC_FENCED_MAX_OPERATIONS; i++)
	{
		const PgracFencedScheduledOperationV1 *operation =
			&schedule->operations[i];

		if (!operation->used)
			continue;
		for (j = 0; j < operation->client_count; j++)
		{
			if (operation->clients[j].client_id == client_id)
				return true;
		}
	}
	return false;
}

static bool
allocate_operation(PgracFencedScheduleV1 *schedule, uint32 *slot)
{
	uint32 i;

	if (schedule->operation_count >= PGRAC_FENCED_MAX_OPERATIONS)
		return false;
	for (i = 0; i < PGRAC_FENCED_MAX_OPERATIONS; i++)
	{
		if (!schedule->operations[i].used)
		{
			*slot = i;
			return true;
		}
	}
	return false;
}

static void
ticket_clear(PgracFencedScheduleTicketV1 *ticket)
{
	memset(ticket, 0, sizeof(*ticket));
}

static void
operation_remove(PgracFencedScheduleV1 *schedule, uint32 slot)
{
	PgracFencedScheduledOperationV1 *operation = &schedule->operations[slot];

	if (operation->active)
		schedule->active_count--;
	schedule->client_count -= operation->client_count;
	schedule->operation_count--;
	memset(operation, 0, sizeof(*operation));
}

static bool
start_fifo_head(PgracFencedScheduleV1 *schedule,
				const uint8 target_uuid[16],
				PgracFencedScheduleTicketV1 *started)
{
	PgracFencedScheduledOperationV1 *head = NULL;
	uint32 head_slot = 0;
	uint32 i;

	for (i = 0; i < PGRAC_FENCED_MAX_OPERATIONS; i++)
	{
		PgracFencedScheduledOperationV1 *operation = &schedule->operations[i];

		if (!operation->used || operation->active ||
			memcmp(operation->target_uuid, target_uuid, 16) != 0)
			continue;
		if (head == NULL || operation->queue_order < head->queue_order)
		{
			head = operation;
			head_slot = i;
		}
	}
	if (head == NULL)
		return true;
	head->active = true;
	head->state = PGRAC_FENCED_STATE_RESOLVING;
	head->queue_order = 0;
	schedule->active_count++;
	started->slot = head_slot;
	started->serial = head->serial;
	return true;
}

bool
pgrac_fenced_schedule_init(PgracFencedScheduleV1 *schedule)
{
	if (schedule == NULL)
		return false;
	memset(schedule, 0, sizeof(*schedule));
	schedule->next_serial = 1;
	schedule->next_queue_order = 1;
	return true;
}

bool
pgrac_fenced_schedule_init_owned(PgracFencedScheduleV1 *schedule)
{
	if (!pgrac_fenced_schedule_init(schedule))
		return false;
	schedule->caller_independent = true;
	return true;
}

static void
join_operation(PgracFencedScheduleV1 *schedule, uint32 slot, int client_id,
			   uint64 deadline_mono_ns, PgracFencedScheduleTicketV1 *ticket)
{
	PgracFencedScheduledOperationV1 *operation = &schedule->operations[slot];

	operation->clients[operation->client_count].client_id = client_id;
	operation->clients[operation->client_count].deadline_mono_ns = deadline_mono_ns;
	operation->client_count++;
	schedule->client_count++;
	ticket->slot = slot;
	ticket->serial = operation->serial;
}

PgracFencedScheduleResult
pgrac_fenced_schedule_submit(PgracFencedScheduleV1 *schedule,
						 int client_id, const uint8 target_uuid[16],
						 const uint8 binding_digest[32],
						 uint64 deadline_mono_ns,
						 PgracFencedScheduleTicketV1 *ticket)
{
	PgracFencedScheduledOperationV1 *active = NULL;
	PgracFencedScheduledOperationV1 *operation;
	uint32 active_slot = 0;
	uint32 slot;
	uint32 i;

	if (ticket == NULL)
		return PGRAC_FENCED_SCHEDULE_DENY;
	ticket_clear(ticket);
	if (schedule == NULL || client_id < -1 || target_uuid == NULL ||
		binding_digest == NULL ||
		(client_id == -1 && (!schedule->caller_independent || deadline_mono_ns != 0)) ||
		(client_id >= 0 && deadline_mono_ns == 0) ||
		!bytes_nonzero(target_uuid, 16) ||
		!bytes_nonzero(binding_digest, 32) ||
		(client_id >= 0 && (schedule->client_count >= PGRAC_FENCED_MAX_CLIENTS ||
		 client_present(schedule, client_id))))
		return PGRAC_FENCED_SCHEDULE_DENY;
	for (i = 0; i < PGRAC_FENCED_MAX_OPERATIONS; i++)
	{
		operation = &schedule->operations[i];
		if (!operation->used || !operation->active ||
			memcmp(operation->target_uuid, target_uuid, 16) != 0)
			continue;
		if (active != NULL)
			return PGRAC_FENCED_SCHEDULE_DENY;
		active = operation;
		active_slot = i;
	}
	if (client_id >= 0 && active != NULL && state_joinable(active->state) &&
		memcmp(active->binding_digest, binding_digest, 32) == 0)
	{
		if (active->client_count >= PGRAC_FENCED_MAX_CLIENTS)
			return PGRAC_FENCED_SCHEDULE_DENY;
		join_operation(schedule, active_slot, client_id, deadline_mono_ns, ticket);
		return PGRAC_FENCED_SCHEDULE_JOIN;
	}
	if (schedule->caller_independent && client_id >= 0)
	{
		/* A live waiter may join queued work; replayed UUIDs never coalesce. */
		for (i = 0; i < PGRAC_FENCED_MAX_OPERATIONS; i++)
		{
			operation = &schedule->operations[i];
			if (operation->used && !operation->active &&
				operation->state == PGRAC_FENCED_STATE_QUEUED &&
				memcmp(operation->target_uuid, target_uuid, 16) == 0 &&
				memcmp(operation->binding_digest, binding_digest, 32) == 0)
			{
				join_operation(schedule, i, client_id, deadline_mono_ns, ticket);
				return PGRAC_FENCED_SCHEDULE_JOIN;
			}
		}
	}
	if (!allocate_operation(schedule, &slot) || schedule->next_serial == 0 ||
		schedule->next_queue_order == 0)
		return PGRAC_FENCED_SCHEDULE_DENY;
	operation = &schedule->operations[slot];
	memset(operation, 0, sizeof(*operation));
	operation->used = true;
	operation->active = active == NULL;
	operation->state = active == NULL ? PGRAC_FENCED_STATE_RESOLVING :
		PGRAC_FENCED_STATE_QUEUED;
	memcpy(operation->target_uuid, target_uuid, 16);
	memcpy(operation->binding_digest, binding_digest, 32);
	operation->deadline_mono_ns = schedule->caller_independent ? 0 : deadline_mono_ns;
	operation->serial = schedule->next_serial++;
	if (!operation->active)
		operation->queue_order = schedule->next_queue_order++;
	schedule->operation_count++;
	if (client_id >= 0)
		join_operation(schedule, slot, client_id, deadline_mono_ns, ticket);
	if (operation->active)
		schedule->active_count++;
	ticket->slot = slot;
	ticket->serial = operation->serial;
	return operation->active ? PGRAC_FENCED_SCHEDULE_START :
		PGRAC_FENCED_SCHEDULE_QUEUE;
}

bool
pgrac_fenced_schedule_set_state(PgracFencedScheduleV1 *schedule,
						const PgracFencedScheduleTicketV1 *ticket,
						PgracFencedState state)
{
	PgracFencedScheduledOperationV1 *operation =
		ticket_operation(schedule, ticket);

	if (operation == NULL || !operation->active ||
		state < PGRAC_FENCED_STATE_RESOLVING ||
		state > PGRAC_FENCED_STATE_REENABLING)
		return false;
	operation->state = state;
	return true;
}

bool
pgrac_fenced_schedule_snapshot(const PgracFencedScheduleV1 *schedule,
					   const PgracFencedScheduleTicketV1 *ticket,
					   PgracFencedScheduleSnapshotV1 *snapshot)
{
	const PgracFencedScheduledOperationV1 *operation =
		const_ticket_operation(schedule, ticket);

	if (operation == NULL || snapshot == NULL)
		return false;
	memset(snapshot, 0, sizeof(*snapshot));
	snapshot->state = operation->state;
	snapshot->active = operation->active;
	memcpy(snapshot->target_uuid, operation->target_uuid, 16);
	memcpy(snapshot->binding_digest, operation->binding_digest, 32);
	snapshot->deadline_mono_ns = operation->deadline_mono_ns;
	snapshot->client_count = operation->client_count;
	return true;
}

bool
pgrac_fenced_schedule_release(PgracFencedScheduleV1 *schedule,
					  const PgracFencedScheduleTicketV1 *ticket,
					  PgracFencedScheduleTicketV1 *started)
{
	PgracFencedScheduledOperationV1 *operation =
		ticket_operation(schedule, ticket);
	uint8 target_uuid[16];
	uint32 slot = ticket == NULL ? 0 : ticket->slot;

	if (started == NULL)
		return false;
	ticket_clear(started);
	if (operation == NULL || !operation->active)
		return false;
	memcpy(target_uuid, operation->target_uuid, sizeof(target_uuid));
	operation_remove(schedule, slot);
	return start_fifo_head(schedule, target_uuid, started);
}

bool
pgrac_fenced_schedule_cancel_client(PgracFencedScheduleV1 *schedule,
						int client_id,
						PgracFencedScheduleTicketV1 *started)
{
	PgracFencedScheduledOperationV1 *operation;
	uint32 i;
	uint32 j;

	if (schedule == NULL || client_id < 0 || started == NULL)
		return false;
	ticket_clear(started);
	for (i = 0; i < PGRAC_FENCED_MAX_OPERATIONS; i++)
	{
		operation = &schedule->operations[i];
		if (!operation->used)
			continue;
		for (j = 0; j < operation->client_count; j++)
		{
			if (operation->clients[j].client_id != client_id)
				continue;
			operation->client_count--;
			if (j < operation->client_count)
				memmove(&operation->clients[j], &operation->clients[j + 1],
					(operation->client_count - j) *
					sizeof(operation->clients[0]));
			memset(&operation->clients[operation->client_count], 0,
				sizeof(operation->clients[0]));
			schedule->client_count--;
			if (operation->client_count == 0 && !operation->active &&
				!schedule->caller_independent)
			{
				schedule->operation_count--;
				memset(operation, 0, sizeof(*operation));
			}
			return true;
		}
	}
	return false;
}

uint32
pgrac_fenced_schedule_operation_count(const PgracFencedScheduleV1 *schedule)
{
	return schedule == NULL ? 0 : schedule->operation_count;
}

uint32
pgrac_fenced_schedule_active_count(const PgracFencedScheduleV1 *schedule)
{
	return schedule == NULL ? 0 : schedule->active_count;
}

bool
pgrac_fenced_schedule_copy_clients(
	const PgracFencedScheduleV1 *schedule,
	const PgracFencedScheduleTicketV1 *ticket,
	PgracFencedScheduledClientV1 *clients,
	uint32 maximum,
	uint32 *client_count)
{
	const PgracFencedScheduledOperationV1 *operation =
		const_ticket_operation(schedule, ticket);

	if (operation == NULL || clients == NULL || client_count == NULL ||
		maximum < operation->client_count)
		return false;
	memcpy(clients, operation->clients,
		operation->client_count * sizeof(operation->clients[0]));
	*client_count = operation->client_count;
	return true;
}
