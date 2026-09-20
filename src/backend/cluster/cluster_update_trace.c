/*-------------------------------------------------------------------------
 * cluster_update_trace.c
 *   Per-UPDATE inclusive/exclusive accounting and finite diagnostic capture.
 *
 * Portions Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 * IDENTIFICATION
 *   src/backend/cluster/cluster_update_trace.c
 * NOTES
 *   No allocation, logging, lock acquisition, or product decisions on the
 *   timed path. Open frames are owned here, never by a longjmp'd C stack.
 *-------------------------------------------------------------------------
 */
#include "postgres.h"
#include "cluster/cluster_guc.h"
#include "cluster/cluster_shmem.h"
#include "cluster/cluster_update_trace.h"
#include "miscadmin.h"
#include "storage/backendid.h"
#include "storage/shmem.h"

#define TRACE_STACK_DEPTH 64

typedef struct TraceFrame {
	uint64 token;
	uint64 start_ns;
	uint64 child_ns;
	int bucket;
} TraceFrame;

PGDLLIMPORT ClusterUpdateTraceShared *ClusterUpdateTraceCtl = NULL;
static ClusterUpdateTraceSnapshot current;
static TraceFrame frames[TRACE_STACK_DEPTH];
static int depth;
static uint64 next_token;
static uint64 backend_sequence;
static bool collecting;

Size
cluster_update_trace_shmem_size(void)
{
	return MAXALIGN(sizeof(ClusterUpdateTraceShared));
}

void
cluster_update_trace_shmem_init(void)
{
	bool found;

	ClusterUpdateTraceCtl
		= ShmemInitStruct("pgrac cluster update trace", cluster_update_trace_shmem_size(), &found);
	if (!found) {
		uint32 i;

		pg_atomic_init_u64(&ClusterUpdateTraceCtl->next_op_id, 0);
		for (i = 0; i < CLUSTER_UPDATE_TRACE_MAX_RECORDS; i++)
			pg_atomic_init_u32(&ClusterUpdateTraceCtl->records[i].publication, 0);
		pg_atomic_init_u64(&ClusterUpdateTraceCtl->next_event, 0);
		for (i = 0; i < CLUSTER_UPDATE_TRACE_MAX_EVENTS; i++)
			pg_atomic_init_u32(&ClusterUpdateTraceCtl->events[i].published, 0);
	}
}

static const ClusterShmemRegion cluster_update_trace_region = {
	.name = "pgrac cluster update trace",
	.size_fn = cluster_update_trace_shmem_size,
	.init_fn = cluster_update_trace_shmem_init,
	.lwlock_count = 0,
	.owner_subsys = "cluster_update_trace",
	.reserved_flags = 0,
};

void
cluster_update_trace_shmem_register(void)
{
	cluster_shmem_register_region(&cluster_update_trace_region);
}

void
cluster_update_trace_event_at(const ClusterUpdateTraceEvent *event, uint64 now)
{
	ClusterUpdateTraceEventRecord *record;
	uint64 index;

	if (!cluster_update_trace_enabled || ClusterUpdateTraceCtl == NULL || event == NULL)
		return;
	if (event->kind == CLUTRACE_CR_LINK && !collecting)
		return;
	index = pg_atomic_fetch_add_u64(&ClusterUpdateTraceCtl->next_event, 1);
	if (index >= CLUSTER_UPDATE_TRACE_MAX_EVENTS)
		return;
	record = &ClusterUpdateTraceCtl->events[index];
	record->data = *event;
	record->data.stamp_ns = now;
	record->data.node_id = cluster_node_id;
	record->data.pid = MyProcPid;
	record->data.op_id = event->kind == CLUTRACE_CR_LINK ? current.op_id : 0;
	pg_write_barrier();
	pg_atomic_write_u32(&record->published, 1);
}

uint32
cluster_update_trace_event_count(void)
{
	uint64 count = ClusterUpdateTraceCtl == NULL
					   ? 0
					   : pg_atomic_read_u64(&ClusterUpdateTraceCtl->next_event);
	return (uint32)Min(count, (uint64)CLUSTER_UPDATE_TRACE_MAX_EVENTS);
}

void
cluster_update_trace_interval_enter_at(ClusterUpdateTraceInterval *interval,
									   const ClusterUpdateTraceEvent *event, uint64 now)
{
	if (!cluster_update_trace_enabled) {
		interval->active = false;
		return;
	}
	if (interval->active
		&& (interval->event.request_id != event->request_id
			|| interval->event.requester_node != event->requester_node
			|| interval->event.requester_backend != event->requester_backend
			|| interval->event.value != event->value)) {
		/* Identity replacement is evidence loss, never a completed phase. */
		interval->event.kind = CLUTRACE_ORIGIN_INCOMPLETE;
		cluster_update_trace_event_at(&interval->event, now);
		interval->active = false;
	}
	if (!interval->active) {
		interval->event = *event;
		interval->start_ns = now;
		interval->work_ns = 0;
		interval->active = true;
	}
	interval->entry_ns = now;
}

void
cluster_update_trace_interval_leave_at(ClusterUpdateTraceInterval *interval, bool finished,
									   uint64 now)
{
	if (!interval->active)
		return;
	if (now < interval->entry_ns || now < interval->start_ns) {
		interval->event.kind = CLUTRACE_ORIGIN_INCOMPLETE;
		cluster_update_trace_event_at(&interval->event, now);
		interval->active = false;
		return;
	}
	interval->work_ns += now - interval->entry_ns;
	if (finished) {
		interval->event.duration_ns = now - interval->start_ns;
		interval->event.work_ns = interval->work_ns;
		cluster_update_trace_event_at(&interval->event, now);
		interval->active = false;
	}
}

uint64
cluster_update_trace_event_dropped_count(void)
{
	uint64 count = ClusterUpdateTraceCtl == NULL
					   ? 0
					   : pg_atomic_read_u64(&ClusterUpdateTraceCtl->next_event);
	return count > CLUSTER_UPDATE_TRACE_MAX_EVENTS ? count - CLUSTER_UPDATE_TRACE_MAX_EVENTS : 0;
}

bool
cluster_update_trace_event_snapshot(uint32 slot, ClusterUpdateTraceEvent *event)
{
	ClusterUpdateTraceEventRecord *record;
	if (ClusterUpdateTraceCtl == NULL || event == NULL
		|| slot >= cluster_update_trace_event_count())
		return false;
	record = &ClusterUpdateTraceCtl->events[slot];
	if (pg_atomic_read_u32(&record->published) != 1)
		return false;
	pg_read_barrier();
	*event = record->data;
	return true;
}

void
cluster_update_trace_begin_at(ClusterUpdateTraceScope *scope, uint64 now)
{
	uint64 id;
	ClusterUpdateTraceRecord *record;

	scope->active = false;
	if (!cluster_update_trace_enabled || ClusterUpdateTraceCtl == NULL || collecting)
		return;
	id = pg_atomic_fetch_add_u64(&ClusterUpdateTraceCtl->next_op_id, 1) + 1;
	backend_sequence++;
	/* Drop explicitly instead of wrapping over a slow or incomplete writer. */
	if (id > CLUSTER_UPDATE_TRACE_MAX_RECORDS)
		return;
	memset(&current, 0, sizeof(current));
	current.op_id = id;
	current.backend_sequence = backend_sequence;
	current.start_ns = now;
	current.node_id = cluster_node_id;
	current.backend_id = MyBackendId;
	current.pid = MyProcPid;
	current.status = CLUTRACE_STATUS_INCOMPLETE;
	depth = 0;
	collecting = true;
	scope->active = true;
	scope->op_id = id;
	record = &ClusterUpdateTraceCtl->records[id - 1];
	record->data = current;
	pg_write_barrier();
	pg_atomic_write_u32(&record->publication, 1);
}

uint64
cluster_update_trace_phase_begin_at(int bucket, uint64 now)
{
	TraceFrame *frame;

	if (!collecting)
		return 0;
	if (bucket < 0 || bucket >= CLXP_NBUCKETS || depth == TRACE_STACK_DEPTH) {
		current.accounting_errors++;
		return 0;
	}
	frame = &frames[depth++];
	frame->token = ++next_token;
	frame->bucket = bucket;
	frame->start_ns = now;
	frame->child_ns = 0;
	return frame->token;
}

static void
close_frame(uint64 now)
{
	TraceFrame *frame = &frames[--depth];
	uint64 elapsed = now >= frame->start_ns ? now - frame->start_ns : 0;

	if (now < frame->start_ns || frame->child_ns > elapsed)
		current.accounting_errors++;
	current.phase_nanos[frame->bucket] += elapsed;
	current.phase_exclusive_nanos[frame->bucket] += elapsed - Min(elapsed, frame->child_ns);
	current.phase_events[frame->bucket]++;
	if (depth > 0)
		frames[depth - 1].child_ns += elapsed;
}

void
cluster_update_trace_phase_end_at(uint64 token, uint64 now)
{
	int pos;

	if (!collecting || token == 0)
		return;
	for (pos = depth - 1; pos >= 0; pos--)
		if (frames[pos].token == token)
			break;
	/* A pre-statement, duplicate, or earlier ERROR scope owns no current frame. */
	if (pos < 0)
		return;
	while (depth - 1 > pos) {
		current.accounting_errors++;
		close_frame(now);
	}
	close_frame(now);
}

void
cluster_update_trace_end_at(ClusterUpdateTraceScope *scope, uint32 status, uint64 affected_rows,
							uint64 now)
{
	ClusterUpdateTraceSnapshot *row;
	ClusterUpdateTraceRecord *record;
	uint64 exclusive = 0;
	uint64 wall;
	int i;

	if (!scope->active || !collecting || scope->op_id != current.op_id)
		return;
	while (depth > 0) {
		current.accounting_errors++;
		close_frame(now);
	}
	collecting = false;
	scope->active = false;
	wall = now >= current.start_ns ? now - current.start_ns : 0;
	for (i = 0; i < CLXP_NBUCKETS; i++)
		exclusive += current.phase_exclusive_nanos[i];
	if (exclusive > wall || now < current.start_ns)
		current.accounting_errors++;
	record = &ClusterUpdateTraceCtl->records[current.op_id - 1];
	row = &record->data;
	/* Do not rewrite immutable identity: ACTIVE snapshots can read it now. */
	row->status = status;
	row->end_ns = now;
	row->affected_rows = affected_rows;
	row->unattributed_ns = wall - Min(wall, exclusive);
	row->accounting_errors = current.accounting_errors;
	memcpy(row->phase_nanos, current.phase_nanos, sizeof(row->phase_nanos));
	memcpy(row->phase_exclusive_nanos, current.phase_exclusive_nanos,
		   sizeof(row->phase_exclusive_nanos));
	memcpy(row->phase_events, current.phase_events, sizeof(row->phase_events));
	pg_write_barrier();
	pg_atomic_write_u32(&record->publication, 2);
}

uint32
cluster_update_trace_snapshot_count(void)
{
	uint64 count = ClusterUpdateTraceCtl == NULL
					   ? 0
					   : pg_atomic_read_u64(&ClusterUpdateTraceCtl->next_op_id);

	return (uint32)Min(count, (uint64)CLUSTER_UPDATE_TRACE_MAX_RECORDS);
}

uint64
cluster_update_trace_dropped_count(void)
{
	uint64 count = ClusterUpdateTraceCtl == NULL
					   ? 0
					   : pg_atomic_read_u64(&ClusterUpdateTraceCtl->next_op_id);

	return count > CLUSTER_UPDATE_TRACE_MAX_RECORDS ? count - CLUSTER_UPDATE_TRACE_MAX_RECORDS : 0;
}

bool
cluster_update_trace_snapshot(uint32 slot, ClusterUpdateTraceSnapshot *snapshot)
{
	ClusterUpdateTraceRecord *record;
	uint32 state;

	if (ClusterUpdateTraceCtl == NULL || slot >= CLUSTER_UPDATE_TRACE_MAX_RECORDS
		|| snapshot == NULL)
		return false;
	record = &ClusterUpdateTraceCtl->records[slot];
	state = pg_atomic_read_u32(&record->publication);
	if (state == 0)
		return false;
	pg_read_barrier();
	if (state == 2)
		*snapshot = record->data;
	else {
		/* Only immutable fields may be read concurrently with completion. */
		memset(snapshot, 0, sizeof(*snapshot));
		snapshot->op_id = record->data.op_id;
		snapshot->backend_sequence = record->data.backend_sequence;
		snapshot->node_id = record->data.node_id;
		snapshot->backend_id = record->data.backend_id;
		snapshot->pid = record->data.pid;
		snapshot->start_ns = record->data.start_ns;
		snapshot->status = CLUTRACE_STATUS_INCOMPLETE;
	}
	return true;
}
