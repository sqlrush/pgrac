/*-------------------------------------------------------------------------
 * test_cluster_update_trace.c
 *   Deterministic tests of the real per-UPDATE diagnostic accounting.
 *
 * Portions Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 * IDENTIFICATION
 *   src/test/cluster_unit/test_cluster_update_trace.c
 * NOTES
 *   Only PostgreSQL process/shmem boundaries are stubbed, not accounting.
 *-------------------------------------------------------------------------
 */
#include "postgres.h"
#include "cluster/cluster_update_trace.h"
#include "cluster/cluster_shmem.h"
#include "storage/backendid.h"

#undef printf
#undef fprintf
#undef snprintf
#include <assert.h>
#include <stdio.h>
#include <stdlib.h>

bool cluster_update_trace_enabled = false;
int cluster_node_id = 2;
int MyProcPid = 1234;
BackendId MyBackendId = 7;

void *ShmemInitStruct(const char *name, Size size, bool *found);
void *
ShmemInitStruct(const char *name, Size size, bool *found)
{
	(void)name;
	*found = false;
	return calloc(1, size);
}

void
cluster_shmem_register_region(const ClusterShmemRegion *region)
{
	(void)region;
}

void
ExceptionalCondition(const char *condition, const char *file, int line)
{
	fprintf(stderr, "%s at %s:%d\n", condition, file, line);
	abort();
}

int
main(void)
{
	ClusterUpdateTraceScope scope;
	ClusterUpdateTraceScope nested;
	ClusterUpdateTraceSnapshot row;
	uint64 parent;
	uint64 child;
	uint32 i;
	ClusterUpdateTraceEvent event = { 0 };
	ClusterUpdateTraceEvent observed;
	ClusterUpdateTraceInterval interval = { 0 };
	uint32 interval_records;

	cluster_update_trace_shmem_init();
	cluster_update_trace_begin_at(&scope, 100);
	assert(!scope.active);
	assert(cluster_update_trace_snapshot_count() == 0);
	event.kind = CLUTRACE_CR_LINK;
	event.request_id = 42;
	cluster_update_trace_event_at(&event, 90);
	assert(cluster_update_trace_event_count() == 0);

	cluster_update_trace_enabled = true;
	cluster_update_trace_begin_at(&scope, 100);
	assert(cluster_update_trace_snapshot(0, &row));
	assert(row.status == CLUTRACE_STATUS_INCOMPLETE && row.end_ns == 0);
	assert(row.pid == 1234 && row.node_id == 2 && row.backend_sequence == 1);
	cluster_update_trace_event_at(&event, 105);
	assert(cluster_update_trace_event_snapshot(0, &observed));
	assert(observed.op_id == 1 && observed.request_id == 42 && observed.stamp_ns == 105);
	assert(observed.node_id == 2 && observed.pid == 1234);
	/* Background service identity is caller-provided, not inferred from time. */
	event.kind = CLUTRACE_CR_STATE;
	event.requester_node = 1;
	event.requester_backend = 8;
	event.generation = 9;
	event.value = 5;
	cluster_update_trace_event_at(&event, 106);
	assert(cluster_update_trace_event_snapshot(1, &observed));
	assert(observed.op_id == 0 && observed.requester_node == 1 && observed.generation == 9);
	cluster_update_trace_begin_at(&nested, 101);
	assert(!nested.active);
	parent = cluster_update_trace_phase_begin_at(CLXP_W_GES_ENQUEUE, 110);
	child = cluster_update_trace_phase_begin_at(CLXP_W_GES_WAIT, 120);
	cluster_update_trace_phase_end_at(child, 150);
	cluster_update_trace_phase_end_at(parent, 170);
	cluster_update_trace_end_at(&scope, CLUTRACE_STATUS_OK, 1, 200);
	assert(cluster_update_trace_snapshot(0, &row));
	assert(row.phase_nanos[CLXP_W_GES_ENQUEUE] == 60);
	assert(row.phase_exclusive_nanos[CLXP_W_GES_ENQUEUE] == 30);
	assert(row.phase_exclusive_nanos[CLXP_W_GES_WAIT] == 30);
	assert(row.unattributed_ns == 40 && row.accounting_errors == 0);
	assert(row.affected_rows == 1 && row.end_ns == 200);

	/* An ERROR unwinds open timers without retaining stack addresses. */
	cluster_update_trace_begin_at(&scope, 300);
	parent = cluster_update_trace_phase_begin_at(CLXP_I_TX_WAIT, 310);
	cluster_update_trace_end_at(&scope, CLUTRACE_STATUS_ERROR, 0, 350);
	assert(cluster_update_trace_snapshot(1, &row));
	assert(row.status == CLUTRACE_STATUS_ERROR);
	assert(row.phase_exclusive_nanos[CLXP_I_TX_WAIT] == 40);
	assert(row.unattributed_ns == 10 && row.accounting_errors == 1);
	/* A stale scope end cannot close a later operation's phase. */
	cluster_update_trace_begin_at(&scope, 400);
	child = cluster_update_trace_phase_begin_at(CLXP_I_TX_WAIT, 410);
	cluster_update_trace_phase_end_at(parent, 420);
	cluster_update_trace_phase_end_at(child, 430);
	cluster_update_trace_end_at(&scope, CLUTRACE_STATUS_OK, 1, 450);
	assert(cluster_update_trace_snapshot(2, &row));
	assert(row.phase_nanos[CLXP_I_TX_WAIT] == 20 && row.accounting_errors == 0);

	/* Residence includes the gap between polls, work only the calls themselves. */
	interval_records = cluster_update_trace_event_count();
	event.kind = CLUTRACE_ORIGIN_PHASE;
	event.value = 0x401;
	cluster_update_trace_interval_enter_at(&interval, &event, 500);
	cluster_update_trace_interval_leave_at(&interval, false, 510);
	cluster_update_trace_interval_enter_at(&interval, &event, 600);
	cluster_update_trace_interval_leave_at(&interval, true, 620);
	assert(cluster_update_trace_event_snapshot(interval_records, &observed));
	interval_records++;
	assert(observed.duration_ns == 120 && observed.work_ns == 30);
	assert(observed.value == 0x401 && !interval.active);
	/* A different identity cannot inherit the old phase's age. */
	cluster_update_trace_interval_enter_at(&interval, &event, 700);
	cluster_update_trace_interval_leave_at(&interval, false, 710);
	event.request_id++;
	cluster_update_trace_interval_enter_at(&interval, &event, 800);
	cluster_update_trace_interval_leave_at(&interval, true, 810);
	assert(cluster_update_trace_event_snapshot(interval_records, &observed));
	interval_records++;
	assert(observed.kind == CLUTRACE_ORIGIN_INCOMPLETE && observed.request_id == 42);
	assert(cluster_update_trace_event_snapshot(interval_records, &observed));
	interval_records++;
	assert(observed.request_id == 43 && observed.duration_ns == 10 && observed.work_ns == 10);
	cluster_update_trace_enabled = false;
	cluster_update_trace_interval_enter_at(&interval, &event, 850);
	cluster_update_trace_interval_leave_at(&interval, true, 860);
	assert(!interval.active && cluster_update_trace_event_count() == interval_records);
	cluster_update_trace_enabled = true;

	/* Finite capture: full never overwrites the earliest or live records. */
	for (i = 3; i < CLUSTER_UPDATE_TRACE_MAX_RECORDS; i++) {
		cluster_update_trace_begin_at(&scope, 500 + i * 2);
		cluster_update_trace_end_at(&scope, CLUTRACE_STATUS_OK, 1, 501 + i * 2);
	}
	cluster_update_trace_begin_at(&scope, 1000000);
	assert(!scope.active);
	assert(cluster_update_trace_dropped_count() == 1);
	assert(cluster_update_trace_snapshot(0, &row) && row.start_ns == 100);
	assert(cluster_update_trace_snapshot_count() == CLUSTER_UPDATE_TRACE_MAX_RECORDS);
	for (i = interval_records; i < CLUSTER_UPDATE_TRACE_MAX_EVENTS; i++)
		cluster_update_trace_event_at(&event, 2000000 + i);
	cluster_update_trace_event_at(&event, 3000000);
	assert(cluster_update_trace_event_dropped_count() == 1);
	assert(cluster_update_trace_event_snapshot(0, &observed) && observed.request_id == 42);
	puts("update trace: disabled, nesting, exclusive conservation, ERROR, stale token, finite "
		 "capture PASS");
	return 0;
}
