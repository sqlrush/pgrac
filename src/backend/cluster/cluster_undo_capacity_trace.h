/* Author: SqlRush <sqlrush@gmail.com>
 * Copyright (c) 2026, pgrac contributors.
 * Private passive sinks for original capacity observations. No authority.
 */
#ifndef CLUSTER_UNDO_CAPACITY_TRACE_H
#define CLUSTER_UNDO_CAPACITY_TRACE_H

#include "cluster/cluster_terminal_ref_census.h"
#include "cluster/cluster_tt_slot.h"
#include "cluster/storage/cluster_undo_block0.h"

#define CLUSTER_UNDO_CAPACITY_ROUTE_TRACE_V2 1
#define CLUSTER_UNDO_CAPACITY_ROUTE_FINAL_READ_V2 1
enum { CAP_TRACE_CURRENT = 1, CAP_TRACE_POOL, CAP_TRACE_FALLBACK, CAP_TRACE_HISTORY };
enum { CAP_TRACE_PRESENT = 1, CAP_TRACE_KEY = 2, CAP_TRACE_OWNER = 4,
	CAP_TRACE_SLOT = 8, CAP_TRACE_ADMISSION = 16, CAP_TRACE_ROOT = 32,
	CAP_TRACE_GENERATION = 64, CAP_TRACE_ACQUIRE = 128, CAP_TRACE_RELEASE = 256,
	CAP_TRACE_GENERATION_KNOWN = 512, CAP_TRACE_CLEANER = 1024,
	CAP_TRACE_SAMPLE = 2048, CAP_TRACE_CAPACITY_MODE = 4096 };

typedef struct UndoCapacityTraceRow {
	ClusterCtrcTxnKeyV1 key;
	union {
		ClusterTTSlotCurrentOwner owner;
		struct { uint64 coordinator, worker; } cleaner;
	} owner_data;
	TTSlot slot;
	uint64 binding, formation, record_generation, root, root_generation, generation;
	const char *gate;
	uint16 known, position, sample_slot;
	int16 result;
	uint8 kind, branch, native_known, native_values, acquire, release, failure, generation_result;
} UndoCapacityTraceRow;

typedef struct UndoCapacityRouteTrace {
	UndoCapacityTraceRow current[48], pool[16], fallback, history;
	uint32 current_count, pool_count, dropped;
	uint32 current_gets, pool_gets;
	bool counters_known, current_captured, final_snapshot_known, final_snapshot_equal;
	bool final_read_known, final_read_ready;
	const char *final_field;
	uint64 final_before, final_after;
	int16 final_slot;
} UndoCapacityRouteTrace;

/* These sinks only copy caller values into the existing local refusal record. */
extern void cluster_undo_cleaner_capacity_trace_row(const UndoCapacityTraceRow *row);
extern void cluster_undo_cleaner_capacity_trace_counts(unsigned current_gets, unsigned pool_gets,
	bool captured);
extern void cluster_undo_cleaner_capacity_trace_final(bool equal,
	const ClusterTTSlotCurrentSnapshot *before, const ClusterTTSlotCurrentSnapshot *after);
extern void cluster_undo_cleaner_capacity_trace_reset(void);
extern void cluster_undo_cleaner_capacity_trace_final_read(bool ready);

static inline bool
capacity_trace_test(UndoCapacityTraceRow *row, const char *gate, bool result)
{
	if (row != NULL && !result && row->gate == NULL)
		row->gate = gate;
	return result;
}

static inline bool
capacity_trace_failed(UndoCapacityTraceRow *row, const char *gate, bool failed)
{
	(void)capacity_trace_test(row, gate, !failed);
	return failed;
}

static inline bool
capacity_trace_native(UndoCapacityTraceRow *row, unsigned bit, bool value)
{
	if (row != NULL) {
		row->native_known |= 1 << bit;
		if (value) row->native_values |= 1 << bit;
		else row->native_values &= ~(1 << bit);
	}
	return value;
}

static inline void
capacity_trace_key(UndoCapacityTraceRow *row, const ClusterCtrcTxnKeyV1 *key)
{
	if (row != NULL) {
		row->key = *key;
		row->known |= CAP_TRACE_KEY;
	}
}

static inline bool
capacity_trace_owner(UndoCapacityTraceRow *row, bool result, const ClusterCtrcTxnKeyV1 *key,
	const ClusterTTSlotCurrentOwner *owner, const uint64 *binding)
{
	if (row != NULL && result) {
		capacity_trace_key(row, key);
		row->owner_data.owner = *owner;
		row->binding = *binding;
		row->known |= CAP_TRACE_OWNER;
	}
	return result;
}

static inline bool
capacity_trace_cleaner(UndoCapacityTraceRow *row, bool result, const uint64 *coordinator,
	const uint64 *worker)
{
	if (row != NULL && result) {
		row->owner_data.cleaner.coordinator = *coordinator;
		row->owner_data.cleaner.worker = *worker;
		row->known |= CAP_TRACE_CLEANER;
	}
	return result;
}

static inline bool
capacity_trace_root(UndoCapacityTraceRow *row, bool result, const ClusterUndoBlock0ResolvedRoot *root)
{
	if (row != NULL && result) {
		row->root = root->root_id;
		row->root_generation = root->root_generation;
		row->known |= CAP_TRACE_ROOT;
	}
	return result;
}

static inline ClusterUndoBlock0Result
capacity_trace_generation(UndoCapacityTraceRow *row, ClusterUndoBlock0Result result,
	const ClusterUndoBlock0Generation *generation)
{
	if (row != NULL) {
		row->generation_result = result;
		row->known |= CAP_TRACE_GENERATION;
		row->known &= ~CAP_TRACE_GENERATION_KNOWN;
		if (result == CLUSTER_UNDO_BLOCK0_OK && generation->known) {
			row->generation = generation->value;
			row->known |= CAP_TRACE_GENERATION_KNOWN;
		}
	}
	return result;
}

static inline bool
capacity_trace_final_read(bool ready)
{
	cluster_undo_cleaner_capacity_trace_final_read(ready);
	return ready;
}

static inline bool
capacity_trace_final_snapshot(bool equal, const ClusterTTSlotCurrentSnapshot *before,
	const ClusterTTSlotCurrentSnapshot *after)
{
	cluster_undo_cleaner_capacity_trace_final(equal, before, after);
	return equal;
}

static inline int
capacity_trace_result(UndoCapacityTraceRow *row, int result)
{
	if (row != NULL) {
		row->result = result;
		cluster_undo_cleaner_capacity_trace_row(row);
	}
	return result;
}
#endif
