/*-------------------------------------------------------------------------
 *
 * cluster_config_use_gate.c
 *    Hold native producers across a configuration application episode.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *    src/backend/cluster/cluster_config_use_gate.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"
#include "cluster/cluster_config_use_gate.h"

#define USE_CLOSED UINT64CONST(0x8000000000000000)
#define USE_EPOCH_MASK UINT64CONST(0x7fffffff00000000)
#define USE_COUNT_MASK UINT64CONST(0x00000000ffffffff)
#define USE_EPOCH_MAX UINT32_C(0x7fffffff)

void
cluster_config_use_gate_init(ClusterConfigUseGate *gate)
{
	pg_atomic_init_u64(&gate->state, UINT64CONST(1) << 32);
}

ClusterConfigUseGateState
cluster_config_use_gate_read(ClusterConfigUseGate *gate)
{
	uint64 word = pg_atomic_read_u64(&gate->state);
	ClusterConfigUseGateState out;
	out.closed = (word & USE_CLOSED) != 0;
	out.epoch = (word & USE_EPOCH_MASK) >> 32;
	out.owners = word & USE_COUNT_MASK;
	return out;
}

bool
cluster_config_use_gate_close(ClusterConfigUseGate *gate, uint32 *cut)
{
	uint64 old = pg_atomic_read_u64(&gate->state);
	*cut = 0;
	for (;;) {
		uint32 epoch = (old & USE_EPOCH_MASK) >> 32;
		uint64 next;
		if ((old & USE_CLOSED) != 0) {
			*cut = epoch;
			return epoch != 0;
		}
		/* Exhaustion is a held, unopenable cut (epoch zero), not wraparound.
		 * Existing owners still retire normally. Only a new native family
		 * may initialize it again after all old children have gone. */
		if (epoch == USE_EPOCH_MAX || epoch == 0)
			next = USE_CLOSED | (old & USE_COUNT_MASK);
		else
			next = USE_CLOSED | ((uint64)(epoch + 1) << 32) | (old & USE_COUNT_MASK);
		if (pg_atomic_compare_exchange_u64(&gate->state, &old, next)) {
			*cut = (next & USE_EPOCH_MASK) >> 32;
			return *cut != 0;
		}
	}
}

bool
cluster_config_use_gate_open(ClusterConfigUseGate *gate, uint32 cut)
{
	uint64 expected;
	if (cut == 0 || cut > USE_EPOCH_MAX)
		return false;
	expected = USE_CLOSED | ((uint64)cut << 32);
	/* A competing continuation either joins before this CAS (OPEN fails)
	 * or observes zero and cannot invent an old owner after OPEN. */
	return pg_atomic_compare_exchange_u64(&gate->state, &expected, (uint64)cut << 32);
}

bool
cluster_config_use_gate_enter(ClusterConfigUseGate *gate, bool continuation, uint32 *epoch)
{
	uint64 old = pg_atomic_read_u64(&gate->state);
	*epoch = 0;
	for (;;) {
		uint32 generation = (old & USE_EPOCH_MASK) >> 32;
		uint32 owners = old & USE_COUNT_MASK;
		bool closed = (old & USE_CLOSED) != 0;
		if (generation == 0 || owners == PG_UINT32_MAX
			|| (closed && (!continuation || owners == 0 || generation <= 1)))
			return false;
		if (pg_atomic_compare_exchange_u64(&gate->state, &old, old + 1)) {
			*epoch = closed ? generation - 1 : generation;
			return true;
		}
	}
}

bool
cluster_config_use_gate_leave(ClusterConfigUseGate *gate)
{
	uint64 old = pg_atomic_read_u64(&gate->state);
	for (;;) {
		if ((old & USE_COUNT_MASK) == 0)
			return false;
		if (pg_atomic_compare_exchange_u64(&gate->state, &old, old - 1))
			return true;
	}
}
