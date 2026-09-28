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

static bool
use_nonzero(const void *bytes, Size size)
{
	const uint8 *p = bytes;
	for (Size i = 0; i < size; i++)
		if (p[i] != 0)
			return true;
	return false;
}

static bool
use_overlap(const void *a, Size an, const void *b, Size bn)
{
	uintptr_t x = (uintptr_t)a, y = (uintptr_t)b;
	return a != NULL && b != NULL && (x <= y ? y - x < an : x - y < bn);
}

static bool
use_ref_valid(const ClusterSharedConfigRef *ref, uint32 node)
{
	return ref != NULL && node < 128 && ref->identity.system_identifier != 0
		   && ref->identity.database_incarnation != 0 && ref->identity.generation != 0
		   && (ref->identity.configured[node / 64] & (UINT64CONST(1) << (node % 64))) != 0
		   && use_nonzero(ref->identity.storage_uuid, 16)
		   && use_nonzero(ref->identity.authority_uuid, 16) && use_nonzero(ref->sha256, 32);
}

static bool
use_common_valid(const ClusterSharedConfigActive *common)
{
	return common != NULL && common->version == CLUSTER_SHARED_CONFIG_COMMON_VERSION
		   && common->static_entries > 0 && common->dynamic_entries > 0
		   && (uint64)common->static_entries + common->dynamic_entries
				  <= CLUSTER_SHARED_CONFIG_MAX_ENTRIES
		   && use_nonzero(common->static_sha256, 32) && use_nonzero(common->dynamic_sha256, 32);
}

bool
cluster_config_use_target_bind(ClusterConfigUseGate *gate, ClusterConfigUseTarget *target,
							   uint32 cut, uint32 node, const ClusterSharedConfigRef *ref,
							   const ClusterSharedConfigActive *common)
{
	ClusterConfigUseTarget next = { 0 };
	ClusterConfigUseGateState state;
	if (gate == NULL || target == NULL || cut == 0 || cut > USE_EPOCH_MAX
		|| use_overlap(target, sizeof(*target), gate, sizeof(*gate))
		|| use_overlap(target, sizeof(*target), ref, sizeof(*ref))
		|| use_overlap(target, sizeof(*target), common, sizeof(*common))
		|| !use_ref_valid(ref, node) || !use_common_valid(common))
		return false;
	state = cluster_config_use_gate_read(gate);
	if (!state.closed || state.owners != 0 || state.epoch != cut || target->epoch > cut)
		return false;
	next.epoch = cut;
	next.node_id = node;
	next.ref = *ref;
	next.common = *common;
	if (target->epoch == cut)
		return target->node_id == node && memcmp(&target->ref, ref, sizeof(*ref)) == 0
			   && memcmp(&target->common, common, sizeof(*common)) == 0;
	/* Exact CLOSED/zero prevents every independent or parallel entrant.
	 * The single controller publishes with OPEN only after this copy. */
	*target = next;
	return true;
}

bool
cluster_config_use_target_matches(const ClusterConfigUseTarget *target, uint32 epoch,
								  const ClusterSharedConfigRegistration *actual)
{
	ClusterSharedConfigIdentity identity;
	const ClusterSharedConfigProcess *p;
	if (target == NULL || actual == NULL || epoch == 0 || target->epoch != epoch
		|| !use_ref_valid(&target->ref, target->node_id) || !use_common_valid(&target->common)
		|| actual->pid <= 0 || actual->registration == 0 || !actual->observed)
		return false;
	p = &actual->process;
	if (p->applier_pid <= 0 || p->failed || p->parallel_snapshot || p->node_id != target->node_id
		|| !use_ref_valid(&p->ref, p->node_id)
		|| p->ref.identity.generation < target->ref.identity.generation
		|| memcmp(&target->common, &actual->common, sizeof(target->common)) != 0)
		return false;
	identity = p->ref.identity;
	identity.generation = target->ref.identity.generation;
	return memcmp(&identity, &target->ref.identity, sizeof(identity)) == 0
		   && (p->ref.identity.generation != target->ref.identity.generation
			   || memcmp(p->ref.sha256, target->ref.sha256, 32) == 0);
}

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
