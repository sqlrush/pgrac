/*-------------------------------------------------------------------------
 *
 * cluster_config_producers.c
 *    Retain ordered local producer cuts in the original LMON owner.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 * IDENTIFICATION
 *    src/backend/cluster/cluster_config_producers.c
 * NOTES
 *    PGRAC-original local adapter. No global retirement or APPLY authority:
 *    the retained cluster controller owns the external phase prerequisites.
 *-------------------------------------------------------------------------
 */
#include "postgres.h"
#include "cluster/cluster_config_producers.h"
#include "cluster/cluster_config_channels.h"
#include "cluster/cluster_clean_leave.h"
#include "cluster/cluster_guc.h"
#include "miscadmin.h"
#include "storage/proc.h"

#define PRODUCER_ALL ((UINT32_C(1) << CLUSTER_CONFIG_PRODUCERS_COUNT) - 1)
StaticAssertDecl(CLUSTER_CONFIG_PRODUCERS_COUNT == 8, "ordered native producer roster");

typedef struct ProducerCut {
	ClusterConfigMembersKey key;
	ClusterConfigChannelOwner owner;
	uint8 episode[16];
	ClusterConfigUseGate *gate[CLUSTER_CONFIG_PRODUCERS_COUNT];
	uint32 cookie[CLUSTER_CONFIG_PRODUCERS_COUNT];
	uint32 held;
	ClusterConfigProducerStage stage;
	ClusterSharedConfigActive common;
	bool active;
	bool invalid;
	bool bound;
	bool released;
} ProducerCut;

static ProducerCut producer_cut;

static ClusterConfigUseGate *
producer_gate(unsigned i, bool *failed)
{
	*failed = false;
	if (i == 0)
		return cluster_shared_config_delivery_native_gate();
	if (i == 1)
		return cluster_shared_config_delivery_cleaner_gate(failed);
	return cluster_shared_config_delivery_background_gate(i - 2, failed);
}

static ClusterConfigUseTarget *
producer_target(unsigned i)
{
	if (i == 0)
		return cluster_shared_config_delivery_native_target();
	if (i == 1)
		return cluster_shared_config_delivery_cleaner_target();
	return cluster_shared_config_delivery_background_target(i - 2);
}

/* Actual original registration, never a caller's desired process identity. */
static bool
producer_self(ClusterConfigChannelOwner *out)
{
	ClusterSharedConfigRegistration reg;
	uint64 sequence;
	int32 pid;
	if (!IsUnderPostmaster || !cluster_enabled || !cluster_shared_config || MyBackendType != B_LMON
		|| !AmLmonProcess() || MyProc == NULL || ProcGlobal == NULL || ProcGlobal->allProcs == NULL
		|| MyProc->pgprocno < 0 || (uint32)MyProc->pgprocno >= ProcGlobal->allProcCount
		|| MyProc != &ProcGlobal->allProcs[MyProc->pgprocno] || MyProcPid <= 0
		|| MyProc->pid != MyProcPid || cluster_normal_stop_requested()
		|| cluster_shared_config_delivery_channels(&pid) == NULL || pid != MyProcPid)
		return false;
	sequence = pg_atomic_read_u64(&MyProc->cluster_config.sequence);
	if ((sequence & 1) != 0
		|| !cluster_shared_config_registration_read(&MyProc->cluster_config, &reg) || !reg.observed
		|| reg.registration == 0 || reg.pid != MyProcPid || reg.role != B_LMON
		|| reg.aux_type != LmonProcess
		|| sequence != pg_atomic_read_u64(&MyProc->cluster_config.sequence))
		return false;
	memset(out, 0, sizeof(*out));
	out->pid = MyProcPid;
	out->procno = MyProc->pgprocno;
	out->registration = reg.registration;
	return true;
}

static bool
producer_key(const ClusterSharedConfigRef *ref, ClusterConfigMembersKey *key)
{
	ClusterR4MembershipSnapshot members;
	return ref != NULL && cluster_node_id >= 0 && cluster_node_id < CLUSTER_MAX_NODES
		   && cluster_reconfig_lmon_snapshot_r4_membership(&members)
		   && cluster_config_members_make_key(ref, &members, key);
}

bool
cluster_config_producers_fresh_allowed(ClusterConfigProducerStage stage)
{
	ClusterConfigChannelOwner owner;
	ClusterConfigUseGate *gate;
	bool failed = false;
	if (stage != CLUSTER_CONFIG_PRODUCERS_FRONT && stage != CLUSTER_CONFIG_PRODUCERS_QUIET)
		return false;
	if (!cluster_enabled || !cluster_shared_config)
		return true;
	if (!producer_self(&owner))
		return false;
	/* The sole original LMON closes these gates in normal context. No second
	 * local closer can race this same-process fresh entry. Read the family
	 * gate even after a private owner is lost; replacement cannot undo CLOSE.
	 * QUIET's first gate is WALWRITER, before any other periodic producer. */
	gate = stage == CLUSTER_CONFIG_PRODUCERS_FRONT
			   ? cluster_shared_config_delivery_native_gate()
			   : cluster_shared_config_delivery_background_gate(CLUSTER_CONFIG_BACKGROUND_WALWRITER,
																&failed);
	return gate != NULL && !failed && !cluster_config_use_gate_read(gate).closed;
}

static bool
producer_current(void)
{
	ClusterConfigChannelOwner owner;
	ClusterConfigMembersKey key;
	if (!producer_cut.active || producer_cut.invalid)
		return false;
	if (!producer_self(&owner) || !producer_key(&producer_cut.key.ref, &key)
		|| memcmp(&owner, &producer_cut.owner, sizeof(owner)) != 0
		|| memcmp(&key, &producer_cut.key, sizeof(key)) != 0) {
		producer_cut.invalid = true;
		return false;
	}
	return true;
}

ClusterConfigProducerResult
cluster_config_producers_observe(ClusterConfigProducerCensus *out)
{
	ClusterConfigProducerCensus result = { 0 };
	if (out == NULL)
		return CLUSTER_CONFIG_PRODUCERS_INVALID;
	memset(out, 0, sizeof(*out));
	if (!producer_current())
		return CLUSTER_CONFIG_PRODUCERS_INVALID;
	result.key = producer_cut.key;
	memcpy(result.episode, producer_cut.episode, sizeof(result.episode));
	result.stage = producer_cut.stage;
	result.held = producer_cut.held;
	result.bound = producer_cut.bound;
	result.common = producer_cut.common;
	for (unsigned i = 0; i < CLUSTER_CONFIG_PRODUCERS_COUNT; ++i) {
		ClusterConfigUseGate *gate;
		ClusterConfigUseGateState state;
		bool failed;
		if (!(result.held & (1u << i)))
			continue;
		gate = producer_gate(i, &failed);
		if (gate == NULL || gate != producer_cut.gate[i] || failed) {
			result.failed |= 1u << i;
			continue;
		}
		state = cluster_config_use_gate_read(gate);
		if (!state.closed || state.epoch == 0 || state.epoch != producer_cut.cookie[i])
			result.failed |= 1u << i;
		else if (state.owners != 0)
			result.pending |= 1u << i;
	}
	if (result.failed != 0)
		producer_cut.invalid = true;
	*out = result;
	if (producer_cut.invalid || !producer_current())
		return CLUSTER_CONFIG_PRODUCERS_INVALID;
	return result.pending ? CLUSTER_CONFIG_PRODUCERS_PENDING : CLUSTER_CONFIG_PRODUCERS_READY;
}

/*
 * cluster_config_producers_hold -- Close one dependency-ordered local stage.
 *
 * The caller owns actual CF selection and all-member predecessor retirement.
 * This function proves only exact local owner/cookies and existing counts.
 * Partial failure keeps retained cuts and cannot be reset by a new episode.
 * Author: SqlRush <sqlrush@gmail.com>
 */
ClusterConfigProducerResult
cluster_config_producers_hold(const ClusterSharedConfigRef *selected, const uint8 episode[16],
							  ClusterConfigProducerStage stage)
{
	ClusterConfigMembersKey key;
	ClusterConfigChannelOwner owner;
	ClusterConfigProducerCensus before;
	ClusterConfigProducerResult result;
	uint32 desired;
	uint8 nonzero = 0;
	if (episode == NULL || selected == NULL || stage < CLUSTER_CONFIG_PRODUCERS_FRONT
		|| stage > CLUSTER_CONFIG_PRODUCERS_QUIET)
		return CLUSTER_CONFIG_PRODUCERS_INVALID;
	for (unsigned i = 0; i < 16; ++i)
		nonzero |= episode[i];
	if (nonzero == 0 || !producer_self(&owner) || !producer_key(selected, &key))
		return CLUSTER_CONFIG_PRODUCERS_INVALID;
	if (!producer_cut.active) {
		if (stage != CLUSTER_CONFIG_PRODUCERS_FRONT
			|| (producer_cut.released && memcmp(producer_cut.episode, episode, 16) == 0))
			return CLUSTER_CONFIG_PRODUCERS_INVALID;
		memset(&producer_cut, 0, sizeof(producer_cut));
		producer_cut.active = true;
		producer_cut.owner = owner;
		producer_cut.key = key;
		memcpy(producer_cut.episode, episode, 16);
	} else {
		if (memcmp(&key, &producer_cut.key, sizeof(key)) != 0
			|| memcmp(episode, producer_cut.episode, 16) != 0 || stage < producer_cut.stage
			|| stage > producer_cut.stage + 1)
			return CLUSTER_CONFIG_PRODUCERS_INVALID;
		result = cluster_config_producers_observe(&before);
		if (result != CLUSTER_CONFIG_PRODUCERS_READY || stage == producer_cut.stage)
			return result;
	}
	desired = stage == CLUSTER_CONFIG_PRODUCERS_FRONT	  ? 3u
			  : stage == CLUSTER_CONFIG_PRODUCERS_STORAGE ? 15u
														  : PRODUCER_ALL;
	producer_cut.stage = stage;
	for (unsigned i = 0; i < CLUSTER_CONFIG_PRODUCERS_COUNT; ++i) {
		ClusterConfigUseGate *gate;
		bool failed;
		if (!(desired & (1u << i)) || (producer_cut.held & (1u << i)))
			continue;
		gate = producer_gate(i, &failed);
		/* A foreign CLOSED cookie is not ours to adopt, even with zero users. */
		if (gate == NULL || failed || cluster_config_use_gate_read(gate).closed) {
			producer_cut.invalid = true;
			return CLUSTER_CONFIG_PRODUCERS_INVALID;
		}
		producer_cut.gate[i] = gate;
		producer_cut.held |= 1u << i;
		if (!cluster_config_use_gate_close(gate, &producer_cut.cookie[i])) {
			producer_cut.invalid = true;
			return CLUSTER_CONFIG_PRODUCERS_INVALID;
		}
	}
	return cluster_config_producers_observe(&before);
}

static bool
producer_census_complete(const ClusterSharedConfigCensus *census)
{
	return census->node_id == (uint32)cluster_node_id
		   && memcmp(&census->ref, &producer_cut.key.ref, sizeof(census->ref)) == 0
		   && census->participants != 0 && census->participants == census->current_processes
		   && census->waiting_processes == 0 && census->failed_processes == 0
		   && census->parallel_processes == 0 && census->active_missing_processes == 0
		   && census->static_mismatch_processes == 0 && census->dynamic_mismatch_processes == 0
		   && census->active.version == CLUSTER_SHARED_CONFIG_COMMON_VERSION;
}

static bool
producer_target_same(unsigned i, const ClusterSharedConfigActive *common)
{
	ClusterConfigUseTarget *target = producer_target(i);
	return target != NULL && target->epoch == producer_cut.cookie[i]
		   && target->node_id == (uint32)cluster_node_id
		   && memcmp(&target->ref, &producer_cut.key.ref, sizeof(target->ref)) == 0
		   && memcmp(&target->common, common, sizeof(*common)) == 0;
}

ClusterConfigProducerResult
cluster_config_producers_bind(void)
{
	ClusterConfigProducerCensus held;
	ClusterSharedConfigCensus actual;
	ClusterConfigProducerResult result = cluster_config_producers_observe(&held);
	if (result != CLUSTER_CONFIG_PRODUCERS_READY)
		return result;
	if (held.stage != CLUSTER_CONFIG_PRODUCERS_QUIET || held.held != PRODUCER_ALL)
		return CLUSTER_CONFIG_PRODUCERS_INVALID;
	if (!cluster_shared_config_node_common_census(&producer_cut.key.ref, cluster_node_id, &actual)
		|| !producer_census_complete(&actual))
		return CLUSTER_CONFIG_PRODUCERS_PENDING;
	if (cluster_config_producers_observe(&held) != CLUSTER_CONFIG_PRODUCERS_READY)
		return CLUSTER_CONFIG_PRODUCERS_INVALID;
	if (producer_cut.bound
		&& memcmp(&producer_cut.common, &actual.active, sizeof(actual.active)) != 0) {
		producer_cut.invalid = true;
		return CLUSTER_CONFIG_PRODUCERS_INVALID;
	}
	/* Validate ALL already-bound targets before changing any target. Missing
	 * native values or a foreign binding cannot produce a partial OPEN. */
	for (unsigned i = 0; i < CLUSTER_CONFIG_PRODUCERS_COUNT; ++i) {
		ClusterConfigUseTarget *target = producer_target(i);
		if (target == NULL || target->epoch > producer_cut.cookie[i]
			|| (target->epoch == producer_cut.cookie[i]
				&& !producer_target_same(i, &actual.active))) {
			producer_cut.invalid = true;
			return CLUSTER_CONFIG_PRODUCERS_INVALID;
		}
	}
	for (unsigned i = 0; i < CLUSTER_CONFIG_PRODUCERS_COUNT; ++i)
		if (!cluster_config_use_target_bind(producer_cut.gate[i], producer_target(i),
											producer_cut.cookie[i], cluster_node_id,
											&producer_cut.key.ref, &actual.active)) {
			producer_cut.invalid = true;
			return CLUSTER_CONFIG_PRODUCERS_INVALID;
		}
	producer_cut.common = actual.active;
	producer_cut.bound = true;
	return cluster_config_producers_observe(&held);
}

bool
cluster_config_producers_release(void)
{
	/* Completion providers first, independent native commands last. All-member
	 * APPLY is an external prerequisite, not inferred from this local order. */
	static const unsigned order[] = { 4, 2, 3, 5, 6, 7, 1, 0 };
	if (!producer_cut.bound || cluster_config_producers_bind() != CLUSTER_CONFIG_PRODUCERS_READY)
		return false;
	for (unsigned n = 0; n < lengthof(order); ++n) {
		unsigned i = order[n];
		if (!cluster_config_use_gate_open(producer_cut.gate[i], producer_cut.cookie[i])) {
			/* Exact CLOSED/zero and the sole owner should make this impossible.
			 * Do not roll back already released new-value work or report OPEN. */
			producer_cut.invalid = true;
			return false;
		}
	}
	producer_cut.active = false;
	producer_cut.released = true;
	return true;
}
