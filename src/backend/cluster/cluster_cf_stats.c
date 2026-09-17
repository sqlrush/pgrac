/*-------------------------------------------------------------------------
 *
 * cluster_cf_stats.c
 *	  CF (control file) shared-authority lock-free shared state (spec-5.6).
 *
 *	  Five lock-free shmem counters bumped from the CF modules (X/S acquire and
 *	  fail-closed in the enqueue path, single-node authority in the storage
 *	  bootstrap, and .bak fallback in the authority read).  Mirrors the
 *	  cluster_advisory counter region: atomics only, no LWLock, so any backend
 *	  can read what the checkpointer or startup process produced.
 *
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/backend/cluster/cluster_cf_stats.c
 *
 * NOTES
 *	  This is a pgrac-original file (no derivation from PostgreSQL).
 *	  Spec: spec-5.6-cf-enqueue-shared-controlfile-authority.md
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "cluster/cluster_cf_stats.h"
#include "cluster/cluster_clean_leave.h"
#include "cluster/cluster_shmem.h"
#include "miscadmin.h"
#include "port/atomics.h"
#include "storage/shmem.h"

/*
 * spec-5.6 -- CF observability counters live in a dedicated lock-free
 * shmem region (mirror cluster_advisory: atomics only, no LWLock).
 */
typedef struct ClusterCfStatsSharedState {
	pg_atomic_uint64 counters[CLUSTER_CF_COUNTER_COUNT];
	/*
	 * Cross-process JOIN_READONLY bring-up flag (0/1).  Node-level: the startup
	 * process sets it at the role gate; the checkpointer reads it to skip CF X
	 * during the pre-GES bring-up window and clears it once GES is available.
	 */
	pg_atomic_uint32 join_readonly;
	/* RF-B boot-local OWNER -> EOR handoff; not an authority source. */
	pg_atomic_uint32 owner_eor_phase;
} ClusterCfStatsSharedState;

static ClusterCfStatsSharedState *cluster_cf_stats_state = NULL;

ClusterNormalStopPollResult
cluster_cf_normal_stop_shared_poll(bool post_checkpoint, const char **reason)
{
	ClusterNormalStopPollResult result = CLUSTER_NORMAL_STOP_INVALID;
	const char *why = "CF_SHARED_OWNER_OR_SHMEM";
	uint32 phase, join;
	if (!IsUnderPostmaster || (!AmCheckpointerProcess() && MyBackendType != B_LMON)
		|| cluster_cf_stats_state == NULL)
		goto done;
	phase = pg_atomic_read_u32(&cluster_cf_stats_state->owner_eor_phase);
	join = pg_atomic_read_u32(&cluster_cf_stats_state->join_readonly);
	/* Counters are history, not responsibilities. The ordinary non-EOR
	 * checkpoint clears JOIN_READONLY only after GES is available; do not
	 * pre-empt that producer or block the very checkpoint that clears it. */
	if (phase > CLUSTER_CF_OWNER_EOR_DONE || join > 1)
		why = "CF_SHARED_STATE_INVALID";
	else if (post_checkpoint && join != 0)
		why = "CF_POST_CHECKPOINT_JOIN_READONLY";
	else if (phase != CLUSTER_CF_OWNER_EOR_EMPTY) {
		why = "CF_OWNER_EOR_PENDING";
		result = CLUSTER_NORMAL_STOP_PENDING;
	} else {
		why = "NONE";
		result = CLUSTER_NORMAL_STOP_READY;
	}
done:
	if (reason)
		*reason = why;
	return result;
}


/* ============================================================
 * shmem region lifecycle (mirror cluster_advisory pattern).
 * ============================================================ */

Size
cluster_cf_stats_shmem_size(void)
{
	return sizeof(ClusterCfStatsSharedState);
}

void
cluster_cf_stats_shmem_init(void)
{
	bool found;

	cluster_cf_stats_state
		= ShmemInitStruct("pgrac cluster cf stats", cluster_cf_stats_shmem_size(), &found);
	if (!found) {
		int i;

		for (i = 0; i < CLUSTER_CF_COUNTER_COUNT; i++)
			pg_atomic_init_u64(&cluster_cf_stats_state->counters[i], 0);
		pg_atomic_init_u32(&cluster_cf_stats_state->join_readonly, 0);
		pg_atomic_init_u32(&cluster_cf_stats_state->owner_eor_phase, CLUSTER_CF_OWNER_EOR_EMPTY);
	}
}

static const ClusterShmemRegion cluster_cf_stats_region = {
	.name = "pgrac cluster cf stats",
	.size_fn = cluster_cf_stats_shmem_size,
	.init_fn = cluster_cf_stats_shmem_init,
	.lwlock_count = 0, /* atomics only */
	.owner_subsys = "cluster_cf_stats",
	.reserved_flags = 0,
};

void
cluster_cf_stats_shmem_register(void)
{
	cluster_shmem_register_region(&cluster_cf_stats_region);
}


/* ============================================================
 * counter mutate + read (NULL/uninit-safe).
 * ============================================================ */

void
cluster_cf_counter_inc(ClusterCfCounter which)
{
	if (cluster_cf_stats_state == NULL || (int)which < 0 || (int)which >= CLUSTER_CF_COUNTER_COUNT)
		return;
	pg_atomic_fetch_add_u64(&cluster_cf_stats_state->counters[which], 1);
}

uint64
cluster_cf_counter_read(ClusterCfCounter which)
{
	if (cluster_cf_stats_state == NULL || (int)which < 0 || (int)which >= CLUSTER_CF_COUNTER_COUNT)
		return 0;
	return pg_atomic_read_u64(&cluster_cf_stats_state->counters[which]);
}


/* ============================================================
 * cross-process JOIN_READONLY bring-up flag (NULL/uninit-safe).
 * ============================================================ */

void
cluster_cf_stats_set_join_readonly(bool on)
{
	if (cluster_cf_stats_state == NULL)
		return;
	pg_atomic_write_u32(&cluster_cf_stats_state->join_readonly, on ? 1 : 0);
}

bool
cluster_cf_stats_get_join_readonly(void)
{
	if (cluster_cf_stats_state == NULL)
		return false;
	return pg_atomic_read_u32(&cluster_cf_stats_state->join_readonly) != 0;
}


/* ============================================================
 * RF-B boot-local OWNER -> EOR handoff (NULL/uninit-safe).
 * ============================================================ */

ClusterCfOwnerEorPhase
cluster_cf_owner_eor_phase_read(void)
{
	if (cluster_cf_stats_state == NULL)
		return CLUSTER_CF_OWNER_EOR_EMPTY;
	return (ClusterCfOwnerEorPhase)pg_atomic_read_u32(&cluster_cf_stats_state->owner_eor_phase);
}

static bool
owner_eor_phase_cas(ClusterCfOwnerEorPhase old_phase, ClusterCfOwnerEorPhase new_phase)
{
	uint32 expected = (uint32)old_phase;

	if (cluster_cf_stats_state == NULL)
		return false;
	return pg_atomic_compare_exchange_u32(&cluster_cf_stats_state->owner_eor_phase, &expected,
										  (uint32)new_phase);
}

bool
cluster_cf_owner_eor_phase_install(void)
{
	return AmStartupProcess()
		   && owner_eor_phase_cas(CLUSTER_CF_OWNER_EOR_EMPTY, CLUSTER_CF_OWNER_EOR_INSTALLED);
}

bool
cluster_cf_owner_eor_phase_activate(void)
{
	return AmCheckpointerProcess()
		   && owner_eor_phase_cas(CLUSTER_CF_OWNER_EOR_INSTALLED, CLUSTER_CF_OWNER_EOR_ACTIVE);
}

bool
cluster_cf_owner_eor_phase_done(void)
{
	return AmCheckpointerProcess()
		   && owner_eor_phase_cas(CLUSTER_CF_OWNER_EOR_ACTIVE, CLUSTER_CF_OWNER_EOR_DONE);
}

bool
cluster_cf_owner_eor_phase_clear(void)
{
	ClusterCfOwnerEorPhase phase;

	if (!AmStartupProcess())
		return false;

	phase = cluster_cf_owner_eor_phase_read();
	if (phase != CLUSTER_CF_OWNER_EOR_INSTALLED && phase != CLUSTER_CF_OWNER_EOR_DONE)
		return false;
	return owner_eor_phase_cas(phase, CLUSTER_CF_OWNER_EOR_EMPTY);
}
