/*-------------------------------------------------------------------------
 *
 * cluster_multixact_current_stats.c
 *	  Shared observability for current-DML MultiXact authority.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/backend/cluster/cluster_multixact_current_stats.c
 *
 * NOTES
 *	  This is a pgrac-original file.
 *	  Spec: spec-3.6b-multixact-current-dml.md
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "miscadmin.h"
#include "port/atomics.h"
#include "storage/shmem.h"

#include "cluster/cluster_guc.h"
#include "cluster/cluster_multixact_current_stats.h"
#include "cluster/cluster_shmem.h"
#include "utils/timestamp.h"

#ifdef USE_PGRAC_CLUSTER

typedef struct ClusterCurrentMxStatsShmem {
	TimestampTz stats_since;
	pg_atomic_uint64 counters[CMX_STAT_COUNT];
} ClusterCurrentMxStatsShmem;

static ClusterCurrentMxStatsShmem *ClusterCurrentMxStats;

Size
cluster_multixact_current_stats_shmem_size(void)
{
	if (IsBootstrapProcessingMode() || !cluster_enabled || cluster_node_id < 0)
		return 0;
	return MAXALIGN(sizeof(ClusterCurrentMxStatsShmem));
}

void
cluster_multixact_current_stats_shmem_init(void)
{
	bool found;
	int i;

	if (IsBootstrapProcessingMode() || !cluster_enabled || cluster_node_id < 0)
		return;
	ClusterCurrentMxStats = ShmemInitStruct("pgrac current multixact stats",
											sizeof(ClusterCurrentMxStatsShmem), &found);
	if (!found) {
		ClusterCurrentMxStats->stats_since = GetCurrentTimestamp();
		for (i = 0; i < CMX_STAT_COUNT; i++)
			pg_atomic_init_u64(&ClusterCurrentMxStats->counters[i], 0);
	}
}

static const ClusterShmemRegion cluster_multixact_current_stats_region = {
	.name = "pgrac current multixact stats",
	.size_fn = cluster_multixact_current_stats_shmem_size,
	.init_fn = cluster_multixact_current_stats_shmem_init,
	.lwlock_count = 0,
	.owner_subsys = "cluster_multixact_current",
	.reserved_flags = 0,
};

void
cluster_multixact_current_stats_shmem_register(void)
{
	cluster_shmem_register_region(&cluster_multixact_current_stats_region);
}

void
cluster_multixact_current_stats_bump(ClusterCurrentMxStatId stat)
{
	if (ClusterCurrentMxStats != NULL && stat >= 0 && stat < CMX_STAT_COUNT)
		pg_atomic_fetch_add_u64(&ClusterCurrentMxStats->counters[stat], 1);
}

uint64
cluster_multixact_current_stats_get(ClusterCurrentMxStatId stat)
{
	if (ClusterCurrentMxStats == NULL || stat < 0 || stat >= CMX_STAT_COUNT)
		return 0;
	return pg_atomic_read_u64(&ClusterCurrentMxStats->counters[stat]);
}

TimestampTz
cluster_multixact_current_stats_since(void)
{
	if (ClusterCurrentMxStats == NULL)
		return 0;
	return ClusterCurrentMxStats->stats_since;
}

bool
cluster_multixact_current_stats_snapshot(uint32 node_id, uint64 cluster_epoch,
										 ClusterCurrentMxStatsSnapshot *snapshot)
{
	int i;

	if (ClusterCurrentMxStats == NULL || snapshot == NULL)
		return false;
	memset(snapshot, 0, sizeof(*snapshot));
	snapshot->node_id = node_id;
	snapshot->cluster_epoch = cluster_epoch;
	snapshot->stats_since = ClusterCurrentMxStats->stats_since;
	for (i = 0; i < CMX_STAT_COUNT; i++)
		snapshot->counters[i] = pg_atomic_read_u64(&ClusterCurrentMxStats->counters[i]);
	return snapshot->stats_since != 0;
}

ClusterCurrentMxStatId
cluster_multixact_current_restart_bucket(uint32 restarts)
{
	if (restarts == 0)
		return CMX_STAT_RESTART_BUCKET_0;
	if (restarts == 1)
		return CMX_STAT_RESTART_BUCKET_1;
	if (restarts <= 3)
		return CMX_STAT_RESTART_BUCKET_2_3;
	if (restarts <= 7)
		return CMX_STAT_RESTART_BUCKET_4_7;
	return CMX_STAT_RESTART_BUCKET_8_PLUS;
}

void
cluster_multixact_current_stats_record_restarts(uint32 restarts)
{
	uint64 expected;

	if (ClusterCurrentMxStats == NULL)
		return;

	cluster_multixact_current_stats_bump(cluster_multixact_current_restart_bucket(restarts));

	expected = pg_atomic_read_u64(&ClusterCurrentMxStats->counters[CMX_STAT_RESTART_MAX]);
	while (expected < restarts
		   && !pg_atomic_compare_exchange_u64(
			   &ClusterCurrentMxStats->counters[CMX_STAT_RESTART_MAX], &expected, restarts))
		;
}

void
cluster_multixact_current_stats_alert_sample(void)
{
	static const ClusterCurrentMxStatId alert_stats[]
		= { CMX_STAT_DESCRIBE_REMOTE_TIMEOUT,	 CMX_STAT_DESCRIBE_REMOTE_UNKNOWN,
			CMX_STAT_MEMBER_PROOF_UNKNOWN,		 CMX_STAT_DESCRIBE_INVALID_REPLY,
			CMX_STAT_MEMBER_PROOF_INVALID_REPLY, CMX_STAT_RESTART_BUCKET_8_PLUS,
			CMX_STAT_FOREIGN_SLRU_GUARD };
	static uint64 previous[lengthof(alert_stats)];
	static TimestampTz last_sample;
	TimestampTz now;
	uint64 deltas[lengthof(alert_stats)];
	bool changed = false;
	int i;

	if (ClusterCurrentMxStats == NULL)
		return;

	now = GetCurrentTimestamp();
	if (last_sample != 0 && now >= last_sample && now - last_sample < INT64CONST(60000000))
		return;

	for (i = 0; i < lengthof(alert_stats); i++) {
		uint64 current = cluster_multixact_current_stats_get(alert_stats[i]);

		deltas[i] = current >= previous[i] ? current - previous[i] : current;
		previous[i] = current;
		changed |= deltas[i] != 0;
	}
	last_sample = now;

	if (changed)
		elog(WARNING,
			 "current MultiXact authority alert: describe_timeout=%llu "
			 "describe_unknown=%llu member_proof_unknown=%llu "
			 "describe_invalid_reply=%llu member_proof_invalid_reply=%llu "
			 "restart_8_plus=%llu foreign_slru_guard=%llu",
			 (unsigned long long)deltas[0], (unsigned long long)deltas[1],
			 (unsigned long long)deltas[2], (unsigned long long)deltas[3],
			 (unsigned long long)deltas[4], (unsigned long long)deltas[5],
			 (unsigned long long)deltas[6]);
}

#endif /* USE_PGRAC_CLUSTER */
