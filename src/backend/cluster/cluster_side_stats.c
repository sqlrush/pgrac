/*-------------------------------------------------------------------------
 *
 * cluster_side_stats.c
 *	  RF-SIDE D-SIDE-10 — observability counters (implementation).
 *
 *	  Spec: specs/spec-rf-side-typed-recovery-and-shared-terminal.md
 *	  §1.2 D-SIDE-10 / §5.1 U-SIDE-16.
 *
 *	  Observational only: no verdict function reads any counter, so a
 *	  counter can never change a decision (U-SIDE-16).
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * IDENTIFICATION
 *	  src/backend/cluster/cluster_side_stats.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "cluster/cluster_side_route.h"
#include "cluster/cluster_side_stats.h"

void
cluster_side_stats_init(ClusterSideStats *stats)
{
	int i;
	pg_atomic_uint64 *fields;
	int nfields;

	if (stats == NULL)
		return;
	fields = (pg_atomic_uint64 *)stats;
	nfields = sizeof(ClusterSideStats) / sizeof(pg_atomic_uint64);
	for (i = 0; i < nfields; i++)
		pg_atomic_init_u64(&fields[i], 0);
}

void
cluster_side_stats_route(ClusterSideStats *stats, int verdict)
{
	if (stats == NULL)
		return;
	switch ((ClusterSideRouteVerdict)verdict) {
	case CLUSTER_SIDE_ROUTE_VERDICT_APPLY:
		pg_atomic_fetch_add_u64(&stats->route_applies, 1);
		break;
	case CLUSTER_SIDE_ROUTE_VERDICT_PROVED_NOOP:
		pg_atomic_fetch_add_u64(&stats->route_noops, 1);
		break;
	case CLUSTER_SIDE_ROUTE_VERDICT_BLOCKED:
	default:
		pg_atomic_fetch_add_u64(&stats->route_blocked, 1);
		break;
	}
}

void
cluster_side_stats_domain(ClusterSideStats *stats, int route_kind)
{
	if (stats == NULL)
		return;
	switch ((ClusterSideRouteKind)route_kind) {
	case CLUSTER_SIDE_ROUTE_TT_UNDO:
		pg_atomic_fetch_add_u64(&stats->domain_tt_undo, 1);
		break;
	case CLUSTER_SIDE_ROUTE_PROJECTION:
		pg_atomic_fetch_add_u64(&stats->domain_projection, 1);
		break;
	case CLUSTER_SIDE_ROUTE_STORAGE:
		pg_atomic_fetch_add_u64(&stats->domain_storage, 1);
		break;
	default:
		break; /* PAGE/NOOP/BLOCKED are not domain events */
	}
}

void
cluster_side_stats_blocked(ClusterSideStats *stats, bool unknown_class)
{
	if (stats == NULL)
		return;
	if (unknown_class)
		pg_atomic_fetch_add_u64(&stats->blocked_unknown_class, 1);
	else
		pg_atomic_fetch_add_u64(&stats->blocked_authority, 1);
}

void
cluster_side_stats_rebuild(ClusterSideStats *stats)
{
	if (stats != NULL)
		pg_atomic_fetch_add_u64(&stats->rebuild_events, 1);
}

void
cluster_side_stats_durability(ClusterSideStats *stats)
{
	if (stats != NULL)
		pg_atomic_fetch_add_u64(&stats->durability_events, 1);
}

/* G2 vocabulary: all EVENT; unknown names fail closed. */
typedef struct ClusterSideStatName {
	const char *name;
	int kind; /* 0 = EVENT (the only kind here) */
} ClusterSideStatName;

static const ClusterSideStatName cluster_side_stat_names[] = {
	{ "cluster.side.route_applies", 0 },		 { "cluster.side.route_noops", 0 },
	{ "cluster.side.route_blocked", 0 },		 { "cluster.side.domain_tt_undo", 0 },
	{ "cluster.side.domain_projection", 0 },	 { "cluster.side.domain_storage", 0 },
	{ "cluster.side.blocked_unknown_class", 0 }, { "cluster.side.blocked_authority", 0 },
	{ "cluster.side.rebuild_events", 0 },		 { "cluster.side.durability_events", 0 },
};

bool
cluster_side_stats_describe(const char *name, int *kind)
{
	int i;

	if (name == NULL)
		return false;
	for (i = 0; i < (int)lengthof(cluster_side_stat_names); i++) {
		if (strcmp(cluster_side_stat_names[i].name, name) == 0) {
			if (kind != NULL)
				*kind = cluster_side_stat_names[i].kind;
			return true;
		}
	}
	return false;
}
