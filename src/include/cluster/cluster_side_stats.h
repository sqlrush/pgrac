/*-------------------------------------------------------------------------
 *
 * cluster_side_stats.h
 *	  RF-SIDE D-SIDE-10 — observability surfaces (route/domain/blocked/
 *	  rebuild/durability events) with the hard rule that counters are
 *	  observational only and can NEVER become authority.
 *
 *	  Spec: specs/spec-rf-side-typed-recovery-and-shared-terminal.md
 *	  §1.2 D-SIDE-10 ("route/domain/blocked/rebuild/durability events；
 *	  counter 只观测，不能成为 authority"), §5.1 U-SIDE-16 ("metrics
 *	  semantic type 与 live consumer；counter 置大不能改变 verdict").
 *
 *	  DELIVERED HERE:
 *	    - ClusterSideStats: the event counter set (all EVENT semantics,
 *	      one producer per counter — the D-SIDE judgement call sites,
 *	      fired by the production wiring);
 *	    - cluster_side_stats_describe: the single G2 vocabulary (name ->
 *	      kind); unknown names fail closed;
 *	    - U-SIDE-16 guarantee: NO verdict judgement reads a counter —
 *	      the verdict functions are pure over their typed facts, so a
 *	      counter can never change a verdict (the unit suite proves it
 *	      by inflating counters and re-running the judgements).
 *
 *	  NOT DELIVERED HERE: the SQL consumer surface (pg_cluster_state /
 *	  SRF mirror) is the production observability wiring (later); the
 *	  registry names below are the exact names the consumer will mirror.
 *	  No ABI change.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * IDENTIFICATION
 *	  src/include/cluster/cluster_side_stats.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef CLUSTER_SIDE_STATS_H
#define CLUSTER_SIDE_STATS_H

#include "port/atomics.h"

typedef struct ClusterSideStats {
	/* route events (D-SIDE-01 verdicts observed) */
	pg_atomic_uint64 route_applies;
	pg_atomic_uint64 route_noops;
	pg_atomic_uint64 route_blocked;
	/* domain events (D-SIDE-02..05 primitives observed) */
	pg_atomic_uint64 domain_tt_undo;
	pg_atomic_uint64 domain_projection;
	pg_atomic_uint64 domain_storage;
	/* blocked reasons */
	pg_atomic_uint64 blocked_unknown_class;
	pg_atomic_uint64 blocked_authority;
	/* rebuild + durability */
	pg_atomic_uint64 rebuild_events;
	pg_atomic_uint64 durability_events;
} ClusterSideStats;

extern void cluster_side_stats_init(ClusterSideStats *stats);

/* EVENT producers (one site each). */
extern void cluster_side_stats_route(ClusterSideStats *stats, int verdict);
extern void cluster_side_stats_domain(ClusterSideStats *stats, int route_kind);
extern void cluster_side_stats_blocked(ClusterSideStats *stats, bool unknown_class);
extern void cluster_side_stats_rebuild(ClusterSideStats *stats);
extern void cluster_side_stats_durability(ClusterSideStats *stats);

/*
 * G2 vocabulary: every name maps to exactly one kind (all EVENT here);
 * unknown names fail closed.  Names are the exact keys the production
 * SQL consumer mirrors.
 */
extern bool cluster_side_stats_describe(const char *name, int *kind);

#endif /* CLUSTER_SIDE_STATS_H */
