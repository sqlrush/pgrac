/*-------------------------------------------------------------------------
 *
 * cluster_cf_stats.h
 *	  CF (control file) shared-authority observability counters (spec-5.6 Dc4).
 *
 *	  A small, dependency-light owner for the five CF shared-authority
 *	  counters so that every CF module (authority read, storage bootstrap,
 *	  enqueue) can bump a counter by including only this header -- it pulls in
 *	  no lock/GES machinery, unlike cluster_cf_enqueue.h.  The counters live in
 *	  a dedicated lock-free shmem region (mirror cluster_advisory: atomics
 *	  only, no LWLock) so a monitoring backend can read what the checkpointer
 *	  or startup process produced.
 *
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/include/cluster/cluster_cf_stats.h
 *
 * NOTES
 *	  This is a pgrac-original file (no derivation from PostgreSQL).
 *	  Spec: spec-5.6-cf-enqueue-shared-controlfile-authority.md (Dc4)
 *
 *-------------------------------------------------------------------------
 */
#ifndef CLUSTER_CF_STATS_H
#define CLUSTER_CF_STATS_H

#include "c.h"

/*
 * spec-5.6 Dc4 -- CF shared-authority observability counters.  Each has at
 * least one call site (DoD); they are pure observability and never gate logic.
 */
typedef enum ClusterCfCounter
{
	CLUSTER_CF_X_ACQUIRE = 0,			/* CF X granted (write authority) */
	CLUSTER_CF_S_ACQUIRE = 1,			/* CF S granted (strong-consistency read) */
	CLUSTER_CF_FAILCLOSED = 2,			/* CF lock unprovable -> caller fails closed */
	CLUSTER_CF_SINGLE_NODE_AUTHORITY = 3,	/* bootstrap single-node authority window opened */
	CLUSTER_CF_BAK_FALLBACK = 4,		/* authority read fell back to a valid .bak */
	CLUSTER_CF_COUNTER_COUNT = 5
} ClusterCfCounter;

/* shmem region lifecycle (mirror cluster_advisory). */
extern Size cluster_cf_stats_shmem_size(void);
extern void cluster_cf_stats_shmem_init(void);
extern void cluster_cf_stats_shmem_register(void);

/* counter mutate + read (NULL/uninit-safe; out-of-range is a no-op / 0). */
extern void cluster_cf_counter_inc(ClusterCfCounter which);
extern uint64 cluster_cf_counter_read(ClusterCfCounter which);

/*
 * spec-5.6 increment (iii) follow-up -- cross-process JOIN_READONLY bring-up
 * flag.  Lives in this lock-free CF shmem region (not a process-local static)
 * so the checkpointer can see the role the startup process set: a multi-node
 * node whose Phase-2 proved a peer alive attaches read-only and, during the
 * pre-GES bring-up window, must skip CF X + the shared-authority write (the
 * checkpointer's end-of-recovery checkpoint would otherwise try CF X before
 * LMS/GES is ready and fail).  The flag is set by the startup-process role gate
 * and cleared once GES is available so EVERY steady-state checkpoint takes CF X
 * (it is strictly a bring-up window, never a steady-state CF-X bypass).
 * NULL/uninit-safe: reads false when the region is absent (single-node / off).
 */
extern void cluster_cf_stats_set_join_readonly(bool on);
extern bool cluster_cf_stats_get_join_readonly(void);

#endif							/* CLUSTER_CF_STATS_H */
