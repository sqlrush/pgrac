/*-------------------------------------------------------------------------
 *
 * cluster_advisory.h
 *	  UL user lock: cross-node advisory (user) lock scope + observability.
 *
 *	  Advisory locks reuse the existing LOCKTAG_ADVISORY + GES enqueue
 *	  substrate (spec-5.3); this module holds the small UL-specific pieces
 *	  that do NOT belong in the generic lock-acquire path:
 *	    - session-vs-xact scope derivation from a LOCALLOCK (spec-5.5 D3),
 *	      used by the backend-exit LockReleaseAll(allLocks=true) drain to
 *	      label the cluster release correctly (the xact-end path already
 *	      filters session owners before reaching the cluster hook);
 *	    - UL observability counters (spec-5.5 D8).
 *
 *	  Session lifecycle itself is NOT tracked here:  it reuses PG's native
 *	  session lock-owner (allLocks=false skips the session locallock so the
 *	  GES holder survives across transactions, F0-12 / Q3).  No parallel
 *	  holder table is built.
 *
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/include/cluster/cluster_advisory.h
 *
 * NOTES
 *	  Spec: spec-5.5-ul-advisory-lock-cross-node.md
 *
 *-------------------------------------------------------------------------
 */
#ifndef CLUSTER_ADVISORY_H
#define CLUSTER_ADVISORY_H

#include "c.h"

struct LOCALLOCK;

/*
 * spec-5.5 D8 — UL observability counters.  Backed by the cluster_advisory
 * shmem region so any backend on the node can read them via pg_cluster_state
 * (cross-cluster aggregation is a forward enhancement, Q8).
 */
typedef enum ClusterAdvisoryCounter {
	CLUSTER_ADVISORY_GLOBALIZE = 0,		  /* 0->1 edge entered the cluster path */
	CLUSTER_ADVISORY_SESSION_RELEASE = 1, /* session-scoped holder drained */
	CLUSTER_ADVISORY_TRY_GRANT = 2,		  /* NOWAIT request granted by master (S4;
										   * the local PG-native take still follows) */
	CLUSTER_ADVISORY_TRY_NOTAVAIL = 3,	  /* NOWAIT returned false (conflict) */
	CLUSTER_ADVISORY_FAILCLOSED = 4,	  /* mutual exclusion unprovable -> ERROR */
	CLUSTER_ADVISORY_COUNTER_COUNT = 5
} ClusterAdvisoryCounter;

/*
 * spec-5.5 D3 / §3.2 L5 — true iff the LOCALLOCK carries a session lock-owner
 * (PG convention: lockOwners[i].owner == NULL).  A mixed session+xact same-key
 * locallock is session-scoped (the session owner outlives the transaction).
 */
extern bool cluster_advisory_locallock_is_session_scoped(const struct LOCALLOCK *locallock);

/* spec-5.5 D8 — shmem region lifecycle (mirror cluster_ges pattern). */
extern Size cluster_advisory_shmem_size(void);
extern void cluster_advisory_shmem_init(void);
extern void cluster_advisory_shmem_register(void);

/* spec-5.5 D8 — counter mutate + read (NULL/uninit-safe). */
extern void cluster_advisory_counter_inc(ClusterAdvisoryCounter which);
extern uint64 cluster_advisory_counter_read(ClusterAdvisoryCounter which);

#endif /* CLUSTER_ADVISORY_H */
