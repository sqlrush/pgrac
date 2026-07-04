/*-------------------------------------------------------------------------
 *
 * cluster_ges_handoff.h
 *	  pgrac spec-6.12e1 -- GES release-side deterministic handoff.
 *
 *	  The deterministic drain itself already exists: on a RELEASE the
 *	  master runs cluster_grd_release_and_drain (spec-5.3 D3) under one
 *	  entry spinlock -- remove the releasing holder, drop stale-epoch
 *	  queue entries, grant every now-compatible convert (FIFO, priority
 *	  over waiters), then pop one FIFO waiter -- and returns the full set
 *	  of granted identities for the caller to route.  There is no
 *	  per-waiter re-evaluation window; the handoff is a single pass.
 *
 *	  6.12e1 does NOT rewrite that logic.  It (1) hardens it with an
 *	  8.A-dual verifier that certifies the three safety/liveness
 *	  invariants of every drain, exercisable as a pure function under
 *	  model-check, and (2) exposes counters so the handoff is observable.
 *	  cluster.ges_handoff arms the verify + counters; off keeps the drain
 *	  path byte-identical.
 *
 *	  Three invariants (spec-6.12 §3.4a):
 *	    no-double-grant    at most one exclusive grant per resid at any
 *	                       instant: no two granted identities in one drain
 *	                       hold mutually-incompatible modes, and no grant
 *	                       is incompatible with a holder that survived.
 *	    no-stale-holder    the releasing holder is gone before any grant
 *	                       is emitted (checked by the caller's remove-first
 *	                       ordering; the verifier confirms the released
 *	                       identity is absent from the post-drain holders).
 *	    no-lost-waiter     a waiter that became compatible with the
 *	                       post-drain holder set and is not barriered was
 *	                       either granted this pass or a strictly-earlier
 *	                       fair_queue_seq waiter/convert was granted ahead
 *	                       of it (progress, never silent drop).
 *
 *	  Pure layer (cluster_ges_handoff_policy.c): no shmem, no locks, no
 *	  ereport.  The runtime shim (cluster_ges_handoff.c) counts + logs.
 *
 * Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * Spec: spec-6.12-crossnode-cache-fusion-perf-optimization.md
 *
 * IDENTIFICATION
 *	  src/include/cluster/cluster_ges_handoff.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef CLUSTER_GES_HANDOFF_H
#define CLUSTER_GES_HANDOFF_H

#include "c.h"
#include "storage/lock.h" /* LOCKMODE */

/*
 * A flattened, runtime-free snapshot of one drain: the holders that
 * survived, the waiters/converts still queued, and the identities the
 * drain granted this pass.  The policy verifier consumes only these
 * plain arrays so it is directly testable.
 */
#define CLUSTER_GES_HANDOFF_MAX 32

typedef struct ClusterGesHandoffParty {
	int32 node_id;
	uint32 procno;
	LOCKMODE mode;
	uint64 fair_queue_seq; /* waiters/converts only; 0 for holders */
	bool barriered;		   /* waiters only: held behind an earlier boosted */
} ClusterGesHandoffParty;

typedef struct ClusterGesHandoffSnapshot {
	/* post-drain surviving holders */
	ClusterGesHandoffParty holders[CLUSTER_GES_HANDOFF_MAX];
	int nholders;
	/* still-queued waiters/converts after the drain */
	ClusterGesHandoffParty waiters[CLUSTER_GES_HANDOFF_MAX];
	int nwaiters;
	/* identities granted this drain pass */
	ClusterGesHandoffParty granted[CLUSTER_GES_HANDOFF_MAX];
	int ngranted;
	/* the released holder identity (must be absent post-drain) */
	int32 released_node_id;
	uint32 released_procno;
} ClusterGesHandoffSnapshot;

typedef enum ClusterGesHandoffVerdict {
	CLUSTER_GES_HANDOFF_OK = 0,
	CLUSTER_GES_HANDOFF_DOUBLE_GRANT, /* two grants / grant vs holder conflict */
	CLUSTER_GES_HANDOFF_STALE_HOLDER, /* released identity still a holder */
	CLUSTER_GES_HANDOFF_LOST_WAITER	  /* a servable waiter was left behind */
} ClusterGesHandoffVerdict;

/* Pure verifier (cluster_ges_handoff_policy.c). */
extern ClusterGesHandoffVerdict cluster_ges_handoff_verify(const ClusterGesHandoffSnapshot *snap);

/* Pure helper: are two GES modes compatible? (mirrors ges_modes_compatible
 * without the runtime include, so the policy unit links standalone). */
extern bool cluster_ges_handoff_modes_compatible(LOCKMODE a, LOCKMODE b);

/* Runtime observability (cluster_ges_handoff.c). */
extern bool cluster_ges_handoff_enabled(void);
extern void cluster_ges_handoff_note_drain(int n_granted, ClusterGesHandoffVerdict verdict);

extern uint64 cluster_ges_handoff_grant_count(void);
extern uint64 cluster_ges_handoff_drain_count(void);
extern uint64 cluster_ges_handoff_invariant_violation_count(void);

#endif /* CLUSTER_GES_HANDOFF_H */
