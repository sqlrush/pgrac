/*-------------------------------------------------------------------------
 *
 * cluster_lmd_probe_collector.h
 *	  pgrac LMD cross-node DEADLOCK_REPORT collector — shared-memory
 *	  LMON->LMD hand-off (spec-5.8 D8 / Option B).
 *
 *	  The coordinator's cross-node deadlock scan runs in the LMD process,
 *	  but inbound DEADLOCK_REPORT envelopes are received and dispatched in
 *	  the LMON process (LMON owns the interconnect recv loop).  A process-
 *	  local collector therefore cannot work: LMON would write its own copy
 *	  and LMD would read an empty one.  This module is the shared-memory
 *	  rendezvous, mirroring the established cluster_lmd cancel-queue pattern
 *	  (LMON enqueues on receive, LMD drains in its own loop).
 *
 *	  Roles:
 *	    LMD  arms one in-flight probe (probe_id + expected-responder bitmap),
 *	         polls the received count, then drains the accumulated remote
 *	         edges into its Tarjan union and resets.
 *	    LMON validates + appends each received REPORT's edges under the
 *	         region LWLock.
 *
 *	  Rule 8.A safety contract (a duplicate / stale / overflowing REPORT must
 *	  never let the coordinator cancel a victim it should not):
 *	    - a REPORT is accepted only for the CURRENT probe_id (else dropped
 *	      stale) and only from a node in the expected set that has not yet
 *	      reported this round (cluster_lmd_probe_member_admit — dedup);
 *	    - if a REPORT's edges do not all fit, the ring is marked OVERFLOW and
 *	      no partial edges are appended — the LMD drain treats an overflowed
 *	      round as INCOMPLETE and does not confirm / cancel;
 *	    - a missing / dropped / overflowed REPORT only makes the union
 *	      smaller (fewer edges => fewer cycles), so it can only MISS a
 *	      deadlock, never invent one.
 *
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/include/cluster/cluster_lmd_probe_collector.h
 *
 * NOTES
 *	  pgrac-original file.  Compiled only in --enable-cluster builds.
 *	  Spec: spec-5.8-full-cross-node-deadlock-detector.md.
 *
 *-------------------------------------------------------------------------
 */
#ifndef CLUSTER_LMD_PROBE_COLLECTOR_H
#define CLUSTER_LMD_PROBE_COLLECTOR_H

#include "c.h"

struct GesDeadlockReportHeader;
struct ClusterLmdWaitEdge;

/*
 * Result of a drain: the remote edges collected this round plus the metadata
 * the coordinator needs to judge completeness (Rule 8.A).
 */
typedef struct ClusterLmdProbeDrain {
	int n_edges;	  /* remote edges copied into the caller's buffer */
	int n_received;	  /* distinct responders accepted this round */
	uint64 member_lo; /* responders that reported (node 0..63) */
	uint64 member_hi; /* responders that reported (node 64..127) */
	bool overflow;	  /* a REPORT did not fit — round is INCOMPLETE */
} ClusterLmdProbeDrain;

/* shmem region helpers — registered via the spec-1.3 region registry. */
extern Size cluster_lmd_probe_collector_shmem_size(void);
extern void cluster_lmd_probe_collector_shmem_init(void);
extern void cluster_lmd_probe_collector_shmem_register(void);

/*
 * LMD coordinator side.
 *
 *	arm:    open a fresh in-flight probe (probe_id, expected-responder
 *	        bitmap); clears any prior round.
 *	n_received: poll how many distinct expected responders have reported.
 *	drain:  copy up to max_edges remote edges into out_buf and fill *out;
 *	        returns the number of edges copied.  An overflowed round reports
 *	        out->overflow = true so the caller discards it.
 *	reset:  return to idle (probe_id = 0); frees nothing (fixed ring).
 */
extern void cluster_lmd_probe_arm(uint64 probe_id, uint64 expected_lo, uint64 expected_hi);
extern int cluster_lmd_probe_n_received(void);
extern int cluster_lmd_probe_drain(struct ClusterLmdWaitEdge *out_buf, int max_edges,
								   ClusterLmdProbeDrain *out);
extern void cluster_lmd_probe_reset(void);

/*
 * LMON receive side — validate + append one REPORT's edges.  Returns true if
 * accepted.  Drops (returning false) a stale probe_id, an unexpected node, a
 * duplicate responder, or an overflowing REPORT (bumping the matching
 * counter).  Safe (no-op false) before shmem init.
 */
extern bool cluster_lmd_probe_collect_receive(const struct GesDeadlockReportHeader *report,
											  Size report_len);

/* Counters (spec-5.8 D8 — constraint 6). */
extern uint64 cluster_lmd_probe_report_enqueue_count_get(void);
extern uint64 cluster_lmd_probe_drop_stale_count_get(void);
extern uint64 cluster_lmd_probe_drop_duplicate_count_get(void);
extern uint64 cluster_lmd_probe_queue_full_count_get(void);
extern uint64 cluster_lmd_probe_partial_report_count_get(void);

#endif /* CLUSTER_LMD_PROBE_COLLECTOR_H */
