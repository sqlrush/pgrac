/*-------------------------------------------------------------------------
 *
 * cluster_extend_gate.h
 *	  Shared runtime-liveness engage gate for the relation-extend enqueue
 *	  classes (HW / DL / KO).
 *
 *	  spec-5.7 §3.1d (v1.5 amend): the enqueue classes must decide *whether* to
 *	  engage cross-node coordination from the cluster's *runtime liveness*, not
 *	  from the static configured node count.  cluster_conf_node_count() > 1 is
 *	  true for a configured-multi-node cluster even when no peer is actually
 *	  alive (single-alive / degraded survivor) or before the GES/LMS substrate
 *	  is ready -- engaging there fail-closes ordinary DML.  This gate is the
 *	  single source of truth for that decision, shared by all three classes so
 *	  they cannot drift (Q19).
 *
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/include/cluster/cluster_extend_gate.h
 *
 * NOTES
 *	  This is a pgrac-original file.
 *	  Spec: spec-5.7-misc-enqueue-classes.md (§3.1d, v1.5 amend)
 *
 *-------------------------------------------------------------------------
 */
#ifndef CLUSTER_EXTEND_GATE_H
#define CLUSTER_EXTEND_GATE_H

#include "cluster/cluster_cf_storage.h" /* ClusterCfLiveness */

/*
 * ClusterExtendEngage -- outcome of the engage decision.
 *
 *	NATIVE       use the PG-native local path; no cross-node coordination is
 *	             needed (no alive peer can fork the authority).
 *	COORDINATE   engage the cluster authority/lock (a peer is alive and the
 *	             GES/LMS substrate is ready).
 *	FAIL_CLOSED  the class cannot prove the extend is safe -> the caller fails
 *	             closed (e.g. HW raises 53RA6) rather than guess.
 *	WAIT_LMS     classify-only intermediate: a peer is alive but LMS is not yet
 *	             ready.  cluster_extend_liveness_engage() resolves this to
 *	             COORDINATE (after a bounded wait) or FAIL_CLOSED; the public
 *	             entry point never returns WAIT_LMS.
 */
typedef enum ClusterExtendEngage {
	CLUSTER_EXTEND_ENGAGE_NATIVE = 0,
	CLUSTER_EXTEND_ENGAGE_COORDINATE,
	CLUSTER_EXTEND_ENGAGE_FAIL_CLOSED,
	CLUSTER_EXTEND_ENGAGE_WAIT_LMS
} ClusterExtendEngage;

/*
 * cluster_extend_engage_classify -- the pure decision core (no shmem reads, no
 * blocking), exposed for exhaustive unit testing.  Inputs are the runtime
 * facts the wrapper gathers; the wrapper resolves WAIT_LMS.
 *
 *	liveness        cluster_cf_assess_liveness() result.
 *	lms_ready       cluster_lms_is_ready().
 *	no_fencing      no voting disks configured (single-node-compat mode).
 *	cssd_ready      CSSD has reached READY (so alive_peer_count is trustworthy).
 *	any_alive_peer  cluster_cssd_get_alive_peer_count() != 0.
 */
extern ClusterExtendEngage cluster_extend_engage_classify(ClusterCfLiveness liveness,
														  bool lms_ready, bool no_fencing,
														  bool cssd_ready, bool any_alive_peer);

/*
 * cluster_extend_liveness_engage -- the public entry point.  Gathers the
 * runtime facts, calls the classifier, and (when wait_for_lms is true) performs
 * a bounded cluster_lms_wait_for_ready() to resolve a PEER_ALIVE + LMS-warming
 * case.  Returns NATIVE / COORDINATE / FAIL_CLOSED (never WAIT_LMS).
 *
 *	wait_for_lms  HW passes true (DML must wait out a brief LMS warmup rather
 *	              than fail closed); DL/KO pass false (they fail open / use
 *	              their own inner gates and must never block a bulk load on the
 *	              coordination layer being warm).
 */
extern ClusterExtendEngage cluster_extend_liveness_engage(bool wait_for_lms);

/*
 * cluster_extend_liveness_is_sole_native -- thin boolean over the §3.1d
 * classifier for callers that only need the SOLE->native decision (skip cluster
 * lock globalization) and must never introduce a bounded LMS wait.
 *
 * Returns true iff the runtime liveness resolves to the NATIVE class, i.e. no
 * alive peer can fork the authority:
 *	  - liveness == SOLE (CSSD-ready + in-quorum + no alive peer), or
 *	  - the approved no-fencing single-node-compat carve-out (Q21): UNKNOWN +
 *	    CSSD-ready + no voting disks + no alive peer.
 *
 * PEER_ALIVE (coordination needed), LMS-warming, and fenced-but-no-quorum all
 * return false, so a caller that gates on this predicate keeps its existing
 * globalize / fail-closed path unchanged for every non-SOLE case.  This is the
 * shared SOLE->native boundary already enforced for the HW/DL/KO extend classes
 * (§3.1d), reused by the lock.c globalize gate so a single-alive / declared-but-
 * dead-peer cluster takes the PG-native lock instead of timing out on a REQUEST
 * to a master node that is not alive (t/066).
 *
 * wait_for_lms is forced false internally: the lock hot path can never block on
 * the coordination layer warming up, and only the NATIVE result is acted upon,
 * so a PEER_ALIVE + LMS-warming case simply returns false (keep globalizing).
 */
extern bool cluster_extend_liveness_is_sole_native(void);

#endif /* CLUSTER_EXTEND_GATE_H */
