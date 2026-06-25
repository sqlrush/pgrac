/*-------------------------------------------------------------------------
 *
 * cluster_extend_gate.c
 *	  Shared runtime-liveness engage gate for the relation-extend enqueue
 *	  classes (HW / DL / KO).  See cluster_extend_gate.h for the contract.
 *
 *	  spec-5.7 §3.1d (v1.5 amend): replaces the cluster_conf_node_count() > 1
 *	  (configured-count) gate that fail-closed ordinary DML on single-alive /
 *	  degraded / LMS-warming clusters.  The decision is driven by runtime
 *	  liveness (cluster_cf_assess_liveness) + LMS readiness, with a
 *	  single-node-compat (no-fencing) carve-out so a node running without voting
 *	  disks and with no alive peer extends natively (Q21).
 *
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/backend/cluster/cluster_extend_gate.c
 *
 * NOTES
 *	  This is a pgrac-original file.
 *	  Spec: spec-5.7-misc-enqueue-classes.md (§3.1d, v1.5 amend)
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "cluster/cluster_cf_storage.h"
#include "cluster/cluster_cssd.h"
#include "cluster/cluster_extend_gate.h"
#include "cluster/cluster_guc.h"
#include "cluster/cluster_lms.h"
#include "cluster/cluster_qvotec.h"

/*
 * cluster_extend_engage_classify -- pure decision core (see header).
 *
 * Order matters and encodes the 8.A reasoning:
 *	1. PEER_ALIVE: a peer is demonstrably up, so an uncoordinated native extend
 *	   could let two nodes advance the same block-0 authority -> corruption.
 *	   Coordinate; if LMS is not ready yet, defer to a bounded wait (WAIT_LMS).
 *	2. SOLE: cluster_cf_assess_liveness proved CSSD-ready + in-quorum + no alive
 *	   peer -> nobody can fork the authority -> native is safe (this is also the
 *	   degraded-survivor-with-voting-disks path).
 *	3. UNKNOWN: assess_liveness could not prove SOLE.  Carve out the
 *	   single-node-compat case: with no fencing configured there is no partition
 *	   detection at all (the operator runs single-node semantics), so a
 *	   CSSD-ready node with no alive peer extends natively.  Everything else
 *	   (fencing configured but quorum not yet OK, or CSSD not ready) cannot be
 *	   proven safe -> fail closed.
 */
ClusterExtendEngage
cluster_extend_engage_classify(ClusterCfLiveness liveness, bool lms_ready, bool no_fencing,
							   bool cssd_ready, bool any_alive_peer)
{
	if (liveness == CLUSTER_CF_LIVENESS_PEER_ALIVE)
		return lms_ready ? CLUSTER_EXTEND_ENGAGE_COORDINATE : CLUSTER_EXTEND_ENGAGE_WAIT_LMS;

	if (liveness == CLUSTER_CF_LIVENESS_SOLE)
		return CLUSTER_EXTEND_ENGAGE_NATIVE;

	/* UNKNOWN: single-node-compat (no fencing) carve-out, else fail closed. */
	if (no_fencing && cssd_ready && !any_alive_peer)
		return CLUSTER_EXTEND_ENGAGE_NATIVE;

	return CLUSTER_EXTEND_ENGAGE_FAIL_CLOSED;
}

/*
 * cluster_extend_liveness_engage -- public entry point (see header).
 */
ClusterExtendEngage
cluster_extend_liveness_engage(bool wait_for_lms)
{
	ClusterExtendEngage decision;

	decision = cluster_extend_engage_classify(cluster_cf_assess_liveness(), cluster_lms_is_ready(),
											  cluster_qvotec_get_disks_total_count() == 0,
											  cluster_cssd_get_status() == CLUSTER_CSSD_READY,
											  cluster_cssd_get_alive_peer_count() != 0);

	if (decision == CLUSTER_EXTEND_ENGAGE_WAIT_LMS) {
		/*
		 * A peer is alive but LMS is still warming up.  HW waits out the brief,
		 * autonomous STARTING->READY window (cluster_lms.c) rather than fail a
		 * DML; if it does not become ready within the GES request budget the
		 * caller still fails closed (we never extend uncoordinated with a live
		 * peer).  Callers that pass wait_for_lms = false get FAIL_CLOSED here
		 * and map it to their own fail-open/inner-gate policy.
		 */
		if (wait_for_lms && cluster_lms_wait_for_ready(cluster_ges_request_timeout_ms))
			return CLUSTER_EXTEND_ENGAGE_COORDINATE;
		return CLUSTER_EXTEND_ENGAGE_FAIL_CLOSED;
	}

	return decision;
}

/*
 * cluster_extend_liveness_is_sole_native -- thin SOLE->native predicate over the
 * shared classifier (see header).  Reused by the lock.c globalize gate.
 */
bool
cluster_extend_liveness_is_sole_native(void)
{
	/*
	 * wait_for_lms = false: the lock gate must never block a plain lock acquire
	 * on the coordination layer warming up, and we act only on the NATIVE
	 * result.  A PEER_ALIVE + LMS-warming case therefore resolves to
	 * FAIL_CLOSED here, which is not NATIVE, so the caller falls through to its
	 * existing globalize path -- the peer-alive behaviour is unchanged.
	 */
	return cluster_extend_liveness_engage(false) == CLUSTER_EXTEND_ENGAGE_NATIVE;
}
