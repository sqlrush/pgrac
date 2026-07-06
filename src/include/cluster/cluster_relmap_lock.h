/*-------------------------------------------------------------------------
 *
 * cluster_relmap_lock.h
 *	  Singleton cross-node relmap-authority X lock (spec-6.14 D5-activation).
 *
 *	  Serialises relation-map authority writers cluster-wide: a mapped-catalog
 *	  rewrite takes this GES X lock before staging its pending image and holds
 *	  it across commit until the post-publish invalidation is acknowledged by
 *	  every live peer (spec §3.2: the map-before-content ordering comes from
 *	  locking, not from a pre-publish barrier).  Mirrors the OID-authority
 *	  singleton lock (spec-6.14 D6) and the CF singleton lock (spec-5.6).
 *
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/include/cluster/cluster_relmap_lock.h
 *
 * NOTES
 *	  This is a pgrac-original file.
 *	  Spec: spec-6.14-shared-catalog-single-authority.md (D5, §3.2)
 *
 *-------------------------------------------------------------------------
 */
#ifndef CLUSTER_RELMAP_LOCK_H
#define CLUSTER_RELMAP_LOCK_H

#include "cluster/cluster_grd.h"		/* ClusterResId */

/*
 * CLUSTER_RELMAP_RESID_TYPE -- relmap-authority resource-id namespace marker
 * for the singleton cross-node lock.  0xF7 is the OID authority (D6); 0xF8
 * was reserved for the relmap global lock at D5-substrate time.
 */
#define CLUSTER_RELMAP_RESID_TYPE 0xF8

/*
 * cluster_relmap_resid_encode -- build the singleton relmap-authority
 * resource id (all map fields zero; the type byte places it in the relmap
 * namespace).  Mirrors cluster_oid_resid_encode.
 */
extern void cluster_relmap_resid_encode(ClusterResId *dst);

/*
 * cluster_relmap_authority_x_lock -- acquire the singleton relmap-authority
 * X lock (GES; ClusterRelmapWrite wait event).  Returns false on timeout /
 * lock-service unavailability (caller fail-closes).  Must not be called
 * while already holding it (Assert).
 */
extern bool cluster_relmap_authority_x_lock(void);

/*
 * cluster_relmap_authority_x_unlock -- release if held (idempotent).
 */
extern void cluster_relmap_authority_x_unlock(void);

/*
 * cluster_relmap_authority_x_held -- is this backend the holder?
 */
extern bool cluster_relmap_authority_x_held(void);

/*
 * cluster_relmap_lock_abort_release -- (Sub)AbortTransaction hook: drop the
 * hold when the owning transaction aborts BEFORE its commit record (the
 * pending image is then dead and arbitration discards it).  Post-commit
 * failures never come through here: the publish path retries and PANICs
 * (r4-P1), so the lock dies with the node and reconfig + crash arbitration
 * take over.
 */
extern void cluster_relmap_lock_abort_release(void);

#endif							/* CLUSTER_RELMAP_LOCK_H */
