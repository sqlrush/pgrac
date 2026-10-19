/*-------------------------------------------------------------------------
 *
 * cluster_relmap_arb.h
 *	  Crash arbitration of staged relmap-authority pending images
 *	  (spec-6.14 D5-activation, §3.2).
 *
 *	  A relmap writer that crashed after staging its pending image but
 *	  before publishing left the authority with pending_generation != 0 and
 *	  a durable owner identity {owner_node, owner_xid, owner_epoch,
 *	  relmap_lsn}.  The sole merger (merge-claim holder, INV-D9-R) resolves
 *	  every such pending at the merged-recovery completion point, before
 *	  releasing the claim:
 *
 *	    owner committed  -> publish (the deferred half of the owner's
 *	                        post-commit publish) + relmap invalidation
 *	                        broadcast to all live peers
 *	    owner aborted    -> discard the pending
 *	    indeterminable   -> keep the pending; relmap WRITES stay 53RB
 *	                        fail-closed, reads are unaffected (committed
 *	                        image only, INV-14-7)
 *
 *	  Status lookup is routed by origin: an OWN owner_xid requires an
 *	  explicit COMMITTED or ABORTED slot in local pg_xact, and a FOREIGN
 *	  owner_xid reads the per-origin remote-xact store, durable-checked.
 *	  Missing commit evidence and SUBTRANS parent inference cannot resolve
 *	  an own owner.  Origin routing and this v1 identity do not establish a
 *	  FullTransactionId era or retention bounds.
 *
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/include/cluster/cluster_relmap_arb.h
 *
 * NOTES
 *	  This is a pgrac-original file.
 *	  Spec: spec-6.14-shared-catalog-single-authority.md (D5, §3.2)
 *
 *-------------------------------------------------------------------------
 */
#ifndef CLUSTER_RELMAP_ARB_H
#define CLUSTER_RELMAP_ARB_H

/*
 * cluster_relmap_arbitrate_pendings -- resolve every staged pending relmap
 * image (global + per-database authorities).  Called from StartupXLOG right
 * before the merge claim is released; no-op unless cluster.shared_catalog
 * is on and this node holds the claim (i.e. it was the sole merger).
 */
extern void cluster_relmap_arbitrate_pendings(void);

#endif /* CLUSTER_RELMAP_ARB_H */
