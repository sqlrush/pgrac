/*-------------------------------------------------------------------------
 *
 * cluster_dl.h
 *	  DL (bulk-load lease) cross-node coordination lock -- spec-5.7 §3.2 / D4.
 *
 *	  A bulk load (COPY / CREATE TABLE AS / REFRESH MATERIALIZED VIEW) extends a
 *	  relation many times.  DL(X) is a COARSE per-relation lease the loader holds
 *	  across the whole bulk operation so two nodes do not interleave bulk loads of
 *	  the same relation (which would fragment its extents and thrash the HW
 *	  authority).  DL is a COORDINATION lease, not a correctness gate: the HW
 *	  authority (§3.1a) is what guarantees no two nodes ever allocate the same
 *	  block.  So DL backs off cleanly when the cluster coordination layer is
 *	  inactive (HW still protects correctness at the extend), and a genuine grant
 *	  failure fails closed (53RA7) rather than bulk-load without the lease.
 *
 *	  Lock order (DL-M2, fixed, deadlock-free): DL(X) [per relation] ->
 *	  HW(X) [per (rel,fork), acquired inside the extend] -> the PG-native
 *	  LockRelationForExtension.  DL is the OUTERMOST, acquired before any extend.
 *
 *	  Hook (DL-M1): the lease attaches to the BulkInsertState lifecycle.  A bulk
 *	  path (COPY/CTAS/matview) creates a BulkInsertState (GetBulkInsertState) and
 *	  passes it to heap_insert / heap_multi_insert; the lease is acquired lazily on
 *	  the first such call (the relation is known there) and released at
 *	  FreeBulkInsertState.  A regular INSERT / INSERT...SELECT passes bistate ==
 *	  NULL (PG16 row-by-row heap_insert) and takes NO DL -- it is covered by HW
 *	  alone (spec §3.2).
 *
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/include/cluster/cluster_dl.h
 *
 * NOTES
 *	  This is a pgrac-original file (no derivation from PostgreSQL).
 *	  Spec: spec-5.7-misc-enqueue-classes.md (D4, §3.2)
 *
 *-------------------------------------------------------------------------
 */
#ifndef CLUSTER_DL_H
#define CLUSTER_DL_H

#include "cluster/cluster_grd.h"	/* ClusterResId */
#include "cluster/cluster_hw.h"		/* CLUSTER_HW_RESID_TYPE (collision check) */
#include "storage/relfilelocator.h" /* RelFileLocator */
#include "storage/lock.h"			/* LOCKTAG_LAST_TYPE */

/*
 * CLUSTER_DL_RESID_TYPE -- DL resource-id namespace marker.  Above every PG
 * LockTagType, distinct from SQ (0xF0) / CF (0xF1) / HW (0xF2).
 */
#define CLUSTER_DL_RESID_TYPE 0xF3

StaticAssertDecl(CLUSTER_DL_RESID_TYPE > LOCKTAG_LAST_TYPE,
				 "DL resid namespace must not collide with any PG LockTagType");
StaticAssertDecl(CLUSTER_DL_RESID_TYPE != CLUSTER_HW_RESID_TYPE,
				 "DL and HW resid namespaces must be distinct");

/* ---- pure layer (standalone-linkable; never ereports) --------------- */

/*
 * cluster_dl_resid_encode -- build the DL resource id for a relation.  DL is
 * per-RELATION (not per-fork like HW): field1 = dbOid, field2 = relNumber (the
 * relfilenode -- ABA defence), field3 = spcOid, field4 = 0 (no fork).  Two
 * nodes bulk-loading the same relation hash to the same resid and serialise.
 */
extern void cluster_dl_resid_encode(RelFileLocator rloc, ClusterResId *dst);

#ifndef FRONTEND

#include "cluster/cluster_lock_acquire.h" /* ClusterLockAcquireRequest */
#include "utils/palloc.h"				  /* MemoryContextCallback */

struct RelationData;
struct BulkInsertStateData;

/*
 * DlLock -- a held DL(X) lease.  Allocated in CurTransactionContext off the
 * BulkInsertState that carries it.  coordinated == false means the lease was not
 * GES-coordinated (cluster layer inactive) -- release is a no-op, HW still
 * protects the extend.
 *
 * reset_cb is a (sub)transaction-end backstop: a bulk load holds DL across the
 * whole operation, but FreeBulkInsertState is NOT in a PG_FINALLY, so a COPY/CTAS
 * that ERRORs mid-load would skip the explicit release.  DL has no PG-native
 * heavyweight lock, so the spec-2.21 LockReleaseAll release hook does not cover
 * it either.  Registering this callback on CurTransactionContext releases the
 * lease when that (sub)transaction ends (commit OR abort), so an aborted bulk
 * load can never leak the lease and wedge peer bulk loads on the same relation.
 * dl_unlock is idempotent, so the explicit release + this backstop are safe
 * together.
 */
typedef struct DlLock {
	bool held;
	bool coordinated;
	ClusterResId resid;
	ClusterLockAcquireRequest req;	/* for the S6 release */
	MemoryContextCallback reset_cb; /* xact-end release backstop */
} DlLock;

/*
 * cluster_dl_bulk_acquire -- §3.2 DL-M1: acquire the DL(X) lease for a bulk load,
 * lazily, keyed on the BulkInsertState.  No-op when: bistate == NULL (regular
 * insert -> HW only); the lease is already held (subsequent batches); the cluster
 * relation-extend coordination is off / single-node / in recovery; or the
 * relation is not GLOBALIZE (temp / unlogged -> HW handles it at the extend).
 * On a GLOBALIZE relation in a live multi-node cluster it acquires DL(X) before
 * any extend (lock order DL -> HW -> local); a genuine grant failure ereports
 * 53RA7 (never bulk-loads without the lease).  An uncoordinated/native outcome
 * proceeds (held=false) -- HW still gates correctness at the extend.
 */
extern void cluster_dl_bulk_acquire(struct RelationData *rel, struct BulkInsertStateData *bistate);

/*
 * cluster_dl_bulk_release -- release the DL(X) lease a BulkInsertState holds (if
 * any) and free its DlLock.  Called from FreeBulkInsertState.  A no-op when no
 * lease is held.
 */
extern void cluster_dl_bulk_release(struct BulkInsertStateData *bistate);

/* Minimal shmem region (four counters); mirror cluster_sequence_shmem_*. */
extern Size cluster_dl_shmem_size(void);
extern void cluster_dl_shmem_init(void);
extern void cluster_dl_shmem_register(void);

/* Observability counters (dump_dl; surfaced by pg_cluster_state). */
extern uint64 cluster_dl_lease_count(void);		 /* DL(X) leases granted (coordinated) */
extern uint64 cluster_dl_native_count(void);	 /* uncoordinated / native proceed */
extern uint64 cluster_dl_failclosed_count(void); /* 53RA7 fail-closed */
extern uint64
cluster_dl_release_count(void); /* coordinated leases released (incl. xact-end backstop) */

#endif /* !FRONTEND */

#endif /* CLUSTER_DL_H */
