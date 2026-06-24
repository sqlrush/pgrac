/*-------------------------------------------------------------------------
 *
 * cluster_ts.h
 *	  TT (tablespace-DDL) cross-node enqueue lock -- spec-5.7 §3.3 / D5.
 *
 *	  spec-5.7 calls this lock class "TT" (the tablespace-DDL enqueue), and its
 *	  resid namespace marker is CLUSTER_TT_RESID_TYPE (0xF4, §2.1).  The MODULE is
 *	  named cluster_ts (not cluster_tt) on purpose: the cluster_tt_* file family is
 *	  the MVCC Transaction Table (cluster_tt_slot / _status / _2pc / _durable ...),
 *	  an entirely different subsystem -- a cluster_tt.c here would read as
 *	  "Transaction Table core".  The resid constant keeps the spec name.
 *
 *	  CREATE / DROP / ALTER / RENAME TABLESPACE have only a LOCAL pg_tablespace
 *	  RowExclusiveLock + the local TablespaceCreateLock today (§0.4 F0-15), so two
 *	  nodes can run conflicting tablespace DDL concurrently.  TT(X) is the
 *	  cross-node mutex around those four DDLs.  resid IDENTITY is split by DDL kind
 *	  (TT-M1):
 *	    - DROP / ALTER (an EXISTING object) -> resid = tablespace OID (stable).
 *	    - CREATE / RENAME (NAME-based DDL)  -> resid = hash(spcname).  At CREATE
 *	      time the OID is not yet assigned, and two nodes creating the same name
 *	      each get a DIFFERENT OID (GetNewOidWithIndex) -> an OID resid would NOT
 *	      collide and the cross-node same-name CREATE would slip through.  The
 *	      catalog NAME hash is the only identity that serialises it (R14).
 *
 *	  TT(S) is a NARROW in-use guard (TT-M2, P1#6): ONLY placement / membership
 *	  DDL takes it -- CREATE a relation INTO a tablespace, ALTER ... SET TABLESPACE
 *	  -- so a placement DDL blocks a concurrent cross-node DROP TABLESPACE (TT(X)).
 *	  It is NEVER taken on the normal DML read/write path (that would serialise the
 *	  whole cluster on a tablespace -- a performance/blast disaster).  S and X
 *	  conflict through the standard lock matrix (ShareLock vs ExclusiveLock).
 *
 *	  Lifetime: a TT lock is held until the TOP transaction that took it ends
 *	  (commit OR abort), via a TopTransactionContext reset callback -- so a peer
 *	  cannot proceed until this node's DDL is durable.  The tablespace DDLs are
 *	  PreventInTransactionBlock (each its own single-statement top transaction), so
 *	  the same-backend S->X convert (TT-M3) does not arise within tablespace DDL;
 *	  the S/X CONFLICT it guards against is cross-backend and handled by the matrix.
 *
 *	  A genuine grant failure fails closed (53RA8) rather than running tablespace
 *	  DDL without the cross-node lock; an uncoordinated/native outcome (single node
 *	  / cluster off) proceeds (the local catalog locks are sufficient there).
 *
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/include/cluster/cluster_ts.h
 *
 * NOTES
 *	  This is a pgrac-original file (no derivation from PostgreSQL).
 *	  Spec: spec-5.7-misc-enqueue-classes.md (D5, §3.3)
 *
 *-------------------------------------------------------------------------
 */
#ifndef CLUSTER_TS_H
#define CLUSTER_TS_H

#include "cluster/cluster_grd.h" /* ClusterResId */
#include "cluster/cluster_hw.h"	 /* CLUSTER_HW_RESID_TYPE (collision check) */
#include "cluster/cluster_ir.h"	 /* CLUSTER_IR_RESID_TYPE (collision check) */
#include "storage/lock.h"		 /* LOCKTAG_LAST_TYPE */

/*
 * CLUSTER_TT_RESID_TYPE -- TT (tablespace-DDL) resource-id namespace marker
 * (spec §2.1).  Above every PG LockTagType, distinct from SQ (0xF0) / CF (0xF1) /
 * HW (0xF2) / DL (0xF3) / IR (0xF5).
 */
#define CLUSTER_TT_RESID_TYPE 0xF4

StaticAssertDecl(CLUSTER_TT_RESID_TYPE > LOCKTAG_LAST_TYPE,
				 "TT resid namespace must not collide with any PG LockTagType");
StaticAssertDecl(CLUSTER_TT_RESID_TYPE != CLUSTER_HW_RESID_TYPE,
				 "TT and HW resid namespaces must be distinct");
StaticAssertDecl(CLUSTER_TT_RESID_TYPE != CLUSTER_IR_RESID_TYPE,
				 "TT and IR resid namespaces must be distinct");

/* ---- pure layer (standalone-linkable; never ereports) --------------- */

/*
 * cluster_ts_resid_encode_oid -- TT resid for an EXISTING tablespace (DROP /
 * ALTER): field1 = tablespace OID (stable identity), field2/3/4 = 0.
 */
extern void cluster_ts_resid_encode_oid(Oid tablespace_oid, ClusterResId *dst);

/*
 * cluster_ts_resid_encode_name -- TT resid for a NAME-based DDL (CREATE /
 * RENAME): field1 = a deterministic hash of the tablespace name (same on every
 * node -- the OID is not yet stable), field2/3/4 = 0.  A hash collision only
 * over-serialises two different names (no missed mutex), which is safe.
 */
extern void cluster_ts_resid_encode_name(const char *spcname, ClusterResId *dst);

#ifndef FRONTEND

#include "cluster/cluster_lock_acquire.h" /* ClusterLockAcquireRequest */
#include "utils/palloc.h"				  /* MemoryContextCallback */

/*
 * ClusterTsLock -- a held TT lock.  Allocated in TopTransactionContext; its
 * reset callback releases the lock when the top transaction ends (commit OR
 * abort).  coordinated == false means the GES resolved this to a native/local
 * outcome (cluster off / single node) -- release is a no-op.
 */
typedef struct ClusterTsLock {
	bool held;
	bool coordinated;
	ClusterResId resid;
	LOCKMODE mode;					/* ExclusiveLock = TT(X), ShareLock = TT(S) */
	ClusterLockAcquireRequest req;	/* for the S6 release */
	MemoryContextCallback reset_cb; /* top-xact-end release */
} ClusterTsLock;

/*
 * cluster_ts_ddl_lock_x_oid -- TT-M1: take TT(X) on an EXISTING tablespace
 * (DROP / ALTER), keyed on its OID, held to top-xact end.  No-op when the cluster
 * tablespace-DDL coordination is off / single-node / in recovery.  A genuine
 * grant failure ereports 53RA8 (never runs the DDL without the lock).
 */
extern void cluster_ts_ddl_lock_x_oid(Oid tablespace_oid);

/*
 * cluster_ts_ddl_lock_x_name -- TT-M1: take TT(X) for a NAME-based DDL (CREATE /
 * RENAME), keyed on hash(spcname), held to top-xact end.  Serialises concurrent
 * cross-node same-name CREATE/RENAME (R14).  Same gating / fail-closed as above.
 */
extern void cluster_ts_ddl_lock_x_name(const char *spcname);

/*
 * cluster_ts_placement_lock_s -- TT-M2: take TT(S) on an EXISTING tablespace a
 * placement DDL is putting an object into (CREATE relation INTO ts / ALTER ...
 * SET TABLESPACE ts), keyed on the ts OID, held to top-xact end.  Blocks a
 * concurrent cross-node DROP TABLESPACE (TT(X)) until the placement commits.
 * NEVER called on the normal DML path.  Same gating / fail-closed as above.
 */
extern void cluster_ts_placement_lock_s(Oid tablespace_oid);

/* Minimal shmem region (four counters); mirror cluster_dl_shmem_*. */
extern Size cluster_ts_shmem_size(void);
extern void cluster_ts_shmem_init(void);
extern void cluster_ts_shmem_register(void);

/*
 * TEST-ONLY (cluster_ts_srf.c): acquire/release TT(`mode`) on an explicit resid,
 * held in *lk WITHOUT the production top-xact-end callback (the probe SRF holds it
 * in a session static across SQL statements, so a competing claim can observe the
 * conflict).  cluster_ts_test_acquire returns 0 granted (held in *lk) / 1 native /
 * 2 conflict-or-failed.  NOT a product path.
 */
extern int cluster_ts_test_acquire(const ClusterResId *resid, LOCKMODE mode, ClusterTsLock *lk);
extern void cluster_ts_test_release(ClusterTsLock *lk);

/* Observability counters (dump_ts; surfaced by pg_cluster_state). */
extern uint64 cluster_ts_x_count(void);			 /* TT(X) DDL locks granted (coordinated) */
extern uint64 cluster_ts_s_count(void);			 /* TT(S) placement locks granted (coordinated) */
extern uint64 cluster_ts_native_count(void);	 /* uncoordinated / native proceed */
extern uint64 cluster_ts_failclosed_count(void); /* 53RA8 fail-closed */

#endif /* !FRONTEND */

#endif /* CLUSTER_TS_H */
