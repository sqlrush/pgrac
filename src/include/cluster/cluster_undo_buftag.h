/*-------------------------------------------------------------------------
 *
 * cluster_undo_buftag.h
 *	  pgrac buffer-backed undo identity layer (spec-3.27 D1, §2.1-A / §4).
 *
 *	  Maps an undo (owner_instance, segment_id, block_no) to a BufferTag so the
 *	  PG shared buffer manager can key undo blocks in its BufTable.  A BufferTag
 *	  is {RelFileLocator(spc,db,rel), ForkNumber, BlockNumber};  this header
 *	  defines the RelFileLocator half (fork = MAIN_FORKNUM, block = undo
 *	  block_no are supplied at the bufmgr call site in D2/D3).
 *
 *	  Reserved-OID isolation (the §4 hard prerequisite — zero collision with any
 *	  real relation's BufferTag):
 *
 *	    spcOid = CLUSTER_UNDO_REL_SPCOID  — a RESERVED tablespace OID that
 *	      initdb / CREATE TABLESPACE never assign.  Only DEFAULTTABLESPACE_OID
 *	      (1663) and GLOBALTABLESPACE_OID (1664) are built in, and
 *	      GetNewObjectId only ever hands out spcOids >= FirstNormalObjectId
 *	      (16384) after it wraps past the reserved [0, FirstNormalObjectId)
 *	      band.  Since NO real relation ever carries this spcOid, an undo
 *	      (spcOid,dbOid,relNumber,fork,block) BufferTag can never equal a real
 *	      relation's — this is the primary uniqueness proof (§4 pt 1,2,7).
 *
 *	    dbOid = CLUSTER_UNDO_DB_BASE + owner_instance  — carries the per-instance
 *	      identity so undo segments of different cluster instances key distinctly
 *	      in the BufTable.  RelFileNumber is a 32-bit Oid (relpath.h:25 — NOT the
 *	      56-bit PG17 type), so we do NOT bit-pack instance into relNumber;  the
 *	      full 32-bit relNumber carries segment_id and dbOid carries instance.
 *
 *	  §4 pt 3 note — DropDatabaseBuffers(Oid dbid) scans bufHdr->tag.dbOid only,
 *	  ignoring spcOid (bufmgr.c).  So spcOid routing does NOT protect undo
 *	  buffers from DropDatabaseBuffers.  The reserved dbOid range
 *	  [CLUSTER_UNDO_DB_BASE, CLUSTER_UNDO_DB_BASE + UNDO_OWNER_INSTANCE_MAX] is
 *	  proven safe not by spcOid but by never being a real database:  CREATE /
 *	  DROP DATABASE only ever operate on OIDs present in pg_database, and
 *	  GetNewObjectId assigns real db OIDs >= FirstNormalObjectId.  The
 *	  StaticAssert below pins the whole range strictly below FirstNormalObjectId
 *	  (and above the built-in Template1/Template0/Postgres db OIDs 1/4/5), so it
 *	  can never overlap a dynamically allocated or built-in db OID.  A human who
 *	  hand-calls DropDatabaseBuffers(CLUSTER_UNDO_DB_BASE+n) would clear undo
 *	  buffers — an operator-error boundary, not a protected path (same class as
 *	  a stray DropRelationBuffers).
 *
 *	  owner_instance is the 1-based undo instance id (= cluster_node_id + 1),
 *	  matching cluster_undo_smgr.c and cluster_undo_segment.h:  node_id
 *	  0..CLUSTER_MAX_NODES-1 maps to owner_instance 1..UNDO_OWNER_INSTANCE_MAX.
 *	  owner_instance 0 (UNDO_OWNER_INSTANCE_INVALID) is never a live undo buffer.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * Spec: spec-3.27-undo-buffer-backed-model.md (FROZEN v1.0, D1 / §2.1-A / §4)
 *
 * IDENTIFICATION
 *	  src/include/cluster/cluster_undo_buftag.h
 *
 * NOTES
 *	  This is a pgrac-original file (no derivation from PostgreSQL).
 *	  Backend-only:  RelFileLocator / bufmgr are backend concepts, so the whole
 *	  header is guarded by #ifndef FRONTEND.
 *
 *-------------------------------------------------------------------------
 */
#ifndef CLUSTER_UNDO_BUFTAG_H
#define CLUSTER_UNDO_BUFTAG_H

#ifndef FRONTEND

#include "access/transam.h"			 /* FirstNormalObjectId */
#include "catalog/pg_database_d.h"	 /* Template1/Template0/Postgres db OIDs */
#include "catalog/pg_tablespace_d.h" /* DEFAULTTABLESPACE_OID, GLOBALTABLESPACE_OID */
#include "common/relpath.h"			 /* RelFileNumber */
#include "storage/relfilelocator.h"	 /* RelFileLocator, RelFileLocatorEquals */

#include "cluster/cluster_conf.h"		  /* CLUSTER_MAX_NODES */
#include "cluster/cluster_undo_segment.h" /* UNDO_OWNER_INSTANCE_MAX/_INVALID */


/*
 * Reserved tablespace OID for all buffer-backed undo (§4 pt 1,2,7).  Distinct
 * from the two built-in tablespaces;  never assigned by initdb / CREATE
 * TABLESPACE / GetNewObjectId (all real spcOids are 1663, 1664, or
 * >= FirstNormalObjectId).
 */
#define CLUSTER_UNDO_REL_SPCOID ((Oid)9998)

/*
 * Base of the reserved per-instance dbOid range (§4 pt 3).  dbOid =
 * CLUSTER_UNDO_DB_BASE + owner_instance, owner_instance in
 * [1, UNDO_OWNER_INSTANCE_MAX].  The whole range stays below FirstNormalObjectId
 * so it can never overlap a dynamically allocated database OID, and above the
 * built-in db OIDs (1/4/5) so it never overlaps a built-in one.
 */
#define CLUSTER_UNDO_DB_BASE ((Oid)9200)

StaticAssertDecl(CLUSTER_UNDO_DB_BASE + UNDO_OWNER_INSTANCE_MAX < FirstNormalObjectId,
				 "reserved undo dbOid range must stay below FirstNormalObjectId "
				 "(never dynamically allocated)");
StaticAssertDecl(CLUSTER_UNDO_DB_BASE > PostgresDbOid,
				 "reserved undo dbOid range must sit above all built-in database OIDs");
StaticAssertDecl(CLUSTER_UNDO_REL_SPCOID < FirstNormalObjectId,
				 "reserved undo spcOid must stay below dynamically-allocated tablespace OIDs");
StaticAssertDecl(CLUSTER_UNDO_REL_SPCOID != DEFAULTTABLESPACE_OID
					 && CLUSTER_UNDO_REL_SPCOID != GLOBALTABLESPACE_OID,
				 "reserved undo spcOid must not collide with a built-in tablespace");
StaticAssertDecl(UNDO_OWNER_INSTANCE_MAX <= CLUSTER_MAX_NODES,
				 "undo owner-instance bound must fit the cluster node-id space");


/*
 * cluster_undo_relfilelocator -- map (owner_instance, segment_id) to the
 *	reserved RelFileLocator half of an undo BufferTag.
 *
 *	The BufferTag is completed at the bufmgr call site with MAIN_FORKNUM and the
 *	undo block_no.  owner_instance in [1, UNDO_OWNER_INSTANCE_MAX];  segment_id
 *	uses the full 32-bit relNumber space.
 */
static inline RelFileLocator
cluster_undo_relfilelocator(uint8 owner_instance, RelFileNumber segment_id)
{
	RelFileLocator rl;

	rl.spcOid = CLUSTER_UNDO_REL_SPCOID;
	rl.dbOid = CLUSTER_UNDO_DB_BASE + owner_instance;
	rl.relNumber = segment_id;
	return rl;
}

/*
 * cluster_undo_locator_is_undo -- true iff a RelFileLocator addresses undo
 *	storage (spcOid == the reserved undo tablespace).  A single spcOid test is
 *	sufficient because no real relation ever carries CLUSTER_UNDO_REL_SPCOID.
 */
static inline bool
cluster_undo_locator_is_undo(RelFileLocator rl)
{
	return rl.spcOid == CLUSTER_UNDO_REL_SPCOID;
}

/*
 * cluster_undo_locator_owner_instance / _segment_id -- reverse-decode an undo
 *	RelFileLocator back to its (owner_instance, segment_id).  Callers must first
 *	confirm cluster_undo_locator_is_undo().
 */
static inline uint8
cluster_undo_locator_owner_instance(RelFileLocator rl)
{
	return (uint8)(rl.dbOid - CLUSTER_UNDO_DB_BASE);
}

static inline RelFileNumber
cluster_undo_locator_segment_id(RelFileLocator rl)
{
	return rl.relNumber;
}

#endif /* !FRONTEND */

#endif /* CLUSTER_UNDO_BUFTAG_H */
