/*-------------------------------------------------------------------------
 *
 * cluster_ts.c
 *	  TT (tablespace-DDL) cross-node enqueue lock -- PURE layer (spec-5.7 §3.3 /
 *	  D5).  Just the two resource-id encoders, standalone-linkable so the
 *	  cluster_unit test links them directly.  The backend (shmem counters, GES
 *	  acquire, top-xact-end release) lives in cluster_ts_lock.c.
 *
 *	  See cluster_ts.h for why the module is named "ts" while the spec lock class
 *	  + resid constant are "TT".
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/backend/cluster/cluster_ts.c
 *
 * NOTES
 *	  This is a pgrac-original file (no derivation from PostgreSQL).
 *	  Spec: spec-5.7-misc-enqueue-classes.md (D5, §3.3)
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "cluster/cluster_ts.h"


/*
 * ts_name_hash -- a deterministic 32-bit hash of a tablespace name (FNV-1a).
 * Self-contained (no libpgcommon dependency, so the pure layer links standalone)
 * and seed-free, so every node hashes the same name to the same value -- which is
 * the whole point: two nodes creating the same-named tablespace must route to the
 * same TT resid (the OID is not yet stable at CREATE time, R14).
 */
static uint32
ts_name_hash(const char *spcname)
{
	uint32 hash = 2166136261U; /* FNV-1a 32-bit offset basis */
	const unsigned char *p;

	if (spcname == NULL)
		return 0;
	for (p = (const unsigned char *)spcname; *p != '\0'; p++) {
		hash ^= (uint32)*p;
		hash *= 16777619U; /* FNV-1a 32-bit prime */
	}
	return hash;
}

/*
 * cluster_ts_resid_encode_oid -- TT resid for an EXISTING tablespace (DROP /
 * ALTER): field1 = tablespace OID (stable).
 */
void
cluster_ts_resid_encode_oid(Oid tablespace_oid, ClusterResId *dst)
{
	Assert(dst != NULL);
	if (dst == NULL)
		return;

	dst->field1 = (uint32)tablespace_oid;
	dst->field2 = 0;
	dst->field3 = 0;
	dst->field4 = 0;
	dst->type = CLUSTER_TT_RESID_TYPE;
	dst->lockmethodid = DEFAULT_LOCKMETHOD;
}

/*
 * cluster_ts_resid_encode_name -- TT resid for a NAME-based DDL (CREATE /
 * RENAME): field1 = hash(spcname).  The high bit pattern of field4 stays 0 so an
 * OID resid (field1 = OID, often small) and a name resid (field1 = a spread-out
 * hash) almost never alias; even if they did, both being TT(0xF4) on field1
 * only over-serialises, which is safe.
 */
void
cluster_ts_resid_encode_name(const char *spcname, ClusterResId *dst)
{
	Assert(dst != NULL);
	if (dst == NULL)
		return;

	dst->field1 = ts_name_hash(spcname);
	dst->field2 = 0;
	dst->field3 = 0;
	dst->field4 = 0;
	dst->type = CLUSTER_TT_RESID_TYPE;
	dst->lockmethodid = DEFAULT_LOCKMETHOD;
}
