/*-------------------------------------------------------------------------
 *
 * cluster_block_recovery_srf.c
 *	  pgrac TEST-ONLY SQL entry point driving the online single-block
 *	  recovery reconstructor, for the spec-4.10 D2 crash-recovery differential.
 *
 *	  cluster_block_recovery_reconstruct_test(rel regclass, forknum int4,
 *	                                          blocknum int8, scan_lower pg_lsn,
 *	                                          scan_upper pg_lsn) -> bytea
 *
 *	  Resolves the relation's storage identity and calls
 *	  cluster_block_recovery_reconstruct() over the explicit WAL window.
 *	  Returns the reconstructed BLCKSZ page as bytea on RECOVERED, or NULL when
 *	  reconstruction fails closed (UNRECOVERABLE).
 *
 *	  The cluster_tap differential (t/256) compares this page byte-for-byte
 *	  against the same block as produced by PG's real crash-recovery redo --
 *	  the 8.A correctness guarantee for the reconstruct path.
 *
 *	  TEST-ONLY: a diagnostic entry point, NOT a product query interface;
 *	  superuser-only.  Mirrors cluster_block_apply_srf.c.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * Spec: spec-4.10-online-block-recovery.md (FROZEN v0.4)
 *
 * IDENTIFICATION
 *	  src/backend/cluster/cluster_block_recovery_srf.c
 *
 * NOTES
 *	  This is a pgrac-original file (no derivation from PostgreSQL).
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "fmgr.h"

PG_FUNCTION_INFO_V1(cluster_block_recovery_reconstruct_test);

#ifdef USE_PGRAC_CLUSTER

#include "access/relation.h"
#include "access/xlogdefs.h"
#include "miscadmin.h"
#include "storage/bufpage.h"
#include "storage/relfilelocator.h"
#include "utils/pg_lsn.h"
#include "utils/rel.h"
#include "varatt.h"

#include "cluster/cluster_block_recovery.h"

Datum
cluster_block_recovery_reconstruct_test(PG_FUNCTION_ARGS)
{
	Oid relid;
	int32 forknum;
	int64 blocknum;
	XLogRecPtr scan_lower;
	XLogRecPtr scan_upper;
	RelFileLocator target;
	Relation rel;
	PGAlignedBlock pagebuf;
	ClusterBlkRecResult res;
	bytea *result;

	if (!superuser())
		ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
						errmsg("cluster_block_recovery_reconstruct_test is superuser-only")));

	relid = PG_GETARG_OID(0);
	forknum = PG_GETARG_INT32(1);
	blocknum = PG_GETARG_INT64(2);
	scan_lower = PG_GETARG_LSN(3);
	scan_upper = PG_GETARG_LSN(4);

	if (forknum < 0 || forknum > MAX_FORKNUM)
		ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						errmsg("invalid fork number %d", forknum)));
	if (blocknum < 0 || blocknum > (int64)MaxBlockNumber)
		ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						errmsg("invalid block number " INT64_FORMAT, blocknum)));
	if (scan_lower > scan_upper)
		ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						errmsg("scan_lower must be <= scan_upper")));

	/* Resolve the relation's storage identity, then drop the relation. */
	rel = relation_open(relid, AccessShareLock);
	target = rel->rd_locator;
	relation_close(rel, AccessShareLock);

	res = cluster_block_recovery_reconstruct(target, (ForkNumber)forknum, (BlockNumber)blocknum,
											 scan_lower, scan_upper, pagebuf.data);

	if (res != CLUSTER_BLKREC_RECOVERED)
		PG_RETURN_NULL();

	result = (bytea *)palloc(BLCKSZ + VARHDRSZ);
	SET_VARSIZE(result, BLCKSZ + VARHDRSZ);
	memcpy(VARDATA(result), pagebuf.data, BLCKSZ);
	PG_RETURN_BYTEA_P(result);
}

#else /* !USE_PGRAC_CLUSTER */

Datum
cluster_block_recovery_reconstruct_test(PG_FUNCTION_ARGS pg_attribute_unused())
{
	ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					errmsg("cluster_block_recovery_reconstruct_test requires --enable-cluster")));
}

#endif /* USE_PGRAC_CLUSTER */
