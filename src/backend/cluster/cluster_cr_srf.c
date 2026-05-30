/*-------------------------------------------------------------------------
 *
 * cluster_cr_srf.c
 *	  pgrac spec-3.9 D10 — TEST-ONLY SQL entry point to drive own-instance
 *	  CR block construction deterministically from cluster_tap t/215.
 *
 *	  cluster_cr_test_construct(rel regclass, blockno int4, itl_idx int4,
 *	                            read_scn int8) -> bool
 *
 *	  Reads block `blockno` of `rel` under a SHARE content lock and calls
 *	  cluster_cr_construct_block(buf, read_scn, itl_idx).  Returns true on a
 *	  successful construction; raises the CR taxonomy SQLSTATE (53R9F /
 *	  53R9G / data_corrupted) when a cr_* injection point is armed or the
 *	  undo chain is genuinely unreconstructable.  Superuser-only.
 *
 *	  This bypasses the (default-off, codereview-pending) MVCC gate so the
 *	  CR read path is exercisable without depending on the gate's firing
 *	  condition.  Mirrors spec-3.8's cluster_undo_test_force_segment_end.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * Spec: spec-3.9-own-instance-cr-block-construction.md (FROZEN v0.4)
 *
 * IDENTIFICATION
 *	  src/backend/cluster/cluster_cr_srf.c
 *
 * NOTES
 *	  This is a pgrac-original file (no derivation from PostgreSQL).
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "fmgr.h"

PG_FUNCTION_INFO_V1(cluster_cr_test_construct);

#ifdef USE_PGRAC_CLUSTER

#include "access/relation.h"
#include "miscadmin.h"
#include "storage/bufmgr.h"
#include "utils/rel.h"
#include "utils/relcache.h"

#include "cluster/cluster_cr.h"
#include "cluster/cluster_scn.h"

Datum
cluster_cr_test_construct(PG_FUNCTION_ARGS)
{
	Oid relid;
	int32 blockno;
	int32 itl_idx;
	int64 read_scn_in;
	Relation rel;
	Buffer buf;
	const char *cr_page;
	SCN read_scn;

	if (!superuser())
		ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
						errmsg("cluster_cr_test_construct is superuser-only")));

	relid = PG_GETARG_OID(0);
	blockno = PG_GETARG_INT32(1);
	itl_idx = PG_GETARG_INT32(2);
	read_scn_in = PG_GETARG_INT64(3);
	read_scn = (SCN)read_scn_in;

	rel = relation_open(relid, AccessShareLock);

	buf = ReadBuffer(rel, (BlockNumber)blockno);
	LockBuffer(buf, BUFFER_LOCK_SHARE);

	PG_TRY();
	{
		/* construct_block ereports the CR taxonomy SQLSTATE on failure /
		 * armed injection; on success returns the scratch image pointer. */
		cr_page = cluster_cr_construct_block(buf, read_scn, itl_idx);
	}
	PG_CATCH();
	{
		LockBuffer(buf, BUFFER_LOCK_UNLOCK);
		ReleaseBuffer(buf);
		relation_close(rel, AccessShareLock);
		PG_RE_THROW();
	}
	PG_END_TRY();

	LockBuffer(buf, BUFFER_LOCK_UNLOCK);
	ReleaseBuffer(buf);
	relation_close(rel, AccessShareLock);

	PG_RETURN_BOOL(cr_page != NULL);
}

#else /* !USE_PGRAC_CLUSTER */

Datum
cluster_cr_test_construct(PG_FUNCTION_ARGS pg_attribute_unused())
{
	ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					errmsg("cluster_cr_test_construct requires --enable-cluster")));
	PG_RETURN_NULL();
}

#endif /* USE_PGRAC_CLUSTER */
