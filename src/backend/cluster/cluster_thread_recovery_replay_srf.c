/*-------------------------------------------------------------------------
 *
 * cluster_thread_recovery_replay_srf.c
 *	  pgrac TEST-ONLY SQL entry point driving the online thread-recovery RMW
 *	  replay engine over real WAL + shared storage, for the spec-4.11 increment
 *	  3a differential (corruption-critical 8.A).
 *
 *	  cluster_thread_replay_test(scan_lower pg_lsn, scan_upper pg_lsn,
 *	                             seed_rel regclass, seed_forknum int4,
 *	                             seed_blocknum int8, seed_page bytea) -> text
 *
 *	  Optionally seeds one shared-storage block with a known (typically older)
 *	  page, then runs cluster_thread_recovery_replay_data() over [scan_lower,
 *	  scan_upper] -- the local-WAL convenience entry that builds a reader and
 *	  drives the source-agnostic core.  Returns a ':'-delimited summary the
 *	  cluster_tap (t/259) parses:
 *
 *	      <result>:<records_scanned>:<blocks_applied>:<blocks_gated>:
 *	      <blocks_out_of_scope>:<recovered_through>
 *
 *	  where <result> is done / blocked / not_applicable.
 *
 *	  The seed makes the engine's APPLY + write-back branch reachable on a single
 *	  machine: seed block 0 with the page at an intermediate LSN, replay the full
 *	  window, and the page on shared storage must come back byte-for-byte equal
 *	  to PG real redo (read by the cluster_tap via pg_read_binary_file).  Without
 *	  a seed the shared page is already current, so every record is LSN-gated
 *	  (idempotence: blocks_applied = 0, page unchanged).
 *
 *	  TEST-ONLY: a diagnostic entry point, NOT a product query interface;
 *	  superuser-only.  Mirrors cluster_thread_recovery_apply_srf.c.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * Spec: spec-4.11-thread-recovery.md (FROZEN v0.3)
 *
 * IDENTIFICATION
 *	  src/backend/cluster/cluster_thread_recovery_replay_srf.c
 *
 * NOTES
 *	  This is a pgrac-original file (no derivation from PostgreSQL).
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "fmgr.h"

PG_FUNCTION_INFO_V1(cluster_thread_replay_test);

#ifdef USE_PGRAC_CLUSTER

#include "access/relation.h"
#include "access/xlogdefs.h"
#include "miscadmin.h"
#include "storage/backendid.h"
#include "storage/bufpage.h"
#include "storage/relfilelocator.h"
#include "storage/smgr.h"
#include "utils/builtins.h"
#include "utils/pg_lsn.h"
#include "utils/rel.h"
#include "varatt.h"

#include "cluster/cluster_thread_recovery.h"

/*
 * seed_shared_block -- write a known page to one block of a shared-storage
 *		relation, so a single-machine replay can apply forward from a stale base.
 */
static void
seed_shared_block(Oid relid, ForkNumber forknum, BlockNumber blocknum, bytea *seed_page)
{
	Relation rel;
	RelFileLocator rlocator;
	SMgrRelation reln;

	if (VARSIZE_ANY_EXHDR(seed_page) != BLCKSZ)
		ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						errmsg("seed_page must be exactly %d bytes", BLCKSZ)));

	rel = relation_open(relid, AccessShareLock);
	rlocator = rel->rd_locator;
	relation_close(rel, AccessShareLock);

	reln = smgropen(rlocator, InvalidBackendId);
	if (!smgrexists(reln, forknum))
		ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
						errmsg("seed relation fork does not exist on shared storage")));

	smgrwrite(reln, forknum, blocknum, VARDATA_ANY(seed_page), false);
}

Datum
cluster_thread_replay_test(PG_FUNCTION_ARGS)
{
	XLogRecPtr scan_lower;
	XLogRecPtr scan_upper;
	ClusterThreadReplayStats stats;
	ClusterThreadRecResult res;
	const char *result_text;
	char *out;

	if (!superuser())
		ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
						errmsg("cluster_thread_replay_test is superuser-only")));

	/* Non-strict: the seed_* arguments are optional. */
	if (PG_ARGISNULL(0) || PG_ARGISNULL(1))
		ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						errmsg("scan_lower and scan_upper must not be NULL")));

	scan_lower = PG_GETARG_LSN(0);
	scan_upper = PG_GETARG_LSN(1);

	/* Optional seed: requires both the relation and the page. */
	if (!PG_ARGISNULL(2) && !PG_ARGISNULL(5)) {
		Oid relid = PG_GETARG_OID(2);
		int32 forknum = PG_ARGISNULL(3) ? MAIN_FORKNUM : PG_GETARG_INT32(3);
		int64 blocknum = PG_ARGISNULL(4) ? 0 : PG_GETARG_INT64(4);

		if (forknum < 0 || forknum > MAX_FORKNUM)
			ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
							errmsg("invalid fork number %d", forknum)));
		if (blocknum < 0 || blocknum > (int64)MaxBlockNumber)
			ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
							errmsg("invalid block number " INT64_FORMAT, blocknum)));

		seed_shared_block(relid, (ForkNumber)forknum, (BlockNumber)blocknum, PG_GETARG_BYTEA_PP(5));
	}

	res = cluster_thread_recovery_replay_data(scan_lower, scan_upper, &stats);

	switch (res) {
	case CLUSTER_THREADREC_DONE:
		result_text = "done";
		break;
	case CLUSTER_THREADREC_BLOCKED:
		result_text = "blocked";
		break;
	default:
		result_text = "not_applicable";
		break;
	}

	out = psprintf("%s:" UINT64_FORMAT ":" UINT64_FORMAT ":" UINT64_FORMAT ":" UINT64_FORMAT
				   ":%X/%X",
				   result_text, stats.records_scanned, stats.blocks_applied, stats.blocks_gated,
				   stats.blocks_out_of_scope, LSN_FORMAT_ARGS(stats.recovered_through));

	PG_RETURN_TEXT_P(cstring_to_text(out));
}

#else /* !USE_PGRAC_CLUSTER */

Datum
cluster_thread_replay_test(PG_FUNCTION_ARGS pg_attribute_unused())
{
	ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					errmsg("cluster_thread_replay_test requires --enable-cluster")));
}

#endif /* USE_PGRAC_CLUSTER */
