/*-------------------------------------------------------------------------
 *
 * cluster_thread_recovery_apply_srf.c
 *	  pgrac TEST-ONLY SQL entry point driving the online thread-recovery page
 *	  apply matrix over real WAL, for the spec-4.11 byte-for-byte differential.
 *
 *	  cluster_thread_apply_redo_test(rel regclass, forknum int4, blocknum int8,
 *	                                 start_lsn pg_lsn, end_lsn pg_lsn,
 *	                                 base_page bytea) -> bytea
 *
 *	  Reconstructs one block by replaying, onto a single page, every WAL record
 *	  in [start_lsn, end_lsn] that references (rel, forknum, blocknum), via
 *	  cluster_thread_apply_record_to_page() -- the LSN-gated apply-through
 *	  wrapper.  Returns the page as bytea, or NULL when apply fails closed (a
 *	  delta with no base, or a record type not on the apply matrix -> BLOCKED).
 *
 *	  base_page selects the differential mode:
 *	    - NULL: start from a zero page.  The first matching record MUST be an
 *	      apply-able FPI (it establishes the base).  This proves PARITY: a
 *	      single apply-through built from the FPI + deltas is byte-for-byte
 *	      identical to PG's real crash-recovery redo of the same window.
 *	    - a BLCKSZ page (the real-redo reference page, already at the window's
 *	      end LSN): start from it, with a base already established, and replay
 *	      a DELTA-ONLY window (no FPI to reset the page).  This proves
 *	      IDEMPOTENCE (spec-4.11 v0.3 partial-apply, 8.A): the redo LSN-gate
 *	      skips a record the page already reflects, leaving it unchanged.
 *	      WITHOUT the gate a re-applied delta corrupts the page (e.g. a heap
 *	      INSERT delta -> PageAddItem at an occupied offset -> FAILED -> NULL),
 *	      so this leg distinguishes a correct gate from none -- an FPI-bearing
 *	      window cannot, because the FPI would reset the page each replay.
 *
 *	  The cluster_tap differential (t/258) compares this page byte-for-byte
 *	  against the same block as produced by PG's real redo, which is the 8.A
 *	  correctness guarantee for the online thread-recovery apply path.
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
 * Spec: spec-4.11-thread-recovery.md (FROZEN v0.3)
 *
 * IDENTIFICATION
 *	  src/backend/cluster/cluster_thread_recovery_apply_srf.c
 *
 * NOTES
 *	  This is a pgrac-original file (no derivation from PostgreSQL).
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "fmgr.h"

PG_FUNCTION_INFO_V1(cluster_thread_apply_redo_test);

#ifdef USE_PGRAC_CLUSTER

#include "access/relation.h"
#include "access/xlog.h"
#include "access/xlogreader.h"
#include "access/xlogrecord.h"
#include "access/xlogutils.h"
#include "miscadmin.h"
#include "storage/bufpage.h"
#include "storage/relfilelocator.h"
#include "utils/pg_lsn.h"
#include "utils/rel.h"
#include "varatt.h"

#include "cluster/cluster_thread_recovery_apply.h"

Datum
cluster_thread_apply_redo_test(PG_FUNCTION_ARGS)
{
	Oid relid;
	int32 forknum;
	int64 blocknum;
	XLogRecPtr start_lsn;
	XLogRecPtr end_lsn;
	RelFileLocator target;
	Relation rel;
	XLogReaderState *xlogreader;
	ReadLocalXLogPageNoWaitPrivate *private_data;
	XLogRecPtr first_valid;
	PGAlignedBlock pagebuf;
	char *page = pagebuf.data;
	bool have_base;
	bool failed = false;
	bytea *result;

	if (!superuser())
		ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
						errmsg("cluster_thread_apply_redo_test is superuser-only")));

	/*
	 * Non-strict (base_page may be NULL); the other arguments are required.
	 */
	if (PG_ARGISNULL(0) || PG_ARGISNULL(1) || PG_ARGISNULL(2) || PG_ARGISNULL(3) || PG_ARGISNULL(4))
		ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						errmsg("rel, forknum, blocknum, start_lsn and end_lsn must not be NULL")));

	relid = PG_GETARG_OID(0);
	forknum = PG_GETARG_INT32(1);
	blocknum = PG_GETARG_INT64(2);
	start_lsn = PG_GETARG_LSN(3);
	end_lsn = PG_GETARG_LSN(4);

	if (forknum < 0 || forknum > MAX_FORKNUM)
		ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						errmsg("invalid fork number %d", forknum)));
	if (blocknum < 0 || blocknum > (int64)MaxBlockNumber)
		ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						errmsg("invalid block number " INT64_FORMAT, blocknum)));
	if (start_lsn > end_lsn)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("start_lsn must be <= end_lsn")));

	/*
	 * base_page: NULL -> build from a zero page (FPI-first, parity mode); a
	 * BLCKSZ page -> seed the page with a base already established (delta-only
	 * idempotence mode).
	 */
	if (PG_ARGISNULL(5)) {
		memset(page, 0, BLCKSZ);
		have_base = false;
	} else {
		bytea *base = PG_GETARG_BYTEA_PP(5);

		if (VARSIZE_ANY_EXHDR(base) != BLCKSZ)
			ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
							errmsg("base_page must be exactly %d bytes", BLCKSZ)));
		memcpy(page, VARDATA_ANY(base), BLCKSZ);
		have_base = true;
	}

	/* Resolve the relation's storage identity, then drop the relation. */
	rel = relation_open(relid, AccessShareLock);
	target = rel->rd_locator;
	relation_close(rel, AccessShareLock);

	/* WAL reader over the local stream (flat pg_wal); pg_walinspect idiom. */
	private_data
		= (ReadLocalXLogPageNoWaitPrivate *)palloc0(sizeof(ReadLocalXLogPageNoWaitPrivate));
	xlogreader = XLogReaderAllocate(wal_segment_size, NULL,
									XL_ROUTINE(.page_read = &read_local_xlog_page_no_wait,
											   .segment_open = &wal_segment_open,
											   .segment_close = &wal_segment_close),
									private_data);
	if (xlogreader == NULL)
		ereport(ERROR, (errcode(ERRCODE_OUT_OF_MEMORY), errmsg("out of memory"),
						errdetail("Failed while allocating a WAL reading processor.")));

	first_valid = XLogFindNextRecord(xlogreader, start_lsn);
	if (XLogRecPtrIsInvalid(first_valid))
		ereport(ERROR, (errmsg("could not find a valid WAL record at or after %X/%X",
							   LSN_FORMAT_ARGS(start_lsn))));

	for (;;) {
		char *errormsg;
		const XLogRecord *record;
		int max_id;
		int block_id;

		record = XLogReadRecord(xlogreader, &errormsg);
		if (record == NULL) {
			if (private_data->end_of_wal)
				break;
			if (errormsg)
				ereport(ERROR, (errcode_for_file_access(),
								errmsg("could not read WAL at %X/%X: %s",
									   LSN_FORMAT_ARGS(xlogreader->EndRecPtr), errormsg)));
			else
				ereport(ERROR, (errcode_for_file_access(),
								errmsg("could not read WAL at %X/%X",
									   LSN_FORMAT_ARGS(xlogreader->EndRecPtr))));
		}

		/* Stop before applying any record that ENDS past the window (a record
		 * straddling end_lsn must not be applied -- mirrors reconstruct). */
		if (xlogreader->EndRecPtr > end_lsn)
			break;

		max_id = XLogRecMaxBlockId(xlogreader);
		for (block_id = 0; block_id <= max_id; block_id++) {
			RelFileLocator rl;
			ForkNumber f;
			BlockNumber b;
			bool is_fpi;
			XLogRecPtr applied_lsn;
			ClusterThreadApplyResult res;

			if (!XLogRecGetBlockTagExtended(xlogreader, (uint8)block_id, &rl, &f, &b, NULL))
				continue;
			if (!RelFileLocatorEquals(rl, target) || f != (ForkNumber)forknum
				|| b != (BlockNumber)blocknum)
				continue;

			/*
			 * A delta cannot be applied before a base is established (it would
			 * feed cluster_block_apply_one a bare zero page).  Only an
			 * apply-able FPI establishes the base -- fail closed otherwise
			 * (8.A).  A seeded base_page already has one.
			 */
			is_fpi = XLogRecHasBlockImage(xlogreader, (uint8)block_id)
					 && XLogRecBlockImageApply(xlogreader, (uint8)block_id);
			if (!have_base && !is_fpi) {
				failed = true;
				break;
			}

			res = cluster_thread_apply_record_to_page(xlogreader, (uint8)block_id, page,
													  &applied_lsn);
			if (res == CLUSTER_THREADAPPLY_APPLIED || res == CLUSTER_THREADAPPLY_DONE)
				have_base = true;
			else {
				/* BLOCKED fail-closed (NOOP impossible: tag matched). */
				failed = true;
				break;
			}
		}

		if (failed)
			break;
	}

	XLogReaderFree(xlogreader);
	pfree(private_data);

	if (failed || !have_base)
		PG_RETURN_NULL();

	result = (bytea *)palloc(BLCKSZ + VARHDRSZ);
	SET_VARSIZE(result, BLCKSZ + VARHDRSZ);
	memcpy(VARDATA(result), page, BLCKSZ);
	PG_RETURN_BYTEA_P(result);
}

#else /* !USE_PGRAC_CLUSTER */

Datum
cluster_thread_apply_redo_test(PG_FUNCTION_ARGS pg_attribute_unused())
{
	ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					errmsg("cluster_thread_apply_redo_test requires --enable-cluster")));
}

#endif /* USE_PGRAC_CLUSTER */
