/*-------------------------------------------------------------------------
 *
 * cluster_pi_recovery_srf.c
 *	  pgrac TEST-ONLY SQL entry point driving the spec-6.12h D-h3b Past
 *	  Image recovery rebuild over a dead thread's per-thread WAL, for the
 *	  byte-for-byte differential (L-h).
 *
 *	  cluster_pi_apply_redo_test(rel regclass, forknum int4, blocknum int8,
 *	                             thread_id int4, start_lsn pg_lsn,
 *	                             end_lsn pg_lsn, use_pi bool) -> text
 *
 *	  Rebuilds one block by replaying every record in [start_lsn, end_lsn]
 *	  of <cluster.wal_threads_dir>/thread_<thread_id> that references
 *	  (rel, forknum, blocknum):
 *
 *	    use_pi = true : base = the local Past Image's frozen bytes
 *	      (cluster_bufmgr_snapshot_pi_block), each record gated by the
 *	      D-h3a ship-SCN recovery boundary
 *	      (cluster_pi_thread_apply_record_to_page): lineage records are
 *	      skipped, post-ship records applied, an unprovable operand
 *	      abandons the rebuild (blocked).
 *
 *	    use_pi = false: base = a zero page; the first matching record must
 *	      be an apply-able FPI (else 'no-base', fail-closed) and every
 *	      record goes through the spec-4.11 LSN-gated wrapper
 *	      (cluster_thread_apply_record_to_page) -- the storage-path parity
 *	      reference, over the SAME thread window.
 *
 *	  Returns 'status:ship_scn:applied:skipped:md5' where status is one of
 *	  ok / no-pi / no-base / blocked; md5 is the hex digest of the final
 *	  page (empty unless ok).  The cluster_tap differential compares the
 *	  use_pi=true digest against the peer's checkpointed shared-storage
 *	  bytes (the ground truth), and uses the use_pi=false leg to show the
 *	  single-thread storage window alone cannot rebuild a block whose
 *	  history spans threads (the PI value claim).
 *
 *	  A NULL record before end_lsn is treated as end-of-stream (the
 *	  per-thread tail may pad past the last record); a short source shows
 *	  up as a digest mismatch in the differential, never as a false 'ok'
 *	  claim beyond the bytes actually replayed.
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
 * Spec: spec-6.12-crossnode-cache-fusion-perf-optimization.md
 *
 * IDENTIFICATION
 *	  src/backend/cluster/cluster_pi_recovery_srf.c
 *
 * NOTES
 *	  This is a pgrac-original file (no derivation from PostgreSQL).
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "fmgr.h"

PG_FUNCTION_INFO_V1(cluster_pi_apply_redo_test);

#ifdef USE_PGRAC_CLUSTER

#include "access/relation.h"
#include "access/xlog.h"
#include "access/xlogreader.h"
#include "access/xlogrecord.h"
#include "access/xlogutils.h"
#include "common/md5.h"
#include "miscadmin.h"
#include "storage/buf_internals.h"
#include "storage/bufpage.h"
#include "storage/relfilelocator.h"
#include "utils/builtins.h"
#include "utils/pg_lsn.h"
#include "utils/rel.h"

#include "cluster/cluster_gcs_block.h"
#include "cluster/cluster_pi_shadow.h"
#include "cluster/cluster_thread_recovery.h"
#include "cluster/cluster_thread_recovery_apply.h"

Datum
cluster_pi_apply_redo_test(PG_FUNCTION_ARGS)
{
	Oid relid = PG_GETARG_OID(0);
	int32 forknum = PG_GETARG_INT32(1);
	int64 blocknum = PG_GETARG_INT64(2);
	int32 thread_id = PG_GETARG_INT32(3);
	XLogRecPtr start_lsn = PG_GETARG_LSN(4);
	XLogRecPtr end_lsn = PG_GETARG_LSN(5);
	bool use_pi = PG_GETARG_BOOL(6);
	Relation rel;
	RelFileLocator target;
	BufferTag tag;
	XLogReaderState *xlogreader;
	void *reader_priv;
	XLogRecPtr first_valid;
	PGAlignedBlock pagebuf;
	char *page = pagebuf.data;
	SCN ship_scn = InvalidScn;
	bool have_base;
	bool failed = false;
	uint64 applied = 0;
	uint64 skipped = 0;
	const char *status;
	char md5hex[MD5_DIGEST_LENGTH * 2 + 1];
	const char *md5_errstr;

	if (!superuser())
		ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
						errmsg("cluster_pi_apply_redo_test is superuser-only")));

	if (forknum < 0 || forknum > MAX_FORKNUM)
		ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						errmsg("invalid fork number %d", forknum)));
	if (blocknum < 0 || blocknum > (int64)MaxBlockNumber)
		ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						errmsg("invalid block number " INT64_FORMAT, blocknum)));
	if (start_lsn > end_lsn)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("start_lsn must be <= end_lsn")));

	/* Resolve the relation's storage identity, then drop the relation. */
	rel = relation_open(relid, AccessShareLock);
	target = rel->rd_locator;
	relation_close(rel, AccessShareLock);

	if (use_pi) {
		InitBufferTag(&tag, &target, (ForkNumber)forknum, (BlockNumber)blocknum);
		if (!cluster_bufmgr_snapshot_pi_block(tag, page, &ship_scn)) {
			/* No stamped, intact Past Image resident for this block: the
			 * PI path is simply unavailable (fail-safe, not an error). */
			PG_RETURN_TEXT_P(cstring_to_text("no-pi:0:0:0:"));
		}
		have_base = true; /* the PI IS the base */
	} else {
		memset(page, 0, BLCKSZ);
		have_base = false; /* an apply-able FPI must establish it */
	}

	/*
	 * WAL reader over the dead thread's per-thread dir -- the same source
	 * the spec-4.11 data driver reads.  Fail closed loudly: the TAP always
	 * points at a configured, flushed thread window.
	 */
	xlogreader = cluster_thread_wal_reader_make((uint16)thread_id, &reader_priv);
	if (xlogreader == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("could not open per-thread WAL for thread %d", thread_id),
				 errhint("cluster.wal_threads_dir must be set and contain thread_%d.", thread_id)));

	first_valid = XLogFindNextRecord(xlogreader, start_lsn);
	if (XLogRecPtrIsInvalid(first_valid)) {
		cluster_thread_wal_reader_free(xlogreader, reader_priv);
		ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						errmsg("could not find a valid WAL record at or after %X/%X in thread %d",
							   LSN_FORMAT_ARGS(start_lsn), thread_id),
						errhint("start_lsn must lie inside the thread's flushed WAL.")));
	}

	for (;;) {
		char *errormsg;
		const XLogRecord *record;
		int max_id;
		int block_id;

		record = XLogReadRecord(xlogreader, &errormsg);
		if (record == NULL)
			break; /* end of the flushed per-thread stream (see header) */

		/* Stop before applying any record that ENDS past the window (a record
		 * straddling end_lsn must not be applied -- mirrors the apply SRF). */
		if (xlogreader->EndRecPtr > end_lsn)
			break;

		max_id = XLogRecMaxBlockId(xlogreader);
		for (block_id = 0; block_id <= max_id; block_id++) {
			RelFileLocator rl;
			ForkNumber f;
			BlockNumber b;
			XLogRecPtr applied_lsn;
			ClusterThreadApplyResult res;

			if (!XLogRecGetBlockTagExtended(xlogreader, (uint8)block_id, &rl, &f, &b, NULL))
				continue;
			if (!RelFileLocatorEquals(rl, target) || f != (ForkNumber)forknum
				|| b != (BlockNumber)blocknum)
				continue;

			if (use_pi) {
				/* Ship-SCN boundary gate over the PI base (D-h3b). */
				res = cluster_pi_thread_apply_record_to_page(xlogreader, (uint8)block_id, page,
															 ship_scn, &applied_lsn);
			} else {
				bool is_fpi = XLogRecHasBlockImage(xlogreader, (uint8)block_id)
							  && XLogRecBlockImageApply(xlogreader, (uint8)block_id);

				/* A delta cannot be applied before a base is established
				 * (fail-closed, 8.A) -- mirror of the spec-4.11 apply SRF. */
				if (!have_base && !is_fpi) {
					failed = true;
					break;
				}
				res = cluster_thread_apply_record_to_page(xlogreader, (uint8)block_id, page,
														  &applied_lsn);
			}

			if (res == CLUSTER_THREADAPPLY_APPLIED) {
				have_base = true;
				applied++;
			} else if (res == CLUSTER_THREADAPPLY_DONE) {
				have_base = true;
				skipped++;
			} else {
				/* BLOCKED fail-closed (NOOP impossible: the tag matched). */
				failed = true;
				break;
			}
		}

		if (failed)
			break;
	}

	cluster_thread_wal_reader_free(xlogreader, reader_priv);

	if (failed)
		status = (!use_pi && !have_base) ? "no-base" : "blocked";
	else if (!have_base)
		status = "no-base"; /* zero-base window held no FPI at all */
	else
		status = "ok";

	md5hex[0] = '\0';
	if (strcmp(status, "ok") == 0 && !pg_md5_hash(page, BLCKSZ, md5hex, &md5_errstr))
		ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR),
						errmsg("could not compute page digest: %s", md5_errstr)));

	PG_RETURN_TEXT_P(
		cstring_to_text(psprintf("%s:" UINT64_FORMAT ":" UINT64_FORMAT ":" UINT64_FORMAT ":%s",
								 status, (uint64)ship_scn, applied, skipped, md5hex)));
}

#else /* !USE_PGRAC_CLUSTER */

Datum
cluster_pi_apply_redo_test(PG_FUNCTION_ARGS pg_attribute_unused())
{
	ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					errmsg("cluster_pi_apply_redo_test requires --enable-cluster")));
}

#endif /* USE_PGRAC_CLUSTER */
