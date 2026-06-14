/*-------------------------------------------------------------------------
 *
 * cluster_thread_recovery_replay.c
 *	  pgrac online thread-recovery RMW replay engine (spec-4.11 D1, increment 3a).
 *
 *	  A survivor online-replays a dead WAL thread's data to shared storage within
 *	  the reconfig freeze window.  This engine streams the dead thread's WAL and,
 *	  for every block reference to a genuinely shared user-relation page, does a
 *	  read-modify-write directly against shared storage -- bypassing the buffer
 *	  pool.  Bypassing is safe because the dead thread's pages are fenced for the
 *	  freeze window: there is no concurrent access (the coherence precondition).
 *
 *	  This is crash-recovery redo, scoped to ONE thread's WAL, applied to shared
 *	  storage, with PG redo's global side effects stripped (the D0 STOP GATE A
 *	  reason ApplyWalRecord is not online-safe for a foreign segment's records:
 *	  it would advance the live survivor's global nextXid with a foreign xid).
 *	  The base for each apply is the LIVE shared page (not a clean FPI-base
 *	  reconstruction like spec-4.10); the record-end-vs-page LSN gate inside
 *	  cluster_thread_apply_record_to_page() makes the stream idempotent, exactly
 *	  as PG redo gates already-applied records on the on-disk page LSN.
 *
 *	  Corruption-critical contract (8.A).  Three gates before any write:
 *	    1. routing -- only cluster_smgr_which_for()==1 (genuinely shared user
 *	       relation) pages are touched; everything else (temp / catalog / opt-in
 *	       off) is a per-node concern the survivor owns -> data-pass skip.
 *	    2. existence (amend 1) -- a relation whose file is gone fails CLOSED, not
 *	       a BLK_NOTFOUND-style skip: 3a runs only the data-page apply matrix and
 *	       never the storage create/drop/truncate rmgr, so it cannot prove a
 *	       missing file is a legitimate drop.
 *	    3. range -- a block at/beyond EOF (relation extension / new init page)
 *	       fails CLOSED and forwards (Stage 5); the engine never reads past EOF.
 *	  Plus: any record the apply matrix cannot handle byte-for-byte (off-matrix
 *	  rmgr / unusable image) -> BLOCKED; a read error in-window -> BLOCKED; and
 *	  reaching clean end-of-WAL short of the validated scan_upper -> BLOCKED (the
 *	  WAL is incomplete; scan_upper is a durable boundary by precondition).
 *
 *	  SCOPE (increment 3a).  This engine ONLY writes shared pages.  It does NOT
 *	  publish authority, start a worker, unfreeze, or flush WAL.  smgrwrite is a
 *	  WRITE-BACK, not a durable write (cluster_fs write is a bare pwrite with no
 *	  inline fsync, amend 2); 3b must issue a durability barrier on the touched
 *	  relations BEFORE publishing any 3-way authority.  A crash before that
 *	  barrier simply re-replays from a validated lower bound (redo-idempotent via
 *	  the LSN-gate), since no authority was ever published.
 *
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/backend/cluster/cluster_thread_recovery_replay.c
 *
 * NOTES
 *	  This is a pgrac-original file (no derivation from PostgreSQL).
 *	  Spec: spec-4.11-thread-recovery.md (FROZEN v0.3)
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#ifdef USE_PGRAC_CLUSTER

#include "access/xlog.h"
#include "access/xlogreader.h"
#include "access/xlogrecord.h"
#include "access/xlogutils.h"
#include "storage/backendid.h"
#include "storage/bufpage.h"
#include "storage/relfilelocator.h"
#include "storage/smgr.h"

#include "cluster/cluster_thread_recovery.h"
#include "cluster/cluster_thread_recovery_apply.h"
#include "cluster/storage/cluster_smgr.h"

/*
 * replay_one_block -- classify and (if TARGET) read-modify-write one block
 *		reference of one record against shared storage.
 *
 *	Returns true on a clean outcome (TARGET applied/gated, or OUT_OF_SCOPE
 *	skipped) and false on a fail-closed BLOCKED outcome.  On true, *st is
 *	advanced; on false the caller stops the whole replay (8.A).  The page buffer
 *	is the caller's scratch (one read-modify-write at a time, no buffer pool).
 */
static bool
replay_one_block(XLogReaderState *reader, uint8 block_id, char *page, ClusterThreadReplayStats *st)
{
	RelFileLocator rl;
	ForkNumber forknum;
	BlockNumber blocknum;
	SMgrRelation reln;
	bool rel_exists;
	BlockNumber nblocks;
	ClusterThreadReplayBlockClass cls;
	XLogRecPtr applied_lsn;
	ClusterThreadApplyResult res;

	if (!XLogRecGetBlockTagExtended(reader, block_id, &rl, &forknum, &blocknum, NULL))
		return true; /* this block id carries no reference: nothing to do */

	/* Gate 1: only genuinely shared user-relation pages are this engine's job. */
	if (cluster_smgr_which_for(rl, InvalidBackendId) != 1) {
		st->blocks_out_of_scope++;
		return true; /* per-node (temp / catalog): data-pass skip */
	}

	/*
	 * Gates 2 (existence) and 3 (range) are decided by the pure classifier so
	 * the corruption-critical branches are unit-pinned; here we only compute its
	 * smgr inputs.  smgrnblocks is read only once existence is established.
	 */
	reln = smgropen(rl, InvalidBackendId);
	rel_exists = smgrexists(reln, forknum);
	nblocks = rel_exists ? smgrnblocks(reln, forknum) : 0;
	cls = cluster_thread_replay_classify_block(1, rel_exists, blocknum, nblocks);
	if (cls == CLUSTER_THREADREPLAY_BLK_BLOCKED) {
		ereport(DEBUG2,
				(errmsg_internal(
					"thread recovery replay fail-closed: rel %u/%u/%u fork %d block %u %s",
					rl.spcOid, rl.dbOid, rl.relNumber, forknum, blocknum,
					rel_exists ? "beyond EOF (extension/new page)" : "relation does not exist")));
		return false;
	}
	/* cls == TARGET (OUT_OF_SCOPE was handled by gate 1 above). */

	/*
	 * Read the LIVE shared page and apply the record onto it.  The LSN-gate
	 * inside cluster_thread_apply_record_to_page leaves an already-reflected
	 * page untouched (DONE), which is what makes a retry / cold redo idempotent.
	 */
	smgrread(reln, forknum, blocknum, page);
	res = cluster_thread_apply_record_to_page(reader, block_id, page, &applied_lsn);

	switch (res) {
	case CLUSTER_THREADAPPLY_APPLIED:

		/*
		 * Write-back (NOT durable, amend 2): stamp the write-time checksum (a
		 * no-op when checksums are off) and pwrite the page to shared storage.
		 * 3b fsyncs before publishing authority.
		 */
		PageSetChecksumInplace((Page)page, blocknum);
		smgrwrite(reln, forknum, blocknum, page, false);
		st->blocks_applied++;
		return true;

	case CLUSTER_THREADAPPLY_DONE:
		st->blocks_gated++; /* LSN-gate idempotent skip */
		return true;

	case CLUSTER_THREADAPPLY_NOOP:

		/*
		 * The tag matched above, so the wrapper cannot report NOOP (it only does
		 * for a missing block ref).  Treat defensively as a clean no-write.
		 */
		return true;

	case CLUSTER_THREADAPPLY_BLOCKED:
	default:
		ereport(DEBUG2, (errmsg_internal("thread recovery replay fail-closed: rel %u/%u/%u fork %d "
										 "block %u off-matrix or unusable record",
										 rl.spcOid, rl.dbOid, rl.relNumber, forknum, blocknum)));
		return false;
	}
}

/*
 * cluster_thread_recovery_replay_stream -- source-agnostic RMW replay core.
 *
 *	Replays a positioned WAL reader up to scan_upper onto shared storage.  See
 *	the header for the precondition (scan_upper is a validated-durable boundary)
 *	and the DONE / BLOCKED contract.
 *
 * Author: SqlRush <sqlrush@gmail.com>
 */
ClusterThreadRecResult
cluster_thread_recovery_replay_stream(XLogReaderState *reader, XLogRecPtr scan_upper,
									  ClusterThreadReplayStats *stats)
{
	ClusterThreadReplayStats st;
	PGAlignedBlock pagebuf;
	bool aborted = false;
	bool saw_straddle = false;
	bool reached;

	memset(&st, 0, sizeof(st));

	if (reader == NULL || XLogRecPtrIsInvalid(scan_upper)) {
		if (stats)
			*stats = st;
		return CLUSTER_THREADREC_BLOCKED; /* invalid input -> fail-closed */
	}

	for (;;) {
		char *errormsg;
		const XLogRecord *record;
		int max_id;
		int block_id;
		bool record_blocked = false;

		record = XLogReadRecord(reader, &errormsg);
		if (record == NULL) {
			/*
			 * A clean end of stream is fine; an in-window read error (a torn or
			 * unreadable record) fails closed.  Either way the reached-check
			 * below catches a window that ended short of scan_upper.
			 */
			if (errormsg != NULL && errormsg[0] != '\0')
				aborted = true;
			break;
		}

		/*
		 * Stop before applying any record that ENDS past scan_upper: the
		 * recovered version must not overshoot the validated boundary (mirror
		 * spec-4.10 reconstruct).  Seeing such a record also proves the WAL up
		 * to scan_upper was fully present -> the window was reached.
		 */
		if (reader->EndRecPtr > scan_upper) {
			saw_straddle = true;
			break;
		}

		st.records_scanned++;

		max_id = XLogRecMaxBlockId(reader);
		for (block_id = 0; block_id <= max_id; block_id++) {
			if (!replay_one_block(reader, (uint8)block_id, pagebuf.data, &st)) {
				record_blocked = true;
				break;
			}
		}

		if (record_blocked) {
			aborted = true;
			break;
		}

		/*
		 * Advance recovered_through ONLY after every block reference of this
		 * record was handled cleanly, so a fail-closed never claims an
		 * unfinished record (8.A).
		 */
		st.recovered_through = reader->EndRecPtr;
	}

	if (stats)
		*stats = st;

	/*
	 * Completeness (8.A): success requires reaching the validated boundary --
	 * either we read a record past scan_upper (so everything <= scan_upper was
	 * present) or the last processed record ended exactly at scan_upper.
	 * Reaching clean end-of-WAL short of scan_upper means the WAL is incomplete.
	 */
	reached = saw_straddle || (st.recovered_through == scan_upper);
	if (aborted || !reached)
		return CLUSTER_THREADREC_BLOCKED;
	return CLUSTER_THREADREC_DONE;
}

/*
 * cluster_thread_recovery_replay_data -- 3a-local / test convenience entry.
 *
 *	Builds a local-WAL reader over [scan_lower, scan_upper], positions it, and
 *	drives the source-agnostic core.  LOCAL source only (the single-machine test
 *	simulates a foreign thread with local WAL); 3b adds the foreign-source entry
 *	calling the same core.  See the header.
 *
 * Author: SqlRush <sqlrush@gmail.com>
 */
ClusterThreadRecResult
cluster_thread_recovery_replay_data(XLogRecPtr scan_lower, XLogRecPtr scan_upper,
									ClusterThreadReplayStats *stats)
{
	XLogReaderState *reader;
	ReadLocalXLogPageNoWaitPrivate *private_data;
	XLogRecPtr first_valid;
	ClusterThreadRecResult res;

	if (stats)
		memset(stats, 0, sizeof(*stats));

	if (XLogRecPtrIsInvalid(scan_lower) || XLogRecPtrIsInvalid(scan_upper)
		|| scan_lower > scan_upper)
		return CLUSTER_THREADREC_BLOCKED;

	private_data
		= (ReadLocalXLogPageNoWaitPrivate *)palloc0(sizeof(ReadLocalXLogPageNoWaitPrivate));
	reader = XLogReaderAllocate(wal_segment_size, NULL,
								XL_ROUTINE(.page_read = &read_local_xlog_page_no_wait,
										   .segment_open = &wal_segment_open,
										   .segment_close = &wal_segment_close),
								private_data);
	if (reader == NULL) {
		pfree(private_data);
		return CLUSTER_THREADREC_BLOCKED;
	}

	first_valid = XLogFindNextRecord(reader, scan_lower);
	if (XLogRecPtrIsInvalid(first_valid)) {
		XLogReaderFree(reader);
		pfree(private_data);
		return CLUSTER_THREADREC_BLOCKED; /* cannot position -> fail-closed */
	}

	res = cluster_thread_recovery_replay_stream(reader, scan_upper, stats);

	XLogReaderFree(reader);
	pfree(private_data);
	return res;
}

#else /* !USE_PGRAC_CLUSTER */

/* Disable-cluster build: this file compiles to nothing. */

#endif /* USE_PGRAC_CLUSTER */
