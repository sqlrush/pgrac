/*-------------------------------------------------------------------------
 *
 * cluster_thread_recovery_driver.c
 *	  pgrac online thread-recovery DATA DRIVER (spec-4.11 D1, increment 3b-1).
 *
 *	  Increment 3a built the source-agnostic RMW engine
 *	  (cluster_thread_recovery_replay_stream): given a positioned WAL reader and
 *	  a validated boundary, it read-modify-writes a dead thread's data pages onto
 *	  shared storage, byte-for-byte equal to PG real redo.  This driver is the
 *	  layer that turns a DEAD THREAD ID into a driven replay: it builds a reader
 *	  over that thread's per-thread WAL (<cluster.wal_threads_dir>/thread_<tid>)
 *	  and drives the engine under an R13 error-demotion harness.
 *
 *	  SCOPE (increment 3b-1).  DATA ONLY.  This driver:
 *	    - publishes NO authority (registry / node-local / target);
 *	    - does NO visibility pass (foreign XACT/CLOG -> remote store), so a
 *	      survivor cannot yet judge the dead thread's commit state;
 *	    - issues NO durability barrier (the engine's writes are write-back, not
 *	      durable -- amend 2);
 *	    - has NO live caller (amend 2): only the TEST SRF (oid 8936) and the
 *	      future replay_one orchestrator (3b-2) call it; it is NOT wired to the
 *	      GRD reconfig FSM and starts no worker.
 *	  Because no authority is ever published and the D3 unfreeze gate (3b-3) reads
 *	  the node-local materialization authority (which this driver never writes), a
 *	  data-only DONE here is consumed by nobody: the thread stays frozen and never
 *	  serves a possibly-stale page (8.A).
 *
 *	  WINDOW = CALLER CONTRACT (amend 3).  scan_lower / scan_upper are the
 *	  caller's responsibility: scan_upper MUST be a validated-complete AND durable
 *	  dead-thread WAL boundary.  This driver does only BASIC legality checks
 *	  (non-NULL, lower <= upper) -- it does NOT claim to validate the target (that
 *	  is D4, 3b-4, via merge_compute_valid_end's torn-tail gate).  A wrong window
 *	  still cannot publish authority, so it cannot make a stale page visible; the
 *	  engine's completeness check turns an incomplete window into BLOCKED.
 *
 *	  FAIL-CLOSED (amend 4).  A bad source or window is NEVER a silent empty
 *	  success: a dead_tid out of range, a missing per-thread WAL dir, a reader
 *	  that cannot be allocated or positioned, or any in-window read error all
 *	  return BLOCKED (the engine reports the in-window errors).  NOT_APPLICABLE is
 *	  reserved for the scope gate (single-node / no shared backend / >2-node),
 *	  which the orchestrator applies; this data driver does not gate scope.
 *
 *	  R13 (amend 1).  The drive runs under PG_TRY/PG_CATCH.  A catchable ERROR
 *	  (e.g. an online variant of a cold component that today would FATAL -- wired
 *	  in 3b-2 -- or an out-of-memory while building the reader) is demoted to a
 *	  result-returning BLOCKED so the recovery-apply worker SURVIVES.  But a
 *	  FATAL/PANIC is NEVER swallowed: cluster_thread_recovery_should_rethrow()
 *	  re-throws it, so a survivor crash the cold component intended can never
 *	  masquerade as "recovery blocked".  The keep_frozen-vs-panic POLICY
 *	  (cluster.thread_recovery_on_unrecoverable) is applied by the orchestrator
 *	  on the final BLOCKED, not here -- this driver is a clean mechanism.
 *
 *	  Single-node stand-in (L239): the genuine cross-node dead-peer reader (a
 *	  foreign thread's stream on shared storage, with its own timeline) is
 *	  forward; on one machine the dead thread's own per-thread dir stands in, and
 *	  the local insertion timeline is used.
 *
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/backend/cluster/cluster_thread_recovery_driver.c
 *
 * NOTES
 *	  This is a pgrac-original file (no derivation from PostgreSQL).
 *	  Spec: spec-4.11-thread-recovery.md (FROZEN v0.3)
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#ifdef USE_PGRAC_CLUSTER

#include <sys/stat.h>
#include <unistd.h>

#include "access/xlog.h"
#include "access/xlog_internal.h"
#include "access/xlogreader.h"
#include "miscadmin.h"
#include "storage/fd.h"
#include "utils/wait_event.h"

#include "cluster/cluster_guc.h"	/* cluster_wal_threads_dir */
#include "cluster/cluster_inject.h" /* CLUSTER_INJECTION_POINT (R13 proof) */
#include "cluster/cluster_thread_recovery.h"
#include "cluster/cluster_wal_state.h" /* CLUSTER_WAL_STATE_SLOT_COUNT */

/*
 * Private state for the per-thread WAL reader.  A minimal file-based reader over
 * one thread directory (mirrors pg_waldump / the merged-recovery reader): the
 * dead thread's stream is not this node's running WAL, so there is no XLogCtl
 * dependency.  Kept local to the driver so 3b-1 touches no cold merge code.
 */
typedef struct ThreadWalReadPrivate {
	int seg_fd; /* currently open segment, -1 = none */
	XLogSegNo seg_no;
	char dir[MAXPGPATH];
} ThreadWalReadPrivate;

static void
/* cppcheck-suppress constParameterCallback */
thread_wal_segment_open(XLogReaderState *state, XLogSegNo nextSegNo, TimeLineID *tli_p)
{
	ThreadWalReadPrivate *p = (ThreadWalReadPrivate *)state->private_data;
	char fname[MAXFNAMELEN];
	char fpath[MAXPGPATH];

	if (p->seg_fd >= 0) {
		close(p->seg_fd);
		p->seg_fd = -1;
	}
	XLogFileName(fname, *tli_p, nextSegNo, state->segcxt.ws_segsize);
	snprintf(fpath, sizeof(fpath), "%s/%s", p->dir, fname);
	p->seg_fd = BasicOpenFile(fpath, O_RDONLY | PG_BINARY);
	if (p->seg_fd < 0) {
		/*
		 * A MISSING next segment is end-of-stream, not a hard error (the dead
		 * thread can stop exactly at a segment boundary): leave seg_fd = -1 so
		 * thread_wal_page_read returns -1 and decode stops.  The engine's
		 * completeness check then turns a window that ends short of scan_upper
		 * into BLOCKED (8.A) -- a missing segment is never a silent DONE.  A
		 * non-ENOENT failure (real I/O error) is fatal.
		 */
		if (errno == ENOENT)
			return;
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not open WAL segment \"%s\" for thread recovery: %m", fpath)));
	}
	p->seg_no = nextSegNo;
	state->seg.ws_file = p->seg_fd;
}

static void
thread_wal_segment_close(XLogReaderState *state)
{
	ThreadWalReadPrivate *p = (ThreadWalReadPrivate *)state->private_data;

	if (p->seg_fd >= 0) {
		close(p->seg_fd);
		p->seg_fd = -1;
	}
	state->seg.ws_file = -1;
}

static int
thread_wal_page_read(XLogReaderState *state, XLogRecPtr targetPagePtr, int reqLen,
					 XLogRecPtr targetRecPtr, char *readBuf)
{
	ThreadWalReadPrivate *p = (ThreadWalReadPrivate *)state->private_data;
	XLogSegNo segno;
	uint32 offset;
	int nread;

	XLByteToSeg(targetPagePtr, segno, state->segcxt.ws_segsize);
	if (p->seg_fd < 0 || segno != p->seg_no)
		thread_wal_segment_open(state, segno, &state->seg.ws_tli);
	if (p->seg_fd < 0)
		return -1; /* missing segment -> stream end (ENOENT above) */

	offset = XLogSegmentOffset(targetPagePtr, state->segcxt.ws_segsize);
	pgstat_report_wait_start(WAIT_EVENT_WAL_READ);
	nread = pg_pread(p->seg_fd, readBuf, XLOG_BLCKSZ, (off_t)offset);
	pgstat_report_wait_end();
	if (nread != XLOG_BLCKSZ)
		return -1; /* torn / short -> stream end */
	return XLOG_BLCKSZ;
}

/*
 * drive_data_inner -- build the dead thread's reader and drive the engine.
 *
 *	Returns BLOCKED (fail-closed) for every bad source / window (amend 4); a
 *	clean run returns whatever cluster_thread_recovery_replay_stream returns.
 *	Runs INSIDE the R13 PG_TRY: it may ereport (the injection point, or an OOM
 *	building the reader); the caller's PG_CATCH demotes a catchable ERROR.  The
 *	injection point sits right after the reader is allocated but BEFORE any
 *	segment fd is opened, so a demoted error leaks no descriptor.
 */
static ClusterThreadRecResult
drive_data_inner(uint16 dead_tid, XLogRecPtr scan_lower, XLogRecPtr scan_upper,
				 ClusterThreadReplayStats *stats)
{
	ThreadWalReadPrivate *priv;
	XLogReaderState *reader;
	XLogRecPtr first_valid;
	ClusterThreadRecResult res;
	struct stat dirstat;
	int n;

	/* amend 4: dead_tid must name a real thread slot. */
	if (dead_tid < 1 || dead_tid > CLUSTER_WAL_STATE_SLOT_COUNT)
		return CLUSTER_THREADREC_BLOCKED;

	/*
	 * amend 3: basic window legality only -- NOT a claim that scan_upper is a
	 * validated durable boundary (that is the caller's contract / D4).
	 */
	if (XLogRecPtrIsInvalid(scan_lower) || XLogRecPtrIsInvalid(scan_upper)
		|| scan_lower > scan_upper)
		return CLUSTER_THREADREC_BLOCKED;

	/* amend 4: the per-thread WAL source must be configured and present. */
	if (cluster_wal_threads_dir == NULL || cluster_wal_threads_dir[0] == '\0')
		return CLUSTER_THREADREC_BLOCKED;

	priv = (ThreadWalReadPrivate *)palloc0(sizeof(ThreadWalReadPrivate));
	priv->seg_fd = -1;
	n = snprintf(priv->dir, sizeof(priv->dir), "%s/thread_%u", cluster_wal_threads_dir,
				 (unsigned)dead_tid);
	if (n < 0 || n >= (int)sizeof(priv->dir)) {
		pfree(priv);
		return CLUSTER_THREADREC_BLOCKED; /* path overflow -> fail-closed */
	}
	if (stat(priv->dir, &dirstat) != 0 || !S_ISDIR(dirstat.st_mode)) {
		ereport(DEBUG2,
				(errmsg_internal("thread recovery: per-thread WAL dir \"%s\" absent -> BLOCKED",
								 priv->dir)));
		pfree(priv);
		return CLUSTER_THREADREC_BLOCKED; /* missing source -> fail-closed */
	}

	reader = XLogReaderAllocate(wal_segment_size, priv->dir,
								XL_ROUTINE(.page_read = thread_wal_page_read,
										   .segment_open = thread_wal_segment_open,
										   .segment_close = thread_wal_segment_close),
								priv);
	if (reader == NULL) {
		pfree(priv);
		return CLUSTER_THREADREC_BLOCKED;
	}

	/*
	 * Single-node stand-in (L239): the local insertion timeline is the dead
	 * thread's timeline on one machine; the genuine cross-node TLI is part of
	 * the source contract, forward.
	 */
	reader->seg.ws_tli = GetWALInsertionTimeLine();

	/*
	 * R13 proof site (3b-1): when this injection point is armed with an ERROR,
	 * the catchable ereport is demoted to BLOCKED by the caller's PG_CATCH and
	 * the worker survives.  Placed before any segment fd is opened.
	 */
	CLUSTER_INJECTION_POINT("cluster-thread-recovery-drive");

	first_valid = XLogFindNextRecord(reader, scan_lower);
	if (XLogRecPtrIsInvalid(first_valid)) {
		if (priv->seg_fd >= 0)
			close(priv->seg_fd);
		XLogReaderFree(reader);
		pfree(priv);
		return CLUSTER_THREADREC_BLOCKED; /* cannot position at lower -> fail-closed */
	}

	res = cluster_thread_recovery_replay_stream(reader, scan_upper, stats);

	if (priv->seg_fd >= 0)
		close(priv->seg_fd);
	XLogReaderFree(reader);
	pfree(priv);
	return res;
}

/*
 * cluster_thread_recovery_drive_data -- the public R13-guarded data driver.
 *
 *	See the file header.  Builds the dead thread's reader, drives the engine, and
 *	demotes a catchable ERROR to BLOCKED (worker survives) while NEVER swallowing
 *	a FATAL/PANIC (amend 1).  Publishes no authority and has no live caller (this
 *	is 3b-1).
 *
 * Author: SqlRush <sqlrush@gmail.com>
 */
ClusterThreadRecResult
cluster_thread_recovery_drive_data(uint16 dead_tid, XLogRecPtr scan_lower, XLogRecPtr scan_upper,
								   ClusterThreadReplayStats *stats)
{
	volatile ClusterThreadRecResult res = CLUSTER_THREADREC_BLOCKED;
	MemoryContext caller_ctx = CurrentMemoryContext;

	if (stats)
		memset(stats, 0, sizeof(*stats));

	PG_TRY();
	{
		res = drive_data_inner(dead_tid, scan_lower, scan_upper, stats);
	}
	PG_CATCH();
	{
		ErrorData *edata;

		/* CopyErrorData requires a context other than ErrorContext. */
		MemoryContextSwitchTo(caller_ctx);
		edata = CopyErrorData();

		/* amend 1: a FATAL/PANIC is never demoted -- re-throw it. */
		if (cluster_thread_recovery_should_rethrow(edata->elevel)) {
			FreeErrorData(edata);
			PG_RE_THROW();
		}

		/*
		 * A catchable ERROR -> result-returning BLOCKED; the worker survives.
		 * Reset stats so a partial run never reports progress (8.A): a BLOCKED
		 * has, by contract, recovered nothing.  The keep_frozen-vs-panic policy
		 * is the orchestrator's (3b-2), not this mechanism's.
		 */
		FlushErrorState();
		FreeErrorData(edata);
		if (stats)
			memset(stats, 0, sizeof(*stats));
		res = CLUSTER_THREADREC_BLOCKED;
	}
	PG_END_TRY();

	return res;
}

#else /* !USE_PGRAC_CLUSTER */

/* Disable-cluster build: this file compiles to nothing. */

#endif /* USE_PGRAC_CLUSTER */
