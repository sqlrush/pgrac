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

#include "cluster/cluster_guc.h"		 /* cluster_wal_threads_dir */
#include "cluster/cluster_inject.h"		 /* CLUSTER_INJECTION_POINT (R13 proof) */
#include "cluster/cluster_remote_xact.h" /* online-writer scope (R14, spec-4.11 3b-2) */
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

static XLogRecPtr
thread_validated_lsn_floor(XLogRecPtr highest_lsn)
{
	XLogRecPtr prior;

	if (XLogRecPtrIsInvalid(highest_lsn))
		return InvalidXLogRecPtr;

	prior = highest_lsn - 1;
	return prior - (prior % XLOG_BLCKSZ);
}

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
 * thread_wal_reader_make -- allocate a reader over the dead thread's per-thread
 * WAL dir (<cluster.wal_threads_dir>/thread_<tid>), positioned by the caller.
 *
 *	Shared by drive_data_inner (3b-1) and validated_end_inner (D4, 3b-4a) so the
 *	per-thread reader setup lives in one place.  Returns the reader (with *priv_out
 *	set) or NULL on any fail-closed condition: a dead_tid that names no real slot,
 *	an unset/oversize/missing per-thread dir, or an allocation failure.  On NULL
 *	the private state is already freed; on success the CALLER owns both and must
 *	close priv->seg_fd, XLogReaderFree(reader) and pfree(priv) on every exit.  The
 *	reader is NOT positioned -- the caller runs XLogFindNextRecord.
 */
static XLogReaderState *
thread_wal_reader_make(uint16 dead_tid, ThreadWalReadPrivate **priv_out)
{
	ThreadWalReadPrivate *priv;
	XLogReaderState *reader;
	struct stat dirstat;
	int n;

	*priv_out = NULL;

	/* dead_tid must name a real thread slot. */
	if (dead_tid < 1 || dead_tid > CLUSTER_WAL_STATE_SLOT_COUNT)
		return NULL;
	/* the per-thread WAL source must be configured. */
	if (cluster_wal_threads_dir == NULL || cluster_wal_threads_dir[0] == '\0')
		return NULL;

	priv = (ThreadWalReadPrivate *)palloc0(sizeof(ThreadWalReadPrivate));
	priv->seg_fd = -1;
	n = snprintf(priv->dir, sizeof(priv->dir), "%s/thread_%u", cluster_wal_threads_dir,
				 (unsigned)dead_tid);
	if (n < 0 || n >= (int)sizeof(priv->dir)) {
		pfree(priv);
		return NULL; /* path overflow -> fail-closed */
	}
	if (stat(priv->dir, &dirstat) != 0 || !S_ISDIR(dirstat.st_mode)) {
		ereport(DEBUG2,
				(errmsg_internal("thread recovery: per-thread WAL dir \"%s\" absent -> BLOCKED",
								 priv->dir)));
		pfree(priv);
		return NULL; /* missing source -> fail-closed */
	}

	reader = XLogReaderAllocate(wal_segment_size, priv->dir,
								XL_ROUTINE(.page_read = thread_wal_page_read,
										   .segment_open = thread_wal_segment_open,
										   .segment_close = thread_wal_segment_close),
								priv);
	if (reader == NULL) {
		pfree(priv);
		return NULL;
	}

	/*
	 * Single-node stand-in (L239): the local insertion timeline is the dead
	 * thread's timeline on one machine; the genuine cross-node TLI is part of
	 * the source contract, forward.
	 */
	reader->seg.ws_tli = GetWALInsertionTimeLine();

	*priv_out = priv;
	return reader;
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
				 const ClusterThreadVisCtx *vis, ClusterThreadTouchedRels *touched,
				 ClusterThreadReplayStats *stats)
{
	ThreadWalReadPrivate *priv;
	XLogReaderState *reader;
	XLogRecPtr first_valid;
	ClusterThreadRecResult res;

	/*
	 * amend 3: basic window legality only -- NOT a claim that scan_upper is a
	 * validated durable boundary (that is the caller's contract / D4).
	 */
	if (XLogRecPtrIsInvalid(scan_lower) || XLogRecPtrIsInvalid(scan_upper)
		|| scan_lower > scan_upper)
		return CLUSTER_THREADREC_BLOCKED;

	/* amend 4: bad tid / unset / missing per-thread dir -> fail-closed. */
	reader = thread_wal_reader_make(dead_tid, &priv);
	if (reader == NULL)
		return CLUSTER_THREADREC_BLOCKED;

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

	res = cluster_thread_recovery_replay_stream_ex(reader, scan_upper, vis, touched, stats);

	if (priv->seg_fd >= 0)
		close(priv->seg_fd);
	XLogReaderFree(reader);
	pfree(priv);
	return res;
}

/*
 * cluster_thread_recovery_drive -- the public R13-guarded GENERAL driver
 *		(spec-4.11 3b-2).
 *
 *	Builds the dead thread's reader and drives the combined replay engine, with
 *	an optional visibility pass (vis) + touched-rel collector (touched).  Demotes
 *	a catchable ERROR to BLOCKED (worker survives) while NEVER swallowing a
 *	FATAL/PANIC (amend 1).  When vis->do_visibility, the whole drive runs inside
 *	an episode-fenced online-writer scope so the visibility apply's
 *	cluster_remote_xact_set is admitted (R14); the scope is popped on every
 *	survivable exit (a re-thrown FATAL tears the process down, so the unpopped
 *	depth dies with it).
 *
 * Author: SqlRush <sqlrush@gmail.com>
 */
ClusterThreadRecResult
cluster_thread_recovery_drive(uint16 dead_tid, XLogRecPtr scan_lower, XLogRecPtr scan_upper,
							  const ClusterThreadVisCtx *vis, ClusterThreadTouchedRels *touched,
							  ClusterThreadReplayStats *stats)
{
	volatile ClusterThreadRecResult res = CLUSTER_THREADREC_BLOCKED;
	MemoryContext caller_ctx = CurrentMemoryContext;
	bool writer_scope = (vis != NULL && vis->do_visibility);

	if (stats)
		memset(stats, 0, sizeof(*stats));

	/* R14: admit the visibility apply's per-origin writes from this bgworker. */
	if (writer_scope)
		cluster_remote_xact_online_writer_push();

	PG_TRY();
	{
		res = drive_data_inner(dead_tid, scan_lower, scan_upper, vis, touched, stats);
	}
	PG_CATCH();
	{
		ErrorData *edata;

		/* CopyErrorData requires a context other than ErrorContext. */
		MemoryContextSwitchTo(caller_ctx);
		edata = CopyErrorData();

		/* amend 1: a FATAL/PANIC is never demoted -- re-throw it.  Pop the
		 * online-writer scope first so the depth balances on the (unlikely)
		 * chance the re-throw is itself caught further up; a process-fatal
		 * re-throw makes this moot. */
		if (cluster_thread_recovery_should_rethrow(edata->elevel)) {
			FreeErrorData(edata);
			if (writer_scope)
				cluster_remote_xact_online_writer_pop();
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

	if (writer_scope)
		cluster_remote_xact_online_writer_pop();

	return res;
}

/*
 * cluster_thread_recovery_drive_data -- the public R13-guarded DATA driver
 *		(increment 3b-1): the vis-off / no-collector special case of
 *		cluster_thread_recovery_drive.  Publishes no authority, does no
 *		visibility pass, issues no durability barrier (the future replay_one
 *		orchestrator and the TEST SRF call it).
 *
 * Author: SqlRush <sqlrush@gmail.com>
 */
ClusterThreadRecResult
cluster_thread_recovery_drive_data(uint16 dead_tid, XLogRecPtr scan_lower, XLogRecPtr scan_upper,
								   ClusterThreadReplayStats *stats)
{
	return cluster_thread_recovery_drive(dead_tid, scan_lower, scan_upper, NULL, NULL, stats);
}

/*
 * validated_end_inner -- decode-only pass over the dead thread's per-thread WAL
 * to find the VALIDATED torn-tail boundary (spec-4.11 D4, 3b-4a).
 *
 *	Mirrors the cold merged-recovery merge_compute_valid_end (recovery_merge.c)
 *	over the thread driver's reader (the cold helper is static + bound to the
 *	merge reader/dir, so the thread path gets its own pass; the cold code is
 *	untouched, per the spec-4.11 "cold byte-for-byte unchanged" discipline).
 *
 *	From scan_lower, decode every record and track the EndRecPtr of the LAST
 *	COMPLETE one (a torn / short tail just stops the decode -- the dead thread
 *	may legitimately stop mid-record at the crash point).  The dead thread is
 *	always a FOREIGN candidate, so both fail-closed checks apply (8.A):
 *	  (a) no complete record decoded from scan_lower -> corruption at the start;
 *	  (b) valid_end < validated_floor (the registry's highest_lsn rounded down
 *	      to the start of its final WAL page) -> the decode stopped BELOW the
 *	      durable complete-page floor = mid-stream corruption, NOT a torn tail.
 *	      The final observed WAL page itself can be a crash-time partial page,
 *	      especially after pg_switch_wal(), so it is not used as the hard floor.
 *	      Treating earlier corruption as a torn tail would silently drop the
 *	      dead thread's committed WAL.
 *	Either yields BLOCKED (result-returning, NOT the cold FATAL -- online R13);
 *	a clean decode yields DONE with *out_valid_end set to the boundary the
 *	replay pass must reach.
 */
static ClusterThreadRecResult
validated_end_inner(uint16 dead_tid, XLogRecPtr scan_lower, XLogRecPtr validated_min,
					XLogRecPtr *out_valid_end)
{
	ThreadWalReadPrivate *priv;
	XLogReaderState *reader;
	XLogRecPtr first_valid;
	XLogRecPtr valid_end;
	XLogRecPtr validated_floor;
	char *errm = NULL;

	*out_valid_end = InvalidXLogRecPtr;

	if (XLogRecPtrIsInvalid(scan_lower))
		return CLUSTER_THREADREC_BLOCKED;

	reader = thread_wal_reader_make(dead_tid, &priv);
	if (reader == NULL)
		return CLUSTER_THREADREC_BLOCKED;

	first_valid = XLogFindNextRecord(reader, scan_lower);
	if (XLogRecPtrIsInvalid(first_valid)) {
		if (priv->seg_fd >= 0)
			close(priv->seg_fd);
		XLogReaderFree(reader);
		pfree(priv);
		return CLUSTER_THREADREC_BLOCKED; /* cannot position at lower -> fail-closed */
	}

	/*
	 * Decode forward; valid_end tracks the last complete record's end (seeded to
	 * the start, so "no complete record" leaves valid_end == first_valid).  A
	 * NULL return (torn / short / decode error) ends the scan -- errm is not a
	 * hard error here: a legitimate torn tail at the crash point is expected and
	 * the (a)/(b) checks below catch a tail that is actually corruption.
	 */
	valid_end = first_valid;
	while (XLogReadRecord(reader, &errm) != NULL)
		valid_end = reader->EndRecPtr;

	if (priv->seg_fd >= 0)
		close(priv->seg_fd);
	XLogReaderFree(reader);
	pfree(priv);

	/* (a) not one complete record / (b) stopped below the durable page floor. */
	validated_floor = thread_validated_lsn_floor(validated_min);
	if (validated_floor <= first_valid)
		validated_floor = InvalidXLogRecPtr;
	if (valid_end == first_valid
		|| (!XLogRecPtrIsInvalid(validated_floor) && valid_end < validated_floor))
		return CLUSTER_THREADREC_BLOCKED;

	*out_valid_end = valid_end;
	return CLUSTER_THREADREC_DONE;
}

/*
 * cluster_thread_recovery_validated_end -- the public R13-guarded D4 boundary
 *		pass (spec-4.11 3b-4a).
 *
 *	Returns DONE with *out_valid_end = the validated torn-tail boundary the
 *	replay must reach, or BLOCKED (fail-closed: bad source, unpositionable,
 *	corruption below the watermark, or a demoted catchable ERROR -- e.g. a real
 *	I/O error opening a segment).  A FATAL/PANIC is re-thrown, never swallowed
 *	(amend 1, same contract as cluster_thread_recovery_drive).  *out_valid_end is
 *	always written (Invalid on BLOCKED).
 *
 * Author: SqlRush <sqlrush@gmail.com>
 */
ClusterThreadRecResult
cluster_thread_recovery_validated_end(uint16 dead_tid, XLogRecPtr scan_lower,
									  XLogRecPtr validated_min, XLogRecPtr *out_valid_end)
{
	volatile ClusterThreadRecResult res = CLUSTER_THREADREC_BLOCKED;
	MemoryContext caller_ctx = CurrentMemoryContext;

	*out_valid_end = InvalidXLogRecPtr;

	PG_TRY();
	{
		res = validated_end_inner(dead_tid, scan_lower, validated_min, out_valid_end);
	}
	PG_CATCH();
	{
		ErrorData *edata;

		MemoryContextSwitchTo(caller_ctx);
		edata = CopyErrorData();

		/* A FATAL/PANIC is never demoted -- re-throw it (amend 1). */
		if (cluster_thread_recovery_should_rethrow(edata->elevel)) {
			FreeErrorData(edata);
			PG_RE_THROW();
		}

		/* A catchable ERROR -> result-returning BLOCKED; out stays Invalid. */
		FlushErrorState();
		FreeErrorData(edata);
		*out_valid_end = InvalidXLogRecPtr;
		res = CLUSTER_THREADREC_BLOCKED;
	}
	PG_END_TRY();

	return res;
}

#else /* !USE_PGRAC_CLUSTER */

/* Disable-cluster build: this file compiles to nothing. */

#endif /* USE_PGRAC_CLUSTER */
