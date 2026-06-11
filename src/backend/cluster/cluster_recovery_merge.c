/*-------------------------------------------------------------------------
 *
 * cluster_recovery_merge.c
 *	  pgrac k-way SCN merge replay engine -- backend driver (spec-4.5).
 *
 *	  Wraps one file-based XLogReader per merge-set thread (each reading
 *	  <cluster.wal_threads_dir>/thread_<id>) around the pure min-heap in
 *	  cluster_recovery_merge.h.  begin() seeds each stream's first
 *	  record into the heap; next() returns the global-SCN-minimum record
 *	  (lazy-advancing the previously returned stream first); end() frees
 *	  everything.  Streams terminate naturally at their torn tail.
 *
 *	  Per-record decode exposes header.xl_scn as the ordering key, so a
 *	  zero scn that appears mid-stream (only legal as a boot prefix) or
 *	  a backwards scn is already rejected by ValidXLogRecordHeader
 *	  (spec-4.5 D3) before it reaches here.
 *
 *	  The engine only READS and ORDERS; the §3.3b freshness contract,
 *	  the §3.3c authority bound and the §3.3e G/S/L/U apply matrix are
 *	  applied by the caller (xlogrecovery.c) per popped record.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/backend/cluster/cluster_recovery_merge.c
 *
 * NOTES
 *	  This is a pgrac-original file (no derivation from PostgreSQL).
 *	  Spec: spec-4.5-kway-scn-merge-replay.md FROZEN v1.0
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#ifdef USE_PGRAC_CLUSTER

#include <unistd.h>

#include "access/xlog.h"
#include "access/xlog_internal.h"
#include "access/xlogreader.h"
#include "cluster/cluster_guc.h"
#include "cluster/cluster_recovery_plan.h"
#include "cluster/cluster_recovery_merge.h"
#include "cluster/cluster_recovery_worker.h"
#include "cluster/cluster_wal_thread.h"
#include "lib/stringinfo.h"
#include "storage/fd.h"
#include "utils/wait_event.h"

/*
 * Merged-replay window state (spec-4.5 §3.3b freshness + §3.3d/D8 GCS
 * cold bypass).  Single-process (startup): a plain file-scope flag is
 * enough.  The current record's xl_scn is published per record so the
 * §3.3b freshness skip and the central pd_block_scn stamp can read it.
 */
static bool merge_window_active = false;
static uint64 merge_window_scn = 0;

void
cluster_recovery_merge_window_enter(void)
{
	merge_window_active = true;
	merge_window_scn = 0;
}

void
cluster_recovery_merge_window_leave(void)
{
	merge_window_active = false;
	merge_window_scn = 0;
}

void
cluster_recovery_merge_set_scn(uint64 scn)
{
	merge_window_scn = scn;
}

bool
cluster_recovery_merge_window_is_active(void)
{
	return merge_window_active;
}

uint64
cluster_recovery_merge_window_scn(void)
{
	return merge_window_scn;
}

typedef struct MergeStream {
	uint16 thread_id;
	XLogReaderState *reader;
	int seg_fd; /* currently open segment, -1 = none */
	XLogSegNo seg_no;
	char dir[MAXPGPATH];
	bool exhausted;
} MergeStream;

struct ClusterRecoveryMergeState {
	int n_streams;
	MergeStream streams[CLUSTER_RECMERGE_MAX_STREAMS];
	ClusterRecmergeHeap heap;
	int last_stream; /* stream returned by the previous next(), -1 = none */
	TimeLineID tli;
};

/*
 * Segment open/close + page read for an arbitrary thread directory.
 * Mirrors pg_waldump's file-based reader (no XLogCtl dependency -- the
 * foreign streams are not this node's running WAL).
 */
static void
/* cppcheck-suppress constParameterCallback */
merge_segment_open(XLogReaderState *state, XLogSegNo nextSegNo, TimeLineID *tli_p)
{
	MergeStream *ms = (MergeStream *)state->private_data;
	char fname[MAXFNAMELEN];
	char fpath[MAXPGPATH];

	if (ms->seg_fd >= 0) {
		close(ms->seg_fd);
		ms->seg_fd = -1;
	}
	XLogFileName(fname, *tli_p, nextSegNo, state->segcxt.ws_segsize);
	snprintf(fpath, sizeof(fpath), "%s/%s", ms->dir, fname);
	ms->seg_fd = BasicOpenFile(fpath, O_RDONLY | PG_BINARY);
	if (ms->seg_fd < 0)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not open WAL segment \"%s\" for merged recovery: %m", fpath)));
	ms->seg_no = nextSegNo;
	state->seg.ws_file = ms->seg_fd;
}

static void
merge_segment_close(XLogReaderState *state)
{
	MergeStream *ms = (MergeStream *)state->private_data;

	if (ms->seg_fd >= 0) {
		close(ms->seg_fd);
		ms->seg_fd = -1;
	}
	state->seg.ws_file = -1;
}

static int
merge_page_read(XLogReaderState *state, XLogRecPtr targetPagePtr, int reqLen,
				XLogRecPtr targetRecPtr, char *readBuf)
{
	MergeStream *ms = (MergeStream *)state->private_data;
	XLogSegNo segno;
	uint32 offset;
	int nread;

	XLByteToSeg(targetPagePtr, segno, state->segcxt.ws_segsize);
	if (ms->seg_fd < 0 || segno != ms->seg_no)
		merge_segment_open(state, segno, &state->seg.ws_tli);

	offset = XLogSegmentOffset(targetPagePtr, state->segcxt.ws_segsize);
	pgstat_report_wait_start(WAIT_EVENT_WAL_READ);
	nread = pg_pread(ms->seg_fd, readBuf, XLOG_BLCKSZ, (off_t)offset);
	pgstat_report_wait_end();
	if (nread != XLOG_BLCKSZ)
		return -1; /* torn / short -> stream end */
	return XLOG_BLCKSZ;
}

/* Read one record on a stream; push its key on success, mark exhausted
 * on torn tail / read failure. */
static void
stream_advance(ClusterRecoveryMergeState *st, int idx)
{
	MergeStream *ms = &st->streams[idx];
	char *errm = NULL;
	const XLogRecord *rec;

	if (ms->exhausted)
		return;
	rec = XLogReadRecord(ms->reader, &errm);
	if (rec == NULL) {
		ms->exhausted = true;
		return;
	}
	{
		ClusterRecmergeKey key;

		key.scn = rec->xl_scn;
		key.lsn = ms->reader->ReadRecPtr;
		key.node = (int32)ms->thread_id - 1;
		cluster_recmerge_heap_push(&st->heap, key, idx);
	}
}

/*
 * cluster_recovery_merge_decide -- §3.1 engage + §3.2 53RA3 gate.
 */
ClusterMergeEngage
cluster_recovery_merge_decide(uint16 own_thread, XLogRecPtr own_redo, uint64 out_bitmap[2],
							  XLogRecPtr *out_start)
{
	ClusterRecoveryPlan plan;
	ClusterRecoveryWorkerPool pool;
	bool have_pool;
	uint16 tid;
	StringInfoData blockers;

	if (!cluster_merged_recovery)
		return CLUSTER_MERGE_NO_DISABLED;
	if (cluster_wal_threads_dir == NULL || cluster_wal_threads_dir[0] == '\0'
		|| own_thread == XLP_THREAD_ID_LEGACY)
		return CLUSTER_MERGE_NO_NOT_CONFIGURED;
	if (!cluster_recovery_plan_snapshot(&plan) || plan.failed)
		return CLUSTER_MERGE_NO_NO_PLAN;
	if (plan.n_crashed_candidate == 0)
		return CLUSTER_MERGE_NO_NO_CANDIDATES;
	if (plan.n_alive > 0)
		return CLUSTER_MERGE_NO_NOT_COLD; /* warm -> 4.6/4.7, not us */

	/*
	 * spec-4.5 capability gate (A-closure item 4): merged recovery
	 * replays a crashed peer's SHARED-storage pages.  No real
	 * shared-data backend exists yet -- cluster_shared_fs LOCAL writes
	 * each node's own PGDATA, STUB is pure md.c (spec-3.18 V-2).  With
	 * neither providing genuinely shared data files, a peer's S-class
	 * record cannot be honestly applied to shared storage.  Fail-closed
	 * 53RA3 even when the operator set merged_recovery=on, so the
	 * feature cannot be mis-engaged before the shared-storage backend
	 * (roadmap 4.5a) lands.
	 */
	ereport(FATAL,
			(errcode(ERRCODE_CLUSTER_MERGED_RECOVERY_BLOCKED),
			 errmsg("merged k-way recovery is not supported without a shared-data storage backend"),
			 errdetail("cluster.merged_recovery is on and crash candidates exist, but no shared "
					   "data-file backend is available (cluster_shared_fs is stub/local, which is "
					   "per-node, not shared)."),
			 errhint("Recover this node's own stream with cluster.merged_recovery=off.  True "
					 "shared-storage merged recovery awaits the shared-data backend (roadmap "
					 "4.5a).")));

	/*
	 * §3.2 hard gate.  Collect every blocking reason, then FATAL 53RA3
	 * once with the full list -- never silently fall back to single
	 * stream (that would skip a crashed peer's committed WAL).
	 */
	initStringInfo(&blockers);
	if (plan.n_unknown > 0)
		appendStringInfo(&blockers, "%s%u UNKNOWN plan thread(s)", blockers.len ? "; " : "",
						 (unsigned)plan.n_unknown);
	if (!fullPageWrites)
		appendStringInfo(&blockers, "%slocal full_page_writes=off", blockers.len ? "; " : "");

	have_pool = cluster_recovery_worker_pool_snapshot(&pool);

	/* Build the merge set (candidates + own) and validate each. */
	memset(out_bitmap, 0, sizeof(uint64) * 2);
	for (tid = 1; tid <= CLUSTER_WAL_STATE_SLOT_COUNT; tid++) {
		bool is_candidate = cluster_recovery_plan_candidate_test(&plan, tid);
		ClusterWalStateSlot slot;

		if (!is_candidate && tid != own_thread)
			continue;

		out_bitmap[(tid - 1) / 64] |= ((uint64)1 << ((tid - 1) % 64));

		if (tid == own_thread) {
			out_start[tid] = own_redo;
			continue; /* own thread: gate items below are peer-only */
		}

		/*
		 * Candidate stream must validate OK.  Use the worker verdict if
		 * present; NONE means the workers did not finish in time, so
		 * re-validate inline (Q6).  SKIPPED is fatal -- the peer was
		 * alive, so the cold premise broke.
		 */
		{
			ClusterRecoveryStreamVerdict v
				= have_pool ? (ClusterRecoveryStreamVerdict)pool.stream_verdict[tid]
							: CLUSTER_RECOVERY_STREAM_NONE;

			if (v == CLUSTER_RECOVERY_STREAM_NONE)
				v = cluster_recovery_worker_revalidate(tid);

			if (v == CLUSTER_RECOVERY_STREAM_SKIPPED)
				appendStringInfo(&blockers, "%sthread %u stream SKIPPED (peer was alive)",
								 blockers.len ? "; " : "", (unsigned)tid);
			else if (v != CLUSTER_RECOVERY_STREAM_OK)
				appendStringInfo(&blockers, "%sthread %u stream not OK (verdict %u)",
								 blockers.len ? "; " : "", (unsigned)tid, (unsigned)v);
		}

		/* Candidate start point + fpw history from its slot. */
		if (cluster_wal_state_read_slot(tid, &slot) != CLUSTER_WAL_SLOT_OK) {
			appendStringInfo(&blockers, "%sthread %u slot unreadable", blockers.len ? "; " : "",
							 (unsigned)tid);
		} else {
			if (slot.checkpoint_redo_lsn == 0)
				appendStringInfo(&blockers, "%sthread %u has no checkpoint redo start",
								 blockers.len ? "; " : "", (unsigned)tid);
			if (slot.fpw_was_off != 0)
				appendStringInfo(&blockers, "%sthread %u ran with full_page_writes=off",
								 blockers.len ? "; " : "", (unsigned)tid);
			out_start[tid] = (XLogRecPtr)slot.checkpoint_redo_lsn;
		}
	}

	if (blockers.len > 0)
		ereport(FATAL, (errcode(ERRCODE_CLUSTER_MERGED_RECOVERY_BLOCKED),
						errmsg("merged k-way recovery refused"), errdetail("%s.", blockers.data),
						errhint("Resolve the shared WAL storage / configuration, or set "
								"cluster.merged_recovery=off to recover this node's own "
								"stream only (a crashed peer's committed WAL will not be "
								"recovered).")));
	pfree(blockers.data);
	return CLUSTER_MERGE_ENGAGE;
}

ClusterRecoveryMergeState *
cluster_recovery_merge_begin(const uint64 merge_bitmap[2], const XLogRecPtr *start_lsn,
							 TimeLineID tli)
{
	ClusterRecoveryMergeState *st;
	uint16 tid;
	int idx = 0;

	st = (ClusterRecoveryMergeState *)palloc0(sizeof(ClusterRecoveryMergeState));
	cluster_recmerge_heap_init(&st->heap);
	st->last_stream = -1;
	st->tli = tli;

	for (tid = 1; tid <= CLUSTER_WAL_STATE_SLOT_COUNT; tid++) {
		MergeStream *ms;

		if ((merge_bitmap[(tid - 1) / 64] & ((uint64)1 << ((tid - 1) % 64))) == 0)
			continue;
		if (idx >= CLUSTER_RECMERGE_MAX_STREAMS)
			ereport(ERROR, (errcode(ERRCODE_CLUSTER_MERGED_RECOVERY_BLOCKED),
							errmsg("merged recovery merge set exceeds %d streams",
								   CLUSTER_RECMERGE_MAX_STREAMS)));
		ms = &st->streams[idx];
		ms->thread_id = tid;
		ms->seg_fd = -1;
		ms->exhausted = false;
		snprintf(ms->dir, sizeof(ms->dir), "%s/thread_%u", cluster_wal_threads_dir, (unsigned)tid);
		ms->reader = XLogReaderAllocate(wal_segment_size, ms->dir,
										XL_ROUTINE(.page_read = merge_page_read,
												   .segment_open = merge_segment_open,
												   .segment_close = merge_segment_close),
										ms);
		if (ms->reader == NULL)
			ereport(ERROR, (errcode(ERRCODE_OUT_OF_MEMORY),
							errmsg("out of memory allocating merged-recovery reader")));
		ms->reader->seg.ws_tli = tli;
		XLogBeginRead(ms->reader, start_lsn[tid]);
		idx++;
	}
	st->n_streams = idx;

	/* Seed the heap with each stream's first record. */
	for (idx = 0; idx < st->n_streams; idx++)
		stream_advance(st, idx);
	return st;
}

XLogReaderState *
cluster_recovery_merge_next(ClusterRecoveryMergeState *st, uint16 *thread_out, char **errmsg_out)
{
	ClusterRecmergeHeapEntry top;

	if (errmsg_out)
		*errmsg_out = NULL;

	/* Lazily advance the stream returned last time (its record has now
	 * been consumed by the caller). */
	if (st->last_stream >= 0) {
		stream_advance(st, st->last_stream);
		st->last_stream = -1;
	}

	if (!cluster_recmerge_heap_pop(&st->heap, &top))
		return NULL; /* all streams exhausted */

	st->last_stream = top.stream;
	if (thread_out)
		*thread_out = st->streams[top.stream].thread_id;
	return st->streams[top.stream].reader;
}

void
cluster_recovery_merge_end(ClusterRecoveryMergeState *st)
{
	int i;

	if (st == NULL)
		return;
	for (i = 0; i < st->n_streams; i++) {
		MergeStream *ms = &st->streams[i];

		if (ms->seg_fd >= 0)
			close(ms->seg_fd);
		if (ms->reader)
			XLogReaderFree(ms->reader);
	}
	pfree(st);
}

#else /* !USE_PGRAC_CLUSTER */

/* Disable-cluster build: this file compiles to nothing. */

#endif /* USE_PGRAC_CLUSTER */
