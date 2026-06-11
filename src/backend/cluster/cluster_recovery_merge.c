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
#include "cluster/cluster_recovery_merge.h"
#include "storage/fd.h"
#include "utils/wait_event.h"

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
	XLogRecord *rec;

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
