/*-------------------------------------------------------------------------
 *
 * cluster_recovery_worker.h
 *	  pgrac Recovery Worker skeleton -- per-thread worker pool +
 *	  candidate stream validation (spec-4.4).
 *
 *	  The Coordinator (= the startup process, spec-4.3) launches up to
 *	  cluster.recovery_workers_max dynamic background workers when the
 *	  recovery plan reports CRASHED_CANDIDATE threads.  Each worker
 *	  validates its assigned candidate streams -- claim file CONTENT
 *	  (40B magic/version/crc/identity, spec-4.1 semantics) and the
 *	  LAST WRITTEN WAL page located from the registry highest_lsn
 *	  watermark (never the preallocated/recycled segment tail) -- and
 *	  publishes per-thread verdicts.  Observational only: no record
 *	  decoding, no apply, never blocks or delays recovery (merged
 *	  replay = spec-4.5, which also flips non-OK verdicts into the
 *	  53RA3 fail-closed gate together with the plan's UNKNOWN rule).
 *
 *	  Pool concurrency (spec-4.4 round-1 P1-3):
 *	    - generation / workers_requested / assigned_bitmap are written
 *	      by the coordinator only, before any worker starts.
 *	    - slot_state[] is pg_atomic_uint32: REQUESTED -> RUNNING ->
 *	      DONE on the worker's normal path; RUNNING -> FAILED from the
 *	      worker's before_shmem_exit callback (ERROR/FATAL exits);
 *	      REQUESTED -> FAILED from the coordinator on spawn failure.
 *	    - started/done/failed counts are NOT stored: dump derives them
 *	      from slot_state[] on read.
 *	    - stream_verdict[tid] has exactly one writer (assignments are
 *	      pairwise disjoint -- unit-locked conservation).
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/include/cluster/cluster_recovery_worker.h
 *
 * NOTES
 *	  This is a pgrac-original file (no derivation from PostgreSQL).
 *	  Spec: spec-4.4-recovery-worker-skeleton.md FROZEN v1.0
 *
 *-------------------------------------------------------------------------
 */
#ifndef CLUSTER_RECOVERY_WORKER_H
#define CLUSTER_RECOVERY_WORKER_H

#include "access/xlog_internal.h"
#include "cluster/cluster_wal_state.h"
#include "cluster/cluster_xlog.h"
#include "port/atomics.h"

/* Worker slot lifecycle (§2.1; on-shmem as pg_atomic_uint32 values). */
typedef enum ClusterRecoveryWorkerState {
	CLUSTER_RECOVERY_WORKER_UNUSED = 0,
	CLUSTER_RECOVERY_WORKER_REQUESTED,	  /* coordinator registered the bgworker */
	CLUSTER_RECOVERY_WORKER_RUNNING,	  /* worker attached, validating         */
	CLUSTER_RECOVERY_WORKER_DONE,		  /* all assigned verdicts written       */
	CLUSTER_RECOVERY_WORKER_FAILED,		  /* worker error exit after RUNNING     */
	CLUSTER_RECOVERY_WORKER_SPAWN_FAILED, /* registration failed: no worker
										   * ever existed (round-2 P2: must not
										   * count as started)                 */
} ClusterRecoveryWorkerState;

/* Per-thread stream validation verdict. */
typedef enum ClusterRecoveryStreamVerdict {
	CLUSTER_RECOVERY_STREAM_NONE = 0,	/* not validated                     */
	CLUSTER_RECOVERY_STREAM_OK,			/* claim + last-written page check   */
	CLUSTER_RECOVERY_STREAM_SUSPECT,	/* readable but claim/page mismatch  */
	CLUSTER_RECOVERY_STREAM_UNREADABLE, /* open/pread failure / no slot      */
	CLUSTER_RECOVERY_STREAM_SKIPPED,	/* stale assignment: peer no longer a
										 * crash candidate at worker re-check
										 * (alive again / clean) -- never
										 * read a live peer's stream         */
} ClusterRecoveryStreamVerdict;

#define CLUSTER_RECOVERY_WORKER_MAX_SLOTS 16

typedef struct ClusterRecoveryWorkerPool {
	uint32 generation;		  /* coordinator-only writer; +1 per launch  */
	uint16 workers_requested; /* coordinator-only writer                 */
	uint16 _pad;
	pg_atomic_uint32 slot_state[CLUSTER_RECOVERY_WORKER_MAX_SLOTS];
	uint64 assigned_bitmap[CLUSTER_RECOVERY_WORKER_MAX_SLOTS][2];
	uint8 stream_verdict[CLUSTER_WAL_STATE_SLOT_COUNT + 1]; /* by thread */
} ClusterRecoveryWorkerPool;

/* ---- pure helpers (header-only; unit-testable, no backend deps) ---- */

/*
 * cluster_recovery_worker_assign -- §2.3 round-robin striping.
 *
 *	Walks candidate threads in ascending tid order and stripes them
 *	over n_workers = Min(n_candidates, cap) slots (candidate i goes to
 *	slot i % n_workers; a worker handles multiple threads when the
 *	candidate count exceeds the cap).  Returns n_workers (0 when there
 *	is nothing to do); out_assigned is fully zeroed first.
 */
static inline int
cluster_recovery_worker_assign(const uint64 candidate_bitmap[2], uint16 n_candidates, int cap,
							   uint64 out_assigned[CLUSTER_RECOVERY_WORKER_MAX_SLOTS][2])
{
	int n_workers;
	int idx = 0;
	uint16 tid;

	memset(out_assigned, 0, sizeof(uint64) * CLUSTER_RECOVERY_WORKER_MAX_SLOTS * 2);
	if (cap > CLUSTER_RECOVERY_WORKER_MAX_SLOTS)
		cap = CLUSTER_RECOVERY_WORKER_MAX_SLOTS;
	n_workers = Min((int)n_candidates, cap);
	if (n_workers <= 0)
		return 0;

	for (tid = 1; tid <= CLUSTER_WAL_STATE_SLOT_COUNT; tid++) {
		if ((candidate_bitmap[(tid - 1) / 64] & ((uint64)1 << ((tid - 1) % 64))) == 0)
			continue;
		out_assigned[idx % n_workers][(tid - 1) / 64] |= ((uint64)1 << ((tid - 1) % 64));
		idx++;
	}
	return n_workers;
}

/*
 * cluster_recovery_stream_page_check -- one WAL page header check.
 *
 *	page points at XLOG_BLCKSZ bytes pread from a segment at a WRITTEN
 *	offset (the caller derives it from the registry highest_lsn -- the
 *	preallocated/recycled tail must never reach here, spec-4.4 P0).
 *
 *	OK requires exactly what the production WAL reader requires of a
 *	page header (round-2 P1-1: stream_ok feeds spec-4.5 as pre-merge
 *	evidence and must never accept a page replay would reject):
 *	xlp_magic; no undefined xlp_info bits (~XLP_ALL_FLAGS, mirroring
 *	XLogReaderValidatePageHeader); xlp_pageaddr equals the page's own
 *	address (kills recycled-segment stale content); and the spec-4.1
 *	cluster-field predicate cluster_xlog_validate_page_header
 *	(cluster_flags reserved-0, thread range, LEGACY-0 legal, real id
 *	must equal the expected thread).
 */
static inline ClusterRecoveryStreamVerdict
cluster_recovery_stream_page_check(const char *page, uint64 expected_pageaddr,
								   uint16 expected_thread)
{
	const XLogPageHeaderData *hdr = (const XLogPageHeaderData *)page;

	if (hdr->xlp_magic != XLOG_PAGE_MAGIC)
		return CLUSTER_RECOVERY_STREAM_SUSPECT;
	if ((hdr->xlp_info & ~XLP_ALL_FLAGS) != 0)
		return CLUSTER_RECOVERY_STREAM_SUSPECT;
	if (hdr->xlp_pageaddr != (XLogRecPtr)expected_pageaddr)
		return CLUSTER_RECOVERY_STREAM_SUSPECT;
	if (!cluster_xlog_validate_page_header(hdr->xlp_thread_id, hdr->xlp_cluster_flags,
										   expected_thread))
		return CLUSTER_RECOVERY_STREAM_SUSPECT;
	return CLUSTER_RECOVERY_STREAM_OK;
}

/*
 * cluster_recovery_worker_target_page -- locate the last WRITTEN page
 *	from the registry highest_lsn watermark (spec-4.4 P0).
 *
 *	target byte = highest_lsn - 1 (the last byte known written), so a
 *	highest_lsn sitting exactly on a segment boundary resolves to the
 *	LAST page of the PREVIOUS segment -- never into unwritten space.
 *	Returns false when highest_lsn is 0/invalid (nothing written).
 */
static inline bool
cluster_recovery_worker_target_page(uint64 highest_lsn, int wal_segsz_bytes, uint64 *segno_out,
									uint32 *page_offset_out, uint64 *pageaddr_out)
{
	uint64 target;
	XLogSegNo segno;

	if (highest_lsn == 0)
		return false;
	target = highest_lsn - 1;
	XLByteToSeg((XLogRecPtr)target, segno, wal_segsz_bytes);
	*segno_out = (uint64)segno;
	*page_offset_out
		= (uint32)(XLogSegmentOffset((XLogRecPtr)target, wal_segsz_bytes)
				   - (XLogSegmentOffset((XLogRecPtr)target, wal_segsz_bytes) % XLOG_BLCKSZ));
	*pageaddr_out = target - (target % XLOG_BLCKSZ);
	return true;
}

#ifndef FRONTEND

/*
 * Backend API (cluster_recovery_worker.c).  All paths are no-ops when
 * cluster.wal_threads_dir is unset / the thread id is LEGACY.
 */

/* Coordinator (startup process), right after plan generation: spawns
 * min(candidates, cluster.recovery_workers_max) dynamic bgworkers and
 * RETURNS WITHOUT WAITING (recovery is never delayed); no-op when the
 * plan has no crash candidates or the cap is 0.  Spawn failures are
 * fail-open (WARNING + slot FAILED). */
extern void cluster_recovery_workers_launch(void);

/* bgworker entry point (InternalBGWorkers[]); main_arg = slot index. */
extern void cluster_recovery_worker_main(Datum main_arg);

/* Dump-side snapshot of the pool; false before shmem init. */
extern bool cluster_recovery_worker_pool_snapshot(ClusterRecoveryWorkerPool *out);

/* The pool lives inside the spec-4.3 plan shmem wrapper (round-1
 * P1-2): accessor defined in cluster_recovery_plan.c. */
extern ClusterRecoveryWorkerPool *cluster_recovery_worker_pool_ptr(void);

#endif /* !FRONTEND */

#endif /* CLUSTER_RECOVERY_WORKER_H */
