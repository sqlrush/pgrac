/*-------------------------------------------------------------------------
 *
 * cluster_recovery_worker.c
 *	  pgrac Recovery Worker skeleton -- coordinator launch + worker
 *	  main (spec-4.4).
 *
 *	  Launch side (startup process, right after the spec-4.3 plan
 *	  pass): when the plan reports crash candidates, register
 *	  min(candidates, cluster.recovery_workers_max) dynamic background
 *	  workers (BGWORKER_SHMEM_ACCESS only; BgWorkerStart_PostmasterStart
 *	  so they launch during PM_STARTUP) and RETURN WITHOUT WAITING --
 *	  recovery is never delayed; workers run concurrently with the
 *	  node's own-stream replay and only ever read OTHER threads'
 *	  directories.
 *
 *	  Worker side: for each assigned candidate thread, (1) re-read the
 *	  registry slot and re-classify -- a peer that is alive again is
 *	  SKIPPED, never read while it is writing; (2) validate the claim
 *	  file CONTENT (40B, spec-4.1 pure validator); (3) locate the LAST
 *	  WRITTEN page from the slot's highest_lsn watermark and check its
 *	  header (magic + pageaddr + thread stamp -- the pageaddr check
 *	  kills recycled-segment stale content), plus the segment's first
 *	  page; never touch the preallocated tail (spec-4.4 P0).  Verdicts
 *	  land in the shared pool; everything is observational/fail-open
 *	  (spec-4.5 flips non-OK verdicts into the 53RA3 gate).
 *
 *	  Exit discipline (round-1 P1-4): a before_shmem_exit callback
 *	  flips a still-RUNNING slot to FAILED on any abnormal exit
 *	  (ERROR -> proc_exit(1) included), so DONE/FAILED accounting needs
 *	  no coordinator-side reaping in this stage.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/backend/cluster/cluster_recovery_worker.c
 *
 * NOTES
 *	  This is a pgrac-original file (no derivation from PostgreSQL).
 *	  Spec: spec-4.4-recovery-worker-skeleton.md FROZEN v1.0
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#ifdef USE_PGRAC_CLUSTER

#include <unistd.h>

#include "access/xlog.h" /* wal_segment_size */
#include "access/xlog_internal.h"
#include "cluster/cluster_guc.h"
#include "cluster/cluster_recovery_plan.h"
#include "cluster/cluster_recovery_worker.h"
#include "cluster/cluster_semantic_activation.h" /* bit22 cutover latch (contract §B) */
#include "cluster/cluster_wal_thread.h"
#include "miscadmin.h"
#include "postmaster/bgworker.h"
#include "postmaster/bgwriter.h" /* CheckPointTimeout (contract liveness threshold) */
#include "storage/fd.h"
#include "storage/ipc.h"
#include "utils/timestamp.h"
#include "utils/wait_event.h"

/*
 * mark_failed_on_exit -- before_shmem_exit callback (round-1 P1-4).
 *
 *	Any exit path that leaves the slot RUNNING (ERROR/FATAL ->
 *	proc_exit) flips it to FAILED; the normal path has already CAS'd
 *	RUNNING -> DONE by the time this runs.
 */
static void
mark_failed_on_exit(int code, Datum arg)
{
	ClusterRecoveryWorkerPool *pool = cluster_recovery_worker_pool_ptr();
	int slot = DatumGetInt32(arg);
	uint32 expected = CLUSTER_RECOVERY_WORKER_RUNNING;

	if (pool == NULL || slot < 0 || slot >= CLUSTER_RECOVERY_WORKER_MAX_SLOTS)
		return;
	pg_atomic_compare_exchange_u32(&pool->slot_state[slot], &expected,
								   CLUSTER_RECOVERY_WORKER_FAILED);
}

/*
 * validate_claim_content -- spec-4.4 P1-1: the claim is ownership
 *	evidence; existence alone proves nothing.  Reuses the spec-4.1
 *	pure validator (magic/version/crc/thread_id/node_id).
 */
static bool
validate_claim_content(uint16 tid)
{
	char path[MAXPGPATH];
	ClusterWalThreadClaim claim;
	int fd;
	ssize_t nread;
	const char *reason = NULL;

	snprintf(path, sizeof(path), "%s/thread_%u/%s", cluster_wal_threads_dir, (unsigned)tid,
			 CLUSTER_WAL_THREAD_CLAIM_FILENAME);
	fd = BasicOpenFile(path, O_RDONLY | PG_BINARY);
	if (fd < 0)
		return false;
	pgstat_report_wait_start(WAIT_EVENT_CLUSTER_WAL_THREAD_CLAIM_READ);
	nread = pg_pread(fd, &claim, sizeof(claim), 0);
	pgstat_report_wait_end();
	close(fd);
	if (nread != (ssize_t)sizeof(claim))
		return false;
	return cluster_wal_thread_claim_validate(&claim, tid, (int32)tid - 1, &reason);
}

/*
 * check_written_page -- pread one page at a WRITTEN offset and run the
 *	header check.  Returns a stream verdict (OK/SUSPECT/UNREADABLE).
 */
static ClusterRecoveryStreamVerdict
check_written_page(const char *segpath, uint32 page_offset, uint64 expected_pageaddr, uint16 tid)
{
	char page[XLOG_BLCKSZ];
	int fd;
	ssize_t nread;

	fd = BasicOpenFile(segpath, O_RDONLY | PG_BINARY);
	if (fd < 0)
		return CLUSTER_RECOVERY_STREAM_UNREADABLE;
	/* P2: reusing the core WAL-read wait event requires explicit
	 * reporting -- raw pread carries nothing by itself. */
	pgstat_report_wait_start(WAIT_EVENT_WAL_READ);
	nread = pg_pread(fd, page, XLOG_BLCKSZ, (off_t)page_offset);
	pgstat_report_wait_end();
	close(fd);
	if (nread != (ssize_t)XLOG_BLCKSZ)
		return CLUSTER_RECOVERY_STREAM_UNREADABLE;
	return cluster_recovery_stream_page_check(page, expected_pageaddr, tid);
}

/* RF-ROOT P7 (contract §B / follow-up): dual-path by the bit22 cutover
 * latch.  Pre-bit22 (frozen §17.8: wal-state remains the selected
 * authority) the registry-sourced validate_stream below is the revalidate
 * path; post-bit22 the canonical-root validate_stream_from_root is the only
 * path.  The registry variant was removed at G1b step 4 ② and is restored
 * here under the gate idiom (the pre-bit22 branch is the frozen behavior). */
/*
 * validate_stream -- §3.2: claim content + last-written page (from the
 *	re-read slot's highest_lsn) + segment first page.
 */
static ClusterRecoveryStreamVerdict
validate_stream(uint16 tid, const ClusterWalStateSlot *slot)
{
	char fname[MAXFNAMELEN];
	char segpath[MAXPGPATH];
	uint64 segno;
	uint32 page_offset;
	uint64 pageaddr;
	ClusterRecoveryStreamVerdict v;

	if (!validate_claim_content(tid))
		return CLUSTER_RECOVERY_STREAM_SUSPECT;

	if (!cluster_recovery_worker_target_page(slot->highest_lsn, wal_segment_size, &segno,
											 &page_offset, &pageaddr))
		return CLUSTER_RECOVERY_STREAM_UNREADABLE; /* no written bytes */

	/* Segment file name is CONSTRUCTED (never a directory scan; the
	 * claim file would sort after hex segment names -- spec-4.4 P0). */
	XLogFileName(fname, (TimeLineID)slot->tli, (XLogSegNo)segno, wal_segment_size);
	snprintf(segpath, sizeof(segpath), "%s/thread_%u/%s", cluster_wal_threads_dir, (unsigned)tid,
			 fname);

	v = check_written_page(segpath, page_offset, pageaddr, tid);
	if (v != CLUSTER_RECOVERY_STREAM_OK)
		return v;

	/* Cheap extra anchor: the segment's own first page. */
	if (page_offset != 0) {
		uint64 seg_start_addr = (uint64)segno * wal_segment_size;

		v = check_written_page(segpath, 0, seg_start_addr, tid);
		if (v != CLUSTER_RECOVERY_STREAM_OK)
			return v;
	}
	return CLUSTER_RECOVERY_STREAM_OK;
}
/*
 * validate_stream_from_root -- RF-ROOT P7 G1b step 4 (site worker.c:192,
 * Stage 8 contract / follow-up item 4): the §3.2 stream precheck with
 * the canonical root as the only source.  The registry's write-position
 * watermark (highest_lsn) has no root equivalent; the target-page anchor is
 * the canonical validated_tail_lsn_exclusive (the CHECKPOINT_ADVANCE
 * validated extent) with its matching tail_tli.  The precheck range narrows
 * to the checkpoint-validated extent; final stream completeness stays with
 * the replay-side validated_end scan (fail-closed direction — a torn page
 * after the validated boundary is caught there, never silently accepted).
 */
static ClusterRecoveryStreamVerdict
validate_stream_from_root(uint16 tid, const ClusterControlRootSnapshot *snapshot)
{
	char fname[MAXFNAMELEN];
	char segpath[MAXPGPATH];
	uint64 segno;
	uint32 page_offset;
	uint64 pageaddr;
	ClusterRecoveryStreamVerdict v;

	if (!cluster_recovery_worker_root_anchor_valid(snapshot))
		return CLUSTER_RECOVERY_STREAM_UNREADABLE; /* no validated bytes */

	if (!validate_claim_content(tid))
		return CLUSTER_RECOVERY_STREAM_SUSPECT;

	if (!cluster_recovery_worker_target_page(snapshot->validated_tail_lsn_exclusive,
											 wal_segment_size, &segno, &page_offset, &pageaddr))
		return CLUSTER_RECOVERY_STREAM_UNREADABLE; /* no written bytes */

	/* Segment file name is CONSTRUCTED (never a directory scan; the
	 * claim file would sort after hex segment names -- spec-4.4 P0). */
	XLogFileName(fname, (TimeLineID)snapshot->tail_tli, (XLogSegNo)segno, wal_segment_size);
	snprintf(segpath, sizeof(segpath), "%s/thread_%u/%s", cluster_wal_threads_dir, (unsigned)tid,
			 fname);

	v = check_written_page(segpath, page_offset, pageaddr, tid);
	if (v != CLUSTER_RECOVERY_STREAM_OK)
		return v;

	/* Cheap extra anchor: the segment's own first page. */
	if (page_offset != 0) {
		uint64 seg_start_addr = (uint64)segno * wal_segment_size;

		v = check_written_page(segpath, 0, seg_start_addr, tid);
		if (v != CLUSTER_RECOVERY_STREAM_OK)
			return v;
	}
	return CLUSTER_RECOVERY_STREAM_OK;
}

/*
 * cluster_recovery_worker_revalidate -- spec-4.5 Q6 inline path.
 *	The merge coordinator calls this when the worker pool verdict is
 *	NONE/FAILED (workers did not finish in time): re-run the same
 *	validation serially in the startup process.  RF-ROOT P7 (contract §B):
 *	dual-path by the bit22 cutover latch — pre-bit22 the wal-state registry
 *	is the authority (frozen §17.8; the startup process is the CF(S)-capable
 *	recovery admission, AD-023 §4, but the registry read needs no CF);
 *	post-bit22 the canonical control root is the only source (STRONG read
 *	via the contract §A two-step discovered-identity pattern).
 */
ClusterRecoveryStreamVerdict
cluster_recovery_worker_revalidate(uint16 thread_id)
{
	if (cluster_r4_bit22_cutover_active()) {
		ClusterControlRootSnapshot snapshot;
		ClusterControlRootReadToken token;
		ClusterControlRootResult root_result;

		root_result = cluster_control_root_read_canonical_discovered(thread_id, &snapshot, &token);
		if (root_result != CLUSTER_CONTROL_ROOT_OK_PRIMARY
			&& root_result != CLUSTER_CONTROL_ROOT_OK_PRIMARY_DEGRADED)
			return CLUSTER_RECOVERY_STREAM_UNREADABLE;
		return validate_stream_from_root(thread_id, &snapshot);
	}

	/* Pre-bit22 (frozen §17.8): the wal-state registry is the selected
	 * authority — the restored pre-migration shape. */
	{
		ClusterWalStateSlot slot;

		if (cluster_wal_state_read_slot(thread_id, &slot) != CLUSTER_WAL_SLOT_OK)
			return CLUSTER_RECOVERY_STREAM_UNREADABLE;
		return validate_stream(thread_id, &slot);
	}
}

/*
 * cluster_recovery_worker_main -- bgworker entry (InternalBGWorkers).
 */
void
cluster_recovery_worker_main(Datum main_arg)
{
	ClusterRecoveryWorkerPool *pool = cluster_recovery_worker_pool_ptr();
	int slot = DatumGetInt32(main_arg);
	uint32 expected = CLUSTER_RECOVERY_WORKER_REQUESTED;
	uint16 own_thread = cluster_wal_thread_id();
	int64 now_us;
	uint16 tid;
	int n_ok = 0;
	int n_bad = 0;
	int n_skipped = 0;

	/* Returning from a bgworker entry point is a clean exit(0)
	 * (StartBackgroundWorker proc_exits after we return); cppcheck
	 * does not model proc_exit as noreturn, so guards use return. */
	if (pool == NULL || slot < 0 || slot >= CLUSTER_RECOVERY_WORKER_MAX_SLOTS)
		return;

	/* Claim the slot; a stale/raced state means this spawn is moot. */
	if (!pg_atomic_compare_exchange_u32(&pool->slot_state[slot], &expected,
										CLUSTER_RECOVERY_WORKER_RUNNING))
		return;

	before_shmem_exit(mark_failed_on_exit, Int32GetDatum(slot));

	/* Round-2 P1-2: the bgworker framework starts the entry point with
	 * signals blocked; unblock before any file I/O so SIGTERM during a
	 * stuck shared-storage read or a shutdown is delivered promptly
	 * (the default bgworker_die handler FATALs, which lands in the
	 * before_shmem_exit FAILED path above). */
	BackgroundWorkerUnblockSignals();

	now_us = (int64)GetCurrentTimestamp();
	for (tid = 1; tid <= CLUSTER_WAL_STATE_SLOT_COUNT; tid++) {
		ClusterRecoveryStreamVerdict sv;

		if ((pool->assigned_bitmap[slot][(tid - 1) / 64] & ((uint64)1 << ((tid - 1) % 64))) == 0)
			continue;

		/*
		 * RF-ROOT P7 (contract §B / follow-up): dual-path by the bit22
		 * latch.  Pre-bit22 the wal-state registry is the authority
		 * (frozen §17.8; the bgworker needs no CF for registry reads);
		 * post-bit22 the pre-IR pinned projection is consumed.
		 */
		if (cluster_r4_bit22_cutover_active()) {
			ClusterControlRootReadToken pin_token;
			uint64 pin_validated_tail;
			uint64 pin_checkpoint_lower;
			uint64 pin_lifecycle;
			uint32 pin_tail_tli;
			uint32 pin_checkpoint_tli;
			ClusterControlRootSnapshot pin_snapshot;

			/*
			 * RF-ROOT P7 G1b step 4 ②: consume the pre-IR pinned
			 * projection — the startup process STRONG-read the root
			 * before spawning, so this bgworker performs NO CF(S)
			 * inside the episode (follow-up item 2 / §1.3).  A missing
			 * or stale projection fails closed.
			 */
			if (!cluster_thread_recovery_projection_current(
					tid, (uint64)pool->generation, &pin_token, &pin_validated_tail,
					&pin_checkpoint_lower, &pin_lifecycle, &pin_tail_tli, &pin_checkpoint_tli)) {
				sv = CLUSTER_RECOVERY_STREAM_UNREADABLE;
			} else {
				memset(&pin_snapshot, 0, sizeof(pin_snapshot));
				pin_snapshot.identity.origin_thread_id = tid;
				pin_snapshot.identity.origin_node_id = (int32)tid - 1;
				pin_snapshot.lifecycle = (uint32)pin_lifecycle;
				pin_snapshot.validated_tail_lsn_exclusive = pin_validated_tail;
				pin_snapshot.checkpoint_lower_lsn = pin_checkpoint_lower;
				pin_snapshot.tail_tli = pin_tail_tli;
				pin_snapshot.checkpoint_tli = pin_checkpoint_tli;
				(void)pin_token;
				(void)pin_checkpoint_lower;
				(void)pin_checkpoint_tli;

				/*
				 * Re-classify from the pinned lifecycle (ALIVE-biased,
				 * contract): a peer with a recent publication is
				 * SKIPPED.  Only CRASHED_CANDIDATE gets validated.
				 */
				if (cluster_recovery_classify_root_slot(CLUSTER_CONTROL_ROOT_OK_PRIMARY,
														&pin_snapshot, own_thread, tid, now_us,
														CheckPointTimeout)
					!= CLUSTER_RECOVERY_THREAD_CRASHED_CANDIDATE) {
					sv = CLUSTER_RECOVERY_STREAM_SKIPPED;
				} else {
					sv = validate_stream_from_root(tid, &pin_snapshot);
				}
			}
		} else {
			ClusterWalStateSlot reg_slot;
			ClusterWalSlotVerdict v;

			/*
			 * Pre-bit22 (frozen §17.8): the wal-state registry is the
			 * selected authority — the restored pre-migration bgworker
			 * shape (a9be5590d0^).  No CF dependency.
			 */
			v = cluster_wal_state_read_slot(tid, &reg_slot);
			if (v != CLUSTER_WAL_SLOT_OK) {
				sv = CLUSTER_RECOVERY_STREAM_UNREADABLE;
			} else {
				ClusterRecoveryThreadVerdict verdict;

				verdict = cluster_recovery_classify_slot(v, &reg_slot, own_thread, tid, now_us,
														 cluster_recovery_stale_active_ms);
				if (verdict != CLUSTER_RECOVERY_THREAD_CRASHED_CANDIDATE) {
					sv = CLUSTER_RECOVERY_STREAM_SKIPPED;
				} else {
					sv = validate_stream(tid, &reg_slot);
				}
			}
		}

		pool->stream_verdict[tid] = (uint8)sv;
		switch (sv) {
		case CLUSTER_RECOVERY_STREAM_OK:
			n_ok++;
			break;
		case CLUSTER_RECOVERY_STREAM_SKIPPED:
			n_skipped++;
			break;
		default:
			n_bad++;
			break;
		}
		ereport(DEBUG1, (errmsg("recovery stream validation: thread %u verdict %d", (unsigned)tid,
								(int)sv)));
	}

	ereport(LOG, (errmsg("recovery stream validation (not replaying): worker %d, "
						 "%d ok, %d suspect/unreadable, %d skipped",
						 slot, n_ok, n_bad, n_skipped),
				  n_bad > 0 ? errhint("Non-OK streams are reported, not acted upon; check the "
									  "shared WAL storage if they persist.")
							: 0));

	expected = CLUSTER_RECOVERY_WORKER_RUNNING;
	pg_atomic_compare_exchange_u32(&pool->slot_state[slot], &expected,
								   CLUSTER_RECOVERY_WORKER_DONE);
}

/*
 * cluster_recovery_workers_launch -- coordinator side (spec-4.4 §3.1).
 */
void
cluster_recovery_workers_launch(void)
{
	ClusterRecoveryWorkerPool *pool = cluster_recovery_worker_pool_ptr();
	ClusterRecoveryPlan plan;
	int n_workers;
	int slot;
	uint16 tid;
	bool warned = false;

	if (cluster_wal_threads_dir == NULL || cluster_wal_threads_dir[0] == '\0')
		return;
	if (cluster_wal_thread_id() == XLP_THREAD_ID_LEGACY)
		return;
	if (pool == NULL)
		return;
	if (cluster_recovery_workers_max <= 0)
		return;
	if (!cluster_recovery_plan_snapshot(&plan))
		return;
	if (plan.failed || plan.n_crashed_candidate == 0)
		return;

	/* Coordinator-only writers: set up the pool before any spawn. */
	pool->generation++;
	memset(pool->stream_verdict, 0, sizeof(pool->stream_verdict));
	n_workers = cluster_recovery_worker_assign(plan.candidate_bitmap, plan.n_crashed_candidate,
											   cluster_recovery_workers_max, pool->assigned_bitmap);
	pool->workers_requested = (uint16)n_workers;
	for (slot = 0; slot < CLUSTER_RECOVERY_WORKER_MAX_SLOTS; slot++)
		pg_atomic_write_u32(&pool->slot_state[slot], slot < n_workers
														 ? CLUSTER_RECOVERY_WORKER_REQUESTED
														 : CLUSTER_RECOVERY_WORKER_UNUSED);
	if (n_workers == 0)
		return;

	/*
	 * RF-ROOT P7 (contract §B): pin the canonical-root projection for every
	 * plan candidate BEFORE spawning — post-bit22 only (pre-bit22 the
	 * worker_main consumes the registry directly under the gate idiom, so a
	 * projection is never needed).  This runs in the startup process at
	 * pre-IR (zero resource locks — the launch follows the plan pass and
	 * precedes any episode freeze); the workers then consume ONLY the
	 * pinned fields and never re-acquire CF(S) (follow-up item 2).  A pin
	 * failure keeps that thread fail-closed: the worker's projection_current
	 * will refuse it (UNREADABLE verdict).
	 */
	for (tid = XLP_THREAD_ID_FIRST_REAL; tid <= CLUSTER_WAL_THREAD_MAX; tid++) {
		if ((plan.candidate_bitmap[(tid - 1) / 64] & (UINT64_C(1) << ((tid - 1) % 64))) == 0)
			continue;
		if (cluster_r4_bit22_cutover_active()) {
			(void)cluster_thread_recovery_pin_projection(tid, (uint64)pool->generation);
		}
	}

	for (slot = 0; slot < n_workers; slot++) {
		BackgroundWorker bgw;
		BackgroundWorkerHandle *handle = NULL;

		memset(&bgw, 0, sizeof(bgw));
		/* Q1 conditions: SHMEM_ACCESS only -- no database connection,
		 * no parallel-class accounting. */
		bgw.bgw_flags = BGWORKER_SHMEM_ACCESS;
		bgw.bgw_start_time = BgWorkerStart_PostmasterStart;
		bgw.bgw_restart_time = BGW_NEVER_RESTART;
		strlcpy(bgw.bgw_library_name, "postgres", sizeof(bgw.bgw_library_name));
		strlcpy(bgw.bgw_function_name, "cluster_recovery_worker_main",
				sizeof(bgw.bgw_function_name));
		snprintf(bgw.bgw_name, sizeof(bgw.bgw_name), "pgrac recovery worker %d", slot);
		strlcpy(bgw.bgw_type, "cluster recovery worker", sizeof(bgw.bgw_type));
		bgw.bgw_main_arg = Int32GetDatum(slot);
		bgw.bgw_notify_pid = 0;

		if (!RegisterDynamicBackgroundWorker(&bgw, &handle)) {
			uint32 expected = CLUSTER_RECOVERY_WORKER_REQUESTED;

			/* Round-2 P2: registration failure means no worker ever
			 * existed -- distinct terminal state so the dump never
			 * counts it as started. */
			pg_atomic_compare_exchange_u32(&pool->slot_state[slot], &expected,
										   CLUSTER_RECOVERY_WORKER_SPAWN_FAILED);
			if (!warned) {
				warned = true;
				ereport(WARNING,
						(errmsg("could not register recovery stream-validation worker %d", slot),
						 errhint("Background worker slots are exhausted "
								 "(max_worker_processes); stream validation is observational "
								 "and startup continues.")));
			}
		}
	}

	ereport(LOG, (errmsg("recovery stream validation: launched %d worker(s) for %u crash "
						 "candidate(s) (not waiting; recovery proceeds)",
						 n_workers, (unsigned)plan.n_crashed_candidate)));
}

/*
 * cluster_recovery_worker_pool_snapshot -- dump-side copy.
 *
 *	Loose copy: slot_state values are read individually (atomic reads);
 *	stream_verdict bytes may show a mid-flight mix -- acceptable for an
 *	observational surface (counts are derived on read, round-1 P1-3).
 */
bool
cluster_recovery_worker_pool_snapshot(ClusterRecoveryWorkerPool *out)
{
	ClusterRecoveryWorkerPool *pool = cluster_recovery_worker_pool_ptr();
	int slot;

	if (pool == NULL)
		return false;
	memcpy(out, pool, sizeof(ClusterRecoveryWorkerPool));
	for (slot = 0; slot < CLUSTER_RECOVERY_WORKER_MAX_SLOTS; slot++)
		pg_atomic_write_u32(&out->slot_state[slot], pg_atomic_read_u32(&pool->slot_state[slot]));
	return true;
}

#else /* !USE_PGRAC_CLUSTER */

/* Disable-cluster build: this file compiles to nothing. */

#endif /* USE_PGRAC_CLUSTER */
