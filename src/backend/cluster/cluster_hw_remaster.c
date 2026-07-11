/*-------------------------------------------------------------------------
 *
 * cluster_hw_remaster.c
 *	  HW (relation extend) authority online-remaster rebuild (spec-5.7 D3 S5,
 *	  §3.1b R4/R6/R9).
 *
 *	  A survivor that has just adopted a dead master's shards rebuilds the
 *	  adopted (rel,fork) HWMs from durable state before it serves HW_ALLOC for
 *	  them (the §3.1b serve gate keeps them fail-closed until cluster_hw_mark_
 *	  shard_rebuilt opens it):
 *
 *	    1. load the dead master's HW snapshot (cluster_hw_snapshot_read) and
 *	       apply each entry the survivor now masters (raise-to-max);
 *	    2. replay the dead master's HW_RESERVE WAL tail from the snapshot_lsn up
 *	       to the validated complete-record boundary, applying each reservation
 *	       the survivor now masters -- so a reservation made after the dead
 *	       master's last checkpoint (durably flushed before reply, R5) is not
 *	       lost (R3: rebuild = max(snapshot, tail >= snapshot_lsn));
 *	    3. durably write the survivor's own ADOPTION snapshot (R9 lineage);
 *	    4. mark the adopted shards rebuilt for the current remaster generation.
 *
 *	  Any condition that cannot PROVE the HWM is fully rebuilt fails closed
 *	  (CLUSTER_HW_REMASTER_BLOCKED): the shard stays frozen, a later HW_ALLOC
 *	  raises 53RA6, NEVER an auto-create at block 0 over an already-allocated
 *	  range (R6, 8.A).  The dead master's WAL tail is read with a minimal
 *	  per-thread file reader (the same shape as the spec-4.11 thread-recovery
 *	  driver) so the HW rebuild stays independent of the default-off online
 *	  thread recovery feature.
 *
 *	  Scope: 2-node (a single survivor adopts every dead shard, so the whole
 *	  dead-master tail is replayed; multi-survivor shard-split is forward).  The
 *	  WAL-tail scan can be large, so the GRD reconfig FSM drives this off the
 *	  LMON heartbeat tick in a dedicated rebuild worker (S5d) and gates P7 on it.
 *
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/backend/cluster/cluster_hw_remaster.c
 *
 * NOTES
 *	  This is a pgrac-original file (no derivation from PostgreSQL).
 *	  Spec: spec-5.7-misc-enqueue-classes.md (D3, §3.1b R4/R6/R9)
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
#include "postmaster/bgworker.h"
#include "storage/fd.h"
#include "storage/ipc.h"
#include "utils/wait_event.h"

#include "cluster/cluster_conf.h" /* CLUSTER_MAX_NODES */
#include "cluster/cluster_grd.h"
#include "cluster/cluster_guc.h" /* cluster_node_id, cluster_wal_threads_dir */
#include "cluster/cluster_hw.h"
#include "cluster/cluster_hw_remaster.h"
#include "cluster/cluster_hw_snapshot.h"
#include "cluster/cluster_thread_recovery.h"   /* validated_end (torn-tail boundary) */
#include "cluster/cluster_wal_state.h"		   /* read_slot (durable watermark) */
#include "cluster/cluster_wal_thread.h"		   /* node id -> thread id */
#include "cluster/storage/cluster_undo_xlog.h" /* xl_hw_reserve, XLOG_HW_RESERVE */

/* One worker process owns exactly one dead origin / episode. */
static int hw_worker_dead_node = -1;
static uint64 hw_worker_episode = 0;
static bool hw_worker_armed = false;

static uint64
hw_remaster_next_attempt_deadline(uint32 completed_retry_attempts)
{
	uint32 backoff_ms;
	TimestampTz deadline;

	backoff_ms = cluster_hw_remaster_compute_backoff_ms(cluster_hw_remaster_retry_backoff_ms,
														completed_retry_attempts);
	deadline = TimestampTzPlusMilliseconds(GetCurrentTimestamp(), (int)backoff_ms);
	return (uint64)deadline;
}

static void
hw_remaster_record_terminal(int dead_node, ClusterHwRemasterResult res)
{
	/* Amendment v1.2 (R6): the worker stores next_attempt_at BEFORE the
	 * terminal result, and the LMON relaunch decision reads them in the
	 * opposite order (result first).  Order the two stores so a decider
	 * that observes the terminal result also observes the matching backoff
	 * deadline — without the fence a weakly-ordered CPU could let it pair a
	 * fresh BLOCKED with a stale (zero) deadline and skip one backoff wait
	 * (bounded self-correcting, but cheap to close outright).  r3-P3(b):
	 * paired with the pg_read_barrier() between the result and deadline
	 * loads in cluster_hw_remaster_launch_workers (acquire/release pair). */
	if (res == CLUSTER_HW_REMASTER_DONE) {
		cluster_hw_remaster_set_next_attempt_at(dead_node, 0);
		pg_memory_barrier();
		cluster_hw_remaster_set_result(dead_node, res);
		cluster_hw_bump_remaster_done();
	} else if (res == CLUSTER_HW_REMASTER_BLOCKED) {
		uint32 attempts = cluster_hw_remaster_attempts(dead_node);

		cluster_hw_remaster_set_next_attempt_at(dead_node,
												hw_remaster_next_attempt_deadline(attempts));
		pg_memory_barrier();
		cluster_hw_remaster_set_result(dead_node, res);
		cluster_hw_bump_remaster_blocked();
	} else if (res == CLUSTER_HW_REMASTER_BLOCKED_STRUCTURAL) {
		/* Amendment v1.2 (R7): leave next_attempt_at at 0 (not NO_DEADLINE)
		 * so the FSM's next tick takes the MARK_STRUCTURAL decision branch
		 * and emits the episode-once operator WARNING + errhint — covering
		 * the SIGHUP race where only the WORKER (not the launch-side
		 * precheck) discovers the structural cause.  MARK_STRUCTURAL then
		 * stamps NO_DEADLINE, making the WARNING once-per-episode. */
		cluster_hw_remaster_set_next_attempt_at(dead_node, 0);
		pg_memory_barrier();
		cluster_hw_remaster_set_result(dead_node, res);
		cluster_hw_bump_remaster_blocked();
	} else if (res == CLUSTER_HW_REMASTER_NOT_APPLICABLE) {
		cluster_hw_remaster_set_next_attempt_at(dead_node, 0);
		pg_memory_barrier();
		cluster_hw_remaster_set_result(dead_node, res);
	}
}

static void
hw_remaster_mark_blocked_on_exit(int code pg_attribute_unused(), Datum arg)
{
	int dead_node = DatumGetInt32(arg);

	if (!hw_worker_armed)
		return;
	if (dead_node != hw_worker_dead_node)
		return;
	if (cluster_hw_remaster_launched_episode(dead_node) != hw_worker_episode)
		return;
	if (cluster_hw_remaster_result(dead_node) != CLUSTER_HW_REMASTER_RUNNING)
		return;

	cluster_hw_bump_failclosed();
	hw_remaster_record_terminal(dead_node, CLUSTER_HW_REMASTER_BLOCKED);
}


/* ============================================================
 * Minimal per-thread WAL reader over a dead origin's WAL stream.
 *
 *	Mirrors the spec-4.11 thread-recovery driver's file reader (a dead thread's
 *	stream is not this node's running WAL, so there is no XLogCtl dependency),
 *	kept local so the HW rebuild does not depend on the default-off thread-
 *	recovery worker.
 * ============================================================ */

typedef struct HwWalReadPrivate {
	int seg_fd; /* currently open segment fd, -1 = none */
	XLogSegNo seg_no;
	char dir[MAXPGPATH];
} HwWalReadPrivate;

static void
/* cppcheck-suppress constParameterCallback */
hw_wal_segment_open(XLogReaderState *state, XLogSegNo nextSegNo, TimeLineID *tli_p)
{
	HwWalReadPrivate *p = (HwWalReadPrivate *)state->private_data;
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
		/* A MISSING next segment is end-of-stream (the dead thread can stop at a
		 * segment boundary): leave seg_fd = -1 so hw_wal_page_read returns -1 and
		 * the scan stops.  The caller then checks recovered_through against the
		 * validated boundary -- a short stream fails closed (8.A).  A non-ENOENT
		 * failure (real I/O error) is fatal. */
		if (errno == ENOENT)
			return;
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not open WAL segment \"%s\" for HW remaster rebuild: %m", fpath)));
	}
	p->seg_no = nextSegNo;
	state->seg.ws_file = p->seg_fd;
}

static void
hw_wal_segment_close(XLogReaderState *state)
{
	HwWalReadPrivate *p = (HwWalReadPrivate *)state->private_data;

	if (p->seg_fd >= 0) {
		close(p->seg_fd);
		p->seg_fd = -1;
	}
	state->seg.ws_file = -1;
}

static int
hw_wal_page_read(XLogReaderState *state, XLogRecPtr targetPagePtr, int reqLen,
				 XLogRecPtr targetRecPtr, char *readBuf)
{
	HwWalReadPrivate *p = (HwWalReadPrivate *)state->private_data;
	XLogSegNo segno;
	uint32 offset;
	int nread;

	XLByteToSeg(targetPagePtr, segno, state->segcxt.ws_segsize);
	if (p->seg_fd < 0 || segno != p->seg_no)
		hw_wal_segment_open(state, segno, &state->seg.ws_tli);
	if (p->seg_fd < 0)
		return -1; /* missing segment -> stream end */

	offset = XLogSegmentOffset(targetPagePtr, state->segcxt.ws_segsize);
	pgstat_report_wait_start(WAIT_EVENT_WAL_READ);
	nread = pg_pread(p->seg_fd, readBuf, XLOG_BLCKSZ, (off_t)offset);
	pgstat_report_wait_end();
	if (nread != XLOG_BLCKSZ)
		return -1; /* torn / short -> stream end */
	return XLOG_BLCKSZ;
}

/*
 * hw_wal_reader_make -- allocate a reader over a dead origin's per-thread WAL dir
 * (<cluster.wal_threads_dir>/thread_<tid>).  Returns the reader (with *priv_out
 * set) or NULL on any fail-closed condition (bad tid, unset/missing dir, alloc
 * failure).  On NULL the private state is freed; on success the caller owns both
 * and must close priv->seg_fd, XLogReaderFree(reader) and pfree(priv).  Not
 * positioned -- the caller runs XLogFindNextRecord.
 */
static XLogReaderState *
hw_wal_reader_make(uint16 dead_tid, HwWalReadPrivate **priv_out)
{
	HwWalReadPrivate *priv;
	XLogReaderState *reader;
	struct stat dirstat;
	int n;

	*priv_out = NULL;

	if (dead_tid < XLP_THREAD_ID_FIRST_REAL || dead_tid > CLUSTER_WAL_THREAD_MAX)
		return NULL;
	if (cluster_wal_threads_dir == NULL || cluster_wal_threads_dir[0] == '\0')
		return NULL;

	priv = (HwWalReadPrivate *)palloc0(sizeof(HwWalReadPrivate));
	priv->seg_fd = -1;
	n = snprintf(priv->dir, sizeof(priv->dir), "%s/thread_%u", cluster_wal_threads_dir,
				 (unsigned)dead_tid);
	if (n < 0 || n >= (int)sizeof(priv->dir)) {
		pfree(priv);
		return NULL;
	}
	if (stat(priv->dir, &dirstat) != 0 || !S_ISDIR(dirstat.st_mode)) {
		pfree(priv);
		return NULL; /* missing source -> fail-closed */
	}

	reader = XLogReaderAllocate(wal_segment_size, priv->dir,
								XL_ROUTINE(.page_read = hw_wal_page_read,
										   .segment_open = hw_wal_segment_open,
										   .segment_close = hw_wal_segment_close),
								priv);
	if (reader == NULL) {
		pfree(priv);
		return NULL;
	}
	/* Single-node stand-in (L239): the local insertion timeline is the dead
	 * thread's timeline on one machine; the cross-node TLI is forward. */
	reader->seg.ws_tli = GetWALInsertionTimeLine();

	*priv_out = priv;
	return reader;
}

static void
hw_wal_reader_free(XLogReaderState *reader, HwWalReadPrivate *priv)
{
	if (priv != NULL && priv->seg_fd >= 0) {
		close(priv->seg_fd);
		priv->seg_fd = -1;
	}
	if (reader != NULL)
		XLogReaderFree(reader);
	if (priv != NULL)
		pfree(priv);
}


/* ============================================================
 * HW_RESERVE WAL-tail apply.
 * ============================================================ */

/*
 * hw_apply_reserve_record -- apply one decoded HW_RESERVE record to the local
 * authority IF the survivor now masters that resid (raise-to-max).  In 2-node a
 * single survivor masters every dead resid, so the master filter is a no-op; it
 * is kept so the merge is correct under a future multi-survivor shard split (a
 * resid the survivor does not master is another survivor's to rebuild).
 */
static void
hw_apply_reserve_record(XLogReaderState *record)
{
	const xl_hw_reserve *rec;
	RelFileLocator rloc;
	ClusterResId resid;

	if (XLogRecGetDataLen(record) != sizeof(*rec))
		return; /* malformed: skip (the boundary scan validated record framing) */
	rec = (const xl_hw_reserve *)XLogRecGetData(record);

	rloc.spcOid = rec->spcOid;
	rloc.dbOid = rec->dbOid;
	rloc.relNumber = rec->relNumber;
	cluster_hw_resid_encode(rloc, (ForkNumber)rec->fork, &resid);

	if (cluster_grd_lookup_master(&resid) == cluster_node_id)
		cluster_hw_apply_hwm(&resid, (BlockNumber)rec->new_hwm);
}

/*
 * hw_scan_reserve_tail -- decode the dead origin's WAL from `lower` and apply
 * every HW_RESERVE up to `upper` (the validated complete-record boundary).
 * Returns true iff the scan reached `upper` cleanly; false (fail-closed) on a
 * reader/positioning failure or a stream that stops short of `upper` (a mid-
 * stream gap, never a silent truncation -- 8.A).  `upper` was produced by
 * cluster_thread_recovery_validated_end over the same stream, so a complete
 * stream re-decodes to exactly there.
 */
static bool
hw_scan_reserve_tail(uint16 dead_tid, XLogRecPtr lower, XLogRecPtr upper)
{
	HwWalReadPrivate *priv = NULL;
	XLogReaderState *reader;
	XLogRecPtr first_rec;
	XLogRecPtr recovered = InvalidXLogRecPtr;
	bool reached = false;

	reader = hw_wal_reader_make(dead_tid, &priv);
	if (reader == NULL)
		return false;

	first_rec = XLogFindNextRecord(reader, lower);
	if (XLogRecPtrIsInvalid(first_rec)) {
		hw_wal_reader_free(reader, priv);
		return false;
	}

	for (;;) {
		char *errormsg = NULL;
		const XLogRecord *record = XLogReadRecord(reader, &errormsg);

		if (record == NULL)
			break; /* clean end-of-stream or read error -- checked against upper below */

		if (XLogRecGetRmid(reader) == RM_CLUSTER_UNDO_ID
			&& (XLogRecGetInfo(reader) & ~XLR_INFO_MASK) == XLOG_HW_RESERVE)
			hw_apply_reserve_record(reader);

		recovered = reader->EndRecPtr;
		if (recovered >= upper) {
			reached = true;
			break;
		}
	}

	hw_wal_reader_free(reader, priv);

	/* The validated boundary must be reached; a stream that stops short means a
	 * gap (missing segment) below the durable watermark -> fail-closed (8.A). */
	return reached || (!XLogRecPtrIsInvalid(recovered) && recovered >= upper);
}


/* ============================================================
 * Online-remaster rebuild for one dead origin.
 * ============================================================ */

ClusterHwRemasterResult
cluster_hw_remaster_rebuild_origin(int dead_node_id, uint64 episode_epoch)
{
	uint16 dead_tid;
	ClusterHwSnapshotHeader hdr;
	ClusterHwSnapshotEntry *entries;
	ClusterHwSnapshotValidity v;
	ClusterWalStateSlot slot;
	XLogRecPtr lower;
	XLogRecPtr validated_min;
	XLogRecPtr upper = InvalidXLogRecPtr;
	uint32 i;
	uint32 shard;
	uint32 marked = 0;

	if (!cluster_hw_authority_active())
		return CLUSTER_HW_REMASTER_NOT_APPLICABLE;
	if (dead_node_id < 0 || dead_node_id == cluster_node_id)
		return CLUSTER_HW_REMASTER_NOT_APPLICABLE;
	if (!cluster_hw_remaster_recoverable()) {
		cluster_hw_bump_failclosed();
		ereport(LOG, (errmsg("cluster HW remaster: cluster.wal_threads_dir is not configured; "
							 "adopted shards stay fail-closed"),
					  errhint("Set cluster.wal_threads_dir to the shared per-thread WAL root and "
							  "restart, or restart the dead node so a JOIN rebuild can replace "
							  "the failure-driven remaster.")));
		return CLUSTER_HW_REMASTER_BLOCKED_STRUCTURAL;
	}

	dead_tid = cluster_wal_thread_id_for(true, dead_node_id);
	if (dead_tid < XLP_THREAD_ID_FIRST_REAL || dead_tid > CLUSTER_WAL_THREAD_MAX) {
		cluster_hw_bump_failclosed();
		ereport(LOG, (errmsg("cluster HW remaster: dead node %d maps to invalid WAL thread id %u; "
							 "adopted shards stay fail-closed",
							 dead_node_id, (unsigned)dead_tid)));
		return CLUSTER_HW_REMASTER_BLOCKED;
	}

	entries = (ClusterHwSnapshotEntry *)palloc(sizeof(ClusterHwSnapshotEntry)
											   * CLUSTER_HW_AUTHORITY_MAX);

	/*
	 * Step 1 (R3/R6): load the dead master's HW snapshot.  A missing / corrupt /
	 * foreign snapshot cannot anchor the rebuild -> fail-closed, never auto-create
	 * at 0 (the dead master, having served HW_ALLOC, must have a durable snapshot;
	 * a present-and-completed checkpoint guarantees one -- §3.1b R1/R6).
	 *
	 * Cross-node identity (§3.1c S7 note): the snapshot's system_id is the WRITER
	 * (dead) node's GetSystemIdentifier(), which a survivor does NOT share unless
	 * the cluster runs the shared pg_control authority (spec-5.6, opt-in).  The
	 * cluster boundary here is the operator-configured shared_data_dir, and the
	 * snapshot is uniquely identified within it by owner_node_id + shard +
	 * generation -- so we pass expected_sysid = 0 to skip only the per-node
	 * system_id match and keep the owner/shard identity check (which catches a
	 * mis-bound file).  A storage-swap guard (the spec-5.6 shared-storage UUID) is
	 * a forward hardening.
	 */
	v = cluster_hw_snapshot_read((uint32)dead_node_id, /*expected_sysid=*/0, (uint32)dead_node_id,
								 (uint32)dead_node_id, &hdr, entries, CLUSTER_HW_AUTHORITY_MAX);
	if (v != CLUSTER_HW_SNAPSHOT_VALID) {
		pfree(entries);
		cluster_hw_bump_failclosed();
		ereport(LOG, (errmsg("cluster HW remaster: dead node %d snapshot unusable (status %d); "
							 "adopted shards stay fail-closed",
							 dead_node_id, (int)v)));
		return CLUSTER_HW_REMASTER_BLOCKED;
	}

	for (i = 0; i < hdr.n_entries; i++) {
		if (cluster_grd_lookup_master(&entries[i].resid) == cluster_node_id)
			cluster_hw_apply_hwm(&entries[i].resid, entries[i].next_hwm);
	}
	lower = (XLogRecPtr)hdr.snapshot_lsn;
	pfree(entries);

	/*
	 * Step 2 (R3/R5): replay the HW_RESERVE tail from the snapshot_lsn up to the
	 * validated complete-record boundary.  validated_min = the dead thread's
	 * registry durable watermark (a decode that stops below it is mid-stream
	 * corruption, not a torn tail); validated_end derives the boundary and fails
	 * closed otherwise (mirrors the spec-4.11 replay_one window contract).
	 */
	if (cluster_wal_state_read_slot(dead_tid, &slot) != CLUSTER_WAL_SLOT_OK
		|| slot.highest_lsn == 0) {
		cluster_hw_bump_failclosed();
		ereport(LOG, (errmsg("cluster HW remaster: dead node %d WAL state slot is unusable; "
							 "adopted shards stay fail-closed",
							 dead_node_id)));
		return CLUSTER_HW_REMASTER_BLOCKED;
	}
	validated_min = (XLogRecPtr)slot.highest_lsn;

	if (cluster_thread_recovery_validated_end(dead_tid, lower, validated_min, &upper)
		!= CLUSTER_THREADREC_DONE) {
		cluster_hw_bump_failclosed();
		ereport(LOG, (errmsg("cluster HW remaster: dead node %d validated WAL end is unavailable; "
							 "adopted shards stay fail-closed",
							 dead_node_id)));
		return CLUSTER_HW_REMASTER_BLOCKED;
	}

	if (!hw_scan_reserve_tail(dead_tid, lower, upper)) {
		cluster_hw_bump_failclosed();
		ereport(LOG, (errmsg("cluster HW remaster: dead node %d HW_RESERVE tail incomplete; "
							 "adopted shards stay fail-closed",
							 dead_node_id)));
		return CLUSTER_HW_REMASTER_BLOCKED;
	}

	/*
	 * Step 3 (R9): durably write this survivor's ADOPTION snapshot BEFORE marking
	 * any shard rebuilt, so a chained remaster reads one collapsed anchor and the
	 * inherited HWM is never lost.  PANICs on a shared-storage I/O failure (the
	 * checkpoint snapshot write does the same).
	 */
	cluster_hw_snapshot_adoption_write();

	/*
	 * Staleness guard (R9 / L235): a second reconfig that started DURING this
	 * rebuild may have re-mastered these shards and bumped their generation.
	 * Marking them rebuilt for the now-current generation would open the serve
	 * gate over HWMs rebuilt for the OLD generation -> a possible stale-HWM serve
	 * (8.A).  If the locked episode advanced, do NOT mark: return BLOCKED and let
	 * the new episode relaunch the rebuild.  The adoption snapshot just written is
	 * harmless (it is this owner's current full-htab dump, valid for whatever it
	 * masters).  Episode-stable => the per-shard generation read below is exactly
	 * the one this rebuild reflects.
	 */
	if (cluster_grd_redeclare_episode_epoch() != episode_epoch) {
		cluster_hw_bump_failclosed();
		ereport(LOG,
				(errmsg("cluster HW remaster: episode advanced during rebuild of dead node %d; "
						"not marking shards (relaunch under the new episode)",
						dead_node_id)));
		return CLUSTER_HW_REMASTER_BLOCKED;
	}

	/*
	 * Step 4 (R4 gate ③): mark every shard this survivor now masters that is
	 * still REBUILDING (adopted this episode) rebuilt for its current generation.
	 * The serve gate (cluster_hw_serve_allowed) then opens once P7 sets NORMAL.
	 * In 2-node every REBUILDING shard belongs to the single survivor; the master
	 * filter keeps it correct under a future multi-survivor split.
	 */
	for (shard = 0; shard < PGRAC_GRD_SHARD_COUNT; shard++) {
		if (cluster_grd_shard_phase(shard) == GRD_SHARD_REBUILDING
			&& cluster_grd_shard_master(shard) == cluster_node_id) {
			cluster_hw_mark_shard_rebuilt(shard, cluster_grd_shard_master_generation(shard));
			marked++;
		}
	}

	ereport(LOG,
			(errmsg("cluster HW remaster: rebuilt authority from dead node %d (snapshot %X/%X, "
					"validated end %X/%X); marked %u adopted shard(s) rebuilt",
					dead_node_id, LSN_FORMAT_ARGS(lower), LSN_FORMAT_ARGS(upper), marked)));
	return CLUSTER_HW_REMASTER_DONE;
}


/* ============================================================
 * Dedicated rebuild worker + GRD reconfig FSM launch / P7 gate (S5d).
 * ============================================================ */

void
cluster_hw_remaster_worker_main(Datum main_arg)
{
	int dead_node = DatumGetInt32(main_arg);
	uint64 episode;
	ClusterHwRemasterResult res;

	/* The bgworker framework starts the entry point with signals blocked; unblock
	 * before any I/O so a SIGTERM during a stuck shared-storage read / shutdown is
	 * delivered (the default die handler FATALs into the BLOCKED exit callback). */
	BackgroundWorkerUnblockSignals();

	/* Capture the live reconfig episode this rebuild is for; rebuild_origin uses
	 * it as the staleness fence before it marks any shard rebuilt. */
	episode = cluster_grd_redeclare_episode_epoch();
	hw_worker_dead_node = dead_node;
	hw_worker_episode = episode;
	hw_worker_armed = true;
	before_shmem_exit(hw_remaster_mark_blocked_on_exit, Int32GetDatum(dead_node));

	res = cluster_hw_remaster_rebuild_origin(dead_node, episode);
	hw_remaster_record_terminal(dead_node, res);
	hw_worker_armed = false;

	ereport(LOG, (errmsg("cluster HW remaster worker: dead node %d -> %s", dead_node,
						 cluster_hw_remaster_result_name(res))));
	/* Returning is a clean exit(0).  On BLOCKED / abnormal exit the adopted shards
	 * stay unmarked, so the serve gate keeps them fail-closed (8.A); the terminal
	 * result lets the LMON FSM retry within the same episode when appropriate. */
}

/*
 * register_one_worker -- register one dynamic bgworker for dead origin `node`.
 * Returns false when registration fails (bgworker slots exhausted) so the caller
 * reverts the launch claim and retries on a later tick.
 */
static bool
register_one_worker(int node)
{
	BackgroundWorker bgw;
	BackgroundWorkerHandle *handle = NULL;

	memset(&bgw, 0, sizeof(bgw));
	bgw.bgw_flags = BGWORKER_SHMEM_ACCESS; /* no DB connection: raw resid + WAL reader */
	bgw.bgw_start_time = BgWorkerStart_PostmasterStart;
	bgw.bgw_restart_time = BGW_NEVER_RESTART;
	strlcpy(bgw.bgw_library_name, "postgres", sizeof(bgw.bgw_library_name));
	strlcpy(bgw.bgw_function_name, "cluster_hw_remaster_worker_main",
			sizeof(bgw.bgw_function_name));
	snprintf(bgw.bgw_name, sizeof(bgw.bgw_name), "pgrac hw remaster %d", node);
	strlcpy(bgw.bgw_type, "cluster hw remaster", sizeof(bgw.bgw_type));
	bgw.bgw_main_arg = Int32GetDatum(node);
	bgw.bgw_notify_pid = 0;

	return RegisterDynamicBackgroundWorker(&bgw, &handle);
}

void
cluster_hw_remaster_launch_workers(const uint64 *dead, int nwords, uint64 episode_epoch)
{
	int node;
	int max_node;
	uint64 now;

	if (dead == NULL || nwords <= 0)
		return;
	/*
	 * Only when the HW authority is engaged (multi-node + shared storage): that is
	 * the only configuration where a relation extend is globalized and a survivor
	 * must rebuild an adopted HWM.  Independent of cluster.online_thread_recovery
	 * (default-off), so post-reconfig extends work out of the box (the default-on
	 * HW authority's recovery is itself default-on).  A non-HW reconfig is a no-op.
	 */
	if (!cluster_hw_authority_active())
		return;

	max_node = nwords * 64;
	if (max_node > CLUSTER_MAX_NODES)
		max_node = CLUSTER_MAX_NODES;
	now = (uint64)GetCurrentTimestamp();

	for (node = 0; node < max_node; node++) {
		ClusterHwRemasterResult result;
		ClusterHwRemasterRelaunchDecision d;
		uint64 launched;
		uint64 next_attempt_at;
		uint32 attempts;

		if ((dead[node / 64] & (UINT64CONST(1) << (node % 64))) == 0)
			continue;
		if (node == cluster_node_id)
			continue; /* self is never its own dead origin */

		launched = cluster_hw_remaster_launched_episode(node);
		if (!cluster_hw_remaster_recoverable() && launched != episode_epoch) {
			cluster_hw_remaster_set_launched(node, episode_epoch);
			cluster_hw_remaster_set_attempts(node, 0);
			cluster_hw_remaster_set_next_attempt_at(node, 0);
			cluster_hw_remaster_set_result(node, CLUSTER_HW_REMASTER_BLOCKED_STRUCTURAL);
			launched = episode_epoch;
		}

		result = cluster_hw_remaster_result(node);
		/* r3-P3(b) — read-side pairing for the hw_remaster_record_terminal
		 * write fence (deadline stored BEFORE the terminal result, with
		 * pg_memory_barrier between).  Load the result FIRST, fence, then the
		 * deadline: a decider that observed a terminal result is then
		 * guaranteed to observe the matching backoff deadline.  Without this
		 * the release fence is one-sided and a weakly-ordered reader could
		 * still pair a fresh BLOCKED with a stale (zero) deadline.  attempts
		 * needs no fence: it is written only by this FSM (single-writer
		 * LMON). */
		pg_read_barrier();
		attempts = cluster_hw_remaster_attempts(node);
		next_attempt_at = cluster_hw_remaster_next_attempt_at(node);
		d = cluster_hw_remaster_relaunch_decide(launched, episode_epoch, result, attempts,
												next_attempt_at, now,
												cluster_hw_remaster_retry_max_attempts);

		if (d.action == CLUSTER_HW_REMASTER_LAUNCH_MARK_STRUCTURAL) {
			cluster_hw_remaster_set_next_attempt_at(node, d.next_attempt_at);
			ereport(WARNING,
					(errcode(ERRCODE_CLUSTER_GRD_SHARD_REMASTERING),
					 errmsg("cluster HW remaster is structurally blocked for dead node %d; "
							"adopted shards stay fail-closed",
							node),
					 errhint("Set cluster.wal_threads_dir to the shared per-thread WAL root "
							 "and restart, or restart the dead node so a JOIN rebuild can "
							 "replace the failure-driven remaster.")));
			continue;
		}
		if (d.action == CLUSTER_HW_REMASTER_LAUNCH_MARK_EXHAUSTED) {
			cluster_hw_remaster_set_next_attempt_at(node, d.next_attempt_at);
			cluster_hw_bump_remaster_retry_exhausted();
			ereport(WARNING,
					(errcode(ERRCODE_CLUSTER_GRD_SHARD_REMASTERING),
					 errmsg("cluster HW remaster retries exhausted for dead node %d after %u "
							"attempt(s); adopted shards stay fail-closed",
							node, attempts),
					 errhint("Fix the shared HW snapshot/WAL source, then raise "
							 "cluster.hw_remaster_retry_max_attempts with SIGHUP to resume "
							 "same-episode retrying.")));
			continue;
		}
		if (d.action != CLUSTER_HW_REMASTER_LAUNCH_INITIAL
			&& d.action != CLUSTER_HW_REMASTER_LAUNCH_RETRY)
			continue;

		cluster_hw_remaster_set_launched(node, episode_epoch);
		cluster_hw_remaster_set_attempts(node, d.next_attempts);
		cluster_hw_remaster_set_next_attempt_at(node, 0);
		cluster_hw_remaster_set_result(node, CLUSTER_HW_REMASTER_RUNNING);
		if (!register_one_worker(node)) {
			cluster_hw_remaster_set_launched(
				node, d.action == CLUSTER_HW_REMASTER_LAUNCH_INITIAL ? 0 : launched);
			cluster_hw_remaster_set_attempts(node, attempts);
			cluster_hw_remaster_set_next_attempt_at(node, next_attempt_at);
			cluster_hw_remaster_set_result(node, result);
			ereport(WARNING,
					(errmsg("could not register HW remaster worker for dead node %d", node),
					 errhint("Background worker slots are exhausted (max_worker_processes); the "
							 "adopted shards stay fail-closed until the rebuild can run.")));
			continue;
		}
		if (d.action == CLUSTER_HW_REMASTER_LAUNCH_RETRY) {
			cluster_hw_bump_remaster_retry();
			ereport(LOG,
					(errmsg("cluster HW remaster: retrying dead node %d in episode " UINT64_FORMAT
							" (attempt %u/%d)",
							node, episode_epoch, d.next_attempts,
							cluster_hw_remaster_retry_max_attempts)));
		}
	}
}


bool
cluster_hw_remaster_gate_unfreeze(void)
{
	uint32 shard;

	if (!cluster_hw_authority_active())
		return false; /* HW not engaged -> no gating (reconfig path unchanged) */

	for (shard = 0; shard < PGRAC_GRD_SHARD_COUNT; shard++) {
		if (cluster_grd_shard_phase(shard) != GRD_SHARD_REBUILDING)
			continue;
		if (cluster_grd_shard_master(shard) != cluster_node_id)
			continue;
		if (cluster_hw_shard_rebuilt_generation(shard)
			!= cluster_grd_shard_master_generation(shard))
			return true; /* an adopted shard's HWM not yet rebuilt -> stay frozen */
	}
	return false; /* every adopted shard rebuilt -> ready to unfreeze */
}

#endif /* USE_PGRAC_CLUSTER */
