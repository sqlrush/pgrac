/*-------------------------------------------------------------------------
 *
 * cluster_hang.c
 *	  pgrac Hang Manager skeleton runtime (spec-5.11).
 *
 *	  This file holds the PostgreSQL-runtime half of the Hang Manager: the
 *	  DIAG-hosted long-wait sampling round (D2), the local wait-chain edge
 *	  resolution (D3), the long-wait LOG-once (D4), the consistent dump
 *	  snapshot accessor (D4), and the ProcSignal backend-local self-dump
 *	  (D5).  All side-effect-free decisions live in cluster_hang_policy.c.
 *
 *	  Discipline (spec-5.11 §1.4 / §3.2): diagnostic only, fail-OPEN.  A
 *	  sampling round never raises an error to reject itself — it tags
 *	  doubtful samples with a ClusterHangSampleQuality and keeps going, and
 *	  a PG_TRY backstop turns any unexpected error into an error_count bump
 *	  so DIAG never dies.  No cancel / kill / victim / cross-node DFS lives
 *	  here (that is spec-5.12).
 *
 *	  Wait-duration source (§0.2 matrix, resolved at D0): local heavyweight
 *	  LOCK waits use LockInstanceData.waitStart (a true wait-start →
 *	  COMPLETE).  GES reply-waits expose no locally-readable wait_start in
 *	  the shipped spec-5.8 surface, so they are APPROXIMATE and marked
 *	  REMOTE_BOUNDARY (the holder is off-node; cross-node DFS is 5.12).
 *	  Generic PG-native waits (LWLock / IO / ...) carry no wait-start →
 *	  APPROXIMATE.
 *
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/backend/cluster/cluster_hang.c
 *
 * NOTES
 *	  This is a pgrac-original file.  Compiled only in --enable-cluster
 *	  builds.
 *	  Spec: spec-5.11-hang-manager-skeleton.md.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "cluster/cluster_diag.h"
#include "cluster/cluster_guc.h"
#include "cluster/cluster_hang.h"
#include "cluster/cluster_lmd_wait_state.h"
#include "fmgr.h"
#include "miscadmin.h"
#include "storage/lock.h"
#include "storage/proc.h"
#include "storage/procarray.h"
#include "storage/procsignal.h"
#include "utils/backend_status.h"
#include "utils/memutils.h"
#include "utils/timestamp.h"
#include "utils/wait_event.h"

/* Same lock-free single read pgstatfuncs.c uses for proc->wait_event_info. */
#define UINT32_ACCESS_ONCE(var) ((uint32)(*((volatile uint32 *)&(var))))

/*
 * ProcSignal pending flag for a backend self-dump (D5).  Set async-signal-
 * safe in the handler, consumed at the next CHECK_FOR_INTERRUPTS().
 */
volatile sig_atomic_t cluster_hang_dump_pending = false;

/*
 * DIAG-local working buffer for one sampling round.  The sampler is single-
 * threaded (DIAG only) and non-reentrant, so a file-static buffer avoids an
 * ~8 KiB stack frame / per-round allocation.
 */
static ClusterHangSampleStore hang_round;

/*
 * Per-round scratch context.  GetLockStatusData() (and other helpers) palloc
 * into CurrentMemoryContext every round; DIAG is a long-lived aux process
 * whose main loop never resets a context, so without a dedicated context the
 * round's allocations would accumulate in a long-lived parent and leak (the
 * bgwriter / checkpointer / walwriter aux processes solve this the same way).
 * Created lazily on first round, reset at the end of every round.
 */
static MemoryContext hang_sample_context = NULL;

/*
 * DIAG-local LOG-once suppression set (§3.1b).  Not shared memory: DIAG is
 * the only logger.  Each round we mark every key unseen, log + (re)admit any
 * long-wait whose key is new, and drop keys not seen this round (so a backend
 * exit or a changed wait naturally clears suppression).
 */
typedef struct HangLogKey {
	bool used;
	bool seen_this_round;
	int pid;
	int backendId;
	char wait_event[CLUSTER_HANG_WAIT_EVENT_LEN];
	TimestampTz anchor; /* wait_since (TRUE) or state start (APPROX) */
} HangLogKey;

static HangLogKey log_suppress[CLUSTER_HANG_MAX_SAMPLES];


/* ============================================================
 * Enum → string helpers (for dump_hang).
 * ============================================================ */

const char *
cluster_hang_wait_source_str(uint8 source)
{
	switch ((ClusterHangWaitSource)source) {
	case HANG_WAIT_LOCK:
		return "lock";
	case HANG_WAIT_CACHE_FUSION:
		return "cache_fusion";
	case HANG_WAIT_IO:
		return "io";
	case HANG_WAIT_UNDO:
		return "undo";
	case HANG_WAIT_LWLOCK:
		return "lwlock";
	case HANG_WAIT_IDLE_IN_TX_BLOCKER:
		return "idle_in_tx_blocker";
	case HANG_WAIT_UNKNOWN:
	default:
		return "unknown";
	}
}

const char *
cluster_hang_quality_str(uint8 quality)
{
	switch ((ClusterHangSampleQuality)quality) {
	case HANG_SAMPLE_COMPLETE:
		return "complete";
	case HANG_SAMPLE_APPROXIMATE:
		return "approximate";
	case HANG_SAMPLE_INCOMPLETE:
		return "incomplete";
	case HANG_SAMPLE_BLOCKER_GONE:
		return "blocker_gone";
	case HANG_SAMPLE_REMOTE_BOUNDARY:
		return "remote_boundary";
	default:
		return "unknown";
	}
}


/* ============================================================
 * GUC gate.
 * ============================================================ */

bool
cluster_hang_sample_due(TimestampTz now)
{
	ClusterDiagSharedState *st = cluster_diag_get_shared_state();
	TimestampTz last;

	if (st == NULL)
		return false;

	LWLockAcquire(&st->lwlock, LW_SHARED);
	last = st->last_sample_at;
	LWLockRelease(&st->lwlock);

	if (last == 0)
		return true; /* never sampled — do the first round now */

	return TimestampDifferenceExceeds(last, now, cluster_hang_sample_interval_ms);
}


/* ============================================================
 * D3 — local wait-chain edge resolution.
 * ============================================================ */

/*
 * hang_find_lock_wait — find the LockInstanceData for a waiting backend.
 *
 *	Returns the index into lockData->locks of the entry that represents
 *	`pid` waiting on a heavyweight lock (waitLockMode != NoLock), or -1 if
 *	`pid` is not currently waiting on a heavyweight lock in this snapshot.
 */
static int
hang_find_lock_wait(const LockData *lockData, int pid)
{
	int i;

	for (i = 0; i < lockData->nelements; i++) {
		const LockInstanceData *inst = &lockData->locks[i];

		if (inst->pid == pid && inst->waitLockMode != NoLock)
			return i;
	}
	return -1;
}

/*
 * hang_find_hard_blocker — resolve the hard blocker of a heavyweight-lock
 * waiter from a single GetLockStatusData snapshot.
 *
 *	A hard blocker is a different PGPROC that holds a lock on the same
 *	object in a mode conflicting with the waiter's awaited mode (§3.2).
 *	Sets *blocker_pid / *blocker_backendId and returns true on success.
 *	When no conflicting holder is found the waiter is only behind queued
 *	waiters (a soft blocker we cannot identify from this snapshot), so we
 *	set *soft_only and return false — honestly INCOMPLETE, never "no
 *	blocker" (§3.2, R14).
 */
static bool
hang_find_hard_blocker(const LockData *lockData, int wait_idx, int *blocker_pid,
					   int *blocker_backendId, bool *soft_only)
{
	const LockInstanceData *waiter = &lockData->locks[wait_idx];
	int i;

	*blocker_pid = -1;
	*blocker_backendId = -1;
	*soft_only = false;

	for (i = 0; i < lockData->nelements; i++) {
		const LockInstanceData *holder = &lockData->locks[i];
		int m;

		if (i == wait_idx || holder->pid == waiter->pid)
			continue;
		if (holder->holdMask == 0)
			continue; /* not a holder of this object */
		if (memcmp(&holder->locktag, &waiter->locktag, sizeof(LOCKTAG)) != 0)
			continue; /* different locked object */

		for (m = 1; m < MAX_LOCKMODES; m++) {
			if ((holder->holdMask & LOCKBIT_ON(m))
				&& DoLockModesConflict(waiter->waitLockMode, m)) {
				/* GES_MODE_OK: local heavyweight-lock conflict (pg_locks
				 * hard-blocker detection), not a GES cluster-lock mode
				 * decision; DoLockModesConflict is the correct PG primitive. */
				*blocker_pid = holder->pid;
				*blocker_backendId = holder->backend;
				return true;
			}
		}
	}

	/* waiting on a heavyweight lock but behind queued waiters only */
	*soft_only = true;
	return false;
}


/* ============================================================
 * D4 — LOG-once.
 * ============================================================ */

static void
hang_log_once_begin_round(void)
{
	int i;

	for (i = 0; i < CLUSTER_HANG_MAX_SAMPLES; i++)
		log_suppress[i].seen_this_round = false;
}

/* Returns true if this long-wait key is new (caller should LOG it). */
static bool
hang_log_once_admit(const ClusterHangNode *node)
{
	int i;
	int free_idx = -1;

	for (i = 0; i < CLUSTER_HANG_MAX_SAMPLES; i++) {
		if (!log_suppress[i].used) {
			if (free_idx < 0)
				free_idx = i;
			continue;
		}
		if (log_suppress[i].pid == node->pid && log_suppress[i].backendId == node->backendId
			&& log_suppress[i].anchor == node->wait_since
			&& strcmp(log_suppress[i].wait_event, node->wait_event) == 0) {
			log_suppress[i].seen_this_round = true;
			return false; /* already logged this exact wait */
		}
	}

	if (free_idx < 0)
		return false; /* suppression set full — do not flood */

	log_suppress[free_idx].used = true;
	log_suppress[free_idx].seen_this_round = true;
	log_suppress[free_idx].pid = node->pid;
	log_suppress[free_idx].backendId = node->backendId;
	log_suppress[free_idx].anchor = node->wait_since;
	strlcpy(log_suppress[free_idx].wait_event, node->wait_event,
			sizeof(log_suppress[free_idx].wait_event));
	return true;
}

static void
hang_log_once_end_round(void)
{
	int i;

	for (i = 0; i < CLUSTER_HANG_MAX_SAMPLES; i++) {
		if (log_suppress[i].used && !log_suppress[i].seen_this_round)
			log_suppress[i].used = false; /* wait ended / backend exited */
	}
}


/* ============================================================
 * D2 — one sampling round.
 * ============================================================ */

static void
hang_do_sample_round(ClusterDiagSharedState *st)
{
	TimestampTz now = GetCurrentTimestamp();
	int numbackends;
	int i;
	int my_node_id = cluster_node_id;
	LockData *lockData;
	int64 long_wait_count = 0;
	int64 longest_wait_us = 0;
	uint64 excl_idle = 0;
	uint64 excl_bg = 0;
	uint64 excl_dl = 0;
	uint64 long_waits = 0;
	uint64 incomplete = 0;

	cluster_hang_store_reset(&hang_round);
	hang_log_once_begin_round();

	/*
	 * freshness: DIAG is a long-lived aux process with no transaction
	 * boundary, so pgstat_read_current_status() would otherwise hand back the
	 * previous round's cached snapshot.  Clear once per round (never inside
	 * the loop — that would break index stability).
	 */
	pgstat_clear_backend_activity_snapshot();
	numbackends = pgstat_fetch_stat_numbackends();

	/* One lock snapshot for the whole round (§3.2: per-round, not per-waiter). */
	lockData = GetLockStatusData();

	for (i = 1; i <= numbackends; i++) {
		LocalPgBackendStatus *lb = pgstat_get_local_beentry_by_index(i);
		PgBackendStatus *be;
		PGPROC *proc;
		uint32 wait_info;
		const char *we_name;
		ClusterHangExcludeReason excl;
		ClusterHangNode node;
		ClusterHangSampleSlot slot;
		int lock_idx;
		bool has_true_wait_start = false;
		TimestampTz wait_since = 0;
		int64 duration_us;
		ClusterLmdWaitStateSnapshot cwait;
		bool cluster_waiting = false;

		if (lb == NULL)
			continue;
		be = &lb->backendStatus;
		if (be->st_procpid == 0)
			continue; /* unused slot */

		/* wait_event lives on PGPROC (mirrors pg_stat_get_activity). */
		proc = BackendPidGetProc(be->st_procpid);
		if (proc == NULL && be->st_backendType != B_BACKEND)
			proc = AuxiliaryPidGetProc(be->st_procpid);
		wait_info = (proc != NULL) ? UINT32_ACCESS_ONCE(proc->wait_event_info) : 0;
		we_name = (wait_info != 0) ? pgstat_get_wait_event(wait_info) : NULL;

		/* cluster-dimension wait identity (5.8 D1d, read-only consume, Q2-A). */
		if (proc != NULL && cluster_lmd_wait_state_read(&proc->cluster_lmd_wait, &cwait))
			cluster_waiting = cwait.active;

		/* Wait duration source (§0.2 matrix). */
		lock_idx = hang_find_lock_wait(lockData, be->st_procpid);
		if (lock_idx >= 0 && lockData->locks[lock_idx].waitStart != 0) {
			has_true_wait_start = true;
			wait_since = lockData->locks[lock_idx].waitStart;
			duration_us = now - wait_since;
		} else {
			/* No true wait-start (GES / generic): approximate from state. */
			wait_since = be->st_state_start_timestamp;
			duration_us = (wait_since != 0) ? now - wait_since : 0;
		}

		if (!cluster_hang_wait_exceeds_threshold(duration_us, cluster_hang_threshold_ms))
			continue;

		/*
		 * D6 v1: shipped spec-5.8 exposes deadlock confirmation only as
		 * aggregate counters + WFG membership, not a per-proc confirmed-cycle
		 * flag, so in_confirmed_deadlock is left false (fail-OPEN: cannot
		 * prove → do not claim).  Live per-proc exclusion is forward to a 5.9
		 * per-proc victim/confirmed signal (see D0 re-ground).
		 */
		excl = cluster_hang_exclude_reason(be->st_state, be->st_backendType, false, wait_info);
		if (excl != HANG_EXCLUDE_NONE) {
			switch (excl) {
			case HANG_EXCLUDE_IDLE:
				excl_idle++;
				break;
			case HANG_EXCLUDE_BGWORKER:
				excl_bg++;
				break;
			case HANG_EXCLUDE_DEADLOCK:
				excl_dl++;
				break;
			default:
				break;
			}
			continue;
		}

		/* Build the working node. */
		memset(&node, 0, sizeof(node));
		node.pid = be->st_procpid;
		node.backendId = lb->backend_id;
		node.xid = lb->backend_xid;
		node.wait_since = wait_since;
		node.duration_us = duration_us;
		node.duration_kind = cluster_hang_duration_kind(has_true_wait_start);
		node.source = cluster_hang_classify_wait_source(wait_info, we_name);
		node.blocker_pid = -1;
		node.blocker_backendId = -1;
		node.blocker_remote_node = -1;
		node.is_root = false;
		node.in_confirmed_deadlock = false; /* D6 v1 (see above) */
		node.being_resolved = false;		/* 5.9 forward (not compiled) */
		node.fairness_boosted = false;		/* 5.10 forward (not compiled) */
		if (we_name != NULL)
			strlcpy(node.wait_event, we_name, sizeof(node.wait_event));

		/* D3 — local blocker edge / quality. */
		if (lock_idx >= 0) {
			bool soft_only;

			if (hang_find_hard_blocker(lockData, lock_idx, &node.blocker_pid,
									   &node.blocker_backendId, &soft_only))
				node.quality
					= cluster_hang_quality(node.duration_kind, node.blocker_pid, -1, false, false);
			else
				node.quality = cluster_hang_quality(node.duration_kind, -1, -1, false, soft_only);
		} else if (cluster_waiting) {
			/*
			 * Cluster (GES/TX) wait with no local heavyweight-lock entry: the
			 * holder is off-node and shipped 5.8 exposes no local holder
			 * snapshot, so mark a REMOTE_BOUNDARY (cross-node DFS is 5.12).
			 * Boundary is real — we do not pretend to have walked the chain.
			 */
			node.is_root = false;
			node.quality = HANG_SAMPLE_REMOTE_BOUNDARY;
		} else {
			/* Generic PG-native wait (LWLock / IO / ...): no blocker, approx. */
			node.quality = cluster_hang_quality(node.duration_kind, -1, -1, false, false);
		}

		long_waits++;
		if (node.quality != HANG_SAMPLE_COMPLETE)
			incomplete++;
		long_wait_count++;
		if (duration_us > longest_wait_us)
			longest_wait_us = duration_us;

		/* LOG-once on first sighting of this exact long wait (§3.1b). */
		if (cluster_hang_dump_enabled && hang_log_once_admit(&node))
			ereport(LOG, (errmsg("cluster hang manager: backend pid %d waiting %ld ms on \"%s\" "
								 "(source %s, quality %s, blocker pid %d)",
								 node.pid, (long)(duration_us / 1000),
								 node.wait_event[0] ? node.wait_event : "(none)",
								 cluster_hang_wait_source_str((uint8)node.source),
								 cluster_hang_quality_str((uint8)node.quality), node.blocker_pid)));

		cluster_hang_node_to_slot(&node, my_node_id, &slot);
		cluster_hang_store_consider(&hang_round, &slot, cluster_hang_max_sampled);
	}

	hang_log_once_end_round();

	/*
	 * Publish the round + aggregates + counters in ONE short LW_EXCLUSIVE
	 * section so a concurrent reader (cluster_hang_get_dump_data) never sees a
	 * torn round: a new sample store + bumped epoch paired with stale
	 * aggregates/counters.  publish_locked advances sample_epoch under the
	 * lock we already hold.
	 */
	LWLockAcquire(&st->lwlock, LW_EXCLUSIVE);
	cluster_hang_store_publish_locked(&st->hang_store, &hang_round);
	st->last_sample_at = now;
	st->long_wait_count = long_wait_count;
	st->longest_wait_us = longest_wait_us;
	if (cluster_hang_dump_enabled && long_waits > 0)
		st->last_dump_emitted_at = now;
	st->hang_counters.samples_taken++;
	st->hang_counters.long_waits_seen += long_waits;
	st->hang_counters.incomplete_samples += incomplete;
	st->hang_counters.excluded_idle += excl_idle;
	st->hang_counters.excluded_bgworker += excl_bg;
	st->hang_counters.excluded_deadlock += excl_dl;
	if (cluster_hang_dump_enabled && long_waits > 0)
		st->hang_counters.dumps_emitted++;
	LWLockRelease(&st->lwlock);
}

void
cluster_hang_sample_once(void)
{
	ClusterDiagSharedState *st = cluster_diag_get_shared_state();
	MemoryContext oldcxt;

	if (st == NULL)
		return;

	/*
	 * Run the round in a dedicated context (created once) and reset it on the
	 * way out so the per-round palloc'd snapshots (GetLockStatusData et al.)
	 * never accumulate in a long-lived parent.  Both the normal and the
	 * fail-OPEN error path fall through to the switch-back + reset below.
	 */
	if (hang_sample_context == NULL)
		hang_sample_context
			= AllocSetContextCreate(TopMemoryContext, "ClusterHangSample", ALLOCSET_DEFAULT_SIZES);
	oldcxt = MemoryContextSwitchTo(hang_sample_context);

	/*
	 * fail-OPEN (§1.4 / §3.2): a diagnostic round must never crash DIAG.
	 * Any unexpected error is logged once, the error state flushed, and the
	 * round abandoned with an error_count bump.
	 */
	PG_TRY();
	{
		hang_do_sample_round(st);
	}
	PG_CATCH();
	{
		MemoryContextSwitchTo(oldcxt);
		EmitErrorReport();
		FlushErrorState();

		LWLockAcquire(&st->lwlock, LW_EXCLUSIVE);
		st->hang_counters.error_count++;
		LWLockRelease(&st->lwlock);
	}
	PG_END_TRY();

	/* Restore the caller's context and free this round's allocations. */
	MemoryContextSwitchTo(oldcxt);
	MemoryContextReset(hang_sample_context);
}


/* ============================================================
 * D4 — consistent dump snapshot accessor.
 * ============================================================ */

void
cluster_hang_get_dump_data(ClusterHangDumpData *out)
{
	ClusterDiagSharedState *st = cluster_diag_get_shared_state();

	memset(out, 0, sizeof(*out));
	if (st == NULL) {
		out->available = false;
		return;
	}

	out->available = true;
	LWLockAcquire(&st->lwlock, LW_SHARED);
	out->last_sample_at = st->last_sample_at;
	out->last_dump_emitted_at = st->last_dump_emitted_at;
	out->long_wait_count = st->long_wait_count;
	out->longest_wait_us = st->longest_wait_us;
	out->counters = st->hang_counters;
	out->store = st->hang_store;
	LWLockRelease(&st->lwlock);
}


/* ============================================================
 * D5 — ProcSignal backend-local self-dump.
 * ============================================================ */

/*
 * cluster_handle_hang_dump_interrupt — ProcSignal handler.
 *
 *	async-signal-safe (rule 16): set the flag, request an interrupt, wake
 *	the latch.  All real work happens at the next CHECK_FOR_INTERRUPTS().
 */
void
cluster_handle_hang_dump_interrupt(void)
{
	cluster_hang_dump_pending = true;
	InterruptPending = true;
	SetLatch(MyLatch);
}

void
cluster_hang_check_pending_interrupt(void)
{
	if (cluster_hang_dump_pending) {
		cluster_hang_dump_pending = false;
		cluster_hang_dump_self_to_log();
	}
}

void
cluster_hang_dump_self_to_log(void)
{
	uint32 wait_info;
	const char *we_type;
	const char *we_name;
	ClusterDiagSharedState *st;
	ClusterLmdWaitStateSnapshot cwait;
	bool cluster_waiting = false;

	if (MyProc == NULL)
		return;

	wait_info = UINT32_ACCESS_ONCE(MyProc->wait_event_info);
	we_type = (wait_info != 0) ? pgstat_get_wait_event_type(wait_info) : "none";
	we_name = (wait_info != 0) ? pgstat_get_wait_event(wait_info) : "none";

	if (cluster_lmd_wait_state_read(&MyProc->cluster_lmd_wait, &cwait))
		cluster_waiting = cwait.active;

	if (cluster_waiting)
		ereport(LOG, (errmsg("cluster hang dump: pid %d wait %s/%s; cluster wait kind %u "
							 "request_id " UINT64_FORMAT " epoch " UINT64_FORMAT,
							 MyProcPid, we_type, we_name, (unsigned)cwait.kind, cwait.request_id,
							 cwait.cluster_epoch)));
	else
		ereport(LOG, (errmsg("cluster hang dump: pid %d wait %s/%s; no cluster wait-state",
							 MyProcPid, we_type, we_name)));

	st = cluster_diag_get_shared_state();
	if (st != NULL) {
		LWLockAcquire(&st->lwlock, LW_EXCLUSIVE);
		st->hang_counters.proc_signal_dumps++;
		LWLockRelease(&st->lwlock);
	}
}


/*
 * pg_cluster_hang_dump(pid) — ask the backend with the given pid to log its
 * own cluster wait / hang state.  Mirrors pg_log_backend_memory_contexts:
 * superuser-only by default (REVOKE in system_views.sql), WARNING (not
 * ERROR) on a vanished pid so a loop-through-resultset does not abort.
 */
PG_FUNCTION_INFO_V1(pg_cluster_hang_dump);

Datum
pg_cluster_hang_dump(PG_FUNCTION_ARGS)
{
	int pid = PG_GETARG_INT32(0);
	const PGPROC *proc;
	BackendId backendId = InvalidBackendId;

	proc = BackendPidGetProc(pid);
	if (proc != NULL)
		backendId = proc->backendId;
	else
		proc = AuxiliaryPidGetProc(pid);

	if (proc == NULL) {
		ereport(WARNING, (errmsg("PID %d is not a PostgreSQL server process", pid)));
		PG_RETURN_BOOL(false);
	}

	if (SendProcSignal(pid, PROCSIG_CLUSTER_HANG_DUMP, backendId) < 0) {
		ereport(WARNING, (errmsg("could not send hang-dump signal to process %d: %m", pid)));
		PG_RETURN_BOOL(false);
	}

	PG_RETURN_BOOL(true);
}
