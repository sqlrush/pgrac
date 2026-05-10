/*-------------------------------------------------------------------------
 *
 * cluster_fence.c
 *	  pgrac Fence-lite — spec-2.28 Sprint A Step 1.
 *
 *	  Internal-only fence-lite skeleton.  ProcSignal-driven freeze/thaw
 *	  broadcast for in-flight transaction abort + postmaster orderly
 *	  self-shutdown on persistent quorum loss.  Consumes ClusterQvotec
 *	  Shmem.quorum_state (spec-2.6) via LMON tick.
 *
 *	  Step 1 scope (this commit):
 *	    - ClusterFenceShmem private 128-byte (2 cache-line) region with
 *	      LWLock for multi-writer coordination
 *	    - ClusterFenceFreezePending volatile sig_atomic_t per-backend
 *	      flag (defined here, set by cluster_signal.c handler in Step 2)
 *	    - shmem_size / shmem_init / shmem_register
 *	    - broadcast_freeze / broadcast_thaw / self_request stubs that
 *	      update ClusterFenceShmem fields but do NOT iterate ProcArray /
 *	      send signals (Step 3 D5 wires real broadcast)
 *	    - cluster_fence_check_interrupts no-op stub (Step 2 D4 wires
 *	      ereport ERRCODE_CLUSTER_QUORUM_LOST_BACKEND in postgres.c)
 *	    - cluster_fence_postmaster_check no-op stub (Step 3 D6 wires
 *	      kill(MyProcPid, SIGINT) per v0.3 F4 amend)
 *	    - cluster_get_fence_state SRF callback skeleton (Step 4 D10
 *	      wires into pg_proc.dat;Step 1 returns ERRCODE_FEATURE_NOT
 *	      _SUPPORTED)
 *
 *	  Step 1 explicitly DEFERS:
 *	    - Real ProcSignal handler bodies in cluster_signal.c — Step 2 D3
 *	    - postgres.c ProcessInterrupts hook wiring — Step 2 D4
 *	    - cluster_lmon.c quorum_state poll + broadcast helper — Step 3 D5
 *	    - postmaster.c kill self SIGINT trigger — Step 3 D6
 *	    - 1 SQLSTATE 53R50 / 1 wait event / view / 4 atomic counters /
 *	      3 inject points — Step 4
 *	    - 098 TAP fence_freeze_writes_2node + 096 L4-L5 unblock — Step 5
 *
 *	  Three user-imposed invariants (spec §3.0) are enforced by
 *	  later steps;Step 1 is just shmem region + API surface.  See
 *	  cluster_fence.h banner for full context.
 *
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/backend/cluster/cluster_fence.c
 *
 * NOTES
 *	  pgrac-original file.  Compiled only in --enable-cluster builds;
 *	  see src/backend/cluster/Makefile for OBJS rules.
 *
 *	  Spec authority:  pgrac:specs/spec-2.28-fence-lite-self-fence-
 *	  procsignal-freeze-thaw.md (frozen v0.3 2026-05-10).
 *
 *	  Sprint A Step 1 prerequisite (Invariant I3):  linkdb tag
 *	  v0.14.2-stage2.6 nightly run 25618433189 ✓ (Hardening v0.6
 *	  F1 quorum_state post-write recompute + F2 fast-restart ghost-
 *	  detect dual-layer).
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "cluster/cluster_fence.h"

#ifdef USE_PGRAC_CLUSTER

#include <string.h>

#include <signal.h> /* kill, SIGINT (spec-2.28 Step 3 D6 self-fence) */
#include <unistd.h>

#include "access/xact.h" /* IsTransactionState (spec-2.28 Step 2 D4 hook) */
#include "miscadmin.h"
#include "port/atomics.h"
#include "storage/proc.h"		/* ProcGlobal->allProcs (Step 3 D5) */
#include "storage/procarray.h"	/* (Step 3 D5 broadcast loop) */
#include "storage/procsignal.h" /* SendProcSignal + PROCSIG_CLUSTER_* */
#include "storage/sinvaladt.h"	/* BackendIdGetProc (Step 3 D5) */
#include "storage/lwlock.h"
#include "storage/shmem.h"
#include "utils/timestamp.h"
#include "funcapi.h"

#include "cluster/cluster_guc.h"	/* cluster_enabled */
#include "cluster/cluster_inject.h" /* CLUSTER_INJECTION_POINT (Step 4 D12) */
#include "cluster/cluster_pgstat.h" /* cluster_pgstat_lookup/_inc (Step 4 D11) */
#include "cluster/cluster_qvotec.h" /* cluster_qvotec_get_quorum_state (Step 3 D5) */
#include "cluster/cluster_shmem.h"	/* cluster_shmem_register_region */
#include "funcapi.h"				/* SRF for cluster_get_fence_state (Step 4 D10) */
#include "pgstat.h"					/* pgstat_report_wait_start/_end (Step 4 D9) */
#include "utils/builtins.h"			/* TimestampTzGetDatum */


/* ============================================================
 * Step 4 D11 — pgstat counter handles (lazy-init).
 *
 *	cluster_pgstat_lookup is O(N_counters) per call;cache the pointers
 *	on first use so hot paths (broadcast / check_interrupts) avoid
 *	repeated string lookups.  All four are appended to cluster_pgstat
 *	_counters[] in cluster_pgstat.c (registry order: freeze_broadcast /
 *	thaw_broadcast / self_fence_initiated / freeze_signal_received).
 * ============================================================ */
static ClusterPgstatCounter *fence_counter_freeze_broadcast = NULL;
static ClusterPgstatCounter *fence_counter_thaw_broadcast = NULL;
static ClusterPgstatCounter *fence_counter_self_fence_initiated = NULL;
static ClusterPgstatCounter *fence_counter_freeze_signal_received = NULL;

static void
fence_counters_lookup_lazy(void)
{
	if (fence_counter_freeze_broadcast != NULL)
		return; /* cached */
	fence_counter_freeze_broadcast = cluster_pgstat_lookup("cluster.fence.freeze_broadcast_count");
	fence_counter_thaw_broadcast = cluster_pgstat_lookup("cluster.fence.thaw_broadcast_count");
	fence_counter_self_fence_initiated
		= cluster_pgstat_lookup("cluster.fence.self_fence_initiated_count");
	fence_counter_freeze_signal_received
		= cluster_pgstat_lookup("cluster.fence.freeze_signal_received_count");
}


/* ============================================================
 * Per-backend volatile flag.
 *
 *	Set by cluster_signal.c handler for PROCSIG_CLUSTER_FREEZE_WRITES
 *	(Step 2 D3) using async-signal-safe write.  Read-and-cleared by
 *	cluster_fence_check_interrupts (Step 2 D4) when called from
 *	postgres.c ProcessInterrupts.  Storage class is sig_atomic_t per
 *	POSIX signal-safe write requirement.
 * ============================================================ */
volatile sig_atomic_t ClusterFenceFreezePending = 0;


/* ============================================================
 * ClusterFenceShmem — private 128-byte (2 cache-line) region.
 *
 *	Layout (offset / size / field):
 *	   0..63  LWLock (sized to one cache line on most platforms;
 *	                  shmem.c PG-native; protects all fields below)
 *	  64..71  uint64 last_freeze_at_us         (TimestampTz, 0 = never)
 *	  72..79  uint64 last_thaw_at_us           (0 = never)
 *	  80..87  uint64 self_fence_requested_at_us (0 = no pending request)
 *	  88..95  uint64 freeze_event_count
 *	  96..103 uint64 thaw_event_count
 *	 104..111 uint64 self_fence_initiated_count
 *	 112..127 uint8[16] _reserved
 *
 *	Note:  freeze_signal_received_count is NOT in this region —
 *	per spec D11, that counter is per-backend pg_atomic in the
 *	cluster_pgstat shmem region (cluster.fence.freeze_signal_received_
 *	count) so cross-process aggregation works via the existing mirror
 *	sync framework.  This avoids spec-2.6 backlog #4 limitation.
 *
 *	The LWLock is sized via PG's standard LWLockPadded mechanism;
 *	this struct uses a direct LWLock member (one tranche slot) and
 *	relies on natural alignment.  StaticAssertDecl pins total size.
 * ============================================================ */
typedef struct ClusterFenceShmemStruct {
	LWLock lock;							/* protects all fields below */
	TimestampTz last_freeze_at_us;			/* 0 = never */
	TimestampTz last_thaw_at_us;			/* 0 = never */
	TimestampTz self_fence_requested_at_us; /* 0 = no pending */
	uint64 freeze_event_count;				/* lifetime broadcasts */
	uint64 thaw_event_count;
	uint64 self_fence_initiated_count; /* postmaster-only writer */
	uint8 _reserved[16];
} ClusterFenceShmemStruct;

static ClusterFenceShmemStruct *ClusterFenceShmem = NULL;

/* The LWLock alone is platform-variable in size (LWLOCK_PADDED_SIZE
 * is 32 or 64 depending on cache line + atomics support).  Don't
 * StaticAssertDecl an exact byte count of the whole struct — the
 * size_fn computes from sizeof at runtime;cache-line layout is
 * conceptual not byte-exact. */


/* ============================================================
 * shmem hookup.
 * ============================================================ */

Size
cluster_fence_shmem_size(void)
{
	return MAXALIGN(sizeof(ClusterFenceShmemStruct));
}

void
cluster_fence_shmem_init(void)
{
	bool found;

	ClusterFenceShmem = (ClusterFenceShmemStruct *)ShmemInitStruct(
		"pgrac cluster fence", cluster_fence_shmem_size(), &found);

	if (!found) {
		LWLockInitialize(&ClusterFenceShmem->lock, LWTRANCHE_CLUSTER_FENCE);
		ClusterFenceShmem->last_freeze_at_us = 0;
		ClusterFenceShmem->last_thaw_at_us = 0;
		ClusterFenceShmem->self_fence_requested_at_us = 0;
		ClusterFenceShmem->freeze_event_count = 0;
		ClusterFenceShmem->thaw_event_count = 0;
		ClusterFenceShmem->self_fence_initiated_count = 0;
		memset(ClusterFenceShmem->_reserved, 0, sizeof(ClusterFenceShmem->_reserved));
	}
}

static const ClusterShmemRegion cluster_fence_region = {
	.name = "pgrac cluster fence",
	.size_fn = cluster_fence_shmem_size,
	.init_fn = cluster_fence_shmem_init,
	.lwlock_count = 1,
	.owner_subsys = "cluster_fence",
	.reserved_flags = 0,
};

void
cluster_fence_shmem_register(void)
{
	cluster_shmem_register_region(&cluster_fence_region);
}


/* ============================================================
 * Step 1 stubs — broadcast / self_request / check_interrupts /
 * postmaster_check.  Real bodies land Step 2-3 per spec §1.2.
 *
 *	The shmem fields ARE updated by these stubs so cluster_unit
 *	T-fence-1 can verify the API surface compiles + links cleanly.
 *	Real ProcArray loop / SendProcSignal / kill(SIGINT) wiring is
 *	in cluster_lmon.c (Step 3 D5) and postmaster.c (Step 3 D6).
 * ============================================================ */

void
cluster_fence_broadcast_freeze(const char *reason, uint64 scn)
{
	int signaled = 0;

	if (!cluster_enabled)
		return; /* L19/L20 silent skip */
	if (!cluster_freeze_writes_enabled)
		return; /* §3.6.1 GUC kill switch */
	if (ClusterFenceShmem == NULL)
		return; /* shmem not yet initialised — pre-RUNNING */

	(void)scn; /* recorded in audit LOG below — counter aggregates events */

	fence_counters_lookup_lazy();

	/* Step 4 D12 inject point — pre-broadcast pause for 098 TAP. */
	CLUSTER_INJECTION_POINT("cluster-fence-pre-freeze-broadcast");

	/* Update shmem timestamp + counter under lock. */
	LWLockAcquire(&ClusterFenceShmem->lock, LW_EXCLUSIVE);
	ClusterFenceShmem->last_freeze_at_us = GetCurrentTimestamp();
	ClusterFenceShmem->freeze_event_count++;
	LWLockRelease(&ClusterFenceShmem->lock);

	/* Step 4 D11 cross-process visible counter (pgstat). */
	cluster_pgstat_inc(fence_counter_freeze_broadcast);

	/*
	 * Loop ProcArray (PG-native via BackendIdGetProc) and SendProcSignal
	 * PROCSIG_CLUSTER_FREEZE_WRITES to every live backend.  Per
	 * sinvaladt.c:741-755 prior art, do NOT hold any lock while calling
	 * SendProcSignal — kernel signal delivery may not be fast.  ProcArray
	 * read of pid + backendId is a snapshot;a backend exiting between
	 * read and SendProcSignal harmlessly fails (kernel ESRCH, swallowed).
	 *
	 * Skip self (LMON's own pid) — LMON is the broadcaster and does not
	 * itself need to receive the freeze signal (no in-flight tx).
	 *
	 * Backend slots are 1..MaxBackends (BackendId namespace);auxiliary
	 * processes do not run user transactions and don't need freeze.
	 */
	{
		int beid;
		pid_t self_pid = MyProcPid;

		for (beid = 1; beid <= MaxBackends; beid++) {
			PGPROC *proc = BackendIdGetProc((BackendId)beid);
			pid_t pid;

			if (proc == NULL)
				continue;
			pid = proc->pid;
			if (pid == 0 || pid == self_pid)
				continue;
			(void)SendProcSignal(pid, PROCSIG_CLUSTER_FREEZE_WRITES, (BackendId)beid);
			signaled++;
		}
	}

	if (cluster_fence_audit_log >= CLUSTER_FENCE_AUDIT_LOG_LOG)
		ereport(LOG, (errmsg("cluster fence: broadcasting FREEZE_WRITES to %d backend(s),"
							 " reason=%s",
							 signaled, reason ? reason : "(null)")));
}

void
cluster_fence_broadcast_thaw(const char *reason, uint64 scn)
{
	int signaled = 0;

	if (!cluster_enabled)
		return;
	if (ClusterFenceShmem == NULL)
		return;

	(void)scn;

	fence_counters_lookup_lazy();

	/* Update shmem + cancel pending self-fence under lock. */
	LWLockAcquire(&ClusterFenceShmem->lock, LW_EXCLUSIVE);
	ClusterFenceShmem->last_thaw_at_us = GetCurrentTimestamp();
	ClusterFenceShmem->thaw_event_count++;
	/* Per Invariant I2:  thaw is observation-only on backend side —
	 * the handler does NOT clear ClusterFenceFreezePending and does NOT
	 * change the commit gate predicate.  But it DOES cancel pending
	 * self-fence: if quorum recovered before grace_ms elapsed,
	 * postmaster_check on next tick reads self_fence_requested_at_us = 0
	 * and skips pmdie. */
	ClusterFenceShmem->self_fence_requested_at_us = 0;
	LWLockRelease(&ClusterFenceShmem->lock);

	/*
	 * Broadcast THAW signal — informational on backend side per
	 * Invariant I2.  Same ProcArray loop pattern as freeze.
	 *
	 * Deliberately do not gate the self-fence cancellation above on
	 * cluster.freeze_writes_enabled.  That GUC disables the in-flight
	 * abort path only; a recovered quorum must still clear a pending
	 * postmaster self-fence request.
	 */
	{
		int beid;
		pid_t self_pid = MyProcPid;

		for (beid = 1; beid <= MaxBackends; beid++) {
			PGPROC *proc = BackendIdGetProc((BackendId)beid);
			pid_t pid;

			if (proc == NULL)
				continue;
			pid = proc->pid;
			if (pid == 0 || pid == self_pid)
				continue;
			(void)SendProcSignal(pid, PROCSIG_CLUSTER_THAW_WRITES, (BackendId)beid);
			signaled++;
		}
	}

	cluster_pgstat_inc(fence_counter_thaw_broadcast);

	/* Step 4 D12 inject point — post-thaw synchronisation. */
	CLUSTER_INJECTION_POINT("cluster-fence-post-thaw-broadcast");

	if (cluster_fence_audit_log >= CLUSTER_FENCE_AUDIT_LOG_LOG)
		ereport(LOG, (errmsg("cluster fence: broadcasting THAW_WRITES to %d backend(s),"
							 " reason=%s",
							 signaled, reason ? reason : "(null)")));
}

void
cluster_fence_self_request(const char *reason, uint64 scn)
{
	if (!cluster_enabled)
		return;
	if (!cluster_self_fence_enabled)
		return; /* §3.6.1 GUC kill switch */
	if (ClusterFenceShmem == NULL)
		return;

	(void)reason;
	(void)scn;

	LWLockAcquire(&ClusterFenceShmem->lock, LW_EXCLUSIVE);
	if (ClusterFenceShmem->self_fence_requested_at_us == 0)
		ClusterFenceShmem->self_fence_requested_at_us = GetCurrentTimestamp();
	/* Idempotent:  multiple calls within grace_ms window keep the
	 * earliest timestamp so postmaster_check measures from first
	 * request, not most recent. */
	LWLockRelease(&ClusterFenceShmem->lock);
}


/* ============================================================
 * cluster_fence_lmon_tick — quorum_state transition state machine.
 *
 *	LMON main loop calls this every tick after lmon_advance_liveness_
 *	tick.  Reads cluster_qvotec_get_quorum_state();compares to last-
 *	seen state in process-local static (only LMON calls this, single
 *	writer);on transition triggers broadcast.
 *
 *	OK→LOST    : freeze broadcast IMMEDIATE per Invariant I1 +
 *	             self_request (postmaster_check runs grace_ms later)
 *	OK→UNCERTAIN: same as OK→LOST treat as quorum loss for safety
 *	LOST→OK    : thaw broadcast (clears self_request inside thaw)
 *	UNCERTAIN→OK: same as LOST→OK
 *	others     : no action (transitions involving INITIALIZING are
 *	             startup-only;LOST→UNCERTAIN / UNCERTAIN→LOST stay
 *	             in fail-closed mode no broadcast needed)
 *
 *	Initial state is INITIALIZING;first transition out of it does NOT
 *	count as a real quorum loss/gain event (LMON not yet observed).
 * ============================================================ */
void
cluster_fence_lmon_tick(void)
{
	static int prev_state = (int)CLUSTER_QVOTEC_QUORUM_INITIALIZING;
	int curr;

	if (!cluster_enabled)
		return;
	if (ClusterFenceShmem == NULL)
		return;

	curr = cluster_qvotec_get_quorum_state();
	if (curr == prev_state)
		return; /* no transition */

	/* OK→{LOST,UNCERTAIN}:  immediate freeze + self_request. */
	if (prev_state == (int)CLUSTER_QVOTEC_QUORUM_OK
		&& (curr == (int)CLUSTER_QVOTEC_QUORUM_LOST
			|| curr == (int)CLUSTER_QVOTEC_QUORUM_UNCERTAIN)) {
		cluster_fence_broadcast_freeze(curr == (int)CLUSTER_QVOTEC_QUORUM_LOST
										   ? "qvotec quorum_state=LOST"
										   : "qvotec quorum_state=UNCERTAIN",
									   0);
		cluster_fence_self_request(
			curr == (int)CLUSTER_QVOTEC_QUORUM_LOST ? "quorum_lost" : "quorum_uncertain", 0);
	}
	/* {LOST,UNCERTAIN}→OK:  thaw broadcast (clears self_request). */
	else if ((prev_state == (int)CLUSTER_QVOTEC_QUORUM_LOST
			  || prev_state == (int)CLUSTER_QVOTEC_QUORUM_UNCERTAIN)
			 && curr == (int)CLUSTER_QVOTEC_QUORUM_OK) {
		cluster_fence_broadcast_thaw("qvotec quorum_state=OK", 0);
	}
	/* INITIALIZING→OK is the normal startup path, no event.
	 * INITIALIZING→{LOST,UNCERTAIN} is a degraded boot — no in-flight
	 * tx exists yet, so no broadcast needed;commit gate fail-closes
	 * naturally. */

	prev_state = curr;
}

void
cluster_fence_check_interrupts(void)
{
	if (!cluster_enabled)
		return; /* L19/L20:  cluster.enabled silent skip first line */

	/* Per v0.3 F1 amend:  no manual CS guard — PG ProcessInterrupts
	 * already returned early when CritSectionCount > 0 (postgres.c:
	 * 3226-3227), so we cannot be inside a CS here. */

	/*
	 * Per spec-2.28 §3.7 C4:  read-clear-then-decide.
	 *
	 * 1. Cheap pre-check returns immediately for the common no-pending
	 *    path (avoids the cost of writing a sig_atomic_t in the hot
	 *    interrupt-checking loop).
	 * 2. Clear the flag BEFORE the enabled-check + IsTransactionState
	 *    check.  If we leave the flag set when GUC is disabled or we
	 *    are idle, a later GUC re-enable + new transaction would see
	 *    a stale freeze bit and ereport against a quorum loss that
	 *    has long since recovered.  Clear-first is the same idiom PG
	 *    uses for QueryCancelPending (postgres.c:3047-3070).
	 * 3. After clearing, decide whether to actually ereport based on
	 *    the GUC + transaction state.
	 */
	if (ClusterFenceFreezePending == 0)
		return;

	ClusterFenceFreezePending = 0;

	/*
	 * Step 4 D9:  emit wait event around the body so pg_stat_activity
	 * can attribute freeze-induced abort to its dedicated wait class.
	 * Wrapping read-clear inside the wait event window is fine — the
	 * read-clear is microsecond scale and ereport() unwinds tx in the
	 * outer error path.
	 */
	pgstat_report_wait_start(WAIT_EVENT_CLUSTER_FENCE_BACKEND_INTERRUPT_CHECK);

	/* Step 4 D11:  per-backend received counter — across all backends
	 * sums to total freeze events × N_backends_alive_at_broadcast. */
	fence_counters_lookup_lazy();
	cluster_pgstat_inc(fence_counter_freeze_signal_received);

	if (!cluster_freeze_writes_enabled) {
		pgstat_report_wait_end();
		return; /* dev/debug GUC absorb */
	}

	if (!IsTransactionState()) {
		pgstat_report_wait_end();
		return; /* idle backend absorb — commit gate handles next BEGIN */
	}

	pgstat_report_wait_end();

	ereport(ERROR, (errcode(ERRCODE_CLUSTER_QUORUM_LOST_BACKEND),
					errmsg("transaction aborted: cluster quorum lost in flight"),
					errhint("the cluster lost majority quorum during your "
							"transaction;all uncommitted writes have been "
							"rolled back;auto-retry will resume after quorum "
							"recovers (typically O(seconds))")));
}

void
cluster_fence_postmaster_check(void)
{
	TimestampTz requested_at;
	TimestampTz now_us;
	int64 grace_us;
	bool should_initiate = false;

	if (IsUnderPostmaster)
		return; /* CLAUDE.md rule 16 §postmaster-once / L91 EXEC_BACKEND */
	if (!cluster_enabled)
		return;
	if (!cluster_self_fence_enabled)
		return;
	if (ClusterFenceShmem == NULL)
		return;

	/* Snapshot under lock — keep critical region small (no kill/ereport
	 * inside lock per sinvaladt.c:741-755 pattern). */
	LWLockAcquire(&ClusterFenceShmem->lock, LW_EXCLUSIVE);
	requested_at = ClusterFenceShmem->self_fence_requested_at_us;
	now_us = GetCurrentTimestamp();
	grace_us = (int64)cluster_self_fence_grace_ms * INT64CONST(1000);
	if (requested_at != 0 && (now_us - requested_at) >= grace_us) {
		should_initiate = true;
		ClusterFenceShmem->self_fence_initiated_count++;
		/* Clear the request so we don't re-trigger if the SIGINT delivery
		 * is somehow delayed and postmaster_check runs again. */
		ClusterFenceShmem->self_fence_requested_at_us = 0;
	}
	LWLockRelease(&ClusterFenceShmem->lock);

	if (!should_initiate)
		return;

	/* Step 4 D11 cross-process counter (postmaster writer only). */
	fence_counters_lookup_lazy();
	cluster_pgstat_inc(fence_counter_self_fence_initiated);

	/* Step 4 D12 inject point — pre-shutdown for 098 TAP cancel test. */
	CLUSTER_INJECTION_POINT("cluster-fence-pre-self-fence-shutdown");

	ereport(LOG, (errmsg("postmaster shutting down: self-fence on persistent quorum loss"),
				  errdetail("cluster.self_fence_grace_ms (%d ms) elapsed since LMON detected"
							" quorum loss",
							cluster_self_fence_grace_ms),
				  errhint("postmaster will perform orderly fast shutdown via SIGINT;"
						  " backends receive SIGTERM;recovery on next start will resume"
						  " from voting-disk slot")));

	/*
	 * Per spec-2.28 v0.3 F4 amend:  PG postmaster has no callable
	 * pmdie() function.  Self-signal SIGINT — PG postmaster.c SIGINT
	 * handler (line ~2825-2839) sets pending_pm_fast_shutdown_request
	 * + pending_pm_shutdown_request + SetLatch(MyLatch);ServerLoop
	 * next iteration line 1791 calls process_pm_shutdown_request()
	 * which drives Shutdown = FastShutdown (postmaster.c:2849+) +
	 * PostmasterStateMachine SIGTERM children + drain checkpointer/
	 * bgwriter/walwriter/archiver + postmaster exits with status 0.
	 * Equivalent to `pg_ctl stop -m fast`.
	 */
	if (kill(MyProcPid, SIGINT) != 0) {
		/* Should never happen — MyProcPid is the postmaster's own pid.
		 * Defensive log only. */
		ereport(WARNING, (errmsg("postmaster self-fence SIGINT failed: %m")));
	}
}


/* ============================================================
 * SRF callback for pg_cluster_fence_state view (Step 4 D10).
 *
 *	Step 1 returns FEATURE_NOT_SUPPORTED — view is not yet wired in
 *	pg_proc.dat, so this function is unreachable from SQL.  When
 *	Step 4 wires the view, this body implements the 8-column single-
 *	row return.
 * ============================================================ */
Datum
cluster_get_fence_state(PG_FUNCTION_ARGS)
{
	ReturnSetInfo *rsinfo;
	Datum values[8];
	bool nulls[8];
	int col = 0;
	TimestampTz last_freeze;
	TimestampTz last_thaw;
	TimestampTz self_fence_requested;
	uint64 freeze_count;
	uint64 thaw_count;
	uint64 self_fence_initiated_count;
	uint64 freeze_signal_received;

	InitMaterializedSRF(fcinfo, 0);
	rsinfo = (ReturnSetInfo *)fcinfo->resultinfo;

	if (ClusterFenceShmem == NULL) {
		/* Pre-RUNNING / cluster.enabled=off — emit one row, all-NULL +
		 * zero counters per spec §2.4 NULL semantics. */
		Datum n_values[8] = { 0 };
		bool n_nulls[8] = { true, true, false, false, false, false, false, false };

		n_values[2] = BoolGetDatum(false);	   /* self_fence_pending */
		n_values[3] = Int32GetDatum(-1);	   /* self_fence_grace_remaining_ms */
		n_values[4] = Int64GetDatum((int64)0); /* freeze_broadcast_count */
		n_values[5] = Int64GetDatum((int64)0); /* thaw_broadcast_count */
		n_values[6] = Int64GetDatum((int64)0); /* self_fence_initiated_count */
		n_values[7] = Int64GetDatum((int64)0); /* freeze_signal_received_count */
		tuplestore_putvalues(rsinfo->setResult, rsinfo->setDesc, n_values, n_nulls);
		return (Datum)0;
	}

	memset(nulls, false, sizeof(nulls));

	/* Snapshot under lock — keep counters atomic across columns. */
	LWLockAcquire(&ClusterFenceShmem->lock, LW_SHARED);
	last_freeze = ClusterFenceShmem->last_freeze_at_us;
	last_thaw = ClusterFenceShmem->last_thaw_at_us;
	self_fence_requested = ClusterFenceShmem->self_fence_requested_at_us;
	freeze_count = ClusterFenceShmem->freeze_event_count;
	thaw_count = ClusterFenceShmem->thaw_event_count;
	self_fence_initiated_count = ClusterFenceShmem->self_fence_initiated_count;
	LWLockRelease(&ClusterFenceShmem->lock);

	/* col 0:  last_freeze_at (NULL = never broadcast) */
	if (last_freeze == 0)
		nulls[col] = true;
	else
		values[col] = TimestampTzGetDatum(last_freeze);
	col++;

	/* col 1:  last_thaw_at (NULL = never broadcast) */
	if (last_thaw == 0)
		nulls[col] = true;
	else
		values[col] = TimestampTzGetDatum(last_thaw);
	col++;

	/* col 2:  self_fence_pending (true if requested_at_us > 0) */
	values[col++] = BoolGetDatum(self_fence_requested > 0);

	/* col 3:  self_fence_grace_remaining_ms (-1 if not pending) */
	if (self_fence_requested == 0)
		values[col++] = Int32GetDatum(-1);
	else {
		TimestampTz now = GetCurrentTimestamp();
		int64 elapsed_ms = (now - self_fence_requested) / 1000;
		int32 remaining_ms = cluster_self_fence_grace_ms - (int32)elapsed_ms;
		if (remaining_ms < 0)
			remaining_ms = 0; /* postmaster_check will fire next tick */
		values[col++] = Int32GetDatum(remaining_ms);
	}

	/* col 4-6:  lifetime counters from shmem (NOT pgstat — shmem is
	 * authoritative; pgstat is the cross-process visible mirror). */
	values[col++] = Int64GetDatum((int64)freeze_count);
	values[col++] = Int64GetDatum((int64)thaw_count);
	values[col++] = Int64GetDatum((int64)self_fence_initiated_count);

	/* col 7:  freeze_signal_received_count (per-backend cluster_pgstat
	 * counter — sums across backends to total received events). */
	{
		ClusterPgstatCounter *c
			= cluster_pgstat_lookup("cluster.fence.freeze_signal_received_count");
		freeze_signal_received = (c == NULL) ? 0 : pg_atomic_read_u64(&c->value);
	}
	values[col++] = Int64GetDatum((int64)freeze_signal_received);

	Assert(col == 8);
	tuplestore_putvalues(rsinfo->setResult, rsinfo->setDesc, values, nulls);

	return (Datum)0;
}

#endif /* USE_PGRAC_CLUSTER */
