/*-------------------------------------------------------------------------
 *
 * cluster_startup_phase.c
 *	  pgrac postmaster startup phase machinery (Stage 1.10 skeleton).
 *
 *	  Implements the Phase 0 -> 1 -> 2 -> 3 -> 4 -> RUNNING state
 *	  machine that splits the previously single cluster_init() entry
 *	  into named, observable, timeout-bounded transitions.
 *
 *	  See cluster_startup_phase.h for the architectural overview and
 *	  HC1-HC5 hard constraints; spec-1.10-postmaster-startup-phase-
 *	  skeleton.md for the full design.
 *
 *	  Driver / handler split (HC3):
 *
 *	    The driver in cluster_run_startup_sequence() owns the phase
 *	    transition (advance + log + wait event + history + timeout +
 *	    inject points).  Phase handlers (phase_1_handler, phase_2_
 *	    handler, ..., phase_4_handler) only do their phase's work and
 *	    return PhaseRunResult.  The driver decides whether to advance.
 *
 *	    Stage 1.10 phase handlers 1-3 are no-op stubs returning
 *	    PHASE_RUN_OK; phase 4 handler delegates to PG's existing
 *	    walwriter / bgwriter / etc. spawn paths (no new process).
 *	    Stage 1.11-1.14 / Stage 2-4 replace handler bodies without
 *	    breaking the driver loop.
 *
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/backend/cluster/cluster_startup_phase.c
 *
 * NOTES
 *	  This is a pgrac-original file (no derivation from PostgreSQL).
 *	  Compiled only in --enable-cluster builds.
 *	  Spec: spec-1.10-postmaster-startup-phase-skeleton.md (frozen
 *	  2026-05-03 v1.1 with 5 user hard-constraint refinements).
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#ifdef USE_PGRAC_CLUSTER

#include "miscadmin.h" /* IsUnderPostmaster (HC1) */
#include "storage/ipc.h"
#include "storage/lwlock.h"
#include "storage/shmem.h"
#include "utils/elog.h"
#include "utils/timestamp.h"

#include "cluster/cluster_elog.h"	/* cluster_phase legacy mirror (HC2) */
#include "cluster/cluster_cssd.h"	/* cluster_cssd_start / wait_for_ready (2.5 Sprint A) */
#include "cluster/cluster_qvotec.h" /* cluster_qvotec_start / wait_for_ready (spec-2.6 Step 3 D8) */
#include "cluster/cluster_diag.h"	/* cluster_diag_start / wait_for_ready (1.13 Sprint A) */
#include "cluster/cluster_guc.h"	/* cluster_phase{1..4}_timeout (D2 F2) */
#include "cluster/cluster_stats.h"	/* cluster_stats_start / wait_for_ready (1.14 Sprint A) */
#include "cluster/cluster_inject.h" /* CLUSTER_INJECTION_POINT */
#include "cluster/cluster_lck.h"	/* cluster_lck_start / wait_for_ready (1.12 Sprint A) */
#include "cluster/cluster_lms.h"	/* cluster_lms_start / wait_for_ready (spec-2.18 Sprint A) */
#include "cluster/cluster_lmon.h"	/* cluster_lmon_start / wait_for_ready (1.11 Sprint A) */
#include "cluster/cluster_scn.h"	/* SCN_NODE_ID_VALID (spec-1.16 D13) */
#include "cluster/cluster_shmem.h"	/* cluster_shmem_register_region */
#include "cluster/cluster_startup_phase.h"
#include "cluster/cluster_wal_state.h" /* spec-4.2 publish_active (phase->RUNNING) */


/*
 * Phase enum ↔ string lookup table.  Position in the array MUST line
 * up with ClusterStartupPhase enum values; out-of-range returns
 * "(unknown)".
 */
static const char *const cluster_phase_strings[] = {
	"pre_init",		   /* CLUSTER_PHASE_PRE_INIT  = 0 */
	"phase0_base",	   /* CLUSTER_PHASE_0_BASE    = 1 */
	"phase1_cluster",  /* CLUSTER_PHASE_1_CLUSTER = 2 */
	"phase2_lock",	   /* CLUSTER_PHASE_2_LOCK    = 3 */
	"phase3_recovery", /* CLUSTER_PHASE_3_RECOVERY= 4 */
	"phase4_normal",   /* CLUSTER_PHASE_4_NORMAL  = 5 */
	"running",		   /* CLUSTER_PHASE_RUNNING   = 6 */
	"shutdown"		   /* CLUSTER_PHASE_SHUTDOWN  = 7 */
};

/*
 * Phase state in shared memory (spec-1.10.1 D1 F1 hardening).
 *
 *	Five static globals (current_phase / phase_start_times[] /
 *	phase_history[] / count / head) used to live here.  EXEC_BACKEND/
 *	Windows children re-execed and re-ran their static initializers,
 *	so they observed the PRE_INIT seed regardless of postmaster's
 *	actual state.  Migrating to shmem gives every process a coherent
 *	view backed by ShmemInitStruct, with LWLock LWTRANCHE_CLUSTER_
 *	STARTUP_PHASE guarding writes (postmaster, LW_EXCLUSIVE) and
 *	reads (any backend, LW_SHARED).
 *
 *	cluster_phase_state is set by cluster_phase_shmem_init() during
 *	postmaster startup and (on EXEC_BACKEND children) during
 *	SubPostmasterMain shmem rebind.  It stays NULL only inside the
 *	cluster_unit test harness when the helper is not invoked --
 *	accessors all early-return safe defaults in that case.
 */
static ClusterPhaseSharedState *cluster_phase_state = NULL;


/* ============================================================
 * Public accessors (read-only; callable from any backend)
 * ============================================================ */

const char *
cluster_startup_phase_to_string(ClusterStartupPhase phase)
{
	if ((int)phase < 0 || (int)phase > CLUSTER_PHASE_LAST)
		return "(unknown)";
	return cluster_phase_strings[(int)phase];
}


ClusterStartupPhase
cluster_current_phase(void)
{
	ClusterStartupPhase result;

	if (cluster_phase_state == NULL)
		return CLUSTER_PHASE_PRE_INIT;

	LWLockAcquire(&cluster_phase_state->lwlock, LW_SHARED);
	result = cluster_phase_state->current_phase;
	LWLockRelease(&cluster_phase_state->lwlock);
	return result;
}


TimestampTz
cluster_phase_started_at(ClusterStartupPhase phase)
{
	TimestampTz result;

	if ((int)phase < 0 || (int)phase > CLUSTER_PHASE_LAST)
		return 0;
	if (cluster_phase_state == NULL)
		return 0;

	LWLockAcquire(&cluster_phase_state->lwlock, LW_SHARED);
	result = cluster_phase_state->phase_start_times[(int)phase];
	LWLockRelease(&cluster_phase_state->lwlock);
	return result;
}


int64
cluster_phase_elapsed_seconds(void)
{
	ClusterStartupPhase phase;
	TimestampTz started;
	long secs;
	int usecs;

	if (cluster_phase_state == NULL)
		return 0;

	LWLockAcquire(&cluster_phase_state->lwlock, LW_SHARED);
	phase = cluster_phase_state->current_phase;
	started = cluster_phase_state->phase_start_times[(int)phase];
	LWLockRelease(&cluster_phase_state->lwlock);

	if (started == 0)
		return 0;

	TimestampDifference(started, GetCurrentTimestamp(), &secs, &usecs);
	return (int64)secs;
}


void
cluster_phase_history_format(char *buf, size_t size)
{
	int start;
	int i;
	int emit_count;
	size_t offset = 0;
	int local_count;
	int local_head;
	PhaseHistoryEntry local_history[CLUSTER_PHASE_HISTORY_RING_SIZE];

	if (buf == NULL || size == 0)
		return;
	buf[0] = '\0';

	if (cluster_phase_state == NULL)
		return;

	/*
	 * Snapshot the ring under LW_SHARED so the formatter can iterate
	 * without holding the lock during the (potentially many) snprintf
	 * calls.  Eight entries fit comfortably on the stack.
	 */
	LWLockAcquire(&cluster_phase_state->lwlock, LW_SHARED);
	local_count = cluster_phase_state->phase_history_count;
	local_head = cluster_phase_state->phase_history_head;
	memcpy(local_history, cluster_phase_state->phase_history, sizeof(local_history));
	LWLockRelease(&cluster_phase_state->lwlock);

	emit_count = (local_count < CLUSTER_PHASE_HISTORY_RING_SIZE) ? local_count
																 : CLUSTER_PHASE_HISTORY_RING_SIZE;

	if (emit_count == 0)
		return;

	/*
	 * Walk in chronological order: oldest entry is at head when the
	 * ring is full, otherwise at slot 0.
	 */
	start = (local_count < CLUSTER_PHASE_HISTORY_RING_SIZE) ? 0 : local_head;

	for (i = 0; i < emit_count; i++) {
		int idx = (start + i) % CLUSTER_PHASE_HISTORY_RING_SIZE;
		const PhaseHistoryEntry *entry = &local_history[idx];
		const char *phase_str = cluster_startup_phase_to_string(entry->phase);
		const char *ts_str = timestamptz_to_str(entry->entered_at);
		int n;

		n = snprintf(buf + offset, size - offset, "%s%s@%s", (i > 0) ? "," : "", phase_str, ts_str);
		if (n < 0 || (size_t)n >= size - offset) {
			/* Truncate cleanly; the ring is bounded so this rarely fires. */
			break;
		}
		offset += (size_t)n;
	}
}


/* ============================================================
 * Phase shmem region helpers (spec-1.10.1 D1 F1)
 * ============================================================ */

Size
cluster_phase_shmem_size(void)
{
	return MAXALIGN(sizeof(ClusterPhaseSharedState));
}


void
cluster_phase_shmem_init(void)
{
	bool found;

	cluster_phase_state = (ClusterPhaseSharedState *)ShmemInitStruct(
		"pgrac cluster startup phase", sizeof(ClusterPhaseSharedState), &found);

	if (!found) {
		/*
		 * First attach (postmaster on POSIX fork; postmaster on
		 * EXEC_BACKEND too -- the EXEC_BACKEND child takes the found=true
		 * branch).  Initialise everything to the PRE_INIT seed and
		 * register the LWLock with its dedicated tranche.
		 */
		memset(cluster_phase_state, 0, sizeof(*cluster_phase_state));
		LWLockInitialize(&cluster_phase_state->lwlock, LWTRANCHE_CLUSTER_STARTUP_PHASE);
		cluster_phase_state->current_phase = CLUSTER_PHASE_PRE_INIT;
	}
}


/*
 * cluster_phase_shmem_region -- spec-1.3 shmem registry descriptor.
 *
 *	Registered from cluster_init_shmem_module() in cluster_shmem.c so
 *	the registry is the single dispatch path; cluster_request_shmem
 *	iterates and calls cluster_phase_shmem_size().
 */
static const ClusterShmemRegion cluster_phase_region = {
	.name = "pgrac cluster startup phase",
	.size_fn = cluster_phase_shmem_size,
	.init_fn = cluster_phase_shmem_init,
	.lwlock_count = 1, /* the embedded ClusterPhaseSharedState.lwlock */
	.owner_subsys = "cluster_startup_phase",
	.reserved_flags = 0,
};


void
cluster_phase_shmem_register(void)
{
	cluster_shmem_register_region(&cluster_phase_region);
}


/* ============================================================
 * Phase advance (driver-internal API; HC2 SSOT, HC1 postmaster-only)
 * ============================================================ */

void
cluster_advance_phase(ClusterStartupPhase target)
{
	ClusterStartupPhase prev;
	TimestampTz now;
	int slot;

	/*
	 * HC1: postmaster-only.  This is the ONLY function that mutates
	 * the shmem-backed phase state; calling it from a child backend
	 * would corrupt the postmaster's view of its own startup.
	 */
	Assert(!IsUnderPostmaster);

	/*
	 * spec-1.10.1 D1 F1: phase state lives in shmem now.  This guards
	 * against early callers (cluster_init_shmem_module not yet run)
	 * by failing loudly rather than dereferencing NULL.
	 */
	if (cluster_phase_state == NULL)
		ereport(FATAL,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("cluster_advance_phase called before phase shmem state was attached"),
				 errhint("cluster_phase_shmem_init() must run during "
						 "CreateSharedMemoryAndSemaphores().")));

	prev = cluster_phase_state->current_phase;

	/*
	 * Strict transition rules.  The only legitimate transitions are:
	 *   prev + 1 == target          (forward step)
	 *   target == CLUSTER_PHASE_SHUTDOWN  (any phase can enter shutdown)
	 * Everything else is a programming error -> ereport(FATAL).
	 *
	 * Validate before taking the lock so the FATAL stack is shorter.
	 */
	if (target == CLUSTER_PHASE_SHUTDOWN) {
		/* allowed from any current phase */
	} else if ((int)target == (int)prev + 1) {
		/* allowed forward step */
	} else {
		ereport(FATAL, (errcode(ERRCODE_CLUSTER_PHASE_PRECONDITION_FAILED),
						errmsg("invalid cluster phase transition: %s -> %s",
							   cluster_startup_phase_to_string(prev),
							   cluster_startup_phase_to_string(target)),
						errdetail("Cluster startup phases must advance strictly +1 or "
								  "transition to SHUTDOWN.  Backward transitions and "
								  "skipped phases indicate a programming error in "
								  "cluster_run_startup_sequence() driver loop.")));
	}

	/*
	 * Fire the prev phase's "-exit" injection point before switching,
	 * unless we're transitioning out of PRE_INIT (no exit for the
	 * sentinel) or into SHUTDOWN (the prev phase may not have a
	 * meaningful exit -- shutdown is a special transition).
	 *
	 * Inject points run outside the LWLock to avoid Assert(!locked)
	 * paths inside ereport in fault-injected sleep modes.
	 */
	if (prev != CLUSTER_PHASE_PRE_INIT && target != CLUSTER_PHASE_SHUTDOWN) {
		switch (prev) {
		case CLUSTER_PHASE_0_BASE:
			CLUSTER_INJECTION_POINT("cluster-startup-phase-0-exit");
			break;
		case CLUSTER_PHASE_1_CLUSTER:
			CLUSTER_INJECTION_POINT("cluster-startup-phase-1-exit");
			break;
		case CLUSTER_PHASE_2_LOCK:
			CLUSTER_INJECTION_POINT("cluster-startup-phase-2-exit");
			break;
		case CLUSTER_PHASE_3_RECOVERY:
			CLUSTER_INJECTION_POINT("cluster-startup-phase-3-exit");
			break;
		case CLUSTER_PHASE_4_NORMAL:
			CLUSTER_INJECTION_POINT("cluster-startup-phase-4-exit");
			break;
		default:
			break;
		}
	}

	now = GetCurrentTimestamp();

	/* Commit the transition under LW_EXCLUSIVE (HC2 SSOT mutate). */
	LWLockAcquire(&cluster_phase_state->lwlock, LW_EXCLUSIVE);
	cluster_phase_state->current_phase = target;
	cluster_phase_state->phase_start_times[(int)target] = now;

	/* Append to fixed-size history ring (HC5). */
	slot = cluster_phase_state->phase_history_head;
	cluster_phase_state->phase_history[slot].phase = target;
	cluster_phase_state->phase_history[slot].entered_at = now;
	cluster_phase_state->phase_history_head = (slot + 1) % CLUSTER_PHASE_HISTORY_RING_SIZE;
	cluster_phase_state->phase_history_count++;
	LWLockRelease(&cluster_phase_state->lwlock);

	/*
	 * Update the legacy cluster_phase const char * mirror (HC2: this
	 * is the ONLY writer in the codebase).  cluster_startup_phase_to_
	 * string returns a pointer to a static string literal so no
	 * lifetime concerns; child backends inherit this pointer at fork
	 * time.  Note: under EXEC_BACKEND the legacy mirror starts at
	 * "pre_init" again in each child.  Backends needing the live phase
	 * value should call cluster_current_phase() (shmem-backed) and
	 * cluster_startup_phase_to_string(); the mirror is retained for
	 * cluster_elog.c log decorations only.
	 */
	cluster_phase = cluster_startup_phase_to_string(target);

	/*
	 * Phase enter logging.  LOG so it's visible at default verbosity
	 * (postmaster startup is the only realistic observation channel
	 * when phase machinery is mid-flight; pg_cluster_state and
	 * pg_stat_activity require SQL access which is not yet up).
	 */
	ereport(LOG, (errmsg("cluster startup: %s -> %s", cluster_startup_phase_to_string(prev),
						 cluster_startup_phase_to_string(target))));

	/* Fire the new phase's "-enter" injection point. */
	switch (target) {
	case CLUSTER_PHASE_0_BASE:
		CLUSTER_INJECTION_POINT("cluster-startup-phase-0-enter");
		break;
	case CLUSTER_PHASE_1_CLUSTER:
		CLUSTER_INJECTION_POINT("cluster-startup-phase-1-enter");
		break;
	case CLUSTER_PHASE_2_LOCK:
		CLUSTER_INJECTION_POINT("cluster-startup-phase-2-enter");
		break;
	case CLUSTER_PHASE_3_RECOVERY:
		CLUSTER_INJECTION_POINT("cluster-startup-phase-3-enter");
		break;
	case CLUSTER_PHASE_4_NORMAL:
		CLUSTER_INJECTION_POINT("cluster-startup-phase-4-enter");
		break;
	default:
		break;
	}
}


/* ============================================================
 * Phase handlers (HC3 driver/handler split; handlers DO NOT call
 * cluster_advance_phase()).
 *
 *	Stage 1.10 skeleton: phase 1-3 handlers are no-op stubs.  Phase 4
 *	is also a stub at this stage -- the actual walwriter / bgwriter /
 *	checkpointer / autovacuum / etc. spawn happens in PG's PostmasterMain
 *	later in the startup sequence (between cluster_run_startup_sequence
 *	and the ServerLoop entry).  Handler bodies are placeholders for
 *	1.11-1.14 / Stage 2-4 replacement.
 * ============================================================ */

/* Forward decls used by multi-child phase handlers. */
static int cluster_phase_timeout_for(ClusterStartupPhase phase);
static int cluster_phase_remaining_budget_ms(TimestampTz deadline, int driver_buffer_ms);

static PhaseRunResult
phase_1_handler(PhaseRunFailContext *fail_ctx)
{
	int lmon_pid;
	bool ready;
	int wait_budget_ms;

	Assert(!IsUnderPostmaster);
	Assert(fail_ctx != NULL);

	if (!cluster_enabled) {
		elog(DEBUG1, "cluster phase 1: cluster.enabled=false; skipping LMON "
					 "spawn (degraded to spec-1.10 stub behavior)");
		return PHASE_RUN_OK;
	}

	lmon_pid = cluster_lmon_start();
	if (lmon_pid == 0) {
		/*
		 * Spec-1.11.1 F13 (codex round 4 P2/P3 fix): write the LMON-
		 * specific SQLSTATE into fail_ctx so the driver's FATAL exit
		 * carries 53R0A (was generic 53R09 before F13).  Sprint B's
		 * LOG-only ereport is removed -- single FATAL path is cleaner
		 * for external supervisors / TAP tests.
		 */
		fail_ctx->errcode = ERRCODE_CLUSTER_LMON_SPAWN_FAILED;
		fail_ctx->errmsg = "cluster phase 1: failed to spawn LMON aux process";
		fail_ctx->errhint = "Check fork() / system limits (ulimit -u) and "
							"postmaster log for LMON child startup errors.";
		return PHASE_RUN_FATAL;
	}

	wait_budget_ms = (cluster_phase1_timeout - 5) * 1000;
	if (wait_budget_ms < 1000)
		wait_budget_ms = 1000;

	ready = cluster_lmon_wait_for_ready(wait_budget_ms);
	if (!ready) {
		fail_ctx->errcode = ERRCODE_CLUSTER_LMON_NOT_READY;
		fail_ctx->errmsg = "cluster phase 1: LMON did not publish READY in time";
		fail_ctx->errhint = "Increase cluster.phase1_timeout or check LMON log "
							"for stuck startup; LMON status sticks at SPAWNING "
							"when the child crashed during initialization.";
		return PHASE_RUN_FATAL;
	}

	elog(DEBUG1,
		 "cluster phase 1: LMON ready (pid %d); interconnect listener / "
		 "heartbeat consumer remain stubs (Stage 1.15+)",
		 lmon_pid);
	return PHASE_RUN_OK;
}


static PhaseRunResult
phase_2_handler(PhaseRunFailContext *fail_ctx)
{
	int lck_pid;
	bool ready;
	int wait_budget_ms;

	Assert(!IsUnderPostmaster);
	Assert(fail_ctx != NULL);

	/* Spec-1.12 HC4: cluster.enabled=false 退化 stub (与 phase_1 对称). */
	if (!cluster_enabled) {
		elog(DEBUG1, "cluster phase 2: cluster.enabled=false; skipping LCK "
					 "spawn (degraded to spec-1.10 stub behavior)");
		return PHASE_RUN_OK;
	}

	/* Stage 1.12 Sprint A: spawn LCK and wait for READY. */
	lck_pid = cluster_lck_start();
	if (lck_pid == 0) {
		fail_ctx->errcode = ERRCODE_CLUSTER_LCK_SPAWN_FAILED;
		fail_ctx->errmsg = "cluster phase 2: failed to spawn LCK aux process";
		fail_ctx->errhint = "Check fork() / system limits (ulimit -u) and "
							"postmaster log for LCK child startup errors.";
		return PHASE_RUN_FATAL;
	}

	wait_budget_ms = (cluster_phase2_timeout - 5) * 1000;
	if (wait_budget_ms < 1000)
		wait_budget_ms = 1000;

	ready = cluster_lck_wait_for_ready(wait_budget_ms);
	if (!ready) {
		fail_ctx->errcode = ERRCODE_CLUSTER_LCK_NOT_READY;
		fail_ctx->errmsg = "cluster phase 2: LCK did not publish READY in time";
		fail_ctx->errhint = "Increase cluster.phase2_timeout or check LCK log "
							"for stuck startup; LCK status sticks at SPAWNING "
							"when the child crashed during initialization.";
		return PHASE_RUN_FATAL;
	}

	elog(DEBUG1,
		 "cluster phase 2: LCK ready (pid %d); LMS starts under PM_RUN "
		 "ServerLoop respawn, LMD remains stub (Stage 2+ GES feature)",
		 lck_pid);
	return PHASE_RUN_OK;
}


static PhaseRunResult
phase_3_handler(PhaseRunFailContext *fail_ctx pg_attribute_unused())
{
	Assert(!IsUnderPostmaster);
	elog(DEBUG1, "Phase 3 stub: PG-native startup process unchanged; "
				 "Recovery Coordinator / merged recovery deferred to Stage 4 spec");
	return PHASE_RUN_OK;
}


/*
 * cluster_phase_remaining_budget_ms -- single-deadline remaining budget.
 *
 *	Returns max(deadline - now - driver_buffer_ms, 100ms floor).  Each
 *	child wait inside phase_4_handler computes its budget off the same
 *	phase4_deadline so the sum of all children's wait times cannot
 *	exceed cluster.phase4_timeout.  Without this, two serial waits
 *	(DIAG + Cluster Stats) each of (phase4_timeout - 5s) would total
 *	2 * (30s - 5s) = 50s, breaching the 30s contract.
 *
 *	Driver buffer (5s default) is reserved for the outer driver's
 *	TimestampDifferenceExceeds check so 53R09 PHASE_TRANSITION_TIMEOUT
 *	can still trip cleanly if a child wait runs to its full slice.
 *	Min 100ms floor guarantees at least one polling iteration so
 *	wait_for_ready does not spuriously fail on a tight overflow.
 */
static int
cluster_phase_remaining_budget_ms(TimestampTz deadline, int driver_buffer_ms)
{
	long secs;
	int microsecs;
	long remaining_ms;

	TimestampDifference(GetCurrentTimestamp(), deadline, &secs, &microsecs);
	remaining_ms = secs * 1000 + microsecs / 1000 - driver_buffer_ms;
	return remaining_ms > 100 ? (int)remaining_ms : 100;
}

static PhaseRunResult
phase_4_handler(PhaseRunFailContext *fail_ctx)
{
	int diag_pid;
	int stats_pid;
	int cssd_pid;
	int qvotec_pid;
	int diag_remaining_ms;
	int stats_remaining_ms;
	int cssd_remaining_ms;
	int qvotec_remaining_ms;
	TimestampTz phase4_start;
	TimestampTz phase4_deadline;

	Assert(!IsUnderPostmaster);

	/*
	 * HC4 (spec-1.13 §1.4 #4 / spec-1.14 §1.4): if cluster.enabled =
	 * false, phase 4 degrades to spec-1.10 stub behavior — no DIAG
	 * spawn AND no Cluster Stats spawn, no FATAL.  Tested by 063 L10
	 * (DIAG-only) + 064 L10 (DIAG + Cluster Stats双 process disabled).
	 */
	if (!cluster_enabled) {
		elog(DEBUG1, "cluster phase 4: cluster.enabled=false; skipping DIAG + "
					 "Cluster Stats + CSSD + QVOTEC spawn (degraded to spec-1.10 "
					 "stub behavior).  PG-native walwriter / bgwriter / "
					 "checkpointer / autovacuum spawn unchanged.");
		return PHASE_RUN_OK;
	}

	/*
	 * spec-1.14 Q3 user 修订: phase 4 single deadline pattern.
	 *
	 *	Both child waits below compute remaining budget off the same
	 *	phase4_deadline (= phase4_start + cluster.phase4_timeout).
	 *	This caps the sum of all child waits so phase 4 cannot run
	 *	past cluster.phase4_timeout regardless of how many children
	 *	live in this phase (1.14: DIAG + Cluster Stats; Stage 2+ may
	 *	add Sinval Broadcaster etc.).
	 */
	phase4_start = GetCurrentTimestamp();
	phase4_deadline = TimestampTzPlusMilliseconds(
		phase4_start, cluster_phase_timeout_for(CLUSTER_PHASE_4_NORMAL) * 1000);

	/* ----------
	 * spec-1.13 D6: DIAG spawn + sync wait ready (first phase 4 child).
	 * ----------
	 */
	diag_pid = cluster_diag_start();
	if (diag_pid <= 0) {
		fail_ctx->errcode = ERRCODE_CLUSTER_DIAG_SPAWN_FAILED;
		fail_ctx->errmsg = "cluster phase 4: failed to spawn DIAG aux process";
		fail_ctx->errhint = "Check postmaster log for fork() error.  Confirm OS process "
							"limits (ulimit -u) leave room for the DIAG aux process; if "
							"the limit is exhausted, raise it via ulimit or systemd "
							"LimitNPROC and restart postmaster.";
		return PHASE_RUN_FATAL;
	}

	diag_remaining_ms = cluster_phase_remaining_budget_ms(phase4_deadline, 5000);
	if (!cluster_diag_wait_for_ready(diag_remaining_ms)) {
		fail_ctx->errcode = ERRCODE_CLUSTER_DIAG_NOT_READY;
		fail_ctx->errmsg = "cluster phase 4: DIAG did not publish READY in time";
		fail_ctx->errhint = "Check postmaster log for DIAG-side errors.  If DIAG is "
							"slow on this hardware, raise cluster.phase4_timeout (PGC_SIGHUP).";
		return PHASE_RUN_FATAL;
	}

	/* ----------
	 * spec-1.14 D6: Cluster Stats spawn + sync wait ready (second
	 * phase 4 child).  Remaining budget recomputed off the same
	 * phase4_deadline (Q3 single deadline pattern).
	 * ----------
	 */
	stats_pid = cluster_stats_start();
	if (stats_pid <= 0) {
		fail_ctx->errcode = ERRCODE_CLUSTER_STATS_SPAWN_FAILED;
		fail_ctx->errmsg = "cluster phase 4: failed to spawn Cluster Stats aux process";
		fail_ctx->errhint = "Check postmaster log for fork() error.  Confirm OS process "
							"limits (ulimit -u) leave room for the Cluster Stats aux "
							"process; if the limit is exhausted, raise it via ulimit "
							"or systemd LimitNPROC and restart postmaster.";
		return PHASE_RUN_FATAL;
	}

	stats_remaining_ms = cluster_phase_remaining_budget_ms(phase4_deadline, 5000);
	if (!cluster_stats_wait_for_ready(stats_remaining_ms)) {
		fail_ctx->errcode = ERRCODE_CLUSTER_STATS_NOT_READY;
		fail_ctx->errmsg = "cluster phase 4: Cluster Stats did not publish READY in time";
		fail_ctx->errhint = "Check postmaster log for Cluster Stats-side errors.  If "
							"Cluster Stats is slow on this hardware, raise "
							"cluster.phase4_timeout (PGC_SIGHUP).";
		return PHASE_RUN_FATAL;
	}

	/* ----------
	 * spec-2.5 D5: CSSD spawn + sync wait ready (third phase 4 child).
	 * Same Q3 single deadline pattern shared with DIAG + Stats.
	 * Step 5 D10 lands proper SQLSTATEs (53R30 / 53R31);Step 4 reuses
	 * Cluster Stats SQLSTATE codes as placeholders to keep the
	 * compile-time link clean.
	 * ----------
	 */
	cssd_pid = cluster_cssd_start();
	if (cssd_pid <= 0) {
		fail_ctx->errcode = ERRCODE_CLUSTER_CSSD_SPAWN_FAILED;
		fail_ctx->errmsg = "cluster phase 4: failed to spawn CSSD aux process";
		fail_ctx->errhint = "Check postmaster log for fork() error.  Confirm OS "
							"process limits leave room for the CSSD aux process.";
		return PHASE_RUN_FATAL;
	}

	cssd_remaining_ms = cluster_phase_remaining_budget_ms(phase4_deadline, 5000);
	if (!cluster_cssd_wait_for_ready(cssd_remaining_ms)) {
		fail_ctx->errcode = ERRCODE_CLUSTER_CSSD_NOT_READY;
		fail_ctx->errmsg = "cluster phase 4: CSSD did not publish READY in time";
		fail_ctx->errhint = "Check postmaster log for CSSD-side errors.  If CSSD is "
							"slow on this hardware, raise cluster.phase4_timeout "
							"(PGC_SIGHUP).";
		return PHASE_RUN_FATAL;
	}

	/* ----------
	 * spec-2.6 Sprint A Step 3 D8 — QVOTEC spawn + sync wait ready.
	 * Same Q3 single deadline pattern shared with DIAG + Stats + CSSD.
	 * ----------
	 */
	qvotec_pid = cluster_qvotec_start();
	if (qvotec_pid <= 0) {
		fail_ctx->errcode = ERRCODE_CLUSTER_QVOTEC_SPAWN_FAILED;
		fail_ctx->errmsg = "cluster phase 4: failed to spawn QVOTEC aux process";
		fail_ctx->errhint = "Check postmaster log for fork() error.  Confirm OS "
							"process limits leave room for the QVOTEC aux process.";
		return PHASE_RUN_FATAL;
	}

	qvotec_remaining_ms = cluster_phase_remaining_budget_ms(phase4_deadline, 5000);
	if (!cluster_qvotec_wait_for_ready(qvotec_remaining_ms)) {
		fail_ctx->errcode = ERRCODE_CLUSTER_QVOTEC_NOT_READY;
		fail_ctx->errmsg = "cluster phase 4: QVOTEC did not publish READY in time";
		fail_ctx->errhint = "Check postmaster log for QVOTEC-side errors.  If QVOTEC "
							"is slow on this hardware, raise cluster.phase4_timeout "
							"(PGC_SIGHUP).";
		return PHASE_RUN_FATAL;
	}

	elog(DEBUG1,
		 "cluster phase 4: DIAG ready (pid %d) + Cluster Stats ready (pid %d) + "
		 "CSSD ready (pid %d) + QVOTEC ready (pid %d).  PG-native "
		 "walwriter / bgwriter / checkpointer / autovacuum spawn unchanged.  "
		 "Sinval Broadcaster / Recovery Coordinator deferred to Stage 2+.",
		 diag_pid, stats_pid, cssd_pid, qvotec_pid);

	return PHASE_RUN_OK;
}


/* ============================================================
 * Sequence drivers (HC1 postmaster-only)
 * ============================================================ */

/*
 * Static dispatch table from phase to handler.  Indexed by
 * ClusterStartupPhase enum value.  PRE_INIT / 0_BASE / RUNNING /
 * SHUTDOWN have NULL because they don't run a phase handler -- their
 * transitions are driven directly by cluster_advance_phase().
 */
typedef PhaseRunResult (*ClusterPhaseHandler)(PhaseRunFailContext *fail_ctx);

static const ClusterPhaseHandler phase_handlers[CLUSTER_PHASE_LAST + 1]
	= { [CLUSTER_PHASE_PRE_INIT] = NULL,
		[CLUSTER_PHASE_0_BASE] = NULL,
		[CLUSTER_PHASE_1_CLUSTER] = phase_1_handler,
		[CLUSTER_PHASE_2_LOCK] = phase_2_handler,
		[CLUSTER_PHASE_3_RECOVERY] = phase_3_handler,
		[CLUSTER_PHASE_4_NORMAL] = phase_4_handler,
		[CLUSTER_PHASE_RUNNING] = NULL,
		[CLUSTER_PHASE_SHUTDOWN] = NULL };


/*
 * cluster_phase_timeout_for -- read the GUC seconds value for a phase.
 *
 *	Spec-1.10.1 D2 F2 / Q2=D: driver synchronous elapsed check uses
 *	this helper to fetch the deadline that was promised by GUC help
 *	text.  Phase 0 has no GUC (skeleton trivial); only phases 1..4
 *	are bounded.
 */
static int
cluster_phase_timeout_for(ClusterStartupPhase phase)
{
	switch (phase) {
	case CLUSTER_PHASE_1_CLUSTER:
		return cluster_phase1_timeout;
	case CLUSTER_PHASE_2_LOCK:
		return cluster_phase2_timeout;
	case CLUSTER_PHASE_3_RECOVERY:
		return cluster_phase3_timeout;
	case CLUSTER_PHASE_4_NORMAL:
		return cluster_phase4_timeout;
	default:
		/* phases without timeout GUC (PRE_INIT / 0_BASE / RUNNING / SHUTDOWN) */
		return 0;
	}
}


/*
 * cluster_phase_fail_inject -- fire the per-phase "-fail" inject point.
 *
 *	Spec-1.10.1 D6 F6: the driver invokes this on every FATAL exit
 *	path (handler PHASE_RUN_FATAL + driver elapsed timeout) so the
 *	previously dead cluster-startup-phase-N-fail injection points are
 *	reachable from tests.  Inject framework treats this as a no-op
 *	when no fault is armed.
 */
static void
cluster_phase_fail_inject(ClusterStartupPhase phase)
{
	switch (phase) {
	case CLUSTER_PHASE_1_CLUSTER:
		CLUSTER_INJECTION_POINT("cluster-startup-phase-1-fail");
		break;
	case CLUSTER_PHASE_2_LOCK:
		CLUSTER_INJECTION_POINT("cluster-startup-phase-2-fail");
		break;
	case CLUSTER_PHASE_3_RECOVERY:
		CLUSTER_INJECTION_POINT("cluster-startup-phase-3-fail");
		break;
	case CLUSTER_PHASE_4_NORMAL:
		CLUSTER_INJECTION_POINT("cluster-startup-phase-4-fail");
		break;
	default:
		break;
	}
}


void
cluster_run_startup_sequence(void)
{
	ClusterStartupPhase phase;

	/*
	 * HC1 PostmasterMain-only.  This must be called from PostmasterMain
	 * function body, NOT from inside CreateSharedMemoryAndSemaphores
	 * (the latter is also called by SubPostmasterMain on EXEC_BACKEND
	 * children; running phase machinery there would violate Postmaster-
	 * once semantics, CLAUDE.md rule 16 §Postmaster-once).
	 */
	Assert(!IsUnderPostmaster);

	CLUSTER_INJECTION_POINT("cluster-run-startup-top");

	/*
	 * Driver loop.  Walk Phase 0 -> 1 -> 2 -> 3 -> 4.  The driver
	 * advances; handlers only do work + return status (HC3).  Phase
	 * RUNNING is NOT advanced from this driver -- spec-1.10.1 D4 F4
	 * pushes that transition to cluster_finalize_startup_running()
	 * called from PostmasterMain just before ServerLoop() so phase=
	 * running accurately reflects "PostgreSQL ready to accept
	 * connections", not just "pgrac skeleton finished".
	 *
	 * Phase 0 entry is the "post-shmem ready" point reached by the
	 * caller (PostmasterMain after CreateSharedMemoryAndSemaphores +
	 * cluster_init).  We advance into 0_BASE here as the explicit
	 * skeleton starting point.
	 */
	cluster_advance_phase(CLUSTER_PHASE_0_BASE);

	/*
	 * Spec-1.13 v0.2 Q2 A': cluster_run_startup_sequence() walks
	 * phase 1 -> 3 ONLY.  Phase 4 (the post-recovery / post-PM_RUN
	 * normal-running phase that DIAG and Cluster Stats spawn into)
	 * is driven by cluster_run_phase4_sequence() below, which the
	 * postmaster reaper invokes after the startup process has finished
	 * recovery and pmState transitions to PM_RUN.  Splitting the
	 * driver moves DIAG spawn from "pre-recovery" (the original 1.10
	 * skeleton timing) to "post-recovery / DB OPEN" (correct Oracle
	 * DIAG semantics).
	 */
	for (phase = CLUSTER_PHASE_1_CLUSTER; phase <= CLUSTER_PHASE_3_RECOVERY; phase++) {
		PhaseRunResult result;
		ClusterPhaseHandler handler;
		TimestampTz started;
		TimestampTz now;
		long elapsed_secs;
		int microsecs;
		int timeout_secs;

		/*
		 * Spec-1.10.1 D2 F2 / Q2=D: driver synchronous elapsed check.
		 *
		 *	Record the phase wall-clock start before cluster_advance_
		 *	phase() so the measured interval covers both the transition
		 *	overhead (including any -enter inject point sleeps used by
		 *	tests to simulate a stuck phase) AND the handler body.
		 *	After handler returns the driver compares elapsed against
		 *	the per-phase GUC timeout and ereport(FATAL) on overrun.
		 *
		 *	Handlers MUST self-bound any blocking wait via WaitLatch
		 *	(..., timeout_ms, WAIT_EVENT_CLUSTER_STARTUP_PHASE_N) per
		 *	the contract in cluster_startup_phase.h; a handler that
		 *	hangs without using WaitLatch+timeout will hang the entire
		 *	postmaster, defeating this enforcement.
		 */
		PhaseRunFailContext fail_ctx = { 0 };

		started = GetCurrentTimestamp();
		cluster_advance_phase(phase);

		handler = phase_handlers[(int)phase];
		/*
		 * Every iterated phase (1..4) has a handler defined in
		 * phase_handlers[].  If a future amend leaves a slot NULL we
		 * fail loudly rather than dereference it (cppcheck flagged
		 * the prior Assert-only form as a potential null deref).
		 */
		if (handler == NULL)
			ereport(FATAL, (errcode(ERRCODE_INTERNAL_ERROR),
							errmsg("cluster startup phase %s has no handler in dispatch table",
								   cluster_startup_phase_to_string(phase))));

		/*
		 * Spec-1.11.1 F13 (codex round 4 P2/P3 fix): handler writes
		 * PHASE_RUN_FATAL diagnostic into fail_ctx (errcode/errmsg/
		 * errhint).  Driver loop's FATAL path uses fail_ctx values
		 * when non-zero/non-NULL, otherwise falls back to generic
		 * 53R09 PHASE_PRECONDITION_FAILED + standard message.
		 */
		result = handler(&fail_ctx);

		/*
		 * Spec-1.10.2 F8 (2026-05-04 codex review fix): use
		 * TimestampDifferenceExceeds for millisecond-precision boundary
		 * checking.  The prior comparison `elapsed_secs > timeout_secs`
		 * floored sub-second elapsed and yielded false negatives when
		 * elapsed was 1.0s..1.999s with timeout_secs == 1 (boundary
		 * leak).  TimestampDifferenceExceeds(start, stop, ms) returns
		 * true iff (stop - start) exceeds ms, with full us precision.
		 *
		 * Capture `now` once so the FATAL errmsg reports the same
		 * sample the deadline was checked against.
		 */
		now = GetCurrentTimestamp();
		timeout_secs = cluster_phase_timeout_for(phase);

		if (timeout_secs > 0 && TimestampDifferenceExceeds(started, now, timeout_secs * 1000)) {
			TimestampDifference(started, now, &elapsed_secs, &microsecs);
			cluster_phase_fail_inject(phase);
			ereport(FATAL, (errcode(ERRCODE_CLUSTER_PHASE_TRANSITION_TIMEOUT),
							errmsg("cluster startup phase %s exceeded timeout (%ld.%03d s > %d s)",
								   cluster_startup_phase_to_string(phase), elapsed_secs,
								   microsecs / 1000, timeout_secs),
							errhint("Increase cluster.phase%d_timeout GUC or "
									"fix handler hang (handler must self-bound "
									"blocking waits via WaitLatch+timeout per the "
									"phase handler contract).",
									(int)phase - 1)));
		}

		/*
		 * Spec-1.10.1 D3 F3: explicit switch on PhaseRunResult.  The
		 * earlier Assert(result == PHASE_RUN_OK) tripped only in debug
		 * builds, leaving production builds to silently advance on
		 * unknown values.  Handle every case explicitly.
		 */
		switch (result) {
		case PHASE_RUN_OK:
			break;

		case PHASE_RUN_FATAL:
			cluster_phase_fail_inject(phase);
			/*
			 * Spec-1.11.1 F13: use handler-supplied SQLSTATE / errmsg
			 * / errhint when present (e.g. 53R0A LMON_SPAWN_FAILED for
			 * phase_1_handler), else fall back to generic 53R09
			 * PHASE_PRECONDITION_FAILED.
			 */
			ereport(FATAL,
					(errcode(fail_ctx.errcode != 0 ? fail_ctx.errcode
												   : ERRCODE_CLUSTER_PHASE_PRECONDITION_FAILED),
					 errmsg("cluster startup phase %s failed: %s",
							cluster_startup_phase_to_string(phase),
							fail_ctx.errmsg != NULL ? fail_ctx.errmsg
													: "see postmaster log for diagnostics"),
					 errhint("%s",
							 fail_ctx.errhint != NULL
								 ? fail_ctx.errhint
								 : "spec-1.10 / spec-1.11+ document the phase handler contract.")));
			break;

		case PHASE_RUN_RETRY:
			/*
			 * RETRY enum value is reserved for spec-1.11+ retry semantics
			 * (per-phase retry count + backoff schedule).  At spec-1.10.1
			 * the driver does not implement retry, so a RETRY return is a
			 * programming error: ereport(FATAL) rather than silently
			 * advancing.
			 */
			cluster_phase_fail_inject(phase);
			ereport(FATAL, (errcode(ERRCODE_INTERNAL_ERROR),
							errmsg("cluster startup phase %s returned PHASE_RUN_RETRY "
								   "but driver does not implement retry yet",
								   cluster_startup_phase_to_string(phase)),
							errhint("Future spec must define per-phase retry "
									"count + backoff before handlers may return "
									"PHASE_RUN_RETRY.")));
			break;

		default:
			cluster_phase_fail_inject(phase);
			ereport(FATAL, (errcode(ERRCODE_INTERNAL_ERROR),
							errmsg("cluster startup phase %s handler returned unknown "
								   "PhaseRunResult %d",
								   cluster_startup_phase_to_string(phase), (int)result)));
			break;
		}
	}

	/* Driver leaves phase machinery at CLUSTER_PHASE_3_RECOVERY.
	 * cluster_run_phase4_sequence() advances to CLUSTER_PHASE_4_NORMAL
	 * later from the reaper PM_RUN transition path. */
}


/*
 * cluster_run_phase4_sequence -- spec-1.13 v0.2 Q2 A' driver for the
 * post-recovery / post-PM_RUN phase 4 transition.
 *
 *	Walks just CLUSTER_PHASE_4_NORMAL by invoking phase_4_handler.
 *	Same per-phase timeout / FATAL / SQLSTATE machinery as
 *	cluster_run_startup_sequence().  Caller (postmaster reaper at
 *	PM_STARTUP -> PM_RUN transition) invokes this AFTER the startup
 *	process has succeeded.  cluster_finalize_startup_running() runs
 *	immediately after to advance phase machinery to RUNNING.
 *
 *	Why split: spec-1.10 originally walked phase 1->4 inside
 *	cluster_run_startup_sequence() in PostmasterMain, before
 *	StartupDataBase().  That made phase 4 fire pre-recovery, which
 *	would mis-position DIAG (1.13) and Cluster Stats (1.14) into the
 *	WAL replay window.  Round 5 (user codex review) caught this; the
 *	fix is the driver split: phase 1-3 stay in startup_sequence;
 *	phase 4 lives here.
 */
void
cluster_run_phase4_sequence(void)
{
	PhaseRunResult result;
	ClusterPhaseHandler handler;
	TimestampTz started;
	TimestampTz now;
	long elapsed_secs;
	int microsecs;
	int timeout_secs;
	const ClusterStartupPhase phase = CLUSTER_PHASE_4_NORMAL;
	PhaseRunFailContext fail_ctx = { 0 };

	Assert(!IsUnderPostmaster);

	CLUSTER_INJECTION_POINT("cluster-run-phase4-top");

	started = GetCurrentTimestamp();
	cluster_advance_phase(phase);

	handler = phase_handlers[(int)phase];
	if (handler == NULL)
		ereport(FATAL, (errcode(ERRCODE_INTERNAL_ERROR),
						errmsg("cluster startup phase %s has no handler in dispatch table",
							   cluster_startup_phase_to_string(phase))));

	result = handler(&fail_ctx);

	now = GetCurrentTimestamp();
	timeout_secs = cluster_phase_timeout_for(phase);

	if (timeout_secs > 0 && TimestampDifferenceExceeds(started, now, timeout_secs * 1000)) {
		TimestampDifference(started, now, &elapsed_secs, &microsecs);
		cluster_phase_fail_inject(phase);
		ereport(FATAL, (errcode(ERRCODE_CLUSTER_PHASE_TRANSITION_TIMEOUT),
						errmsg("cluster startup phase %s exceeded timeout (%ld.%03d s > %d s)",
							   cluster_startup_phase_to_string(phase), elapsed_secs,
							   microsecs / 1000, timeout_secs),
						errhint("Increase cluster.phase%d_timeout GUC or "
								"fix handler hang.",
								(int)phase - 1)));
	}

	switch (result) {
	case PHASE_RUN_OK:
		break;

	case PHASE_RUN_FATAL:
		cluster_phase_fail_inject(phase);
		ereport(FATAL, (errcode(fail_ctx.errcode != 0 ? fail_ctx.errcode
													  : ERRCODE_CLUSTER_PHASE_PRECONDITION_FAILED),
						errmsg("cluster startup phase %s failed: %s",
							   cluster_startup_phase_to_string(phase),
							   fail_ctx.errmsg != NULL ? fail_ctx.errmsg
													   : "see postmaster log for diagnostics"),
						errhint("%s", fail_ctx.errhint != NULL
										  ? fail_ctx.errhint
										  : "spec-1.13 documents the phase 4 handler contract.")));
		break;

	case PHASE_RUN_RETRY:
		cluster_phase_fail_inject(phase);
		ereport(FATAL, (errcode(ERRCODE_INTERNAL_ERROR),
						errmsg("cluster startup phase %s returned PHASE_RUN_RETRY "
							   "but driver does not implement retry yet",
							   cluster_startup_phase_to_string(phase))));
		break;

	default:
		cluster_phase_fail_inject(phase);
		ereport(FATAL, (errcode(ERRCODE_INTERNAL_ERROR),
						errmsg("cluster startup phase %s handler returned unknown "
							   "PhaseRunResult %d",
							   cluster_startup_phase_to_string(phase), (int)result)));
		break;
	}
}


void
cluster_finalize_startup_running(void)
{
	/*
	 * Spec-1.10.1 D4 F4: explicit RUNNING transition is now the
	 * responsibility of PostmasterMain (just before ServerLoop()) so
	 * that "phase=running" reflects PG-ready, not just pgrac-skeleton-
	 * finished.  HC1 postmaster-only.
	 *
	 * Spec-1.11.1 F9: idempotent guard.  Crash reinit also calls this
	 * to advance phase 4 -> RUNNING after the reinit cycle completes;
	 * normal startup calls it once from PostmasterMain.  An already-
	 * RUNNING state is a benign no-op rather than a strict +1
	 * cluster_advance_phase failure.
	 */
	Assert(!IsUnderPostmaster);

	if (cluster_current_phase() == CLUSTER_PHASE_RUNNING)
		return;

	/*
	 * Spec-1.16 v0.2 Q9 / D13: surface cluster.node_id BEFORE entering
	 * RUNNING so admins notice misconfiguration at startup rather than
	 * mid-transaction (per L18 startup-time validation).
	 *
	 * Spec-2.1 D2 (Stage 2.1 tightening, 2026-05-06): WARNING/FATAL dual
	 * path gated on cluster.allow_single_node:
	 *   - allow_single_node = on  (Stage 2.1 default; backward-compat):
	 *       WARNING + single-node fallback (Stage 1.16 behavior preserved
	 *       so frozen Stage 1 specs keep working unchanged).
	 *   - allow_single_node = off (Stage 2 strict mode):
	 *       FATAL -- per spec-2.0 §3 Invariant 3 "uncertainty fail-closed".
	 *
	 * cluster_enabled = off path skips entirely (no warning, no FATAL --
	 * vanilla PG behaviour).
	 */
	if (cluster_enabled && !SCN_NODE_ID_VALID(cluster_node_id)) {
		if (cluster_allow_single_node) {
			/* Stage 2.1 backward-compat path: WARNING + single-node fallback */
			ereport(WARNING, (errcode(ERRCODE_WARNING),
							  errmsg("cluster.node_id (%d) is outside the valid range 0..%d; "
									 "cluster SCN advance will silently skip",
									 cluster_node_id, SCN_MAX_VALID_NODE_ID),
							  errhint("Set cluster.node_id in postgresql.conf to an integer 0..127 "
									  "to enable SCN advance, or set cluster.enabled = off for "
									  "vanilla PG behaviour.  Currently running in single-node "
									  "compatibility mode (cluster.allow_single_node = on).  Set "
									  "cluster.allow_single_node = off to enforce strict mode.")));
		} else {
			/* Stage 2 strict path: FATAL */
			ereport(FATAL, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
							errmsg("cluster.node_id (%d) is outside the valid range 0..%d",
								   cluster_node_id, SCN_MAX_VALID_NODE_ID),
							errhint("Set cluster.node_id in postgresql.conf to an integer 0..127, "
									"or set cluster.allow_single_node = on for single-node "
									"compatibility mode.")));
		}
	}

	/*
	 * spec-2.6 Q7 v0.2 validator: multi-node + cluster.voting_disks empty +
	 * cluster.allow_single_node=off ⇒ refuse to advance to RUNNING.
	 *
	 * Without voting disks, qvotec has no quorum protocol to run; backends
	 * would be forced fail-open since the xact.c gate (P1.2) keys on
	 * cluster_voting_disks being non-empty.  In a multi-node cluster
	 * topology that combination is silent fail-open — exactly the
	 * production misconfiguration spec-2.6 was added to prevent.
	 *
	 * cluster.allow_single_node=on still allows the dev / single-node
	 * compat path (qvotec stays alive, no I/O, backends don't enforce).
	 *
	 * pgrac.conf parse already ran during phase 1 (cluster_conf_load);
	 * by the time we reach this RUNNING transition the topology is
	 * authoritative — single-node fallback when pgrac.conf is missing
	 * + allow_single_node=on returns node_count = 1, which we exempt.
	 */
	if (cluster_enabled && cluster_conf_node_count() > 1 && !cluster_allow_single_node) {
		const char *vd = cluster_voting_disks;
		bool empty = true;

		if (vd != NULL) {
			while (*vd) {
				if (*vd != ' ' && *vd != '\t' && *vd != ',') {
					empty = false;
					break;
				}
				vd++;
			}
		}

		if (empty)
			ereport(FATAL, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
							errmsg("multi-node cluster requires cluster.voting_disks to be "
								   "configured when cluster.allow_single_node=off"),
							errdetail("pgrac.conf declares %d nodes but cluster.voting_disks is "
									  "empty.  Without voting disks the cluster has no quorum "
									  "protocol and backends would silently fail-open under "
									  "partition.",
									  cluster_conf_node_count()),
							errhint("Set cluster.voting_disks in postgresql.conf to a comma-"
									"separated list of pre-formatted voting-disk file paths "
									"(odd majority recommended: 1 / 3 / 5 / 7 disks across "
									"distinct failure domains), or set cluster.allow_single_"
									"node = on for single-node development mode.")));
	}

	/*
	 * spec-4.2 v0.2 P1: publish ACTIVE to the ClusterWalState registry
	 * ONLY here -- recovery has succeeded and the node is about to
	 * serve.  A node that dies during recovery never reaches this
	 * point, so its slot keeps the previous content (EMPTY on a first
	 * boot), which is exactly the raw material spec-4.3 needs for the
	 * crashed inference.  FATAL 53RA2 inside on write failure.
	 */
	cluster_wal_state_publish_active();

	cluster_advance_phase(CLUSTER_PHASE_RUNNING);
}


void
cluster_run_shutdown_sequence(void)
{
	Assert(!IsUnderPostmaster);

	CLUSTER_INJECTION_POINT("cluster-run-shutdown-top");

	/*
	 * Spec-1.10.1 D5 F5: this entry is now wired into pmdie() in
	 * postmaster.c after children have been reaped.  pmdie may invoke
	 * the shutdown sequence more than once if a smart shutdown is
	 * upgraded to fast / immediate shutdown; cluster_advance_phase()
	 * permits any -> SHUTDOWN transition (including SHUTDOWN -> SHUTDOWN
	 * is rejected by the strict +1 check, so guard against re-entry).
	 *
	 * Stage 1.10 stub: directly transition to SHUTDOWN.  Reverse-order
	 * graceful tear-down (RUNNING -> 4 -> 3 -> 2 -> 1 -> SHUTDOWN) is
	 * deferred to 1.11-1.14 / Stage 6 once the per-phase background
	 * processes that need graceful stop are spawned.
	 */
	if (cluster_current_phase() == CLUSTER_PHASE_SHUTDOWN)
		return;

	cluster_advance_phase(CLUSTER_PHASE_SHUTDOWN);
}

#endif /* USE_PGRAC_CLUSTER */
