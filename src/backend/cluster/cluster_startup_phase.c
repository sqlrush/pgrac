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
#include "utils/elog.h"
#include "utils/timestamp.h"

#include "cluster/cluster_elog.h"	/* cluster_phase legacy mirror (HC2) */
#include "cluster/cluster_inject.h" /* CLUSTER_INJECTION_POINT */
#include "cluster/cluster_startup_phase.h"


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
 * Current phase + per-phase entry timestamps (HC2 SSOT).  Both backed
 * by static globals: postmaster startup is single-threaded, and child
 * backends inherit a snapshot via fork() that they must not mutate
 * (the cluster_advance_phase API guards against this with
 * Assert(!IsUnderPostmaster), HC1).
 */
static ClusterStartupPhase cluster_startup_phase_current = CLUSTER_PHASE_PRE_INIT;

static TimestampTz cluster_phase_start_times[CLUSTER_PHASE_LAST + 1] = { 0 };

/*
 * Fixed-size phase history ring (HC5 user 修订 5).
 *
 *	Bounded to CLUSTER_PHASE_HISTORY_RING_SIZE = 8 entries.  Each
 *	entry records a (phase, entered_at) pair.  When the ring fills,
 *	the oldest entry is overwritten.  This bounds the
 *	pg_cluster_state.phase.phase_history string size and prevents
 *	Stage 6 reconfig phase reentry from causing unbounded growth.
 */
typedef struct PhaseHistoryEntry {
	ClusterStartupPhase phase;
	TimestampTz entered_at;
} PhaseHistoryEntry;

static PhaseHistoryEntry cluster_phase_history[CLUSTER_PHASE_HISTORY_RING_SIZE] = { { 0 } };
static int cluster_phase_history_count = 0; /* total entries ever written */
static int cluster_phase_history_head = 0;	/* next slot to write (0..RING_SIZE-1) */


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
	return cluster_startup_phase_current;
}


TimestampTz
cluster_phase_started_at(ClusterStartupPhase phase)
{
	if ((int)phase < 0 || (int)phase > CLUSTER_PHASE_LAST)
		return 0;
	return cluster_phase_start_times[(int)phase];
}


int64
cluster_phase_elapsed_seconds(void)
{
	TimestampTz started = cluster_phase_started_at(cluster_startup_phase_current);
	long secs;
	int usecs;

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

	if (buf == NULL || size == 0)
		return;
	buf[0] = '\0';

	emit_count = (cluster_phase_history_count < CLUSTER_PHASE_HISTORY_RING_SIZE)
					 ? cluster_phase_history_count
					 : CLUSTER_PHASE_HISTORY_RING_SIZE;

	if (emit_count == 0)
		return;

	/*
	 * Walk in chronological order: oldest entry is at head when the
	 * ring is full, otherwise at slot 0.
	 */
	start = (cluster_phase_history_count < CLUSTER_PHASE_HISTORY_RING_SIZE)
				? 0
				: cluster_phase_history_head;

	for (i = 0; i < emit_count; i++) {
		int idx = (start + i) % CLUSTER_PHASE_HISTORY_RING_SIZE;
		const PhaseHistoryEntry *entry = &cluster_phase_history[idx];
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
 * Phase advance (driver-internal API; HC2 SSOT, HC1 postmaster-only)
 * ============================================================ */

void
cluster_advance_phase(ClusterStartupPhase target)
{
	ClusterStartupPhase prev = cluster_startup_phase_current;
	int slot;

	/*
	 * HC1: postmaster-only.  This is the ONLY function that mutates
	 * cluster_startup_phase_current; calling it from a child backend
	 * would corrupt the postmaster's view of its own startup.
	 */
	Assert(!IsUnderPostmaster);

	/*
	 * Strict transition rules.  The only legitimate transitions are:
	 *   prev + 1 == target          (forward step)
	 *   target == CLUSTER_PHASE_SHUTDOWN  (any phase can enter shutdown)
	 * Everything else is a programming error -> ereport(FATAL).
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

	/* Commit the transition (HC2 SSOT mutate). */
	cluster_startup_phase_current = target;
	cluster_phase_start_times[(int)target] = GetCurrentTimestamp();

	/*
	 * Update the legacy cluster_phase const char * mirror (HC2: this
	 * is the ONLY writer in the codebase).  cluster_startup_phase_to_
	 * string returns a pointer to a static string literal so no
	 * lifetime concerns; child backends inherit this pointer at fork
	 * time.
	 */
	cluster_phase = cluster_startup_phase_to_string(target);

	/* Append to fixed-size history ring (HC5). */
	slot = cluster_phase_history_head;
	cluster_phase_history[slot].phase = target;
	cluster_phase_history[slot].entered_at = cluster_phase_start_times[(int)target];
	cluster_phase_history_head = (slot + 1) % CLUSTER_PHASE_HISTORY_RING_SIZE;
	cluster_phase_history_count++;

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

static PhaseRunResult
phase_1_handler(void)
{
	Assert(!IsUnderPostmaster);
	elog(DEBUG1, "Phase 1 stub: skipping interconnect listener / heartbeat / LMON "
				 "(spec-1.11+ replaces this handler)");
	return PHASE_RUN_OK;
}


static PhaseRunResult
phase_2_handler(void)
{
	Assert(!IsUnderPostmaster);
	elog(DEBUG1, "Phase 2 stub: skipping LMS / LMD / LCK "
				 "(spec-1.12 LCK + Stage 2 GES replace this handler)");
	return PHASE_RUN_OK;
}


static PhaseRunResult
phase_3_handler(void)
{
	Assert(!IsUnderPostmaster);
	elog(DEBUG1, "Phase 3 stub: PG-native startup process unchanged; "
				 "Recovery Coordinator / merged recovery deferred to Stage 4 spec");
	return PHASE_RUN_OK;
}


static PhaseRunResult
phase_4_handler(void)
{
	Assert(!IsUnderPostmaster);
	elog(DEBUG1, "Phase 4 stub: PG-native walwriter / bgwriter / checkpointer / "
				 "autovacuum spawn unchanged; DIAG (1.13) / Cluster Stats (1.14) "
				 "deferred to those specs");
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
typedef PhaseRunResult (*ClusterPhaseHandler)(void);

static const ClusterPhaseHandler phase_handlers[CLUSTER_PHASE_LAST + 1]
	= { [CLUSTER_PHASE_PRE_INIT] = NULL,
		[CLUSTER_PHASE_0_BASE] = NULL,
		[CLUSTER_PHASE_1_CLUSTER] = phase_1_handler,
		[CLUSTER_PHASE_2_LOCK] = phase_2_handler,
		[CLUSTER_PHASE_3_RECOVERY] = phase_3_handler,
		[CLUSTER_PHASE_4_NORMAL] = phase_4_handler,
		[CLUSTER_PHASE_RUNNING] = NULL,
		[CLUSTER_PHASE_SHUTDOWN] = NULL };


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
	 * Driver loop.  Walk Phase 0 -> 1 -> 2 -> 3 -> 4 -> RUNNING.  The
	 * driver advances; handlers only do work + return status (HC3).
	 *
	 * Phase 0 entry is the "post-shmem ready" point reached by the
	 * caller (PostmasterMain after CreateSharedMemoryAndSemaphores +
	 * cluster_init).  We advance into 0_BASE here as the explicit
	 * skeleton starting point.
	 */
	cluster_advance_phase(CLUSTER_PHASE_0_BASE);

	for (phase = CLUSTER_PHASE_1_CLUSTER; phase <= CLUSTER_PHASE_4_NORMAL; phase++) {
		PhaseRunResult result;
		ClusterPhaseHandler handler;

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
		result = handler();

		if (result == PHASE_RUN_FATAL) {
			/* Handler signalled unrecoverable failure (HC4). */
			ereport(FATAL, (errcode(ERRCODE_CLUSTER_PHASE_PRECONDITION_FAILED),
							errmsg("cluster startup phase %s failed",
								   cluster_startup_phase_to_string(phase)),
							errhint("See postmaster log for handler-specific "
									"diagnostics; spec-1.10 / spec-1.11+ document "
									"the phase handler contract.")));
		}

		/*
		 * PHASE_RUN_RETRY is currently unused -- 1.10 stub handlers
		 * always return PHASE_RUN_OK.  When 1.11+ real handlers retry
		 * (e.g. transient interconnect failure) they would loop within
		 * the same phase before returning PHASE_RUN_OK.
		 */
		Assert(result == PHASE_RUN_OK);

		/*
		 * Timeout enforcement (HC4) is integrated by the per-phase
		 * timeout GUC.  Stage 1.10 stub handlers return immediately so
		 * we don't naturally trigger the timeout; tests use the
		 * cluster-startup-phase-N-enter inject point with a sleep
		 * fault to simulate a stuck phase and verify timeout handling.
		 *
		 * Real timeout enforcement requires reading
		 * cluster.phase{1..4}_timeout GUC and tracking
		 * cluster_phase_start_times[phase] vs current time.  At the
		 * skeleton layer there is no event loop / sub-process to
		 * monitor; the inject point covers the regression case until
		 * 1.11+ real handlers introduce loops that need a real
		 * deadline.
		 */
	}

	cluster_advance_phase(CLUSTER_PHASE_RUNNING);
}


void
cluster_run_shutdown_sequence(void)
{
	Assert(!IsUnderPostmaster);

	CLUSTER_INJECTION_POINT("cluster-run-shutdown-top");

	/*
	 * Stage 1.10 stub: directly transition to SHUTDOWN.  Reverse-order
	 * graceful tear-down (RUNNING -> 4 -> 3 -> 2 -> 1 -> SHUTDOWN) is
	 * deferred to 1.11-1.14 / Stage 6 once the per-phase background
	 * processes that need graceful stop are spawned.  See spec-1.10
	 * §3.2 SHUTDOWN row.
	 */
	cluster_advance_phase(CLUSTER_PHASE_SHUTDOWN);
}

#endif /* USE_PGRAC_CLUSTER */
