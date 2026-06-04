/*-------------------------------------------------------------------------
 *
 * cluster_undo_cleaner.h
 *	  pgrac Undo Cleaner cluster background process — Stage 3.13.
 *
 *	  Eighth-family cluster aux process (LMON 1.11 / LCK 1.12 / DIAG
 *	  1.13 / Cluster Stats 1.14 / CSSD 2.5 / QVOTEC 2.6 / LMS 2.18 /
 *	  LMD 2.19 / SinvalBcast 2.3x preceded).  Consumes the
 *	  B_UNDO_CLEANER BackendType reserved at Stage 1 per the
 *	  background-process roster.
 *
 *	  The Undo Cleaner is the proactive half of undo/TT-slot retention:
 *	  spec-3.12 shipped the lazy half (horizon tracking + alloc Pass-2
 *	  recycle gate + retention-pressure rollover); this process turns
 *	  "judged recyclable" into "actually recycled" -- shmem TT slot GC,
 *	  durable header scan, SEGMENT_COMMITTED -> SEGMENT_RECYCLABLE
 *	  advancement, and (allocator-side) segment file reuse supply.
 *
 *	  Lifecycle model -- ServerLoop-managed, best-effort:
 *	    Unlike LMON/DIAG/Stats/CSSD (phase-4 gated, sync wait-ready,
 *	    dedicated spawn-fail SQLSTATEs), the Undo Cleaner is NOT part
 *	    of cluster startup phases 0-4.  postmaster's ServerLoop spawns
 *	    it once pmState == PM_RUN and respawns it after normal exit;
 *	    its absence degrades to spec-3.12 lazy-only recycling and is
 *	    never a startup failure.  Hence: no PhaseRunFailContext path,
 *	    no new SQLSTATE (TODO-003 "no").
 *
 *	  HC1: spawn is postmaster-only (ServerLoop); UndoCleanerMain
 *	      Asserts IsUnderPostmaster (reverse defense in depth).
 *	  HC2: UndoCleanerStatus enum is the single source of truth; all
 *	      status writes happen in the cleaner process under the region
 *	      LWLock; postmaster only writes shutdown_requested.
 *	  HC3: pass scope at 3.13 = own-instance only (mirrors spec-3.12
 *	      retention scope).  Cross-instance cleaning is Stage 4+/5.
 *	  HC4: cluster_enabled=false -> ServerLoop never spawns the
 *	      cleaner; cluster.undo_cleaner_enabled=off -> process stays
 *	      resident but each pass no-ops (diagnostic parity with the
 *	      3.12 lazy-only mode).
 *	  HC5: normal shutdown via proc_exit(0) -> reaper normal-exit path
 *	      -> no crash recovery; abnormal exit -> HandleChildCrash.
 *	  HC6: stale CTS_ACTIVE / TT_SLOT_ACTIVE / SEGMENT_ACTIVE entries
 *	      are never judged dead by the cleaner (rule 8.A); resolution
 *	      of crash residue belongs to Stage 4 recovery (spec-4.8).
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/include/cluster/cluster_undo_cleaner.h
 *
 * NOTES
 *	  This is a pgrac-original file.  Compiled only in --enable-cluster
 *	  builds.
 *	  Spec: spec-3.13-undo-cleaner-tt-gc.md (FROZEN v0.3, 2026-06-04).
 *
 *-------------------------------------------------------------------------
 */
#ifndef CLUSTER_UNDO_CLEANER_H
#define CLUSTER_UNDO_CLEANER_H

#include "datatype/timestamp.h"
#include "storage/latch.h"
#include "storage/lwlock.h"


/*
 * UndoCleanerStatus -- HC2 SSOT for Undo Cleaner lifecycle state.
 *
 *	Numeric values are observable via SQL (pg_cluster_state); preserve
 *	the 0..4 mapping when amending.
 */
typedef enum UndoCleanerStatus {
	UNDO_CLEANER_NOT_STARTED = 0,	/* ServerLoop has not yet spawned the cleaner */
	UNDO_CLEANER_SPAWNING = 1,		/* child forked; main not yet in its loop */
	UNDO_CLEANER_READY = 2,			/* main loop active */
	UNDO_CLEANER_SHUTTING_DOWN = 3, /* shutdown_requested seen; exiting */
	UNDO_CLEANER_EXITED = 4			/* proc_exit complete; reaper to harvest */
} UndoCleanerStatus;

#define UNDO_CLEANER_STATUS_LAST UNDO_CLEANER_EXITED


/*
 * UndoCleanerSharedState -- cleaner state visible across postmaster /
 * cleaner / SQL backends.
 *
 *	Single writer for status / timestamps / iters / pass counters: the
 *	cleaner process itself (LW_EXCLUSIVE).  Postmaster writes
 *	shutdown_requested.  Any backend reads (LW_SHARED) for SQL views,
 *	and may read `latch` to deliver a pressure wakeup (spec-3.13 Q8:
 *	retention-pressure rollover / hard-cap paths SetLatch the cleaner
 *	so RECYCLABLE supply is produced when it is needed most).
 */
typedef struct UndoCleanerSharedState {
	LWLock lwlock;					   /* LWTRANCHE_CLUSTER_UNDO_CLEANER */
	UndoCleanerStatus status;		   /* HC2 SSOT */
	pid_t pid;						   /* set in SPAWNING */
	TimestampTz spawned_at;			   /* set in SPAWNING */
	TimestampTz ready_at;			   /* set in READY */
	TimestampTz last_liveness_tick_at; /* local liveness tick (not heartbeat) */
	int64 main_loop_iters;			   /* monotone; observable liveness proof */
	bool shutdown_requested;		   /* postmaster sets; main loop polls */

	/*
	 * Pressure-wakeup channel (Q8).  Points at the cleaner's own
	 * PGPROC procLatch (shmem-resident, so cross-process SetLatch is
	 * safe).  Set in SPAWNING, cleared on SHUTTING_DOWN; readers must
	 * copy the pointer under LW_SHARED and tolerate NULL.
	 */
	Latch *latch;

	/*
	 * D6 pass counters (spec-3.13 step 8 fills; zero-init).  Updated by
	 * the cleaner only, read lock-free via accessors (single-writer
	 * monotone counters; mirrors spec-3.12 counter discipline).
	 */
	uint64 pass_count;					  /* completed cleaner passes */
	uint64 shmem_tt_slots_gcd;			  /* D2-A: shmem slots recycled proactively */
	uint64 header_tt_slots_below_horizon; /* D2-B scan-only: counted, not mutated */
	uint64 segments_marked_recyclable;	  /* D3: COMMITTED -> RECYCLABLE transitions */
	uint64 stale_active_skipped;		  /* HC6: ACTIVE entries skipped, never judged */
	uint64 slots_wrap_retired;			  /* D5: shmem entries retired at TT_WRAP_MAX */
} UndoCleanerSharedState;


/*
 * Public API.
 */

/* Read-only accessors for SQL view + diagnostics (LW_SHARED). */
extern UndoCleanerStatus cluster_undo_cleaner_status(void);
extern pid_t cluster_undo_cleaner_pid(void);
extern TimestampTz cluster_undo_cleaner_spawned_at(void);
extern TimestampTz cluster_undo_cleaner_ready_at(void);
extern TimestampTz cluster_undo_cleaner_last_liveness_tick_at(void);
extern int64 cluster_undo_cleaner_main_loop_iters(void);

/* Status enum -> canonical lowercase string ("ready", ...). */
extern const char *cluster_undo_cleaner_status_to_string(UndoCleanerStatus s);

/*
 * Pressure wakeup (Q8).  Called from allocator pressure paths
 * (retention rollover / hard-cap).  Safe from any backend; no-op when
 * the cleaner is not running.  Never throws.
 */
extern void cluster_undo_cleaner_wakeup(void);

/*
 * Postmaster shutdown request (belt-and-suspenders besides SIGTERM).
 */
extern void cluster_undo_cleaner_request_shutdown(void);

/*
 * shmem region helpers — registered by cluster_init_shmem_module()
 * via the spec-1.3 region registry (L206 five-step pattern).
 */
extern Size cluster_undo_cleaner_shmem_size(void);
extern void cluster_undo_cleaner_shmem_init(void);
extern void cluster_undo_cleaner_shmem_register(void);

/*
 * AuxiliaryProcessMain dispatch entry.  Asserts IsUnderPostmaster.
 * Never returns (proc_exit on shutdown).
 */
extern void UndoCleanerMain(void) pg_attribute_noreturn();


#endif /* CLUSTER_UNDO_CLEANER_H */
