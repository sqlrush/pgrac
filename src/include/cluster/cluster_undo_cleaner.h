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
#include "cluster/cluster_scn.h" /* SCN (pass-stats horizon plumbing) */


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

	/*
	 * spec-4.12a D4 (Finding C): round-robin segment-scan cursor.  The per-pass
	 * scan is bounded to cluster.undo_cleaner_batch_segments readable headers
	 * (R7); persisting where the last pass stopped lets successive passes resume
	 * across the inventory so every segment is eventually visited, instead of
	 * restarting at the first segment every pass (which let the low ids consume
	 * the whole batch budget forever and starved high-id ACTIVE segments -- the
	 * spec-4.13 leak).  Cleaner-process is the sole reader/writer; shmem-resident
	 * is sufficient (no WAL / on-disk / catversion -- a restart re-derives it
	 * from base).  0 (zero-init) is normalized to base on first use.
	 */
	uint32 scan_resume_seg;
} UndoCleanerSharedState;


/*
 * spec-4.12a D4 (Finding C): pure round-robin scan-cursor arithmetic.  Kept
 * pure so cluster_unit can exercise the wrap / normalize boundaries directly.
 * These govern coverage fairness only -- the ACTIVE->COMMITTED 8.A drain gate
 * (cluster_undo_record_segment_drainable) and RECYCLABLE-only reuse are
 * unchanged.  The scan window is the segment-id range [base, max_seg] for this
 * instance.
 */

/* Number of segments in the window [base, max_seg] (0 when the window is empty). */
static inline uint32
cluster_undo_cleaner_scan_inventory(uint32 base, uint32 max_seg)
{
	return (max_seg >= base) ? (max_seg - base + 1) : 0;
}

/*
 * Normalize a persisted resume cursor into [base, max_seg].  An out-of-range
 * cursor restarts at base: 0 on first use, or a value left of base / right of a
 * shrunken max_seg.
 */
static inline uint32
cluster_undo_cleaner_scan_cursor_start(uint32 resume, uint32 base, uint32 max_seg)
{
	return (resume < base || resume > max_seg) ? base : resume;
}

/* Advance the cursor one segment, wrapping from max_seg back to base. */
static inline uint32
cluster_undo_cleaner_scan_cursor_next(uint32 cur, uint32 base, uint32 max_seg)
{
	return (cur >= max_seg) ? base : cur + 1;
}


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

/* D6 counter accessors (dump_undo + tests). */
extern uint64 cluster_undo_cleaner_pass_count(void);
extern uint64 cluster_undo_cleaner_shmem_tt_slots_gcd(void);
extern uint64 cluster_undo_cleaner_segments_marked_recyclable(void);
extern uint64 cluster_undo_cleaner_stale_active_skipped(void);

/* D6 segment-scan wait wrappers (stubbed by cluster_unit binaries). */
extern void cluster_undo_cleaner_scan_wait_start(void);
extern void cluster_undo_cleaner_scan_wait_end(void);

/*
 * ClusterUndoCleanerPassStats -- per-pass tallies (D2/D3/D5 producers,
 * D6 publishes a chosen subset into UndoCleanerSharedState).  Plain
 * stack struct; zero it before each pass.
 */
typedef struct ClusterUndoCleanerPassStats {
	uint32 segments_scanned;
	uint32 shmem_tt_slots_gcd;			  /* D2-A: shmem slots recycled to FREE */
	uint32 header_tt_slots_below_horizon; /* D2-B scan-only: would-be-recyclable */
	uint32 header_unresolved_committed;	  /* D2-B: COMMITTED with invalid scn (8.A retain) */
	uint32 segments_marked_recyclable;	  /* D3 */
	uint32 segments_reused;				  /* D4 (allocator-side, reported back) */
	uint32 header_retained_committed;	  /* D2-B: COMMITTED at/above horizon (pinned signal) */
	uint32 stale_active_skipped;		  /* HC6: durable-side ACTIVE residue skipped */
	uint32 slots_wrap_retired;			  /* D5 */
} ClusterUndoCleanerPassStats;

/*
 * D2-A: proactive GC of the CURRENT shmem allocator segment for this
 * node.  Caller supplies the horizon (computed ONCE per pass, before
 * any seg->lock — spec-3.12 C17).  Shares the recycle transition
 * helper with alloc Pass-2 (C-R1: single typed implementation).
 */
extern void cluster_tt_slot_gc_current_pass(SCN horizon, ClusterUndoCleanerPassStats *stats);

/*
 * D2-B: READ-ONLY scan of one segment's durable header TTSlot[]
 * (block 0).  Counts sub-horizon committed / unresolved / stale-active
 * slots; never mutates durable bytes (v0.3 ③).  Returns false when the
 * header cannot be read (absent file / I/O) — caller counts and moves on.
 */
extern bool cluster_undo_segment_tt_header_scan_pass(uint32 segment_id, uint8 owner_instance,
													 SCN horizon,
													 ClusterUndoCleanerPassStats *stats);

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
