/*-------------------------------------------------------------------------
 *
 * cluster_hang.h
 *	  pgrac Hang Manager skeleton (spec-5.11) — per-instance long-wait
 *	  sampling + diagnostic dump + backend-local ProcSignal dump hook.
 *
 *	  Diagnostic-only: this module samples long-waiting backends, records a
 *	  bounded shared snapshot, and exposes it through the `hang` category of
 *	  pg_cluster_state plus a ProcSignal-driven backend self-dump.  It NEVER
 *	  cancels, kills, selects victims, or auto-resolves anything — all
 *	  remediation belongs to spec-5.12.
 *
 *	  fail-OPEN discipline: when a sample cannot be fully resolved (torn
 *	  read, blocker gone, remote boundary, no true wait-start) the sampler
 *	  tags the sample with a ClusterHangSampleQuality and keeps going; it
 *	  never raises an error to reject itself.  The quality tag is the
 *	  forward-safety contract that lets spec-5.12 act only on samples that
 *	  are provably COMPLETE (cluster_hang_sample_actionable()).
 *
 *	  The decision/policy helpers (threshold, exclusion, wait-source tag,
 *	  duration kind, quality, top-N store) live in cluster_hang_policy.c and
 *	  carry no PostgreSQL runtime dependency, so they are exercised directly
 *	  by cluster_unit.  The runtime gathering (pgstat + lock snapshots),
 *	  DIAG main-loop tick, and ProcSignal glue live in cluster_hang.c.
 *
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/include/cluster/cluster_hang.h
 *
 * NOTES
 *	  This is a pgrac-original file.  Compiled only in --enable-cluster
 *	  builds.
 *	  Spec: spec-5.11-hang-manager-skeleton.md
 *	  Leaf header: depends only on base postgres types + lwlock/timestamp,
 *	  so it can be embedded into ClusterDiagSharedState (cluster_diag.h).
 *
 *-------------------------------------------------------------------------
 */
#ifndef CLUSTER_HANG_H
#define CLUSTER_HANG_H

#include <signal.h> /* sig_atomic_t (cluster_hang_dump_pending) */

#include "datatype/timestamp.h"
#include "storage/lwlock.h"

/*
 * Fixed upper bound on the number of long-wait samples kept per round.  The
 * cluster.hang_max_sampled GUC must never exceed this compile-time constant;
 * the shared store is a fixed-size array so it can be embedded directly into
 * the DIAG shmem region (spec-5.11 Q13-A, no new region / tranche).
 */
#define CLUSTER_HANG_MAX_SAMPLES 64

/* Bounded copy length for the wait_event name carried in a sample slot. */
#define CLUSTER_HANG_WAIT_EVENT_LEN 64

/*
 * ClusterHangWaitSource — coarse wait-source tag (feature-054 subset).  This
 * is a lightweight classification of the wait_event class, NOT the full
 * feature-054 classify_hang() decision tree (which is forward to #54).
 */
typedef enum ClusterHangWaitSource {
	HANG_WAIT_UNKNOWN = 0,
	HANG_WAIT_LOCK, /* heavyweight lock / GES enqueue */
	HANG_WAIT_CACHE_FUSION,
	HANG_WAIT_IO,
	HANG_WAIT_UNDO,
	HANG_WAIT_LWLOCK,
	HANG_WAIT_IDLE_IN_TX_BLOCKER
} ClusterHangWaitSource;

/*
 * ClusterHangDurationKind — does duration_us reflect a true wait-start?
 *
 *	HANG_DUR_TRUE  : local heavyweight LOCK (LockInstanceData.waitStart) or a
 *	                 D0-confirmed GES wait_start snapshot — see §0.2 matrix.
 *	HANG_DUR_APPROX: GES (default, no locally-readable wait_start), cross-node
 *	                 TX, or PG-native generic waits (LWLock/IO/...) which carry
 *	                 no wait-start; duration is a state-duration approximation.
 */
typedef enum ClusterHangDurationKind {
	HANG_DUR_TRUE = 0,
	HANG_DUR_APPROX = 1
} ClusterHangDurationKind;

/*
 * ClusterHangSampleQuality — completeness label (forward-safety SSOT).  spec-
 * 5.12 may only consider a sample for remediation when quality == COMPLETE
 * AND in_confirmed_deadlock == false (cluster_hang_sample_actionable()).
 */
typedef enum ClusterHangSampleQuality {
	HANG_SAMPLE_COMPLETE = 0,	/* true wait duration + fully resolved */
	HANG_SAMPLE_APPROXIMATE,	/* no true wait-start; 5.12 not actionable */
	HANG_SAMPLE_INCOMPLETE,		/* torn read / soft blocker only / partial */
	HANG_SAMPLE_BLOCKER_GONE,	/* blocker backend disappeared */
	HANG_SAMPLE_REMOTE_BOUNDARY /* blocker on a remote node; DFS = 5.12 */
} ClusterHangSampleQuality;

/*
 * ClusterHangExcludeReason — why a waiting backend is NOT counted as a hang
 * candidate (feature-054 exclusion subset).
 */
typedef enum ClusterHangExcludeReason {
	HANG_EXCLUDE_NONE = 0, /* genuine long-wait hang candidate */
	HANG_EXCLUDE_IDLE,	   /* idle / idle-in-tx client wait / not waiting */
	HANG_EXCLUDE_BGWORKER, /* background worker */
	HANG_EXCLUDE_DEADLOCK  /* already handled by the deadlock detector */
} ClusterHangExcludeReason;

/*
 * ClusterHangSampleSlot — one long-wait sample.  POD (no pointers) so it can
 * be memcpy'd whole into / out of the shared store under the DIAG LWLock.
 */
typedef struct ClusterHangSampleSlot {
	int pid;
	int backendId;
	int node_id; /* this instance; reserved for cross-node chain */
	char wait_event[CLUSTER_HANG_WAIT_EVENT_LEN];
	uint8 duration_kind;	/* ClusterHangDurationKind */
	TimestampTz wait_since; /* TRUE: real wait-start; APPROX: state start */
	int64 duration_us;
	uint8 source;				/* ClusterHangWaitSource */
	uint8 quality;				/* ClusterHangSampleQuality */
	int blocker_pid;			/* local hard blocker pid; -1 = none */
	int blocker_backendId;		/* same-snapshot backendId; -1 = none */
	int blocker_remote_node;	/* >= 0 = cross-node hop boundary */
	bool in_confirmed_deadlock; /* 5.8 reader fills (see D6 / D0) */
	bool being_resolved;		/* v1 const false (5.9 forward) */
	bool fairness_boosted;		/* v1 const false (5.10 forward) */
} ClusterHangSampleSlot;

/*
 * ClusterHangSampleStore — bounded shared snapshot of the latest round.
 * Embedded into ClusterDiagSharedState (Q13-A); guarded by the DIAG LWLock.
 * Single writer (DIAG); many readers (backends running dump_hang).
 */
typedef struct ClusterHangSampleStore {
	uint64 sample_epoch; /* +1 per completed round */
	int n_samples;		 /* valid slots this round (<= MAX_SAMPLES) */
	bool truncated;		 /* long-waits exceeded the cap; top-N kept */
	ClusterHangSampleSlot slots[CLUSTER_HANG_MAX_SAMPLES];
} ClusterHangSampleStore;

/*
 * ClusterHangCounters — cumulative observability counters (D8).  Embedded in
 * the DIAG shmem region so a backend running dump_hang can read what the DIAG
 * sampler wrote.  All updates take the DIAG LWLock.
 */
typedef struct ClusterHangCounters {
	uint64 samples_taken;	   /* completed sampling rounds */
	uint64 long_waits_seen;	   /* cumulative long-waits recorded */
	uint64 dumps_emitted;	   /* long-wait LOG-once events emitted */
	uint64 incomplete_samples; /* samples tagged not COMPLETE */
	uint64 excluded_deadlock;  /* candidates excluded: confirmed deadlock */
	uint64 excluded_idle;	   /* candidates excluded: idle / client wait */
	uint64 excluded_bgworker;  /* candidates excluded: background worker */
	uint64 proc_signal_dumps;  /* backend self-dumps via ProcSignal */
	uint64 error_count;		   /* sampler PG_TRY backstops (fail-open) */
} ClusterHangCounters;

/*
 * ClusterHangNode — sampler-internal working node (feature-054 HangChainNode
 * skeleton).  Distilled into a ClusterHangSampleSlot before publishing.
 */
typedef struct ClusterHangNode {
	int pid;
	int backendId;
	TransactionId xid;
	ClusterHangWaitSource source;
	char wait_event[CLUSTER_HANG_WAIT_EVENT_LEN];
	ClusterHangDurationKind duration_kind;
	TimestampTz wait_since;
	int64 duration_us;
	int blocker_pid;
	int blocker_backendId;
	int blocker_remote_node;
	bool is_root;
	ClusterHangSampleQuality quality;
	bool in_confirmed_deadlock;
	bool being_resolved;
	bool fairness_boosted;
} ClusterHangNode;


/* ============================================================
 * Policy helpers (cluster_hang_policy.c) — pure, no PG runtime.
 * ============================================================ */

/* True when a measured wait duration is at/over the configured threshold. */
extern bool cluster_hang_wait_exceeds_threshold(int64 duration_us, int threshold_ms);

/*
 * Map a PGPROC wait_event_info (class<<24 | event) to a coarse wait source.
 * wait_event_name (may be NULL) refines cluster waits that share a generic
 * class (Cache Fusion / undo); the full classifier is forward to feature-054.
 */
extern ClusterHangWaitSource cluster_hang_classify_wait_source(uint32 wait_event_info,
															   const char *wait_event_name);

/* True when the wait class is an idle wait (client / activity / not waiting). */
extern bool cluster_hang_wait_is_idle_class(uint32 wait_event_info);

/* TRUE iff a real wait-start timestamp is available, else APPROX. */
extern ClusterHangDurationKind cluster_hang_duration_kind(bool has_true_wait_start);

/* Resolve the completeness/quality label from blocker + duration facts. */
extern ClusterHangSampleQuality cluster_hang_quality(ClusterHangDurationKind dk, int blocker_pid,
													 int blocker_remote_node, bool blocker_gone,
													 bool soft_blocker_only);

/* Decide whether a waiting backend is excluded from hang accounting. */
extern ClusterHangExcludeReason cluster_hang_exclude_reason(int backend_state, int backend_type,
															bool in_confirmed_deadlock,
															uint32 wait_event_info);

/* forward-safety gate: 5.12 may act only on COMPLETE && !confirmed-deadlock. */
extern bool cluster_hang_sample_actionable(const ClusterHangSampleSlot *slot);

/* Build-side store ops (pure). */
extern void cluster_hang_store_reset(ClusterHangSampleStore *store);
extern void cluster_hang_store_consider(ClusterHangSampleStore *store,
										const ClusterHangSampleSlot *cand, int max_samples);

/* Consistent-snapshot store ops (use the DIAG LWLock passed in). */
extern void cluster_hang_store_publish(ClusterHangSampleStore *shared, LWLock *lock,
									   const ClusterHangSampleStore *round);
extern int cluster_hang_store_snapshot(const ClusterHangSampleStore *shared, LWLock *lock,
									   ClusterHangSampleStore *out);

/* Distill a working node into a publishable slot. */
extern void cluster_hang_node_to_slot(const ClusterHangNode *node, int node_id,
									  ClusterHangSampleSlot *slot);


/*
 * ClusterHangDumpData — one consistent snapshot of everything dump_hang
 * needs (aggregates + counters + per-row store), copied under the DIAG
 * LWLock in a single section so the view is internally consistent.
 */
typedef struct ClusterHangDumpData {
	bool available; /* false when the DIAG region is not attached */
	TimestampTz last_sample_at;
	TimestampTz last_dump_emitted_at;
	int64 long_wait_count;
	int64 longest_wait_us;
	ClusterHangCounters counters;
	ClusterHangSampleStore store;
} ClusterHangDumpData;


/* ============================================================
 * Runtime entry points (cluster_hang.c).
 * ============================================================ */

/* ProcSignal pending flag for a backend self-dump (D5; async-safe set). */
extern PGDLLIMPORT volatile sig_atomic_t cluster_hang_dump_pending;

/* Copy a consistent dump snapshot (aggregates + counters + rows). */
extern void cluster_hang_get_dump_data(ClusterHangDumpData *out);

/* String name for a wait-source / quality enum (for the dump). */
extern const char *cluster_hang_wait_source_str(uint8 source);
extern const char *cluster_hang_quality_str(uint8 quality);

/*
 * GUC-gate helper.  The cluster.hang_* GUC variables themselves live in
 * cluster_guc.c (declared in cluster_guc.h); DiagMain reads
 * cluster_hang_manager_enabled directly.  cluster_hang_sample_due() answers
 * whether enough time has elapsed since the last completed round.
 */
extern bool cluster_hang_sample_due(TimestampTz now);

/*
 * Run one long-wait sampling round.  fail-OPEN: never throws (internal
 * PG_TRY backstop).  Called from DiagMain when a sample is due.
 */
extern void cluster_hang_sample_once(void);

/* Backend self-dump of its own wait identity (ProcSignal CFI path). */
extern void cluster_hang_dump_self_to_log(void);

/* ProcSignal handler (async-signal-safe: flag + latch only). */
extern void cluster_handle_hang_dump_interrupt(void);

/* CFI-time processing of a pending self-dump request. */
extern void cluster_hang_check_pending_interrupt(void);

#endif /* CLUSTER_HANG_H */
