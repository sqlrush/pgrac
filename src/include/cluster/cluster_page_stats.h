/*-------------------------------------------------------------------------
 *
 * cluster_page_stats.h
 *	  RF-PAGE PGDEL-08 — recovery-only observability: the §9.1 counter
 *	  set (EVENT/GAUGE/TIMESTAMP classified), the §9.3 attempt dump, and
 *	  the G2/G4′ producer-consumer contract.
 *
 *	  Spec: specs/spec-rf-page-crash-safe-page-replay-journal.md
 *	  §2.1 PGDEL-08 ("counters/waits/dump 与错误映射；G2/G4′
 *	  producer-consumer 必查"), §9.1/§9.3, §11.1 G2/G4′.
 *
 *	  SEMANTIC RULE (G2): every counter is exactly ONE of EVENT, GAUGE or
 *	  TIMESTAMP — never mixed.  `cluster_page_stats_describe` is the
 *	  single vocabulary table (name -> kind) that the dump/SQL consumers
 *	  read, so a producer can never sneak a mixed-semantic field past the
 *	  consumers.
 *
 *	  PRODUCER-CONSUMER RULE (G4′): every counter has exactly one
 *	  producer (the cluster_page_stats_* functions, fired by the PGDEL-09
 *	  wiring at the owning proof points) and one consumer (the
 *	  pg_stat_cluster_counters mirror registered in cluster_pgstat.c, and
 *	  the §9.3 dump).  No counter is written from two sites; the
 *	  triggering tests are the PGDEL-08 unit suite + the PGDEL-09
 *	  production-caller legs.
 *
 *	  §9.2 wait events are deliberately NOT added here (spec: "名称与
 *	  catalog ripple 在 product plan 定；本文不新增 dead enum").
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * IDENTIFICATION
 *	  src/include/cluster/cluster_page_stats.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef CLUSTER_PAGE_STATS_H
#define CLUSTER_PAGE_STATS_H

#include "port/atomics.h"
#include "cluster/cluster_page_apply.h"

typedef enum ClusterPageStatKind {
	CLUSTER_PAGE_STAT_EVENT = 0, /* 一次发生（计数） */
	CLUSTER_PAGE_STAT_GAUGE,	 /* 当前值（可设可读） */
	CLUSTER_PAGE_STAT_TIMESTAMP	 /* 最近一次发生时刻（uint64 us） */
} ClusterPageStatKind;

/*
 * §9.1 recovery-only counter set.  Each field is written by exactly one
 * producer function below (G4′).  Timestamps use GetCurrentTimestamp()
 * microsecond values; latency measurement is the consumer's subtraction.
 */
typedef struct ClusterPageRecoveryStats {
	/* ---- EVENTS ---- */
	pg_atomic_uint64 source_selected_current;  /* §9.1-1 CURRENT */
	pg_atomic_uint64 source_selected_pi;	   /* §9.1-1 PI */
	pg_atomic_uint64 source_selected_storage;  /* §9.1-1 STORAGE */
	pg_atomic_uint64 source_invalid;		   /* §9.1-2 invalid by reason */
	pg_atomic_uint64 source_missing;		   /* §9.1-2 missing */
	pg_atomic_uint64 source_conflict;		   /* §9.1-2 conflict */
	pg_atomic_uint64 result_skip;			   /* §9.1-3 */
	pg_atomic_uint64 apply_count;			   /* §9.1-4 expected-before apply */
	pg_atomic_uint64 version_mismatch;		   /* §9.1-4 mismatch */
	pg_atomic_uint64 unknown_class_blocked;	   /* §9.1-5 */
	pg_atomic_uint64 authority_stale_rejected; /* §9.1-8 */
	pg_atomic_uint64 resource_early_release;   /* §9.1-9 (bug signal) */
	pg_atomic_uint64 retire_denied;			   /* §9.1-10 PL-12 */
	pg_atomic_uint64 d3_rebuild;			   /* §9.1-11 */
	pg_atomic_uint64 d3_optimization_hit;	   /* §9.1-11 */
	pg_atomic_uint64 stable_base_unresolved;   /* §9.1-12 */
	/* ---- GAUGES ---- */
	pg_atomic_uint64 contributor_records;	/* §9.1-6 */
	pg_atomic_uint64 contributor_threads;	/* §9.1-6 */
	pg_atomic_uint64 contributor_gaps;		/* §9.1-6 */
	pg_atomic_uint64 retained_pinned_bytes; /* §9.1-10 */
	/* ---- TIMESTAMPS (last occurrence, us) ---- */
	pg_atomic_uint64 last_page_write_ts;		 /* §9.1-7 */
	pg_atomic_uint64 last_durability_barrier_ts; /* §9.1-7 */
	pg_atomic_uint64 last_post_read_ts;			 /* §9.1-7 */
} ClusterPageRecoveryStats;

extern void cluster_page_stats_init(ClusterPageRecoveryStats *stats);

/* EVENT producers (one site each — the PGDEL-09 wiring). */
extern void cluster_page_stats_source_selected(ClusterPageRecoveryStats *stats,
											   ClusterPageSourceKind kind);
extern void cluster_page_stats_source_invalid(ClusterPageRecoveryStats *stats);
extern void cluster_page_stats_source_missing(ClusterPageRecoveryStats *stats);
extern void cluster_page_stats_source_conflict(ClusterPageRecoveryStats *stats);
extern void cluster_page_stats_result_skip(ClusterPageRecoveryStats *stats);
extern void cluster_page_stats_apply(ClusterPageRecoveryStats *stats);
extern void cluster_page_stats_version_mismatch(ClusterPageRecoveryStats *stats);
extern void cluster_page_stats_unknown_class_blocked(ClusterPageRecoveryStats *stats);
extern void cluster_page_stats_authority_stale(ClusterPageRecoveryStats *stats);
extern void cluster_page_stats_early_release(ClusterPageRecoveryStats *stats);
extern void cluster_page_stats_retire_denied(ClusterPageRecoveryStats *stats);
extern void cluster_page_stats_d3_rebuild(ClusterPageRecoveryStats *stats, bool optimization_hit);
extern void cluster_page_stats_stable_base_unresolved(ClusterPageRecoveryStats *stats);

/* GAUGE producers. */
extern void cluster_page_stats_contributors(ClusterPageRecoveryStats *stats, uint64 records,
											uint64 threads, uint64 gaps);
extern void cluster_page_stats_pinned_bytes(ClusterPageRecoveryStats *stats, uint64 bytes);

/* TIMESTAMP producers. */
extern void cluster_page_stats_note_page_write(ClusterPageRecoveryStats *stats);
extern void cluster_page_stats_note_durability(ClusterPageRecoveryStats *stats);
extern void cluster_page_stats_note_post_read(ClusterPageRecoveryStats *stats);

/*
 * G2 vocabulary: every counter name maps to exactly ONE kind.  Returns
 * false for an unknown name (the consumers must fail closed rather than
 * guess a semantics).  Names match the cluster_pgstat registry entries
 * ("cluster.page.*").
 */
extern bool cluster_page_stats_describe(const char *name, ClusterPageStatKind *kind);

/*
 * §9.3 attempt dump — the full field set of ONE block attempt.  The API
 * takes no page bytes: page content is never dumped (spec §9.3 last
 * sentence).  Writes into `buf` (NUL-terminated, truncated at buflen);
 * returns the number of chars that WOULD have been written (snprintf
 * semantics) so the caller can size the buffer.
 */
typedef struct ClusterPageAttemptDump {
	uint16 failed_origin_thread;
	uint64 failure_generation;
	ClusterPageIdentity identity;
	ClusterPageClass page_class;
	ClusterPageSourceKind source_kind;
	ClusterPageVersion source_version;
	ClusterPageVersion terminal_version;
	uint64 contributor_count;
	uint64 contributor_thread_set;
	bool applied; /* apply 或 skip 结论 */
	bool version_mismatch;
	bool durability_ok;
	bool post_read_ok;
	bool authority_revalidated;
	bool released;
	const char *stop_reason; /* NULL = no STOP */
} ClusterPageAttemptDump;

extern int cluster_page_attempt_dump(const ClusterPageAttemptDump *dump, char *buf, Size buflen);

#endif /* CLUSTER_PAGE_STATS_H */
