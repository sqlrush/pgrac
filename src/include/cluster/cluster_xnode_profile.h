/*-------------------------------------------------------------------------
 *
 * cluster_xnode_profile.h
 *	  Cross-node performance profiling buckets (spec-5.59 D1).
 *
 *	  Four-axis (write / read / index / commit-SCN) per-bucket INSTR_TIME
 *	  accumulators living in a dedicated shmem region, plus the read-side
 *	  amortization probe (reship vs S-holder-hit).  Everything is gated by
 *	  the cluster.xnode_profile GUC (default off): with the GUC off the
 *	  begin/end helpers take a single predictable branch and perform no
 *	  clock syscall, keeping the instrumented hot paths byte-equivalent in
 *	  behaviour to the un-instrumented baseline.
 *
 *	  Attribution contract: pp folding (bucket_nanos / txn_wall x tax) is
 *	  only valid for requester-exclusive-wait buckets; LMON / remote-holder
 *	  / IC service-time buckets accumulate concurrently across processes
 *	  and are reported as raw nanos/event only (two-table separation).
 *
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/include/cluster/cluster_xnode_profile.h
 *
 * NOTES
 *	  This is a pgrac-original file (no derivation from PostgreSQL).
 *	  Spec: spec-5.59-two-node-crossnode-perf-profile.md (D1, §2.2 / §2.3)
 *
 *-------------------------------------------------------------------------
 */
#ifndef CLUSTER_XNODE_PROFILE_H
#define CLUSTER_XNODE_PROFILE_H

#ifndef FRONTEND

#include "cluster/cluster_guc.h"
#include "port/atomics.h"
#include "portability/instr_time.h"

/*
 * Bucket identifiers, four axes (~23 buckets).  Keep in sync with
 * cluster_xp_bucket_names[] in cluster_xnode_profile.c (unit test U1
 * asserts every bucket has a name).
 */
typedef enum ClusterXnodeBucket {
	/* --- W write axis (requester exclusive-wait unless noted) --- */
	CLXP_W_GCS_X_REQUEST = 0, /* block X request -> reply wait */
	CLXP_W_GCS_X_RECEIVE,	  /* X reply payload receive/verify */
	CLXP_W_GCS_X_INSTALL,	  /* granted X image install into buffer */
	CLXP_W_GCS_X_INVALIDATE,  /* invalidate broadcast + ack collection */
	CLXP_W_GES_ENQUEUE,		  /* GES REQUEST send -> grant/reject */
	CLXP_W_GES_CONVERT,		  /* GES CONVERT send -> grant/reject */
	CLXP_W_GES_WAIT,		  /* GES reply-wait CV block interval */
	CLXP_W_GES_WAKE,		  /* master-side waiter wake (service-time) */
	CLXP_W_HW_EXTEND,		  /* HW relation-extend acquire -> grant */
	/* --- R read axis, layer 1: block transfer --- */
	CLXP_R_GCS_S_REQUEST,	  /* block S/read request -> reply wait */
	CLXP_R_GCS_S_RECEIVE,	  /* S/read reply receive/verify */
	CLXP_R_READIMAGE_SHIP,	  /* holder-side read-image ship (service-time) */
	CLXP_R_SHOLDER_CACHE_HIT, /* reserved: S-holder cache hit (lever probe) */
	/* --- R read axis, layer 2: CR construction (local undo walk) --- */
	CLXP_R_CR_CONSTRUCT,  /* CR image construction total */
	CLXP_R_CR_CHAIN_WALK, /* undo chain walk inside construction */
	/* --- R read axis, layer 3: per-tuple visibility resolution --- */
	CLXP_R_TT_VISIBILITY_RESOLVE, /* per-tuple cross-node xid -> TT SCN */
	/* --- I index axis --- */
	CLXP_I_INDEX_BLOCK_XFER,	/* index-relation block cross-node X/S */
	CLXP_I_RIGHTMOST_LEAF_PING, /* optional nbtree P_RIGHTMOST count-only */
	/* --- C commit/SCN axis --- */
	CLXP_C_SCN_COMMIT_ADVANCE, /* per-commit SCN advance */
	CLXP_C_SCN_BOC_BROADCAST,  /* BOC broadcast fanout (send side) */
	/* --- shared: IC one-way service-time (never folded into pp) --- */
	CLXP_IC_SEND_SERVICE,	  /* backend-local: build+enqueue+send syscall */
	CLXP_IC_INBOUND_DISPATCH, /* recv side: verify + dispatch */
	/* --- local write-side machinery (undo + ITL + WAL, per DML) --- */
	CLXP_LOCAL_UNDO_ITL_WAL,
	/*
	 * Commit-path decomposition (spec-7.4 D0 census; D4 makes them a
	 * permanent surface).  All are requester-exclusive-wait: the
	 * committing backend alone spends the interval, so per-commit
	 * nanos/event means are directly comparable against the client-side
	 * end-to-end commit latency denominator.
	 *
	 * APPENDED at the tail on purpose: bucket indexes address a shmem
	 * array, so the enum is append-only (an insertion renumbers every
	 * later bucket and any stale .o keeps writing the old index).
	 */
	CLXP_C_COMMIT_UNDO_FLUSH,  /* pre-commit undo segment fsync */
	CLXP_C_COMMIT_ITL_STAMP,   /* pre-commit ITL COMMITTED stamp + WAL */
	CLXP_C_COMMIT_TT_STAMP,	   /* pre-commit durable TT slot commit_scn stamp */
	CLXP_C_COMMIT_WAL_FLUSH,   /* commit-record XLogFlush (incl. group-commit queue) */
	CLXP_C_COMMIT_QUORUM_READ, /* PRE_COMMIT local quorum/lease read */
	CLXP_NBUCKETS
} ClusterXnodeBucket;

/*
 * Commit-latency histogram (spec-7.4 D4).  The commit-decomposition buckets
 * (undo-flush / itl-stamp / tt-stamp / wal-flush / scn-commit-advance) also
 * feed a microsecond-scale log histogram, so the p99 tail is visible and not
 * just the mean the {total_nanos, n_events} accumulator yields.  The edges
 * share the census staleness 1-2-5 log shape (spec-7.4 §2.4) but in
 * microseconds, since commit components run tens of microseconds while their
 * queueing tail reaches milliseconds.  CLXP_HIST_EDGES_US is the single
 * source of truth for both the inline classifier and the dump-label edge
 * array cluster_xp_hist_edge_us[] (defined in the .c); keep them derived from
 * this one macro.
 */
#define CLXP_HIST_EDGES_US 20, 50, 100, 200, 500, 1000, 2000, 5000, 10000, 20000, 50000
#define CLXP_HIST_NEDGES 11
#define CLXP_HIST_NBUCKETS (CLXP_HIST_NEDGES + 1)

/* Commit-decomposition components that own a latency histogram. */
typedef enum ClusterXpHistComponent {
	CLXP_HIST_UNDO_FLUSH = 0,
	CLXP_HIST_ITL_STAMP,
	CLXP_HIST_TT_STAMP,
	CLXP_HIST_WAL_FLUSH,
	CLXP_HIST_SCN_COMMIT_ADVANCE,
	CLXP_HIST_NCOMPONENTS
} ClusterXpHistComponent;

/*
 * Classify a nanosecond latency sample: the first bucket whose upper edge
 * (µs) exceeds nanos/1000.  Buckets are half-open [lo,hi) so a value exactly
 * on an edge lands in the next bucket; the top bucket (index CLXP_HIST_NEDGES)
 * is the ">= last-edge" overflow.
 */
static inline int
cluster_xp_hist_bucket_index(uint64 nanos)
{
	static const uint32 edges[CLXP_HIST_NEDGES] = {CLXP_HIST_EDGES_US};
	uint64		us = nanos / 1000;
	int			i;

	for (i = 0; i < CLXP_HIST_NEDGES; i++) {
		if (us < edges[i])
			return i;
	}
	return CLXP_HIST_NEDGES;
}

/*
 * Map a profiling bucket to its histogram component, or -1 if the bucket is
 * not histogrammed.  Only the commit-decomposition buckets feed a histogram.
 */
static inline int
cluster_xp_hist_slot_for(ClusterXnodeBucket b)
{
	switch (b) {
		case CLXP_C_COMMIT_UNDO_FLUSH:
			return CLXP_HIST_UNDO_FLUSH;
		case CLXP_C_COMMIT_ITL_STAMP:
			return CLXP_HIST_ITL_STAMP;
		case CLXP_C_COMMIT_TT_STAMP:
			return CLXP_HIST_TT_STAMP;
		case CLXP_C_COMMIT_WAL_FLUSH:
			return CLXP_HIST_WAL_FLUSH;
		case CLXP_C_SCN_COMMIT_ADVANCE:
			return CLXP_HIST_SCN_COMMIT_ADVANCE;
		default:
			return -1;
	}
}

/* One accumulator: total nanoseconds + event count, both monotonic. */
typedef struct ClusterXpAccum {
	pg_atomic_uint64 total_nanos;
	pg_atomic_uint64 n_events;
} ClusterXpAccum;

/*
 * The shmem region.  reset_generation increments on every
 * cluster_xp_reset() so external samplers can detect resets; the two probe
 * counters implement the read-side amortization probe (spec-5.59 §3.6).
 */
typedef struct ClusterXnodeProfileShared {
	ClusterXpAccum bucket[CLXP_NBUCKETS];
	pg_atomic_uint64 reset_generation;
	pg_atomic_uint64 read_reship_count;
	pg_atomic_uint64 read_sholder_hit_count;
	/* HW extend master-locality split (spec-5.59 D2 dimension) */
	pg_atomic_uint64 hw_extend_local_count;
	pg_atomic_uint64 hw_extend_remote_count;
	/* spec-7.4 D4: per-commit-component microsecond latency histogram. */
	pg_atomic_uint64 hist[CLXP_HIST_NCOMPONENTS][CLXP_HIST_NBUCKETS];
} ClusterXnodeProfileShared;

/* Number of non-bucket probe keys emitted by the dump (unit test U5). */
#define CLUSTER_XP_N_PROBE_KEYS 5

/* Scope handle for one timed interval (stack-allocated by callers). */
typedef struct ClusterXpScope {
	bool active;
	ClusterXnodeBucket bucket;
	instr_time start;
} ClusterXpScope;

/* Set once by shmem init; NULL until the region is attached. */
extern PGDLLIMPORT ClusterXnodeProfileShared *ClusterXnodeProfileCtl;

extern Size cluster_xnode_profile_shmem_size(void);
extern void cluster_xnode_profile_shmem_init(void);
extern void cluster_xnode_profile_shmem_register(void);

extern void cluster_xp_note_read(bool was_reship);
extern void cluster_xp_note_hw_extend(bool remote_master);
extern void cluster_xp_count(ClusterXnodeBucket b);
extern void cluster_xp_reset(void);

/*
 * Relation-kind hint for the index axis (D4): the buffer-access caller
 * (which has the Relation) tags the next buffer it reads; the GCS block
 * layer consumes the tag by exact BufferTag match.  Backend-local,
 * profiling-only, approximate attribution by design.
 */
struct buftag; /* storage/buf_internals.h */
extern void cluster_xp_relkind_hint_set(const struct buftag *tag, bool is_index);
extern bool cluster_xp_relkind_hint_is_index_for(const struct buftag *tag);

extern const char *cluster_xp_bucket_name(ClusterXnodeBucket b);

/* spec-7.4 D4 commit-latency histogram dump support. */
extern PGDLLIMPORT const uint32 cluster_xp_hist_edge_us[CLXP_HIST_NEDGES];
extern const char *cluster_xp_hist_component_name(ClusterXpHistComponent c);

/*
 * cluster_xp_begin / cluster_xp_end -- wrap a timed interval.
 *
 *	Off path: one branch on the GUC, no clock read, scope marked inactive.
 *	On path: INSTR_TIME_SET_CURRENT at begin; at end the delta is folded
 *	into the bucket with two atomic adds.
 */
static inline void
cluster_xp_begin(ClusterXpScope *s, ClusterXnodeBucket b)
{
	if (likely(!cluster_xnode_profile_enabled) || ClusterXnodeProfileCtl == NULL) {
		s->active = false;
		return;
	}
	s->active = true;
	s->bucket = b;
	INSTR_TIME_SET_CURRENT(s->start);
}

/* Discard a started scope without accumulating (conditional paths). */
static inline void
cluster_xp_abort(ClusterXpScope *s)
{
	s->active = false;
}

static inline void
cluster_xp_end(ClusterXpScope *s)
{
	instr_time	now;
	uint64		nanos;
	int			slot;

	if (!s->active)
		return;
	s->active = false;
	INSTR_TIME_SET_CURRENT(now);
	INSTR_TIME_SUBTRACT(now, s->start);
	nanos = (uint64) INSTR_TIME_GET_NANOSEC(now);
	pg_atomic_fetch_add_u64(&ClusterXnodeProfileCtl->bucket[s->bucket].total_nanos,
							nanos);
	pg_atomic_fetch_add_u64(&ClusterXnodeProfileCtl->bucket[s->bucket].n_events, 1);

	/*
	 * spec-7.4 D4: commit-decomposition buckets also fold the sample into a
	 * μs latency histogram (constant-time bucket classify + one atomic add).
	 * Non-commit buckets return slot < 0 and skip it, so the write/read/index
	 * axes keep their exact prior behaviour.
	 */
	slot = cluster_xp_hist_slot_for(s->bucket);
	if (slot >= 0)
		pg_atomic_fetch_add_u64(&ClusterXnodeProfileCtl->hist[slot][cluster_xp_hist_bucket_index(nanos)],
								1);
}

#endif /* !FRONTEND */

#endif /* CLUSTER_XNODE_PROFILE_H */
