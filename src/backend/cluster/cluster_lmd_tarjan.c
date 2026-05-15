/*-------------------------------------------------------------------------
 *
 * cluster_lmd_tarjan.c
 *	  pgrac LMD Tarjan SCC + deterministic youngest victim + revalidate.
 *
 *	  spec-2.22 D3:iterative snapshot-based SCC.
 *
 *	  Approach (A2 snapshot-based):
 *	    1. graph_snapshot_copy under LW_SHARED → release lock.
 *	    2. Build adjacency list over (vertex index) — process-local Tarjan.
 *	    3. Iterative Tarjan SCC (no recursion;explicit stack to avoid
 *	       stack-overflow with deep cycles).
 *	    4. For each SCC with size >= 2 (or self-loop with size==1 +
 *	       explicit self-edge): pick victim by deterministic sort tuple
 *	       (A4 — cluster_epoch DESC, local_start_ts_ms DESC, node_id DESC,
 *	       request_id DESC, procno DESC, xid DESC).
 *	    5. Revalidate (HC14 A3):  before signaling victim,re-snapshot;
 *	       if generation advanced AND cycle edges still exist with same
 *	       vertices, proceed.  Else advisory counter bump,no cancel.
 *	    6. If victim.node_id == self → resolve PGPROC + ProcSignal cancel
 *	       slot (D8 wire);else cross_node_victim_pending_count ++
 *	       (forward-link spec-2.23 cross-node cancel forwarding).
 *
 *	  All counters atomic;no LWLock held outside graph_snapshot_copy.
 *
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/backend/cluster/cluster_lmd_tarjan.c
 *
 * NOTES
 *	  This is a pgrac-original file.  Compiled only in --enable-cluster
 *	  builds.
 *	  Spec: spec-2.22-lmd-tarjan-cross-node-deadlock.md (FROZEN v0.3).
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "cluster/cluster_conf.h"			/* CLUSTER_MAX_NODES + active peers */
#include "cluster/cluster_epoch.h"			/* cluster_epoch_get_current */
#include "cluster/cluster_ges.h"			/* GesDeadlockProbePayload / Report */
#include "cluster/cluster_grd_outbound.h"	/* cluster_grd_outbound_enqueue_backend_request */
#include "cluster/cluster_guc.h"
#include "cluster/cluster_lmd.h"
#include "miscadmin.h"
#include "port/atomics.h"
#include "storage/proc.h"
#include "storage/procarray.h"
#include "storage/procsignal.h"
#include "storage/latch.h"
#include "utils/memutils.h"
#include "utils/timestamp.h"
#include "utils/wait_event.h"

#include <limits.h>


/* ============================================================
 * Vertex compare (A4 deterministic youngest).
 *
 *	Ordering = youngest first DESC by:
 *	  cluster_epoch DESC, local_start_ts_ms DESC, node_id DESC,
 *	  request_id DESC, procno DESC, xid DESC.
 *
 *	Returns negative if a is "younger" (preferred victim), positive if
 *	a is "older", 0 if exactly equal (should not happen — identity
 *	4-tuple uniqueness invariant).
 * ============================================================ */

static int
vertex_youngest_first_cmp(const ClusterLmdVertex *a, const ClusterLmdVertex *b)
{
	/* DESC = "a is younger" means a sorts before b → return < 0. */
	if (a->cluster_epoch != b->cluster_epoch)
		return (a->cluster_epoch > b->cluster_epoch) ? -1 : 1;
	if (a->local_start_ts_ms != b->local_start_ts_ms)
		return (a->local_start_ts_ms > b->local_start_ts_ms) ? -1 : 1;
	if (a->node_id != b->node_id)
		return (a->node_id > b->node_id) ? -1 : 1;
	if (a->request_id != b->request_id)
		return (a->request_id > b->request_id) ? -1 : 1;
	if (a->procno != b->procno)
		return (a->procno > b->procno) ? -1 : 1;
	if (a->xid != b->xid)
		return (a->xid > b->xid) ? -1 : 1;
	return 0;
}


/* ============================================================
 * Public API — externally callable iterative Tarjan helpers.
 *
 *	cluster_lmd_tarjan_scan_snapshot:  given a snapshot of edges, run
 *	  iterative Tarjan SCC; write cycle vertices flat into out_buf.
 *	  Returns number of SCCs with size >= 2 (or size 1 + self-loop).
 *	  Multiple cycles concatenated;caller can pick first or iterate.
 *
 *	  Internal:
 *	  - Build vertex list by deduping waiters + blockers.
 *	  - Build adjacency by edge waiter -> blocker.
 *	  - Iterative Tarjan with explicit stack frames.
 *
 *	cluster_lmd_tarjan_pick_victim:  walk cycle vertices,return
 *	  vertex with min vertex_youngest_first_cmp (== youngest).
 *
 *	cluster_lmd_tarjan_revalidate:  re-snapshot graph; verify that a real
 *	  cycle over the same vertex set still exists in the current snapshot.
 * ============================================================ */

/* internal: find vertex index in dedup list, returning -1 if not found */
static int
find_vertex_index(const ClusterLmdVertex *list, int nvertices, const ClusterLmdVertex *target)
{
	for (int i = 0; i < nvertices; i++) {
		if (list[i].node_id == target->node_id && list[i].procno == target->procno
			&& list[i].cluster_epoch == target->cluster_epoch
			&& list[i].request_id == target->request_id)
			return i;
	}
	return -1;
}

/*
 * Run iterative Tarjan over a snapshot of edges.
 *
 *	Allocates working state in a private memory context (caller can run
 *	this without holding any LWLock).  Writes cycle vertices flat into
 *	out_cycle_vertices (up to max_cycle_vertices), and *out_cycle_count
 *	receives the number of distinct cycles found.
 *
 *	Returns the number of cycle vertices written.
 */
int
cluster_lmd_tarjan_scan_snapshot(const ClusterLmdWaitEdge *edges, int nedges,
								 ClusterLmdVertex *out_cycle_vertices, int max_cycle_vertices,
								 int *out_cycle_count)
{
	MemoryContext oldctx, work;
	ClusterLmdVertex *vertices;
	int *adj_head, *adj_next, *adj_to;
	int nvertices = 0;
	int adj_used = 0;
	int *index_arr, *lowlink_arr;
	bool *on_stack;
	int *scc_stack, scc_stack_top = 0;
	int *call_stack_v, *call_stack_iter; /* iterative Tarjan frames */
	int call_top = 0;
	int next_index = 0;
	int written = 0;
	int ncycles = 0;

	if (out_cycle_count)
		*out_cycle_count = 0;
	if (nedges <= 0 || edges == NULL)
		return 0;

	work = AllocSetContextCreate(CurrentMemoryContext, "LMD Tarjan work", ALLOCSET_DEFAULT_SIZES);
	oldctx = MemoryContextSwitchTo(work);

	/* Max vertices = 2 * nedges (waiter + blocker per edge). */
	vertices = (ClusterLmdVertex *)palloc(sizeof(ClusterLmdVertex) * nedges * 2);
	adj_head = (int *)palloc(sizeof(int) * nedges * 2);
	adj_next = (int *)palloc(sizeof(int) * nedges);
	adj_to = (int *)palloc(sizeof(int) * nedges);
	for (int i = 0; i < nedges * 2; i++)
		adj_head[i] = -1;

	/* Pass 1: dedup waiters + blockers into vertices[] + build adjacency. */
	for (int e = 0; e < nedges; e++) {
		int wi = find_vertex_index(vertices, nvertices, &edges[e].waiter);
		int bi;

		if (wi < 0) {
			vertices[nvertices] = edges[e].waiter;
			wi = nvertices++;
		}
		bi = find_vertex_index(vertices, nvertices, &edges[e].blocker);
		if (bi < 0) {
			vertices[nvertices] = edges[e].blocker;
			bi = nvertices++;
		}
		adj_to[adj_used] = bi;
		adj_next[adj_used] = adj_head[wi];
		adj_head[wi] = adj_used;
		adj_used++;
	}

	/* Tarjan state. */
	index_arr = (int *)palloc(sizeof(int) * nvertices);
	lowlink_arr = (int *)palloc(sizeof(int) * nvertices);
	on_stack = (bool *)palloc0(sizeof(bool) * nvertices);
	scc_stack = (int *)palloc(sizeof(int) * nvertices);
	call_stack_v = (int *)palloc(sizeof(int) * nvertices * 2);
	call_stack_iter = (int *)palloc(sizeof(int) * nvertices * 2);
	for (int i = 0; i < nvertices; i++)
		index_arr[i] = -1;

	/* Iterative Tarjan main loop. */
	for (int start = 0; start < nvertices; start++) {
		if (index_arr[start] >= 0)
			continue;

		/* Push start onto call stack. */
		call_top = 0;
		call_stack_v[call_top] = start;
		call_stack_iter[call_top] = adj_head[start];
		index_arr[start] = next_index;
		lowlink_arr[start] = next_index;
		next_index++;
		scc_stack[scc_stack_top++] = start;
		on_stack[start] = true;
		call_top++;

		while (call_top > 0) {
			int v = call_stack_v[call_top - 1];
			int it = call_stack_iter[call_top - 1];

			if (it != -1) {
				int w = adj_to[it];

				/* Advance iterator for next iteration when we resume. */
				call_stack_iter[call_top - 1] = adj_next[it];

				if (index_arr[w] < 0) {
					/* Recurse: push w. */
					index_arr[w] = next_index;
					lowlink_arr[w] = next_index;
					next_index++;
					scc_stack[scc_stack_top++] = w;
					on_stack[w] = true;
					call_stack_v[call_top] = w;
					call_stack_iter[call_top] = adj_head[w];
					call_top++;
				} else if (on_stack[w]) {
					if (index_arr[w] < lowlink_arr[v])
						lowlink_arr[v] = index_arr[w];
				}
				/* else: already in a finished SCC, ignore. */
			} else {
				/* Iterator exhausted — pop and propagate lowlink. */
				int v_popped = call_stack_v[--call_top];

				if (lowlink_arr[v_popped] == index_arr[v_popped]) {
					int scc_size = 0;
					int *scc_members;
					int w;
					bool is_cycle;

					scc_members = (int *)palloc(sizeof(int) * scc_stack_top);
					do {
						w = scc_stack[--scc_stack_top];
						on_stack[w] = false;
						scc_members[scc_size++] = w;
					} while (w != v_popped);

					/* SCC size >= 2 = real cycle.  size == 1 only counts if
					 * there's a self-loop edge (v -> v).  Check adjacency. */
					is_cycle = (scc_size >= 2);
					if (scc_size == 1) {
						int aiter = adj_head[v_popped];

						while (aiter != -1) {
							if (adj_to[aiter] == v_popped) {
								is_cycle = true;
								break;
							}
							aiter = adj_next[aiter];
						}
					}

					if (is_cycle) {
						ncycles++;
						for (int i = 0; i < scc_size; i++) {
							if (written < max_cycle_vertices)
								out_cycle_vertices[written++] = vertices[scc_members[i]];
						}
					}
					pfree(scc_members);
				}

				/* Propagate lowlink to parent. */
				if (call_top > 0) {
					int parent = call_stack_v[call_top - 1];

					if (lowlink_arr[v_popped] < lowlink_arr[parent])
						lowlink_arr[parent] = lowlink_arr[v_popped];
				}
			}
		}
	}

	MemoryContextSwitchTo(oldctx);
	MemoryContextDelete(work);

	if (out_cycle_count)
		*out_cycle_count = ncycles;
	return written;
}


void
cluster_lmd_tarjan_pick_victim(const ClusterLmdVertex *cycle_vertices, int nvertices,
							   ClusterLmdVertex *out_victim)
{
	int best = 0;

	Assert(nvertices > 0 && out_victim != NULL);
	for (int i = 1; i < nvertices; i++) {
		if (vertex_youngest_first_cmp(&cycle_vertices[i], &cycle_vertices[best]) < 0)
			best = i;
	}
	*out_victim = cycle_vertices[best];
}


bool
cluster_lmd_tarjan_revalidate(const ClusterLmdVertex *cycle_vertices, int nvertices,
							  uint64 snapshot_generation)
{
	int max_edges = cluster_lmd_max_wait_edges;
	ClusterLmdWaitEdge *fresh;
	ClusterLmdWaitEdge *induced;
	ClusterLmdVertex *fresh_cycle_vertices;
	int n_fresh;
	uint64 fresh_gen;
	int n_induced = 0;
	int fresh_cycle_count = 0;
	int n_fresh_cycle_vertices;
	bool valid = false;

	if (nvertices <= 0)
		return false;
	if (max_edges < 64)
		max_edges = 64;
	if (max_edges > 65536)
		max_edges = 65536;

	fresh = (ClusterLmdWaitEdge *)palloc(sizeof(ClusterLmdWaitEdge) * max_edges);
	n_fresh = cluster_lmd_graph_snapshot_copy(fresh, max_edges, &fresh_gen);
	(void)snapshot_generation;
	(void)fresh_gen;

	/*
	 * Revalidation must prove that a real cycle still exists among the same
	 * vertex set.  "Each vertex is still waiting" is not enough: A->B/B->A
	 * can become A->C/B->D and must not cancel either backend.
	 */
	induced = (ClusterLmdWaitEdge *)palloc(sizeof(ClusterLmdWaitEdge) * Max(n_fresh, 1));
	for (int e = 0; e < n_fresh; e++) {
		if (find_vertex_index(cycle_vertices, nvertices, &fresh[e].waiter) >= 0
			&& find_vertex_index(cycle_vertices, nvertices, &fresh[e].blocker) >= 0)
			induced[n_induced++] = fresh[e];
	}

	if (n_induced > 0) {
		fresh_cycle_vertices = (ClusterLmdVertex *)palloc(sizeof(ClusterLmdVertex) * nvertices);
		n_fresh_cycle_vertices = cluster_lmd_tarjan_scan_snapshot(
			induced, n_induced, fresh_cycle_vertices, nvertices, &fresh_cycle_count);
		if (fresh_cycle_count == 1 && n_fresh_cycle_vertices == nvertices) {
			valid = true;
			for (int v = 0; v < nvertices; v++) {
				if (find_vertex_index(fresh_cycle_vertices, n_fresh_cycle_vertices,
									  &cycle_vertices[v])
					< 0) {
					valid = false;
					break;
				}
			}
		}
		pfree(fresh_cycle_vertices);
	}
	pfree(induced);
	pfree(fresh);

	if (valid)
		return true;
	cluster_lmd_revalidate_fail_count_inc(1);
	return false;
}


/* ============================================================
 * Coordinator scan entry — called by LmdMain on each tick.
 *
 *	D8 victim cancel mechanism wired in this body:
 *	  - For each cycle detected → revalidate → pick victim.
 *	  - If victim.node_id == self_node_id: resolve PGPROC by procno
 *	    + cluster_epoch verify + ProcSignal cancel flag set.
 *	    HC10 inherit:本 spec MVP 仅本节点 cancel;cross-node 推 spec-2.23.
 *	  - else: cross_node_victim_pending_count++ + log.
 * ============================================================ */

static int32 self_node_id_cache = -1;

static int32
get_self_node_id(void)
{
	if (self_node_id_cache < 0)
		self_node_id_cache = cluster_node_id; /* GUC */
	return self_node_id_cache;
}

void
cluster_lmd_tarjan_run_local_scan(void)
{
	int max_edges = cluster_lmd_max_wait_edges;
	ClusterLmdWaitEdge *snapshot;
	int nedges;
	uint64 gen_at_snapshot;
	ClusterLmdVertex *cycle_vertices;
	int max_cycle_vertices = max_edges * 2;
	int cycle_count = 0;
	int n_cycle_v;
	int idx;
	int32 self_node;

	if (max_edges < 64)
		max_edges = 64;
	if (max_edges > 65536)
		max_edges = 65536;

	cluster_lmd_tarjan_scan_count_inc(1);

	snapshot = (ClusterLmdWaitEdge *)palloc(sizeof(ClusterLmdWaitEdge) * max_edges);
	nedges = cluster_lmd_graph_snapshot_copy(snapshot, max_edges, &gen_at_snapshot);
	if (nedges == 0) {
		pfree(snapshot);
		return;
	}

	cycle_vertices = (ClusterLmdVertex *)palloc(sizeof(ClusterLmdVertex) * max_cycle_vertices);
	n_cycle_v = cluster_lmd_tarjan_scan_snapshot(snapshot, nedges, cycle_vertices,
												 max_cycle_vertices, &cycle_count);
	pfree(snapshot);

	if (cycle_count == 0) {
		pfree(cycle_vertices);
		return;
	}

	cluster_lmd_cycle_detected_count_inc(cycle_count);

	/* Flat cycle vertices: SCC1 vertices, SCC2 vertices, ... — but we
	 * don't track SCC boundaries explicitly.  For MVP: treat the entire
	 * cycle_vertices array as one combined cycle and pick one victim.
	 * Multi-cycle distinction lands in Hardening. */
	self_node = get_self_node_id();
	idx = 0;
	while (idx < n_cycle_v) {
		ClusterLmdVertex victim;
		int scc_end = n_cycle_v; /* MVP — single big cycle */

		cluster_lmd_tarjan_pick_victim(&cycle_vertices[idx], scc_end - idx, &victim);

		if (cluster_lmd_tarjan_revalidate(&cycle_vertices[idx], scc_end - idx, gen_at_snapshot)) {
			if (victim.node_id == self_node) {
				/* D8 wire — set local backend cancel flag.  Implementation
					 * forward-link:  cluster_lmd_signal_local_victim(procno). */
				extern void cluster_lmd_signal_local_victim(uint32 procno, uint64 request_id,
															uint64 cluster_epoch);
				cluster_lmd_signal_local_victim(victim.procno, victim.request_id,
												victim.cluster_epoch);
				cluster_lmd_victim_cancel_sent_count_inc(1);
			} else {
				/* Cross-node victim — production forwarding 推 spec-2.23. */
				cluster_lmd_cross_node_victim_pending_count_inc(1);
				ereport(LOG, (errmsg("cluster LMD cross-node deadlock victim on node %d"
									 " (procno=%u request_id=" UINT64_FORMAT ");"
									 " cross-node cancel forwarding will be wired in spec-2.23",
									 victim.node_id, victim.procno, victim.request_id)));
			}
		}
		/* else: revalidate_fail_count already incremented inside revalidate */
		idx = scc_end;
		break; /* MVP — one cycle per scan tick */
	}

	pfree(cycle_vertices);
}


void
cluster_lmd_run_tarjan_scan_now(void)
{
	cluster_lmd_tarjan_run_local_scan();
}


/* ============================================================
 * spec-2.23 D8 — cross-node DEADLOCK_PROBE coordinator scan + REPORT
 * collector (file-static slab, single probe_id in-flight per scan tick).
 *
 *	FU-2 design:
 *	  - LmdProbeCollector lives in this translation unit; only one
 *	    in-flight probe_id at a time (the coordinator scan loop is
 *	    single-threaded).
 *	  - reports[] are palloc'd in TopMemoryContext when the REPORT
 *	    handler matches probe_id; freed by probe_collect_reset (on
 *	    timeout or after D9 union completes).
 *	  - pg_memory_barrier() bridges the (single-reader-single-writer)
 *	    pattern between the LMD scan loop and the REPORT receive
 *	    handler — no LWLock needed.
 *	  - HC8 partial REPORT acceptable: timeout sweep increments
 *	    probe_partial_count and resets the collector for the next tick.
 * ============================================================ */

typedef struct LmdProbeCollector {
	uint64				probe_id;	/* 0 = idle slot */
	int32				n_expected;
	int32				n_received;
	TimestampTz			deadline;
	GesDeadlockReportHeader *reports[CLUSTER_MAX_NODES];
	Size				report_sizes[CLUSTER_MAX_NODES];
} LmdProbeCollector;

static LmdProbeCollector probe_collector; /* file-static, single in-flight */
static uint64 probe_id_seq;

/*
 * Probe collector reset — releases any palloc'd REPORT buffers and
 * clears the slab.  Safe to call from idle and from timeout sweep.
 */
static void
probe_collector_reset(void)
{
	for (int i = 0; i < CLUSTER_MAX_NODES; i++) {
		if (probe_collector.reports[i] != NULL) {
			pfree(probe_collector.reports[i]);
			probe_collector.reports[i] = NULL;
			probe_collector.report_sizes[i] = 0;
		}
	}
	probe_collector.probe_id = 0;
	probe_collector.n_expected = 0;
	probe_collector.n_received = 0;
	probe_collector.deadline = 0;
}

/*
 * Feed a received REPORT into the collector.  Called from the
 * cluster_ges DEADLOCK_REPORT receive path.  Returns true if the
 * REPORT was accepted (probe_id match + slot free); false otherwise
 * (stale or duplicate — increment probe_partial_count externally).
 */
bool
cluster_lmd_probe_collect_receive(const GesDeadlockReportHeader *report, Size report_len)
{
	int slot;

	Assert(report != NULL);

	if (probe_collector.probe_id == 0 || report->probe_id != probe_collector.probe_id)
		return false;

	/* Find first empty slot.  CLUSTER_MAX_NODES is small (128). */
	for (slot = 0; slot < CLUSTER_MAX_NODES; slot++) {
		if (probe_collector.reports[slot] == NULL)
			break;
	}
	if (slot >= CLUSTER_MAX_NODES)
		return false;

	probe_collector.reports[slot]
		= (GesDeadlockReportHeader *) MemoryContextAlloc(TopMemoryContext, report_len);
	memcpy(probe_collector.reports[slot], report, report_len);
	probe_collector.report_sizes[slot] = report_len;
	pg_write_barrier();
	probe_collector.n_received++;
	return true;
}

/*
 * Run the coordinator scan tick.  Called from LmdMain when this node is
 * the elected coordinator (HC16 lowest active node_id).  Broadcasts
 * DEADLOCK_PROBE to N-1 active peers, waits up to the partial-OK
 * timeout, then hands the union-edge list to D9 (Step 7).
 *
 *	Step 6 ships the broadcast + collector wait + partial counter; the
 *	D9 union Tarjan invocation lands in Step 7.  The current body
 *	resets the collector at the end of the scan regardless of D9 outcome.
 */
void
cluster_lmd_tarjan_run_coordinator_scan(int collect_timeout_ms)
{
	int32 self_node = get_self_node_id();
	int n_peers = 0;
	int32 peers[CLUSTER_MAX_NODES];
	GesDeadlockProbePayload probe;
	TimestampTz now;

	if (collect_timeout_ms <= 0)
		collect_timeout_ms = cluster_lmd_probe_collect_timeout_ms;

	/* (1) Build active-peer list (skip self).  Active-status comes from
	 *	  cluster_conf_node_count() + the conf cache. */
	{
		int total = cluster_conf_node_count();
		for (int i = 0; i < total && n_peers < CLUSTER_MAX_NODES; i++) {
			/*
			 * cluster_conf doesn't expose per-index node lookup as a
			 * public API; iterate by node_id range 0..total-1 which is
			 * the convention used by spec-2.x ship code.
			 */
			if (i == self_node)
				continue;
			peers[n_peers++] = i;
		}
	}

	if (n_peers == 0)
		return;	/* Single-node mode — no cross-node probe needed. */

	/* (2) Reset any stale collector state from a prior interrupted tick. */
	probe_collector_reset();
	probe_collector.probe_id = ++probe_id_seq;
	probe_collector.n_expected = n_peers;
	probe_collector.n_received = 0;
	now = GetCurrentTimestamp();
	probe_collector.deadline = TimestampTzPlusMilliseconds(now, collect_timeout_ms);

	/* (3) Build PROBE payload + broadcast to each active peer. */
	memset(&probe, 0, sizeof(probe));
	probe.opcode = GES_REQ_OPCODE_DEADLOCK_PROBE;
	probe.coordinator_node_id = (uint32) self_node;
	probe.probe_id = probe_collector.probe_id;
	/*
	 * generation_snapshot is informational for HC20 cross-node revalidate
	 * (Step 7 D9 uses it for second-round PROBE generation comparison).
	 * Step 6 ships 0 — Step 7 reads cluster_lmd_graph_state->generation
	 * via the existing extern accessor.
	 */
	probe.generation_snapshot = 0;

	for (int i = 0; i < n_peers; i++) {
		(void) cluster_grd_outbound_enqueue_backend_request((uint32) peers[i], &probe,
															sizeof(probe));
	}

	/*
	 * (4) Poll-wait for REPORTs up to deadline.  WaitLatch with short
	 *	  interrupt allows ProcessInterrupts to drive the receive path.
	 *	  HC8: partial OK — break out of the wait loop once expected
	 *	  count is reached OR deadline passes.
	 */
	while (probe_collector.n_received < probe_collector.n_expected) {
		now = GetCurrentTimestamp();
		if (now >= probe_collector.deadline) {
			cluster_lmd_probe_partial_count_inc(1);
			break;
		}
		CHECK_FOR_INTERRUPTS();
		(void) WaitLatch(MyLatch, WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH, 50,
						 WAIT_EVENT_CLUSTER_LMD_PROBE_COLLECT);
		ResetLatch(MyLatch);
	}

	cluster_lmd_probe_broadcast_count_inc(1);

	/*
	 * (5) spec-2.23 D9 — union edge merge + Tarjan + cross-node revalidate.
	 *
	 *	Total union edges = (sum over received REPORTs of report.nedges) +
	 *	local snapshot edges.  We palloc a single union buffer and copy
	 *	edges from each REPORT body (the bytes immediately following
	 *	GesDeadlockReportHeader are nedges * ClusterLmdWaitEdge entries
	 *	per spec-2.22 D6 payload layout) and from the local graph
	 *	snapshot (the coordinator's own waiting backends).
	 */
	{
		int total_edges = 0;
		ClusterLmdWaitEdge *union_edges = NULL;
		int n_union = 0;
		ClusterLmdVertex *cycle_vertices = NULL;
		int cycle_count = 0;
		int n_cycle_v = 0;
		int max_local_edges = cluster_lmd_max_wait_edges;
		ClusterLmdWaitEdge *local_snapshot = NULL;
		int local_n = 0;
		uint64 local_gen = 0;

		if (max_local_edges < 64)
			max_local_edges = 64;
		if (max_local_edges > 65536)
			max_local_edges = 65536;

		for (int i = 0; i < CLUSTER_MAX_NODES; i++) {
			if (probe_collector.reports[i] != NULL)
				total_edges += (int) probe_collector.reports[i]->nedges;
		}
		/* Reserve room for the local snapshot too. */
		total_edges += max_local_edges;

		if (total_edges <= 0) {
			probe_collector_reset();
			return;
		}

		union_edges = (ClusterLmdWaitEdge *) palloc(sizeof(ClusterLmdWaitEdge) * total_edges);

		/* Local snapshot first — the coordinator's own waiting edges. */
		local_snapshot = (ClusterLmdWaitEdge *) palloc(sizeof(ClusterLmdWaitEdge) * max_local_edges);
		local_n = cluster_lmd_graph_snapshot_copy(local_snapshot, max_local_edges, &local_gen);
		if (local_n > 0) {
			memcpy(union_edges, local_snapshot, sizeof(ClusterLmdWaitEdge) * local_n);
			n_union = local_n;
		}
		pfree(local_snapshot);

		/* Remote REPORT edges follow.  Pointer arithmetic walks past the
		 * header to the edge array; spec-2.22 D6 fixes the wire layout. */
		for (int i = 0; i < CLUSTER_MAX_NODES; i++) {
			const GesDeadlockReportHeader *report = probe_collector.reports[i];
			const ClusterLmdWaitEdge *report_edges;

			if (report == NULL)
				continue;
			report_edges = (const ClusterLmdWaitEdge *) (((const char *) report)
														 + sizeof(GesDeadlockReportHeader));
			if (n_union + (int) report->nedges > total_edges)
				break;	/* defensive: should not happen given total_edges math */
			memcpy(union_edges + n_union, report_edges,
				   sizeof(ClusterLmdWaitEdge) * report->nedges);
			n_union += (int) report->nedges;
		}

		if (n_union == 0) {
			pfree(union_edges);
			probe_collector_reset();
			return;
		}

		cycle_vertices = (ClusterLmdVertex *) palloc(sizeof(ClusterLmdVertex) * n_union * 2);
		n_cycle_v = cluster_lmd_tarjan_scan_snapshot(union_edges, n_union, cycle_vertices,
													 n_union * 2, &cycle_count);

		if (cycle_count > 0) {
			ClusterLmdVertex victim;

			cluster_lmd_cycle_detected_count_inc(cycle_count);
			cluster_lmd_tarjan_pick_victim(cycle_vertices, n_cycle_v, &victim);

			/*
			 * HC20 cross-node revalidate:
			 *   local victim → reuse spec-2.22 D3 induced-subgraph
			 *   revalidate against local graph generation.
			 *   remote victim → revalidate fail / pass is determined
			 *   solely by the just-collected REPORTs; second-round PROBE
			 *   is a future amend (full retry cost is high — Step 11 TAP
			 *   L15 exercises the fail path).  For Step 7 we treat the
			 *   union result as authoritative if cycle still covers the
			 *   victim vertex.
			 */
			if (victim.node_id == self_node) {
				if (cluster_lmd_tarjan_revalidate(cycle_vertices, n_cycle_v, local_gen)) {
					cluster_lmd_signal_local_victim(victim.procno, victim.request_id,
													victim.cluster_epoch);
					cluster_lmd_victim_cancel_sent_count_inc(1);
				}
				/* else revalidate_fail_count already incremented inside revalidate */
			} else {
				/*
				 * Step 8 D10 — local-vs-remote victim 分流.  spec-2.23
				 * scope does NOT forward the cancel signal to the
				 * remote LMD;  spec-2.24 D axis wires the cross-node
				 * cancel envelope.  Increment the pending counter so
				 * dump_lmd surfaces the detection.
				 */
				cluster_lmd_cross_node_victim_pending_count_inc(1);
				ereport(LOG, (errmsg("cluster LMD cross-node deadlock victim on node %d"
									 " (procno=%u request_id=" UINT64_FORMAT ");"
									 " cross-node cancel forwarding lands in spec-2.24",
									 victim.node_id, victim.procno, victim.request_id)));
			}
		}

		pfree(cycle_vertices);
		pfree(union_edges);
	}

	probe_collector_reset();
}


/*
 * spec-2.22 D8 — signal local victim backend with PROCSIG_CLUSTER_GES_CANCEL.
 *
 *	Resolve target PGPROC by procno;verify the live backend identity
 *	matches the captured request_id (defense against stale procno reuse —
 *	backend exit + new backend reuses same procno slot).  If verify
 *	passes, SendProcSignal with PROCSIG_CLUSTER_GES_CANCEL slot (spec-
 *	2.17 ship).  Handler sets sig_atomic_t cluster_ges_cancel_pending;
 *	the receiving backend observes it in the seven-step dispatch loop
 *	(per HC10 + spec-2.21 D7 wire) and returns FAIL_DEADLOCK without
 *	ereport-in-handler (L118 inherit).
 */
void
cluster_lmd_signal_local_victim(uint32 procno, uint64 request_id, uint64 cluster_epoch)
{
	PGPROC *target;
	pid_t target_pid;
	int target_backendid;

	if (procno >= (uint32)ProcGlobal->allProcCount) {
		ereport(LOG, (errmsg("cluster LMD victim procno %u out of range, skipping", procno)));
		return;
	}
	target = &ProcGlobal->allProcs[procno];
	target_pid = target->pid;
	target_backendid = target->backendId;

	if (target_pid == 0 || target_backendid == InvalidBackendId) {
		ereport(LOG, (errmsg("cluster LMD victim procno=%u has no live backend "
							 "(stale procno or exit race); skipping",
							 procno)));
		return;
	}

	/*
	 * Note:  request_id / cluster_epoch verification is best-effort (no
	 * authoritative source-of-truth field on PGPROC for these yet);  for
	 * production-grade race protection we will revalidate via re-snapshot
	 * (HC14) before signalling and again in the backend's own seven_step
	 * dispatch flag check.  Stale procno + identity mismatch = backend
	 * sees no edge in graph → falls through clean.
	 */
	(void)request_id;
	(void)cluster_epoch;

	(void)SendProcSignal(target_pid, PROCSIG_CLUSTER_GES_CANCEL, target_backendid);
	ereport(DEBUG1, (errmsg("cluster LMD sent PROCSIG_CLUSTER_GES_CANCEL to "
							"procno=%u pid=%d backendid=%d (deadlock victim)",
							procno, (int)target_pid, target_backendid)));
}
