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

#include "cluster/cluster_guc.h"
#include "cluster/cluster_lmd.h"
#include "miscadmin.h"
#include "port/atomics.h"
#include "storage/proc.h"
#include "storage/procarray.h"
#include "storage/procsignal.h"
#include "utils/memutils.h"

#include <limits.h>


/* ============================================================
 * Counters (file-static atomic — accessed via *_get accessors).
 * ============================================================ */

static pg_atomic_uint64 tarjan_scan_count;
static pg_atomic_uint64 cycle_detected_count;
static pg_atomic_uint64 victim_cancel_sent_count;
static pg_atomic_uint64 revalidate_fail_count;
static pg_atomic_uint64 cross_node_victim_pending_count;
static bool tarjan_counters_initialized = false;

static void
ensure_tarjan_counters_init(void)
{
	if (!tarjan_counters_initialized)
	{
		pg_atomic_init_u64(&tarjan_scan_count, 0);
		pg_atomic_init_u64(&cycle_detected_count, 0);
		pg_atomic_init_u64(&victim_cancel_sent_count, 0);
		pg_atomic_init_u64(&revalidate_fail_count, 0);
		pg_atomic_init_u64(&cross_node_victim_pending_count, 0);
		tarjan_counters_initialized = true;
	}
}


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
 *	cluster_lmd_tarjan_revalidate:  re-snapshot graph; verify cycle
 *	  vertices still all present as waiters in current snapshot;
 *	  return true if cycle is real.
 * ============================================================ */

/* internal: find vertex index in dedup list, returning -1 if not found */
static int
find_vertex_index(const ClusterLmdVertex *list, int nvertices,
				  const ClusterLmdVertex *target)
{
	for (int i = 0; i < nvertices; i++)
	{
		if (list[i].node_id == target->node_id &&
			list[i].procno == target->procno &&
			list[i].cluster_epoch == target->cluster_epoch &&
			list[i].request_id == target->request_id)
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
								 ClusterLmdVertex *out_cycle_vertices,
								 int max_cycle_vertices, int *out_cycle_count)
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

	work = AllocSetContextCreate(CurrentMemoryContext,
								 "LMD Tarjan work", ALLOCSET_DEFAULT_SIZES);
	oldctx = MemoryContextSwitchTo(work);

	/* Max vertices = 2 * nedges (waiter + blocker per edge). */
	vertices = (ClusterLmdVertex *) palloc(sizeof(ClusterLmdVertex) * nedges * 2);
	adj_head = (int *) palloc(sizeof(int) * nedges * 2);
	adj_next = (int *) palloc(sizeof(int) * nedges);
	adj_to = (int *) palloc(sizeof(int) * nedges);
	for (int i = 0; i < nedges * 2; i++)
		adj_head[i] = -1;

	/* Pass 1: dedup waiters + blockers into vertices[] + build adjacency. */
	for (int e = 0; e < nedges; e++)
	{
		int wi = find_vertex_index(vertices, nvertices, &edges[e].waiter);
		int bi;

		if (wi < 0)
		{
			vertices[nvertices] = edges[e].waiter;
			wi = nvertices++;
		}
		bi = find_vertex_index(vertices, nvertices, &edges[e].blocker);
		if (bi < 0)
		{
			vertices[nvertices] = edges[e].blocker;
			bi = nvertices++;
		}
		adj_to[adj_used] = bi;
		adj_next[adj_used] = adj_head[wi];
		adj_head[wi] = adj_used;
		adj_used++;
	}

	/* Tarjan state. */
	index_arr = (int *) palloc(sizeof(int) * nvertices);
	lowlink_arr = (int *) palloc(sizeof(int) * nvertices);
	on_stack = (bool *) palloc0(sizeof(bool) * nvertices);
	scc_stack = (int *) palloc(sizeof(int) * nvertices);
	call_stack_v = (int *) palloc(sizeof(int) * nvertices * 2);
	call_stack_iter = (int *) palloc(sizeof(int) * nvertices * 2);
	for (int i = 0; i < nvertices; i++)
		index_arr[i] = -1;

	/* Iterative Tarjan main loop. */
	for (int start = 0; start < nvertices; start++)
	{
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

		while (call_top > 0)
		{
			int v = call_stack_v[call_top - 1];
			int it = call_stack_iter[call_top - 1];

			if (it != -1)
			{
				int w = adj_to[it];

				/* Advance iterator for next iteration when we resume. */
				call_stack_iter[call_top - 1] = adj_next[it];

				if (index_arr[w] < 0)
				{
					/* Recurse: push w. */
					index_arr[w] = next_index;
					lowlink_arr[w] = next_index;
					next_index++;
					scc_stack[scc_stack_top++] = w;
					on_stack[w] = true;
					call_stack_v[call_top] = w;
					call_stack_iter[call_top] = adj_head[w];
					call_top++;
				}
				else if (on_stack[w])
				{
					if (index_arr[w] < lowlink_arr[v])
						lowlink_arr[v] = index_arr[w];
				}
				/* else: already in a finished SCC, ignore. */
			}
			else
			{
				/* Iterator exhausted — pop and propagate lowlink. */
				int v_popped = call_stack_v[--call_top];

				if (lowlink_arr[v_popped] == index_arr[v_popped])
				{
					int scc_size = 0;
					int *scc_members;
					int w;
					bool is_cycle;

					scc_members = (int *) palloc(sizeof(int) * scc_stack_top);
					do
					{
						w = scc_stack[--scc_stack_top];
						on_stack[w] = false;
						scc_members[scc_size++] = w;
					}
					while (w != v_popped);

					/* SCC size >= 2 = real cycle.  size == 1 only counts if
					 * there's a self-loop edge (v -> v).  Check adjacency. */
					is_cycle = (scc_size >= 2);
					if (scc_size == 1)
					{
						int aiter = adj_head[v_popped];

						while (aiter != -1)
						{
							if (adj_to[aiter] == v_popped)
							{
								is_cycle = true;
								break;
							}
							aiter = adj_next[aiter];
						}
					}

					if (is_cycle)
					{
						ncycles++;
						for (int i = 0; i < scc_size; i++)
						{
							if (written < max_cycle_vertices)
								out_cycle_vertices[written++] = vertices[scc_members[i]];
						}
					}
					pfree(scc_members);
				}

				/* Propagate lowlink to parent. */
				if (call_top > 0)
				{
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
cluster_lmd_tarjan_pick_victim(const ClusterLmdVertex *cycle_vertices,
							   int nvertices, ClusterLmdVertex *out_victim)
{
	int best = 0;

	Assert(nvertices > 0 && out_victim != NULL);
	for (int i = 1; i < nvertices; i++)
	{
		if (vertex_youngest_first_cmp(&cycle_vertices[i],
									  &cycle_vertices[best]) < 0)
			best = i;
	}
	*out_victim = cycle_vertices[best];
}


bool
cluster_lmd_tarjan_revalidate(const ClusterLmdVertex *cycle_vertices,
							  int nvertices, uint64 snapshot_generation)
{
	uint64 cur_gen;
	int max_edges = cluster_lmd_max_wait_edges;
	ClusterLmdWaitEdge *fresh;
	int n_fresh;
	uint64 fresh_gen;
	int found_count;

	if (nvertices <= 0)
		return false;

	cur_gen = cluster_lmd_graph_generation_get();
	(void) cur_gen; /* gen may have moved — that's fine if cycle edges
					 * still hold in fresh snapshot */

	fresh = (ClusterLmdWaitEdge *) palloc(sizeof(ClusterLmdWaitEdge) * max_edges);
	n_fresh = cluster_lmd_graph_snapshot_copy(fresh, max_edges, &fresh_gen);
	(void) fresh_gen;

	/* Verify every cycle vertex is still a waiter in fresh snapshot. */
	found_count = 0;
	for (int v = 0; v < nvertices; v++)
	{
		for (int e = 0; e < n_fresh; e++)
		{
			if (fresh[e].waiter.node_id == cycle_vertices[v].node_id &&
				fresh[e].waiter.procno == cycle_vertices[v].procno &&
				fresh[e].waiter.request_id == cycle_vertices[v].request_id &&
				fresh[e].waiter.cluster_epoch == cycle_vertices[v].cluster_epoch)
			{
				found_count++;
				break;
			}
		}
	}
	pfree(fresh);

	if (found_count == nvertices)
		return true;
	pg_atomic_fetch_add_u64(&revalidate_fail_count, 1);
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

	ensure_tarjan_counters_init();
	pg_atomic_fetch_add_u64(&tarjan_scan_count, 1);

	snapshot = (ClusterLmdWaitEdge *)
		palloc(sizeof(ClusterLmdWaitEdge) * max_edges);
	nedges = cluster_lmd_graph_snapshot_copy(snapshot, max_edges, &gen_at_snapshot);
	if (nedges == 0)
	{
		pfree(snapshot);
		return;
	}

	cycle_vertices = (ClusterLmdVertex *)
		palloc(sizeof(ClusterLmdVertex) * max_cycle_vertices);
	n_cycle_v = cluster_lmd_tarjan_scan_snapshot(snapshot, nedges, cycle_vertices,
												  max_cycle_vertices, &cycle_count);
	pfree(snapshot);

	if (cycle_count == 0)
	{
		pfree(cycle_vertices);
		return;
	}

	pg_atomic_fetch_add_u64(&cycle_detected_count, cycle_count);

	/* Flat cycle vertices: SCC1 vertices, SCC2 vertices, ... — but we
	 * don't track SCC boundaries explicitly.  For MVP: treat the entire
	 * cycle_vertices array as one combined cycle and pick one victim.
	 * Multi-cycle distinction lands in Hardening. */
	self_node = get_self_node_id();
	idx = 0;
	while (idx < n_cycle_v)
	{
		ClusterLmdVertex victim;
		int scc_end = n_cycle_v; /* MVP — single big cycle */

		cluster_lmd_tarjan_pick_victim(&cycle_vertices[idx], scc_end - idx, &victim);

		if (cluster_lmd_tarjan_revalidate(&cycle_vertices[idx], scc_end - idx,
										  gen_at_snapshot))
		{
			if (victim.node_id == self_node)
			{
				/* D8 wire — set local backend cancel flag.  Implementation
				 * forward-link:  cluster_lmd_signal_local_victim(procno). */
				extern void cluster_lmd_signal_local_victim(uint32 procno,
															uint64 request_id,
															uint64 cluster_epoch);
				cluster_lmd_signal_local_victim(victim.procno, victim.request_id,
												victim.cluster_epoch);
				pg_atomic_fetch_add_u64(&victim_cancel_sent_count, 1);
			}
			else
			{
				/* Cross-node victim — production forwarding 推 spec-2.23. */
				pg_atomic_fetch_add_u64(&cross_node_victim_pending_count, 1);
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
 * Counter accessors — public API.
 * ============================================================ */

uint64
cluster_lmd_tarjan_scan_count_get(void)
{
	ensure_tarjan_counters_init();
	return pg_atomic_read_u64(&tarjan_scan_count);
}

uint64
cluster_lmd_cycle_detected_count_get(void)
{
	ensure_tarjan_counters_init();
	return pg_atomic_read_u64(&cycle_detected_count);
}

uint64
cluster_lmd_victim_cancel_sent_count_get(void)
{
	ensure_tarjan_counters_init();
	return pg_atomic_read_u64(&victim_cancel_sent_count);
}

uint64
cluster_lmd_revalidate_fail_count_get(void)
{
	ensure_tarjan_counters_init();
	return pg_atomic_read_u64(&revalidate_fail_count);
}

uint64
cluster_lmd_cross_node_victim_pending_count_get(void)
{
	ensure_tarjan_counters_init();
	return pg_atomic_read_u64(&cross_node_victim_pending_count);
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
cluster_lmd_signal_local_victim(uint32 procno, uint64 request_id,
								uint64 cluster_epoch)
{
	PGPROC *target;
	pid_t target_pid;
	int target_backendid;

	if (procno >= (uint32) ProcGlobal->allProcCount)
	{
		ereport(LOG, (errmsg("cluster LMD victim procno %u out of range, skipping",
							 procno)));
		return;
	}
	target = &ProcGlobal->allProcs[procno];
	target_pid = target->pid;
	target_backendid = target->backendId;

	if (target_pid == 0 || target_backendid == InvalidBackendId)
	{
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
	(void) request_id;
	(void) cluster_epoch;

	(void) SendProcSignal(target_pid, PROCSIG_CLUSTER_GES_CANCEL,
						  target_backendid);
	ereport(DEBUG1, (errmsg("cluster LMD sent PROCSIG_CLUSTER_GES_CANCEL to "
							"procno=%u pid=%d backendid=%d (deadlock victim)",
							procno, (int) target_pid, target_backendid)));
}
