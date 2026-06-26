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

#include "cluster/cluster_conf.h"		  /* CLUSTER_MAX_NODES + active peers */
#include "cluster/cluster_epoch.h"		  /* cluster_epoch_get_current */
#include "cluster/cluster_ges.h"		  /* GesDeadlockProbePayload / Report */
#include "cluster/cluster_grd.h"		  /* spec-2.24 ClusterGrdHolderId */
#include "cluster/cluster_grd_outbound.h" /* cluster_grd_outbound_enqueue_backend_request */
#include "cluster/cluster_guc.h"
#include "cluster/cluster_lmd.h"
#include "cluster/cluster_lmd_probe_collector.h"
#include "cluster/cluster_lmd_wait_state.h"
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

/*
 * Find a cycle-graph vertex index, returning -1 if not found.
 *
 *	spec-5.8 D8 — a WFG cycle vertex is a BACKEND, identified by
 *	(node_id, procno, cluster_epoch).  request_id / wait_seq are deliberately
 *	NOT part of the cycle identity: a backend that HOLDS one resource (granted
 *	under request_id rA) and WAITS for another (request_id rB) must be the SAME
 *	graph vertex, or a real hold-and-wait deadlock splits into disjoint edges
 *	(rA-holder vs rB-waiter) that Tarjan can never close.  request_id / wait_seq
 *	/ xid ride on the vertex as victim-targeting metadata (the WAIT identity,
 *	via waiter-identity-wins in the dedup pass) — the D5 resolver still cancels
 *	the exact (request_id, cluster_epoch, wait_seq) the victim is waiting on.
 */
static int
find_vertex_index(const ClusterLmdVertex *list, int nvertices, const ClusterLmdVertex *target)
{
	for (int i = 0; i < nvertices; i++) {
		if (list[i].node_id == target->node_id && list[i].procno == target->procno
			&& list[i].cluster_epoch == target->cluster_epoch)
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

	/*
	 * spec-5.8 D2 (T2) — resolve cross-node TX holder placeholders on a
	 * private mutable copy (the input is const) before deduping vertices, so a
	 * holder placeholder unifies with the real waiter vertex it names and
	 * Tarjan can close the cross-node cycle.  Done here so every caller (local
	 * scan, revalidate, coordinator union) gets it.
	 */
	{
		ClusterLmdWaitEdge *resolved;

		resolved = (ClusterLmdWaitEdge *)palloc(sizeof(ClusterLmdWaitEdge) * nedges);
		memcpy(resolved, edges, sizeof(ClusterLmdWaitEdge) * nedges);
		cluster_lmd_resolve_tx_placeholders(resolved, nedges);
		edges = resolved;
	}

	/* Max vertices = 2 * nedges (waiter + blocker per edge). */
	vertices = (ClusterLmdVertex *)palloc(sizeof(ClusterLmdVertex) * nedges * 2);
	adj_head = (int *)palloc(sizeof(int) * nedges * 2);
	adj_next = (int *)palloc(sizeof(int) * nedges);
	adj_to = (int *)palloc(sizeof(int) * nedges);
	for (int i = 0; i < nedges * 2; i++)
		adj_head[i] = -1;

	/*
	 * Pass 1: dedup waiters + blockers into vertices[] (by backend identity —
	 * node/procno/epoch) + build adjacency.
	 *
	 *	spec-5.8 D8 waiter-identity-wins:  the waiter half of an edge carries
	 *	the victim-targeting metadata that matters (the WAIT's request_id +
	 *	wait_seq + xid), so a waiter occurrence OVERWRITES the stored vertex's
	 *	metadata, while a blocker occurrence only fills a not-yet-seen backend
	 *	(its hold metadata is a placeholder until that backend shows up as a
	 *	waiter).  Every vertex in a real cycle has an out-edge — i.e. it is a
	 *	waiter — so the chosen victim always ends up carrying its WAIT identity,
	 *	and the D5 resolver cancels the exact request it is waiting on (8.A).
	 */
	for (int e = 0; e < nedges; e++) {
		int wi = find_vertex_index(vertices, nvertices, &edges[e].waiter);
		int bi;

		if (wi < 0) {
			vertices[nvertices] = edges[e].waiter;
			wi = nvertices++;
		} else {
			/* Waiter-identity-wins: overwrite holder-origin metadata with the
			 * WAIT identity (same node/procno/epoch, but the request the victim
			 * is actually blocked on). */
			vertices[wi] = edges[e].waiter;
		}
		bi = find_vertex_index(vertices, nvertices, &edges[e].blocker);
		if (bi < 0) {
			/* First sighting of this backend is as a holder — keep its hold
			 * metadata only until/unless it also appears as a waiter above. */
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
				/* D5 — signal the local victim; signal_local_victim revalidates
				 * it against the live per-proc wait-state and returns true only
				 * if it actually sent the cancel.  Count only real cancels. */
				if (cluster_lmd_signal_local_victim(victim.procno, victim.request_id,
													victim.cluster_epoch, victim.wait_seq))
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
 * spec-2.23 D8 / spec-5.8 D8 — cross-node DEADLOCK_PROBE coordinator scan.
 *
 *	The REPORT collector itself now lives in shared memory
 *	(cluster_lmd_probe_collector.c): inbound REPORTs are received and
 *	appended by LMON, while the coordinator scan below runs in LMD, so the
 *	rendezvous must be cross-process (a process-local collector would let
 *	LMON write a copy LMD never sees).  This file keeps only the scan
 *	orchestration: arm the shmem collector, broadcast, poll, drain, Tarjan.
 *
 *	probe_id_seq is the LMD-local monotonic round counter (only the single
 *	coordinator scan thread mints probe ids, so no shared counter needed).
 * ============================================================ */

static uint64 probe_id_seq;

/*
 * spec-5.8 D3 — one coordinator probe round (NEVER cancels).
 *
 *	A round broadcasts a PROBE to every peer, collects REPORTs up to the
 *	partial-OK deadline, unions the remote edges with the coordinator's own
 *	local wait-for snapshot, resolves cross-node TX holder placeholders (D2 in
 *	scan_snapshot), and runs Tarjan.  It only reports whether a cycle exists,
 *	the chosen victim, and a fingerprint of the observation — the participating
 *	member set plus an order-independent hash of the cycle's vertex identities.
 *	The two-round driver cancels only when two rounds, separated by
 *	deadlock_confirm_interval_ms, produce the SAME fingerprint (Rule 8.A
 *	transient-edge filter).
 */
typedef struct LmdCoordRound {
	bool has_cycle;
	ClusterLmdVertex victim; /* valid iff has_cycle */
	/* Fingerprint (transient filter): same participants + same cycle. */
	uint64 member_lo;  /* bitmap of participating node_id 0..63 */
	uint64 member_hi;  /* bitmap of participating node_id 64..127 */
	uint64 cycle_hash; /* order-independent hash of cycle vertex identities */
	int member_count;  /* popcount(member) — observability */
} LmdCoordRound;

/*
 * Order-independent FNV-1a hash of a wait-for vertex over its backend-level
 * IDENTITY (node_id, procno, cluster_epoch) — it MUST match find_vertex_index.
 *
 *	spec-5.8: a backend is exactly one WFG cycle vertex; request_id and
 *	wait_seq are victim-targeting metadata (consumed by the D5 per-proc
 *	revalidate at the victim's home node), NOT cycle identity, so they are
 *	excluded here just as they are in find_vertex_index.  Including request_id
 *	would make the two-round cycle_hash differ whenever a waiter retransmits
 *	with a fresh request_id inside the confirm window — so a genuinely
 *	persistent cross-node deadlock would be misjudged "transient" and never
 *	confirm (it would only ever resolve via the GES request-timeout backstop).
 */
static uint64
lmd_vertex_identity_hash(const ClusterLmdVertex *v)
{
	uint64 h = UINT64CONST(1469598103934665603);

	h = (h ^ (uint64)(uint32)v->node_id) * UINT64CONST(1099511628211);
	h = (h ^ (uint64)v->procno) * UINT64CONST(1099511628211);
	h = (h ^ v->cluster_epoch) * UINT64CONST(1099511628211);
	return h;
}

/*
 * Two rounds confirm a deadlock iff both found a cycle over the SAME member
 * set with the SAME cycle hash.
 *
 *	Per-node graph generation is deliberately NOT part of the equality: it is
 *	bumped by every wait-edge add/remove on a node, so unrelated cross-node
 *	lock churn within the confirm window would make a literal "generation-
 *	equal" gate never match on a busy cluster — i.e. it would never confirm a
 *	real deadlock.  Member-set + cycle-hash equality is the sound transient
 *	filter (a cycle that persists identically across the confirm interval is a
 *	real deadlock), and the chosen victim is still gated by the D5 per-proc
 *	wait-state revalidate at its home node before any cancel fires.
 */
static bool
lmd_coord_round_confirms(const LmdCoordRound *a, const LmdCoordRound *b)
{
	return a->has_cycle && b->has_cycle && a->member_lo == b->member_lo
		   && a->member_hi == b->member_hi && a->cycle_hash == b->cycle_hash;
}

/* Mark node_id as a participant in this round's fingerprint. */
static void
lmd_coord_member_set(LmdCoordRound *out, int32 node_id)
{
	if (node_id < 0 || node_id >= CLUSTER_MAX_NODES)
		return;
	if (node_id < 64) {
		if (!(out->member_lo & (UINT64CONST(1) << node_id)))
			out->member_count++;
		out->member_lo |= (UINT64CONST(1) << node_id);
	} else {
		int bit = node_id - 64;

		if (!(out->member_hi & (UINT64CONST(1) << bit)))
			out->member_count++;
		out->member_hi |= (UINT64CONST(1) << bit);
	}
}

/*
 * Run one probe round and fill *out.  NEVER cancels (the two-round driver
 * decides).  Resets the probe collector on every exit path.
 */
static void
lmd_coordinator_probe_round(int32 self_node, const int32 *peers, int n_peers,
							int collect_timeout_ms, LmdCoordRound *out)
{
	GesDeadlockProbePayload probe;
	TimestampTz now, deadline;
	uint64 probe_id;
	uint64 expected_lo = 0, expected_hi = 0;
	int max_local_edges = cluster_lmd_max_wait_edges;
	ClusterLmdWaitEdge *union_edges = NULL;
	int n_union = 0;
	ClusterLmdWaitEdge *local_snapshot = NULL;
	int local_n = 0;
	uint64 local_gen = 0;
	ClusterLmdProbeDrain drain;
	int remote_n;
	ClusterLmdVertex *cycle_vertices = NULL;
	int cycle_count = 0;
	int n_cycle_v = 0;

	memset(out, 0, sizeof(*out));

	if (max_local_edges < 64)
		max_local_edges = 64;
	if (max_local_edges > 65536)
		max_local_edges = 65536;

	/* (1) Build the expected-responder bitmap + arm the shmem collector
	 * (D4 — only these nodes' REPORTs are admitted; LMON appends into shmem). */
	for (int i = 0; i < n_peers; i++) {
		int32 p = peers[i];

		if (p < 0 || p >= CLUSTER_MAX_NODES)
			continue;
		if (p < 64)
			expected_lo |= (UINT64CONST(1) << p);
		else
			expected_hi |= (UINT64CONST(1) << (p - 64));
	}
	probe_id = ++probe_id_seq;
	cluster_lmd_probe_arm(probe_id, expected_lo, expected_hi);

	/* (2) Broadcast PROBE to each peer. */
	memset(&probe, 0, sizeof(probe));
	probe.opcode = GES_REQ_OPCODE_DEADLOCK_PROBE;
	probe.coordinator_node_id = (uint32)self_node;
	probe.probe_id = probe_id;
	probe.generation_snapshot = 0;
	for (int i = 0; i < n_peers; i++)
		(void)cluster_grd_outbound_enqueue_backend_request((uint32)peers[i], &probe, sizeof(probe));

	/* (3) Poll-wait for REPORTs (received in LMON, appended into the shmem
	 * collector) up to the partial-OK deadline (HC8). */
	now = GetCurrentTimestamp();
	deadline = TimestampTzPlusMilliseconds(now, collect_timeout_ms);
	while (cluster_lmd_probe_n_received() < n_peers) {
		now = GetCurrentTimestamp();
		if (now >= deadline) {
			cluster_lmd_probe_partial_count_inc(1);
			break;
		}
		CHECK_FOR_INTERRUPTS();
		(void)WaitLatch(MyLatch, WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH, 50,
						WAIT_EVENT_CLUSTER_LMD_PROBE_COLLECT);
		ResetLatch(MyLatch);
	}
	cluster_lmd_probe_broadcast_count_inc(1);

	/* (4) Union the coordinator's own local snapshot + the drained remote
	 * edges.  The shmem ring is capped at max_local_edges, so a buffer of
	 * 2 * max_local_edges always holds local + remote. */
	union_edges
		= (ClusterLmdWaitEdge *)palloc(sizeof(ClusterLmdWaitEdge) * (Size)max_local_edges * 2);

	local_snapshot = (ClusterLmdWaitEdge *)palloc(sizeof(ClusterLmdWaitEdge) * max_local_edges);
	local_n = cluster_lmd_graph_snapshot_copy(local_snapshot, max_local_edges, &local_gen);
	if (local_n > 0) {
		memcpy(union_edges, local_snapshot, sizeof(ClusterLmdWaitEdge) * local_n);
		n_union = local_n;
	}
	pfree(local_snapshot);
	/* The coordinator's own node always participates (its local snapshot). */
	lmd_coord_member_set(out, self_node);

	remote_n = cluster_lmd_probe_drain(union_edges + n_union, max_local_edges, &drain);
	n_union += remote_n;

	/*
	 * An overflowed round is INCOMPLETE — a REPORT's edges did not fit, so the
	 * union is a partial graph.  Never derive a cycle from a partial graph
	 * (Rule 8.A constraints 2 + 5): reset + return no-cycle so the two-round
	 * driver cannot confirm / cancel from it.  A later round re-collects.
	 */
	if (drain.overflow) {
		pfree(union_edges);
		cluster_lmd_probe_reset();
		return;
	}

	/* Record the responders into the round fingerprint member set. */
	for (int32 nid = 0; nid < CLUSTER_MAX_NODES; nid++) {
		uint64 bit = (nid < 64) ? (drain.member_lo & (UINT64CONST(1) << nid))
								: (drain.member_hi & (UINT64CONST(1) << (nid - 64)));

		if (bit != 0)
			lmd_coord_member_set(out, nid);
	}

	if (n_union == 0) {
		pfree(union_edges);
		cluster_lmd_probe_reset();
		return;
	}

	/* (5) Tarjan over the union; scan_snapshot resolves TX placeholders (D2). */
	cycle_vertices = (ClusterLmdVertex *)palloc(sizeof(ClusterLmdVertex) * n_union * 2);
	n_cycle_v = cluster_lmd_tarjan_scan_snapshot(union_edges, n_union, cycle_vertices, n_union * 2,
												 &cycle_count);

	if (cycle_count > 0) {
		out->has_cycle = true;
		cluster_lmd_tarjan_pick_victim(cycle_vertices, n_cycle_v, &out->victim);
		for (int i = 0; i < n_cycle_v; i++)
			out->cycle_hash += lmd_vertex_identity_hash(&cycle_vertices[i]);
	}

	pfree(cycle_vertices);
	pfree(union_edges);
	cluster_lmd_probe_reset();
}

/*
 * spec-5.8 D3 — coordinator two-round cross-node deadlock scan.
 *
 *	Called from LmdMain ONLY on the elected coordinator (HC16 lowest-active
 *	node — see the LmdMain coordinator tick).  Runs one probe round; if it
 *	finds a cycle it records the candidate, waits deadlock_confirm_interval_ms,
 *	then runs a second round.  A victim is cancelled ONLY when both rounds
 *	report the same fingerprint (Rule 8.A — never cancel on a single
 *	observation: a transient cross-node wait edge that vanishes between rounds
 *	is not a deadlock).  The victim is then revalidated against its live
 *	per-proc wait-state at its home node (D5) before the cancel fires.
 */
void
cluster_lmd_tarjan_run_coordinator_scan(int collect_timeout_ms)
{
	int32 self_node = get_self_node_id();
	int n_peers = 0;
	int32 peers[CLUSTER_MAX_NODES];
	LmdCoordRound round1;
	LmdCoordRound round2;
	int confirm_ms;
	ClusterLmdVertex victim;
	uint64 scan_epoch;

	if (collect_timeout_ms <= 0)
		collect_timeout_ms = cluster_lmd_probe_collect_timeout_ms;

	/* spec-5.8 D4b — capture the reconfig epoch at scan start.  If a reconfig
	 * (node join / leave / fence) lands during the detection window, the
	 * cross-node graph snapshot spans two memberships and must not drive a
	 * cancel — we re-check this before cancelling and discard on change. */
	scan_epoch = cluster_epoch_get_current();

	/* Build the peer list (skip self).  cluster_conf doesn't expose per-index
	 * node lookup, so iterate node_id range 0..total-1 (spec-2.x convention). */
	{
		int total = cluster_conf_node_count();

		for (int i = 0; i < total && n_peers < CLUSTER_MAX_NODES; i++) {
			if (i == self_node)
				continue;
			peers[n_peers++] = i;
		}
	}
	if (n_peers == 0)
		return; /* Single-node mode — no cross-node deadlock possible. */

	/* Round 1 — candidate only; NEVER cancel on a single observation (8.A). */
	lmd_coordinator_probe_round(self_node, peers, n_peers, collect_timeout_ms, &round1);
	if (!round1.has_cycle)
		return;
	cluster_lmd_cycle_detected_count_inc(1);

	/*
	 * Confirm delay — a responsive WaitLatch loop (not a blocking sleep) so the
	 * coordinator keeps draining REPORT receives + interrupts between rounds.
	 */
	confirm_ms = cluster_lmd_deadlock_confirm_interval_ms;
	if (confirm_ms > 0) {
		TimestampTz deadline = TimestampTzPlusMilliseconds(GetCurrentTimestamp(), confirm_ms);

		for (;;) {
			TimestampTz now = GetCurrentTimestamp();
			long wait_ms;

			if (now >= deadline)
				break;
			CHECK_FOR_INTERRUPTS();
			wait_ms = (long)((deadline - now) / 1000);
			if (wait_ms <= 0)
				wait_ms = 1;
			if (wait_ms > 50)
				wait_ms = 50;
			(void)WaitLatch(MyLatch, WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH, wait_ms,
							WAIT_EVENT_CLUSTER_LMD_PROBE_COLLECT);
			ResetLatch(MyLatch);
		}
	}

	/* Round 2 — confirm. */
	lmd_coordinator_probe_round(self_node, peers, n_peers, collect_timeout_ms, &round2);

	if (!lmd_coord_round_confirms(&round1, &round2)) {
		/* Transient or shifted cycle — do NOT cancel (Rule 8.A).  This is the
		 * value of the two-round gate; surface it (false positive avoided). */
		cluster_lmd_confirm_unconfirmed_count_inc(1);
		ereport(DEBUG1, (errmsg("cluster LMD cross-node deadlock candidate not confirmed across "
								"two rounds (transient); no victim cancelled")));
		return;
	}

	/*
	 * spec-5.8 D4b — reconfig fail-closed.  If membership changed at any point
	 * during the two-round detection window, the union spanned two memberships;
	 * discard the result rather than cancel a victim chosen against a stale
	 * topology (the next scan re-detects under the new epoch if the deadlock
	 * persists).
	 */
	if (cluster_epoch_get_current() != scan_epoch) {
		cluster_lmd_reconfig_discard_count_inc(1);
		ereport(DEBUG1, (errmsg("cluster LMD cross-node deadlock confirm spanned a reconfig "
								"(epoch " UINT64_FORMAT " -> " UINT64_FORMAT "); discarding, no "
								"victim cancelled",
								scan_epoch, cluster_epoch_get_current())));
		return;
	}

	/* Two rounds agreed under a stable epoch — a real cross-node deadlock. */
	cluster_lmd_deadlock_confirmed_count_inc(1);

	/*
	 * Confirmed: cancel the round-2 victim.  The actual signal is still gated
	 * by the D5 per-proc wait-state revalidate at the victim's home node
	 * (local: signal_local_victim; remote: the receiver's drain dispatch).
	 */
	victim = round2.victim;
	if (victim.node_id == self_node) {
		/* Count only a real cancel — D5 revalidate may refuse a victim that is
		 * no longer waiting (Rule 8.A). */
		if (cluster_lmd_signal_local_victim(victim.procno, victim.request_id, victim.cluster_epoch,
											victim.wait_seq))
			cluster_lmd_victim_cancel_sent_count_inc(1);
	} else {
		ClusterGrdHolderId victim_target;

		victim_target.node_id = (uint32)victim.node_id;
		victim_target.procno = victim.procno;
		victim_target.cluster_epoch = victim.cluster_epoch;
		victim_target.request_id = victim.request_id;

		/* spec-5.8 D1e — echo the victim's wait_seq for the remote node's D5
		 * ABA revalidate. */
		cluster_ges_send_cancel_pending(victim.node_id, &victim_target, victim.wait_seq);
		ereport(LOG, (errmsg("cluster LMD cross-node deadlock confirmed (victim node=%d procno=%u "
							 "request_id=" UINT64_FORMAT "); cancel forwarded via "
							 "CLUSTER_GRD_OUTBOUND_LMD_CANCEL",
							 victim.node_id, victim.procno, victim.request_id)));
	}
}


/*
 * spec-2.22 D8 / spec-5.8 D5 — signal local victim backend with
 * PROCSIG_CLUSTER_GES_CANCEL.
 *
 *	Resolve target PGPROC by procno;  revalidate against the victim's live
 *	per-proc wait-state (D1d) — cancel ONLY if it is still waiting on exactly
 *	this (request_id, cluster_epoch, wait_seq), where wait_seq is the
 *	monotonic ABA guard against procno reuse (spec-5.8 D5;  replaces the legacy
 *	spec-2.24 has_waiter edge probe).  If revalidation passes, SendProcSignal
 *	with PROCSIG_CLUSTER_GES_CANCEL slot (spec-2.17 ship).  Handler sets
 *	sig_atomic_t cluster_ges_cancel_pending;
 *	the receiving backend observes it in the seven-step dispatch loop
 *	(per HC10 + spec-2.21 D7 wire) and returns FAIL_DEADLOCK without
 *	ereport-in-handler (L118 inherit).
 */
bool
cluster_lmd_signal_local_victim(uint32 procno, uint64 request_id, uint64 cluster_epoch,
								uint64 wait_seq)
{
	PGPROC *target;
	pid_t target_pid;
	int target_backendid;
	ClusterLmdWaitStateSnapshot ws;

	if (cluster_epoch != cluster_epoch_get_current()) {
		cluster_lmd_revalidate_fail_count_inc(1);
		ereport(LOG, (errmsg("cluster LMD victim procno=%u request_id=" UINT64_FORMAT
							 " stale epoch " UINT64_FORMAT ", skipping",
							 procno, request_id, cluster_epoch)));
		return false;
	}

	if (procno >= (uint32)ProcGlobal->allProcCount) {
		ereport(LOG, (errmsg("cluster LMD victim procno %u out of range, skipping", procno)));
		return false;
	}
	target = &ProcGlobal->allProcs[procno];

	/*
	 * spec-5.8 D5 — revalidate against the victim's live per-proc wait-state
	 * (D1d) before cancelling.  The coordinator's two-round confirm proves the
	 * cross-node cycle was stable, but between the confirm and this signal the
	 * victim may have been granted / cancelled / timed out and entered a NEW
	 * wait that reused the same procno slot.  Cancel ONLY if the backend is
	 * still waiting on exactly this (request_id, cluster_epoch, wait_seq);
	 * wait_seq is the monotonic ABA guard against procno reuse.  This replaces
	 * the legacy spec-2.24 has_waiter() edge probe (Rule 8.A — never cancel a
	 * backend that has already moved on).
	 */
	if (!cluster_lmd_wait_state_read(&target->cluster_lmd_wait, &ws) || !ws.active
		|| ws.request_id != request_id || ws.cluster_epoch != cluster_epoch
		|| ws.wait_seq != wait_seq) {
		cluster_lmd_revalidate_fail_count_inc(1);
		ereport(LOG, (errmsg("cluster LMD victim procno=%u request_id=" UINT64_FORMAT
							 " no longer waiting on the same request (D5 revalidate), skipping",
							 procno, request_id)));
		return false;
	}

	target_pid = target->pid;
	target_backendid = target->backendId;

	if (target_pid == 0 || target_backendid == InvalidBackendId) {
		ereport(LOG, (errmsg("cluster LMD victim procno=%u has no live backend "
							 "(stale procno or exit race); skipping",
							 procno)));
		return false;
	}

	(void)SendProcSignal(target_pid, PROCSIG_CLUSTER_GES_CANCEL, target_backendid);
	ereport(DEBUG1, (errmsg("cluster LMD sent PROCSIG_CLUSTER_GES_CANCEL to "
							"procno=%u pid=%d backendid=%d (deadlock victim)",
							procno, (int)target_pid, target_backendid)));
	return true;
}
