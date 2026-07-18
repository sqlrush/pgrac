/*-------------------------------------------------------------------------
 *
 * cluster_lmd_graph.c
 *	  pgrac LMD wait-for graph — hash table of wait edges + atomic
 *	  generation + snapshot copy under shard-style LWLock.
 *
 *	  spec-2.22 D4 / D5:本地 wait-for graph state lives in a dedicated
 *	  shmem region "pgrac cluster lmd graph" (separate from spec-2.19
 *	  ClusterLmdShmem daemon-state region, per L98 ownership).
 *
 *	  Vertex identity is the 4-tuple (node_id, procno, cluster_epoch,
 *	  request_id) per HC13.  The HTAB key uses the full identity; this is
 *	  required because procno + request_id can be reused after backend exit
 *	  across epochs.  Sort metadata (xid, local_start_ts_ms) is opaque to
 *	  the graph layer;Tarjan picks victims using sort metadata after
 *	  snapshot copy.
 *
 *	  Cap surface:cluster.lmd_max_wait_edges GUC (default 1024).
 *	  Overflow fail-closed per HC12 (P1.2) — submit returns false;
 *	  caller maps to 53R82 ERRCODE_CLUSTER_LMD_WAIT_EDGE_FULL.
 *
 *	  Generation:atomic uint64, bumped under shmem->lwlock on every
 *	  add/remove. Snapshot copy读 generation_at_snapshot 给 Tarjan +
 *	  revalidate fence (HC14 A3).
 *
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/backend/cluster/cluster_lmd_graph.c
 *
 * NOTES
 *	  This is a pgrac-original file.  Compiled only in --enable-cluster
 *	  builds.
 *	  Spec: spec-2.22-lmd-tarjan-cross-node-deadlock.md (FROZEN v0.3).
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/transam.h"
#include "cluster/cluster_conf.h" /* CLUSTER_MAX_NODES (D4 member admission) */
#include "cluster/cluster_guc.h"
#include "cluster/cluster_lmd.h"
#include "cluster/cluster_shmem.h"
#include "miscadmin.h"
#include "port/atomics.h"
#include "storage/ipc.h"
#include "storage/lwlock.h"
#include "storage/shmem.h"
#include "utils/hsearch.h"
#include "utils/palloc.h"


/* ============================================================
 * HTAB key — full vertex identity.  Sort metadata is deliberately excluded.
 * ============================================================ */

/*
 * spec-5.8 D1a — multi-edge key.  spec-2.22 keyed the graph on the waiter
 * 4-tuple alone, so a waiter blocked by several holders kept only the last
 * edge written (overwrite at HASH_FIND below).  Once master-side edge
 * authority (spec-5.8 D1b) registers a waiter against every conflicting
 * holder, that overwrite would drop the very edges a cross-node cycle is
 * built from.  The key is widened to (waiter 4-tuple, blocker 4-tuple) so
 * each (waiter, blocker) pair is a distinct entry, and remove-by-waiter
 * deletes every blocker edge of a waiter.  The Tarjan layer already builds
 * a multi-out-edge adjacency list (cluster_lmd_tarjan.c), so no consumer
 * change is required.
 */
typedef struct LmdVertexKey {
	int32 node_id;
	uint32 procno;
	uint64 cluster_epoch;
	uint64 request_id;
} LmdVertexKey;

StaticAssertDecl(sizeof(LmdVertexKey) == 24, "LmdVertexKey identity 4-tuple 24-byte lock");

typedef struct LmdEdgeKey {
	LmdVertexKey waiter;
	LmdVertexKey blocker;
} LmdEdgeKey;

StaticAssertDecl(sizeof(LmdEdgeKey) == 48,
				 "LmdEdgeKey HTAB key ABI 48-byte lock (waiter+blocker 4-tuples, spec-5.8 D1a)");

typedef struct LmdEdgeEntry {
	LmdEdgeKey key; /* HTAB key — must be first field */
	ClusterLmdWaitEdge edge;
} LmdEdgeEntry;


/* ============================================================
 * Shared state — region "pgrac cluster lmd graph".
 * ============================================================ */

typedef struct ClusterLmdGraphShared {
	LWLock
		lwlock; /* LW_EXCLUSIVE for add/remove/snapshot;readers also exclusive (write generation)*/
	pg_atomic_uint64 generation; /* monotonic; bumped on add/remove */
	pg_atomic_uint64 edge_count; /* current edge count (cached) */
	pg_atomic_uint64 wait_edge_full_count;
	pg_atomic_uint64 inject_call_count;
	pg_atomic_uint64 tarjan_scan_count;
	pg_atomic_uint64 cycle_detected_count;
	pg_atomic_uint64 victim_cancel_sent_count;
	pg_atomic_uint64 revalidate_fail_count;
	pg_atomic_uint64 cross_node_victim_pending_count;
	/* spec-2.23 D8 counters — coordinator probe broadcast + partial REPORT. */
	pg_atomic_uint64 probe_broadcast_count;
	pg_atomic_uint64 probe_partial_count;
	/* spec-2.24 D12 counters — cross-node cancel forwarding + cleanup. */
	pg_atomic_uint64 cross_node_victim_cancel_sent_count;
	pg_atomic_uint64 cross_node_cancel_received_count;
	pg_atomic_uint64 cross_node_cancel_queue_full_count;
	pg_atomic_uint64 cleanup_on_backend_exit_count;
	pg_atomic_uint64 cleanup_lmd_sweep_count;
	pg_atomic_uint64 cleanup_skip_other_owner_count;
	/* spec-5.8 D6 counters — coordinator two-round confirm + reconfig gate. */
	pg_atomic_uint64 deadlock_confirmed_count;	/* two-round confirm succeeded */
	pg_atomic_uint64 confirm_unconfirmed_count; /* round-1 cycle NOT confirmed by round 2 */
	pg_atomic_uint64 reconfig_discard_count;	/* confirm spanned a reconfig (D4b) */
	/* spec-5.8 Hardening v1.0.1 — FC1 acting gate: a probe round's received
	 * member set was not the full expected (CSSD-alive) set, so the partial
	 * cross-node union was discarded before Tarjan (never confirm/cancel). */
	pg_atomic_uint64 member_incomplete_count;
	/* spec-5.9 D10 — victim policy + cancel robustness counters. */
	pg_atomic_uint64 victim_protected_skip_count;	   /* HARD-skip victim -> ACK(PROTECTED) */
	pg_atomic_uint64 victim_repeat_avoided_count;	   /* anti-thrash chose an alternate */
	pg_atomic_uint64 cancel_token_installed_count;	   /* per-proc cancel token installed */
	pg_atomic_uint64 cancel_consumed_count;			   /* backend matched + honored cancel */
	pg_atomic_uint64 cancel_stale_cleared_count;	   /* stale token cleared, no 40P01 */
	pg_atomic_uint64 cancel_wait_stale_rejected_count; /* CANCEL_WAIT wait_seq mismatch */
	pg_atomic_uint64 cancel_ack_received_count;		   /* CANCEL_ACK received by coordinator */
	pg_atomic_uint64 cancel_retransmit_count;		   /* bounded cancel retransmit */
	pg_atomic_uint64 cancel_escalated_alternate_count; /* escalated to alternate victim */
	pg_atomic_uint64 cancel_exhausted_timeout_count;   /* cancellable exhausted -> timeout */
	pg_atomic_uint64 cancel_no_safe_victim_count;	   /* all HARD-skip -> degrade */
	pg_atomic_uint64 cleanup_orphan_edge_swept_count;  /* orphan master-side edge GC'd */
	pg_atomic_uint64 reconfig_cancel_discarded_count;  /* in-flight cancel dropped on reconfig */
	/* PGRAC: spec-5.9 Hardening v1.0.1 (P1#1) — a CANCEL_ACK whose echoed victim
	 * identity + wait_seq did not match the pending-cancel entry the cancel_id
	 * resolved to (stale cancel_id after an LMD restart / misrouted ACK); dropped,
	 * never honored, so it cannot wrongly clear / escalate the live victim. */
	pg_atomic_uint64 cancel_ack_mismatch_count;
	int max_edges; /* snapshot of cluster.lmd_max_wait_edges at init */
	/* Appended connector accounting; existing field offsets stay unchanged. */
	pg_atomic_uint64 pcm_convert_wfg_replace_count;
	pg_atomic_uint64 pcm_convert_wfg_remove_count;
	pg_atomic_uint64 pcm_convert_wfg_replace_fail_count;
	pg_atomic_uint64 pcm_convert_wfg_exact_remove_stale_count;
} ClusterLmdGraphShared;

/* Pin the old tail and the appended connector counters byte-for-byte. */
StaticAssertDecl(offsetof(ClusterLmdGraphShared, max_edges) == 296,
				 "LMD graph max_edges offset ABI");
StaticAssertDecl(offsetof(ClusterLmdGraphShared, pcm_convert_wfg_replace_count) == 304,
				 "LMD graph PCM-X replace counter offset ABI");
StaticAssertDecl(offsetof(ClusterLmdGraphShared, pcm_convert_wfg_remove_count) == 312,
				 "LMD graph PCM-X remove counter offset ABI");
StaticAssertDecl(offsetof(ClusterLmdGraphShared, pcm_convert_wfg_replace_fail_count) == 320,
				 "LMD graph PCM-X replace-fail counter offset ABI");
StaticAssertDecl(offsetof(ClusterLmdGraphShared, pcm_convert_wfg_exact_remove_stale_count) == 328,
				 "LMD graph PCM-X exact-remove-stale counter offset ABI");
StaticAssertDecl(sizeof(ClusterLmdGraphShared) == 336, "LMD graph shared-memory ABI");

static ClusterLmdGraphShared *cluster_lmd_graph_state = NULL;
static HTAB *cluster_lmd_graph_htab = NULL;


/* ============================================================
 * Forward declarations.
 * ============================================================ */

static void fill_vertex_key(const ClusterLmdVertex *v, LmdVertexKey *out);
static void make_key(const ClusterLmdVertex *waiter, const ClusterLmdVertex *blocker,
					 LmdEdgeKey *out);
static bool key_waiter_matches(const LmdEdgeKey *key, const ClusterLmdVertex *waiter);
static bool vertex_identity_equal(const ClusterLmdVertex *a, const ClusterLmdVertex *b);
static int vertex_identity_qsort_cmp(const void *a, const void *b);
static bool vertex_metadata_equal(const ClusterLmdVertex *a, const ClusterLmdVertex *b);
static void restore_replaced_waiter_set(const ClusterLmdVertex *waiter,
										const ClusterLmdVertex *new_blockers, int inserted_count,
										const ClusterLmdWaitEdge *old_edges, int removed_count,
										uint64 edge_count_before, uint64 generation_before);

#define LMD_REMOVE_BATCH 32


/* ============================================================
 * Shmem region request / init / register.
 * ============================================================ */

Size
cluster_lmd_graph_shmem_size(void)
{
	Size sz = MAXALIGN(sizeof(ClusterLmdGraphShared));
	int max_edges = cluster_lmd_max_wait_edges;

	if (max_edges < 64)
		max_edges = 64;
	if (max_edges > 65536)
		max_edges = 65536;

	/* HTAB sizing per PG hash_estimate_size pattern. */
	sz = add_size(sz, hash_estimate_size((Size)max_edges, sizeof(LmdEdgeEntry)));
	return sz;
}

void
cluster_lmd_graph_shmem_init(void)
{
	bool found;
	HASHCTL hctl;
	int max_edges = cluster_lmd_max_wait_edges;

	if (max_edges < 64)
		max_edges = 64;
	if (max_edges > 65536)
		max_edges = 65536;

	cluster_lmd_graph_state = (ClusterLmdGraphShared *)ShmemInitStruct(
		"pgrac cluster lmd graph", MAXALIGN(sizeof(ClusterLmdGraphShared)), &found);

	if (!IsUnderPostmaster) {
		LWLockInitialize(&cluster_lmd_graph_state->lwlock, LWTRANCHE_CLUSTER_LMD_GRAPH);
		pg_atomic_init_u64(&cluster_lmd_graph_state->generation, 1);
		pg_atomic_init_u64(&cluster_lmd_graph_state->edge_count, 0);
		pg_atomic_init_u64(&cluster_lmd_graph_state->wait_edge_full_count, 0);
		pg_atomic_init_u64(&cluster_lmd_graph_state->inject_call_count, 0);
		pg_atomic_init_u64(&cluster_lmd_graph_state->tarjan_scan_count, 0);
		pg_atomic_init_u64(&cluster_lmd_graph_state->cycle_detected_count, 0);
		pg_atomic_init_u64(&cluster_lmd_graph_state->victim_cancel_sent_count, 0);
		pg_atomic_init_u64(&cluster_lmd_graph_state->revalidate_fail_count, 0);
		pg_atomic_init_u64(&cluster_lmd_graph_state->cross_node_victim_pending_count, 0);
		pg_atomic_init_u64(&cluster_lmd_graph_state->pcm_convert_wfg_replace_count, 0);
		pg_atomic_init_u64(&cluster_lmd_graph_state->pcm_convert_wfg_remove_count, 0);
		pg_atomic_init_u64(&cluster_lmd_graph_state->pcm_convert_wfg_replace_fail_count, 0);
		pg_atomic_init_u64(&cluster_lmd_graph_state->pcm_convert_wfg_exact_remove_stale_count, 0);
		pg_atomic_init_u64(&cluster_lmd_graph_state->probe_broadcast_count, 0);
		pg_atomic_init_u64(&cluster_lmd_graph_state->probe_partial_count, 0);
		/* spec-2.24 D12 init. */
		pg_atomic_init_u64(&cluster_lmd_graph_state->cross_node_victim_cancel_sent_count, 0);
		pg_atomic_init_u64(&cluster_lmd_graph_state->cross_node_cancel_received_count, 0);
		pg_atomic_init_u64(&cluster_lmd_graph_state->cross_node_cancel_queue_full_count, 0);
		pg_atomic_init_u64(&cluster_lmd_graph_state->cleanup_on_backend_exit_count, 0);
		pg_atomic_init_u64(&cluster_lmd_graph_state->cleanup_lmd_sweep_count, 0);
		pg_atomic_init_u64(&cluster_lmd_graph_state->cleanup_skip_other_owner_count, 0);
		/* spec-5.8 D6 init. */
		pg_atomic_init_u64(&cluster_lmd_graph_state->deadlock_confirmed_count, 0);
		pg_atomic_init_u64(&cluster_lmd_graph_state->confirm_unconfirmed_count, 0);
		pg_atomic_init_u64(&cluster_lmd_graph_state->reconfig_discard_count, 0);
		/* spec-5.8 Hardening v1.0.1 — FC1 acting gate init. */
		pg_atomic_init_u64(&cluster_lmd_graph_state->member_incomplete_count, 0);
		/* spec-5.9 D10 init. */
		pg_atomic_init_u64(&cluster_lmd_graph_state->victim_protected_skip_count, 0);
		pg_atomic_init_u64(&cluster_lmd_graph_state->victim_repeat_avoided_count, 0);
		pg_atomic_init_u64(&cluster_lmd_graph_state->cancel_token_installed_count, 0);
		pg_atomic_init_u64(&cluster_lmd_graph_state->cancel_consumed_count, 0);
		pg_atomic_init_u64(&cluster_lmd_graph_state->cancel_stale_cleared_count, 0);
		pg_atomic_init_u64(&cluster_lmd_graph_state->cancel_wait_stale_rejected_count, 0);
		pg_atomic_init_u64(&cluster_lmd_graph_state->cancel_ack_received_count, 0);
		pg_atomic_init_u64(&cluster_lmd_graph_state->cancel_retransmit_count, 0);
		pg_atomic_init_u64(&cluster_lmd_graph_state->cancel_escalated_alternate_count, 0);
		pg_atomic_init_u64(&cluster_lmd_graph_state->cancel_exhausted_timeout_count, 0);
		pg_atomic_init_u64(&cluster_lmd_graph_state->cancel_no_safe_victim_count, 0);
		pg_atomic_init_u64(&cluster_lmd_graph_state->cleanup_orphan_edge_swept_count, 0);
		pg_atomic_init_u64(&cluster_lmd_graph_state->reconfig_cancel_discarded_count, 0);
		pg_atomic_init_u64(&cluster_lmd_graph_state->cancel_ack_mismatch_count, 0);
		cluster_lmd_graph_state->max_edges = max_edges;
	}

	MemSet(&hctl, 0, sizeof(hctl));
	hctl.keysize = sizeof(LmdEdgeKey);
	hctl.entrysize = sizeof(LmdEdgeEntry);
	cluster_lmd_graph_htab = ShmemInitHash("pgrac cluster lmd graph htab", max_edges, max_edges,
										   &hctl, HASH_ELEM | HASH_BLOBS);
}


/* ============================================================
 * Mutator API.
 * ============================================================ */

static void
fill_vertex_key(const ClusterLmdVertex *v, LmdVertexKey *out)
{
	out->node_id = v->node_id;
	out->procno = v->procno;
	out->cluster_epoch = v->cluster_epoch;
	out->request_id = v->request_id;
}

static void
make_key(const ClusterLmdVertex *waiter, const ClusterLmdVertex *blocker, LmdEdgeKey *out)
{
	memset(out, 0, sizeof(*out)); /* clear padding for HTAB binary compare */
	fill_vertex_key(waiter, &out->waiter);
	fill_vertex_key(blocker, &out->blocker);
}

/* True iff the key's waiter half equals the given vertex's 4-tuple identity. */
static bool
key_waiter_matches(const LmdEdgeKey *key, const ClusterLmdVertex *waiter)
{
	return key->waiter.node_id == waiter->node_id && key->waiter.procno == waiter->procno
		   && key->waiter.cluster_epoch == waiter->cluster_epoch
		   && key->waiter.request_id == waiter->request_id;
}

static bool
vertex_identity_equal(const ClusterLmdVertex *a, const ClusterLmdVertex *b)
{
	return a->node_id == b->node_id && a->procno == b->procno
		   && a->cluster_epoch == b->cluster_epoch && a->request_id == b->request_id;
}

static int
vertex_identity_qsort_cmp(const void *a, const void *b)
{
	const ClusterLmdVertex *va = (const ClusterLmdVertex *)a;
	const ClusterLmdVertex *vb = (const ClusterLmdVertex *)b;

	if (va->node_id != vb->node_id)
		return va->node_id < vb->node_id ? -1 : 1;
	if (va->procno != vb->procno)
		return va->procno < vb->procno ? -1 : 1;
	if (va->cluster_epoch != vb->cluster_epoch)
		return va->cluster_epoch < vb->cluster_epoch ? -1 : 1;
	if (va->request_id != vb->request_id)
		return va->request_id < vb->request_id ? -1 : 1;
	return 0;
}

static bool
vertex_metadata_equal(const ClusterLmdVertex *a, const ClusterLmdVertex *b)
{
	return a->xid == b->xid && a->local_start_ts_ms == b->local_start_ts_ms
		   && a->wait_seq == b->wait_seq;
}

/*
 * Roll back a replacement that hit the dynahash's non-throwing insertion
 * guard after mutation began.  The caller holds the graph lock EXCLUSIVE.
 * Removing the inserted prefix recreates exactly removed_count free slots,
 * so restoring that many saved old edges cannot normally fail.  A failure in
 * that second invariant means the shared graph itself is corrupt; continuing
 * would let the deadlock detector act on a partial graph, so PANIC is the only
 * fail-closed outcome.
 */
static void
restore_replaced_waiter_set(const ClusterLmdVertex *waiter, const ClusterLmdVertex *new_blockers,
							int inserted_count, const ClusterLmdWaitEdge *old_edges,
							int removed_count, uint64 edge_count_before, uint64 generation_before)
{
	int i;

	for (i = 0; i < inserted_count; i++) {
		LmdEdgeKey key;
		bool found;

		make_key(waiter, &new_blockers[i], &key);
		(void)hash_search(cluster_lmd_graph_htab, &key, HASH_REMOVE, &found);
		if (!found)
			ereport(PANIC, (errmsg("could not roll back partial LMD blocker-set replacement")));
	}

	for (i = 0; i < removed_count; i++) {
		LmdEdgeKey key;
		LmdEdgeEntry *entry;
		bool found;

		make_key(&old_edges[i].waiter, &old_edges[i].blocker, &key);
		entry = (LmdEdgeEntry *)hash_search(cluster_lmd_graph_htab, &key, HASH_ENTER_NULL, &found);
		if (entry == NULL || found)
			ereport(PANIC, (errmsg("could not restore LMD blocker set after replacement failure")));
		entry->edge = old_edges[i];
	}

	/* No snapshot can have observed the intermediate values under EXCLUSIVE. */
	pg_atomic_write_u64(&cluster_lmd_graph_state->edge_count, edge_count_before);
	pg_atomic_write_u64(&cluster_lmd_graph_state->generation, generation_before);
}

bool
cluster_lmd_graph_add_edge(const ClusterLmdWaitEdge *edge)
{
	LmdEdgeKey key;
	LmdEdgeEntry *entry;
	bool found;
	uint64 cur_count;

	Assert(edge != NULL);
	if (cluster_lmd_graph_state == NULL || cluster_lmd_graph_htab == NULL)
		return false;

	/* Reject self-cycle (defensive — caller must check before).  See
	 * TAP 109 L3 scenario. */
	if (edge->waiter.node_id == edge->blocker.node_id && edge->waiter.procno == edge->blocker.procno
		&& edge->waiter.cluster_epoch == edge->blocker.cluster_epoch
		&& edge->waiter.request_id == edge->blocker.request_id)
		return false;

	make_key(&edge->waiter, &edge->blocker, &key);

	LWLockAcquire(&cluster_lmd_graph_state->lwlock, LW_EXCLUSIVE);

	entry = (LmdEdgeEntry *)hash_search(cluster_lmd_graph_htab, &key, HASH_FIND, NULL);
	if (entry != NULL) {
		entry->edge = *edge;
		entry->edge.graph_generation
			= pg_atomic_fetch_add_u64(&cluster_lmd_graph_state->generation, 1);
		LWLockRelease(&cluster_lmd_graph_state->lwlock);
		return true;
	}

	cur_count = pg_atomic_read_u64(&cluster_lmd_graph_state->edge_count);
	if (cur_count >= (uint64)cluster_lmd_graph_state->max_edges) {
		pg_atomic_fetch_add_u64(&cluster_lmd_graph_state->wait_edge_full_count, 1);
		LWLockRelease(&cluster_lmd_graph_state->lwlock);
		return false; /* HC12 fail-closed */
	}

	entry = (LmdEdgeEntry *)hash_search(cluster_lmd_graph_htab, &key, HASH_ENTER_NULL, &found);
	if (entry == NULL) {
		pg_atomic_fetch_add_u64(&cluster_lmd_graph_state->wait_edge_full_count, 1);
		LWLockRelease(&cluster_lmd_graph_state->lwlock);
		return false; /* HTAB exhausted */
	}
	if (!found)
		pg_atomic_fetch_add_u64(&cluster_lmd_graph_state->edge_count, 1);
	entry->edge = *edge;
	entry->edge.graph_generation = pg_atomic_fetch_add_u64(&cluster_lmd_graph_state->generation, 1);

	LWLockRelease(&cluster_lmd_graph_state->lwlock);
	return true;
}

/*
 * spec-2.36a C2 -- publish a waiter's complete blocker set atomically.
 *
 * The old cancel-then-submit loop exposes an empty or partial graph to a
 * concurrent Tarjan snapshot and cannot roll back a prefix when the graph is
 * full.  This operation holds the graph LWLock across capacity reservation,
 * removal, and insertion.  Capacity is reserved against the FINAL cardinality
 * before the first mutation, so a normal saturation failure leaves the prior
 * set and graph generation untouched.
 *
 * The dynahash is fixed-size and the exclusive lock prevents competing
 * inserts between the final-cardinality check and HASH_ENTER_NULL.  A
 * surprising insertion NULL is nevertheless handled: remove the inserted
 * prefix, restore the saved old set and its count/generation, then return
 * false.  If that restoration invariant itself fails, PANIC prevents any
 * detector from acting on a partial shared graph.  The exact entry point
 * returns the graph generation committed by this batch while still holding
 * the graph lock; zero means no batch was published.  The legacy boolean API
 * remains a compatibility wrapper.
 */
uint64
cluster_lmd_graph_replace_waiter_edges_exact(const ClusterLmdVertex *waiter,
											 const ClusterLmdVertex *blockers, int nblockers,
											 uint64 request_id)
{
	HASH_SEQ_STATUS scan;
	LmdEdgeEntry *e;
	ClusterLmdVertex *canonical = NULL;
	ClusterLmdWaitEdge *old_edges;
	uint64 cur_count;
	uint64 committed_generation;
	uint64 generation_before;
	uint64 survivor_count;
	int old_capacity;
	int old_count;
	int unique_count = 0;
	int i;

	if (waiter == NULL || nblockers < 0 || (nblockers > 0 && blockers == NULL)
		|| cluster_lmd_graph_state == NULL || cluster_lmd_graph_htab == NULL
		|| request_id != waiter->request_id || nblockers > cluster_lmd_graph_state->max_edges)
		return 0;

	/*
	 * Canonicalize outside the global graph lock.  Equal identities may be
	 * folded only when victim-selection/ABA metadata is also exact; silently
	 * choosing one of two wait_seq values could cancel a reused backend wait.
	 */
	if (nblockers > 0) {
		canonical = palloc_array(ClusterLmdVertex, nblockers);
		memcpy(canonical, blockers, sizeof(ClusterLmdVertex) * nblockers);
		qsort(canonical, nblockers, sizeof(ClusterLmdVertex), vertex_identity_qsort_cmp);

		for (i = 0; i < nblockers; i++) {
			if (vertex_identity_equal(waiter, &canonical[i])) {
				pfree(canonical);
				return 0;
			}
			if (unique_count > 0
				&& vertex_identity_equal(&canonical[unique_count - 1], &canonical[i])) {
				if (!vertex_metadata_equal(&canonical[unique_count - 1], &canonical[i])) {
					pfree(canonical);
					return 0;
				}
				continue;
			}
			canonical[unique_count++] = canonical[i];
		}
	}

	/*
	 * Save the old edge payloads both for O(old_count) deletion and for exact
	 * rollback if HASH_ENTER_NULL reports an invariant failure.  Size the
	 * first pass from the replacement, then grow outside the LWLock if a
	 * waiter currently has a larger set.
	 */
	old_capacity = Max(LMD_REMOVE_BATCH, unique_count);
	old_capacity = Min(old_capacity, cluster_lmd_graph_state->max_edges);
	old_edges = palloc_array(ClusterLmdWaitEdge, old_capacity);

	for (;;) {
		bool overflow = false;

		old_count = 0;
		LWLockAcquire(&cluster_lmd_graph_state->lwlock, LW_EXCLUSIVE);
		hash_seq_init(&scan, cluster_lmd_graph_htab);
		while ((e = (LmdEdgeEntry *)hash_seq_search(&scan)) != NULL) {
			if (!key_waiter_matches(&e->key, waiter))
				continue;
			if (old_count == old_capacity) {
				overflow = true;
				hash_seq_term(&scan);
				break;
			}
			old_edges[old_count++] = e->edge;
		}
		if (!overflow)
			break;

		LWLockRelease(&cluster_lmd_graph_state->lwlock);
		if (old_capacity == cluster_lmd_graph_state->max_edges) {
			pfree(old_edges);
			if (canonical != NULL)
				pfree(canonical);
			return 0;
		}
		old_capacity = Min(old_capacity * 2, cluster_lmd_graph_state->max_edges);
		old_edges = repalloc_array(old_edges, ClusterLmdWaitEdge, old_capacity);
	}

	cur_count = pg_atomic_read_u64(&cluster_lmd_graph_state->edge_count);
	Assert((uint64)old_count <= cur_count);
	if ((uint64)old_count > cur_count) {
		LWLockRelease(&cluster_lmd_graph_state->lwlock);
		pfree(old_edges);
		if (canonical != NULL)
			pfree(canonical);
		return 0;
	}
	survivor_count = cur_count - (uint64)old_count;
	if (survivor_count > (uint64)cluster_lmd_graph_state->max_edges
		|| (uint64)unique_count > (uint64)cluster_lmd_graph_state->max_edges - survivor_count) {
		pg_atomic_fetch_add_u64(&cluster_lmd_graph_state->wait_edge_full_count, 1);
		LWLockRelease(&cluster_lmd_graph_state->lwlock);
		pfree(old_edges);
		if (canonical != NULL)
			pfree(canonical);
		return 0;
	}
	generation_before = pg_atomic_read_u64(&cluster_lmd_graph_state->generation);

	/* Delete the saved exact set in O(old_count), still under EXCLUSIVE. */
	for (i = 0; i < old_count; i++) {
		LmdEdgeKey key;
		bool found;

		make_key(&old_edges[i].waiter, &old_edges[i].blocker, &key);
		(void)hash_search(cluster_lmd_graph_htab, &key, HASH_REMOVE, &found);
		if (!found) {
			restore_replaced_waiter_set(waiter, canonical, 0, old_edges, i, cur_count,
										generation_before);
			LWLockRelease(&cluster_lmd_graph_state->lwlock);
			pfree(old_edges);
			if (canonical != NULL)
				pfree(canonical);
			return 0;
		}
		pg_atomic_fetch_sub_u64(&cluster_lmd_graph_state->edge_count, 1);
		pg_atomic_fetch_add_u64(&cluster_lmd_graph_state->generation, 1);
	}

	/* Insert the canonical set.  A surprising NULL is rolled back exactly. */
	for (i = 0; i < unique_count; i++) {
		ClusterLmdWaitEdge edge;
		LmdEdgeKey key;
		LmdEdgeEntry *entry;
		bool found;

		memset(&edge, 0, sizeof(edge));
		edge.waiter = *waiter;
		edge.blocker = canonical[i];
		edge.request_id = request_id;
		make_key(waiter, &canonical[i], &key);
		entry = (LmdEdgeEntry *)hash_search(cluster_lmd_graph_htab, &key, HASH_ENTER_NULL, &found);
		if (entry == NULL) {
			pg_atomic_fetch_add_u64(&cluster_lmd_graph_state->wait_edge_full_count, 1);
			restore_replaced_waiter_set(waiter, canonical, i, old_edges, old_count, cur_count,
										generation_before);
			LWLockRelease(&cluster_lmd_graph_state->lwlock);
			pfree(old_edges);
			if (canonical != NULL)
				pfree(canonical);
			return 0;
		}
		if (found)
			ereport(PANIC, (errmsg("duplicate LMD edge appeared during atomic replacement")));
		entry->edge = edge;
		entry->edge.graph_generation
			= pg_atomic_fetch_add_u64(&cluster_lmd_graph_state->generation, 1);
		pg_atomic_fetch_add_u64(&cluster_lmd_graph_state->edge_count, 1);
	}

	committed_generation = pg_atomic_read_u64(&cluster_lmd_graph_state->generation);
	LWLockRelease(&cluster_lmd_graph_state->lwlock);
	pfree(old_edges);
	if (canonical != NULL)
		pfree(canonical);
	return committed_generation;
}

bool
cluster_lmd_graph_replace_waiter_edges(const ClusterLmdVertex *waiter,
									   const ClusterLmdVertex *blockers, int nblockers,
									   uint64 request_id)
{
	return cluster_lmd_graph_replace_waiter_edges_exact(waiter, blockers, nblockers, request_id)
		   != 0;
}

bool
cluster_lmd_graph_has_waiter(const ClusterLmdVertex *waiter)
{
	HASH_SEQ_STATUS scan;
	LmdEdgeEntry *e;
	bool found = false;

	Assert(waiter != NULL);
	if (cluster_lmd_graph_state == NULL || cluster_lmd_graph_htab == NULL)
		return false;

	/*
	 * spec-5.8 D1a — multi-edge: a waiter may have many blocker edges, and
	 * the key now includes the blocker, so a single keyed probe cannot
	 * answer "is this waiter present" without a blocker.  Scan for any edge
	 * whose waiter half matches.  Only the legacy spec-2.24 resolver uses
	 * this;  spec-5.8 D5 replaces it with per-proc wait-state revalidation.
	 */
	LWLockAcquire(&cluster_lmd_graph_state->lwlock, LW_SHARED);
	hash_seq_init(&scan, cluster_lmd_graph_htab);
	while ((e = (LmdEdgeEntry *)hash_seq_search(&scan)) != NULL) {
		if (key_waiter_matches(&e->key, waiter)) {
			found = true;
			hash_seq_term(&scan);
			break;
		}
	}
	LWLockRelease(&cluster_lmd_graph_state->lwlock);
	return found;
}

/*
 * spec-5.8 D1a — remove every blocker edge of a waiter.
 *
 *	Multi-edge keys include the blocker, so a single keyed HASH_REMOVE no
 *	longer removes all of a waiter's edges.  Collect the matching full keys
 *	in a bounded batch, then HASH_REMOVE each — the removes run after the
 *	scan ends, never deleting during an active hash_seq_search (dynahash
 *	only sanctions deleting the just-returned element, and a collect-then-
 *	remove is robust against either rule and against the standalone fake
 *	HTAB).  A waiter is blocked by at most the holders of one resource, so
 *	the batch normally drains in one pass;  the outer loop is defensive for
 *	the generic graph.  Held EXCLUSIVE across the whole operation so a
 *	waiter's edges are removed atomically.
 */
bool
cluster_lmd_graph_remove_edge_by_waiter(const ClusterLmdVertex *waiter)
{
	bool removed_any = false;

	Assert(waiter != NULL);
	if (cluster_lmd_graph_state == NULL || cluster_lmd_graph_htab == NULL)
		return false;

	LWLockAcquire(&cluster_lmd_graph_state->lwlock, LW_EXCLUSIVE);
	for (;;) {
		HASH_SEQ_STATUS scan;
		LmdEdgeEntry *e;
		LmdEdgeKey batch[LMD_REMOVE_BATCH];
		int nbatch = 0;
		bool more = false;
		int i;

		hash_seq_init(&scan, cluster_lmd_graph_htab);
		while ((e = (LmdEdgeEntry *)hash_seq_search(&scan)) != NULL) {
			if (!key_waiter_matches(&e->key, waiter))
				continue;
			if (nbatch == LMD_REMOVE_BATCH) {
				more = true; /* more matches remain for a later pass */
				hash_seq_term(&scan);
				break;
			}
			batch[nbatch++] = e->key;
		}

		for (i = 0; i < nbatch; i++) {
			bool found;

			(void)hash_search(cluster_lmd_graph_htab, &batch[i], HASH_REMOVE, &found);
			if (found) {
				pg_atomic_fetch_sub_u64(&cluster_lmd_graph_state->edge_count, 1);
				pg_atomic_fetch_add_u64(&cluster_lmd_graph_state->generation, 1);
				removed_any = true;
			}
		}

		if (!more)
			break;
	}
	LWLockRelease(&cluster_lmd_graph_state->lwlock);
	return removed_any;
}

/*
 * PCM-X cancellation additionally binds removal to the backend wait_seq.  The
 * graph key intentionally remains the canonical waiter 4-tuple, so first
 * prove that every matching edge still belongs to this exact wait instance;
 * a stale cancel must leave a newer instance byte-stable.  The exclusive lock
 * makes ABSENT a stable observation relative to graph writers, rather than a
 * false alias for STALE.
 */
ClusterLmdGraphRemoveResult
cluster_lmd_graph_remove_edge_by_waiter_exact_result(const ClusterLmdVertex *waiter)
{
	HASH_SEQ_STATUS proof_scan;
	LmdEdgeEntry *entry;
	bool matched_any = false;
	bool removed_any = false;

	Assert(waiter != NULL);
	if (waiter == NULL || waiter->wait_seq == 0)
		return CLUSTER_LMD_GRAPH_REMOVE_STALE;
	if (cluster_lmd_graph_state == NULL || cluster_lmd_graph_htab == NULL)
		return CLUSTER_LMD_GRAPH_REMOVE_STALE;

	LWLockAcquire(&cluster_lmd_graph_state->lwlock, LW_EXCLUSIVE);
	hash_seq_init(&proof_scan, cluster_lmd_graph_htab);
	while ((entry = (LmdEdgeEntry *)hash_seq_search(&proof_scan)) != NULL) {
		if (!key_waiter_matches(&entry->key, waiter))
			continue;
		matched_any = true;
		if (entry->edge.waiter.wait_seq != waiter->wait_seq) {
			hash_seq_term(&proof_scan);
			LWLockRelease(&cluster_lmd_graph_state->lwlock);
			return CLUSTER_LMD_GRAPH_REMOVE_STALE;
		}
	}
	if (!matched_any) {
		LWLockRelease(&cluster_lmd_graph_state->lwlock);
		return CLUSTER_LMD_GRAPH_REMOVE_ABSENT;
	}

	for (;;) {
		HASH_SEQ_STATUS scan;
		LmdEdgeEntry *candidate;
		LmdEdgeKey batch[LMD_REMOVE_BATCH];
		int nbatch = 0;
		bool more = false;
		int i;

		hash_seq_init(&scan, cluster_lmd_graph_htab);
		while ((candidate = (LmdEdgeEntry *)hash_seq_search(&scan)) != NULL) {
			if (!key_waiter_matches(&candidate->key, waiter))
				continue;
			if (nbatch == LMD_REMOVE_BATCH) {
				more = true;
				hash_seq_term(&scan);
				break;
			}
			batch[nbatch++] = candidate->key;
		}
		for (i = 0; i < nbatch; i++) {
			bool found;

			(void)hash_search(cluster_lmd_graph_htab, &batch[i], HASH_REMOVE, &found);
			if (found) {
				pg_atomic_fetch_sub_u64(&cluster_lmd_graph_state->edge_count, 1);
				pg_atomic_fetch_add_u64(&cluster_lmd_graph_state->generation, 1);
				removed_any = true;
			}
		}
		if (!more)
			break;
	}
	LWLockRelease(&cluster_lmd_graph_state->lwlock);
	return removed_any ? CLUSTER_LMD_GRAPH_REMOVE_REMOVED : CLUSTER_LMD_GRAPH_REMOVE_STALE;
}

bool
cluster_lmd_graph_remove_edge_by_waiter_exact(const ClusterLmdVertex *waiter)
{
	return cluster_lmd_graph_remove_edge_by_waiter_exact_result(waiter)
		   == CLUSTER_LMD_GRAPH_REMOVE_REMOVED;
}

int
cluster_lmd_graph_snapshot_copy(ClusterLmdWaitEdge *out_buf, int max_edges,
								uint64 *out_gen_at_snapshot)
{
	HASH_SEQ_STATUS scan;
	LmdEdgeEntry *e;
	int copied = 0;

	if (cluster_lmd_graph_state == NULL || cluster_lmd_graph_htab == NULL) {
		if (out_gen_at_snapshot)
			*out_gen_at_snapshot = 0;
		return 0;
	}

	LWLockAcquire(&cluster_lmd_graph_state->lwlock, LW_SHARED);
	if (out_gen_at_snapshot)
		*out_gen_at_snapshot = pg_atomic_read_u64(&cluster_lmd_graph_state->generation);
	hash_seq_init(&scan, cluster_lmd_graph_htab);
	while ((e = (LmdEdgeEntry *)hash_seq_search(&scan)) != NULL) {
		if (copied >= max_edges) {
			hash_seq_term(&scan);
			break;
		}
		out_buf[copied++] = e->edge;
	}
	LWLockRelease(&cluster_lmd_graph_state->lwlock);
	return copied;
}


/* ============================================================
 * Accessors + counter helpers.
 * ============================================================ */

uint64
cluster_lmd_graph_generation_get(void)
{
	if (cluster_lmd_graph_state == NULL)
		return 0;
	return pg_atomic_read_u64(&cluster_lmd_graph_state->generation);
}

uint64
cluster_lmd_wait_edge_count_get(void)
{
	if (cluster_lmd_graph_state == NULL)
		return 0;
	return pg_atomic_read_u64(&cluster_lmd_graph_state->edge_count);
}

uint64
cluster_lmd_wait_edge_full_count_get(void)
{
	if (cluster_lmd_graph_state == NULL)
		return 0;
	return pg_atomic_read_u64(&cluster_lmd_graph_state->wait_edge_full_count);
}

uint64
cluster_lmd_pcm_convert_wfg_replace_count_get(void)
{
	if (cluster_lmd_graph_state == NULL)
		return 0;
	return pg_atomic_read_u64(&cluster_lmd_graph_state->pcm_convert_wfg_replace_count);
}

uint64
cluster_lmd_pcm_convert_wfg_remove_count_get(void)
{
	if (cluster_lmd_graph_state == NULL)
		return 0;
	return pg_atomic_read_u64(&cluster_lmd_graph_state->pcm_convert_wfg_remove_count);
}

uint64
cluster_lmd_pcm_convert_wfg_replace_fail_count_get(void)
{
	if (cluster_lmd_graph_state == NULL)
		return 0;
	return pg_atomic_read_u64(&cluster_lmd_graph_state->pcm_convert_wfg_replace_fail_count);
}

uint64
cluster_lmd_pcm_convert_wfg_exact_remove_stale_count_get(void)
{
	if (cluster_lmd_graph_state == NULL)
		return 0;
	return pg_atomic_read_u64(&cluster_lmd_graph_state->pcm_convert_wfg_exact_remove_stale_count);
}

void
cluster_lmd_pcm_convert_wfg_note_replace(void)
{
	if (cluster_lmd_graph_state != NULL)
		(void)pg_atomic_fetch_add_u64(&cluster_lmd_graph_state->pcm_convert_wfg_replace_count, 1);
}

void
cluster_lmd_pcm_convert_wfg_note_remove(void)
{
	if (cluster_lmd_graph_state != NULL)
		(void)pg_atomic_fetch_add_u64(&cluster_lmd_graph_state->pcm_convert_wfg_remove_count, 1);
}

void
cluster_lmd_pcm_convert_wfg_note_replace_fail(void)
{
	if (cluster_lmd_graph_state != NULL)
		(void)pg_atomic_fetch_add_u64(&cluster_lmd_graph_state->pcm_convert_wfg_replace_fail_count,
									  1);
}

void
cluster_lmd_pcm_convert_wfg_note_exact_remove_stale(void)
{
	if (cluster_lmd_graph_state != NULL)
		(void)pg_atomic_fetch_add_u64(
			&cluster_lmd_graph_state->pcm_convert_wfg_exact_remove_stale_count, 1);
}

uint64
cluster_lmd_inject_call_count_get(void)
{
	if (cluster_lmd_graph_state == NULL)
		return 0;
	return pg_atomic_read_u64(&cluster_lmd_graph_state->inject_call_count);
}

void
cluster_lmd_inject_call_count_inc(void)
{
	if (cluster_lmd_graph_state != NULL)
		pg_atomic_fetch_add_u64(&cluster_lmd_graph_state->inject_call_count, 1);
}

uint64
cluster_lmd_tarjan_scan_count_get(void)
{
	if (cluster_lmd_graph_state == NULL)
		return 0;
	return pg_atomic_read_u64(&cluster_lmd_graph_state->tarjan_scan_count);
}

uint64
cluster_lmd_cycle_detected_count_get(void)
{
	if (cluster_lmd_graph_state == NULL)
		return 0;
	return pg_atomic_read_u64(&cluster_lmd_graph_state->cycle_detected_count);
}

uint64
cluster_lmd_victim_cancel_sent_count_get(void)
{
	if (cluster_lmd_graph_state == NULL)
		return 0;
	return pg_atomic_read_u64(&cluster_lmd_graph_state->victim_cancel_sent_count);
}

uint64
cluster_lmd_revalidate_fail_count_get(void)
{
	if (cluster_lmd_graph_state == NULL)
		return 0;
	return pg_atomic_read_u64(&cluster_lmd_graph_state->revalidate_fail_count);
}

uint64
cluster_lmd_cross_node_victim_pending_count_get(void)
{
	if (cluster_lmd_graph_state == NULL)
		return 0;
	return pg_atomic_read_u64(&cluster_lmd_graph_state->cross_node_victim_pending_count);
}

void
cluster_lmd_tarjan_scan_count_inc(uint64 delta)
{
	if (cluster_lmd_graph_state != NULL)
		pg_atomic_fetch_add_u64(&cluster_lmd_graph_state->tarjan_scan_count, delta);
}

void
cluster_lmd_cycle_detected_count_inc(uint64 delta)
{
	if (cluster_lmd_graph_state != NULL)
		pg_atomic_fetch_add_u64(&cluster_lmd_graph_state->cycle_detected_count, delta);
}

void
cluster_lmd_victim_cancel_sent_count_inc(uint64 delta)
{
	if (cluster_lmd_graph_state != NULL)
		pg_atomic_fetch_add_u64(&cluster_lmd_graph_state->victim_cancel_sent_count, delta);
}

void
cluster_lmd_revalidate_fail_count_inc(uint64 delta)
{
	if (cluster_lmd_graph_state != NULL)
		pg_atomic_fetch_add_u64(&cluster_lmd_graph_state->revalidate_fail_count, delta);
}

void
cluster_lmd_cross_node_victim_pending_count_inc(uint64 delta)
{
	if (cluster_lmd_graph_state != NULL)
		pg_atomic_fetch_add_u64(&cluster_lmd_graph_state->cross_node_victim_pending_count, delta);
}

uint64
cluster_lmd_probe_broadcast_count_get(void)
{
	if (cluster_lmd_graph_state == NULL)
		return 0;
	return pg_atomic_read_u64(&cluster_lmd_graph_state->probe_broadcast_count);
}

uint64
cluster_lmd_probe_partial_count_get(void)
{
	if (cluster_lmd_graph_state == NULL)
		return 0;
	return pg_atomic_read_u64(&cluster_lmd_graph_state->probe_partial_count);
}

void
cluster_lmd_probe_broadcast_count_inc(uint64 delta)
{
	if (cluster_lmd_graph_state != NULL)
		pg_atomic_fetch_add_u64(&cluster_lmd_graph_state->probe_broadcast_count, delta);
}

void
cluster_lmd_probe_partial_count_inc(uint64 delta)
{
	if (cluster_lmd_graph_state != NULL)
		pg_atomic_fetch_add_u64(&cluster_lmd_graph_state->probe_partial_count, delta);
}

/* spec-2.24 D12 — 6 NEW counter accessors. */
#define DEFINE_GET_INC(field)                                                                      \
	uint64 cluster_lmd_##field##_get(void)                                                         \
	{                                                                                              \
		if (cluster_lmd_graph_state == NULL)                                                       \
			return 0;                                                                              \
		return pg_atomic_read_u64(&cluster_lmd_graph_state->field);                                \
	}                                                                                              \
	void cluster_lmd_##field##_inc(uint64 delta)                                                   \
	{                                                                                              \
		if (cluster_lmd_graph_state != NULL)                                                       \
			pg_atomic_fetch_add_u64(&cluster_lmd_graph_state->field, delta);                       \
	}

DEFINE_GET_INC(cross_node_victim_cancel_sent_count)
DEFINE_GET_INC(cross_node_cancel_received_count)
DEFINE_GET_INC(cross_node_cancel_queue_full_count)
DEFINE_GET_INC(cleanup_on_backend_exit_count)
DEFINE_GET_INC(cleanup_lmd_sweep_count)
DEFINE_GET_INC(cleanup_skip_other_owner_count)
/* spec-5.8 D6. */
DEFINE_GET_INC(deadlock_confirmed_count)
DEFINE_GET_INC(confirm_unconfirmed_count)
DEFINE_GET_INC(reconfig_discard_count)
/* spec-5.8 Hardening v1.0.1 — FC1 acting gate. */
DEFINE_GET_INC(member_incomplete_count)
/* spec-5.9 D10 — victim policy + cancel robustness. */
DEFINE_GET_INC(victim_protected_skip_count)
DEFINE_GET_INC(victim_repeat_avoided_count)
DEFINE_GET_INC(cancel_token_installed_count)
DEFINE_GET_INC(cancel_consumed_count)
DEFINE_GET_INC(cancel_stale_cleared_count)
DEFINE_GET_INC(cancel_wait_stale_rejected_count)
DEFINE_GET_INC(cancel_ack_received_count)
DEFINE_GET_INC(cancel_retransmit_count)
DEFINE_GET_INC(cancel_escalated_alternate_count)
DEFINE_GET_INC(cancel_exhausted_timeout_count)
DEFINE_GET_INC(cancel_no_safe_victim_count)
DEFINE_GET_INC(cleanup_orphan_edge_swept_count)
DEFINE_GET_INC(reconfig_cancel_discarded_count)
DEFINE_GET_INC(cancel_ack_mismatch_count)

#undef DEFINE_GET_INC


/* ============================================================
 * submit/cancel real wire — top-level entry from S4/S5/S7.
 * ============================================================ */

bool
cluster_lmd_submit_wait_edge_real(const ClusterLmdVertex *waiter, const ClusterLmdVertex *blocker,
								  uint64 request_id)
{
	ClusterLmdWaitEdge edge;

	Assert(waiter != NULL && blocker != NULL);

	memset(&edge, 0, sizeof(edge));
	edge.waiter = *waiter;
	edge.blocker = *blocker;
	edge.request_id = request_id;
	/* graph_generation 在 add_edge 内 set */

	return cluster_lmd_graph_add_edge(&edge);
}

void
cluster_lmd_cancel_wait_edge_real(const ClusterLmdVertex *waiter)
{
	if (waiter == NULL)
		return;
	(void)cluster_lmd_graph_remove_edge_by_waiter(waiter);
}

/*
 * spec-5.8 D2 (T2) — resolve cross-node TX holder placeholders in place.
 *
 *	A cross-node TX (row-lock) wait edge is registered knowing the holder
 *	only by its transaction identity (origin node + xid, taken from the TT
 *	status key);  the holder backend's procno is never on the wire, so the
 *	blocker vertex is stamped with CLUSTER_LMD_TX_HOLDER_PROCNO and the
 *	holder's xid.  The coordinator union holds every node's waiter vertices,
 *	each carrying its real identity 4-tuple and its (node, xid).  This pass
 *	rewrites each placeholder blocker to the full identity of the waiter
 *	vertex that owns the same (node, valid xid) — i.e. the holder transaction
 *	is itself a waiter elsewhere — so the dedup in scan_snapshot unifies the
 *	blocker with that waiter and Tarjan can close the cross-node cycle.
 *
 *	Rule 8.A — never fabricate an edge:
 *	  - A placeholder is rewritten ONLY on an exact (node match + equal valid
 *	    xid) against a real waiter vertex (procno != sentinel).  xid is unique
 *	    only within a node, so the node must match;  an Invalid xid is never a
 *	    match (TransactionIdEquals would otherwise treat Invalid == Invalid).
 *	  - An unmatched placeholder is left untouched.  It is a pure sink — only
 *	    ever a blocker, never a waiter — so it has no out-edge and can never
 *	    sit on a cycle.  find_vertex_index ignores xid, so two unresolved
 *	    placeholders on the same (node, epoch) may merge into one sink, but a
 *	    sink cannot create a false cycle;  and the sentinel procno can never
 *	    equal a real backend procno (< MaxBackends), so a placeholder never
 *	    merges with a real vertex.
 *	A missed resolution only loses a detection (a finite timeout backstops);
 *	it never invents one.
 */
void
cluster_lmd_resolve_tx_placeholders(ClusterLmdWaitEdge *edges, int nedges)
{
	int e;

	if (edges == NULL || nedges <= 0)
		return;

	for (e = 0; e < nedges; e++) {
		ClusterLmdVertex *blocker = &edges[e].blocker;
		int w;

		if (blocker->procno != CLUSTER_LMD_TX_HOLDER_PROCNO)
			continue; /* not a TX placeholder — a real GES/master edge */
		if (!TransactionIdIsValid(blocker->xid))
			continue; /* no holder xid to resolve against (8.A) */

		/* Find the real waiter vertex that IS this holder transaction. */
		for (w = 0; w < nedges; w++) {
			const ClusterLmdVertex *cand = &edges[w].waiter;

			if (cand->procno == CLUSTER_LMD_TX_HOLDER_PROCNO)
				continue; /* a placeholder is never a real waiter */
			if (cand->node_id == blocker->node_id && TransactionIdEquals(cand->xid, blocker->xid)) {
				/* Rewrite to the full waiter identity so the vertex dedup in
				 * scan_snapshot unifies this blocker with that waiter. */
				*blocker = *cand;
				break;
			}
		}
	}
}

/*
 * spec-5.8 D4 — coordinator REPORT-collector member admission (pure).
 *
 *	Returns ADMIT only when node_id is in the expected (probed) set AND has not
 *	already reported (received bit clear).  An out-of-range id or an id outside
 *	the expected set is DROP_UNEXPECTED;  an already-received id is
 *	DROP_DUPLICATE.  Bitmaps are uint64 lo/hi pairs over node_id 0..127.  This
 *	is the Rule 8.A guard against a duplicate REPORT inflating n_received and
 *	masking a genuinely missing node.
 */
ClusterLmdProbeAdmit
cluster_lmd_probe_member_admit(uint64 expected_lo, uint64 expected_hi, uint64 received_lo,
							   uint64 received_hi, int32 node_id)
{
	uint64 bit;
	uint64 expected_word;
	uint64 received_word;

	if (node_id < 0 || node_id >= CLUSTER_MAX_NODES)
		return CLUSTER_LMD_PROBE_DROP_UNEXPECTED;

	if (node_id < 64) {
		bit = UINT64CONST(1) << node_id;
		expected_word = expected_lo;
		received_word = received_lo;
	} else {
		bit = UINT64CONST(1) << (node_id - 64);
		expected_word = expected_hi;
		received_word = received_hi;
	}

	if (!(expected_word & bit))
		return CLUSTER_LMD_PROBE_DROP_UNEXPECTED;
	if (received_word & bit)
		return CLUSTER_LMD_PROBE_DROP_DUPLICATE;
	return CLUSTER_LMD_PROBE_ADMIT;
}

/*
 * spec-5.8 Hardening v1.0.1 — FC1 probe-round completeness (pure).
 *
 *	A coordinator probe round is COMPLETE iff every expected responder (the
 *	CSSD-alive peers snapshotted when the probe was armed) actually reported.
 *	Returns true only on an EXACT member-set match (§2.4 D4a / §3.5 FC1).
 *
 *	cluster_lmd_probe_member_admit already keeps received ⊆ expected (it drops
 *	reports from unexpected nodes and de-dups duplicates), so equality here is
 *	equivalent to "received ⊇ expected", i.e. nobody expected is missing.  A
 *	received ⊊ expected round is PARTIAL: the cross-node union is missing a
 *	peer's edges, so a cycle derived from it could cancel a victim whose true
 *	blocker lives on a node we never heard from — the two-round driver MUST
 *	discard such a round (never confirm / cancel) and re-collect next scan.
 *	This errs only toward NOT cancelling (a real deadlock is re-detected once
 *	the member set is complete), never toward a false victim (Rule 8.A).
 */
bool
cluster_lmd_probe_round_complete(uint64 expected_lo, uint64 expected_hi, uint64 received_lo,
								 uint64 received_hi)
{
	return expected_lo == received_lo && expected_hi == received_hi;
}

/* D16 — test-only injection. */
bool
cluster_lmd_inject_wait_edge(const ClusterLmdVertex *waiter, const ClusterLmdVertex *blocker)
{
	bool ok;

	Assert(waiter != NULL && blocker != NULL);
	cluster_lmd_inject_call_count_inc();
	ok = cluster_lmd_submit_wait_edge_real(waiter, blocker, waiter->request_id);
	return ok;
}
