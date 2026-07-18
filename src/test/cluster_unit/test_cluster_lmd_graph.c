/*-------------------------------------------------------------------------
 *
 * test_cluster_lmd_graph.c
 *	  Standalone unit tests for the LMD wait-for graph multi-edge schema
 *	  (spec-5.8 D1a).
 *
 *	  spec-2.22 shipped a waiter-keyed graph: the HTAB key was the waiter
 *	  4-tuple only, so a waiter blocked by multiple holders collapsed to a
 *	  single edge (the last write won).  That is correct for a single
 *	  resource master but loses the very edges a real deadlock cycle needs
 *	  once master-side edge authority (spec-5.8 D1b) registers a waiter
 *	  against every conflicting holder.
 *
 *	  spec-5.8 D1a widens the key to (waiter 4-tuple, blocker 4-tuple) so
 *	  the same waiter against N distinct blockers keeps N edges, and
 *	  remove-by-waiter deletes all of them.  These tests pin that contract:
 *
 *	    U1a: same waiter + 3 distinct blockers -> 3 edges survive (no
 *	         overwrite); every edge carries that waiter; the blocker set is
 *	         exactly {B1,B2,B3}.
 *	    U1b: remove_edge_by_waiter(W) deletes every blocker edge of W and
 *	         leaves the count at zero / has_waiter false.
 *	    U1c: edges of distinct waiters are independent — removing W1 does
 *	         not disturb W2's edges.
 *	    U1d: re-submitting the same (waiter, blocker) is idempotent (one
 *	         edge, generation advances) rather than a duplicate row.
 *
 *	  spec-5.8 D2 (T2) — cluster_lmd_resolve_tx_placeholders pins the
 *	  cross-node TX holder-placeholder resolution (Rule 8.A):
 *	    U2a: a cross-node TX cycle expressed with holder placeholders
 *	         resolves — each placeholder blocker becomes the real waiter
 *	         vertex that owns the same (node, xid), closing the cycle.
 *	    U2b: a placeholder whose xid matches no waiter is left untouched
 *	         (an unresolved sink, never a false edge).
 *	    U2c: a placeholder with an Invalid xid is never resolved, even
 *	         against an Invalid-xid waiter on the same node.
 *	    U2d: resolution requires a node match — a shared xid on a
 *	         different node must not bind.
 *	    U2e: a real (non-placeholder) blocker is never rewritten.
 *
 *	  Harness:
 *	    The graph layer uses a dynahash HTAB; the standalone build links a
 *	    faithful fake HTAB that compares infoP->keysize bytes (captured in
 *	    ShmemInitHash).  Because the fake keys off whatever width the graph
 *	    code declares, this same file fails RED against the 24-byte
 *	    waiter-only key and passes GREEN against the 48-byte multi-edge key
 *	    with no test change.  The real dynahash path is exercised end to
 *	    end by the spec-5.8 TAP suite.
 *
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/test/cluster_unit/test_cluster_lmd_graph.c
 *
 * NOTES
 *	  This is a pgrac-original file.  Standalone binary linking
 *	  cluster_lmd_graph.o only;all PG backend symbols stubbed locally.
 *	  Spec: spec-5.8-full-cross-node-deadlock-detector.md.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <string.h>

#include "access/transam.h"
#include "cluster/cluster_lmd.h"
#include "port/atomics.h"
#include "storage/lwlock.h"
#include "storage/shmem.h"
#include "utils/hsearch.h"

/* Drop PG's port.h printf override; unit_test.h uses stdlib printf. */
#ifdef vprintf
#undef vprintf
#endif
#ifdef printf
#undef printf
#endif
#ifdef fprintf
#undef fprintf
#endif

#include "unit_test.h"

UT_DEFINE_GLOBALS();


/* ============================================================
 * PG runtime globals + stubs the graph object references.
 * ============================================================ */

bool IsUnderPostmaster = false;

void *
palloc(Size size)
{
	void *ptr = malloc(size);

	if (ptr == NULL)
		abort();
	return ptr;
}

void *
repalloc(void *pointer, Size size)
{
	void *ptr = realloc(pointer, size);

	if (ptr == NULL)
		abort();
	return ptr;
}

void
pfree(void *pointer)
{
	free(pointer);
}

bool
errstart(int elevel pg_attribute_unused(), const char *domain pg_attribute_unused())
{
	return true;
}

bool
errstart_cold(int elevel pg_attribute_unused(), const char *domain pg_attribute_unused())
{
	return true;
}

int
errmsg(const char *fmt pg_attribute_unused(), ...)
{
	return 0;
}

void
errfinish(const char *filename pg_attribute_unused(), int lineno pg_attribute_unused(),
		  const char *funcname pg_attribute_unused())
{
	abort();
}

/*
 * cluster_lmd_graph.c reads cluster.lmd_max_wait_edges (cluster_guc.c GUC).
 * The standalone harness defines it directly to avoid pulling cluster_guc.o.
 */
int cluster_lmd_max_wait_edges = 1024;

void
ExceptionalCondition(const char *conditionName pg_attribute_unused(),
					 const char *fileName pg_attribute_unused(),
					 int lineNumber pg_attribute_unused())
{
	abort();
}

Size
add_size(Size s1, Size s2)
{
	return s1 + s2;
}

Size
hash_estimate_size(long num_entries pg_attribute_unused(), Size entrysize pg_attribute_unused())
{
	if (num_entries <= 0 || entrysize == 0)
		return 0;
	return (Size)num_entries * entrysize + 1024;
}

/* LWLock stubs — lock state is not exercised in the standalone harness. */
void
LWLockInitialize(LWLock *l pg_attribute_unused(), int tranche_id pg_attribute_unused())
{}

bool
LWLockAcquire(LWLock *lock pg_attribute_unused(), LWLockMode mode pg_attribute_unused())
{
	return true;
}

void
LWLockRelease(LWLock *lock pg_attribute_unused())
{}


/* ============================================================
 * Fake HTAB — faithful enough to exercise multi-edge key width.
 *
 *	ShmemInitHash captures infoP->keysize so HASH_FIND / HASH_REMOVE
 *	compare exactly the bytes the graph layer declares as its key.  With
 *	the 24-byte waiter-only key, (W,B1) and (W,B2) compare equal and
 *	collapse;  with the 48-byte (waiter,blocker) key they stay distinct.
 *
 *	HASH_REMOVE uses swap-with-last, so callers must finish any
 *	hash_seq_search before removing (the graph's remove-by-waiter collects
 *	keys first, then removes — never deletes during an active scan).
 * ============================================================ */

#define FAKE_LMD_HTAB_MAX_ENTRIES 128
#define FAKE_LMD_HTAB_ENTRY_BYTES 256

static int fake_htab_token;
static int fake_htab_count;
static int fake_htab_seq_index;
static int fake_htab_seq_init_count;
static int fake_hash_enter_null_fail_after = -1;
static Size fake_htab_entrysize;
static Size fake_htab_keysize;
static union {
	uint64 force_align;
	char data[FAKE_LMD_HTAB_MAX_ENTRIES][FAKE_LMD_HTAB_ENTRY_BYTES];
} fake_htab;

static union {
	uint64 force_align;
	char data[16 * 1024];
} stub_graph_buf;
static bool stub_graph_initialized = false;

void *
ShmemInitStruct(const char *name, Size size, bool *foundPtr)
{
	if (name != NULL && strcmp(name, "pgrac cluster lmd graph") == 0) {
		Assert(size <= sizeof(stub_graph_buf.data));
		*foundPtr = stub_graph_initialized;
		stub_graph_initialized = true;
		return stub_graph_buf.data;
	}

	*foundPtr = true;
	return NULL;
}

HTAB *
ShmemInitHash(const char *name pg_attribute_unused(), long init_size pg_attribute_unused(),
			  long max_size pg_attribute_unused(), HASHCTL *infoP,
			  int hash_flags pg_attribute_unused())
{
	Assert(infoP != NULL);
	Assert(infoP->entrysize <= FAKE_LMD_HTAB_ENTRY_BYTES);

	fake_htab_entrysize = infoP->entrysize;
	fake_htab_keysize = infoP->keysize;
	fake_htab_count = 0;
	fake_htab_seq_index = 0;
	fake_htab_seq_init_count = 0;
	fake_hash_enter_null_fail_after = -1;
	memset(&fake_htab, 0, sizeof(fake_htab));
	return (HTAB *)&fake_htab_token;
}

void *
hash_search(HTAB *hashp pg_attribute_unused(), const void *keyPtr, HASHACTION action,
			bool *foundPtr)
{
	int i;

	Assert(keyPtr != NULL);
	Assert(fake_htab_keysize > 0);

	for (i = 0; i < fake_htab_count; i++) {
		char *entry = fake_htab.data[i];

		if (memcmp(entry, keyPtr, fake_htab_keysize) == 0) {
			if (action == HASH_REMOVE) {
				if (foundPtr != NULL)
					*foundPtr = true;
				if (i < fake_htab_count - 1)
					memcpy(fake_htab.data[i], fake_htab.data[fake_htab_count - 1],
						   fake_htab_entrysize);
				memset(fake_htab.data[fake_htab_count - 1], 0, fake_htab_entrysize);
				fake_htab_count--;
				return entry;
			}
			if (foundPtr != NULL)
				*foundPtr = true;
			return entry;
		}
	}

	if (foundPtr != NULL)
		*foundPtr = false;

	if (action == HASH_FIND || action == HASH_REMOVE)
		return NULL;

	if (action == HASH_ENTER_NULL || action == HASH_ENTER) {
		char *entry;

		if (action == HASH_ENTER_NULL && fake_hash_enter_null_fail_after >= 0) {
			if (fake_hash_enter_null_fail_after-- == 0) {
				fake_hash_enter_null_fail_after = -1;
				return NULL;
			}
		}
		if (fake_htab_count >= FAKE_LMD_HTAB_MAX_ENTRIES)
			return NULL;

		entry = fake_htab.data[fake_htab_count++];
		memset(entry, 0, fake_htab_entrysize);
		memcpy(entry, keyPtr, fake_htab_keysize);
		return entry;
	}

	return NULL;
}

void
hash_seq_init(HASH_SEQ_STATUS *status pg_attribute_unused(), HTAB *hashp pg_attribute_unused())
{
	fake_htab_seq_index = 0;
	fake_htab_seq_init_count++;
}

void *
hash_seq_search(HASH_SEQ_STATUS *status pg_attribute_unused())
{
	if (fake_htab_seq_index >= fake_htab_count)
		return NULL;
	return fake_htab.data[fake_htab_seq_index++];
}

void
hash_seq_term(HASH_SEQ_STATUS *status pg_attribute_unused())
{}


/* ============================================================
 * Test helpers.
 * ============================================================ */

static void
reset_graph(void)
{
	stub_graph_initialized = false;
	memset(&stub_graph_buf, 0, sizeof(stub_graph_buf));
	cluster_lmd_graph_shmem_init();
}

static ClusterLmdVertex
mkvertex(int32 node_id, uint32 procno, uint64 epoch, uint64 request_id)
{
	ClusterLmdVertex v;

	memset(&v, 0, sizeof(v));
	v.node_id = node_id;
	v.procno = procno;
	v.cluster_epoch = epoch;
	v.request_id = request_id;
	v.xid = InvalidTransactionId;
	v.local_start_ts_ms = 0;
	return v;
}

static bool
vertex_eq(const ClusterLmdVertex *a, const ClusterLmdVertex *b)
{
	return a->node_id == b->node_id && a->procno == b->procno
		   && a->cluster_epoch == b->cluster_epoch && a->request_id == b->request_id;
}

/* Count snapshot edges whose (waiter, blocker) match the given pair. */
static int
count_edges(const ClusterLmdWaitEdge *edges, int n, const ClusterLmdVertex *waiter,
			const ClusterLmdVertex *blocker)
{
	int hits = 0;

	for (int i = 0; i < n; i++) {
		if (vertex_eq(&edges[i].waiter, waiter) && vertex_eq(&edges[i].blocker, blocker))
			hits++;
	}
	return hits;
}

/* mkvertex variant that stamps an explicit xid (resolution keys on it). */
static ClusterLmdVertex
mkvertex_xid(int32 node_id, uint32 procno, uint64 epoch, uint64 request_id, TransactionId xid)
{
	ClusterLmdVertex v = mkvertex(node_id, procno, epoch, request_id);

	v.xid = xid;
	return v;
}

static ClusterLmdWaitEdge
mkedge(ClusterLmdVertex waiter, ClusterLmdVertex blocker)
{
	ClusterLmdWaitEdge e;

	memset(&e, 0, sizeof(e));
	e.waiter = waiter;
	e.blocker = blocker;
	e.request_id = 0;
	return e;
}


/* ============================================================
 * U1a — same waiter + multiple distinct blockers must NOT overwrite.
 * ============================================================ */

UT_TEST(test_multi_blocker_edges_not_overwritten)
{
	ClusterLmdVertex w = mkvertex(1, 100, 7, 5000);
	ClusterLmdVertex b1 = mkvertex(1, 200, 7, 6000);
	ClusterLmdVertex b2 = mkvertex(2, 201, 7, 6001);
	ClusterLmdVertex b3 = mkvertex(3, 202, 7, 6002);
	ClusterLmdWaitEdge snap[32];
	uint64 gen;
	int n;

	reset_graph();

	UT_ASSERT(cluster_lmd_submit_wait_edge_real(&w, &b1, w.request_id));
	UT_ASSERT(cluster_lmd_submit_wait_edge_real(&w, &b2, w.request_id));
	UT_ASSERT(cluster_lmd_submit_wait_edge_real(&w, &b3, w.request_id));

	/* All three blocker edges must survive — not collapse to one. */
	UT_ASSERT_EQ((int)cluster_lmd_wait_edge_count_get(), 3);

	n = cluster_lmd_graph_snapshot_copy(snap, 32, &gen);
	UT_ASSERT_EQ(n, 3);
	UT_ASSERT_EQ(count_edges(snap, n, &w, &b1), 1);
	UT_ASSERT_EQ(count_edges(snap, n, &w, &b2), 1);
	UT_ASSERT_EQ(count_edges(snap, n, &w, &b3), 1);
}


/* ============================================================
 * U1b — remove_edge_by_waiter must delete ALL of a waiter's edges.
 * ============================================================ */

UT_TEST(test_remove_by_waiter_removes_all_blocker_edges)
{
	ClusterLmdVertex w = mkvertex(1, 100, 7, 5000);
	ClusterLmdVertex b1 = mkvertex(1, 200, 7, 6000);
	ClusterLmdVertex b2 = mkvertex(2, 201, 7, 6001);
	ClusterLmdVertex b3 = mkvertex(3, 202, 7, 6002);
	ClusterLmdWaitEdge snap[32];
	uint64 gen;

	reset_graph();

	UT_ASSERT(cluster_lmd_submit_wait_edge_real(&w, &b1, w.request_id));
	UT_ASSERT(cluster_lmd_submit_wait_edge_real(&w, &b2, w.request_id));
	UT_ASSERT(cluster_lmd_submit_wait_edge_real(&w, &b3, w.request_id));
	UT_ASSERT_EQ((int)cluster_lmd_wait_edge_count_get(), 3);
	UT_ASSERT(cluster_lmd_graph_has_waiter(&w));

	UT_ASSERT(cluster_lmd_graph_remove_edge_by_waiter(&w));

	UT_ASSERT_EQ((int)cluster_lmd_wait_edge_count_get(), 0);
	UT_ASSERT(!cluster_lmd_graph_has_waiter(&w));
	UT_ASSERT_EQ(cluster_lmd_graph_snapshot_copy(snap, 32, &gen), 0);
}

UT_TEST(test_exact_remove_requires_wait_seq_and_preserves_new_wait_instance)
{
	ClusterLmdVertex waiter = mkvertex(1, 100, 7, 5000);
	ClusterLmdVertex stale = waiter;
	ClusterLmdVertex blocker = mkvertex(2, 200, 7, 6000);

	reset_graph();
	waiter.wait_seq = UINT64_C(81);
	stale.wait_seq = UINT64_C(80);
	UT_ASSERT(cluster_lmd_submit_wait_edge_real(&waiter, &blocker, waiter.request_id));
	UT_ASSERT(!cluster_lmd_graph_remove_edge_by_waiter_exact(&stale));
	UT_ASSERT(cluster_lmd_graph_has_waiter(&waiter));
	UT_ASSERT_EQ((int)cluster_lmd_wait_edge_count_get(), 1);
	UT_ASSERT(cluster_lmd_graph_remove_edge_by_waiter_exact(&waiter));
	UT_ASSERT(!cluster_lmd_graph_has_waiter(&waiter));
	UT_ASSERT_EQ((int)cluster_lmd_wait_edge_count_get(), 0);
}

UT_TEST(test_exact_remove_result_distinguishes_absent_stale_and_removed)
{
	ClusterLmdVertex waiter = mkvertex(1, 100, 7, 5000);
	ClusterLmdVertex stale = waiter;
	ClusterLmdVertex blocker = mkvertex(2, 200, 7, 6000);
	uint64 generation;

	reset_graph();
	waiter.wait_seq = UINT64_C(81);
	stale.wait_seq = UINT64_C(80);
	generation = cluster_lmd_graph_generation_get();

	UT_ASSERT_EQ(cluster_lmd_graph_remove_edge_by_waiter_exact_result(&waiter),
				 CLUSTER_LMD_GRAPH_REMOVE_ABSENT);
	UT_ASSERT_EQ(cluster_lmd_graph_generation_get(), generation);

	UT_ASSERT(cluster_lmd_submit_wait_edge_real(&waiter, &blocker, waiter.request_id));
	generation = cluster_lmd_graph_generation_get();
	UT_ASSERT_EQ(cluster_lmd_graph_remove_edge_by_waiter_exact_result(&stale),
				 CLUSTER_LMD_GRAPH_REMOVE_STALE);
	UT_ASSERT_EQ(cluster_lmd_graph_generation_get(), generation);
	UT_ASSERT_EQ((int)cluster_lmd_wait_edge_count_get(), 1);

	UT_ASSERT_EQ(cluster_lmd_graph_remove_edge_by_waiter_exact_result(&waiter),
				 CLUSTER_LMD_GRAPH_REMOVE_REMOVED);
	UT_ASSERT_EQ(cluster_lmd_graph_generation_get(), generation + 1);
	UT_ASSERT_EQ((int)cluster_lmd_wait_edge_count_get(), 0);
	UT_ASSERT_EQ(cluster_lmd_graph_remove_edge_by_waiter_exact_result(&waiter),
				 CLUSTER_LMD_GRAPH_REMOVE_ABSENT);
}


/* ============================================================
 * U1c — distinct waiters are isolated; removing one keeps the other.
 * ============================================================ */

UT_TEST(test_distinct_waiters_isolated)
{
	ClusterLmdVertex w1 = mkvertex(1, 100, 7, 5000);
	ClusterLmdVertex w2 = mkvertex(1, 101, 7, 5001);
	ClusterLmdVertex b1 = mkvertex(2, 200, 7, 6000);
	ClusterLmdVertex b2 = mkvertex(3, 201, 7, 6001);
	ClusterLmdWaitEdge snap[32];
	uint64 gen;
	int n;

	reset_graph();

	/* w1 -> {b1, b2};  w2 -> {b1}. */
	UT_ASSERT(cluster_lmd_submit_wait_edge_real(&w1, &b1, w1.request_id));
	UT_ASSERT(cluster_lmd_submit_wait_edge_real(&w1, &b2, w1.request_id));
	UT_ASSERT(cluster_lmd_submit_wait_edge_real(&w2, &b1, w2.request_id));
	UT_ASSERT_EQ((int)cluster_lmd_wait_edge_count_get(), 3);

	UT_ASSERT(cluster_lmd_graph_remove_edge_by_waiter(&w1));

	/* Only w2 -> b1 remains. */
	UT_ASSERT_EQ((int)cluster_lmd_wait_edge_count_get(), 1);
	UT_ASSERT(!cluster_lmd_graph_has_waiter(&w1));
	UT_ASSERT(cluster_lmd_graph_has_waiter(&w2));

	n = cluster_lmd_graph_snapshot_copy(snap, 32, &gen);
	UT_ASSERT_EQ(n, 1);
	UT_ASSERT_EQ(count_edges(snap, n, &w2, &b1), 1);
}


/* ============================================================
 * U1d — re-submitting the same (waiter, blocker) is idempotent.
 * ============================================================ */

UT_TEST(test_duplicate_edge_is_idempotent)
{
	ClusterLmdVertex w = mkvertex(1, 100, 7, 5000);
	ClusterLmdVertex b = mkvertex(2, 200, 7, 6000);
	uint64 gen_before, gen_after;

	reset_graph();

	UT_ASSERT(cluster_lmd_submit_wait_edge_real(&w, &b, w.request_id));
	UT_ASSERT_EQ((int)cluster_lmd_wait_edge_count_get(), 1);
	gen_before = cluster_lmd_graph_generation_get();

	/* Same edge again — refresh, not duplicate. */
	UT_ASSERT(cluster_lmd_submit_wait_edge_real(&w, &b, w.request_id));
	UT_ASSERT_EQ((int)cluster_lmd_wait_edge_count_get(), 1);
	gen_after = cluster_lmd_graph_generation_get();
	UT_ASSERT(gen_after > gen_before);
}


/* ============================================================
 * U1e-U1j -- spec-2.36a C2 atomic blocker-set replacement.
 *
 * A PCM convert waiter cannot publish blockers as cancel + N independent
 * adds: a concurrent snapshot could observe an empty/partial set, and an add
 * failure would strand only a prefix.  The batch API is all-or-nothing.
 * ============================================================ */

UT_TEST(test_replace_waiter_edges_publishes_exact_batch)
{
	ClusterLmdVertex w = mkvertex(1, 100, 7, 5000);
	ClusterLmdVertex old = mkvertex(2, 200, 7, 6000);
	ClusterLmdVertex blockers[3]
		= { mkvertex(0, 300, 7, 7000), mkvertex(2, 301, 7, 7001), mkvertex(3, 302, 7, 7002) };
	ClusterLmdWaitEdge snap[8];
	uint64 gen;
	int n;

	reset_graph();
	UT_ASSERT(cluster_lmd_submit_wait_edge_real(&w, &old, w.request_id));
	UT_ASSERT(cluster_lmd_graph_replace_waiter_edges(&w, blockers, 3, w.request_id));

	n = cluster_lmd_graph_snapshot_copy(snap, 8, &gen);
	UT_ASSERT_EQ(n, 3);
	UT_ASSERT_EQ(count_edges(snap, n, &w, &old), 0);
	for (int i = 0; i < 3; i++)
		UT_ASSERT_EQ(count_edges(snap, n, &w, &blockers[i]), 1);
}

UT_TEST(test_replace_waiter_edges_full_preserves_previous_set)
{
	ClusterLmdVertex w = mkvertex(1, 100, 7, 5000);
	ClusterLmdVertex other = mkvertex(2, 101, 7, 5001);
	ClusterLmdVertex old[2] = { mkvertex(0, 200, 7, 6000), mkvertex(3, 201, 7, 6001) };
	ClusterLmdVertex replacement[3]
		= { mkvertex(0, 300, 7, 7000), mkvertex(2, 301, 7, 7001), mkvertex(3, 302, 7, 7002) };
	ClusterLmdWaitEdge snap[64];
	uint64 gen_before;
	uint64 gen_after;
	uint64 full_before;
	int n;

	cluster_lmd_max_wait_edges = 64;
	reset_graph();
	cluster_lmd_max_wait_edges = 1024;
	for (int i = 0; i < 2; i++)
		UT_ASSERT(cluster_lmd_submit_wait_edge_real(&w, &old[i], w.request_id));
	/* graph minimum capacity is 64; fill the remaining 62 entries. */
	for (int i = 0; i < 62; i++) {
		ClusterLmdVertex b = mkvertex(0, 1000 + (uint32)i, 7, 8000 + (uint64)i);
		UT_ASSERT(cluster_lmd_submit_wait_edge_real(&other, &b, other.request_id));
	}
	UT_ASSERT_EQ((int)cluster_lmd_wait_edge_count_get(), 64);
	gen_before = cluster_lmd_graph_generation_get();
	full_before = cluster_lmd_wait_edge_full_count_get();

	UT_ASSERT(!cluster_lmd_graph_replace_waiter_edges(&w, replacement, 3, w.request_id));
	gen_after = cluster_lmd_graph_generation_get();
	UT_ASSERT_EQ(gen_after, gen_before);
	UT_ASSERT_EQ(cluster_lmd_wait_edge_full_count_get(), full_before + 1);
	UT_ASSERT_EQ((int)cluster_lmd_wait_edge_count_get(), 64);

	n = cluster_lmd_graph_snapshot_copy(snap, 64, NULL);
	for (int i = 0; i < 2; i++)
		UT_ASSERT_EQ(count_edges(snap, n, &w, &old[i]), 1);
	for (int i = 0; i < 3; i++)
		UT_ASSERT_EQ(count_edges(snap, n, &w, &replacement[i]), 0);
}

UT_TEST(test_replace_waiter_edges_can_shrink_at_capacity)
{
	ClusterLmdVertex w = mkvertex(1, 100, 7, 5000);
	ClusterLmdVertex other = mkvertex(2, 101, 7, 5001);
	ClusterLmdVertex old1 = mkvertex(0, 200, 7, 6000);
	ClusterLmdVertex old2 = mkvertex(3, 201, 7, 6001);
	ClusterLmdVertex replacement = mkvertex(2, 300, 7, 7000);
	ClusterLmdVertex extra = mkvertex(3, 999, 7, 9999);

	cluster_lmd_max_wait_edges = 64;
	reset_graph();
	cluster_lmd_max_wait_edges = 1024;
	UT_ASSERT(cluster_lmd_submit_wait_edge_real(&w, &old1, w.request_id));
	UT_ASSERT(cluster_lmd_submit_wait_edge_real(&w, &old2, w.request_id));
	for (int i = 0; i < 62; i++) {
		ClusterLmdVertex b = mkvertex(0, 1000 + (uint32)i, 7, 8000 + (uint64)i);
		UT_ASSERT(cluster_lmd_submit_wait_edge_real(&other, &b, other.request_id));
	}

	UT_ASSERT(cluster_lmd_graph_replace_waiter_edges(&w, &replacement, 1, w.request_id));
	UT_ASSERT_EQ((int)cluster_lmd_wait_edge_count_get(), 63);
	UT_ASSERT(cluster_lmd_submit_wait_edge_real(&other, &extra, other.request_id));
	UT_ASSERT_EQ((int)cluster_lmd_wait_edge_count_get(), 64);
}

UT_TEST(test_replace_waiter_edges_deduplicates_blocker_identities)
{
	ClusterLmdVertex w = mkvertex(1, 100, 7, 5000);
	ClusterLmdVertex b1 = mkvertex(2, 200, 7, 6000);
	ClusterLmdVertex b2 = mkvertex(3, 201, 7, 6001);
	ClusterLmdVertex blockers[3] = { b1, b1, b2 };
	ClusterLmdWaitEdge snap[4];
	int n;

	reset_graph();
	UT_ASSERT(cluster_lmd_graph_replace_waiter_edges(&w, blockers, 3, w.request_id));
	n = cluster_lmd_graph_snapshot_copy(snap, 4, NULL);
	UT_ASSERT_EQ(n, 2);
	UT_ASSERT_EQ(count_edges(snap, n, &w, &b1), 1);
	UT_ASSERT_EQ(count_edges(snap, n, &w, &b2), 1);
}

UT_TEST(test_replace_waiter_edges_rejects_self_cycle_without_mutation)
{
	ClusterLmdVertex w = mkvertex(1, 100, 7, 5000);
	ClusterLmdVertex old = mkvertex(2, 200, 7, 6000);
	ClusterLmdVertex blockers[2] = { mkvertex(3, 300, 7, 7000), w };
	ClusterLmdWaitEdge snap[4];
	uint64 gen_before;
	int n;

	reset_graph();
	UT_ASSERT(cluster_lmd_submit_wait_edge_real(&w, &old, w.request_id));
	gen_before = cluster_lmd_graph_generation_get();
	UT_ASSERT(!cluster_lmd_graph_replace_waiter_edges(&w, blockers, 2, w.request_id));
	UT_ASSERT_EQ(cluster_lmd_graph_generation_get(), gen_before);
	n = cluster_lmd_graph_snapshot_copy(snap, 4, NULL);
	UT_ASSERT_EQ(n, 1);
	UT_ASSERT_EQ(count_edges(snap, n, &w, &old), 1);
}

UT_TEST(test_replace_waiter_edges_empty_set_removes_only_that_waiter)
{
	ClusterLmdVertex w1 = mkvertex(1, 100, 7, 5000);
	ClusterLmdVertex w2 = mkvertex(2, 101, 7, 5001);
	ClusterLmdVertex b1 = mkvertex(2, 200, 7, 6000);
	ClusterLmdVertex b2 = mkvertex(3, 201, 7, 6001);

	reset_graph();
	UT_ASSERT(cluster_lmd_submit_wait_edge_real(&w1, &b1, w1.request_id));
	UT_ASSERT(cluster_lmd_submit_wait_edge_real(&w2, &b2, w2.request_id));
	UT_ASSERT(cluster_lmd_graph_replace_waiter_edges(&w1, NULL, 0, w1.request_id));
	UT_ASSERT(!cluster_lmd_graph_has_waiter(&w1));
	UT_ASSERT(cluster_lmd_graph_has_waiter(&w2));
	UT_ASSERT_EQ((int)cluster_lmd_wait_edge_count_get(), 1);
}

UT_TEST(test_replace_waiter_edges_more_than_batch_at_capacity)
{
	ClusterLmdVertex w = mkvertex(1, 100, 7, 5000);
	ClusterLmdVertex other = mkvertex(2, 101, 7, 5001);
	ClusterLmdVertex old[40];
	ClusterLmdVertex replacement[40];
	ClusterLmdWaitEdge snap[64];
	uint64 gen_before;
	int scans_before;
	int n;

	cluster_lmd_max_wait_edges = 64;
	reset_graph();
	cluster_lmd_max_wait_edges = 1024;
	for (int i = 0; i < 40; i++) {
		old[i] = mkvertex(0, 2000 + (uint32)i, 7, 6000 + (uint64)i);
		replacement[i] = mkvertex(3, 3000 + (uint32)i, 7, 7000 + (uint64)i);
		UT_ASSERT(cluster_lmd_submit_wait_edge_real(&w, &old[i], w.request_id));
	}
	for (int i = 0; i < 24; i++) {
		ClusterLmdVertex b = mkvertex(0, 4000 + (uint32)i, 7, 8000 + (uint64)i);
		UT_ASSERT(cluster_lmd_submit_wait_edge_real(&other, &b, other.request_id));
	}
	UT_ASSERT_EQ((int)cluster_lmd_wait_edge_count_get(), 64);
	gen_before = cluster_lmd_graph_generation_get();
	scans_before = fake_htab_seq_init_count;

	UT_ASSERT(cluster_lmd_graph_replace_waiter_edges(&w, replacement, 40, w.request_id));
	UT_ASSERT_EQ(fake_htab_seq_init_count - scans_before, 1);
	UT_ASSERT_EQ((int)cluster_lmd_wait_edge_count_get(), 64);
	UT_ASSERT_EQ(cluster_lmd_graph_generation_get(), gen_before + 80);

	n = cluster_lmd_graph_snapshot_copy(snap, 64, NULL);
	UT_ASSERT_EQ(n, 64);
	for (int i = 0; i < 40; i++) {
		UT_ASSERT_EQ(count_edges(snap, n, &w, &old[i]), 0);
		UT_ASSERT_EQ(count_edges(snap, n, &w, &replacement[i]), 1);
	}
}

UT_TEST(test_replace_waiter_edges_rejects_conflicting_duplicate_metadata)
{
	ClusterLmdVertex w = mkvertex(1, 100, 7, 5000);
	ClusterLmdVertex old = mkvertex(2, 200, 7, 6000);
	ClusterLmdVertex duplicate = mkvertex(3, 300, 7, 7000);
	ClusterLmdWaitEdge snap[4];
	uint64 gen_before;
	int n;

	for (int variant = 0; variant < 3; variant++) {
		ClusterLmdVertex blockers[2] = { duplicate, duplicate };

		reset_graph();
		UT_ASSERT(cluster_lmd_submit_wait_edge_real(&w, &old, w.request_id));
		gen_before = cluster_lmd_graph_generation_get();
		if (variant == 0)
			blockers[1].wait_seq++;
		else if (variant == 1)
			blockers[1].xid++;
		else
			blockers[1].local_start_ts_ms++;

		UT_ASSERT(!cluster_lmd_graph_replace_waiter_edges(&w, blockers, 2, w.request_id));
		UT_ASSERT_EQ(cluster_lmd_graph_generation_get(), gen_before);
		UT_ASSERT_EQ((int)cluster_lmd_wait_edge_count_get(), 1);
		n = cluster_lmd_graph_snapshot_copy(snap, 4, NULL);
		UT_ASSERT_EQ(n, 1);
		UT_ASSERT_EQ(count_edges(snap, n, &w, &old), 1);
	}
}

UT_TEST(test_replace_waiter_edges_insert_failure_rolls_back_exactly)
{
	ClusterLmdVertex w = mkvertex(1, 100, 7, 5000);
	ClusterLmdVertex old[2] = { mkvertex(0, 200, 7, 6000), mkvertex(2, 201, 7, 6001) };
	ClusterLmdVertex replacement[3]
		= { mkvertex(0, 300, 7, 7000), mkvertex(2, 301, 7, 7001), mkvertex(3, 302, 7, 7002) };
	ClusterLmdWaitEdge snap[8];
	uint64 gen_before;
	uint64 full_before;
	int n;

	reset_graph();
	for (int i = 0; i < 2; i++)
		UT_ASSERT(cluster_lmd_submit_wait_edge_real(&w, &old[i], w.request_id));
	gen_before = cluster_lmd_graph_generation_get();
	full_before = cluster_lmd_wait_edge_full_count_get();
	/* Let one replacement insert succeed, then fail the second non-throwing enter. */
	fake_hash_enter_null_fail_after = 1;

	UT_ASSERT(!cluster_lmd_graph_replace_waiter_edges(&w, replacement, 3, w.request_id));
	UT_ASSERT_EQ(cluster_lmd_graph_generation_get(), gen_before);
	UT_ASSERT_EQ((int)cluster_lmd_wait_edge_count_get(), 2);
	UT_ASSERT_EQ(cluster_lmd_wait_edge_full_count_get(), full_before + 1);
	n = cluster_lmd_graph_snapshot_copy(snap, 8, NULL);
	UT_ASSERT_EQ(n, 2);
	for (int i = 0; i < 2; i++)
		UT_ASSERT_EQ(count_edges(snap, n, &w, &old[i]), 1);
	for (int i = 0; i < 3; i++)
		UT_ASSERT_EQ(count_edges(snap, n, &w, &replacement[i]), 0);
}

UT_TEST(test_replace_waiter_edges_rejects_request_id_mismatch)
{
	ClusterLmdVertex w = mkvertex(1, 100, 7, 5000);
	ClusterLmdVertex old = mkvertex(2, 200, 7, 6000);
	ClusterLmdVertex replacement = mkvertex(3, 300, 7, 7000);
	uint64 gen_before;

	reset_graph();
	UT_ASSERT(cluster_lmd_submit_wait_edge_real(&w, &old, w.request_id));
	gen_before = cluster_lmd_graph_generation_get();
	UT_ASSERT(!cluster_lmd_graph_replace_waiter_edges(&w, &replacement, 1, w.request_id + 1));
	UT_ASSERT_EQ(cluster_lmd_graph_generation_get(), gen_before);
	UT_ASSERT_EQ((int)cluster_lmd_wait_edge_count_get(), 1);
}

UT_TEST(test_replace_waiter_edges_rejects_raw_count_above_graph_bound)
{
	ClusterLmdVertex w = mkvertex(1, 100, 7, 5000);
	ClusterLmdVertex old = mkvertex(2, 200, 7, 6000);
	ClusterLmdVertex replacement = mkvertex(3, 300, 7, 7000);
	uint64 gen_before;

	cluster_lmd_max_wait_edges = 64;
	reset_graph();
	cluster_lmd_max_wait_edges = 1024;
	UT_ASSERT(cluster_lmd_submit_wait_edge_real(&w, &old, w.request_id));
	gen_before = cluster_lmd_graph_generation_get();
	/* The raw count is rejected before the one-element pointer can be read. */
	UT_ASSERT(!cluster_lmd_graph_replace_waiter_edges(&w, &replacement, 65, w.request_id));
	UT_ASSERT_EQ(cluster_lmd_graph_generation_get(), gen_before);
	UT_ASSERT_EQ((int)cluster_lmd_wait_edge_count_get(), 1);
}

UT_TEST(test_replace_waiter_edges_scan_growth_retry_has_no_generation_side_effect)
{
	ClusterLmdVertex w = mkvertex(1, 100, 7, 5000);
	uint64 gen_before;
	uint64 full_before;
	int scans_before;

	cluster_lmd_max_wait_edges = 64;
	reset_graph();
	cluster_lmd_max_wait_edges = 1024;
	for (int i = 0; i < 40; i++) {
		ClusterLmdVertex old = mkvertex(0, 2000 + (uint32)i, 7, 6000 + (uint64)i);
		UT_ASSERT(cluster_lmd_submit_wait_edge_real(&w, &old, w.request_id));
	}
	gen_before = cluster_lmd_graph_generation_get();
	full_before = cluster_lmd_wait_edge_full_count_get();
	scans_before = fake_htab_seq_init_count;

	/* Empty replacement starts with capacity 32, forcing one lock-free grow/retry. */
	UT_ASSERT(cluster_lmd_graph_replace_waiter_edges(&w, NULL, 0, w.request_id));
	UT_ASSERT_EQ(fake_htab_seq_init_count - scans_before, 2);
	UT_ASSERT_EQ(cluster_lmd_graph_generation_get(), gen_before + 40);
	UT_ASSERT_EQ(cluster_lmd_wait_edge_full_count_get(), full_before);
	UT_ASSERT_EQ((int)cluster_lmd_wait_edge_count_get(), 0);
}

UT_TEST(test_replace_waiter_edges_exact_returns_committed_generation)
{
	ClusterLmdVertex w = mkvertex(1, 100, 7, 5000);
	ClusterLmdVertex old = mkvertex(0, 200, 7, 6000);
	ClusterLmdVertex replacement[2] = { mkvertex(2, 300, 7, 7000), mkvertex(3, 301, 7, 7001) };
	uint64 gen_before;
	uint64 committed_generation;

	reset_graph();
	UT_ASSERT(cluster_lmd_submit_wait_edge_real(&w, &old, w.request_id));
	gen_before = cluster_lmd_graph_generation_get();

	committed_generation
		= cluster_lmd_graph_replace_waiter_edges_exact(&w, replacement, 2, w.request_id);

	UT_ASSERT_NE(committed_generation, 0);
	UT_ASSERT_EQ(committed_generation, gen_before + 3);
	UT_ASSERT_EQ(cluster_lmd_graph_generation_get(), committed_generation);
}

UT_TEST(test_replace_waiter_edges_exact_capacity_failure_is_byte_stable)
{
	ClusterLmdVertex w = mkvertex(1, 100, 7, 5000);
	ClusterLmdVertex other = mkvertex(2, 101, 7, 5001);
	ClusterLmdVertex old[2] = { mkvertex(0, 200, 7, 6000), mkvertex(3, 201, 7, 6001) };
	ClusterLmdVertex replacement[3]
		= { mkvertex(0, 300, 7, 7000), mkvertex(2, 301, 7, 7001), mkvertex(3, 302, 7, 7002) };
	ClusterLmdWaitEdge before[64];
	ClusterLmdWaitEdge after[64];
	uint64 gen_before;
	uint64 gen_after;
	int n_before;
	int n_after;

	cluster_lmd_max_wait_edges = 64;
	reset_graph();
	cluster_lmd_max_wait_edges = 1024;
	for (int i = 0; i < 2; i++)
		UT_ASSERT(cluster_lmd_submit_wait_edge_real(&w, &old[i], w.request_id));
	for (int i = 0; i < 62; i++) {
		ClusterLmdVertex b = mkvertex(0, 1000 + (uint32)i, 7, 8000 + (uint64)i);

		UT_ASSERT(cluster_lmd_submit_wait_edge_real(&other, &b, other.request_id));
	}
	n_before = cluster_lmd_graph_snapshot_copy(before, 64, &gen_before);
	UT_ASSERT_EQ(n_before, 64);

	UT_ASSERT_EQ(cluster_lmd_graph_replace_waiter_edges_exact(&w, replacement, 3, w.request_id), 0);

	n_after = cluster_lmd_graph_snapshot_copy(after, 64, &gen_after);
	UT_ASSERT_EQ(n_after, n_before);
	UT_ASSERT_EQ(gen_after, gen_before);
	UT_ASSERT_EQ(memcmp(after, before, sizeof(before)), 0);
}


/* ============================================================
 * U2a — spec-5.8 D2 (T2):  a cross-node TX cycle expressed with holder
 * placeholders resolves so each placeholder blocker becomes the real waiter
 * vertex that owns the same (node, xid).  This is the RED→GREEN driver: the
 * no-op resolver leaves procno == sentinel and fails the assertion.
 * ============================================================ */

UT_TEST(test_resolve_tx_placeholder_closes_cross_node_cycle)
{
	/* A on node 1 (xid 1001) waits on holder txn 2002 on node 2;
	 * B on node 2 (xid 2002) waits on holder txn 1001 on node 1. */
	ClusterLmdVertex wa = mkvertex_xid(1, 100, 7, 0, 1001);
	ClusterLmdVertex wb = mkvertex_xid(2, 200, 7, 0, 2002);
	ClusterLmdVertex pa = mkvertex_xid(2, CLUSTER_LMD_TX_HOLDER_PROCNO, 7, 0, 2002);
	ClusterLmdVertex pb = mkvertex_xid(1, CLUSTER_LMD_TX_HOLDER_PROCNO, 7, 0, 1001);
	ClusterLmdWaitEdge edges[2];

	edges[0] = mkedge(wa, pa);
	edges[1] = mkedge(wb, pb);

	cluster_lmd_resolve_tx_placeholders(edges, 2);

	/* edge0's placeholder (holder txn 2002 on node 2) is waiter B. */
	UT_ASSERT(edges[0].blocker.procno != CLUSTER_LMD_TX_HOLDER_PROCNO);
	UT_ASSERT(vertex_eq(&edges[0].blocker, &wb));
	/* edge1's placeholder (holder txn 1001 on node 1) is waiter A. */
	UT_ASSERT(edges[1].blocker.procno != CLUSTER_LMD_TX_HOLDER_PROCNO);
	UT_ASSERT(vertex_eq(&edges[1].blocker, &wa));
}


/* ============================================================
 * U2b — a placeholder whose xid matches no waiter is left untouched (an
 * unresolved sink, never a fabricated edge).
 * ============================================================ */

UT_TEST(test_resolve_tx_placeholder_no_match_unchanged)
{
	ClusterLmdVertex wa = mkvertex_xid(1, 100, 7, 0, 1001);
	ClusterLmdVertex orphan = mkvertex_xid(2, CLUSTER_LMD_TX_HOLDER_PROCNO, 7, 0, 9999);
	ClusterLmdWaitEdge edges[1];

	edges[0] = mkedge(wa, orphan);
	cluster_lmd_resolve_tx_placeholders(edges, 1);

	UT_ASSERT(edges[0].blocker.procno == CLUSTER_LMD_TX_HOLDER_PROCNO);
	UT_ASSERT(vertex_eq(&edges[0].blocker, &orphan));
}


/* ============================================================
 * U2c — a placeholder with an Invalid xid is never resolved (Rule 8.A), even
 * against an Invalid-xid waiter on the same node (TransactionIdEquals would
 * otherwise treat Invalid == Invalid as a match).
 * ============================================================ */

UT_TEST(test_resolve_tx_placeholder_invalid_xid_never_matches)
{
	ClusterLmdVertex winv = mkvertex_xid(2, 200, 7, 0, InvalidTransactionId);
	ClusterLmdVertex wc = mkvertex_xid(1, 100, 7, 0, 5000);
	ClusterLmdVertex pinv
		= mkvertex_xid(2, CLUSTER_LMD_TX_HOLDER_PROCNO, 7, 0, InvalidTransactionId);
	ClusterLmdWaitEdge edges[2];

	edges[0] = mkedge(wc, pinv);
	edges[1] = mkedge(winv, mkvertex_xid(3, CLUSTER_LMD_TX_HOLDER_PROCNO, 7, 0, 7777));

	cluster_lmd_resolve_tx_placeholders(edges, 2);

	/* The Invalid-xid placeholder must stay a placeholder. */
	UT_ASSERT(edges[0].blocker.procno == CLUSTER_LMD_TX_HOLDER_PROCNO);
}


/* ============================================================
 * U2d — resolution requires a node match: a shared xid on a different node
 * must not bind (xid is unique only within a node).
 * ============================================================ */

UT_TEST(test_resolve_tx_placeholder_requires_node_match)
{
	ClusterLmdVertex wa = mkvertex_xid(1, 100, 7, 0, 1001);
	ClusterLmdVertex pwrong = mkvertex_xid(2, CLUSTER_LMD_TX_HOLDER_PROCNO, 7, 0, 1001);
	ClusterLmdWaitEdge edges[1];

	edges[0] = mkedge(wa, pwrong);
	cluster_lmd_resolve_tx_placeholders(edges, 1);

	/* The only waiter with xid 1001 is on node 1; the placeholder is node 2. */
	UT_ASSERT(edges[0].blocker.procno == CLUSTER_LMD_TX_HOLDER_PROCNO);
}


/* ============================================================
 * U2e — a real (non-placeholder) blocker is never rewritten.
 * ============================================================ */

UT_TEST(test_resolve_tx_placeholder_leaves_real_blocker)
{
	ClusterLmdVertex wa = mkvertex_xid(1, 100, 7, 500, 1001);
	ClusterLmdVertex real = mkvertex_xid(2, 200, 7, 600, 2002);
	ClusterLmdWaitEdge edges[1];

	edges[0] = mkedge(wa, real);
	cluster_lmd_resolve_tx_placeholders(edges, 1);

	UT_ASSERT(vertex_eq(&edges[0].blocker, &real));
}


/* ============================================================
 * spec-5.8 D4 — coordinator REPORT-collector member admission.
 *
 *	U3a: a probed peer reporting for the first time is admitted.
 *	U3b: a second report from the same peer is dropped as a duplicate
 *	     (it must not double-count and mask a missing node).
 *	U3c: a report from a node that was never probed is dropped.
 *	U3d: an out-of-range / negative node_id is dropped (defensive).
 *	U3e: the hi word (node_id >= 64) is handled independently of lo.
 * ============================================================ */

/* node_id bit helpers mirroring the collector's lo/hi layout. */
#define MEMBIT_LO(n) ((n) < 64 ? (UINT64CONST(1) << (n)) : UINT64CONST(0))
#define MEMBIT_HI(n) ((n) >= 64 ? (UINT64CONST(1) << ((n) - 64)) : UINT64CONST(0))

UT_TEST(test_probe_admit_first_report_from_expected_peer)
{
	/* expected = {1,2,3}; received = {} → node 2 admitted. */
	uint64 exp_lo = MEMBIT_LO(1) | MEMBIT_LO(2) | MEMBIT_LO(3);

	UT_ASSERT_EQ((int)cluster_lmd_probe_member_admit(exp_lo, 0, 0, 0, 2),
				 (int)CLUSTER_LMD_PROBE_ADMIT);
}

UT_TEST(test_probe_admit_duplicate_dropped)
{
	/* expected = {1,2,3}; received already has 2 → node 2 is a duplicate. */
	uint64 exp_lo = MEMBIT_LO(1) | MEMBIT_LO(2) | MEMBIT_LO(3);
	uint64 rcv_lo = MEMBIT_LO(2);

	UT_ASSERT_EQ((int)cluster_lmd_probe_member_admit(exp_lo, 0, rcv_lo, 0, 2),
				 (int)CLUSTER_LMD_PROBE_DROP_DUPLICATE);
}

UT_TEST(test_probe_admit_unexpected_node_dropped)
{
	/* expected = {1,2,3}; node 7 was never probed → dropped. */
	uint64 exp_lo = MEMBIT_LO(1) | MEMBIT_LO(2) | MEMBIT_LO(3);

	UT_ASSERT_EQ((int)cluster_lmd_probe_member_admit(exp_lo, 0, 0, 0, 7),
				 (int)CLUSTER_LMD_PROBE_DROP_UNEXPECTED);
}

UT_TEST(test_probe_admit_out_of_range_dropped)
{
	uint64 exp_lo = MEMBIT_LO(1) | MEMBIT_LO(2);

	UT_ASSERT_EQ((int)cluster_lmd_probe_member_admit(exp_lo, 0, 0, 0, -1),
				 (int)CLUSTER_LMD_PROBE_DROP_UNEXPECTED);
	UT_ASSERT_EQ((int)cluster_lmd_probe_member_admit(exp_lo, 0, 0, 0, 128),
				 (int)CLUSTER_LMD_PROBE_DROP_UNEXPECTED);
}

UT_TEST(test_probe_admit_hi_word_independent)
{
	/* expected = {70}; node 70 lives in the hi word.  First report admits;
	 * a duplicate in the hi word drops; lo-word state is irrelevant. */
	uint64 exp_hi = MEMBIT_HI(70);

	UT_ASSERT_EQ((int)cluster_lmd_probe_member_admit(0, exp_hi, 0, 0, 70),
				 (int)CLUSTER_LMD_PROBE_ADMIT);
	UT_ASSERT_EQ((int)cluster_lmd_probe_member_admit(0, exp_hi, 0, MEMBIT_HI(70), 70),
				 (int)CLUSTER_LMD_PROBE_DROP_DUPLICATE);
	/* node 70 not in an lo-only expected set → unexpected. */
	UT_ASSERT_EQ((int)cluster_lmd_probe_member_admit(MEMBIT_LO(5), 0, 0, 0, 70),
				 (int)CLUSTER_LMD_PROBE_DROP_UNEXPECTED);
}

/*
 * spec-5.8 Hardening v1.0.1 — FC1 acting gate completeness (cluster_lmd_probe_
 * round_complete).  A round is COMPLETE only on an exact member-set match;
 * received ⊊ expected is partial and must NOT be confirmed/cancelled.
 */
UT_TEST(test_probe_round_complete_exact_match)
{
	/* expected = received = {1,2,3} -> complete (would proceed to Tarjan). */
	uint64 exp_lo = MEMBIT_LO(1) | MEMBIT_LO(2) | MEMBIT_LO(3);

	UT_ASSERT_EQ((int)cluster_lmd_probe_round_complete(exp_lo, 0, exp_lo, 0), 1);
	/* hi-word participant included in both -> still complete. */
	UT_ASSERT_EQ(
		(int)cluster_lmd_probe_round_complete(exp_lo, MEMBIT_HI(70), exp_lo, MEMBIT_HI(70)), 1);
}

UT_TEST(test_probe_round_incomplete_partial_subset)
{
	/* expected = {1,2,3}; received = {1,2} (node 3 silent) -> INCOMPLETE.
	 * This is the FC1 fail-closed case: the partial union is discarded. */
	uint64 exp_lo = MEMBIT_LO(1) | MEMBIT_LO(2) | MEMBIT_LO(3);
	uint64 rcv_lo = MEMBIT_LO(1) | MEMBIT_LO(2);

	UT_ASSERT_EQ((int)cluster_lmd_probe_round_complete(exp_lo, 0, rcv_lo, 0), 0);
	/* a hi-word expected peer (node 70) that did not report -> incomplete. */
	UT_ASSERT_EQ((int)cluster_lmd_probe_round_complete(exp_lo, MEMBIT_HI(70), exp_lo, 0), 0);
	/* empty received against a non-empty expected -> incomplete. */
	UT_ASSERT_EQ((int)cluster_lmd_probe_round_complete(exp_lo, 0, 0, 0), 0);
}

UT_TEST(test_probe_round_two_identical_partial_subsets_never_complete)
{
	/*
	 * The reviewer's worst case: two confirm rounds observe the SAME partial
	 * subset (node 3 silent both times).  Each round is independently judged
	 * incomplete, so the two-round driver can never confirm/cancel from them —
	 * a stable partial subset must NOT be mistaken for a confirmed deadlock.
	 */
	uint64 exp_lo = MEMBIT_LO(1) | MEMBIT_LO(2) | MEMBIT_LO(3);
	uint64 rcv_lo = MEMBIT_LO(1) | MEMBIT_LO(2); /* same subset both rounds */

	UT_ASSERT_EQ((int)cluster_lmd_probe_round_complete(exp_lo, 0, rcv_lo, 0), 0); /* round 1 */
	UT_ASSERT_EQ((int)cluster_lmd_probe_round_complete(exp_lo, 0, rcv_lo, 0), 0); /* round 2 */
}


UT_TEST(test_pcm_convert_wfg_note_counters_are_narrow_and_exact)
{
	reset_graph();
	UT_ASSERT_EQ(cluster_lmd_pcm_convert_wfg_replace_count_get(), 0);
	UT_ASSERT_EQ(cluster_lmd_pcm_convert_wfg_remove_count_get(), 0);
	UT_ASSERT_EQ(cluster_lmd_pcm_convert_wfg_replace_fail_count_get(), 0);
	UT_ASSERT_EQ(cluster_lmd_pcm_convert_wfg_exact_remove_stale_count_get(), 0);

	cluster_lmd_pcm_convert_wfg_note_replace();
	cluster_lmd_pcm_convert_wfg_note_replace();
	cluster_lmd_pcm_convert_wfg_note_remove();
	cluster_lmd_pcm_convert_wfg_note_replace_fail();
	cluster_lmd_pcm_convert_wfg_note_exact_remove_stale();
	cluster_lmd_pcm_convert_wfg_note_exact_remove_stale();

	UT_ASSERT_EQ(cluster_lmd_pcm_convert_wfg_replace_count_get(), 2);
	UT_ASSERT_EQ(cluster_lmd_pcm_convert_wfg_remove_count_get(), 1);
	UT_ASSERT_EQ(cluster_lmd_pcm_convert_wfg_replace_fail_count_get(), 1);
	UT_ASSERT_EQ(cluster_lmd_pcm_convert_wfg_exact_remove_stale_count_get(), 2);
}


int
main(void)
{
	UT_PLAN(34);
	UT_RUN(test_multi_blocker_edges_not_overwritten);
	UT_RUN(test_remove_by_waiter_removes_all_blocker_edges);
	UT_RUN(test_exact_remove_requires_wait_seq_and_preserves_new_wait_instance);
	UT_RUN(test_exact_remove_result_distinguishes_absent_stale_and_removed);
	UT_RUN(test_distinct_waiters_isolated);
	UT_RUN(test_duplicate_edge_is_idempotent);
	UT_RUN(test_replace_waiter_edges_publishes_exact_batch);
	UT_RUN(test_replace_waiter_edges_full_preserves_previous_set);
	UT_RUN(test_replace_waiter_edges_can_shrink_at_capacity);
	UT_RUN(test_replace_waiter_edges_deduplicates_blocker_identities);
	UT_RUN(test_replace_waiter_edges_rejects_self_cycle_without_mutation);
	UT_RUN(test_replace_waiter_edges_empty_set_removes_only_that_waiter);
	UT_RUN(test_replace_waiter_edges_more_than_batch_at_capacity);
	UT_RUN(test_replace_waiter_edges_rejects_conflicting_duplicate_metadata);
	UT_RUN(test_replace_waiter_edges_insert_failure_rolls_back_exactly);
	UT_RUN(test_replace_waiter_edges_rejects_request_id_mismatch);
	UT_RUN(test_replace_waiter_edges_rejects_raw_count_above_graph_bound);
	UT_RUN(test_replace_waiter_edges_scan_growth_retry_has_no_generation_side_effect);
	UT_RUN(test_replace_waiter_edges_exact_returns_committed_generation);
	UT_RUN(test_replace_waiter_edges_exact_capacity_failure_is_byte_stable);
	UT_RUN(test_resolve_tx_placeholder_closes_cross_node_cycle);
	UT_RUN(test_resolve_tx_placeholder_no_match_unchanged);
	UT_RUN(test_resolve_tx_placeholder_invalid_xid_never_matches);
	UT_RUN(test_resolve_tx_placeholder_requires_node_match);
	UT_RUN(test_resolve_tx_placeholder_leaves_real_blocker);
	UT_RUN(test_probe_admit_first_report_from_expected_peer);
	UT_RUN(test_probe_admit_duplicate_dropped);
	UT_RUN(test_probe_admit_unexpected_node_dropped);
	UT_RUN(test_probe_admit_out_of_range_dropped);
	UT_RUN(test_probe_admit_hi_word_independent);
	UT_RUN(test_probe_round_complete_exact_match);
	UT_RUN(test_probe_round_incomplete_partial_subset);
	UT_RUN(test_probe_round_two_identical_partial_subsets_never_complete);
	UT_RUN(test_pcm_convert_wfg_note_counters_are_narrow_and_exact);
	UT_DONE();
	return 0;
}
