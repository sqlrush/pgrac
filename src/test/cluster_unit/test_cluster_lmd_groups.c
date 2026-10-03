/*-------------------------------------------------------------------------
 * test_cluster_lmd_groups.c
 *    Run the production Tarjan detector with parallel lock groups.
 * Portions Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 *-------------------------------------------------------------------------
 */
#include "postgres.h"
#include "cluster/cluster_lmd.h"
#include "utils/memutils.h"
#undef printf
#include "unit_test.h"
UT_DEFINE_GLOBALS();

/* Only allocation and graph snapshot boundaries are fixtures. */
typedef struct TestAllocation {
	struct TestAllocation *next;
	struct TestContext *owner;
	max_align_t alignment;
} TestAllocation;
typedef struct TestContext {
	MemoryContextData header;
	TestAllocation *allocations;
} TestContext;
static TestContext root = { .header.type = T_AllocSetContext };
MemoryContext CurrentMemoryContext = &root.header;
static ClusterLmdWaitEdge live_edges[8];
static int live_count;
static uint64 revalidate_failures;
int cluster_lmd_max_wait_edges = 64;

MemoryContext
AllocSetContextCreateInternal(MemoryContext parent, const char *name, Size minsize, Size initsize,
							  Size maxsize)
{
	TestContext *ctx = calloc(1, sizeof(*ctx));

	ctx->header.type = T_AllocSetContext;
	return &ctx->header;
}
void *
palloc(Size size)
{
	TestContext *ctx = (TestContext *)CurrentMemoryContext;
	TestAllocation *a = malloc(sizeof(*a) + size);

	if (!a)
		abort();
	a->owner = ctx;
	a->next = ctx->allocations;
	ctx->allocations = a;
	return a + 1;
}
void *
palloc0(Size size)
{
	void *p = palloc(size);
	memset(p, 0, size);
	return p;
}
void
pfree(void *pointer)
{
	TestAllocation *a = (TestAllocation *)pointer - 1;
	TestAllocation **link = &a->owner->allocations;

	while (*link != a)
		link = &(*link)->next;
	*link = a->next;
	free(a);
}
void
MemoryContextDelete(MemoryContext context)
{
	TestContext *ctx = (TestContext *)context;

	while (ctx->allocations)
		pfree(ctx->allocations + 1);
	free(ctx);
}
void
ExceptionalCondition(const char *condition, const char *file, int line)
{
	printf("# %s at %s:%d\n", condition, file, line);
	abort();
}
void
cluster_lmd_resolve_tx_placeholders(ClusterLmdWaitEdge *edges, int n)
{
	/* These are real GES identities; TX resolution has separate graph tests. */
	for (int i = 0; i < n; i++)
		Assert(edges[i].blocker.procno != CLUSTER_LMD_TX_HOLDER_PROCNO);
}
int
cluster_lmd_graph_snapshot_copy(ClusterLmdWaitEdge *out, int capacity, uint64 *generation)
{
	Assert(capacity >= live_count);
	memcpy(out, live_edges, sizeof(*out) * live_count);
	*generation = 123;
	return live_count;
}
void
cluster_lmd_revalidate_fail_count_inc(uint64 delta)
{
	revalidate_failures += delta;
}

#include "../../backend/cluster/cluster_lmd_tarjan.c"

static ClusterLmdVertex
vertex(int node, uint32 proc, uint32 group, uint64 request)
{
	ClusterLmdVertex v = { 0 };

	v.node_id = node;
	v.procno = proc;
	v.lock_group_procno_plus_one = group;
	v.cluster_epoch = 7;
	v.request_id = request;
	v.wait_seq = request + 1000;
	v.local_start_ts_ms = request;
	return v;
}
static void
set_edge(int i, ClusterLmdVertex waiter, ClusterLmdVertex blocker)
{
	memset(&live_edges[i], 0, sizeof(live_edges[i]));
	live_edges[i].waiter = waiter;
	live_edges[i].blocker = blocker;
	live_edges[i].request_id = waiter.request_id;
}
static void
group_cycle(void)
{
	/* Leader acquired before its group existed: group metadata is zero. */
	set_edge(0, vertex(1, 11, 11, 90), vertex(2, 20, 0, 10));
	set_edge(1, vertex(2, 20, 0, 20), vertex(1, 10, 0, 1));
	live_count = 2;
}
static int
scan(ClusterLmdVertex *out, int *cycles)
{
	return cluster_lmd_tarjan_scan_snapshot(live_edges, live_count, out, 8, cycles);
}

UT_TEST(worker_wait_and_leader_hold_close_cycle)
{
	ClusterLmdVertex out[8] = { 0 }, victim = { 0 };
	int cycles;

	group_cycle();
	UT_ASSERT_EQ(scan(out, &cycles), 2);
	UT_ASSERT_EQ(cycles, 1);
	UT_ASSERT(cluster_lmd_tarjan_pick_victim(out, 2, NULL, &victim));
	UT_ASSERT_EQ(victim.procno, 11);
	UT_ASSERT_EQ(victim.request_id, 90);
	UT_ASSERT_EQ(victim.wait_seq, 1090);
	UT_ASSERT_EQ(victim.lock_group_procno_plus_one, 11);
}
UT_TEST(two_groups_with_nonwaiting_holder_members)
{
	ClusterLmdVertex out[8] = { 0 };
	int cycles;

	set_edge(0, vertex(1, 11, 11, 90), vertex(2, 22, 21, 10));
	set_edge(1, vertex(2, 21, 21, 20), vertex(1, 12, 11, 1));
	live_count = 2;
	UT_ASSERT_EQ(scan(out, &cycles), 2);
	UT_ASSERT_EQ(cycles, 1);
	for (int i = 0; i < 2; i++)
		UT_ASSERT(out[i].procno == 11 || out[i].procno == 21);
}
UT_TEST(group_alias_does_not_create_self_cycle)
{
	ClusterLmdVertex out[8] = { 0 };
	int cycles;

	set_edge(0, vertex(1, 11, 11, 90), vertex(1, 10, 0, 1));
	live_count = 1;
	UT_ASSERT_EQ(scan(out, &cycles), 0);
	UT_ASSERT_EQ(cycles, 0);
}
UT_TEST(node_and_epoch_still_separate_groups)
{
	ClusterLmdVertex out[8] = { 0 };
	int cycles;

	group_cycle();
	live_edges[1].blocker.node_id = 3;
	UT_ASSERT_EQ(scan(out, &cycles), 0);
	group_cycle();
	live_edges[1].blocker.cluster_epoch++;
	UT_ASSERT_EQ(scan(out, &cycles), 0);
}
UT_TEST(victim_must_wait_on_an_edge_inside_the_cycle)
{
	ClusterLmdVertex out[8] = { 0 }, victim = { 0 };
	int cycles;

	group_cycle();
	/* A younger group member waits on an unrelated sink. */
	set_edge(2, vertex(1, 12, 11, 99), vertex(3, 30, 0, 2));
	live_count = 3;
	UT_ASSERT_EQ(scan(out, &cycles), 2);
	UT_ASSERT(cluster_lmd_tarjan_pick_victim(out, 2, NULL, &victim));
	UT_ASSERT_EQ(victim.procno, 11);
	UT_ASSERT_EQ(victim.request_id, 90);
}
UT_TEST(group_fingerprint_ignores_member_and_edge_order)
{
	ClusterLmdVertex out[8] = { 0 }, victim = { 0 };
	ClusterLmdWaitEdge first;
	uint64 hash1 = 0, hash2 = 0;
	int cycles;

	group_cycle();
	set_edge(2, vertex(1, 12, 11, 99), vertex(2, 20, 0, 10));
	live_count = 3;
	UT_ASSERT_EQ(scan(out, &cycles), 2);
	UT_ASSERT(cluster_lmd_tarjan_pick_victim(out, 2, NULL, &victim));
	UT_ASSERT_EQ(victim.procno, 12);
	for (int i = 0; i < 2; i++)
		hash1 += lmd_vertex_identity_hash(&out[i]);
	first = live_edges[0];
	live_edges[0] = live_edges[2];
	live_edges[2] = first;
	UT_ASSERT_EQ(scan(out, &cycles), 2);
	UT_ASSERT(cluster_lmd_tarjan_pick_victim(out, 2, NULL, &victim));
	UT_ASSERT_EQ(victim.procno, 12);
	/* The same group may report a different waiting member next round. */
	live_edges[0].waiter.procno = 13;
	UT_ASSERT_EQ(scan(out, &cycles), 2);
	for (int i = 0; i < 2; i++)
		hash2 += lmd_vertex_identity_hash(&out[i]);
	UT_ASSERT_EQ(hash1, hash2);
}
UT_TEST(revalidation_requires_the_group_cycle_to_remain)
{
	ClusterLmdVertex out[8] = { 0 };
	int cycles;

	group_cycle();
	UT_ASSERT_EQ(scan(out, &cycles), 2);
	UT_ASSERT(cluster_lmd_tarjan_revalidate(out, 2, 100));
	live_edges[0].blocker = vertex(3, 30, 0, 2);
	UT_ASSERT(!cluster_lmd_tarjan_revalidate(out, 2, 100));
	UT_ASSERT(revalidate_failures > 0);
	UT_ASSERT(root.allocations == NULL);
}
UT_TEST(nonparallel_waiter_identity_and_exclusion_unchanged)
{
	ClusterLmdVertex out[8] = { 0 }, victim = { 0 };
	LmdVictimExcludeSet exclude;
	int cycles;

	group_cycle();
	live_edges[0].waiter.procno = 10;
	live_edges[0].waiter.lock_group_procno_plus_one = 0;
	UT_ASSERT_EQ(scan(out, &cycles), 2);
	UT_ASSERT(cluster_lmd_tarjan_pick_victim(out, 2, NULL, &victim));
	UT_ASSERT_EQ(victim.procno, 10);
	UT_ASSERT_EQ(victim.request_id, 90);
	cluster_lmd_victim_exclude_init(&exclude);
	UT_ASSERT(cluster_lmd_victim_exclude_add(&exclude, &victim));
	UT_ASSERT(cluster_lmd_tarjan_pick_victim(out, 2, &exclude, &victim));
	UT_ASSERT_EQ(victim.node_id, 2);
}
int
main(void)
{
	UT_PLAN(8);
	UT_RUN(worker_wait_and_leader_hold_close_cycle);
	UT_RUN(two_groups_with_nonwaiting_holder_members);
	UT_RUN(group_alias_does_not_create_self_cycle);
	UT_RUN(node_and_epoch_still_separate_groups);
	UT_RUN(victim_must_wait_on_an_edge_inside_the_cycle);
	UT_RUN(group_fingerprint_ignores_member_and_edge_order);
	UT_RUN(revalidation_requires_the_group_cycle_to_remain);
	UT_RUN(nonparallel_waiter_identity_and_exclusion_unchanged);
	UT_DONE();
	return ut_failed_count ? 1 : 0;
}
