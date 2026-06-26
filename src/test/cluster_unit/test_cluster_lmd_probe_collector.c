/*-------------------------------------------------------------------------
 *
 * test_cluster_lmd_probe_collector.c
 *	  Standalone unit tests for the LMD cross-node REPORT collector
 *	  (spec-5.8 D8 / Option B) — the shared-memory LMON->LMD hand-off.
 *
 *	  These pin the Rule 8.A safety contract of the collector that LMON
 *	  appends into and LMD drains:
 *	    T1 complete:    an expected node's REPORT is accepted; its edges drain
 *	                    into the union, the member bitmap + n_received reflect
 *	                    it, report_enqueue_count advances.
 *	    T2 duplicate:   a second REPORT from the same node is dropped
 *	                    (drop_duplicate_count++), never double-counted.
 *	    T3 stale:       a REPORT for a different probe_id is dropped
 *	                    (drop_stale_count++).
 *	    T4 unexpected:  a REPORT from a node that was not probed is dropped
 *	                    (drop_stale_count++).
 *	    T5 overflow:    a REPORT whose edges do not fit marks the round
 *	                    OVERFLOW (queue_full + partial_report counters), no
 *	                    partial edges are appended, and the drain reports
 *	                    overflow so the coordinator discards the round.
 *	    T6 multi-node:  two expected nodes both report; edges concatenate and
 *	                    both appear in the member bitmap.
 *	    T7 reset/arm:   reset returns to idle; a fresh arm clears prior state.
 *
 *	  Harness: a fake ShmemInitStruct (union force-align per L105) backs the
 *	  region; LWLock is a no-op (single-threaded test); cluster_lmd_probe_
 *	  member_admit is a faithful local copy (its own logic is covered by
 *	  test_cluster_lmd_graph U3a-e).
 *
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/test/cluster_unit/test_cluster_lmd_probe_collector.c
 *
 * NOTES
 *	  pgrac-original file.  Standalone binary linking
 *	  cluster_lmd_probe_collector.o only.
 *	  Spec: spec-5.8-full-cross-node-deadlock-detector.md.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <string.h>

#include "cluster/cluster_ges.h" /* GesDeadlockReportHeader */
#include "cluster/cluster_lmd.h" /* ClusterLmdWaitEdge + ClusterLmdProbeAdmit */
#include "cluster/cluster_lmd_probe_collector.h"
#include "miscadmin.h" /* ProcessingMode / NormalProcessing (Mode global) */
#include "port/atomics.h"
#include "storage/lwlock.h"
#include "storage/shmem.h"

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
 * Stubs.
 * ============================================================ */

bool cluster_enabled = true;
int cluster_lmd_max_wait_edges = 64; /* small cap so the overflow test is cheap */
ProcessingMode Mode = NormalProcessing;

void
ExceptionalCondition(const char *c pg_attribute_unused(), const char *f pg_attribute_unused(),
					 int l pg_attribute_unused())
{
	abort();
}

void
LWLockInitialize(LWLock *l pg_attribute_unused(), int t pg_attribute_unused())
{}
bool
LWLockAcquire(LWLock *l pg_attribute_unused(), LWLockMode m pg_attribute_unused())
{
	return true;
}
void
LWLockRelease(LWLock *l pg_attribute_unused())
{}

void
cluster_shmem_register_region(const void *r pg_attribute_unused())
{}

/* Faithful copy of cluster_lmd_probe_member_admit (logic covered by
 * test_cluster_lmd_graph U3a-e); kept here so this binary need not link the
 * graph object. */
ClusterLmdProbeAdmit
cluster_lmd_probe_member_admit(uint64 expected_lo, uint64 expected_hi, uint64 received_lo,
							   uint64 received_hi, int32 node_id)
{
	uint64 bit, ew, rw;

	if (node_id < 0 || node_id >= 128)
		return CLUSTER_LMD_PROBE_DROP_UNEXPECTED;
	if (node_id < 64) {
		bit = UINT64CONST(1) << node_id;
		ew = expected_lo;
		rw = received_lo;
	} else {
		bit = UINT64CONST(1) << (node_id - 64);
		ew = expected_hi;
		rw = received_hi;
	}
	if (!(ew & bit))
		return CLUSTER_LMD_PROBE_DROP_UNEXPECTED;
	if (rw & bit)
		return CLUSTER_LMD_PROBE_DROP_DUPLICATE;
	return CLUSTER_LMD_PROBE_ADMIT;
}

/* Fake shmem region. */
static union {
	uint64 force_align;
	char data[64 * 1024]; /* header + 64 edges * 112 < 8KB; generous */
} probe_buf;
static bool probe_initialized = false;

void *
ShmemInitStruct(const char *name, Size size, bool *foundPtr)
{
	if (name != NULL && strcmp(name, "pgrac cluster lmd probe") == 0) {
		Assert(size <= sizeof(probe_buf.data));
		*foundPtr = probe_initialized;
		probe_initialized = true;
		return probe_buf.data;
	}
	*foundPtr = true;
	return NULL;
}

static void
reset_region(void)
{
	probe_initialized = false;
	memset(&probe_buf, 0, sizeof(probe_buf));
	cluster_lmd_probe_collector_shmem_init();
}


/* ============================================================
 * Test helpers.
 * ============================================================ */

#define MAX_TEST_EDGES 80

/* Build a REPORT (header + nedges edges) into buf; returns byte length. */
static Size
build_report(char *buf, uint64 probe_id, int32 responding_node, int nedges)
{
	GesDeadlockReportHeader *hdr = (GesDeadlockReportHeader *)buf;
	ClusterLmdWaitEdge *edges = (ClusterLmdWaitEdge *)(buf + sizeof(GesDeadlockReportHeader));

	memset(hdr, 0, sizeof(*hdr));
	hdr->responding_node_id = (uint32)responding_node;
	hdr->probe_id = probe_id;
	hdr->nedges = (uint32)nedges;

	for (int i = 0; i < nedges; i++) {
		memset(&edges[i], 0, sizeof(edges[i]));
		/* Distinct waiter identity per edge so the union is non-degenerate. */
		edges[i].waiter.node_id = responding_node;
		edges[i].waiter.procno = (uint32)(1000 + i);
		edges[i].waiter.request_id = (uint64)(5000 + i);
		edges[i].blocker.node_id = responding_node;
		edges[i].blocker.procno = (uint32)(2000 + i);
		edges[i].blocker.request_id = (uint64)(6000 + i);
	}
	return sizeof(GesDeadlockReportHeader) + (Size)nedges * sizeof(ClusterLmdWaitEdge);
}

/* Expected bitmap helper for a single low-word node. */
static uint64
exp_bit(int32 node)
{
	return (node < 64) ? (UINT64CONST(1) << node) : 0;
}


/* ============================================================
 * T1 — complete REPORT accepted + drained.
 * ============================================================ */

UT_TEST(test_probe_complete_report_accepted)
{
	char buf[sizeof(GesDeadlockReportHeader) + MAX_TEST_EDGES * sizeof(ClusterLmdWaitEdge)];
	Size len;
	ClusterLmdWaitEdge out[MAX_TEST_EDGES];
	ClusterLmdProbeDrain drain;
	int n;

	reset_region();
	cluster_lmd_probe_arm(42, exp_bit(1), 0);

	len = build_report(buf, 42, 1, 3);
	UT_ASSERT(cluster_lmd_probe_collect_receive((GesDeadlockReportHeader *)buf, len));
	UT_ASSERT_EQ(cluster_lmd_probe_n_received(), 1);
	UT_ASSERT_EQ(cluster_lmd_probe_report_enqueue_count_get(), 1ULL);

	n = cluster_lmd_probe_drain(out, MAX_TEST_EDGES, &drain);
	UT_ASSERT_EQ(n, 3);
	UT_ASSERT_EQ(drain.n_edges, 3);
	UT_ASSERT_EQ(drain.n_received, 1);
	UT_ASSERT(!drain.overflow);
	UT_ASSERT(drain.member_lo & exp_bit(1));
}


/* ============================================================
 * T2 — duplicate REPORT dropped.
 * ============================================================ */

UT_TEST(test_probe_duplicate_dropped)
{
	char buf[sizeof(GesDeadlockReportHeader) + MAX_TEST_EDGES * sizeof(ClusterLmdWaitEdge)];
	Size len;

	reset_region();
	cluster_lmd_probe_arm(7, exp_bit(2), 0);

	len = build_report(buf, 7, 2, 2);
	UT_ASSERT(cluster_lmd_probe_collect_receive((GesDeadlockReportHeader *)buf, len));
	/* Same node reports again — dropped, not double counted. */
	UT_ASSERT(!cluster_lmd_probe_collect_receive((GesDeadlockReportHeader *)buf, len));
	UT_ASSERT_EQ(cluster_lmd_probe_n_received(), 1);
	UT_ASSERT_EQ(cluster_lmd_probe_drop_duplicate_count_get(), 1ULL);
}


/* ============================================================
 * T3 — stale probe_id dropped.
 * ============================================================ */

UT_TEST(test_probe_stale_probe_id_dropped)
{
	char buf[sizeof(GesDeadlockReportHeader) + MAX_TEST_EDGES * sizeof(ClusterLmdWaitEdge)];
	Size len;

	reset_region();
	cluster_lmd_probe_arm(100, exp_bit(1), 0);

	len = build_report(buf, 999, 1, 1); /* wrong probe_id */
	UT_ASSERT(!cluster_lmd_probe_collect_receive((GesDeadlockReportHeader *)buf, len));
	UT_ASSERT_EQ(cluster_lmd_probe_n_received(), 0);
	UT_ASSERT_EQ(cluster_lmd_probe_drop_stale_count_get(), 1ULL);
}


/* ============================================================
 * T4 — unexpected node dropped.
 * ============================================================ */

UT_TEST(test_probe_unexpected_node_dropped)
{
	char buf[sizeof(GesDeadlockReportHeader) + MAX_TEST_EDGES * sizeof(ClusterLmdWaitEdge)];
	Size len;

	reset_region();
	cluster_lmd_probe_arm(5, exp_bit(1), 0); /* only node 1 expected */

	len = build_report(buf, 5, 9, 1); /* node 9 was never probed */
	UT_ASSERT(!cluster_lmd_probe_collect_receive((GesDeadlockReportHeader *)buf, len));
	UT_ASSERT_EQ(cluster_lmd_probe_n_received(), 0);
	UT_ASSERT_EQ(cluster_lmd_probe_drop_stale_count_get(), 1ULL);
}


/* ============================================================
 * T5 — overflow: a REPORT bigger than the ring marks the round incomplete.
 * ============================================================ */

UT_TEST(test_probe_overflow_marks_incomplete)
{
	char buf[sizeof(GesDeadlockReportHeader) + MAX_TEST_EDGES * sizeof(ClusterLmdWaitEdge)];
	Size len;
	ClusterLmdWaitEdge out[MAX_TEST_EDGES];
	ClusterLmdProbeDrain drain;

	reset_region();
	cluster_lmd_probe_arm(11, exp_bit(1), 0);

	/* Ring cap = cluster_lmd_max_wait_edges (64); send 70 edges → does not fit. */
	len = build_report(buf, 11, 1, 70);
	UT_ASSERT(!cluster_lmd_probe_collect_receive((GesDeadlockReportHeader *)buf, len));
	UT_ASSERT_EQ(cluster_lmd_probe_queue_full_count_get(), 1ULL);
	UT_ASSERT_EQ(cluster_lmd_probe_partial_report_count_get(), 1ULL);

	(void)cluster_lmd_probe_drain(out, MAX_TEST_EDGES, &drain);
	UT_ASSERT(drain.overflow);
	/* No partial edges were appended. */
	UT_ASSERT_EQ(drain.n_edges, 0);
}


/* ============================================================
 * T6 — two expected nodes both report; edges concatenate.
 * ============================================================ */

UT_TEST(test_probe_multi_node_union)
{
	char buf[sizeof(GesDeadlockReportHeader) + MAX_TEST_EDGES * sizeof(ClusterLmdWaitEdge)];
	Size len;
	ClusterLmdWaitEdge out[MAX_TEST_EDGES];
	ClusterLmdProbeDrain drain;
	int n;

	reset_region();
	cluster_lmd_probe_arm(3, exp_bit(1) | exp_bit(2), 0);

	len = build_report(buf, 3, 1, 4);
	UT_ASSERT(cluster_lmd_probe_collect_receive((GesDeadlockReportHeader *)buf, len));
	len = build_report(buf, 3, 2, 5);
	UT_ASSERT(cluster_lmd_probe_collect_receive((GesDeadlockReportHeader *)buf, len));

	UT_ASSERT_EQ(cluster_lmd_probe_n_received(), 2);
	n = cluster_lmd_probe_drain(out, MAX_TEST_EDGES, &drain);
	UT_ASSERT_EQ(n, 9); /* 4 + 5 */
	UT_ASSERT(!drain.overflow);
	UT_ASSERT(drain.member_lo & exp_bit(1));
	UT_ASSERT(drain.member_lo & exp_bit(2));
}


/* ============================================================
 * T7 — reset returns to idle; a stale-after-reset REPORT is dropped.
 * ============================================================ */

UT_TEST(test_probe_reset_to_idle)
{
	char buf[sizeof(GesDeadlockReportHeader) + MAX_TEST_EDGES * sizeof(ClusterLmdWaitEdge)];
	Size len;

	reset_region();
	cluster_lmd_probe_arm(8, exp_bit(1), 0);
	len = build_report(buf, 8, 1, 2);
	UT_ASSERT(cluster_lmd_probe_collect_receive((GesDeadlockReportHeader *)buf, len));

	cluster_lmd_probe_reset();
	UT_ASSERT_EQ(cluster_lmd_probe_n_received(), 0);
	/* Idle (probe_id 0) — any REPORT is now stale. */
	UT_ASSERT(!cluster_lmd_probe_collect_receive((GesDeadlockReportHeader *)buf, len));
}


int
main(void)
{
	UT_PLAN(7);
	UT_RUN(test_probe_complete_report_accepted);
	UT_RUN(test_probe_duplicate_dropped);
	UT_RUN(test_probe_stale_probe_id_dropped);
	UT_RUN(test_probe_unexpected_node_dropped);
	UT_RUN(test_probe_overflow_marks_incomplete);
	UT_RUN(test_probe_multi_node_union);
	UT_RUN(test_probe_reset_to_idle);
	UT_DONE();
	return 0;
}
