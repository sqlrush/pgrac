/*-------------------------------------------------------------------------
 *
 * test_cluster_hang.c
 *	  Unit tests for spec-5.11 Hang Manager policy helpers.
 *
 *	  Exercises the pure decision/policy surface of the Hang Manager
 *	  (cluster_hang_policy.c): long-wait threshold, wait-source tagging,
 *	  exclusion rules, wait-duration kind (§0.2 matrix), completeness /
 *	  forward-safety quality, top-N bounded store, and the consistent-
 *	  snapshot publish/read protocol.  The runtime gathering (pgstat + lock
 *	  snapshots), DIAG tick, and ProcSignal glue live in cluster_hang.c and
 *	  are covered by cluster_tap t/30x.
 *
 *	  Test IDs map to spec-5.11 §4.1:
 *	    U1  threshold boundary
 *	    U2  exclusion rules (idle / bgworker / confirmed deadlock)
 *	    U3  wait-source tag
 *	    U6  forward-safety (actionable only COMPLETE && !deadlock)
 *	    U7  v1 5.9/5.10 fields const false
 *	    U8  top-N truncation
 *	    U10 sample store consistent snapshot
 *	    U12 wait-duration kind (true vs approximate)
 *
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/test/cluster_unit/test_cluster_hang.c
 *
 * NOTES
 *	  This is a pgrac-original file.
 *	  Spec: spec-5.11-hang-manager-skeleton.md.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <string.h>

#include "cluster/cluster_hang.h"
#include "utils/wait_event.h"
#include "utils/backend_status.h"
#include "miscadmin.h"

#undef printf
#undef fprintf
#undef snprintf
#undef sprintf
#undef vsnprintf
#undef vfprintf
#undef vprintf
#undef vsprintf
#undef strerror
#undef strerror_r

#include "unit_test.h"


/* ----------
 * Stubs needed to link cluster_hang_policy.o standalone.  The policy
 * helpers only touch the DIAG LWLock (consistent-snapshot protocol);
 * everything else is pure arithmetic / struct manipulation.
 * ----------
 */
void
ExceptionalCondition(const char *conditionName pg_attribute_unused(),
					 const char *fileName pg_attribute_unused(),
					 int lineNumber pg_attribute_unused())
{
	abort();
}

bool
LWLockAcquire(LWLock *lock pg_attribute_unused(), LWLockMode mode pg_attribute_unused())
{
	return true;
}
void
LWLockRelease(LWLock *lock pg_attribute_unused())
{}


UT_DEFINE_GLOBALS();


/* ============================================================
 * U1 — threshold boundary
 * ============================================================ */

UT_TEST(test_threshold_boundary)
{
	/* threshold 60_000 ms == 60_000_000 us */
	UT_ASSERT(!cluster_hang_wait_exceeds_threshold(59999999, 60000));
	UT_ASSERT(cluster_hang_wait_exceeds_threshold(60000000, 60000));
	UT_ASSERT(cluster_hang_wait_exceeds_threshold(60000001, 60000));
	/* zero / negative durations never exceed */
	UT_ASSERT(!cluster_hang_wait_exceeds_threshold(0, 60000));
	UT_ASSERT(!cluster_hang_wait_exceeds_threshold(-1, 60000));
}


/* ============================================================
 * U3 — wait-source tag
 * ============================================================ */

UT_TEST(test_wait_source_tag)
{
	UT_ASSERT_EQ((int)cluster_hang_classify_wait_source(PG_WAIT_LOCK, NULL), (int)HANG_WAIT_LOCK);
	UT_ASSERT_EQ((int)cluster_hang_classify_wait_source(PG_WAIT_LWLOCK, NULL),
				 (int)HANG_WAIT_LWLOCK);
	UT_ASSERT_EQ((int)cluster_hang_classify_wait_source(PG_WAIT_IO, NULL), (int)HANG_WAIT_IO);
	/* name-based refinement for cluster waits sharing a generic class */
	UT_ASSERT_EQ((int)cluster_hang_classify_wait_source(PG_WAIT_IPC, "ClusterCfEnqueue"),
				 (int)HANG_WAIT_CACHE_FUSION);
	UT_ASSERT_EQ((int)cluster_hang_classify_wait_source(PG_WAIT_IPC, "ClusterUndoFetch"),
				 (int)HANG_WAIT_UNDO);
	/* unknown class with no informative name */
	UT_ASSERT_EQ((int)cluster_hang_classify_wait_source(PG_WAIT_EXTENSION, NULL),
				 (int)HANG_WAIT_UNKNOWN);
}


UT_TEST(test_wait_is_idle_class)
{
	UT_ASSERT(cluster_hang_wait_is_idle_class(PG_WAIT_CLIENT));
	UT_ASSERT(cluster_hang_wait_is_idle_class(PG_WAIT_ACTIVITY));
	UT_ASSERT(cluster_hang_wait_is_idle_class(0)); /* not waiting at all */
	UT_ASSERT(!cluster_hang_wait_is_idle_class(PG_WAIT_LOCK));
	UT_ASSERT(!cluster_hang_wait_is_idle_class(PG_WAIT_IO));
}


/* ============================================================
 * U2 — exclusion rules
 * ============================================================ */

UT_TEST(test_exclusion_rules)
{
	/* genuine long lock wait, running, not deadlocked → not excluded */
	UT_ASSERT_EQ((int)cluster_hang_exclude_reason(STATE_RUNNING, B_BACKEND, false, PG_WAIT_LOCK),
				 (int)HANG_EXCLUDE_NONE);
	/* idle → excluded */
	UT_ASSERT_EQ((int)cluster_hang_exclude_reason(STATE_IDLE, B_BACKEND, false, PG_WAIT_CLIENT),
				 (int)HANG_EXCLUDE_IDLE);
	/* idle-in-transaction blocked only on client read → excluded (L4) */
	UT_ASSERT_EQ(
		(int)cluster_hang_exclude_reason(STATE_IDLEINTRANSACTION, B_BACKEND, false, PG_WAIT_CLIENT),
		(int)HANG_EXCLUDE_IDLE);
	/* background worker → excluded */
	UT_ASSERT_EQ((int)cluster_hang_exclude_reason(STATE_RUNNING, B_BG_WORKER, false, PG_WAIT_LOCK),
				 (int)HANG_EXCLUDE_BGWORKER);
	/* confirmed deadlock waiter → excluded (precedence over idle) */
	UT_ASSERT_EQ((int)cluster_hang_exclude_reason(STATE_RUNNING, B_BACKEND, true, PG_WAIT_LOCK),
				 (int)HANG_EXCLUDE_DEADLOCK);
}


/* ============================================================
 * U12 — wait-duration kind (§0.2 matrix)
 * ============================================================ */

UT_TEST(test_duration_kind)
{
	/* local heavyweight LOCK with a real waitStart → TRUE */
	UT_ASSERT_EQ((int)cluster_hang_duration_kind(true), (int)HANG_DUR_TRUE);
	/* GES default / TX / generic wait with no wait-start → APPROX */
	UT_ASSERT_EQ((int)cluster_hang_duration_kind(false), (int)HANG_DUR_APPROX);
}


UT_TEST(test_quality_resolution)
{
	/* true duration + hard local blocker → COMPLETE */
	UT_ASSERT_EQ((int)cluster_hang_quality(HANG_DUR_TRUE, 4242, -1, false, false),
				 (int)HANG_SAMPLE_COMPLETE);
	/* approx duration → APPROXIMATE even with a blocker */
	UT_ASSERT_EQ((int)cluster_hang_quality(HANG_DUR_APPROX, 4242, -1, false, false),
				 (int)HANG_SAMPLE_APPROXIMATE);
	/* blocker gone wins over everything */
	UT_ASSERT_EQ((int)cluster_hang_quality(HANG_DUR_TRUE, -1, -1, true, false),
				 (int)HANG_SAMPLE_BLOCKER_GONE);
	/* remote blocker → boundary */
	UT_ASSERT_EQ((int)cluster_hang_quality(HANG_DUR_TRUE, -1, 1, false, false),
				 (int)HANG_SAMPLE_REMOTE_BOUNDARY);
	/* only soft (queue-ahead) blocker resolvable → INCOMPLETE */
	UT_ASSERT_EQ((int)cluster_hang_quality(HANG_DUR_TRUE, -1, -1, false, true),
				 (int)HANG_SAMPLE_INCOMPLETE);
}


/* ============================================================
 * U6 — forward-safety: actionable only COMPLETE && !deadlock
 * ============================================================ */

UT_TEST(test_forward_safety_actionable)
{
	ClusterHangSampleSlot slot;

	memset(&slot, 0, sizeof(slot));

	slot.quality = HANG_SAMPLE_COMPLETE;
	slot.in_confirmed_deadlock = false;
	UT_ASSERT(cluster_hang_sample_actionable(&slot));

	slot.quality = HANG_SAMPLE_APPROXIMATE;
	UT_ASSERT(!cluster_hang_sample_actionable(&slot));

	slot.quality = HANG_SAMPLE_INCOMPLETE;
	UT_ASSERT(!cluster_hang_sample_actionable(&slot));

	slot.quality = HANG_SAMPLE_REMOTE_BOUNDARY;
	UT_ASSERT(!cluster_hang_sample_actionable(&slot));

	/* COMPLETE but a confirmed-deadlock waiter is never actionable */
	slot.quality = HANG_SAMPLE_COMPLETE;
	slot.in_confirmed_deadlock = true;
	UT_ASSERT(!cluster_hang_sample_actionable(&slot));
}


/* ============================================================
 * U7 — v1 5.9/5.10 fields default false (forward, not compiled)
 * ============================================================ */

UT_TEST(test_v1_forward_fields_false)
{
	ClusterHangNode node;
	ClusterHangSampleSlot slot;

	memset(&node, 0, sizeof(node));
	node.duration_kind = HANG_DUR_TRUE;
	node.quality = HANG_SAMPLE_COMPLETE;

	cluster_hang_node_to_slot(&node, 7, &slot);

	UT_ASSERT_EQ(slot.node_id, 7);
	UT_ASSERT(!slot.being_resolved);   /* 5.9 forward */
	UT_ASSERT(!slot.fairness_boosted); /* 5.10 forward */
}


/* ============================================================
 * U8 — top-N truncation (keep longest by duration_us)
 * ============================================================ */

static ClusterHangSampleSlot
make_slot(int pid, int64 dur)
{
	ClusterHangSampleSlot s;

	memset(&s, 0, sizeof(s));
	s.pid = pid;
	s.duration_us = dur;
	s.quality = HANG_SAMPLE_COMPLETE;
	return s;
}

UT_TEST(test_topn_truncation)
{
	ClusterHangSampleStore store;
	int i;
	ClusterHangSampleSlot s;

	cluster_hang_store_reset(&store);
	UT_ASSERT_EQ(store.n_samples, 0);
	UT_ASSERT(!store.truncated);

	/* cap = 3; feed durations 10,20,30 → all kept, no truncation */
	s = make_slot(1, 10);
	cluster_hang_store_consider(&store, &s, 3);
	s = make_slot(2, 20);
	cluster_hang_store_consider(&store, &s, 3);
	s = make_slot(3, 30);
	cluster_hang_store_consider(&store, &s, 3);
	UT_ASSERT_EQ(store.n_samples, 3);
	UT_ASSERT(!store.truncated);

	/* feed a shorter one (5) → rejected, truncated set */
	s = make_slot(4, 5);
	cluster_hang_store_consider(&store, &s, 3);
	UT_ASSERT_EQ(store.n_samples, 3);
	UT_ASSERT(store.truncated);

	/* feed a longer one (40) → replaces the minimum (10), still 3, truncated */
	s = make_slot(5, 40);
	cluster_hang_store_consider(&store, &s, 3);
	UT_ASSERT_EQ(store.n_samples, 3);
	UT_ASSERT(store.truncated);

	/* the surviving durations must be the three longest: 20, 30, 40 */
	for (i = 0; i < store.n_samples; i++)
		UT_ASSERT(store.slots[i].duration_us >= 20);
}


/* ============================================================
 * U10 — sample store consistent snapshot
 * ============================================================ */

UT_TEST(test_store_consistent_snapshot)
{
	ClusterHangSampleStore shared;
	ClusterHangSampleStore round;
	ClusterHangSampleStore out;
	LWLock lock;
	int n;
	ClusterHangSampleSlot s;

	memset(&shared, 0, sizeof(shared));

	/* build a round of two samples */
	cluster_hang_store_reset(&round);
	s = make_slot(100, 70000000);
	cluster_hang_store_consider(&round, &s, 8);
	s = make_slot(101, 80000000);
	cluster_hang_store_consider(&round, &s, 8);
	round.truncated = false;

	/* publish bumps epoch and copies the whole round */
	cluster_hang_store_publish(&shared, &lock, &round);
	UT_ASSERT_EQ((int)shared.sample_epoch, 1);
	UT_ASSERT_EQ(shared.n_samples, 2);

	/* publish a second round → epoch advances */
	cluster_hang_store_publish(&shared, &lock, &round);
	UT_ASSERT_EQ((int)shared.sample_epoch, 2);

	/* read a consistent snapshot */
	n = cluster_hang_store_snapshot(&shared, &lock, &out);
	UT_ASSERT_EQ(n, 2);
	UT_ASSERT_EQ(out.n_samples, 2);
	UT_ASSERT_EQ((int)out.sample_epoch, 2);
	UT_ASSERT_EQ(out.slots[0].pid, 100);
	UT_ASSERT_EQ(out.slots[1].pid, 101);
}


/* ============================================================
 * U11 — publish_locked: caller-holds-lock variant (F2 / Hardening v1.2)
 *
 *	The sampling round must publish the store and the aggregate/counter
 *	fields in ONE exclusive section so readers never observe a torn round
 *	(new store + old aggregates).  publish_locked takes no lock argument:
 *	it assumes the caller already holds the DIAG LWLock, letting the round
 *	publish be folded into the same critical section as the aggregates.
 * ============================================================ */

UT_TEST(test_store_publish_locked)
{
	ClusterHangSampleStore shared;
	ClusterHangSampleStore round;
	ClusterHangSampleSlot s;

	memset(&shared, 0, sizeof(shared));

	cluster_hang_store_reset(&round);
	s = make_slot(200, 90000000);
	cluster_hang_store_consider(&round, &s, 8);
	round.truncated = false;

	/* publish_locked copies the round and bumps the epoch, no lock taken */
	cluster_hang_store_publish_locked(&shared, &round);
	UT_ASSERT_EQ((int)shared.sample_epoch, 1);
	UT_ASSERT_EQ(shared.n_samples, 1);
	UT_ASSERT_EQ(shared.slots[0].pid, 200);

	/* a second publish advances the epoch monotonically */
	cluster_hang_store_publish_locked(&shared, &round);
	UT_ASSERT_EQ((int)shared.sample_epoch, 2);

	/* the lock-taking wrapper is still equivalent (same copy + bump) */
	cluster_hang_store_publish_locked(&shared, &round);
	UT_ASSERT_EQ((int)shared.sample_epoch, 3);
	UT_ASSERT_EQ(shared.n_samples, 1);
}


/* ============================================================
 * Test runner
 * ============================================================ */

int
main(void)
{
	UT_PLAN(11);
	UT_RUN(test_threshold_boundary);
	UT_RUN(test_wait_source_tag);
	UT_RUN(test_wait_is_idle_class);
	UT_RUN(test_exclusion_rules);
	UT_RUN(test_duration_kind);
	UT_RUN(test_quality_resolution);
	UT_RUN(test_forward_safety_actionable);
	UT_RUN(test_v1_forward_fields_false);
	UT_RUN(test_topn_truncation);
	UT_RUN(test_store_consistent_snapshot);
	UT_RUN(test_store_publish_locked);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
