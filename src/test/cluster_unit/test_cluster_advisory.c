/*-------------------------------------------------------------------------
 *
 * test_cluster_advisory.c
 *	  Standalone unit tests for spec-5.5 UL user lock (cross-node advisory).
 *
 *	  U7  — holder identity is backend-scoped (node, procno, epoch, request_id)
 *	        with NO transaction id, so a session-advisory holder lives across
 *	        transactions and cross-backend holders never collide (Q9 / L363).
 *	  U8  — cluster_advisory_locallock_is_session_scoped() derives the scope a
 *	        LOCALLOCK carries from its lock-owners (owner == NULL → session),
 *	        including the mixed session+xact same-key case (§3.2 L5 / D2 / D3).
 *	  U-counter — the D8 shmem observability counters (inc / read / bounds).
 *
 *	  Links cluster_advisory.o; ShmemInitStruct + region register stubbed
 *	  (L105 union force-align).  Release-path / backend-exit drain behaviour
 *	  is exercised end-to-end by cluster_tap t/130_ul_advisory.pl.
 *
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/test/cluster_unit/test_cluster_advisory.c
 *
 * NOTES
 *	  This is a pgrac-original file.  Spec: spec-5.5-ul-advisory-lock-cross-node.md
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "cluster/cluster_advisory.h"
#include "cluster/cluster_grd.h"
#include "storage/lock.h"

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


/* ============================================================
 * PG runtime stubs.
 * ============================================================ */

void
ExceptionalCondition(const char *conditionName pg_attribute_unused(),
					 const char *fileName pg_attribute_unused(),
					 int lineNumber pg_attribute_unused())
{
	abort();
}

/*
 * ShmemInitStruct stub — L105 union force-align (8-byte alignment for the
 * pg_atomic_uint64 counters inside ClusterAdvisorySharedState).
 */
void *
ShmemInitStruct(const char *name, Size size, bool *foundPtr)
{
	static union
	{
		uint64 force_align;
		char data[256]; /* cluster_advisory_shmem_size() << 256B */
	} adv_buf;
	static bool adv_initialized = false;

	if (name != NULL && strcmp(name, "pgrac cluster advisory") == 0)
	{
		Assert(size <= sizeof(adv_buf.data)); /* catch shmem layout growth */
		*foundPtr = adv_initialized;
		adv_initialized = true;
		return adv_buf.data;
	}

	*foundPtr = true;
	return NULL;
}

void
cluster_shmem_register_region(const void *r pg_attribute_unused())
{}


/* ============================================================
 * U7 — holder identity is backend-scoped, NOT xid-bound (Q9 / L363).
 * ============================================================ */
UT_TEST(test_ul_holder_identity_backend_scoped_no_xid)
{
	ClusterGrdHolderId h1;
	ClusterGrdHolderId h2;

	/* The GES holder identity 4-tuple is (node_id, procno, cluster_epoch,
	 * request_id) — 24 bytes, NO transaction id.  A session-advisory holder
	 * survives across transactions precisely because its identity carries no
	 * xid to invalidate; reconfig revoke is therefore backend/epoch-scoped. */
	UT_ASSERT_EQ((int)sizeof(ClusterGrdHolderId), 24);

	memset(&h1, 0, sizeof(h1));
	memset(&h2, 0, sizeof(h2));
	h1.node_id = 0;
	h1.procno = 10;
	h1.cluster_epoch = 1;
	h1.request_id = 100;
	/* Same node + same resid, different backend → distinct holder identity
	 * (cross-backend no collision). */
	h2 = h1;
	h2.procno = 11;
	UT_ASSERT(h1.procno != h2.procno);
	UT_ASSERT_EQ((int)(h1.node_id == h2.node_id), 1);
}


/* ============================================================
 * U8 — session-vs-xact scope derivation (§3.2 L5 / D2 / D3).
 * ============================================================ */
UT_TEST(test_ul_locallock_session_scope_derivation)
{
	LOCALLOCK ll;
	LOCALLOCKOWNER owners[3];
	int dummy_a;
	int dummy_b;
	struct ResourceOwnerData *xact_owner_a = (struct ResourceOwnerData *)&dummy_a;
	struct ResourceOwnerData *xact_owner_b = (struct ResourceOwnerData *)&dummy_b;

	memset(&ll, 0, sizeof(ll));
	memset(owners, 0, sizeof(owners));
	ll.lockOwners = owners;

	/* NULL locallock → false (defensive). */
	UT_ASSERT_EQ(cluster_advisory_locallock_is_session_scoped(NULL), false);

	/* Zero owners → false. */
	ll.numLockOwners = 0;
	UT_ASSERT_EQ(cluster_advisory_locallock_is_session_scoped(&ll), false);

	/* All xact owners (owner != NULL) → false (xact-scoped). */
	owners[0].owner = xact_owner_a;
	owners[1].owner = xact_owner_b;
	ll.numLockOwners = 2;
	UT_ASSERT_EQ(cluster_advisory_locallock_is_session_scoped(&ll), false);

	/* A session owner at index 0 (owner == NULL) → true (session-scoped). */
	owners[0].owner = NULL;
	owners[1].owner = xact_owner_b;
	ll.numLockOwners = 2;
	UT_ASSERT_EQ(cluster_advisory_locallock_is_session_scoped(&ll), true);

	/* Mixed: xact owner first, session owner second → still session-scoped
	 * (the session owner is what outlives the transaction, §3.2 L5). */
	owners[0].owner = xact_owner_a;
	owners[1].owner = NULL;
	ll.numLockOwners = 2;
	UT_ASSERT_EQ(cluster_advisory_locallock_is_session_scoped(&ll), true);
}


/* ============================================================
 * U-counter — D8 shmem observability counters.
 * ============================================================ */
UT_TEST(test_ul_counters_inc_read_bounds)
{
	cluster_advisory_shmem_init();

	/* All counters start at zero. */
	UT_ASSERT_EQ((int)cluster_advisory_counter_read(CLUSTER_ADVISORY_GLOBALIZE), 0);
	UT_ASSERT_EQ((int)cluster_advisory_counter_read(CLUSTER_ADVISORY_TRY_NOTAVAIL), 0);

	/* Independent accumulation per counter. */
	cluster_advisory_counter_inc(CLUSTER_ADVISORY_GLOBALIZE);
	cluster_advisory_counter_inc(CLUSTER_ADVISORY_GLOBALIZE);
	cluster_advisory_counter_inc(CLUSTER_ADVISORY_TRY_NOTAVAIL);
	UT_ASSERT_EQ((int)cluster_advisory_counter_read(CLUSTER_ADVISORY_GLOBALIZE), 2);
	UT_ASSERT_EQ((int)cluster_advisory_counter_read(CLUSTER_ADVISORY_TRY_NOTAVAIL), 1);
	/* Untouched counters stay zero. */
	UT_ASSERT_EQ((int)cluster_advisory_counter_read(CLUSTER_ADVISORY_SESSION_RELEASE), 0);
	UT_ASSERT_EQ((int)cluster_advisory_counter_read(CLUSTER_ADVISORY_FAILCLOSED), 0);

	/* Out-of-range index is a no-op / zero (bounds guard). */
	cluster_advisory_counter_inc(CLUSTER_ADVISORY_COUNTER_COUNT);
	UT_ASSERT_EQ((int)cluster_advisory_counter_read(CLUSTER_ADVISORY_COUNTER_COUNT), 0);
}


UT_DEFINE_GLOBALS();


int
main(int argc pg_attribute_unused(), char **const argv pg_attribute_unused())
{
	UT_PLAN(3);

	UT_RUN(test_ul_holder_identity_backend_scoped_no_xid);
	UT_RUN(test_ul_locallock_session_scope_derivation);
	UT_RUN(test_ul_counters_inc_read_bounds);

	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
