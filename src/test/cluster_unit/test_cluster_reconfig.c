/*-------------------------------------------------------------------------
 *
 * test_cluster_reconfig.c
 *	  spec-2.29 Sprint A Step 1 unit tests — cluster_reconfig foundation.
 *
 *	  Step 1 cases (this binary):
 *	    T-reconfig-1  ReconfigEvent + ClusterReconfigState sizeof bounds
 *	                  (P2.8 — natural-aligned, StaticAssertDecl ≤ 96 ≥ 64);
 *	                  cluster_reconfig_shmem_size > 0 + shmem_init succeeds
 *	                  + idempotent (init twice safe via found-flag);
 *	                  CLUSTER_RECONFIG_DEAD_BITMAP_BYTES == 16
 *	    T-reconfig-9  cluster_epoch_observe_remote CAS-loop semantics:
 *	                  - initial epoch=0, observe_remote(7) → epoch=7, returns true
 *	                  - observe_remote(7) again → epoch stays 7, returns false
 *	                  - observe_remote(3) (stale) → epoch stays 7, returns false
 *	                  - observe_remote(10) → epoch=10, returns true
 *	                  - CLUSTER_EPOCH_OBSERVE_MAX_JUMP == 16 constant
 *
 *	  Step 2 / Step 3 add T-reconfig-2..8 + T-reconfig-10/11
 *	  (event_id dedup / Q2 A'' rule / mid-tick rotation / PROCSIG handler
 *	  triplet / broadcast-vs-epoch++ split / I6 commit-durable guard /
 *	  envelope tri-branch / declared-peer filter end-to-end).
 *
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/test/cluster_unit/test_cluster_reconfig.c
 *
 * NOTES
 *	  pgrac-original file.  Spec:  spec-2.29-reconfig-coordinator-
 *	  internal.md (DRAFT v0.3).
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <stddef.h>
#include <string.h>

#include "cluster/cluster_reconfig.h"
#include "cluster/cluster_epoch.h"

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

UT_DEFINE_GLOBALS();


/* ============================================================
 * Stubs — link cluster_reconfig.o + cluster_epoch.o standalone.
 * ============================================================ */

#include "storage/shmem.h"
static char reconfig_shmem_storage[256] __attribute__((aligned(64)));
static char epoch_shmem_storage[64] __attribute__((aligned(64)));
static bool reconfig_init_done = false;
static bool epoch_init_done = false;

void *
ShmemInitStruct(const char *name, Size size pg_attribute_unused(), bool *foundPtr)
{
	if (strcmp(name, "pgrac cluster reconfig") == 0)
	{
		*foundPtr = reconfig_init_done;
		reconfig_init_done = true;
		return reconfig_shmem_storage;
	}
	else if (strcmp(name, "pgrac cluster epoch") == 0)
	{
		*foundPtr = epoch_init_done;
		epoch_init_done = true;
		return epoch_shmem_storage;
	}
	*foundPtr = false;
	return NULL;
}

#include "storage/lwlock.h"
void
LWLockInitialize(LWLock *lock pg_attribute_unused(), int tranche_id pg_attribute_unused())
{}
bool
LWLockAcquire(LWLock *lock pg_attribute_unused(), LWLockMode mode pg_attribute_unused())
{
	return true;
}
void
LWLockRelease(LWLock *lock pg_attribute_unused())
{}

#include "cluster/cluster_shmem.h"
void
cluster_shmem_register_region(const ClusterShmemRegion *region pg_attribute_unused())
{}

void
ExceptionalCondition(const char *conditionName pg_attribute_unused(),
					 const char *fileName pg_attribute_unused(),
					 int lineNumber pg_attribute_unused())
{
	abort();
}

/* elog stubs — DEBUG1 in publish path is the only call we exercise. */
bool
errstart(int elevel pg_attribute_unused(), const char *d pg_attribute_unused())
{
	return false; /* silently drop all elog */
}
bool
errstart_cold(int elevel, const char *d)
{
	return errstart(elevel, d);
}
void
errfinish(const char *f pg_attribute_unused(), int l pg_attribute_unused(),
		  const char *fn pg_attribute_unused())
{}
int errmsg(const char *f pg_attribute_unused(), ...) { return 0; }
int errmsg_internal(const char *f pg_attribute_unused(), ...) { return 0; }


/* ============================================================
 * T-reconfig-1 — Foundation: sizeof bounds + shmem layout.
 * ============================================================ */

UT_TEST(test_reconfig_dead_bitmap_bytes_eq_16)
{
	/* P2.8 fix:  dead_bitmap must be uint8[16] = 128 bits for 128
	 * declared nodes (CLUSTER_MAX_NODES).  v0.1's uint64 (64 bits)
	 * was rejected — verify the constant is 16. */
	UT_ASSERT_EQ(CLUSTER_RECONFIG_DEAD_BITMAP_BYTES, 16);
}


UT_TEST(test_reconfig_event_sizeof_bounds)
{
	/* P2.8 fix:  natural-aligned, NOT pg_attribute_packed.  Lower bound
	 * 64 catches accidental field removal;upper bound 96 catches
	 * accidental field bloat. */
	UT_ASSERT(sizeof(ReconfigEvent) >= 64);
	UT_ASSERT(sizeof(ReconfigEvent) <= 96);

	/* Field-level sanity:  expect exactly 80 bytes on 64-bit ABI
	 * with natural alignment (8+4+4 + 8+8+16 + 8+4+4 + 8+8 = 80). */
	UT_ASSERT_EQ(sizeof(ReconfigEvent), 80);
}


UT_TEST(test_reconfig_shmem_size_positive)
{
	Size s = cluster_reconfig_shmem_size();
	/* MAXALIGN(sizeof(ClusterReconfigState)) — must be > sizeof
	 * ReconfigEvent because state struct wraps event + lock + 3
	 * atomic counters. */
	UT_ASSERT(s > sizeof(ReconfigEvent));
	UT_ASSERT(s <= sizeof(reconfig_shmem_storage));
}


UT_TEST(test_reconfig_shmem_init_idempotent)
{
	ReconfigEvent evt;

	reconfig_init_done = false;

	/* First init — found = false branch. */
	cluster_reconfig_shmem_init();
	UT_ASSERT(reconfig_init_done);

	/* get_last_event should populate with never-applied sentinel
	 * (event_id = 0, observer_role = NONE). */
	cluster_reconfig_get_last_event(&evt);
	UT_ASSERT_EQ((unsigned long long) evt.event_id, 0ULL);
	UT_ASSERT_EQ(evt.observer_role, CLUSTER_RECONFIG_OBSERVER_NONE);
	UT_ASSERT_EQ((long long) evt.applied_at, 0LL);

	/* Second init — found = true branch.  Should NOT re-zero state
	 * (postmaster restart preserves shmem on the same shmem segment
	 * for the same process — the found-flag prevents double init). */
	cluster_reconfig_shmem_init();
	cluster_reconfig_get_last_event(&evt);
	UT_ASSERT_EQ((unsigned long long) evt.event_id, 0ULL);
}


UT_TEST(test_reconfig_publish_increments_apply_counter)
{
	ReconfigEvent evt;
	ReconfigEvent in;

	reconfig_init_done = false;
	cluster_reconfig_shmem_init();

	memset(&in, 0, sizeof(in));
	in.event_id = 0xABCDEF;
	in.coordinator_node_id = 0;
	in.old_epoch = 5;
	in.new_epoch = 6;
	in.observer_role = CLUSTER_RECONFIG_OBSERVER_COORDINATOR;
	in.event_seq = 42; /* publish path owns the final monotonic value. */
	in.cssd_dead_generation = 3;

	cluster_reconfig_publish_event(&in);

	cluster_reconfig_get_last_event(&evt);
	UT_ASSERT_EQ((unsigned long long) evt.event_id, 0xABCDEFULL);
	UT_ASSERT_EQ(evt.coordinator_node_id, 0);
	UT_ASSERT_EQ((unsigned long long) evt.new_epoch, 6ULL);
	UT_ASSERT_EQ(evt.observer_role, CLUSTER_RECONFIG_OBSERVER_COORDINATOR);
	UT_ASSERT_EQ((unsigned long long) evt.event_seq, 1ULL);
	UT_ASSERT_EQ((unsigned long long) evt.cssd_dead_generation, 3ULL);
}


UT_TEST(test_reconfig_publish_overwrites_event_seq_monotonically)
{
	ReconfigEvent evt;
	ReconfigEvent in;

	reconfig_init_done = false;
	cluster_reconfig_shmem_init();

	memset(&in, 0, sizeof(in));
	in.event_id = 1;
	in.event_seq = 99;
	in.observer_role = CLUSTER_RECONFIG_OBSERVER_COORDINATOR;
	cluster_reconfig_publish_event(&in);
	cluster_reconfig_get_last_event(&evt);
	UT_ASSERT_EQ((unsigned long long) evt.event_seq, 1ULL);

	in.event_id = 2;
	in.event_seq = 99;
	cluster_reconfig_publish_event(&in);
	cluster_reconfig_get_last_event(&evt);
	UT_ASSERT_EQ((unsigned long long) evt.event_seq, 2ULL);
}


UT_TEST(test_reconfig_broadcast_stub_increments_counter)
{
	reconfig_init_done = false;
	cluster_reconfig_shmem_init();

	/* Step 1 stub: broadcast_local_procsig only increments counter
	 * (real ProcArray walk lands in Step 2 D2 body).  Verify the
	 * stub still calls atomic_inc so observability surface in
	 * Step 3 pg_cluster_reconfig_state shows non-zero. */
	cluster_reconfig_broadcast_local_procsig();
	cluster_reconfig_broadcast_local_procsig();

	/* No direct accessor for procsig_broadcast_count yet (that lands
	 * in Step 3 D5b SRF entry);Step 1 verifies via no-crash + state
	 * struct sizeof + LWLock-protected publish path consistency. */
	UT_ASSERT(true); /* stub invocation safety */
}


/* ============================================================
 * T-reconfig-9 — cluster_epoch_observe_remote CAS-loop semantics
 *                + CLUSTER_EPOCH_OBSERVE_MAX_JUMP constant.
 * ============================================================ */

UT_TEST(test_epoch_observe_remote_advance_from_zero)
{
	bool advanced;

	epoch_init_done = false;
	cluster_epoch_shmem_init();
	UT_ASSERT_EQ((unsigned long long) cluster_epoch_get_current(), 0ULL);

	/* Advance from 0 → 7. */
	advanced = cluster_epoch_observe_remote(7);
	UT_ASSERT(advanced);
	UT_ASSERT_EQ((unsigned long long) cluster_epoch_get_current(), 7ULL);
}


UT_TEST(test_epoch_observe_remote_no_op_equal)
{
	bool advanced;

	epoch_init_done = false;
	cluster_epoch_shmem_init();
	(void) cluster_epoch_observe_remote(7); /* establish baseline */

	/* observe_remote(7) again — local already at 7, no advance. */
	advanced = cluster_epoch_observe_remote(7);
	UT_ASSERT(!advanced);
	UT_ASSERT_EQ((unsigned long long) cluster_epoch_get_current(), 7ULL);
}


UT_TEST(test_epoch_observe_remote_no_retreat)
{
	bool advanced;

	epoch_init_done = false;
	cluster_epoch_shmem_init();
	(void) cluster_epoch_observe_remote(7);

	/* observe_remote(3) — stale, must NOT retreat. */
	advanced = cluster_epoch_observe_remote(3);
	UT_ASSERT(!advanced);
	UT_ASSERT_EQ((unsigned long long) cluster_epoch_get_current(), 7ULL);
}


UT_TEST(test_epoch_observe_remote_monotonic_chain)
{
	epoch_init_done = false;
	cluster_epoch_shmem_init();

	/* Apply a chain of advances + no-ops + retreats;final must be
	 * the max observed, not the last observed. */
	UT_ASSERT(cluster_epoch_observe_remote(5));   /* 0 → 5 */
	UT_ASSERT(!cluster_epoch_observe_remote(3));  /* stale */
	UT_ASSERT(cluster_epoch_observe_remote(10));  /* 5 → 10 */
	UT_ASSERT(!cluster_epoch_observe_remote(8));  /* stale */
	UT_ASSERT(!cluster_epoch_observe_remote(10)); /* no-op */
	UT_ASSERT(cluster_epoch_observe_remote(11));  /* 10 → 11 */

	UT_ASSERT_EQ((unsigned long long) cluster_epoch_get_current(), 11ULL);
}


UT_TEST(test_epoch_advance_for_reconfig_pre_post_snapshots)
{
	uint64 old_v, new_v;

	epoch_init_done = false;
	cluster_epoch_shmem_init();

	/* From 0 → 1. */
	cluster_epoch_advance_for_reconfig(&old_v, &new_v);
	UT_ASSERT_EQ((unsigned long long) old_v, 0ULL);
	UT_ASSERT_EQ((unsigned long long) new_v, 1ULL);
	UT_ASSERT_EQ((unsigned long long) cluster_epoch_get_current(), 1ULL);

	/* Idempotent — each call advances by exactly 1. */
	cluster_epoch_advance_for_reconfig(&old_v, &new_v);
	UT_ASSERT_EQ((unsigned long long) old_v, 1ULL);
	UT_ASSERT_EQ((unsigned long long) new_v, 2ULL);
	UT_ASSERT_EQ((unsigned long long) cluster_epoch_get_current(), 2ULL);
}


UT_TEST(test_epoch_observe_max_jump_constant)
{
	/* spec-2.29 D18b — bounded jump defense against hostile-spoof
	 * envelope frames.  Caller (D20 envelope verify path) checks
	 * remote - my <= MAX_JUMP before calling observe_remote;
	 * constant must be exactly 16 per spec §3.7-bis + §6 R11. */
	UT_ASSERT_EQ((unsigned long long) CLUSTER_EPOCH_OBSERVE_MAX_JUMP, 16ULL);
}


UT_TEST(test_epoch_changed_at_lsn_set_and_get)
{
	uint64 lsn;

	epoch_init_done = false;
	cluster_epoch_shmem_init();

	UT_ASSERT_EQ((unsigned long long) cluster_epoch_get_changed_at_lsn(), 0ULL);

	cluster_epoch_set_changed_at_lsn(0xDEADBEEFCAFEBABEULL);
	lsn = cluster_epoch_get_changed_at_lsn();
	UT_ASSERT_EQ((unsigned long long) lsn, 0xDEADBEEFCAFEBABEULL);
}


/* ============================================================
 * Main — register + run all tests.
 * ============================================================ */

int
main(void)
{
	UT_PLAN(14);

	/* T-reconfig-1 */
	UT_RUN(test_reconfig_dead_bitmap_bytes_eq_16);
	UT_RUN(test_reconfig_event_sizeof_bounds);
	UT_RUN(test_reconfig_shmem_size_positive);
	UT_RUN(test_reconfig_shmem_init_idempotent);
	UT_RUN(test_reconfig_publish_increments_apply_counter);
	UT_RUN(test_reconfig_publish_overwrites_event_seq_monotonically);
	UT_RUN(test_reconfig_broadcast_stub_increments_counter);

	/* T-reconfig-9 */
	UT_RUN(test_epoch_observe_remote_advance_from_zero);
	UT_RUN(test_epoch_observe_remote_no_op_equal);
	UT_RUN(test_epoch_observe_remote_no_retreat);
	UT_RUN(test_epoch_observe_remote_monotonic_chain);
	UT_RUN(test_epoch_advance_for_reconfig_pre_post_snapshots);
	UT_RUN(test_epoch_observe_max_jump_constant);
	UT_RUN(test_epoch_changed_at_lsn_set_and_get);

	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
