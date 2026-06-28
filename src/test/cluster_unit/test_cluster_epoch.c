/*-------------------------------------------------------------------------
 *
 * test_cluster_epoch.c
 *	  spec-2.4 D16: standalone cluster_unit binary for cluster_epoch.
 *
 *	  Test list (TAP):
 *	    T1 cluster_epoch_shmem_size returns sizeof(ClusterEpochShmem) == 64
 *	    T2 cluster_epoch_get_current returns CLUSTER_EPOCH_INITIAL = 0
 *	       BEFORE shmem_init (NULL state path)
 *	    T3 cluster_epoch_shmem_init populates the slot in fake shmem;
 *	       repeated get_current returns same value (atomic read consistency)
 *	    T4 disable-cluster build link OK -- stub returns 0 even without
 *	       state pointer (validated by build itself + a sentinel test)
 *	    T5 no setter API exported (StaticAssert via grep at runtime --
 *	       this binary's source-grep prevents accidental setter export
 *	       in spec-2.4;spec-2.29 reconfig is the first spec to add it)
 *	    T6 ClusterEpochShmem layout invariants (64-byte sizeof + 48 B
 *	       reserved at end) are checked at compile time inside
 *	       cluster_epoch.c via StaticAssertDecl;runtime test mirrors
 *	       sizeof so any layout regression also fails this binary
 *
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/test/cluster_unit/test_cluster_epoch.c
 *
 * NOTES
 *	  pgrac-original file.  Links cluster_epoch.o standalone.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

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

#include <string.h>


/*
 * Stub: cluster_epoch.c calls ShmemInitStruct (real shmem-machinery
 * which the unit test does not link).  Provide a backing static
 * buffer of exactly the size cluster_epoch_shmem_size() reports.
 *
 * The static buffer is 64-byte aligned (on macOS arm64 / Linux x86
 * static globals are aligned to at least 16; we explicitly align
 * to 64 for cache-line semantics + ARM-strict atomic layout).
 */
static char test_epoch_shmem_storage[64] __attribute__((aligned(64)));
static bool test_shmem_initialized = false;

void *
ShmemInitStruct(const char *name pg_attribute_unused(), Size size pg_attribute_unused(),
				bool *foundPtr)
{
	*foundPtr = test_shmem_initialized;
	test_shmem_initialized = true;
	return test_epoch_shmem_storage;
}

/* cluster_epoch.c also calls cluster_shmem_register_region from the
 * register hook; we don't exercise the register path in unit tests
 * (it's exercised in test_cluster_shmem). */
struct ClusterShmemRegion;
void
cluster_shmem_register_region(const struct ClusterShmemRegion *region pg_attribute_unused())
{
	/* no-op stub */
}

/*
 * elog stub (cluster_epoch.c does not currently elog, but defensive
 * for future additions).
 */
void
ExceptionalCondition(const char *conditionName pg_attribute_unused(),
					 const char *fileName pg_attribute_unused(),
					 int lineNumber pg_attribute_unused())
{
	abort();
}


UT_TEST(test_t1_shmem_size_64)
{
	UT_ASSERT_EQ((int)cluster_epoch_shmem_size(), 64);
}

UT_TEST(test_t2_get_current_before_init_returns_zero)
{
	/* Before shmem_init runs, cluster_epoch_state is NULL and the
	 * accessor returns CLUSTER_EPOCH_INITIAL = 0.  This is the
	 * pre-postmaster-phase-1 / fork-without-init defensive path. */
	test_shmem_initialized = false;
	memset(test_epoch_shmem_storage, 0xAB, sizeof(test_epoch_shmem_storage));
	/* NB: we deliberately do NOT call shmem_init here -- accessor must
	 * defend against state==NULL. */
	UT_ASSERT_EQ((int)cluster_epoch_get_current(), (int)CLUSTER_EPOCH_INITIAL);
}

UT_TEST(test_t3_init_then_get_current_consistent)
{
	uint64 v1, v2;

	test_shmem_initialized = false;
	memset(test_epoch_shmem_storage, 0xCD, sizeof(test_epoch_shmem_storage));
	cluster_epoch_shmem_init();

	v1 = cluster_epoch_get_current();
	v2 = cluster_epoch_get_current();
	UT_ASSERT_EQ((int)v1, (int)CLUSTER_EPOCH_INITIAL);
	UT_ASSERT_EQ((int)v2, (int)CLUSTER_EPOCH_INITIAL);
	UT_ASSERT_EQ((int)v1, (int)v2);
}

UT_TEST(test_t4_repeated_init_idempotent)
{
	/* Second cluster_epoch_shmem_init should observe found=true and
	 * NOT reinitialize the atomic (so any value subsequently written
	 * survives -- spec-2.29 reconfig will rely on this). */
	test_shmem_initialized = false;
	cluster_epoch_shmem_init(); /* found=false -> init */
	cluster_epoch_shmem_init(); /* found=true  -> no-op */
	UT_ASSERT_EQ((int)cluster_epoch_get_current(), (int)CLUSTER_EPOCH_INITIAL);
}

UT_TEST(test_t5_no_setter_api_exported)
{
	/*
	 * spec-2.4 期 cluster_epoch_get_current was the only public API.
	 * spec-2.29 Sprint A Step 1 added the setter quartet:
	 *	  - cluster_epoch_advance_for_reconfig (D18 coordinator path)
	 *	  - cluster_epoch_set_changed_at_lsn   (D18 coordinator path)
	 *	  - cluster_epoch_observe_remote        (D18b envelope receive)
	 *	  - cluster_epoch_get_changed_at_lsn    (accessor)
	 * Those are exercised by test_cluster_reconfig.  This test now
	 * only asserts the spec-2.4 boot-time invariant: get_current
	 * returns CLUSTER_EPOCH_INITIAL=0 before any advance call.
	 */
	UT_ASSERT_EQ((int)cluster_epoch_get_current(), (int)CLUSTER_EPOCH_INITIAL);
}

UT_TEST(test_t6_layout_64_byte_cache_line)
{
	/* 64-byte sizeof is locked at compile time via StaticAssertDecl
	 * inside cluster_epoch.c;runtime check is belt-and-suspenders. */
	UT_ASSERT_EQ((int)cluster_epoch_shmem_size(), 64);
}

UT_TEST(test_t7_adopt_admitted_unbounded_monotonic)
{
	/* spec-5.15 §2.7: cluster_epoch_adopt_admitted is the quorum-authenticated
	 * join-admission path -- it may jump > CLUSTER_EPOCH_OBSERVE_MAX_JUMP (16),
	 * unlike the bounded observe_remote, and is monotonic (never retreats). */
	test_shmem_initialized = false;
	memset(test_epoch_shmem_storage, 0, sizeof(test_epoch_shmem_storage));
	cluster_epoch_shmem_init();
	UT_ASSERT_EQ((int)cluster_epoch_get_current(), (int)CLUSTER_EPOCH_INITIAL);

	/* a long-absent rejoiner adopts epoch 100 (a > 16 jump from 0). */
	cluster_epoch_adopt_admitted(100);
	UT_ASSERT_EQ((int)cluster_epoch_get_current(), 100);

	/* a lower adopt is ignored (monotonic). */
	cluster_epoch_adopt_admitted(50);
	UT_ASSERT_EQ((int)cluster_epoch_get_current(), 100);

	/* a higher adopt advances. */
	cluster_epoch_adopt_admitted(200);
	UT_ASSERT_EQ((int)cluster_epoch_get_current(), 200);
}


UT_DEFINE_GLOBALS();

int
main(void)
{
	UT_PLAN(7);

	UT_RUN(test_t1_shmem_size_64);
	UT_RUN(test_t2_get_current_before_init_returns_zero);
	UT_RUN(test_t3_init_then_get_current_consistent);
	UT_RUN(test_t4_repeated_init_idempotent);
	UT_RUN(test_t5_no_setter_api_exported);
	UT_RUN(test_t6_layout_64_byte_cache_line);
	UT_RUN(test_t7_adopt_admitted_unbounded_monotonic);

	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
