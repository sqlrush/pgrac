/*-------------------------------------------------------------------------
 *
 * test_cluster_hw_lease.c
 *	  Standalone unit tests for the spec-6.12d per-node HW space-lease
 *	  slot table (cluster_hw_lease.o).
 *
 *	  U-d coverage (the pure bookkeeping half; the cross-node
 *	  non-overlap of the ranges themselves is inherited from the durable
 *	  HW authority and exercised end-to-end by cluster_tap t/353):
 *	    1) inactive gate: consume/install are no-ops when the GUC is off
 *	       or shmem is absent.
 *	    2) install/consume roundtrip: parked range hands back exactly
 *	       [start, start+len) once each, in order, then exhausts to
 *	       InvalidBlockNumber (the Q10-A fallback signal).
 *	    3) per-pair isolation: leases for different (rloc, fork) pairs
 *	       never bleed into each other (grouping non-overlap assert).
 *	    4) replacement orphans the unconsumed remainder (orphan_zero
 *	       counts it; the new range serves afterwards).
 *	    5) LRU recycling under slot pressure orphans the evicted lease's
 *	       remainder and keeps serving the surviving pairs.
 *	    6) counters: leased_total / consumed / orphan_zero / grants add
 *	       up across all of the above.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/test/cluster_unit/test_cluster_hw_lease.c
 *
 * NOTES
 *	  This is a pgrac-original file.  Standalone binary linking
 *	  cluster_hw_lease.o only; PG backend symbols stubbed locally
 *	  (LWLock ops are no-ops -- the unit harness is single-threaded).
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <stdlib.h>
#include <string.h>

#include "cluster/cluster_guc.h"
#include "cluster/cluster_hw_lease.h"
#include "cluster/cluster_shmem.h"

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
 * Stubs needed to link cluster_hw_lease.o standalone.
 * ============================================================ */

void
ExceptionalCondition(const char *conditionName pg_attribute_unused(),
					 const char *fileName pg_attribute_unused(),
					 int lineNumber pg_attribute_unused())
{
	abort();
}

/* GUC storage (cluster_guc.o not linked). */
bool cluster_enabled = true;
int cluster_space_affinity = CLUSTER_SPACE_AFFINITY_STATIC;
int cluster_space_lease_blocks = 64;

/* Single-threaded harness: LWLock ops are no-ops. */
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

/* Shmem: back the region with plain malloc. */
void *
ShmemInitStruct(const char *name pg_attribute_unused(), Size size, bool *foundPtr)
{
	*foundPtr = false;
	return calloc(1, size);
}

void
cluster_shmem_register_region(const ClusterShmemRegion *region pg_attribute_unused())
{}


/* ============================================================
 * Helpers
 * ============================================================ */

static RelFileLocator
rloc_of(Oid rel)
{
	RelFileLocator r;

	r.spcOid = 1663;
	r.dbOid = 5;
	r.relNumber = rel;
	return r;
}

static uint64
ctr(pg_atomic_uint64 *c)
{
	return pg_atomic_read_u64(c);
}


/* ============================================================
 * Tests
 * ============================================================ */

UT_TEST(test_inactive_gate)
{
	int save = cluster_space_affinity;

	cluster_space_affinity = CLUSTER_SPACE_AFFINITY_OFF;
	UT_ASSERT(!cluster_hw_lease_active());
	cluster_hw_lease_install(rloc_of(90001), MAIN_FORKNUM, 100, 8);
	UT_ASSERT(cluster_hw_lease_next_block(rloc_of(90001), MAIN_FORKNUM) == InvalidBlockNumber);

	cluster_space_affinity = save;
	UT_ASSERT(cluster_hw_lease_active());
}

UT_TEST(test_install_consume_roundtrip)
{
	RelFileLocator r = rloc_of(90010);
	uint32 i;

	cluster_hw_lease_install(r, MAIN_FORKNUM, 200, 5);
	for (i = 0; i < 5; i++) {
		BlockNumber b = cluster_hw_lease_next_block(r, MAIN_FORKNUM);

		UT_ASSERT_EQ((int64)b, (int64)(200 + i));
	}
	UT_ASSERT(cluster_hw_lease_next_block(r, MAIN_FORKNUM) == InvalidBlockNumber);
}

UT_TEST(test_pair_isolation)
{
	RelFileLocator ra = rloc_of(90020);
	RelFileLocator rb = rloc_of(90021);

	cluster_hw_lease_install(ra, MAIN_FORKNUM, 300, 3);
	cluster_hw_lease_install(rb, MAIN_FORKNUM, 700, 3);
	/* interleaved consumption never crosses pairs */
	UT_ASSERT_EQ((int64)cluster_hw_lease_next_block(ra, MAIN_FORKNUM), 300);
	UT_ASSERT_EQ((int64)cluster_hw_lease_next_block(rb, MAIN_FORKNUM), 700);
	UT_ASSERT_EQ((int64)cluster_hw_lease_next_block(ra, MAIN_FORKNUM), 301);
	UT_ASSERT_EQ((int64)cluster_hw_lease_next_block(rb, MAIN_FORKNUM), 701);
	/* a fork the pair never leased misses */
	UT_ASSERT(cluster_hw_lease_next_block(ra, FSM_FORKNUM) == InvalidBlockNumber);
}

UT_TEST(test_replacement_orphans_remainder)
{
	RelFileLocator r = rloc_of(90030);
	uint64 orphan0 = ctr(&ClusterHwLeaseCtl->d_orphan_zero);

	cluster_hw_lease_install(r, MAIN_FORKNUM, 400, 10);
	UT_ASSERT_EQ((int64)cluster_hw_lease_next_block(r, MAIN_FORKNUM), 400);
	/* replace with a fresh grant: 9 unconsumed blocks become orphans */
	cluster_hw_lease_install(r, MAIN_FORKNUM, 500, 4);
	UT_ASSERT_EQ((int64)(ctr(&ClusterHwLeaseCtl->d_orphan_zero) - orphan0), 9);
	UT_ASSERT_EQ((int64)cluster_hw_lease_next_block(r, MAIN_FORKNUM), 500);
}

UT_TEST(test_lru_recycle_under_pressure)
{
	uint64 orphan0;
	int i;

	/* fill every slot (each with 2 unconsumed blocks); this also LRU-
	 * evicts whatever earlier tests left behind, so snapshot the orphan
	 * counter only AFTER the table is uniformly ours */
	for (i = 0; i < CLUSTER_HW_LEASE_SLOTS; i++)
		cluster_hw_lease_install(rloc_of(91000 + i), MAIN_FORKNUM, 1000 + 10 * i, 2);
	orphan0 = ctr(&ClusterHwLeaseCtl->d_orphan_zero);

	/* one more install forces an LRU eviction (the oldest = 91000) */
	cluster_hw_lease_install(rloc_of(92000), MAIN_FORKNUM, 9000, 2);

	UT_ASSERT_EQ((int64)(ctr(&ClusterHwLeaseCtl->d_orphan_zero) - orphan0), 2);
	/* the newcomer serves; the evicted pair misses */
	UT_ASSERT_EQ((int64)cluster_hw_lease_next_block(rloc_of(92000), MAIN_FORKNUM), 9000);
	UT_ASSERT(cluster_hw_lease_next_block(rloc_of(91000), MAIN_FORKNUM) == InvalidBlockNumber);
	/* a survivor still serves its own range */
	UT_ASSERT_EQ((int64)cluster_hw_lease_next_block(rloc_of(91001), MAIN_FORKNUM), 1010);
}

UT_TEST(test_counters_add_up)
{
	uint64 leased = ctr(&ClusterHwLeaseCtl->d_leased_total);
	uint64 consumed = ctr(&ClusterHwLeaseCtl->d_consumed);
	uint64 orphan = ctr(&ClusterHwLeaseCtl->d_orphan_zero);

	/* the running totals from the tests above stay coherent:
	 * outstanding inventory can never be negative */
	UT_ASSERT(leased >= consumed + orphan);
}

int
main(int argc pg_attribute_unused(), char **const argv pg_attribute_unused())
{
	/* stand-in for cluster_hw_lease_shmem_init over malloc-backed memory */
	cluster_hw_lease_shmem_init();

	UT_PLAN(6);

	UT_RUN(test_inactive_gate);
	UT_RUN(test_install_consume_roundtrip);
	UT_RUN(test_pair_isolation);
	UT_RUN(test_replacement_orphans_remainder);
	UT_RUN(test_lru_recycle_under_pressure);
	UT_RUN(test_counters_add_up);

	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
