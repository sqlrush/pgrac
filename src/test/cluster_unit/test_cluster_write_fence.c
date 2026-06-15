/*-------------------------------------------------------------------------
 *
 * test_cluster_write_fence.c
 *	  Unit tests for the spec-4.12 cooperative write-fence PURE judge
 *	  cluster_write_fence_decide -- the truth table the hot write paths
 *	  consult before any shared-storage write.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/test/cluster_unit/test_cluster_write_fence.c
 *
 * NOTES
 *	  This is a pgrac-original file (no derivation from PostgreSQL).
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "cluster/cluster_write_fence.h"

#include "unit_test.h"

UT_DEFINE_GLOBALS();

/*
 * libpgport's snprintf.c references ExceptionalCondition in a cassert build; this
 * pure-inline test links libpgport for the unit harness but no backend object, so
 * provide a local stub (mirrors test_cluster_thread_apply.c).  It must never fire.
 */
void
ExceptionalCondition(const char *conditionName, const char *fileName, int lineNumber)
{
	printf("# unexpected Assert: %s at %s:%d\n", conditionName, fileName, lineNumber);
	abort();
}

/* Named-field helper so the truth table reads clearly. */
#define DECIDE(enf, attached, ep, auth, now, expire, self)                                         \
	cluster_write_fence_decide((enf), (attached), (ep), (auth), (now), (expire), (self))

/* A fully-authorized baseline (allowed): on, attached, epoch matches, lease valid, not fenced. */
#define ALLOWED_BASELINE DECIDE(true, true, 42, 42, 100, 200, false)


UT_TEST(test_enforcement_off_is_escape_hatch)
{
	/* enforcement off -> always allowed, regardless of every other input. */
	UT_ASSERT(DECIDE(false, false, 7, 9, 999, 0, true));
	UT_ASSERT(DECIDE(false, true, 42, 42, 100, 200, false));
}

UT_TEST(test_baseline_authorized_is_allowed)
{
	UT_ASSERT(ALLOWED_BASELINE);
}

UT_TEST(test_detached_region_fails_closed)
{
	/* L110: enforcement on but the token region is not attached -> fail closed. */
	UT_ASSERT(!DECIDE(true, false, 42, 42, 100, 200, false));
}

UT_TEST(test_self_fenced_fails_closed)
{
	UT_ASSERT(!DECIDE(true, true, 42, 42, 100, 200, true));
}

UT_TEST(test_stale_epoch_fails_closed_exact_compare)
{
	/* exact == : neither ahead nor behind passes (a stale node must not write). */
	UT_ASSERT(!DECIDE(true, true, 41, 42, 100, 200, false)); /* behind */
	UT_ASSERT(!DECIDE(true, true, 43, 42, 100, 200, false)); /* ahead (NOT >=) */
}

UT_TEST(test_lease_expired_fails_closed)
{
	/* now >= lease_expire -> the node failed to refresh (partition) -> fail closed. */
	UT_ASSERT(!DECIDE(true, true, 42, 42, 200, 200, false)); /* now == expire */
	UT_ASSERT(!DECIDE(true, true, 42, 42, 201, 200, false)); /* now > expire */
}

UT_TEST(test_lease_just_valid_is_allowed)
{
	UT_ASSERT(DECIDE(true, true, 42, 42, 199, 200, false)); /* now < expire */
}


/* ----------
 * spec-4.12 D1: the durable fence marker pack/unpack into the voting slot's
 * _reserved1 bytes (the CRC over 0..507 already protects it).
 * ----------
 */
UT_TEST(test_marker_pack_unpack_roundtrip)
{
	uint8 reserved1[368];
	ClusterFenceMarker in;
	ClusterFenceMarker out;

	memset(reserved1, 0xAB, sizeof(reserved1)); /* non-zero fill: catch over/under-copy */
	memset(&in, 0, sizeof(in));
	in.magic = CLUSTER_FENCE_MARKER_MAGIC;
	in.version = CLUSTER_FENCE_MARKER_VERSION;
	in.fence_epoch = UINT64CONST(0x1122334455667788);
	in.fence_event_id = UINT64CONST(0xDEADBEEFCAFEBABE);
	in.fence_generation = 7;
	in.issuer_node_id = 3;
	in.fenced_dead_bitmap[0] = 0x05; /* nodes 0 and 2 declared dead (multi-dead) */

	cluster_fence_marker_pack(reserved1, &in);
	UT_ASSERT(cluster_fence_marker_unpack(reserved1, &out));
	UT_ASSERT(out.fence_epoch == in.fence_epoch);
	UT_ASSERT(out.fence_event_id == in.fence_event_id);
	UT_ASSERT(out.fence_generation == 7);
	UT_ASSERT(out.issuer_node_id == 3);
	UT_ASSERT(out.fenced_dead_bitmap[0] == 0x05);
}

UT_TEST(test_marker_magic_absent_is_no_marker)
{
	uint8 reserved1[368];
	ClusterFenceMarker out;

	memset(reserved1, 0, sizeof(reserved1)); /* zeroed reserved -> magic 0 -> no marker */
	UT_ASSERT(!cluster_fence_marker_unpack(reserved1, &out));
}

UT_TEST(test_marker_pack_leaves_trailing_reserved_untouched)
{
	uint8 reserved1[368];
	ClusterFenceMarker in;

	memset(reserved1, 0xAB, sizeof(reserved1));
	memset(&in, 0, sizeof(in));
	in.magic = CLUSTER_FENCE_MARKER_MAGIC;
	cluster_fence_marker_pack(reserved1, &in);
	/* the marker occupies [0..63]; the rest of _reserved1 must be untouched. */
	UT_ASSERT(reserved1[CLUSTER_FENCE_MARKER_BYTES] == 0xAB);
	UT_ASSERT(reserved1[367] == 0xAB);
}

UT_TEST(test_marker_size_pinned)
{
	UT_ASSERT((int)sizeof(ClusterFenceMarker) == CLUSTER_FENCE_MARKER_BYTES);
}


int
main(void)
{
	UT_PLAN(11);
	UT_RUN(test_enforcement_off_is_escape_hatch);
	UT_RUN(test_baseline_authorized_is_allowed);
	UT_RUN(test_detached_region_fails_closed);
	UT_RUN(test_self_fenced_fails_closed);
	UT_RUN(test_stale_epoch_fails_closed_exact_compare);
	UT_RUN(test_lease_expired_fails_closed);
	UT_RUN(test_lease_just_valid_is_allowed);
	UT_RUN(test_marker_pack_unpack_roundtrip);
	UT_RUN(test_marker_magic_absent_is_no_marker);
	UT_RUN(test_marker_pack_leaves_trailing_reserved_untouched);
	UT_RUN(test_marker_size_pinned);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
