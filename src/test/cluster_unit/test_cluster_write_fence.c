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


/* ----------
 * spec-4.12 D2: the PURE marker-authority selector that qvotec's poll runs over
 * the per-disk markers it read.  Quorum-majority (P0a), order by fence_epoch
 * (monotonic) with fence_generation tie-break (P0b), and the per-disk preserve
 * that must NOT amplify a minority marker (R13).
 * ----------
 */

/* Helper: build one fence marker tuple. */
static ClusterFenceMarker
mk_marker(uint64 epoch, uint64 generation, uint64 event_id, int32 issuer, uint8 dead0)
{
	ClusterFenceMarker m;

	memset(&m, 0, sizeof(m));
	m.magic = CLUSTER_FENCE_MARKER_MAGIC;
	m.version = CLUSTER_FENCE_MARKER_VERSION;
	m.fence_epoch = epoch;
	m.fence_generation = generation;
	m.fence_event_id = event_id;
	m.issuer_node_id = issuer;
	m.fenced_dead_bitmap[0] = dead0;
	return m;
}

UT_TEST(test_marker_node_is_fenced)
{
	uint8 bitmap[CLUSTER_FENCE_MARKER_DEAD_BITMAP_BYTES];

	memset(bitmap, 0, sizeof(bitmap));
	bitmap[0] = 0x05; /* nodes 0 and 2 */
	bitmap[1] = 0x80; /* node 15 */
	UT_ASSERT(cluster_fence_marker_node_is_fenced(bitmap, 0));
	UT_ASSERT(!cluster_fence_marker_node_is_fenced(bitmap, 1));
	UT_ASSERT(cluster_fence_marker_node_is_fenced(bitmap, 2));
	UT_ASSERT(!cluster_fence_marker_node_is_fenced(bitmap, 3));
	UT_ASSERT(cluster_fence_marker_node_is_fenced(bitmap, 15));
	/* out-of-range node ids never read past the bitmap. */
	UT_ASSERT(!cluster_fence_marker_node_is_fenced(bitmap, -1));
	UT_ASSERT(!cluster_fence_marker_node_is_fenced(bitmap, 128));
}

UT_TEST(test_authority_unanimous)
{
	ClusterFenceMarker markers[3];
	const bool has[3] = { true, true, true };
	ClusterFenceAuthority a;

	markers[0] = mk_marker(5, 2, 0xAA, 1, 0x04);
	markers[1] = mk_marker(5, 2, 0xAA, 1, 0x04);
	markers[2] = mk_marker(5, 2, 0xAA, 1, 0x04);
	a = cluster_fence_authority_decide(markers, has, 3);
	UT_ASSERT(a.has_authority);
	UT_ASSERT(a.agree_disk_count == 3);
	UT_ASSERT(a.marker.fence_epoch == 5);
	UT_ASSERT(!a.minority_seen);
}

UT_TEST(test_authority_majority_beats_minority_higher_epoch)
{
	/* P0a: a minority higher-epoch marker (1 disk) must NOT win; the majority
	 * (2 disks) lower-epoch tuple is authoritative. */
	ClusterFenceMarker markers[3];
	const bool has[3] = { true, true, true };
	ClusterFenceAuthority a;

	markers[0] = mk_marker(5, 2, 0xAA, 1, 0x00);
	markers[1] = mk_marker(5, 2, 0xAA, 1, 0x00);
	markers[2] = mk_marker(7, 9, 0xBB, 2, 0x00); /* higher epoch, but minority */
	a = cluster_fence_authority_decide(markers, has, 3);
	UT_ASSERT(a.has_authority);
	UT_ASSERT(a.marker.fence_epoch == 5); /* NOT 7 */
	UT_ASSERT(a.agree_disk_count == 2);
}

UT_TEST(test_authority_single_disk_no_authority)
{
	/* anti-P0a / R13: one disk carries a CRC-ok marker, the other two do not.
	 * No quorum-majority -> NO authority, minority_seen counter fires. */
	ClusterFenceMarker markers[3];
	const bool has[3] = { true, false, false };
	ClusterFenceAuthority a;

	markers[0] = mk_marker(9, 1, 0xCC, 0, 0x02);
	memset(&markers[1], 0, sizeof(markers[1]));
	memset(&markers[2], 0, sizeof(markers[2]));
	a = cluster_fence_authority_decide(markers, has, 3);
	UT_ASSERT(!a.has_authority);
	UT_ASSERT(a.minority_seen);
}

UT_TEST(test_authority_no_markers)
{
	ClusterFenceMarker markers[3];
	const bool has[3] = { false, false, false };
	ClusterFenceAuthority a;

	memset(markers, 0, sizeof(markers));
	a = cluster_fence_authority_decide(markers, has, 3);
	UT_ASSERT(!a.has_authority);
	UT_ASSERT(!a.minority_seen); /* nothing present -> not a minority event */
}

UT_TEST(test_authority_generation_tiebreak)
{
	/* same epoch, different generation: the tuple that reaches majority wins;
	 * a higher-generation minority does not. */
	ClusterFenceMarker markers[3];
	const bool has[3] = { true, true, true };
	ClusterFenceAuthority a;

	markers[0] = mk_marker(5, 2, 0xAA, 1, 0x00);
	markers[1] = mk_marker(5, 2, 0xAA, 1, 0x00);
	markers[2] = mk_marker(5, 3, 0xDD, 1, 0x00); /* higher gen, minority */
	a = cluster_fence_authority_decide(markers, has, 3);
	UT_ASSERT(a.has_authority);
	UT_ASSERT(a.marker.fence_generation == 2);
}

UT_TEST(test_authority_event_id_distinguishes_tuple)
{
	/* two markers same epoch+gen but different event_id are DIFFERENT fences;
	 * only the one on majority disks is authoritative. */
	ClusterFenceMarker markers[3];
	const bool has[3] = { true, true, true };
	ClusterFenceAuthority a;

	markers[0] = mk_marker(5, 2, 0xAAAA, 1, 0x00);
	markers[1] = mk_marker(5, 2, 0xAAAA, 1, 0x00);
	markers[2] = mk_marker(5, 2, 0xBBBB, 1, 0x00); /* same epoch+gen, other id */
	a = cluster_fence_authority_decide(markers, has, 3);
	UT_ASSERT(a.has_authority);
	UT_ASSERT(a.marker.fence_event_id == 0xAAAA);
	UT_ASSERT(a.agree_disk_count == 2);
}

UT_TEST(test_preserve_per_disk_no_amplification)
{
	/* R13 anti-P0a: simulate qvotec's per-disk preserve over 3 disks where only
	 * disk 0's own-slot carries a marker.  After the heartbeat rebuild, the
	 * marker count must stay 1/3 -- never amplified to 3/3 by cross-disk copy. */
	uint8 prior[3][368];
	uint8 fresh[3][368];
	ClusterFenceMarker injected;
	ClusterFenceMarker out;
	int i;
	int marker_count;

	injected = mk_marker(11, 1, 0xEE, 0, 0x00);

	/* prior own-slot reserved areas: disk 0 has the marker, 1 and 2 are clean. */
	memset(prior, 0, sizeof(prior));
	cluster_fence_marker_pack(prior[0], &injected);

	/* qvotec rebuilds a fresh self_slot per disk (reserved zeroed) then preserves
	 * per-disk from THAT disk's own prior slot only. */
	for (i = 0; i < 3; i++) {
		memset(fresh[i], 0, sizeof(fresh[i]));
		cluster_fence_marker_preserve_per_disk(fresh[i], prior[i]);
	}

	marker_count = 0;
	for (i = 0; i < 3; i++)
		if (cluster_fence_marker_unpack(fresh[i], &out))
			marker_count++;

	UT_ASSERT(marker_count == 1);							/* still 1/3, not 3/3 */
	UT_ASSERT(cluster_fence_marker_unpack(fresh[0], &out)); /* disk 0 kept it */
	UT_ASSERT(out.fence_epoch == 11);
	UT_ASSERT(!cluster_fence_marker_unpack(fresh[1], &out)); /* disk 1 stays clean */
	UT_ASSERT(!cluster_fence_marker_unpack(fresh[2], &out)); /* disk 2 stays clean */
}


/* ----------
 * spec-4.12b D1: marker_kind is OBSERVABILITY ONLY -- it survives pack/unpack but
 * is NEVER part of tuple identity (P0-1), and an old (4.12) reader sees it as the
 * FENCE default (0).
 * ----------
 */
UT_TEST(test_marker_kind_roundtrip)
{
	uint8 reserved1[368];
	ClusterFenceMarker in;
	ClusterFenceMarker out;
	uint8 dead[CLUSTER_FENCE_MARKER_DEAD_BITMAP_BYTES];

	memset(dead, 0, sizeof(dead));
	dead[0] = 0x06; /* nodes 1 and 2 */
	cluster_fence_marker_build_baseline(&in, 8, dead, 3, 0xFEEDU, 1);
	UT_ASSERT(in.marker_kind == CLUSTER_FENCE_MARKER_KIND_BASELINE);

	memset(reserved1, 0xAB, sizeof(reserved1));
	cluster_fence_marker_pack(reserved1, &in);
	UT_ASSERT(cluster_fence_marker_unpack(reserved1, &out));
	UT_ASSERT(out.marker_kind == CLUSTER_FENCE_MARKER_KIND_BASELINE);
	UT_ASSERT(out.fence_epoch == 8);
	UT_ASSERT(out.fence_generation == 3);
	UT_ASSERT(out.issuer_node_id == 1);
	UT_ASSERT(out.fenced_dead_bitmap[0] == 0x06);
}

UT_TEST(test_marker_kind_not_in_equality)
{
	/* P0-1: two markers identical in the 4.12 identity (epoch+gen+event_id+issuer+
	 * dead) but DIFFERENT marker_kind must still compare equal -- kind is not part
	 * of identity (else a baseline republish of a fence would split the quorum). */
	ClusterFenceMarker fence = mk_marker(5, 2, 0xAA, 1, 0x04);
	ClusterFenceMarker base = mk_marker(5, 2, 0xAA, 1, 0x04);

	fence.marker_kind = CLUSTER_FENCE_MARKER_KIND_FENCE;
	base.marker_kind = CLUSTER_FENCE_MARKER_KIND_BASELINE;
	UT_ASSERT(cluster_fence_marker_tuple_equal(&fence, &base));
}

UT_TEST(test_marker_kind_old_reader_default_is_fence)
{
	/* mk_marker memsets to 0 -> marker_kind defaults to FENCE (an old 4.12 marker,
	 * whose pad byte is 0, reads as FENCE). */
	ClusterFenceMarker m = mk_marker(1, 1, 1, 0, 0x00);

	UT_ASSERT(m.marker_kind == CLUSTER_FENCE_MARKER_KIND_FENCE);
}

/* ----------
 * spec-4.12b D5: the PURE double-monotonic refresh guard (P0-2) -- epoch must not
 * roll back AND the dead set must never shrink (never release a fenced node).
 * ----------
 */
UT_TEST(test_dead_superset)
{
	uint8 a[CLUSTER_FENCE_MARKER_DEAD_BITMAP_BYTES];
	uint8 b[CLUSTER_FENCE_MARKER_DEAD_BITMAP_BYTES];

	memset(a, 0, sizeof(a));
	memset(b, 0, sizeof(b));
	UT_ASSERT(cluster_fence_dead_superset(a, b)); /* empty covers empty */

	a[0] = 0x07;								   /* {0,1,2} */
	b[0] = 0x03;								   /* {0,1} */
	UT_ASSERT(cluster_fence_dead_superset(a, b));  /* {0,1,2} covers {0,1} */
	UT_ASSERT(!cluster_fence_dead_superset(b, a)); /* {0,1} does NOT cover {0,1,2} */

	b[0] = 0x07;
	UT_ASSERT(cluster_fence_dead_superset(a, b)); /* equal -> superset */
}

UT_TEST(test_authority_advances_epoch_monotonic)
{
	uint8 dead[CLUSTER_FENCE_MARKER_DEAD_BITMAP_BYTES];

	memset(dead, 0, sizeof(dead));
	/* epoch rolls back -> reject regardless of dead set. */
	UT_ASSERT(!cluster_write_fence_authority_advances(4, dead, 5, dead));
	/* epoch advances, dead unchanged -> advance. */
	UT_ASSERT(cluster_write_fence_authority_advances(6, dead, 5, dead));
	/* same epoch, dead unchanged (steady-state baseline republish) -> advance. */
	UT_ASSERT(cluster_write_fence_authority_advances(5, dead, 5, dead));
	/* first authority: latched (0, empty) -> always advances. */
	UT_ASSERT(cluster_write_fence_authority_advances(5, dead, 0, dead));
}

UT_TEST(test_authority_advances_dead_superset_required)
{
	uint8 latched[CLUSTER_FENCE_MARKER_DEAD_BITMAP_BYTES];
	uint8 grown[CLUSTER_FENCE_MARKER_DEAD_BITMAP_BYTES];
	uint8 shrunk[CLUSTER_FENCE_MARKER_DEAD_BITMAP_BYTES];

	memset(latched, 0, sizeof(latched));
	memset(grown, 0, sizeof(grown));
	memset(shrunk, 0, sizeof(shrunk));
	latched[0] = 0x01; /* node 0 fenced */
	grown[0] = 0x03;   /* nodes 0,1 fenced (superset) */
	shrunk[0] = 0x00;  /* node 0 released (NOT superset) */

	/* same epoch, dead set grows -> advance. */
	UT_ASSERT(cluster_write_fence_authority_advances(5, grown, 5, latched));
	/* same epoch, dead set shrinks -> REJECT (8.A: would release fenced node 0). */
	UT_ASSERT(!cluster_write_fence_authority_advances(5, shrunk, 5, latched));
	/* HIGHER epoch but dead set shrinks -> still REJECT (the 8.A key boundary:
	 * a higher epoch must NOT be allowed to silently release a fenced node). */
	UT_ASSERT(!cluster_write_fence_authority_advances(6, shrunk, 5, latched));
	/* higher epoch + grown dead -> advance (legitimate new fence). */
	UT_ASSERT(cluster_write_fence_authority_advances(6, grown, 5, latched));
}

UT_TEST(test_build_baseline_fields)
{
	ClusterFenceMarker out;
	uint8 dead[CLUSTER_FENCE_MARKER_DEAD_BITMAP_BYTES];

	memset(dead, 0, sizeof(dead));
	dead[1] = 0x80; /* node 15 fenced */
	cluster_fence_marker_build_baseline(&out, 12, dead, 4, 0xABCDU, 2);
	UT_ASSERT(out.magic == CLUSTER_FENCE_MARKER_MAGIC);
	UT_ASSERT(out.version == CLUSTER_FENCE_MARKER_VERSION);
	UT_ASSERT(out.marker_kind == CLUSTER_FENCE_MARKER_KIND_BASELINE);
	UT_ASSERT(out.fence_epoch == 12);
	UT_ASSERT(out.fence_generation == 4);
	UT_ASSERT(out.fence_event_id == 0xABCDU);
	UT_ASSERT(out.issuer_node_id == 2);
	UT_ASSERT(out.fenced_dead_bitmap[1] == 0x80);
	UT_ASSERT(cluster_fence_marker_node_is_fenced(out.fenced_dead_bitmap, 15));
}

UT_TEST(test_baseline_tuple_identical_to_fence_same_issuer)
{
	/* spec-4.12b P1: a baseline built with issuer = coordinator_node_id is
	 * tuple-IDENTICAL to the fence marker the coordinator issued for the same
	 * membership (epoch+gen+event_id+issuer+dead all match; kind differs but kind
	 * is NOT part of identity).  So the steady-state republish reinforces the fence
	 * marker's quorum instead of forming a second, competing tuple. */
	int32 coordinator = 2;
	uint8 dead[CLUSTER_FENCE_MARKER_DEAD_BITMAP_BYTES];
	ClusterFenceMarker fence;
	ClusterFenceMarker base;

	memset(dead, 0, sizeof(dead));
	dead[0] = 0x02; /* node 1 fenced by the reconfig */

	/* the fence marker as cluster_reconfig.c builds it: issuer = coordinator. */
	fence = mk_marker(7, 3, 0x99, coordinator, 0x02);

	/* the baseline republish: issuer = last_applied.coordinator_node_id = same. */
	cluster_fence_marker_build_baseline(&base, 7, dead, 3, 0x99, coordinator);

	UT_ASSERT(cluster_fence_marker_tuple_equal(&fence, &base)); /* one tuple */
	UT_ASSERT(base.marker_kind == CLUSTER_FENCE_MARKER_KIND_BASELINE);
	UT_ASSERT(fence.marker_kind == CLUSTER_FENCE_MARKER_KIND_FENCE);
}

UT_TEST(test_baseline_pristine_issuer_is_sentinel_and_uniform)
{
	/* pristine baseline uses the fixed sentinel issuer so every node's pristine
	 * baseline is the SAME tuple (reaches majority without depending on who wrote
	 * it).  Two nodes building the pristine baseline produce equal tuples. */
	uint8 empty[CLUSTER_FENCE_MARKER_DEAD_BITMAP_BYTES];
	ClusterFenceMarker a;
	ClusterFenceMarker b;

	memset(empty, 0, sizeof(empty));
	cluster_fence_marker_build_baseline(&a, 0, empty, 0, 0, CLUSTER_FENCE_BASELINE_INITIAL_ISSUER);
	cluster_fence_marker_build_baseline(&b, 0, empty, 0, 0, CLUSTER_FENCE_BASELINE_INITIAL_ISSUER);
	UT_ASSERT(cluster_fence_marker_tuple_equal(&a, &b));
	UT_ASSERT(a.issuer_node_id == CLUSTER_FENCE_BASELINE_INITIAL_ISSUER);
}

/* ----------
 * spec-4.12b D2 (U5): the PURE deterministic-leader selector.  The baseline
 * author is the lowest live node_id in the current alive_bitmap (same rule the
 * reconfig coordinator uses, cluster_reconfig.c dead_bitmap_lowest_bit_set), so
 * exactly one node authors the steady-state baseline -- no election, no second
 * competing per-disk minority marker (R4).  qvotec_self_is_membership_leader()
 * is just (this == cluster_node_id).
 * ----------
 */
UT_TEST(test_lowest_live_node)
{
	uint8 alive[CLUSTER_FENCE_MARKER_DEAD_BITMAP_BYTES];

	/* empty alive view -> no leader (-1): nobody authors a baseline. */
	memset(alive, 0, sizeof(alive));
	UT_ASSERT(cluster_write_fence_lowest_live_node(alive) == -1);

	/* single live node 0 -> leader 0. */
	alive[0] = 0x01;
	UT_ASSERT(cluster_write_fence_lowest_live_node(alive) == 0);

	/* live {2,3} (self == 2 would be leader; self == 3 would NOT). */
	memset(alive, 0, sizeof(alive));
	alive[0] = 0x0C; /* bits 2,3 */
	UT_ASSERT(cluster_write_fence_lowest_live_node(alive) == 2);

	/* live {1,2,3} -> leader is the lowest, 1 (so node 2 is no longer leader:
	 * alive_bitmap change re-selects the leader). */
	alive[0] = 0x0E; /* bits 1,2,3 */
	UT_ASSERT(cluster_write_fence_lowest_live_node(alive) == 1);

	/* cross-byte: node 7 (byte0 bit7) + node 8 (byte1 bit0) -> lowest is 7. */
	memset(alive, 0, sizeof(alive));
	alive[0] = 0x80; /* node 7 */
	alive[1] = 0x01; /* node 8 */
	UT_ASSERT(cluster_write_fence_lowest_live_node(alive) == 7);

	/* high-only: node 8 alone (byte1 bit0) -> leader 8 (scan walks past byte0). */
	memset(alive, 0, sizeof(alive));
	alive[1] = 0x80; /* node 15 */
	UT_ASSERT(cluster_write_fence_lowest_live_node(alive) == 15);
}

/* ----------
 * spec-4.12b D3 (Q3=A): the PURE bring-up grace predicate.  Before the hot gate
 * latches its first authority, an enforcing+attached node grants a grace (the
 * qvotec quorum gate covers no-quorum); after the latch it falls through to the
 * strict judge.  A detached region NEVER gets the grace (L110 fail-closed).
 * ----------
 */
UT_TEST(test_grace_before_engage)
{
	/* enforcement off -> no grace (the escape hatch lives in decide(), not here). */
	UT_ASSERT(!cluster_write_fence_grace_before_engage(false, true, false, false));
	/* enforcement on, region attached, NOT engaged, not self-fenced -> grace. */
	UT_ASSERT(cluster_write_fence_grace_before_engage(true, true, false, false));
	/* enforcement on, region attached, ENGAGED -> no grace (strict from here on). */
	UT_ASSERT(!cluster_write_fence_grace_before_engage(true, true, true, false));
	/* enforcement on, region DETACHED -> no grace (L110: fall through, fail closed). */
	UT_ASSERT(!cluster_write_fence_grace_before_engage(true, false, false, false));
	/*
	 * spec-4.12b Hardening v1.0.2: enforcement on, attached, NOT engaged, but the
	 * token already says self_fenced -> NO grace (defense-in-depth behind the
	 * engage-first publish order).  Without the guard the bring-up grace would let a
	 * self-fenced node write during the refresh publish window (8.A fail-open).
	 */
	UT_ASSERT(!cluster_write_fence_grace_before_engage(true, true, false, true));
}

int
main(void)
{
	UT_PLAN(30);
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
	UT_RUN(test_marker_node_is_fenced);
	UT_RUN(test_authority_unanimous);
	UT_RUN(test_authority_majority_beats_minority_higher_epoch);
	UT_RUN(test_authority_single_disk_no_authority);
	UT_RUN(test_authority_no_markers);
	UT_RUN(test_authority_generation_tiebreak);
	UT_RUN(test_authority_event_id_distinguishes_tuple);
	UT_RUN(test_preserve_per_disk_no_amplification);
	UT_RUN(test_marker_kind_roundtrip);
	UT_RUN(test_marker_kind_not_in_equality);
	UT_RUN(test_marker_kind_old_reader_default_is_fence);
	UT_RUN(test_dead_superset);
	UT_RUN(test_authority_advances_epoch_monotonic);
	UT_RUN(test_authority_advances_dead_superset_required);
	UT_RUN(test_build_baseline_fields);
	UT_RUN(test_baseline_tuple_identical_to_fence_same_issuer);
	UT_RUN(test_baseline_pristine_issuer_is_sentinel_and_uniform);
	UT_RUN(test_lowest_live_node);
	UT_RUN(test_grace_before_engage);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
