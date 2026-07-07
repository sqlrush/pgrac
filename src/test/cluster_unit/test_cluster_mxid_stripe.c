/*-------------------------------------------------------------------------
 *
 * test_cluster_mxid_stripe.c
 *	  Truth tables for the mxid stripe derivation pure layer (D3-a).
 *
 *	  Covers (spec-7.1 §4 unit, D3-a):
 *	  - cluster_mxid_origin_slot_floored: half-space truth table --
 *	    below floor -> -1, inside [floor, floor + 2^31) -> mxid mod
 *	    STRIDE, at/beyond the half-space boundary -> -1, invalid args
 *	    -> -1, half-space spanning the 2^32 wrap stays derivable.
 *	  - cluster_mxid_next_striped: in-class rounding, the
 *	    InvalidMultiXactId (0) in-class position skipped, 2^32 wrap
 *	    carry, result invariants over all 16 slots.
 *	  - cluster_mxid_halfspace_exceeded: guardrail boundary +/- 1,
 *	    invalid floor fails closed, wrap-spanning boundary.
 *	  - runtime wrappers (cluster_mxid_origin_slot / is_mine /
 *	    allocation_slot / stripe_floor): fail-closed before latch,
 *	    derivation after latch, defensive latch rejections.
 *	  - durable "PGXM" activation-slot extension record: roundtrip
 *	    accepts; every sanity / CRC corruption rejects (treat-as-
 *	    absent, fail-closed); the all-zeros pre-extension slot rejects.
 *
 *	  The mxid stripe layer is pure (no shmem / no locks); this binary
 *	  links cluster_mxid_stripe.o standalone with the boot lazy latch
 *	  stubbed to a no-op (latch tests drive the latch directly).
 *
 *	  Spec: spec-7.1-cross-instance-positive-interread.md
 *
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/test/cluster_unit/test_cluster_mxid_stripe.c
 *
 * NOTES
 *	  This is a pgrac-original file (no derivation from PostgreSQL).
 *	  Each test_*.c is a standalone executable; see unit_test.h.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/multixact.h"
#include "cluster/cluster_mxid_stripe.h"
#include "cluster/cluster_xid_stripe_boot.h" /* lazy-latch stub prototype */

/* Drop PG's port.h printf -> pg_printf override; unit_test.h uses
 * stdlib printf (libpgport is linked for CRC32C only). */
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

/* ----------
 * Stubs needed to link cluster_mxid_stripe.o standalone.
 * ----------
 */

void
ExceptionalCondition(const char *conditionName pg_attribute_unused(),
					 const char *fileName pg_attribute_unused(),
					 int lineNumber pg_attribute_unused())
{
	abort();
}

/* GUC / conf face consumed by cluster_mxid_allocation_slot (cluster_guc.o
 * is not linked here; the gate is a pure function of these three plus the
 * latch). */
bool cluster_enabled = false;
int cluster_node_id = -1;
bool cluster_xid_striping = false;

/* No-op stand-in for the boot lazy latch (cluster_xid_stripe_boot.o is
 * not linked here; the latch tests drive cluster_mxid_stripe_latch_runtime
 * directly). */
void
cluster_mxid_stripe_lazy_latch(void)
{}


/* ----------
 * cluster_mxid_origin_slot_floored
 * ----------
 */

UT_TEST(test_origin_slot_floored_basic)
{
	/* stride-aligned floor so the absolute congruence class is easy to
	 * read off (the slot is value mod STRIDE, NOT distance-from-floor) */
	MultiXactId floor_mxid = 4096;

	/* at the floor: derivable, slot = value mod STRIDE */
	UT_ASSERT(cluster_mxid_origin_slot_floored(4096, floor_mxid) == 0);

	/* inside the half-space */
	UT_ASSERT(cluster_mxid_origin_slot_floored(4101, floor_mxid) == 5);
	UT_ASSERT(cluster_mxid_origin_slot_floored(4096 + 16 * 7 + 3, floor_mxid) == 3);

	/* last derivable position: floor + 2^31 - 1 (0x7FFFFFFF mod 16 = 15) */
	UT_ASSERT(cluster_mxid_origin_slot_floored((MultiXactId)4096 + 0x7FFFFFFF, floor_mxid) == 15);

	/* first ambiguous position: floor + 2^31 -> underivable */
	UT_ASSERT(cluster_mxid_origin_slot_floored((MultiXactId)(4096 + (uint32)0x80000000), floor_mxid)
			  == -1);

	/* behind the floor (pre-stripe history) -> underivable */
	UT_ASSERT(cluster_mxid_origin_slot_floored(4095, floor_mxid) == -1);
	UT_ASSERT(cluster_mxid_origin_slot_floored(1, floor_mxid) == -1);
}

UT_TEST(test_origin_slot_floored_invalid_args)
{
	UT_ASSERT(cluster_mxid_origin_slot_floored(InvalidMultiXactId, 1000) == -1);
	UT_ASSERT(cluster_mxid_origin_slot_floored(1000, InvalidMultiXactId) == -1);
	UT_ASSERT(cluster_mxid_origin_slot_floored(InvalidMultiXactId, InvalidMultiXactId) == -1);
}

UT_TEST(test_origin_slot_floored_wrap_spanning)
{
	/* Half-space spanning the 2^32 boundary: floor near the top of the
	 * id space, mxid a wrapped small value ahead of it.  uint32 modular
	 * distance keeps the derivation exact. */
	MultiXactId floor_mxid = (MultiXactId)0xFFFF0010;

	/* 0x20 is 0x1_0010 ahead of the floor (mod 2^32): derivable */
	UT_ASSERT(cluster_mxid_origin_slot_floored((MultiXactId)0x20, floor_mxid) == 0);
	UT_ASSERT(cluster_mxid_origin_slot_floored((MultiXactId)0x2D, floor_mxid) == 13);

	/* still inside: floor + 2^31 - 1 (wrapped) */
	UT_ASSERT(cluster_mxid_origin_slot_floored((MultiXactId)(0xFFFF0010 + 0x7FFFFFFF), floor_mxid)
			  != -1);

	/* boundary (wrapped): floor + 2^31 -> underivable */
	UT_ASSERT(
		cluster_mxid_origin_slot_floored((MultiXactId)(0xFFFF0010 + (uint32)0x80000000), floor_mxid)
		== -1);

	/* just behind the floor -> underivable (distance wraps to ~2^32) */
	UT_ASSERT(cluster_mxid_origin_slot_floored((MultiXactId)0xFFFF000F, floor_mxid) == -1);
}

UT_TEST(test_origin_slot_floored_congruence_all_slots)
{
	MultiXactId floor_mxid = 4096; /* multiple of 16 */
	int k;

	for (k = 0; k < CLUSTER_MXID_STRIDE; k++) {
		MultiXactId m = floor_mxid + 16 * 100 + (MultiXactId)k;

		UT_ASSERT(cluster_mxid_origin_slot_floored(m, floor_mxid) == k);
	}
}


/* ----------
 * cluster_mxid_next_striped
 * ----------
 */

UT_TEST(test_next_striped_in_class)
{
	/* already in class: returned unchanged */
	UT_ASSERT(cluster_mxid_next_striped(1600, 0) == 1600);
	UT_ASSERT(cluster_mxid_next_striped(1605, 5) == 1605);

	/* rounding up to the class */
	UT_ASSERT(cluster_mxid_next_striped(1601, 0) == 1616);
	UT_ASSERT(cluster_mxid_next_striped(1601, 5) == 1605);
	UT_ASSERT(cluster_mxid_next_striped(1606, 5) == 1621);
}

UT_TEST(test_next_striped_invalid_zero_skip)
{
	/* The class-0 position at value 0 is InvalidMultiXactId: skipped one
	 * stride to 16.  (Only value 0 is special for multixacts --
	 * FirstMultiXactId is 1 -- unlike the xid space's 0-2.) */
	UT_ASSERT(cluster_mxid_next_striped((MultiXactId)0xFFFFFFF1, 0) == 16);

	/* wrap carry into a legal small value: no skip for slot >= 1 */
	UT_ASSERT(cluster_mxid_next_striped((MultiXactId)0xFFFFFFFE, 5) == 5);

	/* asking exactly at 0 for slot 0 skips to 16 */
	UT_ASSERT(cluster_mxid_next_striped(InvalidMultiXactId, 0) == 16);

	/* slot 1 from 0 rounds to 1 (FirstMultiXactId itself is legal) */
	UT_ASSERT(cluster_mxid_next_striped(InvalidMultiXactId, 1) == 1);
}

UT_TEST(test_next_striped_invariants_all_slots)
{
	MultiXactId starts[] = { 1, 17, 2047, 2048, 65535, (MultiXactId)0xFFFFFFF0 };
	int s, k;

	for (s = 0; s < (int)lengthof(starts); s++) {
		for (k = 0; k < CLUSTER_MXID_STRIDE; k++) {
			MultiXactId r = cluster_mxid_next_striped(starts[s], k);

			/* in class, valid, and within one skip of the start */
			UT_ASSERT((int)(r % CLUSTER_MXID_STRIDE) == k);
			UT_ASSERT(r != InvalidMultiXactId);
			UT_ASSERT((uint32)(r - starts[s]) < 2 * CLUSTER_MXID_STRIDE);
		}
	}
}


/* ----------
 * cluster_mxid_halfspace_exceeded
 * ----------
 */

UT_TEST(test_halfspace_boundary)
{
	MultiXactId floor_mxid = 1000;

	UT_ASSERT(!cluster_mxid_halfspace_exceeded(1000, floor_mxid));
	UT_ASSERT(!cluster_mxid_halfspace_exceeded((MultiXactId)1000 + 0x7FFFFFFF, floor_mxid));
	UT_ASSERT(
		cluster_mxid_halfspace_exceeded((MultiXactId)(1000 + (uint32)0x80000000), floor_mxid));
}

UT_TEST(test_halfspace_invalid_floor_fails_closed)
{
	UT_ASSERT(cluster_mxid_halfspace_exceeded(1000, InvalidMultiXactId));
}

UT_TEST(test_halfspace_wrap_spanning)
{
	MultiXactId floor_mxid = (MultiXactId)0xFFFFFF00;

	/* wrapped candidate still inside the half-space */
	UT_ASSERT(!cluster_mxid_halfspace_exceeded((MultiXactId)0x10, floor_mxid));
	UT_ASSERT(!cluster_mxid_halfspace_exceeded((MultiXactId)(0xFFFFFF00 + 0x7FFFFFFF), floor_mxid));

	/* wrapped boundary */
	UT_ASSERT(cluster_mxid_halfspace_exceeded((MultiXactId)(0xFFFFFF00 + (uint32)0x80000000),
											  floor_mxid));
}


/* ----------
 * runtime wrappers + latch
 * ----------
 */

UT_TEST(test_runtime_inactive_fail_closed)
{
	/* nothing latched yet (and the stubbed lazy latch is a no-op) */
	UT_ASSERT(cluster_mxid_origin_slot(5000) == -1);
	UT_ASSERT(!cluster_mxid_is_mine(5000));
	UT_ASSERT(cluster_mxid_allocation_slot() == -1);
	UT_ASSERT(cluster_mxid_stripe_floor() == InvalidMultiXactId);
}

UT_TEST(test_runtime_latch_defensive)
{
	/* active=true with junk latches the INACTIVE state (fail-closed) */
	cluster_mxid_stripe_latch_runtime(true, -1, 1000);
	UT_ASSERT(cluster_mxid_origin_slot(5000) == -1);

	cluster_mxid_stripe_latch_runtime(true, CLUSTER_MXID_STRIDE, 1000);
	UT_ASSERT(cluster_mxid_origin_slot(5000) == -1);

	cluster_mxid_stripe_latch_runtime(true, 3, InvalidMultiXactId);
	UT_ASSERT(cluster_mxid_origin_slot(5000) == -1);
	UT_ASSERT(cluster_mxid_stripe_floor() == InvalidMultiXactId);
}

UT_TEST(test_runtime_latched_derivation)
{
	cluster_node_id = 3;
	cluster_enabled = true;
	cluster_xid_striping = true;

	cluster_mxid_stripe_latch_runtime(true, 3, 4096);

	UT_ASSERT(cluster_mxid_stripe_floor() == 4096);
	UT_ASSERT(cluster_mxid_origin_slot(4096 + 16 * 4 + 3) == 3);
	UT_ASSERT(cluster_mxid_is_mine(4096 + 16 * 4 + 3));
	UT_ASSERT(cluster_mxid_origin_slot(4096 + 16 * 4 + 7) == 7);
	UT_ASSERT(!cluster_mxid_is_mine(4096 + 16 * 4 + 7));

	/* below-floor / beyond-half-space stay underivable after latch */
	UT_ASSERT(cluster_mxid_origin_slot(4095) == -1);
	UT_ASSERT(cluster_mxid_origin_slot((MultiXactId)(4096 + (uint32)0x80000000)) == -1);
	UT_ASSERT(!cluster_mxid_is_mine(4095));

	/* allocation gate open with the three GUC facts + latch */
	UT_ASSERT(cluster_mxid_allocation_slot() == 3);

	/* GUC off closes the gate even while latched */
	cluster_xid_striping = false;
	UT_ASSERT(cluster_mxid_allocation_slot() == -1);
	cluster_xid_striping = true;

	cluster_enabled = false;
	UT_ASSERT(cluster_mxid_allocation_slot() == -1);
	cluster_enabled = true;

	/* node outside the stripe width closes the gate */
	cluster_node_id = CLUSTER_MXID_STRIDE;
	UT_ASSERT(cluster_mxid_allocation_slot() == -1);
	cluster_node_id = 3;

	/* restore for later tests */
	cluster_mxid_stripe_latch_runtime(false, -1, InvalidMultiXactId);
	cluster_enabled = false;
	cluster_xid_striping = false;
	cluster_node_id = -1;
}


/* ----------
 * durable "PGXM" activation-slot extension record
 * ----------
 */

static ClusterMxidStripeExtensionRecord
make_valid_ext(void)
{
	ClusterMxidStripeExtensionRecord rec;

	memset(&rec, 0, sizeof(rec));
	rec.magic = CLUSTER_PGXM_MAGIC;
	rec.version = CLUSTER_PGXM_VERSION;
	rec.activated_mxid_floor = 1024;
	rec.generation = 1;
	cluster_mxid_stripe_extension_record_compute_crc(&rec);
	return rec;
}

UT_TEST(test_ext_record_roundtrip_valid)
{
	ClusterMxidStripeExtensionRecord rec = make_valid_ext();

	UT_ASSERT(cluster_mxid_stripe_extension_record_valid(&rec));
}

UT_TEST(test_ext_record_absent_all_zeros)
{
	/* a slot written by a pre-extension binary reads back zeros here:
	 * record-absent, the fail-closed empty state */
	ClusterMxidStripeExtensionRecord rec;

	memset(&rec, 0, sizeof(rec));
	UT_ASSERT(!cluster_mxid_stripe_extension_record_valid(&rec));
}

UT_TEST(test_ext_record_sanity_rejections)
{
	ClusterMxidStripeExtensionRecord rec;

	UT_ASSERT(!cluster_mxid_stripe_extension_record_valid(NULL));

	rec = make_valid_ext();
	rec.magic = 0x12345678;
	cluster_mxid_stripe_extension_record_compute_crc(&rec);
	UT_ASSERT(!cluster_mxid_stripe_extension_record_valid(&rec));

	rec = make_valid_ext();
	rec.version = 2;
	cluster_mxid_stripe_extension_record_compute_crc(&rec);
	UT_ASSERT(!cluster_mxid_stripe_extension_record_valid(&rec));

	rec = make_valid_ext();
	rec.activated_mxid_floor = InvalidMultiXactId;
	cluster_mxid_stripe_extension_record_compute_crc(&rec);
	UT_ASSERT(!cluster_mxid_stripe_extension_record_valid(&rec));

	rec = make_valid_ext();
	rec.generation = 0;
	cluster_mxid_stripe_extension_record_compute_crc(&rec);
	UT_ASSERT(!cluster_mxid_stripe_extension_record_valid(&rec));
}

UT_TEST(test_ext_record_crc_rejections)
{
	ClusterMxidStripeExtensionRecord rec = make_valid_ext();

	rec.activated_mxid_floor = 2048; /* content changed, CRC stale */
	UT_ASSERT(!cluster_mxid_stripe_extension_record_valid(&rec));

	rec = make_valid_ext();
	rec._pad0 = 1; /* padding is CRC-covered: torn/garbage write rejects */
	UT_ASSERT(!cluster_mxid_stripe_extension_record_valid(&rec));

	rec = make_valid_ext();
	rec.crc32c ^= 0x1;
	UT_ASSERT(!cluster_mxid_stripe_extension_record_valid(&rec));
}


int
main(void)
{
	UT_RUN(test_origin_slot_floored_basic);
	UT_RUN(test_origin_slot_floored_invalid_args);
	UT_RUN(test_origin_slot_floored_wrap_spanning);
	UT_RUN(test_origin_slot_floored_congruence_all_slots);

	UT_RUN(test_next_striped_in_class);
	UT_RUN(test_next_striped_invalid_zero_skip);
	UT_RUN(test_next_striped_invariants_all_slots);

	UT_RUN(test_halfspace_boundary);
	UT_RUN(test_halfspace_invalid_floor_fails_closed);
	UT_RUN(test_halfspace_wrap_spanning);

	UT_RUN(test_runtime_inactive_fail_closed);
	UT_RUN(test_runtime_latch_defensive);
	UT_RUN(test_runtime_latched_derivation);

	UT_RUN(test_ext_record_roundtrip_valid);
	UT_RUN(test_ext_record_absent_all_zeros);
	UT_RUN(test_ext_record_sanity_rejections);
	UT_RUN(test_ext_record_crc_rejections);

	UT_DONE();
}
