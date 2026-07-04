/*-------------------------------------------------------------------------
 *
 * test_cluster_xid_stripe.c
 *	  U1 truth tables for the xid stripe derivation pure layer.
 *
 *	  Covers (spec-6.15 §4 U1):
 *	  - cluster_xid_widen: signed relative window (next ± 2^31) --
 *	    same-epoch behind/ahead, epoch-crossing behind/ahead, epoch-0
 *	    underflow guard, special-xid rejection.
 *	  - cluster_xid_origin_slot_full: floor truth table (< floor -> -1,
 *	    >= floor -> xid mod STRIDE, invalid args -> -1, wrap-crossing
 *	    new xid derivable -- the v0.2 P1-1 regression pin).
 *	  - cluster_xid_next_striped_full: in-class rounding, illegal
 *	    32-bit values 0-2 skipped to next in-class position, epoch
 *	    carry when rounding crosses 2^32, FirstNormalTransactionId
 *	    edge, and result invariants over all 16 slots.
 *	  - cluster_xid_lag_exceeds: herding slack / hard-limit predicate.
 *	  - runtime wrappers (cluster_xid_origin_slot / cluster_xid_is_mine):
 *	    fail-closed before latch, derivation after latch, remote-ahead
 *	    window, wrap-crossing floor comparison.
 *	  - durable stripe records (D5a): "PGXS"/"PGXA" integrity truth
 *	    tables -- roundtrip accepts, every sanity / identity / CRC
 *	    corruption rejects (treat-as-absent, fail-closed), all-zeros
 *	    lazily-materialised slot rejects.
 *
 *	  The stripe layer is pure (no shmem / no locks); this binary links
 *	  cluster_xid_stripe.o standalone with ReadNextFullTransactionId
 *	  stubbed to a settable value.
 *
 *	  Spec: spec-6.15-xid-space-segmentation.md §4 (U1)
 *
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/test/cluster_unit/test_cluster_xid_stripe.c
 *
 * NOTES
 *	  This is a pgrac-original file (no derivation from PostgreSQL).
 *	  Each test_*.c is a standalone executable; see unit_test.h.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/transam.h"
#include "cluster/cluster_xid_stripe.h"
#include "cluster/cluster_xid_stripe_boot.h" /* lazy-latch stub prototype */

/* Drop PG's port.h printf -> pg_printf override; unit_test.h uses
 * stdlib printf (libpgport is linked for CRC32C only, D5a). */
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
 * Stubs needed to link cluster_xid_stripe.o standalone.
 * ----------
 */

void
ExceptionalCondition(const char *conditionName pg_attribute_unused(),
					 const char *fileName pg_attribute_unused(),
					 int lineNumber pg_attribute_unused())
{
	abort();
}

/* GUC / conf face consumed by cluster_xid_allocation_slot (cluster_guc.o
 * is not linked here; the gate is a pure function of these three). */
bool cluster_enabled = false;
int cluster_node_id = -1;
bool cluster_xid_striping = false;

/* No-op stand-in for the D5b lazy latch (cluster_xid_stripe_boot.o is
 * not linked here; the latch tests drive cluster_xid_stripe_latch_runtime
 * directly). */
void
cluster_xid_stripe_lazy_latch(void)
{}

/* Settable stand-in for varsup.c ReadNextFullTransactionId (the runtime
 * wrappers read the local nextXid through it). */
static FullTransactionId stub_next_full = { 0 };

FullTransactionId
ReadNextFullTransactionId(void)
{
	return stub_next_full;
}

/* Shorthand: build a FullTransactionId from (epoch, xid32). */
static FullTransactionId
fxid(uint32 epoch, uint32 xid32)
{
	return FullTransactionIdFromU64(((uint64)epoch << 32) | xid32);
}


/* ----------
 * cluster_xid_widen
 * ----------
 */

UT_TEST(test_widen_behind_same_epoch)
{
	/* behind, no epoch crossing: 50 relative to next {1,100} -> {1,50} */
	FullTransactionId w = cluster_xid_widen(50, fxid(1, 100));

	UT_ASSERT(U64FromFullTransactionId(w) == U64FromFullTransactionId(fxid(1, 50)));

	/* boundary: xid == low32(next) -> same position */
	w = cluster_xid_widen(100, fxid(1, 100));
	UT_ASSERT(U64FromFullTransactionId(w) == U64FromFullTransactionId(fxid(1, 100)));
}

UT_TEST(test_widen_ahead_same_epoch)
{
	/* Remote xid ahead of local next (within herding slack): must stay in
	 * the SAME epoch.  A one-sided snapshot-style widening (xid8funcs.c
	 * widen_snapshot_xid) would misplace this into epoch-1 -- the whole
	 * reason the stripe layer uses a signed relative window. */
	FullTransactionId w = cluster_xid_widen(200, fxid(1, 100));

	UT_ASSERT(U64FromFullTransactionId(w) == U64FromFullTransactionId(fxid(1, 200)));
}

UT_TEST(test_widen_behind_epoch_crossing)
{
	/* 0xFFFFFF00 is 356 behind {1,100} in signed-window terms -> {0,...} */
	FullTransactionId w = cluster_xid_widen(0xFFFFFF00, fxid(1, 100));

	UT_ASSERT(U64FromFullTransactionId(w) == U64FromFullTransactionId(fxid(0, 0xFFFFFF00)));
}

UT_TEST(test_widen_ahead_epoch_crossing)
{
	/* xid 5 is 21 ahead of {1, 0xFFFFFFF0} -> lands in epoch 2 */
	FullTransactionId w = cluster_xid_widen(5, fxid(1, 0xFFFFFFF0));

	UT_ASSERT(U64FromFullTransactionId(w) == U64FromFullTransactionId(fxid(2, 5)));
}

UT_TEST(test_widen_epoch0_underflow_guard)
{
	/* A "behind" interpretation that would precede fxid 0 cannot exist as
	 * a live xid: fail-closed Invalid (upstream PG code does not guard
	 * this; the stripe layer must, per §3.2 fail-closed contract). */
	FullTransactionId w = cluster_xid_widen(0xFFFFFF00, fxid(0, 100));

	UT_ASSERT(!FullTransactionIdIsValid(w));
}

UT_TEST(test_widen_special_xids)
{
	/* InvalidTransactionId / BootstrapTransactionId / FrozenTransactionId
	 * carry no origin information: underivable. */
	UT_ASSERT(!FullTransactionIdIsValid(cluster_xid_widen(InvalidTransactionId, fxid(0, 100))));
	UT_ASSERT(!FullTransactionIdIsValid(cluster_xid_widen(BootstrapTransactionId, fxid(0, 100))));
	UT_ASSERT(!FullTransactionIdIsValid(cluster_xid_widen(FrozenTransactionId, fxid(0, 100))));

	/* invalid reference next_full -> Invalid */
	UT_ASSERT(!FullTransactionIdIsValid(cluster_xid_widen(50, InvalidFullTransactionId)));
}


/* ----------
 * cluster_xid_origin_slot_full
 * ----------
 */

UT_TEST(test_origin_slot_full_basic)
{
	FullTransactionId floor_full = fxid(0, 1000);

	/* >= floor: derivable, slot = xid32 mod 16 */
	UT_ASSERT_EQ(cluster_xid_origin_slot_full(fxid(0, 1996), floor_full), 12);
	/* boundary: exactly at floor is derivable (§3.2 ">= floor") */
	UT_ASSERT_EQ(cluster_xid_origin_slot_full(fxid(0, 1000), floor_full), 8);
	/* below floor: underivable */
	UT_ASSERT_EQ(cluster_xid_origin_slot_full(fxid(0, 999), floor_full), -1);
}

UT_TEST(test_origin_slot_full_invalid_args)
{
	UT_ASSERT_EQ(cluster_xid_origin_slot_full(InvalidFullTransactionId, fxid(0, 1000)), -1);
	UT_ASSERT_EQ(cluster_xid_origin_slot_full(fxid(0, 2000), InvalidFullTransactionId), -1);
}

UT_TEST(test_origin_slot_full_wrap_regression_pin)
{
	/* v0.2 P1-1 regression pin: floor near the top of epoch 0, a new
	 * post-wrap xid in epoch 1 must be derivable.  A uint32 comparison
	 * (100 < 4000000000) would wrongly report underivable. */
	FullTransactionId floor_full = fxid(0, 4000000000U);

	UT_ASSERT_EQ(cluster_xid_origin_slot_full(fxid(1, 100), floor_full), 4);
	/* and epoch-0 values above the floor stay derivable */
	UT_ASSERT_EQ(cluster_xid_origin_slot_full(fxid(0, 4000000100U), floor_full), 4);
}


/* ----------
 * cluster_xid_next_striped_full
 * ----------
 */

UT_TEST(test_next_striped_in_class)
{
	FullTransactionId r;

	/* exact hit: 100 mod 16 == 4 -> candidate is next itself */
	r = cluster_xid_next_striped_full(fxid(0, 100), 4);
	UT_ASSERT(U64FromFullTransactionId(r) == U64FromFullTransactionId(fxid(0, 100)));

	/* slot behind remainder: round up past the next class boundary */
	r = cluster_xid_next_striped_full(fxid(0, 101), 4);
	UT_ASSERT(U64FromFullTransactionId(r) == U64FromFullTransactionId(fxid(0, 116)));

	/* slot "ahead" of remainder within the same stride block */
	r = cluster_xid_next_striped_full(fxid(0, 100), 7);
	UT_ASSERT(U64FromFullTransactionId(r) == U64FromFullTransactionId(fxid(0, 103)));
}

UT_TEST(test_next_striped_first_normal_edge)
{
	FullTransactionId r;

	/* nextXid at FirstNormalTransactionId: slot 3 is an exact legal hit */
	r = cluster_xid_next_striped_full(fxid(0, FirstNormalTransactionId), 3);
	UT_ASSERT(U64FromFullTransactionId(r) == U64FromFullTransactionId(fxid(0, 3)));

	/* slot 0 at position 0 would be illegal (InvalidTransactionId):
	 * skip to the next in-class position 16 */
	r = cluster_xid_next_striped_full(fxid(0, FirstNormalTransactionId), 0);
	UT_ASSERT(U64FromFullTransactionId(r) == U64FromFullTransactionId(fxid(0, 16)));

	/* defensive: even from position 0 itself */
	r = cluster_xid_next_striped_full(fxid(0, 0), 2);
	UT_ASSERT(U64FromFullTransactionId(r) == U64FromFullTransactionId(fxid(0, 18)));
}

UT_TEST(test_next_striped_epoch_carry_legal)
{
	/* rounding crosses 2^32 into a legal low value: epoch increments,
	 * no skip needed (slot 5 >= FirstNormalTransactionId) */
	FullTransactionId r = cluster_xid_next_striped_full(fxid(0, 0xFFFFFFFE), 5);

	UT_ASSERT(U64FromFullTransactionId(r) == U64FromFullTransactionId(fxid(1, 5)));
}

UT_TEST(test_next_striped_epoch_carry_illegal_skip)
{
	FullTransactionId r;

	/* slot 0: carry lands on low32 == 0 (illegal) -> skip to 16 */
	r = cluster_xid_next_striped_full(fxid(0, 0xFFFFFFF8), 0);
	UT_ASSERT(U64FromFullTransactionId(r) == U64FromFullTransactionId(fxid(1, 16)));

	/* slot 1: carry lands on low32 == 1 (BootstrapTransactionId) -> 17;
	 * epoch base > 0 exercises the same table away from epoch 0 */
	r = cluster_xid_next_striped_full(fxid(2, 0xFFFFFFF2), 1);
	UT_ASSERT(U64FromFullTransactionId(r) == U64FromFullTransactionId(fxid(3, 17)));

	/* slot 2: carry lands on low32 == 2 (FrozenTransactionId) -> 18 */
	r = cluster_xid_next_striped_full(fxid(0, 0xFFFFFFF3), 2);
	UT_ASSERT(U64FromFullTransactionId(r) == U64FromFullTransactionId(fxid(1, 18)));
}

UT_TEST(test_next_striped_invariants_all_slots)
{
	/* Property pins over all 16 slots x assorted starting points:
	 * result >= next, low32 in class, low32 never a special value. */
	static const uint64 starts[] = {
		3,
		100,
		0xFFFFFFF0,
		0x100000000ULL /* {1,0} */,
		0x1FFFFFFFDULL /* {1,0xFFFFFFFD} */,
		0x2000000A0ULL /* {2,160} */
	};
	int slot;
	size_t i;

	for (slot = 0; slot < CLUSTER_XID_STRIDE; slot++) {
		for (i = 0; i < sizeof(starts) / sizeof(starts[0]); i++) {
			FullTransactionId next_full = FullTransactionIdFromU64(starts[i]);
			FullTransactionId r = cluster_xid_next_striped_full(next_full, slot);
			uint32 lo = XidFromFullTransactionId(r);

			UT_ASSERT(U64FromFullTransactionId(r) >= starts[i]);
			UT_ASSERT(lo % CLUSTER_XID_STRIDE == (uint32)slot);
			UT_ASSERT(lo >= FirstNormalTransactionId);
			/* candidate is within one stride block (+ skip) of next */
			UT_ASSERT(U64FromFullTransactionId(r) - starts[i] < 2 * CLUSTER_XID_STRIDE);
		}
	}
}


/* ----------
 * cluster_xid_lag_exceeds (herding slack / hard-limit predicate)
 * ----------
 */

UT_TEST(test_lag_exceeds)
{
	FullTransactionId local = fxid(0, 1000);

	/* equal / local ahead: never exceeds */
	UT_ASSERT(!cluster_xid_lag_exceeds(local, fxid(0, 1000), 100));
	UT_ASSERT(!cluster_xid_lag_exceeds(local, fxid(0, 900), 100));
	/* at threshold: not exceeded (strict >) */
	UT_ASSERT(!cluster_xid_lag_exceeds(local, fxid(0, 1100), 100));
	/* one past threshold: exceeded */
	UT_ASSERT(cluster_xid_lag_exceeds(local, fxid(0, 1101), 100));
	/* epoch-crossing lag measured in full 64-bit space */
	UT_ASSERT(cluster_xid_lag_exceeds(fxid(0, 0xFFFFFFF0), fxid(1, 200), 100));
}


/* ----------
 * runtime wrappers (latch + origin_slot + is_mine)
 * ----------
 */

UT_TEST(test_runtime_inactive_fail_closed)
{
	/* before any latch: everything underivable / not mine */
	stub_next_full = fxid(0, 2000);
	UT_ASSERT_EQ(cluster_xid_origin_slot(1996), -1);
	UT_ASSERT(!cluster_xid_is_mine(1996));
}

UT_TEST(test_runtime_latched_derivation)
{
	stub_next_full = fxid(0, 2000);
	cluster_xid_stripe_latch_runtime(true, 4, fxid(0, 1000));

	/* behind local next, above floor */
	UT_ASSERT_EQ(cluster_xid_origin_slot(1996), 12);
	/* remote xid ahead of local next (within slack window): derivable */
	UT_ASSERT_EQ(cluster_xid_origin_slot(2100), 4);
	/* below floor: underivable */
	UT_ASSERT_EQ(cluster_xid_origin_slot(500), -1);
	/* special xid: underivable */
	UT_ASSERT_EQ(cluster_xid_origin_slot(FrozenTransactionId), -1);

	/* is_mine: slot match on my_slot == 4 */
	UT_ASSERT(cluster_xid_is_mine(1988));  /* 1988 mod 16 == 4 */
	UT_ASSERT(!cluster_xid_is_mine(1989)); /* slot 5 */
	UT_ASSERT(!cluster_xid_is_mine(500));  /* underivable -> false */

	/* reset for other tests */
	cluster_xid_stripe_latch_runtime(false, -1, InvalidFullTransactionId);
}

UT_TEST(test_runtime_latched_wrap_floor)
{
	/* post-wrap: floor near top of epoch 0, local next in epoch 1 */
	stub_next_full = fxid(1, 1000);
	cluster_xid_stripe_latch_runtime(true, 0, fxid(0, 4000000000U));

	/* new epoch-1 xid: derivable (P1-1 pin at wrapper level) */
	UT_ASSERT_EQ(cluster_xid_origin_slot(100), 4);
	/* old epoch-0 xid above the floor: derivable via behind-window */
	UT_ASSERT_EQ(cluster_xid_origin_slot(4000000100U), 4);

	cluster_xid_stripe_latch_runtime(false, -1, InvalidFullTransactionId);
	stub_next_full = fxid(0, 0);
}

UT_TEST(test_allocation_slot_gate)
{
	/* off in every dimension by default */
	UT_ASSERT_EQ(cluster_xid_allocation_slot(), -1);

	/* fully enabled with a stripeable identity: slot == node_id */
	cluster_enabled = true;
	cluster_xid_striping = true;
	cluster_node_id = 5;
	UT_ASSERT_EQ(cluster_xid_allocation_slot(), 5);
	cluster_node_id = 0;
	UT_ASSERT_EQ(cluster_xid_allocation_slot(), 0);
	cluster_node_id = CLUSTER_XID_STRIDE - 1;
	UT_ASSERT_EQ(cluster_xid_allocation_slot(), 15);

	/* identity outside the stripe width: vanilla (boot validation in
	 * cluster_init_shmem makes this unreachable on an enabled node) */
	cluster_node_id = CLUSTER_XID_STRIDE;
	UT_ASSERT_EQ(cluster_xid_allocation_slot(), -1);
	cluster_node_id = -1;
	UT_ASSERT_EQ(cluster_xid_allocation_slot(), -1);

	/* each gate dimension individually off */
	cluster_node_id = 5;
	cluster_xid_striping = false;
	UT_ASSERT_EQ(cluster_xid_allocation_slot(), -1);
	cluster_xid_striping = true;
	cluster_enabled = false;
	UT_ASSERT_EQ(cluster_xid_allocation_slot(), -1);

	/* reset */
	cluster_enabled = false;
	cluster_xid_striping = false;
	cluster_node_id = -1;
}

UT_TEST(test_runtime_latch_defensive)
{
	/* active=true with an out-of-range slot is treated as inactive
	 * (fail-closed, no partial activation) */
	stub_next_full = fxid(0, 2000);
	cluster_xid_stripe_latch_runtime(true, CLUSTER_XID_STRIDE, fxid(0, 1000));
	UT_ASSERT_EQ(cluster_xid_origin_slot(1996), -1);

	/* active=true with an invalid floor likewise */
	cluster_xid_stripe_latch_runtime(true, 4, InvalidFullTransactionId);
	UT_ASSERT_EQ(cluster_xid_origin_slot(1996), -1);

	cluster_xid_stripe_latch_runtime(false, -1, InvalidFullTransactionId);
}


/* ----------
 * Durable stripe records (D5a): "PGXS" / "PGXA" integrity truth tables.
 * Every corruption / sanity failure must reject (treat-as-absent,
 * fail-closed); the roundtrip with a recomputed CRC must accept.
 * ----------
 */

/* Build a valid, CRC-stamped slot record for stripe slot `node`. */
static ClusterXidStripeSlotRecord
make_slot_record(int32 node)
{
	ClusterXidStripeSlotRecord r;

	memset(&r, 0, sizeof(r));
	r.magic = CLUSTER_PGXS_MAGIC;
	r.version = CLUSTER_PGXS_VERSION;
	r.node_id = node;
	r.retired = 0;
	r.owner_incarnation = UINT64CONST(778899001122);
	r.floor_full = (UINT64CONST(1) << 32) | 0x10; /* epoch-1 floor */
	r.next_xid_hwm_full = (UINT64CONST(1) << 32) | 0x200;
	r.stride_mode_epoch = 1;
	r.generation = 1;
	cluster_xid_stripe_slot_record_compute_crc(&r);
	return r;
}

/* Build a valid, CRC-stamped activation record. */
static ClusterXidStripeActivationRecord
make_activation_record(void)
{
	ClusterXidStripeActivationRecord r;

	memset(&r, 0, sizeof(r));
	r.magic = CLUSTER_PGXA_MAGIC;
	r.version = CLUSTER_PGXA_VERSION;
	r.activated_floor_full = (UINT64CONST(1) << 32) | 0x10;
	r.stride_mode_epoch = 1;
	r.generation = 1;
	cluster_xid_stripe_activation_record_compute_crc(&r);
	return r;
}

UT_TEST(test_slot_record_roundtrip_valid)
{
	ClusterXidStripeSlotRecord r = make_slot_record(4);

	UT_ASSERT(cluster_xid_stripe_slot_record_valid(&r, 4));

	/* retired=1 is a legal domain value (5.18 removal cross-write) */
	r.retired = 1;
	cluster_xid_stripe_slot_record_compute_crc(&r);
	UT_ASSERT(cluster_xid_stripe_slot_record_valid(&r, 4));

	/* hwm == floor is legal (freshly claimed slot) */
	r = make_slot_record(0);
	r.next_xid_hwm_full = r.floor_full;
	cluster_xid_stripe_slot_record_compute_crc(&r);
	UT_ASSERT(cluster_xid_stripe_slot_record_valid(&r, 0));

	/* NULL rejects */
	UT_ASSERT(!cluster_xid_stripe_slot_record_valid(NULL, 0));
}

UT_TEST(test_slot_record_sanity_rejections)
{
	ClusterXidStripeSlotRecord r;

	/* each corruption is applied to a fresh valid record and CRC is
	 * RECOMPUTED, so the sanity gate itself (not the CRC) must reject */
	r = make_slot_record(4);
	r.magic = CLUSTER_PGXS_MAGIC + 1;
	cluster_xid_stripe_slot_record_compute_crc(&r);
	UT_ASSERT(!cluster_xid_stripe_slot_record_valid(&r, 4));

	r = make_slot_record(4);
	r.version = CLUSTER_PGXS_VERSION + 1;
	cluster_xid_stripe_slot_record_compute_crc(&r);
	UT_ASSERT(!cluster_xid_stripe_slot_record_valid(&r, 4));

	r = make_slot_record(4);
	r.node_id = -1;
	cluster_xid_stripe_slot_record_compute_crc(&r);
	UT_ASSERT(!cluster_xid_stripe_slot_record_valid(&r, -1));

	r = make_slot_record(4);
	r.node_id = CLUSTER_XID_STRIDE;
	cluster_xid_stripe_slot_record_compute_crc(&r);
	UT_ASSERT(!cluster_xid_stripe_slot_record_valid(&r, CLUSTER_XID_STRIDE));

	/* misplaced write: record names slot 4, read from slot 5 */
	r = make_slot_record(4);
	UT_ASSERT(!cluster_xid_stripe_slot_record_valid(&r, 5));

	r = make_slot_record(4);
	r.retired = 2;
	cluster_xid_stripe_slot_record_compute_crc(&r);
	UT_ASSERT(!cluster_xid_stripe_slot_record_valid(&r, 4));

	r = make_slot_record(4);
	r.owner_incarnation = 0;
	cluster_xid_stripe_slot_record_compute_crc(&r);
	UT_ASSERT(!cluster_xid_stripe_slot_record_valid(&r, 4));

	r = make_slot_record(4);
	r.floor_full = 0;
	cluster_xid_stripe_slot_record_compute_crc(&r);
	UT_ASSERT(!cluster_xid_stripe_slot_record_valid(&r, 4));

	r = make_slot_record(4);
	r.next_xid_hwm_full = r.floor_full - 1;
	cluster_xid_stripe_slot_record_compute_crc(&r);
	UT_ASSERT(!cluster_xid_stripe_slot_record_valid(&r, 4));

	r = make_slot_record(4);
	r.stride_mode_epoch = 0;
	cluster_xid_stripe_slot_record_compute_crc(&r);
	UT_ASSERT(!cluster_xid_stripe_slot_record_valid(&r, 4));

	r = make_slot_record(4);
	r.generation = 0;
	cluster_xid_stripe_slot_record_compute_crc(&r);
	UT_ASSERT(!cluster_xid_stripe_slot_record_valid(&r, 4));
}

UT_TEST(test_slot_record_crc_rejections)
{
	ClusterXidStripeSlotRecord r;

	/* payload bit flipped after the CRC stamp -> CRC mismatch */
	r = make_slot_record(4);
	r.next_xid_hwm_full += 16;
	UT_ASSERT(!cluster_xid_stripe_slot_record_valid(&r, 4));

	/* CRC field itself corrupted */
	r = make_slot_record(4);
	r.crc32c ^= 1;
	UT_ASSERT(!cluster_xid_stripe_slot_record_valid(&r, 4));

	/* all-zeros slot (lazily materialised region reads back zeros) */
	memset(&r, 0, sizeof(r));
	UT_ASSERT(!cluster_xid_stripe_slot_record_valid(&r, 0));
}

UT_TEST(test_activation_record_roundtrip_valid)
{
	ClusterXidStripeActivationRecord r = make_activation_record();

	UT_ASSERT(cluster_xid_stripe_activation_record_valid(&r));
	UT_ASSERT(!cluster_xid_stripe_activation_record_valid(NULL));
}

UT_TEST(test_activation_record_sanity_rejections)
{
	ClusterXidStripeActivationRecord r;

	r = make_activation_record();
	r.magic = CLUSTER_PGXA_MAGIC + 1;
	cluster_xid_stripe_activation_record_compute_crc(&r);
	UT_ASSERT(!cluster_xid_stripe_activation_record_valid(&r));

	r = make_activation_record();
	r.version = CLUSTER_PGXA_VERSION + 1;
	cluster_xid_stripe_activation_record_compute_crc(&r);
	UT_ASSERT(!cluster_xid_stripe_activation_record_valid(&r));

	r = make_activation_record();
	r.activated_floor_full = 0;
	cluster_xid_stripe_activation_record_compute_crc(&r);
	UT_ASSERT(!cluster_xid_stripe_activation_record_valid(&r));

	r = make_activation_record();
	r.stride_mode_epoch = 0;
	cluster_xid_stripe_activation_record_compute_crc(&r);
	UT_ASSERT(!cluster_xid_stripe_activation_record_valid(&r));

	r = make_activation_record();
	r.generation = 0;
	cluster_xid_stripe_activation_record_compute_crc(&r);
	UT_ASSERT(!cluster_xid_stripe_activation_record_valid(&r));
}

UT_TEST(test_activation_record_crc_rejections)
{
	ClusterXidStripeActivationRecord r;

	r = make_activation_record();
	r.activated_floor_full += 16;
	UT_ASSERT(!cluster_xid_stripe_activation_record_valid(&r));

	r = make_activation_record();
	r.crc32c ^= 1;
	UT_ASSERT(!cluster_xid_stripe_activation_record_valid(&r));

	memset(&r, 0, sizeof(r));
	UT_ASSERT(!cluster_xid_stripe_activation_record_valid(&r));
}


int
main(void)
{
	UT_PLAN(26);

	UT_RUN(test_widen_behind_same_epoch);
	UT_RUN(test_widen_ahead_same_epoch);
	UT_RUN(test_widen_behind_epoch_crossing);
	UT_RUN(test_widen_ahead_epoch_crossing);
	UT_RUN(test_widen_epoch0_underflow_guard);
	UT_RUN(test_widen_special_xids);

	UT_RUN(test_origin_slot_full_basic);
	UT_RUN(test_origin_slot_full_invalid_args);
	UT_RUN(test_origin_slot_full_wrap_regression_pin);

	UT_RUN(test_next_striped_in_class);
	UT_RUN(test_next_striped_first_normal_edge);
	UT_RUN(test_next_striped_epoch_carry_legal);
	UT_RUN(test_next_striped_epoch_carry_illegal_skip);
	UT_RUN(test_next_striped_invariants_all_slots);

	UT_RUN(test_lag_exceeds);

	UT_RUN(test_runtime_inactive_fail_closed);
	UT_RUN(test_runtime_latched_derivation);
	UT_RUN(test_runtime_latched_wrap_floor);
	UT_RUN(test_allocation_slot_gate);
	UT_RUN(test_runtime_latch_defensive);

	UT_RUN(test_slot_record_roundtrip_valid);
	UT_RUN(test_slot_record_sanity_rejections);
	UT_RUN(test_slot_record_crc_rejections);
	UT_RUN(test_activation_record_roundtrip_valid);
	UT_RUN(test_activation_record_sanity_rejections);
	UT_RUN(test_activation_record_crc_rejections);

	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
