/* Real freeze-boundary consumer with original stripe widening arithmetic.
 * Author: SqlRush <sqlrush@gmail.com> */
int heap_horizon_stripe_fixture_main(void);
#define main heap_horizon_stripe_fixture_main
#include "test_cluster_xid_stripe.c"
#undef main
#include "cluster/cluster_heap_horizon.h"
#include "cluster/cluster_undo_horizon.h"
#include "access/multixact.h"
#include "utils/rel.h"

/* Compile the actual production SCN-time comparator, including cross-node ties. */
#include "test_cluster_heap_horizon_scn_time.inc"

TransactionId RecentXmin;
MultiXactId
GetOldestMultiXactId(void)
{
	return 42;
}
/* Execute the original metadata prefix, stopping at physical file creation. */
#include "test_cluster_heap_horizon_create.inc"

bool cluster_shared_config;
static bool activation_available;
static uint64 activation_floor, activation_epoch, activation_generation;
static unsigned activation_reads;

bool
cluster_xid_stripe_get_activation(uint64 *floor, uint64 *epoch, uint64 *generation)
{
	activation_reads++;
	if (!activation_available)
		return false;
	*floor = activation_floor;
	*epoch = activation_epoch;
	*generation = activation_generation;
	return true;
}

static void
setup_horizon(void)
{
	cluster_enabled = cluster_shared_config = cluster_xid_striping = true;
	activation_available = true;
	activation_floor = 4195104;
	activation_epoch = activation_generation = 1;
	activation_reads = 0;
	stub_next_full = FullTransactionIdFromU64(4195121);
}

#include "test_cluster_heap_horizon_vacuum_fixture.inc"

UT_TEST(policy_sample_retains_original_legal_next_and_allocation_fence)
{
	ClusterHeapFreezePolicySampleV1 sample = {0};
	ClusterXidStripeSampleFence expected = {
		.activation_floor_full = UINT64CONST(0x200000004),
		.activation_epoch = 17,
		.activation_generation = 29,
		.slot_floor_full = UINT64CONST(0x200000008),
		.slot_owner_incarnation = 73,
		.incarnation = 81,
		.lease_floor_full = UINT64CONST(0x20000000c),
		.lease_limit_full = UINT64CONST(0x200010000),
		.slot = 2
	};

	setup_age();
	stub_next_full = age_cache.nextXid = fxid(2, 100);
	age_future = fxid(2, 1000);
	age_sample_fence = expected;
	UT_ASSERT(cluster_heap_freeze_policy_sample_v1(80, 10, 180, 100000, &sample));
	UT_ASSERT(FullTransactionIdEquals(sample.legal_next, fxid(2, 1000)));
	UT_ASSERT(FullTransactionIdEquals(sample.freeze_limit, fxid(2, 80)));
	UT_ASSERT_EQ(sample.allocation_fence.incarnation, 81);
	UT_ASSERT_EQ(sample.allocation_fence.activation_floor_full, expected.activation_floor_full);
	UT_ASSERT_EQ(sample.allocation_fence.activation_epoch, expected.activation_epoch);
	UT_ASSERT_EQ(sample.allocation_fence.activation_generation, expected.activation_generation);
	UT_ASSERT_EQ(sample.allocation_fence.slot_floor_full, expected.slot_floor_full);
	UT_ASSERT_EQ(sample.allocation_fence.slot_owner_incarnation, expected.slot_owner_incarnation);
	UT_ASSERT_EQ(sample.allocation_fence.lease_floor_full, expected.lease_floor_full);
	UT_ASSERT_EQ(sample.allocation_fence.lease_limit_full, expected.lease_limit_full);
	UT_ASSERT_EQ(sample.allocation_fence.slot, expected.slot);
	UT_ASSERT(!sample.aggressive);
	UT_ASSERT_EQ(age_sample_reads, 1);
	UT_ASSERT_EQ(age_watermark_reads, 1);
	UT_ASSERT_EQ(age_fence_reads, 1);
	UT_ASSERT_EQ(age_lock_acquires, age_lock_releases);
}

UT_TEST(policy_sample_refusal_preserves_complete_output)
{
	for (unsigned variant = 0; variant < 4; variant++)
	{
		ClusterHeapFreezePolicySampleV1 sample, before;

		setup_age();
		memset(&sample, 0xA5, sizeof(sample));
		before = sample;
		if (variant == 0) age_promise_ready = false;
		if (variant == 1) age_promise_current = false;
		if (variant == 2) age_future = InvalidFullTransactionId;
		UT_ASSERT(!cluster_heap_freeze_policy_sample_v1(80, 10,
			variant == 3 ? -1 : 180, 100000, &sample));
		UT_ASSERT(memcmp(&sample, &before, sizeof(sample)) == 0);
		UT_ASSERT(!age_xid_lock);
	}
	UT_ASSERT(!cluster_heap_freeze_policy_sample_v1(80, 10, 180, 100000, NULL));
}

UT_TEST(policy_sample_rechecks_original_fence_without_resampling_next)
{
	ClusterHeapFreezePolicySampleV1 sample = {0};

	setup_age();
	stub_next_full = age_cache.nextXid = fxid(2, 100);
	age_future = fxid(2, 1000);
	UT_ASSERT(cluster_heap_freeze_policy_sample_v1(80, 10, 180, 100000, &sample));
	stub_next_full = age_cache.nextXid = fxid(3, 200);
	age_future = fxid(3, 2000);
	UT_ASSERT(cluster_heap_freeze_policy_sample_current_v1(&sample));
	UT_ASSERT_EQ(age_sample_reads, 1);
	UT_ASSERT_EQ(age_watermark_reads, 1);
	UT_ASSERT_EQ(age_fence_reads, 2);
	UT_ASSERT(FullTransactionIdEquals(sample.legal_next, fxid(2, 1000)));
	age_promise_current = false;
	UT_ASSERT(!cluster_heap_freeze_policy_sample_current_v1(&sample));
	UT_ASSERT_EQ(age_sample_reads, 1);
	UT_ASSERT_EQ(age_fence_reads, 3);
}

UT_TEST(policy_sample_current_refuses_unrepresentable_bounds_before_fence_read)
{
	ClusterHeapFreezePolicySampleV1 sample = {0};

	setup_age();
	stub_next_full = age_cache.nextXid = fxid(2, 100);
	age_future = fxid(2, 1000);
	UT_ASSERT(cluster_heap_freeze_policy_sample_v1(80, 10, 180, 100000, &sample));
	UT_ASSERT(!cluster_heap_freeze_policy_sample_current_v1(NULL));
	for (unsigned variant = 0; variant < 7; variant++)
	{
		ClusterHeapFreezePolicySampleV1 bad = sample;

		if (variant == 0) bad.legal_next = InvalidFullTransactionId;
		if (variant == 1) bad.freeze_limit = InvalidFullTransactionId;
		if (variant == 2) bad.legal_next = FullTransactionIdFromU64(UINT64_MAX);
		if (variant == 3) bad.freeze_limit = FullTransactionIdFromU64(UINT64_MAX);
		if (variant == 4) bad.freeze_limit = fxid(2, 1001);
		if (variant == 5) bad.legal_next = FullTransactionIdFromU64(
			U64FromFullTransactionId(bad.freeze_limit) + UINT64CONST(0x80000000));
		if (variant == 6) bad.freeze_limit = fxid(2, 1);
		UT_ASSERT(!cluster_heap_freeze_policy_sample_current_v1(&bad));
		UT_ASSERT_EQ(age_fence_reads, 1);
		UT_ASSERT_EQ(age_sample_reads, 1);
	}
}

UT_TEST(full_policy_retains_the_sampled_legal_epoch)
{
	for (uint32 epoch = 1; epoch <= 2; epoch++) {
		FullTransactionId cutoff = fxid(9, 42);
		bool aggressive = true;

		setup_age();
		stub_next_full = age_cache.nextXid = fxid(epoch, 100);
		age_future = fxid(epoch, 1000);
		UT_ASSERT(cluster_heap_freeze_policy_full_v1(80, 10, 180, 100000,
			&cutoff, &aggressive));
		UT_ASSERT(FullTransactionIdEquals(cutoff, fxid(epoch, 80)));
		UT_ASSERT(!aggressive);
		UT_ASSERT_EQ(age_sample_reads, 1);
		UT_ASSERT_EQ(age_watermark_reads, 1);
		UT_ASSERT_EQ(age_fence_reads, 1);
		UT_ASSERT_EQ(age_lock_acquires, age_lock_releases);
		UT_ASSERT(!age_xid_lock);
	}
}

UT_TEST(full_policy_keeps_prior_epoch_floor_and_raw_wrapper_compatibility)
{
	FullTransactionId cutoff;
	TransactionId raw = 0;
	bool aggressive, raw_aggressive;

	setup_age();
	stub_next_full = age_cache.nextXid = fxid(2, 100);
	age_future = fxid(2, 1000);
	age_watermark_ready = true;
	age_watermark.active_full_xid_min = U64FromFullTransactionId(fxid(1, UINT32_MAX - 20));
	UT_ASSERT(cluster_heap_freeze_policy_full_v1(80, 10, 180, 100000, &cutoff, &aggressive));
	UT_ASSERT(FullTransactionIdEquals(cutoff, fxid(1, UINT32_MAX - 20)));
	UT_ASSERT(cluster_heap_freeze_policy_v1(80, 10, 180, 100000, &raw, &raw_aggressive));
	UT_ASSERT_EQ(raw, XidFromFullTransactionId(cutoff));
	UT_ASSERT_EQ(raw_aggressive, aggressive);
	UT_ASSERT_EQ(age_sample_reads, 2);
}

UT_TEST(full_policy_refusal_preserves_both_outputs)
{
	for (int variant = 0; variant < 8; variant++) {
		FullTransactionId cutoff = fxid(9, 42);
		bool aggressive = true;

		setup_age();
		if (variant == 0) age_promise_ready = false;
		if (variant == 1) age_revoke_on_release = true;
		if (variant == 2) age_revoke_on_watermark = true;
		if (variant == 3) {
			stub_next_full = age_cache.nextXid = fxid(1, 100);
			age_future = fxid(1, 1000);
			age_watermark_ready = true;
			age_watermark.active_full_xid_min = FirstNormalTransactionId;
		}
		UT_ASSERT(!cluster_heap_freeze_policy_full_v1(variant == 4 ? 0 : 80,
			variant == 5 ? 0 : 10, variant == 6 ? -1 : 180,
			variant == 7 ? -1 : 100000, &cutoff, &aggressive));
		UT_ASSERT(FullTransactionIdEquals(cutoff, fxid(9, 42)));
		UT_ASSERT(aggressive);
		UT_ASSERT_EQ(age_lock_acquires, age_lock_releases);
	}
	{
		FullTransactionId cutoff = fxid(9, 42);
		bool aggressive = true;
		setup_age();
		UT_ASSERT(!cluster_heap_freeze_policy_full_v1(80, 10, 180, 100000, NULL, &aggressive));
		UT_ASSERT(!cluster_heap_freeze_policy_full_v1(80, 10, 180, 100000, &cutoff, NULL));
		UT_ASSERT(FullTransactionIdEquals(cutoff, fxid(9, 42)) && aggressive);
		UT_ASSERT_EQ(age_sample_reads, 0);
	}
}

UT_TEST(real_vacuum_retains_full_policy_before_both_scan_exits)
{
	for (int variant = 0; variant < 2; variant++) {
		struct VacuumCutoffs cutoffs;
		memset(&cutoffs, 0xA5, sizeof(cutoffs));
		setup_age();
		stub_next_full = age_cache.nextXid = fxid(2, 100);
		activation_floor = U64FromFullTransactionId(fxid(2, 100));
		age_future = fxid(2, 1000);
		cutoffs.SharedFreezeLimitFull = fxid(9, 42);
		UT_ASSERT_EQ(vacuum_policy_scan(10, 180, variant == 0 ? 100000 : 1,
			RELPERSISTENCE_PERMANENT, 90, &cutoffs), variant != 0);
		UT_ASSERT(FullTransactionIdEquals(cutoffs.SharedFreezeLimitFull, fxid(2, 820)));
		UT_ASSERT(FullTransactionIdEquals(cutoffs.SharedFreezePolicySample.legal_next, fxid(2, 1000)));
		UT_ASSERT(FullTransactionIdEquals(cutoffs.SharedFreezePolicySample.freeze_limit, fxid(2, 820)));
		UT_ASSERT_EQ(cutoffs.SharedFreezePolicySample.allocation_fence.incarnation, 81);
		UT_ASSERT_EQ(cutoffs.FreezeLimit, 80);
		UT_ASSERT_EQ(cutoffs.MultiXactCutoff, 42);
		UT_ASSERT_EQ(age_sample_reads, 1);
		UT_ASSERT_EQ(age_watermark_reads, 1);
	}
}

UT_TEST(real_vacuum_shared_age_is_independent_of_native_deletion_ceiling)
{
	const int ages[] = {0, 180, 980};
	const TransactionId shared[] = {900, 820, 20};
	const TransactionId native[] = {80, 80, 20};

	for (unsigned i = 0; i < lengthof(ages); i++) {
		struct VacuumCutoffs cutoffs;
		setup_age();
		age_watermark_ready = true;
		age_watermark.active_full_xid_min = 900;
		UT_ASSERT(!vacuum_policy_scan(10, ages[i], 100000,
			RELPERSISTENCE_PERMANENT, 90, &cutoffs));
		UT_ASSERT(FullTransactionIdEquals(cutoffs.SharedFreezeLimitFull, fxid(0, shared[i])));
		UT_ASSERT(FullTransactionIdEquals(cutoffs.SharedFreezePolicySample.freeze_limit,
			cutoffs.SharedFreezeLimitFull));
		UT_ASSERT_EQ(cutoffs.OldestXmin, 80);
		UT_ASSERT_EQ(cutoffs.FreezeLimit, native[i]);
		UT_ASSERT_EQ(cutoffs.MultiXactCutoff, 42);
		UT_ASSERT_EQ(age_sample_reads, 1); UT_ASSERT_EQ(age_watermark_reads, 1);
		UT_ASSERT_EQ(age_fence_reads, 1);
		UT_ASSERT_EQ(age_lock_acquires, 1); UT_ASSERT_EQ(age_lock_releases, 1);
	}
}

UT_TEST(real_vacuum_shared_age_preserves_full_epoch_above_activation_floor)
{
	struct VacuumCutoffs cutoffs;
	setup_age();
	stub_next_full = age_cache.nextXid = fxid(1, 100);
	activation_floor = U64FromFullTransactionId(fxid(1, 50));
	age_future = fxid(1, 1000);
	age_watermark_ready = true;
	age_watermark.active_full_xid_min = U64FromFullTransactionId(fxid(1, 900));
	UT_ASSERT(!vacuum_policy_scan(10, 0, 100000,
		RELPERSISTENCE_PERMANENT, 90, &cutoffs));
	UT_ASSERT(FullTransactionIdEquals(cutoffs.SharedFreezeLimitFull, fxid(1, 900)));
	UT_ASSERT(FullTransactionIdEquals(cutoffs.SharedFreezePolicySample.legal_next, fxid(1, 1000)));
	UT_ASSERT_EQ(cutoffs.OldestXmin, 50); UT_ASSERT_EQ(cutoffs.FreezeLimit, 50);
	UT_ASSERT_EQ(cutoffs.MultiXactCutoff, 42);
	UT_ASSERT(cluster_heap_freeze_policy_sample_current_v1(&cutoffs.SharedFreezePolicySample));
	UT_ASSERT_EQ(age_sample_reads, 1); UT_ASSERT_EQ(age_watermark_reads, 1);
	UT_ASSERT_EQ(age_lock_acquires, 1); UT_ASSERT_EQ(age_lock_releases, 1);
}

UT_TEST(real_vacuum_discards_previous_full_policy_on_refusal_or_nontarget)
{
	for (int variant = 0; variant < 5; variant++) {
		struct VacuumCutoffs cutoffs;
		setup_age();
		cutoffs.SharedFreezeLimitFull = fxid(9, 42);
		memset(&cutoffs.SharedFreezePolicySample, 0xA5, sizeof(cutoffs.SharedFreezePolicySample));
		if (variant == 0) age_promise_ready = false;
		if (variant == 1) age_revoke_on_release = true;
		if (variant == 2) age_revoke_on_watermark = true;
		if (variant == 3) cluster_shared_config = false;
		(void) vacuum_policy_scan(10, 180, 100000,
			variant == 4 ? RELPERSISTENCE_TEMP : RELPERSISTENCE_PERMANENT, 90, &cutoffs);
		UT_ASSERT(!FullTransactionIdIsValid(cutoffs.SharedFreezeLimitFull));
		{
			ClusterHeapFreezePolicySampleV1 cleared = {0};
			UT_ASSERT(memcmp(&cutoffs.SharedFreezePolicySample, &cleared, sizeof(cleared)) == 0);
		}
		UT_ASSERT_EQ(age_sample_reads, variant >= 3 ? 0 : 1);
	}
}

UT_TEST(published_watermark_caps_existing_vacuum_limit)
{
	struct VacuumCutoffs cutoffs;
	setup_age();
	age_watermark_ready = true;
	age_watermark.active_full_xid_min = 60;
	UT_ASSERT(!vacuum_policy_scan(10, 180, 100000, RELPERSISTENCE_PERMANENT, 90, &cutoffs));
	UT_ASSERT_EQ(cutoffs.FreezeLimit, 60);
	UT_ASSERT_EQ(cutoffs.OldestXmin, 80);
	UT_ASSERT_EQ(cutoffs.MultiXactCutoff, 42);
	UT_ASSERT_EQ(age_watermark_reads, 1);
	UT_ASSERT_EQ(age_sample_reads, 1);
	UT_ASSERT_EQ(age_fence_reads, 1);
	UT_ASSERT_EQ(age_lock_acquires, age_lock_releases);
	UT_ASSERT(!age_xid_lock);
	/* The F cap does not replace the N_legal table-age scan policy. */
	UT_ASSERT(vacuum_policy_scan(10, 180, 500, RELPERSISTENCE_PERMANENT, 90, &cutoffs));
	UT_ASSERT_EQ(cutoffs.FreezeLimit, 60);
}

UT_TEST(published_watermark_never_raises_age_or_activation_ceiling)
{
	struct VacuumCutoffs cutoffs;
	setup_age();
	age_watermark_ready = true;
	age_watermark.active_full_xid_min = 2000;
	UT_ASSERT(!vacuum_policy_scan(10, 180, 100000, RELPERSISTENCE_PERMANENT, 90, &cutoffs));
	UT_ASSERT_EQ(cutoffs.FreezeLimit, 80);
	UT_ASSERT(!vacuum_policy_scan(10, 980, 100000, RELPERSISTENCE_PERMANENT, 90, &cutoffs));
	UT_ASSERT_EQ(cutoffs.FreezeLimit, 20);
	age_watermark.active_full_xid_min = 60;
	activation_floor = 50;
	UT_ASSERT(!vacuum_policy_scan(10, 0, 100000, RELPERSISTENCE_PERMANENT, 90, &cutoffs));
	UT_ASSERT_EQ(cutoffs.FreezeLimit, 50);
}

UT_TEST(published_watermark_minimum_uses_full_xid_epoch)
{
	struct VacuumCutoffs cutoffs;
	setup_age();
	activation_floor = UINT64_C(0x100000000);
	stub_next_full = fxid(1, 100);
	age_cache.nextXid = stub_next_full;
	age_future = fxid(1, 1000);
	age_watermark_ready = true;
	age_watermark.active_full_xid_min = UINT64_C(0xFFFFFF80);
	UT_ASSERT(!vacuum_policy_scan(UINT32_C(0xFFFFF000), 1100, 100000, RELPERSISTENCE_PERMANENT, 90,
								  &cutoffs));
	UT_ASSERT_EQ(cutoffs.FreezeLimit, UINT32_C(0xFFFFFF80));
	UT_ASSERT_EQ(cutoffs.OldestXmin, FirstNormalTransactionId);
}

UT_TEST(published_watermark_ambiguous_floor_refuses_raw_projection)
{
	struct VacuumCutoffs cutoffs;
	TransactionId limit = 777;
	bool aggressive = false;
	setup_age();
	activation_floor = UINT64_C(0x100000064);
	stub_next_full = fxid(1, 200);
	age_cache.nextXid = stub_next_full;
	age_future = fxid(1, 1000);
	age_watermark_ready = true;
	age_watermark.active_full_xid_min = U64FromFullTransactionId(age_future) - UINT64_C(0x80000000);
	UT_ASSERT(!cluster_heap_freeze_policy_v1(80, 10, 180, 100000, &limit, &aggressive));
	UT_ASSERT_EQ(limit, 777);
	UT_ASSERT(!aggressive);
	/* Existing caller requests a scan and retains its conservative fallback. */
	UT_ASSERT(vacuum_policy_scan(10, 180, 100000, RELPERSISTENCE_PERMANENT, 90, &cutoffs));
	UT_ASSERT_EQ(cutoffs.FreezeLimit, 10);
}

UT_TEST(unavailable_watermark_preserves_original_v1_policy)
{
	struct VacuumCutoffs cutoffs;
	setup_age();
	/* Refused bytes carry no meaning and must never become a cutoff. */
	age_watermark.active_full_xid_min = 1;
	UT_ASSERT(!vacuum_policy_scan(10, 180, 100000, RELPERSISTENCE_PERMANENT, 90, &cutoffs));
	UT_ASSERT_EQ(cutoffs.FreezeLimit, 80);
	UT_ASSERT_EQ(age_watermark_reads, 1);
}

UT_TEST(lease_revoked_during_watermark_read_refuses_policy)
{
	struct VacuumCutoffs cutoffs;
	TransactionId limit = 777;
	bool aggressive = false;
	setup_age();
	age_watermark_ready = age_revoke_on_watermark = true;
	age_watermark.active_full_xid_min = 60;
	UT_ASSERT(!cluster_heap_freeze_policy_v1(80, 10, 180, 100000, &limit, &aggressive));
	UT_ASSERT_EQ(limit, 777);
	UT_ASSERT(!aggressive);
	UT_ASSERT_EQ(age_watermark_reads, 1);
	UT_ASSERT_EQ(age_fence_reads, 1);
	UT_ASSERT(vacuum_policy_scan(10, 180, 100000, RELPERSISTENCE_PERMANENT, 90, &cutoffs));
	UT_ASSERT_EQ(cutoffs.FreezeLimit, 10);
}

UT_TEST(nonshared_and_temporary_policy_skip_watermark)
{
	struct VacuumCutoffs cutoffs;
	setup_age();
	age_watermark_ready = true;
	age_watermark.active_full_xid_min = 10;
	cluster_shared_config = false;
	UT_ASSERT(!vacuum_policy_scan(10, 180, 100000, RELPERSISTENCE_PERMANENT, 90, &cutoffs));
	UT_ASSERT_EQ(cutoffs.FreezeLimit, 20);
	UT_ASSERT_EQ(age_watermark_reads, 0);
	setup_age();
	age_watermark_ready = true;
	age_watermark.active_full_xid_min = 10;
	UT_ASSERT(!vacuum_policy_scan(10, 180, 100000, RELPERSISTENCE_TEMP, 90, &cutoffs));
	UT_ASSERT_EQ(cutoffs.FreezeLimit, 20);
	UT_ASSERT_EQ(age_watermark_reads, 0);
}

UT_TEST(legal_min_age_uses_lease_next_but_keeps_original_ceiling)
{
	struct VacuumCutoffs cutoffs;
	setup_age();
	UT_ASSERT(!vacuum_policy_scan(10, 180, 100000, RELPERSISTENCE_PERMANENT, 90, &cutoffs));
	UT_ASSERT_EQ(cutoffs.FreezeLimit, 80);
	UT_ASSERT_EQ(cutoffs.OldestXmin, 80);
	UT_ASSERT_EQ(cutoffs.MultiXactCutoff, 42);
	UT_ASSERT_EQ(age_sample_reads, 1);
	UT_ASSERT_EQ(age_fence_reads, 1);
	UT_ASSERT_EQ(age_lock_acquires, age_lock_releases);
	UT_ASSERT(!age_xid_lock);
	/* A legal-next value cannot move the original activation ceiling. */
	activation_floor = 50;
	UT_ASSERT(!vacuum_policy_scan(10, 180, 100000, RELPERSISTENCE_PERMANENT, 90, &cutoffs));
	UT_ASSERT_EQ(cutoffs.FreezeLimit, 50);
	UT_ASSERT_EQ(cutoffs.OldestXmin, 50);
}

UT_TEST(legal_min_age_subtracts_in_full_xid_epoch)
{
	struct VacuumCutoffs cutoffs;
	setup_age();
	activation_floor = UINT64_C(0x100000000);
	stub_next_full = fxid(1, 100);
	age_cache.nextXid = stub_next_full;
	age_future = fxid(1, 1000);
	UT_ASSERT(!vacuum_policy_scan(UINT32_C(0xFFFFF000), 1100, 100000, RELPERSISTENCE_PERMANENT, 90,
								  &cutoffs));
	UT_ASSERT_EQ(cutoffs.FreezeLimit, UINT32_C(0xFFFFFF9C));
	UT_ASSERT_EQ(cutoffs.OldestXmin, FirstNormalTransactionId);
}

UT_TEST(legal_min_age_saturates_instead_of_raw_underflow)
{
	struct VacuumCutoffs cutoffs;
	setup_age();
	activation_floor = 10;
	stub_next_full = FullTransactionIdFromU64(20);
	age_cache.nextXid = stub_next_full;
	age_future = FullTransactionIdFromU64(30);
	UT_ASSERT(!vacuum_policy_scan(5, 100, 100000, RELPERSISTENCE_PERMANENT, 90, &cutoffs));
	UT_ASSERT_EQ(cutoffs.FreezeLimit, FirstNormalTransactionId);
	UT_ASSERT_EQ(cutoffs.OldestXmin, 10);
}

UT_TEST(legal_min_age_respects_default_and_native_safety_clamp)
{
	struct VacuumCutoffs cutoffs;
	setup_age();
	vacuum_freeze_min_age = 180;
	UT_ASSERT(!vacuum_policy_scan(5, -1, 100000, RELPERSISTENCE_PERMANENT, 90, &cutoffs));
	UT_ASSERT_EQ(cutoffs.FreezeLimit, 80);
	age_future = FullTransactionIdFromU64(210);
	autovacuum_freeze_max_age = 400;
	UT_ASSERT(!vacuum_policy_scan(5, 1000, 100000, RELPERSISTENCE_PERMANENT, 90, &cutoffs));
	UT_ASSERT_EQ(cutoffs.FreezeLimit, 10);
}

UT_TEST(legal_min_age_freeze_zero_still_obeys_activation_ceiling)
{
	struct VacuumCutoffs cutoffs;
	setup_age();
	activation_floor = 50;
	(void)vacuum_policy_scan(10, 0, 0, RELPERSISTENCE_PERMANENT, 90, &cutoffs);
	UT_ASSERT_EQ(cutoffs.FreezeLimit, 50);
	UT_ASSERT_EQ(cutoffs.OldestXmin, 50);
	UT_ASSERT_EQ(age_sample_reads, 1);
}

UT_TEST(legal_min_age_refuses_future_catalog_without_new_freeze)
{
	struct VacuumCutoffs cutoffs;
	setup_age();
	age_future = FullTransactionIdFromU64(30);
	UT_ASSERT(vacuum_policy_scan(40, 180, 100000, RELPERSISTENCE_PERMANENT, 90, &cutoffs));
	UT_ASSERT_EQ(cutoffs.FreezeLimit, 40);
	UT_ASSERT_EQ(cutoffs.OldestXmin, 80);
}

UT_TEST(nonshared_and_temporary_min_age_stay_native)
{
	struct VacuumCutoffs cutoffs;
	setup_age();
	cluster_shared_config = false;
	(void)vacuum_policy_scan(10, 180, 100000, RELPERSISTENCE_PERMANENT, 90, &cutoffs);
	UT_ASSERT_EQ(cutoffs.FreezeLimit, 20);
	cluster_shared_config = true;
	(void)vacuum_policy_scan(10, 180, 100000, RELPERSISTENCE_TEMP, 90, &cutoffs);
	UT_ASSERT_EQ(cutoffs.FreezeLimit, 20);
	UT_ASSERT_EQ(age_sample_reads, 0);
	UT_ASSERT_EQ(age_lock_acquires, 0);
}

UT_TEST(legal_min_age_skips_special_ids_after_full_subtraction)
{
	struct VacuumCutoffs cutoffs;
	setup_age();
	activation_floor = UINT64_C(0x100000000);
	stub_next_full = fxid(1, 100);
	age_cache.nextXid = stub_next_full;
	age_future = fxid(1, 1000);
	UT_ASSERT(!vacuum_policy_scan(UINT32_C(0xFFFFF000), 999, 100000, RELPERSISTENCE_PERMANENT, 90,
								  &cutoffs));
	UT_ASSERT_EQ(cutoffs.FreezeLimit, FirstNormalTransactionId);
}

UT_TEST(missing_min_age_evidence_preserves_oldest_xmin_ceiling)
{
	struct VacuumCutoffs cutoffs;
	setup_age();
	age_promise_ready = false;
	age_oldest_xmin = 20;
	UT_ASSERT(vacuum_policy_scan(40, 100, 100000, RELPERSISTENCE_PERMANENT, 90, &cutoffs));
	UT_ASSERT_EQ(cutoffs.FreezeLimit, 20);
	UT_ASSERT_EQ(cutoffs.OldestXmin, 20);
}

UT_TEST(ambiguous_min_age_ceiling_requests_scan_without_advancing)
{
	struct VacuumCutoffs cutoffs;
	setup_age();
	age_future = FullTransactionIdFromU64(UINT32_C(0x80000050));
	UT_ASSERT(vacuum_policy_scan(80, 180, 100000, RELPERSISTENCE_PERMANENT, 90, &cutoffs));
	UT_ASSERT_EQ(cutoffs.FreezeLimit, 80);
	UT_ASSERT_EQ(cutoffs.OldestXmin, 80);
}

UT_TEST(legal_age_requests_scan_when_local_next_lags)
{
	struct VacuumCutoffs cutoffs;
	setup_age();
	UT_ASSERT(vacuum_age_scan(40, 500, RELPERSISTENCE_PERMANENT, 90, &cutoffs));
	UT_ASSERT_EQ(age_sample_reads, 1);
	UT_ASSERT_EQ(age_fence_reads, 1);
	UT_ASSERT_EQ(age_lock_acquires, 1);
	UT_ASSERT_EQ(age_lock_releases, 1);
	UT_ASSERT(!age_xid_lock);
	/* Scanning must not itself raise tuple freeze or MX cutoffs. */
	UT_ASSERT_EQ(cutoffs.FreezeLimit, 80);
	UT_ASSERT_EQ(cutoffs.MultiXactCutoff, 42);
}

UT_TEST(legal_age_threshold_is_inclusive_without_raw_underflow)
{
	struct VacuumCutoffs cutoffs;
	setup_age();
	UT_ASSERT(vacuum_age_scan(40, 960, RELPERSISTENCE_PERMANENT, 90, &cutoffs));
	UT_ASSERT(!vacuum_age_scan(40, 961, RELPERSISTENCE_PERMANENT, 90, &cutoffs));
	UT_ASSERT(!vacuum_age_scan(40, 100000, RELPERSISTENCE_PERMANENT, 90, &cutoffs));
	UT_ASSERT_EQ(age_sample_reads, 3);
}

UT_TEST(legal_age_crosses_epoch_with_full_xid_difference)
{
	struct VacuumCutoffs cutoffs;
	setup_age();
	activation_floor = UINT64_C(0x100000000);
	stub_next_full = fxid(1, 100);
	age_cache.nextXid = stub_next_full;
	age_future = fxid(1, 1000);
	UT_ASSERT(vacuum_age_scan(UINT32_C(0xFFFFFFF0), 1016, RELPERSISTENCE_PERMANENT, 90, &cutoffs));
	UT_ASSERT(!vacuum_age_scan(UINT32_C(0xFFFFFFF0), 1017, RELPERSISTENCE_PERMANENT, 90, &cutoffs));
}

UT_TEST(missing_age_promise_requests_scan_without_freeze_permission)
{
	struct VacuumCutoffs cutoffs;
	setup_age();
	age_promise_ready = false;
	UT_ASSERT(vacuum_age_scan(40, 500, RELPERSISTENCE_PERMANENT, 90, &cutoffs));
	UT_ASSERT_EQ(age_sample_reads, 1);
	UT_ASSERT_EQ(age_lock_acquires, age_lock_releases);
	UT_ASSERT_EQ(cutoffs.FreezeLimit, 40);
}

UT_TEST(age_promise_revoked_after_unlock_requests_scan)
{
	struct VacuumCutoffs cutoffs;
	setup_age();
	age_revoke_on_release = true;
	UT_ASSERT(vacuum_age_scan(40, 1000, RELPERSISTENCE_PERMANENT, 90, &cutoffs));
	UT_ASSERT_EQ(age_fence_reads, 1);
	UT_ASSERT_EQ(age_lock_acquires, age_lock_releases);
	UT_ASSERT(!age_xid_lock);
	UT_ASSERT_EQ(cutoffs.FreezeLimit, 40);
}

UT_TEST(legal_age_respects_native_default_and_safety_clamp)
{
	struct VacuumCutoffs cutoffs;
	setup_age();
	UT_ASSERT(vacuum_age_scan(40, -1, RELPERSISTENCE_PERMANENT, 90, &cutoffs));
	autovacuum_freeze_max_age = 1000;
	UT_ASSERT(vacuum_age_scan(40, 100000, RELPERSISTENCE_PERMANENT, 90, &cutoffs));
}

UT_TEST(nonshared_and_temporary_age_stay_native)
{
	struct VacuumCutoffs cutoffs;
	setup_age();
	cluster_shared_config = false;
	UT_ASSERT(!vacuum_age_scan(40, 500, RELPERSISTENCE_PERMANENT, 90, &cutoffs));
	UT_ASSERT(vacuum_age_scan(40, 100, RELPERSISTENCE_PERMANENT, 90, &cutoffs));
	cluster_shared_config = true;
	UT_ASSERT(!vacuum_age_scan(40, 500, RELPERSISTENCE_TEMP, 90, &cutoffs));
	UT_ASSERT(vacuum_age_scan(40, 100, RELPERSISTENCE_TEMP, 90, &cutoffs));
	UT_ASSERT_EQ(age_sample_reads, 0);
	UT_ASSERT_EQ(age_lock_acquires, 0);
}

UT_TEST(young_legal_xid_keeps_independent_multixact_scan_trigger)
{
	struct VacuumCutoffs cutoffs;
	setup_age();
	UT_ASSERT(!vacuum_age_scan(40, 1000, RELPERSISTENCE_PERMANENT, 90, &cutoffs));
	vacuum_multixact_freeze_table_age = 50;
	UT_ASSERT(vacuum_age_scan(40, 1000, RELPERSISTENCE_PERMANENT, 50, &cutoffs));
	vacuum_multixact_freeze_table_age = 10000;
}

UT_TEST(slower_writer_can_issue_below_local_freeze_proposal)
{
	TransactionId cutoff = InvalidTransactionId;
	setup_horizon();
	UT_ASSERT(cluster_heap_freeze_cutoff_v1(4195120, &cutoff));
	UT_ASSERT_EQ(cutoff, 4195104);
	UT_ASSERT(cutoff <= 4195105);
	UT_ASSERT_EQ(activation_reads, 1);
}

UT_TEST(older_local_cutoff_is_never_advanced)
{
	TransactionId cutoff;
	setup_horizon();
	UT_ASSERT(cluster_heap_freeze_cutoff_v1(793, &cutoff));
	UT_ASSERT_EQ(cutoff, 793);
	/* Before this idle member allocates its first striped XID. */
	stub_next_full = FullTransactionIdFromU64(794);
	UT_ASSERT(cluster_heap_freeze_cutoff_v1(793, &cutoff));
	UT_ASSERT_EQ(cutoff, 793);
}

UT_TEST(missing_or_invalid_authority_preserves_output)
{
	for (unsigned fault = 0; fault < 7; fault++) {
		TransactionId cutoff = 1234;
		setup_horizon();
		if (fault == 0)
			activation_available = false;
		if (fault == 1)
			activation_floor = 0;
		if (fault == 2)
			activation_epoch = 0;
		if (fault == 3)
			activation_generation = 0;
		if (fault == 4)
			cluster_xid_striping = false;
		if (fault == 5)
			stub_next_full = InvalidFullTransactionId;
		UT_ASSERT(
			!cluster_heap_freeze_cutoff_v1(fault == 6 ? FrozenTransactionId : 4195120, &cutoff));
		UT_ASSERT_EQ(cutoff, 1234);
	}
	UT_ASSERT(!cluster_heap_freeze_cutoff_v1(4195120, NULL));
}

UT_TEST(full_xid_window_cannot_alias_old_activation)
{
	TransactionId cutoff = 1234;
	setup_horizon();
	stub_next_full = FullTransactionIdFromU64(activation_floor + UINT64_C(0x80000000));
	UT_ASSERT(
		!cluster_heap_freeze_cutoff_v1((uint32)U64FromFullTransactionId(stub_next_full), &cutoff));
	UT_ASSERT_EQ(cutoff, 1234);
	activation_floor = U64FromFullTransactionId(stub_next_full) + UINT64_C(0x80000000);
	UT_ASSERT(
		!cluster_heap_freeze_cutoff_v1((uint32)U64FromFullTransactionId(stub_next_full), &cutoff));
	UT_ASSERT_EQ(cutoff, 1234);
}

UT_TEST(wrap_crossing_uses_full_identity_and_skips_special_floor)
{
	TransactionId cutoff;
	setup_horizon();
	activation_floor = UINT64_C(0x100000000);
	stub_next_full = FullTransactionIdFromU64(activation_floor + 100);
	UT_ASSERT(cluster_heap_freeze_cutoff_v1(80, &cutoff));
	UT_ASSERT_EQ(cutoff, FirstNormalTransactionId);
	UT_ASSERT(cluster_heap_freeze_cutoff_v1(UINT32_C(0xFFFFFFF0), &cutoff));
	UT_ASSERT_EQ(cutoff, UINT32_C(0xFFFFFFF0));
}

UT_TEST(nonshared_mode_preserves_native_without_activation_read)
{
	TransactionId cutoff;
	setup_horizon();
	cluster_shared_config = false;
	activation_available = false;
	UT_ASSERT(cluster_heap_freeze_cutoff_v1(4195120, &cutoff));
	UT_ASSERT_EQ(cutoff, 4195120);
	UT_ASSERT_EQ(activation_reads, 0);
}

UT_TEST(new_permanent_relation_is_not_exempt_as_transaction_local)
{
	RelationData rel = { 0 };
	FormData_pg_class form = { 0 };
	TransactionId frozen;
	MultiXactId multi;
	setup_horizon();
	RecentXmin = 4195120;
	rel.rd_rel = &form;
	form.relpersistence = RELPERSISTENCE_PERMANENT;
	rel.rd_createSubid = 1;
	UT_ASSERT(RELATION_IS_LOCAL(&rel));
	heapam_relation_set_new_filelocator(&rel, NULL, RELPERSISTENCE_PERMANENT, &frozen, &multi);
	UT_ASSERT_EQ(frozen, 4195104);
	UT_ASSERT_EQ(multi, 42);
	activation_reads = 0;
	heapam_relation_set_new_filelocator(&rel, NULL, RELPERSISTENCE_TEMP, &frozen, &multi);
	UT_ASSERT_EQ(frozen, RecentXmin);
	UT_ASSERT_EQ(activation_reads, 0);
}

static ClusterHeapFreezeReportV2
make_freeze_report_v2(void)
{
	ClusterHeapFreezeReportV2 report = {0};

	report.version = CLUSTER_HEAP_FREEZE_REPORT_V2_VERSION;
	report.length = CLUSTER_HEAP_FREEZE_REPORT_V2_BYTES;
	report.sysid = UINT64CONST(0x0102030405060708);
	report.record_generation = UINT64CONST(0x1112131415161718);
	report.formation_epoch = UINT64CONST(0x2122232425262728);
	report.incarnation = UINT64CONST(0x3132333435363738);
	report.report_sequence = UINT64CONST(0x4142434445464748);
	report.future_full_xid = UINT64CONST(0x6162636465666768);
	report.active_full_xid_min = UINT64CONST(0x5152535455565758);
	report.horizon_scn = UINT64CONST(0x7172737475767778);
	report.requested_full_xid = UINT64CONST(0x8182838485868788);
	report.origin = 0xA1B2C3D4;
	report.sender_interval_ms = 1234;
	report.f_sample_id = UINT64CONST(0x9192939495969798);
	report.h_sample_clock_scn = UINT64CONST(0xA1A2A3A4A5A6A7A8);
	report.h_contributor_kind = CLUSTER_HEAP_FREEZE_REPORT_V2_CONTRIBUTOR_PREPARED;
	report.h_contributor_token = UINT64CONST(0xB1B2B3B4B5B6B7B8);
	return report;
}

static void
assert_freeze_report_v2_equal(const ClusterHeapFreezeReportV2 *left,
							  const ClusterHeapFreezeReportV2 *right)
{
	UT_ASSERT_EQ(left->version, right->version);
	UT_ASSERT_EQ(left->length, right->length);
	UT_ASSERT_EQ(left->flags, right->flags);
	UT_ASSERT_EQ(left->sysid, right->sysid);
	UT_ASSERT_EQ(left->record_generation, right->record_generation);
	UT_ASSERT_EQ(left->formation_epoch, right->formation_epoch);
	UT_ASSERT_EQ(left->incarnation, right->incarnation);
	UT_ASSERT_EQ(left->report_sequence, right->report_sequence);
	UT_ASSERT_EQ(left->future_full_xid, right->future_full_xid);
	UT_ASSERT_EQ(left->active_full_xid_min, right->active_full_xid_min);
	UT_ASSERT_EQ(left->horizon_scn, right->horizon_scn);
	UT_ASSERT_EQ(left->requested_full_xid, right->requested_full_xid);
	UT_ASSERT_EQ(left->origin, right->origin);
	UT_ASSERT_EQ(left->sender_interval_ms, right->sender_interval_ms);
	UT_ASSERT_EQ(left->f_sample_id, right->f_sample_id);
	UT_ASSERT_EQ(left->h_sample_clock_scn, right->h_sample_clock_scn);
	UT_ASSERT_EQ(left->h_contributor_kind, right->h_contributor_kind);
	UT_ASSERT_EQ(left->reserved0, right->reserved0);
	UT_ASSERT_EQ(left->h_contributor_token, right->h_contributor_token);
}

UT_TEST(freeze_report_v2_roundtrips_exact_little_endian_layout)
{
	ClusterHeapFreezeReportV2 input = make_freeze_report_v2();
	ClusterHeapFreezeReportV2 output = {0};
	uint8 bytes[CLUSTER_HEAP_FREEZE_REPORT_V2_BYTES];

	memset(bytes, 0xA5, sizeof(bytes));
	UT_ASSERT(cluster_heap_freeze_report_v2_encode(&input, bytes));
	UT_ASSERT_EQ(bytes[CLUSTER_HEAP_FREEZE_REPORT_V2_OFF_VERSION], 2);
	UT_ASSERT_EQ(bytes[CLUSTER_HEAP_FREEZE_REPORT_V2_OFF_VERSION + 1], 0);
	UT_ASSERT_EQ(bytes[CLUSTER_HEAP_FREEZE_REPORT_V2_OFF_LENGTH], 120);
	UT_ASSERT_EQ(bytes[CLUSTER_HEAP_FREEZE_REPORT_V2_OFF_LENGTH + 1], 0);
	UT_ASSERT_EQ(bytes[CLUSTER_HEAP_FREEZE_REPORT_V2_OFF_SYSID], 0x08);
	UT_ASSERT_EQ(bytes[CLUSTER_HEAP_FREEZE_REPORT_V2_OFF_SYSID + 7], 0x01);
	UT_ASSERT_EQ(bytes[CLUSTER_HEAP_FREEZE_REPORT_V2_OFF_ORIGIN], 0xD4);
	UT_ASSERT_EQ(bytes[CLUSTER_HEAP_FREEZE_REPORT_V2_OFF_ORIGIN + 3], 0xA1);
	UT_ASSERT_EQ(bytes[CLUSTER_HEAP_FREEZE_REPORT_V2_OFF_H_CONTRIBUTOR_KIND],
					CLUSTER_HEAP_FREEZE_REPORT_V2_CONTRIBUTOR_PREPARED);
	UT_ASSERT_EQ(bytes[CLUSTER_HEAP_FREEZE_REPORT_V2_OFF_H_CONTRIBUTOR_TOKEN], 0xB8);
	UT_ASSERT_EQ(bytes[CLUSTER_HEAP_FREEZE_REPORT_V2_OFF_H_CONTRIBUTOR_TOKEN + 7], 0xB1);
	UT_ASSERT(cluster_heap_freeze_report_v2_decode(bytes, sizeof(bytes), &output));
	assert_freeze_report_v2_equal(&input, &output);
}

UT_TEST(freeze_report_v2_rejects_bad_header_reserved_and_interval)
{
	ClusterHeapFreezeReportV2 input = make_freeze_report_v2();
	ClusterHeapFreezeReportV2 output;
	uint8 bytes[CLUSTER_HEAP_FREEZE_REPORT_V2_BYTES];

	UT_ASSERT(cluster_heap_freeze_report_v2_encode(&input, bytes));
	output = input;
	bytes[CLUSTER_HEAP_FREEZE_REPORT_V2_OFF_VERSION]++;
	UT_ASSERT(!cluster_heap_freeze_report_v2_decode(bytes, sizeof(bytes), &output));
	assert_freeze_report_v2_equal(&input, &output);

	UT_ASSERT(cluster_heap_freeze_report_v2_encode(&input, bytes));
	bytes[CLUSTER_HEAP_FREEZE_REPORT_V2_OFF_LENGTH]--;
	UT_ASSERT(!cluster_heap_freeze_report_v2_decode(bytes, sizeof(bytes), &output));

	UT_ASSERT(cluster_heap_freeze_report_v2_encode(&input, bytes));
	bytes[CLUSTER_HEAP_FREEZE_REPORT_V2_OFF_FLAGS] = 1;
	UT_ASSERT(!cluster_heap_freeze_report_v2_decode(bytes, sizeof(bytes), &output));

	UT_ASSERT(cluster_heap_freeze_report_v2_encode(&input, bytes));
	bytes[CLUSTER_HEAP_FREEZE_REPORT_V2_OFF_RESERVED0] = 1;
	UT_ASSERT(!cluster_heap_freeze_report_v2_decode(bytes, sizeof(bytes), &output));

	UT_ASSERT(cluster_heap_freeze_report_v2_encode(&input, bytes));
	bytes[CLUSTER_HEAP_FREEZE_REPORT_V2_OFF_SENDER_INTERVAL_MS] = 0;
	bytes[CLUSTER_HEAP_FREEZE_REPORT_V2_OFF_SENDER_INTERVAL_MS + 1] = 0;
	UT_ASSERT(!cluster_heap_freeze_report_v2_decode(bytes, sizeof(bytes), &output));
	UT_ASSERT(!cluster_heap_freeze_report_v2_decode(bytes, sizeof(bytes) - 1, &output));
}

UT_TEST(freeze_report_v2_rejects_unknown_contributor_kind)
{
	ClusterHeapFreezeReportV2 input = make_freeze_report_v2();
	ClusterHeapFreezeReportV2 output;
	uint8 bytes[CLUSTER_HEAP_FREEZE_REPORT_V2_BYTES];

	input.h_contributor_kind = 5;
	UT_ASSERT(!cluster_heap_freeze_report_v2_encode(&input, bytes));
	input = make_freeze_report_v2();
	UT_ASSERT(cluster_heap_freeze_report_v2_encode(&input, bytes));
	bytes[CLUSTER_HEAP_FREEZE_REPORT_V2_OFF_H_CONTRIBUTOR_KIND] = 5;
	UT_ASSERT(!cluster_heap_freeze_report_v2_decode(bytes, sizeof(bytes), &output));
}

UT_TEST(freeze_report_v2_rejects_missing_identity_and_sample)
{
	ClusterHeapFreezeReportV2 input = make_freeze_report_v2();
	uint8 bytes[CLUSTER_HEAP_FREEZE_REPORT_V2_BYTES];

	input.sysid = 0;
	UT_ASSERT(!cluster_heap_freeze_report_v2_encode(&input, bytes));
	input = make_freeze_report_v2();
	input.record_generation = 0;
	UT_ASSERT(!cluster_heap_freeze_report_v2_encode(&input, bytes));
	input = make_freeze_report_v2();
	input.formation_epoch = 0;
	UT_ASSERT(!cluster_heap_freeze_report_v2_encode(&input, bytes));
	input = make_freeze_report_v2();
	input.incarnation = 0;
	UT_ASSERT(!cluster_heap_freeze_report_v2_encode(&input, bytes));
	input = make_freeze_report_v2();
	input.report_sequence = 0;
	UT_ASSERT(!cluster_heap_freeze_report_v2_encode(&input, bytes));
	input = make_freeze_report_v2();
	input.f_sample_id = 0;
	UT_ASSERT(!cluster_heap_freeze_report_v2_encode(&input, bytes));
	input = make_freeze_report_v2();
	input.horizon_scn = 0;
	UT_ASSERT(!cluster_heap_freeze_report_v2_encode(&input, bytes));
	input = make_freeze_report_v2();
	input.h_sample_clock_scn = 0;
	UT_ASSERT(!cluster_heap_freeze_report_v2_encode(&input, bytes));
	input = make_freeze_report_v2();
	input.horizon_scn = CLUSTER_UNDO_HORIZON_REPORT_UNCONSTRAINED;
	UT_ASSERT(!cluster_heap_freeze_report_v2_encode(&input, bytes));
}

UT_TEST(freeze_report_v2_rejects_invalid_full_xid_window)
{
	ClusterHeapFreezeReportV2 input = make_freeze_report_v2();
	uint8 bytes[CLUSTER_HEAP_FREEZE_REPORT_V2_BYTES];

	input.future_full_xid = UINT64CONST(0x6162636400000000);
	UT_ASSERT(!cluster_heap_freeze_report_v2_encode(&input, bytes));
	input = make_freeze_report_v2();
	input.active_full_xid_min = UINT64CONST(0x5152535400000000);
	UT_ASSERT(!cluster_heap_freeze_report_v2_encode(&input, bytes));
	input = make_freeze_report_v2();
	input.active_full_xid_min = UINT64CONST(0x6162636465666769);
	UT_ASSERT(!cluster_heap_freeze_report_v2_encode(&input, bytes));
	input = make_freeze_report_v2();
	input.requested_full_xid = UINT64CONST(0x8182838400000000);
	UT_ASSERT(!cluster_heap_freeze_report_v2_encode(&input, bytes));
	input = make_freeze_report_v2();
	input.requested_full_xid = 0;
	UT_ASSERT(cluster_heap_freeze_report_v2_encode(&input, bytes));
}

/* Exercise both codec boundaries without relying on a malformed header. */
static void
assert_freeze_report_v2_rejects_full_xid(unsigned field, uint64 value)
{
	static const unsigned offsets[] = {
		CLUSTER_HEAP_FREEZE_REPORT_V2_OFF_FUTURE_FULL_XID,
		CLUSTER_HEAP_FREEZE_REPORT_V2_OFF_ACTIVE_FULL_XID_MIN,
		CLUSTER_HEAP_FREEZE_REPORT_V2_OFF_REQUESTED_FULL_XID
	};
	ClusterHeapFreezeReportV2 input = make_freeze_report_v2();
	ClusterHeapFreezeReportV2 invalid;
	ClusterHeapFreezeReportV2 output;
	uint8 bytes[CLUSTER_HEAP_FREEZE_REPORT_V2_BYTES];
	uint8 before[CLUSTER_HEAP_FREEZE_REPORT_V2_BYTES];
	unsigned i;

	input.future_full_xid = UINT64CONST(0x00000003000003e8);
	input.active_full_xid_min = UINT64CONST(0x0000000100000003);
	input.requested_full_xid = 0;
	UT_ASSERT(field < lengthof(offsets));
	UT_ASSERT(cluster_heap_freeze_report_v2_encode(&input, bytes));
	memcpy(before, bytes, sizeof(before));
	invalid = input;
	if (field == 0)
		invalid.future_full_xid = value;
	else if (field == 1)
		invalid.active_full_xid_min = value;
	else
		invalid.requested_full_xid = value;
	UT_ASSERT(!cluster_heap_freeze_report_v2_encode(&invalid, bytes));
	UT_ASSERT(memcmp(before, bytes, sizeof(bytes)) == 0);
	for (i = 0; i < 8; i++)
		bytes[offsets[field] + i] = (uint8)(value >> (8 * i));
	output = input;
	UT_ASSERT(!cluster_heap_freeze_report_v2_decode(bytes, sizeof(bytes), &output));
	assert_freeze_report_v2_equal(&input, &output);
}

UT_TEST(freeze_report_v2_rejects_special_and_exhausted_required_full_xids)
{
	static const uint64 invalid[] = {
		UINT64CONST(0x0000000200000001),
		UINT64CONST(0x0000000200000002),
		UINT64_MAX
	};
	unsigned field;
	unsigned i;

	for (field = 0; field < 2; field++)
		for (i = 0; i < lengthof(invalid); i++)
			assert_freeze_report_v2_rejects_full_xid(field, invalid[i]);
}

UT_TEST(freeze_report_v2_rejects_special_and_exhausted_requested_full_xid)
{
	assert_freeze_report_v2_rejects_full_xid(2, UINT64CONST(0x0000000200000001));
	assert_freeze_report_v2_rejects_full_xid(2, UINT64CONST(0x0000000200000002));
	assert_freeze_report_v2_rejects_full_xid(2, UINT64_MAX);
}

UT_TEST(freeze_report_v2_preserves_finite_normal_full_xid_boundaries)
{
	ClusterHeapFreezeReportV2 input = make_freeze_report_v2();
	ClusterHeapFreezeReportV2 output;
	uint8 bytes[CLUSTER_HEAP_FREEZE_REPORT_V2_BYTES];

	input.future_full_xid = UINT64_MAX - 1;
	input.active_full_xid_min = FirstNormalTransactionId;
	input.requested_full_xid = UINT64_MAX - 1;
	UT_ASSERT(cluster_heap_freeze_report_v2_encode(&input, bytes));
	UT_ASSERT(cluster_heap_freeze_report_v2_decode(bytes, sizeof(bytes), &output));
	assert_freeze_report_v2_equal(&input, &output);
	input.active_full_xid_min = UINT64CONST(0x0000000200000003);
	input.requested_full_xid = 0;
	UT_ASSERT(cluster_heap_freeze_report_v2_encode(&input, bytes));
	UT_ASSERT(cluster_heap_freeze_report_v2_decode(bytes, sizeof(bytes), &output));
	assert_freeze_report_v2_equal(&input, &output);
}

UT_TEST(freeze_report_v2_decode_rejects_invalid_payload)
{
	ClusterHeapFreezeReportV2 input = make_freeze_report_v2();
	ClusterHeapFreezeReportV2 output = input;
	uint8 bytes[CLUSTER_HEAP_FREEZE_REPORT_V2_BYTES];

	UT_ASSERT(cluster_heap_freeze_report_v2_encode(&input, bytes));
	bytes[CLUSTER_HEAP_FREEZE_REPORT_V2_OFF_SYSID] = 0;
	bytes[CLUSTER_HEAP_FREEZE_REPORT_V2_OFF_SYSID + 1] = 0;
	bytes[CLUSTER_HEAP_FREEZE_REPORT_V2_OFF_SYSID + 2] = 0;
	bytes[CLUSTER_HEAP_FREEZE_REPORT_V2_OFF_SYSID + 3] = 0;
	bytes[CLUSTER_HEAP_FREEZE_REPORT_V2_OFF_SYSID + 4] = 0;
	bytes[CLUSTER_HEAP_FREEZE_REPORT_V2_OFF_SYSID + 5] = 0;
	bytes[CLUSTER_HEAP_FREEZE_REPORT_V2_OFF_SYSID + 6] = 0;
	bytes[CLUSTER_HEAP_FREEZE_REPORT_V2_OFF_SYSID + 7] = 0;
	UT_ASSERT(!cluster_heap_freeze_report_v2_decode(bytes, sizeof(bytes), &output));
	assert_freeze_report_v2_equal(&input, &output);
}

/* Controlled original source images for otherwise unchanged policy tests. */
static void
observed_bounds_set_full_xid_sources(ClusterHeapFreezeCohortObservation *cohort)
{
	cohort->local_registration_cut.status = CTRC_ACTIVE_SCAN_COMPLETE;
	cohort->local_registration_cut.min_full = FullTransactionIdFromU64(cohort->active_full_xid_min);
	cohort->local_registration_cut.future_full = FullTransactionIdFromU64(cohort->future_full_xid_max);
	for (int peer = 0; peer < CLUSTER_MAX_NODES; peer++) {
		cohort->source_peer_active_full_xid_min[peer] = cohort->active_full_xid_min;
		cohort->source_peer_future_full_xid[peer] = cohort->future_full_xid_max;
	}
}

static void
observed_bounds_fixture(ClusterHeapFreezePolicySampleV1 *policy,
	ClusterHeapFreezeCohortObservation *cohort, ClusterSemanticAdmissionToken *admission)
{
	memset(policy, 0, sizeof(*policy));
	memset(cohort, 0, sizeof(*cohort));
	memset(admission, 0, sizeof(*admission));
	policy->legal_next = FullTransactionIdFromEpochAndXid(2, 1000);
	policy->freeze_limit = FullTransactionIdFromEpochAndXid(2, 80);
	cohort->active_full_xid_min = U64FromFullTransactionId(FullTransactionIdFromEpochAndXid(2, 60));
	cohort->future_full_xid_max = U64FromFullTransactionId(FullTransactionIdFromEpochAndXid(2, 2000));
	observed_bounds_set_full_xid_sources(cohort);
	cohort->horizon_scn = 500;
	cohort->source_local_horizon_scn = 700;
	cohort->source_peer_horizon_scn[0] = 700;
	cohort->source_peer_horizon_scn[1] = 500;
	cohort->source_local_history.sample_clock_scn = 1000;
	cohort->source_local_history.contributor_kind = CLUSTER_HEAP_FREEZE_REPORT_V2_CONTRIBUTOR_CLOCK;
	cohort->source_peer_history[0] = cohort->source_local_history;
	cohort->source_peer_history[1] = cohort->source_local_history;
	cohort->record_generation = admission->record_generation = 17;
	cohort->formation_epoch = admission->formation_epoch = 9;
	cohort->admitted_members_lo = 3;
	cohort->admission_fence.record_generation = 17;
	cohort->admission_fence.formation_epoch = 9;
	cohort->admission_fence.admitted_members_lo = 3;
	cohort->admission_fence.local_node_id = 0;
	admission->entered = true;
}

UT_TEST(observed_bounds_cap_policy_at_same_cohort_and_preserve_h_domain)
{
	ClusterHeapFreezePolicySampleV1 policy;
	ClusterHeapFreezeCohortObservation cohort;
	ClusterSemanticAdmissionToken admission;
	ClusterHeapFreezeObservedBoundsV1 out;
	unsigned before;

	observed_bounds_fixture(&policy, &cohort, &admission);
	before = activation_reads;
	UT_ASSERT(cluster_heap_freeze_observed_bounds_v1(&policy, &cohort, &admission, 0, &out));
	UT_ASSERT_EQ(U64FromFullTransactionId(out.freeze_limit), cohort.active_full_xid_min);
	UT_ASSERT_EQ(out.horizon_scn, cohort.horizon_scn);
	cohort.active_full_xid_min = U64FromFullTransactionId(FullTransactionIdFromEpochAndXid(2, 90));
	observed_bounds_set_full_xid_sources(&cohort);
	UT_ASSERT(cluster_heap_freeze_observed_bounds_v1(&policy, &cohort, &admission, 0, &out));
	UT_ASSERT(FullTransactionIdEquals(out.freeze_limit, policy.freeze_limit));
	UT_ASSERT_EQ(out.horizon_scn, cohort.horizon_scn);
	UT_ASSERT_EQ(activation_reads, before);
}

UT_TEST(observed_bounds_reproduces_original_fixed_cohort_h_minimum)
{
	ClusterHeapFreezePolicySampleV1 policy;
	ClusterHeapFreezeCohortObservation cohort;
	ClusterSemanticAdmissionToken admission;
	ClusterHeapFreezeObservedBoundsV1 out;
	unsigned before = activation_reads;

	observed_bounds_fixture(&policy, &cohort, &admission);
	cohort.source_local_horizon_scn = scn_encode(7, 90);
	cohort.source_peer_horizon_scn[1] = scn_encode(1, 100);
	cohort.source_peer_horizon_scn[2] = 0; /* not required */
	cohort.horizon_scn = cohort.source_local_horizon_scn;
	UT_ASSERT(cluster_heap_freeze_observed_bounds_v1(&policy, &cohort, &admission, 0, &out));
	UT_ASSERT_EQ(out.horizon_scn, cohort.source_local_horizon_scn);
	cohort.source_peer_horizon_scn[1] = scn_encode(1, 90);
	UT_ASSERT(cluster_heap_freeze_observed_bounds_v1(&policy, &cohort, &admission, 0, &out));
	UT_ASSERT_EQ(out.horizon_scn, cohort.source_local_horizon_scn); /* same-time tie */
	cohort.admitted_members_hi = cohort.admission_fence.admitted_members_hi = UINT64CONST(1) << 63;
	cohort.source_peer_horizon_scn[127] = scn_encode(9, 80);
	cohort.source_peer_history[127] = cohort.source_local_history;
	cohort.horizon_scn = cohort.source_peer_horizon_scn[127];
	UT_ASSERT(cluster_heap_freeze_observed_bounds_v1(&policy, &cohort, &admission, 0, &out));
	UT_ASSERT_EQ(out.horizon_scn, cohort.source_peer_horizon_scn[127]);
	UT_ASSERT_EQ(activation_reads, before);
}

UT_TEST(observed_bounds_rejects_changed_or_missing_original_h_atomically)
{
	for (int variant = 0; variant < 7; variant++) {
		ClusterHeapFreezePolicySampleV1 policy;
		ClusterHeapFreezeCohortObservation cohort;
		ClusterSemanticAdmissionToken admission;
		ClusterHeapFreezeObservedBoundsV1 out, saved;
		observed_bounds_fixture(&policy, &cohort, &admission);
		memset(&out, 0xa5, sizeof(out)); saved = out;
		switch (variant) {
		case 0: cohort.source_local_horizon_scn = 0; break;
		case 1: cohort.source_local_horizon_scn = UINT64_MAX; break;
		case 2: cohort.source_local_horizon_scn = 400; break;
		case 3: cohort.source_peer_horizon_scn[1] = 0; break;
		case 4: cohort.source_peer_horizon_scn[1] = UINT64_MAX; break;
		case 5: cohort.source_peer_horizon_scn[1] = 400; break;
		case 6: cohort.horizon_scn++; break;
		}
		UT_ASSERT(!cluster_heap_freeze_observed_bounds_v1(&policy, &cohort, &admission, 0, &out));
		UT_ASSERT(memcmp(&out, &saved, sizeof(out)) == 0);
	}
}

UT_TEST(observed_bounds_requires_original_history_metadata_atomically)
{
	for (int peer = 0; peer < 2; peer++) {
		for (int field = 0; field < 3; field++) {
			ClusterHeapFreezePolicySampleV1 policy;
			ClusterHeapFreezeCohortObservation cohort;
			ClusterSemanticAdmissionToken admission;
			ClusterHeapFreezeObservedBoundsV1 out, before;
			ClusterHeapFreezeHistorySourceObservationV1 *history;
			observed_bounds_fixture(&policy, &cohort, &admission);
			history = peer == 0 ? &cohort.source_local_history : &cohort.source_peer_history[1];
			if (field == 0) history->sample_clock_scn = 0;
			if (field == 1) history->sample_clock_scn = peer == 0 ? 699 : 499;
			if (field == 2) history->contributor_kind =
				(ClusterHeapFreezeReportV2ContributorKind)5;
			memset(&out, 0xa5, sizeof(out)); before = out;
			UT_ASSERT(!cluster_heap_freeze_observed_bounds_v1(&policy, &cohort,
				&admission, 0, &out));
			UT_ASSERT(memcmp(&out, &before, sizeof(out)) == 0);
		}
	}
}

UT_TEST(observed_bounds_identity_refusal_preserves_complete_output)
{
	for (int variant = 0; variant < 15; variant++) {
		ClusterHeapFreezePolicySampleV1 policy;
		ClusterHeapFreezeCohortObservation cohort;
		ClusterSemanticAdmissionToken admission;
		ClusterHeapFreezeObservedBoundsV1 out, saved;
		int node = 0;
		observed_bounds_fixture(&policy, &cohort, &admission);
		memset(&out, 0x5a, sizeof(out)); saved = out;
		switch (variant) {
		case 0: admission.entered = false; break;
		case 1: admission.record_generation++; break;
		case 2: admission.formation_epoch++; break;
		case 3: cohort.admission_fence.record_generation++; break;
		case 4: cohort.admission_fence.formation_epoch++; break;
		case 5: cohort.admission_fence.admitted_members_lo ^= 4; break;
		case 6: cohort.admission_fence.admitted_members_hi = 1; break;
		case 7: cohort.admission_fence.local_node_id = 1; break;
		case 8: cohort.admitted_members_lo = cohort.admission_fence.admitted_members_lo = 2; break;
		case 9: node = -1; break;
		case 10: node = 128; break;
		case 11: cohort.local_unqualified_entries = 1; break;
		case 12: cohort.local_unqualified_entries = UINT64_MAX; break;
		case 13: cohort.record_generation = admission.record_generation = cohort.admission_fence.record_generation = 0; break;
		case 14: cohort.formation_epoch = admission.formation_epoch = cohort.admission_fence.formation_epoch = 0; break;
		}
		UT_ASSERT(!cluster_heap_freeze_observed_bounds_v1(&policy, &cohort, &admission, node, &out));
		UT_ASSERT_EQ(memcmp(&out, &saved, sizeof(out)), 0);
	}
}

UT_TEST(observed_bounds_policy_window_refusal_is_atomic)
{
	for (int variant = 0; variant < 8; variant++) {
		ClusterHeapFreezePolicySampleV1 policy;
		ClusterHeapFreezeCohortObservation cohort;
		ClusterSemanticAdmissionToken admission;
		ClusterHeapFreezeObservedBoundsV1 out, saved;
		observed_bounds_fixture(&policy, &cohort, &admission);
		memset(&out, 0x5a, sizeof(out)); saved = out;
		switch (variant) {
		case 0: policy.legal_next = InvalidFullTransactionId; break;
		case 1: policy.freeze_limit = InvalidFullTransactionId; break;
		case 2: policy.legal_next = FullTransactionIdFromEpochAndXid(2, FrozenTransactionId); break;
		case 3: policy.freeze_limit = FullTransactionIdFromEpochAndXid(2, BootstrapTransactionId); break;
		case 4: policy.legal_next = FullTransactionIdFromU64(UINT64_MAX); break;
		case 5: policy.freeze_limit = FullTransactionIdFromU64(UINT64_MAX); break;
		case 6: policy.freeze_limit = FullTransactionIdFromEpochAndXid(2, 1001); break;
		case 7: policy.legal_next = FullTransactionIdFromU64(U64FromFullTransactionId(policy.freeze_limit) + UINT64CONST(0x80000000)); break;
		}
		UT_ASSERT(!cluster_heap_freeze_observed_bounds_v1(&policy, &cohort, &admission, 0, &out));
		UT_ASSERT_EQ(memcmp(&out, &saved, sizeof(out)), 0);
	}
}

UT_TEST(observed_bounds_cohort_refusal_and_nulls_preserve_output)
{
	for (int variant = 0; variant < 11; variant++) {
		ClusterHeapFreezePolicySampleV1 policy;
		ClusterHeapFreezeCohortObservation cohort;
		ClusterSemanticAdmissionToken admission;
		ClusterHeapFreezeObservedBoundsV1 out, saved;
		observed_bounds_fixture(&policy, &cohort, &admission);
		memset(&out, 0x5a, sizeof(out)); saved = out;
		switch (variant) {
		case 0: cohort.active_full_xid_min = (UINT64CONST(2) << 32) | FrozenTransactionId; break;
		case 1: cohort.future_full_xid_max = (UINT64CONST(2) << 32) | BootstrapTransactionId; break;
		case 2: cohort.future_full_xid_max = UINT64_MAX; break;
		case 3: cohort.active_full_xid_min = cohort.future_full_xid_max + 1; break;
		case 4: cohort.horizon_scn = 0; break;
		case 5: cohort.horizon_scn = UINT64_MAX; break;
		case 6: cohort.active_full_xid_min = UINT64_MAX; break;
		default: break;
		}
		UT_ASSERT(!cluster_heap_freeze_observed_bounds_v1(variant == 7 ? NULL : &policy,
			variant == 8 ? NULL : &cohort, variant == 9 ? NULL : &admission,
			0, variant == 10 ? NULL : &out));
		UT_ASSERT_EQ(memcmp(&out, &saved, sizeof(out)), 0);
	}
}

UT_TEST(observed_bounds_selected_cutoff_refuses_native_halfspace_alias)
{
	for (int variant = 0; variant < 4; variant++) {
		ClusterHeapFreezePolicySampleV1 policy;
		ClusterHeapFreezeCohortObservation cohort;
		ClusterSemanticAdmissionToken admission;
		ClusterHeapFreezeObservedBoundsV1 out, saved;
		uint64 legal;
		observed_bounds_fixture(&policy, &cohort, &admission);
		legal = U64FromFullTransactionId(policy.legal_next);
		switch (variant) {
		case 0: cohort.active_full_xid_min = legal - UINT64CONST(0x80000000); break;
		case 1: cohort.active_full_xid_min = legal - UINT64CONST(0x80000001); break;
		case 2: cohort.active_full_xid_min = legal - UINT64CONST(0x100000000); break;
		case 3: cohort.active_full_xid_min = FirstNormalTransactionId; break;
		}
		observed_bounds_set_full_xid_sources(&cohort);
		memset(&out, 0x5a, sizeof(out)); saved = out;
		UT_ASSERT(!cluster_heap_freeze_observed_bounds_v1(&policy, &cohort,
			&admission, 0, &out));
		UT_ASSERT_EQ(memcmp(&out, &saved, sizeof(out)), 0);
		UT_ASSERT_EQ(activation_reads, 0);
	}
}

UT_TEST(observed_bounds_selected_cutoff_keeps_exact_native_window_edge)
{
	for (int variant = 0; variant < 2; variant++) {
		ClusterHeapFreezePolicySampleV1 policy;
		ClusterHeapFreezeCohortObservation cohort;
		ClusterSemanticAdmissionToken admission;
		ClusterHeapFreezeObservedBoundsV1 out;
		uint64 legal;
		observed_bounds_fixture(&policy, &cohort, &admission);
		legal = U64FromFullTransactionId(policy.legal_next);
		cohort.active_full_xid_min = variant == 0
			? legal - UINT64CONST(0x7fffffff)
			: U64FromFullTransactionId(FullTransactionIdFromEpochAndXid(1, UINT32_MAX));
		observed_bounds_set_full_xid_sources(&cohort);
		UT_ASSERT(cluster_heap_freeze_observed_bounds_v1(&policy, &cohort,
			&admission, 0, &out));
		UT_ASSERT_EQ(U64FromFullTransactionId(out.freeze_limit), cohort.active_full_xid_min);
		UT_ASSERT_EQ(out.horizon_scn, cohort.horizon_scn);
		UT_ASSERT_EQ(activation_reads, 0);
	}
}

UT_TEST(observed_bounds_checks_selected_minimum_not_ignored_future_bound)
{
	ClusterHeapFreezePolicySampleV1 policy;
	ClusterHeapFreezeCohortObservation cohort;
	ClusterSemanticAdmissionToken admission;
	ClusterHeapFreezeObservedBoundsV1 out;
	observed_bounds_fixture(&policy, &cohort, &admission);
	cohort.active_full_xid_min = U64FromFullTransactionId(FullTransactionIdFromEpochAndXid(3, 80));
	cohort.future_full_xid_max = cohort.active_full_xid_min + 1000;
	observed_bounds_set_full_xid_sources(&cohort);
	UT_ASSERT(cluster_heap_freeze_observed_bounds_v1(&policy, &cohort,
		&admission, 0, &out));
	UT_ASSERT(FullTransactionIdEquals(out.freeze_limit, policy.freeze_limit));
	UT_ASSERT_EQ(out.horizon_scn, cohort.horizon_scn);
	UT_ASSERT_EQ(activation_reads, 0);
}

UT_TEST(observed_bounds_keep_fullxid_epoch_and_high_member_bit)
{
	ClusterHeapFreezePolicySampleV1 policy;
	ClusterHeapFreezeCohortObservation cohort;
	ClusterSemanticAdmissionToken admission;
	ClusterHeapFreezeObservedBoundsV1 out;

	observed_bounds_fixture(&policy, &cohort, &admission);
	cohort.active_full_xid_min = U64FromFullTransactionId(FullTransactionIdFromEpochAndXid(1, UINT32_MAX - 100));
	observed_bounds_set_full_xid_sources(&cohort);
	cohort.admitted_members_hi = cohort.admission_fence.admitted_members_hi = UINT64CONST(1) << 63;
	cohort.admission_fence.local_node_id = 127;
	UT_ASSERT(cluster_heap_freeze_observed_bounds_v1(&policy, &cohort, &admission, 127, &out));
	UT_ASSERT_EQ(U64FromFullTransactionId(out.freeze_limit), cohort.active_full_xid_min);
	UT_ASSERT(!FullTransactionIdEquals(out.freeze_limit, policy.freeze_limit));
}

UT_TEST(observed_bounds_reproduces_exact_fullxid_min_and_future_max_from_fixed_sources)
{
	ClusterHeapFreezePolicySampleV1 policy;
	ClusterHeapFreezeCohortObservation cohort;
	ClusterSemanticAdmissionToken admission;
	ClusterHeapFreezeObservedBoundsV1 out;
	observed_bounds_fixture(&policy, &cohort, &admission);
	cohort.local_registration_cut.min_full = FullTransactionIdFromEpochAndXid(2, 90);
	cohort.local_registration_cut.future_full = FullTransactionIdFromEpochAndXid(2, 1500);
	UT_ASSERT(cluster_heap_freeze_observed_bounds_v1(&policy, &cohort, &admission, 0, &out));
	UT_ASSERT_EQ(U64FromFullTransactionId(out.freeze_limit), cohort.source_peer_active_full_xid_min[1]);
	/* A required high member keeps its complete older epoch, while its
	 * future contributes only to the max, never to the freeze minimum. */
	cohort.admitted_members_hi = cohort.admission_fence.admitted_members_hi = UINT64CONST(1) << 63;
	cohort.source_peer_active_full_xid_min[127] =
		U64FromFullTransactionId(FullTransactionIdFromEpochAndXid(1, UINT32_MAX - 100));
	cohort.source_peer_future_full_xid[127] =
		U64FromFullTransactionId(FullTransactionIdFromEpochAndXid(3, FirstNormalTransactionId));
	cohort.active_full_xid_min = cohort.source_peer_active_full_xid_min[127];
	cohort.future_full_xid_max = cohort.source_peer_future_full_xid[127];
	cohort.source_peer_horizon_scn[127] = cohort.source_local_horizon_scn;
	cohort.source_peer_history[127] = cohort.source_local_history;
	/* Nonmembers never contribute, even with malformed diagnostic storage. */
	cohort.source_peer_active_full_xid_min[65] = UINT64_MAX;
	cohort.source_peer_future_full_xid[65] = 1;
	UT_ASSERT(cluster_heap_freeze_observed_bounds_v1(&policy, &cohort, &admission, 0, &out));
	UT_ASSERT_EQ(U64FromFullTransactionId(out.freeze_limit), cohort.source_peer_active_full_xid_min[127]);
	UT_ASSERT_EQ(out.horizon_scn, 500);
	UT_ASSERT_EQ(activation_reads, 0);
}

UT_TEST(observed_bounds_missing_or_changed_fullxid_source_is_atomic)
{
	for (int source = 0; source < 2; source++) {
		for (int fault = 0; fault < 8; fault++) {
			ClusterHeapFreezePolicySampleV1 policy;
			ClusterHeapFreezeCohortObservation cohort;
			ClusterSemanticAdmissionToken admission;
			ClusterHeapFreezeObservedBoundsV1 out, before;
			uint64 min, future;
			observed_bounds_fixture(&policy, &cohort, &admission);
			min = cohort.active_full_xid_min; future = cohort.future_full_xid_max;
			switch (fault) {
			case 0: min = 0; break;
			case 1: future = 0; break;
			case 2: min = (UINT64CONST(2) << 32) | FrozenTransactionId; break;
			case 3: future = (UINT64CONST(2) << 32) | BootstrapTransactionId; break;
			case 4: min = UINT64_MAX; break;
			case 5: future = UINT64_MAX; break;
			case 6: min--; break; /* aggregate no longer equals the saved source */
			case 7: future++; break;
			}
			if (source == 0) {
				cohort.local_registration_cut.min_full = FullTransactionIdFromU64(min);
				cohort.local_registration_cut.future_full = FullTransactionIdFromU64(future);
			} else {
				cohort.source_peer_active_full_xid_min[1] = min;
				cohort.source_peer_future_full_xid[1] = future;
			}
			memset(&out, 0xa5, sizeof(out)); before = out;
			UT_ASSERT(!cluster_heap_freeze_observed_bounds_v1(&policy, &cohort, &admission, 0, &out));
			UT_ASSERT(memcmp(&out, &before, sizeof(out)) == 0);
			UT_ASSERT_EQ(activation_reads, 0);
		}
	}
}

UT_TEST(observed_bounds_aggregate_fullxid_cannot_replace_saved_source_even_if_policy_ignores_f)
{
	for (int fault = 0; fault < 3; fault++) {
		ClusterHeapFreezePolicySampleV1 policy;
		ClusterHeapFreezeCohortObservation cohort;
		ClusterSemanticAdmissionToken admission;
		ClusterHeapFreezeObservedBoundsV1 out, before;
		observed_bounds_fixture(&policy, &cohort, &admission);
		if (fault < 2) {
			if (fault == 1) policy.freeze_limit = FullTransactionIdFromEpochAndXid(2, 40);
			cohort.active_full_xid_min++;
		} else cohort.future_full_xid_max++;
		memset(&out, 0xa5, sizeof(out)); before = out;
		UT_ASSERT(!cluster_heap_freeze_observed_bounds_v1(&policy, &cohort, &admission, 0, &out));
		UT_ASSERT(memcmp(&out, &before, sizeof(out)) == 0);
		UT_ASSERT_EQ(activation_reads, 0);
	}
}

int
main(void)
{
	UT_PLAN(66);
	UT_RUN(observed_bounds_cap_policy_at_same_cohort_and_preserve_h_domain);
	UT_RUN(observed_bounds_reproduces_original_fixed_cohort_h_minimum);
	UT_RUN(observed_bounds_rejects_changed_or_missing_original_h_atomically);
	UT_RUN(observed_bounds_requires_original_history_metadata_atomically);
	UT_RUN(observed_bounds_identity_refusal_preserves_complete_output);
	UT_RUN(observed_bounds_policy_window_refusal_is_atomic);
	UT_RUN(observed_bounds_cohort_refusal_and_nulls_preserve_output);
	UT_RUN(observed_bounds_selected_cutoff_refuses_native_halfspace_alias);
	UT_RUN(observed_bounds_selected_cutoff_keeps_exact_native_window_edge);
	UT_RUN(observed_bounds_checks_selected_minimum_not_ignored_future_bound);
	UT_RUN(observed_bounds_keep_fullxid_epoch_and_high_member_bit);
	UT_RUN(observed_bounds_reproduces_exact_fullxid_min_and_future_max_from_fixed_sources);
	UT_RUN(observed_bounds_missing_or_changed_fullxid_source_is_atomic);
	UT_RUN(observed_bounds_aggregate_fullxid_cannot_replace_saved_source_even_if_policy_ignores_f);
	UT_RUN(policy_sample_retains_original_legal_next_and_allocation_fence);
	UT_RUN(policy_sample_refusal_preserves_complete_output);
	UT_RUN(policy_sample_rechecks_original_fence_without_resampling_next);
	UT_RUN(policy_sample_current_refuses_unrepresentable_bounds_before_fence_read);
	UT_RUN(full_policy_retains_the_sampled_legal_epoch);
	UT_RUN(full_policy_keeps_prior_epoch_floor_and_raw_wrapper_compatibility);
	UT_RUN(full_policy_refusal_preserves_both_outputs);
	UT_RUN(real_vacuum_retains_full_policy_before_both_scan_exits);
	UT_RUN(real_vacuum_shared_age_is_independent_of_native_deletion_ceiling);
	UT_RUN(real_vacuum_shared_age_preserves_full_epoch_above_activation_floor);
	UT_RUN(real_vacuum_discards_previous_full_policy_on_refusal_or_nontarget);
	UT_RUN(published_watermark_caps_existing_vacuum_limit);
	UT_RUN(published_watermark_never_raises_age_or_activation_ceiling);
	UT_RUN(published_watermark_minimum_uses_full_xid_epoch);
	UT_RUN(published_watermark_ambiguous_floor_refuses_raw_projection);
	UT_RUN(unavailable_watermark_preserves_original_v1_policy);
	UT_RUN(lease_revoked_during_watermark_read_refuses_policy);
	UT_RUN(nonshared_and_temporary_policy_skip_watermark);
	UT_RUN(legal_min_age_uses_lease_next_but_keeps_original_ceiling);
	UT_RUN(legal_min_age_subtracts_in_full_xid_epoch);
	UT_RUN(legal_min_age_saturates_instead_of_raw_underflow);
	UT_RUN(legal_min_age_respects_default_and_native_safety_clamp);
	UT_RUN(legal_min_age_freeze_zero_still_obeys_activation_ceiling);
	UT_RUN(legal_min_age_refuses_future_catalog_without_new_freeze);
	UT_RUN(nonshared_and_temporary_min_age_stay_native);
	UT_RUN(legal_min_age_skips_special_ids_after_full_subtraction);
	UT_RUN(missing_min_age_evidence_preserves_oldest_xmin_ceiling);
	UT_RUN(ambiguous_min_age_ceiling_requests_scan_without_advancing);
	UT_RUN(legal_age_requests_scan_when_local_next_lags);
	UT_RUN(legal_age_threshold_is_inclusive_without_raw_underflow);
	UT_RUN(legal_age_crosses_epoch_with_full_xid_difference);
	UT_RUN(missing_age_promise_requests_scan_without_freeze_permission);
	UT_RUN(age_promise_revoked_after_unlock_requests_scan);
	UT_RUN(legal_age_respects_native_default_and_safety_clamp);
	UT_RUN(nonshared_and_temporary_age_stay_native);
	UT_RUN(young_legal_xid_keeps_independent_multixact_scan_trigger);
	UT_RUN(slower_writer_can_issue_below_local_freeze_proposal);
	UT_RUN(older_local_cutoff_is_never_advanced);
	UT_RUN(missing_or_invalid_authority_preserves_output);
	UT_RUN(full_xid_window_cannot_alias_old_activation);
	UT_RUN(wrap_crossing_uses_full_identity_and_skips_special_floor);
	UT_RUN(nonshared_mode_preserves_native_without_activation_read);
	UT_RUN(new_permanent_relation_is_not_exempt_as_transaction_local);
	UT_RUN(freeze_report_v2_roundtrips_exact_little_endian_layout);
	UT_RUN(freeze_report_v2_rejects_bad_header_reserved_and_interval);
	UT_RUN(freeze_report_v2_rejects_unknown_contributor_kind);
	UT_RUN(freeze_report_v2_rejects_missing_identity_and_sample);
	UT_RUN(freeze_report_v2_rejects_invalid_full_xid_window);
	UT_RUN(freeze_report_v2_decode_rejects_invalid_payload);
	UT_RUN(freeze_report_v2_rejects_special_and_exhausted_required_full_xids);
	UT_RUN(freeze_report_v2_rejects_special_and_exhausted_requested_full_xid);
	UT_RUN(freeze_report_v2_preserves_finite_normal_full_xid_boundaries);
	UT_DONE();
	return ut_failed_count ? 1 : 0;
}
