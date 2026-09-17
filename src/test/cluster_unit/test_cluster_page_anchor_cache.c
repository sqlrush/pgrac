/*-------------------------------------------------------------------------
 *
 * test_cluster_page_anchor_cache.c
 *    STOP-06 process-local anchor-known cache contract.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"
#include "access/xloginsert.h"
#include "cluster/cluster_control_root.h"
#include "cluster/cluster_external_fence.h"
#include "cluster/cluster_recovery_duty.h"
#include "cluster/cluster_wal_retention.h"

#if defined(__has_include)
#if __has_include("cluster/cluster_page_anchor_cache.h")
#include "cluster/cluster_page_anchor_cache.h"
#define TEST_HAVE_CLUSTER_PAGE_ANCHOR_CACHE 1
#endif
#endif

#include "unit_test.h"

UT_DEFINE_GLOBALS();

void
ExceptionalCondition(const char *condition_name, const char *file_name, int line_number)
{
	printf("# Assert failed: %s at %s:%d\n", condition_name, file_name, line_number);
	abort();
}

/* This binary links stable-base only for shared key helpers.  Keep every
 * external authority seam fail closed if its owner branch becomes reachable. */
ClusterControlRootResult
cluster_control_root_revalidate(const ClusterControlRootReadToken *token pg_attribute_unused(),
								const ClusterControlRootIdentity *expected_identity
									pg_attribute_unused(),
								ClusterControlRootSnapshot *out_snapshot pg_attribute_unused())
{
	return CLUSTER_CONTROL_ROOT_STALE_TOKEN;
}

ClusterFormationWitnessResult
cluster_formation_witness_revalidate_nowait(
	const ClusterFormationWitnessV1 *witness pg_attribute_unused())
{
	return CLUSTER_FORMATION_WITNESS_UNSTABLE;
}

bool
cluster_external_fence_need_set_revalidate_nowait(
	const PgracExternalFenceNeedSetV1 *needs pg_attribute_unused(),
	const ClusterFormationWitnessV1 *formation pg_attribute_unused(),
	PgracExternalFenceDenyReason *reason pg_attribute_unused())
{
	return false;
}

bool
cluster_external_fence_revalidate_set_nowait(
	const PgracExternalFenceAdmissionSetV1 *admissions pg_attribute_unused(),
	const PgracExternalFenceNeedSetV1 *needs pg_attribute_unused(),
	const ClusterFormationWitnessV1 *formation pg_attribute_unused(),
	PgracExternalFenceDenyReason *reason pg_attribute_unused())
{
	return false;
}

ClusterWalPinResult
cluster_wal_retention_pin_preflight_revalidate_wait_v1(
	ClusterWalRetentionPin *pin pg_attribute_unused())
{
	return CLUSTER_WAL_PIN_STALE;
}

ClusterWalPinResult
cluster_wal_retention_pin_revalidate(ClusterWalRetentionPin *pin pg_attribute_unused())
{
	return CLUSTER_WAL_PIN_STALE;
}

#ifndef TEST_HAVE_CLUSTER_PAGE_ANCHOR_CACHE

UT_TEST(test_anchor_cache_capability_red)
{
	printf("# JIT_SEMANTIC_RED:D6-ANCHOR-CACHE\n");
	UT_ASSERT(false);
}

int
main(void)
{
	UT_PLAN(1);
	UT_RUN(test_anchor_cache_capability_red);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}

#else

int
errcode(int sqlerrcode pg_attribute_unused())
{
	return 0;
}

int
errmsg(const char *fmt pg_attribute_unused(), ...)
{
	return 0;
}

int
errmsg_internal(const char *fmt pg_attribute_unused(), ...)
{
	return 0;
}

bool
errstart_cold(int elevel pg_attribute_unused(), const char *domain pg_attribute_unused())
{
	return false;
}

void
errfinish(const char *filename pg_attribute_unused(), int lineno pg_attribute_unused(),
		  const char *funcname pg_attribute_unused())
{}

static void
fill_bytes(uint8 bytes[16], uint8 seed)
{
	int i;

	for (i = 0; i < 16; i++)
		bytes[i] = seed + i;
}

static RfPageAnchorCacheKeyV1
make_key(BlockNumber blockno)
{
	RfPageAnchorCacheKeyV1 key;

	memset(&key, 0, sizeof(key));
	key.page_identity.system_identifier = 9001;
	fill_bytes(key.page_identity.storage_uuid, 1);
	key.page_identity.locator.spcOid = 1663;
	key.page_identity.locator.dbOid = 5;
	key.page_identity.locator.relNumber = 42;
	key.page_identity.forknum = MAIN_FORKNUM;
	key.page_identity.blockno = blockno;
	fill_bytes(key.segment_incarnation, 20);
	fill_bytes(key.root_lineage, 40);
	key.checkpoint_epoch = 7;
	key.fpw_epoch = 3;
	return key;
}

UT_TEST(test_cache_miss_forces_image)
{
	RfPageAnchorCacheKeyV1 key = make_key(1);

	rf_page_anchor_cache_forget_all_v1();
	UT_ASSERT(rf_page_anchor_cache_should_force_v1(&key));
}

UT_TEST(test_successful_apply_image_earns_hit)
{
	RfPageAnchorCacheKeyV1 key = make_key(1);

	rf_page_anchor_cache_forget_all_v1();
	UT_ASSERT(rf_page_anchor_cache_record_v1(&key, true, true));
	UT_ASSERT(!rf_page_anchor_cache_should_force_v1(&key));
}

UT_TEST(test_failed_insert_never_earns_hit)
{
	RfPageAnchorCacheKeyV1 key = make_key(1);

	rf_page_anchor_cache_forget_all_v1();
	UT_ASSERT(!rf_page_anchor_cache_record_v1(&key, false, true));
	UT_ASSERT(rf_page_anchor_cache_should_force_v1(&key));
}

UT_TEST(test_nonapply_image_never_earns_hit)
{
	RfPageAnchorCacheKeyV1 key = make_key(1);

	rf_page_anchor_cache_forget_all_v1();
	UT_ASSERT(!rf_page_anchor_cache_record_v1(&key, true, false));
	UT_ASSERT(rf_page_anchor_cache_should_force_v1(&key));
}

UT_TEST(test_page_identity_change_is_miss)
{
	RfPageAnchorCacheKeyV1 key = make_key(1);
	RfPageAnchorCacheKeyV1 other = key;

	rf_page_anchor_cache_forget_all_v1();
	UT_ASSERT(rf_page_anchor_cache_record_v1(&key, true, true));
	other.page_identity.blockno++;
	UT_ASSERT(rf_page_anchor_cache_should_force_v1(&other));
}

UT_TEST(test_checkpoint_advance_is_miss)
{
	RfPageAnchorCacheKeyV1 key = make_key(1);
	RfPageAnchorCacheKeyV1 other = key;

	rf_page_anchor_cache_forget_all_v1();
	UT_ASSERT(rf_page_anchor_cache_record_v1(&key, true, true));
	other.checkpoint_epoch++;
	UT_ASSERT(rf_page_anchor_cache_should_force_v1(&other));
}

UT_TEST(test_root_lineage_change_is_miss)
{
	RfPageAnchorCacheKeyV1 key = make_key(1);
	RfPageAnchorCacheKeyV1 other = key;

	rf_page_anchor_cache_forget_all_v1();
	UT_ASSERT(rf_page_anchor_cache_record_v1(&key, true, true));
	other.root_lineage[15] ^= 0x80;
	UT_ASSERT(rf_page_anchor_cache_should_force_v1(&other));
}

UT_TEST(test_incarnation_change_is_miss)
{
	RfPageAnchorCacheKeyV1 key = make_key(1);
	RfPageAnchorCacheKeyV1 other = key;

	rf_page_anchor_cache_forget_all_v1();
	UT_ASSERT(rf_page_anchor_cache_record_v1(&key, true, true));
	other.segment_incarnation[15] ^= 0x80;
	UT_ASSERT(rf_page_anchor_cache_should_force_v1(&other));
}

UT_TEST(test_fpw_transition_is_miss)
{
	RfPageAnchorCacheKeyV1 key = make_key(1);
	RfPageAnchorCacheKeyV1 other = key;

	rf_page_anchor_cache_forget_all_v1();
	UT_ASSERT(rf_page_anchor_cache_record_v1(&key, true, true));
	other.fpw_epoch++;
	UT_ASSERT(rf_page_anchor_cache_should_force_v1(&other));
}

UT_TEST(test_explicit_cache_loss_is_safe_miss)
{
	RfPageAnchorCacheKeyV1 key = make_key(1);

	rf_page_anchor_cache_forget_all_v1();
	UT_ASSERT(rf_page_anchor_cache_record_v1(&key, true, true));
	rf_page_anchor_cache_forget_all_v1();
	UT_ASSERT(rf_page_anchor_cache_should_force_v1(&key));
}

UT_TEST(test_capacity_exhaustion_is_safe_miss)
{
	RfPageAnchorCacheKeyV1 key;
	int i;

	rf_page_anchor_cache_forget_all_v1();
	for (i = 0; i < RF_PAGE_ANCHOR_CACHE_CAPACITY; i++) {
		key = make_key((BlockNumber)i + 1);
		UT_ASSERT(rf_page_anchor_cache_record_v1(&key, true, true));
	}
	key = make_key(999);
	UT_ASSERT(!rf_page_anchor_cache_record_v1(&key, true, true));
	UT_ASSERT(rf_page_anchor_cache_should_force_v1(&key));
	key = make_key(1);
	UT_ASSERT(!rf_page_anchor_cache_should_force_v1(&key));
}

UT_TEST(test_invalid_key_never_earns_credit)
{
	RfPageAnchorCacheKeyV1 key = make_key(1);

	rf_page_anchor_cache_forget_all_v1();
	memset(key.root_lineage, 0, sizeof(key.root_lineage));
	UT_ASSERT(!rf_page_anchor_cache_record_v1(&key, true, true));
	UT_ASSERT(rf_page_anchor_cache_should_force_v1(&key));
	UT_ASSERT(rf_page_anchor_cache_should_force_v1(NULL));
}

UT_TEST(test_cache_miss_adds_explicit_force_flag)
{
	RfPageAnchorCacheKeyV1 key = make_key(1);
	uint8 flags;

	rf_page_anchor_cache_forget_all_v1();
	flags = rf_page_anchor_cache_registration_flags_v1(&key, REGBUF_WILL_INIT | REGBUF_STANDARD);
	UT_ASSERT((flags & REGBUF_FORCE_IMAGE) != 0);
	UT_ASSERT((flags & REGBUF_WILL_INIT) == REGBUF_WILL_INIT);
}

UT_TEST(test_cache_hit_preserves_caller_flags)
{
	RfPageAnchorCacheKeyV1 key = make_key(1);
	uint8 flags = REGBUF_STANDARD;

	rf_page_anchor_cache_forget_all_v1();
	UT_ASSERT(rf_page_anchor_cache_record_v1(&key, true, true));
	UT_ASSERT_EQ(rf_page_anchor_cache_registration_flags_v1(&key, flags), flags);
}

UT_TEST(test_anchor_key_binds_exact_registered_block)
{
	RfPageAnchorCacheKeyV1 key = make_key(11);
	RelFileLocator locator = key.page_identity.locator;

	UT_ASSERT(rf_page_anchor_cache_block_matches_v1(&key, &locator, MAIN_FORKNUM, 11));
	locator.relNumber++;
	UT_ASSERT(!rf_page_anchor_cache_block_matches_v1(&key, &locator, MAIN_FORKNUM, 11));
	locator = key.page_identity.locator;
	UT_ASSERT(!rf_page_anchor_cache_block_matches_v1(&key, &locator, FSM_FORKNUM, 11));
	UT_ASSERT(!rf_page_anchor_cache_block_matches_v1(&key, &locator, MAIN_FORKNUM, 12));
}

UT_TEST(test_anchor_key_binds_exact_result_incarnation)
{
	RfPageAnchorCacheKeyV1 key = make_key(11);
	uint8 incarnation[16];

	memcpy(incarnation, key.segment_incarnation, 16);
	UT_ASSERT(rf_page_anchor_cache_incarnation_matches_v1(&key, incarnation));
	incarnation[15] ^= 0x80;
	UT_ASSERT(!rf_page_anchor_cache_incarnation_matches_v1(&key, incarnation));
	UT_ASSERT(!rf_page_anchor_cache_incarnation_matches_v1(&key, NULL));
}

int
main(void)
{
	UT_PLAN(16);
	UT_RUN(test_cache_miss_forces_image);
	UT_RUN(test_successful_apply_image_earns_hit);
	UT_RUN(test_failed_insert_never_earns_hit);
	UT_RUN(test_nonapply_image_never_earns_hit);
	UT_RUN(test_page_identity_change_is_miss);
	UT_RUN(test_checkpoint_advance_is_miss);
	UT_RUN(test_root_lineage_change_is_miss);
	UT_RUN(test_incarnation_change_is_miss);
	UT_RUN(test_fpw_transition_is_miss);
	UT_RUN(test_explicit_cache_loss_is_safe_miss);
	UT_RUN(test_capacity_exhaustion_is_safe_miss);
	UT_RUN(test_invalid_key_never_earns_credit);
	UT_RUN(test_cache_miss_adds_explicit_force_flag);
	UT_RUN(test_cache_hit_preserves_caller_flags);
	UT_RUN(test_anchor_key_binds_exact_registered_block);
	UT_RUN(test_anchor_key_binds_exact_result_incarnation);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}

#endif
