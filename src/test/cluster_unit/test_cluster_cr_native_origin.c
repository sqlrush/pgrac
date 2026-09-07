/*-------------------------------------------------------------------------
 * test_cluster_cr_native_origin.c
 *
 * Exercise the production native-origin predicate and stripe derivation.
 * Shared memory, LWLocks and nextXid are process-boundary fixtures only.
 * No production branch is removed by a unit-test conditional.
 *
 * Portions Copyright (c) 2026, pgrac contributors
 *-------------------------------------------------------------------------
 */
#include "postgres.h"
#ifndef CLUSTER_CR_SOURCE_PATH
#error "The exact production CR translation unit is required"
#endif
#include CLUSTER_CR_SOURCE_PATH
#include "cluster/cluster_xid_stripe_boot.h"
#undef printf
#include "unit_test.h"

UT_DEFINE_GLOBALS();

bool cluster_enabled = true;
bool cluster_xid_striping = true;
int cluster_node_id = 0;
static ClusterCRShared native_shared;
static FullTransactionId native_next;
static int native_lock_depth;

FullTransactionId
ReadNextFullTransactionId(void)
{
	return native_next;
}

void
cluster_xid_stripe_lazy_latch(void)
{}

bool
LWLockAcquire(LWLock *lock, LWLockMode mode)
{
	UT_ASSERT(lock == &native_shared.native_prehistory_lock);
	UT_ASSERT_EQ(mode, LW_SHARED);
	native_lock_depth++;
	return true;
}

void
LWLockRelease(LWLock *lock)
{
	UT_ASSERT(lock == &native_shared.native_prehistory_lock);
	UT_ASSERT_EQ(native_lock_depth, 1);
	native_lock_depth--;
}

bool
LWLockHeldByMeInMode(LWLock *lock, LWLockMode mode)
{
	return lock == &native_shared.native_prehistory_lock && mode == LW_SHARED
		   && native_lock_depth == 1;
}

void
ExceptionalCondition(const char *conditionName pg_attribute_unused(),
					 const char *fileName pg_attribute_unused(),
					 int lineNumber pg_attribute_unused())
{
	abort();
}

static void
native_reset(void)
{
	memset(&native_shared, 0, sizeof(native_shared));
	pg_atomic_init_u64(&native_shared.native_prehistory_disabled, 0);
	pg_atomic_init_u64(&native_shared.native_prehistory_covered_hw, 0);
	CRShared = &native_shared;
	native_lock_depth = 0;
	native_next = FullTransactionIdFromU64(4239264);
	cluster_xid_stripe_latch_runtime(true, 0, FullTransactionIdFromU64(4195104));
}

UT_TEST(native_absent_shared_state_is_not_a_positive_proof)
{
	native_reset();
	CRShared = NULL;
	cluster_cr_native_prehistory_reader_lock();
	UT_ASSERT(!cluster_cr_native_origin_epoch0_provable(4238864));
	cluster_cr_native_prehistory_reader_unlock();
	UT_ASSERT_EQ(native_lock_depth, 0);
}

UT_TEST(native_zero_prehistory_does_not_disable_own_epoch_zero_identity)
{
	native_reset();
	cluster_cr_native_prehistory_reader_lock();
	UT_ASSERT_EQ(cluster_cr_native_prehistory_covered_hw(), 0);
	UT_ASSERT(cluster_cr_native_origin_epoch0_provable(4238864));
	cluster_cr_native_prehistory_reader_unlock();
	UT_ASSERT_EQ(native_lock_depth, 0);
}

UT_TEST(native_disabled_or_later_epoch_has_no_alias_free_proof)
{
	for (int disabled = 0; disabled < 2; disabled++) {
		native_reset();
		if (disabled)
			pg_atomic_write_u64(&native_shared.native_prehistory_disabled, 1);
		else
			native_next = FullTransactionIdFromU64((UINT64CONST(1) << 32) + 4239264);
		cluster_cr_native_prehistory_reader_lock();
		UT_ASSERT(!cluster_cr_native_origin_epoch0_provable(4238864));
		cluster_cr_native_prehistory_reader_unlock();
	}
}

UT_TEST(native_wrong_origin_prehistory_and_unallocated_xids_refuse)
{
	const TransactionId rejected[]
		= { InvalidTransactionId, FrozenTransactionId, 4238865, 4195088, 4239264, 4239280 };

	native_reset();
	cluster_cr_native_prehistory_reader_lock();
	for (unsigned i = 0; i < lengthof(rejected); i++)
		UT_ASSERT(!cluster_cr_native_origin_epoch0_provable(rejected[i]));
	cluster_cr_native_prehistory_reader_unlock();
}

UT_TEST(native_inactive_stripe_is_not_origin_authority)
{
	native_reset();
	cluster_xid_stripe_latch_runtime(false, 0, InvalidFullTransactionId);
	cluster_cr_native_prehistory_reader_lock();
	UT_ASSERT(!cluster_cr_native_origin_epoch0_provable(4238864));
	cluster_cr_native_prehistory_reader_unlock();
}

int
main(void)
{
	UT_PLAN(5);
	UT_RUN(native_absent_shared_state_is_not_a_positive_proof);
	UT_RUN(native_zero_prehistory_does_not_disable_own_epoch_zero_identity);
	UT_RUN(native_disabled_or_later_epoch_has_no_alias_free_proof);
	UT_RUN(native_wrong_origin_prehistory_and_unallocated_xids_refuse);
	UT_RUN(native_inactive_stripe_is_not_origin_authority);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
