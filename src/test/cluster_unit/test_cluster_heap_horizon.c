/* Real freeze-boundary consumer with original stripe widening arithmetic.
 * Author: SqlRush <sqlrush@gmail.com> */
int heap_horizon_stripe_fixture_main(void);
#define main heap_horizon_stripe_fixture_main
#include "test_cluster_xid_stripe.c"
#undef main
#include "cluster/cluster_heap_horizon.h"
#include "access/multixact.h"
#include "utils/rel.h"

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
		if (fault == 0) activation_available = false;
		if (fault == 1) activation_floor = 0;
		if (fault == 2) activation_epoch = 0;
		if (fault == 3) activation_generation = 0;
		if (fault == 4) cluster_xid_striping = false;
		if (fault == 5) stub_next_full = InvalidFullTransactionId;
		UT_ASSERT(!cluster_heap_freeze_cutoff_v1(fault == 6 ? FrozenTransactionId : 4195120,
			&cutoff));
		UT_ASSERT_EQ(cutoff, 1234);
	}
	UT_ASSERT(!cluster_heap_freeze_cutoff_v1(4195120, NULL));
}

UT_TEST(full_xid_window_cannot_alias_old_activation)
{
	TransactionId cutoff = 1234;
	setup_horizon();
	stub_next_full = FullTransactionIdFromU64(activation_floor + UINT64_C(0x80000000));
	UT_ASSERT(!cluster_heap_freeze_cutoff_v1((uint32)U64FromFullTransactionId(stub_next_full),
		&cutoff));
	UT_ASSERT_EQ(cutoff, 1234);
	activation_floor = U64FromFullTransactionId(stub_next_full) + UINT64_C(0x80000000);
	UT_ASSERT(!cluster_heap_freeze_cutoff_v1((uint32)U64FromFullTransactionId(stub_next_full),
		&cutoff));
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

int
main(void)
{
	UT_PLAN(7);
	UT_RUN(slower_writer_can_issue_below_local_freeze_proposal);
	UT_RUN(older_local_cutoff_is_never_advanced);
	UT_RUN(missing_or_invalid_authority_preserves_output);
	UT_RUN(full_xid_window_cannot_alias_old_activation);
	UT_RUN(wrap_crossing_uses_full_identity_and_skips_special_floor);
	UT_RUN(nonshared_mode_preserves_native_without_activation_read);
	UT_RUN(new_permanent_relation_is_not_exempt_as_transaction_local);
	UT_DONE();
	return ut_failed_count ? 1 : 0;
}
