/* PGRAC: relmap owner tests using the original native WAL writer above.
 * Only transaction/context providers are fixture boundaries. No relmap WAL
 * is inserted and no durable/retention or live-SQL completion is claimed.
 * Author: SqlRush <sqlrush@gmail.com>
 * Spec: 2026-10-14-pre2-m5-e1-format-closure-v1.md */
#include "access/xact.h"
#include "cluster/cluster_relmap_lock.h"
#include "cluster/cluster_relmap_owner.h"

static bool relmap_global_x, relmap_local_lock, relmap_in_transaction;
static uint64 relmap_top_full;
static unsigned relmap_assign_calls;
static bool relmap_advance_epoch_on_assign;

FullTransactionId
GetTopFullTransactionId(void)
{
	if (relmap_local_lock || CritSectionCount != 0 || !relmap_global_x)
		abort();
	relmap_assign_calls++;
	if (relmap_advance_epoch_on_assign)
		epoch++;
	return FullTransactionIdFromU64(relmap_top_full);
}

FullTransactionId
GetTopFullTransactionIdIfAny(void)
{
	return FullTransactionIdFromU64(relmap_top_full);
}

bool
IsTransactionState(void)
{
	return relmap_in_transaction;
}

TimeLineID
GetWALInsertionTimeLine(void)
{
	return native_ctl.InsertTimeLineID;
}

bool
LWLockHeldByMe(LWLock *lock)
{
	return lock == RelationMappingLock && relmap_local_lock;
}

bool
cluster_relmap_authority_x_held(void)
{
	return relmap_global_x;
}

#include "../../backend/cluster/cluster_relmap_owner.c"

static void
relmap_owner_fixture(void)
{
	fixture();
	CritSectionCount = 0;
	MyProcPid = 79;
	native_ctl.InsertTimeLineID = 1;
	relmap_global_x = relmap_in_transaction = true;
	relmap_local_lock = flush_probe_recovery = false;
	relmap_assign_calls = 0;
	relmap_advance_epoch_on_assign = false;
	relmap_top_full = UINT64CONST(0x400000123);
}

UT_TEST(test_relmap_capture_preserves_original_full_identity)
{
	ClusterRelmapOwnerSampleV2 sample;
	relmap_owner_fixture();
	UT_ASSERT(cluster_relmap_owner_capture_v2(&sample));
	UT_ASSERT_EQ(relmap_assign_calls, 1);
	UT_ASSERT_EQ(sample.owner.top_full_xid, UINT64CONST(0x400000123));
	UT_ASSERT_EQ(sample.owner.node, 0);
	UT_ASSERT_EQ(sample.owner.membership_epoch, 7);
	UT_ASSERT_EQ(sample.owner.boot_incarnation, 99);
	UT_ASSERT_EQ(sample.owner.admitted_incarnation, 99);
	UT_ASSERT_EQ(sample.owner.wal_end, 0);
	UT_ASSERT_EQ(sample.producer_pid, MyProcPid);
	UT_ASSERT_EQ(memcmp(&sample.writer.ref, &ref, sizeof(ref)), 0);
	UT_ASSERT(cluster_relmap_owner_matches_v2(&sample));
	UT_ASSERT_EQ(relmap_assign_calls, 1);
	UT_ASSERT_EQ(sync_calls + rename_calls, 0);
}

UT_TEST(test_relmap_capture_refuses_unsafe_context_before_assigning)
{
	for (unsigned fault = 0; fault < 12; fault++) {
		ClusterRelmapOwnerSampleV2 sample, zero = { 0 };
		relmap_owner_fixture();
		if (fault == 0)
			CritSectionCount = 1;
		if (fault == 1)
			relmap_local_lock = true;
		if (fault == 2)
			relmap_global_x = false;
		if (fault == 3)
			relmap_in_transaction = false;
		if (fault == 4)
			MyBackendType = B_BG_WRITER;
		if (fault == 5)
			ShutdownRequestPending = true;
		if (fault == 6)
			cluster_enabled = false;
		if (fault == 7)
			cluster_shared_config = false;
		if (fault == 8)
			cluster_node_id = -1;
		if (fault == 9)
			cluster_node_id = CLUSTER_MAX_NODES;
		if (fault == 10)
			MyProcPid = 0;
		if (fault == 11)
			flush_probe_recovery = true;
		memset(&sample, 0x55, sizeof(sample));
		UT_ASSERT(!cluster_relmap_owner_capture_v2(&sample));
		UT_ASSERT_EQ(memcmp(&sample, &zero, sizeof(zero)), 0);
		UT_ASSERT_EQ(relmap_assign_calls, 0);
		cluster_node_id = 0;
	}
	relmap_owner_fixture();
	UT_ASSERT(!cluster_relmap_owner_capture_v2(NULL));
	UT_ASSERT_EQ(relmap_assign_calls, 0);
}

UT_TEST(test_relmap_writer_is_sampled_after_original_top_assignment)
{
	ClusterRelmapOwnerSampleV2 sample;
	relmap_owner_fixture();
	relmap_advance_epoch_on_assign = true;
	UT_ASSERT(cluster_relmap_owner_capture_v2(&sample));
	UT_ASSERT_EQ(sample.writer.epoch, 8);
	UT_ASSERT_EQ(sample.owner.membership_epoch, 8);
	UT_ASSERT_EQ(relmap_assign_calls, 1);
}

UT_TEST(test_relmap_invalid_top_or_unqualified_writer_cannot_be_captured)
{
	for (unsigned fault = 0; fault < 9; fault++) {
		ClusterRelmapOwnerSampleV2 sample, zero = { 0 };
		relmap_owner_fixture();
		if (fault == 0)
			relmap_top_full = 0;
		if (fault == 1)
			relmap_top_full = UINT64CONST(0x400000002);
		if (fault == 2)
			have_ref = false;
		if (fault == 3)
			member = false;
		if (fault == 4)
			self_fenced = true;
		if (fault == 5)
			ref.claim.identity.origin_owner_incarnation++;
		if (fault == 6)
			relmap_admission_delta = 1;
		if (fault == 7)
			epoch = 0; /* native writer refuses; codec still allows zero */
		if (fault == 8)
			active = false;
		memset(&sample, 0x55, sizeof(sample));
		UT_ASSERT(!cluster_relmap_owner_capture_v2(&sample));
		UT_ASSERT_EQ(memcmp(&sample, &zero, sizeof(zero)), 0);
		UT_ASSERT_EQ(relmap_assign_calls, 1);
	}
}

UT_TEST(test_relmap_revalidation_never_adopts_new_writer_or_reassigns_top)
{
	for (unsigned fault = 0; fault < 14; fault++) {
		ClusterRelmapOwnerSampleV2 sample, before;
		relmap_owner_fixture();
		UT_ASSERT(cluster_relmap_owner_capture_v2(&sample));
		before = sample;
		if (fault == 0)
			epoch++;
		if (fault == 1)
			ref.claim.claim_sha256[0]++;
		if (fault == 2)
			ref.claim.database_incarnation++;
		if (fault == 3)
			native_ctl.InsertTimeLineID++;
		if (fault == 4)
			incarnation++;
		if (fault == 5)
			relmap_admission_delta = 1;
		if (fault == 6)
			relmap_top_full += UINT64CONST(0x100000000); /* same raw XID */
		if (fault == 7)
			MyProcPid++;
		if (fault == 8)
			relmap_global_x = false;
		if (fault == 9)
			self_fenced = true;
		if (fault == 10)
			member = false;
		if (fault == 11)
			have_ref = false;
		if (fault == 12)
			CritSectionCount = 1;
		if (fault == 13)
			ShutdownRequestPending = true;
		UT_ASSERT(!cluster_relmap_owner_matches_v2(&sample));
		UT_ASSERT_EQ(memcmp(&sample, &before, sizeof(sample)), 0);
		UT_ASSERT_EQ(relmap_assign_calls, 1);
		UT_ASSERT_EQ(sync_calls + rename_calls, 0);
	}
}

UT_TEST(test_relmap_sample_fields_and_original_lock_context_are_exact)
{
	ClusterRelmapOwnerSampleV2 sample;

	for (unsigned fault = 0; fault < 7; fault++) {
		relmap_owner_fixture();
		UT_ASSERT(cluster_relmap_owner_capture_v2(&sample));
		if (fault == 0)
			sample.owner.node++;
		if (fault == 1)
			sample.owner.boot_incarnation++;
		if (fault == 2)
			sample.owner.admitted_incarnation++;
		if (fault == 3)
			sample.owner.membership_epoch++;
		if (fault == 4)
			sample.owner.wal_end = 42;
		if (fault == 5)
			sample.writer.startup_first_lsn = 42;
		if (fault == 6)
			sample.owner.top_full_xid++;
		UT_ASSERT(!cluster_relmap_owner_matches_v2(&sample));
	}
	relmap_owner_fixture();
	UT_ASSERT(cluster_relmap_owner_capture_v2(&sample));
	relmap_local_lock = true; /* recheck must not allocate under this lock */
	UT_ASSERT(cluster_relmap_owner_matches_v2(&sample));
	UT_ASSERT_EQ(relmap_assign_calls, 1);
	UT_ASSERT(!cluster_relmap_owner_matches_v2(NULL));
}
