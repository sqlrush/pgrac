/* PGRAC: relmap owner tests using the original native WAL writer above.
 * Transaction/context, TT receipt and lock providers are fixture boundaries.
 * No relmap WAL, TT publication or live-SQL completion is claimed.
 * Author: SqlRush <sqlrush@gmail.com>
 * Spec: 2026-10-14-pre2-m5-e1-format-closure-v1.md */
#include "access/xact.h"
#include "cluster/cluster_relmap_lock.h"
#include "cluster/cluster_relmap_owner.h"
#include "cluster/cluster_relmap_retention.h"
#include "cluster/cluster_tt_slot.h"

static bool relmap_global_x, relmap_local_lock, relmap_in_transaction;
static uint64 relmap_top_full;
static unsigned relmap_assign_calls;
static bool relmap_advance_epoch_on_assign;
static bool relmap_have_binding, relmap_binding_drift;
static bool relmap_active_owner;
static unsigned relmap_binding_owner_fault;
static unsigned relmap_binding_reads;
static ClusterCanonicalTxnBinding relmap_binding;

static void
relmap_retention_lock_enter(void)
{
	if ((!relmap_global_x && !relmap_active_owner) || relmap_local_lock || CritSectionCount != 0)
		abort();
}

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
	if (relmap_retention_held)
		abort();
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
	if (lock == &relmap_retention_lock || lock == relmap_runtime_test_lock)
		return relmap_retention_held;
	return lock == RelationMappingLock && relmap_local_lock;
}

bool
cluster_tt_local_get_published_binding(TransactionId top_xid, ClusterCanonicalTxnBinding *out)
{
	if (relmap_retention_held || relmap_local_lock || (!relmap_global_x && !relmap_active_owner)
		|| CritSectionCount != 0 || top_xid != (uint32) relmap_top_full)
		abort();
	relmap_binding_reads++;
	memset(out, 0, sizeof(*out));
	if (relmap_have_binding)
		*out = relmap_binding;
	if (relmap_binding_drift)
		relmap_top_full += UINT64CONST(0x100000000);
	if (relmap_binding_owner_fault == 1) epoch++;
	if (relmap_binding_owner_fault == 2) ref.claim.claim_sha256[0]++;
	if (relmap_binding_owner_fault == 3) native_ctl.InsertTimeLineID++;
	if (relmap_binding_owner_fault == 4) ShutdownRequestPending = true;
	if (relmap_binding_owner_fault == 5) cluster_node_id++;
	if (relmap_binding_owner_fault == 6) have_ref = false;
	if (relmap_binding_owner_fault == 7) relmap_admission_delta = 1;
	return relmap_have_binding;
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
	relmap_retention_held = relmap_have_binding = relmap_binding_drift = false;
	relmap_active_owner = false;
	relmap_binding_owner_fault = 0;
	relmap_retention_acquires = relmap_binding_reads = 0;
	memset(&relmap_binding, 0, sizeof(relmap_binding));
	relmap_binding.segment_id = 12;
	relmap_binding.xid = (uint32) relmap_top_full;
	relmap_binding.slot_offset = 3;
	relmap_binding.origin_instance = 1;
	relmap_binding.publish_state = CLUSTER_CANONICAL_TXN_PUBLISHED;
	relmap_binding.active_lsn = 456;
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

static ClusterRelmapV2Namespace
relmap_owner_namespace(void)
{
	ClusterRelmapV2Namespace identity = { 0 };
	identity.system_identifier = ref.claim.identity.system_identifier;
	identity.database_incarnation = ref.claim.database_incarnation;
	identity.dbid = MyDatabaseId;
	return identity;
}

static void
relmap_owner_ready(ClusterRelmapRetentionDirectory *directory)
{
	memset(directory, 0, sizeof(*directory));
	UT_ASSERT_EQ(cluster_relmap_retention_build_start(directory,
		ref.claim.identity.system_identifier, ref.claim.database_incarnation, 0, 7,
		relmap_top_full - 100, relmap_top_full + 100), CLUSTER_RELMAP_RETENTION_OK);
	UT_ASSERT_EQ(cluster_relmap_retention_build_finish(directory), CLUSTER_RELMAP_RETENTION_OK);
}

UT_TEST(test_relmap_owner_retention_uses_original_key_and_published_receipt)
{
	ClusterRelmapOwnerSampleV2 sample;
	ClusterRelmapRetentionDirectory directory;
	ClusterRelmapV2Namespace identity;
	const ClusterRelmapRetentionPin *pin;

	relmap_owner_fixture();
	UT_ASSERT(cluster_relmap_owner_capture_v2(&sample));
	identity = relmap_owner_namespace();
	relmap_owner_ready(&directory);
	relmap_have_binding = true;
	UT_ASSERT_EQ(cluster_relmap_owner_retention_begin_v2(&sample, &identity, 8,
		&directory, &relmap_retention_lock), CLUSTER_RELMAP_RETENTION_OK);
	pin = &directory.pins[0];
	UT_ASSERT(pin->occupied && pin->has_tt_binding && !pin->may_be_staged);
	UT_ASSERT_EQ(pin->wal_end, 0);
	UT_ASSERT_EQ(pin->key.top_full_xid, sample.owner.top_full_xid);
	UT_ASSERT_EQ(pin->key.membership_epoch, sample.owner.membership_epoch);
	UT_ASSERT_EQ(pin->key.admitted_incarnation, sample.owner.admitted_incarnation);
	UT_ASSERT_EQ(pin->key.boot_incarnation, sample.owner.boot_incarnation);
	UT_ASSERT_EQ(pin->key.origin, sample.owner.node);
	UT_ASSERT_EQ(pin->key.generation, 8);
	UT_ASSERT_EQ(pin->key.identity.system_identifier, identity.system_identifier);
	UT_ASSERT_EQ(pin->key.identity.database_incarnation, identity.database_incarnation);
	UT_ASSERT_EQ(pin->key.identity.dbid, MyDatabaseId);
	UT_ASSERT(!pin->key.identity.shared);
	UT_ASSERT_EQ(memcmp(&pin->binding, &relmap_binding, sizeof(relmap_binding)), 0);
	UT_ASSERT_EQ(relmap_assign_calls, 1);
	UT_ASSERT_EQ(relmap_binding_reads, 1);
	UT_ASSERT_EQ(relmap_retention_acquires, 1);
	UT_ASSERT(!relmap_retention_held);
	UT_ASSERT_EQ(sync_calls + rename_calls, 0);
	identity.shared = true;
	identity.dbid = InvalidOid;
	UT_ASSERT_EQ(cluster_relmap_owner_retention_begin_v2(&sample, &identity, 8,
		&directory, &relmap_retention_lock), CLUSTER_RELMAP_RETENTION_OK);
	UT_ASSERT(directory.pins[1].key.identity.shared && directory.pins[1].has_tt_binding);
}

UT_TEST(test_relmap_owner_retention_absent_binding_stays_unresolved)
{
	ClusterRelmapOwnerSampleV2 sample;
	ClusterRelmapRetentionDirectory directory;
	ClusterRelmapV2Namespace identity;

	relmap_owner_fixture();
	UT_ASSERT(cluster_relmap_owner_capture_v2(&sample));
	identity = relmap_owner_namespace();
	relmap_owner_ready(&directory);
	UT_ASSERT_EQ(cluster_relmap_owner_retention_begin_v2(&sample, &identity, 8,
		&directory, &relmap_retention_lock), CLUSTER_RELMAP_RETENTION_OK);
	UT_ASSERT(directory.pins[0].occupied && !directory.pins[0].has_tt_binding);
	UT_ASSERT_EQ(cluster_relmap_retention_tt_release_check(&directory, &relmap_binding),
		CLUSTER_RELMAP_RETENTION_UNAVAILABLE);
	UT_ASSERT_EQ(relmap_binding_reads, 1);
	UT_ASSERT_EQ(relmap_assign_calls, 1);
}

UT_TEST(test_relmap_owner_retention_refuses_stale_or_unsafe_owner)
{
	for (unsigned fault = 0; fault < 12; fault++) {
		ClusterRelmapOwnerSampleV2 sample;
		ClusterRelmapRetentionDirectory directory, before;
		ClusterRelmapV2Namespace identity;

		relmap_owner_fixture();
		UT_ASSERT(cluster_relmap_owner_capture_v2(&sample));
		identity = relmap_owner_namespace();
		relmap_owner_ready(&directory);
		before = directory;
		if (fault == 0) relmap_local_lock = true;
		if (fault == 1) relmap_retention_held = true;
		if (fault == 2) CritSectionCount = 1;
		if (fault == 3) relmap_global_x = false;
		if (fault == 4) relmap_in_transaction = false;
		if (fault == 5) ShutdownRequestPending = true;
		if (fault == 6) MyProcPid++;
		if (fault == 7) epoch++;
		if (fault == 8) ref.claim.claim_sha256[0]++;
		if (fault == 9) relmap_top_full += UINT64CONST(0x100000000);
		if (fault == 10) flush_probe_recovery = true;
		if (fault == 11) MyBackendType = B_BG_WRITER;
		UT_ASSERT_NE(cluster_relmap_owner_retention_begin_v2(&sample, &identity, 8,
			&directory, &relmap_retention_lock), CLUSTER_RELMAP_RETENTION_OK);
		UT_ASSERT_EQ(memcmp(&directory, &before, sizeof(directory)), 0);
		UT_ASSERT_EQ(relmap_binding_reads + relmap_retention_acquires, 0);
		UT_ASSERT_EQ(relmap_assign_calls, 1);
	}
}

UT_TEST(test_relmap_owner_retention_rejects_foreign_namespace_and_arguments)
{
	for (unsigned fault = 0; fault < 10; fault++) {
		ClusterRelmapOwnerSampleV2 sample;
		ClusterRelmapRetentionDirectory directory, before;
		ClusterRelmapV2Namespace identity;
		const ClusterRelmapOwnerSampleV2 *sample_arg = &sample;
		const ClusterRelmapV2Namespace *identity_arg = &identity;
		ClusterRelmapRetentionDirectory *directory_arg = &directory;
		LWLock *lock_arg = &relmap_retention_lock;

		relmap_owner_fixture();
		UT_ASSERT(cluster_relmap_owner_capture_v2(&sample));
		identity = relmap_owner_namespace();
		relmap_owner_ready(&directory);
		before = directory;
		if (fault == 0) identity.system_identifier++;
		if (fault == 1) identity.database_incarnation++;
		if (fault == 2) identity.dbid++;
		if (fault == 3) identity.shared = true;
		if (fault == 4) identity.dbid = InvalidOid;
		if (fault == 5) sample_arg = NULL;
		if (fault == 6) identity_arg = NULL;
		if (fault == 7) directory_arg = NULL;
		if (fault == 8) lock_arg = NULL;
		if (fault == 9) lock_arg = RelationMappingLock;
		UT_ASSERT_NE(cluster_relmap_owner_retention_begin_v2(sample_arg, identity_arg, 8,
			directory_arg, lock_arg), CLUSTER_RELMAP_RETENTION_OK);
		UT_ASSERT_EQ(memcmp(&directory, &before, sizeof(directory)), 0);
		UT_ASSERT_EQ(relmap_binding_reads + relmap_retention_acquires, 0);
	}
}

UT_TEST(test_relmap_owner_retention_rechecks_after_original_binding_read)
{
	ClusterRelmapOwnerSampleV2 sample;
	ClusterRelmapRetentionDirectory directory, before;
	ClusterRelmapV2Namespace identity;

	relmap_owner_fixture();
	UT_ASSERT(cluster_relmap_owner_capture_v2(&sample));
	identity = relmap_owner_namespace();
	relmap_owner_ready(&directory);
	before = directory;
	relmap_have_binding = relmap_binding_drift = true;
	UT_ASSERT_EQ(cluster_relmap_owner_retention_begin_v2(&sample, &identity, 8,
		&directory, &relmap_retention_lock), CLUSTER_RELMAP_RETENTION_UNAVAILABLE);
	UT_ASSERT_EQ(memcmp(&directory, &before, sizeof(directory)), 0);
	UT_ASSERT_EQ(relmap_binding_reads, 1);
	UT_ASSERT_EQ(relmap_retention_acquires, 0);
	UT_ASSERT_EQ(relmap_assign_calls, 1);
}

UT_TEST(test_relmap_owner_retention_preserves_core_refusals)
{
	for (unsigned fault = 0; fault < 10; fault++) {
		ClusterRelmapOwnerSampleV2 sample;
		ClusterRelmapRetentionDirectory directory, before;
		ClusterRelmapV2Namespace identity;
		uint64 generation = 8;
		ClusterRelmapRetentionResult expected;

		relmap_owner_fixture();
		UT_ASSERT(cluster_relmap_owner_capture_v2(&sample));
		identity = relmap_owner_namespace();
		relmap_owner_ready(&directory);
		expected = CLUSTER_RELMAP_RETENTION_UNAVAILABLE;
		if (fault == 0) directory.state = CLUSTER_RELMAP_RETENTION_UNKNOWN;
		if (fault == 1) directory.state = CLUSTER_RELMAP_RETENTION_BUILDING;
		if (fault == 2) { directory.origin++; expected = CLUSTER_RELMAP_RETENTION_INVALID; }
		if (fault == 3) { directory.native_floor = relmap_top_full + 1; expected = CLUSTER_RELMAP_RETENTION_NATIVE_GONE; }
		if (fault == 4) { directory.last_issued = relmap_top_full - 1; expected = CLUSTER_RELMAP_RETENTION_INVALID; }
		if (fault == 5) { generation = 1; expected = CLUSTER_RELMAP_RETENTION_INVALID; }
		if (fault == 6) { relmap_have_binding = true; relmap_binding.xid++; expected = CLUSTER_RELMAP_RETENTION_INVALID; }
		if (fault == 7) {
			UT_ASSERT_EQ(cluster_relmap_owner_retention_begin_v2(&sample, &identity, 8,
				&directory, &relmap_retention_lock), CLUSTER_RELMAP_RETENTION_OK);
			directory.pins[0].may_be_staged = true;
			directory.pins[0].wal_end = 600;
			expected = CLUSTER_RELMAP_RETENTION_RETAINED;
		}
		if (fault == 8) {
			directory.last_issued = relmap_top_full + UINT64CONST(0x80000000);
			expected = CLUSTER_RELMAP_RETENTION_XID_WINDOW;
		}
		if (fault == 9) {
			ClusterRelmapRetentionTruncate reservation;
			UT_ASSERT_EQ(cluster_relmap_retention_truncate_begin(&directory, relmap_top_full + 1,
				&reservation), CLUSTER_RELMAP_RETENTION_OK);
			expected = CLUSTER_RELMAP_RETENTION_NATIVE_GONE;
		}
		before = directory;
		UT_ASSERT_EQ(cluster_relmap_owner_retention_begin_v2(&sample, &identity, generation,
			&directory, &relmap_retention_lock), expected);
		UT_ASSERT_EQ(memcmp(&directory, &before, sizeof(directory)), 0);
		UT_ASSERT(!relmap_retention_held);
		UT_ASSERT_EQ(relmap_assign_calls, 1);
	}
}
