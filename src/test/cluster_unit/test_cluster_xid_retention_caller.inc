/* Original owner capture/begin beside actual native retained allocation.
 * Xact state, native writer authority, TT lookup and scheduling are leaves.
 * Author: SqlRush <sqlrush@gmail.com> */
#include "cluster/cluster_membership.h"
#include "cluster/cluster_qvotec.h"
#include "cluster/cluster_relmap_authority.h"
#include "cluster/cluster_relmap_lock.h"

BackendType MyBackendType;
int MyProcPid;
Oid MyDatabaseId;
volatile uint32 CritSectionCount;
volatile sig_atomic_t ShutdownRequestPending;
static bool caller_transaction, caller_global_x, caller_writer_ready;
static bool caller_binding_drift;
static uint64 caller_boot, caller_admitted;
static FullTransactionId caller_top;
static ClusterWalWriterToken caller_writer;
static unsigned caller_assignments, caller_owner_lock_errors;
static FullTransactionId (*caller_top_reader)(void);
static FullTransactionId (*caller_top_if_any_reader)(void);
static bool (*caller_state_reader)(void);

static void
caller_observation(void)
{
	if (held || retention_held) caller_owner_lock_errors++;
}

bool
IsTransactionState(void)
{
	caller_observation();
	if (caller_state_reader != NULL) return caller_state_reader();
	return caller_transaction;
}

bool
cluster_relmap_authority_x_held(void)
{
	caller_observation();
	return caller_global_x;
}

TimeLineID
GetWALInsertionTimeLine(void)
{
	caller_observation();
	return 1;
}

uint64
cluster_qvotec_get_self_incarnation(void)
{
	caller_observation();
	return caller_boot;
}

uint64
cluster_membership_get_last_admitted_incarnation(int node)
{
	caller_observation();
	return node == cluster_node_id ? caller_admitted : 0;
}

ClusterControlRootResult
cluster_wal_writer_begin(TimeLineID timeline, ClusterWalWriterToken *out)
{
	caller_observation();
	if (!caller_writer_ready || timeline != 1 || out == NULL)
		return CLUSTER_CONTROL_ROOT_LOCK_UNAVAILABLE;
	*out = caller_writer;
	return CLUSTER_CONTROL_ROOT_OK_PRIMARY;
}

ClusterControlRootResult
cluster_wal_writer_check(const ClusterWalWriterToken *token)
{
	caller_observation();
	return caller_writer_ready && memcmp(token, &caller_writer, sizeof(*token)) == 0
		? CLUSTER_CONTROL_ROOT_OK_PRIMARY : CLUSTER_CONTROL_ROOT_LOCK_UNAVAILABLE;
}

FullTransactionId
GetTopFullTransactionId(void)
{
	caller_observation();
	if (caller_top_reader != NULL) {
		caller_top = caller_top_reader();
		return caller_top;
	}
	if (!FullTransactionIdIsValid(caller_top)) {
		caller_top = cluster_relmap_retention_allocate_xid_v2(false,
			retention_sysid, retention_dbinc, retention_origin);
		caller_assignments++;
	}
	return caller_top;
}

FullTransactionId
GetTopFullTransactionIdIfAny(void)
{
	caller_observation();
	if (caller_top_if_any_reader != NULL) return caller_top_if_any_reader();
	return caller_top;
}

bool
cluster_tt_local_get_published_binding(TransactionId top, ClusterCanonicalTxnBinding *out)
{
	caller_observation();
	if (top != XidFromFullTransactionId(caller_top)) caller_owner_lock_errors++;
	(void)out;
	if (caller_binding_drift) caller_boot++;
	return false; /* No TT receipt is fabricated for these native-only pins. */
}

#include "test_cluster_xid_retention_owner.inc"

static void
caller_fixture(uint64 first, unsigned origin)
{
	retention_fixture(first, origin);
	retention_owner_begin = false;
	memset(&relmap_runtime_active_owner, 0, sizeof(relmap_runtime_active_owner));
	caller_transaction = caller_global_x = caller_writer_ready = true;
	caller_binding_drift = false;
	caller_top_reader = caller_top_if_any_reader = NULL;
	caller_state_reader = NULL;
	caller_assignments = caller_owner_lock_errors = 0;
	caller_top = InvalidFullTransactionId;
	MyBackendType = B_BACKEND;
	MyProcPid = 201;
	MyDatabaseId = 123;
	ShutdownRequestPending = false;
	CritSectionCount = 0;
	cluster_node_id = origin;
	caller_boot = 17;
	caller_admitted = 31;
	memset(&caller_writer, 0, sizeof(caller_writer));
	caller_writer.epoch = 43;
	caller_writer.ref.timeline = 1;
	caller_writer.ref.claim.identity.system_identifier = retention_sysid;
	caller_writer.ref.claim.database_incarnation = retention_dbinc;
	caller_writer.ref.claim.identity.origin_node_id = origin;
	caller_writer.ref.claim.identity.origin_thread_id = origin + 1;
	caller_writer.ref.claim.identity.origin_owner_incarnation = caller_admitted;
}

static ClusterRelmapV2Namespace
caller_namespace(bool shared)
{
	ClusterRelmapV2Namespace identity = { 0 };

	identity.system_identifier = retention_sysid;
	identity.database_incarnation = retention_dbinc;
	identity.shared = shared;
	identity.dbid = shared ? InvalidOid : MyDatabaseId;
	return identity;
}

static ClusterRelmapRetentionResult
caller_begin(const ClusterRelmapOwnerSampleV2 *sample, bool shared)
{
	ClusterRelmapV2Namespace identity = caller_namespace(shared);
	ClusterRelmapRetentionResult result;

	retention_owner_begin = true;
	result = cluster_relmap_retention_runtime_begin_v2(sample, &identity, shared ? 7 : 8);
	retention_owner_begin = false;
	UT_ASSERT(!held && !retention_held);
	UT_ASSERT_EQ(InterruptHoldoffCount, 0);
	return result;
}

UT_TEST(original_owner_capture_and_begin_use_the_native_assignment)
{
	for (unsigned which = 0; which < 4; which++) {
		uint64 first = ((which / 2 ? UINT64CONST(7) : 0) << 32) + 2052;
		unsigned origin = which % 2 ? CLUSTER_MAX_NODES - 1 : 0;
		ClusterRelmapOwnerSampleV2 sample;
		ClusterRelmapRetentionDirectory *directory;
		ClusterRelmapRetentionPin saved[CLUSTER_RELMAP_RETENTION_CAPACITY];
		bool captured;

		caller_fixture(first, origin);
		captured = cluster_relmap_owner_capture_v2(&sample);
		UT_ASSERT(captured);
		if (!captured) return;
		UT_ASSERT_EQ(sample.owner.top_full_xid, first);
		UT_ASSERT_EQ(sample.owner.node, origin);
		UT_ASSERT_EQ(caller_assignments, 1);
		UT_ASSERT_EQ(proc.xid, 2052);
		UT_ASSERT_EQ(visible_xids[0], 2052);
		UT_ASSERT_EQ(U64FromFullTransactionId(variables.clusterLastIssuedXid), first);
		UT_ASSERT_EQ(caller_begin(&sample, true), CLUSTER_RELMAP_RETENTION_OK);
		UT_ASSERT_EQ(caller_begin(&sample, false), CLUSTER_RELMAP_RETENTION_OK);
		directory = &relmap_runtime_shmem->directories[origin];
		UT_ASSERT(directory->pins[0].occupied && directory->pins[1].occupied);
		UT_ASSERT_EQ(directory->pins[0].key.top_full_xid, first);
		UT_ASSERT_EQ(directory->pins[1].key.top_full_xid, first);
		UT_ASSERT(directory->pins[0].key.identity.shared && !directory->pins[1].key.identity.shared);
		UT_ASSERT(!directory->pins[0].has_tt_binding && !directory->pins[1].has_tt_binding);
		memcpy(saved, directory->pins, sizeof(saved));
		UT_ASSERT_EQ(U64FromFullTransactionId(retention_call(true, 0)), first + 1);
		UT_ASSERT_EQ(memcmp(saved, directory->pins, sizeof(saved)), 0);
		UT_ASSERT_EQ(directory->last_issued, first + 1);
		UT_ASSERT_EQ(retention_final_observations, 2);
		UT_ASSERT_EQ(caller_owner_lock_errors, 0);
	}
}

UT_TEST(original_namespace_pin_survives_other_prepare_cancellation)
{
	for (unsigned era = 0; era < 2; era++) {
		uint64 first = ((era ? UINT64CONST(7) : 0) << 32) + 2052;
		uint64 limit = first + UINT64CONST(0x80000000);
		ClusterRelmapOwnerSampleV2 sample;
		ClusterRelmapRetentionDirectory *directory;
		ClusterRelmapRetentionKey shared;
		ClusterRelmapRetentionPin remaining;
		unsigned before_extensions;
		bool captured;

		caller_fixture(first, 0);
		captured = cluster_relmap_owner_capture_v2(&sample);
		UT_ASSERT(captured);
		if (!captured) return;
		UT_ASSERT_EQ(caller_begin(&sample, true), CLUSTER_RELMAP_RETENTION_OK);
		UT_ASSERT_EQ(caller_begin(&sample, false), CLUSTER_RELMAP_RETENTION_OK);
		directory = &relmap_runtime_shmem->directories[0];
		shared = directory->pins[0].key;
		remaining = directory->pins[1];
		variables.nextXid = FullTransactionIdFromU64(limit - 1);
		variables.xidVacLimit = (uint32)limit + 4096;
		variables.xidWarnLimit = (uint32)limit + 8192;
		UT_ASSERT_EQ(U64FromFullTransactionId(retention_call(true, 0)), limit - 1);
		before_extensions = extensions;
		(void)retention_call(true, ERRCODE_CLUSTER_XID_AUTHORITY_UNAVAILABLE);
		UT_ASSERT_EQ(extensions, before_extensions);
		UT_ASSERT_EQ(directory->last_issued, limit - 1);
		UT_ASSERT_EQ(U64FromFullTransactionId(variables.nextXid), limit);
		retention_owner_begin = true;
		LWLockAcquire(&relmap_runtime_shmem->lock, LW_EXCLUSIVE);
		UT_ASSERT_EQ(cluster_relmap_retention_cancel_prepare(directory, &shared), CLUSTER_RELMAP_RETENTION_OK);
		LWLockRelease(&relmap_runtime_shmem->lock);
		retention_owner_begin = false;
		UT_ASSERT(!directory->pins[0].occupied);
		(void)retention_call(true, ERRCODE_CLUSTER_XID_AUTHORITY_UNAVAILABLE);
		UT_ASSERT_EQ(extensions, before_extensions);
		UT_ASSERT_EQ(memcmp(&remaining, &directory->pins[1], sizeof(remaining)), 0);
		UT_ASSERT_EQ(caller_owner_lock_errors, 0);
	}
}

UT_TEST(original_owner_refusal_keeps_existing_native_pins)
{
	for (unsigned which = 0; which < 6; which++) {
		ClusterRelmapOwnerSampleV2 sample, refused, zero = { 0 };
		ClusterRelmapRetentionDirectory before;
		ClusterRelmapRetentionResult result;
		bool captured;

		caller_fixture((UINT64CONST(7) << 32) + 2052, 0);
		captured = cluster_relmap_owner_capture_v2(&sample);
		UT_ASSERT(captured);
		if (!captured) return;
		UT_ASSERT_EQ(caller_begin(&sample, true), CLUSTER_RELMAP_RETENTION_OK);
		before = relmap_runtime_shmem->directories[0];
		switch (which) {
		case 0: caller_global_x = false; break;
		case 1: caller_transaction = false; break;
		case 2: MyBackendType = B_STARTUP; break;
		case 3: caller_writer_ready = false; break;
		case 4: caller_admitted++; break;
		case 5: caller_binding_drift = true; break;
		}
		if (which < 5) {
			memset(&refused, 0xa5, sizeof(refused));
			UT_ASSERT(!cluster_relmap_owner_capture_v2(&refused));
			UT_ASSERT_EQ(memcmp(&refused, &zero, sizeof(zero)), 0);
		}
		result = caller_begin(&sample, false);
		UT_ASSERT_EQ(result, CLUSTER_RELMAP_RETENTION_UNAVAILABLE);
		UT_ASSERT_EQ(memcmp(&before, &relmap_runtime_shmem->directories[0], sizeof(before)), 0);
		UT_ASSERT_EQ(caller_assignments, 1);
		UT_ASSERT_EQ(U64FromFullTransactionId(variables.nextXid), (UINT64CONST(7) << 32) + 2053);
		UT_ASSERT_EQ(caller_owner_lock_errors, 0);
	}
}
