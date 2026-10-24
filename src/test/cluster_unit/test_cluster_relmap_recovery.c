/* Actual native publication tracker and bounds capture. The original owner
 * guard is a leaf provider; no claim of a complete recovery certificate. */
#include "postgres.h"
#include "access/transam.h"
#include "cluster/cluster_relmap_owner.h"
#include "cluster/cluster_relmap_recovery.h"
#include "storage/lwlock.h"
#include "unit_test.h"
UT_DEFINE_GLOBALS();
static VariableCacheData variables;
VariableCache ShmemVariableCache = &variables;
static LWLockPadded locks[NUM_INDIVIDUAL_LWLOCKS];
LWLockPadded *MainLWLockArray = locks;
static bool owner_current, issuing;
static unsigned depth, checks, change_check, caller_locks;
static ClusterRelmapOwnerSampleV2 original;

bool
cluster_relmap_owner_terminal_matches_v2(const ClusterRelmapOwnerSampleV2 *owner)
{
	return owner == &original && owner_current && ++checks != change_check;
}
bool
LWLockHeldByMeInMode(LWLock *lock, LWLockMode mode)
{
	return lock == XidGenLock && mode == LW_EXCLUSIVE && issuing;
}
bool
LWLockHeldByMe(LWLock *lock)
{
	return (lock == XidGenLock && (caller_locks & 1))
		   || (lock == XactTruncationLock && (caller_locks & 2));
}
bool
LWLockAcquire(LWLock *lock, LWLockMode mode)
{
	if (mode != LW_SHARED || (depth == 0 && lock != XactTruncationLock)
		|| (depth == 1 && lock != XidGenLock) || depth >= 2)
		abort();
	++depth;
	return true;
}
void
LWLockRelease(LWLock *lock)
{
	if ((depth == 2 && lock != XidGenLock) || (depth == 1 && lock != XactTruncationLock)
		|| depth == 0)
		abort();
	--depth;
}
#define FEATURE_ISSUANCE_SOURCE_ONLY
#include "../../backend/cluster/cluster_relmap_recovery.c"

static void
setup(void)
{
	memset(&variables, 0, sizeof(variables));
	memset(&original, 0, sizeof(original));
	owner_current = true;
	depth = checks = change_check = caller_locks = 0;
	original.owner.node = 3;
	original.owner.top_full_xid = UINT64CONST(0x200000064);
	original.owner.membership_epoch = 7;
	original.owner.admitted_incarnation = 8;
	original.owner.boot_incarnation = 55;
	original.writer.ref.claim.identity.system_identifier = 123;
	original.writer.ref.claim.identity.origin_node_id = 3;
	original.writer.ref.claim.identity.origin_owner_incarnation = 8;
	original.writer.ref.claim.database_incarnation = 456;
	original.writer.epoch = 7;
	variables.nextXid = FullTransactionIdFromU64(UINT64CONST(0x200000069));
	variables.oldestClogXid = 90;
}
static void
publish(uint64 full)
{
	issuing = true;
	cluster_relmap_recovery_issued_publish(FullTransactionIdFromU64(full));
	issuing = false;
}
static void
capture_actual_published_window(void)
{
	ClusterRelmapRecoveryNativeBounds out;
	setup();
	publish(UINT64CONST(0x200000068));
	UT_ASSERT(cluster_relmap_recovery_native_bounds(&original, &out));
	UT_ASSERT_EQ(out.native_floor, UINT64CONST(0x20000005a));
	UT_ASSERT_EQ(out.last_issued, UINT64CONST(0x200000068));
	UT_ASSERT_EQ(out.origin, 3);
	UT_ASSERT_EQ(out.admitted_incarnation, 8);
	UT_ASSERT_EQ(out.boot_incarnation, 55);
	UT_ASSERT_EQ(out.database_incarnation, 456);
	/* A legal future counter is never relabeled as an actual issuance. */
	variables.nextXid = FullTransactionIdFromU64(UINT64CONST(0x200001000));
	UT_ASSERT(cluster_relmap_recovery_native_bounds(&original, &out));
	UT_ASSERT_EQ(out.last_issued, UINT64CONST(0x200000068));
	UT_ASSERT_EQ(depth, 0);
}
static void
rejects_unknown_rewind_and_ambiguous_era(void)
{
	for (unsigned fault = 0; fault < 13; ++fault) {
		ClusterRelmapRecoveryNativeBounds out, zero = { 0 };
		setup();
		if (fault != 0)
			publish(UINT64CONST(0x200000068));
		switch (fault) {
		case 0:
			break; /* initialized/replayed nextXid alone */
		case 1:
			owner_current = false;
			break;
		case 2:
			variables.oldestClogXid = 0;
			break;
		case 3:
			variables.oldestClogXid = 0x80000068u;
			break;
		case 4:
			variables.oldestClogXid = 110;
			break;
		case 5:
			variables.nextXid = FullTransactionIdFromU64(UINT64CONST(0x200000068));
			break;
		case 6:
			publish(UINT64CONST(0x200000067));
			break;
		case 7:
			publish(UINT64CONST(0x200000068));
			break;
		case 8:
			publish(UINT64CONST(0x200000001));
			break;
		case 9:
			original.owner.top_full_xid = UINT64CONST(0x100000064);
			break;
		case 10:
			original.owner.top_full_xid = UINT64CONST(0x200000069);
			break;
		case 11:
			change_check = 2;
			break;
		case 12:
			original.writer.ref.claim.identity.origin_owner_incarnation = 55;
			break;
		}
		memset(&out, 0xa5, sizeof(out));
		UT_ASSERT(!cluster_relmap_recovery_native_bounds(&original, &out));
		UT_ASSERT(memcmp(&out, &zero, sizeof(out)) == 0);
		UT_ASSERT_EQ(depth, 0);
	}
}
static void
invalidation_stays_unknown(void)
{
	ClusterRelmapRecoveryNativeBounds out;
	setup();
	publish(UINT64CONST(0x200000068));
	publish(UINT64CONST(0x200000067));
	publish(UINT64CONST(0x200000069));
	variables.nextXid = FullTransactionIdFromU64(UINT64CONST(0x20000006a));
	UT_ASSERT(!cluster_relmap_recovery_native_bounds(&original, &out));
	UT_ASSERT_EQ(variables.clusterIssuedState, 2);
}
static void
input_alias_is_unchanged(void)
{
	ClusterRelmapOwnerSampleV2 before;
	setup();
	publish(UINT64CONST(0x200000068));
	before = original;
	UT_ASSERT(!cluster_relmap_recovery_native_bounds(
		&original, (ClusterRelmapRecoveryNativeBounds *)&original));
	UT_ASSERT(memcmp(&original, &before, sizeof(before)) == 0);
	UT_ASSERT_EQ(depth, 0);
}
static void
refuses_caller_native_locks_without_queries_or_unlock(void)
{
	for (unsigned mask = 1; mask < 4; ++mask) {
		ClusterRelmapRecoveryNativeBounds out, zero = { 0 };
		setup();
		publish(UINT64CONST(0x200000068));
		caller_locks = mask;
		memset(&out, 0xa5, sizeof(out));
		UT_ASSERT(!cluster_relmap_recovery_native_bounds(&original, &out));
		UT_ASSERT(memcmp(&out, &zero, sizeof(out)) == 0);
		UT_ASSERT_EQ(caller_locks, mask);
		UT_ASSERT_EQ(depth, 0);
		UT_ASSERT_EQ(checks, 0);
	}
}
static void
native_floor_epoch_transition(void)
{
	ClusterRelmapRecoveryNativeBounds out;
	setup();
	publish(UINT64CONST(0x300000006));
	variables.nextXid = FullTransactionIdFromU64(UINT64CONST(0x300000007));
	variables.oldestClogXid = 0xfffffff0u;
	original.owner.top_full_xid = UINT64CONST(0x300000004);
	UT_ASSERT(cluster_relmap_recovery_native_bounds(&original, &out));
	UT_ASSERT_EQ(out.native_floor, UINT64CONST(0x2fffffff0));
	UT_ASSERT_EQ(out.last_issued, UINT64CONST(0x300000006));
	setup();
	publish(10);
	variables.nextXid = FullTransactionIdFromU64(11);
	variables.oldestClogXid = 0xfffffff0u;
	original.owner.top_full_xid = 5;
	UT_ASSERT(!cluster_relmap_recovery_native_bounds(&original, &out));
	UT_ASSERT_EQ(depth, 0);
}
int
main(void)
{
	UT_PLAN(6);
	UT_RUN(capture_actual_published_window);
	UT_RUN(rejects_unknown_rewind_and_ambiguous_era);
	UT_RUN(invalidation_stays_unknown);
	UT_RUN(input_alias_is_unchanged);
	UT_RUN(refuses_caller_native_locks_without_queries_or_unlock);
	UT_RUN(native_floor_epoch_transition);
	UT_DONE();
	return ut_failed_count ? 1 : 0;
}
