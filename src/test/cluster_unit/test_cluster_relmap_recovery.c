/* Actual native publication tracker and bounds capture. The original owner
 * guard is a leaf provider; no claim of a complete recovery certificate. */
#include "postgres.h"
#include "access/transam.h"
#include "access/clog.h"
#include "cluster/cluster_relmap_owner.h"
#include "cluster/cluster_relmap_recovery.h"
#include "storage/lwlock.h"
#include "miscadmin.h"
#include "unit_test.h"
UT_DEFINE_GLOBALS();
static VariableCacheData variables;
VariableCache ShmemVariableCache = &variables;
static LWLockPadded locks[NUM_INDIVIDUAL_LWLOCKS];
LWLockPadded *MainLWLockArray = locks;
static bool owner_current, issuing;
static unsigned depth, checks, change_check, caller_locks;
static ClusterRelmapOwnerSampleV2 original;
static XidStatus native_status;
static XLogRecPtr native_lsn;
static unsigned status_reads, interrupt_errors;
volatile uint32 InterruptHoldoffCount;
static bool status_error, invalidate_on_read;
sigjmp_buf *PG_exception_stack;
ErrorContextCallback *error_context_stack;

void
pg_re_throw(void)
{
	siglongjmp(*PG_exception_stack, 1);
}
static TransactionId expected_status_xid;

XidStatus
TransactionIdGetStatus(TransactionId xid, XLogRecPtr *lsn)
{
	if (depth != 1 || xid != expected_status_xid)
		abort();
	++status_reads;
	if (status_error) {
		/* Native errfinish(ERROR) resets this before PG_RE_THROW. */
		InterruptHoldoffCount = 0;
		siglongjmp(*PG_exception_stack, 1);
	}
	if (invalidate_on_read)
		variables.clusterIssuedState = 2;
	*lsn = native_lsn;
	return native_status;
}


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
		   || (lock == XactTruncationLock && (caller_locks & 2))
		   || (lock == XactSLRULock && (caller_locks & 4))
		   || (lock == XactTruncationLock && depth >= 1) || (lock == XidGenLock && depth == 2);
}
bool
LWLockAcquire(LWLock *lock, LWLockMode mode)
{
	if (mode != LW_SHARED || (depth == 0 && lock != XactTruncationLock)
		|| (depth == 1 && lock != XidGenLock) || depth >= 2)
		abort();
	++depth;
	HOLD_INTERRUPTS();
	return true;
}
void
LWLockRelease(LWLock *lock)
{
	if ((depth == 2 && lock != XidGenLock) || (depth == 1 && lock != XactTruncationLock)
		|| depth == 0)
		abort();
	--depth;
	if (InterruptHoldoffCount == 0)
		++interrupt_errors;
	else
		RESUME_INTERRUPTS();
}
#ifndef CLUSTER_RELMAP_RECOVERY_SOURCE
#define CLUSTER_RELMAP_RECOVERY_SOURCE "../../backend/cluster/cluster_relmap_recovery.c"
#endif
#include CLUSTER_RELMAP_RECOVERY_SOURCE

static void
setup(void)
{
	memset(&variables, 0, sizeof(variables));
	memset(&original, 0, sizeof(original));
	owner_current = true;
	native_status = TRANSACTION_STATUS_COMMITTED;
	native_lsn = 0;
	status_reads = interrupt_errors = 0;
	InterruptHoldoffCount = 0;
	status_error = invalidate_on_read = false;
	expected_status_xid = 100;
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
static void
capture_native_terminal_and_zero_lsn(void)
{
	for (unsigned outcome = 0; outcome < 3; ++outcome) {
		ClusterRelmapRecoveryNativeTerminal out;
		setup();
		publish(UINT64CONST(0x200000068));
		native_status = outcome == 1 ? TRANSACTION_STATUS_ABORTED : TRANSACTION_STATUS_COMMITTED;
		native_lsn = outcome == 0 ? 1234 : 0;
		UT_ASSERT(cluster_relmap_recovery_native_terminal_capture(&original, &out));
		UT_ASSERT_EQ(out.top_full_xid, original.owner.top_full_xid);
		UT_ASSERT_EQ(out.status, native_status);
		UT_ASSERT_EQ(out.status_lsn, native_lsn);
		UT_ASSERT_EQ(out.bounds.native_floor, UINT64CONST(0x20000005a));
		UT_ASSERT_EQ(out.bounds.last_issued, UINT64CONST(0x200000068));
		UT_ASSERT_EQ(out.bounds.system_identifier, 123);
		UT_ASSERT_EQ(out.bounds.database_incarnation, 456);
		UT_ASSERT_EQ(out.bounds.origin, 3);
		UT_ASSERT_EQ(out.bounds.membership_epoch, 7);
		UT_ASSERT_EQ(out.bounds.admitted_incarnation, 8);
		UT_ASSERT_EQ(out.bounds.boot_incarnation, 55);
		UT_ASSERT_EQ(status_reads, 1);
		UT_ASSERT_EQ(checks, 2);
		UT_ASSERT_EQ(depth, 0);
	}
}
static void
refuses_nonterminal_and_invalid_native_window(void)
{
	for (unsigned cut = 0; cut < 8; ++cut) {
		ClusterRelmapRecoveryNativeTerminal out, zero = { 0 };
		setup();
		if (cut != 2)
			publish(UINT64CONST(0x200000068));
		switch (cut) {
		case 0:
			native_status = TRANSACTION_STATUS_IN_PROGRESS;
			break;
		case 1:
			native_status = TRANSACTION_STATUS_SUB_COMMITTED;
			break;
		case 2:
			break; /* Unknown actual issuance, even with a legal future limit. */
		case 3:
			variables.oldestClogXid = 101;
			break;
		case 4:
			original.owner.top_full_xid = UINT64CONST(0x20000006a);
			break;
		case 5:
			invalidate_on_read = true;
			break;
		case 6:
			owner_current = false;
			break;
		case 7:
			original.owner.top_full_xid = UINT64CONST(0x200000001);
			break;
		}
		memset(&out, 0xa5, sizeof(out));
		UT_ASSERT(!cluster_relmap_recovery_native_terminal_capture(&original, &out));
		UT_ASSERT(memcmp(&out, &zero, sizeof(out)) == 0);
		UT_ASSERT_EQ(status_reads, cut < 2 || cut == 5 ? 1 : 0);
		UT_ASSERT_EQ(depth, 0);
	}
}
static void
native_terminal_rechecks_owner_before_publishing(void)
{
	ClusterRelmapRecoveryNativeTerminal out, zero = { 0 };
	setup();
	publish(UINT64CONST(0x200000068));
	change_check = 2;
	memset(&out, 0xa5, sizeof(out));
	UT_ASSERT(!cluster_relmap_recovery_native_terminal_capture(&original, &out));
	UT_ASSERT(memcmp(&out, &zero, sizeof(out)) == 0);
	UT_ASSERT_EQ(status_reads, 1);
	UT_ASSERT_EQ(depth, 0);
}
static void
native_terminal_preserves_alias_and_caller_locks(void)
{
	ClusterRelmapOwnerSampleV2 before;
	ClusterRelmapRecoveryNativeTerminal out, zero = { 0 };
	setup();
	publish(UINT64CONST(0x200000068));
	before = original;
	UT_ASSERT(!cluster_relmap_recovery_native_terminal_capture(
		&original, (ClusterRelmapRecoveryNativeTerminal *)&original));
	UT_ASSERT(memcmp(&original, &before, sizeof(before)) == 0);
	UT_ASSERT_EQ(checks, 0);
	for (unsigned mask = 1; mask < 8; ++mask) {
		caller_locks = mask;
		memset(&out, 0xa5, sizeof(out));
		UT_ASSERT(!cluster_relmap_recovery_native_terminal_capture(&original, &out));
		UT_ASSERT(memcmp(&out, &zero, sizeof(out)) == 0);
		UT_ASSERT_EQ(caller_locks, mask);
		UT_ASSERT_EQ(status_reads, 0);
		UT_ASSERT_EQ(checks, 0);
		UT_ASSERT_EQ(depth, 0);
	}
}
static void
native_terminal_error_keeps_output_empty_and_rethrows(void)
{
	ClusterRelmapRecoveryNativeTerminal out, zero = { 0 };
	volatile bool caught = false;
	setup();
	publish(UINT64CONST(0x200000068));
	status_error = true;
	memset(&out, 0xa5, sizeof(out));
	PG_TRY();
	{
		(void)cluster_relmap_recovery_native_terminal_capture(&original, &out);
	}
	PG_CATCH();
	{
		caught = true;
	}
	PG_END_TRY();
	UT_ASSERT(caught);
	UT_ASSERT(memcmp(&out, &zero, sizeof(out)) == 0);
	UT_ASSERT_EQ(depth, 0);
	UT_ASSERT_EQ(status_reads, 1);
	UT_ASSERT_EQ(interrupt_errors, 0);
	UT_ASSERT_EQ(InterruptHoldoffCount, 0);
}
int
main(void)
{
	UT_PLAN(11);
	UT_RUN(capture_actual_published_window);
	UT_RUN(rejects_unknown_rewind_and_ambiguous_era);
	UT_RUN(invalidation_stays_unknown);
	UT_RUN(input_alias_is_unchanged);
	UT_RUN(refuses_caller_native_locks_without_queries_or_unlock);
	UT_RUN(native_floor_epoch_transition);
	UT_RUN(capture_native_terminal_and_zero_lsn);
	UT_RUN(refuses_nonterminal_and_invalid_native_window);
	UT_RUN(native_terminal_rechecks_owner_before_publishing);
	UT_RUN(native_terminal_preserves_alias_and_caller_locks);
	UT_RUN(native_terminal_error_keeps_output_empty_and_rethrows);
	UT_DONE();
	return ut_failed_count ? 1 : 0;
}
