/* PGRAC: original recovery advancement and the retained native next check.
 * Role, locks, system/DB, initial COMPLETE and ERROR providers are controlled.
 * This fixture does not qualify native source/history/lifetime or admission.
 * Author: SqlRush <sqlrush@gmail.com>
 * Spec: 2026-10-14-pre2-m5-e1-format-closure-v1.md */
#include "postgres.h"
#include "access/transam.h"
#include "access/xlog.h"
#include "cluster/cluster_guc.h"
#include "cluster/cluster_relmap_retention_runtime.h"
#include "miscadmin.h"
#include "postmaster/interrupt.h"
#include "storage/lwlock.h"
#undef printf
#include "unit_test.h"
UT_DEFINE_GLOBALS();

bool cluster_enabled, cluster_shared_config, IsUnderPostmaster;
int cluster_node_id;
AuxProcType MyAuxProcType;
volatile uint32 InterruptHoldoffCount;
sigjmp_buf *PG_exception_stack;
ErrorContextCallback *error_context_stack;
static VariableCacheData variables;
VariableCache ShmemVariableCache = &variables;
static LWLockPadded locks[NUM_INDIVIDUAL_LWLOCKS];
LWLockPadded *MainLWLockArray = locks;
static bool xid_held, retention_held, error_expected;
static unsigned lock_errors, retention_acquires, error_code;
static uint64 native_sysid;

#include "../../backend/cluster/cluster_relmap_retention.c"
#include "test_cluster_xid_recovery_runtime.inc"

uint64
GetSystemIdentifier(void)
{
	if (retention_held) lock_errors++;
	return native_sysid;
}

bool
LWLockAcquire(LWLock *lock, LWLockMode mode)
{
	if (mode != LW_EXCLUSIVE) lock_errors++;
	if (lock == XidGenLock) {
		if (xid_held || retention_held) lock_errors++;
		xid_held = true;
	} else {
		if (relmap_runtime_shmem == NULL || lock != &relmap_runtime_shmem->lock
			|| !xid_held || retention_held) lock_errors++;
		retention_held = true;
		retention_acquires++;
	}
	HOLD_INTERRUPTS();
	return false;
}

void
LWLockRelease(LWLock *lock)
{
	if (lock == XidGenLock) {
		if (!xid_held || retention_held) lock_errors++;
		xid_held = false;
	} else {
		if (relmap_runtime_shmem == NULL || lock != &relmap_runtime_shmem->lock
			|| !retention_held || !xid_held) lock_errors++;
		retention_held = false;
	}
	RESUME_INTERRUPTS();
}

bool
LWLockHeldByMe(LWLock *lock)
{
	return (lock == XidGenLock && xid_held)
		|| (relmap_runtime_shmem != NULL && lock == &relmap_runtime_shmem->lock && retention_held);
}

bool
LWLockHeldByMeInMode(LWLock *lock, LWLockMode mode)
{
	return mode == LW_EXCLUSIVE && LWLockHeldByMe(lock);
}

void
ExceptionalCondition(const char *condition, const char *file, int line)
{
	(void)condition; (void)file; (void)line;
	abort();
}

#undef errstart
#undef errstart_cold
bool
errstart(int level, const char *domain)
{
	(void)domain;
	if (!error_expected || level < ERROR) abort();
	return true;
}
bool
errstart_cold(int level, const char *domain)
{
	return errstart(level, domain);
}
int
errcode(int code)
{
	error_code = code;
	return 0;
}
int
errmsg(const char *format,...)
{
	(void)format;
	return 0;
}
void
errfinish(const char *file, int line, const char *function)
{
	(void)file; (void)line; (void)function;
	InterruptHoldoffCount = 0;
	pg_re_throw();
}
void
pg_re_throw(void)
{
	if (PG_exception_stack != NULL) siglongjmp(*PG_exception_stack, 1);
	abort();
}

/* The whole native functions are selected and counted by the Make rule. */
void cluster_relmap_retention_check_recovered_next_v2(uint64 system_identifier,
	uint64 database_incarnation, uint16 origin);
#ifndef CLUSTER_XID_RECOVERY_NATIVE_BODY
#define CLUSTER_XID_RECOVERY_NATIVE_BODY "test_cluster_xid_recovery_native.inc"
#endif
#include CLUSTER_XID_RECOVERY_NATIVE_BODY

static void
fixture(uint64 next, uint64 pin, unsigned origin, bool shared)
{
	ClusterRelmapRetentionDirectory *directory;
	ClusterRelmapRetentionKey key = { 0 };
	uint64 issued = next - ((uint32)next == FirstNormalTransactionId ? 4 : 1);

	free(relmap_runtime_shmem);
	relmap_runtime_shmem = calloc(1, sizeof(*relmap_runtime_shmem));
	UT_ASSERT(relmap_runtime_shmem != NULL);
	memset(&variables, 0, sizeof(variables));
	variables.nextXid = FullTransactionIdFromU64(next);
	ShmemVariableCache = &variables;
	cluster_enabled = cluster_shared_config = IsUnderPostmaster = true;
	cluster_node_id = origin;
	MyAuxProcType = StartupProcess;
	native_sysid = 11;
	xid_held = retention_held = error_expected = false;
	InterruptHoldoffCount = 0;
	lock_errors = retention_acquires = error_code = 0;
	directory = &relmap_runtime_shmem->directories[origin];
	UT_ASSERT_EQ(cluster_relmap_retention_build_start(directory, 11, 22, origin,
		9, pin != 0 ? pin : next - 100, issued), CLUSTER_RELMAP_RETENTION_OK);
	if (pin != 0) {
		key.identity.system_identifier = 11;
		key.identity.database_incarnation = 22;
		key.identity.dbid = shared ? InvalidOid : 42;
		key.identity.shared = shared;
		key.generation = 7;
		key.origin = origin;
		key.top_full_xid = pin;
		key.membership_epoch = key.admitted_incarnation = key.boot_incarnation = 1;
		UT_ASSERT_EQ(cluster_relmap_retention_build_add(directory, &key, 99), CLUSTER_RELMAP_RETENTION_OK);
	}
	UT_ASSERT_EQ(cluster_relmap_retention_build_finish(directory), CLUSTER_RELMAP_RETENTION_OK);
}

/* Each call retains exactly the incoming native lock and never changes either
 * the imported native state or the complete original runtime, including pins. */
static void
check(unsigned origin, unsigned expected)
{
	VariableCacheData before = variables;
	RelmapRetentionRuntimeShmem saved = *relmap_runtime_shmem;
	bool incoming_xid = xid_held;
	bool incoming_retention = retention_held;
	volatile bool caught = false;

	error_expected = expected != 0;
	error_code = 0;
	PG_TRY();
	{
		cluster_relmap_retention_check_recovered_next_v2(11, 22, origin);
	}
	PG_CATCH();
	{
		caught = true;
		/* Only restore holdoff accounting for the original incoming locks. */
		if (incoming_xid) HOLD_INTERRUPTS();
		if (retention_held) HOLD_INTERRUPTS();
	}
	PG_END_TRY();
	UT_ASSERT_EQ(caught, expected != 0);
	UT_ASSERT_EQ(error_code, expected);
	UT_ASSERT_EQ(xid_held, incoming_xid);
	UT_ASSERT_EQ(retention_held, incoming_retention);
	UT_ASSERT(memcmp(&before, &variables, sizeof(before)) == 0);
	UT_ASSERT(memcmp(&saved, relmap_runtime_shmem, sizeof(saved)) == 0);
	UT_ASSERT_EQ(lock_errors, 0);
	error_expected = false;
}

UT_TEST(original_recovery_import_is_checked_without_issuance)
{
	for (unsigned i = 0; i < 3; i++) {
		uint64 next = i == 2 ? (UINT64CONST(8) << 32) + 3 : ((uint64)i * 7 << 32) + 6000;
		unsigned origin = i % 2 ? CLUSTER_MAX_NODES - 1 : 0;

		fixture(next, next - 100, origin, i % 2);
		variables.nextXid = FullTransactionIdFromU64(next - (i == 2 ? 5 : 10));
		AdvanceNextFullTransactionIdPastXid((uint32)(next - (i == 2 ? 4 : 1)));
		UT_ASSERT_EQ(U64FromFullTransactionId(variables.nextXid), next);
		UT_ASSERT_EQ(variables.clusterIssuedState, 0);
		UT_ASSERT_EQ(U64FromFullTransactionId(variables.clusterLastIssuedXid), 0);
		LWLockAcquire(XidGenLock, LW_EXCLUSIVE);
		check(origin, 0);
		UT_ASSERT_EQ(retention_acquires, 1);
		LWLockRelease(XidGenLock);
		UT_ASSERT_EQ(InterruptHoldoffCount, 0);
	}
}

UT_TEST(imported_halfspace_equality_and_overflow_preserve_native_next)
{
	for (unsigned ns = 0; ns < 2; ns++) {
		for (unsigned era = 0; era < 2; era++) {
			uint64 pin = ((uint64)era * 7 << 32) + 4099;
			unsigned origin = era ? CLUSTER_MAX_NODES - 1 : 0;

			for (unsigned delta = 0; delta < 3; delta++) {
				uint64 next = pin + UINT64CONST(0x80000000) - 1 + delta;

				fixture(pin + 100, pin, origin, ns);
				AdvanceNextFullTransactionIdPastXid((uint32)(next - 1));
				UT_ASSERT_EQ(U64FromFullTransactionId(variables.nextXid), next);
				LWLockAcquire(XidGenLock, LW_EXCLUSIVE);
				check(origin, delta == 0 ? 0 : ERRCODE_CLUSTER_MERGED_RECOVERY_BLOCKED);
				LWLockRelease(XidGenLock);
			}
		}
	}
	/* Such a pin cannot pass build_add. Inject contradictory published bytes
	 * solely to ensure arithmetic overflow also fails closed on recheck. */
	fixture(6000, 5900, 0, false);
	relmap_runtime_shmem->directories[0].pins[0].key.top_full_xid = UINT64_MAX - 100;
	relmap_runtime_shmem->directories[0].last_issued = UINT64_MAX - 11;
	variables.nextXid = FullTransactionIdFromU64(UINT64_MAX - 10);
	LWLockAcquire(XidGenLock, LW_EXCLUSIVE);
	check(0, ERRCODE_CLUSTER_MERGED_RECOVERY_BLOCKED);
	LWLockRelease(XidGenLock);
}

UT_TEST(unqualified_or_inconsistent_recovered_inputs_never_open_allocation)
{
	for (unsigned fault = 0; fault < 10; fault++) {
		ClusterRelmapRetentionDirectory *directory;

		fixture(6000, 5900, 0, false);
		directory = &relmap_runtime_shmem->directories[0];
		switch (fault) {
			case 0: directory->state = CLUSTER_RELMAP_RETENTION_UNKNOWN; break;
			case 1: directory->state = CLUSTER_RELMAP_RETENTION_BUILDING; break;
			case 2: directory->system_identifier++; break;
			case 3: directory->database_incarnation++; break;
			case 4: directory->origin++; break;
			case 5: variables.nextXid = FullTransactionIdFromU64(2); break;
			case 6: variables.nextXid = FullTransactionIdFromU64(UINT64_MAX); break;
			case 7: directory->native_floor = 6001; break;
			case 8: directory->last_issued = 6000; break;
			case 9: native_sysid++; break;
		}
		LWLockAcquire(XidGenLock, LW_EXCLUSIVE);
		check(0, ERRCODE_CLUSTER_MERGED_RECOVERY_BLOCKED);
		LWLockRelease(XidGenLock);
		UT_ASSERT_EQ(InterruptHoldoffCount, 0);
	}
}

UT_TEST(original_startup_role_lock_and_current_import_are_required)
{
	for (unsigned fault = 0; fault < 7; fault++) {
		fixture(6000, 5900, 0, false);
		if (fault != 0) LWLockAcquire(XidGenLock, LW_EXCLUSIVE);
		switch (fault) {
			case 0: break;
			case 1: MyAuxProcType = CheckpointerProcess; break;
			case 2: cluster_node_id = 1; break;
			case 3: cluster_shared_config = false; break;
			case 4: cluster_enabled = false; break;
			case 5: ShmemVariableCache = NULL; break;
			case 6: LWLockAcquire(&relmap_runtime_shmem->lock, LW_EXCLUSIVE); break;
		}
		check(0, ERRCODE_CLUSTER_MERGED_RECOVERY_BLOCKED);
		if (fault == 6) LWLockRelease(&relmap_runtime_shmem->lock);
		if (xid_held) LWLockRelease(XidGenLock);
		UT_ASSERT_EQ(InterruptHoldoffCount, 0);
	}
	fixture(6000, 5900, 0, false);
	LWLockAcquire(XidGenLock, LW_EXCLUSIVE);
	check(0, 0);
	LWLockRelease(XidGenLock);
	/* A successful snapshot grants no admission and cannot mask later replay. */
	AdvanceNextFullTransactionIdPastXid(5900 + 0x80000000U - 1);
	LWLockAcquire(XidGenLock, LW_EXCLUSIVE);
	check(0, ERRCODE_CLUSTER_MERGED_RECOVERY_BLOCKED);
	LWLockRelease(XidGenLock);
}

int
main(void)
{
	UT_PLAN(4);
	UT_RUN(original_recovery_import_is_checked_without_issuance);
	UT_RUN(imported_halfspace_equality_and_overflow_preserve_native_next);
	UT_RUN(unqualified_or_inconsistent_recovered_inputs_never_open_allocation);
	UT_RUN(original_startup_role_lock_and_current_import_are_required);
	free(relmap_runtime_shmem);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
