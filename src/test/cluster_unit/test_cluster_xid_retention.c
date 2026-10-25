/* PGRAC: original allocator composed with the original retention guard.
 * COMPLETE, native authority, SLRU, locks and ERROR are controlled fixtures.
 * Author: SqlRush <sqlrush@gmail.com>
 * Spec: 2026-10-14-pre2-m5-e1-format-closure-v1.md */
#include "postgres.h"
#include "cluster/cluster_relmap_retention_runtime.h"
#include "postmaster/interrupt.h"
#include "storage/lwlock.h"

static bool retention_native_lock_acquire(LWLock *lock, LWLockMode mode);
static void retention_native_lock_release(LWLock *lock);
static bool retention_native_lock_held(LWLock *lock, LWLockMode mode);
static void retention_native_extended(void);
static void retention_native_published(FullTransactionId issued);
static bool retention_error_expected;
static bool retention_wrap_ready = true;
int xid_allocation_original_main(void);

#define CLUSTER_XID_RETENTION_TEST
#define main xid_allocation_original_main
#include "test_cluster_xid_allocation.c"
#undef main
#include "../../backend/cluster/cluster_relmap_retention.c"
#include "test_cluster_xid_retention_runtime.inc"

static bool retention_held, retention_active, retention_recompute;
static unsigned retention_acquires, retention_final_observations, retention_fault;
static unsigned retention_error_code, retention_origin;
static uint64 retention_sysid, retention_dbinc;

static bool
retention_native_lock_acquire(LWLock *lock, LWLockMode mode)
{
	if (lock == XidGenLock) {
		if (held || retention_held || mode != LW_EXCLUSIVE) lock_errors++;
		held = true;
	} else {
		if (relmap_runtime_shmem == NULL || lock != &relmap_runtime_shmem->lock
			|| !held || retention_held || mode != LW_EXCLUSIVE) lock_errors++;
		retention_held = true;
		retention_acquires++;
	}
	HOLD_INTERRUPTS();
	return false;
}

static void
retention_native_lock_release(LWLock *lock)
{
	if (lock == XidGenLock) {
		if (!held || retention_held) lock_errors++;
		held = false;
		if (retention_recompute && extensions == 0) {
			variables.nextXid = FullTransactionIdFromU64(2084);
			retention_recompute = false;
			if (retention_fault == 4)
				relmap_runtime_shmem->directories[retention_origin].state = CLUSTER_RELMAP_RETENTION_UNKNOWN;
		}
	} else {
		if (relmap_runtime_shmem == NULL || lock != &relmap_runtime_shmem->lock
			|| !retention_held || !held) lock_errors++;
		retention_held = false;
	}
	RESUME_INTERRUPTS();
}

static bool
retention_native_lock_held(LWLock *lock, LWLockMode mode)
{
	return mode == LW_EXCLUSIVE && ((lock == XidGenLock && held)
		|| (relmap_runtime_shmem != NULL && lock == &relmap_runtime_shmem->lock && retention_held));
}

static void
retention_native_published(FullTransactionId issued)
{
	if (!retention_active) return;
	if (!held || !retention_held || extensions == 0
		|| relmap_runtime_shmem->directories[retention_origin].last_issued != U64FromFullTransactionId(issued))
		native_publish_order_errors++;
	retention_final_observations++;
}

static void
retention_native_extended(void)
{
	ClusterRelmapRetentionDirectory *directory;

	if (!retention_active) return;
	if (retention_held || !held) lock_errors++;
	directory = &relmap_runtime_shmem->directories[retention_origin];
	if (extensions == 1 && retention_fault == 1) {
		directory->pins[0].occupied = true;
		directory->pins[0].key.top_full_xid = U64FromFullTransactionId(variables.nextXid) - UINT64CONST(0x80000000);
	}
	if (extensions == 1 && retention_fault == 2)
		directory->state = CLUSTER_RELMAP_RETENTION_UNKNOWN;
	if (retention_fault == 3)
		ereport(ERROR, (errcode(ERRCODE_DISK_FULL), errmsg("controlled SLRU failure")));
}

#undef errstart
#undef errstart_cold
bool
errstart(int level, const char *domain)
{
	(void)domain;
	if (!retention_error_expected || level < ERROR) abort();
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
	retention_error_code = code;
	return 0;
}
int
errmsg(const char *format,...)
{
	(void)format;
	return 0;
}
int
errdetail(const char *format,...)
{
	(void)format;
	return 0;
}
int
errhint(const char *format,...)
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

static void
retention_fixture(uint64 candidate, unsigned origin)
{
	uint64 native_floor = (uint32)candidate >= 0x80000000U
		? candidate - UINT64CONST(0x80000000) : candidate - 100;

	free(relmap_runtime_shmem);
	relmap_runtime_shmem = NULL;
	reset_allocator();
	retention_active = true;
	retention_held = retention_recompute = retention_error_expected = false;
	retention_wrap_ready = true;
	retention_acquires = retention_final_observations = retention_fault = retention_error_code = 0;
	InterruptHoldoffCount = 0;
	IsUnderPostmaster = false;
	Mode = NormalProcessing;
	cluster_shared_catalog = cluster_xid_striping = false;
	variables.nextXid = FullTransactionIdFromU64(candidate);
	/* Keep the fixture's ordinary native wrap limits ahead in raw-XID order.
	 * Individual StopLimit/recompute cases override only the gate they test. */
	variables.xidVacLimit = (uint32)candidate + 4096;
	variables.xidWarnLimit = (uint32)candidate + 8192;
	retention_origin = origin;
	retention_sysid = 11;
	retention_dbinc = 22;
	relmap_runtime_shmem = calloc(1, sizeof(*relmap_runtime_shmem));
	UT_ASSERT(relmap_runtime_shmem != NULL);
	UT_ASSERT_EQ(cluster_relmap_retention_build_start(&relmap_runtime_shmem->directories[origin],
		11, 22, origin, 9, native_floor, candidate - 1), CLUSTER_RELMAP_RETENTION_OK);
	UT_ASSERT_EQ(cluster_relmap_retention_build_finish(&relmap_runtime_shmem->directories[origin]),
		CLUSTER_RELMAP_RETENTION_OK);
}

static FullTransactionId
retention_call(bool child, unsigned expected_error)
{
	FullTransactionId result = InvalidFullTransactionId;
	volatile bool caught = false;

	retention_error_expected = expected_error != 0;
	PG_TRY();
	{
		result = cluster_relmap_retention_allocate_xid_v2(child, retention_sysid, retention_dbinc, retention_origin);
	}
	PG_CATCH();
	{
		caught = true;
	}
	PG_END_TRY();
	UT_ASSERT_EQ(caught, expected_error != 0);
	UT_ASSERT(!retention_held);
	UT_ASSERT_EQ(retention_error_code, expected_error);
	if (held) {
		UT_ASSERT(caught);
		HOLD_INTERRUPTS(); /* Simulate only original native lock cleanup. */
		LWLockRelease(XidGenLock);
	}
	UT_ASSERT_EQ(InterruptHoldoffCount, 0);
	UT_ASSERT_EQ(lock_errors + native_publish_order_errors, 0);
	retention_error_expected = false;
	return result;
}

UT_TEST(retention_final_callback_publishes_real_top_and_subxid)
{
	for (unsigned i = 0; i < 4; i++) {
		uint64 first = ((i / 2 ? UINT64CONST(7) : 0) << 32) + 2052;
		unsigned origin = i % 2 ? CLUSTER_MAX_NODES - 1 : 0;

		retention_fixture(first, origin);
		UT_ASSERT_EQ(U64FromFullTransactionId(retention_call(false, 0)), first);
		UT_ASSERT_EQ(proc.xid, 2052);
		UT_ASSERT_EQ(U64FromFullTransactionId(retention_call(true, 0)), first + 1);
		UT_ASSERT_EQ(proc.subxids.xids[0], 2053);
		UT_ASSERT_EQ(subxids[0].count, 1);
		UT_ASSERT_EQ(U64FromFullTransactionId(variables.nextXid), first + 2);
		UT_ASSERT_EQ(U64FromFullTransactionId(variables.clusterLastIssuedXid), first + 1);
		UT_ASSERT_EQ(relmap_runtime_shmem->directories[origin].last_issued, first + 1);
		UT_ASSERT_EQ(retention_final_observations, 2);
		UT_ASSERT_EQ(retention_acquires, 4);
	}
}

UT_TEST(retention_unknown_building_and_wrong_identity_cannot_issue)
{
	for (unsigned which = 0; which < 7; which++) {
		retention_fixture(2052, 0);
		if (which < 2) relmap_runtime_shmem->directories[0].state = which ? CLUSTER_RELMAP_RETENTION_BUILDING : CLUSTER_RELMAP_RETENTION_UNKNOWN;
		if (which == 2) retention_sysid++;
		if (which == 3) retention_dbinc++;
		if (which == 4) retention_origin = CLUSTER_MAX_NODES;
		if (which == 5) Mode = BootstrapProcessing;
		if (which == 6) { free(relmap_runtime_shmem); relmap_runtime_shmem = NULL; }
		(void)retention_call(false, ERRCODE_CLUSTER_XID_AUTHORITY_UNAVAILABLE);
		UT_ASSERT_EQ(extensions + native_publish_count + proc.xid + visible_xids[0], 0);
		UT_ASSERT_EQ(U64FromFullTransactionId(variables.nextXid), 2052);
		UT_ASSERT_EQ(variables.clusterIssuedState, 0);
	}
}

UT_TEST(retention_halfspace_boundary_refuses_without_mutating_native_state)
{
	uint64 candidate = (UINT64CONST(7) << 32) + UINT64CONST(0x80000804);

	retention_fixture(candidate, 0);
	relmap_runtime_shmem->directories[0].pins[0].occupied = true;
	relmap_runtime_shmem->directories[0].pins[0].key.top_full_xid = (UINT64CONST(7) << 32) + 2052;
	(void)retention_call(false, ERRCODE_CLUSTER_XID_AUTHORITY_UNAVAILABLE);
	UT_ASSERT_EQ(extensions + native_publish_count, 0);
	UT_ASSERT_EQ(relmap_runtime_shmem->directories[0].last_issued, candidate - 1);
	UT_ASSERT(relmap_runtime_shmem->directories[0].pins[0].occupied);
	UT_ASSERT_EQ(U64FromFullTransactionId(variables.nextXid), candidate);
}

UT_TEST(retention_rechecks_after_fallible_extensions_before_any_store)
{
	for (unsigned fault = 1; fault <= 2; fault++) {
		uint64 candidate = (UINT64CONST(7) << 32) + UINT64CONST(0x80000804);

		retention_fixture(candidate, 0);
		retention_fault = fault;
		(void)retention_call(false, ERRCODE_CLUSTER_XID_AUTHORITY_UNAVAILABLE);
		UT_ASSERT_EQ(extensions, 3);
		UT_ASSERT_EQ(retention_acquires, 2);
		UT_ASSERT_EQ(native_publish_count + proc.xid + visible_xids[0], 0);
		UT_ASSERT_EQ(relmap_runtime_shmem->directories[0].last_issued, candidate - 1);
		UT_ASSERT_EQ(U64FromFullTransactionId(variables.nextXid), candidate);
	}
}

UT_TEST(retention_slru_error_never_observes_or_publishes_issuance)
{
	retention_fixture(2052, 0);
	retention_fault = 3;
	(void)retention_call(false, ERRCODE_DISK_FULL);
	UT_ASSERT_EQ(extensions, 1);
	UT_ASSERT_EQ(retention_acquires, 1);
	UT_ASSERT_EQ(native_publish_count + proc.xid + visible_xids[0], 0);
	UT_ASSERT_EQ(relmap_runtime_shmem->directories[0].last_issued, 2051);
	UT_ASSERT_EQ(U64FromFullTransactionId(variables.nextXid), 2052);
}

UT_TEST(retention_recomputed_candidate_is_checked_again)
{
	for (unsigned fault = 0; fault <= 4; fault += 4) {
		retention_fixture(2052, 0);
		variables.xidVacLimit = 2052;
		variables.xidWarnLimit = 4096;
		retention_recompute = true;
		retention_fault = fault;
		(void)retention_call(false, fault ? ERRCODE_CLUSTER_XID_AUTHORITY_UNAVAILABLE : 0);
		UT_ASSERT_EQ(U64FromFullTransactionId(variables.nextXid), fault ? 2084 : 2085);
		UT_ASSERT_EQ(extensions, fault ? 0 : 3);
		UT_ASSERT_EQ(retention_acquires, fault ? 2 : 3);
		UT_ASSERT_EQ(relmap_runtime_shmem->directories[0].last_issued, fault ? 2051 : 2084);
	}
}

UT_TEST(retention_preserves_original_stop_and_lease_gates)
{
	retention_fixture(2052, 0);
	variables.xidVacLimit = variables.xidStopLimit = 2052;
	IsUnderPostmaster = true;
	(void)retention_call(false, ERRCODE_PROGRAM_LIMIT_EXCEEDED);
	UT_ASSERT_EQ(extensions + native_publish_count, 0);
	UT_ASSERT_EQ(relmap_runtime_shmem->directories[0].last_issued, 2051);
	retention_fixture(2052, 0);
	cluster_shared_catalog = cluster_xid_striping = true;
	advance_while_unlocked = true;
	UT_ASSERT_EQ(U64FromFullTransactionId(retention_call(false, 0)), 2084);
	UT_ASSERT_EQ(waits, 1);
	UT_ASSERT_EQ(early_extensions, 0);
	UT_ASSERT_EQ(retention_acquires, 3);
	UT_ASSERT_EQ(relmap_runtime_shmem->directories[0].last_issued, 2084);
	retention_fixture((UINT64CONST(7) << 32) + 2052, 0);
	cluster_shared_catalog = true;
	retention_wrap_ready = false;
	(void)retention_call(false, ERRCODE_CLUSTER_XID_AUTHORITY_UNAVAILABLE);
	UT_ASSERT_EQ(retention_acquires + extensions + native_publish_count, 0);
}

int
main(void)
{
	UT_PLAN(13);
	UT_RUN(native_subxid_publication_preserves_the_final_cache_slot);
	UT_RUN(native_publication_keeps_the_full_era_across_raw_rollover);
	UT_RUN(no_storage_or_procarray_mutation_before_reservation);
	UT_RUN(wait_rederives_native_counter_and_new_boot_floor);
	UT_RUN(native_allocation_preserves_ordinary_counter);
	UT_RUN(actual_issuance_is_recorded_after_native_publication);
	UT_RUN(retention_final_callback_publishes_real_top_and_subxid);
	UT_RUN(retention_unknown_building_and_wrong_identity_cannot_issue);
	UT_RUN(retention_halfspace_boundary_refuses_without_mutating_native_state);
	UT_RUN(retention_rechecks_after_fallible_extensions_before_any_store);
	UT_RUN(retention_slru_error_never_observes_or_publishes_issuance);
	UT_RUN(retention_recomputed_candidate_is_checked_again);
	UT_RUN(retention_preserves_original_stop_and_lease_gates);
	free(relmap_runtime_shmem);
	UT_DONE();
	return ut_failed_count ? 1 : 0;
}
