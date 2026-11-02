/*-------------------------------------------------------------------------
 * test_cluster_xid_limits.c
 *    Original wrap-limit consumers with controlled allocation/catalogue leaves.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 * IDENTIFICATION
 *    src/test/cluster_unit/test_cluster_xid_limits.c
 *-------------------------------------------------------------------------
 */
#include "postgres.h"
#include "unit_test.h"
#include "access/transam.h"
#include "access/xact.h"
#include "access/xlog.h"
#include "cluster/cluster_guc.h"
#include "cluster/cluster_mode.h"
#include "cluster/cluster_xid_stripe.h"
#include "cluster/cluster_xid_stripe_boot.h"
#include "commands/dbcommands.h"
#include "miscadmin.h"
#include "postmaster/autovacuum.h"
#include "storage/lwlock.h"
#include "storage/pmsignal.h"
#include "utils/syscache.h"

UT_DEFINE_GLOBALS();
bool cluster_enabled = true;
bool cluster_shared_catalog = true;
bool cluster_xid_striping = true;
int cluster_node_id = 1;
bool IsUnderPostmaster = true;
bool InRecovery = false;
ProcessingMode Mode = NormalProcessing;
AuxProcType MyAuxProcType = NotAnAuxProcess;
int autovacuum_freeze_max_age = 200000000;
static VariableCacheData cache;
VariableCache ShmemVariableCache = &cache;
static LWLockPadded locks[NUM_INDIVIDUAL_LWLOCKS];
LWLockPadded *MainLWLockArray = locks;
static bool locked, ready, database_exists, in_transaction;
static uint64 legal_next;
static unsigned samples, acquires, releases, signals, warnings, lookups;
static int error_level, error_code;
static sigjmp_buf error_jump;

void
ExceptionalCondition(const char *conditionName, const char *fileName, int lineNumber)
{
	fprintf(stderr, "assertion failed: %s at %s:%d\n", conditionName, fileName, lineNumber);
	abort();
}

bool
LWLockAcquire(LWLock *lock, LWLockMode mode)
{
	UT_ASSERT(lock == XidGenLock && !locked);
	UT_ASSERT(mode == LW_SHARED || mode == LW_EXCLUSIVE);
	locked = true;
	acquires++;
	return true;
}

void
LWLockRelease(LWLock *lock)
{
	UT_ASSERT(lock == XidGenLock && locked);
	locked = false;
	releases++;
}

bool
cluster_xid_stripe_sample_future(FullTransactionId reference, FullTransactionId *future,
								 ClusterXidStripeSampleFence *fence)
{
	UT_ASSERT(locked);
	UT_ASSERT_EQ(U64FromFullTransactionId(reference), U64FromFullTransactionId(cache.nextXid));
	samples++;
	memset(fence, 0, sizeof(*fence));
	*future = ready ? FullTransactionIdFromU64(legal_next) : InvalidFullTransactionId;
	return ready;
}

void
SendPostmasterSignal(PMSignalReason reason)
{
	UT_ASSERT(!locked && reason == PMSIGNAL_START_AUTOVAC_LAUNCHER);
	signals++;
}

bool IsTransactionState(void) { return in_transaction; }
char *get_database_name(Oid oid) { UT_ASSERT(!locked && oid == 42); return "oldest"; }

bool
SearchSysCacheExists(int cacheId, Datum key1, Datum key2, Datum key3, Datum key4)
{
	UT_ASSERT(!locked && cacheId == DATABASEOID);
	UT_ASSERT_EQ(DatumGetObjectId(key1), cache.oldestXidDB);
	lookups++;
	return database_exists;
}

bool errstart(int level, const char *domain) { error_level = level; return level >= WARNING; }
bool errstart_cold(int level, const char *domain) { return errstart(level, domain); }
int errcode(int code) { error_code = code; return 0; }
int errmsg(const char *fmt, ...) { return 0; }
int errmsg_internal(const char *fmt, ...) { return 0; }
int errdetail(const char *fmt, ...) { return 0; }
int errhint(const char *fmt, ...) { return 0; }
void
errfinish(const char *file, int line, const char *func)
{
	UT_ASSERT(!locked);
	if (error_level >= ERROR)
		siglongjmp(error_jump, 1);
	warnings++;
}

#include "test_cluster_xid_limits_order.inc"
#include "test_cluster_xid_limits.inc"

static void
reset(void)
{
	memset(&cache, 0, sizeof(cache));
	cache.nextXid = FullTransactionIdFromU64(1000);
	cache.oldestXid = 100;
	cache.xidVacLimit = 200000100;
	cache.xidWarnLimit = 2107483747;
	cache.xidStopLimit = 2144483747;
	cache.xidWrapLimit = 2147483747;
	cache.oldestXidDB = 42;
	cluster_enabled = cluster_shared_catalog = cluster_xid_striping = true;
	cluster_node_id = 1;
	IsUnderPostmaster = true;
	InRecovery = in_transaction = false;
	Mode = NormalProcessing;
	MyAuxProcType = NotAnAuxProcess;
	locked = false;
	ready = database_exists = true;
	legal_next = 300000100;
	samples = acquires = releases = signals = warnings = lookups = 0;
	error_code = 0;
}

static void
expect_refusal(TransactionId oldest, int code)
{
	VariableCacheData before = cache;
	bool caught = false;

	if (sigsetjmp(error_jump, 1) == 0)
		SetTransactionIdLimit(oldest, 42);
	else
		caught = true;
	UT_ASSERT(caught);
	UT_ASSERT_EQ(error_code, code);
	UT_ASSERT(memcmp(&before, &cache, sizeof(cache)) == 0);
	UT_ASSERT(!locked);
	UT_ASSERT_EQ(acquires, releases);
	UT_ASSERT_EQ(signals + warnings + lookups, 0);
}

UT_TEST(nonshared_limits_keep_original_database_arithmetic)
{
	reset();
	cluster_shared_catalog = false;
	SetTransactionIdLimit(100, 42);
	UT_ASSERT_EQ(samples, 0);
	UT_ASSERT_EQ(signals + warnings, 0);
	UT_ASSERT_EQ(cache.oldestXid, 100);
	UT_ASSERT_EQ(cache.xidVacLimit, 200000100);
	UT_ASSERT_EQ(cache.xidWarnLimit, 2107483747);
	UT_ASSERT_EQ(cache.xidStopLimit, 2144483747);
	UT_ASSERT_EQ(cache.xidWrapLimit, 2147483747);
}

UT_TEST(shared_legal_next_triggers_original_autovacuum)
{
	reset();
	SetTransactionIdLimit(100, 42);
	UT_ASSERT_EQ(signals, 1);
	UT_ASSERT_EQ(samples, 1);
	UT_ASSERT_EQ(warnings, 0);
	UT_ASSERT_EQ(cache.xidWrapLimit, 2147483747);
	UT_ASSERT_EQ(U64FromFullTransactionId(cache.nextXid), 1000);
	UT_ASSERT_EQ(acquires, 1);
	UT_ASSERT_EQ(releases, 1);
}

UT_TEST(shared_warning_and_recovery_use_original_thresholds)
{
	for (unsigned recovery = 0; recovery < 2; recovery++) {
		reset();
		legal_next = 2120000000;
		InRecovery = recovery;
		in_transaction = true;
		SetTransactionIdLimit(100, 42);
		UT_ASSERT_EQ(signals, !recovery);
		UT_ASSERT_EQ(warnings, !recovery);
		UT_ASSERT_EQ(cache.xidWrapLimit, 2147483747);
	}
}

UT_TEST(shared_future_database_refuses_before_limit_mutation)
{
	reset();
	legal_next = 200;
	expect_refusal(300, ERRCODE_DATA_CORRUPTED);
}

UT_TEST(shared_half_space_database_refuses_before_limit_mutation)
{
	reset();
	legal_next = UINT64CONST(0x180000064);
	expect_refusal(100, ERRCODE_DATA_CORRUPTED);
}

UT_TEST(unavailable_shared_promise_preserves_all_limits)
{
	for (unsigned disabled = 0; disabled < 2; disabled++) {
		reset();
		ready = disabled;
		cluster_xid_striping = !disabled;
		expect_refusal(100, ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE);
	}
}

UT_TEST(invalid_shared_next_preserves_all_limits)
{
	const uint64 invalid[] = { 0, 1, 2, UINT64CONST(0x100000002), UINT64_MAX };
	for (unsigned i = 0; i < lengthof(invalid); i++) {
		reset();
		legal_next = invalid[i];
		expect_refusal(100, ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE);
	}
}

UT_TEST(previous_epoch_database_keeps_raw_wrap_limits)
{
	reset();
	legal_next = UINT64CONST(0x100000100);
	SetTransactionIdLimit(UINT32_C(0xfffffff0), 42);
	UT_ASSERT_EQ(cache.oldestXid, UINT32_C(0xfffffff0));
	UT_ASSERT_EQ(cache.xidWrapLimit, UINT32_C(0x7fffffef));
	UT_ASSERT_EQ(cache.xidVacLimit, 199999984);
	UT_ASSERT_EQ(signals + warnings, 0);
	UT_ASSERT_EQ(samples, 1);
}

UT_TEST(refresh_uses_legal_age_instead_of_local_counter)
{
	reset();
	UT_ASSERT(ForceTransactionIdLimitUpdate());
	UT_ASSERT_EQ(samples, 1);
	UT_ASSERT_EQ(lookups, 0);
	UT_ASSERT_EQ(signals + warnings, 0);
}

UT_TEST(refresh_unknown_promise_requests_recalculation_without_mutation)
{
	reset();
	ready = false;
	UT_ASSERT(ForceTransactionIdLimitUpdate());
	UT_ASSERT_EQ(lookups, 0);
	UT_ASSERT_EQ(cache.oldestXid, 100);
	UT_ASSERT_EQ(cache.xidWrapLimit, 2147483747);
	UT_ASSERT(!locked);
}

UT_TEST(refresh_rejects_unknown_or_ambiguous_stored_database_cut)
{
	const TransactionId invalid[] = { 0, 1, 2, 300 };
	for (unsigned i = 0; i < lengthof(invalid); i++) {
		reset();
		legal_next = 200;
		cache.oldestXid = invalid[i];
		UT_ASSERT(ForceTransactionIdLimitUpdate());
		UT_ASSERT_EQ(lookups, 0);
	}
}

UT_TEST(refresh_keeps_original_database_lookup_outside_lock)
{
	for (unsigned exists = 0; exists < 2; exists++) {
		reset();
		legal_next = 200;
		database_exists = exists;
		UT_ASSERT_EQ(ForceTransactionIdLimitUpdate(), !exists);
		UT_ASSERT_EQ(lookups, 1);
		UT_ASSERT_EQ(samples, 1);
		UT_ASSERT(!locked);
	}
}

UT_TEST(creation_and_nonstorage_modes_keep_original_behavior)
{
	for (unsigned mode = 0; mode < 3; mode++) {
		reset();
		cluster_enabled = mode != 0;
		cluster_node_id = mode == 1 ? -1 : 1;
		cluster_shared_catalog = mode != 2;
		SetTransactionIdLimit(100, 42);
		UT_ASSERT(!ForceTransactionIdLimitUpdate());
		UT_ASSERT_EQ(samples + signals + warnings, 0);
	}
}

UT_TEST(checkpoint_restoration_does_not_require_an_online_lease)
{
	for (unsigned stage = 0; stage < 4; stage++) {
		bool caught = false;

		reset();
		ready = false;
		Mode = stage == 0 ? InitProcessing : stage == 1 ? BootstrapProcessing : NormalProcessing;
		MyAuxProcType = stage == 2 ? StartupProcess : NotAnAuxProcess;
		InRecovery = stage == 3;
		if (sigsetjmp(error_jump, 1) == 0)
			SetTransactionIdLimit(100, 42);
		else
			caught = true;
		UT_ASSERT(!caught);
		UT_ASSERT(!ForceTransactionIdLimitUpdate());
		UT_ASSERT_EQ(samples + signals + warnings, 0);
		UT_ASSERT_EQ(cache.xidWrapLimit, 2147483747);
		UT_ASSERT(!locked);
	}
}

UT_TEST(special_database_xids_fail_with_a_production_error)
{
	for (TransactionId xid = InvalidTransactionId; xid < FirstNormalTransactionId; xid++) {
		reset();
		expect_refusal(xid, ERRCODE_DATA_CORRUPTED);
	}
}

int
main(void)
{
	UT_PLAN(15);
	UT_RUN(nonshared_limits_keep_original_database_arithmetic);
	UT_RUN(shared_legal_next_triggers_original_autovacuum);
	UT_RUN(shared_warning_and_recovery_use_original_thresholds);
	UT_RUN(shared_future_database_refuses_before_limit_mutation);
	UT_RUN(shared_half_space_database_refuses_before_limit_mutation);
	UT_RUN(unavailable_shared_promise_preserves_all_limits);
	UT_RUN(invalid_shared_next_preserves_all_limits);
	UT_RUN(previous_epoch_database_keeps_raw_wrap_limits);
	UT_RUN(refresh_uses_legal_age_instead_of_local_counter);
	UT_RUN(refresh_unknown_promise_requests_recalculation_without_mutation);
	UT_RUN(refresh_rejects_unknown_or_ambiguous_stored_database_cut);
	UT_RUN(refresh_keeps_original_database_lookup_outside_lock);
	UT_RUN(creation_and_nonstorage_modes_keep_original_behavior);
	UT_RUN(checkpoint_restoration_does_not_require_an_online_lease);
	UT_RUN(special_database_xids_fail_with_a_production_error);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
