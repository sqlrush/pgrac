/* Author: SqlRush <sqlrush@gmail.com>
 * Actual CLOG extension/WAL bodies. SLRU allocation, issuer lock and WAL sink
 * are bounded providers; this does not qualify managed I/O or replay. */
#include "postgres.h"
#include <setjmp.h>
#ifdef CLOG_BIRTH_SOURCE
#include CLOG_BIRTH_SOURCE
#else
#include "../../backend/access/transam/clog.c"
#endif
#undef printf
#undef fprintf
#include "unit_test.h"
UT_DEFINE_GLOBALS();

#ifdef USE_PGRAC_CLUSTER
bool cluster_enabled;
volatile uint32 InterruptHoldoffCount;
sigjmp_buf *PG_exception_stack;
ErrorContextCallback *error_context_stack;
void pg_re_throw(void) { abort(); }
ClusterControlNativeModeV2 cluster_control_native_files_mode_v2(void)
{ return CLUSTER_NATIVE_FILES_UNKNOWN_V2; }
int SimpleLruNativeClogZeroV2(SlruCtl ctl pg_attribute_unused(),
							 FullTransactionId birth pg_attribute_unused())
{ abort(); } /* This legacy fixture must never enter managed birth. */
#endif

static LWLockPadded locks[NUM_INDIVIDUAL_LWLOCKS];
LWLockPadded *MainLWLockArray = locks;
static SlruSharedData shared;
static ClusterClogPageIdentityV2 identities[1];
static unsigned zeros, wal_calls, lock_calls;
static bool issuer_held, clog_held, expect_error;
static jmp_buf error_env;
static int last_page, wal_page, error_code;

void ExceptionalCondition(const char *c, const char *f, int l)
{ fprintf(stderr, "%s at %s:%d\n", c, f, l); abort(); }
bool LWLockHeldByMeInMode(LWLock *lock, LWLockMode mode)
{ return lock == XidGenLock && mode == LW_EXCLUSIVE && issuer_held; }
bool LWLockAcquire(LWLock *lock, LWLockMode mode)
{
	UT_ASSERT(lock == XactSLRULock && mode == LW_EXCLUSIVE && !clog_held);
	clog_held = true; lock_calls++; return true;
}
void LWLockRelease(LWLock *lock)
{ UT_ASSERT(lock == XactSLRULock && clog_held); clog_held = false; }
int SimpleLruZeroPage(SlruCtl ctl, int pageno)
{
	UT_ASSERT(ctl == XactCtl && clog_held);
	/* The actual reset is independently executed in the SLRU fixture. */
	memset(identities, 0, sizeof(identities));
	zeros++; last_page = pageno; return 0;
}
void XLogBeginInsert(void) { UT_ASSERT(clog_held); }
void XLogRegisterData(char *data, uint32 len)
{ UT_ASSERT_EQ(len, sizeof(int)); memcpy(&wal_page, data, sizeof(int)); }
XLogRecPtr XLogInsert(RmgrId id, uint8 info)
{
	UT_ASSERT_EQ(id, RM_CLOG_ID); UT_ASSERT_EQ(info, CLOG_ZEROPAGE);
	UT_ASSERT(clog_held); wal_calls++; return 123;
}
bool errstart(int level, const char *domain pg_attribute_unused())
{ if (level != ERROR || !expect_error) abort(); return true; }
bool errstart_cold(int level, const char *domain) { return errstart(level, domain); }
int errcode(int code) { error_code = code; return 0; }
int errmsg(const char *fmt pg_attribute_unused(), ...) { return 0; }
void errfinish(const char *f pg_attribute_unused(), int l pg_attribute_unused(),
			   const char *func pg_attribute_unused()) { longjmp(error_env, 1); }


static void reset(void)
{
	memset(&shared, 0, sizeof(shared));
	memset(identities, 0, sizeof(identities));
	XactCtl->shared = &shared;
	shared.clog_page_identity = identities;
	issuer_held = true; clog_held = expect_error = false;
	zeros = wal_calls = lock_calls = 0; error_code = 0;
	last_page = wal_page = -1;
}
static bool refuses(FullTransactionId xid)
{
	expect_error = true;
	if (setjmp(error_env) == 0) { ExtendCLOGFull(xid); expect_error = false; return false; }
	expect_error = false; return true;
}
UT_TEST(full_birth_comes_from_original_issuer_under_clog_lock)
{
	FullTransactionId birth = FullTransactionIdFromEpochAndXid(7, 32768);
	reset(); ExtendCLOGFull(birth);
	UT_ASSERT_EQ(zeros, 1); UT_ASSERT_EQ(wal_calls, 1);
	UT_ASSERT_EQ(lock_calls, 1); UT_ASSERT(!clog_held);
	UT_ASSERT_EQ(last_page, 1); UT_ASSERT_EQ(wal_page, 1);
	UT_ASSERT(identities[0].identity.birth_observed);
	UT_ASSERT_EQ(U64FromFullTransactionId(identities[0].identity.birth_full_xid), U64FromFullTransactionId(birth));
	UT_ASSERT_EQ(identities[0].identity.page_generation, U64FromFullTransactionId(birth));
	UT_ASSERT_EQ(identities[0].identity.logical_page, 1);
	UT_ASSERT_EQ(identities[0].identity.page_lsn, 0);
	UT_ASSERT_EQ(identities[0].image_lsn, 0);
}
UT_TEST(same_raw_page_in_new_epoch_has_different_observed_birth)
{
	uint64 previous;
	reset(); ExtendCLOGFull(FullTransactionIdFromEpochAndXid(7, FirstNormalTransactionId));
	previous = identities[0].identity.page_generation;
	ExtendCLOGFull(FullTransactionIdFromEpochAndXid(8, FirstNormalTransactionId));
	UT_ASSERT_EQ(zeros, 2); UT_ASSERT_EQ(last_page, 0);
	UT_ASSERT(identities[0].identity.page_generation != previous);
	UT_ASSERT_EQ(U64FromFullTransactionId(identities[0].identity.birth_full_xid), (UINT64CONST(8) << 32) + 3);
}
UT_TEST(raw_only_legacy_zero_cannot_retain_previous_epoch)
{
	reset(); ExtendCLOGFull(FullTransactionIdFromEpochAndXid(7, 32768));
	ExtendCLOG(32768);
	UT_ASSERT_EQ(zeros, 2); UT_ASSERT_EQ(wal_calls, 2);
	UT_ASSERT(!identities[0].identity.birth_observed);
	UT_ASSERT_EQ(U64FromFullTransactionId(identities[0].identity.birth_full_xid), 0);
}
UT_TEST(nonboundary_is_no_zero_and_preserves_birth)
{
	ClusterClogPageIdentityV2 before;
	reset(); ExtendCLOGFull(FullTransactionIdFromEpochAndXid(7, 32768));
	before = identities[0];
	ExtendCLOGFull(FullTransactionIdFromEpochAndXid(7, 32769));
	UT_ASSERT_EQ(zeros, 1); UT_ASSERT_EQ(lock_calls, 1);
	UT_ASSERT(memcmp(&before, identities, sizeof(before)) == 0);
}
UT_TEST(invalid_full_or_missing_issuer_cannot_mutate_native_page)
{
	const uint64 invalid[] = {0, 1, 2, UINT64_MAX, UINT64CONST(7) << 32,
		(UINT64CONST(7) << 32) + 1, (UINT64CONST(7) << 32) + 2};
	for (unsigned i = 0; i < lengthof(invalid); i++) {
		reset(); UT_ASSERT(refuses(FullTransactionIdFromU64(invalid[i])));
		UT_ASSERT_EQ(zeros, 0); UT_ASSERT_EQ(lock_calls, 0);
	}
	reset(); issuer_held = false;
	UT_ASSERT(refuses(FullTransactionIdFromU64(32768)));
	UT_ASSERT_EQ(zeros, 0); UT_ASSERT_EQ(wal_calls, 0);
	UT_ASSERT_EQ(error_code, ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE);
	reset(); shared.clog_page_identity = NULL;
	UT_ASSERT(refuses(FullTransactionIdFromU64(32768)));
	UT_ASSERT_EQ(zeros, 0); UT_ASSERT_EQ(wal_calls, 0);
}
int main(void)
{
	UT_PLAN(5);
	UT_RUN(full_birth_comes_from_original_issuer_under_clog_lock);
	UT_RUN(same_raw_page_in_new_epoch_has_different_observed_birth);
	UT_RUN(raw_only_legacy_zero_cannot_retain_previous_epoch);
	UT_RUN(nonboundary_is_no_zero_and_preserves_birth);
	UT_RUN(invalid_full_or_missing_issuer_cannot_mutate_native_page);
	UT_DONE(); return ut_failed_count != 0;
}
