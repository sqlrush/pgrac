/* Actual issuer extension, managed SLRU birth and WAL-first file writer.
 * Table/lock/WAL timing are finite providers; file bytes use real local I/O. */
#include "postgres.h"
#include "storage/lwlock.h"
#include "access/rmgr.h"
#include "access/xlogdefs.h"
#include <sys/wait.h>
static bool counted_holdoff, cleanup_throw, expect_cleanup_panic;
static int birth_error_level;
static void birth_error_reset(void);
#define SLRU_FIXTURE_COUNT_HOLDOFF() counted_holdoff
#define SLRU_FIXTURE_ERROR_LEVEL(level) (birth_error_level = (level))
#define SLRU_FIXTURE_ERROR_RESET() birth_error_reset()
static LWLock *birth_issuer_lock(void);
static LWLock *birth_control_lock(void);
static XLogRecPtr birth_insert(RmgrId, uint8);
static void birth_flush(XLogRecPtr);
static bool issuer_held, birth_pending;
int birth_snapshot_open(const char *, int);
int birth_managed_original_main(void);
#undef XidGenLock
#undef XactSLRULock
#define XidGenLock birth_issuer_lock()
#define XactSLRULock birth_control_lock()
#define SLRU_FIXTURE_ERROR_LOCKS() (!io_held || control_held)
#define SLRU_FIXTURE_ISSUER_HELD() issuer_held
#define SLRU_FIXTURE_BIRTH_PENDING() birth_pending
#define SLRU_FIXTURE_BIRTH_INSERT(r, i) birth_insert(r, i)
#define SLRU_FIXTURE_BIRTH_FLUSH(l) birth_flush(l)
#define SLRU_FIXTURE_OPEN birth_snapshot_open
#define CLOG_MANAGED_MAIN birth_managed_original_main
#include "test_cluster_slru_clog_managed_write.c"
#include "access/clog.h"
static void WriteZeroPageXlogRec(int);
#define XactCtl (&ctl)
#include "test_cluster_clog_native_birth.inc"

static LWLock issuer;
static unsigned borrows, releases, current_calls, fail_current_at, birth_records;
static bool file_available, release_available, caught_family;
static ClusterClogPageIdentityV2 birth_observed;
static void birth_error_reset(void)
{
	if (!counted_holdoff) return;
	/* errfinish(ERROR) resets holds but leaves the locks held. A real PANIC
	 * never reaches a PG_CATCH, so observe its retained duties and exit. */
	if (birth_error_level >= FATAL) {
		bool preserved = expect_cleanup_panic && birth_error_level == PANIC &&
			borrows == 1 && releases == 0 && closes == 1 && control_held && !io_held &&
			states[0] == SLRU_PAGE_EMPTY && !dirty[0] &&
			proc.delayChkptFlags == (DELAY_CHKPT_START | DELAY_CHKPT_COMPLETE) &&
			fcntl(borrowed, F_GETFD) >= 0;
		_exit(preserved ? 0 : 2);
	}
	InterruptHoldoffCount = 0;
}
static LWLock *birth_issuer_lock(void) { return &issuer; }
static LWLock *birth_control_lock(void) { return &control; }
ClusterControlNativeModeV2 cluster_control_native_files_mode_v2(void)
{ return CLUSTER_NATIVE_FILES_MANAGED_V2; }
int OpenTransientFile(const char *path, int flags)
{
	UT_ASSERT(!control_held); UT_ASSERT(strcmp(path, filename) == 0);
	UT_ASSERT((flags & (O_CREAT | O_TRUNC | O_APPEND)) == 0);
	return open(path, flags, 0600);
}
static bool birth_required(void) { return true; }
static bool birth_acquire(int pageno, ClusterControlNativeFileBorrowV2 *out)
{
	UT_ASSERT(control_held); UT_ASSERT_EQ(pageno, 3);
	if (!file_available) return false;
	out->expected = binding.identity; out->private_self = (uintptr_t)out;
	borrows++; return true;
}
static bool birth_current(const ClusterControlNativeFileBorrowV2 *borrow)
{
	UT_ASSERT_EQ(borrow->private_self, (uintptr_t)borrow);
	current_calls++; return !fail_current_at || current_calls != fail_current_at;
}
static bool birth_release(ClusterControlNativeFileBorrowV2 *borrow)
{
	UT_ASSERT_EQ(borrow->private_self, (uintptr_t)borrow);
	if (cleanup_throw) {
		cleanup_throw = false;
		ereport(ERROR, (errcode(ERRCODE_IO_ERROR), errmsg("injected birth cleanup error")));
	}
	if (!release_available) return false;
	releases++; memset(borrow, 0, sizeof(*borrow)); return true;
}
static XLogRecPtr birth_insert(RmgrId rmgr, uint8 info)
{
	ClusterClogWalObservationV2 observed;
	UT_ASSERT_EQ(rmgr, RM_CLOG_ID); UT_ASSERT_EQ(info, 0x50);
	UT_ASSERT(!control_held && io_held && issuer_held && CritSectionCount == 0);
	UT_ASSERT(proc.delayChkptFlags & DELAY_CHKPT_START);
	UT_ASSERT(registered != NULL);
	UT_ASSERT(cluster_clog_wal_decode_v2(info, registered, sizeof(native_record), &observed));
	UT_ASSERT_EQ(observed.page.identity.logical_page, 3);
	UT_ASSERT_EQ(observed.page.image_lsn, 0);
	UT_ASSERT_EQ(observed.page.identity.page_lsn, 0);
	for (Size i = 64; i < observed.image_bytes; i++) UT_ASSERT_EQ(observed.image[i], 0);
	birth_observed = observed.page;
	memcpy(native_record, registered, sizeof(native_record));
	inserts++; birth_records++; record_event('I');
	if (wal_throw == 1) {
		birth_error_level = ERROR; birth_error_reset();
		siglongjmp(*PG_exception_stack, 1);
	}
	return actual_end;
}
static void birth_flush(XLogRecPtr lsn)
{
	UT_ASSERT(!control_held && io_held && issuer_held);
	UT_ASSERT(proc.delayChkptFlags & DELAY_CHKPT_START);
	UT_ASSERT_EQ(lsn, actual_end); flushes++; flushed = lsn; record_event('F');
	if (wal_throw == 2) {
		birth_error_level = ERROR; birth_error_reset();
		siglongjmp(*PG_exception_stack, 1);
	}
}
static void reset_birth(void)
{
	UT_ASSERT(CLOGNativeIORequired());
	managed_reset();
	if (ftruncate(borrowed, 8192) != 0) abort();
	memset(cached_bindings, 0, sizeof(cached_bindings));
	memset(identities, 0, sizeof(identities));
	states[0] = SLRU_PAGE_EMPTY; dirty[0] = false;
	numbers[0] = 0; shared.latest_page_number = 0;
	ctl.PagePrecedes = page_precedes;
	ctl.clog_native_required = birth_required; ctl.clog_native_acquire = birth_acquire;
	ctl.clog_native_current = birth_current; ctl.clog_native_release = birth_release;
	ctl.clog_native_read = SimpleLruNativeClogReadV2;
	ctl.clog_native_write = SimpleLruNativeClogWriteV2;
	borrows = releases = current_calls = fail_current_at = birth_records = 0;
	file_available = release_available = issuer_held = true;
	birth_pending = caught_family = false; control_held = false;
	counted_holdoff = cleanup_throw = expect_cleanup_panic = false;
	birth_error_level = 0;
}
static bool extend_birth(FullTransactionId birth)
{
	volatile bool caught = false;
	birth_pending = true;
	PG_TRY(); { ExtendCLOGFull(birth); }
	PG_CATCH(); {
		caught = true; caught_family = control_held;
		/* Model backend ERROR cleanup, while separately asserting that the
		 * managed extension itself retired its original family lock. */
		if (control_held) LWLockRelease(&control);
	}
	PG_END_TRY();
	birth_pending = false;
	return !caught;
}
static FullTransactionId actual_birth(void)
{ return FullTransactionIdFromEpochAndXid(7, 3 * 32768); }
UT_TEST(actual_issuer_birth_logs_full_image_before_cache_and_original_write)
{
	ClusterClogPageIdentityV2 observed;
	uint8 bytes[8192];
	reset_birth();
	if (!extend_birth(actual_birth())) { UT_ASSERT(false); close(borrowed); return; }
	UT_ASSERT(!control_held && !io_held); UT_ASSERT_EQ(borrows, 1); UT_ASSERT_EQ(releases, 1);
	UT_ASSERT_EQ(birth_records, 1); UT_ASSERT_EQ(flushes, 1); UT_ASSERT_EQ(writes, 0);
	UT_ASSERT_EQ(states[0], SLRU_PAGE_VALID); UT_ASSERT(dirty[0]);
	UT_ASSERT_EQ(identities[0].identity.page_lsn, 1000); UT_ASSERT_EQ(identities[0].image_lsn, 0);
	UT_ASSERT(FullTransactionIdEquals(identities[0].identity.birth_full_xid, actual_birth()));
	UT_ASSERT_EQ(lsns[0] + lsns[1], 0);
	{ uint8 zero[BLCKSZ] = {0}; UT_ASSERT(memcmp(page.data, zero, BLCKSZ) == 0); }
	actual_end = 2000; control_held = true;
	SimpleLruWritePage(&ctl, 0);
	UT_ASSERT(control_held && !io_held && !dirty[0]);
	UT_ASSERT_EQ(states[0], SLRU_PAGE_VALID); UT_ASSERT_EQ(queues, 0);
	UT_ASSERT_EQ(closes, 2); UT_ASSERT(fcntl(borrowed, F_GETFD) >= 0);
	UT_ASSERT_EQ(inserts, 2); UT_ASSERT_EQ(borrows, 2); UT_ASSERT_EQ(releases, 2);
	control_held = false; io_held = true;
	UT_ASSERT(cluster_clog_storage_read_page_v2(borrowed, &binding, 3, &observed, bytes, sizeof(bytes)));
	control_held = true; io_held = false;
	UT_ASSERT_EQ(observed.image_lsn, 2000); UT_ASSERT_EQ(observed.identity.page_lsn, 1000);
	UT_ASSERT(FullTransactionIdEquals(observed.identity.birth_full_xid, actual_birth()));
	close(borrowed);
}
UT_TEST(exact_resident_retry_preserves_terminal_bytes_without_another_birth)
{
	reset_birth();
	if (!extend_birth(actual_birth())) { UT_ASSERT(false); close(borrowed); return; }
	page.data[10] = 2;
	UT_ASSERT(extend_birth(actual_birth()));
	UT_ASSERT_EQ((uint8)page.data[10], 2); UT_ASSERT_EQ(birth_records, 1);
	UT_ASSERT_EQ(borrows, releases); close(borrowed);
}
UT_TEST(unknown_issuer_or_reserved_cannot_zero_or_emit_birth)
{
	for (unsigned variant = 0; variant < 2; variant++) {
		reset_birth(); if (variant) file_available = false; else issuer_held = false;
		UT_ASSERT(!extend_birth(actual_birth())); UT_ASSERT(!caught_family);
		UT_ASSERT_EQ(inserts + writes, 0);
		UT_ASSERT_EQ(states[0], SLRU_PAGE_EMPTY); UT_ASSERT_EQ(borrows, releases);
		close(borrowed);
	}
}
UT_TEST(existing_disk_frame_header_damage_or_partial_slot_never_becomes_birth)
{
	for (unsigned variant = 0; variant < 3; variant++) {
		uint8 frame[8256], bytes[8192] = {1};
		reset_birth();
		if (!variant) {
			ClusterClogPageIdentityV2 old = {0};
			old.identity.logical_page = 3; old.identity.page_generation = 9;
			old.identity.birth_full_xid = FullTransactionIdFromEpochAndXid(6, 3 * 32768);
			old.identity.birth_observed = true; old.image_lsn = 10;
			UT_ASSERT(cluster_clog_file_page_encode_v2(&binding, &old, bytes, sizeof(bytes), frame, sizeof(frame)));
			UT_ASSERT_EQ(pwrite(borrowed, frame, sizeof(frame), 8192 + 3 * 8256), sizeof(frame));
		} else if (variant == 1) UT_ASSERT_EQ(pwrite(borrowed, "x", 1, 0), 1);
		else UT_ASSERT_EQ(ftruncate(borrowed, 8192 + 3 * 8256 + 1), 0);
		UT_ASSERT(!extend_birth(actual_birth())); UT_ASSERT(!caught_family);
		UT_ASSERT_EQ(inserts + writes, 0);
		UT_ASSERT_EQ(states[0], SLRU_PAGE_EMPTY); UT_ASSERT(!dirty[0] && !io_held);
		UT_ASSERT_EQ(borrows, releases); close(borrowed);
	}
}
UT_TEST(wal_insert_and_flush_errors_remove_only_the_incomplete_reservation)
{
	for (unsigned variant = 1; variant <= 3; variant++) {
		for (unsigned preowned = 0; preowned < 2; preowned++) {
			reset_birth();
			proc.delayChkptFlags = DELAY_CHKPT_COMPLETE | (preowned ? DELAY_CHKPT_START : 0);
			if (variant <= 2) wal_throw = variant; else fail_current_at = 3;
			UT_ASSERT(!extend_birth(actual_birth())); UT_ASSERT(!caught_family);
			UT_ASSERT_EQ(inserts, 1); UT_ASSERT_EQ(writes, 0);
			UT_ASSERT_EQ(states[0], SLRU_PAGE_EMPTY); UT_ASSERT(!dirty[0] && !io_held);
			UT_ASSERT_EQ(cached_bindings[0].identity.creation_generation, 0);
			UT_ASSERT_EQ(proc.delayChkptFlags, DELAY_CHKPT_COMPLETE | (preowned ? DELAY_CHKPT_START : 0));
			UT_ASSERT_EQ(borrows, 1); UT_ASSERT_EQ(releases, 1); close(borrowed);
		}
	}
}
UT_TEST(other_resident_birth_is_preserved_and_nonboundary_has_no_work)
{
	ClusterClogPageIdentityV2 before;
	reset_birth();
	if (!extend_birth(actual_birth())) { UT_ASSERT(false); close(borrowed); return; } before = identities[0];
	UT_ASSERT(!extend_birth(FullTransactionIdFromEpochAndXid(8, 3 * 32768)));
	UT_ASSERT(memcmp(&before, identities, sizeof(before)) == 0); UT_ASSERT_EQ(birth_records, 1);
	UT_ASSERT(extend_birth(FullTransactionIdFromEpochAndXid(7, 3 * 32768 + 1)));
	UT_ASSERT_EQ(birth_records, 1); close(borrowed);
}
static void counted_birth_child(unsigned fault, unsigned preowned)
{
	bool success;
	reset_birth(); counted_holdoff = true;
	/* Caller issuer lock plus three unrelated holds must survive success.
	 * ERROR instead starts cleanup at elog's zero baseline. */
	InterruptHoldoffCount = 4;
	proc.delayChkptFlags = DELAY_CHKPT_COMPLETE | (preowned ? DELAY_CHKPT_START : 0);
	if (fault == 1 || fault == 2) wal_throw = fault;
	if (fault == 3) fail_current_at = 3;
	if (fault == 4) fail_current_at = 1;
	if (fault >= 5) {
		wal_throw = 2; expect_cleanup_panic = true;
		if (fault == 5) release_available = false;
		else cleanup_throw = true;
	}
	success = extend_birth(actual_birth());
	UT_ASSERT_EQ(success, fault == 0);
	UT_ASSERT(!expect_cleanup_panic); /* A required PANIC cannot return. */
	UT_ASSERT(!caught_family && !control_held && !io_held);
	UT_ASSERT_EQ(InterruptHoldoffCount, fault == 0 ? 4 : 0);
	UT_ASSERT_EQ(borrows, 1); UT_ASSERT_EQ(releases, 1); UT_ASSERT_EQ(closes, fault == 4 ? 0 : 1);
	UT_ASSERT_EQ(proc.delayChkptFlags, DELAY_CHKPT_COMPLETE | (preowned ? DELAY_CHKPT_START : 0));
	UT_ASSERT_EQ(states[0], fault == 0 ? SLRU_PAGE_VALID : SLRU_PAGE_EMPTY);
	UT_ASSERT_EQ(dirty[0], fault == 0);
	UT_ASSERT_EQ(writes, 0);
	UT_ASSERT(fcntl(borrowed, F_GETFD) >= 0);
	close(borrowed);
	fflush(NULL);
	_exit(ut_current_failed ? 1 : 0);
}
static void counted_birth_run(unsigned fault, unsigned preowned)
{
	int status = 0;
	pid_t child;
	fflush(NULL);
	child = fork();
	if (child == 0) counted_birth_child(fault, preowned);
	UT_ASSERT(child > 0);
	if (child <= 0) return;
	UT_ASSERT_EQ(waitpid(child, &status, 0), child);
	UT_ASSERT(WIFEXITED(status));
	UT_ASSERT_EQ(status, 0);
}
UT_TEST(counted_success_preserves_original_caller_holds)
{
	counted_birth_run(0, 0); counted_birth_run(0, 1);
}
UT_TEST(real_error_reset_balances_each_new_lock_release)
{
	for (unsigned fault = 1; fault <= 4; fault++)
		for (unsigned preowned = 0; preowned < 2; preowned++) counted_birth_run(fault, preowned);
}
UT_TEST(cleanup_refusal_or_second_error_retains_borrow_and_panics)
{
	counted_birth_run(5, 1); counted_birth_run(6, 1);
}
int main(void)
{
	strlcpy(directory, "/tmp/pgrac-clog-birth-XXXXXX", sizeof(directory));
	if (mkdtemp(directory) == NULL) abort();
	snprintf(filename, sizeof(filename), "%s/0000", directory);
	UT_PLAN(9);
	UT_RUN(actual_issuer_birth_logs_full_image_before_cache_and_original_write);
	UT_RUN(exact_resident_retry_preserves_terminal_bytes_without_another_birth);
	UT_RUN(unknown_issuer_or_reserved_cannot_zero_or_emit_birth);
	UT_RUN(existing_disk_frame_header_damage_or_partial_slot_never_becomes_birth);
	UT_RUN(wal_insert_and_flush_errors_remove_only_the_incomplete_reservation);
	UT_RUN(other_resident_birth_is_preserved_and_nonboundary_has_no_work);
	UT_RUN(counted_success_preserves_original_caller_holds);
	UT_RUN(real_error_reset_balances_each_new_lock_release);
	UT_RUN(cleanup_refusal_or_second_error_retains_borrow_and_panics);
	unlink(filename); rmdir(directory);
	UT_DONE(); return ut_failed_count != 0;
}
