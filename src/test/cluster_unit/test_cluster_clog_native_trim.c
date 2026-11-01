/* Original managed SLRU/file/WAL writer under the actual startup trim.
 * Table, lock scheduling, startup cut and WAL/ERROR providers are finite. */
#include "postgres.h"
#include "storage/lwlock.h"
static bool trim_count_holds;
static bool trim_open_error;
static LWLock *trim_control_lock(void);
static void trim_error_reset(void);
int trim_snapshot_open(const char *, int);
#undef XactSLRULock
#define XactSLRULock trim_control_lock()
#define SLRU_FIXTURE_COUNT_HOLDOFF() trim_count_holds
#define SLRU_FIXTURE_ERROR_LOCKS() (!io_held || control_held)
#define SLRU_FIXTURE_ERROR_RESET() trim_error_reset()
#define SLRU_FIXTURE_OPEN trim_snapshot_open
#define CLOG_MANAGED_MAIN trim_managed_fixture_main
int trim_managed_fixture_main(void);
#include "test_cluster_slru_clog_managed_write.c"
#include "access/clog.h"
#include "cluster/cluster_native_startup.h"
#define XactCtl (&ctl)
#ifdef NATIVE_CLOG_TRIM_PRODUCT
#include NATIVE_CLOG_TRIM_PRODUCT
#else
#include "test_cluster_clog_native_trim.inc"
#endif

static VariableCacheData variables;
VariableCache ShmemVariableCache = &variables;
bool cluster_shared_config;
bool IsPostmasterEnvironment;
BackendType MyBackendType;
static ClusterControlNativeModeV2 trim_mode;
static unsigned trim_borrows, trim_releases, trim_currents, trim_change_next, trim_throw_at;
static bool trim_available;
static LWLock *trim_control_lock(void) { return &control; }
static void trim_error_reset(void) { if (trim_count_holds) InterruptHoldoffCount = 0; }
int OpenTransientFile(const char *path, int flags)
{
	UT_ASSERT(!control_held); UT_ASSERT(strcmp(path, filename) == 0);
	UT_ASSERT((flags & (O_CREAT | O_TRUNC | O_APPEND)) == 0);
	if (trim_open_error)
		ereport(ERROR, (errcode(ERRCODE_IO_ERROR), errmsg("injected trim open ERROR")));
	return open(path, flags, 0600);
}
ClusterControlNativeModeV2 cluster_control_native_files_mode_v2(void) { return trim_mode; }
static bool trim_required(void) { return CLOGNativeIORequired(); }
static bool trim_acquire(int pageno, ClusterControlNativeFileBorrowV2 *out)
{
	UT_ASSERT(control_held);
	if (!trim_available || pageno / 32 != binding.segment) return false;
	out->expected = binding.identity; out->private_self = (uintptr_t)out;
	trim_borrows++; return true;
}
static bool trim_current(const ClusterControlNativeFileBorrowV2 *borrow)
{
	UT_ASSERT_EQ(borrow->private_self, (uintptr_t)borrow);
	trim_currents++;
	if (trim_change_next == trim_currents)
		variables.nextXid = FullTransactionIdFromU64(U64FromFullTransactionId(variables.nextXid) + 4);
	if (trim_throw_at == trim_currents)
		ereport(ERROR, (errcode(ERRCODE_IO_ERROR), errmsg("injected selected trim failure")));
	return trim_mode == CLUSTER_NATIVE_FILES_MANAGED_V2;
}
static bool trim_release(ClusterControlNativeFileBorrowV2 *borrow)
{
	UT_ASSERT_EQ(borrow->private_self, (uintptr_t)borrow);
	trim_releases++; memset(borrow, 0, sizeof(*borrow)); return true;
}
static void trim_reset(bool is_shared, uint32 epoch, TransactionId next)
{
	trim_count_holds = false;
	managed_reset();
	ctl.PagePrecedes = page_precedes;
	ctl.clog_native_required = trim_required; ctl.clog_native_acquire = trim_acquire;
	ctl.clog_native_current = trim_current; ctl.clog_native_release = trim_release;
	ctl.clog_native_read = SimpleLruNativeClogReadV2;
	ctl.clog_native_write = SimpleLruNativeClogWriteV2;
	memset(&variables, 0, sizeof(variables));
	variables.nextXid = FullTransactionIdFromEpochAndXid(epoch, next);
	cluster_shared_config = is_shared; MyBackendType = B_STARTUP;
	trim_mode = CLUSTER_NATIVE_FILES_MANAGED_V2;
	trim_borrows = trim_releases = trim_currents = trim_change_next = trim_throw_at = 0;
	trim_available = true;
	trim_open_error = IsPostmasterEnvironment = false;
	states[0] = SLRU_PAGE_EMPTY; dirty[0] = false;
	control_held = false; InterruptHoldoffCount = 3;
	trim_count_holds = true;
}
static void trim_disk(FullTransactionId birth, const uint8 *payload)
{
	uint8 frame[8256];
	ClusterClogPageIdentityV2 observed = identities[0];

	observed.identity.logical_page = XidFromFullTransactionId(birth) / 32768;
	observed.identity.birth_full_xid = birth;
	UT_ASSERT(cluster_clog_file_page_encode_v2(&binding, &observed, payload, BLCKSZ,
		frame, sizeof(frame)));
	UT_ASSERT_EQ(pwrite(borrowed, frame, sizeof(frame), 8192 + observed.identity.logical_page * 8256), sizeof(frame));
}
static bool trim_run(void)
{
	volatile bool caught = false;
	PG_TRY(); { TrimCLOG(); }
	PG_CATCH(); { caught = true; }
	PG_END_TRY();
	return !caught;
}
static void trim_finish(void)
{
	/* Parent RED retains its original family lock. Clean the fixture only
	 * after each test has observed that failure, without hiding the debt. */
	if (control_held) { HOLD_INTERRUPTS(); LWLockRelease(&control); }
	trim_count_holds = false;
	close(borrowed);
	unlink(filename);
}
UT_TEST(nonshared_exact_full_cut_preserves_prefix_and_writes_native_frame)
{
	uint8 before[BLCKSZ], expected[BLCKSZ], payload[BLCKSZ];
	ClusterClogPageIdentityV2 observed;
	TransactionId next = 3 * 32768 + 101;
	trim_reset(false, 7, next);
	memset(before, 0xff, sizeof(before)); memcpy(expected, before, BLCKSZ);
	expected[101 / 4] &= 3; memset(expected + 101 / 4 + 1, 0, BLCKSZ - 101 / 4 - 1);
	trim_disk(FullTransactionIdFromEpochAndXid(7, 3 * 32768), before);
	UT_ASSERT(trim_run()); UT_ASSERT(!control_held && !io_held);
	UT_ASSERT_EQ(InterruptHoldoffCount, 3); UT_ASSERT(dirty[0]);
	UT_ASSERT(memcmp(page.data, expected, BLCKSZ) == 0);
	UT_ASSERT_EQ(trim_borrows, trim_releases); UT_ASSERT_EQ(inserts + writes, 0);
	LWLockAcquire(&control, LW_EXCLUSIVE);
	SimpleLruWritePage(&ctl, 0);
	LWLockRelease(&control);
	UT_ASSERT(!dirty[0]); UT_ASSERT_EQ(inserts, 1); UT_ASSERT_EQ(flushes, 1);
	UT_ASSERT_EQ(writes, 1); UT_ASSERT_EQ(queues, 0); UT_ASSERT_EQ(trim_borrows, trim_releases);
	control_held = false; io_held = true;
	UT_ASSERT(cluster_clog_storage_read_page_v2(borrowed, &binding, 3, &observed, payload, BLCKSZ));
	io_held = false;
	UT_ASSERT(memcmp(payload, expected, BLCKSZ) == 0);
	UT_ASSERT_EQ(U64FromFullTransactionId(observed.identity.birth_full_xid), (UINT64CONST(7) << 32) + 3 * 32768);
	UT_ASSERT_EQ(observed.image_lsn, 1000); UT_ASSERT_EQ(observed.identity.page_lsn, 15);
	trim_finish();
}
UT_TEST(shared_exact_cut_is_readonly_and_nonzero_suffix_stays_closed)
{
	for (unsigned fault = 0; fault < 3; fault++) {
		uint8 payload[BLCKSZ] = {1};
		trim_reset(true, 7, 3 * 32768 + 101);
		if (fault) payload[fault == 1 ? 101 / 4 : BLCKSZ - 1] = 0x40;
		trim_disk(FullTransactionIdFromEpochAndXid(7, 3 * 32768), payload);
		UT_ASSERT_EQ(trim_run(), fault == 0);
		UT_ASSERT(memcmp(page.data, payload, BLCKSZ) == 0);
		UT_ASSERT(!control_held && !io_held && !dirty[0]);
		UT_ASSERT_EQ(InterruptHoldoffCount, fault ? 0 : 3);
		UT_ASSERT_EQ(trim_borrows, trim_releases); UT_ASSERT_EQ(writes + inserts, 0);
		trim_finish();
	}
}
UT_TEST(equal_raw_page_from_other_epoch_never_trims_or_certifies_unused)
{
	for (unsigned shared_case = 0; shared_case < 2; shared_case++) {
		for (unsigned epoch = 6; epoch <= 8; epoch += 2) {
			uint8 payload[BLCKSZ] = {1};
			trim_reset(shared_case, 7, 3 * 32768 + 101);
			trim_disk(FullTransactionIdFromEpochAndXid(epoch, 3 * 32768), payload);
			UT_ASSERT(!trim_run()); UT_ASSERT(!control_held && !io_held && !dirty[0]);
			UT_ASSERT(memcmp(page.data, payload, BLCKSZ) == 0);
			UT_ASSERT_EQ(writes + inserts, 0); UT_ASSERT_EQ(trim_borrows, trim_releases);
			trim_finish();
		}
	}
}
UT_TEST(nonfirst_birth_in_the_same_raw_page_is_not_an_issuer_birth)
{
	uint8 payload[BLCKSZ] = {1};
	trim_reset(false, 7, 3 * 32768 + 101);
	trim_disk(FullTransactionIdFromEpochAndXid(7, 3 * 32768 + 4), payload);
	UT_ASSERT(!trim_run()); UT_ASSERT(!dirty[0] && !control_held);
	UT_ASSERT(memcmp(page.data, payload, BLCKSZ) == 0);
	UT_ASSERT_EQ(trim_borrows, trim_releases); trim_finish();
}
UT_TEST(changed_startup_full_cut_is_refused_after_original_read)
{
	uint8 payload[BLCKSZ];
	trim_reset(false, 7, 3 * 32768 + 101); memset(payload, 0xff, BLCKSZ);
	trim_disk(FullTransactionIdFromEpochAndXid(7, 3 * 32768), payload);
	trim_change_next = 2;
	UT_ASSERT(!trim_run()); UT_ASSERT(!dirty[0] && !control_held);
	UT_ASSERT(memcmp(page.data, payload, BLCKSZ) == 0);
	UT_ASSERT_EQ(writes + inserts, 0); UT_ASSERT_EQ(trim_borrows, trim_releases);
	trim_finish();
}
UT_TEST(native_startup_owner_and_finite_cut_precede_any_io)
{
	for (unsigned fault = 0; fault < 4; fault++) {
		trim_reset(false, 7, 3 * 32768 + 101);
		if (fault == 0) MyBackendType = B_BACKEND;
		if (fault == 1) InRecovery = true;
		if (fault == 2) variables.nextXid = FullTransactionIdFromU64(UINT64_MAX);
		if (fault == 3) variables.nextXid = FullTransactionIdFromEpochAndXid(7, 2);
		UT_ASSERT(!trim_run()); UT_ASSERT(!control_held && !dirty[0]);
		UT_ASSERT_EQ(trim_borrows + reads + inserts + writes, 0);
		trim_finish();
	}
	/* The original InitPostgres->StartupXLOG caller is also legitimate.
	 * Its later normal-processing phase and every shared standalone are not. */
	for (unsigned fault = 0; fault < 5; fault++) {
		uint8 payload[BLCKSZ] = {1};
		trim_reset(fault == 1, 7, 3 * 32768 + 101);
		MyBackendType = B_STANDALONE_BACKEND; Mode = InitProcessing;
		if (fault == 2) Mode = NormalProcessing;
		if (fault == 3) IsUnderPostmaster = true;
		if (fault == 4) IsPostmasterEnvironment = true;
		trim_disk(FullTransactionIdFromEpochAndXid(7, 3 * 32768), payload);
		UT_ASSERT_EQ(trim_run(), fault == 0); UT_ASSERT(!control_held);
		UT_ASSERT_EQ(dirty[0], fault == 0);
		if (fault) UT_ASSERT_EQ(trim_borrows + reads + writes, 0);
		trim_finish();
		IsUnderPostmaster = false;
	}
}
UT_TEST(page_boundary_has_no_io_and_page_zero_preserves_special_xids)
{
	uint8 payload[BLCKSZ], expected[BLCKSZ];
	trim_reset(false, 7, 3 * 32768);
	UT_ASSERT(trim_run()); UT_ASSERT(!control_held);
	UT_ASSERT_EQ(trim_borrows + reads + writes, 0); UT_ASSERT_EQ(InterruptHoldoffCount, 3);
	trim_finish();
	trim_reset(false, 8, 5); memset(payload, 0xff, BLCKSZ);
	memcpy(expected, payload, BLCKSZ); expected[1] = 3; memset(expected + 2, 0, BLCKSZ - 2);
	trim_disk(FullTransactionIdFromEpochAndXid(8, FirstNormalTransactionId), payload);
	UT_ASSERT(trim_run()); UT_ASSERT(!control_held && dirty[0]);
	UT_ASSERT(memcmp(page.data, expected, BLCKSZ) == 0);
	UT_ASSERT_EQ(trim_borrows, trim_releases); trim_finish();
}
UT_TEST(actual_error_zero_holdoff_releases_only_the_new_family_lock)
{
	for (unsigned fault = 1; fault <= 4; fault++) {
		trim_reset(false, 7, 3 * 32768 + 101);
		if (fault <= 3) trim_throw_at = fault; else trim_open_error = true;
		UT_ASSERT(!trim_run()); UT_ASSERT(!control_held && !io_held && !dirty[0]);
		UT_ASSERT_EQ(InterruptHoldoffCount, 0); UT_ASSERT_EQ(trim_borrows, trim_releases);
		UT_ASSERT_EQ(inserts + writes, 0); trim_finish();
	}
}
int main(void)
{
	strlcpy(directory, "/tmp/pgrac-clog-trim-XXXXXX", sizeof(directory));
	if (mkdtemp(directory) == NULL) abort();
	snprintf(filename, sizeof(filename), "%s/0000", directory);
	UT_PLAN(8);
	UT_RUN(nonshared_exact_full_cut_preserves_prefix_and_writes_native_frame);
	UT_RUN(shared_exact_cut_is_readonly_and_nonzero_suffix_stays_closed);
	UT_RUN(equal_raw_page_from_other_epoch_never_trims_or_certifies_unused);
	UT_RUN(nonfirst_birth_in_the_same_raw_page_is_not_an_issuer_birth);
	UT_RUN(changed_startup_full_cut_is_refused_after_original_read);
	UT_RUN(native_startup_owner_and_finite_cut_precede_any_io);
	UT_RUN(page_boundary_has_no_io_and_page_zero_preserves_special_xids);
	UT_RUN(actual_error_zero_holdoff_releases_only_the_new_family_lock);
	rmdir(directory); UT_DONE(); return ut_failed_count != 0;
}
