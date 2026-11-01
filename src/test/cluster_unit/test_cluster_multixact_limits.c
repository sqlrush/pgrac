/*-------------------------------------------------------------------------
 * test_cluster_multixact_limits.c
 *    Native allocation and retention boundaries for striped MultiXacts.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 * IDENTIFICATION
 *    src/test/cluster_unit/test_cluster_multixact_limits.c
 * NOTES
 *    Actual native bodies and stripe arithmetic execute. Locks, SLRU pages,
 *    authority-file reads and WAL delivery are explicit test boundaries;
 *    this is not a physical restart or VACUUM integration certification.
 *-------------------------------------------------------------------------
 */
#include "postgres.h"
#include <setjmp.h>
#include "cluster/cluster_xid_authority.h"
#include "cluster/cluster_xid_stripe_boot.h"
#ifdef MULTIXACT_TEST_SOURCE
#include MULTIXACT_TEST_SOURCE
#else
#include "../../backend/access/transam/multixact.c"
#endif
#include "../../backend/cluster/cluster_mxid_stripe.c"
#undef printf
#undef fprintf
#undef snprintf
#include "unit_test.h"
UT_DEFINE_GLOBALS();

bool cluster_shared_config = true;
bool cluster_shared_catalog = true;
bool cluster_enabled = true;
bool cluster_xid_striping = true;
int cluster_node_id;
bool InRecovery;
bool IsUnderPostmaster;
BackendType MyBackendType;
volatile uint32 CritSectionCount;
int autovacuum_multixact_freeze_max_age = 400000000;
static LWLockPadded locks[128];
LWLockPadded *MainLWLockArray = locks;
static PGPROC proc;
PGPROC *MyProc = &proc;
static MultiXactStateData state;
static SlruSharedData off_shared, mem_shared;
static char off_pages[4][BLCKSZ], mem_pages[4][BLCKSZ];
static char *off_buffers[4], *mem_buffers[4];
static bool off_dirty[4], mem_dirty[4];
static int held[128];
static LWLockMode modes[128];
static unsigned reads, writes, zeroes, wal_records, wal_flushes, deletions, signals;
static unsigned offset_truncates, authority_reads, reentries;
static bool exists, io_error, expect_error, native_valid;
static bool change_g_on_unlock, change_g_on_read, change_slot_on_read;
static bool change_floor_on_read, change_next_on_read, change_scope_on_header;
static ClusterXidStripeObs observation;
static bool empty_input, cancel_input_on_recheck, latch_on_header;
static unsigned empty_input_calls, proven_logs, unbound_logs;
static bool need_vacuum;
static bool latch_on_unlock, poison_on_unlock, move_cursor_on_observe;
static bool change_next_on_unlock, change_slot_on_unlock, change_floor_on_unlock;
static bool forget_members_on_unlock;
static int zeroed_offsets[4096];
static unsigned offset_zeroes;
static char last_detail[512];
static MultiXactId changed_next;
static MultiXactId changed_g, last_read;
static int deleted[100000];
static int earliest_page;
static int last_error, last_elevel, error_gen_lock;
static xl_multixact_truncate truncate_record;
static char wal_data[SizeOfMultiXactTruncate];
static unsigned wal_length;
static ClusterXidAuthorityHeader native;
static jmp_buf error_boundary;

void
ExceptionalCondition(const char *condition, const char *file, int line)
{
	UT_ASSERT(false);
	(void)condition;
	(void)file;
	(void)line;
	abort();
}
bool
errstart(int level, const char *domain pg_attribute_unused())
{
	if (level < ERROR && level != LOG)
		return false;
	last_elevel = level;
	last_error = 0;
	return true;
}
bool
errstart_cold(int level, const char *domain)
{
	return errstart(level, domain);
}
void
errfinish(const char *f pg_attribute_unused(), int l pg_attribute_unused(),
		  const char *fn pg_attribute_unused())
{
	if (last_elevel < ERROR) {
		if (strstr(last_detail, "NATIVE_EMPTY_BOOT_PROVEN"))
			proven_logs++;
		if (strstr(last_detail, "STRIPE_BINDING_PENDING"))
			unbound_logs++;
		return;
	}
	error_gen_lock = held[(LWLockPadded *)MultiXactGenLock - locks];
	if (expect_error)
		longjmp(error_boundary, 1);
	abort();
}
int
errcode(int code)
{
	last_error = code;
	return 0;
}
int
errmsg(const char *s pg_attribute_unused(), ...)
{
	return 0;
}
int
errmsg_internal(const char *s pg_attribute_unused(), ...)
{
	return 0;
}
int
errdetail(const char *s, ...)
{
	va_list ap;
	va_start(ap, s);
	vsnprintf(last_detail, sizeof(last_detail), s, ap);
	va_end(ap);
	return 0;
}
int
errhint(const char *s pg_attribute_unused(), ...)
{
	return 0;
}
int
errmsg_plural(const char *s pg_attribute_unused(), const char *p pg_attribute_unused(),
			  unsigned long n pg_attribute_unused(), ...)
{
	return 0;
}
int
errdetail_plural(const char *s pg_attribute_unused(), const char *p pg_attribute_unused(),
				 unsigned long n pg_attribute_unused(), ...)
{
	return 0;
}
bool
RecoveryInProgress(void)
{
	return InRecovery;
}
bool
IsTransactionState(void)
{
	return false;
}
char *
get_database_name(Oid id pg_attribute_unused())
{
	return NULL;
}
void
SendPostmasterSignal(PMSignalReason reason pg_attribute_unused())
{
	signals++;
}
void
cluster_mxid_stripe_lazy_latch(void)
{}
bool
cluster_cr_injection_armed(const char *name pg_attribute_unused(),
						   uint64 *param pg_attribute_unused())
{
	return false;
}
ClusterSemanticAdmissionResult
cluster_multixact_source_dispatch(ClusterMultiXactSourceOp op pg_attribute_unused(),
								  const ClusterMultiXactSourceRequest *r pg_attribute_unused(),
								  ClusterMultiXactSourceResult *out pg_attribute_unused())
{
	return CLUSTER_SEMANTIC_ADMISSION_OK;
}
bool
cluster_xid_authority_read(ClusterXidAuthorityHeader *out)
{
	authority_reads++;
	for (unsigned i = 0; i < lengthof(held); i++)
		UT_ASSERT_EQ(held[i], 0);
	*out = native;
	if (change_scope_on_header)
		cluster_node_id++;
	if (latch_on_header)
		cluster_mxid_stripe_latch_runtime(true, 0, 32);
	return native_valid;
}

bool
cluster_xid_authority_read_checked(ClusterXidAuthorityHeader *out, bool *from_bak)
{
	if (from_bak)
		*from_bak = false;
	return cluster_xid_authority_read(out);
}
bool ClusterXLogStartupNativeEmptyMultiXactInput(void);
bool
ClusterXLogStartupNativeEmptyMultiXactInput(void)
{
	for (unsigned i = 0; i < lengthof(held); i++)
		UT_ASSERT_EQ(held[i], 0);
	empty_input_calls++;
	return empty_input && !(cancel_input_on_recheck && empty_input_calls > 1);
}
void
cluster_xid_stripe_observe(ClusterXidStripeObs *out)
{
	*out = observation;
	if (move_cursor_on_observe) {
		state.nextMXact = 2;
		move_cursor_on_observe = false;
	}
}

bool
LWLockAcquire(LWLock *lock, LWLockMode mode)
{
	int i = (LWLockPadded *)lock - locks;
	UT_ASSERT(i >= 0 && i < 128 && held[i] == 0);
	held[i]++;
	modes[i] = mode;
	if (lock == MultiXactGenLock)
		reentries++;
	return true;
}
void
LWLockRelease(LWLock *lock)
{
	int i = (LWLockPadded *)lock - locks;
	UT_ASSERT(i >= 0 && i < 128 && held[i] == 1);
	held[i]--;
	if (lock == MultiXactGenLock && latch_on_unlock) {
		latch_on_unlock = false;
		cluster_mxid_stripe_latch_runtime(true, 0, 32);
	}
	if (lock == MultiXactGenLock && poison_on_unlock) {
		poison_on_unlock = false;
		cluster_mxid_stripe_latch_poisoned();
	}
	if (lock == MultiXactGenLock && change_g_on_unlock) {
		change_g_on_unlock = false;
		state.oldestMultiXactId = changed_g;
	}
	if (lock == MultiXactGenLock && change_next_on_unlock) {
		change_next_on_unlock = false;
		state.nextMXact = changed_next;
	}
	if (lock == MultiXactGenLock && change_slot_on_unlock) {
		change_slot_on_unlock = false;
		cluster_node_id++;
	}
	if (lock == MultiXactGenLock && change_floor_on_unlock) {
		change_floor_on_unlock = false;
		mxid_stripe_runtime.floor_mxid += CLUSTER_MXID_STRIDE;
	}
	if (lock == MultiXactGenLock && forget_members_on_unlock) {
		forget_members_on_unlock = false;
		state.oldestOffsetKnown = false;
	}
}
bool
LWLockHeldByMe(LWLock *lock)
{
	return held[(LWLockPadded *)lock - locks] != 0;
}
int
SimpleLruReadPage(SlruCtl ctl, int page, bool write_ok pg_attribute_unused(), TransactionId xid)
{
	UT_ASSERT(ctl == MultiXactOffsetCtl || ctl == MultiXactMemberCtl);
	reads++;
	last_read = xid;
	if (change_g_on_read) {
		state.oldestMultiXactId = changed_g;
		change_g_on_read = false;
	}
	if (change_slot_on_read) {
		cluster_node_id++;
		change_slot_on_read = false;
	}
	if (change_floor_on_read) {
		mxid_stripe_runtime.floor_mxid += CLUSTER_MXID_STRIDE;
		change_floor_on_read = false;
	}
	if (change_next_on_read) {
		state.nextMXact = changed_next;
		change_next_on_read = false;
	}
	if (io_error)
		ereport(ERROR, (errcode(ERRCODE_IO_ERROR), errmsg("fixture SLRU read failure")));
	return page & 3;
}
int
SimpleLruReadPage_ReadOnly(SlruCtl ctl, int page, TransactionId xid)
{
	LWLockAcquire(MultiXactOffsetSLRULock, LW_SHARED);
	return SimpleLruReadPage(ctl, page, false, xid);
}
int
SimpleLruZeroPage(SlruCtl ctl, int page)
{
	zeroes++;
	if (ctl == MultiXactOffsetCtl) {
		if (offset_zeroes < lengthof(zeroed_offsets))
			zeroed_offsets[offset_zeroes] = page;
		offset_zeroes++;
	}
	memset(ctl->shared->page_buffer[page & 3], 0, BLCKSZ);
	return page & 3;
}
void
SimpleLruWriteAll(SlruCtl ctl pg_attribute_unused(), bool r pg_attribute_unused())
{
	writes++;
}
void
SimpleLruWritePage(SlruCtl ctl, int slot)
{
	writes++;
	ctl->shared->page_dirty[slot] = false;
}
bool
SimpleLruDoesPhysicalPageExist(SlruCtl ctl pg_attribute_unused(), int p pg_attribute_unused())
{
	return exists;
}
bool
SlruScanDirectory(SlruCtl ctl, SlruScanCallback callback, void *arg)
{
	return callback(ctl, "0000", earliest_page, arg);
}
void
SlruDeleteSegment(SlruCtl ctl, int seg)
{
	UT_ASSERT(ctl == MultiXactMemberCtl);
	UT_ASSERT(wal_flushes > 0 && CritSectionCount > 0);
	UT_ASSERT(deletions < lengthof(deleted));
	deleted[deletions++] = seg;
}
void
SimpleLruTruncate(SlruCtl ctl, int page pg_attribute_unused())
{
	UT_ASSERT(ctl == MultiXactOffsetCtl);
	UT_ASSERT(wal_flushes > 0 && CritSectionCount > 0);
	offset_truncates++;
}
void
XLogBeginInsert(void)
{
	wal_length = 0;
}
void
XLogRegisterData(char *data, uint32 len)
{
	UT_ASSERT(len <= sizeof(wal_data));
	memcpy(wal_data, data, len);
	wal_length = len;
}
XLogRecPtr
XLogInsert(RmgrId id, uint8 info)
{
	UT_ASSERT_EQ(id, RM_MULTIXACT_ID);
	wal_records++;
	if (info == XLOG_MULTIXACT_TRUNCATE_ID) {
		UT_ASSERT_EQ(wal_length, SizeOfMultiXactTruncate);
		memcpy(&truncate_record, wal_data, wal_length);
	}
	return 100;
}
void
XLogFlush(XLogRecPtr ptr pg_attribute_unused())
{
	wal_flushes++;
}

static void
put_offset(MultiXactId id, MultiXactOffset offset)
{
	((MultiXactOffset *)off_pages[MultiXactIdToOffsetPage(id) & 3])[MultiXactIdToOffsetEntry(id)]
		= offset;
}
static void
reset_fixture(int slot, MultiXactId floor, MultiXactId lower, MultiXactId next,
			  MultiXactOffset offset)
{
	memset(&state, 0, sizeof(state));
	memset(&proc, 0, sizeof(proc));
	memset(&off_shared, 0, sizeof(off_shared));
	memset(&mem_shared, 0, sizeof(mem_shared));
	memset(off_pages, 0, sizeof(off_pages));
	memset(mem_pages, 0, sizeof(mem_pages));
	memset(held, 0, sizeof(held));
	memset(&mxid_stripe_runtime, 0, sizeof(mxid_stripe_runtime));
	for (unsigned i = 0; i < 4; i++) {
		off_buffers[i] = off_pages[i];
		mem_buffers[i] = mem_pages[i];
	}
	MultiXactState = &state;
	MultiXactOffsetCtl->shared = &off_shared;
	MultiXactMemberCtl->shared = &mem_shared;
	off_shared.ControlLock = MultiXactOffsetSLRULock;
	mem_shared.ControlLock = MultiXactMemberSLRULock;
	off_shared.page_buffer = off_buffers;
	mem_shared.page_buffer = mem_buffers;
	off_shared.page_dirty = off_dirty;
	mem_shared.page_dirty = mem_dirty;
	MultiXactOffsetCtl->PagePrecedes = MultiXactOffsetPagePrecedes;
	MultiXactMemberCtl->PagePrecedes = MultiXactMemberPagePrecedes;
	state.nextMXact = next;
	state.oldestMultiXactId = lower;
	state.nextOffset = offset;
	state.oldestOffsetKnown = true;
	state.oldestOffset = 19;
	state.offsetStopLimit = UINT32_MAX - 5000000;
	state.finishedStartup = true;
	state.oldestMultiXactDB = 5;
	state.multiVacLimit = next + 1000;
	state.multiWarnLimit = next + 2000;
	state.multiStopLimit = next + 3000;
	state.multiWrapLimit = next + 4000;
	cluster_shared_config = cluster_enabled = cluster_xid_striping = true;
	cluster_node_id = slot;
	cluster_mxid_stripe_latch_runtime(true, slot, floor);
	InRecovery = IsUnderPostmaster = false;
	MyBackendType = B_BACKEND;
	reads = writes = zeroes = wal_records = wal_flushes = deletions = signals = 0;
	offset_truncates = authority_reads = reentries = 0;
	exists = native_valid = true;
	io_error = expect_error = false;
	change_g_on_unlock = change_g_on_read = change_slot_on_read = false;
	change_floor_on_read = change_next_on_read = change_scope_on_header = false;
	earliest_page = 0;
	CritSectionCount = 0;
	last_error = last_elevel = error_gen_lock = 0;
	memset(&native, 0, sizeof(native));
	native.magic = CLUSTER_XID_AUTHORITY_MAGIC;
	native.version = CLUSTER_XID_AUTHORITY_VERSION;
	native.flags = CLUSTER_XID_AUTHORITY_FLAG_SEALED;
	native.next_multi = FirstMultiXactId;
	memset(&observation, 0, sizeof(observation));
	observation.disk_state = CLUSTER_XID_STRIPE_DISK_ABSENT;
	observation.mxid_disk_state = CLUSTER_MXID_DISK_ABSENT;
	empty_input = cancel_input_on_recheck = latch_on_header = false;
	latch_on_unlock = poison_on_unlock = move_cursor_on_observe = false;
	change_next_on_unlock = change_slot_on_unlock = change_floor_on_unlock = false;
	forget_members_on_unlock = false;
	offset_zeroes = 0;
	empty_input_calls = proven_logs = unbound_logs = 0;
	need_vacuum = true;
	last_detail[0] = '\0';
}
static bool
limits_error(bool startup)
{
	expect_error = true;
	if (setjmp(error_boundary) == 0) {
		need_vacuum = SetOffsetVacuumLimit(startup);
		expect_error = false;
		return false;
	}
	expect_error = false;
	return true;
}
static bool
allocate_error(MultiXactId *id)
{
	MultiXactOffset offset = 9876;
	expect_error = true;
	if (setjmp(error_boundary) == 0) {
		*id = GetNewMultiXactId(2, &offset);
		expect_error = false;
		END_CRIT_SECTION();
		return false;
	}
	expect_error = false;
	return true;
}

UT_TEST(foreign_lower_uses_first_exact_local_id)
{
	for (int slot = 0; slot < 4; slot++) {
		MultiXactId local;
		reset_fixture(slot, 32, 37, 100, 1000);
		local = cluster_mxid_next_striped(37, slot);
		put_offset(local, 500 + slot);
		UT_ASSERT(!limits_error(true));
		UT_ASSERT_EQ(state.oldestOffset, 500 + slot);
		UT_ASSERT_EQ(state.oldestMultiXactId, 37);
		UT_ASSERT_EQ(last_read, local);
		UT_ASSERT_EQ(writes + zeroes, 0);
	}
}
UT_TEST(local_empty_accepts_zero_and_lower_beyond_next)
{
	const MultiXactId lower[] = { 33, 49, 80 };
	for (unsigned i = 0; i < lengthof(lower); i++) {
		reset_fixture(0, 32, lower[i], 33, 0);
		UT_ASSERT(!limits_error(true));
		UT_ASSERT(state.oldestOffsetKnown);
		UT_ASSERT_EQ(state.oldestOffset, 0);
		UT_ASSERT_EQ(reads, 0);
	}
}
UT_TEST(native_prefix_requires_qualified_empty_input)
{
	for (int invalid = 0; invalid < 4; invalid++) {
		reset_fixture(0, 32, 1, 49, 99);
		put_offset(32, 7);
		if (invalid == 1)
			native.next_multi = 2;
		if (invalid == 2)
			native.flags = 0;
		if (invalid == 3)
			native_valid = false;
		UT_ASSERT_EQ(limits_error(true), invalid != 0);
		if (invalid == 0)
			UT_ASSERT_EQ(state.oldestOffset, 7);
		UT_ASSERT_EQ(zeroes + writes, 0);
	}
}
UT_TEST(runtime_unknown_retains_proven_limit_and_startup_refuses)
{
	MultiXactOffset stop;
	reset_fixture(0, 32, 37, 100, 1000);
	put_offset(64, 90);
	stop = state.offsetStopLimit;
	UT_ASSERT(!limits_error(false));
	UT_ASSERT_EQ(state.oldestOffset, 19);
	UT_ASSERT_EQ(state.offsetStopLimit, stop);
	UT_ASSERT_EQ(last_read, 48);
	UT_ASSERT_EQ(reads, 1);
	UT_ASSERT(limits_error(true));
}
UT_TEST(modular_id_window_and_member_space_are_distinct)
{
	reset_fixture(0, UINT32_MAX - 31, UINT32_MAX - 14, 17, UINT32_MAX - 9);
	put_offset(UINT32_MAX - 15, 7);
	put_offset(16, UINT32_MAX - 19);
	UT_ASSERT(!limits_error(true));
	UT_ASSERT_EQ(last_read, 16);
	UT_ASSERT_EQ(state.oldestOffset, UINT32_MAX - 19);
	reset_fixture(0, 32, 32 + CLUSTER_MXID_HALFSPACE, 64, 100);
	UT_ASSERT(limits_error(true));
	UT_ASSERT_EQ(reads, 0);
}
UT_TEST(ordinary_native_dense_limits_remain_unchanged)
{
	reset_fixture(0, 32, 37, 100, 1000);
	cluster_shared_config = false;
	put_offset(37, 123);
	UT_ASSERT(!limits_error(false));
	UT_ASSERT_EQ(state.oldestOffset, 123);
	UT_ASSERT_EQ(last_read, 37);
	UT_ASSERT_EQ(authority_reads, 0);
}
UT_TEST(vacuum_limit_recalculation_keeps_local_member_protection)
{
	reset_fixture(2, 32, 34, 100, 1000);
	put_offset(50, 510);
	SetMultiXactIdLimit(37, 5, false);
	UT_ASSERT_EQ(state.oldestMultiXactId, 37);
	UT_ASSERT_EQ(state.oldestOffset, 510);
	UT_ASSERT(state.oldestOffsetKnown);
}
UT_TEST(truncation_uses_two_local_endpoints_and_preserves_live_segment)
{
	const uint32 span = MULTIXACT_MEMBERS_PER_PAGE * SLRU_PAGES_PER_SEGMENT;
	for (int wrap = 0; wrap < 2; wrap++) {
		MultiXactOffset from = wrap ? UINT32_MAX - span : span;
		MultiXactOffset to = span * 3;
		reset_fixture(0, 32, 33, 100, span * 5);
		put_offset(48, from);
		put_offset(80, to);
		TruncateMultiXact(65, 5);
		UT_ASSERT_EQ(wal_records, 1);
		UT_ASSERT_EQ(wal_flushes, 1);
		UT_ASSERT_EQ(truncate_record.startTruncOff, 33);
		UT_ASSERT_EQ(truncate_record.endTruncOff, 65);
		UT_ASSERT_EQ(truncate_record.startTruncMemb, from);
		UT_ASSERT_EQ(truncate_record.endTruncMemb, to);
		for (unsigned i = 0; i < deletions; i++)
			UT_ASSERT(deleted[i] != MXOffsetToMemberSegment(to));
		UT_ASSERT_EQ(offset_truncates, 1);
		UT_ASSERT_EQ(CritSectionCount, 0);
	}
}
UT_TEST(unknown_endpoint_refuses_all_truncation)
{
	for (int absent = 0; absent < 2; absent++) {
		reset_fixture(0, 32, 48, 100, 1000);
		put_offset(48, absent == 0 ? 0 : 100);
		put_offset(80, absent == 1 ? 0 : 500);
		TruncateMultiXact(80, 5);
		UT_ASSERT_EQ(wal_records + deletions + offset_truncates, 0);
		UT_ASSERT_EQ(state.oldestMultiXactId, 48);
	}
}
UT_TEST(allocation_catches_up_to_current_lower_bound)
{
	MultiXactId id = 0;
	reset_fixture(0, 32, 100, 33, 500);
	UT_ASSERT(!allocate_error(&id));
	UT_ASSERT_EQ(id, 112);
	UT_ASSERT_EQ(state.nextMXact, 113);
	UT_ASSERT_EQ(state.nextOffset, 502);
	UT_ASSERT_EQ(zeroes + wal_records, 0);
}
UT_TEST(allocation_rechecks_lower_bound_after_warning_unlock)
{
	MultiXactId id = 0;
	for (int move = 0; move < 2; move++) {
		reset_fixture(0, 32, 32, 33, 500);
		state.multiVacLimit = 32;
		changed_g = move ? 100 : 32;
		change_g_on_unlock = true;
		UT_ASSERT(!allocate_error(&id));
		UT_ASSERT(reentries >= 2);
		UT_ASSERT_EQ(id, move ? 112 : 48);
		UT_ASSERT_EQ(state.nextMXact, id + 1);
		UT_ASSERT_EQ(state.nextOffset, 502);
	}
}

UT_TEST(allocation_both_candidates_use_native_order_for_every_stripe)
{
	static const struct {
		MultiXactId floor, next, lower;
		MultiXactId expected[16];
	} cases[] = {
		{32, 100, 37, {112,113,114,115,100,101,102,103,104,105,106,107,108,109,110,111}},
		{32, 100, 100, {112,113,114,115,100,101,102,103,104,105,106,107,108,109,110,111}},
		{32, 33, 100, {112,113,114,115,100,101,102,103,104,105,106,107,108,109,110,111}},
		{UINT32_MAX - 63, UINT32_MAX - 31, 17,
		 {32,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31}}
	};
	for (unsigned c = 0; c < lengthof(cases); c++) {
		for (int warning = 0; warning < 2; warning++) {
			for (int slot = 0; slot < 16; slot++) {
				MultiXactId id = 0;
				reset_fixture(slot, cases[c].floor, cases[c].lower, cases[c].next, 500);
				if (warning)
					state.multiVacLimit = cases[c].lower;
				UT_ASSERT(!allocate_error(&id));
				UT_ASSERT_EQ(id, cases[c].expected[slot]);
				UT_ASSERT_EQ(id % 16, slot);
				UT_ASSERT_EQ(state.nextMXact, id + 1);
				UT_ASSERT_EQ(state.nextOffset, 502);
			}
		}
	}
}

UT_TEST(warning_rechecks_counter_identity_and_member_protection)
{
	for (int fault = 0; fault < 6; fault++) {
		MultiXactId id = 0;
		reset_fixture(0, 32, 32, 33, 500);
		state.multiVacLimit = 32;
		changed_next = 100;
		changed_g = 129;
		change_next_on_unlock = fault < 2;
		change_g_on_unlock = fault == 1;
		change_floor_on_unlock = fault == 2;
		change_slot_on_unlock = fault == 3;
		poison_on_unlock = fault == 4;
		forget_members_on_unlock = fault == 5;
		UT_ASSERT_EQ(allocate_error(&id), fault >= 2);
		if (fault < 2) {
			UT_ASSERT_EQ(id, fault ? 144 : 112);
			UT_ASSERT_EQ(state.nextOffset, 502);
		} else {
			/* Poison removes the floor, retaining the original reacquired
			 * half-space refusal and normal ERROR lock cleanup. */
			UT_ASSERT_EQ(last_error, fault == 4 ? ERRCODE_CLUSTER_MXID_HALFSPACE_LIMIT
												  : ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE);
			UT_ASSERT_EQ(error_gen_lock, fault == 4 ? 1 : 0);
			UT_ASSERT_EQ(state.nextMXact, 33);
			UT_ASSERT_EQ(state.nextOffset, 500);
			UT_ASSERT_EQ(zeroes + wal_records, 0);
		}
	}
}

UT_TEST(shared_member_stop_precedes_any_offset_extension)
{
	static const MultiXactOffset starts[] = {500, 0, UINT32_MAX};
	static const MultiXactOffset stops[] = {501, 2, 1};
	for (unsigned i = 0; i < lengthof(starts); i++) {
		MultiXactId id = 0;
		reset_fixture(15, 32, 2047, 2047, starts[i]);
		state.offsetStopLimit = stops[i];
		UT_ASSERT(allocate_error(&id));
		UT_ASSERT_EQ(last_error, ERRCODE_PROGRAM_LIMIT_EXCEEDED);
		UT_ASSERT_EQ(error_gen_lock, 0);
		UT_ASSERT_EQ(state.nextMXact, 2047);
		UT_ASSERT_EQ(state.nextOffset, starts[i]);
		UT_ASSERT_EQ(zeroes + wal_records, 0);
		UT_ASSERT_EQ(signals, 1);
	}
}

UT_TEST(gap_skips_truncated_pages_and_keeps_prepared_next_page)
{
	MultiXactId id = 0;
	reset_fixture(0, 32, 8193, 33, 500);
	put_offset(48, 777);
	UT_ASSERT(!allocate_error(&id));
	UT_ASSERT_EQ(id, 8208);
	UT_ASSERT_EQ(offset_zeroes, 1);
	UT_ASSERT_EQ(zeroed_offsets[0], 4);
	/* A different fixture page aliases the four-slot cache: check the actual
	 * already prepared page separately, not a claim of disk persistence. */
	for (int warning = 0; warning < 2; warning++) {
		reset_fixture(1, 32, 2000, 2048, 500);
		put_offset(2048, 499);
		if (warning)
			state.multiVacLimit = 2000;
		UT_ASSERT(!allocate_error(&id));
		UT_ASSERT_EQ(id, 2049);
		UT_ASSERT_EQ(offset_zeroes, 0);
		UT_ASSERT_EQ(((MultiXactOffset *)off_pages[1])[0], 499);
	}
}

UT_TEST(qualified_empty_floor_gap_extends_each_new_page_once)
{
	MultiXactId id = 0;
	reset_fixture(3, 8193, FirstMultiXactId, FirstMultiXactId, 500);
	UT_ASSERT(!limits_error(true));
	UT_ASSERT_EQ(state.oldestOffset, 500);
	UT_ASSERT(!allocate_error(&id));
	UT_ASSERT_EQ(id, 8195);
	UT_ASSERT_EQ(offset_zeroes, 4);
	for (unsigned i = 0; i < offset_zeroes; i++)
		UT_ASSERT_EQ(zeroed_offsets[i], i + 1);
	UT_ASSERT(!allocate_error(&id));
	UT_ASSERT_EQ(id, 8211);
	UT_ASSERT_EQ(offset_zeroes, 4);
}

UT_TEST(allocation_does_not_reinterpret_an_ambiguous_id_boundary)
{
	for (int input = 0; input < 2; input++) {
		MultiXactId id = 0;
		reset_fixture(0, 32, 32, 33, 500);
		if (input)
			state.nextMXact = 32 + CLUSTER_MXID_HALFSPACE + 16;
		else
			state.oldestMultiXactId = 32 + CLUSTER_MXID_HALFSPACE + 16;
		UT_ASSERT(allocate_error(&id));
		UT_ASSERT_EQ(zeroes + wal_records, 0);
		UT_ASSERT_EQ(state.nextOffset, 500);
	}
}
UT_TEST(unproven_limit_blocks_maintenance_and_allocation_before_normalization)
{
	MultiXactId id = 0;
	reset_fixture(0, 32, 32, 0, 1000);
	state.oldestOffsetKnown = false;
	UT_ASSERT(allocate_error(&id));
	UT_ASSERT_EQ(last_error, ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE);
	UT_ASSERT_EQ(state.nextMXact, 0);
	UT_ASSERT_EQ(state.nextOffset, 1000);
	UT_ASSERT_EQ(zeroes + wal_records, 0);
	reset_fixture(0, 32, 33, 100, 1000);
	state.oldestOffsetKnown = false;
	UT_ASSERT(limits_error(false));
	UT_ASSERT_EQ(last_error, ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE);
	UT_ASSERT(!state.oldestOffsetKnown);
}
UT_TEST(concurrent_cut_or_origin_change_cannot_install_new_limit)
{
	for (int origin = 0; origin < 2; origin++) {
		MultiXactOffset stop;
		reset_fixture(0, 32, 37, 100, 1000);
		put_offset(48, 700);
		stop = state.offsetStopLimit;
		changed_g = 65;
		change_g_on_read = !origin;
		change_slot_on_read = origin;
		(void)limits_error(false);
		UT_ASSERT_EQ(state.oldestOffset, 19);
		UT_ASSERT_EQ(state.offsetStopLimit, stop);
		UT_ASSERT_EQ(wal_records + deletions, 0);
	}
}

UT_TEST(invalid_native_binding_and_ambiguous_id_windows_are_refused)
{
	for (int fault = 0; fault < 8; fault++) {
		reset_fixture(0, 32, 1, 49, 99);
		put_offset(32, 7);
		if (fault == 0)
			native.version++;
		else if (fault == 1)
			cluster_mxid_stripe_latch_runtime(false, 0, 32);
		else if (fault == 2)
			cluster_mxid_stripe_latch_poisoned();
		else if (fault == 3)
			cluster_node_id = CLUSTER_MXID_STRIDE;
		else if (fault == 4)
			state.oldestMultiXactId = 0;
		else if (fault == 5)
			state.nextMXact = 32 + CLUSTER_MXID_HALFSPACE;
		else if (fault == 6)
			state.oldestMultiXactId = 2;
		else
			change_scope_on_header = true;
		UT_ASSERT(limits_error(true));
		UT_ASSERT_EQ(last_error, ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE);
		UT_ASSERT_EQ(writes + zeroes, 0);
		UT_ASSERT_EQ(state.oldestOffset, 19);
	}
}

UT_TEST(missing_exact_page_keeps_runtime_limit_and_io_error_propagates)
{
	for (int startup = 0; startup < 2; startup++) {
		reset_fixture(0, 32, 37, 100, 1000);
		exists = false;
		UT_ASSERT_EQ(limits_error(startup), startup != 0);
		UT_ASSERT_EQ(state.oldestOffset, 19);
		UT_ASSERT_EQ(reads, 0);
		UT_ASSERT_EQ(writes, startup ? 0 : 2);
		reset_fixture(0, 32, 37, 100, 1000);
		io_error = true;
		UT_ASSERT(limits_error(startup));
		UT_ASSERT_EQ(last_error, ERRCODE_IO_ERROR);
		UT_ASSERT_EQ(state.oldestOffset, 19);
		UT_ASSERT_EQ(wal_records + deletions, 0);
	}
}

UT_TEST(all_origins_keep_member_ring_bounds_without_foreign_offsets)
{
	const MultiXactId floors[] = { 32, UINT32_MAX - 63 };
	const uint32 span = MULTIXACT_MEMBERS_PER_PAGE * SLRU_PAGES_PER_SEGMENT;
	for (unsigned f = 0; f < lengthof(floors); f++) {
		for (int slot = 0; slot < 4; slot++) {
			for (unsigned delta = 0; delta < 64; delta++) {
				MultiXactId lower = floors[f] + delta;
				MultiXactId local = cluster_mxid_next_striped(lower, slot);
				MultiXactOffset expected = UINT32_MAX - span + delta;
				reset_fixture(slot, floors[f], lower, floors[f] + 97, span * 2);
				put_offset(lower, 1);
				put_offset(local, expected);
				UT_ASSERT(!limits_error(true));
				UT_ASSERT_EQ(state.oldestOffset, expected);
				UT_ASSERT_EQ(state.offsetStopLimit, expected - expected % span - span);
				UT_ASSERT_EQ(last_read, local);
			}
		}
	}
}

UT_TEST(truncation_rechecks_cut_origin_and_next_after_endpoint_reads)
{
	for (int fault = 0; fault < 4; fault++) {
		reset_fixture(0, 32, 33, 100, 1000);
		put_offset(48, 100);
		put_offset(80, 500);
		changed_g = 49;
		changed_next = 81;
		change_g_on_read = fault == 0;
		change_slot_on_read = fault == 1;
		change_floor_on_read = fault == 2;
		change_next_on_read = fault == 3;
		TruncateMultiXact(65, 5);
		UT_ASSERT_EQ(wal_records + deletions + offset_truncates, 0);
	}
}

UT_TEST(empty_endpoint_can_use_ring_zero_but_cut_cannot_exceed_next)
{
	reset_fixture(0, 32, 33, 81, 0);
	put_offset(48, UINT32_MAX - 1000);
	TruncateMultiXact(81, 5);
	UT_ASSERT_EQ(wal_records, 1);
	UT_ASSERT_EQ(truncate_record.endTruncMemb, 0);
	reset_fixture(0, 32, 33, 81, 0);
	put_offset(48, 100);
	TruncateMultiXact(82, 5);
	UT_ASSERT_EQ(wal_records + deletions + offset_truncates, 0);
	UT_ASSERT_EQ(state.oldestMultiXactId, 33);
}

UT_TEST(retention_uses_read_snapshot_only_when_next_does_not_regress)
{
	for (int change = 0; change < 3; change++) {
		reset_fixture(0, 32, 37, 100, 1000);
		put_offset(48, 500);
		changed_next = change == 0 ? 116 : change == 1 ? 84 : 32 + CLUSTER_MXID_HALFSPACE;
		change_next_on_read = true;
		UT_ASSERT(!limits_error(false));
		UT_ASSERT_EQ(state.oldestOffset, change == 0 ? 500 : 19);
	}
}

UT_TEST(reservation_gap_keeps_old_limit_then_published_members_advance)
{
	MultiXactId id = 0;
	MultiXactMember members[]
		= { { 100, MultiXactStatusForKeyShare }, { 116, MultiXactStatusForKeyShare } };
	reset_fixture(0, 32, 32, 33, 500);
	UT_ASSERT(!allocate_error(&id));
	UT_ASSERT_EQ(id, 48);
	UT_ASSERT_EQ(state.nextMXact, 49);
	state.oldestMultiXactId = 48;
	UT_ASSERT(!limits_error(false));
	UT_ASSERT_EQ(state.oldestOffset, 19);
	TruncateMultiXact(49, 5);
	UT_ASSERT_EQ(wal_records + deletions + offset_truncates, 0);
	RecordNewMultiXact(id, 500, lengthof(members), members);
	UT_ASSERT(!limits_error(false));
	UT_ASSERT_EQ(state.oldestOffset, 500);
	UT_ASSERT_EQ(*(TransactionId *)(mem_pages[0] + MXOffsetToMemberOffset(500)), 100);
	UT_ASSERT_EQ(*(TransactionId *)(mem_pages[0] + MXOffsetToMemberOffset(501)), 116);
}

static void
empty_boot_fixture(void)
{
	reset_fixture(0, 32, 1, 1, 0);
	cluster_mxid_stripe_latch_runtime(false, 0, 0);
	state.oldestOffsetKnown = false;
	empty_input = true;
	native.flags |= CLUSTER_XID_AUTHORITY_FLAG_CLUSTER_ERA;
}
UT_TEST(empty_boot_installs_the_same_bound_as_striped_empty)
{
	MultiXactOffset empty_stop;
	empty_boot_fixture();
	UT_ASSERT(!limits_error(true));
	UT_ASSERT(state.oldestOffsetKnown);
	UT_ASSERT_EQ(state.oldestOffset, 0);
	empty_stop = state.offsetStopLimit;
	UT_ASSERT_EQ(cluster_mxid_allocation_slot(), -1);
	UT_ASSERT_EQ(cluster_mxid_stripe_floor(), 0);
	UT_ASSERT_EQ(reads + writes + zeroes + wal_records, 0);
	UT_ASSERT_EQ(proven_logs, 1);
	UT_ASSERT(!need_vacuum);
	UT_ASSERT_EQ(state.nextMXact, 1);
	UT_ASSERT_EQ(state.nextOffset, 0);
	cluster_mxid_stripe_latch_runtime(true, 0, 32);
	UT_ASSERT(!limits_error(false));
	UT_ASSERT_EQ(state.oldestOffset, 0);
	UT_ASSERT_EQ(state.offsetStopLimit, empty_stop);
	UT_ASSERT_EQ(proven_logs, 1);
}
UT_TEST(empty_boot_authority_must_be_the_exact_sealed_native_image)
{
	for (int fault = 0; fault < 8; fault++) {
		empty_boot_fixture();
		switch (fault) {
		case 0:
			native_valid = false;
			break;
		case 1:
			native.magic++;
			break;
		case 2:
			native.version++;
			break;
		case 3:
			native.reserved = 1;
			break;
		case 4:
			native.flags &= ~CLUSTER_XID_AUTHORITY_FLAG_SEALED;
			break;
		case 5:
			native.flags &= ~CLUSTER_XID_AUTHORITY_FLAG_CLUSTER_ERA;
			break;
		case 6:
			native.flags |= 0x8000;
			break;
		case 7:
			native.next_multi = 2;
			break;
		}
		UT_ASSERT(limits_error(true));
		UT_ASSERT_EQ(last_elevel, FATAL);
		UT_ASSERT_EQ(last_error, ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE);
		UT_ASSERT(!state.oldestOffsetKnown);
		UT_ASSERT_EQ(writes + zeroes + wal_records + proven_logs, 0);
	}
}
UT_TEST(empty_boot_rejects_every_unproven_activation_observation)
{
	for (int fault = 0; fault < 9; fault++) {
		empty_boot_fixture();
		switch (fault) {
		case 0:
			observation.disk_state = CLUSTER_XID_STRIPE_DISK_UNKNOWN;
			break;
		case 1:
			observation.disk_state = CLUSTER_XID_STRIPE_DISK_CORRUPT;
			break;
		case 2:
			observation.disk_state = CLUSTER_XID_STRIPE_DISK_PUBLISHED;
			break;
		case 3:
			observation.mxid_disk_state = CLUSTER_MXID_DISK_UNKNOWN;
			break;
		case 4:
			observation.mxid_disk_state = CLUSTER_MXID_DISK_POISONED;
			break;
		case 5:
			observation.mxid_disk_state = CLUSTER_MXID_DISK_PUBLISHED;
			break;
		case 6:
			observation.activated_floor_full = 32;
			break;
		case 7:
			observation.activated_mxid_floor = 32;
			break;
		case 8:
			cluster_mxid_stripe_latch_poisoned();
			break;
		}
		UT_ASSERT(limits_error(true));
		UT_ASSERT(!state.oldestOffsetKnown);
		UT_ASSERT_EQ(writes + zeroes + wal_records + proven_logs, 0);
	}
}
UT_TEST(empty_boot_rechecks_original_input_and_native_cursor)
{
	for (int fault = 0; fault < 6; fault++) {
		empty_boot_fixture();
		if (fault == 0)
			empty_input = false;
		if (fault == 1)
			state.oldestMultiXactId = 2;
		if (fault == 2)
			state.nextMXact = 2;
		if (fault == 3)
			state.nextOffset = 1;
		if (fault == 4)
			cancel_input_on_recheck = true;
		if (fault == 5)
			move_cursor_on_observe = true;
		UT_ASSERT(limits_error(true));
		UT_ASSERT(!state.oldestOffsetKnown);
		UT_ASSERT_EQ(writes + zeroes + wal_records + proven_logs, 0);
		if (fault == 0)
			UT_ASSERT(strstr(last_detail, "NATIVE_EMPTY_BOOT_INPUT_UNPROVEN") == NULL);
	}
}
UT_TEST(empty_boot_known_does_not_authorize_unbound_allocation)
{
	MultiXactId id = 0;
	for (int fault = 0; fault < 4; fault++) {
		id = 0;
		empty_boot_fixture();
		UT_ASSERT(!limits_error(true));
		if (fault)
			state.multiVacLimit = 1;
		latch_on_unlock = fault == 2;
		poison_on_unlock = fault == 3;
		UT_ASSERT(allocate_error(&id));
		UT_ASSERT_EQ(last_error, ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE);
		UT_ASSERT_EQ(state.nextMXact, 1);
		UT_ASSERT_EQ(state.nextOffset, 0);
		UT_ASSERT_EQ(zeroes + wal_records, 0);
		UT_ASSERT_EQ(error_gen_lock, 0);
	}
	empty_boot_fixture();
	cluster_mxid_stripe_latch_poisoned();
	UT_ASSERT(allocate_error(&id));
	UT_ASSERT_EQ(last_error, ERRCODE_DATA_CORRUPTED);
	UT_ASSERT_EQ(zeroes + wal_records, 0);
}
UT_TEST(empty_boot_diagnostics_do_not_outlive_the_selected_scope)
{
	ClusterMxidLocalScope scope;

	empty_boot_fixture();
	cancel_input_on_recheck = true;
	UT_ASSERT(limits_error(true));
	UT_ASSERT(strstr(last_detail, "LOCAL_ENDPOINT_UNPROVEN") != NULL);
	UT_ASSERT(strstr(last_detail, "NATIVE_EMPTY_BOOT_INPUT_UNPROVEN") == NULL);
	UT_ASSERT(!state.oldestOffsetKnown);
	UT_ASSERT_EQ(writes + zeroes + wal_records + proven_logs, 0);

	empty_boot_fixture();
	latch_on_header = true;
	scope = cluster_mxid_local_scope(FirstMultiXactId, true);
	UT_ASSERT_EQ(scope.source, CLUSTER_MXID_SCOPE_STRIPED);
	UT_ASSERT(scope.reason == NULL);
	UT_ASSERT(cluster_mxid_local_scope_current(&scope));
	UT_ASSERT_EQ(writes + zeroes + wal_records + proven_logs, 0);
}
UT_TEST(empty_boot_runtime_cannot_recreate_proof_or_truncate)
{
	unsigned before;
	MultiXactOffset stop;
	empty_boot_fixture();
	UT_ASSERT(!limits_error(true));
	before = empty_input_calls;
	stop = state.offsetStopLimit;
	UT_ASSERT(!limits_error(false));
	UT_ASSERT(!need_vacuum);
	UT_ASSERT(!limits_error(false));
	UT_ASSERT(!need_vacuum);
	UT_ASSERT_EQ(unbound_logs, 1);
	TruncateMultiXact(1, 5);
	UT_ASSERT_EQ(empty_input_calls, before);
	UT_ASSERT_EQ(proven_logs, 1);
	UT_ASSERT_EQ(state.oldestOffset, 0);
	UT_ASSERT_EQ(state.offsetStopLimit, stop);
	UT_ASSERT_EQ(wal_records + deletions + offset_truncates, 0);
	state.oldestOffsetKnown = false;
	UT_ASSERT(limits_error(false));
	UT_ASSERT_EQ(last_error, ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE);
}
UT_TEST(empty_boot_striped_members_recalculate_at_segment_boundary)
{
	const uint32 span = MULTIXACT_MEMBERS_PER_PAGE * SLRU_PAGES_PER_SEGMENT;
	MultiXactOffset offsets[] = { 1, span - 1, span, span + 1 };
	MultiXactOffset stops[] = { (MultiXactOffset)(0 - span), (MultiXactOffset)(0 - span), 0, 0 };
	MultiXactId id = 0;
	MultiXactMember members[]
		= { { 100, MultiXactStatusForKeyShare }, { 116, MultiXactStatusForKeyShare } };
	empty_boot_fixture();
	UT_ASSERT(!limits_error(true));
	UT_ASSERT_EQ(state.offsetStopLimit, stops[0]);
	cluster_mxid_stripe_latch_runtime(true, 0, 32);
	for (unsigned i = 0; i < lengthof(offsets); i++) {
		/* Advance the fixture to both sides of the native member segment. */
		state.nextOffset = offsets[i];
		UT_ASSERT(!allocate_error(&id));
		UT_ASSERT_EQ(id, 32 + i * CLUSTER_MXID_STRIDE);
		UT_ASSERT_EQ(state.nextOffset, offsets[i] + 2);
		RecordNewMultiXact(id, offsets[i], 2, members);
		state.oldestMultiXactId = id;
		UT_ASSERT(!limits_error(false));
		UT_ASSERT_EQ(state.oldestOffset, offsets[i]);
		UT_ASSERT_EQ(state.offsetStopLimit, stops[i]);
		UT_ASSERT_EQ(proven_logs, 1);
	}
}
UT_TEST(single_shared_dense_mode_retains_native_limits_and_allocation)
{
	MultiXactId id = 0;
	reset_fixture(0, 32, 37, 100, 1000);
	cluster_xid_striping = false;
	put_offset(37, 123);
	UT_ASSERT(!limits_error(true));
	UT_ASSERT_EQ(state.oldestOffset, 123);
	UT_ASSERT_EQ(last_read, 37);
	UT_ASSERT(!allocate_error(&id));
	UT_ASSERT_EQ(id, 100);
	UT_ASSERT_EQ(proven_logs, 0);
	UT_ASSERT_EQ(authority_reads, 0);
}

int
main(void)
{
	UT_PLAN(35);
	UT_RUN(foreign_lower_uses_first_exact_local_id);
	UT_RUN(local_empty_accepts_zero_and_lower_beyond_next);
	UT_RUN(native_prefix_requires_qualified_empty_input);
	UT_RUN(runtime_unknown_retains_proven_limit_and_startup_refuses);
	UT_RUN(modular_id_window_and_member_space_are_distinct);
	UT_RUN(ordinary_native_dense_limits_remain_unchanged);
	UT_RUN(vacuum_limit_recalculation_keeps_local_member_protection);
	UT_RUN(truncation_uses_two_local_endpoints_and_preserves_live_segment);
	UT_RUN(unknown_endpoint_refuses_all_truncation);
	UT_RUN(allocation_catches_up_to_current_lower_bound);
	UT_RUN(allocation_rechecks_lower_bound_after_warning_unlock);
	UT_RUN(allocation_both_candidates_use_native_order_for_every_stripe);
	UT_RUN(warning_rechecks_counter_identity_and_member_protection);
	UT_RUN(shared_member_stop_precedes_any_offset_extension);
	UT_RUN(gap_skips_truncated_pages_and_keeps_prepared_next_page);
	UT_RUN(qualified_empty_floor_gap_extends_each_new_page_once);
	UT_RUN(allocation_does_not_reinterpret_an_ambiguous_id_boundary);
	UT_RUN(unproven_limit_blocks_maintenance_and_allocation_before_normalization);
	UT_RUN(concurrent_cut_or_origin_change_cannot_install_new_limit);
	UT_RUN(invalid_native_binding_and_ambiguous_id_windows_are_refused);
	UT_RUN(missing_exact_page_keeps_runtime_limit_and_io_error_propagates);
	UT_RUN(all_origins_keep_member_ring_bounds_without_foreign_offsets);
	UT_RUN(truncation_rechecks_cut_origin_and_next_after_endpoint_reads);
	UT_RUN(empty_endpoint_can_use_ring_zero_but_cut_cannot_exceed_next);
	UT_RUN(retention_uses_read_snapshot_only_when_next_does_not_regress);
	UT_RUN(reservation_gap_keeps_old_limit_then_published_members_advance);
	UT_RUN(empty_boot_installs_the_same_bound_as_striped_empty);
	UT_RUN(empty_boot_authority_must_be_the_exact_sealed_native_image);
	UT_RUN(empty_boot_rejects_every_unproven_activation_observation);
	UT_RUN(empty_boot_rechecks_original_input_and_native_cursor);
	UT_RUN(empty_boot_known_does_not_authorize_unbound_allocation);
	UT_RUN(empty_boot_diagnostics_do_not_outlive_the_selected_scope);
	UT_RUN(empty_boot_runtime_cannot_recreate_proof_or_truncate);
	UT_RUN(empty_boot_striped_members_recalculate_at_segment_boundary);
	UT_RUN(single_shared_dense_mode_retains_native_limits_and_allocation);
	UT_DONE();
	return ut_failed_count != 0;
}
