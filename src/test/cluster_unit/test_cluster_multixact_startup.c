/*-------------------------------------------------------------------------
 *
 * test_cluster_multixact_startup.c
 *    Retained native MultiXact startup, including boundary and wrap sentinels.
 *
 * The actual native body runs; SLRU pages and locks are memory fixtures.
 * This is not physical durability, replay or runtime truncation certification.
 *
 * Portions Copyright (c) 2026, PGRAC contributors
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *    src/test/cluster_unit/test_cluster_multixact_startup.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"
#include <setjmp.h>
#include "../../backend/access/transam/multixact.c"
#undef printf
#undef fprintf
#include "unit_test.h"
UT_DEFINE_GLOBALS();

bool cluster_shared_config;
bool InRecovery;
bool IsUnderPostmaster;
BackendType MyBackendType;
int autovacuum_multixact_freeze_max_age = 400000000;
static LWLockPadded locks[128];
LWLockPadded *MainLWLockArray = locks;
static SlruSharedData offset_shared, member_shared;
static MultiXactStateData state;
static MultiXactOffset offsets[MULTIXACT_OFFSETS_PER_PAGE];
static char members[BLCKSZ];
static char *offset_buffers[] = { (char *)offsets };
static char *member_buffers[] = { members };
static bool offset_dirty[1], member_dirty[1];
static unsigned reads, zeroes, writes;
static bool physical_exists, expect_error, read_error;
static int held[128];
static jmp_buf error_boundary;

void
ExceptionalCondition(const char *condition, const char *file, int line)
{
	fprintf(stderr, "unexpected assertion %s (%s:%d)\n", condition, file, line);
	abort();
}
bool
errstart(int level, const char *domain pg_attribute_unused())
{
	if (level < ERROR)
		return false;
	if (expect_error)
		longjmp(error_boundary, 1);
	abort();
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
	abort();
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
errmsg_plural(const char *a pg_attribute_unused(), const char *b pg_attribute_unused(),
			  unsigned long n pg_attribute_unused(), ...)
{
	return 0;
}
int
errhint(const char *s pg_attribute_unused(), ...)
{
	return 0;
}
int
errcode(int code pg_attribute_unused())
{
	return 0;
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
SendPostmasterSignal(PMSignalReason r pg_attribute_unused())
{
	abort();
}

bool
LWLockAcquire(LWLock *lock, LWLockMode mode pg_attribute_unused())
{
	int i = ((LWLockPadded *)lock) - locks;
	UT_ASSERT(i >= 0 && i < 128 && held[i] == 0);
	held[i]++;
	return true;
}
void
LWLockRelease(LWLock *lock)
{
	int i = ((LWLockPadded *)lock) - locks;
	UT_ASSERT(i >= 0 && i < 128 && held[i] == 1);
	held[i]--;
}
int
SimpleLruReadPage(SlruCtl ctl, int pageno pg_attribute_unused(),
				  bool write_ok pg_attribute_unused(), TransactionId xid pg_attribute_unused())
{
	UT_ASSERT(ctl == MultiXactOffsetCtl || ctl == MultiXactMemberCtl);
	reads++;
	if (read_error)
		(void)errstart(ERROR, NULL);
	return 0;
}
int
SimpleLruReadPage_ReadOnly(SlruCtl ctl, int pageno, TransactionId xid)
{
	LWLockAcquire(MultiXactOffsetSLRULock, LW_SHARED);
	return SimpleLruReadPage(ctl, pageno, false, xid);
}
int
SimpleLruZeroPage(SlruCtl ctl, int page pg_attribute_unused())
{
	zeroes++;
	memset(ctl->shared->page_buffer[0], 0, BLCKSZ);
	ctl->shared->page_dirty[0] = true;
	return 0;
}
void
SimpleLruWriteAll(SlruCtl ctl pg_attribute_unused(), bool r pg_attribute_unused())
{
	writes++;
}
bool
SimpleLruDoesPhysicalPageExist(SlruCtl ctl pg_attribute_unused(), int page pg_attribute_unused())
{
	return physical_exists;
}

static void
reset_fixture(MultiXactId next, MultiXactOffset offset, bool clustered)
{
	memset(&state, 0, sizeof(state));
	memset(&offset_shared, 0, sizeof(offset_shared));
	memset(&member_shared, 0, sizeof(member_shared));
	memset(offsets, 0, sizeof(offsets));
	memset(members, 0x55, sizeof(members));
	memset(held, 0, sizeof(held));
	MultiXactState = &state;
	state.nextMXact = next;
	state.nextOffset = offset;
	state.oldestMultiXactId = next == 0 ? MaxMultiXactId - 1 : FirstMultiXactId;
	state.oldestMultiXactDB = 5;
	offsets[MultiXactIdToOffsetEntry(state.oldestMultiXactId)] = 1;
	offsets[MultiXactIdToOffsetEntry(next < FirstMultiXactId ? FirstMultiXactId : next)]
		= offset == 0 && next != FirstMultiXactId ? 1 : offset;
	MultiXactOffsetCtl->shared = &offset_shared;
	MultiXactMemberCtl->shared = &member_shared;
	offset_shared.page_buffer = offset_buffers;
	member_shared.page_buffer = member_buffers;
	offset_shared.page_dirty = offset_dirty;
	member_shared.page_dirty = member_dirty;
	offset_dirty[0] = member_dirty[0] = false;
	cluster_shared_config = clustered;
	InRecovery = IsUnderPostmaster = false;
	MyBackendType = B_STARTUP;
	reads = zeroes = writes = 0;
	physical_exists = true;
	expect_error = read_error = false;
}
static bool
trim_refuses(void)
{
	expect_error = true;
	if (setjmp(error_boundary) == 0) {
		TrimMultiXact();
		expect_error = false;
		return false;
	}
	expect_error = false;
	return true;
}

UT_TEST(shared_partial_preserves_offsets_members_and_limits)
{
	char before_offsets[BLCKSZ], before_members[BLCKSZ];
	reset_fixture(101, 197, true);
	/* Unallocated bytes are not permission to reset retained files. */
	offsets[102] = 771;
	memcpy(before_offsets, offsets, BLCKSZ);
	memcpy(before_members, members, BLCKSZ);
	StartupMultiXact();
	TrimMultiXact();
	UT_ASSERT_EQ(memcmp(before_offsets, offsets, BLCKSZ), 0);
	UT_ASSERT_EQ(memcmp(before_members, members, BLCKSZ), 0);
	UT_ASSERT_EQ(zeroes + writes, 0);
	UT_ASSERT(!offset_dirty[0] && !member_dirty[0]);
	UT_ASSERT(state.finishedStartup && state.oldestOffsetKnown);
	UT_ASSERT_EQ(state.oldestOffset, 1);
}
UT_TEST(shared_page_boundary_preserves_existing_next_sentinel)
{
	reset_fixture(MULTIXACT_OFFSETS_PER_PAGE, 4000, true);
	TrimMultiXact();
	UT_ASSERT_EQ(offsets[0], 4000);
	UT_ASSERT_EQ(zeroes + writes, 0);
	UT_ASSERT(!offset_dirty[0] && !member_dirty[0]);
	UT_ASSERT_EQ(offset_shared.latest_page_number, 1);
}
UT_TEST(shared_wrap_and_initial_empty_sentinels)
{
	const MultiXactId ids[] = { 0, 71, FirstMultiXactId };
	const MultiXactOffset starts[] = { 90, 0, 0 };
	for (unsigned i = 0; i < lengthof(ids); i++) {
		char before[BLCKSZ];
		reset_fixture(ids[i], starts[i], true);
		memcpy(before, offsets, BLCKSZ);
		TrimMultiXact();
		UT_ASSERT_EQ(memcmp(before, offsets, BLCKSZ), 0);
		UT_ASSERT_EQ(zeroes + writes, 0);
		UT_ASSERT(state.finishedStartup);
	}
}
UT_TEST(shared_mismatched_sentinel_refuses_without_repair)
{
	reset_fixture(101, 197, true);
	offsets[101] = 198;
	UT_ASSERT(trim_refuses());
	UT_ASSERT(!state.finishedStartup);
	UT_ASSERT_EQ(offsets[101], 198);
	UT_ASSERT_EQ(zeroes + writes, 0);
}
UT_TEST(shared_missing_oldest_cannot_disable_wrap_protection)
{
	reset_fixture(101, 197, true);
	physical_exists = false;
	UT_ASSERT(trim_refuses());
	UT_ASSERT_EQ(zeroes + writes, 0);
	UT_ASSERT(!offset_dirty[0] && !member_dirty[0]);
}
UT_TEST(shared_wrong_cut_refuses_before_mutation)
{
	for (unsigned i = 0; i < 2; i++) {
		reset_fixture(101, 197, true);
		if (i == 0)
			InRecovery = true;
		else
			MyBackendType = B_BACKEND;
		UT_ASSERT(trim_refuses());
		UT_ASSERT_EQ(reads + zeroes + writes, 0);
		UT_ASSERT(!state.finishedStartup);
	}
}
UT_TEST(shared_read_error_never_becomes_zeroes)
{
	reset_fixture(101, 197, true);
	read_error = true;
	UT_ASSERT(trim_refuses());
	UT_ASSERT_EQ(zeroes + writes, 0);
	UT_ASSERT(!state.finishedStartup);
}
UT_TEST(ordinary_startup_keeps_native_trim)
{
	reset_fixture(101, 197, false);
	offsets[102] = 771;
	TrimMultiXact();
	UT_ASSERT_EQ(offsets[101], 197);
	UT_ASSERT_EQ(offsets[102], 0);
	UT_ASSERT(offset_dirty[0] && member_dirty[0]);
	UT_ASSERT_EQ(members[BLCKSZ - 1], 0);
	UT_ASSERT_EQ(writes, 2);
	UT_ASSERT(state.finishedStartup && state.oldestOffsetKnown);
}
int
main(void)
{
	UT_PLAN(8);
	UT_RUN(shared_partial_preserves_offsets_members_and_limits);
	UT_RUN(shared_page_boundary_preserves_existing_next_sentinel);
	UT_RUN(shared_wrap_and_initial_empty_sentinels);
	UT_RUN(shared_mismatched_sentinel_refuses_without_repair);
	UT_RUN(shared_missing_oldest_cannot_disable_wrap_protection);
	UT_RUN(shared_wrong_cut_refuses_before_mutation);
	UT_RUN(shared_read_error_never_becomes_zeroes);
	UT_RUN(ordinary_startup_keeps_native_trim);
	UT_DONE();
	return ut_failed_count != 0;
}
