/* Exercise the actual optional pause in the native page test module. */
#include "postgres.h"
#include <stdarg.h>
#include "miscadmin.h"
#include "storage/bufmgr.h"
#include "utils/rel.h"
#include "utils/timestamp.h"
#include "varatt.h"

#undef printf
#undef fprintf
#undef snprintf
#undef vsnprintf
#include "unit_test.h"

UT_DEFINE_GLOBALS();

static RelationData relation;
static char page[BLCKSZ];
static bool pinned;
static bool shared;
static int flushes;
static int sleeps;
static int closes;
static int messages;
static int test_pcm_read_source_hold_shared_ms;
static char logged[1024];
int MyProcPid = 4321;
int cluster_node_id = 2;

static Relation
fixture_open(Oid oid)
{
	UT_ASSERT_EQ(oid, 123);
	UT_ASSERT(!pinned && !shared);
	return &relation;
}

static Buffer
fixture_read(Relation rel, BlockNumber block)
{
	UT_ASSERT(rel == &relation);
	UT_ASSERT_EQ(block, 0);
	pinned = true;
	return 1;
}

static void
fixture_lock(Buffer buffer, int mode)
{
	UT_ASSERT_EQ(buffer, 1);
	UT_ASSERT_EQ(mode, BUFFER_LOCK_SHARE);
	UT_ASSERT(pinned && !shared);
	shared = true;
}

static void
fixture_flush(Buffer buffer)
{
	UT_ASSERT_EQ(buffer, 1);
	UT_ASSERT(pinned && shared);
	flushes++;
}

static Page
fixture_page(Buffer buffer)
{
	UT_ASSERT_EQ(buffer, 1);
	UT_ASSERT(pinned && shared);
	return page;
}

static Datum
fixture_sleep(Datum interval)
{
	UT_ASSERT(pinned && shared);
	UT_ASSERT_EQ(messages, 1);
	UT_ASSERT_EQ(DatumGetFloat8(interval) * 1000,
				 test_pcm_read_source_hold_shared_ms);
	sleeps++;
	/* Prove that the real page copy occurs after this boundary. */
	memset(page, 0x5a, sizeof(page));
	return (Datum)0;
}

static int
fixture_message(const char *format, ...)
{
	va_list args;
	size_t used = strlen(logged);
	int length;

	UT_ASSERT(pinned && shared);
	va_start(args, format);
	length = vsnprintf(logged + used, sizeof(logged) - used, format, args);
	va_end(args);
	if (length < 0 || (size_t)length + 2 > sizeof(logged) - used)
		abort();
	logged[used + length] = '\n';
	logged[used + length + 1] = '\0';
	messages++;
	return 0;
}

static void
fixture_release(Buffer buffer)
{
	UT_ASSERT_EQ(buffer, 1);
	UT_ASSERT(pinned && shared);
	pinned = shared = false;
}

static void
fixture_close(Relation rel, LOCKMODE lock)
{
	UT_ASSERT(rel == &relation);
	UT_ASSERT_EQ(lock, AccessShareLock);
	UT_ASSERT(!pinned && !shared);
	closes++;
}

#define open_test_relation fixture_open
#define ReadBuffer fixture_read
#define LockBuffer fixture_lock
#define FlushOneBuffer fixture_flush
#undef BufferGetPage
#define BufferGetPage fixture_page
#define UnlockReleaseBuffer fixture_release
#define relation_close fixture_close
#define palloc malloc
#undef DirectFunctionCall1
#define DirectFunctionCall1(function, argument) fixture_sleep(argument)
#define GetCurrentTimestamp() ((TimestampTz)1234567)
#define errmsg_internal fixture_message
#undef ereport
#define ereport(level, arguments) ((void)(arguments))
#include "test_cluster_first_fill_hot_block.inc"

static void
setup(void)
{
	memset(&relation, 0, sizeof(relation));
	relation.rd_locator = (RelFileLocator){ .spcOid = 1663, .dbOid = 5, .relNumber = 16392 };
	memset(page, 0x31, sizeof(page));
	logged[0] = '\0';
	pinned = shared = false;
	flushes = sleeps = closes = messages = 0;
	test_pcm_read_source_hold_shared_ms = 0;
}

static void
check_result(bytea *result, int value)
{
	int different = 0;

	UT_ASSERT_EQ(VARSIZE(result), VARHDRSZ + BLCKSZ);
	for (int i = 0; i < BLCKSZ; i++)
		different += (unsigned char)VARDATA(result)[i] != value;
	UT_ASSERT_EQ(different, 0);
	UT_ASSERT(!pinned && !shared);
	UT_ASSERT_EQ(closes, 1);
	free(result);
}

UT_TEST(unarmed_read_preserves_original_native_page_copy)
{
	setup();
	check_result(read_test_page(123, false), 0x31);
	UT_ASSERT_EQ(sleeps + messages + flushes, 0);
}

UT_TEST(armed_read_pauses_under_share_before_copy)
{
	setup();
	test_pcm_read_source_hold_shared_ms = 15000;
	check_result(read_test_page(123, false), 0x5a);
	UT_ASSERT_EQ(sleeps, 1);
	UT_ASSERT_EQ(messages, 2);
	UT_ASSERT_EQ(flushes, 0);
	UT_ASSERT_STR_CONTAINS(logged, "phase=SHARED_HELD time=1234567 node=2 pid=4321 tag=1663/5/16392/0/0 ");
	UT_ASSERT_STR_CONTAINS(logged, "phase=SHARED_RELEASE time=1234567 node=2 pid=4321 tag=1663/5/16392/0/0 ");
}

UT_TEST(flush_never_uses_the_reader_pause)
{
	setup();
	test_pcm_read_source_hold_shared_ms = 15000;
	check_result(read_test_page(123, true), 0x31);
	UT_ASSERT_EQ(flushes, 1);
	UT_ASSERT_EQ(sleeps + messages, 0);
}

int
main(void)
{
	UT_PLAN(3);
	UT_RUN(unarmed_read_preserves_original_native_page_copy);
	UT_RUN(armed_read_pauses_under_share_before_copy);
	UT_RUN(flush_never_uses_the_reader_pause);
	UT_DONE();
	return ut_failed_count ? 1 : 0;
}
