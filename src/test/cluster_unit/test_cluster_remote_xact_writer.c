/*-------------------------------------------------------------------------
 * Real remote-outcome mutation entrypoints, including release-build guards.
 * Only process roles and SLRU/lock/storage boundaries are fixtures. This is
 * not a shared-storage recovery or fencing qualification.
 * Author: SqlRush <sqlrush@gmail.com>
 *-------------------------------------------------------------------------
 */
#include "postgres.h"
/* Use c.h's exact release definitions even in the cassert test build. */
#undef Assert
#undef AssertMacro
#define Assert(condition) ((void)true)
#define AssertMacro(condition) ((void)true)
#include "access/slru.h"
#include "cluster/cluster_remote_xact.h"
#include "miscadmin.h"
#include "storage/fd.h"
#include "storage/lwlock.h"
#include "storage/sync.h"
#include <setjmp.h>

#undef printf
#undef fprintf
#include "unit_test.h"
UT_DEFINE_GLOBALS();

bool IsUnderPostmaster;
static bool fixture_startup;
#undef AmStartupProcess
#define AmStartupProcess() fixture_startup

static SlruCtlData ClusterRemoteXactCtlData;
#define ClusterRemoteXactCtl (&ClusterRemoteXactCtlData)
static void *RemoteXactShared;
static int remote_xact_online_writer_depth_v;
static SlruSharedData fixture_shared;
static LWLock fixture_lock;
static PGAlignedBlock fixture_page;
static char *fixture_buffers[1];
static SlruPageStatus fixture_status[1];
static bool fixture_dirty[1];
static int fixture_number[1];
static unsigned locks, writes, syncs, dirsyncs, scans, deletes;
static int lock_depth, deleted_segments[4];
static int sync_error;
static bool expect_error;
static jmp_buf error_boundary;

static void
fixture_report(int elevel)
{
	UT_ASSERT_EQ(elevel, ERROR);
	if (!expect_error)
		abort();
	longjmp(error_boundary, 1);
}
#undef ereport
#define ereport(elevel, args) fixture_report(elevel)

bool
LWLockAcquire(LWLock *lock, LWLockMode mode)
{
	UT_ASSERT(lock == &fixture_lock && mode == LW_EXCLUSIVE && lock_depth == 0);
	lock_depth++;
	locks++;
	return true;
}
void
LWLockRelease(LWLock *lock)
{
	UT_ASSERT(lock == &fixture_lock && lock_depth == 1);
	lock_depth--;
}
bool
SimpleLruDoesPhysicalPageExist(SlruCtl ctl, int pageno)
{
	UT_ASSERT(ctl == ClusterRemoteXactCtl);
	(void)pageno;
	return false;
}
int
SimpleLruZeroPage(SlruCtl ctl, int pageno)
{
	UT_ASSERT(ctl == ClusterRemoteXactCtl && lock_depth == 1);
	UT_ASSERT_EQ(fixture_status[0], SLRU_PAGE_EMPTY);
	memset(fixture_page.data, 0, BLCKSZ);
	fixture_number[0] = pageno;
	fixture_status[0] = SLRU_PAGE_VALID;
	fixture_dirty[0] = true;
	return 0;
}
int
SimpleLruReadPage(SlruCtl ctl, int pageno, bool write_ok, TransactionId xid)
{
	UT_ASSERT(ctl == ClusterRemoteXactCtl && lock_depth == 1 && write_ok);
	UT_ASSERT_EQ(fixture_number[0], pageno);
	UT_ASSERT_EQ(xid, 1000);
	return 0;
}
void
SimpleLruWriteAll(SlruCtl ctl, bool allow_redirtied)
{
	UT_ASSERT(ctl == ClusterRemoteXactCtl && lock_depth == 0 && allow_redirtied);
	writes++;
	fixture_dirty[0] = false;
}
int
SlruSyncFileTag(SlruCtl ctl, const FileTag *tag, char *path)
{
	UT_ASSERT(ctl == ClusterRemoteXactCtl && lock_depth == 0);
	UT_ASSERT_EQ(tag->segno, 524288); /* origin 1, xid 1000 */
	strcpy(path, "fixture-segment");
	syncs++;
	return sync_error;
}
void
fsync_fname(const char *path, bool isdir)
{
	UT_ASSERT(isdir && lock_depth == 0);
	UT_ASSERT(strcmp(path, "pg_xact_remote_v2") == 0);
	dirsyncs++;
}
bool
SlruScanDirectory(SlruCtl ctl, SlruScanCallback callback, void *data)
{
	/* Two origin-1 segments below cutoff, cutoff itself, foreign origin. */
	const int pages[] = { 16777216, 16777248, 16777280, 0 };
	scans++;
	for (unsigned i = 0; i < lengthof(pages); i++)
		(void)callback(ctl, "fixture", pages[i], data);
	return false;
}
void
SlruDeleteSegment(SlruCtl ctl, int segno)
{
	UT_ASSERT(ctl == ClusterRemoteXactCtl && lock_depth == 0);
	if (deletes < lengthof(deleted_segments))
		deleted_segments[deletes] = segno;
	deletes++;
}

#include "test_cluster_remote_xact_writer.inc"

static void
reset_fixture(void)
{
	memset(&fixture_page, 0, sizeof(fixture_page));
	memset(&fixture_shared, 0, sizeof(fixture_shared));
	memset(&ClusterRemoteXactCtlData, 0, sizeof(ClusterRemoteXactCtlData));
	fixture_buffers[0] = fixture_page.data;
	fixture_dirty[0] = false;
	fixture_status[0] = SLRU_PAGE_EMPTY;
	fixture_number[0] = -1;
	fixture_shared.ControlLock = &fixture_lock;
	fixture_shared.num_slots = 1;
	fixture_shared.page_buffer = fixture_buffers;
	fixture_shared.page_dirty = fixture_dirty;
	fixture_shared.page_status = fixture_status;
	fixture_shared.page_number = fixture_number;
	ClusterRemoteXactCtl->shared = &fixture_shared;
	RemoteXactShared = &fixture_shared;
	remote_xact_online_writer_depth_v = 0;
	IsUnderPostmaster = true;
	fixture_startup = false;
	locks = writes = syncs = dirsyncs = scans = deletes = 0;
	lock_depth = sync_error = 0;
	expect_error = false;
}

static void
assert_no_effect(const PGAlignedBlock *before)
{
	UT_ASSERT(memcmp(before->data, fixture_page.data, BLCKSZ) == 0);
	UT_ASSERT(!fixture_dirty[0]);
	UT_ASSERT_EQ(lock_depth, 0);
	UT_ASSERT_EQ(locks + writes + syncs + dirsyncs + scans + deletes, 0);
}

UT_TEST(ordinary_backend_cannot_prepare)
{
	const uint8 digest[28] = { 1 };
	PGAlignedBlock before;
	reset_fixture();
	before = fixture_page;
	UT_ASSERT_EQ(cluster_remote_xact_store_prepared_v2(1, 1000, digest),
				 CLUSTER_REMOTE_XACT_MUTATION_INVALID);
	assert_no_effect(&before);
}
UT_TEST(ordinary_backend_cannot_publish_terminal)
{
	PGAlignedBlock before;
	reset_fixture();
	before = fixture_page;
	UT_ASSERT_EQ(cluster_remote_xact_store_terminal_v2(
					 1, 1000, false, NULL, CLUSTER_REMOTE_XACT_COMMITTED, 123, 456, true, 2),
				 CLUSTER_REMOTE_XACT_MUTATION_INVALID);
	assert_no_effect(&before);
}
UT_TEST(ordinary_backend_cannot_reset_or_truncate)
{
	PGAlignedBlock before;
	ClusterRemoteXactEntryV2 *entry;
	reset_fixture();
	fixture_status[0] = SLRU_PAGE_VALID;
	fixture_number[0] = 16777219; /* origin 1 + floor(1000/256) */
	entry = &((ClusterRemoteXactEntryV2 *)fixture_page.data)[232];
	entry->format_version = 2;
	entry->status = 2; /* an existing ABORTED projection */
	before = fixture_page;
	UT_ASSERT(!cluster_remote_xact_reset_range_v2(1, 1000, 1));
	assert_no_effect(&before);
	UT_ASSERT(!cluster_remote_xact_truncate_before_v2(1, 16384));
	assert_no_effect(&before);
}
UT_TEST(ordinary_backend_cannot_flush)
{
	PGAlignedBlock before;
	reset_fixture();
	before = fixture_page;
	expect_error = true;
	if (setjmp(error_boundary) == 0) {
		cluster_remote_xact_flush();
		UT_ASSERT(false);
	}
	expect_error = false;
	assert_no_effect(&before);
}

static void
exercise_allowed_writer(void)
{
	const uint8 digest[28] = { 1 };
	ClusterRemoteXactEntryDecodedV2 decoded;
	ClusterRemoteXactEntryV2 *entry;
	UT_ASSERT_EQ(cluster_remote_xact_store_prepared_v2(1, 1000, digest),
				 CLUSTER_REMOTE_XACT_MUTATION_STORED);
	if (ut_current_failed)
		return;
	UT_ASSERT(cluster_remote_xact_pending_matches_v2(1, 1000, digest));
	UT_ASSERT_EQ(cluster_remote_xact_store_terminal_v2(
					 1, 1000, true, digest, CLUSTER_REMOTE_XACT_COMMITTED, 123, 456, true, 2),
				 CLUSTER_REMOTE_XACT_MUTATION_STORED);
	entry = &((ClusterRemoteXactEntryV2 *)fixture_page.data)[232];
	UT_ASSERT(cluster_remote_xact_entry_decode_terminal_v2(entry, &decoded));
	UT_ASSERT_EQ(decoded.commit_scn, 123);
	UT_ASSERT_EQ(decoded.commit_timestamp, 456);
	UT_ASSERT_EQ(decoded.wrap, 2);
	UT_ASSERT_EQ(writes, 2);
	UT_ASSERT_EQ(syncs, 2);
	UT_ASSERT_EQ(dirsyncs, 2);
	UT_ASSERT_EQ(cluster_remote_xact_store_terminal_v2(
					 1, 1000, true, digest, CLUSTER_REMOTE_XACT_COMMITTED, 123, 456, true, 2),
				 CLUSTER_REMOTE_XACT_MUTATION_UNCHANGED);
	UT_ASSERT_EQ(writes, 3); /* duplicate must still prove durability */
	UT_ASSERT_EQ(cluster_remote_xact_store_terminal_v2(
					 1, 1000, false, NULL, CLUSTER_REMOTE_XACT_ABORTED, InvalidScn, 0, false, 0),
				 CLUSTER_REMOTE_XACT_MUTATION_CONFLICT);
	UT_ASSERT_EQ(writes, 3);
	UT_ASSERT(cluster_remote_xact_reset_range_v2(1, 1000, 1));
	UT_ASSERT_EQ(entry->status, 0);
	UT_ASSERT_EQ(writes, 4);
	UT_ASSERT_EQ(syncs, 4);
	UT_ASSERT(cluster_remote_xact_truncate_before_v2(1, 16384));
	UT_ASSERT_EQ(deletes, 2);
	UT_ASSERT_EQ(deleted_segments[0], 524288);
	UT_ASSERT_EQ(deleted_segments[1], 524289);
	cluster_remote_xact_flush();
	UT_ASSERT_EQ(writes, 5);
	UT_ASSERT_EQ(lock_depth, 0);
}
UT_TEST(startup_writer_preserves_transitions)
{
	reset_fixture();
	fixture_startup = true;
	exercise_allowed_writer();
}
UT_TEST(standalone_writer_preserves_transitions)
{
	reset_fixture();
	IsUnderPostmaster = false;
	exercise_allowed_writer();
}
UT_TEST(online_writer_permission_ends_at_final_pop)
{
	const uint8 digest[28] = { 1 };
	PGAlignedBlock before;
	reset_fixture();
	cluster_remote_xact_online_writer_push();
	cluster_remote_xact_online_writer_push();
	cluster_remote_xact_online_writer_pop();
	UT_ASSERT_EQ(cluster_remote_xact_online_writer_depth(), 1);
	exercise_allowed_writer();
	cluster_remote_xact_online_writer_pop();
	UT_ASSERT_EQ(cluster_remote_xact_online_writer_depth(), 0);
	before = fixture_page;
	locks = writes = syncs = dirsyncs = scans = deletes = 0;
	UT_ASSERT_EQ(cluster_remote_xact_store_prepared_v2(1, 1000, digest),
				 CLUSTER_REMOTE_XACT_MUTATION_INVALID);
	assert_no_effect(&before);
}
UT_TEST(ordinary_backend_retains_read_only_queries)
{
	const uint8 digest[28] = { 1 };
	reset_fixture();
	fixture_startup = true;
	UT_ASSERT_EQ(cluster_remote_xact_store_prepared_v2(1, 1000, digest),
				 CLUSTER_REMOTE_XACT_MUTATION_STORED);
	fixture_startup = false;
	writes = syncs = dirsyncs = 0;
	UT_ASSERT(cluster_remote_xact_pending_matches_v2(1, 1000, digest));
	UT_ASSERT(!cluster_remote_xact_range_empty_v2(1, 1000, 1));
	UT_ASSERT_EQ(writes + syncs + dirsyncs, 0);
	UT_ASSERT_EQ(lock_depth, 0);
}
UT_TEST(durability_error_is_not_reported_as_stored)
{
	const uint8 digest[28] = { 1 };
	reset_fixture();
	fixture_startup = true;
	sync_error = -1;
	expect_error = true;
	if (setjmp(error_boundary) == 0) {
		(void)cluster_remote_xact_store_prepared_v2(1, 1000, digest);
		UT_ASSERT(false);
	}
	expect_error = false;
	UT_ASSERT_EQ(syncs, 1);
	UT_ASSERT_EQ(dirsyncs, 0);
	UT_ASSERT_EQ(lock_depth, 0);
}

int
main(void)
{
	UT_PLAN(9);
	UT_RUN(ordinary_backend_cannot_prepare);
	UT_RUN(ordinary_backend_cannot_publish_terminal);
	UT_RUN(ordinary_backend_cannot_reset_or_truncate);
	UT_RUN(ordinary_backend_cannot_flush);
	UT_RUN(startup_writer_preserves_transitions);
	UT_RUN(standalone_writer_preserves_transitions);
	UT_RUN(online_writer_permission_ends_at_final_pop);
	UT_RUN(ordinary_backend_retains_read_only_queries);
	UT_RUN(durability_error_is_not_reported_as_stored);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
