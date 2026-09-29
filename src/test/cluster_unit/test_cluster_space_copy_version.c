/*-------------------------------------------------------------------------
 *
 * test_cluster_space_copy_version.c
 *    Native file-copy WAL binds pages to their new SPACE identity.
 *
 * Portions Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *    src/test/cluster_unit/test_cluster_space_copy_version.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/xlog.h"
#include "access/xloginsert.h"
#include "catalog/pg_class.h"
#include "catalog/pg_control.h"
#include "catalog/pg_tablespace_d.h"
#include "catalog/storage.h"
#include "cluster/cluster_guc.h"
#include "cluster/cluster_space_storage.h"
#include "cluster/cluster_wal_thread.h"
#include "cluster/storage/cluster_smgr.h"
#include "miscadmin.h"
#include "storage/bufmgr.h"
#include "storage/smgr.h"
#include "unit_test.h"

UT_DEFINE_GLOBALS();

bool cluster_enabled = true, cluster_shared_config = true;
int cluster_node_id = 0, wal_level = WAL_LEVEL_REPLICA;
int NBuffers = 1, NLocBuffer;
char *BufferBlocks;
Block *LocalBufferBlockPointers;
volatile uint32 CritSectionCount;
volatile sig_atomic_t InterruptPending;
bool cluster_recmerge_window_active, cluster_recmerge_apply_foreign;
uint64 cluster_recmerge_window_scn, cluster_recmerge_window_own_lsn;

static SMgrRelationData source, destination;
static SMgrRelationData reopened_source, reopened_destination;
static PGAlignedBlock space, source_page, written, image;
static ClusterWalDurablePrefixRef ref;
static ClusterSpaceIdentity identity;
static RfPageVersionEdgeEntryV1 entry;
static ForkNumber test_fork;
static unsigned old_wal, new_wal, edges, writes, syncs, flushes, space_reads;
static bool pinned, locked, begun;
static bool invalidate_on_read, handles_retired;
static unsigned reopens;
static uint64 token;

void
ExceptionalCondition(const char *condition, const char *file, int line)
{
	printf("# Unexpected assertion: %s at %s:%d\n", condition, file, line);
	abort();
}

void
ProcessInterrupts(void)
{
	abort();
}

void *
palloc(Size size)
{
	return malloc(size);
}

void
pfree(void *ptr)
{
	free(ptr);
}

char *
pstrdup(const char *str)
{
	return strdup(str);
}

bool
RecoveryInProgress(void)
{
	return false;
}

bool
cluster_wal_thread_current_v2_ref(ClusterWalDurablePrefixRef *out)
{
	*out = ref;
	return true;
}

bool
cluster_wal_thread_restart_v2_ref(ClusterWalDurablePrefixRef *out)
{
	(void)out;
	abort();
}

int
cluster_smgr_which_for(RelFileLocator tag, BackendId backend)
{
	if (!RelFileLocatorEquals(tag, identity.key.locator) || backend != InvalidBackendId)
		abort();
	return 1;
}

SMgrRelation
smgropen(RelFileLocator tag, BackendId backend)
{
	if (backend != InvalidBackendId)
		abort();
	if (RelFileLocatorEquals(tag, reopened_source.smgr_rlocator.locator)) {
		if (handles_retired)
			reopens++;
		return handles_retired ? &reopened_source : &source;
	}
	if (RelFileLocatorEquals(tag, reopened_destination.smgr_rlocator.locator)) {
		if (handles_retired)
			reopens++;
		return handles_retired ? &reopened_destination : &destination;
	}
	abort();
}

bool
smgrexists(SMgrRelation rel, ForkNumber forknum)
{
	if (rel != &destination || forknum != SPACE_FORKNUM)
		abort();
	return true;
}

BlockNumber
smgrnblocks(SMgrRelation rel, ForkNumber forknum)
{
	SMgrRelation current_src = handles_retired ? &reopened_source : &source;
	SMgrRelation current_dst = handles_retired ? &reopened_destination : &destination;

	if (rel != current_src && rel != current_dst)
		abort();
	if (rel == current_dst && forknum == SPACE_FORKNUM)
		return 1;
	if (forknum != test_fork)
		abort();
	return rel == current_src ? 1 : writes;
}

void
smgrread(SMgrRelation rel, ForkNumber forknum, BlockNumber block, void *out)
{
	if (rel != (handles_retired ? &reopened_source : &source) || forknum != test_fork || block != 0)
		abort();
	memcpy(out, source_page.data, BLCKSZ);
}

bool
PageIsVerifiedForFork(Page page, ForkNumber forknum, BlockNumber block, int flags)
{
	(void)flags;
	return forknum == test_fork && block == 0 && memcmp(page, source_page.data, BLCKSZ) == 0;
}

void
PageSetChecksumInplace(Page page, BlockNumber block)
{
	(void)page;
	if (block != 0)
		abort();
}

void
smgrextend(SMgrRelation rel, ForkNumber forknum, BlockNumber block, const void *bytes, bool skip)
{
	if (rel != (handles_retired ? &reopened_destination : &destination) || forknum != test_fork
		|| block != 0 || !skip)
		abort();
	if (cluster_shared_config)
		UT_ASSERT_EQ(flushes, 1);
	memcpy(written.data, bytes, BLCKSZ);
	writes++;
}

void
smgrimmedsync(SMgrRelation rel, ForkNumber forknum)
{
	if (rel != (handles_retired ? &reopened_destination : &destination) || forknum != test_fork
		|| writes != 1)
		abort();
	syncs++;
}

XLogRecPtr
log_newpage(RelFileLocator *tag, ForkNumber forknum, BlockNumber block, Page page, bool standard)
{
	if (!RelFileLocatorEquals(*tag, identity.key.locator) || forknum != test_fork || block != 0
		|| standard)
		abort();
	old_wal++;
	PageSetLSN(page, 100);
	return 100;
}

Buffer
ReadBufferWithoutRelcache(RelFileLocator tag, ForkNumber forknum, BlockNumber block,
						  ReadBufferMode mode, BufferAccessStrategy strategy, bool permanent)
{
	if (!RelFileLocatorEquals(tag, identity.key.locator) || forknum != SPACE_FORKNUM || block != 0
		|| mode != RBM_NORMAL || strategy != NULL || !permanent || pinned)
		abort();
	pinned = true;
	space_reads++;
	if (invalidate_on_read) {
		memset(&source, 0xa5, sizeof(source));
		memset(&destination, 0xa5, sizeof(destination));
		handles_retired = true;
	}
	return 1;
}

void
LockBuffer(Buffer buffer, int mode)
{
	if (buffer != 1 || !pinned || mode != BUFFER_LOCK_SHARE || locked)
		abort();
	locked = true;
}

BlockNumber
BufferGetBlockNumber(Buffer buffer)
{
	if (buffer != 1 || !pinned || !locked)
		abort();
	return 0;
}

void
UnlockReleaseBuffer(Buffer buffer)
{
	if (buffer != 1 || !pinned || !locked)
		abort();
	pinned = locked = false;
}

SCN
cluster_scn_advance(void)
{
	return ++token;
}

void
XLogBeginInsert(void)
{
	if (begun || pinned || locked)
		abort();
	begun = true;
}

void
XLogRegisterBlock(uint8 id, RelFileLocator *tag, ForkNumber forknum, BlockNumber block, Page page,
				  uint8 flags)
{
	if (!begun || id != 0 || !RelFileLocatorEquals(*tag, identity.key.locator)
		|| forknum != test_fork || block != 0 || flags != REGBUF_FORCE_IMAGE)
		abort();
	memcpy(image.data, page, BLCKSZ);
}

void
XLogRegisterPageVersionEdge(uint64 result, const RfPageVersionEdgeEntryV1 *entries, uint8 count)
{
	if (!begun || count != 1 || result != token)
		abort();
	entry = entries[0];
	edges++;
}

XLogRecPtr
XLogInsert(RmgrId rmid, uint8 info)
{
	if (!begun || rmid != RM_XLOG_ID || info != XLOG_FPI || edges != 1)
		abort();
	begun = false;
	new_wal++;
	return 200;
}

void
XLogFlush(XLogRecPtr lsn)
{
	if (lsn != 200 || new_wal != 1 || writes != 0)
		abort();
	flushes++;
}

int
errmsg(const char *fmt, ...)
{
	(void)fmt;
	abort();
}

int
errcode(int code)
{
	(void)code;
	abort();
}

int
errhint(const char *fmt, ...)
{
	(void)fmt;
	abort();
}

static void
reset(ForkNumber forknum)
{
	PageHeader header;

	memset(&source, 0, sizeof(source));
	memset(&destination, 0, sizeof(destination));
	memset(&source_page, 0, sizeof(source_page));
	memset(&written, 0, sizeof(written));
	memset(&image, 0, sizeof(image));
	memset(&entry, 0, sizeof(entry));
	memset(&ref, 0, sizeof(ref));
	memset(&identity, 0, sizeof(identity));
	source.smgr_rlocator.locator = (RelFileLocator){ DEFAULTTABLESPACE_OID, 5, 16384 };
	destination.smgr_rlocator.locator = (RelFileLocator){ DEFAULTTABLESPACE_OID, 5, 16385 };
	source.smgr_rlocator.backend = destination.smgr_rlocator.backend = InvalidBackendId;
	reopened_source = source;
	reopened_destination = destination;
	identity.key.locator = destination.smgr_rlocator.locator;
	identity.key.system_identifier = ref.claim.identity.system_identifier = 11;
	identity.key.database_incarnation = ref.claim.database_incarnation = 12;
	identity.key.storage_uuid[15] = ref.claim.identity.storage_uuid[15] = 13;
	identity.incarnation[0] = 14;
	identity.incarnation[15] = 15;
	identity.sequence = identity.operation = 1;
	identity.state = CLUSTER_SPACE_IDENTITY_LIVE;
	if (!cluster_space_identity_page_encode(&identity, 2, space.data, BLCKSZ))
		abort();
	header = (PageHeader)source_page.data;
	header->pd_lower = SizeOfPageHeaderData;
	header->pd_upper = header->pd_special = BLCKSZ;
	header->pd_pagesize_version = BLCKSZ | PG_PAGE_LAYOUT_VERSION;
	header->pd_block_scn = 7;
	source_page.data[512] = 'x';
	BufferBlocks = space.data;
	pinned = locked = begun = false;
	invalidate_on_read = handles_retired = false;
	reopens = 0;
	old_wal = new_wal = edges = writes = syncs = flushes = space_reads = 0;
	cluster_shared_config = true;
	test_fork = forknum;
	token = 99;
}

UT_TEST(test_copy_main_has_new_identity_edge_and_wal_before_data)
{
	reset(MAIN_FORKNUM);
	RelationCopyStorage(&source, &destination, MAIN_FORKNUM, RELPERSISTENCE_PERMANENT);
	UT_ASSERT_EQ(old_wal, 0);
	UT_ASSERT_EQ(new_wal, 1);
	UT_ASSERT_EQ(edges, 1);
	UT_ASSERT_EQ(entry.before_kind, RF_PAGE_STATE_ABSENT);
	UT_ASSERT_EQ(entry.before.mutation_token, 0);
	UT_ASSERT(memcmp(entry.result_incarnation, identity.incarnation, 16) == 0);
	UT_ASSERT_EQ(((PageHeader)written.data)->pd_block_scn, 100);
	UT_ASSERT_EQ(((PageHeader)image.data)->pd_block_scn, 100);
	UT_ASSERT_EQ(((PageHeader)source_page.data)->pd_block_scn, 7);
	UT_ASSERT_EQ(written.data[512], 'x');
	UT_ASSERT_EQ(writes, 1);
	UT_ASSERT_EQ(syncs, 1);
	UT_ASSERT_EQ(space_reads, 1);
}

UT_TEST(test_nonshared_copy_keeps_native_path)
{
	reset(MAIN_FORKNUM);
	cluster_shared_config = false;
	RelationCopyStorage(&source, &destination, MAIN_FORKNUM, RELPERSISTENCE_PERMANENT);
	UT_ASSERT_EQ(old_wal, 1);
	UT_ASSERT_EQ(new_wal, 0);
	UT_ASSERT_EQ(space_reads, 0);
	UT_ASSERT_EQ(((PageHeader)written.data)->pd_block_scn, 7);
	UT_ASSERT_EQ(syncs, 1);
}

UT_TEST(test_vm_is_ordinary_but_fsm_is_explicitly_rebuildable)
{
	reset(VISIBILITYMAP_FORKNUM);
	RelationCopyStorage(&source, &destination, test_fork, RELPERSISTENCE_PERMANENT);
	UT_ASSERT_EQ(entry.page_class, RF_PAGE_CLASS_ORDINARY);
	UT_ASSERT_EQ(entry.before_kind, RF_PAGE_STATE_ABSENT);
	UT_ASSERT_EQ(entry.result_kind, RF_PAGE_STATE_PRESENT);
	UT_ASSERT_EQ(((PageHeader)written.data)->pd_block_scn, 100);
	UT_ASSERT_EQ(flushes, 1);
	reset(FSM_FORKNUM);
	RelationCopyStorage(&source, &destination, test_fork, RELPERSISTENCE_PERMANENT);
	UT_ASSERT_EQ(entry.page_class, RF_PAGE_CLASS_REBUILDABLE_FSM);
	UT_ASSERT_EQ(entry.before_kind, RF_PAGE_STATE_REBUILDABLE);
	UT_ASSERT_EQ(entry.result_kind, RF_PAGE_STATE_REBUILDABLE);
	UT_ASSERT_EQ(entry.result_incarnation[0], 0);
	UT_ASSERT_EQ(((PageHeader)written.data)->pd_block_scn, 7);
	UT_ASSERT_EQ(flushes, 1);
}

UT_TEST(test_copy_refusal_leaves_page_and_output_unchanged)
{
	PGAlignedBlock saved;
	XLogRecPtr lsn = 999;

	reset(MAIN_FORKNUM);
	saved = source_page;
	identity.key.database_incarnation++;
	UT_ASSERT(!cluster_space_copy_page_wal(&identity, test_fork, 0, source_page.data, &lsn));
	UT_ASSERT(memcmp(&saved, &source_page, BLCKSZ) == 0);
	identity.key.database_incarnation--;
	identity.state = CLUSTER_SPACE_IDENTITY_TOMBSTONED;
	UT_ASSERT(!cluster_space_copy_page_wal(&identity, test_fork, 0, source_page.data, &lsn));
	UT_ASSERT(memcmp(&saved, &source_page, BLCKSZ) == 0);
	identity.state = CLUSTER_SPACE_IDENTITY_LIVE;
	UT_ASSERT(!cluster_space_copy_page_wal(&identity, SPACE_FORKNUM, 0, source_page.data, &lsn));
	UT_ASSERT(!cluster_space_copy_page_wal(&identity, INIT_FORKNUM, 0, source_page.data, &lsn));
	UT_ASSERT(memcmp(&saved, &source_page, BLCKSZ) == 0);
	((PageHeader)source_page.data)->pd_flags |= PD_SPACE_METADATA;
	saved = source_page;
	UT_ASSERT(!cluster_space_copy_page_wal(&identity, test_fork, 0, source_page.data, &lsn));
	UT_ASSERT(memcmp(&saved, &source_page, BLCKSZ) == 0);
	memset(&source_page, 0, sizeof(source_page));
	saved = source_page;
	UT_ASSERT(!cluster_space_copy_page_wal(&identity, test_fork, 0, source_page.data, &lsn));
	UT_ASSERT(memcmp(&saved, &source_page, BLCKSZ) == 0);
	UT_ASSERT_EQ(lsn, 999);
	UT_ASSERT_EQ(new_wal, 0);
	UT_ASSERT_EQ(edges, 0);
	UT_ASSERT(!begun);
}

UT_TEST(test_native_copy_reopens_handles_after_identity_buffer_read)
{
	reset(MAIN_FORKNUM);
	invalidate_on_read = true;
	RelationCopyStorage(&source, &destination, test_fork, RELPERSISTENCE_PERMANENT);
	UT_ASSERT(handles_retired);
	UT_ASSERT_EQ(reopens, 2);
	UT_ASSERT_EQ(new_wal, 1);
	UT_ASSERT_EQ(writes, 1);
	UT_ASSERT_EQ(syncs, 1);
}

int
main(void)
{
	UT_PLAN(5);
	UT_RUN(test_copy_main_has_new_identity_edge_and_wal_before_data);
	UT_RUN(test_nonshared_copy_keeps_native_path);
	UT_RUN(test_vm_is_ordinary_but_fsm_is_explicitly_rebuildable);
	UT_RUN(test_copy_refusal_leaves_page_and_output_unchanged);
	UT_RUN(test_native_copy_reopens_handles_after_identity_buffer_read);
	UT_DONE();
	return ut_failed_count ? 1 : 0;
}
