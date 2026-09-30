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

#include <setjmp.h>

#include "access/xlog.h"
#include "access/xloginsert.h"
#include "catalog/pg_class.h"
#include "catalog/pg_control.h"
#include "catalog/pg_tablespace_d.h"
#include "catalog/storage.h"
#include "cluster/cluster_itl_slot.h"
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
static bool minimal_wal_copy;
int NBuffers = 3, NLocBuffer;
char *BufferBlocks;
Block *LocalBufferBlockPointers;
volatile uint32 CritSectionCount;
volatile sig_atomic_t InterruptPending;
bool cluster_recmerge_window_active, cluster_recmerge_apply_foreign;
uint64 cluster_recmerge_window_scn, cluster_recmerge_window_own_lsn;

static SMgrRelationData source, destination;
static SMgrRelationData reopened_source, reopened_destination;
static PGAlignedBlock buffers[3], written, image;
#define space buffers[0]
#define source_page buffers[1]
#define target_page buffers[2]
static ClusterWalSourceRef ref;
static ClusterSpaceIdentity identity;
static RfPageVersionEdgeEntryV1 entry;
static ForkNumber test_fork;
static unsigned old_wal, new_wal, edges, writes, syncs, flushes, space_reads;
static bool pinned, locked, begun;
static bool invalidate_on_read, handles_retired;
static unsigned reopens;
static uint64 token;
static bool buffered;
static bool data_pins[2], data_locks[2];
static unsigned dirties, strategies;
static RelFileLocator buffer_locator;
static bool expecting_error;
static jmp_buf error_jump;
static char last_error[256];

void
ExceptionalCondition(const char *condition, const char *file, int line)
{
	printf("# Unexpected assertion: %s at %s:%d\n", condition, file, line);
	abort();
}

/* The actual native heap initializer, shared with the extension test. */
#include "test_cluster_heap_extend_page.inc"

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
cluster_wal_thread_current_v2_ref(ClusterWalSourceRef *out)
{
	*out = ref;
	return true;
}

bool
cluster_wal_thread_restart_v2_ref(ClusterWalSourceRef *out)
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
		return 2;
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
	if (buffered) {
		PGAlignedBlock zero;

		memset(&zero, 0, sizeof(zero));
		UT_ASSERT(memcmp(bytes, zero.data, BLCKSZ) == 0);
		UT_ASSERT_EQ(new_wal, 0);
		memcpy(target_page.data, bytes, BLCKSZ);
	} else if (cluster_shared_config)
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
	if (buffered && forknum == test_fork) {
		bool is_source = RelFileLocatorEquals(tag, reopened_source.smgr_rlocator.locator);
		int slot = is_source ? 0 : 1;

		if ((!is_source && !RelFileLocatorEquals(tag, identity.key.locator)) || block != 0
			|| !permanent || strategy == NULL || data_pins[slot]
			|| mode != (is_source ? RBM_NORMAL : RBM_ZERO_AND_LOCK))
			abort();
		data_pins[slot] = true;
		data_locks[slot] = !is_source;
		return is_source ? 2 : 3;
	}
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
	if (buffered && buffer == 2) {
		if (!data_pins[0] || data_locks[0] || mode != BUFFER_LOCK_SHARE)
			abort();
		data_locks[0] = true;
		return;
	}
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
	if (buffered && (buffer == 2 || buffer == 3)) {
		int slot = buffer - 2;

		if (!data_pins[slot] || !data_locks[slot] || CritSectionCount != 0)
			abort();
		data_pins[slot] = data_locks[slot] = false;
		return;
	}
	if (buffer != 1 || !pinned || !locked)
		abort();
	pinned = locked = false;
}

BufferAccessStrategy
GetAccessStrategy(BufferAccessStrategyType type)
{
	if (!buffered || (type != BAS_BULKREAD && type != BAS_BULKWRITE))
		abort();
	strategies++;
	return (BufferAccessStrategy)(uintptr_t)(type + 1);
}

void
FreeAccessStrategy(BufferAccessStrategy strategy)
{
	if (!buffered || strategy == NULL || strategies == 0)
		abort();
	strategies--;
}

void
BufferGetTag(Buffer buffer, RelFileLocator *locator, ForkNumber *forknum, BlockNumber *block)
{
	if (!buffered || buffer != 3 || !data_pins[1] || !data_locks[1])
		abort();
	*locator = buffer_locator;
	*forknum = test_fork;
	*block = 0;
}

bool
BufferIsPermanent(Buffer buffer)
{
	if (!buffered || buffer != 3 || !data_pins[1])
		abort();
	return true;
}

void
MarkBufferDirty(Buffer buffer)
{
	if (!buffered || buffer != 3 || !data_pins[1] || !data_locks[1] || CritSectionCount == 0)
		abort();
	dirties++;
}

XLogRecPtr
log_newpage_buffer(Buffer buffer, bool standard)
{
	if (!buffered || buffer != 3 || !standard || CritSectionCount == 0)
		abort();
	old_wal++;
	PageSetLSN(target_page.data, 100);
	return 100;
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
	if (buffered && (!data_pins[0] || !data_locks[0] || !data_pins[1] || !data_locks[1]))
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
	if (buffered) {
		PGAlignedBlock zero;

		memset(&zero, 0, sizeof(zero));
		UT_ASSERT(CritSectionCount > 0);
		/* A checkpoint starting after this insertion can put its redo
		 * beyond the FPI. Its dirty-buffer scan must already select this
		 * target, even though the content lock delays its DATA write. */
		UT_ASSERT_EQ(dirties, 1);
		UT_ASSERT(memcmp(target_page.data, zero.data, BLCKSZ) == 0);
	}
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
	if (expecting_error)
		longjmp(error_jump, 1);
	abort();
}

bool
errstart(int elevel, const char *domain)
{
	(void)domain;
	if (elevel == DEBUG1)
		return false;
	if (expecting_error && elevel == ERROR)
		return true;
	abort();
}

int
errmsg_internal(const char *fmt, ...)
{
	va_list args;

	if (!expecting_error)
		abort();
	va_start(args, fmt);
	vsnprintf(last_error, sizeof(last_error), fmt, args);
	va_end(args);
	longjmp(error_jump, 1);
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

/* Exact original buffer-copy body, not a fixture reimplementation. */
#include "test_cluster_space_copy_buffer.inc"

static void
reset(ForkNumber forknum)
{
	PageHeader header;

	wal_level = minimal_wal_copy ? WAL_LEVEL_MINIMAL : WAL_LEVEL_REPLICA;
	memset(&source, 0, sizeof(source));
	memset(&destination, 0, sizeof(destination));
	memset(&source_page, 0, sizeof(source_page));
	memset(&target_page, 0, sizeof(target_page));
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
	buffer_locator = identity.key.locator;
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
	buffered = false;
	memset(data_pins, 0, sizeof(data_pins));
	memset(data_locks, 0, sizeof(data_locks));
	dirties = strategies = 0;
	CritSectionCount = 0;
	expecting_error = false;
	last_error[0] = '\0';
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

UT_TEST(test_buffer_copy_records_real_unformatted_predecessor)
{
	reset(MAIN_FORKNUM);
	buffered = true;
	RelationCopyStorageUsingBuffer(source.smgr_rlocator.locator, destination.smgr_rlocator.locator,
								   test_fork, true);
	UT_ASSERT_EQ(old_wal, 0);
	UT_ASSERT_EQ(new_wal, 1);
	UT_ASSERT_EQ(edges, 1);
	UT_ASSERT_EQ(entry.before_kind, RF_PAGE_STATE_UNFORMATTED);
	UT_ASSERT_EQ(entry.before.mutation_token, 0);
	UT_ASSERT(memcmp(entry.before.segment_incarnation, identity.incarnation, 16) == 0);
	UT_ASSERT(memcmp(entry.result_incarnation, identity.incarnation, 16) == 0);
	UT_ASSERT_EQ(((PageHeader)target_page.data)->pd_block_scn, 100);
	UT_ASSERT_EQ(((PageHeader)image.data)->pd_block_scn, 100);
	UT_ASSERT_EQ(((PageHeader)source_page.data)->pd_block_scn, 7);
	UT_ASSERT_EQ(PageGetLSN(target_page.data), 200);
	UT_ASSERT_EQ(target_page.data[512], 'x');
	UT_ASSERT_EQ(space_reads, 1);
	UT_ASSERT_EQ(writes, 1);
	UT_ASSERT_EQ(dirties, 1);
	UT_ASSERT_EQ(flushes, 0);
	UT_ASSERT_EQ(syncs, 0);
	UT_ASSERT_EQ(strategies, 0);
	UT_ASSERT(!data_pins[0] && !data_pins[1] && !pinned && !locked);
}

UT_TEST(test_nonshared_buffer_copy_keeps_native_path)
{
	reset(MAIN_FORKNUM);
	buffered = true;
	cluster_shared_config = false;
	RelationCopyStorageUsingBuffer(source.smgr_rlocator.locator, destination.smgr_rlocator.locator,
								   test_fork, true);
	UT_ASSERT_EQ(old_wal, 1);
	UT_ASSERT_EQ(new_wal, 0);
	UT_ASSERT_EQ(space_reads, 0);
	UT_ASSERT_EQ(((PageHeader)target_page.data)->pd_block_scn, 7);
	UT_ASSERT_EQ(dirties, 1);
	UT_ASSERT_EQ(strategies, 0);
}

UT_TEST(test_buffer_copy_vm_and_fsm_classes)
{
	reset(VISIBILITYMAP_FORKNUM);
	buffered = true;
	RelationCopyStorageUsingBuffer(source.smgr_rlocator.locator, destination.smgr_rlocator.locator,
								   test_fork, true);
	UT_ASSERT_EQ(entry.page_class, RF_PAGE_CLASS_ORDINARY);
	UT_ASSERT_EQ(entry.before_kind, RF_PAGE_STATE_UNFORMATTED);
	UT_ASSERT_EQ(entry.result_kind, RF_PAGE_STATE_PRESENT);
	UT_ASSERT_EQ(((PageHeader)target_page.data)->pd_block_scn, 100);
	reset(FSM_FORKNUM);
	buffered = true;
	RelationCopyStorageUsingBuffer(source.smgr_rlocator.locator, destination.smgr_rlocator.locator,
								   test_fork, true);
	UT_ASSERT_EQ(entry.page_class, RF_PAGE_CLASS_REBUILDABLE_FSM);
	UT_ASSERT_EQ(entry.before_kind, RF_PAGE_STATE_REBUILDABLE);
	UT_ASSERT_EQ(entry.result_kind, RF_PAGE_STATE_REBUILDABLE);
	UT_ASSERT_EQ(entry.result_incarnation[0], 0);
	UT_ASSERT_EQ(((PageHeader)target_page.data)->pd_block_scn, 7);
	UT_ASSERT_EQ(dirties, 1);
	UT_ASSERT_EQ(flushes, 0);
}

UT_TEST(test_buffer_refusal_never_changes_target_or_emits_wal)
{
	PGAlignedBlock saved;

	reset(MAIN_FORKNUM);
	buffered = true;
	data_pins[0] = data_pins[1] = data_locks[0] = data_locks[1] = true;
	/* PageIsNew alone would wrongly accept this retained payload. */
	target_page.data[BLCKSZ - 1] = 1;
	saved = target_page;
	UT_ASSERT(PageIsNew(target_page.data));
	UT_ASSERT(!cluster_space_copy_buffer_wal(&identity, source_page.data, 3));
	UT_ASSERT(memcmp(&saved, &target_page, BLCKSZ) == 0);
	memset(&target_page, 0, BLCKSZ);
	saved = target_page;
	buffer_locator.relNumber++;
	UT_ASSERT(!cluster_space_copy_buffer_wal(&identity, source_page.data, 3));
	buffer_locator.relNumber--;
	identity.key.database_incarnation++;
	UT_ASSERT(!cluster_space_copy_buffer_wal(&identity, source_page.data, 3));
	identity.key.database_incarnation--;
	identity.state = CLUSTER_SPACE_IDENTITY_TOMBSTONED;
	UT_ASSERT(!cluster_space_copy_buffer_wal(&identity, source_page.data, 3));
	identity.state = CLUSTER_SPACE_IDENTITY_LIVE;
	UT_ASSERT(!cluster_space_copy_buffer_wal(&identity, target_page.data, 3));
	((PageHeader)source_page.data)->pd_flags |= PD_UNDO_SEG_HEADER;
	UT_ASSERT(!cluster_space_copy_buffer_wal(&identity, source_page.data, 3));
	((PageHeader)source_page.data)->pd_flags &= ~PD_UNDO_SEG_HEADER;
	test_fork = SPACE_FORKNUM;
	UT_ASSERT(!cluster_space_copy_buffer_wal(&identity, source_page.data, 3));
	test_fork = MAIN_FORKNUM;
	memset(&source_page, 0, BLCKSZ);
	UT_ASSERT(!cluster_space_copy_buffer_wal(&identity, source_page.data, 3));
	UT_ASSERT(memcmp(&saved, &target_page, BLCKSZ) == 0);
	UT_ASSERT_EQ(new_wal, 0);
	UT_ASSERT_EQ(edges, 0);
	UT_ASSERT_EQ(dirties, 0);
	UT_ASSERT_EQ(token, 99);
	UT_ASSERT(!begun);
	UT_ASSERT_EQ(CritSectionCount, 0);
}

UT_TEST(test_nonempty_destination_refused_before_bulk_extension)
{
	reset(MAIN_FORKNUM);
	buffered = true;
	writes = 2;
	expecting_error = true;
	if (setjmp(error_jump) == 0) {
		RelationCopyStorageUsingBuffer(source.smgr_rlocator.locator,
									   destination.smgr_rlocator.locator, test_fork, true);
		UT_ASSERT(false);
	}
	expecting_error = false;
	UT_ASSERT(strstr(last_error, "requires an empty destination fork") != NULL);
	UT_ASSERT_EQ(writes, 2);
	UT_ASSERT_EQ(strategies, 0);
	UT_ASSERT_EQ(new_wal, 0);
	UT_ASSERT_EQ(dirties, 0);
	UT_ASSERT(!pinned && !locked && !data_pins[0] && !data_pins[1]);
}

UT_TEST(test_buffer_copy_reopens_after_identity_read)
{
	RelFileLocator src, dst;

	reset(MAIN_FORKNUM);
	src = source.smgr_rlocator.locator;
	dst = destination.smgr_rlocator.locator;
	buffered = invalidate_on_read = true;
	RelationCopyStorageUsingBuffer(src, dst, test_fork, true);
	UT_ASSERT(handles_retired);
	UT_ASSERT_EQ(reopens, 2);
	UT_ASSERT_EQ(new_wal, 1);
	UT_ASSERT_EQ(dirties, 1);
	UT_ASSERT_EQ(strategies, 0);
}

UT_TEST(test_heap_init_records_actual_zero_before_and_native_layout)
{
	reset(MAIN_FORKNUM);
	buffered = true;
	data_pins[0] = data_pins[1] = data_locks[0] = data_locks[1] = true;
	UT_ASSERT(cluster_space_init_heap_buffer_wal(&identity, 3));
	UT_ASSERT(PageHasItl(target_page.data));
	UT_ASSERT_EQ(PageGetMaxOffsetNumber(target_page.data), 0);
	UT_ASSERT_EQ(((PageHeader)target_page.data)->pd_special,
				 BLCKSZ - MAXALIGN(CLUSTER_ITL_SPECIAL_SIZE));
	UT_ASSERT_EQ(((PageHeader)target_page.data)->pd_block_scn, 100);
	UT_ASSERT_EQ(entry.before_kind, RF_PAGE_STATE_UNFORMATTED);
	UT_ASSERT_EQ(entry.before.mutation_token, 0);
	UT_ASSERT(memcmp(entry.before.segment_incarnation, identity.incarnation, 16) == 0);
	UT_ASSERT(memcmp(entry.result_incarnation, identity.incarnation, 16) == 0);
	UT_ASSERT_EQ(entry.result_kind, RF_PAGE_STATE_PRESENT);
	UT_ASSERT_EQ(PageGetLSN(target_page.data), 200);
	UT_ASSERT_EQ(new_wal, 1);
	UT_ASSERT_EQ(dirties, 1);
	UT_ASSERT_EQ(flushes, 0);
	UT_ASSERT_EQ(CritSectionCount, 0);
}

UT_TEST(test_heap_init_refuses_nonzero_new_page_and_wrong_fork)
{
	PGAlignedBlock saved;

	reset(MAIN_FORKNUM);
	buffered = true;
	data_pins[0] = data_pins[1] = data_locks[0] = data_locks[1] = true;
	target_page.data[BLCKSZ - 1] = 1;
	saved = target_page;
	UT_ASSERT(PageIsNew(target_page.data));
	UT_ASSERT(!cluster_space_init_heap_buffer_wal(&identity, 3));
	UT_ASSERT(memcmp(saved.data, target_page.data, BLCKSZ) == 0);
	memset(&target_page, 0, BLCKSZ);
	test_fork = VISIBILITYMAP_FORKNUM;
	UT_ASSERT(!cluster_space_init_heap_buffer_wal(&identity, 3));
	test_fork = MAIN_FORKNUM;
	identity.state = CLUSTER_SPACE_IDENTITY_TOMBSTONED;
	UT_ASSERT(!cluster_space_init_heap_buffer_wal(&identity, 3));
	UT_ASSERT_EQ(new_wal, 0);
	UT_ASSERT_EQ(edges, 0);
	UT_ASSERT_EQ(dirties, 0);
}

UT_TEST(test_vm_init_versions_zero_before_without_heap_layout)
{
	reset(VISIBILITYMAP_FORKNUM);
	buffered = true;
	data_pins[0] = data_pins[1] = data_locks[0] = data_locks[1] = true;
	UT_ASSERT(cluster_space_init_vm_buffer_wal(&identity, 3));
	UT_ASSERT(!PageHasItl(target_page.data));
	UT_ASSERT_EQ(((PageHeader)target_page.data)->pd_special, BLCKSZ);
	UT_ASSERT_EQ(((PageHeader)target_page.data)->pd_lower, SizeOfPageHeaderData);
	UT_ASSERT_EQ(((PageHeader)target_page.data)->pd_block_scn, 100);
	UT_ASSERT_EQ(entry.before_kind, RF_PAGE_STATE_UNFORMATTED);
	UT_ASSERT_EQ(entry.before.mutation_token, 0);
	UT_ASSERT(memcmp(entry.before.segment_incarnation, identity.incarnation, 16) == 0);
	UT_ASSERT_EQ(entry.result_kind, RF_PAGE_STATE_PRESENT);
	UT_ASSERT_EQ(PageGetLSN(target_page.data), 200);
	UT_ASSERT_EQ(new_wal, 1);
	UT_ASSERT_EQ(dirties, 1);
	UT_ASSERT_EQ(flushes, 0);
}

UT_TEST(test_vm_init_refuses_nonzero_before_and_wrong_fork)
{
	PGAlignedBlock before;

	reset(VISIBILITYMAP_FORKNUM);
	buffered = true;
	data_pins[0] = data_pins[1] = data_locks[0] = data_locks[1] = true;
	target_page.data[BLCKSZ - 1] = 1;
	before = target_page;
	UT_ASSERT(!cluster_space_init_vm_buffer_wal(&identity, 3));
	UT_ASSERT(memcmp(&before, &target_page, BLCKSZ) == 0);
	memset(&target_page, 0, BLCKSZ);
	test_fork = MAIN_FORKNUM;
	UT_ASSERT(!cluster_space_init_vm_buffer_wal(&identity, 3));
	test_fork = VISIBILITYMAP_FORKNUM;
	identity.state = CLUSTER_SPACE_IDENTITY_TOMBSTONED;
	UT_ASSERT(!cluster_space_init_vm_buffer_wal(&identity, 3));
	UT_ASSERT_EQ(new_wal, 0);
	UT_ASSERT_EQ(dirties, 0);
}

UT_TEST(test_minimal_wal_copy_uses_versioned_producers)
{
	minimal_wal_copy = true;
	test_copy_main_has_new_identity_edge_and_wal_before_data();
	test_native_copy_reopens_handles_after_identity_buffer_read();
	test_buffer_copy_reopens_after_identity_read();
	minimal_wal_copy = false;
}

int
main(void)
{
	UT_PLAN(16);
	UT_RUN(test_vm_init_versions_zero_before_without_heap_layout);
	UT_RUN(test_vm_init_refuses_nonzero_before_and_wrong_fork);
	UT_RUN(test_minimal_wal_copy_uses_versioned_producers);
	UT_RUN(test_heap_init_records_actual_zero_before_and_native_layout);
	UT_RUN(test_heap_init_refuses_nonzero_new_page_and_wrong_fork);
	UT_RUN(test_copy_main_has_new_identity_edge_and_wal_before_data);
	UT_RUN(test_nonshared_copy_keeps_native_path);
	UT_RUN(test_vm_is_ordinary_but_fsm_is_explicitly_rebuildable);
	UT_RUN(test_copy_refusal_leaves_page_and_output_unchanged);
	UT_RUN(test_native_copy_reopens_handles_after_identity_buffer_read);
	UT_RUN(test_buffer_copy_records_real_unformatted_predecessor);
	UT_RUN(test_nonshared_buffer_copy_keeps_native_path);
	UT_RUN(test_buffer_copy_vm_and_fsm_classes);
	UT_RUN(test_buffer_refusal_never_changes_target_or_emits_wal);
	UT_RUN(test_nonempty_destination_refused_before_bulk_extension);
	UT_RUN(test_buffer_copy_reopens_after_identity_read);
	UT_DONE();
	return ut_failed_count ? 1 : 0;
}
