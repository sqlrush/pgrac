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

#include "access/xact.h"
#include "access/xlog.h"
#include "access/xloginsert.h"
#include "catalog/pg_class.h"
#include "catalog/pg_control.h"
#include "catalog/pg_tablespace_d.h"
#include "catalog/storage.h"
#include "cluster/cluster_itl_slot.h"
#include "cluster/cluster_guc.h"
#include "cluster/cluster_ko.h"
#include "cluster/cluster_page_wal.h"
#include "cluster/cluster_pcm_x_bufmgr.h"
#include "cluster/cluster_space_storage.h"
#include "cluster/cluster_wal_thread.h"
#include "cluster/storage/cluster_smgr.h"
#include "miscadmin.h"
#include "storage/bufmgr.h"
#include "storage/smgr.h"
#include "storage/shmem.h"
#include "utils/rel.h"
#include "unit_test.h"

UT_DEFINE_GLOBALS();

bool cluster_enabled = true, cluster_shared_config = true, cluster_shared_catalog;
int cluster_node_id = 0, wal_level = WAL_LEVEL_REPLICA;
int cluster_interconnect_tier = CLUSTER_IC_TIER_1;
sigjmp_buf *PG_exception_stack;
ErrorContextCallback *error_context_stack;
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
static BufferDescPadded descriptors[3];
BufferDescPadded *BufferDescriptors = descriptors;
static void *binding_memory;
static bool source_available, lose_source_on_insert, capture_permitted;
static uint32 wait_event;
uint32 *my_wait_event_info = &wait_event;

/* The file-copy fixture invokes the native producer in a top-level xact. */
bool
IsSubTransaction(void)
{
	return false;
}

Size
add_size(Size a, Size b)
{
	return a + b;
}
Size
mul_size(Size a, Size b)
{
	return a * b;
}
void *
ShmemInitStruct(const char *name, Size size, bool *found)
{
	free(binding_memory);
	binding_memory = calloc(1, size);
	*found = false;
	return binding_memory;
}

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
bool
LWLockHeldByMeInMode(LWLock *lock, LWLockMode mode)
{
	return lock == BufferDescriptorGetContentLock(&descriptors[2].bufferdesc) && data_locks[1];
}
bool
LWLockHeldByMe(LWLock *lock)
{
	return LWLockHeldByMeInMode(lock, LW_EXCLUSIVE);
}
bool
cluster_bufmgr_pcm_x_content_holder_write_permitted(BufferDesc *buf)
{
	return buf == &descriptors[2].bufferdesc && capture_permitted && data_locks[1];
}
static unsigned dirties, strategies;
static RelFileLocator buffer_locator;
static bool expecting_error;
static jmp_buf error_jump;
static char last_error[256];
static int last_error_level;
static unsigned reservations;
static bool reservation_refused, invalidate_on_reserve;
static unsigned copy_storage_checks;
static bool refuse_copy_storage;

/* The real catalog/WAL producers run below. Only the existing bounded KO
 * owner and external IO are fixtures; errors follow the product PG_CATCH.
 * Author: SqlRush <sqlrush@gmail.com> */
enum CopySite {
	COPY_SPACE = 1,
	COPY_RESERVE,
	COPY_READ,
	COPY_WAL,
	COPY_FLUSH,
	COPY_WRITE,
	COPY_SYNC,
	COPY_SOURCE_BUFFER,
	COPY_DEST_BUFFER,
	COPY_RELEASE
};
static bool copy_begin_allowed, copy_permission, copy_live, copy_aborting;
static bool copy_armed, private_storage, temporary_copy;
static bool copy_failed_retained;
static unsigned copy_begins, copy_ends, copy_aborts, copy_observed;
static unsigned copy_fail_site, copy_lose_site;
static BlockNumber source_blocks;
static unsigned copy_targets, copy_buffer_reads, copy_target_checks;
static bool buffer_copy_scope;
static bool fixture_recovering;

/* Prospective typed producer entry; its consumer is tested in ko_stop. */
bool cluster_ko_buffer_copy_begin_v3(RelFileLocator locator, ForkNumber fork, BlockNumber nblocks,
									 uint64 *serial);
bool cluster_ko_buffer_copy_target_v3(RelFileLocator locator, uint64 serial, ForkNumber fork,
									  BlockNumber block, ClusterKoCurrentAdmissionV3 *out);

static void
copy_observe(enum CopySite site)
{
	if (copy_live && copy_armed && !copy_aborting)
		copy_observed |= 1U << (site - 1);
	if (copy_lose_site == site)
		copy_permission = false;
	if (copy_fail_site == site)
		elog(ERROR, "fixture raw copy IO failure");
}

bool
cluster_ko_copy_begin_v3(RelFileLocator locator, uint64 *serial)
{
	UT_ASSERT(RelFileLocatorEquals(locator, identity.key.locator));
	UT_ASSERT(!copy_live);
	*serial = 0;
	copy_begins++;
	if (!copy_begin_allowed)
		return false;
	copy_live = true;
	copy_armed = copy_aborting = false;
	*serial = 77;
	return true;
}

bool
cluster_ko_create_begin_v3(RelFileLocator locator, uint64 *serial)
{
	/* The real constructor state machine is independently tested by ko_stop.
	 * Here only its boundary drives the actual auxiliary-fork caller. */
	UT_ASSERT(!buffer_copy_scope);
	return cluster_ko_copy_begin_v3(locator, serial);
}

bool
cluster_ko_buffer_copy_begin_v3(RelFileLocator locator, ForkNumber fork, BlockNumber nblocks,
								uint64 *serial)
{
	UT_ASSERT(buffered && fork == test_fork && nblocks == source_blocks && nblocks != 0);
	/* writes also represents pre-existing destination size in the original
	 * nonempty-file negative; that is not an effect of this invocation. */
	UT_ASSERT_EQ(space_reads + reservations + new_wal, 0);
	buffer_copy_scope = true;
	return cluster_ko_copy_begin_v3(locator, serial);
}

bool
cluster_ko_buffer_copy_target_v3(RelFileLocator locator, uint64 serial, ForkNumber fork,
								 BlockNumber block, ClusterKoCurrentAdmissionV3 *out)
{
	UT_ASSERT(buffer_copy_scope && fork == test_fork && block == 0);
	UT_ASSERT(!pinned && !data_pins[0] && !data_pins[1]);
	UT_ASSERT(!locked && !data_locks[0] && !data_locks[1]);
	copy_targets++;
	memset(out, 0, sizeof(*out));
	if (!cluster_ko_create_write_begin_v3(locator, serial))
		return false;
	out->target.locator = locator;
	out->target.fork = fork;
	out->target.block = block;
	out->target.extension_serial = serial;
	return true;
}

bool
cluster_ko_peer_current_valid_v3(const ClusterKoCurrentAdmissionV3 *sample, RelFileLocator locator,
								 ForkNumber fork, BlockNumber block)
{
	UT_ASSERT(data_locks[0] && data_locks[1] && data_pins[0] && data_pins[1]);
	UT_ASSERT_EQ(CritSectionCount, 0);
	copy_target_checks++;
	return sample != NULL && sample->target.extension_serial == 77 && copy_live && !copy_aborting
		   && copy_permission && sample->target.fork == fork && sample->target.block == block
		   && RelFileLocatorEquals(sample->target.locator, locator);
}

bool
cluster_ko_create_write_begin_v3(RelFileLocator locator, uint64 serial)
{
	UT_ASSERT(RelFileLocatorEquals(locator, identity.key.locator));
	UT_ASSERT_EQ(serial, 77);
	if (!copy_live || copy_aborting || !copy_permission)
		return false;
	copy_armed = true;
	return true;
}

void
cluster_ko_create_abort_v3(uint64 serial)
{
	if (serial != 0) {
		UT_ASSERT_EQ(serial, 77);
		UT_ASSERT(copy_live);
		copy_aborting = true;
		copy_aborts++;
	}
}

bool
cluster_ko_create_end_v3(uint64 serial, bool complete)
{
	UT_ASSERT_EQ(serial, 77);
	UT_ASSERT(copy_live && complete && !copy_aborting);
	copy_ends++;
	if (buffer_copy_scope)
		UT_ASSERT(!data_pins[0] && !data_pins[1] && !data_locks[0] && !data_locks[1]
				  && strategies == 0);
	copy_live = false;
	if (!copy_permission) {
		copy_failed_retained = copy_armed;
		return false;
	}
	return true;
}

bool fixture_copy_reserve(const ClusterSpaceIdentity *id, BlockNumber first, uint32 count);

bool
fixture_copy_reserve(const ClusterSpaceIdentity *id, BlockNumber first, uint32 count)
{
	copy_observe(COPY_RESERVE);
	UT_ASSERT(!pinned && !locked && !data_pins[0] && !data_pins[1]);
	UT_ASSERT_EQ(first, 0);
	UT_ASSERT_EQ(count, 1);
	UT_ASSERT(memcmp(id, &identity, sizeof(identity)) == 0);
	UT_ASSERT_EQ(writes + new_wal, 0);
	if (reservation_refused)
		return false;
	if (invalidate_on_reserve) {
		memset(&source, 0xa5, sizeof(source));
		memset(&destination, 0xa5, sizeof(destination));
		handles_retired = true;
	}
	reservations++;
	return true;
}

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
	if (expecting_error)
		elog(ERROR, "fixture copy cancel");
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
	return fixture_recovering;
}

bool
cluster_wal_thread_current_v2_ref(ClusterWalSourceRef *out)
{
	if (!source_available)
		return false;
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
	return private_storage ? 0 : 1;
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
	return rel == current_src ? source_blocks : writes;
}

void
smgrread(SMgrRelation rel, ForkNumber forknum, BlockNumber block, void *out)
{
	if (rel != (handles_retired ? &reopened_source : &source) || forknum != test_fork || block != 0)
		abort();
	copy_observe(COPY_READ);
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
	if (cluster_shared_config && !private_storage && !temporary_copy && forknum == MAIN_FORKNUM)
		UT_ASSERT_EQ(reservations, 1);
	if (buffered) {
		PGAlignedBlock zero;

		memset(&zero, 0, sizeof(zero));
		UT_ASSERT(memcmp(bytes, zero.data, BLCKSZ) == 0);
		UT_ASSERT_EQ(new_wal, 0);
		memcpy(target_page.data, bytes, BLCKSZ);
	} else if (cluster_shared_config && !private_storage && !temporary_copy)
		UT_ASSERT_EQ(flushes, 1);
	copy_observe(COPY_WRITE);
	memcpy(written.data, bytes, BLCKSZ);
	writes++;
}

void
smgrimmedsync(SMgrRelation rel, ForkNumber forknum)
{
	if (rel != (handles_retired ? &reopened_destination : &destination) || forknum != test_fork
		|| writes != source_blocks)
		abort();
	copy_observe(COPY_SYNC);
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
		copy_buffer_reads++;
		copy_observe(is_source ? COPY_SOURCE_BUFFER : COPY_DEST_BUFFER);
		return is_source ? 2 : 3;
	}
	if (!RelFileLocatorEquals(tag, identity.key.locator) || forknum != SPACE_FORKNUM || block != 0
		|| mode != RBM_NORMAL || strategy != NULL || !permanent || pinned)
		abort();
	copy_observe(COPY_SPACE);
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
		if (slot == 0)
			copy_observe(COPY_RELEASE);
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
	pg_atomic_fetch_or_u32(&descriptors[2].bufferdesc.state, BM_DIRTY);
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
	copy_observe(COPY_WAL);
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
	if (lose_source_on_insert)
		source_available = false;
	return 200;
}

bool
XLogGetLastInsertRecord(XLogRecPtr end, XLogRecPtr *start, XLogRecord *record)
{
	UT_ASSERT(!begun && new_wal == 1 && buffered && data_locks[1]);
	UT_ASSERT_EQ(CritSectionCount, 1);
	UT_ASSERT_EQ(end, 200);
	UT_ASSERT_EQ(PageGetLSN(target_page.data), 200);
	UT_ASSERT_EQ(((PageHeader)target_page.data)->pd_block_scn, token);
	memset(record, 0, sizeof(*record));
	record->xl_rmid = RM_XLOG_ID;
	record->xl_info = XLOG_FPI;
	record->xl_crc = 0x1928;
	*start = 120;
	return true;
}

void
XLogFlush(XLogRecPtr lsn)
{
	if (lsn != 200 || new_wal != 1 || writes != 0)
		abort();
	copy_observe(COPY_FLUSH);
	flushes++;
}

int
errmsg(const char *fmt, ...)
{
	va_list args;

	if (!expecting_error)
		abort();
	va_start(args, fmt);
	vsnprintf(last_error, sizeof(last_error), fmt, args);
	va_end(args);
	return 0;
}

bool
errstart(int elevel, const char *domain)
{
	(void)domain;
	if (elevel == DEBUG1)
		return false;
	if (expecting_error && elevel >= ERROR) {
		last_error_level = elevel;
		return true;
	}
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
	return 0;
}

void
pg_re_throw(void)
{
	if (!expecting_error)
		abort();
	if (PG_exception_stack != NULL)
		siglongjmp(*PG_exception_stack, 1);
	longjmp(error_jump, 1);
}

void
errfinish(const char *file, int line, const char *function)
{
	(void)file;
	(void)line;
	(void)function;
	pg_re_throw();
}

int
errcode(int code)
{
	(void)code;
	return 0;
}

int
errhint(const char *fmt, ...)
{
	(void)fmt;
	abort();
}

int
errdetail(const char *fmt, ...)
{
	(void)fmt;
	abort();
}

/* Exact original buffer-copy body, not a fixture reimplementation. */
static void
fixture_copy_storage_check(RelFileLocator locator, char persistence, const char *operation)
{
	UT_ASSERT(RelFileLocatorEquals(locator, destination.smgr_rlocator.locator));
	UT_ASSERT_EQ(persistence, RELPERSISTENCE_PERMANENT);
	UT_ASSERT(strcmp(operation, "copy") == 0);
	copy_storage_checks++;
	if (refuse_copy_storage)
		longjmp(error_jump, 1);
}

#define RelationCheckStorageTransaction fixture_copy_storage_check
#define cluster_space_reserve_exact fixture_copy_reserve
#include "test_cluster_space_copy_buffer.inc"
#undef cluster_space_reserve_exact
#undef RelationCheckStorageTransaction

/* Execute the complete auxiliary-fork producer with explicit native IO/WAL,
 * MAIN constructor and downstream buffered-copy boundaries. No native CREATE
 * record, filesystem, DATA durability or combined KO/fabric is simulated. */
enum ForkCreateSite { FORK_OPEN = 1, FORK_CREATE, FORK_WAL };
static uint32 extra_forks;
static unsigned fork_opens, fork_creates, fork_wal, fork_copies, fork_closes;
static unsigned main_creates, main_copies, fork_owner_observed;
static unsigned fork_fail_site, fork_lose_site;
static ForkNumber fork_last_created;
static bool fork_refuse_next, fork_permanent;

static void
fixture_fork_observe(enum ForkCreateSite site)
{
	if (copy_live && copy_armed && !copy_aborting)
		fork_owner_observed |= 1U << (site - 1);
	if (fork_lose_site == site)
		copy_permission = false;
	if (fork_fail_site == site)
		elog(ERROR, "fixture auxiliary fork failure");
}

static SMgrRelation
fixture_copy_create_main(RelFileLocator locator, char persistence, bool register_delete)
{
	UT_ASSERT(RelFileLocatorEquals(locator, identity.key.locator));
	UT_ASSERT_EQ(persistence, fork_permanent ? RELPERSISTENCE_PERMANENT : RELPERSISTENCE_UNLOGGED);
	UT_ASSERT(!register_delete && !copy_live);
	main_creates++;
	return &destination;
}

static void
fixture_copy_fork_data(RelFileLocator src, RelFileLocator dst, ForkNumber fork, bool permanent)
{
	UT_ASSERT(RelFileLocatorEquals(src, source.smgr_rlocator.locator));
	UT_ASSERT(RelFileLocatorEquals(dst, identity.key.locator));
	UT_ASSERT(permanent == fork_permanent && !copy_live);
	if (fork == MAIN_FORKNUM)
		main_copies++;
	else {
		UT_ASSERT_EQ(fork, fork_last_created);
		fork_copies++;
		if (fork_refuse_next)
			copy_begin_allowed = false;
	}
}

static SMgrRelation
fixture_copy_fork_open(RelFileLocator locator, BackendId backend)
{
	if (RelFileLocatorEquals(locator, identity.key.locator)) {
		fork_opens++;
		fixture_fork_observe(FORK_OPEN);
	}
	return smgropen(locator, backend);
}

static bool
fixture_copy_fork_exists(SMgrRelation rel, ForkNumber fork)
{
	UT_ASSERT(rel == &source && fork > MAIN_FORKNUM && fork <= MAX_FORKNUM);
	UT_ASSERT(fork != SPACE_FORKNUM);
	return (extra_forks & (1U << fork)) != 0;
}

static void
fixture_copy_fork_create(SMgrRelation rel, ForkNumber fork, bool is_redo)
{
	UT_ASSERT(rel == &destination && !is_redo && fork != SPACE_FORKNUM);
	UT_ASSERT((extra_forks & (1U << fork)) != 0);
	fork_creates++;
	fork_last_created = fork;
	fixture_fork_observe(FORK_CREATE);
}

static void
fixture_copy_fork_wal(const RelFileLocator *locator, ForkNumber fork)
{
	UT_ASSERT(RelFileLocatorEquals(*locator, identity.key.locator));
	UT_ASSERT_EQ(fork, fork_last_created);
	UT_ASSERT(fork_permanent || fork == INIT_FORKNUM);
	fork_wal++;
	fixture_fork_observe(FORK_WAL);
}

static void
fixture_copy_fork_close(RelFileLocatorBackend locator)
{
	UT_ASSERT(locator.backend == InvalidBackendId && !copy_live);
	UT_ASSERT(RelFileLocatorEquals(locator.locator, source.smgr_rlocator.locator)
			  || RelFileLocatorEquals(locator.locator, identity.key.locator));
	fork_closes++;
}

#define RelationCreateStorage fixture_copy_create_main
#define RelationCopyStorageUsingBuffer fixture_copy_fork_data
#define smgropen fixture_copy_fork_open
#define smgrexists fixture_copy_fork_exists
#define smgrcreate fixture_copy_fork_create
#define log_smgrcreate fixture_copy_fork_wal
#define smgrcloserellocator fixture_copy_fork_close
#include "test_cluster_space_copy_create.inc"
#undef RelationCreateStorage
#undef RelationCopyStorageUsingBuffer
#undef smgropen
#undef smgrexists
#undef smgrcreate
#undef log_smgrcreate
#undef smgrcloserellocator

/* The two tablespace-copy callers execute in full. Their original MAIN,
 * raw-copy, flush/drop and SMGR operations are explicit fixture boundaries. */
static RelationData raw_relation;
static FormData_pg_class raw_relation_class;
static unsigned raw_opens, raw_flushes, raw_drops, raw_closes;

void
smgrsetowner(SMgrRelation *owner, SMgrRelation rel)
{
	/* Both callers start with the original source handle installed. */
	abort();
}

static void
fixture_raw_check(RelFileLocator locator, char persistence, const char *operation)
{
	UT_ASSERT(RelFileLocatorEquals(locator, identity.key.locator));
	UT_ASSERT_EQ(persistence, raw_relation_class.relpersistence);
	UT_ASSERT(strcmp(operation, "copy") == 0);
	copy_storage_checks++;
	if (refuse_copy_storage)
		longjmp(error_jump, 1);
}

static SMgrRelation
fixture_raw_open(RelFileLocator locator, BackendId backend)
{
	UT_ASSERT(RelFileLocatorEquals(locator, identity.key.locator));
	UT_ASSERT_EQ(backend, raw_relation.rd_backend);
	UT_ASSERT(!copy_live);
	raw_opens++;
	return &destination;
}

static SMgrRelation
fixture_raw_main(RelFileLocator locator, char persistence, bool register_delete)
{
	UT_ASSERT(RelFileLocatorEquals(locator, identity.key.locator));
	UT_ASSERT_EQ(persistence, raw_relation_class.relpersistence);
	UT_ASSERT(register_delete && !copy_live);
	main_creates++;
	return &destination;
}

static void
fixture_raw_data(SMgrRelation src, SMgrRelation dst, ForkNumber fork, char persistence)
{
	UT_ASSERT(src == &source && dst == &destination);
	UT_ASSERT_EQ(persistence, raw_relation_class.relpersistence);
	fixture_copy_fork_data(src->smgr_rlocator.locator, dst->smgr_rlocator.locator, fork,
						   persistence == RELPERSISTENCE_PERMANENT);
}

static void
fixture_raw_flush(Relation rel)
{
	UT_ASSERT(rel == &raw_relation && !copy_live);
	raw_flushes++;
}

static void
fixture_raw_drop(Relation rel)
{
	UT_ASSERT(rel == &raw_relation && !copy_live);
	raw_drops++;
}

static void
fixture_raw_close(SMgrRelation rel)
{
	UT_ASSERT(rel == &destination && !copy_live);
	raw_closes++;
}

#define RelationCheckStorageTransaction fixture_raw_check
#define RelationCreateStorage fixture_raw_main
#define RelationCopyStorage fixture_raw_data
#define FlushRelationBuffers fixture_raw_flush
#define RelationDropStorage fixture_raw_drop
#define smgropen fixture_raw_open
#define smgrexists fixture_copy_fork_exists
#define smgrcreate fixture_copy_fork_create
#define log_smgrcreate fixture_copy_fork_wal
#define smgrclose fixture_raw_close
#include "test_cluster_space_copy_index.inc"
#include "test_cluster_space_copy_heap.inc"
#undef RelationCheckStorageTransaction
#undef RelationCreateStorage
#undef RelationCopyStorage
#undef FlushRelationBuffers
#undef RelationDropStorage
#undef smgropen
#undef smgrexists
#undef smgrcreate
#undef log_smgrcreate
#undef smgrclose

static void
run_raw_caller(unsigned caller)
{
	if (caller == 0)
		index_copy_data(&raw_relation, identity.key.locator);
	else
		heapam_relation_copy_data(&raw_relation, &identity.key.locator);
}

static void
reset(ForkNumber forknum)
{
	PageHeader header;
	UT_ASSERT(PG_exception_stack == NULL);

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
	last_error_level = 0;
	invalidate_on_read = handles_retired = false;
	reopens = 0;
	old_wal = new_wal = edges = writes = syncs = flushes = space_reads = 0;
	reservations = 0;
	reservation_refused = invalidate_on_reserve = false;
	copy_storage_checks = 0;
	refuse_copy_storage = false;
	copy_begin_allowed = copy_permission = true;
	copy_live = copy_aborting = copy_armed = private_storage = temporary_copy = false;
	copy_failed_retained = false;
	copy_begins = copy_ends = copy_aborts = copy_observed = 0;
	copy_fail_site = copy_lose_site = 0;
	copy_targets = copy_buffer_reads = copy_target_checks = 0;
	buffer_copy_scope = false;
	fixture_recovering = false;
	extra_forks = 0;
	fork_opens = fork_creates = fork_wal = fork_copies = fork_closes = 0;
	main_creates = main_copies = fork_owner_observed = 0;
	fork_fail_site = fork_lose_site = 0;
	fork_last_created = InvalidForkNumber;
	fork_refuse_next = false;
	fork_permanent = true;
	memset(&raw_relation, 0, sizeof(raw_relation));
	memset(&raw_relation_class, 0, sizeof(raw_relation_class));
	raw_relation.rd_rel = &raw_relation_class;
	raw_relation.rd_smgr = &source;
	raw_relation.rd_locator = source.smgr_rlocator.locator;
	raw_relation.rd_backend = InvalidBackendId;
	raw_relation_class.relpersistence = RELPERSISTENCE_PERMANENT;
	raw_opens = raw_flushes = raw_drops = raw_closes = 0;
	source_blocks = 1;
	InterruptPending = false;
	cluster_enabled = true;
	cluster_shared_config = true;
	test_fork = forknum;
	token = 99;
	source_available = capture_permitted = true;
	lose_source_on_insert = false;
	ref.claim.identity.authority_uuid[0] = 31;
	ref.claim.identity.origin_thread_id = 1;
	ref.claim.identity.thread_claim_created_at = 32;
	ref.claim.identity.origin_owner_incarnation = 33;
	ref.claim.identity.root_lineage_seq = 1;
	ref.claim.max_config_generation = 1;
	ref.claim.claim_sha256[0] = 34;
	ref.timeline = 1;
	memset(descriptors, 0, sizeof(descriptors));
	for (int i = 0; i < 3; i++) {
		descriptors[i].bufferdesc.buf_id = i;
		InitBufferTag(&descriptors[i].bufferdesc.tag, &identity.key.locator, forknum, 0);
		pg_atomic_init_u32(&descriptors[i].bufferdesc.state,
						   BM_VALID | BM_TAG_VALID | BM_PERMANENT);
	}
	cluster_page_wal_shmem_init();
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
	UT_ASSERT_EQ(reopens, 4); /* Identity and reservation reads each retire handles. */
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

UT_TEST(test_buffer_copy_checks_storage_eligibility_before_io)
{
	reset(MAIN_FORKNUM);
	buffered = true;
	RelationCopyStorageUsingBuffer(source.smgr_rlocator.locator, destination.smgr_rlocator.locator,
								   test_fork, true);
	UT_ASSERT_EQ(copy_storage_checks, 1);
	UT_ASSERT_EQ(space_reads, 1);
	UT_ASSERT_EQ(writes, 1);
}

UT_TEST(test_buffer_copy_refusal_has_no_storage_effect)
{
	volatile bool caught = false;

	reset(MAIN_FORKNUM);
	buffered = true;
	refuse_copy_storage = expecting_error = true;
	if (setjmp(error_jump) == 0) {
		RelationCopyStorageUsingBuffer(source.smgr_rlocator.locator,
									   destination.smgr_rlocator.locator, test_fork, true);
		UT_ASSERT(false);
	} else
		caught = true;
	expecting_error = false;
	UT_ASSERT(caught);
	UT_ASSERT_EQ(copy_storage_checks, 1);
	UT_ASSERT_EQ(space_reads, 0);
	UT_ASSERT_EQ(writes + new_wal + dirties + reservations, 0);
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

UT_TEST(test_copy_reserves_before_data_and_reopens_after_current_read)
{
	for (int variant = 0; variant < 2; variant++) {
		reset(MAIN_FORKNUM);
		buffered = variant != 0;
		invalidate_on_reserve = true;
		if (buffered)
			RelationCopyStorageUsingBuffer(source.smgr_rlocator.locator,
										   destination.smgr_rlocator.locator, MAIN_FORKNUM, true);
		else
			RelationCopyStorage(&source, &destination, MAIN_FORKNUM, RELPERSISTENCE_PERMANENT);
		UT_ASSERT_EQ(reservations, 1);
		UT_ASSERT_EQ(writes, 1);
		UT_ASSERT(reopens >= 1);
	}
}

UT_TEST(test_copy_refuses_before_any_zero_fill_or_wal_without_reservation)
{
	for (int variant = 0; variant < 2; variant++) {
		reset(MAIN_FORKNUM);
		buffered = variant != 0;
		reservation_refused = expecting_error = true;
		if (setjmp(error_jump) == 0) {
			if (buffered)
				RelationCopyStorageUsingBuffer(source.smgr_rlocator.locator,
											   destination.smgr_rlocator.locator, MAIN_FORKNUM,
											   true);
			else
				RelationCopyStorage(&source, &destination, MAIN_FORKNUM, RELPERSISTENCE_PERMANENT);
			UT_ASSERT(false);
		}
		expecting_error = false;
		UT_ASSERT_EQ(writes + new_wal + flushes + reservations, 0);
	}
}

static ClusterPageWalFirstResultV1
resident_first(ClusterPageWalRefV1 *first)
{
	ClusterPageWalFirstResultV1 result;
	BufferDesc *buf = &descriptors[2].bufferdesc;
	pg_atomic_fetch_or_u32(&buf->state, BM_LOCKED);
	result = cluster_page_wal_first_observe_locked_v1(buf, first);
	pg_atomic_fetch_and_u32(&buf->state, ~BM_LOCKED);
	return result;
}

/* Real producer + real resident source pool; only WAL insertion and its
 * last-record metadata are the explicit fixture boundary. */
UT_TEST(test_resident_init_and_copy_bind_the_first_record)
{
	for (int variant = 0; variant < 3; variant++) {
		ClusterPageWalRefV1 first;
		ClusterPageWalBindingV1 latest = { 0 };
		ClusterPageWalDirtyFloorV1 floor;
		reset(variant == 1 ? VISIBILITYMAP_FORKNUM : MAIN_FORKNUM);
		buffered = true;
		data_pins[0] = data_locks[0] = data_pins[1] = data_locks[1] = true;
		UT_ASSERT(variant == 0	 ? cluster_space_init_heap_buffer_wal(&identity, 3)
				  : variant == 1 ? cluster_space_init_vm_buffer_wal(&identity, 3)
								 : cluster_space_copy_buffer_wal(&identity, source_page.data, 3));
		UT_ASSERT_EQ(resident_first(&first), CLUSTER_PAGE_WAL_FIRST_PRESENT);
		UT_ASSERT_EQ(first.start, 120);
		UT_ASSERT_EQ(first.end, 200);
		UT_ASSERT_EQ(first.crc, 0x1928);
		UT_ASSERT_EQ(first.rmid, RM_XLOG_ID);
		UT_ASSERT_EQ(first.info, XLOG_FPI);
		UT_ASSERT(cluster_page_wal_snapshot_v1(3, &latest));
		UT_ASSERT_EQ(latest.record_start, 120);
		UT_ASSERT(cluster_page_wal_dirty_floor_v1(&ref, &floor));
		UT_ASSERT_EQ(floor.floor, 120);
		UT_ASSERT_EQ(floor.dirty, 1);
		UT_ASSERT_EQ(flushes + syncs, 0);

		/* A later mutation cannot replace the still-unwritten init/copy. */
		((PageHeader)target_page.data)->pd_block_scn = 101;
		UT_ASSERT_EQ(
			cluster_page_wal_capture_native_v1(3, &entry, 101, 210, 300, 0x1929, RM_HEAP_ID, 0),
			CLUSTER_PAGE_WAL_CAPTURED);
		PageSetLSN(target_page.data, 300);
		UT_ASSERT_EQ(resident_first(&first), CLUSTER_PAGE_WAL_FIRST_PRESENT);
		UT_ASSERT_EQ(first.start, 120);
		UT_ASSERT(cluster_page_wal_snapshot_v1(3, &latest));
		UT_ASSERT_EQ(latest.record_start, 210);
	}
}

UT_TEST(test_resident_capture_loss_keeps_unknown_first)
{
	ClusterPageWalRefV1 first;
	ClusterPageWalBindingV1 latest = { 0 };
	ClusterPageWalDirtyFloorV1 floor;
	reset(MAIN_FORKNUM);
	buffered = true;
	data_pins[0] = data_locks[0] = data_pins[1] = data_locks[1] = true;
	lose_source_on_insert = true;
	UT_ASSERT(cluster_space_init_heap_buffer_wal(&identity, 3));
	UT_ASSERT_EQ(resident_first(&first), CLUSTER_PAGE_WAL_FIRST_UNATTRIBUTED);
	UT_ASSERT_EQ(first.start, 120);
	UT_ASSERT(!cluster_page_wal_snapshot_v1(3, &latest));
	UT_ASSERT(cluster_page_wal_dirty_floor_v1(&ref, &floor));
	UT_ASSERT_EQ(floor.unattributed, 1);
	UT_ASSERT_EQ(flushes + syncs, 0);
}

UT_TEST(test_resident_broken_owner_panics_before_unlock)
{
	volatile bool caught = false;
	reset(MAIN_FORKNUM);
	buffered = true;
	data_pins[0] = data_locks[0] = data_pins[1] = data_locks[1] = true;
	capture_permitted = false;
	expecting_error = true;
	if (setjmp(error_jump) == 0)
		(void)cluster_space_init_heap_buffer_wal(&identity, 3);
	else
		caught = true;
	expecting_error = false;
	UT_ASSERT(caught);
	UT_ASSERT_EQ(last_error_level, PANIC);
	UT_ASSERT_EQ(CritSectionCount, 1);
	UT_ASSERT(data_locks[1]);
	UT_ASSERT_EQ(new_wal, 1);
	UT_ASSERT_EQ(flushes + syncs, 0);
	CritSectionCount = 0;
}

UT_TEST(test_raw_copy_fence_refusal_precedes_target_effects)
{
	volatile bool caught = false;

	reset(MAIN_FORKNUM);
	copy_begin_allowed = false;
	expecting_error = true;
	if (setjmp(error_jump) == 0)
		RelationCopyStorage(&source, &destination, test_fork, RELPERSISTENCE_PERMANENT);
	else
		caught = true;
	expecting_error = false;
	UT_ASSERT(caught);
	UT_ASSERT_EQ(space_reads + reservations + new_wal + writes + syncs, 0);
	UT_ASSERT_EQ(copy_begins, 1);
	UT_ASSERT(!copy_live);
	UT_ASSERT_EQ(copy_aborts + copy_ends, 0);
}

UT_TEST(test_raw_copy_owner_covers_space_wal_data_and_final_sync)
{
	const ForkNumber forks[] = { MAIN_FORKNUM, FSM_FORKNUM, VISIBILITYMAP_FORKNUM };

	for (unsigned i = 0; i < lengthof(forks); i++) {
		reset(forks[i]);
		RelationCopyStorage(&source, &destination, test_fork, RELPERSISTENCE_PERMANENT);
		UT_ASSERT_EQ(copy_begins, 1);
		UT_ASSERT_EQ(copy_ends, 1);
		UT_ASSERT_EQ(copy_aborts, 0);
		UT_ASSERT_EQ(copy_observed, i == 0 ? 127 : 125);
		UT_ASSERT(!copy_live && !copy_failed_retained);
		UT_ASSERT_EQ(writes, 1);
		UT_ASSERT_EQ(syncs, 1);
	}
	reset(MAIN_FORKNUM);
	source_blocks = 0;
	RelationCopyStorage(&source, &destination, test_fork, RELPERSISTENCE_PERMANENT);
	UT_ASSERT_EQ(copy_observed, 65); /* SPACE read and final sync even when empty. */
	UT_ASSERT_EQ(copy_ends, 1);
	UT_ASSERT_EQ(reservations + writes + new_wal, 0);
	UT_ASSERT_EQ(syncs, 1);
}

UT_TEST(test_raw_copy_rechecks_before_wal_data_and_sync)
{
	const unsigned sites[] = { COPY_READ, COPY_FLUSH, COPY_WRITE };
	const unsigned wal_expected[] = { 0, 1, 1 };
	const unsigned writes_expected[] = { 0, 0, 1 };

	for (unsigned i = 0; i < lengthof(sites); i++) {
		volatile bool caught = false;

		reset(MAIN_FORKNUM);
		copy_lose_site = sites[i];
		expecting_error = true;
		if (setjmp(error_jump) == 0)
			RelationCopyStorage(&source, &destination, test_fork, RELPERSISTENCE_PERMANENT);
		else
			caught = true;
		expecting_error = false;
		UT_ASSERT(caught);
		UT_ASSERT_EQ(new_wal, wal_expected[i]);
		UT_ASSERT_EQ(writes, writes_expected[i]);
		UT_ASSERT_EQ(syncs, 0);
		UT_ASSERT_EQ(copy_ends, 0);
		UT_ASSERT_EQ(copy_aborts, 1);
		UT_ASSERT(copy_live && copy_aborting);
	}
}

UT_TEST(test_raw_copy_lost_owner_after_sync_cannot_report_completion)
{
	volatile bool caught = false;

	reset(MAIN_FORKNUM);
	copy_lose_site = COPY_SYNC;
	expecting_error = true;
	if (setjmp(error_jump) == 0)
		RelationCopyStorage(&source, &destination, test_fork, RELPERSISTENCE_PERMANENT);
	else
		caught = true;
	expecting_error = false;
	UT_ASSERT(caught);
	UT_ASSERT_EQ(writes, 1);
	UT_ASSERT_EQ(syncs, 1);
	UT_ASSERT_EQ(copy_ends, 1);
	UT_ASSERT(copy_failed_retained);
}

UT_TEST(test_raw_copy_error_and_cancel_transfer_the_same_owner)
{
	for (unsigned site = COPY_SPACE; site <= COPY_SYNC + 1; site++) {
		volatile bool caught = false;

		reset(MAIN_FORKNUM);
		if (site <= COPY_SYNC)
			copy_fail_site = site;
		else
			InterruptPending = true;
		expecting_error = true;
		if (setjmp(error_jump) == 0)
			RelationCopyStorage(&source, &destination, test_fork, RELPERSISTENCE_PERMANENT);
		else
			caught = true;
		expecting_error = false;
		InterruptPending = false;
		UT_ASSERT(caught);
		UT_ASSERT_EQ(copy_aborts, 1);
		UT_ASSERT_EQ(copy_ends, 0);
		UT_ASSERT(copy_live && copy_aborting);
		UT_ASSERT(PG_exception_stack == NULL);
	}
}

UT_TEST(test_raw_copy_private_and_temp_keep_native_lifetime)
{
	for (unsigned variant = 0; variant < 3; variant++) {
		reset(MAIN_FORKNUM);
		private_storage = variant == 0;
		cluster_shared_config = variant != 1;
		temporary_copy = variant == 2;
		copy_begin_allowed = false;
		RelationCopyStorage(&source, &destination, test_fork,
							temporary_copy ? RELPERSISTENCE_TEMP : RELPERSISTENCE_PERMANENT);
		UT_ASSERT_EQ(copy_begins + copy_ends + copy_aborts, 0);
		UT_ASSERT_EQ(space_reads + reservations + new_wal, 0);
		UT_ASSERT_EQ(old_wal, temporary_copy ? 0 : 1);
		UT_ASSERT_EQ(syncs, temporary_copy ? 0 : 1);
		UT_ASSERT_EQ(writes, 1);
	}
}

UT_TEST(test_raw_copy_initial_owner_loss_precedes_space_read)
{
	volatile bool caught = false;

	reset(MAIN_FORKNUM);
	copy_permission = false;
	expecting_error = true;
	if (setjmp(error_jump) == 0)
		RelationCopyStorage(&source, &destination, test_fork, RELPERSISTENCE_PERMANENT);
	else
		caught = true;
	expecting_error = false;
	UT_ASSERT(caught);
	UT_ASSERT_EQ(copy_begins, 1);
	UT_ASSERT_EQ(copy_aborts, 1);
	UT_ASSERT_EQ(copy_ends, 0);
	UT_ASSERT_EQ(space_reads + reservations + new_wal + writes + syncs, 0);
}

UT_TEST(test_buffer_copy_owner_refusal_precedes_destination_effects)
{
	volatile bool caught = false;

	reset(MAIN_FORKNUM);
	buffered = true;
	copy_begin_allowed = false;
	expecting_error = true;
	if (setjmp(error_jump) == 0)
		RelationCopyStorageUsingBuffer(source.smgr_rlocator.locator,
									   destination.smgr_rlocator.locator, test_fork, true);
	else
		caught = true;
	expecting_error = false;
	UT_ASSERT(caught);
	UT_ASSERT_EQ(space_reads + reservations + new_wal + writes + copy_buffer_reads, 0);
	UT_ASSERT_EQ(copy_begins, 1);
	UT_ASSERT(!copy_live);
	UT_ASSERT_EQ(copy_aborts + copy_ends, 0);
}

UT_TEST(test_buffer_copy_owner_covers_original_forks_and_unpins_before_end)
{
	const ForkNumber forks[] = { MAIN_FORKNUM, FSM_FORKNUM, VISIBILITYMAP_FORKNUM };

	for (unsigned i = 0; i < lengthof(forks); i++) {
		reset(forks[i]);
		buffered = true;
		RelationCopyStorageUsingBuffer(source.smgr_rlocator.locator,
									   destination.smgr_rlocator.locator, test_fork, true);
		UT_ASSERT_EQ(copy_begins, 1);
		UT_ASSERT_EQ(copy_ends, 1);
		UT_ASSERT_EQ(copy_aborts, 0);
		UT_ASSERT_EQ(copy_targets, 1);
		UT_ASSERT_EQ(copy_target_checks, 1);
		UT_ASSERT_EQ(copy_buffer_reads, 2);
		UT_ASSERT_EQ(copy_observed,
					 (1U << (COPY_SPACE - 1)) | (1U << (COPY_WAL - 1)) | (1U << (COPY_WRITE - 1))
						 | (1U << (COPY_SOURCE_BUFFER - 1)) | (1U << (COPY_DEST_BUFFER - 1))
						 | (1U << (COPY_RELEASE - 1)) | (i == 0 ? 1U << (COPY_RESERVE - 1) : 0));
		UT_ASSERT(!copy_live && !copy_failed_retained);
		UT_ASSERT_EQ(new_wal, 1);
		UT_ASSERT_EQ(dirties, 1);
		UT_ASSERT_EQ(syncs + flushes + strategies, 0);
	}
}

UT_TEST(test_buffer_copy_owner_loss_before_reserve_extend_or_buffer_wal)
{
	const unsigned sites[]
		= { COPY_SPACE, COPY_RESERVE, COPY_WRITE, COPY_SOURCE_BUFFER, COPY_DEST_BUFFER };

	for (unsigned i = 0; i < lengthof(sites); i++) {
		volatile bool caught = false;

		reset(MAIN_FORKNUM);
		buffered = true;
		copy_lose_site = sites[i];
		expecting_error = true;
		if (setjmp(error_jump) == 0)
			RelationCopyStorageUsingBuffer(source.smgr_rlocator.locator,
										   destination.smgr_rlocator.locator, test_fork, true);
		else
			caught = true;
		expecting_error = false;
		UT_ASSERT(caught);
		UT_ASSERT_EQ(new_wal + dirties + flushes + syncs, 0);
		UT_ASSERT_EQ(reservations, i == 0 ? 0 : 1);
		UT_ASSERT_EQ(writes, i < 2 ? 0 : 1);
		UT_ASSERT_EQ(copy_buffer_reads, i < 3 ? 0 : 2);
		UT_ASSERT_EQ(copy_ends, 0);
		UT_ASSERT_EQ(copy_aborts, 1);
		UT_ASSERT(copy_live && copy_aborting);
		UT_ASSERT(PG_exception_stack == NULL);
	}
}

UT_TEST(test_buffer_copy_error_and_cancel_retain_original_owner)
{
	const unsigned sites[] = { COPY_SPACE,		 COPY_RESERVE, COPY_WRITE, COPY_SOURCE_BUFFER,
							   COPY_DEST_BUFFER, COPY_WAL,	   0 };

	for (unsigned i = 0; i < lengthof(sites); i++) {
		volatile bool caught = false;

		reset(MAIN_FORKNUM);
		buffered = true;
		copy_fail_site = sites[i];
		InterruptPending = sites[i] == 0;
		expecting_error = true;
		if (setjmp(error_jump) == 0)
			RelationCopyStorageUsingBuffer(source.smgr_rlocator.locator,
										   destination.smgr_rlocator.locator, test_fork, true);
		else
			caught = true;
		expecting_error = InterruptPending = false;
		UT_ASSERT(caught);
		UT_ASSERT_EQ(copy_aborts, 1);
		UT_ASSERT_EQ(copy_ends, 0);
		UT_ASSERT(copy_live && copy_aborting);
		UT_ASSERT(PG_exception_stack == NULL);
		/* Original ResourceOwner cleanup is outside this component fixture. */
		CritSectionCount = 0;
	}
}

UT_TEST(test_buffer_copy_lost_owner_after_unlock_is_not_success)
{
	volatile bool caught = false;

	reset(MAIN_FORKNUM);
	buffered = true;
	copy_lose_site = COPY_RELEASE;
	expecting_error = true;
	if (setjmp(error_jump) == 0)
		RelationCopyStorageUsingBuffer(source.smgr_rlocator.locator,
									   destination.smgr_rlocator.locator, test_fork, true);
	else
		caught = true;
	expecting_error = false;
	UT_ASSERT(caught);
	UT_ASSERT_EQ(new_wal, 1);
	UT_ASSERT_EQ(dirties, 1);
	UT_ASSERT_EQ(copy_ends, 1);
	UT_ASSERT(copy_failed_retained);
	UT_ASSERT_EQ(strategies + syncs + flushes, 0);
}

UT_TEST(test_buffer_copy_empty_private_and_native_keep_original_lifetime)
{
	for (unsigned variant = 0; variant < 3; variant++) {
		reset(MAIN_FORKNUM);
		buffered = true;
		source_blocks = variant == 0 ? 0 : 1;
		private_storage = variant == 1;
		cluster_shared_config = variant != 2;
		copy_begin_allowed = false;
		RelationCopyStorageUsingBuffer(source.smgr_rlocator.locator,
									   destination.smgr_rlocator.locator, test_fork, true);
		UT_ASSERT_EQ(copy_begins + copy_ends + copy_aborts + copy_targets, 0);
		UT_ASSERT_EQ(space_reads + reservations + new_wal, 0);
		UT_ASSERT_EQ(writes, variant == 0 ? 0 : 1);
		UT_ASSERT_EQ(dirties, variant == 0 ? 0 : 1);
		UT_ASSERT_EQ(old_wal, variant == 0 ? 0 : 1);
	}
}

UT_TEST(test_buffer_copy_initial_owner_loss_precedes_identity_read)
{
	volatile bool caught = false;

	reset(MAIN_FORKNUM);
	buffered = true;
	copy_permission = false;
	expecting_error = true;
	if (setjmp(error_jump) == 0)
		RelationCopyStorageUsingBuffer(source.smgr_rlocator.locator,
									   destination.smgr_rlocator.locator, test_fork, true);
	else
		caught = true;
	expecting_error = false;
	UT_ASSERT(caught);
	UT_ASSERT_EQ(copy_begins, 1);
	UT_ASSERT_EQ(copy_aborts, 1);
	UT_ASSERT_EQ(copy_ends, 0);
	UT_ASSERT_EQ(space_reads + reservations + new_wal + writes + copy_buffer_reads, 0);
}

UT_TEST(test_auxiliary_fork_create_holds_owner_through_native_wal)
{
	reset(MAIN_FORKNUM);
	extra_forks = (1U << FSM_FORKNUM) | (1U << VISIBILITYMAP_FORKNUM) | (1U << SPACE_FORKNUM);
	CreateAndCopyRelationData(source.smgr_rlocator.locator, identity.key.locator, true);
	UT_ASSERT_EQ(main_creates, 1);
	UT_ASSERT_EQ(main_copies, 1);
	UT_ASSERT_EQ(copy_begins, 2);
	UT_ASSERT_EQ(copy_ends, 2);
	UT_ASSERT_EQ(copy_aborts, 0);
	UT_ASSERT_EQ(fork_opens, 2);
	UT_ASSERT_EQ(fork_creates, 2);
	UT_ASSERT_EQ(fork_wal, 2);
	UT_ASSERT_EQ(fork_copies, 2);
	UT_ASSERT_EQ(fork_owner_observed, 7);
	UT_ASSERT_EQ(fork_closes, 2);
	UT_ASSERT(!copy_live && !copy_failed_retained && !buffer_copy_scope);
}

UT_TEST(test_auxiliary_fork_refusal_precedes_its_open_create_and_wal)
{
	volatile bool caught = false;

	reset(MAIN_FORKNUM);
	extra_forks = 1U << FSM_FORKNUM;
	copy_begin_allowed = false;
	expecting_error = true;
	if (setjmp(error_jump) == 0)
		CreateAndCopyRelationData(source.smgr_rlocator.locator, identity.key.locator, true);
	else
		caught = true;
	expecting_error = false;
	UT_ASSERT(caught);
	/* The completed MAIN calls are separate original producers. */
	UT_ASSERT_EQ(main_creates + main_copies, 2);
	UT_ASSERT_EQ(copy_begins, 1);
	UT_ASSERT_EQ(fork_opens + fork_creates + fork_wal + fork_copies, 0);
	UT_ASSERT_EQ(copy_ends + copy_aborts, 0);
	UT_ASSERT(!copy_live);
}

UT_TEST(test_auxiliary_fork_owner_loss_after_create_prevents_wal_and_copy)
{
	volatile bool caught = false;

	reset(MAIN_FORKNUM);
	extra_forks = 1U << FSM_FORKNUM;
	fork_lose_site = FORK_CREATE;
	expecting_error = true;
	if (setjmp(error_jump) == 0)
		CreateAndCopyRelationData(source.smgr_rlocator.locator, identity.key.locator, true);
	else
		caught = true;
	expecting_error = false;
	UT_ASSERT(caught);
	UT_ASSERT_EQ(fork_creates, 1);
	UT_ASSERT_EQ(fork_wal + fork_copies + copy_ends, 0);
	UT_ASSERT_EQ(copy_aborts, 1);
	UT_ASSERT(copy_live && copy_aborting && copy_armed);
}

UT_TEST(test_auxiliary_fork_error_retains_original_creation_responsibility)
{
	for (unsigned site = FORK_OPEN; site <= FORK_WAL; site++) {
		volatile bool caught = false;

		reset(MAIN_FORKNUM);
		extra_forks = 1U << FSM_FORKNUM;
		fork_fail_site = site;
		expecting_error = true;
		if (setjmp(error_jump) == 0)
			CreateAndCopyRelationData(source.smgr_rlocator.locator, identity.key.locator, true);
		else
			caught = true;
		expecting_error = false;
		UT_ASSERT(caught);
		UT_ASSERT_EQ(copy_aborts, 1);
		UT_ASSERT_EQ(copy_ends + fork_copies + fork_closes, 0);
		UT_ASSERT_EQ(fork_creates, site >= FORK_CREATE ? 1 : 0);
		UT_ASSERT_EQ(fork_wal, site >= FORK_WAL ? 1 : 0);
		UT_ASSERT(copy_live && copy_aborting && copy_armed);
		UT_ASSERT(PG_exception_stack == NULL);
	}
}

UT_TEST(test_auxiliary_fork_end_failure_does_not_start_data_copy)
{
	volatile bool caught = false;

	reset(MAIN_FORKNUM);
	extra_forks = 1U << FSM_FORKNUM;
	fork_lose_site = FORK_WAL;
	expecting_error = true;
	if (setjmp(error_jump) == 0)
		CreateAndCopyRelationData(source.smgr_rlocator.locator, identity.key.locator, true);
	else
		caught = true;
	expecting_error = false;
	UT_ASSERT(caught);
	UT_ASSERT_EQ(fork_creates, 1);
	UT_ASSERT_EQ(fork_wal, 1);
	UT_ASSERT_EQ(copy_ends, 1);
	UT_ASSERT_EQ(fork_copies + copy_aborts, 0);
	UT_ASSERT(copy_failed_retained);
}

UT_TEST(test_auxiliary_fork_each_new_fork_requires_its_own_admission)
{
	volatile bool caught = false;

	reset(MAIN_FORKNUM);
	extra_forks = (1U << FSM_FORKNUM) | (1U << VISIBILITYMAP_FORKNUM);
	fork_refuse_next = true;
	expecting_error = true;
	if (setjmp(error_jump) == 0)
		CreateAndCopyRelationData(source.smgr_rlocator.locator, identity.key.locator, true);
	else
		caught = true;
	expecting_error = false;
	UT_ASSERT(caught);
	UT_ASSERT_EQ(copy_begins, 2);
	UT_ASSERT_EQ(copy_ends, 1);
	UT_ASSERT_EQ(fork_creates, 1);
	UT_ASSERT_EQ(fork_wal, 1);
	UT_ASSERT_EQ(fork_copies, 1);
	UT_ASSERT_EQ(copy_aborts, 0);
	UT_ASSERT(!copy_live);
}

UT_TEST(test_auxiliary_fork_empty_native_private_and_recovery_lifetimes_unchanged)
{
	for (unsigned variant = 0; variant < 5; variant++) {
		reset(MAIN_FORKNUM);
		extra_forks = 1U << SPACE_FORKNUM; /* Always belongs to the original typed constructor. */
		if (variant != 0)
			extra_forks |= 1U << FSM_FORKNUM;
		cluster_shared_config = variant != 1;
		private_storage = variant == 2;
		cluster_enabled = variant != 3;
		fixture_recovering = variant == 4;
		copy_begin_allowed = false;
		CreateAndCopyRelationData(source.smgr_rlocator.locator, identity.key.locator, true);
		UT_ASSERT_EQ(copy_begins + copy_ends + copy_aborts, 0);
		UT_ASSERT_EQ(fork_creates, variant == 0 ? 0 : 1);
		UT_ASSERT_EQ(fork_wal, variant == 0 ? 0 : 1);
		UT_ASSERT_EQ(fork_copies, variant == 0 ? 0 : 1);
		UT_ASSERT_EQ(fork_closes, 2);
		UT_ASSERT_EQ(fork_owner_observed, 0);
	}
}

UT_TEST(test_auxiliary_unlogged_forks_keep_init_only_wal_and_finite_owner)
{
	reset(MAIN_FORKNUM);
	extra_forks = (1U << FSM_FORKNUM) | (1U << VISIBILITYMAP_FORKNUM) | (1U << INIT_FORKNUM);
	fork_permanent = false;
	CreateAndCopyRelationData(source.smgr_rlocator.locator, identity.key.locator, false);
	UT_ASSERT_EQ(copy_begins, 3);
	UT_ASSERT_EQ(copy_ends, 3);
	UT_ASSERT_EQ(copy_aborts, 0);
	UT_ASSERT_EQ(fork_creates, 3);
	UT_ASSERT_EQ(fork_copies, 3);
	UT_ASSERT_EQ(fork_wal, 1);
	UT_ASSERT_EQ(fork_owner_observed, 7);
}

UT_TEST(test_raw_fork_both_callers_hold_owner_through_create_and_wal)
{
	for (unsigned caller = 0; caller < 2; caller++) {
		reset(MAIN_FORKNUM);
		extra_forks = (1U << FSM_FORKNUM) | (1U << VISIBILITYMAP_FORKNUM) | (1U << SPACE_FORKNUM);
		run_raw_caller(caller);
		UT_ASSERT_EQ(copy_begins, 2);
		UT_ASSERT_EQ(copy_ends, 2);
		UT_ASSERT_EQ(copy_aborts, 0);
		UT_ASSERT_EQ(fork_creates, 2);
		UT_ASSERT_EQ(fork_wal, 2);
		UT_ASSERT_EQ(fork_copies, 2);
		UT_ASSERT_EQ(fork_owner_observed, (1U << (FORK_CREATE - 1)) | (1U << (FORK_WAL - 1)));
		UT_ASSERT_EQ(main_creates + main_copies, 2);
		UT_ASSERT_EQ(raw_opens + raw_flushes + raw_drops + raw_closes, 4);
		UT_ASSERT(!copy_live && !copy_failed_retained && !buffer_copy_scope);
	}
}

UT_TEST(test_raw_fork_refusal_precedes_auxiliary_effects)
{
	for (unsigned caller = 0; caller < 2; caller++) {
		volatile bool caught = false;
		reset(MAIN_FORKNUM);
		extra_forks = 1U << FSM_FORKNUM;
		copy_begin_allowed = false;
		expecting_error = true;
		if (setjmp(error_jump) == 0)
			run_raw_caller(caller);
		else
			caught = true;
		expecting_error = false;
		UT_ASSERT(caught);
		UT_ASSERT_EQ(copy_begins, 1);
		UT_ASSERT_EQ(fork_creates + fork_wal + fork_copies + raw_drops + raw_closes, 0);
		UT_ASSERT_EQ(copy_ends + copy_aborts, 0);
		UT_ASSERT_EQ(main_creates + main_copies + raw_flushes + raw_opens, 4);
		UT_ASSERT(!copy_live);
	}
}

UT_TEST(test_raw_fork_initial_owner_loss_precedes_create)
{
	for (unsigned caller = 0; caller < 2; caller++) {
		volatile bool caught = false;
		reset(MAIN_FORKNUM);
		extra_forks = 1U << FSM_FORKNUM;
		copy_permission = false;
		expecting_error = true;
		if (setjmp(error_jump) == 0)
			run_raw_caller(caller);
		else
			caught = true;
		expecting_error = false;
		UT_ASSERT(caught);
		UT_ASSERT_EQ(copy_begins, 1);
		UT_ASSERT_EQ(copy_aborts, 1);
		UT_ASSERT_EQ(fork_creates + fork_wal + fork_copies + copy_ends + raw_drops, 0);
		UT_ASSERT(copy_live && copy_aborting && !copy_armed);
	}
}

UT_TEST(test_raw_fork_loss_after_create_prevents_wal_and_copy)
{
	for (unsigned caller = 0; caller < 2; caller++) {
		volatile bool caught = false;
		reset(MAIN_FORKNUM);
		extra_forks = 1U << FSM_FORKNUM;
		fork_lose_site = FORK_CREATE;
		expecting_error = true;
		if (setjmp(error_jump) == 0)
			run_raw_caller(caller);
		else
			caught = true;
		expecting_error = false;
		UT_ASSERT(caught);
		UT_ASSERT_EQ(fork_creates, 1);
		UT_ASSERT_EQ(copy_aborts, 1);
		UT_ASSERT_EQ(fork_wal + fork_copies + copy_ends + raw_drops, 0);
		UT_ASSERT(copy_live && copy_aborting && copy_armed);
	}
}

UT_TEST(test_raw_fork_errors_keep_original_owner)
{
	for (unsigned caller = 0; caller < 2; caller++)
		for (unsigned site = FORK_CREATE; site <= FORK_WAL; site++) {
			volatile bool caught = false;
			reset(MAIN_FORKNUM);
			extra_forks = 1U << FSM_FORKNUM;
			fork_fail_site = site;
			expecting_error = true;
			if (setjmp(error_jump) == 0)
				run_raw_caller(caller);
			else
				caught = true;
			expecting_error = false;
			UT_ASSERT(caught);
			UT_ASSERT_EQ(copy_aborts, 1);
			UT_ASSERT_EQ(copy_ends + fork_copies + raw_drops + raw_closes, 0);
			UT_ASSERT_EQ(fork_creates, 1);
			UT_ASSERT_EQ(fork_wal, site == FORK_WAL ? 1 : 0);
			UT_ASSERT(copy_live && copy_aborting && copy_armed);
			UT_ASSERT(PG_exception_stack == NULL);
		}
}

UT_TEST(test_raw_fork_end_failure_prevents_copy_and_old_drop)
{
	for (unsigned caller = 0; caller < 2; caller++) {
		volatile bool caught = false;
		reset(MAIN_FORKNUM);
		extra_forks = 1U << FSM_FORKNUM;
		fork_lose_site = FORK_WAL;
		expecting_error = true;
		if (setjmp(error_jump) == 0)
			run_raw_caller(caller);
		else
			caught = true;
		expecting_error = false;
		UT_ASSERT(caught);
		UT_ASSERT_EQ(fork_creates + fork_wal, 2);
		UT_ASSERT_EQ(copy_ends, 1);
		UT_ASSERT_EQ(fork_copies + copy_aborts + raw_drops + raw_closes, 0);
		UT_ASSERT(copy_failed_retained);
	}
}

UT_TEST(test_raw_fork_new_fork_requires_independent_admission)
{
	for (unsigned caller = 0; caller < 2; caller++) {
		volatile bool caught = false;
		reset(MAIN_FORKNUM);
		extra_forks = (1U << FSM_FORKNUM) | (1U << VISIBILITYMAP_FORKNUM);
		fork_refuse_next = true;
		expecting_error = true;
		if (setjmp(error_jump) == 0)
			run_raw_caller(caller);
		else
			caught = true;
		expecting_error = false;
		UT_ASSERT(caught);
		UT_ASSERT_EQ(copy_begins, 2);
		UT_ASSERT_EQ(copy_ends, 1);
		UT_ASSERT_EQ(fork_creates + fork_wal + fork_copies, 3);
		UT_ASSERT_EQ(copy_aborts + raw_drops + raw_closes, 0);
		UT_ASSERT(!copy_live);
	}
}

UT_TEST(test_raw_fork_native_private_temp_recovery_and_empty_unchanged)
{
	for (unsigned caller = 0; caller < 2; caller++)
		for (unsigned variant = 0; variant < 6; variant++) {
			reset(MAIN_FORKNUM);
			extra_forks = 1U << SPACE_FORKNUM;
			if (variant != 0)
				extra_forks |= 1U << FSM_FORKNUM;
			cluster_shared_config = variant != 1;
			private_storage = variant == 2;
			cluster_enabled = variant != 3;
			fixture_recovering = variant == 4;
			if (variant == 5) {
				raw_relation_class.relpersistence = RELPERSISTENCE_TEMP;
				raw_relation.rd_backend = 9;
				fork_permanent = false;
			}
			copy_begin_allowed = false;
			run_raw_caller(caller);
			UT_ASSERT_EQ(copy_begins + copy_ends + copy_aborts, 0);
			UT_ASSERT_EQ(fork_creates, variant == 0 ? 0 : 1);
			UT_ASSERT_EQ(fork_wal, variant == 0 || variant == 5 ? 0 : 1);
			UT_ASSERT_EQ(fork_copies, variant == 0 ? 0 : 1);
			UT_ASSERT_EQ(raw_drops + raw_closes, 2);
			UT_ASSERT_EQ(fork_owner_observed, 0);
		}
}

UT_TEST(test_raw_fork_unlogged_init_only_wal_unchanged)
{
	for (unsigned caller = 0; caller < 2; caller++) {
		reset(MAIN_FORKNUM);
		extra_forks = (1U << FSM_FORKNUM) | (1U << VISIBILITYMAP_FORKNUM) | (1U << INIT_FORKNUM);
		fork_permanent = false;
		raw_relation_class.relpersistence = RELPERSISTENCE_UNLOGGED;
		run_raw_caller(caller);
		UT_ASSERT_EQ(copy_begins, 3);
		UT_ASSERT_EQ(copy_ends, 3);
		UT_ASSERT_EQ(fork_creates, 3);
		UT_ASSERT_EQ(fork_copies, 3);
		UT_ASSERT_EQ(fork_wal, 1);
		UT_ASSERT_EQ(copy_aborts, 0);
	}
}

UT_TEST(test_raw_fork_r1_refusal_remains_before_any_open_or_effect)
{
	for (unsigned caller = 0; caller < 2; caller++) {
		volatile bool caught = false;
		reset(MAIN_FORKNUM);
		extra_forks = 1U << FSM_FORKNUM;
		refuse_copy_storage = true;
		if (setjmp(error_jump) == 0)
			run_raw_caller(caller);
		else
			caught = true;
		UT_ASSERT(caught);
		UT_ASSERT_EQ(copy_storage_checks, 1);
		UT_ASSERT_EQ(raw_opens + raw_flushes + main_creates + main_copies + copy_begins
						 + fork_creates + fork_wal + fork_copies + raw_drops + raw_closes,
					 0);
	}
}

int
main(void)
{
	UT_PLAN(55);
	UT_RUN(test_raw_fork_both_callers_hold_owner_through_create_and_wal);
	UT_RUN(test_raw_fork_refusal_precedes_auxiliary_effects);
	UT_RUN(test_raw_fork_initial_owner_loss_precedes_create);
	UT_RUN(test_raw_fork_loss_after_create_prevents_wal_and_copy);
	UT_RUN(test_raw_fork_errors_keep_original_owner);
	UT_RUN(test_raw_fork_end_failure_prevents_copy_and_old_drop);
	UT_RUN(test_raw_fork_new_fork_requires_independent_admission);
	UT_RUN(test_raw_fork_native_private_temp_recovery_and_empty_unchanged);
	UT_RUN(test_raw_fork_unlogged_init_only_wal_unchanged);
	UT_RUN(test_raw_fork_r1_refusal_remains_before_any_open_or_effect);
	UT_RUN(test_auxiliary_fork_create_holds_owner_through_native_wal);
	UT_RUN(test_auxiliary_fork_refusal_precedes_its_open_create_and_wal);
	UT_RUN(test_auxiliary_fork_owner_loss_after_create_prevents_wal_and_copy);
	UT_RUN(test_auxiliary_fork_error_retains_original_creation_responsibility);
	UT_RUN(test_auxiliary_fork_end_failure_does_not_start_data_copy);
	UT_RUN(test_auxiliary_fork_each_new_fork_requires_its_own_admission);
	UT_RUN(test_auxiliary_fork_empty_native_private_and_recovery_lifetimes_unchanged);
	UT_RUN(test_auxiliary_unlogged_forks_keep_init_only_wal_and_finite_owner);
	UT_RUN(test_buffer_copy_owner_refusal_precedes_destination_effects);
	UT_RUN(test_buffer_copy_owner_covers_original_forks_and_unpins_before_end);
	UT_RUN(test_buffer_copy_owner_loss_before_reserve_extend_or_buffer_wal);
	UT_RUN(test_buffer_copy_error_and_cancel_retain_original_owner);
	UT_RUN(test_buffer_copy_lost_owner_after_unlock_is_not_success);
	UT_RUN(test_buffer_copy_empty_private_and_native_keep_original_lifetime);
	UT_RUN(test_buffer_copy_initial_owner_loss_precedes_identity_read);
	UT_RUN(test_raw_copy_fence_refusal_precedes_target_effects);
	UT_RUN(test_raw_copy_owner_covers_space_wal_data_and_final_sync);
	UT_RUN(test_raw_copy_rechecks_before_wal_data_and_sync);
	UT_RUN(test_raw_copy_lost_owner_after_sync_cannot_report_completion);
	UT_RUN(test_raw_copy_error_and_cancel_transfer_the_same_owner);
	UT_RUN(test_raw_copy_private_and_temp_keep_native_lifetime);
	UT_RUN(test_raw_copy_initial_owner_loss_precedes_space_read);
	UT_RUN(test_resident_init_and_copy_bind_the_first_record);
	UT_RUN(test_resident_capture_loss_keeps_unknown_first);
	UT_RUN(test_resident_broken_owner_panics_before_unlock);
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
	UT_RUN(test_buffer_copy_checks_storage_eligibility_before_io);
	UT_RUN(test_buffer_copy_refusal_has_no_storage_effect);
	UT_RUN(test_nonshared_buffer_copy_keeps_native_path);
	UT_RUN(test_buffer_copy_vm_and_fsm_classes);
	UT_RUN(test_buffer_refusal_never_changes_target_or_emits_wal);
	UT_RUN(test_nonempty_destination_refused_before_bulk_extension);
	UT_RUN(test_buffer_copy_reopens_after_identity_read);
	UT_RUN(test_copy_reserves_before_data_and_reopens_after_current_read);
	UT_RUN(test_copy_refuses_before_any_zero_fill_or_wal_without_reservation);
	UT_DONE();
	return ut_failed_count ? 1 : 0;
}
