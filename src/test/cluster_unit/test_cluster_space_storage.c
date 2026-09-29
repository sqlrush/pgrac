/*-------------------------------------------------------------------------
 *
 * test_cluster_space_storage.c
 *    Actual SPACE create/replay adapters with native I/O boundary fixtures.
 *
 * Portions Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *    src/test/cluster_unit/test_cluster_space_storage.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/xlog.h"
#include "access/xloginsert.h"
#include "access/xact.h"
#include "access/xlogutils.h"
#include "access/visibilitymap.h"
#include "catalog/pg_class.h"
#include "catalog/pg_tablespace_d.h"
#include "catalog/storage.h"
#include "catalog/storage_xlog.h"
#include "cluster/cluster_guc.h"
#include "cluster/cluster_ko.h"
#include "cluster/cluster_page_producer.h"
#include "cluster/cluster_space_storage.h"
#include "cluster/cluster_wal_thread.h"
#include "cluster/storage/cluster_smgr.h"
#include "miscadmin.h"
#include "storage/bufmgr.h"
#include "storage/freespace.h"
#include "storage/smgr.h"
#include "utils/hsearch.h"
#include "utils/memutils.h"
#include "utils/rel.h"
#include "unit_test.h"

UT_DEFINE_GLOBALS();

bool cluster_shared_config = true, cluster_enabled = true;
int cluster_node_id = 0;
int NBuffers = 1, NLocBuffer;
char *BufferBlocks;
Block *LocalBufferBlockPointers;
volatile uint32 CritSectionCount;
bool cluster_recmerge_window_active, cluster_recmerge_apply_foreign;
uint64 cluster_recmerge_window_scn, cluster_recmerge_window_own_lsn;
int wal_level = WAL_LEVEL_REPLICA;
BackendId MyBackendId = 1, ParallelLeaderBackendId = InvalidBackendId;
MemoryContext TopMemoryContext = (MemoryContext)1;
MemoryContext TopTransactionContext = (MemoryContext)2;

static PGAlignedBlock page;
static SMgrRelationData storage;
static ClusterWalDurablePrefixRef ref;
static bool have_ref, shared, exists, recovering, pinned, locked;
static unsigned io_calls, create_calls, wal_calls, dirty_calls, release_calls;
static BlockNumber blocks;
static uint8 wal_bytes[CLUSTER_SPACE_WAL_BYTES];
static uint8 wal_info;
static uint32 registered_len;
static unsigned main_create_calls, unlink_calls;
static bool native_owner, delete_registered;
static RelFileLocator locator = { DEFAULTTABLESPACE_OID, 5, 16384 };
static BlockNumber main_blocks;
static unsigned truncate_calls, flush_calls, fsm_vacuums;
static bool auxiliary_forks;
static Relation fake_relation;
static unsigned fake_allocations, fake_frees;
static int nest_level;
static unsigned ko_calls;

void
ExceptionalCondition(const char *condition, const char *file, int line)
{
	printf("# Unexpected assertion: %s at %s:%d\n", condition, file, line);
	abort();
}

bool
pg_strong_random(void *bytes, size_t len)
{
	memset(bytes, 0x45, len);
	return true;
}

uint64
rf_page_mutation_token_next(void)
{
	return 17;
}

bool
cluster_wal_thread_current_v2_ref(ClusterWalDurablePrefixRef *out)
{
	*out = ref;
	return have_ref;
}

bool
cluster_wal_thread_restart_v2_ref(ClusterWalDurablePrefixRef *out)
{
	return cluster_wal_thread_current_v2_ref(out);
}

bool
RecoveryInProgress(void)
{
	return recovering;
}

int
cluster_smgr_which_for(RelFileLocator tag, BackendId backend)
{
	if (!RelFileLocatorEquals(tag, locator) || backend != InvalidBackendId)
		abort();
	return shared ? 1 : 0;
}

SMgrRelation
smgropen(RelFileLocator tag, BackendId backend)
{
	if (!RelFileLocatorEquals(tag, locator) || backend != InvalidBackendId)
		abort();
	io_calls++;
	return &storage;
}

bool
smgrexists(SMgrRelation rel, ForkNumber forknum)
{
	if (rel != &storage)
		abort();
	if (recovering && (forknum == FSM_FORKNUM || forknum == VISIBILITYMAP_FORKNUM))
		return auxiliary_forks;
	if (forknum != SPACE_FORKNUM)
		abort();
	return exists;
}

void
smgrcreate(SMgrRelation rel, ForkNumber forknum, bool is_redo)
{
	if (rel != &storage || is_redo != recovering)
		abort();
	if (forknum == MAIN_FORKNUM && (native_owner || recovering)) {
		main_create_calls++;
		return;
	}
	if (forknum != SPACE_FORKNUM || (native_owner && !delete_registered))
		abort();
	create_calls++;
	exists = true;
}

BlockNumber
smgrnblocks(SMgrRelation rel, ForkNumber forknum)
{
	if (rel != &storage)
		abort();
	if (recovering && forknum == MAIN_FORKNUM)
		return main_blocks;
	if (recovering && auxiliary_forks && forknum == FSM_FORKNUM)
		return 8;
	if (recovering && auxiliary_forks && forknum == VISIBILITYMAP_FORKNUM)
		return 3;
	if (forknum != SPACE_FORKNUM)
		abort();
	return blocks;
}

void
XLogFlush(XLogRecPtr lsn)
{
	if (!recovering || lsn != UINT64_C(0x20000200) || !locked)
		abort();
	flush_calls++;
}

void
XLogTruncateRelation(RelFileLocator tag, ForkNumber forknum, BlockNumber count)
{
	if (!RelFileLocatorEquals(tag, locator) || forknum != MAIN_FORKNUM || count != 4)
		abort();
}

void
smgrclearowner(SMgrRelation *owner, SMgrRelation rel)
{
	if (rel != &storage || owner != &fake_relation->rd_smgr)
		abort();
	*owner = NULL;
	rel->smgr_owner = NULL;
}

BlockNumber
FreeSpaceMapPrepareTruncateRel(Relation rel, BlockNumber count)
{
	if (rel != fake_relation || count != 4 || !auxiliary_forks
		|| !RelFileLocatorEquals(rel->rd_locator, locator)
		|| rel->rd_rel->relpersistence != RELPERSISTENCE_PERMANENT)
		abort();
	return 1;
}
BlockNumber
visibilitymap_prepare_truncate(Relation rel, BlockNumber count)
{
	if (rel != fake_relation || count != 4 || !auxiliary_forks)
		abort();
	return 1;
}
void
FreeSpaceMapVacuumRange(Relation rel, BlockNumber start, BlockNumber end)
{
	if (rel != fake_relation || start != 4 || end != InvalidBlockNumber || !auxiliary_forks
		|| truncate_calls == 0)
		abort();
	fsm_vacuums++;
}

void
smgrtruncate2(SMgrRelation rel, ForkNumber *forks, int nforks, BlockNumber *oldblocks,
			  BlockNumber *newblocks)
{
	if (rel != &storage || nforks != (auxiliary_forks ? 3 : 1) || forks[0] != MAIN_FORKNUM
		|| !locked || !pinned || CritSectionCount == 0 || flush_calls != truncate_calls + 1
		|| oldblocks[0] != main_blocks || newblocks[0] != 4)
		abort();
	if (auxiliary_forks
		&& (forks[1] != FSM_FORKNUM || forks[2] != VISIBILITYMAP_FORKNUM || oldblocks[1] != 8
			|| oldblocks[2] != 3 || newblocks[1] != 1 || newblocks[2] != 1))
		abort();
	main_blocks = newblocks[0];
	truncate_calls++;
}

Buffer
ReadBufferWithoutRelcache(RelFileLocator tag, ForkNumber forknum, BlockNumber block,
						  ReadBufferMode mode, BufferAccessStrategy strategy, bool permanent)
{
	if (!RelFileLocatorEquals(tag, locator) || forknum != SPACE_FORKNUM || strategy != NULL
		|| !permanent || pinned)
		abort();
	if (block == P_NEW) {
		if (blocks != 0 || mode != RBM_ZERO_AND_LOCK)
			abort();
		blocks = 1;
		locked = true;
	} else if (block != 0 || blocks != 1 || mode != RBM_NORMAL)
		abort();
	pinned = true;
	return 1;
}

BlockNumber
BufferGetBlockNumber(Buffer buffer)
{
	if (buffer != 1 || !pinned)
		abort();
	return 0;
}

void
LockBuffer(Buffer buffer, int mode)
{
	if (buffer != 1 || !pinned || locked
		|| (mode != BUFFER_LOCK_EXCLUSIVE && mode != BUFFER_LOCK_SHARE))
		abort();
	locked = true;
}

void
UnlockReleaseBuffer(Buffer buffer)
{
	if (buffer != 1 || !pinned || !locked || CritSectionCount != 0)
		abort();
	pinned = locked = false;
	release_calls++;
}

void
MarkBufferDirty(Buffer buffer)
{
	if (buffer != 1 || !pinned || !locked || CritSectionCount != 1
		|| !cluster_space_identity_page_valid(page.data, BLCKSZ))
		abort();
	dirty_calls++;
}

void
XLogBeginInsert(void)
{
	if ((!locked && !native_owner) || CritSectionCount != 0)
		abort();
}

void
XLogRegisterData(char *bytes, uint32 len)
{
	registered_len = len;
	if (native_owner && len == sizeof(xl_smgr_create) && CritSectionCount == 0)
		return;
	if (len != sizeof(wal_bytes) || CritSectionCount != 1)
		abort();
	memcpy(wal_bytes, bytes, len);
}

XLogRecPtr
XLogInsert(RmgrId rmid, uint8 info)
{
	ClusterSpaceWalChange change;
	ClusterSpaceIdentity id;
	uint64 token;

	if (native_owner && info == (XLOG_SMGR_CREATE | XLR_SPECIAL_REL_UPDATE)
		&& registered_len == sizeof(xl_smgr_create) && rmid == RM_SMGR_ID)
		return UINT64_C(0x10000100);
	if (rmid != RM_SMGR_ID || CritSectionCount != 1
		|| !cluster_space_wal_decode(wal_bytes, sizeof(wal_bytes), &change)
		|| !cluster_space_identity_page_decode(page.data, BLCKSZ, SPACE_FORKNUM, 0,
											   &change.result.key, &id, &token)
		|| token != change.result_token)
		abort();
	wal_calls++;
	wal_info = info;
	return UINT64_C(0x10000200);
}

/* Native relation-create/pending-delete ownership is real; only memory,
 * external storage, and unused wal_level=minimal hash boundaries are faked. */
bool
IsInParallelMode(void)
{
	return false;
}
int
GetCurrentTransactionNestLevel(void)
{
	return nest_level;
}
void *
MemoryContextAlloc(MemoryContext context, Size size)
{
	if (context != TopMemoryContext || !native_owner)
		abort();
	delete_registered = true;
	return malloc(size);
}
void *
palloc(Size size)
{
	return malloc(size);
}
void *
palloc0(Size size)
{
	/* Real CreateFakeRelcacheEntry calls this boundary. Model a normal
	 * recovery memory context, which disallows allocation in a critical
	 * section, rather than masking the native allocation with a static rel. */
	UT_ASSERT_EQ(CritSectionCount, 0);
	if (fake_relation != NULL || size < sizeof(RelationData))
		abort();
	fake_relation = calloc(1, size);
	fake_allocations++;
	return fake_relation;
}
void *
repalloc(void *ptr, Size size)
{
	return realloc(ptr, size);
}
void
pfree(void *ptr)
{
	if (ptr == fake_relation) {
		fake_frees++;
		fake_relation = NULL;
	}
	free(ptr);
}
HTAB *
hash_create(const char *name, long count, const HASHCTL *info, int flags)
{
	(void)name;
	(void)count;
	(void)info;
	(void)flags;
	abort();
}
void *
hash_search(HTAB *hash, const void *key, HASHACTION action, bool *found)
{
	(void)hash;
	(void)key;
	(void)action;
	(void)found;
	abort();
}
void
smgrdounlinkall(SMgrRelation *rels, int count, bool redo)
{
	if (count != 1 || rels[0] != &storage || redo || !delete_registered)
		abort();
	unlink_calls++;
}
void
smgrclose(SMgrRelation rel)
{
	if (rel != &storage)
		abort();
	if (rel->smgr_owner != NULL)
		*rel->smgr_owner = NULL;
}
void
cluster_ko_flush_and_wait_ack(RelFileLocator tag, char persistence)
{
	if (!RelFileLocatorEquals(tag, locator) || persistence != RELPERSISTENCE_PERMANENT)
		abort();
	ko_calls++;
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

static void
reset(void)
{
	if (fake_relation != NULL)
		abort();
	fake_allocations = fake_frees = 0;
	memset(&page, 0, sizeof(page));
	memset(&storage, 0, sizeof(storage));
	memset(&ref, 0, sizeof(ref));
	ref.claim.identity.system_identifier = 1;
	ref.claim.identity.storage_uuid[15] = 3;
	ref.claim.database_incarnation = 2;
	ref.claim.identity.origin_node_id = 0;
	ref.claim.identity.origin_thread_id = 1;
	cluster_shared_config = cluster_enabled = have_ref = shared = true;
	exists = recovering = pinned = locked = false;
	cluster_recmerge_window_active = cluster_recmerge_apply_foreign = false;
	native_owner = delete_registered = false;
	main_create_calls = unlink_calls = registered_len = 0;
	main_blocks = 10;
	truncate_calls = flush_calls = fsm_vacuums = 0;
	auxiliary_forks = false;
	nest_level = 1;
	ko_calls = 0;
	CritSectionCount = blocks = io_calls = create_calls = wal_calls = dirty_calls = release_calls
		= 0;
	BufferBlocks = page.data;
	storage.smgr_rlocator.locator = locator;
}

UT_TEST(test_real_create_binds_selected_namespace_and_wal)
{
	ClusterSpaceWalChange change;

	reset();
	UT_ASSERT(cluster_space_relation_create(locator));
	UT_ASSERT_EQ(create_calls, 1);
	UT_ASSERT_EQ(wal_calls, 1);
	UT_ASSERT_EQ(dirty_calls, 1);
	UT_ASSERT_EQ(release_calls, 1);
	UT_ASSERT(!pinned && !locked);
	UT_ASSERT_EQ(wal_info, 0x30 | XLR_SPECIAL_REL_UPDATE);
	UT_ASSERT_EQ(PageGetLSN(page.data), UINT64_C(0x10000200));
	UT_ASSERT_EQ(((PageHeader)page.data)->pd_block_scn, 17);
	UT_ASSERT(cluster_space_wal_decode(wal_bytes, sizeof(wal_bytes), &change));
	UT_ASSERT_EQ(change.result.key.database_incarnation, 2);
	UT_ASSERT_EQ(change.result.key.storage_uuid[15], 3);
	UT_ASSERT_EQ(change.result.incarnation[15], 0x45);
}

UT_TEST(test_no_create_on_legacy_or_unproved_namespace)
{
	reset();
	cluster_shared_config = false;
	UT_ASSERT(cluster_space_relation_create(locator));
	UT_ASSERT_EQ(io_calls, 0);
	cluster_shared_config = true;
	shared = false;
	UT_ASSERT(cluster_space_relation_create(locator));
	UT_ASSERT_EQ(io_calls, 0);
	shared = true;
	have_ref = false;
	UT_ASSERT(!cluster_space_relation_create(locator));
	UT_ASSERT_EQ(io_calls, 0);
	have_ref = true;
	ref.claim.database_incarnation = 0;
	UT_ASSERT(!cluster_space_relation_create(locator));
	UT_ASSERT_EQ(io_calls, 0);
}

UT_TEST(test_existing_identity_is_never_recreated)
{
	PGAlignedBlock saved;

	reset();
	exists = true;
	memset(&page, 0x5a, sizeof(page));
	saved = page;
	UT_ASSERT(!cluster_space_relation_create(locator));
	UT_ASSERT_EQ(wal_calls, 0);
	UT_ASSERT_EQ(create_calls, 0);
	UT_ASSERT(memcmp(&page, &saved, BLCKSZ) == 0);
}

UT_TEST(test_identity_read_is_exact_and_never_creates)
{
	ClusterSpaceIdentity out;
	ClusterSpaceIdentity saved;
	unsigned prior_creates;

	reset();
	memset(&out, 0xa5, sizeof(out));
	saved = out;
	UT_ASSERT(!cluster_space_relation_read_identity(locator, &out));
	UT_ASSERT_EQ(create_calls, 0);
	UT_ASSERT(memcmp(&out, &saved, sizeof(out)) == 0);
	UT_ASSERT(cluster_space_relation_create(locator));
	prior_creates = create_calls;
	UT_ASSERT(cluster_space_relation_read_identity(locator, &out));
	UT_ASSERT_EQ(out.state, CLUSTER_SPACE_IDENTITY_LIVE);
	UT_ASSERT_EQ(out.incarnation[15], 0x45);
	UT_ASSERT(!pinned && !locked);
	saved = out;
	ref.claim.database_incarnation++;
	UT_ASSERT(!cluster_space_relation_read_identity(locator, &out));
	UT_ASSERT(memcmp(&out, &saved, sizeof(out)) == 0);
	ref.claim.database_incarnation--;
	page.data[160] = 1;
	UT_ASSERT(!cluster_space_relation_read_identity(locator, &out));
	UT_ASSERT(memcmp(&out, &saved, sizeof(out)) == 0);
	UT_ASSERT_EQ(create_calls, prior_creates);
	UT_ASSERT(!pinned && !locked);
	page.data[160] = 0;
	out.state = CLUSTER_SPACE_IDENTITY_TOMBSTONED;
	UT_ASSERT(cluster_space_identity_page_encode(&out, 18, page.data, BLCKSZ));
	out = saved;
	UT_ASSERT(!cluster_space_relation_read_identity(locator, &out));
	UT_ASSERT(memcmp(&out, &saved, sizeof(out)) == 0);
	UT_ASSERT(!pinned && !locked);
}

UT_TEST(test_real_replay_exact_duplicate_and_preserved_token)
{
	DecodedXLogRecord decoded;
	XLogReaderState reader;
	PGAlignedBlock saved;

	reset();
	UT_ASSERT(cluster_space_relation_create(locator));
	memset(&decoded, 0, sizeof(decoded));
	memset(&reader, 0, sizeof(reader));
	decoded.header.xl_rmid = RM_SMGR_ID;
	decoded.header.xl_info = 0x30 | XLR_SPECIAL_REL_UPDATE;
	decoded.main_data = (char *)wal_bytes;
	decoded.main_data_len = sizeof(wal_bytes);
	decoded.max_block_id = -1;
	reader.record = &decoded;
	reader.EndRecPtr = UINT64_C(0x20000200);
	reader.cluster_expected_thread_id = 1;
	recovering = true;
	memset(&page, 0, sizeof(page));
	blocks = 0;
	dirty_calls = 0;
	/* Legacy merge's SCN stamp must not replace the typed result token. */
	cluster_recmerge_window_active = true;
	cluster_recmerge_window_scn = 999;
	UT_ASSERT(cluster_space_relation_redo(&reader));
	UT_ASSERT_EQ(((PageHeader)page.data)->pd_block_scn, 17);
	UT_ASSERT_EQ(dirty_calls, 1);
	saved = page;
	UT_ASSERT(cluster_space_relation_redo(&reader));
	UT_ASSERT_EQ(dirty_calls, 1);
	UT_ASSERT(memcmp(&page, &saved, BLCKSZ) == 0);
	ref.claim.database_incarnation++;
	UT_ASSERT(!cluster_space_relation_redo(&reader));
	UT_ASSERT(memcmp(&page, &saved, BLCKSZ) == 0);
	UT_ASSERT_EQ(CritSectionCount, 0);
	UT_ASSERT(!pinned && !locked);
}

UT_TEST(test_native_create_registers_abort_cleanup_before_space)
{
	reset();
	native_owner = true;
	UT_ASSERT(RelationCreateStorage(locator, RELPERSISTENCE_PERMANENT, true) == &storage);
	UT_ASSERT_EQ(main_create_calls, 1);
	UT_ASSERT_EQ(create_calls, 1);
	UT_ASSERT_EQ(wal_calls, 1);
	UT_ASSERT(delete_registered);
	smgrDoPendingDeletes(false);
	UT_ASSERT_EQ(unlink_calls, 1);
}

UT_TEST(test_native_descriptor_recognizes_typed_record)
{
	const char *name = smgr_identify(0x30 | XLR_SPECIAL_REL_UPDATE);

	UT_ASSERT(name != NULL);
	if (name != NULL)
		UT_ASSERT(strcmp(name, "SPACE_IDENTITY") == 0);
	UT_ASSERT(smgr_identify(0xf0) == NULL);
}

static void
truncate_record(XLogReaderState *reader, DecodedXLogRecord *decoded)
{
	ClusterSpaceWalChange change;

	if (!cluster_space_relation_create(locator)
		|| !cluster_space_wal_decode(wal_bytes, sizeof(wal_bytes), &change))
		abort();
	change.action = CLUSTER_SPACE_WAL_TRUNCATE;
	change.nblocks = 4;
	change.expected = change.result;
	change.before_token = change.result_token;
	change.result.incarnation[0] ^= 1;
	change.result.sequence++;
	change.result.operation = ++change.result_token;
	if (!cluster_space_wal_encode(&change, wal_bytes, sizeof(wal_bytes)))
		abort();
	memset(decoded, 0, sizeof(*decoded));
	memset(reader, 0, sizeof(*reader));
	decoded->header.xl_rmid = RM_SMGR_ID;
	decoded->header.xl_info = XLOG_SMGR_SPACE_IDENTITY | XLR_SPECIAL_REL_UPDATE;
	decoded->main_data = (char *)wal_bytes;
	decoded->main_data_len = sizeof(wal_bytes);
	decoded->max_block_id = -1;
	reader->record = decoded;
	reader->EndRecPtr = UINT64_C(0x20000200);
	reader->cluster_expected_thread_id = 1;
	recovering = true;
}

UT_TEST(test_truncate_replay_checks_identity_before_native_shrink)
{
	XLogReaderState reader;
	DecodedXLogRecord decoded;
	PGAlignedBlock saved;

	reset();
	truncate_record(&reader, &decoded);
	smgr_redo(&reader);
	UT_ASSERT_EQ(main_blocks, 4);
	UT_ASSERT_EQ(truncate_calls, 1);
	UT_ASSERT_EQ(((PageHeader)page.data)->pd_block_scn, 18);
	UT_ASSERT(!pinned && !locked);
	saved = page;
	UT_ASSERT(cluster_space_relation_redo(&reader));
	UT_ASSERT_EQ(truncate_calls, 2);
	UT_ASSERT(memcmp(&page, &saved, BLCKSZ) == 0);
	UT_ASSERT_EQ(dirty_calls, 2); /* CREATE and the first TRUNCATE only. */
	UT_ASSERT_EQ(fake_allocations, 2);
	UT_ASSERT_EQ(fake_frees, fake_allocations);
}

UT_TEST(test_truncate_mismatch_or_foreign_input_does_not_touch_files)
{
	XLogReaderState reader;
	DecodedXLogRecord decoded;
	PGAlignedBlock saved;

	reset();
	truncate_record(&reader, &decoded);
	/* No claim that the legacy local minRecoveryPoint is a foreign-WAL
	 * durability coordinate. The per-origin input binding is still required. */
	cluster_recmerge_apply_foreign = true;
	saved = page;
	UT_ASSERT(!cluster_space_relation_redo(&reader));
	UT_ASSERT_EQ(truncate_calls, 0);
	UT_ASSERT_EQ(flush_calls, 0);
	UT_ASSERT(memcmp(&page, &saved, BLCKSZ) == 0);
	cluster_recmerge_apply_foreign = false;
	reader.cluster_expected_thread_id = 2;
	UT_ASSERT(!cluster_space_relation_redo(&reader));
	UT_ASSERT_EQ(truncate_calls, 0);
	UT_ASSERT_EQ(flush_calls, 0);
	reader.cluster_expected_thread_id = 1;
	((PageHeader)page.data)->pd_block_scn = 99;
	saved = page;
	UT_ASSERT(!cluster_space_relation_redo(&reader));
	UT_ASSERT_EQ(truncate_calls, 0);
	UT_ASSERT_EQ(main_blocks, 10);
	UT_ASSERT_EQ(flush_calls, 0);
	UT_ASSERT(memcmp(&page, &saved, BLCKSZ) == 0);
	UT_ASSERT(!pinned && !locked);
}

UT_TEST(test_truncate_replay_preserves_native_auxiliary_forks)
{
	XLogReaderState reader;
	DecodedXLogRecord decoded;

	reset();
	truncate_record(&reader, &decoded);
	auxiliary_forks = true;
	smgr_redo(&reader);
	UT_ASSERT_EQ(main_blocks, 4);
	UT_ASSERT_EQ(truncate_calls, 1);
	UT_ASSERT_EQ(fsm_vacuums, 1);
	UT_ASSERT_EQ(blocks, 1); /* SPACE was not physically truncated. */
	UT_ASSERT_EQ(fake_allocations, 1);
	UT_ASSERT_EQ(fake_frees, fake_allocations);
	UT_ASSERT_EQ(((PageHeader)page.data)->pd_block_scn, 18);
	UT_ASSERT_EQ(CritSectionCount, 0);
	UT_ASSERT(!pinned && !locked);
}

UT_TEST(test_aborted_drop_keeps_existing_live_identity)
{
	RelationData rel;
	FormData_pg_class form;
	PGAlignedBlock saved;

	reset();
	UT_ASSERT(cluster_space_relation_create(locator));
	saved = page;
	native_owner = true;
	memset(&rel, 0, sizeof(rel));
	memset(&form, 0, sizeof(form));
	rel.rd_locator = locator;
	rel.rd_backend = InvalidBackendId;
	rel.rd_smgr = &storage;
	rel.rd_rel = &form;
	form.relpersistence = RELPERSISTENCE_PERMANENT;
	storage.smgr_owner = &rel.rd_smgr;
	RelationDropStorage(&rel);
	UT_ASSERT_EQ(ko_calls, 1);
	UT_ASSERT_EQ(unlink_calls, 0);
	UT_ASSERT(memcmp(&page, &saved, BLCKSZ) == 0);
	smgrDoPendingDeletes(false);
	UT_ASSERT_EQ(unlink_calls, 0);
	UT_ASSERT(memcmp(&page, &saved, BLCKSZ) == 0);
	UT_ASSERT_EQ(wal_calls, 1); /* No early irreversible tombstone. */
}

UT_TEST(test_subabort_forgets_drop_without_tombstoning)
{
	RelationData rel;
	FormData_pg_class form;
	PGAlignedBlock saved;

	reset();
	UT_ASSERT(cluster_space_relation_create(locator));
	saved = page;
	native_owner = true;
	nest_level = 2;
	memset(&rel, 0, sizeof(rel));
	memset(&form, 0, sizeof(form));
	rel.rd_locator = locator;
	rel.rd_backend = InvalidBackendId;
	rel.rd_smgr = &storage;
	rel.rd_rel = &form;
	form.relpersistence = RELPERSISTENCE_PERMANENT;
	storage.smgr_owner = &rel.rd_smgr;
	RelationDropStorage(&rel);
	AtSubAbort_smgr();
	nest_level = 1;
	smgrDoPendingDeletes(true);
	UT_ASSERT_EQ(unlink_calls, 0);
	UT_ASSERT(memcmp(&page, &saved, BLCKSZ) == 0);
	UT_ASSERT_EQ(wal_calls, 1);
}

int
main(void)
{
	UT_PLAN(12);
	UT_RUN(test_real_create_binds_selected_namespace_and_wal);
	UT_RUN(test_no_create_on_legacy_or_unproved_namespace);
	UT_RUN(test_existing_identity_is_never_recreated);
	UT_RUN(test_identity_read_is_exact_and_never_creates);
	UT_RUN(test_real_replay_exact_duplicate_and_preserved_token);
	UT_RUN(test_native_create_registers_abort_cleanup_before_space);
	UT_RUN(test_native_descriptor_recognizes_typed_record);
	UT_RUN(test_truncate_replay_checks_identity_before_native_shrink);
	UT_RUN(test_truncate_mismatch_or_foreign_input_does_not_touch_files);
	UT_RUN(test_truncate_replay_preserves_native_auxiliary_forks);
	UT_RUN(test_aborted_drop_keeps_existing_live_identity);
	UT_RUN(test_subabort_forgets_drop_without_tombstoning);
	UT_DONE();
	return ut_failed_count ? 1 : 0;
}
