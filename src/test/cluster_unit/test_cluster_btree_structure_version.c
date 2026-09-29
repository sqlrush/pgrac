/*-------------------------------------------------------------------------
 *
 * test_cluster_btree_structure_version.c
 *    Native btree allocation, insertion, split and root version publication.
 *
 * Portions Copyright (c) 1996-2023, PostgreSQL Global Development Group
 * Portions Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *    src/test/cluster_unit/test_cluster_btree_structure_version.c
 *
 * NOTES
 *    Native bodies plus real page/version helpers; buffer/WAL I/O,
 *    recyclability and split/truncation decisions are explicit fixtures.
 *    This is not a concurrent index algorithm or recovery qualification.
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <setjmp.h>

#include "access/nbtree.h"
#include "access/nbtxlog.h"
#include "access/xlog.h"
#include "access/xloginsert.h"
#include "catalog/catalog.h"
#include "catalog/pg_class.h"
#include "cluster/cluster_guc.h"
#include "cluster/cluster_space_storage.h"
#include "cluster/cluster_wal_thread.h"
#include "cluster/cluster_xnode_profile.h"
#include "cluster/storage/cluster_smgr.h"
#include "miscadmin.h"
#include "storage/indexfsm.h"
#include "storage/procarray.h"
#include "storage/predicate.h"
#include "utils/memdebug.h"
#include "utils/memutils.h"
#include "utils/rel.h"
#include "unit_test.h"

UT_DEFINE_GLOBALS();
bool cluster_enabled = true, cluster_shared_config = true;
int cluster_node_id, wal_level = WAL_LEVEL_REPLICA, NBuffers = 6, NLocBuffer;
char *BufferBlocks;
Block *LocalBufferBlockPointers;
volatile uint32 CritSectionCount;
bool cluster_recmerge_window_active, cluster_recmerge_apply_foreign;
uint64 cluster_recmerge_window_scn, cluster_recmerge_window_own_lsn;

static PGAlignedBlock pages[6], before[6], images[6];
static RelationData relation_data;
static FormData_pg_class relform;
static FormData_pg_index indexform;
static ClusterSpaceIdentity space_identity;
static RfPageVersionEdgeEntryV1 edges[4];
static unsigned wal_count, edge_count, dirties, edge_blocks, registered, fsm_calls;
static uint64 token;
static uint8 expected_info;
static bool locked[6], new_from_fsm, recyclable = true, error_expected, begun;
volatile sig_atomic_t InterruptPending;
static unsigned pending_fsm;
static jmp_buf error_jump;
static const BlockNumber blocks[6] = { 7, 8, 9, 0, 6, 10 };

void
ExceptionalCondition(const char *condition, const char *file, int line)
{
	printf("# assertion %s %s:%d\n", condition, file, line);
	abort();
}
bool
errstart(int level, const char *domain)
{
	(void)domain;
	return level >= ERROR;
}
bool
errstart_cold(int level, const char *domain)
{
	return errstart(level, domain);
}
int
errmsg_internal(const char *format, ...)
{
	(void)format;
	return 0;
}
int
errmsg(const char *format, ...)
{
	(void)format;
	return 0;
}
int
errcode(int code)
{
	(void)code;
	return 0;
}
void
errfinish(const char *file, int line, const char *function)
{
	if (!error_expected) {
		printf("# unexpected %s:%d %s\n", file, line, function);
		abort();
	}
	longjmp(error_jump, 1);
}
void *
palloc(Size size)
{
	return malloc(size);
}
void *
palloc0(Size size)
{
	return calloc(1, size);
}
void *
MemoryContextAlloc(MemoryContext context, Size size)
{
	(void)context;
	return malloc(size);
}
void
pfree(void *p)
{
	free(p);
}
bool
RecoveryInProgress(void)
{
	return false;
}
bool
IsCatalogRelation(Relation rel)
{
	(void)rel;
	return false;
}
bool
GlobalVisCheckRemovableFullXid(Relation rel, FullTransactionId xid)
{
	(void)rel;
	UT_ASSERT_EQ(U64FromFullTransactionId(xid), 50);
	return recyclable;
}
FullTransactionId
GetTopFullTransactionId(void)
{
	abort();
}
bool
cluster_wal_thread_current_v2_ref(ClusterWalDurablePrefixRef *out)
{
	memset(out, 0, sizeof(*out));
	out->claim.identity.system_identifier = 11;
	out->claim.database_incarnation = 12;
	memset(out->claim.identity.storage_uuid, 13, 16);
	return true;
}
bool
cluster_wal_thread_restart_v2_ref(ClusterWalDurablePrefixRef *out)
{
	(void)out;
	abort();
}
int
cluster_smgr_which_for(RelFileLocator locator, BackendId backend)
{
	UT_ASSERT_EQ(locator.dbOid, 5);
	UT_ASSERT_EQ(backend, InvalidBackendId);
	return 1;
}
static bool
fixture_get_identity(Relation rel, ClusterSpaceIdentity *out)
{
	(void)rel;
	*out = space_identity;
	return true;
}
void
BufferGetTag(Buffer buffer, RelFileLocator *locator, ForkNumber *forknum, BlockNumber *block)
{
	UT_ASSERT(buffer > 0 && buffer <= 6);
	*locator = relation_data.rd_locator;
	*forknum = MAIN_FORKNUM;
	*block = blocks[buffer - 1];
}
bool
BufferIsPermanent(Buffer buffer)
{
	return buffer > 0 && buffer <= 6;
}
BlockNumber
BufferGetBlockNumber(Buffer buffer)
{
	Assert(buffer > 0 && buffer <= 6);
	return blocks[buffer - 1];
}
SCN
cluster_scn_advance(void)
{
	UT_ASSERT_EQ(CritSectionCount, 0);
	for (int i = 0; i < 6; i++)
		UT_ASSERT(memcmp(pages[i].data, before[i].data, BLCKSZ) == 0);
	return ++token;
}
static Buffer
buffer_for(BlockNumber block)
{
	for (int i = 0; i < 6; i++)
		if (blocks[i] == block)
			return i + 1;
	abort();
}
Buffer
ReadBuffer(Relation rel, BlockNumber block)
{
	(void)rel;
	return buffer_for(block);
}
Buffer
_bt_getbuf(Relation rel, BlockNumber block, int access)
{
	Buffer b = ReadBuffer(rel, block);
	(void)access;
	UT_ASSERT(!locked[b - 1]);
	locked[b - 1] = true;
	return b;
}
void
_bt_unlockbuf(Relation rel, Buffer buffer)
{
	(void)rel;
	UT_ASSERT(locked[buffer - 1]);
	locked[buffer - 1] = false;
}
void
_bt_lockbuf(Relation rel, Buffer buffer, int access)
{
	(void)rel;
	(void)access;
	UT_ASSERT(!locked[buffer - 1]);
	locked[buffer - 1] = true;
}
void
_bt_relbuf(Relation rel, Buffer buffer)
{
	_bt_unlockbuf(rel, buffer);
}
Buffer
_bt_relandgetbuf(Relation rel, Buffer old, BlockNumber block, int access)
{
	_bt_relbuf(rel, old);
	return _bt_getbuf(rel, block, access);
}
bool
_bt_conditionallockbuf(Relation rel, Buffer buffer)
{
	(void)rel;
	UT_ASSERT(!locked[buffer - 1]);
	locked[buffer - 1] = true;
	return true;
}
void
ReleaseBuffer(Buffer buffer)
{
	(void)buffer;
}
BlockNumber
GetFreeIndexPage(Relation rel)
{
	(void)rel;
	return new_from_fsm && fsm_calls++ == 0 ? 8 : InvalidBlockNumber;
}
Buffer
ExtendBufferedRel(BufferManagerRelation bmr, ForkNumber forknum, BufferAccessStrategy strategy,
				  uint32 flags)
{
	(void)bmr;
	UT_ASSERT(forknum == MAIN_FORKNUM && strategy == NULL && flags == EB_LOCK_FIRST);
	UT_ASSERT(PageIsNew(pages[1].data));
	locked[1] = true;
	return 2;
}
void
MarkBufferDirty(Buffer buffer)
{
	UT_ASSERT(locked[buffer - 1] && CritSectionCount == 1);
	dirties |= 1U << (buffer - 1);
}
void
XLogBeginInsert(void)
{
	UT_ASSERT(!begun);
	begun = true;
}
void
XLogRegisterData(char *data, uint32 length)
{
	UT_ASSERT(begun && data && length);
}
void
XLogRegisterBufData(uint8 id, char *data, uint32 length)
{
	(void)id;
	UT_ASSERT(begun && data && length);
}
void
XLogRegisterBuffer(uint8 id, Buffer buffer, uint8 flags)
{
	(void)flags;
	UT_ASSERT(begun && CritSectionCount == 1 && locked[buffer - 1]);
	registered |= 1U << id;
	memcpy(images[buffer - 1].data, pages[buffer - 1].data, BLCKSZ);
}
void
XLogRegisterPageVersionEdge(uint64 result, const RfPageVersionEdgeEntryV1 *entries, uint8 count)
{
	UT_ASSERT(begun && result == 201 && count <= 4);
	memcpy(edges, entries, count * sizeof(*entries));
	edge_count = count;
	edge_blocks++;
}
XLogRecPtr
XLogInsert(RmgrId rmgr, uint8 info)
{
	UT_ASSERT(begun && rmgr == RM_BTREE_ID);
	begun = false;
	if (info == XLOG_BTREE_REUSE_PAGE) {
		UT_ASSERT_EQ(CritSectionCount, 0);
		return 0x8000;
	}
	UT_ASSERT_EQ(info, expected_info);
	UT_ASSERT_EQ(CritSectionCount, 1);
	wal_count++;
	return 0x9000;
}
BTCycleId
_bt_vacuum_cycleid(Relation rel)
{
	(void)rel;
	return 0;
}
bool
_bt_check_natts(Relation rel, bool heapkeyspace, Page page, OffsetNumber offset)
{
	(void)rel;
	(void)heapkeyspace;
	(void)page;
	(void)offset;
	return true;
}
OffsetNumber
_bt_findsplitloc(Relation rel, Page page, OffsetNumber offset, Size size, IndexTuple item,
				 bool *newleft)
{
	(void)rel;
	(void)page;
	(void)offset;
	(void)size;
	(void)item;
	*newleft = false;
	return 3;
}
IndexTuple
CopyIndexTuple(IndexTuple source)
{
	Size size = IndexTupleSize(source);
	IndexTuple out = palloc(size);
	memcpy(out, source, size);
	return out;
}
IndexTuple
_bt_truncate(Relation rel, IndexTuple left, IndexTuple right, BTScanInsert key)
{
	IndexTuple out = CopyIndexTuple(right);
	(void)rel;
	(void)left;
	(void)key;
	BTreeTupleSetNAtts(out, 1, false);
	return out;
}
static void
fixture_parent(Relation rel, ...)
{
	(void)rel;
	abort();
}
static Buffer
fixture_split(Relation rel, ...)
{
	(void)rel;
	abort();
}
IndexTuple
_bt_swap_posting(IndexTuple item, IndexTuple posting, int pos)
{
	(void)item;
	(void)posting;
	(void)pos;
	abort();
}
void
cluster_xp_count(ClusterXnodeBucket counter)
{
	(void)counter;
}
void
PredicateLockPageSplit(Relation rel, BlockNumber oldblock, BlockNumber newblock)
{
	(void)rel;
	(void)oldblock;
	(void)newblock;
	abort();
}
int
_bt_getrootheight(Relation rel)
{
	(void)rel;
	return 0;
}
void
ProcessInterrupts(void)
{
	abort();
}
FullTransactionId
ReadNextFullTransactionId(void)
{
	return FullTransactionIdFromU64(50);
}
void
PredicateLockPageCombine(Relation rel, BlockNumber left, BlockNumber right)
{
	(void)rel;
	UT_ASSERT(left == 7 && right == 9);
}
static bool
_bt_rightsib_halfdeadflag(Relation rel, BlockNumber block)
{
	(void)rel;
	UT_ASSERT_EQ(block, 9);
	return false;
}
static bool
_bt_lock_subtree_parent(Relation rel, Relation heaprel, BlockNumber child, BTStack stack,
						Buffer *parent, OffsetNumber *offset, BlockNumber *top, BlockNumber *right,
						const ClusterSpaceIdentity *identity)
{
	(void)heaprel;
	(void)stack;
	UT_ASSERT(child == 7 && *top == 7 && *right == 9);
	UT_ASSERT(identity == &space_identity);
	*parent = _bt_getbuf(rel, 6, BT_WRITE);
	*offset = 1;
	return true;
}
static void
_bt_pendingfsm_add(BTVacState *vstate, BlockNumber target, FullTransactionId xid)
{
	(void)vstate;
	UT_ASSERT(target == 7 || target == 6);
	UT_ASSERT_EQ(U64FromFullTransactionId(xid), 50);
	UT_ASSERT_EQ(CritSectionCount, 0);
	pending_fsm++;
}

#define cluster_space_relation_get_identity fixture_get_identity
#include "test_cluster_btree_structure_helpers.inc"
#undef cluster_space_relation_get_identity
#include "test_cluster_btree_structure_alloc.inc"
#include "test_cluster_btree_structure_root.inc"
#include "test_cluster_btree_structure_delete.inc"
#include "test_cluster_btree_structure_add.inc"

static Buffer
run_split(Relation rel, Relation heaprel, BTScanInsert itup_key, Buffer buf, Buffer cbuf,
		  OffsetNumber newitemoff, Size newitemsz, IndexTuple newitem, IndexTuple orignewitem,
		  IndexTuple nposting, uint16 postingoff,
		  const ClusterSpaceIdentity *identity pg_attribute_unused())
#include "test_cluster_btree_structure_split.inc"

	static Buffer run_newlevel(Relation rel, Relation heaprel, Buffer lbuf, Buffer rbuf,
							   const ClusterSpaceIdentity *identity pg_attribute_unused())
#include "test_cluster_btree_structure_newlevel.inc"

#define _bt_split fixture_split
#define _bt_insert_parent fixture_parent
		static void run_insert(Relation rel, Relation heaprel, BTScanInsert itup_key, Buffer buf,
							   Buffer cbuf, BTStack stack, IndexTuple itup, Size itemsz,
							   OffsetNumber newitemoff, int postingoff, bool split_only_page,
							   const ClusterSpaceIdentity *identity pg_attribute_unused())
#include "test_cluster_btree_structure_insert.inc"
#undef _bt_split
#undef _bt_insert_parent

			static void reset(void)
{
	memset(pages, 0, sizeof(pages));
	memset(images, 0, sizeof(images));
	memset(locked, 0, sizeof(locked));
	memset(&relation_data, 0, sizeof(relation_data));
	memset(&space_identity, 0, sizeof(space_identity));
	memset(&relform, 0, sizeof(relform));
	memset(&indexform, 0, sizeof(indexform));
	memset(edges, 0, sizeof(edges));
	relation_data.rd_rel = &relform;
	relation_data.rd_index = &indexform;
	relation_data.rd_locator = (RelFileLocator){ 1663, 5, 900 };
	relation_data.rd_backend = InvalidBackendId;
	relform.relpersistence = RELPERSISTENCE_PERMANENT;
	relform.relkind = RELKIND_INDEX;
	relform.relam = BTREE_AM_OID;
	indexform.indnkeyatts = indexform.indnatts = 1;
	space_identity.key.system_identifier = 11;
	space_identity.key.database_incarnation = 12;
	memset(space_identity.key.storage_uuid, 13, 16);
	space_identity.key.locator = relation_data.rd_locator;
	memset(space_identity.incarnation, 30, 16);
	space_identity.sequence = 1;
	space_identity.operation = 40;
	space_identity.state = CLUSTER_SPACE_IDENTITY_LIVE;
	for (int i = 0; i < 6; i++) {
		if (i == 1)
			continue;
		PageInit(pages[i].data, BLCKSZ, sizeof(BTPageOpaqueData));
		BTPageGetOpaque(pages[i].data)->btpo_flags = BTP_LEAF;
		((PageHeader)pages[i].data)->pd_block_scn = 70 + i;
	}
	BTPageGetOpaque(pages[3].data)->btpo_flags = BTP_META;
	BTPageGetMeta(pages[3].data)->btm_magic = BTREE_MAGIC;
	BTPageGetMeta(pages[3].data)->btm_version = BTREE_VERSION;
	BTPageGetMeta(pages[3].data)->btm_root = BTPageGetMeta(pages[3].data)->btm_fastroot = 7;
	((PageHeader)pages[3].data)->pd_lower = SizeOfPageHeaderData + sizeof(BTMetaPageData);
	token = 200;
	wal_count = edge_count = edge_blocks = dirties = registered = fsm_calls = 0;
	CritSectionCount = 0;
	begun = new_from_fsm = error_expected = false;
	recyclable = cluster_shared_config = true;
	BufferBlocks = pages[0].data;
	pending_fsm = 0;
}
static void
save_before(void)
{
	memcpy(before, pages, sizeof(pages));
}
static IndexTuple
new_tuple(int key)
{
	IndexTuple out = palloc0(16);
	out->t_info = 16;
	ItemPointerSet(&out->t_tid, 2, key);
	*(uint64 *)((char *)out + sizeof(IndexTupleData)) = key;
	return out;
}
static void
check_version(unsigned count, unsigned mask)
{
	UT_ASSERT_EQ(edge_count, count);
	UT_ASSERT_EQ(edge_blocks, 1);
	UT_ASSERT_EQ(registered, mask);
	UT_ASSERT_EQ(wal_count, 1);
	for (int i = 0; i < 6; i++) {
		if (!(dirties & (1U << i)))
			continue;
		UT_ASSERT_EQ(((PageHeader)pages[i].data)->pd_block_scn, 201);
		UT_ASSERT_EQ(((PageHeader)images[i].data)->pd_block_scn, 201);
		UT_ASSERT_EQ(PageGetLSN(pages[i].data), 0x9000);
	}
}
UT_TEST(test_shared_allocator_preserves_exact_predecessor)
{
	for (int kind = 0; kind < 3; kind++) {
		reset();
		new_from_fsm = kind != 0;
		if (kind == 2) {
			PageInit(pages[1].data, BLCKSZ, sizeof(BTPageOpaqueData));
			BTPageSetDeleted(pages[1].data, FullTransactionIdFromU64(50));
			((PageHeader)pages[1].data)->pd_block_scn = 88;
		}
		save_before();
		UT_ASSERT_EQ(_bt_allocbuf(&relation_data, &relation_data), 2);
		UT_ASSERT(memcmp(before[1].data, pages[1].data, BLCKSZ) == 0);
		UT_ASSERT(locked[1]);
		UT_ASSERT_EQ(dirties, 0);
	}
}
UT_TEST(test_native_insert_has_one_version)
{
	IndexTuple item;
	reset();
	locked[0] = true;
	item = new_tuple(1);
	save_before();
	expected_info = XLOG_BTREE_INSERT_LEAF;
	run_insert(&relation_data, &relation_data, NULL, 1, InvalidBuffer, NULL, item, 16, 1, 0, false,
			   &space_identity);
	UT_ASSERT_EQ(PageGetMaxOffsetNumber(pages[0].data), 1);
	check_version(1, 1);
	pfree(item);
}
UT_TEST(test_native_split_captures_zero_new_page)
{
	IndexTuple item;
	BTScanInsertData key;
	reset();
	locked[0] = true;
	memset(&key, 0, sizeof(key));
	key.heapkeyspace = true;
	for (int i = 1; i <= 3; i++) {
		item = new_tuple(i);
		UT_ASSERT_EQ(PageAddItem(pages[0].data, (Item)item, 16, i, false, false), i);
		pfree(item);
	}
	item = new_tuple(4);
	save_before();
	expected_info = XLOG_BTREE_SPLIT_R;
	UT_ASSERT_EQ(run_split(&relation_data, &relation_data, &key, 1, InvalidBuffer, 4, 16, item,
						   NULL, NULL, 0, &space_identity),
				 2);
	UT_ASSERT_EQ(BTPageGetOpaque(pages[0].data)->btpo_next, 8);
	UT_ASSERT_EQ(BTPageGetOpaque(pages[1].data)->btpo_prev, 7);
	check_version(2, 3);
	UT_ASSERT_EQ(edges[1].before_kind, RF_PAGE_STATE_UNFORMATTED);
	pfree(item);
}
UT_TEST(test_native_newlevel_versions_root_child_and_meta)
{
	IndexTuple high;
	reset();
	locked[0] = locked[2] = true;
	BTPageGetOpaque(pages[0].data)->btpo_flags |= BTP_INCOMPLETE_SPLIT;
	BTPageGetOpaque(pages[0].data)->btpo_next = 9;
	high = new_tuple(4);
	BTreeTupleSetNAtts(high, 1, false);
	UT_ASSERT_EQ(PageAddItem(pages[0].data, (Item)high, 16, 1, false, false), 1);
	pfree(high);
	save_before();
	expected_info = XLOG_BTREE_NEWROOT;
	UT_ASSERT_EQ(run_newlevel(&relation_data, &relation_data, 1, 3, &space_identity), 2);
	UT_ASSERT_EQ(BTPageGetMeta(pages[3].data)->btm_root, 8);
	UT_ASSERT_EQ(PageGetMaxOffsetNumber(pages[1].data), 2);
	check_version(3, 7);
	UT_ASSERT_EQ(edges[0].before_kind, RF_PAGE_STATE_UNFORMATTED);
}

UT_TEST(test_first_root_preserves_zero_or_recycled_predecessor)
{
	for (int reused = 0; reused < 2; reused++) {
		reset();
		BTPageGetMeta(pages[3].data)->btm_root = P_NONE;
		BTPageGetMeta(pages[3].data)->btm_fastroot = P_NONE;
		if (reused) {
			new_from_fsm = true;
			PageInit(pages[1].data, BLCKSZ, sizeof(BTPageOpaqueData));
			BTPageSetDeleted(pages[1].data, FullTransactionIdFromU64(50));
			((PageHeader)pages[1].data)->pd_block_scn = 88;
		}
		save_before();
		expected_info = XLOG_BTREE_NEWROOT;
		UT_ASSERT_EQ(_bt_getroot(&relation_data, &relation_data, BT_WRITE, &space_identity), 2);
		check_version(2, 5);
		UT_ASSERT_EQ(edges[0].before_kind,
					 reused ? RF_PAGE_STATE_PRESENT : RF_PAGE_STATE_UNFORMATTED);
		UT_ASSERT_EQ(edges[0].before.mutation_token, reused ? 88 : 0);
		UT_ASSERT_EQ(BTPageGetMeta(pages[3].data)->btm_root, 8);
		UT_ASSERT(P_ISROOT(BTPageGetOpaque(pages[1].data)));
		UT_ASSERT(locked[1] && !locked[3]);
	}
}

UT_TEST(test_upper_insert_versions_child_and_fastroot)
{
	IndexTuple item;
	reset();
	locked[0] = locked[4] = true;
	BTPageGetOpaque(pages[0].data)->btpo_flags = 0;
	BTPageGetOpaque(pages[0].data)->btpo_level = 1;
	BTPageGetOpaque(pages[4].data)->btpo_flags |= BTP_INCOMPLETE_SPLIT;
	item = new_tuple(1);
	BTreeTupleSetNAtts(item, 0, false);
	UT_ASSERT_EQ(PageAddItem(pages[0].data, (Item)item, 16, 1, false, false), 1);
	pfree(item);
	item = new_tuple(3);
	BTreeTupleSetNAtts(item, 1, false);
	save_before();
	expected_info = XLOG_BTREE_INSERT_META;
	run_insert(&relation_data, &relation_data, NULL, 1, 5, NULL, item, 16, 2, 0, true,
			   &space_identity);
	check_version(3, 7);
	UT_ASSERT(!P_INCOMPLETE_SPLIT(BTPageGetOpaque(pages[4].data)));
	UT_ASSERT_EQ(BTPageGetMeta(pages[3].data)->btm_fastlevel, 1);
	pfree(item);
}

UT_TEST(test_internal_split_versions_all_four_native_blocks)
{
	IndexTuple item;
	reset();
	locked[0] = locked[4] = true;
	BTPageGetOpaque(pages[0].data)->btpo_flags = 0;
	BTPageGetOpaque(pages[0].data)->btpo_level = 1;
	BTPageGetOpaque(pages[0].data)->btpo_next = 9;
	BTPageGetOpaque(pages[2].data)->btpo_prev = 7;
	BTPageGetOpaque(pages[4].data)->btpo_flags |= BTP_INCOMPLETE_SPLIT;
	for (int i = 1; i <= 4; i++) {
		item = new_tuple(i);
		BTreeTupleSetNAtts(item, 1, false);
		UT_ASSERT_EQ(PageAddItem(pages[0].data, (Item)item, 16, i, false, false), i);
		pfree(item);
	}
	item = new_tuple(5);
	BTreeTupleSetNAtts(item, 1, false);
	save_before();
	expected_info = XLOG_BTREE_SPLIT_R;
	UT_ASSERT_EQ(run_split(&relation_data, &relation_data, NULL, 1, 5, 5, 16, item, NULL, NULL, 0,
						   &space_identity),
				 2);
	check_version(4, 15);
	UT_ASSERT_EQ(BTPageGetOpaque(pages[2].data)->btpo_prev, 8);
	UT_ASSERT(!P_INCOMPLETE_SPLIT(BTPageGetOpaque(pages[4].data)));
	UT_ASSERT_EQ(edges[1].before_kind, RF_PAGE_STATE_UNFORMATTED);
	pfree(item);
}

UT_TEST(test_bad_first_root_predecessor_or_identity_changes_no_page)
{
	for (int kind = 0; kind < 3; kind++) {
		reset();
		BTPageGetMeta(pages[3].data)->btm_root = P_NONE;
		if (kind == 0)
			pages[1].data[BLCKSZ - 1] = 42;
		if (kind == 1)
			space_identity.key.database_incarnation++;
		save_before();
		error_expected = true;
		if (setjmp(error_jump) == 0) {
			_bt_getroot(&relation_data, &relation_data, BT_WRITE,
						kind == 2 ? NULL : &space_identity);
			UT_ASSERT(false);
		}
		error_expected = false;
		UT_ASSERT(memcmp(pages, before, sizeof(pages)) == 0);
		UT_ASSERT_EQ(wal_count + edge_count + dirties, 0);
		UT_ASSERT_EQ(token, 200);
		UT_ASSERT_EQ(CritSectionCount, 0);
	}
}

UT_TEST(test_nonshared_first_root_preserves_native_wal)
{
	reset();
	cluster_shared_config = false;
	BTPageGetMeta(pages[3].data)->btm_root = P_NONE;
	save_before();
	expected_info = XLOG_BTREE_NEWROOT;
	UT_ASSERT_EQ(_bt_getroot(&relation_data, &relation_data, BT_WRITE, NULL), 2);
	UT_ASSERT_EQ(edge_count + edge_blocks, 0);
	UT_ASSERT_EQ(wal_count, 1);
	UT_ASSERT_EQ(PageGetLSN(pages[1].data), 0x9000);
	UT_ASSERT_EQ(((PageHeader)pages[1].data)->pd_block_scn, 0);
}

static void
add_downlink(Page page, OffsetNumber offset, BlockNumber child, bool high)
{
	IndexTuple item = new_tuple(offset);
	BTreeTupleSetNAtts(item, 1, false);
	if (high)
		BTreeTupleSetTopParent(item, child);
	else
		BTreeTupleSetDownLink(item, child);
	UT_ASSERT_EQ(PageAddItem(page, (Item)item, 16, offset, false, false), offset);
	pfree(item);
}

UT_TEST(test_mark_halfdead_versions_leaf_and_parent)
{
	reset();
	locked[0] = true;
	BTPageGetOpaque(pages[0].data)->btpo_next = 9;
	add_downlink(pages[0].data, 1, 9, false);
	BTPageGetOpaque(pages[4].data)->btpo_flags = 0;
	BTPageGetOpaque(pages[4].data)->btpo_level = 1;
	add_downlink(pages[4].data, 1, 7, false);
	add_downlink(pages[4].data, 2, 9, false);
	save_before();
	expected_info = XLOG_BTREE_MARK_PAGE_HALFDEAD;
	UT_ASSERT(_bt_mark_page_halfdead(&relation_data, &relation_data, 1, NULL, &space_identity));
	check_version(2, 3);
	UT_ASSERT(P_ISHALFDEAD(BTPageGetOpaque(pages[0].data)));
	UT_ASSERT_EQ(PageGetMaxOffsetNumber(pages[4].data), 1);
}

UT_TEST(test_unlink_versions_exact_native_component_sets)
{
	for (int internal = 0; internal < 2; internal++) {
		for (int left = 0; left < 2; left++) {
			BTVacState vacuum;
			IndexBulkDeleteResult stats;
			bool empty;
			Buffer target = internal ? 5 : 1;
			reset();
			locked[0] = true;
			memset(&vacuum, 0, sizeof(vacuum));
			memset(&stats, 0, sizeof(stats));
			vacuum.stats = &stats;
			vacuum.version_identity = space_identity;
			BTPageGetOpaque(pages[0].data)->btpo_flags |= BTP_HALF_DEAD;
			BTPageGetOpaque(pages[0].data)->btpo_next = internal ? 10 : 9;
			add_downlink(pages[0].data, 1, internal ? 6 : InvalidBlockNumber, true);
			if (internal) {
				BTPageGetOpaque(pages[4].data)->btpo_flags = 0;
				BTPageGetOpaque(pages[4].data)->btpo_level = 1;
				BTPageGetOpaque(pages[4].data)->btpo_next = 9;
				add_downlink(pages[4].data, 1, 9, false);
				add_downlink(pages[4].data, 2, 7, false);
			}
			BTPageGetOpaque(pages[target - 1].data)->btpo_prev = left ? 8 : P_NONE;
			PageInit(pages[1].data, BLCKSZ, sizeof(BTPageOpaqueData));
			((PageHeader)pages[1].data)->pd_block_scn = 71;
			BTPageGetOpaque(pages[1].data)->btpo_next = blocks[target - 1];
			BTPageGetOpaque(pages[2].data)->btpo_prev = blocks[target - 1];
			BTPageGetOpaque(pages[2].data)->btpo_level = internal;
			BTPageGetMeta(pages[3].data)->btm_fastlevel = internal + 1;
			save_before();
			expected_info = left ? XLOG_BTREE_UNLINK_PAGE : XLOG_BTREE_UNLINK_PAGE_META;
			UT_ASSERT(_bt_unlink_halfdead_page(&relation_data, 1, 20, &empty, &vacuum));
			check_version(internal ? 4 : 3, (left ? 7 : 21) | (internal ? 8 : 0));
			UT_ASSERT(P_ISDELETED(BTPageGetOpaque(pages[target - 1].data)));
			UT_ASSERT_EQ(stats.pages_newly_deleted, 1);
			UT_ASSERT_EQ(pending_fsm, 1);
			UT_ASSERT(locked[0]);
		}
	}
}
int
main(void)
{
	UT_PLAN(11);
	UT_RUN(test_shared_allocator_preserves_exact_predecessor);
	UT_RUN(test_native_insert_has_one_version);
	UT_RUN(test_native_split_captures_zero_new_page);
	UT_RUN(test_native_newlevel_versions_root_child_and_meta);
	UT_RUN(test_first_root_preserves_zero_or_recycled_predecessor);
	UT_RUN(test_upper_insert_versions_child_and_fastroot);
	UT_RUN(test_internal_split_versions_all_four_native_blocks);
	UT_RUN(test_bad_first_root_predecessor_or_identity_changes_no_page);
	UT_RUN(test_nonshared_first_root_preserves_native_wal);
	UT_RUN(test_mark_halfdead_versions_leaf_and_parent);
	UT_RUN(test_unlink_versions_exact_native_component_sets);
	UT_DONE();
	return ut_failed_count != 0;
}
