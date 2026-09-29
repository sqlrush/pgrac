/*-------------------------------------------------------------------------
 *
 * test_cluster_btree_leaf_version.c
 *    Exact page versions in native btree leaf maintenance WAL.
 *
 * Portions Copyright (c) 1996-2023, PostgreSQL Global Development Group
 * Portions Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *    src/test/cluster_unit/test_cluster_btree_leaf_version.c
 *
 * NOTES
 *    Real native mutation bodies and page/SPACE/version helpers. Buffer and
 *    WAL I/O, the pre-lock identity read and key equality are fixture seams.
 *    Does not qualify distributed VACUUM, split or replay.
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
#include "cluster/storage/cluster_smgr.h"
#include "miscadmin.h"
#include "utils/rel.h"
#include "unit_test.h"

UT_DEFINE_GLOBALS();
bool cluster_enabled = true, cluster_shared_config = true;
int cluster_node_id, wal_level = WAL_LEVEL_REPLICA, NBuffers = 1, NLocBuffer;
char *BufferBlocks;
Block *LocalBufferBlockPointers;
volatile uint32 CritSectionCount;
bool cluster_recmerge_window_active, cluster_recmerge_apply_foreign;
uint64 cluster_recmerge_window_scn, cluster_recmerge_window_own_lsn;

static PGAlignedBlock current_page, before_page, wal_image;
static RelationData relation_data;
static FormData_pg_class relform;
static FormData_pg_index indexform;
static ClusterSpaceIdentity space_identity;
static RfPageVersionEdgeEntryV1 edge;
static unsigned inserts, edges, dirties, identity_reads, releases;
static uint64 next_token, edge_token;
static uint8 expected_info;
static bool begun, expecting_error, content_locked;
static jmp_buf error_jump;

void
ExceptionalCondition(const char *condition, const char *file, int line)
{
	printf("# assertion %s at %s:%d\n", condition, file, line);
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
	if (!expecting_error) {
		printf("# unexpected error %s:%d %s\n", file, line, function);
		abort();
	}
	longjmp(error_jump, 1);
}
void *
palloc(Size bytes)
{
	return malloc(bytes);
}
void *
palloc0(Size bytes)
{
	return calloc(1, bytes);
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
	UT_ASSERT(rel == &relation_data);
	UT_ASSERT(!content_locked && CritSectionCount == 0);
	identity_reads++;
	*out = space_identity;
	return true;
}
void
BufferGetTag(Buffer buffer, RelFileLocator *locator, ForkNumber *forknum, BlockNumber *block)
{
	UT_ASSERT_EQ(buffer, 1);
	*locator = (RelFileLocator){ 1663, 5, 900 };
	*forknum = MAIN_FORKNUM;
	*block = expected_info == XLOG_BTREE_META_CLEANUP ? 0 : 7;
}
bool
BufferIsPermanent(Buffer buffer)
{
	return buffer == 1;
}
BlockNumber
BufferGetBlockNumber(Buffer buffer)
{
	UT_ASSERT_EQ(buffer, 1);
	return expected_info == XLOG_BTREE_META_CLEANUP ? 0 : 7;
}
SCN
cluster_scn_advance(void)
{
	UT_ASSERT(content_locked && CritSectionCount == 0);
	UT_ASSERT(memcmp(current_page.data, before_page.data, BLCKSZ) == 0);
	return ++next_token;
}
Buffer
_bt_getbuf(Relation rel, BlockNumber block, int access)
{
	UT_ASSERT(rel == &relation_data && block == 0 && access == BT_READ);
	UT_ASSERT(!content_locked);
	content_locked = true;
	return 1;
}
void
_bt_unlockbuf(Relation rel, Buffer buffer)
{
	(void)rel;
	UT_ASSERT(buffer == 1 && content_locked);
	content_locked = false;
}
void
_bt_lockbuf(Relation rel, Buffer buffer, int access)
{
	(void)rel;
	UT_ASSERT(buffer == 1 && !content_locked && access == BT_WRITE);
	content_locked = true;
}
void
_bt_relbuf(Relation rel, Buffer buffer)
{
	_bt_unlockbuf(rel, buffer);
	releases++;
}
void
MarkBufferDirty(Buffer buffer)
{
	UT_ASSERT(buffer == 1 && content_locked && CritSectionCount == 1);
	dirties++;
}
void
XLogBeginInsert(void)
{
	UT_ASSERT(!begun && dirties == 1 && CritSectionCount == 1);
	begun = true;
}
void
XLogRegisterBuffer(uint8 id, Buffer buffer, uint8 flags)
{
	UT_ASSERT(begun && id == 0 && buffer == 1);
	UT_ASSERT_EQ(flags, expected_info == XLOG_BTREE_META_CLEANUP
							? REGBUF_WILL_INIT | REGBUF_STANDARD
							: REGBUF_STANDARD);
	memcpy(wal_image.data, current_page.data, BLCKSZ);
}
void
XLogRegisterData(char *data, uint32 length)
{
	UT_ASSERT(begun && data != NULL && length > 0);
}
void
XLogRegisterBufData(uint8 id, char *data, uint32 length)
{
	UT_ASSERT(begun && id == 0 && data != NULL && length > 0);
}
void
XLogRegisterPageVersionEdge(uint64 token, const RfPageVersionEdgeEntryV1 *entries, uint8 count)
{
	UT_ASSERT(begun && count == 1);
	edge = entries[0];
	edge_token = token;
	edges++;
}
XLogRecPtr
XLogInsert(RmgrId rmgr, uint8 info)
{
	UT_ASSERT(begun && rmgr == RM_BTREE_ID && info == expected_info);
	begun = false;
	inserts++;
	return UINT64_C(0x9000);
}

/* Single integer key fixture; native posting-list construction stays real. */
int
_bt_keep_natts_fast(Relation rel, IndexTuple left, IndexTuple right)
{
	(void)rel;
	return *(uint64 *)((char *)left + sizeof(IndexTupleData))
				   == *(uint64 *)((char *)right + sizeof(IndexTupleData))
			   ? 2
			   : 1;
}
static bool _bt_posting_valid(IndexTuple posting);
static bool _bt_do_singleval(Relation rel, Page page, BTDedupState state, OffsetNumber minoff,
							 IndexTuple newitem);
static void _bt_singleval_fillfactor(Page page, BTDedupState state, Size newitemsz);
#define cluster_space_relation_get_identity fixture_get_identity
#include "test_cluster_btree_leaf_helpers.inc"
#undef cluster_space_relation_get_identity
#include "test_cluster_btree_leaf_dedup_helpers.inc"

static void
run_meta(Relation rel, BlockNumber num_delpages)
#include "test_cluster_btree_leaf_meta.inc"
	static void run_vacuum(Relation rel, Buffer buf, OffsetNumber *deletable, int ndeletable,
						   BTVacuumPosting *updatable, int nupdatable,
						   const ClusterSpaceIdentity *identity pg_attribute_unused())
#include "test_cluster_btree_leaf_vacuum.inc"
		static void run_delete(Relation rel, Buffer buf, TransactionId snapshotConflictHorizon,
							   bool isCatalogRel, OffsetNumber *deletable, int ndeletable,
							   BTVacuumPosting *updatable, int nupdatable,
							   const ClusterSpaceIdentity *identity pg_attribute_unused())
#include "test_cluster_btree_leaf_delete.inc"
			static void run_dedup(Relation rel, Buffer buf, IndexTuple newitem, Size newitemsz,
								  bool bottomupdedup,
								  const ClusterSpaceIdentity *identity pg_attribute_unused())
#include "test_cluster_btree_leaf_dedup.inc"

				static void reset(uint8 info, bool shared)
{
	IndexTupleData tuple[2];
	BTPageOpaque opaque;
	cluster_shared_config = shared;
	CritSectionCount = 0;
	inserts = edges = dirties = identity_reads = releases = 0;
	next_token = 200;
	edge_token = 0;
	expected_info = info;
	begun = expecting_error = content_locked = false;
	memset(&relation_data, 0, sizeof(relation_data));
	memset(&relform, 0, sizeof(relform));
	memset(&space_identity, 0, sizeof(space_identity));
	memset(&edge, 0, sizeof(edge));
	relation_data.rd_rel = &relform;
	relation_data.rd_index = &indexform;
	indexform.indnkeyatts = indexform.indnatts = 1;
	relation_data.rd_locator = (RelFileLocator){ 1663, 5, 900 };
	relation_data.rd_backend = InvalidBackendId;
	relform.relpersistence = RELPERSISTENCE_PERMANENT;
	space_identity.key.system_identifier = 11;
	space_identity.key.database_incarnation = 12;
	memset(space_identity.key.storage_uuid, 13, 16);
	space_identity.key.locator = relation_data.rd_locator;
	memset(space_identity.incarnation, 30, 16);
	space_identity.sequence = 1;
	space_identity.operation = 40;
	space_identity.state = CLUSTER_SPACE_IDENTITY_LIVE;
	PageInit(current_page.data, BLCKSZ, sizeof(BTPageOpaqueData));
	opaque = BTPageGetOpaque(current_page.data);
	if (info == XLOG_BTREE_META_CLEANUP) {
		BTMetaPageData *meta = BTPageGetMeta(current_page.data);
		opaque->btpo_flags = BTP_META;
		meta->btm_magic = BTREE_MAGIC;
		meta->btm_version = BTREE_VERSION;
		meta->btm_root = meta->btm_fastroot = 1;
		((PageHeader)current_page.data)->pd_lower
			= (char *)meta + sizeof(BTMetaPageData) - current_page.data;
	} else {
		opaque->btpo_flags = BTP_LEAF | BTP_HAS_GARBAGE;
		opaque->btpo_cycleid = 9;
		memset(tuple, 0, sizeof(tuple));
		tuple[0].t_info = sizeof(tuple);
		*(uint64 *)&tuple[1] = 42;
		for (int n = 1; n <= 3; n++) {
			ItemPointerSet(&tuple[0].t_tid, 2, n);
			UT_ASSERT_EQ(
				PageAddItem(current_page.data, (Item)tuple, sizeof(tuple), n, false, false), n);
		}
		content_locked = true;
	}
	((PageHeader)current_page.data)->pd_block_scn = 77;
	before_page = current_page;
	BufferBlocks = current_page.data;
}
static void
check_version(void)
{
	UT_ASSERT_EQ(edges, 1);
	UT_ASSERT_EQ(edge.before_kind, RF_PAGE_STATE_PRESENT);
	UT_ASSERT_EQ(edge.before.mutation_token, 77);
	UT_ASSERT_EQ(edge_token, 201);
	UT_ASSERT_EQ(edge.block_id, 0);
	UT_ASSERT(memcmp(edge.before.segment_incarnation, space_identity.incarnation, 16) == 0);
	UT_ASSERT_EQ(((PageHeader)current_page.data)->pd_block_scn, 201);
	UT_ASSERT_EQ(((PageHeader)wal_image.data)->pd_block_scn, 201);
	UT_ASSERT_EQ(PageGetLSN(current_page.data), 0x9000);
	UT_ASSERT_EQ(inserts, 1);
	UT_ASSERT_EQ(CritSectionCount, 0);
}
UT_TEST(test_meta_cleanup_publishes_exact_version)
{
	reset(XLOG_BTREE_META_CLEANUP, true);
	run_meta(&relation_data, 3);
	UT_ASSERT_EQ(BTPageGetMeta(current_page.data)->btm_last_cleanup_num_delpages, 3);
	check_version();
	UT_ASSERT_EQ(identity_reads, 1);
	UT_ASSERT_EQ(releases, 1);
}
UT_TEST(test_vacuum_publishes_exact_version)
{
	OffsetNumber remove = 2;
	reset(XLOG_BTREE_VACUUM, true);
	run_vacuum(&relation_data, 1, &remove, 1, NULL, 0, &space_identity);
	UT_ASSERT_EQ(PageGetMaxOffsetNumber(current_page.data), 2);
	UT_ASSERT_EQ(BTPageGetOpaque(current_page.data)->btpo_cycleid, 0);
	check_version();
}
UT_TEST(test_delete_publishes_exact_version_preserves_cycle)
{
	OffsetNumber remove = 2;
	reset(XLOG_BTREE_DELETE, true);
	run_delete(&relation_data, 1, 23, false, &remove, 1, NULL, 0, &space_identity);
	UT_ASSERT_EQ(PageGetMaxOffsetNumber(current_page.data), 2);
	UT_ASSERT_EQ(BTPageGetOpaque(current_page.data)->btpo_cycleid, 9);
	check_version();
}
UT_TEST(test_dedup_restoration_preserves_before_then_result)
{
	IndexTuple item;
	reset(XLOG_BTREE_DEDUP, true);
	item = (IndexTuple)PageGetItem(current_page.data, PageGetItemId(current_page.data, 1));
	run_dedup(&relation_data, 1, item, 16, true, &space_identity);
	UT_ASSERT_EQ(PageGetMaxOffsetNumber(current_page.data), 1);
	item = (IndexTuple)PageGetItem(current_page.data, PageGetItemId(current_page.data, 1));
	UT_ASSERT_EQ(BTreeTupleGetNPosting(item), 3);
	check_version();
}

static void
run_kind(uint8 info, const ClusterSpaceIdentity *identity)
{
	OffsetNumber remove = 2;
	IndexTuple item;

	if (info == XLOG_BTREE_META_CLEANUP)
		run_meta(&relation_data, 3);
	else if (info == XLOG_BTREE_VACUUM)
		run_vacuum(&relation_data, 1, &remove, 1, NULL, 0, identity);
	else if (info == XLOG_BTREE_DELETE)
		run_delete(&relation_data, 1, 23, false, &remove, 1, NULL, 0, identity);
	else {
		item = (IndexTuple)PageGetItem(current_page.data, PageGetItemId(current_page.data, 1));
		run_dedup(&relation_data, 1, item, 16, true, identity);
	}
}

UT_TEST(test_bad_identity_or_page_never_changes_bytes)
{
	const uint8 kinds[]
		= { XLOG_BTREE_META_CLEANUP, XLOG_BTREE_VACUUM, XLOG_BTREE_DELETE, XLOG_BTREE_DEDUP };

	for (int k = 0; k < lengthof(kinds); k++) {
		for (int variant = 0; variant < 7; variant++) {
			PGAlignedBlock before;
			reset(kinds[k], true);
			switch (variant) {
			case 0:
				space_identity.key.database_incarnation++;
				break;
			case 1:
				memset(space_identity.incarnation, 0, 16);
				break;
			case 2:
				space_identity.state = CLUSTER_SPACE_IDENTITY_TOMBSTONED;
				break;
			case 3:
				space_identity.key.locator.relNumber++;
				break;
			case 4:
				((PageHeader)current_page.data)->pd_block_scn = 0;
				break;
			case 5:
				((PageHeader)current_page.data)->pd_flags |= PD_SPACE_METADATA;
				break;
			case 6:
				((PageHeader)current_page.data)->pd_flags |= PD_UNDO_SEG_HEADER;
				break;
			}
			before = current_page;
			expecting_error = true;
			if (setjmp(error_jump) == 0) {
				run_kind(kinds[k], &space_identity);
				UT_ASSERT(false);
			}
			expecting_error = false;
			UT_ASSERT(memcmp(before.data, current_page.data, BLCKSZ) == 0);
			UT_ASSERT_EQ(edges + inserts + dirties, 0);
			UT_ASSERT_EQ(CritSectionCount, 0);
			UT_ASSERT_EQ(next_token, 200);
		}
	}
}
UT_TEST(test_nonshared_keeps_native_behavior)
{
	const uint8 kinds[]
		= { XLOG_BTREE_META_CLEANUP, XLOG_BTREE_VACUUM, XLOG_BTREE_DELETE, XLOG_BTREE_DEDUP };
	for (int k = 0; k < lengthof(kinds); k++) {
		reset(kinds[k], false);
		run_kind(kinds[k], NULL);
		UT_ASSERT_EQ(edges + identity_reads, 0);
		UT_ASSERT_EQ(inserts, 1);
		UT_ASSERT_EQ(next_token, 200);
		UT_ASSERT_EQ(CritSectionCount, 0);
	}
}
UT_TEST(test_noop_allocates_no_token_or_wal)
{
	IndexTuple item;
	reset(XLOG_BTREE_META_CLEANUP, true);
	run_meta(&relation_data, 0);
	UT_ASSERT(memcmp(before_page.data, current_page.data, BLCKSZ) == 0);
	UT_ASSERT_EQ(edges + inserts + dirties, 0);
	UT_ASSERT_EQ(next_token, 200);

	reset(XLOG_BTREE_DEDUP, true);
	for (int n = 1; n <= 3; n++) {
		item = (IndexTuple)PageGetItem(current_page.data, PageGetItemId(current_page.data, n));
		*(uint64 *)((char *)item + sizeof(IndexTupleData)) = n;
	}
	before_page = current_page;
	item = (IndexTuple)PageGetItem(current_page.data, PageGetItemId(current_page.data, 1));
	run_dedup(&relation_data, 1, item, 16, true, &space_identity);
	UT_ASSERT(memcmp(before_page.data, current_page.data, BLCKSZ) == 0);
	UT_ASSERT_EQ(edges + inserts + dirties, 0);
	UT_ASSERT_EQ(next_token, 200);
}
UT_TEST(test_posting_updates_keep_native_tid_and_wal_semantics)
{
	const uint8 kinds[] = { XLOG_BTREE_VACUUM, XLOG_BTREE_DELETE };
	for (int k = 0; k < lengthof(kinds); k++) {
		ItemPointerData tids[3];
		IndexTupleData base[2];
		IndexTuple posting, remaining;
		BTVacuumPosting update;

		reset(kinds[k], true);
		memset(base, 0, sizeof(base));
		base[0].t_info = sizeof(base);
		for (int n = 0; n < 3; n++)
			ItemPointerSet(&tids[n], 2, n + 1);
		posting = _bt_form_posting(&base[0], tids, 3);
		PageInit(current_page.data, BLCKSZ, sizeof(BTPageOpaqueData));
		BTPageGetOpaque(current_page.data)->btpo_flags = BTP_LEAF;
		UT_ASSERT_EQ(
			PageAddItem(current_page.data, (Item)posting, IndexTupleSize(posting), 1, false, false),
			1);
		pfree(posting);
		((PageHeader)current_page.data)->pd_block_scn = 77;
		before_page = current_page;
		update = palloc0(sizeof(BTVacuumPostingData) + sizeof(uint16));
		update->itup
			= (IndexTuple)PageGetItem(current_page.data, PageGetItemId(current_page.data, 1));
		update->updatedoffset = 1;
		update->ndeletedtids = 1;
		update->deletetids[0] = 1;
		if (k == 0)
			run_vacuum(&relation_data, 1, NULL, 0, &update, 1, &space_identity);
		else
			run_delete(&relation_data, 1, 23, false, NULL, 0, &update, 1, &space_identity);
		remaining = (IndexTuple)PageGetItem(current_page.data, PageGetItemId(current_page.data, 1));
		UT_ASSERT_EQ(BTreeTupleGetNPosting(remaining), 2);
		UT_ASSERT_EQ(ItemPointerGetOffsetNumber(BTreeTupleGetPostingN(remaining, 0)), 1);
		UT_ASSERT_EQ(ItemPointerGetOffsetNumber(BTreeTupleGetPostingN(remaining, 1)), 3);
		check_version();
		pfree(update);
	}
}
int
main(void)
{
	UT_PLAN(8);
	UT_RUN(test_meta_cleanup_publishes_exact_version);
	UT_RUN(test_vacuum_publishes_exact_version);
	UT_RUN(test_delete_publishes_exact_version_preserves_cycle);
	UT_RUN(test_dedup_restoration_preserves_before_then_result);
	UT_RUN(test_bad_identity_or_page_never_changes_bytes);
	UT_RUN(test_nonshared_keeps_native_behavior);
	UT_RUN(test_noop_allocates_no_token_or_wal);
	UT_RUN(test_posting_updates_keep_native_tid_and_wal_semantics);
	UT_DONE();
	return ut_failed_count != 0;
}
