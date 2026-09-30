/*-------------------------------------------------------------------------
 *
 * test_cluster_heap_inplace_version.c
 *    Native inplace WAL publishes a private version before visible bytes.
 *
 * Portions Copyright (c) 1996-2023, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *    src/test/cluster_unit/test_cluster_heap_inplace_version.c
 *
 * NOTES
 *    PGRAC test of actual heapam bodies and native version helpers. Buffer,
 *    WAL insertion and invalidation I/O are external seams, not recovery.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <setjmp.h>

#include "access/heapam.h"
#include "access/heapam_xlog.h"
#include "access/htup_details.h"
#include "access/xlog.h"
#include "access/xloginsert.h"
#include "catalog/pg_class.h"
#include "cluster/cluster_guc.h"
#include "cluster/cluster_space_storage.h"
#include "cluster/cluster_wal_thread.h"
#include "cluster/storage/cluster_smgr.h"
#include "miscadmin.h"
#include "storage/bufmgr.h"
#include "storage/lmgr.h"
#include "storage/proc.h"
#include "utils/inval.h"
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
ProcessingMode Mode = NormalProcessing;
static PGPROC process;
PGPROC *MyProc = &process;

static PGAlignedBlock current_page, before_page, wal_image, new_tuple_bytes;
static RelationData relation;
static FormData_pg_class relform;
static HeapTupleData old_tuple, new_tuple;
static ClusterSpaceIdentity identity;
static RfPageVersionEdgeEntryV1 edge;
static uint64 next_token, wal_token;
static unsigned edges, inserts, dirties, invals, events;
static bool locked, tuple_locked, begun, expecting_error, legacy;
static unsigned identity_reads;
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
		printf("# unexpected ERROR %s:%d %s\n", file, line, function);
		abort();
	}
	longjmp(error_jump, 1);
}
bool
RecoveryInProgress(void)
{
	return false;
}
bool
cluster_wal_thread_current_v2_ref(ClusterWalSourceRef *out)
{
	memset(out, 0, sizeof(*out));
	out->claim.identity.system_identifier = 11;
	out->claim.database_incarnation = 12;
	memset(out->claim.identity.storage_uuid, 13, 16);
	return true;
}
bool
cluster_wal_thread_restart_v2_ref(ClusterWalSourceRef *out)
{
	(void)out;
	abort();
}
int
cluster_smgr_which_for(RelFileLocator locator, BackendId backend)
{
	if (locator.dbOid != 5 || backend != InvalidBackendId)
		abort();
	return 1;
}
void
BufferGetTag(Buffer buffer, RelFileLocator *locator, ForkNumber *forknum, BlockNumber *block)
{
	if (buffer != 1)
		abort();
	*locator = (RelFileLocator){ 1663, 5, 900 };
	*forknum = MAIN_FORKNUM;
	*block = 7;
}
bool
BufferIsPermanent(Buffer buffer)
{
	return buffer == 1;
}
SCN
cluster_scn_advance(void)
{
	UT_ASSERT(locked && (legacy || tuple_locked));
	UT_ASSERT_EQ(CritSectionCount, 0);
	UT_ASSERT(memcmp(current_page.data, before_page.data, BLCKSZ) == 0);
	return ++next_token;
}
void
PreInplace_Inval(void)
{
	UT_ASSERT(locked && tuple_locked);
	UT_ASSERT_EQ(events++, 0);
}
void
XLogBeginInsert(void)
{
	UT_ASSERT(locked && (legacy || tuple_locked) && !begun);
	UT_ASSERT_EQ(CritSectionCount, 1);
	UT_ASSERT_EQ(MyProc->delayChkptFlags, legacy ? 0 : DELAY_CHKPT_START);
	begun = true;
}
void
XLogRegisterData(char *data, uint32 length)
{
	UT_ASSERT_EQ(length, SizeOfHeapInplace);
	UT_ASSERT_EQ(((xl_heap_inplace *)data)->offnum, 1);
}
void
XLogRegisterBlock(uint8 id, RelFileLocator *locator, ForkNumber forknum, BlockNumber block,
				  Page page, uint8 flags)
{
	UT_ASSERT(begun && locked);
	UT_ASSERT_EQ(id, 0);
	UT_ASSERT_EQ(forknum, MAIN_FORKNUM);
	UT_ASSERT_EQ(block, 7);
	UT_ASSERT_EQ(locator->relNumber, 900);
	UT_ASSERT_EQ(flags, REGBUF_STANDARD);
	UT_ASSERT(page != current_page.data);
	memcpy(wal_image.data, page, BLCKSZ);
}
void
XLogRegisterBufData(uint8 id, char *data, uint32 length)
{
	UT_ASSERT_EQ(id, 0);
	UT_ASSERT_EQ(length, sizeof(int));
	UT_ASSERT_EQ(*(int *)data, 33);
}
void
XLogRegisterPageVersionEdge(uint64 token, const RfPageVersionEdgeEntryV1 *entries, uint8 count)
{
	UT_ASSERT(begun);
	UT_ASSERT_EQ(count, 1);
	edge = entries[0];
	wal_token = token;
	edges++;
}
XLogRecPtr
XLogInsert(RmgrId rmgr, uint8 info)
{
	UT_ASSERT(begun);
	UT_ASSERT_EQ(rmgr, RM_HEAP_ID);
	UT_ASSERT_EQ(info, XLOG_HEAP_INPLACE);
	UT_ASSERT_EQ(dirties, legacy ? 1 : 0);
	if (!legacy) {
		UT_ASSERT(memcmp(current_page.data, before_page.data, BLCKSZ) == 0);
		UT_ASSERT_EQ(events++, 1);
	}
	begun = false;
	inserts++;
	return 9000;
}
void
MarkBufferDirty(Buffer buffer)
{
	UT_ASSERT_EQ(buffer, 1);
	UT_ASSERT(locked && (legacy || tuple_locked));
	UT_ASSERT_EQ(inserts, legacy ? 0 : 1);
	if (!legacy)
		UT_ASSERT_EQ(events++, 2);
	dirties++;
}
void
LockBuffer(Buffer buffer, int mode)
{
	UT_ASSERT_EQ(buffer, 1);
	if (legacy && mode == BUFFER_LOCK_EXCLUSIVE) {
		UT_ASSERT(!locked);
		locked = true;
		return;
	}
	UT_ASSERT_EQ(mode, BUFFER_LOCK_UNLOCK);
	UT_ASSERT(locked && tuple_locked);
	UT_ASSERT_EQ(events++, 3);
	locked = false;
}
void
AtInplace_Inval(void)
{
	UT_ASSERT(!locked && tuple_locked);
	UT_ASSERT_EQ(CritSectionCount, 1);
	UT_ASSERT_EQ(MyProc->delayChkptFlags, DELAY_CHKPT_START);
	UT_ASSERT_EQ(events++, 4);
	invals++;
}
void
UnlockTuple(Relation rel, ItemPointer tid, LOCKMODE mode)
{
	UT_ASSERT(rel == &relation && tuple_locked && !locked);
	UT_ASSERT_EQ(CritSectionCount, 0);
	UT_ASSERT_EQ(MyProc->delayChkptFlags, 0);
	UT_ASSERT_EQ(events++, 5);
	tuple_locked = false;
}
void
AcceptInvalidationMessages(void)
{
	UT_ASSERT(!locked && !tuple_locked);
	UT_ASSERT_EQ(events++, 6);
}
void
CacheInvalidateHeapTuple(Relation rel, HeapTuple tuple, HeapTuple next)
{
	UT_ASSERT(rel == &relation && tuple == &new_tuple && next == NULL);
	if (!legacy)
		UT_ASSERT_EQ(events++, 7);
}

#include "test_cluster_heap_inplace_version.inc"

bool
IsInParallelMode(void)
{
	return false;
}
Buffer
ReadBuffer(Relation rel, BlockNumber block)
{
	UT_ASSERT(rel == &relation && !locked);
	UT_ASSERT_EQ(block, 7);
	return 1;
}
void
UnlockReleaseBuffer(Buffer buffer)
{
	UT_ASSERT_EQ(buffer, 1);
	UT_ASSERT(legacy && locked);
	UT_ASSERT_EQ(CritSectionCount, 0);
	locked = false;
}
void
XLogRegisterBuffer(uint8 id, Buffer buffer, uint8 flags)
{
	UT_ASSERT(legacy && begun && locked);
	UT_ASSERT_EQ(id, 0);
	UT_ASSERT_EQ(buffer, 1);
	UT_ASSERT_EQ(flags, REGBUF_STANDARD);
	memcpy(wal_image.data, current_page.data, BLCKSZ);
}
/* SPACE cache I/O is tested separately. The native entry must call it before
 * reading or locking the target; version capture/stamp are real objects. */
static bool
legacy_get_identity(Relation rel, ClusterSpaceIdentity *out)
{
	UT_ASSERT(rel == &relation && !locked);
	identity_reads++;
	*out = identity;
	return true;
}
#define cluster_space_relation_get_identity legacy_get_identity
#include "test_cluster_heap_inplace_legacy_version.inc"
#undef cluster_space_relation_get_identity

static void
reset(void)
{
	HeapTupleHeader tuple = (HeapTupleHeader)new_tuple_bytes.data;
	memset(&relation, 0, sizeof(relation));
	memset(&relform, 0, sizeof(relform));
	memset(&identity, 0, sizeof(identity));
	memset(&process, 0, sizeof(process));
	memset(&edge, 0, sizeof(edge));
	memset(new_tuple_bytes.data, 0, BLCKSZ);
	BufferBlocks = current_page.data;
	cluster_shared_config = true;
	CritSectionCount = 0;
	edges = inserts = dirties = invals = events = 0;
	begun = expecting_error = false;
	legacy = false;
	identity_reads = 0;
	locked = tuple_locked = true;
	next_token = 100;
	wal_token = 0;
	relation.rd_rel = &relform;
	relation.rd_locator = (RelFileLocator){ 1663, 5, 900 };
	relform.relpersistence = RELPERSISTENCE_PERMANENT;
	identity.key.system_identifier = 11;
	identity.key.database_incarnation = 12;
	memset(identity.key.storage_uuid, 13, 16);
	identity.key.locator = relation.rd_locator;
	memset(identity.incarnation, 30, 16);
	identity.sequence = 1;
	identity.operation = 40;
	identity.state = CLUSTER_SPACE_IDENTITY_LIVE;
	PageInit(current_page.data, BLCKSZ, 0);
	((PageHeader)current_page.data)->pd_block_scn = 20;
	tuple->t_hoff = MAXALIGN(SizeofHeapTupleHeader);
	HeapTupleHeaderSetXmin(tuple, 77);
	*(int *)(new_tuple_bytes.data + tuple->t_hoff) = 11;
	new_tuple.t_len = tuple->t_hoff + sizeof(int);
	new_tuple.t_data = tuple;
	ItemPointerSet(&new_tuple.t_self, 7, 1);
	UT_ASSERT_EQ(PageAddItem(current_page.data, (Item)tuple, new_tuple.t_len, 1, false, true), 1);
	old_tuple = new_tuple;
	old_tuple.t_data
		= (HeapTupleHeader)PageGetItem(current_page.data, PageGetItemId(current_page.data, 1));
	*(int *)(new_tuple_bytes.data + tuple->t_hoff) = 33;
	memcpy(before_page.data, current_page.data, BLCKSZ);
}
static void
finish(void)
{
	heap_inplace_update_and_unlock(&relation, &old_tuple, &new_tuple, 1, &identity);
}
UT_TEST(test_wal_first_private_version)
{
	reset();
	finish();
	UT_ASSERT_EQ(edges, 1);
	UT_ASSERT_EQ(wal_token, 101);
	UT_ASSERT_EQ(edge.before.mutation_token, 20);
	UT_ASSERT_EQ(((PageHeader)wal_image.data)->pd_block_scn, 101);
	UT_ASSERT_EQ(*(int *)(PageGetItem(wal_image.data, PageGetItemId(wal_image.data, 1))
						  + old_tuple.t_data->t_hoff),
				 33);
	UT_ASSERT_EQ(edge.block_id, 0);
	UT_ASSERT_EQ(edge.before_kind, RF_PAGE_STATE_PRESENT);
	UT_ASSERT_EQ(edge.result_kind, RF_PAGE_STATE_PRESENT);
	UT_ASSERT(memcmp(edge.before.segment_incarnation, identity.incarnation, 16) == 0);
	UT_ASSERT(memcmp(edge.result_incarnation, identity.incarnation, 16) == 0);
	UT_ASSERT_EQ(((PageHeader)current_page.data)->pd_block_scn, 101);
	UT_ASSERT_EQ(*(int *)((char *)old_tuple.t_data + old_tuple.t_data->t_hoff), 33);
	UT_ASSERT_EQ(events, 8);
	UT_ASSERT_EQ(invals, 1);
}
UT_TEST(test_native_nonshared_unchanged)
{
	reset();
	cluster_shared_config = false;
	finish();
	UT_ASSERT_EQ(edges, 0);
	UT_ASSERT_EQ(((PageHeader)current_page.data)->pd_block_scn, 20);
	UT_ASSERT_EQ(events, 8);
}
UT_TEST(test_legacy_inplace_version)
{
	reset();
	legacy = true;
	locked = tuple_locked = false;
	heap_inplace_update(&relation, &new_tuple);
	UT_ASSERT_EQ(edges, 1);
	UT_ASSERT_EQ(wal_token, 101);
	UT_ASSERT_EQ(edge.before.mutation_token, 20);
	UT_ASSERT_EQ(identity_reads, 1);
	UT_ASSERT_EQ(((PageHeader)wal_image.data)->pd_block_scn, 101);
	UT_ASSERT_EQ(((PageHeader)current_page.data)->pd_block_scn, 101);
	UT_ASSERT_EQ(*(int *)((char *)old_tuple.t_data + old_tuple.t_data->t_hoff), 33);
	UT_ASSERT(!locked);
}
UT_TEST(test_invalid_identity_or_tuple_refuses_before_publication)
{
	int fault;
	for (fault = 0; fault < 6; fault++) {
		volatile bool caught = false;
		reset();
		switch (fault) {
		case 0:
			identity.key.database_incarnation++;
			break;
		case 1:
			identity.key.locator.relNumber++;
			break;
		case 2:
			identity.state = CLUSTER_SPACE_IDENTITY_TOMBSTONED;
			break;
		case 3:
			((PageHeader)current_page.data)->pd_block_scn = 0;
			memcpy(before_page.data, current_page.data, BLCKSZ);
			break;
		case 4:
			new_tuple.t_len++;
			break;
		case 5:
			break;
		}
		expecting_error = true;
		if (setjmp(error_jump) == 0)
			heap_inplace_update_and_unlock(&relation, &old_tuple, &new_tuple, 1,
										   fault == 5 ? NULL : &identity);
		else
			caught = true;
		expecting_error = false;
		UT_ASSERT(caught);
		UT_ASSERT_EQ(CritSectionCount, 0);
		UT_ASSERT_EQ(events, 0);
		UT_ASSERT_EQ(inserts, 0);
		UT_ASSERT_EQ(dirties, 0);
		UT_ASSERT_EQ(next_token, 100);
		UT_ASSERT(memcmp(current_page.data, before_page.data, BLCKSZ) == 0);
	}
}
UT_TEST(test_legacy_nonshared_unchanged)
{
	reset();
	cluster_shared_config = false;
	legacy = true;
	locked = tuple_locked = false;
	heap_inplace_update(&relation, &new_tuple);
	UT_ASSERT_EQ(edges, 0);
	UT_ASSERT_EQ(identity_reads, 0);
	UT_ASSERT_EQ(((PageHeader)current_page.data)->pd_block_scn, 20);
	UT_ASSERT_EQ(inserts, 1);
}
int
main(void)
{
	UT_PLAN(5);
	UT_RUN(test_wal_first_private_version);
	UT_RUN(test_native_nonshared_unchanged);
	UT_RUN(test_legacy_inplace_version);
	UT_RUN(test_invalid_identity_or_tuple_refuses_before_publication);
	UT_RUN(test_legacy_nonshared_unchanged);
	UT_DONE();
	return ut_failed_count != 0;
}
