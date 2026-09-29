/*-------------------------------------------------------------------------
 *
 * test_cluster_sequence_version.c
 *    Native sequence page mutation and WAL version publication.
 *
 * Portions Copyright (c) 1996-2023, PostgreSQL Global Development Group
 * Portions Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *    src/test/cluster_unit/test_cluster_sequence_version.c
 *
 * NOTES
 *    Actual native producer bodies, page/SPACE/version and allocation math.
 *    Catalog, buffer ownership and WAL I/O are explicit fixtures. This is
 *    not distributed sequence-cache or recovery qualification.
 *-------------------------------------------------------------------------
 */
#include "postgres.h"
#include <setjmp.h>
#include "access/htup_details.h"
#include "access/relation.h"
#include "access/transam.h"
#include "access/xact.h"
#include "access/xlog.h"
#include "access/xloginsert.h"
#include "access/xlogutils.h"
#include "catalog/pg_sequence.h"
#include "cluster/cluster_guc.h"
#include "cluster/cluster_block_apply.h"
#include "cluster/cluster_mode.h"
#include "cluster/cluster_pcm_lock.h"
#include "cluster/cluster_sequence.h"
#include "cluster/cluster_space_storage.h"
#include "cluster/cluster_wal_thread.h"
#include "cluster/storage/cluster_smgr.h"
#include "commands/sequence.h"
#include "miscadmin.h"
#include "storage/lmgr.h"
#include "utils/acl.h"
#include "utils/memutils.h"
#include "utils/rel.h"
#include "utils/syscache.h"
#include "unit_test.h"

UT_DEFINE_GLOBALS();
bool cluster_enabled = true, cluster_shared_config = true;
int cluster_node_id, wal_level = WAL_LEVEL_REPLICA, NBuffers = 1, NLocBuffer;
char *BufferBlocks;
Block *LocalBufferBlockPointers;
volatile uint32 CritSectionCount;
bool cluster_recmerge_window_active, cluster_recmerge_apply_foreign;
uint64 cluster_recmerge_window_scn, cluster_recmerge_window_own_lsn;

#define SEQ_LOG_VALS 32
typedef struct SeqTableData {
	Oid relid;
	RelFileNumber filenumber;
	LocalTransactionId lxid;
	bool last_valid;
	int64 last, cached, increment;
} SeqTableData;
typedef SeqTableData *SeqTable;
typedef enum ClusterSqDisposition {
	CLSQ_NATIVE,
	CLSQ_MANAGED,
	CLSQ_UNSUPPORTED
} ClusterSqDisposition;
static SeqTable last_used_seq;

static PGAlignedBlock page_data, before, tuple_data, params_data;
static PGAlignedBlock wal_payload;
static Size wal_payload_len;
static bool replaying;
static union {
	DecodedXLogRecord decoded;
	char bytes[sizeof(DecodedXLogRecord) + sizeof(DecodedBkpBlock)];
} record_space;
static XLogReaderState reader;
static RelationData relation_data;
static FormData_pg_class relform;
static HeapTupleData tuple, params_tuple;
static SeqTableData cache_data;
static ClusterSpaceIdentity identity;
static RfPageVersionEdgeEntryV1 edge;
static xl_seq_rec wal_header;
static FormData_pg_sequence_data wal_tuple;
static unsigned wal_count, edge_count, data_count, dirties, flushes, identity_reads;
static bool locked, begun, pcm_active, error_expected, identity_valid;
static uint64 token;
static jmp_buf error_jump;

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
int
errhint(const char *format, ...)
{
	(void)format;
	return 0;
}
int
errdetail(const char *format, ...)
{
	(void)format;
	return 0;
}
void
errfinish(const char *file, int line, const char *fn)
{
	if (!error_expected) {
		printf("# unexpected error %s:%d %s\n", file, line, fn);
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
pfree(void *ptr)
{
	free(ptr);
}
bool
RecoveryInProgress(void)
{
	return false;
}
static bool
fixture_pcm_active(void)
{
	return pcm_active;
}
int
cluster_smgr_which_for(RelFileLocator locator, BackendId backend)
{
	UT_ASSERT_EQ(locator.dbOid, 5);
	UT_ASSERT_EQ(backend, InvalidBackendId);
	return 1;
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
static bool
fixture_identity(Relation rel, ClusterSpaceIdentity *out)
{
	UT_ASSERT(rel == &relation_data && !locked);
	identity_reads++;
	if (!identity_valid)
		return false;
	*out = identity;
	return true;
}
void
BufferGetTag(Buffer buffer, RelFileLocator *locator, ForkNumber *fork, BlockNumber *block)
{
	UT_ASSERT_EQ(buffer, 1);
	*locator = relation_data.rd_locator;
	*fork = MAIN_FORKNUM;
	*block = 0;
}
bool
BufferIsPermanent(Buffer buffer)
{
	UT_ASSERT_EQ(buffer, 1);
	return true;
}
BlockNumber
BufferGetBlockNumber(Buffer buffer)
{
	UT_ASSERT_EQ(buffer, 1);
	return 0;
}
SCN
cluster_scn_advance(void)
{
	UT_ASSERT_EQ(CritSectionCount, 0);
	if (cluster_shared_config)
		UT_ASSERT(memcmp(page_data.data, before.data, BLCKSZ) == 0);
	return ++token;
}
static void
init_sequence(Oid relid, SeqTable *elm, Relation *rel)
{
	UT_ASSERT_EQ(relid, 20000);
	*elm = &cache_data;
	*rel = &relation_data;
}
static Form_pg_sequence_data
read_seq_tuple(Relation rel, Buffer *buffer, HeapTuple out)
{
	UT_ASSERT(rel == &relation_data && !locked);
	if (cluster_shared_config)
		UT_ASSERT_EQ(identity_reads, 1);
	locked = true;
	*buffer = 1;
	out->t_data = (HeapTupleHeader)PageGetItem(page_data.data, PageGetItemId(page_data.data, 1));
	out->t_len = ItemIdGetLength(PageGetItemId(page_data.data, 1));
	return (Form_pg_sequence_data)GETSTRUCT(out);
}
static ClusterSqDisposition
cluster_sq_classify(Relation rel, ClusterResId *resid)
{
	(void)rel;
	(void)resid;
	return CLSQ_NATIVE;
}
static int64
cluster_sq_nextval(Relation rel, const ClusterResId *resid, int64 i, int64 min, int64 max,
				   int64 cache)
{
	(void)rel;
	(void)resid;
	(void)i;
	(void)min;
	(void)max;
	(void)cache;
	abort();
}
static void
cluster_seq_flush_if_shared(Relation rel, Buffer buffer)
{
	(void)rel;
	UT_ASSERT(buffer == 1 && locked && CritSectionCount == 0);
	flushes++;
}
static void
cluster_sq_invalidate_if_managed(Relation rel)
{
	(void)rel;
}
void
cluster_sq_bump_page_writeback(void)
{}
void
cluster_sq_bump_dup_guard_fail(void)
{
	abort();
}
void
cluster_sq_bump_cycle_rejected(void)
{
	abort();
}
HeapTuple
SearchSysCache1(int cacheId, Datum key)
{
	UT_ASSERT(cacheId == SEQRELID && key == 20000);
	return &params_tuple;
}
void
ReleaseSysCache(HeapTuple tup)
{
	UT_ASSERT(tup == &params_tuple);
}
void
PreventCommandIfReadOnly(const char *name)
{
	(void)name;
}
void
PreventCommandIfParallelMode(const char *name)
{
	(void)name;
}
Oid
GetUserId(void)
{
	return 10;
}
AclResult
pg_class_aclcheck(Oid table, Oid user, AclMode mode)
{
	(void)table;
	(void)user;
	(void)mode;
	return ACLCHECK_OK;
}
void
relation_close(Relation rel, LOCKMODE mode)
{
	(void)rel;
	UT_ASSERT_EQ(mode, NoLock);
}
TransactionId
GetTopTransactionId(void)
{
	UT_ASSERT_EQ(CritSectionCount, 0);
	return 3;
}
XLogRecPtr
GetRedoRecPtr(void)
{
	return 0x100;
}
BlockNumber
RelationGetNumberOfBlocksInFork(Relation rel, ForkNumber fork)
{
	(void)rel;
	(void)fork;
	return 0;
}
Buffer
ReadBufferExtended(Relation rel, ForkNumber fork, BlockNumber block, ReadBufferMode mode,
				   BufferAccessStrategy strategy)
{
	(void)rel;
	(void)fork;
	(void)block;
	(void)mode;
	(void)strategy;
	abort();
}
Buffer
ExtendBufferedRel(BufferManagerRelation bmr, ForkNumber fork, BufferAccessStrategy strategy,
				  uint32 flags)
{
	(void)bmr;
	(void)flags;
	UT_ASSERT(fork == MAIN_FORKNUM && strategy == NULL && !locked);
	if (cluster_shared_config)
		UT_ASSERT_EQ(identity_reads, 1);
	locked = true;
	return 1;
}
void
LockBuffer(Buffer buffer, int mode)
{
	(void)buffer;
	(void)mode;
	abort();
}
void
ReleaseBuffer(Buffer buffer)
{
	(void)buffer;
	abort();
}
void
UnlockReleaseBuffer(Buffer buffer)
{
	UT_ASSERT(buffer == 1 && locked);
	locked = false;
}
void
MarkBufferDirty(Buffer buffer)
{
	UT_ASSERT(buffer == 1 && locked && CritSectionCount == (replaying ? 0 : 1));
	dirties++;
}
void
XLogBeginInsert(void)
{
	UT_ASSERT(!begun);
	begun = true;
	data_count = 0;
	wal_payload_len = 0;
}
void
XLogRegisterBuffer(uint8 id, Buffer buffer, uint8 flags)
{
	UT_ASSERT(begun && locked && id == 0 && buffer == 1 && flags == REGBUF_WILL_INIT);
}
void
XLogRegisterData(char *data, uint32 length)
{
	UT_ASSERT(begun);
	Assert(wal_payload_len + length < BLCKSZ);
	memcpy(wal_payload.data + wal_payload_len, data, length);
	wal_payload_len += length;
	if (data_count++ == 0) {
		UT_ASSERT_EQ(length, sizeof(wal_header));
		memcpy(&wal_header, data, length);
	} else {
		HeapTupleHeader hdr = (HeapTupleHeader)data;
		Size size = offsetof(FormData_pg_sequence_data, is_called) + sizeof(bool);
		UT_ASSERT(length >= hdr->t_hoff + size);
		memset(&wal_tuple, 0, sizeof(wal_tuple));
		memcpy(&wal_tuple, data + hdr->t_hoff, size);
	}
}
Buffer
XLogInitBufferForRedo(XLogReaderState *record, uint8 block_id)
{
	UT_ASSERT(replaying && record == &reader && block_id == 0 && !locked);
	locked = true;
	return 1;
}
bool
RestoreBlockImage(XLogReaderState *record, uint8 block_id, char *page)
{
	(void)record;
	(void)block_id;
	(void)page;
	abort();
}
char *
XLogRecGetBlockData(XLogReaderState *record, uint8 block_id, Size *length)
{
	(void)record;
	(void)block_id;
	(void)length;
	abort();
}
ClusterBlkApplyResult
cluster_block_apply_heap(XLogReaderState *record, uint8 block_id, char *page)
{
	(void)record;
	(void)block_id;
	(void)page;
	abort();
}
void
XLogRegisterPageVersionEdge(uint64 result, const RfPageVersionEdgeEntryV1 *entries, uint8 count)
{
	UT_ASSERT(begun && result == 201 && count == 1);
	edge = entries[0];
	edge_count++;
}
XLogRecPtr
XLogInsert(RmgrId rmgr, uint8 info)
{
	UT_ASSERT(begun && rmgr == RM_SEQ_ID && info == XLOG_SEQ_LOG && CritSectionCount == 1);
	begun = false;
	wal_count++;
	return 0x9000;
}

#define cluster_space_relation_get_identity fixture_identity
#define cluster_pcm_is_active fixture_pcm_active
#include "test_cluster_sequence_version_helpers.inc"
#include "test_cluster_sequence_version_native.inc"
#undef cluster_space_relation_get_identity

static Form_pg_sequence_data
page_tuple(void)
{
	HeapTupleData out;
	out.t_data = (HeapTupleHeader)PageGetItem(page_data.data, PageGetItemId(page_data.data, 1));
	return (Form_pg_sequence_data)GETSTRUCT(&out);
}
static void
reset(bool fresh)
{
	Form_pg_sequence params;
	Form_pg_sequence_data data;
	memset(&relation_data, 0, sizeof(relation_data));
	memset(&relform, 0, sizeof(relform));
	relation_data.rd_rel = &relform;
	relation_data.rd_id = 20000;
	relation_data.rd_locator = (RelFileLocator){ 1663, 5, 20000 };
	relform.relkind = RELKIND_SEQUENCE;
	relform.relpersistence = RELPERSISTENCE_PERMANENT;
	memset(&identity, 0, sizeof(identity));
	identity.key.system_identifier = 11;
	identity.key.database_incarnation = 12;
	memset(identity.key.storage_uuid, 13, 16);
	identity.key.locator = relation_data.rd_locator;
	memset(identity.incarnation, 17, 16);
	identity.sequence = 1;
	identity.operation = 1;
	identity.state = CLUSTER_SPACE_IDENTITY_LIVE;
	memset(&tuple_data, 0, sizeof(tuple_data));
	tuple.t_data = (HeapTupleHeader)tuple_data.data;
	tuple.t_data->t_hoff = MAXALIGN(SizeofHeapTupleHeader);
	HeapTupleHeaderSetNatts(tuple.t_data, 3);
	tuple.t_len
		= tuple.t_data->t_hoff + offsetof(FormData_pg_sequence_data, is_called) + sizeof(bool);
	data = (Form_pg_sequence_data)GETSTRUCT(&tuple);
	data->last_value = 5;
	data->is_called = true;
	data->log_cnt = 32;
	memset(&params_data, 0, sizeof(params_data));
	params_tuple.t_data = (HeapTupleHeader)params_data.data;
	params_tuple.t_data->t_hoff = MAXALIGN(SizeofHeapTupleHeader);
	params = (Form_pg_sequence)GETSTRUCT(&params_tuple);
	params->seqincrement = 1;
	params->seqmin = 1;
	params->seqmax = 1000;
	params->seqcache = 4;
	memset(&page_data, 0, sizeof(page_data));
	BufferBlocks = page_data.data;
	if (!fresh) {
		PageInit(page_data.data, BLCKSZ, sizeof(sequence_magic));
		((sequence_magic *)PageGetSpecialPointer(page_data.data))->magic = SEQ_MAGIC;
		UT_ASSERT_EQ(PageAddItem(page_data.data, (Item)tuple.t_data, tuple.t_len, 1, false, false),
					 1);
		((PageHeader)page_data.data)->pd_block_scn = 44;
		PageSetLSN(page_data.data, 0x8000);
	}
	memcpy(before.data, page_data.data, BLCKSZ);
	memset(&cache_data, 0, sizeof(cache_data));
	cache_data.relid = 20000;
	memset(&edge, 0, sizeof(edge));
	memset(&wal_header, 0, sizeof(wal_header));
	wal_count = edge_count = data_count = dirties = flushes = identity_reads = 0;
	locked = begun = pcm_active = error_expected = false;
	identity_valid = true;
	cluster_shared_config = true;
	wal_level = WAL_LEVEL_REPLICA;
	replaying = false;
	CritSectionCount = 0;
	token = 200;
}
static void
check_version(uint8 before_kind)
{
	UT_ASSERT_EQ(edge_count, 1);
	UT_ASSERT_EQ(wal_count, 1);
	UT_ASSERT_EQ(dirties, 1);
	UT_ASSERT_EQ(identity_reads, 1);
	UT_ASSERT_EQ(flushes, 1);
	UT_ASSERT(!locked);
	UT_ASSERT_EQ(edge.block_id, 0);
	UT_ASSERT_EQ(edge.before_kind, before_kind);
	UT_ASSERT_EQ(edge.before.mutation_token, before_kind == RF_PAGE_STATE_PRESENT ? 44 : 0);
	UT_ASSERT_EQ(edge.result_kind, RF_PAGE_STATE_PRESENT);
	UT_ASSERT(memcmp(edge.result_incarnation, identity.incarnation, 16) == 0);
	UT_ASSERT_EQ(((PageHeader)page_data.data)->pd_block_scn, 201);
	UT_ASSERT_EQ(wal_header.write_scn, 201);
	UT_ASSERT_EQ(PageGetLSN(page_data.data), 0x9000);
	UT_ASSERT(memcmp(&wal_tuple, page_tuple(), sizeof(wal_tuple)) == 0);
}
static void
init_version(void)
{
	reset(true);
	fill_seq_fork_with_data(&relation_data, &tuple, MAIN_FORKNUM);
	check_version(RF_PAGE_STATE_UNFORMATTED);
	UT_ASSERT_EQ(page_tuple()->last_value, 5);
}
static void
refill_version(void)
{
	int64 first, last;
	reset(false);
	cluster_sq_refill_page(&relation_data, 1, 1, 1000, 4, &first, &last);
	check_version(RF_PAGE_STATE_PRESENT);
	UT_ASSERT_EQ(first, 6);
	UT_ASSERT_EQ(last, 9);
	UT_ASSERT_EQ(page_tuple()->last_value, 9);
	UT_ASSERT_EQ(page_tuple()->log_cnt, 0);
}
static void
setval_version(void)
{
	for (int called = 0; called <= 1; called++) {
		reset(false);
		do_setval(20000, 90, called);
		check_version(RF_PAGE_STATE_PRESENT);
		UT_ASSERT_EQ(page_tuple()->last_value, 90);
		UT_ASSERT_EQ(page_tuple()->is_called, called);
	}
}
static void
native_cache_version(void)
{
	reset(false);
	UT_ASSERT_EQ(nextval_internal(20000, false), 6);
	check_version(RF_PAGE_STATE_PRESENT);
	UT_ASSERT_EQ(page_tuple()->last_value, 9);
	UT_ASSERT_EQ(page_tuple()->log_cnt, 0);
	UT_ASSERT_EQ(nextval_internal(20000, false), 7);
	UT_ASSERT_EQ(wal_count, 1);
	UT_ASSERT_EQ(identity_reads, 1);
}
static void
cache_boundaries(void)
{
	for (int scenario = 0; scenario < 4; scenario++) {
		Form_pg_sequence params;
		int64 expected_first, expected_last;
		reset(false);
		params = (Form_pg_sequence)GETSTRUCT(&params_tuple);
		if (scenario == 0) {
			page_tuple()->is_called = false;
			expected_first = 5;
			expected_last = 8;
		} else if (scenario == 1) {
			params->seqmax = 7;
			expected_first = 6;
			expected_last = 7;
		} else if (scenario == 2) {
			params->seqincrement = -2;
			params->seqmin = -100;
			expected_first = 3;
			expected_last = -3;
		} else {
			page_tuple()->is_called = false;
			params->seqcache = 1;
			expected_first = expected_last = 5;
		}
		memcpy(before.data, page_data.data, BLCKSZ);
		UT_ASSERT_EQ(nextval_internal(20000, false), expected_first);
		check_version(RF_PAGE_STATE_PRESENT);
		UT_ASSERT_EQ(page_tuple()->last_value, expected_last);
		UT_ASSERT_EQ(page_tuple()->log_cnt, 0);
	}
}
static void
nonshared_native(void)
{
	reset(false);
	cluster_shared_config = false;
	UT_ASSERT_EQ(nextval_internal(20000, false), 6);
	UT_ASSERT_EQ(wal_count, 0);
	UT_ASSERT_EQ(edge_count, 0);
	UT_ASSERT_EQ(identity_reads, 0);
	UT_ASSERT_EQ(page_tuple()->last_value, 9);
	UT_ASSERT_EQ(page_tuple()->log_cnt, 28);
	reset(false);
	cluster_shared_config = false;
	page_tuple()->log_cnt = 0;
	UT_ASSERT_EQ(nextval_internal(20000, false), 6);
	UT_ASSERT_EQ(wal_count, 1);
	UT_ASSERT_EQ(edge_count, 0);
	UT_ASSERT_EQ(identity_reads, 0);
	UT_ASSERT_EQ(page_tuple()->last_value, 9);
	UT_ASSERT_EQ(wal_tuple.last_value, 41);
	UT_ASSERT_EQ(page_tuple()->log_cnt, 32);
	reset(true);
	cluster_shared_config = false;
	fill_seq_fork_with_data(&relation_data, &tuple, MAIN_FORKNUM);
	UT_ASSERT_EQ(wal_count, 1);
	UT_ASSERT_EQ(edge_count, 0);
	UT_ASSERT_EQ(identity_reads, 0);
}
static void
bad_initial_predecessor(void)
{
	for (int scenario = 0; scenario < 3; scenario++) {
		reset(scenario != 1);
		if (scenario == 0)
			page_data.data[4000] = 1;
		if (scenario == 2)
			identity_valid = false;
		memcpy(before.data, page_data.data, BLCKSZ);
		error_expected = true;
		if (setjmp(error_jump) == 0) {
			fill_seq_fork_with_data(&relation_data, &tuple, MAIN_FORKNUM);
			UT_ASSERT(false);
		}
		UT_ASSERT_EQ(wal_count, 0);
		UT_ASSERT_EQ(dirties, 0);
		UT_ASSERT_EQ(token, 200);
		UT_ASSERT(memcmp(before.data, page_data.data, BLCKSZ) == 0);
	}
}
static void
rejected_boundary_is_not_cached(void)
{
	for (int operation = 0; operation < 3; operation++) {
		int64 start, end;
		reset(false);
		((PageHeader)page_data.data)->pd_block_scn = 0;
		memcpy(before.data, page_data.data, BLCKSZ);
		error_expected = true;
		if (setjmp(error_jump) == 0) {
			if (operation == 0)
				(void)nextval_internal(20000, false);
			else if (operation == 1)
				do_setval(20000, 100, true);
			else
				cluster_sq_refill_page(&relation_data, 1, 1, 1000, 4, &start, &end);
			UT_ASSERT(false);
		}
		UT_ASSERT(memcmp(before.data, page_data.data, BLCKSZ) == 0);
		UT_ASSERT_EQ(wal_count, 0);
		UT_ASSERT_EQ(token, 200);
		UT_ASSERT_EQ(cache_data.last, 0);
		UT_ASSERT_EQ(cache_data.cached, 0);
		UT_ASSERT(!cache_data.last_valid);
	}
}
static void
invalid_identity(void)
{
	for (int scenario = 0; scenario < 2; scenario++) {
		reset(false);
		if (scenario == 0)
			identity.key.database_incarnation++;
		else
			identity_valid = false;
		error_expected = true;
		if (setjmp(error_jump) == 0) {
			do_setval(20000, 100, true);
			UT_ASSERT(false);
		}
		UT_ASSERT(memcmp(before.data, page_data.data, BLCKSZ) == 0);
		UT_ASSERT_EQ(wal_count, 0);
		UT_ASSERT_EQ(token, 200);
	}
}
static void
exhausted_does_not_mutate(void)
{
	for (int operation = 0; operation < 2; operation++) {
		int64 start, end;
		reset(false);
		page_tuple()->last_value = 1000;
		memcpy(before.data, page_data.data, BLCKSZ);
		error_expected = true;
		if (setjmp(error_jump) == 0) {
			if (operation == 0)
				(void)nextval_internal(20000, false);
			else
				cluster_sq_refill_page(&relation_data, 1, 1, 1000, 4, &start, &end);
			UT_ASSERT(false);
		}
		UT_ASSERT(memcmp(before.data, page_data.data, BLCKSZ) == 0);
		UT_ASSERT_EQ(wal_count, 0);
		UT_ASSERT_EQ(token, 200);
	}
}
static void
shared_minimal_wal(void)
{
	for (int operation = 0; operation < 4; operation++) {
		int64 start, end;
		reset(operation == 0);
		wal_level = WAL_LEVEL_MINIMAL;
		relation_data.rd_createSubid = 1;
		error_expected = true;
		if (setjmp(error_jump) == 0) {
			if (operation == 0)
				fill_seq_fork_with_data(&relation_data, &tuple, MAIN_FORKNUM);
			else if (operation == 1)
				cluster_sq_refill_page(&relation_data, 1, 1, 1000, 4, &start, &end);
			else if (operation == 2)
				do_setval(20000, 90, true);
			else
				(void)nextval_internal(20000, false);
			check_version(operation == 0 ? RF_PAGE_STATE_UNFORMATTED : RF_PAGE_STATE_PRESENT);
		} else
			UT_ASSERT(false);
	}
}
static void
make_replay_record(void)
{
	DecodedXLogRecord *decoded = &record_space.decoded;
	memset(&record_space, 0, sizeof(record_space));
	memset(&reader, 0, sizeof(reader));
	decoded->header.xl_rmid = RM_SEQ_ID;
	decoded->header.xl_info = XLOG_SEQ_LOG;
	decoded->main_data = wal_payload.data;
	decoded->main_data_len = wal_payload_len;
	decoded->max_block_id = 0;
	decoded->blocks[0].in_use = true;
	decoded->blocks[0].flags = BKPBLOCK_WILL_INIT;
	decoded->blocks[0].rlocator = relation_data.rd_locator;
	decoded->blocks[0].forknum = MAIN_FORKNUM;
	decoded->blocks[0].blkno = 0;
	decoded->has_page_version_edge = edge_count != 0;
	if (edge_count != 0) {
		decoded->page_version_edge.entry_count = 1;
		decoded->page_version_edge.result_token = wal_header.write_scn;
		decoded->page_version_edge.entries[0] = edge;
	}
	reader.record = decoded;
	reader.EndRecPtr = 0x9000;
}
static void
native_redo_differential(void)
{
	for (int operation = 0; operation < 5; operation++) {
		PGAlignedBlock detached;
		int64 start, end;
		reset(operation == 0);
		if (operation == 0)
			fill_seq_fork_with_data(&relation_data, &tuple, MAIN_FORKNUM);
		else if (operation == 1)
			cluster_sq_refill_page(&relation_data, 1, 1, 1000, 4, &start, &end);
		else if (operation == 2)
			do_setval(20000, 90, false);
		else {
			if (operation == 4) {
				cluster_shared_config = false;
				page_tuple()->log_cnt = 0;
			}
			(void)nextval_internal(20000, false);
		}
		make_replay_record();
		memset(detached.data, 0xa5, BLCKSZ);
		memcpy(page_data.data, detached.data, BLCKSZ);
		replaying = true;
		seq_redo(&reader);
		UT_ASSERT_EQ(cluster_block_apply_one(&reader, 0, detached.data), CLUSTER_BLKAPPLY_OK);
		UT_ASSERT(memcmp(detached.data, page_data.data, BLCKSZ) == 0);
		UT_ASSERT_EQ(page_tuple()->last_value, operation == 4 ? 41 : wal_tuple.last_value);
	}
}
static void
bad_redo_preserves_output(void)
{
	for (int scenario = 0; scenario < 13; scenario++) {
		PGAlignedBlock detached, saved;
		DecodedXLogRecord *decoded;
		HeapTupleHeader header;
		reset(true);
		fill_seq_fork_with_data(&relation_data, &tuple, MAIN_FORKNUM);
		make_replay_record();
		decoded = reader.record;
		header = (HeapTupleHeader)(wal_payload.data + sizeof(xl_seq_rec));
		switch (scenario) {
		case 0:
			decoded->main_data_len = sizeof(xl_seq_rec) - 1;
			break;
		case 1:
			decoded->main_data_len = sizeof(xl_seq_rec) + 4;
			break;
		case 2:
			decoded->blocks[0].rlocator.relNumber++;
			break;
		case 3:
			decoded->blocks[0].forknum = VISIBILITYMAP_FORKNUM;
			break;
		case 4:
			decoded->blocks[0].blkno = 1;
			break;
		case 5:
			header->t_hoff = 200;
			break;
		case 6:
			decoded->page_version_edge.result_token++;
			break;
		case 7:
			decoded->header.xl_info = 0x10;
			break;
		case 8:
			header->t_infomask |= HEAP_HASNULL;
			break;
		case 9:
			HeapTupleHeaderSetNatts(header, 2);
			break;
		case 10:
			decoded->blocks[0].flags = 0;
			break;
		case 11:
			((uint8 *)header)[header->t_hoff + offsetof(FormData_pg_sequence_data, is_called)] = 2;
			break;
		case 12:
			decoded->main_data_len = BLCKSZ;
			break;
		}
		memset(detached.data, 0xa5, BLCKSZ);
		memcpy(saved.data, detached.data, BLCKSZ);
		UT_ASSERT(cluster_block_apply_one(&reader, 0, detached.data) != CLUSTER_BLKAPPLY_OK);
		UT_ASSERT(memcmp(saved.data, detached.data, BLCKSZ) == 0);
	}
}
int
main(void)
{
	UT_PLAN(13);
	UT_RUN(init_version);
	UT_RUN(refill_version);
	UT_RUN(setval_version);
	UT_RUN(native_cache_version);
	UT_RUN(cache_boundaries);
	UT_RUN(nonshared_native);
	UT_RUN(bad_initial_predecessor);
	UT_RUN(rejected_boundary_is_not_cached);
	UT_RUN(invalid_identity);
	UT_RUN(exhausted_does_not_mutate);
	UT_RUN(shared_minimal_wal);
	UT_RUN(native_redo_differential);
	UT_RUN(bad_redo_preserves_output);
	UT_DONE();
	return ut_failed_count ? 1 : 0;
}
