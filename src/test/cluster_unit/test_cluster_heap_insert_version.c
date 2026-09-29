/*-------------------------------------------------------------------------
 *
 * test_cluster_heap_insert_version.c
 *    Execute native INSERT/DELETE critical mutation and WAL publication.
 *
 * Portions Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *    src/test/cluster_unit/test_cluster_heap_insert_version.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/heapam.h"
#include "access/heapam_xlog.h"
#include "access/htup_details.h"
#include "access/visibilitymap.h"
#include "access/xlog.h"
#include "access/xloginsert.h"
#include "catalog/catalog.h"
#include "catalog/pg_class.h"
#include "cluster/cluster_guc.h"
#include "cluster/cluster_itl.h"
#include "cluster/cluster_page_producer.h"
#include "cluster/cluster_space_storage.h"
#include "cluster/cluster_wal_thread.h"
#include "cluster/storage/cluster_smgr.h"
#include "miscadmin.h"
#include "storage/bufmgr.h"
#include "utils/rel.h"
#include "unit_test.h"

UT_DEFINE_GLOBALS();

bool cluster_enabled = true, cluster_shared_config = true;
int wal_level = WAL_LEVEL_REPLICA, NBuffers = 2, NLocBuffer, cluster_node_id;
char *BufferBlocks;
Block *LocalBufferBlockPointers;
volatile uint32 CritSectionCount;
bool cluster_recmerge_window_active, cluster_recmerge_apply_foreign;
uint64 cluster_recmerge_window_scn, cluster_recmerge_window_own_lsn;

static PGAlignedBlock pages[2], images[2];
static FormData_pg_class relform;
static RelationData relation_data;
static HeapTupleData tuple;
static char tuple_bytes[MAXALIGN(SizeofHeapTupleHeader) + 16];
static bool dirty[2], registered[2], begun, want_vm;
static uint8 register_flags[2], wal_info, heap_flags, edge_count;
static RfPageVersionEdgeEntryV1 edges[2];
static uint64 next_token, edge_token;
static unsigned inserts, vm_clears, itl_stamps, data_calls, origin_flags;
static RfPageProducerBatchV1 prepared;
static ClusterSpaceIdentity identity;
static ClusterWalDurablePrefixRef ref;
static RelFileLocator tags[2];
static ForkNumber forks[2];
static bool permanent, claim_ready, recovering;
static bool deleting;

void
ExceptionalCondition(const char *condition, const char *file, int line)
{
	printf("# Unexpected assertion: %s at %s:%d\n", condition, file, line);
	abort();
}

bool
RecoveryInProgress(void)
{
	return recovering;
}

bool
cluster_wal_thread_current_v2_ref(ClusterWalDurablePrefixRef *out)
{
	*out = ref;
	return claim_ready;
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
	if (!RelFileLocatorEquals(locator, identity.key.locator) || backend != InvalidBackendId)
		abort();
	return 1;
}

void
BufferGetTag(Buffer buffer, RelFileLocator *locator, ForkNumber *forknum, BlockNumber *block)
{
	if (buffer != 1 && buffer != 2)
		abort();
	*locator = tags[buffer - 1];
	*forknum = forks[buffer - 1];
	*block = BufferGetBlockNumber(buffer);
}

bool
BufferIsPermanent(Buffer buffer)
{
	if (buffer != 1 && buffer != 2)
		abort();
	return permanent;
}

bool
IsCatalogRelation(Relation relation)
{
	(void)relation;
	return false;
}

bool
IsToastRelation(Relation relation)
{
	(void)relation;
	return false;
}

static void
log_heap_new_cid(Relation relation, HeapTuple heaptup)
{
	(void)relation;
	(void)heaptup;
	abort();
}

BlockNumber
BufferGetBlockNumber(Buffer buffer)
{
	if (buffer != 1 && buffer != 2)
		abort();
	return buffer == 1 ? 10 : 0;
}

void
MarkBufferDirty(Buffer buffer)
{
	if (CritSectionCount == 0 || (buffer != 1 && buffer != 2))
		abort();
	dirty[buffer - 1] = true;
}

/* Receipt/authority setup and the ITL stamp's own internal history are not
 * reproduced here. The real ITL producer has a separate production-C test. */
void
cluster_itl_stamp_active_with_history(Buffer buffer, uint8 slot, TransactionId xid, SCN write_scn,
									  UBA uba)
{
	ClusterItlSlotData *itl = &ClusterPageGetItlSlots(BufferGetPage(buffer))[slot];

	itl->xid = xid;
	itl->flags = ITL_FLAG_ACTIVE;
	itl->write_scn = write_scn;
	itl->undo_segment_head = uba;
	itl_stamps++;
}

bool
visibilitymap_clear_locked(Relation rel, BlockNumber heapblock, Buffer vmbuffer, uint8 flags)
{
	bool changed;

	if (rel != &relation_data || heapblock != 10 || vmbuffer != 2
		|| flags != VISIBILITYMAP_VALID_BITS || !want_vm || CritSectionCount == 0)
		abort();
	changed = pages[1].data[SizeOfPageHeaderData] != 0;
	pages[1].data[SizeOfPageHeaderData] = 0;
	if (changed)
		MarkBufferDirty(vmbuffer);
	vm_clears++;
	return changed;
}

SCN
cluster_scn_advance(void)
{
	return ++next_token;
}

void
XLogBeginInsert(void)
{
	if (begun || CritSectionCount == 0)
		abort();
	begun = true;
}

void
XLogRegisterData(char *bytes, uint32 len)
{
	if (!begun)
		abort();
	if (data_calls++ == 0) {
		if (len != (deleting ? SizeOfHeapDelete : SizeOfHeapInsert))
			abort();
		heap_flags = deleting ? ((xl_heap_delete *)bytes)->flags : ((xl_heap_insert *)bytes)->flags;
	}
}

void
XLogRegisterBuffer(uint8 id, Buffer buffer, uint8 flags)
{
	if (!begun || id >= 2 || buffer != id + 1 || registered[id])
		abort();
	registered[id] = true;
	register_flags[id] = flags;
	images[id] = pages[id];
}

void
XLogRegisterBufData(uint8 id, char *bytes, uint32 len)
{
	if (!begun || id != 0 || !registered[id] || bytes == NULL || len == 0)
		abort();
}

void
XLogRegisterPageVersionEdge(uint64 result, const RfPageVersionEdgeEntryV1 *entries, uint8 count)
{
	if (!begun || count == 0 || count > 2)
		abort();
	edge_count = count;
	edge_token = result;
	memcpy(edges, entries, count * sizeof(*entries));
}

void
XLogSetRecordFlags(uint8 flags)
{
	origin_flags = flags;
}

XLogRecPtr
XLogInsert(RmgrId rmid, uint8 info)
{
	if (!begun || rmid != RM_HEAP_ID
		|| (info & XLOG_HEAP_OPMASK) != (deleting ? XLOG_HEAP_DELETE : XLOG_HEAP_INSERT))
		abort();
	UT_ASSERT(dirty[0]);
	if (edge_count == 2)
		UT_ASSERT(dirty[1]);
	wal_info = info;
	inserts++;
	begun = false;
	return 200;
}

bool
errstart(int elevel, const char *domain)
{
	(void)domain;
	/* Native CRC dispatch logs its hardware choice on Linux. */
	if (elevel == DEBUG1)
		return false;
	printf("# Unexpected error level %d\n", elevel);
	abort();
}

int
errmsg_internal(const char *fmt, ...)
{
	(void)fmt;
	abort();
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

void
errfinish(const char *file, int line, const char *func)
{
	(void)file;
	(void)line;
	(void)func;
	abort();
}

#include "test_cluster_heap_put_tuple.inc"
#include "test_cluster_heap_infobits.inc"
#include "test_cluster_space_xid_precedes.inc"

static void
run_insert(bool versioned, bool active_itl, int options)
{
	Relation relation = &relation_data;
	HeapTuple heaptup = &tuple;
	Buffer buffer = 1, vmbuffer = want_vm ? 2 : InvalidBuffer;
	bool all_visible_cleared = false;
	bool cluster_itl_active = active_itl;
	uint8 cluster_itl_slot = 0;
	OffsetNumber cluster_itl_insert_offnum = PageGetMaxOffsetNumber(pages[0].data) + 1;
	TransactionId canonical_xid = 500;
	SCN cluster_itl_write_scn = 800;
	UBA cluster_itl_uba = InvalidUba_init;
	bool cluster_page_versioned = versioned;
	RfPageProducerBatchV1 cluster_page_versions = prepared;

	(void)cluster_page_versioned;
	(void)cluster_page_versions;
#include "test_cluster_heap_insert_version.inc"
}

static void
run_delete(bool versioned, bool vm, bool recomposed)
{
	Relation relation = &relation_data;
	Buffer buffer = 1, vmbuffer = vm ? 2 : InvalidBuffer;
	Page page = pages[0].data;
	HeapTupleData tp = tuple;
	HeapTuple old_key_tuple = NULL;
	TransactionId xid = 501, canonical_xid = 501, new_xmax = 501;
	CommandId cid = 2;
	uint16 new_infomask = 0, new_infomask2 = 0;
	bool all_visible_cleared = false, iscombo = false, changingPart = false;
	bool cluster_itl_active = true, cluster_current_mx_recomposed = recomposed;
	uint8 cluster_itl_slot = 0;
	SCN cluster_itl_write_scn = 810;
	UBA cluster_itl_uba = InvalidUba_init;
	uint8 cluster_current_mx_planned_header[SizeofHeapTupleHeader] pg_attribute_aligned(
		MAXIMUM_ALIGNOF);
	bool cluster_page_versioned = versioned;
	RfPageProducerBatchV1 cluster_page_versions = prepared;

	tp.t_data = (HeapTupleHeader)PageGetItem(page, PageGetItemId(page, FirstOffsetNumber));
	memcpy(cluster_current_mx_planned_header, tp.t_data, SizeofHeapTupleHeader);
	if (recomposed) {
		HeapTupleHeader planned = (HeapTupleHeader)cluster_current_mx_planned_header;

		new_xmax = 900;
		planned->t_itl_slot_idx = 0;
		planned->t_infomask = HEAP_XMAX_IS_MULTI;
		HeapTupleHeaderSetXmax(planned, new_xmax);
	}
	(void)cluster_page_versioned;
	(void)cluster_page_versions;
	deleting = true;
#include "test_cluster_heap_delete_version.inc"
}

static void
reset(bool versioned, bool vm, bool bit_set)
{
	RfPageProducerComponentV1 components[2];
	int i;

	memset(pages, 0, sizeof(pages));
	memset(images, 0, sizeof(images));
	memset(&relform, 0, sizeof(relform));
	memset(&relation_data, 0, sizeof(relation_data));
	memset(&tuple, 0, sizeof(tuple));
	memset(tuple_bytes, 0, sizeof(tuple_bytes));
	memset(dirty, 0, sizeof(dirty));
	memset(registered, 0, sizeof(registered));
	memset(register_flags, 0, sizeof(register_flags));
	memset(edges, 0, sizeof(edges));
	memset(components, 0, sizeof(components));
	memset(&prepared, 0, sizeof(prepared));
	memset(&identity, 0, sizeof(identity));
	memset(&ref, 0, sizeof(ref));
	identity.key.system_identifier = ref.claim.identity.system_identifier = 11;
	identity.key.database_incarnation = ref.claim.database_incarnation = 12;
	identity.key.storage_uuid[15] = ref.claim.identity.storage_uuid[15] = 13;
	identity.key.locator = (RelFileLocator){ 1663, 5, 16384 };
	identity.incarnation[0] = 4;
	identity.incarnation[15] = 9;
	identity.sequence = identity.operation = 1;
	identity.state = CLUSTER_SPACE_IDENTITY_LIVE;
	tags[0] = tags[1] = identity.key.locator;
	forks[0] = MAIN_FORKNUM;
	forks[1] = VISIBILITYMAP_FORKNUM;
	permanent = claim_ready = true;
	recovering = deleting = false;
	NLocBuffer = 1;
	BufferBlocks = pages[0].data;
	PageInitHeapPage(pages[0].data, BLCKSZ, 0);
	PageInit(pages[1].data, BLCKSZ, 0);
	((PageHeader)pages[0].data)->pd_block_scn = 17;
	((PageHeader)pages[1].data)->pd_block_scn = 31;
	if (vm)
		PageSetAllVisible(pages[0].data);
	pages[1].data[SizeOfPageHeaderData] = bit_set ? 3 : 0;
	/* The far bitmap byte must never be treated as a STANDARD-page hole. */
	pages[1].data[BLCKSZ - 1] = 42;
	relform.relpersistence = RELPERSISTENCE_PERMANENT;
	relform.relkind = RELKIND_RELATION;
	relation_data.rd_rel = &relform;
	tuple.t_data = (HeapTupleHeader)tuple_bytes;
	tuple.t_data->t_hoff = MAXALIGN(SizeofHeapTupleHeader);
	tuple.t_len = sizeof(tuple_bytes);
	next_token = 99;
	inserts = vm_clears = itl_stamps = data_calls = origin_flags = 0;
	edge_token = edge_count = heap_flags = wal_info = 0;
	begun = false;
	want_vm = vm;
	CritSectionCount = 0;
	cluster_shared_config = versioned;
	if (!versioned)
		return;
	for (i = 0; i < (vm ? 2 : 1); i++) {
		components[i].block_id = i;
		components[i].component_ordinal = i;
		components[i].page_class = RF_PAGE_CLASS_ORDINARY;
		components[i].before_kind = RF_PAGE_STATE_PRESENT;
		components[i].segment_incarnation[0] = 4;
		components[i].segment_incarnation[15] = 9;
		components[i].page = pages[i].data;
	}
	UT_ASSERT(rf_page_producer_prepare_v1(components, vm ? 2 : 1, &prepared));
	UT_ASSERT_EQ(((PageHeader)pages[0].data)->pd_block_scn, 17);
	UT_ASSERT_EQ(((PageHeader)pages[1].data)->pd_block_scn, 31);
}

UT_TEST(test_native_insert_publishes_heap_edge_with_itl_delta)
{
	reset(true, false, false);
	run_insert(true, true, 0);
	UT_ASSERT_EQ(inserts, 1);
	UT_ASSERT_EQ(edge_count, 1);
	UT_ASSERT_EQ(edge_token, 100);
	UT_ASSERT_EQ(edges[0].before.mutation_token, 17);
	UT_ASSERT_EQ(edges[0].before.segment_incarnation[15], 9);
	UT_ASSERT_EQ(((PageHeader)pages[0].data)->pd_block_scn, 100);
	UT_ASSERT_EQ(((PageHeader)images[0].data)->pd_block_scn, 100);
	UT_ASSERT_EQ(PageGetLSN(pages[0].data), 200);
	UT_ASSERT_EQ(PageGetMaxOffsetNumber(pages[0].data), 1);
	UT_ASSERT_EQ(itl_stamps, 1);
	UT_ASSERT_EQ(data_calls, 3);
	UT_ASSERT((heap_flags & XLH_INSERT_ITL_DELTA) != 0);
	UT_ASSERT((wal_info & XLOG_HEAP_INIT_PAGE) != 0);
	UT_ASSERT((register_flags[0] & REGBUF_WILL_INIT) != 0);
	UT_ASSERT_EQ(origin_flags, XLOG_INCLUDE_ORIGIN);
	UT_ASSERT_EQ(CritSectionCount, 0);
}

UT_TEST(test_native_insert_covers_vm_even_when_bit_was_already_clear)
{
	int bit;

	for (bit = 0; bit < 2; bit++) {
		reset(true, true, bit != 0);
		run_insert(true, false, 0);
		UT_ASSERT_EQ(edge_count, 2);
		UT_ASSERT_EQ(edges[1].block_id, 1);
		UT_ASSERT_EQ(edges[1].before.mutation_token, 31);
		UT_ASSERT_EQ(edges[1].result_incarnation[15], 9);
		UT_ASSERT_EQ(((PageHeader)pages[1].data)->pd_block_scn, 100);
		UT_ASSERT_EQ(PageGetLSN(pages[1].data), 200);
		UT_ASSERT(dirty[1] && registered[1]);
		UT_ASSERT_EQ(register_flags[1], REGBUF_FORCE_IMAGE);
		UT_ASSERT_EQ(images[1].data[BLCKSZ - 1], 42);
		UT_ASSERT_EQ(vm_clears, 1);
		UT_ASSERT(!PageIsAllVisible(pages[0].data));
		UT_ASSERT((heap_flags & XLH_INSERT_ALL_VISIBLE_CLEARED) != 0);
	}
}

UT_TEST(test_legacy_insert_keeps_existing_wal_shape)
{
	reset(false, true, true);
	run_insert(false, false, 0);
	UT_ASSERT_EQ(edge_count, 0);
	UT_ASSERT_EQ(((PageHeader)pages[0].data)->pd_block_scn, 17);
	UT_ASSERT_EQ(((PageHeader)pages[1].data)->pd_block_scn, 31);
	UT_ASSERT(!registered[1]);
	UT_ASSERT_EQ(inserts, 1);
	UT_ASSERT_EQ(vm_clears, 1);
	UT_ASSERT_EQ(CritSectionCount, 0);
}

UT_TEST(test_buffer_capture_has_no_mutation_before_apply)
{
	Buffer buffers[2] = { 1, 2 };
	uint8 ids[2] = { 0, 1 };
	PGAlignedBlock saved[2];

	reset(true, true, true);
	memcpy(saved, pages, sizeof(saved));
	UT_ASSERT(cluster_space_prepare_buffer_versions(&identity, buffers, ids, 2, &prepared));
	UT_ASSERT(memcmp(saved, pages, sizeof(saved)) == 0);
	UT_ASSERT(!prepared.stamped);
	UT_ASSERT_EQ(prepared.result_token, 101);
	UT_ASSERT_EQ(prepared.entries[0].before.mutation_token, 17);
	UT_ASSERT_EQ(prepared.entries[1].before.mutation_token, 31);
	UT_ASSERT_EQ(prepared.entries[1].before.segment_incarnation[15], 9);
	UT_ASSERT_EQ(inserts, 0);
	UT_ASSERT(!dirty[0] && !dirty[1]);
	run_insert(true, true, 0);
	UT_ASSERT_EQ(edge_count, 2);
	UT_ASSERT_EQ(edge_token, 101);
}

UT_TEST(test_capture_refuses_whole_batch_without_token_or_byte_change)
{
	int reason;

	for (reason = 0; reason < 15; reason++) {
		Buffer buffers[2] = { 1, 2 };
		uint8 ids[2] = { 0, 1 };
		uint8 count = 2;
		PGAlignedBlock saved[2];
		RfPageProducerBatchV1 out, old;

		reset(true, true, true);
		switch (reason) {
		case 0:
			tags[1].relNumber++;
			break;
		case 1:
			ref.claim.database_incarnation++;
			break;
		case 2:
			identity.state = CLUSTER_SPACE_IDENTITY_TOMBSTONED;
			break;
		case 3:
			memset(identity.incarnation, 0, sizeof(identity.incarnation));
			break;
		case 4:
			memset(&pages[1], 0, sizeof(pages[1]));
			break;
		case 5:
			((PageHeader)pages[1].data)->pd_block_scn = 0;
			break;
		case 6:
			((PageHeader)pages[1].data)->pd_flags |= PD_UNDO_SEG_HEADER;
			break;
		case 7:
			forks[1] = SPACE_FORKNUM;
			break;
		case 8:
			buffers[1] = -1;
			break;
		case 9:
			permanent = false;
			break;
		case 10:
			buffers[1] = 1;
			break;
		case 11:
			ids[0] = 2;
			break;
		case 12:
			count = 0;
			break;
		case 13:
			claim_ready = false;
			break;
		case 14:
			recovering = true;
			break;
		}
		memcpy(saved, pages, sizeof(saved));
		memset(&old, 0xa5, sizeof(old));
		out = old;
		UT_ASSERT(!cluster_space_prepare_buffer_versions(&identity, buffers, ids, count, &out));
		UT_ASSERT(memcmp(&old, &out, sizeof(out)) == 0);
		UT_ASSERT(memcmp(saved, pages, sizeof(saved)) == 0);
		UT_ASSERT_EQ(next_token, 100);
		UT_ASSERT_EQ(inserts, 0);
		UT_ASSERT(!dirty[0] && !dirty[1]);
	}
}

UT_TEST(test_same_transaction_second_insert_has_distinct_before_and_result)
{
	Buffer buffer = 1;
	uint8 id = 0;

	reset(true, false, false);
	run_insert(true, true, 0);
	UT_ASSERT(cluster_space_prepare_buffer_versions(&identity, &buffer, &id, 1, &prepared));
	registered[0] = false;
	data_calls = 0;
	run_insert(true, true, 0);
	UT_ASSERT_EQ(inserts, 2);
	UT_ASSERT_EQ(edge_token, 101);
	UT_ASSERT_EQ(edges[0].before.mutation_token, 100);
	UT_ASSERT_EQ(((PageHeader)pages[0].data)->pd_block_scn, 101);
	UT_ASSERT_EQ(PageGetMaxOffsetNumber(pages[0].data), 2);
	UT_ASSERT_EQ(itl_stamps, 2);
	UT_ASSERT((wal_info & XLOG_HEAP_INIT_PAGE) == 0);
}

UT_TEST(test_native_delete_has_heap_vm_edge_and_retains_recomposed_header)
{
	int mode;

	for (mode = 0; mode < 3; mode++) {
		Buffer buffers[2] = { 1, 2 };
		uint8 ids[2] = { 0, 1 };
		bool vm = mode > 0;
		HeapTupleHeader deleted;

		reset(true, false, false);
		run_insert(true, true, 0);
		want_vm = vm;
		if (vm)
			PageSetAllVisible(pages[0].data);
		pages[1].data[SizeOfPageHeaderData] = mode == 1 ? 3 : 0;
		UT_ASSERT(
			cluster_space_prepare_buffer_versions(&identity, buffers, ids, vm ? 2 : 1, &prepared));
		memset(registered, 0, sizeof(registered));
		memset(dirty, 0, sizeof(dirty));
		edge_count = data_calls = 0;
		run_delete(true, vm, mode == 2);
		deleted = (HeapTupleHeader)PageGetItem(pages[0].data,
											   PageGetItemId(pages[0].data, FirstOffsetNumber));
		UT_ASSERT_EQ(edge_count, vm ? 2 : 1);
		UT_ASSERT_EQ(edge_token, 101);
		UT_ASSERT_EQ(edges[0].before.mutation_token, 100);
		UT_ASSERT_EQ(((PageHeader)pages[0].data)->pd_block_scn, 101);
		UT_ASSERT_EQ(HeapTupleHeaderGetRawXmax(deleted), mode == 2 ? 900 : 501);
		UT_ASSERT_EQ(deleted->t_itl_slot_idx, 0);
		UT_ASSERT((heap_flags & XLH_DELETE_ITL_DELTA) != 0);
		UT_ASSERT_EQ(((PageHeader)pages[0].data)->pd_prune_xid, 501);
		if (vm) {
			UT_ASSERT_EQ(edges[1].before.mutation_token, 31);
			UT_ASSERT_EQ(((PageHeader)pages[1].data)->pd_block_scn, 101);
			UT_ASSERT_EQ(PageGetLSN(pages[1].data), 200);
			UT_ASSERT(dirty[1] && registered[1]);
			UT_ASSERT_EQ(register_flags[1], REGBUF_FORCE_IMAGE);
			UT_ASSERT_EQ(images[1].data[BLCKSZ - 1], 42);
		}
	}
}

UT_TEST(test_legacy_delete_keeps_implicit_vm_wal_shape)
{
	reset(false, false, false);
	run_insert(false, true, 0);
	want_vm = true;
	PageSetAllVisible(pages[0].data);
	pages[1].data[SizeOfPageHeaderData] = 3;
	memset(registered, 0, sizeof(registered));
	data_calls = 0;
	run_delete(false, true, false);
	UT_ASSERT_EQ(edge_count, 0);
	UT_ASSERT(!registered[1]);
	UT_ASSERT_EQ(((PageHeader)pages[0].data)->pd_block_scn, 17);
	UT_ASSERT_EQ(((PageHeader)pages[1].data)->pd_block_scn, 31);
	UT_ASSERT_EQ(inserts, 2);
}

int
main(void)
{
	UT_PLAN(8);
	UT_RUN(test_native_insert_publishes_heap_edge_with_itl_delta);
	UT_RUN(test_native_insert_covers_vm_even_when_bit_was_already_clear);
	UT_RUN(test_legacy_insert_keeps_existing_wal_shape);
	UT_RUN(test_buffer_capture_has_no_mutation_before_apply);
	UT_RUN(test_capture_refuses_whole_batch_without_token_or_byte_change);
	UT_RUN(test_same_transaction_second_insert_has_distinct_before_and_result);
	UT_RUN(test_native_delete_has_heap_vm_edge_and_retains_recomposed_header);
	UT_RUN(test_legacy_delete_keeps_implicit_vm_wal_shape);
	UT_DONE();
	return ut_failed_count ? 1 : 0;
}
