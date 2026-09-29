/*-------------------------------------------------------------------------
 *
 * test_cluster_heap_insert_version.c
 *    Execute native INSERT/DELETE/UPDATE critical mutation and WAL publication.
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
#include "cluster/cluster_pcm_lock.h"
#include "cluster/cluster_pcm_x_bufmgr.h"
#include "cluster/cluster_space_storage.h"
#include "cluster/cluster_wal_thread.h"
#include "cluster/storage/cluster_smgr.h"
#include "miscadmin.h"
#include "storage/bufmgr.h"
#include "storage/buf_internals.h"
#include "utils/rel.h"
#include "unit_test.h"

UT_DEFINE_GLOBALS();

bool cluster_enabled = true, cluster_shared_config = true, cluster_shared_catalog;
int wal_level = WAL_LEVEL_REPLICA, NBuffers = 4, NLocBuffer, cluster_node_id;
char *BufferBlocks;
Block *LocalBufferBlockPointers;
volatile uint32 CritSectionCount;
bool cluster_recmerge_window_active, cluster_recmerge_apply_foreign;
uint64 cluster_recmerge_window_scn, cluster_recmerge_window_own_lsn;

static PGAlignedBlock pages[4], images[4];
static BufferDescPadded descriptors[4];
BufferDescPadded *BufferDescriptors = descriptors;
static FormData_pg_class relform;
static RelationData relation_data;
static HeapTupleData tuple;
static char tuple_bytes[MAXALIGN(SizeofHeapTupleHeader) + 16];
static bool dirty[4], registered[4], begun, want_vm;
static uint8 register_flags[4], wal_info, heap_flags, edge_count;
static Buffer registered_buffers[4];
static RfPageVersionEdgeEntryV1 edges[4];
static uint64 next_token, edge_token;
static unsigned inserts, vm_clears, itl_stamps, data_calls, origin_flags;
static RfPageProducerBatchV1 prepared;
static ClusterSpaceIdentity identity;
static ClusterWalDurablePrefixRef ref;
static RelFileLocator tags[4];
static ForkNumber forks[4];
static BlockNumber blocks[4];
static bool permanent, claim_ready, recovering;
static bool deleting, updating, temp_locking;

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
	if (buffer < 1 || buffer > 4)
		abort();
	*locator = tags[buffer - 1];
	*forknum = forks[buffer - 1];
	*block = BufferGetBlockNumber(buffer);
}

bool
BufferIsPermanent(Buffer buffer)
{
	if (buffer < 1 || buffer > 4)
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
	if (buffer < 1 || buffer > 4)
		abort();
	return blocks[buffer - 1];
}

void
MarkBufferDirty(Buffer buffer)
{
	if (CritSectionCount == 0 || buffer < 1 || buffer > 4)
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

	if (rel != &relation_data || (heapblock != blocks[0] && heapblock != blocks[2])
		|| (vmbuffer != 2 && vmbuffer != 4)
		|| flags != (temp_locking ? VISIBILITYMAP_ALL_FROZEN : VISIBILITYMAP_VALID_BITS) || !want_vm
		|| CritSectionCount == 0)
		abort();
	changed = (pages[vmbuffer - 1].data[SizeOfPageHeaderData] & flags) != 0;
	pages[vmbuffer - 1].data[SizeOfPageHeaderData] &= ~flags;
	if (changed)
		MarkBufferDirty(vmbuffer);
	vm_clears++;
	return changed;
}

bool
visibilitymap_pin_ok(BlockNumber heapblock, Buffer buffer)
{
	return (heapblock == blocks[0] || heapblock == blocks[2]) && (buffer == 2 || buffer == 4);
}

uint8
visibilitymap_get_status(Relation rel, BlockNumber heapblock, Buffer *buffer)
{
	if (rel != &relation_data || !visibilitymap_pin_ok(heapblock, *buffer))
		abort();
	return pages[*buffer - 1].data[SizeOfPageHeaderData];
}

void
cluster_pcm_vm_metric_note(const BufferTag *tag, PcmVmMetric metric)
{
	(void)tag;
	(void)metric;
	abort();
}

void
cluster_pcm_vm_clear_note(const BufferTag *tag, BlockNumber heapblock)
{
	(void)tag;
	(void)heapblock;
}

bool
XLogCheckBufferNeedsBackup(Buffer buffer)
{
	if (buffer < 1 || buffer > 4)
		abort();
	return false;
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
		if (len
			!= (temp_locking ? SizeOfHeapLock
				: updating	 ? SizeOfHeapUpdate
				: deleting	 ? SizeOfHeapDelete
							 : SizeOfHeapInsert))
			abort();
		heap_flags = temp_locking ? ((xl_heap_lock *)bytes)->flags
					 : updating	  ? ((xl_heap_update *)bytes)->flags
					 : deleting	  ? ((xl_heap_delete *)bytes)->flags
								  : ((xl_heap_insert *)bytes)->flags;
	}
}

void
XLogRegisterBuffer(uint8 id, Buffer buffer, uint8 flags)
{
	if (!begun || id >= 4 || buffer < 1 || buffer > 4 || registered[id])
		abort();
	registered[id] = true;
	registered_buffers[id] = buffer;
	register_flags[id] = flags;
	images[id] = pages[buffer - 1];
}

void
XLogRegisterBufData(uint8 id, char *bytes, uint32 len)
{
	/* Native UPDATE legitimately registers a zero-length suffix when its
	 * entire payload was captured by the prefix optimization. */
	if (!begun || id != 0 || !registered[id] || bytes == NULL)
		abort();
	(void)len;
}

void
XLogRegisterPageVersionEdge(uint64 result, const RfPageVersionEdgeEntryV1 *entries, uint8 count)
{
	if (!begun || count == 0 || count > 4)
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
	int i;
	uint8 operation = info & XLOG_HEAP_OPMASK;

	if (!begun || rmid != RM_HEAP_ID
		|| (temp_locking ? operation != XLOG_HEAP_LOCK
			: updating	 ? operation != XLOG_HEAP_UPDATE && operation != XLOG_HEAP_HOT_UPDATE
						 : operation != (deleting ? XLOG_HEAP_DELETE : XLOG_HEAP_INSERT)))
		abort();
	for (i = 0; i < 4; i++)
		if (registered[i])
			UT_ASSERT(dirty[registered_buffers[i] - 1]);
	for (i = 0; i < edge_count; i++)
		UT_ASSERT(registered[edges[i].block_id]);
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
#include "test_cluster_heap_update_log.inc"

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
	memset(registered_buffers, 0, sizeof(registered_buffers));
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
	tags[0] = tags[1] = tags[2] = tags[3] = identity.key.locator;
	forks[0] = forks[2] = MAIN_FORKNUM;
	forks[1] = forks[3] = VISIBILITYMAP_FORKNUM;
	blocks[0] = 10;
	blocks[1] = 0;
	blocks[2] = 11;
	blocks[3] = 1;
	permanent = claim_ready = true;
	recovering = deleting = updating = temp_locking = false;
	NLocBuffer = 1;
	BufferBlocks = pages[0].data;
	PageInitHeapPage(pages[0].data, BLCKSZ, 0);
	PageInit(pages[1].data, BLCKSZ, 0);
	PageInitHeapPage(pages[2].data, BLCKSZ, 0);
	PageInit(pages[3].data, BLCKSZ, 0);
	((PageHeader)pages[0].data)->pd_block_scn = 17;
	((PageHeader)pages[1].data)->pd_block_scn = 31;
	((PageHeader)pages[2].data)->pd_block_scn = 41;
	((PageHeader)pages[3].data)->pd_block_scn = 61;
	if (vm)
		PageSetAllVisible(pages[0].data);
	pages[1].data[SizeOfPageHeaderData] = bit_set ? 3 : 0;
	/* The far bitmap byte must never be treated as a STANDARD-page hole. */
	pages[1].data[BLCKSZ - 1] = 42;
	relform.relpersistence = RELPERSISTENCE_PERMANENT;
	relform.relkind = RELKIND_RELATION;
	relation_data.rd_rel = &relform;
	relation_data.rd_locator = identity.key.locator;
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

/* Authority and receipt APPLY are separate tests. These wrappers execute the
 * complete original mutation/WAL regions, including the native WAL encoder. */
static void
run_temp_lock(bool versioned)
{
	Relation relation = &relation_data;
	Buffer buffer = 1, vmbuffer = want_vm ? 2 : InvalidBuffer;
	Page page = pages[0].data;
	BlockNumber block = blocks[0];
	HeapTupleData oldtup = tuple;
	TransactionId xmax_lock_old_tuple = 501;
	uint16 infomask_lock_old_tuple = HEAP_XMAX_LOCK_ONLY | HEAP_XMAX_EXCL_LOCK;
	uint16 infomask2_lock_old_tuple = 0;
	CommandId cid = 2;
	bool iscombo = false, cleared_all_frozen = false, cluster_current_mx_recomposed = false;
	uint8 cluster_current_mx_temp_lock_header[SizeofHeapTupleHeader] pg_attribute_aligned(
		MAXIMUM_ALIGNOF);
	bool cluster_page_versioned = versioned;
	RfPageProducerBatchV1 cluster_temp_lock_versions = prepared;

	oldtup.t_data = (HeapTupleHeader)PageGetItem(page, PageGetItemId(page, FirstOffsetNumber));
	(void)cluster_page_versioned;
	(void)cluster_temp_lock_versions;
	temp_locking = true;
#include "test_cluster_heap_update_temp_version.inc"
	temp_locking = false;
}

static void
run_update(bool versioned, bool crosspage, int vm_mode, bool hot)
{
	Relation relation = &relation_data;
	Buffer buffer = 1, newbuf = crosspage ? 3 : 1;
	Buffer vmbuffer = vm_mode == 0 || vm_mode == 3 ? InvalidBuffer : 2;
	Buffer vmbuffer_new = vm_mode >= 2 ? 4 : vmbuffer;
	bool vm_locked = vm_mode == 1 || vm_mode == 2, vm_locked_new = vm_mode >= 2;
	Page page = pages[0].data;
	HeapTupleData oldtup = tuple;
	HeapTuple heaptup = &tuple, newtup = &tuple, old_key_tuple = NULL;
	TransactionId xid = 501, canonical_xid = 501, xmax_old_tuple = 501;
	CommandId cid = 2;
	bool iscombo = false, use_hot_update = hot;
	bool all_visible_cleared = false, all_visible_cleared_new = false;
	bool cluster_itl_old_active = true, cluster_itl_new_active = crosspage;
	bool cluster_current_mx_recomposed = false;
	uint8 cluster_itl_old_slot = 0, cluster_itl_new_slot = 0;
	uint16 infomask_old_tuple = 0, infomask2_old_tuple = 0;
	SCN cluster_itl_old_write_scn = 810, cluster_itl_new_write_scn = 811;
	UBA cluster_itl_uba = InvalidUba_init;
	struct {
		bool active;
	} cluster_current_mx_new_plan = { false };
	uint8 cluster_current_mx_old_header[SizeofHeapTupleHeader] pg_attribute_aligned(
		MAXIMUM_ALIGNOF);
	uint8 cluster_current_mx_new_header[SizeofHeapTupleHeader] pg_attribute_aligned(
		MAXIMUM_ALIGNOF);
	ItemPointerData cluster_current_mx_new_tid = tuple.t_self;
	ItemIdData cluster_current_mx_new_line_pointer = { 0 };
	bool cluster_page_versioned = versioned;
	RfPageProducerBatchV1 cluster_page_versions = prepared;

	oldtup.t_data = (HeapTupleHeader)PageGetItem(page, PageGetItemId(page, FirstOffsetNumber));
	(void)cluster_page_versioned;
	(void)cluster_page_versions;
	updating = true;
#include "test_cluster_heap_update_version.inc"
	updating = false;
}

static void
capture_update(bool versioned, bool crosspage, int vm_mode, bool temp)
{
	Buffer buffer = 1, newbuf = crosspage ? 3 : 1;
	Buffer vmbuffer = vm_mode == 0 || vm_mode == 3 ? InvalidBuffer : 2;
	Buffer vmbuffer_new = vm_mode >= 2 ? 4 : vmbuffer;
	bool vm_locked = vm_mode == 1 || vm_mode == 2, vm_locked_new = vm_mode >= 2;
	bool cluster_page_versioned = versioned;
	ClusterSpaceIdentity cluster_page_identity = identity;
	RfPageProducerBatchV1 cluster_temp_lock_versions = { 0 }, cluster_page_versions = { 0 };
	PGAlignedBlock before[4];

	memcpy(before, pages, sizeof(before));
	if (temp) {
#include "test_cluster_heap_update_temp_capture.inc"
		prepared = cluster_temp_lock_versions;
	} else {
#include "test_cluster_heap_update_capture.inc"
		prepared = cluster_page_versions;
	}
	UT_ASSERT(memcmp(before, pages, sizeof(before)) == 0);
	UT_ASSERT(!prepared.stamped);
}

static void
prepare_update(bool versioned, bool crosspage, int vm_mode, bool temp)
{
	reset(versioned, false, false);
	run_insert(versioned, true, 0);
	want_vm = vm_mode != 0;
	if (want_vm) {
		if (vm_mode != 3)
			PageSetAllVisible(pages[0].data);
		pages[1].data[SizeOfPageHeaderData] = 3;
		if (crosspage) {
			PageSetAllVisible(pages[2].data);
			pages[3].data[SizeOfPageHeaderData] = 3;
		}
	}
	if (vm_mode >= 2)
		blocks[2] = (BLCKSZ - MAXALIGN(SizeOfPageHeaderData)) * 4;
	pages[3].data[BLCKSZ - 1] = 73;
	memset(registered, 0, sizeof(registered));
	memset(dirty, 0, sizeof(dirty));
	memset(&prepared, 0, sizeof(prepared));
	inserts = data_calls = edge_count = edge_token = vm_clears = itl_stamps = 0;
	capture_update(versioned, crosspage, vm_mode, temp);
}

UT_TEST(test_update_temp_lock_has_its_own_heap_and_vm_wal_edge)
{
	int vm;

	for (vm = 0; vm < 2; vm++) {
		prepare_update(true, false, vm, true);
		run_temp_lock(true);
		UT_ASSERT_EQ(inserts, 1);
		UT_ASSERT_EQ(edge_count, vm ? 2 : 1);
		UT_ASSERT_EQ(edge_token, 101);
		UT_ASSERT_EQ(edges[0].before.mutation_token, 100);
		UT_ASSERT_EQ(((PageHeader)pages[0].data)->pd_block_scn, 101);
		UT_ASSERT_EQ(PageGetLSN(pages[0].data), 200);
		if (vm) {
			UT_ASSERT_EQ(register_flags[1], REGBUF_FORCE_IMAGE);
			UT_ASSERT_EQ(((PageHeader)pages[1].data)->pd_block_scn, 101);
			UT_ASSERT_EQ(PageGetLSN(pages[1].data), 200);
			UT_ASSERT_EQ(pages[1].data[SizeOfPageHeaderData], 1);
			UT_ASSERT_EQ(images[1].data[BLCKSZ - 1], 42);
		}
		UT_ASSERT_EQ(CritSectionCount, 0);
	}
}

UT_TEST(test_update_same_page_hot_and_plain_keep_native_wal)
{
	int hot;

	for (hot = 0; hot < 2; hot++) {
		prepare_update(true, false, 1, false);
		run_update(true, false, 1, hot != 0);
		UT_ASSERT_EQ(edge_count, 2);
		UT_ASSERT_EQ(edges[0].block_id, 0);
		UT_ASSERT_EQ(edges[1].block_id, 2);
		UT_ASSERT_EQ(edges[0].before.mutation_token, 100);
		UT_ASSERT_EQ(edge_token, 101);
		UT_ASSERT_EQ(wal_info & XLOG_HEAP_OPMASK, hot ? XLOG_HEAP_HOT_UPDATE : XLOG_HEAP_UPDATE);
		UT_ASSERT((heap_flags & XLH_UPDATE_ITL_DELTA) != 0);
		UT_ASSERT_EQ(itl_stamps, 1);
		UT_ASSERT_EQ(PageGetMaxOffsetNumber(pages[0].data), 2);
		UT_ASSERT_EQ(register_flags[2], REGBUF_FORCE_IMAGE);
		UT_ASSERT_EQ(PageGetLSN(pages[1].data), 200);
		UT_ASSERT_EQ(CritSectionCount, 0);
	}
}

UT_TEST(test_update_cross_page_single_or_two_vm_wal_components)
{
	int vm;

	for (vm = 0; vm < 3; vm++) {
		prepare_update(true, true, vm, false);
		run_update(true, true, vm, false);
		UT_ASSERT_EQ(edge_count, 2 + vm);
		UT_ASSERT_EQ(edge_token, 101);
		UT_ASSERT_EQ(edges[0].before.mutation_token, 41);
		UT_ASSERT_EQ(edges[1].before.mutation_token, 100);
		UT_ASSERT_EQ(registered_buffers[0], 3);
		UT_ASSERT_EQ(registered_buffers[1], 1);
		UT_ASSERT_EQ(((PageHeader)pages[0].data)->pd_block_scn, 101);
		UT_ASSERT_EQ(((PageHeader)pages[2].data)->pd_block_scn, 101);
		UT_ASSERT_EQ(itl_stamps, 2);
		UT_ASSERT_EQ(PageGetLSN(pages[2].data), 200);
		if (vm > 0) {
			UT_ASSERT_EQ(register_flags[2], REGBUF_FORCE_IMAGE);
			UT_ASSERT_EQ(PageGetLSN(pages[1].data), 200);
			UT_ASSERT_EQ(images[2].data[BLCKSZ - 1], 42);
		}
		if (vm == 2) {
			UT_ASSERT_EQ(register_flags[3], REGBUF_FORCE_IMAGE);
			UT_ASSERT_EQ(PageGetLSN(pages[3].data), 200);
			UT_ASSERT_EQ(images[3].data[BLCKSZ - 1], 73);
		}
		UT_ASSERT_EQ(CritSectionCount, 0);
	}
}

UT_TEST(test_legacy_update_keeps_implicit_vm_and_native_tokens)
{
	prepare_update(false, true, 2, false);
	run_update(false, true, 2, false);
	UT_ASSERT_EQ(edge_count, 0);
	UT_ASSERT_EQ(inserts, 1);
	UT_ASSERT(!registered[2] && !registered[3]);
	UT_ASSERT_EQ(((PageHeader)pages[0].data)->pd_block_scn, 17);
	UT_ASSERT_EQ(((PageHeader)pages[2].data)->pd_block_scn, 41);
	prepare_update(false, false, 1, true);
	run_temp_lock(false);
	UT_ASSERT_EQ(edge_count, 0);
	UT_ASSERT(!registered[1]);
}

UT_TEST(test_update_new_vm_only_keeps_native_block_id_gap)
{
	prepare_update(true, true, 3, false);
	run_update(true, true, 3, false);
	UT_ASSERT_EQ(edge_count, 3);
	UT_ASSERT_EQ(edges[2].block_id, 3);
	UT_ASSERT_EQ(edges[2].component_ordinal, 2);
	UT_ASSERT_EQ(edges[2].before.mutation_token, 61);
	UT_ASSERT(!registered[2] && registered[3]);
	UT_ASSERT_EQ(register_flags[3], REGBUF_FORCE_IMAGE);
	UT_ASSERT_EQ(PageGetLSN(pages[3].data), 200);
	UT_ASSERT_EQ(((PageHeader)pages[3].data)->pd_block_scn, 101);
	UT_ASSERT_EQ(((PageHeader)pages[1].data)->pd_block_scn, 31);
	UT_ASSERT_EQ(vm_clears, 1);
}

UT_TEST(test_final_update_recaptures_after_temp_lock_and_already_clear_vm)
{
	prepare_update(true, false, 1, true);
	run_temp_lock(true);
	/* Keep the actual TEMP_LOCK page bytes, not the old prepared snapshot. */
	pages[1].data[SizeOfPageHeaderData] = 0;
	memset(registered, 0, sizeof(registered));
	memset(dirty, 0, sizeof(dirty));
	data_calls = edge_count = 0;
	capture_update(true, false, 1, false);
	run_update(true, false, 1, true);
	UT_ASSERT_EQ(inserts, 2);
	UT_ASSERT_EQ(edge_token, 102);
	UT_ASSERT_EQ(edges[0].before.mutation_token, 101);
	UT_ASSERT_EQ(edges[1].before.mutation_token, 101);
	UT_ASSERT_EQ(((PageHeader)pages[0].data)->pd_block_scn, 102);
	UT_ASSERT_EQ(((PageHeader)pages[1].data)->pd_block_scn, 102);
	UT_ASSERT(dirty[1] && registered[2]);
	UT_ASSERT_EQ(PageGetLSN(pages[1].data), 200);
}

int
main(void)
{
	UT_PLAN(14);
	UT_RUN(test_native_insert_publishes_heap_edge_with_itl_delta);
	UT_RUN(test_native_insert_covers_vm_even_when_bit_was_already_clear);
	UT_RUN(test_legacy_insert_keeps_existing_wal_shape);
	UT_RUN(test_buffer_capture_has_no_mutation_before_apply);
	UT_RUN(test_capture_refuses_whole_batch_without_token_or_byte_change);
	UT_RUN(test_same_transaction_second_insert_has_distinct_before_and_result);
	UT_RUN(test_native_delete_has_heap_vm_edge_and_retains_recomposed_header);
	UT_RUN(test_legacy_delete_keeps_implicit_vm_wal_shape);
	UT_RUN(test_update_temp_lock_has_its_own_heap_and_vm_wal_edge);
	UT_RUN(test_update_same_page_hot_and_plain_keep_native_wal);
	UT_RUN(test_update_cross_page_single_or_two_vm_wal_components);
	UT_RUN(test_legacy_update_keeps_implicit_vm_and_native_tokens);
	UT_RUN(test_update_new_vm_only_keeps_native_block_id_gap);
	UT_RUN(test_final_update_recaptures_after_temp_lock_and_already_clear_vm);
	UT_DONE();
	return ut_failed_count ? 1 : 0;
}
