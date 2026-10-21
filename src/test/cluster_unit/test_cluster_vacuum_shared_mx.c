/*-------------------------------------------------------------------------
 *
 * test_cluster_vacuum_shared_mx.c
 *    Execute real VACUUM scan and cutoff consumers over retained MX pages.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *    src/test/cluster_unit/test_cluster_vacuum_shared_mx.c
 *
 * NOTES
 *    Native page processing and storage are leaves. The production scan loop
 *    must avoid those leaves for a retained page without discarding earlier
 *    dead items or publishing new freeze cutoffs. Physical coverage is in TAP.
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <setjmp.h>

#include "access/heapam.h"
#include "access/htup_details.h"
#include "access/multixact.h"
#include "access/visibilitymap.h"
#include "access/xlog.h"
#include "catalog/pg_control.h"
#include "cluster/cluster_heap_freeze.h"
#include "cluster/cluster_space_storage.h"
#include "commands/progress.h"
#include "commands/vacuum.h"
#include "storage/bufmgr.h"
#include "utils/rel.h"
#include "unit_test.h"

UT_DEFINE_GLOBALS();

/* Native wrap comparisons are not the consumer under test. */
bool
TransactionIdPrecedesOrEquals(TransactionId left, TransactionId right)
{
	return (int32)(left - right) <= 0;
}

bool
MultiXactIdPrecedesOrEquals(MultiXactId left, MultiXactId right)
{
	return (int32)(left - right) <= 0;
}

bool cluster_shared_config = true;
int wal_level = WAL_LEVEL_REPLICA;
static PGAlignedBlock fixture_pages[3];
static bool fixture_peer, fixture_cleanup, fixture_identity, fixture_read_error;
static bool fixture_storage;
#define cluster_storage_mode_enabled() fixture_storage
#include "test_cluster_heap_maintenance_policy.inc"
static int fixture_smgr, fixture_pins, fixture_locks;
static unsigned fixture_prune, fixture_noprune, fixture_fsm_writes;
static unsigned fixture_vm_writes, fixture_dirties, fixture_consumed_dead;
static jmp_buf fixture_error;

void
ExceptionalCondition(const char *condition, const char *file, int line)
{
	(void)condition;
	(void)file;
	(void)line;
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

void
errfinish(const char *file, int line, const char *function)
{
	(void)file;
	(void)line;
	(void)function;
	longjmp(fixture_error, 1);
}

/* Explicit maintenance intents are outside this page-scan fixture. */
#define cluster_heap_freeze_request_maintenance() false
#include "test_cluster_vacuum_shared_mx.inc"

static LVRelState state;
static RelationData relation;
static FormData_pg_class form;
static union {
	max_align_t alignment;
	char bytes[sizeof(VacDeadItems) + 2 * MaxHeapTuplesPerPage * sizeof(ItemPointerData)];
} dead_storage;

static void
make_page(unsigned block, bool multi, MultiXactId mx)
{
	Page page = (Page)fixture_pages[block].data;
	PageHeader header = (PageHeader)page;

	memset(page, 0, BLCKSZ);
	header->pd_lower = SizeOfPageHeaderData + 5 * sizeof(ItemIdData);
	header->pd_upper = BLCKSZ - 5 * MAXALIGN(SizeofHeapTupleHeader);
	header->pd_special = BLCKSZ;
	header->pd_pagesize_version = BLCKSZ | PG_PAGE_LAYOUT_VERSION;
	for (unsigned i = 1; i <= 5; i++) {
		ItemId item = PageGetItemId(page, i);
		HeapTupleHeader tuple;

		ItemIdSetNormal(item, BLCKSZ - i * MAXALIGN(SizeofHeapTupleHeader), SizeofHeapTupleHeader);
		tuple = (HeapTupleHeader)PageGetItem(page, item);
		tuple->t_hoff = SizeofHeapTupleHeader;
		HeapTupleHeaderSetXmin(tuple, 7);
		HeapTupleHeaderSetXmax(tuple, mx);
		if (multi && i == 2)
			tuple->t_infomask = HEAP_XMAX_IS_MULTI | HEAP_XMAX_LOCK_ONLY;
	}
	ItemIdSetDead(PageGetItemId(page, 3));
	ItemIdSetRedirect(PageGetItemId(page, 4), 1);
}

static void
setup(bool cleanup)
{
	memset(&state, 0, sizeof(state));
	memset(&relation, 0, sizeof(relation));
	memset(&form, 0, sizeof(form));
	memset(&dead_storage, 0, sizeof(dead_storage));
	fixture_storage = fixture_peer = fixture_identity = cluster_shared_config = true;
	fixture_smgr = 1;
	fixture_cleanup = cleanup;
	fixture_read_error = false;
	fixture_pins = fixture_locks = 0;
	fixture_prune = fixture_noprune = fixture_fsm_writes = 0;
	fixture_vm_writes = fixture_dirties = fixture_consumed_dead = 0;
	state.rel = &relation;
	relation.rd_rel = &form;
	form.relpersistence = RELPERSISTENCE_PERMANENT;
	fixture_bind_relation(&state, &relation);
	/* The identity lookup leaf only returns success; supply its live copy. */
	state.identity.state = CLUSTER_SPACE_IDENTITY_LIVE;
	state.identity.key.system_identifier = 101;
	state.identity.key.database_incarnation = 202;
	state.identity.key.locator.spcOid = 303;
	state.identity.key.locator.dbOid = 404;
	state.identity.key.locator.relNumber = 505;
	memset(state.identity.key.storage_uuid, 0x61, sizeof(state.identity.key.storage_uuid));
	memset(state.identity.incarnation, 0x72, sizeof(state.identity.incarnation));
	state.identity.sequence = 606;
	state.identity.operation = 707;
	state.rel_pages = 1;
	state.nindexes = 1;
	state.dead_items = (VacDeadItems *)dead_storage.bytes;
	state.dead_items->max_items = 2 * MaxHeapTuplesPerPage;
	state.NewRelfrozenXid = state.NewRelminMxid = 100;
	state.cutoffs.relfrozenxid = state.cutoffs.relminmxid = 20;
	state.cutoffs.FreezeLimit = state.cutoffs.MultiXactCutoff = 50;
	state.cutoffs.OldestXmin = state.cutoffs.OldestMxact = 200;
	make_page(0, true, 33);
}

static unsigned
deferred_pages(void)
{
#ifdef FIXTURE_HAS_MX_DEFER
	return state.shared_mx_deferred_pages;
#else
	/* The exact baseline has no preservation accounting. */
	return 0;
#endif
}

static int64
deferred_tuples(void)
{
#ifdef FIXTURE_HAS_MX_DEFER
	return state.shared_mx_deferred_tuples;
#else
	return 0;
#endif
}

UT_TEST(share_scan_preserves_every_reference_before_native_consumer)
{
	PGAlignedBlock before;

	setup(false);
	before = fixture_pages[0];
	lazy_scan_heap(&state);
	UT_ASSERT_EQ(fixture_noprune, 0);
	UT_ASSERT_EQ(fixture_prune, 0);
	UT_ASSERT_EQ(deferred_pages(), 1);
	UT_ASSERT_EQ(deferred_tuples(), 3);
	UT_ASSERT_EQ(state.live_tuples, 3);
	UT_ASSERT_EQ(state.nonempty_pages, 1);
	UT_ASSERT_EQ(memcmp(&before, &fixture_pages[0], sizeof(before)), 0);
	UT_ASSERT_EQ(fixture_fsm_writes + fixture_vm_writes + fixture_dirties, 0);
	UT_ASSERT_EQ(fixture_locks + fixture_pins, 0);
}

UT_TEST(cleanup_scan_preserves_before_prune_and_freeze)
{
	setup(true);
	lazy_scan_heap(&state);
	UT_ASSERT_EQ(fixture_prune, 0);
	UT_ASSERT_EQ(deferred_pages(), 1);
	UT_ASSERT_EQ(deferred_tuples(), 3);
	UT_ASSERT_EQ(fixture_fsm_writes + fixture_vm_writes + fixture_dirties, 0);
	UT_ASSERT_EQ(fixture_locks + fixture_pins, 0);
}

UT_TEST(raw_multi_value_never_selects_local_reader)
{
	const MultiXactId values[] = { 0, 1, 31, 32, 33, 34, 1000000, UINT32_MAX };

	for (unsigned x = 0; x < lengthof(values); x++)
		for (unsigned cleanup = 0; cleanup < 2; cleanup++) {
			setup(cleanup);
			make_page(0, true, values[x]);
			lazy_scan_heap(&state);
			UT_ASSERT_EQ(fixture_prune + fixture_noprune, 0);
			UT_ASSERT_EQ(deferred_pages(), 1);
		}
}

UT_TEST(both_cutoffs_remain_unpublished_after_a_preserved_page)
{
	for (unsigned aggressive = 0; aggressive < 2; aggressive++) {
		setup(true);
		state.aggressive = aggressive;
		lazy_scan_heap(&state);
		fixture_publish_cutoffs(&state);
		UT_ASSERT_EQ(state.NewRelfrozenXid, InvalidTransactionId);
		UT_ASSERT_EQ(state.NewRelminMxid, InvalidMultiXactId);
	}
}

UT_TEST(nontarget_modes_retain_the_original_native_path)
{
	for (unsigned mode = 0; mode < 5; mode++)
		for (unsigned cleanup = 0; cleanup < 2; cleanup++) {
			setup(cleanup);
			if (mode == 0)
				fixture_peer = false;
			if (mode == 1)
				cluster_shared_config = false;
			if (mode == 2)
				fixture_smgr = 0;
			if (mode == 3)
				form.relpersistence = RELPERSISTENCE_TEMP;
			if (mode == 4)
				fixture_storage = fixture_peer = false;
			fixture_bind_relation(&state, &relation);
			lazy_scan_heap(&state);
			fixture_publish_cutoffs(&state);
			UT_ASSERT_EQ(fixture_prune + fixture_noprune, 1);
			UT_ASSERT_EQ(deferred_pages(), 0);
			UT_ASSERT_EQ(state.NewRelfrozenXid, state.shared_maintenance_restricted ? InvalidTransactionId : 100);
			UT_ASSERT_EQ(state.NewRelminMxid, state.shared_maintenance_restricted ? InvalidMultiXactId : 100);
		}
}

UT_TEST(no_multi_and_empty_pages_do_not_acquire_a_preservation_claim)
{
	setup(true);
	make_page(0, false, 33);
	lazy_scan_heap(&state);
	fixture_publish_cutoffs(&state);
	UT_ASSERT_EQ(fixture_prune, 1);
	UT_ASSERT_EQ(deferred_pages(), 0);
	UT_ASSERT_EQ(state.NewRelfrozenXid, state.shared_maintenance_restricted ? InvalidTransactionId : 100);
	setup(false);
	((PageHeader)fixture_pages[0].data)->pd_lower = SizeOfPageHeaderData;
	lazy_scan_heap(&state);
	UT_ASSERT_EQ(fixture_noprune, 0);
	UT_ASSERT_EQ(deferred_pages(), 0);
}

UT_TEST(other_pages_dead_items_and_tail_are_preserved_independently)
{
	for (unsigned indexes = 0; indexes < 2; indexes++) {
		setup(true);
		state.nindexes = indexes;
		state.rel_pages = 2;
		make_page(0, false, 20);
		make_page(1, true, 33);
		if (indexes)
			state.dead_items->num_items = 1;
		lazy_scan_heap(&state);
		UT_ASSERT_EQ(fixture_prune, 1);
		UT_ASSERT_EQ(fixture_consumed_dead, indexes);
		UT_ASSERT_EQ(deferred_pages(), 1);
		UT_ASSERT_EQ(state.nonempty_pages, 2);
		UT_ASSERT_EQ(fixture_fsm_writes, 1);
	}
}

UT_TEST(identity_and_io_failures_are_not_converted_to_preservation)
{
	setup(true);
	fixture_identity = false;
	if (setjmp(fixture_error) == 0) {
		fixture_bind_relation(&state, &relation);
		UT_ASSERT(false);
	}
	UT_ASSERT_EQ(deferred_pages(), 0);
	setup(false);
	fixture_read_error = true;
	if (setjmp(fixture_error) == 0) {
		lazy_scan_heap(&state);
		UT_ASSERT(false);
	}
	UT_ASSERT_EQ(fixture_prune + fixture_noprune, 0);
	UT_ASSERT_EQ(deferred_pages(), 0);
	UT_ASSERT_EQ(fixture_locks + fixture_pins, 0);
}

static const ClusterHeapFreezePageInputV1 *
captured_input(void)
{
#ifdef FIXTURE_HAS_FREEZE_INPUT
	return state.shared_freeze_input_valid ? &state.shared_freeze_input : NULL;
#else
	return NULL;
#endif
}

static void
make_itl_page(void)
{
	Page page = (Page) fixture_pages[0].data;
	PageHeader header = (PageHeader) page;
	ClusterItlSlotData *slots;

	make_page(0, true, 33);
	for (OffsetNumber i = 1; i <= 5; i++)
	{
		ItemId item = PageGetItemId(page, i);
		unsigned offset;

		if (!ItemIdIsNormal(item))
			continue;
		offset = ItemIdGetOffset(item) - CLUSTER_ITL_SPECIAL_SIZE;
		memmove(page + offset, PageGetItem(page, item), ItemIdGetLength(item));
		ItemIdSetNormal(item, offset, SizeofHeapTupleHeader);
	}
	header->pd_upper -= CLUSTER_ITL_SPECIAL_SIZE;
	header->pd_special -= CLUSTER_ITL_SPECIAL_SIZE;
	header->pd_flags |= PD_HAS_ITL;
	header->pd_block_scn = 0x123456789ULL;
	PageXLogRecPtrSet(header->pd_lsn, 0x987654321ULL);
	memset(PageGetSpecialPointer(page), 0x39, CLUSTER_ITL_SPECIAL_SIZE);
	slots = ClusterPageGetItlSlots(page);
	slots[0].xid = 77;
	slots[0].wrap = 0xfffe;
	slots[1].xid = 77;
	slots[1].wrap = 2;
}

UT_TEST(capture_owns_exact_physical_carrier_bytes)
{
	ClusterHeapFreezePageInputV1 input;
	PGAlignedBlock before;
	ClusterSpaceIdentity identity;
	Page page;
	bool captured;

	setup(true);
	make_itl_page();
	page = (Page) fixture_pages[0].data;
	before = fixture_pages[0];
	identity = state.identity;
	memset(&input, 0xa5, sizeof(input));
	captured = cluster_heap_freeze_capture_page_v1(&identity, 17, page, &input);
	UT_ASSERT(captured);
	if (!captured)
		return;
	UT_ASSERT_EQ(input.block, 17);
	UT_ASSERT_EQ(input.max_offset, 5);
	UT_ASSERT_EQ(input.normal_tuples, 3);
	UT_ASSERT_EQ(input.multi_tuples, 1);
	UT_ASSERT_EQ(memcmp(&before, &input.image, sizeof(before)), 0);
	UT_ASSERT_EQ(memcmp(&before, &fixture_pages[0], sizeof(before)), 0);
	UT_ASSERT_EQ(input.identity.key.system_identifier, identity.key.system_identifier);
	UT_ASSERT_EQ(input.identity.key.database_incarnation, identity.key.database_incarnation);
	UT_ASSERT_EQ(input.identity.key.locator.relNumber, identity.key.locator.relNumber);
	UT_ASSERT_EQ(input.identity.sequence, identity.sequence);
	UT_ASSERT_EQ(input.identity.operation, identity.operation);
	UT_ASSERT_EQ(memcmp(input.identity.key.storage_uuid, identity.key.storage_uuid, 16), 0);
	UT_ASSERT_EQ(memcmp(input.identity.incarnation, identity.incarnation, 16), 0);
	memset(page, 0, BLCKSZ);
	memset(&identity, 0, sizeof(identity));
	UT_ASSERT_EQ(memcmp(&before, &input.image, sizeof(before)), 0);
	UT_ASSERT_EQ(input.identity.key.system_identifier, 101);
	UT_ASSERT_EQ(ClusterPageGetItlSlots((Page) input.image.data)[0].wrap, 0xfffe);
	UT_ASSERT_EQ(ClusterPageGetItlSlots((Page) input.image.data)[1].wrap, 2);
}

UT_TEST(capture_rejects_malformed_inputs_without_partial_output)
{
	ClusterHeapFreezePageInputV1 input, before;

	for (unsigned bad = 0; bad < 13; bad++)
	{
		Page page;
		PageHeader header;
		ItemId item;

		setup(true);
		page = (Page) fixture_pages[0].data;
		header = (PageHeader) page;
		item = PageGetItemId(page, 1);
		memset(&input, 0xa5, sizeof(input));
		before = input;
		switch (bad)
		{
			case 0: header->pd_lower = SizeOfPageHeaderData - 1; break;
			case 1: header->pd_upper = header->pd_lower - 1; break;
			case 2: header->pd_special = BLCKSZ + 1; break;
			case 3: header->pd_pagesize_version = 0; break;
			case 4: header->pd_lower++; break;
			case 5: ItemIdSetNormal(item, header->pd_upper - 8, 23); break;
			case 6: ItemIdSetNormal(item, BLCKSZ - 8, 23); break;
			case 7: ItemIdSetNormal(item, header->pd_upper, 1); break;
			case 8: ((HeapTupleHeader) PageGetItem(page, item))->t_hoff = 1; break;
			case 9: ((HeapTupleHeader) PageGetItem(page, item))->t_hoff = 255; break;
			case 10: header->pd_flags |= PD_HAS_ITL; break;
			case 11:
				header->pd_lower = SizeOfPageHeaderData + (MaxHeapTuplesPerPage + 1) * sizeof(ItemIdData);
				break;
			case 12: state.identity.state = CLUSTER_SPACE_IDENTITY_TOMBSTONED; break;
		}
		UT_ASSERT(!cluster_heap_freeze_capture_page_v1(&state.identity, 0, page, &input));
		UT_ASSERT_EQ(memcmp(&input, &before, sizeof(input)), 0);
	}
	UT_ASSERT(!cluster_heap_freeze_capture_page_v1(NULL, 0, (Page) fixture_pages[0].data, &input));
	UT_ASSERT(!cluster_heap_freeze_capture_page_v1(&state.identity, InvalidBlockNumber,
													(Page) fixture_pages[0].data, &input));
	UT_ASSERT(!cluster_heap_freeze_capture_page_v1(&state.identity, 0, NULL, &input));
	UT_ASSERT(!cluster_heap_freeze_capture_page_v1(&state.identity, 0,
													(Page) fixture_pages[0].data, NULL));
}

UT_TEST(real_scan_captures_share_cleanup_and_nonmulti_inputs)
{
	for (unsigned cleanup = 0; cleanup < 2; cleanup++)
		for (unsigned multi = 0; multi < 2; multi++)
		{
			const ClusterHeapFreezePageInputV1 *input;
			PGAlignedBlock before;

			setup(cleanup);
			make_page(0, multi, 33);
			before = fixture_pages[0];
			lazy_scan_heap(&state);
			input = captured_input();
			UT_ASSERT(input != NULL);
			if (input == NULL)
				continue;
			UT_ASSERT_EQ(input->block, 0);
			UT_ASSERT_EQ(input->normal_tuples, 3);
			UT_ASSERT_EQ(input->multi_tuples, multi);
			UT_ASSERT_EQ(memcmp(&input->image, &before, sizeof(before)), 0);
		}
}

UT_TEST(next_empty_or_nontarget_page_cannot_reuse_input)
{
	const ClusterHeapFreezePageInputV1 *input;

	setup(true);
	lazy_scan_heap(&state);
	input = captured_input();
	UT_ASSERT(input != NULL);
	state.rel_pages = 2;
	make_page(1, false, 33);
	((PageHeader) fixture_pages[1].data)->pd_lower = SizeOfPageHeaderData;
	lazy_scan_heap(&state);
	UT_ASSERT(captured_input() == NULL);
	setup(true);
	lazy_scan_heap(&state);
	fixture_peer = false;
	lazy_scan_heap(&state);
	UT_ASSERT(captured_input() == NULL);
}

UT_TEST(real_capture_failure_is_a_hard_error)
{
	setup(true);
	((PageHeader) fixture_pages[0].data)->pd_upper = SizeOfPageHeaderData;
	if (setjmp(fixture_error) == 0)
	{
		lazy_scan_heap(&state);
		UT_ASSERT(false);
	}
	UT_ASSERT_EQ(fixture_prune + fixture_noprune, 0);
	UT_ASSERT_EQ(deferred_pages(), 0);
	UT_ASSERT(captured_input() == NULL);
}

static const ClusterHeapFreezePageBatchV1 *
prepared_batch(void)
{
#ifdef FIXTURE_HAS_FREEZE_BATCH
	return state.shared_freeze_input_valid ? &state.shared_freeze_batch : NULL;
#else
	return NULL;
#endif
}

UT_TEST(prepare_preserves_raw_headers_and_owns_physical_rows)
{
	ClusterHeapFreezePageInputV1 input;
	ClusterHeapFreezePageBatchV1 batch, before;
	const OffsetNumber offsets[] = {1, 2, 5};
	Page page;
	bool ok;

	setup(true);
	make_itl_page();
	page = (Page) fixture_pages[0].data;
	for (unsigned i = 0; i < lengthof(offsets); i++)
	{
		HeapTupleHeader tuple = (HeapTupleHeader) PageGetItem(page, PageGetItemId(page, offsets[i]));

		HeapTupleHeaderSetXmin(tuple, 77 + i);
		tuple->t_infomask |= HEAP_XMIN_FROZEN;
		tuple->t_infomask2 = i + 1;
		tuple->t_itl_slot_idx = i;
		ItemPointerSet(&tuple->t_ctid, 91, offsets[i]);
	}
	UT_ASSERT(cluster_heap_freeze_capture_page_v1(&state.identity, 17, page, &input));
	ok = cluster_heap_freeze_prepare_page_v1(&input, &batch);
	UT_ASSERT(ok);
	if (!ok)
		return;
	UT_ASSERT_EQ(batch.block, 17);
	UT_ASSERT_EQ(batch.page_lsn, 0x987654321ULL);
	UT_ASSERT_EQ(batch.page_version, 0x123456789ULL);
	UT_ASSERT_EQ(memcmp(&batch.identity, &input.identity, sizeof(batch.identity)), 0);
	UT_ASSERT_EQ(batch.tuple_count, 3);
	UT_ASSERT_EQ(batch.multi_tuples, 1);
	for (unsigned i = 0; i < lengthof(offsets); i++)
	{
		const ClusterHeapFreezeTupleInputV1 *row = &batch.tuples[i];
		ItemId item = PageGetItemId(page, offsets[i]);
		HeapTupleHeader tuple = (HeapTupleHeader) PageGetItem(page, item);

		UT_ASSERT_EQ(row->offset, offsets[i]);
		UT_ASSERT_EQ(row->tuple_offset, ItemIdGetOffset(item));
		UT_ASSERT_EQ(row->tuple_length, ItemIdGetLength(item));
		UT_ASSERT_EQ(row->raw_xmin, 77 + i);
		UT_ASSERT_EQ(row->raw_xmax, 33);
		UT_ASSERT_EQ(row->infomask, tuple->t_infomask);
		UT_ASSERT_EQ(row->infomask2, tuple->t_infomask2);
		UT_ASSERT_EQ(row->header_length, tuple->t_hoff);
		UT_ASSERT_EQ(memcmp(&row->ctid, &tuple->t_ctid, sizeof(row->ctid)), 0);
		UT_ASSERT_EQ(row->itl_slot_index, i);
		UT_ASSERT(row->has_carrier);
		UT_ASSERT_EQ(memcmp(&row->carrier, &ClusterPageGetItlSlots(page)[i], sizeof(row->carrier)), 0);
	}
	before = batch;
	memset(&input, 0, sizeof(input));
	memset(page, 0, BLCKSZ);
	UT_ASSERT_EQ(memcmp(&batch, &before, sizeof(batch)), 0);
}

UT_TEST(prepare_never_aliases_creator_and_current_itl_carriers)
{
	ClusterHeapFreezePageInputV1 input;
	ClusterHeapFreezePageBatchV1 batch;
	const OffsetNumber offsets[] = {1, 2, 5};
	Page page;
	bool ok;

	setup(true);
	make_itl_page();
	page = (Page) fixture_pages[0].data;
	for (unsigned i = 0; i < lengthof(offsets); i++)
	{
		HeapTupleHeader tuple = (HeapTupleHeader) PageGetItem(page, PageGetItemId(page, offsets[i]));

		HeapTupleHeaderSetXmin(tuple, 77);
		tuple->t_itl_slot_idx = i;
	}
	ClusterPageGetItlSlots(page)[2].xid = 99;
	ClusterPageGetItlSlots(page)[2].flags = ITL_FLAG_FREE;
	UT_ASSERT(cluster_heap_freeze_capture_page_v1(&state.identity, 0, page, &input));
	ok = cluster_heap_freeze_prepare_page_v1(&input, &batch);
	UT_ASSERT(ok);
	if (!ok)
		return;
	UT_ASSERT_EQ(batch.tuple_count, 3);
	UT_ASSERT_EQ(batch.tuples[0].carrier.wrap, 0xfffe);
	UT_ASSERT_EQ(batch.tuples[1].carrier.wrap, 2);
	UT_ASSERT_EQ(batch.tuples[2].raw_xmin, 77);
	UT_ASSERT_EQ(batch.tuples[2].carrier.xid, 99);
	UT_ASSERT_EQ(batch.tuples[2].carrier.flags, ITL_FLAG_FREE);
	UT_ASSERT(batch.tuples[2].has_carrier);
}

UT_TEST(prepare_keeps_rows_without_an_addressable_itl)
{
	const uint8 indexes[] = {CLUSTER_ITL_SLOT_UNALLOCATED, CLUSTER_ITL_INITRANS_DEFAULT, 254, 0};
	const ClusterItlSlotData zero = {0};

	for (unsigned mode = 0; mode < lengthof(indexes); mode++)
	{
		ClusterHeapFreezePageInputV1 input;
		ClusterHeapFreezePageBatchV1 batch;
		const OffsetNumber offsets[] = {1, 2, 5};
		Page page;
		bool ok;

		setup(true);
		if (mode != 3)
			make_itl_page();
		page = (Page) fixture_pages[0].data;
		for (unsigned i = 0; i < lengthof(offsets); i++)
			((HeapTupleHeader) PageGetItem(page, PageGetItemId(page, offsets[i])))->t_itl_slot_idx = indexes[mode];
		UT_ASSERT(cluster_heap_freeze_capture_page_v1(&state.identity, 0, page, &input));
		ok = cluster_heap_freeze_prepare_page_v1(&input, &batch);
		UT_ASSERT(ok);
		if (!ok)
			continue;
		UT_ASSERT_EQ(batch.tuple_count, 3);
		for (unsigned i = 0; i < lengthof(offsets); i++)
		{
			UT_ASSERT_EQ(batch.tuples[i].itl_slot_index, indexes[mode]);
			UT_ASSERT(!batch.tuples[i].has_carrier);
			UT_ASSERT_EQ(memcmp(&batch.tuples[i].carrier, &zero, sizeof(zero)), 0);
		}
	}
}

UT_TEST(prepare_is_bounded_at_maximum_and_empty_page)
{
	ClusterHeapFreezePageInputV1 input;
	ClusterHeapFreezePageBatchV1 batch;
	Page page;
	PageHeader header;
	bool ok;

	setup(true);
	page = (Page) fixture_pages[0].data;
	memset(page, 0, BLCKSZ);
	header = (PageHeader) page;
	header->pd_lower = SizeOfPageHeaderData + MaxHeapTuplesPerPage * sizeof(ItemIdData);
	header->pd_upper = BLCKSZ - MaxHeapTuplesPerPage * MAXALIGN(SizeofHeapTupleHeader);
	header->pd_special = BLCKSZ;
	header->pd_pagesize_version = BLCKSZ | PG_PAGE_LAYOUT_VERSION;
	for (OffsetNumber i = 1; i <= MaxHeapTuplesPerPage; i++)
	{
		ItemId item = PageGetItemId(page, i);
		HeapTupleHeader tuple;

		ItemIdSetNormal(item, BLCKSZ - i * MAXALIGN(SizeofHeapTupleHeader), SizeofHeapTupleHeader);
		tuple = (HeapTupleHeader) PageGetItem(page, item);
		tuple->t_hoff = SizeofHeapTupleHeader;
		tuple->t_itl_slot_idx = CLUSTER_ITL_SLOT_UNALLOCATED;
	}
	UT_ASSERT(cluster_heap_freeze_capture_page_v1(&state.identity, 0, page, &input));
	ok = cluster_heap_freeze_prepare_page_v1(&input, &batch);
	UT_ASSERT(ok);
	if (ok)
	{
		UT_ASSERT_EQ(batch.tuple_count, MaxHeapTuplesPerPage);
		UT_ASSERT_EQ(batch.tuples[MaxHeapTuplesPerPage - 1].offset, MaxHeapTuplesPerPage);
	}
	header->pd_lower = SizeOfPageHeaderData;
	UT_ASSERT(cluster_heap_freeze_capture_page_v1(&state.identity, 0, page, &input));
	ok = cluster_heap_freeze_prepare_page_v1(&input, &batch);
	UT_ASSERT(ok);
	if (ok)
		UT_ASSERT_EQ(batch.tuple_count, 0);
}

UT_TEST(prepare_rejects_inconsistent_inputs_without_partial_output)
{
	ClusterHeapFreezePageInputV1 input;
	ClusterHeapFreezePageBatchV1 batch, before;

	setup(true);
	for (unsigned bad = 0; bad < 9; bad++)
	{
		UT_ASSERT(cluster_heap_freeze_capture_page_v1(&state.identity, 0, (Page) fixture_pages[0].data, &input));
		switch (bad)
		{
			case 0: input.identity.state = CLUSTER_SPACE_IDENTITY_TOMBSTONED; break;
			case 1: input.block = InvalidBlockNumber; break;
			case 2: input.max_offset++; break;
			case 3: input.normal_tuples--; break;
			case 4: input.multi_tuples++; break;
			case 5: ((PageHeader) input.image.data)->pd_lower++; break;
			case 6: ((PageHeader) input.image.data)->pd_upper = BLCKSZ; break;
			case 7: ((PageHeader) input.image.data)->pd_flags |= PD_HAS_ITL; break;
			case 8: ((PageHeader) input.image.data)->pd_lower = SizeOfPageHeaderData + (MaxHeapTuplesPerPage + 1) * sizeof(ItemIdData); break;
		}
		memset(&batch, 0xa5, sizeof(batch));
		before = batch;
		UT_ASSERT(!cluster_heap_freeze_prepare_page_v1(&input, &batch));
		UT_ASSERT_EQ(memcmp(&batch, &before, sizeof(batch)), 0);
	}
	UT_ASSERT(!cluster_heap_freeze_prepare_page_v1(NULL, &batch));
	UT_ASSERT_EQ(memcmp(&batch, &before, sizeof(batch)), 0);
	UT_ASSERT(!cluster_heap_freeze_prepare_page_v1(&input, NULL));
}

UT_TEST(real_scan_consumes_prepared_rows_under_both_original_locks)
{
	for (unsigned cleanup = 0; cleanup < 2; cleanup++)
		for (unsigned multi = 0; multi < 2; multi++)
		{
			const ClusterHeapFreezePageBatchV1 *batch;

			setup(cleanup);
			make_page(0, multi, 33);
			lazy_scan_heap(&state);
			batch = prepared_batch();
			UT_ASSERT(batch != NULL);
			if (batch == NULL)
				continue;
			UT_ASSERT_EQ(batch->block, 0);
			UT_ASSERT_EQ(batch->tuple_count, 3);
			UT_ASSERT_EQ(batch->multi_tuples, multi);
			UT_ASSERT_EQ(batch->tuples[2].offset, 5);
			UT_ASSERT_EQ(deferred_tuples(), multi ? batch->tuple_count : 0);
			UT_ASSERT_EQ(fixture_locks + fixture_pins, 0);
		}
}

UT_TEST(real_scan_replaces_and_invalidates_prepared_batch)
{
	const ClusterHeapFreezePageBatchV1 *batch;

	setup(true);
	state.rel_pages = 2;
	make_page(1, true, 123);
	lazy_scan_heap(&state);
	batch = prepared_batch();
	UT_ASSERT(batch != NULL);
	if (batch != NULL)
	{
		UT_ASSERT_EQ(batch->block, 1);
		UT_ASSERT_EQ(batch->tuples[1].raw_xmax, 123);
	}
	((PageHeader) fixture_pages[1].data)->pd_lower = SizeOfPageHeaderData;
	lazy_scan_heap(&state);
	UT_ASSERT(prepared_batch() == NULL);
	setup(false);
	lazy_scan_heap(&state);
	fixture_peer = false;
	lazy_scan_heap(&state);
	UT_ASSERT(prepared_batch() == NULL);
}

int
main(void)
{
	UT_PLAN(20);
	UT_RUN(share_scan_preserves_every_reference_before_native_consumer);
	UT_RUN(cleanup_scan_preserves_before_prune_and_freeze);
	UT_RUN(raw_multi_value_never_selects_local_reader);
	UT_RUN(both_cutoffs_remain_unpublished_after_a_preserved_page);
	UT_RUN(nontarget_modes_retain_the_original_native_path);
	UT_RUN(no_multi_and_empty_pages_do_not_acquire_a_preservation_claim);
	UT_RUN(other_pages_dead_items_and_tail_are_preserved_independently);
	UT_RUN(identity_and_io_failures_are_not_converted_to_preservation);
	UT_RUN(capture_owns_exact_physical_carrier_bytes);
	UT_RUN(capture_rejects_malformed_inputs_without_partial_output);
	UT_RUN(real_scan_captures_share_cleanup_and_nonmulti_inputs);
	UT_RUN(next_empty_or_nontarget_page_cannot_reuse_input);
	UT_RUN(real_capture_failure_is_a_hard_error);
	UT_RUN(prepare_preserves_raw_headers_and_owns_physical_rows);
	UT_RUN(prepare_never_aliases_creator_and_current_itl_carriers);
	UT_RUN(prepare_keeps_rows_without_an_addressable_itl);
	UT_RUN(prepare_is_bounded_at_maximum_and_empty_page);
	UT_RUN(prepare_rejects_inconsistent_inputs_without_partial_output);
	UT_RUN(real_scan_consumes_prepared_rows_under_both_original_locks);
	UT_RUN(real_scan_replaces_and_invalidates_prepared_batch);
	UT_DONE();
	return ut_failed_count ? 1 : 0;
}
