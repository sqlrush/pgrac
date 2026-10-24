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
#include "cluster/cluster_heap_horizon.h"
#include "cluster/cluster_scn.h"
#include "cluster/cluster_space_storage.h"
#include "commands/progress.h"
#include "commands/vacuum.h"
#include "storage/bufmgr.h"
#include "utils/rel.h"
#include "unit_test.h"

UT_DEFINE_GLOBALS();

#include "test_cluster_heap_small_scn.inc"
#include "test_cluster_vacuum_freeze_locators.inc"

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
static int fixture_vm_pins;
static unsigned fixture_reads, fixture_vm_acquires, fixture_vm_releases;
static unsigned fixture_prepares, fixture_prepare_held_mask;
static bool fixture_finish_noprune, fixture_recordspace, fixture_prune_lpdead;
static bool fixture_noprune_hint, fixture_vacuum_change;
static unsigned fixture_prune_change;
static bool fixture_clear_released_page;
static bool fixture_cleanup_empty;
static TransactionId fixture_cleanup_xmin;
static unsigned fixture_prune, fixture_noprune, fixture_fsm_writes;
static unsigned fixture_vm_writes, fixture_dirties, fixture_consumed_dead;
static int fixture_error_code;
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

int
errcode(int code)
{
	fixture_error_code = code;
	return 0;
}

int
errmsg(const char *format, ...)
{
	(void)format;
	return 0;
}

int
errhint(const char *format, ...)
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

/* Observe ownership at the real preparation call, then run its actual body. */
static bool
fixture_prepare_page(const ClusterHeapFreezePageInputV1 *input, ClusterHeapFreezePageBatchV1 *out)
{
	fixture_prepares++;
	fixture_prepare_held_mask
		|= (fixture_locks ? 1 : 0) | (fixture_pins ? 2 : 0) | (fixture_vm_pins ? 4 : 0);
	return cluster_heap_freeze_prepare_page_v1(input, out);
}

#define cluster_heap_freeze_prepare_page_v1 fixture_prepare_page
#include "test_cluster_vacuum_shared_mx.inc"
#undef cluster_heap_freeze_prepare_page_v1

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
	fixture_vm_pins = 0;
	fixture_reads = fixture_vm_acquires = fixture_vm_releases = 0;
	fixture_prepares = fixture_prepare_held_mask = 0;
	fixture_finish_noprune = true;
	fixture_recordspace = fixture_prune_lpdead = false;
	fixture_noprune_hint = fixture_vacuum_change = false;
	fixture_prune_change = 0;
	fixture_error_code = 0;
	fixture_clear_released_page = false;
	fixture_cleanup_empty = false;
	fixture_cleanup_xmin = InvalidTransactionId;
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

UT_TEST(real_scan_consumes_rows_captured_under_both_original_locks)
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

static void
make_valid_carrier(Page page, uint8 index, TransactionId xid, uint16 wrap)
{
	ClusterItlSlotData *slot = &ClusterPageGetItlSlots(page)[index];

	memset(slot, 0, sizeof(*slot));
	slot->xid = xid;
	slot->wrap = wrap;
	slot->flags = ITL_FLAG_ACTIVE;
	slot->undo_segment_head = uba_encode(1 + index, 11 + index, 21 + index, 31 + index);
	slot->write_scn = 100 + index;
	slot->first_change_lsn = 200 + index;
}

static void
name_three_carriers(Page page, uint8 first, uint8 second, uint8 third)
{
	const OffsetNumber offsets[] = { 1, 2, 5 };
	const uint8 indexes[] = { first, second, third };

	for (unsigned i = 0; i < lengthof(offsets); i++)
		((HeapTupleHeader)PageGetItem(page, PageGetItemId(page, offsets[i])))->t_itl_slot_idx
			= indexes[i];
}

UT_TEST(requests_share_only_the_same_physical_carrier)
{
	ClusterHeapFreezePageInputV1 input;
	ClusterHeapFreezePageBatchV1 batch, before;
	Page page;

	setup(true);
	make_itl_page();
	page = (Page)fixture_pages[0].data;
	name_three_carriers(page, 0, 0, 1);
	make_valid_carrier(page, 0, 77, TT_WRAP_MAX);
	make_valid_carrier(page, 1, 77, 2);
	UT_ASSERT(cluster_heap_freeze_capture_page_v1(&state.identity, 0, page, &input));
	UT_ASSERT(cluster_heap_freeze_prepare_page_v1(&input, &batch));
	UT_ASSERT_EQ(batch.carrier_request_count, 2);
	UT_ASSERT_EQ(batch.tuples[0].carrier_request_index, 0);
	UT_ASSERT_EQ(batch.tuples[1].carrier_request_index, 0);
	UT_ASSERT_EQ(batch.tuples[2].carrier_request_index, 1);
	for (unsigned i = 0; i < 2; i++) {
		const ClusterHeapFreezeCarrierRequestV1 *request = &batch.carrier_requests[i];
		ClusterItlSlotData *slot = &ClusterPageGetItlSlots(page)[i];

		UT_ASSERT_EQ(request->locator.xid, 77);
		UT_ASSERT_EQ(request->locator.itl_slot_index, i);
		UT_ASSERT_EQ(request->locator.itl_kind, slot->flags);
		UT_ASSERT_EQ(request->locator.tt_wrap, TT_WRAP_INVALID);
		UT_ASSERT_EQ(memcmp(&request->locator.uba, &slot->undo_segment_head, sizeof(UBA)), 0);
		UT_ASSERT_EQ(memcmp(&request->carrier, slot, sizeof(*slot)), 0);
	}
	UT_ASSERT_EQ(batch.carrier_requests[0].carrier.wrap, TT_WRAP_MAX);
	UT_ASSERT_EQ(batch.carrier_requests[1].carrier.wrap, 2);
	before = batch;
	memset(&input, 0, sizeof(input));
	memset(page, 0, BLCKSZ);
	UT_ASSERT_EQ(memcmp(&batch, &before, sizeof(batch)), 0);
}

UT_TEST(requests_keep_updater_and_lock_carriers_distinct_from_raw_xmin)
{
	ClusterHeapFreezePageInputV1 input;
	ClusterHeapFreezePageBatchV1 batch;
	Page page;

	setup(true);
	make_itl_page();
	page = (Page)fixture_pages[0].data;
	name_three_carriers(page, 0, 1, 2);
	make_valid_carrier(page, 0, 99, 3);
	make_valid_carrier(page, 1, 7, 4);
	make_valid_carrier(page, 2, 7, 5);
	ClusterPageGetItlSlots(page)[1].flags = ITL_FLAG_LOCK_ONLY_COMMITTED;
	ClusterPageGetItlSlots(page)[2].flags = ITL_FLAG_COMMITTED;
	ClusterPageGetItlSlots(page)[1].commit_scn = 123;
	ClusterPageGetItlSlots(page)[2].commit_scn = 124;
	UT_ASSERT(cluster_heap_freeze_capture_page_v1(&state.identity, 0, page, &input));
	UT_ASSERT(cluster_heap_freeze_prepare_page_v1(&input, &batch));
	UT_ASSERT_EQ(batch.carrier_request_count, 3);
	for (unsigned i = 0; i < 3; i++) {
		UT_ASSERT_EQ(batch.tuples[i].raw_xmin, 7);
		UT_ASSERT_EQ(batch.tuples[i].carrier_request_index, i);
		UT_ASSERT_EQ(batch.carrier_requests[i].locator.xid, i == 0 ? 99 : 7);
		UT_ASSERT_EQ(batch.carrier_requests[i].locator.tt_wrap, TT_WRAP_INVALID);
	}
	UT_ASSERT_EQ(batch.carrier_requests[1].locator.itl_kind, ITL_FLAG_LOCK_ONLY_COMMITTED);
	UT_ASSERT_EQ(batch.carrier_requests[2].locator.itl_kind, ITL_FLAG_COMMITTED);
	UT_ASSERT_EQ(memcmp(page, input.image.data, BLCKSZ), 0);
}

UT_TEST(data_groups_keep_distinct_carriers_in_one_physical_block)
{
	ClusterHeapFreezePageInputV1 input;
	ClusterHeapFreezePageBatchV1 batch;
	Page page;

	setup(true);
	make_itl_page();
	page = (Page)fixture_pages[0].data;
	name_three_carriers(page, 0, 1, 2);
	for (uint8 i = 0; i < 3; i++) {
		make_valid_carrier(page, i, 77, 3 + i);
		ClusterPageGetItlSlots(page)[i].undo_segment_head = uba_encode(257, 11, 21 + i, 31 + i);
	}
	UT_ASSERT(cluster_heap_freeze_capture_page_v1(&state.identity, 0, page, &input));
	UT_ASSERT(cluster_heap_freeze_prepare_page_v1(&input, &batch));
	UT_ASSERT_EQ(batch.carrier_request_count, 3);
	UT_ASSERT_EQ(batch.data_group_count, 1);
	UT_ASSERT_EQ(batch.data_groups[0].segment_id, 257);
	UT_ASSERT_EQ(batch.data_groups[0].block_no, 11);
	UT_ASSERT_EQ(batch.data_groups[0].request_count, 3);
	for (uint8 i = 0; i < 3; i++) {
		UT_ASSERT_EQ(batch.data_groups[0].request_indexes[i], i);
		UT_ASSERT_EQ(batch.carrier_requests[i].data_group_index, 0);
		UT_ASSERT_EQ(batch.tuples[i].carrier_request_index, i);
		UT_ASSERT_EQ(batch.carrier_requests[i].locator.tt_wrap, TT_WRAP_INVALID);
		UT_ASSERT_EQ(batch.carrier_requests[i].carrier.wrap, 3 + i);
	}
}

UT_TEST(data_groups_preserve_first_appearance_and_duplicate_addresses)
{
	ClusterHeapFreezePageInputV1 input;
	ClusterHeapFreezePageBatchV1 batch;
	Page page;

	setup(true);
	make_itl_page();
	page = (Page)fixture_pages[0].data;
	name_three_carriers(page, 7, 1, 5);
	make_valid_carrier(page, 7, 77, 10);
	make_valid_carrier(page, 1, 88, 11);
	make_valid_carrier(page, 5, 77, 12);
	ClusterPageGetItlSlots(page)[7].undo_segment_head = uba_encode(256, 12, 21, 31);
	ClusterPageGetItlSlots(page)[1].undo_segment_head = uba_encode(256, 11, 21, 31);
	ClusterPageGetItlSlots(page)[5].undo_segment_head = uba_encode(256, 12, 21, 31);
	UT_ASSERT(cluster_heap_freeze_capture_page_v1(&state.identity, 0, page, &input));
	UT_ASSERT(cluster_heap_freeze_prepare_page_v1(&input, &batch));
	UT_ASSERT_EQ(batch.carrier_request_count, 3);
	UT_ASSERT_EQ(batch.data_group_count, 2);
	UT_ASSERT_EQ(batch.data_groups[0].segment_id, 256);
	UT_ASSERT_EQ(batch.data_groups[0].block_no, 12);
	UT_ASSERT_EQ(batch.data_groups[0].request_count, 2);
	UT_ASSERT_EQ(batch.data_groups[0].request_indexes[0], 0);
	UT_ASSERT_EQ(batch.data_groups[0].request_indexes[1], 2);
	UT_ASSERT_EQ(batch.data_groups[1].block_no, 11);
	UT_ASSERT_EQ(batch.data_groups[1].request_count, 1);
	UT_ASSERT_EQ(batch.data_groups[1].request_indexes[0], 1);
	UT_ASSERT_EQ(batch.carrier_requests[0].locator.itl_slot_index, 7);
	UT_ASSERT_EQ(batch.carrier_requests[1].locator.itl_slot_index, 1);
	UT_ASSERT_EQ(batch.carrier_requests[2].locator.itl_slot_index, 5);
	UT_ASSERT_EQ(batch.carrier_requests[0].data_group_index, 0);
	UT_ASSERT_EQ(batch.carrier_requests[1].data_group_index, 1);
	UT_ASSERT_EQ(batch.carrier_requests[2].data_group_index, 0);
}

UT_TEST(data_groups_separate_complete_segment_and_origin)
{
	ClusterHeapFreezePageInputV1 input;
	ClusterHeapFreezePageBatchV1 batch;
	const uint32 segments[] = { 1, 257, UNDO_OWNER_INSTANCE_MAX * CLUSTER_UNDO_SEGS_PER_INSTANCE };
	Page page;

	setup(true);
	make_itl_page();
	page = (Page)fixture_pages[0].data;
	name_three_carriers(page, 0, 1, 2);
	for (uint8 i = 0; i < 3; i++) {
		make_valid_carrier(page, i, 77, 3);
		ClusterPageGetItlSlots(page)[i].undo_segment_head
			= uba_encode(segments[i], UNDO_BLOCKS_PER_SEGMENT - 1, 21, 31);
	}
	UT_ASSERT(cluster_heap_freeze_capture_page_v1(&state.identity, 0, page, &input));
	UT_ASSERT(cluster_heap_freeze_prepare_page_v1(&input, &batch));
	UT_ASSERT_EQ(batch.data_group_count, 3);
	for (uint8 i = 0; i < 3; i++) {
		UT_ASSERT_EQ(batch.data_groups[i].segment_id, segments[i]);
		UT_ASSERT_EQ(batch.data_groups[i].block_no, UNDO_BLOCKS_PER_SEGMENT - 1);
		UT_ASSERT_EQ(batch.data_groups[i].request_count, 1);
		UT_ASSERT_EQ(batch.data_groups[i].request_indexes[0], i);
		UT_ASSERT_EQ(batch.carrier_requests[i].data_group_index, i);
	}
}

UT_TEST(nondata_legacy_requests_keep_witness_without_a_read_group)
{
	ClusterHeapFreezePageInputV1 input;
	ClusterHeapFreezePageBatchV1 batch;
	Page page;

	setup(true);
	make_itl_page();
	page = (Page)fixture_pages[0].data;
	name_three_carriers(page, 0, 1, 2);
	for (uint8 i = 0; i < 3; i++)
		make_valid_carrier(page, i, 77, 3);
	ClusterPageGetItlSlots(page)[0].undo_segment_head = uba_encode(1, 0, 21, 31);
	ClusterPageGetItlSlots(page)[1].undo_segment_head = uba_encode(1, UNDO_BLOCKS_PER_SEGMENT, 21, 31);
	ClusterPageGetItlSlots(page)[2].undo_segment_head
		= uba_encode(UNDO_OWNER_INSTANCE_MAX * CLUSTER_UNDO_SEGS_PER_INSTANCE + 1, 11, 21, 31);
	UT_ASSERT(cluster_heap_freeze_capture_page_v1(&state.identity, 0, page, &input));
	UT_ASSERT(cluster_heap_freeze_prepare_page_v1(&input, &batch));
	UT_ASSERT_EQ(batch.carrier_request_count, 3);
	UT_ASSERT_EQ(batch.data_group_count, 0);
	for (uint8 i = 0; i < 3; i++) {
		UT_ASSERT_EQ(batch.carrier_requests[i].data_group_index, CLUSTER_HEAP_FREEZE_NO_DATA_GROUP);
		UT_ASSERT_EQ(batch.tuples[i].carrier_request_index, i);
		UT_ASSERT_EQ(memcmp(&batch.carrier_requests[i].carrier, &ClusterPageGetItlSlots(page)[i],
						   sizeof(ClusterItlSlotData)), 0);
	}
}

UT_TEST(real_scan_groups_owned_requests_after_original_locks_and_pins)
{
	for (unsigned cleanup = 0; cleanup < 2; cleanup++) {
		const ClusterHeapFreezePageBatchV1 *batch;
		Page page;

		setup(cleanup);
		make_itl_page();
		page = (Page)fixture_pages[0].data;
		name_three_carriers(page, 0, 1, 0);
		make_valid_carrier(page, 0, 77, 3);
		make_valid_carrier(page, 1, 77, 4);
		ClusterPageGetItlSlots(page)[1].undo_segment_head = uba_encode(1, 11, 22, 32);
		lazy_scan_heap(&state);
		batch = prepared_batch();
		UT_ASSERT(batch != NULL);
		if (batch == NULL)
			continue;
		UT_ASSERT_EQ(fixture_prepares, 1);
		UT_ASSERT_EQ(fixture_prepare_held_mask, 0);
		UT_ASSERT_EQ(batch->carrier_request_count, 2);
		UT_ASSERT_EQ(batch->data_group_count, 1);
		UT_ASSERT_EQ(batch->data_groups[0].request_count, 2);
		UT_ASSERT_EQ(batch->data_groups[0].request_indexes[0], 0);
		UT_ASSERT_EQ(batch->data_groups[0].request_indexes[1], 1);
		UT_ASSERT_EQ(batch->tuples[2].carrier_request_index, 0);
		state.rel_pages = 2;
		make_page(1, true, 123);
		lazy_scan_heap(&state);
		batch = prepared_batch();
		UT_ASSERT(batch != NULL);
		if (batch != NULL) {
			UT_ASSERT_EQ(batch->data_group_count, 0);
			UT_ASSERT_EQ(batch->carrier_request_count, 0);
			for (unsigned i = 0; i < sizeof(batch->data_groups); i++)
				UT_ASSERT_EQ(((const unsigned char *)batch->data_groups)[i], 0);
		}
		UT_ASSERT_EQ(fixture_locks + fixture_pins + fixture_vm_pins, 0);
	}
}

UT_TEST(unusable_carriers_keep_rows_without_a_request)
{
	for (unsigned bad = 0; bad < 10; bad++) {
		ClusterHeapFreezePageInputV1 input;
		ClusterHeapFreezePageBatchV1 batch;
		ClusterItlSlotData *slot;
		Page page;

		setup(true);
		make_itl_page();
		page = (Page)fixture_pages[0].data;
		name_three_carriers(page, 0, 0, 0);
		make_valid_carrier(page, 0, 77, 3);
		slot = &ClusterPageGetItlSlots(page)[0];
		switch (bad) {
		case 0:
			slot->flags = ITL_FLAG_FREE;
			break;
		case 1:
			slot->wrap = TT_WRAP_INVALID;
			break;
		case 2:
			memset(&slot->undo_segment_head, 0, sizeof(UBA));
			break;
		case 3:
			slot->undo_segment_head.raw[1] |= UINT64CONST(1) << 32;
			break;
		case 4:
			slot->undo_segment_head.raw[0] = UINT64CONST(11) << 32;
			break;
		case 5:
			slot->undo_segment_head.raw[0] |= UINT32_MAX;
			break;
		case 6:
			slot->undo_segment_head.raw[1] = TT_SLOTS_PER_SEGMENT;
			break;
		case 7:
			slot->flags = ITL_FLAG_LOCK_ONLY_XMAX_IS_MULTI;
			break;
		case 8:
			slot->xid = FrozenTransactionId;
			break;
		case 9:
			slot->flags = 255;
			break;
		}
		UT_ASSERT(cluster_heap_freeze_capture_page_v1(&state.identity, 0, page, &input));
		UT_ASSERT(cluster_heap_freeze_prepare_page_v1(&input, &batch));
		UT_ASSERT_EQ(batch.tuple_count, 3);
		UT_ASSERT_EQ(batch.carrier_request_count, 0);
		for (unsigned i = 0; i < 3; i++) {
			UT_ASSERT_EQ(batch.tuples[i].carrier_request_index,
						 CLUSTER_HEAP_FREEZE_NO_CARRIER_REQUEST);
			UT_ASSERT(batch.tuples[i].has_carrier);
			UT_ASSERT_EQ(memcmp(&batch.tuples[i].carrier, slot, sizeof(*slot)), 0);
		}
	}
}

UT_TEST(missing_and_unused_carriers_cannot_supply_requests)
{
	for (unsigned mode = 0; mode < 3; mode++) {
		ClusterHeapFreezePageInputV1 input;
		ClusterHeapFreezePageBatchV1 batch;
		Page page;

		setup(true);
		if (mode != 0)
			make_itl_page();
		page = (Page)fixture_pages[0].data;
		if (mode != 0)
			for (uint8 i = 0; i < CLUSTER_ITL_INITRANS_DEFAULT; i++)
				make_valid_carrier(page, i, 77, 3);
		name_three_carriers(page, CLUSTER_ITL_SLOT_UNALLOCATED, CLUSTER_ITL_INITRANS_DEFAULT, 254);
		/* No-ITL page with a superficially addressable index is still absent. */
		if (mode == 0)
			name_three_carriers(page, 0, 0, 0);
		if (mode == 2)
			((PageHeader)page)->pd_lower = SizeOfPageHeaderData;
		UT_ASSERT(cluster_heap_freeze_capture_page_v1(&state.identity, 0, page, &input));
		UT_ASSERT(cluster_heap_freeze_prepare_page_v1(&input, &batch));
		UT_ASSERT_EQ(batch.carrier_request_count, 0);
		UT_ASSERT_EQ(batch.tuple_count, mode == 2 ? 0 : 3);
		for (unsigned i = 0; i < batch.tuple_count; i++) {
			UT_ASSERT(!batch.tuples[i].has_carrier);
			UT_ASSERT_EQ(batch.tuples[i].carrier_request_index,
						 CLUSTER_HEAP_FREEZE_NO_CARRIER_REQUEST);
		}
	}
}

UT_TEST(request_batch_is_bounded_by_eight_physical_slots)
{
	ClusterHeapFreezePageInputV1 input;
	ClusterHeapFreezePageBatchV1 batch;
	Page page;
	PageHeader header;
	unsigned count = (BLCKSZ - SizeOfPageHeaderData - CLUSTER_ITL_SPECIAL_SIZE)
					 / (MAXALIGN(SizeofHeapTupleHeader) + sizeof(ItemIdData));

	setup(true);
	page = (Page)fixture_pages[0].data;
	memset(page, 0, BLCKSZ);
	header = (PageHeader)page;
	header->pd_lower = SizeOfPageHeaderData + count * sizeof(ItemIdData);
	header->pd_upper = BLCKSZ - CLUSTER_ITL_SPECIAL_SIZE - count * MAXALIGN(SizeofHeapTupleHeader);
	header->pd_special = BLCKSZ - CLUSTER_ITL_SPECIAL_SIZE;
	header->pd_pagesize_version = BLCKSZ | PG_PAGE_LAYOUT_VERSION;
	header->pd_flags = PD_HAS_ITL;
	for (uint8 i = 0; i < CLUSTER_ITL_INITRANS_DEFAULT; i++)
		make_valid_carrier(page, i, 77, 3 + i);
	for (OffsetNumber i = 1; i <= count; i++) {
		ItemId item = PageGetItemId(page, i);
		HeapTupleHeader tuple;

		ItemIdSetNormal(item, header->pd_special - i * MAXALIGN(SizeofHeapTupleHeader),
						SizeofHeapTupleHeader);
		tuple = (HeapTupleHeader)PageGetItem(page, item);
		tuple->t_hoff = SizeofHeapTupleHeader;
		tuple->t_itl_slot_idx = (i - 1) % CLUSTER_ITL_INITRANS_DEFAULT;
	}
	UT_ASSERT(count <= MaxHeapTuplesPerPage);
	UT_ASSERT(cluster_heap_freeze_capture_page_v1(&state.identity, 0, page, &input));
	UT_ASSERT(cluster_heap_freeze_prepare_page_v1(&input, &batch));
	UT_ASSERT_EQ(batch.tuple_count, count);
	UT_ASSERT_EQ(batch.carrier_request_count, CLUSTER_ITL_INITRANS_DEFAULT);
	UT_ASSERT_EQ(batch.data_group_count, CLUSTER_ITL_INITRANS_DEFAULT);
	for (unsigned i = 0; i < count; i++)
		UT_ASSERT_EQ(batch.tuples[i].carrier_request_index, i % CLUSTER_ITL_INITRANS_DEFAULT);
	for (unsigned i = 0; i < CLUSTER_ITL_INITRANS_DEFAULT; i++) {
		UT_ASSERT_EQ(batch.carrier_requests[i].locator.itl_slot_index, i);
		UT_ASSERT_EQ(batch.carrier_requests[i].locator.tt_wrap, TT_WRAP_INVALID);
		UT_ASSERT_EQ(batch.carrier_requests[i].carrier.wrap, 3 + i);
		UT_ASSERT_EQ(batch.data_groups[i].request_count, 1);
		UT_ASSERT_EQ(batch.data_groups[i].request_indexes[0], i);
		UT_ASSERT_EQ(batch.carrier_requests[i].data_group_index, i);
	}
	/* The same eight distinct carrier requests also fit in one DATA read. */
	for (uint8 i = 0; i < CLUSTER_ITL_INITRANS_DEFAULT; i++)
		ClusterPageGetItlSlots(page)[i].undo_segment_head = uba_encode(1, 11, 21 + i, 31 + i);
	UT_ASSERT(cluster_heap_freeze_capture_page_v1(&state.identity, 0, page, &input));
	UT_ASSERT(cluster_heap_freeze_prepare_page_v1(&input, &batch));
	UT_ASSERT_EQ(batch.carrier_request_count, CLUSTER_ITL_INITRANS_DEFAULT);
	UT_ASSERT_EQ(batch.data_group_count, 1);
	UT_ASSERT_EQ(batch.data_groups[0].request_count, CLUSTER_ITL_INITRANS_DEFAULT);
	for (uint8 i = 0; i < CLUSTER_ITL_INITRANS_DEFAULT; i++) {
		UT_ASSERT_EQ(batch.data_groups[0].request_indexes[i], i);
		UT_ASSERT_EQ(batch.carrier_requests[i].data_group_index, 0);
	}
}

UT_TEST(real_scan_prepares_owned_requests_and_resets_next_page)
{
	for (unsigned cleanup = 0; cleanup < 2; cleanup++) {
		const ClusterHeapFreezePageBatchV1 *batch;
		PGAlignedBlock before;
		Page page;

		setup(cleanup);
		make_itl_page();
		page = (Page)fixture_pages[0].data;
		name_three_carriers(page, 0, 0, 1);
		make_valid_carrier(page, 0, 77, TT_WRAP_MAX);
		make_valid_carrier(page, 1, 77, 2);
		before = fixture_pages[0];
		lazy_scan_heap(&state);
		batch = prepared_batch();
		UT_ASSERT(batch != NULL);
		if (batch == NULL)
			continue;
		UT_ASSERT_EQ(batch->carrier_request_count, 2);
		UT_ASSERT_EQ(batch->tuples[2].carrier_request_index, 1);
		UT_ASSERT_EQ(batch->carrier_requests[0].carrier.wrap, TT_WRAP_MAX);
		UT_ASSERT_EQ(batch->carrier_requests[0].locator.tt_wrap, TT_WRAP_INVALID);
		UT_ASSERT_EQ(memcmp(&fixture_pages[0], &before, sizeof(before)), 0);
		UT_ASSERT_EQ(fixture_prune + fixture_noprune + fixture_dirties, 0);
		UT_ASSERT_EQ(fixture_locks + fixture_pins, 0);
		fixture_publish_cutoffs(&state);
		UT_ASSERT_EQ(state.NewRelfrozenXid, InvalidTransactionId);
		UT_ASSERT_EQ(state.NewRelminMxid, InvalidMultiXactId);
		state.rel_pages = 2;
		make_page(1, true, 123);
		lazy_scan_heap(&state);
		batch = prepared_batch();
		UT_ASSERT(batch != NULL);
		if (batch != NULL) {
			UT_ASSERT_EQ(batch->block, 1);
			UT_ASSERT_EQ(batch->carrier_request_count, 0);
			UT_ASSERT_EQ(batch->tuples[2].carrier_request_index,
						 CLUSTER_HEAP_FREEZE_NO_CARRIER_REQUEST);
		}
		((PageHeader)fixture_pages[1].data)->pd_lower = SizeOfPageHeaderData;
		lazy_scan_heap(&state);
		UT_ASSERT(prepared_batch() == NULL);
		fixture_peer = false;
		lazy_scan_heap(&state);
		UT_ASSERT(prepared_batch() == NULL);
	}
}

UT_TEST(real_scan_prepares_after_heap_lock_and_both_pins_are_released)
{
	for (unsigned cleanup = 0; cleanup < 2; cleanup++)
		for (unsigned multi = 0; multi < 2; multi++) {
			setup(cleanup);
			make_page(0, multi, 33);
			fixture_recordspace = true;
			lazy_scan_heap(&state);
			UT_ASSERT_EQ(fixture_prepares, 1);
			UT_ASSERT_EQ(fixture_prepare_held_mask, 0);
			UT_ASSERT_EQ(fixture_reads, 1);
			UT_ASSERT_EQ(fixture_vm_acquires, 1);
			UT_ASSERT_EQ(fixture_vm_releases, 1);
			UT_ASSERT_EQ(fixture_pins + fixture_locks + fixture_vm_pins, 0);
			UT_ASSERT(prepared_batch() != NULL);
			/* The real consumer has no transport/fence provider yet. */
			UT_ASSERT_EQ(state.shared_freeze_apply_status,
							 CLUSTER_HEAP_FREEZE_PROOF_KEEP_UNPROVEN);
			UT_ASSERT(!state.shared_freeze_fence_valid);
		}
}

UT_TEST(released_live_page_cannot_replace_owned_preparation_input)
{
	for (unsigned cleanup = 0; cleanup < 2; cleanup++) {
		const ClusterHeapFreezePageBatchV1 *batch;
		PGAlignedBlock before;
		Page page;

		setup(cleanup);
		make_itl_page();
		page = (Page)fixture_pages[0].data;
		name_three_carriers(page, 0, 0, 1);
		make_valid_carrier(page, 0, 77, TT_WRAP_MAX);
		make_valid_carrier(page, 1, 88, 2);
		before = fixture_pages[0];
		fixture_clear_released_page = true;
		lazy_scan_heap(&state);
		batch = prepared_batch();
		UT_ASSERT(batch != NULL);
		if (batch == NULL)
			continue;
		UT_ASSERT_EQ(fixture_prepare_held_mask, 0);
		UT_ASSERT_EQ(memcmp(&state.shared_freeze_input.image, &before, sizeof(before)), 0);
		UT_ASSERT(PageIsNew((Page)fixture_pages[0].data));
		UT_ASSERT_EQ(batch->carrier_request_count, 2);
		UT_ASSERT_EQ(batch->carrier_requests[0].locator.xid, 77);
		UT_ASSERT_EQ(batch->carrier_requests[1].locator.xid, 88);
		UT_ASSERT_EQ(fixture_reads, 1);
	}
}

UT_TEST(cleanup_fallback_prepares_only_its_refreshed_capture)
{
	const ClusterHeapFreezePageBatchV1 *batch;

	setup(false);
	make_page(0, false, 33);
	state.aggressive = true;
	fixture_finish_noprune = false;
	fixture_cleanup_xmin = 88;
	lazy_scan_heap(&state);
	batch = prepared_batch();
	UT_ASSERT(batch != NULL);
	if (batch == NULL)
		return;
	UT_ASSERT_EQ(batch->tuples[0].raw_xmin, 88);
	UT_ASSERT_EQ(fixture_prepares, 1);
	UT_ASSERT_EQ(fixture_prepare_held_mask, 0);
	UT_ASSERT_EQ(fixture_noprune, 1);
	UT_ASSERT_EQ(fixture_prune, 1);
	UT_ASSERT_EQ(fixture_reads, 1);
	UT_ASSERT_EQ(fixture_pins + fixture_locks + fixture_vm_pins, 0);
}

UT_TEST(indexed_lpdead_exit_also_releases_before_preparation)
{
	setup(true);
	make_page(0, false, 33);
	state.do_index_vacuuming = true;
	fixture_prune_lpdead = true;
	lazy_scan_heap(&state);
	UT_ASSERT_EQ(fixture_prepares, 1);
	UT_ASSERT_EQ(fixture_prepare_held_mask, 0);
	UT_ASSERT_EQ(fixture_prune, 1);
	UT_ASSERT_EQ(fixture_fsm_writes, 0);
	UT_ASSERT_EQ(fixture_pins + fixture_locks + fixture_vm_pins, 0);
}

UT_TEST(nontarget_keeps_vm_pin_reuse_and_never_prepares)
{
	setup(true);
	fixture_peer = false;
	state.rel_pages = 2;
	make_page(1, true, 33);
	lazy_scan_heap(&state);
	UT_ASSERT_EQ(fixture_prepares, 0);
	UT_ASSERT_EQ(fixture_prune, 2);
	UT_ASSERT_EQ(fixture_reads, 2);
	UT_ASSERT_EQ(fixture_vm_acquires, 1);
	UT_ASSERT_EQ(fixture_vm_releases, 1);
	UT_ASSERT_EQ(fixture_pins + fixture_locks + fixture_vm_pins, 0);
	UT_ASSERT(prepared_batch() == NULL);
}

UT_TEST(each_captured_page_gets_one_unpinned_preparation)
{
	const ClusterHeapFreezePageBatchV1 *batch;

	setup(true);
	state.rel_pages = 2;
	make_page(1, true, 44);
	lazy_scan_heap(&state);
	batch = prepared_batch();
	UT_ASSERT(batch != NULL);
	if (batch == NULL)
		return;
	UT_ASSERT_EQ(batch->block, 1);
	UT_ASSERT_EQ(batch->tuples[0].raw_xmax, 44);
	UT_ASSERT_EQ(fixture_prepares, 2);
	UT_ASSERT_EQ(fixture_prepare_held_mask, 0);
	UT_ASSERT_EQ(fixture_reads, 2);
	UT_ASSERT_EQ(fixture_vm_acquires, 2);
	UT_ASSERT_EQ(fixture_vm_releases, 2);
	UT_ASSERT_EQ(fixture_pins + fixture_locks + fixture_vm_pins, 0);
}

UT_TEST(cleanup_fallback_to_empty_discards_pending_capture)
{
	setup(false);
	make_page(0, false, 33);
	state.aggressive = true;
	fixture_finish_noprune = false;
	fixture_cleanup_empty = true;
	lazy_scan_heap(&state);
	UT_ASSERT_EQ(fixture_prepares, 0);
	UT_ASSERT(prepared_batch() == NULL);
	UT_ASSERT(!state.shared_freeze_input_captured);
	UT_ASSERT_EQ(fixture_pins + fixture_locks + fixture_vm_pins, 0);
}

UT_TEST(noindex_lpdead_exit_prepares_once_after_all_buffers_are_released)
{
	setup(true);
	make_page(0, false, 33);
	state.nindexes = 0;
	fixture_prune_lpdead = true;
	lazy_scan_heap(&state);
	UT_ASSERT_EQ(fixture_prepares, 1);
	UT_ASSERT_EQ(fixture_prepare_held_mask, 0);
	UT_ASSERT(!state.shared_freeze_input_captured);
	UT_ASSERT(prepared_batch() != NULL);
	UT_ASSERT_EQ(fixture_reads, 1);
	UT_ASSERT_EQ(fixture_prune, 1);
	UT_ASSERT_EQ(fixture_fsm_writes, 1);
	UT_ASSERT_EQ(fixture_vm_acquires, 1);
	UT_ASSERT_EQ(fixture_vm_releases, 1);
	UT_ASSERT_EQ(fixture_pins + fixture_locks + fixture_vm_pins, 0);
}

UT_TEST(share_native_hint_change_discards_captured_input)
{
	setup(false);
	make_page(0, false, 33);
	fixture_noprune_hint = true;
	lazy_scan_heap(&state);
	UT_ASSERT(prepared_batch() == NULL);
	UT_ASSERT_EQ(fixture_prepares, 0);
	UT_ASSERT(!state.shared_freeze_input_captured);
	UT_ASSERT_EQ(fixture_noprune, 1);
	UT_ASSERT_EQ(fixture_reads, 1);
	UT_ASSERT_EQ(fixture_pins + fixture_locks + fixture_vm_pins, 0);
}

UT_TEST(cleanup_prune_change_discards_input_on_both_final_exits)
{
	for (unsigned lpdead = 0; lpdead < 2; lpdead++) {
		setup(true);
		make_page(0, false, 33);
		fixture_prune_change = 1;
		fixture_prune_lpdead = lpdead;
		state.do_index_vacuuming = lpdead;
		lazy_scan_heap(&state);
		UT_ASSERT(prepared_batch() == NULL);
		UT_ASSERT_EQ(fixture_prepares, 0);
		UT_ASSERT(!state.shared_freeze_input_captured);
		UT_ASSERT_EQ(fixture_prune, 1);
		UT_ASSERT_EQ(fixture_reads, 1);
		UT_ASSERT_EQ(fixture_pins + fixture_locks + fixture_vm_pins, 0);
	}
}

UT_TEST(noindex_heap_vacuum_change_discards_captured_input)
{
	setup(true);
	make_page(0, false, 33);
	state.nindexes = 0;
	fixture_prune_lpdead = true;
	fixture_vacuum_change = true;
	lazy_scan_heap(&state);
	UT_ASSERT(prepared_batch() == NULL);
	UT_ASSERT_EQ(fixture_prepares, 0);
	UT_ASSERT(!state.shared_freeze_input_captured);
	UT_ASSERT_EQ(fixture_prune, 1);
	UT_ASSERT_EQ(fixture_fsm_writes, 1);
	UT_ASSERT_EQ(fixture_reads, 1);
	UT_ASSERT_EQ(fixture_pins + fixture_locks + fixture_vm_pins, 0);
}

UT_TEST(malformed_current_page_is_not_a_changed_input_retry)
{
	setup(true);
	make_page(0, false, 33);
	fixture_prune_change = 2;
	if (setjmp(fixture_error) == 0) {
		lazy_scan_heap(&state);
		UT_ASSERT(false);
	} else {
		UT_ASSERT_EQ(fixture_error_code, ERRCODE_DATA_CORRUPTED);
		UT_ASSERT(prepared_batch() == NULL);
		UT_ASSERT_EQ(fixture_prepares, 0);
		UT_ASSERT_EQ(fixture_reads, 1);
	}
}

UT_TEST(physical_validation_matches_owned_image_and_ignores_identity_padding)
{
	ClusterHeapFreezePageInputV1 input;
	ClusterSpaceIdentity identity;
	PGAlignedBlock other;

	setup(true);
	make_itl_page();
	UT_ASSERT(cluster_heap_freeze_capture_page_v1(&state.identity, 0, (Page)fixture_pages[0].data,
												  &input));
	other = fixture_pages[0];
	memset(&identity, 0xa5, sizeof(identity));
	identity.key.system_identifier = state.identity.key.system_identifier;
	identity.key.database_incarnation = state.identity.key.database_incarnation;
	memcpy(identity.key.storage_uuid, state.identity.key.storage_uuid,
		   sizeof(identity.key.storage_uuid));
	identity.key.locator.spcOid = state.identity.key.locator.spcOid;
	identity.key.locator.dbOid = state.identity.key.locator.dbOid;
	identity.key.locator.relNumber = state.identity.key.locator.relNumber;
	memcpy(identity.incarnation, state.identity.incarnation, sizeof(identity.incarnation));
	identity.sequence = state.identity.sequence;
	identity.operation = state.identity.operation;
	identity.state = state.identity.state;
	UT_ASSERT_EQ(cluster_heap_freeze_validate_page_v1(&input, &identity, 0, (Page)other.data),
				 CLUSTER_HEAP_FREEZE_PAGE_MATCHED);
	UT_ASSERT_EQ(memcmp(&other, &fixture_pages[0], sizeof(other)), 0);
	UT_ASSERT_EQ(memcmp(&input.image, &fixture_pages[0], sizeof(other)), 0);
}

UT_TEST(physical_validation_rejects_each_space_and_block_identity_change)
{
	ClusterHeapFreezePageInputV1 input;
	Page page;

	setup(true);
	page = (Page)fixture_pages[0].data;
	UT_ASSERT(cluster_heap_freeze_capture_page_v1(&state.identity, 0, page, &input));
	for (unsigned change = 0; change < 11; change++) {
		ClusterSpaceIdentity identity = state.identity;
		BlockNumber block = 0;

		switch (change) {
		case 0:
			identity.key.system_identifier++;
			break;
		case 1:
			identity.key.database_incarnation++;
			break;
		case 2:
			identity.key.storage_uuid[15] ^= 1;
			break;
		case 3:
			identity.key.locator.spcOid++;
			break;
		case 4:
			identity.key.locator.dbOid++;
			break;
		case 5:
			identity.key.locator.relNumber++;
			break;
		case 6:
			identity.incarnation[15] ^= 1;
			break;
		case 7:
			identity.sequence++;
			break;
		case 8:
			identity.operation++;
			break;
		case 9:
			identity.state = CLUSTER_SPACE_IDENTITY_TOMBSTONED;
			break;
		case 10:
			block++;
			break;
		}
		UT_ASSERT_EQ(cluster_heap_freeze_validate_page_v1(&input, &identity, block, page),
					 CLUSTER_HEAP_FREEZE_PAGE_CHANGED);
	}
}

UT_TEST(physical_validation_covers_all_page_bytes_and_itl_carriers)
{
	ClusterHeapFreezePageInputV1 input;
	PGAlignedBlock before;
	Page page;

	setup(true);
	make_itl_page();
	page = (Page)fixture_pages[0].data;
	UT_ASSERT(cluster_heap_freeze_capture_page_v1(&state.identity, 0, page, &input));
	before = fixture_pages[0];
	/* LSN/origin, version, tuple header/locator and all 48 bytes of every ITL. */
	for (unsigned byte = 0; byte < BLCKSZ; byte++) {
		ClusterHeapFreezePageMatchV1 result;

		page[byte] ^= 1;
		result = cluster_heap_freeze_validate_page_v1(&input, &state.identity, 0, page);
		/* Structural damage may be INVALID; no changed byte may match. */
		UT_ASSERT(result != CLUSTER_HEAP_FREEZE_PAGE_MATCHED);
		page[byte] ^= 1;
	}
	UT_ASSERT_EQ(memcmp(&before, &fixture_pages[0], sizeof(before)), 0);
	UT_ASSERT_EQ(cluster_heap_freeze_validate_page_v1(&input, &state.identity, 0, page),
				 CLUSTER_HEAP_FREEZE_PAGE_MATCHED);
}

UT_TEST(physical_validation_distinguishes_invalid_arguments_and_layout)
{
	ClusterHeapFreezePageInputV1 input;
	ClusterSpaceIdentity identity;
	Page page;

	setup(true);
	page = (Page)fixture_pages[0].data;
	UT_ASSERT(cluster_heap_freeze_capture_page_v1(&state.identity, 0, page, &input));
	UT_ASSERT_EQ(cluster_heap_freeze_validate_page_v1(NULL, &state.identity, 0, page),
				 CLUSTER_HEAP_FREEZE_PAGE_INVALID);
	UT_ASSERT_EQ(cluster_heap_freeze_validate_page_v1(&input, NULL, 0, page),
				 CLUSTER_HEAP_FREEZE_PAGE_INVALID);
	UT_ASSERT_EQ(
		cluster_heap_freeze_validate_page_v1(&input, &state.identity, InvalidBlockNumber, page),
		CLUSTER_HEAP_FREEZE_PAGE_INVALID);
	UT_ASSERT_EQ(cluster_heap_freeze_validate_page_v1(&input, &state.identity, 0, NULL),
				 CLUSTER_HEAP_FREEZE_PAGE_INVALID);
	identity = state.identity;
	identity.state = 0;
	UT_ASSERT_EQ(cluster_heap_freeze_validate_page_v1(&input, &identity, 0, page),
				 CLUSTER_HEAP_FREEZE_PAGE_INVALID);
	input.normal_tuples++;
	UT_ASSERT_EQ(cluster_heap_freeze_validate_page_v1(&input, &state.identity, 0, page),
				 CLUSTER_HEAP_FREEZE_PAGE_INVALID);
	input.normal_tuples--;
	((PageHeader)page)->pd_upper = BLCKSZ;
	UT_ASSERT_EQ(cluster_heap_freeze_validate_page_v1(&input, &state.identity, 0, page),
				 CLUSTER_HEAP_FREEZE_PAGE_INVALID);
	memcpy(page, input.image.data, BLCKSZ);
	((PageHeader)input.image.data)->pd_lower = 1;
	UT_ASSERT_EQ(cluster_heap_freeze_validate_page_v1(&input, &state.identity, 0, page),
				 CLUSTER_HEAP_FREEZE_PAGE_INVALID);
}

/* Run the real capture/prepare without turning raw fields into proof. */
static void
prepare_retained_rows(ClusterHeapFreezePageBatchV1 *batch)
{
	ClusterHeapFreezePageInputV1 input;

	if (!cluster_heap_freeze_capture_page_v1(&state.identity, 0, (Page)fixture_pages[0].data,
											 &input)
		|| !cluster_heap_freeze_prepare_page_v1(&input, batch))
		abort();
}

static HeapTupleHeader
retained_row(OffsetNumber offset)
{
	Page page = (Page)fixture_pages[0].data;

	return (HeapTupleHeader)PageGetItem(page, PageGetItemId(page, offset));
}

UT_TEST(retained_counts_cover_physical_rows_without_merging_raw_xids)
{
	ClusterHeapFreezePageBatchV1 batch;

	setup(true);
	make_page(0, false, 33);
	prepare_retained_rows(&batch);
	UT_ASSERT_EQ(batch.tuple_count, 3);
	UT_ASSERT_EQ(batch.unproven_xmin_tuples, 3);
	UT_ASSERT_EQ(batch.unproven_xmax_tuples, 3);
	UT_ASSERT_EQ(batch.multi_tuples, 0);
	UT_ASSERT_EQ(batch.carrier_request_count, 0);
}

UT_TEST(only_both_frozen_hint_bits_suppress_creator_work)
{
	ClusterHeapFreezePageBatchV1 batch;
	const uint16 masks[] = { 0, HEAP_XMIN_COMMITTED, HEAP_XMIN_INVALID, HEAP_XMIN_FROZEN };
	const TransactionId xids[] = { InvalidTransactionId, FrozenTransactionId, 7 };

	for (unsigned xid = 0; xid < lengthof(xids); xid++)
		for (unsigned mask = 0; mask < lengthof(masks); mask++) {
			setup(true);
			make_page(0, false, 33);
			for (OffsetNumber off = 1; off <= 5; off++) {
				if (off == 3 || off == 4)
					continue;
				HeapTupleHeaderSetXmin(retained_row(off), xids[xid]);
				retained_row(off)->t_infomask = masks[mask] | HEAP_XMAX_INVALID;
			}
			prepare_retained_rows(&batch);
			UT_ASSERT_EQ(batch.unproven_xmin_tuples, mask == 3 ? 0 : 3);
			UT_ASSERT_EQ(batch.unproven_xmax_tuples, 0);
		}
}

UT_TEST(frozen_creator_does_not_clear_committed_or_lock_only_xmax)
{
	ClusterHeapFreezePageBatchV1 batch;
	const uint16 masks[] = { 0, HEAP_XMAX_COMMITTED, HEAP_XMAX_LOCK_ONLY, HEAP_XMAX_INVALID };

	for (unsigned mask = 0; mask < lengthof(masks); mask++) {
		setup(true);
		make_page(0, false, 33);
		for (OffsetNumber off = 1; off <= 5; off++) {
			if (off == 3 || off == 4)
				continue;
			retained_row(off)->t_infomask = HEAP_XMIN_FROZEN | masks[mask];
		}
		prepare_retained_rows(&batch);
		UT_ASSERT_EQ(batch.unproven_xmin_tuples, 0);
		UT_ASSERT_EQ(batch.unproven_xmax_tuples, mask == 3 ? 0 : 3);
	}
}

UT_TEST(multis_stay_opaque_and_special_raw_xmax_does_not_supply_proof)
{
	ClusterHeapFreezePageBatchV1 batch;

	setup(true);
	make_page(0, false, 33);
	retained_row(1)->t_infomask = HEAP_XMIN_FROZEN | HEAP_XMAX_IS_MULTI | HEAP_XMAX_INVALID;
	retained_row(2)->t_infomask = HEAP_XMIN_FROZEN;
	HeapTupleHeaderSetXmax(retained_row(2), InvalidTransactionId);
	retained_row(5)->t_infomask = HEAP_XMIN_FROZEN | HEAP_XMAX_INVALID;
	prepare_retained_rows(&batch);
	UT_ASSERT_EQ(batch.multi_tuples, 1);
	UT_ASSERT_EQ(batch.unproven_xmin_tuples, 0);
	UT_ASSERT_EQ(batch.unproven_xmax_tuples, 1);
}

UT_TEST(real_scan_accumulates_retained_work_after_both_original_lock_paths)
{
	for (unsigned cleanup = 0; cleanup < 2; cleanup++)
		for (unsigned multi = 0; multi < 2; multi++) {
			setup(cleanup);
			make_page(0, multi, 33);
			lazy_scan_heap(&state);
			UT_ASSERT_EQ(state.shared_xmin_deferred_tuples, 3);
			UT_ASSERT_EQ(state.shared_xmax_deferred_tuples, multi ? 2 : 3);
			UT_ASSERT_EQ(state.shared_freeze_changed_pages, 0);
			UT_ASSERT_EQ(fixture_prepare_held_mask, 0);
			UT_ASSERT_EQ(fixture_pins + fixture_locks + fixture_vm_pins, 0);
		}
}

UT_TEST(retained_work_survives_replacement_and_empty_page_invalidation)
{
	setup(true);
	make_page(1, false, 44);
	make_page(2, false, 55);
	((PageHeader)fixture_pages[2].data)->pd_lower = SizeOfPageHeaderData;
	((PageHeader)fixture_pages[2].data)->pd_upper = BLCKSZ;
	state.rel_pages = 3;
	lazy_scan_heap(&state);
	UT_ASSERT_EQ(state.shared_xmin_deferred_tuples, 6);
	UT_ASSERT_EQ(state.shared_xmax_deferred_tuples, 5);
	UT_ASSERT_EQ(state.shared_mx_deferred_pages, 1);
	UT_ASSERT_EQ(fixture_prepares, 2);
	UT_ASSERT(!state.shared_freeze_input_valid);
}

UT_TEST(changed_inputs_record_missing_coverage_without_counting_old_rows)
{
	for (unsigned cleanup = 0; cleanup < 2; cleanup++) {
		setup(cleanup);
		make_page(0, false, 33);
		fixture_noprune_hint = !cleanup;
		fixture_prune_change = cleanup ? 1 : 0;
		lazy_scan_heap(&state);
		UT_ASSERT_EQ(state.shared_freeze_changed_pages, 1);
		UT_ASSERT_EQ(state.shared_xmin_deferred_tuples, 0);
		UT_ASSERT_EQ(state.shared_xmax_deferred_tuples, 0);
		UT_ASSERT_EQ(fixture_prepares, 0);
		UT_ASSERT(!state.shared_freeze_input_valid);
	}
}

UT_TEST(retained_or_changed_inputs_independently_prevent_catalog_publication)
{
	/* Isolate the new negative guards, without weakening the product policy. */
	for (unsigned reason = 0; reason < 3; reason++) {
		setup(true);
		state.shared_maintenance_restricted = false;
		state.shared_xmin_deferred_tuples = reason == 0;
		state.shared_xmax_deferred_tuples = reason == 1;
		state.shared_freeze_changed_pages = reason == 2;
		fixture_publish_cutoffs(&state);
		UT_ASSERT_EQ(state.NewRelfrozenXid, InvalidTransactionId);
		UT_ASSERT_EQ(state.NewRelminMxid, InvalidMultiXactId);
	}
}

UT_TEST(zero_retained_counts_never_remove_the_original_shared_restriction)
{
	setup(true);
	fixture_publish_cutoffs(&state);
	UT_ASSERT_EQ(state.NewRelfrozenXid, InvalidTransactionId);
	UT_ASSERT_EQ(state.NewRelminMxid, InvalidMultiXactId);
	setup(true);
	state.versioned = false;
	state.shared_maintenance_restricted = false;
	lazy_scan_heap(&state);
	UT_ASSERT_EQ(state.shared_xmin_deferred_tuples, 0);
	UT_ASSERT_EQ(state.shared_xmax_deferred_tuples, 0);
	UT_ASSERT_EQ(state.shared_freeze_changed_pages, 0);
	fixture_publish_cutoffs(&state);
	UT_ASSERT_EQ(state.NewRelfrozenXid, 100);
	UT_ASSERT_EQ(state.NewRelminMxid, 100);
}

static void
init_canonical_proof_fixture(ClusterHeapFreezeCanonicalProofV1 *proof)
{
	memset(proof, 0, sizeof(*proof));
	setup(true);
	proof->requested_locator.uba = uba_encode(1, 1, 0, 1);
	proof->requested_locator.xid = 77;
	proof->requested_locator.tt_wrap = TT_WRAP_INVALID;
	proof->requested_locator.itl_kind = ITL_FLAG_ACTIVE;
	proof->requested_locator.itl_slot_index = 0;
	proof->resolution.locator_echo = proof->requested_locator;
	proof->resolution.locator_echo.tt_wrap = 2;
	proof->resolution.outcome = CLUSTER_TX_COMMITTED;
	proof->resolution.proof_kind = CLUSTER_TX_PROOF_ORIGIN_DURABLE_TT_CLOG;
	proof->resolution.commit_scn = 9;
	proof->resolution.authority.origin_epoch = 1;
	proof->resolution.authority.live_hwm_lsn = 1;
	proof->resolution.authority.tt_generation = 1;
	proof->resolution.authority.authority_scn = 9;
	proof->resolution.local_canonical_valid = true;
	proof->resolution.local_canonical_generation = 1;
	proof->resolution.local_canonical.segment_id = 1;
	proof->resolution.local_canonical.xid = 77;
	proof->resolution.local_canonical.slot_offset = 0;
	proof->resolution.local_canonical.wrap = 2;
	proof->c1b_finalized = true;
	proof->authority_current = true;
	proof->admission_current = true;
	proof->expected_fence.identity = state.identity;
	proof->expected_fence.block = 17;
	proof->expected_fence.page_lsn = 0x987654321ULL;
	proof->expected_fence.page_version = 0x123456789ULL;
	proof->expected_fence.holder_sequence = 11;
	proof->expected_fence.master_sequence = 12;
	proof->expected_fence.admission_generation = 13;
	proof->current_fence = proof->expected_fence;
}

UT_TEST(canonical_proof_requires_exact_row_identity_and_c1b)
{
	ClusterHeapFreezeCanonicalProofV1 proof;

	init_canonical_proof_fixture(&proof);
	UT_ASSERT_EQ(cluster_heap_freeze_validate_canonical_proof_v1(&proof),
					 CLUSTER_HEAP_FREEZE_PROOF_PROVEN);
	proof.requested_locator.uba.raw[0] ^= UINT64_C(1);
	UT_ASSERT_EQ(cluster_heap_freeze_validate_canonical_proof_v1(&proof),
					 CLUSTER_HEAP_FREEZE_PROOF_HARD_ERROR);
	proof.requested_locator.uba.raw[0] ^= UINT64_C(1);
	proof.resolution.local_canonical.wrap++;
	UT_ASSERT_EQ(cluster_heap_freeze_validate_canonical_proof_v1(&proof),
					 CLUSTER_HEAP_FREEZE_PROOF_HARD_ERROR);
	proof.resolution.local_canonical.wrap--;
	proof.resolution.locator_echo.uba.raw[0] ^= UINT64_C(1);
	UT_ASSERT_EQ(cluster_heap_freeze_validate_canonical_proof_v1(&proof),
					 CLUSTER_HEAP_FREEZE_PROOF_HARD_ERROR);
	proof.resolution.locator_echo.uba.raw[0] ^= UINT64_C(1);
	proof.c1b_finalized = false;
	UT_ASSERT_EQ(cluster_heap_freeze_validate_canonical_proof_v1(&proof),
					 CLUSTER_HEAP_FREEZE_PROOF_KEEP_UNPROVEN);
	proof.c1b_finalized = true;
	proof.resolution.local_canonical.xid++;
	UT_ASSERT_EQ(cluster_heap_freeze_validate_canonical_proof_v1(&proof),
					 CLUSTER_HEAP_FREEZE_PROOF_HARD_ERROR);
}

UT_TEST(canonical_proof_builder_copies_transport_and_fence)
{
	ClusterHeapFreezeCanonicalProofV1 source;
	ClusterHeapFreezeCanonicalProofV1 out;

	init_canonical_proof_fixture(&source);
	memset(&out, 0xA5, sizeof(out));
	UT_ASSERT(cluster_heap_freeze_build_canonical_proof_v1(
		&source.requested_locator, &source.resolution, &source.expected_fence,
		&source.current_fence, source.c1b_finalized, source.authority_current,
		source.admission_current, &out));
	UT_ASSERT_EQ(cluster_heap_freeze_validate_canonical_proof_v1(&out),
					 CLUSTER_HEAP_FREEZE_PROOF_PROVEN);
	UT_ASSERT(memcmp(&out.requested_locator, &source.requested_locator,
						 sizeof(out.requested_locator)) == 0);
	UT_ASSERT(memcmp(&out.resolution, &source.resolution,
						 sizeof(out.resolution)) == 0);
	UT_ASSERT(memcmp(&out.expected_fence, &source.expected_fence,
						 sizeof(out.expected_fence)) == 0);
	UT_ASSERT(memcmp(&out.current_fence, &source.current_fence,
						 sizeof(out.current_fence)) == 0);
}

UT_TEST(canonical_proof_builder_rejects_null_without_clobber)
{
	ClusterHeapFreezeCanonicalProofV1 out;
	ClusterHeapFreezeCanonicalProofV1 before;
	ClusterHeapFreezeCanonicalProofV1 source;

	init_canonical_proof_fixture(&source);
	memset(&out, 0xA5, sizeof(out));
	before = out;
	UT_ASSERT(!cluster_heap_freeze_build_canonical_proof_v1(
		NULL, &source.resolution, &source.expected_fence, &source.current_fence,
		true, true, true, &out));
	UT_ASSERT(memcmp(&out, &before, sizeof(out)) == 0);
}

UT_TEST(final_fence_mismatch_retries_without_proof)
{
	ClusterHeapFreezeCanonicalProofV1 proof;

	init_canonical_proof_fixture(&proof);
	proof.current_fence.holder_sequence++;
	UT_ASSERT_EQ(cluster_heap_freeze_validate_canonical_proof_v1(&proof),
					 CLUSTER_HEAP_FREEZE_PROOF_RETRY_CHANGED);
	proof.current_fence = proof.expected_fence;
	proof.current_fence.master_sequence++;
	UT_ASSERT_EQ(cluster_heap_freeze_validate_canonical_proof_v1(&proof),
					 CLUSTER_HEAP_FREEZE_PROOF_RETRY_CHANGED);
	proof.current_fence = proof.expected_fence;
	proof.current_fence.identity.state = CLUSTER_SPACE_IDENTITY_TOMBSTONED;
	UT_ASSERT_EQ(cluster_heap_freeze_validate_canonical_proof_v1(&proof),
					 CLUSTER_HEAP_FREEZE_PROOF_RETRY_CHANGED);
	proof.current_fence = proof.expected_fence;
	proof.current_fence.block++;
	UT_ASSERT_EQ(cluster_heap_freeze_validate_canonical_proof_v1(&proof),
					 CLUSTER_HEAP_FREEZE_PROOF_RETRY_CHANGED);
	UT_ASSERT_EQ(cluster_heap_freeze_validate_canonical_proof_v1(NULL),
					 CLUSTER_HEAP_FREEZE_PROOF_HARD_ERROR);
}

UT_TEST(unproven_tt_and_snapshot_never_admit_freeze)
{
	ClusterHeapFreezeCanonicalProofV1 proof;

	init_canonical_proof_fixture(&proof);
	proof.resolution.outcome = CLUSTER_TX_IN_PROGRESS;
	proof.resolution.proof_kind = CLUSTER_TX_PROOF_NONE;
	UT_ASSERT_EQ(cluster_heap_freeze_validate_canonical_proof_v1(&proof),
					 CLUSTER_HEAP_FREEZE_PROOF_KEEP_UNPROVEN);
	init_canonical_proof_fixture(&proof);
	proof.authority_current = false;
	UT_ASSERT_EQ(cluster_heap_freeze_validate_canonical_proof_v1(&proof),
					 CLUSTER_HEAP_FREEZE_PROOF_KEEP_UNPROVEN);
	init_canonical_proof_fixture(&proof);
	proof.admission_current = false;
	UT_ASSERT_EQ(cluster_heap_freeze_validate_canonical_proof_v1(&proof),
					 CLUSTER_HEAP_FREEZE_PROOF_KEEP_UNPROVEN);
}

UT_TEST(proof_batch_is_all_or_nothing_for_conditional_apply)
{
	ClusterHeapFreezeCanonicalProofV1 first;
	ClusterHeapFreezeCanonicalProofV1 second;
	ClusterHeapFreezeProofBatchV1 batch;

	init_canonical_proof_fixture(&first);
	second = first;
	memset(&batch, 0, sizeof(batch));
	batch.proof_count = 2;
	batch.proofs[0] = first;
	batch.proofs[1] = second;
	UT_ASSERT_EQ(cluster_heap_freeze_validate_proof_batch_v1(&batch),
					 CLUSTER_HEAP_FREEZE_PROOF_PROVEN);

	/* One missing proof keeps the whole conditional apply blocked. */
	batch.proofs[1].c1b_finalized = false;
	UT_ASSERT_EQ(cluster_heap_freeze_validate_proof_batch_v1(&batch),
					 CLUSTER_HEAP_FREEZE_PROOF_KEEP_UNPROVEN);

	/* A changed current fence retries the whole batch, without partial apply. */
	batch.proofs[1] = second;
	batch.proofs[1].current_fence.holder_sequence++;
	UT_ASSERT_EQ(cluster_heap_freeze_validate_proof_batch_v1(&batch),
					 CLUSTER_HEAP_FREEZE_PROOF_RETRY_CHANGED);

	/* Individually valid proofs with different fences are ambiguous. */
	batch.proofs[1] = second;
	batch.proofs[1].expected_fence.block++;
	batch.proofs[1].current_fence.block++;
	UT_ASSERT_EQ(cluster_heap_freeze_validate_proof_batch_v1(&batch),
					 CLUSTER_HEAP_FREEZE_PROOF_HARD_ERROR);

	batch.proof_count = 0;
	UT_ASSERT_EQ(cluster_heap_freeze_validate_proof_batch_v1(&batch),
					 CLUSTER_HEAP_FREEZE_PROOF_KEEP_UNPROVEN);
}

UT_TEST(conditional_apply_requires_exact_carriers_and_final_fence)
{
	ClusterHeapFreezeCanonicalProofV1 proof;
	ClusterHeapFreezePageBatchV1 page;
	ClusterHeapFreezeProofBatchV1 proofs;
	ClusterHeapFreezeFinalFenceV1 current_fence;

	init_canonical_proof_fixture(&proof);
	memset(&page, 0, sizeof(page));
	memset(&proofs, 0, sizeof(proofs));
	page.identity = state.identity;
	page.block = proof.expected_fence.block;
	page.carrier_request_count = 1;
	page.carrier_requests[0].locator = proof.requested_locator;
	proofs.proof_count = 1;
	proofs.proofs[0] = proof;
	current_fence = proof.expected_fence;

	UT_ASSERT_EQ(cluster_heap_freeze_validate_conditional_apply_v1(&page,
																	 &proofs,
																	 &current_fence),
						 CLUSTER_HEAP_FREEZE_PROOF_PROVEN);

	/* The provider and final fence are mandatory; no proof stays fail-closed. */
	proofs.proof_count = 0;
	UT_ASSERT_EQ(cluster_heap_freeze_validate_conditional_apply_v1(&page,
																	 &proofs,
																	 &current_fence),
						 CLUSTER_HEAP_FREEZE_PROOF_KEEP_UNPROVEN);
	proofs.proof_count = 1;

	/* A changed current owner retries the whole conditional apply. */
	current_fence.holder_sequence++;
	UT_ASSERT_EQ(cluster_heap_freeze_validate_conditional_apply_v1(&page,
																	 &proofs,
																	 &current_fence),
						 CLUSTER_HEAP_FREEZE_PROOF_RETRY_CHANGED);
	current_fence = proof.expected_fence;

	/* A physical locator mismatch is an ambiguous proof, not a retry. */
	page.carrier_requests[0].locator.xid++;
	UT_ASSERT_EQ(cluster_heap_freeze_validate_conditional_apply_v1(&page,
																	 &proofs,
																	 &current_fence),
						 CLUSTER_HEAP_FREEZE_PROOF_HARD_ERROR);
	page.carrier_requests[0].locator = proof.requested_locator;

	/* Two equal physical requests make the proof-to-row mapping ambiguous. */
	page.carrier_request_count = 2;
	page.carrier_requests[1] = page.carrier_requests[0];
	UT_ASSERT_EQ(cluster_heap_freeze_validate_conditional_apply_v1(&page,
																	 &proofs,
																	 &current_fence),
						 CLUSTER_HEAP_FREEZE_PROOF_HARD_ERROR);

	/* A valid proof for only part of the carrier set keeps the page unproven. */
	page.carrier_requests[1].locator.xid++;
	UT_ASSERT_EQ(cluster_heap_freeze_validate_conditional_apply_v1(&page,
																	 &proofs,
																	 &current_fence),
						 CLUSTER_HEAP_FREEZE_PROOF_KEEP_UNPROVEN);
}

static void
init_subset_proof_fixture(ClusterHeapFreezePageBatchV1 *page,
						  ClusterHeapFreezeProofBatchV1 *proofs,
						  ClusterHeapFreezeFinalFenceV1 *fence)
{
	memset(page, 0, sizeof(*page));
	memset(proofs, 0, sizeof(*proofs));
	init_canonical_proof_fixture(&proofs->proofs[0]);
	*fence = proofs->proofs[0].expected_fence;
	proofs->proof_count = 1;
	page->identity = fence->identity;
	page->block = fence->block;
	page->carrier_request_count = 2;
	page->carrier_requests[0].locator = proofs->proofs[0].requested_locator;
	page->carrier_requests[1] = page->carrier_requests[0];
	page->carrier_requests[1].locator.itl_slot_index = 1;
}

UT_TEST(exact_subset_can_cleanout_without_proving_the_whole_page)
{
	ClusterHeapFreezePageBatchV1 page;
	ClusterHeapFreezeProofBatchV1 proofs;
	ClusterHeapFreezeFinalFenceV1 fence;
	ClusterHeapFreezeCarrierRequestV1 swap;

	init_subset_proof_fixture(&page, &proofs, &fence);
	/* Equal raw XIDs do not merge distinct physical carriers or require order. */
	swap = page.carrier_requests[0];
	page.carrier_requests[0] = page.carrier_requests[1];
	page.carrier_requests[1] = swap;
	UT_ASSERT_EQ(cluster_heap_freeze_validate_conditional_apply_v1(&page, &proofs, &fence),
				 CLUSTER_HEAP_FREEZE_PROOF_KEEP_UNPROVEN);
	UT_ASSERT_EQ(cluster_heap_freeze_validate_carrier_cleanout_v1(&page, &proofs, &fence),
				 CLUSTER_HEAP_FREEZE_PROOF_PROVEN);
}

UT_TEST(cleanout_subset_rejects_duplicate_or_unrequested_proofs)
{
	ClusterHeapFreezePageBatchV1 page;
	ClusterHeapFreezeProofBatchV1 proofs;
	ClusterHeapFreezeFinalFenceV1 fence;

	init_subset_proof_fixture(&page, &proofs, &fence);
	proofs.proof_count = 2;
	proofs.proofs[1] = proofs.proofs[0];
	UT_ASSERT_EQ(cluster_heap_freeze_validate_carrier_cleanout_v1(&page, &proofs, &fence),
				 CLUSTER_HEAP_FREEZE_PROOF_HARD_ERROR);
	proofs.proof_count = 1;
	page.carrier_requests[1] = page.carrier_requests[0];
	UT_ASSERT_EQ(cluster_heap_freeze_validate_carrier_cleanout_v1(&page, &proofs, &fence),
				 CLUSTER_HEAP_FREEZE_PROOF_HARD_ERROR);
	init_subset_proof_fixture(&page, &proofs, &fence);
	page.carrier_requests[0].locator.tt_wrap = 3;
	UT_ASSERT_EQ(cluster_heap_freeze_validate_carrier_cleanout_v1(&page, &proofs, &fence),
				 CLUSTER_HEAP_FREEZE_PROOF_HARD_ERROR);
}

UT_TEST(cleanout_subset_requires_the_same_final_current_fence)
{
	ClusterHeapFreezePageBatchV1 page;
	ClusterHeapFreezeProofBatchV1 proofs;
	ClusterHeapFreezeFinalFenceV1 fence;

	init_subset_proof_fixture(&page, &proofs, &fence);
	fence.holder_sequence++;
	UT_ASSERT_EQ(cluster_heap_freeze_validate_carrier_cleanout_v1(&page, &proofs, &fence),
				 CLUSTER_HEAP_FREEZE_PROOF_RETRY_CHANGED);
}

UT_TEST(cleanout_subset_never_promotes_missing_canonical_evidence)
{
	ClusterHeapFreezePageBatchV1 page;
	ClusterHeapFreezeProofBatchV1 proofs;
	ClusterHeapFreezeFinalFenceV1 fence;
	const ClusterTxOutcome unproven[] = {CLUSTER_TX_UNKNOWN, CLUSTER_TX_IN_PROGRESS,
		CLUSTER_TX_PREPARED};

	for (unsigned index = 0; index < lengthof(unproven); index++)
	{
		init_subset_proof_fixture(&page, &proofs, &fence);
		proofs.proofs[0].resolution.outcome = unproven[index];
		UT_ASSERT_EQ(cluster_heap_freeze_validate_carrier_cleanout_v1(&page, &proofs, &fence),
					 CLUSTER_HEAP_FREEZE_PROOF_KEEP_UNPROVEN);
	}
	init_subset_proof_fixture(&page, &proofs, &fence);
	proofs.proofs[0].c1b_finalized = false;
	UT_ASSERT_EQ(cluster_heap_freeze_validate_carrier_cleanout_v1(&page, &proofs, &fence),
				 CLUSTER_HEAP_FREEZE_PROOF_KEEP_UNPROVEN);
	proofs.proofs[0].c1b_finalized = true;
	proofs.proofs[0].admission_current = false;
	UT_ASSERT_EQ(cluster_heap_freeze_validate_carrier_cleanout_v1(&page, &proofs, &fence),
				 CLUSTER_HEAP_FREEZE_PROOF_KEEP_UNPROVEN);
	proofs.proof_count = 0;
	UT_ASSERT_EQ(cluster_heap_freeze_validate_carrier_cleanout_v1(&page, &proofs, &fence),
				 CLUSTER_HEAP_FREEZE_PROOF_KEEP_UNPROVEN);
}


static void
init_creation_binding_fixture(ClusterHeapFreezePageBatchV1 *page,
	ClusterHeapFreezeProofBatchV1 *proofs, ClusterHeapFreezeFinalFenceV1 *fence)
{
	init_subset_proof_fixture(page, proofs, fence);
	page->carrier_request_count = 1;
	page->page_lsn = fence->page_lsn;
	page->page_version = fence->page_version;
	page->tuple_count = 2;
	for (int index = 0; index < 2; index++) {
		ClusterHeapFreezeTupleInputV1 *tuple = &page->tuples[index];
		tuple->offset = index + 1;
		tuple->tuple_offset = BLCKSZ - CLUSTER_ITL_SPECIAL_SIZE - (index + 1) * 64;
		tuple->tuple_length = 60;
		tuple->header_length = SizeofHeapTupleHeader;
		tuple->raw_xmin = 77;
		tuple->has_carrier = true;
		tuple->carrier_request_index = 0;
		tuple->itl_slot_index = 0;
		tuple->carrier = page->carrier_requests[0].carrier;
		/* A prior attempt must never survive rejection or replacement. */
		tuple->creation_target_proof_index = 0;
	}
	proofs->proofs[0].resolution.local_creation_target = (ClusterTxCreationTarget){
		.locator = page->identity.key.locator, .fork = MAIN_FORKNUM,
		.block = page->block, .offset = 1, .tuple_length = 60, .valid = true};
}

UT_TEST(creation_binding_is_physical_even_with_equal_raw_xids_and_carriers)
{
	ClusterHeapFreezePageBatchV1 page;
	ClusterHeapFreezeProofBatchV1 proofs;
	ClusterHeapFreezeFinalFenceV1 fence;

	init_creation_binding_fixture(&page, &proofs, &fence);
	UT_ASSERT_EQ(cluster_heap_freeze_bind_creation_targets_v1(&page, &proofs, &fence), 1);
	UT_ASSERT_EQ(page.tuples[0].creation_target_proof_index, 0);
	UT_ASSERT_EQ(page.tuples[1].creation_target_proof_index, CLUSTER_HEAP_FREEZE_NO_CREATION_TARGET);
	proofs.proofs[0].resolution.local_creation_target.offset = 2;
	proofs.proofs[0].resolution.local_creation_target.tuple_length = 0;
	UT_ASSERT_EQ(cluster_heap_freeze_bind_creation_targets_v1(&page, &proofs, &fence), 1);
	UT_ASSERT_EQ(page.tuples[0].creation_target_proof_index, CLUSTER_HEAP_FREEZE_NO_CREATION_TARGET);
	UT_ASSERT_EQ(page.tuples[1].creation_target_proof_index, 0);
}

UT_TEST(creation_binding_keeps_own_carrier_and_proof_order)
{
	ClusterHeapFreezePageBatchV1 page;
	ClusterHeapFreezeProofBatchV1 proofs;
	ClusterHeapFreezeFinalFenceV1 fence;
	ClusterHeapFreezeCanonicalProofV1 swap;

	init_creation_binding_fixture(&page, &proofs, &fence);
	page.carrier_request_count = proofs.proof_count = 2;
	page.carrier_requests[1] = page.carrier_requests[0];
	page.carrier_requests[1].locator.itl_slot_index = 1;
	proofs.proofs[1] = proofs.proofs[0];
	proofs.proofs[1].requested_locator.itl_slot_index = 1;
	proofs.proofs[1].resolution.locator_echo.itl_slot_index = 1;
	proofs.proofs[1].resolution.local_creation_target.offset = 2;
	page.tuples[1].carrier_request_index = 1;
	page.tuples[1].itl_slot_index = 1;
	swap = proofs.proofs[0];
	proofs.proofs[0] = proofs.proofs[1];
	proofs.proofs[1] = swap;
	UT_ASSERT_EQ(cluster_heap_freeze_bind_creation_targets_v1(&page, &proofs, &fence), 2);
	UT_ASSERT_EQ(page.tuples[0].creation_target_proof_index, 1);
	UT_ASSERT_EQ(page.tuples[1].creation_target_proof_index, 0);
	/* Equal raw XID does not permit a row to borrow the other carrier's target. */
	proofs.proofs[1].resolution.local_creation_target.offset = 2;
	UT_ASSERT_EQ(cluster_heap_freeze_bind_creation_targets_v1(&page, &proofs, &fence), 1);
	UT_ASSERT_EQ(page.tuples[0].creation_target_proof_index, CLUSTER_HEAP_FREEZE_NO_CREATION_TARGET);
	for (unsigned byte = 0; byte < sizeof(ClusterItlSlotData); byte++) {
		init_creation_binding_fixture(&page, &proofs, &fence);
		((char *)&page.tuples[0].carrier)[byte] ^= 1;
		UT_ASSERT_EQ(cluster_heap_freeze_bind_creation_targets_v1(&page, &proofs, &fence), 0);
		UT_ASSERT_EQ(page.tuples[0].creation_target_proof_index, CLUSTER_HEAP_FREEZE_NO_CREATION_TARGET);
	}
}

UT_TEST(creation_binding_rejects_incomplete_or_changed_evidence_and_resets_rows)
{
	for (int variant = 0; variant < 19; variant++) {
		ClusterHeapFreezePageBatchV1 page;
		ClusterHeapFreezeProofBatchV1 proofs;
		ClusterHeapFreezeFinalFenceV1 fence;
		ClusterTxCreationTarget *target;
		init_creation_binding_fixture(&page, &proofs, &fence);
		target = &proofs.proofs[0].resolution.local_creation_target;
		switch (variant) {
		case 0: target->valid = false; break;
		case 1: target->locator.spcOid++; break;
		case 2: target->locator.dbOid++; break;
		case 3: target->locator.relNumber++; break;
		case 4: target->fork = FSM_FORKNUM; break;
		case 5: target->block++; break;
		case 6: target->offset = InvalidOffsetNumber; break;
		case 7: target->tuple_length++; break;
		case 8: page.tuples[0].raw_xmin++; break;
		case 9: page.tuples[0].raw_xmin = FrozenTransactionId; break;
		case 10: page.tuples[0].has_carrier = false; break;
		case 11: page.tuples[0].carrier_request_index = CLUSTER_HEAP_FREEZE_NO_CARRIER_REQUEST; break;
		case 12: page.tuples[0].itl_slot_index++; break;
		case 13: page.tuples[0].tuple_offset = BLCKSZ; break;
		case 14: page.tuples[0].header_length = 0; break;
		case 15: fence.master_sequence++; break;
		case 16: page.page_lsn++; break;
		case 17: proofs.proofs[0].c1b_finalized = false; break;
		case 18: page.carrier_request_count = 2; break;
		}
		UT_ASSERT_EQ(cluster_heap_freeze_bind_creation_targets_v1(&page, &proofs, &fence), 0);
		UT_ASSERT_EQ(page.tuples[0].creation_target_proof_index, CLUSTER_HEAP_FREEZE_NO_CREATION_TARGET);
		UT_ASSERT_EQ(page.tuples[1].creation_target_proof_index, CLUSTER_HEAP_FREEZE_NO_CREATION_TARGET);
	}
}

UT_TEST(creation_binding_missing_or_malformed_batches_cannot_reuse_old_indexes)
{
	ClusterHeapFreezePageBatchV1 page;
	ClusterHeapFreezeProofBatchV1 proofs;
	ClusterHeapFreezeFinalFenceV1 fence;

	UT_ASSERT_EQ(cluster_heap_freeze_bind_creation_targets_v1(NULL, NULL, NULL), 0);
	for (int variant = 0; variant < 4; variant++) {
		init_creation_binding_fixture(&page, &proofs, &fence);
		if (variant == 0)
			proofs.proof_count = 0;
		if (variant == 1)
			proofs.proof_count = CLUSTER_HEAP_FREEZE_MAX_PROOF_BATCH + 1;
		if (variant == 2)
			page.tuple_count = MaxHeapTuplesPerPage + 1;
		UT_ASSERT_EQ(cluster_heap_freeze_bind_creation_targets_v1(&page, &proofs,
			variant == 3 ? NULL : &fence), 0);
		UT_ASSERT_EQ(page.tuples[0].creation_target_proof_index, CLUSTER_HEAP_FREEZE_NO_CREATION_TARGET);
	}
}

/* Model the completed local provenance handoff using the real row binder. */
static void
init_xmin_plan_fixture(ClusterHeapFreezePageBatchV1 *page,
	ClusterHeapFreezeProofBatchV1 *proofs, ClusterHeapFreezeFinalFenceV1 *fence)
{
	init_creation_binding_fixture(page, proofs, fence);
	proofs->proofs[0].resolution.local_original_full_xid =
		FullTransactionIdFromEpochAndXid(1, 77);
	UT_ASSERT_EQ(cluster_heap_freeze_bind_creation_targets_v1(page, proofs, fence), 1);
}

static bool
prepare_fixture_xmin(const ClusterHeapFreezePageBatchV1 *page,
	const ClusterHeapFreezeProofBatchV1 *proofs,
	const ClusterHeapFreezeFinalFenceV1 *fence, HeapTupleFreeze *out)
{
	return cluster_heap_freeze_prepare_xmin_v1(page, proofs, fence, 0,
		FullTransactionIdFromEpochAndXid(1, 77),
		FullTransactionIdFromEpochAndXid(1, 100), 10, out);
}

UT_TEST(xmin_plan_requires_the_bound_exact_origin_fullxid)
{
	ClusterHeapFreezePageBatchV1 page;
	ClusterHeapFreezeProofBatchV1 proofs;
	ClusterHeapFreezeFinalFenceV1 fence;
	HeapTupleFreeze plan, saved;
	FullTransactionId original = FullTransactionIdFromEpochAndXid(1, 77);
	FullTransactionId limit = FullTransactionIdFromEpochAndXid(3, 100);

	for (int variant = 0; variant < 9; variant++) {
		init_creation_binding_fixture(&page, &proofs, &fence);
		proofs.proofs[0].resolution.local_original_full_xid = original;
		UT_ASSERT_EQ(cluster_heap_freeze_bind_creation_targets_v1(&page, &proofs, &fence), 1);
		UT_ASSERT(cluster_heap_freeze_prepare_xmin_v1(&page, &proofs, &fence,
			0, original, limit, 10, &plan));
		saved = plan;
		switch (variant) {
		case 0: page.tuples[0].original_creator_full_xid = InvalidFullTransactionId; break;
		case 1: page.tuples[0].original_creator_full_xid = FullTransactionIdFromEpochAndXid(1, 78); break;
		case 2: page.tuples[0].original_creator_full_xid = FullTransactionIdFromEpochAndXid(2, 77); break;
		case 3: proofs.proofs[0].resolution.local_original_full_xid = InvalidFullTransactionId; break;
		case 4: proofs.proofs[0].resolution.local_original_full_xid = FullTransactionIdFromEpochAndXid(2, 77); break;
		case 5: proofs.proofs[0].resolution.local_original_full_xid = FullTransactionIdFromEpochAndXid(1, FrozenTransactionId); break;
		case 6: proofs.proofs[0].resolution.local_original_full_xid = FullTransactionIdFromEpochAndXid(1, 78); break;
		case 7:
			page.tuples[0].original_creator_full_xid = FullTransactionIdFromEpochAndXid(2, 77);
			proofs.proofs[0].resolution.local_original_full_xid = page.tuples[0].original_creator_full_xid;
			break;
		case 8: break;
		}
		UT_ASSERT(!cluster_heap_freeze_prepare_xmin_v1(&page, &proofs, &fence, 0,
			variant == 8 ? FullTransactionIdFromEpochAndXid(2, 77) : original,
			limit, 10, &plan));
		UT_ASSERT(memcmp(&plan, &saved, sizeof(plan)) == 0);
	}
}

UT_TEST(xmin_plan_requires_strict_fullxid_and_scn_age)
{
	ClusterHeapFreezePageBatchV1 page;
	ClusterHeapFreezeProofBatchV1 proofs;
	ClusterHeapFreezeFinalFenceV1 fence;
	HeapTupleFreeze plan = {0}, saved;
	FullTransactionId creator = FullTransactionIdFromEpochAndXid(1, 77);

	init_xmin_plan_fixture(&page, &proofs, &fence);
	UT_ASSERT(prepare_fixture_xmin(&page, &proofs, &fence, &plan));
	memset(&saved, 0xa5, sizeof(saved));
	for (int variant = 0; variant < 6; variant++) {
		FullTransactionId cutoff = FullTransactionIdFromEpochAndXid(1, 100);
		SCN horizon = 10;
		init_xmin_plan_fixture(&page, &proofs, &fence);
		if (variant == 0) cutoff = creator;
		if (variant == 1) cutoff = FullTransactionIdFromEpochAndXid(1, 76);
		if (variant == 2) cutoff = FullTransactionIdFromEpochAndXid(0, UINT32_MAX);
		if (variant == 3) horizon = 9;
		if (variant == 4) horizon = 8;
		if (variant == 5) horizon = 0;
		plan = saved;
		UT_ASSERT(!cluster_heap_freeze_prepare_xmin_v1(&page, &proofs, &fence,
			0, creator, cutoff, horizon, &plan));
		UT_ASSERT(memcmp(&plan, &saved, sizeof(plan)) == 0);
	}
	/* An explicit older epoch is ordered in FullXid space, not raw-XID space. */
	proofs.proofs[0].resolution.local_original_full_xid = FullTransactionIdFromEpochAndXid(0, 77);
	UT_ASSERT_EQ(cluster_heap_freeze_bind_creation_targets_v1(&page, &proofs, &fence), 1);
	UT_ASSERT(cluster_heap_freeze_prepare_xmin_v1(&page, &proofs, &fence, 0,
		FullTransactionIdFromEpochAndXid(0, 77),
		FullTransactionIdFromEpochAndXid(1, 3), 10, &plan));
}

UT_TEST(xmin_plan_compares_scn_time_without_node_id_ordering)
{
	const struct {
		NodeId commit_node, horizon_node;
		uint64 commit_time, horizon_time;
		bool expected;
	} cases[] = {
		{1, 2, 11, 10, false},
		{1, 2, 10, 10, false},
		{2, 1, 9, 10, true},
		{2, 1, 10, 10, false},
		{2, 1, 11, 10, false},
		{1, 2, 9, 10, true}
	};

	for (unsigned index = 0; index < lengthof(cases); index++) {
		ClusterHeapFreezePageBatchV1 page;
		ClusterHeapFreezeProofBatchV1 proofs;
		ClusterHeapFreezeFinalFenceV1 fence;
		HeapTupleFreeze plan, saved;
		bool prepared;

		init_xmin_plan_fixture(&page, &proofs, &fence);
		proofs.proofs[0].resolution.commit_scn =
			scn_encode(cases[index].commit_node, cases[index].commit_time);
		proofs.proofs[0].resolution.authority.authority_scn =
			proofs.proofs[0].resolution.commit_scn;
		memset(&saved, 0xa5, sizeof(saved));
		plan = saved;
		prepared = cluster_heap_freeze_prepare_xmin_v1(&page, &proofs, &fence, 0,
			FullTransactionIdFromEpochAndXid(1, 77),
			FullTransactionIdFromEpochAndXid(1, 100),
			scn_encode(cases[index].horizon_node, cases[index].horizon_time), &plan);
		UT_ASSERT_EQ(prepared, cases[index].expected);
		if (!cases[index].expected)
			UT_ASSERT(memcmp(&plan, &saved, sizeof(plan)) == 0);
		else if (prepared)
			UT_ASSERT_EQ(plan.t_infomask, page.tuples[0].infomask | HEAP_XMIN_FROZEN);
	}
}

UT_TEST(xmin_plan_preserves_xmax_multi_and_other_header_bits)
{
	const uint16 masks[] = {
		HEAP_XMAX_INVALID, HEAP_XMAX_COMMITTED,
		HEAP_XMAX_LOCK_ONLY | HEAP_XMAX_EXCL_LOCK,
		HEAP_XMAX_IS_MULTI | HEAP_XMAX_LOCK_ONLY | HEAP_XMAX_KEYSHR_LOCK,
		HEAP_XMAX_IS_MULTI | HEAP_XMAX_COMMITTED
	};
	for (unsigned index = 0; index < lengthof(masks); index++) {
		ClusterHeapFreezePageBatchV1 page;
		ClusterHeapFreezeProofBatchV1 proofs;
		ClusterHeapFreezeFinalFenceV1 fence;
		HeapTupleFreeze plan = {0};
		init_xmin_plan_fixture(&page, &proofs, &fence);
		page.tuples[0].raw_xmax = 88;
		page.tuples[0].infomask = masks[index] | HEAP_HASNULL | HEAP_UPDATED;
		page.tuples[0].infomask2 = HEAP_HOT_UPDATED | HEAP_ONLY_TUPLE | 7;
		UT_ASSERT(prepare_fixture_xmin(&page, &proofs, &fence, &plan));
		UT_ASSERT_EQ(plan.offset, 1);
		UT_ASSERT_EQ(plan.xmax, 88);
		UT_ASSERT_EQ(plan.t_infomask, page.tuples[0].infomask | HEAP_XMIN_FROZEN);
		UT_ASSERT_EQ(plan.t_infomask2, page.tuples[0].infomask2);
		UT_ASSERT_EQ(plan.frzflags, 0);
		UT_ASSERT_EQ(plan.checkflags, 0);
	}
}

UT_TEST(xmin_plan_revalidates_physical_target_instead_of_old_binding)
{
	for (int variant = 0; variant < 13; variant++) {
		ClusterHeapFreezePageBatchV1 page;
		ClusterHeapFreezeProofBatchV1 proofs;
		ClusterHeapFreezeFinalFenceV1 fence;
		HeapTupleFreeze plan, saved;
		init_xmin_plan_fixture(&page, &proofs, &fence);
		memset(&saved, 0x5a, sizeof(saved));
		plan = saved;
		switch (variant) {
		case 0: proofs.proofs[0].resolution.local_creation_target.offset = 2; break;
		case 1: proofs.proofs[0].resolution.local_creation_target.valid = false; break;
		case 2: proofs.proofs[0].resolution.local_creation_target.locator.relNumber++; break;
		case 3: proofs.proofs[0].resolution.local_creation_target.fork = FSM_FORKNUM; break;
		case 4: proofs.proofs[0].resolution.local_creation_target.block++; break;
		case 5: proofs.proofs[0].resolution.local_creation_target.tuple_length++; break;
		case 6: page.tuples[0].carrier.wrap++; break;
		case 7: page.tuples[0].has_carrier = false; break;
		case 8: page.tuples[0].carrier_request_index = CLUSTER_HEAP_FREEZE_NO_CARRIER_REQUEST; break;
		case 9: page.tuples[0].itl_slot_index++; break;
		case 10: page.tuples[0].tuple_offset = BLCKSZ; break;
		case 11: page.tuples[0].header_length = 0; break;
		case 12: page.tuples[0].raw_xmin++; break;
		}
		UT_ASSERT(!prepare_fixture_xmin(&page, &proofs, &fence, &plan));
		UT_ASSERT(memcmp(&plan, &saved, sizeof(plan)) == 0);
	}
	{
		ClusterHeapFreezePageBatchV1 page;
		ClusterHeapFreezeProofBatchV1 proofs;
		ClusterHeapFreezeFinalFenceV1 fence;
		HeapTupleFreeze plan = {0};
		init_xmin_plan_fixture(&page, &proofs, &fence);
		page.tuples[0].creation_target_proof_index = CLUSTER_HEAP_FREEZE_NO_CREATION_TARGET;
		UT_ASSERT(prepare_fixture_xmin(&page, &proofs, &fence, &plan));
		/* The other physical row has the same raw XID and carrier, but no target. */
		UT_ASSERT(!cluster_heap_freeze_prepare_xmin_v1(&page, &proofs, &fence, 1,
			FullTransactionIdFromEpochAndXid(1, 77),
			FullTransactionIdFromEpochAndXid(1, 100), 10, &plan));
	}
}

UT_TEST(xmin_plan_rejects_incomplete_proof_and_changed_final_fence)
{
	for (int variant = 0; variant < 14; variant++) {
		ClusterHeapFreezePageBatchV1 page;
		ClusterHeapFreezeProofBatchV1 proofs;
		ClusterHeapFreezeFinalFenceV1 fence;
		HeapTupleFreeze plan, saved;
		init_xmin_plan_fixture(&page, &proofs, &fence);
		memset(&saved, 0x5a, sizeof(saved));
		plan = saved;
		switch (variant) {
		case 0: proofs.proofs[0].c1b_finalized = false; break;
		case 1: proofs.proofs[0].authority_current = false; break;
		case 2: proofs.proofs[0].admission_current = false; break;
		case 3: proofs.proofs[0].resolution.outcome = CLUSTER_TX_IN_PROGRESS; break;
		case 4: proofs.proofs[0].resolution.outcome = CLUSTER_TX_ABORTED;
			proofs.proofs[0].resolution.commit_scn = 0; break;
		case 5: proofs.proofs[0].resolution.commit_scn = 0; break;
		case 6: proofs.proofs[0].resolution.local_canonical_valid = false; break;
		case 7: proofs.proof_count = 0; break;
		case 8: page.carrier_request_count = 2; break;
		case 9: fence.holder_sequence++; break;
		case 10: fence.master_sequence++; break;
		case 11: fence.admission_generation++; break;
		case 12: page.page_lsn++; break;
		case 13: page.page_version++; break;
		}
		UT_ASSERT(!prepare_fixture_xmin(&page, &proofs, &fence, &plan));
		UT_ASSERT(memcmp(&plan, &saved, sizeof(plan)) == 0);
	}
}

UT_TEST(xmin_plan_rejects_special_raw_fullxids_and_legacy_xmin_states)
{
	for (int variant = 0; variant < 11; variant++) {
		ClusterHeapFreezePageBatchV1 page;
		ClusterHeapFreezeProofBatchV1 proofs;
		ClusterHeapFreezeFinalFenceV1 fence;
		HeapTupleFreeze plan, saved;
		FullTransactionId creator = FullTransactionIdFromEpochAndXid(1, 77);
		FullTransactionId cutoff = FullTransactionIdFromEpochAndXid(1, 100);
		init_xmin_plan_fixture(&page, &proofs, &fence);
		memset(&saved, 0x5a, sizeof(saved));
		plan = saved;
		if (variant < 3) creator = FullTransactionIdFromEpochAndXid(1, variant);
		if (variant >= 3 && variant < 6) cutoff = FullTransactionIdFromEpochAndXid(2, variant - 3);
		if (variant == 6) creator = FullTransactionIdFromEpochAndXid(1, 78);
		if (variant == 7) page.tuples[0].infomask = HEAP_XMIN_FROZEN;
		if (variant == 8) page.tuples[0].infomask = HEAP_XMIN_INVALID;
		if (variant == 9) page.tuples[0].infomask = HEAP_MOVED_OFF;
		if (variant == 10) page.tuples[0].infomask = HEAP_MOVED_IN;
		UT_ASSERT(!cluster_heap_freeze_prepare_xmin_v1(&page, &proofs, &fence,
			0, creator, cutoff, 10, &plan));
		UT_ASSERT(memcmp(&plan, &saved, sizeof(plan)) == 0);
	}
}

UT_TEST(xmin_plan_requires_finite_fullxid_and_history_bounds)
{
	for (int variant = 0; variant < 3; variant++) {
		ClusterHeapFreezePageBatchV1 page;
		ClusterHeapFreezeProofBatchV1 proofs;
		ClusterHeapFreezeFinalFenceV1 fence;
		HeapTupleFreeze plan, saved;
		FullTransactionId limit = FullTransactionIdFromEpochAndXid(1, 100);
		SCN horizon = 10;

		init_xmin_plan_fixture(&page, &proofs, &fence);
		memset(&saved, 0x5a, sizeof(saved));
		plan = saved;
		if (variant != 0)
			limit = FullTransactionIdFromU64(UINT64_MAX);
		if (variant != 1)
			horizon = (SCN)UINT64_MAX;
		UT_ASSERT(!cluster_heap_freeze_prepare_xmin_v1(&page, &proofs, &fence,
			0, FullTransactionIdFromEpochAndXid(1, 77), limit, horizon, &plan));
		UT_ASSERT(memcmp(&plan, &saved, sizeof(plan)) == 0);
	}
}

UT_TEST(xmin_plan_missing_arguments_and_bad_bounds_leave_output_untouched)
{
	for (int variant = 0; variant < 7; variant++) {
		ClusterHeapFreezePageBatchV1 page;
		ClusterHeapFreezeProofBatchV1 proofs;
		ClusterHeapFreezeFinalFenceV1 fence;
		HeapTupleFreeze plan, saved;
		init_xmin_plan_fixture(&page, &proofs, &fence);
		memset(&saved, 0x5a, sizeof(saved));
		plan = saved;
		if (variant == 4) page.tuple_count = MaxHeapTuplesPerPage + 1;
		if (variant == 5) proofs.proof_count = CLUSTER_HEAP_FREEZE_MAX_PROOF_BATCH + 1;
		UT_ASSERT(!cluster_heap_freeze_prepare_xmin_v1(variant == 0 ? NULL : &page,
			variant == 1 ? NULL : &proofs, variant == 2 ? NULL : &fence,
			variant == 6 ? page.tuple_count : 0,
			FullTransactionIdFromEpochAndXid(1, 77),
			FullTransactionIdFromEpochAndXid(1, 100), 10, variant == 3 ? NULL : &plan));
		UT_ASSERT(memcmp(&plan, &saved, sizeof(plan)) == 0);
	}
}

UT_TEST(creation_provenance_only_follows_exact_physical_creation_target)
{
	ClusterHeapFreezePageBatchV1 page;
	ClusterHeapFreezeProofBatchV1 proofs;
	ClusterHeapFreezeFinalFenceV1 fence;
	FullTransactionId original = FullTransactionIdFromEpochAndXid(9, 77);

	init_creation_binding_fixture(&page, &proofs, &fence);
	proofs.proofs[0].resolution.local_original_full_xid = original;
	UT_ASSERT_EQ(cluster_heap_freeze_bind_creation_targets_v1(&page, &proofs, &fence), 1);
	UT_ASSERT(FullTransactionIdEquals(page.tuples[0].original_creator_full_xid, original));
	UT_ASSERT(!FullTransactionIdIsValid(page.tuples[1].original_creator_full_xid));
	/* Equal raw xmin/current carrier cannot propagate provenance to another
	 * physical row. Rebinding retires the old row observation. */
	proofs.proofs[0].resolution.local_creation_target.offset = 2;
	UT_ASSERT_EQ(cluster_heap_freeze_bind_creation_targets_v1(&page, &proofs, &fence), 1);
	UT_ASSERT(!FullTransactionIdIsValid(page.tuples[0].original_creator_full_xid));
	UT_ASSERT(FullTransactionIdEquals(page.tuples[1].original_creator_full_xid, original));
}

UT_TEST(creation_provenance_missing_or_raw_mismatch_never_guesses_an_epoch)
{
	const FullTransactionId invalid[] = {
		InvalidFullTransactionId, FullTransactionIdFromU64(2),
		FullTransactionIdFromEpochAndXid(9, 78)
	};

	for (unsigned variant = 0; variant < lengthof(invalid); variant++) {
		ClusterHeapFreezePageBatchV1 page;
		ClusterHeapFreezeProofBatchV1 proofs;
		ClusterHeapFreezeFinalFenceV1 fence;

		init_creation_binding_fixture(&page, &proofs, &fence);
		proofs.proofs[0].resolution.local_original_full_xid = invalid[variant];
		UT_ASSERT_EQ(cluster_heap_freeze_bind_creation_targets_v1(&page, &proofs, &fence), 1);
		UT_ASSERT_EQ(page.tuples[0].creation_target_proof_index, 0);
		UT_ASSERT(!FullTransactionIdIsValid(page.tuples[0].original_creator_full_xid));
	}
}

UT_TEST(creation_provenance_rejection_clears_preceding_attempt)
{
	for (int variant = 0; variant < 4; variant++) {
		ClusterHeapFreezePageBatchV1 page;
		ClusterHeapFreezeProofBatchV1 proofs;
		ClusterHeapFreezeFinalFenceV1 fence;

		init_creation_binding_fixture(&page, &proofs, &fence);
		for (int row = 0; row < 2; row++)
			page.tuples[row].original_creator_full_xid = FullTransactionIdFromEpochAndXid(9, 77);
		switch (variant) {
		case 0: proofs.proof_count = 0; break;
		case 1: fence.page_version++; break;
		case 2: proofs.proofs[0].resolution.local_canonical_generation = 0; break;
		case 3: proofs.proofs[0].resolution.local_creation_target.valid = false; break;
		}
		UT_ASSERT_EQ(cluster_heap_freeze_bind_creation_targets_v1(&page, &proofs, &fence), 0);
		UT_ASSERT(!FullTransactionIdIsValid(page.tuples[0].original_creator_full_xid));
		UT_ASSERT(!FullTransactionIdIsValid(page.tuples[1].original_creator_full_xid));
	}
}

UT_TEST(creation_provenance_keeps_distinct_carriers_with_equal_raw_xmin)
{
	ClusterHeapFreezePageBatchV1 page;
	ClusterHeapFreezeProofBatchV1 proofs;
	ClusterHeapFreezeFinalFenceV1 fence;
	ClusterHeapFreezeCanonicalProofV1 swap;
	HeapTupleFreeze plan;
	FullTransactionId first = FullTransactionIdFromEpochAndXid(9, 77);
	FullTransactionId second = FullTransactionIdFromEpochAndXid(10, 77);

	init_creation_binding_fixture(&page, &proofs, &fence);
	page.carrier_request_count = proofs.proof_count = 2;
	page.carrier_requests[1] = page.carrier_requests[0];
	page.carrier_requests[1].locator.itl_slot_index = 1;
	proofs.proofs[0].resolution.local_original_full_xid = first;
	proofs.proofs[1] = proofs.proofs[0];
	proofs.proofs[1].requested_locator.itl_slot_index = 1;
	proofs.proofs[1].resolution.locator_echo.itl_slot_index = 1;
	proofs.proofs[1].resolution.local_canonical.slot_offset = 1;
	proofs.proofs[1].resolution.local_creation_target.offset = 2;
	proofs.proofs[1].resolution.local_original_full_xid = second;
	page.tuples[1].carrier_request_index = 1;
	page.tuples[1].itl_slot_index = 1;
	swap = proofs.proofs[0]; proofs.proofs[0] = proofs.proofs[1]; proofs.proofs[1] = swap;
	UT_ASSERT_EQ(cluster_heap_freeze_bind_creation_targets_v1(&page, &proofs, &fence), 2);
	UT_ASSERT(FullTransactionIdEquals(page.tuples[0].original_creator_full_xid, first));
	UT_ASSERT(FullTransactionIdEquals(page.tuples[1].original_creator_full_xid, second));
	for (uint16 index = 0; index < 2; index++)
		UT_ASSERT(cluster_heap_freeze_prepare_xmin_v1(&page, &proofs, &fence,
			index, index == 0 ? first : second,
			FullTransactionIdFromEpochAndXid(11, 100), 10, &plan));
}


UT_TEST(xmin_candidates_keep_exact_physical_origin_without_native_plans)
{
	ClusterHeapFreezePageBatchV1 page, saved_page;
	ClusterHeapFreezeProofBatchV1 proofs, saved_proofs;
	ClusterHeapFreezeFinalFenceV1 fence;
	ClusterHeapFreezeXminCandidateBatchV1 out;

	init_xmin_plan_fixture(&page, &proofs, &fence);
	saved_page = page; saved_proofs = proofs;
	memset(&out, 0xa5, sizeof(out));
	UT_ASSERT(cluster_heap_freeze_select_xmin_candidates_v1(&page, &proofs, &fence,
		FullTransactionIdFromEpochAndXid(1, 100), 10, &out));
	UT_ASSERT_EQ(out.target_count, 1);
	UT_ASSERT_EQ(out.targets[0].tuple_index, 0);
	UT_ASSERT(FullTransactionIdEquals(out.targets[0].original_creator_full_xid,
		FullTransactionIdFromEpochAndXid(1, 77)));
	UT_ASSERT_EQ(memcmp(&page, &saved_page, sizeof(page)), 0);
	UT_ASSERT_EQ(memcmp(&proofs, &saved_proofs, sizeof(proofs)), 0);
}

UT_TEST(xmin_candidates_require_strict_observed_fullxid_and_scn_age)
{
	for (unsigned variant = 0; variant < 4; variant++) {
		ClusterHeapFreezePageBatchV1 page;
		ClusterHeapFreezeProofBatchV1 proofs;
		ClusterHeapFreezeFinalFenceV1 fence;
		ClusterHeapFreezeXminCandidateBatchV1 out;
		FullTransactionId limit = FullTransactionIdFromEpochAndXid(1, 100);

		init_xmin_plan_fixture(&page, &proofs, &fence);
		if (variant == 0) limit = FullTransactionIdFromEpochAndXid(1, 77);
		if (variant == 1) limit = FullTransactionIdFromEpochAndXid(0, 100);
		if (variant >= 2) proofs.proofs[0].resolution.commit_scn = 10 + variant - 2;
		UT_ASSERT(cluster_heap_freeze_select_xmin_candidates_v1(&page, &proofs, &fence,
			limit, 10, &out));
		UT_ASSERT_EQ(out.target_count, 0);
	}
}

UT_TEST(xmin_candidates_cannot_use_raw_alias_or_saved_creation_index)
{
	for (unsigned variant = 0; variant < 6; variant++) {
		ClusterHeapFreezePageBatchV1 page;
		ClusterHeapFreezeProofBatchV1 proofs;
		ClusterHeapFreezeFinalFenceV1 fence;
		ClusterHeapFreezeXminCandidateBatchV1 out;

		init_xmin_plan_fixture(&page, &proofs, &fence);
		switch (variant) {
		case 0: page.tuples[0].original_creator_full_xid = InvalidFullTransactionId; break;
		case 1: proofs.proofs[0].resolution.local_original_full_xid = InvalidFullTransactionId; break;
		case 2: page.tuples[0].original_creator_full_xid = FullTransactionIdFromEpochAndXid(2, 77); break;
		case 3: proofs.proofs[0].resolution.local_original_full_xid = FullTransactionIdFromEpochAndXid(2, 77); break;
		case 4: proofs.proofs[0].resolution.local_creation_target.offset = 2; break;
		case 5: page.tuples[0].infomask |= HEAP_XMIN_FROZEN; break;
		}
		UT_ASSERT(cluster_heap_freeze_select_xmin_candidates_v1(&page, &proofs, &fence,
			FullTransactionIdFromEpochAndXid(3, 100), 10, &out));
		UT_ASSERT_EQ(out.target_count, 0);
	}
}

UT_TEST(xmin_candidates_preserve_distinct_carriers_with_equal_raw_xmin)
{
	ClusterHeapFreezePageBatchV1 page;
	ClusterHeapFreezeProofBatchV1 proofs;
	ClusterHeapFreezeFinalFenceV1 fence;
	ClusterHeapFreezeCanonicalProofV1 swap;
	ClusterHeapFreezeXminCandidateBatchV1 out;
	FullTransactionId first = FullTransactionIdFromEpochAndXid(9, 77);
	FullTransactionId second = FullTransactionIdFromEpochAndXid(10, 77);

	init_creation_binding_fixture(&page, &proofs, &fence);
	page.carrier_request_count = proofs.proof_count = 2;
	page.carrier_requests[1] = page.carrier_requests[0];
	page.carrier_requests[1].locator.itl_slot_index = 1;
	proofs.proofs[0].resolution.local_original_full_xid = first;
	proofs.proofs[1] = proofs.proofs[0];
	proofs.proofs[1].requested_locator.itl_slot_index = 1;
	proofs.proofs[1].resolution.locator_echo.itl_slot_index = 1;
	proofs.proofs[1].resolution.local_canonical.slot_offset = 1;
	proofs.proofs[1].resolution.local_creation_target.offset = 2;
	proofs.proofs[1].resolution.local_original_full_xid = second;
	page.tuples[1].carrier_request_index = 1;
	page.tuples[1].itl_slot_index = 1;
	swap = proofs.proofs[0]; proofs.proofs[0] = proofs.proofs[1]; proofs.proofs[1] = swap;
	UT_ASSERT_EQ(cluster_heap_freeze_bind_creation_targets_v1(&page, &proofs, &fence), 2);
	UT_ASSERT(cluster_heap_freeze_select_xmin_candidates_v1(&page, &proofs, &fence,
		FullTransactionIdFromEpochAndXid(11, 100), 10, &out));
	UT_ASSERT_EQ(out.target_count, 2);
	UT_ASSERT_EQ(out.targets[0].tuple_index, 0);
	UT_ASSERT_EQ(out.targets[1].tuple_index, 1);
	UT_ASSERT(FullTransactionIdEquals(out.targets[0].original_creator_full_xid, first));
	UT_ASSERT(FullTransactionIdEquals(out.targets[1].original_creator_full_xid, second));
}

UT_TEST(xmin_candidates_bad_bounds_preserve_output)
{
	for (unsigned variant = 0; variant < 5; variant++) {
		ClusterHeapFreezePageBatchV1 page;
		ClusterHeapFreezeProofBatchV1 proofs;
		ClusterHeapFreezeFinalFenceV1 fence;
		ClusterHeapFreezeXminCandidateBatchV1 out, saved;
		FullTransactionId limit = FullTransactionIdFromEpochAndXid(1, 100);
		SCN horizon = 10;

		init_xmin_plan_fixture(&page, &proofs, &fence);
		memset(&out, 0xa5, sizeof(out)); saved = out;
		if (variant == 0) limit = InvalidFullTransactionId;
		if (variant == 1) limit = FullTransactionIdFromEpochAndXid(1, FrozenTransactionId);
		if (variant == 2) limit = FullTransactionIdFromU64(UINT64_MAX);
		if (variant == 3) horizon = InvalidScn;
		if (variant == 4) horizon = UINT64_MAX;
		UT_ASSERT(!cluster_heap_freeze_select_xmin_candidates_v1(&page, &proofs, &fence,
			limit, horizon, &out));
		UT_ASSERT_EQ(memcmp(&out, &saved, sizeof(out)), 0);
	}
}

UT_TEST(xmin_candidates_changed_fence_or_missing_proof_preserve_output)
{
	for (unsigned variant = 0; variant < 7; variant++) {
		ClusterHeapFreezePageBatchV1 page;
		ClusterHeapFreezeProofBatchV1 proofs;
		ClusterHeapFreezeFinalFenceV1 fence;
		ClusterHeapFreezeXminCandidateBatchV1 out, saved;

		init_xmin_plan_fixture(&page, &proofs, &fence);
		memset(&out, 0xa5, sizeof(out)); saved = out;
		if (variant == 0) fence.page_version++;
		if (variant == 1) fence.page_lsn++;
		if (variant == 2) fence.master_sequence++;
		if (variant == 3) proofs.proof_count = 0;
		if (variant == 4) page.tuple_count = MaxHeapTuplesPerPage + 1;
		UT_ASSERT(!cluster_heap_freeze_select_xmin_candidates_v1(variant == 5 ? NULL : &page,
			&proofs, &fence, FullTransactionIdFromEpochAndXid(1, 100), 10,
			variant == 6 ? NULL : &out));
		UT_ASSERT_EQ(memcmp(&out, &saved, sizeof(out)), 0);
	}
}

UT_TEST(xmin_candidates_empty_selection_retires_previous_batch)
{
	ClusterHeapFreezePageBatchV1 page;
	ClusterHeapFreezeProofBatchV1 proofs;
	ClusterHeapFreezeFinalFenceV1 fence;
	ClusterHeapFreezeXminCandidateBatchV1 out, empty = {0};

	init_xmin_plan_fixture(&page, &proofs, &fence);
	UT_ASSERT(cluster_heap_freeze_select_xmin_candidates_v1(&page, &proofs, &fence,
		FullTransactionIdFromEpochAndXid(1, 100), 10, &out));
	UT_ASSERT_EQ(out.target_count, 1);
	UT_ASSERT(cluster_heap_freeze_select_xmin_candidates_v1(&page, &proofs, &fence,
		FullTransactionIdFromEpochAndXid(1, 77), 10, &out));
	UT_ASSERT_EQ(memcmp(&out, &empty, sizeof(out)), 0);
}

UT_TEST(xmin_candidates_revalidate_even_without_cached_binding_index)
{
	ClusterHeapFreezePageBatchV1 page;
	ClusterHeapFreezeProofBatchV1 proofs;
	ClusterHeapFreezeFinalFenceV1 fence;
	ClusterHeapFreezeXminCandidateBatchV1 out;

	init_xmin_plan_fixture(&page, &proofs, &fence);
	page.tuples[0].creation_target_proof_index = CLUSTER_HEAP_FREEZE_NO_CREATION_TARGET;
	UT_ASSERT(cluster_heap_freeze_select_xmin_candidates_v1(&page, &proofs, &fence,
		FullTransactionIdFromEpochAndXid(1, 100), 10, &out));
	UT_ASSERT_EQ(out.target_count, 1);
}

static void
init_xmin_batch_plan_fixture(ClusterHeapFreezePageBatchV1 *page,
	ClusterHeapFreezeProofBatchV1 *proofs, ClusterHeapFreezeFinalFenceV1 *fence,
	ClusterHeapFreezeXminTargetV1 targets[2])
{
	init_creation_binding_fixture(page, proofs, fence);
	page->carrier_request_count = proofs->proof_count = 2;
	page->carrier_requests[1] = page->carrier_requests[0];
	page->carrier_requests[1].locator.itl_slot_index = 1;
	proofs->proofs[0].resolution.local_original_full_xid = FullTransactionIdFromEpochAndXid(1, 77);
	proofs->proofs[1] = proofs->proofs[0];
	proofs->proofs[1].requested_locator.itl_slot_index = 1;
	proofs->proofs[1].resolution.locator_echo.itl_slot_index = 1;
	proofs->proofs[1].resolution.local_canonical.slot_offset = 1;
	proofs->proofs[1].resolution.local_creation_target.offset = 2;
	page->tuples[1].carrier_request_index = 1;
	page->tuples[1].itl_slot_index = 1;
	UT_ASSERT_EQ(cluster_heap_freeze_bind_creation_targets_v1(page, proofs, fence), 2);
	for (int i = 0; i < 2; i++)
		targets[i] = (ClusterHeapFreezeXminTargetV1){i, page->tuples[i].original_creator_full_xid};
}

UT_TEST(xmin_batch_plan_keeps_target_order_and_native_fields)
{
	ClusterHeapFreezePageBatchV1 page;
	ClusterHeapFreezeProofBatchV1 proofs;
	ClusterHeapFreezeFinalFenceV1 fence;
	ClusterHeapFreezeXminTargetV1 targets[2], swap;
	HeapTupleFreeze plans[3], expected[2], tail;
	TransactionId horizon = InvalidTransactionId;
	FullTransactionId limit = FullTransactionIdFromEpochAndXid(1, 100);

	init_xmin_batch_plan_fixture(&page, &proofs, &fence, targets);
	swap = targets[0]; targets[0] = targets[1]; targets[1] = swap;
	memset(plans, 0x5a, sizeof(plans)); tail = plans[2];
	for (int i = 0; i < 2; i++)
		UT_ASSERT(cluster_heap_freeze_prepare_xmin_v1(&page, &proofs, &fence,
			targets[i].tuple_index, targets[i].original_creator_full_xid, limit, 10, &expected[i]));
	UT_ASSERT(cluster_heap_freeze_prepare_xmin_batch_v1(&page, &proofs, &fence,
		targets, 2, limit, 10, plans, lengthof(plans), &horizon));
	UT_ASSERT_EQ(memcmp(plans, expected, sizeof(expected)), 0);
	UT_ASSERT_EQ(memcmp(&plans[2], &tail, sizeof(tail)), 0);
	UT_ASSERT_EQ(horizon, 77);
}

UT_TEST(xmin_batch_plan_later_refusal_preserves_all_outputs)
{
	for (int variant = 0; variant < 8; variant++) {
		ClusterHeapFreezePageBatchV1 page;
		ClusterHeapFreezeProofBatchV1 proofs;
		ClusterHeapFreezeFinalFenceV1 fence;
		ClusterHeapFreezeXminTargetV1 targets[2];
		HeapTupleFreeze plans[2], saved[2];
		TransactionId horizon = 12345;
		SCN h = 10;

		init_xmin_batch_plan_fixture(&page, &proofs, &fence, targets);
		memset(plans, 0x5a, sizeof(plans)); memcpy(saved, plans, sizeof(saved));
		switch (variant) {
		case 0: targets[1].original_creator_full_xid = FullTransactionIdFromEpochAndXid(2, 77); break;
		case 1: targets[1] = targets[0]; break;
		case 2: targets[1].tuple_index = page.tuple_count; break;
		case 3: page.tuples[1].offset = page.tuples[0].offset; break;
		case 4: fence.page_version++; break;
		case 5: proofs.proofs[1].resolution.outcome = CLUSTER_TX_ABORTED; break;
		case 6: page.tuples[1].original_creator_full_xid = InvalidFullTransactionId; break;
		case 7: h = proofs.proofs[1].resolution.commit_scn; break;
		}
		UT_ASSERT(!cluster_heap_freeze_prepare_xmin_batch_v1(&page, &proofs, &fence,
			targets, 2, FullTransactionIdFromEpochAndXid(3, 100), h,
			plans, lengthof(plans), &horizon));
		UT_ASSERT_EQ(memcmp(plans, saved, sizeof(plans)), 0);
		UT_ASSERT_EQ(horizon, 12345);
	}
}

UT_TEST(xmin_batch_plan_runtime_counts_and_capacity_are_bounded)
{
	for (int variant = 0; variant < 9; variant++) {
		ClusterHeapFreezePageBatchV1 page;
		ClusterHeapFreezeProofBatchV1 proofs;
		ClusterHeapFreezeFinalFenceV1 fence;
		ClusterHeapFreezeXminTargetV1 targets[2];
		HeapTupleFreeze plans[2], saved[2];
		TransactionId horizon = 12345;
		uint16 count = variant == 0 ? 0 : variant == 1 ? MaxHeapTuplesPerPage + 1 : 2;

		init_xmin_batch_plan_fixture(&page, &proofs, &fence, targets);
		memset(plans, 0x5a, sizeof(plans)); memcpy(saved, plans, sizeof(saved));
		UT_ASSERT(!cluster_heap_freeze_prepare_xmin_batch_v1(
			variant == 3 ? NULL : &page, variant == 4 ? NULL : &proofs,
			variant == 5 ? NULL : &fence, variant == 6 ? NULL : targets, count,
			FullTransactionIdFromEpochAndXid(1, 100), 10,
			variant == 7 ? NULL : plans, variant == 2 ? 1 : lengthof(plans),
			variant == 8 ? NULL : &horizon));
		UT_ASSERT_EQ(memcmp(plans, saved, sizeof(plans)), 0);
		UT_ASSERT_EQ(horizon, 12345);
	}
}

UT_TEST(xmin_batch_plan_rejects_individually_valid_raw_alias_epochs)
{
	ClusterHeapFreezePageBatchV1 page;
	ClusterHeapFreezeProofBatchV1 proofs;
	ClusterHeapFreezeFinalFenceV1 fence;
	ClusterHeapFreezeXminTargetV1 targets[2];
	HeapTupleFreeze plans[2], saved[2], single;
	TransactionId horizon = 12345;
	FullTransactionId limit = FullTransactionIdFromEpochAndXid(3, 100);

	init_xmin_batch_plan_fixture(&page, &proofs, &fence, targets);
	proofs.proofs[1].resolution.local_original_full_xid = FullTransactionIdFromEpochAndXid(2, 77);
	UT_ASSERT_EQ(cluster_heap_freeze_bind_creation_targets_v1(&page, &proofs, &fence), 2);
	targets[1].original_creator_full_xid = page.tuples[1].original_creator_full_xid;
	for (int i = 0; i < 2; i++)
		UT_ASSERT(cluster_heap_freeze_prepare_xmin_v1(&page, &proofs, &fence,
			i, targets[i].original_creator_full_xid, limit, 10, &single));
	memset(plans, 0x5a, sizeof(plans)); memcpy(saved, plans, sizeof(saved));
	UT_ASSERT(!cluster_heap_freeze_prepare_xmin_batch_v1(&page, &proofs, &fence,
		targets, 2, limit, 10, plans, lengthof(plans), &horizon));
	UT_ASSERT_EQ(memcmp(plans, saved, sizeof(plans)), 0);
	UT_ASSERT_EQ(horizon, 12345);
}

UT_TEST(xmin_batch_plan_single_target_preserves_original_plan)
{
	ClusterHeapFreezePageBatchV1 page;
	ClusterHeapFreezeProofBatchV1 proofs;
	ClusterHeapFreezeFinalFenceV1 fence;
	ClusterHeapFreezeXminTargetV1 targets[2];
	HeapTupleFreeze plans[2], expected, tail;
	TransactionId horizon = InvalidTransactionId;
	FullTransactionId limit = FullTransactionIdFromEpochAndXid(1, 100);

	init_xmin_batch_plan_fixture(&page, &proofs, &fence, targets);
	UT_ASSERT(cluster_heap_freeze_prepare_xmin_v1(&page, &proofs, &fence,
		0, targets[0].original_creator_full_xid, limit, 10, &expected));
	memset(plans, 0x5a, sizeof(plans)); tail = plans[1];
	UT_ASSERT(cluster_heap_freeze_prepare_xmin_batch_v1(&page, &proofs, &fence,
		targets, 1, limit, 10, plans, lengthof(plans), &horizon));
	UT_ASSERT_EQ(memcmp(&plans[0], &expected, sizeof(expected)), 0);
	UT_ASSERT_EQ(memcmp(&plans[1], &tail, sizeof(tail)), 0);
	UT_ASSERT_EQ(horizon, 77);
}

int
main(void)
{
	UT_PLAN(97);
	UT_RUN(xmin_batch_plan_keeps_target_order_and_native_fields);
	UT_RUN(xmin_batch_plan_later_refusal_preserves_all_outputs);
	UT_RUN(xmin_batch_plan_runtime_counts_and_capacity_are_bounded);
	UT_RUN(xmin_batch_plan_rejects_individually_valid_raw_alias_epochs);
	UT_RUN(xmin_batch_plan_single_target_preserves_original_plan);
	UT_RUN(xmin_candidates_keep_exact_physical_origin_without_native_plans);
	UT_RUN(xmin_candidates_require_strict_observed_fullxid_and_scn_age);
	UT_RUN(xmin_candidates_cannot_use_raw_alias_or_saved_creation_index);
	UT_RUN(xmin_candidates_preserve_distinct_carriers_with_equal_raw_xmin);
	UT_RUN(xmin_candidates_bad_bounds_preserve_output);
	UT_RUN(xmin_candidates_changed_fence_or_missing_proof_preserve_output);
	UT_RUN(xmin_candidates_empty_selection_retires_previous_batch);
	UT_RUN(xmin_candidates_revalidate_even_without_cached_binding_index);

	UT_RUN(creation_provenance_only_follows_exact_physical_creation_target);
	UT_RUN(creation_provenance_missing_or_raw_mismatch_never_guesses_an_epoch);
	UT_RUN(creation_provenance_rejection_clears_preceding_attempt);
	UT_RUN(creation_provenance_keeps_distinct_carriers_with_equal_raw_xmin);
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
	UT_RUN(real_scan_consumes_rows_captured_under_both_original_locks);
	UT_RUN(real_scan_replaces_and_invalidates_prepared_batch);
	UT_RUN(requests_share_only_the_same_physical_carrier);
	UT_RUN(requests_keep_updater_and_lock_carriers_distinct_from_raw_xmin);
	UT_RUN(data_groups_keep_distinct_carriers_in_one_physical_block);
	UT_RUN(data_groups_preserve_first_appearance_and_duplicate_addresses);
	UT_RUN(data_groups_separate_complete_segment_and_origin);
	UT_RUN(nondata_legacy_requests_keep_witness_without_a_read_group);
	UT_RUN(real_scan_groups_owned_requests_after_original_locks_and_pins);
	UT_RUN(unusable_carriers_keep_rows_without_a_request);
	UT_RUN(missing_and_unused_carriers_cannot_supply_requests);
	UT_RUN(request_batch_is_bounded_by_eight_physical_slots);
	UT_RUN(real_scan_prepares_owned_requests_and_resets_next_page);
	UT_RUN(real_scan_prepares_after_heap_lock_and_both_pins_are_released);
	UT_RUN(released_live_page_cannot_replace_owned_preparation_input);
	UT_RUN(cleanup_fallback_prepares_only_its_refreshed_capture);
	UT_RUN(indexed_lpdead_exit_also_releases_before_preparation);
	UT_RUN(nontarget_keeps_vm_pin_reuse_and_never_prepares);
	UT_RUN(each_captured_page_gets_one_unpinned_preparation);
	UT_RUN(cleanup_fallback_to_empty_discards_pending_capture);
	UT_RUN(noindex_lpdead_exit_prepares_once_after_all_buffers_are_released);
	UT_RUN(share_native_hint_change_discards_captured_input);
	UT_RUN(cleanup_prune_change_discards_input_on_both_final_exits);
	UT_RUN(noindex_heap_vacuum_change_discards_captured_input);
	UT_RUN(malformed_current_page_is_not_a_changed_input_retry);
	UT_RUN(physical_validation_matches_owned_image_and_ignores_identity_padding);
	UT_RUN(physical_validation_rejects_each_space_and_block_identity_change);
	UT_RUN(physical_validation_covers_all_page_bytes_and_itl_carriers);
	UT_RUN(physical_validation_distinguishes_invalid_arguments_and_layout);
	UT_RUN(retained_counts_cover_physical_rows_without_merging_raw_xids);
	UT_RUN(only_both_frozen_hint_bits_suppress_creator_work);
	UT_RUN(frozen_creator_does_not_clear_committed_or_lock_only_xmax);
	UT_RUN(multis_stay_opaque_and_special_raw_xmax_does_not_supply_proof);
	UT_RUN(real_scan_accumulates_retained_work_after_both_original_lock_paths);
	UT_RUN(retained_work_survives_replacement_and_empty_page_invalidation);
	UT_RUN(changed_inputs_record_missing_coverage_without_counting_old_rows);
	UT_RUN(retained_or_changed_inputs_independently_prevent_catalog_publication);
	UT_RUN(zero_retained_counts_never_remove_the_original_shared_restriction);
	UT_RUN(canonical_proof_requires_exact_row_identity_and_c1b);
	UT_RUN(canonical_proof_builder_copies_transport_and_fence);
	UT_RUN(canonical_proof_builder_rejects_null_without_clobber);
	UT_RUN(final_fence_mismatch_retries_without_proof);
	UT_RUN(unproven_tt_and_snapshot_never_admit_freeze);
	UT_RUN(proof_batch_is_all_or_nothing_for_conditional_apply);
	UT_RUN(conditional_apply_requires_exact_carriers_and_final_fence);
	UT_RUN(exact_subset_can_cleanout_without_proving_the_whole_page);
	UT_RUN(cleanout_subset_rejects_duplicate_or_unrequested_proofs);
	UT_RUN(cleanout_subset_requires_the_same_final_current_fence);
	UT_RUN(cleanout_subset_never_promotes_missing_canonical_evidence);
	UT_RUN(creation_binding_is_physical_even_with_equal_raw_xids_and_carriers);
	UT_RUN(creation_binding_keeps_own_carrier_and_proof_order);
	UT_RUN(creation_binding_rejects_incomplete_or_changed_evidence_and_resets_rows);
	UT_RUN(creation_binding_missing_or_malformed_batches_cannot_reuse_old_indexes);
	UT_RUN(xmin_plan_requires_the_bound_exact_origin_fullxid);
	UT_RUN(xmin_plan_requires_strict_fullxid_and_scn_age);
	UT_RUN(xmin_plan_compares_scn_time_without_node_id_ordering);
	UT_RUN(xmin_plan_preserves_xmax_multi_and_other_header_bits);
	UT_RUN(xmin_plan_revalidates_physical_target_instead_of_old_binding);
	UT_RUN(xmin_plan_rejects_incomplete_proof_and_changed_final_fence);
	UT_RUN(xmin_plan_rejects_special_raw_fullxids_and_legacy_xmin_states);
	UT_RUN(xmin_plan_requires_finite_fullxid_and_history_bounds);
	UT_RUN(xmin_plan_missing_arguments_and_bad_bounds_leave_output_untouched);
	UT_DONE();
	return ut_failed_count ? 1 : 0;
}
