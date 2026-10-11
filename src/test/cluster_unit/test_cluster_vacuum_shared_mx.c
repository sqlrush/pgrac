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
	fixture_peer = fixture_identity = cluster_shared_config = true;
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
	for (unsigned mode = 0; mode < 4; mode++)
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
			fixture_bind_relation(&state, &relation);
			lazy_scan_heap(&state);
			fixture_publish_cutoffs(&state);
			UT_ASSERT_EQ(fixture_prune + fixture_noprune, 1);
			UT_ASSERT_EQ(deferred_pages(), 0);
			UT_ASSERT_EQ(state.NewRelfrozenXid, 100);
			UT_ASSERT_EQ(state.NewRelminMxid, 100);
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
	UT_ASSERT_EQ(state.NewRelfrozenXid, 100);
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

int
main(void)
{
	UT_PLAN(8);
	UT_RUN(share_scan_preserves_every_reference_before_native_consumer);
	UT_RUN(cleanup_scan_preserves_before_prune_and_freeze);
	UT_RUN(raw_multi_value_never_selects_local_reader);
	UT_RUN(both_cutoffs_remain_unpublished_after_a_preserved_page);
	UT_RUN(nontarget_modes_retain_the_original_native_path);
	UT_RUN(no_multi_and_empty_pages_do_not_acquire_a_preservation_claim);
	UT_RUN(other_pages_dead_items_and_tail_are_preserved_independently);
	UT_RUN(identity_and_io_failures_are_not_converted_to_preservation);
	UT_DONE();
	return ut_failed_count ? 1 : 0;
}
