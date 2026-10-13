/*-------------------------------------------------------------------------
 *
 * test_cluster_heap_maintenance.c
 *    Exercise the production retained-tuple and all-visible predicates.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *    src/test/cluster_unit/test_cluster_heap_maintenance.c
 *
 * NOTES
 *    Tuple layout and production decisions are real. Transaction status and
 *    buffer lookup are deterministic leaves, not a physical SQL scenario.
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/heapam.h"
#include "access/htup_details.h"
#include "access/multixact.h"
#include "access/visibilitymap.h"
#include "cluster/cluster_space_storage.h"
#include "cluster/cluster_visibility_resolve.h"
#include "commands/progress.h"
#include "commands/vacuum.h"
#include "executor/instrument.h"
#include "pgstat.h"
#include "storage/bufmgr.h"
#include "storage/procarray.h"
#include "utils/rel.h"
#include "unit_test.h"

UT_DEFINE_GLOBALS();

static PGAlignedBlock fixture_page;
static bool fixture_storage;
static bool fixture_deleting;
static unsigned fixture_kept;
static unsigned fixture_prepare, fixture_execute, fixture_should, fixture_vm_reads;
WalUsage pgWalUsage;

/* Only fields consumed by the unchanged production predicate are needed. */
typedef struct LVRelState
{
	Relation rel;
	struct VacuumCutoffs cutoffs;
	OffsetNumber offnum;
	bool shared_maintenance_restricted;
	bool aggressive, versioned, skipwithvm, skippedallvis;
	ClusterSpaceIdentity identity;
	GlobalVisState *vistest;
	VacDeadItems *dead_items;
	TransactionId NewRelfrozenXid;
	MultiXactId NewRelminMxid;
	BlockNumber frozen_pages, lpdead_item_pages, missed_dead_pages, rel_pages;
	int nindexes;
	int64 tuples_deleted, tuples_frozen, lpdead_items, live_tuples;
	int64 recently_dead_tuples, missed_dead_tuples;
} LVRelState;

typedef struct LVPagePruneState
{
	bool hastup, has_lpdead_items, all_visible, all_frozen;
	TransactionId visibility_cutoff_xid;
} LVPagePruneState;

#define SKIP_PAGES_THRESHOLD 32
#define cluster_storage_mode_enabled() fixture_storage
#define cluster_vis_bump_prune_remote_keep_count() (fixture_kept++)
#undef BufferGetPage
#define BufferGetPage(buf) ((void)(buf), (Page)fixture_page.data)
#undef BufferIsValid
#define BufferIsValid(buf) ((buf) != InvalidBuffer)
#define BufferGetBlockNumber(buf) ((void)(buf), 0)
#define SetHintBits(tuple, buffer, bits, xid) ((tuple)->t_infomask |= (bits))
#undef elog
#define elog(...) abort()

void
ExceptionalCondition(const char *condition, const char *file, int line)
{
	(void)condition;
	(void)file;
	(void)line;
	abort();
}

bool
TransactionIdPrecedes(TransactionId left, TransactionId right)
{
	return (int32)(left - right) < 0;
}

bool
TransactionIdFollows(TransactionId left, TransactionId right)
{
	return (int32)(left - right) > 0;
}

bool
TransactionIdIsCurrentTransactionId(TransactionId xid)
{
	(void)xid;
	return false;
}

bool
TransactionIdIsInProgress(TransactionId xid)
{
	return fixture_deleting && xid == 20;
}

bool
TransactionIdDidCommit(TransactionId xid)
{
	return !TransactionIdIsInProgress(xid);
}

bool
MultiXactIdIsRunning(MultiXactId multi, bool is_lock)
{
	(void)multi;
	(void)is_lock;
	abort();
}

TransactionId
HeapTupleGetUpdateXid(HeapTupleHeader tuple)
{
	(void)tuple;
	abort();
}

bool
HeapTupleHeaderIsOnlyLocked(HeapTupleHeader tuple)
{
	(void)tuple;
	abort();
}

bool
heap_tuple_needs_eventual_freeze(HeapTupleHeader tuple)
{
	/* This leaf cannot grant visibility; it only supplies all-frozen output. */
	return !HeapTupleHeaderXminFrozen(tuple);
}

/* Freeze leaves deliberately offer a plan; shared callers must not ask. */
bool
heap_prepare_freeze_tuple(HeapTupleHeader tuple, const struct VacuumCutoffs *cutoffs,
						 HeapPageFreeze *pagefrz, HeapTupleFreeze *plan, bool *total)
{
	(void)tuple; (void)cutoffs; (void)plan;
	fixture_prepare++;
	pagefrz->freeze_required = true;
	pagefrz->FreezePageRelfrozenXid = 70;
	pagefrz->FreezePageRelminMxid = 8;
	*total = true;
	return true;
}

bool
heap_tuple_should_freeze(HeapTupleHeader tuple, const struct VacuumCutoffs *cutoffs,
						TransactionId *xid, MultiXactId *mxid)
{
	(void)tuple; (void)cutoffs;
	fixture_should++;
	*xid = 70; *mxid = 8;
	return true;
}

void
heap_freeze_execute_prepared(Relation rel, Buffer buf, TransactionId horizon,
							HeapTupleFreeze *plans, int ntuples,
							const ClusterSpaceIdentity *identity)
{
	Page page = (Page)fixture_page.data;
	(void)rel; (void)buf; (void)horizon; (void)identity;
	fixture_execute++;
	for (int i = 0; i < ntuples; i++)
	{
		HeapTupleHeader tuple = (HeapTupleHeader)PageGetItem(page,
			PageGetItemId(page, plans[i].offset));
		HeapTupleHeaderSetXminFrozen(tuple);
	}
}

int
heap_page_prune(Relation rel, Buffer buf, TransactionId oldest,
				GlobalVisState *vistest, TransactionId old_snap, TimestampTz old_ts,
				int *nnewlpdead, OffsetNumber *offnum,
				const ClusterSpaceIdentity *identity)
{
	(void)rel; (void)buf; (void)oldest; (void)vistest; (void)old_snap;
	(void)old_ts; (void)offnum; (void)identity;
	*nnewlpdead = 0;
	return 0;
}

void
pgstat_progress_update_param(int index, int64 value)
{
	(void)index; (void)value;
}

uint8
visibilitymap_get_status(Relation rel, BlockNumber block, Buffer *buf)
{
	(void)rel; (void)block; (void)buf;
	fixture_vm_reads++;
	return VISIBILITYMAP_ALL_VISIBLE | VISIBILITYMAP_ALL_FROZEN;
}

void
vacuum_delay_point(void)
{
}

#include "test_cluster_heap_maintenance.inc"

static RelationData fixture_relation;
static FormData_pg_class fixture_form;
static LVRelState fixture_state;

static HeapTupleHeader
setup(bool shared, bool deleting)
{
	Page page = (Page)fixture_page.data;
	PageHeader header = (PageHeader)page;
	ItemId item;
	HeapTupleHeader tuple;

	memset(&fixture_page, 0, sizeof(fixture_page));
	memset(&fixture_relation, 0, sizeof(fixture_relation));
	memset(&fixture_form, 0, sizeof(fixture_form));
	memset(&fixture_state, 0, sizeof(fixture_state));
	fixture_relation.rd_rel = &fixture_form;
	fixture_relation.rd_id = 16384;
	fixture_form.relpersistence = RELPERSISTENCE_PERMANENT;
	fixture_state.rel = &fixture_relation;
	fixture_state.cutoffs.OldestXmin = 100;
	fixture_state.shared_maintenance_restricted = shared;
	fixture_state.NewRelfrozenXid = 100;
	fixture_state.NewRelminMxid = 10;
	fixture_state.rel_pages = 64;
	fixture_state.skipwithvm = true;
	fixture_storage = shared;
	fixture_deleting = deleting;
	fixture_kept = 0;
	fixture_prepare = fixture_execute = fixture_should = fixture_vm_reads = 0;
	header->pd_lower = SizeOfPageHeaderData + sizeof(ItemIdData);
	header->pd_upper = BLCKSZ - MAXALIGN(SizeofHeapTupleHeader);
	header->pd_special = BLCKSZ;
	header->pd_pagesize_version = BLCKSZ | PG_PAGE_LAYOUT_VERSION;
	item = PageGetItemId(page, 1);
	ItemIdSetNormal(item, header->pd_upper, SizeofHeapTupleHeader);
	tuple = (HeapTupleHeader)PageGetItem(page, item);
	tuple->t_hoff = SizeofHeapTupleHeader;
	HeapTupleHeaderSetXmin(tuple, 7);
	HeapTupleHeaderSetXmax(tuple, 20);
	tuple->t_infomask = HEAP_XMIN_COMMITTED;
	if (!deleting)
		tuple->t_infomask |= HEAP_XMAX_COMMITTED;
	return tuple;
}

static void
check_retained_delete(bool deleting)
{
	HeapTupleHeader tuple = setup(true, deleting);
	HeapTupleData input = {0};
	TransactionId dead_after;
	TransactionId cutoff = 99;
	bool frozen = true;
	PGAlignedBlock before = fixture_page;

	input.t_data = tuple;
	input.t_len = SizeofHeapTupleHeader;
	input.t_tableOid = 16384;
	ItemPointerSet(&input.t_self, 0, 1);
	UT_ASSERT_EQ(HeapTupleSatisfiesVacuumHorizon(&input, 1, &dead_after), HEAPTUPLE_LIVE);
	UT_ASSERT_EQ(dead_after, InvalidTransactionId);
	UT_ASSERT_EQ(fixture_kept, 1);
	UT_ASSERT(!heap_page_is_all_visible(&fixture_state, 1, &cutoff, &frozen));
	UT_ASSERT_EQ(cutoff, InvalidTransactionId);
	UT_ASSERT(!frozen);
	UT_ASSERT_EQ(memcmp(&before, &fixture_page, sizeof(before)), 0);
}

UT_TEST(committed_delete_retention_is_not_all_visible)
{
	check_retained_delete(false);
}

UT_TEST(in_progress_delete_retention_is_not_all_visible)
{
	check_retained_delete(true);
}

UT_TEST(native_live_tuple_keeps_original_positive_path)
{
	HeapTupleHeader tuple = setup(false, false);
	TransactionId cutoff;
	bool frozen;

	tuple->t_infomask = HEAP_XMIN_COMMITTED | HEAP_XMAX_INVALID;
	UT_ASSERT(heap_page_is_all_visible(&fixture_state, 1, &cutoff, &frozen));
	UT_ASSERT_EQ(cutoff, 7);
	UT_ASSERT_EQ(fixture_kept, 0);
}

UT_TEST(native_committed_delete_is_not_all_visible)
{
	TransactionId cutoff;
	bool frozen;

	setup(false, false);
	UT_ASSERT(!heap_page_is_all_visible(&fixture_state, 1, &cutoff, &frozen));
	UT_ASSERT_EQ(fixture_kept, 0);
}

UT_TEST(storage_mode_policy_keeps_temp_and_native_unrestricted)
{
	setup(true, false);
	UT_ASSERT(cluster_heap_maintenance_restricted(&fixture_relation));
	fixture_form.relpersistence = RELPERSISTENCE_TEMP;
	UT_ASSERT(!cluster_heap_maintenance_restricted(&fixture_relation));
	fixture_form.relpersistence = RELPERSISTENCE_UNLOGGED;
	UT_ASSERT(cluster_heap_maintenance_restricted(&fixture_relation));
	fixture_storage = false;
	UT_ASSERT(!cluster_heap_maintenance_restricted(&fixture_relation));
}

UT_TEST(shared_prune_keeps_tuple_and_never_prepares_freeze)
{
	for (int mode = 0; mode < 4; mode++)
	{
		HeapTupleHeader tuple = setup(true, mode == 1);
		LVPagePruneState state;
		PGAlignedBlock before;

		if (mode == 2)
			tuple->t_infomask = HEAP_XMIN_COMMITTED | HEAP_XMAX_IS_MULTI;
		if (mode == 3)
			tuple->t_infomask = HEAP_XMIN_COMMITTED | HEAP_XMAX_INVALID;
		fixture_state.aggressive = true;
		before = fixture_page;
		lazy_scan_prune(&fixture_state, 1, 0, fixture_page.data, &state);
		UT_ASSERT(state.hastup && !state.all_visible && !state.all_frozen);
		UT_ASSERT_EQ(fixture_prepare + fixture_execute, 0);
		UT_ASSERT_EQ(fixture_state.tuples_frozen, 0);
		UT_ASSERT_EQ(fixture_state.frozen_pages, 0);
		UT_ASSERT_EQ(fixture_state.NewRelfrozenXid, 100);
		UT_ASSERT_EQ(fixture_state.NewRelminMxid, 10);
		UT_ASSERT_EQ(memcmp(&before, &fixture_page, sizeof(before)), 0);
	}
}

UT_TEST(shared_s_only_aggressive_scan_does_not_request_cleanup_for_freeze)
{
	bool hastup, freespace;
	setup(true, false);
	fixture_state.aggressive = true;
	UT_ASSERT(lazy_scan_noprune(&fixture_state, 1, 0, fixture_page.data,
							   &hastup, &freespace));
	UT_ASSERT(hastup && freespace);
	UT_ASSERT_EQ(fixture_should, 0);
	UT_ASSERT_EQ(fixture_state.NewRelfrozenXid, 100);
	UT_ASSERT_EQ(fixture_state.NewRelminMxid, 10);
}

UT_TEST(native_prune_and_s_only_scan_keep_freeze_planning)
{
	HeapTupleHeader tuple = setup(false, false);
	LVPagePruneState state;
	bool hastup, freespace;

	tuple->t_infomask = HEAP_XMIN_COMMITTED | HEAP_XMAX_INVALID;
	lazy_scan_prune(&fixture_state, 1, 0, fixture_page.data, &state);
	UT_ASSERT_EQ(fixture_prepare, 1);
	UT_ASSERT_EQ(fixture_execute, 1);
	UT_ASSERT_EQ(fixture_state.frozen_pages, 1);
	UT_ASSERT_EQ(fixture_state.NewRelfrozenXid, 70);
	UT_ASSERT_EQ(fixture_state.NewRelminMxid, 8);
	UT_ASSERT(HeapTupleHeaderXminFrozen(tuple));
	setup(false, false);
	fixture_state.aggressive = true;
	UT_ASSERT(!lazy_scan_noprune(&fixture_state, 1, 0, fixture_page.data,
								&hastup, &freespace));
	UT_ASSERT_EQ(fixture_should, 1);
}

UT_TEST(shared_vacuum_does_not_read_raw_vm_to_skip)
{
	Buffer vm = InvalidBuffer;
	bool allvis, skipping;

	setup(true, false);
	UT_ASSERT_EQ(lazy_scan_skip(&fixture_state, &vm, 0, &allvis, &skipping), 0);
	UT_ASSERT_EQ(fixture_vm_reads, 0);
	UT_ASSERT(!allvis && !skipping);
	setup(false, false);
	UT_ASSERT_EQ(lazy_scan_skip(&fixture_state, &vm, 0, &allvis, &skipping), 63);
	UT_ASSERT_EQ(fixture_vm_reads, 64);
	UT_ASSERT(allvis && skipping);
}

int
main(void)
{
	UT_PLAN(9);
	UT_RUN(shared_prune_keeps_tuple_and_never_prepares_freeze);
	UT_RUN(shared_s_only_aggressive_scan_does_not_request_cleanup_for_freeze);
	UT_RUN(native_prune_and_s_only_scan_keep_freeze_planning);
	UT_RUN(shared_vacuum_does_not_read_raw_vm_to_skip);
	UT_RUN(storage_mode_policy_keeps_temp_and_native_unrestricted);
	UT_RUN(committed_delete_retention_is_not_all_visible);
	UT_RUN(in_progress_delete_retention_is_not_all_visible);
	UT_RUN(native_live_tuple_keeps_original_positive_path);
	UT_RUN(native_committed_delete_is_not_all_visible);
	UT_DONE();
	return ut_failed_count != 0;
}
