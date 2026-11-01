/* Author: SqlRush <sqlrush@gmail.com>
 * Copyright (c) 2026, pgrac contributors.
 * Real native VACUUM -> index AM dispatch -> second heap pass/page/WAL setup.
 * AM storage, buffer scheduling and WAL sink are controlled boundaries. This
 * does not certify shared authority, actual index WAL replay or crash safety.
 */
#include "postgres.h"
#include "access/amapi.h"
#include "access/heapam.h"
#include "access/heapam_xlog.h"
#include "access/htup_details.h"
#include "access/visibilitymap.h"
#include "access/xact.h"
#include "access/xlog.h"
#include "access/xloginsert.h"
#include "catalog/index.h"
#include "cluster/cluster_guc.h"
#include "cluster/cluster_heap_freeze.h"
#include "cluster/cluster_heap_horizon.h"
#include "cluster/cluster_space_storage.h"
#include "commands/progress.h"
#include "commands/vacuum.h"
#include "miscadmin.h"
#include "storage/bufmgr.h"
#include "storage/freespace.h"
#include "utils/backend_progress.h"
#include "utils/rel.h"
#include "unit_test.h"

UT_DEFINE_GLOBALS();
bool cluster_shared_config = false;
bool cluster_recmerge_window_active = false;
bool cluster_recmerge_apply_foreign = false;
uint64 cluster_recmerge_window_scn, cluster_recmerge_window_own_lsn;
int NBuffers = 128;
int NLocBuffer;
int wal_level = WAL_LEVEL_REPLICA;
bool VacuumFailsafeActive, VacuumCostActive;
int VacuumCostBalance;
volatile uint32 CritSectionCount;
sigjmp_buf *PG_exception_stack;
ErrorContextCallback *error_context_stack;
static int error_code;
static PGAlignedBlock pages[2], original_pages[2];
static ItemPointerData tids[8];
static bool references[3][8];
static unsigned expected_indexes, am_calls, am_completed, am_wal_events;
static unsigned failsafe_checks, heap_reads, heap_dirties, heap_wal_events;
static unsigned fsm_writes, delay_calls;
static int heap_locks, heap_pins, vm_pins;
static int failsafe_at, error_at, reindex_at;
static int am_error;
static bool null_stats;
static Buffer wal_buffer;
static xl_heap_vacuum wal_header;
static OffsetNumber wal_offsets[MaxHeapTuplesPerPage];
static unsigned wal_offset_count;
static unsigned batch;

void
ExceptionalCondition(const char *condition, const char *file, int line)
{
	fprintf(stderr, "assertion %s %s:%d\n", condition, file, line);
	abort();
}
bool
errstart(int level, const char *domain)
{
	(void)domain;
	if (level >= ERROR)
		error_code = ERRCODE_INTERNAL_ERROR;
	return level >= ERROR;
}
bool
errstart_cold(int level, const char *domain)
{
	return errstart(level, domain);
}
int
errcode(int code)
{
	error_code = code;
	return 0;
}
int
errmsg(const char *format, ...)
{
	(void)format;
	return 0;
}
int
errmsg_internal(const char *format, ...)
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
int
errhint(const char *format, ...)
{
	(void)format;
	return 0;
}
void
errfinish(const char *file, int line, const char *function)
{
	(void)file; (void)line; (void)function;
	if (PG_exception_stack != NULL)
		siglongjmp(*PG_exception_stack, 1);
	abort();
}
void
pg_re_throw(void)
{
	if (PG_exception_stack != NULL)
		siglongjmp(*PG_exception_stack, 1);
	abort();
}
char *
pstrdup(const char *text)
{
	char *copy = strdup(text);
	if (copy == NULL)
		abort();
	return copy;
}
void
pfree(void *pointer)
{
	free(pointer);
}
bool
ReindexIsProcessingIndex(Oid oid)
{
	return reindex_at >= 0 && oid == (Oid)(200 + reindex_at);
}

void
XLogBeginInsert(void)
{
	UT_ASSERT_EQ(heap_locks, 1);
	UT_ASSERT_EQ(CritSectionCount, 1);
	wal_buffer = InvalidBuffer;
	wal_offset_count = 0;
}
void
XLogRegisterData(char *data, uint32 length)
{
	UT_ASSERT_EQ(length, SizeOfHeapVacuum);
	memcpy(&wal_header, data, length);
}
void
XLogRegisterBuffer(uint8 id, Buffer buffer, uint8 flags)
{
	UT_ASSERT_EQ(id, 0);
	UT_ASSERT_EQ(flags, REGBUF_STANDARD);
	wal_buffer = buffer;
}
void
XLogRegisterBufData(uint8 id, char *data, uint32 length)
{
	UT_ASSERT_EQ(id, 0);
	UT_ASSERT(length <= sizeof(wal_offsets));
	UT_ASSERT_EQ(length % sizeof(OffsetNumber), 0);
	wal_offset_count = length / sizeof(OffsetNumber);
	memcpy(wal_offsets, data, length);
}
XLogRecPtr
XLogInsert(RmgrId id, uint8 info)
{
	UT_ASSERT_EQ(id, RM_HEAP2_ID);
	UT_ASSERT_EQ(info, XLOG_HEAP2_VACUUM);
	UT_ASSERT_EQ(am_completed, expected_indexes);
	UT_ASSERT_EQ(am_wal_events, expected_indexes);
	UT_ASSERT(wal_buffer == 1 || wal_buffer == 2);
	UT_ASSERT_EQ(wal_header.nunused, 1);
	UT_ASSERT_EQ(wal_offset_count, 1);
	UT_ASSERT_EQ(wal_offsets[0], batch == 0 ? (wal_buffer == 1 ? 1 : 2) : 2);
	UT_ASSERT(!ItemIdIsUsed(PageGetItemId((Page)pages[wal_buffer - 1].data, wal_offsets[0])));
	heap_wal_events++;
	return 100 + heap_wal_events;
}

#include "test_cluster_l1b_index_order.inc"

static struct RelationData heap_relation, index_relations[3];
static FormData_pg_class heap_form, index_forms[3];
static IndexAmRoutine am_routine;
static Relation index_list[3];
static IndexBulkDeleteResult am_stats[3], *stats_list[3];
static union { max_align_t alignment; char bytes[offsetof(VacDeadItems, items) + sizeof(tids)]; } inventory;
static LVRelState state;

static IndexBulkDeleteResult *
test_am_bulkdelete(IndexVacuumInfo *info, IndexBulkDeleteResult *stats,
				   IndexBulkDeleteCallback callback, void *callback_state)
{
	unsigned index = info->index->rd_id - 200;

	UT_ASSERT(index < expected_indexes);
	UT_ASSERT_EQ(heap_locks + heap_pins, 0);
	UT_ASSERT_EQ(info->heaprel, &heap_relation);
	UT_ASSERT(info->estimated_count && !info->analyze_only);
	UT_ASSERT_EQ(info->num_heap_tuples, 42);
	UT_ASSERT(stats == NULL || stats == &am_stats[index]);
	UT_ASSERT(ItemIdIsDead(PageGetItemId((Page)pages[0].data, batch == 0 ? 1 : 2)));
	if (batch == 0)
		UT_ASSERT(ItemIdIsDead(PageGetItemId((Page)pages[1].data, 2)));
	am_calls++;
	if (error_at == (int)index)
		ereport(ERROR, (errcode(am_error), errmsg("controlled index AM interruption")));
	for (unsigned item = 0; item < lengthof(tids); item++) {
		bool remove = callback(&tids[item], callback_state);
		bool expected = batch == 0 ? (item == 0 || item == 5) : item == 1;

		UT_ASSERT_EQ(remove, expected);
		if (remove)
			references[index][item] = false;
	}
	am_completed++;
	am_wal_events++; /* A modeled AM boundary event, not real WAL durability. */
	return null_stats ? NULL : &am_stats[index];
}

static void
new_invocation(unsigned indexes)
{
	if (state.indname != NULL)
		free(state.indname); /* Error context teardown is a fixture boundary. */
	memset(&state, 0, sizeof(state));
	memset(stats_list, 0, sizeof(stats_list));
	state.rel = &heap_relation;
	state.indrels = index_list;
	state.nindexes = indexes;
	state.indstats = stats_list;
	state.dead_items = (VacDeadItems *)inventory.bytes;
	state.dead_items->max_items = lengthof(tids);
	state.dead_items->num_items = batch == 0 ? 2 : 1;
	state.dead_items->items[0] = tids[batch == 0 ? 0 : 1];
	if (batch == 0)
		state.dead_items->items[1] = tids[5];
	state.lpdead_items = state.dead_items->num_items;
	state.lpdead_item_pages = batch == 0 ? 2 : 1;
	state.rel_pages = 2;
	state.do_index_vacuuming = state.do_index_cleanup = state.do_rel_truncate = true;
	state.bstrategy = (BufferAccessStrategy)&inventory;
	state.dbname = state.relnamespace = state.relname = "fixture";
	state.blkno = InvalidBlockNumber;
	state.offnum = InvalidOffsetNumber;
	expected_indexes = indexes;
	am_calls = am_completed = am_wal_events = failsafe_checks = 0;
	heap_reads = heap_dirties = heap_wal_events = fsm_writes = delay_calls = 0;
	heap_locks = heap_pins = vm_pins = 0;
	CritSectionCount = 0;
	VacuumFailsafeActive = false;
	VacuumCostActive = true;
	VacuumCostBalance = 17;
	failsafe_at = error_at = reindex_at = -1;
	error_code = 0;
	am_error = ERRCODE_RAISE_EXCEPTION;
	null_stats = false;
}

static void
reset_fixture(unsigned indexes)
{
	memset(&heap_relation, 0, sizeof(heap_relation));
	memset(&heap_form, 0, sizeof(heap_form));
	heap_relation.rd_rel = &heap_form;
	heap_relation.rd_id = 100;
	heap_form.relpersistence = RELPERSISTENCE_PERMANENT;
	heap_form.reltuples = 42;
	memset(&am_routine, 0, sizeof(am_routine));
	am_routine.ambulkdelete = test_am_bulkdelete;
	for (unsigned index = 0; index < lengthof(index_list); index++) {
		memset(&index_relations[index], 0, sizeof(index_relations[index]));
		memset(&index_forms[index], 0, sizeof(index_forms[index]));
		index_list[index] = &index_relations[index];
		index_list[index]->rd_rel = &index_forms[index];
		index_list[index]->rd_id = 200 + index;
		index_list[index]->rd_indam = &am_routine;
		snprintf(NameStr(index_forms[index].relname), NAMEDATALEN, "index%u", index);
		for (unsigned item = 0; item < lengthof(tids); item++)
			references[index][item] = true;
	}
	for (unsigned block = 0; block < lengthof(pages); block++) {
		Page page = (Page)pages[block].data;
		PageInit(page, BLCKSZ, 0);
		for (unsigned off = 1; off <= 4; off++) {
			char tuple[64] = { 0 };
			HeapTupleHeader header = (HeapTupleHeader)tuple;
			header->t_hoff = SizeofHeapTupleHeader;
			HeapTupleHeaderSetXmin(header, 1000 + block * 4 + off);
			tuple[63] = 40 + block * 4 + off;
			UT_ASSERT_EQ(PageAddItemExtended(page, tuple, sizeof(tuple), off, PAI_IS_HEAP), off);
			ItemPointerSet(&tids[block * 4 + off - 1], block, off);
		}
	}
	ItemIdSetDead(PageGetItemId((Page)pages[0].data, 1));
	ItemIdSetDead(PageGetItemId((Page)pages[1].data, 2));
	ItemIdSetRedirect(PageGetItemId((Page)pages[0].data, 3), 4);
	batch = 0;
	new_invocation(indexes);
	memcpy(original_pages, pages, sizeof(pages));
}

static bool
invoke(void)
{
	volatile bool caught = false;
	PG_TRY();
	{
		lazy_vacuum(&state);
	}
	PG_CATCH();
	{
		caught = true;
	}
	PG_END_TRY();
	return caught;
}

static void
assert_retained(void)
{
	UT_ASSERT_EQ(memcmp(pages, original_pages, sizeof(pages)), 0);
	UT_ASSERT_EQ(heap_reads + heap_dirties + heap_wal_events + fsm_writes, 0);
	UT_ASSERT_EQ(heap_locks + heap_pins + vm_pins + CritSectionCount, 0);
}

UT_TEST(test_every_index_precedes_real_heap_second_pass_and_wal)
{
	for (unsigned n = 1; n <= 3; n++)
		for (unsigned optional_stats = 0; optional_stats < 2; optional_stats++) {
			reset_fixture(n);
			null_stats = optional_stats != 0;
			UT_ASSERT(!invoke());
			UT_ASSERT_EQ(am_calls, n);
			UT_ASSERT_EQ(state.num_index_scans, 1);
			UT_ASSERT_EQ(state.dead_items->num_items, 0);
			UT_ASSERT_EQ(heap_reads, 2);
			UT_ASSERT_EQ(heap_dirties, 2);
			UT_ASSERT_EQ(heap_wal_events, 2);
			UT_ASSERT_EQ(fsm_writes, 2);
			UT_ASSERT_EQ(heap_locks + heap_pins + vm_pins + CritSectionCount, 0);
			for (unsigned index = 0; index < n; index++)
				for (unsigned item = 0; item < lengthof(tids); item++)
					UT_ASSERT_EQ(references[index][item], item != 0 && item != 5);
			UT_ASSERT(!ItemIdIsUsed(PageGetItemId((Page)pages[0].data, 1)));
			UT_ASSERT(!ItemIdIsUsed(PageGetItemId((Page)pages[1].data, 2)));
			UT_ASSERT(ItemIdIsRedirected(PageGetItemId((Page)pages[0].data, 3)));
			UT_ASSERT_EQ(ItemIdGetRedirect(PageGetItemId((Page)pages[0].data, 3)), 4);
			for (unsigned block = 0; block < 2; block++)
				for (unsigned off = 1; off <= 4; off++) {
					ItemId old = PageGetItemId((Page)original_pages[block].data, off);
					ItemId now = PageGetItemId((Page)pages[block].data, off);
					if ((block == 0 && off == 1) || (block == 1 && off == 2))
						continue;
					UT_ASSERT_EQ(memcmp(old, now, sizeof(ItemIdData)), 0);
					if (ItemIdIsNormal(old))
						UT_ASSERT_EQ(memcmp(PageGetItem((Page)original_pages[block].data, old),
							PageGetItem((Page)pages[block].data, now), ItemIdGetLength(old)), 0);
				}
		}
}

UT_TEST(test_am_error_and_cancel_at_each_index_keep_the_original_dead_pages)
{
	static const int codes[] = { ERRCODE_RAISE_EXCEPTION, ERRCODE_QUERY_CANCELED };
	for (unsigned code = 0; code < lengthof(codes); code++)
		for (unsigned index = 0; index < 3; index++) {
			reset_fixture(3);
			error_at = index;
			am_error = codes[code];
			UT_ASSERT(invoke());
			UT_ASSERT_EQ(error_code, codes[code]);
			UT_ASSERT_EQ(am_calls, index + 1);
			UT_ASSERT_EQ(am_completed, index);
			UT_ASSERT_EQ(state.num_index_scans, 0);
			UT_ASSERT_EQ(state.dead_items->num_items, 2);
			assert_retained();
		}
}

UT_TEST(test_failsafe_before_or_after_any_index_cannot_reuse_heap_slots)
{
	for (int check = 0; check <= 3; check++) {
		reset_fixture(3);
		failsafe_at = check;
		UT_ASSERT(!invoke());
		UT_ASSERT_EQ(am_calls, check);
		UT_ASSERT(VacuumFailsafeActive);
		UT_ASSERT(!state.do_index_vacuuming && !state.do_index_cleanup && !state.do_rel_truncate);
		UT_ASSERT_EQ(state.dead_items->num_items, 0);
		UT_ASSERT_EQ(state.num_index_scans, check == 0 ? 0 : 1);
		assert_retained();
	}
}

UT_TEST(test_disabled_and_bypass_leave_the_original_lp_dead)
{
	for (unsigned bypass = 0; bypass < 2; bypass++) {
		reset_fixture(3);
		if (bypass) {
			state.consider_bypass_optimization = true;
			state.rel_pages = 1000;
		} else
			state.do_index_vacuuming = state.do_index_cleanup = false;
		UT_ASSERT(!invoke());
		UT_ASSERT_EQ(am_calls, 0);
		UT_ASSERT_EQ(state.dead_items->num_items, 0);
		assert_retained();
	}
}

UT_TEST(test_reindex_and_missing_am_use_original_dispatch_rejection)
{
	for (unsigned index = 0; index < 3; index++) {
		reset_fixture(3);
		reindex_at = index;
		UT_ASSERT(invoke());
		UT_ASSERT_EQ(error_code, ERRCODE_FEATURE_NOT_SUPPORTED);
		UT_ASSERT_EQ(am_calls, index);
		assert_retained();
	}
	reset_fixture(3);
	am_routine.ambulkdelete = NULL;
	UT_ASSERT(invoke());
	UT_ASSERT_EQ(am_calls, 0);
	assert_retained();
}

UT_TEST(test_fresh_invocation_retries_every_index_after_partial_delete)
{
	for (unsigned index = 0; index < 3; index++) {
		reset_fixture(3);
		error_at = index;
		UT_ASSERT(invoke());
		assert_retained();
		/* New native invocation: preserved physical leaves, fresh inventory/state. */
		new_invocation(3);
		UT_ASSERT(!invoke());
		UT_ASSERT_EQ(am_calls, 3);
		UT_ASSERT_EQ(heap_wal_events, 2);
		UT_ASSERT_EQ(state.num_index_scans, 1);
	}
}

UT_TEST(test_fresh_batch_does_not_reap_a_new_tuple_at_an_old_tid)
{
	char tuple[64] = { 0 };
	char preserved[64];
	reset_fixture(3);
	UT_ASSERT(!invoke());
	UT_ASSERT_EQ(state.dead_items->num_items, 0);
	HeapTupleHeaderSetXmin((HeapTupleHeader)tuple, 9001);
	((HeapTupleHeader)tuple)->t_hoff = SizeofHeapTupleHeader;
	tuple[63] = 99;
	UT_ASSERT_EQ(PageAddItemExtended((Page)pages[0].data, tuple, sizeof(tuple), 1,
								PAI_IS_HEAP | PAI_OVERWRITE), 1);
	memcpy(preserved, tuple, sizeof(tuple));
	ItemIdSetDead(PageGetItemId((Page)pages[0].data, 2));
	for (unsigned index = 0; index < 3; index++)
		references[index][0] = true;
	batch = 1;
	new_invocation(3);
	UT_ASSERT(!invoke());
	UT_ASSERT_EQ(heap_wal_events, 1);
	UT_ASSERT(ItemIdIsNormal(PageGetItemId((Page)pages[0].data, 1)));
	UT_ASSERT_EQ(memcmp(preserved, PageGetItem((Page)pages[0].data,
							PageGetItemId((Page)pages[0].data, 1)), sizeof(preserved)), 0);
	for (unsigned index = 0; index < 3; index++) {
		UT_ASSERT(references[index][0]);
		UT_ASSERT(!references[index][1]);
	}
}

int
main(void)
{
	UT_PLAN(7);
	UT_RUN(test_every_index_precedes_real_heap_second_pass_and_wal);
	UT_RUN(test_am_error_and_cancel_at_each_index_keep_the_original_dead_pages);
	UT_RUN(test_failsafe_before_or_after_any_index_cannot_reuse_heap_slots);
	UT_RUN(test_disabled_and_bypass_leave_the_original_lp_dead);
	UT_RUN(test_reindex_and_missing_am_use_original_dispatch_rejection);
	UT_RUN(test_fresh_invocation_retries_every_index_after_partial_delete);
	UT_RUN(test_fresh_batch_does_not_reap_a_new_tuple_at_an_old_tid);
	UT_DONE();
	return ut_failed_count != 0;
}
