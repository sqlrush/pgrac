/* Original raw VM reader and metadata reports; buffer, heap I/O, allocation,
 * fmgr and cancellation are controlled leaves, not native backend lifetimes.
 * Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com> */
int vm_report_inherited_main(void);
#define main vm_report_inherited_main
#include "test_cluster_vm_read_visibility.c"
#undef main

#include "funcapi.h"

Datum pg_visibility_map(PG_FUNCTION_ARGS);
Datum pg_visibility(PG_FUNCTION_ARGS);
Datum pg_visibility_map_summary(PG_FUNCTION_ARGS);

static RelationData report_relation;
static FormData_pg_class report_form;
static PGAlignedBlock report_heap[4];
static HeapTupleData report_result;
static Datum report_values[3];
static unsigned report_nattrs;
static void *report_allocation;
static unsigned report_vm_pins, report_heap_reads, report_heap_unlocks, report_checks;
static unsigned report_cancel_at, report_error_at, report_drops;
static BlockNumber report_nblocks, report_heap_block;
static bool report_heap_locked, report_drop;
static sigjmp_buf report_jump;

static void
report_outside_vm(void)
{
	if (cluster_heap_maintenance_restricted(&report_relation))
		UT_ASSERT_EQ(report_vm_pins, 0);
}

static void
report_interrupt(void)
{
	report_outside_vm();
	if (++report_checks == report_cancel_at)
		siglongjmp(report_jump, 1);
}

static uint8
report_raw_status(Relation rel, BlockNumber block, Buffer *buffer)
{
	bool had_pin = BufferIsValid(*buffer);
	uint8 status = visibilitymap_get_status(rel, block, buffer);

	if (!had_pin && BufferIsValid(*buffer))
		report_vm_pins++;
	return status;
}

static void
report_release(Buffer buffer)
{
	UT_ASSERT_EQ(report_vm_pins, 1);
	UT_ASSERT_EQ(buffer, 1);
	if (report_vm_pins > 0)
		report_vm_pins--;
	ReleaseBuffer(buffer);
}

static Buffer
report_read_heap(Relation rel, ForkNumber fork, BlockNumber block,
				 ReadBufferMode mode, BufferAccessStrategy strategy pg_attribute_unused())
{
	report_outside_vm();
	UT_ASSERT(rel == &report_relation && fork == MAIN_FORKNUM && mode == RBM_NORMAL);
	UT_ASSERT(block < report_nblocks && !report_heap_locked);
	report_heap_block = block;
	if (++report_heap_reads == report_error_at)
		siglongjmp(report_jump, 1);
	/* Model the original holder's DROP refusing a pinned VM. The next raw
	 * read can observe the changed byte only after a fresh pin. */
	if (report_drop && report_vm_pins == 0) {
		PageGetContents(disk_vm.data)[0] &= ~(VISIBILITYMAP_VALID_BITS << 2);
		report_drop = false;
		report_drops++;
	}
	return 2;
}

static void
report_lock_heap(Buffer buffer, int mode)
{
	report_outside_vm();
	UT_ASSERT(buffer == 2 && mode == BUFFER_LOCK_SHARE && !report_heap_locked);
	report_heap_locked = true;
}

static Page
report_heap_page(Buffer buffer)
{
	UT_ASSERT(buffer == 2 && report_heap_locked);
	return report_heap[report_heap_block].data;
}

static void
report_unlock_heap(Buffer buffer)
{
	UT_ASSERT(buffer == 2 && report_heap_locked);
	report_heap_locked = false;
	report_heap_unlocks++;
}

static void *
report_allocate(Size size)
{
	UT_ASSERT(report_allocation == NULL);
	report_allocation = calloc(1, size);
	UT_ASSERT_NOT_NULL(report_allocation);
	return report_allocation;
}

static HeapTuple
report_form_tuple(TupleDesc desc pg_attribute_unused(), const Datum *values,
				  const bool *nulls pg_attribute_unused())
{
	memcpy(report_values, values, sizeof(Datum) * report_nattrs);
	report_result.t_data = (HeapTupleHeader)report_values;
	return &report_result;
}

static void
report_close(Relation rel, LOCKMODE mode)
{
	UT_ASSERT(rel == &report_relation && mode == AccessShareLock);
	UT_ASSERT_EQ(report_vm_pins, 0);
}

#undef CHECK_FOR_INTERRUPTS
#define CHECK_FOR_INTERRUPTS() report_interrupt()
#define visibilitymap_get_status report_raw_status
#define relation_open(oid, mode) ((void)(oid), (void)(mode), &report_relation)
#define relation_close report_close
#undef RelationGetNumberOfBlocks
#define RelationGetNumberOfBlocks(rel) ((void)(rel), report_nblocks)
#define GetAccessStrategy(kind) ((void)(kind), (BufferAccessStrategy)1)
#define palloc0 report_allocate
#define pg_visibility_tupdesc(block, pd) ((void)(block), report_nattrs = (pd) ? 3 : 2, (TupleDesc)1)
#define get_call_result_type(call, rsinfo, desc) ((void)(call), (void)(rsinfo), report_nattrs = 2, *(desc) = (TupleDesc)1, TYPEFUNC_COMPOSITE)
#define heap_form_tuple report_form_tuple
#define HeapTupleGetDatum(tuple) PointerGetDatum((tuple)->t_data)
#undef ReleaseBuffer
#define ReleaseBuffer report_release
#define ReadBuffer(rel, block) report_read_heap(rel, MAIN_FORKNUM, block, RBM_NORMAL, NULL)
#define ReadBufferExtended report_read_heap
#undef LockBuffer
#define LockBuffer report_lock_heap
#undef BufferGetPage
#define BufferGetPage report_heap_page
#define UnlockReleaseBuffer report_unlock_heap
#ifdef FIXTURE_NO_ASSERTS
#undef USE_ASSERT_CHECKING
#undef Assert
#define Assert(condition) ((void)0)
#endif
#include "test_cluster_vm_report_pin.inc"

static void
report_reset(bool shared, bool local)
{
	free(report_allocation);
	report_allocation = NULL;
	memset(&report_relation, 0, sizeof(report_relation));
	memset(&report_form, 0, sizeof(report_form));
	report_relation.rd_rel = &report_form;
	report_relation.rd_id = 101;
	report_form.relkind = RELKIND_RELATION;
	report_form.relpersistence = local ? RELPERSISTENCE_TEMP : RELPERSISTENCE_PERMANENT;
	fixture_shared = shared;
	fixture_reads = fixture_releases = 0;
	fixture_smgr.smgr_cached_nblocks[VISIBILITYMAP_FORKNUM] = 1;
	memset(&disk_vm, 0, sizeof(disk_vm));
	((PageHeader)disk_vm.data)->pd_lower = SizeOfPageHeaderData;
	((PageHeader)disk_vm.data)->pd_upper = BLCKSZ;
	/* Raw diagnostics must retain the AF-only fourth pair. */
	PageGetContents(disk_vm.data)[0] = 0x87;
	for (int i = 0; i < 4; i++) {
		memset(&report_heap[i], 0, sizeof(report_heap[i]));
		if (i % 2 == 0)
			PageSetAllVisible(report_heap[i].data);
	}
	report_vm_pins = report_heap_reads = report_heap_unlocks = report_checks = 0;
	report_cancel_at = report_error_at = report_drops = 0;
	report_heap_locked = report_drop = false;
	report_nblocks = 4;
	report_nattrs = 2;
}

static void
report_call(bool summary, bool pd, BlockNumber block)
{
	LOCAL_FCINFO(call, 2);
	Datum result;

	InitFunctionCallInfoData(*call, NULL, summary ? 1 : 2, InvalidOid, NULL, NULL);
	call->args[0].value = ObjectIdGetDatum(101);
	call->args[0].isnull = false;
	call->args[1].value = Int64GetDatum(block);
	call->args[1].isnull = false;
	memset(report_values, 0, sizeof(report_values));
	result = summary ? pg_visibility_map_summary(call) : pd ? pg_visibility(call) : pg_visibility_map(call);
	UT_ASSERT_EQ(result, PointerGetDatum(report_values));
}

UT_TEST(shared_scalar_reports_release_exactly_once)
{
	for (int pd = 0; pd < 2; pd++) {
		report_reset(true, false);
		report_call(false, pd, 0);
		UT_ASSERT(DatumGetBool(report_values[0]) && DatumGetBool(report_values[1]));
		UT_ASSERT_EQ(fixture_releases, 1);
		UT_ASSERT_EQ(report_heap_reads, pd);
		if (pd)
			UT_ASSERT(DatumGetBool(report_values[2]));
	}
}

UT_TEST(shared_page_report_retires_vm_before_heap_and_next_interrupt)
{
	const uint8 expected[] = {7, 1, 4, 2};
	vbits *info;

	report_reset(true, false);
	info = collect_visibility_data(101, true);
	UT_ASSERT_EQ(info->count, 4);
	UT_ASSERT(memcmp(info->bits, expected, sizeof(expected)) == 0);
	UT_ASSERT_EQ(report_heap_reads, 4);
	UT_ASSERT_EQ(report_heap_unlocks, 4);
	UT_ASSERT_EQ(fixture_releases, 4);
}

UT_TEST(shared_map_report_retires_vm_before_next_interrupt)
{
	const uint8 expected[] = {3, 1, 0, 2};
	vbits *info;

	report_reset(true, false);
	info = collect_visibility_data(101, false);
	UT_ASSERT(memcmp(info->bits, expected, sizeof(expected)) == 0);
	UT_ASSERT_EQ(report_heap_reads, 0);
	UT_ASSERT_EQ(fixture_releases, 4);
}

UT_TEST(shared_summary_preserves_raw_counts_with_no_retained_vm_pin)
{
	report_reset(true, false);
	report_call(true, false, 0);
	UT_ASSERT_EQ(DatumGetInt64(report_values[0]), 2);
	UT_ASSERT_EQ(DatumGetInt64(report_values[1]), 2);
	UT_ASSERT_EQ(fixture_releases, 4);
}

UT_TEST(native_and_local_reports_preserve_original_pin_cache)
{
	for (int local = 0; local < 2; local++) {
		report_reset(local, local);
		(void)collect_visibility_data(101, true);
		UT_ASSERT_EQ(fixture_reads, 1);
		UT_ASSERT_EQ(fixture_releases, 1);
		report_reset(local, local);
		report_call(true, false, 0);
		UT_ASSERT_EQ(fixture_reads, 1);
		UT_ASSERT_EQ(fixture_releases, 1);
	}
}

UT_TEST(shared_report_cancellation_reaches_no_vm_pin)
{
	for (int kind = 0; kind < 3; kind++) {
		report_reset(true, false);
		report_cancel_at = 2;
		if (sigsetjmp(report_jump, 1) == 0) {
			if (kind == 2) report_call(true, false, 0);
			else (void)collect_visibility_data(101, kind != 0);
			UT_ASSERT(false);
		}
		UT_ASSERT_EQ(report_checks, 2);
		UT_ASSERT_EQ(report_vm_pins, 0);
		UT_ASSERT(!report_heap_locked);
	}
}

UT_TEST(shared_report_heap_error_reaches_no_vm_pin)
{
	for (int scalar = 0; scalar < 2; scalar++) {
		report_reset(true, false);
		report_error_at = 1;
		if (sigsetjmp(report_jump, 1) == 0) {
			if (scalar) report_call(false, true, 0);
			else (void)collect_visibility_data(101, true);
			UT_ASSERT(false);
		}
		UT_ASSERT_EQ(report_heap_reads, 1);
		UT_ASSERT_EQ(report_vm_pins, 0);
		UT_ASSERT(!report_heap_locked);
	}
}

UT_TEST(missing_vm_preserves_zero_map_and_independent_pd_data)
{
	const uint8 expected[] = {4, 0, 4, 0};
	vbits *info;

	report_reset(true, false);
	fixture_smgr.smgr_cached_nblocks[VISIBILITYMAP_FORKNUM] = 0;
	info = collect_visibility_data(101, true);
	UT_ASSERT(memcmp(info->bits, expected, sizeof(expected)) == 0);
	UT_ASSERT_EQ(fixture_reads, 0);
	UT_ASSERT_EQ(fixture_releases, 0);
}

UT_TEST(empty_relation_reports_remain_empty)
{
	vbits *info;

	report_reset(true, false);
	report_nblocks = 0;
	info = collect_visibility_data(101, true);
	UT_ASSERT_EQ(info->count, 0);
	report_call(true, false, 0);
	UT_ASSERT_EQ(DatumGetInt64(report_values[0]), 0);
	UT_ASSERT_EQ(DatumGetInt64(report_values[1]), 0);
	UT_ASSERT_EQ(fixture_reads, 0);
}

UT_TEST(scalar_eof_keeps_raw_map_bits_without_heap_read)
{
	report_reset(true, false);
	PageGetContents(disk_vm.data)[1] = VISIBILITYMAP_VALID_BITS;
	report_call(false, true, 4);
	UT_ASSERT(DatumGetBool(report_values[0]) && DatumGetBool(report_values[1]));
	UT_ASSERT_EQ(report_heap_reads, 0);
	UT_ASSERT_EQ(report_vm_pins, 0);
	UT_ASSERT(!DatumGetBool(report_values[2]));
}

UT_TEST(shared_report_allows_holder_drop_before_next_map_observation)
{
	vbits *info;

	report_reset(true, false);
	report_drop = true;
	info = collect_visibility_data(101, true);
	UT_ASSERT_EQ(report_drops, 1);
	UT_ASSERT_EQ(info->bits[0], 7);
	UT_ASSERT_EQ(info->bits[1], 0);
	UT_ASSERT_EQ(fixture_reads, 4);
}

int
main(void)
{
	UT_PLAN(11);
	UT_RUN(shared_scalar_reports_release_exactly_once);
	UT_RUN(shared_page_report_retires_vm_before_heap_and_next_interrupt);
	UT_RUN(shared_map_report_retires_vm_before_next_interrupt);
	UT_RUN(shared_summary_preserves_raw_counts_with_no_retained_vm_pin);
	UT_RUN(native_and_local_reports_preserve_original_pin_cache);
	UT_RUN(shared_report_cancellation_reaches_no_vm_pin);
	UT_RUN(shared_report_heap_error_reaches_no_vm_pin);
	UT_RUN(missing_vm_preserves_zero_map_and_independent_pd_data);
	UT_RUN(empty_relation_reports_remain_empty);
	UT_RUN(scalar_eof_keeps_raw_map_bits_without_heap_read);
	UT_RUN(shared_report_allows_holder_drop_before_next_map_observation);
	free(report_allocation);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
