/*-------------------------------------------------------------------------
 * test_cluster_heap_sample_full.c
 *    Original TABLESAMPLE callbacks consuming private FULL observations.
 *
 * Portions Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * Storage, sampling and transport are explicit boundary leaves. FULL bytes
 * use the actual holder, server and registered landing; MVCC is real code.
 *-------------------------------------------------------------------------
 */
#define CLUSTER_SCAN_FULL_ENTRY sample_unused_scan_main
int sample_unused_scan_main(void);
#include "test_cluster_heap_scan_full.c"
#include "access/tsmapi.h"
#include "nodes/execnodes.h"

static const OffsetNumber sample_offsets[] = { 13, 12, 1, 3, 2, 5 };
typedef struct SampleFixture {
	HeapScanDescData scan;
	RelationData relation;
	FormData_pg_class form;
	SnapshotData snapshot;
	TsmRoutine routine;
	SampleScanState state;
	unsigned block_calls, tuple_calls, remaining_blocks;
	unsigned tuple_fault;
	bool cancel_block;
} SampleFixture;

void
ss_report_location(Relation relation, BlockNumber block)
{
	UT_ASSERT(!scan_locked);
}

static BlockNumber
sample_block(SampleScanState *state, BlockNumber nblocks)
{
	SampleFixture *f = state->tsm_state;
	UT_ASSERT(!scan_locked && nblocks == f->scan.rs_nblocks);
	f->block_calls++;
	if (f->cancel_block)
		ereport(ERROR, (errcode(ERRCODE_QUERY_CANCELED), errmsg("sample block canceled")));
	f->tuple_calls = 0;
	if (f->remaining_blocks == 0)
		return InvalidBlockNumber;
	f->remaining_blocks--;
	return scan_fixture.tag.blockNum;
}

static OffsetNumber
sample_tuple(SampleScanState *state, BlockNumber block, OffsetNumber maxoffset)
{
	SampleFixture *f = state->tsm_state;
	UT_ASSERT_EQ(block, scan_fixture.tag.blockNum);
	/* The real holder keeps 25 line pointers, 13 NORMAL in this fixture. */
	UT_ASSERT_EQ(maxoffset, 25);
	if (f->tuple_fault == 1)
		ereport(ERROR, (errcode(ERRCODE_QUERY_CANCELED), errmsg("sample tuple canceled")));
	if (f->tuple_fault == 2) {
		f->tuple_fault = 0;
		ut_current_epoch++;
	}
	if (f->tuple_fault == 3) {
		f->tuple_fault = 0;
		/* Only inject into the new bounded consumer, not native tableam. */
		return f->scan.rs_owned_kind == 3 ? maxoffset + 1 : InvalidOffsetNumber;
	}
	return f->tuple_calls < lengthof(sample_offsets) ? sample_offsets[f->tuple_calls++]
													 : InvalidOffsetNumber;
}

static bool SampleHeapTupleVisible(TableScanDesc scan, Buffer buffer, HeapTuple tuple,
								   OffsetNumber offset);
#define HeapTupleSatisfiesVisibility scan_native_visibility
#define HeapCheckForSerializableConflictOut scan_conflict
#undef ExecClearTuple
#define ExecClearTuple scan_clear_slot
#include "test_cluster_heap_sample_callbacks.inc"
#undef ExecClearTuple
#undef HeapTupleSatisfiesVisibility
#undef HeapCheckForSerializableConflictOut

static void
sample_setup(SampleFixture *f, bool pagemode)
{
	memset(f, 0, sizeof(*f));
	scan_setup(&f->scan, &f->relation, &f->form, &f->snapshot, pagemode, false);
	f->scan.rs_base.rs_flags = SO_TYPE_SAMPLESCAN | (pagemode ? SO_ALLOW_PAGEMODE : 0);
	f->scan.rs_cblock = InvalidBlockNumber;
	f->scan.rs_startblock = scan_fixture.tag.blockNum;
	f->routine.NextSampleBlock = sample_block;
	f->routine.NextSampleTuple = sample_tuple;
	f->state.tsmroutine = &f->routine;
	f->state.tsm_state = f;
	f->remaining_blocks = 1;
}

/* The catch is outside both production frames, as in the executor. */
static int
sample_next(SampleFixture *f, bool block)
{
	TupleTableSlot slot = { 0 };
	volatile int result = -1;
	ut_error_code = 0;
	ut_error_armed = true;
	PG_TRY();
	{
		result = block ? heapam_scan_sample_next_block((TableScanDesc)&f->scan, &f->state)
					   : heapam_scan_sample_next_tuple((TableScanDesc)&f->scan, &f->state, &slot);
	}
	PG_CATCH();
	{
		result = -1;
	}
	PG_END_TRY();
	ut_error_armed = false;
	UT_ASSERT(PG_exception_stack == NULL && ut_snapshot_scope == NULL);
	return result;
}

static void
sample_check(unsigned kind, bool pagemode)
{
	static const OffsetNumber inserted[] = { 13, 12, 1, 3, 2, 5 };
	static const OffsetNumber updated[] = { 13, 12, 3, 2, 5 };
	static const OffsetNumber deleted[] = { 13, 3, 2, 5 };
	const OffsetNumber *want = kind == 0 ? inserted : kind & 1 ? updated : deleted;
	unsigned expected = kind == 0  ? lengthof(inserted)
						: kind & 1 ? lengthof(updated)
								   : lengthof(deleted);
	SampleFixture f;
	unsigned count = 0;
	int result;
	if (!scan_load(kind))
		return;
	sample_setup(&f, pagemode);
	UT_ASSERT_EQ(sample_next(&f, true), 1);
	while ((result = sample_next(&f, false)) == 1) {
		UT_ASSERT(count < expected);
		if (count < expected)
			UT_ASSERT_EQ(ItemPointerGetOffsetNumber(&scan_stored_tuple.t_self), want[count]);
		count++;
		/* Concurrent current installation must not replace this FULL. */
		memset(ut_visibility_page.data, 0xa5, BLCKSZ);
	}
	if (result < 0)
		printf("# sample kind=%u page=%u: %s\n", kind, pagemode, ut_error_message);
	UT_ASSERT_EQ(result, 0);
	UT_ASSERT_EQ(count, expected);
	UT_ASSERT_EQ(scan_fetches, 1);
	UT_ASSERT_EQ(scan_live_calls + scan_prunes + scan_buffer_stores + scan_conflicts, 0);
	UT_ASSERT(!scan_locked && !f.scan.rs_projection.valid && f.scan.rs_owned_kind == 0);
	UT_ASSERT_EQ(sample_next(&f, true), 0);
	UT_ASSERT(!BufferIsValid(f.scan.rs_cbuf) && !f.scan.rs_inited);
	scan_cleanup(&f.scan);
}

UT_TEST(page_sample_uses_full_rows_in_sampler_order)
{
	for (unsigned kind = 0; kind <= 4; kind++)
		sample_check(kind, true);
}
UT_TEST(row_sample_uses_full_rows_in_sampler_order)
{
	for (unsigned kind = 0; kind <= 4; kind++)
		sample_check(kind, false);
}

UT_TEST(sequential_sampling_uses_the_same_full_page)
{
	SampleFixture f;
	if (!scan_load(0))
		return;
	sample_setup(&f, false);
	f.routine.NextSampleBlock = NULL;
	UT_ASSERT_EQ(sample_next(&f, true), 1);
	UT_ASSERT_EQ(f.scan.rs_cblock, scan_fixture.tag.blockNum);
	UT_ASSERT(f.scan.rs_inited);
	if (sample_next(&f, false) == 1)
		UT_ASSERT_EQ(ItemPointerGetOffsetNumber(&scan_stored_tuple.t_self), 13);
	else
		UT_ASSERT(false);
	UT_ASSERT_EQ(scan_fetches, 1);
	UT_ASSERT_EQ(scan_live_calls + scan_prunes, 0);
	cluster_heap_scan_forget_owned(&f.scan);
	scan_cleanup(&f.scan);
}

UT_TEST(missing_or_wrong_proof_refuses_before_sample_output)
{
	TestOwnerFullFixture saved;
	if (!scan_load(0))
		return;
	saved = scan_fixture;
	for (unsigned fault = 0; fault < 7; fault++) {
		SampleFixture f;
		unsigned before = ut_vis_unknown_count;
		ClusterR4ProjectionBinding binding;
		scan_fixture = saved;
		if (fault == 0)
			scan_fixture.projection.valid = false;
		if (fault == 1)
			scan_fixture.projection.wire.bytes[12] ^= 1;
		if (fault == 2) {
			unsigned n = ClusterR4OperationRead(saved.projection.wire.bytes + 8, 2);
			UT_ASSERT(ClusterR4ProjectionDecode(&saved.projection.wire,
												saved.projection.reply_header, saved.full.data,
												saved.projection.wire.bytes + 140, &binding));
			UT_ASSERT(ClusterR4ProjectionSeal(&scan_fixture.projection.wire, n - 1, &binding,
											  scan_fixture.projection.reply_header,
											  scan_fixture.full.data));
		}
		if (fault == 3)
			((PageHeader)scan_fixture.full.data)->pd_lower = 0;
		sample_setup(&f, fault % 2 == 0);
		if (fault >= 4) {
			scan_fetch_result
				= fault == 4 ? CLUSTER_CR_BUILD_FAIL_CLOSED : CLUSTER_CR_BUILD_RETRYABLE;
			scan_fetch_reason
				= fault == 5 ? CLUSTER_CR_BUILD_NONE : CLUSTER_CR_BUILD_EPOCH_MISMATCH;
		}
		UT_ASSERT_EQ(sample_next(&f, true), -1);
		UT_ASSERT_EQ(ut_error_code, ERRCODE_CLUSTER_TT_STATUS_UNKNOWN);
		UT_ASSERT_EQ(ut_vis_unknown_count, before + 1);
		UT_ASSERT_EQ(scan_stores + scan_live_calls + scan_prunes, 0);
		UT_ASSERT(!f.scan.rs_projection.valid && f.scan.rs_owned_kind == 0);
		scan_cleanup(&f.scan);
	}
	scan_fixture = saved;
}

UT_TEST(retained_full_rejects_changed_snapshot_epoch_or_page)
{
	if (!scan_load(0))
		return;
	for (unsigned fault = 0; fault < 5; fault++) {
		SampleFixture f;
		unsigned before;
		sample_setup(&f, fault % 2 == 0);
		UT_ASSERT_EQ(sample_next(&f, true), 1);
		UT_ASSERT_EQ(sample_next(&f, false), 1);
		before = ut_vis_unknown_count;
		if (fault == 0)
			f.snapshot.read_scn++;
		if (fault == 1)
			f.snapshot.curcid++;
		if (fault == 2)
			ut_current_epoch++;
		if (fault == 3)
			f.scan.rs_owned_page[BLCKSZ - 1] ^= 1;
		if (fault == 4)
			f.scan.rs_projection.valid = false;
		UT_ASSERT_EQ(sample_next(&f, false), -1);
		UT_ASSERT_EQ(ut_error_code, ERRCODE_CLUSTER_TT_STATUS_UNKNOWN);
		UT_ASSERT_EQ(ut_vis_unknown_count, before + 1);
		UT_ASSERT_EQ(scan_stores, 1);
		UT_ASSERT(!f.scan.rs_projection.valid && f.scan.rs_owned_kind == 0);
		scan_cleanup(&f.scan);
	}
}

UT_TEST(empty_full_still_needs_a_complete_page_receipt)
{
	TestOwnerFullFixture saved;
	ClusterR4ProjectionBinding binding;
	if (!scan_load(0))
		return;
	saved = scan_fixture;
	UT_ASSERT(ClusterR4ProjectionDecode(&saved.projection.wire, saved.projection.reply_header,
										saved.full.data, saved.projection.wire.bytes + 140,
										&binding));
	for (unsigned fault = 0; fault < 2; fault++) {
		SampleFixture f;
		Page page = (Page)scan_fixture.full.data;
		scan_fixture = saved;
		for (OffsetNumber off = 1; off <= PageGetMaxOffsetNumber(page); off++)
			ItemIdSetUnused(PageGetItemId(page, off));
		UT_ASSERT(ClusterR4ProjectionSeal(&scan_fixture.projection.wire, 0, &binding,
										  scan_fixture.projection.reply_header,
										  scan_fixture.full.data));
		scan_fixture.projection.valid = fault == 0;
		sample_setup(&f, fault == 0);
		UT_ASSERT_EQ(sample_next(&f, true), fault == 0 ? 1 : -1);
		if (fault == 0)
			UT_ASSERT_EQ(sample_next(&f, false), 0);
		else
			UT_ASSERT_EQ(ut_error_code, ERRCODE_CLUSTER_TT_STATUS_UNKNOWN);
		UT_ASSERT_EQ(scan_stores + scan_live_calls, 0);
		UT_ASSERT(!f.scan.rs_projection.valid && f.scan.rs_owned_kind == 0);
		scan_cleanup(&f.scan);
	}
	scan_fixture = saved;
}

UT_TEST(fetch_error_keeps_cancellation_and_horizon_sqlstates)
{
	if (!scan_load(0))
		return;
	for (unsigned fault = 0; fault < 2; fault++) {
		SampleFixture f;
		sample_setup(&f, fault == 0);
		if (fault == 0)
			scan_fetch_cancel = true;
		else {
			scan_fetch_result = CLUSTER_CR_BUILD_FAIL_CLOSED;
			scan_fetch_reason = CLUSTER_CR_BUILD_SNAPSHOT_TOO_OLD;
		}
		UT_ASSERT_EQ(sample_next(&f, true), -1);
		UT_ASSERT_EQ(ut_error_code,
					 fault == 0 ? ERRCODE_QUERY_CANCELED : ERRCODE_CLUSTER_CR_SNAPSHOT_TOO_OLD);
		UT_ASSERT_EQ(scan_fetches, 1);
		UT_ASSERT_EQ(scan_stores + scan_live_calls + scan_prunes, 0);
		UT_ASSERT(!f.scan.rs_projection.valid && f.scan.rs_owned_kind == 0);
		scan_cleanup(&f.scan);
	}
}

UT_TEST(sampler_error_or_binding_drift_retires_private_full)
{
	if (!scan_load(0))
		return;
	for (unsigned fault = 1; fault <= 3; fault++) {
		SampleFixture f;
		PGAlignedBlock zero = { 0 };
		sample_setup(&f, fault % 2 == 0);
		UT_ASSERT_EQ(sample_next(&f, true), 1);
		f.tuple_fault = fault;
		UT_ASSERT_EQ(sample_next(&f, false), -1);
		UT_ASSERT_EQ(ut_error_code,
					 fault == 1 ? ERRCODE_QUERY_CANCELED : ERRCODE_CLUSTER_TT_STATUS_UNKNOWN);
		UT_ASSERT_EQ(scan_stores + scan_live_calls + scan_prunes, 0);
		UT_ASSERT(!scan_locked && !f.scan.rs_projection.valid && f.scan.rs_owned_kind == 0);
		UT_ASSERT_EQ(memcmp(f.scan.rs_owned_page, zero.data, BLCKSZ), 0);
		scan_cleanup(&f.scan);
	}
}

UT_TEST(next_block_eof_and_block_error_retire_prior_full)
{
	if (!scan_load(0))
		return;
	for (unsigned fault = 0; fault < 3; fault++) {
		SampleFixture f;
		PGAlignedBlock zero = { 0 };
		sample_setup(&f, fault % 2 == 0);
		UT_ASSERT_EQ(sample_next(&f, true), 1);
		UT_ASSERT_EQ(sample_next(&f, false), 1);
		if (fault == 0) {
			f.remaining_blocks = 1;
			UT_ASSERT_EQ(sample_next(&f, true), 1);
			UT_ASSERT_EQ(scan_fetches, 2);
			if (sample_next(&f, false) == 1)
				UT_ASSERT_EQ(ItemPointerGetOffsetNumber(&scan_stored_tuple.t_self), 13);
			else
				UT_ASSERT(false);
			cluster_heap_scan_forget_owned(&f.scan);
		} else {
			f.cancel_block = fault == 2;
			UT_ASSERT_EQ(sample_next(&f, true), fault == 1 ? 0 : -1);
			if (fault == 2)
				UT_ASSERT_EQ(ut_error_code, ERRCODE_QUERY_CANCELED);
		}
		UT_ASSERT(!f.scan.rs_projection.valid && f.scan.rs_owned_kind == 0);
		UT_ASSERT_EQ(memcmp(f.scan.rs_owned_page, zero.data, BLCKSZ), 0);
		scan_cleanup(&f.scan);
	}
}

UT_TEST(native_local_non_mvcc_and_dormant_sampling_keep_original_path)
{
	if (!scan_load(0))
		return;
	for (unsigned mode = 0; mode < 2; mode++)
		for (unsigned control = 0; control < 5; control++) {
			SampleFixture f;
			unsigned count = 0;
			sample_setup(&f, mode == 0);
			if (control == 0) {
				scan_fetch_result = CLUSTER_CR_BUILD_RETRYABLE;
				scan_fetch_reason = CLUSTER_CR_BUILD_TARGET_DISABLED;
			}
			if (control == 1)
				f.snapshot.cluster_source = SNAPSHOT_SOURCE_LOCAL;
			if (control == 2)
				f.form.relpersistence = RELPERSISTENCE_TEMP;
			if (control == 3)
				f.snapshot.snapshot_type = SNAPSHOT_ANY;
			if (control == 4)
				cluster_shared_config = cluster_shared_catalog = cluster_enabled = false;
			scan_live_visible = true;
			UT_ASSERT_EQ(sample_next(&f, true), 1);
			while (sample_next(&f, false) == 1)
				count++;
			UT_ASSERT_EQ(count, 6);
			UT_ASSERT_EQ(scan_fetches, control == 0 ? 1 : 0);
			UT_ASSERT_EQ(scan_live_calls, mode == 0 ? 13 : 6);
			UT_ASSERT_EQ(scan_buffer_stores, control >= 2 && control != 3 ? 6 : 0);
			UT_ASSERT(!scan_locked && !f.scan.rs_projection.valid);
			UT_ASSERT_EQ(sample_next(&f, true), 0);
			scan_cleanup(&f.scan);
		}
}

UT_TEST(serializable_full_refuses_and_dormant_checks_stay_locked)
{
	if (!scan_load(0))
		return;
	for (unsigned mode = 0; mode < 2; mode++) {
		SampleFixture f;
		sample_setup(&f, mode == 0);
		XactIsoLevel = XACT_SERIALIZABLE;
		UT_ASSERT_EQ(sample_next(&f, true), -1);
		UT_ASSERT_EQ(ut_error_code, ERRCODE_CLUSTER_TT_STATUS_UNKNOWN);
		UT_ASSERT_EQ(scan_stores + scan_live_calls + scan_conflicts, 0);
		UT_ASSERT(!f.scan.rs_projection.valid && f.scan.rs_owned_kind == 0);
		scan_cleanup(&f.scan);
		sample_setup(&f, mode == 0);
		XactIsoLevel = XACT_SERIALIZABLE;
		scan_fetch_result = CLUSTER_CR_BUILD_RETRYABLE;
		scan_fetch_reason = CLUSTER_CR_BUILD_TARGET_DISABLED;
		scan_live_visible = true;
		UT_ASSERT_EQ(sample_next(&f, true), 1);
		UT_ASSERT_EQ(sample_next(&f, false), 1);
		UT_ASSERT_EQ(scan_conflicts, mode == 0 ? 13 : 1);
		UT_ASSERT_EQ(sample_next(&f, true), 0);
		scan_cleanup(&f.scan);
	}
}

UT_TEST(rescan_and_endscan_retire_full_owner)
{
	SampleFixture f;
	HeapScanDesc end;
	if (!scan_load(0))
		return;
	sample_setup(&f, true);
	UT_ASSERT_EQ(sample_next(&f, true), 1);
	UT_ASSERT_EQ(sample_next(&f, false), 1);
	heap_rescan((TableScanDesc)&f.scan, NULL, false, false, false, false);
	UT_ASSERT(!f.scan.rs_projection.valid && f.scan.rs_owned_kind == 0);
	UT_ASSERT_EQ(scan_pins, 0);
	/* The same descriptor can be used by the sampler after a rescan. */
	f.remaining_blocks = 1;
	UT_ASSERT_EQ(sample_next(&f, true), 1);
	UT_ASSERT_EQ(sample_next(&f, false), 1);
	end = malloc(sizeof(*end));
	*end = f.scan;
	heap_endscan((TableScanDesc)end);
	scan_finish();
}

int
main(void)
{
	UT_PLAN(12);
	UT_RUN(page_sample_uses_full_rows_in_sampler_order);
	UT_RUN(row_sample_uses_full_rows_in_sampler_order);
	UT_RUN(sequential_sampling_uses_the_same_full_page);
	UT_RUN(missing_or_wrong_proof_refuses_before_sample_output);
	UT_RUN(retained_full_rejects_changed_snapshot_epoch_or_page);
	UT_RUN(empty_full_still_needs_a_complete_page_receipt);
	UT_RUN(fetch_error_keeps_cancellation_and_horizon_sqlstates);
	UT_RUN(sampler_error_or_binding_drift_retires_private_full);
	UT_RUN(next_block_eof_and_block_error_retire_prior_full);
	UT_RUN(native_local_non_mvcc_and_dormant_sampling_keep_original_path);
	UT_RUN(serializable_full_refuses_and_dormant_checks_stay_locked);
	UT_RUN(rescan_and_endscan_retire_full_owner);
	UT_DONE();
	return ut_failed_count != 0;
}
