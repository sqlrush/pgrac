/*-------------------------------------------------------------------------
 *
 * test_cluster_heap_scan_full.c
 *    Original sequential scan consumers of private FULL projections.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *    src/test/cluster_unit/test_cluster_heap_scan_full.c
 *
 * NOTES
 *    Production scan bodies and scratch visibility are executed. Storage,
 *    transport endpoint and native live visibility are controlled leaves.
 *    FULL bytes come from the original holder, server and registered landing.
 *
 *-------------------------------------------------------------------------
 */
#define CLUSTER_SCRATCH_SELF_ENTRY scan_unused_self_main
int scan_unused_self_main(void);
#include "test_cluster_scratch_self.c"
#include "access/syncscan.h"
#include "access/valid.h"
#include "access/visibilitymap.h"
#include "cluster/cluster_heap_horizon.h"
#include "utils/rel.h"
#include "test_cluster_heap_maintenance_policy.inc"

bool synchronize_seqscans;
volatile sig_atomic_t InterruptPending;
int XactIsoLevel = XACT_READ_COMMITTED;
static TestOwnerFullFixture scan_fixture;
static PGAlignedBlock scan_stored;
static HeapTupleData scan_stored_tuple;
static unsigned scan_fetches, scan_live_calls, scan_stores, scan_prunes, scan_buffer_stores;
static unsigned scan_ends;
static unsigned scan_conflicts;
static int scan_pins;
static bool scan_locked, scan_live_visible, scan_fetch_cancel;
static ClusterCrBuildResult scan_fetch_result;
static ClusterCrBuildReason scan_fetch_reason;

void
ProcessInterrupts(void)
{
	abort();
}

Buffer
ReadBufferExtended(Relation relation, ForkNumber fork, BlockNumber block, ReadBufferMode mode,
				   BufferAccessStrategy strategy)
{
	UT_ASSERT_EQ(fork, MAIN_FORKNUM);
	UT_ASSERT_EQ(block, scan_fixture.tag.blockNum);
	UT_ASSERT_EQ(scan_pins, 0);
	scan_pins++;
	return 1;
}

void
ReleaseBuffer(Buffer buffer)
{
	UT_ASSERT_EQ(buffer, 1);
	UT_ASSERT(!scan_locked && scan_pins == 1);
	scan_pins--;
}

void
LockBuffer(Buffer buffer, int mode)
{
	UT_ASSERT_EQ(buffer, 1);
	UT_ASSERT_EQ(scan_pins, 1);
	UT_ASSERT(mode == BUFFER_LOCK_SHARE ? !scan_locked : scan_locked);
	scan_locked = mode == BUFFER_LOCK_SHARE;
}

void
heap_page_prune_opt(Relation relation, Buffer buffer)
{
	UT_ASSERT(!scan_locked);
	scan_prunes++;
}

ClusterCrBuildResult
cluster_gcs_block_cr_fetch_projection(BufferTag tag, SCN scn, char page[BLCKSZ],
									  ClusterR4ProjectionScratch *proof,
									  ClusterCrBuildReason *reason)
{
	UT_ASSERT(!scan_locked && scan_pins == 1);
	UT_ASSERT(BufferTagsEqual(&tag, &scan_fixture.tag));
	UT_ASSERT_EQ(scn, scan_fixture.read_scn);
	scan_fetches++;
	memcpy(page, scan_fixture.full.data, BLCKSZ);
	*proof = scan_fixture.projection;
	*reason = scan_fetch_reason;
	if (scan_fetch_cancel)
		ereport(ERROR,
				(errcode(ERRCODE_QUERY_CANCELED), errmsg("scan fetch cancellation fixture")));
	return scan_fetch_result;
}

static bool
scan_native_visibility(HeapTuple tuple, Snapshot snapshot, Buffer buffer)
{
	UT_ASSERT(scan_locked);
	scan_live_calls++;
	return scan_live_visible;
}

static void
scan_conflict(bool visible, Relation relation, HeapTuple tuple, Buffer buffer, Snapshot snapshot)
{
	scan_conflicts++;
	if (IsolationIsSerializable())
		UT_ASSERT(scan_locked);
}

void
ExecForceStoreHeapTuple(HeapTuple tuple, TupleTableSlot *slot, bool free_tuple)
{
	UT_ASSERT(tuple->t_len <= BLCKSZ && !free_tuple);
	scan_stored_tuple = *tuple;
	memcpy(scan_stored.data, tuple->t_data, tuple->t_len);
	scan_stored_tuple.t_data = (HeapTupleHeader)scan_stored.data;
	scan_stores++;
}

TupleTableSlot *
ExecStoreBufferHeapTuple(HeapTuple tuple, TupleTableSlot *slot, Buffer buffer)
{
	UT_ASSERT(buffer == 1 && scan_pins == 1);
	scan_buffer_stores++;
	ExecForceStoreHeapTuple(tuple, slot, false);
	return slot;
}

void
RelationDecrementReferenceCount(Relation relation)
{
	scan_ends++;
}

void
UnregisterSnapshot(Snapshot snapshot)
{
	abort();
}

void
pfree(void *pointer)
{
	HeapScanDesc scan = pointer;
	PGAlignedBlock zero = { 0 };
	UT_ASSERT(!scan->rs_projection.valid && scan->rs_owned_kind == 0);
	UT_ASSERT_EQ(memcmp(scan->rs_owned_page, zero.data, BLCKSZ), 0);
	free(pointer);
}

static void
scan_clear_slot(TupleTableSlot *slot)
{}

static void
scan_old_snapshot(Snapshot snapshot, Relation relation, Page page)
{}

BlockNumber
RelationGetNumberOfBlocksInFork(Relation relation, ForkNumber fork)
{
	return scan_fixture.tag.blockNum + 1;
}

BufferAccessStrategy
GetAccessStrategy(BufferAccessStrategyType type)
{
	abort();
}
void
FreeAccessStrategy(BufferAccessStrategy strategy)
{
	abort();
}
BlockNumber
ss_get_location(Relation relation, BlockNumber blocks)
{
	abort();
}
void
pgstat_assoc_relation(Relation relation)
{}

static BlockNumber
heapgettup_initial_block(HeapScanDesc scan, ScanDirection direction)
{
	return scan_fixture.tag.blockNum;
}
static BlockNumber
heapgettup_advance_block(HeapScanDesc scan, BlockNumber block, ScanDirection direction)
{
	return InvalidBlockNumber;
}

int32
ItemPointerCompare(ItemPointer left, ItemPointer right)
{
	if (ItemPointerGetBlockNumber(left) != ItemPointerGetBlockNumber(right))
		return ItemPointerGetBlockNumber(left) < ItemPointerGetBlockNumber(right) ? -1 : 1;
	return (int32)ItemPointerGetOffsetNumber(left) - ItemPointerGetOffsetNumber(right);
}

Datum
FunctionCall2Coll(FmgrInfo *info, Oid collation, Datum left, Datum right)
{
	abort();
}
Datum
nocachegetattr(HeapTuple tuple, int attnum, TupleDesc desc)
{
	abort();
}
Datum
heap_getsysattr(HeapTuple tuple, int attnum, TupleDesc desc, bool *isnull)
{
	abort();
}
Datum
getmissingattr(TupleDesc desc, int attnum, bool *isnull)
{
	abort();
}

/* The original live route must not answer for a retained FULL page. */
#define HeapTupleSatisfiesVisibility scan_native_visibility
#define HeapCheckForSerializableConflictOut scan_conflict
#define TestForOldSnapshot scan_old_snapshot
#undef ExecClearTuple
#define ExecClearTuple scan_clear_slot
#include "test_cluster_heap_scan_r4.inc"
#include "test_cluster_heap_scan_lifetime.inc"
#undef HeapTupleSatisfiesVisibility
#undef HeapCheckForSerializableConflictOut
#undef TestForOldSnapshot
#undef ExecClearTuple

static bool
scan_load(unsigned kind)
{
	char command[512];
	FILE *producer;
	bool received;
	int rc;

	snprintf(command, sizeof(command),
			 "./test_cluster_r4_tx_outcome --peer-full-proof-fixture %u | "
			 "./test_cluster_r4_slot_reservation --peer-full-frame-fixture | "
			 "./test_cluster_r4_route_policy --peer-full-land-fixture",
			 kind);
	producer = popen(command, "r");
	received = producer != NULL && fread(&scan_fixture, sizeof(scan_fixture), 1, producer) == 1;
	rc = producer != NULL ? pclose(producer) : -1;
	UT_ASSERT(received && rc == 0);
	return received && rc == 0;
}

static void
scan_setup(HeapScanDesc scan, Relation relation, Form_pg_class form, Snapshot snapshot,
		   bool pagemode, bool tidrange)
{
	self_child = true;
	self_reset_case(false, 0);
	self_current = false;
	self_owner_full = scan_fixture;
	self_owner_full_pipe = self_peer_full_pipe = true;
	cluster_node_id = 2;
	cluster_shared_config = cluster_shared_catalog = cluster_enabled = true;
	memset(scan, 0, sizeof(*scan));
	memset(relation, 0, sizeof(*relation));
	memset(form, 0, sizeof(*form));
	memset(snapshot, 0, sizeof(*snapshot));
	relation->rd_rel = form;
	relation->rd_id = FirstNormalObjectId;
	relation->rd_locator = (RelFileLocator){ scan_fixture.tag.spcOid, scan_fixture.tag.dbOid,
											 scan_fixture.tag.relNumber };
	form->relkind = RELKIND_RELATION;
	form->relpersistence = RELPERSISTENCE_PERMANENT;
	snapshot->snapshot_type = SNAPSHOT_MVCC;
	snapshot->cluster_source = SNAPSHOT_SOURCE_CLUSTER;
	snapshot->read_scn = scan_fixture.read_scn;
	snapshot->read_epoch = 9;
	ut_current_epoch = snapshot->read_epoch;
	snapshot->curcid = 8;
	scan->rs_base.rs_rd = relation;
	scan->rs_base.rs_snapshot = snapshot;
	scan->rs_base.rs_flags
		= (tidrange ? SO_TYPE_TIDRANGESCAN : SO_TYPE_SEQSCAN) | (pagemode ? SO_ALLOW_PAGEMODE : 0);
	scan->rs_nblocks = scan_fixture.tag.blockNum + 1;
	scan->rs_numblocks = InvalidBlockNumber;
	scan->rs_cbuf = InvalidBuffer;
	scan->rs_ctup.t_tableOid = relation->rd_id;
	ItemPointerSet(&scan->rs_base.rs_mintid, scan_fixture.tag.blockNum, 1);
	ItemPointerSet(&scan->rs_base.rs_maxtid, scan_fixture.tag.blockNum, 25);
	memcpy(ut_visibility_page.data, scan_fixture.full.data, BLCKSZ);
	scan_fetches = scan_live_calls = scan_stores = scan_prunes = scan_buffer_stores = 0;
	scan_conflicts = 0;
	XactIsoLevel = XACT_READ_COMMITTED;
	scan_pins = 0;
	scan_locked = scan_live_visible = scan_fetch_cancel = false;
	scan_fetch_result = CLUSTER_CR_BUILD_FULL;
	scan_fetch_reason = CLUSTER_CR_BUILD_NONE;
}

static void
scan_finish(void)
{
	UT_ASSERT(!scan_locked && scan_pins == 0);
	UT_ASSERT(ut_snapshot_scope == NULL);
	self_owner_full_pipe = self_peer_full_pipe = self_child = false;
	cluster_node_id = UT_SELF_NODE;
}

static void
scan_check_rows(unsigned kind, bool pagemode, bool tidrange, ScanDirection direction)
{
	static const OffsetNumber inserts[] = { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13 };
	static const OffsetNumber updated[] = { 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13 };
	/* holder_mixed_modifier replaces the offset-12 INSERT with DELETE. */
	static const OffsetNumber deleted[] = { 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 13 };
	const OffsetNumber *expected = kind == 0 ? inserts : kind & 1 ? updated : deleted;
	unsigned expected_count = kind == 0	 ? lengthof(inserts)
							  : kind & 1 ? lengthof(updated)
										 : lengthof(deleted);
	HeapScanDescData scan;
	RelationData relation;
	FormData_pg_class form;
	SnapshotData snapshot;
	TupleTableSlot slot = { 0 };
	unsigned count = 0;
	volatile bool caught = false;

	if (!scan_load(kind))
		return;
	UT_ASSERT_EQ(ItemIdIsNormal(PageGetItemId((Page)scan_fixture.full.data, 12)),
				 kind == 0 || (kind & 1) != 0);
	scan_setup(&scan, &relation, &form, &snapshot, pagemode, tidrange);
	ut_error_armed = true;
	PG_TRY();
	{
		while (tidrange ? heap_getnextslot_tidrange((TableScanDesc)&scan, direction, &slot)
						: heap_getnextslot((TableScanDesc)&scan, direction, &slot)) {
			UT_ASSERT(count < expected_count);
			if (count < expected_count)
				UT_ASSERT_EQ(
					ItemPointerGetOffsetNumber(&scan_stored_tuple.t_self),
					expected[direction == ForwardScanDirection ? count
															   : expected_count - 1 - count]);
			UT_ASSERT_EQ(scan_stores, ++count);
			memset(ut_visibility_page.data, 0xa5, BLCKSZ);
		}
	}
	PG_CATCH();
	{
		caught = true;
	}
	PG_END_TRY();
	ut_error_armed = false;
	if (caught)
		printf("# scan refused kind=%u mode=%d tidrange=%d: %s\n", kind, pagemode, tidrange,
			   ut_error_message);
	UT_ASSERT(!caught);
	UT_ASSERT_EQ(count, expected_count);
	UT_ASSERT_EQ(scan_fetches, 1);
	UT_ASSERT_EQ(scan_live_calls, 0);
	UT_ASSERT_EQ(scan_prunes, 0);
	UT_ASSERT(!scan.rs_projection.valid && scan.rs_owned_kind == 0);
	if (BufferIsValid(scan.rs_cbuf))
		ReleaseBuffer(scan.rs_cbuf);
	scan_finish();
}

UT_TEST(page_mode_full_scan_preserves_private_rows_both_directions)
{
	scan_check_rows(0, true, false, ForwardScanDirection);
	scan_check_rows(0, true, false, BackwardScanDirection);
}
UT_TEST(row_mode_full_scan_preserves_private_rows_both_directions)
{
	scan_check_rows(0, false, false, ForwardScanDirection);
	scan_check_rows(0, false, false, BackwardScanDirection);
}
UT_TEST(tid_range_full_scan_preserves_private_rows_both_directions)
{
	scan_check_rows(0, true, true, ForwardScanDirection);
	scan_check_rows(0, true, true, BackwardScanDirection);
}
UT_TEST(v1_and_child_modifiers_use_both_roles_in_scan)
{
	for (unsigned kind = 1; kind <= 4; kind++) {
		scan_check_rows(kind, true, false, ForwardScanDirection);
		scan_check_rows(kind, false, false, BackwardScanDirection);
	}
}

/* The catch lives outside the scan owner's frame, as it does in the executor. */
static int
scan_next(HeapScanDesc scan, bool tidrange, ScanDirection direction)
{
	TupleTableSlot slot = { 0 };
	volatile int result = -1;
	ut_error_code = 0;
	ut_error_armed = true;
	PG_TRY();
	{
		result = tidrange ? heap_getnextslot_tidrange((TableScanDesc)scan, direction, &slot)
						  : heap_getnextslot((TableScanDesc)scan, direction, &slot);
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
scan_cleanup(HeapScanDesc scan)
{
	UT_ASSERT(!scan_locked);
	if (BufferIsValid(scan->rs_cbuf))
		ReleaseBuffer(scan->rs_cbuf);
	scan_finish();
}

UT_TEST(incomplete_or_wrong_full_proof_refuses_whole_page)
{
	TestOwnerFullFixture saved;
	if (!scan_load(0))
		return;
	saved = scan_fixture;
	for (unsigned fault = 0; fault < 9; fault++) {
		HeapScanDescData scan;
		RelationData relation;
		FormData_pg_class form;
		SnapshotData snapshot;
		PGAlignedBlock zero = { 0 };
		ClusterR4ProjectionBinding binding;
		unsigned count, before = ut_vis_unknown_count;
		scan_fixture = saved;
		UT_ASSERT(ClusterR4ProjectionDecode(&saved.projection.wire, saved.projection.reply_header,
											saved.full.data, saved.projection.wire.bytes + 140,
											&binding));
		count = ClusterR4OperationRead(saved.projection.wire.bytes + 8, 2);
		if (fault == 0)
			scan_fixture.projection.valid = false;
		if (fault == 1)
			scan_fixture.projection.wire.bytes[12] ^= 1;
		if (fault == 2)
			binding.tag.relNumber++;
		if (fault == 3)
			binding.requester++;
		if (fault == 4)
			count--; /* Last receipt is absent, including when the first row is intact. */
		if (fault >= 2 && fault <= 4)
			UT_ASSERT(ClusterR4ProjectionSeal(&scan_fixture.projection.wire, count, &binding,
											  scan_fixture.projection.reply_header,
											  scan_fixture.full.data));
		if (fault == 5)
			((PageHeader)scan_fixture.full.data)->pd_lower = 0;
		scan_setup(&scan, &relation, &form, &snapshot, fault % 2 == 0, false);
		if (fault >= 6) {
			scan_fetch_result
				= fault == 6 ? CLUSTER_CR_BUILD_FAIL_CLOSED : CLUSTER_CR_BUILD_RETRYABLE;
			scan_fetch_reason
				= fault == 7 ? CLUSTER_CR_BUILD_NONE : CLUSTER_CR_BUILD_EPOCH_MISMATCH;
		}
		UT_ASSERT_EQ(scan_next(&scan, false, ForwardScanDirection), -1);
		UT_ASSERT_EQ(ut_error_code, ERRCODE_CLUSTER_TT_STATUS_UNKNOWN);
		UT_ASSERT_EQ(ut_vis_unknown_count, before + 1);
		UT_ASSERT_EQ(scan_stores, 0);
		UT_ASSERT_EQ(scan_fetches, 1);
		UT_ASSERT_EQ(scan_live_calls + scan_prunes, 0);
		UT_ASSERT(!scan.rs_projection.valid && scan.rs_owned_kind == 0);
		UT_ASSERT_EQ(memcmp(scan.rs_owned_page, zero.data, BLCKSZ), 0);
		scan_cleanup(&scan);
	}
	scan_fixture = saved;
}

UT_TEST(snapshot_too_old_keeps_its_distinct_error)
{
	HeapScanDescData scan;
	RelationData relation;
	FormData_pg_class form;
	SnapshotData snapshot;
	if (!scan_load(0))
		return;
	scan_setup(&scan, &relation, &form, &snapshot, true, false);
	scan_fetch_result = CLUSTER_CR_BUILD_FAIL_CLOSED;
	scan_fetch_reason = CLUSTER_CR_BUILD_SNAPSHOT_TOO_OLD;
	UT_ASSERT_EQ(scan_next(&scan, false, ForwardScanDirection), -1);
	UT_ASSERT_EQ(ut_error_code, ERRCODE_CLUSTER_CR_SNAPSHOT_TOO_OLD);
	UT_ASSERT_EQ(scan_stores + scan_live_calls, 0);
	UT_ASSERT(!scan.rs_projection.valid && scan.rs_owned_kind == 0);
	scan_cleanup(&scan);
}

UT_TEST(retained_full_rejects_snapshot_epoch_or_page_change)
{
	if (!scan_load(0))
		return;
	for (unsigned fault = 0; fault < 9; fault++) {
		HeapScanDescData scan;
		RelationData relation;
		FormData_pg_class form;
		SnapshotData snapshot, replacement;
		unsigned before;
		scan_setup(&scan, &relation, &form, &snapshot, fault % 2 == 0, false);
		if (scan_next(&scan, false, ForwardScanDirection) != 1) {
			UT_ASSERT(false);
			scan_cleanup(&scan);
			continue;
		}
		before = ut_vis_unknown_count;
		if (fault == 0)
			snapshot.read_scn++;
		if (fault == 1)
			snapshot.read_epoch++;
		if (fault == 2)
			ut_current_epoch++;
		if (fault == 3)
			snapshot.curcid++;
		if (fault == 4)
			snapshot.snapshot_type = SNAPSHOT_ANY;
		if (fault == 5)
			snapshot.cluster_source = SNAPSHOT_SOURCE_LOCAL;
		if (fault == 6) {
			replacement = snapshot;
			scan.rs_base.rs_snapshot = &replacement;
		}
		if (fault == 7)
			scan.rs_owned_page[1024] ^= 1;
		if (fault == 8)
			scan.rs_projection.valid = false;
		UT_ASSERT_EQ(scan_next(&scan, false, ForwardScanDirection), -1);
		UT_ASSERT_EQ(ut_error_code, ERRCODE_CLUSTER_TT_STATUS_UNKNOWN);
		UT_ASSERT_EQ(ut_vis_unknown_count, before + 1);
		UT_ASSERT_EQ(scan_fetches, 1);
		UT_ASSERT_EQ(scan_stores, 1);
		UT_ASSERT(!scan.rs_projection.valid && scan.rs_owned_kind == 0);
		scan_cleanup(&scan);
	}
}

UT_TEST(rescan_and_new_page_retire_full_with_its_bytes)
{
	if (!scan_load(0))
		return;
	for (unsigned pagemode = 0; pagemode < 2; pagemode++) {
		HeapScanDescData scan;
		RelationData relation;
		FormData_pg_class form;
		SnapshotData snapshot;
		PGAlignedBlock stored, zero = { 0 };
		scan_setup(&scan, &relation, &form, &snapshot, pagemode, false);
		if (scan_next(&scan, false, ForwardScanDirection) != 1) {
			UT_ASSERT(false);
			scan_cleanup(&scan);
			continue;
		}
		stored = scan_stored;
		heap_rescan((TableScanDesc)&scan, NULL, false, false, false, false);
		UT_ASSERT(!scan.rs_projection.valid && scan.rs_owned_kind == 0 && scan_pins == 0);
		UT_ASSERT_EQ(memcmp(scan.rs_owned_page, zero.data, BLCKSZ), 0);
		UT_ASSERT_EQ(memcmp(scan_stored.data, stored.data, BLCKSZ), 0);
		UT_ASSERT_EQ(scan_next(&scan, false, ForwardScanDirection), 1);
		UT_ASSERT_EQ(scan_fetches, 2);
		/* A new observation must replace the previous page/proof together. */
		scan.rs_projection.valid = false;
		heapgetpage((TableScanDesc)&scan, scan_fixture.tag.blockNum);
		UT_ASSERT(scan.rs_projection.valid && scan.rs_owned_kind == 3);
		UT_ASSERT_EQ(scan_fetches, 3);
		scan_cleanup(&scan);
	}
}

UT_TEST(endscan_forgets_full_before_freeing_owner)
{
	HeapScanDesc scan = malloc(sizeof(*scan));
	RelationData relation;
	FormData_pg_class form;
	SnapshotData snapshot;
	unsigned before = scan_ends;
	if (!scan_load(0)) {
		free(scan);
		return;
	}
	scan_setup(scan, &relation, &form, &snapshot, true, false);
	if (scan_next(scan, false, ForwardScanDirection) == 1)
		heap_endscan((TableScanDesc)scan);
	else {
		UT_ASSERT(false);
		scan_cleanup(scan);
		free(scan);
		return;
	}
	UT_ASSERT_EQ(scan_ends, before + 1);
	scan_finish();
}

UT_TEST(tid_range_bounds_retire_page_and_allow_reverse_restart)
{
	if (!scan_load(0))
		return;
	for (unsigned pagemode = 0; pagemode < 2; pagemode++) {
		HeapScanDescData scan;
		RelationData relation;
		FormData_pg_class form;
		SnapshotData snapshot;
		scan_setup(&scan, &relation, &form, &snapshot, pagemode, true);
		ItemPointerSet(&scan.rs_base.rs_mintid, scan_fixture.tag.blockNum, 4);
		ItemPointerSet(&scan.rs_base.rs_maxtid, scan_fixture.tag.blockNum, 6);
		for (unsigned reverse = 0; reverse < 2; reverse++) {
			for (unsigned row = 0; row < 3; row++) {
				int result = scan_next(&scan, true,
									   reverse ? BackwardScanDirection : ForwardScanDirection);
				UT_ASSERT_EQ(result, 1);
				if (result == 1)
					UT_ASSERT_EQ(ItemPointerGetOffsetNumber(&scan_stored_tuple.t_self),
								 reverse ? 6 - row : 4 + row);
			}
			UT_ASSERT_EQ(
				scan_next(&scan, true, reverse ? BackwardScanDirection : ForwardScanDirection), 0);
			UT_ASSERT(!scan.rs_projection.valid && scan.rs_owned_kind == 0 && scan_pins == 0);
		}
		scan_cleanup(&scan);
	}
}

UT_TEST(dormant_native_local_and_non_mvcc_keep_original_path)
{
	if (!scan_load(0))
		return;
	for (unsigned control = 0; control < 5; control++) {
		HeapScanDescData scan;
		RelationData relation;
		FormData_pg_class form;
		SnapshotData snapshot;
		unsigned rows = 0;
		scan_setup(&scan, &relation, &form, &snapshot, control % 2 == 0, false);
		if (control == 0) {
			scan_fetch_result = CLUSTER_CR_BUILD_RETRYABLE;
			scan_fetch_reason = CLUSTER_CR_BUILD_TARGET_DISABLED;
		}
		if (control == 1)
			snapshot.cluster_source = SNAPSHOT_SOURCE_LOCAL;
		if (control == 2)
			form.relpersistence = RELPERSISTENCE_TEMP;
		if (control == 3)
			snapshot.snapshot_type = SNAPSHOT_ANY;
		if (control == 4)
			cluster_shared_config = cluster_shared_catalog = cluster_enabled = false;
		scan_live_visible = true;
		while (scan_next(&scan, false, ForwardScanDirection) == 1)
			rows++;
		UT_ASSERT_EQ(rows, 13);
		UT_ASSERT_EQ(scan_fetches, control == 0 ? 1 : 0);
		UT_ASSERT_EQ(scan_live_calls, 13);
		UT_ASSERT_EQ(scan_buffer_stores, control >= 2 && control != 3 ? 13 : 0);
		UT_ASSERT(!scan.rs_projection.valid);
		scan_cleanup(&scan);
	}
}

UT_TEST(empty_full_still_requires_the_whole_page_receipt)
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
		HeapScanDescData scan;
		RelationData relation;
		FormData_pg_class form;
		SnapshotData snapshot;
		Page page = (Page)scan_fixture.full.data;
		scan_fixture = saved;
		for (OffsetNumber off = 1; off <= PageGetMaxOffsetNumber(page); off++)
			ItemIdSetUnused(PageGetItemId(page, off));
		UT_ASSERT(ClusterR4ProjectionSeal(&scan_fixture.projection.wire, 0, &binding,
										  scan_fixture.projection.reply_header,
										  scan_fixture.full.data));
		scan_fixture.projection.valid = fault == 0;
		scan_setup(&scan, &relation, &form, &snapshot, fault == 0, false);
		UT_ASSERT_EQ(scan_next(&scan, false, ForwardScanDirection), fault == 0 ? 0 : -1);
		if (fault != 0)
			UT_ASSERT_EQ(ut_error_code, ERRCODE_CLUSTER_TT_STATUS_UNKNOWN);
		UT_ASSERT_EQ(scan_stores + scan_live_calls, 0);
		UT_ASSERT(!scan.rs_projection.valid && scan.rs_owned_kind == 0);
		scan_cleanup(&scan);
	}
	scan_fixture = saved;
}

UT_TEST(fetch_cancellation_retires_page_proof_and_snapshot_scope)
{
	HeapScanDescData scan;
	RelationData relation;
	FormData_pg_class form;
	SnapshotData snapshot;
	PGAlignedBlock zero = { 0 };
	if (!scan_load(0))
		return;
	scan_setup(&scan, &relation, &form, &snapshot, true, false);
	scan_fetch_cancel = true;
	UT_ASSERT_EQ(scan_next(&scan, false, ForwardScanDirection), -1);
	UT_ASSERT_EQ(ut_error_code, ERRCODE_QUERY_CANCELED);
	UT_ASSERT_EQ(scan_fetches, 1);
	UT_ASSERT_EQ(scan_stores + scan_live_calls + scan_prunes, 0);
	UT_ASSERT(!scan.rs_projection.valid && scan.rs_owned_kind == 0);
	UT_ASSERT_EQ(memcmp(scan.rs_owned_page, zero.data, BLCKSZ), 0);
	scan_cleanup(&scan);
}

UT_TEST(serializable_full_refuses_before_output_or_unlocked_conflict)
{
	if (!scan_load(0))
		return;
	for (unsigned mode = 0; mode < 4; mode++) {
		HeapScanDescData scan;
		RelationData relation;
		FormData_pg_class form;
		SnapshotData snapshot;
		unsigned before = ut_vis_unknown_count;
		scan_setup(&scan, &relation, &form, &snapshot, mode & 1, mode & 2);
		XactIsoLevel = XACT_SERIALIZABLE;
		UT_ASSERT_EQ(scan_next(&scan, mode & 2, ForwardScanDirection), -1);
		UT_ASSERT_EQ(ut_error_code, ERRCODE_CLUSTER_TT_STATUS_UNKNOWN);
		UT_ASSERT_EQ(ut_vis_unknown_count, before + 1);
		UT_ASSERT_EQ(scan_fetches, 1);
		UT_ASSERT_EQ(scan_stores + scan_live_calls + scan_prunes + scan_conflicts, 0);
		UT_ASSERT(!scan.rs_projection.valid && scan.rs_owned_kind == 0);
		scan_cleanup(&scan);
	}
}

UT_TEST(serializable_dormant_scan_keeps_locked_native_conflicts)
{
	if (!scan_load(0))
		return;
	for (unsigned mode = 0; mode < 4; mode++) {
		HeapScanDescData scan;
		RelationData relation;
		FormData_pg_class form;
		SnapshotData snapshot;
		unsigned rows = 0;
		scan_setup(&scan, &relation, &form, &snapshot, mode & 1, mode & 2);
		XactIsoLevel = XACT_SERIALIZABLE;
		scan_fetch_result = CLUSTER_CR_BUILD_RETRYABLE;
		scan_fetch_reason = CLUSTER_CR_BUILD_TARGET_DISABLED;
		scan_live_visible = true;
		while (scan_next(&scan, mode & 2, ForwardScanDirection) == 1)
			rows++;
		UT_ASSERT_EQ(rows, 13);
		UT_ASSERT_EQ(scan_fetches, 1);
		UT_ASSERT_EQ(scan_conflicts, 13);
		UT_ASSERT_EQ(scan_live_calls, 13);
		UT_ASSERT(!scan.rs_projection.valid && scan.rs_owned_kind == 0);
		scan_cleanup(&scan);
	}
}

#ifndef CLUSTER_SCAN_FULL_ENTRY
#define CLUSTER_SCAN_FULL_ENTRY main
#endif
int
CLUSTER_SCAN_FULL_ENTRY(void)
{
	UT_PLAN(15);
	UT_RUN(page_mode_full_scan_preserves_private_rows_both_directions);
	UT_RUN(row_mode_full_scan_preserves_private_rows_both_directions);
	UT_RUN(tid_range_full_scan_preserves_private_rows_both_directions);
	UT_RUN(v1_and_child_modifiers_use_both_roles_in_scan);
	UT_RUN(incomplete_or_wrong_full_proof_refuses_whole_page);
	UT_RUN(snapshot_too_old_keeps_its_distinct_error);
	UT_RUN(retained_full_rejects_snapshot_epoch_or_page_change);
	UT_RUN(rescan_and_new_page_retire_full_with_its_bytes);
	UT_RUN(endscan_forgets_full_before_freeing_owner);
	UT_RUN(tid_range_bounds_retire_page_and_allow_reverse_restart);
	UT_RUN(dormant_native_local_and_non_mvcc_keep_original_path);
	UT_RUN(empty_full_still_requires_the_whole_page_receipt);
	UT_RUN(fetch_cancellation_retires_page_proof_and_snapshot_scope);
	UT_RUN(serializable_full_refuses_before_output_or_unlocked_conflict);
	UT_RUN(serializable_dormant_scan_keeps_locked_native_conflicts);
	UT_DONE();
	return ut_failed_count != 0;
}
