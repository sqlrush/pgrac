/* Original complete heap checker and raw VM reader. Heap I/O, tuple/TOAST
 * classifiers, fmgr and cancellation are controlled leaves. This verifies
 * diagnostic pin ownership, not native ResourceOwner, ACK or durability.
 * Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com> */
#include "postgres.h"
#include "access/heaptoast.h"
#include "catalog/pg_am.h"
#include "funcapi.h"
#include "utils/builtins.h"
#include "utils/snapmgr.h"

int amcheck_vm_inherited_main(void);
#define main amcheck_vm_inherited_main
#include "test_cluster_vm_read_visibility.c"
#undef main

#include "test_cluster_vm_amcheck_types.inc"

Datum verify_heapam(PG_FUNCTION_ARGS);

static RelationData am_relation;
static FormData_pg_class am_form;
static SnapshotData am_snapshot;
static PGAlignedBlock am_heap[4];
static BlockNumber am_nblocks, am_block;
static unsigned am_pins, am_reads, am_unlocks, am_checks, am_corruptions;
static unsigned am_mask, am_cancel_at, am_error_at, am_drops;
static bool am_locked, am_drop;
static sigjmp_buf am_jump;

static void
am_no_shared_vm_pin(void)
{
	if (cluster_heap_maintenance_restricted(&am_relation))
		UT_ASSERT_EQ(am_pins, 0);
}

static void
am_interrupt(void)
{
	am_no_shared_vm_pin();
	if (++am_checks == am_cancel_at)
		siglongjmp(am_jump, 1);
}

static uint8
am_raw_status(Relation rel, BlockNumber block, Buffer *buffer)
{
	bool had_pin = BufferIsValid(*buffer);
	uint8 status = visibilitymap_get_status(rel, block, buffer);

	if (!had_pin && BufferIsValid(*buffer))
		am_pins++;
	return status;
}

static void
am_release(Buffer buffer)
{
	UT_ASSERT_EQ(buffer, 1);
	UT_ASSERT_EQ(am_pins, 1);
	UT_ASSERT(!am_locked);
	if (am_pins > 0)
		am_pins--;
	ReleaseBuffer(buffer);
}

static Buffer
am_read_heap(Relation rel, ForkNumber fork, BlockNumber block,
			 ReadBufferMode mode, BufferAccessStrategy strategy pg_attribute_unused())
{
	am_no_shared_vm_pin();
	UT_ASSERT(rel == &am_relation && fork == MAIN_FORKNUM && mode == RBM_NORMAL);
	UT_ASSERT(block < am_nblocks && !am_locked);
	am_reads++;
	am_block = block;
	am_mask |= 1U << block;
	if (am_reads == am_error_at)
		siglongjmp(am_jump, 1);
	/* A storage leaf changes the next pair only once this diagnostic has
	 * retired its pin. This is not a real holder or invalidation ACK. */
	if (am_drop && am_pins == 0) {
		PageGetContents(disk_vm.data)[0] &= ~(VISIBILITYMAP_VALID_BITS << 2);
		am_drop = false;
		am_drops++;
	}
	return 2;
}

static void
am_lock_heap(Buffer buffer, int mode)
{
	am_no_shared_vm_pin();
	UT_ASSERT(buffer == 2 && mode == BUFFER_LOCK_SHARE && !am_locked);
	am_locked = true;
}

static Page
am_heap_page(Buffer buffer)
{
	UT_ASSERT(buffer == 2 && am_locked);
	return am_heap[am_block].data;
}

static void
am_unlock_heap(Buffer buffer)
{
	UT_ASSERT(buffer == 2 && am_locked);
	am_locked = false;
	am_unlocks++;
}

static void
am_close(Relation rel, LOCKMODE mode)
{
	UT_ASSERT(rel == &am_relation && mode == AccessShareLock);
	UT_ASSERT_EQ(am_pins, 0);
	UT_ASSERT(!am_locked);
}

static void
am_corrupt(HeapCheckContext *ctx)
{
	UT_ASSERT(ctx->page == am_heap[am_block].data && am_locked);
	am_corruptions++;
	ctx->is_corrupt = true;
}

/* Only the diagnostic page driver is under test; no normal tuple or TOAST
 * classification can silently succeed through these controlled leaves. */
#define check_tuple(...) abort()
#define check_toasted_attribute(ctx, attribute) ((void)(ctx), (void)(attribute), abort())
#define report_corruption(ctx, ...) am_corrupt(ctx)
#define update_cached_xid_range(ctx) ((void)(ctx))
#define update_cached_mxid_range(ctx) ((void)(ctx))
#define FullTransactionIdFromXidAndCtx(xid, ctx) ((void)(ctx), FullTransactionIdFromEpochAndXid(0, (xid)))
#undef HeapTupleHeaderGetUpdateXid
#define HeapTupleHeaderGetUpdateXid(header) ((void)(header), abort(), InvalidTransactionId)
#define TransactionIdIsInProgress(xid) ((void)(xid), abort(), false)
#define GetTransactionSnapshot() (&am_snapshot)
#define RecoveryInProgress() false
#undef PG_GETARG_TEXT_PP
#define PG_GETARG_TEXT_PP(n) ((text *) DatumGetPointer(PG_GETARG_DATUM(n)))
#define text_to_cstring(value) ((char *)(value))
#define InitMaterializedSRF(...) ((void)0)
#define relation_open(oid, mode) ((void)(oid), (void)(mode), &am_relation)
#define relation_close am_close
#undef RelationGetNumberOfBlocks
#define RelationGetNumberOfBlocks(rel) ((void)(rel), am_nblocks)
#define GetAccessStrategy(kind) ((void)(kind), (BufferAccessStrategy)1)
#define toast_open_indexes(...) (abort(), 0)
#define toast_close_indexes(...) abort()
#define table_open(...) (abort(), (Relation)NULL)
#define table_close(...) abort()
#define list_free_deep(...) abort()
#undef CHECK_FOR_INTERRUPTS
#define CHECK_FOR_INTERRUPTS() am_interrupt()
#define visibilitymap_get_status am_raw_status
#undef ReleaseBuffer
#define ReleaseBuffer am_release
#define ReadBufferExtended am_read_heap
#undef LockBuffer
#define LockBuffer am_lock_heap
#undef BufferGetPage
#define BufferGetPage am_heap_page
#define UnlockReleaseBuffer am_unlock_heap
#ifdef FIXTURE_NO_ASSERTS
#undef USE_ASSERT_CHECKING
#undef Assert
#define Assert(condition) ((void)0)
#endif
#ifndef AMCHECK_VM_ENTRY_FIXTURE
#define AMCHECK_VM_ENTRY_FIXTURE "test_cluster_vm_amcheck_entry.inc"
#endif
#include AMCHECK_VM_ENTRY_FIXTURE

static void
am_reset(bool shared, bool local, uint8 bits)
{
	memset(&am_relation, 0, sizeof(am_relation));
	memset(&am_form, 0, sizeof(am_form));
	memset(&am_snapshot, 0, sizeof(am_snapshot));
	am_relation.rd_rel = &am_form;
	am_relation.rd_id = 101;
	am_form.relkind = RELKIND_RELATION;
	am_form.relam = HEAP_TABLE_AM_OID;
	am_form.relpersistence = local ? RELPERSISTENCE_TEMP : RELPERSISTENCE_PERMANENT;
	am_form.relfrozenxid = 3;
	am_snapshot.xmin = 50;
	fixture_shared = shared;
	fixture_reads = fixture_releases = 0;
	fixture_smgr.smgr_cached_nblocks[VISIBILITYMAP_FORKNUM] = 1;
	memset(&disk_vm, 0, sizeof(disk_vm));
	((PageHeader)disk_vm.data)->pd_lower = SizeOfPageHeaderData;
	((PageHeader)disk_vm.data)->pd_upper = BLCKSZ;
	PageGetContents(disk_vm.data)[0] = bits;
	for (int i = 0; i < 4; i++) {
		memset(&am_heap[i], 0, sizeof(am_heap[i]));
		((PageHeader)am_heap[i].data)->pd_lower = SizeOfPageHeaderData + sizeof(ItemIdData);
		((PageHeader)am_heap[i].data)->pd_upper = BLCKSZ;
	}
	am_nblocks = 4;
	am_pins = am_reads = am_unlocks = am_checks = am_corruptions = 0;
	am_mask = am_cancel_at = am_error_at = am_drops = 0;
	am_locked = am_drop = false;
}

static void
am_call(const char *skip, bool stop)
{
	LOCAL_FCINFO(call, 6);
	ReturnSetInfo result = {0};

	InitFunctionCallInfoData(*call, NULL, 6, InvalidOid, NULL, (Node *)&result);
	for (int i = 0; i < 6; i++)
		call->args[i].isnull = false;
	call->args[0].value = ObjectIdGetDatum(101);
	call->args[1].value = BoolGetDatum(stop);
	call->args[2].value = BoolGetDatum(false);
	call->args[3].value = CStringGetDatum(skip);
	call->args[4].isnull = call->args[5].isnull = true;
	(void)verify_heapam(call);
	UT_ASSERT(call->isnull);
}

UT_TEST(shared_checked_pages_drop_vm_before_heap_io)
{
	am_reset(true, false, 0);
	am_call("all-visible", false);
	UT_ASSERT_EQ(am_mask, 15);
	UT_ASSERT_EQ(am_reads, 4);
	UT_ASSERT_EQ(am_unlocks, 4);
	UT_ASSERT_EQ(fixture_releases, 4);
}

UT_TEST(raw_av_af_and_af_only_keep_original_skip_results)
{
	/* Four raw pairs: AV+AF, AV, 0, AF-only. */
	am_reset(true, false, 0x87);
	am_call("all-visible", false);
	UT_ASSERT_EQ(am_mask, 12);
	UT_ASSERT_EQ(fixture_releases, 4);
	am_reset(true, false, 0x87);
	am_call("all-frozen", false);
	UT_ASSERT_EQ(am_mask, 6);
	UT_ASSERT_EQ(fixture_releases, 4);
}

UT_TEST(all_skipped_pages_drop_vm_before_next_cancel_check)
{
	am_reset(true, false, 0xff);
	am_call("all-frozen", false);
	UT_ASSERT_EQ(am_reads, 0);
	UT_ASSERT_EQ(am_checks, 4);
	UT_ASSERT_EQ(fixture_releases, 4);
}

UT_TEST(skip_none_never_reads_vm)
{
	am_reset(true, false, 0xff);
	am_call("none", false);
	UT_ASSERT_EQ(am_mask, 15);
	UT_ASSERT_EQ(fixture_reads, 0);
	UT_ASSERT_EQ(fixture_releases, 0);
}

UT_TEST(native_and_local_keep_original_cached_raw_reader)
{
	for (int local = 0; local < 2; local++) {
		am_reset(local, local, 0x87);
		am_call("all-visible", false);
		UT_ASSERT_EQ(am_mask, 12);
		UT_ASSERT_EQ(fixture_reads, 1);
		UT_ASSERT_EQ(fixture_releases, 1);
	}
}

UT_TEST(shared_cancellation_arrives_without_vm_pin)
{
	am_reset(true, false, 0xff);
	am_cancel_at = 2;
	if (sigsetjmp(am_jump, 1) == 0) {
		am_call("all-visible", false);
		UT_ASSERT(false);
	}
	UT_ASSERT_EQ(am_checks, 2);
	UT_ASSERT_EQ(am_pins, 0);
	UT_ASSERT(!am_locked);
}

UT_TEST(shared_heap_error_arrives_without_vm_pin)
{
	am_reset(true, false, 0);
	am_error_at = 1;
	if (sigsetjmp(am_jump, 1) == 0) {
		am_call("all-visible", false);
		UT_ASSERT(false);
	}
	UT_ASSERT_EQ(am_reads, 1);
	UT_ASSERT_EQ(am_pins, 0);
	UT_ASSERT(!am_locked);
}

UT_TEST(missing_vm_and_empty_relation_keep_original_results)
{
	am_reset(true, false, 0xff);
	fixture_smgr.smgr_cached_nblocks[VISIBILITYMAP_FORKNUM] = 0;
	am_call("all-frozen", false);
	UT_ASSERT_EQ(am_mask, 15);
	UT_ASSERT_EQ(fixture_reads, 0);
	UT_ASSERT_EQ(fixture_releases, 0);
	am_reset(true, false, 0xff);
	am_nblocks = 0;
	am_call("all-visible", false);
	UT_ASSERT_EQ(am_reads, 0);
	UT_ASSERT_EQ(fixture_reads, 0);
}

UT_TEST(original_corruption_stop_does_not_retain_vm_pin)
{
	am_reset(true, false, 0);
	ItemIdSetRedirect(PageGetItemId(am_heap[0].data, 1), 0);
	am_call("all-visible", true);
	UT_ASSERT_EQ(am_corruptions, 1);
	UT_ASSERT_EQ(am_reads, 1);
	UT_ASSERT_EQ(am_unlocks, 1);
	UT_ASSERT_EQ(fixture_releases, 1);
}

UT_TEST(shared_next_raw_read_can_observe_after_pin_retirement)
{
	am_reset(true, false, 0xfc);
	am_drop = true;
	am_call("all-visible", false);
	UT_ASSERT_EQ(am_drops, 1);
	UT_ASSERT_EQ(am_mask, 3);
	UT_ASSERT_EQ(fixture_reads, 4);
}

int
main(void)
{
	UT_PLAN(10);
	UT_RUN(shared_checked_pages_drop_vm_before_heap_io);
	UT_RUN(raw_av_af_and_af_only_keep_original_skip_results);
	UT_RUN(all_skipped_pages_drop_vm_before_next_cancel_check);
	UT_RUN(skip_none_never_reads_vm);
	UT_RUN(native_and_local_keep_original_cached_raw_reader);
	UT_RUN(shared_cancellation_arrives_without_vm_pin);
	UT_RUN(shared_heap_error_arrives_without_vm_pin);
	UT_RUN(missing_vm_and_empty_relation_keep_original_results);
	UT_RUN(original_corruption_stop_does_not_retain_vm_pin);
	UT_RUN(shared_next_raw_read_can_observe_after_pin_retirement);
	UT_DONE();
	return ut_failed_count != 0;
}
