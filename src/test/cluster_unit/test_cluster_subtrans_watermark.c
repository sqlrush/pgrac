/*-------------------------------------------------------------------------
 *
 * test_cluster_subtrans_watermark.c
 *    Exercise the actual checkpoint SUBTRANS truncation consumer.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *    src/test/cluster_unit/test_cluster_subtrans_watermark.c
 *
 * NOTES
 *    TruncateSUBTRANS and FullXid widening are production code. Lease,
 *    watermark, LWLock and SLRU leaves are controlled process boundaries.
 *    Dispatch checks do not prove physical deletion or complete native pins.
 *-------------------------------------------------------------------------
 */
#include "postgres.h"
#include "unit_test.h"
#include "access/slru.h"
#include "access/subtrans.h"
#include "access/transam.h"
#include "cluster/cluster_guc.h"
#include "cluster/cluster_heap_horizon.h"
#include "cluster/cluster_mode.h"
#include "cluster/cluster_xid_stripe.h"
#include "cluster/cluster_xid_stripe_boot.h"
#include "storage/lwlock.h"

UT_DEFINE_GLOBALS();

bool cluster_enabled = true;
bool cluster_shared_catalog = true;
bool cluster_xid_striping = true;
int cluster_node_id = 1;
static VariableCacheData cache;
VariableCache ShmemVariableCache = &cache;
static LWLockPadded locks[NUM_INDIVIDUAL_LWLOCKS];
LWLockPadded *MainLWLockArray = locks;
static SlruCtlData SubTransCtlData;
#define SubTransCtl (&SubTransCtlData)
#define TransactionIdToPage(xid) ((xid) / (TransactionId)(BLCKSZ / sizeof(TransactionId)))

static bool xid_locked, lease_ready, lease_current, watermark_ready;
static bool revoke_on_watermark;
static uint64 legal_next, watermark_full;
static unsigned samples, watermarks, fences, truncations;
static int truncated_page;

bool
LWLockAcquire(LWLock *lock, LWLockMode mode)
{
	UT_ASSERT(lock == XidGenLock && !xid_locked);
	UT_ASSERT_EQ(mode, LW_SHARED);
	xid_locked = true;
	return true;
}

void
LWLockRelease(LWLock *lock)
{
	UT_ASSERT(lock == XidGenLock && xid_locked);
	xid_locked = false;
}

bool
cluster_xid_stripe_sample_future(FullTransactionId reference, FullTransactionId *future,
								 ClusterXidStripeSampleFence *fence)
{
	UT_ASSERT(xid_locked);
	UT_ASSERT_EQ(U64FromFullTransactionId(reference), U64FromFullTransactionId(cache.nextXid));
	samples++;
	memset(fence, 0, sizeof(*fence));
	*future = InvalidFullTransactionId;
	if (!lease_ready)
		return false;
	fence->incarnation = 81;
	*future = FullTransactionIdFromU64(legal_next);
	return true;
}

bool
cluster_heap_freeze_watermark_sample(ClusterHeapFreezeWatermark *out)
{
	UT_ASSERT(!xid_locked);
	UT_ASSERT_EQ(truncations, 0);
	watermarks++;
	if (revoke_on_watermark)
		lease_current = false;
	if (!watermark_ready)
		return false;
	memset(out, 0, sizeof(*out));
	out->active_full_xid_min = watermark_full;
	return true;
}

bool
cluster_xid_stripe_sample_fence_current(const ClusterXidStripeSampleFence *fence)
{
	UT_ASSERT(!xid_locked);
	UT_ASSERT_EQ(fence->incarnation, 81);
	UT_ASSERT_EQ(truncations, 0);
	fences++;
	return lease_current;
}

void
SimpleLruTruncate(SlruCtl ctl, int cutoffPage)
{
	UT_ASSERT(ctl == SubTransCtl);
	UT_ASSERT(!xid_locked);
	truncations++;
	truncated_page = cutoffPage;
}

void
ExceptionalCondition(const char *condition, const char *file, int line)
{
	fprintf(stderr, "Assertion failed: %s, %s:%d\n", condition, file, line);
	abort();
}

#include "test_cluster_subtrans_watermark.inc"

static void
reset(void)
{
	UT_ASSERT(!xid_locked);
	cluster_enabled = cluster_shared_catalog = cluster_xid_striping = true;
	cluster_node_id = 1;
	ShmemVariableCache = &cache;
	cache.nextXid = FullTransactionIdFromU64(20000);
	legal_next = 20000;
	watermark_full = 7000;
	lease_ready = lease_current = watermark_ready = true;
	revoke_on_watermark = false;
	samples = watermarks = fences = truncations = 0;
	truncated_page = -1;
}

static void
expect_kept(void)
{
	UT_ASSERT_EQ(truncations, 0);
	UT_ASSERT(!xid_locked);
}

static void
expect_page(int page)
{
	UT_ASSERT_EQ(truncations, 1);
	UT_ASSERT_EQ(truncated_page, page);
	UT_ASSERT(!xid_locked);
}

UT_TEST(watermark_retains_history_before_running_checkpoint_cut)
{
	reset();
	TruncateSUBTRANS(10000);
	expect_page(3);
	UT_ASSERT_EQ(samples, 1);
	UT_ASSERT_EQ(watermarks, 1);
	UT_ASSERT_EQ(fences, 1);
}

UT_TEST(watermark_never_raises_running_checkpoint_cut)
{
	reset();
	watermark_full = 16000;
	TruncateSUBTRANS(10000);
	expect_page(4);
}

UT_TEST(missing_watermark_keeps_subtrans)
{
	reset();
	watermark_ready = false;
	TruncateSUBTRANS(10000);
	expect_kept();
}

UT_TEST(invalid_watermark_keeps_subtrans)
{
	static const uint64 invalid[] = { 0, 1, 2, 20001, UINT64CONST(0x100000001) };

	for (unsigned i = 0; i < lengthof(invalid); i++) {
		reset();
		watermark_full = invalid[i];
		TruncateSUBTRANS(10000);
		expect_kept();
	}
}

UT_TEST(half_space_or_retained_generation_ambiguity_keeps_subtrans)
{
	for (unsigned older = 0; older < 2; older++) {
		reset();
		legal_next = UINT64CONST(0x100004e20);
		watermark_full = legal_next - UINT64CONST(0x80000000) - older;
		TruncateSUBTRANS(10000);
		expect_kept();
	}
}

UT_TEST(previous_epoch_watermark_is_compared_as_fullxid)
{
	reset();
	legal_next = UINT64CONST(0x100004e20);
	watermark_full = UINT64CONST(0xfffff801);
	TruncateSUBTRANS(10000);
	expect_page(2097151);
}

UT_TEST(watermark_page_boundary_keeps_original_retreat)
{
	reset();
	watermark_full = 6144;
	TruncateSUBTRANS(10000);
	expect_page(2);
}

UT_TEST(native_page_boundary_keeps_original_retreat)
{
	reset();
	watermark_full = 16000;
	TruncateSUBTRANS(6144);
	expect_page(2);
}

UT_TEST(first_normal_watermark_retreat_skips_special_xids)
{
	reset();
	legal_next = UINT64CONST(0x100004e20);
	watermark_full = UINT64CONST(0x100000003);
	TruncateSUBTRANS(10000);
	expect_page(2097151);
}

UT_TEST(missing_allocator_promise_keeps_subtrans)
{
	for (unsigned missing = 0; missing < 3; missing++) {
		reset();
		if (missing == 0)
			lease_ready = false;
		if (missing == 1)
			cluster_xid_striping = false;
		if (missing == 2)
			ShmemVariableCache = NULL;
		TruncateSUBTRANS(10000);
		expect_kept();
	}
}

UT_TEST(invalid_native_or_legal_bound_keeps_subtrans)
{
	static const TransactionId invalid_native[] = { 0, 1, 2, 20001, 0x80004e20U };
	static const uint64 invalid_legal[] = { 0, 1, UINT64CONST(0x100000002), 9000 };

	for (unsigned i = 0; i < lengthof(invalid_native); i++) {
		reset();
		TruncateSUBTRANS(invalid_native[i]);
		expect_kept();
	}
	for (unsigned i = 0; i < lengthof(invalid_legal); i++) {
		reset();
		legal_next = invalid_legal[i];
		TruncateSUBTRANS(10000);
		expect_kept();
	}
}

UT_TEST(retired_lease_after_watermark_read_keeps_subtrans)
{
	reset();
	revoke_on_watermark = true;
	TruncateSUBTRANS(10000);
	expect_kept();
}

UT_TEST(nontarget_modes_preserve_native_truncation)
{
	for (unsigned mode = 0; mode < 3; mode++) {
		reset();
		lease_ready = watermark_ready = false;
		if (mode == 0)
			cluster_enabled = false;
		if (mode == 1)
			cluster_shared_catalog = false;
		if (mode == 2)
			cluster_node_id = -1;
		TruncateSUBTRANS(6144);
		expect_page(2);
		UT_ASSERT_EQ(samples, 0);
		UT_ASSERT_EQ(watermarks, 0);
		UT_ASSERT_EQ(fences, 0);
	}
}

int
main(void)
{
	UT_PLAN(13);
	UT_RUN(watermark_retains_history_before_running_checkpoint_cut);
	UT_RUN(watermark_never_raises_running_checkpoint_cut);
	UT_RUN(missing_watermark_keeps_subtrans);
	UT_RUN(invalid_watermark_keeps_subtrans);
	UT_RUN(half_space_or_retained_generation_ambiguity_keeps_subtrans);
	UT_RUN(previous_epoch_watermark_is_compared_as_fullxid);
	UT_RUN(watermark_page_boundary_keeps_original_retreat);
	UT_RUN(native_page_boundary_keeps_original_retreat);
	UT_RUN(first_normal_watermark_retreat_skips_special_xids);
	UT_RUN(missing_allocator_promise_keeps_subtrans);
	UT_RUN(invalid_native_or_legal_bound_keeps_subtrans);
	UT_RUN(retired_lease_after_watermark_read_keeps_subtrans);
	UT_RUN(nontarget_modes_preserve_native_truncation);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
