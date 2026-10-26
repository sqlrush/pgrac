/*-------------------------------------------------------------------------
 *
 * test_cluster_xid_full_clog.inc
 *    Check original allocator FullXid inputs to dormant native CLOG birth.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *    src/test/cluster_unit/test_cluster_xid_full_clog.inc
 *
 * NOTES
 *    Spec: 2026-10-14-pre2-m5-e1-format-closure-v1.md
 *
 *-------------------------------------------------------------------------
 */

#ifdef XID_FULL_CLOG_CONTRACT_RED
/* Explicit old raw-entry contract comparison, not a new native entry. */
#define GetNewTransactionIdFullCLOG GetNewTransactionId
#endif
extern FullTransactionId GetNewTransactionIdFullCLOG(bool isSubXact);
static void reset_allocator(void);
static void extended(void);

static struct {
	bool active;
	bool expect_error;
	unsigned count;
	unsigned raw_calls;
	unsigned early_publication;
	unsigned fail_after;
	uint64 full[64];
	jmp_buf error;
} full_clog_fixture;

void
ExtendCLOGFull(FullTransactionId candidate)
{
	unsigned i = full_clog_fixture.count++;

	if (!full_clog_fixture.active || i >= lengthof(full_clog_fixture.full))
		abort();
	full_clog_fixture.full[i] = U64FromFullTransactionId(candidate);
	if (proc.xid != 0 || visible_xids[0] != 0 || proc.subxidStatus.count != 0)
		full_clog_fixture.early_publication++;
	extended();
	if (full_clog_fixture.fail_after == i + 1)
		longjmp(full_clog_fixture.error, 1);
}

static void
full_clog_reset(uint64 next)
{
	reset_allocator();
	memset(&full_clog_fixture, 0, sizeof(full_clog_fixture));
	full_clog_fixture.active = true;
	variables.nextXid = FullTransactionIdFromU64(next);
	reservation = true;
	floor_full = 0;
	limit_full = UINT64_MAX;
}

static bool
full_clog_call_caught(void)
{
	if (setjmp(full_clog_fixture.error) == 0) {
		(void)GetNewTransactionIdFullCLOG(false);
		return false;
	}
	return true;
}

UT_TEST(full_clog_allocator_uses_original_candidate_and_gap)
{
	static const uint64 expected[] = {
		UINT64_C(0x500007ffe), UINT64_C(0x500007fff),
		UINT64_C(0x500008000), UINT64_C(0x500008001),
		UINT64_C(0x500008002), UINT64_C(0x500008003),
		UINT64_C(0x500008004)
	};
	full_clog_reset(UINT64_C(0x500007ffe));
	UT_ASSERT_EQ(U64FromFullTransactionId(GetNewTransactionIdFullCLOG(false)),
		UINT64_C(0x500008004));
	UT_ASSERT_EQ(full_clog_fixture.count, lengthof(expected));
	for (unsigned i = 0; i < lengthof(expected); ++i)
		UT_ASSERT_EQ(full_clog_fixture.full[i], expected[i]);
	UT_ASSERT_EQ(full_clog_fixture.raw_calls, 0);
	UT_ASSERT_EQ(full_clog_fixture.early_publication, 0);
	UT_ASSERT_EQ(extensions, 21);
	UT_ASSERT_EQ(proc.xid, 32772);
	UT_ASSERT_EQ(visible_xids[0], 32772);
	UT_ASSERT_EQ(U64FromFullTransactionId(variables.nextXid), UINT64_C(0x500008005));
	UT_ASSERT_EQ(lock_errors, 0);
	UT_ASSERT(!held);
}

UT_TEST(full_clog_allocator_keeps_epoch_across_native_wrap)
{
	static const uint64 expected[] = {
		UINT64_C(0x7fffffffe), UINT64_C(0x7ffffffff),
		UINT64_C(0x800000003), UINT64_C(0x800000004)
	};
	full_clog_reset(UINT64_C(0x7fffffffe));
	UT_ASSERT_EQ(U64FromFullTransactionId(GetNewTransactionIdFullCLOG(true)),
		UINT64_C(0x800000004));
	UT_ASSERT_EQ(full_clog_fixture.count, lengthof(expected));
	for (unsigned i = 0; i < lengthof(expected); ++i)
		UT_ASSERT_EQ(full_clog_fixture.full[i], expected[i]);
	UT_ASSERT_EQ(full_clog_fixture.raw_calls | full_clog_fixture.early_publication, 0);
	UT_ASSERT_EQ(proc.xid | visible_xids[0], 0);
	UT_ASSERT_EQ(subxids[0].count, 1);
	UT_ASSERT_EQ(proc.subxids.xids[0], 4);
	UT_ASSERT_EQ(U64FromFullTransactionId(variables.nextXid), UINT64_C(0x800000005));
	UT_ASSERT_EQ(lock_errors, 0);
	UT_ASSERT(!held);
}

UT_TEST(full_clog_allocator_rederives_after_original_lease_wait)
{
	full_clog_reset(2052);
	reservation = false;
	advance_while_unlocked = true;
	UT_ASSERT_EQ(U64FromFullTransactionId(GetNewTransactionIdFullCLOG(false)), 2084);
	UT_ASSERT_EQ(waits, 1);
	UT_ASSERT_EQ(full_clog_fixture.count, 1);
	UT_ASSERT_EQ(full_clog_fixture.full[0], 2084);
	UT_ASSERT_EQ(full_clog_fixture.raw_calls | full_clog_fixture.early_publication, 0);
	UT_ASSERT_EQ(early_extensions | lock_errors, 0);
	UT_ASSERT_EQ(visible_xids[0], 2084);
	UT_ASSERT(!held);
}

UT_TEST(full_clog_extension_error_precedes_native_publication)
{
	static const unsigned fail_after[] = {1, 3, 7};

	for (unsigned i = 0; i < lengthof(fail_after); ++i) {
		full_clog_reset(UINT64_C(0x500007ffe));
		full_clog_fixture.fail_after = fail_after[i];
		UT_ASSERT(full_clog_call_caught());
		UT_ASSERT_EQ(full_clog_fixture.count, fail_after[i]);
		UT_ASSERT_EQ(U64FromFullTransactionId(variables.nextXid), UINT64_C(0x500007ffe));
		UT_ASSERT_EQ(proc.xid | visible_xids[0] | subxids[0].count, 0);
		UT_ASSERT_EQ(full_clog_fixture.raw_calls | full_clog_fixture.early_publication, 0);
		UT_ASSERT(held);
		/* Original native caller ERROR cleanup; never retry the allocation. */
		LWLockRelease(XidGenLock);
		UT_ASSERT_EQ(lock_errors, 0);
	}
}

UT_TEST(full_clog_entry_refuses_non_normal_processing)
{
	static const ProcessingMode modes[] = {BootstrapProcessing, InitProcessing};

	for (unsigned i = 0; i < lengthof(modes); ++i) {
		full_clog_reset(2052);
		Mode = modes[i];
		full_clog_fixture.expect_error = true;
		UT_ASSERT(full_clog_call_caught());
		UT_ASSERT_EQ(full_clog_fixture.count | extensions | proc.xid | visible_xids[0], 0);
		UT_ASSERT_EQ(U64FromFullTransactionId(variables.nextXid), 2052);
		UT_ASSERT(!held);
		Mode = NormalProcessing;
	}
}

UT_TEST(ordinary_allocator_keeps_raw_clog_route)
{
	full_clog_reset(2053);
	cluster_enabled = cluster_shared_catalog = cluster_xid_striping = false;
	UT_ASSERT_EQ(U64FromFullTransactionId(GetNewTransactionId(false)), 2053);
	UT_ASSERT_EQ(full_clog_fixture.count, 0);
	UT_ASSERT_EQ(full_clog_fixture.raw_calls, 1);
	UT_ASSERT_EQ(extensions, 3);
	UT_ASSERT_EQ(proc.xid, 2053);
	UT_ASSERT_EQ(lock_errors, 0);
	UT_ASSERT(!held);
	full_clog_fixture.active = false;
}
