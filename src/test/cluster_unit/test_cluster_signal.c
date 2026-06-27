/*-------------------------------------------------------------------------
 *
 * test_cluster_signal.c
 *	  Compile-time and link-level invariants for the cluster signal
 *	  framework introduced in stage 0.15.
 *
 *	  Stage 0.15 extends PG's ProcSignalReason enum with
 *	  PROCSIG_CLUSTER_RECONFIG_START and wires a matching async-signal-safe
 *	  handler.  This unit test exercises only the pieces a PG-free
 *	  binary can verify:
 *
 *	  - PROCSIG_CLUSTER_RECONFIG_START exists and sits at the very end
 *	    of the enum (i.e. greater than the last PG-native reason).
 *	  - PG's original 14 reasons (PROCSIG_CATCHUP_INTERRUPT through
 *	    PROCSIG_RECOVERY_CONFLICT_STARTUP_DEADLOCK) keep their numeric
 *	    positions (spot-checked: PROCSIG_NOTIFY_INTERRUPT == 1).
 *	  - NUM_PROCSIGNALS == 15 (PG 14 + pgrac 1).
 *	  - cluster_reconfig_start_pending defaults to 0 (false).
 *	  - cluster_handle_reconfig_start_interrupt is linkable.
 *	  - Calling the handler sets cluster_reconfig_start_pending = 1.
 *
 *	  Runtime regression (server starts cleanly, NOTIFY/LISTEN still
 *	  work) is covered by cluster_tap t/009_signal.pl.
 *
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/test/cluster_unit/test_cluster_signal.c
 *
 * NOTES
 *	  This is a pgrac-original file.  Linking cluster_signal.o pulls in
 *	  SetLatch + MyLatch + MyProc references; we stub those locally so
 *	  the standalone test does not need a full PG backend.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "cluster/cluster_qvotec.h"
#include "storage/procsignal.h"
#include "cluster/cluster_signal.h"

/*
 * postgres.h transitively pulls in port.h which redirects printf etc.
 * Standalone unit-test binaries do not link libpgport, so undo the
 * redirection before pulling in unit_test.h.
 */
#undef printf
#undef fprintf
#undef snprintf
#undef sprintf
#undef vsnprintf
#undef vfprintf
#undef vprintf
#undef vsprintf
#undef strerror
#undef strerror_r

#include "unit_test.h"


/* ----------
 * Stubs needed to link cluster_signal.o standalone.
 *
 *	cluster_signal.c calls SetLatch(MyLatch) which pulls in PG backend
 *	symbols.  Provide local stubs so the unit test resolves at link
 *	time without dragging in the whole backend.  The test directly
 *	calls cluster_handle_reconfig_start_interrupt() and observes the
 *	flag + InterruptPending transitions; the SetLatch stub does nothing.
 * ----------
 */
#include "storage/latch.h"

struct Latch fake_latch;
struct Latch *MyLatch = &fake_latch;

void
SetLatch(Latch *latch pg_attribute_unused())
{
	/* Stub: real impl is in src/backend/storage/ipc/latch.c */
}

/*
 * spec-2.28 Sprint A Step 2 stubs: cluster_signal.o now references
 * cluster_freeze_writes_set / cluster_thaw_writes_set (spec-2.6
 * cluster_qvotec.c) and ClusterFenceFreezePending (spec-2.28
 * cluster_fence.c) from its dual-set freeze handler + thaw handler.
 * Provide standalone stubs so test_cluster_signal still links.  The
 * ProcessInterrupts arm is verified here;the remaining asymmetric-thaw
 * semantic is verified end-to-end by test_cluster_fence.
 */
#include <signal.h>
volatile sig_atomic_t ClusterFenceFreezePending = 0;
volatile sig_atomic_t InterruptPending = 0;
static int ut_freeze_writes_set_count = 0;
void
cluster_freeze_writes_set(void)
{
	ut_freeze_writes_set_count++;
}
void
cluster_thaw_writes_set(void)
{}


UT_DEFINE_GLOBALS();


UT_TEST(test_cluster_reconfig_start_after_recovery_conflict_bufferpin)
{
	/*
	 * The cluster reason must be appended after PG's last native reason
	 * (PROCSIG_RECOVERY_CONFLICT_STARTUP_DEADLOCK).  This guards against
	 * accidentally inserting cluster values into the middle of PG's
	 * range, which would shift PG numeric positions and break ABI.
	 */
	UT_ASSERT(PROCSIG_CLUSTER_RECONFIG_START > PROCSIG_RECOVERY_CONFLICT_STARTUP_DEADLOCK);
}


UT_TEST(test_pg_native_reasons_unchanged)
{
	/*
	 * Spot-check that PG's first few reasons retain their canonical
	 * numeric values.  PROCSIG_CATCHUP_INTERRUPT is the first entry
	 * (== 0); PROCSIG_NOTIFY_INTERRUPT is the second (== 1).
	 */
	UT_ASSERT_EQ(PROCSIG_CATCHUP_INTERRUPT, 0);
	UT_ASSERT_EQ(PROCSIG_NOTIFY_INTERRUPT, 1);
}


UT_TEST(test_num_procsignals_is_22)
{
	/* PG's 14 native reasons (PROCSIG_CATCHUP_INTERRUPT through
	 * PROCSIG_RECOVERY_CONFLICT_STARTUP_DEADLOCK) + 8 pgrac reasons:
	 * RECONFIG_START (spec-1.11) + CLUSTER_FREEZE_WRITES +
	 * CLUSTER_THAW_WRITES (spec-2.6 D5) + GES_BAST + GES_CANCEL
	 * (spec-2.17 Q8 + Q9) + GRD_REDECLARE (spec-4.6 D3) +
	 * CLUSTER_HANG_DUMP (spec-5.11 D5) +
	 * CLUSTER_CLEAN_LEAVE_QUIESCE (spec-5.13 D7). */
	UT_ASSERT_EQ(NUM_PROCSIGNALS, 22);
}


UT_TEST(test_cluster_reconfig_start_pending_default_false)
{
	UT_ASSERT_EQ(cluster_reconfig_start_pending, 0);
}


UT_TEST(test_handler_symbol_linkable)
{
	UT_ASSERT_NOT_NULL((void *)cluster_handle_reconfig_start_interrupt);
}


UT_TEST(test_handler_sets_pending_flag)
{
	/*
	 * Direct invocation of the handler proves end-to-end behavior:
	 * before the call the flag is false; after the call it is true.
	 * The SetLatch stub above absorbs the latch poke without doing
	 * anything observable in this binary.
	 */
	cluster_reconfig_start_pending = 0;
	InterruptPending = 0;
	cluster_handle_reconfig_start_interrupt();
	UT_ASSERT_EQ(cluster_reconfig_start_pending, 1);
	UT_ASSERT_EQ((int)InterruptPending, 1);

	/* Restore for any subsequent tests; harmless either way. */
	cluster_reconfig_start_pending = 0;
	InterruptPending = 0;
}

UT_TEST(test_freeze_handler_arms_process_interrupts)
{
	ClusterFenceFreezePending = 0;
	InterruptPending = 0;
	ut_freeze_writes_set_count = 0;

	cluster_handle_freeze_writes_interrupt();

	UT_ASSERT_EQ(ut_freeze_writes_set_count, 1);
	UT_ASSERT_EQ((int)ClusterFenceFreezePending, 1);
	UT_ASSERT_EQ((int)InterruptPending, 1);

	/* Restore for any subsequent tests; harmless either way. */
	ClusterFenceFreezePending = 0;
	InterruptPending = 0;
}

UT_TEST(test_ges_bast_cancel_handlers_arm_process_interrupts)
{
	cluster_ges_bast_pending = 0;
	cluster_ges_cancel_pending = 0;
	InterruptPending = 0;

	cluster_handle_ges_bast_interrupt();
	UT_ASSERT_EQ((int)cluster_ges_bast_pending, 1);
	UT_ASSERT_EQ((int)InterruptPending, 1);

	InterruptPending = 0;
	cluster_handle_ges_cancel_interrupt();
	UT_ASSERT_EQ((int)cluster_ges_cancel_pending, 1);
	UT_ASSERT_EQ((int)InterruptPending, 1);

	cluster_ges_bast_pending = 0;
	cluster_ges_cancel_pending = 0;
	InterruptPending = 0;
}


int
main(void)
{
	UT_PLAN(8);
	UT_RUN(test_cluster_reconfig_start_after_recovery_conflict_bufferpin);
	UT_RUN(test_pg_native_reasons_unchanged);
	UT_RUN(test_num_procsignals_is_22);
	UT_RUN(test_cluster_reconfig_start_pending_default_false);
	UT_RUN(test_handler_symbol_linkable);
	UT_RUN(test_handler_sets_pending_flag);
	UT_RUN(test_freeze_handler_arms_process_interrupts);
	UT_RUN(test_ges_bast_cancel_handlers_arm_process_interrupts);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
