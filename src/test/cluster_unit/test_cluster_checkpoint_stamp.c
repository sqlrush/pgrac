/* Author: SqlRush <sqlrush@gmail.com>
 * Native checkpoint consumer plus original shared slot. WAL insertion, IO,
 * shutdown and scheduler are controlled boundaries, not field acceptance. */
#include "postgres.h"
#include "access/xlog.h"
#include "catalog/pg_control.h"
#include "postmaster/startup.h"
#include "cluster/cluster_epoch.h"
#include "cluster/cluster_wal_retained_cut.h"
#include "postmaster/checkpointer.h"
#include "postmaster/interrupt.h"
#include "storage/latch.h"
#include "utils/wait_event.h"
int stamp_state_fixture_main(void);
#define main stamp_state_fixture_main
#include "test_cluster_recovery_stamp.c"
#undef main

static sigjmp_buf error_boundary;
static int waits, absorbs, guts, retained, charges, startup_interrupts;
static bool stop_now, stopping_budget_fail, interrupt_fail, drift;
static ClusterRecoveryStampHandleV1 held, later;
static Latch fixture_latch;
Latch *MyLatch = &fixture_latch;
volatile sig_atomic_t ShutdownRequestPending;
static void (*on_wait)(void);

bool
errstart(int level, const char *domain)
{
	return true;
}
bool
errstart_cold(int level, const char *domain)
{
	return errstart(level, domain);
}
int
errmsg(const char *format, ...)
{
	return 0;
}
int
errcode(int sqlerrcode)
{
	return 0;
}
int
errdetail(const char *format, ...)
{
	return 0;
}
void
errfinish(const char *file, int line, const char *func)
{
	siglongjmp(error_boundary, 1);
}
static void
fixture_interrupts(void)
{
	if (interrupt_fail)
		siglongjmp(error_boundary, 1);
}
#undef CHECK_FOR_INTERRUPTS
#define CHECK_FOR_INTERRUPTS() fixture_interrupts()
void
HandleStartupProcInterrupts(void)
{
	startup_interrupts++;
	fixture_interrupts();
}
static void
ClusterCheckpointYieldForShutdown(bool online)
{
	if (online && (stop_now || ShutdownRequestPending))
		siglongjmp(error_boundary, 1);
}
uint64
cluster_epoch_get_current(void)
{
	return owner.writer.epoch;
}
bool
cluster_wal_thread_current_v2_ref(ClusterWalSourceRef *out)
{
	*out = owner.writer.ref;
	if (drift)
		out->claim.identity.origin_owner_incarnation++;
	return true;
}
uint64
cluster_checkpoint_wait_budget_now(void)
{
	return 1;
}
long
cluster_checkpoint_wait_budget_charge(bool pending, uint64 started, const ClusterWalSourceRef *ref,
									  uint64 epoch)
{
	charges++;
	UT_ASSERT_EQ(epoch, owner.writer.epoch);
	if (stopping_budget_fail)
		siglongjmp(error_boundary, 1);
	return 7;
}
void
ResetLatch(Latch *latch)
{
	UT_ASSERT(latch == MyLatch);
}
void
AbsorbSyncRequests(void)
{
	absorbs++;
}
int
WaitLatch(Latch *latch, int events, long timeout, uint32 event)
{
	waits++;
	UT_ASSERT(waits < 5);
	UT_ASSERT_EQ(guts, 0);
	UT_ASSERT_EQ(retained, 0);
	UT_ASSERT_EQ(event, WAIT_EVENT_RECOVERY_STAMP);
	UT_ASSERT(timeout > 0 && timeout <= 20);
	if (on_wait)
		on_wait();
	else
		siglongjmp(error_boundary, 1);
	return WL_LATCH_SET;
}
void
cluster_wal_retained_cut_before_sync_v1(XLogRecPtr redo)
{
	retained++;
	UT_ASSERT_EQ(guts, 0);
}
static void
CheckPointGuts(XLogRecPtr redo, int flags)
{
	guts++;
}
#include "test_cluster_checkpoint_stamp.inc"
static void
boundary(int flags)
{
	bool shutdown = (flags & (CHECKPOINT_IS_SHUTDOWN | CHECKPOINT_END_OF_RECOVERY)) != 0;
	CheckPoint checkPoint = { .redo = 0x2000100 };
#include "test_cluster_checkpoint_stamp_boundary.inc"
}
static void
reset_fixture(void)
{
	fresh();
	waits = absorbs = guts = retained = charges = startup_interrupts = 0;
	stop_now = stopping_budget_fail = interrupt_fail = drift = false;
	ShutdownRequestPending = false;
	on_wait = NULL;
}
static bool
run_boundary(int flags)
{
	if (sigsetjmp(error_boundary, 1) == 0) {
		boundary(flags);
		return true;
	}
	return false;
}
static void
finish_then_new(void)
{
	BackendType saved = MyBackendType;
	MyBackendType = B_STARTUP;
	UT_ASSERT(cluster_recovery_stamp_durable_v1(&held));
	UT_ASSERT(cluster_recovery_stamp_clear_v1(&held));
	UT_ASSERT(begin(&later));
	MyBackendType = saved;
}
static void
cancel_preinsert(void)
{
	BackendType saved = MyBackendType;
	MyBackendType = B_STARTUP;
	UT_ASSERT(cluster_recovery_stamp_cancel_v1(&held));
	MyBackendType = saved;
}
static void
stop_while_waiting(void)
{
	stop_now = true;
}
UT_TEST(native_capture_precedes_target_sync_and_ignores_later_windows)
{
	reset_fixture();
	if (!begin(&held))
		return;
	insert(&held);
	MyBackendType = B_CHECKPOINTER;
	on_wait = finish_then_new;
	UT_ASSERT(run_boundary(0));
	UT_ASSERT_EQ(waits, 1);
	UT_ASSERT_EQ(absorbs, 1);
	UT_ASSERT_EQ(retained, 1);
	UT_ASSERT_EQ(guts, 1);
	UT_ASSERT_EQ(region.thread_replay[2].stamp.phase, CLUSTER_RECOVERY_STAMP_PRE_INSERT);
}
UT_TEST(native_checkpoint_waits_for_d0_commit_and_abort)
{
	for (unsigned outcome = TT_SLOT_COMMITTED; outcome <= TT_SLOT_ABORTED; outcome++) {
		reset_fixture();
		if (!begin_d0(&held, outcome)) {
			UT_ASSERT(false);
			return;
		}
		insert(&held);
		MyBackendType = B_CHECKPOINTER;
		on_wait = finish_then_new;
		UT_ASSERT(run_boundary(0));
		UT_ASSERT_EQ(waits, 1);
		UT_ASSERT_EQ(absorbs, 1);
		UT_ASSERT_EQ(retained, 1);
		UT_ASSERT_EQ(guts, 1);
		UT_ASSERT_EQ(region.thread_replay[2].stamp.phase, CLUSTER_RECOVERY_STAMP_PRE_INSERT);
	}
}
UT_TEST(empty_and_preinsert_cancellation)
{
	reset_fixture();
	MyBackendType = B_CHECKPOINTER;
	UT_ASSERT(run_boundary(0));
	UT_ASSERT_EQ(waits, 0);
	UT_ASSERT_EQ(guts, 1);
	reset_fixture();
	if (!begin(&held))
		return;
	MyBackendType = B_CHECKPOINTER;
	on_wait = cancel_preinsert;
	UT_ASSERT(run_boundary(0));
	UT_ASSERT_EQ(waits, 1);
	UT_ASSERT_EQ(guts, 1);
}
UT_TEST(error_shutdown_and_native_budget_never_clear_window)
{
	reset_fixture();
	if (!begin(&held))
		return;
	insert(&held);
	MyBackendType = B_CHECKPOINTER;
	on_wait = stop_while_waiting;
	UT_ASSERT(!run_boundary(0));
	UT_ASSERT_EQ(waits, 1);
	UT_ASSERT_EQ(guts, 0);
	UT_ASSERT_EQ(region.thread_replay[2].stamp.phase, CLUSTER_RECOVERY_STAMP_INSERTED);
	reset_fixture();
	if (!begin(&held))
		return;
	insert(&held);
	MyBackendType = B_CHECKPOINTER;
	stopping_budget_fail = true;
	UT_ASSERT(!run_boundary(CHECKPOINT_IS_SHUTDOWN));
	UT_ASSERT_EQ(guts, 0);
	UT_ASSERT(charges > 0);
	UT_ASSERT_EQ(region.thread_replay[2].stamp.phase, CLUSTER_RECOVERY_STAMP_INSERTED);
	reset_fixture();
	if (!begin(&held))
		return;
	insert(&held);
	MyBackendType = B_CHECKPOINTER;
	interrupt_fail = true;
	UT_ASSERT(!run_boundary(0));
	UT_ASSERT_EQ(guts, 0);
	UT_ASSERT_EQ(region.thread_replay[2].stamp.phase, CLUSTER_RECOVERY_STAMP_INSERTED);
	reset_fixture();
	if (!begin(&held))
		return;
	insert(&held);
	MyBackendType = B_CHECKPOINTER;
	on_wait = finish_then_new;
	UT_ASSERT(run_boundary(CHECKPOINT_IS_SHUTDOWN));
	UT_ASSERT_EQ(waits, 1);
	UT_ASSERT_EQ(charges, 2);
	UT_ASSERT_EQ(guts, 1);
	UT_ASSERT_EQ(retained, 0);
}
UT_TEST(failed_or_missing_owner_blocks_and_eor_empty_is_legal)
{
	reset_fixture();
	if (!begin(&held))
		return;
	insert(&held);
	UT_ASSERT(cluster_recovery_stamp_fail_v1(&held));
	MyBackendType = B_CHECKPOINTER;
	UT_ASSERT(!run_boundary(0));
	UT_ASSERT_EQ(guts, 0);
	UT_ASSERT_EQ(waits, 0);
	reset_fixture();
	cluster_recovery_plan_shmem = NULL;
	MyBackendType = B_CHECKPOINTER;
	UT_ASSERT(!run_boundary(0));
	UT_ASSERT_EQ(guts, 0);
	reset_fixture();
	if (!begin(&held))
		return;
	insert(&held);
	UT_ASSERT(!run_boundary(CHECKPOINT_END_OF_RECOVERY));
	UT_ASSERT_EQ(waits, 0);
	UT_ASSERT_EQ(guts, 0);
	reset_fixture();
	UT_ASSERT(run_boundary(CHECKPOINT_END_OF_RECOVERY));
	UT_ASSERT_EQ(guts, 1);
	UT_ASSERT_EQ(waits, 0);
}
int
main(void)
{
	UT_PLAN(5);
	UT_RUN(native_capture_precedes_target_sync_and_ignores_later_windows);
	UT_RUN(native_checkpoint_waits_for_d0_commit_and_abort);
	UT_RUN(empty_and_preinsert_cancellation);
	UT_RUN(error_shutdown_and_native_budget_never_clear_window);
	UT_RUN(failed_or_missing_owner_blocks_and_eor_empty_is_legal);
	UT_DONE();
	return ut_failed_count ? 1 : 0;
}
