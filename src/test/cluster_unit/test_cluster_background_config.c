/*-------------------------------------------------------------------------
 *
 * test_cluster_background_config.c
 *    Execute original auxiliary loop bodies around their producer cuts.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 * IDENTIFICATION
 *    src/test/cluster_unit/test_cluster_background_config.c
 * NOTES
 *    Actual loop bodies and PG_TRY/FINALLY; IO, signals, locks and local gate
 *    are boundaries. Actual gate/family tests and native TAP are separate.
 *-------------------------------------------------------------------------
 */
#include "postgres.h"
#include "access/xlog.h"
#include "access/xlogrecovery.h"
#include "cluster/cluster_adg_xlog.h"
#include "cluster/cluster_cf_enqueue.h"
#include "cluster/cluster_config_use_gate.h"
#include "cluster/cluster_guc.h"
#include "cluster/cluster_lock_owner.h"
#include "cluster/cluster_scn.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "pgtime.h"
#include "postmaster/bgwriter.h"
#include "postmaster/interrupt.h"
#include "storage/buf_internals.h"
#include "storage/bufmgr.h"
#include "storage/condition_variable.h"
#include "storage/latch.h"
#include "storage/proc.h"
#include "storage/standby.h"
#include "utils/timestamp.h"
#include "utils/wait_event.h"
#undef printf
#include "unit_test.h"
UT_DEFINE_GLOBALS();

sigjmp_buf *PG_exception_stack;
ErrorContextCallback *error_context_stack;
static Latch latch;
Latch *MyLatch = &latch;
static PGPROC proc;
PGPROC *MyProc = &proc;
bool cluster_enabled = true, cluster_enable_adg = true;
int cluster_dg_role = CLUSTER_DG_ROLE_PRIMARY, cluster_adg_barrier_interval_ms = 100;
int cluster_boc_sweep_interval_ms = 1, WalWriterDelay = 200;
int BgWriterDelay = 200, CheckPointTimeout = 300, CheckPointWarning = 30;
int XLogArchiveTimeout = 0, wal_level = WAL_LEVEL_REPLICA;
PgStat_CheckpointerStats PendingCheckpointerStats;
/* Test shmem observation boundary. The body uses the original field names;
 * native TAP, not this fixture, proves real checkpoint queue layout/behavior. */
typedef struct {
	int ckpt_lck, ckpt_flags, ckpt_started, ckpt_done;
	ConditionVariable start_cv, done_cv;
} CheckpointerShmemStruct;
static CheckpointerShmemStruct shmem, *CheckpointerShmem = &shmem;
static pg_time_t last_checkpoint_time, last_xlog_switch_time, ckpt_start_time;
static XLogRecPtr ckpt_start_recptr, last_snapshot_lsn;
static TimestampTz last_snapshot_ts;
static double ckpt_cached_elapsed;
static bool ckpt_active;
static bool admitted, held, fail_io;
static unsigned interrupts, cf_polls, lock_polls, absorbs, enters, leaves, io_calls, failed_ends;
static unsigned archives, boc, adg, closes, sleeps, observations;
static unsigned iterations;
static bool was_held_at_interrupt;

#undef SpinLockAcquire
#undef SpinLockRelease
#define SpinLockAcquire(lock) ((void)(lock))
#define SpinLockRelease(lock) ((void)(lock))
#define HIBERNATE_FACTOR 50
#define LOOPS_UNTIL_HIBERNATE 50
#define LOG_SNAPSHOT_INTERVAL_MS 15000

static void
native_interrupt(void)
{
	interrupts++;
	was_held_at_interrupt |= held;
}
static void
HandleCheckpointerInterrupts(void)
{
	native_interrupt();
}
static void
HandleWalWriterInterrupts(void)
{
	native_interrupt();
}
void
HandleMainLoopInterrupts(void)
{
	native_interrupt();
}
void
ResetLatch(Latch *unused)
{
	(void)unused;
}
void
AbsorbSyncRequests(void)
{
	absorbs++;
}
void
cluster_cf_retirement_poll(void)
{
	cf_polls++;
}
void
cluster_lock_owners_service_poll(void)
{
	lock_polls++;
}
bool
cluster_shared_config_delivery_retry_idle(void)
{
	observations++;
	return false;
}
bool
cluster_shared_config_background_begin(void)
{
	UT_ASSERT(interrupts > 0 && cf_polls > 0 && lock_polls > 0);
	enters++;
	held = admitted;
	return admitted;
}
void
cluster_shared_config_background_end(bool completed)
{
	UT_ASSERT(held);
	leaves++;
	if (completed)
		held = false;
	else
		failed_ends++;
}
void
pg_re_throw(void)
{
	if (PG_exception_stack == NULL)
		abort();
	siglongjmp(*PG_exception_stack, 1);
}
static void
native_io(void)
{
	UT_ASSERT(held && enters == 1);
	io_calls++;
	if (fail_io)
		pg_re_throw();
}
void
CreateCheckPoint(int flags)
{
	(void)flags;
	native_io();
}
bool
CreateRestartPoint(int flags)
{
	(void)flags;
	native_io();
	return true;
}
bool
BgBufferSync(WritebackContext *ctx)
{
	(void)ctx;
	native_io();
	return false;
}
bool
XLogBackgroundFlush(void)
{
	native_io();
	return true;
}
bool
RecoveryInProgress(void)
{
	return false;
}
XLogRecPtr
GetInsertRecPtr(void)
{
	return 10;
}
XLogRecPtr
GetXLogReplayRecPtr(TimeLineID *tli)
{
	(void)tli;
	return 10;
}
bool
cluster_cf_owner_eor_local_active(void)
{
	return false;
}
bool
cluster_cf_owner_eor_complete(void)
{
	abort();
}
void
ConditionVariableBroadcast(ConditionVariable *cv)
{
	(void)cv;
}
void
smgrcloseall(void)
{
	UT_ASSERT(held);
	closes++;
}
bool
FirstCallSinceLastCheckpoint(void)
{
	return true;
}
static void
CheckArchiveTimeout(void)
{
	UT_ASSERT(held);
	archives++;
}
void
pgstat_report_checkpointer(void)
{
	UT_ASSERT(held);
}
void
pgstat_report_bgwriter(void)
{
	UT_ASSERT(held);
}
void
pgstat_report_wal(bool force)
{
	(void)force;
	UT_ASSERT(held);
}
void
cluster_scn_boc_tick(void)
{
	UT_ASSERT(held);
	boc++;
}
uint64
cluster_scn_boc_pending_since_last_sweep(void)
{
	return 1;
}
XLogRecPtr
cluster_adg_emit_thread_barrier(void)
{
	UT_ASSERT(held);
	adg++;
	return 100;
}
TimestampTz
GetCurrentTimestamp(void)
{
	return 1000000;
}
XLogRecPtr
GetLastImportantRecPtr(void)
{
	return 0;
}
XLogRecPtr
LogStandbySnapshot(void)
{
	UT_ASSERT(held);
	return 1;
}
void
StrategyNotifyBgWriter(int pgprocno)
{
	(void)pgprocno;
}
void
SetWalWriterSleeping(bool sleeping)
{
	(void)sleeping;
}
int
WaitLatch(Latch *unused, int events, long timeout, uint32 event)
{
	(void)unused;
	UT_ASSERT(!held);
	UT_ASSERT(events == (WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH));
	if (!admitted) {
		UT_ASSERT(event == WAIT_EVENT_RECONFIG_SHARED_CONFIG_WAIT && timeout == 100L);
		UT_ASSERT_EQ(io_calls + boc + adg + archives, 0);
	}
	sleeps++;
	return WL_LATCH_SET;
}
bool
errstart(int elevel, const char *domain)
{
	(void)elevel;
	(void)domain;
	abort();
}
bool
errstart_cold(int elevel, const char *domain)
{
	return errstart(elevel, domain);
}
void
errfinish(const char *file, int line, const char *fn)
{
	abort();
}
int
errcode(int code)
{
	return code;
}
int
errmsg(const char *fmt, ...)
{
	return 0;
}
int
errhint(const char *fmt, ...)
{
	return 0;
}
int
errmsg_plural(const char *one, const char *many, unsigned long n, ...)
{
	return 0;
}

/* An external scheduling boundary bounds the original loop without letting
 * compilation fold its sigsetjmp scopes into a single linear execution. */
static pg_noinline bool
native_next_iteration(void)
{
	return iterations++ == 0;
}

static void
checkpoint_loop(void)
{
#include "test_cluster_background_checkpointer.inc"
}
static void
bgwriter_loop(void)
{
	WritebackContext wb_context;
	bool prev_hibernate = false;
#include "test_cluster_background_bgwriter.inc"
}
#undef HIBERNATE_FACTOR
#define HIBERNATE_FACTOR 25
static void
walwriter_loop(void)
{
	int left_till_hibernate = LOOPS_UNTIL_HIBERNATE;
	bool hibernating = false;
	int64 last_adg_barrier_ms = 0;
#include "test_cluster_background_walwriter.inc"
}
static void (*const loops[])(void) = { checkpoint_loop, bgwriter_loop, walwriter_loop };

static void
reset(void)
{
	memset(&shmem, 0, sizeof(shmem));
	shmem.ckpt_flags = CHECKPOINT_FORCE;
	last_checkpoint_time = (pg_time_t)time(NULL);
	interrupts = cf_polls = lock_polls = absorbs = enters = leaves = io_calls = failed_ends = 0;
	archives = boc = adg = closes = sleeps = observations = 0;
	iterations = 0;
	admitted = true;
	held = fail_io = was_held_at_interrupt = false;
	PG_exception_stack = NULL;
}
UT_TEST(closed_native_loops_keep_control_and_requests)
{
	for (size_t i = 0; i < lengthof(loops); i++) {
		reset();
		admitted = false;
		loops[i]();
		UT_ASSERT_EQ(interrupts, 1);
		UT_ASSERT_EQ(cf_polls, 1);
		UT_ASSERT_EQ(lock_polls, 1);
		UT_ASSERT_EQ(enters, 1);
		UT_ASSERT_EQ(leaves + io_calls + boc + adg + archives, 0);
		UT_ASSERT_EQ(sleeps, 1);
		UT_ASSERT_EQ(shmem.ckpt_flags, CHECKPOINT_FORCE);
		UT_ASSERT_EQ(shmem.ckpt_started, 0);
		if (i == 0)
			UT_ASSERT_EQ(absorbs, 1);
	}
}
UT_TEST(native_pass_retires_after_last_work_before_sleep)
{
	for (size_t i = 0; i < lengthof(loops); i++) {
		reset();
		loops[i]();
		UT_ASSERT_EQ(io_calls, 1);
		UT_ASSERT_EQ(leaves, 1);
		UT_ASSERT(!held && !was_held_at_interrupt && failed_ends == 0);
		UT_ASSERT_EQ(sleeps, 1);
		if (i == 0) {
			UT_ASSERT_EQ(shmem.ckpt_flags, 0);
			UT_ASSERT_EQ(shmem.ckpt_started, 1);
			UT_ASSERT_EQ(shmem.ckpt_done, 1);
			UT_ASSERT_EQ(archives, 1);
			UT_ASSERT_EQ(interrupts, 2);
		} else if (i == 2) {
			UT_ASSERT_EQ(boc, 1);
			UT_ASSERT_EQ(adg, 1);
		}
	}
}
UT_TEST(native_error_marks_failure_and_rethrows_without_false_retirement)
{
	for (size_t i = 0; i < lengthof(loops); i++) {
		sigjmp_buf outer;
		reset();
		fail_io = true;
		PG_exception_stack = &outer;
		if (sigsetjmp(outer, 0) == 0) {
			loops[i]();
			UT_ASSERT(false);
		}
		UT_ASSERT(PG_exception_stack == &outer && held);
		UT_ASSERT_EQ(failed_ends, 1);
		UT_ASSERT_EQ(leaves, 1);
		UT_ASSERT_EQ(io_calls, 1);
		UT_ASSERT_EQ(boc + adg + sleeps + archives, 0);
		if (i == 0)
			UT_ASSERT(shmem.ckpt_started == 1 && shmem.ckpt_done == 0);
		PG_exception_stack = NULL;
	}
}

int
main(void)
{
	UT_PLAN(3);
	UT_RUN(closed_native_loops_keep_control_and_requests);
	UT_RUN(native_pass_retires_after_last_work_before_sleep);
	UT_RUN(native_error_marks_failure_and_rethrows_without_false_retirement);
	UT_DONE();
	return ut_failed_count ? 1 : 0;
}
