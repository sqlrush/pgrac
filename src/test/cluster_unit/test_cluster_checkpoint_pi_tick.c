/*-------------------------------------------------------------------------
 *
 * test_cluster_checkpoint_pi_tick.c
 *	  The shared PI writeback batch during a checkpoint's write phase.
 *
 *	  Runs the real CheckpointWriteDelay and its PGRAC tick helper,
 *	  extracted from checkpointer.c.  In shared mode the batch advances at
 *	  most every CLUSTER_CHECKPOINT_PI_TICK_MS, whether the write phase is
 *	  on schedule, behind it or immediate; never for a shutdown or
 *	  end-of-recovery checkpoint, during a shutdown request, outside the
 *	  checkpointer or without shared configuration.  CheckPointGuts
 *	  releases the batch between the write phase and the sync phase.
 *
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/test/cluster_unit/test_cluster_checkpoint_pi_tick.c
 *
 * NOTES
 *	  This is a pgrac-original file (no derivation from PostgreSQL).
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/xlog.h"
#include "cluster/cluster_guc.h"
#include "cluster/cluster_pi_writeback.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "postmaster/bgwriter.h"
#include "postmaster/interrupt.h"
#include "storage/latch.h"
#include "storage/procsignal.h"
#include "utils/guc.h"
#include "utils/timestamp.h"
#include "utils/wait_event.h"
#include "unit_test.h"

UT_DEFINE_GLOBALS();

AuxProcType MyAuxProcType = NotAnAuxProcess;
volatile sig_atomic_t ShutdownRequestPending = false;
volatile sig_atomic_t ConfigReloadPending = false;
volatile sig_atomic_t ProcSignalBarrierPending = false;
bool cluster_enabled = false;
bool cluster_shared_config = false;
struct Latch *MyLatch = NULL;

/* The native duties of the write delay. */
static bool on_schedule, immediate_requested;
static unsigned sleeps, absorbs, ticks;
static TimestampTz clock_now = 1000000000;

static bool
ImmediateCheckpointRequested(void)
{
	return immediate_requested;
}
static bool
IsCheckpointOnSchedule(double progress pg_attribute_unused())
{
	return on_schedule;
}
static void
CheckArchiveTimeout(void)
{}
static void
UpdateSharedMemoryConfig(void)
{}
void
ProcessConfigFile(GucContext context pg_attribute_unused())
{}
void
AbsorbSyncRequests(void)
{
	absorbs++;
}
void
pgstat_report_checkpointer(void)
{}
void
ProcessProcSignalBarrier(void)
{}
int
WaitLatch(Latch *latch pg_attribute_unused(), int wakeEvents pg_attribute_unused(), long timeout,
		  uint32 wait_event_info)
{
	UT_ASSERT_EQ(timeout, 100);
	UT_ASSERT_EQ(wait_event_info, WAIT_EVENT_CHECKPOINT_WRITE_DELAY);
	sleeps++;
	return 0;
}
void
ResetLatch(Latch *latch pg_attribute_unused())
{}
TimestampTz
GetCurrentTimestamp(void)
{
	return clock_now;
}
bool
TimestampDifferenceExceeds(TimestampTz start_time, TimestampTz stop_time, int msec)
{
	return stop_time - start_time >= (TimestampTz)msec * 1000;
}
bool
cluster_pi_writeback_checkpointer_tick_v1(void)
{
	ticks++;
	return true;
}

void
ExceptionalCondition(const char *condition pg_attribute_unused(),
					 const char *file pg_attribute_unused(), int line pg_attribute_unused())
{
	abort();
}

#ifndef USE_PGRAC_CLUSTER
#define USE_PGRAC_CLUSTER 1
#endif
#include "test_cluster_checkpoint_pi_tick.inc"
#include "test_cluster_checkpoint_pi_order.inc"

static void
reset(bool shared)
{
	MyAuxProcType = CheckpointerProcess;
	cluster_enabled = cluster_shared_config = shared;
	ShutdownRequestPending = ConfigReloadPending = ProcSignalBarrierPending = false;
	on_schedule = true;
	immediate_requested = false;
	sleeps = absorbs = ticks = 0;
	/* Leave any earlier tick far behind. */
	clock_now += (TimestampTz)10 * CLUSTER_CHECKPOINT_PI_TICK_MS * 1000;
}

static void
advance_ms(int ms)
{
	clock_now += (TimestampTz)ms * 1000;
}

UT_TEST(write_phase_on_schedule_ticks_at_the_rate_limit)
{
	reset(true);
	CheckpointWriteDelay(0, 0.1);
	UT_ASSERT_EQ(sleeps, 1);
	UT_ASSERT_EQ(ticks, 1);
	advance_ms(CLUSTER_CHECKPOINT_PI_TICK_MS - 1);
	CheckpointWriteDelay(0, 0.2);
	UT_ASSERT_EQ(sleeps, 2);
	UT_ASSERT_EQ(ticks, 1);
	advance_ms(1);
	CheckpointWriteDelay(0, 0.3);
	UT_ASSERT_EQ(ticks, 2);
}

UT_TEST(behind_schedule_and_immediate_writes_still_tick)
{
	reset(true);
	on_schedule = false;
	CheckpointWriteDelay(0, 0.9);
	UT_ASSERT_EQ(sleeps, 0);
	UT_ASSERT_EQ(ticks, 1);
	CheckpointWriteDelay(0, 0.9);
	UT_ASSERT_EQ(ticks, 1);
	advance_ms(CLUSTER_CHECKPOINT_PI_TICK_MS);
	CheckpointWriteDelay(CHECKPOINT_IMMEDIATE, 1.0);
	UT_ASSERT_EQ(sleeps, 0);
	UT_ASSERT_EQ(ticks, 2);
	advance_ms(CLUSTER_CHECKPOINT_PI_TICK_MS);
	on_schedule = true;
	immediate_requested = true;
	CheckpointWriteDelay(0, 0.1);
	UT_ASSERT_EQ(sleeps, 0);
	UT_ASSERT_EQ(ticks, 3);
}

UT_TEST(shutdown_and_end_of_recovery_never_tick)
{
	reset(true);
	CheckpointWriteDelay(CHECKPOINT_IS_SHUTDOWN, 0.1);
	CheckpointWriteDelay(CHECKPOINT_END_OF_RECOVERY | CHECKPOINT_IMMEDIATE, 0.1);
	UT_ASSERT_EQ(ticks, 0);
	ShutdownRequestPending = true;
	CheckpointWriteDelay(0, 0.1);
	UT_ASSERT_EQ(ticks, 0);
	/* The native duty is unchanged: no sleep during a shutdown request. */
	UT_ASSERT_EQ(sleeps, 1);
	ShutdownRequestPending = false;
	CheckpointWriteDelay(0, 0.1);
	UT_ASSERT_EQ(ticks, 1);
}

UT_TEST(native_and_non_checkpointer_never_tick)
{
	reset(false);
	CheckpointWriteDelay(0, 0.1);
	UT_ASSERT_EQ(sleeps, 1);
	UT_ASSERT_EQ(ticks, 0);
	cluster_enabled = true;
	CheckpointWriteDelay(0, 0.1);
	UT_ASSERT_EQ(ticks, 0);
	reset(true);
	MyAuxProcType = BgWriterProcess;
	CheckpointWriteDelay(0, 0.1);
	UT_ASSERT_EQ(sleeps + absorbs + ticks, 0);
}

/* The batch never spans the sync phase or the checkpoint's own writes. */
UT_TEST(checkpoint_guts_releases_between_write_and_sync)
{
	UT_ASSERT(GUTS_LINE_BUFFERS > 0);
	UT_ASSERT(GUTS_LINE_RELEASE > GUTS_LINE_BUFFERS);
	UT_ASSERT(GUTS_LINE_UNDO > GUTS_LINE_RELEASE);
	UT_ASSERT(GUTS_LINE_SYNC > GUTS_LINE_UNDO);
}

UT_TEST(clock_rollback_restarts_the_interval_without_stalling)
{
	reset(true);
	CheckpointWriteDelay(CHECKPOINT_IMMEDIATE, 0.1);
	UT_ASSERT_EQ(ticks, 1);
	advance_ms(-10000);
	CheckpointWriteDelay(CHECKPOINT_IMMEDIATE, 0.2);
	UT_ASSERT_EQ(ticks, 2);
	advance_ms(CLUSTER_CHECKPOINT_PI_TICK_MS - 1);
	CheckpointWriteDelay(CHECKPOINT_IMMEDIATE, 0.3);
	UT_ASSERT_EQ(ticks, 2);
	advance_ms(1);
	CheckpointWriteDelay(CHECKPOINT_IMMEDIATE, 0.4);
	UT_ASSERT_EQ(ticks, 3);
}

int
main(void)
{
	UT_PLAN(6);
	UT_RUN(write_phase_on_schedule_ticks_at_the_rate_limit);
	UT_RUN(behind_schedule_and_immediate_writes_still_tick);
	UT_RUN(shutdown_and_end_of_recovery_never_tick);
	UT_RUN(native_and_non_checkpointer_never_tick);
	UT_RUN(checkpoint_guts_releases_between_write_and_sync);
	UT_RUN(clock_rollback_restarts_the_interval_without_stalling);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
