/*-------------------------------------------------------------------------
 *
 * test_cluster_phase_publication.c
 *    Deterministic publication interleavings through the real phase accessors.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *    src/test/cluster_unit/test_cluster_phase_publication.c
 *
 * NOTES
 *    PGRAC original. The unit object keeps the production publisher and reader;
 *    only barrier scheduling and operating-system dependency boundaries differ.
 *-------------------------------------------------------------------------
 */
#define main startup_fixture_main
#define ShmemInitStruct phase_original_shmem_init
#define ResetLatch phase_original_reset_latch
#define WaitLatch phase_original_wait_latch
#include "test_cluster_startup_phase.c"
#undef main
#undef ShmemInitStruct
#undef ResetLatch
#undef WaitLatch

#include <sys/mman.h>
#include "access/xlog_internal.h"
#include "catalog/pg_control.h"
#include "cluster/cluster_recovery_anchor.h"
#include "cluster/cluster_stats.h"
#include "cluster/cluster_wal_state.h"
#include "cluster/cluster_wal_thread.h"

static void *publication_shmem;
static int reader_pipe[2];
static int writer_pipe[2];
static bool pause_writer;
static unsigned pause_at_write_barrier;
static unsigned write_barriers;
static bool pipe_wait;
static bool other_work_pending;
static unsigned read_barriers;
static unsigned publish_at_read_barrier;
static ClusterStartupPhase interleaved_phase;
static unsigned anchor_writes;
static ClusterWalStateSlot anchor_slot;
static bool anchor_identity_drift;

void *ShmemInitStruct(const char *name, Size size, bool *found);
void ResetLatch(Latch *latch);
int WaitLatch(Latch *latch, int wake_events, long timeout, uint32 wait_event);
void phase_fixture_read_barrier_hook(void);
void phase_fixture_write_barrier_hook(void);

void *
ShmemInitStruct(const char *name, Size size, bool *found)
{
	if (publication_shmem == NULL) {
		publication_shmem = mmap(NULL, 4096, PROT_READ | PROT_WRITE, MAP_SHARED | MAP_ANON, -1, 0);
		UT_ASSERT(publication_shmem != MAP_FAILED);
	}
	UT_ASSERT(size <= 4096);
	*found = phase_attach_existing;
	phase_fixture_state = publication_shmem;
	return publication_shmem;
}

static void
pipe_byte(int fd, bool send)
{
	char byte = 'P';
	ssize_t n = send ? write(fd, &byte, 1) : read(fd, &byte, 1);
	UT_ASSERT_EQ(n, 1);
}

void
ResetLatch(Latch *latch)
{
	phase_reset_calls++;
	latch->is_set = false;
}

int
WaitLatch(Latch *latch, int wake_events, long timeout, uint32 wait_event)
{
	if (!pipe_wait)
		return phase_original_wait_latch(latch, wake_events, timeout, wait_event);
	UT_ASSERT_EQ(wake_events, WL_LATCH_SET | WL_TIMEOUT | WL_POSTMASTER_DEATH);
	UT_ASSERT_EQ(timeout, 1);
	UT_ASSERT_EQ(wait_event, WAIT_EVENT_CLUSTER_STARTUP_PHASE_PUBLISH);
	UT_ASSERT_EQ(phase_lwlock_depth, 0);
	phase_wait_calls++;
	/* A different notification arrives while the reader yields to its writer. */
	other_work_pending = true;
	latch->is_set = true;
	pipe_byte(reader_pipe[1], true);
	pipe_byte(writer_pipe[0], false);
	return WL_LATCH_SET;
}

void
phase_fixture_write_barrier_hook(void)
{
	if (!pause_writer || ++write_barriers != pause_at_write_barrier)
		return;
	UT_ASSERT_EQ(pg_atomic_read_u32(&phase_fixture_state->phase_change_count) & 1, 1);
	pause_writer = false;
	pipe_byte(writer_pipe[1], true);
	pipe_byte(reader_pipe[0], false);
}

void
phase_fixture_read_barrier_hook(void)
{
	bool saved_under = IsUnderPostmaster;
	PGPROC *saved_proc = MyProc;

	if (publish_at_read_barrier == 0 || ++read_barriers != publish_at_read_barrier)
		return;
	publish_at_read_barrier = 0;
	if (anchor_identity_drift) {
		phase_test_self_incarnation++;
	} else {
		IsUnderPostmaster = false;
		MyProc = NULL;
		phase4_test_now = 9000;
		cluster_advance_phase(interleaved_phase);
		IsUnderPostmaster = saved_under;
		MyProc = saved_proc;
	}
}

ClusterStatsStatus
cluster_stats_status(void)
{
	return CLUSTER_STATS_SPAWNING;
}
TimestampTz
cluster_stats_spawned_at(void)
{
	return 10000;
}
bool
cluster_cf_held_is_clusterwide(LOCKMODE mode)
{
	return mode == ExclusiveLock && phase_test_cf_held;
}
bool
cluster_cf_held_is_usable(LOCKMODE mode)
{
	return cluster_cf_held_is_clusterwide(mode);
}
bool
cluster_cf_exactly_one_declared_node(void)
{
	return false;
}
bool
cluster_cf_owner_eor_local_active(void)
{
	return false;
}
bool
cluster_wal_thread_dir_configured(void)
{
	return true;
}
bool
cluster_wal_thread_dir_validated(void)
{
	return true;
}
uint16
cluster_wal_thread_dump_thread_id(void)
{
	return cluster_wal_thread_id();
}
bool
cluster_wal_state_registry_ready(void)
{
	return true;
}
ClusterWalSlotVerdict
cluster_wal_state_read_slot(uint16 thread_id, ClusterWalStateSlot *slot)
{
	*slot = anchor_slot;
	return CLUSTER_WAL_SLOT_OK;
}
bool
cluster_write_fence_allowed(void)
{
	return true;
}
void
cluster_write_fence_reject_if_fenced(const char *op)
{}
void
cluster_recovery_anchor_write(const ClusterRecoveryAnchor *anchor)
{
	anchor_writes++;
}

/* Original complete publisher and both admission predicates, without edits. */
#include "test_cluster_phase_anchor_native.inc"

static void
reset_publication(void)
{
	IsUnderPostmaster = false;
	MyProc = NULL;
	CritSectionCount = 0;
	phase_test_cf_held = false;
	phase_attach_existing = false;
	pause_writer = pipe_wait = false;
	pause_at_write_barrier = 1;
	write_barriers = 0;
	phase_complete_on_wait = false;
	phase_wait_calls = phase_reset_calls = read_barriers = 0;
	publish_at_read_barrier = 0;
	anchor_identity_drift = false;
	anchor_writes = 0;
	cluster_phase_shmem_init();
}

UT_TEST(test_changed_cut_is_retried_before_timestamp_or_history_returns)
{
	for (int barrier = 1; barrier <= 2; barrier++) {
		char history[256];
		reset_publication();
		phase4_test_now = 1000;
		cluster_advance_phase(CLUSTER_PHASE_0_BASE);
		interleaved_phase = CLUSTER_PHASE_1_CLUSTER;
		publish_at_read_barrier = barrier;
		IsUnderPostmaster = true;
		UT_ASSERT_EQ(cluster_phase_started_at(CLUSTER_PHASE_1_CLUSTER), 9000);
		UT_ASSERT_EQ(cluster_current_phase(), CLUSTER_PHASE_1_CLUSTER);
		UT_ASSERT_EQ(phase_fixture_state->phase_history_count, 2);
		cluster_phase_history_format(history, sizeof(history));
		UT_ASSERT_STR_EQ(history, "phase0_base@(stub),phase1_cluster@(stub)");
		UT_ASSERT_EQ(phase_wait_calls, 0);
	}
	reset_publication();
}

UT_TEST(test_real_writer_pause_and_other_notification_without_pgproc)
{
	for (int critical = 0; critical <= 1; critical++) {
		pid_t child;
		int status;
		reset_publication();
		cluster_advance_phase(CLUSTER_PHASE_0_BASE);
		UT_ASSERT_EQ(pipe(reader_pipe), 0);
		UT_ASSERT_EQ(pipe(writer_pipe), 0);
		fflush(NULL);
		child = fork();
		UT_ASSERT(child >= 0);
		if (child == 0) {
			alarm(5);
			close(reader_pipe[0]);
			close(writer_pipe[1]);
			IsUnderPostmaster = true;
			CritSectionCount = critical;
			phase_test_cf_held = critical;
			pipe_wait = true;
			other_work_pending = false;
			pipe_byte(writer_pipe[0], false);
			UT_ASSERT_EQ(cluster_phase_started_at(CLUSTER_PHASE_1_CLUSTER), 7000);
			UT_ASSERT_EQ(phase_wait_calls, 1);
			UT_ASSERT_EQ(phase_reset_calls, 1);
			UT_ASSERT(MyLatch->is_set);
			UT_ASSERT(other_work_pending);
			/* The owning loop keeps its wakeup as well as its persistent work. */
			if (other_work_pending)
				other_work_pending = false;
			UT_ASSERT(!other_work_pending);
			_exit(ut_current_failed ? 1 : 0);
		}
		close(reader_pipe[1]);
		close(writer_pipe[0]);
		pause_writer = true;
		phase4_test_now = 7000;
		cluster_advance_phase(CLUSTER_PHASE_1_CLUSTER);
		pipe_byte(writer_pipe[1], true);
		UT_ASSERT_EQ(waitpid(child, &status, 0), child);
		UT_ASSERT(WIFEXITED(status));
		UT_ASSERT_EQ(WEXITSTATUS(status), 0);
		close(reader_pipe[0]);
		close(writer_pipe[1]);
	}
	reset_publication();
}

UT_TEST(test_uncontended_snapshot_preserves_callers_latch)
{
	for (int signaled = 0; signaled <= 1; signaled++) {
		reset_publication();
		phase4_test_now = 7000;
		cluster_advance_phase(CLUSTER_PHASE_0_BASE);
		IsUnderPostmaster = true;
		MyLatch->is_set = signaled;
		UT_ASSERT_EQ(cluster_phase_started_at(CLUSTER_PHASE_0_BASE), 7000);
		UT_ASSERT_EQ(phase_reset_calls, 0);
		UT_ASSERT_EQ(phase_wait_calls, 0);
		UT_ASSERT_EQ(MyLatch->is_set, signaled);
	}
	reset_publication();
}

static void
prepare_anchor(ClusterStartupPhase last_phase)
{
	reset_publication();
	phase4_test_now = 1000;
	for (int phase = CLUSTER_PHASE_0_BASE; phase <= last_phase; phase++)
		cluster_advance_phase(phase);
	phase_test_self_incarnation = 11;
	phase_test_membership_member = false;
	phase_test_nonmember_state = CLUSTER_MEMBER_ABSENT;
	phase_test_cf_held = true;
	IsUnderPostmaster = true;
	CritSectionCount = 1;
	memset(&anchor_slot, 0, sizeof(anchor_slot));
	anchor_slot.thread_id = cluster_wal_thread_id();
	anchor_slot.node_id = cluster_node_id;
	anchor_slot.state = CLUSTER_WAL_SLOT_STATE_ACTIVE;
	anchor_slot.started_at = 10000;
}

UT_TEST(test_anchor_waits_for_original_writer_while_holding_cf_x)
{
	pid_t child;
	int status;
	prepare_anchor(CLUSTER_PHASE_3_RECOVERY);
	UT_ASSERT_EQ(pipe(reader_pipe), 0);
	UT_ASSERT_EQ(pipe(writer_pipe), 0);
	fflush(NULL);
	child = fork();
	UT_ASSERT(child >= 0);
	if (child == 0) {
		CheckPoint cp = { 0 };
		alarm(5);
		close(reader_pipe[0]);
		close(writer_pipe[1]);
		pipe_wait = true;
		pipe_byte(writer_pipe[0], false);
		cluster_recovery_anchor_publish_checkpoint(100, &cp, 12345, DB_IN_PRODUCTION, 0);
		UT_ASSERT_EQ(anchor_writes, 1);
		UT_ASSERT_EQ(phase_wait_calls, 1);
		UT_ASSERT_EQ(CritSectionCount, 1);
		UT_ASSERT(phase_test_cf_held);
		_exit(ut_current_failed ? 1 : 0);
	}
	close(reader_pipe[1]);
	close(writer_pipe[0]);
	IsUnderPostmaster = false;
	CritSectionCount = 0;
	phase_test_cf_held = false;
	pause_writer = true;
	pause_at_write_barrier = 3;
	phase4_test_now = 7000;
	cluster_advance_phase(CLUSTER_PHASE_4_NORMAL);
	pipe_byte(writer_pipe[1], true);
	UT_ASSERT_EQ(waitpid(child, &status, 0), child);
	UT_ASSERT(WIFEXITED(status));
	UT_ASSERT_EQ(WEXITSTATUS(status), 0);
	close(reader_pipe[0]);
	close(writer_pipe[1]);
	reset_publication();
}

UT_TEST(test_anchor_refuses_phase_or_identity_drift_during_snapshot)
{
	for (int drift = 0; drift <= 1; drift++) {
		CheckPoint cp = { 0 };
		bool caught = false;
		prepare_anchor(CLUSTER_PHASE_4_NORMAL);
		interleaved_phase = CLUSTER_PHASE_RUNNING;
		anchor_identity_drift = drift;
		publish_at_read_barrier = 2;
		phase4_capture_fatal = true;
		if (setjmp(phase4_fatal_jump) == 0)
			cluster_recovery_anchor_publish_checkpoint(100, &cp, 12345, DB_IN_PRODUCTION, 0);
		else
			caught = true;
		phase4_capture_fatal = false;
		UT_ASSERT(caught);
		UT_ASSERT_EQ(anchor_writes, 0);
	}
	reset_publication();
}

int
main(void)
{
	alarm(15);
	UT_PLAN(5);
	UT_RUN(test_uncontended_snapshot_preserves_callers_latch);
	UT_RUN(test_changed_cut_is_retried_before_timestamp_or_history_returns);
	UT_RUN(test_real_writer_pause_and_other_notification_without_pgproc);
	UT_RUN(test_anchor_waits_for_original_writer_while_holding_cf_x);
	UT_RUN(test_anchor_refuses_phase_or_identity_drift_during_snapshot);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
